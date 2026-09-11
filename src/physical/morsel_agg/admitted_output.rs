//! Final Arrow construction with admitted payload, scratch and metadata owners.
use super::group_rows::GroupRows;
use crate::{
    execution::{
        reserved_scalar::{ReservedDataType, ReservedScalar},
        reserved_vec::ReservedVec,
        MemoryPool, MemoryReservation, ReservedBufferBuilder,
    },
    planner::{DecimalValue, ScalarValue},
    QueryError, Result,
};
use arrow::{
    array::ArrayData,
    array::*,
    buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::*,
    record_batch::RecordBatch,
};
use std::{any::Any, borrow::Cow, sync::Arc};

fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("admitted aggregate output: {message}"))
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or_else(|| invalid("size overflow"))
}

enum Value<'a> {
    Borrowed(&'a ScalarValue),
    Inline(ScalarValue),
    Owned(ReservedScalar),
}
impl Value<'_> {
    fn scalar(&self) -> &ScalarValue {
        match self {
            Self::Borrowed(v) => v,
            Self::Inline(v) => v,
            Self::Owned(v) => v.as_scalar(),
        }
    }
}

#[derive(Debug)]
struct ArrayOwner {
    _data_type: ReservedDataType,
    _metadata: MemoryReservation,
    _batch: Arc<MemoryReservation>,
}
struct BufferOwner {
    buffer: Buffer,
    _owner: Arc<ArrayOwner>,
}
impl AsRef<[u8]> for BufferOwner {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}
fn owned(buffer: Buffer, owner: &Arc<ArrayOwner>) -> Buffer {
    Buffer::from(bytes::Bytes::from_owner(BufferOwner {
        buffer,
        _owner: owner.clone(),
    }))
}

/// Buffers also retain this owner, so extracting ArrayData does not detach
/// payload/type admission. The wrapper retains metadata for bufferless NULL and
/// empty arrays. Arrow-created clone/slice headers retain Arrow's own contract.
#[derive(Debug)]
struct OwnedArray {
    inner: ArrayRef,
    owner: Arc<ArrayOwner>,
}
// SAFETY: Every representation/data/null/extent operation delegates to the same
// valid Arrow array. The added owner changes lifetime only, never interpretation.
unsafe impl Array for OwnedArray {
    fn as_any(&self) -> &dyn Any {
        self.inner.as_any()
    }
    fn to_data(&self) -> ArrayData {
        self.inner.to_data()
    }
    fn into_data(self) -> ArrayData {
        self.inner.to_data()
    }
    fn data_type(&self) -> &DataType {
        self.inner.data_type()
    }
    fn slice(&self, offset: usize, length: usize) -> ArrayRef {
        Arc::new(Self {
            inner: self.inner.slice(offset, length),
            owner: self.owner.clone(),
        })
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    fn offset(&self) -> usize {
        self.inner.offset()
    }
    fn nulls(&self) -> Option<&NullBuffer> {
        self.inner.nulls()
    }
    fn logical_nulls(&self) -> Option<NullBuffer> {
        self.inner.logical_nulls()
    }
    fn is_nullable(&self) -> bool {
        self.inner.is_nullable()
    }
    fn get_buffer_memory_size(&self) -> usize {
        self.inner.get_buffer_memory_size()
    }
    fn get_array_memory_size(&self) -> usize {
        self.inner.get_array_memory_size() + std::mem::size_of::<Self>()
    }
}

fn validity(
    pool: &MemoryPool,
    values: &[&ScalarValue],
    owner: &Arc<ArrayOwner>,
) -> Result<Option<NullBuffer>> {
    // Actual values prove the all-valid representation; schema nullability does not.
    if values
        .iter()
        .all(|value| !matches!(value, ScalarValue::Null))
    {
        return Ok(None);
    }
    let len = values.len();
    let bytes = len.div_ceil(8);
    let mut buffer = ReservedBufferBuilder::<u8>::with_capacity(pool, bytes)?;
    buffer.extend_reserved(
        bytes,
        (0..bytes).map(|i| {
            let mut byte = 0;
            for bit in 0..8.min(len - i * 8) {
                if !matches!(values[i * 8 + bit], ScalarValue::Null) {
                    byte |= 1 << bit;
                }
            }
            byte
        }),
    )?;
    Ok(Some(NullBuffer::new(BooleanBuffer::new(
        owned(buffer.finish(), owner),
        0,
        len,
    ))))
}

fn integer(value: &ScalarValue, ty: &DataType) -> Result<i128> {
    Ok(match value {
        ScalarValue::Null => 0,
        ScalarValue::Int8(v) => *v as i128,
        ScalarValue::Int16(v) => *v as i128,
        ScalarValue::Int32(v) => *v as i128,
        ScalarValue::Int64(v) => *v as i128,
        ScalarValue::UInt8(v) => *v as i128,
        ScalarValue::UInt16(v) => *v as i128,
        ScalarValue::UInt32(v) => *v as i128,
        ScalarValue::UInt64(v) => *v as i128,
        ScalarValue::Decimal128(v) if v.scale() == 0 && ty == &DataType::UInt64 => v.mantissa(),
        ScalarValue::Date32(v) if ty == &DataType::Date32 => *v as i128,
        ScalarValue::Date64(v) if ty == &DataType::Date64 => *v as i128,
        ScalarValue::Timestamp(v) if ty == &DataType::Timestamp(v.unit, v.timezone.clone()) => {
            v.ticks as i128
        }
        _ => return Err(invalid("integer/temporal value differs from output type")),
    })
}

fn borrowed_utf8_array(
    pool: &MemoryPool,
    values: &[Option<&str>],
    batch: &Arc<MemoryReservation>,
) -> Result<ArrayRef> {
    let total = values
        .iter()
        .try_fold(0, |n, v| add(n, v.map_or(0, str::len)))?;
    i32::try_from(total).map_err(|_| invalid("UTF8 output offset overflow"))?;
    let owner = Arc::new(ArrayOwner {
        _metadata: pool.allocate(
            512 + std::mem::size_of::<ArrayOwner>() + std::mem::size_of::<OwnedArray>(),
        )?,
        _data_type: ReservedDataType::try_copy(pool, &DataType::Utf8)?,
        _batch: batch.clone(),
    });
    let len = values.len();
    let mut data = ReservedBufferBuilder::<u8>::with_capacity(pool, total)?;
    let mut offsets = ReservedBufferBuilder::<i32>::with_capacity(pool, add(len, 1)?)?;
    let mut validity = if values.iter().any(Option::is_none) {
        let mut bits = ReservedBufferBuilder::<u8>::with_capacity(pool, len.div_ceil(8))?;
        bits.extend_reserved(len.div_ceil(8), std::iter::repeat_n(0, len.div_ceil(8)))?;
        Some(bits)
    } else {
        None
    };
    offsets.extend_reserved(1, std::iter::once(0))?;
    for (index, value) in values.iter().enumerate() {
        if let Some(value) = value {
            if let Some(bits) = &mut validity {
                bits.as_mut_slice()[index / 8] |= 1 << (index % 8);
            }
            data.extend_from_slice(value.as_bytes())?;
        }
        offsets.extend_reserved(1, std::iter::once(data.as_slice().len() as i32))?;
    }
    let inner = Arc::new(StringArray::try_new(
        OffsetBuffer::new(ScalarBuffer::new(
            owned(offsets.finish(), &owner),
            0,
            len + 1,
        )),
        owned(data.finish(), &owner),
        validity
            .map(|bits| NullBuffer::new(BooleanBuffer::new(owned(bits.finish(), &owner), 0, len))),
    )?);
    Ok(Arc::new(OwnedArray { inner, owner }))
}

fn array(
    pool: &MemoryPool,
    values: &[&ScalarValue],
    ty: &DataType,
    batch: &Arc<MemoryReservation>,
    depth: usize,
) -> Result<ArrayRef> {
    if depth > 64 {
        return Err(invalid("list output exceeds nesting limit"));
    }
    let owner = Arc::new(ArrayOwner {
        _metadata: pool.allocate(
            512 + std::mem::size_of::<ArrayOwner>() + std::mem::size_of::<OwnedArray>(),
        )?,
        _data_type: ReservedDataType::try_copy(pool, ty)?,
        _batch: batch.clone(),
    });
    let len = values.len();
    let nulls = if ty == &DataType::Null {
        None // NullArray has no validity buffer; its branch still validates every value.
    } else {
        validity(pool, values, &owner)?
    };
    macro_rules! primitive {
        ($t:ty,$native:ty,$convert:expr) => {{
            let mut data = ReservedBufferBuilder::<$native>::with_capacity(pool, len)?;
            data.try_extend_reserved(len, values.iter().map(|v| ($convert)(*v)))?;
            Arc::new(
                PrimitiveArray::<$t>::new(
                    ScalarBuffer::new(owned(data.finish(), &owner), 0, len),
                    nulls,
                )
                .with_data_type(ty.clone()),
            ) as ArrayRef
        }};
    }
    macro_rules! int {
        ($t:ty,$native:ty) => {
            primitive!($t, $native, |v| <$native>::try_from(integer(v, ty)?)
                .map_err(|_| invalid("integer output overflow")))
        };
    }
    let inner: ArrayRef = match ty {
        DataType::Null => {
            if values.iter().any(|v| !matches!(v, ScalarValue::Null)) {
                return Err(invalid("non-NULL value for NULL output"));
            }
            Arc::new(NullArray::new(len))
        }
        DataType::Boolean => {
            let bytes = len.div_ceil(8);
            let mut data = ReservedBufferBuilder::<u8>::with_capacity(pool, bytes)?;
            data.extend_reserved(bytes, std::iter::repeat_n(0, bytes))?;
            for (i, v) in values.iter().enumerate() {
                match v {
                    ScalarValue::Boolean(true) => data.as_mut_slice()[i / 8] |= 1 << (i % 8),
                    ScalarValue::Boolean(false) | ScalarValue::Null => (),
                    _ => return Err(invalid("non-Boolean output value")),
                }
            }
            Arc::new(BooleanArray::new(
                BooleanBuffer::new(owned(data.finish(), &owner), 0, len),
                nulls,
            ))
        }
        DataType::Int8 => int!(Int8Type, i8),
        DataType::Int16 => int!(Int16Type, i16),
        DataType::Int32 => int!(Int32Type, i32),
        DataType::Int64 => int!(Int64Type, i64),
        DataType::UInt8 => int!(UInt8Type, u8),
        DataType::UInt16 => int!(UInt16Type, u16),
        DataType::UInt32 => int!(UInt32Type, u32),
        DataType::UInt64 => int!(UInt64Type, u64),
        DataType::Date32 => int!(Date32Type, i32),
        DataType::Date64 => int!(Date64Type, i64),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => int!(TimestampSecondType, i64),
            TimeUnit::Millisecond => int!(TimestampMillisecondType, i64),
            TimeUnit::Microsecond => int!(TimestampMicrosecondType, i64),
            TimeUnit::Nanosecond => int!(TimestampNanosecondType, i64),
        },
        DataType::Float32 => primitive!(Float32Type, f32, |v: &ScalarValue| match v {
            ScalarValue::Float32(v) => Ok(v.into_inner()),
            ScalarValue::Null => Ok(0.0),
            _ => Err(invalid("non-Float32 output")),
        }),
        DataType::Float64 => primitive!(Float64Type, f64, |v: &ScalarValue| match v {
            ScalarValue::Float64(v) => Ok(v.into_inner()),
            ScalarValue::Null => Ok(0.0),
            _ => Err(invalid("non-Float64 output")),
        }),
        DataType::Decimal128(precision, scale) => {
            primitive!(Decimal128Type, i128, |v: &ScalarValue| match v {
                ScalarValue::Decimal128(v) => {
                    let coefficient = v.rescale(*scale)?;
                    DecimalValue::validate_precision(coefficient, *precision)?;
                    Ok(coefficient)
                }
                ScalarValue::Null => Ok(0),
                _ => Err(invalid("non-decimal output")),
            })
        }
        DataType::Utf8 => {
            let total = values.iter().try_fold(0, |size, v| match v {
                ScalarValue::Utf8(v) => add(size, v.len()),
                ScalarValue::Null => Ok(size),
                _ => Err(invalid("non-string output")),
            })?;
            i32::try_from(total).map_err(|_| invalid("UTF8 output offset overflow"))?;
            let mut data = ReservedBufferBuilder::<u8>::with_capacity(pool, total)?;
            let mut offsets = ReservedBufferBuilder::<i32>::with_capacity(pool, add(len, 1)?)?;
            offsets.extend_reserved(1, std::iter::once(0))?;
            for v in values {
                if let ScalarValue::Utf8(v) = v {
                    data.extend_from_slice(v.as_bytes())?;
                }
                offsets.extend_reserved(1, std::iter::once(data.as_slice().len() as i32))?;
            }
            Arc::new(StringArray::try_new(
                OffsetBuffer::new(ScalarBuffer::new(
                    owned(offsets.finish(), &owner),
                    0,
                    len + 1,
                )),
                owned(data.finish(), &owner),
                nulls,
            )?)
        }
        DataType::List(field) => {
            let total = values.iter().try_fold(0, |size, v| match v {
                ScalarValue::List(v, t) if t.as_ref() == field.data_type() => add(size, v.len()),
                ScalarValue::Null => Ok(size),
                _ => Err(invalid("list element type mismatch")),
            })?;
            i32::try_from(total).map_err(|_| invalid("list output offset overflow"))?;
            let mut children = ReservedVec::<&ScalarValue>::with_capacity(pool, total)?;
            let mut offsets = ReservedBufferBuilder::<i32>::with_capacity(pool, add(len, 1)?)?;
            offsets.extend_reserved(1, std::iter::once(0))?;
            for value in values {
                if let ScalarValue::List(v, _) = value {
                    if !field.is_nullable() && v.iter().any(|v| matches!(v, ScalarValue::Null)) {
                        return Err(invalid("NULL in non-nullable output list"));
                    }
                    children.extend_reserved(v.len(), v.iter())?;
                }
                offsets.extend_reserved(1, std::iter::once(children.as_slice().len() as i32))?;
            }
            let child = array(
                pool,
                children.as_slice(),
                field.data_type(),
                batch,
                depth + 1,
            )?;
            Arc::new(ListArray::try_new(
                field.clone(),
                OffsetBuffer::new(ScalarBuffer::new(
                    owned(offsets.finish(), &owner),
                    0,
                    len + 1,
                )),
                child,
                nulls,
            )?)
        }
        _ => return Err(invalid("unsupported normalized output type")),
    };
    Ok(Arc::new(OwnedArray { inner, owner }))
}

impl GroupRows {
    /// Caller chooses a bounded output range and admits its result collection.
    /// New buffers keep payload, array/type and batch metadata leases alive after
    /// this group store and the query's bound layout have been dropped.
    pub(super) fn build_output_range(
        &self,
        schema: &SchemaRef,
        start: usize,
        end: usize,
    ) -> Result<RecordBatch> {
        if start > end || end > self.len() || schema.fields().len() != self.output_columns() {
            return Err(invalid("output range or arity mismatch"));
        }
        self.validate_output_schema(schema.as_ref())?;
        let pool = self.output_pool();
        let columns = schema.fields().len();
        let rows = end - start;
        let mut metadata = add(
            512,
            columns
                .checked_mul(std::mem::size_of::<ArrayRef>())
                .ok_or_else(|| invalid("column metadata overflow"))?,
        )?;
        for field in schema.fields() {
            metadata = add(metadata, field.size())?;
        }
        for (key, value) in &schema.metadata {
            metadata = add(metadata, add(key.len(), value.len())?)?;
        }
        let batch_owner = Arc::new(pool.allocate(metadata)?);
        let mut arrays = Vec::new();
        arrays
            .try_reserve_exact(columns)
            .map_err(|_| invalid("column allocation refused"))?;
        if arrays.capacity() != columns {
            return Err(invalid("column capacity differs from admission"));
        }
        for (column, field) in schema.fields().iter().enumerate() {
            if column < self.key_layout().len() && field.data_type() == &DataType::Utf8 {
                // Borrow canonical key strings, then copy once into admitted
                // Arrow payload. Avoid an owned scalar allocation for every key.
                let mut values = ReservedVec::with_capacity(pool, rows)?;
                for row in start..end {
                    let key = self.key(row)?;
                    let value = self.key_layout().utf8_field(key.bytes(), column)?;
                    values.extend_reserved(1, std::iter::once(value))?;
                }
                arrays.push(borrowed_utf8_array(pool, values.as_slice(), &batch_owner)?);
                continue;
            }
            let mut values = ReservedVec::with_capacity(pool, rows)?;
            for row in start..end {
                let value = if column < self.key_layout().len() {
                    if field.data_type() != self.key_layout().data_type(column)? {
                        return Err(invalid("output group type mismatch"));
                    }
                    let key = self.key(row)?;
                    let bytes = self.key_layout().field_payload(key.bytes(), column)?;
                    if let Some((value, _)) =
                        ReservedScalar::try_decode_inline(field.data_type(), bytes)?
                    {
                        Value::Inline(value)
                    } else {
                        Value::Owned(ReservedScalar::try_decode(pool, field.data_type(), bytes)?.0)
                    }
                } else {
                    match self.value(row, column - self.key_layout().len())? {
                        Cow::Borrowed(v) => Value::Borrowed(v),
                        Cow::Owned(v) => Value::Inline(v),
                    }
                };
                values.extend_reserved(1, std::iter::once(value))?;
            }
            let mut refs = ReservedVec::with_capacity(pool, rows)?;
            refs.extend_reserved(rows, values.as_slice().iter().map(Value::scalar))?;
            arrays.push(array(
                pool,
                refs.as_slice(),
                field.data_type(),
                &batch_owner,
                0,
            )?);
        }
        Ok(RecordBatch::try_new(schema.clone(), arrays)?)
    }
}

#[cfg(test)]
mod tests {
    use super::super::group_rows::GroupLayout;
    use super::*;
    use crate::planner::AggregateFunction;

    #[test]
    fn borrowed_strings_preserve_values_ownership_and_denial_cleanup() {
        let pool = MemoryPool::new_named("borrowed key output", 65536);
        let batch_owner = Arc::new(pool.allocate(512).unwrap());
        let source = "日本語\0é".repeat(64);
        let values = [Some(""), None, Some(source.as_str())];
        let baseline = pool.used();
        let (mut denied, mut completed) = (0, 0);
        for available in (0..=8192).step_by(256) {
            let hold = pool.allocate(pool.available() - available).unwrap();
            let before = pool.used();
            match borrowed_utf8_array(&pool, &values, &batch_owner) {
                Ok(array) => {
                    completed += 1;
                    drop(array);
                }
                Err(error) => {
                    assert!(error.is_memory_limit(), "{error}");
                    denied += 1;
                }
            }
            assert_eq!(pool.used(), before);
            drop(hold);
            assert_eq!(pool.used(), baseline);
        }
        assert!(denied > 0 && completed > 0);
        let array = borrowed_utf8_array(&pool, &values, &batch_owner).unwrap();
        let data = array.to_data();
        let slice = array.slice(1, 2);
        drop((source, array, batch_owner));
        assert!(pool.used() > 0);
        let restored = StringArray::from(data);
        assert_eq!(restored.value(0), "");
        assert!(!restored.is_null(0));
        assert!(restored.is_null(1));
        assert_eq!(restored.value(2), "日本語\0é".repeat(64));
        assert!(slice.is_null(0));
        drop((restored, slice));
        assert_eq!(pool.used(), 0);
    }

    fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
        let fields: Vec<_> = columns
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }
    #[test]
    fn output_survives_state_drop_and_arrow_data_clones_with_exact_values() {
        let pool = MemoryPool::new_named("group output", 262144);
        let exact = (1i128 << 80) + 17;
        let input = batch(vec![
            Arc::new(StringArray::from(vec![Some("abc"), Some("abc"), None])),
            Arc::new(
                Decimal128Array::from(vec![Some(exact), Some(exact * 3), None])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(7), None]),
                Some(vec![Some(99)]),
                Some(vec![]),
            ])),
            Arc::new(
                TimestampNanosecondArray::from(vec![Some(-17), Some(8), None]).with_timezone("UTC"),
            ),
        ]);
        let states: Vec<_> = [
            AggregateFunction::Sum,
            AggregateFunction::AnyValue,
            AggregateFunction::Min,
        ]
        .iter()
        .zip(&input.columns()[1..])
        .map(|(f, a)| (*f, a.data_type().clone(), false))
        .collect();
        let layout = GroupLayout::bind(&pool, &[DataType::Utf8], &states)
            .unwrap()
            .unwrap();
        let mut groups = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        groups
            .process_evaluated_from(&input, 1, 0, &mut key, &mut row)
            .unwrap();
        let result = groups
            .build_output_range(&input.schema(), 0, groups.len())
            .unwrap();
        drop((groups, key, row, layout));
        let slice = result.slice(0, 1);
        let data = result.column(1).to_data();
        let list = result
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(list.value_offsets(), &[0, 2, 2]);
        let child = list.values().as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(child.value(0), 7);
        assert!(child.is_null(1));
        let sums = result
            .column(1)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(sums.value(0), exact * 4);
        assert!(sums.is_null(1));
        let times = result
            .column(3)
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap();
        assert_eq!(times.value(0), -17);
        assert!(times.is_null(1));
        assert_eq!(times.timezone(), Some("UTC"));
        drop(result);
        assert!(pool.used() > 0);
        assert_eq!(
            slice
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "abc"
        );
        drop(slice);
        assert!(pool.used() > 0);
        let sums = make_array(data);
        assert_eq!(
            sums.as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value(0),
            exact * 4
        );
        drop(sums);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn output_denial_and_invalid_schema_preserve_groups_and_release_partial_buffers() {
        let pool = MemoryPool::new_named("output rollback", 65536);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let mut groups = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        let input = batch(vec![
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(Int64Array::from(vec![1, 1])),
        ]);
        groups
            .process_evaluated_from(&input, 1, 0, &mut key, &mut row)
            .unwrap();
        let baseline = pool.used();
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(groups
            .build_output_range(&input.schema(), 0, 1)
            .unwrap_err()
            .is_memory_limit());
        drop(pressure);
        assert_eq!(pool.used(), baseline);
        let wrong = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("count", DataType::Utf8, true),
        ]));
        assert!(groups.build_output_range(&wrong, 0, 1).is_err());
        assert_eq!(pool.used(), baseline);
        assert_eq!(*groups.value(0, 0).unwrap(), ScalarValue::Int64(2));
        let result = groups.build_output_range(&input.schema(), 0, 1).unwrap();
        assert_eq!(
            result
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            2
        );
        drop(result);
        assert_eq!(pool.used(), baseline);
        drop((groups, key, row, layout));
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn unsigned_sum_preserves_values_above_float_precision_and_checks_output_overflow() {
        for (values, expected) in [
            (vec![(1u64 << 53) + 1, 1], Some((1u64 << 53) + 2)),
            (vec![u64::MAX, 0], Some(u64::MAX)),
            (vec![u64::MAX, 1], None),
        ] {
            let pool = MemoryPool::new_named("unsigned output", 65536);
            let input = batch(vec![
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(UInt64Array::from(values)),
            ]);
            let layout = GroupLayout::bind(
                &pool,
                &[DataType::Int64],
                &[(AggregateFunction::Sum, DataType::UInt64, false)],
            )
            .unwrap()
            .unwrap();
            let mut groups = GroupRows::new(layout.clone()).unwrap();
            let mut key = layout.key_workspace().unwrap();
            let mut row = layout.row_workspace().unwrap();
            groups
                .process_evaluated_from(&input, 1, 0, &mut key, &mut row)
                .unwrap();
            let baseline = pool.used();
            let output = groups.build_output_range(&input.schema(), 0, 1);
            if let Some(expected) = expected {
                let output = output.unwrap();
                assert_eq!(
                    output
                        .column(1)
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(0),
                    expected
                );
            } else {
                assert!(output.is_err());
            }
            assert_eq!(pool.used(), baseline);
            drop((groups, key, row, layout));
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn controller_emits_empty_global_and_exact_unsigned_spill_results() {
        use super::super::{ingestion_controller::IngestionController, spill_files::RunDirectory};
        let parent = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("controller Arrow output", 262144);
        let layout = GroupLayout::bind(
            &pool,
            &[],
            &[
                (AggregateFunction::Count, DataType::Int64, false),
                (AggregateFunction::Avg, DataType::Float64, false),
            ],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let controller =
            IngestionController::new(layout.clone(), directory.clone(), 2, 1024, 1).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("count", DataType::Int64, false),
            Field::new("avg", DataType::Float64, true),
        ]));
        let mut emitted = 0;
        controller
            .finish(|groups| {
                let result = groups.build_output_range(&schema, 0, groups.len())?;
                assert_eq!(result.num_rows(), 1);
                emitted += 1;
                assert_eq!(
                    result
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    0
                );
                assert!(result.column(1).is_null(0));
                Ok(())
            })
            .unwrap();
        assert_eq!(emitted, 1);
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Sum, DataType::UInt64, false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let mut controller =
            IngestionController::new(layout.clone(), directory.clone(), 2, 1024, 1).unwrap();
        let input = batch(vec![
            Arc::new(Int64Array::from(vec![1, 2, 1, 2])),
            Arc::new(UInt64Array::from(vec![(1u64 << 53) + 1, u64::MAX, 1, 0])),
        ]);
        controller.ingest(&input, 1).unwrap();
        let mut seen = [false; 2];
        let stats = controller
            .finish(|groups| {
                let result = groups.build_output_range(&input.schema(), 0, groups.len())?;
                let keys = result
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let sums = result
                    .column(1)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap();
                for row in 0..result.num_rows() {
                    let i = (keys.value(row) - 1) as usize;
                    assert!(!seen[i]);
                    seen[i] = true;
                    assert_eq!(sums.value(row), [(1u64 << 53) + 2, u64::MAX][i]);
                }
                Ok(())
            })
            .unwrap();
        assert!(stats.flushes > 0);
        assert_eq!(seen, [true, true]);
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn null_boolean_and_float_outputs_preserve_logical_nulls_and_selected_bits() {
        let pool = MemoryPool::new_named("output NULL and bits", 65536);
        let input = batch(vec![
            Arc::new(NullArray::new(2)),
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(Float64Array::from(vec![-0.0, 0.0])),
        ]);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Null],
            &[
                (AggregateFunction::BoolAnd, DataType::Boolean, false),
                (AggregateFunction::Min, DataType::Float64, false),
            ],
        )
        .unwrap()
        .unwrap();
        let mut groups = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        groups
            .process_evaluated_from(&input, 1, 0, &mut key, &mut row)
            .unwrap();
        let result = groups.build_output_range(&input.schema(), 0, 1).unwrap();
        drop((groups, key, row, layout));
        assert_eq!(result.column(0).logical_null_count(), 1);
        assert!(result
            .column(1)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap()
            .value(0));
        assert_eq!(
            result
                .column(2)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            (-0.0f64).to_bits()
        );
        let null = result.column(0).slice(0, 1);
        drop(result);
        assert!(pool.used() > 0);
        drop(null);
        assert_eq!(pool.used(), 0);
    }
}

#[cfg(test)]
mod optional_validity_regressions {
    use super::*;
    // Scratch-only tests for admitted_output.rs. Install before candidate code to
    // reproduce the first test's admission failure without changing production paths.
    #[test]
    fn actual_all_valid_output_needs_no_bitmap_admission() {
        let pool = MemoryPool::new_named("all-valid output", 65536);
        let batch = Arc::new(pool.allocate(512).unwrap());
        let owner = Arc::new(ArrayOwner {
            _metadata: pool.allocate(512).unwrap(),
            _data_type: ReservedDataType::try_copy(&pool, &DataType::Int64).unwrap(),
            _batch: batch.clone(),
        });
        let a = ScalarValue::Int64(-7);
        let b = ScalarValue::Int64(i64::MAX);
        let null = ScalarValue::Null;
        let pressure = pool.allocate(pool.available()).unwrap();
        let occupied = pool.used();
        assert!(
            validity(&pool, &[&a, &b], &owner).is_ok(),
            "actual all-valid values need no validity allocation"
        );
        assert_eq!(pool.used(), occupied);
        assert!(validity(&pool, &[], &owner).is_ok());
        assert_eq!(pool.used(), occupied);
        assert!(
            validity(&pool, &[&a, &null], &owner)
                .unwrap_err()
                .is_memory_limit(),
            "NULL input must refuse instead of silently losing its bitmap"
        );
        assert_eq!(pool.used(), occupied);
        drop((pressure, owner, batch));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn optional_validity_preserves_values_nested_nulls_and_extracted_ownership() {
        let pool = MemoryPool::new_named("optional validity semantics", 131072);
        let batch = Arc::new(pool.allocate(512).unwrap());
        let values = [ScalarValue::Int64(-7), ScalarValue::Int64(i64::MAX)];
        let refs = values.iter().collect::<Vec<_>>();
        let output = array(&pool, &refs, &DataType::Int64, &batch, 0).unwrap();
        let slice = output.slice(1, 1);
        let data = output.to_data();
        drop(output);
        assert_eq!(
            slice
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            i64::MAX
        );
        assert_eq!(slice.null_count(), 0);
        let restored = make_array(data);
        let ints = restored.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(ints.values().as_ref(), &[-7, i64::MAX]);
        let null = ScalarValue::Null;
        let yes = ScalarValue::Boolean(true);
        let boolean = array(&pool, &[&yes, &null, &yes], &DataType::Boolean, &batch, 0).unwrap();
        let flags = boolean.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert!(flags.value(0) && flags.is_null(1) && flags.value(2));
        let strings = borrowed_utf8_array(&pool, &[Some("é"), Some("")], &batch).unwrap();
        let nullable = borrowed_utf8_array(&pool, &[None, Some("é")], &batch).unwrap();
        assert_eq!(
            strings
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "é"
        );
        assert_eq!(strings.null_count(), 0);
        assert!(nullable.is_null(0));
        assert_eq!(
            nullable
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(1),
            "é"
        );
        let list = ScalarValue::List(
            vec![ScalarValue::Int64(9), ScalarValue::Null],
            Box::new(DataType::Int64),
        );
        let empty = ScalarValue::List(vec![], Box::new(DataType::Int64));
        let ty = DataType::List(Arc::new(Field::new("item", DataType::Int64, true)));
        let nested = array(&pool, &[&list, &empty, &null], &ty, &batch, 0).unwrap();
        let lists = nested.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(lists.value_offsets(), &[0, 2, 2, 2]);
        assert!(!lists.is_null(0) && !lists.is_null(1) && lists.is_null(2));
        let children = lists
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(children.value(0), 9);
        assert!(children.is_null(1));
        let nulls = array(&pool, &[&null, &null], &DataType::Null, &batch, 0).unwrap();
        assert_eq!(nulls.logical_null_count(), 2);
        drop(batch);
        assert!(
            pool.used() > 0,
            "extracted buffers and wrappers retain admitted owners"
        );
        drop((slice, restored, boolean, strings, nullable, nested, nulls));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn empty_all_valid_payload_keeps_extracted_buffer_owner() {
        let pool = MemoryPool::new_named("empty extracted output", 65536);
        let batch = Arc::new(pool.allocate(512).unwrap());
        let output = array(&pool, &[], &DataType::Int64, &batch, 0).unwrap();
        assert_eq!(output.len(), 0);
        let data = output.to_data();
        drop((output, batch));
        assert!(
            pool.used() > 0,
            "zero-length payload still retains output admission"
        );
        let restored = make_array(data);
        assert_eq!(restored.len(), 0);
        assert_eq!(restored.null_count(), 0);
        drop(restored);
        assert_eq!(pool.used(), 0);
    }
}
