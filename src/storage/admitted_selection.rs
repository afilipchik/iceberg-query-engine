//! Nullable, admission-owned row selection for flat physical outputs.
use crate::execution::{reserved_vec::ReservedVec, MemoryPool, ReservedBufferBuilder};
use crate::{QueryError, Result};
use arrow::record_batch::RecordBatch;
use arrow::{
    array::*,
    buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::*,
};
use std::sync::Arc;

mod range;
pub(crate) use range::copy_range;

pub(crate) type Row = Option<(usize, usize)>;
fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("admitted selection: {message}"))
}
pub(crate) fn supported(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Date32
            | DataType::Date64
            | DataType::Decimal128(_, _)
            | DataType::Timestamp(_, _)
            | DataType::Utf8
    )
}
// Logical NULLs include NULL dictionary keys and NULL dictionary values. Resolve
// against the actual array, never an estimate or dictionary identity heuristic.
fn cell(mut array: &dyn Array, mut row: usize) -> Result<Option<(&dyn Array, usize)>> {
    loop {
        if row >= array.len() {
            return Err(invalid("row outside array domain"));
        }
        if array.is_null(row) {
            return Ok(None);
        }
        macro_rules! dictionary {
            ($t:ty) => {{
                let d = array
                    .as_any()
                    .downcast_ref::<DictionaryArray<$t>>()
                    .ok_or_else(|| invalid("dictionary representation differs"))?;
                row = d
                    .key(row)
                    .ok_or_else(|| invalid("invalid dictionary key"))?;
                array = d.values().as_ref();
            }};
        }
        match array.data_type() {
            DataType::Dictionary(key, _) => match key.as_ref() {
                DataType::Int8 => dictionary!(Int8Type),
                DataType::Int16 => dictionary!(Int16Type),
                DataType::Int32 => dictionary!(Int32Type),
                DataType::Int64 => dictionary!(Int64Type),
                DataType::UInt8 => dictionary!(UInt8Type),
                DataType::UInt16 => dictionary!(UInt16Type),
                DataType::UInt32 => dictionary!(UInt32Type),
                DataType::UInt64 => dictionary!(UInt64Type),
                _ => return Err(invalid("unsupported dictionary key type")),
            },
            _ => return Ok(Some((array, row))),
        }
    }
}
// Bind ordinary arrays once per source column. Encoded values retain checked
// dictionary traversal; no guessed key domain or unchecked row addressing.
enum BoundColumn<'a, A> {
    Plain(&'a A),
    Encoded(&'a dyn Array),
}
fn bind<'a, A: Array + 'static>(
    sources: &'a [RecordBatch],
    column: usize,
    pool: &MemoryPool,
) -> Result<ReservedVec<BoundColumn<'a, A>>> {
    let mut bound = ReservedVec::with_capacity(pool, sources.len())?;
    bound.try_extend_reserved(
        sources.len(),
        sources.iter().map(|batch| {
            let array = batch
                .columns()
                .get(column)
                .ok_or_else(|| invalid("column outside domain"))?;
            if let Some(plain) = array.as_any().downcast_ref::<A>() {
                Ok(BoundColumn::Plain(plain))
            } else if matches!(array.data_type(), DataType::Dictionary(_, _)) {
                Ok(BoundColumn::Encoded(array.as_ref()))
            } else {
                Err(invalid("physical representation differs"))
            }
        }),
    )?;
    Ok(bound)
}
fn selected_bound<'a, A: Array + 'static>(
    bound: &[BoundColumn<'a, A>],
    row: Row,
) -> Result<Option<(&'a A, usize)>> {
    let Some((batch, row)) = row else {
        return Ok(None);
    };
    match bound
        .get(batch)
        .ok_or_else(|| invalid("batch outside domain"))?
    {
        BoundColumn::Plain(array) => {
            if row >= array.len() {
                return Err(invalid("row outside array domain"));
            }
            Ok((!array.is_null(row)).then_some((*array, row)))
        }
        BoundColumn::Encoded(array) => {
            let Some((array, row)) = cell(*array, row)? else {
                return Ok(None);
            };
            Ok(Some((
                array
                    .as_any()
                    .downcast_ref::<A>()
                    .ok_or_else(|| invalid("decoded representation differs"))?,
                row,
            )))
        }
    }
}
fn zero_bits(rows: usize, pool: &MemoryPool) -> Result<ReservedBufferBuilder<u8>> {
    let mut bits = ReservedBufferBuilder::with_capacity(pool, rows.div_ceil(8))?;
    bits.extend_reserved(rows.div_ceil(8), std::iter::repeat(0))?;
    Ok(bits)
}
fn primitive<T: ArrowPrimitiveType>(
    sources: &[RecordBatch],
    column: usize,
    rows: &[Row],
    dt: &DataType,
    pool: &MemoryPool,
) -> Result<ArrayRef> {
    let bound = bind::<PrimitiveArray<T>>(sources, column, pool)?;
    let mut bits = zero_bits(rows.len(), pool)?;
    let mut values = ReservedBufferBuilder::<T::Native>::with_capacity(pool, rows.len())?;
    values.try_extend_reserved(
        rows.len(),
        rows.iter().enumerate().map(|(i, row)| {
            let Some((array, index)) = selected_bound(bound.as_slice(), *row)? else {
                return Ok(T::Native::default());
            };
            bits.as_mut_slice()[i / 8] |= 1 << (i % 8);
            Ok(array.value(index))
        }),
    )?;
    let nulls = NullBuffer::new(BooleanBuffer::new(bits.finish(), 0, rows.len()));
    Ok(Arc::new(
        PrimitiveArray::<T>::new(
            ScalarBuffer::new(values.finish(), 0, rows.len()),
            Some(nulls),
        )
        .with_data_type(dt.clone()),
    ))
}
fn column(
    sources: &[RecordBatch],
    column: usize,
    rows: &[Row],
    dt: &DataType,
    pool: &MemoryPool,
) -> Result<ArrayRef> {
    for batch in sources {
        let array = batch
            .columns()
            .get(column)
            .ok_or_else(|| invalid("column outside domain"))?;
        let mut actual = array.data_type();
        while let DataType::Dictionary(_, value) = actual {
            actual = value.as_ref();
        }
        if actual != dt {
            return Err(invalid("source logical type differs"));
        }
    }
    macro_rules! p {
        ($t:ty) => {
            primitive::<$t>(sources, column, rows, dt, pool)
        };
    }
    match dt {
        DataType::Int8 => p!(Int8Type),
        DataType::Int16 => p!(Int16Type),
        DataType::Int32 => p!(Int32Type),
        DataType::Int64 => p!(Int64Type),
        DataType::UInt8 => p!(UInt8Type),
        DataType::UInt16 => p!(UInt16Type),
        DataType::UInt32 => p!(UInt32Type),
        DataType::UInt64 => p!(UInt64Type),
        DataType::Float32 => p!(Float32Type),
        DataType::Float64 => p!(Float64Type),
        DataType::Date32 => p!(Date32Type),
        DataType::Date64 => p!(Date64Type),
        DataType::Decimal128(_, _) => p!(Decimal128Type),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => p!(TimestampSecondType),
            TimeUnit::Millisecond => p!(TimestampMillisecondType),
            TimeUnit::Microsecond => p!(TimestampMicrosecondType),
            TimeUnit::Nanosecond => p!(TimestampNanosecondType),
        },
        DataType::Null => Ok(Arc::new(NullArray::new(rows.len()))),
        DataType::Boolean => {
            let bound = bind::<BooleanArray>(sources, column, pool)?;
            let mut validity = zero_bits(rows.len(), pool)?;
            let mut bits = zero_bits(rows.len(), pool)?;
            for (i, row) in rows.iter().enumerate() {
                if let Some((array, index)) = selected_bound(bound.as_slice(), *row)? {
                    validity.as_mut_slice()[i / 8] |= 1 << (i % 8);
                    if array.value(index) {
                        bits.as_mut_slice()[i / 8] |= 1 << (i % 8);
                    }
                }
            }
            let nulls = NullBuffer::new(BooleanBuffer::new(validity.finish(), 0, rows.len()));
            Ok(Arc::new(BooleanArray::new(
                BooleanBuffer::new(bits.finish(), 0, rows.len()),
                Some(nulls),
            )))
        }
        DataType::Utf8 => {
            let bound = bind::<StringArray>(sources, column, pool)?;
            let mut validity = zero_bits(rows.len(), pool)?;
            let value = |row| -> Result<Option<&str>> {
                Ok(selected_bound(bound.as_slice(), row)?.map(|(array, index)| array.value(index)))
            };
            let bytes = rows.iter().enumerate().try_fold(0usize, |n, (i, row)| {
                let text = value(*row)?;
                if text.is_some() {
                    validity.as_mut_slice()[i / 8] |= 1 << (i % 8);
                }
                n.checked_add(text.map_or(0, str::len))
                    .filter(|n| *n <= i32::MAX as usize)
                    .ok_or_else(|| invalid("UTF8 extent overflow"))
            })?;
            let nulls = NullBuffer::new(BooleanBuffer::new(validity.finish(), 0, rows.len()));
            let mut offsets = ReservedBufferBuilder::<i32>::with_capacity(
                pool,
                rows.len()
                    .checked_add(1)
                    .ok_or_else(|| invalid("offset extent overflow"))?,
            )?;
            let mut values = ReservedBufferBuilder::<u8>::with_capacity(pool, bytes)?;
            offsets.extend_reserved(1, [0])?;
            for row in rows {
                let text = value(*row)?.unwrap_or("").as_bytes();
                values.extend_reserved(text.len(), text.iter().copied())?;
                offsets.extend_reserved(1, [values.as_slice().len() as i32])?;
            }
            Ok(Arc::new(StringArray::try_new(
                OffsetBuffer::new(ScalarBuffer::new(offsets.finish(), 0, rows.len() + 1)),
                values.finish(),
                Some(nulls),
            )?))
        }
        _ => Err(invalid("unsupported output representation")),
    }
}
/// Columns are mapped explicitly: build storage may already be pruned while
/// probe storage retains ON-only columns. All new buffers own their reservations.
pub(crate) fn gather(
    sources: &[RecordBatch],
    rows: &[Row],
    columns: &[usize],
    schema: SchemaRef,
    pool: &MemoryPool,
) -> Result<RecordBatch> {
    if columns.len() != schema.fields().len() {
        return Err(invalid("schema width differs"));
    }
    let mut arrays = ReservedVec::with_capacity(pool, columns.len())?;
    for (ordinal, field) in columns.iter().zip(schema.fields()) {
        arrays.extend_reserved(
            1,
            [column(sources, *ordinal, rows, field.data_type(), pool)?],
        )?;
    }
    crate::storage::admitted_batch::finish(schema, rows.len(), arrays, pool)
}

/// Decode a physical dictionary key with admitted IDs and values. Plain arrays
/// are borrowed; no Arrow cast allocation escapes the pool.
pub(crate) fn decode_key(array: ArrayRef, pool: &MemoryPool) -> Result<ArrayRef> {
    let mut dt = array.data_type().clone();
    if !matches!(dt, DataType::Dictionary(_, _)) {
        return Ok(array);
    }
    while let DataType::Dictionary(_, value) = dt {
        dt = *value;
    }
    let len = array.len();
    let batch = RecordBatch::try_from_iter([("key", array)])?;
    let mut rows = ReservedVec::with_capacity(pool, len)?;
    rows.extend_reserved(len, (0..len).map(|row| Some((0, row))))?;
    column(std::slice::from_ref(&batch), 0, rows.as_slice(), &dt, pool)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal_float_bits_and_dictionary_nulls_survive_nullable_selection() {
        let nan = f64::from_bits(0x7ff8_0000_0000_0123);
        let dictionary = DictionaryArray::<Int16Type>::try_new(
            Int16Array::from(vec![Some(2), None, Some(1), Some(0)]),
            Arc::new(Int64Array::from(vec![Some(-7), None, Some(99)])),
        )
        .unwrap();
        let source = RecordBatch::try_from_iter(vec![
            (
                "d",
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(i128::MAX / 100),
                        Some(-312345),
                        None,
                        Some(0),
                    ])
                    .with_precision_and_scale(38, 4)
                    .unwrap(),
                ) as ArrayRef,
            ),
            (
                "f",
                Arc::new(Float64Array::from(vec![
                    Some(nan),
                    Some(-0.0),
                    None,
                    Some(7.5),
                ])) as ArrayRef,
            ),
            ("k", Arc::new(dictionary) as ArrayRef),
        ])
        .unwrap()
        .slice(1, 3);
        let schema = Arc::new(Schema::new(vec![
            Field::new("d", DataType::Decimal128(38, 4), true),
            Field::new("f", DataType::Float64, true),
            Field::new("k", DataType::Int64, true),
        ]));
        let pool = MemoryPool::new(128 * 1024);
        let output = gather(
            &[source],
            &[Some((0, 2)), None, Some((0, 0)), Some((0, 1)), Some((0, 2))],
            &[0, 1, 2],
            schema,
            &pool,
        )
        .unwrap();
        let d = output
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(d.data_type(), &DataType::Decimal128(38, 4));
        assert_eq!(
            d.iter().collect::<Vec<_>>(),
            vec![Some(0), None, Some(-312345), None, Some(0)]
        );
        let f = output
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            f.iter().map(|v| v.map(f64::to_bits)).collect::<Vec<_>>(),
            vec![
                Some(7.5f64.to_bits()),
                None,
                Some((-0.0f64).to_bits()),
                None,
                Some(7.5f64.to_bits())
            ]
        );
        let k = output
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            k.iter().collect::<Vec<_>>(),
            vec![Some(-7), None, None, None, Some(-7)]
        );
        drop(output);
        assert_eq!(pool.used(), 0);
        let nan_batch = RecordBatch::try_from_iter([(
            "f",
            Arc::new(Float64Array::from(vec![nan])) as ArrayRef,
        )])
        .unwrap();
        let result = gather(
            std::slice::from_ref(&nan_batch),
            &[Some((0, 0))],
            &[0],
            nan_batch.schema(),
            &pool,
        )
        .unwrap();
        assert_eq!(
            result
                .column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0)
                .to_bits(),
            nan.to_bits()
        );
        drop(result);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn utf8_growth_refuses_before_allocation_and_output_owns_its_lease() {
        let source = RecordBatch::try_from_iter([(
            "s",
            Arc::new(StringArray::from(vec![
                Some("x".repeat(9000)),
                None,
                Some("short".to_string()),
            ])) as ArrayRef,
        )])
        .unwrap();
        let small = MemoryPool::new(4096);
        assert!(gather(
            std::slice::from_ref(&source),
            &[Some((0, 0))],
            &[0],
            source.schema(),
            &small
        )
        .unwrap_err()
        .is_memory_limit());
        assert_eq!(small.used(), 0);
        let pool = MemoryPool::new(128 * 1024);
        let result = gather(
            std::slice::from_ref(&source),
            &[Some((0, 2)), None, Some((0, 0)), Some((0, 1))],
            &[0],
            source.schema(),
            &pool,
        )
        .unwrap();
        let array = result.column(0).slice(1, 2);
        let strings = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(
            strings.iter().collect::<Vec<_>>(),
            vec![Some("short"), None, Some("x".repeat(9000).as_str()), None]
        );
        drop(result);
        assert!(pool.used() > 0);
        drop(array);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn malformed_selection_and_all_null_wrong_types_fail_cleanly() {
        let source = RecordBatch::try_from_iter([(
            "v",
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        )])
        .unwrap();
        let pool = MemoryPool::new(64 * 1024);
        let logical = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        assert!(gather(
            std::slice::from_ref(&source),
            &[Some((0, 0))],
            &[0],
            logical,
            &pool
        )
        .is_err());
        assert_eq!(pool.used(), 0);
        for row in [Some((0, 9)), Some((9, 0))] {
            assert!(gather(
                std::slice::from_ref(&source),
                &[row],
                &[0],
                source.schema(),
                &pool
            )
            .is_err());
            assert_eq!(pool.used(), 0);
        }
    }
}

#[cfg(test)]
mod bound_tests {
    use super::*;
    #[test]
    fn bound_sources_preserve_mixed_encodings_and_wide_decimal_coefficients() {
        let plain = RecordBatch::try_from_iter([(
            "k",
            Arc::new(Int64Array::from(vec![Some(2), None, Some(4)])) as ArrayRef,
        )])
        .unwrap();
        let dictionary = DictionaryArray::<UInt8Type>::try_new(
            UInt8Array::from(vec![Some(0), Some(1), None, Some(2)]),
            Arc::new(Int64Array::from(vec![Some(-9), None, Some(7)])),
        )
        .unwrap();
        let encoded =
            RecordBatch::try_from_iter([("k", Arc::new(dictionary) as ArrayRef)]).unwrap();
        let pool = MemoryPool::new(128 * 1024);
        let schema = plain.schema();
        let rows = [
            Some((1, 3)),
            Some((0, 0)),
            None,
            Some((1, 0)),
            Some((0, 1)),
            Some((1, 1)),
            Some((1, 2)),
            Some((0, 2)),
        ];
        let output = gather(&[plain, encoded], &rows, &[0], schema, &pool).unwrap();
        let values = output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(7), Some(2), None, Some(-9), None, None, None, Some(4)]
        );
        drop(output);
        let wide = i128::MAX / 100;
        let source = RecordBatch::try_from_iter([(
            "d",
            Arc::new(
                Decimal128Array::from(vec![Some(wide), None, Some(-wide)])
                    .with_precision_and_scale(38, 7)
                    .unwrap(),
            ) as ArrayRef,
        )])
        .unwrap();
        let output = gather(
            std::slice::from_ref(&source),
            &[Some((0, 2)), Some((0, 0)), None, Some((0, 1))],
            &[0],
            source.schema(),
            &pool,
        )
        .unwrap();
        let values = output
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert_eq!(values.data_type(), &DataType::Decimal128(38, 7));
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(-wide), Some(wide), None, None]
        );
        drop(output);
        assert_eq!(pool.used(), 0);
    }
}
