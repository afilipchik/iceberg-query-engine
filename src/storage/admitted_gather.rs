//! Flat survivor copying using exact, pre-admitted output buffers.
use crate::{
    execution::{reserved_vec::ReservedVec, MemoryPool, ReservedBufferBuilder},
    QueryError, Result,
};
use arrow::{
    array::*,
    buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::*,
    record_batch::RecordBatch,
};
use std::sync::Arc;
fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("admitted gather: {message}"))
}
fn validity(array: &dyn Array, ids: &UInt32Array, pool: &MemoryPool) -> Result<Option<NullBuffer>> {
    if array.null_count() == 0 {
        return Ok(None);
    }
    let mut bits = ReservedBufferBuilder::<u8>::with_capacity(pool, ids.len().div_ceil(8))?;
    bits.extend_reserved(ids.len().div_ceil(8), std::iter::repeat(0))?;
    for (row, index) in ids.values().iter().enumerate() {
        if array.is_valid(*index as usize) {
            bits.as_mut_slice()[row / 8] |= 1 << (row % 8);
        }
    }
    Ok(Some(NullBuffer::new(BooleanBuffer::new(
        bits.finish(),
        0,
        ids.len(),
    ))))
}
fn primitive<T: ArrowPrimitiveType>(
    array: &ArrayRef,
    ids: &UInt32Array,
    pool: &MemoryPool,
) -> Result<ArrayRef> {
    let input = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| invalid("primitive representation mismatch"))?;
    let nulls = validity(input, ids, pool)?;
    let mut values = ReservedBufferBuilder::<T::Native>::with_capacity(pool, ids.len())?;
    values.extend_reserved(
        ids.len(),
        ids.values().iter().map(|i| {
            if input.is_null(*i as usize) {
                T::Native::default()
            } else {
                input.value(*i as usize)
            }
        }),
    )?;
    Ok(Arc::new(
        PrimitiveArray::<T>::new(ScalarBuffer::new(values.finish(), 0, ids.len()), nulls)
            .with_data_type(input.data_type().clone()),
    ))
}
fn column(array: &ArrayRef, ids: &UInt32Array, pool: &MemoryPool) -> Result<ArrayRef> {
    macro_rules! p {
        ($t:ty) => {
            primitive::<$t>(array, ids, pool)
        };
    }
    match array.data_type() {
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
        DataType::Boolean => {
            let input = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| invalid("Boolean representation mismatch"))?;
            let nulls = validity(input, ids, pool)?;
            let mut bits = ReservedBufferBuilder::<u8>::with_capacity(pool, ids.len().div_ceil(8))?;
            bits.extend_reserved(ids.len().div_ceil(8), std::iter::repeat(0))?;
            for (row, index) in ids.values().iter().enumerate() {
                if input.is_valid(*index as usize) && input.value(*index as usize) {
                    bits.as_mut_slice()[row / 8] |= 1 << (row % 8);
                }
            }
            Ok(Arc::new(BooleanArray::new(
                BooleanBuffer::new(bits.finish(), 0, ids.len()),
                nulls,
            )))
        }
        DataType::Utf8 => {
            let input = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid("UTF8 representation mismatch"))?;
            let bytes = ids.values().iter().try_fold(0usize, |total, index| {
                total
                    .checked_add(if input.is_null(*index as usize) {
                        0
                    } else {
                        input.value(*index as usize).len()
                    })
                    .filter(|n| *n <= i32::MAX as usize)
                    .ok_or_else(|| invalid("UTF8 offset domain overflow"))
            })?;
            let nulls = validity(input, ids, pool)?;
            let mut offsets = ReservedBufferBuilder::<i32>::with_capacity(
                pool,
                ids.len()
                    .checked_add(1)
                    .ok_or_else(|| invalid("offset count overflow"))?,
            )?;
            let mut values = ReservedBufferBuilder::<u8>::with_capacity(pool, bytes)?;
            offsets.extend_reserved(1, [0])?;
            for index in ids.values() {
                if input.is_valid(*index as usize) {
                    let value = input.value(*index as usize).as_bytes();
                    values.extend_reserved(value.len(), value.iter().copied())?;
                }
                offsets.extend_reserved(1, [values.as_slice().len() as i32])?;
            }
            Ok(Arc::new(StringArray::try_new(
                OffsetBuffer::new(ScalarBuffer::new(offsets.finish(), 0, ids.len() + 1)),
                values.finish(),
                nulls,
            )?))
        }
        other => Err(QueryError::NotImplemented(format!(
            "admitted gather type {other:?}"
        ))),
    }
}
/// IDs may repeat/reorder rows, but must be non-NULL and in the exact input domain.
pub(crate) fn take(
    batch: &RecordBatch,
    ids: &UInt32Array,
    pool: &MemoryPool,
) -> Result<RecordBatch> {
    if ids.null_count() != 0 || ids.values().iter().any(|i| *i as usize >= batch.num_rows()) {
        return Err(invalid("invalid row ID domain"));
    }
    let mut columns = ReservedVec::with_capacity(pool, batch.num_columns())?;
    for input in batch.columns() {
        columns.extend_reserved(1, [column(input, ids, pool)?])?;
    }
    super::admitted_batch::finish(batch.schema(), ids.len(), columns, pool)
}
/// SQL WHERE keeps only valid true mask rows. NULL mask rows are discarded.
pub(crate) fn filter(
    batch: &RecordBatch,
    mask: &BooleanArray,
    pool: &MemoryPool,
) -> Result<RecordBatch> {
    if mask.len() != batch.num_rows() || batch.num_rows() > u32::MAX as usize {
        return Err(invalid("mask length or row domain differs"));
    }
    let count = (0..mask.len())
        .filter(|i| mask.is_valid(*i) && mask.value(*i))
        .count();
    let mut ids = ReservedBufferBuilder::<u32>::with_capacity(pool, count)?;
    ids.extend_reserved(
        count,
        (0..mask.len())
            .filter(|i| mask.is_valid(*i) && mask.value(*i))
            .map(|i| i as u32),
    )?;
    take(
        batch,
        &UInt32Array::new(ScalarBuffer::new(ids.finish(), 0, count), None),
        pool,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    fn fixture() -> RecordBatch {
        let wide = 10i128.pow(30);
        let long = "é".repeat(4096);
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![
                Some(9),
                Some(i64::MIN),
                None,
                Some(7),
                Some(i64::MAX),
                Some(0),
            ])),
            Arc::new(
                Decimal128Array::from(vec![
                    Some(0),
                    Some(-wide),
                    None,
                    Some(wide),
                    Some(1),
                    Some(0),
                ])
                .with_precision_and_scale(38, 4)
                .unwrap(),
            ),
            Arc::new(Date32Array::from(vec![
                Some(0),
                Some(-1),
                None,
                Some(20000),
                Some(100),
                Some(0),
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(false),
                Some(true),
                None,
                Some(false),
                Some(true),
                Some(false),
            ])),
            Arc::new(StringArray::from(vec![
                Some("unused"),
                Some("é"),
                None,
                Some(""),
                Some(long.as_str()),
                Some("unused"),
            ])),
            Arc::new(Float64Array::from(vec![
                0.0,
                -0.0,
                f64::from_bits(0x7ff8000000000042),
                f64::INFINITY,
                -2.5,
                0.0,
            ])),
        ];
        let schema = Arc::new(Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, arrays).unwrap().slice(1, 4)
    }
    #[test]
    fn typed_duplicate_gather_and_sql_null_masks_match_independent_arrow_oracle() {
        let pool = MemoryPool::new(131072);
        let batch = fixture();
        let ids = UInt32Array::from(vec![3, 0, 2, 0, 1]);
        let output = take(&batch, &ids, &pool).unwrap();
        for (index, input) in batch.columns().iter().enumerate() {
            let expected = arrow::compute::take(input.as_ref(), &ids, None).unwrap();
            assert_eq!(output.column(index).to_data(), expected.to_data());
        }
        assert_eq!(output.schema(), batch.schema());
        let floats = output
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(floats.value(1).to_bits(), (-0.0f64).to_bits());
        assert_eq!(floats.value(4).to_bits(), 0x7ff8000000000042);
        drop(output);
        assert_eq!(pool.used(), 0);
        let mask = BooleanArray::from(vec![Some(true), None, Some(false), Some(true)]);
        let filtered = filter(&batch, &mask, &pool).unwrap();
        let expected = arrow::compute::filter_record_batch(&batch, &mask).unwrap();
        for (actual, expected) in filtered.columns().iter().zip(expected.columns()) {
            assert_eq!(actual.to_data(), expected.to_data());
        }
        drop(filtered);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn denial_after_partial_columns_releases_all_output_and_retry_is_exact() {
        let batch = fixture();
        let ids = UInt32Array::from(vec![3, 3]);
        let small = MemoryPool::new(10000);
        assert!(take(&batch, &ids, &small).unwrap_err().is_memory_limit());
        assert_eq!(small.used(), 0);
        let pool = MemoryPool::new(131072);
        let output = take(&batch, &ids, &pool).unwrap();
        let retained = output
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .clone();
        drop(output);
        drop(batch);
        assert_eq!(retained.value(0), retained.value(1));
        assert!(pool.used() >= 16384);
        let data = retained.to_data();
        drop(retained);
        assert!(pool.used() > 0);
        drop(data);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn empty_output_and_invalid_masks_or_ids_have_explicit_results() {
        let pool = MemoryPool::new(131072);
        let batch = fixture();
        for mask in [
            BooleanArray::from(vec![false; 4]),
            BooleanArray::from(vec![None; 4]),
        ] {
            let empty = filter(&batch, &mask, &pool).unwrap();
            assert_eq!(empty.num_rows(), 0);
            assert_eq!(empty.schema(), batch.schema());
            drop(empty);
            assert_eq!(pool.used(), 0);
        }
        for ids in [UInt32Array::from(vec![4]), UInt32Array::from(vec![None])] {
            assert!(take(&batch, &ids, &pool).is_err());
            assert_eq!(pool.used(), 0);
        }
        assert!(filter(&batch, &BooleanArray::from(vec![true]), &pool).is_err());
        let fixed = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)])),
            vec![batch.column(0).clone()],
        )
        .unwrap();
        let empty = filter(&fixed, &BooleanArray::from(vec![false; 4]), &pool).unwrap();
        let retained = empty
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        drop(empty);
        assert!(
            pool.used() >= 4096,
            "empty fixed arrays must retain handoff metadata"
        );
        let data = retained.to_data();
        drop(retained);
        assert!(pool.used() >= 4096);
        drop(data);
        assert_eq!(pool.used(), 0);
    }
}
