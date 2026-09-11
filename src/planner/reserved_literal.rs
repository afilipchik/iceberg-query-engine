//! Scalar expansion with payload admission before allocation.
use crate::error::{QueryError, Result};
use crate::execution::{MemoryPool, ReservedBufferBuilder};
use crate::planner::ScalarValue;
use arrow::array::*;
use arrow::buffer::{BooleanBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::*;
use std::sync::Arc;

fn repeated<T: ArrowPrimitiveType>(
    pool: &MemoryPool,
    value: T::Native,
    rows: usize,
) -> Result<ArrayRef> {
    let mut buffer = ReservedBufferBuilder::<T::Native>::with_capacity(pool, rows)?;
    buffer.extend_reserved(rows, std::iter::repeat(value))?;
    Ok(Arc::new(PrimitiveArray::<T>::new(
        ScalarBuffer::new(buffer.finish(), 0, rows),
        None,
    )))
}

pub(crate) fn expand(pool: &MemoryPool, value: &ScalarValue, rows: usize) -> Result<ArrayRef> {
    macro_rules! primitive {
        ($ty:ty, $value:expr) => {
            repeated::<$ty>(pool, $value, rows)
        };
    }
    match value {
        ScalarValue::Null => Ok(Arc::new(NullArray::new(rows))),
        ScalarValue::Boolean(value) => {
            let bytes = rows.div_ceil(8);
            let mut buffer = ReservedBufferBuilder::<u8>::with_capacity(pool, bytes)?;
            buffer.extend_reserved(bytes, std::iter::repeat(if *value { 255 } else { 0 }))?;
            Ok(Arc::new(BooleanArray::new(
                BooleanBuffer::new(buffer.finish(), 0, rows),
                None,
            )))
        }
        ScalarValue::Int8(v) => primitive!(Int8Type, *v),
        ScalarValue::Int16(v) => primitive!(Int16Type, *v),
        ScalarValue::Int32(v) => primitive!(Int32Type, *v),
        ScalarValue::Int64(v) => primitive!(Int64Type, *v),
        ScalarValue::UInt8(v) => primitive!(UInt8Type, *v),
        ScalarValue::UInt16(v) => primitive!(UInt16Type, *v),
        ScalarValue::UInt32(v) => primitive!(UInt32Type, *v),
        ScalarValue::UInt64(v) => primitive!(UInt64Type, *v),
        ScalarValue::Float32(v) => primitive!(Float32Type, v.0),
        ScalarValue::Float64(v) => primitive!(Float64Type, v.0),
        ScalarValue::Date32(v) => primitive!(Date32Type, *v),
        ScalarValue::Date64(v) => primitive!(Date64Type, *v),
        ScalarValue::Timestamp(v) => {
            let counts = repeated::<Int64Type>(pool, v.ticks, rows)?;
            Ok(v.wrap_counts(counts.as_any().downcast_ref::<Int64Array>().unwrap()))
        }
        ScalarValue::Interval(v) => primitive!(
            IntervalDayTimeType,
            IntervalDayTime::new(*v as i32, (*v >> 32) as i32)
        ),
        ScalarValue::Decimal128(v) => {
            let array = repeated::<Decimal128Type>(pool, v.mantissa(), rows)?;
            let array = array.as_any().downcast_ref::<Decimal128Array>().unwrap();
            Ok(Arc::new(
                array.clone().with_precision_and_scale(38, v.scale())?,
            ))
        }
        ScalarValue::Utf8(value) => {
            let overflow = || {
                QueryError::Execution("literal string expansion exceeds Arrow offset range".into())
            };
            let bytes = value.len().checked_mul(rows).ok_or_else(overflow)?;
            if bytes > i32::MAX as usize {
                return Err(overflow());
            }
            let offsets_len = rows.checked_add(1).ok_or_else(overflow)?;
            let mut offsets = ReservedBufferBuilder::<i32>::with_capacity(pool, offsets_len)?;
            let mut data = ReservedBufferBuilder::<u8>::with_capacity(pool, bytes)?;
            offsets.extend_reserved(
                offsets_len,
                (0..offsets_len).map(|i| (i * value.len()) as i32),
            )?;
            for _ in 0..rows {
                data.extend_from_slice(value.as_bytes())?;
            }
            Ok(Arc::new(StringArray::new(
                OffsetBuffer::new(ScalarBuffer::new(offsets.finish(), 0, offsets_len)),
                data.finish(),
                None,
            )))
        }
        ScalarValue::List(..) => Err(QueryError::Execution(
            "budgeted list literal expansion is unsupported".into(),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::DecimalValue;

    #[test]
    fn admitted_expansion_preserves_all_flat_scalar_types_and_empty_batches() {
        let values = vec![
            ScalarValue::Null,
            ScalarValue::Boolean(true),
            ScalarValue::Boolean(false),
            ScalarValue::Int8(i8::MIN),
            ScalarValue::Int16(i16::MIN),
            ScalarValue::Int32(i32::MIN),
            ScalarValue::Int64(i64::MIN),
            ScalarValue::UInt8(u8::MAX),
            ScalarValue::UInt16(u16::MAX),
            ScalarValue::UInt32(u32::MAX),
            ScalarValue::UInt64(u64::MAX),
            ScalarValue::Float32((-0.0).into()),
            ScalarValue::Float64(f64::INFINITY.into()),
            ScalarValue::Utf8("é🦀".into()),
            ScalarValue::Utf8(String::new()),
            ScalarValue::Date32(-1),
            ScalarValue::Date64(-86400000),
            ScalarValue::Timestamp((-1234567).into()),
            ScalarValue::Interval(-1),
            ScalarValue::Decimal128(DecimalValue::new(123456789012345678901234567890, 4)),
        ];
        for value in values {
            for rows in [0, 1, 9] {
                let pool = MemoryPool::new(65536);
                let expected = crate::physical::operators::scalar_to_array(&value, rows).unwrap();
                let actual = expand(&pool, &value, rows).unwrap();
                assert_eq!(actual.to_data(), expected.to_data(), "{value:?}/{rows}");
                drop(actual);
                assert_eq!(pool.used(), 0);
            }
        }
    }

    #[test]
    fn payload_refusal_and_offset_overflow_release_partial_admission() {
        let pool = MemoryPool::new(65536);
        for value in [ScalarValue::Int64(1), ScalarValue::Utf8("hello".into())] {
            assert!(expand(&pool, &value, 16384).is_err());
            assert_eq!(pool.used(), 0);
        }
        assert!(expand(&pool, &ScalarValue::Utf8("aa".into()), i32::MAX as usize).is_err());
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn invalid_decimal_metadata_returns_error_and_releases_payload() {
        let pool = MemoryPool::new(65536);
        assert!(expand(
            &pool,
            &ScalarValue::Decimal128(DecimalValue::new(1, 100)),
            9
        )
        .is_err());
        assert_eq!(pool.used(), 0);
    }
}
