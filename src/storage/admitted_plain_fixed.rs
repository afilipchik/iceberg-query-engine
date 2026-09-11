//! Admitted, endian-correct PLAIN fixed-width output with retained input owners.
//! Caller validates the Parquet logical annotation (including timestamp units)
//! against the Arrow type before selecting this decoder. No implicit rescaling.
use crate::{
    execution::{MemoryPool, ReservedBufferBuilder},
    QueryError, Result,
};
use arrow::{
    array::{Array, ArrayRef, BooleanArray, PrimitiveArray, UInt32Array},
    buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer},
    datatypes::{
        ArrowPrimitiveType, DataType, Date32Type, Decimal128Type, Float32Type, Float64Type,
        Int32Type, Int64Type, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
        TimestampNanosecondType, TimestampSecondType, UInt32Type, UInt64Type,
    },
};
use parquet::basic::Type;
use std::sync::Arc;
fn invalid(message: &str) -> QueryError {
    QueryError::Storage(format!("PLAIN fixed page: {message}"))
}
#[derive(Clone)]
pub(crate) struct PlainFixedDecoder {
    data: Buffer,
    validity: Option<NullBuffer>,
    data_type: DataType,
    physical: Type,
    width: usize,
    rows: usize,
    row: usize,
    dense: usize,
    failed: bool,
    ids: Option<UInt32Array>,
}
impl PlainFixedDecoder {
    pub(crate) fn new(
        data: Buffer,
        rows: usize,
        validity: Option<NullBuffer>,
        physical: Type,
        fixed_length: usize,
        data_type: DataType,
    ) -> Result<Self> {
        let width = match (&physical, &data_type) {
            (Type::BOOLEAN, DataType::Boolean) => 0,
            (Type::INT32, DataType::Int32 | DataType::UInt32 | DataType::Date32) => 4,
            (Type::INT64, DataType::Int64 | DataType::UInt64 | DataType::Timestamp(_, _)) => 8,
            (Type::FLOAT, DataType::Float32) => 4,
            (Type::DOUBLE, DataType::Float64) => 8,
            (
                Type::INT32 | Type::INT64 | Type::FIXED_LEN_BYTE_ARRAY,
                DataType::Decimal128(p, s),
            ) => {
                if !(1..=38).contains(p) || *s > *p as i8 {
                    return Err(invalid("invalid decimal precision/scale"));
                }
                match physical {
                    Type::INT32 => 4,
                    Type::INT64 => 8,
                    _ if (1..=16).contains(&fixed_length) => fixed_length,
                    _ => return Err(invalid("decimal width outside i128 domain")),
                }
            }
            _ => {
                return Err(QueryError::NotImplemented(format!(
                    "PLAIN {physical:?} to {data_type:?}"
                )))
            }
        };
        if validity.as_ref().is_some_and(|v| v.len() != rows) {
            return Err(invalid("validity row count differs"));
        }
        let dense = rows - validity.as_ref().map_or(0, NullBuffer::null_count);
        let expected = if width == 0 {
            dense.div_ceil(8)
        } else {
            dense
                .checked_mul(width)
                .ok_or_else(|| invalid("value extent overflow"))?
        };
        if data.len() != expected {
            return Err(invalid("encoded length differs from value count"));
        }
        Ok(Self {
            data,
            validity,
            data_type,
            physical,
            width,
            rows,
            row: 0,
            dense: 0,
            failed: false,
            ids: None,
        })
    }
    /// Dense IDs reference exact dictionary entries, with NULLs represented only
    /// by the separate row validity mask. Both handles retain their owners.
    pub(crate) fn with_dictionary_ids(
        mut self,
        ids: UInt32Array,
        validity: Option<NullBuffer>,
    ) -> Result<Self> {
        if self.row != 0
            || self.ids.is_some()
            || self.validity.as_ref().is_some_and(|v| v.null_count() != 0)
        {
            return Err(invalid("dictionary must be attached before decoding"));
        }
        if ids.null_count() != 0 || ids.values().iter().any(|id| *id as usize >= self.rows) {
            return Err(invalid("invalid dictionary ID domain"));
        }
        let rows = validity.as_ref().map_or(ids.len(), NullBuffer::len);
        if rows - validity.as_ref().map_or(0, NullBuffer::null_count) != ids.len() {
            return Err(invalid("ID count differs from non-NULL rows"));
        }
        self.rows = rows;
        self.validity = validity;
        self.ids = Some(ids);
        Ok(self)
    }
    fn value_index(&self, dense: usize) -> usize {
        self.ids
            .as_ref()
            .map_or(dense, |ids| ids.value(dense) as usize)
    }
    fn primitive<T: ArrowPrimitiveType>(
        &self,
        count: usize,
        pool: &MemoryPool,
        decode: impl Fn(&[u8]) -> Result<T::Native>,
    ) -> Result<ArrayRef> {
        let mut output = ReservedBufferBuilder::<T::Native>::with_capacity(pool, count)?;
        let mut dense = self.dense;
        output.try_extend_reserved(
            count,
            (self.row..self.row + count).map(|row| {
                if self.validity.as_ref().is_some_and(|v| !v.is_valid(row)) {
                    return Ok(T::Native::default());
                }
                let start = self.value_index(dense) * self.width;
                dense += 1;
                decode(&self.data[start..start + self.width])
            }),
        )?;
        let nulls = self.validity.as_ref().map(|v| v.slice(self.row, count));
        Ok(Arc::new(
            PrimitiveArray::<T>::new(ScalarBuffer::new(output.finish(), 0, count), nulls)
                .with_data_type(self.data_type.clone()),
        ))
    }
    pub(crate) fn remaining(&self) -> usize {
        self.rows - self.row
    }

    pub(crate) fn next(&mut self, max_rows: usize, pool: &MemoryPool) -> Result<Option<ArrayRef>> {
        if self.failed {
            return Err(invalid("decoder is poisoned"));
        }
        if max_rows == 0 {
            return Err(invalid("positive row quantum required"));
        }
        // A quantum is an upper bound. next_chunk commits its cursors only on
        // success; memory denial drops provisional output without consuming
        // input. Retry smaller output, never a source read or a semantic error.
        let mut rows = max_rows.min(self.rows - self.row).max(1);
        let result = loop {
            let result = self.next_chunk(rows, pool);
            if rows > 1 && result.as_ref().is_err_and(|e| e.is_memory_limit()) {
                rows = (rows / 2).max(1);
                continue;
            }
            break result;
        };
        if result.as_ref().is_err_and(|e| !e.is_memory_limit()) {
            self.failed = true;
        }
        result
    }
    fn next_chunk(&mut self, max_rows: usize, pool: &MemoryPool) -> Result<Option<ArrayRef>> {
        if self.row == self.rows {
            return Ok(None);
        }
        let count = max_rows.min(self.rows - self.row);
        macro_rules! primitive {
            ($ty:ty, $native:ty) => {
                self.primitive::<$ty>(count, pool, |b| {
                    Ok(<$native>::from_le_bytes(b.try_into().unwrap()))
                })
            };
        }
        let result = match &self.data_type {
            DataType::Int32 => primitive!(Int32Type, i32),
            DataType::UInt32 => primitive!(UInt32Type, u32),
            DataType::Date32 => primitive!(Date32Type, i32),
            DataType::Int64 => primitive!(Int64Type, i64),
            DataType::UInt64 => primitive!(UInt64Type, u64),
            DataType::Float32 => primitive!(Float32Type, f32),
            DataType::Float64 => primitive!(Float64Type, f64),
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => primitive!(TimestampSecondType, i64),
                TimeUnit::Millisecond => primitive!(TimestampMillisecondType, i64),
                TimeUnit::Microsecond => primitive!(TimestampMicrosecondType, i64),
                TimeUnit::Nanosecond => primitive!(TimestampNanosecondType, i64),
            },
            DataType::Decimal128(precision, _) => {
                let bound = 10i128.pow(*precision as u32) - 1;
                self.primitive::<Decimal128Type>(count, pool, |b| {
                    let value = match self.physical {
                        Type::INT32 => i32::from_le_bytes(b.try_into().unwrap()) as i128,
                        Type::INT64 => i64::from_le_bytes(b.try_into().unwrap()) as i128,
                        _ => {
                            let mut extended = [if b[0] & 128 == 0 { 0 } else { 255 }; 16];
                            extended[16 - b.len()..].copy_from_slice(b);
                            i128::from_be_bytes(extended)
                        }
                    };
                    if value < -bound || value > bound {
                        return Err(invalid("coefficient exceeds declared precision"));
                    }
                    Ok(value)
                })
            }
            DataType::Boolean => {
                let mut bits = ReservedBufferBuilder::<u8>::with_capacity(pool, count.div_ceil(8))?;
                bits.extend_reserved(count.div_ceil(8), std::iter::repeat(0))?;
                let mut dense = self.dense;
                for i in 0..count {
                    if self
                        .validity
                        .as_ref()
                        .is_none_or(|v| v.is_valid(self.row + i))
                    {
                        let index = self.value_index(dense);
                        if self.data[index / 8] & (1 << (index % 8)) != 0 {
                            bits.as_mut_slice()[i / 8] |= 1 << (i % 8);
                        }
                        dense += 1;
                    }
                }
                Ok(Arc::new(BooleanArray::new(
                    BooleanBuffer::new(bits.finish(), 0, count),
                    self.validity.as_ref().map(|v| v.slice(self.row, count)),
                )) as ArrayRef)
            }
            _ => return Err(invalid("unsupported output type after validation")),
        }?;
        let nulls = self
            .validity
            .as_ref()
            .map_or(0, |v| v.slice(self.row, count).null_count());
        self.dense += count - nulls;
        self.row += count;
        Ok(Some(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Decimal128Array, Float64Array, Int64Array};
    #[test]
    fn dictionary_output_shrinks_to_budget_without_losing_rows() {
        let pool = MemoryPool::new(4096);
        let values = [-7i64, 91, -7];
        let expected = (0..1024)
            .map(|i| {
                if i % 5 == 0 {
                    None
                } else {
                    Some(values[i % 3])
                }
            })
            .collect::<Vec<_>>();
        let ids = UInt32Array::from(
            (0..1024)
                .filter(|i| i % 5 != 0)
                .map(|i| (i % 3) as u32)
                .collect::<Vec<_>>(),
        );
        let mut decoder = PlainFixedDecoder::new(
            Buffer::from(
                values
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
            ),
            3,
            None,
            Type::INT64,
            0,
            DataType::Int64,
        )
        .unwrap()
        .with_dictionary_ids(
            ids,
            Some(NullBuffer::from(
                expected.iter().map(Option::is_some).collect::<Vec<_>>(),
            )),
        )
        .unwrap();
        let held = pool.allocate(pool.max()).unwrap();
        assert!(decoder.next(1024, &pool).unwrap_err().is_memory_limit());
        assert_eq!(decoder.row, 0);
        assert_eq!(decoder.dense, 0);
        drop(held);
        let mut actual = Vec::new();
        while let Some(chunk) = decoder.next(1024, &pool).unwrap() {
            assert!(chunk.len() < 1024);
            actual.extend(chunk.as_any().downcast_ref::<Int64Array>().unwrap().iter());
        }
        assert_eq!(actual, expected);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn nullable_unaligned_integer_chunks_refuse_without_losing_prefix() {
        let pool = MemoryPool::new(8192);
        let expected = [i64::MIN, 7, i64::MAX];
        let mut raw = vec![99];
        raw.extend(expected.iter().flat_map(|v| v.to_le_bytes()));
        let mut storage = ReservedBufferBuilder::<u8>::with_capacity(&pool, raw.len()).unwrap();
        storage.extend_reserved(raw.len(), raw).unwrap();
        let body = storage.finish().slice(1);
        let input_charge = pool.used();
        let mask = NullBuffer::from(vec![true, false, true, true]);
        let mut decoder =
            PlainFixedDecoder::new(body, 4, Some(mask), Type::INT64, 0, DataType::Int64).unwrap();
        let first = decoder.next(2, &pool).unwrap().unwrap();
        assert_eq!(
            first
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(i64::MIN), None]
        );
        drop(first);
        let held = pool.allocate(pool.max() - pool.used()).unwrap();
        assert!(decoder.next(2, &pool).unwrap_err().is_memory_limit());
        drop(held);
        assert_eq!(pool.used(), input_charge);
        let last = decoder.next(2, &pool).unwrap().unwrap();
        assert_eq!(
            last.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[7, i64::MAX]
        );
        let slice = last.slice(1, 1);
        drop(last);
        drop(decoder);
        assert!(pool.used() > 0);
        drop(slice);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn decimals_preserve_wide_signed_coefficients_and_reject_precision_overflow() {
        let pool = MemoryPool::new(8192);
        for (physical, width, bytes, values, precision) in [
            (
                Type::INT32,
                0,
                [-123i32, 456]
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect::<Vec<_>>(),
                vec![-123i128, 456],
                9,
            ),
            (
                Type::INT64,
                0,
                [i64::MIN, i64::MAX]
                    .iter()
                    .flat_map(|v| v.to_le_bytes())
                    .collect(),
                vec![i64::MIN as i128, i64::MAX as i128],
                19,
            ),
            (
                Type::FIXED_LEN_BYTE_ARRAY,
                3,
                vec![255, 255, 254, 0, 0, 3],
                vec![-2, 3],
                7,
            ),
            (
                Type::FIXED_LEN_BYTE_ARRAY,
                16,
                [-(10i128.pow(37)), 10i128.pow(37)]
                    .iter()
                    .flat_map(|v| v.to_be_bytes())
                    .collect(),
                vec![-10i128.pow(37), 10i128.pow(37)],
                38,
            ),
        ] {
            let mut decoder = PlainFixedDecoder::new(
                Buffer::from(bytes),
                2,
                None,
                physical,
                width,
                DataType::Decimal128(precision, 2),
            )
            .unwrap();
            let output = decoder.next(4, &pool).unwrap().unwrap();
            assert_eq!(
                output
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .values()
                    .as_ref(),
                values
            );
        }
        let bytes = [1i64, 1000]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<_>>();
        let mut decoder = PlainFixedDecoder::new(
            Buffer::from(bytes),
            2,
            None,
            Type::INT64,
            0,
            DataType::Decimal128(3, -2),
        )
        .unwrap();
        drop(decoder.next(1, &pool).unwrap());
        assert!(decoder
            .next(1, &pool)
            .unwrap_err()
            .to_string()
            .contains("precision"));
        assert_eq!(pool.used(), 0);
        assert!(decoder
            .next(1, &pool)
            .unwrap_err()
            .to_string()
            .contains("poisoned"));
    }
    #[test]
    fn floats_and_booleans_keep_bits_nulls_and_dense_positions() {
        let pool = MemoryPool::new(8192);
        let bits = [
            0x8000000000000000u64,
            0x7ff8000000000042,
            0xfff0000000000000,
        ];
        let bytes = bits
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<_>>();
        let mut decoder = PlainFixedDecoder::new(
            Buffer::from(bytes),
            3,
            None,
            Type::DOUBLE,
            0,
            DataType::Float64,
        )
        .unwrap();
        let output = decoder.next(3, &pool).unwrap().unwrap();
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .values()
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>(),
            bits
        );
        drop(output);
        drop(decoder);
        let mut decoder = PlainFixedDecoder::new(
            Buffer::from(vec![0b101u8]),
            4,
            Some(NullBuffer::from(vec![true, false, true, true])),
            Type::BOOLEAN,
            0,
            DataType::Boolean,
        )
        .unwrap();
        let mut result = Vec::new();
        while let Some(output) = decoder.next(2, &pool).unwrap() {
            result.extend(
                output
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter(),
            );
        }
        assert_eq!(result, vec![Some(true), None, Some(false), Some(true)]);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn invalid_extents_types_and_empty_nullable_inputs() {
        assert!(PlainFixedDecoder::new(
            Buffer::from(vec![0u8; 7]),
            1,
            None,
            Type::INT64,
            0,
            DataType::Int64
        )
        .is_err());
        assert!(PlainFixedDecoder::new(
            Buffer::from(vec![0u8; 8]),
            1,
            None,
            Type::INT64,
            0,
            DataType::Float64
        )
        .is_err());
        assert!(PlainFixedDecoder::new(
            Buffer::from(Vec::<u8>::new()),
            1,
            Some(NullBuffer::new_null(2)),
            Type::INT32,
            0,
            DataType::Date32
        )
        .is_err());
        let pool = MemoryPool::new(8192);
        let mut empty = PlainFixedDecoder::new(
            Buffer::from(Vec::<u8>::new()),
            0,
            None,
            Type::INT32,
            0,
            DataType::Date32,
        )
        .unwrap();
        assert!(empty.next(1, &pool).unwrap().is_none());
        let mut nulls = PlainFixedDecoder::new(
            Buffer::from(Vec::<u8>::new()),
            3,
            Some(NullBuffer::new_null(3)),
            Type::INT32,
            0,
            DataType::Date32,
        )
        .unwrap();
        let output = nulls.next(3, &pool).unwrap().unwrap();
        assert_eq!(output.null_count(), 3);
        assert_eq!(output.data_type(), &DataType::Date32);
    }
    #[test]
    fn dictionary_ids_preserve_nulls_duplicates_and_retry_prefix() {
        let pool = MemoryPool::new(8192);
        let bytes = [-10i64, 20, -10]
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect::<Vec<_>>();
        let mut decoder = PlainFixedDecoder::new(
            Buffer::from(bytes.clone()),
            3,
            None,
            Type::INT64,
            0,
            DataType::Int64,
        )
        .unwrap()
        .with_dictionary_ids(
            UInt32Array::from(vec![2, 0, 1]),
            Some(NullBuffer::from(vec![true, false, true, true])),
        )
        .unwrap();
        let first = decoder.next(2, &pool).unwrap().unwrap();
        assert_eq!(
            first
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-10), None]
        );
        drop(first);
        let held = pool.allocate(pool.max()).unwrap();
        assert!(decoder.next(2, &pool).unwrap_err().is_memory_limit());
        drop(held);
        let rest = decoder.next(2, &pool).unwrap().unwrap();
        assert_eq!(
            rest.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[-10, 20]
        );
        drop(rest);
        drop(decoder);
        assert_eq!(pool.used(), 0);
        for ids in [UInt32Array::from(vec![3]), UInt32Array::from(vec![None])] {
            assert!(PlainFixedDecoder::new(
                Buffer::from(bytes.clone()),
                3,
                None,
                Type::INT64,
                0,
                DataType::Int64
            )
            .unwrap()
            .with_dictionary_ids(ids, None)
            .is_err());
        }
    }
}
