//! Primitive and integer-to-decimal coercion with pre-admitted output buffers.
use crate::error::{QueryError, Result};
use crate::execution::{MemoryPool, ReservedBufferBuilder};
use crate::planner::CastMode;
use arrow::array::{Array, ArrayRef, PrimitiveArray};
use arrow::buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
use arrow::datatypes::*;
use num_traits::{AsPrimitive, NumCast};
use std::sync::Arc;

/// `convert` performs value conversion only. Allocation/admission happens
/// outside it, so TRY_CAST cannot swallow a resource refusal. Failed values use
/// Option; error messages are constructed only for a strict-cast failure.
fn convert<F: ArrowPrimitiveType, T: ArrowPrimitiveType>(
    pool: &MemoryPool,
    array: &ArrayRef,
    mode: CastMode,
    mut convert: impl FnMut(F::Native) -> Option<T::Native>,
    mut conversion_error: impl FnMut(F::Native) -> QueryError,
) -> Result<PrimitiveArray<T>> {
    let source = array.as_any().downcast_ref::<PrimitiveArray<F>>().unwrap();
    let len = source.len();
    let mut values = ReservedBufferBuilder::<T::Native>::with_capacity(pool, len)?;
    let nulls = if mode == CastMode::Strict {
        if source.null_count() == 0 {
            values.try_extend_reserved(
                len,
                source
                    .values()
                    .iter()
                    .copied()
                    .map(|value| convert(value).ok_or_else(|| conversion_error(value))),
            )?;
        } else {
            values.try_extend_reserved(
                len,
                source.iter().map(|value| match value {
                    Some(value) => convert(value).ok_or_else(|| conversion_error(value)),
                    None => Ok(T::Native::default()),
                }),
            )?;
        }
        source.nulls().cloned()
    } else {
        let mut validity = ReservedBufferBuilder::<u8>::with_capacity(pool, len.div_ceil(8))?;
        // Arrow's chunk iterator preserves non-byte-aligned slice offsets.
        let input_chunks = if source.null_count() == 0 {
            None
        } else {
            source.nulls().map(|n| n.inner().bit_chunks())
        };
        let mut input_masks = input_chunks.as_ref().map(|chunks| chunks.iter_padded());
        let mut valid_count = 0;
        for chunk in source.values().chunks(64) {
            let input_bits = match input_masks.as_mut() {
                None => u64::MAX,
                Some(chunks) => chunks.next().ok_or_else(|| {
                    QueryError::Internal("cast input validity ended before values".into())
                })?,
            };
            // Begin with input validity, then clear only conversion failures.
            // The final block's padding stays zero without a per-row update.
            let mut output_bits = input_bits & (u64::MAX >> (64 - chunk.len()));
            let mut write_value = |i: usize, value: Option<T::Native>| match value {
                Some(value) => value,
                None => {
                    output_bits &= !(1_u64 << i);
                    T::Native::default()
                }
            };
            if input_bits == 0 {
                values.extend_reserved(chunk.len(), std::iter::repeat(T::Native::default()))?;
            } else if input_bits == u64::MAX {
                values.extend_reserved(
                    chunk.len(),
                    chunk
                        .iter()
                        .copied()
                        .enumerate()
                        .map(|(i, value)| write_value(i, convert(value))),
                )?;
            } else {
                // Initialize NULL slots in bulk and visit only valid inputs.
                // All indexing is bounded by the current block's extent.
                let start = values.as_slice().len();
                values.extend_reserved(chunk.len(), std::iter::repeat(T::Native::default()))?;
                let output = &mut values.as_mut_slice()[start..];
                let mut remaining = input_bits & (u64::MAX >> (64 - chunk.len()));
                while remaining != 0 {
                    let i = remaining.trailing_zeros() as usize;
                    output[i] = write_value(i, convert(chunk[i]));
                    remaining &= remaining - 1;
                }
            }
            valid_count += output_bits.count_ones() as usize;
            // One bitmap write per block, with zero padding in the final byte.
            // Both buffers were admitted before any conversion; no growth occurs.
            validity.extend_reserved(chunk.len().div_ceil(8), output_bits.to_le_bytes())?;
        }
        if valid_count == len {
            None
        } else {
            Some(NullBuffer::new(BooleanBuffer::new(
                validity.finish(),
                0,
                len,
            )))
        }
    };
    Ok(PrimitiveArray::<T>::new(
        ScalarBuffer::new(values.finish(), 0, len),
        nulls,
    ))
}

/// Only dispatch here for type pairs whose complete source domain converts
/// without failure. Integer-to-float rounding is intentional SQL cast behavior.
/// NULLs remain NULLs; neither strict nor TRY mode needs a new validity buffer.
fn infallible<F: ArrowPrimitiveType, T: ArrowPrimitiveType>(
    pool: &MemoryPool,
    array: &ArrayRef,
) -> Result<ArrayRef>
where
    F::Native: AsPrimitive<T::Native>,
{
    let source = array.as_any().downcast_ref::<PrimitiveArray<F>>().unwrap();
    let len = source.len();
    let mut values = ReservedBufferBuilder::<T::Native>::with_capacity(pool, len)?;
    values.extend_reserved(len, source.values().iter().copied().map(|v| v.as_()))?;
    Ok(Arc::new(PrimitiveArray::<T>::new(
        ScalarBuffer::new(values.finish(), 0, len),
        source.nulls().cloned(),
    )))
}

fn numeric<F: ArrowPrimitiveType, T: ArrowPrimitiveType>(
    pool: &MemoryPool,
    array: &ArrayRef,
    mode: CastMode,
) -> Result<ArrayRef>
where
    F::Native: NumCast,
    T::Native: NumCast,
{
    Ok(Arc::new(convert::<F, T>(
        pool,
        array,
        mode,
        arrow_cast::cast::num_cast::<F::Native, T::Native>,
        |value| {
            arrow::error::ArrowError::CastError(format!(
                "Can't cast value {:?} to type {}",
                value,
                T::DATA_TYPE
            ))
            .into()
        },
    )?))
}

fn decimal<F: ArrowPrimitiveType>(
    pool: &MemoryPool,
    array: &ArrayRef,
    precision: u8,
    scale: i8,
    mode: CastMode,
) -> Result<ArrayRef>
where
    F::Native: NumCast,
{
    // Metadata and scale-factor errors are errors even for TRY_CAST or no rows.
    arrow_array::types::validate_decimal_precision_and_scale::<Decimal128Type>(precision, scale)?;
    let factor = 10_i128
        .checked_pow(scale.unsigned_abs() as u32)
        .ok_or_else(|| QueryError::Type("integer-to-decimal scale factor overflow".into()))?;
    let coefficient = |value| {
        let value = arrow_cast::cast::num_cast::<F::Native, i128>(value)?;
        if scale < 0 {
            value.checked_div(factor)
        } else {
            value.checked_mul(factor)
        }
    };
    let result = convert::<F, Decimal128Type>(
        pool,
        array,
        mode,
        |value| {
            coefficient(value).filter(|&v| Decimal128Type::is_valid_decimal_precision(v, precision))
        },
        |value| match coefficient(value) {
            None => QueryError::Type("integer-to-decimal coefficient overflow".into()),
            Some(value) => {
                match Decimal128Type::validate_decimal_precision(value, precision, scale) {
                    Err(error) => error.into(),
                    Ok(()) => QueryError::Internal(
                        "decimal conversion rejected a valid coefficient".into(),
                    ),
                }
            }
        },
    )?;
    Ok(Arc::new(result.with_precision_and_scale(precision, scale)?))
}

fn target<F: ArrowPrimitiveType>(
    pool: &MemoryPool,
    array: &ArrayRef,
    target: &DataType,
    mode: CastMode,
) -> Option<Result<ArrayRef>>
where
    F::Native: NumCast,
{
    macro_rules! dispatch { ($($name:ident => $ty:ty),*) => { match target {
        $(DataType::$name => Some(numeric::<F,$ty>(pool,array,mode)),)* _ => None,
    }}; }
    dispatch!(Int8=>Int8Type,Int16=>Int16Type,Int32=>Int32Type,Int64=>Int64Type,
        UInt8=>UInt8Type,UInt16=>UInt16Type,UInt32=>UInt32Type,UInt64=>UInt64Type,
        Float32=>Float32Type,Float64=>Float64Type)
}

pub(super) fn cast(
    pool: &MemoryPool,
    array: &ArrayRef,
    target_type: &DataType,
    mode: CastMode,
) -> Option<Result<ArrayRef>> {
    // This classification is a type-domain proof, never a data sample or range
    // estimate. All integer values fit in the finite range of either float.
    macro_rules! integer_float {
        ($($name:ident => $ty:ty),*) => {
            match (array.data_type(), target_type) {
                $((DataType::$name, DataType::Float32) =>
                    return Some(infallible::<$ty, Float32Type>(pool, array)),
                  (DataType::$name, DataType::Float64) =>
                    return Some(infallible::<$ty, Float64Type>(pool, array)),)*
                (DataType::Float32, DataType::Float64) =>
                    return Some(infallible::<Float32Type, Float64Type>(pool, array)),
                _ => {}
            }
        };
    }
    integer_float!(Int8=>Int8Type,Int16=>Int16Type,Int32=>Int32Type,Int64=>Int64Type,
        UInt8=>UInt8Type,UInt16=>UInt16Type,UInt32=>UInt32Type,UInt64=>UInt64Type);
    if let DataType::Decimal128(p, s) = target_type {
        macro_rules! dispatch { ($($name:ident => $ty:ty),*) => { match array.data_type() {
            $(DataType::$name => Some(decimal::<$ty>(pool,array,*p,*s,mode)),)* _ => None,
        }}; }
        return dispatch!(Int8=>Int8Type,Int16=>Int16Type,Int32=>Int32Type,Int64=>Int64Type,
            UInt8=>UInt8Type,UInt16=>UInt16Type,UInt32=>UInt32Type,UInt64=>UInt64Type);
    }
    macro_rules! dispatch { ($($name:ident => $ty:ty),*) => { match array.data_type() {
        $(DataType::$name => target::<$ty>(pool,array,target_type,mode),)* _ => None,
    }}; }
    dispatch!(Int8=>Int8Type,Int16=>Int16Type,Int32=>Int32Type,Int64=>Int64Type,
        UInt8=>UInt8Type,UInt16=>UInt16Type,UInt32=>UInt32Type,UInt64=>UInt64Type,
        Float32=>Float32Type,Float64=>Float64Type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::*;
    use arrow::compute::{cast_with_options, CastOptions};

    #[test]
    fn conversion_bitmap_matches_arrow_across_blocks_and_sliced_input_masks() {
        for pattern in 0..4 {
            for len in [0, 1, 7, 8, 9, 63, 64, 65, 127, 128, 129] {
                for offset in [0, 1, 7, 63] {
                    let source: ArrayRef = Arc::new(Int64Array::from_iter(
                        (0..offset + len + 7).map(|i| match pattern {
                            0 => Some(7),
                            1 => Some(128),
                            2 => None,
                            _ => match i % 4 {
                                0 => None,
                                1 => Some(-128),
                                2 => Some(127),
                                _ => Some(128),
                            },
                        }),
                    ));
                    let source = source.slice(offset, len);
                    for target in [DataType::Int8, DataType::Decimal128(2, 0)] {
                        for mode in [CastMode::Strict, CastMode::Try] {
                            let pool = MemoryPool::new(65536);
                            let expected = cast_with_options(
                                source.as_ref(),
                                &target,
                                &CastOptions {
                                    safe: mode == CastMode::Try,
                                    ..Default::default()
                                },
                            );
                            let actual = cast(&pool, &source, &target, mode).unwrap();
                            match (actual, expected) {
                                (Ok(actual), Ok(expected)) => assert_eq!(
                                    actual.to_data(), expected.to_data(),
                                    "pattern={pattern}, len={len}, offset={offset}, target={target:?}, mode={mode:?}"
                                ),
                                (Err(_), Err(_)) => {},
                                (actual, expected) => panic!(
                                    "pattern={pattern}, len={len}, offset={offset}, target={target:?}, mode={mode:?}: actual={actual:?}, expected={expected:?}"
                                ),
                            }
                            assert_eq!(pool.used(), 0);
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn try_cast_never_constructs_strict_conversion_errors() {
        let pool = MemoryPool::new(65536);
        let source: ArrayRef = Arc::new(Int64Array::from(vec![Some(128), None, Some(129)]));
        let result = convert::<Int64Type, Int8Type>(
            &pool,
            &source,
            CastMode::Try,
            |_| None,
            |_| panic!("TRY_CAST must not construct an error for a NULL result"),
        )
        .unwrap();
        assert_eq!(result.null_count(), 3);
        drop(result);
        assert_eq!(pool.used(), 0);
        let errors = std::cell::Cell::new(0);
        let result = convert::<Int64Type, Int8Type>(
            &pool,
            &source,
            CastMode::Strict,
            |_| None,
            |_| {
                errors.set(errors.get() + 1);
                QueryError::Type("conversion refused".into())
            },
        );
        assert!(result.is_err());
        assert_eq!(errors.get(), 1);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn primitive_and_integer_decimal_casts_match_pinned_arrow_values_types_and_errors() {
        let mut arrays: Vec<ArrayRef> = vec![];
        macro_rules! signed {
            ($array:ty,$ty:ty) => {
                arrays.push(Arc::new(<$array>::from(vec![
                    Some(<$ty>::MIN),
                    Some(-1),
                    Some(0),
                    Some(1),
                    Some(<$ty>::MAX),
                    None,
                ])));
            };
        }
        macro_rules! unsigned {
            ($array:ty,$ty:ty) => {
                arrays.push(Arc::new(<$array>::from(vec![
                    Some(0),
                    Some(1),
                    Some(127),
                    Some(255),
                    Some(<$ty>::MAX),
                    None,
                ])));
            };
        }
        signed!(Int8Array, i8);
        signed!(Int16Array, i16);
        signed!(Int32Array, i32);
        signed!(Int64Array, i64);
        unsigned!(UInt8Array, u8);
        unsigned!(UInt16Array, u16);
        unsigned!(UInt32Array, u32);
        unsigned!(UInt64Array, u64);
        arrays.push(Arc::new(Int64Array::from(vec![
            Some(-(1_i64 << 53) - 1),
            Some((1_i64 << 24) + 1),
            Some((1_i64 << 24) + 3),
            Some((1_i64 << 53) + 1),
            Some((1_i64 << 53) + 3),
            None,
        ])));
        arrays.push(Arc::new(UInt64Array::from(vec![
            Some((1_u64 << 24) + 1),
            Some((1_u64 << 53) + 1),
            Some((1_u64 << 53) + 3),
            Some((1_u64 << 63) + 1),
            Some(u64::MAX - 1024),
            None,
        ])));
        arrays.push(Arc::new(Float32Array::from(vec![
            Some(f32::NEG_INFINITY),
            Some(-0.0),
            Some(1.5),
            Some(f32::MAX),
            Some(f32::NAN),
            None,
        ])));
        arrays.push(Arc::new(Float64Array::from(vec![
            Some(f64::INFINITY),
            Some(-0.0),
            Some(-1.5),
            Some(f64::MAX),
            Some(f64::NAN),
            None,
        ])));
        for source in arrays {
            let mut targets = vec![
                DataType::Int8,
                DataType::Int16,
                DataType::Int32,
                DataType::Int64,
                DataType::UInt8,
                DataType::UInt16,
                DataType::UInt32,
                DataType::UInt64,
                DataType::Float32,
                DataType::Float64,
            ];
            if !matches!(source.data_type(), DataType::Float32 | DataType::Float64) {
                targets.extend(
                    [
                        (38, 0),
                        (20, 4),
                        (3, -2),
                        (2, 0),
                        (38, 38),
                        (38, -128),
                        (0, 0),
                        (2, 3),
                    ]
                    .map(|(p, s)| DataType::Decimal128(p, s)),
                );
            }
            for (offset, rows) in [
                (0, 0),
                (0, source.len() - 1),
                (0, source.len()),
                (source.len() - 2, 2),
                (1, source.len() - 2),
            ] {
                let source = source.slice(offset, rows);
                for target in &targets {
                    for mode in [CastMode::Strict, CastMode::Try] {
                        let pool = MemoryPool::new(65536);
                        let expected = cast_with_options(
                            source.as_ref(),
                            target,
                            &CastOptions {
                                safe: mode == CastMode::Try,
                                ..Default::default()
                            },
                        );
                        let actual = cast(&pool, &source, target, mode).unwrap();
                        match (actual,expected) {
                        (Ok(actual),Ok(expected))=>assert_eq!(actual.to_data(),expected.to_data(),"{:?}->{target:?}/{mode:?}/{rows}",source.data_type()),
                        (Err(_),Err(_))=>{},
                        (actual,expected)=>panic!("{:?}->{target:?}/{mode:?}/{rows}: actual={actual:?}; expected={expected:?}",source.data_type()),
                    }
                        assert_eq!(pool.used(), 0);
                    }
                }
            }
        }
    }
}
