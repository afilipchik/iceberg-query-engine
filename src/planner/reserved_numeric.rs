//! Pre-admitted primitive output kernels. Decimal rescaling and other casts
//! continue through the shared numeric implementation until separately admitted.
use crate::error::{QueryError, Result};
use crate::execution::{MemoryPool, ReservedBufferBuilder};
use crate::planner::BinaryOp;
use arrow::array::{Array, ArrayRef, PrimitiveArray, StringArray};
use arrow::buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::datatypes::*;
use arrow_array::ArrowNativeTypeOp;
use std::sync::Arc;

fn nulls(pool: &MemoryPool, len: usize, valid: impl Fn(usize) -> bool) -> Result<NullBuffer> {
    let mut bytes = ReservedBufferBuilder::<u8>::with_capacity(pool, len.div_ceil(8))?;
    for start in (0..len).step_by(8) {
        let mut byte = 0;
        for bit in 0..8.min(len - start) {
            byte |= u8::from(valid(start + bit)) << bit;
        }
        bytes.extend_from_slice(&[byte])?;
    }
    Ok(NullBuffer::new(BooleanBuffer::new(bytes.finish(), 0, len)))
}
fn binary<T: ArrowPrimitiveType>(
    pool: &MemoryPool,
    op: BinaryOp,
    l: &ArrayRef,
    r: &ArrayRef,
) -> Result<ArrayRef>
where
    T::Native: ArrowNativeTypeOp,
{
    let l = l.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let r = r.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    if l.len() != r.len() {
        return Err(QueryError::Execution(
            "arithmetic array lengths differ".into(),
        ));
    }
    let len = l.len();
    if crate::execution::expression_memory::trace_enabled() {
        eprintln!(
            "[reserved-expression] {}",
            serde_json::json!({"event":"arithmetic","rows":len,"type":format!("{:?}",l.data_type()),"operator":format!("{op:?}")})
        );
    }
    let mut values = ReservedBufferBuilder::<T::Native>::with_capacity(pool, len)?;
    let validity = if l.null_count() + r.null_count() > 0 {
        Some(nulls(pool, len, |i| l.is_valid(i) && r.is_valid(i))?)
    } else {
        None
    };
    // Choose the checked operation and validity path once. Fill the admitted
    // output directly, without the previous chunk buffer and payload copy.
    macro_rules! fill {
        ($method:ident) => {
            if let Some(validity) = &validity {
                values.try_extend_reserved(
                    len,
                    (0..len).map(|i| {
                        if validity.is_null(i) {
                            Ok(T::Native::default())
                        } else {
                            l.value(i).$method(r.value(i)).map_err(Into::into)
                        }
                    }),
                )?;
            } else {
                values.try_extend_reserved(
                    len,
                    l.values()
                        .iter()
                        .zip(r.values().iter())
                        .map(|(&a, &b)| a.$method(b).map_err(Into::into)),
                )?;
            }
        };
    }
    match op {
        BinaryOp::Add => fill!(add_checked),
        BinaryOp::Subtract => fill!(sub_checked),
        BinaryOp::Multiply => fill!(mul_checked),
        BinaryOp::Divide => fill!(div_checked),
        BinaryOp::Modulo => fill!(mod_checked),
        _ => {
            return Err(QueryError::Internal(
                "non-arithmetic reserved kernel".into(),
            ))
        }
    }
    Ok(Arc::new(PrimitiveArray::<T>::new(
        ScalarBuffer::new(values.finish(), 0, len),
        validity,
    )))
}
// Called only for non-null Float32/Float64. Wrapping and checked Arrow
// arithmetic have identical IEEE behavior here; integer overflow still uses
// the checked kernel above. Select the operation once outside the value loop.
fn non_null_float<T: ArrowPrimitiveType>(
    pool: &MemoryPool,
    op: BinaryOp,
    l: &ArrayRef,
    r: &ArrayRef,
) -> Result<ArrayRef>
where
    T::Native: ArrowNativeTypeOp,
{
    let l = l.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    let r = r.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
    if l.len() != r.len() {
        return Err(QueryError::Execution(
            "arithmetic array lengths differ".into(),
        ));
    }
    let len = l.len();
    if crate::execution::expression_memory::trace_enabled() {
        eprintln!(
            "[reserved-expression] {}",
            serde_json::json!({"event":"arithmetic","rows":len,"type":format!("{:?}",l.data_type()),"operator":format!("{op:?}")})
        );
    }
    let mut values = ReservedBufferBuilder::<T::Native>::with_capacity(pool, len)?;
    macro_rules! fill {
        ($method:ident) => {
            values.extend_reserved(
                len,
                l.values()
                    .iter()
                    .zip(r.values().iter())
                    .map(|(&a, &b)| a.$method(b)),
            )?
        };
    }
    match op {
        BinaryOp::Add => fill!(add_wrapping),
        BinaryOp::Subtract => fill!(sub_wrapping),
        BinaryOp::Multiply => fill!(mul_wrapping),
        BinaryOp::Divide => fill!(div_wrapping),
        BinaryOp::Modulo => fill!(mod_wrapping),
        _ => {
            return Err(QueryError::Internal(
                "non-arithmetic reserved float kernel".into(),
            ))
        }
    }
    Ok(Arc::new(PrimitiveArray::<T>::new(
        ScalarBuffer::new(values.finish(), 0, len),
        None,
    )))
}

pub(super) fn arithmetic(
    pool: &MemoryPool,
    op: BinaryOp,
    l: &ArrayRef,
    r: &ArrayRef,
) -> Option<Result<ArrayRef>> {
    if l.data_type() != r.data_type() {
        return None;
    }
    if l.null_count() == 0 && r.null_count() == 0 {
        match l.data_type() {
            DataType::Float32 => return Some(non_null_float::<Float32Type>(pool, op, l, r)),
            DataType::Float64 => return Some(non_null_float::<Float64Type>(pool, op, l, r)),
            _ => {}
        }
    }
    macro_rules! dispatch { ($($dt:ident => $ty:ty),*) => { match l.data_type() {
        $(DataType::$dt=>Some(binary::<$ty>(pool,op,l,r)),)* _=>None,
    }}; }
    dispatch!(Int8=>Int8Type,Int16=>Int16Type,Int32=>Int32Type,Int64=>Int64Type,
        UInt8=>UInt8Type,UInt16=>UInt16Type,UInt32=>UInt32Type,UInt64=>UInt64Type,
        Float32=>Float32Type,Float64=>Float64Type)
}

struct TextWriter<'a> {
    bytes: &'a mut ReservedBufferBuilder<u8>,
    error: Option<QueryError>,
}
impl std::fmt::Write for TextWriter<'_> {
    fn write_str(&mut self, value: &str) -> std::fmt::Result {
        self.bytes.extend_from_slice(value.as_bytes()).map_err(|e| {
            self.error = Some(e);
            std::fmt::Error
        })
    }
}
fn integer_text(pool: &MemoryPool, array: &ArrayRef) -> Result<ArrayRef> {
    let options = arrow_cast::display::FormatOptions::default();
    let formatter = arrow_cast::display::ArrayFormatter::try_new(array.as_ref(), &options)?;
    let source_nulls = array.nulls();
    let len = array.len();
    if crate::execution::expression_memory::trace_enabled() {
        eprintln!(
            "[reserved-expression] {}",
            serde_json::json!({"event":"integer_string","rows":len,"type":format!("{:?}",array.data_type())})
        );
    }
    let count = len
        .checked_add(1)
        .ok_or_else(|| QueryError::Execution("string offset count overflow".into()))?;
    let mut offsets = ReservedBufferBuilder::<i32>::with_capacity(pool, count)?;
    let mut bytes = ReservedBufferBuilder::<u8>::new(pool)?;
    let validity = if array.null_count() > 0 {
        Some(nulls(pool, len, |i| array.is_valid(i))?)
    } else {
        None
    };
    offsets.extend_from_slice(&[0])?;
    for i in 0..len {
        if source_nulls.is_none_or(|n| n.is_valid(i)) {
            let mut writer = TextWriter {
                bytes: &mut bytes,
                error: None,
            };
            if let Err(format_error) = formatter.value(i).write(&mut writer) {
                return Err(writer.error.unwrap_or_else(|| format_error.into()));
            }
        }
        let offset = i32::try_from(bytes.as_slice().len())
            .map_err(|_| QueryError::Execution("string offset exceeds Int32".into()))?;
        offsets.extend_from_slice(&[offset])?;
    }
    Ok(Arc::new(StringArray::try_new(
        OffsetBuffer::new(ScalarBuffer::new(offsets.finish(), 0, count)),
        bytes.finish(),
        validity,
    )?))
}
pub(super) fn cast(
    pool: &MemoryPool,
    array: &ArrayRef,
    target: &DataType,
) -> Option<Result<ArrayRef>> {
    if target != &DataType::Utf8 {
        return None;
    }
    match array.data_type() {
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Some(integer_text(pool, array)),
        _ => None,
    }
}
