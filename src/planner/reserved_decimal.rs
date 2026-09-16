//! Decimal128 arithmetic writes directly into pre-admitted output buffers.
//! Operand coercion and checked scale-domain validation remain in numeric.rs.
use crate::{
    execution::{MemoryPool, MemoryReservation, ReservedBufferBuilder},
    planner::{BinaryOp, DecimalValue},
    QueryError, Result,
};
use arrow::{
    array::{Array, ArrayRef, Decimal128Array},
    buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer},
    datatypes::DataType,
};
use arrow_array::ArrowNativeTypeOp;
use std::sync::Arc;

struct OutputBuffer {
    buffer: Buffer,
    _metadata: Arc<MemoryReservation>,
}
impl AsRef<[u8]> for OutputBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}
fn retain(buffer: Buffer, metadata: &Arc<MemoryReservation>) -> Buffer {
    Buffer::from(bytes::Bytes::from_owner(OutputBuffer {
        buffer,
        _metadata: metadata.clone(),
    }))
}
pub(super) fn arithmetic(
    pool: &MemoryPool,
    op: BinaryOp,
    left: &ArrayRef,
    right: &ArrayRef,
) -> Option<Result<ArrayRef>> {
    arithmetic_broadcast(pool, op, left, right, false, false, left.len())
}

pub(super) fn arithmetic_broadcast(
    pool: &MemoryPool,
    op: BinaryOp,
    left: &ArrayRef,
    right: &ArrayRef,
    left_scalar: bool,
    right_scalar: bool,
    len: usize,
) -> Option<Result<ArrayRef>> {
    let (DataType::Decimal128(lp, ls), DataType::Decimal128(rp, rs)) =
        (left.data_type(), right.data_type())
    else {
        return None;
    };
    Some((|| {
        let l = left
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| QueryError::Type("decimal left representation mismatch".into()))?;
        let r = right
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .ok_or_else(|| QueryError::Type("decimal right representation mismatch".into()))?;
        if l.len() != if left_scalar { 1 } else { len }
            || r.len() != if right_scalar { 1 } else { len }
        {
            return Err(QueryError::Execution(
                "arithmetic array lengths differ".into(),
            ));
        }
        // Same pinned Arrow/Hive result metadata, evaluated in wider integers.
        let (lp, rp, ls, rs) = (
            i16::from(*lp),
            i16::from(*rp),
            i16::from(*ls),
            i16::from(*rs),
        );
        let scale = if op == BinaryOp::Multiply {
            ls + rs
        } else {
            ls.max(rs)
        };
        let precision = match op {
            BinaryOp::Add | BinaryOp::Subtract => (scale + (lp - ls).max(rp - rs) + 1).min(38),
            BinaryOp::Multiply => (lp + rp + 1).min(38),
            BinaryOp::Modulo => (scale + (lp - ls).min(rp - rs)).min(38),
            _ => {
                return Err(QueryError::Internal(
                    "non-decimal arithmetic operator".into(),
                ))
            }
        };
        let precision = u8::try_from(precision)
            .map_err(|_| QueryError::Type("decimal precision out of range".into()))?;
        let scale = i8::try_from(scale)
            .map_err(|_| QueryError::Type("decimal scale out of range".into()))?;
        arrow_array::types::validate_decimal_precision_and_scale::<arrow::datatypes::Decimal128Type>(
            precision, scale,
        )?;
        let (lm, rm) = if op == BinaryOp::Multiply {
            (1, 1)
        } else {
            (
                10i128.pow_checked((i16::from(scale) - ls) as u32)?,
                10i128.pow_checked((i16::from(scale) - rs) as u32)?,
            )
        };
        // Small fixed allowance covers output array/buffer owner bookkeeping.
        // The same owner survives extraction of either values or validity.
        let metadata = Arc::new(pool.allocate(1024)?);
        let mut values = ReservedBufferBuilder::<i128>::with_capacity(pool, len)?;
        let nulls = if l.null_count() + r.null_count() > 0 {
            let mut validity = ReservedBufferBuilder::<u8>::with_capacity(pool, len.div_ceil(8))?;
            for start in (0..len).step_by(8) {
                let mut bits = 0;
                for bit in 0..8.min(len - start) {
                    let row = start + bit;
                    bits |= u8::from(
                        l.is_valid(if left_scalar { 0 } else { row })
                            && r.is_valid(if right_scalar { 0 } else { row }),
                    ) << bit;
                }
                validity.extend_reserved(1, [bits])?;
            }
            Some(NullBuffer::new(BooleanBuffer::new(
                retain(validity.finish(), &metadata),
                0,
                len,
            )))
        } else {
            None
        };
        macro_rules! fill {
            ($method:ident, $scaled:expr) => {{
                values.try_extend_reserved(
                    len,
                    (0..len).map(|row| {
                        if nulls.as_ref().is_some_and(|n| n.is_null(row)) {
                            return Ok(0);
                        }
                        let li = if left_scalar { 0 } else { row };
                        let ri = if right_scalar { 0 } else { row };
                        let (a, b) = if $scaled {
                            (l.value(li).mul_checked(lm)?, r.value(ri).mul_checked(rm)?)
                        } else {
                            (l.value(li), r.value(ri))
                        };
                        let value = a.$method(b)?;
                        DecimalValue::validate_precision(value, precision)?;
                        Ok(value)
                    }),
                )?;
            }};
        }
        match op {
            BinaryOp::Add => fill!(add_checked, true),
            BinaryOp::Subtract => fill!(sub_checked, true),
            BinaryOp::Multiply => fill!(mul_checked, false),
            BinaryOp::Modulo => fill!(mod_checked, true),
            _ => unreachable!(),
        }
        Ok(Arc::new(
            Decimal128Array::new(
                ScalarBuffer::new(retain(values.finish(), &metadata), 0, len),
                nulls,
            )
            .with_precision_and_scale(precision, scale)?,
        ) as ArrayRef)
    })())
}
#[cfg(test)]
mod tests;
