//! One operand/result type contract shared by planning and Arrow evaluation.
use crate::error::{QueryError, Result};
use crate::planner::BinaryOp;
use arrow::array::{new_empty_array, ArrayRef};
use arrow::compute::{cast_with_options, kernels::numeric, CastOptions};
use arrow::datatypes::DataType;

/// Conversion failures are errors unless SQL explicitly requested TRY_CAST.
pub(crate) fn cast_array(
    array: &ArrayRef,
    target: &DataType,
    mode: super::CastMode,
) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(array.clone());
    }
    if let Some(pool) = crate::execution::expression_memory::expression_pool() {
        if let Some(result) = super::reserved_cast::cast(&pool, array, target, mode) {
            return result;
        }
        if let Some(result) = super::reserved_numeric::cast(&pool, array, target) {
            return result;
        }
    }
    Ok(cast_with_options(
        array,
        target,
        &CastOptions {
            safe: mode == super::CastMode::Try,
            ..Default::default()
        },
    )?)
}

pub(crate) fn cast_strict(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    cast_array(array, target, super::CastMode::Strict)
}

fn integer_domain(t: &DataType) -> Option<(bool, u8)> {
    use DataType::*;
    Some(match t {
        Int8 => (true, 8),
        Int16 => (true, 16),
        Int32 => (true, 32),
        Int64 => (true, 64),
        UInt8 => (false, 8),
        UInt16 => (false, 16),
        UInt32 => (false, 32),
        UInt64 => (false, 64),
        _ => return None,
    })
}

fn integer_digits(t: &DataType) -> Option<u8> {
    use DataType::*;
    match t {
        Int8 | UInt8 => Some(3),
        Int16 | UInt16 => Some(5),
        Int32 | UInt32 => Some(10),
        Int64 => Some(19),
        UInt64 => Some(20),
        _ => None,
    }
}

pub(crate) fn common_type(left: &DataType, right: &DataType) -> Result<DataType> {
    use DataType::*;
    if left == right {
        return Ok(left.clone());
    }
    // Encodings do not change a value's SQL type.
    if let Dictionary(_, value) = left {
        return common_type(value, right);
    }
    if let Dictionary(_, value) = right {
        return common_type(left, value);
    }
    if let (Some((ls, lb)), Some((rs, rb))) = (integer_domain(left), integer_domain(right)) {
        if ls == rs {
            return Ok(match (ls, lb.max(rb)) {
                (true, 8) => Int8,
                (true, 16) => Int16,
                (true, 32) => Int32,
                (true, _) => Int64,
                (false, 8) => UInt8,
                (false, 16) => UInt16,
                (false, 32) => UInt32,
                (false, _) => UInt64,
            });
        }
        let signed_bits = if ls { lb } else { rb };
        let unsigned_bits = if ls { rb } else { lb };
        let needed = signed_bits.max(unsigned_bits + 1);
        return Ok(match needed {
            0..=8 => Int8,
            9..=16 => Int16,
            17..=32 => Int32,
            33..=64 => Int64,
            _ => Decimal128(20, 0),
        });
    }
    match (left, right) {
        (Null, t) | (t, Null) => Ok(t.clone()),
        (Timestamp(left_unit, left_zone), Timestamp(right_unit, right_zone)) => {
            // Coerce toward greater precision, never truncate a comparison's
            // subsecond values. Arrow's strict cast reports tick overflow.
            let unit = left_unit.max(right_unit).clone();
            // Identical zones retain display metadata. Mixed aware/naive or
            // differently named zones use this engine's UTC session semantics.
            let timezone = if left_zone == right_zone {
                left_zone.clone()
            } else {
                Some(std::sync::Arc::<str>::from("UTC"))
            };
            Ok(Timestamp(unit, timezone))
        }
        (Float64 | Float32, _) | (_, Float64 | Float32) => Ok(Float64),
        (Decimal128(_, _), _) | (_, Decimal128(_, _)) => {
            let decimal = |t: &DataType| match t {
                Decimal128(p, s) => Some((*p, *s)),
                _ => integer_digits(t).map(|p| (p, 0)),
            };
            let ((p1, s1), (p2, s2)) = decimal(left)
                .zip(decimal(right))
                .ok_or_else(|| QueryError::Type(format!("Cannot coerce {left:?} and {right:?}")))?;
            let scale = s1.max(s2);
            let precision = (i16::from(p1) - i16::from(s1)).max(i16::from(p2) - i16::from(s2))
                + i16::from(scale);
            if precision > 38 {
                return Err(QueryError::Type(
                    "decimal coercion exceeds 38 digits".into(),
                ));
            }
            Ok(Decimal128(precision.max(1) as u8, scale))
        }
        (Int64, _) | (_, Int64) => Ok(Int64),
        (Int32, _) | (_, Int32) => Ok(Int64),
        (Int16, _) | (_, Int16) => Ok(Int32),
        (Int8, _) | (_, Int8) => Ok(Int16),
        (UInt64 | UInt32, _) | (_, UInt64 | UInt32) => Ok(UInt64),
        (Date32, Utf8) | (Utf8, Date32) => Ok(Date32),
        (Utf8, _) | (_, Utf8) => Ok(Utf8),
        _ => Err(QueryError::Type(format!(
            "Cannot coerce {left:?} and {right:?}"
        ))),
    }
}

fn operands(op: BinaryOp, left: &DataType, right: &DataType) -> Result<(DataType, DataType)> {
    use DataType::*;
    let decimal = matches!(left, Decimal128(..)) || matches!(right, Decimal128(..));
    if decimal && op == BinaryOp::Divide {
        // The SQL / result for exact decimals is floating; SUM remains exact.
        return Ok((Float64, Float64));
    }
    if decimal && !matches!(left, Float32 | Float64) && !matches!(right, Float32 | Float64) {
        let as_decimal = |t: &DataType, other: &DataType| -> Result<DataType> {
            match t {
                Decimal128(..) => Ok(t.clone()),
                Null => Ok(other.clone()),
                _ => integer_digits(t)
                    .map(|p| Decimal128(p, 0))
                    .ok_or_else(|| QueryError::Type(format!("Invalid decimal operand {t:?}"))),
            }
        };
        // Arithmetic kernels handle differing decimal scales themselves.
        // Casting both sides to a common integer or scale loses information.
        return Ok((as_decimal(left, right)?, as_decimal(right, left)?));
    }
    let common = common_type(left, right)?;
    // Mixed signed/unsigned integers can promote to an exact decimal domain.
    // Division must use the same floating contract as explicit decimal inputs,
    // before either metadata inference or runtime kernel dispatch.
    if op == BinaryOp::Divide && matches!(common, Decimal128(..)) {
        return Ok((Float64, Float64));
    }
    Ok((common.clone(), common))
}

fn validate_arithmetic_domain(op: BinaryOp, lt: &DataType, rt: &DataType) -> Result<()> {
    if let (DataType::Decimal128(lp, ls), DataType::Decimal128(rp, rs)) = (lt, rt) {
        let (ls, rs) = (i16::from(*ls), i16::from(*rs));
        // Arrow 58 computes some metadata in i8 and saturates multiply
        // scales; guard domains where that would change the represented value.
        let invalid = if op == BinaryOp::Multiply {
            !(-128..=38).contains(&(ls + rs))
        } else {
            i16::from(*lp) - ls > 127 || i16::from(*rp) - rs > 127 || (ls - rs).abs() > 38
        };
        if invalid {
            return Err(QueryError::Type(
                "decimal arithmetic scale exceeds the checked kernel domain".into(),
            ));
        }
    }
    Ok(())
}

fn arrow_arithmetic(op: BinaryOp, l: &ArrayRef, r: &ArrayRef) -> Result<ArrayRef> {
    Ok(match op {
        BinaryOp::Add => numeric::add(l, r)?,
        BinaryOp::Subtract => numeric::sub(l, r)?,
        BinaryOp::Multiply => numeric::mul(l, r)?,
        BinaryOp::Divide => numeric::div(l, r)?,
        BinaryOp::Modulo => numeric::rem(l, r)?,
        _ => return Err(QueryError::Internal("non-arithmetic operator".into())),
    })
}

fn arithmetic_operands(
    op: BinaryOp,
    left: &ArrayRef,
    right: &ArrayRef,
) -> Result<(ArrayRef, ArrayRef)> {
    let (lt, rt) = operands(op, left.data_type(), right.data_type())?;
    validate_arithmetic_domain(op, &lt, &rt)?;

    let l = if &lt == left.data_type() {
        left.clone()
    } else {
        cast_strict(left, &lt)?
    };
    let r = if &rt == right.data_type() {
        right.clone()
    } else {
        cast_strict(right, &rt)?
    };
    Ok((l, r))
}

/// Scalar flags describe representation, never inferred uniqueness or values.
/// Coerce singleton operands once; the decimal kernel still emits `rows` values.
pub(crate) fn scalar_decimal_arithmetic(
    pool: &crate::execution::SharedMemoryPool,
    op: BinaryOp,
    left: &ArrayRef,
    right: &ArrayRef,
    left_scalar: bool,
    right_scalar: bool,
    rows: usize,
) -> Result<ArrayRef> {
    if left.len() != if left_scalar { 1 } else { rows }
        || right.len() != if right_scalar { 1 } else { rows }
    {
        return Err(QueryError::Execution(
            "scalar arithmetic operand extent mismatch".into(),
        ));
    }
    // An explicit pool API must also bind coercion allocations to that pool,
    // even when invoked inside a different or absent expression scope.
    let (l, r) = crate::execution::expression_memory::with_expression_pool(pool, || {
        arithmetic_operands(op, left, right)
    })?;
    super::reserved_decimal::arithmetic_broadcast(pool, op, &l, &r, left_scalar, right_scalar, rows)
        .ok_or_else(|| QueryError::Internal("scalar decimal capability mismatch".into()))?
}

pub(crate) fn arithmetic(op: BinaryOp, left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
    let (l, r) = arithmetic_operands(op, left, right)?;
    if let Some(pool) = crate::execution::expression_memory::expression_pool() {
        if let Some(result) = super::reserved_decimal::arithmetic(&pool, op, &l, &r) {
            return result;
        }
        if let Some(result) = super::reserved_numeric::arithmetic(&pool, op, &l, &r) {
            return result;
        }
    }
    let result = arrow_arithmetic(op, &l, &r)?;
    if let DataType::Decimal128(precision, _) = result.data_type() {
        use arrow::array::Decimal128Array;
        let decimals = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
        for coefficient in decimals.iter().flatten() {
            crate::planner::DecimalValue::validate_precision(coefficient, *precision)?;
        }
    }
    Ok(result)
}

pub(crate) fn arithmetic_type(op: BinaryOp, left: &DataType, right: &DataType) -> Result<DataType> {
    // Derive decimal precision/scale with the exact same pinned Arrow kernels
    // as execution. Empty arrays perform no data execution during planning.
    let (l, r) = operands(op, left, right)?;
    if matches!(l, DataType::Decimal128(..)) || matches!(r, DataType::Decimal128(..)) {
        // Metadata inference must not enter the caller's query allocator.
        validate_arithmetic_domain(op, &l, &r)?;
        return Ok(
            arrow_arithmetic(op, &new_empty_array(&l), &new_empty_array(&r))?
                .data_type()
                .clone(),
        );
    }
    Ok(l)
}

/// SQL float comparisons: signed zeros are equal; all NaNs compare equal and
/// greater than every non-NaN value. Keep interpreter, folding and compiled
/// predicates on this contract rather than Arrow's IEEE totalOrder.
#[inline(always)]
pub(crate) fn sql_float_compare(left: f64, op: BinaryOp, right: f64) -> bool {
    match op {
        BinaryOp::Eq => left == right || (left.is_nan() && right.is_nan()),
        BinaryOp::NotEq => !(left == right || (left.is_nan() && right.is_nan())),
        BinaryOp::Lt => left < right || (!left.is_nan() && right.is_nan()),
        BinaryOp::LtEq => left <= right || right.is_nan(),
        BinaryOp::Gt => left > right || (left.is_nan() && !right.is_nan()),
        BinaryOp::GtEq => left >= right || left.is_nan(),
        _ => unreachable!("SQL float comparison requires a comparison operator"),
    }
}

/// Select an extremum using the same SQL ordering as predicates. Keep the
/// existing value on ties (including signed zero and different NaN payloads).
#[inline(always)]
pub(crate) fn sql_float_min(current: f64, incoming: f64) -> f64 {
    if sql_float_compare(incoming, BinaryOp::Lt, current) {
        incoming
    } else {
        current
    }
}

#[inline(always)]
pub(crate) fn sql_float_max(current: f64, incoming: f64) -> f64 {
    if sql_float_compare(incoming, BinaryOp::Gt, current) {
        incoming
    } else {
        current
    }
}

/// Float-only SQL comparison, including scalar broadcasts. None delegates
/// non-float types to the existing typed Arrow kernels. No normalized input
/// copies are allocated; the only allocation is the output Boolean array.
pub(crate) fn compare_float_arrays(
    left: &ArrayRef,
    left_scalar: bool,
    op: BinaryOp,
    right: &ArrayRef,
    right_scalar: bool,
    rows: usize,
) -> Option<Result<arrow::array::BooleanArray>> {
    use arrow::array::{Array, BooleanArray, Float32Array, Float64Array};
    if !matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
    ) || !matches!(left.data_type(), DataType::Float32 | DataType::Float64)
        || left.data_type() != right.data_type()
    {
        return None;
    }
    if left.len() != if left_scalar { 1 } else { rows }
        || right.len() != if right_scalar { 1 } else { rows }
    {
        return Some(Err(QueryError::Internal(
            "float comparison operand shape mismatch".into(),
        )));
    }
    use arrow::buffer::{BooleanBuffer, NullBuffer};
    if (left_scalar && left.is_null(0)) || (right_scalar && right.is_null(0)) {
        return Some(Ok(BooleanArray::new_null(rows)));
    }
    let nulls = match (left_scalar, right_scalar) {
        (true, true) => None,
        (true, false) => right.nulls().cloned(),
        (false, true) => left.nulls().cloned(),
        (false, false) => NullBuffer::union(left.nulls(), right.nulls()),
    };
    macro_rules! compare {
        ($array:ty) => {{
            let a = left
                .as_any()
                .downcast_ref::<$array>()
                .expect("checked float type");
            let b = right
                .as_any()
                .downcast_ref::<$array>()
                .expect("checked float type");
            // Resolve operator, broadcast shape and validity outside the row
            // loop. Build packed values directly, retaining Arrow validity.
            macro_rules! values {
                ($op:ident) => {{
                    match (left_scalar, right_scalar) {
                        (true, true) => {
                            if sql_float_compare(
                                a.value(0) as f64,
                                BinaryOp::$op,
                                b.value(0) as f64,
                            ) {
                                BooleanBuffer::new_set(rows)
                            } else {
                                BooleanBuffer::new_unset(rows)
                            }
                        }
                        (true, false) => {
                            let scalar = a.value(0) as f64;
                            BooleanBuffer::collect_bool(rows, |i| {
                                sql_float_compare(scalar, BinaryOp::$op, b.value(i) as f64)
                            })
                        }
                        (false, true) => {
                            let scalar = b.value(0) as f64;
                            BooleanBuffer::collect_bool(rows, |i| {
                                sql_float_compare(a.value(i) as f64, BinaryOp::$op, scalar)
                            })
                        }
                        (false, false) => BooleanBuffer::collect_bool(rows, |i| {
                            sql_float_compare(a.value(i) as f64, BinaryOp::$op, b.value(i) as f64)
                        }),
                    }
                }};
            }
            let values = match op {
                BinaryOp::Eq => values!(Eq),
                BinaryOp::NotEq => values!(NotEq),
                BinaryOp::Lt => values!(Lt),
                BinaryOp::LtEq => values!(LtEq),
                BinaryOp::Gt => values!(Gt),
                BinaryOp::GtEq => values!(GtEq),
                _ => unreachable!("comparison checked above"),
            };
            BooleanArray::new(values, nulls)
        }};
    }
    Some(Ok(match left.data_type() {
        DataType::Float32 => compare!(Float32Array),
        DataType::Float64 => compare!(Float64Array),
        _ => unreachable!(),
    }))
}

/// Canonical hash/dictionary representation for SQL float equality. Equal signed
/// zeros and distinct NaN payloads must enter the same bucket before equality
/// checks can establish a match. NULL is handled separately by key consumers.
#[inline(always)]
pub(crate) fn sql_float_key(value: f64) -> u64 {
    if value == 0.0 {
        0
    } else if value.is_nan() {
        f64::NAN.to_bits()
    } else {
        value.to_bits()
    }
}
