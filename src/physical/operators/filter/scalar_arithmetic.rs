//! Keep exact arithmetic literals scalar through admitted decimal coercion.
use super::*;

fn supported(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Modulo
    )
}

fn literal(expr: &Expr) -> Option<&ScalarValue> {
    match expr {
        Expr::Literal(value) => Some(value),
        Expr::Alias { expr, .. } => literal(expr),
        _ => None,
    }
}

// Structural capability only: no evaluation, temporary arrays, statistics, or
// value sampling. Exact arithmetic with a decimal leaf stays decimal for these
// operators. Casts/functions/CASE/dictionaries and other domains retain the old
// evaluator, including their evaluation and error order.
fn exact_kind(batch: &RecordBatch, expr: &Expr) -> Option<bool> {
    let kind = |ty: &DataType| {
        if matches!(ty, DataType::Decimal128(..)) {
            Some(true)
        } else if ty.is_integer() {
            Some(false)
        } else {
            None
        }
    };
    match expr {
        Expr::Column(column) => kind(
            batch
                .column(find_column_index(batch, column).ok()?)
                .data_type(),
        ),
        Expr::Literal(value) => kind(&value.data_type()),
        Expr::Alias { expr, .. } => exact_kind(batch, expr),
        Expr::BinaryExpr { left, op, right } if supported(*op) => {
            Some(exact_kind(batch, left)? | exact_kind(batch, right)?)
        }
        _ => None,
    }
}

pub(super) fn evaluate(
    batch: &RecordBatch,
    left: &Expr,
    op: BinaryOp,
    right: &Expr,
    mut evaluate_array: impl FnMut(&Expr) -> Result<ArrayRef>,
) -> Option<Result<ArrayRef>> {
    if !supported(op) || (literal(left).is_none() && literal(right).is_none()) {
        return None;
    }
    if !(exact_kind(batch, left)? | exact_kind(batch, right)?) {
        return None;
    }
    let pool = crate::execution::expression_memory::expression_pool()?;
    Some((|| {
        let rows = batch.num_rows();
        // Empty input evaluates zero-length operands, preserving the existing
        // absence of value conversion/overflow on rows that do not exist.
        let left_scalar = rows != 0 && literal(left).is_some();
        let right_scalar = rows != 0 && literal(right).is_some();
        let l = if left_scalar {
            scalar_to_array(literal(left).unwrap(), 1)?
        } else {
            evaluate_array(left)?
        };
        let r = if right_scalar {
            scalar_to_array(literal(right).unwrap(), 1)?
        } else {
            evaluate_array(right)?
        };
        crate::planner::numeric::scalar_decimal_arithmetic(
            &pool,
            op,
            &l,
            &r,
            left_scalar,
            right_scalar,
            rows,
        )
    })())
}
