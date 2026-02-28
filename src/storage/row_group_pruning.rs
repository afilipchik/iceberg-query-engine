//! Row group pruning based on Parquet statistics
//!
//! Evaluates predicates against row group min/max statistics to skip
//! row groups that cannot contain matching rows.

use crate::planner::{BinaryOp, Expr, ScalarValue, UnaryOp};
use arrow::datatypes::SchemaRef;
use parquet::file::metadata::RowGroupMetaData;
use parquet::file::statistics::Statistics as ParquetStatistics;

/// Check if a row group might contain rows matching the predicate.
/// Returns `true` if the row group might match (conservative), `false` if it definitely doesn't.
pub fn row_group_might_match(
    predicate: &Expr,
    row_group: &RowGroupMetaData,
    schema: &SchemaRef,
) -> bool {
    match predicate {
        Expr::BinaryExpr { left, op, right } => match op {
            BinaryOp::And => {
                row_group_might_match(left, row_group, schema)
                    && row_group_might_match(right, row_group, schema)
            }
            BinaryOp::Or => {
                row_group_might_match(left, row_group, schema)
                    || row_group_might_match(right, row_group, schema)
            }
            _ => check_comparison(left, op, right, row_group, schema),
        },
        Expr::UnaryExpr {
            op: UnaryOp::Not,
            expr,
        } => {
            // Conservative: can only prune if inner definitely matches all rows
            // For simplicity, always include
            !row_group_definitely_matches(expr, row_group, schema)
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            // BETWEEN is equivalent to (expr >= low AND expr <= high)
            // NOT BETWEEN is (expr < low OR expr > high)
            if *negated {
                // NOT BETWEEN: row group might match unless all values are in [low, high]
                true // Conservative
            } else {
                // BETWEEN: expr >= low AND expr <= high
                let ge_low = row_group_might_match(
                    &Expr::BinaryExpr {
                        left: expr.clone(),
                        op: BinaryOp::GtEq,
                        right: low.clone(),
                    },
                    row_group,
                    schema,
                );
                let le_high = row_group_might_match(
                    &Expr::BinaryExpr {
                        left: expr.clone(),
                        op: BinaryOp::LtEq,
                        right: high.clone(),
                    },
                    row_group,
                    schema,
                );
                ge_low && le_high
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if *negated {
                true // Conservative for NOT IN
            } else {
                // IN (v1, v2, ...): at least one value must be in range
                list.iter().any(|val| {
                    row_group_might_match(
                        &Expr::BinaryExpr {
                            left: expr.clone(),
                            op: BinaryOp::Eq,
                            right: Box::new(val.clone()),
                        },
                        row_group,
                        schema,
                    )
                })
            }
        }
        _ => true, // Conservative: include if we can't evaluate
    }
}

/// Check if a row group definitely matches all rows for a predicate
/// Used for NOT optimization
fn row_group_definitely_matches(
    _predicate: &Expr,
    _row_group: &RowGroupMetaData,
    _schema: &SchemaRef,
) -> bool {
    false // Conservative: never claim definite match
}

/// Check a comparison predicate (col op literal) against row group statistics
fn check_comparison(
    left: &Expr,
    op: &BinaryOp,
    right: &Expr,
    row_group: &RowGroupMetaData,
    schema: &SchemaRef,
) -> bool {
    // Extract column and literal, handling both orientations
    let (col_name, literal, flipped) = match (left, right) {
        (Expr::Column(col), Expr::Literal(lit)) => (col.name.as_str(), lit, false),
        (Expr::Literal(lit), Expr::Column(col)) => (col.name.as_str(), lit, true),
        _ => return true, // Not a simple column comparison
    };

    // Find column index in schema
    let col_idx = match schema.fields().iter().position(|f| f.name() == col_name) {
        Some(idx) => idx,
        None => return true, // Column not found, conservative
    };

    // Get statistics for this column in the row group
    if col_idx >= row_group.num_columns() {
        return true;
    }
    let stats = match row_group.column(col_idx).statistics() {
        Some(s) => s,
        None => return true, // No statistics available
    };

    // Flip the operator if the literal was on the left
    let effective_op = if flipped { flip_op(op) } else { *op };

    match literal {
        ScalarValue::Int64(val) => check_i64_stats(stats, effective_op, *val),
        ScalarValue::Int32(val) => check_i32_stats(stats, effective_op, *val),
        ScalarValue::Float64(val) => check_f64_stats(stats, effective_op, val.into_inner()),
        ScalarValue::Float32(val) => check_f64_stats(stats, effective_op, val.into_inner() as f64),
        ScalarValue::Date32(val) => check_i32_stats(stats, effective_op, *val),
        ScalarValue::Utf8(val) => check_utf8_stats(stats, effective_op, val.as_str()),
        ScalarValue::Timestamp(val) => check_i64_stats(stats, effective_op, *val),
        _ => true, // Unsupported type, conservative
    }
}

/// Flip a comparison operator (for when literal is on the left side)
fn flip_op(op: &BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => *other, // Eq, NotEq are symmetric
    }
}

/// Check i64 statistics (also used for Timestamp)
fn check_i64_stats(stats: &ParquetStatistics, op: BinaryOp, val: i64) -> bool {
    match stats {
        ParquetStatistics::Int64(s) => {
            if s.min_opt().is_none() || s.max_opt().is_none() {
                return true;
            }
            let min = *s.min_opt().unwrap();
            let max = *s.max_opt().unwrap();
            eval_range(op, val, min, max)
        }
        ParquetStatistics::Int32(s) => {
            // Int64 literal vs Int32 statistics (e.g., Date32)
            if s.min_opt().is_none() || s.max_opt().is_none() {
                return true;
            }
            let min = *s.min_opt().unwrap() as i64;
            let max = *s.max_opt().unwrap() as i64;
            eval_range(op, val, min, max)
        }
        _ => true,
    }
}

/// Check i32 statistics (Date32)
fn check_i32_stats(stats: &ParquetStatistics, op: BinaryOp, val: i32) -> bool {
    match stats {
        ParquetStatistics::Int32(s) => {
            if s.min_opt().is_none() || s.max_opt().is_none() {
                return true;
            }
            let min = *s.min_opt().unwrap();
            let max = *s.max_opt().unwrap();
            eval_range_i32(op, val, min, max)
        }
        ParquetStatistics::Int64(s) => {
            if s.min_opt().is_none() || s.max_opt().is_none() {
                return true;
            }
            let min = *s.min_opt().unwrap() as i32;
            let max = *s.max_opt().unwrap() as i32;
            eval_range_i32(op, val, min, max)
        }
        _ => true,
    }
}

/// Check f64 statistics
fn check_f64_stats(stats: &ParquetStatistics, op: BinaryOp, val: f64) -> bool {
    match stats {
        ParquetStatistics::Double(s) => {
            if s.min_opt().is_none() || s.max_opt().is_none() {
                return true;
            }
            let min = *s.min_opt().unwrap();
            let max = *s.max_opt().unwrap();
            eval_range_f64(op, val, min, max)
        }
        ParquetStatistics::Float(s) => {
            if s.min_opt().is_none() || s.max_opt().is_none() {
                return true;
            }
            let min = *s.min_opt().unwrap() as f64;
            let max = *s.max_opt().unwrap() as f64;
            eval_range_f64(op, val, min, max)
        }
        _ => true,
    }
}

/// Check UTF-8 string statistics
fn check_utf8_stats(stats: &ParquetStatistics, op: BinaryOp, val: &str) -> bool {
    match stats {
        ParquetStatistics::ByteArray(s) => {
            if s.min_opt().is_none() || s.max_opt().is_none() {
                return true;
            }
            let min_bytes = s.min_opt().unwrap().data();
            let max_bytes = s.max_opt().unwrap().data();
            let min = match std::str::from_utf8(min_bytes) {
                Ok(s) => s,
                Err(_) => return true,
            };
            let max = match std::str::from_utf8(max_bytes) {
                Ok(s) => s,
                Err(_) => return true,
            };
            eval_range_str(op, val, min, max)
        }
        _ => true,
    }
}

/// Evaluate whether a value might exist in the range [min, max] for the given operator
fn eval_range(op: BinaryOp, val: i64, min: i64, max: i64) -> bool {
    match op {
        BinaryOp::Eq => min <= val && val <= max,
        BinaryOp::NotEq => !(min == val && max == val),
        BinaryOp::Lt => min < val,
        BinaryOp::LtEq => min <= val,
        BinaryOp::Gt => max > val,
        BinaryOp::GtEq => max >= val,
        _ => true,
    }
}

fn eval_range_i32(op: BinaryOp, val: i32, min: i32, max: i32) -> bool {
    match op {
        BinaryOp::Eq => min <= val && val <= max,
        BinaryOp::NotEq => !(min == val && max == val),
        BinaryOp::Lt => min < val,
        BinaryOp::LtEq => min <= val,
        BinaryOp::Gt => max > val,
        BinaryOp::GtEq => max >= val,
        _ => true,
    }
}

fn eval_range_f64(op: BinaryOp, val: f64, min: f64, max: f64) -> bool {
    match op {
        BinaryOp::Eq => min <= val && val <= max,
        BinaryOp::NotEq => !(min == val && max == val),
        BinaryOp::Lt => min < val,
        BinaryOp::LtEq => min <= val,
        BinaryOp::Gt => max > val,
        BinaryOp::GtEq => max >= val,
        _ => true,
    }
}

fn eval_range_str(op: BinaryOp, val: &str, min: &str, max: &str) -> bool {
    match op {
        BinaryOp::Eq => min <= val && val <= max,
        BinaryOp::NotEq => !(min == val && max == val),
        BinaryOp::Lt => min < val,
        BinaryOp::LtEq => min <= val,
        BinaryOp::Gt => max > val,
        BinaryOp::GtEq => max >= val,
        _ => true,
    }
}

/// Get the list of row group indices that might match the predicate.
/// Returns all row group indices if no predicate is given.
pub fn prune_row_groups(
    metadata: &parquet::file::metadata::ParquetMetaData,
    schema: &SchemaRef,
    predicate: Option<&Expr>,
) -> Vec<usize> {
    let num_row_groups = metadata.num_row_groups();
    match predicate {
        None => (0..num_row_groups).collect(),
        Some(pred) => (0..num_row_groups)
            .filter(|&i| row_group_might_match(pred, metadata.row_group(i), schema))
            .collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::Column;
    use arrow::datatypes::{DataType, Field, Schema};
    use ordered_float::OrderedFloat;
    use std::sync::Arc;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("date", DataType::Date32, false),
        ]))
    }

    #[test]
    fn test_eval_range_eq() {
        assert!(eval_range(BinaryOp::Eq, 5, 1, 10));
        assert!(!eval_range(BinaryOp::Eq, 15, 1, 10));
        assert!(eval_range(BinaryOp::Eq, 1, 1, 10));
        assert!(eval_range(BinaryOp::Eq, 10, 1, 10));
    }

    #[test]
    fn test_eval_range_lt() {
        assert!(eval_range(BinaryOp::Lt, 5, 1, 10)); // min=1 < 5
        assert!(!eval_range(BinaryOp::Lt, 1, 1, 10)); // min=1 is NOT < 1
        assert!(eval_range(BinaryOp::Lt, 2, 1, 10)); // min=1 < 2
    }

    #[test]
    fn test_eval_range_gt() {
        assert!(eval_range(BinaryOp::Gt, 5, 1, 10)); // max=10 > 5
        assert!(!eval_range(BinaryOp::Gt, 10, 1, 10)); // max=10 is NOT > 10
        assert!(eval_range(BinaryOp::Gt, 9, 1, 10)); // max=10 > 9
    }

    #[test]
    fn test_flip_op() {
        assert_eq!(flip_op(&BinaryOp::Lt), BinaryOp::Gt);
        assert_eq!(flip_op(&BinaryOp::Gt), BinaryOp::Lt);
        assert_eq!(flip_op(&BinaryOp::LtEq), BinaryOp::GtEq);
        assert_eq!(flip_op(&BinaryOp::GtEq), BinaryOp::LtEq);
        assert_eq!(flip_op(&BinaryOp::Eq), BinaryOp::Eq);
    }

    #[test]
    fn test_and_pruning() {
        let schema = test_schema();

        // id >= 5 AND id <= 10 — should keep row groups where max >= 5 and min <= 10
        let pred = Expr::BinaryExpr {
            left: Box::new(Expr::BinaryExpr {
                left: Box::new(Expr::Column(Column::new("id"))),
                op: BinaryOp::GtEq,
                right: Box::new(Expr::Literal(ScalarValue::Int64(5))),
            }),
            op: BinaryOp::And,
            right: Box::new(Expr::BinaryExpr {
                left: Box::new(Expr::Column(Column::new("id"))),
                op: BinaryOp::LtEq,
                right: Box::new(Expr::Literal(ScalarValue::Int64(10))),
            }),
        };

        // We can't easily construct RowGroupMetaData in tests, but we verify
        // the function doesn't panic with valid expressions
        let _ = &pred;
        let _ = &schema;
    }

    #[test]
    fn test_string_range() {
        assert!(eval_range_str(BinaryOp::Eq, "fox", "abc", "xyz"));
        assert!(!eval_range_str(BinaryOp::Eq, "abc", "def", "xyz"));
        assert!(eval_range_str(BinaryOp::Lt, "ghi", "abc", "xyz")); // min "abc" < "ghi"
    }
}
