//! Constant folding optimization rule

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::planner::{BinaryOp, Expr, LogicalPlan, ScalarValue};
use ordered_float::OrderedFloat;
use std::sync::Arc;

/// Constant folding rule - evaluates constant expressions at compile time
pub struct ConstantFolding;

impl OptimizerRule for ConstantFolding {
    fn name(&self) -> &str {
        "ConstantFolding"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.optimize_plan(plan)
    }
}

impl ConstantFolding {
    fn optimize_plan(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // First optimize children
        let optimized_children: Result<Vec<Arc<LogicalPlan>>> = plan
            .children()
            .into_iter()
            .map(|child| self.optimize_plan(child).map(Arc::new))
            .collect();
        let optimized_children = optimized_children?;

        let plan = if !optimized_children.is_empty() {
            plan.with_new_children(optimized_children)
        } else {
            plan.clone()
        };

        // Then optimize expressions in this node
        match plan {
            LogicalPlan::Scan(mut node) => {
                node.filter = node.filter.as_ref().map(|e| self.fold_expr(e));
                Ok(LogicalPlan::Scan(node))
            }
            LogicalPlan::Sort(mut node) => {
                for sort in &mut node.order_by {
                    sort.expr = self.fold_expr(&sort.expr);
                }
                Ok(LogicalPlan::Sort(node))
            }
            LogicalPlan::Filter(mut node) => {
                node.predicate = self.fold_expr(&node.predicate);
                Ok(LogicalPlan::Filter(node))
            }
            LogicalPlan::Project(mut node) => {
                node.exprs = node.exprs.into_iter().map(|e| self.fold_expr(&e)).collect();
                Ok(LogicalPlan::Project(node))
            }
            LogicalPlan::Join(mut node) => {
                node.on = node
                    .on
                    .into_iter()
                    .map(|(l, r)| (self.fold_expr(&l), self.fold_expr(&r)))
                    .collect();
                if let Some(filter) = node.filter {
                    node.filter = Some(self.fold_expr(&filter));
                }
                Ok(LogicalPlan::Join(node))
            }
            LogicalPlan::Aggregate(mut node) => {
                node.group_by = node
                    .group_by
                    .into_iter()
                    .map(|e| self.fold_expr(&e))
                    .collect();
                node.aggregates = node
                    .aggregates
                    .into_iter()
                    .map(|e| self.fold_expr(&e))
                    .collect();
                Ok(LogicalPlan::Aggregate(node))
            }
            _ => Ok(plan),
        }
    }

    fn fold_expr(&self, expr: &Expr) -> Expr {
        match expr {
            Expr::BinaryExpr { left, op, right } => {
                let left = self.fold_expr(left);
                let right = self.fold_expr(right);

                // Try to evaluate if both sides are literals
                if let (Expr::Literal(l), Expr::Literal(r)) = (&left, &right) {
                    if let Some(result) = self.eval_binary(l, *op, r) {
                        return Expr::Literal(result);
                    }
                }

                // Simplify boolean expressions
                match op {
                    BinaryOp::And => {
                        // x AND true = x
                        if matches!(&right, Expr::Literal(ScalarValue::Boolean(true))) {
                            return left;
                        }
                        // true AND x = x
                        if matches!(&left, Expr::Literal(ScalarValue::Boolean(true))) {
                            return right;
                        }
                        // x AND false = false
                        if matches!(&left, Expr::Literal(ScalarValue::Boolean(false)))
                            || matches!(&right, Expr::Literal(ScalarValue::Boolean(false)))
                        {
                            return Expr::Literal(ScalarValue::Boolean(false));
                        }
                    }
                    BinaryOp::Or => {
                        // x OR false = x
                        if matches!(&right, Expr::Literal(ScalarValue::Boolean(false))) {
                            return left;
                        }
                        // false OR x = x
                        if matches!(&left, Expr::Literal(ScalarValue::Boolean(false))) {
                            return right;
                        }
                        // x OR true = true
                        if matches!(&left, Expr::Literal(ScalarValue::Boolean(true)))
                            || matches!(&right, Expr::Literal(ScalarValue::Boolean(true)))
                        {
                            return Expr::Literal(ScalarValue::Boolean(true));
                        }
                    }
                    _ => {}
                }

                Expr::BinaryExpr {
                    left: Box::new(left),
                    op: *op,
                    right: Box::new(right),
                }
            }
            Expr::UnaryExpr { op, expr } => {
                let folded = self.fold_expr(expr);
                Expr::UnaryExpr {
                    op: *op,
                    expr: Box::new(folded),
                }
            }
            Expr::Cast {
                expr,
                data_type,
                mode,
            } => {
                let folded = self.fold_expr(expr);
                if let Expr::Literal(value) = &folded {
                    if let Some(value) = self.eval_cast(value, data_type, *mode) {
                        return Expr::Literal(value);
                    }
                }
                Expr::Cast {
                    expr: Box::new(folded),
                    data_type: data_type.clone(),
                    mode: *mode,
                }
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(self.fold_expr(expr)),
                list: list.iter().map(|e| self.fold_expr(e)).collect(),
                negated: *negated,
            },
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => Expr::Between {
                expr: Box::new(self.fold_expr(expr)),
                low: Box::new(self.fold_expr(low)),
                high: Box::new(self.fold_expr(high)),
                negated: *negated,
            },
            Expr::Alias { expr, name } => Expr::Alias {
                expr: Box::new(self.fold_expr(expr)),
                name: name.clone(),
            },
            Expr::ScalarFunc { func, args } => Expr::ScalarFunc {
                func: func.clone(),
                args: args.iter().map(|a| self.fold_expr(a)).collect(),
            },
            Expr::Aggregate {
                func,
                args,
                distinct,
            } => Expr::Aggregate {
                func: *func,
                args: args.iter().map(|a| self.fold_expr(a)).collect(),
                distinct: *distinct,
            },
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => Expr::Case {
                operand: operand.as_ref().map(|e| Box::new(self.fold_expr(e))),
                when_then: when_then
                    .iter()
                    .map(|(w, t)| (self.fold_expr(w), self.fold_expr(t)))
                    .collect(),
                else_expr: else_expr.as_ref().map(|e| Box::new(self.fold_expr(e))),
            },
            _ => expr.clone(),
        }
    }

    /// Use the execution cast kernel, but never introduce a planning error or
    /// erase a type that ScalarValue cannot represent (notably typed NULL,
    /// decimal precision, timezone, and nested field metadata).
    fn eval_cast(
        &self,
        value: &ScalarValue,
        target: &arrow::datatypes::DataType,
        mode: crate::planner::CastMode,
    ) -> Option<ScalarValue> {
        use arrow::array::{Array, Date64Array};
        use arrow::datatypes::DataType;
        if matches!(value, ScalarValue::List(..)) {
            return None;
        }
        let input = crate::physical::operators::scalar_to_array(value, 1).ok()?;
        let output = crate::planner::numeric::cast_array(&input, target, mode).ok()?;
        if output.is_null(0) {
            return None;
        }
        let value = match target {
            DataType::Date64 => {
                ScalarValue::Date64(output.as_any().downcast_ref::<Date64Array>()?.value(0))
            }
            DataType::Timestamp(_, _) => ScalarValue::Timestamp(
                crate::planner::TimestampValue::from_array(output.as_ref(), 0)?,
            ),
            _ => crate::physical::morsel_agg::extract_scalar(&output, 0),
        };
        (value.data_type() == *target && !value.is_null()).then_some(value)
    }

    fn eval_binary(
        &self,
        left: &ScalarValue,
        op: BinaryOp,
        right: &ScalarValue,
    ) -> Option<ScalarValue> {
        match (left, right) {
            (ScalarValue::Int64(l), ScalarValue::Int64(r)) => self.eval_int64(*l, op, *r),
            (ScalarValue::Float64(l), ScalarValue::Float64(r)) => self.eval_float64(l.0, op, r.0),
            (ScalarValue::Boolean(l), ScalarValue::Boolean(r)) => self.eval_bool(*l, op, *r),
            (ScalarValue::Utf8(l), ScalarValue::Utf8(r)) => self.eval_string(l, op, r),
            _ => None,
        }
    }

    fn eval_int64(&self, left: i64, op: BinaryOp, right: i64) -> Option<ScalarValue> {
        match op {
            BinaryOp::Add => left.checked_add(right).map(ScalarValue::Int64),
            BinaryOp::Subtract => left.checked_sub(right).map(ScalarValue::Int64),
            BinaryOp::Multiply => left.checked_mul(right).map(ScalarValue::Int64),
            BinaryOp::Divide => left.checked_div(right).map(ScalarValue::Int64),
            BinaryOp::Modulo => left.checked_rem(right).map(ScalarValue::Int64),
            BinaryOp::Eq => Some(ScalarValue::Boolean(left == right)),
            BinaryOp::NotEq => Some(ScalarValue::Boolean(left != right)),
            BinaryOp::Lt => Some(ScalarValue::Boolean(left < right)),
            BinaryOp::LtEq => Some(ScalarValue::Boolean(left <= right)),
            BinaryOp::Gt => Some(ScalarValue::Boolean(left > right)),
            BinaryOp::GtEq => Some(ScalarValue::Boolean(left >= right)),
            _ => None,
        }
    }

    fn eval_float64(&self, left: f64, op: BinaryOp, right: f64) -> Option<ScalarValue> {
        match op {
            BinaryOp::Add => Some(ScalarValue::Float64(OrderedFloat(left + right))),
            BinaryOp::Subtract => Some(ScalarValue::Float64(OrderedFloat(left - right))),
            BinaryOp::Multiply => Some(ScalarValue::Float64(OrderedFloat(left * right))),
            BinaryOp::Divide => {
                if right == 0.0 {
                    None
                } else {
                    Some(ScalarValue::Float64(OrderedFloat(left / right)))
                }
            }
            BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq => Some(ScalarValue::Boolean(
                crate::planner::numeric::sql_float_compare(left, op, right),
            )),
            _ => None,
        }
    }

    fn eval_bool(&self, left: bool, op: BinaryOp, right: bool) -> Option<ScalarValue> {
        match op {
            BinaryOp::And => Some(ScalarValue::Boolean(left && right)),
            BinaryOp::Or => Some(ScalarValue::Boolean(left || right)),
            BinaryOp::Eq => Some(ScalarValue::Boolean(left == right)),
            BinaryOp::NotEq => Some(ScalarValue::Boolean(left != right)),
            _ => None,
        }
    }

    fn eval_string(&self, left: &str, op: BinaryOp, right: &str) -> Option<ScalarValue> {
        match op {
            BinaryOp::Eq => Some(ScalarValue::Boolean(left == right)),
            BinaryOp::NotEq => Some(ScalarValue::Boolean(left != right)),
            BinaryOp::Lt => Some(ScalarValue::Boolean(left < right)),
            BinaryOp::LtEq => Some(ScalarValue::Boolean(left <= right)),
            BinaryOp::Gt => Some(ScalarValue::Boolean(left > right)),
            BinaryOp::GtEq => Some(ScalarValue::Boolean(left >= right)),
            BinaryOp::StringConcat => Some(ScalarValue::Utf8(format!("{}{}", left, right))),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn constant_casts_use_execution_types_and_preserve_failures() {
        use crate::planner::CastMode;
        use arrow::datatypes::DataType;
        let cast = |value, data_type, mode| Expr::Cast {
            expr: Box::new(Expr::Literal(value)),
            data_type,
            mode,
        };
        let rule = ConstantFolding;
        assert_eq!(
            rule.fold_expr(&cast(
                ScalarValue::Utf8("1995-10-01".into()),
                DataType::Date32,
                CastMode::Strict
            )),
            Expr::Literal(ScalarValue::Date32(9404))
        );
        assert_eq!(
            rule.fold_expr(&cast(
                ScalarValue::Utf8("18446744073709551615".into()),
                DataType::UInt64,
                CastMode::Strict
            )),
            Expr::Literal(ScalarValue::UInt64(u64::MAX))
        );
        for expr in [
            cast(
                ScalarValue::Utf8("bad".into()),
                DataType::Int64,
                CastMode::Strict,
            ),
            cast(
                ScalarValue::Utf8("bad".into()),
                DataType::Int64,
                CastMode::Try,
            ),
            cast(ScalarValue::Null, DataType::Date32, CastMode::Strict),
            cast(
                ScalarValue::Int64(1),
                DataType::Decimal128(12, 2),
                CastMode::Strict,
            ),
        ] {
            assert_eq!(rule.fold_expr(&expr), expr);
        }
    }

    #[test]
    fn constant_integer_overflow_is_deferred_without_panicking() {
        let rule = ConstantFolding;
        for (a, op, b) in [
            (i64::MAX, BinaryOp::Add, 1),
            (i64::MIN, BinaryOp::Subtract, 1),
            (i64::MAX, BinaryOp::Multiply, 2),
            (i64::MIN, BinaryOp::Divide, -1),
            (i64::MIN, BinaryOp::Modulo, -1),
            (1, BinaryOp::Divide, 0),
        ] {
            assert_eq!(rule.eval_int64(a, op, b), None);
        }
    }

    #[test]
    fn test_fold_arithmetic() {
        let rule = ConstantFolding;

        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::Literal(ScalarValue::Int64(2))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(ScalarValue::Int64(3))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(5)));
    }

    #[test]
    fn test_fold_comparison() {
        let rule = ConstantFolding;

        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::Literal(ScalarValue::Int64(5))),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Int64(3))),
        };

        let folded = rule.fold_expr(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Boolean(true)));
    }

    #[test]
    fn test_fold_and_true() {
        let rule = ConstantFolding;

        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::column("a")),
            op: BinaryOp::And,
            right: Box::new(Expr::Literal(ScalarValue::Boolean(true))),
        };

        let folded = rule.fold_expr(&expr);
        assert!(matches!(folded, Expr::Column(_)));
    }

    #[test]
    fn test_fold_or_false() {
        let rule = ConstantFolding;

        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::column("a")),
            op: BinaryOp::Or,
            right: Box::new(Expr::Literal(ScalarValue::Boolean(false))),
        };

        let folded = rule.fold_expr(&expr);
        assert!(matches!(folded, Expr::Column(_)));
    }
}
