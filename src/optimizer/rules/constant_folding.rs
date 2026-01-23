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
                node.group_by = node.group_by.into_iter().map(|e| self.fold_expr(&e)).collect();
                node.aggregates = node.aggregates.into_iter().map(|e| self.fold_expr(&e)).collect();
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
            Expr::Cast { expr, data_type } => {
                let folded = self.fold_expr(expr);
                Expr::Cast {
                    expr: Box::new(folded),
                    data_type: data_type.clone(),
                }
            }
            Expr::Alias { expr, name } => Expr::Alias {
                expr: Box::new(self.fold_expr(expr)),
                name: name.clone(),
            },
            Expr::ScalarFunc { func, args } => Expr::ScalarFunc {
                func: func.clone(),
                args: args.iter().map(|a| self.fold_expr(a)).collect(),
            },
            Expr::Aggregate { func, args, distinct } => Expr::Aggregate {
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

    fn eval_binary(&self, left: &ScalarValue, op: BinaryOp, right: &ScalarValue) -> Option<ScalarValue> {
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
            BinaryOp::Add => Some(ScalarValue::Int64(left.checked_add(right)?)),
            BinaryOp::Subtract => Some(ScalarValue::Int64(left.checked_sub(right)?)),
            BinaryOp::Multiply => Some(ScalarValue::Int64(left.checked_mul(right)?)),
            BinaryOp::Divide => {
                if right == 0 {
                    None
                } else {
                    Some(ScalarValue::Int64(left / right))
                }
            }
            BinaryOp::Modulo => {
                if right == 0 {
                    None
                } else {
                    Some(ScalarValue::Int64(left % right))
                }
            }
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
            BinaryOp::Eq => Some(ScalarValue::Boolean(left == right)),
            BinaryOp::NotEq => Some(ScalarValue::Boolean(left != right)),
            BinaryOp::Lt => Some(ScalarValue::Boolean(left < right)),
            BinaryOp::LtEq => Some(ScalarValue::Boolean(left <= right)),
            BinaryOp::Gt => Some(ScalarValue::Boolean(left > right)),
            BinaryOp::GtEq => Some(ScalarValue::Boolean(left >= right)),
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
