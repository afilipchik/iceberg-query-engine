//! Symbolic empty-input values. Never execute an expression during planning.
use crate::planner::{
    AggregateFunction as A, CastMode, Expr, LogicalPlan, PlanSchema, ScalarFunction, ScalarValue,
};

pub(super) fn scalar(plan: &LogicalPlan) -> Option<Expr> {
    let mut values = outputs(plan)?;
    (values.len() == 1).then(|| values.remove(0))
}

fn outputs(plan: &LogicalPlan) -> Option<Vec<Expr>> {
    match plan {
        LogicalPlan::Aggregate(node)
            if node.group_by.is_empty() && node.aggregates.len() == node.schema.fields().len() =>
        {
            node.aggregates
                .iter()
                .zip(node.schema.fields())
                .map(|(expr, field)| {
                    let mut expr = expr;
                    while let Expr::Alias { expr: inner, .. } = expr {
                        expr = inner;
                    }
                    let Expr::Aggregate { func, .. } = expr else {
                        return None;
                    };
                    let value = match func {
                        A::Count | A::CountDistinct => ScalarValue::Int64(0),
                        A::Sum | A::Avg | A::Min | A::Max => ScalarValue::Null,
                        _ => return None,
                    };
                    Some(Expr::Cast {
                        expr: Box::new(Expr::Literal(value)),
                        data_type: field.data_type.clone(),
                        mode: CastMode::Strict,
                    })
                })
                .collect()
        }
        LogicalPlan::Project(node) => {
            let values = outputs(&node.input)?;
            node.exprs
                .iter()
                .map(|expr| substitute(expr, &node.input.schema(), &values))
                .collect()
        }
        // Grouped scalar queries, HAVING, limits and unknown operators need
        // an existence proof of their own; retain scalar execution for them.
        _ => None,
    }
}

fn substitute(expr: &Expr, schema: &PlanSchema, values: &[Expr]) -> Option<Expr> {
    Some(match expr {
        Expr::Column(column) => values
            .get(crate::optimizer::properties::column_index(schema, column)?)?
            .clone(),
        Expr::Literal(_) => expr.clone(),
        Expr::Alias { expr, .. } => substitute(expr, schema, values)?,
        Expr::BinaryExpr { left, op, right } => Expr::BinaryExpr {
            left: Box::new(substitute(left, schema, values)?),
            op: *op,
            right: Box::new(substitute(right, schema, values)?),
        },
        Expr::UnaryExpr { op, expr } => Expr::UnaryExpr {
            op: *op,
            expr: Box::new(substitute(expr, schema, values)?),
        },
        Expr::Cast {
            expr,
            data_type,
            mode,
        } => Expr::Cast {
            expr: Box::new(substitute(expr, schema, values)?),
            data_type: data_type.clone(),
            mode: *mode,
        },
        Expr::ScalarFunc {
            func: func @ (ScalarFunction::Coalesce | ScalarFunction::NullIf),
            args,
        } => Expr::ScalarFunc {
            func: func.clone(),
            args: args
                .iter()
                .map(|arg| substitute(arg, schema, values))
                .collect::<Option<_>>()?,
        },
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => Expr::Case {
            operand: operand
                .as_ref()
                .map(|e| substitute(e, schema, values).map(Box::new))
                .transpose_option()?,
            when_then: when_then
                .iter()
                .map(|(a, b)| {
                    Some((
                        substitute(a, schema, values)?,
                        substitute(b, schema, values)?,
                    ))
                })
                .collect::<Option<_>>()?,
            else_expr: else_expr
                .as_ref()
                .map(|e| substitute(e, schema, values).map(Box::new))
                .transpose_option()?,
        },
        _ => return None,
    })
}

trait TransposeOption<T> {
    fn transpose_option(self) -> Option<Option<T>>;
}
impl<T> TransposeOption<T> for Option<Option<T>> {
    fn transpose_option(self) -> Option<Option<T>> {
        match self {
            None => Some(None),
            Some(v) => v.map(Some),
        }
    }
}

pub(super) fn fresh(base: &str, schemas: &[&PlanSchema]) -> String {
    let mut name = base.to_owned();
    while schemas
        .iter()
        .any(|s| s.fields().iter().any(|f| f.name == name))
    {
        name.push('_');
    }
    name
}
