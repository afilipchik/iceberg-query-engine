//! Semantic properties derived from operators, never from estimates.
//!
//! Keys use output ordinals and SQL grouping equality (NULLs are equal).
//! An empty key proves at most one row. Unknown properties are conservative.
use crate::planner::{Column, Expr, JoinType, LogicalPlan, PlanSchema};

pub(crate) fn column_index(schema: &PlanSchema, column: &Column) -> Option<usize> {
    let mut matches = schema.fields().iter().enumerate().filter(|(_, field)| {
        field.name == column.name
            && column
                .relation
                .as_ref()
                .is_none_or(|rel| field.relation.as_ref() == Some(rel))
    });
    let index = matches.next()?.0;
    matches.next().is_none().then_some(index)
}

fn identity_index(expr: &Expr, schema: &PlanSchema) -> Option<usize> {
    match expr {
        Expr::Column(column) => column_index(schema, column),
        Expr::Alias { expr, .. } => identity_index(expr, schema),
        _ => None,
    }
}

pub(crate) fn proven_keys(plan: &LogicalPlan) -> Vec<Vec<usize>> {
    match plan {
        LogicalPlan::Aggregate(node) => vec![(0..node.group_by.len()).collect()],
        LogicalPlan::Distinct(node) => vec![(0..node.input.schema().len()).collect()],
        LogicalPlan::Filter(node) => proven_keys(&node.input),
        LogicalPlan::Sort(node) => proven_keys(&node.input),
        LogicalPlan::Limit(node) => proven_keys(&node.input),
        LogicalPlan::SubqueryAlias(node) => proven_keys(&node.input),
        LogicalPlan::Project(node) => {
            let input = node.input.schema();
            proven_keys(&node.input)
                .into_iter()
                .filter_map(|key| {
                    key.into_iter()
                        .map(|index| {
                            node.exprs
                                .iter()
                                .position(|expr| identity_index(expr, &input) == Some(index))
                        })
                        .collect()
                })
                .collect()
        }
        LogicalPlan::Join(node) if matches!(node.join_type, JoinType::Semi | JoinType::Anti) => {
            proven_keys(&node.left)
        }
        // In particular scans, joins and UNION ALL provide no key proof.
        _ => vec![],
    }
}

pub(crate) fn proves_unique_column(plan: &LogicalPlan, column: &Column) -> bool {
    let Some(index) = column_index(&plan.schema(), column) else {
        return false;
    };
    proven_keys(plan)
        .iter()
        .any(|key| key.iter().all(|&part| part == index))
}
