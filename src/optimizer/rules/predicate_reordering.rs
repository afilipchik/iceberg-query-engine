//! Predicate reordering optimization rule
//!
//! Reorders conjunctive predicates to evaluate the most selective ones first.
//! Ported from upstream commit edbea72, adapted for ea/ui_and_fixups branch.
//!
//! Design doc: .claude/plans/predicate-reordering-design.md

use crate::error::Result;
use crate::optimizer::{cost::CostEstimator, OptimizerRule};
use crate::planner::{BinaryOp, Expr, FilterNode, LogicalPlan, ScalarValue};
use std::sync::Arc;

/// Predicate reordering rule - evaluates most selective predicates first
pub struct PredicateReordering {
    estimator: CostEstimator,
}

impl PredicateReordering {
    pub fn new() -> Self {
        Self {
            estimator: CostEstimator::new(),
        }
    }
}

impl Default for PredicateReordering {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerRule for PredicateReordering {
    fn name(&self) -> &str {
        "PredicateReordering"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(node) => {
                // Split conjunctive predicates
                let predicates = self.split_conjunction(&node.predicate);

                if predicates.len() <= 1 {
                    // Nothing to reorder
                    let input = self.optimize(&node.input)?;
                    return Ok(LogicalPlan::Filter(FilterNode {
                        input: Arc::new(input),
                        predicate: node.predicate.clone(),
                    }));
                }

                // Estimate selectivity for each predicate
                let mut predicates_with_selectivity: Vec<(Expr, f64)> = predicates
                    .iter()
                    .map(|p| {
                        let selectivity = self.estimate_selectivity(p, &node.input);
                        (p.clone(), selectivity)
                    })
                    .collect();

                // Sort by selectivity (lowest first = most selective)
                predicates_with_selectivity
                    .sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));

                // Rebuild the predicate chain in order of selectivity
                let reordered_predicate = self.combine_predicates(
                    predicates_with_selectivity
                        .into_iter()
                        .map(|(p, _)| p)
                        .collect(),
                );

                // Recursively optimize input
                let input = self.optimize(&node.input)?;

                Ok(LogicalPlan::Filter(FilterNode {
                    input: Arc::new(input),
                    predicate: reordered_predicate,
                }))
            }

            // For other plan types, recursively process children
            LogicalPlan::Join(node) => {
                let left = self.optimize(&node.left)?;
                let right = self.optimize(&node.right)?;
                Ok(LogicalPlan::Join(crate::planner::JoinNode {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    join_type: node.join_type,
                    on: node.on.clone(),
                    filter: node.filter.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Project(node) => {
                let input = self.optimize(&node.input)?;
                Ok(LogicalPlan::Project(crate::planner::ProjectNode {
                    input: Arc::new(input),
                    exprs: node.exprs.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Aggregate(node) => {
                let input = self.optimize(&node.input)?;
                Ok(LogicalPlan::Aggregate(crate::planner::AggregateNode {
                    input: Arc::new(input),
                    group_by: node.group_by.clone(),
                    aggregates: node.aggregates.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Sort(node) => {
                let input = self.optimize(&node.input)?;
                Ok(LogicalPlan::Sort(crate::planner::SortNode {
                    input: Arc::new(input),
                    order_by: node.order_by.clone(),
                }))
            }

            LogicalPlan::Limit(node) => {
                let input = self.optimize(&node.input)?;
                Ok(LogicalPlan::Limit(crate::planner::LimitNode {
                    input: Arc::new(input),
                    skip: node.skip,
                    fetch: node.fetch,
                }))
            }

            LogicalPlan::Distinct(node) => {
                let input = self.optimize(&node.input)?;
                Ok(LogicalPlan::Distinct(crate::planner::DistinctNode {
                    input: Arc::new(input),
                }))
            }

            LogicalPlan::SubqueryAlias(node) => {
                let input = self.optimize(&node.input)?;
                Ok(LogicalPlan::SubqueryAlias(
                    crate::planner::SubqueryAliasNode {
                        input: Arc::new(input),
                        alias: node.alias.clone(),
                        schema: node.schema.clone(),
                    },
                ))
            }

            LogicalPlan::Union(node) => {
                let inputs: Result<Vec<_>> = node
                    .inputs
                    .iter()
                    .map(|i| self.optimize(i).map(Arc::new))
                    .collect();
                Ok(LogicalPlan::Union(crate::planner::UnionNode {
                    inputs: inputs?,
                    schema: node.schema.clone(),
                    all: node.all,
                }))
            }

            // Leaf nodes and complex nodes (DelimJoin, DelimGet, Values, etc.)
            _ => Ok(plan.clone()),
        }
    }
}

impl PredicateReordering {
    /// Split a conjunction into individual predicates
    fn split_conjunction(&self, expr: &Expr) -> Vec<Expr> {
        let mut predicates = Vec::new();
        self.split_conjunction_recursive(expr, &mut predicates);
        predicates
    }

    fn split_conjunction_recursive(&self, expr: &Expr, predicates: &mut Vec<Expr>) {
        match expr {
            Expr::BinaryExpr {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.split_conjunction_recursive(left, predicates);
                self.split_conjunction_recursive(right, predicates);
            }
            _ => {
                predicates.push(expr.clone());
            }
        }
    }

    /// Combine predicates with AND
    fn combine_predicates(&self, predicates: Vec<Expr>) -> Expr {
        predicates
            .into_iter()
            .reduce(|a, b| a.and(b))
            .expect("predicates should not be empty")
    }

    /// Estimate selectivity of a predicate (0.0 = no rows, 1.0 = all rows)
    fn estimate_selectivity(&self, expr: &Expr, input: &LogicalPlan) -> f64 {
        match expr {
            // Equality predicates are very selective
            Expr::BinaryExpr {
                op: BinaryOp::Eq, ..
            } => {
                if self.is_unique_column_comparison(expr, input) {
                    return 1.0 / self.get_input_row_count(input).max(1) as f64;
                }
                0.1
            }

            // Inequality predicates are somewhat selective
            Expr::BinaryExpr {
                op: BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq,
                ..
            } => 0.3,

            // Like patterns
            Expr::BinaryExpr {
                op: BinaryOp::Like | BinaryOp::NotLike,
                right,
                ..
            } => {
                if let Expr::Literal(ScalarValue::Utf8(pattern)) = right.as_ref() {
                    if pattern.starts_with('%') {
                        0.5 // Leading wildcard = less selective
                    } else {
                        0.2 // No leading wildcard = more selective
                    }
                } else {
                    0.3
                }
            }

            // IS NOT NULL is not very selective
            Expr::UnaryExpr {
                op: crate::planner::UnaryOp::IsNotNull,
                ..
            } => 0.95,

            // IS NULL predicates are very selective
            Expr::UnaryExpr {
                op: crate::planner::UnaryOp::IsNull,
                ..
            } => 0.05,

            // IN predicates
            Expr::InList { list, .. } => {
                let list_size = list.len().max(1);
                (list_size as f64 / 100.0).min(0.5)
            }

            // BETWEEN predicates
            Expr::Between { .. } => 0.1,

            // NOT equals
            Expr::BinaryExpr {
                op: BinaryOp::NotEq,
                ..
            } => 0.9,

            // Default selectivity
            _ => 0.5,
        }
    }

    /// Check if an expression is a comparison with a unique column
    fn is_unique_column_comparison(&self, _expr: &Expr, _input: &LogicalPlan) -> bool {
        // Placeholder: could be enhanced with table statistics
        false
    }

    /// Get the estimated row count of the input
    fn get_input_row_count(&self, input: &LogicalPlan) -> usize {
        self.estimator
            .estimate_statistics(input)
            .row_count
            .unwrap_or(1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{Expr, LogicalPlanBuilder, PlanSchema, ScalarValue, SchemaField};
    use arrow::datatypes::DataType;

    fn sample_schema() -> PlanSchema {
        PlanSchema::new(vec![
            SchemaField::new("id", DataType::Int64),
            SchemaField::new("status", DataType::Utf8),
            SchemaField::new("amount", DataType::Float64),
        ])
    }

    #[test]
    fn test_predicate_splitting() {
        let rule = PredicateReordering::new();

        let plan = LogicalPlanBuilder::scan("orders", sample_schema())
            .filter(
                Expr::column("status")
                    .eq(Expr::literal(ScalarValue::Utf8("pending".into())))
                    .and(
                        Expr::column("amount")
                            .gt(Expr::literal(ScalarValue::Float64(100.0.into()))),
                    ),
            )
            .build();

        let optimized = rule.optimize(&plan).unwrap();
        assert!(matches!(optimized, LogicalPlan::Filter(_)));
    }

    #[test]
    fn test_selectivity_estimation() {
        let rule = PredicateReordering::new();

        let scan = LogicalPlanBuilder::scan("orders", sample_schema()).build();

        // Equality should be more selective than inequality
        let eq_selectivity = rule.estimate_selectivity(
            &Expr::column("status").eq(Expr::literal(ScalarValue::Utf8("pending".into()))),
            &scan,
        );

        let gt_selectivity = rule.estimate_selectivity(
            &Expr::column("amount").gt(Expr::literal(ScalarValue::Float64(100.0.into()))),
            &scan,
        );

        assert!(eq_selectivity < gt_selectivity);
    }

    #[test]
    fn test_reorders_most_selective_first() {
        let rule = PredicateReordering::new();

        // Create: WHERE amount > 100 AND status = 'pending' AND id IS NULL
        // Expected reorder: IS NULL (0.05) then Eq (0.1) then Gt (0.3)
        let plan = LogicalPlanBuilder::scan("orders", sample_schema())
            .filter(
                Expr::column("amount")
                    .gt(Expr::literal(ScalarValue::Float64(100.0.into())))
                    .and(
                        Expr::column("status")
                            .eq(Expr::literal(ScalarValue::Utf8("pending".into()))),
                    )
                    .and(Expr::UnaryExpr {
                        op: crate::planner::UnaryOp::IsNull,
                        expr: Box::new(Expr::column("id")),
                    }),
            )
            .build();

        let optimized = rule.optimize(&plan).unwrap();

        // Verify the result is still a valid filter
        if let LogicalPlan::Filter(node) = &optimized {
            // The leftmost predicate in the AND chain should be IS NULL (most selective)
            if let Expr::BinaryExpr {
                left,
                op: BinaryOp::And,
                ..
            } = &node.predicate
            {
                if let Expr::BinaryExpr {
                    left: innermost,
                    op: BinaryOp::And,
                    ..
                } = left.as_ref()
                {
                    assert!(
                        matches!(
                            innermost.as_ref(),
                            Expr::UnaryExpr {
                                op: crate::planner::UnaryOp::IsNull,
                                ..
                            }
                        ),
                        "IS NULL should be first (most selective)"
                    );
                }
            }
        }
    }
}
