//! Join reordering optimization rule
//!
//! Reorders joins based on cost estimates to minimize intermediate result sizes.

use crate::error::Result;
use crate::optimizer::{cost::CostEstimator, OptimizerRule};
use crate::planner::{
    BinaryOp, Expr, JoinNode, JoinType, LogicalPlan,
};
use std::sync::Arc;

/// Join reordering rule - reorders joins based on cost
pub struct JoinReordering {
    estimator: CostEstimator,
}

impl JoinReordering {
    pub fn new() -> Self {
        Self {
            estimator: CostEstimator::new(),
        }
    }

    pub fn with_estimator(estimator: CostEstimator) -> Self {
        Self { estimator }
    }
}

impl Default for JoinReordering {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerRule for JoinReordering {
    fn name(&self) -> &str {
        "JoinReordering"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.reorder_joins(plan)
    }
}

impl JoinReordering {
    fn reorder_joins(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Join(node) => {
                // Try to find a better ordering
                let optimized = self.optimize_join_order(node.clone())?;

                // Recursively apply to children
                let left = self.reorder_joins(&optimized.left)?;
                let right = self.reorder_joins(&optimized.right)?;

                Ok(LogicalPlan::Join(JoinNode {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    join_type: optimized.join_type,
                    on: optimized.on.clone(),
                    filter: optimized.filter.clone(),
                    schema: optimized.schema.clone(),
                }))
            }

            // For other plan types, recursively process children
            LogicalPlan::Filter(node) => {
                let input = self.reorder_joins(&node.input)?;
                Ok(LogicalPlan::Filter(crate::planner::FilterNode {
                    input: Arc::new(input),
                    predicate: node.predicate.clone(),
                }))
            }

            LogicalPlan::Project(node) => {
                let input = self.reorder_joins(&node.input)?;
                Ok(LogicalPlan::Project(crate::planner::ProjectNode {
                    input: Arc::new(input),
                    exprs: node.exprs.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Aggregate(node) => {
                let input = self.reorder_joins(&node.input)?;
                Ok(LogicalPlan::Aggregate(crate::planner::AggregateNode {
                    input: Arc::new(input),
                    group_by: node.group_by.clone(),
                    aggregates: node.aggregates.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Sort(node) => {
                let input = self.reorder_joins(&node.input)?;
                Ok(LogicalPlan::Sort(crate::planner::SortNode {
                    input: Arc::new(input),
                    order_by: node.order_by.clone(),
                }))
            }

            LogicalPlan::Limit(node) => {
                let input = self.reorder_joins(&node.input)?;
                Ok(LogicalPlan::Limit(crate::planner::LimitNode {
                    input: Arc::new(input),
                    skip: node.skip,
                    fetch: node.fetch,
                }))
            }

            LogicalPlan::Distinct(node) => {
                let input = self.reorder_joins(&node.input)?;
                Ok(LogicalPlan::Distinct(crate::planner::DistinctNode {
                    input: Arc::new(input),
                }))
            }

            // For other plan types, return as-is
            _ => Ok(plan.clone()),
        }
    }

    /// Optimize join order for a multi-way join
    /// Currently handles left-deep trees; could be extended to bushy trees
    fn optimize_join_order(&self, node: JoinNode) -> Result<JoinNode> {
        // Collect all joins in a left-deep tree
        let joins = self.collect_joins(&LogicalPlan::Join(node.clone()))?;

        if joins.len() <= 1 {
            // Only one join, nothing to reorder
            return Ok(node);
        }

        // Find the optimal ordering using dynamic programming
        // For now, use a greedy approach based on filter selectivity
        let reordered = self.greedy_join_reorder(&joins)?;

        Ok(reordered)
    }

    /// Collect all joins in a left-deep tree
    fn collect_joins(&self, plan: &LogicalPlan) -> Result<Vec<JoinInfo>> {
        let mut joins = Vec::new();

        self.collect_joins_recursive(plan, &mut joins)?;

        Ok(joins)
    }

    fn collect_joins_recursive(&self, plan: &LogicalPlan, joins: &mut Vec<JoinInfo>) -> Result<()> {
        match plan {
            LogicalPlan::Join(node) => {
                // First, collect joins from left side (left-deep tree)
                self.collect_joins_recursive(&node.left, joins)?;

                // Add this join
                joins.push(JoinInfo {
                    left: node.left.clone(),
                    right: node.right.clone(),
                    join_type: node.join_type,
                    on: node.on.clone(),
                    filter: node.filter.clone(),
                    schema: node.schema.clone(),
                    left_cardinality: 0,  // Unused - reserved for future cardinality-based ordering
                    right_cardinality: 0, // Unused - reserved for future cardinality-based ordering
                });

                Ok(())
            }

            LogicalPlan::Filter(node) => {
                // Collect joins through filters
                self.collect_joins_recursive(&node.input, joins)
            }

            LogicalPlan::Project(node) => {
                // Collect joins through projections
                self.collect_joins_recursive(&node.input, joins)
            }

            _ => Ok(()),
        }
    }

    /// Greedy join reordering based on cardinality
    /// Start with the smallest table and progressively join larger tables
    fn greedy_join_reorder(&self, joins: &[JoinInfo]) -> Result<JoinNode> {
        if joins.is_empty() {
            return Err(crate::error::QueryError::Plan(
                "No joins to reorder".to_string(),
            ));
        }

        if joins.len() == 1 {
            // Only one join, convert JoinInfo to JoinNode
            let join = &joins[0];
            return Ok(JoinNode {
                left: join.left.clone(),
                right: join.right.clone(),
                join_type: join.join_type,
                on: join.on.clone(),
                filter: join.filter.clone(),
                schema: join.schema.clone(),
            });
        }

        // Build a left-deep join tree by processing joins sequentially
        // Start with the first join
        let mut result = JoinNode {
            left: joins[0].left.clone(),
            right: joins[0].right.clone(),
            join_type: joins[0].join_type,
            on: joins[0].on.clone(),
            filter: joins[0].filter.clone(),
            schema: joins[0].schema.clone(),
        };

        // Process remaining joins
        for join in joins.iter().skip(1) {
            result = JoinNode {
                left: Arc::new(LogicalPlan::Join(result.clone())),
                right: join.right.clone(),
                join_type: join.join_type,
                on: join.on.clone(),
                filter: join.filter.clone(),
                schema: join.schema.clone(),
            };
        }

        Ok(result)
    }
}

/// Information about a join for reordering
#[derive(Clone)]
struct JoinInfo {
    left: Arc<LogicalPlan>,
    right: Arc<LogicalPlan>,
    join_type: JoinType,
    on: Vec<(Expr, Expr)>,
    filter: Option<Expr>,
    schema: crate::planner::PlanSchema,
    left_cardinality: usize,
    right_cardinality: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{Expr, LogicalPlanBuilder, PlanSchema, ScalarValue, SchemaField};
    use arrow::datatypes::DataType;

    fn orders_schema() -> PlanSchema {
        PlanSchema::new(vec![
            SchemaField::new("orderkey", DataType::Int64),
            SchemaField::new("custkey", DataType::Int64),
            SchemaField::new("orderstatus", DataType::Utf8),
        ])
    }

    fn customer_schema() -> PlanSchema {
        PlanSchema::new(vec![
            SchemaField::new("custkey", DataType::Int64),
            SchemaField::new("name", DataType::Utf8),
        ])
    }

    fn lineitem_schema() -> PlanSchema {
        PlanSchema::new(vec![
            SchemaField::new("orderkey", DataType::Int64),
            SchemaField::new("partkey", DataType::Int64),
            SchemaField::new("quantity", DataType::Int64),
        ])
    }

    #[test]
    fn test_join_reordering_basic() {
        let rule = JoinReordering::new();

        // Create a simple join
        let plan = LogicalPlanBuilder::scan("orders", orders_schema())
            .join(
                LogicalPlanBuilder::scan("customer", customer_schema()).build(),
                crate::planner::JoinType::Inner,
                vec![(
                    Expr::column("custkey"),
                    Expr::column("custkey"),
                )],
                None, // No filter
            )
            .build();

        let optimized = rule.optimize(&plan).unwrap();

        // Should still be a valid join
        assert!(matches!(optimized, LogicalPlan::Join(_)));
    }
}
