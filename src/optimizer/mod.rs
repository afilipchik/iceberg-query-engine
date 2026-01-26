//! Query optimizer module
//!
//! Implements rule-based and cost-based optimization

mod cost;
mod rules;

pub use cost::*;
pub use rules::*;

use crate::error::Result;
use crate::planner::LogicalPlan;
use std::sync::Arc;

/// Optimizer trait for plan transformations
pub trait OptimizerRule: Send + Sync {
    /// Name of this rule
    fn name(&self) -> &str;

    /// Apply this rule to the plan
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan>;
}

/// Main optimizer that applies rules to logical plans
pub struct Optimizer {
    rules: Vec<Arc<dyn OptimizerRule>>,
    max_iterations: usize,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    pub fn new() -> Self {
        Self {
            rules: vec![
                Arc::new(rules::ConstantFolding),
                Arc::new(rules::JoinReorder), // Must run before PredicatePushdown
                Arc::new(rules::PredicatePushdown),
                Arc::new(rules::ProjectionPushdown),
                Arc::new(rules::JoinReordering::default()),
                Arc::new(rules::PredicateReordering::default()),
            ],
            max_iterations: 10,
        }
    }

    /// Create optimizer with custom rules
    pub fn with_rules(rules: Vec<Arc<dyn OptimizerRule>>) -> Self {
        Self {
            rules,
            max_iterations: 10,
        }
    }

    /// Optimize a logical plan
    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        let mut current = plan;

        for _ in 0..self.max_iterations {
            let mut changed = false;

            for rule in &self.rules {
                let new_plan = rule.optimize(&current)?;

                // Simple check if plan changed (by string representation)
                // A proper implementation would use plan hashing
                if format!("{:?}", new_plan) != format!("{:?}", current) {
                    changed = true;
                    current = new_plan;
                }
            }

            if !changed {
                break;
            }
        }

        Ok(current)
    }

    #[allow(dead_code)] // Reserved for future cost-based optimization
    fn find_first_join(
        plan: &LogicalPlan,
    ) -> Option<(
        crate::planner::JoinType,
        Vec<(crate::planner::Expr, crate::planner::Expr)>,
    )> {
        match plan {
            LogicalPlan::Join(node) => Some((node.join_type, node.on.clone())),
            LogicalPlan::Filter(node) => Self::find_first_join(&node.input),
            LogicalPlan::Project(node) => Self::find_first_join(&node.input),
            LogicalPlan::Aggregate(node) => Self::find_first_join(&node.input),
            LogicalPlan::Sort(node) => Self::find_first_join(&node.input),
            LogicalPlan::Limit(node) => Self::find_first_join(&node.input),
            _ => None,
        }
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
            SchemaField::new("name", DataType::Utf8),
            SchemaField::new("amount", DataType::Float64),
        ])
    }

    #[test]
    fn test_optimizer_basic() {
        let plan = LogicalPlanBuilder::scan("orders", sample_schema())
            .filter(Expr::column("amount").gt(Expr::literal(ScalarValue::Float64(100.0.into()))))
            .build();

        let optimizer = Optimizer::new();
        let optimized = optimizer.optimize(plan).unwrap();

        // Plan should still be valid after optimization
        assert!(!optimized.schema().is_empty());
    }
}
