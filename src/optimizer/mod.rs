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
                // First pass: push predicates and fold constants before decorrelation
                Arc::new(rules::ConstantFolding),
                Arc::new(rules::PredicatePushdown), // Push join conditions before decorrelation
                // FlattenDependentJoin: DelimJoin-based subquery flattening
                // Only handles simple single-EXISTS cases; complex patterns (Q21, Q22)
                // with multiple EXISTS/NOT EXISTS fall through to SubqueryDecorrelation
                Arc::new(rules::FlattenDependentJoin),
                // Decorrelate subqueries to regular joins
                Arc::new(rules::SubqueryDecorrelation),
                // Reorder joins after decorrelation
                Arc::new(rules::JoinReorder),
                // Final predicate pushdown for any remaining opportunities
                Arc::new(rules::PredicatePushdown),
                Arc::new(rules::ProjectionPushdown),
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
        self.optimize_inner(plan, false)
    }

    /// Optimize with optional diagnostic output
    pub fn optimize_with_diag(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        self.optimize_inner(plan, true)
    }

    fn optimize_inner(&self, plan: LogicalPlan, diag: bool) -> Result<LogicalPlan> {
        let mut current = plan;

        for iter in 0..self.max_iterations {
            let mut changed = false;

            for rule in &self.rules {
                let new_plan = rule.optimize(&current)?;

                // Simple check if plan changed (by string representation)
                // A proper implementation would use plan hashing
                if format!("{:?}", new_plan) != format!("{:?}", current) {
                    changed = true;
                    if diag {
                        eprintln!("[OPT iter={} rule={}] Plan changed", iter, rule.name());
                        Self::print_plan_summary(&new_plan, 0);
                    }
                    current = new_plan;
                }
            }

            if !changed {
                break;
            }
        }

        Ok(current)
    }

    /// Print a compact summary of the plan structure
    fn print_plan_summary(plan: &LogicalPlan, indent: usize) {
        let pad = "  ".repeat(indent);
        match plan {
            LogicalPlan::Scan(n) => {
                eprintln!(
                    "{}Scan: {} ({} cols)",
                    pad,
                    n.table_name,
                    n.schema.fields().len()
                );
            }
            LogicalPlan::Filter(n) => {
                eprintln!("{}Filter: {:?}", pad, n.predicate);
                Self::print_plan_summary(&n.input, indent + 1);
            }
            LogicalPlan::Project(n) => {
                eprintln!("{}Project ({} exprs)", pad, n.exprs.len());
                Self::print_plan_summary(&n.input, indent + 1);
            }
            LogicalPlan::Join(n) => {
                eprintln!(
                    "{}Join {:?} on={} filter={}",
                    pad,
                    n.join_type,
                    n.on.len(),
                    n.filter.is_some()
                );
                Self::print_plan_summary(&n.left, indent + 1);
                Self::print_plan_summary(&n.right, indent + 1);
            }
            LogicalPlan::Aggregate(n) => {
                eprintln!(
                    "{}Agg group_by={} aggs={}",
                    pad,
                    n.group_by.len(),
                    n.aggregates.len()
                );
                Self::print_plan_summary(&n.input, indent + 1);
            }
            LogicalPlan::Sort(_) => {
                eprintln!("{}Sort", pad);
                if let LogicalPlan::Sort(n) = plan {
                    Self::print_plan_summary(&n.input, indent + 1);
                }
            }
            LogicalPlan::Limit(n) => {
                eprintln!("{}Limit {:?}/{:?}", pad, n.skip, n.fetch);
                Self::print_plan_summary(&n.input, indent + 1);
            }
            LogicalPlan::SubqueryAlias(n) => {
                eprintln!("{}SubqueryAlias: {}", pad, n.alias);
                Self::print_plan_summary(&n.input, indent + 1);
            }
            LogicalPlan::Distinct(_) => eprintln!("{}Distinct", pad),
            LogicalPlan::Union(_) => eprintln!("{}Union", pad),
            _ => eprintln!("{}Other: {:?}", pad, std::mem::discriminant(plan)),
        }
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
