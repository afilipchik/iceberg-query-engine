//! Query optimizer module
//!
//! Implements rule-based and cost-based optimization

mod cost;
mod properties;
pub(crate) mod rules;

pub use cost::*;
pub use rules::*;

use crate::error::Result;
use crate::physical::operators::TableStatistics;
use crate::planner::LogicalPlan;
use std::collections::HashMap;
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
    /// Table statistics for statistics-based optimization
    table_stats: HashMap<String, TableStatistics>,
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
                // Derive per-column IN-lists implied by OR predicates so they
                // can push to scans (Q07 nation pair, Q19 brand/container)
                Arc::new(rules::DeriveOrPredicates),
                Arc::new(rules::PredicatePushdown), // Push join conditions before decorrelation
                // FlattenDependentJoin: DelimJoin-based subquery flattening
                // Only handles simple single-EXISTS cases; complex patterns (Q21, Q22)
                // with multiple EXISTS/NOT EXISTS fall through to SubqueryDecorrelation
                Arc::new(rules::FlattenDependentJoin),
                // Decorrelate subqueries to regular joins
                Arc::new(rules::SubqueryDecorrelation),
                // Push Semi/Anti joins below inner joins so they filter early
                Arc::new(rules::SemiJoinPushdown),
                // Reorder joins after decorrelation
                Arc::new(rules::JoinReorder::new()),
                // Final predicate pushdown for any remaining opportunities
                Arc::new(rules::PredicatePushdown),
                // Share the pipeline between HAVING totals and the aggregate
                Arc::new(rules::HavingTotalCse),
                // Collapse FD-redundant group keys to the unique key column
                Arc::new(rules::GroupKeyReduction::new()),
                // Pre-aggregate duplicated join inputs (needs footer stats)
                Arc::new(rules::EagerAggregation::new()),
                // Pack dual int group keys onto the raw aggregation path
                Arc::new(rules::PackedGroupKeys::new()),
                // Pack dual int JOIN keys onto the single-int64 probe path
                // (needs footer stats to prove the pack collision-free)
                Arc::new(rules::PackedJoinKeys::new()),
                Arc::new(rules::ProjectionPushdown),
                // Last: needs the final projection/filter shape to recognize a
                // k-NN, and produces an opaque node the other rules skip.
                Arc::new(rules::VectorSearchPushdown),
            ],
            max_iterations: 10,
            table_stats: HashMap::new(),
        }
    }

    /// Set table statistics for statistics-based join optimization
    pub fn with_table_statistics(mut self, stats: HashMap<String, TableStatistics>) -> Self {
        self.table_stats = stats;
        self
    }

    /// Create optimizer with custom rules
    pub fn with_rules(rules: Vec<Arc<dyn OptimizerRule>>) -> Self {
        Self {
            rules,
            max_iterations: 10,
            table_stats: HashMap::new(),
        }
    }

    /// Optimize a logical plan
    pub fn optimize(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        // If we have table statistics, rebuild rules with stats-aware JoinReorder
        if !self.table_stats.is_empty() {
            let rules: Vec<Arc<dyn OptimizerRule>> = self
                .rules
                .iter()
                .map(|rule| {
                    if rule.name() == "JoinReorder" {
                        Arc::new(rules::JoinReorder::with_table_statistics(
                            self.table_stats.clone(),
                        )) as Arc<dyn OptimizerRule>
                    } else if rule.name() == "EagerAggregation" {
                        Arc::new(rules::EagerAggregation::with_table_statistics(
                            self.table_stats.clone(),
                        )) as Arc<dyn OptimizerRule>
                    } else if rule.name() == "GroupKeyReduction" {
                        Arc::new(rules::GroupKeyReduction::with_table_statistics(
                            self.table_stats.clone(),
                        )) as Arc<dyn OptimizerRule>
                    } else if rule.name() == "PackedGroupKeys" {
                        Arc::new(rules::PackedGroupKeys::with_table_statistics(
                            self.table_stats.clone(),
                        )) as Arc<dyn OptimizerRule>
                    } else if rule.name() == "PackedJoinKeys" {
                        Arc::new(rules::PackedJoinKeys::with_table_statistics(
                            self.table_stats.clone(),
                        )) as Arc<dyn OptimizerRule>
                    } else {
                        rule.clone()
                    }
                })
                .collect();
            return Self::optimize_with_rules(plan, &rules, self.max_iterations, false);
        }
        self.optimize_inner(plan, false)
    }

    /// Optimize with optional diagnostic output
    pub fn optimize_with_diag(&self, plan: LogicalPlan) -> Result<LogicalPlan> {
        self.optimize_inner(plan, true)
    }

    fn optimize_inner(&self, plan: LogicalPlan, diag: bool) -> Result<LogicalPlan> {
        Self::optimize_with_rules(plan, &self.rules, self.max_iterations, diag)
    }

    fn optimize_with_rules(
        plan: LogicalPlan,
        rules: &[Arc<dyn OptimizerRule>],
        max_iterations: usize,
        diag: bool,
    ) -> Result<LogicalPlan> {
        // PackedJoinKeys rewrites a dual-column equi-join's ON into ONE
        // packed EXPRESSION pair. JoinReorder's graph extraction only sees
        // column=column predicates as edges, so if the fixpoint loop runs
        // another iteration after the pack, JoinReorder rebuilds the join
        // graph MINUS that edge and can manufacture a CROSS join (Q5 at
        // SF=10 planned supplier x customer as a 21-billion-row cross).
        // The pack is a pure key-representation change with no downstream
        // rule dependencies, so it runs exactly once, AFTER the loop.
        let (loop_rules, final_rules): (Vec<Arc<dyn OptimizerRule>>, Vec<Arc<dyn OptimizerRule>>) =
            rules
                .iter()
                .cloned()
                .partition(|r| r.name() != "PackedJoinKeys");

        let mut current = plan;

        for iter in 0..max_iterations {
            // Rules can introduce and remove an intermediate representation
            // within one round. Convergence concerns the complete pipeline.
            let round_start = current.clone();
            for rule in &loop_rules {
                let new_plan = rule.optimize(&current).map_err(|e| {
                    crate::error::QueryError::Internal(format!(
                        "optimizer rule `{}` failed: {e}",
                        rule.name()
                    ))
                })?;
                if diag && new_plan != current {
                    eprintln!("[OPT iter={} rule={}] Plan changed", iter, rule.name());
                    Self::print_plan_summary(&new_plan, 0);
                }
                current = new_plan;
            }
            if current == round_start {
                break;
            }
        }

        for rule in &final_rules {
            let new_plan = rule.optimize(&current).map_err(|e| {
                crate::error::QueryError::Internal(format!(
                    "optimizer rule `{}` failed: {e}",
                    rule.name()
                ))
            })?;
            if diag && new_plan != current {
                eprintln!("[OPT final rule={}] Plan changed", rule.name());
                Self::print_plan_summary(&new_plan, 0);
            }
            current = new_plan;
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

#[cfg(test)]
mod round_convergence_contract_tests {
    use super::*;
    use crate::planner::{EmptyRelationNode, PlanSchema};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct SetEmptyFlag {
        name: &'static str,
        flag: bool,
        calls: Arc<AtomicUsize>,
    }
    impl OptimizerRule for SetEmptyFlag {
        fn name(&self) -> &str {
            self.name
        }
        fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let LogicalPlan::EmptyRelation(node) = plan else {
                panic!("fixture plan")
            };
            Ok(LogicalPlan::EmptyRelation(EmptyRelationNode {
                produce_one_row: self.flag,
                schema: node.schema.clone(),
            }))
        }
    }
    fn input() -> LogicalPlan {
        LogicalPlan::EmptyRelation(EmptyRelationNode {
            produce_one_row: true,
            schema: PlanSchema::empty(),
        })
    }
    #[test]
    fn inverse_rules_stop_after_a_complete_unchanged_round_and_final_rule_runs_once() {
        let calls: Vec<_> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
        let rules: Vec<Arc<dyn OptimizerRule>> = vec![
            Arc::new(SetEmptyFlag {
                name: "Intermediate",
                flag: false,
                calls: calls[0].clone(),
            }),
            Arc::new(SetEmptyFlag {
                name: "Restore",
                flag: true,
                calls: calls[1].clone(),
            }),
            Arc::new(SetEmptyFlag {
                name: "PackedJoinKeys",
                flag: true,
                calls: calls[2].clone(),
            }),
        ];
        assert_eq!(
            Optimizer::optimize_with_rules(input(), &rules, 10, false).unwrap(),
            input()
        );
        assert_eq!(
            calls
                .iter()
                .map(|c| c.load(Ordering::Relaxed))
                .collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
    }
    #[test]
    fn a_real_round_change_requires_another_round() {
        let calls = Arc::new(AtomicUsize::new(0));
        let rules: Vec<Arc<dyn OptimizerRule>> = vec![Arc::new(SetEmptyFlag {
            name: "Set",
            flag: false,
            calls: calls.clone(),
        })];
        let result = Optimizer::optimize_with_rules(input(), &rules, 10, false).unwrap();
        let LogicalPlan::EmptyRelation(node) = result else {
            panic!("fixture plan")
        };
        assert!(!node.produce_one_row);
        assert_eq!(calls.load(Ordering::Relaxed), 2);
    }
}
