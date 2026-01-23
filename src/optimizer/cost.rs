//! Cost estimation for query optimization

use crate::planner::LogicalPlan;

/// Statistics for a table or intermediate result
#[derive(Debug, Clone, Default)]
pub struct Statistics {
    /// Estimated row count
    pub row_count: Option<usize>,
    /// Estimated size in bytes
    pub total_byte_size: Option<usize>,
    /// Column statistics
    pub column_stats: Vec<ColumnStatistics>,
}

/// Statistics for a single column
#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    /// Number of distinct values
    pub distinct_count: Option<usize>,
    /// Null count
    pub null_count: Option<usize>,
    /// Min value (as string for simplicity)
    pub min_value: Option<String>,
    /// Max value (as string for simplicity)
    pub max_value: Option<String>,
}

/// Cost estimate for a physical plan
#[derive(Debug, Clone, Default)]
pub struct Cost {
    /// CPU cost (in arbitrary units)
    pub cpu: f64,
    /// I/O cost (in arbitrary units)
    pub io: f64,
    /// Memory cost (in bytes)
    pub memory: f64,
}

impl Cost {
    pub fn new(cpu: f64, io: f64, memory: f64) -> Self {
        Self { cpu, io, memory }
    }

    pub fn total(&self) -> f64 {
        // Weighted sum of costs
        self.cpu + self.io * 10.0 + self.memory * 0.001
    }
}

impl std::ops::Add for Cost {
    type Output = Cost;

    fn add(self, other: Cost) -> Cost {
        Cost {
            cpu: self.cpu + other.cpu,
            io: self.io + other.io,
            memory: self.memory + other.memory,
        }
    }
}

/// Cost estimator for logical plans
pub struct CostEstimator {
    /// Default row count for tables without statistics
    pub default_row_count: usize,
    /// Cost per row for scan
    pub scan_cost_per_row: f64,
    /// Cost per row for filter
    pub filter_cost_per_row: f64,
    /// Cost per row for projection
    pub project_cost_per_row: f64,
    /// Cost per row for hash join (build side)
    pub hash_join_build_cost: f64,
    /// Cost per row for hash join (probe side)
    pub hash_join_probe_cost: f64,
    /// Cost per row for sort
    pub sort_cost_per_row: f64,
    /// Cost per row for aggregation
    pub agg_cost_per_row: f64,
}

impl Default for CostEstimator {
    fn default() -> Self {
        Self {
            default_row_count: 1000,
            scan_cost_per_row: 1.0,
            filter_cost_per_row: 0.5,
            project_cost_per_row: 0.2,
            hash_join_build_cost: 2.0,
            hash_join_probe_cost: 1.0,
            sort_cost_per_row: 10.0, // n log n amortized
            agg_cost_per_row: 1.5,
        }
    }
}

impl CostEstimator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Estimate the cost of a logical plan
    pub fn estimate(&self, plan: &LogicalPlan) -> Cost {
        let stats = self.estimate_statistics(plan);
        self.estimate_cost(plan, &stats)
    }

    /// Estimate statistics for a plan
    pub fn estimate_statistics(&self, plan: &LogicalPlan) -> Statistics {
        match plan {
            LogicalPlan::Scan(node) => Statistics {
                row_count: Some(self.default_row_count),
                total_byte_size: Some(self.default_row_count * node.schema.len() * 8),
                column_stats: vec![],
            },

            LogicalPlan::Filter(node) => {
                let input_stats = self.estimate_statistics(&node.input);
                // Assume filter passes 30% of rows
                let selectivity = 0.3;
                Statistics {
                    row_count: input_stats
                        .row_count
                        .map(|r| (r as f64 * selectivity) as usize),
                    total_byte_size: input_stats
                        .total_byte_size
                        .map(|s| (s as f64 * selectivity) as usize),
                    column_stats: vec![],
                }
            }

            LogicalPlan::Project(node) => {
                let input_stats = self.estimate_statistics(&node.input);
                let output_cols = node.exprs.len();
                let input_cols = node.input.schema().len().max(1);
                let col_ratio = output_cols as f64 / input_cols as f64;
                Statistics {
                    row_count: input_stats.row_count,
                    total_byte_size: input_stats
                        .total_byte_size
                        .map(|s| (s as f64 * col_ratio) as usize),
                    column_stats: vec![],
                }
            }

            LogicalPlan::Join(node) => {
                let left_stats = self.estimate_statistics(&node.left);
                let right_stats = self.estimate_statistics(&node.right);
                // Simple estimate: product of row counts with selectivity
                let selectivity = 0.1;
                let left_rows = left_stats.row_count.unwrap_or(self.default_row_count);
                let right_rows = right_stats.row_count.unwrap_or(self.default_row_count);
                Statistics {
                    row_count: Some((left_rows as f64 * right_rows as f64 * selectivity) as usize),
                    total_byte_size: None,
                    column_stats: vec![],
                }
            }

            LogicalPlan::Aggregate(node) => {
                let input_stats = self.estimate_statistics(&node.input);
                // Estimate distinct groups
                let num_groups = if node.group_by.is_empty() {
                    1
                } else {
                    // Assume each group-by column reduces cardinality
                    let input_rows = input_stats.row_count.unwrap_or(self.default_row_count);
                    (input_rows as f64 / (node.group_by.len() as f64 * 10.0).max(1.0)) as usize
                };
                Statistics {
                    row_count: Some(num_groups.max(1)),
                    total_byte_size: None,
                    column_stats: vec![],
                }
            }

            LogicalPlan::Sort(node) => self.estimate_statistics(&node.input),

            LogicalPlan::Limit(node) => {
                let input_stats = self.estimate_statistics(&node.input);
                let output_rows = node
                    .fetch
                    .unwrap_or_else(|| input_stats.row_count.unwrap_or(self.default_row_count));
                Statistics {
                    row_count: Some(output_rows),
                    total_byte_size: None,
                    column_stats: vec![],
                }
            }

            LogicalPlan::Distinct(node) => {
                let input_stats = self.estimate_statistics(&node.input);
                // Assume distinct reduces to 80% of input
                Statistics {
                    row_count: input_stats.row_count.map(|r| (r as f64 * 0.8) as usize),
                    total_byte_size: None,
                    column_stats: vec![],
                }
            }

            LogicalPlan::Union(node) => {
                let total_rows: usize = node
                    .inputs
                    .iter()
                    .map(|i| {
                        self.estimate_statistics(i)
                            .row_count
                            .unwrap_or(self.default_row_count)
                    })
                    .sum();
                Statistics {
                    row_count: Some(total_rows),
                    total_byte_size: None,
                    column_stats: vec![],
                }
            }

            LogicalPlan::SubqueryAlias(node) => self.estimate_statistics(&node.input),

            LogicalPlan::EmptyRelation(node) => Statistics {
                row_count: Some(if node.produce_one_row { 1 } else { 0 }),
                total_byte_size: Some(0),
                column_stats: vec![],
            },

            LogicalPlan::Values(node) => Statistics {
                row_count: Some(node.values.len()),
                total_byte_size: None,
                column_stats: vec![],
            },
        }
    }

    fn estimate_cost(&self, plan: &LogicalPlan, stats: &Statistics) -> Cost {
        let row_count = stats.row_count.unwrap_or(self.default_row_count) as f64;

        match plan {
            LogicalPlan::Scan(_) => Cost::new(
                row_count * self.scan_cost_per_row,
                row_count * 0.1, // I/O cost
                row_count * 8.0, // Memory for batch
            ),

            LogicalPlan::Filter(node) => {
                let input_cost = self.estimate(&node.input);
                input_cost + Cost::new(row_count * self.filter_cost_per_row, 0.0, 0.0)
            }

            LogicalPlan::Project(node) => {
                let input_cost = self.estimate(&node.input);
                input_cost + Cost::new(row_count * self.project_cost_per_row, 0.0, 0.0)
            }

            LogicalPlan::Join(node) => {
                let left_cost = self.estimate(&node.left);
                let right_cost = self.estimate(&node.right);
                let left_rows = self
                    .estimate_statistics(&node.left)
                    .row_count
                    .unwrap_or(self.default_row_count) as f64;
                let right_rows = self
                    .estimate_statistics(&node.right)
                    .row_count
                    .unwrap_or(self.default_row_count) as f64;

                left_cost
                    + right_cost
                    + Cost::new(
                        left_rows * self.hash_join_build_cost
                            + right_rows * self.hash_join_probe_cost,
                        0.0,
                        left_rows * 16.0, // Hash table memory
                    )
            }

            LogicalPlan::Aggregate(node) => {
                let input_cost = self.estimate(&node.input);
                let input_rows = self
                    .estimate_statistics(&node.input)
                    .row_count
                    .unwrap_or(self.default_row_count) as f64;

                input_cost
                    + Cost::new(
                        input_rows * self.agg_cost_per_row,
                        0.0,
                        row_count * 64.0, // Memory for aggregation state
                    )
            }

            LogicalPlan::Sort(node) => {
                let input_cost = self.estimate(&node.input);
                let input_rows = self
                    .estimate_statistics(&node.input)
                    .row_count
                    .unwrap_or(self.default_row_count) as f64;

                input_cost
                    + Cost::new(
                        input_rows * self.sort_cost_per_row * (input_rows.ln().max(1.0)),
                        0.0,
                        input_rows * 16.0, // Memory for sort
                    )
            }

            LogicalPlan::Limit(node) => self.estimate(&node.input),

            LogicalPlan::Distinct(node) => {
                let input_cost = self.estimate(&node.input);
                let input_rows = self
                    .estimate_statistics(&node.input)
                    .row_count
                    .unwrap_or(self.default_row_count) as f64;

                input_cost + Cost::new(input_rows * self.agg_cost_per_row, 0.0, row_count * 64.0)
            }

            LogicalPlan::Union(node) => node
                .inputs
                .iter()
                .map(|i| self.estimate(i))
                .fold(Cost::default(), |acc, c| acc + c),

            LogicalPlan::SubqueryAlias(node) => self.estimate(&node.input),

            LogicalPlan::EmptyRelation(_) => Cost::default(),

            LogicalPlan::Values(_) => Cost::new(row_count * 0.1, 0.0, row_count * 16.0),
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
    fn test_scan_cost() {
        let estimator = CostEstimator::new();
        let plan = LogicalPlanBuilder::scan("orders", sample_schema()).build();

        let cost = estimator.estimate(&plan);
        assert!(cost.cpu > 0.0);
    }

    #[test]
    fn test_filter_reduces_rows() {
        let estimator = CostEstimator::new();

        let scan = LogicalPlanBuilder::scan("orders", sample_schema()).build();
        let filter = LogicalPlanBuilder::scan("orders", sample_schema())
            .filter(Expr::column("amount").gt(Expr::literal(ScalarValue::Float64(100.0.into()))))
            .build();

        let scan_stats = estimator.estimate_statistics(&scan);
        let filter_stats = estimator.estimate_statistics(&filter);

        assert!(filter_stats.row_count.unwrap() < scan_stats.row_count.unwrap());
    }
}
