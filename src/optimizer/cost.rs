//! Cost estimation for query optimization

use crate::planner::LogicalPlan;

/// Why a join-key NDV estimate could not be trusted at face value.
///
/// Shared, reusable classification for "this relation's join-key statistics
/// are missing or degenerate" — built for `optimizer::rules::join_reorder`'s
/// DPsize cost model (join-order-stats-hardening epic, task 001), but
/// deliberately free-standing here rather than living inside
/// `join_reorder.rs` itself: `JoinReorder`'s own module is private
/// (`mod join_reorder;` in `rules/mod.rs`, only the `JoinReorder` struct is
/// re-exported), while `cost`'s public items are glob-re-exported at
/// `crate::optimizer::*` (see `optimizer/mod.rs`'s `pub use cost::*;`), so
/// this is reachable crate-wide as `crate::optimizer::classify_join_key_ndv`
/// / `crate::optimizer::warn_untrustworthy_join_key_stats` with zero further
/// plumbing. That is the intended reuse seam for task 002 (native-table
/// statistics staleness after mutation): once a DELETE/UPDATE has
/// materially invalidated a native table's own derived NDV, task 002 should
/// route that case through THIS SAME `UntrustworthyStats::Degenerate`
/// classification and `warn_untrustworthy_join_key_stats` call, not build a
/// second, parallel "don't trust this number" mechanism. Note the dead
/// `CostEstimator` below this point in the file is genuinely unrelated to
/// this mechanism — it has zero call sites anywhere in the crate (confirmed
/// via `grep`), operates on a completely different `Statistics`/
/// `ColumnStatistics` pair of types that never carries NDV at all
/// (`column_stats: vec![]` at every call site), and was investigated and
/// ruled out as a fit before this code was added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UntrustworthyStats {
    /// No NDV estimate was recorded at all for this relation/column — e.g.
    /// a relation registered via `ExecutionContext::register_table`
    /// (`MemoryTable`), whose `statistics()` always returns a real row
    /// count but an EMPTY `column_stats` map (see
    /// `examples/adaptive_reopt_ndv_repro.rs` and CLAUDE.md's "Adaptive
    /// join-order re-optimization" section for the load-bearing repro).
    Missing,
    /// An NDV estimate exists but is degenerate given the relation's own
    /// row count: non-positive (reachable today — `ParquetTable`'s
    /// `ndv_est` derivation can compute `Some(0)` for an all-null integer
    /// column), or a single-value range for a relation that plainly holds
    /// far more rows than that (a collapsed min==max range, which for a
    /// native table can also arise from `Segment` statistics that were
    /// computed once at write time and never revisited by a later
    /// DELETE/UPDATE — see `src/storage/native_manifest.rs`).
    Degenerate,
}

impl UntrustworthyStats {
    pub fn as_str(&self) -> &'static str {
        match self {
            UntrustworthyStats::Missing => "no recorded NDV statistics",
            UntrustworthyStats::Degenerate => {
                "a degenerate NDV estimate (non-positive, or collapsed to a single value despite a much larger row count)"
            }
        }
    }
}

/// A relation whose join key resolves to a single-value NDV is only
/// classified as degenerate (as opposed to a genuinely constant column)
/// once its own row count clears this bar — a small dimension table
/// (TPC-H `region`/`nation`-scale, tens of rows) can legitimately have a
/// tiny or even singleton NDV without that being a statistics problem.
const DEGENERATE_SINGLETON_ROW_THRESHOLD: f64 = 1000.0;

/// Classify a join-key NDV estimate: `Ok(ndv)` when it is directly usable,
/// `Err(reason)` when it is missing or degenerate and the caller should
/// fall back to an estimate AND surface it via
/// `warn_untrustworthy_join_key_stats`. Pure and side-effect-free by
/// design (no tracing here) so a caller can classify without necessarily
/// warning immediately.
pub fn classify_join_key_ndv(
    ndv_est: Option<f64>,
    relation_row_count: f64,
) -> std::result::Result<f64, UntrustworthyStats> {
    match ndv_est {
        None => Err(UntrustworthyStats::Missing),
        Some(v) if v <= 0.0 => Err(UntrustworthyStats::Degenerate),
        Some(v) if v <= 1.0 && relation_row_count > DEGENERATE_SINGLETON_ROW_THRESHOLD => {
            Err(UntrustworthyStats::Degenerate)
        }
        Some(v) => Ok(v),
    }
}

/// The single, shared visibility mechanism this task builds: fires in
/// NORMAL operation via `tracing::warn!` — never gated behind a
/// debug-only env var the way this codebase's `DP_DEBUG`/`PLAN_DEBUG`
/// switches are — so a catastrophically bad join order caused by
/// missing/degenerate statistics is diagnosable without anyone already
/// knowing to look for it. Names the relation and column explicitly (as
/// structured `tracing` fields, matching this codebase's existing
/// convention in e.g. `src/distributed/server.rs`) and states the
/// fallback NDV being substituted so the resulting plan's cost numbers
/// are traceable back to this exact decision.
pub fn warn_untrustworthy_join_key_stats(
    relation: &str,
    column: &str,
    reason: UntrustworthyStats,
    fallback_ndv: f64,
) {
    tracing::warn!(
        relation = relation,
        column = column,
        reason = reason.as_str(),
        fallback_ndv = fallback_ndv,
        "join_reorder: join key has untrustworthy statistics for this join edge; \
         falling back to an estimated NDV (can misorder joins catastrophically if wrong \
         -- see CLAUDE.md's 'Adaptive join-order re-optimization' section)"
    );
}

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

            LogicalPlan::Window(node) => self.estimate_statistics(&node.input),

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

            LogicalPlan::DelimJoin(node) => {
                // DelimJoin is similar to a regular join but more efficient
                // due to deduplication
                let left_stats = self.estimate_statistics(&node.left);
                let right_stats = self.estimate_statistics(&node.right);
                let left_rows = left_stats.row_count.unwrap_or(self.default_row_count);
                let right_rows = right_stats.row_count.unwrap_or(self.default_row_count);

                // For Semi/Anti joins, output is at most left side
                let output_rows = match node.join_type {
                    crate::planner::JoinType::Semi | crate::planner::JoinType::Anti => left_rows,
                    crate::planner::JoinType::Single => left_rows,
                    crate::planner::JoinType::Mark => left_rows,
                    _ => (left_rows as f64 * right_rows as f64 * 0.1) as usize,
                };

                Statistics {
                    row_count: Some(output_rows),
                    total_byte_size: None,
                    column_stats: vec![],
                }
            }

            // A top-k search emits exactly k rows, whatever its input costs.
            LogicalPlan::VectorSearch(node) => Statistics {
                row_count: Some(node.k),
                total_byte_size: Some(node.k * node.schema.len() * 8),
                column_stats: vec![],
            },

            LogicalPlan::DelimGet(node) => {
                // DelimGet receives distinct correlation values - estimate based on delim columns
                Statistics {
                    row_count: Some(self.default_row_count / 10), // Distinct values typically smaller
                    total_byte_size: Some(node.schema.len() * 8 * self.default_row_count / 10),
                    column_stats: vec![],
                }
            }
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

            // Window: a sort plus a per-row evaluation pass.
            LogicalPlan::Window(node) => {
                let input_cost = self.estimate(&node.input);
                let sort_cost = row_count * (row_count.max(2.0)).log2() * 0.1;
                input_cost + Cost::new(sort_cost + row_count, 0.0, row_count * 8.0)
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

            LogicalPlan::DelimJoin(node) => {
                let left_cost = self.estimate(&node.left);
                let right_cost = self.estimate(&node.right);
                let left_rows = self
                    .estimate_statistics(&node.left)
                    .row_count
                    .unwrap_or(self.default_row_count) as f64;

                // DelimJoin is more efficient than regular join due to deduplication
                // Build cost is for distinct values only, not all rows
                let distinct_factor = 0.1; // Assume 10% distinct values
                left_cost
                    + right_cost
                    + Cost::new(
                        left_rows * distinct_factor * self.hash_join_build_cost
                            + left_rows * self.hash_join_probe_cost,
                        0.0,
                        left_rows * distinct_factor * 16.0, // Hash table memory for distinct values
                    )
            }

            LogicalPlan::VectorSearch(node) => self.estimate(&node.input),

            LogicalPlan::DelimGet(_) => {
                // DelimGet is a source that receives data from parent - minimal cost
                Cost::new(row_count * 0.1, 0.0, row_count * 8.0)
            }
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

    #[test]
    fn classify_missing_ndv_as_missing() {
        assert_eq!(
            classify_join_key_ndv(None, 1_500_000.0),
            Err(UntrustworthyStats::Missing)
        );
    }

    #[test]
    fn classify_zero_ndv_as_degenerate() {
        assert_eq!(
            classify_join_key_ndv(Some(0.0), 1_500_000.0),
            Err(UntrustworthyStats::Degenerate)
        );
    }

    #[test]
    fn classify_collapsed_range_on_large_relation_as_degenerate() {
        // A join key that resolves to a single distinct value on a
        // 1.5M-row relation is suspicious, not a genuine constant column.
        assert_eq!(
            classify_join_key_ndv(Some(1.0), 1_500_000.0),
            Err(UntrustworthyStats::Degenerate)
        );
    }

    #[test]
    fn classify_singleton_ndv_on_small_dimension_table_is_not_degenerate() {
        // TPC-H `region`-scale table (5 rows): a singleton NDV there is a
        // perfectly ordinary real value, not a statistics problem.
        assert_eq!(classify_join_key_ndv(Some(1.0), 5.0), Ok(1.0));
    }

    #[test]
    fn classify_real_ndv_as_ok() {
        assert_eq!(classify_join_key_ndv(Some(25.0), 1_500_000.0), Ok(25.0));
    }

    #[test]
    fn untrustworthy_stats_reason_strings_are_distinct() {
        assert_ne!(
            UntrustworthyStats::Missing.as_str(),
            UntrustworthyStats::Degenerate.as_str()
        );
    }
}
