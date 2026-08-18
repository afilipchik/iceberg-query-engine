//! Pack a two-integer-column equi-join key into one Int64 — when footer
//! statistics PROVE the pack collision-free.
//!
//! # Why
//!
//! The hash join's single-int64 path (VHT, direct hashing) is measured ~10x
//! faster per probe than the generic composite-key path: Q9 at SF=100 probes
//! partsupp's 80M-entry `(ps_suppkey, ps_partkey)` table at 12-15.7s per
//! partition while the LARGER 150M-entry single-key orders table probes in
//! 0.6-1.3s. At small scale EagerAggregation happened to deliver the same
//! packing as a side effect of its `__ea_key`; its own gates keep it out of
//! SF=100, and the join was left on the slow path.
//!
//! # The proof obligation (identical to EagerAggregation's dual-key gate)
//!
//! `pack(a, b) = a * K + b` is injective iff `0 <= b < K` and `a >= 0` on
//! BOTH sides, with `max(a) * K + max(b)` inside i64. K is the next power of
//! two above the larger side's second-key maximum, read from parquet footer
//! statistics. If any bound is missing or violated, the rule declines —
//! packing on hope would alias different key pairs onto one slot and return
//! wrong JOIN matches, the worst class of bug this engine knows.
//!
//! NULL semantics survive the rewrite: an SQL equi-join never matches NULL
//! keys, and `CAST(NULL) * K + x` is NULL, which the hash join also never
//! matches. INNER joins only — they are where the cost lives (Q9), and outer
//! joins' NULL-extension paths are not worth the review surface yet.

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::physical::operators::TableStatistics;
use crate::planner::{BinaryOp, Expr, JoinType, LogicalPlan};
use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PackedJoinKeys {
    table_stats: HashMap<String, TableStatistics>,
}

impl PackedJoinKeys {
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
        }
    }

    pub fn with_table_statistics(table_stats: HashMap<String, TableStatistics>) -> Self {
        Self { table_stats }
    }

    /// (min, max) of a plain integer column, by unqualified name, from any
    /// table that has it. Ambiguity across tables is safe: bounds only ever
    /// WIDEN the proof obligation, so take the widest.
    fn column_bounds(&self, name: &str) -> Option<(i64, i64)> {
        let key = name.to_lowercase();
        let mut out: Option<(i64, i64)> = None;
        for stats in self.table_stats.values() {
            if let Some(cs) = stats.column_stats.get(&key) {
                if let (Some(lo), Some(hi)) = (cs.min_i64, cs.max_i64) {
                    out = Some(match out {
                        None => (lo, hi),
                        Some((a, b)) => (a.min(lo), b.max(hi)),
                    });
                }
            }
        }
        out
    }

    fn as_int_column(e: &Expr) -> Option<&crate::planner::Column> {
        match e {
            Expr::Column(c) => Some(c),
            _ => None,
        }
    }

    /// `CAST(col AS Int64) * k + CAST(col2 AS Int64)`, minus casts that are
    /// provably no-ops at bind time (the binder types these columns later; a
    /// redundant cast costs a kernel pass over 600M rows).
    fn pack_expr(first: &Expr, second: &Expr, k: i64) -> Expr {
        let int64 = |e: &Expr| Expr::Cast {
            expr: Box::new(e.clone()),
            data_type: DataType::Int64,
        };
        Expr::BinaryExpr {
            left: Box::new(Expr::BinaryExpr {
                left: Box::new(int64(first)),
                op: BinaryOp::Multiply,
                right: Box::new(Expr::Literal(crate::planner::ScalarValue::Int64(k))),
            }),
            op: BinaryOp::Add,
            right: Box::new(int64(second)),
        }
    }

    fn try_pack(&self, node: &crate::planner::JoinNode) -> Option<(Expr, Expr)> {
        if node.join_type != JoinType::Inner || node.on.len() != 2 {
            return None;
        }
        let (l1, r1) = (&node.on[0].0, &node.on[0].1);
        let (l2, r2) = (&node.on[1].0, &node.on[1].1);
        let cols = [
            Self::as_int_column(l1)?,
            Self::as_int_column(r1)?,
            Self::as_int_column(l2)?,
            Self::as_int_column(r2)?,
        ];
        let bounds: Vec<(i64, i64)> = cols
            .iter()
            .map(|c| self.column_bounds(&c.name))
            .collect::<Option<Vec<_>>>()?;
        // Non-negative firsts and seconds on both sides.
        if bounds.iter().any(|(lo, _)| *lo < 0) {
            return None;
        }
        // K covers the SECOND key of both sides.
        let max2 = bounds[2].1.max(bounds[3].1);
        let k = (max2 as u64 + 1).checked_next_power_of_two()? as i128;
        let max1 = bounds[0].1.max(bounds[1].1) as i128;
        let max2 = max2 as i128;
        if max1 * k + max2 > i64::MAX as i128 {
            return None;
        }
        let k = k as i64;
        Some((Self::pack_expr(l1, l2, k), Self::pack_expr(r1, r2, k)))
    }

    fn rewrite(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // Rebuild children first (bottom-up).
        let children: Vec<Arc<LogicalPlan>> = plan
            .children()
            .iter()
            .map(|c| self.rewrite(c).map(Arc::new))
            .collect::<Result<_>>()?;
        let rebuilt = if children.is_empty() {
            plan.clone()
        } else {
            plan.with_new_children(children)
        };

        if let LogicalPlan::Join(node) = &rebuilt {
            if let Some((l, r)) = self.try_pack(node) {
                let mut new_node = node.clone();
                new_node.on = vec![(l, r)];
                return Ok(LogicalPlan::Join(new_node));
            }
        }
        Ok(rebuilt)
    }
}

impl Default for PackedJoinKeys {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerRule for PackedJoinKeys {
    fn name(&self) -> &str {
        "PackedJoinKeys"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        if self.table_stats.is_empty() {
            // No stats, no proof, no rewrite.
            return Ok(plan.clone());
        }
        self.rewrite(plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::operators::ColumnStatistics;

    fn stats(cols: &[(&str, i64, i64)]) -> HashMap<String, TableStatistics> {
        let mut column_stats = HashMap::new();
        for (n, lo, hi) in cols {
            column_stats.insert(
                n.to_string(),
                ColumnStatistics {
                    min_i64: Some(*lo),
                    max_i64: Some(*hi),
                    null_count: Some(0),
                    ndv_est: None,
                    ..Default::default()
                },
            );
        }
        let mut m = HashMap::new();
        m.insert(
            "t".to_string(),
            TableStatistics {
                row_count: 1,
                total_byte_size: 1,
                column_stats,
            },
        );
        m
    }

    fn join(on: Vec<(Expr, Expr)>, join_type: JoinType) -> crate::planner::JoinNode {
        use crate::planner::{PlanSchema, ScanNode};
        let scan = |t: &str| {
            Arc::new(LogicalPlan::Scan(ScanNode {
                table_name: t.to_string(),
                schema: PlanSchema::new(vec![]),
                projection: None,
                filter: None,
            }))
        };
        crate::planner::JoinNode {
            left: scan("a"),
            right: scan("b"),
            join_type,
            on,
            filter: None,
            schema: PlanSchema::new(vec![]),
        }
    }

    fn col(n: &str) -> Expr {
        Expr::Column(crate::planner::Column::new(n.to_string()))
    }

    #[test]
    fn packs_a_bounded_dual_int_inner_join() {
        let r = PackedJoinKeys::with_table_statistics(stats(&[
            ("ps_suppkey", 1, 1_000_000),
            ("l_suppkey", 1, 1_000_000),
            ("ps_partkey", 1, 20_000_000),
            ("l_partkey", 1, 20_000_000),
        ]));
        let n = join(
            vec![
                (col("ps_suppkey"), col("l_suppkey")),
                (col("ps_partkey"), col("l_partkey")),
            ],
            JoinType::Inner,
        );
        let packed = r.try_pack(&n).expect("must pack");
        let s = format!("{:?}", packed.0);
        assert!(s.contains("33554432"), "K must be 2^25: {s}");
    }

    #[test]
    fn declines_without_proof() {
        // Missing bounds for one column: no rewrite.
        let r = PackedJoinKeys::with_table_statistics(stats(&[
            ("ps_suppkey", 1, 1_000_000),
            ("l_suppkey", 1, 1_000_000),
            ("ps_partkey", 1, 20_000_000),
        ]));
        let n = join(
            vec![
                (col("ps_suppkey"), col("l_suppkey")),
                (col("ps_partkey"), col("l_partkey")),
            ],
            JoinType::Inner,
        );
        assert!(r.try_pack(&n).is_none());

        // Negative minimum: carries could alias; no rewrite.
        let r = PackedJoinKeys::with_table_statistics(stats(&[
            ("ps_suppkey", -5, 1_000_000),
            ("l_suppkey", 1, 1_000_000),
            ("ps_partkey", 1, 20_000_000),
            ("l_partkey", 1, 20_000_000),
        ]));
        assert!(r.try_pack(&n).is_none());

        // Overflow: max1 * K + max2 outside i64; no rewrite.
        let r = PackedJoinKeys::with_table_statistics(stats(&[
            ("ps_suppkey", 1, i64::MAX / 2),
            ("l_suppkey", 1, 1_000_000),
            ("ps_partkey", 1, i64::MAX / 2),
            ("l_partkey", 1, 20_000_000),
        ]));
        assert!(r.try_pack(&n).is_none());
    }

    #[test]
    fn declines_outer_joins_and_wrong_arity() {
        let r = PackedJoinKeys::with_table_statistics(stats(&[
            ("a1", 1, 10),
            ("b1", 1, 10),
            ("a2", 1, 10),
            ("b2", 1, 10),
        ]));
        let left = join(
            vec![(col("a1"), col("b1")), (col("a2"), col("b2"))],
            JoinType::Left,
        );
        assert!(r.try_pack(&left).is_none(), "LEFT join must not pack");
        let single = join(vec![(col("a1"), col("b1"))], JoinType::Inner);
        assert!(r.try_pack(&single).is_none(), "single key needs no pack");
    }
}
