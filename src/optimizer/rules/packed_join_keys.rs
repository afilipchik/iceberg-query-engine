//! Pack dual integer inner-join keys using structural/type/predicate proofs.
//! Table statistics are costing hints and cannot establish collision freedom.
//! NULLs propagate through packing and remain non-matching for inner equijoins.

use super::packed_group_keys::{integer_domain, packing_radix, IntegerDomain};
use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::physical::operators::TableStatistics;
use crate::planner::{BinaryOp, Expr, JoinType, LogicalPlan};
use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::sync::Arc;

pub struct PackedJoinKeys;

impl PackedJoinKeys {
    pub fn new() -> Self {
        Self
    }

    /// Retained constructor compatibility. Estimates are never packing proofs.
    pub fn with_table_statistics(_table_stats: HashMap<String, TableStatistics>) -> Self {
        Self
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
            mode: crate::planner::CastMode::Strict,
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
        let first_left = integer_domain(&node.left, cols[0])?;
        let first_right = integer_domain(&node.right, cols[1])?;
        let second_left = integer_domain(&node.left, cols[2])?;
        let second_right = integer_domain(&node.right, cols[3])?;
        let union = |a: IntegerDomain, b: IntegerDomain| IntegerDomain {
            min: a.min.min(b.min),
            max: a.max.max(b.max),
            nullable: a.nullable || b.nullable,
        };
        let k = packing_radix(
            union(first_left, first_right),
            union(second_left, second_right),
        )?;
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
    fn packs_integer_type_domains_without_statistics() {
        use crate::planner::{PlanSchema, ScanNode, SchemaField};
        let scan = |table: &str| {
            Arc::new(LogicalPlan::Scan(ScanNode {
                table_name: table.into(),
                schema: PlanSchema::new(vec![
                    SchemaField::new("a", DataType::UInt16),
                    SchemaField::new("b", DataType::UInt16),
                ]),
                projection: None,
                filter: None,
            }))
        };
        let mut node = join(
            vec![(col("a"), col("a")), (col("b"), col("b"))],
            JoinType::Inner,
        );
        node.left = scan("left");
        node.right = scan("right");
        let packed = PackedJoinKeys::new()
            .try_pack(&node)
            .expect("unsigned type domains fit");
        assert!(format!("{:?}", packed.0).contains("65536"));
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

    #[test]
    fn name_only_statistics_are_not_proof_for_an_unresolved_column() {
        let rule = PackedJoinKeys::with_table_statistics(stats(&[("a", 0, 1), ("b", 0, 1)]));
        let node = join(
            vec![(col("a"), col("a")), (col("b"), col("b"))],
            JoinType::Inner,
        );
        assert!(rule.try_pack(&node).is_none());
    }

    #[test]
    fn computed_alias_and_outer_subtree_do_not_inherit_named_stats() {
        use crate::planner::{PlanSchema, ProjectNode, ScalarValue, ScanNode, SchemaField};
        let scan = || {
            Arc::new(LogicalPlan::Scan(ScanNode {
                table_name: "t".into(),
                schema: PlanSchema::new(vec![
                    SchemaField::new("a", DataType::UInt16),
                    SchemaField::new("b", DataType::UInt16),
                ]),
                projection: None,
                filter: None,
            }))
        };
        let base = scan();
        let projected = Arc::new(LogicalPlan::Project(ProjectNode {
            schema: base.schema(),
            input: base,
            exprs: vec![
                col("a"),
                Expr::Alias {
                    name: "b".into(),
                    expr: Box::new(Expr::BinaryExpr {
                        left: Box::new(col("b")),
                        op: BinaryOp::Multiply,
                        right: Box::new(Expr::Literal(ScalarValue::UInt16(2))),
                    }),
                },
            ],
        }));
        let mut node = join(
            vec![(col("a"), col("a")), (col("b"), col("b"))],
            JoinType::Inner,
        );
        node.left = projected;
        node.right = scan();
        let rule = PackedJoinKeys::with_table_statistics(stats(&[("a", 0, 1), ("b", 0, 1)]));
        assert!(rule.try_pack(&node).is_none());
        let mut outer = node.clone();
        outer.left = scan();
        outer.join_type = JoinType::Left;
        outer.schema = outer.left.schema();
        node.left = Arc::new(LogicalPlan::Join(outer));
        assert!(rule.try_pack(&node).is_none());
    }
}
