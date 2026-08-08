//! Semi/Anti join pushdown.
//!
//! Subquery decorrelation attaches Semi/Anti joins ABOVE the whole join tree
//! (`SELECT ... FROM a, b, c WHERE a.k IN (subquery)` becomes
//! `Semi(a ⋈ b ⋈ c, sub)`), so the semi filter runs after every inner join has
//! already materialized. When the semi condition only references columns from
//! one side of an inner join, the Semi can be pushed onto that side:
//!
//!   Semi(Inner(A, B), S) on A.k = S.k   →   Inner(Semi(A, S), B)
//!
//! Semi/Anti only FILTER their left input (the output schema is exactly the
//! left schema), so this rewrite is always sound as long as the condition's
//! left columns resolve unambiguously to one child. Q18's `o_orderkey IN
//! (... HAVING SUM(l_quantity) > 300)` filters 15M orders down to ~650K
//! before the customer and lineitem joins instead of after.

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::planner::{Expr, JoinNode, JoinType, LogicalPlan};
use std::collections::HashSet;
use std::sync::Arc;

pub struct SemiJoinPushdown;

impl OptimizerRule for SemiJoinPushdown {
    fn name(&self) -> &str {
        "SemiJoinPushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        Ok(push_down(plan))
    }
}

fn push_down(plan: &LogicalPlan) -> LogicalPlan {
    // Transform children first
    let children: Vec<Arc<LogicalPlan>> = plan
        .children()
        .iter()
        .map(|c| Arc::new(push_down(c)))
        .collect();
    let plan = if children.is_empty() {
        plan.clone()
    } else {
        plan.with_new_children(children)
    };

    if let LogicalPlan::Join(node) = &plan {
        if matches!(node.join_type, JoinType::Semi | JoinType::Anti) && node.filter.is_none() {
            if let Some(rewritten) = try_push_semi(node) {
                return rewritten;
            }
        }
    }
    plan
}

/// Column names (lowercased, unqualified) referenced by the semi condition's
/// left-side expressions. Returns None if any expression is not a plain column
/// (pushing computed keys is possible but not worth the risk).
fn semi_left_columns(node: &JoinNode) -> Option<HashSet<String>> {
    let mut cols = HashSet::new();
    for (l, _) in &node.on {
        match l {
            Expr::Column(c) => {
                cols.insert(c.name.to_lowercase());
            }
            _ => return None,
        }
    }
    if cols.is_empty() {
        None
    } else {
        Some(cols)
    }
}

fn schema_has_all(plan: &LogicalPlan, cols: &HashSet<String>) -> bool {
    let names: HashSet<String> = plan
        .schema()
        .fields()
        .iter()
        .map(|f| f.name.to_lowercase())
        .collect();
    cols.iter().all(|c| names.contains(c))
}

/// Try to push a Semi/Anti join below its left child. Returns the rewritten
/// plan, or None if no push is possible.
fn try_push_semi(semi: &JoinNode) -> Option<LogicalPlan> {
    let needed = semi_left_columns(semi)?;

    match &*semi.left {
        LogicalPlan::Join(inner)
            if matches!(inner.join_type, JoinType::Inner | JoinType::Cross) =>
        {
            let left_has = schema_has_all(&inner.left, &needed);
            let right_has = schema_has_all(&inner.right, &needed);
            // Require unambiguous resolution to exactly one side
            let target_left = left_has && !right_has;
            let target_right = right_has && !left_has;
            if !target_left && !target_right {
                return None;
            }

            let (new_child, other) = if target_left {
                (&inner.left, &inner.right)
            } else {
                (&inner.right, &inner.left)
            };

            // Semi output schema == its left input's schema
            let pushed = LogicalPlan::Join(JoinNode {
                left: Arc::clone(new_child),
                right: Arc::clone(&semi.right),
                join_type: semi.join_type,
                on: semi.on.clone(),
                filter: None,
                schema: new_child.schema(),
            });

            // Keep pushing further down if possible
            let pushed = if let LogicalPlan::Join(pushed_node) = &pushed {
                try_push_semi(pushed_node).unwrap_or(pushed)
            } else {
                pushed
            };

            let (new_left, new_right) = if target_left {
                (Arc::new(pushed), Arc::clone(other))
            } else {
                (Arc::clone(other), Arc::new(pushed))
            };

            Some(LogicalPlan::Join(JoinNode {
                left: new_left,
                right: new_right,
                join_type: inner.join_type,
                on: inner.on.clone(),
                filter: inner.filter.clone(),
                schema: inner.schema.clone(),
            }))
        }
        LogicalPlan::Filter(f) => {
            // Filter output schema == input schema; semi condition columns pass through
            let pushed = LogicalPlan::Join(JoinNode {
                left: Arc::clone(&f.input),
                right: Arc::clone(&semi.right),
                join_type: semi.join_type,
                on: semi.on.clone(),
                filter: None,
                schema: f.input.schema(),
            });
            let pushed = if let LogicalPlan::Join(pushed_node) = &pushed {
                try_push_semi(pushed_node).unwrap_or(pushed)
            } else {
                pushed
            };
            Some(LogicalPlan::Filter(crate::planner::FilterNode {
                input: Arc::new(pushed),
                predicate: f.predicate.clone(),
            }))
        }
        _ => None,
    }
}
