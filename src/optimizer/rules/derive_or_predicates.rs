//! Derive implied per-column predicates from OR expressions.
//!
//! `(n1 = 'FRANCE' AND n2 = 'GERMANY') OR (n1 = 'GERMANY' AND n2 = 'FRANCE')`
//! implies `n1 IN ('FRANCE','GERMANY') AND n2 IN ('FRANCE','GERMANY')`.
//! The OR itself cannot be pushed below a join because it references several
//! tables, but each derived IN-list references one column and pushes all the
//! way to that table's scan (TPC-H Q07: both nation scans drop from 25 rows'
//! worth of join fan-out to 2; Q19's brand/container lists push to part).
//!
//! The rewrite is sound (the derived conjunct is implied by the OR) and
//! idempotent: derived conjuncts already present are not added again.

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::planner::{BinaryOp, Expr, FilterNode, LogicalPlan};
use std::collections::HashMap;
use std::sync::Arc;

pub struct DeriveOrPredicates;

impl OptimizerRule for DeriveOrPredicates {
    fn name(&self) -> &str {
        "DeriveOrPredicates"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        Ok(rewrite(plan))
    }
}

fn rewrite(plan: &LogicalPlan) -> LogicalPlan {
    let children: Vec<Arc<LogicalPlan>> = plan
        .children()
        .iter()
        .map(|c| Arc::new(rewrite(c)))
        .collect();
    let plan = if children.is_empty() {
        plan.clone()
    } else {
        plan.with_new_children(children)
    };

    if let LogicalPlan::Filter(node) = &plan {
        let augmented = augment_predicate(&node.predicate);
        if let Some(predicate) = augmented {
            return LogicalPlan::Filter(FilterNode {
                input: Arc::clone(&node.input),
                predicate,
            });
        }
    }
    plan
}

/// Add derived conjuncts for every OR conjunct of the predicate.
/// Returns None when nothing new can be derived.
fn augment_predicate(predicate: &Expr) -> Option<Expr> {
    let mut conjuncts = Vec::new();
    flatten_and(predicate, &mut conjuncts);

    let existing: Vec<String> = conjuncts.iter().map(|e| e.to_string()).collect();
    let mut derived: Vec<Expr> = Vec::new();

    for c in &conjuncts {
        if is_or(c) {
            for d in derive_from_or(c) {
                let repr = d.to_string();
                if !existing.contains(&repr) && !derived.iter().any(|e| e.to_string() == repr) {
                    derived.push(d);
                }
            }
        }
    }

    if derived.is_empty() {
        return None;
    }

    let mut result = predicate.clone();
    for d in derived {
        result = Expr::BinaryExpr {
            left: Box::new(result),
            op: BinaryOp::And,
            right: Box::new(d),
        };
    }
    Some(result)
}

fn flatten_and<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryExpr {
            left,
            op: BinaryOp::And,
            right,
        } => {
            flatten_and(left, out);
            flatten_and(right, out);
        }
        other => out.push(other),
    }
}

fn flatten_or<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryExpr {
            left,
            op: BinaryOp::Or,
            right,
        } => {
            flatten_or(left, out);
            flatten_or(right, out);
        }
        other => out.push(other),
    }
}

fn is_or(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::BinaryExpr {
            op: BinaryOp::Or,
            ..
        }
    )
}

/// For an OR expression, derive one IN-list per column that is constrained by
/// an equality/IN against literals in EVERY disjunct.
fn derive_from_or(or_expr: &Expr) -> Vec<Expr> {
    let mut disjuncts = Vec::new();
    flatten_or(or_expr, &mut disjuncts);
    if disjuncts.len() < 2 {
        return vec![];
    }

    // Per disjunct: column display name -> (representative column expr, literal values)
    let mut per_disjunct: Vec<HashMap<String, (Expr, Vec<Expr>)>> = Vec::new();
    for d in &disjuncts {
        let mut cols: HashMap<String, (Expr, Vec<Expr>)> = HashMap::new();
        let mut parts = Vec::new();
        flatten_and(d, &mut parts);
        for p in parts {
            match p {
                Expr::BinaryExpr {
                    left,
                    op: BinaryOp::Eq,
                    right,
                } => {
                    if let (Expr::Column(_), Expr::Literal(_)) = (&**left, &**right) {
                        cols.entry(left.to_string())
                            .or_insert_with(|| ((**left).clone(), Vec::new()))
                            .1
                            .push((**right).clone());
                    } else if let (Expr::Literal(_), Expr::Column(_)) = (&**left, &**right) {
                        cols.entry(right.to_string())
                            .or_insert_with(|| ((**right).clone(), Vec::new()))
                            .1
                            .push((**left).clone());
                    }
                }
                Expr::InList {
                    expr,
                    list,
                    negated: false,
                } => {
                    if matches!(&**expr, Expr::Column(_))
                        && list.iter().all(|v| matches!(v, Expr::Literal(_)))
                    {
                        cols.entry(expr.to_string())
                            .or_insert_with(|| ((**expr).clone(), Vec::new()))
                            .1
                            .extend(list.iter().cloned());
                    }
                }
                _ => {}
            }
        }
        per_disjunct.push(cols);
    }

    // Columns constrained in every disjunct → union of values
    let first = &per_disjunct[0];
    let mut derived = Vec::new();
    for (name, (col_expr, _)) in first {
        if !per_disjunct.iter().all(|m| m.contains_key(name)) {
            continue;
        }
        let mut values: Vec<Expr> = Vec::new();
        for m in &per_disjunct {
            for v in &m[name].1 {
                if !values.contains(v) {
                    values.push(v.clone());
                }
            }
        }
        if values.is_empty() || values.len() > 20 {
            continue;
        }
        derived.push(Expr::InList {
            expr: Box::new(col_expr.clone()),
            list: values,
            negated: false,
        });
    }
    derived
}
