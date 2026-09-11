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
use crate::planner::{BinaryOp, Column, Expr, FilterNode, LogicalPlan};
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

    let mut derived: Vec<Expr> = Vec::new();

    for c in &conjuncts {
        if is_or(c) {
            for d in derive_from_or(c) {
                if !conjuncts.iter().any(|existing| *existing == &d) && !derived.contains(&d) {
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

    // Semantic column identity determines implication. Preserve first occurrence
    // order explicitly; HashMap iteration is not a stable rewrite order.
    let mut per_disjunct: Vec<HashMap<Column, (Expr, Vec<Expr>)>> = Vec::new();
    let mut first_order = Vec::new();
    for (index, d) in disjuncts.iter().enumerate() {
        let mut cols: HashMap<Column, (Expr, Vec<Expr>)> = HashMap::new();
        let mut parts = Vec::new();
        flatten_and(d, &mut parts);
        for p in parts {
            match p {
                Expr::BinaryExpr {
                    left,
                    op: BinaryOp::Eq,
                    right,
                } => {
                    if let (Expr::Column(column), Expr::Literal(_)) = (&**left, &**right) {
                        if index == 0 && !cols.contains_key(column) {
                            first_order.push(column.clone());
                        }
                        cols.entry(column.clone())
                            .or_insert_with(|| ((**left).clone(), Vec::new()))
                            .1
                            .push((**right).clone());
                    } else if let (Expr::Literal(_), Expr::Column(column)) = (&**left, &**right) {
                        if index == 0 && !cols.contains_key(column) {
                            first_order.push(column.clone());
                        }
                        cols.entry(column.clone())
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
                    if let Expr::Column(column) = &**expr {
                        if !list.iter().all(|v| matches!(v, Expr::Literal(_))) {
                            continue;
                        }
                        if index == 0 && !cols.contains_key(column) {
                            first_order.push(column.clone());
                        }
                        cols.entry(column.clone())
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
    for name in first_order {
        let (col_expr, _) = &first[&name];
        if !per_disjunct.iter().all(|m| m.contains_key(&name)) {
            continue;
        }
        let mut values: Vec<Expr> = Vec::new();
        for m in &per_disjunct {
            for v in &m[&name].1 {
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

// Uses the private rule helper and an independent three-valued evaluator.
#[cfg(test)]
mod structural_or_identity_contract {
    use super::*;
    use crate::planner::{Column, ScalarValue};
    fn eq(column: &Column, value: i64) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(Expr::Column(column.clone())),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(value))),
        }
    }
    fn or(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(left),
            op: BinaryOp::Or,
            right: Box::new(right),
        }
    }
    fn scalar(expr: &Expr, row: &[(Column, Option<i64>)]) -> Option<i64> {
        match expr {
            Expr::Column(c) => row.iter().find(|(key, _)| key == c).unwrap().1,
            Expr::Literal(ScalarValue::Int64(v)) => Some(*v),
            Expr::Literal(ScalarValue::Null) => None,
            _ => panic!("fixture contains an unexpected scalar"),
        }
    }
    fn and(a: Option<bool>, b: Option<bool>) -> Option<bool> {
        match (a, b) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        }
    }
    fn union(a: Option<bool>, b: Option<bool>) -> Option<bool> {
        match (a, b) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        }
    }
    fn evaluate(expr: &Expr, row: &[(Column, Option<i64>)]) -> Option<bool> {
        match expr {
            Expr::BinaryExpr {
                left,
                op: BinaryOp::Eq,
                right,
            } => scalar(left, row)
                .zip(scalar(right, row))
                .map(|(a, b)| a == b),
            Expr::BinaryExpr {
                left,
                op: BinaryOp::And,
                right,
            } => and(evaluate(left, row), evaluate(right, row)),
            Expr::BinaryExpr {
                left,
                op: BinaryOp::Or,
                right,
            } => union(evaluate(left, row), evaluate(right, row)),
            Expr::InList {
                expr,
                list,
                negated: false,
            } => {
                let v = scalar(expr, row);
                list.iter().fold(Some(false), |state, item| {
                    union(state, v.zip(scalar(item, row)).map(|(a, b)| a == b))
                })
            }
            _ => panic!("fixture contains an unexpected Boolean expression"),
        }
    }
    #[test]
    fn distinct_columns_with_equal_display_do_not_create_false_implication() {
        let a = Column::new_qualified("a", "b.c");
        let b = Column::new_qualified("a.b", "c");
        assert_ne!(a, b);
        assert_eq!(a.to_string(), b.to_string());
        let original = or(eq(&a, 1), eq(&b, 2));
        let augmented = augment_predicate(&original).unwrap_or_else(|| original.clone());
        for av in [None, Some(1), Some(2), Some(3)] {
            for bv in [None, Some(1), Some(2), Some(3)] {
                let row = [(a.clone(), av), (b.clone(), bv)];
                assert_eq!(
                    evaluate(&original, &row) == Some(true),
                    evaluate(&augmented, &row) == Some(true),
                    "row={row:?}"
                );
            }
        }
        assert!(augment_predicate(&original).is_none());
    }
    #[test]
    fn one_structural_column_in_every_branch_still_derives() {
        let a = Column::new_qualified("a", "b.c");
        let original = or(eq(&a, 1), eq(&a, 2));
        let augmented = augment_predicate(&original).expect("positive control must derive");
        for value in [None, Some(1), Some(2), Some(3)] {
            let row = [(a.clone(), value)];
            assert_eq!(evaluate(&original, &row), evaluate(&augmented, &row));
        }
    }

    fn constraint(column: &Column, value: i64, shape: usize) -> Expr {
        match shape {
            0 => eq(column, value),
            1 => Expr::BinaryExpr {
                left: Box::new(Expr::Literal(ScalarValue::Int64(value))),
                op: BinaryOp::Eq,
                right: Box::new(Expr::Column(column.clone())),
            },
            _ => Expr::InList {
                expr: Box::new(Expr::Column(column.clone())),
                list: vec![
                    Expr::Literal(ScalarValue::Int64(value)),
                    Expr::Literal(ScalarValue::Null),
                    Expr::Literal(ScalarValue::Int64(value)),
                ],
                negated: false,
            },
        }
    }
    fn conjunction(left: Expr, right: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(left),
            op: BinaryOp::And,
            right: Box::new(right),
        }
    }
    #[test]
    fn implication_preserves_filter_truth_across_constraint_shapes_and_nulls() {
        let a = Column::new_qualified("a", "b.c");
        let b = Column::new_qualified("a.b", "c");
        for left_shape in 0..3 {
            for right_shape in 0..3 {
                let left = constraint(&a, 1, left_shape);
                let right = constraint(&b, 2, right_shape);
                for original in [
                    or(left.clone(), right.clone()),
                    or(right.clone(), left.clone()),
                    or(conjunction(left, eq(&b, 3)), conjunction(right, eq(&a, 3))),
                ] {
                    let augmented =
                        augment_predicate(&original).unwrap_or_else(|| original.clone());
                    for av in [None, Some(1), Some(2), Some(3)] {
                        for bv in [None, Some(1), Some(2), Some(3)] {
                            let row = [(a.clone(), av), (b.clone(), bv)];
                            assert_eq!(
                                evaluate(&original, &row) == Some(true),
                                evaluate(&augmented, &row) == Some(true),
                                "original={original:?}; row={row:?}"
                            );
                        }
                    }
                    assert!(
                        augment_predicate(&augmented).is_none(),
                        "second augmentation must be idempotent"
                    );
                }
            }
        }
    }
    #[test]
    fn displayed_existing_conjunct_does_not_hide_another_columns_derivation() {
        let a = Column::new_qualified("a", "b.c");
        let b = Column::new_qualified("a.b", "c");
        let existing = Expr::InList {
            expr: Box::new(Expr::Column(a)),
            list: vec![
                Expr::Literal(ScalarValue::Int64(1)),
                Expr::Literal(ScalarValue::Int64(2)),
            ],
            negated: false,
        };
        let original = conjunction(existing, or(eq(&b, 1), eq(&b, 2)));
        let augmented = augment_predicate(&original).expect("different column must still derive");
        assert!(augment_predicate(&augmented).is_none());
    }
    #[test]
    fn first_occurrence_order_is_stable_across_repeated_rewrites() {
        let a = Column::new_qualified("z", "second_lexically");
        let b = Column::new_qualified("a", "first_lexically");
        let original = or(
            conjunction(eq(&a, 1), eq(&b, 2)),
            conjunction(eq(&b, 3), eq(&a, 2)),
        );
        for _ in 0..64 {
            let derived = derive_from_or(&original);
            let columns: Vec<_> = derived
                .iter()
                .map(|e| match e {
                    Expr::InList { expr, .. } => &**expr,
                    _ => panic!("expected derived IN"),
                })
                .collect();
            assert_eq!(
                columns,
                vec![&Expr::Column(a.clone()), &Expr::Column(b.clone())]
            );
        }
    }
}
