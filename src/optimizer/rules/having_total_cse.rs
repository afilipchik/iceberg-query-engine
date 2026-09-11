//! HAVING-threshold subquery CSE.
//!
//! `Filter(v <cmp> ScalarSubquery(Project*(Aggregate[no group, SUM(e)],
//! input)))` directly above `Aggregate(g, ..SUM(e) AS v.., input)` with the
//! SAME input plan runs the join+aggregate pipeline twice (TPC-H Q11's
//! HAVING threshold re-joins partsupp x supplier x nation). Since the
//! global SUM equals the sum of the group sums, the subquery can instead
//! aggregate the OUTER aggregate's output column — and the shared CTE
//! machinery (matching `cte_name`) materializes that output exactly once
//! for both consumers.

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::planner::{
    AggregateFunction, AggregateNode, Column, Expr, FilterNode, LogicalPlan, SubqueryAliasNode,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

static CSE_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub struct HavingTotalCse;

impl OptimizerRule for HavingTotalCse {
    fn name(&self) -> &str {
        "HavingTotalCse"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        rewrite(plan)
    }
}

fn rewrite(plan: &LogicalPlan) -> Result<LogicalPlan> {
    let children: Vec<Arc<LogicalPlan>> = plan
        .children()
        .iter()
        .map(|c| rewrite(c).map(Arc::new))
        .collect::<Result<Vec<_>>>()?;
    let plan = plan.with_new_children(children);

    if let LogicalPlan::Filter(f) = &plan {
        if let Some(r) = try_cse(f) {
            return Ok(r);
        }
    }
    Ok(plan)
}

fn strip_alias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias { expr, .. } => strip_alias(expr),
        _ => e,
    }
}

fn try_cse(filter: &FilterNode) -> Option<LogicalPlan> {
    let LogicalPlan::Aggregate(outer) = &*filter.input else {
        return None;
    };
    // Predicate: Column(v) cmp ScalarSubquery(sub)
    let Expr::BinaryExpr { left, op, right } = &filter.predicate else {
        return None;
    };
    let Expr::Column(v_col) = &**left else {
        return None;
    };
    let Expr::ScalarSubquery(sub) = &**right else {
        return None;
    };

    // v must be one of the outer SUM aggregates
    let n_groups = outer.group_by.len();
    let fields = outer.schema.fields();
    let v_idx = fields.iter().position(|f| f.name == v_col.name)?;
    if v_idx < n_groups {
        return None;
    }
    let outer_agg_expr = strip_alias(&outer.aggregates[v_idx - n_groups]);
    let Expr::Aggregate {
        func: AggregateFunction::Sum,
        args: outer_args,
        distinct: false,
    } = outer_agg_expr
    else {
        return None;
    };

    // Subquery: Project chain over a no-group Aggregate with the same SUM
    // over the same input plan
    let mut projects: Vec<&crate::planner::ProjectNode> = Vec::new();
    let mut cur: &LogicalPlan = sub;
    while let LogicalPlan::Project(p) = cur {
        projects.push(p);
        cur = &p.input;
    }
    let LogicalPlan::Aggregate(inner) = cur else {
        return None;
    };
    if !inner.group_by.is_empty() || inner.aggregates.len() != 1 {
        return None;
    }
    let inner_agg_expr = strip_alias(&inner.aggregates[0]);
    let Expr::Aggregate {
        func: AggregateFunction::Sum,
        args: inner_args,
        distinct: false,
    } = inner_agg_expr
    else {
        return None;
    };
    if inner_args != outer_args || inner.input != outer.input {
        return None;
    }

    // Shared CTE alias over the outer aggregate
    let cte_name = format!(
        "__having_total_cse_{}",
        CSE_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let shared_agg = Arc::new(LogicalPlan::Aggregate(outer.clone()));
    let make_alias = || {
        LogicalPlan::SubqueryAlias(SubqueryAliasNode {
            input: Arc::clone(&shared_agg),
            alias: cte_name.clone(),
            schema: outer.schema.clone(),
            cte_name: Some(cte_name.clone()),
        })
    };

    // New subquery: total = SUM of the outer aggregate's output column,
    // preserving the inner aggregate's output field name so the Project
    // chain above it keeps resolving.
    let inner_field_name = inner.schema.fields()[0].name.clone();
    let new_inner = LogicalPlan::Aggregate(AggregateNode {
        input: Arc::new(make_alias()),
        group_by: vec![],
        aggregates: vec![Expr::Alias {
            expr: Box::new(Expr::Aggregate {
                func: AggregateFunction::Sum,
                args: vec![Expr::Column(Column {
                    relation: None,
                    name: fields[v_idx].name.clone(),
                })],
                distinct: false,
            }),
            name: inner_field_name,
        }],
        schema: inner.schema.clone(),
    });
    let mut new_sub = new_inner;
    for p in projects.iter().rev() {
        new_sub = LogicalPlan::Project(crate::planner::ProjectNode {
            input: Arc::new(new_sub),
            exprs: p.exprs.clone(),
            schema: p.schema.clone(),
        });
    }

    Some(LogicalPlan::Filter(FilterNode {
        input: Arc::new(make_alias()),
        predicate: Expr::BinaryExpr {
            left: Box::new(Expr::Column(v_col.clone())),
            op: *op,
            right: Box::new(Expr::ScalarSubquery(Arc::new(new_sub))),
        },
    }))
}

#[cfg(test)]
mod typed_having_substitution_contract_tests {
    use super::*;
    use crate::planner::{BinaryOp, DecimalValue, PlanSchema, ScalarValue, ScanNode, SchemaField};
    use arrow::datatypes::DataType;
    fn sum(scale: i8) -> Expr {
        Expr::Aggregate {
            func: AggregateFunction::Sum,
            args: vec![Expr::Literal(ScalarValue::Decimal128(DecimalValue::new(
                10_i128.pow(scale as u32),
                scale,
            )))],
            distinct: false,
        }
    }
    fn fixture(inner_scale: i8) -> LogicalPlan {
        let source = Arc::new(LogicalPlan::Scan(ScanNode {
            table_name: "fixture".into(),
            schema: PlanSchema::new(vec![SchemaField::new("g", DataType::Int64)]),
            projection: None,
            filter: None,
        }));
        let outer = LogicalPlan::Aggregate(AggregateNode {
            input: source.clone(),
            group_by: vec![Expr::column("g")],
            aggregates: vec![sum(1)],
            schema: PlanSchema::new(vec![
                SchemaField::new("g", DataType::Int64),
                SchemaField::new("v", DataType::Decimal128(38, 1)),
            ]),
        });
        let inner = LogicalPlan::Aggregate(AggregateNode {
            input: source,
            group_by: vec![],
            aggregates: vec![sum(inner_scale)],
            schema: PlanSchema::new(vec![SchemaField::new(
                "total",
                DataType::Decimal128(38, inner_scale),
            )]),
        });
        LogicalPlan::Filter(FilterNode {
            input: Arc::new(outer),
            predicate: Expr::BinaryExpr {
                left: Box::new(Expr::column("v")),
                op: BinaryOp::Lt,
                right: Box::new(Expr::ScalarSubquery(Arc::new(inner))),
            },
        })
    }
    #[test]
    fn having_total_substitution_cannot_replace_scale_two_with_scale_one() {
        let rewritten = HavingTotalCse.optimize(&fixture(2)).unwrap();
        let LogicalPlan::Filter(filter) = rewritten else {
            panic!("expected filter")
        };
        let Expr::BinaryExpr { right, .. } = filter.predicate else {
            panic!("expected predicate")
        };
        let Expr::ScalarSubquery(sub) = *right else {
            panic!("expected subquery")
        };
        let LogicalPlan::Aggregate(aggregate) = sub.as_ref() else {
            panic!("expected scalar aggregate")
        };
        // Infer from the actual substituted input, independently of the stale
        // declared field. The old rewrite produces scale1 under a scale2 field.
        assert_eq!(
            aggregate.schema.fields()[0].data_type,
            DataType::Decimal128(38, 2)
        );
        assert_eq!(
            aggregate.aggregates[0]
                .data_type(&aggregate.input.schema())
                .unwrap(),
            DataType::Decimal128(38, 2)
        );
        assert!(
            matches!(aggregate.input.as_ref(), LogicalPlan::Scan(_)),
            "different physical scales must retain their own input expression"
        );
    }
    #[test]
    fn identical_scale_having_cse_preserves_inferred_output_type() {
        let rewritten = HavingTotalCse.optimize(&fixture(1)).unwrap();
        let LogicalPlan::Filter(filter) = rewritten else {
            panic!("expected filter")
        };
        let Expr::BinaryExpr { right, .. } = filter.predicate else {
            panic!("expected predicate")
        };
        let Expr::ScalarSubquery(sub) = *right else {
            panic!("expected subquery")
        };
        let LogicalPlan::Aggregate(aggregate) = sub.as_ref() else {
            panic!("expected aggregate")
        };
        assert_eq!(
            aggregate.aggregates[0]
                .data_type(&aggregate.input.schema())
                .unwrap(),
            DataType::Decimal128(38, 1)
        );
        assert!(matches!(
            aggregate.input.as_ref(),
            LogicalPlan::SubqueryAlias(_)
        ));
    }
}
