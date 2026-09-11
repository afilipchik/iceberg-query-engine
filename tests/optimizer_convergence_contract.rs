use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use query_engine::{
    planner::{BinaryOp, Expr, LogicalPlan},
    ExecutionContext,
};
use std::sync::Arc;
fn context() -> ExecutionContext {
    let mut ctx = ExecutionContext::new().with_parallel_partitions(3);
    for (name, ids, values) in [
        (
            "a",
            vec![1, 1, 2, 3, 4],
            vec![Some("A"), Some("A"), Some("B"), None, Some("A")],
        ),
        (
            "b",
            vec![1, 1, 2, 3],
            vec![Some("B"), Some("C"), Some("A"), Some("A")],
        ),
    ] {
        let batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(ids)) as ArrayRef),
            ("v", Arc::new(StringArray::from(values)) as ArrayRef),
        ])
        .unwrap();
        ctx.register_table(
            name,
            batch.schema(),
            vec![batch.slice(0, 2), batch.slice(2, batch.num_rows() - 2)],
        );
    }
    ctx
}
fn conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::BinaryExpr {
        left,
        op: BinaryOp::And,
        right,
    } = expr
    {
        conjuncts(left, out);
        conjuncts(right, out);
    } else {
        out.push(expr);
    }
}
fn assert_scan_predicates_unique(plan: &LogicalPlan) {
    if let LogicalPlan::Scan(node) = plan {
        if let Some(expr) = &node.filter {
            let mut parts = vec![];
            conjuncts(expr, &mut parts);
            for (i, p) in parts.iter().enumerate() {
                if matches!(p, Expr::InList { .. }) {
                    assert!(!parts[..i].contains(p), "duplicate scan predicate: {p}");
                }
            }
        }
    }
    for child in plan.children() {
        assert_scan_predicates_unique(child);
    }
}
#[tokio::test]
async fn derived_or_pipeline_is_bounded_and_preserves_duplicates_and_nulls() {
    let ctx = context();
    let sql="SELECT a.id FROM a JOIN b ON a.id=b.id WHERE (a.v='A' AND b.v='B') OR (a.v='B' AND b.v='A') ORDER BY a.id";
    let plan = ctx.optimized_plan(sql).unwrap();
    assert_scan_predicates_unique(&plan);
    let result = ctx.sql(sql).await.unwrap();
    let actual: Vec<_> = result
        .batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(actual, vec![1, 1, 2]);
}
#[tokio::test]
async fn outer_join_null_extension_remains_a_pushdown_barrier() {
    let ctx = context();
    let result=ctx.sql("SELECT a.id FROM a LEFT JOIN b ON a.id=b.id WHERE b.v IS NULL OR a.v='B' ORDER BY a.id").await.unwrap();
    let actual: Vec<_> = result
        .batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    assert_eq!(actual, vec![2, 4]);
}
