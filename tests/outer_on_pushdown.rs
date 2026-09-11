use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use query_engine::{planner::LogicalPlan, ExecutionContext};
use std::sync::Arc;

fn context() -> ExecutionContext {
    context_with_empty(None)
}

fn context_with_empty(empty: Option<&str>) -> ExecutionContext {
    let mut ctx = ExecutionContext::new().with_parallel_partitions(3);
    for name in ["l", "r"] {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    Some(1),
                    Some(2),
                    Some(3),
                    None,
                ])) as ArrayRef,
            ),
            (
                "label",
                Arc::new(StringArray::from(vec![
                    Some("keep"),
                    Some("drop"),
                    None,
                    Some("keep"),
                    Some("keep"),
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        let batch = if empty == Some(name) {
            batch.slice(0, 0)
        } else {
            batch
        };
        let split = batch.num_rows().min(2);
        ctx.register_table(
            name,
            batch.schema(),
            vec![
                batch.slice(0, 0),
                batch.slice(0, split),
                batch.slice(split, batch.num_rows() - split),
            ],
        );
    }
    ctx
}

async fn rows(ctx: &ExecutionContext, sql: &str) -> Vec<(Option<i64>, Option<i64>)> {
    let mut rows = Vec::new();
    for batch in ctx.sql(sql).await.unwrap().batches {
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                (!a.is_null(i)).then(|| a.value(i)),
                (!b.is_null(i)).then(|| b.value(i)),
            ));
        }
    }
    rows.sort();
    rows
}

#[tokio::test]
async fn where_null_and_empty_input_semantics_are_separate_from_on() {
    let ctx = context();
    assert_eq!(
        rows(
            &ctx,
            "SELECT l.id,r.id FROM l LEFT JOIN r ON l.id=r.id WHERE r.label LIKE 'keep%'"
        )
        .await,
        vec![(Some(1), Some(1)), (Some(1), Some(1)), (Some(3), Some(3))]
    );
    let mut expected = vec![
        (Some(1), None),
        (Some(1), None),
        (Some(2), Some(2)),
        (Some(3), None),
        (None, None),
    ];
    expected.sort();
    assert_eq!(
        rows(
            &ctx,
            "SELECT l.id,r.id FROM l LEFT JOIN r ON l.id=r.id AND r.label IS NULL"
        )
        .await,
        expected
    );
    for (kind, nonpreserved, preserved) in [("LEFT", "r", "l"), ("RIGHT", "l", "r")] {
        let sql=format!("SELECT l.id,r.id FROM l {kind} JOIN r ON l.id=r.id AND {nonpreserved}.label LIKE 'keep%'");
        let mut expected = vec![
            (Some(1), None),
            (Some(1), None),
            (Some(2), None),
            (Some(3), None),
            (None, None),
        ];
        if kind == "RIGHT" {
            expected = expected.into_iter().map(|(a, b)| (b, a)).collect();
        }
        expected.sort();
        assert_eq!(
            rows(&context_with_empty(Some(nonpreserved)), &sql).await,
            expected
        );
        assert!(rows(&context_with_empty(Some(preserved)), &sql)
            .await
            .is_empty());
    }
}

fn residual(plan: &LogicalPlan) -> Option<bool> {
    if let LogicalPlan::Join(join) = plan {
        return Some(join.filter.is_some());
    }
    plan.children().into_iter().find_map(residual)
}

#[tokio::test]
async fn nonpreserved_on_filter_moves_without_losing_unmatched_or_duplicate_rows() {
    for (kind, side) in [("LEFT", "r"), ("RIGHT", "l")] {
        let ctx = context();
        let sql = format!(
            "SELECT l.id, r.id FROM l {kind} JOIN r ON l.id=r.id AND {side}.label LIKE 'keep%'"
        );
        let optimized = ctx.optimized_plan(&sql).unwrap();
        assert_eq!(residual(&optimized), Some(false), "{optimized}");
        let mut actual = Vec::new();
        for batch in ctx.sql(&sql).await.unwrap().batches {
            let a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let b = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                actual.push((
                    (!a.is_null(i)).then(|| a.value(i)),
                    (!b.is_null(i)).then(|| b.value(i)),
                ));
            }
        }
        let mut expected = vec![
            (Some(1), Some(1)),
            (Some(1), Some(1)),
            (Some(2), None),
            (Some(3), Some(3)),
            (None, None),
        ];
        if kind == "RIGHT" {
            expected = expected.into_iter().map(|(a, b)| (b, a)).collect();
        }
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected, "{sql}");
    }
}

#[test]
fn preserved_cross_side_and_unsafe_on_filters_stay_at_join() {
    let ctx = context();
    for (kind, filter) in [
        ("LEFT", "l.label LIKE 'keep%'"),
        ("RIGHT", "r.label LIKE 'keep%'"),
        ("FULL", "r.label LIKE 'keep%'"),
        ("LEFT", "l.label <> r.label"),
        ("LEFT", "r.id / 0 > 1"),
        ("LEFT", "random() > r.id"),
    ] {
        let sql = format!("SELECT l.id,r.id FROM l {kind} JOIN r ON l.id=r.id AND {filter}");
        let optimized = ctx.optimized_plan(&sql).unwrap();
        assert_eq!(residual(&optimized), Some(true), "{sql}: {optimized}");
    }
}
