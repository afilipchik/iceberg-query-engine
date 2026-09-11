//! Positive initialized MemoryTable NOT IN routing and independent SQL values.
use arrow::array::{Array, Int64Array};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use query_engine::{physical::PhysicalOperator, ExecutionContext};
use std::sync::Arc;

fn batch(name: &str, values: Vec<Option<i64>>) -> RecordBatch {
    RecordBatch::try_from_iter([(name, Arc::new(Int64Array::from(values)) as Arc<dyn Array>)])
        .unwrap()
}
fn context(rhs: Vec<Option<i64>>) -> ExecutionContext {
    let mut ctx = ExecutionContext::with_memory_limit(8 << 20).with_parallel_partitions(3);
    let lhs = batch("x", vec![Some(1), Some(1), Some(2), None, Some(i64::MIN)]);
    let rhs = batch("k", rhs);
    ctx.register_table(
        "lhs",
        lhs.schema(),
        vec![lhs.slice(0, 2), lhs.slice(2, 1), lhs.slice(3, 2)],
    );
    ctx.register_table(
        "rhs",
        rhs.schema(),
        vec![
            rhs.slice(0, rhs.num_rows() / 2),
            rhs.slice(rhs.num_rows() / 2, rhs.num_rows() - rhs.num_rows() / 2),
        ],
    );
    ctx
}
fn find_filter(plan: Arc<dyn PhysicalOperator>) -> Option<Arc<dyn PhysicalOperator>> {
    if plan.name() == "FilterExec" || plan.name() == "Filter" {
        return Some(plan);
    }
    plan.children().into_iter().find_map(find_filter)
}
#[tokio::test]
async fn planner_installs_prepared_membership_without_static_subquery_certificate() {
    let ctx = context(vec![Some(2), Some(2)]);
    let plan = ctx
        .physical_plan("SELECT x FROM lhs WHERE x NOT IN (SELECT k FROM rhs)")
        .unwrap();
    let filter = find_filter(plan).expect("root NOT IN Filter");
    assert!(filter.pool_independent_queue_copy_bound().is_none());
    let prepared = filter
        .prepare_queue_input()
        .await
        .unwrap()
        .expect("pinned MemoryTable initializer");
    assert!(prepared.output.max_bytes().is_some());
    assert_eq!(prepared.streams.len(), filter.output_partitions());
    let mut values = Vec::new();
    for mut stream in prepared.streams {
        while let Some(batch) = stream.try_next().await.unwrap() {
            let a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            values.extend(a.iter());
        }
    }
    values.sort();
    assert_eq!(values, vec![Some(i64::MIN), Some(1), Some(1)]);
}
#[tokio::test]
async fn independent_sql_not_in_null_empty_and_duplicates_across_batches() {
    for (rhs, expected) in [
        (
            vec![Some(2), Some(2)],
            vec![Some(i64::MIN), Some(1), Some(1)],
        ),
        (vec![Some(2), None], vec![]),
        (
            vec![],
            vec![None, Some(i64::MIN), Some(1), Some(1), Some(2)],
        ),
    ] {
        let result = context(rhs)
            .sql("SELECT x FROM lhs WHERE x NOT IN (SELECT k FROM rhs)")
            .await
            .unwrap();
        let mut values = Vec::new();
        for batch in result.batches {
            values.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter(),
            );
        }
        values.sort();
        assert_eq!(values, expected);
    }
}
