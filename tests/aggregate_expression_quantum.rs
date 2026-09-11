//! Large provider batches must not force equally large expression temporaries.
use arrow::{
    array::{Array, ArrayRef, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use query_engine::{
    execution::MemoryPool,
    physical::{
        operators::{spillable, MemoryTableExec},
        PhysicalOperator,
    },
    planner::{AggregateFunction, Expr, ScalarValue},
    ExecutionConfig,
};
use std::sync::Arc;

async fn check(budget: usize) {
    let rows = 16384;
    let batch = RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(Int64Array::from(
            (0..rows)
                .map(|i| (i % 2 == 0).then_some(1))
                .collect::<Vec<_>>(),
        )) as ArrayRef,
    )])
    .unwrap();
    let input = Arc::new(MemoryTableExec::new(
        "input",
        batch.schema(),
        vec![batch.slice(0, 0), batch],
        None,
    ));
    let pool = Arc::new(MemoryPool::new_named("expression quantum", budget));
    let op = spillable::SpillableHashAggregateExec::new(
        input,
        vec![Expr::literal(ScalarValue::Int64(7))],
        vec![
            spillable::AggregateExpr {
                func: AggregateFunction::Count,
                input: Expr::Wildcard,
                distinct: false,
                second_arg: None,
            },
            spillable::AggregateExpr {
                func: AggregateFunction::Sum,
                input: Expr::column("v").add(Expr::literal(ScalarValue::Int64(1))),
                distinct: false,
                second_arg: None,
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("n", DataType::Int64, false),
            Field::new("s", DataType::Int64, true),
        ])),
        pool.clone(),
        ExecutionConfig::new().with_memory_limit(budget),
    );
    let output = op
        .execute(0)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut actual = Vec::new();
    for b in &output {
        for r in 0..b.num_rows() {
            let key = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            assert!(!key.is_null(r));
            assert_eq!(key.value(r), 7);
            assert!(!b.column(1).is_null(r));
            assert!(!b.column(2).is_null(r));
            actual.push((
                b.column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(r),
                b.column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(r),
            ));
        }
    }
    assert_eq!(actual, vec![(rows as i64, rows as i64)]);
    drop(output);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn count_and_computed_sum_fit_with_full_source_still_retained() {
    check(256 * 1024).await;
}

#[tokio::test]
async fn larger_budget_preserves_exact_values_across_larger_evaluation_slices() {
    check(64 * 1024 * 1024).await;
}
