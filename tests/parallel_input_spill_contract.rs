//! The existing exact spill oracle exercised through admitted parallel input pulls.
//! Source data/expected values match the serial transition fixture deliberately.
use arrow::{
    array::{Array, Decimal128Array, Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{stream, TryStreamExt};
use query_engine::{
    execution::MemoryPool,
    physical::{operators::spillable, PhysicalOperator, RecordBatchStream},
    planner::{AggregateFunction as A, Expr},
    ExecutionConfig, Result,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
struct ConsumingInput {
    batches: [RecordBatch; 2],
    calls: [AtomicUsize; 2],
}
#[async_trait]
impl PhysicalOperator for ConsumingInput {
    fn schema(&self) -> SchemaRef {
        self.batches[0].schema()
    }
    fn name(&self) -> &str {
        "ConsumingBudgetTransitionInput"
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn output_partitions(&self) -> usize {
        2
    }
    fn pool_independent_queue_copy_bound(
        &self,
    ) -> Option<query_engine::physical::queue_layout::QueueCopyBound> {
        query_engine::physical::queue_layout::QueueCopyBound::from_batches(
            &self.batches[0].schema(),
            &self.batches,
        )
    }
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, partition)?;
        if self.calls[partition].fetch_add(1, Ordering::SeqCst) != 0 {
            return Ok(Box::pin(stream::empty()));
        }
        let batch = &self.batches[partition];
        let repeats = if partition == 0 { 1 } else { 3 };
        Ok(Box::pin(stream::iter(
            std::iter::once(Ok(batch.slice(0, 0)))
                .chain((0..repeats).map(|_| Ok(batch.clone())))
                .collect::<Vec<_>>(),
        )))
    }
}

async fn check(disjoint: bool) {
    let input_schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("v", DataType::Int64, true),
        Field::new("d", DataType::Decimal128(38, 2), true),
    ]));
    let batch = |value: i64| {
        RecordBatch::try_new(
            input_schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    (0..512)
                        .map(|k| (k != 511).then_some(k))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    (0..512)
                        .map(|k| (k % 7 != 0).then_some(value))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(
                    Decimal128Array::from(
                        (0..512)
                            .map(|k| (k % 7 != 0).then_some(i128::from(value) * 100))
                            .collect::<Vec<_>>(),
                    )
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
                ),
            ],
        )
        .unwrap()
    };
    let input = Arc::new(ConsumingInput {
        batches: [batch(10), batch(30)],
        calls: [AtomicUsize::new(0), AtomicUsize::new(0)],
    });
    let pool = Arc::new(MemoryPool::new_named("transition query", 32 * 1024 * 1024));
    let directory = tempfile::tempdir().unwrap();
    // Separate operator pressure from the query hierarchy, leaving enough
    // admitted working space for exact spill output and encoding scratch.
    let config = ExecutionConfig::new()
        .with_memory_limit(8 * 1024)
        .with_spill_path(directory.path().to_path_buf());
    let functions = [
        A::Count,
        A::Sum,
        A::Avg,
        A::Min,
        A::Max,
        A::AnyValue,
        A::Arbitrary,
    ];
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("c", DataType::Int64, false),
        Field::new("s", DataType::Decimal128(38, 2), true),
        Field::new("a", DataType::Float64, true),
        Field::new("lo", DataType::Int64, true),
        Field::new("hi", DataType::Int64, true),
        Field::new("pick", DataType::Int64, true),
        Field::new("arbitrary", DataType::Int64, true),
    ]));
    let operator = spillable::SpillableHashAggregateExec::new(
        input.clone(),
        vec![Expr::column("g")],
        functions
            .into_iter()
            .enumerate()
            .map(|(i, func)| spillable::AggregateExpr {
                func,
                input: Expr::column(if i == 1 { "d" } else { "v" }),
                distinct: false,
                second_arg: None,
            })
            .collect(),
        schema,
        pool.clone(),
        config,
    )
    .with_disjoint_groups(disjoint);
    let batches: Vec<RecordBatch> = operator
        .execute(0)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    for count in &input.calls {
        assert_eq!(
            count.load(Ordering::SeqCst),
            1,
            "consumed input was replayed"
        );
    }
    let mut seen = [false; 512];
    for batch in &batches {
        let g = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let c = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let s = batch
            .column(2)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        let a = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(s.scale(), 2);
        for row in 0..batch.num_rows() {
            let key = if g.is_null(row) {
                511
            } else {
                usize::try_from(g.value(row)).unwrap()
            };
            assert!(!seen[key], "duplicate output group");
            seen[key] = true;
            let empty = key % 7 == 0;
            assert_eq!(c.value(row), if empty { 0 } else { 4 });
            assert_eq!(s.is_null(row), empty);
            assert_eq!(a.is_null(row), empty);
            if !empty {
                assert_eq!(s.value(row), 10000);
                assert_eq!(a.value(row), 25.0);
            }
            for col in 4..8 {
                let v = batch
                    .column(col)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                assert_eq!(v.is_null(row), empty);
                if !empty {
                    match col {
                        4 => assert_eq!(v.value(row), 10),
                        5 => assert_eq!(v.value(row), 30),
                        _ => assert!([10, 30].contains(&v.value(row))),
                    }
                }
            }
        }
    }
    assert!(seen.iter().all(|v| *v), "missing groups");
    assert!(
        pool.spilled() > 0,
        "the operator budget must exercise actual spilling"
    );
    drop(batches);
    assert_eq!(pool.used(), 0, "transition leaked reservations");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_shared_budget_transition_preserves_consumed_state() {
    check(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn parallel_disjoint_budget_transition_preserves_consumed_state() {
    check(true).await;
}
