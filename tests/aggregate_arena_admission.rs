//! Public physical boundaries must propagate state admission failure without
//! selecting an unreserved fallback or replaying a consumed input.
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{stream, TryStreamExt};
use query_engine::{
    execution::MemoryPool,
    physical::{
        operators::{hash_agg, spillable},
        HashAggregateExec, PhysicalOperator, RecordBatchStream,
    },
    planner::{AggregateFunction, Expr},
    ExecutionConfig, Result,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
struct Input {
    batch: RecordBatch,
    calls: [AtomicUsize; 2],
}
#[async_trait]
impl PhysicalOperator for Input {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
    fn name(&self) -> &str {
        "ArenaAdmissionInput"
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn output_partitions(&self) -> usize {
        2
    }
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, partition)?;
        self.calls[partition].fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(stream::iter([Ok(self.batch.clone())])))
    }
}
fn fixture() -> Arc<Input> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, false),
    ]));
    Arc::new(Input {
        batch: RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from_iter_values((0..65536).map(|v| v % 4096))),
                Arc::new(Int64Array::from(vec![1i64; 65536])),
            ],
        )
        .unwrap(),
        calls: [AtomicUsize::new(0), AtomicUsize::new(0)],
    })
}
fn output() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("s", DataType::Int64, true),
    ]))
}
async fn check(operator: &dyn PhysicalOperator, input: &Input, pool: &MemoryPool) {
    let result = match operator.execute(0).await {
        Ok(stream) => stream.try_collect::<Vec<RecordBatch>>().await,
        Err(error) => Err(error),
    };
    let error = result.unwrap_err();
    assert!(error.to_string().contains("arena query budget"), "{error}");
    assert_eq!(pool.used(), 0);
    for count in &input.calls {
        assert_eq!(count.load(Ordering::SeqCst), 1, "input was replayed");
    }
}

#[tokio::test]
async fn materialized_morsel_refusal_does_not_fall_through_to_vector_table() {
    let input = fixture();
    let pool = Arc::new(MemoryPool::new_named("arena query budget", 4096));
    let op = HashAggregateExec::new(
        input.clone(),
        vec![Expr::column("g")],
        vec![hash_agg::AggregateExpr {
            func: AggregateFunction::Sum,
            input: Expr::column("v"),
            distinct: false,
            second_arg: None,
        }],
        output(),
    )
    .with_memory_pool(pool.clone());
    check(&op, &input, &pool).await;
}

#[tokio::test]
async fn fused_worker_refusal_is_observable_without_tracing_and_does_not_replay() {
    let input = fixture();
    let pool = Arc::new(MemoryPool::new_named("arena query budget", 4096));
    // A large operator threshold avoids the legacy group-count restart. The
    // small query hierarchy must still refuse actual state allocation by name.
    let config = ExecutionConfig::new().with_memory_limit(64 << 20);
    let op = spillable::SpillableHashAggregateExec::new(
        input.clone(),
        vec![Expr::column("g")],
        vec![spillable::AggregateExpr {
            func: AggregateFunction::Sum,
            input: Expr::column("v"),
            distinct: false,
            second_arg: None,
        }],
        output(),
        pool.clone(),
        config,
    );
    check(&op, &input, &pool).await;
}
