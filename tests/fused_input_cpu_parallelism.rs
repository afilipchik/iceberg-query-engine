//! Multiple input partitions must permit CPU work to overlap when their copied
//! output has an explicit bound; FuturesUnordered/SelectAll alone do not do this.
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{stream, TryStreamExt};
use query_engine::{
    execution::MemoryPool,
    physical::{
        operators::spillable, queue_layout::QueueCopyBound, PhysicalOperator, RecordBatchStream,
    },
    planner::{AggregateFunction, Expr},
    ExecutionConfig, Result,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
struct BoundedCpuInput {
    batch: RecordBatch,
    calls: [AtomicUsize; 4],
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    fail_after_prefix: bool,
    known_static: bool,
    prepared: Option<query_engine::physical::PreparedOutputBound>,
}

#[async_trait]
impl PhysicalOperator for BoundedCpuInput {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
    fn name(&self) -> &str {
        "BoundedCpuInput"
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn output_partitions(&self) -> usize {
        4
    }
    fn pool_independent_queue_copy_bound(&self) -> Option<QueueCopyBound> {
        self.known_static
            .then(|| {
                QueueCopyBound::from_batches(
                    &self.batch.schema(),
                    std::slice::from_ref(&self.batch),
                )
            })
            .flatten()
    }
    async fn prepare_queue_input(
        &self,
    ) -> Result<Option<query_engine::physical::PreparedQueueInput>> {
        let Some(output) = self.prepared.clone() else {
            return Ok(None);
        };
        let mut streams = Vec::new();
        for p in 0..4 {
            streams.push(self.execute(p).await?);
        }
        Ok(Some(query_engine::physical::PreparedQueueInput {
            streams,
            output,
        }))
    }
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, partition)?;
        assert_eq!(self.calls[partition].fetch_add(1, Ordering::SeqCst), 0);
        let batch = self.batch.clone();
        let active = self.active.clone();
        let peak = self.peak.clone();
        let fail = self.fail_after_prefix && partition == 0;
        Ok(Box::pin(stream::iter((0..4).map(move |index| {
            let now = active.fetch_add(1, Ordering::SeqCst) + 1;
            peak.fetch_max(now, Ordering::SeqCst);
            // Deliberately synchronous work within poll, as in Arrow Parquet
            // decoding. A short sleep makes overlap observable without treating
            // elapsed speed as an assertion or consuming a core in a spin loop.
            std::thread::sleep(std::time::Duration::from_millis(20));
            active.fetch_sub(1, Ordering::SeqCst);
            if fail && index == 1 {
                return Err(query_engine::QueryError::Storage(
                    "parallel source prefix failure".into(),
                ));
            }
            Ok(batch.clone())
        }))))
    }
}

async fn run(
    known_static: bool,
    prepared: Option<query_engine::physical::PreparedOutputBound>,
    minimum: usize,
    maximum: usize,
    fail_after_prefix: bool,
) {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1), None])),
            Arc::new(Int64Array::from(vec![Some(7), None, Some(3)])),
        ],
    )
    .unwrap();
    let input = Arc::new(BoundedCpuInput {
        batch,
        calls: std::array::from_fn(|_| AtomicUsize::new(0)),
        active: Arc::new(AtomicUsize::new(0)),
        peak: Arc::new(AtomicUsize::new(0)),
        known_static,
        prepared,
        fail_after_prefix,
    });
    let pool = Arc::new(MemoryPool::new_named(
        "bounded parallel input",
        16 * 1024 * 1024,
    ));
    let operator = spillable::SpillableHashAggregateExec::new(
        input.clone(),
        vec![Expr::column("g")],
        vec![spillable::AggregateExpr {
            func: AggregateFunction::Sum,
            input: Expr::column("v"),
            distinct: false,
            second_arg: None,
        }],
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("s", DataType::Int64, true),
        ])),
        pool.clone(),
        ExecutionConfig::new().with_memory_limit(16 * 1024 * 1024),
    );
    let result = match operator.execute(0).await {
        Ok(stream) => stream.try_collect::<Vec<_>>().await,
        Err(error) => Err(error),
    };
    if fail_after_prefix {
        let error = result.expect_err("failed parallel input became a successful result");
        assert!(
            matches!(error.root(), query_engine::QueryError::Storage(message) if message == "parallel source prefix failure")
        );
        assert!(input.calls.iter().all(|n| n.load(Ordering::SeqCst) <= 1));
        assert_eq!(
            input.active.load(Ordering::SeqCst),
            0,
            "running producer outlived error return"
        );
        assert_eq!(pool.used(), 0, "parallel error leaked owned state/batches");
        return;
    }
    let batches = result.unwrap();
    let mut rows = Vec::new();
    for batch in &batches {
        let g = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let s = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            rows.push(((!g.is_null(row)).then(|| g.value(row)), s.value(row)));
        }
    }
    rows.sort_unstable();
    assert_eq!(rows, vec![(None, 48), (Some(1), 112)]);
    assert!(input.calls.iter().all(|n| n.load(Ordering::SeqCst) == 1));
    assert_eq!(input.active.load(Ordering::SeqCst), 0);
    drop(batches);
    assert_eq!(
        pool.used(),
        0,
        "input/output reservations leaked after completion"
    );
    let peak = input.peak.load(Ordering::SeqCst);
    assert!(
        (minimum..=maximum).contains(&peak),
        "partition peak {peak}, expected {minimum}..={maximum}"
    );
}

async fn verify(
    known_static: bool,
    prepared: Option<query_engine::physical::PreparedOutputBound>,
    minimum: usize,
    maximum: usize,
) {
    run(known_static, prepared, minimum, maximum, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parallel_prefix_failure_joins_producers_and_releases_every_charge() {
    run(true, None, 0, 4, true).await;
    run(
        false,
        Some(query_engine::physical::PreparedOutputBound::Bytes(
            6 * 1024 * 1024,
        )),
        0,
        2,
        true,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_partition_polls_overlap_without_replay_or_result_changes() {
    verify(true, None, 2, 4).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_frontier_concurrency_follows_available_admission() {
    // At most two 6MiB slots fit beneath the16MiB shared query limit.
    verify(
        false,
        Some(query_engine::physical::PreparedOutputBound::Bytes(
            6 * 1024 * 1024,
        )),
        2,
        2,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepared_streams_survive_insufficient_parallel_admission() {
    verify(
        false,
        Some(query_engine::physical::PreparedOutputBound::Bytes(
            32 * 1024 * 1024,
        )),
        1,
        1,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_prepared_output_never_inherits_an_unprepared_bound() {
    verify(
        true,
        Some(query_engine::physical::PreparedOutputBound::Unknown),
        1,
        1,
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_unprepared_source_remains_serial() {
    verify(false, None, 1, 1).await;
}
