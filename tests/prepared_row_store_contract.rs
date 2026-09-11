//! Independent multibatch row-store preparation and retained-owner regression.
use arrow::{
    array::{ArrayRef, Int64Array},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{stream, TryStreamExt};
use query_engine::{
    execution::create_memory_pool,
    physical::{
        queue_layout::GatherCopyBound, HashJoinExec, MemoryTableExec, PhysicalOperator,
        RecordBatchStream,
    },
    planner::{Expr, JoinType},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
struct Probe {
    batch: RecordBatch,
    calls: Arc<AtomicUsize>,
    polls: Arc<AtomicUsize>,
    known: bool,
    overlap: Option<Arc<tokio::sync::Barrier>>,
}
#[async_trait]
impl PhysicalOperator for Probe {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn name(&self) -> &str {
        "RowStoreProbe"
    }
    fn output_partitions(&self) -> usize {
        3
    }
    fn resident_gather_copy_bound(&self) -> Option<GatherCopyBound> {
        self.known
            .then(|| {
                GatherCopyBound::from_batches(
                    &self.batch.schema(),
                    std::slice::from_ref(&self.batch),
                )
            })
            .flatten()
    }
    async fn execute(&self, p: usize) -> query_engine::Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, p)?;
        self.calls.fetch_add(1, Ordering::SeqCst);
        let batch = self.batch.clone();
        let polls = self.polls.clone();
        let overlap = self.overlap.clone();
        Ok(Box::pin(stream::once(async move {
            let order = polls.fetch_add(1, Ordering::SeqCst);
            if order < 2 {
                if let Some(barrier) = overlap {
                    barrier.wait().await;
                }
            }
            Ok(batch)
        })))
    }
}
fn fixture_with_overlap(
    overlap: bool,
) -> (
    Arc<HashJoinExec>,
    query_engine::execution::SharedMemoryPool,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let batches = (0..2)
        .map(|b| {
            RecordBatch::try_from_iter([
                (
                    "bk",
                    Arc::new(Int64Array::from_iter_values(
                        (b * 50000..(b + 1) * 50000).map(|i| i as i64),
                    )) as ArrayRef,
                ),
                (
                    "payload",
                    Arc::new(Int64Array::from_iter_values(
                        (b * 50000..(b + 1) * 50000).map(|i| -(i as i64) - 1),
                    )) as ArrayRef,
                ),
            ])
            .unwrap()
        })
        .collect::<Vec<_>>();
    let build = Arc::new(MemoryTableExec::new(
        "build",
        batches[0].schema(),
        batches,
        None,
    ));
    let calls = Arc::new(AtomicUsize::new(0));
    let polls = Arc::new(AtomicUsize::new(0));
    let probe = Arc::new(Probe {
        batch: RecordBatch::try_from_iter([(
            "pk",
            Arc::new(Int64Array::from(vec![Some(1), Some(50001), Some(1), None])) as ArrayRef,
        )])
        .unwrap(),
        calls: calls.clone(),
        polls: polls.clone(),
        known: true,
        overlap: overlap.then(|| Arc::new(tokio::sync::Barrier::new(2))),
    });
    let pool = create_memory_pool(32 * 1024 * 1024);
    let join = Arc::new(
        HashJoinExec::new(
            build,
            probe,
            vec![(Expr::column("bk"), Expr::column("pk"))],
            JoinType::Inner,
        )
        .with_memory_pool(pool.clone()),
    );
    (join, pool, calls, polls)
}
async fn collect(streams: Vec<RecordBatchStream>) -> Vec<(i64, i64, i64)> {
    let mut result = vec![];
    for mut stream in streams {
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert_eq!(batch.num_columns(), 3);
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
            let c = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                result.push((a.value(i), b.value(i), c.value(i)));
            }
        }
    }
    result.sort();
    result
}
fn expected() -> Vec<(i64, i64, i64)> {
    let mut rows = vec![];
    for _ in 0..3 {
        rows.extend([(1, -2, 1), (1, -2, 1), (50001, -50002, 50001)]);
    }
    rows.sort();
    rows
}
#[tokio::test]
async fn multibatch_row_store_prepared_exact_once_and_last_stream_ownership() {
    let (join, pool, calls, polls) = fixture_with_overlap(false);
    let prepared = join
        .prepare_queue_input()
        .await
        .unwrap()
        .expect("validated fixed multibatch row store must prepare");
    assert!(prepared.output.max_bytes().is_some());
    assert_eq!(prepared.streams.len(), 3);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(
        polls.load(Ordering::SeqCst),
        0,
        "preparation must never pull probe outputs"
    );
    let held = pool.used();
    assert!(held >= 1_600_000, "retained row payload must be admitted");
    drop(join);
    assert_eq!(
        pool.used(),
        held,
        "prepared streams retain packed build and indexes"
    );
    assert_eq!(collect(prepared.streams).await, expected());
    assert_eq!(polls.load(Ordering::SeqCst), 3);
    assert_eq!(pool.used(), 0);
    let (ordinary, pool, _, polls) = fixture_with_overlap(false);
    let mut streams = vec![];
    for p in 0..ordinary.output_partitions() {
        streams.push(ordinary.execute(p).await.unwrap());
    }
    assert_eq!(collect(streams).await, expected());
    assert_eq!(polls.load(Ordering::SeqCst), 3);
    drop(ordinary);
    assert_eq!(pool.used(), 0);
}

#[test]
fn aggregate_over_multibatch_row_store_overlaps_probe_and_releases_owners() {
    let rayon = rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    rayon.install(|| {
        runtime.block_on(async {
            use arrow::datatypes::{DataType, Field, Schema};
            use query_engine::{
                physical::operators::{spillable::AggregateExpr, SpillableHashAggregateExec},
                planner::AggregateFunction,
                ExecutionConfig,
            };
            let (join, pool, calls, polls) = fixture_with_overlap(true);
            let aggregate = SpillableHashAggregateExec::new(
                join,
                vec![],
                vec![AggregateExpr {
                    func: AggregateFunction::Count,
                    input: Expr::column("pk"),
                    distinct: false,
                    second_arg: None,
                }],
                Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, true)])),
                pool.clone(),
                ExecutionConfig::new().with_memory_limit(pool.max()),
            );
            let result = tokio::time::timeout(std::time::Duration::from_secs(10), async {
                let mut values = vec![];
                for p in 0..aggregate.output_partitions() {
                    let mut output = aggregate.execute(p).await.unwrap();
                    while let Some(batch) = output.try_next().await.unwrap() {
                        let a = batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap();
                        values.extend(a.values().iter().copied());
                    }
                }
                values
            })
            .await
            .expect("two upstream probes must overlap under the prepared envelope");
            assert_eq!(result, vec![9]);
            assert_eq!(calls.load(Ordering::SeqCst), 3);
            assert_eq!(polls.load(Ordering::SeqCst), 3);
            drop(aggregate);
            assert_eq!(pool.used(), 0);
        })
    });
}

#[derive(Debug)]
struct UnknownProbe {
    batch: RecordBatch,
    prepared: Arc<AtomicUsize>,
    pulled: Arc<AtomicUsize>,
    released: Arc<AtomicUsize>,
}
struct ReleaseAck(Arc<AtomicUsize>);
impl Drop for ReleaseAck {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl PhysicalOperator for UnknownProbe {
    fn name(&self) -> &str {
        "UnknownRowStoreProbe"
    }
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn output_partitions(&self) -> usize {
        3
    }
    async fn execute(&self, _: usize) -> query_engine::Result<RecordBatchStream> {
        panic!("Some(Unknown) must not reexecute its child")
    }
    async fn prepare_queue_input(
        &self,
    ) -> query_engine::Result<Option<query_engine::physical::PreparedQueueInput>> {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        let mut streams = Vec::new();
        for _ in 0..3 {
            let batch = self.batch.clone();
            let pulled = self.pulled.clone();
            let ack = ReleaseAck(self.released.clone());
            streams.push(Box::pin(stream::once(async move {
                let _ack = ack;
                pulled.fetch_add(1, Ordering::SeqCst);
                Ok(batch)
            })) as RecordBatchStream);
        }
        Ok(Some(query_engine::physical::PreparedQueueInput {
            streams,
            output: query_engine::physical::PreparedOutputBound::Unknown,
        }))
    }
}
#[tokio::test]
async fn multibatch_row_store_unknown_child_is_reused_and_owned_until_drain_or_drop() {
    for drain in [false, true] {
        let (old, pool, _, _) = fixture_with_overlap(false);
        let build = old.children()[0].clone();
        drop(old);
        let prepared_count = Arc::new(AtomicUsize::new(0));
        let pulled = Arc::new(AtomicUsize::new(0));
        let released = Arc::new(AtomicUsize::new(0));
        let probe = Arc::new(UnknownProbe {
            batch: RecordBatch::try_from_iter([(
                "pk",
                Arc::new(Int64Array::from(vec![Some(1), Some(50001), Some(1), None])) as ArrayRef,
            )])
            .unwrap(),
            prepared: prepared_count.clone(),
            pulled: pulled.clone(),
            released: released.clone(),
        });
        let join = HashJoinExec::new(
            build,
            probe,
            vec![(Expr::column("bk"), Expr::column("pk"))],
            JoinType::Inner,
        )
        .with_memory_pool(pool.clone());
        let output = join
            .prepare_queue_input()
            .await
            .unwrap()
            .expect("Unknown child lifecycle must be retained");
        assert!(matches!(
            output.output,
            query_engine::physical::PreparedOutputBound::Unknown
        ));
        assert_eq!(prepared_count.load(Ordering::SeqCst), 1);
        assert_eq!(pulled.load(Ordering::SeqCst), 0);
        assert_eq!(released.load(Ordering::SeqCst), 0);
        assert_eq!(output.streams.len(), 3);
        let held = pool.used();
        assert!(held >= 1_600_000);
        drop(join);
        assert_eq!(pool.used(), held);
        if drain {
            assert_eq!(collect(output.streams).await, expected());
        } else {
            drop(output.streams);
        }
        assert_eq!(pulled.load(Ordering::SeqCst), if drain { 3 } else { 0 });
        assert_eq!(released.load(Ordering::SeqCst), 3);
        assert_eq!(prepared_count.load(Ordering::SeqCst), 1);
        assert_eq!(pool.used(), 0);
    }
}

#[tokio::test]
async fn vectorized_index_refusal_releases_previously_admitted_row_store() {
    let (join, pool, calls, polls) = fixture_with_overlap(false);
    let blocker = pool.allocate(pool.max() - 2 * 1024 * 1024).unwrap();
    let blocked = blocker.size();
    let error = match join.prepare_queue_input().await {
        Err(error) => error,
        Ok(_) => panic!("packed row store fits but subsequent VHT index must refuse"),
    };
    match error.root() {
        query_engine::QueryError::MemoryLimit {
            pool: name,
            requested,
            used,
            limit,
        } => {
            assert_eq!(name, "memory");
            assert_eq!(*limit, pool.max());
            assert!(*requested > limit - used);
        }
        other => panic!("expected named pool refusal, got {other}"),
    }
    // Two actual Int64 payload columns * 100000 rows, independent of the
    // implementation's descriptor sizes. This peak proves the packed payload
    // admission occurred before the later refusal, not merely a first-stage fail.
    assert!(pool.reserved_peak() > blocked + 1_600_000);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(polls.load(Ordering::SeqCst), 0);
    assert_eq!(
        pool.used(),
        blocked,
        "failed cache initialization must release RowStore"
    );
    drop(join);
    assert_eq!(pool.used(), blocked);
    drop(blocker);
    assert_eq!(pool.used(), 0);
}
