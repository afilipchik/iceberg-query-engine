//! Prepared join exact-result, initialization and queue ownership contracts.
use arrow::{
    array::{ArrayRef, DictionaryArray, Int64Array, StringArray},
    datatypes::{DataType, Field, Int32Type, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{stream, TryStreamExt};
use query_engine::{
    execution::{create_memory_pool, SharedMemoryPool},
    physical::{
        queue_layout::{GatherCopyBound, QueueCopyBound},
        HashJoinExec, MemoryTableExec, PhysicalOperator, RecordBatchStream,
    },
    planner::{Expr, JoinType},
    ExecutionConfig, QueryError,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::Semaphore;
type Result<T> = query_engine::Result<T>;
const GUARD: Duration = Duration::from_secs(10);
#[derive(Debug)]
struct Signals {
    calls: AtomicUsize,
    polls: AtomicUsize,
    released: Semaphore,
    parked: Semaphore,
    proceed: Semaphore,
}
impl Signals {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            calls: AtomicUsize::new(0),
            polls: AtomicUsize::new(0),
            released: Semaphore::new(0),
            parked: Semaphore::new(0),
            proceed: Semaphore::new(0),
        })
    }
}
struct DropAck(Arc<Signals>);
impl Drop for DropAck {
    fn drop(&mut self) {
        self.0.released.add_permits(1);
    }
}
async fn wait(sem: &Semaphore, n: u32) {
    tokio::time::timeout(GUARD, sem.acquire_many(n))
        .await
        .expect("ownership deadlock")
        .unwrap()
        .forget();
}
#[derive(Debug)]
struct Probe {
    batch: RecordBatch,
    partitions: usize,
    certified: bool,
    fail_second: bool,
    park_second: bool,
    pool: SharedMemoryPool,
    signals: Arc<Signals>,
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
        "PreparedProbeFixture"
    }
    fn output_partitions(&self) -> usize {
        self.partitions
    }
    fn resident_gather_copy_bound(&self) -> Option<GatherCopyBound> {
        self.certified
            .then(|| GatherCopyBound::from_batches(&self.batch.schema(), &[self.batch.clone()]))
            .flatten()
    }
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, partition)?;
        self.signals.calls.fetch_add(1, Ordering::SeqCst);
        let ack = DropAck(self.signals.clone());
        // A tiny retained join index may already exist. Require all remaining
        // capacity, with at most 1KiB of persistent build state. Prepared output
        // envelopes are larger than this allowance and must not exist yet.
        if self.pool.used() > 1024 {
            return Err(QueryError::Execution(
                "probe initialization observed a premature output envelope".into(),
            ));
        }
        let initialization = self.pool.allocate(self.pool.available())?;
        if partition == 1 && (self.fail_second || self.park_second) {
            self.signals.parked.add_permits(1);
            self.signals.proceed.acquire().await.unwrap().forget();
            if self.fail_second {
                return Err(QueryError::Execution(
                    "injected prepared probe initialization error".into(),
                ));
            }
        }
        drop(initialization);
        let batch = self.batch.clone();
        let signals = self.signals.clone();
        Ok(Box::pin(stream::once(async move {
            let _ack = ack;
            signals.polls.fetch_add(1, Ordering::SeqCst);
            Ok(batch)
        })))
    }
}
#[tokio::test]
async fn probe_fixture_rejects_premature_output_reservation() {
    let probe = probe(false);
    let envelope = probe.pool.allocate(2048).unwrap();
    let err = match probe.execute(0).await {
        Err(e) => e,
        Ok(_) => panic!("premature envelope must be detected"),
    };
    assert!(err.to_string().contains("premature output envelope"));
    drop(envelope);
    assert_eq!(probe.pool.used(), 0);
}

fn values(name: &str, keys: Vec<i64>, values: Vec<Option<&str>>, dictionary: bool) -> RecordBatch {
    let array: ArrayRef = if dictionary {
        Arc::new(
            values
                .iter()
                .copied()
                .collect::<DictionaryArray<Int32Type>>(),
        )
    } else {
        Arc::new(StringArray::from(values))
    };
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(format!("{name}k"), DataType::Int64, false),
            Field::new(format!("{name}v"), array.data_type().clone(), true),
        ])),
        vec![Arc::new(Int64Array::from(keys)), array],
    )
    .unwrap()
}
fn probe(dictionary: bool) -> Arc<Probe> {
    Arc::new(Probe {
        batch: values(
            "p",
            vec![1, 2],
            vec![Some("probe-long-string"), None],
            dictionary,
        ),
        partitions: 3,
        certified: true,
        fail_second: false,
        park_second: false,
        pool: create_memory_pool(8 * 1024 * 1024),
        signals: Signals::new(),
    })
}
fn build(dictionary: bool) -> Arc<dyn PhysicalOperator> {
    let batch = values(
        "b",
        vec![1, 1, 2],
        vec![Some("a"), None, Some("b")],
        dictionary,
    );
    Arc::new(MemoryTableExec::new(
        "build",
        batch.schema(),
        vec![batch],
        None,
    ))
}
fn make_join(
    probe: Arc<Probe>,
    dictionary: bool,
    swapped: bool,
    masked: bool,
    kind: JoinType,
) -> HashJoinExec {
    let (left, right, on): (Arc<dyn PhysicalOperator>, Arc<dyn PhysicalOperator>, _) = if swapped {
        (
            probe,
            build(dictionary),
            vec![(Expr::column("pk"), Expr::column("bk"))],
        )
    } else {
        (
            build(dictionary),
            probe,
            vec![(Expr::column("bk"), Expr::column("pk"))],
        )
    };
    let mut join = HashJoinExec::new(left, right, on, kind).with_build_right(swapped);
    if masked {
        join.set_retained(Some(vec![false, true, false, true]));
    }
    join
}
fn string_values(batch: &RecordBatch, index: usize) -> Vec<Option<String>> {
    let array = arrow::compute::cast(batch.column(index), &DataType::Utf8).unwrap();
    array
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .iter()
        .map(|s| s.map(str::to_owned))
        .collect()
}
#[tokio::test]
async fn prepared_inner_exact_duplicates_encodings_masks_and_unpulled_streams() {
    for dictionary in [false, true] {
        for swapped in [false, true] {
            for masked in [false, true] {
                let probe = probe(dictionary);
                let join = make_join(probe.clone(), dictionary, swapped, masked, JoinType::Inner);
                let prepared = join
                    .prepare_queue_input()
                    .await
                    .unwrap()
                    .expect("eligible string Inner must prepare");
                assert_eq!(prepared.streams.len(), 3);
                assert_eq!(probe.signals.calls.load(Ordering::SeqCst), 3);
                assert_eq!(probe.signals.polls.load(Ordering::SeqCst), 0);
                assert_eq!(probe.pool.used(), 0);
                let mut actual = Vec::new();
                for mut stream in prepared.streams {
                    while let Some(batch) = stream.try_next().await.unwrap() {
                        assert!(
                            QueueCopyBound::from_batches(&batch.schema(), &[batch.clone()])
                                .unwrap()
                                .max_bytes()
                                .unwrap()
                                <= prepared.output.max_bytes().unwrap()
                        );
                        let (left_index, right_index) = if masked { (0, 1) } else { (1, 3) };
                        let left = string_values(&batch, left_index);
                        let right = string_values(&batch, right_index);
                        for pair in left.into_iter().zip(right) {
                            actual.push(if swapped { (pair.1, pair.0) } else { pair });
                        }
                    }
                }
                let one_partition = vec![
                    (Some("a".into()), Some("probe-long-string".into())),
                    (None, Some("probe-long-string".into())),
                    (Some("b".into()), None),
                ];
                let mut expected = (0..3)
                    .flat_map(|_| one_partition.iter().cloned())
                    .collect::<Vec<_>>();
                actual.sort();
                expected.sort();
                assert_eq!(actual, expected);
                assert_eq!(probe.signals.calls.load(Ordering::SeqCst), 3);
                assert_eq!(probe.signals.polls.load(Ordering::SeqCst), 3);
                wait(&probe.signals.released, 3).await;
            }
        }
    }
}
async fn initialization_failure(cancel: bool) {
    let mut probe = probe(false);
    {
        let p = Arc::get_mut(&mut probe).unwrap();
        p.fail_second = !cancel;
        p.park_second = cancel;
    }
    let join = Arc::new(make_join(
        probe.clone(),
        false,
        false,
        false,
        JoinType::Inner,
    ));
    let task = tokio::spawn(async move { join.prepare_queue_input().await });
    wait(&probe.signals.parked, 1).await;
    assert_eq!(probe.signals.calls.load(Ordering::SeqCst), 2);
    assert_eq!(probe.signals.polls.load(Ordering::SeqCst), 0);
    if cancel {
        task.abort();
        match task.await {
            Err(error) => assert!(error.is_cancelled()),
            Ok(_) => panic!("cancelled preparation completed"),
        }
    } else {
        probe.signals.proceed.add_permits(1);
        match tokio::time::timeout(GUARD, task).await.unwrap().unwrap() {
            Err(QueryError::Execution(message)) => {
                assert_eq!(message, "injected prepared probe initialization error")
            }
            _ => panic!("expected original probe error"),
        }
    }
    wait(&probe.signals.released, 2).await;
    assert_eq!(probe.pool.used(), 0);
    assert_eq!(probe.signals.polls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn probe_initialization_error_drops_prior_unpulled_stream() {
    initialization_failure(false).await;
}
#[tokio::test]
async fn probe_initialization_cancellation_drops_prior_stream_and_current_future() {
    initialization_failure(true).await;
}
#[tokio::test]
async fn unsupported_probe_or_non_inner_declines_without_probe_execution() {
    for (certified, kind) in [
        (false, JoinType::Inner),
        (true, JoinType::Semi),
        (true, JoinType::Left),
    ] {
        let mut probe = probe(false);
        Arc::get_mut(&mut probe).unwrap().certified = certified;
        let join = make_join(probe.clone(), false, false, false, kind);
        assert!(join.prepare_queue_input().await.unwrap().is_none());
        assert_eq!(probe.signals.calls.load(Ordering::SeqCst), 0);
        assert_eq!(probe.signals.polls.load(Ordering::SeqCst), 0);
    }
}
#[tokio::test]
async fn spill_decision_declines_preparation_without_starting_probe_outputs() {
    let probe = probe(false);
    let directory = tempfile::tempdir().unwrap();
    let batch = values(
        "b",
        vec![1; 4096],
        vec![Some("force-real-build-over-budget"); 4096],
        false,
    );
    let input = Arc::new(MemoryTableExec::new(
        "build",
        batch.schema(),
        vec![batch],
        None,
    ));
    let join = query_engine::physical::operators::SpillableHashJoinExec::new(
        input,
        probe.clone(),
        vec![(Expr::column("bk"), Expr::column("pk"))],
        JoinType::Inner,
        create_memory_pool(1024),
        ExecutionConfig::new()
            .with_memory_limit(1024)
            .with_spill_path(directory.path().to_path_buf()),
    );
    assert!(join.prepare_queue_input().await.unwrap().is_none());
    assert_eq!(probe.signals.calls.load(Ordering::SeqCst), 0);
    assert_eq!(probe.signals.polls.load(Ordering::SeqCst), 0);
    let spill_dirs = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    assert_eq!(spill_dirs.len(), 1);
    assert!(
        std::fs::read_dir(&spill_dirs[0]).unwrap().next().is_some(),
        "declined build decision must actually spill"
    );
    drop(join);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn scalar_aggregate_initializes_join_before_reserving_shared_output_envelope() {
    use query_engine::physical::operators::spillable::AggregateExpr;
    use query_engine::physical::operators::{SpillableHashAggregateExec, SpillableHashJoinExec};
    use query_engine::planner::AggregateFunction;
    let mut probe = probe(false);
    let pool = create_memory_pool(1024 * 1024);
    Arc::get_mut(&mut probe).unwrap().pool = pool.clone();
    let directory = tempfile::tempdir().unwrap();
    let config = ExecutionConfig::new()
        .with_memory_limit(pool.max())
        .with_spill_path(directory.path().to_path_buf());
    let join = Arc::new(SpillableHashJoinExec::new(
        build(false),
        probe.clone(),
        vec![(Expr::column("bk"), Expr::column("pk"))],
        JoinType::Inner,
        pool.clone(),
        config.clone(),
    ));
    assert_eq!(join.output_partitions(), 3);
    // Empty grouping makes this scalar Count fused-streaming-ineligible, so the
    // actual aggregate input queue must prepare the join and reserve its envelope.
    let aggregate = SpillableHashAggregateExec::new(
        join,
        vec![],
        vec![AggregateExpr {
            func: AggregateFunction::Count,
            input: Expr::column("pk"),
            distinct: false,
            second_arg: None,
        }],
        Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::Int64,
            true,
        )])),
        pool.clone(),
        config,
    );
    let mut results = Vec::new();
    for partition in 0..aggregate.output_partitions() {
        let mut output = tokio::time::timeout(GUARD, aggregate.execute(partition))
            .await
            .unwrap()
            .unwrap();
        while let Some(batch) = tokio::time::timeout(GUARD, output.try_next())
            .await
            .unwrap()
            .unwrap()
        {
            assert_eq!(batch.num_columns(), 1);
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(!arrow::array::Array::is_null(values, row));
                results.push(values.value(row));
            }
        }
        drop(output);
    }
    // Per probe partition: key1 matches two distinct build rows and key2 one.
    // Three partitions therefore produce exactly nine non-NULL counted keys.
    assert_eq!(results, vec![9]);
    assert_eq!(probe.signals.calls.load(Ordering::SeqCst), 3);
    assert_eq!(probe.signals.polls.load(Ordering::SeqCst), 3);
    wait(&probe.signals.released, 3).await;
    drop(aggregate);
    // Abort cleanup may be scheduled asynchronously; wait on the actual state,
    // not a guessed delay or an immediate Drop-is-synchronous assumption.
    tokio::time::timeout(GUARD, async {
        while pool.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queue reservations retained after all consumers dropped");
    assert_eq!(pool.used(), 0);
    assert_eq!(
        pool.reserved_peak(),
        pool.max(),
        "full-pool probe initialization must have been admitted"
    );
}

#[tokio::test]
async fn prepared_filter_project_preserves_real_inner_duplicates_and_physical_variants() {
    use query_engine::physical::{FilterExec, ProjectExec};
    use query_engine::planner::ScalarValue;
    for dictionary in [false, true] {
        for swapped in [false, true] {
            let probe = probe(dictionary);
            let join = Arc::new(make_join(
                probe.clone(),
                dictionary,
                swapped,
                false,
                JoinType::Inner,
            ));
            let filtered = Arc::new(FilterExec::new(
                join,
                Expr::column("pk").eq(Expr::literal(ScalarValue::Int64(1))),
            ));
            let project = ProjectExec::new(
                filtered,
                vec![
                    Expr::column("bv").alias("build"),
                    Expr::column("pv").alias("probe"),
                    Expr::column("bv").alias("duplicate"),
                ],
                Arc::new(Schema::new(vec![
                    Field::new("build", DataType::Utf8, true),
                    Field::new("probe", DataType::Utf8, true),
                    Field::new("duplicate", DataType::Utf8, true),
                ])),
            );
            let prepared = project
                .prepare_queue_input()
                .await
                .unwrap()
                .expect("wrappers must retain child preparation");
            assert_eq!(probe.signals.calls.load(Ordering::SeqCst), 3);
            assert_eq!(probe.signals.polls.load(Ordering::SeqCst), 0);
            assert_eq!(probe.pool.used(), 0);
            let bound = prepared
                .output
                .max_bytes()
                .expect("known Inner variants remain bounded through wrappers");
            let mut rows = Vec::new();
            for mut stream in prepared.streams {
                while let Some(batch) = stream.try_next().await.unwrap() {
                    assert!(
                        QueueCopyBound::from_batches(&batch.schema(), &[batch.clone()])
                            .unwrap()
                            .max_bytes()
                            .unwrap()
                            <= bound
                    );
                    let a = string_values(&batch, 0);
                    let b = string_values(&batch, 1);
                    let duplicate = string_values(&batch, 2);
                    assert_eq!(a, duplicate);
                    rows.extend(a.into_iter().zip(b));
                }
            }
            rows.sort();
            let mut expected = (0..3)
                .flat_map(|_| {
                    vec![
                        (Some("a".to_owned()), Some("probe-long-string".to_owned())),
                        (None, Some("probe-long-string".to_owned())),
                    ]
                })
                .collect::<Vec<_>>();
            expected.sort();
            assert_eq!(rows, expected);
            assert_eq!(probe.signals.calls.load(Ordering::SeqCst), 3);
            wait(&probe.signals.released, 3).await;
        }
    }
}

#[tokio::test]
async fn aggregate_residual_or_filter_prepares_real_inner_with_null_duplicates() {
    use query_engine::physical::operators::spillable::AggregateExpr;
    use query_engine::physical::operators::{SpillableHashAggregateExec, SpillableHashJoinExec};
    use query_engine::planner::AggregateFunction;
    let mut probe = probe(false);
    let pool = create_memory_pool(1024 * 1024);
    Arc::get_mut(&mut probe).unwrap().pool = pool.clone();
    let directory = tempfile::tempdir().unwrap();
    let config = ExecutionConfig::new()
        .with_memory_limit(pool.max())
        .with_spill_path(directory.path().to_path_buf());
    let join = Arc::new(SpillableHashJoinExec::new(
        build(false),
        probe.clone(),
        vec![(Expr::column("bk"), Expr::column("pk"))],
        JoinType::Inner,
        pool.clone(),
        config.clone(),
    ));
    assert_eq!(join.output_partitions(), 3);
    let predicate = Expr::column("bv")
        .eq(Expr::literal(query_engine::planner::ScalarValue::Utf8(
            "a".into(),
        )))
        .or(Expr::UnaryExpr {
            op: query_engine::planner::UnaryOp::IsNull,
            expr: Box::new(Expr::column("bv")),
        });
    let filtered = Arc::new(query_engine::physical::FilterExec::new(join, predicate));
    // Empty grouping makes this scalar Count fused-streaming-ineligible, so the
    // actual aggregate input queue must prepare the join and reserve its envelope.
    let aggregate = SpillableHashAggregateExec::new(
        filtered,
        vec![],
        vec![AggregateExpr {
            func: AggregateFunction::Count,
            input: Expr::column("pk"),
            distinct: false,
            second_arg: None,
        }],
        Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::Int64,
            true,
        )])),
        pool.clone(),
        config,
    );
    let mut results = Vec::new();
    for partition in 0..aggregate.output_partitions() {
        let mut output = tokio::time::timeout(GUARD, aggregate.execute(partition))
            .await
            .unwrap()
            .unwrap();
        while let Some(batch) = tokio::time::timeout(GUARD, output.try_next())
            .await
            .unwrap()
            .unwrap()
        {
            assert_eq!(batch.num_columns(), 1);
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(!arrow::array::Array::is_null(values, row));
                results.push(values.value(row));
            }
        }
        drop(output);
    }
    // Independent oracle: key1 matches build payload "a" and NULL, both
    // retained by (bv='a' OR bv IS NULL); key2 payload "b" is rejected.
    // The duplicate key1 matches survive, giving two rows per probe partition.
    assert_eq!(results, vec![6]);
    assert_eq!(probe.signals.calls.load(Ordering::SeqCst), 3);
    assert_eq!(probe.signals.polls.load(Ordering::SeqCst), 3);
    wait(&probe.signals.released, 3).await;
    drop(aggregate);
    // Abort cleanup may be scheduled asynchronously; wait on the actual state,
    // not a guessed delay or an immediate Drop-is-synchronous assumption.
    tokio::time::timeout(GUARD, async {
        while pool.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("queue reservations retained after all consumers dropped");
    assert_eq!(pool.used(), 0);
    assert_eq!(
        pool.reserved_peak(),
        pool.max(),
        "full-pool probe initialization must have been admitted"
    );
}

fn nested_join_fixture(
    p: Arc<Probe>,
    dictionary: bool,
    swapped: bool,
    masked: bool,
) -> HashJoinExec {
    use query_engine::physical::{FilterExec, ProjectExec};
    use query_engine::planner::{BinaryOp, ScalarValue};
    let inner: Arc<dyn PhysicalOperator> =
        Arc::new(make_join(p, dictionary, false, false, JoinType::Inner));
    let filtered: Arc<dyn PhysicalOperator> = Arc::new(FilterExec::new(
        inner,
        Expr::BinaryExpr {
            left: Box::new(Expr::column("pk")),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(1))),
        },
    ));
    let project: Arc<dyn PhysicalOperator> = Arc::new(ProjectExec::new(
        filtered,
        vec![
            Expr::Alias {
                expr: Box::new(Expr::column("pk")),
                name: "key".into(),
            },
            Expr::column("bv"),
            Expr::column("pv"),
            Expr::Alias {
                expr: Box::new(Expr::column("bv")),
                name: "bv2".into(),
            },
        ],
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("bv", DataType::Utf8, true),
            Field::new("pv", DataType::Utf8, true),
            Field::new("bv2", DataType::Utf8, true),
        ])),
    ));
    let b = values(
        "x",
        vec![1, 1, 2],
        vec![Some("x"), None, Some("y")],
        dictionary,
    );
    let outer_build: Arc<dyn PhysicalOperator> =
        Arc::new(MemoryTableExec::new("outer", b.schema(), vec![b], None));
    let mut join = if swapped {
        HashJoinExec::new(
            project,
            outer_build,
            vec![(Expr::column("key"), Expr::column("xk"))],
            JoinType::Inner,
        )
        .with_build_right(true)
    } else {
        HashJoinExec::new(
            outer_build,
            project,
            vec![(Expr::column("xk"), Expr::column("key"))],
            JoinType::Inner,
        )
    };
    if masked {
        join.set_retained(Some(if swapped {
            vec![false, true, true, true, false, true]
        } else {
            vec![false, true, false, true, true, true]
        }));
    }
    join
}
#[tokio::test]
async fn nested_prepared_inner_keeps_variants_and_exact_duplicate_null_rows() {
    for dictionary in [false, true] {
        for swapped in [false, true] {
            for masked in [false, true] {
                let p = probe(dictionary);
                let join = nested_join_fixture(p.clone(), dictionary, swapped, masked);
                let prepared = join
                    .prepare_queue_input()
                    .await
                    .unwrap()
                    .expect("bounded nested Inner must prepare");
                assert_eq!(prepared.streams.len(), 3);
                assert!(prepared.output.max_bytes().is_some());
                assert_eq!(p.signals.calls.load(Ordering::SeqCst), 3);
                assert_eq!(p.signals.polls.load(Ordering::SeqCst), 0);
                let mut rows = Vec::new();
                for mut stream in prepared.streams {
                    while let Some(b) = stream.try_next().await.unwrap() {
                        assert!(
                            QueueCopyBound::from_batches(&b.schema(), &[b.clone()])
                                .unwrap()
                                .max_bytes()
                                .unwrap()
                                <= prepared.output.max_bytes().unwrap()
                        );
                        let get = |name| string_values(&b, b.schema().index_of(name).unwrap());
                        let x = get("xv");
                        let v = get("bv");
                        let pv = get("pv");
                        let duplicate = get("bv2");
                        assert_eq!(v, duplicate);
                        rows.extend(x.into_iter().zip(v).zip(pv).map(|((x, v), p)| (x, v, p)));
                    }
                }
                let per = vec![
                    (
                        Some("x".to_owned()),
                        Some("a".to_owned()),
                        Some("probe-long-string".to_owned()),
                    ),
                    (
                        None,
                        Some("a".to_owned()),
                        Some("probe-long-string".to_owned()),
                    ),
                    (
                        Some("x".to_owned()),
                        None,
                        Some("probe-long-string".to_owned()),
                    ),
                    (None, None, Some("probe-long-string".to_owned())),
                    (Some("y".to_owned()), Some("b".to_owned()), None),
                ];
                let mut expected = (0..3).flat_map(|_| per.clone()).collect::<Vec<_>>();
                rows.sort();
                expected.sort();
                assert_eq!(rows, expected);
                assert_eq!(p.signals.polls.load(Ordering::SeqCst), 3);
                wait(&p.signals.released, 3).await;
            }
        }
    }
}
#[tokio::test]
async fn nested_prepared_initialization_error_and_cancel_drop_prior_streams() {
    for cancel in [false, true] {
        let mut p = probe(false);
        Arc::get_mut(&mut p).unwrap().fail_second = !cancel;
        Arc::get_mut(&mut p).unwrap().park_second = cancel;
        let join = Arc::new(nested_join_fixture(p.clone(), false, false, false));
        let task = tokio::spawn(async move { join.prepare_queue_input().await });
        wait(&p.signals.parked, 1).await;
        assert_eq!(p.signals.calls.load(Ordering::SeqCst), 2);
        assert_eq!(p.signals.polls.load(Ordering::SeqCst), 0);
        if cancel {
            task.abort();
            assert!(matches!(task.await, Err(e) if e.is_cancelled()));
        } else {
            p.signals.proceed.add_permits(1);
            assert!(task.await.unwrap().is_err());
        }
        wait(&p.signals.released, 2).await;
        assert_eq!(p.pool.used(), 0);
    }
}

#[tokio::test]
async fn nested_prepared_gather_bound_covers_repeated_actual_output_takes() {
    for dictionary in [false, true] {
        let p = probe(dictionary);
        let join = nested_join_fixture(p, false, true, true);
        let prepared = join.prepare_queue_input().await.unwrap().unwrap();
        let query_engine::physical::PreparedOutputBound::Layouts(layouts) = prepared.output else {
            panic!("expected physical variants")
        };
        let limit = layouts
            .gather_max_bytes(19)
            .expect("nested repeated-take metadata");
        for mut input in prepared.streams {
            while let Some(batch) = input.try_next().await.unwrap() {
                let indices = arrow::array::UInt32Array::from(vec![0; 19]);
                let taken = arrow::compute::take_record_batch(&batch, &indices).unwrap();
                assert!(
                    QueueCopyBound::from_batches(&taken.schema(), &[taken])
                        .unwrap()
                        .max_bytes()
                        .unwrap()
                        <= limit
                );
            }
        }
    }
}
#[derive(Debug)]
struct PreparedOnlyChild {
    input: Arc<dyn PhysicalOperator>,
    unknown: bool,
    prepares: Arc<AtomicUsize>,
    build_ready: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait]
impl PhysicalOperator for PreparedOnlyChild {
    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.input.clone()]
    }
    fn name(&self) -> &str {
        "PreparedOnlyChild"
    }
    fn output_partitions(&self) -> usize {
        self.input.output_partitions()
    }
    async fn execute(&self, _: usize) -> Result<RecordBatchStream> {
        panic!("prepared child must never be executed again")
    }
    async fn prepare_queue_input(
        &self,
    ) -> Result<Option<query_engine::physical::PreparedQueueInput>> {
        assert!(
            self.build_ready.load(Ordering::SeqCst),
            "outer build must finish before child preparation"
        );
        self.prepares.fetch_add(1, Ordering::SeqCst);
        let mut prepared = self
            .input
            .prepare_queue_input()
            .await?
            .expect("child must prepare");
        if self.unknown {
            prepared.output = query_engine::physical::PreparedOutputBound::Unknown;
        }
        Ok(Some(prepared))
    }
}
#[derive(Debug)]
struct OrderedBuild {
    batch: RecordBatch,
    ready: Arc<std::sync::atomic::AtomicBool>,
}
#[async_trait]
impl PhysicalOperator for OrderedBuild {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn name(&self) -> &str {
        "OrderedBuild"
    }
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, partition)?;
        let batch = self.batch.clone();
        let ready = self.ready.clone();
        Ok(Box::pin(stream::once(async move {
            ready.store(true, Ordering::SeqCst);
            Ok(batch)
        })))
    }
}
#[tokio::test]
async fn nested_unknown_reuses_original_streams_without_execute() {
    let p = probe(false);
    let prepares = Arc::new(AtomicUsize::new(0));
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let child: Arc<dyn PhysicalOperator> = Arc::new(PreparedOnlyChild {
        input: Arc::new(make_join(p.clone(), false, false, false, JoinType::Inner)),
        unknown: true,
        prepares: prepares.clone(),
        build_ready: ready.clone(),
    });
    let b = values("x", vec![1], vec![Some("x")], false);
    let b: Arc<dyn PhysicalOperator> = Arc::new(OrderedBuild { batch: b, ready });
    let join = HashJoinExec::new(
        b,
        child,
        vec![(Expr::column("xk"), Expr::column("pk"))],
        JoinType::Inner,
    );
    let prepared = join.prepare_queue_input().await.unwrap().unwrap();
    assert!(prepared.output.max_bytes().is_none());
    assert_eq!(prepares.load(Ordering::SeqCst), 1);
    assert_eq!(p.signals.polls.load(Ordering::SeqCst), 0);
    let mut actual = Vec::new();
    for mut s in prepared.streams {
        while let Some(b) = s.try_next().await.unwrap() {
            actual.extend(string_values(&b, b.schema().index_of("bv").unwrap()));
        }
    }
    actual.sort();
    assert_eq!(
        actual,
        vec![
            None,
            None,
            None,
            Some("a".into()),
            Some("a".into()),
            Some("a".into())
        ]
    );
    assert_eq!(p.signals.calls.load(Ordering::SeqCst), 3);
    wait(&p.signals.released, 3).await;
}
#[derive(Debug)]
struct ParkedPreparedLeaf {
    batch: RecordBatch,
    pool: SharedMemoryPool,
    signals: Arc<Signals>,
}
#[async_trait]
impl PhysicalOperator for ParkedPreparedLeaf {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn name(&self) -> &str {
        "ParkedPreparedLeaf"
    }
    fn output_partitions(&self) -> usize {
        3
    }
    async fn execute(&self, _: usize) -> Result<RecordBatchStream> {
        panic!("prepared leaf execute forbidden")
    }
    async fn prepare_queue_input(
        &self,
    ) -> Result<Option<query_engine::physical::PreparedQueueInput>> {
        // Requiring the whole shared pool detects any outer envelope admitted
        // before nested initialization finishes.
        let guard = self.pool.allocate(self.pool.max())?;
        let metadata =
            GatherCopyBound::from_batches(&self.batch.schema(), &[self.batch.clone()]).unwrap();
        let mut streams = Vec::new();
        for _ in 0..3 {
            self.signals.calls.fetch_add(1, Ordering::SeqCst);
            let signals = self.signals.clone();
            let batch = self.batch.clone();
            let ack = DropAck(signals.clone());
            streams.push(Box::pin(stream::once(async move {
                let _ack = ack;
                signals.polls.fetch_add(1, Ordering::SeqCst);
                signals.parked.add_permits(1);
                signals.proceed.acquire().await.unwrap().forget();
                Ok(batch)
            })) as RecordBatchStream);
        }
        drop(guard);
        Ok(Some(query_engine::physical::PreparedQueueInput {
            streams,
            output: query_engine::physical::PreparedOutputBound::Layouts(
                query_engine::physical::queue_layout::PreparedOutputLayouts::from_gather(metadata)
                    .unwrap(),
            ),
        }))
    }
}
#[test]
fn nested_prepared_aggregate_overlaps_two_pulls_and_cancels_parked_sources() {
    // A private Rayon context makes the expected two-slot envelope deterministic.
    rayon::ThreadPoolBuilder::new()
        .num_threads(2)
        .build()
        .unwrap()
        .install(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    for cancel in [false, true] {
                        use query_engine::physical::operators::spillable::{
                            AggregateExpr, SpillableHashAggregateExec,
                        };
                        use query_engine::planner::{AggregateFunction, ScalarValue};
                        let pool = create_memory_pool(16 * 1024 * 1024);
                        let signals = Signals::new();
                        let leaf: Arc<dyn PhysicalOperator> = Arc::new(ParkedPreparedLeaf {
                            batch: values("p", vec![1], vec![Some("probe")], false),
                            pool: pool.clone(),
                            signals: signals.clone(),
                        });
                        let inner: Arc<dyn PhysicalOperator> = Arc::new(HashJoinExec::new(
                            build(false),
                            leaf,
                            vec![(Expr::column("bk"), Expr::column("pk"))],
                            JoinType::Inner,
                        ));
                        let b = values("x", vec![1], vec![Some("outer")], false);
                        let b: Arc<dyn PhysicalOperator> =
                            Arc::new(MemoryTableExec::new("outer", b.schema(), vec![b], None));
                        let outer: Arc<dyn PhysicalOperator> = Arc::new(HashJoinExec::new(
                            b,
                            inner,
                            vec![(Expr::column("xk"), Expr::column("pk"))],
                            JoinType::Inner,
                        ));
                        let directory = tempfile::tempdir().unwrap();
                        let agg = SpillableHashAggregateExec::new(
                            outer,
                            vec![],
                            vec![AggregateExpr {
                                func: AggregateFunction::Count,
                                input: Expr::Literal(ScalarValue::Int64(1)),
                                distinct: false,
                                second_arg: None,
                            }],
                            Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)])),
                            pool.clone(),
                            ExecutionConfig::new()
                                .with_memory_limit(pool.max())
                                .with_spill_path(directory.path().to_owned()),
                        );
                        let task = tokio::spawn(async move {
                            agg.execute(0).await?.try_collect::<Vec<_>>().await
                        });
                        wait(&signals.parked, 2).await;
                        assert_eq!(signals.polls.load(Ordering::SeqCst), 2);
                        assert!(
                            pool.used() > 0,
                            "outer envelope must be admitted after leaf initialization"
                        );
                        if cancel {
                            task.abort();
                            assert!(matches!(task.await,Err(e) if e.is_cancelled()));
                        } else {
                            signals.proceed.add_permits(2);
                            wait(&signals.parked, 1).await;
                            signals.proceed.add_permits(1);
                            let out = task.await.unwrap().unwrap();
                            let count = out
                                .iter()
                                .map(|b| {
                                    b.column(0)
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap()
                                        .value(0)
                                })
                                .sum::<i64>();
                            assert_eq!(count, 6);
                        }
                        wait(&signals.released, 3).await;
                        assert_eq!(pool.used(), 0);
                    }
                });
        });
}
