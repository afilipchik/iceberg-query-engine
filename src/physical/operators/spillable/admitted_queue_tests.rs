use super::*;
use crate::execution::{create_memory_pool, reserved_vec::ReservedVec, ReservedBufferBuilder};
use crate::physical::{admit_stream, PreparedAdmittedInput};
use arrow::array::{ArrayRef, Int64Array};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::atomic::{AtomicUsize, Ordering};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn queue_error_waits_for_running_producer_cleanup() {
    let pool = create_memory_pool(1024 * 1024);
    let lease = pool.allocate(4096).unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(2));
    let worker_barrier = barrier.clone();
    let (started, ready) = tokio::sync::oneshot::channel();
    let mut producers = tokio::task::JoinSet::new();
    producers.spawn(async move {
        let _lease = lease;
        started.send(()).unwrap();
        // Model a synchronous decoder poll that cancellation cannot preempt.
        worker_barrier.wait();
        Ok(())
    });
    ready.await.unwrap();
    producers.spawn(async { Err(QueryError::Execution("producer failure".into())) });
    let (tx, receiver) = tokio::sync::mpsc::channel(1);
    drop(tx);
    let mut queue = InputQueueStream {
        receiver,
        producers,
        label: "cleanup",
        finished: false,
        pending_error: None,
        envelope: None,
        _queue_metadata: None,
    };
    let early = tokio::time::timeout(std::time::Duration::from_millis(30), queue.try_next()).await;
    // Always release the synchronous worker, including when testing the old bug.
    barrier.wait();
    assert!(
        early.is_err(),
        "error escaped while a producer still owned query memory"
    );
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), queue.try_next())
        .await
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("producer failure"));
    assert_eq!(pool.used(), 0);
}

#[derive(Debug)]
struct Source {
    prepared: Arc<AtomicUsize>,
    polled: Arc<AtomicUsize>,
    barrier: Arc<tokio::sync::Barrier>,
    wrong_pool: bool,
    tail: Option<Arc<tokio::sync::Notify>>,
}
impl Source {
    fn new(wrong_pool: bool) -> Self {
        Self {
            prepared: Arc::new(AtomicUsize::new(0)),
            polled: Arc::new(AtomicUsize::new(0)),
            barrier: Arc::new(tokio::sync::Barrier::new(4)),
            wrong_pool,
            tail: None,
        }
    }
}
#[async_trait::async_trait]
impl PhysicalOperator for Source {
    fn schema(&self) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn name(&self) -> &str {
        "audited test source"
    }
    fn output_partitions(&self) -> usize {
        4
    }
    async fn execute(&self, _: usize) -> Result<RecordBatchStream> {
        panic!("admitted descriptor must not replay ordinary execution")
    }
    async fn prepare_queue_input(&self) -> Result<Option<crate::physical::PreparedQueueInput>> {
        panic!("admitted descriptor must not prepare a second route")
    }
    async fn prepare_admitted_queue_input(
        &self,
        pool: SharedMemoryPool,
    ) -> Result<Option<PreparedAdmittedInput>> {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        let pool = if self.wrong_pool {
            create_memory_pool(1024 * 1024)
        } else {
            pool
        };
        let mut streams = ReservedVec::with_capacity(&pool, 4)?;
        for part in 0..4 {
            let barrier = self.barrier.clone();
            let polled = self.polled.clone();
            let output_pool = pool.clone();
            let schema = self.schema();
            let stream = stream::once(async move {
                polled.fetch_add(1, Ordering::SeqCst);
                barrier.wait().await;
                let mut values = ReservedBufferBuilder::<i64>::with_capacity(&output_pool, 1)?;
                values.extend_reserved(1, [part])?;
                let array: ArrayRef = Arc::new(Int64Array::new(
                    ScalarBuffer::new(values.finish(), 0, 1),
                    None,
                ));
                let mut arrays = ReservedVec::with_capacity(&output_pool, 1)?;
                arrays.extend_reserved(1, [array])?;
                crate::storage::admitted_batch::finish(schema, 1, arrays, &output_pool)
            });
            let tail = self.tail.clone();
            let count = usize::from(tail.is_some());
            let stream = stream.chain(
                stream::once(async move {
                    if let Some(gate) = tail {
                        gate.notified().await;
                    }
                    Err(QueryError::Execution("admitted late failure".into()))
                })
                .take(count),
            );
            streams.extend_reserved(1, [admit_stream(stream, &pool)?])?;
        }
        Ok(Some(PreparedAdmittedInput { pool, streams }))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_pulls_overlap_and_output_retains_its_lease() {
    assert!(
        rayon::current_num_threads() >= 4,
        "run with RAYON_NUM_THREADS=4"
    );
    let source = Arc::new(Source::new(false));
    let input: Arc<dyn PhysicalOperator> = source.clone();
    let pool = create_memory_pool(1024 * 1024);
    let queue = stream_merge_input_partitions(&input, &pool, "overlap")
        .await
        .unwrap();
    let batches = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        queue.try_collect::<Vec<_>>(),
    )
    .await
    .expect("four pulls must overlap")
    .unwrap();
    let mut actual: Vec<_> = batches
        .iter()
        .map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0)
        })
        .collect();
    actual.sort();
    assert_eq!(actual, [0, 1, 2, 3]);
    assert_eq!(source.prepared.load(Ordering::SeqCst), 1);
    assert_eq!(source.polled.load(Ordering::SeqCst), 4);
    assert!(pool.used() > 0, "consumer-held buffers remain charged");
    drop(batches);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn admitted_wrong_pool_is_rejected_before_polling() {
    let source = Arc::new(Source::new(true));
    let input: Arc<dyn PhysicalOperator> = source.clone();
    let pool = create_memory_pool(1024 * 1024);
    let error = stream_merge_input_partitions(&input, &pool, "wrong pool")
        .await
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("pool/partition contract"),
        "{error}"
    );
    assert_eq!(source.polled.load(Ordering::SeqCst), 0);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn admitted_queue_metadata_refusal_starts_no_producers() {
    let source = Arc::new(Source::new(false));
    let input: Arc<dyn PhysicalOperator> = source.clone();
    let pool = create_memory_pool(16 * 1024);
    let error = stream_merge_input_partitions(&input, &pool, "small queue")
        .await
        .err()
        .unwrap();
    assert!(error.is_memory_limit(), "{error}");
    assert_eq!(source.prepared.load(Ordering::SeqCst), 1);
    assert_eq!(source.polled.load(Ordering::SeqCst), 0);
    assert_eq!(pool.used(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_cancellation_releases_producers_but_preserves_held_output() {
    let mut source = Source::new(false);
    source.tail = Some(Arc::new(tokio::sync::Notify::new()));
    let input: Arc<dyn PhysicalOperator> = Arc::new(source);
    let pool = create_memory_pool(1024 * 1024);
    let mut queue = stream_merge_input_partitions(&input, &pool, "cancel admitted")
        .await
        .unwrap();
    let held = tokio::time::timeout(std::time::Duration::from_secs(5), queue.try_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    drop(queue);
    assert!(pool.used() > 0);
    assert_eq!(held.num_rows(), 1);
    drop(held);
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while pool.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("cancelled producer owners must be released");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_late_error_joins_other_producers_without_replay() {
    let mut source = Source::new(false);
    let gate = Arc::new(tokio::sync::Notify::new());
    source.tail = Some(gate.clone());
    let source = Arc::new(source);
    let input: Arc<dyn PhysicalOperator> = source.clone();
    let pool = create_memory_pool(1024 * 1024);
    let mut queue = stream_merge_input_partitions(&input, &pool, "late admitted")
        .await
        .unwrap();
    let held = tokio::time::timeout(std::time::Duration::from_secs(5), queue.try_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    gate.notify_one();
    let error = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match queue.try_next().await {
                Err(error) => break error,
                Ok(Some(_)) => (),
                Ok(None) => panic!("late error was lost"),
            }
        }
    })
    .await
    .unwrap();
    assert!(
        error.to_string().contains("admitted late failure"),
        "{error}"
    );
    assert_eq!(source.prepared.load(Ordering::SeqCst), 1);
    drop(queue);
    assert!(pool.used() > 0);
    drop(held);
    assert_eq!(pool.used(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_raw_scan_project_preserves_nulls_duplicates_and_partitions() {
    use crate::physical::operators::{ProjectExec, StreamingParquetScanExec};
    use arrow::array::{Array, StringArray};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("queue.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("text", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from_iter_values(0..24)),
            Arc::new(StringArray::from(
                (0..24)
                    .map(|i| (i % 4 != 0).then(|| format!("duplicate-{}", i % 3)))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_compression(parquet::basic::Compression::ZSTD(Default::default()))
        .set_max_row_group_size(2)
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        schema.clone(),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let pool = create_memory_pool(16 * 1024 * 1024);
    let scan = StreamingParquetScanExec::try_new_with_batch_size(
        "queue",
        &[path],
        schema.clone(),
        None,
        None,
        &schema,
        8192,
        true,
    )
    .unwrap()
    .with_memory_pool(pool.clone());
    assert!(scan.output_partitions() > 1);
    let projected = Arc::new(Schema::new(vec![
        Field::new("text", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
        Field::new("again", DataType::Utf8, true),
    ]));
    let input: Arc<dyn PhysicalOperator> = Arc::new(
        ProjectExec::new(
            Arc::new(scan),
            vec![
                Expr::column("text"),
                Expr::column("id"),
                Expr::column("text"),
            ],
            projected,
        )
        .with_memory_pool(pool.clone()),
    );
    // Preparation owns streams but must not decode/output; dropping it releases
    // all construction charges and leaves the immutable scan reusable.
    let prepared = input
        .prepare_admitted_queue_input(pool.clone())
        .await
        .unwrap()
        .expect("raw scan through column Project must be admitted");
    assert_eq!(prepared.streams.as_slice().len(), input.output_partitions());
    drop(prepared);
    assert_eq!(pool.used(), 0);
    let queue = stream_merge_input_partitions(&input, &pool, "real scan")
        .await
        .unwrap();
    let batches = queue.try_collect::<Vec<_>>().await.unwrap();
    let mut rows = vec![];
    for batch in &batches {
        let text = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ids = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let again = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            let value = (!text.is_null(i)).then(|| text.value(i).to_owned());
            assert_eq!(
                value,
                (!again.is_null(i)).then(|| again.value(i).to_owned())
            );
            rows.push((ids.value(i), value));
        }
    }
    rows.sort();
    assert_eq!(
        rows,
        (0..24)
            .map(|i| (i, (i % 4 != 0).then(|| format!("duplicate-{}", i % 3))))
            .collect::<Vec<_>>()
    );
    assert!(pool.used() > 0);
    drop(batches);
    assert_eq!(pool.used(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spillable_inner_wrapper_preserves_admitted_partition_contract() {
    use crate::physical::MemoryTableExec;
    let pool = create_memory_pool(16 * 1024 * 1024);
    let source = Arc::new(Source::new(false));
    let batch = RecordBatch::try_from_iter(vec![(
        "bk",
        Arc::new(Int64Array::from(vec![Some(0), Some(0), Some(2), None])) as ArrayRef,
    )])
    .unwrap();
    let build = Arc::new(MemoryTableExec::new(
        "build",
        batch.schema(),
        vec![batch],
        None,
    ));
    let dir = tempfile::tempdir().unwrap();
    let join = SpillableHashJoinExec::new(
        build,
        source.clone(),
        vec![(Expr::column("bk"), Expr::column("id"))],
        JoinType::Inner,
        pool.clone(),
        ExecutionConfig::new()
            .with_memory_limit(pool.max())
            .with_spill_path(dir.path().join("spill")),
    );
    let prepared = join
        .prepare_admitted_queue_input(pool.clone())
        .await
        .unwrap()
        .expect("planner's spillable inner wrapper must delegate its admitted in-memory decision");
    assert_eq!(prepared.streams.as_slice().len(), 4);
    assert_eq!(source.prepared.load(Ordering::SeqCst), 1);
    assert_eq!(source.polled.load(Ordering::SeqCst), 0);
    let output = futures::stream::select_all(prepared.streams.into_owned_iter());
    let batches = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        output.try_collect::<Vec<_>>(),
    )
    .await
    .unwrap()
    .unwrap();
    let mut actual = vec![];
    for batch in &batches {
        let l = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let r = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            actual.push((l.value(row), r.value(row)));
        }
    }
    actual.sort();
    assert_eq!(actual, vec![(0, 0), (0, 0), (2, 2)]);
    assert_eq!(source.polled.load(Ordering::SeqCst), 4);
    drop(prepared.pool);
    drop(join);
    assert!(pool.used() > 0);
    drop(batches);
    assert_eq!(pool.used(), 0);
}

#[derive(Debug)]
struct EncodedBuild {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}
#[async_trait::async_trait]
impl PhysicalOperator for EncodedBuild {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn name(&self) -> &str {
        "logical UTF8 with encoded physical payload"
    }
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        crate::physical::check_partition(self, partition)?;
        Ok(Box::pin(stream::iter(
            self.batches.clone().into_iter().map(Ok),
        )))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_build_encoding_does_not_replace_declared_join_schema() {
    use arrow::array::{Array, DictionaryArray, Int8Array, StringArray};
    use arrow::datatypes::Int8Type;
    for swapped in [false, true] {
        let pool = create_memory_pool(16 * 1024 * 1024);
        let schema = Arc::new(Schema::new(vec![
            Field::new("bk", DataType::Int64, false),
            Field::new("label", DataType::Utf8, true).with_metadata(
                std::collections::HashMap::from([(
                    "producer_contract".into(),
                    "logical_utf8".into(),
                )]),
            ),
        ]));
        let encode = |keys: Vec<i64>, codes: Vec<Option<i8>>, words: Vec<Option<&str>>| {
            let encoded = DictionaryArray::<Int8Type>::try_new(
                Int8Array::from(codes),
                Arc::new(StringArray::from(words)),
            )
            .unwrap();
            RecordBatch::try_from_iter(vec![
                ("bk", Arc::new(Int64Array::from(keys)) as ArrayRef),
                ("label", Arc::new(encoded) as ArrayRef),
            ])
            .unwrap()
        };
        let batches = vec![
            encode(
                vec![0, 0, 2, 3],
                vec![Some(0), None, Some(1), Some(2)],
                vec![Some("zero"), None, Some("three")],
            ),
            encode(vec![], vec![], vec![Some("different empty codebook")]),
            encode(
                vec![0, 3],
                vec![Some(1), Some(0)],
                vec![Some("zero"), Some("again")],
            ),
        ];
        let source = Arc::new(Source::new(false));
        let build: Arc<dyn PhysicalOperator> = Arc::new(EncodedBuild { schema, batches });
        let (left, right, on): (Arc<dyn PhysicalOperator>, Arc<dyn PhysicalOperator>, _) =
            if swapped {
                (
                    source.clone(),
                    build,
                    vec![(Expr::column("id"), Expr::column("bk"))],
                )
            } else {
                (
                    build,
                    source.clone(),
                    vec![(Expr::column("bk"), Expr::column("id"))],
                )
            };
        let dir = tempfile::tempdir().unwrap();
        let join = SpillableHashJoinExec::new(
            left,
            right,
            on,
            JoinType::Inner,
            pool.clone(),
            ExecutionConfig::new()
                .with_memory_limit(pool.max())
                .with_spill_path(dir.path().join("spill")),
        )
        .with_build_right(swapped);
        let prepared = join
            .prepare_admitted_queue_input(pool.clone())
            .await
            .unwrap()
            .expect("physical dictionary encoding must not replace the declared UTF8 domain");
        assert_eq!(source.prepared.load(Ordering::SeqCst), 1);
        assert_eq!(source.polled.load(Ordering::SeqCst), 0);
        let output = futures::stream::select_all(prepared.streams.into_owned_iter());
        let batches = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            output.try_collect::<Vec<_>>(),
        )
        .await
        .unwrap()
        .unwrap();
        let mut actual = vec![];
        for batch in &batches {
            let schema = batch.schema();
            let index = schema.index_of("label").unwrap();
            assert_eq!(schema.field(index).data_type(), &DataType::Utf8);
            assert_eq!(
                schema.field(index).metadata()["producer_contract"],
                "logical_utf8"
            );
            let key = batch
                .column(schema.index_of("bk").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let label = batch
                .column(index)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let id = batch
                .column(schema.index_of("id").unwrap())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for r in 0..batch.num_rows() {
                actual.push((
                    key.value(r),
                    (!label.is_null(r)).then(|| label.value(r).to_owned()),
                    id.value(r),
                ));
            }
        }
        actual.sort();
        assert_eq!(
            actual,
            vec![
                (0, None, 0),
                (0, Some("again".into()), 0),
                (0, Some("zero".into()), 0),
                (2, None, 2),
                (3, Some("three".into()), 3),
                (3, Some("zero".into()), 3)
            ]
        );
        assert_eq!(source.polled.load(Ordering::SeqCst), 4);
        drop(prepared.pool);
        drop(join);
        assert!(pool.used() > 0);
        drop(batches);
        assert_eq!(pool.used(), 0);
    }
}
