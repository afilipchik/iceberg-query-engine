//! Enforced streaming output bounds and prepared join integration contracts.
use arrow::{
    array::{Array, ArrayRef, Date32Array, Decimal128Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use query_engine::{
    execution::{create_memory_pool, SharedMemoryPool},
    physical::{
        operators::spillable::AggregateExpr,
        operators::{
            FilterExec, ProjectExec, SpillableHashAggregateExec, SpillableHashJoinExec,
            StreamingParquetScanExec,
        },
        queue_layout::QueueCopyBound,
        MemoryTableExec, PhysicalOperator,
    },
    planner::{AggregateFunction, BinaryOp, Expr, JoinType, ScalarValue},
    ExecutionConfig,
};
use std::{sync::Arc, time::Duration};
const GUARD: Duration = Duration::from_secs(10);
type Row = (i64, Option<String>, i64, Option<i128>, i32);
struct Fixture {
    _dir: tempfile::TempDir,
    path: std::path::PathBuf,
    scan: Arc<StreamingParquetScanExec>,
    probe: Arc<dyn PhysicalOperator>,
    expected: Vec<Row>,
}
fn fixture() -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("probe.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("amount", DataType::Decimal128(20, 3), true),
        Field::new("day", DataType::Date32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                (0..93)
                    .map(|i| if i % 11 == 0 { None } else { Some(i % 4) })
                    .collect::<Vec<_>>(),
            )),
            Arc::new(
                Decimal128Array::from(
                    (0..93)
                        .map(|i| {
                            if i % 7 == 0 {
                                None
                            } else {
                                Some(i as i128 * 101 - 1000)
                            }
                        })
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(20, 3)
                .unwrap(),
            ),
            Arc::new(Date32Array::from_iter_values((0..93).map(|i| i - 50))),
        ],
    )
    .unwrap();
    let properties = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(23))
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        schema.clone(),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    // Fix the declared scan partition count independently of test runner CPUs.
    let workers = rayon::ThreadPoolBuilder::new()
        .num_threads(3)
        .build()
        .unwrap();
    let scan = Arc::new(workers.install(|| {
        StreamingParquetScanExec::try_new_with_batch_size(
            "probe",
            &[path.clone()],
            schema.clone(),
            None,
            None,
            &schema,
            17,
            true,
        )
        .unwrap()
    }));
    assert_eq!(scan.output_partitions(), 3);
    assert!(scan.resident_queue_copy_bound().is_none());
    assert!(scan.resident_gather_copy_bound().is_none());
    assert!(scan.pool_independent_queue_copy_bound().is_some());
    assert!(scan.pool_independent_gather_copy_bound().is_some());
    let filter: Arc<dyn PhysicalOperator> = Arc::new(FilterExec::new(
        scan.clone(),
        Expr::BinaryExpr {
            left: Box::new(Expr::column("key")),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(1))),
        },
    ));
    assert!(filter.pool_independent_queue_copy_bound().is_some());
    assert!(filter.pool_independent_gather_copy_bound().is_some());
    let probe: Arc<dyn PhysicalOperator> = Arc::new(ProjectExec::new(
        filter,
        vec![
            Expr::Alias {
                expr: Box::new(Expr::column("key")),
                name: "pk".into(),
            },
            Expr::column("amount"),
            Expr::column("day"),
        ],
        Arc::new(Schema::new(vec![
            Field::new("pk", DataType::Int64, true),
            Field::new("amount", DataType::Decimal128(20, 3), true),
            Field::new("day", DataType::Date32, false),
        ])),
    ));
    assert!(probe.resident_queue_copy_bound().is_none());
    assert!(probe.pool_independent_queue_copy_bound().is_some());
    assert!(probe.pool_independent_gather_copy_bound().is_some());
    let mut expected = Vec::new();
    for i in 0..93 {
        if i % 11 == 0 {
            continue;
        }
        let key = i % 4;
        for (build_key, tag) in [(1, Some("a")), (1, None), (2, Some("b"))] {
            if key >= 1 && key == build_key {
                expected.push((
                    build_key,
                    tag.map(str::to_owned),
                    key,
                    if i % 7 == 0 {
                        None
                    } else {
                        Some(i as i128 * 101 - 1000)
                    },
                    i as i32 - 50,
                ));
            }
        }
    }
    expected.sort();
    Fixture {
        _dir: dir,
        path,
        scan,
        probe,
        expected,
    }
}
fn make_join(fixture: &Fixture, pool: SharedMemoryPool) -> Arc<SpillableHashJoinExec> {
    let batch = RecordBatch::try_from_iter(vec![
        ("bk", Arc::new(Int64Array::from(vec![1, 1, 2])) as ArrayRef),
        (
            "tag",
            Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])) as ArrayRef,
        ),
    ])
    .unwrap();
    let build = Arc::new(MemoryTableExec::new(
        "build",
        batch.schema(),
        vec![batch],
        None,
    ));
    Arc::new(SpillableHashJoinExec::new(
        build,
        fixture.probe.clone(),
        vec![(Expr::column("bk"), Expr::column("pk"))],
        JoinType::Inner,
        pool.clone(),
        ExecutionConfig::new()
            .with_memory_limit(pool.max())
            .with_spill_path(fixture._dir.path().join("spill")),
    ))
}
async fn clean(pool: &SharedMemoryPool) {
    tokio::time::timeout(GUARD, async {
        while pool.used() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("reservations remained after consumers dropped");
}
#[tokio::test]
async fn streaming_prepared_join_and_scalar_aggregate_match_independent_oracle() {
    let fixture = fixture();
    // Check the direct raw-output row contract across every partition first.
    for p in 0..fixture.scan.output_partitions() {
        let mut stream = fixture.scan.execute(p).await.unwrap();
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert!(batch.num_rows() <= 17);
        }
    }
    let pool = create_memory_pool(4 * 1024 * 1024);
    let join = make_join(&fixture, pool.clone());
    let prepared = join
        .prepare_queue_input()
        .await
        .unwrap()
        .expect("fixed raw probe is eligible");
    assert_eq!(prepared.streams.len(), 3);
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
            let tag = arrow::compute::cast(batch.column(1), &DataType::Utf8).unwrap();
            let tag = tag.as_any().downcast_ref::<StringArray>().unwrap();
            let bk = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let pk = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let amount = batch
                .column(3)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            let day = batch
                .column(4)
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                actual.push((
                    bk.value(row),
                    (!tag.is_null(row)).then(|| tag.value(row).to_owned()),
                    pk.value(row),
                    (!amount.is_null(row)).then(|| amount.value(row)),
                    day.value(row),
                ));
            }
        }
    }
    actual.sort();
    assert_eq!(actual, fixture.expected);
    drop(join);
    clean(&pool).await;
    // Fresh join: the aggregate must reach the actual prepared queue route.
    let aggregate = SpillableHashAggregateExec::new(
        make_join(&fixture, pool.clone()),
        vec![],
        vec![AggregateExpr {
            func: AggregateFunction::Count,
            input: Expr::column("amount"),
            distinct: false,
            second_arg: None,
        }],
        Arc::new(Schema::new(vec![Field::new(
            "count",
            DataType::Int64,
            true,
        )])),
        pool.clone(),
        ExecutionConfig::new()
            .with_memory_limit(pool.max())
            .with_spill_path(fixture._dir.path().join("aggspill")),
    );
    let mut counts = Vec::new();
    for p in 0..aggregate.output_partitions() {
        let mut stream = aggregate.execute(p).await.unwrap();
        while let Some(batch) = stream.try_next().await.unwrap() {
            let count = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            counts.extend(count.iter().map(|v| v.expect("COUNT is non-NULL")));
        }
    }
    assert_eq!(
        counts,
        vec![fixture
            .expected
            .iter()
            .filter(|row| row.3.is_some())
            .count() as i64]
    );
    drop(aggregate);
    clean(&pool).await;
    assert_eq!(pool.used(), 0);
}
#[tokio::test]
async fn preparation_does_not_open_or_decode_probe_files() {
    let fixture = fixture();
    let pool = create_memory_pool(4 * 1024 * 1024);
    let join = make_join(&fixture, pool.clone());
    let prepared = join
        .prepare_queue_input()
        .await
        .unwrap()
        .expect("eligible raw probe");
    assert_eq!(prepared.streams.len(), 3);
    // Construction read footers earlier. Preparation must not open reader file
    // handles or pull batches: unlink now must be observed by every first pull.
    std::fs::remove_file(&fixture.path).unwrap();
    for mut stream in prepared.streams {
        assert!(tokio::time::timeout(GUARD, stream.try_next())
            .await
            .unwrap()
            .is_err());
    }
    drop(join);
    clean(&pool).await;
    assert_eq!(pool.used(), 0);
}
