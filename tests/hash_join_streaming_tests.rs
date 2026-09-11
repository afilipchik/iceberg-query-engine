use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{stream, TryStreamExt};
use query_engine::{
    error::Result,
    physical::{HashJoinExec, PhysicalOperator, RecordBatchStream},
    planner::{BinaryOp, Expr, JoinType},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
struct Scan {
    schema: SchemaRef,
    parts: Vec<Vec<RecordBatch>>,
    polls: Arc<AtomicUsize>,
}
#[async_trait]
impl PhysicalOperator for Scan {
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        Vec::new()
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn output_partitions(&self) -> usize {
        self.parts.len()
    }
    async fn execute(&self, p: usize) -> Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, p)?;
        let polls = self.polls.clone();
        Ok(Box::pin(stream::iter(
            self.parts[p].clone().into_iter().map(move |batch| {
                polls.fetch_add(1, Ordering::SeqCst);
                Ok(batch)
            }),
        )))
    }
    fn name(&self) -> &str {
        "StreamingTestScan"
    }
}
fn batch(prefix: &str, keys: Vec<Option<i64>>, start: i64) -> RecordBatch {
    let rows = keys.len();
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(format!("{prefix}k"), DataType::Int64, true),
            Field::new(format!("{prefix}v"), DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from_iter_values(start..start + rows as i64)),
        ],
    )
    .unwrap()
}
fn scan(parts: Vec<Vec<RecordBatch>>) -> Arc<Scan> {
    Arc::new(Scan {
        schema: parts[0][0].schema(),
        parts,
        polls: Arc::new(AtomicUsize::new(0)),
    })
}
fn join(left: Arc<Scan>, right: Arc<Scan>, filter: Option<Expr>, swapped: bool) -> HashJoinExec {
    HashJoinExec::with_filter(
        left,
        right,
        vec![(Expr::column("lk"), Expr::column("rk"))],
        JoinType::Inner,
        filter,
    )
    .with_build_right(swapped)
}

#[tokio::test]
async fn hot_key_is_chunked_and_probe_is_lazy_on_drop() {
    let left = scan(vec![vec![batch("l", vec![Some(1); 20_037], 0)]]);
    let right = scan(vec![vec![
        batch("r", vec![Some(1); 100_000], 0),
        batch("r", vec![Some(1); 2], 100_000),
    ]]);
    let operator = join(left, right.clone(), None, false);
    let mut output = operator.execute(0).await.unwrap();
    assert_eq!(
        right.polls.load(Ordering::SeqCst),
        0,
        "execute must not collect probes"
    );
    for _ in 0..3 {
        let batch = output.try_next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 4096);
    }
    assert_eq!(right.polls.load(Ordering::SeqCst), 1);
    drop(output);
    tokio::task::yield_now().await;
    assert_eq!(
        right.polls.load(Ordering::SeqCst),
        1,
        "no background producer after drop"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn unmatched_probe_work_yields_and_can_be_cancelled_before_next_batch() {
    let left = scan(vec![vec![batch("l", vec![Some(1)], 0)]]);
    let right = scan(vec![vec![
        batch("r", vec![None; 100_000], 0),
        batch("r", vec![Some(1)], 100_000),
    ]]);
    let operator = join(left, right.clone(), None, false);
    let mut output = operator.execute(0).await.unwrap();
    let mut next = Box::pin(output.try_next());
    // No timing threshold: polling an all-ready source must cooperatively
    // suspend inside its first no-match batch, before producing the later hit.
    assert!(matches!(
        futures::poll!(next.as_mut()),
        std::task::Poll::Pending
    ));
    assert_eq!(right.polls.load(Ordering::SeqCst), 1);
    drop(next);
    drop(output);
    tokio::task::yield_now().await;
    assert_eq!(right.polls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn exact_multibatch_multipartition_null_filter_and_swapped_oracle() {
    for swapped in [false, true] {
        for filtered in [false, true] {
            let left_batches = vec![
                batch(
                    "l",
                    (0..150)
                        .map(|i| if i % 11 == 0 { None } else { Some(i % 2) })
                        .collect(),
                    0,
                ),
                batch("l", vec![Some(1); 90], 150),
            ];
            let right_batches = vec![
                batch(
                    "r",
                    (0..100)
                        .map(|i| if i % 13 == 0 { None } else { Some(i % 2) })
                        .collect(),
                    0,
                ),
                batch("r", vec![Some(1); 80], 100),
            ];
            let mut expected = Vec::new();
            for l in &left_batches {
                for r in &right_batches {
                    let lk = l.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                    let rk = r.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                    let lv = l.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                    let rv = r.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                    for i in 0..l.num_rows() {
                        for j in 0..r.num_rows() {
                            if !lk.is_null(i)
                                && !rk.is_null(j)
                                && lk.value(i) == rk.value(j)
                                && (!filtered || lv.value(i) < rv.value(j))
                            {
                                expected.push((lk.value(i), lv.value(i), rk.value(j), rv.value(j)));
                            }
                        }
                    }
                }
            }
            let filter = filtered.then(|| Expr::BinaryExpr {
                left: Box::new(Expr::column("lv")),
                op: BinaryOp::Lt,
                right: Box::new(Expr::column("rv")),
            });
            let operator = join(
                scan(left_batches.into_iter().map(|b| vec![b]).collect()),
                scan(right_batches.into_iter().map(|b| vec![b]).collect()),
                filter,
                swapped,
            );
            let mut actual = Vec::new();
            for partition in 0..operator.output_partitions() {
                let mut stream = operator.execute(partition).await.unwrap();
                while let Some(b) = stream.try_next().await.unwrap() {
                    assert!(b.num_rows() <= 4096);
                    let a: Vec<_> = b
                        .columns()
                        .iter()
                        .map(|a| a.as_any().downcast_ref::<Int64Array>().unwrap())
                        .collect();
                    for row in 0..b.num_rows() {
                        actual.push((
                            a[0].value(row),
                            a[1].value(row),
                            a[2].value(row),
                            a[3].value(row),
                        ));
                    }
                }
            }
            actual.sort_unstable();
            expected.sort_unstable();
            assert_eq!(actual, expected);
        }
    }
}

#[tokio::test]
async fn row_store_and_retained_columns_stream_all_duplicate_matches() {
    let left = scan(vec![vec![
        batch("l", vec![Some(7); 50_000], 0),
        batch("l", vec![Some(7); 50_000], 50_000),
    ]]);
    let right = scan(vec![vec![batch("r", vec![Some(7)], 123)]]);
    let mut operator = join(left, right, None, false);
    operator.set_retained(Some(vec![false, true, false, true]));
    let mut output = operator.execute(0).await.unwrap();
    let mut seen = vec![false; 100_000];
    let mut chunks = 0;
    while let Some(batch) = output.try_next().await.unwrap() {
        chunks += 1;
        assert_eq!(batch.num_columns(), 2);
        assert!(batch.num_rows() <= 4096);
        let build = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let probe = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let index = build.value(row) as usize;
            assert!(!seen[index]);
            seen[index] = true;
            assert_eq!(probe.value(row), 123);
        }
    }
    assert_eq!(chunks, 100_000usize.div_ceil(4096));
    assert!(seen.into_iter().all(|value| value));
}

#[tokio::test]
async fn composite_hash_keys_verify_every_component() {
    let left = scan(vec![vec![batch("l", vec![Some(1); 12_000], 0)]]);
    let right = scan(vec![vec![batch("r", vec![Some(1); 12_000], 0)]]);
    let operator = HashJoinExec::new(
        left,
        right,
        vec![
            (Expr::column("lk"), Expr::column("rk")),
            (Expr::column("lv"), Expr::column("rv")),
        ],
        JoinType::Inner,
    );
    let mut output = operator.execute(0).await.unwrap();
    let mut count = 0;
    while let Some(batch) = output.try_next().await.unwrap() {
        let a = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let b = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert_eq!(a.value(row), b.value(row));
        }
        count += batch.num_rows();
    }
    assert_eq!(count, 12_000);
}

#[tokio::test]
async fn legacy_computed_key_outer_high_fanout_refuses_before_match_materialization() {
    let left = scan(vec![vec![batch("l", vec![Some(1); 2000], 0)]]);
    let right = scan(vec![vec![batch("r", vec![Some(1); 2000], 0)]]);
    let operator = HashJoinExec::new(
        left,
        right,
        vec![(
            Expr::BinaryExpr {
                left: Box::new(Expr::column("lk")),
                op: BinaryOp::Subtract,
                right: Box::new(Expr::column("lk")),
            },
            Expr::BinaryExpr {
                left: Box::new(Expr::column("rk")),
                op: BinaryOp::Subtract,
                right: Box::new(Expr::column("rk")),
            },
        )],
        JoinType::Left,
    );
    let error = match operator.execute(0).await {
        Ok(_) => panic!("legacy expansion must refuse"),
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("requires bounded output support"),
        "{error}"
    );
}

#[tokio::test]
async fn legacy_candidate_bound_is_per_batch_not_total_partition() {
    let left = scan(vec![vec![batch("l", vec![Some(1); 1024], 0)]]);
    let right = scan(vec![vec![
        batch("r", vec![Some(1); 600], 0),
        batch("r", vec![Some(1); 600], 600),
    ]]);
    let operator = HashJoinExec::new(
        left,
        right,
        vec![(Expr::column("lk"), Expr::column("rk"))],
        JoinType::Left,
    );
    let mut output = operator.execute(0).await.unwrap();
    let mut rows = 0;
    while let Some(batch) = output.try_next().await.unwrap() {
        rows += batch.num_rows();
    }
    assert_eq!(rows, 1024 * 1200);
}
