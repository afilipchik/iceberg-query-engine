use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{stream, TryStreamExt};
use query_engine::{
    execution::MemoryPool,
    physical::{HashJoinExec, PhysicalOperator, RecordBatchStream},
    planner::{BinaryOp, Expr, JoinType},
    Result,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
struct Input {
    schema: SchemaRef,
    parts: Vec<Vec<RecordBatch>>,
    pulls: Arc<AtomicUsize>,
}
#[async_trait]
impl PhysicalOperator for Input {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn output_partitions(&self) -> usize {
        self.parts.len()
    }
    fn name(&self) -> &str {
        "OuterContractInput"
    }
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, partition)?;
        let pulls = self.pulls.clone();
        Ok(Box::pin(stream::iter(
            self.parts[partition].clone().into_iter().map(move |b| {
                pulls.fetch_add(1, Ordering::SeqCst);
                Ok(b)
            }),
        )))
    }
}
fn batch(side: &str, keys: Vec<Option<i64>>, values: Vec<i64>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new(format!("{side}k"), DataType::Int64, true),
            Field::new(format!("{side}v"), DataType::Int64, false),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}
fn input(parts: Vec<Vec<RecordBatch>>) -> Arc<Input> {
    Arc::new(Input {
        schema: parts[0][0].schema(),
        parts,
        pulls: Arc::new(AtomicUsize::new(0)),
    })
}
fn operator(
    left: Arc<Input>,
    right: Arc<Input>,
    kind: JoinType,
    swapped: bool,
    filter: Option<Expr>,
    pool: Arc<MemoryPool>,
) -> HashJoinExec {
    HashJoinExec::with_filter(
        left,
        right,
        vec![(Expr::column("lk"), Expr::column("rk"))],
        kind,
        filter,
    )
    .with_build_right(swapped)
    .with_memory_pool(pool)
}
type Row = Vec<Option<i64>>;
fn rows(batch: &RecordBatch) -> Vec<Row> {
    (0..batch.num_rows())
        .map(|r| {
            batch
                .columns()
                .iter()
                .map(|a| {
                    let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
                    (!a.is_null(r)).then(|| a.value(r))
                })
                .collect()
        })
        .collect()
}
async fn collect(join: &HashJoinExec) -> Vec<Row> {
    let mut result = vec![];
    // Sequential partition consumption deliberately tests that the last
    // completion emits unmatched BUILD rows even after earlier streams drop.
    for p in 0..join.output_partitions() {
        let mut stream = join.execute(p).await.unwrap();
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert!(batch.num_rows() <= 4096);
            result.extend(rows(&batch));
        }
    }
    result.sort();
    result
}

#[tokio::test]
async fn bounded_outer_produces_before_full_probe_and_owns_output_after_drop() {
    let left = input(vec![vec![batch(
        "l",
        vec![Some(1); 2000],
        (0..2000).collect(),
    )]]);
    let right = input(vec![vec![
        batch("r", vec![Some(1); 2000], (0..2000).collect()),
        batch("r", vec![Some(2)], vec![7]),
    ]]);
    let pool = Arc::new(MemoryPool::new(16 * 1024 * 1024));
    let join = operator(
        left,
        right.clone(),
        JoinType::Left,
        false,
        None,
        pool.clone(),
    );
    let mut output = join
        .execute(0)
        .await
        .expect("bounded outer must accept high fanout");
    assert_eq!(
        right.pulls.load(Ordering::SeqCst),
        0,
        "execute collected the probe"
    );
    let first = output.try_next().await.unwrap().unwrap();
    assert!(first.num_rows() > 0 && first.num_rows() <= 4096);
    assert_eq!(
        right.pulls.load(Ordering::SeqCst),
        1,
        "must not pull the next input batch"
    );
    for row in rows(&first) {
        assert_eq!(row[0], Some(1));
        assert_eq!(row[2], Some(1));
        assert!((0..2000).contains(&row[1].unwrap()));
    }
    let retained = pool.used();
    assert!(
        pool.reserved_peak() < 2 * 1024 * 1024,
        "candidate cardinality must not determine retained memory"
    );
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert_eq!(
        pool.used(),
        retained,
        "slow consumer caused eager production"
    );
    assert_eq!(right.pulls.load(Ordering::SeqCst), 1);
    drop(output);
    drop(join);
    drop(right);
    assert!(
        pool.used() > 0,
        "returned buffers must retain their own leases"
    );
    drop(first);
    assert_eq!(
        pool.used(),
        0,
        "all stream, round and output owners released"
    );
}

#[tokio::test]
async fn outer_rows_match_independent_oracle_across_orientations_and_rounds() {
    for kind in [JoinType::Left, JoinType::Right, JoinType::Full] {
        for swapped in [false, true] {
            for empty in [None, Some("l"), Some("r")] {
                let lv = if empty == Some("l") {
                    vec![]
                } else {
                    vec![(Some(1), 10), (Some(1), 11), (Some(2), 12), (None, 13)]
                };
                let rv = if empty == Some("r") {
                    vec![]
                } else {
                    vec![(Some(1), 20), (Some(1), 21), (Some(3), 22), (None, 23)]
                };
                let make = |side, values: &Vec<(Option<i64>, i64)>| {
                    let mid = values.len() / 2;
                    input(vec![
                        vec![batch(
                            side,
                            values[..mid].iter().map(|v| v.0).collect(),
                            values[..mid].iter().map(|v| v.1).collect(),
                        )],
                        vec![batch(
                            side,
                            values[mid..].iter().map(|v| v.0).collect(),
                            values[mid..].iter().map(|v| v.1).collect(),
                        )],
                    ])
                };
                let pool = Arc::new(MemoryPool::new(16 * 1024 * 1024));
                let join = operator(
                    make("l", &lv),
                    make("r", &rv),
                    kind,
                    swapped,
                    None,
                    pool.clone(),
                );
                let mut expected = vec![];
                for &(lk, lvalue) in &lv {
                    let mut found = false;
                    for &(rk, rvalue) in &rv {
                        if lk.is_some() && lk == rk {
                            expected.push(vec![lk, Some(lvalue), rk, Some(rvalue)]);
                            found = true;
                        }
                    }
                    if !found && kind != JoinType::Right {
                        expected.push(vec![lk, Some(lvalue), None, None]);
                    }
                }
                if kind != JoinType::Left {
                    for &(rk, rvalue) in &rv {
                        if !lv.iter().any(|(lk, _)| lk.is_some() && *lk == rk) {
                            expected.push(vec![None, None, rk, Some(rvalue)]);
                        }
                    }
                }
                expected.sort();
                assert_eq!(
                    collect(&join).await,
                    expected,
                    "{kind:?} swapped={swapped} empty={empty:?}"
                );
                assert_eq!(collect(&join).await, expected, "second round");
                drop(join);
                assert_eq!(pool.used(), 0);
            }
        }
    }
}

#[tokio::test]
async fn residual_predicate_tracks_matches_across_candidate_chunks() {
    let left = input(vec![vec![batch(
        "l",
        vec![Some(1), Some(2), None],
        vec![1, 0, 0],
    )]]);
    let right = input(vec![vec![batch(
        "r",
        vec![Some(1); 6000],
        (0..6000).collect(),
    )]]);
    let filter = Expr::BinaryExpr {
        left: Box::new(Expr::column("rv")),
        op: BinaryOp::Lt,
        right: Box::new(Expr::column("lv")),
    };
    let join = operator(
        left,
        right,
        JoinType::Left,
        true,
        Some(filter),
        Arc::new(MemoryPool::new(16 * 1024 * 1024)),
    );
    let mut expected = vec![
        vec![Some(1), Some(1), Some(1), Some(0)],
        vec![Some(2), Some(0), None, None],
        vec![None, Some(0), None, None],
    ];
    expected.sort();
    assert_eq!(collect(&join).await, expected);
}

#[tokio::test]
async fn cancelled_partition_does_not_authorize_unmatched_build_completion() {
    let left = input(vec![vec![batch("l", vec![Some(1), Some(2)], vec![10, 20])]]);
    let right = input(vec![
        vec![batch("r", vec![Some(1)], vec![30])],
        vec![batch("r", vec![Some(3)], vec![40])],
    ]);
    let pool = Arc::new(MemoryPool::new(16 * 1024 * 1024));
    let join = operator(left, right, JoinType::Full, false, None, pool.clone());
    let abandoned = join.execute(0).await.unwrap();
    let mut other = join.execute(1).await.unwrap();
    drop(abandoned);
    let error = loop {
        match other.try_next().await {
            Ok(Some(_)) => {}
            Ok(None) => panic!("cancelled cohort completed successfully"),
            Err(e) => break e,
        }
    };
    assert!(
        error.to_string().contains("failed or was cancelled"),
        "{error}"
    );
    drop(other);
    // A subsequent complete round starts with fresh match bits.
    assert_eq!(collect(&join).await.len(), 3);
    drop(join);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn hot_key_cursor_emits_every_pair_exactly_once() {
    let mut keys = vec![Some(1); 20003];
    keys.extend([Some(9), None]);
    let left = input(vec![vec![batch("l", keys, (0..20005).collect())]]);
    let right = input(vec![
        vec![batch("r", vec![Some(1), None], vec![10, 11])],
        vec![batch("r", vec![Some(1), Some(3)], vec![20, 21])],
    ]);
    let join = operator(
        left,
        right,
        JoinType::Full,
        false,
        None,
        Arc::new(MemoryPool::new(16 * 1024 * 1024)),
    );
    let mut expected = vec![];
    for value in 0..20003 {
        for other in [10, 20] {
            expected.push(vec![Some(1), Some(value), Some(1), Some(other)]);
        }
    }
    expected.extend([
        vec![Some(9), Some(20003), None, None],
        vec![None, Some(20004), None, None],
        vec![None, None, None, Some(11)],
        vec![None, None, Some(3), Some(21)],
    ]);
    expected.sort();
    assert_eq!(collect(&join).await, expected);
}
