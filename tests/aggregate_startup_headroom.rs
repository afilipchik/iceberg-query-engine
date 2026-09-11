//! Reclaim unused worker state without reopening an unknown-bound input.
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
        operators::spillable, PhysicalOperator, PreparedOutputBound, PreparedQueueInput,
        RecordBatchStream,
    },
    planner::{AggregateFunction, Expr},
    ExecutionConfig, QueryError, Result,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fault {
    None,
    Prepare,
    Late,
}

#[derive(Debug)]
struct PreparedInput {
    batch: RecordBatch,
    prepares: AtomicUsize,
    opens: [AtomicUsize; 2],
    active: Arc<AtomicUsize>,
    fault: Fault,
    pool: Arc<MemoryPool>,
    prepare_bytes: usize,
    produce_bytes: usize,
}
struct StreamOwner(Arc<AtomicUsize>);
impl Drop for StreamOwner {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl PhysicalOperator for PreparedInput {
    fn name(&self) -> &str {
        "UnknownBoundStartupInput"
    }
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn output_partitions(&self) -> usize {
        2
    }
    async fn prepare_queue_input(&self) -> Result<Option<PreparedQueueInput>> {
        assert_eq!(
            self.prepares.fetch_add(1, Ordering::SeqCst),
            0,
            "input factory replayed"
        );
        if self.fault == Fault::Prepare {
            return Err(QueryError::Storage("startup prepare sentinel".into()));
        }
        let _build_scratch = self.pool.allocate(self.prepare_bytes)?;
        let mut streams = Vec::new();
        for partition in 0..2 {
            streams.push(self.execute(partition).await?);
        }
        Ok(Some(PreparedQueueInput {
            streams,
            output: PreparedOutputBound::Unknown,
        }))
    }
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, partition)?;
        assert_eq!(
            self.opens[partition].fetch_add(1, Ordering::SeqCst),
            0,
            "partition replayed"
        );
        self.active.fetch_add(1, Ordering::SeqCst);
        let owner = StreamOwner(self.active.clone());
        let late = self.fault == Fault::Late && partition == 0;
        let pool = self.pool.clone();
        let produce_bytes = self.produce_bytes;
        let initial = usize::from(produce_bytes != 0);
        Ok(Box::pin(stream::unfold(
            (initial, self.batch.clone(), owner),
            move |(index, batch, owner)| {
                let pool = pool.clone();
                async move {
                    if index == initial {
                        if let Err(error) = pool.allocate(produce_bytes) {
                            return Some((Err(error), (4, batch, owner)));
                        }
                    }
                    let item = match index {
                        0 => Ok(RecordBatch::new_empty(batch.schema())),
                        1 | 2 => Ok(batch.clone()),
                        3 if late => Err(QueryError::Storage("startup late sentinel".into())),
                        _ => return None,
                    };
                    Some((item, (index + 1, batch, owner)))
                }
            },
        )))
    }
}

async fn check(
    rows: i64,
    fault: Fault,
    budget: usize,
    denied: bool,
    prepare_bytes: usize,
    produce_bytes: usize,
) {
    let keys: Vec<Option<i64>> = (0..rows).map(|i| (i % 17 != 0).then_some(i % 5)).collect();
    let values: Vec<Option<i64>> = (0..rows).map(|i| (i % 11 != 0).then_some(i)).collect();
    let mut expected = BTreeMap::<Option<i64>, (i64, Option<i64>)>::new();
    for (&key, &value) in keys.iter().zip(&values) {
        let state = expected.entry(key).or_default();
        if let Some(value) = value {
            state.0 += 4;
            *state.1.get_or_insert(0) += 4 * value;
        }
    }
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(keys)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap();
    let pool = Arc::new(MemoryPool::new_named("startup query", budget));
    let input = Arc::new(PreparedInput {
        batch,
        prepares: AtomicUsize::new(0),
        opens: [AtomicUsize::new(0), AtomicUsize::new(0)],
        active: Arc::new(AtomicUsize::new(0)),
        fault,
        pool: pool.clone(),
        prepare_bytes,
        produce_bytes,
    });
    let directory = tempfile::tempdir().unwrap();
    let operator = spillable::SpillableHashAggregateExec::new(
        input.clone(),
        vec![Expr::column("g")],
        [AggregateFunction::Count, AggregateFunction::Sum]
            .into_iter()
            .map(|func| spillable::AggregateExpr {
                func,
                input: Expr::column("v"),
                distinct: false,
                second_arg: None,
            })
            .collect(),
        Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("c", DataType::Int64, false),
            Field::new("s", DataType::Int64, true),
        ])),
        pool.clone(),
        ExecutionConfig::new()
            .with_memory_limit(budget)
            .with_spill_path(directory.path().to_path_buf()),
    );
    let result: Result<Vec<RecordBatch>> =
        async { operator.execute(0).await?.try_collect().await }.await;
    if !denied && fault == Fault::None {
        assert!(result.is_ok(), "fitting prepared input failed: {result:?}");
    }
    assert_eq!(input.prepares.load(Ordering::SeqCst), 1);
    for opens in &input.opens {
        assert_eq!(
            opens.load(Ordering::SeqCst),
            usize::from(fault != Fault::Prepare)
        );
    }
    assert_eq!(
        input.active.load(Ordering::SeqCst),
        0,
        "input stream owner leaked"
    );
    if denied {
        assert!(result.unwrap_err().is_memory_limit());
    } else if fault != Fault::None {
        let error = result.unwrap_err().to_string();
        assert!(
            error.contains(if fault == Fault::Prepare {
                "startup prepare sentinel"
            } else {
                "startup late sentinel"
            }),
            "{error}"
        );
    } else {
        for batch in result.unwrap() {
            let arrays: Vec<_> = batch
                .columns()
                .iter()
                .map(|a| a.as_any().downcast_ref::<Int64Array>().unwrap())
                .collect();
            for row in 0..batch.num_rows() {
                let key = (!arrays[0].is_null(row)).then(|| arrays[0].value(row));
                let actual = (
                    arrays[1].value(row),
                    (!arrays[2].is_null(row)).then(|| arrays[2].value(row)),
                );
                assert!(!arrays[1].is_null(row));
                assert_eq!(
                    Some(actual),
                    expected.remove(&key),
                    "duplicate or incorrect group"
                );
            }
        }
        assert!(expected.is_empty());
    }
    drop(operator);
    assert_eq!(pool.used(), 0);
    assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn prepared_unknown_input_completes_once_with_exact_null_and_duplicate_groups() {
    for rows in [0, 1, 8192] {
        check(rows, Fault::None, 256 * 1024, false, 0, 0).await;
    }
}
#[tokio::test]
async fn prepared_and_late_errors_remain_terminal_without_source_replay() {
    for fault in [Fault::Prepare, Fault::Late] {
        check(8192, fault, 256 * 1024, false, 0, 0).await;
    }
}
#[tokio::test]
async fn input_larger_than_query_budget_refuses_and_releases_owners() {
    check(8192, Fault::None, 64 * 1024, true, 0, 0).await;
}

#[tokio::test]
async fn prepared_build_scratch_is_admitted_before_worker_startup() {
    check(8192, Fault::None, 256 * 1024, false, 128 * 1024, 0).await;
}

#[tokio::test]
async fn first_producer_pull_is_admitted_before_optional_worker_state() {
    check(8192, Fault::None, 256 * 1024, false, 0, 160 * 1024).await;
}
