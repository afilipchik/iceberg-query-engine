//! Initialization owns every async producer; acknowledgments replace timing assumptions.
use arrow::{
    array::Int64Array,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{stream, TryStreamExt};
use query_engine::{
    error::{QueryError, Result},
    physical::{HashJoinExec, PhysicalOperator, RecordBatchStream},
    planner::{Expr, JoinType},
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::Semaphore;

const GUARD: Duration = Duration::from_secs(10);
#[derive(Debug)]
struct Signals {
    started: Semaphore,
    dropped: Semaphore,
    trigger: Semaphore,
    mode: AtomicUsize,
    calls: AtomicUsize,
}
impl Signals {
    fn new(mode: usize) -> Arc<Self> {
        Arc::new(Self {
            started: Semaphore::new(0),
            dropped: Semaphore::new(0),
            trigger: Semaphore::new(0),
            mode: AtomicUsize::new(mode),
            calls: AtomicUsize::new(0),
        })
    }
}
struct DropAck(Arc<Signals>);
impl Drop for DropAck {
    fn drop(&mut self) {
        self.0.dropped.add_permits(1);
    }
}
async fn permits(semaphore: &Semaphore, count: u32) {
    tokio::time::timeout(GUARD, semaphore.acquire_many(count))
        .await
        .expect("producer ownership deadlock")
        .unwrap()
        .forget();
}
#[derive(Debug)]
struct Input {
    schema: SchemaRef,
    partitions: usize,
    signals: Arc<Signals>,
    payload: i64,
}
impl Input {
    fn new(prefix: &str, partitions: usize, mode: usize, payload: i64) -> Arc<Self> {
        Arc::new(Self {
            schema: Arc::new(Schema::new(vec![
                Field::new(format!("{prefix}k"), DataType::Int64, false),
                Field::new(format!("{prefix}v"), DataType::Int64, false),
            ])),
            partitions,
            signals: Signals::new(mode),
            payload,
        })
    }
}
#[async_trait]
impl PhysicalOperator for Input {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn name(&self) -> &str {
        "InitializationOwnershipInput"
    }
    fn output_partitions(&self) -> usize {
        self.partitions
    }
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        self.signals.calls.fetch_add(1, Ordering::SeqCst);
        query_engine::physical::check_partition(self, partition)?;
        let signals = self.signals.clone();
        let schema = self.schema.clone();
        let payload = self.payload + partition as i64;
        let is_last = partition + 1 == self.partitions;
        let ack = DropAck(signals.clone());
        Ok(Box::pin(stream::once(async move {
            let _ack = ack;
            signals.started.add_permits(1);
            let mode = signals.mode.load(Ordering::SeqCst);
            match mode {
                0 => futures::future::pending::<()>().await,
                2 | 3 if is_last => {
                    signals.trigger.acquire().await.unwrap().forget();
                    if mode == 3 {
                        panic!("injected partition panic");
                    }
                    return Err(QueryError::Execution("injected partition failure".into()));
                }
                2 | 3 => futures::future::pending::<()>().await,
                1 => (),
                _ => unreachable!(),
            }
            Ok(RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![1])),
                    Arc::new(Int64Array::from(vec![payload])),
                ],
            )
            .unwrap())
        })))
    }
}
fn join(build: Arc<Input>, probe: Arc<Input>, kind: JoinType) -> Arc<HashJoinExec> {
    Arc::new(HashJoinExec::new(
        build,
        probe,
        vec![(Expr::column("bk"), Expr::column("pk"))],
        kind,
    ))
}

#[tokio::test]
async fn cancelled_build_drops_all_partitions_and_same_cache_retries() {
    let build = Input::new("b", 3, 0, 10);
    let probe = Input::new("p", 1, 1, 100);
    let join = join(build.clone(), probe, JoinType::Inner);
    let task = tokio::spawn({
        let join = join.clone();
        async move { join.execute(0).await }
    });
    permits(&build.signals.started, 3).await;
    task.abort();
    match task.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("aborted initializer completed unexpectedly"),
    }
    permits(&build.signals.dropped, 3).await;
    build.signals.mode.store(1, Ordering::SeqCst);
    let mut stream = tokio::time::timeout(GUARD, join.execute(0))
        .await
        .unwrap()
        .unwrap();
    let mut rows = vec![];
    while let Some(batch) = tokio::time::timeout(GUARD, stream.try_next())
        .await
        .unwrap()
        .unwrap()
    {
        for row in 0..batch.num_rows() {
            rows.push(
                (0..4)
                    .map(|column| {
                        batch
                            .column(column)
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(row)
                    })
                    .collect::<Vec<_>>(),
            );
        }
    }
    rows.sort();
    assert_eq!(
        rows,
        vec![
            vec![1, 10, 1, 100],
            vec![1, 11, 1, 100],
            vec![1, 12, 1, 100]
        ]
    );
    assert_eq!(build.signals.calls.load(Ordering::SeqCst), 6);
}

async fn failed_build(mode: usize) {
    let build = Input::new("b", 3, mode, 10);
    let join = join(build.clone(), Input::new("p", 1, 1, 100), JoinType::Inner);
    let task = tokio::spawn(async move { join.execute(0).await });
    permits(&build.signals.started, 3).await;
    build.signals.trigger.add_permits(1);
    let outcome = tokio::time::timeout(GUARD, task)
        .await
        .expect("failure must not wait for earlier parked partition")
        .unwrap();
    assert!(
        outcome.is_err(),
        "partition failure/panic must be a query error"
    );
    permits(&build.signals.dropped, 3).await;
}
#[tokio::test]
async fn build_error_cancels_parked_siblings() {
    failed_build(2).await;
}
#[tokio::test]
async fn build_panic_cancels_parked_siblings() {
    failed_build(3).await;
}

#[tokio::test]
async fn semi_and_anti_build_failure_cancel_prefetched_probe_children() {
    for kind in [JoinType::Semi, JoinType::Anti] {
        let build = Input::new("b", 2, 2, 10);
        let probe = Input::new("p", 3, 0, 100);
        let join = join(build.clone(), probe.clone(), kind);
        let task = tokio::spawn(async move { join.execute(0).await });
        permits(&build.signals.started, 2).await;
        permits(&probe.signals.started, 3).await;
        build.signals.trigger.add_permits(1);
        let outcome = tokio::time::timeout(GUARD, task).await.unwrap().unwrap();
        assert!(outcome.is_err());
        permits(&build.signals.dropped, 2).await;
        permits(&probe.signals.dropped, 3).await;
    }
}

#[tokio::test]
async fn zero_declared_build_partitions_are_never_executed() {
    let build = Input::new("b", 0, 1, 10);
    let join = join(build.clone(), Input::new("p", 1, 1, 100), JoinType::Inner);
    let mut output = tokio::time::timeout(GUARD, join.execute(0))
        .await
        .unwrap()
        .unwrap();
    assert!(output.try_next().await.unwrap().is_none());
    assert_eq!(build.signals.calls.load(Ordering::SeqCst), 0);
}
