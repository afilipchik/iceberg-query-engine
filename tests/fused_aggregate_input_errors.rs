//! A failed consuming input must never be replayed into a successful result.
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};
use query_engine::{
    execution::MemoryPool,
    physical::{operators::spillable, PhysicalOperator, RecordBatchStream},
    planner::{AggregateFunction, BinaryOp, Expr, ScalarValue},
    ExecutionConfig, QueryError, Result,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Clone, Copy, Debug)]
enum Fault {
    None,
    ExecuteError,
    StreamError,
    ExecutePanic,
    StreamPanic,
    WorkerError,
    BindingError,
}

#[derive(Debug)]
struct OneShotInput {
    batch: RecordBatch,
    calls: [AtomicUsize; 2],
    fault: Fault,
    pending_peer: bool,
    parallel: bool,
    peer_started: tokio::sync::Notify,
    pending_streams: Arc<AtomicUsize>,
}

struct PendingPeer(Arc<AtomicUsize>);
impl futures::Stream for PendingPeer {
    type Item = Result<RecordBatch>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Pending
    }
}
impl Drop for PendingPeer {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl PhysicalOperator for OneShotInput {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
    fn name(&self) -> &str {
        "OneShotFailingAggregateInput"
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn output_partitions(&self) -> usize {
        2
    }
    fn pool_independent_queue_copy_bound(
        &self,
    ) -> Option<query_engine::physical::queue_layout::QueueCopyBound> {
        self.parallel.then(|| {
            query_engine::physical::queue_layout::QueueCopyBound::from_batches(
                &self.batch.schema(),
                std::slice::from_ref(&self.batch),
            )
            .expect("fixed nullable Int64 fixture has a copied-output bound")
        })
    }
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, partition)?;
        let first = self.calls[partition].fetch_add(1, Ordering::SeqCst) == 0;
        // On the admitted parallel route, prove the sibling has actually
        // initialized before injecting the error; never rely on scheduler luck.
        if self.parallel {
            if partition == 0 {
                self.peer_started.notified().await;
            } else if !self.pending_peer {
                self.peer_started.notify_one();
            }
        }
        if self.pending_peer {
            if partition == 1 {
                self.pending_streams.fetch_add(1, Ordering::SeqCst);
                let stream = Box::pin(PendingPeer(self.pending_streams.clone()));
                if self.parallel {
                    self.peer_started.notify_one();
                }
                return Ok(stream);
            }
            tokio::task::yield_now().await;
        }
        if partition == 0 && first {
            match self.fault {
                Fault::ExecuteError => {
                    return Err(QueryError::Storage("one-shot source failure".into()))
                }
                Fault::ExecutePanic => panic!("one-shot source panic"),
                Fault::StreamError => {
                    return Ok(Box::pin(stream::iter([
                        Ok(self.batch.clone()),
                        Err(QueryError::Storage("one-shot source failure".into())),
                    ])))
                }
                Fault::StreamPanic => {
                    return Ok(Box::pin(
                        stream::iter([Ok(self.batch.clone())])
                            .chain(stream::once(async { panic!("one-shot source panic") })),
                    ))
                }
                Fault::WorkerError | Fault::BindingError | Fault::None => {}
            }
        }
        // A replay would succeed and thereby hide the first execution's failure.
        Ok(Box::pin(stream::iter([
            Ok(self.batch.slice(0, 0)),
            Ok(self.batch.clone()),
        ])))
    }
}

async fn check(fault: Fault, pending_peer: bool) {
    for (disjoint, parallel) in [(false, false), (true, false), (false, true), (true, true)] {
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, true),
            Field::new("v", DataType::Int64, true),
        ]));
        let input = Arc::new(OneShotInput {
            batch: RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![Some(1), Some(1), None])),
                    Arc::new(Int64Array::from(vec![Some(7), None, Some(3)])),
                ],
            )
            .unwrap(),
            calls: [AtomicUsize::new(0), AtomicUsize::new(0)],
            fault,
            pending_peer,
            parallel,
            peer_started: tokio::sync::Notify::new(),
            pending_streams: Arc::new(AtomicUsize::new(0)),
        });
        let pool = Arc::new(MemoryPool::new_named(
            "fused input error test",
            4 * 1024 * 1024,
        ));
        let operator = spillable::SpillableHashAggregateExec::new(
            input.clone(),
            vec![Expr::column("g")],
            vec![spillable::AggregateExpr {
                func: AggregateFunction::Sum,
                input: match fault {
                    Fault::BindingError => Expr::column("missing"),
                    Fault::WorkerError => Expr::BinaryExpr {
                        left: Box::new(Expr::column("v")),
                        op: BinaryOp::Divide,
                        right: Box::new(Expr::literal(ScalarValue::Int64(0))),
                    },
                    _ => Expr::column("v"),
                },
                distinct: false,
                second_arg: None,
            }],
            Arc::new(Schema::new(vec![
                Field::new("g", DataType::Int64, true),
                Field::new("s", DataType::Int64, true),
            ])),
            pool.clone(),
            ExecutionConfig::new().with_memory_limit(4 * 1024 * 1024),
        )
        .with_disjoint_groups(disjoint);
        let result = tokio::time::timeout(std::time::Duration::from_secs(2), async {
            match operator.execute(0).await {
                Ok(stream) => stream.try_collect::<Vec<RecordBatch>>().await,
                Err(error) => Err(error),
            }
        })
        .await
        .expect("input failure waited forever for a pending sibling");
        if matches!(fault, Fault::None) {
            let batches = result.unwrap();
            let mut rows = Vec::new();
            for batch in &batches {
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let sums = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    rows.push((
                        (!keys.is_null(row)).then(|| keys.value(row)),
                        (!sums.is_null(row)).then(|| sums.value(row)),
                    ));
                }
            }
            rows.sort_unstable();
            assert_eq!(rows, vec![(None, Some(6)), (Some(1), Some(14))]);
            drop(batches);
        } else {
            let error = result.expect_err("a failed input was replayed into success");
            match fault {
                Fault::ExecuteError | Fault::StreamError => assert!(
                    matches!(error.root(), QueryError::Storage(message) if message == "one-shot source failure"),
                    "original error classification lost: {error}"
                ),
                Fault::ExecutePanic | Fault::StreamPanic => assert!(
                    error.to_string().contains("one-shot source panic"),
                    "panic lost: {error}"
                ),
                Fault::WorkerError => assert!(
                    error.to_string().to_lowercase().contains("zero"),
                    "runtime arithmetic error lost: {error}"
                ),
                Fault::BindingError => assert!(
                    error.to_string().contains("missing"),
                    "binding error lost: {error}"
                ),
                Fault::None => unreachable!(),
            }
        }
        for (partition, calls) in input.calls.iter().enumerate() {
            assert_eq!(
                calls.load(Ordering::SeqCst),
                if matches!(fault, Fault::BindingError) {
                    0
                } else if partition == 0 || parallel || matches!(fault, Fault::None) {
                    1
                } else {
                    // Serial failures must not start a later partition.
                    0
                },
                "input start/replay contract violated (parallel={parallel}, partition={partition})"
            );
        }
        assert_eq!(pool.used(), 0, "worker state leaked on error");
        assert_eq!(
            input.pending_streams.load(Ordering::SeqCst),
            0,
            "pending input leaked"
        );
    }
}

#[tokio::test]
async fn execute_error_is_terminal_without_replay() {
    check(Fault::ExecuteError, false).await;
}
#[tokio::test]
async fn stream_error_after_a_prefix_is_terminal_without_replay() {
    check(Fault::StreamError, false).await;
}
#[tokio::test]
async fn execute_panic_is_terminal_without_replay() {
    check(Fault::ExecutePanic, false).await;
}
#[tokio::test]
async fn stream_panic_after_a_prefix_is_terminal_without_replay() {
    check(Fault::StreamPanic, false).await;
}

#[tokio::test]
async fn input_failure_cancels_pending_sibling() {
    check(Fault::StreamError, true).await;
}

#[tokio::test]
async fn worker_failure_cancels_pending_input() {
    check(Fault::WorkerError, true).await;
}

#[tokio::test]
async fn successful_partitions_keep_duplicates_nulls_and_empty_batches() {
    check(Fault::None, false).await;
}

#[tokio::test]
async fn binding_failure_opens_no_input_partition() {
    check(Fault::BindingError, true).await;
}
