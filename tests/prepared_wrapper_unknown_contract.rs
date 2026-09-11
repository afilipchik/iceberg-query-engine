//! Prepared lifecycle survives loss of a physical output bound.
use arrow::{
    array::{Array, ArrayRef, NullArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{stream, TryStreamExt};
use query_engine::{
    physical::{
        queue_layout::{PreparedOutputLayouts, QueueCopyBound},
        PhysicalOperator, PreparedOutputBound, PreparedQueueInput, ProjectExec, RecordBatchStream,
    },
    planner::Expr,
    Result,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
struct NullPrepared {
    batch: RecordBatch,
    calls: Arc<AtomicUsize>,
    pulls: Arc<AtomicUsize>,
    released: Arc<AtomicUsize>,
}
struct Ack(Arc<AtomicUsize>);
impl Drop for Ack {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl PhysicalOperator for NullPrepared {
    fn name(&self) -> &str {
        "null prepared"
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
    async fn execute(&self, _: usize) -> Result<RecordBatchStream> {
        panic!("must not execute prepared source twice")
    }
    async fn prepare_queue_input(&self) -> Result<Option<PreparedQueueInput>> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let bound =
            QueueCopyBound::from_batches(&self.batch.schema(), &[self.batch.clone()]).unwrap();
        let mut streams = Vec::new();
        for _ in 0..3 {
            let batch = self.batch.clone();
            let pulls = self.pulls.clone();
            let ack = Ack(self.released.clone());
            streams.push(Box::pin(stream::once(async move {
                let _ack = ack;
                pulls.fetch_add(1, Ordering::SeqCst);
                Ok(batch)
            })) as RecordBatchStream);
        }
        Ok(Some(PreparedQueueInput {
            streams,
            output: PreparedOutputBound::Layouts(PreparedOutputLayouts::from_bound(bound).unwrap()),
        }))
    }
}
fn fixture() -> Arc<NullPrepared> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "source",
        DataType::Null,
        true,
    )]));
    Arc::new(NullPrepared {
        batch: RecordBatch::try_new(schema, vec![Arc::new(NullArray::new(3)) as ArrayRef]).unwrap(),
        calls: Arc::new(AtomicUsize::new(0)),
        pulls: Arc::new(AtomicUsize::new(0)),
        released: Arc::new(AtomicUsize::new(0)),
    })
}
fn projected(source: Arc<NullPrepared>) -> ProjectExec {
    ProjectExec::new(
        source,
        vec![
            Expr::column("source").alias("typed"),
            Expr::column("source").alias("duplicate"),
        ],
        Arc::new(Schema::new(vec![
            Field::new("typed", DataType::Int64, true),
            Field::new("duplicate", DataType::Int64, true),
        ])),
    )
}
#[tokio::test]
async fn post_preparation_null_retyping_decline_reuses_exact_unpulled_streams() {
    let source = fixture();
    let project = projected(source.clone());
    let prepared = project
        .prepare_queue_input()
        .await
        .unwrap()
        .expect("lifecycle must survive metadata decline");
    assert!(matches!(prepared.output, PreparedOutputBound::Unknown));
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
    assert_eq!(source.pulls.load(Ordering::SeqCst), 0);
    let mut rows = 0;
    for mut stream in prepared.streams {
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert_eq!(batch.num_columns(), 2);
            for array in batch.columns() {
                assert_eq!(array.data_type(), &DataType::Int64);
                assert_eq!(array.null_count(), 3);
            }
            rows += batch.num_rows();
        }
    }
    assert_eq!(rows, 9);
    assert_eq!(source.pulls.load(Ordering::SeqCst), 3);
    assert_eq!(source.released.load(Ordering::SeqCst), 3);
    assert_eq!(source.calls.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn dropping_unknown_prepared_wrapper_drops_each_unpulled_stream() {
    let source = fixture();
    let project = projected(source.clone());
    let prepared = project.prepare_queue_input().await.unwrap().unwrap();
    assert!(prepared.output.max_bytes().is_none());
    drop(prepared);
    assert_eq!(source.pulls.load(Ordering::SeqCst), 0);
    assert_eq!(source.released.load(Ordering::SeqCst), 3);
}
