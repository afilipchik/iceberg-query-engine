//! Optional shared-scan caching must not hide a consuming provider's error.
use arrow::{array::Int64Array, datatypes::SchemaRef, record_batch::RecordBatch};
use query_engine::{physical::operators::TableProvider, ExecutionContext, QueryError, Result};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

#[derive(Debug)]
struct FailFirst {
    batch: RecordBatch,
    calls: AtomicUsize,
    fail: bool,
    memory_failure: bool,
}

impl TableProvider for FailFirst {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        if self.calls.fetch_add(1, Ordering::SeqCst) == 0 && self.fail {
            if self.memory_failure {
                return Err(QueryError::MemoryLimit {
                    pool: "shared provider".into(),
                    requested: 2048,
                    used: 512,
                    limit: 1024,
                });
            }
            return Err(QueryError::Storage(
                "shared source consumed then failed".into(),
            ));
        }
        let batch = match projection {
            Some(indices) => self.batch.project(indices)?,
            None => self.batch.clone(),
        };
        Ok(vec![batch.slice(0, 0), batch])
    }
}

fn provider(fail: bool) -> Arc<FailFirst> {
    Arc::new(FailFirst {
        batch: RecordBatch::try_from_iter(vec![(
            "v",
            Arc::new(Int64Array::from(vec![Some(1), None, Some(1)])) as arrow::array::ArrayRef,
        )])
        .unwrap(),
        calls: AtomicUsize::new(0),
        fail,
        memory_failure: false,
    })
}

#[tokio::test]
async fn shared_scan_preserves_typed_memory_refusal() {
    let mut table = provider(true);
    Arc::get_mut(&mut table).unwrap().memory_failure = true;
    let mut context = ExecutionContext::new();
    context.register_table_provider("t", table.clone());
    let error = context
        .sql("SELECT v FROM t UNION ALL SELECT v FROM t")
        .await
        .unwrap_err();
    assert!(
        matches!(error, QueryError::MemoryLimit { ref pool, requested: 2048, used: 512, limit: 1024 } if pool == "shared provider")
    );
    assert_eq!(table.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn shared_scan_failure_is_not_retried_into_success() {
    let table = provider(true);
    let mut context = ExecutionContext::new();
    context.register_table_provider("t", table.clone());
    let result = context
        .sql("SELECT v FROM t UNION ALL SELECT v FROM t")
        .await;
    assert!(
        matches!(&result, Err(QueryError::Storage(message)) if message == "shared source consumed then failed"),
        "original provider failure was hidden: {result:?}"
    );
    assert_eq!(table.calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn successful_shared_scan_is_cached_without_losing_duplicates_or_nulls() {
    let table = provider(false);
    let mut context = ExecutionContext::new();
    context.register_table_provider("t", table.clone());
    let result = context
        .sql("SELECT v FROM t UNION ALL SELECT v FROM t")
        .await
        .unwrap();
    assert_eq!(result.row_count, 6);
    let mut values = result
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    values.sort();
    assert_eq!(values, vec![None, None, Some(1), Some(1), Some(1), Some(1)]);
    assert_eq!(table.calls.load(Ordering::SeqCst), 1);
}
