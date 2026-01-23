//! Limit operator

use crate::error::Result;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream::StreamExt;
use std::fmt;
use std::sync::Arc;

/// Limit execution operator
#[derive(Debug)]
pub struct LimitExec {
    input: Arc<dyn PhysicalOperator>,
    skip: usize,
    fetch: Option<usize>,
    schema: SchemaRef,
}

impl LimitExec {
    pub fn new(input: Arc<dyn PhysicalOperator>, skip: usize, fetch: Option<usize>) -> Self {
        let schema = input.schema();
        Self {
            input,
            skip,
            fetch,
            schema,
        }
    }
}

#[async_trait]
impl PhysicalOperator for LimitExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.input.clone()]
    }

    #[allow(unused_assignments)] // Variables are read across multiple closure invocations
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        let input_stream = self.input.execute(partition).await?;
        let skip = self.skip;
        let fetch = self.fetch;
        let schema = self.schema.clone();

        let mut skipped = 0usize;
        let mut fetched = 0usize;

        let limited = input_stream.filter_map(move |result| {
            let _schema = schema.clone();
            async move {
                match result {
                    Ok(batch) => {
                        let num_rows = batch.num_rows();

                        // Handle skip
                        if skipped < skip {
                            let to_skip = (skip - skipped).min(num_rows);
                            skipped += to_skip;

                            if to_skip >= num_rows {
                                // Skip entire batch
                                return None;
                            }

                            // Partial skip - slice batch
                            let remaining = num_rows - to_skip;
                            let sliced = batch.slice(to_skip, remaining);

                            // Now handle fetch
                            if let Some(limit) = fetch {
                                if fetched >= limit {
                                    return None;
                                }
                                let to_fetch = (limit - fetched).min(sliced.num_rows());
                                fetched += to_fetch;
                                if to_fetch < sliced.num_rows() {
                                    return Some(Ok(sliced.slice(0, to_fetch)));
                                }
                            }

                            return Some(Ok(sliced));
                        }

                        // Handle fetch after skip is done
                        if let Some(limit) = fetch {
                            if fetched >= limit {
                                return None;
                            }
                            let to_fetch = (limit - fetched).min(num_rows);
                            fetched += to_fetch;
                            if to_fetch < num_rows {
                                return Some(Ok(batch.slice(0, to_fetch)));
                            }
                        }

                        Some(Ok(batch))
                    }
                    Err(e) => Some(Err(e)),
                }
            }
        });

        Ok(Box::pin(limited))
    }

    fn name(&self) -> &str {
        "Limit"
    }
}

impl fmt::Display for LimitExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Limit: skip={}, fetch={:?}", self.skip, self.fetch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::MemoryTableExec;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_limit_fetch() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));
        let limit = LimitExec::new(scan, 0, Some(3));

        let stream = limit.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_limit_skip() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));
        let limit = LimitExec::new(scan, 2, None);

        let stream = limit.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // 5 - 2 = 3
    }

    #[tokio::test]
    async fn test_limit_skip_and_fetch() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));
        let limit = LimitExec::new(scan, 1, Some(2));

        let stream = limit.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        // Should be values 2 and 3 (skip 1, fetch 2)
        let ids = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
        assert_eq!(ids.value(1), 3);
    }
}
