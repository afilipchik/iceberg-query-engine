//! Table scan operator

use crate::error::Result;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

/// Table statistics from a data source
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// Exact row count from metadata
    pub row_count: usize,
    /// Total size in bytes (approximate)
    pub total_byte_size: u64,
}

/// Table provider trait for accessing table data
pub trait TableProvider: Send + Sync + fmt::Debug {
    /// Get the schema of the table
    fn schema(&self) -> SchemaRef;

    /// Get all batches from the table
    fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>>;

    /// Scan with an optional filter predicate for row group pruning.
    /// Default implementation ignores the filter and delegates to `scan()`.
    fn scan_with_filter(
        &self,
        projection: Option<&[usize]>,
        _filter: Option<&crate::planner::Expr>,
    ) -> Result<Vec<RecordBatch>> {
        self.scan(projection)
    }

    /// Get table-level statistics (row count, byte size).
    /// Returns None if statistics are not available.
    fn statistics(&self) -> Option<TableStatistics> {
        None
    }

    /// Get Parquet file paths if this is a Parquet-based table
    /// Returns None for non-Parquet tables (e.g., MemoryTable)
    fn parquet_files(&self) -> Option<Vec<PathBuf>> {
        None
    }
}

/// In-memory table provider
#[derive(Debug, Clone)]
pub struct MemoryTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl MemoryTable {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }

    pub fn try_new(batches: Vec<RecordBatch>) -> Result<Self> {
        let schema = if batches.is_empty() {
            Arc::new(arrow::datatypes::Schema::empty())
        } else {
            batches[0].schema()
        };
        Ok(Self { schema, batches })
    }
}

impl TableProvider for MemoryTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        match projection {
            Some(indices) => self
                .batches
                .iter()
                .map(|batch| {
                    let columns: Vec<_> =
                        indices.iter().map(|&i| batch.column(i).clone()).collect();
                    let fields: Vec<_> = indices
                        .iter()
                        .map(|&i| self.schema.field(i).clone())
                        .collect();
                    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
                    RecordBatch::try_new(schema, columns).map_err(Into::into)
                })
                .collect(),
            None => Ok(self.batches.clone()),
        }
    }
}

/// Memory table scan operator
#[derive(Debug)]
pub struct MemoryTableExec {
    table_name: String,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    projection: Option<Vec<usize>>,
}

impl MemoryTableExec {
    pub fn new(
        table_name: impl Into<String>,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let projected_schema = match &projection {
            Some(indices) => {
                let fields: Vec<_> = indices.iter().map(|&i| schema.field(i).clone()).collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => schema.clone(),
        };

        Self {
            table_name: table_name.into(),
            schema: projected_schema,
            batches,
            projection,
        }
    }

    pub fn from_provider(
        table_name: impl Into<String>,
        provider: &dyn TableProvider,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let batches = provider.scan(projection.as_deref())?;
        let schema = match &projection {
            Some(indices) => {
                let base_schema = provider.schema();
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| base_schema.field(i).clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => provider.schema(),
        };

        Ok(Self {
            table_name: table_name.into(),
            schema,
            batches,
            projection: None, // Already projected
        })
    }

    /// Create from provider with a specified logical schema (preserves table aliases)
    pub fn from_provider_with_schema(
        table_name: impl Into<String>,
        provider: &dyn TableProvider,
        projection: Option<Vec<usize>>,
        logical_schema: SchemaRef,
    ) -> Result<Self> {
        let batches = provider.scan(projection.as_deref())?;

        // Use the logical schema which has proper qualified names
        let schema = match &projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| logical_schema.field(i).clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => logical_schema,
        };

        Ok(Self {
            table_name: table_name.into(),
            schema,
            batches,
            projection: None, // Already projected
        })
    }
}

#[async_trait]
impl PhysicalOperator for MemoryTableExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        // Determine the number of partitions to use
        let num_partitions = self.output_partitions().max(1);

        // Split batches across partitions
        let partition_batches: Vec<RecordBatch> = self
            .batches
            .iter()
            .enumerate()
            .filter(|(i, _)| i % num_partitions == partition)
            .map(|(_, batch)| batch.clone())
            .collect();

        let batches = match &self.projection {
            Some(indices) => partition_batches
                .iter()
                .map(|batch| {
                    let columns: Vec<_> =
                        indices.iter().map(|&i| batch.column(i).clone()).collect();
                    RecordBatch::try_new(self.schema.clone(), columns).map_err(Into::into)
                })
                .collect::<Result<Vec<_>>>()?,
            None => {
                // Re-wrap batches with the logical schema to preserve qualified names
                // (e.g., "n1.n_name" vs "n2.n_name" for self-joins)
                partition_batches
                    .into_iter()
                    .map(|batch| {
                        if batch.schema() != self.schema
                            && batch.num_columns() == self.schema.fields().len()
                        {
                            RecordBatch::try_new(self.schema.clone(), batch.columns().to_vec())
                                .map_err(Into::into)
                        } else {
                            Ok(batch)
                        }
                    })
                    .collect::<Result<Vec<_>>>()?
            }
        };

        let stream = stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(stream))
    }

    fn output_partitions(&self) -> usize {
        // Use rayon to determine the number of CPU cores for parallel execution
        // For small tables, use fewer partitions to avoid overhead
        let total_rows: usize = self.batches.iter().map(|b| b.num_rows()).sum();
        if total_rows < 1000 {
            1 // Small table, single partition
        } else {
            std::cmp::min(rayon::current_num_threads(), self.batches.len())
        }
    }

    fn name(&self) -> &str {
        "MemoryTableScan"
    }
}

impl fmt::Display for MemoryTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryTableScan: {}", self.table_name)?;
        if let Some(proj) = &self.projection {
            write!(f, " projection={:?}", proj)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::TryStreamExt;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_memory_scan() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let exec = MemoryTableExec::new("test", schema, vec![batch.clone()], None);

        let stream = exec.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
        assert_eq!(results[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn test_memory_scan_with_projection() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let exec = MemoryTableExec::new("test", schema, vec![batch], Some(vec![0]));

        let stream = exec.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
        assert_eq!(results[0].num_columns(), 1);
    }
}
