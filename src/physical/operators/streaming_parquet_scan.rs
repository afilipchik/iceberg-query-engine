//! Streaming Parquet scan operator
//!
//! Reads Parquet files row-group by row-group instead of materializing
//! entire tables into memory. Supports row group pruning via filter predicates.

use crate::error::{QueryError, Result};
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::Expr;
use crate::storage::row_group_pruning;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fmt;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

/// A work unit: one row group from one file
#[derive(Debug, Clone)]
struct RowGroupWork {
    file_path: PathBuf,
    row_group_idx: usize,
}

/// Streaming Parquet scan operator that reads row groups on-demand.
///
/// Unlike `MemoryTableExec` which materializes all data before processing,
/// this operator lazily reads one row group at a time, keeping memory usage
/// bounded by `batch_size * num_partitions`.
pub struct StreamingParquetScanExec {
    table_name: String,
    /// Logical schema with proper qualified column names
    schema: SchemaRef,
    /// Projection column indices
    projection: Option<Vec<usize>>,
    /// Row groups to read, distributed across partitions
    partitioned_work: Vec<Vec<RowGroupWork>>,
    /// Batch size for reading
    batch_size: usize,
}

impl fmt::Debug for StreamingParquetScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingParquetScanExec")
            .field("table_name", &self.table_name)
            .field("partitions", &self.partitioned_work.len())
            .field(
                "total_row_groups",
                &self.partitioned_work.iter().map(|p| p.len()).sum::<usize>(),
            )
            .finish()
    }
}

impl StreamingParquetScanExec {
    /// Create a new streaming Parquet scan.
    ///
    /// Reads file footers, applies row group pruning based on the filter,
    /// and distributes work items across partitions.
    pub fn try_new(
        table_name: impl Into<String>,
        files: &[PathBuf],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filter: Option<&Expr>,
        provider_schema: &SchemaRef,
    ) -> Result<Self> {
        let table_name = table_name.into();
        let batch_size = 8_192;

        // Discover matching row groups from all files
        let mut all_work = Vec::new();
        for file_path in files {
            let file = File::open(file_path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let metadata = builder.metadata().clone();

            let matching_rgs =
                row_group_pruning::prune_row_groups(&metadata, provider_schema, filter);

            for rg_idx in matching_rgs {
                all_work.push(RowGroupWork {
                    file_path: file_path.clone(),
                    row_group_idx: rg_idx,
                });
            }
        }

        // Distribute row groups round-robin across partitions
        let num_threads = rayon::current_num_threads();
        let num_partitions = if all_work.is_empty() {
            1
        } else {
            std::cmp::min(num_threads, all_work.len())
        };

        let mut partitioned_work = vec![Vec::new(); num_partitions];
        for (i, work) in all_work.into_iter().enumerate() {
            partitioned_work[i % num_partitions].push(work);
        }

        // Compute projected schema
        let projected_schema = match &projection {
            Some(indices) => {
                let fields: Vec<_> = indices.iter().map(|&i| schema.field(i).clone()).collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => schema,
        };

        Ok(Self {
            table_name,
            schema: projected_schema,
            projection,
            partitioned_work,
            batch_size,
        })
    }
}

#[async_trait]
impl PhysicalOperator for StreamingParquetScanExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        if partition >= self.partitioned_work.len() {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let work_items = self.partitioned_work[partition].clone();
        let projection = self.projection.clone();
        let batch_size = self.batch_size;
        let schema = self.schema.clone();

        // Create a stream that lazily reads row groups
        let stream = futures::stream::unfold(
            (
                work_items.into_iter().peekable(),
                projection,
                batch_size,
                schema,
                None::<parquet::arrow::arrow_reader::ParquetRecordBatchReader>,
            ),
            |(mut work_iter, projection, batch_size, schema, current_reader)| async move {
                // Try to get next batch from current reader
                if let Some(mut reader) = current_reader {
                    match reader.next() {
                        Some(Ok(batch)) => {
                            // Re-wrap with logical schema if needed
                            let result = if batch.schema() != schema
                                && batch.num_columns() == schema.fields().len()
                            {
                                RecordBatch::try_new(schema.clone(), batch.columns().to_vec())
                                    .map_err(|e| {
                                        QueryError::Execution(format!("Schema mismatch: {}", e))
                                    })
                            } else {
                                Ok(batch)
                            };
                            return Some((
                                result,
                                (work_iter, projection, batch_size, schema, Some(reader)),
                            ));
                        }
                        Some(Err(e)) => {
                            return Some((
                                Err(QueryError::Arrow(e)),
                                (work_iter, projection, batch_size, schema, None),
                            ));
                        }
                        None => {
                            // Reader exhausted, fall through to open next
                        }
                    }
                }

                // Open next row group
                let work = work_iter.next()?;
                let file = match File::open(&work.file_path) {
                    Ok(f) => f,
                    Err(e) => {
                        return Some((
                            Err(QueryError::Io(e)),
                            (work_iter, projection, batch_size, schema, None),
                        ))
                    }
                };
                let builder = match ParquetRecordBatchReaderBuilder::try_new(file) {
                    Ok(b) => b,
                    Err(e) => {
                        return Some((
                            Err(QueryError::Parquet(e)),
                            (work_iter, projection, batch_size, schema, None),
                        ))
                    }
                };

                let builder = builder
                    .with_batch_size(batch_size)
                    .with_row_groups(vec![work.row_group_idx]);

                let builder = if let Some(ref indices) = projection {
                    let mask = parquet::arrow::ProjectionMask::roots(
                        builder.parquet_schema(),
                        indices.iter().copied(),
                    );
                    builder.with_projection(mask)
                } else {
                    builder
                };

                let mut reader = match builder.build() {
                    Ok(r) => r,
                    Err(e) => {
                        return Some((
                            Err(QueryError::Parquet(e)),
                            (work_iter, projection, batch_size, schema, None),
                        ))
                    }
                };

                // Get first batch from this reader
                match reader.next() {
                    Some(Ok(batch)) => {
                        let result = if batch.schema() != schema
                            && batch.num_columns() == schema.fields().len()
                        {
                            RecordBatch::try_new(schema.clone(), batch.columns().to_vec()).map_err(
                                |e| QueryError::Execution(format!("Schema mismatch: {}", e)),
                            )
                        } else {
                            Ok(batch)
                        };
                        Some((
                            result,
                            (work_iter, projection, batch_size, schema, Some(reader)),
                        ))
                    }
                    Some(Err(e)) => Some((
                        Err(QueryError::Arrow(e)),
                        (work_iter, projection, batch_size, schema, None),
                    )),
                    None => {
                        // Empty row group, try next (recursive via unfold)
                        None
                    }
                }
            },
        );

        Ok(Box::pin(stream))
    }

    fn output_partitions(&self) -> usize {
        self.partitioned_work.len().max(1)
    }

    fn name(&self) -> &str {
        "StreamingParquetScan"
    }
}

impl fmt::Display for StreamingParquetScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total_rgs: usize = self.partitioned_work.iter().map(|p| p.len()).sum();
        write!(
            f,
            "StreamingParquetScan: {} ({} row groups, {} partitions)",
            self.table_name,
            total_rgs,
            self.partitioned_work.len()
        )?;
        if let Some(ref proj) = self.projection {
            write!(f, " projection={:?}", proj)?;
        }
        Ok(())
    }
}
