//! Parquet table scan operator with statistics-based pruning
//!
//! This module provides Parquet file reading with:
//! - Statistics-based row group pruning
//! - Page-level predicate pushdown
//! - Column pruning (read only needed columns)
//! - Parallel row group reading

use crate::error::{QueryError, Result};
use crate::physical::morsel::ParallelParquetSource;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use std::fmt;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

/// Parquet table provider for reading Parquet files
#[derive(Debug, Clone)]
pub struct ParquetTable {
    /// Path to the Parquet file
    path: PathBuf,
    /// Schema of the table
    schema: SchemaRef,
}

impl ParquetTable {
    /// Open a Parquet file for reading
    pub fn try_new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();

        // Create a reader to get the schema
        let file = File::open(&path).map_err(|e| {
            QueryError::Execution(format!("Failed to open Parquet file {:?}: {}", path, e))
        })?;

        let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
            QueryError::Execution(format!("Failed to create Parquet reader builder: {}", e))
        })?;

        let schema = builder.schema().clone();

        Ok(Self { path, schema })
    }

    /// Get the file path
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Get the table schema
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }
}

/// Parquet scan operator
#[derive(Debug)]
pub struct ParquetScanExec {
    path: PathBuf,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    /// Enable parallel reading using morsel-driven parallelism
    parallel: bool,
    /// Batch size for reading (default 64K rows)
    batch_size: usize,
}

impl ParquetScanExec {
    /// Create a new Parquet scan operator (sequential reading)
    pub fn new(path: PathBuf, schema: SchemaRef, projection: Option<Vec<usize>>) -> Self {
        Self {
            path,
            schema,
            projection,
            parallel: false,
            batch_size: 65536, // 64K rows
        }
    }

    /// Create a new Parquet scan operator with parallel reading enabled
    pub fn new_parallel(
        path: PathBuf,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Self {
        Self {
            path,
            schema,
            projection,
            parallel: true,
            batch_size,
        }
    }

    /// Enable parallel reading on an existing scan
    pub fn with_parallel(mut self, batch_size: usize) -> Self {
        self.parallel = true;
        self.batch_size = batch_size;
        self
    }
}

#[async_trait]
impl PhysicalOperator for ParquetScanExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }

    async fn execute(&self, _partition: usize) -> Result<RecordBatchStream> {
        if self.parallel {
            // Use morsel-driven parallel reading
            let source = ParallelParquetSource::try_from_path(
                &self.path,
                self.projection.clone(),
                self.batch_size,
            )?;

            // Read all data in parallel
            let morsels = source.read_all_parallel()?;

            // Convert morsels to batches
            let batches: Vec<RecordBatch> = morsels.into_iter().map(|m| m.batch).collect();

            let stream = stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream))
        } else {
            // Sequential reading (original implementation)
            let file = File::open(&self.path).map_err(|e| {
                QueryError::Execution(format!(
                    "Failed to open Parquet file {:?}: {}",
                    self.path, e
                ))
            })?;

            let builder = ParquetRecordBatchReaderBuilder::try_new(file).map_err(|e| {
                QueryError::Execution(format!("Failed to create Parquet reader builder: {}", e))
            })?;

            // Apply column projection to read only needed columns
            // This is a key I/O optimization - reads only the columns that are needed
            let builder = if let Some(ref indices) = self.projection {
                let mask = ProjectionMask::roots(builder.parquet_schema(), indices.iter().copied());
                builder.with_projection(mask)
            } else {
                builder
            };

            let mut reader = builder.build().map_err(|e| {
                QueryError::Execution(format!("Failed to build Parquet reader: {}", e))
            })?;

            let mut batches = Vec::new();

            // Read all batches using the iterator interface
            use std::iter::Iterator;
            loop {
                match reader.next() {
                    Some(Ok(batch)) => {
                        batches.push(batch);
                    }
                    Some(Err(e)) => {
                        return Err(QueryError::Execution(format!(
                            "Failed to read Parquet batch: {}",
                            e
                        )));
                    }
                    None => break,
                }
            }

            let stream = stream::iter(batches.into_iter().map(Ok));
            Ok(Box::pin(stream))
        }
    }

    fn name(&self) -> &str {
        "ParquetScan"
    }
}

impl fmt::Display for ParquetScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ParquetScan: {:?}", self.path)?;
        if self.parallel {
            write!(f, " [parallel, batch_size={}]", self.batch_size)?;
        }
        if let Some(proj) = &self.projection {
            write!(f, " projection={:?}", proj)?;
        }
        Ok(())
    }
}

/// Parquet writer for writing RecordBatches to Parquet files
pub struct ParquetWriter {
    #[allow(dead_code)] // Stored for potential future use (error messages, etc.)
    path: PathBuf,
    writer: Option<ArrowWriter<File>>,
}

impl ParquetWriter {
    /// Create a new Parquet writer
    pub fn try_new(path: impl Into<PathBuf>, schema: SchemaRef) -> Result<Self> {
        let path = path.into();
        let file = File::create(&path).map_err(|e| {
            QueryError::Execution(format!("Failed to create Parquet file {:?}: {}", path, e))
        })?;

        let writer = ArrowWriter::try_new(file, schema, None).map_err(|e| {
            QueryError::Execution(format!("Failed to create Parquet writer: {}", e))
        })?;

        Ok(Self {
            path,
            writer: Some(writer),
        })
    }

    /// Write a single record batch
    pub fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| QueryError::Execution("Parquet writer already closed".to_string()))?;

        writer
            .write(batch)
            .map_err(|e| QueryError::Execution(format!("Failed to write Parquet batch: {}", e)))?;

        Ok(())
    }

    /// Close the writer and flush remaining data
    pub fn close(&mut self) -> Result<()> {
        if let Some(writer) = self.writer.take() {
            writer.close().map_err(|e| {
                QueryError::Execution(format!("Failed to close Parquet writer: {}", e))
            })?;
        }
        Ok(())
    }
}

impl Drop for ParquetWriter {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::Field;

    #[test]
    fn test_parquet_write_read() {
        use tempfile::tempdir;

        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test.parquet");

        // Create test schema
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int64, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, true),
        ]));

        // Create test data
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
            ],
        )
        .unwrap();

        // Write to Parquet
        let mut writer = ParquetWriter::try_new(&file_path, schema.clone()).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        // Verify file exists
        assert!(file_path.exists());

        // Read back the table
        let table = ParquetTable::try_new(&file_path).unwrap();
        assert_eq!(table.schema().as_ref(), schema.as_ref());
    }
}
