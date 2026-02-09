//! Morsel-driven parallel execution framework
//!
//! This module implements DuckDB-style morsel-driven parallelism where:
//! - Data is split into fixed-size morsels (~64K rows)
//! - Multiple threads process morsels concurrently
//! - Work-stealing ensures load balancing
//! - Pipeline breakers (joins, aggregations) use thread-local state with final merge

use crate::error::{QueryError, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ProjectionMask;
use rayon::prelude::*;
use std::collections::VecDeque;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Default morsel size (number of rows per morsel)
pub const DEFAULT_MORSEL_SIZE: usize = 8192; // 8K rows - fits L2 cache for vectorized ops

/// A morsel is a chunk of data that can be processed independently
#[derive(Debug)]
pub struct Morsel {
    /// The data batch
    pub batch: RecordBatch,
    /// Source identifier (file index, partition, etc.)
    pub source_id: usize,
    /// Morsel sequence number within source
    pub sequence: usize,
}

impl Morsel {
    pub fn new(batch: RecordBatch, source_id: usize, sequence: usize) -> Self {
        Self {
            batch,
            source_id,
            sequence,
        }
    }

    pub fn num_rows(&self) -> usize {
        self.batch.num_rows()
    }
}

/// Work unit representing a row group to be read
#[derive(Debug, Clone)]
pub struct RowGroupWork {
    pub file_path: PathBuf,
    pub row_group_idx: usize,
    pub file_idx: usize,
}

/// Parallel morsel source that reads Parquet files concurrently
///
/// This source:
/// 1. Discovers all row groups across all Parquet files
/// 2. Distributes row groups to worker threads
/// 3. Each worker reads and produces morsels independently
pub struct ParallelParquetSource {
    /// Schema of the output
    schema: SchemaRef,
    /// Projection indices (columns to read)
    projection: Option<Vec<usize>>,
    /// Batch size for reading
    batch_size: usize,
    /// Work queue of row groups to read
    work_queue: Mutex<VecDeque<RowGroupWork>>,
    /// Number of completed row groups
    completed: AtomicUsize,
    /// Total number of row groups
    total_row_groups: usize,
}

impl std::fmt::Debug for ParallelParquetSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParallelParquetSource")
            .field("batch_size", &self.batch_size)
            .field("total_row_groups", &self.total_row_groups)
            .finish()
    }
}

impl ParallelParquetSource {
    /// Create a new parallel Parquet source
    pub fn try_new(
        files: Vec<PathBuf>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        // Discover all row groups
        let mut work_queue = VecDeque::new();

        for (file_idx, file_path) in files.iter().enumerate() {
            let file = File::open(file_path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let metadata = builder.metadata();

            for row_group_idx in 0..metadata.num_row_groups() {
                work_queue.push_back(RowGroupWork {
                    file_path: file_path.clone(),
                    row_group_idx,
                    file_idx,
                });
            }
        }

        let total_row_groups = work_queue.len();

        Ok(Self {
            schema,
            projection,
            batch_size,
            work_queue: Mutex::new(work_queue),
            completed: AtomicUsize::new(0),
            total_row_groups,
        })
    }

    /// Create from a directory of Parquet files
    pub fn try_from_path(
        path: impl AsRef<Path>,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Result<Self> {
        let path = path.as_ref();
        let files = if path.is_dir() {
            Self::find_parquet_files(path)?
        } else {
            vec![path.to_path_buf()]
        };

        if files.is_empty() {
            return Err(QueryError::Execution(format!(
                "No Parquet files found: {}",
                path.display()
            )));
        }

        // Read schema from first file
        let file = File::open(&files[0])?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let schema = builder.schema().clone();

        Self::try_new(files, schema, projection, batch_size)
    }

    fn find_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "parquet" {
                        files.push(path);
                    }
                }
            }
        }
        files.sort();
        Ok(files)
    }

    /// Get the schema
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Get work from the queue
    pub fn get_work(&self) -> Option<RowGroupWork> {
        let mut queue = self.work_queue.lock().unwrap();
        queue.pop_front()
    }

    /// Mark work as completed
    pub fn complete_work(&self) {
        self.completed.fetch_add(1, Ordering::SeqCst);
    }

    /// Get progress
    pub fn progress(&self) -> (usize, usize) {
        (self.completed.load(Ordering::SeqCst), self.total_row_groups)
    }

    /// Read a single row group and return batches
    pub fn read_row_group(&self, work: &RowGroupWork) -> Result<Vec<RecordBatch>> {
        let file = File::open(&work.file_path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // Apply projection if specified
        let builder = if let Some(ref indices) = self.projection {
            let mask = ProjectionMask::roots(builder.parquet_schema(), indices.iter().copied());
            builder.with_projection(mask)
        } else {
            builder
        };

        // Select specific row group
        let builder = builder
            .with_row_groups(vec![work.row_group_idx])
            .with_batch_size(self.batch_size);

        let reader = builder.build()?;
        let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(batches)
    }

    /// Read all data in parallel and return all batches
    ///
    /// This is the main entry point for parallel reading.
    /// It spawns worker threads that each grab work from the queue
    /// and read row groups independently.
    pub fn read_all_parallel(&self) -> Result<Vec<Morsel>> {
        let num_threads = rayon::current_num_threads();

        // Use rayon to parallelize across row groups
        let results: Vec<Result<Vec<Morsel>>> = (0..num_threads)
            .into_par_iter()
            .map(|_thread_id| {
                let mut morsels = Vec::new();

                // Keep grabbing work until the queue is empty
                while let Some(work) = self.get_work() {
                    let batches = self.read_row_group(&work)?;

                    for (seq, batch) in batches.into_iter().enumerate() {
                        morsels.push(Morsel::new(batch, work.file_idx, seq));
                    }

                    self.complete_work();
                }

                Ok(morsels)
            })
            .collect();

        // Collect results from all threads
        let mut all_morsels = Vec::new();
        for result in results {
            all_morsels.extend(result?);
        }

        Ok(all_morsels)
    }

    /// Read all data in parallel and process each morsel with a function
    ///
    /// This is more memory-efficient as it doesn't collect all morsels.
    /// Instead, it processes each morsel immediately with the provided function.
    pub fn read_and_process<F, T>(&self, processor: F) -> Result<Vec<T>>
    where
        F: Fn(Morsel) -> Result<T> + Sync,
        T: Send,
    {
        let num_threads = rayon::current_num_threads();

        let results: Vec<Result<Vec<T>>> = (0..num_threads)
            .into_par_iter()
            .map(|_thread_id| {
                let mut outputs = Vec::new();

                while let Some(work) = self.get_work() {
                    let batches = self.read_row_group(&work)?;

                    for (seq, batch) in batches.into_iter().enumerate() {
                        let morsel = Morsel::new(batch, work.file_idx, seq);
                        let output = processor(morsel)?;
                        outputs.push(output);
                    }

                    self.complete_work();
                }

                Ok(outputs)
            })
            .collect();

        let mut all_outputs = Vec::new();
        for result in results {
            all_outputs.extend(result?);
        }

        Ok(all_outputs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_morsel_creation() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let morsel = Morsel::new(batch, 0, 0);
        assert_eq!(morsel.num_rows(), 3);
        assert_eq!(morsel.source_id, 0);
        assert_eq!(morsel.sequence, 0);
    }
}
