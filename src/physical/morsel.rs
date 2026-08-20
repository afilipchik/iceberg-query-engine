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
    /// Zone maps prove the scan filter true for every row in this group.
    pub filter_all_true: bool,
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
    /// Per-file IPC sidecar dirs (index = file_idx) when the cache is on.
    ipc_dirs: Vec<Option<std::path::PathBuf>>,
    /// Pre-built schema override requesting Dictionary(Int32, Utf8) for
    /// chosen string columns (constructed ONCE from the provider schema —
    /// a per-row-group probe re-parsed the footer and cost more than the
    /// dictionary reads saved).
    dict_schema: Option<SchemaRef>,
    /// Filter to push into the parquet decoder as an arrow RowFilter.
    /// Some((expr, column indices in the FULL file schema)) when eligible.
    row_filter: Option<(crate::planner::Expr, Vec<usize>)>,
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
        Self::try_new_with_filter(files, schema, projection, batch_size, None)
    }

    /// Create a new parallel Parquet source with optional filter for row group pruning
    pub fn try_new_with_filter(
        files: Vec<PathBuf>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
        filter: Option<&crate::planner::Expr>,
    ) -> Result<Self> {
        // Discover all row groups, pruning based on filter
        let mut work_queue = VecDeque::new();

        let ipc_dirs: Vec<Option<std::path::PathBuf>> = if crate::storage::ipc_cache::enabled() {
            files
                .iter()
                .map(|f| crate::storage::ipc_cache::ensure_sidecar(f))
                .collect()
        } else {
            vec![None; files.len()]
        };
        for (file_idx, file_path) in files.iter().enumerate() {
            let md = crate::storage::metadata_cache::cached_metadata(file_path)?;
            let metadata = md.metadata().clone();

            let matching_rgs =
                crate::storage::row_group_pruning::prune_row_groups(&metadata, &schema, filter);

            for row_group_idx in matching_rgs {
                // Zone-map proof: if min/max show every row passes, the
                // decoder RowFilter is dropped for this row group (single-
                // phase decode, no per-row predicate evaluation).
                let all_true = filter
                    .map(|f| {
                        crate::storage::row_group_pruning::row_group_definitely_matches(
                            f,
                            metadata.row_group(row_group_idx),
                            &schema,
                        )
                    })
                    .unwrap_or(false);
                work_queue.push_back(RowGroupWork {
                    file_path: file_path.clone(),
                    row_group_idx,
                    file_idx,
                    filter_all_true: all_true,
                });
            }
        }

        let total_row_groups = work_queue.len();

        // Decide row-filter pushdown: decode-time filtering skips materializing
        // non-matching rows in every other column. Eligible when every column
        // referenced by the filter exists in the file schema.
        let row_filter = filter.and_then(|expr| {
            let mut cols: Vec<String> = Vec::new();
            collect_expr_columns(expr, &mut cols);
            if cols.is_empty() {
                return None;
            }
            let mut indices = Vec::with_capacity(cols.len());
            for c in &cols {
                let idx = schema.fields().iter().position(|f| f.name() == c)?;
                if !indices.contains(&idx) {
                    indices.push(idx);
                }
            }
            Some((expr.clone(), indices))
        });

        Ok(Self {
            schema,
            projection,
            batch_size,
            work_queue: Mutex::new(work_queue),
            completed: AtomicUsize::new(0),
            total_row_groups,
            row_filter,
            ipc_dirs,
            dict_schema: None,
        })
    }

    /// Replace the output projection. The work queue, the row-group pruning
    /// and the RowFilter mask are all derived from the FILE schema and the
    /// filter, never from the output projection, so this is safe to call
    /// after construction — callers use it to drop filter-only columns once
    /// they know the filter is served by the decoder.
    pub fn set_projection(&mut self, projection: Option<Vec<usize>>) {
        self.projection = projection;
    }

    /// True when the filter is applied inside the parquet decoder — callers
    /// must not re-apply it.
    /// Request dictionary reads for the given file-schema column indices.
    pub fn with_dict_strings(mut self, cols: Vec<usize>) -> Self {
        if cols.is_empty() {
            return self;
        }
        let fields: Vec<arrow::datatypes::Field> = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if cols.contains(&i) && f.data_type() == &arrow::datatypes::DataType::Utf8 {
                    arrow::datatypes::Field::new(
                        f.name(),
                        arrow::datatypes::DataType::Dictionary(
                            Box::new(arrow::datatypes::DataType::Int32),
                            Box::new(arrow::datatypes::DataType::Utf8),
                        ),
                        f.is_nullable(),
                    )
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        self.dict_schema = Some(std::sync::Arc::new(arrow::datatypes::Schema::new(fields)));
        self
    }

    pub fn filter_pushed_down(&self) -> bool {
        self.row_filter.is_some()
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

    /// Row groups this source will hand out, i.e. the number of indivisible
    /// units of work available. Row-group pruning has already been applied, so
    /// a selective `WHERE` can leave far fewer than the file contains.
    ///
    /// Fan-out sites use this to size their worker count: spawning 32 workers
    /// for 3 row groups allocates 29 thread-local aggregation states that never
    /// see a row, then shards the merge across all of them.
    pub fn total_work(&self) -> usize {
        self.total_row_groups
    }

    /// Read a single row group and return batches
    pub fn read_row_group(&self, work: &RowGroupWork) -> Result<Vec<RecordBatch>> {
        // IPC sidecar: decode-free read of the row group; the scan filter
        // (if any) applies vectorized post-load unless zone maps proved it
        // ALWAYS_TRUE. v2 sidecars store low-cardinality strings
        // dictionary-encoded, so a dict-coercion scan takes the IPC path
        // whenever every column it wants coerced is stored dict; only a
        // request the sidecar cannot serve falls back to parquet.
        let ipc_serves = match self.ipc_dirs.get(work.file_idx) {
            Some(Some(dir)) => match &self.dict_schema {
                None => true,
                Some(ds) => {
                    let stored = crate::storage::ipc_cache::sidecar_dict_cols(dir);
                    ds.fields()
                        .iter()
                        .filter(|f| {
                            matches!(f.data_type(), arrow::datatypes::DataType::Dictionary(_, _))
                        })
                        .all(|f| stored.contains(&f.name().to_lowercase()))
                }
            },
            _ => false,
        };
        if ipc_serves {
            if let Some(Some(dir)) = self.ipc_dirs.get(work.file_idx) {
                let mut batches = crate::storage::ipc_cache::read_row_group(
                    dir,
                    work.row_group_idx,
                    self.projection.as_deref(),
                    None,
                )?;
                if let Some((expr, _)) = &self.row_filter {
                    if !work.filter_all_true {
                        batches = crate::physical::operators::filter_batches(batches, expr)?;
                    }
                }
                batches = crate::storage::ipc_cache::reslice_large(batches, 16384, 8192);
                return Ok(batches);
            }
        }
        let builder = match &self.dict_schema {
            Some(schema) => {
                let file = File::open(&work.file_path)?;
                let opts = parquet::arrow::arrow_reader::ArrowReaderOptions::new()
                    .with_schema(schema.clone());
                match parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new_with_options(
                    file, opts,
                ) {
                    Ok(b) => b,
                    Err(_) => {
                        let file = File::open(&work.file_path)?;
                        ParquetRecordBatchReaderBuilder::try_new(file)?
                    }
                }
            }
            None => crate::storage::metadata_cache::cached_reader_builder(&work.file_path)?,
        };

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

        // Push the filter into the decoder: the predicate columns are decoded
        // first and only matching rows are materialized for the rest.
        let builder = if work.filter_all_true {
            builder
        } else if let Some((expr, indices)) = &self.row_filter {
            use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
            let mask = ProjectionMask::roots(builder.parquet_schema(), indices.iter().copied());
            let expr = expr.clone();
            let pred = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
                let arr = crate::physical::operators::evaluate_expr(&batch, &expr)
                    .map_err(|e| arrow::error::ArrowError::ComputeError(e.to_string()))?;
                arr.as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .cloned()
                    .ok_or_else(|| {
                        arrow::error::ArrowError::ComputeError(
                            "row filter did not evaluate to boolean".into(),
                        )
                    })
            });
            builder.with_row_filter(RowFilter::new(vec![Box::new(pred)]))
        } else {
            builder
        };

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
        let num_threads = crate::execution::topology::workers_for(
            self.total_work(),
            rayon::current_num_threads(),
        );

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
        let num_threads = crate::execution::topology::workers_for(
            self.total_work(),
            rayon::current_num_threads(),
        );

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

/// Collect the (unqualified) column names referenced by an expression.
pub(crate) fn collect_expr_columns(expr: &crate::planner::Expr, out: &mut Vec<String>) {
    use crate::planner::Expr;
    match expr {
        Expr::Column(c) => out.push(c.name.clone()),
        Expr::BinaryExpr { left, right, .. } => {
            collect_expr_columns(left, out);
            collect_expr_columns(right, out);
        }
        Expr::UnaryExpr { expr, .. } => collect_expr_columns(expr, out),
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_columns(expr, out);
            collect_expr_columns(low, out);
            collect_expr_columns(high, out);
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_columns(expr, out);
            for e in list {
                collect_expr_columns(e, out);
            }
        }
        Expr::Cast { expr, .. } => collect_expr_columns(expr, out),
        Expr::ScalarFunc { args, .. } => {
            for a in args {
                collect_expr_columns(a, out);
            }
        }
        Expr::Alias { expr, .. } => collect_expr_columns(expr, out),
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(o) = operand {
                collect_expr_columns(o, out);
            }
            for (w, t) in when_then {
                collect_expr_columns(w, out);
                collect_expr_columns(t, out);
            }
            if let Some(e) = else_expr {
                collect_expr_columns(e, out);
            }
        }
        // Subqueries and other constructs: bail by reporting a column that
        // never resolves, disabling pushdown.
        Expr::ScalarSubquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
            out.push("__unpushable__".to_string())
        }
        _ => {}
    }
}
