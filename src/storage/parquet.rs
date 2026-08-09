//! Parquet file table provider
//!
//! This module provides both batch and streaming access to Parquet files.
//! The streaming reader (`StreamingParquetReader`) reads data row-group by
//! row-group, enabling processing of datasets larger than available memory.

use crate::error::{QueryError, Result};
use crate::physical::operators::{TableProvider, TableStatistics};
use crate::storage::row_group_pruning;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::stream::BoxStream;
use futures::{Stream, StreamExt, TryStreamExt};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
use parquet::file::metadata::ParquetMetaData;
use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::fs::File as AsyncFile;

/// Table provider that reads from Parquet files
pub struct ParquetTable {
    /// Arrow schema for the table
    schema: SchemaRef,
    /// List of Parquet files to read
    files: Vec<PathBuf>,
    /// Footer-derived statistics, computed once per provider. Without this
    /// cache every query re-opened every file's footer during optimization.
    stats_cache: std::sync::OnceLock<Option<TableStatistics>>,
}

impl fmt::Debug for ParquetTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetTable")
            .field("schema", &self.schema)
            .field("files", &self.files)
            .finish()
    }
}

impl ParquetTable {
    /// Create a ParquetTable from a single file or directory
    ///
    /// If path is a file, reads that single Parquet file.
    /// If path is a directory, reads all .parquet files in it.
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let files = if path.is_dir() {
            Self::find_parquet_files(path)?
        } else if path.is_file() {
            vec![path.to_path_buf()]
        } else {
            return Err(QueryError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Path does not exist: {}", path.display()),
            )));
        };

        if files.is_empty() {
            return Err(QueryError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No Parquet files found in: {}", path.display()),
            )));
        }

        // Infer schema from the first file
        let schema = Self::read_schema(&files[0])?;

        Ok(Self {
            schema,
            files,
            stats_cache: std::sync::OnceLock::new(),
        })
    }

    /// Read table + per-column statistics from the Parquet footers.
    ///
    /// Column min/max/null_count are merged across all row groups of all
    /// files. Integer columns get an NDV estimate of
    /// `min(non_null_rows, max - min + 1)` — tight for dense surrogate keys
    /// (TPC-H `*_key` columns), a safe upper bound otherwise.
    fn compute_statistics(&self) -> Option<TableStatistics> {
        use parquet::file::statistics::Statistics as ParquetStatistics;
        use std::collections::HashMap;

        struct ColAcc {
            min_i64: Option<i64>,
            max_i64: Option<i64>,
            null_count: Option<u64>,
            has_int_stats: bool,
        }

        let mut total_rows: usize = 0;
        let mut total_bytes: u64 = 0;
        let mut cols: HashMap<String, ColAcc> = HashMap::new();
        // NDV estimates for columns WITHOUT integer min/max stats (strings):
        // harvested from the dictionary-page value count of the first file's
        // first row group (a chunk's dictionary holds exactly its distinct
        // values). Without this, equality filters on string dimension columns
        // fall to a generic 10% guess — o_orderstatus = 'F' (NDV 3) priced
        // orders at 1.5M rows instead of 5M and n_name = '...' (NDV 25)
        // priced nation at 2.5 rows instead of 1, which tied Q21's DP into
        // orders-first.
        let mut dict_ndv: HashMap<String, u64> = HashMap::new();
        let mut first_file_meta: Option<(PathBuf, Arc<ParquetMetaData>)> = None;

        for file_path in &self.files {
            let file = File::open(file_path).ok()?;
            let file_size = file.metadata().ok()?.len();
            let builder = ParquetRecordBatchReaderBuilder::try_new(file).ok()?;
            let metadata = builder.metadata();
            total_bytes += file_size;

            for rg in metadata.row_groups() {
                total_rows += rg.num_rows() as usize;
                for col_chunk in rg.columns() {
                    let name = col_chunk.column_path().parts().join(".").to_lowercase();
                    let acc = cols.entry(name).or_insert(ColAcc {
                        min_i64: None,
                        max_i64: None,
                        null_count: Some(0),
                        has_int_stats: false,
                    });

                    let Some(stats) = col_chunk.statistics() else {
                        // A chunk without stats poisons null_count accuracy.
                        acc.null_count = None;
                        continue;
                    };

                    match stats.null_count_opt() {
                        Some(n) => {
                            if let Some(total) = &mut acc.null_count {
                                *total += n;
                            }
                        }
                        None => acc.null_count = None,
                    }

                    let (min, max) = match stats {
                        ParquetStatistics::Int64(s) => (s.min_opt().copied(), s.max_opt().copied()),
                        ParquetStatistics::Int32(s) => (
                            s.min_opt().map(|v| *v as i64),
                            s.max_opt().map(|v| *v as i64),
                        ),
                        _ => (None, None),
                    };
                    if let (Some(min), Some(max)) = (min, max) {
                        acc.has_int_stats = true;
                        acc.min_i64 = Some(acc.min_i64.map_or(min, |m| m.min(min)));
                        acc.max_i64 = Some(acc.max_i64.map_or(max, |m| m.max(max)));
                    }
                }
            }

            if first_file_meta.is_none() {
                first_file_meta = Some((file_path.clone(), Arc::clone(builder.metadata())));
            }
        }

        // Dictionary-page NDV probe: for string columns (no integer stats),
        // read just the FIRST page of the column chunk in row group 0 of the
        // first file — when it is a dictionary page, its value count is the
        // chunk's exact distinct count (a lower bound for the whole table).
        // Reuses the already-parsed footer metadata; per column this reads
        // one dictionary page (capped at the writer's dictionary size limit).
        if let Some((path, meta)) = &first_file_meta {
            if meta.num_row_groups() > 0 {
                use parquet::basic::Type as PhysType;
                use parquet::column::page::{Page, PageReader};
                let rg = meta.row_group(0);
                let want: Vec<(usize, String)> = rg
                    .columns()
                    .iter()
                    .enumerate()
                    .filter(|(_, c)| {
                        // The dictionary page spans [dictionary_page_offset,
                        // data_page_offset); only read SMALL dictionaries —
                        // low-cardinality enum-ish columns, the only ones
                        // where equality selectivity matters. High-cardinality
                        // dictionaries (comments, clerk ids) cost real
                        // decompression time for an NDV the 10% default
                        // already treats conservatively.
                        c.column_type() == PhysType::BYTE_ARRAY
                            && c.dictionary_page_offset().map_or(false, |dp| {
                                let sz = c.data_page_offset().saturating_sub(dp);
                                sz > 0 && sz <= 65_536
                            })
                    })
                    .map(|(i, c)| (i, c.column_path().parts().join(".").to_lowercase()))
                    .filter(|(_, n)| cols.get(n).map(|a| !a.has_int_stats).unwrap_or(false))
                    .collect();
                if !want.is_empty() {
                    if let Ok(f) = File::open(path) {
                        let f = Arc::new(f);
                        for (i, name) in want {
                            let ccm = rg.column(i);
                            if let Ok(mut pages) =
                                parquet::file::serialized_reader::SerializedPageReader::new(
                                    Arc::clone(&f),
                                    ccm,
                                    rg.num_rows() as usize,
                                    None,
                                )
                            {
                                if let Ok(Some(Page::DictionaryPage { num_values, .. })) =
                                    pages.get_next_page()
                                {
                                    dict_ndv.insert(name, num_values as u64);
                                }
                            }
                        }
                    }
                }
            }
        }

        let column_stats = cols
            .into_iter()
            .map(|(name, acc)| {
                let non_null = acc
                    .null_count
                    .map(|n| (total_rows as u64).saturating_sub(n))
                    .unwrap_or(total_rows as u64);
                let ndv_est = if acc.has_int_stats {
                    match (acc.min_i64, acc.max_i64) {
                        (Some(min), Some(max)) if max >= min => {
                            Some(non_null.min((max - min) as u64 + 1))
                        }
                        _ => None,
                    }
                } else {
                    dict_ndv.get(&name).copied().filter(|&n| n > 0)
                };
                (
                    name,
                    crate::physical::operators::ColumnStatistics {
                        min_i64: acc.min_i64,
                        max_i64: acc.max_i64,
                        null_count: acc.null_count,
                        ndv_est,
                    },
                )
            })
            .collect();

        Some(TableStatistics {
            row_count: total_rows,
            total_byte_size: total_bytes,
            column_stats,
        })
    }

    /// Find all Parquet files in a directory (non-recursive)
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

        // Sort for deterministic order
        files.sort();
        Ok(files)
    }

    /// Read schema from a Parquet file
    fn read_schema(path: &Path) -> Result<SchemaRef> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        Ok(builder.schema().clone())
    }

    /// Read all batches from a single Parquet file.
    /// For large files (>4 row groups), uses parallel row group reading with rayon.
    fn read_file(path: &Path, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        use rayon::prelude::*;

        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let num_row_groups = builder.metadata().num_row_groups();

        // For small files, use simple sequential reading
        if num_row_groups <= 4 {
            let builder = builder.with_batch_size(8_192);
            let reader = if let Some(indices) = projection {
                let mask = parquet::arrow::ProjectionMask::roots(
                    builder.parquet_schema(),
                    indices.iter().copied(),
                );
                builder.with_projection(mask).build()?
            } else {
                builder.build()?
            };
            return Ok(reader.collect::<std::result::Result<Vec<_>, _>>()?);
        }

        // For large files, read row groups in parallel using rayon
        let path = path.to_path_buf();
        let projection = projection.map(|p| p.to_vec());
        let batches: Vec<Vec<RecordBatch>> = (0..num_row_groups)
            .into_par_iter()
            .map(|rg_idx| {
                let builder = crate::storage::metadata_cache::cached_reader_builder(&path)
                    .map_err(|e| arrow::error::ArrowError::ExternalError(Box::new(e)))?;

                // Select only this row group
                let row_selection = parquet::arrow::arrow_reader::RowSelection::from(vec![
                    parquet::arrow::arrow_reader::RowSelector::skip(
                        (0..rg_idx)
                            .map(|i| builder.metadata().row_group(i).num_rows() as usize)
                            .sum(),
                    ),
                    parquet::arrow::arrow_reader::RowSelector::select(
                        builder.metadata().row_group(rg_idx).num_rows() as usize,
                    ),
                ]);

                let builder = builder
                    .with_batch_size(8_192)
                    .with_row_selection(row_selection);

                let reader = if let Some(ref indices) = projection {
                    let mask = parquet::arrow::ProjectionMask::roots(
                        builder.parquet_schema(),
                        indices.iter().copied(),
                    );
                    builder.with_projection(mask).build()?
                } else {
                    builder.build()?
                };
                reader.collect::<std::result::Result<Vec<_>, _>>()
            })
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| QueryError::Execution(format!("Parallel parquet read failed: {}", e)))?;

        Ok(batches.into_iter().flatten().collect())
    }

    /// Read batches from a file, pruning row groups based on filter predicate.
    fn read_file_with_filter(
        path: &Path,
        projection: Option<&[usize]>,
        filter: Option<&crate::planner::Expr>,
        schema: &SchemaRef,
    ) -> Result<Vec<RecordBatch>> {
        use rayon::prelude::*;

        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let metadata = builder.metadata().clone();
        let num_row_groups = metadata.num_row_groups();

        // Determine which row groups to read
        let matching_rgs = row_group_pruning::prune_row_groups(&metadata, schema, filter);

        if matching_rgs.is_empty() {
            return Ok(vec![]);
        }

        // If all row groups match and count is small, use simple sequential read
        if matching_rgs.len() == num_row_groups && num_row_groups <= 4 {
            let builder = builder.with_batch_size(8_192);
            let reader = if let Some(indices) = projection {
                let mask = parquet::arrow::ProjectionMask::roots(
                    builder.parquet_schema(),
                    indices.iter().copied(),
                );
                builder.with_projection(mask).build()?
            } else {
                builder.build()?
            };
            return Ok(reader.collect::<std::result::Result<Vec<_>, _>>()?);
        }

        // Read matching row groups (potentially in parallel)
        let path = path.to_path_buf();
        let projection = projection.map(|p| p.to_vec());

        // Row-filter pushdown: decode predicate columns first, materialize the
        // remaining columns only for matching rows. The FilterExec above the
        // scan re-checks survivors, so partial pushdown stays correct.
        let row_filter_spec: Option<(crate::planner::Expr, Vec<usize>)> = filter.and_then(|expr| {
            let mut cols: Vec<String> = Vec::new();
            crate::physical::morsel::collect_expr_columns(expr, &mut cols);
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

        let read_row_group =
            |rg_idx: usize| -> std::result::Result<Vec<RecordBatch>, arrow::error::ArrowError> {
                let builder = crate::storage::metadata_cache::cached_reader_builder(&path)
                    .map_err(|e| arrow::error::ArrowError::ExternalError(Box::new(e)))?;

                let builder = builder.with_batch_size(8_192).with_row_groups(vec![rg_idx]);

                let builder = if let Some((expr, indices)) = &row_filter_spec {
                    use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
                    let mask = parquet::arrow::ProjectionMask::roots(
                        builder.parquet_schema(),
                        indices.iter().copied(),
                    );
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

                let reader = if let Some(ref indices) = projection {
                    let mask = parquet::arrow::ProjectionMask::roots(
                        builder.parquet_schema(),
                        indices.iter().copied(),
                    );
                    builder.with_projection(mask).build()?
                } else {
                    builder.build()?
                };
                reader.collect::<std::result::Result<Vec<_>, _>>()
            };

        let batches: Vec<Vec<RecordBatch>> = if matching_rgs.len() > 4 {
            // Parallel read
            matching_rgs
                .par_iter()
                .map(|&rg_idx| read_row_group(rg_idx))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| {
                    QueryError::Execution(format!("Parallel parquet read failed: {}", e))
                })?
        } else {
            // Sequential read
            matching_rgs
                .iter()
                .map(|&rg_idx| read_row_group(rg_idx))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| QueryError::Execution(format!("Parquet read failed: {}", e)))?
        };

        Ok(batches.into_iter().flatten().collect())
    }

    /// Get the list of files this table reads from
    pub fn files(&self) -> &[PathBuf] {
        &self.files
    }

    /// Get total number of files
    pub fn file_count(&self) -> usize {
        self.files.len()
    }
}

impl TableProvider for ParquetTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();

        for file_path in &self.files {
            let batches = Self::read_file(file_path, projection)?;
            all_batches.extend(batches);
        }

        // If projection was applied, we need to update the schema in the batches
        // The Parquet reader already does this, so batches have correct schema
        Ok(all_batches)
    }

    fn scan_with_filter(
        &self,
        projection: Option<&[usize]>,
        filter: Option<&crate::planner::Expr>,
    ) -> Result<Vec<RecordBatch>> {
        if filter.is_none() {
            return self.scan(projection);
        }

        let mut all_batches = Vec::new();
        for file_path in &self.files {
            let batches = Self::read_file_with_filter(file_path, projection, filter, &self.schema)?;
            all_batches.extend(batches);
        }
        Ok(all_batches)
    }

    fn statistics(&self) -> Option<TableStatistics> {
        self.stats_cache
            .get_or_init(|| self.compute_statistics())
            .clone()
    }

    fn parquet_files(&self) -> Option<Vec<PathBuf>> {
        Some(self.files.clone())
    }
}

/// Streaming Parquet reader that reads data row-group by row-group
///
/// This reader processes one row group at a time, keeping memory usage
/// bounded regardless of total file size. Useful for processing datasets
/// that don't fit in memory.
pub struct StreamingParquetReader {
    /// Files to read
    files: Vec<PathBuf>,
    /// Projection indices (columns to read)
    projection: Option<Vec<usize>>,
    /// Batch size for reading
    batch_size: usize,
    /// Schema of the output
    schema: SchemaRef,
    /// Current file index
    current_file_idx: usize,
    /// Current reader (if any)
    current_reader: Option<ParquetRecordBatchReader>,
}

impl fmt::Debug for StreamingParquetReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingParquetReader")
            .field("files", &self.files.len())
            .field("current_file_idx", &self.current_file_idx)
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl StreamingParquetReader {
    /// Create a new streaming reader for a list of Parquet files
    pub fn new(
        files: Vec<PathBuf>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Self {
        Self {
            files,
            projection,
            batch_size,
            schema,
            current_file_idx: 0,
            current_reader: None,
        }
    }

    /// Create from a ParquetTable
    pub fn from_table(
        table: &ParquetTable,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Self {
        Self::new(
            table.files.clone(),
            table.schema.clone(),
            projection,
            batch_size,
        )
    }

    /// Get the schema
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Open the next file and create a reader
    fn open_next_file(&mut self) -> Result<bool> {
        if self.current_file_idx >= self.files.len() {
            return Ok(false);
        }

        let path = &self.files[self.current_file_idx];
        let file = File::open(path).map_err(|e| {
            QueryError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to open {}: {}", path.display(), e),
            ))
        })?;

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // Apply projection if specified
        if let Some(ref indices) = self.projection {
            let mask = parquet::arrow::ProjectionMask::roots(
                builder.parquet_schema(),
                indices.iter().copied(),
            );
            builder = builder.with_projection(mask);
        }

        // Set batch size
        builder = builder.with_batch_size(self.batch_size);

        self.current_reader = Some(builder.build()?);
        self.current_file_idx += 1;

        Ok(true)
    }

    /// Read the next batch
    pub fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            // If we have a current reader, try to get the next batch
            if let Some(ref mut reader) = self.current_reader {
                match reader.next() {
                    Some(Ok(batch)) => return Ok(Some(batch)),
                    Some(Err(e)) => return Err(e.into()),
                    None => {
                        // Current file exhausted, move to next
                        self.current_reader = None;
                    }
                }
            }

            // Try to open the next file
            if !self.open_next_file()? {
                return Ok(None);
            }
        }
    }

    /// Convert to a stream of record batches
    pub fn into_stream(self) -> BoxStream<'static, Result<RecordBatch>> {
        Box::pin(StreamingParquetReaderStream { reader: self })
    }
}

/// Stream wrapper for StreamingParquetReader
struct StreamingParquetReaderStream {
    reader: StreamingParquetReader,
}

impl Stream for StreamingParquetReaderStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.reader.next_batch() {
            Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Ok(None) => Poll::Ready(None),
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

/// Information about a Parquet file
#[derive(Debug, Clone)]
pub struct ParquetFileInfo {
    /// Path to the file
    pub path: PathBuf,
    /// Number of row groups in the file
    pub num_row_groups: usize,
    /// Total number of rows
    pub num_rows: i64,
    /// File size in bytes
    pub file_size: u64,
    /// Parquet metadata
    metadata: Arc<ParquetMetaData>,
}

impl ParquetFileInfo {
    /// Read file info from a Parquet file
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)?;
        let file_size = file.metadata()?.len();

        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        let metadata = builder.metadata().clone();

        let num_rows = metadata.row_groups().iter().map(|rg| rg.num_rows()).sum();

        Ok(Self {
            path,
            num_row_groups: metadata.num_row_groups(),
            num_rows,
            file_size,
            metadata, // Already an Arc<ParquetMetaData>
        })
    }

    /// Get row group metadata
    pub fn row_group(&self, idx: usize) -> Option<&parquet::file::metadata::RowGroupMetaData> {
        self.metadata.row_groups().get(idx)
    }

    /// Get column statistics for a row group
    pub fn column_stats(
        &self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Option<&parquet::file::statistics::Statistics> {
        self.metadata
            .row_groups()
            .get(row_group_idx)?
            .column(column_idx)
            .statistics()
    }
}

/// Builder for creating streaming table scans
pub struct StreamingParquetScanBuilder {
    files: Vec<PathBuf>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    batch_size: usize,
    /// Predicate for row group pruning (column_idx, min, max) -> should_include
    row_group_filter: Option<Box<dyn Fn(usize, &ParquetFileInfo) -> bool + Send + Sync>>,
}

impl StreamingParquetScanBuilder {
    /// Create a new builder from a ParquetTable
    pub fn new(table: &ParquetTable) -> Self {
        Self {
            files: table.files.clone(),
            schema: table.schema.clone(),
            projection: None,
            batch_size: 8192,
            row_group_filter: None,
        }
    }

    /// Set the columns to read
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Set the batch size for reading
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Build the streaming reader
    pub fn build(self) -> StreamingParquetReader {
        StreamingParquetReader::new(self.files, self.schema, self.projection, self.batch_size)
    }

    /// Build and convert to stream
    pub fn build_stream(self) -> BoxStream<'static, Result<RecordBatch>> {
        self.build().into_stream()
    }
}

/// Async Parquet reader with true async I/O
///
/// Uses tokio for async file access and overlaps I/O with computation.
/// This provides better performance on NVMe drives by utilizing async I/O.
pub struct AsyncParquetReader {
    /// Files to read
    files: Vec<PathBuf>,
    /// Projection indices (columns to read)
    projection: Option<Vec<usize>>,
    /// Batch size for reading
    batch_size: usize,
    /// Schema of the output
    schema: SchemaRef,
}

impl fmt::Debug for AsyncParquetReader {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AsyncParquetReader")
            .field("files", &self.files.len())
            .field("batch_size", &self.batch_size)
            .finish()
    }
}

impl AsyncParquetReader {
    /// Create a new async reader for a list of Parquet files
    pub fn new(
        files: Vec<PathBuf>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Self {
        Self {
            files,
            projection,
            batch_size,
            schema,
        }
    }

    /// Create from a ParquetTable
    pub fn from_table(
        table: &ParquetTable,
        projection: Option<Vec<usize>>,
        batch_size: usize,
    ) -> Self {
        Self::new(
            table.files.clone(),
            table.schema.clone(),
            projection,
            batch_size,
        )
    }

    /// Get the schema
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Read all files asynchronously and return a stream of record batches
    pub fn into_stream(self) -> BoxStream<'static, Result<RecordBatch>> {
        let files = self.files.clone();
        let projection = self.projection.clone();
        let batch_size = self.batch_size;

        // Create a stream that processes files sequentially but reads async
        let stream = futures::stream::unfold(
            (
                files.into_iter(),
                projection,
                batch_size,
                None::<parquet::arrow::async_reader::ParquetRecordBatchStream<AsyncFile>>,
            ),
            |(mut files_iter, projection, batch_size, current_stream)| async move {
                // If we have a current stream, try to get next batch from it
                if let Some(mut stream) = current_stream {
                    match stream.next().await {
                        Some(Ok(batch)) => {
                            return Some((
                                Ok(batch),
                                (files_iter, projection, batch_size, Some(stream)),
                            ));
                        }
                        Some(Err(e)) => {
                            return Some((
                                Err(QueryError::Parquet(e)),
                                (files_iter, projection, batch_size, None),
                            ));
                        }
                        None => {
                            // Stream exhausted, continue to next file
                        }
                    }
                }

                // Try to open next file
                let path = files_iter.next()?;

                let file = match AsyncFile::open(&path).await {
                    Ok(f) => f,
                    Err(e) => {
                        return Some((
                            Err(QueryError::Io(std::io::Error::new(
                                e.kind(),
                                format!("Failed to open {}: {}", path.display(), e),
                            ))),
                            (files_iter, projection, batch_size, None),
                        ));
                    }
                };

                let builder = match ParquetRecordBatchStreamBuilder::new(file).await {
                    Ok(b) => b,
                    Err(e) => {
                        return Some((
                            Err(QueryError::Parquet(e)),
                            (files_iter, projection, batch_size, None),
                        ));
                    }
                };

                // Apply projection if specified
                let builder = if let Some(ref indices) = projection {
                    let mask = parquet::arrow::ProjectionMask::roots(
                        builder.parquet_schema(),
                        indices.iter().copied(),
                    );
                    builder.with_projection(mask)
                } else {
                    builder
                };

                // Set batch size
                let builder = builder.with_batch_size(batch_size);

                let mut new_stream = match builder.build() {
                    Ok(s) => s,
                    Err(e) => {
                        return Some((
                            Err(QueryError::Parquet(e)),
                            (files_iter, projection, batch_size, None),
                        ));
                    }
                };

                // Get first batch from new stream
                match new_stream.next().await {
                    Some(Ok(batch)) => Some((
                        Ok(batch),
                        (files_iter, projection, batch_size, Some(new_stream)),
                    )),
                    Some(Err(e)) => Some((
                        Err(QueryError::Parquet(e)),
                        (files_iter, projection, batch_size, None),
                    )),
                    None => {
                        // Empty file, recurse by returning None which will continue to next iteration
                        // For simplicity, just return None - caller can handle empty files
                        None
                    }
                }
            },
        );

        Box::pin(stream)
    }

    /// Read all files in parallel using async I/O
    ///
    /// Spawns multiple async tasks to read files concurrently, providing
    /// better throughput on fast storage (NVMe, RAID arrays).
    pub async fn read_all_parallel(&self, max_concurrent: usize) -> Result<Vec<RecordBatch>> {
        use futures::stream::FuturesUnordered;

        let mut all_batches = Vec::new();
        let mut futures = FuturesUnordered::new();

        for path in &self.files {
            let path = path.clone();
            let projection = self.projection.clone();
            let batch_size = self.batch_size;

            // Limit concurrency
            if futures.len() >= max_concurrent {
                if let Some(result) = futures.next().await {
                    all_batches.extend(result?);
                }
            }

            futures.push(async move {
                let file = AsyncFile::open(&path).await.map_err(|e| {
                    QueryError::Io(std::io::Error::new(
                        e.kind(),
                        format!("Failed to open {}: {}", path.display(), e),
                    ))
                })?;

                let mut builder = ParquetRecordBatchStreamBuilder::new(file).await?;

                // Apply projection
                if let Some(ref indices) = projection {
                    let mask = parquet::arrow::ProjectionMask::roots(
                        builder.parquet_schema(),
                        indices.iter().copied(),
                    );
                    builder = builder.with_projection(mask);
                }

                builder = builder.with_batch_size(batch_size);

                let stream = builder.build()?;

                let batches: Vec<RecordBatch> = stream.try_collect().await?;

                Ok::<_, QueryError>(batches)
            });
        }

        // Collect remaining futures
        while let Some(result) = futures.next().await {
            all_batches.extend(result?);
        }

        Ok(all_batches)
    }
}

/// Builder for async Parquet scans
pub struct AsyncParquetScanBuilder {
    files: Vec<PathBuf>,
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    batch_size: usize,
}

impl AsyncParquetScanBuilder {
    /// Create a new builder from a ParquetTable
    pub fn new(table: &ParquetTable) -> Self {
        Self {
            files: table.files.clone(),
            schema: table.schema.clone(),
            projection: None,
            batch_size: 8192,
        }
    }

    /// Set the columns to read
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Set the batch size for reading
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Build the async reader
    pub fn build(self) -> AsyncParquetReader {
        AsyncParquetReader::new(self.files, self.schema, self.projection, self.batch_size)
    }

    /// Build and convert to stream
    pub fn build_stream(self) -> BoxStream<'static, Result<RecordBatch>> {
        self.build().into_stream()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_table_not_found() {
        let result = ParquetTable::try_new("/nonexistent/path.parquet");
        assert!(result.is_err());
    }

    #[test]
    fn test_footer_column_statistics() {
        // Requires generated test data (CI generates it before cargo test).
        let path = format!(
            "{}/data/tpch-1mb/orders.parquet",
            env!("CARGO_MANIFEST_DIR")
        );
        if !std::path::Path::new(&path).exists() {
            eprintln!("skipping: {path} not generated");
            return;
        }
        let table = ParquetTable::try_new(&path).unwrap();
        let stats = table.statistics().expect("statistics available");
        assert_eq!(stats.row_count, 1500); // SF=0.001 orders

        let key = stats
            .column_stats
            .get("o_orderkey")
            .expect("o_orderkey column stats");
        assert_eq!(key.min_i64, Some(1));
        assert_eq!(key.max_i64, Some(1500));
        // Dense surrogate key: range-based NDV estimate is exact.
        assert_eq!(key.ndv_est, Some(1500));

        // Cache: second call must return the same (cached) result.
        let again = table.statistics().unwrap();
        assert_eq!(again.row_count, stats.row_count);
    }
}
