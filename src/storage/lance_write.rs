//! Writing Lance datasets from Rust.
//!
//! Reading a format is half of supporting it. Until this module existed the
//! only way to produce a Lance dataset for this engine was
//! `scripts/lance_convert.py`, which meant a Python interpreter, a matching
//! pylance build, and a conversion the engine itself could not perform or test.
//!
//! What is here:
//!
//! - [`write_batches`] — write in-memory Arrow batches (a query result) as a
//!   new dataset, an append, or an overwrite.
//! - [`write_from_parquet`] — convert Parquet to Lance without materializing
//!   either side.
//! - [`create_vector_index`] — build the IVF_PQ index the k-NN pushdown uses,
//!   closing the loop: write vectors, index them, search them, no Python.
//!
//! # Versioning is the observable effect
//!
//! Every one of these commits a new manifest, so each returns the version it
//! produced. An append to a v1 dataset yields v2 and leaves v1 readable — see
//! `LanceTable::try_new_at_version`.

use crate::error::{QueryError, Result};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use lance::dataset::{Dataset, WriteMode, WriteParams};
use std::path::Path;

/// How a write relates to whatever is already at the destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LanceWriteMode {
    /// Fail if a dataset already exists there.
    Create,
    /// Add rows to an existing dataset, producing a new version.
    Append,
    /// Replace the contents, producing a new version. The old version stays
    /// readable — an overwrite is not a delete.
    Overwrite,
}

impl LanceWriteMode {
    fn to_lance(self) -> WriteMode {
        match self {
            LanceWriteMode::Create => WriteMode::Create,
            LanceWriteMode::Append => WriteMode::Append,
            LanceWriteMode::Overwrite => WriteMode::Overwrite,
        }
    }
}

impl std::str::FromStr for LanceWriteMode {
    type Err = QueryError;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "create" => Ok(LanceWriteMode::Create),
            "append" => Ok(LanceWriteMode::Append),
            "overwrite" => Ok(LanceWriteMode::Overwrite),
            other => Err(QueryError::NotImplemented(format!(
                "unknown Lance write mode `{}` (expected create, append or overwrite)",
                other
            ))),
        }
    }
}

/// What a write produced.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LanceWriteResult {
    /// Rows in the dataset AFTER the write (not rows written).
    pub rows: usize,
    /// The version the write committed.
    pub version: u64,
}

fn lance_err(context: &str, e: impl std::fmt::Display) -> QueryError {
    QueryError::Storage(format!("Lance {}: {}", context, e))
}

/// Run a Lance write on the shared Lance runtime.
///
/// Same reasoning as the reader's `block_on_lance`: `Runtime::block_on` panics
/// if called from a thread a runtime already owns, and writes are invoked from
/// synchronous CLI code that may or may not be inside one. Driving the future
/// from a dedicated thread is safe either way.
fn block_on_write<F, T>(fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    let rt = super::lance::lance_runtime();
    std::thread::spawn(move || rt.block_on(fut))
        .join()
        .unwrap_or_else(|_| Err(QueryError::Execution("Lance write thread panicked".into())))
}

/// Write a `RecordBatchReader` to `path`.
///
/// Takes a reader rather than a `Vec<RecordBatch>` so a large conversion never
/// has to be resident: a Parquet reader streams straight into the Lance writer.
fn write_reader(
    reader: Box<dyn RecordBatchReader + Send + 'static>,
    path: &Path,
    mode: LanceWriteMode,
) -> Result<LanceWriteResult> {
    if mode == LanceWriteMode::Create && path.exists() {
        return Err(QueryError::Storage(format!(
            "Lance dataset already exists at {} (use --mode append or --mode overwrite)",
            path.display()
        )));
    }
    if mode == LanceWriteMode::Append && !path.exists() {
        return Err(QueryError::Storage(format!(
            "cannot append: no Lance dataset at {} (use --mode create)",
            path.display()
        )));
    }
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let uri = path.to_string_lossy().to_string();
    let params = WriteParams {
        mode: mode.to_lance(),
        ..Default::default()
    };

    block_on_write(async move {
        let ds = Dataset::write(reader, uri.as_str(), Some(params))
            .await
            .map_err(|e| lance_err(&format!("write {}", uri), e))?;
        let rows = ds
            .count_rows(None)
            .await
            .map_err(|e| lance_err("count_rows", e))?;
        Ok(LanceWriteResult {
            rows,
            version: ds.version().version,
        })
    })
}

/// Write in-memory Arrow batches as a Lance dataset.
///
/// This is the path a `CREATE TABLE ... AS SELECT` takes: hand it a query
/// result's batches and its schema.
///
/// An empty batch list is rejected rather than silently producing an empty
/// dataset, because the overwhelmingly likely cause is a query that returned
/// nothing and a user who has not noticed.
pub fn write_batches(
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    path: impl AsRef<Path>,
    mode: LanceWriteMode,
) -> Result<LanceWriteResult> {
    if batches.is_empty() {
        return Err(QueryError::Storage(
            "refusing to write a Lance dataset from zero batches: the source produced no rows"
                .to_string(),
        ));
    }
    let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema);
    write_reader(Box::new(reader), path.as_ref(), mode)
}

/// Convert Parquet to Lance, in Rust, without materializing either side.
///
/// `parquet` may be a single file or a directory of `.parquet` files. Batches
/// stream from the Parquet reader straight into the Lance writer, so a 10 GB
/// conversion needs a batch of memory rather than 10 GB of it.
///
/// A directory is written as one dataset: the first file establishes it under
/// `mode`, and the rest are appended. That means a multi-file conversion
/// produces multiple VERSIONS, which is a faithful reflection of what happened
/// rather than a wart — the final version holds every row.
pub fn write_from_parquet(
    parquet: impl AsRef<Path>,
    out: impl AsRef<Path>,
    mode: LanceWriteMode,
) -> Result<LanceWriteResult> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let parquet = parquet.as_ref();
    let out = out.as_ref();

    let files: Vec<std::path::PathBuf> = if parquet.is_dir() {
        let mut v: Vec<_> = std::fs::read_dir(parquet)?
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|e| e == "parquet"))
            .collect();
        // Deterministic order, so a conversion is reproducible.
        v.sort();
        v
    } else {
        vec![parquet.to_path_buf()]
    };

    if files.is_empty() {
        return Err(QueryError::Storage(format!(
            "no .parquet files found at {}",
            parquet.display()
        )));
    }

    let mut result = None;
    for (i, file) in files.iter().enumerate() {
        let f = std::fs::File::open(file)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .map_err(|e| lance_err(&format!("open parquet {}", file.display()), e))?
            .build()
            .map_err(|e| lance_err(&format!("read parquet {}", file.display()), e))?;
        // Only the first file honours the requested mode; the rest must append
        // or they would each overwrite the last.
        let this_mode = if i == 0 { mode } else { LanceWriteMode::Append };
        result = Some(write_reader(Box::new(reader), out, this_mode)?);
    }
    result.ok_or_else(|| QueryError::Storage("no parquet files converted".to_string()))
}

/// Build an IVF_PQ vector index over a `FixedSizeList<Float32, N>` column.
///
/// This is what makes `QE_VECTOR_SEARCH=indexed` possible without Python, and
/// it commits a new dataset version like any other write.
///
/// # The parameters, and why the defaults are what they are
///
/// - `partitions`: IVF cells. The usual rule of thumb is `sqrt(rows)`, which is
///   what `None` computes.
/// - `sub_vectors`: PQ sub-vectors. Must DIVIDE the vector dimension, so `None`
///   picks the largest divisor of the dimension at or below `dim / 8`.
/// - `metric`: must match how the queries will be asked. An index built for L2
///   answers cosine queries wrongly, so this is required, not guessed.
///
/// # Read this before trusting an indexed search
///
/// IVF_PQ is APPROXIMATE. On the engine's own 200k x 384 corpus the indexed
/// path returns the exact top-10 for 7 of 10 queries even with refinement,
/// which is why the engine's default vector-search mode stays exact and the
/// index is opt-in. Building one here does not change that default.
pub fn create_vector_index(
    path: impl AsRef<Path>,
    column: &str,
    metric: crate::planner::vector_types::VectorMetric,
    partitions: Option<usize>,
    sub_vectors: Option<usize>,
    replace: bool,
) -> Result<LanceWriteResult> {
    use lance::index::vector::VectorIndexParams;
    use lance::index::DatasetIndexExt;
    use lance_index::IndexType;
    use lance_linalg::distance::MetricType;

    let path = path.as_ref().to_path_buf();
    if !path.exists() {
        return Err(QueryError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Lance dataset does not exist: {}", path.display()),
        )));
    }
    let uri = path.to_string_lossy().to_string();
    let column = column.to_string();

    let metric_type = match metric {
        crate::planner::vector_types::VectorMetric::L2 => MetricType::L2,
        crate::planner::vector_types::VectorMetric::Cosine => MetricType::Cosine,
        crate::planner::vector_types::VectorMetric::Dot => MetricType::Dot,
    };

    block_on_write(async move {
        let mut ds = Dataset::open(&uri)
            .await
            .map_err(|e| lance_err(&format!("open {}", uri), e))?;

        // Validate the column here, with a message naming it, rather than
        // letting Lance fail from inside the index builder.
        let arrow_schema: arrow::datatypes::Schema = ds.schema().into();
        let field = arrow_schema
            .fields()
            .iter()
            .find(|f| f.name().eq_ignore_ascii_case(&column))
            .ok_or_else(|| {
                QueryError::ColumnNotFound(format!(
                    "{} (dataset {} has: {})",
                    column,
                    uri,
                    arrow_schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ))
            })?;
        let dim = match crate::planner::vector_types::as_float_vector(field.data_type()) {
            Some((_, d)) => d,
            None => {
                return Err(QueryError::Type(format!(
                    "cannot build a vector index on column `{}` of type {}: an IVF_PQ index \
                     needs a fixed-size list of floats (an embedding column)",
                    field.name(),
                    crate::planner::vector_types::describe_type(field.data_type())
                )))
            }
        };

        let rows = ds
            .count_rows(None)
            .await
            .map_err(|e| lance_err("count_rows", e))?;
        if rows == 0 {
            return Err(QueryError::Storage(
                "cannot build a vector index on an empty dataset".to_string(),
            ));
        }

        let partitions = partitions.unwrap_or_else(|| ((rows as f64).sqrt() as usize).max(1));
        // PQ requires the dimension to be divisible by the sub-vector count.
        // Silently rounding would build an index that answers a different
        // question, so pick a real divisor or fail.
        let sub_vectors = match sub_vectors {
            Some(n) => {
                if dim % n != 0 {
                    return Err(QueryError::Type(format!(
                        "sub_vectors ({}) must divide the vector dimension ({}) exactly",
                        n, dim
                    )));
                }
                n
            }
            None => (1..=dim.min(dim / 8).max(1))
                .rev()
                .find(|n| dim % n == 0)
                .unwrap_or(1),
        };

        let params = VectorIndexParams::ivf_pq(partitions, 8, sub_vectors, metric_type, 50);
        ds.create_index(&[&column], IndexType::Vector, None, &params, replace)
            .await
            .map(|_metadata| ())
            .map_err(|e| lance_err(&format!("create_index on {}", column), e))?;

        // Re-open so the reported version reflects the committed manifest.
        let ds = Dataset::open(&uri)
            .await
            .map_err(|e| lance_err("reopen after index", e))?;
        let rows = ds
            .count_rows(None)
            .await
            .map_err(|e| lance_err("count_rows", e))?;
        Ok(LanceWriteResult {
            rows,
            version: ds.version().version,
        })
    })
}
