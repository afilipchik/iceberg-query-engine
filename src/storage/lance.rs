//! Lance table provider
//!
//! Reads [Lance](https://lancedb.github.io/lance/) datasets as engine tables.
//!
//! # Why this is cheap to integrate
//!
//! `lance` 0.23.x is the last release line built against **arrow 53**, the same
//! arrow major this engine uses. Lance therefore hands back *our*
//! `RecordBatch` — no IPC round-trip, no FFI bridge, no copy. Anything newer
//! (0.25+, 10.x) moves to arrow 54/55/58 and would fork the arrow version
//! in-tree, so the dependency is pinned to `0.23` on purpose.
//!
//! # Why it is feature-gated
//!
//! The Lance tree is ~490 crates (it vendors DataFusion 44). Compiling it into
//! the default build risks perturbing the tuned TPC-H parquet path for zero
//! benefit to users who do not read Lance. `--features lance` opts in;
//! `cargo build` does not compile a line of it.
//!
//! # What this provider does and does not do
//!
//! It pushes **column projection** into Lance (`Scanner::project`), which is
//! the format's core strength, and scans fragments in parallel. It does *not*
//! get the parquet path's fast lanes — morsel-driven aggregation, runtime
//! filter bitmaps pushed into the decoder, row-group pruning — because those
//! are keyed off `TableProvider::parquet_files()`. See CLAUDE.md for the
//! measured cost of that gap.

use crate::error::{QueryError, Result};
use crate::physical::operators::{ColumnStatistics, TableProvider, TableStatistics};
use arrow::array::{Array, Int64Array, RecordBatchOptions};
use arrow::datatypes::{DataType, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use lance::dataset::Dataset;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Batch size requested from Lance.
///
/// 8K rows, matching the engine's tuned parquet/morsel batch size: wide enough
/// to amortize per-batch dispatch, small enough that a batch's working set
/// stays in L2 during joins and aggregation.
const LANCE_BATCH_SIZE: usize = 8192;

/// Dedicated multi-threaded runtime for Lance I/O.
///
/// `TableProvider::scan` is synchronous but Lance is async, so the two must be
/// bridged. Creating a runtime per scan would be both wasteful and *wrong*:
/// `Runtime::block_on` panics when called from inside another runtime's
/// context, and scans run inside the engine's async execution. A single shared
/// runtime, driven from a dedicated thread (see `block_on_lance`), avoids both.
///
/// Sized to `num_cpus`, not a small constant: a scan fans out one task per
/// fragment (58 for SF=10 lineitem), and a narrow pool would serialize the
/// decode that fragment parallelism exists to overlap. This mirrors the
/// reasoning in `physical::operators::subquery::subquery_runtime`, which was
/// widened from 2 workers to `num_cpus` for exactly this class of problem.
fn lance_runtime() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(num_cpus::get().max(2))
            .enable_all()
            .thread_name("lance-io")
            .build()
            .expect("Failed to create Lance runtime")
    })
}

/// Drive a Lance future to completion from synchronous code.
///
/// The future is driven on the shared Lance runtime from a *separate* thread,
/// so this is safe to call whether or not the caller is already inside an async
/// context — the nested-runtime panic only triggers when `block_on` runs on a
/// thread already owned by a runtime.
fn block_on_lance<F, T>(fut: F) -> Result<T>
where
    F: std::future::Future<Output = Result<T>> + Send + 'static,
    T: Send + 'static,
{
    let rt = lance_runtime();
    std::thread::spawn(move || rt.block_on(fut))
        .join()
        .unwrap_or_else(|_| Err(QueryError::Execution("Lance scan thread panicked".into())))
}

fn lance_err(context: &str, e: impl fmt::Display) -> QueryError {
    QueryError::Storage(format!("Lance {}: {}", context, e))
}

/// Reject Lance column types the engine's execution operators cannot evaluate.
///
/// Failing at registration with a named column beats coercing silently and
/// producing wrong answers, or panicking deep inside an operator at run time.
/// The nested cases are the realistic ones: `FixedSizeList` is how Lance stores
/// vector embeddings, which is precisely what a LanceDB user is most likely to
/// have in a table.
fn unsupported_reason(dt: &DataType) -> Option<String> {
    match dt {
        DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Null => None,
        DataType::Dictionary(key, value) => {
            // Dictionary-encoded strings flow through the engine's string paths.
            match (key.as_ref(), value.as_ref()) {
                (DataType::Int8, DataType::Utf8)
                | (DataType::Int16, DataType::Utf8)
                | (DataType::Int32, DataType::Utf8)
                | (DataType::UInt32, DataType::Utf8) => None,
                _ => Some(format!("dictionary type {:?}", dt)),
            }
        }
        DataType::FixedSizeList(field, width) => Some(format!(
            "fixed-size list of {} x {:?} (a Lance vector/embedding column); \
             the engine has no array type, so select the scalar columns \
             explicitly instead of `SELECT *`",
            width,
            field.data_type()
        )),
        DataType::List(_) | DataType::LargeList(_) => {
            Some("list/array columns (the engine has no array type)".to_string())
        }
        DataType::Struct(_) => Some("struct columns (no nested type support)".to_string()),
        DataType::Map(_, _) => Some("map columns (no nested type support)".to_string()),
        other => Some(format!("column type {:?}", other)),
    }
}

/// Table provider backed by a Lance dataset.
pub struct LanceTable {
    dataset: Arc<Dataset>,
    schema: SchemaRef,
    /// Exact row count, read from dataset metadata at open time.
    num_rows: usize,
    /// On-disk size of the dataset directory.
    total_bytes: u64,
    /// Number of fragments; the unit of scan parallelism.
    num_fragments: usize,
    path: PathBuf,
    /// Lazily computed column statistics; see `compute_column_stats`.
    stats_cache: std::sync::OnceLock<std::collections::HashMap<String, ColumnStatistics>>,
}

impl fmt::Debug for LanceTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LanceTable")
            .field("path", &self.path)
            .field("rows", &self.num_rows)
            .field("fragments", &self.num_fragments)
            .field("schema", &self.schema)
            .finish()
    }
}

impl LanceTable {
    /// Open a Lance dataset, caching its schema and row count.
    ///
    /// Returns an error naming the offending column if the dataset contains a
    /// type the engine cannot execute over.
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            return Err(QueryError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Lance dataset does not exist: {}", path.display()),
            )));
        }

        let uri = path.to_string_lossy().to_string();
        let open_path = path.clone();
        let (dataset, num_rows) = block_on_lance(async move {
            let ds = Dataset::open(&uri)
                .await
                .map_err(|e| lance_err(&format!("open {}", uri), e))?;
            let rows = ds
                .count_rows(None)
                .await
                .map_err(|e| lance_err("count_rows", e))?;
            Ok((ds, rows))
        })
        .map_err(|e| match e {
            QueryError::Storage(msg) => {
                QueryError::Storage(format!("{} (path: {})", msg, open_path.display()))
            }
            other => other,
        })?;

        let arrow_schema: ArrowSchema = dataset.schema().into();

        // Fail loudly, with the column name, rather than coercing.
        let mut rejected = Vec::new();
        for field in arrow_schema.fields() {
            if let Some(reason) = unsupported_reason(field.data_type()) {
                rejected.push(format!("  - {}: {}", field.name(), reason));
            }
        }
        if !rejected.is_empty() {
            return Err(QueryError::NotImplemented(format!(
                "Lance dataset {} has {} column(s) the engine cannot read:\n{}",
                path.display(),
                rejected.len(),
                rejected.join("\n")
            )));
        }

        let num_fragments = dataset.get_fragments().len();
        let total_bytes = dir_size(&path);

        Ok(Self {
            dataset: Arc::new(dataset),
            schema: Arc::new(arrow_schema),
            num_rows,
            total_bytes,
            num_fragments,
            path,
            stats_cache: std::sync::OnceLock::new(),
        })
    }

    /// Number of rows, from metadata (no scan).
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Number of fragments in the dataset.
    pub fn num_fragments(&self) -> usize {
        self.num_fragments
    }

    /// Compute and cache column statistics, doing the work at most once.
    ///
    /// Callers that want the cost paid at a predictable moment (a benchmark
    /// harness, a server warming a catalog) can call this at registration time
    /// instead of letting the first optimized query absorb it.
    pub fn warm_statistics(&self) {
        self.stats_cache.get_or_init(|| self.compute_column_stats());
    }

    /// Per-column min/max/NDV for integer-typed columns.
    ///
    /// **This scans.** Unlike a Parquet footer, Lance 0.23 exposes no per-column
    /// min/max through its public Rust API, so the only way to get them is to
    /// read the columns. Doing so is not optional: the cost-based join
    /// reorderer derives join cardinality from key NDV, and with no NDV at all
    /// it mis-ordered TPC-H Q05 into `supplier ⋈ customer` on `nationkey` — a
    /// ~1.2-billion-row intermediate that never finished. Cardinality alone is
    /// not enough to plan joins.
    ///
    /// Only integer-ish columns are read, which is exactly what the join
    /// reorderer consumes, and they are read through Lance's projection so the
    /// wide string columns (`l_comment` and friends) are never touched. NDV
    /// uses the same `min(non_null_rows, max - min + 1)` estimate as
    /// `ParquetTable`, tight for TPC-H's dense surrogate keys.
    ///
    /// String NDV is NOT computed: `ParquetTable` gets it free from dictionary
    /// pages, but here it would mean hashing every value of every string
    /// column. Equality filters on string columns therefore fall back to the
    /// optimizer's default selectivity.
    fn compute_column_stats(&self) -> std::collections::HashMap<String, ColumnStatistics> {
        use arrow::compute::kernels::aggregate::{max as arr_max, min as arr_min};
        use arrow::compute::kernels::cast::cast;
        use arrow::datatypes::DataType as DT;
        use std::collections::HashMap;

        let int_names: Vec<String> = self
            .schema
            .fields()
            .iter()
            .filter(|f| {
                matches!(
                    f.data_type(),
                    DT::Int8
                        | DT::Int16
                        | DT::Int32
                        | DT::Int64
                        | DT::UInt8
                        | DT::UInt16
                        | DT::UInt32
                        | DT::Date32
                        | DT::Date64
                )
            })
            .map(|f| f.name().clone())
            .collect();

        let mut out: HashMap<String, ColumnStatistics> = HashMap::new();
        if int_names.is_empty() {
            return out;
        }

        let ds = Arc::clone(&self.dataset);
        let names = int_names.clone();
        let Ok(batches) = block_on_lance(async move { scan_fragments(ds, names).await }) else {
            // Statistics are an optimization, not a correctness input: if the
            // probe scan fails, plan without them rather than failing the query.
            return out;
        };

        for (idx, name) in int_names.iter().enumerate() {
            let mut min_v: Option<i64> = None;
            let mut max_v: Option<i64> = None;
            let mut nulls: u64 = 0;

            for batch in &batches {
                let Some(col) = batch.columns().get(idx) else {
                    continue;
                };
                nulls += col.null_count() as u64;
                let Ok(as_i64) = cast(col, &DT::Int64) else {
                    continue;
                };
                let Some(arr) = as_i64.as_any().downcast_ref::<Int64Array>() else {
                    continue;
                };
                if let Some(v) = arr_min(arr) {
                    min_v = Some(min_v.map_or(v, |m: i64| m.min(v)));
                }
                if let Some(v) = arr_max(arr) {
                    max_v = Some(max_v.map_or(v, |m: i64| m.max(v)));
                }
            }

            let non_null = (self.num_rows as u64).saturating_sub(nulls);
            let ndv_est = match (min_v, max_v) {
                (Some(lo), Some(hi)) if hi >= lo => {
                    Some(non_null.min((hi - lo) as u64 + 1)).filter(|v| *v > 0)
                }
                _ => None,
            };

            out.insert(
                name.to_lowercase(),
                ColumnStatistics {
                    min_i64: min_v,
                    max_i64: max_v,
                    null_count: Some(nulls),
                    ndv_est,
                },
            );
        }
        out
    }

    /// Resolve projection indices into Lance column names.
    fn projected_names(&self, projection: Option<&[usize]>) -> Result<Vec<String>> {
        match projection {
            None => Ok(self
                .schema
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()),
            Some(indices) => indices
                .iter()
                .map(|&i| {
                    self.schema
                        .fields()
                        .get(i)
                        .map(|f| f.name().clone())
                        .ok_or_else(|| {
                            QueryError::Internal(format!(
                                "projection index {} out of range for Lance table {} ({} columns)",
                                i,
                                self.path.display(),
                                self.schema.fields().len()
                            ))
                        })
                })
                .collect(),
        }
    }
}

/// Scan every fragment concurrently, preserving fragment order in the output.
///
/// One task per fragment: Lance decode is CPU-bound per fragment, so this is
/// the format's natural parallel unit (58 fragments for SF=10 `lineitem`).
async fn scan_fragments(ds: Arc<Dataset>, names: Vec<String>) -> Result<Vec<RecordBatch>> {
    let fragments = ds.get_fragments();

    // Single fragment: no point paying for task spawn + join.
    if fragments.len() <= 1 {
        return scan_one(ds, names, None).await;
    }

    let mut tasks = Vec::with_capacity(fragments.len());
    for fragment in fragments {
        let ds = Arc::clone(&ds);
        let names = names.clone();
        let meta = fragment.metadata().clone();
        tasks.push(tokio::spawn(async move {
            scan_one(ds, names, Some(meta)).await
        }));
    }

    // Collect in fragment order so results are deterministic across runs.
    let mut out = Vec::new();
    for task in tasks {
        let batches = task
            .await
            .map_err(|e| QueryError::Execution(format!("Lance fragment task failed: {}", e)))??;
        out.extend(batches);
    }
    Ok(out)
}

async fn scan_one(
    ds: Arc<Dataset>,
    names: Vec<String>,
    fragment: Option<lance::table::format::Fragment>,
) -> Result<Vec<RecordBatch>> {
    let mut scanner = ds.scan();
    if let Some(meta) = fragment {
        scanner.with_fragments(vec![meta]);
    }
    let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
    // THE point of the integration: Lance reads only these columns off disk.
    scanner
        .project(&refs)
        .map_err(|e| lance_err(&format!("project {:?}", names), e))?;
    scanner.batch_size(LANCE_BATCH_SIZE);

    let stream = scanner
        .try_into_stream()
        .await
        .map_err(|e| lance_err("open scan stream", e))?;
    stream
        .try_collect()
        .await
        .map_err(|e| lance_err("read batches", e))
}

fn dir_size(path: &Path) -> u64 {
    fn walk(p: &Path, acc: &mut u64) {
        let Ok(entries) = std::fs::read_dir(p) else {
            return;
        };
        for entry in entries.flatten() {
            match entry.metadata() {
                Ok(m) if m.is_dir() => walk(&entry.path(), acc),
                Ok(m) => *acc += m.len(),
                Err(_) => {}
            }
        }
    }
    let mut total = 0;
    if path.is_dir() {
        walk(path, &mut total);
    }
    total
}

impl TableProvider for LanceTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        let names = self.projected_names(projection)?;

        // A zero-column projection (e.g. bare COUNT(*)) is a row count, not a
        // read. Lance rejects an empty projection list, so answer it from
        // metadata and hand back one column-less batch carrying the row count.
        if names.is_empty() {
            let batch = RecordBatch::try_new_with_options(
                Arc::new(ArrowSchema::empty()),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(self.num_rows)),
            )?;
            return Ok(vec![batch]);
        }

        let ds = Arc::clone(&self.dataset);
        block_on_lance(async move { scan_fragments(ds, names).await })
    }

    /// Row count and size come from Lance metadata; integer column min/max/NDV
    /// are computed by a one-time projected scan (see `compute_column_stats`,
    /// which explains why that scan is mandatory rather than an optimization).
    fn statistics(&self) -> Option<TableStatistics> {
        let column_stats = self
            .stats_cache
            .get_or_init(|| self.compute_column_stats())
            .clone();
        Some(TableStatistics {
            row_count: self.num_rows,
            total_byte_size: self.total_bytes,
            column_stats,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sf001() -> Option<PathBuf> {
        let p = PathBuf::from("data/tpch-1mb-lance");
        p.join("orders.lance").exists().then_some(p)
    }

    #[test]
    fn test_open_and_schema() {
        let Some(dir) = sf001() else { return };
        let t = LanceTable::try_new(dir.join("orders.lance")).unwrap();
        assert_eq!(t.num_rows(), 1500);
        assert_eq!(t.schema().fields().len(), 9);
        assert_eq!(t.schema().field(0).name(), "o_orderkey");
    }

    #[test]
    fn test_projection_reads_only_requested_columns() {
        let Some(dir) = sf001() else { return };
        let t = LanceTable::try_new(dir.join("orders.lance")).unwrap();
        let batches = t.scan(Some(&[2, 0])).unwrap();
        assert!(!batches.is_empty());
        let s = batches[0].schema();
        // Order must follow the projection list, not the table schema.
        assert_eq!(s.fields().len(), 2);
        assert_eq!(s.field(0).name(), "o_orderstatus");
        assert_eq!(s.field(1).name(), "o_orderkey");
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1500);
    }

    #[test]
    fn test_empty_projection_is_a_row_count() {
        let Some(dir) = sf001() else { return };
        let t = LanceTable::try_new(dir.join("orders.lance")).unwrap();
        let batches = t.scan(Some(&[])).unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1500);
    }

    #[test]
    fn test_statistics_row_count() {
        let Some(dir) = sf001() else { return };
        let t = LanceTable::try_new(dir.join("orders.lance")).unwrap();
        let stats = t.statistics().unwrap();
        assert_eq!(stats.row_count, 1500);
        assert!(stats.total_byte_size > 0);
    }

    #[test]
    fn test_missing_dataset_errors() {
        let err = LanceTable::try_new("data/definitely-not-here.lance").unwrap_err();
        assert!(matches!(err, QueryError::Io(_)), "got {:?}", err);
    }

    #[test]
    fn test_projection_out_of_range_errors() {
        let Some(dir) = sf001() else { return };
        let t = LanceTable::try_new(dir.join("orders.lance")).unwrap();
        assert!(t.scan(Some(&[99])).is_err());
    }

    #[test]
    fn test_vector_column_is_rejected_by_name() {
        use arrow::datatypes::Field;
        let dt =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 128);
        let reason = unsupported_reason(&dt).expect("vector columns must be rejected");
        assert!(reason.contains("vector"), "unhelpful message: {}", reason);
        assert!(unsupported_reason(&DataType::Int64).is_none());
        assert!(unsupported_reason(&DataType::Utf8).is_none());
    }
}
