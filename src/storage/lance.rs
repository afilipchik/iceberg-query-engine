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
//! the format's core strength, scans fragments in parallel, and derives the
//! table/column statistics the shared optimizer needs (Lance exposes none).
//!
//! It does *not* get the parquet path's decoder-level fast lanes — morsel
//! aggregation, `RowFilter`, row-group pruning, runtime join-filter bitmaps —
//! because those are keyed off `TableProvider::parquet_files()`. As of
//! 2026-08-09 the Lance leg is nevertheless **8% FASTER** than the Parquet leg
//! on SF=10 TPC-H (6.79s vs 7.39s, winning 15 of 22), because statistics
//! parity matters more than decoder tricks. CLAUDE.md, "Which Parquet
//! optimizations the Lance path shares", records which of those lanes were
//! ported, which cannot be, and which were built and then measured negative.

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
pub(super) fn lance_runtime() -> &'static tokio::runtime::Runtime {
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

/// Is this a type the engine's *scalar* operators (compare, arithmetic, hash,
/// sort) can evaluate directly?
///
/// Used to decide whether a nested type's element type is carryable.
fn is_scalar_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
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
            | DataType::Null
    )
}

/// How deep a nested type may nest before the reader refuses it.
///
/// A bound exists so a pathological (or hand-crafted) schema cannot drive the
/// recursion below into a stack overflow. Eight levels is far past anything a
/// real LanceDB table carries.
const MAX_NEST_DEPTH: usize = 8;

/// Reject Lance column types the engine's execution operators cannot evaluate.
///
/// Failing at registration with a named column beats coercing silently and
/// producing wrong answers, or panicking deep inside an operator at run time.
///
/// # The rule: scalars are *evaluated*, nested values are *carried*
///
/// A scalar column can be compared, summed, hashed and sorted. A nested column
/// — a vector, a list, a struct, a map — is accepted as an **opaque carried
/// value**: it can be scanned, projected, aliased, UNION'd and LIMITed, and a
/// vector can additionally be fed to the distance functions. It deliberately
/// cannot be summed, grouped by, compared with `=`/`<`, or sorted; those fail
/// loudly, naming the column, via `crate::planner::vector_types`. Carrying a
/// value the engine cannot order is safe; pretending it has an order is not.
///
/// `FixedSizeList<Float32, N>` is how Lance stores embeddings and is the single
/// most important column type a LanceDB user has. `Struct` is the second: real
/// tables routinely carry a metadata blob next to their vector. Both are read
/// here as long as every *leaf* underneath them is a type the engine knows.
fn unsupported_reason(dt: &DataType) -> Option<String> {
    // Half-precision floats are carryable *inside* a vector — Lance stores fp16
    // embeddings and the distance kernels widen them — but the engine has no
    // fp16 scalar kernels, so a bare fp16 column is rejected rather than
    // silently mis-evaluated by a path that assumes f32/f64.
    if matches!(dt, DataType::Float16) {
        return Some("half-precision float column (no fp16 scalar kernels)".to_string());
    }
    carried_reason(dt, 0)
}

/// Reason `dt` cannot be carried through the engine, or `None` if it can.
///
/// Recursive, so a struct of lists of structs is judged by its leaves: the
/// engine never interprets the nesting, only the scalar types at the bottom
/// (which have to round-trip through Arrow kernels during concat/take/filter).
fn carried_reason(dt: &DataType, depth: usize) -> Option<String> {
    if depth > MAX_NEST_DEPTH {
        return Some(format!("nesting deeper than {} levels", MAX_NEST_DEPTH));
    }
    if is_scalar_type(dt) {
        return None;
    }
    match dt {
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
        DataType::FixedSizeList(field, width) => carried_reason(field.data_type(), depth + 1)
            .map(|r| format!("fixed-size list[{}] element: {}", width, r)),
        DataType::List(field) | DataType::LargeList(field) => {
            carried_reason(field.data_type(), depth + 1).map(|r| format!("list element: {}", r))
        }
        DataType::Struct(fields) => fields.iter().find_map(|f| {
            carried_reason(f.data_type(), depth + 1)
                .map(|r| format!("struct field `{}`: {}", f.name(), r))
        }),
        // A Map is physically a list of key/value structs; judging the entry
        // struct covers both halves in one recursion.
        DataType::Map(entry, _) => {
            carried_reason(entry.data_type(), depth + 1).map(|r| format!("map entry: {}", r))
        }
        other => Some(format!("column type {:?}", other)),
    }
}

/// Table provider backed by a Lance dataset.
/// One fragment's identity and weight, for distributed split enumeration.
#[derive(Clone, Copy, Debug)]
pub struct FragmentInfo {
    pub id: u64,
    pub rows: i64,
    pub bytes: u64,
}

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
    /// Whether `scan_with_filter` pushes the predicate into Lance.
    ///
    /// Diagnostic switch, not a safety switch: BOTH settings must produce
    /// identical rows, and `pushdown_matches_no_pushdown_row_for_row` asserts
    /// exactly that. It exists so the equivalence can be tested and the speedup
    /// measured with one binary. Defaults to on.
    filter_pushdown: bool,
    /// How many scans actually pushed a predicate into Lance.
    ///
    /// Exists so a test can prove the pushdown FIRED. Without it, an A/B that
    /// compares "pushdown on" against "pushdown off" passes trivially if the
    /// renderer silently refuses everything — the most likely way for this
    /// feature to be quietly dead.
    pushed_filters: std::sync::atomic::AtomicUsize,
    /// When set, every scan is restricted to exactly these fragment ids —
    /// the distributed layer's shard of this dataset. `None` = the whole
    /// dataset. Threaded through every scan path; a path that ignored it
    /// would return another shard's rows as its own.
    fragment_subset: Option<std::collections::BTreeSet<u64>>,
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
        Self::try_new_impl(path.as_ref(), None)
    }

    /// Open a HISTORICAL version of a Lance dataset — time travel.
    ///
    /// Lance is a versioned format: every append, overwrite, delete and index
    /// build commits a new manifest and leaves the previous ones intact, so an
    /// older version is a first-class thing to read rather than a backup to
    /// restore. This is the property that distinguishes it from a directory of
    /// Parquet files, and reading it costs nothing extra — the old manifest
    /// points at data files that were never rewritten.
    ///
    /// The returned table is immutable at that version: its schema, row count
    /// and statistics are all the version's own. Registering the same path
    /// twice under different names and versions is therefore a legitimate way
    /// to diff two snapshots in one SQL query.
    pub fn try_new_at_version(path: impl AsRef<Path>, version: u64) -> Result<Self> {
        Self::try_new_impl(path.as_ref(), Some(version))
    }

    /// The dataset version this table reads.
    pub fn version(&self) -> u64 {
        self.dataset.version().version
    }

    /// Every version of the dataset at `path`, oldest first.
    ///
    /// Returns `(version, RFC-3339 timestamp)`. Reads manifests only; no data
    /// file is touched.
    pub fn list_versions(path: impl AsRef<Path>) -> Result<Vec<(u64, String)>> {
        let path = path.as_ref().to_path_buf();
        if !path.exists() {
            return Err(QueryError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Lance dataset does not exist: {}", path.display()),
            )));
        }
        let uri = path.to_string_lossy().to_string();
        block_on_lance(async move {
            let ds = Dataset::open(&uri)
                .await
                .map_err(|e| lance_err(&format!("open {}", uri), e))?;
            let mut versions = ds
                .versions()
                .await
                .map_err(|e| lance_err("versions", e))?
                .into_iter()
                .map(|v| (v.version, v.timestamp.to_rfc3339()))
                .collect::<Vec<_>>();
            versions.sort_by_key(|(v, _)| *v);
            Ok(versions)
        })
    }

    fn try_new_impl(path: &Path, version: Option<u64>) -> Result<Self> {
        let path = path.to_path_buf();
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
            // Checking out an unknown version must fail loudly. Silently
            // serving the latest instead would answer a question about history
            // with today's data, which is the worst possible outcome for a
            // feature whose entire purpose is to read the past.
            let ds = match version {
                None => ds,
                Some(v) => ds
                    .checkout_version(v)
                    .await
                    .map_err(|e| lance_err(&format!("checkout version {}", v), e))?,
            };
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
            filter_pushdown: Self::env_pushdown_enabled(),
            pushed_filters: std::sync::atomic::AtomicUsize::new(0),
            fragment_subset: None,
        })
    }

    /// Number of scans that pushed a predicate into Lance so far.
    pub fn pushed_filter_count(&self) -> usize {
        self.pushed_filters
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Turn predicate pushdown into Lance off (diagnostic only).
    ///
    /// Both settings are correct — the engine re-applies the full predicate
    /// above the scan either way — so this changes speed, never answers. That
    /// is precisely what makes it a usable A/B oracle in tests.
    ///
    /// Also reachable as `QE_LANCE_FILTER_PUSHDOWN=0` on any registration path,
    /// so the same A/B can be run against the shipped binary.
    pub fn with_filter_pushdown(mut self, enabled: bool) -> Self {
        self.filter_pushdown = enabled;
        self
    }

    /// `QE_LANCE_FILTER_PUSHDOWN=0` disables pushdown; anything else leaves it on.
    fn env_pushdown_enabled() -> bool {
        !matches!(
            std::env::var("QE_LANCE_FILTER_PUSHDOWN").as_deref(),
            Ok("0") | Ok("off") | Ok("false")
        )
    }

    /// Number of rows, from metadata (no scan).
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Number of fragments in the dataset.
    pub fn num_fragments(&self) -> usize {
        self.num_fragments
    }

    /// Every fragment's identity, row count and on-disk bytes — the atoms the
    /// distributed layer divides. Sorted by id so every node enumerates the
    /// identical list. Refuses datasets with deletion vectors: a row count
    /// that ignores them is a wrong answer waiting to be summed.
    pub fn fragment_infos(&self) -> Result<Vec<FragmentInfo>> {
        let mut out = Vec::new();
        for f in self.dataset.get_fragments() {
            let meta = f.metadata();
            if meta.deletion_file.is_some() {
                return Err(QueryError::NotImplemented(format!(
                    "Lance dataset {} fragment {} has a deletion vector; distributed \
                     execution over datasets with deletions is not supported — compact first",
                    self.path.display(),
                    meta.id
                )));
            }
            let rows = meta.physical_rows.ok_or_else(|| {
                QueryError::Storage(format!(
                    "Lance dataset {} fragment {} does not record physical_rows",
                    self.path.display(),
                    meta.id
                ))
            })? as i64;
            let mut bytes = 0u64;
            for df in &meta.files {
                let p = self.path.join("data").join(&df.path);
                bytes += std::fs::metadata(&p)
                    .map_err(|e| {
                        QueryError::Storage(format!(
                            "Lance data file {} is listed but unreadable: {e}",
                            p.display()
                        ))
                    })?
                    .len();
            }
            out.push(FragmentInfo {
                id: meta.id,
                rows,
                bytes,
            });
        }
        out.sort_by_key(|f| f.id);
        Ok(out)
    }

    /// A view of this dataset restricted to exactly `ids` — one node's shard.
    /// Row count and byte size are recomputed for the subset so the count(*)
    /// metadata shortcut and the optimizer's estimates describe the SHARD,
    /// not the whole dataset.
    pub fn shard_with_fragments(&self, ids: impl IntoIterator<Item = u64>) -> Result<LanceTable> {
        let subset: std::collections::BTreeSet<u64> = ids.into_iter().collect();
        let infos = self.fragment_infos()?;
        for id in &subset {
            if !infos.iter().any(|f| f.id == *id) {
                return Err(QueryError::Storage(format!(
                    "Lance dataset {} has no fragment {id}; the shard assignment does not \
                     match this node's copy of the data",
                    self.path.display()
                )));
            }
        }
        let owned: Vec<_> = infos.iter().filter(|f| subset.contains(&f.id)).collect();
        Ok(LanceTable {
            dataset: Arc::clone(&self.dataset),
            schema: self.schema.clone(),
            num_rows: owned.iter().map(|f| f.rows).sum::<i64>() as usize,
            total_bytes: owned.iter().map(|f| f.bytes).sum(),
            num_fragments: owned.len(),
            path: self.path.clone(),
            stats_cache: std::sync::OnceLock::new(),
            filter_pushdown: self.filter_pushdown,
            pushed_filters: std::sync::atomic::AtomicUsize::new(0),
            fragment_subset: Some(subset),
        })
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

        // A NOT NULL column has zero nulls. That is a fact carried by the
        // schema, not an estimate, and it costs nothing to read — which makes
        // it the one piece of statistics Lance gives away as freely as a
        // Parquet footer does.
        //
        // It is also load-bearing, and for a column type the scan below never
        // touches. `EagerAggregation` refuses to pre-aggregate unless every
        // column feeding a SUM factor is provably null-free, because summing a
        // group with no non-NULL rows must yield NULL and a pre-aggregate would
        // turn that into 0. Q09's factor is `ps_supplycost`, a **Float64**, so
        // with integer statistics alone the lookup missed, the rule declined,
        // and the Lance plan silently diverged from the Parquet one — 2.06s
        // instead of 1.43s at SF=10. Nothing about that was visible as a
        // "statistics" problem; it looked like the join order was just worse.
        for field in self.schema.fields() {
            if !field.is_nullable() {
                out.insert(
                    field.name().to_lowercase(),
                    ColumnStatistics {
                        null_count: Some(0),
                        ..Default::default()
                    },
                );
            }
        }

        if int_names.is_empty() {
            return out;
        }

        let ds = Arc::clone(&self.dataset);
        let names = int_names.clone();
        let subset = self.fragment_subset.clone();
        let Ok(batches) =
            block_on_lance(async move { scan_fragments(ds, names, None, subset).await })
        else {
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

    /// Should this predicate be pushed into Lance, or is the engine faster?
    ///
    /// # What a pushed filter costs, measured correctly
    ///
    /// An earlier round of these measurements concluded that `Scanner::filter`
    /// is a flat pessimization — 8x slower than decoding the columns outright.
    /// That conclusion was an artifact of two things the benchmark held wrong,
    /// and it is worth spelling out because it nearly buried a real win:
    ///
    /// - it left `MaterializationStyle` at its default, `Heuristic`, which on
    ///   local storage late-materializes only columns wider than 10 bytes and
    ///   therefore late-materializes *nothing* in a table of narrow scalars;
    /// - it ran one scanner over the whole dataset, while the engine's reader
    ///   is fragment-parallel.
    ///
    /// Fixed on both counts (SF=10 `lineitem`, 32 threads, best of 3), the sign
    /// flips. `scan_one` now always asks for `AllLate`:
    ///
    /// | shape | no filter | filter, Heuristic | filter, **AllLate** |
    /// |---|---|---|---|
    /// | Q06, 4 cols, 1.3% out | 192 ms | 1,112 ms | **121 ms (0.63x)** |
    /// | Q12, 5 cols, 2.9% out | 165 ms | 1,707 ms | **134 ms (0.81x)** |
    /// | Q19, 6 cols, 3.6% out | 266 ms | 1,120 ms | 251 ms (0.94x) |
    ///
    /// # But the gate still stands, because the loss side is brutal
    ///
    /// A pushed filter is a win only where it is SELECTIVE. Forcing every
    /// renderable conjunct down (`QE_LANCE_PUSH=all`, with `AllLate` on) took
    /// the SF=10 suite from **6.76s to 10.83s**. The selective queries improved
    /// exactly as the table predicts — Q19 405 -> 325 ms, Q06 151 -> 133,
    /// Q12 241 -> 228 — and the non-selective ones collapsed: **Q01 351 ->
    /// 1801 ms** (its predicate keeps 95% of rows), Q21 556 -> 1432, Q03
    /// 294 -> 811. Unconditional pushdown stays rejected; only the threshold's
    /// justification changed.
    ///
    /// A BTREE scalar index on the filter column also still makes it worse, not
    /// better: 3,323 ms with the index vs 1,531 ms without.
    ///
    /// # Where it pays hugely: a wide payload the filter lets Lance skip
    ///
    /// `data/vectors.lance` carries a 384-float embedding. `SELECT id,
    /// category, embedding FROM vectors WHERE id < 100`, through the engine
    /// binary, with `AllLate`: **33-53 ms unpushed vs 2.1-5.3 ms pushed, 15-25x**
    /// (it was 6.4x before `AllLate`). Selectivity sweep on the same dataset:
    ///
    /// | predicate | selectivity | vs unfiltered |
    /// |---|---|---|
    /// | `id < 100` | 0.05% | 20x faster |
    /// | `id < 10000` | 5% | 5.9x faster |
    /// | `id < 20000` | 10% | 4.5x faster |
    /// | `id < 40000` | 20% | 1.3x faster |
    /// | `id < 100000` | 50% | 0.9x — SLOWER |
    /// | `category = 'books'` (string col) | 20% | 0.4x — SLOWER |
    ///
    /// # The three conditions
    ///
    /// 1. **The projection must contain a nested (wide) column.** This is the
    ///    conservative one, and it is what keeps TPC-H out. Dropping it would
    ///    unlock the Q06/Q12/Q19 wins above (~110 ms of a 6.7s suite) but only
    ///    if the gate could tell those apart from Q01 — and it cannot: their
    ///    selectivity lives in `l_shipmode`/`l_shipinstruct`/`l_discount`, and
    ///    `estimate_selectivity` has statistics for neither strings nor floats.
    ///    Guessing wrong costs 5x on one query to gain 20% on another.
    /// 2. **The filter must not reference that wide column**, or Lance has to
    ///    decode it to evaluate the predicate and saves nothing.
    /// 3. **Estimated selectivity must be known and <= 10%.** Unknown means no
    ///    push, which conveniently excludes the string-column case — the
    ///    slowest row in the table above, at 0.4x.
    fn plan_pushdown(
        &self,
        predicate: &crate::planner::Expr,
        projection: Option<&[usize]>,
    ) -> Option<String> {
        let fields = self.schema.fields();
        let projected: Vec<&arrow::datatypes::FieldRef> = match projection {
            Some(idx) => idx.iter().filter_map(|&i| fields.get(i)).collect(),
            None => fields.iter().collect(),
        };

        // (1) Is there a wide payload column whose decode could be skipped?
        let wide: Vec<String> = projected
            .iter()
            .filter(|f| crate::planner::vector_types::is_opaque_nested(f.data_type()))
            .map(|f| f.name().to_lowercase())
            .collect();
        if wide.is_empty() {
            return None;
        }

        let (_, conjuncts) = lance_pushable_conjuncts(predicate, &self.schema)?;
        let stats = self.stats_cache.get_or_init(|| self.compute_column_stats());

        let mut sql = Vec::new();
        let mut combined = 1.0f64;
        for c in conjuncts {
            // (2) Skip a conjunct that touches the wide column: Lance would
            // have to decode it to evaluate the predicate, which is the whole
            // cost being avoided. Dropping a conjunct is always sound — the
            // engine re-applies the full predicate — so this narrows the push
            // rather than abandoning it.
            let mut cols = Vec::new();
            collect_columns(c, &mut cols);
            if cols.iter().any(|col| wide.contains(col)) {
                continue;
            }
            // (3) Estimatable, or leave it to the engine.
            let (Some(s), Some(rendered)) = (
                estimate_selectivity(c, &self.schema, stats, self.num_rows),
                expr_to_lance_sql(c, &self.schema),
            ) else {
                continue;
            };
            combined *= s;
            sql.push(rendered);
        }

        // Selective enough, in aggregate, to beat a straight decode.
        (!sql.is_empty() && combined <= PUSHDOWN_SELECTIVITY_LIMIT).then(|| sql.join(" AND "))
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
/// `QE_LANCE_TIMING=1` reports every scan's wall time, width and row count.
///
/// Attribution before optimization: this is what showed that the Lance path's
/// TPC-H losses are NOT in the scan (Q09 spends 165ms of 2.06s there) and that
/// Q19's are (176ms of 416ms), which sent the work to the optimizer instead of
/// to the reader.
async fn scan_fragments(
    ds: Arc<Dataset>,
    names: Vec<String>,
    filter: Option<String>,
    subset: Option<std::collections::BTreeSet<u64>>,
) -> Result<Vec<RecordBatch>> {
    if std::env::var("QE_LANCE_TIMING").is_err() {
        return scan_fragments_inner(ds, names, filter, subset).await;
    }
    let t0 = std::time::Instant::now();
    let out = scan_fragments_inner(ds, names.clone(), filter, subset).await;
    let rows: usize = out
        .as_ref()
        .map(|b: &Vec<RecordBatch>| b.iter().map(|x| x.num_rows()).sum())
        .unwrap_or(0);
    eprintln!(
        "[lance-scan] {:>7.1}ms {:>11} rows {} cols {:?}",
        t0.elapsed().as_secs_f64() * 1000.0,
        rows,
        names.len(),
        names
    );
    out
}

async fn scan_fragments_inner(
    ds: Arc<Dataset>,
    names: Vec<String>,
    filter: Option<String>,
    subset: Option<std::collections::BTreeSet<u64>>,
) -> Result<Vec<RecordBatch>> {
    let mut fragments = ds.get_fragments();
    if let Some(keep) = &subset {
        fragments.retain(|f| keep.contains(&f.metadata().id));
        if fragments.is_empty() {
            return Ok(Vec::new());
        }
    }

    // Single fragment: no point paying for task spawn + join. When a subset
    // is active the fragment must still be named explicitly — an unfragmented
    // scan would read the whole dataset.
    if fragments.len() <= 1 {
        let meta = if subset.is_some() {
            fragments.first().map(|f| f.metadata().clone())
        } else {
            None
        };
        return scan_one(ds, names, meta, filter).await;
    }

    let mut tasks = Vec::with_capacity(fragments.len());
    for fragment in fragments {
        let ds = Arc::clone(&ds);
        let names = names.clone();
        let filter = filter.clone();
        let meta = fragment.metadata().clone();
        tasks.push(tokio::spawn(async move {
            scan_one(ds, names, Some(meta), filter).await
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
    filter: Option<String>,
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
    // Late materialization: Lance evaluates the predicate over the filter's own
    // columns and only decodes the projected columns for surviving rows. The
    // filter may reference columns outside the projection — verified — so no
    // widening of `names` is needed.
    if let Some(sql) = &filter {
        scanner
            .filter(sql)
            .map_err(|e| lance_err(&format!("filter {}", sql), e))?;
        // ASK for late materialization; do not accept the default.
        //
        // `MaterializationStyle::Heuristic` (the default) late-materializes a
        // column only if it is wider than 10 bytes on local storage, so for a
        // table of narrow scalars it late-materializes NOTHING: every projected
        // column is decoded for every row and then thrown away. That single
        // knob is most of why a pushed filter used to look like a catastrophe.
        // Measured on SF=10 `lineitem`, fragment-parallel, best of 3, against
        // the same scan with no filter at all:
        //
        // | shape | no filter | filter, Heuristic | filter, AllLate |
        // |---|---|---|---|
        // | Q06, 4 cols, 1.3% | 192 ms | 1,112 ms | **121 ms** |
        // | Q12, 5 cols, 2.9% | 165 ms | 1,707 ms | **134 ms** |
        // | Q19, 6 cols, 3.6% | 266 ms | 1,120 ms | 251 ms |
        //
        // The default turns a 0.63x win into a 5.8x loss. Every earlier
        // conclusion about Lance filter pushdown was measured through it.
        scanner.materialization_style(lance::dataset::scanner::MaterializationStyle::AllLate);
    }
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

/// Resolve an engine column reference to the dataset field it names, if the
/// name can be written as a BARE identifier.
///
/// # The single most dangerous fact about Lance 0.23.2's filter dialect
///
/// **A double-quoted identifier parses as a STRING LITERAL.** `"category" =
/// 'footwear'` is not a column comparison, it is `'category' = 'footwear'` — a
/// constant FALSE. The filter then matches NOTHING, and Lance reports no error:
/// a scan that should return 40,000 rows returns 0. That is a wrong-answers
/// failure mode, not a slow one.
///
/// So identifiers are never quoted, and any name that would *need* quoting
/// (spaces, punctuation, a leading digit, a reserved word's shape) refuses the
/// pushdown instead. A refused pushdown is slow; a quoted one is silently wrong.
fn lance_bare_ident<'a>(
    col: &crate::planner::Column,
    schema: &'a ArrowSchema,
) -> Option<&'a arrow::datatypes::Field> {
    let field = schema
        .fields()
        .iter()
        .find(|f| f.name().eq_ignore_ascii_case(&col.name))?;
    let name = field.name();
    let simple = !name.is_empty()
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    simple.then(|| field.as_ref())
}

/// Render a literal, and REFUSE it unless it means the same thing against
/// `column_type` in Lance's dialect as it does in the engine's evaluator.
///
/// The type gate is the second line of defence after bare identifiers. The
/// engine and DataFusion both do implicit coercion around comparisons, and they
/// do not have to agree: a decimal literal against a float column, an integer
/// against a date, a timestamp with an ambiguous zone. Any disagreement that
/// makes Lance's filter MORE selective than the engine's silently loses rows.
/// Requiring the literal's family to match the column's removes that whole
/// class of hazard rather than reasoning about each case.
fn lance_literal(v: &crate::planner::ScalarValue, column_type: &DataType) -> Option<String> {
    use crate::planner::ScalarValue as SV;

    let is_int_col = matches!(
        column_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    );
    let is_float_col = matches!(column_type, DataType::Float32 | DataType::Float64);
    let is_str_col = matches!(column_type, DataType::Utf8 | DataType::LargeUtf8);

    match v {
        SV::Boolean(b) if matches!(column_type, DataType::Boolean) => Some(b.to_string()),

        SV::Int8(_)
        | SV::Int16(_)
        | SV::Int32(_)
        | SV::Int64(_)
        | SV::UInt8(_)
        | SV::UInt16(_)
        | SV::UInt32(_)
        | SV::UInt64(_)
            if is_int_col || is_float_col =>
        {
            let n: i128 = match v {
                SV::Int8(i) => *i as i128,
                SV::Int16(i) => *i as i128,
                SV::Int32(i) => *i as i128,
                SV::Int64(i) => *i as i128,
                SV::UInt8(i) => *i as i128,
                SV::UInt16(i) => *i as i128,
                SV::UInt32(i) => *i as i128,
                SV::UInt64(i) => *i as i128,
                _ => return None,
            };
            // Against a float column the integer is widened. Beyond 2^53 that
            // widening is lossy and the two engines could round differently.
            if is_float_col && n.unsigned_abs() > (1u128 << 53) {
                return None;
            }
            Some(n.to_string())
        }

        SV::Float32(f) if is_float_col => f.0.is_finite().then(|| format!("{:?}", f.0)),
        SV::Float64(f) if is_float_col => f.0.is_finite().then(|| format!("{:?}", f.0)),

        // Rendered with `{:?}` (shortest round-trip) so DataFusion's parser
        // recovers bit-identical f64s. Verified: the folded TPC-H Q06 bounds
        // 0.049999999999999996 / 0.06999999999999999 round-trip exactly and
        // select the same 1183 rows as the Arrow kernel.
        SV::Utf8(s) if is_str_col => Some(format!("'{}'", s.replace('\'', "''"))),

        SV::Date32(d) if matches!(column_type, DataType::Date32) => {
            // A DATE literal, never the raw day number: `l_shipdate <= 10471`
            // would compare a date against an integer and depends entirely on
            // whose coercion rules win. Lance rejects a bare string here
            // ("could not convert to literal of type 'Date32'"), which is the
            // good kind of failure, but `DATE '1998-09-02'` is unambiguous.
            let date = chrono::NaiveDate::from_num_days_from_ce_opt(*d + 719_163)?;
            Some(format!("DATE '{}'", date.format("%Y-%m-%d")))
        }

        // Decimals, timestamps, intervals, lists, NULL and every mismatched
        // pairing above fall through here. Each has a formatting or coercion
        // subtlety that could change which rows survive, and none is worth a
        // wrong answer.
        _ => None,
    }
}

/// Render an engine predicate as a Lance (DataFusion) SQL filter string.
///
/// # The contract
///
/// Returns `None` for anything not on the whitelist below, and the whitelist is
/// deliberately narrow. Two callers depend on the result meaning **exactly**
/// what the engine's own evaluator computes:
///
/// - `scan_with_filter`, where the engine re-applies the full predicate above
///   the scan. A pushed filter can therefore only be wrong by dropping a row
///   the engine would have KEPT. It can never be wrong by keeping too many.
/// - `scan_knn`, where `None` means "do not push the search down at all",
///   because a k-NN search that applies its filter after the fact returns fewer
///   than k rows rather than the right k.
///
/// # The whitelist
///
/// `column OP literal` and `column OP column` for `= != < <= > >=`; `IS NULL`
/// / `IS NOT NULL` on a column; `AND` / `OR` / `NOT` over the above; `IN` /
/// `NOT IN` with literals; `BETWEEN` (desugared to two comparisons, so no
/// dialect question about BETWEEN itself can arise).
///
/// # What is deliberately absent
///
/// - **LIKE / NOT LIKE.** The engine's pattern semantics against DataFusion's
///   are unverified, and TPC-H leans on `NOT LIKE` in exactly the places where
///   an over-selective filter would silently delete rows.
/// - **Arithmetic.** Integer division and decimal scaling differ.
/// - **Column-to-column comparison across different types**, for the coercion
///   reason in `lance_literal`. Same-type is allowed, and is what makes
///   `l_commitdate < l_receiptdate` pushable.
fn expr_to_lance_sql(expr: &crate::planner::Expr, schema: &ArrowSchema) -> Option<String> {
    use crate::planner::{BinaryOp, Expr, UnaryOp};

    /// Strip aliases/no-op casts that would otherwise hide a plain column.
    fn peel(e: &Expr) -> &Expr {
        match e {
            Expr::Alias { expr, .. } => peel(expr),
            other => other,
        }
    }

    /// Render one comparison, type-checking both sides.
    fn comparison(left: &Expr, sym: &str, right: &Expr, schema: &ArrowSchema) -> Option<String> {
        let (l, r) = (peel(left), peel(right));
        match (l, r) {
            (Expr::Column(c), Expr::Literal(v)) => {
                let f = lance_bare_ident(c, schema)?;
                Some(format!(
                    "({} {} {})",
                    f.name(),
                    sym,
                    lance_literal(v, f.data_type())?
                ))
            }
            (Expr::Literal(v), Expr::Column(c)) => {
                let f = lance_bare_ident(c, schema)?;
                Some(format!(
                    "({} {} {})",
                    lance_literal(v, f.data_type())?,
                    sym,
                    f.name()
                ))
            }
            (Expr::Column(a), Expr::Column(b)) => {
                let (fa, fb) = (lance_bare_ident(a, schema)?, lance_bare_ident(b, schema)?);
                // Identical types only: no coercion, therefore nothing to
                // disagree about.
                (fa.data_type() == fb.data_type())
                    .then(|| format!("({} {} {})", fa.name(), sym, fb.name()))
            }
            _ => None,
        }
    }

    fn go(e: &Expr, schema: &ArrowSchema) -> Option<String> {
        match peel(e) {
            Expr::BinaryExpr { left, op, right } => match op {
                BinaryOp::And => Some(format!(
                    "({} AND {})",
                    go(left, schema)?,
                    go(right, schema)?
                )),
                BinaryOp::Or => Some(format!("({} OR {})", go(left, schema)?, go(right, schema)?)),
                BinaryOp::Eq => comparison(left, "=", right, schema),
                BinaryOp::NotEq => comparison(left, "!=", right, schema),
                BinaryOp::Lt => comparison(left, "<", right, schema),
                BinaryOp::LtEq => comparison(left, "<=", right, schema),
                BinaryOp::Gt => comparison(left, ">", right, schema),
                BinaryOp::GtEq => comparison(left, ">=", right, schema),
                _ => None,
            },
            Expr::UnaryExpr { op, expr } => match (op, peel(expr)) {
                (UnaryOp::Not, _) => Some(format!("(NOT {})", go(expr, schema)?)),
                (UnaryOp::IsNull, Expr::Column(c)) => {
                    Some(format!("({} IS NULL)", lance_bare_ident(c, schema)?.name()))
                }
                (UnaryOp::IsNotNull, Expr::Column(c)) => Some(format!(
                    "({} IS NOT NULL)",
                    lance_bare_ident(c, schema)?.name()
                )),
                _ => None,
            },
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let Expr::Column(c) = peel(expr) else {
                    return None;
                };
                let f = lance_bare_ident(c, schema)?;
                let items: Option<Vec<String>> = list
                    .iter()
                    .map(|l| match peel(l) {
                        Expr::Literal(v) => lance_literal(v, f.data_type()),
                        _ => None,
                    })
                    .collect();
                let items = items?;
                if items.is_empty() {
                    return None;
                }
                Some(format!(
                    "({} {}IN ({}))",
                    f.name(),
                    if *negated { "NOT " } else { "" },
                    items.join(", ")
                ))
            }
            // Desugared rather than emitted as BETWEEN: two comparisons the
            // renderer already type-checks, and no reliance on Lance's BETWEEN.
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let lo = comparison(expr, ">=", low, schema)?;
                let hi = comparison(expr, "<=", high, schema)?;
                Some(if *negated {
                    format!("(NOT ({} AND {}))", lo, hi)
                } else {
                    format!("({} AND {})", lo, hi)
                })
            }
            _ => None,
        }
    }

    go(expr, schema)
}

/// Split a predicate into its top-level AND conjuncts.
fn split_conjuncts<'a>(e: &'a crate::planner::Expr, out: &mut Vec<&'a crate::planner::Expr>) {
    use crate::planner::{BinaryOp, Expr};
    if let Expr::BinaryExpr {
        left,
        op: BinaryOp::And,
        right,
    } = e
    {
        split_conjuncts(left, out);
        split_conjuncts(right, out);
    } else {
        out.push(e);
    }
}

/// Render as much of a WHERE predicate as Lance can evaluate faithfully.
///
/// # Why partial pushdown is sound here, and only here
///
/// The physical planner ALWAYS wraps a filtered scan in a `FilterExec` carrying
/// the full predicate (`planner.rs`, the `node.filter` arm), so whatever Lance
/// returns is filtered again by the engine. Pushdown can therefore only be
/// wrong by returning too FEW rows, never too many.
///
/// That makes it sound to push a subset of the top-level AND conjuncts and drop
/// the rest: under SQL's three-valued WHERE semantics only TRUE passes, so if a
/// conjunct `C` is not TRUE for a row then `C AND rest` is not TRUE either, and
/// the row was never going to survive. Every dropped conjunct is a row Lance
/// removes that the engine would have removed anyway.
///
/// The same reasoning does NOT hold inside an `OR`, which is why disjunctions
/// are pushed whole or not at all.
fn lance_filter_sql(expr: &crate::planner::Expr, schema: &ArrowSchema) -> Option<String> {
    lance_pushable_conjuncts(expr, schema).map(|(sql, _)| sql)
}

/// The renderable conjuncts of `expr`, as one SQL string plus the expressions
/// that produced it (which the cost gate then estimates selectivity over).
fn lance_pushable_conjuncts<'a>(
    expr: &'a crate::planner::Expr,
    schema: &ArrowSchema,
) -> Option<(String, Vec<&'a crate::planner::Expr>)> {
    let mut conjuncts = Vec::new();
    split_conjuncts(expr, &mut conjuncts);

    let mut sql = Vec::new();
    let mut kept = Vec::new();
    for c in conjuncts {
        if let Some(s) = expr_to_lance_sql(c, schema) {
            sql.push(s);
            kept.push(c);
        }
    }
    (!sql.is_empty()).then(|| (sql.join(" AND "), kept))
}

/// Collect the column names an expression references.
///
/// Only the variants `expr_to_lance_sql` can render need covering, since the
/// only caller feeds it already-rendered conjuncts. Anything else contributes
/// no columns, which is safe here: the caller uses this to prove a wide column
/// is ABSENT, and it never sees an expression this does not understand.
fn collect_columns(e: &crate::planner::Expr, out: &mut Vec<String>) {
    use crate::planner::Expr;
    match e {
        Expr::Column(c) => out.push(c.name.to_lowercase()),
        Expr::Alias { expr, .. } => collect_columns(expr, out),
        Expr::BinaryExpr { left, right, .. } => {
            collect_columns(left, out);
            collect_columns(right, out);
        }
        Expr::UnaryExpr { expr, .. } => collect_columns(expr, out),
        Expr::InList { expr, list, .. } => {
            collect_columns(expr, out);
            for l in list {
                collect_columns(l, out);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_columns(expr, out);
            collect_columns(low, out);
            collect_columns(high, out);
        }
        _ => {}
    }
}

/// Estimated fraction of rows a predicate keeps, or `None` when unknown.
///
/// Deliberately partial. Only integer- and date-typed columns have statistics
/// (see `compute_column_stats`), so only predicates over those get an estimate;
/// everything else returns `None` and the caller declines to push. That is the
/// right bias: an unknown-selectivity filter is exactly the case the
/// measurements below say can lose, so "I do not know" must mean "do not".
fn estimate_selectivity(
    e: &crate::planner::Expr,
    schema: &ArrowSchema,
    stats: &std::collections::HashMap<String, ColumnStatistics>,
    rows: usize,
) -> Option<f64> {
    use crate::planner::{BinaryOp, Expr, ScalarValue as SV, UnaryOp};

    /// A literal's value on the same i64 scale the statistics use.
    fn as_i64(v: &SV) -> Option<i64> {
        Some(match v {
            SV::Int8(i) => *i as i64,
            SV::Int16(i) => *i as i64,
            SV::Int32(i) => *i as i64,
            SV::Int64(i) => *i,
            SV::UInt8(i) => *i as i64,
            SV::UInt16(i) => *i as i64,
            SV::UInt32(i) => *i as i64,
            SV::UInt64(i) => i64::try_from(*i).ok()?,
            SV::Date32(d) => *d as i64,
            _ => return None,
        })
    }

    fn col_stats<'a>(
        e: &crate::planner::Expr,
        stats: &'a std::collections::HashMap<String, ColumnStatistics>,
    ) -> Option<&'a ColumnStatistics> {
        match e {
            Expr::Column(c) => stats.get(&c.name.to_lowercase()),
            _ => None,
        }
    }

    /// Fraction of a column's [min, max] range below `v`.
    fn range_fraction(cs: &ColumnStatistics, v: i64, inclusive: bool) -> Option<f64> {
        let (lo, hi) = (cs.min_i64?, cs.max_i64?);
        let span = (hi as i128 - lo as i128 + 1).max(1) as f64;
        let below = (v as i128 - lo as i128 + if inclusive { 1 } else { 0 }).max(0) as f64;
        Some((below / span).clamp(0.0, 1.0))
    }

    match e {
        Expr::BinaryExpr { left, op, right } => {
            match op {
                // Independence assumption, the same one the join reorderer makes.
                BinaryOp::And => {
                    let a = estimate_selectivity(left, schema, stats, rows)?;
                    let b = estimate_selectivity(right, schema, stats, rows)?;
                    return Some(a * b);
                }
                // Over-estimate a disjunction (no inclusion-exclusion term), so
                // an OR is never *more* attractive than it deserves to be.
                BinaryOp::Or => {
                    let a = estimate_selectivity(left, schema, stats, rows)?;
                    let b = estimate_selectivity(right, schema, stats, rows)?;
                    return Some((a + b).min(1.0));
                }
                _ => {}
            }
            // Normalize to `column OP literal`, flipping the operator if the
            // literal is on the left.
            let (cs, lit, op) = match (col_stats(left, stats), right.as_ref()) {
                (Some(cs), Expr::Literal(v)) => (cs, v, *op),
                _ => match (left.as_ref(), col_stats(right, stats)) {
                    (Expr::Literal(v), Some(cs)) => (
                        cs,
                        v,
                        match op {
                            BinaryOp::Lt => BinaryOp::Gt,
                            BinaryOp::LtEq => BinaryOp::GtEq,
                            BinaryOp::Gt => BinaryOp::Lt,
                            BinaryOp::GtEq => BinaryOp::LtEq,
                            other => *other,
                        },
                    ),
                    // Column-to-column and everything else: unknown.
                    _ => return None,
                },
            };
            let v = as_i64(lit)?;
            match op {
                BinaryOp::Eq => Some(1.0 / cs.ndv_est? as f64),
                BinaryOp::NotEq => Some(1.0 - 1.0 / cs.ndv_est? as f64),
                BinaryOp::Lt => range_fraction(cs, v, false),
                BinaryOp::LtEq => range_fraction(cs, v, true),
                BinaryOp::Gt => range_fraction(cs, v, true).map(|f| 1.0 - f),
                BinaryOp::GtEq => range_fraction(cs, v, false).map(|f| 1.0 - f),
                _ => None,
            }
        }
        Expr::UnaryExpr { op, expr } => match op {
            UnaryOp::Not => estimate_selectivity(expr, schema, stats, rows).map(|s| 1.0 - s),
            UnaryOp::IsNull => {
                let cs = col_stats(expr, stats)?;
                Some((cs.null_count? as f64 / rows.max(1) as f64).clamp(0.0, 1.0))
            }
            UnaryOp::IsNotNull => {
                let cs = col_stats(expr, stats)?;
                Some(1.0 - (cs.null_count? as f64 / rows.max(1) as f64).clamp(0.0, 1.0))
            }
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let cs = col_stats(expr, stats)?;
            let s = (list.len() as f64 / cs.ndv_est? as f64).clamp(0.0, 1.0);
            Some(if *negated { 1.0 - s } else { s })
        }
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let cs = col_stats(expr, stats)?;
            let (Expr::Literal(l), Expr::Literal(h)) = (low.as_ref(), high.as_ref()) else {
                return None;
            };
            let lo = range_fraction(cs, as_i64(l)?, false)?;
            let hi = range_fraction(cs, as_i64(h)?, true)?;
            let s = (hi - lo).clamp(0.0, 1.0);
            Some(if *negated { 1.0 - s } else { s })
        }
        _ => None,
    }
}

/// Estimated selectivity at or below which pushing a filter into Lance pays.
///
/// See `LanceTable::pushdown_pays` for the measurements. 10% sits well inside
/// the winning region (4.5x at the measured 10% point, 20x at 0.05%) with
/// margin before the ~40% crossover.
const PUSHDOWN_SELECTIVITY_LIMIT: f64 = 0.10;

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
        let subset = self.fragment_subset.clone();
        block_on_lance(async move { scan_fragments(ds, names, None, subset).await })
    }

    /// Scan with as much of the predicate as Lance can evaluate faithfully.
    ///
    /// This is the Lance path's largest performance lever: without it, a
    /// `WHERE l_shipdate <= DATE '1998-09-02'` scan decodes every row of every
    /// column and throws most of them away in the engine.
    ///
    /// # Safety of a partial push
    ///
    /// The physical planner always wraps a filtered scan in a `FilterExec`
    /// carrying the FULL predicate, so this is a row-reduction hint, not a
    /// semantic commitment: rows Lance keeps are re-checked, rows Lance drops
    /// are gone. That makes over-approximation free and under-approximation
    /// fatal, which is exactly the bias `lance_filter_sql` is built with — see
    /// its doc comment for why an unrenderable conjunct can simply be omitted
    /// and why the same is not true inside an `OR`.
    ///
    /// Falls back to an unfiltered scan whenever nothing can be rendered.
    fn scan_with_filter(
        &self,
        projection: Option<&[usize]>,
        filter: Option<&crate::planner::Expr>,
    ) -> Result<Vec<RecordBatch>> {
        let Some(predicate) = filter.filter(|_| self.filter_pushdown) else {
            return self.scan(projection);
        };
        // `QE_LANCE_PUSH=all` pushes every renderable conjunct, ignoring the
        // cost gate. Diagnostic only: it is how the gate below is calibrated,
        // and it stays correct because the planner re-applies the predicate.
        let forced = matches!(std::env::var("QE_LANCE_PUSH").as_deref(), Ok("all"));
        let sql = if forced {
            lance_filter_sql(predicate, &self.schema)
        } else {
            self.plan_pushdown(predicate, projection)
        };
        let Some(sql) = sql else {
            return self.scan(projection);
        };

        let names = self.projected_names(projection)?;
        // A zero-column projection is answered from metadata by `scan`, and a
        // row count under a predicate is not what the caller asked for anyway
        // (the engine's FilterExec cannot filter a column-less batch). Let the
        // metadata path handle it.
        if names.is_empty() {
            return self.scan(projection);
        }

        self.pushed_filters
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let ds = Arc::clone(&self.dataset);
        let subset = self.fragment_subset.clone();
        block_on_lance(async move { scan_fragments(ds, names, Some(sql), subset).await })
    }

    /// Run a k-NN search through Lance's vector index.
    ///
    /// # Semantics — read this before trusting the output
    ///
    /// Lance's vector indices (IVF_PQ, IVF_HNSW_*) are **approximate**. The
    /// rows this returns are not guaranteed to be the exact top-k, and with
    /// product quantization the returned `_distance` values are quantized
    /// approximations too. `refine_factor` re-ranks `k * factor` candidates
    /// with exact distances, which fixes the *distances* and greatly improves
    /// *which rows* come back, but does not make the search exhaustive.
    ///
    /// `use_index = false` asks Lance for a flat (exact) search. That is still
    /// useful — Lance's scan is faster than pulling every embedding through the
    /// engine — and it is what makes "exact via Lance" a distinct, cheaper
    /// option from "exact via the engine's brute-force path".
    ///
    /// Returns `Ok(None)` when the request cannot be served faithfully:
    /// unknown column, dimension mismatch, or no index when one was required.
    /// The caller then falls back to the exact path.
    fn distributed_splits(
        &self,
        table: &str,
        nodes: usize,
    ) -> Option<Result<crate::distributed::SplitSet>> {
        use crate::distributed::{Split, SplitSet};
        let infos = match self.fragment_infos() {
            Ok(i) => i,
            Err(e) => return Some(Err(e)),
        };
        // The atom is a whole fragment — Lance's own unit of parallel decode.
        // No sub-fragment cutting yet: balance is bounded by fragment sizes
        // (58 fragments for SF=10 lineitem spread fine across <=8 nodes).
        let splits: Vec<Split> = infos
            .iter()
            .map(|f| Split {
                table: table.to_string(),
                path: self.path.clone(),
                file: format!("fragment-{:08}", f.id),
                row_group: f.id as usize,
                row_offset: 0,
                num_rows: f.rows,
                bytes: f.bytes,
            })
            .collect();
        let total_bytes = splits.iter().map(|s| s.bytes).sum();
        let total_rows = splits.iter().map(|s| s.num_rows).sum();
        Some(Ok(SplitSet {
            table: table.to_string(),
            splits,
            total_bytes,
            total_rows,
            target_split_bytes: crate::distributed::splits::target_split_bytes(total_bytes, nodes),
        }))
    }

    fn shard_by_splits(
        &self,
        splits: &[crate::distributed::Split],
    ) -> Option<Result<Arc<dyn TableProvider>>> {
        Some(
            self.shard_with_fragments(splits.iter().map(|s| s.row_group as u64))
                .map(|t| Arc::new(t) as Arc<dyn TableProvider>),
        )
    }

    fn scan_knn(
        &self,
        projection: Option<&[usize]>,
        q: &crate::physical::vector::VectorQuery,
    ) -> Result<Option<Vec<RecordBatch>>> {
        use arrow::array::Float32Array;

        // A sharded view declines the k-NN pushdown: Lance's nearest() has no
        // fragment restriction here, so it would search the WHOLE dataset and
        // attribute the result to this shard. Declining sends the caller to
        // the exact path, whose scans are subset-filtered and therefore
        // correct. (Worker SQL never contains a k-NN shape today; this guard
        // is for whatever calls it tomorrow.)
        if self.fragment_subset.is_some() {
            return Ok(None);
        }

        // Validate the column against the dataset schema before touching Lance,
        // so a typo produces a clean fallback rather than an opaque Lance error.
        let Some(field) = self
            .schema
            .fields()
            .iter()
            .find(|f| f.name().eq_ignore_ascii_case(&q.column))
        else {
            return Ok(None);
        };
        match field.data_type() {
            DataType::FixedSizeList(_, width) if *width as usize == q.query.len() => {}
            _ => return Ok(None),
        }

        let mut names = self.projected_names(projection)?;
        // Lance rejects an empty projection, and the search needs at least one
        // column to hang the result on.
        if names.is_empty() {
            names.push(field.name().clone());
        }

        // A predicate that cannot be rendered faithfully as Lance SQL must
        // abort the pushdown entirely. Running the search WITHOUT the filter
        // and letting a later operator apply it would return the k nearest
        // rows overall and then throw most of them away — fewer than k rows,
        // silently. Falling back is slow; this would be wrong.
        let filter_sql = match &q.filter {
            None => None,
            Some(e) => match expr_to_lance_sql(e, &self.schema) {
                Some(sql) => Some(sql),
                None => return Ok(None),
            },
        };

        let ds = Arc::clone(&self.dataset);
        let column = field.name().clone();
        let query = q.clone();
        let batches = block_on_lance(async move {
            let mut scanner = ds.scan();
            let refs: Vec<&str> = names.iter().map(|s| s.as_str()).collect();
            scanner
                .project(&refs)
                .map_err(|e| lance_err(&format!("project {:?}", names), e))?;

            let key = Arc::new(Float32Array::from(query.query.clone()));
            scanner
                .nearest(&column, key.as_ref(), query.k)
                .map_err(|e| lance_err(&format!("nearest({})", column), e))?;

            // MEASURED BUG IN LANCE 0.23.2: `prefilter(true)` does not
            // prefilter when the vector index is used. A search for
            // `category = 'books'` over a dataset with 40,000 matching rows
            // returns ZERO rows, because the predicate is applied to the
            // index's candidate list instead of before it — and for a query
            // vector whose neighbours are all in another category, that list
            // contains nothing that survives. `use_index(false)` + prefilter
            // returns the correct 10 rows.
            //
            // So: a filtered search runs as a flat (exact) Lance scan. That is
            // slower than the index (67ms vs 11ms on the 200k test set) but it
            // is still ~2x faster than pulling every embedding through the
            // engine, and — the part that matters — it is CORRECT. Returning
            // silently truncated results here would be the exact failure mode
            // this whole operator is built to prevent.
            let use_index = query.use_index && filter_sql.is_none();
            scanner.use_index(use_index);
            if use_index {
                if let Some(n) = query.nprobes {
                    scanner.nprobs(n);
                }
                if let Some(r) = query.refine_factor {
                    scanner.refine(r);
                }
            }
            if let Some(sql) = &filter_sql {
                scanner.prefilter(true);
                scanner
                    .filter(sql)
                    .map_err(|e| lance_err(&format!("filter {}", sql), e))?;
            }
            scanner.batch_size(LANCE_BATCH_SIZE);

            let stream = scanner
                .try_into_stream()
                .await
                .map_err(|e| lance_err("open knn stream", e))?;
            stream
                .try_collect::<Vec<_>>()
                .await
                .map_err(|e| lance_err("read knn batches", e))
        })?;

        Ok(Some(batches))
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
    fn test_vector_column_is_accepted() {
        use arrow::datatypes::Field;
        let dt =
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 128);
        assert!(
            unsupported_reason(&dt).is_none(),
            "embedding columns must be readable"
        );
        assert!(unsupported_reason(&DataType::Int64).is_none());
        assert!(unsupported_reason(&DataType::Utf8).is_none());
        // Lists of scalars pass through too.
        let list = DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert!(unsupported_reason(&list).is_none());
    }

    #[test]
    fn test_struct_and_list_are_accepted() {
        use arrow::datatypes::Field;
        // A metadata blob: the second most common column on a real LanceDB
        // table after the embedding itself.
        let meta = DataType::Struct(
            vec![
                Field::new("source", DataType::Utf8, true),
                Field::new("score", DataType::Float64, true),
            ]
            .into(),
        );
        assert!(unsupported_reason(&meta).is_none(), "{:?}", meta);

        // Struct of list of struct: nesting is judged by its leaves.
        let deep = DataType::Struct(
            vec![Field::new(
                "items",
                DataType::List(Arc::new(Field::new("item", meta.clone(), true))),
                true,
            )]
            .into(),
        );
        assert!(unsupported_reason(&deep).is_none(), "{:?}", deep);

        // A struct holding a vector, which is exactly how some LanceDB tables
        // group an embedding with its model name.
        let with_vec = DataType::Struct(
            vec![Field::new(
                "emb",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 384),
                true,
            )]
            .into(),
        );
        assert!(unsupported_reason(&with_vec).is_none());
    }

    #[test]
    fn test_unreadable_leaf_is_reported_with_its_path() {
        use arrow::datatypes::Field;
        // Duration has no engine kernels; the message must locate it.
        let bad = DataType::Struct(
            vec![Field::new(
                "d",
                DataType::Duration(arrow::datatypes::TimeUnit::Second),
                true,
            )]
            .into(),
        );
        let reason = unsupported_reason(&bad).expect("must be rejected");
        assert!(reason.contains('d'), "{}", reason);
        assert!(reason.contains("Duration"), "{}", reason);

        // Same leaf, one list deeper: the path prefix accumulates.
        let nested = DataType::List(Arc::new(Field::new("item", bad, true)));
        let reason = unsupported_reason(&nested).expect("must be rejected");
        assert!(reason.contains("list element"), "{}", reason);
        assert!(reason.contains("struct field"), "{}", reason);
    }

    #[test]
    fn test_bare_fp16_column_rejected_but_fp16_vector_accepted() {
        use arrow::datatypes::Field;
        // The engine has no fp16 scalar kernels...
        assert!(unsupported_reason(&DataType::Float16).is_some());
        // ...but fp16 embeddings are a real Lance layout and are carried.
        let v = DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float16, true)), 768);
        assert!(unsupported_reason(&v).is_none());
    }

    #[test]
    fn test_recursion_is_bounded() {
        use arrow::datatypes::Field;
        let mut dt = DataType::Int32;
        for _ in 0..(MAX_NEST_DEPTH + 3) {
            dt = DataType::List(Arc::new(Field::new("item", dt, true)));
        }
        assert!(
            unsupported_reason(&dt).is_some_and(|r| r.contains("nesting deeper")),
            "pathological nesting must be refused, not recursed into"
        );
    }

    // -----------------------------------------------------------------------
    // Filter -> Lance SQL rendering.
    //
    // These assert on the STRING, because the failure mode being defended
    // against is a filter Lance accepts and evaluates to something other than
    // what the engine meant. A rendering test catches that at the seam; an
    // end-to-end row-count test catches it only if the data happens to expose
    // it. Both exist (see `tests/lance_tests.rs`).
    // -----------------------------------------------------------------------

    fn filter_schema() -> ArrowSchema {
        use arrow::datatypes::Field;
        ArrowSchema::new(vec![
            Field::new("l_orderkey", DataType::Int64, false),
            Field::new("l_quantity", DataType::Float64, false),
            Field::new("l_returnflag", DataType::Utf8, false),
            Field::new("l_shipdate", DataType::Date32, false),
            Field::new("l_commitdate", DataType::Date32, false),
            Field::new("l_price", DataType::Decimal128(15, 2), true),
            Field::new("weird name", DataType::Int64, true),
        ])
    }

    fn col(name: &str) -> crate::planner::Expr {
        crate::planner::Expr::column(name)
    }
    fn lit_i(v: i64) -> crate::planner::Expr {
        crate::planner::Expr::literal(crate::planner::ScalarValue::Int64(v))
    }
    fn lit_s(v: &str) -> crate::planner::Expr {
        crate::planner::Expr::literal(crate::planner::ScalarValue::Utf8(v.to_string()))
    }

    /// THE headline hazard. Lance 0.23.2 parses a double-quoted identifier as a
    /// STRING LITERAL, so `"l_returnflag" = 'R'` is the constant FALSE and
    /// matches nothing — silently. Nothing this renderer emits may be quoted.
    #[test]
    fn test_identifiers_are_never_double_quoted() {
        let s = filter_schema();
        let sql = expr_to_lance_sql(&col("l_returnflag").eq(lit_s("R")), &s).expect("renderable");
        assert!(
            !sql.contains('"'),
            "a double-quoted identifier is a silent constant-FALSE in Lance 0.23.2: {}",
            sql
        );
        assert_eq!(sql, "(l_returnflag = 'R')");
    }

    /// A name that would need quoting must refuse the pushdown outright rather
    /// than be quoted (see above) or emitted bare (which would not parse).
    #[test]
    fn test_unquotable_name_refuses_pushdown() {
        let s = filter_schema();
        assert!(expr_to_lance_sql(&col("weird name").eq(lit_i(1)), &s).is_none());
    }

    #[test]
    fn test_string_literals_escape_embedded_quotes() {
        let s = filter_schema();
        let sql = expr_to_lance_sql(&col("l_returnflag").eq(lit_s("O'Brien")), &s).unwrap();
        assert_eq!(sql, "(l_returnflag = 'O''Brien')");
        // A quote-and-terminate injection must not escape the literal.
        let sql = expr_to_lance_sql(&col("l_returnflag").eq(lit_s("x' OR '1'='1")), &s).unwrap();
        assert_eq!(sql, "(l_returnflag = 'x'' OR ''1''=''1')");
    }

    /// Dates render as DATE literals, not day numbers. Lance refuses a bare
    /// string ("could not convert to literal of type 'Date32'"), and an integer
    /// would silently depend on whose coercion rules win.
    #[test]
    fn test_dates_render_as_date_literals() {
        let s = filter_schema();
        // 1998-09-02 is 10471 days after the epoch.
        let d = crate::planner::Expr::literal(crate::planner::ScalarValue::Date32(10471));
        let sql = expr_to_lance_sql(&col("l_shipdate").lt_eq(d), &s).unwrap();
        assert_eq!(sql, "(l_shipdate <= DATE '1998-09-02')");
    }

    /// The type gate: a literal whose family does not match the column's is
    /// refused, because the two engines' coercion rules do not have to agree
    /// and a disagreement that narrows the filter loses rows.
    #[test]
    fn test_mismatched_literal_types_refuse_pushdown() {
        let s = filter_schema();
        // string literal against an integer column
        assert!(expr_to_lance_sql(&col("l_orderkey").eq(lit_s("5")), &s).is_none());
        // integer against a date column
        assert!(expr_to_lance_sql(&col("l_shipdate").eq(lit_i(10471)), &s).is_none());
        // anything against a decimal column: scale handling is not verified
        assert!(expr_to_lance_sql(&col("l_price").gt(lit_i(10)), &s).is_none());
        // an integer against a float column IS allowed, and is how TPC-H Q19
        // writes `l_quantity >= 1`
        assert_eq!(
            expr_to_lance_sql(&col("l_quantity").gt_eq(lit_i(1)), &s).unwrap(),
            "(l_quantity >= 1)"
        );
    }

    /// Column-to-column is allowed only when the types are identical, so no
    /// coercion happens on either side. This is what makes the TPC-H Q04/Q12/Q21
    /// predicate `l_commitdate < l_receiptdate` pushable.
    #[test]
    fn test_column_to_column_requires_identical_types() {
        let s = filter_schema();
        assert_eq!(
            expr_to_lance_sql(&col("l_commitdate").lt(col("l_shipdate")), &s).unwrap(),
            "(l_commitdate < l_shipdate)"
        );
        assert!(expr_to_lance_sql(&col("l_orderkey").lt(col("l_quantity")), &s).is_none());
    }

    /// LIKE is deliberately NOT pushed: the engine's pattern semantics against
    /// DataFusion's are unverified, and TPC-H uses NOT LIKE where an
    /// over-selective filter would silently delete rows.
    #[test]
    fn test_like_is_not_pushed() {
        use crate::planner::{BinaryOp, Expr};
        let s = filter_schema();
        let like = Expr::BinaryExpr {
            left: Box::new(col("l_returnflag")),
            op: BinaryOp::Like,
            right: Box::new(lit_s("R%")),
        };
        assert!(expr_to_lance_sql(&like, &s).is_none());
        // ...and an AND containing it still pushes the OTHER conjunct.
        let both = like.and(col("l_orderkey").gt(lit_i(3)));
        assert!(expr_to_lance_sql(&both, &s).is_none(), "whole-expr render");
        assert_eq!(
            lance_filter_sql(&both, &s).unwrap(),
            "(l_orderkey > 3)",
            "conjunct-wise render must keep the pushable half"
        );
    }

    /// An unrenderable conjunct may be dropped (the engine re-checks), but an
    /// unrenderable DISJUNCT may not: `a = 1 OR weird(b)` is not implied by
    /// `a = 1`, and pushing it would delete rows that satisfy the right half.
    #[test]
    fn test_unrenderable_disjunct_refuses_the_whole_predicate() {
        use crate::planner::{BinaryOp, Expr};
        let s = filter_schema();
        let like = Expr::BinaryExpr {
            left: Box::new(col("l_returnflag")),
            op: BinaryOp::Like,
            right: Box::new(lit_s("R%")),
        };
        let disj = col("l_orderkey").gt(lit_i(3)).or(like);
        assert!(
            lance_filter_sql(&disj, &s).is_none(),
            "an OR with an unrenderable arm must not be narrowed to its renderable arm"
        );
    }

    #[test]
    fn test_in_list_and_null_checks() {
        use crate::planner::Expr;
        let s = filter_schema();
        let in_list = Expr::InList {
            expr: Box::new(col("l_returnflag")),
            list: vec![lit_s("R"), lit_s("A")],
            negated: false,
        };
        assert_eq!(
            expr_to_lance_sql(&in_list, &s).unwrap(),
            "(l_returnflag IN ('R', 'A'))"
        );
        // One bad element poisons the whole list: a partial IN list is a
        // NARROWER predicate, which is the one direction that loses rows.
        let mixed = Expr::InList {
            expr: Box::new(col("l_returnflag")),
            list: vec![lit_s("R"), lit_i(7)],
            negated: false,
        };
        assert!(expr_to_lance_sql(&mixed, &s).is_none());

        let is_null = Expr::UnaryExpr {
            op: crate::planner::UnaryOp::IsNull,
            expr: Box::new(col("l_price")),
        };
        assert_eq!(
            expr_to_lance_sql(&is_null, &s).unwrap(),
            "(l_price IS NULL)"
        );
    }

    /// BETWEEN is desugared into two type-checked comparisons, so Lance's own
    /// BETWEEN semantics never enter the picture.
    #[test]
    fn test_between_is_desugared() {
        use crate::planner::Expr;
        let s = filter_schema();
        let b = Expr::Between {
            expr: Box::new(col("l_orderkey")),
            low: Box::new(lit_i(5)),
            high: Box::new(lit_i(9)),
            negated: false,
        };
        assert_eq!(
            expr_to_lance_sql(&b, &s).unwrap(),
            "((l_orderkey >= 5) AND (l_orderkey <= 9))"
        );
    }

    #[test]
    fn test_conjunct_splitting_keeps_every_renderable_part() {
        let s = filter_schema();
        let e = col("l_orderkey")
            .gt(lit_i(3))
            .and(col("l_returnflag").eq(lit_s("R")))
            .and(col("l_quantity").lt(lit_i(24)));
        assert_eq!(
            lance_filter_sql(&e, &s).unwrap(),
            "(l_orderkey > 3) AND (l_returnflag = 'R') AND (l_quantity < 24)"
        );
    }
}
