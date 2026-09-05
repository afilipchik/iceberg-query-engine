//! Execution context - main entry point for query execution

use crate::error::{QueryError, Result};
use crate::execution::{create_memory_pool, ExecutionConfig, SharedMemoryPool, SpillMetrics};
use crate::optimizer::Optimizer;
use crate::parser;
use crate::physical::operators::{MemoryTable, TableProvider};
use crate::physical::{PhysicalOperator, PhysicalPlanner, RecordBatchStream};
use crate::planner::{Binder, InMemoryCatalog, LogicalPlan, PlanSchema, SchemaField};
use crate::storage::native_delete;
use crate::storage::native_manifest;
use crate::storage::native_rollup;
use crate::storage::native_update;
use crate::storage::native_write::{self, NativeWriteMode};
use crate::storage::{NativeTable, ParquetTable};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Query execution result
#[derive(Debug)]
pub struct QueryResult {
    /// Output schema
    pub schema: SchemaRef,
    /// Result batches
    pub batches: Vec<RecordBatch>,
    /// Total row count
    pub row_count: usize,
    /// Execution metrics
    pub metrics: QueryMetrics,
}

/// Query execution metrics
#[derive(Debug, Default)]
pub struct QueryMetrics {
    /// Time spent parsing
    pub parse_time: Duration,
    /// Time spent planning
    pub plan_time: Duration,
    /// Time spent optimizing
    pub optimize_time: Duration,
    /// Time spent executing
    pub execute_time: Duration,
    /// Total time
    pub total_time: Duration,
    /// Peak memory usage during execution (bytes): the memory pool's
    /// high-water mark between this query's start and end (query-ui epic,
    /// task 001 — before that this was `pool.used()` read AFTER every
    /// reservation had been released, i.e. ~0 for any query that spilled).
    /// Pool-wide: concurrent queries sharing the context's pool all see the
    /// peak of their sum.
    pub peak_memory_bytes: usize,
    /// Number of result batches (before dictionary decoding, which is 1:1).
    pub batches: usize,
    /// Base tables the bound plan scans, sorted and deduplicated; when a
    /// rollup answered the query its name is included too, because it is
    /// what was actually read. Debugging surface for the query log
    /// (query-ui epic, task 001).
    pub tables: Vec<String>,
    /// `Display` of the optimized logical plan, capped at
    /// [`DEBUG_TEXT_CAP`] bytes. Rendered from the plan `sql()` already
    /// built — no second planning pass.
    pub optimized_plan: Option<String>,
    /// `display_plan` of the physical operator tree, same cap.
    pub physical_plan: Option<String>,
    /// Spill metrics if any spilling occurred
    pub spill_metrics: Option<SpillMetrics>,
    /// Number of files pruned by Iceberg statistics
    pub files_pruned_by_stats: usize,
    /// Number of files pruned by partition filter
    pub files_pruned_by_partition: usize,
    /// Names of every registered rollup that answered some part of this
    /// query (native-tables-rollups epic, task 001) — empty when no
    /// rollup was involved (including when none is registered at all).
    /// This is this task's structured PROVENANCE record: per the epic's
    /// G3 / the PRD's G5, whenever a rollup answers a query that fact
    /// must be observable, never silently indistinguishable from "the
    /// engine got faster at the real query." `QE_DEBUG_ROLLUP=1` also
    /// traces every match/no-match/staleness decision to stderr, matching
    /// this codebase's established diagnostic-switch convention (see
    /// `storage::native_rollup`).
    pub rollup_answered: Vec<String>,
}

/// What `ExecutionContext::create_table_as_select` produced.
#[derive(Debug, Clone)]
pub struct CreateTableAsSelectResult {
    /// The name the table was registered under (`ct.name` from the SQL).
    pub table_name: String,
    /// The native table's stable identity (UUID v4) — unchanged across a
    /// replace of a table that already existed at this name.
    pub table_id: String,
    /// The snapshot version the write committed (1 for a brand-new table,
    /// else the previous version + 1).
    pub version: u64,
    /// Rows written.
    pub rows: u64,
    /// Number of Arrow IPC segment files the table now has.
    pub segments: usize,
    /// The written table's schema (== the SELECT's output schema).
    pub schema: SchemaRef,
    /// Wall-clock time for the whole parse-bind-plan-execute-write-register
    /// sequence.
    pub elapsed: Duration,
}

/// What `ExecutionContext::insert_into_native_table` produced
/// (native-tables-mutation epic, task 002).
#[derive(Debug, Clone)]
pub struct InsertResult {
    /// The table name the INSERT targeted (`insert.table` from the SQL).
    pub table_name: String,
    /// The native table's stable identity (UUID v4) — unchanged by an
    /// INSERT.
    pub table_id: String,
    /// The snapshot version this INSERT committed. Equal to the
    /// PRE-insert version when the source produced zero rows (a
    /// legitimate no-op, not an error — see
    /// `native_write::append_to_native_table`'s doc).
    pub version: u64,
    /// Rows ADDED by this INSERT (0 for an empty source).
    pub rows_inserted: u64,
    /// Segments ADDED by this INSERT (0 for an empty source).
    pub segments_added: usize,
    /// The table's TOTAL row count after this INSERT.
    pub total_rows: u64,
    /// Every rollup registered against this table, eagerly refreshed (or
    /// attempted and failed — see `RollupRefreshOutcome::error`) as part
    /// of this INSERT (native-tables-rollups epic, task 003's
    /// refresh-on-write model — see `ExecutionContext::
    /// refresh_dependent_rollups`'s own doc). Empty when this table has
    /// no dependent rollups, OR when this INSERT was itself a no-op (an
    /// empty source never makes a rollup stale, so nothing is refreshed
    /// — see this task's own "cost avoidance for the common case").
    pub rollups_refreshed: Vec<RollupRefreshOutcome>,
    /// Wall-clock time for the whole parse-bind-plan-execute-write-
    /// refresh sequence.
    pub elapsed: Duration,
}

/// What `ExecutionContext::delete_from_native_table` produced
/// (native-tables-mutation epic, task 003).
#[derive(Debug, Clone)]
pub struct DeleteResult {
    /// The table name the DELETE targeted (`delete.from` from the SQL).
    pub table_name: String,
    /// The native table's stable identity (UUID v4) — unchanged by a
    /// DELETE.
    pub table_id: String,
    /// The snapshot version this DELETE committed. Equal to the PRE-delete
    /// version when the predicate matched zero rows (a legitimate no-op,
    /// not an error — see `native_delete::delete_from_native_table`'s
    /// doc).
    pub version: u64,
    /// Rows tombstoned by this DELETE (0 for a no-op match).
    pub rows_deleted: u64,
    /// Segments dropped entirely because every one of their rows became
    /// tombstoned by this DELETE (task 001's Decision 3).
    pub segments_dropped: usize,
    /// The table's TOTAL LOGICAL (post-delete, visible) row count after
    /// this DELETE.
    pub total_rows: u64,
    /// Every rollup registered against this table, eagerly refreshed (or
    /// attempted and failed — see `RollupRefreshOutcome::error`) as part
    /// of this DELETE (native-tables-rollups epic, task 003's
    /// refresh-on-write model — see `ExecutionContext::
    /// refresh_dependent_rollups`'s own doc). Empty when this table has
    /// no dependent rollups, OR when this DELETE matched zero rows (a
    /// no-op DELETE never makes a rollup stale, so nothing is
    /// refreshed).
    pub rollups_refreshed: Vec<RollupRefreshOutcome>,
    /// Wall-clock time for the whole parse-bind-identify-publish-refresh
    /// sequence.
    pub elapsed: Duration,
}

/// What `ExecutionContext::update_native_table` produced
/// (native-tables-mutation epic, task 004).
#[derive(Debug, Clone)]
pub struct UpdateResult {
    /// The table name the UPDATE targeted (`update.table` from the SQL).
    pub table_name: String,
    /// The native table's stable identity (UUID v4) — unchanged by an
    /// UPDATE.
    pub table_id: String,
    /// The snapshot version this UPDATE committed. Equal to the PRE-update
    /// version when the predicate matched zero LIVE rows (a legitimate
    /// no-op, not an error — see `native_update::update_native_table`'s
    /// doc).
    pub version: u64,
    /// Rows actually recomputed and rewritten — the NET count of LIVE
    /// matched rows (0 for a no-op match, or a match that only covers
    /// rows already tombstoned by a prior DELETE/UPDATE).
    pub rows_updated: u64,
    /// Old segments dropped entirely because every one of their rows
    /// became tombstoned by this UPDATE.
    pub segments_dropped: usize,
    /// New segments written to hold the recomputed rows.
    pub segments_added: usize,
    /// The table's TOTAL LOGICAL (post-update, visible) row count — always
    /// equal to the pre-update logical row count for a real update (an
    /// UPDATE never changes how many rows are live, only their values).
    pub total_rows: u64,
    /// Every rollup registered against this table, eagerly refreshed (or
    /// attempted and failed — see `RollupRefreshOutcome::error`) as part
    /// of this UPDATE (native-tables-rollups epic, task 003's
    /// refresh-on-write model — see `ExecutionContext::
    /// refresh_dependent_rollups`'s own doc). Empty when this table has
    /// no dependent rollups, OR when this UPDATE matched zero LIVE rows
    /// (a no-op UPDATE never makes a rollup stale, so nothing is
    /// refreshed).
    pub rollups_refreshed: Vec<RollupRefreshOutcome>,
    /// Wall-clock time for the whole parse-bind-identify-evaluate-write-
    /// publish-refresh sequence.
    pub elapsed: Duration,
}

/// What `ExecutionContext::register_rollup` produced (native-tables-
/// rollups epic, task 001).
#[derive(Debug, Clone)]
pub struct RegisterRollupResult {
    /// The name the rollup was registered under.
    pub rollup_name: String,
    /// The base table this rollup is defined against (must already be a
    /// registered native table — see `register_rollup`'s doc).
    pub base_table: String,
    /// The rollup's own native table's stable identity (UUID v4).
    pub table_id: String,
    /// The snapshot version the rollup's write committed.
    pub version: u64,
    /// Rows in the materialized rollup (one per distinct GROUP BY key
    /// combination the defining query computed).
    pub rows: u64,
    /// Number of Arrow IPC segment files the rollup now has.
    pub segments: usize,
    /// The written rollup table's schema (== the defining query's own
    /// output schema).
    pub schema: SchemaRef,
    /// Wall-clock time for the whole parse-bind-plan-execute-write-tag-
    /// register sequence.
    pub elapsed: Duration,
}

/// One dependent rollup's EAGER-refresh outcome (native-tables-rollups
/// epic, task 003), attached to `InsertResult`/`DeleteResult`/
/// `UpdateResult` so a mutation's effect on every rollup registered
/// against the table it just mutated is always observable — never
/// silently indistinguishable from "there were no rollups," matching
/// this epic's own G3/provenance discipline (`QueryMetrics::
/// rollup_answered`) extended to the write side. See
/// `ExecutionContext::refresh_dependent_rollups`'s own doc for the full
/// refresh model this records the outcome of.
#[derive(Debug, Clone)]
pub struct RollupRefreshOutcome {
    /// The rollup's registered name.
    pub rollup_name: String,
    /// `None` on a successful refresh: the rollup's data was fully
    /// recomputed and its manifest now records the just-mutated base
    /// table's NEW `(table_id, version)`, so it is immediately a
    /// matching candidate again for the rest of this session. `Some(
    /// message)` on failure: the rollup was left EXACTLY as it was
    /// (still recording its OLD, now-mismatched `base_table_version`),
    /// which is exactly what keeps it correctly excluded from matching
    /// by task 001's own staleness enforcement
    /// (`ExecutionContext::rollup_candidates`) — a failed refresh is
    /// never escalated into a failure of the base table's own mutation,
    /// which already succeeded and published atomically before this
    /// refresh was even attempted.
    pub error: Option<String>,
}

/// Execution context - manages tables and executes queries
pub struct ExecutionContext {
    catalog: InMemoryCatalog,
    tables: HashMap<String, Arc<dyn TableProvider>>,
    optimizer: Optimizer,
    /// Number of parallel partitions for execution (defaults to CPU count)
    parallel_partitions: usize,
    /// Memory pool for tracking and limiting memory usage
    memory_pool: SharedMemoryPool,
    /// Execution configuration
    config: ExecutionConfig,
    /// Directory `CREATE TABLE <name> AS SELECT ...` writes new native
    /// tables under, as `<native_table_root>/<name>`. SQL has no LOCATION
    /// clause here (refused by name — see `planner::binder`'s CreateTable
    /// validation), so the destination must come from context state rather
    /// than the statement text; overridable via `with_native_table_root`
    /// (tests/CLI point this at a specific directory), defaults to
    /// `./native_tables` so the REPL's zero-config case still works.
    native_table_root: PathBuf,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Pure parsing logic for `QE_INSERT_MERGE_CONCURRENCY`
/// (native-tables-mutation epic, task 005) — factored out of
/// `ExecutionContext::insert_merge_concurrency` so it is unit-testable
/// without mutating the real process environment (this crate's test
/// binaries run many `#[tokio::test]`s concurrently in one process; a test
/// that called `std::env::set_var` here would race every other test
/// reading the same key). Any absent, unparseable, or zero value falls
/// back to the default (8) — never a panic, never a zero-concurrency
/// merge (which would deadlock `flatten_unordered`'s `Some(0)` semantics).
fn parse_merge_concurrency(raw: Option<&str>) -> usize {
    raw.and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(8)
}

/// The formal name of the INSERT/CTAS write-path pre-flight admission check
/// (oom-safety-hardening epic, task 005). Appears verbatim in every refusal
/// message so a refused user (or a harness) can attribute the refusal to
/// THIS check by name, mirroring `NativeTable::check_scan_budget`'s own
/// named-refusal precedent (`src/storage/native_table.rs`).
const INSERT_ADMISSION_CHECK_NAME: &str = "insert/CTAS write-path admission check";

/// Decode-expansion factor applied to a parquet row group's on-disk
/// UNCOMPRESSED byte size (`RowGroupMetaData::total_byte_size`) to estimate
/// its decoded in-memory Arrow footprint plus per-stream pipeline slack.
///
/// Calibration (SF=10 `lineitem`, the measured workload behind
/// native-tables-mutation task 005's own 1.63GB-peak number): each of its
/// 58 row groups is ~59.1MB by `total_byte_size` but decodes to ~132MB of
/// Arrow buffers (dictionary/RLE parquet encodings stay compact even
/// "uncompressed" — measured 2.24x, via pyarrow `Table.nbytes` on a real
/// row group), and each in-flight partition stream additionally holds
/// read-ahead/transit batches. 3x covers both with a margin while keeping
/// the SF=10 estimate (8 streams x 59.1MB x 3 = ~1.42GB) comfortably under
/// a 2GiB memory_limit's budget (1.72GB) — the epic's own "no false
/// refusal" calibration point — and comfortably over a 512MiB limit's
/// budget (0.43GB), the SIGKILL case that must flip to a named refusal.
const INSERT_ADMISSION_DECODE_EXPANSION: u64 = 3;

/// Pure decision core of the INSERT/CTAS write-path admission check —
/// factored out of `ExecutionContext::check_insert_write_admission` so the
/// estimate arithmetic and the refusal message are unit-testable without a
/// real parquet source or a real context (same testability reasoning as
/// `parse_merge_concurrency` directly above).
///
/// The estimated working set is deliberately NOT the source's total size —
/// the write path streams row group by row group, so what is actually
/// resident at once is bounded by `bounded_partition_merge`'s concurrency
/// cap: at most `min(merge_concurrency, num_partitions)` partition streams
/// are polled concurrently, each holding roughly one decoded row group
/// (`StreamingParquetScanExec` partitions ARE row-group work lists).
///
///   estimate = min(merge_concurrency, num_partitions)
///              x max_row_group_bytes x INSERT_ADMISSION_DECODE_EXPANSION
///
/// `max_row_group_bytes == 0` means "no parquet-backed source found in the
/// plan" (a `VALUES` list, a memory table, a native-table source — whose
/// own scan is already budget-guarded by `check_scan_budget`): there is no
/// estimate basis and the statement is admitted, never refused on a guess.
///
/// Returns `Some(message)` when the statement must be refused, `None` when
/// it is admitted. The message names the check, both byte counts, and both
/// knobs (`--memory-limit`, `QE_INSERT_MERGE_CONCURRENCY`).
fn evaluate_insert_admission(
    statement_kind: &str,
    budget_bytes: u64,
    memory_limit_bytes: usize,
    merge_concurrency: usize,
    num_partitions: usize,
    max_row_group_bytes: u64,
) -> Option<String> {
    if max_row_group_bytes == 0 {
        return None;
    }
    let effective = merge_concurrency.min(num_partitions.max(1)).max(1) as u64;
    let per_stream = max_row_group_bytes.saturating_mul(INSERT_ADMISSION_DECODE_EXPANSION);
    let estimated = per_stream.saturating_mul(effective);
    if estimated <= budget_bytes {
        return None;
    }
    Some(format!(
        "{INSERT_ADMISSION_CHECK_NAME}: refusing {statement_kind} before driving any partition \
         stream: the estimated streaming working set of {estimated} bytes ({effective} \
         concurrently polled partition stream(s) x {per_stream} bytes per stream — the largest \
         source parquet row group of {max_row_group_bytes} bytes x \
         {INSERT_ADMISSION_DECODE_EXPANSION}x decode expansion) exceeds the memory safety \
         budget of {budget_bytes} bytes (memory_limit {memory_limit_bytes} bytes * \
         spill_threshold, the same formula NativeTable::check_scan_budget uses). Raise \
         --memory-limit / ExecutionConfig::memory_limit for this statement, or lower \
         QE_INSERT_MERGE_CONCURRENCY (currently {merge_concurrency}, the bounded-merge \
         concurrency cap) so fewer decoded row groups are resident at once."
    ))
}

/// Collect the table names of every `Scan` node reachable through
/// `LogicalPlan::children()` — the sources whose parquet row groups the
/// admission check estimates against. Plans embedded inside subquery
/// EXPRESSIONS are not visited (`children()` does not expose them — the
/// same documented boundary `native_rollup::substitute`'s walk has); the
/// check stays deliberately conservative-but-incomplete rather than
/// growing a second plan-traversal mechanism.
fn collect_scan_table_names(plan: &LogicalPlan, out: &mut Vec<String>) {
    if let LogicalPlan::Scan(node) = plan {
        if !out.contains(&node.table_name) {
            out.push(node.table_name.clone());
        }
    }
    for child in plan.children() {
        collect_scan_table_names(child, out);
    }
}

impl ExecutionContext {
    pub fn new() -> Self {
        let config = ExecutionConfig::default();
        let memory_pool = create_memory_pool(config.memory_limit);
        Self {
            catalog: InMemoryCatalog::new(),
            tables: HashMap::new(),
            optimizer: Optimizer::new(),
            parallel_partitions: rayon::current_num_threads(),
            memory_pool,
            config,
            native_table_root: PathBuf::from("./native_tables"),
        }
    }

    /// Create a new context with a specific memory limit
    pub fn with_memory_limit(max_bytes: usize) -> Self {
        let config = ExecutionConfig::default().with_memory_limit(max_bytes);
        let memory_pool = create_memory_pool(max_bytes);
        Self {
            catalog: InMemoryCatalog::new(),
            tables: HashMap::new(),
            optimizer: Optimizer::new(),
            parallel_partitions: rayon::current_num_threads(),
            memory_pool,
            config,
            native_table_root: PathBuf::from("./native_tables"),
        }
    }

    /// Create a new context with custom configuration
    pub fn with_config(config: ExecutionConfig) -> Self {
        let memory_pool = create_memory_pool(config.memory_limit);
        Self {
            catalog: InMemoryCatalog::new(),
            tables: HashMap::new(),
            optimizer: Optimizer::new(),
            parallel_partitions: rayon::current_num_threads(),
            memory_pool,
            config,
            native_table_root: PathBuf::from("./native_tables"),
        }
    }

    /// Override the directory `create_table_as_select` writes new native
    /// tables under (default `./native_tables`, relative to the process's
    /// CWD). Tests and CLI entrypoints that want a specific/temp location
    /// call this; the REPL's zero-config default just uses it as-is.
    pub fn with_native_table_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.native_table_root = root.into();
        self
    }

    /// The directory new native tables are (or would be) written under.
    pub fn native_table_root(&self) -> &Path {
        &self.native_table_root
    }

    /// Collect table statistics from all registered table providers
    fn collect_table_statistics(
        &self,
    ) -> HashMap<String, crate::physical::operators::TableStatistics> {
        let mut stats = HashMap::new();
        for (name, provider) in &self.tables {
            if let Some(table_stats) = provider.statistics() {
                stats.insert(name.clone(), table_stats);
            }
        }
        stats
    }

    /// Get the memory pool
    /// Opt this context into GPU aggregate offload (`--features gpu`).
    /// Single-process CLI entry points call it; serve and distributed
    /// contexts never do (their gates require byte-exact CPU answers).
    pub fn enable_gpu_offload(&mut self) {
        self.config.gpu_offload = true;
    }

    pub fn memory_pool(&self) -> &SharedMemoryPool {
        &self.memory_pool
    }

    /// Get the execution config
    pub fn config(&self) -> &ExecutionConfig {
        &self.config
    }

    /// Get current memory usage
    pub fn memory_used(&self) -> usize {
        self.memory_pool.used()
    }

    /// Get available memory
    pub fn memory_available(&self) -> usize {
        self.memory_pool.available()
    }

    /// Set the number of parallel partitions for execution
    pub fn with_parallel_partitions(mut self, partitions: usize) -> Self {
        self.parallel_partitions = partitions.max(1);
        self
    }

    /// Get the number of parallel partitions
    pub fn parallel_partitions(&self) -> usize {
        self.parallel_partitions
    }

    /// Register a table from record batches
    pub fn register_table(
        &mut self,
        name: impl Into<String>,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) {
        let name = name.into();

        // Register with catalog for planning
        let plan_schema = arrow_schema_to_plan_schema(&schema);
        self.catalog.register_table(name.clone(), plan_schema);

        // Register table provider for execution
        let table = Arc::new(MemoryTable::new(schema, batches));
        self.tables.insert(name, table);
    }

    /// Register a table from a single batch
    pub fn register_batch(&mut self, name: impl Into<String>, batch: RecordBatch) {
        let schema = batch.schema();
        self.register_table(name, schema, vec![batch]);
    }

    /// Register a custom table provider
    ///
    /// This allows registering any type that implements TableProvider,
    /// such as ParquetTable or IcebergTable.
    pub fn register_table_provider(
        &mut self,
        name: impl Into<String>,
        provider: Arc<dyn TableProvider>,
    ) {
        let name = name.into();

        // Register schema with catalog for planning
        let plan_schema = arrow_schema_to_plan_schema(&provider.schema());
        self.catalog.register_table(name.clone(), plan_schema);

        // Store provider for execution
        self.tables.insert(name, provider);
    }

    /// Register a table from Parquet file(s)
    ///
    /// If path points to a file, loads that single Parquet file.
    /// If path points to a directory, loads all .parquet files in it.
    pub fn register_parquet(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let table = ParquetTable::try_new(path)?;
        self.register_table_provider(name, Arc::new(table));
        Ok(())
    }

    /// Register an Apache Iceberg table directory, optionally at a specific
    /// snapshot (time travel). The provider is the ordinary ParquetTable over
    /// the snapshot's manifest-listed data files, so every Parquet-path
    /// capability — including distributed split enumeration — applies.
    pub fn register_iceberg(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        snapshot_id: Option<i64>,
    ) -> Result<()> {
        let opened = crate::storage::open_iceberg_table(path, snapshot_id)?;
        self.register_table_provider(name, Arc::new(opened.table));
        Ok(())
    }

    /// Register a table from a Lance dataset directory (e.g. `orders.lance`).
    ///
    /// Requires the `lance` cargo feature. Column projection is pushed into
    /// Lance; see `storage::LanceTable` for what the Lance path does and does
    /// not get relative to the Parquet path.
    #[cfg(feature = "lance")]
    pub fn register_lance(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let table = crate::storage::LanceTable::try_new(path)?;
        self.register_table_provider(name, Arc::new(table));
        Ok(())
    }

    /// Register a HISTORICAL version of a Lance dataset — time travel.
    ///
    /// Lance commits a new manifest on every append, overwrite, delete and
    /// index build, keeping the old ones, so `version` selects a past snapshot
    /// of the same path. The table is immutable at that version.
    ///
    /// Registering one path twice under two names and two versions is a
    /// supported way to diff snapshots in a single query:
    ///
    /// ```ignore
    /// ctx.register_lance_version("t_old", "./data/t.lance", 1)?;
    /// ctx.register_lance("t_new", "./data/t.lance")?;
    /// ctx.sql("SELECT COUNT(*) FROM t_new WHERE id NOT IN (SELECT id FROM t_old)").await?;
    /// ```
    ///
    /// An unknown version is an error, never a silent fall back to the latest.
    #[cfg(feature = "lance")]
    pub fn register_lance_version(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        version: u64,
    ) -> Result<()> {
        let table = crate::storage::LanceTable::try_new_at_version(path, version)?;
        self.register_table_provider(name, Arc::new(table));
        Ok(())
    }

    /// Register a Lance dataset and immediately compute its column statistics.
    ///
    /// Same as [`register_lance`](Self::register_lance) except the statistics
    /// scan (which Lance, unlike Parquet, cannot answer from metadata) is paid
    /// here rather than inside whichever query first invokes the optimizer.
    /// Benchmarks use this so load cost is reported as load cost.
    #[cfg(feature = "lance")]
    pub fn register_lance_warm(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let table = crate::storage::LanceTable::try_new(path)?;
        table.warm_statistics();
        self.register_table_provider(name, Arc::new(table));
        Ok(())
    }

    /// Register an existing native table directory (task 002/003's manifest
    /// + Arrow IPC segment format) — mirrors `register_iceberg`/
    /// `register_lance`. Reads and fully validates `_manifest.json`; a
    /// missing or corrupt manifest is a clear `Err`.
    pub fn register_native_table(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        // `NativeTable::scan()` is not spill-aware (task 006 measured this
        // directly: a 60M-row native table's full-table scan needed ~1.6GB
        // peak RSS and was OOM-killed under a bare 1GiB cgroup cap, while
        // the identical query over the identical data as plain Parquet
        // finished in 109ms — see `native_table.rs`'s module doc). Give it
        // the SAME admission-control budget `spillable.rs` already computes
        // at 7 call sites (`memory_limit * spill_threshold`) so a scan that
        // plainly cannot fit refuses cleanly instead of risking OOM.
        let budget = (self.config.memory_limit as f64 * self.config.spill_threshold) as u64;
        let table = NativeTable::try_new(path)?.with_memory_budget(Some(budget));
        self.register_table_provider(name, Arc::new(table));
        Ok(())
    }

    /// Build a snapshot of every registered, NON-STALE rollup — the real
    /// registry access `storage::native_rollup`'s pure matching functions
    /// cannot have themselves (see that module's doc for why this can't
    /// live inside an `OptimizerRule`). A rollup is excluded here
    /// (exactly as if it had never been registered) whenever:
    /// - it is not (or is no longer) a native table with `manifest().
    ///   rollup` populated,
    /// - its recorded base table is not currently registered, or is
    ///   registered but is not (or is no longer) a native table, or
    /// - the base table's CURRENT `(table_id, snapshot.version)` differs
    ///   from what this rollup recorded — this IS task 001's staleness
    ///   enforcement; the model that keeps `RollupMeta` current
    ///   automatically on every base-table mutation is task 003's job.
    ///
    /// `QE_DEBUG_ROLLUP=1` traces every exclusion to stderr, matching
    /// this codebase's established diagnostic-switch convention.
    fn rollup_candidates(&self) -> Vec<native_rollup::RollupCandidate> {
        let debug = std::env::var("QE_DEBUG_ROLLUP").is_ok();
        let mut out = Vec::new();
        for (name, provider) in &self.tables {
            let Some(native) = provider.as_any().downcast_ref::<NativeTable>() else {
                continue;
            };
            let Some(meta) = native.manifest().rollup.clone() else {
                continue;
            };
            let Some(base_provider) = self.tables.get(&meta.base_table) else {
                if debug {
                    eprintln!(
                        "[rollup] `{name}`: base table `{}` is not registered in this \
                         session -- excluded (stale)",
                        meta.base_table
                    );
                }
                continue;
            };
            let Some(base_native) = base_provider.as_any().downcast_ref::<NativeTable>() else {
                if debug {
                    eprintln!(
                        "[rollup] `{name}`: base table `{}` is not a native table -- excluded \
                         (stale)",
                        meta.base_table
                    );
                }
                continue;
            };
            let base_manifest = base_native.manifest();
            if base_manifest.table_id != meta.base_table_id
                || base_manifest.snapshot.version != meta.base_table_version
            {
                if debug {
                    eprintln!(
                        "[rollup] `{name}`: STALE -- base table `{}` is now at (table_id={}, \
                         version={}), rollup recorded (table_id={}, version={})",
                        meta.base_table,
                        base_manifest.table_id,
                        base_manifest.snapshot.version,
                        meta.base_table_id,
                        meta.base_table_version
                    );
                }
                continue;
            }
            out.push(native_rollup::RollupCandidate {
                registered_name: name.clone(),
                meta,
                schema: arrow_schema_to_plan_schema(&native.schema()),
            });
        }
        out
    }

    /// Apply rollup substitution to a freshly-bound logical plan, given
    /// the real registry access [`Self::rollup_candidates`] has — see
    /// `storage::native_rollup`'s module doc for why this cannot be an
    /// `OptimizerRule` and must instead run here, in `ExecutionContext`'s
    /// own flow, positioned BEFORE `Optimizer::optimize()` (not after or
    /// inside it — matching against the RAW bound plan, before any
    /// optimizer rewrite, is a deliberate design decision documented in
    /// full in that module). Structurally a no-op, with zero plan-tree
    /// walk at all, when no rollup is registered: `rollup_candidates`
    /// returns empty and this short-circuits before calling
    /// `native_rollup::substitute` — a rollup is additive, so native
    /// tables/mutation must (and do) behave identically whether or not
    /// any rollup is registered against them.
    fn substitute_rollups(&self, plan: LogicalPlan) -> Result<(LogicalPlan, Vec<String>)> {
        let candidates = self.rollup_candidates();
        if candidates.is_empty() {
            return Ok((plan, Vec::new()));
        }
        let mut matched = Vec::new();
        let rewritten = native_rollup::substitute(&plan, &candidates, &mut matched)?;
        Ok((rewritten, matched))
    }

    /// Register a materialized rollup of an existing, already-registered
    /// NATIVE base table (native-tables-rollups epic, task 001's
    /// programmatic registration API — SQL DDL is task 002's job, not
    /// attempted here). `defining_sql` must bind to exactly `SELECT
    /// <GROUP BY column(s) and/or aggregate(s), any order, any aliases>
    /// FROM <base_table> GROUP BY <...>` — no WHERE/JOIN/HAVING/ORDER
    /// BY/LIMIT/DISTINCT, and every SELECT item must be a bare
    /// (optionally aliased) reference to one of the GROUP BY columns or
    /// aggregates, never a computed expression over them (see
    /// `storage::native_rollup::require_rollup_defining_shape` for the
    /// exact, enforced rule and its rationale).
    ///
    /// Sequence: (1) confirm `base_table` is already a registered native
    /// table (needed for staleness bookkeeping — a rollup over a plain
    /// parquet/Iceberg/Lance table has no version concept to compare
    /// against, so this epic requires a native base table); (2) parse +
    /// bind the defining query and validate/recognize its shape against
    /// the RAW bound plan (the exact same recognition an incoming query
    /// is later matched against — see `storage::native_rollup`'s module
    /// doc for why); (3) plan, optimize and execute it exactly like
    /// [`Self::create_table_as_select`] (a rollup's row data IS a native
    /// table — this reuses `native_write::write_batches` completely
    /// UNCHANGED); (4) attach the resulting `RollupMeta` (base table
    /// identity + canonical GROUP BY/aggregate keys + physical column
    /// mapping) to the just-published manifest via a SECOND, small,
    /// manifest-only atomic patch (`native_manifest::write_manifest_atomic`
    /// — the SAME single-file atomic-rename primitive the mutation
    /// epic's Append/DELETE/UPDATE paths already established for exactly
    /// this "patch just the manifest" shape); (5) register the rollup
    /// normally, so it is immediately queryable AND a substitution
    /// candidate for the rest of this session.
    ///
    /// `&mut self`, unlike `sql()`'s `&self`: registering the new rollup
    /// mutates `self.tables`/`self.catalog`, and a genuinely streaming
    /// write needs the defining query's stream driven directly rather
    /// than via `sql()`'s collecting wrapper — mirrors
    /// `create_table_as_select`'s own reasoning exactly.
    ///
    /// An existing native table at the target directory is replaced
    /// wholesale (`NativeWriteMode::Overwrite`), matching
    /// `create_table_as_select`'s own re-registration/replace semantics —
    /// re-registering a rollup under a name that already holds a rollup
    /// (or any native table) recomputes it from scratch.
    pub async fn register_rollup(
        &mut self,
        name: impl Into<String>,
        base_table: impl Into<String>,
        defining_sql: &str,
    ) -> Result<RegisterRollupResult> {
        let name = name.into();
        let base_table = base_table.into();
        let start = Instant::now();

        let base_provider = self.tables.get(&base_table).cloned().ok_or_else(|| {
            QueryError::TableNotFound(format!(
                "register_rollup: base table `{base_table}` is not registered in this session"
            ))
        })?;
        let base_native = base_provider
            .as_any()
            .downcast_ref::<NativeTable>()
            .ok_or_else(|| {
                QueryError::InvalidArgument(format!(
                    "register_rollup: base table `{base_table}` is not a native table -- a \
                     rollup's staleness bookkeeping needs a real (table_id, version) pair to \
                     compare against, which only a native table's manifest provides"
                ))
            })?;
        let base_table_id = base_native.manifest().table_id.clone();
        let base_table_version = base_native.manifest().snapshot.version;

        let stmt = parser::parse_sql(defining_sql)?;
        let mut binder = Binder::new(&self.catalog);
        // The RAW bound plan -- exactly what an incoming query is later
        // matched against (see storage::native_rollup's module doc).
        let logical = binder.bind(&stmt)?;
        let recognized = native_rollup::require_rollup_defining_shape(&logical, &base_table)?;

        let table_stats = self.collect_table_statistics();
        let optimized = if table_stats.is_empty() {
            self.optimizer.optimize(logical)?
        } else {
            let optimizer = Optimizer::new().with_table_statistics(table_stats);
            optimizer.optimize(logical)?
        };
        if std::env::var("PLAN_DEBUG").is_ok() {
            eprintln!("[plan]\n{}", optimized);
        }

        let mut planner =
            PhysicalPlanner::with_config(self.memory_pool.clone(), self.config.clone());
        for (tname, provider) in &self.tables {
            planner.register_table(tname.clone(), provider.clone());
        }
        planner.enable_subquery_execution();
        let physical = planner.create_physical_plan(&optimized)?;
        // Same qualification-stripping/dictionary-normalizing schema
        // create_table_as_select computes, for the identical reason (a
        // persisted table's column names must be plain, not internally
        // qualified) -- see that function's own call-site comment.
        let write_schema = output_schema_for_native_write(&physical.schema())?;

        let num_partitions = physical.output_partitions().max(1);
        let mut streams: Vec<RecordBatchStream> = Vec::with_capacity(num_partitions);
        for partition_id in 0..num_partitions {
            let stream = physical.execute(partition_id).await.map_err(|e| {
                QueryError::Execution(format!("Partition {partition_id} execution failed: {e}"))
            })?;
            let out_schema = write_schema.clone();
            let decoded: RecordBatchStream = Box::pin(stream.and_then(move |b| {
                let out_schema = out_schema.clone();
                async move {
                    let b = decode_dictionary_batch(b)?;
                    RecordBatch::try_new(out_schema, b.columns().to_vec())
                        .map_err(|e| QueryError::Execution(e.to_string()))
                }
            }));
            streams.push(decoded);
        }
        let merged: RecordBatchStream =
            Self::bounded_partition_merge(streams, Self::insert_merge_concurrency());

        let out_dir = self.native_table_root.join(&name);
        let mode = if native_manifest::is_native_table_dir(&out_dir) {
            NativeWriteMode::Overwrite
        } else {
            NativeWriteMode::Create
        };
        // `write_batches`'s own return value (table_id/version/rows/
        // segments) is superseded by the manifest read back below, which
        // carries the identical data PLUS the `rollup` field this
        // function still needs to attach -- no reason to bind it here.
        native_write::write_batches(merged, write_schema.clone(), &out_dir, mode)
            .await
            .map_err(|e| QueryError::Storage(format!("register_rollup {name}: {e}")))?;

        // Attach rollup metadata: the just-written table's columns are
        // already in the SAME order `recognized.proj_slots` recorded
        // (writing never reorders/drops columns), so this is a plain
        // positional zip -- see `build_rollup_columns`'s own doc.
        let columns = native_rollup::build_rollup_columns(&recognized, &write_schema)?;
        let meta = native_manifest::RollupMeta {
            base_table: base_table.clone(),
            defining_sql: defining_sql.to_string(),
            base_table_id,
            base_table_version,
            columns,
        };
        let manifest = native_manifest::read_manifest(&out_dir)
            .map_err(|e| QueryError::Storage(format!("register_rollup {name}: {e}")))?
            .with_rollup(meta);
        native_manifest::write_manifest_atomic(&out_dir, &manifest)
            .map_err(|e| QueryError::Storage(format!("register_rollup {name}: {e}")))?;

        // Make the freshly written, now rollup-tagged table immediately
        // queryable AND a substitution candidate for the rest of this
        // session.
        self.register_native_table(&name, &out_dir)?;

        Ok(RegisterRollupResult {
            rollup_name: name,
            base_table,
            table_id: manifest.table_id.clone(),
            version: manifest.snapshot.version,
            rows: manifest.snapshot.row_count,
            segments: manifest.segments.len(),
            schema: write_schema,
            elapsed: start.elapsed(),
        })
    }

    /// Eagerly, synchronously refresh every rollup currently registered
    /// against `base_table_name`, immediately after that base table's own
    /// INSERT/DELETE/UPDATE has already published successfully and been
    /// re-registered (native-tables-rollups epic, task 003 — the
    /// refresh-on-write model the epic's own Architecture Decisions call
    /// for: "either eagerly recompute dependent rollups inline or mark
    /// them stale for lazy recompute on next match attempt -- task 003's
    /// call, documented explicitly either way").
    ///
    /// # Why EAGER, not LAZY (a deliberate, explicit choice)
    ///
    /// Task 001 already built the STALENESS bookkeeping: the instant a
    /// base table's `(table_id, version)` changes, every rollup that
    /// still records the OLD pair is excluded from matching by
    /// [`Self::rollup_candidates`] -- that half of "never serve stale
    /// data silently" already existed before this task. What was missing
    /// is making a rollup ACTUALLY CURRENT again without a human
    /// re-running `register_rollup`/`CREATE MATERIALIZED VIEW` by hand.
    /// Two models were considered, per this task's own requirement to
    /// decide and document explicitly, not default into one:
    ///
    /// - **LAZY** ("recompute on next match attempt before answering from
    ///   it") has no viable call site in this codebase as it stands: the
    ///   ONLY place a rollup is ever MATCHED is [`Self::substitute_rollups`],
    ///   called from [`Self::sql`] and [`Self::optimized_plan`] -- both
    ///   `&self`. `register_rollup` (the only refresh mechanism that
    ///   exists, reused UNCHANGED by this task) is `&mut self` (it
    ///   mutates `self.tables`/`self.catalog`). Making a lazy recompute
    ///   possible would require either changing `sql()`'s own signature to
    ///   `&mut self` -- a large, invasive change reaching the HTTP `/sql`
    ///   handler, Arrow Flight, the REPL, and every existing caller of
    ///   `sql()` in this codebase, far outside an S-M task's risk budget
    ///   -- or wrapping `self.tables` in interior mutability
    ///   (`Arc<RwLock<..>>`/`Mutex`) so a `&self` method could still
    ///   mutate it -- a genuinely NEW concurrency-control surface this
    ///   codebase does not have today, exactly the kind of new
    ///   infrastructure this epic's own Architecture Decisions steer away
    ///   from (said there about a background scheduler specifically --
    ///   "no new process/thread lifecycle management" -- but the same
    ///   reasoning applies to inventing new locking around the table
    ///   registry just to make a read path able to mutate it).
    /// - **EAGER** (recompute inline, synchronously, as part of the
    ///   mutation call itself returning) has a natural, minimal-risk home:
    ///   `insert_into_native_table`/`delete_from_native_table`/
    ///   `update_native_table` are ALREADY `&mut self`, ALREADY `async
    ///   fn`, and ALREADY re-register the just-mutated base table before
    ///   returning (so `self.tables` reflects its NEW version by the time
    ///   this method runs, which matters -- see below) -- calling
    ///   `register_rollup` again right there is a direct extension of a
    ///   pattern that already exists, not new infrastructure. This also
    ///   matches the epic's own "always correct, even if that means not
    ///   fast" Architecture Decision most literally: by the time a
    ///   mutation call RETURNS, every dependent rollup is either already
    ///   fresh or has been left safely excluded from matching (see the
    ///   failure case below) -- there is no window, observable from the
    ///   SQL surface, where a rollup sits in a "known-stale, not yet being
    ///   recomputed" limbo the way a lazy model would have between a
    ///   mutation and the NEXT query that happens to attempt to match it.
    ///
    /// **Chosen: EAGER.** The cost (this task's own required performance
    /// measurement, see `.claude/epics/native-tables-rollups/003.md`'s
    /// Outcome) is a FULL recompute of every dependent rollup -- the SAME
    /// mechanism a manual `register_rollup` re-run performs, reused
    /// completely UNCHANGED, proportional to the size of the (new) base
    /// table, NOT to the mutation's own delta size -- documented and
    /// measured, not hidden.
    ///
    /// # Failure handling -- never escalated into a mutation failure
    ///
    /// A rollup whose refresh fails for any reason (I/O error, disk full,
    /// permission denied, ...) is left EXACTLY as it was:
    /// `register_rollup`'s own write path (`native_write::write_batches`
    /// in `Overwrite` mode, since the rollup's own directory already
    /// exists from its original registration) stages a complete
    /// replacement in a sibling staging directory and only atomically
    /// publishes it as the very last step, so any error before that point
    /// leaves the rollup's existing manifest (still recording its OLD,
    /// now-mismatched `base_table_version`) completely untouched -- which
    /// is EXACTLY what keeps it correctly excluded from matching by task
    /// 001's own `rollup_candidates` staleness check until a future
    /// refresh succeeds. This failure is reported back to the caller (see
    /// [`RollupRefreshOutcome`]) rather than turning a successful
    /// base-table mutation (already published atomically, before this
    /// method is ever called) into a reported error just because a
    /// DERIVED, secondary artifact could not be recomputed.
    ///
    /// # Memory safety
    ///
    /// This method itself holds nothing but small `String`s (table/rollup
    /// names, defining SQL text) -- no row data ever touches it directly.
    /// The actual recompute reuses `register_rollup`'s own already-bounded
    /// pipeline UNCHANGED: physical execution streams through the SAME
    /// spillable operators every other query uses, and the write side is
    /// the SAME bounded, ~one-segment-at-a-time streaming writer
    /// (`native_write::write_batches`) task 003 of native-tables-
    /// foundation already measured at ~400MB peak RSS independent of
    /// source scale. The one place "old and new" genuinely coexist is ON
    /// DISK, not in memory: the rollup's OLD live directory and a NEW
    /// staging directory both exist briefly until `publish_table_dir`'s
    /// atomic rename swaps them -- the same pre-existing bounded-overlap
    /// behavior every Create/Overwrite/re-`register_rollup` already has,
    /// not a new pattern this task introduces. No new unbounded-memory
    /// path is added by this method.
    ///
    /// When a base table has SEVERAL dependent rollups, they are
    /// refreshed SEQUENTIALLY, one full `register_rollup` call completing
    /// before the next begins -- a deliberate choice, not an oversight:
    /// running them concurrently (e.g. `futures::future::join_all`) would
    /// let N rollups' worth of physical-execution working sets (spillable
    /// operators, buffered write segments) all be resident at once,
    /// multiplying peak memory by however many rollups happen to be
    /// registered -- exactly the kind of new unbounded-with-N-rollups
    /// memory path this section just said does not exist. Sequential
    /// execution keeps peak memory bounded to a SINGLE recompute's
    /// footprint regardless of how many rollups depend on the mutated
    /// table, at the cost of wall-clock time scaling with rollup count
    /// (measured directly, not assumed -- see this task's own
    /// performance measurement).
    ///
    /// # Cost avoidance for the common case
    ///
    /// Returns immediately (one cheap scan of `self.tables`, no recompute
    /// at all) when `base_table_name` has no dependent rollups -- the
    /// overwhelming majority of mutations against this codebase's own
    /// existing test suite, and the common case for any table with no
    /// rollups registered against it at all. Every call site additionally
    /// skips calling this altogether when a mutation was itself a genuine
    /// no-op (see `insert_into_native_table`/`delete_from_native_table`/
    /// `update_native_table`'s own zero-rows-changed checks) -- a rollup
    /// cannot have gone stale from a mutation that changed nothing, so
    /// there is nothing to refresh.
    ///
    /// # Scope boundary: direct dependents only, not chains
    ///
    /// Only rollups whose OWN recorded `base_table` is `base_table_name`
    /// are refreshed. A rollup-of-a-rollup (registering a second rollup
    /// with `base_table` = another rollup's own name -- structurally
    /// possible, since a rollup's data IS an ordinary native table, but
    /// never built or tested by tasks 001/002) is NOT automatically
    /// cascaded beyond one hop: refreshing rollup A (based on table T)
    /// does not also refresh a hypothetical rollup B based on A. This is a
    /// deliberate, named scope boundary, not a silent gap -- chained
    /// rollups are outside every task this epic has shipped so far.
    async fn refresh_dependent_rollups(
        &mut self,
        base_table_name: &str,
    ) -> Vec<RollupRefreshOutcome> {
        // Collect (rollup_name, base_table, defining_sql) BEFORE taking
        // any &mut self action below -- this borrows self.tables
        // immutably, and the borrow must end (the Vec below owns Strings,
        // borrowing nothing from self) before the subsequent
        // register_rollup calls (&mut self) are legal.
        let mut dependents: Vec<(String, String, String)> = self
            .tables
            .iter()
            .filter_map(|(name, provider)| {
                let native = provider.as_any().downcast_ref::<NativeTable>()?;
                let meta = native.manifest().rollup.as_ref()?;
                meta.base_table
                    .eq_ignore_ascii_case(base_table_name)
                    .then(|| {
                        (
                            name.clone(),
                            meta.base_table.clone(),
                            meta.defining_sql.clone(),
                        )
                    })
            })
            .collect();
        // Deterministic order -- not load-bearing for correctness (every
        // dependent is refreshed independently of the others), but makes
        // a multi-rollup test's `rollups_refreshed` order stable rather
        // than dependent on HashMap iteration order.
        dependents.sort();

        let debug = std::env::var("QE_DEBUG_ROLLUP").is_ok();
        let mut outcomes = Vec::with_capacity(dependents.len());
        for (rollup_name, base_table, defining_sql) in dependents {
            match self
                .register_rollup(rollup_name.clone(), base_table, &defining_sql)
                .await
            {
                Ok(_) => {
                    if debug {
                        eprintln!(
                            "[rollup] REFRESH: `{rollup_name}` recomputed after a mutation to \
                             `{base_table_name}`"
                        );
                    }
                    outcomes.push(RollupRefreshOutcome {
                        rollup_name,
                        error: None,
                    });
                }
                Err(e) => {
                    if debug {
                        eprintln!(
                            "[rollup] REFRESH FAILED for `{rollup_name}` after a mutation to \
                             `{base_table_name}` -- left stale, will correctly fall back to the \
                             base table until a future refresh succeeds: {e}"
                        );
                    }
                    outcomes.push(RollupRefreshOutcome {
                        rollup_name,
                        error: Some(e.to_string()),
                    });
                }
            }
        }
        outcomes
    }

    /// Execute `CREATE MATERIALIZED VIEW <name> AS SELECT ...` end to end
    /// (native-tables-rollups epic, task 002): parse, validate + bind the
    /// statement via the ordinary `Binder::bind()` path (refuses every
    /// unsupported `CreateView` clause by name and requires `materialized
    /// == true` -- see `planner::binder`'s
    /// `require_supported_create_view_shape`), recover the defining
    /// query's own base table from that SAME bound plan via
    /// `native_rollup::recognize` (task 001's own recognition function,
    /// reused completely UNCHANGED -- guarantees this layer and
    /// `register_rollup`'s own internal re-derivation can never disagree
    /// about which table a rollup is defined against), then hands
    /// `(rollup_name, base_table, defining_sql)` to task 001's
    /// `register_rollup`, which does all the real work (re-parses/
    /// re-binds/plans/optimizes/executes/writes/registers). This method is
    /// purely a DDL front end onto that mechanism, per this task's own
    /// scope -- it never itself touches the plan/execute/write pipeline.
    ///
    /// `&mut self`, unlike `sql()`'s `&self`: `register_rollup` mutates
    /// `self.tables`/`self.catalog`, exactly like `create_table_as_select`.
    ///
    /// `IF NOT EXISTS` is refused by name
    /// (`require_supported_create_view_shape`), not silently accepted and
    /// ignored -- see that function's own doc for why (mirrors `CREATE
    /// TABLE`'s identical `ct.if_not_exists` precedent exactly). A rollup
    /// registered under a name that already holds a rollup (or any native
    /// table) is replaced wholesale, matching `register_rollup`'s/
    /// `create_table_as_select`'s own re-registration semantics -- this is
    /// deliberately NOT what `IF NOT EXISTS` asks for (skip silently if it
    /// already exists), which is exactly why this epic refuses the clause
    /// outright rather than mapping it onto that different behavior.
    pub async fn create_materialized_view(&mut self, sql: &str) -> Result<RegisterRollupResult> {
        let stmt = parser::parse_sql(sql)?;
        let rollup_name =
            crate::planner::create_materialized_view_target_name(&stmt).ok_or_else(|| {
                QueryError::InvalidArgument(format!(
                    "create_materialized_view expects a CREATE MATERIALIZED VIEW <name> AS \
                     SELECT ... statement, got: {sql}"
                ))
            })?;

        let sqlparser::ast::Statement::CreateView(cv) = &stmt else {
            unreachable!(
                "create_materialized_view_target_name only returns Some for a \
                 Statement::CreateView"
            )
        };

        let mut binder = Binder::new(&self.catalog);
        // Validates the CREATE VIEW's shape (refusing every unsupported
        // clause by name, requiring `materialized`) AND binds the inner
        // SELECT in one call: see `Binder::bind()`'s `Statement::CreateView`
        // arm. The resulting `LogicalPlan` is used ONLY to recover the base
        // table name below -- `register_rollup` independently re-parses and
        // re-binds `defining_sql` itself (deliberately not refactored into
        // a shared helper under this task's time budget, mirroring task
        // 001's own established precedent for this exact tradeoff -- see
        // that method's own doc).
        let logical = binder.bind(&stmt)?;

        let recognized = native_rollup::recognize(&logical).ok_or_else(|| {
            QueryError::NotImplemented(format!(
                "CREATE MATERIALIZED VIEW {rollup_name}: the defining query must bind to \
                 exactly `SELECT <GROUP BY column(s) and/or aggregate(s), any order, any \
                 aliases> FROM <table> GROUP BY <...>` — no WHERE/JOIN/HAVING/ORDER BY/LIMIT/\
                 DISTINCT, every SELECT item a bare (optionally aliased) column reference, and \
                 the FROM table itself must be unaliased (native-tables-rollups epic's \
                 deliberate exact-match-only scope — see \
                 native_rollup::require_rollup_defining_shape)"
            ))
        })?;

        let defining_sql = cv.query.to_string();
        self.register_rollup(rollup_name, recognized.table_name, &defining_sql)
            .await
    }

    /// Execute a SQL query and return results
    pub async fn sql(&self, query: &str) -> Result<QueryResult> {
        let start = Instant::now();
        let mut metrics = QueryMetrics::default();
        // Open this query's peak-memory window (see `QueryMetrics::peak_memory_bytes`).
        self.memory_pool.reset_peak();

        // Parse
        let parse_start = Instant::now();
        let stmt = parser::parse_sql(query)?;
        metrics.parse_time = parse_start.elapsed();

        // `CREATE TABLE ... AS SELECT` needs `&mut self` (registering the
        // written table mutates `self.tables`/`self.catalog`) and a
        // genuinely streaming write (see `create_table_as_select`'s doc).
        // Both are incompatible with this method's `&self`, fully-
        // materializing signature, so this is refused HERE rather than
        // silently binding just the inner SELECT and returning ITS rows —
        // which is what would happen if this fell through to the ordinary
        // bind/plan/execute path below: the statement would appear to
        // "succeed" while never writing or registering anything.
        if crate::planner::create_table_target_name(&stmt).is_some() {
            return Err(QueryError::InvalidArgument(
                "CREATE TABLE ... AS SELECT must run through \
                 ExecutionContext::create_table_as_select (the REPL calls this \
                 automatically) — sql() cannot register the result (it takes &self) and \
                 would otherwise silently execute only the inner SELECT without writing \
                 or registering anything"
                    .to_string(),
            ));
        }

        // Same reasoning, same fix shape, for `INSERT INTO ... SELECT/
        // VALUES ...` (native-tables-mutation epic, task 002): now that
        // `Binder::bind()` accepts `Statement::Insert` (binding ONLY the
        // source query, exactly like CREATE TABLE), an unguarded INSERT
        // reaching the ordinary bind/plan/execute path below would
        // silently run just the SOURCE query and return ITS rows as a
        // "successful" result — never writing a single row to the target
        // native table. `insert_into_native_table` needs `&mut self` for
        // the same reasons `create_table_as_select` does (re-registering
        // the table so its new rows are visible in this session) and a
        // genuinely streaming write, both incompatible with this method's
        // `&self` signature.
        if crate::planner::insert_target_name(&stmt).is_some() {
            return Err(QueryError::InvalidArgument(
                "INSERT INTO ... must run through \
                 ExecutionContext::insert_into_native_table (the REPL calls this \
                 automatically) — sql() cannot write to a native table (it takes &self) and \
                 would otherwise silently execute only the source query without inserting \
                 anything"
                    .to_string(),
            ));
        }

        // Same reasoning again for `DELETE FROM ... [WHERE ...]`
        // (native-tables-mutation epic, task 003): `Binder::bind()`'s
        // `Statement::Delete` arm only validates shape and always returns
        // `Err` (DELETE has no `LogicalPlan` to give back — see that arm's
        // own comment), so an unguarded DELETE reaching the ordinary
        // bind/plan/execute path below would just surface that
        // NotImplemented error anyway. This guard exists for the same
        // reason the CREATE TABLE/INSERT ones do: fail with a message that
        // names the correct entrypoint BEFORE constructing a `Binder` at
        // all, matching this method's own established pattern rather than
        // relying on `bind()`'s incidental refusal.
        if crate::planner::delete_target_name(&stmt).is_some() {
            return Err(QueryError::InvalidArgument(
                "DELETE FROM ... must run through \
                 ExecutionContext::delete_from_native_table (the REPL calls this \
                 automatically) — sql() cannot delete from a native table (it takes &self, \
                 and DELETE needs a bespoke row-identification loop no LogicalPlan can \
                 express)"
                    .to_string(),
            ));
        }

        // Same reasoning again for `UPDATE <table> SET ... [WHERE ...]`
        // (native-tables-mutation epic, task 004): `Binder::bind()`'s
        // `Statement::Update` arm only validates shape and always returns
        // `Err` (UPDATE has no `LogicalPlan` to give back, same reason
        // DELETE doesn't), so an unguarded UPDATE reaching the ordinary
        // bind/plan/execute path below would just surface that
        // NotImplemented error anyway. This guard exists for the same
        // reason the CREATE TABLE/INSERT/DELETE ones do: fail with a
        // message that names the correct entrypoint BEFORE constructing a
        // `Binder` at all.
        if crate::planner::update_target_name(&stmt).is_some() {
            return Err(QueryError::InvalidArgument(
                "UPDATE ... SET ... must run through \
                 ExecutionContext::update_native_table (the REPL calls this automatically) — \
                 sql() cannot update a native table (it takes &self, and UPDATE needs a \
                 bespoke row-identification + SET-evaluation loop no LogicalPlan can express)"
                    .to_string(),
            ));
        }

        // Same reasoning again for `CREATE MATERIALIZED VIEW <name> AS
        // SELECT ...` (native-tables-rollups epic, task 002):
        // `create_materialized_view` needs `&mut self` (registering the
        // rollup's own native table mutates `self.tables`/`self.catalog`,
        // exactly like `create_table_as_select`) and drives task 001's
        // `register_rollup` directly, incompatible with this method's
        // `&self`, fully-materializing signature. An unguarded CREATE
        // MATERIALIZED VIEW reaching the ordinary bind/plan/execute path
        // below would (now that `Binder::bind()` accepts
        // `Statement::CreateView`) bind and execute the DEFINING query
        // itself and return ITS rows as a "successful" result -- never
        // registering a rollup at all. A PLAIN (non-materialized) `CREATE
        // VIEW` is deliberately NOT caught by this guard
        // (`create_materialized_view_target_name` returns `None` for it) --
        // it falls through to the ordinary path below and is refused
        // there, directly, by `Binder::bind()` itself ("CREATE VIEW
        // (non-materialized) is not supported"), matching
        // `Statement::Delete`/`Statement::Update`'s own "let `bind()`'s own
        // unconditional refusal fire" precedent for shapes with no
        // alternate entrypoint to redirect to.
        if crate::planner::create_materialized_view_target_name(&stmt).is_some() {
            return Err(QueryError::InvalidArgument(
                "CREATE MATERIALIZED VIEW ... AS SELECT must run through \
                 ExecutionContext::create_materialized_view (the REPL calls this \
                 automatically) — sql() cannot register the result (it takes &self) and would \
                 otherwise silently execute only the defining query without registering a \
                 rollup"
                    .to_string(),
            ));
        }

        // Plan
        let plan_start = Instant::now();
        let mut binder = Binder::new(&self.catalog);
        let logical = binder.bind(&stmt)?;
        metrics.plan_time = plan_start.elapsed();
        metrics.tables = collect_scan_tables(&logical);

        // Rollup substitution (native-tables-rollups epic, task 001):
        // given real registry access this method has and a plain
        // `OptimizerRule` cannot (see `storage::native_rollup`'s module
        // doc), check whether a registered, non-stale rollup exactly
        // answers this query and, if so, rewrite the plan to scan it
        // instead of the base table — BEFORE the ordinary optimizer
        // pipeline runs, matching against the plan straight out of the
        // binder. `rollup_answered` is this task's structured provenance
        // record (see `QueryMetrics::rollup_answered`'s own doc).
        let (logical, rollup_answered) = self.substitute_rollups(logical)?;
        metrics.rollup_answered = rollup_answered;

        // Optimize (with table statistics for better join planning)
        let optimize_start = Instant::now();
        let table_stats = self.collect_table_statistics();
        let optimized = if table_stats.is_empty() {
            self.optimizer.optimize(logical)?
        } else {
            // Create a temporary optimizer with stats for this query
            let optimizer = Optimizer::new().with_table_statistics(table_stats);
            optimizer.optimize(logical)?
        };
        metrics.optimize_time = optimize_start.elapsed();
        if std::env::var("PLAN_DEBUG").is_ok() {
            eprintln!("[plan]\n{}", optimized);
        }
        // Decorrelation and rollup substitution can introduce scans the
        // bound plan did not have (DelimJoin sides, the rollup itself):
        // union them in so `tables` names everything actually read.
        for t in collect_scan_tables(&optimized) {
            if !metrics.tables.contains(&t) {
                metrics.tables.push(t);
            }
        }
        metrics.tables.sort();
        metrics.optimized_plan = Some(cap_debug_text(optimized.to_string()));

        // Physical planning with spillable operators for memory safety
        let physical_start = Instant::now();
        let mut planner =
            PhysicalPlanner::with_config(self.memory_pool.clone(), self.config.clone());
        for (name, provider) in &self.tables {
            planner.register_table(name.clone(), provider.clone());
        }
        // Enable subquery execution support
        planner.enable_subquery_execution();
        let physical = planner.create_physical_plan(&optimized)?;
        metrics.plan_time += physical_start.elapsed();
        metrics.physical_plan = Some(cap_debug_text(crate::physical::display_plan(
            physical.as_ref(),
            0,
        )));

        // Execute
        let execute_start = Instant::now();

        // Determine number of output partitions
        let num_partitions = physical.output_partitions().max(1);

        // Execute all partitions concurrently
        // Use futures to execute partitions in parallel
        let partition_futures: Vec<_> = (0..num_partitions)
            .map(|partition_id| {
                let physical = physical.clone();
                async move {
                    let stream = physical.execute(partition_id).await.map_err(|e| {
                        crate::error::QueryError::Execution(format!(
                            "Partition {} execution failed: {}",
                            partition_id, e
                        ))
                    })?;
                    stream.try_collect().await.map_err(|e| {
                        crate::error::QueryError::Execution(format!(
                            "Partition {} collection failed: {}",
                            partition_id, e
                        ))
                    })
                }
            })
            .collect();

        // Execute all partitions concurrently and collect results
        let partition_results: Vec<Result<Vec<RecordBatch>>> =
            futures::future::join_all(partition_futures).await;

        // Check for errors in any partition
        let mut all_batches = Vec::new();
        for partition_result in partition_results {
            all_batches.extend(partition_result?);
        }

        metrics.execute_time = execute_start.elapsed();

        metrics.total_time = start.elapsed();

        // Capture memory metrics: the pool's high-water mark over this
        // query's window, not the post-release residual.
        metrics.peak_memory_bytes = self.memory_pool.peak();
        let spilled = self.memory_pool.spilled();
        if spilled > 0 {
            metrics.spill_metrics = Some(SpillMetrics {
                bytes_spilled: spilled,
                ..Default::default()
            });
        }

        let schema = physical.schema();
        let row_count: usize = all_batches.iter().map(|b| b.num_rows()).sum();
        metrics.batches = all_batches.len();

        // Dictionary-encoded columns (small-build join gathers) are an
        // internal representation; results hand plain arrays to formatters,
        // CSV writers and tests. Cast at the boundary — proportional to
        // OUTPUT size only.
        let all_batches = all_batches
            .into_iter()
            .map(decode_dictionary_batch)
            .collect::<Result<Vec<_>>>()?;

        Ok(QueryResult {
            schema,
            batches: all_batches,
            row_count,
            metrics,
        })
    }

    /// Merge many per-partition streams into ONE, bounding how many are
    /// concurrently polled/decoded at once (native-tables-mutation epic,
    /// task 005). Plain `futures::stream::select_all` (the mechanism this
    /// replaces at both call sites below) keeps EVERY member stream alive
    /// and interleaves polls across ALL of them for the operation's whole
    /// duration — for a wide multi-partition source (a
    /// `StreamingParquetScanExec` partition per up to `rayon::
    /// current_num_threads()`, each effectively a currently-open parquet
    /// row-group reader with its own decoded-column-chunk working set
    /// resident once its turn comes), that lets up to `num_partitions`
    /// readers' working sets sit resident simultaneously with no cap. Task
    /// 002 root-caused this as (most of) the SQL-path INSERT's measured
    /// ~5.3GB-vs-328MB gap over the direct write core; task 005 confirmed
    /// (a) it stays BOUNDED rather than growing with source row count
    /// (60M vs 600M rows measured ~5.4GB vs ~6.0GB peak RSS — the
    /// partition count, not the row count, drives it) but (b) is not
    /// admission-controlled at all today (a configured `--memory-limit`
    /// has zero effect on this path) and (c) a genuinely tight memory cap
    /// gets the process a real kernel cgroup OOM-kill, not a clean
    /// refusal — see `.claude/epics/native-tables-mutation/005.md`'s
    /// Outcome for the full measurement. `flatten_unordered(Some(limit))`
    /// polls at most `limit` inner streams at once: same eventual output
    /// (every row, order-independent — matching `select_all`'s own
    /// pre-existing "unordered merge is fine, only every row exactly once
    /// matters" contract both callers already documented), bounded
    /// concurrent memory instead of unbounded. A single-threaded Append
    /// writer downstream drains one batch at a time regardless, so wide
    /// concurrency here was never buying write throughput — only, at
    /// best, some read-ahead.
    fn bounded_partition_merge(streams: Vec<RecordBatchStream>, limit: usize) -> RecordBatchStream {
        use futures::StreamExt;
        Box::pin(futures::stream::iter(streams).flatten_unordered(Some(limit.max(1))))
    }

    /// Concurrency cap for [`Self::bounded_partition_merge`]. Overridable
    /// via `QE_INSERT_MERGE_CONCURRENCY`, matching this codebase's
    /// established env-gated tuning-switch convention (`QE_MORSEL`,
    /// `QE_IPC_CACHE`, ...). 8 is a deliberately modest, hardware-count-
    /// independent default — task 005 measured it cuts the SQL-path
    /// INSERT/CTAS peak RSS materially with no correctness change (every
    /// row still emitted exactly once) and no measurable wall-time
    /// regression on the shapes it tested.
    fn insert_merge_concurrency() -> usize {
        parse_merge_concurrency(std::env::var("QE_INSERT_MERGE_CONCURRENCY").ok().as_deref())
    }

    /// The largest single parquet row group (by the footer's UNCOMPRESSED
    /// `total_byte_size`) across every parquet-backed table the plan scans
    /// — the per-stream working-set basis of the admission check. Footer
    /// metadata only (same read `StreamingParquetScanExec::new` already
    /// performs at planning time); never touches row data. A table without
    /// `parquet_files()` (memory table, native table, `VALUES`) simply
    /// contributes nothing; an unreadable footer is skipped rather than
    /// failing the statement — this is an ESTIMATE source, and the scan
    /// itself will surface any real I/O error loudly moments later.
    fn max_source_row_group_bytes(&self, plan: &LogicalPlan) -> u64 {
        let mut names: Vec<String> = Vec::new();
        collect_scan_table_names(plan, &mut names);
        let mut max_bytes = 0u64;
        for name in &names {
            let Some(provider) = self.tables.get(name) else {
                continue;
            };
            let Some(files) = provider.parquet_files() else {
                continue;
            };
            for path in files {
                let Ok(file) = std::fs::File::open(&path) else {
                    continue;
                };
                use parquet::file::reader::FileReader;
                let Ok(reader) = parquet::file::reader::SerializedFileReader::new(file) else {
                    continue;
                };
                for rg in reader.metadata().row_groups() {
                    max_bytes = max_bytes.max(rg.total_byte_size().max(0) as u64);
                }
            }
        }
        max_bytes
    }

    /// The formal, named pre-flight admission check for the INSERT/CTAS
    /// write path (oom-safety-hardening epic, task 005 — PRD G3). Runs
    /// AFTER the physical plan is built (so the partition count is known)
    /// and BEFORE any partition stream is driven, mirroring
    /// `NativeTable::check_scan_budget`'s budget formula
    /// (`memory_limit * spill_threshold`) and named-refusal error style.
    ///
    /// Closes the documented residual from native-tables-mutation task 005:
    /// `bounded_partition_merge` cut the write path's peak RSS ~70%, but a
    /// 512MB-class cgroup cap still got a kernel SIGKILL because nothing on
    /// this path consulted `--memory-limit` before starting. Now a
    /// statement whose bounded-merge working set (concurrency x decoded
    /// row group — see `evaluate_insert_admission` for the exact formula
    /// and its SF=10 calibration) provably cannot fit refuses cleanly, by
    /// name, with exact byte counts and the knobs to turn.
    ///
    /// `QE_DEBUG_INSERT_ADMISSION=1` traces every decision to stderr —
    /// the same cheap, env-gated diagnostic-switch convention as
    /// `QE_DEBUG_SCAN_BUDGET`/`QE_DEBUG_ROLLUP`.
    fn check_insert_write_admission(
        &self,
        statement_kind: &str,
        optimized: &LogicalPlan,
        physical: &Arc<dyn PhysicalOperator>,
    ) -> Result<()> {
        let budget = (self.config.memory_limit as f64 * self.config.spill_threshold) as u64;
        let merge_concurrency = Self::insert_merge_concurrency();
        let num_partitions = physical.output_partitions().max(1);
        let max_rg_bytes = self.max_source_row_group_bytes(optimized);
        let verdict = evaluate_insert_admission(
            statement_kind,
            budget,
            self.config.memory_limit,
            merge_concurrency,
            num_partitions,
            max_rg_bytes,
        );
        if std::env::var("QE_DEBUG_INSERT_ADMISSION").is_ok() {
            eprintln!(
                "[insert_admission] {statement_kind}: budget={budget} \
                 memory_limit={} merge_concurrency={merge_concurrency} \
                 num_partitions={num_partitions} max_row_group_bytes={max_rg_bytes} \
                 verdict={}",
                self.config.memory_limit,
                if verdict.is_some() { "REFUSE" } else { "ADMIT" }
            );
        }
        match verdict {
            Some(message) => Err(QueryError::Execution(message)),
            None => Ok(()),
        }
    }

    /// Execute `CREATE TABLE <name> AS SELECT ...` end to end: parse, bind
    /// the inner `SELECT` via the ordinary `Binder::bind()` path (which also
    /// validates the `CREATE TABLE` clause is a shape this epic supports —
    /// see `planner::binder`'s `require_supported_create_table_shape`), plan
    /// and optimize it exactly like [`sql`](Self::sql), then drive its
    /// `RecordBatchStream` DIRECTLY into task 003's native-table writer
    /// (`native_write::write_batches`) — never collecting it into a
    /// `Vec<RecordBatch>` first, unlike `sql()`. Registers the freshly
    /// written table under its target name (`native_table_root().join(name)`)
    /// so a subsequent query in the SAME session can read it immediately.
    ///
    /// `&mut self`, unlike `sql()`'s `&self`: registering the new table
    /// mutates `self.tables`/`self.catalog`, and a genuinely streaming write
    /// needs the plan's stream driven directly rather than via `sql()`'s
    /// collecting wrapper.
    ///
    /// An existing native table at the target directory is replaced
    /// wholesale (`NativeWriteMode::Overwrite` — this epic is full-table
    /// bulk-load/replace only, never partial `INSERT`); its `table_id`
    /// survives the replace (only `snapshot.version` bumps), matching
    /// `native_manifest`'s own identity contract. Any other kind of
    /// pre-existing, non-native directory at that path is refused rather
    /// than silently overwritten.
    pub async fn create_table_as_select(&mut self, sql: &str) -> Result<CreateTableAsSelectResult> {
        let start = Instant::now();
        let stmt = parser::parse_sql(sql)?;
        let table_name = crate::planner::create_table_target_name(&stmt).ok_or_else(|| {
            QueryError::InvalidArgument(format!(
                "create_table_as_select expects a CREATE TABLE ... AS SELECT statement, got: {sql}"
            ))
        })?;

        let mut binder = Binder::new(&self.catalog);
        // Validates the CREATE TABLE's shape (refusing every unsupported
        // clause by name — OR REPLACE, TEMPORARY, PARTITION BY, LIKE,
        // CLONE, ...) AND binds the inner SELECT in one call: see
        // `Binder::bind()`'s `Statement::CreateTable` arm.
        let logical = binder.bind(&stmt)?;

        let table_stats = self.collect_table_statistics();
        let optimized = if table_stats.is_empty() {
            self.optimizer.optimize(logical)?
        } else {
            let optimizer = Optimizer::new().with_table_statistics(table_stats);
            optimizer.optimize(logical)?
        };
        if std::env::var("PLAN_DEBUG").is_ok() {
            eprintln!("[plan]\n{}", optimized);
        }

        let mut planner =
            PhysicalPlanner::with_config(self.memory_pool.clone(), self.config.clone());
        for (name, provider) in &self.tables {
            planner.register_table(name.clone(), provider.clone());
        }
        planner.enable_subquery_execution();
        let physical = planner.create_physical_plan(&optimized)?;
        // Formal pre-flight admission check (oom-safety-hardening task 005):
        // partition count is known now, and NO partition stream has been
        // driven yet — refuse cleanly here, by name, rather than let a
        // statement that provably cannot fit run into a kernel OOM-kill.
        self.check_insert_write_admission(
            &format!("CREATE TABLE {table_name} AS SELECT"),
            &optimized,
            &physical,
        )?;
        // A bare `SELECT * FROM t` (no explicit column list) binds through
        // `Binder::bind_select_items`'s `SelectItem::Wildcard` arm, which
        // reuses the scan's OWN `SchemaField`s verbatim — including their
        // `relation` (this engine's self-join disambiguation, e.g.
        // "n1.n_name" vs "n2.n_name") — unlike an explicit column list,
        // whose fields are rebuilt unqualified. So `physical.schema()` can
        // carry table-qualified Arrow field names like `"orders.o_orderkey"`
        // for that shape specifically (confirmed empirically, not assumed:
        // `SELECT * FROM orders` vs `SELECT o_orderkey FROM orders` produce
        // different-shaped schemas from the SAME table today). That is a
        // pre-existing, general engine property harmless for a transient
        // `QueryResult` (as `sql()` returns), but wrong to bake permanently
        // into a persisted table's column names — a subsequent `SELECT
        // o_orderkey FROM <this table>` must work. `output_schema_for_
        // native_write` strips that qualification (and normalizes
        // Dictionary types to their plain value type, matching what
        // `decode_dictionary_batch` below actually produces) so the WRITTEN
        // table gets ordinary column names, without touching the general
        // wildcard-binding behavior other callers rely on.
        let write_schema = output_schema_for_native_write(&physical.schema())?;

        // Drive every output partition's stream directly, then merge them
        // into ONE stream — the writer takes a single `RecordBatchStream`.
        // An unordered interleave is fine: unlike `sql()`, nothing here
        // needs partition-ordered output, only every row exactly once.
        // Each batch is normalized to plain (non-dictionary) columns AND
        // re-wrapped with `write_schema` (same arrays, new field
        // metadata — `RecordBatch::try_new` does not copy column buffers)
        // so every batch reaching the writer already has its final,
        // persisted shape: the writer's own dictionary-candidate detection
        // (task 003, an Arrow-side equivalent of `ipc_cache.rs`'s
        // parquet-metadata-based technique) sees plain columns to analyze,
        // under their final unqualified names.
        let num_partitions = physical.output_partitions().max(1);
        let mut streams: Vec<RecordBatchStream> = Vec::with_capacity(num_partitions);
        for partition_id in 0..num_partitions {
            let stream = physical.execute(partition_id).await.map_err(|e| {
                QueryError::Execution(format!("Partition {partition_id} execution failed: {e}"))
            })?;
            let out_schema = write_schema.clone();
            let decoded: RecordBatchStream = Box::pin(stream.and_then(move |b| {
                let out_schema = out_schema.clone();
                async move {
                    let b = decode_dictionary_batch(b)?;
                    RecordBatch::try_new(out_schema, b.columns().to_vec())
                        .map_err(|e| QueryError::Execution(e.to_string()))
                }
            }));
            streams.push(decoded);
        }
        let merged: RecordBatchStream =
            Self::bounded_partition_merge(streams, Self::insert_merge_concurrency());

        // Create for a genuinely fresh destination, Overwrite when a native
        // table already lives there (bumps snapshot.version, preserves
        // table_id) — `write_batches`'s own safety contract handles every
        // other case (a non-empty, non-native directory; a `Create` target
        // that already exists) by refusing rather than deleting anything,
        // so no separate pre-check is needed here.
        let out_dir = self.native_table_root.join(&table_name);
        let mode = if native_manifest::is_native_table_dir(&out_dir) {
            NativeWriteMode::Overwrite
        } else {
            NativeWriteMode::Create
        };

        let write_result =
            native_write::write_batches(merged, write_schema.clone(), &out_dir, mode)
                .await
                .map_err(|e| {
                    QueryError::Storage(format!("CREATE TABLE {table_name} AS SELECT: {e}"))
                })?;

        // Make the freshly written table immediately queryable in this
        // session — the whole point of this being an ExecutionContext
        // method rather than a bare CLI writer.
        self.register_native_table(&table_name, &out_dir)?;

        Ok(CreateTableAsSelectResult {
            table_name,
            table_id: write_result.table_id,
            version: write_result.version,
            rows: write_result.rows,
            segments: write_result.segments,
            schema: write_schema,
            elapsed: start.elapsed(),
        })
    }

    /// Execute `INSERT INTO <table> SELECT ...` / `INSERT INTO <table>
    /// VALUES (...)` end to end against an EXISTING native table
    /// (native-tables-mutation epic, task 002): parse, bind the source
    /// query via the ordinary `Binder::bind()` path (which also validates
    /// the `INSERT`'s shape is one this epic supports — see
    /// `planner::binder`'s `require_supported_insert_shape`), plan and
    /// optimize it exactly like [`sql`](Self::sql)/
    /// [`create_table_as_select`](Self::create_table_as_select), then
    /// drive its `RecordBatchStream` DIRECTLY into
    /// `native_write::append_to_native_table` — never collecting it into a
    /// `Vec<RecordBatch>` first, matching `create_table_as_select`'s own
    /// streaming discipline. Re-registers the table afterward so its new
    /// rows are visible to a subsequent query in the SAME session.
    ///
    /// `&mut self`, unlike `sql()`'s `&self`: re-registering the table
    /// mutates `self.tables`, and a genuinely streaming write needs the
    /// plan's stream driven directly rather than via `sql()`'s collecting
    /// wrapper.
    ///
    /// Unlike `create_table_as_select` (which always writes under
    /// `native_table_root().join(name)`, creating that directory if
    /// needed), INSERT never creates a table: the target MUST already be
    /// REGISTERED in this session as a native table (`TableProvider::
    /// as_any().downcast_ref::<NativeTable>()`) — a plain `QueryError::
    /// TableNotFound`/`InvalidArgument` otherwise, the same "register
    /// before you reference it" contract every other table access in this
    /// engine already has (no special-cased auto-discovery under
    /// `native_table_root()` — that would be a surprising, untested
    /// deviation from that contract for this one statement only). An
    /// empty source (zero matched rows) is a legitimate no-op — see
    /// `native_write::append_to_native_table`'s doc — not an error.
    ///
    /// A schema mismatch between the source's output and the target's
    /// declared schema is a clean, named `QueryError::Type` from
    /// `native_write::write_append_segments`'s own validation, never
    /// silent coercion.
    ///
    /// (native-tables-rollups epic, task 003) After the new rows are
    /// published and the table is re-registered, every rollup registered
    /// against it is EAGERLY refreshed before this method returns -- see
    /// `Self::refresh_dependent_rollups`'s own doc for the full design
    /// decision and `InsertResult::rollups_refreshed` for the per-rollup
    /// outcome.
    pub async fn insert_into_native_table(&mut self, sql: &str) -> Result<InsertResult> {
        let start = Instant::now();
        let stmt = parser::parse_sql(sql)?;
        let table_name = crate::planner::insert_target_name(&stmt).ok_or_else(|| {
            QueryError::InvalidArgument(format!(
                "insert_into_native_table expects an INSERT INTO <table> SELECT/VALUES ... \
                 statement, got: {sql}"
            ))
        })?;

        let provider = self.tables.get(&table_name).cloned().ok_or_else(|| {
            QueryError::TableNotFound(format!(
                "INSERT INTO {table_name}: no such table is registered in this session (a \
                 native table must be registered, e.g. via register_native_table or a prior \
                 CREATE TABLE ... AS SELECT, before INSERT can target it)"
            ))
        })?;
        let native = provider
            .as_any()
            .downcast_ref::<NativeTable>()
            .ok_or_else(|| {
                QueryError::InvalidArgument(format!(
                    "INSERT INTO {table_name}: this table is not a native table -- INSERT is \
                     only supported against native tables"
                ))
            })?;
        let table_dir = native.dir().to_path_buf();

        let mut binder = Binder::new(&self.catalog);
        // Validates the INSERT's shape (refusing every unsupported clause
        // by name) AND binds the source query in one call: see
        // `Binder::bind()`'s `Statement::Insert` arm.
        let logical = binder.bind(&stmt)?;

        let table_stats = self.collect_table_statistics();
        let optimized = if table_stats.is_empty() {
            self.optimizer.optimize(logical)?
        } else {
            let optimizer = Optimizer::new().with_table_statistics(table_stats);
            optimizer.optimize(logical)?
        };
        if std::env::var("PLAN_DEBUG").is_ok() {
            eprintln!("[plan]\n{}", optimized);
        }

        let mut planner =
            PhysicalPlanner::with_config(self.memory_pool.clone(), self.config.clone());
        for (name, provider) in &self.tables {
            planner.register_table(name.clone(), provider.clone());
        }
        planner.enable_subquery_execution();
        let physical = planner.create_physical_plan(&optimized)?;
        // Formal pre-flight admission check (oom-safety-hardening task 005)
        // -- see `create_table_as_select`'s identical call site. Runs
        // BEFORE any partition stream is driven AND before the target's
        // write lock is ever taken, so a refusal leaves the table
        // byte-for-byte untouched.
        self.check_insert_write_admission(
            &format!("INSERT INTO {table_name}"),
            &optimized,
            &physical,
        )?;

        // Drive every output partition's stream directly, then merge them
        // into ONE stream -- `native_write::append_to_native_table` takes
        // a single `RecordBatchStream`. Only strips INCIDENTAL
        // Dictionary encoding here (small-build join gathers, or an
        // IPC-sidecar-cached scan of the source) the same way
        // `create_table_as_select` does — the REAL target-schema
        // conformance (by POSITION, never trusting the source query's own
        // field names, which can be table-qualified for a bare `SELECT *`
        // wildcard source — the same trap CTAS already guards against)
        // and dictionary re-application happen inside
        // `native_write::write_append_segments` itself, against the
        // target's ALREADY-DECLARED schema, which already exists for
        // INSERT (unlike CTAS, which is defining a schema fresh).
        let num_partitions = physical.output_partitions().max(1);
        let mut streams: Vec<RecordBatchStream> = Vec::with_capacity(num_partitions);
        for partition_id in 0..num_partitions {
            let stream = physical.execute(partition_id).await.map_err(|e| {
                QueryError::Execution(format!("Partition {partition_id} execution failed: {e}"))
            })?;
            let decoded: RecordBatchStream =
                Box::pin(stream.and_then(|b| async move { decode_dictionary_batch(b) }));
            streams.push(decoded);
        }
        let merged: RecordBatchStream =
            Self::bounded_partition_merge(streams, Self::insert_merge_concurrency());

        let append_result = native_write::append_to_native_table(
            merged,
            &table_dir,
            native_write::NativeWriteOptions::default(),
        )
        .await
        .map_err(|e| QueryError::Storage(format!("INSERT INTO {table_name}: {e}")))?;

        // Re-open the table so a subsequent query in the SAME session
        // sees the newly appended rows -- mirrors `create_table_as_select`'s
        // own "make the write immediately queryable" step. Reuses the SAME
        // memory budget `register_native_table` always computes.
        self.register_native_table(&table_name, &table_dir)?;

        // Eagerly refresh every rollup registered against this table
        // (native-tables-rollups epic, task 003 -- see
        // `Self::refresh_dependent_rollups`'s own doc for the full
        // eager-vs-lazy design decision). MUST run AFTER the
        // re-registration immediately above: `register_rollup` (which
        // this calls) reads the base table's CURRENT `(table_id,
        // version)` from `self.tables`, so refreshing before
        // re-registering would tag the rollup with the STALE pre-insert
        // version. Skipped entirely for a zero-row (no-op) INSERT -- see
        // that method's own "cost avoidance" doc -- a no-op never makes a
        // rollup stale, so there is nothing to refresh.
        let rollups_refreshed = if append_result.rows_appended > 0 {
            self.refresh_dependent_rollups(&table_name).await
        } else {
            Vec::new()
        };

        Ok(InsertResult {
            table_name,
            table_id: append_result.table_id,
            version: append_result.version,
            rows_inserted: append_result.rows_appended,
            segments_added: append_result.segments_appended,
            total_rows: append_result.total_rows,
            rollups_refreshed,
            elapsed: start.elapsed(),
        })
    }

    /// Execute `DELETE FROM <table> [WHERE ...]` end to end against an
    /// EXISTING native table (native-tables-mutation epic, task 003):
    /// parse, recover the target name (`planner::delete_target_name`),
    /// look it up as a REGISTERED native table (same "register before you
    /// reference it" contract as INSERT — no auto-discovery), bind the
    /// WHERE predicate via `Binder::bind_delete` (which ALSO validates the
    /// statement's shape — every clause this epic does not support
    /// refused by name), then drive `native_delete::
    /// delete_from_native_table`'s bespoke row-identification +
    /// deletion-vector-editing + single-file atomic-publish sequence
    /// DIRECTLY — unlike CTAS/INSERT, there is no `LogicalPlan`/
    /// `PhysicalOperator` pipeline involved at all (see `native_delete.rs`'s
    /// module doc for why). Re-registers the table afterward so the
    /// deletion is visible to a subsequent query in the SAME session.
    ///
    /// `&mut self`, unlike `sql()`'s `&self`: re-registering the table
    /// mutates `self.tables`.
    ///
    /// `selection: None` (`DELETE FROM t` with no WHERE) deletes every
    /// row — the table keeps existing, now logically empty, not an error.
    /// A predicate matching zero rows is a clean no-op (no version bump,
    /// manifest untouched) — see `native_delete::delete_from_native_table`'s
    /// doc.
    ///
    /// (native-tables-rollups epic, task 003) After the deletion is
    /// published and the table is re-registered, every rollup registered
    /// against it is EAGERLY refreshed before this method returns -- see
    /// `Self::refresh_dependent_rollups`'s own doc for the full design
    /// decision and `DeleteResult::rollups_refreshed` for the per-rollup
    /// outcome. Skipped for a no-op DELETE (nothing to refresh).
    pub async fn delete_from_native_table(&mut self, sql: &str) -> Result<DeleteResult> {
        let start = Instant::now();
        let stmt = parser::parse_sql(sql)?;
        let table_name = crate::planner::delete_target_name(&stmt).ok_or_else(|| {
            QueryError::InvalidArgument(format!(
                "delete_from_native_table expects a DELETE FROM <table> [WHERE ...] \
                 statement, got: {sql}"
            ))
        })?;

        let provider = self.tables.get(&table_name).cloned().ok_or_else(|| {
            QueryError::TableNotFound(format!(
                "DELETE FROM {table_name}: no such table is registered in this session (a \
                 native table must be registered, e.g. via register_native_table or a prior \
                 CREATE TABLE ... AS SELECT, before DELETE can target it)"
            ))
        })?;
        let native = provider
            .as_any()
            .downcast_ref::<NativeTable>()
            .ok_or_else(|| {
                QueryError::InvalidArgument(format!(
                    "DELETE FROM {table_name}: this table is not a native table -- DELETE is \
                     only supported against native tables"
                ))
            })?;
        let table_dir = native.dir().to_path_buf();

        let sqlparser::ast::Statement::Delete(delete) = &stmt else {
            unreachable!("delete_target_name only returns Some for a Statement::Delete")
        };
        let mut binder = Binder::new(&self.catalog);
        // Validates the DELETE's shape (refusing every unsupported clause
        // by name) AND binds the WHERE predicate (if any) in one call —
        // see `Binder::bind_delete`.
        let (_, predicate) = binder.bind_delete(delete)?;

        let delete_result = native_delete::delete_from_native_table(&table_dir, predicate.as_ref())
            .await
            .map_err(|e| QueryError::Storage(format!("DELETE FROM {table_name}: {e}")))?;

        // Re-open the table so a subsequent query in the SAME session
        // sees the deletion -- mirrors `insert_into_native_table`'s own
        // "make the write immediately visible" step.
        self.register_native_table(&table_name, &table_dir)?;

        // Eagerly refresh every rollup registered against this table --
        // see `insert_into_native_table`'s identical comment (native-
        // tables-rollups epic, task 003) for why this must run AFTER the
        // re-registration above and why it is skipped for a no-op DELETE.
        let rollups_refreshed = if delete_result.rows_deleted > 0 {
            self.refresh_dependent_rollups(&table_name).await
        } else {
            Vec::new()
        };

        Ok(DeleteResult {
            table_name,
            table_id: delete_result.table_id,
            version: delete_result.version,
            rows_deleted: delete_result.rows_deleted,
            segments_dropped: delete_result.segments_dropped,
            total_rows: delete_result.total_rows,
            rollups_refreshed,
            elapsed: start.elapsed(),
        })
    }

    /// Execute `UPDATE <table> SET <col> = <expr>, ... [WHERE ...]` end to
    /// end against an EXISTING native table (native-tables-mutation epic,
    /// task 004): parse, recover the target name
    /// (`planner::update_target_name`), look it up as a REGISTERED native
    /// table (same "register before you reference it" contract as INSERT/
    /// DELETE — no auto-discovery), bind the SET assignments and WHERE
    /// predicate via `Binder::bind_update` (which ALSO validates the
    /// statement's shape — every clause this epic does not support refused
    /// by name), then drive `native_update::update_native_table`'s bespoke
    /// row-identification + SET-evaluation + combined-segment-list +
    /// single-file atomic-publish sequence DIRECTLY — unlike CTAS/INSERT,
    /// there is no `LogicalPlan`/`PhysicalOperator` pipeline involved at
    /// all (see `native_update.rs`'s module doc for why, and for exactly
    /// why this is composed as ONE atomic operation rather than a DELETE
    /// followed by an INSERT). Re-registers the table afterward so the
    /// update is visible to a subsequent query in the SAME session.
    ///
    /// `&mut self`, unlike `sql()`'s `&self`: re-registering the table
    /// mutates `self.tables`.
    ///
    /// `selection: None` (`UPDATE t SET ...` with no WHERE) updates every
    /// LIVE row. A predicate matching zero rows, or matching only rows
    /// already tombstoned by a prior DELETE/UPDATE, is a clean no-op (no
    /// version bump, manifest untouched) — see `native_update::
    /// update_native_table`'s doc.
    ///
    /// (native-tables-rollups epic, task 003) After the update is
    /// published and the table is re-registered, every rollup registered
    /// against it is EAGERLY refreshed before this method returns -- see
    /// `Self::refresh_dependent_rollups`'s own doc for the full design
    /// decision and `UpdateResult::rollups_refreshed` for the per-rollup
    /// outcome. Skipped for a no-op UPDATE (nothing to refresh).
    pub async fn update_native_table(&mut self, sql: &str) -> Result<UpdateResult> {
        let start = Instant::now();
        let stmt = parser::parse_sql(sql)?;
        let table_name = crate::planner::update_target_name(&stmt).ok_or_else(|| {
            QueryError::InvalidArgument(format!(
                "update_native_table expects an UPDATE <table> SET ... [WHERE ...] statement, \
                 got: {sql}"
            ))
        })?;

        let provider = self.tables.get(&table_name).cloned().ok_or_else(|| {
            QueryError::TableNotFound(format!(
                "UPDATE {table_name}: no such table is registered in this session (a native \
                 table must be registered, e.g. via register_native_table or a prior CREATE \
                 TABLE ... AS SELECT, before UPDATE can target it)"
            ))
        })?;
        let native = provider
            .as_any()
            .downcast_ref::<NativeTable>()
            .ok_or_else(|| {
                QueryError::InvalidArgument(format!(
                    "UPDATE {table_name}: this table is not a native table -- UPDATE is only \
                     supported against native tables"
                ))
            })?;
        let table_dir = native.dir().to_path_buf();

        let sqlparser::ast::Statement::Update(update) = &stmt else {
            unreachable!("update_target_name only returns Some for a Statement::Update")
        };
        let mut binder = Binder::new(&self.catalog);
        // Validates the UPDATE's shape (refusing every unsupported clause
        // by name) AND binds every SET assignment's value expression plus
        // the WHERE predicate (if any) in one call — see
        // `Binder::bind_update`.
        let (_, assignments, predicate) = binder.bind_update(update)?;

        let update_result =
            native_update::update_native_table(&table_dir, predicate.as_ref(), &assignments)
                .await
                .map_err(|e| QueryError::Storage(format!("UPDATE {table_name}: {e}")))?;

        // Re-open the table so a subsequent query in the SAME session sees
        // the update -- mirrors `delete_from_native_table`'s own "make the
        // write immediately visible" step.
        self.register_native_table(&table_name, &table_dir)?;

        // Eagerly refresh every rollup registered against this table --
        // see `insert_into_native_table`'s identical comment (native-
        // tables-rollups epic, task 003) for why this must run AFTER the
        // re-registration above and why it is skipped for a no-op UPDATE.
        let rollups_refreshed = if update_result.rows_updated > 0 {
            self.refresh_dependent_rollups(&table_name).await
        } else {
            Vec::new()
        };

        Ok(UpdateResult {
            table_name,
            table_id: update_result.table_id,
            version: update_result.version,
            rows_updated: update_result.rows_updated,
            segments_dropped: update_result.segments_dropped,
            segments_added: update_result.segments_added,
            total_rows: update_result.total_rows,
            rollups_refreshed,
            elapsed: start.elapsed(),
        })
    }

    /// Get the logical plan for a query (for debugging)
    pub fn logical_plan(&self, query: &str) -> Result<LogicalPlan> {
        let stmt = parser::parse_sql(query)?;
        let mut binder = Binder::new(&self.catalog);
        binder.bind(&stmt)
    }

    /// Get the optimized logical plan for a query (for debugging)
    pub fn optimized_plan(&self, query: &str) -> Result<LogicalPlan> {
        let logical = self.logical_plan(query)?;
        // Same rollup substitution step sql() applies (see that method's
        // own comment) — otherwise this debug view would show a
        // different plan than what actually executes.
        let (logical, _) = self.substitute_rollups(logical)?;
        // Use the same statistics-aware optimizer as sql(), otherwise the
        // debug view shows a different (stats-blind) plan than execution uses.
        let table_stats = self.collect_table_statistics();
        if table_stats.is_empty() {
            self.optimizer.optimize(logical)
        } else {
            let optimizer = Optimizer::new().with_table_statistics(table_stats);
            optimizer.optimize(logical)
        }
    }

    /// Get the physical plan for a query (for debugging)
    pub fn physical_plan(&self, query: &str) -> Result<Arc<dyn PhysicalOperator>> {
        let optimized = self.optimized_plan(query)?;

        let mut planner =
            PhysicalPlanner::with_config(self.memory_pool.clone(), self.config.clone());
        for (name, provider) in &self.tables {
            planner.register_table(name.clone(), provider.clone());
        }
        planner.enable_subquery_execution();

        planner.create_physical_plan(&optimized)
    }

    /// List registered tables
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get table schema
    pub fn table_schema(&self, name: &str) -> Option<SchemaRef> {
        self.tables.get(name).map(|t| t.schema())
    }

    /// The provider backing `name`, if it is registered.
    ///
    /// Distributed execution needs this to ask a table what files it is made
    /// of before replacing it with the shard of those files this node owns.
    pub fn table_provider(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).cloned()
    }
}

/// Cast every Dictionary-encoded column of `b` back to its plain value type.
///
/// Dictionary encoding (small-build join gathers, v2 IPC sidecar reads) is
/// an internal representation; both `sql()`'s collected results and
/// `create_table_as_select`'s streamed write hand plain arrays across their
/// respective boundaries (formatters/CSV writers/tests on one side, task
/// 003's own dictionary-candidate re-detection on the other) rather than
/// leaking this engine's internal choice of encoding into either. Cost is
/// proportional to the batch itself, never to a whole table.
fn decode_dictionary_batch(b: RecordBatch) -> Result<RecordBatch> {
    if !b
        .columns()
        .iter()
        .any(|c| matches!(c.data_type(), arrow::datatypes::DataType::Dictionary(_, _)))
    {
        return Ok(b);
    }
    let cols: std::result::Result<Vec<_>, arrow::error::ArrowError> = b
        .columns()
        .iter()
        .map(|c| match c.data_type() {
            arrow::datatypes::DataType::Dictionary(_, v) => arrow::compute::cast(c.as_ref(), v),
            _ => Ok(c.clone()),
        })
        .collect();
    let cols = cols.map_err(|e| QueryError::Execution(e.to_string()))?;
    let fields: Vec<arrow::datatypes::Field> = b
        .schema()
        .fields()
        .iter()
        .zip(cols.iter())
        .map(|(f, c): (_, &arrow::array::ArrayRef)| {
            arrow::datatypes::Field::new(f.name(), c.data_type().clone(), f.is_nullable())
        })
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), cols)
        .map_err(|e| QueryError::Execution(e.to_string()))
}

/// The schema a native table written by `create_table_as_select` should
/// declare, derived from the SELECT's own physical output schema: every
/// field name is stripped to its unqualified form (the part after the last
/// `.`, if any — see `create_table_as_select`'s call-site comment for why
/// qualification can appear at all) and every Dictionary-encoded field's
/// declared type is normalized to its plain value type (matching what
/// `decode_dictionary_batch` actually produces for each batch). A name
/// collision after stripping — two output columns that only differed by
/// table qualification — is refused with a clear, actionable error rather
/// than silently producing a table with duplicate/ambiguous column names.
fn output_schema_for_native_write(schema: &Schema) -> Result<SchemaRef> {
    let mut seen = std::collections::HashSet::with_capacity(schema.fields().len());
    let mut fields = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        let short = f.name().rsplit('.').next().unwrap_or(f.name());
        if !seen.insert(short.to_lowercase()) {
            return Err(QueryError::InvalidArgument(format!(
                "CREATE TABLE ... AS SELECT: output column `{}` collides with another \
                 output column named `{short}` once this engine's internal table \
                 qualification is stripped for the persisted table's column name — give \
                 it an explicit alias (e.g. `SELECT a.x AS a_x, b.x AS b_x`)",
                f.name()
            )));
        }
        let data_type = match f.data_type() {
            DataType::Dictionary(_, value) => value.as_ref().clone(),
            other => other.clone(),
        };
        fields.push(Field::new(short, data_type, f.is_nullable()));
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// Convert Arrow schema to PlanSchema
fn arrow_schema_to_plan_schema(schema: &Schema) -> PlanSchema {
    let fields: Vec<SchemaField> = schema
        .fields()
        .iter()
        .map(|f| {
            SchemaField::new(f.name().clone(), f.data_type().clone()).with_nullable(f.is_nullable())
        })
        .collect();
    PlanSchema::new(fields)
}

/// Utility to print a record batch
pub fn print_batch(batch: &RecordBatch) {
    use arrow::util::pretty::print_batches;
    let _ = print_batches(std::slice::from_ref(batch));
}

/// Utility to print query results
pub fn print_results(result: &QueryResult) {
    use arrow::util::pretty::print_batches;

    println!("Schema: {:?}", result.schema);
    println!("Row count: {}", result.row_count);
    println!(
        "Timing: parse={:?}, plan={:?}, opt={:?}, exec={:?}, total={:?}",
        result.metrics.parse_time,
        result.metrics.plan_time,
        result.metrics.optimize_time,
        result.metrics.execute_time,
        result.metrics.total_time
    );

    // Print memory metrics
    if result.metrics.peak_memory_bytes > 0 {
        println!(
            "Memory: peak={}",
            format_bytes(result.metrics.peak_memory_bytes)
        );
    }

    // Print spill metrics if any
    if let Some(ref spill) = result.metrics.spill_metrics {
        println!(
            "Spill: {} spilled, {} partitions, {} files",
            format_bytes(spill.bytes_spilled),
            spill.partitions_spilled,
            spill.spill_files_created
        );
    }

    // Print pruning stats
    if result.metrics.files_pruned_by_stats > 0 || result.metrics.files_pruned_by_partition > 0 {
        println!(
            "Pruning: {} by stats, {} by partition",
            result.metrics.files_pruned_by_stats, result.metrics.files_pruned_by_partition
        );
    }

    println!();

    if !result.batches.is_empty() {
        let display: Vec<_> = result
            .batches
            .iter()
            .map(summarize_vector_columns)
            .collect();
        let _ = print_batches(&display);
    }
}

/// Replace vector/list columns with a short text summary for display.
///
/// Arrow's `print_batches` renders a `FixedSizeList<Float32, 384>` cell as all
/// 384 numbers, which produces a 5,000-character-wide table for a single
/// embedding and hides every other column. Only the *printed* copy is
/// rewritten; `QueryResult::batches` still holds the real vectors.
fn summarize_vector_columns(batch: &RecordBatch) -> RecordBatch {
    use arrow::array::{Array, ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    let needs_summary = batch
        .schema()
        .fields()
        .iter()
        .any(|f| crate::planner::vector_types::is_opaque_nested(f.data_type()));
    if !needs_summary {
        return batch.clone();
    }

    let mut fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(i);
        if !crate::planner::vector_types::is_opaque_nested(field.data_type()) {
            fields.push(field.as_ref().clone());
            columns.push(col.clone());
            continue;
        }
        let summary: StringArray = (0..col.len())
            .map(|r| {
                if col.is_null(r) {
                    None
                } else {
                    Some(summarize_nested_cell(col, r))
                }
            })
            .collect();
        fields.push(Field::new(field.name(), DataType::Utf8, true));
        columns.push(Arc::new(summary));
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap_or_else(|_| batch.clone())
}

/// Render one nested cell as a short, human-readable string for the printed
/// result table.
///
/// Uses Arrow's own value formatter for the leaves rather than a hand-rolled
/// per-type match: this runs on display output only, so correctness of the
/// rendering matters more than speed, and Arrow already knows how to print
/// every leaf type the reader accepts. Vectors are truncated to a head plus a
/// dimension; structs print all their fields, since a struct is a handful of
/// named values rather than a thousand anonymous ones.
fn summarize_nested_cell(col: &arrow::array::ArrayRef, row: usize) -> String {
    use arrow::array::{Array, FixedSizeListArray, LargeListArray, ListArray, StructArray};
    use arrow::datatypes::DataType;
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    const HEAD: usize = 3;

    fn leaf(arr: &arrow::array::ArrayRef, i: usize) -> String {
        let opts = FormatOptions::default().with_null("NULL");
        match ArrayFormatter::try_new(arr.as_ref(), &opts) {
            Ok(f) => f.value(i).to_string(),
            Err(_) => format!("<{}>", arr.data_type()),
        }
    }

    // Recurse one level so a struct containing a vector still gets truncated.
    fn cell(arr: &arrow::array::ArrayRef, i: usize, depth: usize) -> String {
        if arr.is_null(i) {
            return "NULL".to_string();
        }
        if depth > 2 {
            return leaf(arr, i);
        }
        match arr.data_type() {
            DataType::Struct(fields) => {
                let Some(s) = arr.as_any().downcast_ref::<StructArray>() else {
                    return leaf(arr, i);
                };
                let body: Vec<String> = fields
                    .iter()
                    .enumerate()
                    .map(|(c, f)| format!("{}: {}", f.name(), cell(s.column(c), i, depth + 1)))
                    .collect();
                format!("{{{}}}", body.join(", "))
            }
            DataType::FixedSizeList(_, _) | DataType::List(_) | DataType::LargeList(_) => {
                let child = match arr.data_type() {
                    DataType::FixedSizeList(_, _) => arr
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .map(|a| a.value(i)),
                    DataType::List(_) => {
                        arr.as_any().downcast_ref::<ListArray>().map(|a| a.value(i))
                    }
                    _ => arr
                        .as_any()
                        .downcast_ref::<LargeListArray>()
                        .map(|a| a.value(i)),
                };
                let Some(child) = child else {
                    return leaf(arr, i);
                };
                let n = child.len();
                let head: Vec<String> = (0..n.min(HEAD))
                    .map(|k| cell(&child, k, depth + 1))
                    .collect();
                if n > HEAD {
                    format!("[{}, ...] ({})", head.join(", "), n)
                } else {
                    format!("[{}]", head.join(", "))
                }
            }
            _ => leaf(arr, i),
        }
    }

    cell(col, row, 0)
}

/// Format bytes into human-readable string
fn format_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = KB * 1024;
    const GB: usize = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Upper bound on a plan rendering kept in `QueryMetrics` (and therefore in
/// the `serve` query log): a pathological plan is truncated with a marker
/// rather than allowed to make the bounded ring unbounded.
pub const DEBUG_TEXT_CAP: usize = 64 * 1024;

/// Truncate `s` to [`DEBUG_TEXT_CAP`] bytes on a char boundary, marking the cut.
pub fn cap_debug_text(mut s: String) -> String {
    if s.len() <= DEBUG_TEXT_CAP {
        return s;
    }
    let mut cut = DEBUG_TEXT_CAP;
    while !s.is_char_boundary(cut) {
        cut -= 1;
    }
    s.truncate(cut);
    s.push_str("\n…[truncated]");
    s
}

/// Every table a plan tree scans (including through `VectorSearch`, whose
/// scan sits inside its `input`), sorted and deduplicated. Walks plan
/// children only; a subquery still folded inside an expression is picked up
/// once the optimizer has decorrelated it into the tree.
pub fn collect_scan_tables(plan: &LogicalPlan) -> Vec<String> {
    fn walk(plan: &LogicalPlan, out: &mut Vec<String>) {
        if let LogicalPlan::Scan(node) = plan {
            if !out.contains(&node.table_name) {
                out.push(node.table_name.clone());
            }
        }
        for child in plan.children() {
            walk(child, out);
        }
    }
    let mut out = Vec::new();
    walk(plan, &mut out);
    out.sort();
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    // query-ui epic, task 001: the debugging facts `sql()` now records.
    #[tokio::test]
    async fn sql_records_plans_tables_and_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();
        let mut ctx = ExecutionContext::new();
        ctx.register_batch("people", batch.clone());
        ctx.register_batch("other", batch);

        let r = ctx
            .sql("SELECT p.id FROM people p JOIN other o ON p.id = o.id WHERE p.id > 1")
            .await
            .unwrap();
        assert_eq!(r.row_count, 2);
        assert_eq!(
            r.metrics.tables,
            vec!["other".to_string(), "people".to_string()]
        );
        let opt = r
            .metrics
            .optimized_plan
            .as_deref()
            .expect("optimized plan captured");
        assert!(opt.contains("people") && opt.contains("other"), "{opt}");
        let phys = r
            .metrics
            .physical_plan
            .as_deref()
            .expect("physical plan captured");
        assert!(phys.contains("Join") || phys.contains("join"), "{phys}");
        assert_eq!(r.metrics.batches, r.batches.len());
        assert!(r.metrics.total_time >= r.metrics.execute_time);
    }

    #[test]
    fn cap_debug_text_truncates_on_a_char_boundary() {
        let s = "é".repeat(DEBUG_TEXT_CAP); // 2 bytes each
        let out = cap_debug_text(s);
        assert!(out.ends_with("…[truncated]"));
        assert!(out.len() <= DEBUG_TEXT_CAP + "\n…[truncated]".len());
        assert_eq!(cap_debug_text("short".into()), "short");
    }

    // ------------------------------------------------------------------
    // native-tables-mutation epic, task 005: bounded partition-merge
    // concurrency (the SQL-path INSERT/CTAS memory fix -- see this
    // module's `bounded_partition_merge`/`insert_merge_concurrency` doc
    // comments and `.claude/epics/native-tables-mutation/005.md`'s
    // Outcome for the full before/after measurement).
    // ------------------------------------------------------------------

    #[test]
    fn parse_merge_concurrency_defaults_and_validates() {
        assert_eq!(parse_merge_concurrency(None), 8);
        assert_eq!(parse_merge_concurrency(Some("")), 8);
        assert_eq!(parse_merge_concurrency(Some("not a number")), 8);
        assert_eq!(
            parse_merge_concurrency(Some("0")),
            8,
            "zero would deadlock flatten_unordered's Some(0) -- must fall back"
        );
        assert_eq!(
            parse_merge_concurrency(Some("-1")),
            8,
            "negative is not a valid usize -- must fall back"
        );
        assert_eq!(parse_merge_concurrency(Some("1")), 1);
        assert_eq!(parse_merge_concurrency(Some("32")), 32);
    }

    fn int_batch(schema: &SchemaRef, vals: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))]).unwrap()
    }

    // ------------------------------------------------------------------
    // oom-safety-hardening epic, task 005: the INSERT/CTAS write-path
    // admission check's pure decision core (`evaluate_insert_admission`).
    // The two named constants below are the epic's own calibration
    // points, taken from REAL measurements, not invented for the test:
    // SF=10 lineitem's largest row group is 59,120,848 bytes
    // (`total_byte_size`, 58 row groups -> 32 scan partitions on a
    // 32-thread box), and the measured post-task-005 append peak with
    // the default merge concurrency of 8 was ~1.63GB.
    // ------------------------------------------------------------------

    const SF10_MAX_RG_BYTES: u64 = 59_120_848;

    fn budget_for(memory_limit: usize) -> u64 {
        // The same formula as `check_insert_write_admission` /
        // `check_scan_budget`, with the default spill_threshold.
        (memory_limit as f64 * ExecutionConfig::default().spill_threshold) as u64
    }

    #[test]
    fn insert_admission_admits_the_sf10_calibration_point_at_2gib() {
        // 8 streams x 59.1MB x 3 = ~1.42GB <= 1.72GB budget: the 2GiB
        // memory-limit run that MEASURABLY completes under a 2GB cgroup
        // cap must never be falsely refused.
        let limit = 2 * 1024 * 1024 * 1024usize;
        assert_eq!(
            evaluate_insert_admission(
                "INSERT INTO t",
                budget_for(limit),
                limit,
                8,
                32,
                SF10_MAX_RG_BYTES
            ),
            None
        );
    }

    #[test]
    fn insert_admission_refuses_the_sf10_calibration_point_at_512mib() {
        // Same workload at a 512MiB limit (budget ~429MB): pre-fix this
        // was a kernel SIGKILL; it must now be a named refusal citing the
        // exact byte counts and both knobs.
        let limit = 512 * 1024 * 1024usize;
        let msg = evaluate_insert_admission(
            "INSERT INTO t",
            budget_for(limit),
            limit,
            8,
            32,
            SF10_MAX_RG_BYTES,
        )
        .expect("the 512MiB-limit SF=10 append must be refused");
        let estimated = 8 * SF10_MAX_RG_BYTES * INSERT_ADMISSION_DECODE_EXPANSION;
        assert!(msg.contains(INSERT_ADMISSION_CHECK_NAME), "{msg}");
        assert!(msg.contains(&estimated.to_string()), "{msg}");
        assert!(msg.contains(&budget_for(limit).to_string()), "{msg}");
        assert!(msg.contains("--memory-limit"), "{msg}");
        assert!(msg.contains("QE_INSERT_MERGE_CONCURRENCY"), "{msg}");
        assert!(msg.contains("INSERT INTO t"), "{msg}");
    }

    #[test]
    fn insert_admission_effective_concurrency_is_capped_by_partition_count() {
        // A single-partition source (one row group) can never have more
        // than one stream in flight, whatever the merge concurrency says
        // -- 1 x 59.1MB x 3 = ~177MB fits a 512MiB limit's budget.
        let limit = 512 * 1024 * 1024usize;
        assert_eq!(
            evaluate_insert_admission(
                "INSERT INTO t",
                budget_for(limit),
                limit,
                8,
                1,
                SF10_MAX_RG_BYTES
            ),
            None
        );
    }

    #[test]
    fn insert_admission_lowering_merge_concurrency_shrinks_the_estimate() {
        // The refusal message's own advice must be true: at concurrency 1
        // the SF=10 shape is admitted where concurrency 8 was refused.
        let limit = 1024 * 1024 * 1024usize;
        assert!(evaluate_insert_admission(
            "INSERT INTO t",
            budget_for(limit),
            limit,
            8,
            32,
            SF10_MAX_RG_BYTES
        )
        .is_some());
        assert_eq!(
            evaluate_insert_admission(
                "INSERT INTO t",
                budget_for(limit),
                limit,
                1,
                32,
                SF10_MAX_RG_BYTES
            ),
            None
        );
    }

    #[test]
    fn insert_admission_admits_when_no_parquet_source_exists() {
        // max_row_group_bytes == 0 means "no estimate basis" (VALUES,
        // memory tables, native-table sources already guarded by
        // check_scan_budget) -- admitted even at a zero budget, never
        // refused on a guess.
        assert_eq!(
            evaluate_insert_admission("INSERT INTO t", 0, 0, 8, 32, 0),
            None
        );
    }

    /// `bounded_partition_merge` must emit every row from every input
    /// stream exactly once, regardless of the concurrency limit -- tested
    /// at the extremes (1, the most different from unbounded `select_all`
    /// and therefore the most likely to reveal an off-by-one/deadlock;
    /// and a generous limit) plus the real default.
    #[tokio::test]
    async fn bounded_partition_merge_preserves_every_row_at_every_concurrency_limit() {
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        for limit in [1usize, 2, ExecutionContext::insert_merge_concurrency(), 100] {
            let streams: Vec<RecordBatchStream> = (0..5)
                .map(|p| {
                    let base = p * 10;
                    let batches = vec![
                        int_batch(&schema, vec![base, base + 1]),
                        int_batch(&schema, vec![base + 2]),
                    ];
                    Box::pin(futures::stream::iter(batches.into_iter().map(Ok)))
                        as RecordBatchStream
                })
                .collect();
            let merged = ExecutionContext::bounded_partition_merge(streams, limit);
            let collected: Vec<RecordBatch> =
                merged.try_collect().await.expect("merge must not error");
            let mut values: Vec<i64> = collected
                .iter()
                .flat_map(|b| {
                    b.column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            values.sort_unstable();
            let expected: Vec<i64> = (0..5)
                .flat_map(|p| [p * 10, p * 10 + 1, p * 10 + 2])
                .collect();
            assert_eq!(
                values, expected,
                "limit={limit}: every row must appear exactly once"
            );
        }
    }

    #[tokio::test]
    async fn bounded_partition_merge_handles_zero_streams() {
        let merged = ExecutionContext::bounded_partition_merge(Vec::new(), 8);
        let collected: Vec<RecordBatch> = merged
            .try_collect()
            .await
            .expect("empty merge must not error");
        assert!(collected.is_empty());
    }

    fn create_test_context() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        ctx.register_table("test", schema, vec![batch]);
        ctx
    }

    #[tokio::test]
    async fn test_simple_query() {
        let ctx = create_test_context();
        let result = ctx.sql("SELECT id, value FROM test").await.unwrap();

        assert_eq!(result.row_count, 5);
        assert_eq!(result.schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_filter_query() {
        let ctx = create_test_context();
        let result = ctx
            .sql("SELECT id FROM test WHERE value > 25")
            .await
            .unwrap();

        assert_eq!(result.row_count, 3);
    }

    #[tokio::test]
    async fn test_aggregate_query() {
        let ctx = create_test_context();
        let result = ctx
            .sql("SELECT SUM(value), COUNT(*) FROM test")
            .await
            .unwrap();

        assert_eq!(result.row_count, 1);

        let sum = result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(sum, 150);
    }

    #[tokio::test]
    async fn test_sort_query() {
        let ctx = create_test_context();
        // Note: ORDER BY columns must be in SELECT (planner limitation)
        let result = ctx
            .sql("SELECT id, value FROM test ORDER BY value DESC")
            .await
            .unwrap();

        assert_eq!(result.row_count, 5);

        let ids = result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 5);
        assert_eq!(ids.value(1), 4);
        assert_eq!(ids.value(2), 3);
    }

    #[tokio::test]
    async fn test_limit_query() {
        let ctx = create_test_context();
        let result = ctx.sql("SELECT id FROM test LIMIT 2").await.unwrap();

        assert_eq!(result.row_count, 2);
    }

    #[tokio::test]
    async fn test_group_by_query() {
        let ctx = create_test_context();
        let result = ctx
            .sql("SELECT name, SUM(value) FROM test GROUP BY name")
            .await
            .unwrap();

        assert_eq!(result.row_count, 5); // 5 unique names
    }
}
