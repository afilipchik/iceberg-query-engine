//! Native table `TableProvider`: the read/registration/distribution side of
//! the native-tables-foundation epic (task 004). A native table is a
//! directory holding a `_manifest.json` (task 002,
//! `src/storage/native_manifest.rs`) plus one Arrow IPC segment file per
//! `Segment` it lists, produced by task 003's writer
//! (`src/storage/native_write.rs`).
//!
//! # What this module owns, and what it deliberately does not
//!
//! - Owns: the `NativeTable` type and its `TableProvider` implementation
//!   (`schema`/`scan`/`statistics`/`distributed_splits`/`shard_by_splits`),
//!   plus `--tables`-style directory auto-detection
//!   (`is_native_table_dir`, re-exported from `native_manifest`) and
//!   `ExecutionContext::register_native_table` (in `src/execution/
//!   context.rs`).
//! - Does NOT own reading segment bytes: `scan` calls
//!   `crate::storage::ipc_cache::read_row_group` UNCHANGED — the exact same
//!   mmap/dictionary-aware function the parquet IPC sidecar cache uses.
//!   Task 002's own tests already proved this function reads a
//!   manifest-described segment with zero modification; this module is the
//!   first REAL caller of that proof.
//! - Does NOT own writing: `src/storage/native_write.rs` (task 003) is the
//!   only writer. This module only ever opens an already-published table
//!   directory (`native_manifest::read_manifest`, which validates on read).
//!
//! # No special-cased scan path
//!
//! `PhysicalPlanner::create_physical_plan_inner`'s `LogicalPlan::Scan` arm
//! branches only on `TableProvider::parquet_files()` (`Some` => a streaming
//! Parquet-specific fast path; `None` => the generic
//! `scan_with_filter`-then-`MemoryTableExec` path every non-Parquet
//! provider already uses, e.g. `LanceTable`). `NativeTable` leaves
//! `parquet_files()` at its default `None`, so it plans and executes
//! through the SAME generic path as every other non-Parquet provider —
//! confirmed by reading that function, not assumed. Joins, aggregates,
//! filters, projections all apply unchanged.
//!
//! # Distributed splits: one split per segment
//!
//! A segment (one Arrow IPC file) is this format's natural atomic unit —
//! the same granularity choice `LanceTable` makes for Lance fragments
//! (`distributed_splits` there also does not subdivide a fragment). Unlike
//! Parquet's row-range splitting (`src/distributed/splits.rs`, which cuts a
//! row group into byte-target pieces because a single `lineitem.parquet`
//! row group can be tens of MB), a native table segment's target size is
//! already controlled at WRITE time (task 003), so this task does not add a
//! second, redundant slicing layer on top — "not silently absent" (the
//! acceptance criterion) is satisfied by real per-segment `Split`s with
//! real byte/row counts feeding the existing `assign_lpt` LPT balancer
//! unchanged, not by a stub returning `None`.
//!
//! # Memory safety: `scan()` is NOT spill-aware, so it is capacity-gated
//! (task 006)
//!
//! `scan()` reads every active segment and returns one fully materialized
//! `Vec<RecordBatch>` — the same "generic, non-parquet" provider contract
//! `LanceTable::scan()` already has (see `native_write.rs`'s doc on
//! `write_from_lance`), and the one this module's own doc has always named
//! as NOT owning the write side's streaming discipline. Task 006 measured
//! what that costs directly, not just by inspection: converting SF=10
//! `lineitem` (60M rows) to a native table and running an ungrouped
//! `SELECT COUNT(*), SUM(l_quantity), SUM(l_extendedprice)` against it
//! through the REAL `TableProvider` path (`serve --tables`, not a
//! synthetic microbenchmark) needed **~1.6GB peak RSS** (kernel `VmHWM`,
//! not estimated) and was OOM-killed (SIGKILL, exit 137) under a bare 1GiB
//! cgroup cap — while the IDENTICAL query over the IDENTICAL data as plain
//! Parquet (which routes through `ParallelParquetSource`'s genuinely
//! row-group-streaming path instead of this generic one) finished in
//! 109ms and never came close to that cap. This is a real, measured,
//! native-table-specific gap relative to Parquet, not a hypothetical or a
//! general engine characteristic every provider already shares.
//!
//! A true fix (an incremental/streaming scan comparable to
//! `ParallelParquetSource`) needs a per-provider morsel driver — out of
//! this task's file scope (`src/physical/planner.rs` /
//! `src/physical/operators/morsel_agg.rs`) and a materially larger lift
//! than this epic's task 006 sizing. Per this task's own charter ("a hard,
//! explicit size/row cap enforced at the right layer... pick one, don't
//! ship neither"), `scan()` instead REFUSES cleanly, before touching a
//! single segment, when its (conservative — see below) estimated memory
//! need exceeds a configured budget:
//!
//! - The budget is `ExecutionConfig::memory_limit * spill_threshold` —
//!   the EXACT formula `src/physical/operators/spillable.rs` already uses
//!   at 7 call sites for the identical "how much may one thing in this
//!   query legitimately hold" question; this is not a new concept or a
//!   new opt-out, just this already-existing knob finally reaching a code
//!   path it silently didn't before. `ExecutionContext::register_native_table`
//!   computes it and calls [`NativeTable::with_memory_budget`]; a
//!   `NativeTable` built directly (tests, and `native_write.rs`'s own
//!   round-trip reader, which does not go through `ExecutionContext` at
//!   all) gets `None` and is unaffected — exactly today's behavior.
//! - The estimate is `self.statistics().total_byte_size` — the WHOLE
//!   active segment set's on-disk size, deliberately ignoring any
//!   requested projection: task 002's manifest has no per-column byte
//!   breakdown to compute a narrower number from (adding one is
//!   `native_manifest.rs` — task 002, closed — territory, not this
//!   task's). This is conservative in the safe direction (may refuse a
//!   narrow projection of a huge table that would actually have fit) and
//!   never in the unsafe one (never proceeds when the whole table
//!   plainly would not fit).
//! - Large-scale native-table benchmarking (e.g. a future task 008 at
//!   SF=10/SF=100) must size `--memory-limit` for the data, exactly like
//!   `benchmark-parquet` already does (`(sf * 4.0).max(1.0)` GB, capped at
//!   64GB, `src/main.rs`) — this is the SAME pre-existing convention, not
//!   a new burden invented here; the engine-wide `ExecutionConfig::default()`
//!   1GiB `memory_limit` was never meant to bound a real multi-GB scan (it
//!   already never has for Parquet either — see `spillable.rs`'s own
//!   `memory_limit * spill_threshold` budgets, sized the same way).

use crate::distributed::{Split, SplitSet};
use crate::error::{QueryError, Result};
use crate::physical::operators::{ColumnStatistics, TableProvider, TableStatistics};
use crate::storage::ipc_cache;
use crate::storage::native_manifest::{self, ColumnStats, NativeManifest, Segment};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// `dir` looks like a native table directory (has a `_manifest.json`).
/// Mirrors `iceberg::is_iceberg_dir`/Lance's `.lance` extension check for
/// `--tables`-style auto-detection (`src/main.rs`). Re-exported here rather
/// than only living in `native_manifest` so callers of THIS module (the
/// registration surface) don't need to reach into task 002's module
/// directly for a detection helper that is conceptually part of
/// registration, not of the manifest format itself.
pub fn is_native_table_dir(dir: &Path) -> bool {
    native_manifest::is_native_table_dir(dir)
}

/// A native table: a directory holding a manifest (task 002) plus Arrow IPC
/// segment files (task 003). Implements [`TableProvider`] so it is a real,
/// first-class provider the engine can plan and execute queries against —
/// see the module doc for why no special-cased scan path is needed.
#[derive(Debug, Clone)]
pub struct NativeTable {
    dir: PathBuf,
    manifest: NativeManifest,
    /// `Some(ids)` restricts scan/statistics/enumeration to just these
    /// segment ids — how [`shard_by_splits`](TableProvider::shard_by_splits)
    /// represents one node's shard of the table. `None` (every
    /// freshly-opened, whole-table provider) means every segment in the
    /// manifest.
    only_segments: Option<HashSet<u32>>,
    /// `scan()`'s admission-control budget in bytes, or `None` for no cap
    /// (this provider's behavior before task 006, and still the behavior
    /// for any `NativeTable` built directly rather than through
    /// `ExecutionContext::register_native_table` — see the module doc's
    /// "Memory safety" section and [`with_memory_budget`](Self::with_memory_budget).
    memory_budget_bytes: Option<u64>,
}

impl NativeTable {
    /// Open a native table directory. Reads and fully validates
    /// `_manifest.json` (`native_manifest::read_manifest`): a missing or
    /// corrupt manifest is a clear `Err`, never a panic or a silently empty
    /// table. No `scan()` admission budget is set (see
    /// [`with_memory_budget`](Self::with_memory_budget)) — `scan()` behaves
    /// exactly as it did before task 006 unless a caller opts in.
    pub fn try_new(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        let manifest = native_manifest::read_manifest(&dir)?;
        Ok(Self {
            dir,
            manifest,
            only_segments: None,
            memory_budget_bytes: None,
        })
    }

    /// Attach (or clear, with `None`) a `scan()` admission-control budget in
    /// bytes — see the module doc's "Memory safety" section for the full
    /// rationale and the measurement that motivated it.
    /// `ExecutionContext::register_native_table` calls this with
    /// `memory_limit * spill_threshold`, mirroring `spillable.rs`'s own
    /// budget formula exactly. A `NativeTable` built directly (tests,
    /// `native_write.rs`'s own round-trip reader) is unaffected unless it
    /// opts in.
    pub fn with_memory_budget(mut self, budget_bytes: Option<u64>) -> Self {
        self.memory_budget_bytes = budget_bytes;
        self
    }

    /// Admission control for `scan()`. This provider's scan is not
    /// spill-aware (module doc) — it materializes every active segment into
    /// one `Vec<RecordBatch>` before returning, so a conservative estimate
    /// of its true memory need is this provider's own on-disk byte total
    /// (ignoring any requested projection — task 002's manifest has no
    /// per-column byte breakdown to compute a narrower number from). Refuses
    /// cleanly, before touching a single segment, rather than silently
    /// proceeding toward a possible OOM.
    ///
    /// `QE_DEBUG_SCAN_BUDGET=1` traces every call (dir + configured budget)
    /// to stderr — matching this codebase's existing "cheap, env-gated,
    /// zero cost when unset" diagnostic-switch convention (see CLAUDE.md's
    /// "Diagnostic switches" table); used to confirm THIS check (rather
    /// than some other code path) is what a given query actually goes
    /// through while investigating task 006's own OOM reproduction.
    fn check_scan_budget(&self) -> Result<()> {
        if std::env::var("QE_DEBUG_SCAN_BUDGET").is_ok() {
            eprintln!(
                "[scan_budget] check_scan_budget: dir={} budget={:?}",
                self.dir.display(),
                self.memory_budget_bytes
            );
        }
        let Some(budget) = self.memory_budget_bytes else {
            return Ok(());
        };
        let estimated = self.statistics().map(|s| s.total_byte_size).unwrap_or(0);
        if estimated > budget {
            return Err(QueryError::Execution(format!(
                "native table at {} needs an estimated {estimated} bytes to scan (its full \
                 on-disk size — this provider's scan() is not yet spill-aware, see \
                 .claude/plans/larger-than-memory-support.md), which exceeds the configured \
                 memory safety budget of {budget} bytes (memory_limit * spill_threshold). \
                 Raise --memory-limit / ExecutionConfig::memory_limit for this query (e.g. \
                 sized for the data the way `benchmark-parquet` already does), or query a \
                 narrower projection/predicate.",
                self.dir.display()
            )));
        }
        Ok(())
    }

    /// The manifest this provider was opened from — table_id, schema,
    /// snapshot (version/row_count), and the full segment list.
    pub fn manifest(&self) -> &NativeManifest {
        &self.manifest
    }

    /// The schema this provider exposes to the catalog/binder/optimizer —
    /// deliberately NOT identical to `self.manifest.arrow_schema()` for
    /// Dictionary-typed fields.
    ///
    /// `native_write.rs` (task 003) declares a dictionary-coerced column's
    /// MANIFEST type as `Dictionary(Int32, Utf8)` — correct for describing
    /// what a segment's Arrow IPC file physically contains (see its module
    /// doc). But surfacing that as the LOGICAL/catalog type breaks the
    /// generic aggregate machinery: `AggregationState::build_group_array`
    /// (`src/physical/morsel_agg.rs`, shared by BOTH `HashAggregateExec`
    /// and `MorselAggregateExec` — confirmed by reading both call sites,
    /// not assumed) has no `Dictionary` match arm, because no existing
    /// provider ever asked it to group by one at the logical level:
    /// `ParquetTable::schema()` always reports the plain value type even
    /// when its IPC sidecar cache physically stores the SAME column
    /// dictionary-encoded — dictionary-vs-plain is an intentionally
    /// invisible physical optimization there, never a logical one.
    ///
    /// This costs nothing at the physical-batch level: `scan()` still
    /// returns genuinely `Dictionary`-encoded arrays from `ipc_cache::
    /// read_row_group` completely unchanged (zero-copy, no decode).
    /// `MemoryTableExec::execute`'s own "rewrap" step (`src/physical/
    /// operators/scan.rs`) already re-tags a batch's schema to match its
    /// ACTUAL array type whenever the two differ (its own doc comment:
    /// "intermediate results routed through here... may carry
    /// dictionary-encoded string columns from small-build join gathers"),
    /// so a scan-level mismatch between "declared Utf8, actually
    /// Dictionary" is an already-load-bearing, already-tested pattern in
    /// this engine — not a new one introduced here. This mirrors Parquet's
    /// own transparent-dictionary model rather than inventing a second one.
    fn logical_schema(&self) -> SchemaRef {
        let fields: Vec<Field> = self
            .manifest
            .arrow_schema()
            .fields()
            .iter()
            .map(|f| {
                let dt = match f.data_type() {
                    DataType::Dictionary(_, value) => value.as_ref().clone(),
                    other => other.clone(),
                };
                Field::new(f.name(), dt, f.is_nullable())
            })
            .collect();
        Arc::new(Schema::new(fields))
    }

    /// The table directory this provider reads from.
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Segments this provider actually reads, in canonical (id-ascending)
    /// order. Sorted explicitly rather than trusting the manifest's own
    /// `Vec` order: `distributed_splits` must produce the identical
    /// `SplitSet` (and therefore digest) on every node reading the same
    /// table, which requires a deterministic order independent of whatever
    /// order task 003 happened to push segments onto the `Vec` during the
    /// write.
    fn active_segments(&self) -> Vec<&Segment> {
        let mut segs: Vec<&Segment> = match &self.only_segments {
            Some(ids) => self
                .manifest
                .segments
                .iter()
                .filter(|s| ids.contains(&s.id))
                .collect(),
            None => self.manifest.segments.iter().collect(),
        };
        segs.sort_by_key(|s| s.id);
        segs
    }
}

/// Translate a manifest statistics rollup into `TableStatistics`. Per task
/// 002's design (`ColumnStats` mirrors `ColumnStatistics` field-for-field),
/// this is a direct copy for every field EXCEPT `ndv_est`, which is DERIVED
/// here the same way `ParquetTable`/`LanceTable` already derive it: a dense
/// integer range upper-bounds NDV (`min(non_null_rows, max - min + 1)`) —
/// see `src/physical/operators/scan.rs`'s `ColumnStatistics::ndv_est` doc.
fn table_statistics_from(
    rollup: &BTreeMap<String, ColumnStats>,
    row_count: u64,
    total_byte_size: u64,
) -> TableStatistics {
    let mut column_stats = HashMap::with_capacity(rollup.len());
    for (name, cs) in rollup {
        let non_null = row_count.saturating_sub(cs.null_count.unwrap_or(0));
        let ndv_est = match (cs.min_i64, cs.max_i64) {
            (Some(min), Some(max)) if max >= min => {
                Some(non_null.min((max - min) as u64 + 1)).filter(|v| *v > 0)
            }
            _ => None,
        };
        column_stats.insert(
            name.clone(),
            ColumnStatistics {
                min_i64: cs.min_i64,
                max_i64: cs.max_i64,
                null_count: cs.null_count,
                ndv_est,
                min_f64: cs.min_f64,
                max_f64: cs.max_f64,
                ndv_str: None,
            },
        );
    }
    TableStatistics {
        row_count: row_count as usize,
        total_byte_size,
        column_stats,
    }
}

impl TableProvider for NativeTable {
    fn schema(&self) -> SchemaRef {
        self.logical_schema()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// `table_id` (stable across a full-table replace) + `snapshot.version`
    /// (bumped by one on every replace) — exactly the pair task 001's
    /// Outcome recommends for GPU-cache identity (task 007), and the only
    /// two fields that answer "is this literally the same data a cache
    /// might already hold." Overridden ONLY on a whole-table provider
    /// (`only_segments.is_none()`): a SHARDED provider (returned by
    /// `shard_by_splits`) must never expose the full table's identity, or
    /// a GPU resident-cache keyed on it would alias a worker's PARTIAL
    /// shard with the full table's cached columns from a different
    /// context — the same correctness reasoning
    /// `ShardedParquetTable::parquet_files()` documents for why it always
    /// returns `None` rather than inheriting `ParquetTable`'s file list.
    fn identity(&self) -> Option<Vec<u8>> {
        if self.only_segments.is_some() {
            return None;
        }
        Some(
            [
                self.manifest.table_id.as_bytes(),
                &self.manifest.snapshot.version.to_le_bytes(),
            ]
            .concat(),
        )
    }

    fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        self.check_scan_budget()?;
        let mut out = Vec::new();
        for seg in self.active_segments() {
            let batches = ipc_cache::read_row_group(&self.dir, seg.id as usize, projection, None)?;
            out.extend(batches);
        }
        Ok(out)
    }

    // scan_with_filter: default (delegates to scan) — no predicate pushdown
    // into the IPC segment reader. Every other provider without pushdown
    // (e.g. LanceTable's unfiltered path) relies on the physical planner's
    // own FilterExec above the scan for correctness, and that applies here
    // unchanged; a future task can add pushdown without touching callers.

    fn statistics(&self) -> Option<TableStatistics> {
        let segs = self.active_segments();
        let row_count: u64 = segs.iter().map(|s| s.row_count).sum();
        let total_byte_size: u64 = segs.iter().map(|s| s.byte_size).sum();
        let rollup = match &self.only_segments {
            // Whole table: the manifest's own precomputed rollup (task 002,
            // `NativeManifest::build`) IS this fold already — an O(1) reuse,
            // matching `ParquetTable::statistics()`'s own compute-once-cache
            // intent, rather than a redundant recompute.
            None => self.manifest.table_stats.clone(),
            // A shard: `table_stats` covers the WHOLE table, not just this
            // shard's segments, so it must be refolded over only the
            // segments this provider actually owns — cheap, since
            // per-segment stats are already small and in memory.
            Some(_) => {
                let owned: Vec<Segment> = segs.iter().map(|s| (*s).clone()).collect();
                NativeManifest::rollup(&owned)
            }
        };
        Some(table_statistics_from(&rollup, row_count, total_byte_size))
    }

    fn distributed_splits(&self, table: &str, nodes: usize) -> Option<Result<SplitSet>> {
        // No further subdivision — one split per segment; see the module
        // doc's "Distributed splits" section for why that is the right
        // atom for this format (unlike Parquet row groups, segment size is
        // already controlled at write time).
        let _ = nodes;
        let segs = self.active_segments();
        let mut splits = Vec::with_capacity(segs.len());
        let mut total_bytes = 0u64;
        let mut total_rows = 0i64;
        for seg in &segs {
            total_bytes += seg.byte_size;
            total_rows += seg.row_count as i64;
            splits.push(Split {
                table: table.to_string(),
                path: native_manifest::segment_full_path(&self.dir, seg.id),
                file: seg.path.clone(),
                row_group: seg.id as usize,
                row_offset: 0,
                num_rows: seg.row_count as i64,
                bytes: seg.byte_size,
            });
        }
        let target_split_bytes = if splits.is_empty() {
            1
        } else {
            (total_bytes / splits.len() as u64).max(1)
        };
        Some(Ok(SplitSet {
            table: table.to_string(),
            splits,
            total_bytes,
            total_rows,
            target_split_bytes,
        }))
    }

    fn shard_by_splits(&self, splits: &[Split]) -> Option<Result<Arc<dyn TableProvider>>> {
        let ids: HashSet<u32> = splits.iter().map(|s| s.row_group as u32).collect();
        Some(Ok(Arc::new(NativeTable {
            dir: self.dir.clone(),
            manifest: self.manifest.clone(),
            only_segments: Some(ids),
            // A shard covers a SUBSET of the whole table's segments, so
            // inheriting the same absolute budget (rather than e.g.
            // dropping it) stays conservative-safe and is never tighter
            // than the whole-table check would have been.
            memory_budget_bytes: self.memory_budget_bytes,
        })))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use native_manifest::{publish_table_dir, segment_full_path, staging_dir_for, write_manifest};

    /// Build a real, on-disk, two-segment native table directly against
    /// task 002's manifest API and this crate's own `ipc_cache`-compatible
    /// segment layout — no dependency on task 003's writer, so this test
    /// exercises exactly what `NativeTable` itself is responsible for.
    fn write_test_table(final_dir: &Path) -> (SchemaRef, Vec<RecordBatch>) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Float64, true),
        ]));

        let batch0 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None::<&str>])),
                Arc::new(Float64Array::from(vec![Some(1.5), Some(2.5), None])),
            ],
        )
        .unwrap();
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![4, 5])),
                Arc::new(StringArray::from(vec![Some("c"), Some("d")])),
                Arc::new(Float64Array::from(vec![Some(9.5), Some(-1.0)])),
            ],
        )
        .unwrap();

        let staging = staging_dir_for(final_dir);
        std::fs::create_dir_all(&staging).unwrap();

        let mut segments = Vec::new();
        for (id, batch) in [batch0.clone(), batch1.clone()].iter().enumerate() {
            let id = id as u32;
            let path = segment_full_path(&staging, id);
            let file = std::fs::File::create(&path).unwrap();
            let mut w = arrow::ipc::writer::FileWriter::try_new(file, &batch.schema()).unwrap();
            w.write(batch).unwrap();
            w.finish().unwrap();
            let byte_size = std::fs::metadata(&path).unwrap().len();
            segments.push(Segment {
                id,
                path: Segment::expected_file_name(id),
                row_count: batch.num_rows() as u64,
                byte_size,
                column_stats: native_manifest::compute_batch_stats(batch),
            });
        }

        let manifest = NativeManifest::build(
            &schema,
            NativeManifest::generate_table_id(),
            1,
            segments,
            1_700_000_000_000,
        )
        .unwrap();
        write_manifest(&staging, &manifest).unwrap();
        publish_table_dir(&staging, final_dir).unwrap();

        (schema, vec![batch0, batch1])
    }

    #[test]
    fn is_native_table_dir_detects_the_manifest() {
        let dir = tempfile::tempdir().unwrap();
        assert!(!is_native_table_dir(dir.path()));
        write_test_table(dir.path());
        assert!(is_native_table_dir(dir.path()));
    }

    #[test]
    fn scan_reads_every_segment_in_order_unprojected() {
        let dir = tempfile::tempdir().unwrap();
        let (schema, _batches) = write_test_table(dir.path());
        let table = NativeTable::try_new(dir.path()).unwrap();
        assert_eq!(*table.schema(), *schema);

        let scanned = table.scan(None).unwrap();
        let total_rows: usize = scanned.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);

        let ids: Vec<i64> = scanned
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
        assert_eq!(
            ids,
            vec![1, 2, 3, 4, 5],
            "segment 0 then segment 1, in id order"
        );
    }

    /// `schema()` reports the DECODED value type for a Dictionary-coerced
    /// column (mirroring `ParquetTable`'s transparent-dictionary model),
    /// while `scan()`'s actual returned batch keeps the real, physically
    /// Dictionary-encoded array unchanged. A manifest declaring
    /// `Dictionary(Int32, Utf8)` as a column's LOGICAL type (which task
    /// 003's writer does for low-cardinality strings) would otherwise reach
    /// `AggregationState::build_group_array` (`src/physical/morsel_agg.rs`,
    /// shared by `HashAggregateExec` and `MorselAggregateExec`), which has
    /// no `Dictionary` match arm — this test pins the fix at the boundary
    /// this module owns.
    #[test]
    fn schema_reports_the_decoded_type_for_a_dictionary_coerced_column_but_scan_keeps_the_real_array(
    ) {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::Int32Type;

        let dir = tempfile::tempdir().unwrap();
        let dict_schema = Arc::new(Schema::new(vec![Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )]));
        let keys = Int32Array::from(vec![0, 1, 0]);
        let values = StringArray::from(vec!["OPEN", "CLOSED"]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap();
        let batch = RecordBatch::try_new(dict_schema.clone(), vec![Arc::new(dict)]).unwrap();

        let staging = staging_dir_for(dir.path());
        std::fs::create_dir_all(&staging).unwrap();
        let path = segment_full_path(&staging, 0);
        let file = std::fs::File::create(&path).unwrap();
        let mut w = arrow::ipc::writer::FileWriter::try_new(file, &batch.schema()).unwrap();
        w.write(&batch).unwrap();
        w.finish().unwrap();
        let byte_size = std::fs::metadata(&path).unwrap().len();
        let segment = Segment {
            id: 0,
            path: Segment::expected_file_name(0),
            row_count: 3,
            byte_size,
            column_stats: native_manifest::compute_batch_stats(&batch),
        };
        let manifest =
            NativeManifest::build(&dict_schema, "dict-schema-test", 1, vec![segment], 0).unwrap();
        write_manifest(&staging, &manifest).unwrap();
        publish_table_dir(&staging, dir.path()).unwrap();

        let table = NativeTable::try_new(dir.path()).unwrap();

        // The catalog-visible schema is the DECODED value type.
        let logical = table.schema();
        assert_eq!(logical.field(0).data_type(), &DataType::Utf8);

        // The actual scanned array is UNCHANGED — still really Dictionary,
        // zero-copy, exactly as `ipc_cache::read_row_group` wrote it.
        let scanned = table.scan(None).unwrap();
        assert_eq!(scanned.len(), 1);
        assert_eq!(
            scanned[0].schema().field(0).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        );
    }

    #[test]
    fn scan_honors_projection() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        let table = NativeTable::try_new(dir.path()).unwrap();

        let scanned = table.scan(Some(&[0])).unwrap();
        for b in &scanned {
            assert_eq!(b.num_columns(), 1);
        }
    }

    #[test]
    fn statistics_matches_the_manifest_rollup_with_derived_ndv() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        let table = NativeTable::try_new(dir.path()).unwrap();

        let stats = table.statistics().expect("native tables report statistics");
        assert_eq!(stats.row_count, 5);
        assert!(stats.total_byte_size > 0);

        let id = stats.column_stats.get("id").expect("id stats present");
        assert_eq!(id.min_i64, Some(1));
        assert_eq!(id.max_i64, Some(5));
        assert_eq!(id.null_count, Some(0));
        // Dense surrogate-key range: NDV is exact.
        assert_eq!(id.ndv_est, Some(5));

        let name = stats.column_stats.get("name").expect("name stats present");
        assert_eq!(name.null_count, Some(1));
        assert_eq!(name.min_i64, None, "string column has no int zone-map");

        let price = stats
            .column_stats
            .get("price")
            .expect("price stats present");
        assert_eq!(price.min_f64, Some(-1.0));
        assert_eq!(price.max_f64, Some(9.5));
        assert_eq!(price.null_count, Some(1));
    }

    #[test]
    fn distributed_splits_is_one_split_per_segment_and_covers_every_row() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        let table = NativeTable::try_new(dir.path()).unwrap();

        let set = table
            .distributed_splits("t", 3)
            .expect("native tables implement distributed_splits")
            .expect("enumeration succeeds");
        assert_eq!(set.splits.len(), 2, "one split per segment");
        assert_eq!(set.total_rows, 5);
        assert_eq!(
            set.splits.iter().map(|s| s.num_rows).sum::<i64>(),
            set.total_rows
        );
        assert_eq!(
            set.splits.iter().map(|s| s.bytes).sum::<u64>(),
            set.total_bytes
        );
        // Canonical, deterministic order: segment 0 before segment 1.
        assert_eq!(set.splits[0].row_group, 0);
        assert_eq!(set.splits[1].row_group, 1);
        for s in &set.splits {
            assert_eq!(s.row_offset, 0, "whole-segment splits, no sub-slicing");
        }
    }

    #[test]
    fn shard_by_splits_restricts_scan_and_statistics_to_the_assigned_segments() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        let table = NativeTable::try_new(dir.path()).unwrap();

        let set = table.distributed_splits("t", 2).unwrap().unwrap();
        // Give this shard only segment 0's split (3 of the 5 rows).
        let seg0_only: Vec<Split> = set
            .splits
            .iter()
            .filter(|s| s.row_group == 0)
            .cloned()
            .collect();
        assert_eq!(seg0_only.len(), 1);

        let shard = table
            .shard_by_splits(&seg0_only)
            .expect("native tables implement shard_by_splits")
            .expect("sharding succeeds");

        let scanned = shard.scan(None).unwrap();
        let total_rows: usize = scanned.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3, "shard sees only segment 0's rows");

        let shard_stats = shard.statistics().unwrap();
        assert_eq!(shard_stats.row_count, 3);
        let id = shard_stats.column_stats.get("id").unwrap();
        assert_eq!(id.min_i64, Some(1));
        assert_eq!(
            id.max_i64,
            Some(3),
            "shard's own rollup, not the whole table's"
        );
    }

    #[test]
    fn identity_is_present_on_the_whole_table_and_absent_on_a_shard() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        let table = NativeTable::try_new(dir.path()).unwrap();

        let id = table
            .identity()
            .expect("a whole-table provider reports an identity");
        // table_id (UUID, 36 ASCII bytes) + 8 little-endian version bytes.
        assert_eq!(id.len(), 36 + 8);

        // Re-opening the SAME directory must yield the SAME identity (same
        // table_id, same version) — a cache keyed on this must be able to
        // recognize "this is the same data" across independent opens.
        let reopened = NativeTable::try_new(dir.path()).unwrap();
        assert_eq!(reopened.identity(), table.identity());

        // A sharded provider must NOT inherit the full table's identity —
        // see the doc comment on `identity()` for why (GPU cache aliasing).
        let set = table.distributed_splits("t", 2).unwrap().unwrap();
        let shard = table.shard_by_splits(&set.splits[..1]).unwrap().unwrap();
        assert_eq!(
            shard.identity(),
            None,
            "a sharded native table must not expose the whole table's identity"
        );
    }

    #[test]
    fn opening_a_missing_or_corrupt_table_is_a_clear_error_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let err = NativeTable::try_new(dir.path()).unwrap_err();
        assert!(matches!(err, crate::error::QueryError::Storage(_)));

        std::fs::write(native_manifest::manifest_path(dir.path()), b"{ not json").unwrap();
        let err = NativeTable::try_new(dir.path()).unwrap_err();
        assert!(matches!(err, crate::error::QueryError::Storage(_)));
    }

    // ---------- scan() admission control (task 006) ----------

    #[test]
    fn no_budget_means_scan_is_unaffected() {
        // The default from `try_new` alone (no `with_memory_budget` call) —
        // every test above this one already relies on this implicitly;
        // this test just makes the contract explicit.
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        let table = NativeTable::try_new(dir.path()).unwrap();
        assert!(table.scan(None).is_ok());
    }

    #[test]
    fn a_budget_comfortably_above_the_table_size_does_not_refuse() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        let table = NativeTable::try_new(dir.path())
            .unwrap()
            .with_memory_budget(Some(u64::MAX));
        let scanned = table.scan(None).unwrap();
        let total_rows: usize = scanned.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);
    }

    #[test]
    fn a_budget_below_the_table_size_refuses_cleanly_before_reading_anything() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        let table = NativeTable::try_new(dir.path()).unwrap();
        let real_size = table.statistics().unwrap().total_byte_size;
        assert!(real_size > 0, "the fixture must have a nonzero byte size");

        let capped = table.with_memory_budget(Some(real_size - 1));
        let err = capped.scan(None).unwrap_err();
        assert!(
            matches!(err, crate::error::QueryError::Execution(_)),
            "{err}"
        );
        let msg = err.to_string();
        assert!(msg.contains("memory safety budget"), "{msg}");
        assert!(msg.contains("--memory-limit"), "{msg}");
    }

    #[test]
    fn a_budget_exactly_at_the_table_size_does_not_refuse() {
        // The check is a strict `>`, not `>=` -- a table that fits EXACTLY
        // inside the declared budget must be allowed, not treated as an
        // off-by-one violation.
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        let table = NativeTable::try_new(dir.path()).unwrap();
        let real_size = table.statistics().unwrap().total_byte_size;

        let exact = table.with_memory_budget(Some(real_size));
        assert!(exact.scan(None).is_ok());
    }

    #[test]
    fn shard_by_splits_propagates_the_budget_and_can_itself_refuse() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        let table = NativeTable::try_new(dir.path()).unwrap();
        let real_size = table.statistics().unwrap().total_byte_size;
        let capped = table.with_memory_budget(Some(real_size - 1));

        let set = capped.distributed_splits("t", 2).unwrap().unwrap();
        let seg0_only: Vec<Split> = set
            .splits
            .iter()
            .filter(|s| s.row_group == 0)
            .cloned()
            .collect();
        let shard = capped.shard_by_splits(&seg0_only).unwrap().unwrap();

        // The shard's OWN (smaller) rollup is what gets checked -- a shard
        // that individually fits under the whole table's budget must not be
        // refused just because the WHOLE table would not have fit.
        let shard_size = shard.statistics().unwrap().total_byte_size;
        assert!(
            shard_size < real_size,
            "a one-segment shard must be smaller than the whole table"
        );
        assert!(
            shard.scan(None).is_ok(),
            "a shard that individually fits under the inherited budget must succeed"
        );
    }
}
