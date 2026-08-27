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
use crate::planner::{BinaryOp, Expr, ScalarValue, UnaryOp};
use crate::storage::ipc_cache;
use crate::storage::native_manifest::{self, ColumnStats, NativeManifest, Segment};
use crate::storage::row_group_pruning::{eval_range, eval_range_f64, flip_op};
use arrow::array::{ArrayRef, BooleanArray};
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

/// Filter tombstoned rows out of one segment's already-read `batches`
/// (native-tables-mutation epic, task 003). `deleted_rows` names LOCAL
/// positions in the segment's own combined on-disk row order — the same
/// order `ipc_cache::read_row_group`'s returned batches are already in
/// (stable on-disk block order, never reordered; `native_write.rs` already
/// calls `ipc_cache::reslice_large` before writing, so multiple
/// ~65,536-row batches per segment is the NORMAL case, not an edge case) —
/// so a running local-row offset across `batches` correctly maps each
/// batch's row indices back to the positions `deleted_rows` names. Only
/// called when `deleted_rows` is non-empty (the caller, `scan`, already
/// fast-paths the common empty case before reaching here).
///
/// `deleted_rows` is sorted and deduplicated (`NativeManifest::validate`
/// enforces this), so a single monotonic cursor into it — never
/// rewinding, since batches are processed in increasing offset order —
/// finds each batch's slice in amortized O(1) rather than re-scanning the
/// whole `Vec` per batch. A batch with NO deleted row in its range is
/// passed through completely unchanged (zero-copy, no `compute::filter`
/// call at all) — the common case for a lightly-deleted segment spread
/// across many batches.
fn filter_deleted_rows(
    batches: Vec<RecordBatch>,
    deleted_rows: &[u32],
) -> Result<Vec<RecordBatch>> {
    let mut out = Vec::with_capacity(batches.len());
    let mut local_offset: u32 = 0;
    let mut cursor = 0usize;
    for batch in batches {
        let n = batch.num_rows() as u32;
        let end = local_offset + n;
        while cursor < deleted_rows.len() && deleted_rows[cursor] < local_offset {
            cursor += 1;
        }
        if cursor >= deleted_rows.len() || deleted_rows[cursor] >= end {
            // No deleted row falls inside this batch's range.
            out.push(batch);
            local_offset = end;
            continue;
        }
        let mut keep = vec![true; n as usize];
        while cursor < deleted_rows.len() && deleted_rows[cursor] < end {
            keep[(deleted_rows[cursor] - local_offset) as usize] = false;
            cursor += 1;
        }
        let mask = BooleanArray::from(keep);
        let cols: Result<Vec<ArrayRef>> = batch
            .columns()
            .iter()
            .map(|c| arrow::compute::filter(c.as_ref(), &mask).map_err(Into::into))
            .collect();
        out.push(RecordBatch::try_new(batch.schema(), cols?)?);
        local_offset = end;
    }
    Ok(out)
}

// ============================================================================
// Segment-level scan pruning (native-table-pruning epic, task 001)
//
// Mirrors `row_group_pruning.rs::row_group_might_match`'s exact recursive
// shape (AND/OR/NOT/BETWEEN/InList/comparison) and reuses its low-level
// range-evaluation helpers (`eval_range`/`eval_range_f64`/`flip_op`)
// UNCHANGED — only the top-level walk and the statistics lookup are new,
// because a segment's `ColumnStats` (`min_i64`/`max_i64`/`min_f64`/
// `max_f64`) is a fundamentally different representation from parquet's
// `RowGroupMetaData`/`Statistics`, so that part cannot be reused verbatim.
// Same philosophy as the Parquet version, restated for this module: skip a
// segment ONLY when the predicate is PROVABLY unsatisfiable against its
// stats; absent stats for a column (every string/binary column today, or an
// all-null segment), an unrecognized predicate shape (NOT, unsupported
// literal type, non-column-vs-literal comparison), or any ambiguity all
// return `true` (never skip). `NativeTable::scan_with_filter`'s caller
// (`FilterExec`, via `PhysicalPlanner`) always re-applies the FULL predicate
// to whatever segments ARE read, so a wrong "might match" verdict can only
// cost performance, never correctness.
// ============================================================================

/// Evaluate whether a segment might contain rows matching `predicate`, given
/// only that segment's own `column_stats`. See the section doc above.
fn segment_might_match(predicate: &Expr, stats: &BTreeMap<String, ColumnStats>) -> bool {
    match predicate {
        Expr::BinaryExpr { left, op, right } => match op {
            BinaryOp::And => {
                segment_might_match(left, stats) && segment_might_match(right, stats)
            }
            BinaryOp::Or => segment_might_match(left, stats) || segment_might_match(right, stats),
            _ => check_comparison(left, op, right, stats),
        },
        Expr::UnaryExpr {
            op: UnaryOp::Not, ..
        } => true, // Conservative, mirrors row_group_pruning's own NOT handling.
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            if *negated {
                true // Conservative for NOT BETWEEN.
            } else {
                // BETWEEN: expr >= low AND expr <= high.
                let ge_low = segment_might_match(
                    &Expr::BinaryExpr {
                        left: expr.clone(),
                        op: BinaryOp::GtEq,
                        right: low.clone(),
                    },
                    stats,
                );
                let le_high = segment_might_match(
                    &Expr::BinaryExpr {
                        left: expr.clone(),
                        op: BinaryOp::LtEq,
                        right: high.clone(),
                    },
                    stats,
                );
                ge_low && le_high
            }
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => {
            if *negated {
                true // Conservative for NOT IN.
            } else {
                // IN (v1, v2, ...): at least one value must be possible.
                list.iter().any(|val| {
                    segment_might_match(
                        &Expr::BinaryExpr {
                            left: expr.clone(),
                            op: BinaryOp::Eq,
                            right: Box::new(val.clone()),
                        },
                        stats,
                    )
                })
            }
        }
        _ => true, // Conservative: include if the shape isn't recognized.
    }
}

/// Check a `col op literal` (or `literal op col`) comparison against a
/// segment's stats. Column lookup uses the SAME (unqualified, lowercase) key
/// convention `compute_batch_stats`/`ColumnStats` already use, so a
/// qualified predicate column (`lineitem.l_shipdate`) resolves identically
/// to an unqualified one.
fn check_comparison(
    left: &Expr,
    op: &BinaryOp,
    right: &Expr,
    stats: &BTreeMap<String, ColumnStats>,
) -> bool {
    let (col_name, literal, flipped) = match (left, right) {
        (Expr::Column(col), Expr::Literal(lit)) => (col.name.as_str(), lit, false),
        (Expr::Literal(lit), Expr::Column(col)) => (col.name.as_str(), lit, true),
        _ => return true, // Not a simple column-vs-literal comparison.
    };
    let cs = match stats.get(&col_name.to_lowercase()) {
        Some(cs) => cs,
        None => return true, // No stats recorded for this column at all.
    };
    let effective_op = if flipped { flip_op(op) } else { *op };
    match literal {
        ScalarValue::Int64(v) => check_i64_stats(cs, effective_op, *v),
        ScalarValue::Int32(v) => check_i64_stats(cs, effective_op, *v as i64),
        ScalarValue::Int16(v) => check_i64_stats(cs, effective_op, *v as i64),
        ScalarValue::Int8(v) => check_i64_stats(cs, effective_op, *v as i64),
        ScalarValue::Date32(v) => check_i64_stats(cs, effective_op, *v as i64),
        ScalarValue::Date64(v) => check_i64_stats(cs, effective_op, *v),
        ScalarValue::Timestamp(v) => check_i64_stats(cs, effective_op, *v),
        ScalarValue::Float64(v) => check_f64_stats(cs, effective_op, v.into_inner()),
        ScalarValue::Float32(v) => check_f64_stats(cs, effective_op, v.into_inner() as f64),
        // Utf8/Boolean/Decimal128/UInt*/List/etc: `ColumnStats` has no
        // zone-map for these (string/binary columns are explicitly out of
        // scope for this PRD; the others simply aren't populated by
        // `column_stats_for_array`) — always scan, never skip.
        _ => true,
    }
}

/// `min_i64`/`max_i64` are only ever BOTH present or BOTH absent (see
/// `column_stats_for_array`) — absence means either a non-integer-classed
/// column or a segment with zero non-null values for it, either of which
/// must be scanned rather than skipped.
fn check_i64_stats(cs: &ColumnStats, op: BinaryOp, val: i64) -> bool {
    match (cs.min_i64, cs.max_i64) {
        (Some(min), Some(max)) => eval_range(op, val, min, max),
        _ => true,
    }
}

/// See [`check_i64_stats`]; same reasoning for `min_f64`/`max_f64`.
fn check_f64_stats(cs: &ColumnStats, op: BinaryOp, val: f64) -> bool {
    match (cs.min_f64, cs.max_f64) {
        (Some(min), Some(max)) => eval_range_f64(op, val, min, max),
        _ => true,
    }
}

/// `true` when `QE_DEBUG_NATIVE_PRUNING` is set — matches this codebase's
/// established env-gated diagnostic-switch convention (`QE_DEBUG_SCAN_BUDGET`,
/// `QE_DEBUG_ROLLUP`, `QE_GPU_DEBUG`, ...): zero cost when unset, and lets a
/// segment-skip decision be confirmed directly rather than inferred from
/// wall-clock time.
fn native_pruning_debug_enabled() -> bool {
    std::env::var("QE_DEBUG_NATIVE_PRUNING").is_ok()
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

    /// Reads every active segment, then filters out whatever
    /// `Segment::deleted_rows` (native-tables-mutation epic, task 003)
    /// tombstones — via `filter_deleted_rows`, the SAME helper
    /// `scan_with_filter` below calls for whatever segments IT reads, so a
    /// deleted row can never reach any consumer no matter which entry point
    /// got there (this generic scan, `scan_with_filter`'s own pruning path
    /// below, the dense-direct-address fast path — which calls
    /// `scan_with_filter` with `filter: None` and so lands right back on
    /// this function unchanged — or a distributed shard's own scan). A
    /// segment with an empty `deleted_rows` (every table phase 1 or task
    /// 002 ever wrote, and the common case even for a mutated table's
    /// untouched segments) takes a fast, allocation-free path straight
    /// through — zero behavior or performance change from before this task.
    fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        self.check_scan_budget()?;
        let mut out = Vec::new();
        for seg in self.active_segments() {
            let batches = ipc_cache::read_row_group(&self.dir, seg.id as usize, projection, None)?;
            if seg.deleted_rows.is_empty() {
                out.extend(batches);
                continue;
            }
            out.extend(filter_deleted_rows(batches, &seg.deleted_rows)?);
        }
        Ok(out)
    }

    /// Segment-level scan pruning (native-table-pruning epic, task 001).
    ///
    /// `PhysicalPlanner::create_physical_plan_inner`'s `LogicalPlan::Scan`
    /// arm already calls `provider.scan_with_filter(projection,
    /// node.filter.as_ref())` generically for every non-streaming-Parquet
    /// provider (confirmed by reading that call site, not assumed — see
    /// `src/physical/planner.rs`, the "No cache: use scan_with_filter..."
    /// branch) — the caller-side wiring is ALREADY provider-agnostic and
    /// needed no change; only this override (previously the trait's
    /// default, which silently ignored `filter` and called `scan()`) was
    /// missing.
    ///
    /// For each active segment, `segment_might_match` evaluates `filter`
    /// against that segment's own `ColumnStats`; a segment PROVABLY unable
    /// to match is skipped entirely — `ipc_cache::read_row_group` is never
    /// called for it, so it is never decoded. Every segment that IS read
    /// still goes through the exact same deletion-vector filtering `scan()`
    /// applies (`filter_deleted_rows`), unchanged — pruning only decides
    /// WHETHER a segment is read, never what happens to the rows once it
    /// is. `QE_DEBUG_NATIVE_PRUNING=1` traces every segment's skip/scan
    /// decision plus a per-call summary to stderr, so a skip can be
    /// confirmed directly rather than inferred from wall-clock time. A
    /// `filter: None` call (or a predicate this module can't recognize at
    /// all) degrades to exactly `scan()`'s own behavior.
    fn scan_with_filter(
        &self,
        projection: Option<&[usize]>,
        filter: Option<&Expr>,
    ) -> Result<Vec<RecordBatch>> {
        self.check_scan_budget()?;
        let Some(predicate) = filter else {
            return self.scan(projection);
        };
        let debug = native_pruning_debug_enabled();
        let mut out = Vec::new();
        let mut scanned = 0usize;
        let mut skipped = 0usize;
        for seg in self.active_segments() {
            if !segment_might_match(predicate, &seg.column_stats) {
                skipped += 1;
                if debug {
                    eprintln!(
                        "[native_pruning] table={} segment={} SKIP (predicate provably \
                         unsatisfiable against this segment's stats)",
                        self.dir.display(),
                        seg.id
                    );
                }
                continue;
            }
            scanned += 1;
            if debug {
                eprintln!(
                    "[native_pruning] table={} segment={} scan",
                    self.dir.display(),
                    seg.id
                );
            }
            let batches = ipc_cache::read_row_group(&self.dir, seg.id as usize, projection, None)?;
            if seg.deleted_rows.is_empty() {
                out.extend(batches);
                continue;
            }
            out.extend(filter_deleted_rows(batches, &seg.deleted_rows)?);
        }
        if debug {
            eprintln!(
                "[native_pruning] table={} scanned={} skipped={} total={}",
                self.dir.display(),
                scanned,
                skipped,
                scanned + skipped
            );
        }
        Ok(out)
    }

    fn statistics(&self) -> Option<TableStatistics> {
        let segs = self.active_segments();
        // LOGICAL (post-delete, visible) row count — a NEW, separate
        // computation from `total_byte_size`/the column-stats rollup below,
        // both of which intentionally keep reflecting PHYSICAL/write-time
        // content (task 001/003's Decision 1: deletion vectors never
        // shrink a segment's on-disk bytes, and re-deriving exact
        // post-delete min/max/NDV bounds is not worth chasing — a wider
        // bound is always safe). For a table with no deletions anywhere
        // this is byte-for-byte the same number the pre-task-003 code
        // computed (`live_row_count()` is a no-op subtraction of 0).
        let row_count: u64 = segs.iter().map(|s| s.live_row_count()).sum();
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
    use native_manifest::{
        publish_table_dir, read_manifest, segment_full_path, staging_dir_for, write_manifest,
        write_manifest_atomic,
    };

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
                deleted_rows: Vec::new(),
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
            deleted_rows: Vec::new(),
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

    // ---------- deletion-vector consultation at scan time (task 003) ----------
    //
    // These tests exercise `NativeTable::scan`/`statistics` in ISOLATION
    // from `native_delete.rs`'s own editing logic — a manifest is built by
    // `write_test_table` (task 002's proven fixture, unmodified) and then
    // its `deleted_rows` edited directly, mirroring the exact separation
    // task 001's design established: the READ side (this file) must be
    // correct independent of whatever EDITS the vector (`native_delete.rs`,
    // its own test module).

    fn set_deleted_rows(dir: &Path, segment_id: u32, deleted_rows: Vec<u32>) {
        let mut manifest = read_manifest(dir).unwrap();
        for seg in manifest.segments.iter_mut() {
            if seg.id == segment_id {
                seg.deleted_rows = deleted_rows.clone();
            }
        }
        write_manifest_atomic(dir, &manifest).unwrap();
    }

    fn scanned_ids(table: &NativeTable) -> Vec<i64> {
        table
            .scan(None)
            .unwrap()
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    #[test]
    fn scan_with_no_deletions_is_byte_for_byte_the_pre_task_003_behavior() {
        // Every table phase 1/task 002 ever wrote has empty `deleted_rows`
        // on every segment -- this is the "zero behavior change" contract.
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        let table = NativeTable::try_new(dir.path()).unwrap();
        assert_eq!(scanned_ids(&table), vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn scan_filters_out_a_deleted_row_within_a_segment() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path()); // segment 0: ids [1,2,3] at local [0,1,2]
        set_deleted_rows(dir.path(), 0, vec![1]); // local position 1 -> id 2

        let table = NativeTable::try_new(dir.path()).unwrap();
        assert_eq!(
            scanned_ids(&table),
            vec![1, 3, 4, 5],
            "id 2 (segment 0, local position 1) must be excluded, everything else present"
        );
    }

    #[test]
    fn scan_applies_deletions_to_the_correct_segment_only() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path()); // segment 0: ids [1,2,3]; segment 1: ids [4,5]
        set_deleted_rows(dir.path(), 0, vec![0, 2]); // ids 1 and 3 gone from segment 0
        set_deleted_rows(dir.path(), 1, vec![1]); // id 5 gone from segment 1

        let table = NativeTable::try_new(dir.path()).unwrap();
        assert_eq!(
            scanned_ids(&table),
            vec![2, 4],
            "a segment's deletions must never leak onto -- or be silently applied to all of --
             the other segment"
        );
    }

    #[test]
    fn scan_deleting_every_row_of_one_segment_still_returns_the_other_segments_rows() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        set_deleted_rows(dir.path(), 0, vec![0, 1, 2]); // every row of segment 0

        let table = NativeTable::try_new(dir.path()).unwrap();
        assert_eq!(
            scanned_ids(&table),
            vec![4, 5],
            "a wholly-tombstoned segment (still present in the manifest here -- \
             native_delete.rs is what actually drops it) must scan as zero rows, not error"
        );
    }

    #[test]
    fn statistics_row_count_reflects_deletions_but_byte_size_and_column_stats_do_not() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        let before = NativeTable::try_new(dir.path())
            .unwrap()
            .statistics()
            .unwrap();

        set_deleted_rows(dir.path(), 0, vec![0]); // delete id=1

        let after = NativeTable::try_new(dir.path())
            .unwrap()
            .statistics()
            .unwrap();
        assert_eq!(
            after.row_count,
            before.row_count - 1,
            "logical row count must drop by exactly the number of deleted rows"
        );
        assert_eq!(
            after.total_byte_size, before.total_byte_size,
            "physical byte size must NOT shrink -- check_scan_budget's memory-safety formula \
             relies on this staying the whole active segment set's true on-disk size"
        );
        let id_before = before.column_stats.get("id").unwrap();
        let id_after = after.column_stats.get("id").unwrap();
        assert_eq!(
            id_after.min_i64, id_before.min_i64,
            "column-stats rollup is deliberately NOT recomputed on delete (task 001's decision) \
             -- a wider bound is always safe"
        );
    }

    #[test]
    fn shard_by_splits_scan_and_statistics_respect_the_shards_own_segment_deletions() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        set_deleted_rows(dir.path(), 0, vec![0]); // delete id=1 from segment 0

        let table = NativeTable::try_new(dir.path()).unwrap();
        let set = table.distributed_splits("t", 2).unwrap().unwrap();
        let seg0_only: Vec<Split> = set
            .splits
            .iter()
            .filter(|s| s.row_group == 0)
            .cloned()
            .collect();
        let shard = table.shard_by_splits(&seg0_only).unwrap().unwrap();

        let scanned = shard.scan(None).unwrap();
        let total_rows: usize = scanned.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            total_rows, 2,
            "a distributed shard's own scan must apply its own segment's deletions, not just \
             the whole-table view"
        );
        assert_eq!(shard.statistics().unwrap().row_count, 2);
    }

    // ---------- segment-level scan pruning (native-table-pruning epic, task 001) ----------

    use crate::planner::Column;

    fn i64_stats(min: i64, max: i64, null_count: u64) -> BTreeMap<String, ColumnStats> {
        let mut m = BTreeMap::new();
        m.insert(
            "id".to_string(),
            ColumnStats {
                min_i64: Some(min),
                max_i64: Some(max),
                null_count: Some(null_count),
                ..Default::default()
            },
        );
        m
    }

    fn col(name: &str) -> Expr {
        Expr::Column(Column::new(name))
    }

    fn lit_i64(v: i64) -> Expr {
        Expr::Literal(ScalarValue::Int64(v))
    }

    fn cmp(left: Expr, op: BinaryOp, right: Expr) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    #[test]
    fn segment_might_match_simple_comparisons() {
        let stats = i64_stats(1, 10, 0);
        assert!(segment_might_match(
            &cmp(col("id"), BinaryOp::Eq, lit_i64(5)),
            &stats
        ));
        assert!(
            !segment_might_match(&cmp(col("id"), BinaryOp::Eq, lit_i64(50)), &stats),
            "50 is outside [1, 10] -- must be provably unsatisfiable"
        );
        assert!(!segment_might_match(
            &cmp(col("id"), BinaryOp::Lt, lit_i64(1)),
            &stats
        ));
        assert!(segment_might_match(
            &cmp(col("id"), BinaryOp::Lt, lit_i64(2)),
            &stats
        ));
        assert!(!segment_might_match(
            &cmp(col("id"), BinaryOp::Gt, lit_i64(10)),
            &stats
        ));
    }

    #[test]
    fn segment_might_match_handles_flipped_literal_column_order() {
        let stats = i64_stats(1, 10, 0);
        // `50 = id` must behave identically to `id = 50`.
        assert!(!segment_might_match(
            &cmp(lit_i64(50), BinaryOp::Eq, col("id")),
            &stats
        ));
        // `20 < id` (flips to `id > 20`) is unsatisfiable against max=10.
        assert!(!segment_might_match(
            &cmp(lit_i64(20), BinaryOp::Lt, col("id")),
            &stats
        ));
    }

    #[test]
    fn segment_might_match_and_prunes_when_either_side_proves_impossible() {
        let stats = i64_stats(1, 10, 0);
        // id > 5 AND id < 8: both sides possible -> must scan.
        assert!(segment_might_match(
            &cmp(
                cmp(col("id"), BinaryOp::Gt, lit_i64(5)),
                BinaryOp::And,
                cmp(col("id"), BinaryOp::Lt, lit_i64(8)),
            ),
            &stats
        ));
        // id > 20 AND id < 30: both provably impossible on their own -> skip.
        assert!(!segment_might_match(
            &cmp(
                cmp(col("id"), BinaryOp::Gt, lit_i64(20)),
                BinaryOp::And,
                cmp(col("id"), BinaryOp::Lt, lit_i64(30)),
            ),
            &stats
        ));
        // id > 20 AND id < 5000: the first conjunct alone is impossible, so
        // AND must skip even though the second conjunct alone would not.
        assert!(!segment_might_match(
            &cmp(
                cmp(col("id"), BinaryOp::Gt, lit_i64(20)),
                BinaryOp::And,
                cmp(col("id"), BinaryOp::Lt, lit_i64(5000)),
            ),
            &stats
        ));
    }

    #[test]
    fn segment_might_match_or_requires_both_sides_to_fail_to_skip() {
        let stats = i64_stats(1, 10, 0);
        // id = 50 OR id = 5: the second side is possible -> must scan.
        assert!(segment_might_match(
            &cmp(
                cmp(col("id"), BinaryOp::Eq, lit_i64(50)),
                BinaryOp::Or,
                cmp(col("id"), BinaryOp::Eq, lit_i64(5)),
            ),
            &stats
        ));
        // id = 50 OR id = 60: both sides impossible -> skip.
        assert!(!segment_might_match(
            &cmp(
                cmp(col("id"), BinaryOp::Eq, lit_i64(50)),
                BinaryOp::Or,
                cmp(col("id"), BinaryOp::Eq, lit_i64(60)),
            ),
            &stats
        ));
    }

    #[test]
    fn segment_might_match_between_prunes() {
        let stats = i64_stats(1, 10, 0);
        assert!(!segment_might_match(
            &Expr::Between {
                expr: Box::new(col("id")),
                low: Box::new(lit_i64(20)),
                high: Box::new(lit_i64(30)),
                negated: false,
            },
            &stats
        ));
        assert!(segment_might_match(
            &Expr::Between {
                expr: Box::new(col("id")),
                low: Box::new(lit_i64(5)),
                high: Box::new(lit_i64(8)),
                negated: false,
            },
            &stats
        ));
        // NOT BETWEEN is always conservative (never skips).
        assert!(segment_might_match(
            &Expr::Between {
                expr: Box::new(col("id")),
                low: Box::new(lit_i64(1)),
                high: Box::new(lit_i64(10)),
                negated: true,
            },
            &stats
        ));
    }

    #[test]
    fn segment_might_match_inlist_prunes_only_when_every_value_is_impossible() {
        let stats = i64_stats(1, 10, 0);
        assert!(!segment_might_match(
            &Expr::InList {
                expr: Box::new(col("id")),
                list: vec![lit_i64(50), lit_i64(60), lit_i64(70)],
                negated: false,
            },
            &stats
        ));
        assert!(segment_might_match(
            &Expr::InList {
                expr: Box::new(col("id")),
                list: vec![lit_i64(50), lit_i64(5), lit_i64(70)],
                negated: false,
            },
            &stats
        ));
        // NOT IN is always conservative (never skips).
        assert!(segment_might_match(
            &Expr::InList {
                expr: Box::new(col("id")),
                list: vec![lit_i64(1), lit_i64(2), lit_i64(3)],
                negated: true,
            },
            &stats
        ));
    }

    #[test]
    fn segment_might_match_not_is_always_conservative() {
        let stats = i64_stats(1, 10, 0);
        // NOT (id = 50) is logically always-true here, but this module
        // deliberately never computes a "definitely matches" complement for
        // `might_match` (mirrors row_group_pruning.rs's own NOT handling) --
        // it must always scan rather than reason about it.
        assert!(segment_might_match(
            &Expr::UnaryExpr {
                op: UnaryOp::Not,
                expr: Box::new(cmp(col("id"), BinaryOp::Eq, lit_i64(50))),
            },
            &stats
        ));
    }

    #[test]
    fn segment_might_match_column_with_no_stats_always_scans() {
        // A string column (or any column absent from `column_stats` --
        // e.g. because the segment had zero non-null values) must never be
        // skipped: `ColumnStats` has no zone-map for it.
        let stats: BTreeMap<String, ColumnStats> = BTreeMap::new();
        assert!(segment_might_match(
            &cmp(col("name"), BinaryOp::Eq, Expr::Literal(ScalarValue::Utf8("z".into()))),
            &stats
        ));
    }

    #[test]
    fn segment_might_match_unrecognized_predicate_shape_always_scans() {
        let stats = i64_stats(1, 10, 0);
        // A non-comparison, non-column-vs-literal shape (here: a scalar
        // function call) is not recognized -- must always scan.
        let unrecognized = Expr::ScalarFunc {
            func: crate::planner::ScalarFunction::Abs,
            args: vec![col("id")],
        };
        assert!(segment_might_match(&unrecognized, &stats));
    }

    #[test]
    fn segment_might_match_qualified_column_name_resolves_like_unqualified() {
        let stats = i64_stats(1, 10, 0);
        let qualified = Expr::Column(Column::new_qualified("t", "id"));
        assert!(!segment_might_match(
            &cmp(qualified, BinaryOp::Eq, lit_i64(50)),
            &stats
        ));
    }

    /// End-to-end: `scan_with_filter` against a REAL two-segment table
    /// (segment 0: ids [1,2,3], segment 1: ids [4,5]) actually skips the
    /// segment its own stats prove can't match, and the surviving rows are
    /// exactly the ones the predicate should keep -- pruning changes WHICH
    /// segments are read, never the correctness of what comes back.
    #[test]
    fn scan_with_filter_skips_a_provably_unsatisfiable_segment_end_to_end() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path());
        let table = NativeTable::try_new(dir.path()).unwrap();

        // id <= 3 is entirely inside segment 0's [1,3] range -- segment 1
        // ([4,5]) must be skipped.
        let pred = cmp(col("id"), BinaryOp::LtEq, lit_i64(3));
        let scanned = table.scan_with_filter(None, Some(&pred)).unwrap();
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
        assert_eq!(ids, vec![1, 2, 3]);

        // id >= 4 is entirely inside segment 1's [4,5] range -- segment 0
        // ([1,3]) must be skipped.
        let pred2 = cmp(col("id"), BinaryOp::GtEq, lit_i64(4));
        let scanned2 = table.scan_with_filter(None, Some(&pred2)).unwrap();
        let ids2: Vec<i64> = scanned2
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
        assert_eq!(ids2, vec![4, 5]);

        // A predicate matching nothing in either segment's range still
        // returns an empty (never wrong) result, not an error.
        let pred3 = cmp(col("id"), BinaryOp::Eq, lit_i64(999));
        let scanned3 = table.scan_with_filter(None, Some(&pred3)).unwrap();
        let total3: usize = scanned3.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total3, 0);

        // `filter: None` behaves exactly like `scan()`.
        let unfiltered = table.scan_with_filter(None, None).unwrap();
        let total_unfiltered: usize = unfiltered.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_unfiltered, 5);
    }

    /// Pruning composes correctly with deletion vectors (native-tables-
    /// mutation epic): a segment that survives pruning still has its own
    /// `deleted_rows` applied, unchanged from `scan()`'s own behavior.
    #[test]
    fn scan_with_filter_still_applies_deletion_vectors_to_segments_it_does_read() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path()); // segment 0: ids [1,2,3]; segment 1: ids [4,5]
        set_deleted_rows(dir.path(), 0, vec![1]); // delete local position 1 -> id 2

        let table = NativeTable::try_new(dir.path()).unwrap();
        // id <= 3 keeps only segment 0 (pruned away segment 1), and within
        // segment 0 the deletion vector must still drop id=2.
        let pred = cmp(col("id"), BinaryOp::LtEq, lit_i64(3));
        let scanned = table.scan_with_filter(None, Some(&pred)).unwrap();
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
        assert_eq!(ids, vec![1, 3], "id=2 must still be excluded by the deletion vector");
    }
}
