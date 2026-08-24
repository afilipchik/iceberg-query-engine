//! Native table write path: bulk-load a `RecordBatch` stream — from a
//! parquet directory, an Iceberg table, a Lance dataset, or an arbitrary
//! query result — into a new native table (task 002's manifest format).
//! Task 003 of the native-tables-foundation epic; sibling to
//! `native_manifest.rs` (002, which owns the format and is NOT modified by
//! this file) and `native_table.rs` (004, the `TableProvider` read side,
//! also NOT touched here).
//!
//! # The entrypoint a future `CREATE TABLE ... AS SELECT` calls
//!
//! [`write_batches`] is deliberately shaped so a query result can be
//! streamed straight in: `stream: RecordBatchStream` (`BoxStream<'static,
//! Result<RecordBatch>>`), not `Vec<RecordBatch>`. Task 001's finding
//! (`.claude/epics/native-tables-foundation/001.md`'s Outcome) was that
//! `ExecutionContext::sql()` fully materializes a query result via
//! `try_collect()` before returning — modeling this write path on that
//! would reintroduce the exact unbounded-memory risk the "streaming, no
//! double materialization" architecture decision exists to prevent. A
//! caller with a `PhysicalOperator`'s output should drive
//! `physical.execute(partition)`'s stream (merging multiple partitions with
//! `futures::stream::select_all` first, if there is more than one) directly
//! into [`write_batches`] — never through `ExecutionContext::sql()`.
//!
//! # Memory discipline: bounded per-segment buffering, not zero buffering
//!
//! This does NOT hold the whole stream in memory. It buffers only up to one
//! segment's worth of rows at a time (default
//! [`NativeWriteOptions::target_rows_per_segment`] = 1,000,000 — the same
//! "accumulate up to a bound, then flush" discipline a Parquet row group
//! already uses), computes that segment's statistics and dictionary
//! encoding from the buffered rows, writes its Arrow IPC file, and clears
//! the buffer before accepting more. Peak memory is therefore bounded by
//! one segment, never by the size of the source.
//!
//! # Dictionary encoding: decided once, applied uniformly
//!
//! `ipc_cache.rs` v2 decides dictionary-candidacy per PARQUET COLUMN CHUNK
//! metadata (`col.dictionary_page_offset()`), which arbitrary `RecordBatch`
//! sources (a query result, Iceberg, Lance) do not carry — task 002's
//! Technical Details flagged this as needing an Arrow-side equivalent. This
//! module's equivalent: every plain `Utf8` column is a dictionary
//! CANDIDATE; the first segment actually written casts each candidate to
//! `Dictionary(Int32, Utf8)` and keeps that encoding only if the resulting
//! dictionary has at most [`NativeWriteOptions::dict_max_cardinality`]
//! distinct values (default 4096, mirroring `ipc_cache.rs::build_sidecar`'s
//! own "demote wide dictionaries back to plain" threshold) — otherwise the
//! column stays plain `Utf8`. That decision, once made, is then applied
//! UNIFORMLY to every later segment, regardless of that segment's own local
//! cardinality.
//!
//! This is a deliberate difference from `ipc_cache.rs`, which re-decides
//! per row group (so two row groups of the same parquet file can
//! theoretically disagree on whether the same column is dictionary-typed —
//! harmless there only because `sidecar_dict_cols` always consults row
//! group 0). A native table's manifest declares ONE Arrow type per column
//! for the WHOLE table (`NativeManifest::arrow_schema`), so every segment
//! file must actually agree with it — deciding once and locking it in is
//! what makes that true by construction rather than by accident. For every
//! TPC-H-shaped column this targets (`l_returnflag`, `o_orderstatus`,
//! `l_shipmode`, ...), cardinality is globally low and this makes no
//! difference in practice.
//!
//! # What this module does NOT do
//!
//! No `TableProvider` implementation (that is `native_table.rs`, task 004 —
//! a sibling agent's file, not touched here). [`read_back`] exists only as
//! a minimal, self-contained round-trip reader for THIS module's own tests
//! and its `load-native` CLI validation command: it reads a manifest and
//! every segment through the same already-`pub` `ipc_cache::
//! read_row_group` task 004's provider will also call, but has none of a
//! real provider's projection/filter/statistics/streaming machinery. Do not
//! grow it into one; that is task 004's job.
//!
//! # Append: a DIFFERENT atomic-publish model from Create/Overwrite
//! (native-tables-mutation epic, task 002)
//!
//! Everything above this section describes `Create`/`Overwrite`: stage a
//! COMPLETE new table in a fresh staging directory, then atomically
//! replace the whole destination directory
//! (`native_manifest::publish_table_dir` — `remove_dir_all` + `rename`).
//! Task 001's design spike (`.claude/epics/native-tables-mutation/001.md`'s
//! Outcome, Decision 4) found this is **not** safely reusable unchanged for
//! an INCREMENTAL write: `Append` must preserve every pre-existing segment
//! file, and staging a fresh directory containing only the NEW segments
//! would make `publish_table_dir` delete every OLD one out from under the
//! table. `Append` therefore uses a materially different mechanism, built
//! from three pieces with clean, independently-reusable boundaries — named
//! here precisely because task 003 (DELETE) and task 004 (UPDATE) are
//! expected to reuse them directly rather than re-derive this shape:
//!
//! 1. [`lock_table_for_write`] — acquires the single-writer advisory lock
//!    (`std::fs::File::try_lock()`, task 001's Decision 5) on a sibling
//!    `<table>.lock` file. Non-blocking: a concurrent writer gets a named
//!    `QueryError::Storage` immediately, never blocks. Must be held for the
//!    ENTIRE read-modify-write-publish span of a mutation, not just one
//!    step — the returned [`TableWriteLock`] is an RAII guard (`Drop`
//!    unlocks; the kernel also releases it automatically if the holding
//!    process dies for any reason, verified in task 001's SIGKILL test).
//! 2. [`write_append_segments`] — the NON-PUBLISHING write core: streams
//!    `stream`'s batches into new segment file(s) written DIRECTLY into the
//!    LIVE table directory (never a staging directory) under fresh,
//!    non-colliding segment ids continuing from the existing maximum
//!    (`existing.segments.iter().map(|s| s.id).max().unwrap_or(0) + 1..` —
//!    NOT restarting at 0, which `SegmentWriter::new`'s `next_id: 0` would
//!    silently do if reused as-is). Casts every batch to conform to the
//!    TARGET's already-declared schema AND dictionary encoding (read from
//!    the existing manifest, never rediscovered — task 001's Decision 6);
//!    a real mismatch is a clean, named `QueryError::Type`, never silent
//!    coercion. Returns ONLY the new `Segment` entries — does not read,
//!    build, or publish any manifest. A crash (or a schema-mismatch error
//!    partway through a multi-segment stream) leaves any already-flushed
//!    segment(s) as harmless, manifest-unreferenced orphans, exactly like
//!    an abandoned Create/Overwrite staging directory is already inert
//!    today.
//! 3. [`publish_manifest_update`] — the NON-LOCKING publish core: given a
//!    caller-assembled COMPLETE `Vec<Segment>` (existing segments, possibly
//!    edited, plus any new ones), calls the EXISTING `NativeManifest::
//!    build` UNCHANGED (it already derives `row_count`/`table_stats` fresh
//!    from whatever `Vec<Segment>` it's given — no new merge function
//!    needed) and publishes via `native_manifest::write_manifest_atomic`'s
//!    single-FILE atomic rename (task 001's Decision 4) — never
//!    `publish_table_dir`.
//!
//! [`append_to_native_table`] composes all three into ONE self-publishing
//! entrypoint (lock → read existing manifest → write segments → publish →
//! unlock) and is what [`write_batches_with_options`]`(..., NativeWriteMode
//! ::Append, ...)` and `ExecutionContext::insert_into_native_table` both
//! call. A zero-row source (an empty stream, or a stream of only empty
//! batches) is a legitimate NO-OP — `write_append_segments` returns
//! `Ok(vec![])`, and `append_to_native_table` skips the publish step
//! entirely (no version bump, no manifest write) and reports the table's
//! unchanged current state. This deliberately differs from Create/
//! Overwrite's `write_staged`, which REFUSES a zero-row source — an INSERT
//! that happens to match no source rows is not an error the way creating
//! an empty table from nothing is.
//!
//! Task 004 (UPDATE)'s own composition, per task 001's Decision 2: it must
//! NOT call `append_to_native_table` and a future DELETE's own
//! self-publishing entrypoint sequentially (two independent publishes
//! leave a real half-done window). Instead it should call
//! [`write_append_segments`] directly (for the recomputed rows) and DELETE
//! task 003's own non-publishing identification building block, fold BOTH
//! results into one `Vec<Segment>`, and call [`publish_manifest_update`]
//! exactly ONCE — all under a SINGLE [`lock_table_for_write`] guard held
//! for the whole sequence.

use crate::error::{QueryError, Result};
use crate::physical::operators::TableProvider;
use crate::physical::RecordBatchStream;
use crate::storage::ipc_cache;
use crate::storage::native_manifest::{self, NativeManifest, Segment};
use arrow::array::DictionaryArray;
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// How a write relates to whatever already exists at the destination
/// directory. Phase 1 (native-tables-foundation) shipped `Create`/
/// `Overwrite` only, full-table-replace: "a load always produces one
/// complete new snapshot; no partial append/update in this epic" — task
/// 003's own Description. `Append` (native-tables-mutation epic, task 002)
/// adds the first INCREMENTAL write mode: it does NOT go through this
/// enum's other two variants' staging-directory + whole-directory-rename
/// flow at all (see [`write_batches_with_options`]'s doc and
/// [`append_to_native_table`] for why that flow is unsafe to reuse for an
/// incremental change — it would delete every pre-existing segment file
/// not part of a fresh staging set).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeWriteMode {
    /// Fail if a native table already exists at the destination.
    Create,
    /// Replace the destination wholesale: bumps `snapshot.version`,
    /// preserves the existing `table_id` if the destination is already a
    /// native table. Refuses (rather than silently deleting) a destination
    /// that exists, is non-empty, and is NOT already a native table — see
    /// the module-level safety note on [`write_batches_with_options`].
    Overwrite,
    /// Add new rows to an EXISTING native table (task 002,
    /// native-tables-mutation epic): writes new segment(s) directly into
    /// the live table directory (fresh, non-colliding segment ids
    /// continuing from the existing maximum) and publishes ONE new
    /// manifest via a single-FILE atomic rename
    /// (`native_manifest::write_manifest_atomic`), never touching or
    /// removing any pre-existing segment file. Requires the destination to
    /// ALREADY be a native table (`native_manifest::read_manifest` is a
    /// clean `Err` otherwise — unlike `Create`/`Overwrite`, `Append` never
    /// creates a table from nothing). See [`append_to_native_table`] for
    /// the full mechanism and its lower-level, non-publishing building
    /// blocks ([`write_append_segments`], [`publish_manifest_update`]).
    Append,
}

impl std::str::FromStr for NativeWriteMode {
    type Err = QueryError;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "create" => Ok(Self::Create),
            "overwrite" => Ok(Self::Overwrite),
            "append" => Ok(Self::Append),
            other => Err(QueryError::NotImplemented(format!(
                "unknown native table write mode `{other}` (expected create, overwrite, or append)"
            ))),
        }
    }
}

/// Tunables for [`write_batches_with_options`] and the source-specific
/// `write_from_*_with_options` wrappers. The plain (non-`_with_options`)
/// entrypoints use [`NativeWriteOptions::default`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeWriteOptions {
    /// Flush a segment once buffered rows reach this count (a segment may
    /// end up somewhat larger than this if a single incoming batch crosses
    /// the threshold — batches are never split to hit an exact size).
    pub target_rows_per_segment: usize,
    /// A `Utf8` column whose first-segment dictionary has more than this
    /// many distinct values is written plain instead. Mirrors
    /// `ipc_cache.rs::build_sidecar`'s own wide-dictionary demotion
    /// threshold (4096).
    pub dict_max_cardinality: usize,
    /// Within one segment's Arrow IPC file, batches larger than this are
    /// re-sliced into `slice_rows`-sized zero-copy views before writing —
    /// mirrors `ipc_cache.rs::build_sidecar`'s own chunking (reused
    /// directly via `ipc_cache::reslice_large`, not reimplemented).
    pub slice_rows: usize,
}

impl Default for NativeWriteOptions {
    fn default() -> Self {
        Self {
            target_rows_per_segment: 1_000_000,
            dict_max_cardinality: 4096,
            slice_rows: 65_536,
        }
    }
}

/// What a write produced. For `Create`/`Overwrite` this describes the
/// WHOLE table (every row/segment now present, since both modes fully
/// replace it). `NativeWriteMode::Append` reuses this same struct when
/// reached via [`write_batches`]/[`write_batches_with_options`] (for CLI
/// parity across all three modes), with the SAME "whole table" meaning —
/// `rows`/`segments` are the table's TOTALS after the append, not just the
/// delta this call added. Callers that need the delta (e.g.
/// `ExecutionContext::insert_into_native_table`) should call
/// [`append_to_native_table`] directly instead, which returns
/// [`NativeAppendResult`] (both the delta AND the totals).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeWriteResult {
    /// Stable table identity (UUID v4 on first create, preserved across an
    /// `Overwrite`/`Append`).
    pub table_id: String,
    /// The snapshot version this write committed (1 for a fresh table).
    pub version: u64,
    /// Rows written (== `snapshot.row_count`).
    pub rows: u64,
    /// Segments written.
    pub segments: usize,
}

/// What [`append_to_native_table`] produced — the richer, delta-aware
/// sibling of [`NativeWriteResult`] (see that struct's doc for why
/// `Append` needs its own shape rather than overloading "written" to mean
/// two different things across modes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeAppendResult {
    /// Stable table identity (UUID v4) — unchanged by an Append.
    pub table_id: String,
    /// The snapshot version this append committed. Equal to the PRE-append
    /// version when the source produced zero rows (a no-op, not an error —
    /// see the module doc's "Append" section).
    pub version: u64,
    /// Rows ADDED by this operation (0 for an empty source).
    pub rows_appended: u64,
    /// Segments ADDED by this operation (0 for an empty source).
    pub segments_appended: usize,
    /// The table's TOTAL row count after this append (== the new
    /// manifest's `snapshot.row_count`; unchanged from before if this was
    /// a no-op).
    pub total_rows: u64,
    /// The table's TOTAL segment count after this append.
    pub total_segments: usize,
}

fn dictionary_data_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
}

// ============================================================================
// The core streaming primitive
// ============================================================================

/// Consume `stream` batch-by-batch and write it as a new native table at
/// `out_dir`: segments (Arrow IPC, dictionary-coerced for low-cardinality
/// `Utf8` columns) + `_manifest.json`, per task 002's format. Per-segment
/// statistics are computed as data streams through — never a second pass
/// over written data. Uses [`NativeWriteOptions::default`]; see
/// [`write_batches_with_options`] for tunables and the full safety
/// contract.
///
/// This is THE entrypoint a future `CREATE TABLE ... AS SELECT` calls: hand
/// it a physical plan's stream and schema directly (merge multiple
/// partitions with `futures::stream::select_all` first if there is more
/// than one) — see the module doc for why this must never go through
/// `ExecutionContext::sql()`.
pub async fn write_batches(
    stream: RecordBatchStream,
    schema: SchemaRef,
    out_dir: impl AsRef<Path>,
    mode: NativeWriteMode,
) -> Result<NativeWriteResult> {
    write_batches_with_options(stream, schema, out_dir, mode, NativeWriteOptions::default()).await
}

/// [`write_batches`] with explicit [`NativeWriteOptions`].
///
/// # Safety contract for `mode`
///
/// * `Create`: refuses if `out_dir` already exists (any file or directory),
///   matching `lance_write::write_reader`'s own `Create` check.
/// * `Overwrite`: if `out_dir` exists, is non-empty, and is NOT already a
///   native table directory (`native_manifest::is_native_table_dir`), this
///   REFUSES rather than deleting it. `native_manifest::publish_table_dir`
///   (task 002, not modified here) unconditionally `remove_dir_all`s its
///   target before renaming into place — appropriate once a directory is
///   confirmed to already BE a native table (a full-table replace is
///   supposed to remove the old segments), but not a safe default for "the
///   caller pointed `--out` at the wrong directory". An empty existing
///   directory is adopted silently (the natural "fresh destination"
///   case — same spirit as `Create`, since there is nothing to lose).
/// * `Append`: delegates ENTIRELY to [`append_to_native_table`] — a
///   completely different mechanism (see the module doc's "Append"
///   section), not the staging-directory flow below. `out_dir` MUST
///   already be a native table (a clean `Err` otherwise, never "create
///   it"). `schema` is IGNORED for this mode: the target schema always
///   comes from the EXISTING manifest (task 001's Decision 6 — inherit,
///   never rediscover), never from this parameter, which exists only to
///   define a FRESH table's schema for `Create`/`Overwrite`. Returns
///   [`NativeWriteResult`] with `rows`/`segments` as the table's TOTALS
///   after the append (see that struct's doc); call
///   [`append_to_native_table`] directly for delta-aware
///   [`NativeAppendResult`].
///
/// # On error
///
/// For `Create`/`Overwrite`: the destination directory (`out_dir`) is left
/// completely untouched — a staging directory is written to first and only
/// atomically published (`native_manifest::publish_table_dir`) after
/// everything else succeeds. The staging directory itself is best-effort
/// cleaned up on any error path (not load-bearing for correctness — a
/// leftover `.<pid>.building` staging dir next to `out_dir` is inert, never
/// read by anything). For `Append`: see [`append_to_native_table`]'s own
/// error-path documentation (new segment files may already be written as
/// harmless orphans; the existing manifest is always left intact).
pub async fn write_batches_with_options(
    stream: RecordBatchStream,
    schema: SchemaRef,
    out_dir: impl AsRef<Path>,
    mode: NativeWriteMode,
    options: NativeWriteOptions,
) -> Result<NativeWriteResult> {
    let final_dir = out_dir.as_ref().to_path_buf();

    if mode == NativeWriteMode::Append {
        let _ = &schema; // ignored for Append -- see this function's own doc.
        let result = append_to_native_table(stream, &final_dir, options).await?;
        return Ok(NativeWriteResult {
            table_id: result.table_id,
            version: result.version,
            rows: result.total_rows,
            segments: result.total_segments,
        });
    }

    if mode == NativeWriteMode::Create && final_dir.exists() {
        return Err(QueryError::Storage(format!(
            "native table already exists at {} (use --mode overwrite)",
            final_dir.display()
        )));
    }
    if mode == NativeWriteMode::Overwrite
        && final_dir.exists()
        && !native_manifest::is_native_table_dir(&final_dir)
    {
        let is_empty = std::fs::read_dir(&final_dir)
            .map(|mut it| it.next().is_none())
            .unwrap_or(false);
        if !is_empty {
            return Err(QueryError::Storage(format!(
                "{} exists and is not a native table; refusing to overwrite non-native data \
                 (remove it manually first if this is intentional)",
                final_dir.display()
            )));
        }
    }

    let staging_dir = native_manifest::staging_dir_for(&final_dir);
    let _ = std::fs::remove_dir_all(&staging_dir);
    std::fs::create_dir_all(&staging_dir)?;

    match write_staged(stream, schema, &staging_dir, &final_dir, options).await {
        Ok(result) => Ok(result),
        Err(e) => {
            let _ = std::fs::remove_dir_all(&staging_dir);
            Err(e)
        }
    }
}

/// The happy-path logic, isolated so [`write_batches_with_options`] can
/// clean up the staging directory uniformly on any `?`-propagated error.
async fn write_staged(
    mut stream: RecordBatchStream,
    schema: SchemaRef,
    staging_dir: &Path,
    final_dir: &Path,
    options: NativeWriteOptions,
) -> Result<NativeWriteResult> {
    use futures::TryStreamExt;

    let mut writer = SegmentWriter::new(schema, staging_dir.to_path_buf(), options);
    while let Some(batch) = stream.try_next().await? {
        writer.accept(batch)?;
    }
    let (effective_schema, segments) = writer.finish()?;

    if segments.is_empty() {
        return Err(QueryError::Storage(
            "refusing to write a native table from zero rows: the source produced no rows"
                .to_string(),
        ));
    }

    let table_id = native_manifest::existing_table_id(final_dir)?
        .unwrap_or_else(NativeManifest::generate_table_id);
    let version = native_manifest::next_version(final_dir)?;
    let created_at_ms = now_ms();

    let manifest = NativeManifest::build(
        &effective_schema,
        table_id,
        version,
        segments,
        created_at_ms,
    )?;
    native_manifest::write_manifest(staging_dir, &manifest)?;
    native_manifest::publish_table_dir(staging_dir, final_dir)?;

    Ok(NativeWriteResult {
        table_id: manifest.table_id,
        version: manifest.snapshot.version,
        rows: manifest.snapshot.row_count,
        segments: manifest.segments.len(),
    })
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// ============================================================================
// SegmentWriter: bounded-buffer accumulation, dictionary coercion,
// per-segment statistics, Arrow IPC write.
// ============================================================================

/// Accumulates incoming batches up to `options.target_rows_per_segment`,
/// then flushes them as one segment. Not `pub`: an internal implementation
/// detail of the streaming write, not part of this module's API surface.
struct SegmentWriter {
    /// The schema the caller declared for the stream. Used ONLY as a
    /// lightweight sanity check on incoming batches (column count) — the
    /// actual on-disk schema (including dictionary coercion) is derived
    /// from the first segment's real data in `flush`, not from this.
    declared_schema: SchemaRef,
    staging_dir: PathBuf,
    options: NativeWriteOptions,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    next_id: u32,
    segments: Vec<Segment>,
    /// Set on the FIRST flush, from that flush's own concatenated batch —
    /// see the module doc's "Dictionary encoding: decided once, applied
    /// uniformly" section.
    base_schema: Option<SchemaRef>,
    /// Parallel to `base_schema.fields()`: true where that column is
    /// written dictionary-encoded.
    dict_decision: Option<Vec<bool>>,
}

impl SegmentWriter {
    fn new(declared_schema: SchemaRef, staging_dir: PathBuf, options: NativeWriteOptions) -> Self {
        Self {
            declared_schema,
            staging_dir,
            options,
            pending: Vec::new(),
            pending_rows: 0,
            next_id: 0,
            segments: Vec::new(),
            base_schema: None,
            dict_decision: None,
        }
    }

    fn accept(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        if batch.num_columns() != self.declared_schema.fields().len() {
            return Err(QueryError::Storage(format!(
                "native table write: incoming batch has {} column(s) but the declared schema \
                 has {}",
                batch.num_columns(),
                self.declared_schema.fields().len()
            )));
        }
        self.pending_rows += batch.num_rows();
        self.pending.push(batch);
        if self.pending_rows >= self.options.target_rows_per_segment {
            self.flush()?;
        }
        Ok(())
    }

    /// Flush whatever is currently buffered as one segment. A no-op if
    /// nothing is buffered (safe to call unconditionally from `finish`).
    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        // Mirrors ipc_cache.rs::build_sidecar's own concat call exactly
        // (`concat_batches(&batches[0].schema(), batches.iter())`): use the
        // batches' OWN schema, not any externally declared one, so this
        // never trips on a metadata-only mismatch (e.g. nullability) that
        // wouldn't actually block the concat.
        let batch_schema = self.pending[0].schema();
        let concatenated = arrow::compute::concat_batches(&batch_schema, self.pending.iter())?;
        self.pending.clear();
        self.pending_rows = 0;

        if self.dict_decision.is_none() {
            let dict_ty = dictionary_data_type();
            let mut decision = vec![false; concatenated.num_columns()];
            for (idx, field) in concatenated.schema().fields().iter().enumerate() {
                // Only plain Utf8 is a candidate — mirrors ipc_cache.rs's
                // own candidate detection (`Binary`/`LargeUtf8` excluded).
                if field.data_type() != &DataType::Utf8 {
                    continue;
                }
                if let Ok(dict_arr) = arrow::compute::cast(concatenated.column(idx), &dict_ty) {
                    if let Some(d) = dict_arr
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int32Type>>()
                    {
                        if d.values().len() <= self.options.dict_max_cardinality {
                            decision[idx] = true;
                        }
                    }
                }
            }
            self.base_schema = Some(concatenated.schema());
            self.dict_decision = Some(decision);
        }
        let decision = self
            .dict_decision
            .as_ref()
            .expect("dict_decision set immediately above");

        let dict_ty = dictionary_data_type();
        let mut columns = concatenated.columns().to_vec();
        let mut fields: Vec<Field> = concatenated
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        for (idx, &want_dict) in decision.iter().enumerate() {
            if want_dict {
                let cast_col = arrow::compute::cast(&columns[idx], &dict_ty)?;
                fields[idx] = Field::new(
                    fields[idx].name(),
                    cast_col.data_type().clone(),
                    fields[idx].is_nullable(),
                );
                columns[idx] = cast_col;
            }
        }
        let coerced_schema: SchemaRef = Arc::new(Schema::new(fields));
        let final_batch = RecordBatch::try_new(coerced_schema.clone(), columns)?;

        // Statistics computed from the batch already in memory, right
        // before writing it — never a second pass over the written file.
        let row_count = final_batch.num_rows() as u64;
        let column_stats = native_manifest::compute_batch_stats(&final_batch);

        let id = self.next_id;
        self.next_id += 1;
        let path = native_manifest::segment_full_path(&self.staging_dir, id);
        // Reuses ipc_cache.rs's own proven chunking helper (already `pub`,
        // not reimplemented) so a large segment reads back exactly like a
        // parquet-backed sidecar's row group does.
        let slices = ipc_cache::reslice_large(
            vec![final_batch],
            self.options.slice_rows,
            self.options.slice_rows,
        );
        write_ipc_file(&path, &coerced_schema, &slices)?;
        let byte_size = std::fs::metadata(&path)?.len();

        self.segments.push(Segment {
            id,
            path: Segment::expected_file_name(id),
            row_count,
            byte_size,
            column_stats,
        });
        Ok(())
    }

    /// Flush any remaining buffered rows and return the table's effective
    /// (post dictionary-coercion) schema plus every segment written. An
    /// empty `segments` means the stream produced zero rows across every
    /// batch — the caller ([`write_staged`]) turns that into a clear
    /// refusal rather than publishing an empty table.
    fn finish(mut self) -> Result<(SchemaRef, Vec<Segment>)> {
        if self.pending_rows > 0 {
            self.flush()?;
        }
        let Some(base_schema) = self.base_schema else {
            return Ok((self.declared_schema.clone(), Vec::new()));
        };
        let decision = self
            .dict_decision
            .expect("dict_decision is always set alongside base_schema in flush()");
        let dict_ty = dictionary_data_type();
        let fields: Vec<Field> = base_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, f)| {
                if decision[i] {
                    Field::new(f.name(), dict_ty.clone(), f.is_nullable())
                } else {
                    f.as_ref().clone()
                }
            })
            .collect();
        Ok((Arc::new(Schema::new(fields)), self.segments))
    }
}

fn write_ipc_file(path: &Path, schema: &SchemaRef, batches: &[RecordBatch]) -> Result<()> {
    let file = std::fs::File::create(path)?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new(file, schema)?;
    for b in batches {
        writer.write(b)?;
    }
    writer.finish()?;
    Ok(())
}

// ============================================================================
// Single-writer lock (task 001's Decision 5) — a sibling advisory lock file
// per table directory, held for the WHOLE span of an Append (or any future
// DELETE/UPDATE) mutation, from before the existing manifest is read
// through the final publish. `std::fs::File::{try_lock, unlock}` are
// STABLE std methods (since Rust 1.89.0; this repo pins 1.93.0) wrapping
// `flock(2)` on Unix — confirmed, with a live cross-process SIGKILL test,
// to release automatically the instant a holding process dies for ANY
// reason (task 001's Outcome, Decision 5). Zero new Cargo dependency.
// ============================================================================

/// The sibling lock-file path for `final_dir` — mirrors
/// `native_manifest::staging_dir_for`'s own sibling-path convention.
/// Computed once per TABLE (not per attempt/pid): the whole point is one
/// stable identity every writer contends on, not a fresh one each time.
pub fn lock_path_for(final_dir: &Path) -> PathBuf {
    let mut name = final_dir.file_name().unwrap_or_default().to_os_string();
    name.push(".lock");
    final_dir.with_file_name(name)
}

/// RAII guard for a native table's single-writer advisory lock. Holds an
/// exclusive `std::fs::File::try_lock()` for as long as this guard lives;
/// `Drop` calls `unlock()` so an early `?`-propagated error or a panic
/// still releases it deterministically. The kernel is the backstop for a
/// hard crash (SIGKILL): it releases a `flock` the instant the holding
/// process dies, for any reason, with zero manual cleanup — verified live
/// in task 001's design spike.
#[derive(Debug)]
pub struct TableWriteLock {
    file: std::fs::File,
}

impl Drop for TableWriteLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

/// Acquire the single-writer advisory lock for the native table directory
/// `final_dir` (its sibling lock file, per [`lock_path_for`] — created if
/// absent). Non-blocking: returns a `QueryError::Storage` naming
/// `final_dir` IMMEDIATELY if another writer already holds it
/// (`TryLockError::WouldBlock`), never blocks waiting.
///
/// Callers performing a multi-step read-modify-write-publish mutation
/// (Append here; a future DELETE/UPDATE) must acquire this ONCE, BEFORE
/// reading the existing manifest, and hold the returned guard for the
/// mutation's ENTIRE span through the final atomic publish — this is what
/// prevents a LOST UPDATE between two concurrent writers each computing
/// their own "next" manifest version from the same starting point (a risk
/// each individual atomic rename, on its own, cannot close — see
/// `native_manifest::write_manifest_atomic`'s own doc). Readers never call
/// this (writer-vs-writer only).
pub fn lock_table_for_write(final_dir: &Path) -> Result<TableWriteLock> {
    let lock_path = lock_path_for(final_dir);
    if let Some(parent) = lock_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&lock_path)?;
    file.try_lock().map_err(|e| {
        QueryError::Storage(format!(
            "another writer already holds the lock on native table {} ({}): {e}",
            final_dir.display(),
            lock_path.display()
        ))
    })?;
    Ok(TableWriteLock { file })
}

// ============================================================================
// Append: non-publishing write core (task 002's reusable building block)
// ============================================================================

/// Write `stream`'s batches as new segment(s) DIRECTLY into the LIVE
/// native table directory `table_dir`, conforming to (and validating
/// against) `target`'s ALREADY-DECLARED schema and dictionary encoding —
/// never rediscovering either (task 001's Decision 6). Segment ids
/// continue from `target.segments`'s existing maximum, never restarting
/// at 0. Returns ONLY the newly written segments' `Segment` entries — does
/// NOT read, construct, or publish any manifest; the caller (this
/// module's [`append_to_native_table`], or a future task 003/004 DELETE/
/// UPDATE composing its own atomic publish) is responsible for folding
/// these into a full `Vec<Segment>` and calling
/// [`publish_manifest_update`].
///
/// # Schema conformance
///
/// Every incoming batch is checked, BEFORE it is written, against
/// `target.arrow_schema()` BY POSITION — never by the source's own field
/// names (the same wildcard-qualification trap CTAS already guards
/// against: `SELECT * FROM src` produces table-qualified field names like
/// `"src.col"`, and is even more direct to avoid here since the target
/// schema already exists). For each column position: an EXACT data-type
/// match is used as-is; a target column declared `Dictionary(Int32,
/// Utf8)` whose incoming column is plain `Utf8` (the target's own decoded
/// value type) is cast to match (the ONE sanctioned coercion — the
/// dictionary decision is inherited, never re-derived from the new
/// data's cardinality); anything else is a clean, named `QueryError::Type`
/// naming the column and both types, checked before any write — never
/// silent coercion, never a generic Arrow error. Callers should
/// pre-normalize any INCIDENTALLY dictionary-encoded source columns (e.g.
/// from small-build join gathers or an IPC-sidecar-cached scan) to plain
/// arrays first — mirrors `create_table_as_select`'s own
/// `decode_dictionary_batch` step — since "source already
/// dictionary-encoded but target is plain" is not one of the two accepted
/// shapes above.
///
/// A schema mismatch found on a batch after the first still leaves any
/// EARLIER batches' segment file(s) written on disk — harmless,
/// manifest-unreferenced orphans, identical in kind to a crash between
/// this function and the eventual publish (see the module doc's "Append"
/// section).
///
/// A zero-row source (an empty stream, or a stream of only empty batches)
/// returns `Ok(vec![])` — never an error (unlike `write_batches`'s
/// Create/Overwrite path, which refuses zero rows — see the module doc
/// for why the two modes differ here).
pub async fn write_append_segments(
    mut stream: RecordBatchStream,
    target: &NativeManifest,
    table_dir: &Path,
    options: NativeWriteOptions,
) -> Result<Vec<Segment>> {
    use futures::TryStreamExt;

    let target_schema = target.arrow_schema();
    let next_id = target
        .segments
        .iter()
        .map(|s| s.id)
        .max()
        .map(|m| m + 1)
        .unwrap_or(0);
    let mut writer =
        AppendSegmentWriter::new(target_schema, table_dir.to_path_buf(), next_id, options);
    while let Some(batch) = stream.try_next().await? {
        writer.accept(batch)?;
    }
    writer.finish()
}

/// Accumulates incoming (already schema-conformed) batches up to
/// `options.target_rows_per_segment`, then flushes them as one segment
/// written directly into the LIVE table directory — the `Append` analogue
/// of [`SegmentWriter`], with two deliberate differences: the target
/// schema/dictionary decision is FIXED (from the caller, never derived
/// from the data) and segment ids start from a caller-supplied `next_id`
/// rather than always 0. Not `pub`: an internal implementation detail,
/// like `SegmentWriter`.
struct AppendSegmentWriter {
    target_schema: SchemaRef,
    table_dir: PathBuf,
    options: NativeWriteOptions,
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    next_id: u32,
    segments: Vec<Segment>,
}

impl AppendSegmentWriter {
    fn new(
        target_schema: SchemaRef,
        table_dir: PathBuf,
        next_id: u32,
        options: NativeWriteOptions,
    ) -> Self {
        Self {
            target_schema,
            table_dir,
            options,
            pending: Vec::new(),
            pending_rows: 0,
            next_id,
            segments: Vec::new(),
        }
    }

    fn accept(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let conformed = cast_batch_to_target(&batch, &self.target_schema)?;
        self.pending_rows += conformed.num_rows();
        self.pending.push(conformed);
        if self.pending_rows >= self.options.target_rows_per_segment {
            self.flush()?;
        }
        Ok(())
    }

    /// Flush whatever is currently buffered as one segment. A no-op if
    /// nothing is buffered (safe to call unconditionally from `finish`).
    fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        // Every pending batch was already cast to `target_schema` exactly
        // in `accept`, so concatenation against that schema can never trip
        // on a mismatch here.
        let concatenated =
            arrow::compute::concat_batches(&self.target_schema, self.pending.iter())?;
        self.pending.clear();
        self.pending_rows = 0;

        let row_count = concatenated.num_rows() as u64;
        let column_stats = native_manifest::compute_batch_stats(&concatenated);

        let id = self.next_id;
        self.next_id += 1;
        let path = native_manifest::segment_full_path(&self.table_dir, id);
        let slices = ipc_cache::reslice_large(
            vec![concatenated],
            self.options.slice_rows,
            self.options.slice_rows,
        );
        write_ipc_file(&path, &self.target_schema, &slices)?;
        let byte_size = std::fs::metadata(&path)?.len();

        self.segments.push(Segment {
            id,
            path: Segment::expected_file_name(id),
            row_count,
            byte_size,
            column_stats,
        });
        Ok(())
    }

    /// Flush any remaining buffered rows and return every segment written.
    fn finish(mut self) -> Result<Vec<Segment>> {
        if self.pending_rows > 0 {
            self.flush()?;
        }
        Ok(self.segments)
    }
}

/// Validate `batch`'s columns against `target_schema` BY POSITION and
/// return a new batch conforming to it exactly (same schema object, cast
/// columns where needed) — see [`write_append_segments`]'s doc for the
/// exact rules. Never called with an empty batch (guarded by the one
/// caller, `AppendSegmentWriter::accept`).
fn cast_batch_to_target(batch: &RecordBatch, target_schema: &SchemaRef) -> Result<RecordBatch> {
    if batch.num_columns() != target_schema.fields().len() {
        return Err(QueryError::Type(format!(
            "INSERT/Append: source produced {} column(s) but the target table has {}",
            batch.num_columns(),
            target_schema.fields().len()
        )));
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (i, target_field) in target_schema.fields().iter().enumerate() {
        let src_col = batch.column(i);
        let src_type = src_col.data_type();
        let target_type = target_field.data_type();
        if src_type == target_type {
            columns.push(src_col.clone());
            continue;
        }
        // The one sanctioned coercion: the target column is already
        // dictionary-encoded (a decision made when the table's FIRST
        // segment was ever written, task 001's Decision 6) and the
        // source produced that dictionary's own plain value type — apply
        // the table's existing encoding, never re-derive it.
        if let DataType::Dictionary(_, value_ty) = target_type {
            if src_type == value_ty.as_ref() {
                let cast_col = arrow::compute::cast(src_col, target_type).map_err(|e| {
                    QueryError::Type(format!(
                        "INSERT/Append: column `{}` (position {i}): failed to apply the \
                         target table's existing dictionary encoding: {e}",
                        target_field.name()
                    ))
                })?;
                columns.push(cast_col);
                continue;
            }
        }
        return Err(QueryError::Type(format!(
            "INSERT/Append: column `{}` (position {i}): source produced {src_type:?} but the \
             target table declares {target_type:?} -- schema mismatch is not supported (no \
             implicit coercion; cast explicitly in the SELECT if this is intentional)",
            target_field.name()
        )));
    }
    Ok(RecordBatch::try_new(target_schema.clone(), columns)?)
}

// ============================================================================
// Append: non-locking publish core (task 002's reusable building block)
// ============================================================================

/// Publish an updated segment list for an EXISTING native table: build a
/// fresh manifest via the EXISTING `NativeManifest::build` (re-derives
/// `row_count`/`table_stats` from `segments` — no separate merge function
/// needed) and publish it via `native_manifest::write_manifest_atomic`'s
/// single-FILE atomic rename (task 001's Decision 4) — NEVER
/// `native_manifest::publish_table_dir`, which would `remove_dir_all` the
/// whole directory and delete every segment file not part of a fresh
/// staging set.
///
/// `table_id` is carried through unchanged (identity survives every
/// mutation). `version` and `segments` are the CALLER's responsibility to
/// compute correctly (e.g. `existing.snapshot.version + 1`,
/// `existing.segments` extended with new/tombstoned entries) — this
/// function performs no merge logic of its own beyond what
/// `NativeManifest::build` already does.
///
/// Does NOT acquire the single-writer lock — the caller must already hold
/// it (via [`lock_table_for_write`]) for the ENTIRE read-modify-write span
/// this publish concludes, not just for this call. Calling this without
/// holding that lock risks a lost update (see [`lock_table_for_write`]'s
/// own doc) — nothing here enforces that at the type level.
pub fn publish_manifest_update(
    table_dir: &Path,
    schema: &Schema,
    table_id: impl Into<String>,
    version: u64,
    segments: Vec<Segment>,
    created_at_ms: i64,
) -> Result<NativeManifest> {
    let manifest = NativeManifest::build(schema, table_id, version, segments, created_at_ms)?;
    native_manifest::write_manifest_atomic(table_dir, &manifest)?;
    Ok(manifest)
}

// ============================================================================
// Append: the self-publishing entrypoint
// ============================================================================

/// Append `stream`'s batches to an EXISTING native table at `table_dir` —
/// the full self-publishing entrypoint composing all three of this
/// module's Append building blocks in sequence: acquires the
/// single-writer lock ([`lock_table_for_write`], held for this whole
/// call), reads the existing manifest, streams new segment(s) directly
/// into the live directory ([`write_append_segments`]), and publishes ONE
/// atomically-renamed manifest ([`publish_manifest_update`]) — or, if the
/// source produced zero rows, publishes NOTHING and returns the table's
/// unchanged current state (see the module doc's "Append" section).
///
/// This is the entrypoint `ExecutionContext::insert_into_native_table`
/// calls, and the one [`write_batches_with_options`]`(...,
/// NativeWriteMode::Append, ...)` delegates to for CLI/`write-native
/// --mode append` parity with Create/Overwrite.
///
/// `table_dir` MUST already be a native table directory
/// (`native_manifest::is_native_table_dir`) — unlike Create/Overwrite,
/// Append never creates a table from nothing; a missing/non-native
/// destination is a clean `QueryError::Storage` (from
/// `native_manifest::read_manifest`), not silently treated as "create".
///
/// # On error
///
/// A lock-contention failure or a missing/corrupt manifest leaves
/// `table_dir` completely untouched (nothing was written yet). A schema-
/// mismatch error from the source may leave already-flushed segment
/// file(s) as harmless orphans (see [`write_append_segments`]'s doc) —
/// the existing manifest is NEVER modified unless this function reaches
/// and completes its final [`publish_manifest_update`] call.
pub async fn append_to_native_table(
    stream: RecordBatchStream,
    table_dir: impl AsRef<Path>,
    options: NativeWriteOptions,
) -> Result<NativeAppendResult> {
    let table_dir = table_dir.as_ref().to_path_buf();
    let _lock = lock_table_for_write(&table_dir)?;
    let existing = native_manifest::read_manifest(&table_dir)?;

    let new_segments = write_append_segments(stream, &existing, &table_dir, options).await?;
    if new_segments.is_empty() {
        // A legitimate no-op (see the module doc's "Append" section) --
        // never touch the manifest, never bump the version.
        return Ok(NativeAppendResult {
            table_id: existing.table_id,
            version: existing.snapshot.version,
            rows_appended: 0,
            segments_appended: 0,
            total_rows: existing.snapshot.row_count,
            total_segments: existing.segments.len(),
        });
    }

    let rows_appended: u64 = new_segments.iter().map(|s| s.row_count).sum();
    let segments_appended = new_segments.len();

    let mut all_segments = existing.segments.clone();
    all_segments.extend(new_segments);

    let schema = existing.arrow_schema();
    let manifest = publish_manifest_update(
        &table_dir,
        schema.as_ref(),
        existing.table_id.clone(),
        existing.snapshot.version + 1,
        all_segments,
        now_ms(),
    )?;

    Ok(NativeAppendResult {
        table_id: manifest.table_id.clone(),
        version: manifest.snapshot.version,
        rows_appended,
        segments_appended,
        total_rows: manifest.snapshot.row_count,
        total_segments: manifest.segments.len(),
    })
}

// ============================================================================
// Source-specific convenience paths — read the source, write the target,
// batch by batch, matching lance_write.rs's own streaming-without-
// double-materializing pattern.
// ============================================================================

/// Convert a Parquet file or directory to a native table. Streams row group
/// by row group via the existing `StreamingParquetReader` — never
/// materializes the source.
pub async fn write_from_parquet(
    parquet: impl AsRef<Path>,
    out_dir: impl AsRef<Path>,
    mode: NativeWriteMode,
) -> Result<NativeWriteResult> {
    write_from_parquet_with_options(parquet, out_dir, mode, NativeWriteOptions::default()).await
}

/// [`write_from_parquet`] with explicit [`NativeWriteOptions`].
pub async fn write_from_parquet_with_options(
    parquet: impl AsRef<Path>,
    out_dir: impl AsRef<Path>,
    mode: NativeWriteMode,
    options: NativeWriteOptions,
) -> Result<NativeWriteResult> {
    let table = crate::storage::ParquetTable::try_new(parquet)?;
    let schema = table.schema();
    let reader =
        crate::storage::StreamingParquetReader::from_table(&table, None, options.slice_rows);
    let stream: RecordBatchStream = reader.into_stream();
    write_batches_with_options(stream, schema, out_dir, mode, options).await
}

/// Convert an Apache Iceberg table (a specific snapshot, or the current one
/// if `snapshot_id` is `None`) to a native table. An Iceberg table resolves
/// to an ordinary `ParquetTable` over its snapshot's manifest-listed data
/// files (`storage::iceberg::open_table`), so this streams exactly like
/// [`write_from_parquet`] — never materializes the source.
pub async fn write_from_iceberg(
    iceberg_dir: impl AsRef<Path>,
    out_dir: impl AsRef<Path>,
    mode: NativeWriteMode,
    snapshot_id: Option<i64>,
) -> Result<NativeWriteResult> {
    write_from_iceberg_with_options(
        iceberg_dir,
        out_dir,
        mode,
        snapshot_id,
        NativeWriteOptions::default(),
    )
    .await
}

/// [`write_from_iceberg`] with explicit [`NativeWriteOptions`].
pub async fn write_from_iceberg_with_options(
    iceberg_dir: impl AsRef<Path>,
    out_dir: impl AsRef<Path>,
    mode: NativeWriteMode,
    snapshot_id: Option<i64>,
    options: NativeWriteOptions,
) -> Result<NativeWriteResult> {
    let opened = crate::storage::open_iceberg_table(iceberg_dir, snapshot_id)?;
    let schema = opened.table.schema();
    let reader =
        crate::storage::StreamingParquetReader::from_table(&opened.table, None, options.slice_rows);
    let stream: RecordBatchStream = reader.into_stream();
    write_batches_with_options(stream, schema, out_dir, mode, options).await
}

/// Convert a Lance dataset to a native table. Requires the `lance` cargo
/// feature.
///
/// Streams one FRAGMENT at a time (Lance's own unit of parallel decode,
/// same atom `LanceTable::fragment_infos`/`shard_with_fragments` already
/// use for distributed sharding) rather than the whole dataset: peak memory
/// is bounded by one fragment, not the dataset size. This is a genuine
/// improvement over `LanceTable::scan()`'s own contract (which always
/// returns one fully materialized `Vec<RecordBatch>` for the whole table —
/// a pre-existing characteristic of the Lance reader, not something this
/// task changes, since `lance.rs` is not touched here); reusing
/// `shard_with_fragments` + `scan` per fragment gets the streaming property
/// entirely through already-`pub` API, with zero changes to `lance.rs`.
#[cfg(feature = "lance")]
pub async fn write_from_lance(
    lance_path: impl AsRef<Path>,
    out_dir: impl AsRef<Path>,
    mode: NativeWriteMode,
) -> Result<NativeWriteResult> {
    write_from_lance_with_options(lance_path, out_dir, mode, NativeWriteOptions::default()).await
}

/// [`write_from_lance`] with explicit [`NativeWriteOptions`].
#[cfg(feature = "lance")]
pub async fn write_from_lance_with_options(
    lance_path: impl AsRef<Path>,
    out_dir: impl AsRef<Path>,
    mode: NativeWriteMode,
    options: NativeWriteOptions,
) -> Result<NativeWriteResult> {
    let table = Arc::new(crate::storage::LanceTable::try_new(lance_path)?);
    let schema = table.schema();
    let fragment_ids: std::collections::VecDeque<u64> =
        table.fragment_infos()?.into_iter().map(|f| f.id).collect();

    type FragState = (
        Arc<crate::storage::LanceTable>,
        std::collections::VecDeque<u64>,
        std::vec::IntoIter<RecordBatch>,
    );
    let init: FragState = (table, fragment_ids, Vec::new().into_iter());

    let stream: RecordBatchStream = Box::pin(futures::stream::unfold(
        init,
        |(table, mut ids, mut iter)| async move {
            loop {
                if let Some(batch) = iter.next() {
                    return Some((Ok(batch), (table, ids, iter)));
                }
                let id = ids.pop_front()?;
                let shard_table = Arc::clone(&table);
                let read = tokio::task::spawn_blocking(move || -> Result<Vec<RecordBatch>> {
                    let shard = shard_table.shard_with_fragments([id])?;
                    shard.scan(None)
                })
                .await;
                match read {
                    Ok(Ok(batches)) => iter = batches.into_iter(),
                    Ok(Err(e)) => return Some((Err(e), (table, ids, Vec::new().into_iter()))),
                    Err(e) => {
                        return Some((
                            Err(QueryError::Execution(format!(
                                "Lance fragment {id} read task panicked: {e}"
                            ))),
                            (table, ids, Vec::new().into_iter()),
                        ))
                    }
                }
            }
        },
    ));

    write_batches_with_options(stream, schema, out_dir, mode, options).await
}

// ============================================================================
// Minimal round-trip read-back — for this task's own tests and its
// `load-native` CLI command ONLY. See the module doc's "What this module
// does NOT do" section: this is not a `TableProvider`.
// ============================================================================

/// Read every row of a native table back into memory via
/// `ipc_cache::read_row_group` — the same already-`pub` mechanism task
/// 004's `TableProvider` will call, but with no projection/filter/
/// statistics/streaming: every segment is fully read. Fine for this
/// module's own round-trip tests and CLI validation; NOT the production
/// read path.
pub fn read_back(dir: &Path) -> Result<(SchemaRef, Vec<RecordBatch>)> {
    let manifest = native_manifest::read_manifest(dir)?;
    let schema = manifest.arrow_schema();
    let mut batches = Vec::new();
    for seg in &manifest.segments {
        batches.extend(ipc_cache::read_row_group(dir, seg.id as usize, None, None)?);
    }
    Ok((schema, batches))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType as DT, Field as ArrowField};
    use futures::stream;

    fn boxed_stream(batches: Vec<RecordBatch>) -> RecordBatchStream {
        Box::pin(stream::iter(batches.into_iter().map(Ok)))
    }

    fn small_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ArrowField::new("id", DT::Int64, false),
            ArrowField::new("category", DT::Utf8, true),
            ArrowField::new("price", DT::Float64, true),
        ]))
    }

    /// `n` rows, `id` = 0..n, `category` cycling through `categories`,
    /// `price` = id as f64 * 1.5, with every 7th row's price NULL.
    fn small_batch(schema: &SchemaRef, start: i64, n: i64, categories: &[&str]) -> RecordBatch {
        let ids: Vec<i64> = (start..start + n).collect();
        let cats: Vec<Option<String>> = ids
            .iter()
            .map(|i| Some(categories[(*i as usize) % categories.len()].to_string()))
            .collect();
        let prices: Vec<Option<f64>> = ids
            .iter()
            .map(|i| {
                if i % 7 == 0 {
                    None
                } else {
                    Some(*i as f64 * 1.5)
                }
            })
            .collect();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(cats)),
                Arc::new(Float64Array::from(prices)),
            ],
        )
        .unwrap()
    }

    fn sum_i64_col(batches: &[RecordBatch], idx: usize) -> i64 {
        batches
            .iter()
            .map(|b| {
                let a = b.column(idx).as_any().downcast_ref::<Int64Array>().unwrap();
                a.iter().flatten().sum::<i64>()
            })
            .sum()
    }

    fn row_count(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    // ---------- CTAS-shaped: an in-memory RecordBatch stream ----------

    #[tokio::test]
    async fn write_batches_from_an_in_memory_stream_round_trips() {
        let schema = small_schema();
        let b1 = small_batch(&schema, 0, 50, &["a", "b", "c"]);
        let b2 = small_batch(&schema, 50, 50, &["a", "b", "c"]);
        let expected_id_sum = sum_i64_col(&[b1.clone(), b2.clone()], 0);
        let expected_rows = row_count(&[b1.clone(), b2.clone()]);

        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        let result = write_batches(
            boxed_stream(vec![b1, b2]),
            schema.clone(),
            &out,
            NativeWriteMode::Create,
        )
        .await
        .unwrap();

        assert_eq!(result.rows, expected_rows as u64);
        assert_eq!(result.version, 1);
        assert_eq!(result.segments, 1);
        assert!(!result.table_id.is_empty());

        let (read_schema, batches) = read_back(&out).unwrap();
        assert_eq!(row_count(&batches), expected_rows);
        assert_eq!(sum_i64_col(&batches, 0), expected_id_sum);
        assert_eq!(read_schema.field(0).name(), "id");
    }

    // ---------- dictionary encoding ----------

    #[tokio::test]
    async fn low_cardinality_utf8_column_is_dictionary_encoded() {
        let schema = small_schema();
        let batch = small_batch(&schema, 0, 1000, &["red", "green", "blue"]);
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        write_batches(
            boxed_stream(vec![batch]),
            schema,
            &out,
            NativeWriteMode::Create,
        )
        .await
        .unwrap();

        let manifest = native_manifest::read_manifest(&out).unwrap();
        let category_field = manifest
            .schema
            .iter()
            .find(|f| f.name == "category")
            .unwrap();
        assert!(
            matches!(
                category_field.data_type,
                native_manifest::ManifestDataType::Dictionary { .. }
            ),
            "low-cardinality `category` must be dictionary-encoded: {:?}",
            category_field.data_type
        );

        // The actual segment file must physically match — not just the
        // manifest's declared schema.
        let (schema, batches) = read_back(&out).unwrap();
        assert_eq!(
            schema.field(1).data_type(),
            &dictionary_data_type(),
            "declared schema must be dictionary-typed"
        );
        assert!(matches!(
            batches[0].column(1).data_type(),
            DT::Dictionary(_, _)
        ));
    }

    #[tokio::test]
    async fn high_cardinality_utf8_column_stays_plain() {
        let schema = small_schema();
        // 2000 distinct category values -- above the default 4096
        // threshold is what we want to test cheaply, so lower the
        // threshold instead of generating 4097 distinct strings.
        let categories: Vec<String> = (0..2000).map(|i| format!("cat-{i}")).collect();
        let cat_refs: Vec<&str> = categories.iter().map(|s| s.as_str()).collect();
        let batch = small_batch(&schema, 0, 2000, &cat_refs);

        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        let options = NativeWriteOptions {
            dict_max_cardinality: 100,
            ..NativeWriteOptions::default()
        };
        write_batches_with_options(
            boxed_stream(vec![batch]),
            schema,
            &out,
            NativeWriteMode::Create,
            options,
        )
        .await
        .unwrap();

        let manifest = native_manifest::read_manifest(&out).unwrap();
        let category_field = manifest
            .schema
            .iter()
            .find(|f| f.name == "category")
            .unwrap();
        assert_eq!(
            category_field.data_type,
            native_manifest::ManifestDataType::Utf8,
            "a >100-distinct-value column with dict_max_cardinality=100 must stay plain Utf8"
        );
    }

    #[tokio::test]
    async fn dictionary_decision_is_locked_after_the_first_segment() {
        // Segment 0: low cardinality (3 values) -> dictionary-encoded.
        // Segment 1: high cardinality (600 distinct) -- ipc_cache.rs would
        // demote THIS row group back to plain; this writer instead keeps
        // the segment-0 decision (see the module doc). Prove that both
        // segments come back as Dictionary-typed, not a schema mismatch.
        let schema = small_schema();
        let seg0 = small_batch(&schema, 0, 50, &["a", "b", "c"]);
        let wide_categories: Vec<String> = (0..600).map(|i| format!("wide-{i}")).collect();
        let wide_refs: Vec<&str> = wide_categories.iter().map(|s| s.as_str()).collect();
        let seg1 = small_batch(&schema, 50, 600, &wide_refs);

        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        let options = NativeWriteOptions {
            target_rows_per_segment: 50,
            dict_max_cardinality: 100,
            ..NativeWriteOptions::default()
        };
        let result = write_batches_with_options(
            boxed_stream(vec![seg0, seg1]),
            schema,
            &out,
            NativeWriteMode::Create,
            options,
        )
        .await
        .unwrap();
        assert_eq!(result.segments, 2);

        let (schema, batches) = read_back(&out).unwrap();
        assert_eq!(batches.len(), 2, "one batch per segment");
        for b in &batches {
            assert!(
                matches!(b.column(1).data_type(), DT::Dictionary(_, _)),
                "every segment must share the table-wide dictionary decision"
            );
        }
        assert!(matches!(schema.field(1).data_type(), DT::Dictionary(_, _)));
    }

    // ---------- segment splitting + statistics ----------

    #[tokio::test]
    async fn segments_split_at_target_row_count_with_correct_stats() {
        let schema = small_schema();
        // 25 rows total, target 10 per segment -> segments of 10, 10, 5.
        let batches: Vec<RecordBatch> = (0..25)
            .map(|i| small_batch(&schema, i, 1, &["x", "y"]))
            .collect();

        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        let options = NativeWriteOptions {
            target_rows_per_segment: 10,
            ..NativeWriteOptions::default()
        };
        let result = write_batches_with_options(
            boxed_stream(batches),
            schema,
            &out,
            NativeWriteMode::Create,
            options,
        )
        .await
        .unwrap();

        assert_eq!(result.rows, 25);
        assert_eq!(result.segments, 3);

        let manifest = native_manifest::read_manifest(&out).unwrap();
        let mut row_counts: Vec<u64> = manifest.segments.iter().map(|s| s.row_count).collect();
        row_counts.sort_unstable();
        assert_eq!(row_counts, vec![5, 10, 10]);

        let id_rollup = manifest.table_stats.get("id").expect("id stats present");
        assert_eq!(id_rollup.min_i64, Some(0));
        assert_eq!(id_rollup.max_i64, Some(24));
        assert_eq!(id_rollup.null_count, Some(0));

        // price is NULL on every 7th id (0, 7, 14, 21) => 4 nulls of 25.
        let price_rollup = manifest.table_stats.get("price").expect("price stats");
        assert_eq!(price_rollup.null_count, Some(4));

        // Every segment file must be named exactly per
        // Segment::expected_file_name -- NativeManifest::build/validate
        // already enforces this, but confirm the files are actually THERE.
        for seg in &manifest.segments {
            assert!(native_manifest::segment_full_path(&out, seg.id).is_file());
        }
    }

    #[tokio::test]
    async fn many_small_batches_and_one_big_batch_agree_on_stats() {
        let schema = small_schema();
        let big = small_batch(&schema, 0, 300, &["p", "q", "r"]);
        let many: Vec<RecordBatch> = (0..300)
            .map(|i| small_batch(&schema, i, 1, &["p", "q", "r"]))
            .collect();

        // Target larger than the whole dataset so both scenarios produce
        // exactly one segment regardless of incoming batch granularity.
        let options = NativeWriteOptions {
            target_rows_per_segment: 10_000,
            ..NativeWriteOptions::default()
        };

        let dir_a = tempfile::tempdir().unwrap();
        let out_a = dir_a.path().join("t");
        write_batches_with_options(
            boxed_stream(vec![big]),
            schema.clone(),
            &out_a,
            NativeWriteMode::Create,
            options,
        )
        .await
        .unwrap();

        let dir_b = tempfile::tempdir().unwrap();
        let out_b = dir_b.path().join("t");
        write_batches_with_options(
            boxed_stream(many),
            schema,
            &out_b,
            NativeWriteMode::Create,
            options,
        )
        .await
        .unwrap();

        let ma = native_manifest::read_manifest(&out_a).unwrap();
        let mb = native_manifest::read_manifest(&out_b).unwrap();
        assert_eq!(ma.snapshot.row_count, mb.snapshot.row_count);
        assert_eq!(ma.segments.len(), 1);
        assert_eq!(mb.segments.len(), 1);
        assert_eq!(ma.table_stats, mb.table_stats);
        assert_eq!(ma.schema, mb.schema, "same dictionary decision either way");
    }

    // ---------- zero rows ----------

    #[tokio::test]
    async fn zero_rows_is_refused_and_leaves_no_trace() {
        let schema = small_schema();
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");

        // An entirely empty stream.
        let err = write_batches(
            boxed_stream(vec![]),
            schema.clone(),
            &out,
            NativeWriteMode::Create,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("zero rows"), "{err}");
        assert!(!out.exists(), "no directory must be left behind");

        // A stream with a batch that has zero rows (not zero batches).
        let empty_batch = RecordBatch::new_empty(schema.clone());
        let err = write_batches(
            boxed_stream(vec![empty_batch]),
            schema,
            &out,
            NativeWriteMode::Create,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("zero rows"), "{err}");
        assert!(!out.exists());
    }

    // ---------- Create / Overwrite mode safety ----------

    #[tokio::test]
    async fn create_mode_refuses_an_existing_destination() {
        let schema = small_schema();
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        write_batches(
            boxed_stream(vec![small_batch(&schema, 0, 5, &["a"])]),
            schema.clone(),
            &out,
            NativeWriteMode::Create,
        )
        .await
        .unwrap();

        let err = write_batches(
            boxed_stream(vec![small_batch(&schema, 0, 5, &["a"])]),
            schema,
            &out,
            NativeWriteMode::Create,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("already exists"), "{err}");
    }

    #[tokio::test]
    async fn overwrite_bumps_version_preserves_identity_replaces_wholesale() {
        let schema = small_schema();
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");

        let first = write_batches(
            boxed_stream(vec![small_batch(&schema, 0, 5, &["a"])]),
            schema.clone(),
            &out,
            NativeWriteMode::Create,
        )
        .await
        .unwrap();
        assert_eq!(first.version, 1);

        let second = write_batches(
            boxed_stream(vec![small_batch(&schema, 100, 9, &["a", "b"])]),
            schema,
            &out,
            NativeWriteMode::Overwrite,
        )
        .await
        .unwrap();
        assert_eq!(second.version, 2);
        assert_eq!(
            second.table_id, first.table_id,
            "identity survives a replace"
        );
        assert_eq!(second.rows, 9, "old rows must not linger");

        let manifest = native_manifest::read_manifest(&out).unwrap();
        assert_eq!(manifest.snapshot.row_count, 9);
    }

    #[tokio::test]
    async fn overwrite_refuses_a_non_native_non_empty_destination() {
        let schema = small_schema();
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(out.join("not_a_manifest.txt"), b"hello").unwrap();

        let err = write_batches(
            boxed_stream(vec![small_batch(&schema, 0, 5, &["a"])]),
            schema,
            &out,
            NativeWriteMode::Overwrite,
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not a native table"), "{err}");
        assert!(
            out.join("not_a_manifest.txt").exists(),
            "refused overwrite must not touch the existing directory"
        );
    }

    #[tokio::test]
    async fn overwrite_adopts_an_empty_existing_directory() {
        let schema = small_schema();
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        std::fs::create_dir_all(&out).unwrap();

        let result = write_batches(
            boxed_stream(vec![small_batch(&schema, 0, 5, &["a"])]),
            schema,
            &out,
            NativeWriteMode::Overwrite,
        )
        .await
        .unwrap();
        assert_eq!(result.rows, 5);
    }

    // ---------- stream errors ----------

    #[tokio::test]
    async fn a_stream_error_is_propagated_and_publishes_nothing() {
        let schema = small_schema();
        let ok_batch = small_batch(&schema, 0, 5, &["a"]);
        let items: Vec<Result<RecordBatch>> = vec![
            Ok(ok_batch),
            Err(QueryError::Execution("synthetic mid-stream failure".into())),
        ];
        let broken: RecordBatchStream = Box::pin(stream::iter(items));

        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        let err = write_batches(broken, schema, &out, NativeWriteMode::Create)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("synthetic mid-stream failure"),
            "{err}"
        );
        assert!(!out.exists(), "a failed write must not publish anything");
    }

    // ---------- date32 + int32 zone-map stats through the write path ----------

    #[tokio::test]
    async fn date32_and_int32_columns_get_zone_map_stats() {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new("d", DT::Date32, true),
            ArrowField::new("n", DT::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Date32Array::from(vec![Some(100), Some(50), None])),
                Arc::new(Int32Array::from(vec![Some(7), None, Some(-3)])),
            ],
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        write_batches(
            boxed_stream(vec![batch]),
            schema,
            &out,
            NativeWriteMode::Create,
        )
        .await
        .unwrap();

        let manifest = native_manifest::read_manifest(&out).unwrap();
        let d = manifest.table_stats.get("d").unwrap();
        assert_eq!(d.min_i64, Some(50));
        assert_eq!(d.max_i64, Some(100));
        assert_eq!(d.null_count, Some(1));
        let n = manifest.table_stats.get("n").unwrap();
        assert_eq!(n.min_i64, Some(-3));
        assert_eq!(n.max_i64, Some(7));
        assert_eq!(n.null_count, Some(1));
    }

    // ---------- real-fixture checksum validation ----------
    //
    // Mirrors scripts/iceberg_gen.py's own validation discipline: COUNT(*)
    // + SUM() of every numeric column, source vs. written-back. `close_enough`
    // matches its `math.isclose(rel_tol=1e-9, abs_tol=1e-6)`.

    fn checksum(schema: &Schema, batches: &[RecordBatch]) -> (u64, Vec<(String, f64)>) {
        let row_count: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let mut sums = Vec::new();
        for (i, f) in schema.fields().iter().enumerate() {
            let is_numeric = matches!(
                f.data_type(),
                DT::Int8
                    | DT::Int16
                    | DT::Int32
                    | DT::Int64
                    | DT::UInt8
                    | DT::UInt16
                    | DT::UInt32
                    | DT::UInt64
                    | DT::Float32
                    | DT::Float64
            );
            if !is_numeric {
                continue;
            }
            let mut total = 0.0f64;
            for b in batches {
                let col: ArrayRef = b.column(i).clone();
                let as_f64 = arrow::compute::cast(&col, &DT::Float64).expect("numeric cast");
                let arr = as_f64.as_any().downcast_ref::<Float64Array>().unwrap();
                total += arrow::compute::sum(arr).unwrap_or(0.0);
            }
            sums.push((f.name().clone(), total));
        }
        (row_count, sums)
    }

    fn close_enough(a: f64, b: f64) -> bool {
        if a == b {
            return true;
        }
        (a - b).abs() <= 1e-6 + 1e-9 * a.abs().max(b.abs())
    }

    fn assert_checksums_match(
        label: &str,
        (src_count, src_sums): (u64, Vec<(String, f64)>),
        (native_count, native_sums): (u64, Vec<(String, f64)>),
    ) {
        assert_eq!(src_count, native_count, "{label}: row count mismatch");
        assert_eq!(
            src_sums.len(),
            native_sums.len(),
            "{label}: numeric column count mismatch"
        );
        for ((sn, sv), (nn, nv)) in src_sums.iter().zip(native_sums.iter()) {
            assert_eq!(sn, nn, "{label}: column order/name mismatch");
            assert!(
                close_enough(*sv, *nv),
                "{label}: sum({sn}) source={sv} native={nv}"
            );
        }
    }

    fn fixture(rel: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(rel)
    }

    #[tokio::test]
    async fn write_from_parquet_matches_source_checksums() {
        let src = fixture("data/tpch-1mb/orders.parquet");
        if !src.exists() {
            eprintln!("skipping: {} not generated", src.display());
            return;
        }
        let source = crate::storage::ParquetTable::try_new(&src).unwrap();
        let src_schema = source.schema();
        let src_batches = source.scan(None).unwrap();
        let src_checksum = checksum(&src_schema, &src_batches);

        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("orders_native");
        let result = write_from_parquet(&src, &out, NativeWriteMode::Create)
            .await
            .unwrap();
        assert_eq!(result.rows, src_checksum.0);

        let (native_schema, native_batches) = read_back(&out).unwrap();
        let native_checksum = checksum(&native_schema, &native_batches);
        assert_checksums_match("parquet orders", src_checksum, native_checksum);
    }

    #[tokio::test]
    async fn write_from_iceberg_matches_source_checksums() {
        let src = fixture("data/tpch-1mb-iceberg/orders");
        if !src.exists() {
            eprintln!("skipping: {} not generated", src.display());
            return;
        }
        let opened = crate::storage::open_iceberg_table(&src, None).unwrap();
        let src_schema = opened.table.schema();
        let src_batches = opened.table.scan(None).unwrap();
        let src_checksum = checksum(&src_schema, &src_batches);

        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("orders_native");
        let result = write_from_iceberg(&src, &out, NativeWriteMode::Create, None)
            .await
            .unwrap();
        assert_eq!(result.rows, src_checksum.0);

        let (native_schema, native_batches) = read_back(&out).unwrap();
        let native_checksum = checksum(&native_schema, &native_batches);
        assert_checksums_match("iceberg orders", src_checksum, native_checksum);
    }

    #[cfg(feature = "lance")]
    #[tokio::test]
    async fn write_from_lance_matches_source_checksums() {
        let src = fixture("data/tpch-1mb-lance/orders.lance");
        if !src.exists() {
            eprintln!("skipping: {} not generated", src.display());
            return;
        }
        let source = crate::storage::LanceTable::try_new(&src).unwrap();
        let src_schema = source.schema();
        let src_batches = source.scan(None).unwrap();
        let src_checksum = checksum(&src_schema, &src_batches);

        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("orders_native");
        let result = write_from_lance(&src, &out, NativeWriteMode::Create)
            .await
            .unwrap();
        assert_eq!(result.rows, src_checksum.0);

        let (native_schema, native_batches) = read_back(&out).unwrap();
        let native_checksum = checksum(&native_schema, &native_batches);
        assert_checksums_match("lance orders", src_checksum, native_checksum);
    }

    #[test]
    fn native_write_mode_from_str() {
        assert_eq!(
            "create".parse::<NativeWriteMode>().unwrap(),
            NativeWriteMode::Create
        );
        assert_eq!(
            "OVERWRITE".parse::<NativeWriteMode>().unwrap(),
            NativeWriteMode::Overwrite
        );
        assert_eq!(
            "Append".parse::<NativeWriteMode>().unwrap(),
            NativeWriteMode::Append
        );
        assert!("bogus".parse::<NativeWriteMode>().is_err());
    }

    // ========================================================================
    // Append (native-tables-mutation epic, task 002)
    // ========================================================================

    /// Build a real, on-disk native table via `write_batches(..., Create)`
    /// with `n` rows of `small_schema()`-shaped data, sent as `n` SEPARATE
    /// one-row batches so `target_rows_per_segment` actually controls
    /// segmentation (a single incoming batch is never split mid-way — see
    /// `append_continues_segment_ids_from_the_existing_maximum_never_
    /// restarting_at_0`'s own comment).
    async fn create_base_table(
        dir: &Path,
        n: i64,
        categories: &[&str],
        target_rows_per_segment: usize,
    ) -> NativeWriteResult {
        let schema = small_schema();
        let batches: Vec<RecordBatch> = (0..n)
            .map(|i| small_batch(&schema, i, 1, categories))
            .collect();
        let options = NativeWriteOptions {
            target_rows_per_segment,
            ..NativeWriteOptions::default()
        };
        write_batches_with_options(
            boxed_stream(batches),
            schema,
            dir,
            NativeWriteMode::Create,
            options,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn append_adds_rows_to_a_table_created_via_create_mode() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        let first = create_base_table(&out, 50, &["a", "b", "c"], 1_000_000).await;
        assert_eq!(first.version, 1);
        assert_eq!(first.segments, 1);

        let schema = small_schema();
        let more = small_batch(&schema, 50, 30, &["a", "b", "c"]);
        let expected_total_id_sum =
            sum_i64_col(&[small_batch(&schema, 0, 80, &["a", "b", "c"])], 0);

        let result = append_to_native_table(
            boxed_stream(vec![more]),
            &out,
            NativeWriteOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(
            result.table_id, first.table_id,
            "identity survives an Append"
        );
        assert_eq!(result.version, 2, "version bumps by exactly one");
        assert_eq!(result.rows_appended, 30);
        assert_eq!(result.segments_appended, 1);
        assert_eq!(result.total_rows, 80);
        assert_eq!(result.total_segments, 2);

        // Both the OLD (from Create) and NEW (from Append) rows must be
        // present — read back the whole table and check.
        let (_, batches) = read_back(&out).unwrap();
        assert_eq!(row_count(&batches), 80);
        assert_eq!(sum_i64_col(&batches, 0), expected_total_id_sum);

        let manifest = native_manifest::read_manifest(&out).unwrap();
        assert_eq!(manifest.snapshot.version, 2);
        assert_eq!(manifest.snapshot.row_count, 80);
        assert_eq!(manifest.segments.len(), 2);
    }

    #[tokio::test]
    async fn append_continues_segment_ids_from_the_existing_maximum_never_restarting_at_0() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        // 25 rows, 10/segment -> segments 0, 1, 2 (10, 10, 5).
        let first = create_base_table(&out, 25, &["x", "y"], 10).await;
        assert_eq!(first.segments, 3);
        let existing_ids: Vec<u32> = native_manifest::read_manifest(&out)
            .unwrap()
            .segments
            .iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(existing_ids, vec![0, 1, 2]);

        // Append 25 more rows, 10/segment -> three NEW segments (10, 10,
        // 5). Sent as 25 SEPARATE one-row batches (matching
        // `segments_split_at_target_row_count_with_correct_stats`'s own
        // convention above): a single big incoming batch is never split
        // mid-way (matches `SegmentWriter`'s own documented behavior), so
        // this is what actually exercises the flush-between-batches path.
        // A bug that restarted `next_id` at 0 would collide with segment
        // 0's existing file and/or its manifest entry —
        // `NativeManifest::build`'s own `validate()` (duplicate segment
        // id) would catch a collision, but we assert the actual ids
        // directly for a load-bearing, specific check.
        let schema = small_schema();
        let more: Vec<RecordBatch> = (25..50)
            .map(|i| small_batch(&schema, i, 1, &["x", "y"]))
            .collect();
        let options = NativeWriteOptions {
            target_rows_per_segment: 10,
            ..NativeWriteOptions::default()
        };
        let result = append_to_native_table(boxed_stream(more), &out, options)
            .await
            .unwrap();
        assert_eq!(result.segments_appended, 3);
        assert_eq!(result.total_segments, 6);

        let manifest = native_manifest::read_manifest(&out).unwrap();
        let mut ids: Vec<u32> = manifest.segments.iter().map(|s| s.id).collect();
        ids.sort_unstable();
        assert_eq!(
            ids,
            vec![0, 1, 2, 3, 4, 5],
            "new segment ids must continue from the existing maximum, never restart at 0"
        );
        assert_eq!(manifest.snapshot.row_count, 50);
        // Every segment file physically exists under its own id.
        for id in &ids {
            assert!(native_manifest::segment_full_path(&out, *id).is_file());
        }
    }

    #[tokio::test]
    async fn append_inherits_dictionary_encoding_from_the_target_not_the_new_datas_cardinality() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        // Low-cardinality base table -> `category` is dictionary-encoded.
        let first = create_base_table(&out, 100, &["red", "green", "blue"], 1_000_000).await;
        assert_eq!(first.segments, 1);
        let manifest_before = native_manifest::read_manifest(&out).unwrap();
        let category_before = manifest_before
            .schema
            .iter()
            .find(|f| f.name == "category")
            .unwrap();
        assert!(matches!(
            category_before.data_type,
            native_manifest::ManifestDataType::Dictionary { .. }
        ));

        // Append a batch with HIGH cardinality for `category` -- if this
        // writer re-derived the dictionary decision from the NEW data's
        // own cardinality (the bug task 001 flagged), it would either
        // reject this batch or, worse, silently write a schema-violating
        // plain Utf8 segment. It must instead cast to the TABLE's existing
        // Dictionary(Int32, Utf8) encoding unconditionally.
        let wide_categories: Vec<String> = (0..600).map(|i| format!("wide-{i}")).collect();
        let wide_refs: Vec<&str> = wide_categories.iter().map(|s| s.as_str()).collect();
        let schema = small_schema();
        let wide_batch = small_batch(&schema, 100, 600, &wide_refs);

        let result = append_to_native_table(
            boxed_stream(vec![wide_batch]),
            &out,
            NativeWriteOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(result.rows_appended, 600);

        let manifest_after = native_manifest::read_manifest(&out).unwrap();
        let category_after = manifest_after
            .schema
            .iter()
            .find(|f| f.name == "category")
            .unwrap();
        assert!(
            matches!(
                category_after.data_type,
                native_manifest::ManifestDataType::Dictionary { .. }
            ),
            "the declared schema's dictionary decision must not change on Append"
        );

        // The NEW segment's actual array must ALSO physically be
        // Dictionary-typed, not just the manifest's declared schema.
        let (_, batches) = read_back(&out).unwrap();
        assert_eq!(batches.len(), 2, "one batch per segment");
        for b in &batches {
            assert!(
                matches!(b.column(1).data_type(), DT::Dictionary(_, _)),
                "every segment, old and newly appended, must be Dictionary-encoded to match \
                 the table's inherited decision"
            );
        }
    }

    #[tokio::test]
    async fn append_of_zero_rows_is_a_no_op_and_does_not_touch_the_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        let first = create_base_table(&out, 10, &["a"], 1_000_000).await;
        let manifest_before = native_manifest::read_manifest(&out).unwrap();

        // An entirely empty stream.
        let result =
            append_to_native_table(boxed_stream(vec![]), &out, NativeWriteOptions::default())
                .await
                .unwrap();
        assert_eq!(result.rows_appended, 0);
        assert_eq!(result.segments_appended, 0);
        assert_eq!(result.version, first.version, "no version bump for a no-op");
        assert_eq!(result.total_rows, 10);
        assert_eq!(result.table_id, first.table_id);

        // A stream with a batch that has zero rows (not zero batches).
        let schema = small_schema();
        let empty_batch = RecordBatch::new_empty(schema);
        let result2 = append_to_native_table(
            boxed_stream(vec![empty_batch]),
            &out,
            NativeWriteOptions::default(),
        )
        .await
        .unwrap();
        assert_eq!(result2.rows_appended, 0);
        assert_eq!(result2.version, first.version);

        // The manifest on disk must be BYTE-FOR-BYTE the same object as
        // before -- not just "logically equal", genuinely untouched.
        let manifest_after = native_manifest::read_manifest(&out).unwrap();
        assert_eq!(manifest_before, manifest_after);
    }

    #[tokio::test]
    async fn append_refuses_a_column_count_mismatch_cleanly_and_leaves_the_manifest_intact() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        let first = create_base_table(&out, 10, &["a"], 1_000_000).await;
        let manifest_before = native_manifest::read_manifest(&out).unwrap();

        // Only 2 columns instead of small_schema()'s 3.
        let bad_schema: SchemaRef = Arc::new(Schema::new(vec![
            ArrowField::new("id", DT::Int64, false),
            ArrowField::new("category", DT::Utf8, true),
        ]));
        let bad_batch = RecordBatch::try_new(
            bad_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();

        let err = append_to_native_table(
            boxed_stream(vec![bad_batch]),
            &out,
            NativeWriteOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, QueryError::Type(_)), "{err:?}");
        assert!(err.to_string().contains("2 column"), "{err}");
        assert!(err.to_string().contains('3'), "{err}");

        let manifest_after = native_manifest::read_manifest(&out).unwrap();
        assert_eq!(
            manifest_before, manifest_after,
            "a rejected Append must leave the existing manifest completely intact"
        );
        assert_eq!(first.rows, manifest_after.snapshot.row_count);
    }

    #[tokio::test]
    async fn append_refuses_a_column_type_mismatch_cleanly_and_leaves_the_manifest_intact() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        create_base_table(&out, 10, &["a"], 1_000_000).await;
        let manifest_before = native_manifest::read_manifest(&out).unwrap();

        // `price` (position 2) is Float64 in the target; supply Utf8
        // instead -- not the sanctioned dictionary coercion, so this must
        // be a clean, named error, never a silent cast/corruption.
        let bad_schema: SchemaRef = Arc::new(Schema::new(vec![
            ArrowField::new("id", DT::Int64, false),
            ArrowField::new("category", DT::Utf8, true),
            ArrowField::new("price", DT::Utf8, true),
        ]));
        let bad_batch = RecordBatch::try_new(
            bad_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["a"])),
                Arc::new(StringArray::from(vec!["not-a-number"])),
            ],
        )
        .unwrap();

        let err = append_to_native_table(
            boxed_stream(vec![bad_batch]),
            &out,
            NativeWriteOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, QueryError::Type(_)), "{err:?}");
        let msg = err.to_string();
        assert!(msg.contains("price"), "{msg}");
        assert!(msg.to_lowercase().contains("utf8"), "{msg}");

        let manifest_after = native_manifest::read_manifest(&out).unwrap();
        assert_eq!(
            manifest_before, manifest_after,
            "a rejected Append must leave the existing manifest completely intact"
        );
    }

    #[tokio::test]
    async fn append_refuses_a_missing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("does_not_exist");
        let schema = small_schema();
        let batch = small_batch(&schema, 0, 5, &["a"]);
        let err = append_to_native_table(
            boxed_stream(vec![batch]),
            &out,
            NativeWriteOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, QueryError::Storage(_)), "{err:?}");
    }

    #[tokio::test]
    async fn append_refuses_a_non_native_destination() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        std::fs::create_dir_all(&out).unwrap();
        std::fs::write(out.join("not_a_manifest.txt"), b"hello").unwrap();

        let schema = small_schema();
        let batch = small_batch(&schema, 0, 5, &["a"]);
        let err = append_to_native_table(
            boxed_stream(vec![batch]),
            &out,
            NativeWriteOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, QueryError::Storage(_)), "{err:?}");
        assert!(
            out.join("not_a_manifest.txt").exists(),
            "a refused Append must not touch the existing directory"
        );
    }

    #[tokio::test]
    async fn append_splits_new_rows_into_multiple_segments_per_target_rows_per_segment() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        create_base_table(&out, 5, &["a"], 1_000_000).await;

        let schema = small_schema();
        // 25 SEPARATE one-row batches -- a single big batch is never split
        // mid-way (see the sibling `append_continues_segment_ids...` test's
        // comment for why).
        let more: Vec<RecordBatch> = (5..30)
            .map(|i| small_batch(&schema, i, 1, &["a", "b"]))
            .collect();
        let options = NativeWriteOptions {
            target_rows_per_segment: 10,
            ..NativeWriteOptions::default()
        };
        let result = append_to_native_table(boxed_stream(more), &out, options)
            .await
            .unwrap();
        // 25 new rows / 10 per segment -> 3 new segments (10, 10, 5).
        assert_eq!(result.segments_appended, 3);
        assert_eq!(result.rows_appended, 25);
        assert_eq!(result.total_segments, 4, "1 original + 3 new");
        assert_eq!(result.total_rows, 30);
    }

    // ---------- the reusable, non-publishing building blocks, called
    // DIRECTLY (proving task 003/004 can compose them without going
    // through the self-publishing `append_to_native_table` wrapper) ----------

    #[tokio::test]
    async fn write_append_segments_does_not_touch_or_publish_the_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        create_base_table(&out, 10, &["a"], 1_000_000).await;
        let manifest_before = native_manifest::read_manifest(&out).unwrap();

        let schema = small_schema();
        let more = small_batch(&schema, 10, 5, &["a"]);
        let new_segments = write_append_segments(
            boxed_stream(vec![more]),
            &manifest_before,
            &out,
            NativeWriteOptions::default(),
        )
        .await
        .unwrap();

        assert_eq!(new_segments.len(), 1);
        assert_eq!(new_segments[0].id, 1, "continues from segment 0");
        assert_eq!(new_segments[0].row_count, 5);
        // The segment FILE was written directly into the live directory...
        assert!(native_manifest::segment_full_path(&out, 1).is_file());
        // ...but the MANIFEST must be completely untouched -- this
        // function is explicitly non-publishing.
        let manifest_after = native_manifest::read_manifest(&out).unwrap();
        assert_eq!(manifest_before, manifest_after);
        assert_eq!(
            manifest_after.segments.len(),
            1,
            "still just the original segment"
        );
    }

    #[tokio::test]
    async fn publish_manifest_update_composes_directly_with_write_append_segments_output() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        create_base_table(&out, 10, &["a"], 1_000_000).await;
        let existing = native_manifest::read_manifest(&out).unwrap();

        let schema = small_schema();
        let more = small_batch(&schema, 10, 7, &["a"]);
        let new_segments = write_append_segments(
            boxed_stream(vec![more]),
            &existing,
            &out,
            NativeWriteOptions::default(),
        )
        .await
        .unwrap();

        // A caller (this test, standing in for a future task 004 UPDATE)
        // assembles its OWN full segment list -- here, simply the existing
        // ones plus the new ones, exactly what `append_to_native_table`
        // itself does internally, but invoked directly to prove the two
        // building blocks compose without the self-publishing wrapper.
        let mut all_segments = existing.segments.clone();
        all_segments.extend(new_segments);
        let published = publish_manifest_update(
            &out,
            existing.arrow_schema().as_ref(),
            existing.table_id.clone(),
            existing.snapshot.version + 1,
            all_segments,
            123,
        )
        .unwrap();

        assert_eq!(published.snapshot.version, 2);
        assert_eq!(published.snapshot.row_count, 17);
        assert_eq!(published.segments.len(), 2);
        assert_eq!(published.table_id, existing.table_id);

        let read_back_manifest = native_manifest::read_manifest(&out).unwrap();
        assert_eq!(read_back_manifest, published);
    }

    // ---------- single-writer lock ----------

    #[test]
    fn lock_table_for_write_blocks_a_second_concurrent_attempt() {
        let dir = tempfile::tempdir().unwrap();
        let table_dir = dir.path().join("t");
        std::fs::create_dir_all(&table_dir).unwrap();

        let _first = lock_table_for_write(&table_dir).expect("first lock succeeds");
        let err = lock_table_for_write(&table_dir).expect_err(
            "a second, independent lock attempt on the SAME table directory must fail while \
             the first is held -- flock is per OPEN FILE DESCRIPTION, so two independent \
             std::fs::File::open calls in the SAME process still contend correctly",
        );
        assert!(matches!(err, QueryError::Storage(_)), "{err:?}");
        assert!(
            err.to_string().contains(&table_dir.display().to_string()),
            "the error must name the table directory: {err}"
        );
    }

    #[test]
    fn lock_is_released_deterministically_when_the_guard_drops() {
        let dir = tempfile::tempdir().unwrap();
        let table_dir = dir.path().join("t");
        std::fs::create_dir_all(&table_dir).unwrap();

        {
            let _guard = lock_table_for_write(&table_dir).expect("first lock succeeds");
            assert!(
                lock_table_for_write(&table_dir).is_err(),
                "contended while the guard is alive"
            );
        } // guard dropped here -> Drop::drop calls unlock()

        let second = lock_table_for_write(&table_dir);
        assert!(
            second.is_ok(),
            "must succeed immediately after the first guard is dropped, with no manual cleanup"
        );
    }

    #[tokio::test]
    async fn append_to_native_table_fails_cleanly_when_another_writer_holds_the_lock() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        create_base_table(&out, 10, &["a"], 1_000_000).await;
        let manifest_before = native_manifest::read_manifest(&out).unwrap();

        // Hold the lock externally, simulating a concurrent writer already
        // mid-mutation.
        let _held = lock_table_for_write(&out).expect("acquire the lock externally");

        let schema = small_schema();
        let more = small_batch(&schema, 10, 5, &["a"]);
        let err = append_to_native_table(
            boxed_stream(vec![more]),
            &out,
            NativeWriteOptions::default(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, QueryError::Storage(_)), "{err:?}");
        assert!(
            err.to_string().contains(&out.display().to_string()),
            "{err}"
        );

        // Nothing must have been written -- no new segment files, no
        // manifest change.
        let manifest_after = native_manifest::read_manifest(&out).unwrap();
        assert_eq!(manifest_before, manifest_after);
        assert!(!native_manifest::segment_full_path(&out, 1).exists());
    }

    #[test]
    fn lock_path_for_is_a_stable_sibling_of_the_table_directory() {
        let table_dir = Path::new("/some/root/mytable");
        let lock1 = lock_path_for(table_dir);
        let lock2 = lock_path_for(table_dir);
        assert_eq!(lock1, lock2, "computed once per TABLE, not per attempt");
        assert_eq!(lock1, Path::new("/some/root/mytable.lock"));
    }

    // ---------- write_batches_with_options' Append dispatch (CLI parity) ----------

    #[tokio::test]
    async fn write_batches_with_options_append_mode_matches_append_to_native_table() {
        let dir = tempfile::tempdir().unwrap();
        let out = dir.path().join("t");
        create_base_table(&out, 10, &["a"], 1_000_000).await;

        let schema = small_schema();
        let more = small_batch(&schema, 10, 5, &["a"]);
        // `schema` here is deliberately a DIFFERENT (but structurally
        // compatible) SchemaRef instance than the target's own -- proves
        // it is genuinely ignored for Append (the target's schema from
        // the existing manifest is what actually governs), not merely
        // unused by coincidence.
        let result = write_batches_with_options(
            boxed_stream(vec![more]),
            schema,
            &out,
            NativeWriteMode::Append,
            NativeWriteOptions::default(),
        )
        .await
        .unwrap();

        // write_batches_with_options reports TOTALS (NativeWriteResult's
        // existing "whole table" meaning), not the delta.
        assert_eq!(result.rows, 15);
        assert_eq!(result.segments, 2);
        assert_eq!(result.version, 2);
    }
}
