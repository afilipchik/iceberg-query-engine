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
/// directory. Mirrors `LanceWriteMode` minus `Append`: this epic's write
/// path is full-table-replace only ("a load always produces one complete
/// new snapshot; no partial append/update in this epic" — task 003's own
/// Description).
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
}

impl std::str::FromStr for NativeWriteMode {
    type Err = QueryError;
    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "create" => Ok(Self::Create),
            "overwrite" => Ok(Self::Overwrite),
            other => Err(QueryError::NotImplemented(format!(
                "unknown native table write mode `{other}` (expected create or overwrite)"
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

/// What a write produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeWriteResult {
    /// Stable table identity (UUID v4 on first create, preserved across an
    /// `Overwrite`).
    pub table_id: String,
    /// The snapshot version this write committed (1 for a fresh table).
    pub version: u64,
    /// Rows written (== `snapshot.row_count`).
    pub rows: u64,
    /// Segments written.
    pub segments: usize,
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
///
/// # On error
///
/// The destination directory (`out_dir`) is left completely untouched: a
/// staging directory is written to first and only atomically published
/// (`native_manifest::publish_table_dir`) after everything else succeeds.
/// The staging directory itself is best-effort cleaned up on any error path
/// (not load-bearing for correctness — a leftover `.<pid>.building` staging
/// dir next to `out_dir` is inert, never read by anything).
pub async fn write_batches_with_options(
    stream: RecordBatchStream,
    schema: SchemaRef,
    out_dir: impl AsRef<Path>,
    mode: NativeWriteMode,
    options: NativeWriteOptions,
) -> Result<NativeWriteResult> {
    let final_dir = out_dir.as_ref().to_path_buf();

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
        assert!("append".parse::<NativeWriteMode>().is_err());
    }
}
