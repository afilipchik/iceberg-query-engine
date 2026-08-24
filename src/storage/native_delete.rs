//! Native table DELETE: row-identification + deletion-vector editing
//! (native-tables-mutation epic, task 003). Sibling to `native_write.rs`
//! (task 002's Append) and `native_manifest.rs` (which owns the
//! `Segment::deleted_rows` field this module edits) — DELETE never writes
//! new segment DATA (no new `.arrow` files); it only edits `Vec<Segment>`
//! in memory and publishes ONE new manifest via `native_write::
//! publish_manifest_update` (task 002's single-file atomic-rename
//! primitive, reused completely UNCHANGED — this module adds no second
//! publish path).
//!
//! # Row identification — a BESPOKE loop, not the generic query pipeline
//!
//! The standard `LogicalPlan`/`PhysicalOperator` pipeline (what CTAS/INSERT
//! reuse for their source query) has no way to carry a matched row's
//! (segment id, local position) back out — `TableProvider::scan()` returns
//! only `Vec<RecordBatch>`, no positional metadata. So [`identify_matching_rows`]
//! instead: opens the target's CURRENT manifest's segments directly (in id
//! order), reads each one via `ipc_cache::read_row_group` (the SAME
//! mmap-backed reader `NativeTable::scan` uses — batches come back in
//! stable on-disk block order; `native_write.rs` already reslices before
//! writing, so multiple ~65,536-row batches per segment is the NORMAL
//! case), evaluates the WHERE predicate batch-by-batch via
//! `physical::operators::evaluate_expr` (the SAME function `FilterExec`
//! itself uses), and tracks a running local-row offset across a segment's
//! batches to convert each match into a segment-relative position.
//! `predicate: None` means "match every row" (DELETE with no WHERE) —
//! handled without ever building an all-true mask (see the function body).
//!
//! # Idempotency is structural, not a special case
//!
//! [`apply_deletions`] unions newly matched positions into a segment's
//! EXISTING `deleted_rows` via a `BTreeSet` (insert + re-serialize as a
//! sorted `Vec`) — re-matching an already-deleted row is then naturally a
//! no-op. Two overlapping DELETEs run one after another can never corrupt
//! or double-count the vector.
//!
//! # The narrow in-scope compaction exception
//!
//! [`apply_deletions`] drops a `Segment` entirely from the returned
//! `Vec<Segment>` once `deleted_rows.len() == row_count` (every row
//! tombstoned) — its `.arrow` file becomes an inert, unreferenced orphan,
//! exactly like an abandoned staging directory already is today. This
//! bounds the deletion vector's own worst-case size; it is NOT full
//! compaction (a future epic's job — see task 001's Outcome, Decision 3).
//!
//! # Reusable, non-publishing building blocks (task 001's Decision 2)
//!
//! For task 004 (UPDATE) to compose into ONE atomic publish alongside its
//! own Append, rather than calling two independent self-publishing
//! entrypoints sequentially:
//!
//! 1. [`identify_matching_rows`] — the row-identification core. Task 004
//!    calls this directly to get BOTH the tombstone-candidate positions
//!    AND (`materialize_rows: true`) the matched rows' CURRENT values
//!    (`SegmentMatch::rows`) to evaluate its SET expressions against — do
//!    not re-derive an equivalent scan. DELETE itself calls this with
//!    `materialize_rows: false` (positions only — it never needs row
//!    VALUES, only WHICH rows), so a broad `DELETE FROM t` (matching most
//!    or all of a table) does not pay for materializing row data it will
//!    never use.
//! 2. [`apply_deletions`] — folds a `MatchedRows` into a target manifest's
//!    existing `Vec<Segment>`, returning the new COMPLETE list (old
//!    segments, `deleted_rows`-extended or dropped, per above). Does NOT
//!    touch or publish any manifest.
//!
//! [`delete_from_native_table`] composes: `native_write::
//! lock_table_for_write` (held for the whole span) -> `native_manifest::
//! read_manifest` -> [`identify_matching_rows`] -> [`apply_deletions`] ->
//! `native_write::publish_manifest_update` — the self-publishing
//! entrypoint `ExecutionContext::delete_from_native_table` calls. Task 004
//! is expected to instead: acquire the SAME lock ONCE, read the manifest
//! ONCE, call [`identify_matching_rows`] and [`apply_deletions`] itself,
//! separately call `native_write::write_append_segments` for its
//! recomputed rows, fold BOTH edited-and-new segment lists into one
//! `Vec<Segment>`, and call `native_write::publish_manifest_update` ONCE —
//! never two sequential self-publishing calls (see `native_write.rs`'s own
//! module doc for the full reasoning).

use crate::error::{QueryError, Result};
use crate::physical::operators::evaluate_expr;
use crate::planner::Expr;
use crate::storage::ipc_cache;
use crate::storage::native_manifest::{self, NativeManifest, Segment};
use crate::storage::native_write;
use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeSet, HashMap};
use std::path::Path;

// ============================================================================
// Row identification (building block 1)
// ============================================================================

/// One segment's row-identification result — see the module doc.
#[derive(Debug, Clone)]
pub struct SegmentMatch {
    /// Which segment (`Segment::id`) this result is for.
    pub segment_id: u32,
    /// Sorted, ascending LOCAL positions within this segment's own
    /// on-disk row order — directly union-able into `Segment::
    /// deleted_rows` (same meaning, same indexing).
    pub positions: Vec<u32>,
    /// The matched rows' CURRENT values, in `positions` order, same
    /// schema as [`MatchedRows::schema`] — populated only when
    /// `identify_matching_rows` was called with `materialize_rows: true`
    /// (task 004/UPDATE's own need); `None` otherwise (DELETE's own call,
    /// which needs only `positions`).
    pub rows: Option<RecordBatch>,
}

/// What [`identify_matching_rows`] found, across every active segment.
#[derive(Debug, Clone)]
pub struct MatchedRows {
    /// The target table's schema (`NativeManifest::arrow_schema()`) — the
    /// schema `rows` (when populated) uses.
    pub schema: SchemaRef,
    /// Only segments with >= 1 match, sorted by segment id.
    pub per_segment: Vec<SegmentMatch>,
    /// Total matched row count across every segment — `0` iff
    /// `per_segment` is empty.
    pub total_matched: u64,
}

impl MatchedRows {
    pub fn is_empty(&self) -> bool {
        self.total_matched == 0
    }
}

fn downcast_bool(arr: &ArrayRef) -> Result<BooleanArray> {
    arr.as_any()
        .downcast_ref::<BooleanArray>()
        .cloned()
        .ok_or_else(|| {
            QueryError::Execution(
                "DELETE/UPDATE: WHERE predicate did not evaluate to a boolean array".to_string(),
            )
        })
}

fn concat_matched(schema: &SchemaRef, batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    match batches.len() {
        0 => Ok(RecordBatch::new_empty(schema.clone())),
        1 => Ok(batches.into_iter().next().expect("len checked above")),
        _ => Ok(arrow::compute::concat_batches(schema, batches.iter())?),
    }
}

/// Identify every row in `target`'s active segments matching `predicate`
/// (`None` = match every row — DELETE/UPDATE with no WHERE, a real
/// supported case, not an error). See the module doc for the full
/// algorithm and why this is a bespoke loop rather than the generic query
/// pipeline. `materialize_rows` controls whether matched rows' actual
/// column DATA is gathered (`SegmentMatch::rows`) in addition to their
/// positions — see [`SegmentMatch::rows`]'s own doc for which caller wants
/// which.
///
/// `dir` is the LIVE table directory (segments are read directly from it,
/// exactly like `NativeTable::scan` does). Does NOT consult or edit
/// `Segment::deleted_rows` itself — an ALREADY-deleted row is still
/// "matched" again by an overlapping predicate (this is what makes
/// [`apply_deletions`]'s `BTreeSet` union idempotent rather than this
/// function needing to skip already-deleted positions itself).
pub fn identify_matching_rows(
    dir: &Path,
    target: &NativeManifest,
    predicate: Option<&Expr>,
    materialize_rows: bool,
) -> Result<MatchedRows> {
    let schema = target.arrow_schema();
    let mut segs: Vec<&Segment> = target.segments.iter().collect();
    segs.sort_by_key(|s| s.id);

    let mut per_segment = Vec::new();
    let mut total_matched = 0u64;
    for seg in segs {
        let batches = ipc_cache::read_row_group(dir, seg.id as usize, None, None)?;
        let mut positions: Vec<u32> = Vec::new();
        let mut matched_batches: Vec<RecordBatch> = Vec::new();
        let mut local_offset: u32 = 0;

        for batch in &batches {
            let n = batch.num_rows() as u32;
            match predicate {
                None => {
                    // Every row matches -- append the whole contiguous
                    // range directly rather than building and scanning an
                    // all-true mask. The common, explicitly-supported
                    // "delete/update all rows" shape.
                    positions.extend(local_offset..local_offset + n);
                    if materialize_rows {
                        matched_batches.push(batch.clone());
                    }
                }
                Some(pred) => {
                    let arr = evaluate_expr(batch, pred)?;
                    let mask = downcast_bool(&arr)?;
                    for (i, is_match) in mask.iter().enumerate() {
                        // Three-valued SQL logic: NULL is "not matched",
                        // same convention every other predicate evaluation
                        // in this engine (e.g. `FilterExec`) already uses.
                        if is_match == Some(true) {
                            positions.push(local_offset + i as u32);
                        }
                    }
                    if materialize_rows && mask.true_count() > 0 {
                        let cols: Result<Vec<ArrayRef>> = batch
                            .columns()
                            .iter()
                            .map(|c| arrow::compute::filter(c.as_ref(), &mask).map_err(Into::into))
                            .collect();
                        matched_batches.push(RecordBatch::try_new(batch.schema(), cols?)?);
                    }
                }
            }
            local_offset += n;
        }

        if !positions.is_empty() {
            let rows = if materialize_rows {
                Some(concat_matched(&schema, matched_batches)?)
            } else {
                None
            };
            total_matched += positions.len() as u64;
            per_segment.push(SegmentMatch {
                segment_id: seg.id,
                positions,
                rows,
            });
        }
    }

    Ok(MatchedRows {
        schema,
        per_segment,
        total_matched,
    })
}

// ============================================================================
// Deletion-vector editing (building block 2)
// ============================================================================

/// Fold `matches` into `target`'s existing segment list — see the module
/// doc's "Idempotency" and "narrow in-scope compaction exception"
/// sections for the exact rules. Segments with no match at all are
/// returned completely unchanged (cheap clone, same values). Does NOT
/// touch or publish any manifest.
pub fn apply_deletions(target: &NativeManifest, matches: &MatchedRows) -> Vec<Segment> {
    let by_id: HashMap<u32, &SegmentMatch> = matches
        .per_segment
        .iter()
        .map(|m| (m.segment_id, m))
        .collect();

    target
        .segments
        .iter()
        .filter_map(|seg| {
            let Some(m) = by_id.get(&seg.id) else {
                return Some(seg.clone());
            };
            // BTreeSet union: idempotent by construction -- re-matching an
            // already-deleted position via a second, overlapping DELETE is
            // naturally a no-op, and the result is always sorted +
            // deduplicated (NativeManifest::validate's own requirement).
            let mut set: BTreeSet<u32> = seg.deleted_rows.iter().copied().collect();
            set.extend(m.positions.iter().copied());
            if set.len() as u64 >= seg.row_count {
                // Wholly tombstoned -- drop the segment entirely (task
                // 001's Decision 3's narrow compaction exception).
                return None;
            }
            let mut new_seg = seg.clone();
            new_seg.deleted_rows = set.into_iter().collect();
            Some(new_seg)
        })
        .collect()
}

// ============================================================================
// The self-publishing entrypoint
// ============================================================================

/// What [`delete_from_native_table`] produced.
#[derive(Debug, Clone)]
pub struct NativeDeleteResult {
    /// Stable table identity (UUID v4) — unchanged by a DELETE.
    pub table_id: String,
    /// The snapshot version this DELETE committed. Equal to the PRE-delete
    /// version when the predicate matched zero rows OR every matched row
    /// was ALREADY tombstoned by a prior DELETE (both are legitimate
    /// no-ops, not an error — see `rows_deleted`'s own doc).
    pub version: u64,
    /// Rows NEWLY tombstoned by this DELETE — the NET count (previously
    /// live, now dead), NOT the gross number of rows the predicate
    /// matched. A predicate that re-matches an already-deleted row (an
    /// overlapping repeat DELETE) does not count that row again: this is
    /// what makes a fully-redundant repeat DELETE report `0` and skip
    /// publishing entirely, rather than reporting the same nonzero count
    /// every time it runs.
    pub rows_deleted: u64,
    /// Segments dropped entirely because this DELETE tombstoned their
    /// last remaining live row (task 001's Decision 3). Always `0` when
    /// `rows_deleted` is `0` (dropping a segment requires at least one row
    /// to have newly transitioned from live to dead).
    pub segments_dropped: usize,
    /// The table's TOTAL LOGICAL (post-delete, visible) row count after
    /// this DELETE — NOT `NativeManifest::snapshot.row_count`, which stays
    /// the PHYSICAL count by design (see `Segment::row_count`'s own doc).
    pub total_rows: u64,
    /// The table's TOTAL segment count after this DELETE.
    pub total_segments: usize,
}

/// Delete every row of the native table at `table_dir` matching
/// `predicate` (`None` = delete every row) — the full self-publishing
/// entrypoint composing this module's two building blocks with task 002's
/// lock and publish primitives. This is the entrypoint
/// `ExecutionContext::delete_from_native_table` calls.
///
/// `table_dir` MUST already be a native table directory (a clean
/// `QueryError::Storage` from `native_manifest::read_manifest` otherwise —
/// DELETE never creates a table).
///
/// **Two distinct no-op cases, both clean (manifest untouched, version
/// never bumps) — mirrors `native_write::append_to_native_table`'s own
/// zero-row-source precedent, extended to a second case that precedent
/// does not have an analogue for**:
/// 1. The predicate matches zero PHYSICAL rows (`identify_matching_rows`
///    finds nothing) — skips even calling `apply_deletions`.
/// 2. The predicate matches rows, but EVERY one of them was ALREADY
///    tombstoned by a prior DELETE (a fully-redundant, wholly-overlapping
///    repeat) — `apply_deletions`' `BTreeSet` union produces a
///    `Vec<Segment>` that is structurally IDENTICAL to `existing.segments`
///    (nothing new to insert anywhere), detected via the LOGICAL row-count
///    delta (`rows_deleted == 0`) rather than a segment-by-segment
///    equality check. Publishing an identical manifest under a bumped
///    version would be correct but pointless — this is the concrete
///    mechanism (not just documentation) behind this file's "repeated
///    overlapping deletes must not corrupt or double-count" guarantee.
///
/// # On error
///
/// A lock-contention failure or a missing/corrupt manifest leaves
/// `table_dir` completely untouched. `identify_matching_rows` never
/// writes anything (a pure read), so any error there also leaves the
/// table untouched. Once reached, `publish_manifest_update`'s single-file
/// atomic rename means a reader never observes a half-updated manifest —
/// either the fully-old one or the fully-new one.
pub async fn delete_from_native_table(
    table_dir: impl AsRef<Path>,
    predicate: Option<&Expr>,
) -> Result<NativeDeleteResult> {
    let table_dir = table_dir.as_ref().to_path_buf();
    let _lock = native_write::lock_table_for_write(&table_dir)?;
    let existing = native_manifest::read_manifest(&table_dir)?;
    let total_rows_before: u64 = existing.segments.iter().map(|s| s.live_row_count()).sum();

    let unchanged_result = || NativeDeleteResult {
        table_id: existing.table_id.clone(),
        version: existing.snapshot.version,
        rows_deleted: 0,
        segments_dropped: 0,
        total_rows: total_rows_before,
        total_segments: existing.segments.len(),
    };

    // DELETE never needs matched rows' VALUES, only their positions.
    let matches = identify_matching_rows(&table_dir, &existing, predicate, false)?;
    if matches.is_empty() {
        return Ok(unchanged_result());
    }

    let new_segments = apply_deletions(&existing, &matches);
    let total_rows_after: u64 = new_segments.iter().map(|s| s.live_row_count()).sum();
    let rows_deleted = total_rows_before - total_rows_after;
    if rows_deleted == 0 {
        // Case 2 above: every matched row was already tombstoned.
        // `new_segments` is guaranteed structurally identical to
        // `existing.segments` here (see this function's own doc) --
        // publishing it would be a correct but pointless no-op write.
        return Ok(unchanged_result());
    }

    let segments_dropped = existing.segments.len() - new_segments.len();
    let total_segments = new_segments.len();

    let schema = existing.arrow_schema();
    let manifest = native_write::publish_manifest_update(
        &table_dir,
        schema.as_ref(),
        existing.table_id.clone(),
        existing.snapshot.version + 1,
        new_segments,
        native_write::now_ms(),
    )?;

    Ok(NativeDeleteResult {
        table_id: manifest.table_id.clone(),
        version: manifest.snapshot.version,
        rows_deleted,
        segments_dropped,
        total_rows: total_rows_after,
        total_segments,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{BinaryOp, Column, ScalarValue};
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType as DT, Field as ArrowField, Schema};
    use native_manifest::{publish_table_dir, segment_full_path, staging_dir_for, write_manifest};
    use std::sync::Arc;

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            ArrowField::new("id", DT::Int64, false),
            ArrowField::new("category", DT::Utf8, true),
            ArrowField::new("price", DT::Float64, true),
        ]))
    }

    fn batch(schema: &SchemaRef, ids: &[i64]) -> RecordBatch {
        let cats: Vec<String> = ids.iter().map(|i| format!("c{}", i % 3)).collect();
        let prices: Vec<f64> = ids.iter().map(|i| *i as f64 * 1.5).collect();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(cats)),
                Arc::new(Float64Array::from(prices)),
            ],
        )
        .unwrap()
    }

    /// Writes a table with `segments` (each a `Vec<i64>` of ids) as real
    /// on-disk Arrow IPC segments + a published manifest, directly against
    /// task 002's manifest API (mirrors `native_table.rs`'s own
    /// `write_test_table` discipline — independent of `native_write.rs`'s
    /// writer, so these tests exercise exactly what THIS module owns).
    fn write_test_table(final_dir: &Path, segments: &[Vec<i64>]) -> SchemaRef {
        let schema = schema();
        let staging = staging_dir_for(final_dir);
        std::fs::create_dir_all(&staging).unwrap();

        let mut seg_entries = Vec::new();
        for (id, ids) in segments.iter().enumerate() {
            let id = id as u32;
            let b = batch(&schema, ids);
            let path = segment_full_path(&staging, id);
            let file = std::fs::File::create(&path).unwrap();
            let mut w = arrow::ipc::writer::FileWriter::try_new(file, &b.schema()).unwrap();
            w.write(&b).unwrap();
            w.finish().unwrap();
            let byte_size = std::fs::metadata(&path).unwrap().len();
            seg_entries.push(Segment {
                id,
                path: Segment::expected_file_name(id),
                row_count: b.num_rows() as u64,
                byte_size,
                column_stats: native_manifest::compute_batch_stats(&b),
                deleted_rows: Vec::new(),
            });
        }
        let manifest = NativeManifest::build(
            &schema,
            NativeManifest::generate_table_id(),
            1,
            seg_entries,
            1_700_000_000_000,
        )
        .unwrap();
        write_manifest(&staging, &manifest).unwrap();
        publish_table_dir(&staging, final_dir).unwrap();
        schema
    }

    fn id_lt(n: i64) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(Expr::Column(Column::new("id"))),
            op: BinaryOp::Lt,
            right: Box::new(Expr::Literal(ScalarValue::Int64(n))),
        }
    }

    fn id_eq(n: i64) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(Expr::Column(Column::new("id"))),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(n))),
        }
    }

    // ---------- identify_matching_rows ----------

    #[test]
    fn identify_matching_rows_finds_correct_local_positions_per_segment() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3], vec![4, 5, 6]]);
        let manifest = native_manifest::read_manifest(dir.path()).unwrap();

        // id < 5 matches segment 0 entirely (positions 0,1,2) and segment
        // 1's first row (position 0, id=4).
        let matches =
            identify_matching_rows(dir.path(), &manifest, Some(&id_lt(5)), false).unwrap();
        assert_eq!(matches.total_matched, 4);
        assert_eq!(matches.per_segment.len(), 2);
        assert_eq!(matches.per_segment[0].segment_id, 0);
        assert_eq!(matches.per_segment[0].positions, vec![0, 1, 2]);
        assert_eq!(matches.per_segment[1].segment_id, 1);
        assert_eq!(matches.per_segment[1].positions, vec![0]);
        assert!(
            matches.per_segment[0].rows.is_none(),
            "materialize_rows=false must not populate row data"
        );
    }

    #[test]
    fn identify_matching_rows_with_no_predicate_matches_every_row() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2], vec![3, 4, 5]]);
        let manifest = native_manifest::read_manifest(dir.path()).unwrap();

        let matches = identify_matching_rows(dir.path(), &manifest, None, false).unwrap();
        assert_eq!(matches.total_matched, 5, "None predicate = every row");
        assert_eq!(matches.per_segment[0].positions, vec![0, 1]);
        assert_eq!(matches.per_segment[1].positions, vec![0, 1, 2]);
    }

    #[test]
    fn identify_matching_rows_with_a_predicate_matching_nothing_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3]]);
        let manifest = native_manifest::read_manifest(dir.path()).unwrap();

        let matches =
            identify_matching_rows(dir.path(), &manifest, Some(&id_eq(999)), false).unwrap();
        assert!(matches.is_empty());
        assert_eq!(matches.total_matched, 0);
        assert!(matches.per_segment.is_empty());
    }

    #[test]
    fn identify_matching_rows_materializes_matched_rows_current_values_when_asked() {
        // Task 004 (UPDATE)'s own need: matched rows' CURRENT column
        // values, not just positions.
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![10, 20, 30]]);
        let manifest = native_manifest::read_manifest(dir.path()).unwrap();

        let matches =
            identify_matching_rows(dir.path(), &manifest, Some(&id_lt(25)), true).unwrap();
        assert_eq!(matches.per_segment.len(), 1);
        let rows = matches.per_segment[0]
            .rows
            .as_ref()
            .expect("materialize_rows=true must populate row data");
        assert_eq!(rows.num_rows(), 2);
        let ids = rows
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values(), &[10, 20]);
    }

    #[test]
    fn identify_matching_rows_across_multiple_batches_within_one_segment_tracks_offset_correctly() {
        // A single segment whose IPC file holds MULTIPLE batches (the
        // normal case per `native_write.rs`'s reslicing) must still map
        // matches back to the correct, monotonically-increasing local
        // position across batch boundaries.
        let dir = tempfile::tempdir().unwrap();
        let schema = schema();
        let staging = staging_dir_for(dir.path());
        std::fs::create_dir_all(&staging).unwrap();
        let b1 = batch(&schema, &[1, 2, 3]);
        let b2 = batch(&schema, &[4, 5, 6]);
        let path = segment_full_path(&staging, 0);
        let file = std::fs::File::create(&path).unwrap();
        let mut w = arrow::ipc::writer::FileWriter::try_new(file, &schema).unwrap();
        w.write(&b1).unwrap();
        w.write(&b2).unwrap();
        w.finish().unwrap();
        let byte_size = std::fs::metadata(&path).unwrap().len();
        let seg = Segment {
            id: 0,
            path: Segment::expected_file_name(0),
            row_count: 6,
            byte_size,
            column_stats: native_manifest::compute_batch_stats(
                &arrow::compute::concat_batches(&schema, [&b1, &b2]).unwrap(),
            ),
            deleted_rows: Vec::new(),
        };
        let manifest = NativeManifest::build(&schema, "multi-batch", 1, vec![seg], 0).unwrap();
        write_manifest(&staging, &manifest).unwrap();
        publish_table_dir(&staging, dir.path()).unwrap();

        let manifest = native_manifest::read_manifest(dir.path()).unwrap();
        // id=5 is the SECOND row of the SECOND batch -> local position 4.
        let matches =
            identify_matching_rows(dir.path(), &manifest, Some(&id_eq(5)), false).unwrap();
        assert_eq!(matches.total_matched, 1);
        assert_eq!(matches.per_segment[0].positions, vec![4]);
    }

    // ---------- apply_deletions ----------

    #[test]
    fn apply_deletions_unions_into_existing_deleted_rows_idempotently() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3, 4, 5]]);
        let mut manifest = native_manifest::read_manifest(dir.path()).unwrap();
        manifest.segments[0].deleted_rows = vec![0]; // id=1 already deleted

        // A second, OVERLAPPING delete also matches id=1 (position 0) plus
        // id=3 (position 2).
        let matches =
            identify_matching_rows(dir.path(), &manifest, Some(&id_lt(4)), false).unwrap();
        let new_segments = apply_deletions(&manifest, &matches);
        assert_eq!(new_segments.len(), 1);
        assert_eq!(
            new_segments[0].deleted_rows,
            vec![0, 1, 2],
            "union must be sorted, deduplicated, and include both the pre-existing and newly \
             matched positions -- re-matching position 0 must not duplicate it"
        );
    }

    #[test]
    fn apply_deletions_drops_a_wholly_tombstoned_segment() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2], vec![3, 4, 5]]);
        let manifest = native_manifest::read_manifest(dir.path()).unwrap();

        // Delete every row of segment 0 (ids 1,2) only.
        let matches =
            identify_matching_rows(dir.path(), &manifest, Some(&id_lt(3)), false).unwrap();
        let new_segments = apply_deletions(&manifest, &matches);
        assert_eq!(
            new_segments.len(),
            1,
            "the wholly-tombstoned segment must be dropped entirely"
        );
        assert_eq!(new_segments[0].id, 1, "segment 1 (untouched) must remain");
        assert!(new_segments[0].deleted_rows.is_empty());
    }

    #[test]
    fn apply_deletions_leaves_untouched_segments_completely_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2], vec![3, 4]]);
        let manifest = native_manifest::read_manifest(dir.path()).unwrap();

        let matches =
            identify_matching_rows(dir.path(), &manifest, Some(&id_lt(2)), false).unwrap(); // only segment 0
        let new_segments = apply_deletions(&manifest, &matches);
        assert_eq!(new_segments.len(), 2);
        let seg1 = new_segments.iter().find(|s| s.id == 1).unwrap();
        assert_eq!(seg1, &manifest.segments[1], "must be an identical clone");
    }

    // ---------- delete_from_native_table (self-publishing entrypoint) ----------

    #[tokio::test]
    async fn delete_from_native_table_end_to_end_narrow_delete() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3], vec![4, 5]]);

        let result = delete_from_native_table(dir.path(), Some(&id_eq(2)))
            .await
            .unwrap();
        assert_eq!(result.rows_deleted, 1);
        assert_eq!(result.segments_dropped, 0);
        assert_eq!(result.total_rows, 4);
        assert_eq!(result.total_segments, 2);
        assert_eq!(result.version, 2, "version bumps by exactly one");

        let manifest = native_manifest::read_manifest(dir.path()).unwrap();
        assert_eq!(manifest.segments[0].deleted_rows, vec![1]);
        assert_eq!(
            manifest.snapshot.row_count, 5,
            "PHYSICAL row_count stays unaffected by delete (task 001's Decision 1)"
        );
    }

    #[tokio::test]
    async fn delete_all_rows_leaves_the_table_existing_but_logically_empty() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2], vec![3, 4, 5]]);

        let result = delete_from_native_table(dir.path(), None).await.unwrap();
        assert_eq!(result.rows_deleted, 5);
        assert_eq!(
            result.segments_dropped, 2,
            "every segment becomes wholly tombstoned and is dropped"
        );
        assert_eq!(result.total_rows, 0);
        assert_eq!(result.total_segments, 0);

        // The table must still EXIST and be a valid, readable manifest --
        // not deleted, not corrupted.
        let manifest = native_manifest::read_manifest(dir.path()).unwrap();
        assert!(manifest.segments.is_empty());
        assert_eq!(manifest.snapshot.version, 2);

        // A second delete-all against the now-empty table must be a clean
        // no-op, not an error.
        let second = delete_from_native_table(dir.path(), None).await.unwrap();
        assert_eq!(second.rows_deleted, 0);
        assert_eq!(second.version, 2, "no version bump for a no-op");
    }

    #[tokio::test]
    async fn delete_matching_zero_rows_is_a_clean_no_op() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3]]);
        let before = native_manifest::read_manifest(dir.path()).unwrap();

        let result = delete_from_native_table(dir.path(), Some(&id_eq(999)))
            .await
            .unwrap();
        assert_eq!(result.rows_deleted, 0);
        assert_eq!(result.segments_dropped, 0);
        assert_eq!(result.version, 1, "no version bump for a no-op");
        assert_eq!(result.total_rows, 3);

        let after = native_manifest::read_manifest(dir.path()).unwrap();
        assert_eq!(
            before, after,
            "the manifest must be byte-for-byte untouched"
        );
    }

    #[tokio::test]
    async fn a_fully_overlapping_repeat_delete_is_a_true_no_op() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3, 4, 5]]);

        let first = delete_from_native_table(dir.path(), Some(&id_lt(4)))
            .await
            .unwrap(); // deletes ids 1,2,3
        assert_eq!(first.rows_deleted, 3);
        assert_eq!(first.total_rows, 2);
        assert_eq!(first.version, 2);

        // FULLY overlapping: id < 2 (just id=1) is already deleted --
        // `identify_matching_rows` re-matches it (it doesn't consult
        // `deleted_rows` itself -- see that function's own doc), but
        // EVERY matched row was already tombstoned, so the NET newly-
        // deleted count must be zero and this must be a true no-op: no
        // manifest write, no version bump. This is the concrete mechanism
        // behind this file's own "repeated overlapping deletes must not
        // corrupt or double-count" acceptance criterion.
        let second = delete_from_native_table(dir.path(), Some(&id_lt(2)))
            .await
            .unwrap();
        assert_eq!(second.rows_deleted, 0, "no NEW row was deleted");
        assert_eq!(
            second.total_rows, 2,
            "logical row count must not shrink further"
        );
        assert_eq!(
            second.version, 2,
            "no version bump for a fully-redundant repeat delete"
        );

        let manifest = native_manifest::read_manifest(dir.path()).unwrap();
        assert_eq!(manifest.segments[0].deleted_rows, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn a_partially_overlapping_repeat_delete_counts_only_the_newly_deleted_rows() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3, 4, 5]]);

        let first = delete_from_native_table(dir.path(), Some(&id_lt(3)))
            .await
            .unwrap(); // deletes ids 1,2
        assert_eq!(first.rows_deleted, 2);
        assert_eq!(first.total_rows, 3);

        // PARTIALLY overlapping: id < 4 matches ids 1,2,3 -- 1 and 2 are
        // already gone, only 3 is genuinely new.
        let second = delete_from_native_table(dir.path(), Some(&id_lt(4)))
            .await
            .unwrap();
        assert_eq!(
            second.rows_deleted, 1,
            "only the genuinely NEW match (id=3) counts -- ids 1,2 must not be recounted"
        );
        assert_eq!(second.total_rows, 2);
        assert_eq!(second.version, 3, "a real change still bumps the version");

        let manifest = native_manifest::read_manifest(dir.path()).unwrap();
        assert_eq!(manifest.segments[0].deleted_rows, vec![0, 1, 2]);
    }

    #[tokio::test]
    async fn delete_targets_only_the_correct_segment_in_a_multi_segment_table() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]]);

        let result = delete_from_native_table(dir.path(), Some(&id_eq(5)))
            .await
            .unwrap();
        assert_eq!(result.rows_deleted, 1);
        assert_eq!(result.total_rows, 8);

        let manifest = native_manifest::read_manifest(dir.path()).unwrap();
        assert_eq!(manifest.segments.len(), 3, "no segment dropped");
        assert!(manifest.segments[0].deleted_rows.is_empty());
        assert_eq!(
            manifest.segments[1].deleted_rows,
            vec![1],
            "id=5 is segment 1's local position 1"
        );
        assert!(manifest.segments[2].deleted_rows.is_empty());
    }

    #[tokio::test]
    async fn delete_holds_the_single_writer_lock_for_its_whole_span() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3]]);

        // Hold the lock externally, exactly like a concurrent writer would.
        let _held = native_write::lock_table_for_write(dir.path()).unwrap();
        let err = delete_from_native_table(dir.path(), Some(&id_eq(1)))
            .await
            .unwrap_err();
        assert!(matches!(err, QueryError::Storage(_)), "{err:?}");
    }

    #[tokio::test]
    async fn delete_against_a_missing_table_is_a_clean_storage_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = delete_from_native_table(dir.path(), None)
            .await
            .unwrap_err();
        assert!(matches!(err, QueryError::Storage(_)), "{err:?}");
    }
}
