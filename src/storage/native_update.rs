//! Native table UPDATE: composes task 002's non-publishing Append write
//! core (`native_write::write_append_segments`) with task 003's
//! non-publishing DELETE row-identification/deletion-vector-editing core
//! (`native_delete::identify_matching_rows`/`apply_deletions`) into ONE
//! atomically-published operation (native-tables-mutation epic, task 004).
//! Sibling to `native_write.rs` (002), `native_delete.rs` (003) and
//! `native_manifest.rs` (format) -- adds no new manifest-building or
//! publish path of its own; every one of those is reused completely
//! UNCHANGED, exactly as task 001's design spike and task 002/003's own
//! Outcome sections instruct.
//!
//! # Why DELETE + INSERT must NOT be called as two separate self-publishing
//! steps (task 001's Decision 2)
//!
//! Calling `native_delete::delete_from_native_table` and
//! `native_write::append_to_native_table` sequentially would each acquire
//! their OWN lock, read their OWN manifest, and publish their OWN new
//! manifest -- two independent atomic renames, with a real window between
//! them (a crash, or even just a concurrent reader) where the old rows are
//! already gone but the new rows are not yet visible. This module instead:
//!
//! 1. Acquires `native_write::lock_table_for_write` ONCE, for the whole
//!    statement.
//! 2. Reads the manifest ONCE (`native_manifest::read_manifest`).
//! 3. Calls `native_delete::identify_matching_rows(dir, &existing,
//!    predicate, materialize_rows: true)` ONCE to get BOTH the
//!    tombstone-candidate (segment, position) pairs AND the matched rows'
//!    CURRENT column values (`SegmentMatch::rows`) to evaluate the SET
//!    assignments against.
//! 4. Evaluates every assignment's bound value expression against each
//!    matched row's ORIGINAL (pre-update) values (see
//!    [`assemble_updated_batch`]) to build new-row batches.
//! 5. Calls `native_delete::apply_deletions` for the tombstoning half (old
//!    positions) and `native_write::write_append_segments` (the SAME
//!    non-publishing write core task 002 built, reused UNCHANGED -- schema/
//!    dictionary inheritance and validation come for free) for the
//!    recomputed-rows half, against the SAME already-read manifest.
//! 6. Folds both segment lists into ONE `Vec<Segment>`.
//! 7. Calls `native_write::publish_manifest_update` (task 002's single-file
//!    atomic-rename primitive) exactly ONCE.
//! 8. Releases the lock (the `TableWriteLock` guard's `Drop`).
//!
//! A reader can therefore only ever observe the fully-pre-update manifest
//! or the fully-post-update one -- never an intermediate state -- because
//! exactly one `rename(2)` publishes the whole change. See
//! `tests/native_update_tests.rs` for a live concurrent-reader test that
//! verifies this empirically rather than only by this design argument.
//!
//! # A real correctness gap `identify_matching_rows` alone does NOT close
//! for UPDATE (found and fixed in this task)
//!
//! `identify_matching_rows` deliberately does not consult
//! `Segment::deleted_rows` (see its own doc: this is what makes DELETE's
//! `apply_deletions` union idempotent with zero extra logic, since
//! re-matching an already-deleted position is a harmless no-op there).
//! UPDATE is different: a match means "read this row's CURRENT value and
//! write a BRAND-NEW live row from it," so a match that is ALREADY
//! tombstoned (e.g. a second UPDATE, or an UPDATE after a DELETE, whose
//! predicate happens to still cover an already-removed row) must NOT be
//! resurrected as a new row. [`live_matched_rows`] filters each segment's
//! materialized matches down to only the positions NOT already present in
//! that segment's OWN `deleted_rows` before any SET expression is ever
//! evaluated against them. Without this filter, two overlapping UPDATEs
//! (or an UPDATE following a DELETE) touching the same rows would silently
//! duplicate them -- exactly the kind of subtle bug this program has
//! repeatedly found in partial/multi-step execution paths, and exactly why
//! this task's own acceptance criteria singles out "a second UPDATE that
//! overlaps a first" as a required adversarial test (`tests/
//! native_update_tests.rs`'s `overlapping_sequential_updates_...` test
//! fails without this filter and passes with it).

use crate::error::{QueryError, Result};
use crate::physical::operators::evaluate_expr;
use crate::physical::RecordBatchStream;
use crate::planner::Expr;
use crate::storage::native_delete::{self, SegmentMatch};
use crate::storage::native_manifest::{self, Segment};
use crate::storage::native_write::{self, NativeWriteOptions};
use arrow::array::{ArrayRef, BooleanArray};
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;

/// What [`update_native_table`] produced.
#[derive(Debug, Clone)]
pub struct NativeUpdateResult {
    /// Stable table identity (UUID v4) — unchanged by an UPDATE.
    pub table_id: String,
    /// The snapshot version this UPDATE committed. Equal to the PRE-update
    /// version when the predicate matched zero LIVE rows (a legitimate
    /// no-op, not an error — either the predicate matched nothing at all,
    /// or every matched row was already tombstoned by a prior DELETE/
    /// UPDATE).
    pub version: u64,
    /// Rows actually recomputed and rewritten — the NET count of LIVE
    /// matched rows, NOT the gross number of physical rows the predicate
    /// matched (which may include already-tombstoned rows the predicate
    /// happens to still cover — see the module doc's "real correctness
    /// gap" section). A predicate that matches only already-deleted rows
    /// reports `0`, not a nonzero count.
    pub rows_updated: u64,
    /// Old segments dropped entirely because every one of their rows
    /// became tombstoned by this UPDATE (task 001's Decision 3, reused via
    /// `native_delete::apply_deletions` unchanged).
    pub segments_dropped: usize,
    /// New segments written to hold the recomputed rows.
    pub segments_added: usize,
    /// The table's TOTAL LOGICAL (post-update, visible) row count — always
    /// equal to the PRE-update logical row count for a real update (an
    /// UPDATE never changes how many rows are live, only their values),
    /// computed honestly from the published manifest rather than assumed
    /// equal.
    pub total_rows: u64,
    /// The table's TOTAL segment count after this UPDATE.
    pub total_segments: usize,
}

/// Update every LIVE row of the native table at `table_dir` matching
/// `predicate` (`None` = every row): evaluate `assignments` (each a bound
/// `(column_name, value_expr)` pair — `value_expr` is evaluated against
/// that row's OTHER current column values, exactly like an ordinary
/// projection evaluates several output expressions against one input row)
/// against each matched row's CURRENT values, and publish the recomputed
/// rows as the table's new state for those rows — all as ONE atomically-
/// published operation. See the module doc for the exact composition and
/// why it must not be two sequential self-publishing calls.
///
/// `table_dir` MUST already be a native table directory (a clean
/// `QueryError::Storage` from `native_manifest::read_manifest` otherwise —
/// UPDATE never creates a table).
///
/// **Two distinct no-op cases**, both leave the manifest byte-for-byte
/// untouched and never bump the version — mirrors `native_delete::
/// delete_from_native_table`'s own precedent:
/// 1. The predicate matches zero PHYSICAL rows.
/// 2. The predicate matches rows, but EVERY one was already tombstoned by
///    a prior DELETE/UPDATE (see the module doc's "real correctness gap"
///    section) — nothing LIVE to recompute.
///
/// # On error
///
/// A lock-contention failure or a missing/corrupt manifest leaves
/// `table_dir` completely untouched. `identify_matching_rows` never writes
/// anything (a pure read), so any error there also leaves the table
/// untouched. A SET expression that fails to evaluate (e.g. a type error)
/// or a recomputed value that cannot be cast to the target's declared
/// schema (`write_append_segments`'s own validation — a clean, named
/// `QueryError::Type`, never silent coercion) leaves the table untouched
/// too: nothing is published until every recomputed batch has been
/// successfully written as a new segment file AND the single final
/// `publish_manifest_update` call succeeds.
pub async fn update_native_table(
    table_dir: impl AsRef<Path>,
    predicate: Option<&Expr>,
    assignments: &[(String, Expr)],
) -> Result<NativeUpdateResult> {
    let table_dir = table_dir.as_ref().to_path_buf();
    if assignments.is_empty() {
        return Err(QueryError::InvalidArgument(
            "UPDATE: no SET assignments given".to_string(),
        ));
    }

    // Acquired ONCE, held for the ENTIRE read-identify-evaluate-write-
    // publish span below — task 001's Decision 2/5, task 002's own
    // `lock_table_for_write` doc.
    let _lock = native_write::lock_table_for_write(&table_dir)?;
    let existing = native_manifest::read_manifest(&table_dir)?;
    let target_schema = existing.arrow_schema();
    let resolved_assignments = resolve_assignment_indices(&target_schema, assignments)?;

    let unchanged_result = || NativeUpdateResult {
        table_id: existing.table_id.clone(),
        version: existing.snapshot.version,
        rows_updated: 0,
        segments_dropped: 0,
        segments_added: 0,
        total_rows: existing.segments.iter().map(|s| s.live_row_count()).sum(),
        total_segments: existing.segments.len(),
    };

    // materialize_rows: true -- task 004's own need (task 003's Outcome
    // names this exact call shape): matched rows' CURRENT column values,
    // not just their positions.
    let matches = native_delete::identify_matching_rows(&table_dir, &existing, predicate, true)?;
    if matches.is_empty() {
        return Ok(unchanged_result());
    }

    let by_id: HashMap<u32, &Segment> = existing.segments.iter().map(|s| (s.id, s)).collect();

    // Evaluate every SET expression against each matched row's PRE-update
    // values and assemble the new rows -- per segment, so a segment
    // boundary never mixes data from two different old segments into one
    // evaluation. Rows already tombstoned by a prior mutation are excluded
    // here (see the module doc) BEFORE any SET expression ever sees them.
    let mut new_batches: Vec<RecordBatch> = Vec::new();
    let mut rows_updated: u64 = 0;
    for m in &matches.per_segment {
        let seg = by_id.get(&m.segment_id).ok_or_else(|| {
            QueryError::Execution(format!(
                "internal: UPDATE matched segment {} not present in the manifest just read",
                m.segment_id
            ))
        })?;
        if let Some(live_rows) = live_matched_rows(seg, m)? {
            rows_updated += live_rows.num_rows() as u64;
            new_batches.push(assemble_updated_batch(
                &live_rows,
                &target_schema,
                &resolved_assignments,
            )?);
        }
    }

    if rows_updated == 0 {
        // Every matched row was already dead -- mirrors
        // `delete_from_native_table`'s own "fully redundant repeat"
        // no-op case: `apply_deletions` on these positions would be a
        // pure no-op union (nothing NEW to tombstone) and there is
        // nothing new to write.
        return Ok(unchanged_result());
    }

    // Tombstone the OLD rows. `apply_deletions` (task 003's own function,
    // reused completely UNCHANGED) operates on the FULL, unfiltered match
    // set -- unioning an already-dead position into `deleted_rows` is
    // harmlessly idempotent, so passing `matches` as-is here is correct
    // even though `live_matched_rows` above already excluded those same
    // already-dead positions from the NEW rows.
    let tombstoned_segments = native_delete::apply_deletions(&existing, &matches);
    let segments_dropped = existing.segments.len() - tombstoned_segments.len();

    // Write the recomputed rows as new segment(s) against the SAME
    // already-read manifest (task 002's own non-publishing write core,
    // reused completely UNCHANGED — schema/dictionary inheritance, next
    // segment id continuation, and by-position type validation all come
    // for free from this single call).
    let new_rows_stream: RecordBatchStream =
        Box::pin(futures::stream::iter(new_batches.into_iter().map(Ok)));
    let new_segments = native_write::write_append_segments(
        new_rows_stream,
        &existing,
        &table_dir,
        NativeWriteOptions::default(),
    )
    .await?;
    let segments_added = new_segments.len();

    // ONE combined segment list -- tombstoned-old (with any wholly-dead
    // segment already dropped) PLUS the newly written replacements --
    // published exactly ONCE, still under the SAME lock acquired above.
    // This is the concrete mechanism that makes the whole UPDATE one
    // atomically-published operation (task 001's Decision 2): no reader
    // can ever observe a state where the old rows are gone but the new
    // ones are not yet visible, or vice versa, because there is only ever
    // one `rename(2)` for this whole statement.
    let mut all_segments = tombstoned_segments;
    all_segments.extend(new_segments);
    let total_segments = all_segments.len();

    let manifest = native_write::publish_manifest_update(
        &table_dir,
        target_schema.as_ref(),
        existing.table_id.clone(),
        existing.snapshot.version + 1,
        all_segments,
        native_write::now_ms(),
    )?;

    let total_rows: u64 = manifest.segments.iter().map(|s| s.live_row_count()).sum();

    Ok(NativeUpdateResult {
        table_id: manifest.table_id.clone(),
        version: manifest.snapshot.version,
        rows_updated,
        segments_dropped,
        segments_added,
        total_rows,
        total_segments,
    })
}

/// Resolve each `(column_name, value_expr)` assignment's target column NAME
/// into its INDEX within `target_schema` — exact match first (this
/// codebase's own convention, matching `arrow::datatypes::Schema::
/// index_of`'s case-sensitive lookup), falling back to a case-insensitive
/// scan for robustness. A name that resolves to neither is a clean, named
/// `QueryError::ColumnNotFound` (the binder already validates this at bind
/// time via `Binder::bind_update` — this is defense in depth for any other
/// caller of this function, not the primary place this is expected to
/// fire).
fn resolve_assignment_indices(
    target_schema: &SchemaRef,
    assignments: &[(String, Expr)],
) -> Result<Vec<(usize, Expr)>> {
    assignments
        .iter()
        .map(|(name, expr)| {
            let idx = target_schema.index_of(name).or_else(|_| {
                target_schema
                    .fields()
                    .iter()
                    .position(|f| f.name().eq_ignore_ascii_case(name))
                    .ok_or_else(|| {
                        QueryError::ColumnNotFound(format!(
                            "UPDATE: column `{name}` not found in the target table's schema"
                        ))
                    })
            })?;
            Ok((idx, expr.clone()))
        })
        .collect()
}

/// Filter a `SegmentMatch`'s materialized rows down to only those NOT
/// already tombstoned in `seg`'s OWN `deleted_rows` — see the module doc's
/// "real correctness gap" section for why this is required for UPDATE
/// (unlike DELETE, which needs no such filter). Returns `Ok(None)` if
/// every one of this segment's matches was already dead (the segment
/// contributes no new rows to this UPDATE).
fn live_matched_rows(seg: &Segment, m: &SegmentMatch) -> Result<Option<RecordBatch>> {
    let rows = m.rows.as_ref().ok_or_else(|| {
        QueryError::Execution(
            "internal: UPDATE requires identify_matching_rows(materialize_rows: true)".to_string(),
        )
    })?;
    if seg.deleted_rows.is_empty() {
        // The overwhelmingly common case (a table nothing has ever
        // deleted from, or this segment specifically) — zero allocation,
        // matching `native_table.rs::filter_deleted_rows`'s own
        // empty-deleted_rows fast path.
        return Ok(Some(rows.clone()));
    }
    let dead: BTreeSet<u32> = seg.deleted_rows.iter().copied().collect();
    let mask: BooleanArray = m
        .positions
        .iter()
        .map(|p| Some(!dead.contains(p)))
        .collect();
    let live = mask.true_count();
    if live == 0 {
        return Ok(None);
    }
    if live == mask.len() {
        return Ok(Some(rows.clone()));
    }
    let cols: Result<Vec<ArrayRef>> = rows
        .columns()
        .iter()
        .map(|c| arrow::compute::filter(c.as_ref(), &mask).map_err(Into::into))
        .collect();
    Ok(Some(RecordBatch::try_new(rows.schema(), cols?)?))
}

/// Evaluate every `(column_index, value_expr)` assignment against `rows`
/// (a segment's LIVE matched rows, in their pre-update state) and return a
/// new batch with those columns replaced. Self-referential assignments
/// (`SET x = x + 1`) are correct BY CONSTRUCTION: every assignment's value
/// expression is evaluated directly against `rows` — the matched rows'
/// ORIGINAL, unmodified column values — and only AFTER every assignment
/// has been evaluated are any columns actually replaced, exactly like an
/// ordinary projection evaluates several output expressions against one
/// input row before producing its output batch. No assignment ever reads
/// a column another assignment in the same SET list has already
/// overwritten, regardless of the order `assignments` lists them in.
///
/// If the SAME column is targeted more than once in one SET list, the
/// LAST assignment for it wins (applied after every value in the list has
/// already been computed against the identical pre-update `rows`) — a
/// reasonable, unsurprising default; this epic's own tests do not rely on
/// this edge case.
///
/// The returned batch's schema is derived from each column's ACTUAL
/// post-evaluation Arrow type (not blindly `target_schema`): an assigned
/// column commonly evaluates to a plain type (e.g. a literal string is
/// plain `Utf8` even when the target column is declared
/// `Dictionary(Int32, Utf8)`) — `write_append_segments`'s own by-position
/// casting against the target's REAL declared schema (called by this
/// module's only caller of this function) is what performs the "cast a
/// new plain value back into the target's existing dictionary encoding"
/// coercion; this function does not need to duplicate that logic.
fn assemble_updated_batch(
    rows: &RecordBatch,
    target_schema: &SchemaRef,
    assignments: &[(usize, Expr)],
) -> Result<RecordBatch> {
    // Evaluate ALL assignments first, strictly against `rows` -- before
    // any column is overwritten.
    let mut new_values: Vec<(usize, ArrayRef)> = Vec::with_capacity(assignments.len());
    for (idx, expr) in assignments {
        let arr = evaluate_expr(rows, expr)?;
        if arr.len() != rows.num_rows() {
            return Err(QueryError::Execution(format!(
                "UPDATE: SET expression for column `{}` produced {} value(s) for {} row(s)",
                target_schema.field(*idx).name(),
                arr.len(),
                rows.num_rows()
            )));
        }
        new_values.push((*idx, arr));
    }

    let mut columns = rows.columns().to_vec();
    for (idx, arr) in new_values {
        columns[idx] = arr;
    }

    let fields: Vec<Field> = target_schema
        .fields()
        .iter()
        .zip(columns.iter())
        .map(|(f, c)| Field::new(f.name(), c.data_type().clone(), true))
        .collect();
    let assembled_schema: SchemaRef = Arc::new(Schema::new(fields));
    Ok(RecordBatch::try_new(assembled_schema, columns)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{BinaryOp, Column, ScalarValue};
    use arrow::array::{DictionaryArray, Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType as DT, Field as ArrowField, Int32Type, Schema as ArrowSchema};
    use native_manifest::{
        compute_batch_stats, publish_table_dir, segment_full_path, staging_dir_for, write_manifest,
        NativeManifest,
    };
    use ordered_float::OrderedFloat;
    use std::sync::Arc as StdArc;

    fn schema() -> SchemaRef {
        StdArc::new(ArrowSchema::new(vec![
            ArrowField::new("id", DT::Int64, false),
            ArrowField::new("category", DT::Utf8, true),
            ArrowField::new("x", DT::Float64, true),
        ]))
    }

    fn batch(schema: &SchemaRef, ids: &[i64]) -> RecordBatch {
        let cats: Vec<String> = ids.iter().map(|i| format!("c{}", i % 3)).collect();
        let xs: Vec<f64> = ids.iter().map(|i| *i as f64).collect();
        RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(Int64Array::from(ids.to_vec())),
                StdArc::new(StringArray::from(cats)),
                StdArc::new(Float64Array::from(xs)),
            ],
        )
        .unwrap()
    }

    /// Mirrors `native_delete.rs`'s own `write_test_table` test helper.
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
                column_stats: compute_batch_stats(&b),
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

    fn id_le(n: i64) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(Expr::Column(Column::new("id"))),
            op: BinaryOp::LtEq,
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

    /// `x = x + <delta>` -- the canonical self-referential SET expression.
    fn x_plus(delta: f64) -> Expr {
        Expr::BinaryExpr {
            left: Box::new(Expr::Column(Column::new("x"))),
            op: BinaryOp::Add,
            right: Box::new(Expr::Literal(ScalarValue::Float64(OrderedFloat(delta)))),
        }
    }

    /// Reads the table's LOGICAL (post-deletion) state via the REAL
    /// `TableProvider::scan` path (`NativeTable`, task 003's own
    /// deletion-aware `scan()`) — deliberately NOT `native_write::
    /// read_back`, which reads every segment's RAW physical content
    /// (including tombstoned rows) and is documented as "NOT the
    /// production read path." Using the real read path here is itself
    /// part of this task's own adversarial-testing discipline: it is what
    /// actually proves an UPDATE's old (tombstoned) rows are invisible to
    /// a real reader, not just absent from `native_write::read_back`'s
    /// unfiltered dump.
    fn read_sorted(dir: &Path) -> Vec<(i64, String, f64)> {
        let table = crate::storage::NativeTable::try_new(dir).unwrap();
        let batches = crate::physical::operators::TableProvider::scan(&table, None).unwrap();
        let mut rows: Vec<(i64, String, f64)> = Vec::new();
        for b in &batches {
            let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let cats = string_col_values(b.column(1));
            let xs = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..b.num_rows() {
                rows.push((
                    ids.value(i),
                    cats[i].clone().unwrap_or_default(),
                    xs.value(i),
                ));
            }
        }
        rows.sort_by_key(|r| r.0);
        rows
    }

    /// Handles BOTH a plain `Utf8` column and a `Dictionary(Int32, Utf8)`
    /// one -- this module's write path may legitimately produce either,
    /// depending on cardinality, and several tests here deliberately do
    /// not care which.
    fn string_col_values(col: &ArrayRef) -> Vec<Option<String>> {
        if let Some(dict) = col.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
            let values = dict
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            dict.keys()
                .iter()
                .map(|k| k.map(|k| values.value(k as usize).to_string()))
                .collect()
        } else if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
            s.iter().map(|v| v.map(|s| s.to_string())).collect()
        } else {
            panic!(
                "expected a string or dictionary<string> column, got {:?}",
                col.data_type()
            );
        }
    }

    // ---------- self-referential SET expression ----------

    #[tokio::test]
    async fn self_referential_set_expression_reads_pre_update_values() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3, 4, 5]]);

        // SET x = x + 100 WHERE id <= 3 -- must read each row's OWN
        // pre-update x (== id as f64 per `batch()`), never a partially
        // updated value.
        let result = update_native_table(
            dir.path(),
            Some(&id_le(3)),
            &[("x".to_string(), x_plus(100.0))],
        )
        .await
        .unwrap();
        assert_eq!(result.rows_updated, 3);
        assert_eq!(result.total_rows, 5, "UPDATE never changes row count");
        assert_eq!(result.version, 2);

        let rows = read_sorted(dir.path());
        let expected: Vec<(i64, f64)> =
            vec![(1, 101.0), (2, 102.0), (3, 103.0), (4, 4.0), (5, 5.0)];
        for ((id, _cat, x), (exp_id, exp_x)) in rows.iter().zip(expected.iter()) {
            assert_eq!(id, exp_id);
            assert_eq!(x, exp_x, "row {id}: expected x={exp_x}, got {x}");
        }
    }

    // ---------- zero-match UPDATE ----------

    #[tokio::test]
    async fn update_matching_zero_rows_is_a_clean_no_op() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3]]);
        let before = native_manifest::read_manifest(dir.path()).unwrap();

        let result = update_native_table(
            dir.path(),
            Some(&id_eq(999)),
            &[("x".to_string(), x_plus(1.0))],
        )
        .await
        .unwrap();
        assert_eq!(result.rows_updated, 0);
        assert_eq!(result.version, 1, "no version bump for a no-op");
        assert_eq!(result.total_rows, 3);

        let after = native_manifest::read_manifest(dir.path()).unwrap();
        assert_eq!(
            before, after,
            "the manifest must be byte-for-byte untouched"
        );
    }

    // ---------- all-rows UPDATE ----------

    #[tokio::test]
    async fn update_matching_all_rows_via_no_predicate() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2], vec![3, 4, 5]]);

        let result = update_native_table(dir.path(), None, &[("x".to_string(), x_plus(1000.0))])
            .await
            .unwrap();
        assert_eq!(result.rows_updated, 5);
        assert_eq!(result.total_rows, 5);
        assert_eq!(
            result.segments_dropped, 2,
            "both original segments become wholly tombstoned"
        );

        let rows = read_sorted(dir.path());
        assert_eq!(rows.len(), 5);
        for (id, _cat, x) in &rows {
            assert_eq!(*x, *id as f64 + 1000.0);
        }
    }

    // ---------- dictionary-encoded column round-trip ----------

    #[tokio::test]
    async fn update_on_a_dictionary_encoded_column_round_trips_correctly() {
        let tmp = tempfile::tempdir().unwrap();
        // `NativeWriteMode::Create` refuses a destination that already
        // exists at all (even an empty directory) -- write into a fresh
        // SUBDIRECTORY of the tempdir, mirroring `native_write.rs`'s own
        // tests (`let out = dir.path().join("t");`).
        let dir = tmp.path().join("t");
        // Built through the REAL write path (`native_write::write_batches`,
        // NOT this file's own hand-rolled `write_test_table`, which writes
        // raw Arrow IPC files directly and never exercises dictionary
        // coercion at all) so `category`'s dictionary-candidacy decision
        // actually runs. It cycles through only 3 distinct values
        // (c0/c1/c2) over many rows -- comfortably below the write path's
        // default dict_max_cardinality (4096), so it is written
        // Dictionary(Int32, Utf8).
        let ids: Vec<i64> = (1..=30).collect();
        let schema = schema();
        let b = batch(&schema, &ids);
        let stream: RecordBatchStream = Box::pin(futures::stream::iter(vec![Ok(b)]));
        native_write::write_batches(
            stream,
            schema.clone(),
            &dir,
            native_write::NativeWriteMode::Create,
        )
        .await
        .unwrap();

        let manifest_before = native_manifest::read_manifest(&dir).unwrap();
        let category_field = manifest_before
            .schema
            .iter()
            .find(|f| f.name == "category")
            .unwrap();
        assert!(
            matches!(category_field.to_arrow().data_type(), DT::Dictionary(_, _)),
            "precondition: `category` must actually be dictionary-encoded for this test to \
             mean anything"
        );

        // Leave most rows' `category` untouched (exercises the pass-
        // through path) but assign a BRAND NEW string value (not one of
        // the original c0/c1/c2 dictionary entries) to a subset --
        // exercises the "new plain value cast back into the target's
        // existing Dictionary encoding" coercion.
        let result = update_native_table(
            &dir,
            Some(&id_le(5)),
            &[(
                "category".to_string(),
                Expr::Literal(ScalarValue::Utf8("brand-new-value".to_string())),
            )],
        )
        .await
        .unwrap();
        assert_eq!(result.rows_updated, 5);

        // The manifest's declared type for `category` must NOT have
        // regressed to plain Utf8 -- this epic's own explicit requirement.
        let manifest_after = native_manifest::read_manifest(&dir).unwrap();
        let category_field_after = manifest_after
            .schema
            .iter()
            .find(|f| f.name == "category")
            .unwrap();
        assert!(
            matches!(
                category_field_after.to_arrow().data_type(),
                DT::Dictionary(_, _)
            ),
            "UPDATE must not regress a dictionary-encoded column back to plain Utf8"
        );

        let rows = read_sorted(&dir);
        assert_eq!(rows.len(), 30);
        for (id, cat, _x) in &rows {
            if *id <= 5 {
                assert_eq!(
                    cat, "brand-new-value",
                    "row {id} should have the new category"
                );
            } else {
                assert_eq!(
                    cat,
                    &format!("c{}", id % 3),
                    "row {id}'s untouched category must round-trip unchanged"
                );
            }
        }
    }

    // ---------- overlapping sequential UPDATEs ----------

    #[tokio::test]
    async fn overlapping_sequential_updates_never_duplicate_or_lose_rows() {
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<i64> = (1..=10).collect();
        write_test_table(dir.path(), &[ids]);

        // UPDATE 1: x += 1 for ids 1..=5.
        let r1 = update_native_table(
            dir.path(),
            Some(&id_le(5)),
            &[("x".to_string(), x_plus(1.0))],
        )
        .await
        .unwrap();
        assert_eq!(r1.rows_updated, 5);

        // UPDATE 2: x += 1 for ids 1..=8 -- OVERLAPS update 1 (ids 1..=5,
        // now living in a NEW segment with x already +1) and also reaches
        // ids 6..=8 (still in the ORIGINAL segment, untouched so far).
        // Without `live_matched_rows` filtering out the now-tombstoned
        // ORIGINAL positions for ids 1..=5, this would incorrectly
        // resurrect them a second time (once correctly via the new
        // segment, once wrongly via their dead original positions) --
        // this test fails loudly (duplicate ids / wrong values) if that
        // filter is missing or broken.
        let r2 = update_native_table(
            dir.path(),
            Some(&id_le(8)),
            &[("x".to_string(), x_plus(1.0))],
        )
        .await
        .unwrap();
        assert_eq!(
            r2.rows_updated, 8,
            "ids 1..=8 are all LIVE matches -- 5 from the new segment, 3 from the original"
        );
        assert_eq!(r2.total_rows, 10, "row count must never change");

        let rows = read_sorted(dir.path());
        assert_eq!(rows.len(), 10, "no id may appear more than once");
        let mut seen_ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
        seen_ids.dedup();
        assert_eq!(
            seen_ids.len(),
            10,
            "no duplicate ids after two overlapping UPDATEs"
        );

        for (id, _cat, x) in &rows {
            let expected = if *id <= 5 {
                *id as f64 + 2.0 // updated by BOTH statements
            } else if *id <= 8 {
                *id as f64 + 1.0 // updated by the SECOND statement only
            } else {
                *id as f64 // never matched
            };
            assert_eq!(*x, expected, "row {id}: expected x={expected}, got {x}");
        }
    }

    // ---------- UPDATE after a prior DELETE on the same rows ----------

    #[tokio::test]
    async fn update_after_a_prior_delete_does_not_resurrect_deleted_rows() {
        let dir = tempfile::tempdir().unwrap();
        let ids: Vec<i64> = (1..=5).collect();
        write_test_table(dir.path(), &[ids]);

        let del = native_delete::delete_from_native_table(dir.path(), Some(&id_le(2)))
            .await
            .unwrap(); // deletes ids 1,2
        assert_eq!(del.rows_deleted, 2);

        // An UPDATE whose predicate would (structurally) still cover the
        // now-deleted ids 1,2 must not resurrect them.
        let result = update_native_table(
            dir.path(),
            None, // every row -- including the two already-dead ones
            &[("x".to_string(), x_plus(1.0))],
        )
        .await
        .unwrap();
        assert_eq!(
            result.rows_updated, 3,
            "only the 3 LIVE rows (ids 3,4,5) may be updated"
        );
        assert_eq!(result.total_rows, 3);

        let rows = read_sorted(dir.path());
        let seen_ids: Vec<i64> = rows.iter().map(|r| r.0).collect();
        assert_eq!(
            seen_ids,
            vec![3, 4, 5],
            "deleted ids 1,2 must stay gone, never resurrected by the UPDATE"
        );
        for (id, _cat, x) in &rows {
            assert_eq!(*x, *id as f64 + 1.0);
        }
    }

    // ---------- multi-segment targeting ----------

    #[tokio::test]
    async fn update_targets_only_matching_rows_across_multiple_segments() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]]);

        let result = update_native_table(
            dir.path(),
            Some(&id_eq(5)),
            &[("x".to_string(), x_plus(500.0))],
        )
        .await
        .unwrap();
        assert_eq!(result.rows_updated, 1);
        assert_eq!(result.total_rows, 9);

        let rows = read_sorted(dir.path());
        assert_eq!(rows.len(), 9);
        for (id, _cat, x) in &rows {
            let expected = if *id == 5 { 505.0 } else { *id as f64 };
            assert_eq!(*x, expected, "row {id}");
        }
    }

    // ---------- lock / missing-table error paths ----------

    #[tokio::test]
    async fn update_holds_the_single_writer_lock_for_its_whole_span() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3]]);

        let _held = native_write::lock_table_for_write(dir.path()).unwrap();
        let err = update_native_table(
            dir.path(),
            Some(&id_eq(1)),
            &[("x".to_string(), x_plus(1.0))],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, QueryError::Storage(_)), "{err:?}");
    }

    #[tokio::test]
    async fn update_against_a_missing_table_is_a_clean_storage_error() {
        let dir = tempfile::tempdir().unwrap();
        let err = update_native_table(dir.path(), None, &[("x".to_string(), x_plus(1.0))])
            .await
            .unwrap_err();
        assert!(matches!(err, QueryError::Storage(_)), "{err:?}");
    }

    #[tokio::test]
    async fn update_with_no_assignments_is_a_clean_invalid_argument_error() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3]]);
        let err = update_native_table(dir.path(), None, &[])
            .await
            .unwrap_err();
        assert!(matches!(err, QueryError::InvalidArgument(_)), "{err:?}");
    }

    // ---------- unknown assignment target column ----------

    #[tokio::test]
    async fn update_naming_an_unknown_column_is_a_clean_column_not_found_error() {
        let dir = tempfile::tempdir().unwrap();
        write_test_table(dir.path(), &[vec![1, 2, 3]]);
        let err = update_native_table(
            dir.path(),
            None,
            &[("does_not_exist".to_string(), x_plus(1.0))],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, QueryError::ColumnNotFound(_)), "{err:?}");

        // Must leave the table completely untouched.
        let manifest = native_manifest::read_manifest(dir.path()).unwrap();
        assert_eq!(manifest.snapshot.version, 1);
    }
}
