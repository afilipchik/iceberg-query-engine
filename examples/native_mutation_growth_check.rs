//! Task 005 (native-tables-mutation epic) adversarial diagnostic: three
//! scenarios this task's own acceptance criteria name explicitly, all
//! against the REAL `native_write`/`native_delete`/`native_update` code
//! paths (never a synthetic/mocked model of them):
//!
//! 1. **Deletion vector growth, "many segments, each lightly touched"**
//!    (task 001's own named residual risk, Decision 1) -- NOT just one
//!    big DELETE against one large table (that shape is cheap by
//!    construction: task 003's empty-segment-drop rule and the
//!    per-segment `Vec<u32>` bound already handle it). Builds a table
//!    with hundreds of segments via many separate small Append calls
//!    (mirroring "many small INSERT ... VALUES" sessions), then runs ONE
//!    broad, shallow DELETE (~1% of every segment) and measures the
//!    manifest-size delta attributable SPECIFICALLY to `deleted_rows`
//!    (isolated from the pre-existing per-segment structural overhead —
//!    `column_stats` etc — by measuring the SAME table's manifest size
//!    immediately before and after the one DELETE).
//! 2. **Sequential-mutation segment/manifest growth** -- a long, REALISTIC
//!    mixed sequence of Append/Delete/Update operations against the same
//!    table (not just repeated inserts), with checkpointed measurements
//!    of manifest size, segment count, open file descriptors, and
//!    `scan()`/`statistics()` wall time as segment count grows into the
//!    hundreds -- answering whether growth is bounded-per-operation and
//!    whether a large segment count becomes a correctness/perf cliff.
//! 3. **The empty-segment-drop exception's measured effectiveness** --
//!    fully tombstones several whole segments via one exact-range DELETE
//!    each, confirms they are dropped from the manifest, and computes a
//!    REAL (not hand-waved) "bytes saved vs the exception not firing"
//!    number per dropped segment: the ACTUAL `Segment` value that would
//!    have been retained (deleted_rows filled solid) is serialized with
//!    the SAME `serde_json` the manifest itself uses, and its length is
//!    compared against the 0 bytes it actually costs once dropped.
//!
//! ```text
//! scripts/claude-safe-build.sh cargo build --release --example native_mutation_growth_check
//! scripts/claude-safe-build.sh ./target/release/examples/native_mutation_growth_check
//! ```
//!
//! Tunable via env: `QE_GROWTH_N_APPENDS` (default 400 — number of
//! pure-Append operations, each producing exactly one new segment),
//! `QE_GROWTH_ROWS_PER_SEG` (default 2000).

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use query_engine::physical::{RecordBatchStream, TableProvider};
use query_engine::planner::{BinaryOp, Expr, ScalarValue};
use query_engine::storage::native_manifest;
use query_engine::storage::native_update;
use query_engine::storage::native_write::{self, NativeWriteMode, NativeWriteOptions};
use query_engine::storage::{native_delete, NativeTable};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Float64, false),
    ]))
}

fn make_batch(start: i64, n: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n).collect();
    let vals: Vec<f64> = ids.iter().map(|&i| i as f64 * 1.5).collect();
    RecordBatch::try_new(
        schema(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Float64Array::from(vals)),
        ],
    )
    .expect("build synthetic batch")
}

fn stream_of(b: RecordBatch) -> RecordBatchStream {
    Box::pin(futures::stream::iter(vec![Ok(b)]))
}

fn id_range(lo: i64, hi: i64) -> Expr {
    Expr::column("id")
        .gt_eq(Expr::literal(ScalarValue::Int64(lo)))
        .and(Expr::column("id").lt(Expr::literal(ScalarValue::Int64(hi))))
}

fn manifest_len(dir: &Path) -> u64 {
    std::fs::metadata(native_manifest::manifest_path(dir))
        .map(|m| m.len())
        .unwrap_or(0)
}

fn open_fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd")
        .map(|d| d.count())
        .unwrap_or(0)
}

/// Wall time for opening the table fresh (mirrors a real query: nothing
/// cached from a prior mutation in this same process) and running both
/// `statistics()` (O(1) rollup read) and a full `scan()` (materializes
/// every active segment) -- the two operations this task's own acceptance
/// criteria name as the ones a large segment count could turn into a
/// cliff for.
fn timed_scan_and_stats(dir: &Path) -> (std::time::Duration, std::time::Duration, usize, usize) {
    let t0 = Instant::now();
    let table = NativeTable::try_new(dir).expect("open native table");
    let stats = table.statistics();
    let stats_elapsed = t0.elapsed();

    let t1 = Instant::now();
    let batches = table.scan(None).expect("scan()");
    let scan_elapsed = t1.elapsed();
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    (
        stats_elapsed,
        scan_elapsed,
        rows,
        stats.map(|s| s.row_count).unwrap_or(0),
    )
}

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    let n_appends: i64 = std::env::var("QE_GROWTH_N_APPENDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3000);
    let rows_per_seg: i64 = std::env::var("QE_GROWTH_ROWS_PER_SEG")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2000);

    let scratch = tempfile::tempdir().expect("tempdir");
    let dir = scratch.path().join("t");

    // Seed: a fresh table via Create, tiny first batch. Reserved "scratch"
    // id space [0, 1_000_000) is NEVER touched by the main append loop
    // (which starts at 1_000_000 and climbs) -- kept exclusively for the
    // periodic small DELETE/UPDATE ops below, so every append-created
    // segment's id range stays exact and predictable for the later
    // exact-range full-tombstone test.
    let seed = make_batch(0, 10);
    native_write::write_batches(stream_of(seed), schema(), &dir, NativeWriteMode::Create).await?;
    println!("seeded table at {:?} (fd_count={})", dir, open_fd_count());

    // ========================================================================
    // Scenario 2: sequential-mutation growth -- a REALISTIC mixed sequence
    // of Append/Delete/Update, not just repeated inserts.
    // ========================================================================
    println!(
        "\n=== Scenario 2: sequential mixed-mutation growth ({n_appends} appends x \
         {rows_per_seg} rows, interleaved small deletes/updates) ==="
    );
    let mut next_id: i64 = 1_000_000;
    let mut segment_ranges: Vec<(i64, i64)> = Vec::new(); // (lo, hi) per pure-append op
    let checkpoint_every = (n_appends / 12).max(1);
    let t_start = Instant::now();

    for i in 0..n_appends {
        // Every 7th op: a small DELETE against a handful of rows from an
        // ALREADY-APPENDED segment (rotating which one, and which rows
        // within it, so this does REAL incremental work each time rather
        // than redundantly re-matching -- exercising a genuine mixed
        // INSERT/DELETE/UPDATE sequence, not decorative no-ops).
        if i % 7 == 6 && !segment_ranges.is_empty() {
            let (lo, _) = segment_ranges[(i as usize / 7) % segment_ranges.len()];
            let pred = id_range(lo + 500, lo + 503);
            native_delete::delete_from_native_table(&dir, Some(&pred)).await?;
        } else if i % 11 == 10 && !segment_ranges.is_empty() {
            // Every 11th op: a small UPDATE, same rotating-real-rows idea.
            let (lo, _) = segment_ranges[(i as usize / 11) % segment_ranges.len()];
            let pred = id_range(lo + 700, lo + 703);
            let assignments = vec![(
                "val".to_string(),
                Expr::column("val").add(Expr::literal(ScalarValue::Float64(1.0.into()))),
            )];
            native_update::update_native_table(&dir, Some(&pred), &assignments).await?;
        } else {
            // Otherwise: a pure Append -- exactly one new segment
            // (rows_per_seg is far below the 1,000,000-row default flush
            // threshold, so `finish()` flushes it as exactly one segment).
            let lo = next_id;
            let hi = next_id + rows_per_seg;
            let b = make_batch(lo, rows_per_seg);
            native_write::append_to_native_table(stream_of(b), &dir, NativeWriteOptions::default())
                .await?;
            segment_ranges.push((lo, hi));
            next_id = hi;
        }

        if (i + 1) % checkpoint_every == 0 || i + 1 == n_appends {
            let manifest = native_manifest::read_manifest(&dir)?;
            let (stats_t, scan_t, scan_rows, stats_rows) = timed_scan_and_stats(&dir);
            println!(
                "  op {:>5}: segments={:>5} manifest_bytes={:>9} fd_count={:>4} \
                 elapsed={:>8.2?} statistics()={:>8.2?} scan()={:>8.2?} \
                 (scan_rows={scan_rows}, stats.row_count={stats_rows})",
                i + 1,
                manifest.segments.len(),
                manifest_len(&dir),
                open_fd_count(),
                t_start.elapsed(),
                stats_t,
                scan_t,
            );
        }
    }

    let manifest_before_bulk_delete = native_manifest::read_manifest(&dir)?;
    let size_before_bulk = manifest_len(&dir);
    println!(
        "\nAfter {n_appends} mixed ops: {} segments, manifest={} bytes, {} logical rows, \
         fd_count={}",
        manifest_before_bulk_delete.segments.len(),
        size_before_bulk,
        manifest_before_bulk_delete
            .segments
            .iter()
            .map(|s| s.live_row_count())
            .sum::<u64>(),
        open_fd_count(),
    );

    // ========================================================================
    // Scenario 1: deletion vector growth, "many segments, each lightly
    // touched" -- ONE broad, shallow DELETE hitting ~1% of every segment.
    // ========================================================================
    println!(
        "\n=== Scenario 1: broad shallow DELETE (~1% of every segment, {} segments) ===",
        manifest_before_bulk_delete.segments.len()
    );
    let shallow_pred = Expr::BinaryExpr {
        left: Box::new(Expr::column("id")),
        op: BinaryOp::Modulo,
        right: Box::new(Expr::literal(ScalarValue::Int64(100))),
    }
    .eq(Expr::literal(ScalarValue::Int64(0)));
    let t0 = Instant::now();
    let del_result = native_delete::delete_from_native_table(&dir, Some(&shallow_pred)).await?;
    let delete_elapsed = t0.elapsed();
    let manifest_after_bulk_delete = native_manifest::read_manifest(&dir)?;
    let size_after_bulk = manifest_len(&dir);

    let total_deleted_entries: usize = manifest_after_bulk_delete
        .segments
        .iter()
        .map(|s| s.deleted_rows.len())
        .sum();
    let bytes_per_entry = if total_deleted_entries > 0 {
        (size_after_bulk.saturating_sub(size_before_bulk)) as f64 / total_deleted_entries as f64
    } else {
        0.0
    };
    println!(
        "  DELETE matched (net) {} rows in {:?}",
        del_result.rows_deleted, delete_elapsed
    );
    println!(
        "  segments: before={} after={} (dropped={})",
        manifest_before_bulk_delete.segments.len(),
        manifest_after_bulk_delete.segments.len(),
        del_result.segments_dropped,
    );
    println!(
        "  manifest bytes: before={size_before_bulk} after={size_after_bulk} delta={} \
         ({total_deleted_entries} total deleted_rows entries -> ~{bytes_per_entry:.2} \
         bytes/entry)",
        size_after_bulk as i64 - size_before_bulk as i64
    );
    // Extrapolate to task 001's own literal worry ("1000 segments x
    // ~1,000,000 rows each x 1% deleted" = 10,000,000 deleted entries)
    // using the REAL, measured per-entry byte cost above -- a grounded
    // projection, not a fresh guess.
    let projected_10m = bytes_per_entry * 10_000_000.0;
    println!(
        "  extrapolated (measured bytes/entry x 10,000,000 entries, task 001's own literal \
         '1000 segments x 1M rows x 1% deleted' scenario): ~{:.1} MB",
        projected_10m / 1_000_000.0
    );

    // ========================================================================
    // Scenario 3: empty-segment-drop exception's MEASURED effectiveness.
    // Fully tombstone 5 whole, previously-untouched append-created
    // segments via exact-range DELETEs, confirm they vanish from the
    // manifest, and compute the REAL bytes that would have persisted had
    // the exception not fired (by serializing the actual retained-shape
    // Segment value with the same serde_json the manifest uses).
    // ========================================================================
    println!("\n=== Scenario 3: empty-segment-drop exception effectiveness ===");
    let manifest_pre_drop = native_manifest::read_manifest(&dir)?;
    let pick: Vec<(i64, i64)> = {
        let n = segment_ranges.len();
        [n / 10, n / 4, n / 2, (3 * n) / 4, n - 2]
            .iter()
            .map(|&idx| segment_ranges[idx.min(n - 1)])
            .collect()
    };
    let mut counterfactual_bytes_saved: i64 = 0;
    let mut actually_dropped = 0usize;
    for (lo, hi) in &pick {
        // Find this segment's CURRENT manifest entry before we delete it,
        // to build the "what if it had NOT been dropped" counterfactual:
        // the same Segment, but with deleted_rows filled solid instead of
        // the segment being removed from the list.
        let before_count = native_manifest::read_manifest(&dir)?.segments.len();
        let pred = id_range(*lo, *hi);
        let del = native_delete::delete_from_native_table(&dir, Some(&pred)).await?;
        let after_count = native_manifest::read_manifest(&dir)?.segments.len();
        let dropped_this_time = before_count - after_count;
        actually_dropped += dropped_this_time;

        // Locate the pre-delete segment covering this exact range from
        // manifest_pre_drop (captured before ANY of these 5 deletes ran)
        // to compute the counterfactual honestly against its true
        // pre-delete shape (row_count, column_stats, path, id all real).
        if let Some(seg) = manifest_pre_drop
            .segments
            .iter()
            .find(|s| s.column_stats.get("id").and_then(|cs| cs.min_i64) == Some(*lo))
            .cloned()
        {
            let mut hypothetical = seg.clone();
            hypothetical.deleted_rows = (0..seg.row_count as u32).collect();
            let hypothetical_json_len = serde_json::to_string(&hypothetical)
                .map(|s| s.len())
                .unwrap_or(0);
            counterfactual_bytes_saved += hypothetical_json_len as i64;
            println!(
                "  segment covering id in [{lo}, {hi}): dropped={} rows_deleted={} \
                 hypothetical-retained-entry-size={hypothetical_json_len} bytes (vs 0 actual)",
                dropped_this_time, del.rows_deleted,
            );
        } else {
            println!(
                "  segment covering id in [{lo}, {hi}): dropped={} rows_deleted={} \
                 (could not locate exact pre-delete segment for byte counterfactual)",
                dropped_this_time, del.rows_deleted,
            );
        }
    }
    println!(
        "  totals: {actually_dropped}/{} targeted segments actually dropped; measured bytes \
         that would have persisted WITHOUT the empty-segment-drop exception: {} bytes (~{:.1} \
         KB) -- actual cost with the exception: 0 bytes (entries fully removed)",
        pick.len(),
        counterfactual_bytes_saved,
        counterfactual_bytes_saved as f64 / 1000.0,
    );

    let manifest_final = native_manifest::read_manifest(&dir)?;
    println!(
        "\nFinal state: {} segments, manifest={} bytes, {} logical rows",
        manifest_final.segments.len(),
        manifest_len(&dir),
        manifest_final
            .segments
            .iter()
            .map(|s| s.live_row_count())
            .sum::<u64>(),
    );

    Ok(())
}
