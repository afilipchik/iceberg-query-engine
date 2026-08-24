//! Task 005 (native-tables-mutation epic) adversarial diagnostic: the
//! LAST named sub-scenario from this task's own Technical Details --
//! phase 1 task 006's read-side admission-control mechanism
//! (`NativeTable::check_scan_budget`, gating on `statistics().
//! total_byte_size`) must still refuse CLEANLY (not OOM) for a table that
//! has been HEAVILY DELETED FROM, since task 001/003 both argued
//! (analytically) that deletion never shrinks a segment's on-disk bytes,
//! so the existing formula stays a valid, sufficient upper bound with
//! zero code changes. This task's own charter is to confirm that
//! ADVERSARIALLY, against the REAL 60,000,000-row `lineitem` native table
//! fixture (`data/tpch-10gb-native/lineitem`, 5.3GB on disk, 58 segments)
//! already used by phase 1 task 006's own OOM reproduction -- not a fresh
//! synthetic table, and not just trusting the analysis.
//!
//! Segment `.arrow` files are HARD-LINKED (not copied) into a scratch
//! directory -- DELETE never writes to a segment file (only edits/
//! rewrites `_manifest.json`), so sharing the physical bytes with the
//! committed fixture is safe and avoids a slow multi-GB copy.
//!
//! ```text
//! scripts/claude-safe-build.sh cargo build --release --example native_delete_then_scan_budget_check
//! scripts/claude-safe-build.sh ./target/release/examples/native_delete_then_scan_budget_check
//! SAFE_BUILD_MEM=1500M scripts/claude-safe-build.sh ./target/release/examples/native_delete_then_scan_budget_check
//! ```

use query_engine::physical::TableProvider;
use query_engine::planner::{Expr, ScalarValue};
use query_engine::storage::native_delete;
use query_engine::storage::native_manifest;
use query_engine::storage::NativeTable;
use std::path::Path;

const SOURCE: &str = "data/tpch-10gb-native/lineitem";

fn hardlink_table(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let file_name = entry.file_name();
        std::fs::hard_link(entry.path(), dst.join(file_name))?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    if !Path::new(SOURCE).exists() {
        eprintln!(
            "skipping: {SOURCE} not found (needs the committed SF=10 native lineitem fixture)"
        );
        return Ok(());
    }

    // Hard-linking requires the SAME filesystem as the source -- a
    // `tempfile::tempdir()` (typically `/tmp`) can be a different mount
    // than this repo's own `data/` tree, so the scratch dir lives under
    // `.scratch/` (this repo's own convention for throwaway, gitignored
    // work) instead, guaranteed same-filesystem as `data/`.
    let scratch_root = Path::new(".scratch/native_delete_budget_check");
    let _ = std::fs::remove_dir_all(scratch_root);
    let dir = scratch_root.join("lineitem");
    hardlink_table(Path::new(SOURCE), &dir).expect("hard-link fixture segments into scratch dir");

    let before = native_manifest::read_manifest(&dir)?;
    let before_bytes: u64 = before.segments.iter().map(|s| s.byte_size).sum();
    println!(
        "before DELETE: {} rows, {} segments, {} total on-disk bytes ({:.2} GB)",
        before.snapshot.row_count,
        before.segments.len(),
        before_bytes,
        before_bytes as f64 / 1e9,
    );

    // Delete a large, real chunk of the table. This generator's
    // `l_linenumber` ranges 1-60 (NOT the TPC-H-spec 1-7 -- confirmed
    // against the manifest's own `table_stats` before writing this
    // predicate), so `> 6` looks like it should match ~54/60 = 90% under a
    // UNIFORM assumption -- it does NOT (see the printed actual count
    // below): `l_linenumber` is an item's position WITHIN an order, so its
    // distribution across all rows skews heavily toward small values
    // (every order has a "line 1", few have a "line 60"), a real
    // measured-not-assumed correction worth recording. Whatever the exact
    // fraction, the property under test does not depend on hitting a
    // specific percentage: does the physical on-disk footprint (below,
    // asserted unchanged) stay what the budget check reads, regardless of
    // how large a real DELETE's logical effect is.
    let pred = Expr::column("l_linenumber").gt(Expr::literal(ScalarValue::Int64(6)));
    let t0 = std::time::Instant::now();
    let del = native_delete::delete_from_native_table(&dir, Some(&pred)).await?;
    println!(
        "DELETE l_linenumber > 6: {} rows deleted (net), {} segments dropped, in {:?}",
        del.rows_deleted,
        del.segments_dropped,
        t0.elapsed()
    );

    let after = native_manifest::read_manifest(&dir)?;
    let after_bytes: u64 = after.segments.iter().map(|s| s.byte_size).sum();
    let logical_rows: u64 = after.segments.iter().map(|s| s.live_row_count()).sum();
    println!(
        "after DELETE: {} segments, {} total on-disk bytes ({:.2} GB, delta={} -- must be \
         UNCHANGED, deletion never shrinks segment bytes), {} LOGICAL (post-delete) rows",
        after.segments.len(),
        after_bytes,
        after_bytes as f64 / 1e9,
        after_bytes as i64 - before_bytes as i64,
        logical_rows,
    );
    assert_eq!(
        after_bytes, before_bytes,
        "a segment's on-disk byte_size must be UNCHANGED by DELETE (task 001/003's own \
         invariant) -- if this fails, the memory-budget formula's whole premise is broken"
    );

    // Now the adversarial check: open with a budget CLEARLY too small for
    // the (unchanged) physical footprint but that WOULD comfortably fit
    // the tiny logical (post-delete) row count if the check were (wrongly)
    // reading the post-delete size instead.
    let tight_budget = 200_000_000u64; // 200MB -- far below the ~5.3GB physical footprint
    let table_tight = NativeTable::try_new(&dir)?.with_memory_budget(Some(tight_budget));
    let refused = table_tight.scan(None);
    match &refused {
        Err(e) => println!(
            "[PASS] scan() under a {tight_budget}-byte budget refuses CLEANLY (not a crash): \
             {e}"
        ),
        Ok(batches) => {
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            panic!(
                "[FAIL] scan() should have refused under a {tight_budget}-byte budget against a \
                 {after_bytes}-byte table, but returned {rows} rows"
            );
        }
    }

    // And the converse: a budget that comfortably covers the UNCHANGED
    // physical footprint succeeds and returns the correct LOGICAL row
    // count (post-delete), proving the check isn't just refusing
    // everything.
    let generous_budget = after_bytes + 1_000_000_000;
    let table_generous = NativeTable::try_new(&dir)?.with_memory_budget(Some(generous_budget));
    let t1 = std::time::Instant::now();
    let batches = table_generous.scan(None)?;
    let scanned_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    println!(
        "[PASS] scan() under a generous {generous_budget}-byte budget succeeds in {:?}: \
         {scanned_rows} rows (expected LOGICAL row count {logical_rows})",
        t1.elapsed()
    );
    assert_eq!(
        scanned_rows as u64, logical_rows,
        "scanned row count must equal the LOGICAL (post-delete) count"
    );

    println!("\n=== OVERALL: PASS ===");
    Ok(())
}
