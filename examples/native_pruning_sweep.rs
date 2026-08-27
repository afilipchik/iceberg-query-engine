//! Task 002 (native-table-pruning epic) broader validation: a wider sweep
//! of predicate shapes than task 001's own example
//! (`native_pruning_check.rs`) covered, against a REAL multi-segment
//! native table at SF=10 scale (`data/tpch-10gb-native`, lineitem = 58
//! segments / 60,000,000 rows, orders = 15 segments / 15,000,000 rows).
//!
//! For every predicate this prints, on stdout, a `RESULT <label> <count>`
//! line so a companion Python script
//! (`scripts/native_pruning_sweep_check.py`) can independently verify each
//! count against a fresh DuckDB oracle over the SAME source parquet
//! (`data/tpch-10gb`) -- an engine-independent ground truth, not just an
//! internal cross-check.
//!
//! Internally (engine-independent of DuckDB, but still a real A/B) each
//! predicate is run TWICE against the SAME underlying data through the
//! SAME SQL/FilterExec path, differing ONLY in which `TableProvider`
//! backs the table:
//!   - `..._native`   -- the real `NativeTable`, `scan_with_filter` prunes
//!     segments via `segment_might_match` before decoding them ("pruning
//!     on").
//!   - `..._unpruned` -- a `MemoryTable` built from that SAME native
//!     table's own unfiltered `scan(None)` (i.e. every segment already
//!     decoded, in full, once, up front) registered under a different
//!     name. `MemoryTable::scan_with_filter` is the trait default (`=
//!     scan`, ignores `filter`), so this leg can *never* prune -- the
//!     WHERE clause is enforced solely by the ordinary `FilterExec` sitting
//!     above the scan, exactly the "off" state pruning is supposed to be
//!     invisible against ("pruning off").
//! Both legs run through the literal same `ctx.sql(...)` call, same
//! optimizer, same `FilterExec` evaluator -- the ONLY thing that can
//! differ between them is whether `scan_with_filter` proved a segment
//! unsatisfiable and skipped it. A mismatch would mean pruning produced a
//! wrong answer.
//!
//! Run with `QE_DEBUG_NATIVE_PRUNING=1` to see each predicate's real
//! per-segment skip/scan trace (the `scanned=N skipped=M total=N+M`
//! summary line) directly on stderr -- the "traced, not inferred"
//! evidence this task's acceptance criteria ask for.

use query_engine::physical::operators::TableProvider;
use query_engine::storage::NativeTable;
use query_engine::{ExecutionConfig, ExecutionContext};
use std::sync::Arc;

const LINEITEM_DIR: &str = "data/tpch-10gb-native/lineitem";
const ORDERS_DIR: &str = "data/tpch-10gb-native/orders";

async fn scalar_count(ctx: &ExecutionContext, sql: &str) -> i64 {
    use arrow::array::Int64Array;
    let result = ctx.sql(sql).await.expect("query must succeed");
    assert_eq!(result.row_count, 1, "COUNT(*) must return exactly one row");
    result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT(*) result is Int64")
        .value(0)
}

/// Registers `<base>_native` (the real, prunable `NativeTable`) and
/// `<base>_unpruned` (a `MemoryTable` snapshot of the SAME data, which
/// structurally cannot prune) side by side.
fn register_pruned_and_unpruned(ctx: &mut ExecutionContext, dir: &str, base: &str) {
    // Two independent `NativeTable` opens of the same on-disk directory --
    // cheap (just re-reads the manifest), and keeps the two legs fully
    // decoupled (one is registered as-is for the pruned leg, the other is
    // fully materialized once for the unpruned leg).
    let native_provider = NativeTable::try_new(dir).expect("open native table (pruned leg)");
    let native_name = format!("{base}_native");
    ctx.register_table_provider(&native_name, Arc::new(native_provider));

    let baseline_provider = NativeTable::try_new(dir).expect("open native table (baseline leg)");
    let schema = TableProvider::schema(&baseline_provider);
    let batches =
        TableProvider::scan(&baseline_provider, None).expect("unfiltered scan for baseline");
    let unpruned_name = format!("{base}_unpruned");
    ctx.register_table(&unpruned_name, schema, batches);
}

/// Runs one predicate against both the pruned and unpruned legs, asserts
/// they agree exactly, and prints a `RESULT` line for the external DuckDB
/// cross-check.
async fn run_predicate(ctx: &ExecutionContext, base: &str, label: &str, predicate_sql: &str) {
    let native_sql = format!("SELECT COUNT(*) FROM {base}_native WHERE {predicate_sql}");
    let unpruned_sql = format!("SELECT COUNT(*) FROM {base}_unpruned WHERE {predicate_sql}");
    println!("\n--- {base}/{label}: {predicate_sql} ---");
    let pruned_count = scalar_count(ctx, &native_sql).await;
    let unpruned_count = scalar_count(ctx, &unpruned_sql).await;
    println!("pruned={pruned_count} unpruned={unpruned_count}");
    assert_eq!(
        pruned_count, unpruned_count,
        "pruning-on and pruning-off must agree exactly for {label}"
    );
    println!("RESULT {base}.{label} {pruned_count}");
}

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    if !std::path::Path::new(LINEITEM_DIR).exists() || !std::path::Path::new(ORDERS_DIR).exists() {
        eprintln!("skipping: {LINEITEM_DIR} / {ORDERS_DIR} not found (generate via scripts/native_bench_compare.py --write --source-dir data/tpch-10gb --native-dir data/tpch-10gb-native)");
        return Ok(());
    }

    // Generous budget: lineitem's unfiltered scan (60M rows, all columns)
    // is materialized once for the unpruned baseline leg.
    let config = ExecutionConfig::default().with_memory_limit(32 * 1024 * 1024 * 1024);
    let mut ctx = ExecutionContext::with_config(config);

    register_pruned_and_unpruned(&mut ctx, LINEITEM_DIR, "lineitem");
    register_pruned_and_unpruned(&mut ctx, ORDERS_DIR, "orders");

    // Sanity: unfiltered counts must match the manifest's own row counts.
    let li_total = scalar_count(&ctx, "SELECT COUNT(*) FROM lineitem_native").await;
    println!("lineitem unfiltered COUNT(*): {li_total}");
    assert_eq!(li_total, 60_000_000);
    let o_total = scalar_count(&ctx, "SELECT COUNT(*) FROM orders_native").await;
    println!("orders unfiltered COUNT(*): {o_total}");
    assert_eq!(o_total, 15_000_000);
    println!("RESULT lineitem.unfiltered {li_total}");
    println!("RESULT orders.unfiltered {o_total}");

    // 1. Single-column range predicate on a stat-bearing, segment-correlated
    //    column (l_orderkey is disjoint per segment by construction of the
    //    write path -- an ideal pruning candidate).
    run_predicate(&ctx, "lineitem", "range", "l_orderkey <= 300000").await;

    // 2. Equality predicate, same column.
    run_predicate(&ctx, "lineitem", "equality", "l_orderkey = 14500000").await;

    // 3. Multi-column AND spanning two DIFFERENT column families (i64
    //    l_orderkey range AND f64 l_discount range) -- task 001's own AND
    //    coverage used two comparisons on the SAME column; this is a
    //    genuinely different shape.
    run_predicate(
        &ctx,
        "lineitem",
        "multi_column_and",
        "l_orderkey BETWEEN 5000000 AND 5500000 AND l_discount > 0.095",
    )
    .await;

    // 4. Predicate on a column with NO stats (a string column --
    //    `ColumnStats` never populates min/max for Utf8/Dictionary
    //    columns). Every segment must be scanned; the pruned/unpruned
    //    counts must still agree exactly.
    run_predicate(&ctx, "lineitem", "no_stats_string", "l_shipmode = 'AIR'").await;

    // 5. A predicate whose range spans MULTIPLE segments (not entirely
    //    inside one, not entirely outside all) -- some segments skipped,
    //    some scanned, in the SAME query.
    run_predicate(
        &ctx,
        "lineitem",
        "spans_multiple_segments",
        "l_orderkey BETWEEN 300000 AND 2000000",
    )
    .await;

    // 6. A date-range predicate in the exact shape CLAUDE.md's own
    //    "Current limitations" section names for Q4/Q12/Q13
    //    (`l_shipdate`/`l_commitdate`/`l_receiptdate` range filters). This
    //    is the DIRECT evidence for whether pruning can help those
    //    queries at all: if `l_shipdate` is NOT correlated with segment
    //    write order (unlike `l_orderkey`), every segment's min/max spans
    //    nearly the whole table and this predicate should skip ~nothing.
    run_predicate(
        &ctx,
        "lineitem",
        "date_range_q4_shape",
        "l_shipdate BETWEEN DATE '1996-03-01' AND DATE '1996-03-31'",
    )
    .await;

    // 7/8. orders leg -- confirms the mechanism generalizes to a second
    // table, not just lineitem.
    run_predicate(&ctx, "orders", "range", "o_orderkey <= 500000").await;
    run_predicate(&ctx, "orders", "equality", "o_orderkey = 14000000").await;

    println!("\nPASS: every predicate's pruning-on result matches its pruning-off result exactly.");
    println!(
        "(see stderr with QE_DEBUG_NATIVE_PRUNING=1 for real per-segment skip/scan traces; \
         see RESULT lines above for the DuckDB cross-check via scripts/native_pruning_sweep_check.py)"
    );
    Ok(())
}
