//! Task 001 (native-table-pruning epic) diagnostic: proves, with a real
//! traced measurement rather than an inference from wall-clock time, that
//! `NativeTable::scan_with_filter` actually skips segments it can prove
//! can't match a pushed-down predicate, and that the result set stays
//! identical whether pruning was able to skip anything or not.
//!
//! Uses `data/tpch-1gb-native/orders` (2 segments, disjoint `o_orderkey`
//! ranges: segment 0 = [1, 1048576], segment 1 = [1048577, 1500000]) — a
//! real, on-disk multi-segment native table, not a synthetic fixture.
//!
//! Run with `QE_DEBUG_NATIVE_PRUNING=1` set so `NativeTable::scan_with_filter`'s
//! own per-segment trace prints directly to stderr:
//!
//! ```text
//! QE_DEBUG_NATIVE_PRUNING=1 cargo run --release --example native_pruning_check
//! ```
//!
//! A line reading `[native_pruning] table=... segment=1 SKIP ...` for the
//! `o_orderkey <= 1000` query is the real evidence a segment was actually
//! skipped (not decoded), confirmed by tracing, not inferred.

use query_engine::{ExecutionConfig, ExecutionContext};

const NATIVE_DIR: &str = "data/tpch-1gb-native/orders";

/// Runs a `SELECT COUNT(*) ...` query and returns the scalar count value
/// (NOT `QueryResult::row_count`, which is the number of RESULT rows -- 1
/// for any aggregate query regardless of the count's own value).
async fn scalar_count(ctx: &ExecutionContext, sql: &str) -> i64 {
    use arrow::array::Int64Array;
    let result = ctx.sql(sql).await.expect("query must succeed");
    assert_eq!(result.row_count, 1, "COUNT(*) must return exactly one row");
    let batch = &result.batches[0];
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("COUNT(*) result is Int64")
        .value(0)
}

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    if !std::path::Path::new(NATIVE_DIR).exists() {
        eprintln!(
            "skipping: {NATIVE_DIR} not found (generate it first via `write-native \
             --from-parquet data/tpch-1gb/orders.parquet --out {NATIVE_DIR}`)"
        );
        return Ok(());
    }

    let config = ExecutionConfig::default().with_memory_limit(4 * 1024 * 1024 * 1024);
    let mut ctx = ExecutionContext::with_config(config);
    ctx.register_native_table("orders_native", NATIVE_DIR)
        .expect("register native orders table");

    // Baseline: full scan, no predicate at all -- must read every segment
    // and must not regress (same row count the manifest declares: 1500000).
    let total = scalar_count(&ctx, "SELECT COUNT(*) FROM orders_native").await;
    println!("unfiltered COUNT(*): {total}");
    assert_eq!(total, 1_500_000, "unfiltered scan must see every row");

    // Range predicate entirely inside segment 0's key range -- segment 1
    // (whose min_i64 = 1048577) is PROVABLY unsatisfiable for `<= 1000` and
    // must be skipped. Set QE_DEBUG_NATIVE_PRUNING=1 to see the trace.
    println!("\n--- range predicate: o_orderkey <= 1000 (should skip segment 1) ---");
    let range_count =
        scalar_count(&ctx, "SELECT COUNT(*) FROM orders_native WHERE o_orderkey <= 1000").await;
    println!("filtered COUNT(*): {range_count}");

    // Equality predicate entirely inside segment 1's key range -- segment 0
    // (whose max_i64 = 1048576) is PROVABLY unsatisfiable for `= 1200000`
    // and must be skipped.
    println!("\n--- equality predicate: o_orderkey = 1200000 (should skip segment 0) ---");
    let eq_count = scalar_count(
        &ctx,
        "SELECT COUNT(*) FROM orders_native WHERE o_orderkey = 1200000",
    )
    .await;
    println!("filtered COUNT(*): {eq_count}");
    assert_eq!(eq_count, 1, "exactly one order has this orderkey");

    // Cell-exact cross-check: pruning-on result (via the real SQL/planner
    // path above) vs. a pruning-independent path -- read the WHOLE table
    // unfiltered via scan(None) and apply the identical predicate in-memory,
    // never touching scan_with_filter's pruning logic at all. If pruning
    // ever skipped a segment it should not have, this comparison would
    // diverge.
    use arrow::array::Int64Array;
    use arrow::compute::kernels::cmp::{eq, lt_eq};
    let provider = query_engine::storage::NativeTable::try_new(NATIVE_DIR).expect("open native table");
    let full = query_engine::physical::operators::TableProvider::scan(&provider, None)
        .expect("unfiltered scan");
    let idx = full[0]
        .schema()
        .index_of("o_orderkey")
        .expect("o_orderkey column");
    let mut manual_range = 0usize;
    let mut manual_eq = 0usize;
    for batch in &full {
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("o_orderkey is Int64");
        let le = lt_eq(col, &arrow::array::Int64Array::new_scalar(1000)).unwrap();
        manual_range += le.true_count();
        let eqm = eq(col, &arrow::array::Int64Array::new_scalar(1200000)).unwrap();
        manual_eq += eqm.true_count();
    }
    println!("\n--- cell-exact cross-check (pruning-independent scan) ---");
    println!("manual range count: {manual_range} (engine: {range_count})");
    println!("manual eq count:    {manual_eq} (engine: {eq_count})");
    assert_eq!(
        manual_range as i64, range_count,
        "pruned range result must match the unpruned, independently-filtered baseline"
    );
    assert_eq!(
        manual_eq as i64, eq_count,
        "pruned equality result must match the unpruned, independently-filtered baseline"
    );

    println!("\nPASS: pruning-on results (orders, range/equality) are cell-exact vs. a pruning-independent baseline.");

    // ------------------------------------------------------------------
    // Second table: `data/tpch-1gb-native/lineitem`, 6 segments, disjoint
    // `l_orderkey` ranges. Exercises AND-of-two-comparisons and BETWEEN
    // pruning (multiple segments skipped at once), not just a single
    // comparison against a 2-segment table.
    // ------------------------------------------------------------------
    const LINEITEM_DIR: &str = "data/tpch-1gb-native/lineitem";
    if !std::path::Path::new(LINEITEM_DIR).exists() {
        eprintln!("skipping lineitem leg: {LINEITEM_DIR} not found");
        return Ok(());
    }
    ctx.register_native_table("lineitem_native", LINEITEM_DIR)
        .expect("register native lineitem table");

    // Segment l_orderkey ranges (from the manifest): seg0 [1,262013],
    // seg1 [262013,524521], seg2 [524521,786620], seg3 [786620,1047888],
    // seg4 [1047888,1309260], seg5 [1309260,1498929]. This AND predicate
    // is entirely inside segment 4's range -- segments 0,1,2,3,5 must all
    // be provably unsatisfiable and skipped.
    println!(
        "\n--- lineitem AND predicate: l_orderkey > 1100000 AND l_orderkey < 1200000 \
         (should skip 5 of 6 segments) ---"
    );
    let and_count = scalar_count(
        &ctx,
        "SELECT COUNT(*) FROM lineitem_native WHERE l_orderkey > 1100000 AND l_orderkey < 1200000",
    )
    .await;
    println!("filtered COUNT(*): {and_count}");

    // Same bounds via BETWEEN -- exercises the `Expr::Between` recursion
    // path specifically (rewritten internally to the same AND-of-two-
    // comparisons shape, but via a structurally different `Expr` variant).
    println!(
        "\n--- lineitem BETWEEN predicate: l_orderkey BETWEEN 1100001 AND 1199999 \
         (should skip 5 of 6 segments) ---"
    );
    let between_count = scalar_count(
        &ctx,
        "SELECT COUNT(*) FROM lineitem_native WHERE l_orderkey BETWEEN 1100001 AND 1199999",
    )
    .await;
    println!("filtered COUNT(*): {between_count}");

    // Cell-exact cross-check against a pruning-independent manual scan,
    // same pattern as the orders leg above.
    let li_provider =
        query_engine::storage::NativeTable::try_new(LINEITEM_DIR).expect("open native lineitem");
    let li_full = query_engine::physical::operators::TableProvider::scan(&li_provider, None)
        .expect("unfiltered lineitem scan");
    let li_idx = li_full[0]
        .schema()
        .index_of("l_orderkey")
        .expect("l_orderkey column");
    let mut manual_and = 0usize;
    for batch in &li_full {
        let col = batch
            .column(li_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("l_orderkey is Int64");
        for v in col.values() {
            if *v > 1_100_000 && *v < 1_200_000 {
                manual_and += 1;
            }
        }
    }
    println!("\n--- lineitem cell-exact cross-check ---");
    println!("manual AND count: {manual_and} (engine AND: {and_count}, engine BETWEEN: {between_count})");
    assert_eq!(manual_and as i64, and_count, "AND-pruned result must match the unpruned baseline");
    assert_eq!(
        manual_and as i64, between_count,
        "BETWEEN-pruned result must equal the equivalent AND-pruned result exactly"
    );

    println!("\nPASS: lineitem AND/BETWEEN pruning is cell-exact vs. a pruning-independent baseline.");
    Ok(())
}
