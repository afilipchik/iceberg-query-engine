//! Real, end-to-end validation for join-order-stats-hardening task 002:
//! does a native table's derived NDV go stale in the dangerous direction
//! after a real SQL `DELETE`, and does the fix (`src/storage/
//! native_table.rs::table_statistics_from`'s `DELETION_STALENESS_
//! THRESHOLD` degradation) behave correctly?
//!
//! Unlike `src/storage/native_table.rs`'s own unit tests (which edit
//! `deleted_rows` directly, deliberately isolating the READ side from
//! `native_delete.rs`'s own editing logic — see that file's own comment),
//! this file drives the REAL SQL path a user hits: `CREATE TABLE ... AS
//! SELECT` -> `DELETE FROM ... WHERE ...` (through
//! `ExecutionContext::delete_from_native_table` -> `native_delete::
//! delete_from_native_table`) -> re-query. This is the "real test" named
//! by task 002's own acceptance criteria: DELETE a meaningful, concentrated
//! fraction of a native table's rows (chosen so it would plausibly shift
//! true NDV), then query, and confirm the derived NDV now reflects
//! reality (row-count-bound case) or the degraded-statistics signal fires
//! (range-bound case) -- task 001's shared `classify_join_key_ndv`/
//! `warn_untrustworthy_join_key_stats` mechanism, reused unchanged.
//!
//! Requires `data/tpch-1mb` (committed fixture; CI regenerates it
//! deterministically). `orders.parquet` has exactly 1500 rows with
//! `o_orderkey` densely ranging 1..=1500 -- the same fixture
//! `native_delete_tests.rs` already relies on for the same reason.

use query_engine::optimizer::{classify_join_key_ndv, UntrustworthyStats};
use query_engine::physical::operators::TableProvider;
use query_engine::storage::NativeTable;
use query_engine::ExecutionContext;

fn data_dir() -> String {
    format!("{}/data/tpch-1mb", env!("CARGO_MANIFEST_DIR"))
}

/// A fresh context with `orders_src` registered, plus a native table
/// `cat_native` (`o_orderkey`, and `category = o_orderkey % 3` --
/// deliberately LOW-cardinality relative to the table's 1500 rows, the
/// exact shape whose NDV estimate is RANGE-bound, not row-count-bound)
/// built via the REAL `CREATE TABLE ... AS SELECT` SQL surface.
async fn build_category_native_table() -> (ExecutionContext, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.register_parquet("orders_src", format!("{}/orders.parquet", data_dir()))
        .expect("register source orders parquet");

    let result = ctx
        .create_table_as_select(
            "CREATE TABLE cat_native AS \
             SELECT o_orderkey, o_orderkey % 3 AS category FROM orders_src",
        )
        .await
        .expect("seed CREATE TABLE ... AS SELECT must succeed");
    assert_eq!(result.rows, 1500);

    (ctx, tmp)
}

fn native_stats(
    tmp: &tempfile::TempDir,
    table_name: &str,
) -> query_engine::physical::operators::TableStatistics {
    let dir = tmp.path().join(table_name);
    NativeTable::try_new(&dir)
        .expect("open native table")
        .statistics()
        .expect("native tables always report statistics")
}

/// The load-bearing scenario: DELETE every row of ONE category value
/// (1/3 of the table, 33% -- a meaningful, concentrated fraction chosen
/// specifically because it eliminates a whole distinct value, which is
/// exactly the shape a stale write-time min/max range can never reflect).
#[tokio::test]
async fn concentrated_delete_of_a_whole_category_degrades_its_ndv_and_the_shared_signal_fires() {
    let (mut ctx, tmp) = build_category_native_table().await;

    let before = native_stats(&tmp, "cat_native");
    let category_before = before
        .column_stats
        .get("category")
        .expect("category stats present");
    assert_eq!(
        category_before.ndv_est,
        Some(3),
        "before any deletion: dense low-cardinality range, NDV is exact"
    );

    let delete_result = ctx
        .delete_from_native_table("DELETE FROM cat_native WHERE category = 2")
        .await
        .expect("DELETE must succeed");
    assert!(
        delete_result.rows_deleted > 0,
        "the DELETE must actually remove rows for this test to mean anything"
    );

    let after = native_stats(&tmp, "cat_native");
    let category_after = after
        .column_stats
        .get("category")
        .expect("category stats present");

    // The dangerous direction named by the epic: a stale write-time range
    // (0..=2) can never reflect that value 2 was fully deleted, so a naive
    // re-derivation would keep reporting NDV=3 when the true post-delete
    // NDV is 2 -- an OVERESTIMATE that understates true join selectivity.
    // The fix must not silently keep serving that stale number.
    assert_eq!(
        category_after.ndv_est, None,
        "a range-bound NDV whose deletion fraction crosses the staleness threshold must \
         degrade to Missing, not keep serving a stale (and here, provably wrong) estimate"
    );

    // Confirm this is literally the SAME classification JoinReorder's own
    // DPsize cost model consults for every join edge -- task 001's shared
    // signal, reused directly by this fix, not reinvented (the epic's own
    // "one shared signal, not two" architecture decision).
    assert!(matches!(
        classify_join_key_ndv(
            category_after.ndv_est.map(|v| v as f64),
            after.row_count as f64
        ),
        Err(UntrustworthyStats::Missing)
    ));

    // `o_orderkey` is dense/unique -- row-count-bound, not range-bound --
    // so its NDV correctly reflects reality with zero need for this
    // mechanism: it simply shrinks with the live row count.
    let orderkey_after = after
        .column_stats
        .get("o_orderkey")
        .expect("o_orderkey stats present");
    assert_eq!(
        orderkey_after.ndv_est,
        Some(after.row_count as u64),
        "a dense/unique key's NDV must keep reflecting reality (== live row count) after \
         deletion, with no degradation needed"
    );
    assert_eq!(
        after.row_count,
        before.row_count - delete_result.rows_deleted as usize
    );
}

/// A real DELETE that removes a small, non-value-eliminating fraction of
/// rows must NOT false-positive degrade a range-bound NDV -- the fix is a
/// deliberately modest threshold, not a hair-trigger on any mutation.
#[tokio::test]
async fn a_small_delete_that_does_not_eliminate_a_value_does_not_degrade_ndv() {
    let (mut ctx, tmp) = build_category_native_table().await;

    // Deletes a handful of category-2 rows (o_orderkey in a narrow range),
    // not every category-2 row -- category 2 still has plenty of live
    // rows afterward, and the deletion fraction stays well under
    // DELETION_STALENESS_THRESHOLD (10%).
    let delete_result = ctx
        .delete_from_native_table("DELETE FROM cat_native WHERE category = 2 AND o_orderkey <= 30")
        .await
        .expect("DELETE must succeed");
    assert!(delete_result.rows_deleted > 0);
    assert!(
        (delete_result.rows_deleted as f64) / 1500.0 < 0.10,
        "this test's own premise requires staying under the staleness threshold"
    );

    let after = native_stats(&tmp, "cat_native");
    assert_eq!(
        after.column_stats.get("category").unwrap().ndv_est,
        Some(3),
        "a light deletion must not false-positive degrade a range-bound NDV"
    );
}

/// A genuinely no-op DELETE (predicate matches nothing) must leave
/// statistics byte-for-byte identical -- confirms the fix adds no cost or
/// behavior change on the overwhelmingly common "queried a never-mutated
/// (or not-meaningfully-mutated) table" path.
#[tokio::test]
async fn a_no_op_delete_leaves_statistics_unchanged() {
    let (mut ctx, tmp) = build_category_native_table().await;
    let before = native_stats(&tmp, "cat_native");

    let delete_result = ctx
        .delete_from_native_table("DELETE FROM cat_native WHERE o_orderkey > 999999")
        .await
        .expect("DELETE must succeed (even though it matches nothing)");
    assert_eq!(delete_result.rows_deleted, 0);

    let after = native_stats(&tmp, "cat_native");
    assert_eq!(after.row_count, before.row_count);
    assert_eq!(
        after.column_stats.get("category").unwrap().ndv_est,
        before.column_stats.get("category").unwrap().ndv_est
    );
}
