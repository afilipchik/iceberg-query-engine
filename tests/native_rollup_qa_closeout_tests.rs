//! Task 004 (native-tables-rollups epic) QA close-out: an explicit
//! fallback-correctness sweep run END TO END through the REAL SQL DDL
//! surface — `CREATE MATERIALIZED VIEW ... AS SELECT ...` via
//! `ExecutionContext::create_materialized_view`, ordinary
//! `ExecutionContext::sql()`, and the real mutation SQL entrypoints
//! (`insert_into_native_table`/`delete_from_native_table`/
//! `update_native_table`) — NOT `register_rollup` (task 001's lower-level
//! programmatic API, used throughout `native_rollup_tests.rs` and
//! `native_rollup_refresh_tests.rs`) and NOT internal `native_rollup`/
//! `native_write` functions directly.
//!
//! This file exists because of a real, named gap in the epic's own
//! per-task coverage: task 002's own tests (`native_materialized_view_
//! tests.rs`) exercise `CREATE MATERIALIZED VIEW` but never touch
//! mutation; task 003's own tests (`native_rollup_refresh_tests.rs`)
//! exercise mutation-triggered refresh but register every rollup via
//! `register_rollup`, never via the DDL. No existing test combines
//! "rollup registered via real DDL text" with "base table mutated via
//! real DML text, refresh fires, a subsequent ordinary query is still
//! correctly answered" — this file closes exactly that combination, plus
//! a DDL-registered non-matching-shape fallback sweep (task 001's own
//! three non-matching-shape tests all used `register_rollup`, not the
//! DDL, to register the rollup that then correctly fails to match).
//!
//! Per task 002's own Outcome (`.claude/epics/native-tables-rollups/
//! 002.md`): "No real HTTP-server round-trip test was added... validated
//! instead at the `ExecutionContext::sql()` level, the exact function the
//! HTTP handler calls... matching this codebase's own established test
//! depth." This file follows that same, already-established precedent —
//! `ExecutionContext::sql()`/`create_materialized_view()`/
//! `insert_into_native_table()`/etc. ARE the real SQL surface (they parse
//! and bind real SQL text), and are the identical functions
//! `src/distributed/server.rs::execute_statement` calls for `/sql`
//! (registration/mutation themselves are NOT reachable over HTTP/Flight,
//! per CTAS's own pre-existing, already-documented boundary — spinning up
//! a real `serve` process here would not exercise anything this file's
//! direct `ExecutionContext` calls don't already exercise).
//!
//! Requires `data/tpch-1mb` (committed fixture; CI regenerates it
//! deterministically), same fixture every other rollup test file uses.

use query_engine::ExecutionContext;

fn data_dir() -> String {
    format!("{}/data/tpch-1mb", env!("CARGO_MANIFEST_DIR"))
}

fn render(batches: &[arrow::record_batch::RecordBatch]) -> String {
    arrow::util::pretty::pretty_format_batches(batches)
        .map(|d| d.to_string())
        .unwrap_or_default()
}

/// A fresh context with `lineitem_src` registered from the real fixture,
/// plus a native table `lineitem_native` containing every row, written via
/// the real CTAS SQL surface into a private tempdir. No view/rollup is
/// registered — callers that want one issue `CREATE MATERIALIZED VIEW`
/// themselves, via the real DDL text, per this file's own charter.
async fn build_lineitem_native() -> (ExecutionContext, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.register_parquet("lineitem_src", format!("{}/lineitem.parquet", data_dir()))
        .expect("register source lineitem parquet");
    let result = ctx
        .create_table_as_select("CREATE TABLE lineitem_native AS SELECT * FROM lineitem_src")
        .await
        .expect("seed CREATE TABLE ... AS SELECT must succeed");
    assert!(result.rows > 0, "fixture must be non-empty");
    (ctx, tmp)
}

/// The PRD's own worked example, as `CREATE MATERIALIZED VIEW` DDL text —
/// the real SQL surface, not `register_rollup`.
const CREATE_MATERIALIZED_VIEW_SQL: &str = "CREATE MATERIALIZED VIEW lineitem_rollup AS \
     SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
     SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order \
     FROM lineitem_native GROUP BY l_returnflag, l_linestatus";

const MATCHING_QUERY: &str = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
     SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order \
     FROM lineitem_native GROUP BY l_returnflag, l_linestatus \
     ORDER BY l_returnflag, l_linestatus";

/// A fresh context with `lineitem_native` seeded AND `lineitem_rollup`
/// registered via the real `CREATE MATERIALIZED VIEW` DDL text.
async fn build_with_materialized_view() -> (ExecutionContext, tempfile::TempDir) {
    let (mut ctx, tmp) = build_lineitem_native().await;
    let reg = ctx
        .create_materialized_view(CREATE_MATERIALIZED_VIEW_SQL)
        .await
        .expect("CREATE MATERIALIZED VIEW must succeed for the PRD's own worked example");
    assert!(reg.rows > 0);
    (ctx, tmp)
}

// ============================================================================
// Part 1: non-matching-shape fallback, through the real DDL + sql() surface.
// ============================================================================

#[tokio::test]
async fn ddl_registered_rollup_a_different_group_by_set_falls_back_correctly() {
    let (ctx, _tmp) = build_with_materialized_view().await;
    let (ref_ctx, _tmp2) = build_lineitem_native().await;

    let query = "SELECT l_returnflag, SUM(l_quantity) AS sum_qty FROM lineitem_native \
                 GROUP BY l_returnflag ORDER BY l_returnflag";
    let got = ctx.sql(query).await.unwrap();
    assert!(
        got.metrics.rollup_answered.is_empty(),
        "a different GROUP BY set must NOT match the DDL-registered rollup"
    );
    let want = ref_ctx.sql(query).await.unwrap();
    assert_eq!(
        render(&got.batches),
        render(&want.batches),
        "fallback answer must still be cell-exact vs. an independent reference context that \
         never created the view"
    );
}

#[tokio::test]
async fn ddl_registered_rollup_an_added_filter_falls_back_correctly() {
    let (ctx, _tmp) = build_with_materialized_view().await;
    let (ref_ctx, _tmp2) = build_lineitem_native().await;

    let query = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
                 SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order \
                 FROM lineitem_native WHERE l_quantity > 25 GROUP BY l_returnflag, l_linestatus \
                 ORDER BY l_returnflag, l_linestatus";
    let got = ctx.sql(query).await.unwrap();
    assert!(
        got.metrics.rollup_answered.is_empty(),
        "a WHERE-filtered query must NOT match a rollup defined without one"
    );
    let want = ref_ctx.sql(query).await.unwrap();
    assert_eq!(render(&got.batches), render(&want.batches));
}

#[tokio::test]
async fn ddl_registered_rollup_a_different_aggregate_falls_back_correctly() {
    let (ctx, _tmp) = build_with_materialized_view().await;
    let (ref_ctx, _tmp2) = build_lineitem_native().await;

    let query = "SELECT l_returnflag, l_linestatus, AVG(l_quantity) AS avg_qty \
                 FROM lineitem_native GROUP BY l_returnflag, l_linestatus \
                 ORDER BY l_returnflag, l_linestatus";
    let got = ctx.sql(query).await.unwrap();
    assert!(
        got.metrics.rollup_answered.is_empty(),
        "AVG(l_quantity) must NOT match a rollup storing SUM(l_quantity)/COUNT(*) -- the \
         epic's own explicit no-subsumption/no-re-derivation decision"
    );
    let want = ref_ctx.sql(query).await.unwrap();
    assert_eq!(render(&got.batches), render(&want.batches));
}

#[tokio::test]
async fn ddl_registered_rollup_a_query_against_a_different_table_never_matches() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.register_parquet("lineitem_src", format!("{}/lineitem.parquet", data_dir()))
        .expect("register source lineitem parquet");
    ctx.register_parquet("orders_src", format!("{}/orders.parquet", data_dir()))
        .expect("register source orders parquet");
    ctx.create_table_as_select("CREATE TABLE lineitem_native AS SELECT * FROM lineitem_src")
        .await
        .expect("seed lineitem_native");
    ctx.create_table_as_select("CREATE TABLE orders_native AS SELECT * FROM orders_src")
        .await
        .expect("seed orders_native");
    ctx.create_materialized_view(CREATE_MATERIALIZED_VIEW_SQL)
        .await
        .expect("CREATE MATERIALIZED VIEW over lineitem_native");

    let query = "SELECT o_orderstatus, COUNT(*) AS n FROM orders_native GROUP BY o_orderstatus \
                 ORDER BY o_orderstatus";
    let got = ctx.sql(query).await.unwrap();
    assert!(
        got.metrics.rollup_answered.is_empty(),
        "a query against an unrelated base table must never match lineitem_rollup"
    );

    let ref_tmp = tempfile::tempdir().expect("ref tempdir");
    let mut ref_ctx = ExecutionContext::new().with_native_table_root(ref_tmp.path().to_path_buf());
    ref_ctx
        .register_parquet("orders_src", format!("{}/orders.parquet", data_dir()))
        .expect("register reference orders parquet");
    ref_ctx
        .create_table_as_select("CREATE TABLE orders_native AS SELECT * FROM orders_src")
        .await
        .expect("seed reference orders_native");
    let want = ref_ctx.sql(query).await.unwrap();
    assert_eq!(render(&got.batches), render(&want.batches));
}

// ============================================================================
// Part 2: a DDL-registered rollup whose base table mutation triggers
// refresh, then an ORDINARY sql() query is still correctly, freshly
// answered -- through the real INSERT/DELETE/UPDATE SQL entrypoints, not
// register_rollup and not native_write/native_delete/native_update
// directly.
// ============================================================================

#[tokio::test]
async fn ddl_registered_rollup_survives_an_insert_triggered_refresh_via_ordinary_sql() {
    let (mut ctx, _tmp) = build_with_materialized_view().await;

    let before = ctx.sql(MATCHING_QUERY).await.unwrap();
    assert_eq!(
        before.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );

    let inserted = ctx
        .insert_into_native_table(
            "INSERT INTO lineitem_native SELECT * FROM lineitem_src WHERE l_orderkey = 3",
        )
        .await
        .expect("INSERT via the real SQL entrypoint must succeed");
    assert!(inserted.rows_inserted > 0, "insert must add real rows");
    assert_eq!(inserted.rollups_refreshed.len(), 1);
    assert_eq!(inserted.rollups_refreshed[0].rollup_name, "lineitem_rollup");
    assert!(inserted.rollups_refreshed[0].error.is_none());

    // The SAME query, through the ORDINARY sql() entrypoint -- must still
    // be rollup-answered, immediately, with fresh post-insert data.
    let after = ctx.sql(MATCHING_QUERY).await.unwrap();
    assert_eq!(
        after.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()],
        "a DDL-registered rollup must still answer this query immediately after a real INSERT \
         -- eager refresh must fire regardless of how the rollup was originally registered"
    );
    assert_ne!(render(&before.batches), render(&after.batches));

    let (mut ref_ctx, _tmp2) = build_lineitem_native().await;
    ref_ctx
        .insert_into_native_table(
            "INSERT INTO lineitem_native SELECT * FROM lineitem_src WHERE l_orderkey = 3",
        )
        .await
        .expect("reference insert must succeed");
    let reference = ref_ctx.sql(MATCHING_QUERY).await.unwrap();
    assert_eq!(
        render(&after.batches),
        render(&reference.batches),
        "the DDL-registered, eagerly-refreshed rollup's answer must be cell-exact vs. an \
         independently recomputed reference over the identically-mutated base table"
    );
}

#[tokio::test]
async fn ddl_registered_rollup_survives_a_delete_triggered_refresh_via_ordinary_sql() {
    let (mut ctx, _tmp) = build_with_materialized_view().await;

    let before = ctx.sql(MATCHING_QUERY).await.unwrap();
    assert_eq!(
        before.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );

    let deleted = ctx
        .delete_from_native_table("DELETE FROM lineitem_native WHERE l_orderkey = 1")
        .await
        .expect("DELETE via the real SQL entrypoint must succeed");
    assert!(deleted.rows_deleted > 0);
    assert_eq!(deleted.rollups_refreshed.len(), 1);
    assert!(deleted.rollups_refreshed[0].error.is_none());

    let after = ctx.sql(MATCHING_QUERY).await.unwrap();
    assert_eq!(
        after.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );
    assert_ne!(render(&before.batches), render(&after.batches));

    let (mut ref_ctx, _tmp2) = build_lineitem_native().await;
    ref_ctx
        .delete_from_native_table("DELETE FROM lineitem_native WHERE l_orderkey = 1")
        .await
        .expect("reference delete must succeed");
    let reference = ref_ctx.sql(MATCHING_QUERY).await.unwrap();
    assert_eq!(render(&after.batches), render(&reference.batches));
}

#[tokio::test]
async fn ddl_registered_rollup_survives_an_update_triggered_refresh_via_ordinary_sql() {
    let (mut ctx, _tmp) = build_with_materialized_view().await;

    let before = ctx.sql(MATCHING_QUERY).await.unwrap();
    assert_eq!(
        before.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );

    let updated = ctx
        .update_native_table(
            "UPDATE lineitem_native SET l_quantity = l_quantity + 1 WHERE l_orderkey = 5",
        )
        .await
        .expect("UPDATE via the real SQL entrypoint must succeed");
    assert!(updated.rows_updated > 0);
    assert_eq!(updated.rollups_refreshed.len(), 1);
    assert!(updated.rollups_refreshed[0].error.is_none());

    let after = ctx.sql(MATCHING_QUERY).await.unwrap();
    assert_eq!(
        after.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );
    assert_ne!(render(&before.batches), render(&after.batches));

    let (mut ref_ctx, _tmp2) = build_lineitem_native().await;
    ref_ctx
        .update_native_table(
            "UPDATE lineitem_native SET l_quantity = l_quantity + 1 WHERE l_orderkey = 5",
        )
        .await
        .expect("reference update must succeed");
    let reference = ref_ctx.sql(MATCHING_QUERY).await.unwrap();
    assert_eq!(render(&after.batches), render(&reference.batches));
}

// ============================================================================
// Part 3: two differently-shaped rollups, BOTH registered via real DDL
// text against the SAME base table, one mutation refreshes both, and
// BOTH are still correctly answered via ordinary sql() afterward -- ties
// together "a rollup depending on a table that also has other rollups"
// with the DDL surface and mutation-triggered refresh in one scenario.
// ============================================================================

const SECOND_VIEW_SQL: &str = "CREATE MATERIALIZED VIEW lineitem_rollup_by_shipmode AS \
     SELECT l_shipmode, SUM(l_quantity) AS sum_qty, COUNT(*) AS n \
     FROM lineitem_native GROUP BY l_shipmode";
const SECOND_QUERY: &str = "SELECT l_shipmode, SUM(l_quantity) AS sum_qty, COUNT(*) AS n \
     FROM lineitem_native GROUP BY l_shipmode ORDER BY l_shipmode";

#[tokio::test]
async fn two_ddl_registered_rollups_on_one_table_both_survive_one_mutations_refresh() {
    let (mut ctx, _tmp) = build_with_materialized_view().await;
    let second = ctx
        .create_materialized_view(SECOND_VIEW_SQL)
        .await
        .expect("second CREATE MATERIALIZED VIEW must succeed");
    assert!(second.rows > 0);

    let first_before = ctx.sql(MATCHING_QUERY).await.unwrap();
    let second_before = ctx.sql(SECOND_QUERY).await.unwrap();
    assert_eq!(
        first_before.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );
    assert_eq!(
        second_before.metrics.rollup_answered,
        vec!["lineitem_rollup_by_shipmode".to_string()]
    );

    let updated = ctx
        .update_native_table(
            "UPDATE lineitem_native SET l_quantity = l_quantity + 1 WHERE l_orderkey = 5",
        )
        .await
        .expect("UPDATE must succeed");
    assert!(updated.rows_updated > 0);
    assert_eq!(
        updated.rollups_refreshed.len(),
        2,
        "one mutation must refresh BOTH dependent rollups, not just the first"
    );
    let mut refreshed_names: Vec<&str> = updated
        .rollups_refreshed
        .iter()
        .map(|r| {
            assert!(r.error.is_none(), "refresh must succeed: {:?}", r.error);
            r.rollup_name.as_str()
        })
        .collect();
    refreshed_names.sort_unstable();
    assert_eq!(
        refreshed_names,
        vec!["lineitem_rollup", "lineitem_rollup_by_shipmode"]
    );

    let first_after = ctx.sql(MATCHING_QUERY).await.unwrap();
    let second_after = ctx.sql(SECOND_QUERY).await.unwrap();
    assert_eq!(
        first_after.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );
    assert_eq!(
        second_after.metrics.rollup_answered,
        vec!["lineitem_rollup_by_shipmode".to_string()]
    );
    assert_ne!(render(&first_before.batches), render(&first_after.batches));
    assert_ne!(
        render(&second_before.batches),
        render(&second_after.batches)
    );

    let (mut ref_ctx, _tmp2) = build_lineitem_native().await;
    ref_ctx
        .update_native_table(
            "UPDATE lineitem_native SET l_quantity = l_quantity + 1 WHERE l_orderkey = 5",
        )
        .await
        .expect("reference update must succeed");
    let ref_first = ref_ctx.sql(MATCHING_QUERY).await.unwrap();
    let ref_second = ref_ctx.sql(SECOND_QUERY).await.unwrap();
    assert_eq!(render(&first_after.batches), render(&ref_first.batches));
    assert_eq!(render(&second_after.batches), render(&ref_second.batches));
}
