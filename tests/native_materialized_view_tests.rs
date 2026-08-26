//! End-to-end validation for the SQL DDL surface on top of task 001's
//! rollup matching/substitution mechanism (native-tables-rollups epic,
//! task 002): `CREATE MATERIALIZED VIEW <name> AS SELECT ...`, wired
//! through `Binder::bind()`'s new `Statement::CreateView` arm and
//! `ExecutionContext::create_materialized_view`. Mirrors
//! `native_rollup_tests.rs`'s own discipline: exercise the REAL end-to-end
//! path a user hits (SQL text -> `ExecutionContext::create_materialized_
//! view`/`sql`), not `register_rollup` or `native_rollup` internals
//! directly (those already have their own 34 tests from task 001, which
//! this task must not modify or re-derive).
//!
//! Requires `data/tpch-1mb` (committed fixture; CI regenerates it
//! deterministically).

use query_engine::ExecutionContext;

fn data_dir() -> String {
    format!("{}/data/tpch-1mb", env!("CARGO_MANIFEST_DIR"))
}

/// Cell-exact comparison via identical rendering on both sides.
fn render(batches: &[arrow::record_batch::RecordBatch]) -> String {
    arrow::util::pretty::pretty_format_batches(batches)
        .map(|d| d.to_string())
        .unwrap_or_default()
}

/// A fresh context with `lineitem_src` registered from the real fixture,
/// plus a native table `lineitem_native` containing every row, written via
/// the real CTAS SQL surface into a private tempdir. No rollup/view is
/// registered -- callers that want one call `create_materialized_view`
/// themselves.
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

/// The PRD's own worked example, as a `CREATE MATERIALIZED VIEW` statement
/// -- the exact SQL surface this task exists to wire up.
const CREATE_MATERIALIZED_VIEW_SQL: &str = "CREATE MATERIALIZED VIEW lineitem_rollup AS \
     SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
     SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order \
     FROM lineitem_native GROUP BY l_returnflag, l_linestatus";

const MATCHING_QUERY: &str = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
     SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order \
     FROM lineitem_native GROUP BY l_returnflag, l_linestatus \
     ORDER BY l_returnflag, l_linestatus";

// ============================================================================
// Core end-to-end proof: `CREATE MATERIALIZED VIEW ... AS SELECT ...`
// populates the rollup, and a SUBSEQUENT query through the ordinary
// `ExecutionContext::sql()` path (the same method both the distributed
// HTTP `/sql` endpoint and the REPL's own catch-all arm call) is
// transparently, cell-exact-ly answered from it -- reusing task 001's own
// validated matching mechanism completely unchanged.
// ============================================================================

#[tokio::test]
async fn create_materialized_view_populates_a_rollup_that_sql_transparently_matches() {
    let (mut ctx, _tmp) = build_lineitem_native().await;

    let created = ctx
        .create_materialized_view(CREATE_MATERIALIZED_VIEW_SQL)
        .await
        .expect("CREATE MATERIALIZED VIEW must succeed for the PRD's own worked example");
    assert_eq!(created.rollup_name, "lineitem_rollup");
    assert_eq!(created.base_table, "lineitem_native");
    assert!(created.rows > 0);
    assert_eq!(created.schema.fields().len(), 5);

    // The REAL SQL entrypoint -- the exact method HTTP `/sql` and the
    // REPL's catch-all both call.
    let rollup_result = ctx
        .sql(MATCHING_QUERY)
        .await
        .expect("rollup-answered query must succeed");
    assert_eq!(
        rollup_result.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()],
        "provenance must name the DDL-registered rollup that answered this query"
    );

    // Independent direct computation: a completely separate context, same
    // source data, no CREATE MATERIALIZED VIEW ever run against it.
    let (direct_ctx, _tmp2) = build_lineitem_native().await;
    let direct_result = direct_ctx
        .sql(MATCHING_QUERY)
        .await
        .expect("direct base-table query must succeed");
    assert!(direct_result.metrics.rollup_answered.is_empty());

    assert_eq!(rollup_result.row_count, direct_result.row_count);
    assert_eq!(
        render(&rollup_result.batches),
        render(&direct_result.batches),
        "rollup-answered result must be cell-exact vs. direct base-table computation"
    );
}

/// Confirms the DDL layer wires into the SAME matching mechanism task 001
/// validated, not a subtly different one -- one representative
/// order-independence check (task 001's own 34 tests already cover this
/// mechanism exhaustively; this is a wiring confidence check, not a
/// re-derivation).
#[tokio::test]
async fn create_materialized_view_ddl_registered_rollup_matches_order_independently() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    ctx.create_materialized_view(CREATE_MATERIALIZED_VIEW_SQL)
        .await
        .expect("CREATE MATERIALIZED VIEW must succeed");

    let reversed = "SELECT l_linestatus, l_returnflag, COUNT(*) AS count_order, \
                     SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price \
                     FROM lineitem_native GROUP BY l_linestatus, l_returnflag";
    let result = ctx
        .sql(reversed)
        .await
        .expect("order-reversed query must still match");
    assert_eq!(
        result.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );
}

// ============================================================================
// `sql()`'s fifth DDL/DML redirect case.
// ============================================================================

#[tokio::test]
async fn sql_refuses_create_materialized_view_and_points_at_the_ddl_entrypoint() {
    let (ctx, _tmp) = build_lineitem_native().await;
    let err = ctx.sql(CREATE_MATERIALIZED_VIEW_SQL).await.unwrap_err();
    assert!(
        err.to_string().contains("create_materialized_view"),
        "sql() must point callers at the real DDL entrypoint: {err}"
    );
}

/// A plain (non-materialized) `CREATE VIEW` is NOT caught by `sql()`'s
/// redirect guard (there is no dedicated entrypoint to redirect it to --
/// it is genuinely out of scope) -- it falls through to the ordinary
/// bind/plan/execute path and is refused there, directly, by
/// `Binder::bind()`'s own `Statement::CreateView` arm. This is the
/// REALISTIC path a user hits (`ExecutionContext::sql`, the same method
/// HTTP `/sql` and the REPL's catch-all call), so this test exercises it
/// at that level rather than calling `create_materialized_view` with a
/// mismatched statement (a "used the wrong function" case, not this
/// criterion's concern).
#[tokio::test]
async fn sql_refuses_a_plain_create_view_by_name_not_silently_as_a_rollup() {
    let (ctx, _tmp) = build_lineitem_native().await;
    let err = ctx
        .sql(
            "CREATE VIEW lineitem_view AS SELECT l_returnflag, COUNT(*) AS n \
             FROM lineitem_native GROUP BY l_returnflag",
        )
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("materialized") || msg.contains("MATERIALIZED"),
        "a plain CREATE VIEW must be refused BY NAME, naming that materialized is required: \
         {msg}"
    );
}

// ============================================================================
// `IF NOT EXISTS` -- decided explicitly (error), not left unhandled.
// ============================================================================

#[tokio::test]
async fn create_materialized_view_refuses_if_not_exists_by_name() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    let err = ctx
        .create_materialized_view(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS lineitem_rollup AS \
             SELECT l_returnflag, COUNT(*) AS n FROM lineitem_native GROUP BY l_returnflag",
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("IF NOT EXISTS"),
        "error should name the refused clause: {err}"
    );
}

// ============================================================================
// Unsupported `CreateView` clauses refused BY NAME (representative sample
// -- `require_supported_create_view_shape` enumerates all 17 fields).
// ============================================================================

#[tokio::test]
async fn create_materialized_view_refuses_or_replace_by_name() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    let err = ctx
        .create_materialized_view(
            "CREATE OR REPLACE MATERIALIZED VIEW lineitem_rollup AS \
             SELECT l_returnflag, COUNT(*) AS n FROM lineitem_native GROUP BY l_returnflag",
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("OR REPLACE"),
        "error should name the refused clause: {err}"
    );
}

#[tokio::test]
async fn create_materialized_view_refuses_an_explicit_view_column_list_by_name() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    let err = ctx
        .create_materialized_view(
            "CREATE MATERIALIZED VIEW lineitem_rollup (a, b) AS \
             SELECT l_returnflag, COUNT(*) AS n FROM lineitem_native GROUP BY l_returnflag",
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("column list"),
        "error should name the refused clause: {err}"
    );
}

// ============================================================================
// The defining query itself must still be a valid rollup shape -- the DDL
// layer does not loosen task 001's own requirements at all.
// ============================================================================

#[tokio::test]
async fn create_materialized_view_refuses_a_where_clause_in_the_defining_query() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    let err = ctx
        .create_materialized_view(
            "CREATE MATERIALIZED VIEW r AS SELECT l_returnflag, COUNT(*) AS n \
             FROM lineitem_native WHERE l_quantity > 10 GROUP BY l_returnflag",
        )
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("GROUP BY"),
        "error should explain the required defining-query shape: {err}"
    );
}

#[tokio::test]
async fn create_materialized_view_refuses_a_computed_projection_in_the_defining_query() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    let err = ctx
        .create_materialized_view(
            "CREATE MATERIALIZED VIEW r AS SELECT l_returnflag, SUM(l_quantity) + 1 AS n \
             FROM lineitem_native GROUP BY l_returnflag",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("GROUP BY"));
}

#[tokio::test]
async fn create_materialized_view_refuses_a_non_native_base_table() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    let err = ctx
        .create_materialized_view(
            "CREATE MATERIALIZED VIEW r AS SELECT l_returnflag, COUNT(*) AS n \
             FROM lineitem_src GROUP BY l_returnflag",
        )
        .await
        .unwrap_err();
    // Caught downstream by register_rollup's own, already-tested check --
    // this test confirms the DDL layer reaches it correctly, not that it
    // reproduces its wording.
    assert!(
        err.to_string().contains("native table"),
        "error should surface register_rollup's own native-table requirement: {err}"
    );
}

#[tokio::test]
async fn create_materialized_view_refuses_a_join_in_the_defining_query() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    ctx.register_parquet("orders_src", format!("{}/orders.parquet", data_dir()))
        .unwrap();
    ctx.create_table_as_select("CREATE TABLE orders_native AS SELECT * FROM orders_src")
        .await
        .unwrap();
    let err = ctx
        .create_materialized_view(
            "CREATE MATERIALIZED VIEW r AS SELECT l_returnflag, COUNT(*) AS n \
             FROM lineitem_native JOIN orders_native ON l_orderkey = o_orderkey \
             GROUP BY l_returnflag",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("GROUP BY"));
}
