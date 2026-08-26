//! End-to-end validation for the rollup matching/substitution mechanism
//! (native-tables-rollups epic, task 001): `ExecutionContext::
//! register_rollup` (the programmatic registration API), the real
//! matching pass wired into `sql()` before `Optimizer::optimize()`
//! (`storage::native_rollup`), and its provenance/staleness/fallback
//! behavior. Mirrors `native_delete_tests.rs`/`native_insert_tests.rs`'s
//! own discipline: exercise the REAL end-to-end path a user hits (SQL
//! text -> `ExecutionContext::sql`/`register_rollup`), not the module's
//! internal functions directly (those have their own unit tests in
//! `src/storage/native_rollup.rs`).
//!
//! Requires `data/tpch-1mb` (committed fixture; CI regenerates it
//! deterministically) — the PRD's own worked example (`lineitem` grouped
//! by `l_returnflag`/`l_linestatus`, SUM/COUNT aggregates).

use query_engine::ExecutionContext;

fn data_dir() -> String {
    format!("{}/data/tpch-1mb", env!("CARGO_MANIFEST_DIR"))
}

/// Cell-exact comparison via identical rendering on both sides — mirrors
/// `native_delete_tests.rs`'s own byte-identical comparison idiom.
fn render(batches: &[arrow::record_batch::RecordBatch]) -> String {
    arrow::util::pretty::pretty_format_batches(batches)
        .map(|d| d.to_string())
        .unwrap_or_default()
}

/// A fresh context with `lineitem_src` registered from the real fixture,
/// plus a native table `lineitem_native` containing every row, written
/// via the real CTAS SQL surface into a private tempdir. No rollup is
/// registered — callers that want one call `register_rollup` themselves.
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

const WORKED_EXAMPLE_DEFINING_SQL: &str = "SELECT l_returnflag, l_linestatus, \
     SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, \
     COUNT(*) AS count_order FROM lineitem_native GROUP BY l_returnflag, l_linestatus";

/// The PRD's own worked example, registered as `lineitem_rollup` against
/// a freshly built `lineitem_native`.
async fn build_with_rollup() -> (ExecutionContext, tempfile::TempDir) {
    let (mut ctx, tmp) = build_lineitem_native().await;
    let reg = ctx
        .register_rollup(
            "lineitem_rollup",
            "lineitem_native",
            WORKED_EXAMPLE_DEFINING_SQL,
        )
        .await
        .expect("register_rollup must succeed for the PRD's own worked example");
    assert!(reg.rows > 0);
    assert_eq!(reg.schema.fields().len(), 5);
    (ctx, tmp)
}

// ============================================================================
// Core proof: a matching query is answered by the rollup, cell-exact vs.
// the identical query computed directly against the base table.
// ============================================================================

#[tokio::test]
async fn matching_query_is_answered_by_the_rollup_and_is_cell_exact_vs_direct_computation() {
    let (ctx, _tmp) = build_with_rollup().await;

    let query = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
                 SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order \
                 FROM lineitem_native GROUP BY l_returnflag, l_linestatus \
                 ORDER BY l_returnflag, l_linestatus";

    let rollup_result = ctx
        .sql(query)
        .await
        .expect("rollup-answered query must succeed");
    assert_eq!(
        rollup_result.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()],
        "provenance must name the rollup that answered this query"
    );

    // Independent direct computation: a completely separate context, same
    // source data, NO rollup ever registered against it.
    let (direct_ctx, _tmp2) = build_lineitem_native().await;
    let direct_result = direct_ctx
        .sql(query)
        .await
        .expect("direct base-table query must succeed");
    assert!(
        direct_result.metrics.rollup_answered.is_empty(),
        "the direct-computation context has no rollup registered at all"
    );

    assert_eq!(rollup_result.row_count, direct_result.row_count);
    assert_eq!(
        render(&rollup_result.batches),
        render(&direct_result.batches),
        "rollup-answered result must be cell-exact vs. direct base-table computation"
    );
}

#[tokio::test]
async fn group_by_order_in_the_query_does_not_affect_matching_or_the_answer() {
    let (ctx, _tmp) = build_with_rollup().await;

    let reversed = "SELECT l_linestatus, l_returnflag, COUNT(*) AS count_order, \
                     SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price \
                     FROM lineitem_native GROUP BY l_linestatus, l_returnflag \
                     ORDER BY l_returnflag, l_linestatus";
    let result = ctx
        .sql(reversed)
        .await
        .expect("order-reversed query must still match");
    assert_eq!(
        result.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()],
        "GROUP BY l_linestatus, l_returnflag must match a rollup defined as GROUP BY \
         l_returnflag, l_linestatus -- order-independence, decided explicitly"
    );

    let (direct_ctx, _tmp2) = build_lineitem_native().await;
    let canonical = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
                      SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order \
                      FROM lineitem_native GROUP BY l_returnflag, l_linestatus \
                      ORDER BY l_returnflag, l_linestatus";
    let direct_result = direct_ctx.sql(canonical).await.unwrap();

    // Re-order the rollup-answered columns to the canonical order for a
    // like-for-like cell comparison (the SELECT list itself was reversed).
    let reordered = ctx
        .sql(
            "SELECT l_returnflag, l_linestatus, sum_qty, sum_base_price, count_order FROM (\
             SELECT l_linestatus, l_returnflag, COUNT(*) AS count_order, \
             SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price \
             FROM lineitem_native GROUP BY l_linestatus, l_returnflag) t \
             ORDER BY l_returnflag, l_linestatus",
        )
        .await
        .expect("wrapping SELECT over the rollup-matching subquery must succeed");
    assert_eq!(render(&reordered.batches), render(&direct_result.batches));
}

#[tokio::test]
async fn aliasing_does_not_affect_matching() {
    let (ctx, _tmp) = build_with_rollup().await;
    // Different aliases than the rollup's own defining SQL (`sum_qty` ->
    // `total_qty`, `count_order` -> `n`) -- must still match: aliases are
    // display names for OUTPUT columns, never part of the shape key.
    let query = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS total_qty, \
                 SUM(l_extendedprice) AS total_price, COUNT(*) AS n \
                 FROM lineitem_native GROUP BY l_returnflag, l_linestatus \
                 ORDER BY l_returnflag, l_linestatus";
    let result = ctx
        .sql(query)
        .await
        .expect("differently-aliased query must still match");
    assert_eq!(
        result.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );
    // The OUTPUT column names must reflect the QUERY's own aliases, not
    // the rollup's internal physical column names.
    let names: Vec<&str> = result
        .schema
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    assert_eq!(
        names,
        vec![
            "l_returnflag",
            "l_linestatus",
            "total_qty",
            "total_price",
            "n"
        ]
    );
}

#[tokio::test]
async fn a_query_projecting_only_some_of_the_rollups_columns_still_matches() {
    let (ctx, _tmp) = build_with_rollup().await;
    // Groups by BOTH columns and requests the FULL aggregate set (all
    // three) -- an exact match on both the GROUP BY set and the
    // aggregate set -- but the SELECT list only PROJECTS l_returnflag,
    // dropping l_linestatus from the OUTPUT while still grouping by it.
    // This is the scenario `storage::native_rollup`'s own doc names
    // explicitly: the GROUP BY/aggregate SET equality check is against
    // the Aggregate node's OWN fields, independent of what the outer
    // Project happens to expose. (A query requesting FEWER aggregates
    // than the rollup stores -- e.g. only SUM(l_quantity), omitting
    // SUM(l_extendedprice)/COUNT(*) entirely -- is a genuinely smaller
    // aggregate SET and correctly does NOT match; that is covered by
    // `a_different_aggregate_falls_back_and_stays_correct`-style tests,
    // not this one.)
    let query = "SELECT l_returnflag, SUM(l_quantity) AS sum_qty, \
                 SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order \
                 FROM lineitem_native GROUP BY l_returnflag, l_linestatus \
                 ORDER BY l_returnflag";
    let result = ctx
        .sql(query)
        .await
        .expect("subset-projection query must succeed");
    assert_eq!(
        result.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );
    assert_eq!(result.schema.fields().len(), 4);

    let (direct_ctx, _tmp2) = build_lineitem_native().await;
    let direct = direct_ctx.sql(query).await.unwrap();
    assert_eq!(render(&result.batches), render(&direct.batches));
}

// ============================================================================
// Explicit non-matching-shape cases: fallback stays CORRECT, not just
// "doesn't crash" (the task's own acceptance criterion).
// ============================================================================

#[tokio::test]
async fn a_different_group_by_set_falls_back_and_stays_correct() {
    let (ctx, _tmp) = build_with_rollup().await;
    let query = "SELECT l_returnflag, SUM(l_quantity) AS sum_qty FROM lineitem_native \
                 GROUP BY l_returnflag ORDER BY l_returnflag";
    let result = ctx
        .sql(query)
        .await
        .expect("query must succeed via fallback");
    assert!(
        result.metrics.rollup_answered.is_empty(),
        "GROUP BY l_returnflag alone must NOT match a rollup grouped by \
         (l_returnflag, l_linestatus) -- a real, different GROUP BY set"
    );

    let (direct_ctx, _tmp2) = build_lineitem_native().await;
    let direct = direct_ctx.sql(query).await.unwrap();
    assert_eq!(
        render(&result.batches),
        render(&direct.batches),
        "fallback answer must be cell-exact, not merely non-crashing"
    );
}

#[tokio::test]
async fn an_added_filter_falls_back_and_stays_correct() {
    let (ctx, _tmp) = build_with_rollup().await;
    let query = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
                 SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order \
                 FROM lineitem_native WHERE l_quantity > 25 \
                 GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus";
    let result = ctx
        .sql(query)
        .await
        .expect("query must succeed via fallback");
    assert!(
        result.metrics.rollup_answered.is_empty(),
        "a WHERE clause not present in the rollup's own definition must NOT match"
    );

    let (direct_ctx, _tmp2) = build_lineitem_native().await;
    let direct = direct_ctx.sql(query).await.unwrap();
    assert_eq!(render(&result.batches), render(&direct.batches));
    // Sanity: the filter is doing real work (fewer/narrower rows than the
    // unfiltered rollup would report for SOME group), i.e. this is a
    // meaningfully different query, not a no-op filter.
    assert!(!result.batches.is_empty());
}

#[tokio::test]
async fn a_different_aggregate_falls_back_and_stays_correct() {
    let (ctx, _tmp) = build_with_rollup().await;
    let query = "SELECT l_returnflag, l_linestatus, AVG(l_quantity) AS avg_qty \
                 FROM lineitem_native GROUP BY l_returnflag, l_linestatus \
                 ORDER BY l_returnflag, l_linestatus";
    let result = ctx
        .sql(query)
        .await
        .expect("query must succeed via fallback");
    assert!(
        result.metrics.rollup_answered.is_empty(),
        "AVG(l_quantity) is not in the rollup's own aggregate set (SUM/SUM/COUNT) -- must \
         NOT match"
    );

    let (direct_ctx, _tmp2) = build_lineitem_native().await;
    let direct = direct_ctx.sql(query).await.unwrap();
    assert_eq!(render(&result.batches), render(&direct.batches));
}

#[tokio::test]
async fn a_query_against_a_different_table_never_matches() {
    let (mut ctx, _tmp) = build_with_rollup().await;
    ctx.register_parquet("orders_src", format!("{}/orders.parquet", data_dir()))
        .expect("register orders parquet");
    let result = ctx
        .sql(
            "SELECT o_orderstatus, COUNT(*) AS n FROM orders_src GROUP BY o_orderstatus \
              ORDER BY o_orderstatus",
        )
        .await
        .expect("query against a different table must succeed via the ordinary path");
    assert!(result.metrics.rollup_answered.is_empty());
}

// ============================================================================
// Provenance: observably true when (and only when) a rollup answers.
// ============================================================================

#[tokio::test]
async fn provenance_distinguishes_rollup_answered_from_base_table_answered() {
    let (ctx, _tmp) = build_with_rollup().await;
    let matching = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
                     SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order \
                     FROM lineitem_native GROUP BY l_returnflag, l_linestatus";
    let non_matching = "SELECT l_returnflag, l_linestatus, AVG(l_quantity) AS avg_qty \
                         FROM lineitem_native GROUP BY l_returnflag, l_linestatus";

    let hit = ctx.sql(matching).await.unwrap();
    let miss = ctx.sql(non_matching).await.unwrap();
    assert!(
        !hit.metrics.rollup_answered.is_empty(),
        "a match must be observable"
    );
    assert!(
        miss.metrics.rollup_answered.is_empty(),
        "a miss must be observably different"
    );
}

// ============================================================================
// Staleness: task 001's own bookkeeping mechanism (a base-table mutation
// makes a rollup's recorded (table_id, version) stop matching the base
// table's CURRENT one, so it is excluded from matching and the fallback
// answer reflects the NEW base-table state correctly). Task 003
// (native-tables-rollups epic) wires an EAGER refresh into
// `ExecutionContext::insert_into_native_table`/`delete_from_native_table`/
// `update_native_table` specifically, so a mutation that goes through
// THOSE entrypoints no longer leaves a dependent rollup stale at all --
// see `native_rollup_refresh_tests.rs` for task 003's own dedicated
// coverage (eager refresh through all three mutation types, the
// multi-rollup case, and the refresh-fails-so-correctly-falls-back case).
// The two tests below still exist to prove the STALENESS BOOKKEEPING
// ITSELF (task 001's own mechanism, independent of whatever triggers a
// mutation) continues to work correctly and is not somehow disabled or
// weakened by task 003's addition.
// ============================================================================

#[tokio::test]
async fn a_base_table_mutation_through_execution_context_now_eagerly_refreshes_the_rollup() {
    // Renamed/re-asserted for task 003 (native-tables-rollups epic): this
    // test used to be named `..._falls_back_and_stays_correct` and
    // asserted the query stopped matching after the DELETE below. That
    // assertion described a real but explicitly TEMPORARY gap named by
    // task 001's own Outcome ("today, a mutated base table's rollup
    // simply goes stale ... and stays that way until register_rollup is
    // called again manually ... task 003's job"). Task 003 closes exactly
    // that gap for THIS entrypoint (`ExecutionContext::
    // delete_from_native_table`, called below) by eagerly refreshing every
    // dependent rollup before the DELETE call returns -- so the correct,
    // intended behavior through this entrypoint is now "the SAME query is
    // STILL answered by the rollup, with fresh, post-delete data," not
    // "falls back." The staleness BOOKKEEPING mechanism itself (excluding
    // a rollup whose recorded version doesn't match) is unchanged and
    // still directly tested by
    // `staleness_bookkeeping_still_correctly_excludes_a_rollup_mutated_outside_execution_context`
    // immediately below, which mutates the base table WITHOUT going
    // through this refresh-wired entrypoint.
    let (mut ctx, _tmp) = build_with_rollup().await;
    let query = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
                 SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order \
                 FROM lineitem_native GROUP BY l_returnflag, l_linestatus \
                 ORDER BY l_returnflag, l_linestatus";

    // Confirm the rollup answers BEFORE any mutation.
    let before = ctx.sql(query).await.unwrap();
    assert_eq!(
        before.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );

    // Mutate the BASE table through the real ExecutionContext entrypoint
    // -- bumps lineitem_native's own snapshot.version AND (task 003)
    // eagerly refreshes lineitem_rollup before this call returns.
    let deleted = ctx
        .delete_from_native_table("DELETE FROM lineitem_native WHERE l_orderkey = 1")
        .await
        .expect("delete must succeed");
    assert!(
        deleted.rows_deleted > 0,
        "the delete must actually remove real rows"
    );
    assert_eq!(
        deleted.rollups_refreshed.len(),
        1,
        "exactly one dependent rollup must have been refreshed"
    );
    assert_eq!(deleted.rollups_refreshed[0].rollup_name, "lineitem_rollup");
    assert!(
        deleted.rollups_refreshed[0].error.is_none(),
        "the refresh must have succeeded: {:?}",
        deleted.rollups_refreshed[0].error
    );

    let after = ctx
        .sql(query)
        .await
        .expect("query must still succeed after the base mutates");
    assert_eq!(
        after.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()],
        "task 003: the rollup was eagerly refreshed as part of the DELETE call itself, so the \
         SAME query must STILL be answered by it -- now reflecting the post-delete data, never \
         falling back to the base table for a table this codebase's own refresh wiring keeps \
         current automatically"
    );

    // Independent ground truth: a SEPARATE context, loads the SAME source,
    // applies the IDENTICAL delete, no rollup involved at all.
    let (mut ref_ctx, _tmp3) = build_lineitem_native().await;
    ref_ctx
        .delete_from_native_table("DELETE FROM lineitem_native WHERE l_orderkey = 1")
        .await
        .expect("reference delete must succeed");
    let reference = ref_ctx.sql(query).await.unwrap();

    assert_eq!(
        render(&after.batches),
        render(&reference.batches),
        "the refreshed rollup's answer must be cell-exact vs. an independently recomputed \
         reference over the identically-mutated base table"
    );
    // And, for good measure, the post-mutation answer must differ from the
    // pre-mutation one -- otherwise this test would not actually be
    // exercising a real data change.
    assert_ne!(render(&before.batches), render(&after.batches));
}

#[tokio::test]
async fn staleness_bookkeeping_still_correctly_excludes_a_rollup_mutated_outside_execution_context()
{
    // Task 001's own staleness bookkeeping (`ExecutionContext::
    // rollup_candidates`) is a property of the MANIFEST comparison, not
    // of which code path triggered a mutation -- this test proves it
    // still holds when the base table's underlying storage is mutated
    // via the lower-level `storage::native_delete::delete_from_native_table`
    // function DIRECTLY, bypassing `ExecutionContext::
    // delete_from_native_table` entirely (and therefore bypassing task
    // 003's eager-refresh wiring, which lives ONLY in the
    // `ExecutionContext` entrypoint, not in the storage layer itself).
    // This is exactly the ORIGINAL scenario/coverage of this file's own
    // pre-task-003 staleness test, preserved here at the layer where it
    // is still literally true, rather than silently dropped.
    use query_engine::planner::{BinaryOp, Column, Expr, ScalarValue};
    use query_engine::storage::native_delete;

    fn orderkey_eq_one() -> Expr {
        Expr::BinaryExpr {
            left: Box::new(Expr::Column(Column::new("l_orderkey"))),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(1))),
        }
    }

    let (mut ctx, tmp) = build_with_rollup().await;
    let query = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
                 SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order \
                 FROM lineitem_native GROUP BY l_returnflag, l_linestatus \
                 ORDER BY l_returnflag, l_linestatus";

    let before = ctx.sql(query).await.unwrap();
    assert_eq!(
        before.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );

    // Mutate lineitem_native's on-disk storage directly -- no
    // ExecutionContext involved, so no refresh is ever attempted.
    let table_dir = tmp.path().join("lineitem_native");
    let deleted =
        native_delete::delete_from_native_table(&table_dir, Some(&orderkey_eq_one())).await;
    let deleted = deleted.expect("direct low-level delete must succeed");
    assert!(
        deleted.rows_deleted > 0,
        "the delete must actually remove real rows"
    );

    // Re-register the (now-mutated) base table so `ctx.sql` sees its NEW
    // version -- mirrors what `ExecutionContext::delete_from_native_table`
    // itself does, minus the refresh step this test deliberately skips.
    ctx.register_native_table("lineitem_native", &table_dir)
        .expect("re-register the mutated base table");

    let after = ctx
        .sql(query)
        .await
        .expect("query must still succeed after the base mutates");
    assert!(
        after.metrics.rollup_answered.is_empty(),
        "a rollup whose recorded base-table version no longer matches the base table's \
         CURRENT version must be excluded from matching when nothing ever refreshed it -- \
         task 001's staleness bookkeeping must still hold independent of task 003's refresh \
         wiring"
    );

    // The fallback answer must still be correct (cell-exact vs. an
    // independent reference), never silently wrong or silently stale.
    let (mut ref_ctx, _tmp3) = build_lineitem_native().await;
    ref_ctx
        .delete_from_native_table("DELETE FROM lineitem_native WHERE l_orderkey = 1")
        .await
        .expect("reference delete must succeed");
    let reference = ref_ctx.sql(query).await.unwrap();
    assert_eq!(render(&after.batches), render(&reference.batches));
    assert_ne!(
        render(&before.batches),
        render(&after.batches),
        "the fallback answer must actually reflect the real data change, not coincidentally \
         match the pre-mutation rollup answer"
    );
}

// ============================================================================
// register_rollup: validation / error paths.
// ============================================================================

#[tokio::test]
async fn register_rollup_refuses_an_unregistered_base_table() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    let err = ctx
        .register_rollup("r", "not_a_real_table", WORKED_EXAMPLE_DEFINING_SQL)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("not_a_real_table"));
}

#[tokio::test]
async fn register_rollup_refuses_a_non_native_base_table() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    let err = ctx
        .register_rollup(
            "r",
            "lineitem_src", // plain ParquetTable, not a native table
            "SELECT l_returnflag, COUNT(*) AS n FROM lineitem_src GROUP BY l_returnflag",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("native table"));
}

#[tokio::test]
async fn register_rollup_refuses_a_where_clause_in_the_defining_query() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    let err = ctx
        .register_rollup(
            "r",
            "lineitem_native",
            "SELECT l_returnflag, COUNT(*) AS n FROM lineitem_native WHERE l_quantity > 10 \
             GROUP BY l_returnflag",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("register_rollup"));
}

#[tokio::test]
async fn register_rollup_refuses_a_computed_projection_in_the_defining_query() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    let err = ctx
        .register_rollup(
            "r",
            "lineitem_native",
            "SELECT l_returnflag, SUM(l_quantity) + 1 AS n FROM lineitem_native \
             GROUP BY l_returnflag",
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("register_rollup"));
}

#[tokio::test]
async fn register_rollup_refuses_a_base_table_mismatch() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    ctx.register_parquet("orders_src", format!("{}/orders.parquet", data_dir()))
        .unwrap();
    let created = ctx
        .create_table_as_select("CREATE TABLE orders_native AS SELECT * FROM orders_src")
        .await
        .unwrap();
    assert!(created.rows > 0);
    // Defining query scans lineitem_native but is registered against
    // orders_native.
    let err = ctx
        .register_rollup("r", "orders_native", WORKED_EXAMPLE_DEFINING_SQL)
        .await
        .unwrap_err();
    assert!(err.to_string().contains("lineitem_native"));
}
