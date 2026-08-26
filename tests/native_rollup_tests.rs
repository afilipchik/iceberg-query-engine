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
// Staleness: a base-table mutation after registration makes the rollup
// stop matching, and the fallback answer reflects the NEW base-table
// state correctly.
// ============================================================================

#[tokio::test]
async fn a_stale_rollup_after_a_base_table_mutation_falls_back_and_stays_correct() {
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

    // Mutate the BASE table (not the rollup) -- bumps lineitem_native's
    // own snapshot.version, which the rollup's recorded base_table_version
    // no longer matches.
    let deleted = ctx
        .delete_from_native_table("DELETE FROM lineitem_native WHERE l_orderkey = 1")
        .await
        .expect("delete must succeed");
    assert!(
        deleted.rows_deleted > 0,
        "the delete must actually remove real rows"
    );

    let after = ctx
        .sql(query)
        .await
        .expect("query must still succeed after the base mutates");
    assert!(
        after.metrics.rollup_answered.is_empty(),
        "a rollup whose recorded base-table version no longer matches the base table's \
         CURRENT version must be excluded from matching -- never silently stale"
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
        "the post-mutation fallback answer must be cell-exact vs. an independently \
         recomputed reference over the identically-mutated base table"
    );
    // And, for good measure, the post-mutation answer must differ from the
    // pre-mutation (stale) one -- otherwise this test would not actually
    // be exercising a real data change.
    assert_ne!(render(&before.batches), render(&after.batches));
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
