//! End-to-end validation for the refresh-on-write model (native-tables-
//! rollups epic, task 003): eager, synchronous rollup refresh wired into
//! `ExecutionContext::insert_into_native_table`/`delete_from_native_table`/
//! `update_native_table`. Mirrors `native_rollup_tests.rs`'s own
//! discipline: exercise the REAL end-to-end path a user hits (SQL text ->
//! `ExecutionContext::sql`/`insert_into_native_table`/etc.), not internal
//! functions directly.
//!
//! See `ExecutionContext::refresh_dependent_rollups`'s own doc
//! (`src/execution/context.rs`) for the full eager-vs-lazy design
//! decision this task made, and `.claude/epics/native-tables-rollups/
//! 003.md`'s Outcome section for the performance measurement and memory-
//! safety reasoning this file's tests back up with real, passing
//! assertions.
//!
//! Requires `data/tpch-1mb` (committed fixture; CI regenerates it
//! deterministically) — the PRD's own worked example (`lineitem` grouped
//! by `l_returnflag`/`l_linestatus`, SUM/COUNT aggregates), same fixture
//! `native_rollup_tests.rs` uses.

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

const WORKED_EXAMPLE_QUERY: &str = "SELECT l_returnflag, l_linestatus, \
     SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price, \
     COUNT(*) AS count_order FROM lineitem_native GROUP BY l_returnflag, l_linestatus \
     ORDER BY l_returnflag, l_linestatus";

/// A SECOND, differently-shaped defining query against the SAME base
/// table — GROUP BY a single column, a single aggregate — used by the
/// multi-rollup test to prove one mutation refreshes ALL dependents, not
/// just the first one it happens to iterate.
const SECOND_ROLLUP_DEFINING_SQL: &str =
    "SELECT l_returnflag, SUM(l_quantity) AS b_sum_qty FROM lineitem_native GROUP BY l_returnflag";
const SECOND_ROLLUP_QUERY: &str = "SELECT l_returnflag, SUM(l_quantity) AS b_sum_qty \
     FROM lineitem_native GROUP BY l_returnflag ORDER BY l_returnflag";

/// The PRD's own worked example, registered as `lineitem_rollup` against
/// a freshly built `lineitem_native`.
async fn build_with_rollup() -> (ExecutionContext, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.register_parquet("lineitem_src", format!("{}/lineitem.parquet", data_dir()))
        .expect("register source lineitem parquet");
    let created = ctx
        .create_table_as_select("CREATE TABLE lineitem_native AS SELECT * FROM lineitem_src")
        .await
        .expect("seed CREATE TABLE ... AS SELECT must succeed");
    assert!(created.rows > 0, "fixture must be non-empty");
    let reg = ctx
        .register_rollup(
            "lineitem_rollup",
            "lineitem_native",
            WORKED_EXAMPLE_DEFINING_SQL,
        )
        .await
        .expect("register_rollup must succeed for the PRD's own worked example");
    assert!(reg.rows > 0);
    (ctx, tmp)
}

// ============================================================================
// Eager refresh through each of the three mutation types, cell-exact.
// ============================================================================

#[tokio::test]
async fn insert_eagerly_refreshes_the_dependent_rollup_and_stays_cell_exact() {
    let (mut ctx, _tmp) = build_with_rollup().await;

    let before = ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
    assert_eq!(
        before.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );

    // Real rows from the SAME source, appended -- this genuinely changes
    // every affected group's SUM/COUNT, not a no-op.
    let inserted = ctx
        .insert_into_native_table(
            "INSERT INTO lineitem_native SELECT * FROM lineitem_src WHERE l_orderkey = 3",
        )
        .await
        .expect("insert must succeed");
    assert!(
        inserted.rows_inserted > 0,
        "the insert must actually add real rows"
    );
    assert_eq!(
        inserted.rollups_refreshed.len(),
        1,
        "exactly one dependent rollup must have been refreshed"
    );
    assert_eq!(inserted.rollups_refreshed[0].rollup_name, "lineitem_rollup");
    assert!(
        inserted.rollups_refreshed[0].error.is_none(),
        "refresh must have succeeded: {:?}",
        inserted.rollups_refreshed[0].error
    );

    let after = ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
    assert_eq!(
        after.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()],
        "the rollup must still answer the SAME query immediately after the INSERT -- eager \
         refresh means never falling back just because the base table changed"
    );
    assert_ne!(
        render(&before.batches),
        render(&after.batches),
        "the refreshed answer must actually reflect the inserted rows"
    );

    // Independent reference: a separate context, same source, same
    // INSERT, no rollup involved at all.
    let (mut ref_ctx, _tmp2) = build_lineitem_native().await;
    ref_ctx
        .insert_into_native_table(
            "INSERT INTO lineitem_native SELECT * FROM lineitem_src WHERE l_orderkey = 3",
        )
        .await
        .expect("reference insert must succeed");
    let reference = ref_ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
    assert_eq!(
        render(&after.batches),
        render(&reference.batches),
        "the refreshed rollup's answer must be cell-exact vs. an independently recomputed \
         reference over the identically-mutated base table"
    );
}

#[tokio::test]
async fn delete_eagerly_refreshes_the_dependent_rollup_and_stays_cell_exact() {
    let (mut ctx, _tmp) = build_with_rollup().await;

    let before = ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
    assert_eq!(
        before.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );

    let deleted = ctx
        .delete_from_native_table("DELETE FROM lineitem_native WHERE l_orderkey = 1")
        .await
        .expect("delete must succeed");
    assert!(deleted.rows_deleted > 0);
    assert_eq!(deleted.rollups_refreshed.len(), 1);
    assert_eq!(deleted.rollups_refreshed[0].rollup_name, "lineitem_rollup");
    assert!(deleted.rollups_refreshed[0].error.is_none());

    let after = ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
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
    let reference = ref_ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
    assert_eq!(render(&after.batches), render(&reference.batches));
}

#[tokio::test]
async fn update_eagerly_refreshes_the_dependent_rollup_and_stays_cell_exact() {
    let (mut ctx, _tmp) = build_with_rollup().await;

    let before = ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
    assert_eq!(
        before.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );

    let updated = ctx
        .update_native_table(
            "UPDATE lineitem_native SET l_quantity = l_quantity + 1 WHERE l_orderkey = 5",
        )
        .await
        .expect("update must succeed");
    assert!(
        updated.rows_updated > 0,
        "the update must actually touch real rows"
    );
    assert_eq!(updated.rollups_refreshed.len(), 1);
    assert_eq!(updated.rollups_refreshed[0].rollup_name, "lineitem_rollup");
    assert!(updated.rollups_refreshed[0].error.is_none());

    let after = ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
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
    let reference = ref_ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
    assert_eq!(render(&after.batches), render(&reference.batches));
}

// ============================================================================
// Multi-rollup: one mutation must refresh ALL dependents, not just the
// first.
// ============================================================================

#[tokio::test]
async fn multiple_rollups_on_the_same_base_table_are_all_refreshed_by_one_mutation() {
    let (mut ctx, _tmp) = build_with_rollup().await;
    let reg_b = ctx
        .register_rollup(
            "lineitem_rollup_b",
            "lineitem_native",
            SECOND_ROLLUP_DEFINING_SQL,
        )
        .await
        .expect("second rollup registration must succeed");
    assert!(reg_b.rows > 0);

    // Confirm BOTH answer their own respective queries before mutation.
    let a_before = ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
    assert_eq!(
        a_before.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );
    let b_before = ctx.sql(SECOND_ROLLUP_QUERY).await.unwrap();
    assert_eq!(
        b_before.metrics.rollup_answered,
        vec!["lineitem_rollup_b".to_string()]
    );

    // ONE mutation against the shared base table.
    let deleted = ctx
        .delete_from_native_table("DELETE FROM lineitem_native WHERE l_orderkey = 1")
        .await
        .expect("delete must succeed");
    assert!(deleted.rows_deleted > 0);
    assert_eq!(
        deleted.rollups_refreshed.len(),
        2,
        "both dependent rollups must have been refreshed by the ONE mutation, not just the \
         first: {:?}",
        deleted.rollups_refreshed
    );
    let mut refreshed_names: Vec<&str> = deleted
        .rollups_refreshed
        .iter()
        .map(|o| o.rollup_name.as_str())
        .collect();
    refreshed_names.sort();
    assert_eq!(
        refreshed_names,
        vec!["lineitem_rollup", "lineitem_rollup_b"]
    );
    assert!(
        deleted.rollups_refreshed.iter().all(|o| o.error.is_none()),
        "both refreshes must have succeeded: {:?}",
        deleted.rollups_refreshed
    );

    // Both must STILL answer their own queries, each with fresh data.
    let a_after = ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
    assert_eq!(
        a_after.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );
    assert_ne!(render(&a_before.batches), render(&a_after.batches));

    let b_after = ctx.sql(SECOND_ROLLUP_QUERY).await.unwrap();
    assert_eq!(
        b_after.metrics.rollup_answered,
        vec!["lineitem_rollup_b".to_string()]
    );
    assert_ne!(render(&b_before.batches), render(&b_after.batches));

    // Cell-exact vs. an independent reference for BOTH shapes.
    let (mut ref_ctx, _tmp2) = build_lineitem_native().await;
    ref_ctx
        .delete_from_native_table("DELETE FROM lineitem_native WHERE l_orderkey = 1")
        .await
        .expect("reference delete must succeed");
    let ref_a = ref_ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
    assert_eq!(render(&a_after.batches), render(&ref_a.batches));
    let ref_b = ref_ctx.sql(SECOND_ROLLUP_QUERY).await.unwrap();
    assert_eq!(render(&b_after.batches), render(&ref_b.batches));
}

// ============================================================================
// Cost avoidance: zero dependent rollups, and a genuine no-op mutation,
// must not attempt (or pay for) any refresh.
// ============================================================================

#[tokio::test]
async fn a_mutation_with_zero_dependent_rollups_reports_none_refreshed() {
    let (mut ctx, _tmp) = build_lineitem_native().await;
    let inserted = ctx
        .insert_into_native_table(
            "INSERT INTO lineitem_native SELECT * FROM lineitem_src WHERE l_orderkey = 3",
        )
        .await
        .unwrap();
    assert!(inserted.rows_inserted > 0);
    assert!(
        inserted.rollups_refreshed.is_empty(),
        "a table with no dependent rollups must report none refreshed"
    );
}

#[tokio::test]
async fn a_no_op_mutation_does_not_attempt_to_refresh_any_rollup() {
    use query_engine::storage::native_manifest;

    let (mut ctx, tmp) = build_with_rollup().await;
    let rollup_dir = tmp.path().join("lineitem_rollup");
    let manifest_before = native_manifest::read_manifest(&rollup_dir).unwrap();

    // Matches zero rows -- a genuine no-op (mirrors
    // `native_delete_tests.rs`'s own `delete_matching_zero_rows_is_a_clean_no_op`).
    let deleted = ctx
        .delete_from_native_table("DELETE FROM lineitem_native WHERE l_orderkey = 999999999")
        .await
        .expect("no-op delete must still succeed");
    assert_eq!(deleted.rows_deleted, 0);
    assert!(
        deleted.rollups_refreshed.is_empty(),
        "a no-op mutation must not attempt to refresh any rollup -- there is nothing stale to \
         fix"
    );

    // Prove no wasted recompute actually happened: the rollup's OWN
    // manifest (version, table_id, everything) must be byte-for-byte
    // unchanged.
    let manifest_after = native_manifest::read_manifest(&rollup_dir).unwrap();
    assert_eq!(
        manifest_before, manifest_after,
        "the rollup's manifest must be completely untouched by a no-op base-table mutation"
    );
}

// ============================================================================
// Refresh failure: the base table's own mutation still succeeds, the
// rollup is left correctly stale, and matching queries fall back to the
// base table with a cell-exact answer -- never serving stale data
// silently. This is the acceptance criterion's own explicit "if refresh
// somehow fails" case (task 003, EAGER model chosen -- there is no
// "before refresh completes" window to exercise separately, since the
// mutation call itself does not return until refresh has been attempted).
// ============================================================================

#[cfg(unix)]
#[tokio::test]
async fn a_failed_refresh_leaves_the_rollup_stale_and_matching_queries_fall_back_correctly() {
    use std::os::unix::fs::PermissionsExt;
    use std::path::PathBuf;

    let (mut ctx, tmp) = build_with_rollup().await;

    let before = ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
    assert_eq!(
        before.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()]
    );

    // Warm up lineitem_native's own single-writer lock file (a no-op
    // delete still acquires it, creating it if absent) BEFORE making
    // native_table_root read-only below -- otherwise even the BASE
    // TABLE's own delete would fail at lock acquisition (the lock file
    // is a NEW sibling entry directly under native_table_root the first
    // time, which needs write permission on native_table_root itself).
    ctx.delete_from_native_table("DELETE FROM lineitem_native WHERE l_orderkey = 999999999")
        .await
        .expect("warm-up no-op delete must succeed");

    // Make native_table_root (the shared PARENT of both lineitem_native's
    // and lineitem_rollup's own directories) read-only. This specifically
    // blocks register_rollup's OWN write path (Overwrite mode stages a
    // NEW sibling staging directory directly under native_table_root
    // before atomically renaming it into place -- see native_write.rs's
    // `write_batches_with_options`), while NOT blocking the base table's
    // own DELETE: `native_delete::delete_from_native_table` only writes
    // files INSIDE the already-existing, still-writable
    // `lineitem_native/` subdirectory itself (a manifest update, atomically
    // renamed) -- and its lock file already exists (see above) -- neither
    // of which requires write permission on the PARENT directory.
    let root: PathBuf = tmp.path().to_path_buf();
    let original_perms = std::fs::metadata(&root).unwrap().permissions();

    struct RestorePerms(PathBuf, std::fs::Permissions);
    impl Drop for RestorePerms {
        fn drop(&mut self) {
            let _ = std::fs::set_permissions(&self.0, self.1.clone());
        }
    }
    let _restore = RestorePerms(root.clone(), original_perms.clone());

    let mut perms = original_perms.clone();
    perms.set_mode(0o555); // read + execute, no write
    std::fs::set_permissions(&root, perms).expect("chmod native_table_root read-only");

    let deleted = ctx
        .delete_from_native_table("DELETE FROM lineitem_native WHERE l_orderkey = 1")
        .await
        .expect(
            "the base table's own DELETE must still succeed even though the dependent \
             rollup's refresh that follows it will fail",
        );
    assert!(
        deleted.rows_deleted > 0,
        "the delete must actually remove real rows"
    );
    assert_eq!(deleted.rollups_refreshed.len(), 1);
    assert_eq!(deleted.rollups_refreshed[0].rollup_name, "lineitem_rollup");
    assert!(
        deleted.rollups_refreshed[0].error.is_some(),
        "the refresh must have FAILED (native_table_root is read-only) -- if this is None, \
         the permission-denial trick this test relies on did not actually block the write \
         (e.g. running as root), and this test is not exercising what it claims to"
    );

    // Restore write permission before querying / tempdir cleanup --
    // nothing below this point needs the read-only state.
    std::fs::set_permissions(&root, original_perms).expect("restore permissions");
    drop(_restore); // idempotent second restore; keeps the guard alive until here on purpose

    let after = ctx
        .sql(WORKED_EXAMPLE_QUERY)
        .await
        .expect("query must still succeed after a failed refresh");
    assert!(
        after.metrics.rollup_answered.is_empty(),
        "a rollup whose refresh FAILED must be left exactly as it was (still recording its \
         OLD base_table_version) and therefore correctly excluded from matching by task 001's \
         own staleness enforcement -- never silently serve the now-stale rollup data"
    );

    let (mut ref_ctx, _tmp2) = build_lineitem_native().await;
    ref_ctx
        .delete_from_native_table("DELETE FROM lineitem_native WHERE l_orderkey = 1")
        .await
        .expect("reference delete must succeed");
    let reference = ref_ctx.sql(WORKED_EXAMPLE_QUERY).await.unwrap();
    assert_eq!(
        render(&after.batches),
        render(&reference.batches),
        "the fallback answer (after a failed refresh) must be cell-exact vs. an independently \
         recomputed reference over the identically-mutated base table -- not stale, not wrong"
    );
    assert_ne!(
        render(&before.batches),
        render(&after.batches),
        "the fallback answer must actually reflect the real data change"
    );
}
