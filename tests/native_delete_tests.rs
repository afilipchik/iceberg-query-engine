//! Cell-exact validation for `DELETE FROM <native table> [WHERE ...]`
//! (native-tables-mutation epic, task 003). Mirrors
//! `native_insert_tests.rs`'s own discipline (task 002) applied to the
//! deletion-vector mechanism this task adds. Exercises the REAL end-to-end
//! path a user hits: SQL text -> `ExecutionContext::delete_from_native_table`
//! -> `Binder::bind_delete` -> `native_delete::identify_matching_rows` /
//! `apply_deletions` -> `native_write::publish_manifest_update` -> table
//! re-registered -> queried again through the ordinary `sql()` path.
//!
//! Requires `data/tpch-1mb` (committed fixture; CI regenerates it
//! deterministically). `orders.parquet` has exactly 1500 rows with
//! `o_orderkey` densely ranging 1..=1500 (confirmed directly via pyarrow,
//! also relied on by `native_insert_tests.rs`) — used throughout to build
//! predictable, always-non-empty splits and compare against an
//! independently-computed reference (the complementary predicate against
//! the ORIGINAL, un-deleted-from source).

use query_engine::physical::operators::TableProvider;
use query_engine::storage::NativeTable;
use query_engine::{ExecutionContext, QueryError};

fn data_dir() -> String {
    format!("{}/data/tpch-1mb", env!("CARGO_MANIFEST_DIR"))
}

/// Cell-exact, via the same rendering on both sides — mirrors
/// `native_insert_tests.rs`'s own byte-identical comparison idiom.
fn render(batches: &[arrow::record_batch::RecordBatch]) -> String {
    arrow::util::pretty::pretty_format_batches(batches)
        .map(|d| d.to_string())
        .unwrap_or_default()
}

/// A fresh context with `orders_src` registered from the real fixture,
/// plus a native table `orders_native` containing ALL 1500 rows (a single
/// segment), written via the real CTAS SQL surface into a private tempdir.
async fn build_full_native_orders() -> (ExecutionContext, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.register_parquet("orders_src", format!("{}/orders.parquet", data_dir()))
        .expect("register source orders parquet");

    let result = ctx
        .create_table_as_select("CREATE TABLE orders_native AS SELECT * FROM orders_src")
        .await
        .expect("seed CREATE TABLE ... AS SELECT must succeed");
    assert_eq!(result.rows, 1500);
    assert_eq!(result.segments, 1);

    (ctx, tmp)
}

/// A fresh context with `orders_src` registered, plus a native table
/// `orders_native` built from TWO segments (CTAS wrote the first half,
/// INSERT wrote the second half — the same pattern
/// `native_insert_tests.rs` uses to construct a real multi-segment table).
async fn build_two_segment_native_orders() -> (ExecutionContext, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.register_parquet("orders_src", format!("{}/orders.parquet", data_dir()))
        .expect("register source orders parquet");

    ctx.create_table_as_select(
        "CREATE TABLE orders_native AS SELECT * FROM orders_src WHERE o_orderkey <= 750",
    )
    .await
    .expect("seed CREATE TABLE ... AS SELECT must succeed");
    let insert_result = ctx
        .insert_into_native_table(
            "INSERT INTO orders_native SELECT * FROM orders_src WHERE o_orderkey > 750",
        )
        .await
        .expect("seed INSERT must succeed");
    assert_eq!(insert_result.total_rows, 1500);
    assert_eq!(
        insert_result.segments_added, 1,
        "the INSERT adds exactly 1 new segment -- CTAS's first segment + this one = 2 total"
    );

    (ctx, tmp)
}

async fn count_of(ctx: &ExecutionContext, table: &str) -> i64 {
    let r = ctx
        .sql(&format!("SELECT COUNT(*) AS c FROM {table}"))
        .await
        .unwrap();
    r.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn delete_a_subset_matches_the_independently_computed_complement_cell_exact() {
    let (mut ctx, _tmp) = build_full_native_orders().await;

    let result = ctx
        .delete_from_native_table("DELETE FROM orders_native WHERE o_orderkey <= 500")
        .await
        .expect("DELETE must succeed");
    assert_eq!(result.table_name, "orders_native");
    assert_eq!(result.rows_deleted, 500);
    assert_eq!(
        result.segments_dropped, 0,
        "the single segment is only partially deleted"
    );
    assert_eq!(result.total_rows, 1000);
    assert_eq!(result.version, 2, "version bumps by exactly one");

    let sql = "SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate \
               FROM {} ORDER BY o_orderkey";
    let native = ctx.sql(&sql.replace("{}", "orders_native")).await.unwrap();
    // The independently-computed reference: the complementary predicate
    // against the ORIGINAL, un-deleted-from source.
    let source = ctx
        .sql(
            &sql.replace("{}", "orders_src")
                .replace("FROM orders_src", "FROM orders_src WHERE o_orderkey > 500"),
        )
        .await
        .unwrap();

    assert_eq!(native.row_count, 1000);
    assert_eq!(native.row_count, source.row_count);
    assert_eq!(
        render(&native.batches),
        render(&source.batches),
        "post-DELETE native table must be cell-exact against the independently-computed \
         complement of the original source"
    );
}

#[tokio::test]
async fn delete_all_rows_leaves_the_table_existing_but_logically_empty() {
    let (mut ctx, _tmp) = build_full_native_orders().await;

    let result = ctx
        .delete_from_native_table("DELETE FROM orders_native")
        .await
        .expect("DELETE with no WHERE must delete every row, not error");
    assert_eq!(result.rows_deleted, 1500);
    assert_eq!(result.total_rows, 0);
    assert_eq!(
        result.segments_dropped, 1,
        "the wholly-tombstoned segment must be dropped"
    );

    // Subsequent queries against the (still-registered, still-existing)
    // table must return zero rows correctly, not error.
    assert_eq!(count_of(&ctx, "orders_native").await, 0);
    let all = ctx.sql("SELECT * FROM orders_native").await.unwrap();
    assert_eq!(all.row_count, 0);
    assert!(
        all.batches.iter().all(|b| b.num_rows() == 0),
        "every returned batch must be zero rows, not absent/erroring"
    );
}

#[tokio::test]
async fn delete_matching_zero_rows_is_a_clean_no_op() {
    let (mut ctx, _tmp) = build_full_native_orders().await;
    let before = ctx
        .sql("SELECT * FROM orders_native ORDER BY o_orderkey")
        .await
        .unwrap();

    let result = ctx
        .delete_from_native_table("DELETE FROM orders_native WHERE o_orderkey > 100000")
        .await
        .expect("a predicate matching nothing must not error");
    assert_eq!(result.rows_deleted, 0);
    assert_eq!(result.segments_dropped, 0);
    assert_eq!(result.version, 1, "no version bump for a no-op delete");
    assert_eq!(result.total_rows, 1500);

    let after = ctx
        .sql("SELECT * FROM orders_native ORDER BY o_orderkey")
        .await
        .unwrap();
    assert_eq!(
        render(&before.batches),
        render(&after.batches),
        "a zero-match DELETE must not change the table's visible contents at all"
    );
}

#[tokio::test]
async fn delete_from_a_multi_segment_table_applies_to_the_correct_segments_only() {
    let (mut ctx, _tmp) = build_two_segment_native_orders().await;

    // This predicate spans the segment boundary (segment 0: <=750,
    // segment 1: >750) -- a real, cross-segment DELETE, not a
    // single-segment coincidence.
    let result = ctx
        .delete_from_native_table("DELETE FROM orders_native WHERE o_orderkey BETWEEN 700 AND 800")
        .await
        .expect("cross-segment DELETE must succeed");
    assert_eq!(result.rows_deleted, 101); // 700..=800 inclusive
    assert_eq!(result.total_rows, 1399);

    let sql = "SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate \
               FROM {} ORDER BY o_orderkey";
    let native = ctx.sql(&sql.replace("{}", "orders_native")).await.unwrap();
    let source = ctx
        .sql(&sql.replace(
            "{}",
            "orders_src WHERE o_orderkey < 700 OR o_orderkey > 800",
        ))
        .await
        .unwrap();
    assert_eq!(native.row_count, source.row_count);
    assert_eq!(
        render(&native.batches),
        render(&source.batches),
        "deletions must apply to the correct rows regardless of which segment they fall in"
    );
}

#[tokio::test]
async fn repeated_overlapping_deletes_are_idempotent_and_never_double_count() {
    let (mut ctx, _tmp) = build_full_native_orders().await;

    let first = ctx
        .delete_from_native_table("DELETE FROM orders_native WHERE o_orderkey <= 600")
        .await
        .unwrap();
    assert_eq!(first.rows_deleted, 600);
    assert_eq!(first.total_rows, 900);

    // Overlapping: o_orderkey <= 300 is entirely already-deleted.
    let second = ctx
        .delete_from_native_table("DELETE FROM orders_native WHERE o_orderkey <= 300")
        .await
        .unwrap();
    assert_eq!(
        second.rows_deleted, 0,
        "a fully-redundant repeat DELETE must report zero NEWLY-deleted rows, not the gross \
         match count"
    );
    assert_eq!(
        second.version, first.version,
        "a fully-redundant repeat DELETE must be a true no-op -- no version bump"
    );
    assert_eq!(
        second.total_rows, 900,
        "re-deleting an already-deleted range must not change the visible row count further"
    );

    // A third delete with a predicate that's HALF already-deleted, half new.
    let third = ctx
        .delete_from_native_table("DELETE FROM orders_native WHERE o_orderkey <= 700")
        .await
        .unwrap();
    assert_eq!(
        third.rows_deleted, 100,
        "only the genuinely NEW matches (601..=700) should count as deleted this round"
    );
    assert_eq!(third.total_rows, 800);

    let native = ctx
        .sql("SELECT o_orderkey FROM orders_native ORDER BY o_orderkey")
        .await
        .unwrap();
    let source = ctx
        .sql("SELECT o_orderkey FROM orders_src WHERE o_orderkey > 700 ORDER BY o_orderkey")
        .await
        .unwrap();
    assert_eq!(
        render(&native.batches),
        render(&source.batches),
        "the final state must be exactly as if a single DELETE WHERE o_orderkey <= 700 ran"
    );
}

#[tokio::test]
async fn statistics_reflect_post_delete_row_count() {
    let (mut ctx, _tmp) = build_full_native_orders().await;
    let before_provider = ctx.table_provider("orders_native").expect("registered");
    let before = before_provider
        .as_any()
        .downcast_ref::<NativeTable>()
        .expect("orders_native is a NativeTable");
    let before_stats = before
        .statistics()
        .expect("native tables report statistics");
    assert_eq!(before_stats.row_count, 1500);

    ctx.delete_from_native_table("DELETE FROM orders_native WHERE o_orderkey <= 500")
        .await
        .unwrap();

    // Fetch a FRESH provider handle -- `delete_from_native_table`
    // re-registers the table (mirrors INSERT's own "make it immediately
    // queryable" step).
    let after_provider = ctx.table_provider("orders_native").expect("registered");
    let after = after_provider
        .as_any()
        .downcast_ref::<NativeTable>()
        .expect("orders_native is still a NativeTable");
    let after_stats = after.statistics().expect("native tables report statistics");
    assert_eq!(
        after_stats.row_count, 1000,
        "statistics() row_count must reflect the post-delete LOGICAL count, not the stale \
         pre-delete rollup -- a stale count could mislead the planner (e.g. join ordering)"
    );
}

#[tokio::test]
async fn sql_refuses_delete_and_points_at_delete_from_native_table() {
    let (ctx, _tmp) = build_full_native_orders().await;
    let err = ctx
        .sql("DELETE FROM orders_native WHERE o_orderkey <= 500")
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::InvalidArgument(_)), "{err:?}");
    assert!(
        err.to_string().contains("delete_from_native_table"),
        "{err}"
    );
}

#[tokio::test]
async fn delete_from_an_unregistered_table_is_a_clean_table_not_found_error() {
    let tmp = tempfile::tempdir().unwrap();
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    let err = ctx
        .delete_from_native_table("DELETE FROM nope WHERE x = 1")
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::TableNotFound(_)), "{err:?}");
}

#[tokio::test]
async fn delete_from_a_non_native_table_is_a_clean_invalid_argument_error() {
    let tmp = tempfile::tempdir().unwrap();
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.register_parquet("orders_src", format!("{}/orders.parquet", data_dir()))
        .unwrap();

    let err = ctx
        .delete_from_native_table("DELETE FROM orders_src WHERE o_orderkey = 1")
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::InvalidArgument(_)), "{err:?}");
    assert!(err.to_string().contains("orders_src"), "{err}");
}

#[tokio::test]
async fn no_regression_insert_and_ctas_still_work_after_a_delete_in_the_same_session() {
    // A DELETE must not corrupt the table for subsequent, unrelated
    // mutations against it -- INSERT must still work correctly afterward.
    let (mut ctx, _tmp) = build_full_native_orders().await;
    ctx.delete_from_native_table("DELETE FROM orders_native WHERE o_orderkey <= 1000")
        .await
        .unwrap();
    assert_eq!(count_of(&ctx, "orders_native").await, 500);

    let insert_result = ctx
        .insert_into_native_table(
            "INSERT INTO orders_native SELECT * FROM orders_src WHERE o_orderkey <= 100",
        )
        .await
        .expect("INSERT after a DELETE must still work");
    assert_eq!(insert_result.rows_inserted, 100);
    assert_eq!(count_of(&ctx, "orders_native").await, 600);

    // And a completely independent CREATE TABLE ... AS SELECT in the same
    // session must be unaffected.
    let ctas_result = ctx
        .create_table_as_select("CREATE TABLE another AS SELECT * FROM orders_src LIMIT 10")
        .await
        .expect("CTAS must still work in a session that has performed a DELETE");
    assert_eq!(ctas_result.rows, 10);
}
