//! Cell-exact validation for native tables (native-tables-foundation epic,
//! task 004): a table written via `CREATE TABLE ... AS SELECT` from an
//! existing Parquet source must answer identically to querying the source
//! directly — not just matching row counts — across several query shapes
//! (plain scan, filter, aggregate, join). Exercises the REAL end-to-end
//! path a user hits: SQL text -> `ExecutionContext::create_table_as_select`
//! -> `Binder::bind()`'s `CreateTable` arm -> optimize -> physical plan ->
//! streamed into `native_write::write_batches` -> `NativeTable` registered
//! -> queried again through the ordinary `sql()` path.
//!
//! Also covers task 005 (dense-direct-address fast-path compatibility):
//! `native_table_matches_source_on_a_dense_direct_group_by_cell_exact`
//! exercises `MorselAggregateExec`'s native-mode routing
//! (`try_execute_dense_direct` reading key bounds from
//! `TableProvider::statistics()` instead of parquet footers), and
//! `native_table_matches_source_on_a_filtered_group_by_cell_exact` pins
//! that a filtered GROUP BY correctly stays OFF that path (no fallback
//! tier exists once native mode is selected, so misrouting there would be
//! a hard error, not just a slow query). A real large-scale (SF=1,
//! 6,000,000-row `lineitem`) measurement confirming the fast path actually
//! fires, via `AGG_TIMING=1`, lives in
//! `examples/native_dense_direct_check.rs` rather than as a `#[test]`
//! (stderr-diagnostic output isn't a natural fit for the test harness).
//!
//! Requires `data/tpch-1mb` (committed fixture; CI regenerates it
//! deterministically — see CLAUDE.md's "data<->CSV coupling" note).

use query_engine::ExecutionContext;

fn data_dir() -> String {
    format!("{}/data/tpch-1mb", env!("CARGO_MANIFEST_DIR"))
}

/// Cell-exact, via the same rendering on both sides — mirrors
/// `tests/distributed_cluster.rs`'s own byte-identical comparison idiom.
fn render(batches: &[arrow::record_batch::RecordBatch]) -> String {
    arrow::util::pretty::pretty_format_batches(batches)
        .map(|d| d.to_string())
        .unwrap_or_default()
}

/// A fresh context with `orders`/`customer`/`lineitem` registered from the
/// real Parquet fixture under `*_src` names, plus a native copy of `orders`
/// (`orders_native`) written via the actual `CREATE TABLE ... AS SELECT`
/// SQL surface into a private tempdir (so parallel test runs never share a
/// native-table root and never collide).
async fn build_native_orders_table() -> (ExecutionContext, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());

    ctx.register_parquet("orders_src", format!("{}/orders.parquet", data_dir()))
        .expect("register source orders parquet");
    ctx.register_parquet("customer_src", format!("{}/customer.parquet", data_dir()))
        .expect("register source customer parquet");

    let result = ctx
        .create_table_as_select("CREATE TABLE orders_native AS SELECT * FROM orders_src")
        .await
        .expect("CREATE TABLE ... AS SELECT must succeed");
    assert_eq!(result.table_name, "orders_native");
    assert_eq!(result.version, 1, "first write is snapshot version 1");
    assert!(result.rows > 0, "source table must not be empty");
    assert!(result.segments > 0);

    (ctx, tmp)
}

#[tokio::test]
async fn ctas_creates_a_table_immediately_queryable_in_the_same_session() {
    let (ctx, _tmp) = build_native_orders_table().await;
    assert!(ctx.table_names().contains(&"orders_native".to_string()));
    let r = ctx.sql("SELECT COUNT(*) FROM orders_native").await.unwrap();
    assert_eq!(r.row_count, 1);
}

#[tokio::test]
async fn native_table_matches_source_on_a_plain_scan_cell_exact() {
    let (ctx, _tmp) = build_native_orders_table().await;
    let sql = "SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate \
               FROM {} ORDER BY o_orderkey";
    let src = ctx
        .sql(&sql.replace("{}", "orders_src"))
        .await
        .expect("source query");
    let native = ctx
        .sql(&sql.replace("{}", "orders_native"))
        .await
        .expect("native query");
    assert_eq!(src.row_count, native.row_count);
    assert!(src.row_count > 0);
    assert_eq!(render(&src.batches), render(&native.batches));
}

#[tokio::test]
async fn native_table_matches_source_on_a_filter_cell_exact() {
    let (ctx, _tmp) = build_native_orders_table().await;
    let sql = "SELECT o_orderkey, o_totalprice, o_orderdate FROM {} \
               WHERE o_orderstatus = 'F' AND o_totalprice > 100000 ORDER BY o_orderkey";
    let src = ctx
        .sql(&sql.replace("{}", "orders_src"))
        .await
        .expect("source query");
    let native = ctx
        .sql(&sql.replace("{}", "orders_native"))
        .await
        .expect("native query");
    assert!(src.row_count > 0, "filter must actually select something");
    assert_eq!(src.row_count, native.row_count);
    assert_eq!(render(&src.batches), render(&native.batches));
}

#[tokio::test]
async fn native_table_matches_source_on_an_aggregate_cell_exact() {
    let (ctx, _tmp) = build_native_orders_table().await;
    let sql = "SELECT o_orderstatus, COUNT(*) AS n, SUM(o_totalprice) AS total, \
               AVG(o_totalprice) AS avg_price FROM {} GROUP BY o_orderstatus \
               ORDER BY o_orderstatus";
    let src = ctx
        .sql(&sql.replace("{}", "orders_src"))
        .await
        .expect("source query");
    let native = ctx
        .sql(&sql.replace("{}", "orders_native"))
        .await
        .expect("native query");
    assert_eq!(src.row_count, native.row_count);
    assert!(src.row_count > 0);
    assert_eq!(render(&src.batches), render(&native.batches));
}

/// Task 005 (dense-direct-address fast-path compatibility): a GROUP BY
/// with a WHERE clause must NOT take the native dense-direct path
/// (`try_extract_native_dense_source` refuses any scan-level filter, since
/// `NativeTable::scan_with_filter` has no pushdown and
/// `try_execute_dense_direct` never re-evaluates one) — it must keep using
/// the ordinary generic aggregate tier, exactly as it did before this task.
/// Otherwise-identical shape to the dense-direct test above (single Int64
/// group column, plain COUNT/SUM/AVG) specifically to isolate the filter as
/// the only variable.
#[tokio::test]
async fn native_table_matches_source_on_a_filtered_group_by_cell_exact() {
    let (ctx, _tmp) = build_native_orders_table().await;
    let sql = "SELECT o_orderkey, COUNT(*) AS n, SUM(o_totalprice) AS total \
               FROM {} WHERE o_totalprice > 100000 GROUP BY o_orderkey \
               ORDER BY o_orderkey";
    let src = ctx
        .sql(&sql.replace("{}", "orders_src"))
        .await
        .expect("source query");
    let native = ctx
        .sql(&sql.replace("{}", "orders_native"))
        .await
        .expect("native query");
    assert!(src.row_count > 0, "filter must actually select something");
    assert_eq!(src.row_count, native.row_count);
    assert_eq!(render(&src.batches), render(&native.batches));
}

/// Task 005 (dense-direct-address fast-path compatibility): GROUP BY a
/// single plain Int64 column (`o_orderkey`, the orders primary key — every
/// row its own group, exactly the "single int/date group key, large group
/// count" shape the dense-direct path targets) with plain COUNT/SUM/AVG
/// aggregates is exactly the shape `try_execute_dense_direct` accepts.
/// Cell-exact against the same query over the Parquet source proves BOTH
/// that the result is correct AND that routing didn't silently fall
/// through to `MorselAggregateExec`'s native-mode error path (`execute()`'s
/// safety net) — either would show up here, one as a wrong answer, the
/// other as a hard `Err` that `.expect()` below turns into a panic.
#[tokio::test]
async fn native_table_matches_source_on_a_dense_direct_group_by_cell_exact() {
    let (ctx, _tmp) = build_native_orders_table().await;
    let sql = "SELECT o_orderkey, COUNT(*) AS n, SUM(o_totalprice) AS total, \
               AVG(o_totalprice) AS avg_price FROM {} GROUP BY o_orderkey \
               ORDER BY o_orderkey";
    let src = ctx
        .sql(&sql.replace("{}", "orders_src"))
        .await
        .expect("source query");
    let native = ctx
        .sql(&sql.replace("{}", "orders_native"))
        .await
        .expect("native query");
    assert_eq!(src.row_count, native.row_count);
    assert!(src.row_count > 0);
    assert_eq!(render(&src.batches), render(&native.batches));
}

#[tokio::test]
async fn native_table_matches_source_on_a_join_cell_exact() {
    let (ctx, _tmp) = build_native_orders_table().await;
    let sql = "SELECT o.o_orderkey, o.o_totalprice, c.c_name FROM {} o \
               JOIN customer_src c ON o.o_custkey = c.c_custkey \
               WHERE o.o_totalprice > 200000 ORDER BY o.o_orderkey";
    let src = ctx
        .sql(&sql.replace("{}", "orders_src"))
        .await
        .expect("source join");
    let native = ctx
        .sql(&sql.replace("{}", "orders_native"))
        .await
        .expect("native join");
    assert!(
        src.row_count > 0,
        "join+filter must actually select something"
    );
    assert_eq!(src.row_count, native.row_count);
    assert_eq!(render(&src.batches), render(&native.batches));
}

#[tokio::test]
async fn native_table_statistics_row_count_matches_a_full_scan() {
    let (ctx, _tmp) = build_native_orders_table().await;
    let provider = ctx
        .table_provider("orders_native")
        .expect("orders_native is registered");
    let stats = provider
        .statistics()
        .expect("native tables report statistics, never None");
    let full = ctx.sql("SELECT * FROM orders_src").await.unwrap();
    assert_eq!(stats.row_count, full.row_count);
    assert!(stats.total_byte_size > 0);
}

#[tokio::test]
async fn create_table_as_select_replaces_an_existing_native_table_wholesale() {
    let (mut ctx, _tmp) = build_native_orders_table().await;

    // Recreate under the SAME name with a filtered subset — must overwrite
    // (bump snapshot.version), not append and not fail because the
    // directory already exists.
    let result = ctx
        .create_table_as_select(
            "CREATE TABLE orders_native AS SELECT * FROM orders_src WHERE o_orderstatus = 'F'",
        )
        .await
        .expect("re-CREATE TABLE must overwrite, not fail");
    assert_eq!(result.version, 2, "second write bumps the snapshot version");

    let sql = "SELECT COUNT(*) FROM {}";
    let after = ctx.sql(&sql.replace("{}", "orders_native")).await.unwrap();
    let expected = ctx
        .sql("SELECT COUNT(*) FROM orders_src WHERE o_orderstatus = 'F'")
        .await
        .unwrap();
    assert_eq!(render(&after.batches), render(&expected.batches));
}

#[tokio::test]
async fn columns_only_create_table_is_refused_by_name() {
    let (mut ctx, _tmp) = build_native_orders_table().await;
    let err = ctx
        .create_table_as_select("CREATE TABLE t2 (a INT, b VARCHAR(50))")
        .await
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.to_uppercase().contains("AS SELECT"),
        "error should explain the columns-only form isn't supported: {msg}"
    );
}

#[tokio::test]
async fn unsupported_create_table_clauses_are_refused_by_name() {
    let (mut ctx, _tmp) = build_native_orders_table().await;
    let err = ctx
        .create_table_as_select("CREATE TEMPORARY TABLE t3 AS SELECT * FROM orders_src")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("TEMPORARY"),
        "error should name the refused clause: {err}"
    );
}

#[tokio::test]
async fn sql_refuses_create_table_and_points_at_the_ddl_entrypoint() {
    let (ctx, _tmp) = build_native_orders_table().await;
    let err = ctx
        .sql("CREATE TABLE t4 AS SELECT * FROM orders_src")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("create_table_as_select"),
        "sql() must point callers at the real DDL entrypoint: {err}"
    );
}
