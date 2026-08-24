//! Cell-exact validation for `INSERT INTO <native table> SELECT/VALUES ...`
//! (native-tables-mutation epic, task 002). Mirrors
//! `native_table_validation.rs`'s own discipline (native-tables-foundation
//! epic, task 004) for `CREATE TABLE ... AS SELECT`, applied to the
//! `Append` write path this task adds. Exercises the REAL end-to-end path a
//! user hits: SQL text -> `ExecutionContext::insert_into_native_table` ->
//! `Binder::bind()`'s `Statement::Insert` arm -> optimize -> physical plan
//! -> streamed into `native_write::append_to_native_table` -> table
//! re-registered -> queried again through the ordinary `sql()` path.
//!
//! Requires `data/tpch-1mb` (committed fixture; CI regenerates it
//! deterministically — see CLAUDE.md's "data<->CSV coupling" note).
//! `orders.parquet` has exactly 1500 rows with `o_orderkey` densely
//! ranging 1..=1500 (confirmed directly via pyarrow before writing this
//! file, not assumed) — so `o_orderkey <= 750` / `o_orderkey > 750` is an
//! exact, always-non-empty 750/750 split, used throughout to build a
//! "CTAS wrote the first half, INSERT wrote the second half" scenario and
//! validate the reassembled table against the ORIGINAL, un-split source —
//! an independently-computed reference, per this task's own acceptance
//! criterion.

use query_engine::physical::operators::TableProvider;
use query_engine::storage::NativeTable;
use query_engine::{ExecutionContext, QueryError};

fn data_dir() -> String {
    format!("{}/data/tpch-1mb", env!("CARGO_MANIFEST_DIR"))
}

/// Cell-exact, via the same rendering on both sides — mirrors
/// `tests/native_table_validation.rs`'s own byte-identical comparison
/// idiom.
fn render(batches: &[arrow::record_batch::RecordBatch]) -> String {
    arrow::util::pretty::pretty_format_batches(batches)
        .map(|d| d.to_string())
        .unwrap_or_default()
}

/// A fresh context with `orders_src` registered from the real fixture,
/// plus a native table `orders_native` containing only the FIRST HALF
/// (`o_orderkey <= 750`) of it, written via the real CTAS SQL surface into
/// a private tempdir (so parallel test runs never share a native-table
/// root and never collide).
async fn build_half_populated_native_orders() -> (ExecutionContext, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.register_parquet("orders_src", format!("{}/orders.parquet", data_dir()))
        .expect("register source orders parquet");

    let result = ctx
        .create_table_as_select(
            "CREATE TABLE orders_native AS SELECT * FROM orders_src WHERE o_orderkey <= 750",
        )
        .await
        .expect("seed CREATE TABLE ... AS SELECT must succeed");
    assert_eq!(result.table_name, "orders_native");
    assert_eq!(result.version, 1);
    assert_eq!(
        result.rows, 750,
        "the fixture's o_orderkey split must be exactly 750/750"
    );

    (ctx, tmp)
}

#[tokio::test]
async fn insert_into_a_table_with_existing_ctas_data_matches_the_combined_source_cell_exact() {
    let (mut ctx, _tmp) = build_half_populated_native_orders().await;

    let insert_result = ctx
        .insert_into_native_table(
            "INSERT INTO orders_native SELECT * FROM orders_src WHERE o_orderkey > 750",
        )
        .await
        .expect("INSERT INTO ... SELECT must succeed");
    assert_eq!(insert_result.table_name, "orders_native");
    assert_eq!(insert_result.rows_inserted, 750);
    assert_eq!(insert_result.segments_added, 1);
    assert_eq!(insert_result.total_rows, 1500);
    assert_eq!(insert_result.version, 2, "version bumps by exactly one");

    let sql = "SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate \
               FROM {} ORDER BY o_orderkey";
    let native = ctx.sql(&sql.replace("{}", "orders_native")).await.unwrap();
    // The independently-computed reference: the SAME combined query
    // against the ORIGINAL, never-split source.
    let source = ctx.sql(&sql.replace("{}", "orders_src")).await.unwrap();

    assert_eq!(native.row_count, 1500);
    assert_eq!(native.row_count, source.row_count);
    assert_eq!(
        render(&native.batches),
        render(&source.batches),
        "post-INSERT native table must be cell-exact against the original combined source"
    );
}

#[tokio::test]
async fn insert_with_a_wildcard_select_star_source_is_positionally_correct() {
    // `SELECT * FROM orders_src` produces table-qualified field names
    // ("orders_src.o_orderkey", ...) via `Binder::bind_select_items`'s
    // `Wildcard` arm -- the exact trap task 001 flagged. The main
    // cell-exact test above already exercises this shape implicitly; this
    // test isolates it and additionally confirms the SPECIFIC columns
    // line up correctly (not just that row counts match), which a
    // positional mismatch could get wrong in a way row counts alone would
    // not catch.
    let (mut ctx, _tmp) = build_half_populated_native_orders().await;
    ctx.insert_into_native_table(
        "INSERT INTO orders_native SELECT * FROM orders_src WHERE o_orderkey > 750",
    )
    .await
    .unwrap();

    let native = ctx
        .sql("SELECT o_orderkey, o_orderstatus FROM orders_native WHERE o_orderkey = 1500")
        .await
        .unwrap();
    let source = ctx
        .sql("SELECT o_orderkey, o_orderstatus FROM orders_src WHERE o_orderkey = 1500")
        .await
        .unwrap();
    assert_eq!(native.row_count, 1);
    assert_eq!(render(&native.batches), render(&source.batches));
}

#[tokio::test]
async fn insert_of_an_empty_result_set_is_a_no_op_not_an_error() {
    let (mut ctx, _tmp) = build_half_populated_native_orders().await;
    let before = ctx.sql("SELECT COUNT(*) FROM orders_native").await.unwrap();

    let insert_result = ctx
        .insert_into_native_table(
            "INSERT INTO orders_native SELECT * FROM orders_src WHERE o_orderkey > 100000",
        )
        .await
        .expect("an empty source must not error");
    assert_eq!(insert_result.rows_inserted, 0);
    assert_eq!(insert_result.segments_added, 0);
    assert_eq!(insert_result.version, 1, "no version bump for a no-op");
    assert_eq!(insert_result.total_rows, 750);

    let after = ctx.sql("SELECT COUNT(*) FROM orders_native").await.unwrap();
    assert_eq!(
        render(&before.batches),
        render(&after.batches),
        "an empty INSERT must not change the table's visible contents"
    );
}

#[tokio::test]
async fn insert_with_a_column_count_schema_mismatch_is_a_clean_named_error_not_corruption() {
    // `customer.parquet` has 8 columns; `orders_native` has 9 -- a real,
    // naturally-reachable schema mismatch via two genuine TPC-H fixtures,
    // not a synthetic one. Column-TYPE mismatches (as opposed to count)
    // are covered exhaustively at the write-path unit-test level
    // (`src/storage/native_write.rs`'s
    // `append_refuses_a_column_type_mismatch_cleanly_and_leaves_the_manifest_intact`);
    // this integration test focuses on the count case plus proving the
    // table is genuinely untouched end to end through the real SQL surface.
    let (mut ctx, _tmp) = build_half_populated_native_orders().await;
    ctx.register_parquet("customer_src", format!("{}/customer.parquet", data_dir()))
        .expect("register customer parquet");
    let before = ctx.sql("SELECT COUNT(*) FROM orders_native").await.unwrap();

    let err = ctx
        .insert_into_native_table("INSERT INTO orders_native SELECT * FROM customer_src")
        .await
        .expect_err("a column-count schema mismatch must be a clean, named error");
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("orders_native"),
        "error must name the target table: {msg}"
    );
    assert!(
        msg.contains('8') && msg.contains('9'),
        "error must name both column counts: {msg}"
    );

    let after = ctx.sql("SELECT COUNT(*) FROM orders_native").await.unwrap();
    assert_eq!(
        render(&before.batches),
        render(&after.batches),
        "a rejected INSERT must leave the table's visible contents completely unchanged"
    );
}

#[tokio::test]
async fn insert_values_literal_rows_end_to_end() {
    use arrow::array::{Float64Array, Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    let tmp = tempfile::tempdir().unwrap();
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Float64Array::from(vec![1.5, 2.5])),
        ],
    )
    .unwrap();
    ctx.register_table("seed", schema, vec![batch]);
    ctx.create_table_as_select("CREATE TABLE t AS SELECT * FROM seed")
        .await
        .unwrap();

    // `INSERT ... VALUES` binds through the IDENTICAL path as `INSERT
    // ... SELECT` (task 001's finding: `SetExpr::Values` already has its
    // own binder arm) -- this proves it end to end, not just at bind time.
    let result = ctx
        .insert_into_native_table("INSERT INTO t VALUES (3, 'c', 3.5)")
        .await
        .expect("INSERT ... VALUES must succeed end to end");
    assert_eq!(result.rows_inserted, 1);
    assert_eq!(result.total_rows, 3);

    let after = ctx
        .sql("SELECT id, name, price FROM t ORDER BY id")
        .await
        .unwrap();
    assert_eq!(after.row_count, 3);
    let ids = after.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    assert_eq!(ids.values(), &[1, 2, 3]);
}

#[tokio::test]
async fn statistics_and_distributed_splits_reflect_post_insert_state() {
    let (mut ctx, _tmp) = build_half_populated_native_orders().await;

    let before_provider = ctx.table_provider("orders_native").expect("registered");
    let before = before_provider
        .as_any()
        .downcast_ref::<NativeTable>()
        .expect("orders_native is a NativeTable");
    let before_stats = before
        .statistics()
        .expect("native tables report statistics");
    assert_eq!(before_stats.row_count, 750);
    let before_orderkey = before_stats
        .column_stats
        .get("o_orderkey")
        .expect("o_orderkey stats present");
    assert_eq!(before_orderkey.min_i64, Some(1));
    assert_eq!(before_orderkey.max_i64, Some(750));
    let before_splits = before
        .distributed_splits("orders_native", 1)
        .expect("native tables implement distributed_splits")
        .expect("enumeration succeeds");
    assert_eq!(
        before_splits.splits.len(),
        1,
        "one split per segment, 1 segment so far"
    );
    assert_eq!(before_splits.total_rows, 750);

    ctx.insert_into_native_table(
        "INSERT INTO orders_native SELECT * FROM orders_src WHERE o_orderkey > 750",
    )
    .await
    .unwrap();

    // `insert_into_native_table` re-registers the table after writing
    // (mirrors `create_table_as_select`'s own "make the write immediately
    // queryable" step) -- fetch a FRESH provider handle, exactly like a
    // new query in the same session would see, rather than reusing the
    // stale `before_provider` handle (which still points at the
    // pre-insert manifest snapshot it was opened from).
    let after_provider = ctx.table_provider("orders_native").expect("registered");
    let after = after_provider
        .as_any()
        .downcast_ref::<NativeTable>()
        .expect("orders_native is still a NativeTable");
    let after_stats = after.statistics().expect("native tables report statistics");
    assert_eq!(
        after_stats.row_count, 1500,
        "row count must include the new segment"
    );
    assert!(
        after_stats.total_byte_size > before_stats.total_byte_size,
        "on-disk byte total must grow"
    );
    let after_orderkey = after_stats
        .column_stats
        .get("o_orderkey")
        .expect("o_orderkey stats present");
    assert_eq!(after_orderkey.min_i64, Some(1), "min must not regress");
    assert_eq!(
        after_orderkey.max_i64,
        Some(1500),
        "max must widen to include the new segment"
    );

    let after_splits = after
        .distributed_splits("orders_native", 1)
        .expect("native tables implement distributed_splits")
        .expect("enumeration succeeds");
    assert_eq!(
        after_splits.splits.len(),
        2,
        "one split per segment, now 2 segments"
    );
    assert_eq!(after_splits.total_rows, 1500);
    assert_eq!(
        after_splits.splits.iter().map(|s| s.num_rows).sum::<i64>(),
        1500
    );
}

#[tokio::test]
async fn sql_refuses_insert_and_points_at_insert_into_native_table() {
    let (ctx, _tmp) = build_half_populated_native_orders().await;
    let err = ctx
        .sql("INSERT INTO orders_native SELECT * FROM orders_src WHERE o_orderkey > 750")
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::InvalidArgument(_)), "{err:?}");
    assert!(
        err.to_string().contains("insert_into_native_table"),
        "{err}"
    );
}

#[tokio::test]
async fn insert_into_an_unregistered_table_is_a_clean_table_not_found_error() {
    let tmp = tempfile::tempdir().unwrap();
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    let err = ctx
        .insert_into_native_table("INSERT INTO nope VALUES (1)")
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::TableNotFound(_)), "{err:?}");
}

#[tokio::test]
async fn insert_into_a_non_native_table_is_a_clean_invalid_argument_error() {
    let tmp = tempfile::tempdir().unwrap();
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.register_parquet("orders_src", format!("{}/orders.parquet", data_dir()))
        .unwrap();

    let err = ctx
        .insert_into_native_table("INSERT INTO orders_src SELECT * FROM orders_src LIMIT 1")
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::InvalidArgument(_)), "{err:?}");
    assert!(err.to_string().contains("orders_src"), "{err}");
}
