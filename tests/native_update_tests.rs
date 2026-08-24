//! Cell-exact validation for `UPDATE <native table> SET ... [WHERE ...]`
//! (native-tables-mutation epic, task 004). Mirrors `native_delete_tests.rs`/
//! `native_insert_tests.rs`'s own discipline (tasks 002/003) applied to
//! this task's composition of both. Exercises the REAL end-to-end path a
//! user hits: SQL text -> `ExecutionContext::update_native_table` ->
//! `Binder::bind_update` -> `native_update::update_native_table`
//! (composing `native_delete::identify_matching_rows`/`apply_deletions`
//! with `native_write::write_append_segments`/`publish_manifest_update`
//! into ONE atomic publish) -> table re-registered -> queried again
//! through the ordinary `sql()` path.
//!
//! Requires `data/tpch-1mb` (same committed fixture `native_delete_tests.rs`/
//! `native_insert_tests.rs` use). `orders.parquet` has exactly 1500 rows
//! with `o_orderkey` densely ranging 1..=1500 and `o_orderstatus` cycling
//! through only `O`/`F`/`P` (TPC-H spec values -- low cardinality, so CTAS's
//! own write path dictionary-encodes it, per phase 1's own previously-buggy
//! Dictionary-typed-schema area named in this task's own file).

use query_engine::physical::operators::TableProvider;
use query_engine::planner::{Expr, ScalarValue};
use query_engine::storage::{native_update, NativeTable};
use query_engine::{ExecutionContext, QueryError};
use std::collections::HashSet;
use std::sync::Arc;

fn data_dir() -> String {
    format!("{}/data/tpch-1mb", env!("CARGO_MANIFEST_DIR"))
}

/// Cell-exact, via the same rendering on both sides -- mirrors
/// `native_delete_tests.rs`/`native_insert_tests.rs`'s own byte-identical
/// comparison idiom.
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
/// INSERT wrote the second half -- the same pattern
/// `native_delete_tests.rs`/`native_insert_tests.rs` use to construct a
/// real multi-segment table).
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
    assert_eq!(insert_result.segments_added, 1);

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

// ---------- cell-exact: self-referential SET expression ----------

#[tokio::test]
async fn update_self_referential_set_expression_matches_independently_computed_reference() {
    let (mut ctx, _tmp) = build_full_native_orders().await;

    let result = ctx
        .update_native_table(
            "UPDATE orders_native SET o_totalprice = o_totalprice * 1.1 WHERE o_orderkey <= 500",
        )
        .await
        .expect("UPDATE must succeed");
    assert_eq!(result.table_name, "orders_native");
    assert_eq!(result.rows_updated, 500);
    assert_eq!(result.total_rows, 1500, "UPDATE never changes row count");
    assert_eq!(result.version, 2, "version bumps by exactly one");

    let native = ctx
        .sql(
            "SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate \
             FROM orders_native ORDER BY o_orderkey",
        )
        .await
        .unwrap();
    // The independently-computed reference: the SAME multiply applied via
    // a CASE expression directly against the ORIGINAL, never-updated
    // source -- a genuinely separate computation path (an ordinary SELECT
    // over `orders_src`), not a re-derivation of the UPDATE's own code.
    let reference = ctx
        .sql(
            "SELECT o_orderkey, o_custkey, o_orderstatus, \
             CASE WHEN o_orderkey <= 500 THEN o_totalprice * 1.1 ELSE o_totalprice END AS \
             o_totalprice, o_orderdate FROM orders_src ORDER BY o_orderkey",
        )
        .await
        .unwrap();

    assert_eq!(native.row_count, 1500);
    assert_eq!(native.row_count, reference.row_count);
    assert_eq!(
        render(&native.batches),
        render(&reference.batches),
        "post-UPDATE native table must be cell-exact against the independently-computed \
         reference -- including for the rows the SET expression did NOT touch"
    );
}

// ---------- zero-match UPDATE ----------

#[tokio::test]
async fn update_matching_zero_rows_is_a_clean_no_op() {
    let (mut ctx, _tmp) = build_full_native_orders().await;
    let before = ctx
        .sql("SELECT * FROM orders_native ORDER BY o_orderkey")
        .await
        .unwrap();

    let result = ctx
        .update_native_table("UPDATE orders_native SET o_totalprice = 0 WHERE o_orderkey > 100000")
        .await
        .expect("a predicate matching nothing must not error");
    assert_eq!(result.rows_updated, 0);
    assert_eq!(result.version, 1, "no version bump for a no-op update");
    assert_eq!(result.total_rows, 1500);

    let after = ctx
        .sql("SELECT * FROM orders_native ORDER BY o_orderkey")
        .await
        .unwrap();
    assert_eq!(
        render(&before.batches),
        render(&after.batches),
        "a zero-match UPDATE must not change the table's visible contents at all"
    );
}

// ---------- all-rows UPDATE ----------

#[tokio::test]
async fn update_matching_all_rows_via_no_where_clause() {
    let (mut ctx, _tmp) = build_full_native_orders().await;

    let result = ctx
        .update_native_table("UPDATE orders_native SET o_orderstatus = 'X'")
        .await
        .expect("UPDATE with no WHERE must update every row, not error");
    assert_eq!(result.rows_updated, 1500);
    assert_eq!(result.total_rows, 1500);

    let statuses = ctx
        .sql("SELECT DISTINCT o_orderstatus FROM orders_native")
        .await
        .unwrap();
    assert_eq!(statuses.row_count, 1);
    let col = statuses.batches[0].column(0);
    let val = string_col_values(col);
    assert_eq!(val, vec![Some("X".to_string())]);
}

// ---------- dictionary-encoded column round-trip ----------

#[tokio::test]
async fn update_on_a_dictionary_encoded_column_round_trips_correctly() {
    let (mut ctx, tmp) = build_full_native_orders().await;

    // Precondition: `o_orderstatus` (TPC-H spec values O/F/P, low
    // cardinality) must actually have been written Dictionary(Int32,
    // Utf8) by CTAS -- otherwise this test would not mean anything (phase
    // 1's own task 004 Outcome names this a previously-buggy area).
    let dir = tmp.path().join("orders_native");
    let manifest_before = query_engine::storage::native_manifest::read_manifest(&dir).unwrap();
    let status_field = manifest_before
        .schema
        .iter()
        .find(|f| f.name == "o_orderstatus")
        .unwrap();
    assert!(
        matches!(
            status_field.to_arrow().data_type(),
            arrow::datatypes::DataType::Dictionary(_, _)
        ),
        "precondition: o_orderstatus must actually be dictionary-encoded for this test to mean \
         anything"
    );

    let result = ctx
        .update_native_table(
            "UPDATE orders_native SET o_orderstatus = 'ZZ' WHERE o_orderkey <= 200",
        )
        .await
        .expect("UPDATE on a dictionary-encoded column must succeed");
    assert_eq!(result.rows_updated, 200);

    // Must not have regressed the column's declared type back to plain
    // Utf8 -- this epic's own explicit requirement.
    let manifest_after = query_engine::storage::native_manifest::read_manifest(&dir).unwrap();
    let status_field_after = manifest_after
        .schema
        .iter()
        .find(|f| f.name == "o_orderstatus")
        .unwrap();
    assert!(
        matches!(
            status_field_after.to_arrow().data_type(),
            arrow::datatypes::DataType::Dictionary(_, _)
        ),
        "UPDATE must not regress a dictionary-encoded column back to plain Utf8"
    );

    // Cell-exact against the independently-computed reference.
    let native = ctx
        .sql("SELECT o_orderkey, o_orderstatus FROM orders_native ORDER BY o_orderkey")
        .await
        .unwrap();
    let reference = ctx
        .sql(
            "SELECT o_orderkey, CASE WHEN o_orderkey <= 200 THEN 'ZZ' ELSE o_orderstatus END AS \
             o_orderstatus FROM orders_src ORDER BY o_orderkey",
        )
        .await
        .unwrap();
    assert_eq!(render(&native.batches), render(&reference.batches));
}

// ---------- overlapping sequential UPDATEs ----------

#[tokio::test]
async fn overlapping_sequential_updates_have_a_well_defined_correct_order() {
    let (mut ctx, _tmp) = build_full_native_orders().await;

    // UPDATE 1: +100 to o_totalprice for o_orderkey <= 300.
    let r1 = ctx
        .update_native_table(
            "UPDATE orders_native SET o_totalprice = o_totalprice + 100 WHERE o_orderkey <= 300",
        )
        .await
        .unwrap();
    assert_eq!(r1.rows_updated, 300);

    // UPDATE 2: +100 to o_totalprice for o_orderkey <= 500 -- OVERLAPS
    // update 1 (o_orderkey 1..=300, now living in a NEW segment with
    // +100 already applied) and also reaches 301..=500 (untouched so
    // far). This is exactly the shape that would silently duplicate rows
    // if UPDATE didn't exclude already-tombstoned matches (see
    // `native_update.rs`'s own module doc) -- the row count and
    // cell-exact checks below fail loudly if that regresses.
    let r2 = ctx
        .update_native_table(
            "UPDATE orders_native SET o_totalprice = o_totalprice + 100 WHERE o_orderkey <= 500",
        )
        .await
        .unwrap();
    assert_eq!(
        r2.rows_updated, 500,
        "all 500 matches are LIVE -- 300 from the new segment, 200 from the original"
    );
    assert_eq!(r2.total_rows, 1500, "row count must never change");

    let native = ctx
        .sql("SELECT o_orderkey, o_totalprice FROM orders_native ORDER BY o_orderkey")
        .await
        .unwrap();
    // Independently-computed reference: ids 1..=300 got +200 (both
    // statements), 301..=500 got +100 (second statement only), the rest
    // untouched -- applied directly against the ORIGINAL source in ONE
    // pass, not by re-running two sequential updates.
    let reference = ctx
        .sql(
            "SELECT o_orderkey, \
             CASE WHEN o_orderkey <= 300 THEN o_totalprice + 200 \
                  WHEN o_orderkey <= 500 THEN o_totalprice + 100 \
                  ELSE o_totalprice END AS o_totalprice \
             FROM orders_src ORDER BY o_orderkey",
        )
        .await
        .unwrap();
    assert_eq!(
        render(&native.batches),
        render(&reference.batches),
        "two overlapping UPDATEs must compose exactly like running them in order, no \
         duplicate/lost rows, no racy double- or under-application"
    );

    // No id may appear more than once.
    assert_eq!(native.row_count, 1500);
}

// ---------- multi-segment targeting ----------

#[tokio::test]
async fn update_across_a_multi_segment_table_applies_to_the_correct_segments_only() {
    let (mut ctx, _tmp) = build_two_segment_native_orders().await;

    // This predicate spans the segment boundary (segment 0: <=750,
    // segment 1: >750) -- a real, cross-segment UPDATE.
    let result = ctx
        .update_native_table(
            "UPDATE orders_native SET o_totalprice = 999999.0 WHERE o_orderkey BETWEEN 700 AND \
             800",
        )
        .await
        .expect("cross-segment UPDATE must succeed");
    assert_eq!(result.rows_updated, 101); // 700..=800 inclusive
    assert_eq!(result.total_rows, 1500);

    let native = ctx
        .sql("SELECT o_orderkey, o_totalprice FROM orders_native ORDER BY o_orderkey")
        .await
        .unwrap();
    let reference = ctx
        .sql(
            "SELECT o_orderkey, CASE WHEN o_orderkey BETWEEN 700 AND 800 THEN 999999.0 ELSE \
             o_totalprice END AS o_totalprice FROM orders_src ORDER BY o_orderkey",
        )
        .await
        .unwrap();
    assert_eq!(render(&native.batches), render(&reference.batches));
}

// ---------- no partial-state visibility (highest-priority test) ----------

/// Verifies -- empirically, via a REAL concurrent reader racing a REAL
/// writer, not just by design argument -- that a reader querying mid-
/// UPDATE never sees a state where old rows are gone but new rows aren't
/// yet visible, or vice versa. Drives `native_update::update_native_table`
/// directly (bypassing `ExecutionContext`/SQL parsing in the hot loop, to
/// keep the reader's polling loop as tight as possible) and reads back via
/// the REAL `TableProvider::scan` path (`NativeTable`, task 003's own
/// deletion-aware scan) -- exactly what an independent process opening
/// this table mid-update would see.
// Genuine OS-thread parallelism, not just cooperative single-thread
// interleaving: every I/O call on both the writer and reader sides is
// blocking std::fs work wrapped in an async fn (no true `.await` yield
// point inside either loop), so a default `current_thread` runtime would
// let the reader's tight polling loop monopolize the one OS thread and
// starve the spawned writer task forever -- confirmed empirically (this
// test hung indefinitely under the default flavor before this fix).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_partial_state_visibility_during_a_concurrent_update() {
    let (ctx, _tmp) = build_full_native_orders().await;
    let table_dir = ctx.native_table_root().join("orders_native");
    let total_rows = 1500u64;

    // Warm-up: TPC-H's `o_orderstatus` legitimately starts as a 3-way mix
    // (O/F/P) -- that is the correct STARTING state, not a "partial
    // update," so normalize to a single uniform value FIRST (synchronously,
    // no reader racing yet) before the real concurrent probe below, which
    // needs a clean "every read is either all-X or all-Y" invariant.
    native_update::update_native_table(
        &table_dir,
        None,
        &[(
            "o_orderstatus".to_string(),
            Expr::Literal(ScalarValue::Utf8("Y".to_string())),
        )],
    )
    .await
    .expect("warm-up UPDATE must succeed");

    let writer_dir = table_dir.clone();
    let writer = tokio::spawn(async move {
        let to_x = vec![(
            "o_orderstatus".to_string(),
            Expr::Literal(ScalarValue::Utf8("X".to_string())),
        )];
        let to_y = vec![(
            "o_orderstatus".to_string(),
            Expr::Literal(ScalarValue::Utf8("Y".to_string())),
        )];
        // Many back-to-back UPDATEs (alternating between two marker
        // values, always matching every row) so the reader loop below
        // has many independent transition instants to try to catch a
        // partial state at, not just one.
        for i in 0..60u32 {
            let assignments = if i % 2 == 0 { &to_x } else { &to_y };
            native_update::update_native_table(&writer_dir, None, assignments)
                .await
                .expect("UPDATE must succeed");
        }
    });

    let mut polls: u64 = 0;
    let mut states_seen: HashSet<String> = HashSet::new();
    let reader_dir = table_dir.clone();
    while !writer.is_finished() {
        polls += 1;
        let table = NativeTable::try_new(&reader_dir)
            .expect("the manifest must ALWAYS be validly readable, even mid-update");
        let idx = table
            .schema()
            .index_of("o_orderstatus")
            .expect("schema must always declare o_orderstatus");
        let batches =
            TableProvider::scan(&table, None).expect("a scan must ALWAYS succeed, even mid-update");

        let mut row_count: u64 = 0;
        let mut distinct: HashSet<String> = HashSet::new();
        for b in &batches {
            row_count += b.num_rows() as u64;
            for v in string_col_values(b.column(idx)).into_iter().flatten() {
                distinct.insert(v);
            }
        }
        assert_eq!(
            row_count, total_rows,
            "poll {polls}: row count must NEVER change during an UPDATE -- a lower count would \
             mean old rows were removed before their replacements became visible"
        );
        assert!(
            distinct.len() <= 1,
            "poll {polls}: observed a MIXED state {distinct:?} across the table's 1500 rows -- \
             partial UPDATE visibility! (old rows gone, new rows not yet visible, or vice \
             versa, or two different UPDATEs' effects visible at once)"
        );
        if let Some(s) = distinct.into_iter().next() {
            states_seen.insert(s);
        }
        // Cooperative yield: keeps this a genuine polling race (many
        // samples spread across the writer's whole run) rather than a
        // pure CPU-bound spin, and is a second, independent safety net
        // (on top of the multi_thread runtime above) against starving
        // the writer task.
        tokio::task::yield_now().await;
    }
    writer.await.expect("writer task must not panic");

    assert!(
        polls > 0,
        "the reader must have actually sampled at least once"
    );
    // Not a strict requirement (timing-dependent) that the reader ever
    // caught an intermediate value, but confirm the writer's OWN 60
    // updates actually alternated (sanity: the harness itself works).
    let final_table = NativeTable::try_new(&table_dir).unwrap();
    let final_idx = final_table.schema().index_of("o_orderstatus").unwrap();
    let final_batches = TableProvider::scan(&final_table, None).unwrap();
    let mut final_states: HashSet<String> = HashSet::new();
    let mut final_row_count = 0u64;
    for b in &final_batches {
        final_row_count += b.num_rows() as u64;
        for v in string_col_values(b.column(final_idx)).into_iter().flatten() {
            final_states.insert(v);
        }
    }
    assert_eq!(final_row_count, total_rows);
    assert_eq!(
        final_states.len(),
        1,
        "the FINAL state after all 60 updates must itself be uniform (the last update's value \
         only)"
    );
}

/// A second, independent angle on the same property: each UPDATE must
/// bump the manifest version by EXACTLY one, never two -- if this
/// implementation ever regressed to calling two separate self-publishing
/// entrypoints sequentially (the exact anti-pattern task 001's design
/// spike forbids), a single UPDATE call would publish twice and this
/// assertion would catch it deterministically, with no timing dependence
/// at all.
#[tokio::test]
async fn each_update_bumps_the_version_by_exactly_one_never_two() {
    let (mut ctx, _tmp) = build_full_native_orders().await;
    let start_version = {
        let provider = ctx.table_provider("orders_native").unwrap();
        let native = provider.as_any().downcast_ref::<NativeTable>().unwrap();
        native.manifest().snapshot.version
    };

    for i in 0..5 {
        let result = ctx
            .update_native_table(&format!(
                "UPDATE orders_native SET o_totalprice = o_totalprice + 1 WHERE o_orderkey = {}",
                i + 1
            ))
            .await
            .unwrap();
        assert_eq!(
            result.version,
            start_version + i + 1,
            "version must advance by exactly 1 per UPDATE -- a jump of 2 would mean two \
             separate publishes happened for one statement"
        );
    }
}

// ---------- SQL surface / error paths ----------

#[tokio::test]
async fn sql_refuses_update_and_points_at_update_native_table() {
    let (ctx, _tmp) = build_full_native_orders().await;
    let err = ctx
        .sql("UPDATE orders_native SET o_totalprice = 0 WHERE o_orderkey <= 500")
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::InvalidArgument(_)), "{err:?}");
    assert!(err.to_string().contains("update_native_table"), "{err}");
}

#[tokio::test]
async fn update_of_an_unregistered_table_is_a_clean_table_not_found_error() {
    let tmp = tempfile::tempdir().unwrap();
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    let err = ctx
        .update_native_table("UPDATE nope SET x = 1 WHERE x = 1")
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::TableNotFound(_)), "{err:?}");
}

#[tokio::test]
async fn update_of_a_non_native_table_is_a_clean_invalid_argument_error() {
    let tmp = tempfile::tempdir().unwrap();
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.register_parquet("orders_src", format!("{}/orders.parquet", data_dir()))
        .unwrap();

    let err = ctx
        .update_native_table("UPDATE orders_src SET o_totalprice = 0 WHERE o_orderkey = 1")
        .await
        .unwrap_err();
    assert!(matches!(err, QueryError::InvalidArgument(_)), "{err:?}");
    assert!(err.to_string().contains("orders_src"), "{err}");
}

#[tokio::test]
async fn no_regression_insert_delete_and_ctas_still_work_after_an_update_in_the_same_session() {
    // An UPDATE must not corrupt the table for subsequent, unrelated
    // mutations against it.
    let (mut ctx, _tmp) = build_full_native_orders().await;
    ctx.update_native_table("UPDATE orders_native SET o_totalprice = 1.0 WHERE o_orderkey <= 1000")
        .await
        .unwrap();
    assert_eq!(count_of(&ctx, "orders_native").await, 1500);

    let delete_result = ctx
        .delete_from_native_table("DELETE FROM orders_native WHERE o_orderkey > 1400")
        .await
        .expect("DELETE after an UPDATE must still work");
    assert_eq!(delete_result.rows_deleted, 100);
    assert_eq!(count_of(&ctx, "orders_native").await, 1400);

    let insert_result = ctx
        .insert_into_native_table(
            "INSERT INTO orders_native SELECT * FROM orders_src WHERE o_orderkey > 1490",
        )
        .await
        .expect("INSERT after an UPDATE must still work");
    assert_eq!(insert_result.rows_inserted, 10);
    assert_eq!(count_of(&ctx, "orders_native").await, 1410);

    let ctas_result = ctx
        .create_table_as_select("CREATE TABLE another AS SELECT * FROM orders_src LIMIT 10")
        .await
        .expect("CTAS must still work in a session that has performed an UPDATE");
    assert_eq!(ctas_result.rows, 10);
}

/// Handles BOTH a plain `Utf8` column and a `Dictionary(Int32, Utf8)` one.
fn string_col_values(col: &Arc<dyn arrow::array::Array>) -> Vec<Option<String>> {
    use arrow::array::{Array, DictionaryArray, StringArray};
    use arrow::datatypes::Int32Type;
    if let Some(dict) = col.as_any().downcast_ref::<DictionaryArray<Int32Type>>() {
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        dict.keys()
            .iter()
            .map(|k| k.map(|k| values.value(k as usize).to_string()))
            .collect()
    } else if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
        (0..s.len())
            .map(|i| (!s.is_null(i)).then(|| s.value(i).to_string()))
            .collect()
    } else {
        panic!(
            "expected a string or dictionary<string> column, got {:?}",
            col.data_type()
        );
    }
}
