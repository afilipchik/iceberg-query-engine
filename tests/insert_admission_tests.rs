//! End-to-end validation of the INSERT/CTAS write-path pre-flight
//! admission check (oom-safety-hardening epic, task 005). The pure
//! estimate arithmetic is unit-tested in `src/execution/context.rs`'s own
//! test module against the SF=10 calibration numbers; THIS file exercises
//! the real SQL surface (`create_table_as_select` /
//! `insert_into_native_table`) against the committed `data/tpch-1mb`
//! fixture, asserting:
//!
//!   - a statement whose estimated bounded-merge working set exceeds
//!     `memory_limit * spill_threshold` is refused CLEANLY, BY NAME, with
//!     exact byte counts and both knobs named, BEFORE any partition
//!     stream is driven (no partial table on disk, target untouched);
//!   - the identical statement with an adequate budget still completes
//!     (no false refusal);
//!   - statements with no parquet-backed source (VALUES / table-less
//!     SELECT) are admitted even under a tiny budget — there is no
//!     estimate basis, and refusing on a guess would be a new bug class.
//!
//! `data/tpch-1mb/lineitem.parquet` has ONE row group of 327,259 bytes
//! (`total_byte_size`, confirmed via pyarrow before writing this file), so
//! the estimate is 1 stream x 327,259 x 3 = 981,777 bytes: a 1,000,000-byte
//! memory limit (budget 800,000) refuses, the default limit admits.

use query_engine::{ExecutionConfig, ExecutionContext};

fn lineitem_src() -> String {
    format!(
        "{}/data/tpch-1mb/lineitem.parquet",
        env!("CARGO_MANIFEST_DIR")
    )
}

const TINY_LIMIT: usize = 1_000_000; // budget = 800,000 bytes at the 0.8 threshold

fn tiny_ctx(root: &std::path::Path) -> ExecutionContext {
    let config = ExecutionConfig::default().with_memory_limit(TINY_LIMIT);
    ExecutionContext::with_config(config).with_native_table_root(root.to_path_buf())
}

#[tokio::test]
async fn ctas_is_refused_by_name_under_a_tiny_budget_before_any_write() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = tiny_ctx(tmp.path());
    ctx.register_parquet("lineitem_src", lineitem_src())
        .expect("register source parquet");

    let err = ctx
        .create_table_as_select("CREATE TABLE t AS SELECT * FROM lineitem_src")
        .await
        .expect_err("a 1MB memory limit must refuse this CTAS");
    let msg = err.to_string();
    assert!(
        msg.contains("insert/CTAS write-path admission check"),
        "refusal must be attributable by name: {msg}"
    );
    assert!(
        msg.contains("--memory-limit") && msg.contains("QE_INSERT_MERGE_CONCURRENCY"),
        "refusal must name both knobs: {msg}"
    );
    let budget = (TINY_LIMIT as f64 * ExecutionConfig::default().spill_threshold) as u64;
    assert!(
        msg.contains(&budget.to_string()),
        "refusal must cite the exact budget ({budget}): {msg}"
    );
    assert!(
        msg.contains("bytes"),
        "refusal must cite byte counts: {msg}"
    );
    // Refused BEFORE any partition stream was driven: nothing was staged
    // or published under the native-table root.
    let leftovers: Vec<_> = std::fs::read_dir(tmp.path())
        .expect("read native root")
        .collect();
    assert!(
        leftovers.is_empty(),
        "a refusal must leave the native-table root untouched: {leftovers:?}"
    );
}

#[tokio::test]
async fn ctas_with_an_adequate_budget_is_not_falsely_refused() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.register_parquet("lineitem_src", lineitem_src())
        .expect("register source parquet");

    let result = ctx
        .create_table_as_select("CREATE TABLE t AS SELECT * FROM lineitem_src")
        .await
        .expect("the default memory limit must admit the 1MB fixture");
    assert_eq!(result.rows, 6000, "full fixture row count");
}

#[tokio::test]
async fn insert_refused_by_admission_check_leaves_the_target_untouched() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // Seed the target with a GENEROUS context first (this leg must not be
    // what the check refuses).
    {
        let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
        ctx.register_parquet("lineitem_src", lineitem_src())
            .expect("register source parquet");
        ctx.create_table_as_select(
            "CREATE TABLE t AS SELECT * FROM lineitem_src WHERE l_orderkey <= 100",
        )
        .await
        .expect("seed CTAS");
    }
    let table_dir = tmp.path().join("t");
    let manifest_before =
        std::fs::read_to_string(table_dir.join("_manifest.json")).expect("read seed manifest");

    // Now a TIGHT context: the INSERT must be refused by name, and the
    // target manifest must be byte-for-byte unchanged.
    let mut ctx = tiny_ctx(tmp.path());
    ctx.register_native_table("t", &table_dir)
        .expect("register the seeded native table");
    ctx.register_parquet("lineitem_src", lineitem_src())
        .expect("register source parquet");

    let err = ctx
        .insert_into_native_table("INSERT INTO t SELECT * FROM lineitem_src")
        .await
        .expect_err("a 1MB memory limit must refuse this INSERT");
    let msg = err.to_string();
    assert!(
        msg.contains("insert/CTAS write-path admission check") && msg.contains("INSERT INTO t"),
        "refusal must name the check and the statement: {msg}"
    );

    let manifest_after =
        std::fs::read_to_string(table_dir.join("_manifest.json")).expect("re-read manifest");
    assert_eq!(
        manifest_before, manifest_after,
        "a refused INSERT must leave the target manifest byte-for-byte untouched"
    );
}

#[tokio::test]
async fn statements_without_a_parquet_source_are_admitted_under_a_tiny_budget() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = tiny_ctx(tmp.path());

    // Table-less SELECT source: no scan, no parquet, no estimate basis.
    let created = ctx
        .create_table_as_select("CREATE TABLE t AS SELECT 1 AS a")
        .await
        .expect("no-parquet CTAS must be admitted even at a 1MB limit");
    assert_eq!(created.rows, 1);

    // VALUES source: same reasoning, through the INSERT path.
    let inserted = ctx
        .insert_into_native_table("INSERT INTO t VALUES (2)")
        .await
        .expect("VALUES INSERT must be admitted even at a 1MB limit");
    assert_eq!(inserted.rows_inserted, 1);
    assert_eq!(inserted.total_rows, 2);
}
