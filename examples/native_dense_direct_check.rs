//! Task 005 (native-tables-foundation epic) diagnostic: proves, with a real
//! measurement rather than an assumption, that
//! `MorselAggregateExec::try_execute_dense_direct` fires for a native table
//! at a Q18-shaped scale (single int group key, large group count over a
//! multi-million-row table) — not just at the tiny `data/tpch-1mb` scale
//! `tests/native_table_validation.rs` uses for cell-exact correctness.
//!
//! Writes a native table from `data/tpch-1gb/lineitem.parquet` (SF=1, ~6M
//! rows) projected to just `(l_orderkey, l_quantity)`, then runs
//! `GROUP BY l_orderkey` with plain COUNT/SUM aggregates — the exact shape
//! TPC-H Q18 itself groups by — against both the native copy and the
//! Parquet source, printing row counts and elapsed time for each so a
//! human (or a script) can compare them directly.
//!
//! Run with `AGG_TIMING=1` set so `try_execute_dense_direct`'s own
//! diagnostic print reveals which tier actually executed:
//!
//! ```text
//! AGG_TIMING=1 cargo run --release --example native_dense_direct_check
//! ```
//!
//! A line reading `[AGG_TIMING] dense-direct scan+accumulate (native): ...`
//! is the real evidence the native-table dense-direct-address path fired
//! (not `MorselAggregateExec`'s native-mode error safety net, and not a
//! fallback to the generic hash-aggregate tier). The Parquet leg should
//! print the pre-existing `[AGG_TIMING] dense-direct scan+accumulate: ...`
//! (no `(native)` tag) — proof this task did not regress that path.

use query_engine::{ExecutionConfig, ExecutionContext};
use std::time::Instant;

const SOURCE_PARQUET: &str = "data/tpch-1gb/lineitem.parquet";
const DENSE_GROUP_BY_SQL: &str =
    "SELECT l_orderkey, COUNT(*) AS cnt, SUM(l_quantity) AS total_qty \
     FROM {} GROUP BY l_orderkey";

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    if !std::path::Path::new(SOURCE_PARQUET).exists() {
        eprintln!(
            "skipping: {SOURCE_PARQUET} not found (generate it first, e.g. \
             `query_engine generate-parquet --sf 1 --output data/tpch-1gb`)"
        );
        return Ok(());
    }

    // 4GB: comfortably above `data/tpch-1gb`'s scale so the native table's
    // `scan()` admission-control budget (task 006, memory_limit *
    // spill_threshold) never refuses this diagnostic for an unrelated
    // reason -- sized the same way `benchmark-parquet` sizes SF-scaled
    // runs (`(sf * 4.0).max(1.0)` GB), not picked arbitrarily.
    let scratch = tempfile::tempdir().expect("tempdir for the native table root");
    let config = ExecutionConfig::default().with_memory_limit(4 * 1024 * 1024 * 1024);
    let mut ctx =
        ExecutionContext::with_config(config).with_native_table_root(scratch.path().to_path_buf());

    ctx.register_parquet("lineitem_src", SOURCE_PARQUET)
        .expect("register source lineitem parquet");

    println!("Writing native table from {SOURCE_PARQUET} (l_orderkey, l_quantity only)...");
    let t_write = Instant::now();
    let created = ctx
        .create_table_as_select(
            "CREATE TABLE lineitem_native AS SELECT l_orderkey, l_quantity FROM lineitem_src",
        )
        .await
        .expect("CREATE TABLE ... AS SELECT must succeed");
    println!(
        "  -> lineitem_native: {} rows, {} segments, version {} ({:?})",
        created.rows,
        created.segments,
        created.version,
        t_write.elapsed()
    );

    for (label, table) in [
        ("parquet source", "lineitem_src"),
        ("native table", "lineitem_native"),
    ] {
        let sql = DENSE_GROUP_BY_SQL.replace("{}", table);
        let t0 = Instant::now();
        let result = ctx.sql(&sql).await.unwrap_or_else(|e| {
            panic!("query against {table} ({label}) failed: {e}");
        });
        println!(
            "{label:14} ({table:15}): {:>8} groups in {:?}",
            result.row_count,
            t0.elapsed()
        );
    }

    println!(
        "\nRe-run with AGG_TIMING=1 and grep for \"dense-direct scan+accumulate\" to see \
         which tier each leg above actually took -- \"(native)\" on the lineitem_native line \
         is this task's fast path; its absence there (falling back to the generic morsel or \
         hash-aggregate tier, or an outright error) would be the regression this diagnostic \
         exists to catch."
    );
    Ok(())
}
