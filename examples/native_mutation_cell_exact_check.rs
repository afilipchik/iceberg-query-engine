//! Task 006 (native-tables-mutation epic, QA close-out) diagnostic:
//! cell-exact validation of INSERT/DELETE/UPDATE at REAL scale
//! (`data/tpch-10gb`'s `orders`, 1,500,000 rows) -- not the ~1500-row
//! `data/tpch-1mb` fixtures tasks 002/003/004 individually used for their
//! own cell-exact tests. Mirrors this epic's established
//! `examples/native_*_check.rs` pattern (diagnostic binary, not a
//! `#[test]`, so it can run against real multi-GB fixtures without
//! becoming part of every `cargo test` invocation).
//!
//! Sequence (a realistic incremental-load + correction workflow, not a
//! synthetic stress shape):
//!   1. `CREATE TABLE orders_native AS SELECT * FROM orders_src WHERE
//!      o_orderkey % 5 <> 0`   -- ~80% of the table, the base load.
//!   2. `INSERT INTO orders_native SELECT * FROM orders_src WHERE
//!      o_orderkey % 5 = 0`    -- the remaining ~20%, an incremental load.
//!   3. `DELETE FROM orders_native WHERE o_orderstatus = 'F' AND
//!      o_totalprice < 50000`  -- a real, selective predicate.
//!   4. `UPDATE orders_native SET o_totalprice = o_totalprice * 1.05,
//!      o_orderpriority = '1-URGENT' WHERE o_orderdate >= DATE
//!      '1998-01-01'`          -- a real, selective SET+WHERE.
//!
//! Writes the final table's full contents to CSV
//! (`.scratch/qa006/orders_native_result.csv`, ordered by `o_orderkey`
//! for a deterministic diff). `scripts/native_mutation_cell_exact_check.py`
//! (companion, new) independently recomputes the identical four
//! statements as REAL DuckDB DML against the SAME source parquet file --
//! a wholly separate engine, not this one checking itself -- and compares
//! every cell via a DuckDB `EXCEPT` set-difference in both directions:
//! zero rows either way is the cell-exact verdict.
//!
//! Run: `scripts/claude-safe-build.sh cargo build --release --example
//! native_mutation_cell_exact_check && scripts/claude-safe-build.sh
//! ./target/release/examples/native_mutation_cell_exact_check`, then
//! `.venv/bin/python scripts/native_mutation_cell_exact_check.py`.

use query_engine::{ExecutionConfig, ExecutionContext};
use std::path::PathBuf;
use std::time::Instant;

const SOURCE_PARQUET: &str = "data/tpch-10gb/orders.parquet";
const OUT_DIR: &str = ".scratch/qa006/native_tables";
const RESULT_CSV: &str = ".scratch/qa006/orders_native_result.csv";

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    if !std::path::Path::new(SOURCE_PARQUET).exists() {
        eprintln!(
            "skipping: {SOURCE_PARQUET} not found (expected data/tpch-10gb to already exist \
             in this repo)"
        );
        return Ok(());
    }

    std::fs::create_dir_all(OUT_DIR).expect("create scratch native_tables root");
    // Fresh table every run: this example is not idempotent (a stale
    // orders_native from a prior run would make CREATE fall onto the
    // Overwrite path with different starting statistics, confusing the
    // independent DuckDB reference which always starts from the pristine
    // source parquet).
    let table_dir = PathBuf::from(OUT_DIR).join("orders_native");
    if table_dir.exists() {
        std::fs::remove_dir_all(&table_dir).expect("clear stale orders_native");
    }

    // 8GB: comfortably above the 1.5M-row `orders` table's real footprint
    // (280MB compressed parquet source), matching this epic's own
    // diagnostics' "size generously, name it" convention for the
    // admission-control cap (phase 1 task 006 / this epic's task 003).
    let config = ExecutionConfig::default().with_memory_limit(8 * 1024 * 1024 * 1024);
    let mut ctx =
        ExecutionContext::with_config(config).with_native_table_root(PathBuf::from(OUT_DIR));

    ctx.register_parquet("orders_src", SOURCE_PARQUET)
        .expect("register source orders parquet (data/tpch-10gb)");

    println!("=== native mutation cell-exact check: {SOURCE_PARQUET} ===");

    let seq_start = Instant::now();

    let t0 = Instant::now();
    let created = ctx
        .create_table_as_select(
            "CREATE TABLE orders_native AS SELECT * FROM orders_src WHERE o_orderkey % 5 <> 0",
        )
        .await
        .expect("CREATE TABLE ... AS SELECT must succeed");
    println!(
        "CREATE: {} rows, {} segment(s), version {} ({:?})",
        created.rows,
        created.segments,
        created.version,
        t0.elapsed()
    );

    let t1 = Instant::now();
    let inserted = ctx
        .insert_into_native_table(
            "INSERT INTO orders_native SELECT * FROM orders_src WHERE o_orderkey % 5 = 0",
        )
        .await
        .expect("INSERT INTO ... SELECT must succeed");
    println!(
        "INSERT: +{} rows ({} segment(s) added), total {} rows, version {} ({:?})",
        inserted.rows_inserted,
        inserted.segments_added,
        inserted.total_rows,
        inserted.version,
        t1.elapsed()
    );

    let t2 = Instant::now();
    let deleted = ctx
        .delete_from_native_table(
            "DELETE FROM orders_native WHERE o_orderstatus = 'F' AND o_totalprice < 50000",
        )
        .await
        .expect("DELETE FROM ... WHERE must succeed");
    println!(
        "DELETE: -{} rows ({} segment(s) dropped), total {} rows, version {} ({:?})",
        deleted.rows_deleted,
        deleted.segments_dropped,
        deleted.total_rows,
        deleted.version,
        t2.elapsed()
    );

    let t3 = Instant::now();
    let updated = ctx
        .update_native_table(
            "UPDATE orders_native SET o_totalprice = o_totalprice * 1.05, \
             o_orderpriority = '1-URGENT' WHERE o_orderdate >= DATE '1998-01-01'",
        )
        .await
        .expect("UPDATE ... SET ... WHERE must succeed");
    println!(
        "UPDATE: ~{} rows recomputed ({} segment(s) dropped, {} added), total {} rows, \
         version {} ({:?})",
        updated.rows_updated,
        updated.segments_dropped,
        updated.segments_added,
        updated.total_rows,
        updated.version,
        t3.elapsed()
    );

    println!("\nTotal mutation sequence: {:?}", seq_start.elapsed());

    let t4 = Instant::now();
    let result = ctx
        .sql("SELECT * FROM orders_native ORDER BY o_orderkey")
        .await
        .expect("final SELECT * must succeed");
    println!("Final scan: {} rows ({:?})", result.row_count, t4.elapsed());

    let file = std::fs::File::create(RESULT_CSV).expect("create result CSV");
    let mut writer = arrow::csv::WriterBuilder::new()
        .with_header(true)
        .build(file);
    for batch in &result.batches {
        writer.write(batch).expect("write CSV batch");
    }
    println!("Wrote {RESULT_CSV}");
    Ok(())
}
