//! Task 001 (native-tables-rollups epic) diagnostic: cell-exact
//! validation of the rollup matching/substitution mechanism at REAL
//! scale (`data/tpch-1gb`'s `lineitem`, ~6,000,000 rows) — not the
//! ~small `data/tpch-1mb` fixture `tests/native_rollup_tests.rs` uses
//! for its own (much faster) unit-level integration tests. Mirrors this
//! program's established `examples/native_*_check.rs` pattern
//! (diagnostic binary, not a `#[test]`, so it can run against a real
//! multi-GB fixture without becoming part of every `cargo test`
//! invocation) and specifically `native_mutation_cell_exact_check.rs`'s
//! own shape: build the real table, exercise the real SQL surface, dump
//! the result to CSV, let a companion Python script recompute the
//! identical query independently via DuckDB and diff every cell.
//!
//! This is the PRD's own worked example (functional requirement 6):
//! `lineitem` grouped by `l_returnflag`/`l_linestatus`, SUM/COUNT
//! aggregates.
//!
//! Sequence:
//!   1. `CREATE TABLE lineitem_native AS SELECT * FROM lineitem_src`
//!   2. `register_rollup("lineitem_rollup", "lineitem_native", <the
//!      worked-example defining query>)`
//!   3. Run the IDENTICAL query (order-independent GROUP BY, aliased
//!      differently than the defining query, to also exercise
//!      order-independence/alias-blindness at this scale) via
//!      `ctx.sql(...)`, confirm `QueryMetrics::rollup_answered` names the
//!      rollup (provenance, not assumed).
//!   4. Write the result to CSV (`.scratch/rollup001/lineitem_rollup_result.csv`).
//!
//! `scripts/native_rollup_cell_exact_check.py` (companion, new)
//! independently recomputes the IDENTICAL query as REAL DuckDB SQL
//! against the SAME source parquet file — a wholly separate engine, not
//! this one checking itself — and compares every cell via a DuckDB
//! `EXCEPT` set-difference in both directions: zero rows either way is
//! the cell-exact verdict, matching this repo's established DuckDB-oracle
//! convention.
//!
//! Run: `scripts/claude-safe-build.sh cargo build --release --example
//! native_rollup_cell_exact_check && scripts/claude-safe-build.sh
//! ./target/release/examples/native_rollup_cell_exact_check`, then
//! `.venv/bin/python scripts/native_rollup_cell_exact_check.py`.

use query_engine::{ExecutionConfig, ExecutionContext};
use std::path::PathBuf;
use std::time::Instant;

const SOURCE_PARQUET: &str = "data/tpch-1gb/lineitem.parquet";
const OUT_DIR: &str = ".scratch/rollup001/native_tables";
const RESULT_CSV: &str = ".scratch/rollup001/lineitem_rollup_result.csv";

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    if !std::path::Path::new(SOURCE_PARQUET).exists() {
        eprintln!(
            "skipping: {SOURCE_PARQUET} not found (expected data/tpch-1gb to already exist in \
             this repo)"
        );
        return Ok(());
    }

    std::fs::create_dir_all(OUT_DIR).expect("create scratch native_tables root");
    // Fresh tables every run: neither create_table_as_select nor
    // register_rollup is idempotent in a way that matters here (a stale
    // lineitem_native from a prior run would confuse nothing correctness-
    // wise, since both are full-table Overwrite/Create -- but a fresh
    // start keeps this diagnostic's timings meaningful run over run).
    for name in ["lineitem_native", "lineitem_rollup"] {
        let dir = PathBuf::from(OUT_DIR).join(name);
        if dir.exists() {
            std::fs::remove_dir_all(&dir).expect("clear stale table dir");
        }
    }

    // 16GB: comfortably above SF=1 lineitem's real footprint, matching
    // this program's own "size --memory-limit generously for native
    // tables, name it" convention (native tables have no streaming scan
    // path yet -- see CLAUDE.md's Native Tables section).
    let config = ExecutionConfig::default().with_memory_limit(16 * 1024 * 1024 * 1024);
    let mut ctx =
        ExecutionContext::with_config(config).with_native_table_root(PathBuf::from(OUT_DIR));

    ctx.register_parquet("lineitem_src", SOURCE_PARQUET)
        .expect("register source lineitem parquet (data/tpch-1gb)");

    println!("=== native rollup cell-exact check: {SOURCE_PARQUET} ===");

    let t0 = Instant::now();
    let created = ctx
        .create_table_as_select("CREATE TABLE lineitem_native AS SELECT * FROM lineitem_src")
        .await
        .expect("CREATE TABLE ... AS SELECT must succeed");
    println!(
        "CREATE lineitem_native: {} rows, {} segment(s) ({:?})",
        created.rows,
        created.segments,
        t0.elapsed()
    );

    let t1 = Instant::now();
    let defining_sql = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
                         SUM(l_extendedprice) AS sum_base_price, \
                         SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, \
                         COUNT(*) AS count_order FROM lineitem_native GROUP BY l_returnflag, \
                         l_linestatus";
    let reg = ctx
        .register_rollup("lineitem_rollup", "lineitem_native", defining_sql)
        .await
        .expect("register_rollup must succeed for the PRD's own worked example");
    println!(
        "register_rollup lineitem_rollup: {} rows (one per (l_returnflag, l_linestatus) \
         group), {} segment(s), version {} ({:?})",
        reg.rows,
        reg.segments,
        reg.version,
        t1.elapsed()
    );

    // The QUERY used here deliberately differs from the DEFINING query
    // above in three ways that must NOT affect matching, per this task's
    // own documented semantics: (1) GROUP BY order reversed, (2)
    // different aliases, (3) SELECT list order reversed relative to
    // GROUP BY. If the mechanism were wrong in any of these dimensions,
    // this would silently fall back to the (also-correct, but NOT the
    // thing this check exists to prove) base-table path instead.
    let query = "SELECT l_linestatus, l_returnflag, COUNT(*) AS n, SUM(l_quantity) AS q, \
                 SUM(l_extendedprice) AS base_price, \
                 SUM(l_extendedprice * (1 - l_discount)) AS disc_price \
                 FROM lineitem_native GROUP BY l_linestatus, l_returnflag \
                 ORDER BY l_returnflag, l_linestatus";

    let t2 = Instant::now();
    let result = ctx
        .sql(query)
        .await
        .expect("the rollup-matching query must succeed");
    println!(
        "query: {} rows, rollup_answered={:?} ({:?})",
        result.row_count,
        result.metrics.rollup_answered,
        t2.elapsed()
    );
    assert_eq!(
        result.metrics.rollup_answered,
        vec!["lineitem_rollup".to_string()],
        "PROVENANCE CHECK FAILED: this query must be answered by lineitem_rollup, not the \
         base table -- if this fires, the cell-exact comparison below would not actually be \
         testing what this diagnostic claims to test"
    );

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
