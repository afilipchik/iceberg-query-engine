//! Task 004 (native-tables-rollups epic) broader validation sweep: THREE
//! distinctly-shaped rollups (varied GROUP BY column count/order, varied
//! aggregate sets including MIN/MAX on a date column) registered via the
//! real `CREATE MATERIALIZED VIEW` SQL DDL text against the SAME base
//! table (`lineitem_native`, real SF=1 scale -- `data/tpch-1gb`'s
//! `lineitem`, 6,000,000 rows) -- deliberately exercising "a rollup
//! depending on a table that also has other rollups" (this task's own
//! acceptance criterion's example) at real scale, going beyond tasks
//! 001-003's own per-task tests: task 001's own DuckDB-oracle check
//! (`native_rollup_cell_exact_check.rs`) validated exactly ONE rollup
//! shape; task 003's own multi-rollup test
//! (`multiple_rollups_on_the_same_base_table_are_all_refreshed_by_one_
//! mutation`) validated TWO rollups but only against a direct-computation
//! reference, never a DuckDB oracle, and used `register_rollup` rather
//! than the DDL. No prior artifact combines "several distinctly-shaped
//! rollups, registered via real DDL, simultaneously live against one
//! base table" with "an independent DuckDB oracle" -- this closes that
//! gap.
//!
//! For each rollup, the QUERY run through the ordinary `ctx.sql()` path
//! deliberately differs from that rollup's own DEFINING query (GROUP BY
//! order reversed/reordered, different aliases, reordered SELECT list) --
//! matching `native_rollup_cell_exact_check.rs`'s own established
//! discipline, extended to three shapes registered simultaneously instead
//! of one.
//!
//! Every result is compared against TWO independent references, per this
//! task's own acceptance criterion:
//!   1. Direct base-table computation: a SEPARATE, sibling
//!      `ExecutionContext` with an identically-loaded `lineitem_native`
//!      table but ZERO views/rollups ever registered against it,
//!      computing the same query directly.
//!   2. An independent DuckDB oracle, computing the same query directly
//!      against the SAME source parquet file -- a wholly different
//!      engine, not this one checking itself.
//!
//! Both legs are written to CSV and compared with a NUMERIC,
//! tolerance-aware comparator in the companion Python script
//! (`scripts/native_rollup_multi_shape_check.py`), not an exact string
//! comparison here: an early version of this check compared rendered
//! strings directly and reported spurious "mismatches" on the float SUM
//! columns (e.g. `50488166022.002205` vs `50488166022.00214`) that are
//! exactly the float64 summation-order noise class
//! `native_rollup_cell_exact_check.py` already documented and this
//! repo's own established `cell_compare` tolerance
//! (`tol = max(0.02, |v| * 1e-9)`) exists to absorb -- not a real
//! mismatch. The companion script compares THREE ways: rollup-answered
//! vs. DuckDB, direct-computation vs. DuckDB, and rollup-answered vs.
//! direct-computation directly, all with the same tolerance.
//!
//! Run: `scripts/claude-safe-build.sh cargo build --release --example
//! native_rollup_multi_shape_check && scripts/claude-safe-build.sh
//! ./target/release/examples/native_rollup_multi_shape_check`, then
//! `.venv/bin/python scripts/native_rollup_multi_shape_check.py`.

use query_engine::{ExecutionConfig, ExecutionContext};
use std::path::PathBuf;
use std::time::Instant;

const SOURCE_PARQUET: &str = "data/tpch-1gb/lineitem.parquet";
const OUT_DIR: &str = ".scratch/rollup004/native_tables";
const DIRECT_OUT_DIR: &str = ".scratch/rollup004/native_tables_direct";
const CSV_DIR: &str = ".scratch/rollup004";

/// One rollup shape under test: its `CREATE MATERIALIZED VIEW` DDL text,
/// a differently-phrased matching query to run through ordinary `sql()`,
/// its expected registered name (for provenance assertions), and where to
/// dump the rollup-answered leg's result (`csv_file`) and the
/// direct-base-table-computation leg's result (`direct_csv_file`) -- kept
/// as TWO separate files, compared by the companion Python script with a
/// numeric tolerance, rather than an exact string comparison here (see
/// the module doc for why).
struct Shape {
    name: &'static str,
    create_view_sql: &'static str,
    query_sql: &'static str,
    csv_file: &'static str,
    direct_csv_file: &'static str,
}

const SHAPES: &[Shape] = &[
    // A: 2 GROUP BY columns, 3 aggregates (SUM/SUM/COUNT) -- the PRD's own
    // worked example shape.
    Shape {
        name: "rollup_by_flag_status",
        create_view_sql: "CREATE MATERIALIZED VIEW rollup_by_flag_status AS \
            SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
            SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order \
            FROM lineitem_native GROUP BY l_returnflag, l_linestatus",
        query_sql: "SELECT l_linestatus, l_returnflag, COUNT(*) AS n, SUM(l_quantity) AS q, \
            SUM(l_extendedprice) AS base_price FROM lineitem_native \
            GROUP BY l_linestatus, l_returnflag ORDER BY l_returnflag, l_linestatus",
        csv_file: "rollup_a_flag_status.csv",
        direct_csv_file: "rollup_a_flag_status_direct.csv",
    },
    // B: 1 GROUP BY column, 4 aggregates incl. MIN/MAX on a DATE column --
    // new coverage vs. task 001's own DuckDB check (SUM/COUNT only).
    Shape {
        name: "rollup_by_shipmode",
        create_view_sql: "CREATE MATERIALIZED VIEW rollup_by_shipmode AS \
            SELECT l_shipmode, SUM(l_quantity) AS sum_qty, MIN(l_shipdate) AS min_ship, \
            MAX(l_shipdate) AS max_ship, COUNT(*) AS n FROM lineitem_native \
            GROUP BY l_shipmode",
        query_sql: "SELECT COUNT(*) AS cnt, MAX(l_shipdate) AS latest, MIN(l_shipdate) AS \
            earliest, SUM(l_quantity) AS total_qty, l_shipmode AS mode FROM lineitem_native \
            GROUP BY l_shipmode ORDER BY l_shipmode",
        csv_file: "rollup_b_shipmode.csv",
        direct_csv_file: "rollup_b_shipmode_direct.csv",
    },
    // C: 3 GROUP BY columns in a deliberately unnatural order, 2
    // aggregates -- exercises a wider composite group key than either A
    // or B, and than any existing rollup test in the epic.
    Shape {
        name: "rollup_by_status_mode_flag",
        create_view_sql: "CREATE MATERIALIZED VIEW rollup_by_status_mode_flag AS \
            SELECT l_linestatus, l_shipmode, l_returnflag, \
            SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, COUNT(*) AS n \
            FROM lineitem_native GROUP BY l_linestatus, l_shipmode, l_returnflag",
        query_sql: "SELECT l_returnflag, l_shipmode, l_linestatus, COUNT(*) AS cnt, \
            SUM(l_extendedprice * (1 - l_discount)) AS disc_price FROM lineitem_native \
            GROUP BY l_returnflag, l_shipmode, l_linestatus \
            ORDER BY l_linestatus, l_shipmode, l_returnflag",
        csv_file: "rollup_c_status_mode_flag.csv",
        direct_csv_file: "rollup_c_status_mode_flag_direct.csv",
    },
];

fn write_csv(path: &std::path::Path, batches: &[arrow::record_batch::RecordBatch]) {
    let file =
        std::fs::File::create(path).unwrap_or_else(|e| panic!("create {}: {e}", path.display()));
    let mut writer = arrow::csv::WriterBuilder::new()
        .with_header(true)
        .build(file);
    for batch in batches {
        writer.write(batch).expect("write CSV batch");
    }
}

async fn fresh_context(root: &str, config: ExecutionConfig) -> ExecutionContext {
    let root_path = PathBuf::from(root);
    if root_path.exists() {
        std::fs::remove_dir_all(&root_path).expect("clear stale native_tables root");
    }
    std::fs::create_dir_all(&root_path).expect("create native_tables root");
    let mut ctx = ExecutionContext::with_config(config).with_native_table_root(root_path);
    ctx.register_parquet("lineitem_src", SOURCE_PARQUET)
        .expect("register source lineitem parquet (data/tpch-1gb)");
    ctx.create_table_as_select("CREATE TABLE lineitem_native AS SELECT * FROM lineitem_src")
        .await
        .expect("CREATE TABLE ... AS SELECT must succeed");
    ctx
}

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    if !std::path::Path::new(SOURCE_PARQUET).exists() {
        eprintln!(
            "skipping: {SOURCE_PARQUET} not found (expected data/tpch-1gb to already exist in \
             this repo)"
        );
        return Ok(());
    }

    std::fs::create_dir_all(CSV_DIR).expect("create scratch csv dir");

    // 16GB: comfortably above SF=1 lineitem's real footprint, matching
    // this program's own "size --memory-limit generously for native
    // tables, name it" convention.
    let config = ExecutionConfig::default().with_memory_limit(16 * 1024 * 1024 * 1024);

    println!("=== native rollup multi-shape check: {SOURCE_PARQUET} ===");

    // --- Context A: the table under test, with all THREE rollups
    // registered via the real CREATE MATERIALIZED VIEW DDL text. ---
    let t0 = Instant::now();
    let mut ctx = fresh_context(OUT_DIR, config.clone()).await;
    println!("built lineitem_native (context A) in {:?}", t0.elapsed());

    for shape in SHAPES {
        let t = Instant::now();
        let reg = ctx
            .create_materialized_view(shape.create_view_sql)
            .await
            .unwrap_or_else(|e| panic!("CREATE MATERIALIZED VIEW {}: {e}", shape.name));
        println!(
            "CREATE MATERIALIZED VIEW {}: {} rows, {} segment(s), version {} ({:?})",
            shape.name,
            reg.rows,
            reg.segments,
            reg.version,
            t.elapsed()
        );
        assert_eq!(reg.rollup_name, shape.name);
    }

    // --- Context B: sibling table, ZERO rollups registered -- the
    // "direct base-table computation" reference. ---
    let t1 = Instant::now();
    let direct_ctx = fresh_context(DIRECT_OUT_DIR, config).await;
    println!(
        "built lineitem_native (context B, direct-computation reference, no rollups) in {:?}",
        t1.elapsed()
    );

    let mut all_ok = true;
    for shape in SHAPES {
        let t = Instant::now();
        let result = ctx
            .sql(shape.query_sql)
            .await
            .unwrap_or_else(|e| panic!("query for {}: {e}", shape.name));
        println!(
            "query {}: {} rows, rollup_answered={:?} ({:?})",
            shape.name,
            result.row_count,
            result.metrics.rollup_answered,
            t.elapsed()
        );
        if result.metrics.rollup_answered != vec![shape.name.to_string()] {
            eprintln!(
                "PROVENANCE FAIL for {}: expected rollup_answered == [{:?}], got {:?}",
                shape.name, shape.name, result.metrics.rollup_answered
            );
            all_ok = false;
            continue;
        }

        let direct = direct_ctx
            .sql(shape.query_sql)
            .await
            .unwrap_or_else(|e| panic!("direct-reference query for {}: {e}", shape.name));
        assert!(
            direct.metrics.rollup_answered.is_empty(),
            "the direct-computation reference context registered no rollups -- if this fires, \
             the reference itself is not what this check claims"
        );
        if result.row_count != direct.row_count {
            eprintln!(
                "ROW COUNT MISMATCH for {}: rollup-answered={} direct={}",
                shape.name, result.row_count, direct.row_count
            );
            all_ok = false;
            continue;
        }

        let csv_path = PathBuf::from(CSV_DIR).join(shape.csv_file);
        write_csv(&csv_path, &result.batches);
        let direct_csv_path = PathBuf::from(CSV_DIR).join(shape.direct_csv_file);
        write_csv(&direct_csv_path, &direct.batches);
        println!(
            "  wrote {} and {} -- numeric cell comparison happens in \
             scripts/native_rollup_multi_shape_check.py (rollup vs. direct vs. DuckDB, all \
             three pairs, with this repo's own established float tolerance)",
            csv_path.display(),
            direct_csv_path.display()
        );
    }

    if !all_ok {
        eprintln!("\nFAIL: one or more shapes did not match provenance/row-count checks");
        std::process::exit(1);
    }

    println!(
        "\nAll {} rollup shapes: provenance-confirmed rollup-answered, row counts agree with \
         direct base-table computation. Run scripts/native_rollup_multi_shape_check.py next \
         for the full numeric cell comparison (rollup vs. direct vs. DuckDB oracle).",
        SHAPES.len()
    );
    Ok(())
}
