//! Task 003 (native-tables-rollups epic) diagnostic: does attaching a
//! rollup meaningfully change base-table mutation performance? Measures
//! the SAME small INSERT against a freshly built `lineitem_native` (real
//! SF=1 scale, `data/tpch-1gb/lineitem.parquet`, ~6,000,000 rows) with
//! 0, 1, and 3 rollups registered against it, reporting
//! `InsertResult::elapsed` (which, after this task's change, covers the
//! whole parse-bind-plan-execute-write-REFRESH sequence — see
//! `ExecutionContext::refresh_dependent_rollups`'s own doc) for each
//! premise. Mirrors this repo's own `examples/native_*_check.rs`
//! diagnostic-binary pattern (not a `#[test]`, so it can run against a
//! real multi-GB fixture without becoming part of every `cargo test`
//! invocation) and specifically `native_rollup_cell_exact_check.rs`'s own
//! setup shape.
//!
//! Each scenario builds `lineitem_native` FRESH (a clean `CREATE TABLE
//! ... AS SELECT`, timed separately and NOT part of the reported mutation
//! number) so the three premises are apples-to-apples: same starting row
//! count, same segment count, same everything except how many rollups are
//! registered against it. The mutation itself is repeated
//! [`INSERT_REPEATS`] times per scenario (a small, fixed, real subset of
//! `lineitem_src`'s own rows re-inserted each time — duplicating existing
//! rows is fine for a performance measurement, it is not a correctness
//! check) to reduce single-sample noise; every rollup is refreshed again
//! on every repeat, so this also exercises "does refresh cost stay flat
//! across repeated small mutations" rather than just a single cold run.
//!
//! Run: `scripts/claude-safe-build.sh cargo build --release --example
//! native_rollup_refresh_perf_check && scripts/claude-safe-build.sh
//! ./target/release/examples/native_rollup_refresh_perf_check`.

use query_engine::{ExecutionConfig, ExecutionContext};
use std::path::PathBuf;
use std::time::{Duration, Instant};

const SOURCE_PARQUET: &str = "data/tpch-1gb/lineitem.parquet";
const OUT_ROOT: &str = ".scratch/rollup003perf";
const INSERT_REPEATS: usize = 3;
const INSERT_SQL: &str =
    "INSERT INTO lineitem_native SELECT * FROM lineitem_src WHERE l_orderkey = 3";

const ROLLUP_A_SQL: &str = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
     SUM(l_extendedprice) AS sum_base_price, COUNT(*) AS count_order FROM lineitem_native \
     GROUP BY l_returnflag, l_linestatus";
const ROLLUP_B_SQL: &str = "SELECT l_shipmode, SUM(l_quantity) AS sum_qty, COUNT(*) AS n \
     FROM lineitem_native GROUP BY l_shipmode";
const ROLLUP_C_SQL: &str = "SELECT l_shipinstruct, l_returnflag, SUM(l_extendedprice) AS \
     sum_price, COUNT(*) AS n FROM lineitem_native GROUP BY l_shipinstruct, l_returnflag";

fn fmt_ms(d: Duration) -> String {
    format!("{:.2}ms", d.as_secs_f64() * 1000.0)
}

async fn build_scenario(scratch_subdir: &str, num_rollups: usize) -> ExecutionContext {
    let root = PathBuf::from(OUT_ROOT).join(scratch_subdir);
    if root.exists() {
        std::fs::remove_dir_all(&root).expect("clear stale scenario dir");
    }
    std::fs::create_dir_all(&root).expect("create scenario dir");

    // 16GB: comfortably above SF=1 lineitem's real footprint, matching
    // this repo's own "size --memory-limit generously for native tables"
    // convention (see CLAUDE.md's Native Tables section).
    let config = ExecutionConfig::default().with_memory_limit(16 * 1024 * 1024 * 1024);
    let mut ctx = ExecutionContext::with_config(config).with_native_table_root(root);
    ctx.register_parquet("lineitem_src", SOURCE_PARQUET)
        .expect("register source lineitem parquet");

    let t0 = Instant::now();
    let created = ctx
        .create_table_as_select("CREATE TABLE lineitem_native AS SELECT * FROM lineitem_src")
        .await
        .expect("CREATE TABLE ... AS SELECT must succeed");
    println!(
        "  [setup] CREATE lineitem_native: {} rows, {} segment(s) ({})",
        created.rows,
        created.segments,
        fmt_ms(t0.elapsed())
    );

    let rollup_defs: &[(&str, &str)] = &[
        ("lineitem_rollup_a", ROLLUP_A_SQL),
        ("lineitem_rollup_b", ROLLUP_B_SQL),
        ("lineitem_rollup_c", ROLLUP_C_SQL),
    ];
    for (name, sql) in rollup_defs.iter().take(num_rollups) {
        let t1 = Instant::now();
        let reg = ctx
            .register_rollup(*name, "lineitem_native", sql)
            .await
            .unwrap_or_else(|e| panic!("register_rollup {name} must succeed: {e}"));
        println!(
            "  [setup] register_rollup {name}: {} rows ({})",
            reg.rows,
            fmt_ms(t1.elapsed())
        );
    }

    ctx
}

async fn measure_scenario(label: &str, scratch_subdir: &str, num_rollups: usize) -> Vec<Duration> {
    println!("=== scenario: {label} ({num_rollups} rollup(s)) ===");
    let mut ctx = build_scenario(scratch_subdir, num_rollups).await;

    let mut elapsed_per_insert = Vec::with_capacity(INSERT_REPEATS);
    for i in 0..INSERT_REPEATS {
        let result = ctx
            .insert_into_native_table(INSERT_SQL)
            .await
            .unwrap_or_else(|e| panic!("insert #{i} must succeed: {e}"));
        assert_eq!(
            result.rollups_refreshed.len(),
            num_rollups,
            "every registered rollup must have been (attempted to be) refreshed"
        );
        for outcome in &result.rollups_refreshed {
            assert!(
                outcome.error.is_none(),
                "refresh of {} must succeed in this diagnostic: {:?}",
                outcome.rollup_name,
                outcome.error
            );
        }
        println!(
            "  INSERT #{i}: {} row(s) added, InsertResult::elapsed = {} ({} rollup(s) \
             refreshed)",
            result.rows_inserted,
            fmt_ms(result.elapsed),
            result.rollups_refreshed.len()
        );
        elapsed_per_insert.push(result.elapsed);
    }
    elapsed_per_insert
}

fn summarize(label: &str, samples: &[Duration]) {
    let total: Duration = samples.iter().sum();
    let avg = total / samples.len() as u32;
    let min = samples.iter().min().unwrap();
    let max = samples.iter().max().unwrap();
    println!(
        "{label}: avg={} min={} max={} (n={})",
        fmt_ms(avg),
        fmt_ms(*min),
        fmt_ms(*max),
        samples.len()
    );
}

#[tokio::main]
async fn main() {
    if !std::path::Path::new(SOURCE_PARQUET).exists() {
        eprintln!(
            "skipping: {SOURCE_PARQUET} not found (expected data/tpch-1gb to already exist in \
             this repo)"
        );
        return;
    }
    std::fs::create_dir_all(OUT_ROOT).expect("create scratch root");

    println!("=== native rollup refresh perf check: {SOURCE_PARQUET} ===");
    println!("INSERT statement (repeated {INSERT_REPEATS}x per scenario): {INSERT_SQL}\n");

    let zero = measure_scenario("0 rollups (baseline)", "scenario_0", 0).await;
    println!();
    let one = measure_scenario("1 rollup", "scenario_1", 1).await;
    println!();
    let three = measure_scenario("3 rollups", "scenario_3", 3).await;
    println!();

    println!("=== summary (InsertResult::elapsed, includes any eager rollup refresh) ===");
    summarize("0 rollups", &zero);
    summarize("1 rollup ", &one);
    summarize("3 rollups", &three);

    let base_avg = zero.iter().sum::<Duration>() / zero.len() as u32;
    let one_avg = one.iter().sum::<Duration>() / one.len() as u32;
    let three_avg = three.iter().sum::<Duration>() / three.len() as u32;
    println!(
        "\ndelta vs. 0-rollup baseline: 1 rollup = +{} ({:.1}x), 3 rollups = +{} ({:.1}x)",
        fmt_ms(one_avg.saturating_sub(base_avg)),
        one_avg.as_secs_f64() / base_avg.as_secs_f64().max(1e-9),
        fmt_ms(three_avg.saturating_sub(base_avg)),
        three_avg.as_secs_f64() / base_avg.as_secs_f64().max(1e-9),
    );
    println!(
        "per-rollup average refresh cost (3-rollup total / 3): {}",
        fmt_ms((three_avg.saturating_sub(base_avg)) / 3)
    );
}
