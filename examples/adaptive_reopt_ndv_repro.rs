//! adaptive-reopt-investigation (2026-08-27): a real, live repro of the
//! "bad/missing NDV statistics cause the DPsize join reorderer to pick a
//! catastrophic join order" failure class this codebase's CLAUDE.md already
//! names one historical instance of (Lance/Q05, ~1.2-billion-row
//! intermediate, fixed by adding integer-column NDV stats). This binary
//! builds a FRESH, live instance of the same failure class against REAL
//! SF=10 parquet data, to inform a go/no-go decision on investing in
//! adaptive/runtime join-order re-optimization.
//!
//! TPC-H Q5 is a genuine 6-way join (customer, orders, lineitem, supplier,
//! nation, region) with a real alternate join edge that is dangerous if
//! mis-costed: `c_nationkey = s_nationkey` joins `customer` directly to
//! `supplier` on a foreign-key column whose TRUE cardinality is tiny (25
//! distinct nation values), even though `customer`/`supplier` themselves
//! have far more rows (1.5M / 100K at SF=10). If the cost model does not
//! know that `nationkey`'s NDV is ~25 (not ~customer's or ~supplier's own
//! row count), it can drastically misprice this edge and route the DP
//! through a `customer x supplier` intermediate with many millions/billions
//! of rows — this is the exact shape CLAUDE.md's Lance/Q05 incident
//! describes ("supplier ⋈ customer on nationkey").
//!
//! **The corruption mechanism is a REAL, first-class, documented code path
//! in this engine, not a synthetic hack**: `ExecutionContext::register_table`
//! (in-memory registration, `MemoryTable`) is a normal, public,
//! already-used API. `MemoryTable::statistics()` (src/physical/operators/
//! scan.rs) returns a real row count but an EMPTY `column_stats` map — i.e.
//! zero NDV information for every column, always, by construction. This is
//! the "cold-start table with no stats yet" scenario named in the
//! investigation brief: any table loaded via `register_table`/`register_batch`
//! instead of `register_parquet`/`register_native_table` hits this exact
//! path today, with no warning.
//!
//! Modes:
//!   --mode=plan-only    just print both optimized plans + DP debug (SAFE,
//!                        always run this first — no execution, no risk)
//!   --mode=accurate     execute Q5 with all 6 tables as ParquetTable
//!                        (real footer stats, today's normal behavior)
//!   --mode=corrupted     execute Q5 with `customer`+`supplier` registered
//!                        via `register_table` (MemoryTable, zero column
//!                        stats) and the rest as ParquetTable
//!
//! Usage:
//!   cargo run --release --example adaptive_reopt_ndv_repro -- \
//!       --data data/tpch-10gb --mode plan-only
//!   cargo run --release --example adaptive_reopt_ndv_repro -- \
//!       --data data/tpch-10gb --mode accurate
//!   cargo run --release --example adaptive_reopt_ndv_repro -- \
//!       --data data/tpch-10gb --mode corrupted
//!
//! The `corrupted` mode is run inside a memory+time-capped cgroup by the
//! investigation's own driver script/commands (see the investigation's
//! final report) — do not run it bare on a shared machine, it is
//! deliberately constructing a plan that may attempt a many-billion-row
//! intermediate.

use query_engine::physical::TableProvider;
use query_engine::planner::LogicalPlan;
use query_engine::storage::ParquetTable;
use query_engine::{ExecutionConfig, ExecutionContext};
use std::sync::Arc;
use std::time::Instant;

const TABLES: &[&str] = &[
    "customer", "orders", "lineitem", "supplier", "nation", "region",
];

const Q5: &str = r#"
SELECT
    n_name,
    SUM(l_extendedprice * (1 - l_discount)) AS revenue
FROM
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
WHERE
    c_custkey = o_custkey
    AND l_orderkey = o_orderkey
    AND l_suppkey = s_suppkey
    AND c_nationkey = s_nationkey
    AND s_nationkey = n_nationkey
    AND n_regionkey = r_regionkey
    AND r_name = 'ASIA'
    AND o_orderdate >= DATE '1994-01-01'
    AND o_orderdate < DATE '1995-01-01'
GROUP BY
    n_name
ORDER BY
    revenue DESC
"#;

/// Render a join tree's SHAPE (which base tables get joined together, and in
/// what nesting) so the plan diff is readable without dumping the entire
/// Debug tree. `(a JOIN b)` means a and b are joined directly.
fn describe_join_shape(plan: &LogicalPlan) -> String {
    match plan {
        LogicalPlan::Scan(node) => node.table_name.clone(),
        LogicalPlan::Join(node) => format!(
            "({} JOIN {})",
            describe_join_shape(&node.left),
            describe_join_shape(&node.right)
        ),
        LogicalPlan::Filter(node) => describe_join_shape(&node.input),
        LogicalPlan::Project(node) => describe_join_shape(&node.input),
        LogicalPlan::Aggregate(node) => describe_join_shape(&node.input),
        LogicalPlan::Sort(node) => describe_join_shape(&node.input),
        LogicalPlan::Limit(node) => describe_join_shape(&node.input),
        LogicalPlan::SubqueryAlias(node) => describe_join_shape(&node.input),
        other => format!("<{:?}>", std::mem::discriminant(other)),
    }
}

fn build_ctx(data_dir: &str, corrupt: &[&str], memory_limit_bytes: usize) -> ExecutionContext {
    let config = ExecutionConfig::default().with_memory_limit(memory_limit_bytes);
    let mut ctx = ExecutionContext::with_config(config);

    for &t in TABLES {
        let path = format!("{}/{}.parquet", data_dir, t);
        if corrupt.contains(&t) {
            // Real, first-class code path: load the SAME real parquet data,
            // but register it as an in-memory table. MemoryTable::statistics()
            // reports a real row count but an EMPTY column_stats map, so the
            // DPsize cost model has zero NDV information for this relation's
            // join keys — exactly the "cold-start table with no stats yet"
            // scenario. Nothing about join_reorder.rs is touched.
            let table = ParquetTable::try_new(&path)
                .unwrap_or_else(|e| panic!("failed to open {}: {}", path, e));
            let schema = table.schema();
            let batches = table
                .scan(None)
                .unwrap_or_else(|e| panic!("failed to scan {}: {}", path, e));
            eprintln!(
                "[repro] {} loaded via register_table (MemoryTable, NO column stats), {} rows",
                t,
                batches.iter().map(|b| b.num_rows()).sum::<usize>()
            );
            ctx.register_table(t, schema, batches);
        } else {
            ctx.register_parquet(t, &path)
                .unwrap_or_else(|e| panic!("failed to register {}: {}", path, e));
            eprintln!(
                "[repro] {} loaded via register_parquet (real footer stats)",
                t
            );
        }
    }
    ctx
}

fn parse_args() -> (String, String) {
    let mut data_dir = "data/tpch-10gb".to_string();
    let mut mode = "plan-only".to_string();
    let args: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--data" => {
                data_dir = args.get(i + 1).cloned().unwrap_or(data_dir);
                i += 2;
            }
            "--mode" => {
                mode = args.get(i + 1).cloned().unwrap_or(mode);
                i += 2;
            }
            other => {
                eprintln!("unknown arg: {}", other);
                i += 1;
            }
        }
    }
    (data_dir, mode)
}

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    let (data_dir, mode) = parse_args();
    // Both diagnostic switches this codebase already ships (CLAUDE.md);
    // set programmatically so a single binary invocation prints everything
    // needed without the caller having to remember two env vars.
    std::env::set_var("PLAN_DEBUG", "1");
    std::env::set_var("DP_DEBUG", "1");

    // Generous but bounded — this repro is meant to run under the
    // investigation's own systemd-run memory cap; the in-process limit here
    // is a second, independent line of defense so a spill-capable operator
    // has a defined budget to spill against instead of growing unbounded.
    let memory_limit_bytes = 24usize * 1024 * 1024 * 1024; // 24 GiB

    eprintln!(
        "=== adaptive_reopt_ndv_repro: data={} mode={} ===",
        data_dir, mode
    );

    match mode.as_str() {
        "plan-only" => {
            eprintln!("\n--- ACCURATE (all ParquetTable, real footer NDV stats) ---");
            let ctx_ok = build_ctx(&data_dir, &[], memory_limit_bytes);
            let plan_ok = ctx_ok.optimized_plan(Q5)?;
            eprintln!("[shape] {}", describe_join_shape(&plan_ok));

            eprintln!("\n--- CORRUPTED (customer+supplier via register_table, zero NDV stats) ---");
            let ctx_bad = build_ctx(&data_dir, &["customer", "supplier"], memory_limit_bytes);
            let plan_bad = ctx_bad.optimized_plan(Q5)?;
            eprintln!("[shape] {}", describe_join_shape(&plan_bad));

            let same = describe_join_shape(&plan_ok) == describe_join_shape(&plan_bad);
            eprintln!(
                "\n=== RESULT: join shapes {} ===",
                if same {
                    "IDENTICAL (no repro)"
                } else {
                    "DIFFER (repro confirmed)"
                }
            );
        }
        "accurate" => {
            let ctx = build_ctx(&data_dir, &[], memory_limit_bytes);
            let plan = ctx.optimized_plan(Q5)?;
            eprintln!("[shape] {}", describe_join_shape(&plan));
            let start = Instant::now();
            let result = ctx.sql(Q5).await?;
            let elapsed = start.elapsed();
            eprintln!(
                "=== ACCURATE RESULT: rows={} elapsed={:?} ===",
                result.row_count, elapsed
            );
        }
        "corrupted" => {
            let ctx = build_ctx(&data_dir, &["customer", "supplier"], memory_limit_bytes);
            let plan = ctx.optimized_plan(Q5)?;
            eprintln!("[shape] {}", describe_join_shape(&plan));
            let start = Instant::now();
            let result = ctx.sql(Q5).await?;
            let elapsed = start.elapsed();
            eprintln!(
                "=== CORRUPTED RESULT: rows={} elapsed={:?} ===",
                result.row_count, elapsed
            );
        }
        other => {
            eprintln!(
                "unknown mode: {} (expected plan-only|accurate|corrupted)",
                other
            );
            std::process::exit(2);
        }
    }

    let _ = Arc::new(()); // silence unused Arc import if a branch is trimmed later
    Ok(())
}
