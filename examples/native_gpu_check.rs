//! Task 008 (native-tables-foundation QA) diagnostic: does GPU aggregate
//! offload actually engage for a REAL `NativeTable` provider end to end?
//!
//! Task 007 unit-tested the identity plumbing (`NativeTable::identity()`
//! returns `Some` for a whole table, `None` for a shard) but explicitly
//! left "a live GPU-offload query against an actual native table" as
//! unexercised in its own window. This example closes that loop: it
//! registers a REAL `NativeTable` (via `ExecutionContext::
//! register_native_table`, the production provider -- deliberately NOT
//! `load-native --query`'s CLI path, which materializes through
//! `native_write::read_back` into a plain `MemoryTable` and so could
//! never exercise `NativeTable::identity()`/GPU eligibility no matter
//! what else changed), calls `ctx.enable_gpu_offload()` (every other
//! single-process CLI command does this; `load-native` is the one
//! command that doesn't -- a real, named gap, not fixed here since a
//! throwaway diagnostic is the smaller, safer way to get the evidence),
//! and runs TPC-H Q1/Q6 (both single-table `lineitem` aggregates, the
//! only GPU-eligible shapes that need no other native table) repeatedly
//! so the second+ iteration can show a warm (already VRAM-resident) GPU
//! path if one engages.
//!
//! Run with `--features gpu` and sample `nvidia-smi` concurrently to see
//! VRAM growth (the same independent-evidence pattern task 007/008 use
//! elsewhere -- never just trust a log line).
//!
//! ```text
//! LD_LIBRARY_PATH=.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib \
//!   cargo run --release --features gpu --example native_gpu_check
//! ```

use query_engine::{ExecutionConfig, ExecutionContext};
use std::time::Instant;

const Q1: &str = "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
     SUM(l_extendedprice) AS sum_base_price, \
     SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price, \
     SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge, \
     AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price, \
     AVG(l_discount) AS avg_disc, COUNT(*) AS count_order \
     FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' \
     GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus";

const Q6: &str = "SELECT SUM(l_extendedprice * l_discount) AS revenue FROM lineitem \
     WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01' \
     AND l_discount >= 0.05 AND l_discount <= 0.07 AND l_quantity < 24";

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    query_engine::execution::topology::init_global_pool();

    let native_dir =
        std::env::var("NATIVE_DIR").unwrap_or_else(|_| "data/tpch-10gb-native/lineitem".into());
    let mem_limit_gb: usize = std::env::var("MEM_LIMIT_GB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(40);

    let config = ExecutionConfig::default().with_memory_limit(mem_limit_gb * 1024 * 1024 * 1024);
    let mut ctx = ExecutionContext::with_config(config);
    #[cfg(feature = "gpu")]
    if std::env::var("QE_GPU").as_deref() != Ok("0") {
        ctx.enable_gpu_offload();
        println!("GPU offload ENABLED (register_native_table -> real NativeTable provider)");
    } else {
        println!("GPU offload NOT enabled (QE_GPU=0)");
    }
    #[cfg(not(feature = "gpu"))]
    println!("(default build, no gpu feature compiled in -- this is the CPU baseline)");

    ctx.register_native_table("lineitem", &native_dir)?;
    println!("registered native lineitem from {native_dir}");

    // native-tables-tiering task 003: default stays 6 (unchanged from every
    // prior recorded measurement of this example) -- QE_GPU_CHECK_ITERS
    // exists only so a slower-than-usual upload warm-up window (this
    // program's own established shared-machine-contention caveat, see
    // CLAUDE.md's "GPU Aggregate Offload" section) can still be given
    // enough iterations to reach a genuinely fully-resident warm state
    // before reporting a "warm" number, without changing the default
    // behavior anything else measuring this example relies on.
    let iters: usize = std::env::var("QE_GPU_CHECK_ITERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6);
    for (label, sql) in [("Q1", Q1), ("Q6", Q6)] {
        println!("\n=== {label} ===");
        for iter in 1..=iters {
            let t0 = Instant::now();
            let result = ctx.sql(sql).await?;
            println!(
                "  iter {iter}: {} rows in {:?}",
                result.row_count,
                t0.elapsed()
            );
        }
    }
    Ok(())
}
