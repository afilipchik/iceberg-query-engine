//! Vectorized TPC-H Q1 with row-group filtering
//!
//! This example demonstrates:
//! - Row-group statistics filtering (skips row groups based on min/max)
//! - Vectorized aggregation with typed hash tables
//! - Selection vector processing
//!
//! Run with: cargo run --release --example vectorized_q1

use query_engine::error::Result;
use query_engine::physical::vectorized_agg::execute_vectorized_q1;
use std::time::Instant;

fn main() -> Result<()> {
    let path = "./data/tpch-10gb/lineitem.parquet";

    // TPC-H Q1 date filter: l_shipdate <= date '1998-12-01' - interval '90' day
    // 1998-09-02 = 10471 days from epoch (1970-01-01)
    let date_limit = 10471;

    println!("Running Vectorized TPC-H Q1 with Row-Group Filtering");
    println!("Path: {}", path);
    println!("Date limit: {} (days from epoch)", date_limit);
    println!("Threads: {}", rayon::current_num_threads());
    println!();

    let start = Instant::now();

    let results = execute_vectorized_q1(path, date_limit)?;

    let elapsed = start.elapsed();

    println!("Results ({} groups):", results.len());
    println!(
        "{:>12} {:>12} {:>18} {:>18} {:>18} {:>18} {:>15} {:>15} {:>15} {:>10}",
        "returnflag",
        "linestatus",
        "sum_qty",
        "sum_base_price",
        "sum_disc_price",
        "sum_charge",
        "avg_qty",
        "avg_price",
        "avg_disc",
        "count"
    );
    println!("{}", "-".repeat(160));

    for (returnflag, linestatus, acc) in &results {
        println!(
            "{:>12} {:>12} {:>18.2} {:>18.2} {:>18.2} {:>18.2} {:>15.2} {:>15.2} {:>15.6} {:>10}",
            returnflag,
            linestatus,
            acc.sum_qty,
            acc.sum_base_price,
            acc.sum_disc_price,
            acc.sum_charge,
            acc.avg_qty(),
            acc.avg_price(),
            acc.avg_disc(),
            acc.count
        );
    }

    println!();
    println!("Time: {:?}", elapsed);

    Ok(())
}
