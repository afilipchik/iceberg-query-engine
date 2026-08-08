//! Print per-phase timing breakdown for a TPC-H query against Parquet data.
//!
//! Usage: cargo run --release --example phase_profile -- <data_dir> <query_num>

use query_engine::{tpch, ExecutionContext};
use std::time::Instant;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = std::path::PathBuf::from(
        args.get(1)
            .cloned()
            .unwrap_or_else(|| "./data/tpch-10gb".to_string()),
    );
    let qnum: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(14);
    let sf: f64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10.0);

    let mut ctx = ExecutionContext::with_memory_limit(48 * 1024 * 1024 * 1024);
    for table in [
        "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
    ] {
        ctx.register_parquet(table, path.join(format!("{}.parquet", table)))
            .unwrap();
    }

    let sql = match std::env::var("SQL") {
        Ok(s) => s,
        Err(_) => tpch::get_query_for_sf(qnum, sf).expect("query"),
    };

    if std::env::var("SHOW_PLAN").is_ok() {
        let phys = ctx.physical_plan(&sql).expect("physical plan");
        println!("=== physical plan ===");
        print!("{}", query_engine::physical::display_plan(phys.as_ref(), 0));
        println!("=== partitions: {} ===", phys.output_partitions());
    }

    let t = Instant::now();
    match ctx.sql(&sql).await {
        Ok(r) => {
            println!(
                "Q{:02}: rows={} total={:?}\n  parse={:?}\n  plan(bind+physical)={:?}\n  optimize={:?}\n  execute={:?}",
                qnum,
                r.row_count,
                t.elapsed(),
                r.metrics.parse_time,
                r.metrics.plan_time,
                r.metrics.optimize_time,
                r.metrics.execute_time
            );
        }
        Err(e) => println!("Q{:02}: ERROR {}", qnum, e),
    }
}
