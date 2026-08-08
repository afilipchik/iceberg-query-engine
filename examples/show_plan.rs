//! Print the statistics-aware optimized logical plan for SQL against Parquet data.
//! Usage: SQL="..." cargo run --release --example show_plan -- <data_dir> [qnum] [sf]
use query_engine::{tpch, ExecutionContext};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = std::path::PathBuf::from(
        args.get(1)
            .cloned()
            .unwrap_or_else(|| "./data/tpch-10gb".to_string()),
    );
    let qnum: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(9);
    let sf: f64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10.0);

    let mut ctx = ExecutionContext::new();
    for table in [
        "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
    ] {
        ctx.register_parquet(table, path.join(format!("{}.parquet", table)))
            .unwrap();
    }
    let sql =
        std::env::var("SQL").unwrap_or_else(|_| tpch::get_query_for_sf(qnum, sf).expect("query"));
    match ctx.optimized_plan(&sql) {
        Ok(p) => println!("{}", p),
        Err(e) => println!("ERROR: {}", e),
    }
}
