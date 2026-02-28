//! Debug script to understand why subquery decorrelation fails for Q17-Q22
//!
//! Run with: cargo run --example debug_decorrelation

use query_engine::tpch::{TpchGenerator, Q17, Q21, Q22};
use query_engine::ExecutionContext;
use std::time::Instant;

#[tokio::main]
async fn main() {
    // Use small scale factor for quick debugging
    let sf = 0.001;
    println!("=== Subquery Decorrelation Debug ===");
    println!("Scale factor: {}", sf);
    println!();

    // Generate TPC-H data
    println!("Generating TPC-H data...");
    let mut gen = TpchGenerator::new(sf);
    let mut ctx = ExecutionContext::new();
    gen.generate_all(&mut ctx);
    println!("Done.\n");

    // Test queries with subqueries
    let test_cases = vec![
        ("Q17 - Scalar subquery", Q17),
        ("Q21 - EXISTS + NOT EXISTS with aliases", Q21),
        ("Q22 - NOT EXISTS", Q22),
    ];

    for (name, query) in test_cases {
        println!("========================================");
        println!("Testing: {}", name);
        println!("========================================");
        println!("Query:\n{}\n", query.trim());

        let start = Instant::now();
        match ctx.sql(query).await {
            Ok(result) => {
                let elapsed = start.elapsed();
                println!("✓ Success: {} rows in {:?}", result.row_count, elapsed);

                // Print first few rows
                if !result.batches.is_empty() {
                    let batch = &result.batches[0];
                    println!(
                        "Schema: {:?}",
                        batch
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| f.name())
                            .collect::<Vec<_>>()
                    );
                    if batch.num_rows() > 0 {
                        println!("First row sample available");
                    }
                }
            }
            Err(e) => {
                let elapsed = start.elapsed();
                println!("✗ Error after {:?}: {}", elapsed, e);
            }
        }
        println!();
    }

    // Also test a simpler EXISTS to verify basic functionality
    println!("========================================");
    println!("Testing: Simple EXISTS (should decorrelate)");
    println!("========================================");
    let simple_exists = r#"
        SELECT o_orderkey, o_totalprice
        FROM orders
        WHERE EXISTS (
            SELECT 1 FROM lineitem WHERE l_orderkey = o_orderkey
        )
        LIMIT 10
    "#;
    println!("Query:\n{}\n", simple_exists.trim());

    let start = Instant::now();
    match ctx.sql(simple_exists).await {
        Ok(result) => {
            println!(
                "✓ Success: {} rows in {:?}",
                result.row_count,
                start.elapsed()
            );
        }
        Err(e) => {
            println!("✗ Error: {}", e);
        }
    }
}
