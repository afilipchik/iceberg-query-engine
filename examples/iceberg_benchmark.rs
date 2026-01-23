//! Iceberg larger-than-memory benchmark
//!
//! This example demonstrates processing an Iceberg table with a memory limit
//! smaller than the dataset size.

use query_engine::execution::{ExecutionConfig, ExecutionContext};
use query_engine::physical::operators::TableProvider;
use query_engine::storage::{ParquetTable, StreamingParquetReader};
use std::path::PathBuf;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();

    let data_path = args.get(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp/tpch-1gb"));

    let memory_limit_mb: usize = args.get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(64);

    println!("=== Iceberg Larger-Than-Memory Benchmark ===");
    println!("Data path: {:?}", data_path);
    println!("Memory limit: {} MB", memory_limit_mb);
    println!();

    // Test 1: Streaming Parquet reader (row-group by row-group)
    println!("--- Test 1: Streaming Parquet Reader ---");
    test_streaming_parquet(&data_path).await?;

    // Test 2: Memory-limited execution context
    println!("\n--- Test 2: Memory-Limited ExecutionContext ---");
    test_memory_limited_context(&data_path, memory_limit_mb).await?;

    // Test 3: Aggregation query (would benefit from spillable agg)
    println!("\n--- Test 3: Aggregation Query ---");
    test_aggregation_query(&data_path, memory_limit_mb).await?;

    Ok(())
}

async fn test_streaming_parquet(data_path: &PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let lineitem_path = data_path.join("lineitem.parquet");

    if !lineitem_path.exists() {
        println!("  Skipping: {} does not exist", lineitem_path.display());
        return Ok(());
    }

    let table = ParquetTable::try_new(&lineitem_path)?;
    println!("  File: {:?}", lineitem_path);
    println!("  Schema: {} columns", table.schema().fields().len());

    // Create streaming reader with small batch size
    let batch_size = 8192;
    let mut reader = StreamingParquetReader::from_table(&table, None, batch_size);

    let start = Instant::now();
    let mut total_rows = 0usize;
    let mut batch_count = 0usize;
    let mut peak_batch_size = 0usize;

    while let Some(batch) = reader.next_batch()? {
        let rows = batch.num_rows();
        total_rows += rows;
        batch_count += 1;
        peak_batch_size = peak_batch_size.max(batch.get_array_memory_size());
    }

    let elapsed = start.elapsed();

    println!("  Streaming read completed:");
    println!("    Total rows: {}", total_rows);
    println!("    Batches: {}", batch_count);
    println!("    Peak batch memory: {} KB", peak_batch_size / 1024);
    println!("    Time: {:.2?}", elapsed);
    println!("    Throughput: {:.2} M rows/sec", total_rows as f64 / elapsed.as_secs_f64() / 1_000_000.0);

    Ok(())
}

async fn test_memory_limited_context(data_path: &PathBuf, memory_limit_mb: usize) -> Result<(), Box<dyn std::error::Error>> {
    let memory_limit = memory_limit_mb * 1024 * 1024;

    let config = ExecutionConfig::new()
        .with_memory_limit(memory_limit)
        .with_spill_path(PathBuf::from("/tmp/query_spill"))
        .with_batch_size(8192);

    let mut ctx = ExecutionContext::with_config(config);

    // Register lineitem as Parquet table
    let lineitem_path = data_path.join("lineitem.parquet");
    if lineitem_path.exists() {
        ctx.register_parquet("lineitem", &lineitem_path)?;
        println!("  Registered lineitem table");
    }

    // Simple count query
    let sql = "SELECT COUNT(*) as cnt FROM lineitem";
    println!("  Query: {}", sql);

    let start = Instant::now();
    let result = ctx.sql(sql).await?;
    let elapsed = start.elapsed();

    println!("  Results:");
    println!("    Row count: {}", result.row_count);
    println!("    Time: {:.2?}", elapsed);
    println!("    Peak memory: {} KB", result.metrics.peak_memory_bytes / 1024);

    if let Some(ref spill) = result.metrics.spill_metrics {
        println!("    Spilled: {} KB", spill.bytes_spilled / 1024);
    }

    // Print actual result
    if !result.batches.is_empty() {
        let batch = &result.batches[0];
        if batch.num_columns() > 0 {
            let count_col = batch.column(0);
            if let Some(arr) = count_col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                println!("    COUNT(*) = {}", arr.value(0));
            }
        }
    }

    Ok(())
}

async fn test_aggregation_query(data_path: &PathBuf, memory_limit_mb: usize) -> Result<(), Box<dyn std::error::Error>> {
    let memory_limit = memory_limit_mb * 1024 * 1024;

    let config = ExecutionConfig::new()
        .with_memory_limit(memory_limit)
        .with_spill_path(PathBuf::from("/tmp/query_spill"));

    let mut ctx = ExecutionContext::with_config(config);

    // Register tables
    let lineitem_path = data_path.join("lineitem.parquet");
    if lineitem_path.exists() {
        ctx.register_parquet("lineitem", &lineitem_path)?;
    } else {
        println!("  Skipping: lineitem.parquet not found");
        return Ok(());
    }

    // TPC-H Q1 style aggregation
    let sql = r#"
        SELECT
            l_returnflag,
            l_linestatus,
            SUM(l_quantity) as sum_qty,
            SUM(l_extendedprice) as sum_price,
            COUNT(*) as count_order
        FROM lineitem
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    "#;

    println!("  Query: TPC-H Q1 style aggregation");

    let start = Instant::now();
    let result = ctx.sql(sql).await?;
    let elapsed = start.elapsed();

    println!("  Results:");
    println!("    Row count: {}", result.row_count);
    println!("    Time: {:.2?}", elapsed);
    println!("    Peak memory: {} KB", result.metrics.peak_memory_bytes / 1024);

    if let Some(ref spill) = result.metrics.spill_metrics {
        println!("    Spilled: {} KB", spill.bytes_spilled / 1024);
    }

    // Print results
    println!("  Query results:");
    for batch in &result.batches {
        let flag_col = batch.column(0).as_any().downcast_ref::<arrow::array::StringArray>();
        let status_col = batch.column(1).as_any().downcast_ref::<arrow::array::StringArray>();
        let qty_col = batch.column(2);
        let price_col = batch.column(3);
        let count_col = batch.column(4);

        for row in 0..batch.num_rows() {
            let flag = flag_col.map(|a| a.value(row)).unwrap_or("?");
            let status = status_col.map(|a| a.value(row)).unwrap_or("?");
            println!("    {} | {} | qty: {:?} | price: {:?} | count: {:?}",
                flag, status,
                get_numeric_value(qty_col, row),
                get_numeric_value(price_col, row),
                get_numeric_value(count_col, row));
        }
    }

    Ok(())
}

fn get_numeric_value(arr: &arrow::array::ArrayRef, row: usize) -> String {
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return format!("{}", a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return format!("{:.2}", a.value(row));
    }
    "?".to_string()
}
