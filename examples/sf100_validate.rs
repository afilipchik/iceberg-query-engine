//! Run all 22 TPC-H queries at SF=100 and save results as CSV for comparison with DuckDB.
//! Each query has a timeout of 10x DuckDB time (min 30s, max 600s).
//!
//! Usage: cargo run --release --example sf100_validate

use query_engine::ExecutionContext;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

const DATA_DIR: &str =
    "/media/afilipchik/nvme6tb/src/afilipchik/iceberg-query-engine/data/tpch-100gb";
const OUTPUT_DIR: &str =
    "/media/afilipchik/nvme6tb/src/afilipchik/iceberg-query-engine/data/sf100_engine_results";
const DUCKDB_DIR: &str =
    "/media/afilipchik/nvme6tb/src/afilipchik/iceberg-query-engine/data/sf100_duckdb_results";

// DuckDB best times at SF=100 in ms
const DUCKDB_TIMES: [(usize, u64); 22] = [
    (1, 2773),
    (2, 313),
    (3, 2755),
    (4, 1793),
    (5, 2265),
    (6, 1191),
    (7, 2411),
    (8, 2698),
    (9, 28094),
    (10, 1096),
    (11, 96),
    (12, 985),
    (13, 1251),
    (14, 463),
    (15, 994),
    (16, 709),
    (17, 2191),
    (18, 4748),
    (19, 2625),
    (20, 3210),
    (21, 4012),
    (22, 393),
];

fn get_timeout(qnum: usize) -> Duration {
    let duckdb_ms = DUCKDB_TIMES
        .iter()
        .find(|(q, _)| *q == qnum)
        .map(|(_, t)| *t)
        .unwrap_or(5000);
    let timeout_ms = (duckdb_ms * 10).max(30_000).min(600_000);
    Duration::from_millis(timeout_ms)
}

#[tokio::main]
async fn main() {
    let sf = 100.0;

    fs::create_dir_all(OUTPUT_DIR).unwrap();

    println!("Engine TPC-H Benchmark + Validation (SF=100)");
    println!("Data directory: {}", DATA_DIR);
    println!("Output directory: {}", OUTPUT_DIR);

    let memory_limit = 64 * 1024 * 1024 * 1024; // 64GB
    println!("Memory limit: {} GB", memory_limit / (1024 * 1024 * 1024));
    let mut ctx = ExecutionContext::with_memory_limit(memory_limit);

    let tables = [
        "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
    ];

    let load_start = Instant::now();
    for table in &tables {
        let file_path = PathBuf::from(DATA_DIR).join(format!("{}.parquet", table));
        match ctx.register_parquet(*table, &file_path) {
            Ok(()) => {
                if let Some(schema) = ctx.table_schema(table) {
                    println!("  Loaded {}: {} columns", table, schema.fields().len());
                }
            }
            Err(e) => {
                eprintln!("Error loading {}: {}", table, e);
                std::process::exit(1);
            }
        }
    }
    println!("Data loaded in {:?}\n", load_start.elapsed());

    println!(
        "{:<8} {:<12} {:<12} {:<8} {:<8} {}",
        "Query", "Engine(ms)", "DuckDB(ms)", "Ratio", "Rows", "Status"
    );
    println!("{}", "=".repeat(80));

    let mut total_engine_ms = 0.0f64;
    let mut total_duckdb_ms = 0.0f64;
    let mut pass_count = 0;
    let mut fail_count = 0;
    let mut timeout_count = 0;
    let mut error_count = 0;

    let mut summary_rows: Vec<(usize, f64, u64, usize, String)> = Vec::new();

    for q in 1..=22 {
        let sql = match query_engine::tpch::get_query_for_sf(q, sf) {
            Some(s) => s,
            None => {
                println!("Q{:<7} SKIPPED (no query)", q);
                continue;
            }
        };

        let duckdb_ms = DUCKDB_TIMES
            .iter()
            .find(|(qn, _)| *qn == q)
            .map(|(_, t)| *t)
            .unwrap_or(5000);
        let timeout = get_timeout(q);

        let start = Instant::now();
        let result = tokio::time::timeout(timeout, ctx.sql(&sql)).await;
        let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

        match result {
            Ok(Ok(result)) => {
                total_engine_ms += elapsed_ms;
                total_duckdb_ms += duckdb_ms as f64;

                // Save results as CSV
                let csv_path = format!("{}/q{:02}.csv", OUTPUT_DIR, q);
                save_results_csv(&result, &csv_path);

                // Compare with DuckDB
                let duckdb_path = format!("{}/q{:02}.csv", DUCKDB_DIR, q);
                let (status, status_short) = if std::path::Path::new(&duckdb_path).exists() {
                    match compare_csv(&csv_path, &duckdb_path, q) {
                        Ok(true) => {
                            pass_count += 1;
                            ("PASS".to_string(), "PASS")
                        }
                        Ok(false) => {
                            fail_count += 1;
                            ("FAIL".to_string(), "FAIL")
                        }
                        Err(e) => {
                            fail_count += 1;
                            (format!("FAIL: {}", e), "FAIL")
                        }
                    }
                } else {
                    pass_count += 1;
                    ("OK (no ref)".to_string(), "OK")
                };

                let ratio = elapsed_ms / duckdb_ms as f64;
                println!(
                    "Q{:<7} {:<12.1} {:<12} {:<8.1}x {:<8} {}",
                    q, elapsed_ms, duckdb_ms, ratio, result.row_count, status
                );
                summary_rows.push((
                    q,
                    elapsed_ms,
                    duckdb_ms,
                    result.row_count,
                    status_short.to_string(),
                ));
            }
            Ok(Err(e)) => {
                error_count += 1;
                println!(
                    "Q{:<7} {:<12.1} {:<12} {:<8} {:<8} ERROR: {}",
                    q, elapsed_ms, duckdb_ms, "-", 0, e
                );
                summary_rows.push((q, elapsed_ms, duckdb_ms, 0, "ERROR".to_string()));
            }
            Err(_) => {
                timeout_count += 1;
                let timeout_s = timeout.as_secs();
                println!(
                    "Q{:<7} {:<12} {:<12} {:<8} {:<8} TIMEOUT (>{}s)",
                    q,
                    format!(">{}s", timeout_s),
                    duckdb_ms,
                    ">10x",
                    "-",
                    timeout_s
                );
                summary_rows.push((
                    q,
                    timeout.as_secs_f64() * 1000.0,
                    duckdb_ms,
                    0,
                    "TIMEOUT".to_string(),
                ));
            }
        }
    }

    println!("{}", "=".repeat(80));
    println!("\nSummary:");
    println!("  Passed: {}", pass_count);
    println!("  Failed: {}", fail_count);
    println!("  Timeout: {}", timeout_count);
    println!("  Error: {}", error_count);
    if total_duckdb_ms > 0.0 {
        println!(
            "  Total engine time (completed): {:.1}ms ({:.1}s)",
            total_engine_ms,
            total_engine_ms / 1000.0
        );
        println!(
            "  Total DuckDB time (completed): {:.1}ms ({:.1}s)",
            total_duckdb_ms,
            total_duckdb_ms / 1000.0
        );
        println!("  Overall ratio: {:.1}x", total_engine_ms / total_duckdb_ms);
    }

    // Print markdown table for easy recording
    println!("\n\n### Results Table (Markdown)\n");
    println!("| Query | Engine (ms) | DuckDB (ms) | Ratio | Rows | Status |");
    println!("|-------|-------------|-------------|-------|------|--------|");
    for (q, ems, dms, rows, status) in &summary_rows {
        let ratio = if *status == "TIMEOUT" {
            ">10x".to_string()
        } else {
            format!("{:.1}x", ems / *dms as f64)
        };
        println!(
            "| Q{:02}   | {:<11.1} | {:<11} | {:<5} | {:<4} | {} |",
            q, ems, dms, ratio, rows, status
        );
    }
}

fn save_results_csv(result: &query_engine::QueryResult, path: &str) {
    use arrow::array::*;
    use arrow::datatypes::DataType;
    use std::io::Write;

    let mut file = fs::File::create(path).unwrap();

    if result.batches.is_empty() {
        return;
    }

    let schema = result.batches[0].schema();
    let headers: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    writeln!(file, "{}", headers.join(",")).unwrap();

    for batch in &result.batches {
        for row_idx in 0..batch.num_rows() {
            let mut values = Vec::new();
            for col_idx in 0..batch.num_columns() {
                let col = batch.column(col_idx);
                if col.is_null(row_idx) {
                    values.push(String::new());
                    continue;
                }
                let val = match col.data_type() {
                    DataType::Boolean => {
                        let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                        format!("{}", arr.value(row_idx))
                    }
                    DataType::Int8 => {
                        let arr = col.as_any().downcast_ref::<Int8Array>().unwrap();
                        format!("{}", arr.value(row_idx))
                    }
                    DataType::Int16 => {
                        let arr = col.as_any().downcast_ref::<Int16Array>().unwrap();
                        format!("{}", arr.value(row_idx))
                    }
                    DataType::Int32 => {
                        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                        format!("{}", arr.value(row_idx))
                    }
                    DataType::Int64 => {
                        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        format!("{}", arr.value(row_idx))
                    }
                    DataType::UInt64 => {
                        let arr = col.as_any().downcast_ref::<UInt64Array>().unwrap();
                        format!("{}", arr.value(row_idx))
                    }
                    DataType::Float32 => {
                        let arr = col.as_any().downcast_ref::<Float32Array>().unwrap();
                        format!("{:.4}", arr.value(row_idx))
                    }
                    DataType::Float64 => {
                        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                        format!("{:.4}", arr.value(row_idx))
                    }
                    DataType::Utf8 => {
                        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                        let s = arr.value(row_idx);
                        if s.contains(',') || s.contains('"') || s.contains('\n') {
                            format!("\"{}\"", s.replace('"', "\"\""))
                        } else {
                            s.to_string()
                        }
                    }
                    DataType::Date32 => {
                        let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
                        let days = arr.value(row_idx);
                        let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                            .unwrap_or_default();
                        format!("{}", date)
                    }
                    DataType::Timestamp(_, _) => {
                        if let Some(arr) = col.as_any().downcast_ref::<TimestampMicrosecondArray>()
                        {
                            let us = arr.value(row_idx);
                            let secs = us / 1_000_000;
                            let nsecs = ((us % 1_000_000) * 1000) as u32;
                            let dt =
                                chrono::DateTime::from_timestamp(secs, nsecs).unwrap_or_default();
                            format!("{}", dt.format("%Y-%m-%d"))
                        } else if let Some(arr) =
                            col.as_any().downcast_ref::<TimestampNanosecondArray>()
                        {
                            let ns = arr.value(row_idx);
                            let secs = ns / 1_000_000_000;
                            let nsecs = (ns % 1_000_000_000) as u32;
                            let dt =
                                chrono::DateTime::from_timestamp(secs, nsecs).unwrap_or_default();
                            format!("{}", dt.format("%Y-%m-%d"))
                        } else {
                            "?".to_string()
                        }
                    }
                    _ => format!("{:?}", col.data_type()),
                };
                values.push(val);
            }
            writeln!(file, "{}", values.join(",")).unwrap();
        }
    }
}

fn compare_csv(engine_path: &str, duckdb_path: &str, qnum: usize) -> Result<bool, String> {
    let engine_content =
        fs::read_to_string(engine_path).map_err(|e| format!("read engine: {}", e))?;
    let duckdb_content =
        fs::read_to_string(duckdb_path).map_err(|e| format!("read duckdb: {}", e))?;

    let engine_lines: Vec<&str> = engine_content.trim().lines().collect();
    let duckdb_lines: Vec<&str> = duckdb_content.trim().lines().collect();

    let engine_rows = engine_lines.len().saturating_sub(1);
    let duckdb_rows = duckdb_lines.len().saturating_sub(1);

    if engine_rows != duckdb_rows {
        return Err(format!(
            "row count: engine={} vs duckdb={}",
            engine_rows, duckdb_rows
        ));
    }

    // Compare data rows (skip header)
    for (i, (e_line, d_line)) in engine_lines
        .iter()
        .skip(1)
        .zip(duckdb_lines.iter().skip(1))
        .enumerate()
    {
        let e_fields: Vec<&str> = e_line.split(',').collect();
        let d_fields: Vec<&str> = d_line.split(',').collect();

        if e_fields.len() != d_fields.len() {
            return Err(format!(
                "row {} cols: engine={} vs duckdb={}",
                i + 1,
                e_fields.len(),
                d_fields.len()
            ));
        }

        for (col_idx, (ev, dv)) in e_fields.iter().zip(d_fields.iter()).enumerate() {
            if !values_match(ev.trim(), dv.trim()) {
                return Err(format!(
                    "row {} col {}: engine='{}' vs duckdb='{}'",
                    i + 1,
                    col_idx,
                    ev.trim(),
                    dv.trim()
                ));
            }
        }
    }

    Ok(true)
}

fn values_match(a: &str, b: &str) -> bool {
    if a == b {
        return true;
    }

    // Try numeric comparison with tolerance
    if let (Ok(fa), Ok(fb)) = (a.parse::<f64>(), b.parse::<f64>()) {
        if fa == 0.0 && fb == 0.0 {
            return true;
        }
        let abs_diff = (fa - fb).abs();
        let rel_diff = abs_diff / fa.abs().max(fb.abs()).max(1e-10);
        // 0.01% relative tolerance or 0.01 absolute tolerance
        return rel_diff < 0.0001 || abs_diff < 0.01;
    }

    false
}
