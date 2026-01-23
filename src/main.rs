//! Query Engine CLI

use clap::{Parser, Subcommand};
use query_engine::execution::{print_results, ExecutionContext};
use query_engine::tpch::{self, TpchGenerator};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "query_engine")]
#[command(about = "High-performance SQL Query Engine")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Generate TPC-H data (in-memory only)
    Generate {
        /// Scale factor (0.01 = 10MB, 1 = 1GB, 10 = 10GB)
        #[arg(short, long, default_value = "0.01")]
        sf: f64,
    },

    /// Generate TPC-H data and write to Parquet files
    GenerateParquet {
        /// Scale factor (0.01 = 10MB, 1 = 1GB, 10 = 10GB)
        #[arg(short, long, default_value = "0.01")]
        sf: f64,

        /// Output directory
        #[arg(short, long)]
        output: PathBuf,
    },

    /// Run a single TPC-H query
    Query {
        /// Query number (1-22)
        #[arg(short, long)]
        num: usize,

        /// Scale factor
        #[arg(short, long, default_value = "0.01")]
        sf: f64,

        /// Show query plan
        #[arg(short, long)]
        plan: bool,
    },

    /// Run all TPC-H queries as benchmark
    Benchmark {
        /// Scale factor
        #[arg(short, long, default_value = "0.01")]
        sf: f64,

        /// Number of iterations
        #[arg(short, long, default_value = "1")]
        iterations: usize,
    },

    /// Run a custom SQL query
    Sql {
        /// SQL query string
        query: String,

        /// Scale factor (for TPC-H tables)
        #[arg(short, long, default_value = "0.01")]
        sf: f64,
    },

    /// Load Parquet file(s) and run a query
    LoadParquet {
        /// Path to Parquet file or directory
        #[arg(short, long)]
        path: PathBuf,

        /// Table name to register
        #[arg(short, long)]
        name: String,

        /// SQL query to execute (if omitted, just loads and shows schema)
        #[arg(short, long)]
        query: Option<String>,
    },

    /// Run TPC-H benchmark from Parquet files
    BenchmarkParquet {
        /// Path to directory containing TPC-H Parquet files
        #[arg(short, long)]
        path: PathBuf,

        /// Number of iterations
        #[arg(short, long, default_value = "1")]
        iterations: usize,
    },
}

#[tokio::main]
async fn main() {
    // Set up logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Generate { sf } => {
            println!("Generating TPC-H data with scale factor {}", sf);
            let start = Instant::now();

            let mut ctx = ExecutionContext::new();
            let mut gen = TpchGenerator::new(sf);
            gen.generate_all(&mut ctx);

            println!("Generated data in {:?}", start.elapsed());
            println!("Tables: {:?}", ctx.table_names());
        }

        Commands::GenerateParquet { sf, output } => {
            let start = Instant::now();
            let mut gen = TpchGenerator::new(sf);

            match gen.generate_to_parquet(&output) {
                Ok(()) => {
                    println!("\nGenerated Parquet files in {:?}", start.elapsed());
                }
                Err(e) => {
                    eprintln!("Error generating data: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::Query { num, sf, plan } => {
            let query = tpch::get_query(num);
            match query {
                Some(sql) => {
                    println!("Running TPC-H Q{} (SF={})", num, sf);
                    println!("Query:\n{}", sql);
                    println!();

                    let mut ctx = ExecutionContext::new();
                    let mut gen = TpchGenerator::new(sf);
                    gen.generate_all(&mut ctx);

                    if plan {
                        match ctx.logical_plan(sql) {
                            Ok(logical) => {
                                println!("Logical Plan:\n{}", logical);
                            }
                            Err(e) => {
                                println!("Error creating plan: {}", e);
                            }
                        }
                        println!();
                    }

                    match ctx.sql(sql).await {
                        Ok(result) => {
                            print_results(&result);
                        }
                        Err(e) => {
                            eprintln!("Error: {}", e);
                        }
                    }
                }
                None => {
                    eprintln!("Query {} not found. Valid queries: 1-22", num);
                }
            }
        }

        Commands::Benchmark { sf, iterations } => {
            println!("Running TPC-H benchmark (SF={}, iterations={})", sf, iterations);
            println!();

            let mut ctx = ExecutionContext::new();
            let mut gen = TpchGenerator::new(sf);
            gen.generate_all(&mut ctx);

            println!("Data generated. Running queries...\n");

            let mut total_time = std::time::Duration::ZERO;
            let mut results = Vec::new();

            for iter in 0..iterations {
                if iterations > 1 {
                    println!("=== Iteration {} ===", iter + 1);
                }

                let iter_start = Instant::now();

                for q in tpch::ALL_QUERIES {
                    if let Some(sql) = tpch::get_query(q) {
                        let start = Instant::now();
                        match ctx.sql(sql).await {
                            Ok(result) => {
                                let elapsed = start.elapsed();
                                println!(
                                    "Q{:02}: {:>8} rows in {:>8.3}ms",
                                    q,
                                    result.row_count,
                                    elapsed.as_secs_f64() * 1000.0
                                );
                                if iter == 0 {
                                    results.push((q, result.row_count, elapsed));
                                }
                            }
                            Err(e) => {
                                println!("Q{:02}: ERROR - {}", q, e);
                                if iter == 0 {
                                    results.push((q, 0, std::time::Duration::ZERO));
                                }
                            }
                        }
                    }
                }

                let iter_time = iter_start.elapsed();
                total_time += iter_time;
                println!("Iteration time: {:?}\n", iter_time);
            }

            println!("=== Summary ===");
            println!("Total time: {:?}", total_time);
            println!("Average iteration: {:?}", total_time / iterations as u32);

            let query_total: std::time::Duration = results.iter().map(|(_, _, t)| *t).sum();
            println!("Query execution time: {:?}", query_total);

            let successful = results.iter().filter(|(_, rows, _)| *rows > 0).count();
            println!("Successful queries: {}/{}", successful, results.len());
        }

        Commands::Sql { query, sf } => {
            let mut ctx = ExecutionContext::new();
            let mut gen = TpchGenerator::new(sf);
            gen.generate_all(&mut ctx);

            println!("Running query: {}", query);
            println!();

            match ctx.sql(&query).await {
                Ok(result) => {
                    print_results(&result);
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                }
            }
        }

        Commands::LoadParquet { path, name, query } => {
            let start = Instant::now();
            let mut ctx = ExecutionContext::new();

            println!("Loading Parquet from: {}", path.display());

            match ctx.register_parquet(&name, &path) {
                Ok(()) => {
                    println!("Registered table '{}' in {:?}", name, start.elapsed());

                    if let Some(schema) = ctx.table_schema(&name) {
                        println!("Schema: {} columns", schema.fields().len());
                        for field in schema.fields() {
                            println!("  - {}: {:?}", field.name(), field.data_type());
                        }
                    }
                    println!();

                    if let Some(sql) = query {
                        println!("Running query: {}", sql);
                        println!();

                        match ctx.sql(&sql).await {
                            Ok(result) => {
                                print_results(&result);
                            }
                            Err(e) => {
                                eprintln!("Error: {}", e);
                                std::process::exit(1);
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("Error loading Parquet: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::BenchmarkParquet { path, iterations } => {
            println!("Running TPC-H benchmark from Parquet files");
            println!("Path: {}", path.display());
            println!("Iterations: {}", iterations);
            println!();

            let start = Instant::now();
            let mut ctx = ExecutionContext::new();

            // Load all TPC-H tables from Parquet files
            let tables = ["nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem"];

            for table in &tables {
                let file_path = path.join(format!("{}.parquet", table));
                match ctx.register_parquet(*table, &file_path) {
                    Ok(()) => {
                        if let Some(schema) = ctx.table_schema(*table) {
                            println!("  Loaded {}: {} columns", table, schema.fields().len());
                        }
                    }
                    Err(e) => {
                        eprintln!("Error loading {}: {}", table, e);
                        std::process::exit(1);
                    }
                }
            }

            println!("\nData loaded in {:?}. Running queries...\n", start.elapsed());

            let mut total_time = std::time::Duration::ZERO;
            let mut results = Vec::new();

            for iter in 0..iterations {
                if iterations > 1 {
                    println!("=== Iteration {} ===", iter + 1);
                }

                let iter_start = Instant::now();

                for q in tpch::ALL_QUERIES {
                    if let Some(sql) = tpch::get_query(q) {
                        let query_start = Instant::now();
                        match ctx.sql(sql).await {
                            Ok(result) => {
                                let elapsed = query_start.elapsed();
                                println!(
                                    "Q{:02}: {:>8} rows in {:>8.3}ms",
                                    q,
                                    result.row_count,
                                    elapsed.as_secs_f64() * 1000.0
                                );
                                if iter == 0 {
                                    results.push((q, result.row_count, elapsed));
                                }
                            }
                            Err(e) => {
                                println!("Q{:02}: ERROR - {}", q, e);
                                if iter == 0 {
                                    results.push((q, 0, std::time::Duration::ZERO));
                                }
                            }
                        }
                    }
                }

                let iter_time = iter_start.elapsed();
                total_time += iter_time;
                println!("Iteration time: {:?}\n", iter_time);
            }

            println!("=== Summary ===");
            println!("Total time: {:?}", total_time);
            println!("Average iteration: {:?}", total_time / iterations as u32);

            let query_total: std::time::Duration = results.iter().map(|(_, _, t)| *t).sum();
            println!("Query execution time: {:?}", query_total);

            let successful = results.iter().filter(|(_, rows, _)| *rows > 0).count();
            println!("Successful queries: {}/{}", successful, results.len());
        }
    }
}
