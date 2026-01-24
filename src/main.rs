//! Query Engine CLI

mod cli;

use clap::{Parser, Subcommand};
use cli::{OutputFormat, OutputFormatter, ReplHelper};
use query_engine::execution::{print_results, ExecutionContext};
use query_engine::tpch::{self, TpchGenerator};
use rustyline::error::ReadlineError;
use rustyline::{Config, Editor};
use std::path::PathBuf;
use std::sync::Arc;
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

    /// Start interactive SQL shell (REPL)
    Repl {
        /// Optional: Preload TPC-H tables from Parquet directory
        #[arg(short, long)]
        tpch: Option<PathBuf>,
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
            println!(
                "Running TPC-H benchmark (SF={}, iterations={})",
                sf, iterations
            );
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
            let tables = [
                "nation", "region", "part", "supplier", "partsupp", "customer", "orders",
                "lineitem",
            ];

            for table in &tables {
                let file_path = path.join(format!("{}.parquet", table));
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

            println!(
                "\nData loaded in {:?}. Running queries...\n",
                start.elapsed()
            );

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

        Commands::Repl { tpch } => {
            run_repl(tpch).await;
        }
    }
}

/// REPL state for output formatting and other settings
struct ReplState {
    formatter: OutputFormatter,
}

impl ReplState {
    fn new() -> Self {
        Self {
            formatter: OutputFormatter::new(OutputFormat::Table),
        }
    }
}

/// Run the interactive SQL REPL
async fn run_repl(tpch_path: Option<PathBuf>) {
    println!("Query Engine Interactive SQL Shell");
    println!("Type .help for available commands, or enter SQL queries.");
    println!("Tab completion and syntax highlighting enabled.");
    println!();

    let mut ctx = ExecutionContext::new();
    let mut state = ReplState::new();
    let helper = Arc::new(ReplHelper::new());

    // Preload TPC-H tables if path provided
    if let Some(path) = tpch_path {
        println!("Loading TPC-H tables from: {}", path.display());
        let tables = [
            "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
        ];

        for table in &tables {
            let file_path = path.join(format!("{}.parquet", table));
            match ctx.register_parquet(*table, &file_path) {
                Ok(()) => {
                    if let Some(schema) = ctx.table_schema(table) {
                        let columns: Vec<String> =
                            schema.fields().iter().map(|f| f.name().clone()).collect();
                        println!("  Loaded {}: {} columns", table, columns.len());
                        helper.register_table(table, columns);
                    }
                }
                Err(e) => {
                    eprintln!("  Warning: Could not load {}: {}", table, e);
                }
            }
        }
        println!();
    }

    // Configure rustyline with history and completion settings
    let config = Config::builder()
        .max_history_size(1000)
        .expect("valid history size")
        .history_ignore_dups(true)
        .expect("valid history config")
        .completion_type(rustyline::CompletionType::Fuzzy)
        .build();

    // Create a cloneable helper for the editor
    let helper_for_editor = (*helper).clone();

    let mut rl = match Editor::with_config(config) {
        Ok(mut editor) => {
            editor.set_helper(Some(helper_for_editor));
            editor
        }
        Err(e) => {
            eprintln!("Failed to initialize readline: {}", e);
            return;
        }
    };

    // Set up history file path
    let history_path = dirs_next::home_dir()
        .map(|h| h.join(".query_engine_history"))
        .unwrap_or_else(|| PathBuf::from(".query_engine_history"));

    // Load history if it exists
    if history_path.exists() {
        if let Err(e) = rl.load_history(&history_path) {
            eprintln!("Warning: Could not load history: {}", e);
        }
    }

    loop {
        let readline = rl.readline("sql> ");
        match readline {
            Ok(line) => {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }

                // Add to history (ignore if it fails - not critical)
                let _ = rl.add_history_entry(line);

                // Handle dot commands
                if line.starts_with('.') {
                    if !handle_dot_command(&mut ctx, &mut state, &helper, line).await {
                        break;
                    }
                    continue;
                }

                // Execute SQL query
                let start = Instant::now();
                match ctx.sql(line).await {
                    Ok(result) => {
                        // Use the configured output format
                        if let Err(e) = state.formatter.print(&result.batches) {
                            eprintln!("Error formatting output: {}", e);
                        }
                        println!(
                            "({} rows in {:.3}ms)\n",
                            result.row_count,
                            start.elapsed().as_secs_f64() * 1000.0
                        );
                    }
                    Err(e) => {
                        eprintln!("Error: {}\n", e);
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                println!("Bye!");
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    // Save history
    if let Err(e) = rl.save_history(&history_path) {
        eprintln!("Warning: Could not save history: {}", e);
    }
}

/// Handle dot commands, returns false if should exit
async fn handle_dot_command(
    ctx: &mut ExecutionContext,
    state: &mut ReplState,
    helper: &Arc<ReplHelper>,
    line: &str,
) -> bool {
    let parts: Vec<&str> = line.split_whitespace().collect();
    let cmd = parts.first().copied().unwrap_or("");

    match cmd {
        ".help" | ".h" => {
            println!("Available commands:");
            println!("  .help, .h              Show this help message");
            println!("  .quit, .exit, .q       Exit the shell");
            println!("  .tables                List registered tables");
            println!("  .schema <table>        Show schema for a table");
            println!("  .load <path> <name>    Load Parquet file/directory as table");
            println!("  .tpch <path>           Load all TPC-H tables from directory");
            println!("  .mode <format>         Set output format (table, csv, json, vertical)");
            println!("  .format                Show current output format");
            println!();
            println!("Or enter any SQL query to execute it.");
            println!("Press Tab for auto-completion of SQL keywords and table names.");
            println!();
        }
        ".quit" | ".exit" | ".q" => {
            println!("Bye!");
            return false;
        }
        ".tables" => {
            let tables = ctx.table_names();
            if tables.is_empty() {
                println!("No tables registered.");
            } else {
                println!("Registered tables:");
                for name in tables {
                    if let Some(schema) = ctx.table_schema(&name) {
                        println!("  {} ({} columns)", name, schema.fields().len());
                    }
                }
            }
            println!();
        }
        ".schema" => {
            if parts.len() < 2 {
                eprintln!("Usage: .schema <table_name>\n");
            } else {
                let table_name = parts[1];
                if let Some(schema) = ctx.table_schema(table_name) {
                    println!("Schema for '{}':", table_name);
                    for field in schema.fields() {
                        println!(
                            "  {}: {:?}{}",
                            field.name(),
                            field.data_type(),
                            if field.is_nullable() {
                                " (nullable)"
                            } else {
                                ""
                            }
                        );
                    }
                    println!();
                } else {
                    eprintln!("Table '{}' not found.\n", table_name);
                }
            }
        }
        ".load" => {
            if parts.len() < 3 {
                eprintln!("Usage: .load <path> <table_name>\n");
            } else {
                let path = PathBuf::from(parts[1]);
                let name = parts[2];
                let start = Instant::now();
                match ctx.register_parquet(name, &path) {
                    Ok(()) => {
                        if let Some(schema) = ctx.table_schema(name) {
                            let columns: Vec<String> =
                                schema.fields().iter().map(|f| f.name().clone()).collect();
                            println!(
                                "Loaded '{}' ({} columns) in {:.3}ms\n",
                                name,
                                columns.len(),
                                start.elapsed().as_secs_f64() * 1000.0
                            );
                            helper.register_table(name, columns);
                        }
                    }
                    Err(e) => {
                        eprintln!("Error loading '{}': {}\n", path.display(), e);
                    }
                }
            }
        }
        ".tpch" => {
            if parts.len() < 2 {
                eprintln!("Usage: .tpch <parquet_directory>\n");
            } else {
                let path = PathBuf::from(parts[1]);
                let tables = [
                    "nation", "region", "part", "supplier", "partsupp", "customer", "orders",
                    "lineitem",
                ];
                println!("Loading TPC-H tables from: {}", path.display());
                for table in &tables {
                    let file_path = path.join(format!("{}.parquet", table));
                    match ctx.register_parquet(*table, &file_path) {
                        Ok(()) => {
                            if let Some(schema) = ctx.table_schema(table) {
                                let columns: Vec<String> =
                                    schema.fields().iter().map(|f| f.name().clone()).collect();
                                println!("  Loaded {}: {} columns", table, columns.len());
                                helper.register_table(table, columns);
                            }
                        }
                        Err(e) => {
                            eprintln!("  Warning: Could not load {}: {}", table, e);
                        }
                    }
                }
                println!();
            }
        }
        ".mode" => {
            if parts.len() < 2 {
                println!("Current output format: {}", state.formatter.format().name());
                println!(
                    "Available formats: {}",
                    OutputFormat::all_names().join(", ")
                );
                println!();
            } else {
                let format_str = parts[1];
                match OutputFormat::from_str(format_str) {
                    Some(format) => {
                        state.formatter.set_format(format);
                        println!("Output format set to: {}\n", format.name());
                    }
                    None => {
                        eprintln!("Unknown format: {}", format_str);
                        eprintln!(
                            "Available formats: {}\n",
                            OutputFormat::all_names().join(", ")
                        );
                    }
                }
            }
        }
        ".format" => {
            println!("Current output format: {}", state.formatter.format().name());
            println!(
                "Available formats: {}\n",
                OutputFormat::all_names().join(", ")
            );
        }
        _ => {
            eprintln!(
                "Unknown command: {}. Type .help for available commands.\n",
                cmd
            );
        }
    }
    true
}
