//! Query Engine CLI

mod cli;

use clap::{Parser, Subcommand};
use cli::{OutputFormat, OutputFormatter, ReplHelper};
use futures::StreamExt;
use query_engine::execution::{print_results, ExecutionContext};
use query_engine::tpch::{self, TpchGenerator};
use rustyline::error::ReadlineError;
use rustyline::{Config, Editor};
use std::path::{Path, PathBuf};
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

        /// Run only a specific query number (1-22)
        #[arg(short, long)]
        query: Option<usize>,

        /// Scale factor (for Q11 threshold adjustment). Auto-detected from path if not specified.
        #[arg(short, long)]
        sf: Option<f64>,

        /// Save query results as CSV files to this directory
        #[arg(long)]
        save_csv: Option<PathBuf>,
    },

    /// Load Lance dataset(s) and run a query (requires --features lance)
    #[cfg(feature = "lance")]
    LoadLance {
        /// Path to a Lance dataset directory (e.g. ./data/orders.lance)
        #[arg(short, long)]
        path: PathBuf,

        /// Table name to register
        #[arg(short, long)]
        name: String,

        /// SQL query to execute (if omitted, just loads and shows schema)
        #[arg(short, long)]
        query: Option<String>,

        /// Read a HISTORICAL version instead of the latest (Lance time travel).
        /// Use `lance-versions` to list them. An unknown version is an error,
        /// never a silent fall back to the latest.
        #[arg(long)]
        version: Option<u64>,
    },

    /// List the versions of a Lance dataset (requires --features lance)
    #[cfg(feature = "lance")]
    LanceVersions {
        /// Path to a Lance dataset directory (e.g. ./data/orders.lance)
        #[arg(short, long)]
        path: PathBuf,
    },

    /// Write a Lance dataset from Parquet or from a SQL query (--features lance)
    #[cfg(feature = "lance")]
    WriteLance {
        /// Source Parquet file or directory. Mutually exclusive with --sql.
        #[arg(long)]
        from_parquet: Option<PathBuf>,

        /// Source SQL query. Requires --tables to say what to run it against.
        #[arg(long)]
        sql: Option<String>,

        /// Directory of Parquet files to register before running --sql.
        #[arg(long)]
        tables: Option<PathBuf>,

        /// Destination Lance dataset directory
        #[arg(short, long)]
        out: PathBuf,

        /// create (default, fails if it exists) | append | overwrite
        #[arg(long, default_value = "create")]
        mode: String,
    },

    /// Build an IVF_PQ vector index over an embedding column (--features lance)
    #[cfg(feature = "lance")]
    CreateLanceIndex {
        /// Path to a Lance dataset directory
        #[arg(short, long)]
        path: PathBuf,

        /// Vector column to index (FixedSizeList<Float32, N>)
        #[arg(short, long)]
        column: String,

        /// Distance metric the index is built for: l2 | cosine | dot.
        /// MUST match how queries will be asked — an L2 index answers cosine
        /// queries wrongly.
        #[arg(short, long, default_value = "l2")]
        metric: String,

        /// IVF partitions (default: sqrt(rows))
        #[arg(long)]
        partitions: Option<usize>,

        /// PQ sub-vectors; must divide the vector dimension exactly
        #[arg(long)]
        sub_vectors: Option<usize>,

        /// Replace an existing index on this column
        #[arg(long)]
        replace: bool,
    },

    /// Run TPC-H benchmark from Lance datasets (requires --features lance)
    #[cfg(feature = "lance")]
    BenchmarkLance {
        /// Path to directory containing <table>.lance datasets
        #[arg(short, long)]
        path: PathBuf,

        /// Number of iterations
        #[arg(short, long, default_value = "1")]
        iterations: usize,

        /// Run only a specific query number (1-22)
        #[arg(short, long)]
        query: Option<usize>,

        /// Scale factor (for Q11 threshold adjustment). Auto-detected from path if not specified.
        #[arg(short, long)]
        sf: Option<f64>,

        /// Save query results as CSV files to this directory
        #[arg(long)]
        save_csv: Option<PathBuf>,
    },

    /// Write a native table from Parquet, Iceberg, Lance, or a SQL query
    /// (task 003 of the native-tables-foundation epic). Exactly one of
    /// --from-parquet/--from-iceberg/--from-lance/--sql must be given.
    WriteNative {
        /// Source Parquet file or directory.
        #[arg(long)]
        from_parquet: Option<PathBuf>,

        /// Source Apache Iceberg table directory.
        #[arg(long)]
        from_iceberg: Option<PathBuf>,

        /// Iceberg snapshot id (only with --from-iceberg; default: current).
        #[arg(long)]
        iceberg_snapshot: Option<i64>,

        /// Source Lance dataset directory (requires --features lance).
        #[cfg(feature = "lance")]
        #[arg(long)]
        from_lance: Option<PathBuf>,

        /// Source SQL query. Requires --tables. This is the CREATE TABLE
        /// ... AS SELECT shape: the query's physical plan is streamed
        /// directly into the writer batch-by-batch, never materialized
        /// first (see src/storage/native_write.rs's module doc).
        #[arg(long)]
        sql: Option<String>,

        /// Directory of Parquet files to register before running --sql.
        #[arg(long)]
        tables: Option<PathBuf>,

        /// Destination native table directory.
        #[arg(short, long)]
        out: PathBuf,

        /// create (default, fails if it exists) | overwrite
        #[arg(long, default_value = "create")]
        mode: String,
    },

    /// Load a native table and print its manifest; optionally run a query
    /// against it. The query path fully materializes the table into memory
    /// first (a CLI validation convenience, NOT the production read path —
    /// that is the `TableProvider` in src/storage/native_table.rs).
    LoadNative {
        /// Path to a native table directory (written by write-native).
        #[arg(short, long)]
        path: PathBuf,

        /// Table name to register for --query.
        #[arg(short, long, default_value = "t")]
        name: String,

        /// SQL query to execute (if omitted, just prints the manifest).
        #[arg(short, long)]
        query: Option<String>,
    },

    /// Start interactive SQL shell (REPL)
    Repl {
        /// Optional: Preload TPC-H tables from Parquet directory
        #[arg(short, long)]
        tpch: Option<PathBuf>,

        /// Optional: Preload TPC-H tables from a Lance directory (requires --features lance)
        #[cfg(feature = "lance")]
        #[arg(long)]
        tpch_lance: Option<PathBuf>,
    },

    /// Print the hardware topology the engine detected and how it placed workers
    Topology,

    /// Run this process as a cluster node: HTTP server, peer discovery, health.
    ///
    /// MILESTONE 1: `/sql` executes LOCALLY against this node's own tables.
    /// There is no fan-out and no distributed answer yet — see
    /// `.claude/plans/DISTRIBUTED-IMPLEMENTATION.md`.
    Serve {
        /// Address to bind. Use `:0` for an ephemeral port (tests).
        #[arg(long, default_value = "0.0.0.0:7777")]
        bind: String,

        /// Static peer list, `host:port,host:port`. May include this node.
        #[arg(long, value_delimiter = ',')]
        peers: Vec<String>,

        /// DNS name whose A records are the cluster. In Kubernetes this is the
        /// headless Service; it is re-resolved on a timer so pod churn is seen.
        #[arg(long, conflicts_with = "peers")]
        peers_dns: Option<String>,

        /// Port to assume for `--peers-dns` A records (default: the bound port)
        #[arg(long)]
        peers_dns_port: Option<u16>,

        /// Stable node id. Defaults to $QE_NODE_ID, then the StatefulSet pod
        /// ordinal from the hostname, then a hash of the advertised address.
        #[arg(long)]
        node_id: Option<u64>,

        /// Address peers should use to reach this node. Defaults to
        /// $QE_ADVERTISE_ADDR, then $POD_IP:<port>, then a local interface.
        #[arg(long)]
        advertise: Option<String>,

        /// Directory of TPC-H Parquet files. Registers all eight tables and
        /// FAILS if any is missing — a node serving a partial schema is worse
        /// than a node that refuses to start.
        #[arg(long)]
        data: Option<PathBuf>,

        /// Directory of arbitrary `*.parquet` files, each registered under its
        /// file stem. Can be combined with --data.
        #[arg(long)]
        tables: Option<PathBuf>,

        /// Apache Gravitino metastore URL, e.g. `http://127.0.0.1:8090`.
        /// Every fileset in the schema below is registered as a table, its
        /// reader chosen by the fileset's `format` property (parquet,
        /// iceberg, lance). Every node pulling the SAME schema from the SAME
        /// metastore is what makes the cluster's catalog agree by
        /// construction. Can be combined with --data/--tables.
        #[arg(long)]
        metastore: Option<String>,

        /// Gravitino metalake holding the tables.
        #[arg(long, default_value = "local_lake")]
        metastore_metalake: String,

        /// Gravitino catalog (type FILESET) holding the tables.
        #[arg(long, default_value = "lakehouse")]
        metastore_catalog: String,

        /// Gravitino schema whose filesets become this node's tables.
        #[arg(long, default_value = "tpch")]
        metastore_schema: String,

        /// Memory limit, e.g. `8G`. Default: the engine's normal default.
        #[arg(long)]
        memory_limit: Option<String>,

        /// Discovery re-resolution and peer-probe interval, milliseconds.
        #[arg(long, default_value = "2000")]
        discovery_interval_ms: u64,

        /// Per-probe timeout, milliseconds.
        #[arg(long, default_value = "1000")]
        probe_timeout_ms: u64,

        /// On SIGTERM, keep serving (reporting /readyz false) this long before
        /// closing the listener, so a Kubernetes Service can drop this pod
        /// from its endpoints first.
        #[arg(long, default_value = "0")]
        drain_ms: u64,

        /// Grace period for in-flight requests after the listener closes.
        #[arg(long, default_value = "10000")]
        shutdown_grace_ms: u64,

        /// Arrow Flight (gRPC) bind address. Default: the HTTP host with
        /// port + 1. Pass `none` to disable the Flight endpoint.
        #[arg(long)]
        flight_bind: Option<String>,

        /// Pulsar broker web-service URL (`http://host:8080`): every
        /// schema'd topic of --pulsar-namespace registers as a table
        /// (requires --features pulsar).
        #[arg(long)]
        pulsar_admin: Option<String>,

        /// Pulsar namespace as tenant/namespace (default public/default).
        #[arg(long, default_value = "public/default")]
        pulsar_namespace: String,
    },
}

/// The eight TPC-H tables, in dependency order.
const TPCH_TABLES: [&str; 8] = [
    "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
];

/// Strip any `table.` qualifier from every field name in `schema`.
///
/// `write-native --sql`'s physical plan schema carries QUALIFIED names for
/// a bare `SELECT *` ("orders.o_orderkey", not "o_orderkey" --
/// `SelectItem::Wildcard` preserves `field.relation` in the binder, a
/// pre-existing property of `bind_query`, not introduced by this command).
/// A native table written from such a query should get normal column
/// names, matching what an explicit column list already produces.
fn unqualified_schema(schema: &arrow::datatypes::Schema) -> arrow::datatypes::SchemaRef {
    Arc::new(arrow::datatypes::Schema::new(
        schema
            .fields()
            .iter()
            .map(|f| {
                let name = match f.name().rsplit_once('.') {
                    Some((_, col)) => col.to_string(),
                    None => f.name().clone(),
                };
                arrow::datatypes::Field::new(name, f.data_type().clone(), f.is_nullable())
            })
            .collect::<Vec<_>>(),
    ))
}

/// Auto-detect TPC-H scale factor from a data directory name.
fn detect_sf(path: &Path) -> f64 {
    let path_str = path.to_string_lossy().to_lowercase();
    if path_str.contains("1000gb") || path_str.contains("sf1000") {
        1000.0
    } else if path_str.contains("100gb") || path_str.contains("sf100") {
        100.0
    } else if path_str.contains("10gb") || path_str.contains("sf10") {
        10.0
    } else if path_str.contains("1gb") || path_str.contains("sf1") {
        1.0
    } else if path_str.contains("100mb") || path_str.contains("sf0.1") {
        0.1
    } else if path_str.contains("10mb") || path_str.contains("sf0.01") {
        0.01
    } else if path_str.contains("1mb") || path_str.contains("sf0.001") {
        0.001
    } else {
        1.0 // default to SF=1
    }
}

#[tokio::main]
async fn main() {
    // Serve the engine's memory on 4KB pages, not 2MB transparent huge pages.
    // Measured, not assumed: 17 of 22 TPC-H queries get faster and peak RSS
    // drops. See disable_transparent_hugepages() for the evidence and why the
    // usual "huge pages help big hash tables" intuition does not apply here.
    // Do this first: it only governs faults taken after this point.
    query_engine::execution::disable_transparent_hugepages();

    // Allow any same-user process to attach a debugger/sampler when requested
    // (yama ptrace_scope=1 otherwise restricts attach to ancestors).
    // Linux-only: prctl/PR_SET_PTRACER (and yama itself) don't exist elsewhere.
    #[cfg(target_os = "linux")]
    if std::env::var("QUERY_PTRACE_ANY").is_ok() {
        unsafe {
            libc::prctl(libc::PR_SET_PTRACER, -1i64, 0, 0, 0);
        }
    }

    // Place the rayon worker pool on the machine's best CPUs before anything
    // else touches rayon (the pool is global and first-writer-wins). Pins
    // worker i to the i-th CPU of the topology preference order: physical
    // cores before SMT siblings, fast classes before slow, round-robin across
    // NUMA nodes. `QE_TOPOLOGY=0` restores rayon's default pool.
    query_engine::execution::topology::init_global_pool();

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
            #[cfg(feature = "gpu")]
            ctx.enable_gpu_offload();
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
                    #[cfg(feature = "gpu")]
                    ctx.enable_gpu_offload();
                    let mut gen = TpchGenerator::new(sf);
                    gen.generate_all(&mut ctx);

                    if plan {
                        match ctx.logical_plan(sql) {
                            Ok(logical) => {
                                println!("Logical Plan (before optimization):\n{}", logical);
                            }
                            Err(e) => {
                                println!("Error creating plan: {}", e);
                            }
                        }
                        match ctx.optimized_plan(sql) {
                            Ok(optimized) => {
                                println!("\nOptimized Plan:\n{}", optimized);
                            }
                            Err(e) => {
                                println!("Error optimizing: {}", e);
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
            #[cfg(feature = "gpu")]
            ctx.enable_gpu_offload();
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
            #[cfg(feature = "gpu")]
            ctx.enable_gpu_offload();
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
            #[cfg(feature = "gpu")]
            ctx.enable_gpu_offload();

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

        Commands::BenchmarkParquet {
            path,
            iterations,
            query,
            sf,
            save_csv,
        } => {
            // Auto-detect scale factor from path name if not specified
            let sf = sf.unwrap_or_else(|| detect_sf(&path));
            println!("Running TPC-H benchmark from Parquet files (SF={})", sf);
            println!("Path: {}", path.display());
            println!("Iterations: {}", iterations);
            println!(
                "IPC cache mode: {}",
                query_engine::storage::ipc_cache::mode()
            );
            println!();

            let start = Instant::now();
            // Hard cap at 64GB to prevent OOM on large SF values
            const MAX_MEMORY: usize = 64 * 1024 * 1024 * 1024; // 64GB
            let memory_limit =
                (((sf * 4.0).max(1.0) as usize) * 1024 * 1024 * 1024).min(MAX_MEMORY);
            println!("Memory limit: {} GB", memory_limit / (1024 * 1024 * 1024));
            let mut ctx = ExecutionContext::with_memory_limit(memory_limit);
            #[cfg(feature = "gpu")]
            ctx.enable_gpu_offload();

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

            // Determine which queries to run
            let queries: Vec<usize> = if let Some(q) = query {
                if !(1..=22).contains(&q) {
                    eprintln!("Invalid query number {}. Valid range: 1-22", q);
                    std::process::exit(1);
                }
                vec![q]
            } else {
                tpch::ALL_QUERIES.to_vec()
            };

            let mut total_time = std::time::Duration::ZERO;
            let mut results = Vec::new();

            for iter in 0..iterations {
                if iterations > 1 {
                    println!("=== Iteration {} ===", iter + 1);
                }

                let iter_start = Instant::now();

                for &q in &queries {
                    if let Some(sql) = tpch::get_query_for_sf(q, sf) {
                        let query_start = Instant::now();
                        match ctx.sql(&sql).await {
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
                                    // Save CSV if requested
                                    if let Some(ref csv_dir) = save_csv {
                                        std::fs::create_dir_all(csv_dir).ok();
                                        let csv_path = csv_dir.join(format!("q{:02}.csv", q));
                                        if let Ok(file) = std::fs::File::create(&csv_path) {
                                            let mut writer = arrow::csv::WriterBuilder::new()
                                                .with_header(true)
                                                .build(file);
                                            for batch in &result.batches {
                                                writer.write(batch).ok();
                                            }
                                        }
                                    }
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

        #[cfg(feature = "lance")]
        Commands::WriteLance {
            from_parquet,
            sql,
            tables,
            out,
            mode,
        } => {
            use query_engine::storage::{lance_write, LanceWriteMode};
            let mode: LanceWriteMode = match mode.parse() {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            };
            let start = Instant::now();

            let result = match (from_parquet, sql) {
                (Some(_), Some(_)) => {
                    eprintln!("Error: pass --from-parquet or --sql, not both");
                    std::process::exit(1);
                }
                (None, None) => {
                    eprintln!("Error: pass one of --from-parquet or --sql");
                    std::process::exit(1);
                }
                (Some(src), None) => {
                    println!(
                        "Converting {} -> {} ({:?})",
                        src.display(),
                        out.display(),
                        mode
                    );
                    lance_write::write_from_parquet(&src, &out, mode)
                }
                (None, Some(query)) => {
                    let Some(dir) = tables else {
                        eprintln!("Error: --sql needs --tables <parquet directory>");
                        std::process::exit(1);
                    };
                    let mut ctx = ExecutionContext::new();
                    #[cfg(feature = "gpu")]
                    ctx.enable_gpu_offload();
                    // Register every parquet file in the directory under its
                    // stem, so the query can name tables normally.
                    match std::fs::read_dir(&dir) {
                        Ok(entries) => {
                            for entry in entries.flatten() {
                                let p = entry.path();
                                if p.extension().is_some_and(|e| e == "parquet") {
                                    if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                                        if let Err(e) = ctx.register_parquet(stem, &p) {
                                            eprintln!("Warning: {} not registered: {}", stem, e);
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            eprintln!("Error reading {}: {}", dir.display(), e);
                            std::process::exit(1);
                        }
                    }
                    println!("Running: {}", query);
                    match ctx.sql(&query).await {
                        Ok(r) => {
                            println!("Query produced {} rows", r.row_count);
                            lance_write::write_batches(r.batches, r.schema, &out, mode)
                        }
                        Err(e) => {
                            eprintln!("Error: {}", e);
                            std::process::exit(1);
                        }
                    }
                }
            };

            match result {
                Ok(r) => {
                    println!(
                        "Wrote {} ({} rows, now at version {}) in {:?}",
                        out.display(),
                        r.rows,
                        r.version,
                        start.elapsed()
                    );
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            }
        }

        #[cfg(feature = "lance")]
        Commands::CreateLanceIndex {
            path,
            column,
            metric,
            partitions,
            sub_vectors,
            replace,
        } => {
            use query_engine::planner::vector_types::VectorMetric;
            use query_engine::storage::lance_write;
            let metric = match metric.to_ascii_lowercase().as_str() {
                "l2" | "euclidean" => VectorMetric::L2,
                "cosine" => VectorMetric::Cosine,
                "dot" | "inner_product" => VectorMetric::Dot,
                other => {
                    eprintln!(
                        "Error: unknown metric `{}` (expected l2, cosine or dot)",
                        other
                    );
                    std::process::exit(1);
                }
            };
            let start = Instant::now();
            println!(
                "Building IVF_PQ index on {}.{} (metric {})",
                path.display(),
                column,
                metric
            );
            match lance_write::create_vector_index(
                &path,
                &column,
                metric,
                partitions,
                sub_vectors,
                replace,
            ) {
                Ok(r) => {
                    println!(
                        "Indexed {} rows in {:?}; dataset is now at version {}",
                        r.rows,
                        start.elapsed(),
                        r.version
                    );
                    println!(
                        "\nThe index is APPROXIMATE and stays OPT-IN. Enable it with \
                         QE_VECTOR_SEARCH=indexed (see CLAUDE.md for measured recall)."
                    );
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            }
        }

        #[cfg(feature = "lance")]
        Commands::LanceVersions { path } => {
            match query_engine::storage::LanceTable::list_versions(&path) {
                Ok(versions) => {
                    println!("Lance dataset: {}", path.display());
                    println!("{} version(s):", versions.len());
                    let latest = versions.last().map(|(v, _)| *v).unwrap_or(0);
                    for (v, ts) in &versions {
                        let marker = if *v == latest { "  <- latest" } else { "" };
                        println!("  v{:<6} {}{}", v, ts, marker);
                    }
                    println!(
                        "\nRead one with: load-lance --path {} --version <N>",
                        path.display()
                    );
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            }
        }

        #[cfg(feature = "lance")]
        Commands::LoadLance {
            path,
            name,
            query,
            version,
        } => {
            let start = Instant::now();
            let mut ctx = ExecutionContext::new();
            #[cfg(feature = "gpu")]
            ctx.enable_gpu_offload();

            match version {
                Some(v) => println!(
                    "Loading Lance dataset from: {} (version {})",
                    path.display(),
                    v
                ),
                None => println!("Loading Lance dataset from: {}", path.display()),
            }

            let registered = match version {
                Some(v) => ctx.register_lance_version(&name, &path, v),
                None => ctx.register_lance(&name, &path),
            };
            match registered {
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
                    eprintln!("Error loading Lance dataset: {}", e);
                    std::process::exit(1);
                }
            }
        }

        #[cfg(feature = "lance")]
        Commands::BenchmarkLance {
            path,
            iterations,
            query,
            sf,
            save_csv,
        } => {
            let sf = sf.unwrap_or_else(|| detect_sf(&path));
            println!("Running TPC-H benchmark from Lance datasets (SF={})", sf);
            println!("Path: {}", path.display());
            println!("Iterations: {}", iterations);
            println!();

            let start = Instant::now();
            // Same 64GB cap as the Parquet benchmark so the two are comparable.
            const MAX_MEMORY: usize = 64 * 1024 * 1024 * 1024;
            let memory_limit =
                (((sf * 4.0).max(1.0) as usize) * 1024 * 1024 * 1024).min(MAX_MEMORY);
            println!("Memory limit: {} GB", memory_limit / (1024 * 1024 * 1024));
            let mut ctx = ExecutionContext::with_memory_limit(memory_limit);
            #[cfg(feature = "gpu")]
            ctx.enable_gpu_offload();

            for table in &TPCH_TABLES {
                let dataset_path = path.join(format!("{}.lance", table));
                // Warm statistics here so the (Lance-specific) stats scan is
                // reported as load time instead of inflating Q01.
                match ctx.register_lance_warm(*table, &dataset_path) {
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

            let queries: Vec<usize> = if let Some(q) = query {
                if !(1..=22).contains(&q) {
                    eprintln!("Invalid query number {}. Valid range: 1-22", q);
                    std::process::exit(1);
                }
                vec![q]
            } else {
                tpch::ALL_QUERIES.to_vec()
            };

            let mut total_time = std::time::Duration::ZERO;
            let mut results = Vec::new();

            for iter in 0..iterations {
                if iterations > 1 {
                    println!("=== Iteration {} ===", iter + 1);
                }
                let iter_start = Instant::now();

                for &q in &queries {
                    if let Some(sql) = tpch::get_query_for_sf(q, sf) {
                        let query_start = Instant::now();
                        match ctx.sql(&sql).await {
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
                                    if let Some(ref csv_dir) = save_csv {
                                        std::fs::create_dir_all(csv_dir).ok();
                                        let csv_path = csv_dir.join(format!("q{:02}.csv", q));
                                        if let Ok(file) = std::fs::File::create(&csv_path) {
                                            let mut writer = arrow::csv::WriterBuilder::new()
                                                .with_header(true)
                                                .build(file);
                                            for batch in &result.batches {
                                                writer.write(batch).ok();
                                            }
                                        }
                                    }
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

        Commands::WriteNative {
            from_parquet,
            from_iceberg,
            iceberg_snapshot,
            #[cfg(feature = "lance")]
            from_lance,
            sql,
            tables,
            out,
            mode,
        } => {
            use query_engine::storage::native_write::{self, NativeWriteMode};
            let mode: NativeWriteMode = match mode.parse() {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            };

            let mut present = 0;
            if from_parquet.is_some() {
                present += 1;
            }
            if from_iceberg.is_some() {
                present += 1;
            }
            #[cfg(feature = "lance")]
            if from_lance.is_some() {
                present += 1;
            }
            if sql.is_some() {
                present += 1;
            }
            if present != 1 {
                eprintln!(
                    "Error: pass exactly one of --from-parquet, --from-iceberg, --from-lance, --sql"
                );
                std::process::exit(1);
            }

            let start = Instant::now();
            let result = if let Some(src) = from_parquet {
                println!(
                    "Converting Parquet {} -> {} ({:?})",
                    src.display(),
                    out.display(),
                    mode
                );
                native_write::write_from_parquet(&src, &out, mode).await
            } else if let Some(src) = from_iceberg {
                println!(
                    "Converting Iceberg {} -> {} ({:?}, snapshot={:?})",
                    src.display(),
                    out.display(),
                    mode,
                    iceberg_snapshot
                );
                native_write::write_from_iceberg(&src, &out, mode, iceberg_snapshot).await
            } else if let Some(query) = sql {
                let Some(dir) = tables else {
                    eprintln!("Error: --sql needs --tables <parquet directory>");
                    std::process::exit(1);
                };
                let mut ctx = ExecutionContext::new();
                match std::fs::read_dir(&dir) {
                    Ok(entries) => {
                        for entry in entries.flatten() {
                            let p = entry.path();
                            if p.extension().is_some_and(|e| e == "parquet") {
                                if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                                    if let Err(e) = ctx.register_parquet(stem, &p) {
                                        eprintln!("Warning: {} not registered: {}", stem, e);
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading {}: {}", dir.display(), e);
                        std::process::exit(1);
                    }
                }
                println!("Planning: {}", query);
                let physical = match ctx.physical_plan(&query) {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("Error: {}", e);
                        std::process::exit(1);
                    }
                };
                // `SELECT *`'s physical schema carries QUALIFIED field names
                // ("orders.o_orderkey", not "o_orderkey" -- SelectItem::
                // Wildcard preserves field.relation in the binder; a
                // pre-existing property, not introduced here). Strip any
                // `table.` prefix so a native table written via --sql gets
                // normal column names, matching what an explicit column
                // list already produces.
                let physical_schema = physical.schema();
                let schema = unqualified_schema(&physical_schema);
                let n = physical.output_partitions().max(1);
                let mut streams = Vec::with_capacity(n);
                let mut stream_err = None;
                for i in 0..n {
                    match physical.execute(i).await {
                        Ok(s) => streams.push(s),
                        Err(e) => {
                            stream_err = Some(e);
                            break;
                        }
                    }
                }
                if let Some(e) = stream_err {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
                // Merge every partition into ONE stream and hand it straight
                // to the writer -- streamed, never materialized via
                // ExecutionContext::sql()'s collecting wrapper (see
                // native_write.rs's module doc for why that matters).
                let renamed_schema = schema.clone();
                let merged: query_engine::physical::RecordBatchStream =
                    Box::pin(futures::stream::select_all(streams).map(move |res| {
                        res.and_then(|batch| {
                            arrow::record_batch::RecordBatch::try_new(
                                renamed_schema.clone(),
                                batch.columns().to_vec(),
                            )
                            .map_err(Into::into)
                        })
                    }));
                println!("Running (streamed, not materialized): {}", query);
                native_write::write_batches(merged, schema, &out, mode).await
            } else {
                #[cfg(feature = "lance")]
                {
                    let src = from_lance.expect("checked exactly one source is present");
                    println!(
                        "Converting Lance {} -> {} ({:?})",
                        src.display(),
                        out.display(),
                        mode
                    );
                    native_write::write_from_lance(&src, &out, mode).await
                }
                #[cfg(not(feature = "lance"))]
                unreachable!("checked exactly one source is present")
            };

            match result {
                Ok(r) => {
                    println!(
                        "Wrote {} ({} rows, {} segments, table_id={}, now at version {}) in {:?}",
                        out.display(),
                        r.rows,
                        r.segments,
                        r.table_id,
                        r.version,
                        start.elapsed()
                    );
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            }
        }

        Commands::LoadNative { path, name, query } => {
            use query_engine::storage::{native_manifest, native_write};
            let start = Instant::now();
            let manifest = match native_manifest::read_manifest(&path) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            };
            println!("Native table: {}", path.display());
            println!("  table_id: {}", manifest.table_id);
            println!("  version:  {}", manifest.snapshot.version);
            println!("  rows:     {}", manifest.snapshot.row_count);
            println!("  segments: {}", manifest.segments.len());
            let schema = manifest.arrow_schema();
            println!("Schema: {} columns", schema.fields().len());
            for field in schema.fields() {
                println!("  - {}: {:?}", field.name(), field.data_type());
            }
            println!("Loaded manifest in {:?}", start.elapsed());

            if let Some(sql) = query {
                println!();
                println!(
                    "Running query (whole table materialized for this CLI command -- NOT the \
                     production streaming read path): {}",
                    sql
                );
                match native_write::read_back(&path) {
                    Ok((schema, batches)) => {
                        let mut ctx = ExecutionContext::new();
                        ctx.register_table(name.clone(), schema, batches);
                        match ctx.sql(&sql).await {
                            Ok(result) => print_results(&result),
                            Err(e) => {
                                eprintln!("Error: {}", e);
                                std::process::exit(1);
                            }
                        }
                    }
                    Err(e) => {
                        eprintln!("Error reading back {}: {}", path.display(), e);
                        std::process::exit(1);
                    }
                }
            }
        }

        Commands::Repl {
            tpch,
            #[cfg(feature = "lance")]
            tpch_lance,
        } => {
            #[cfg(feature = "lance")]
            run_repl(tpch, tpch_lance).await;
            #[cfg(not(feature = "lance"))]
            run_repl(tpch).await;
        }

        Commands::Topology => print_topology(),

        Commands::Serve {
            bind,
            peers,
            peers_dns,
            peers_dns_port,
            node_id,
            advertise,
            data,
            tables,
            metastore,
            metastore_metalake,
            metastore_catalog,
            metastore_schema,
            memory_limit,
            discovery_interval_ms,
            probe_timeout_ms,
            drain_ms,
            shutdown_grace_ms,
            flight_bind,
            pulsar_admin,
            pulsar_namespace,
        } => {
            use query_engine::distributed::{serve, ServeOptions};
            use std::time::Duration;

            if data.is_none() && tables.is_none() && metastore.is_none() && pulsar_admin.is_none() {
                eprintln!(
                    "serve: nothing to serve. Pass --data <tpch-dir>, --tables <dir>, \
                     and/or --metastore <url>."
                );
                std::process::exit(2);
            }
            let metastore_source =
                metastore.map(|base_url| query_engine::metastore::GravitinoSource {
                    base_url,
                    metalake: metastore_metalake,
                    catalog: metastore_catalog,
                    schema: metastore_schema,
                });

            let opts = ServeOptions {
                bind,
                advertise,
                node_id,
                peers,
                peers_dns,
                peers_dns_port,
                discovery_interval: Duration::from_millis(discovery_interval_ms),
                probe_timeout: Duration::from_millis(probe_timeout_ms),
                drain: Duration::from_millis(drain_ms),
                shutdown_grace: Duration::from_millis(shutdown_grace_ms),
                flight_bind,
            };

            let loader = Box::new(move || {
                build_serve_context(
                    &data,
                    &tables,
                    &metastore_source,
                    &pulsar_admin,
                    &pulsar_namespace,
                    &memory_limit,
                )
            });
            if let Err(e) = serve(opts, loader).await {
                eprintln!("serve: {}", e);
                std::process::exit(1);
            }
        }
    }
}

/// Build the `serve` node's tables. Runs off the HTTP path, after the listener
/// is up, so `/healthz` answers while this is still working.
///
/// Every failure here is loud and terminal for readiness: a node whose tables
/// did not load reports `/readyz` false forever with the reason attached,
/// rather than starting up and answering queries against a partial schema.
fn build_serve_context(
    data: &Option<PathBuf>,
    tables: &Option<PathBuf>,
    metastore: &Option<query_engine::metastore::GravitinoSource>,
    pulsar_admin: &Option<String>,
    pulsar_namespace: &str,
    memory_limit: &Option<String>,
) -> query_engine::Result<ExecutionContext> {
    use query_engine::error::QueryError;

    // NO GPU offload for serve nodes: the cluster gates demand byte-exact
    // local answers, and GPU float reduction order differs in the last bits.
    let mut ctx = match memory_limit {
        Some(limit) => {
            let config = query_engine::ExecutionConfig::new().with_memory_limit_str(limit)?;
            ExecutionContext::with_config(config)
        }
        None => ExecutionContext::new(),
    };

    if let Some(dir) = data {
        for table in TPCH_TABLES {
            let file = dir.join(format!("{}.parquet", table));
            if !file.exists() {
                return Err(QueryError::Storage(format!(
                    "--data {} is missing {}.parquet; a TPC-H node must serve all eight tables",
                    dir.display(),
                    table
                )));
            }
            ctx.register_parquet(table, &file)?;
        }
    }

    if let Some(dir) = tables {
        let entries = std::fs::read_dir(dir)
            .map_err(|e| QueryError::Storage(format!("--tables {}: {}", dir.display(), e)))?;
        let mut registered = 0usize;
        let mut paths: Vec<PathBuf> = entries.filter_map(|e| e.ok()).map(|e| e.path()).collect();
        paths.sort();
        for path in paths {
            let name = path
                .file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_default();
            if name.is_empty() {
                continue;
            }

            // Iceberg first: an Iceberg table directory CONTAINS parquet
            // files, so the parquet-dir test would match it and register
            // every historical file — including ones the current snapshot
            // deleted. Detection order is a correctness matter here.
            if path.is_dir() && query_engine::storage::iceberg::is_iceberg_dir(&path) {
                ctx.register_iceberg(&name, &path, None)?;
                registered += 1;
                continue;
            }

            // Native tables (native-tables-foundation epic, task 004): a
            // directory holding `_manifest.json`. Structurally disjoint
            // from both Iceberg (no `.metadata.json`) and Lance (no
            // `.lance` extension, and no parquet files either), so check
            // order relative to them doesn't matter — grouped here because
            // it is the same kind of self-describing-directory detection.
            if path.is_dir() && query_engine::storage::native_table::is_native_table_dir(&path) {
                ctx.register_native_table(&name, &path)?;
                registered += 1;
                continue;
            }

            #[cfg(feature = "lance")]
            if path.is_dir() && path.extension().map(|e| e == "lance").unwrap_or(false) {
                ctx.register_lance(&name, &path)?;
                registered += 1;
                continue;
            }

            let is_parquet_file =
                path.is_file() && path.extension().map(|e| e == "parquet").unwrap_or(false);
            let is_parquet_dir = path.is_dir()
                && std::fs::read_dir(&path)
                    .map(|mut it| {
                        it.any(|e| {
                            e.ok()
                                .map(|e| {
                                    e.path()
                                        .extension()
                                        .map(|x| x == "parquet")
                                        .unwrap_or(false)
                                })
                                .unwrap_or(false)
                        })
                    })
                    .unwrap_or(false);
            if !is_parquet_file && !is_parquet_dir {
                continue;
            }
            ctx.register_parquet(&name, &path)?;
            registered += 1;
        }
        if registered == 0 {
            return Err(QueryError::Storage(format!(
                "--tables {} contains no Parquet files/directories, Iceberg tables, \
                 or Lance datasets",
                dir.display()
            )));
        }
    }

    if let Some(source) = metastore {
        // Loud on any failure: a node that comes up serving HALF the catalog
        // answers "table not found" on exactly the joins that matter. This
        // runs inside the loader (spawn_blocking), so readiness stays false
        // with this error attached until the metastore answers.
        let names = source.register_all(&mut ctx)?;
        eprintln!(
            "metastore {}: registered {} table(s) from {}/{}/{}: {}",
            source.base_url,
            names.len(),
            source.metalake,
            source.catalog,
            source.schema,
            names.join(", ")
        );
    }

    if let Some(admin) = pulsar_admin {
        #[cfg(feature = "pulsar")]
        {
            let (tenant, namespace) = pulsar_namespace.split_once('/').ok_or_else(|| {
                QueryError::Storage(format!(
                    "--pulsar-namespace must be tenant/namespace, got `{pulsar_namespace}`"
                ))
            })?;
            let source = query_engine::storage::PulsarSource {
                admin_url: admin.trim_end_matches('/').to_string(),
                tenant: tenant.to_string(),
                namespace: namespace.to_string(),
            };
            let names = query_engine::storage::register_pulsar_namespace(&mut ctx, &source)?;
            eprintln!(
                "pulsar {}: registered {} topic(s) from {}: {}",
                admin,
                names.len(),
                pulsar_namespace,
                names.join(", ")
            );
        }
        #[cfg(not(feature = "pulsar"))]
        {
            let _ = admin;
            return Err(QueryError::NotImplemented(
                "--pulsar-admin requires a binary built with --features pulsar".into(),
            ));
        }
    }

    Ok(ctx)
}

/// Report what the topology probe found and where the workers landed.
fn print_topology() {
    use query_engine::execution::topology::{self, Topology};
    let t = Topology::get();
    println!("NUMA nodes:        {}", t.num_numa_nodes());
    for node in &t.nodes {
        println!(
            "  node {:<3} cpus={:?}\n           distances={:?}",
            node.id, node.cpus, node.distances
        );
    }
    println!("Usable CPUs:       {}", t.cpus.len());
    println!("Physical cores:    {}", t.num_physical_cores());
    println!(
        "Uniform cores:     {}   (heterogeneous placement matters: {})",
        t.is_uniform(),
        t.is_heterogeneous()
    );
    println!("Total weight:      {}", t.total_weight());
    println!("Fast-class CPUs:   {:?}", t.fast_cpus());
    println!("Worker threads:    {}", t.default_worker_threads());
    println!("Placement active:  {}", topology::placement_active());
    println!("Preference order:  {:?}", t.preferred_cpu_order());
    println!();
    println!(
        "{:>4} {:>5} {:>5} {:>4} {:>4} {:>10} {:>7}",
        "cpu", "node", "core", "pkg", "smt", "capacity", "weight"
    );
    for c in &t.cpus {
        println!(
            "{:>4} {:>5} {:>5} {:>4} {:>4} {:>10} {:>7}",
            c.id,
            c.node,
            c.core_id,
            c.package_id,
            if c.is_smt_sibling { "yes" } else { "-" },
            c.raw_capacity
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into()),
            c.weight
        );
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

/// Load all eight TPC-H tables from `dir`, registering each with the REPL's
/// completion helper. `lance` selects the Lance loader over the Parquet one.
fn load_tpch_dir(
    ctx: &mut ExecutionContext,
    helper: &Arc<ReplHelper>,
    dir: &Path,
    #[cfg(feature = "lance")] lance: bool,
) {
    println!("Loading TPC-H tables from: {}", dir.display());
    for table in &TPCH_TABLES {
        #[cfg(feature = "lance")]
        let loaded = if lance {
            ctx.register_lance(*table, dir.join(format!("{}.lance", table)))
        } else {
            ctx.register_parquet(*table, dir.join(format!("{}.parquet", table)))
        };
        #[cfg(not(feature = "lance"))]
        let loaded = ctx.register_parquet(*table, dir.join(format!("{}.parquet", table)));

        match loaded {
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

/// Run the interactive SQL REPL
async fn run_repl(
    tpch_path: Option<PathBuf>,
    #[cfg(feature = "lance")] tpch_lance_path: Option<PathBuf>,
) {
    println!("Query Engine Interactive SQL Shell");
    println!("Type .help for available commands, or enter SQL queries.");
    println!("Tab completion and syntax highlighting enabled.");
    println!();

    let mut ctx = ExecutionContext::new();
    #[cfg(feature = "gpu")]
    ctx.enable_gpu_offload();
    let mut state = ReplState::new();
    let helper = Arc::new(ReplHelper::new());

    // Preload TPC-H tables if path provided
    if let Some(path) = tpch_path {
        load_tpch_dir(
            &mut ctx,
            &helper,
            &path,
            #[cfg(feature = "lance")]
            false,
        );
    }
    #[cfg(feature = "lance")]
    if let Some(path) = tpch_lance_path {
        load_tpch_dir(&mut ctx, &helper, &path, true);
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

                // Execute SQL query. `CREATE TABLE ... AS SELECT`,
                // `INSERT INTO ... SELECT/VALUES ...` and
                // `DELETE FROM ... [WHERE ...]` are all DDL/DML that need
                // `&mut ctx` (registering/re-registering the affected
                // native table), so each is routed to its own
                // `ExecutionContext` entrypoint rather than `sql()` (which
                // now refuses all three outright — see `ExecutionContext::
                // sql`'s doc comment for why). Parsed once up front purely
                // to make this routing decision; on a parse failure this
                // falls through to `sql()`, which re-parses and reports
                // the same error the user would otherwise see.
                let start = Instant::now();
                match query_engine::parser::parse_sql(line) {
                    Ok(stmt)
                        if query_engine::planner::create_table_target_name(&stmt).is_some() =>
                    {
                        match ctx.create_table_as_select(line).await {
                            Ok(r) => {
                                println!(
                                    "Created table '{}' ({} rows, {} segment(s), now at version {}) in {:.3}ms\n",
                                    r.table_name,
                                    r.rows,
                                    r.segments,
                                    r.version,
                                    start.elapsed().as_secs_f64() * 1000.0
                                );
                            }
                            Err(e) => {
                                eprintln!("Error: {}\n", e);
                            }
                        }
                    }
                    Ok(stmt) if query_engine::planner::insert_target_name(&stmt).is_some() => {
                        match ctx.insert_into_native_table(line).await {
                            Ok(r) => {
                                println!(
                                    "Inserted {} row(s) into '{}' ({} segment(s) added, now {} row(s) total, version {}) in {:.3}ms\n",
                                    r.rows_inserted,
                                    r.table_name,
                                    r.segments_added,
                                    r.total_rows,
                                    r.version,
                                    start.elapsed().as_secs_f64() * 1000.0
                                );
                            }
                            Err(e) => {
                                eprintln!("Error: {}\n", e);
                            }
                        }
                    }
                    // `DELETE FROM <native table> [WHERE ...]` (native-
                    // tables-mutation epic, task 003) -- same "needs
                    // &mut ctx, not sql()'s materializing &self path"
                    // reasoning as CREATE TABLE/INSERT above.
                    Ok(stmt) if query_engine::planner::delete_target_name(&stmt).is_some() => {
                        match ctx.delete_from_native_table(line).await {
                            Ok(r) => {
                                println!(
                                    "Deleted {} row(s) from '{}' ({} segment(s) dropped, now {} row(s) total, version {}) in {:.3}ms\n",
                                    r.rows_deleted,
                                    r.table_name,
                                    r.segments_dropped,
                                    r.total_rows,
                                    r.version,
                                    start.elapsed().as_secs_f64() * 1000.0
                                );
                            }
                            Err(e) => {
                                eprintln!("Error: {}\n", e);
                            }
                        }
                    }
                    _ => match ctx.sql(line).await {
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
                    },
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
            #[cfg(feature = "lance")]
            {
                println!("  .lance <path> <name> [version]  Load a Lance dataset as table");
                println!("  .lance-versions <path> List a Lance dataset's versions");
                println!("  .tpch-lance <path>     Load all TPC-H tables from Lance directory");
            }
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
        #[cfg(feature = "pulsar")]
        ".pulsar" => {
            if parts.len() < 3 {
                eprintln!("Usage: .pulsar <admin-url> <tenant/namespace>\n");
            } else {
                let (tenant, namespace) = match parts[2].split_once('/') {
                    Some((t, n)) => (t.to_string(), n.to_string()),
                    None => {
                        eprintln!("namespace must be tenant/namespace\n");
                        return true;
                    }
                };
                let source = query_engine::storage::PulsarSource {
                    admin_url: parts[1].trim_end_matches('/').to_string(),
                    tenant,
                    namespace,
                };
                let start = Instant::now();
                match query_engine::storage::register_pulsar_namespace(ctx, &source) {
                    Ok(names) => {
                        for name in &names {
                            if let Some(schema) = ctx.table_schema(name) {
                                let columns: Vec<String> =
                                    schema.fields().iter().map(|f| f.name().clone()).collect();
                                helper.register_table(name, columns);
                            }
                        }
                        println!(
                            "Registered {} pulsar topic(s) in {:.3}ms: {}\n",
                            names.len(),
                            start.elapsed().as_secs_f64() * 1000.0,
                            names.join(", ")
                        );
                    }
                    Err(e) => eprintln!("Error registering pulsar namespace: {e}\n"),
                }
            }
        }
        ".tpch" => {
            if parts.len() < 2 {
                eprintln!("Usage: .tpch <parquet_directory>\n");
            } else {
                load_tpch_dir(
                    ctx,
                    helper,
                    &PathBuf::from(parts[1]),
                    #[cfg(feature = "lance")]
                    false,
                );
            }
        }
        #[cfg(feature = "lance")]
        #[cfg(feature = "lance")]
        ".lance-versions" => {
            if parts.len() < 2 {
                eprintln!("Usage: .lance-versions <path.lance>\n");
            } else {
                let path = PathBuf::from(parts[1]);
                match query_engine::storage::LanceTable::list_versions(&path) {
                    Ok(versions) => {
                        let latest = versions.last().map(|(v, _)| *v).unwrap_or(0);
                        println!("{} version(s) of {}:", versions.len(), path.display());
                        for (v, ts) in &versions {
                            let marker = if *v == latest { "  <- latest" } else { "" };
                            println!("  v{:<6} {}{}", v, ts, marker);
                        }
                        println!(
                            "\nRead one with: .lance {} <name> <version>\n",
                            path.display()
                        );
                    }
                    Err(e) => eprintln!("Error: {}\n", e),
                }
            }
        }
        #[cfg(feature = "lance")]
        ".lance" => {
            if parts.len() < 3 {
                eprintln!("Usage: .lance <path.lance> <table_name> [version]\n");
            } else {
                let path = PathBuf::from(parts[1]);
                let name = parts[2];
                // Optional 4th token is a historical version to time-travel to.
                let version: Option<u64> = parts.get(3).and_then(|v| v.parse().ok());
                if parts.len() > 3 && version.is_none() {
                    eprintln!("Version must be a number, got '{}'\n", parts[3]);
                    return true;
                }
                let start = Instant::now();
                let registered = match version {
                    Some(v) => ctx.register_lance_version(name, &path, v),
                    None => ctx.register_lance(name, &path),
                };
                match registered {
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
        #[cfg(feature = "lance")]
        ".tpch-lance" => {
            if parts.len() < 2 {
                eprintln!("Usage: .tpch-lance <lance_directory>\n");
            } else {
                load_tpch_dir(ctx, helper, &PathBuf::from(parts[1]), true);
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
