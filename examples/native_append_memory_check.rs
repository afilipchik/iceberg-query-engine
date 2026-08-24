//! Task 002 (native-tables-mutation epic) diagnostic: measures REAL peak
//! RSS (`/usr/bin/time -v`, kernel-reported, not estimated) for appending
//! SF=10 `lineitem` (60,000,000 rows, 2.8GB compressed parquet) into an
//! EXISTING native table, mirroring phase 1 task 003's own
//! ~406MB/60M-row `write_from_parquet` (CREATE) measurement methodology
//! (`.claude/epics/archived/native-tables-foundation/003.md`'s Outcome) —
//! Append's streaming discipline must not be ASSUMED to carry over from
//! Create/Overwrite just because it reuses the same buffered-flush shape;
//! this is the actual measurement, not an inference from code review.
//!
//! Two modes, run as SEPARATE process invocations (peak RSS is a
//! whole-process high-water mark, so measuring both phases in one process
//! would let the larger one pollute the smaller one's number) — reported
//! as two separate premises, per this program's standing "report every
//! premise separately" convention (cache-on/off, native/like-for-like,
//! etc. throughout CLAUDE.md):
//!
//! - `direct` (default): calls `native_write::append_to_native_table`
//!   DIRECTLY, fed by `StreamingParquetReader` — the SAME minimal,
//!   SQL-engine-bypassing source construction task 003's own
//!   `write_from_parquet` uses internally. This isolates EXACTLY the
//!   thing this task's own acceptance criterion claims ("Append writes
//!   new segment(s) using the exact same streaming, bounded-memory
//!   discipline Create/Overwrite already use") from every other variable.
//! - `sql`: the full, realistic `INSERT INTO <table> SELECT ...` SQL
//!   round-trip via `ExecutionContext::insert_into_native_table` — parser,
//!   binder, optimizer, physical planner, and the engine's generic
//!   multi-partition parquet-scan machinery, ending in the SAME Append
//!   write core. Answers a DIFFERENT, broader question ("is the whole
//!   user-facing INSERT statement bounded") that also depends on
//!   characteristics `CREATE TABLE ... AS SELECT` already shares (a
//!   large unfiltered source scan's own partitioning/streaming
//!   behavior) — not specific to, or a defect in, this task's own new
//!   Append write core.
//!
//! ```text
//! scripts/claude-safe-build.sh cargo build --release --example native_append_memory_check
//! scripts/claude-safe-build.sh /usr/bin/time -v ./target/release/examples/native_append_memory_check
//! QE_MEM_CHECK_MODE=sql scripts/claude-safe-build.sh /usr/bin/time -v ./target/release/examples/native_append_memory_check
//! ```
//! Read "Maximum resident set size (kbytes)" from `/usr/bin/time -v`'s
//! output — the same metric phase 1 tasks 003/006 both used, run directly
//! against the compiled binary (not through `cargo run`) so the reported
//! number is unambiguously this process's own peak RSS.

use query_engine::storage::native_write::{self, NativeWriteOptions};
use query_engine::storage::{ParquetTable, StreamingParquetReader};
use query_engine::{ExecutionConfig, ExecutionContext};

const SMALL_SEED: &str = "data/tpch-1mb/lineitem.parquet";
const LARGE_SOURCE: &str = "data/tpch-10gb/lineitem.parquet";

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    if !std::path::Path::new(LARGE_SOURCE).exists() {
        eprintln!(
            "skipping: {LARGE_SOURCE} not found (generate SF=10 parquet first, e.g. \
             `query_engine generate-parquet --sf 10 --output data/tpch-10gb`)"
        );
        return Ok(());
    }
    if !std::path::Path::new(SMALL_SEED).exists() {
        eprintln!("skipping: {SMALL_SEED} not found");
        return Ok(());
    }

    let mode = std::env::var("QE_MEM_CHECK_MODE").unwrap_or_else(|_| "direct".to_string());
    match mode.as_str() {
        "direct" => run_direct().await,
        "sql" => run_sql().await,
        other => {
            eprintln!("unknown QE_MEM_CHECK_MODE `{other}` (expected `direct` or `sql`)");
            std::process::exit(2);
        }
    }
}

/// Seed a native table (via the direct `native_write::write_batches`
/// API, not SQL) from `SMALL_SEED`, in a fresh tempdir, and return its
/// directory.
async fn seed_native_table() -> tempfile::TempDir {
    let scratch = tempfile::tempdir().expect("tempdir for the native table root");
    let out_dir = scratch.path().join("t");
    let table = ParquetTable::try_new(SMALL_SEED).expect("open small seed source");
    let schema = query_engine::physical::operators::TableProvider::schema(&table);
    let reader = StreamingParquetReader::from_table(&table, None, 65_536);
    let stream: query_engine::physical::RecordBatchStream = reader.into_stream();
    let result = native_write::write_batches(
        stream,
        schema,
        &out_dir,
        native_write::NativeWriteMode::Create,
    )
    .await
    .expect("seed write_batches(Create) must succeed");
    println!(
        "seed table: {} rows, {} segment(s), version {}",
        result.rows, result.segments, result.version
    );
    scratch
}

/// `direct` mode: `native_write::append_to_native_table` called
/// DIRECTLY, fed by the SAME `StreamingParquetReader` construction task
/// 003's own `write_from_parquet` uses — bypasses the SQL engine
/// entirely, isolating Append's OWN streaming discipline.
async fn run_direct() -> query_engine::Result<()> {
    let scratch = seed_native_table().await;
    let out_dir = scratch.path().join("t");

    let table = ParquetTable::try_new(LARGE_SOURCE).expect("open large lineitem source");
    let reader = StreamingParquetReader::from_table(&table, None, 65_536);
    let stream: query_engine::physical::RecordBatchStream = reader.into_stream();

    println!(
        "[direct] append_to_native_table(StreamingParquetReader over {LARGE_SOURCE}, 60M rows, \
         2.8GB compressed) ..."
    );
    let t0 = std::time::Instant::now();
    let result =
        native_write::append_to_native_table(stream, &out_dir, NativeWriteOptions::default())
            .await
            .expect("append_to_native_table must succeed");
    println!(
        "[direct] appended {} rows ({} segment(s) added, now {} total rows, version {}) in {:?}",
        result.rows_appended,
        result.segments_appended,
        result.total_rows,
        result.version,
        t0.elapsed()
    );
    Ok(())
}

/// `sql` mode: the full, realistic `INSERT INTO <table> SELECT ...`
/// statement via `ExecutionContext::insert_into_native_table`.
async fn run_sql() -> query_engine::Result<()> {
    let scratch = tempfile::tempdir().expect("tempdir for the native table root");
    // 6GB: far above what a bounded, streaming Append should ever need on
    // its own -- but this cap is not what proves anything; /usr/bin/time
    // -v's measured RSS is the actual evidence, for whichever mechanism
    // this run measures.
    let config = ExecutionConfig::default().with_memory_limit(6 * 1024 * 1024 * 1024);
    let mut ctx =
        ExecutionContext::with_config(config).with_native_table_root(scratch.path().to_path_buf());

    ctx.register_parquet("seed", SMALL_SEED)
        .expect("register small lineitem-shaped seed source");
    let created = ctx
        .create_table_as_select("CREATE TABLE t AS SELECT * FROM seed")
        .await
        .expect("seed CREATE TABLE AS SELECT must succeed");
    println!(
        "seed table: {} rows, {} segment(s), version {}",
        created.rows, created.segments, created.version
    );

    ctx.register_parquet("lineitem_src", LARGE_SOURCE)
        .expect("register large lineitem source");

    println!("[sql] INSERT INTO t SELECT * FROM lineitem_src (60M rows, 2.8GB compressed) ...");
    let t0 = std::time::Instant::now();
    let result = ctx
        .insert_into_native_table("INSERT INTO t SELECT * FROM lineitem_src")
        .await
        .expect("INSERT INTO ... SELECT must succeed");
    println!(
        "[sql] inserted {} rows ({} segment(s) added, now {} total rows, version {}) in {:?}",
        result.rows_inserted,
        result.segments_added,
        result.total_rows,
        result.version,
        t0.elapsed()
    );

    Ok(())
}
