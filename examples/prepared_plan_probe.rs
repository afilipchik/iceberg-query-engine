//! Initialization-only diagnostic. Run a fresh contained process for each child path.
//! NEVER polls a prepared output stream; preparation itself can execute build inputs.
use arrow::ipc::reader::StreamReader;
use query_engine::{
    execution::SharedMemoryPool,
    physical::{PhysicalOperator, PreparedOutputBound},
    ExecutionConfig, ExecutionContext,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    fs::File,
    io::{self, Write},
    path::PathBuf,
    sync::Arc,
};

#[derive(Deserialize)]
struct Table {
    name: String,
    path: PathBuf,
}
#[derive(Deserialize)]
struct Setup {
    track: String,
    memory_limit: String,
    threads: usize,
    tables: Vec<Table>,
    #[serde(default)]
    process_cap_bytes: Option<u64>,
}
fn emit(value: Value) -> anyhow::Result<()> {
    let mut out = io::stdout().lock();
    serde_json::to_writer(&mut out, &value)?;
    writeln!(out)?;
    out.flush()?;
    Ok(())
}
fn pool_state(pool: &SharedMemoryPool) -> Value {
    json!({"used":pool.used(),"available":pool.available(),"limit":pool.max(),"peak":pool.peak(),"reserved_peak":pool.reserved_peak(),"observed_peak":pool.observed_peak()})
}
fn tree(node: &Arc<dyn PhysicalOperator>, path: &str) -> anyhow::Result<()> {
    emit(
        json!({"event":"operator","path":path,"name":node.name(),"partitions":node.output_partitions(),"schema":format!("{:?}",node.schema())}),
    )?;
    for (i, child) in node.children().iter().enumerate() {
        tree(child, &format!("{path}.{i}"))?;
    }
    Ok(())
}
fn main() -> anyhow::Result<()> {
    query_engine::execution::enforce_process_memory_cap();
    query_engine::execution::disable_transparent_hugepages();
    let args: Vec<String> = std::env::args().collect();
    anyhow::ensure!(
        matches!(args.len(), 4 | 5),
        "usage: prepared_plan_probe SETUP.json SQL.sql root[.CHILD_INDEX...] [copied|admitted]"
    );
    let setup: Setup = serde_json::from_reader(File::open(&args[1])?)?;
    anyhow::ensure!(setup.threads > 0, "positive thread count required");
    anyhow::ensure!(
        matches!(setup.track.as_str(), "raw_parquet" | "decoded_ipc"),
        "probe supports raw_parquet/decoded_ipc only"
    );
    if let Some(expected) = setup.process_cap_bytes {
        anyhow::ensure!(u64::try_from(query_engine::execution::process_memory_pool().max())? == expected,
            "setup process cap disagrees with enforced QE_MEM_CAP/process pool; launch with matched environment");
    }
    let path = if args[3] == "root" {
        Vec::new()
    } else {
        let suffix = args[3]
            .strip_prefix("root.")
            .ok_or_else(|| anyhow::anyhow!("path must start root."))?;
        suffix
            .split('.')
            .map(str::parse::<usize>)
            .collect::<std::result::Result<Vec<_>, _>>()?
    };
    std::env::set_var("RAYON_NUM_THREADS", setup.threads.to_string());
    std::env::set_var("QE_IPC_CACHE", "0");
    std::env::set_var("QE_GPU", "0");
    query_engine::execution::topology::init_global_pool();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(setup.threads)
        .enable_all()
        .build()?;
    let sql = std::fs::read_to_string(&args[2])?;
    runtime.block_on(run(setup, sql, path, &args))
}
async fn run(setup: Setup, sql: String, path: Vec<usize>, args: &[String]) -> anyhow::Result<()> {
    let config = ExecutionConfig::new()
        .with_memory_limit_str(&setup.memory_limit)?
        .with_spill_path(std::env::temp_dir().join("engine-spill"));
    let mut context = ExecutionContext::with_config(config).with_parallel_partitions(setup.threads);
    for table in &setup.tables {
        match setup.track.as_str() {
            "raw_parquet" => context.register_parquet(&table.name, &table.path)?,
            "decoded_ipc" => {
                let reader = StreamReader::try_new(File::open(&table.path)?, None)?;
                let schema = reader.schema();
                let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;
                context.register_table(&table.name, schema, batches);
            }
            _ => unreachable!(),
        }
    }
    let pool = context.memory_pool().clone();
    emit(
        json!({"event":"setup","command":args,"track":setup.track,"threads":setup.threads,"rayon_threads":rayon::current_num_threads(),"memory_limit":setup.memory_limit,"declared_process_cap_bytes":setup.process_cap_bytes,"actual_process_pool_limit":query_engine::execution::process_memory_pool().max(),"QE_MEM_CAP":std::env::var("QE_MEM_CAP").ok(),"purpose":"initialization-only; no output pulls; not benchmark timing","pool_scope":"physical_plan debug API uses context pool, not sql() per-query child","pool":pool_state(&pool)}),
    )?;
    let root = context.physical_plan(&sql)?;
    tree(&root, "root")?;
    let mut selected = root.clone();
    for index in path {
        let children = selected.children();
        selected = children
            .get(index)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("selected child index out of range: {index}"))?;
    }
    let before = pool_state(&pool);
    let static_queue = selected
        .pool_independent_queue_copy_bound()
        .and_then(|b| b.max_bytes());
    let static_gather = selected
        .pool_independent_gather_copy_bound()
        .and_then(|b| b.gather(4096))
        .and_then(|b| b.max_bytes());
    emit(
        json!({"event":"before_prepare","path":args[3],"name":selected.name(),"partitions":selected.output_partitions(),"static_queue_max_bytes":static_queue,"static_gather_4096_max_bytes":static_gather,"pool_before_static_metadata":before,"pool":pool_state(&pool)}),
    )?;
    if args.get(4).is_some_and(|mode| mode == "admitted") {
        let prepared = selected.prepare_admitted_queue_input(pool.clone()).await?;
        emit(
            json!({"event":"admitted_prepared_held", "available":prepared.is_some(),
            "declared_partitions":selected.output_partitions(), "output_stream_pulls":0,
            "pool":pool_state(&pool)}),
        )?;
        drop(prepared);
        emit(json!({"event":"after_descriptor_drop", "pool":pool_state(&pool)}))?;
        drop(selected);
        drop(root);
        drop(context);
        tokio::task::yield_now().await;
        emit(
            json!({"event":"after_plan_context_drop", "output_stream_pulls":0,
            "pool":pool_state(&pool), "cleanup_note":"one cooperative yield is not proof of async cleanup"}),
        )?;
        return Ok(());
    }
    anyhow::ensure!(
        args.get(4).is_none_or(|mode| mode == "copied"),
        "unknown preparation mode"
    );
    let prepared = match selected.prepare_queue_input().await {
        Ok(prepared) => prepared,
        Err(error) => {
            emit(
                json!({"event":"prepare_error","kind":error.kind(),"error":error.to_string(),"pool":pool_state(&pool)}),
            )?;
            return Err(error.into());
        }
    };
    let (kind, bytes, count) = match &prepared {
        None => ("None", None, 0),
        Some(p) => (
            match &p.output {
                PreparedOutputBound::Unknown => "Unknown",
                PreparedOutputBound::Bytes(_) => "Bytes",
                PreparedOutputBound::Layouts(_) => "Layouts",
            },
            p.output.max_bytes(),
            p.streams.len(),
        ),
    };
    let slots = selected
        .output_partitions()
        .min(rayon::current_num_threads().max(1));
    let envelope_candidates:Vec<_>=(2..=slots).rev().map(|k|json!({"slots":k,"checked_bytes":bytes.and_then(|b|b.checked_mul(k)),"fits_snapshot_available":bytes.and_then(|b|b.checked_mul(k)).map(|b|b<=pool.available())})).collect();
    emit(
        json!({"event":"prepared_held","kind":kind,"max_bytes":bytes,"stream_count":count,"pool":pool_state(&pool),"envelope_candidates":envelope_candidates,"envelope_note":"arithmetic snapshot only; no admission attempted, no queue selected","output_stream_pulls":0}),
    )?;
    drop(prepared);
    emit(json!({"event":"after_descriptor_drop","pool":pool_state(&pool)}))?;
    drop(selected);
    drop(root);
    drop(context);
    // Yield for cancellation bookkeeping, but never poll any prepared stream.
    tokio::task::yield_now().await;
    emit(
        json!({"event":"after_plan_context_drop","pool":pool_state(&pool),"cleanup_note":"one cooperative yield is not a proof of complete async cleanup; cache/provider ownership can outlive descriptor","output_stream_pulls":0}),
    )?;
    Ok(())
}
