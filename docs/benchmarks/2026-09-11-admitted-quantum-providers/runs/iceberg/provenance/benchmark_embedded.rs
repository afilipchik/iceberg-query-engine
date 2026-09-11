//! Persistent JSON-lines adapter for matched embedded benchmark timing.
//! Run inside scripts/claude-safe-build.sh; the parent enforces deadlines.
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use query_engine::{ExecutionConfig, ExecutionContext, QueryError};
use serde::Deserialize;
use serde_json::json;
use std::fs::File;
use std::io::{self, BufRead, BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;

#[path = "benchmark_support/ipc_preload.rs"]
mod ipc_preload;

#[derive(Deserialize)]
struct Table {
    name: String,
    path: PathBuf,
    #[serde(default)]
    snapshot_id: Option<i64>,
    #[serde(default)]
    version: Option<u64>,
}

#[derive(Deserialize)]
struct Setup {
    track: String,
    memory_limit: String,
    threads: usize,
    tables: Vec<Table>,
    #[serde(default)]
    gpu_residency_required: bool,
    #[serde(default)]
    host_arrow_preloaded: bool,
}

#[derive(Deserialize)]
struct Request {
    id: String,
    sql: String,
    #[serde(default)]
    output: PathBuf,
    #[serde(default = "query_operation")]
    operation: String,
    #[serde(default)]
    session_id: Option<u64>,
    #[serde(default)]
    preparation_timeout_ms: Option<u64>,
}
fn query_operation() -> String {
    "query".into()
}

fn emit(value: serde_json::Value) -> anyhow::Result<()> {
    let mut stdout = io::stdout().lock();
    serde_json::to_writer(&mut stdout, &value)?;
    writeln!(stdout)?;
    stdout.flush()?;
    Ok(())
}

fn main() -> anyhow::Result<()> {
    query_engine::execution::enforce_process_memory_cap();
    query_engine::execution::disable_transparent_hugepages();
    if std::env::args().nth(1).as_deref() == Some("--write-native") {
        let args: Vec<String> = std::env::args().collect();
        anyhow::ensure!(
            args.len() == 4,
            "--write-native requires source Parquet and new destination"
        );
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime.block_on(query_engine::storage::native_write::write_from_parquet(
            &args[2],
            &args[3],
            query_engine::storage::native_write::NativeWriteMode::Create,
        ))?;
        return emit(json!({"status": "converted"}));
    }
    let setup_path = std::env::args()
        .nth(1)
        .ok_or_else(|| anyhow::anyhow!("setup JSON required"))?;
    let setup: Setup = serde_json::from_reader(File::open(setup_path)?)?;
    anyhow::ensure!(setup.threads > 0, "threads must be positive");
    std::env::set_var("RAYON_NUM_THREADS", setup.threads.to_string());
    anyhow::ensure!(
        matches!(
            setup.track.as_str(),
            "raw_parquet" | "native" | "iceberg" | "lance" | "decoded_ipc" | "gpu" | "gpu_control"
        ),
        "unsupported track: {}",
        setup.track
    );
    std::env::set_var("QE_IPC_CACHE", "0");
    if setup.track == "gpu" {
        std::env::set_var("QE_GPU_DEBUG", "1");
    }
    query_engine::execution::topology::init_global_pool();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(setup.threads)
        .enable_all()
        .build()?;
    runtime.block_on(run(setup))
}

async fn run(setup: Setup) -> anyhow::Result<()> {
    let started = Instant::now();
    let config = ExecutionConfig::new()
        .with_memory_limit_str(&setup.memory_limit)?
        .with_spill_path(std::env::temp_dir().join("engine-spill"));
    // Declare first so the context and its registered tables drop before the
    // residency allowance, including on early returns.
    let _preload_admission;
    let mut context = ExecutionContext::with_config(config).with_parallel_partitions(setup.threads);
    if setup.track == "gpu" || setup.track == "gpu_control" {
        anyhow::ensure!(
            cfg!(feature = "gpu"),
            "GPU and its same-binary CPU control require --features gpu"
        );
    }
    if setup.track == "gpu" {
        #[cfg(feature = "gpu")]
        anyhow::ensure!(
            query_engine::physical::gpu::GpuEngine::get().is_some(),
            "GPU device/kernel initialization failed"
        );
        context.enable_gpu_offload();
    }
    anyhow::ensure!(
        !setup.gpu_residency_required || (setup.track == "gpu" && setup.host_arrow_preloaded),
        "resident GPU requires gpu track and immutable preloaded Arrow inputs"
    );
    anyhow::ensure!(
        !setup.host_arrow_preloaded
            || matches!(setup.track.as_str(), "gpu" | "gpu_control" | "decoded_ipc"),
        "explicit host Arrow preload is only supported for IPC/GPU tracks"
    );
    _preload_admission = if setup.track == "decoded_ipc" || setup.host_arrow_preloaded {
        match ipc_preload::admit(
            setup.tables.iter().map(|t| t.path.as_path()),
            context.memory_pool(),
        ) {
            Ok(admission) => Some(admission),
            Err(error) => {
                emit(
                    json!({"status": if error.is_memory_limit() { "refused" } else { "error" },
                    "phase": "setup", "error_kind": "preload_admission", "error": error.to_string()}),
                )?;
                return Err(error.into());
            }
        }
    } else {
        None
    };
    for table in &setup.tables {
        match if setup.host_arrow_preloaded {
            "decoded_ipc"
        } else {
            setup.track.as_str()
        } {
            "raw_parquet" | "gpu" | "gpu_control" => {
                context.register_parquet(&table.name, &table.path)?
            }
            "decoded_ipc" => {
                let reader = StreamReader::try_new(File::open(&table.path)?, None)?;
                let schema = reader.schema();
                let batches = reader.collect::<std::result::Result<Vec<_>, _>>()?;
                context.register_table(&table.name, schema, batches);
            }
            "native" => context.register_native_table(&table.name, &table.path)?,
            "iceberg" => context.register_iceberg(&table.name, &table.path, table.snapshot_id)?,
            "lance" => {
                #[cfg(feature = "lance")]
                {
                    let version = table
                        .version
                        .ok_or_else(|| anyhow::anyhow!("Lance version is required"))?;
                    let provider = query_engine::storage::LanceTable::try_new_at_version(
                        &table.path,
                        version,
                    )?;
                    provider.warm_statistics();
                    context.register_table_provider(&table.name, std::sync::Arc::new(provider));
                }
                #[cfg(not(feature = "lance"))]
                anyhow::bail!("Lance requires a binary built with --features lance");
            }
            _ => unreachable!(),
        }
    }
    emit(
        json!({"status": "ready", "setup_ms": started.elapsed().as_secs_f64() * 1000.0,
                "features": {"lance": cfg!(feature = "lance"), "gpu": cfg!(feature = "gpu")},
                "track": setup.track, "threads": setup.threads,
                "gpu_enabled": setup.track == "gpu", "ipc_cache": "off", "gpu_snapshot": gpu_snapshot(),
                "gpu_residency_required": setup.gpu_residency_required,
                "residency": if setup.track == "decoded_ipc" || setup.host_arrow_preloaded { "host_arrow_preloaded" } else { "warm_host_files" }}),
    )?;
    #[cfg(feature = "gpu")]
    let mut resident_session: Option<query_engine::execution::PreparedGpuSession> = None;
    for line in io::stdin().lock().lines() {
        let request: Request = serde_json::from_str(&line?)?;
        emit(json!({"id": request.id, "event": "started"}))?;
        if request.operation == "prepare_gpu_resident" {
            let started = Instant::now();
            #[cfg(feature = "gpu")]
            let prepared = async {
                if !setup.gpu_residency_required || resident_session.is_some() {
                    return Err(QueryError::InvalidArgument(
                        "resident preparation requires enabled policy and no active session".into(),
                    ));
                }
                let timeout = request
                    .preparation_timeout_ms
                    .filter(|v| *v > 0)
                    .ok_or_else(|| {
                        QueryError::InvalidArgument(
                            "positive preparation_timeout_ms required".into(),
                        )
                    })?;
                context
                    .prepare_gpu_resident(&request.sql, std::time::Duration::from_millis(timeout))
                    .await
            }
            .await;
            #[cfg(feature = "gpu")]
            match prepared {
                Ok(session) => {
                    let metadata = serde_json::to_value(session.metadata())?;
                    resident_session = Some(session);
                    emit(
                        json!({"id":request.id,"status":"prepared", "preparation_ms":started.elapsed().as_secs_f64()*1000.0,"preparation":metadata}),
                    )?;
                }
                Err(error) => emit(
                    json!({"id":request.id,"status":"preparation_error", "preparation_ms":started.elapsed().as_secs_f64()*1000.0,"error_kind":error.kind(),"error":error.to_string()}),
                )?,
            }
            #[cfg(not(feature = "gpu"))]
            emit(
                json!({"id":request.id,"status":"preparation_error","error":"GPU feature unavailable","preparation_ms":started.elapsed().as_secs_f64()*1000.0}),
            )?;
            continue;
        }
        if request.operation == "release_gpu_resident" {
            #[cfg(feature = "gpu")]
            let released = if resident_session.as_ref().map(|s| s.metadata().session_id)
                == request.session_id
                && request.session_id.is_some()
            {
                resident_session = None;
                true
            } else {
                false
            };
            #[cfg(not(feature = "gpu"))]
            let released = false;
            emit(json!({"id":request.id,"status":if released {"released"} else {"query_error"}}))?;
            continue;
        }
        if request.operation != "query" || request.output.as_os_str().is_empty() {
            emit(
                json!({"id":request.id,"status":"query_error","error":"query operation and output path required"}),
            )?;
            continue;
        }
        let gpu_before = gpu_snapshot();
        let profile_before = aggregate_profile();
        let started = Instant::now();
        #[cfg(feature = "gpu")]
        let mut resident_evidence = None;
        #[cfg(feature = "gpu")]
        let result = if setup.gpu_residency_required {
            match resident_session
                .as_ref()
                .filter(|s| Some(s.metadata().session_id) == request.session_id)
            {
                Some(session) => {
                    let outcome = context.sql_gpu_resident(&request.sql, session).await;
                    resident_evidence = Some(outcome.evidence);
                    outcome.result
                }
                None => Err(QueryError::InvalidArgument(
                    "matching prepared GPU session required".into(),
                )),
            }
        } else {
            context.sql(&request.sql).await
        };
        #[cfg(not(feature = "gpu"))]
        let result = context.sql(&request.sql).await;
        // sql() eagerly collects every Arrow batch. Disk serialization is
        // outside this same timing boundary on both sides.
        let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
        #[cfg(feature = "gpu")]
        let gpu_resident_evidence = serde_json::to_value(resident_evidence)?;
        #[cfg(not(feature = "gpu"))]
        let gpu_resident_evidence = serde_json::Value::Null;
        let aggregation_profile = profile_before.zip(aggregate_profile()).map(|(before, after)| {
            let delta: Vec<u64> = after.iter().zip(before.iter()).map(|(a, b)| a.saturating_sub(*b)).collect();
            json!({"scope": "serialized query; cumulative worker nanoseconds",
                "row_group_read_with_pushdown_ns": delta[0], "process_batch_ns": delta[1],
                "group_expression_ns": delta[2], "aggregate_expression_ns": delta[3],
                "state_update_and_key_handling_ns": delta[1].saturating_sub(delta[2]).saturating_sub(delta[3])})
        });
        emit(json!({"id": request.id, "event": "query_finished", "ms": elapsed_ms}))?;
        match result {
            Ok(result) => {
                let serialization = Instant::now();
                let file = File::create(&request.output)?;
                let mut writer = StreamWriter::try_new(BufWriter::new(file), &result.schema)?;
                for batch in &result.batches {
                    writer.write(batch)?;
                }
                writer.finish()?;
                writer.into_inner()?.flush()?;
                emit(
                    json!({"id": request.id, "status": "completed", "ms": elapsed_ms,
                    "rows": result.row_count, "output": request.output,
                    "gpu_before": gpu_before, "gpu_after": gpu_snapshot(),
                    "gpu_resident_evidence": gpu_resident_evidence,
                    "aggregation_profile": aggregation_profile,
                    "physical_plan": result.metrics.physical_plan,
                    "optimized_plan": result.metrics.optimized_plan,
                    "spill_metrics": result.metrics.spill_metrics.as_ref().map(|m| json!({
                        "partitions_spilled": m.partitions_spilled, "bytes_spilled": m.bytes_spilled,
                        "spill_time_ms": m.spill_time_ms, "read_back_time_ms": m.read_back_time_ms,
                        "spill_files_created": m.spill_files_created})),
                    "serialization_ms": serialization.elapsed().as_secs_f64() * 1000.0,
                    "metrics": {"parse_ms": result.metrics.parse_time.as_secs_f64() * 1000.0,
                        "plan_ms": result.metrics.plan_time.as_secs_f64() * 1000.0,
                        "optimize_ms": result.metrics.optimize_time.as_secs_f64() * 1000.0,
                        "execute_ms": result.metrics.execute_time.as_secs_f64() * 1000.0,
                        "local_memory_high_water_bytes": result.metrics.peak_memory_bytes,
                        "query_id": result.metrics.query_id,
                        "reserved_peak_memory_bytes": result.metrics.reserved_peak_memory_bytes,
                        "observed_peak_memory_bytes": result.metrics.observed_peak_memory_bytes}}),
                )?;
            }
            Err(error) => {
                let status = if matches!(error, QueryError::NotImplemented(_)) {
                    "unsupported"
                } else {
                    "query_error"
                };
                emit(json!({"id": request.id, "status": status, "ms": elapsed_ms,
                            "error_kind": error.kind(), "error": error.to_string(),
                            "gpu_resident_evidence": gpu_resident_evidence}))?;
            }
        }
    }
    Ok(())
}

fn aggregate_profile() -> Option<[u64; 4]> {
    use query_engine::physical::morsel_agg::{
        AGG_PROF_AGGEVAL_NS, AGG_PROF_GROUP_NS, AGG_PROF_PROCESS_NS, AGG_PROF_SCAN_NS,
    };
    use std::sync::atomic::Ordering::Relaxed;
    std::env::var_os("QE_AGG_PROF").map(|_| {
        [
            AGG_PROF_SCAN_NS.load(Relaxed),
            AGG_PROF_PROCESS_NS.load(Relaxed),
            AGG_PROF_GROUP_NS.load(Relaxed),
            AGG_PROF_AGGEVAL_NS.load(Relaxed),
        ]
    })
}

fn gpu_snapshot() -> serde_json::Value {
    #[cfg(feature = "gpu")]
    if std::env::var("QE_GPU").as_deref() != Ok("0") {
        if let Some(engine) = query_engine::physical::gpu::GpuEngine::get() {
            let snapshot = engine.snapshot();
            return json!({"resident_columns": snapshot.resident_columns, "resident_bytes": snapshot.resident_bytes,
                "budget_bytes": snapshot.budget_bytes, "eviction_count": snapshot.eviction_count,
                "upload_failures": snapshot.upload_failures, "run_fallbacks": snapshot.run_fallbacks});
        }
    }
    serde_json::Value::Null
}
