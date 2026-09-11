//! Benchmark-only GPU entry point using the production local HTTP/Arrow server.
//! Run through scripts/claude-safe-build.sh. QE_GPU=0 is the CPU control.
//! QE_GPU_DEBUG=1 provides proof of actual device execution.
use clap::Parser;
use query_engine::distributed::{serve, ServeOptions, TableLoader};
use query_engine::{ExecutionConfig, ExecutionContext, QueryError, Result};
use std::path::PathBuf;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:7787")]
    bind: String,
    #[arg(long, default_value = "data/tpch-10gb")]
    data: PathBuf,
    #[arg(long, default_value = "40G")]
    memory_limit: String,
}

#[tokio::main]
async fn main() {
    query_engine::execution::enforce_process_memory_cap();
    query_engine::execution::disable_transparent_hugepages();
    query_engine::execution::topology::init_global_pool();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive(tracing::Level::INFO.into()),
        )
        .init();
    if let Err(error) = run(Args::parse()).await {
        eprintln!("sf10_gpu_serve: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<()> {
    let config = ExecutionConfig::new().with_memory_limit_str(&args.memory_limit)?;
    let gpu_requested = std::env::var("QE_GPU").map(|v| v != "0").unwrap_or(true);
    if gpu_requested {
        #[cfg(feature = "gpu")]
        {
            let engine = query_engine::physical::gpu::GpuEngine::get().ok_or_else(|| {
                QueryError::Execution(
                    "GPU requested but CUDA device/kernel initialization failed".into(),
                )
            })?;
            eprintln!(
                "[sf10-gpu] initialized device; snapshot=[{}]",
                engine.snapshot()
            );
        }
        #[cfg(not(feature = "gpu"))]
        return Err(QueryError::Execution("GPU requires --features gpu".into()));
    } else {
        eprintln!("[sf10-gpu] CPU control selected: QE_GPU=0");
    }
    eprintln!(
        "[sf10-gpu] data={} memory_limit={} ipc_cache={} gpu_requested={}",
        args.data.display(),
        args.memory_limit,
        query_engine::storage::ipc_cache::mode(),
        gpu_requested
    );
    let opts = ServeOptions {
        bind: args.bind,
        flight_bind: Some("none".into()),
        ..ServeOptions::default()
    };
    let loader: TableLoader = Box::new(move || {
        let mut ctx = ExecutionContext::with_config(config);
        if gpu_requested {
            ctx.enable_gpu_offload();
        }
        for table in [
            "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
        ] {
            let path = args.data.join(format!("{table}.parquet"));
            if !path.is_file() {
                return Err(QueryError::Storage(format!("missing {}", path.display())));
            }
            ctx.register_parquet(table, &path)?;
        }
        Ok(ctx)
    });
    serve(opts, loader).await?;
    #[cfg(feature = "gpu")]
    if gpu_requested {
        if let Some(engine) = query_engine::physical::gpu::GpuEngine::get() {
            eprintln!("[sf10-gpu] final snapshot=[{}]", engine.snapshot());
        }
    }
    Ok(())
}
