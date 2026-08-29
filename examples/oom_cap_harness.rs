//! `oom-safety-hardening` epic, task 001: the ONE reusable adversarial
//! memory-cap harness (PRD G6). Four scenarios — one per OOM-shaped gap the
//! epic exists to close — each runnable under BOTH cap levers:
//!
//!   1. `agg`         — large GROUP BY through `SpillableHashAggregateExec`'s
//!                      collect-then-decide path (a `COUNT(DISTINCT ...)`
//!                      aggregate is used deliberately: `distinct` makes the
//!                      shape fused-streaming-INELIGIBLE, so it goes straight
//!                      to `collect_input_partitions_concurrently`, the exact
//!                      pre-fix hole named by the PRD). Pre-fix expectation:
//!                      kernel kill / abort under a cap smaller than the input.
//!   2. `sort`        — large `ORDER BY` through `ExternalSortExec` (same
//!                      collect-then-decide hole). Same pre-fix expectation.
//!   3. `native-scan` — a native-table scan whose segments exceed
//!                      `check_scan_budget`'s admission threshold, feeding an
//!                      aggregate. Pre-fix expectation: CLEAN NAMED REFUSAL
//!                      (exit 2 — that is a PASS today and documents the
//!                      boundary; post-fix per PRD G2 this must COMPLETE by
//!                      spilling instead).
//!   4. `insert`      — `CREATE TABLE ... AS SELECT` from a large parquet
//!                      source at a 512MB-class cap (the documented residual
//!                      from `native-tables-mutation` task 005). Pre-fix
//!                      expectation: SIGKILL.
//!
//! Cap levers (driven by `scripts/oom_cap_harness.sh`, which wraps every run):
//!   - cgroup: `systemd-run --user --scope -p MemoryMax=<N>` — kernel
//!     memcg kill (exit 137).
//!   - rlimit: `QE_MEM_CAP=<N>` — this binary's FIRST statement is
//!     `enforce_process_memory_cap()` (the same in-binary `RLIMIT_DATA` cap
//!     `src/main.rs` applies), so the engine aborts at the cap (exit 134)
//!     with the terminal untouched. Examples do NOT inherit this from
//!     `main.rs`, which is why it is called here explicitly.
//!
//! Exit-code protocol (consumed by the shell driver):
//!   0   = scenario COMPLETED with a correct-looking result
//!   2   = scenario REFUSED cleanly (a named engine error; process alive)
//!   1   = wrong result / unexpected error class
//!   134 = abort at the rlimit cap (allocation failure) — a FAIL verdict
//!   137 = SIGKILL by the kernel/memcg — a FAIL verdict
//!
//! Env knobs:
//!   QE_HARNESS_ROWS          synthetic rows for agg/sort (default 100_000_000)
//!   QE_HARNESS_MEMORY_LIMIT  engine memory_limit bytes for agg/sort
//!                            (default 256MB — far below the input, so the
//!                            operator MUST decide to spill to survive)
//!   QE_HARNESS_NATIVE_TABLE  native table dir for `native-scan`
//!                            (default data/tpch-10gb-native/lineitem)
//!   QE_HARNESS_SCAN_LIMIT    memory_limit for `native-scan` (default 512MB)
//!   QE_HARNESS_PARQUET       parquet source for `insert`
//!                            (default data/tpch-10gb/lineitem.parquet)
//!   QE_HARNESS_CTAS_ROOT     native_table_root for `insert`
//!                            (default .scratch/oom001/ctas_root)

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::stream::TryStreamExt;
use query_engine::execution::create_memory_pool;
use query_engine::physical::operators::spillable::AggregateExpr;
use query_engine::physical::operators::{ExternalSortExec, SpillableHashAggregateExec};
use query_engine::physical::{PhysicalOperator, RecordBatchStream};
use query_engine::planner::{AggregateFunction, Expr, SortExpr};
use query_engine::{ExecutionConfig, ExecutionContext};
use std::sync::Arc;

const BATCH_ROWS: i64 = 131_072;

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn peak_rss_mb() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmHWM:") {
            return rest
                .trim()
                .trim_end_matches(" kB")
                .trim()
                .parse::<u64>()
                .ok()
                .map(|kb| kb / 1024);
        }
    }
    None
}

/// Same lazily-generating source as `spill_join_oom_repro.rs` — a pre-built
/// `Vec<RecordBatch>` would make ITS collection determine peak memory, not
/// the operator under test.
#[derive(Debug)]
struct LazyGeneratorExec {
    schema: SchemaRef,
    total_rows: i64,
}

fn make_batch(schema: &SchemaRef, start: i64, n: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n).collect();
    // `val` cycles through a large-but-bounded space so COUNT(DISTINCT val)
    // and ORDER BY val both do real work without degenerate all-equal keys.
    let vals: Vec<i64> = (0..n).map(|i| (start + i).wrapping_mul(2654435761) % 1_000_003).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
        ],
    )
    .expect("build synthetic batch")
}

#[async_trait::async_trait]
impl PhysicalOperator for LazyGeneratorExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn output_partitions(&self) -> usize {
        1
    }
    fn name(&self) -> &str {
        "LazyGenerator"
    }
    async fn execute(&self, partition: usize) -> query_engine::Result<RecordBatchStream> {
        if partition != 0 {
            return Ok(Box::pin(futures::stream::empty()));
        }
        let schema = self.schema.clone();
        let total_rows = self.total_rows;
        let stream = futures::stream::unfold(0i64, move |emitted| {
            let schema = schema.clone();
            async move {
                if emitted >= total_rows {
                    return None;
                }
                let n = BATCH_ROWS.min(total_rows - emitted);
                Some((Ok(make_batch(&schema, emitted, n)), emitted + n))
            }
        });
        Ok(Box::pin(stream))
    }
}

fn gen_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]))
}

fn spill_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("qe_oom_cap_harness_{}_{}", tag, std::process::id()))
}

async fn scenario_agg() -> query_engine::Result<String> {
    let total_rows = env_usize("QE_HARNESS_ROWS", 100_000_000) as i64;
    let memory_limit = env_usize("QE_HARNESS_MEMORY_LIMIT", 256 * 1024 * 1024);
    let input: Arc<dyn PhysicalOperator> = Arc::new(LazyGeneratorExec {
        schema: gen_schema(),
        total_rows,
    });
    // GROUP BY val (1,000,003 distinct groups), COUNT(DISTINCT id).
    // `distinct: true` makes this fused-streaming-INELIGIBLE by
    // construction (`fused_streaming_eligible` requires `!a.distinct`), so
    // execution goes straight to the collect-then-decide path under test.
    let out_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Int64, false),
        Field::new("cnt", DataType::Int64, true),
    ]));
    let sd = spill_dir("agg");
    let _ = std::fs::remove_dir_all(&sd);
    let config = ExecutionConfig::new()
        .with_memory_limit(memory_limit)
        .with_spill_path(sd.clone());
    let agg = SpillableHashAggregateExec::new(
        input,
        vec![Expr::column("val")],
        vec![AggregateExpr {
            func: AggregateFunction::CountDistinct,
            input: Expr::column("id"),
            distinct: true,
            second_arg: None,
        }],
        out_schema,
        create_memory_pool(memory_limit),
        config,
    );
    let mut stream = agg.execute(0).await?;
    let mut groups = 0usize;
    while let Some(batch) = stream.try_next().await? {
        groups += batch.num_rows();
    }
    let _ = std::fs::remove_dir_all(&sd);
    let expected = 1_000_003.min(total_rows as usize);
    if groups == expected {
        Ok(format!("groups={groups} (expected {expected})"))
    } else {
        Err(query_engine::QueryError::Execution(format!(
            "WRONG RESULT: groups={groups}, expected {expected}"
        )))
    }
}

async fn scenario_sort() -> query_engine::Result<String> {
    let total_rows = env_usize("QE_HARNESS_ROWS", 100_000_000) as i64;
    let memory_limit = env_usize("QE_HARNESS_MEMORY_LIMIT", 256 * 1024 * 1024);
    let input: Arc<dyn PhysicalOperator> = Arc::new(LazyGeneratorExec {
        schema: gen_schema(),
        total_rows,
    });
    let sd = spill_dir("sort");
    let _ = std::fs::remove_dir_all(&sd);
    let config = ExecutionConfig::new()
        .with_memory_limit(memory_limit)
        .with_spill_path(sd.clone());
    let sort = ExternalSortExec::new(
        input,
        vec![SortExpr::new(Expr::column("val")).asc()],
        create_memory_pool(memory_limit),
        config,
    );
    let mut stream = sort.execute(0).await?;
    let mut rows = 0usize;
    let mut last: i64 = i64::MIN;
    let mut ordered = true;
    while let Some(batch) = stream.try_next().await? {
        rows += batch.num_rows();
        let col = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("val column is Int64");
        for i in 0..col.len() {
            let v = col.value(i);
            if v < last {
                ordered = false;
            }
            last = v;
        }
    }
    let _ = std::fs::remove_dir_all(&sd);
    if rows == total_rows as usize && ordered {
        Ok(format!("rows={rows}, globally ordered"))
    } else {
        Err(query_engine::QueryError::Execution(format!(
            "WRONG RESULT: rows={rows} (expected {total_rows}), ordered={ordered}"
        )))
    }
}

async fn scenario_native_scan() -> query_engine::Result<String> {
    let table_dir = env_str("QE_HARNESS_NATIVE_TABLE", "data/tpch-10gb-native/lineitem");
    if !std::path::Path::new(&table_dir).join("_manifest.json").exists() {
        return Err(query_engine::QueryError::Execution(format!(
            "SKIP: native table dir {table_dir} not found"
        )));
    }
    let memory_limit = env_usize("QE_HARNESS_SCAN_LIMIT", 512 * 1024 * 1024);
    let config = ExecutionConfig::new().with_memory_limit(memory_limit);
    let mut ctx = ExecutionContext::with_config(config);
    ctx.register_native_table("lineitem", &table_dir)?;
    let result = ctx
        .sql("SELECT l_returnflag, COUNT(*) AS c FROM lineitem GROUP BY l_returnflag")
        .await?;
    Ok(format!("completed: {} group rows", result.row_count))
}

async fn scenario_insert() -> query_engine::Result<String> {
    let parquet = env_str("QE_HARNESS_PARQUET", "data/tpch-10gb/lineitem.parquet");
    if !std::path::Path::new(&parquet).exists() {
        return Err(query_engine::QueryError::Execution(format!(
            "SKIP: parquet source {parquet} not found"
        )));
    }
    let root = env_str("QE_HARNESS_CTAS_ROOT", ".scratch/oom001/ctas_root");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).ok();
    // Generous engine memory_limit ON PURPOSE: the gap under test is that
    // the CTAS/INSERT write path has NO formal admission check consulting
    // the limit at all (PRD gap 3) — the external cap is what bites.
    let config = ExecutionConfig::new().with_memory_limit(8 * 1024 * 1024 * 1024);
    let mut ctx = ExecutionContext::with_config(config).with_native_table_root(&root);
    ctx.register_parquet("lineitem_src", &parquet)?;
    let res = ctx
        .create_table_as_select("CREATE TABLE oom_harness_ctas AS SELECT * FROM lineitem_src")
        .await?;
    let rows = res.row_count;
    let _ = std::fs::remove_dir_all(&root);
    Ok(format!("CTAS completed: {rows} rows written"))
}

fn main() {
    // FIRST statement, mirroring src/main.rs: the rlimit lever
    // (`QE_MEM_CAP`) only works if the example applies it itself.
    query_engine::execution::enforce_process_memory_cap();

    let scenario = std::env::args().nth(1).unwrap_or_default();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let outcome: query_engine::Result<String> = rt.block_on(async {
        match scenario.as_str() {
            "agg" => scenario_agg().await,
            "sort" => scenario_sort().await,
            "native-scan" => scenario_native_scan().await,
            "insert" => scenario_insert().await,
            other => Err(query_engine::QueryError::InvalidArgument(format!(
                "usage: oom_cap_harness <agg|sort|native-scan|insert> (got {other:?})"
            ))),
        }
    });

    match outcome {
        Ok(msg) => {
            println!(
                "HARNESS RESULT: COMPLETED scenario={scenario} {msg} peak_rss_mb={:?}",
                peak_rss_mb()
            );
            std::process::exit(0);
        }
        Err(e) => {
            println!(
                "HARNESS RESULT: REFUSED scenario={scenario} error={e} peak_rss_mb={:?}",
                peak_rss_mb()
            );
            std::process::exit(2);
        }
    }
}
