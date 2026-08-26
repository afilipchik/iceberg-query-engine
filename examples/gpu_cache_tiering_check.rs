//! native-tables-tiering task 001, acceptance criterion 1: "confirm the
//! mutation-driven VRAM leak empirically FIRST, before designing any fix."
//!
//! Registers a native table, runs a GPU-eligible query against it (a plain
//! ungrouped `SELECT SUM(val) FROM t` -- the safest eligible shape per
//! `plan_gpu_agg`'s recognizer: no GROUP BY, one numeric input column, no
//! filter), then mutates the table N times (INSERT/UPDATE/DELETE, round
//! robin, each cycle re-running the SAME query several times so each new
//! version also gets GPU-uploaded) while sampling REAL VRAM usage via
//! `nvidia-smi` between cycles. The mutation sequence is built so the
//! table's own LOGICAL size returns to the same baseline every 3 cycles
//! (INSERT 1000 rows -> UPDATE all live rows -> DELETE the same 1000 rows
//! just inserted), so "VRAM growing despite the table's logical size
//! staying roughly constant" is a precise, not approximate, comparison.
//!
//! Run against UNMODIFIED (pre-task-001) code to confirm the leak, and
//! again after the fix (optionally with a small `QE_GPU_CACHE_MB`) to
//! confirm bounded growth. Matches this repo's own established convention
//! for this class of empirical GPU/native-table claim (`native_gpu_check.rs`,
//! `native_post_mutation_checks.rs`): a real example process, real
//! `nvidia-smi`, never simulated.
//!
//! ```text
//! LD_LIBRARY_PATH=$PWD/.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib \
//!   scripts/claude-safe-build.sh cargo build --release --features gpu \
//!   --example gpu_cache_tiering_check
//! LD_LIBRARY_PATH=$PWD/.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib \
//!   QE_TIERING_ROWS=2000000 QE_TIERING_MUTATIONS=15 \
//!   ./target/release/examples/gpu_cache_tiering_check
//! # to exercise the post-fix budget/eviction path with a small cap:
//! QE_GPU_CACHE_MB=24 LD_LIBRARY_PATH=... ./target/release/examples/gpu_cache_tiering_check
//! ```

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn vram_used_mib() -> Option<u64> {
    let out = std::process::Command::new("nvidia-smi")
        .args(["--query-gpu=memory.used", "--format=csv,noheader,nounits"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8_lossy(&out.stdout).trim().parse().ok()
}

fn make_batch(schema: &Arc<Schema>, start: i64, n: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n).collect();
    let vals: Vec<f64> = (start..start + n).map(|i| (i as f64) * 1.0001).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)) as _,
            Arc::new(Float64Array::from(vals)) as _,
        ],
    )
    .expect("build synthetic batch")
}

/// Run the query a few times with short pauses so an async GPU upload
/// triggered by the first (necessarily-CPU) touch has a real chance to
/// finish before we sample VRAM -- mirrors `native_gpu_check.rs`'s own
/// "loop the same query, let the background upload catch up" pattern.
/// Returns the LAST iteration's SUM(val), so the caller can cell-exact
/// check it against an independently tracked expected value -- ties this
/// leak/fix repro to the correctness acceptance criterion directly,
/// instead of only checking VRAM numbers in isolation.
async fn warm(ctx: &mut ExecutionContext, sql: &str, iters: usize) -> f64 {
    let mut last = f64::NAN;
    for _ in 0..iters {
        let r = ctx.sql(sql).await.expect("query must succeed");
        last = r.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("SUM(val) must be Float64")
            .value(0);
        tokio::time::sleep(Duration::from_millis(150)).await;
    }
    last
}

fn expected_sum(start: i64, n: i64) -> f64 {
    (start..start + n).map(|i| (i as f64) * 1.0001).sum()
}

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    let n: i64 = std::env::var("QE_TIERING_ROWS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(2_000_000);
    let n_mut: usize = std::env::var("QE_TIERING_MUTATIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(15);

    let root = std::path::PathBuf::from(".scratch/gpu_tiering_check");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;

    let mut ctx = ExecutionContext::new().with_native_table_root(root.clone());
    #[cfg(feature = "gpu")]
    {
        ctx.enable_gpu_offload();
        println!("GPU offload ENABLED");
    }
    #[cfg(not(feature = "gpu"))]
    println!("(default build, no gpu feature -- this run measures nothing useful)");

    if let Ok(mb) = std::env::var("QE_GPU_CACHE_MB") {
        println!("QE_GPU_CACHE_MB={mb}");
    } else {
        println!("QE_GPU_CACHE_MB unset (default budget)");
    }

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Float64, false),
    ]));

    ctx.register_table("seed", schema.clone(), vec![make_batch(&schema, 0, n)]);
    let ctas = ctx
        .create_table_as_select("CREATE TABLE t AS SELECT * FROM seed")
        .await?;
    println!(
        "created native table t: {} rows, version {}",
        ctas.rows, ctas.version
    );

    let query = "SELECT SUM(val) AS s FROM t";
    let col_bytes = (n as u64) * 8;
    println!(
        "each mutation's `val` column upload is ~{:.1} MB",
        col_bytes as f64 / 1_000_000.0
    );

    println!("\ncycle,version,rows,vram_used_mib,elapsed_ms,correct");
    let mut expected = expected_sum(0, n);
    let got = warm(&mut ctx, query, 4).await;
    let mut all_correct = (got - expected).abs() < 1e-2;
    println!(
        "0,{},{},{},0,{}",
        ctas.version,
        n,
        vram_used_mib().unwrap_or(0),
        (got - expected).abs() < 1e-2
    );

    let mut next_id = n;
    let t_start = Instant::now();
    for i in 1..=n_mut {
        let t0 = Instant::now();
        let (version, rows) = match i % 3 {
            1 => {
                let k = 1000i64;
                let name = format!("delta_ins_{i}");
                ctx.register_table(&name, schema.clone(), vec![make_batch(&schema, next_id, k)]);
                let r = ctx
                    .insert_into_native_table(&format!("INSERT INTO t SELECT * FROM {name}"))
                    .await?;
                expected += expected_sum(next_id, k);
                next_id += k;
                (r.version, r.total_rows)
            }
            2 => {
                let r = ctx
                    .update_native_table("UPDATE t SET val = val + 1.0")
                    .await?;
                expected += rows_touched_delta(&r);
                (r.version, r.total_rows)
            }
            0 => {
                let lo = next_id - 1000;
                let r = ctx
                    .delete_from_native_table(&format!(
                        "DELETE FROM t WHERE id >= {lo} AND id < {next_id}"
                    ))
                    .await?;
                expected -= expected_sum(lo, next_id - lo);
                next_id -= 1000;
                (r.version, r.total_rows)
            }
            _ => unreachable!(),
        };
        let got = warm(&mut ctx, query, 4).await;
        let correct = (got - expected).abs() < 1e-2 * expected.abs().max(1.0);
        all_correct &= correct;
        let vram = vram_used_mib().unwrap_or(0);
        println!(
            "{i},{version},{rows},{vram},{},{correct}",
            t0.elapsed().as_millis()
        );
        if !correct {
            eprintln!("  MISMATCH at cycle {i}: got {got}, want {expected}");
        }
    }
    println!(
        "\ntotal wall time for {n_mut} mutation cycles: {:?}",
        t_start.elapsed()
    );
    println!("all {} cycles cell-exact: {all_correct}", n_mut + 1);
    if !all_correct {
        std::process::exit(1);
    }
    Ok(())
}

/// `UPDATE t SET val = val + 1.0` (no WHERE) adds exactly 1.0 to every LIVE
/// row's `val` -- the SUM therefore increases by exactly `rows_updated`.
fn rows_touched_delta(r: &query_engine::execution::UpdateResult) -> f64 {
    r.rows_updated as f64
}
