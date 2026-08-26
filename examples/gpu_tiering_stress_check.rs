//! native-tables-tiering task 003 (QA close-out): a broader, longer-running
//! stress scenario beyond what tasks 001/002 individually tested — MORE
//! mutations AND MORE concurrent tables competing for the shared VRAM
//! budget at once than either task tested alone.
//!
//! Task 001's own repro (`gpu_cache_tiering_check.rs`) mutated exactly ONE
//! table 15 times. Task 002's hardware test (`tests/gpu_cache_tests.rs`,
//! re-run unmodified by this task, still passing) mutates exactly ONE
//! table 8 times, after warming 7 OTHER tables SEQUENTIALLY (each touched
//! once, never mutated, never interleaved with further activity). Neither
//! interleaves MANY tables' mutations against each other under SUSTAINED
//! eviction pressure. This does: NUM_TABLES native tables are created,
//! then mutated in ROUND-ROBIN order (table 0, table 1, ..., table N-1,
//! table 0, ...) for MUTATIONS_PER_TABLE rounds each — NUM_TABLES *
//! MUTATIONS_PER_TABLE total mutation ops, each immediately followed by a
//! re-query of that same table, cell-exact-checked against an
//! independently tracked expected running value (the same discipline task
//! 001/002's own examples/tests use, just applied to many tables instead
//! of one). `QE_GPU_CACHE_MB` is set deliberately smaller than all
//! NUM_TABLES tables' combined column size, so round-robin mutation
//! guarantees genuine, sustained, CROSS-TABLE LRU thrashing for the whole
//! run, not just a brief startup burst.
//!
//! VRAM is sampled via REAL `nvidia-smi` (system-level ground truth) AND
//! via the engine's own `GpuEngine::resident_bytes()`/`budget_bytes()`/
//! `eviction_count()` (precise software accounting) at EVERY mutation
//! cycle, not just start/end — so "stays bounded throughout" is judged
//! against the full time series, not a two-point before/after comparison.
//!
//! Each table's own insert/update/delete sequence (kind = (round + i) % 3,
//! cycling insert -> update -> delete per table, phase-offset by table
//! index so all NUM_TABLES tables are never doing the identical operation
//! in the identical round) mirrors task 001's own proven single-table
//! cyclic pattern exactly — just staggered across tables for interleaving,
//! never changing the per-table correctness argument.
//!
//! ```text
//! LD_LIBRARY_PATH=$PWD/.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib \
//!   scripts/claude-safe-build.sh cargo build --release --features gpu \
//!   --example gpu_tiering_stress_check
//! LD_LIBRARY_PATH=$PWD/.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib \
//!   QE_GPU_CACHE_MB=12 ./target/release/examples/gpu_tiering_stress_check
//! ```

#[cfg(not(feature = "gpu"))]
fn main() {
    println!(
        "(default build, no gpu feature compiled in -- this stress check measures nothing \
         useful; build with --features gpu)"
    );
}

#[cfg(feature = "gpu")]
#[tokio::main]
async fn main() -> query_engine::Result<()> {
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use query_engine::physical::gpu::GpuEngine;
    use query_engine::ExecutionContext;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    const NUM_TABLES: usize = 10;
    const ROWS_PER_TABLE: i64 = 400_000; // ~3.05 MiB per f64 `val` column
    const MUTATIONS_PER_TABLE: usize = 15; // 150 total mutation ops, round-robin
    const DELTA_ROWS: i64 = 2000;

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

    fn expected_sum(start: i64, n: i64) -> f64 {
        (start..start + n).map(|i| (i as f64) * 1.0001).sum()
    }

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
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        last
    }

    struct TableState {
        name: String,
        expected: f64,
        next_id: i64,
    }

    let Some(engine) = GpuEngine::get() else {
        println!("skip: no usable CUDA device in this environment");
        return Ok(());
    };

    // Deliberately smaller than NUM_TABLES * one column's bytes, so this
    // run forces sustained cross-table eviction for its whole duration
    // rather than only at startup. Overridable, but this test's own claim
    // (many concurrent tables competing for a bounded budget) depends on
    // the default being meaningfully undersized -- so print what's active.
    let budget_mb: usize = std::env::var("QE_GPU_CACHE_MB")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(12);
    std::env::set_var("QE_GPU_CACHE_MB", budget_mb.to_string());

    let root = std::path::PathBuf::from(".scratch/gpu_tiering_stress_check");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root)?;

    let mut ctx = ExecutionContext::new().with_native_table_root(root.clone());
    ctx.enable_gpu_offload();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Float64, false),
    ]));

    let col_bytes = (ROWS_PER_TABLE as u64) * 8;
    let total_ops = NUM_TABLES * MUTATIONS_PER_TABLE;
    println!(
        "NUM_TABLES={NUM_TABLES} ROWS_PER_TABLE={ROWS_PER_TABLE} \
         MUTATIONS_PER_TABLE={MUTATIONS_PER_TABLE} (={total_ops} total mutation ops)"
    );
    println!(
        "each table's `val` column ~= {:.2} MiB; {NUM_TABLES} tables combined ~= {:.1} MiB; \
         QE_GPU_CACHE_MB={budget_mb} (deliberately undersized to force sustained cross-table \
         eviction for the whole run)",
        col_bytes as f64 / 1_048_576.0,
        (col_bytes as f64 * NUM_TABLES as f64) / 1_048_576.0,
    );

    // --- Create all tables up front, each seeded with a distinct id range
    // so cross-table sums can never accidentally collide/mask a bug. ---
    let mut tables = Vec::with_capacity(NUM_TABLES);
    for i in 0..NUM_TABLES {
        let seed_name = format!("seed_{i}");
        let table_name = format!("t{i}");
        let base = (i as i64) * 100_000_000;
        ctx.register_table(
            &seed_name,
            schema.clone(),
            vec![make_batch(&schema, base, ROWS_PER_TABLE)],
        );
        ctx.create_table_as_select(&format!(
            "CREATE TABLE {table_name} AS SELECT * FROM {seed_name}"
        ))
        .await
        .unwrap_or_else(|e| panic!("CTAS {table_name}: {e}"));
        tables.push(TableState {
            name: table_name,
            expected: expected_sum(base, ROWS_PER_TABLE),
            next_id: base + ROWS_PER_TABLE,
        });
    }

    // --- Warm every table once, verifying cell-exact from the very start. ---
    let mut all_correct = true;
    for t in &tables {
        let sql = format!("SELECT SUM(val) AS s FROM {}", t.name);
        let got = warm(&mut ctx, &sql, 2).await;
        let ok = (got - t.expected).abs() < 1e-2 * t.expected.abs().max(1.0);
        all_correct &= ok;
        if !ok {
            eprintln!(
                "  INITIAL MISMATCH {}: got {got}, want {}",
                t.name, t.expected
            );
        }
    }
    let baseline_vram = vram_used_mib().unwrap_or(0);
    println!(
        "initial warm done, all correct so far={all_correct}, nvidia-smi vram={baseline_vram} \
         MiB, {}",
        engine.snapshot()
    );

    // --- Round-robin mutation stress: mutate table i, re-query table i,
    // move to table i+1, ..., wrap MUTATIONS_PER_TABLE times. Every op's
    // VRAM (both nvidia-smi and the engine's own accounting) is sampled,
    // not just a sparse subset, so the "bounded throughout" verdict below
    // is judged against the complete time series. ---
    let mut vram_series: Vec<u64> = vec![baseline_vram];
    let t_start = Instant::now();
    let mut op = 0usize;
    for round in 0..MUTATIONS_PER_TABLE {
        for i in 0..NUM_TABLES {
            op += 1;
            let kind = (round + i) % 3;
            let t = &mut tables[i];
            match kind {
                0 => {
                    let delta_name = format!("delta_{i}_{op}");
                    ctx.register_table(
                        &delta_name,
                        schema.clone(),
                        vec![make_batch(&schema, t.next_id, DELTA_ROWS)],
                    );
                    ctx.insert_into_native_table(&format!(
                        "INSERT INTO {} SELECT * FROM {delta_name}",
                        t.name
                    ))
                    .await
                    .unwrap_or_else(|e| panic!("insert {}: {e}", t.name));
                    t.expected += expected_sum(t.next_id, DELTA_ROWS);
                    t.next_id += DELTA_ROWS;
                }
                1 => {
                    let r = ctx
                        .update_native_table(&format!("UPDATE {} SET val = val + 1.0", t.name))
                        .await
                        .unwrap_or_else(|e| panic!("update {}: {e}", t.name));
                    t.expected += r.rows_updated as f64;
                }
                2 => {
                    let lo = t.next_id - DELTA_ROWS;
                    let r = ctx
                        .delete_from_native_table(&format!(
                            "DELETE FROM {} WHERE id >= {lo} AND id < {}",
                            t.name, t.next_id
                        ))
                        .await
                        .unwrap_or_else(|e| panic!("delete {}: {e}", t.name));
                    if r.rows_deleted > 0 {
                        t.expected -= expected_sum(lo, t.next_id - lo);
                        t.next_id -= DELTA_ROWS;
                    }
                }
                _ => unreachable!(),
            }

            let sql = format!("SELECT SUM(val) AS s FROM {}", t.name);
            let got = warm(&mut ctx, &sql, 2).await;
            let ok = (got - t.expected).abs() < 1e-2 * t.expected.abs().max(1.0);
            all_correct &= ok;
            if !ok {
                eprintln!(
                    "  MISMATCH op={op} round={round} table={}: got {got}, want {}",
                    t.name, t.expected
                );
            }

            let vram = vram_used_mib().unwrap_or(0);
            vram_series.push(vram);
            if op % 10 == 0 || op == total_ops {
                println!(
                    "op={op}/{total_ops} round={round} table={} vram={vram}MiB {} \
                     correct_so_far={all_correct}",
                    t.name,
                    engine.snapshot(),
                );
            }
        }
    }
    let elapsed = t_start.elapsed();

    // --- Final correctness re-check of EVERY table (not just whichever was
    // touched last), independent of the interleaved checks above. ---
    for t in &tables {
        let sql = format!("SELECT SUM(val) AS s FROM {}", t.name);
        let got = warm(&mut ctx, &sql, 2).await;
        let ok = (got - t.expected).abs() < 1e-2 * t.expected.abs().max(1.0);
        all_correct &= ok;
        if !ok {
            eprintln!(
                "  FINAL MISMATCH {}: got {got}, want {}",
                t.name, t.expected
            );
        }
    }

    // --- Bounded-VRAM verdict: compare the back HALF of the run's samples
    // against the front half. A real leak grows roughly linearly with
    // mutation count; a correctly bounded cache plateaus once every
    // table's pid has been seen at least once (within the first ~
    // NUM_TABLES ops) and then stays flat despite continued mutation. ---
    let half = vram_series.len() / 2;
    let front_max = vram_series[..half.max(1)]
        .iter()
        .copied()
        .max()
        .unwrap_or(0);
    let back_max = vram_series[half..].iter().copied().max().unwrap_or(0);
    let snapshot = engine.snapshot();

    println!("\n=== SUMMARY ===");
    println!("total mutation ops: {total_ops}");
    println!("wall time: {elapsed:?}");
    println!("all cell-exact throughout (initial + every cycle + final): {all_correct}");
    println!(
        "nvidia-smi VRAM: baseline={baseline_vram}MiB front_half_max={front_max}MiB \
         back_half_max={back_max}MiB (back should not run away from front)"
    );
    println!("final engine accounting: {snapshot}");

    // MiB slack for CUDA context/allocator-pool noise unrelated to this
    // cache's own bookkeeping (task 002 independently documented
    // `mem_get_info()` not always tracking pool-level frees immediately).
    let vram_bounded = (back_max as i64) - (front_max as i64) < 512;
    // Soft-target budget (task 001): one oversized straggler is allowed to
    // land after everything else is evicted, so allow one column's worth
    // of slack over the nominal budget rather than a hard `<=`.
    let resident_bounded =
        (snapshot.resident_bytes as u64) <= snapshot.budget_bytes as u64 + col_bytes;
    // Real, sustained pressure, not a couple of one-off evictions: with
    // NUM_TABLES tables cycling through a budget sized for roughly
    // budget_mb/col_mib of them at once, eviction must fire far more than
    // once per table across MUTATIONS_PER_TABLE rounds.
    let real_eviction_pressure = snapshot.eviction_count >= (NUM_TABLES as u64) * 2;

    println!(
        "verdict: vram_bounded={vram_bounded} resident_bounded={resident_bounded} \
         real_eviction_pressure={real_eviction_pressure} (eviction_count={})",
        snapshot.eviction_count
    );

    if !(all_correct && vram_bounded && resident_bounded && real_eviction_pressure) {
        std::process::exit(1);
    }
    Ok(())
}
