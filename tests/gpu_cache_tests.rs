//! Real, hardware-backed validation for native-tables-tiering task 001's
//! VRAM budget + LRU eviction mechanism (`src/physical/gpu.rs`). Requires
//! `--features gpu`; skips cleanly (prints and returns) when
//! `GpuEngine::get()` finds no usable CUDA device, matching this repo's
//! established convention for hardware-dependent tests (e.g. the vector
//! search suite skipping without its fixture).
//!
//! Deliberately ONE test function, not several: `QE_GPU_CACHE_MB` is a
//! PROCESS-GLOBAL env var, re-read fresh on every eviction check rather
//! than cached (see `cache_budget_bytes`'s own doc) specifically so a test
//! CAN set it — but `cargo test` still runs every `#[test]` in a file from
//! the same process, by default in parallel threads (see
//! `execution::context::parse_merge_concurrency`'s own doc for this exact,
//! already-documented hazard elsewhere in this codebase). Keeping
//! everything sequential inside one test avoids the race entirely without
//! `--test-threads=1` or a serialize-test dependency.

#![cfg(feature = "gpu")]

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use query_engine::physical::gpu::GpuEngine;
use query_engine::ExecutionContext;
use std::sync::Arc;
use std::time::Duration;

fn make_batch(schema: &Arc<Schema>, start: i64, n: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n).collect();
    let vals: Vec<f64> = (start..start + n).map(|i| i as f64 * 1.0001).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)) as _,
            Arc::new(Float64Array::from(vals)) as _,
        ],
    )
    .unwrap()
}

fn expected_sum(start: i64, n: i64) -> f64 {
    (start..start + n).map(|i| i as f64 * 1.0001).sum()
}

/// Runs `sql` a few times with short pauses so an async GPU upload
/// triggered by the first (necessarily-CPU) touch has a real chance to
/// finish before the caller checks anything — mirrors
/// `examples/native_gpu_check.rs`'s own established pattern. Returns the
/// LAST result's single f64 output value.
async fn warm(ctx: &mut ExecutionContext, sql: &str, iters: usize) -> f64 {
    let mut last = f64::NAN;
    for _ in 0..iters {
        let r = ctx.sql(sql).await.expect("query must succeed");
        let arr = r.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("SUM(val) column must be Float64");
        last = arr.value(0);
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    last
}

#[tokio::test]
async fn vram_budget_and_lru_eviction_are_real_and_correct() {
    // Very small budget: forces eviction almost immediately given the
    // column sizes this test uploads (~2.3 MB each). Safe to set here —
    // see the module doc above for why this must stay the file's only test.
    std::env::set_var("QE_GPU_CACHE_MB", "8");

    let Some(engine) = GpuEngine::get() else {
        eprintln!("skip: no usable CUDA device in this environment");
        return;
    };

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Float64, false),
    ]));
    let n = 300_000i64; // 300_000 * 8 bytes ~= 2.29 MiB per `val` column
    let column_bytes = (n as usize) * 8;

    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.enable_gpu_offload();

    // --- Get table `a`'s `val` column resident and confirm correctness. ---
    ctx.register_table("seed_a", schema.clone(), vec![make_batch(&schema, 0, n)]);
    ctx.create_table_as_select("CREATE TABLE a AS SELECT * FROM seed_a")
        .await
        .expect("CTAS a");
    let want_a = expected_sum(0, n);
    let got_a = warm(&mut ctx, "SELECT SUM(val) AS s FROM a", 6).await;
    assert!(
        (got_a - want_a).abs() < 1e-2,
        "table a: got {got_a}, want {want_a}"
    );
    assert!(
        engine.resident_column_count() >= 1,
        "expected at least a's val column resident after warming"
    );

    // --- Byte accounting is real (task 001's own first mechanism): the
    // mirror must reflect at least one real column's worth of bytes, not a
    // row/group count. ---
    assert!(
        engine.resident_bytes() >= column_bytes,
        "resident_bytes() ({}) should be at least one column's real byte size ({column_bytes})",
        engine.resident_bytes()
    );

    // --- Push enough OTHER tables through to exceed the 8 MiB budget and
    // force LRU to evict `a`'s now-cold column. ---
    for i in 0..6i64 {
        let seed = format!("seed_b{i}");
        let tbl = format!("b{i}");
        let base = (i + 1) * 10_000_000;
        ctx.register_table(&seed, schema.clone(), vec![make_batch(&schema, base, n)]);
        ctx.create_table_as_select(&format!("CREATE TABLE {tbl} AS SELECT * FROM {seed}"))
            .await
            .unwrap();
        let want = expected_sum(base, n);
        let got = warm(&mut ctx, &format!("SELECT SUM(val) AS s FROM {tbl}"), 4).await;
        assert!(
            (got - want).abs() < 1e-2,
            "table {tbl}: got {got}, want {want}"
        );
    }

    // 7 columns x ~2.29 MiB ~= 16 MiB resident if NOTHING were ever evicted
    // under an 8 MiB budget -- assert we are well under that, and that at
    // least one real eviction happened.
    assert!(
        engine.eviction_count() > 0,
        "expected at least one eviction after exceeding the 8MB budget with 7 x ~2.3MB columns"
    );
    assert!(
        engine.resident_bytes() < 5 * column_bytes,
        "resident_bytes() ({}) should stay well under the unbounded-accumulation size (~{})",
        engine.resident_bytes(),
        7 * column_bytes
    );

    // --- Correctness after eviction (the other hard acceptance criterion):
    // re-query table `a`. Its column was very likely evicted by now (LRU
    // picked it as coldest) -- confirm the SAME cell-exact answer, whether
    // served by a transparent CPU fallback or a completed re-upload. This
    // is not a new code path: eviction just returns a column to the exact
    // "not yet resident" state a never-uploaded column already handled. ---
    let got_a_again = warm(&mut ctx, "SELECT SUM(val) AS s FROM a", 6).await;
    assert!(
        (got_a_again - want_a).abs() < 1e-2,
        "table a after eviction: got {got_a_again}, want {want_a}"
    );

    // --- The mutation-driven leak, fixed: mutate `a` repeatedly. Each
    // mutation is a new provider identity (`table_id ++ version`) -> a new
    // cache pid -> the OLD version's columns become unreachable and, being
    // the coldest entries once anything else touches the budget, are
    // evicted by the exact same generic LRU policy above. Resident VRAM
    // must stay bounded, not grow with the mutation count. ---
    let mut expected_a = want_a;
    let mut next_id = n;
    let evictions_before_mutations = engine.eviction_count();
    for _ in 0..8 {
        let delta_name = format!("delta_{next_id}");
        let delta_n = 1000i64;
        ctx.register_table(
            &delta_name,
            schema.clone(),
            vec![make_batch(&schema, next_id, delta_n)],
        );
        ctx.insert_into_native_table(&format!("INSERT INTO a SELECT * FROM {delta_name}"))
            .await
            .expect("insert");
        expected_a += expected_sum(next_id, delta_n);
        next_id += delta_n;

        let got = warm(&mut ctx, "SELECT SUM(val) AS s FROM a", 3).await;
        assert!(
            (got - expected_a).abs() < 1e-2,
            "table a after mutation (next_id={next_id}): got {got}, want {expected_a}"
        );
    }

    assert!(
        engine.resident_bytes() < 5 * column_bytes,
        "resident bytes after 8 mutations ({}) should stay bounded near the budget, \
         not grow with mutation count",
        engine.resident_bytes()
    );
    assert!(
        engine.eviction_count() > evictions_before_mutations,
        "mutations against a GPU-queried native table should have produced further \
         evictions of superseded versions' columns"
    );

    println!(
        "gpu cache after full test: resident_bytes={} eviction_count={} resident_columns={}",
        engine.resident_bytes(),
        engine.eviction_count(),
        engine.resident_column_count()
    );
}
