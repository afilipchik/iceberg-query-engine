//! Real, hardware-backed validation for native-tables-tiering task 002's
//! per-column failure isolation (`src/physical/gpu.rs`): a failed upload
//! must not poison the process — every OTHER column/table must remain
//! usable on the GPU afterward. Requires `--features gpu`; skips cleanly
//! (prints and returns) when `GpuEngine::get()` finds no usable CUDA
//! device, matching `tests/gpu_cache_tests.rs`'s own established
//! convention for hardware-dependent tests.
//!
//! Deliberately ONE test function, not several: everything here shares the
//! SAME process-wide `GpuEngine` singleton and, for phase 2, the SAME
//! physical GPU's real VRAM. Running two `#[tokio::test]` fns concurrently
//! in one binary (`cargo test`'s default) could have one test's real
//! VRAM-exhaustion phase spuriously fail an unrelated upload in the other.
//! Mirrors `tests/gpu_cache_tests.rs`'s own identical reasoning for the
//! identical hazard class (see that file's module doc).
//!
//! Two independent failure constructions, both named as acceptable by
//! this task's own acceptance criteria:
//!
//! - **Phase 1**: a deterministic, zero-risk-to-shared-hardware
//!   construction — an Int64 column with every value outside
//!   `load_column_f64`'s accepted `2^52` range. This path returns
//!   `Ok(None)` ("not cacheable"), NOT an `Err` — it never called
//!   `mark_unhealthy()` even before this task's fix (confirmed by reading
//!   the pre-fix code: that function's one call site was the
//!   `stream.memcpy_stod` `Err` branch, a different match arm entirely),
//!   so on its own it does not exercise the specific bug this task fixes.
//!   It IS still a genuine, real (not mocked) upload-attempt failure —
//!   cheap, fast, and fully reproducible — and independently confirms the
//!   "one column's failure -> CPU fallback -> a DIFFERENT column still
//!   uses the GPU" contract this task requires.
//! - **Phase 2**: the construction that actually exercises the poisoning
//!   bug — `mark_unhealthy()`'s ONE pre-fix call site was `Job::Upload`'s
//!   `stream.memcpy_stod` `Err` branch, i.e. a genuine CUDA/driver-level
//!   allocation failure, not a graceful pre-upload refusal. This phase
//!   grabs a second handle onto the SAME primary CUDA context the
//!   `GpuEngine` worker thread uses (`cudarc::driver::CudaContext::new`
//!   retains the device's primary context — shared, not per-caller, per
//!   cudarc's own doc comment) and allocates almost all real free VRAM
//!   directly, forcing a real `stream.memcpy_stod` failure for a victim
//!   column — then releases it and confirms a different, unrelated
//!   table's column still uploads and is usable. This briefly consumes
//!   most of the device's VRAM; the hold is kept as short as possible.

#![cfg(feature = "gpu")]

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use cudarc::driver::CudaContext;
use query_engine::physical::gpu::GpuEngine;
use query_engine::ExecutionContext;
use std::sync::Arc;
use std::time::Duration;

fn f64_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "val",
        DataType::Float64,
        false,
    )]))
}

fn make_f64_batch(schema: &Arc<Schema>, start: i64, n: i64) -> RecordBatch {
    let vals: Vec<f64> = (start..start + n).map(|i| i as f64 * 1.0001).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Float64Array::from(vals)) as _],
    )
    .unwrap()
}

fn expected_f64_sum(start: i64, n: i64) -> f64 {
    (start..start + n).map(|i| i as f64 * 1.0001).sum()
}

fn oversized_int64_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![Field::new(
        "bigval",
        DataType::Int64,
        false,
    )]))
}

/// Every value here is > 2^52 — `load_column_f64`'s own `Int64` match arm
/// refuses the WHOLE column (returns `Ok(None)`) the moment it sees the
/// first one.
fn make_oversized_int64_batch(schema: &Arc<Schema>, n: i64) -> RecordBatch {
    let vals: Vec<i64> = (0..n).map(|i| (1i64 << 52) + i + 1).collect();
    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals)) as _]).unwrap()
}

/// The SUM/COUNT output type isn't pinned by this test (either an exact
/// Int64 or a Float64 is a correct engine choice) — extract whichever it
/// actually is.
fn as_f64(arr: &ArrayRef) -> f64 {
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return a.value(0);
    }
    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return a.value(0) as f64;
    }
    panic!("unexpected aggregate output type: {:?}", arr.data_type());
}

/// Relative-tolerance float comparison, matching this codebase's own
/// documented convention for GPU/CPU float-sum comparisons (`gpu.rs`'s own
/// module doc: "the same 1e-6 tolerance class as the distributed two-phase
/// path"). A FIXED absolute delta is wrong here: this file's `victim`
/// table sums ~60,000,000 rows into a value around 1.8e15, where even a
/// numerically-correct summation reordering (the CPU aggregate's own
/// partition/merge order differs from this file's sequential reference)
/// produces absolute differences into the thousands — nowhere near wrong,
/// but far outside a `< 1.0` absolute check.
fn close_enough(got: f64, want: f64) -> bool {
    let rel_tol = 1e-6;
    (got - want).abs() <= rel_tol * want.abs().max(1.0)
}

/// Runs `sql` a few times with short pauses so an async GPU upload
/// triggered by the first (necessarily-CPU) touch has a real chance to
/// finish before the caller checks anything — mirrors
/// `tests/gpu_cache_tests.rs`'s own established `warm` helper.
async fn warm(ctx: &mut ExecutionContext, sql: &str, iters: usize) -> f64 {
    let mut last = f64::NAN;
    for _ in 0..iters {
        let r = ctx.sql(sql).await.expect("query must succeed");
        last = as_f64(r.batches[0].column(0));
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    last
}

#[tokio::test]
async fn a_genuine_upload_failure_never_poisons_other_columns() {
    let Some(engine) = GpuEngine::get() else {
        eprintln!("skip: no usable CUDA device in this environment");
        return;
    };

    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.enable_gpu_offload();

    // =========================================================================
    // Phase 1: deterministic "not cacheable" failure (Int64 out of
    // load_column_f64's accepted 2^52 range) — cheap, fast, zero risk to
    // the shared GPU. Confirms per-column isolation using a fully
    // reproducible construction, independent of phase 2's heavier one.
    // =========================================================================
    let big_schema = oversized_int64_schema();
    let n_big = 500i64;
    ctx.register_table(
        "seed_big",
        big_schema.clone(),
        vec![make_oversized_int64_batch(&big_schema, n_big)],
    );
    ctx.create_table_as_select("CREATE TABLE oversized AS SELECT * FROM seed_big")
        .await
        .expect("CTAS oversized");

    let failures_before_p1 = engine.upload_failures();
    let got_big = warm(&mut ctx, "SELECT COUNT(bigval) AS s FROM oversized", 6).await;
    assert!(
        close_enough(got_big, n_big as f64),
        "oversized (CPU fallback, exact COUNT): got {got_big}, want {n_big}"
    );
    assert!(
        engine.upload_failures() > failures_before_p1,
        "expected the oversized column's upload attempt(s) to be counted as failures"
    );

    // A DIFFERENT, unrelated, genuinely GPU-eligible table must still work.
    let f_schema = f64_schema();
    ctx.register_table(
        "seed_p1_ok",
        f_schema.clone(),
        vec![make_f64_batch(&f_schema, 0, 50_000)],
    );
    ctx.create_table_as_select("CREATE TABLE p1_ok AS SELECT * FROM seed_p1_ok")
        .await
        .expect("CTAS p1_ok");
    let want_p1_ok = expected_f64_sum(0, 50_000);
    let resident_before_p1_ok = engine.resident_column_count();
    let got_p1_ok = warm(&mut ctx, "SELECT SUM(val) AS s FROM p1_ok", 6).await;
    assert!(
        close_enough(got_p1_ok, want_p1_ok),
        "p1_ok: got {got_p1_ok}, want {want_p1_ok}"
    );
    assert!(
        engine.resident_column_count() > resident_before_p1_ok,
        "an unrelated table's column must have successfully become resident \
         right after a DIFFERENT column's upload failed -- proves phase 1 \
         did not poison the engine"
    );

    // =========================================================================
    // Phase 2: a REAL, induced VRAM-exhaustion failure -- the actual
    // failure class `mark_unhealthy()`'s sole pre-fix call site handled
    // (a genuine `stream.memcpy_stod` CUDA error, not a graceful
    // pre-upload refusal). Grabs a second handle onto the SAME primary
    // CUDA context (shared VRAM pool, not per-caller) and eats almost all
    // real free device memory directly.
    // =========================================================================
    let cuda_ctx = CudaContext::new(0).expect("open a handle onto the same primary CUDA context");
    let stream = cuda_ctx.default_stream();
    let (free_before, total) = cuda_ctx.mem_get_info().expect("mem_get_info");
    eprintln!(
        "gpu failure-isolation test: {free_before} bytes free of {total} total before eating VRAM"
    );

    let headroom: usize = 256 * 1024 * 1024; // leave ~256 MiB genuinely free
    let mut eat_bytes = free_before.saturating_sub(headroom);
    let eater = loop {
        let elems = eat_bytes / std::mem::size_of::<f64>();
        match stream.alloc_zeros::<f64>(elems) {
            Ok(buf) => break buf,
            Err(e) if eat_bytes > 512 * 1024 * 1024 => {
                eprintln!(
                    "gpu failure-isolation test: eating {eat_bytes} bytes failed ({e}); \
                     shrinking and retrying"
                );
                eat_bytes = eat_bytes * 9 / 10;
            }
            Err(e) => {
                panic!("could not allocate a large VRAM-eating buffer even after shrinking: {e}")
            }
        }
    };
    let (free_after_eat, _) = cuda_ctx.mem_get_info().expect("mem_get_info");
    eprintln!("gpu failure-isolation test: {free_after_eat} bytes free after eating VRAM");
    assert!(
        free_after_eat < 2 * 1024 * 1024 * 1024,
        "expected well under 2 GiB free after deliberately eating VRAM, got {free_after_eat}"
    );

    // Victim: needs comfortably more than what's left free.
    let n_victim = 60_000_000i64; // * 8 bytes = ~480 MB
    ctx.register_table(
        "seed_victim",
        f_schema.clone(),
        vec![make_f64_batch(&f_schema, 0, n_victim)],
    );
    ctx.create_table_as_select("CREATE TABLE victim AS SELECT * FROM seed_victim")
        .await
        .expect("CTAS victim");

    let want_victim = expected_f64_sum(0, n_victim);
    let failures_before_p2 = engine.upload_failures();
    let got_victim = warm(&mut ctx, "SELECT SUM(val) AS s FROM victim", 10).await;
    assert!(
        close_enough(got_victim, want_victim),
        "victim (CPU fallback under real VRAM exhaustion): got {got_victim}, want {want_victim}"
    );
    assert!(
        engine.upload_failures() > failures_before_p2,
        "expected the victim column's upload to have genuinely failed \
         (real VRAM exhaustion) and been counted"
    );

    // Release the eater buffer: the VRAM pressure is now gone.
    drop(eater);
    let (free_released, _) = cuda_ctx.mem_get_info().expect("mem_get_info");
    eprintln!(
        "gpu failure-isolation test: {free_released} bytes free after releasing the eater buffer"
    );

    // THE key assertion: a subsequent, unrelated, genuinely GPU-eligible
    // query against a DIFFERENT table must still successfully upload and
    // become resident -- proving the earlier real hardware failure did
    // NOT poison the process. Pre-task-002, `mark_unhealthy()` would have
    // set a process-wide flag on the victim's own failure above and BOTH
    // this table and every future one would be permanently stuck on the
    // CPU path for the rest of the process's lifetime.
    ctx.register_table(
        "seed_p2_ok",
        f_schema.clone(),
        vec![make_f64_batch(&f_schema, 999_000_000, 50_000)],
    );
    ctx.create_table_as_select("CREATE TABLE p2_ok AS SELECT * FROM seed_p2_ok")
        .await
        .expect("CTAS p2_ok");
    let want_p2_ok = expected_f64_sum(999_000_000, 50_000);
    let resident_before_p2_ok = engine.resident_column_count();
    let got_p2_ok = warm(&mut ctx, "SELECT SUM(val) AS s FROM p2_ok", 8).await;
    assert!(
        close_enough(got_p2_ok, want_p2_ok),
        "p2_ok: got {got_p2_ok}, want {want_p2_ok}"
    );
    assert!(
        engine.resident_column_count() > resident_before_p2_ok,
        "an unrelated table's column must have successfully uploaded and \
         become resident right after a REAL, HARDWARE-INDUCED VRAM \
         exhaustion failure on a DIFFERENT table -- proves the process is \
         not poisoned, the whole point of this task"
    );

    // =========================================================================
    // Phase 3 (bonus, demonstrates this task's own "retry, not permanent
    // blacklist" design decision with real evidence): now that VRAM
    // pressure is gone, the ORIGINAL victim column must eventually become
    // resident too if queried again -- a permanent per-key blacklist
    // would keep it stuck on the CPU path forever even now.
    // =========================================================================
    let resident_before_retry = engine.resident_column_count();
    let got_victim_retry = warm(&mut ctx, "SELECT SUM(val) AS s FROM victim", 10).await;
    assert!(
        close_enough(got_victim_retry, want_victim),
        "victim (retried after VRAM freed): got {got_victim_retry}, want {want_victim}"
    );
    assert!(
        engine.resident_column_count() > resident_before_retry,
        "the ORIGINAL victim column must eventually become resident once \
         retried after real VRAM pressure clears -- confirms this task's \
         retry design (not a permanent blacklist)"
    );

    println!(
        "gpu failure-isolation test final state: {}",
        engine.snapshot()
    );
}
