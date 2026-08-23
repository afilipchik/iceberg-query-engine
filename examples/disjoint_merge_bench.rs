//! Microbenchmark for duckdb-parity-2 task 002: isolates the fused-aggregate
//! MERGE step that `disjoint_group_hint` (src/physical/planner.rs) decides
//! whether to pay, at a few range/row-count combinations bracketing SF=10's
//! measured `c_custkey` range (1,500,000) -- the floor's current value is
//! 2,000,000. Same idiom as `examples/radix_bench.rs`: hand-rolled synthetic
//! structures mirroring the production algorithm's actual cost drivers,
//! `Instant`-based timing, env-var-tunable, no dependency on the crate's
//! private internals (which the real functions are: `merge_raw_states_to_
//! batches`'s dense branch and `partition_batch_by_hash` in `morsel_agg.rs`/
//! `spillable.rs` are not `pub`, so this reimplements their algorithm rather
//! than calling them, exactly as `radix_bench.rs` reimplements
//! `VectorizedHashTable` rather than importing it).
//!
//! ## The two pipelines
//!
//! - **shared** (today's default when the range floor isn't met): every
//!   fused-agg worker pulls off ONE shared channel with no key-based
//!   routing, so a worker's ~1/W share of rows carries keys drawn
//!   effectively uniformly from the WHOLE range -- TPC-H foreign keys are a
//!   random permutation, so a worker's batches are a random sample of the
//!   join output, not a contiguous slice of it. That is what makes the
//!   post-hoc MERGE expensive: workers' partial states overlap heavily.
//!   This bench builds worker states that way, then times ONLY the merge
//!   step, reimplementing `merge_raw_states_to_batches`'s dense
//!   range-shard branch (shard by key/width, direct-address dedup per
//!   shard, parallel across shards) field-for-field, including
//!   `merge_shard_count`'s `total/4096` shard sizing.
//! - **disjoint** (what `disjoint_group_hint` turning on buys): every row
//!   is routed by HASH of its key to exactly one worker BEFORE that worker
//!   ever sees it (mirrors `spillable.rs`'s `partition_batch_by_hash`,
//!   same seeded xxhash64), so worker states never overlap and there is no
//!   merge left to do -- `finalize_disjoint_states` just flattens each
//!   state to output. The cost this mode adds instead is the per-row
//!   scatter (hash + bucket push) at ingest, which the shared path never
//!   pays. This bench does NOT model the real scatter's Arrow `take` +
//!   batch-coalescing overhead (see the write-up in
//!   `.claude/epics/duckdb-parity-2/002.md`) -- the printed
//!   `disjoint_scatter` number is a LOWER BOUND on that real cost.
//!
//! ## Calibration
//!
//! `DMB_SF100=1` adds a range=15,000,000/rows=150,000,000 point, matching
//! `disjoint_group_hint`'s own doc comment's motivating case (Q13's
//! c_custkey at SF=100). This model predicts ~126M overlapping partial
//! entries there from first principles (32 workers x range*(1-e^(-mult/32)))
//! purely from range/mult/workers -- if that prediction and the resulting
//! timing don't land close to the doc comment's already-trusted "126M
//! partial slots ... 4.3s shared vs 0.1ms disjoint", the model is not
//! calibrated and its 1.5M verdict should not be trusted either.
//!
//! Run: scripts/claude-safe-build.sh cargo run --release --example disjoint_merge_bench
//! Env: DMB_RANGES="500000,1000000,..." (comma list, overrides the primary sweep)
//!      DMB_MULT=10           (rows = mult * range; 10 = TPC-H orders:customer)
//!      DMB_FIXED_RANGE=1500000, DMB_MULT_SWEEP="3,5,10,20,40" (secondary sweep)
//!      DMB_WORKERS=32        (default: rayon::current_num_threads().clamp(2,32),
//!                             the exact expression spillable.rs uses)
//!      DMB_SF100=1           (append the 15M/150M calibration point)

use hashbrown::HashMap;
use rayon::prelude::*;
use std::hash::Hasher;
use std::time::{Duration, Instant};

/// Mirrors `morsel_agg.rs::MERGE_ENTRIES_PER_SHARD` / `merge_shard_count`.
const MERGE_ENTRIES_PER_SHARD: usize = 4096;
fn merge_shard_count(total_entries: usize, max_workers: usize) -> usize {
    let max = max_workers.clamp(2, 64);
    (total_entries / MERGE_ENTRIES_PER_SHARD).clamp(2, max)
}

/// Same xorshift64 `radix_bench.rs` uses for reproducible synthetic keys.
#[inline(always)]
fn next_xorshift(x: &mut u64) -> u64 {
    *x ^= *x << 13;
    *x ^= *x >> 7;
    *x ^= *x << 17;
    *x
}

/// Same seeded xxhash64 `spillable.rs::partition_batch_by_hash` uses to
/// route a row to a disjoint worker channel (`key.hash(&mut hasher)` there
/// is, for a single raw i64/u64 group key, equivalent to `write_u64`).
#[inline(always)]
fn route_hash(key: u64, workers: usize) -> usize {
    let mut h = xxhash_rust::xxh64::Xxh64::new(0x517c_c1b7_2722_0a95);
    h.write_u64(key);
    (h.finish() as usize) % workers
}

/// Build W worker-local `raw_groups`-equivalent maps the way the SHARED
/// channel does: each worker's rows carry keys drawn uniformly from the
/// whole range (see the module doc for why that is the honest model, not a
/// simplification of convenience).
fn build_shared_states(range: u64, rows: u64, workers: usize) -> Vec<HashMap<u64, u32>> {
    let per_worker = rows / workers as u64;
    (0..workers)
        .into_par_iter()
        .map(|w| {
            let mut x: u64 =
                0x9E3779B97F4A7C15u64.wrapping_add((w as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9));
            let mut map: HashMap<u64, u32> =
                HashMap::with_capacity((per_worker / 3).max(16) as usize);
            for _ in 0..per_worker {
                let k = next_xorshift(&mut x) % range;
                *map.entry(k).or_insert(0) += 1;
            }
            map
        })
        .collect()
}

/// The dense range-shard MERGE step: `merge_raw_states_to_batches`'s dense
/// branch, reimplemented field-for-field (shard by key/width, direct
/// address dedup per shard, in parallel). Returns (shard_time, merge_time,
/// output_row_count, checksum) -- the checksum forces the compiler to keep
/// the actual merged values, not just vector lengths.
fn merge_shared_states(
    states: Vec<HashMap<u64, u32>>,
    range: u64,
    workers: usize,
) -> (Duration, Duration, usize, u64) {
    let t0 = Instant::now();
    let total: usize = states.iter().map(|s| s.len()).sum();
    let p = merge_shard_count(total, workers);
    let w = range.div_ceil(p as u64).max(1);

    let sharded: Vec<Vec<Vec<(u64, u32)>>> = states
        .into_par_iter()
        .map(|m| {
            let mut shards: Vec<Vec<(u64, u32)>> = (0..p).map(|_| Vec::new()).collect();
            for (k, v) in m {
                let shard = ((k / w) as usize).min(p - 1);
                shards[shard].push((k, v));
            }
            shards
        })
        .collect();

    let mut shard_major: Vec<Vec<(u64, u32)>> = (0..p).map(|_| Vec::new()).collect();
    for per_state in sharded {
        for (pi, mut shard) in per_state.into_iter().enumerate() {
            shard_major[pi].append(&mut shard);
        }
    }
    let t_sharded = t0.elapsed();

    let (out_rows, checksum): (usize, u64) = shard_major
        .into_par_iter()
        .enumerate()
        .map(|(pi, entries)| {
            if entries.is_empty() {
                return (0usize, 0u64);
            }
            let lo = pi as u64 * w;
            let width = if pi == p - 1 {
                (range - lo).max(1) as usize
            } else {
                w as usize
            };
            // Direct-address merge: slot index -> dense entry position,
            // exactly mirroring the production dense branch.
            let mut slots: Vec<u32> = vec![u32::MAX; width];
            let mut out_keys: Vec<u64> = Vec::with_capacity(entries.len());
            let mut out_vals: Vec<u32> = Vec::with_capacity(entries.len());
            for (k, v) in entries {
                let idx = (k - lo) as usize;
                let slot = slots[idx];
                if slot == u32::MAX {
                    slots[idx] = out_keys.len() as u32;
                    out_keys.push(k);
                    out_vals.push(v);
                } else {
                    out_vals[slot as usize] += v;
                }
            }
            let sum: u64 = out_vals.iter().map(|&v| v as u64).sum();
            (out_keys.len(), sum)
        })
        .reduce(|| (0, 0), |a, b| (a.0 + b.0, a.1.wrapping_add(b.1)));
    (t_sharded, t0.elapsed() - t_sharded, out_rows, checksum)
}

/// The disjoint SCATTER step: routes every row to a worker by HASH of its
/// key before that worker's local map ever sees it -- the cost the shared
/// path never pays. Mirrors `partition_batch_by_hash` (hash-per-row, bucket
/// by index) across `chunks` parallel "input partitions", minus the real
/// operator's Arrow `take` + batch-coalescing calls (see module doc: this
/// is a lower bound on the real scatter cost).
fn scatter_disjoint(
    range: u64,
    rows: u64,
    workers: usize,
    chunks: usize,
) -> (Duration, Vec<Vec<u64>>) {
    let per_chunk = rows / chunks as u64;
    let t0 = Instant::now();
    let scattered: Vec<Vec<Vec<u64>>> = (0..chunks)
        .into_par_iter()
        .map(|c| {
            let mut x: u64 = 0xD1B5_4A32_D192_ED03u64
                .wrapping_add((c as u64).wrapping_mul(0x2545_F491_4F6C_DD1D));
            let mut buckets: Vec<Vec<u64>> = (0..workers)
                .map(|_| Vec::with_capacity((per_chunk as usize / workers) + 8))
                .collect();
            for _ in 0..per_chunk {
                let k = next_xorshift(&mut x) % range;
                let w = route_hash(k, workers);
                buckets[w].push(k);
            }
            buckets
        })
        .collect();
    let mut per_worker: Vec<Vec<u64>> = (0..workers).map(|_| Vec::new()).collect();
    for chunk in scattered {
        for (w, mut b) in chunk.into_iter().enumerate() {
            per_worker[w].append(&mut b);
        }
    }
    (t0.elapsed(), per_worker)
}

/// DISJOINT build+finalize: each worker turns its (already disjoint, by
/// construction of the hash route) key stream into a local map, then
/// flattens it straight to output -- no cross-worker merge exists to do,
/// exactly `finalize_disjoint_states`'s premise. Returns (build_time,
/// finalize_time, output_row_count, checksum).
fn build_and_finalize_disjoint(per_worker: Vec<Vec<u64>>) -> (Duration, Duration, usize, u64) {
    let t0 = Instant::now();
    let maps: Vec<HashMap<u64, u32>> = per_worker
        .into_par_iter()
        .map(|keys| {
            let mut map: HashMap<u64, u32> = HashMap::with_capacity((keys.len() / 3).max(16));
            for k in keys {
                *map.entry(k).or_insert(0) += 1;
            }
            map
        })
        .collect();
    let t_build = t0.elapsed();
    let (out_rows, checksum): (usize, u64) = maps
        .into_par_iter()
        .map(|m| {
            let mut out_keys: Vec<u64> = Vec::with_capacity(m.len());
            let mut out_vals: Vec<u32> = Vec::with_capacity(m.len());
            for (k, v) in m {
                out_keys.push(k);
                out_vals.push(v);
            }
            let sum: u64 = out_vals.iter().map(|&v| v as u64).sum();
            (out_keys.len(), sum)
        })
        .reduce(|| (0, 0), |a, b| (a.0 + b.0, a.1.wrapping_add(b.1)));
    (t_build, t0.elapsed() - t_build, out_rows, checksum)
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn run_point(range: u64, mult: u64, workers: usize, chunks: usize) {
    let rows = range * mult;
    let shared_states = build_shared_states(range, rows, workers);
    let total_entries: usize = shared_states.iter().map(|s| s.len()).sum();
    let t_build_shared = {
        // Re-time an equivalent build so the printed "shared build" number
        // isn't the (already elapsed, untimed) construction above -- keeps
        // methodology identical to the disjoint side's timed build.
        let t0 = Instant::now();
        let s2 = build_shared_states(range, rows, workers);
        let el = t0.elapsed();
        drop(s2);
        el
    };
    let (t_shard, t_merge, merged_rows, csum1) = merge_shared_states(shared_states, range, workers);

    let (t_scatter, per_worker) = scatter_disjoint(range, rows, workers, chunks);
    let (t_build_dis, t_finalize, dis_rows, csum2) = build_and_finalize_disjoint(per_worker);

    let shared_total = t_build_shared + t_shard + t_merge;
    let disjoint_total = t_scatter + t_build_dis + t_finalize;
    let speedup = shared_total.as_secs_f64() / disjoint_total.as_secs_f64().max(1e-9);

    println!(
        "range={:>9} mult={:>3} rows={:>11} total_entries={:>11} (dup={:.2}x) shard_count~{}",
        range,
        mult,
        rows,
        total_entries,
        total_entries as f64 / range as f64,
        merge_shard_count(total_entries, workers),
    );
    println!(
        "  shared:   build={:>8.2}ms  shard={:>8.2}ms  merge={:>8.2}ms  TOTAL={:>9.2}ms  (out_rows={merged_rows}, csum={csum1:#x})",
        ms(t_build_shared), ms(t_shard), ms(t_merge), ms(shared_total)
    );
    println!(
        "  disjoint: scatter={:>6.2}ms  build={:>8.2}ms  finalize={:>6.2}ms  TOTAL={:>9.2}ms  (out_rows={dis_rows}, csum={csum2:#x})",
        ms(t_scatter), ms(t_build_dis), ms(t_finalize), ms(disjoint_total)
    );
    println!(
        "  => merge-step-alone: {:.2}ms removed; disjoint net: {:.2}x {} shared (shared_total/disjoint_total)",
        ms(t_merge),
        speedup,
        if speedup > 1.0 { "FASTER than" } else { "SLOWER than" }
    );
    println!();
}

fn main() {
    let workers: usize = std::env::var("DMB_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| rayon::current_num_threads().clamp(2, 32));
    let chunks: usize = 8; // mimics parquet row-group-parallel input partitions

    println!(
        "workers={workers} (rayon::current_num_threads()={}, same clamp(2,32) spillable.rs uses)",
        rayon::current_num_threads()
    );
    println!();

    // --- Primary sweep: brackets SF=10's measured c_custkey range
    // (1,500,000) against the current floor (2,000,000), at TPC-H's fixed
    // orders:customer multiplicity (10:1, constant across scale factors).
    let mult: u64 = std::env::var("DMB_MULT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let ranges: Vec<u64> = match std::env::var("DMB_RANGES") {
        Ok(s) => s.split(',').filter_map(|x| x.trim().parse().ok()).collect(),
        Err(_) => vec![
            500_000, 1_000_000, 1_500_000, 2_000_000, 3_000_000, 5_000_000,
        ],
    };
    println!(
        "=== Primary sweep: range brackets 1.5M, mult={mult} fixed (TPC-H orders:customer) ==="
    );
    for &range in &ranges {
        run_point(range, mult, workers, chunks);
    }

    if std::env::var("DMB_SF100").is_ok() {
        println!("=== Calibration point: SF=100's c_custkey shape (range=15M) ===");
        println!("(cross-check vs disjoint_group_hint's doc comment: \"126M partial slots for");
        println!(" 15M real groups\", \"merge 4.3s shared vs 0.1ms disjoint\")");
        run_point(15_000_000, 10, workers, chunks);
    }

    // --- Secondary sweep: row-count sensitivity AT a fixed range, to check
    // whether a criterion based on rows/thread-count (not an absolute range
    // floor) would generalize differently than the range-only test does.
    let fixed_range: u64 = std::env::var("DMB_FIXED_RANGE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_500_000);
    let mult_sweep: Vec<u64> = match std::env::var("DMB_MULT_SWEEP") {
        Ok(s) => s.split(',').filter_map(|x| x.trim().parse().ok()).collect(),
        Err(_) => vec![3, 5, 10, 20, 40],
    };
    println!("=== Secondary sweep: range={fixed_range} fixed, mult varies ===");
    for &m in &mult_sweep {
        run_point(fixed_range, m, workers, chunks);
    }
}
