//! Stage 0 kill-switch microbenchmark for duckdb-parity-2 task 006 ("dense
//! group-id remapping"). Compares TODAY's raw-single-column HashMap
//! fallback tier in `morsel_agg.rs` (`raw_groups: HashMap<u64,
//! Vec<AccumulatorState>>` -- one heap-boxed `Vec<enum>` allocated per
//! group) against the proposed replacement: a `HashMap<u64,u32>` dense-id
//! table + flat, columnar `FlatAgg` storage (`Vec<T>` indexed by dense group
//! id, no per-group heap allocation). Same idiom as `examples/radix_bench.
//! rs` / `examples/gpu_price_bench.rs` / `examples/disjoint_merge_bench.rs`:
//! hand-rolled structures mirroring the production cost drivers, `Instant`
//! timing, env-var-tunable, no dependency on private crate internals (both
//! `AccumulatorState` and `FlatAgg` here are reimplementations, not the
//! real private types in `morsel_agg.rs`).
//!
//! ## What this prices
//!
//! Three phases, timed separately, matching the tiers the real code walks
//! for the raw single-column path: per-worker **build** (morsel-parallel
//! ingest: entry-or-insert on a hot loop of synthetic rows), **merge**
//! (thread-local states reduced to one final state -- this is what
//! `merge_raw_states_to_batches`/`merge_entries_into_map` do today), and
//! **finalize** (materializing the final (key, value...) columns, what
//! `AggregationState::build_output`/`build_output_raw` do today). Both
//! representations are seeded identically and updated in lock-step, so
//! their finalized checksums must match exactly (`assert_eq!` below) --
//! this is a correctness self-check on the benchmark's own faithfulness,
//! not a substitute for the real cell-exact test suite.
//!
//! ## Dense vs sparse domains
//!
//! Group COUNT is controlled directly (`raw_id = xorshift() % groups`, a
//! bijection-free draw that yields close to `groups` distinct values for
//! the `mult` used here). The domain shape sweep asks a narrower question:
//! does spreading those same `groups` distinct values across a much wider
//! numeric range (`scatter()`, an odd-multiplier bijection on u64 -- same
//! distinct-key count, different numeric locality) change either
//! representation's cost? Neither `HashMap<u64, Vec<AccumulatorState>>` nor
//! `HashMap<u64, u32>` should care, since hashing (unlike the production
//! dense-direct-address merge branch, which this bench does NOT model)
//! ignores numeric locality -- a negative/flat result here is expected and
//! is itself useful signal for task 006's write-up.
//!
//! ## Gate
//!
//! Stage 1 (a `FlatAgg` rewrite of morsel_agg.rs's `raw_groups` tier)
//! proceeds only if this bench shows >= 15-20% total wall-time win AND a
//! concrete target query is independently confirmed to still need it (see
//! duckdb-parity-2 task 006's write-up for that confirmation).
//!
//! Run: scripts/claude-safe-build.sh cargo run --release --example dense_group_id_bench
//! Env: DGB_GROUPS="1000000,5000000,..." (comma list, overrides the primary sweep)
//!      DGB_MULT=3            (rows = mult * groups per point)
//!      DGB_WORKERS=32        (default: rayon::current_num_threads().clamp(2,32))
//!      DGB_AGGS_SWEEP="1,2,3" (secondary sweep: aggregate count, at DGB_AGGS_GROUPS)
//!      DGB_AGGS_GROUPS=10000000

use hashbrown::HashMap;
use rayon::prelude::*;
use std::time::{Duration, Instant};

/// Same xorshift64 `radix_bench.rs`/`disjoint_merge_bench.rs` use for
/// reproducible synthetic keys/values.
#[inline(always)]
fn next_xorshift(x: &mut u64) -> u64 {
    *x ^= *x << 13;
    *x ^= *x >> 7;
    *x ^= *x << 17;
    *x
}

/// Odd-multiplier bijection on u64: preserves the exact distinct-key count
/// while spreading values across a much wider numeric range. See the module
/// doc's "Dense vs sparse domains" note for why this is the right way to
/// hold group count fixed while varying domain shape.
#[inline(always)]
fn scatter(v: u64) -> u64 {
    v.wrapping_mul(0x9E3779B97F4A7C15)
}

#[derive(Clone, Copy, Debug)]
enum AggKind {
    Sum,
    Count,
    Avg,
}

/// Mirrors `morsel_agg.rs::AccumulatorState` closely enough to price the
/// boxed per-group `Vec<enum>` representation honestly: one heap allocation
/// per NEW group (the `Vec` itself), enum-dispatch per update. Sum/Count/Avg
/// cover the common shapes; the other production variants (Min/Max/BoolAnd/
/// ...) don't change the allocation-pattern story this bench is pricing.
#[derive(Clone)]
enum AccumulatorState {
    Sum(f64),
    Count(i64),
    Avg(f64, i64),
}

impl AccumulatorState {
    #[inline(always)]
    fn new(kind: AggKind) -> Self {
        match kind {
            AggKind::Sum => AccumulatorState::Sum(0.0),
            AggKind::Count => AccumulatorState::Count(0),
            AggKind::Avg => AccumulatorState::Avg(0.0, 0),
        }
    }
    #[inline(always)]
    fn update(&mut self, v: f64) {
        match self {
            AccumulatorState::Sum(s) => *s += v,
            AccumulatorState::Count(c) => *c += 1,
            AccumulatorState::Avg(s, c) => {
                *s += v;
                *c += 1;
            }
        }
    }
    #[inline(always)]
    fn merge(&mut self, other: &AccumulatorState) {
        match (self, other) {
            (AccumulatorState::Sum(a), AccumulatorState::Sum(b)) => *a += b,
            (AccumulatorState::Count(a), AccumulatorState::Count(b)) => *a += b,
            (AccumulatorState::Avg(sa, ca), AccumulatorState::Avg(sb, cb)) => {
                *sa += sb;
                *ca += cb;
            }
            _ => unreachable!("mismatched accumulator kinds"),
        }
    }
    #[inline(always)]
    fn finalize(&self) -> f64 {
        match self {
            AccumulatorState::Sum(s) => *s,
            AccumulatorState::Count(c) => *c as f64,
            AccumulatorState::Avg(s, c) => {
                if *c > 0 {
                    s / *c as f64
                } else {
                    0.0
                }
            }
        }
    }
}

/// FlatAgg: CLOSED enum (this codebase's established preference over `Box
/// <dyn GroupsAccumulator>` -- the `AggregateFunction` set is closed and
/// compile-time-known, per the Expression Compilation epic's own findings),
/// one flat `Vec<T>` PER AGGREGATE indexed by dense group id. No per-group
/// heap allocation: growing a group's slot is a `Vec::push` on an existing,
/// amortized-growth buffer shared by every group, not a fresh allocation.
enum FlatAgg {
    Sum(Vec<f64>),
    Count(Vec<i64>),
    Avg(Vec<f64>, Vec<i64>),
}

impl FlatAgg {
    fn new(kind: AggKind, cap: usize) -> Self {
        match kind {
            AggKind::Sum => FlatAgg::Sum(Vec::with_capacity(cap)),
            AggKind::Count => FlatAgg::Count(Vec::with_capacity(cap)),
            AggKind::Avg => FlatAgg::Avg(Vec::with_capacity(cap), Vec::with_capacity(cap)),
        }
    }
    #[inline(always)]
    fn push_zero(&mut self) {
        match self {
            FlatAgg::Sum(v) => v.push(0.0),
            FlatAgg::Count(v) => v.push(0),
            FlatAgg::Avg(s, c) => {
                s.push(0.0);
                c.push(0);
            }
        }
    }
    #[inline(always)]
    fn update(&mut self, idx: usize, v: f64) {
        match self {
            FlatAgg::Sum(vec) => vec[idx] += v,
            FlatAgg::Count(vec) => vec[idx] += 1,
            FlatAgg::Avg(s, c) => {
                s[idx] += v;
                c[idx] += 1;
            }
        }
    }
    #[inline(always)]
    fn merge_from(&mut self, idx: usize, other: &FlatAgg, oidx: usize) {
        match (self, other) {
            (FlatAgg::Sum(a), FlatAgg::Sum(b)) => a[idx] += b[oidx],
            (FlatAgg::Count(a), FlatAgg::Count(b)) => a[idx] += b[oidx],
            (FlatAgg::Avg(sa, ca), FlatAgg::Avg(sb, cb)) => {
                sa[idx] += sb[oidx];
                ca[idx] += cb[oidx];
            }
            _ => unreachable!("mismatched FlatAgg kinds"),
        }
    }
    #[inline(always)]
    fn finalize(&self, idx: usize) -> f64 {
        match self {
            FlatAgg::Sum(v) => v[idx],
            FlatAgg::Count(v) => v[idx] as f64,
            FlatAgg::Avg(s, c) => {
                if c[idx] > 0 {
                    s[idx] / c[idx] as f64
                } else {
                    0.0
                }
            }
        }
    }
}

// ---------------------------------------------------------------- BOXED ---

fn build_boxed(
    seed: u64,
    rows: u64,
    groups: u64,
    sparse: bool,
    aggs: &[AggKind],
) -> HashMap<u64, Vec<AccumulatorState>> {
    let mut x = seed;
    let mut map: HashMap<u64, Vec<AccumulatorState>> =
        HashMap::with_capacity((rows / 3).max(16) as usize);
    for _ in 0..rows {
        let raw_id = next_xorshift(&mut x) % groups;
        let key = if sparse { scatter(raw_id) } else { raw_id };
        let v = (next_xorshift(&mut x) % 1000) as f64;
        let accs = map
            .entry(key)
            .or_insert_with(|| aggs.iter().map(|k| AccumulatorState::new(*k)).collect());
        for a in accs.iter_mut() {
            a.update(v);
        }
    }
    map
}

fn merge_boxed(
    states: Vec<HashMap<u64, Vec<AccumulatorState>>>,
) -> HashMap<u64, Vec<AccumulatorState>> {
    let cap: usize = states.iter().map(|s| s.len()).sum();
    let mut final_map: HashMap<u64, Vec<AccumulatorState>> = HashMap::with_capacity(cap);
    for state in states {
        for (k, accs) in state {
            match final_map.entry(k) {
                hashbrown::hash_map::Entry::Occupied(mut e) => {
                    for (a, b) in e.get_mut().iter_mut().zip(accs.iter()) {
                        a.merge(b);
                    }
                }
                hashbrown::hash_map::Entry::Vacant(v) => {
                    v.insert(accs);
                }
            }
        }
    }
    final_map
}

fn finalize_boxed(
    map: &HashMap<u64, Vec<AccumulatorState>>,
    num_aggs: usize,
) -> (Vec<u64>, Vec<Vec<f64>>) {
    let mut keys = Vec::with_capacity(map.len());
    let mut vals: Vec<Vec<f64>> = (0..num_aggs)
        .map(|_| Vec::with_capacity(map.len()))
        .collect();
    for (k, accs) in map.iter() {
        keys.push(*k);
        for (i, a) in accs.iter().enumerate() {
            vals[i].push(a.finalize());
        }
    }
    (keys, vals)
}

// ----------------------------------------------------------------- FLAT ---

fn build_flat(
    seed: u64,
    rows: u64,
    groups: u64,
    sparse: bool,
    aggs: &[AggKind],
) -> (HashMap<u64, u32>, Vec<FlatAgg>) {
    let mut x = seed;
    let cap = (rows / 3).max(16) as usize;
    let mut ids: HashMap<u64, u32> = HashMap::with_capacity(cap);
    let mut flats: Vec<FlatAgg> = aggs.iter().map(|k| FlatAgg::new(*k, cap)).collect();
    let mut next_id: u32 = 0;
    for _ in 0..rows {
        let raw_id = next_xorshift(&mut x) % groups;
        let key = if sparse { scatter(raw_id) } else { raw_id };
        let v = (next_xorshift(&mut x) % 1000) as f64;
        let idx = *ids.entry(key).or_insert_with(|| {
            let id = next_id;
            next_id += 1;
            for f in flats.iter_mut() {
                f.push_zero();
            }
            id
        }) as usize;
        for f in flats.iter_mut() {
            f.update(idx, v);
        }
    }
    (ids, flats)
}

fn merge_flat(
    states: Vec<(HashMap<u64, u32>, Vec<FlatAgg>)>,
    aggs: &[AggKind],
) -> (HashMap<u64, u32>, Vec<FlatAgg>) {
    let cap: usize = states.iter().map(|(ids, _)| ids.len()).sum();
    let mut final_ids: HashMap<u64, u32> = HashMap::with_capacity(cap);
    let mut final_flats: Vec<FlatAgg> = aggs.iter().map(|k| FlatAgg::new(*k, cap)).collect();
    let mut next_id: u32 = 0;
    for (ids, flats) in states {
        for (k, local_idx) in ids {
            let idx = *final_ids.entry(k).or_insert_with(|| {
                let id = next_id;
                next_id += 1;
                for f in final_flats.iter_mut() {
                    f.push_zero();
                }
                id
            }) as usize;
            for (ff, lf) in final_flats.iter_mut().zip(flats.iter()) {
                ff.merge_from(idx, lf, local_idx as usize);
            }
        }
    }
    (final_ids, final_flats)
}

fn finalize_flat(ids: &HashMap<u64, u32>, flats: &[FlatAgg]) -> (Vec<u64>, Vec<Vec<f64>>) {
    let n = ids.len();
    let mut keys = vec![0u64; n];
    for (k, idx) in ids.iter() {
        keys[*idx as usize] = *k;
    }
    let vals: Vec<Vec<f64>> = flats
        .iter()
        .map(|f| (0..n).map(|i| f.finalize(i)).collect())
        .collect();
    (keys, vals)
}

// ------------------------------------------------------------- driver ---

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

fn checksum(vals: &[Vec<f64>]) -> u64 {
    vals.iter().flatten().fold(0u64, |acc, v| acc ^ v.to_bits())
}

fn worker_seed(w: usize) -> u64 {
    0x9E3779B97F4A7C15u64.wrapping_add((w as u64).wrapping_mul(0xBF58_476D_1CE4_E5B9))
}

fn run_point(groups: u64, mult: u64, workers: usize, aggs: &[AggKind], sparse: bool) {
    let rows = groups * mult;
    let per_worker = rows / workers as u64;

    // BOXED: today's HashMap<u64, Vec<AccumulatorState>> raw_groups tier.
    let t0 = Instant::now();
    let boxed_states: Vec<_> = (0..workers)
        .into_par_iter()
        .map(|w| build_boxed(worker_seed(w), per_worker, groups, sparse, aggs))
        .collect();
    let t_build_boxed = t0.elapsed();
    let t1 = Instant::now();
    let boxed_final = merge_boxed(boxed_states);
    let t_merge_boxed = t1.elapsed();
    let t2 = Instant::now();
    let (bk, bv) = finalize_boxed(&boxed_final, aggs.len());
    let t_finalize_boxed = t2.elapsed();
    let boxed_total = t_build_boxed + t_merge_boxed + t_finalize_boxed;
    let boxed_checksum = checksum(&bv);
    drop(boxed_final);

    // FLAT: proposed HashMap<u64,u32> + Vec<FlatAgg>.
    let t3 = Instant::now();
    let flat_states: Vec<_> = (0..workers)
        .into_par_iter()
        .map(|w| build_flat(worker_seed(w), per_worker, groups, sparse, aggs))
        .collect();
    let t_build_flat = t3.elapsed();
    let t4 = Instant::now();
    let (fids, fflats) = merge_flat(flat_states, aggs);
    let t_merge_flat = t4.elapsed();
    let t5 = Instant::now();
    let (fk, fv) = finalize_flat(&fids, &fflats);
    let t_finalize_flat = t5.elapsed();
    let flat_total = t_build_flat + t_merge_flat + t_finalize_flat;
    let flat_checksum = checksum(&fv);
    drop((fids, fflats));

    assert_eq!(
        bk.len(),
        fk.len(),
        "boxed and flat disagree on output group count -- bench is not faithful"
    );
    assert_eq!(
        boxed_checksum, flat_checksum,
        "boxed and flat disagree on finalized values -- bench is not faithful"
    );

    let win_pct = (1.0 - flat_total.as_secs_f64() / boxed_total.as_secs_f64().max(1e-12)) * 100.0;
    let domain_label = if sparse { "sparse" } else { "dense " };

    println!(
        "groups={:>10} mult={:>2} rows={:>12} domain={} aggs={} out_rows={}",
        groups,
        mult,
        rows,
        domain_label,
        aggs.len(),
        bk.len()
    );
    println!(
        "  boxed: build={:>9.2}ms merge={:>9.2}ms finalize={:>9.2}ms TOTAL={:>10.2}ms",
        ms(t_build_boxed),
        ms(t_merge_boxed),
        ms(t_finalize_boxed),
        ms(boxed_total)
    );
    println!(
        "  flat:  build={:>9.2}ms merge={:>9.2}ms finalize={:>9.2}ms TOTAL={:>10.2}ms",
        ms(t_build_flat),
        ms(t_merge_flat),
        ms(t_finalize_flat),
        ms(flat_total)
    );
    println!(
        "  => flat is {win_pct:+.1}% wall-time vs boxed (checksums match: {:#x}) {}",
        boxed_checksum,
        if win_pct >= 15.0 {
            "[CLEARS >=15% gate]"
        } else if win_pct >= 0.0 {
            "[below 15-20% gate]"
        } else {
            "[REGRESSION vs boxed]"
        }
    );
    println!();
}

fn main() {
    let workers: usize = std::env::var("DGB_WORKERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| rayon::current_num_threads().clamp(2, 32));
    let mult: u64 = std::env::var("DGB_MULT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3);

    println!(
        "workers={workers} (rayon::current_num_threads()={}, same clamp(2,32) morsel_agg.rs uses)",
        rayon::current_num_threads()
    );
    println!("mult={mult} (rows = mult * groups per point)");
    println!();

    // --- Primary sweep: Q13/Q20-like cardinalities, 1M-50M groups, single
    // Sum aggregate (the dominant real shape -- Q13's inner GROUP BY
    // c_custkey, Q20's packed __pk key), both domain shapes.
    let groups_sweep: Vec<u64> = match std::env::var("DGB_GROUPS") {
        Ok(s) => s.split(',').filter_map(|x| x.trim().parse().ok()).collect(),
        Err(_) => vec![1_000_000, 5_000_000, 10_000_000, 20_000_000, 50_000_000],
    };
    println!("=== Primary sweep: 1M-50M groups, 1 aggregate (Sum), dense then sparse domain ===");
    for &groups in &groups_sweep {
        run_point(groups, mult, workers, &[AggKind::Sum], false);
        run_point(groups, mult, workers, &[AggKind::Sum], true);
    }

    // --- Secondary sweep: aggregate count sensitivity (1-3 aggregates) at
    // one representative group count, dense domain.
    let aggs_groups: u64 = std::env::var("DGB_AGGS_GROUPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000_000);
    let aggs_sweep: Vec<usize> = match std::env::var("DGB_AGGS_SWEEP") {
        Ok(s) => s.split(',').filter_map(|x| x.trim().parse().ok()).collect(),
        Err(_) => vec![1, 2, 3],
    };
    let all_aggs = [AggKind::Sum, AggKind::Count, AggKind::Avg];
    println!(
        "=== Secondary sweep: groups={aggs_groups} fixed, aggregate count varies (dense domain) ==="
    );
    for &n in &aggs_sweep {
        let aggs = &all_aggs[..n.min(3)];
        run_point(aggs_groups, mult, workers, aggs, false);
    }
}
