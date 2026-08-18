//! Probe-cost microbenchmark: monolithic vs radix-partitioned hash probes
//! on Q9's shape (80M-entry build, packed-i64-class keys, FK-matched probe
//! stream in random order). Task 001 of the radix-execution epic — its
//! verdict gates the whole epic.
//!
//! Mirrors `VectorizedHashTable`'s actual layout: flat chained buckets
//! (`heads`/`next`/`entries` u32 arrays) and its `hash_i64` (Fibonacci
//! seed + value). The partitioned variant selects a partition from the
//! hash TOP bits and indexes a per-partition sub-table with the low bits,
//! pricing IN the per-chunk row-id scatter that the real operator would
//! have to do.
//!
//! Run: scripts/oomsafe.sh cargo run --release --example radix_bench

use rayon::prelude::*;
use std::time::Instant;

const SEED_HASH: u64 = 0x517cc1b727220a95u64.wrapping_mul(0x9e3779b97f4a7c15);

#[inline(always)]
fn hash_i64(v: i64) -> u64 {
    SEED_HASH.wrapping_add(v as u64)
}

struct Mono {
    heads: Vec<u32>,
    next: Vec<u32>,
    keys: Vec<i64>,
    mask: usize,
}

impl Mono {
    fn build(keys: &[i64]) -> Self {
        let nbuckets = (keys.len() * 2).next_power_of_two();
        let mask = nbuckets - 1;
        let mut heads = vec![u32::MAX; nbuckets];
        let mut next = vec![u32::MAX; keys.len()];
        for (i, &k) in keys.iter().enumerate() {
            let b = hash_i64(k) as usize & mask;
            next[i] = heads[b];
            heads[b] = i as u32;
        }
        Self {
            heads,
            next,
            keys: keys.to_vec(),
            mask,
        }
    }

    #[inline(always)]
    fn probe(&self, k: i64) -> u64 {
        let mut acc = 0u64;
        let mut e = self.heads[hash_i64(k) as usize & self.mask];
        while e != u32::MAX {
            if self.keys[e as usize] == k {
                acc += e as u64;
            }
            e = self.next[e as usize];
        }
        acc
    }
}

struct Radix {
    parts: Vec<Mono>,
    p_bits: u32,
}

impl Radix {
    fn build(keys: &[i64], p_bits: u32) -> Self {
        let p = 1usize << p_bits;
        let mut buckets: Vec<Vec<i64>> = vec![Vec::with_capacity(keys.len() / p + 16); p];
        for &k in keys {
            let pi = (hash_i64(k) >> (64 - p_bits)) as usize;
            buckets[pi].push(k);
        }
        let parts: Vec<Mono> = buckets.into_par_iter().map(|b| Mono::build(&b)).collect();
        Self { parts, p_bits }
    }

    /// Probe a chunk the way the real operator would: scatter row ids by
    /// partition first (this cost is INCLUDED), then drain partitions so
    /// each sub-table stays hot while it is being probed.
    fn probe_chunk(&self, chunk: &[i64], scratch: &mut Vec<Vec<u32>>) -> u64 {
        for s in scratch.iter_mut() {
            s.clear();
        }
        for (i, &k) in chunk.iter().enumerate() {
            let pi = (hash_i64(k) >> (64 - self.p_bits)) as usize;
            scratch[pi].push(i as u32);
        }
        let mut acc = 0u64;
        for (pi, rows) in scratch.iter().enumerate() {
            let part = &self.parts[pi];
            for &r in rows {
                acc += part.probe(chunk[r as usize]);
            }
        }
        acc
    }
}

fn main() {
    let build_n: usize = std::env::var("RB_BUILD")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(80_000_000);
    let probe_n: usize = std::env::var("RB_PROBE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300_000_000);
    let chunk_rows: usize = 16 * 1024; // ~ a probe batch

    // Build keys: packed (suppkey * 2^25 + partkey)-shaped, deduped by
    // construction (suppkey = i / 20M-ish spread, partkey = i % 20M).
    println!("generating {build_n} build keys, {probe_n} probes ...");
    let build_keys: Vec<i64> = (0..build_n as i64)
        .map(|i| ((i % 1_000_000) << 25) | (i / 4))
        .collect();

    // Probe stream: build keys in pseudo-random order (xorshift walk),
    // FK-style every-probe-matches.
    let mask = build_n.next_power_of_two() - 1;
    let mut probe_keys: Vec<i64> = Vec::with_capacity(probe_n);
    let mut x: u64 = 0x243F6A8885A308D3;
    while probe_keys.len() < probe_n {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let idx = (x as usize) & mask;
        if idx < build_n {
            probe_keys.push(build_keys[idx]);
        }
    }

    let threads = rayon::current_num_threads();
    let t0 = Instant::now();
    let mono = Mono::build(&build_keys);
    println!(
        "monolithic build: {:?} ({} buckets, {} MB heads)",
        t0.elapsed(),
        mono.mask + 1,
        (mono.mask + 1) * 4 / 1_000_000
    );

    // Single-thread monolithic probe (on a slice to keep wall sane).
    let st_n = probe_n.min(50_000_000);
    let t = Instant::now();
    let mut acc = 0u64;
    for &k in &probe_keys[..st_n] {
        acc += mono.probe(k);
    }
    let mono_st = t.elapsed().as_nanos() as f64 / st_n as f64;
    println!("monolithic 1-thread: {mono_st:.1} ns/probe (acc {acc})");

    // Parallel monolithic probe.
    let t = Instant::now();
    let acc: u64 = probe_keys
        .par_chunks(chunk_rows)
        .map(|c| c.iter().map(|&k| mono.probe(k)).sum::<u64>())
        .sum();
    let mono_mt = t.elapsed().as_nanos() as f64 / probe_n as f64;
    println!("monolithic {threads}-thread: {mono_mt:.2} ns/probe wall (acc {acc})");
    drop(mono);

    for p_bits in [6u32, 8, 10] {
        let p = 1usize << p_bits;
        let t0 = Instant::now();
        let radix = Radix::build(&build_keys, p_bits);
        let heads_mb: usize =
            radix.parts.iter().map(|m| (m.mask + 1) * 4).sum::<usize>() / 1_000_000;
        println!(
            "radix P={p} build: {:?} (~{} KB heads/part, {heads_mb} MB total)",
            t0.elapsed(),
            heads_mb * 1000 / p
        );

        let t = Instant::now();
        let mut scratch: Vec<Vec<u32>> = vec![Vec::with_capacity(chunk_rows / p + 8); p];
        let mut acc = 0u64;
        for c in probe_keys[..st_n].chunks(chunk_rows) {
            acc += radix.probe_chunk(c, &mut scratch);
        }
        let r_st = t.elapsed().as_nanos() as f64 / st_n as f64;
        println!(
            "radix P={p} 1-thread: {r_st:.1} ns/probe incl. scatter ({:.2}x vs mono) (acc {acc})",
            mono_st / r_st
        );

        let t = Instant::now();
        let acc: u64 = probe_keys
            .par_chunks(chunk_rows)
            .map_init(
                || vec![Vec::with_capacity(chunk_rows / p + 8); p],
                |scratch, c| radix.probe_chunk(c, scratch),
            )
            .sum();
        let r_mt = t.elapsed().as_nanos() as f64 / probe_n as f64;
        println!(
            "radix P={p} {threads}-thread: {r_mt:.2} ns/probe wall ({:.2}x vs mono) (acc {acc})",
            mono_mt / r_mt
        );
    }
}
