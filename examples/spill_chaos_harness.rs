//! Fault-injection / differential testing harness for
//! `SpillableHashJoinExec`'s spill/unspill machinery
//! (spill-join-correctness-2 epic, task 003).
//!
//! ## What this replaces
//!
//! The archived `spill-join-correctness` epic's own investigation (its
//! `001.md` Outcome) relied on a manual repro loop against a real SF=10
//! native-table fixture: 140-291s/trial pre-fix, 3-6s/trial post-fix, ~20-290
//! trials total across that epic and this one's own task 001. Its own
//! recorded "single highest-leverage unattempted next step" was "a
//! deliberately downsized synthetic repro... not attempted here." This
//! binary is that: hundreds of trials in well under a minute, by (a) using a
//! small, already-committed fixture (`data/tpch-10mb`, the same one
//! `tests/spill_tests.rs` already uses) and (b) FORCING the spill/unspill
//! machinery to engage deterministically via two new production-code hooks
//! in `src/physical/operators/spillable.rs` — `QE_SPILL_CHAOS_FORCE_SPILL`
//! (WHEN: force the build/no-build decision to cross into the disk-spill
//! branch after a chosen number of build batches, regardless of how much
//! data is actually present) and `QE_SPILL_CHAOS_FORCE_SPILL_PARTITIONS`
//! (WHICH: force specific hash partitions to actually write/read spill
//! files, regardless of memory pressure) — rather than needing gigabytes of
//! real data to organically cross a memory threshold. See that file's own
//! "Fault injection: forced spill" module doc comment for the full design.
//!
//! ## Methodology
//!
//! Each trial runs ONE logical query TWICE against the SAME registered
//! tables in the SAME `ExecutionContext` (so table registration/catalog
//! setup, the expensive one-time cost, is paid once for the whole run, not
//! once per trial):
//!
//! 1. **Baseline**: no chaos env vars set — the query's natural, unforced
//!    execution (over `data/tpch-10mb`, this always stays on the in-memory
//!    `HashJoinExec` delegate; `ExecutionConfig::default()`'s 1GB memory
//!    limit is far more than this fixture needs).
//! 2. **Forced**: `QE_SPILL_CHAOS_FORCE_SPILL`/`QE_SPILL_CHAOS_FORCE_SPILL_PARTITIONS`
//!    set for the duration of this one call only, forcing the SAME query
//!    through `BuildDecision::Spill` — `build_with_partitioning`,
//!    `probe_with_spilling`, and (for every forced partition)
//!    `process_spilled_partition`'s disk read-back.
//!
//! **Differential invariant checked: an order-independent XOR checksum over
//! every output row's every column, plus row count** (`output_checksum`
//! below) — not full cell-by-cell row-vector comparison. Chosen over the
//! stronger (but O(rows log rows)) sort-and-compare `tests/spill_tests.rs`
//! itself uses, specifically FOR per-trial cheapness at "hundreds, not
//! tens" of trials: a checksum is O(rows) with no allocation/sort, computed
//! identically to (and directly modeled on) this epic's own task 001
//! production mechanism (`KeyChecksum`/`batch_key_checksum` in
//! `spillable.rs`, which is order- and batch-split-independent by
//! construction — the exact same reasoning applies here to whole output
//! rows, not just join keys). A checksum collision would need two
//! genuinely different multisets of rows to hash identically — possible in
//! principle, not in practice at these trial counts, and this binary's own
//! sole purpose (repeatedly regenerated real query output, not
//! adversarial input) makes an engineered collision a non-concern. On any
//! mismatch this binary prints full diagnostics (query, trial's exact
//! chosen injection point, both checksums, both row counts) for follow-up
//! — same "direct evidence, not inference" discipline as `KeyChecksum`
//! itself.
//!
//! A before/after DELTA on `ExecutionContext::memory_pool().spilled()`
//! (`tests/spill_tests.rs`'s own `assert_spill_matches` instead uses
//! `QueryResult::metrics.spill_metrics.is_some()`, but that field is
//! derived from the SAME pool's CUMULATIVE lifetime total, which only
//! means "did this query spill" for a context used for exactly one query —
//! this harness deliberately reuses one context/pool across all trials, so
//! a delta is measured explicitly instead) is checked on EVERY trial to
//! confirm the forced run genuinely touched disk when the chosen variant
//! should guarantee it (`All`/`Subset`) — a forced trial that claims to
//! force partitions but never records a spill would mean this harness's
//! OWN injection isn't working, not that the query happened to pass;
//! treated as a harness bug (loud warning, non-zero exit) if it ever
//! happens.
//!
//! ## Usage
//!
//! ```text
//! scripts/claude-safe-build.sh cargo build --release --example spill_chaos_harness
//! ./target/release/examples/spill_chaos_harness                      # 300 trials, default seed
//! QE_CHAOS_TRIALS=1000 QE_CHAOS_SEED=42 ./target/release/examples/spill_chaos_harness
//! QE_CHAOS_DATA_DIR=data/tpch-100mb ./target/release/examples/spill_chaos_harness
//! ```
//!
//! Exits 0 with a summary line on an all-pass run; exits 1 (after printing
//! every mismatch's full diagnostics) if any trial's checksums disagree, or
//! if any trial that should have genuinely spilled did not.
//!
//! This is a **permanent, reusable tool** — kept in `examples/` (not
//! `.scratch/`), matching `examples/spill_join_oom_repro.rs`'s own
//! precedent for this class of adversarial verification, and intended to
//! supersede the archived epic's own `.scratch/spill_join_repro/repro.sh`
//! for spill/unspill CORRECTNESS sweeps specifically (`repro.sh` remains
//! the right tool for reproducing the ORIGINAL SF=10 native-table
//! wrong-answer symptom shape at real scale; this tool is for cheap, high-
//! volume differential regression sweeps of the spill/unspill round trip
//! itself, a job `repro.sh` was never fast enough to do at more than
//! double-digit trial counts).

use query_engine::{ExecutionContext, QueryResult};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::Instant;

const TABLES: [&str; 8] = [
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

/// Three different INNER equi-joins over three different table/key-type
/// pairs — deliberately UNAGGREGATED (raw join output, every matched row
/// visible) so a row-level corruption (extra/missing/duplicated row) is
/// directly visible in the checksum, not potentially masked by an
/// aggregate collapsing many rows into one group total. All three are
/// plain `INNER JOIN`-shaped (implicit cross + equi `WHERE`), the only join
/// shape the join spill path supports (see `left_join_spill_fails_loudly_
/// not_wrong` in `tests/spill_tests.rs`) — required for
/// `QE_SPILL_CHAOS_FORCE_SPILL` to ever succeed rather than hit that
/// INNER-only guard.
const QUERIES: &[&str] = &[
    "SELECT o_orderpriority, o_orderkey, l_partkey, l_suppkey, l_quantity, l_extendedprice \
     FROM lineitem, orders WHERE l_orderkey = o_orderkey",
    "SELECT c_custkey, c_mktsegment, o_orderkey, o_orderdate, o_totalprice \
     FROM customer, orders WHERE c_custkey = o_custkey",
    "SELECT ps_partkey, ps_suppkey, ps_availqty, p_name, p_retailprice \
     FROM partsupp, part WHERE ps_partkey = p_partkey",
];

/// `NUM_PARTITIONS` in `spillable.rs` — duplicated here rather than
/// exported, since it's a private implementation constant of that module;
/// kept in sync manually (a mismatch would only ever make `Subset`
/// trials pick indices past the real partition count, which
/// `ChaosPartitionSpec::parse`'s own `filter_map` silently drops — degraded
/// coverage, never a wrong result).
const NUM_PARTITIONS: usize = 64;

fn register_tables(ctx: &mut ExecutionContext, data_dir: &str) {
    for table in &TABLES {
        let path = format!("{data_dir}/{table}.parquet");
        ctx.register_parquet(*table, &path)
            .unwrap_or_else(|e| panic!("Failed to load {path}: {e}"));
    }
}

/// Order-independent checksum over EVERY column of every output row — see
/// this file's own module doc comment ("Methodology") for why this, not
/// full row-vector comparison, is the chosen invariant. Modeled directly on
/// `spillable.rs`'s own `batch_key_checksum`/`KeyChecksum` (task 001):
/// per-row hash of every column's value (position-sensitive within a row,
/// order-INsensitive across rows — an XOR accumulation), so it is immune to
/// the same "spilled data doesn't preserve original row/batch order" fact
/// that mechanism itself documents.
fn output_checksum(result: &QueryResult) -> (usize, u64) {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    let mut rows = 0usize;
    let mut xor_hash = 0u64;
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            rows += 1;
            // FNV-1a-style running hash, seeded per-row so two rows with
            // the same multiset of column values in a different COLUMN
            // ORDER would not collide (columns are hashed in a fixed,
            // schema-determined order below).
            let mut h: u64 = 0xcbf29ce484222325;
            for (col_idx, col) in batch.columns().iter().enumerate() {
                h ^= (col_idx as u64).wrapping_mul(0x9e3779b97f4a7c15);
                if col.is_null(row) {
                    h = h.wrapping_mul(0x100000001b3).wrapping_add(0xDEAD);
                    continue;
                }
                let bytes: Vec<u8> = match col.data_type() {
                    DataType::Int32 => col
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .value(row)
                        .to_le_bytes()
                        .to_vec(),
                    DataType::Int64 => col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row)
                        .to_le_bytes()
                        .to_vec(),
                    DataType::UInt64 => col
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap()
                        .value(row)
                        .to_le_bytes()
                        .to_vec(),
                    DataType::Float64 => col
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .value(row)
                        .to_bits()
                        .to_le_bytes()
                        .to_vec(),
                    DataType::Utf8 => col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row)
                        .as_bytes()
                        .to_vec(),
                    DataType::Date32 => col
                        .as_any()
                        .downcast_ref::<Date32Array>()
                        .unwrap()
                        .value(row)
                        .to_le_bytes()
                        .to_vec(),
                    other => panic!(
                        "output_checksum(): unhandled column type {other:?} \
                         — extend this harness's checksum function"
                    ),
                };
                for b in bytes {
                    h = h.wrapping_mul(0x100000001b3) ^ (b as u64);
                }
            }
            xor_hash ^= h;
        }
    }
    (rows, xor_hash)
}

/// This trial's WHICH-partitions injection choice — see `spillable.rs`'s
/// own `QE_SPILL_CHAOS_FORCE_SPILL_PARTITIONS` doc comment for what each
/// maps to.
#[derive(Clone, Debug)]
enum PartitionsVariant {
    /// Env var left UNSET: only `QE_SPILL_CHAOS_FORCE_SPILL` (the
    /// build/no-build decision) is forced — exercises `BuildDecision::Spill`
    /// as a code path without necessarily touching disk (this fixture is
    /// small enough that organic memory-pressure eviction inside
    /// `build_with_partitioning` may never trigger either). Real, but
    /// weaker, coverage — tallied separately below.
    DecisionOnly,
    /// Every partition forced — maximum single-trial disk coverage.
    All,
    /// A random subset forced — exercises the MIXED in-memory/spilled
    /// partition case inside `execute_spill_path`/`probe_with_spilling`,
    /// arguably the more subtle code path (both kinds must combine
    /// correctly in one call) versus `All`'s uniform case.
    Subset(Vec<usize>),
}

fn choose_variant(rng: &mut StdRng) -> PartitionsVariant {
    match rng.gen_range(0..10) {
        0 => PartitionsVariant::DecisionOnly, // 10%
        1..=4 => PartitionsVariant::All,      // 40%
        _ => {
            // 50%: a random subset, size 1..=NUM_PARTITIONS, distinct indices.
            let n = rng.gen_range(1..=NUM_PARTITIONS);
            let mut idxs: Vec<usize> = (0..NUM_PARTITIONS).collect();
            // Partial Fisher-Yates shuffle down to `n` elements — good
            // enough for trial-selection purposes (this is test-harness
            // randomness, not anything security- or correctness-sensitive).
            for i in 0..n {
                let j = rng.gen_range(i..NUM_PARTITIONS);
                idxs.swap(i, j);
            }
            idxs.truncate(n);
            idxs.sort_unstable();
            PartitionsVariant::Subset(idxs)
        }
    }
}

#[tokio::main]
async fn main() {
    let n_trials: usize = std::env::var("QE_CHAOS_TRIALS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    let seed: u64 = std::env::var("QE_CHAOS_SEED")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(53170301);
    let data_dir =
        std::env::var("QE_CHAOS_DATA_DIR").unwrap_or_else(|_| "data/tpch-10mb".to_string());

    println!("=== spill_chaos_harness: trials={n_trials} seed={seed} data_dir={data_dir} ===");

    let mut ctx = ExecutionContext::new();
    register_tables(&mut ctx, &data_dir);

    let mut rng = StdRng::seed_from_u64(seed);
    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut genuine_disk_trials = 0usize;
    let mut decision_only_trials = 0usize;
    let mut disk_expected_but_missing = 0usize;
    let t0 = Instant::now();

    for trial in 0..n_trials {
        let query = QUERIES[trial % QUERIES.len()];
        let after_batches: usize = rng.gen_range(0..3);
        let variant = choose_variant(&mut rng);

        // `QueryResult::metrics.spill_metrics` is derived from
        // `ExecutionContext`'s own `memory_pool.spilled()`, a CUMULATIVE
        // counter for the pool's whole lifetime (never reset per query) —
        // fine for `tests/spill_tests.rs`'s own usage (a fresh
        // `ExecutionContext` per test), but this harness deliberately
        // reuses ONE context/pool across all trials (registering 8 tables
        // twice per trial would dominate the per-trial cost otherwise), so
        // "did THIS query spill" is measured as a before/after DELTA on the
        // shared pool instead.
        let spilled_before = ctx.memory_pool().spilled();
        let baseline = ctx
            .sql(query)
            .await
            .unwrap_or_else(|e| panic!("trial {trial}: baseline run failed: {e}"));
        let baseline_spilled_delta = ctx.memory_pool().spilled() - spilled_before;
        assert_eq!(
            baseline_spilled_delta, 0,
            "trial {trial}: baseline (no chaos vars set) unexpectedly spilled organically \
             — this fixture/memory-limit combination should never do that; harness invariant \
             broken, investigate before trusting any result below"
        );

        // Chaos vars are set for the duration of this ONE call only, then
        // removed immediately — this binary runs trials strictly
        // sequentially in one process (no concurrent `.sql()` calls), so
        // mutating the real process environment here is safe (see
        // `spillable.rs`'s own `chaos_force_spill_*` module doc comment for
        // why this would NOT be safe inside `cargo test`'s concurrent test
        // threads, and why that file's own unit test for this mechanism
        // holds a dedicated mutex instead).
        std::env::set_var("QE_SPILL_CHAOS_FORCE_SPILL", after_batches.to_string());
        match &variant {
            PartitionsVariant::DecisionOnly => {
                std::env::remove_var("QE_SPILL_CHAOS_FORCE_SPILL_PARTITIONS");
            }
            PartitionsVariant::All => {
                std::env::set_var("QE_SPILL_CHAOS_FORCE_SPILL_PARTITIONS", "all");
            }
            PartitionsVariant::Subset(idxs) => {
                let spec = idxs
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<_>>()
                    .join(",");
                std::env::set_var("QE_SPILL_CHAOS_FORCE_SPILL_PARTITIONS", spec);
            }
        }
        let spilled_before_forced = ctx.memory_pool().spilled();
        let forced_result = ctx.sql(query).await;
        std::env::remove_var("QE_SPILL_CHAOS_FORCE_SPILL");
        std::env::remove_var("QE_SPILL_CHAOS_FORCE_SPILL_PARTITIONS");

        let forced = match forced_result {
            Ok(r) => r,
            Err(e) => {
                failed += 1;
                eprintln!(
                    "TRIAL {trial} FAIL (forced run errored): query={query:?} \
                     variant={variant:?} after_batches={after_batches} error={e}"
                );
                continue;
            }
        };

        let touched_disk = ctx.memory_pool().spilled() > spilled_before_forced;
        if touched_disk {
            genuine_disk_trials += 1;
        } else {
            decision_only_trials += 1;
            if matches!(
                variant,
                PartitionsVariant::All | PartitionsVariant::Subset(_)
            ) {
                disk_expected_but_missing += 1;
                eprintln!(
                    "TRIAL {trial} WARNING: variant={variant:?} should force a genuine disk \
                     spill but the memory pool's spilled-bytes delta was 0 — the injection \
                     hook may not be firing"
                );
            }
        }

        let base_cs = output_checksum(&baseline);
        let forced_cs = output_checksum(&forced);
        if base_cs == forced_cs {
            passed += 1;
        } else {
            failed += 1;
            eprintln!(
                "TRIAL {trial} MISMATCH: query={query:?} variant={variant:?} \
                 after_batches={after_batches} baseline=(rows={}, xor={:016x}) \
                 forced=(rows={}, xor={:016x})",
                base_cs.0, base_cs.1, forced_cs.0, forced_cs.1
            );
        }
    }

    let elapsed = t0.elapsed();
    println!("=== spill_chaos_harness summary ===");
    println!(
        "trials={n_trials} passed={passed} failed={failed} \
         genuine_disk_spill_trials={genuine_disk_trials} decision_only_trials={decision_only_trials} \
         disk_expected_but_missing={disk_expected_but_missing} elapsed={elapsed:?} \
         ({:.1} ms/trial)",
        elapsed.as_secs_f64() * 1000.0 / n_trials as f64
    );

    if failed > 0 || disk_expected_but_missing > 0 {
        println!("RESULT: FAIL");
        std::process::exit(1);
    }
    println!("RESULT: PASS");
}
