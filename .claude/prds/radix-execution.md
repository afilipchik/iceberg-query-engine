---
name: radix-execution
description: Radix-partitioned (cache-resident) hash join probes and high-NDV aggregation to attack the Q9/Q18-class CPU-saturation residue left by the duckdb-parity epic
status: backlog
created: 2026-08-18T14:18:20Z
---

# PRD: radix-execution

## Executive Summary

The duckdb-parity epic (closed 2026-08-18 at SF=100 parquet 72.7s /
1.81x like-for-like) ended with a named, measured residue: the big join
queries are CPU-SATURATION-bound. Q9 burns ~600s of CPU on 32 cores —
604M probe rows against monolithic 80M–150M-entry hash tables, each
probe chaining 2–4 dependent DRAM misses; software prefetch measured
NEUTRAL because every stage of the pipeline is fighting for the same
cores. The only way down is less WORK per probe: radix-partition both
join sides so each probe hits a cache-resident table, and the same for
high-cardinality aggregation (Q18's 150M-group subquery still runs 3.05s
vs DuckDB's 2.0s). This is PARITY-PLAN "2b generalized" — the rewrite
both prior epics deferred.

## Problem Statement

Per-query evidence at SF=100 (final duckdb-parity sweep vs DuckDB on the
same parquet):

- **Q9 18.7s vs 9.5s**: partsupp join = 80M-entry packed-key VHT probed
  by 604M rows; orders join = 150M entries probed by 604M. Hash tables
  are ~1–4GB; every probe misses LLC. Probe walls of 4.5–14s/partition
  are queueing on saturated cores, not latency per se.
- **Q18 7.6s vs 4.2s**: 150M-group aggregate at ~4ns/row scan+process
  (thread-local maps larger than LLC) + a 604M-row probe of a 6.5M-entry
  build.
- Same mechanism, smaller: Q21 5.5 vs 4.0, Q20 5.0 vs 2.6, Q5 3.7 vs
  1.55, Q3 4.0 vs 1.8.

DuckDB's answer is radix partitioning: hash-partition build AND probe by
key radix so each partition's table fits in L2/LLC-share, turning
DRAM-latency chains into streaming passes. The engine already owns
adjacent machinery: SpillableHashJoinExec partitions for spill,
partition_batch_by_hash + finalize_disjoint_states partition for the
fused aggregate.

## User Stories

**As the engine developer**, I want proof before rewrite: a
microbenchmark that isolates monolithic-vs-radix probe cost on this
box's actual shapes (80M build / 600M probe, packed i64 keys).
- AC: a `.scratch`-class harness reporting per-row probe cost both ways;
  a STOP decision recorded if radix does not win ≥1.4x on the isolated
  shape (partitioning passes cost real bandwidth; the win must cover
  them).

**As the engine developer**, I want an opt-in radix probe path for
Inner i64-key joins (`QE_RADIX=1`) that partitions the build once at
build time and probe batches at probe time.
- AC: cell-exact on SF=10 + SF=100; A/B via env on the shipped binary;
  memory-safe (partition counts bounded; spill path untouched).

**As the engine developer**, I want radix aggregation for high-NDV int
keys beyond the fused path's 2M..64M disjoint gate (Q18's 150M-key
subquery, range ~600M).
- AC: Q18 subquery microbench ≤ 2.4s (from 3.05; DuckDB 2.0); reuses
  finalize_disjoint_states; no regression on Q1/Q13-class.

**As the engine developer**, I want the winning configuration on by
default with honest gates.
- AC: Q9 SF=100 ≤ 15s, Q18 ≤ 6.5s, suite ≤ 66s (≥1.65x→~1.6x l4l), no
  query regresses >5%, 22/22 cell-exact both scales, full suites green.

## Functional Requirements

1. Probe-cost microbenchmark (bin or example; committed).
2. Radix-partitioned build: VectorizedHashTable gains a partitioned mode
   (P sub-tables, P = f(build rows, LLC), each with its own heads/
   entries); build-side batches scattered once.
3. Radix probe: probe batches hash-scattered to partition-local probe
   runs; output (build_idx, probe_idx) pairs feed the existing joined-
   batch construction unchanged (correctness surface stays small).
4. Radix aggregation: partition_batch_by_hash-style scatter feeding
   per-partition AggregationStates for single-int-key high-NDV shapes on
   the morsel path; finalize via existing disjoint machinery.
5. Env switches: QE_RADIX=0/1 override; defaults decided by measurement.

## Non-Functional Requirements

- Memory-safe always: radix partitioning must not duplicate the build
  side beyond a bounded scatter buffer; spillable paths unchanged.
- 22/22 cell-exact vs DuckDB at SF=10 AND SF=100 after every lever.
- Full suites green (default, QE_IPC_CACHE=1, --features lance).
- All heavy runs through scripts/oomsafe.sh (no OOMSAFE_MEMHIGH on
  measurement runs).
- Commit-or-revert per lever; honest negatives recorded (the
  microbenchmark exists to allow a cheap full-stop).

## Success Criteria

- **G1**: microbench verdict recorded (radix ≥1.4x on isolated probe, or
  epic stops with the negative documented).
- **G2**: Q9 SF=100 ≤ 15s; Q18 ≤ 6.5s; suite ≤ 66s warm; 22/22
  cell-exact both scales.
- **G3**: SF=10 ≤ 7.9s; lance leg inherits wins (shared operators),
  verified by warm single-query A/Bs (sweep totals are noise on this
  box).
- **Stretch**: suite ≤ 60s (the duckdb-parity G1 that was out of reach).

## Constraints & Assumptions

- i9-13900KF: 36MB LLC shared, 2MB L2 per P-core; 125GB RAM; THP off.
  Partition sizing targets ≤ ~1MB of hash table per partition per
  concurrent worker (L2-resident) as the first guess; measure.
- The scatter pass costs ~2 extra streaming passes over probe keys —
  ~5GB/s×604M×8B ≈ 1s-class; the microbench must price this in.
- Non-inner joins, filtered joins, and multi-batch builds may keep the
  monolithic path indefinitely (fall back cleanly).

## Out of Scope

- Full selection-vector operator contract (batch, sel) engine-wide.
- Distributed M3, window functions, decode-path rewrites.
- NUMA-aware partition placement (box has no NUMA).

## Dependencies

- duckdb-parity epic's baseline table and diagnostics (HJ_TIMING,
  AGG_TIMING, QE_AGG_PROF), scripts/oomsafe.sh, existing partitioning
  machinery (spill partitioner, partition_batch_by_hash,
  finalize_disjoint_states).
