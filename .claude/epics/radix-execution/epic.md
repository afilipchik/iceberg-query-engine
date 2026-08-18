---
name: radix-execution
status: completed
created: 2026-08-18T14:18:20Z
updated: 2026-08-18T14:18:20Z
progress: 100%
prd: .claude/prds/radix-execution.md
github: (will be set on sync)
---

# Epic: radix-execution

## Overview

Turn the Q9/Q18-class DRAM-miss chains into streaming passes:
radix-partition hash-join builds and probes so each probe hits a
cache-resident sub-table, and radix-partition high-NDV int aggregation
beyond the fused path's 64M-range gate. Proof-first: a microbenchmark
prices the scatter passes against the miss savings on this exact
hardware before any operator is touched; a losing microbench STOPS the
epic cheaply.

## Architecture Decisions

- **Microbench before rewrite (G1 kill-switch).** The scatter costs ~2
  extra streaming passes over 604M×8B keys; the win must beat that with
  margin on THIS box (36MB LLC, hybrid P/E). If isolated radix probe
  < 1.4x over monolithic, record the negative and stop — precedent: the
  measured-and-rejected table in CLAUDE.md's Lance section.
- **Radix INSIDE VectorizedHashTable, not a new operator.** Build mode
  `Partitioned { p_bits, tables: Vec<...> }`: same (bb,br) entry
  payloads, same probe_batch signature returning (bb,br,probe_row)
  matches — `create_joined_batch` and every downstream consumer
  unchanged. The correctness surface stays inside one struct.
- **Partition by the SAME hash the tables use** (top bits for partition,
  low bits in-table) so no second hash pass; probe scatter reorders row
  indices per batch (u32 vecs), never copies key data.
- **Aggregation reuses the disjoint machinery.** partition_batch_by_hash
  + per-partition AggregationState + finalize_disjoint_states already
  exist and are correctness-pinned; the new work is routing the morsel
  raw-sum path through them when NDV-est is high and the range exceeds
  the direct-address gate.
- **Fallbacks stay monolithic**: non-Inner joins, filtered joins,
  multi-key non-packable joins, small builds (< 4M rows — monolithic is
  already cache-resident there).
- **All measurement through scripts/oomsafe.sh, serialized; cell-exact
  after every lever (row counts are not answers).**

## Technical Approach

1. Microbench (`examples/` or `src/bin`): build 80M-entry table from
   synthetic packed keys, probe with 600M keys, monolithic vs
   partitioned at P ∈ {64, 256, 1024}; report ns/probe incl. scatter.
2. `vectorized_hash.rs` / `hash_join.rs`: partitioned build (scatter
   build rows by hash-top-bits, then build P sub-tables in parallel);
   partitioned probe (per batch: compute hashes once, scatter row ids,
   probe each partition's sub-table, emit matches in probe-row order or
   let downstream tolerate reorder — verify joined-batch invariants).
3. `morsel_agg.rs`: high-NDV radix aggregation for the raw-sums path.
4. Gates per lever on Q9/Q18/Q5/Q21 + full sweeps.

## Implementation Strategy

Strictly staged: 001 microbench (verdict gates everything) → 002 join
build+probe behind QE_RADIX → 003 measure/tune/default → 004 radix
aggregation → 005 QA close-out. Any stage can end the epic with a
documented negative.

## Task Breakdown Preview

- 001: Probe-cost microbenchmark + G1 verdict. [S]
- 002: Partitioned VHT build+probe (Inner, i64/packed keys) behind
  QE_RADIX. [XL]
- 003: SF=100/SF=10 gates, partition-count tuning, default decision. [M]
- 004: Radix high-NDV aggregation on the morsel raw-sum path. [L]
- 005: QA close-out: suites, cell-exact both scales/formats, docs, epic
  close. [M]

## Dependencies

001 → 002 → 003; 004 independent of 002/003 (different files) but
measurement-serialized; 005 last.

## Success Criteria (Technical)

G1 microbench verdict; G2 Q9 ≤ 15s, Q18 ≤ 6.5s, suite ≤ 66s, 22/22
cell-exact both scales; G3 SF=10 ≤ 7.9s, lance warm A/Bs inherit.

## Estimated Effort

002 is the epic (XL). Total: one long session IF the microbench says go;
one hour if it says stop.

## Tasks Created
- [ ] 001.md - Probe-cost microbenchmark + G1 verdict (parallel: false)
- [ ] 002.md - Partitioned VHT build+probe behind QE_RADIX (parallel: false)
- [ ] 003.md - SF gates, partition tuning, default decision (parallel: false)
- [ ] 004.md - Radix high-NDV aggregation (morsel raw-sum path) (parallel: true)
- [ ] 005.md - QA close-out — suites, cell-exact, docs, epic close (parallel: false)

Total tasks: 5
Parallel tasks: 1
Sequential tasks: 4
Estimated total effort: 15-29 hours (one hour if 001 says stop)

## Epic close-out (2026-08-18)

The kill-switch epic worked exactly as designed: task 001's
microbenchmark REFUTED radix partitioning in under an hour (probes are
memory-level-parallelism-bound at 3.8 ns/row wall; scatter only adds
work), task 002's HJ_PROF attribution named the real cost
(gather+batch ~75% of Q9's probe pipeline), and task 003 shipped the
fix that evidence pointed at instead: **join-output pruning** — ON-only
columns dropped from Inner-join outputs, row stores and gathers.

**SF=100 parquet 72.7 → 66.1s (0.9x DuckDB NATIVE — first time under),
1.65x like-for-like. Q9 18.7 → 13.6s (gate MET). Lance: Q9 20.8 → 15.2s,
SF=10 6.87s ALL CELL-EXACT. No regressions; 14/14 suites green.**
Task 004 (radix aggregation) refuted without implementation by the same
MLP argument. Q18's ≤6.5s gate not met — its residue is at measured
bandwidth floors, recorded in PARITY-PLAN.

Commits: e817a6a (scaffolding), 8d2a2b3 (pruning + profiler + bench).
