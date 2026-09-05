---
name: join-spill-streaming
description: Make the join spill path stream its probe side and its output and process spilled partitions in parallel, so larger-than-memory joins are bounded by the budget and Q9 at SF=100/1G drops from ~1,400s to minutes
status: active
created: 2026-09-05T01:38:00Z
---

# PRD: join-spill-streaming

## Executive Summary

`SpillableHashJoinExec::execute_spill_path` is correct at SF=100
(certified 2026-09-03) but has two structural costs the certification
measured directly: (1) it collects the ENTIRE probe side into a
`Vec<RecordBatch>` before probing (Q9 at 1G: 333,333,330 probe rows),
and (2) it collects the ENTIRE join OUTPUT into a `Vec<RecordBatch>`
before returning a stream (Q9's second join emitted 1,333,333,320 rows
into memory — 1,396s of the query's ~1,400s). Spilled partitions are
then processed one at a time on one thread. Consequences: the probe
side and the output are unbounded by `--memory-limit` (Q4 at 64M needed
a 32G cap; the SF=100-class harness join peaked at 8GB for a 256MB
budget), and heavy spilling joins are an order of magnitude slower than
they need to be on a 32-core machine.

## Problem Statement

"Memory-safe by default" currently holds for the build side only. A
join whose probe side or output exceeds physical memory still OOMs (or
hits `QE_MEM_CAP`) on the spill path, and the spill path's wall time is
dominated by materialization and single-threaded read-back rather than
by I/O.

## User Stories

**As someone running a larger-than-memory join,** I want peak memory to
follow the configured budget, not the probe side's or the output's size.
- Acceptance: the SF=100-class harness `semi-join`/`anti-join` (600M-row
  build, 300M-row probe, 256MB budget) complete under the DEFAULT 1G cap
  on both levers (today they need 12G); Q4 SF=100 native at 64M
  completes under an 8G cap (today 32G).

**As someone waiting on Q9 at a tight budget,** I want the spilling join
to use the machine.
- Acceptance: Q9 SF=100 parquet at `--memory-limit 1G`, cell-exact, in
  ≤ 300s (today ~1,400s), on the same machine, same cap.

## Functional Requirements

1. **Stream the probe side**: consume `stream_merge_input_partitions(
   probe_side)` batch by batch; never hold more than in-flight batches
   plus the per-partition spill writers.
2. **Stream the output**: `execute_spill_path` returns a stream that
   yields matched batches as they are produced — phase A (resident
   partitions probed per incoming batch, spilled-partition probe rows
   written) and phase B (spilled partitions, per chunk) — instead of a
   fully materialized `Vec`. Repeat-execution semantics (memoized build
   decision; probe files per call) preserved.
3. **Parallel spilled-partition processing**: process up to K spilled
   partitions concurrently (K from available parallelism, default
   bounded), each chunk budget = threshold / K so total transient table
   memory stays within the SAME budget; INNER probing within a partition
   parallel over probe batches. Output order is irrelevant (set
   semantics; downstream operators never assume order from a join).
4. **Diagnostics**: QE_SPILL_DEBUG traces gain per-partition elapsed and
   the chosen K; hash-check semantics unchanged.
5. **Certification**: re-run the 2026-09-03 SF=100 sweeps (parquet 1G,
   256M; native 100G, 1G) with identical verdicts, plus the perf targets.

## Non-Functional Requirements

- Cell-exact everywhere; every existing spill test, chaos battery (≥300
  trials), SEMI/ANTI tests, and the four suite combos green.
- Memory: never exceed the budget by more than one in-flight batch per
  concurrent stream (documented), verified under real caps.
- Every command wrapped/capped; fmt clean.

## Success Criteria

- G1: harness join scenarios at SF=100-class pass under a 1G cap (both
  levers, both orientations, SEMI + ANTI); Q4 SF=100 native @64M under an
  8G cap, cell-exact.
- G2: Q9 SF=100 parquet @1G ≤ 300s cell-exact; the 1G sweep total drops
  accordingly; no query slower than before by more than noise.
- G3: SF=100 sweeps reproduce 2026-09-03's verdicts (parquet 22/22 at 1G,
  20/22 + 2 named refusals at 256M; native 22/22 at 100G, 17/22 + 5 at
  1G); chaos ≥300/300, 0 mismatches; suites green; M1/M2 PASS.
- G4: CLAUDE.md's "probe side materialized" boundary rewritten as closed;
  the certification note updated with the new numbers.

## Constraints & Assumptions

- Sole operator file: `src/physical/operators/spillable.rs` (join
  section). Harness knobs may gain a cap override.
- Streaming the output changes `execute_spill_path`'s shape; the
  memoized `BuildDecision` and the Drop-time cleanup must keep working
  for repeat executions (fused-agg fallback re-executes its input).

## Out of Scope

- Outer-join and ON-filter spill (`spill-boundaries` epic).
- Native-scan streaming for joins (`spill-boundaries`).
- The in-memory join.

## Dependencies

- `hash-join-dictionary-semi-anti-fix` merged first (test fixtures shared).
- SF=100 data + oracle from `.scratch/sjc3-005/oracle/` (recompute if stale).
