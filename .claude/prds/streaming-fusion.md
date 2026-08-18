---
name: streaming-fusion
description: Fuse final-join probes directly into aggregate accumulation to eliminate joined-batch materialization — the ~13s join-side residue at SF=100 (Q9/Q18/Q3/Q5/Q21)
status: completed
created: 2026-08-18T15:21:40Z
---

# PRD: streaming-fusion

## Executive Summary

After the duckdb-parity and radix-execution epics, SF=100 parquet sits
at 66.1s (0.9x DuckDB native, 1.65x like-for-like on identical parquet).
The remaining like-for-like gap is ~26s, of which ~13s is join-side:
Q9 +4.1s, Q18 +3.3s, Q3 +2.2s, Q5 +1.8s, Q21 +1.2s vs DuckDB on the
same files. HJ_PROF showed gather+batch dominating the probe pipeline
BEFORE join-output pruning; this epic starts by RE-attributing after it
(pruning changed the economics), then — if the evidence still points
there — fuses `Aggregate(Join)` shapes so the final join's probe feeds
thread-local aggregation states directly, skipping the joined-batch
write + the aggregate's separate re-read of it.

## Problem Statement

Today the final join of Q9/Q18-class queries materializes its full
output as RecordBatches (post-pruning: exactly the agg-referenced
columns), streams them through a channel, and the fused streaming
aggregate re-evaluates group/agg expressions over them. That is one
full write + one full read of ~600M rows × output width of memory
traffic, plus per-batch assembly, purely between two operators that
could share a loop. DuckDB's pipelines run probe→accumulate in one
pass.

Unknowns the epic must resolve FIRST (the radix lesson: measure before
rewrite):
1. Post-pruning HJ_PROF: how much of Q9/Q18/Q3/Q5/Q21 is still
   gather+batch vs probe vs upstream scans?
2. The theoretical ceiling: joined output bytes × 2 / effective
   bandwidth — is it ≥1.5s across the suite? If not, STOP.

## User Stories

**As the engine developer**, I want a fresh post-pruning attribution of
the five join-residue queries with HJ_PROF + AGG_TIMING, and a computed
traffic ceiling for fusion.
- AC: table in the epic dir; explicit GO/STOP verdict (STOP if ceiling
  < 1.5s suite-wide).

**As the engine developer**, I want `Aggregate(HashJoin)` fusion: when
the join is Inner/unfiltered and the aggregate is fused-streaming-
eligible, the probe's per-batch loop feeds thread-local
AggregationStates directly (group keys and agg inputs evaluated over a
thin non-materialized column view), merged by the existing
merge/finalize machinery.
- AC: opt-out via QE_FUSE_AGG=0; falls back cleanly for every
  non-matching shape; spill-safety unchanged (the aggregate's memory
  budget still enforced — fused states count toward it or fusion
  declines by group-count budget exactly like execute_fused_streaming).

**As the engine developer**, I want gates: Q9 ≤ 12.5s, Q18 ≤ 6.5s,
suite ≤ 62s at SF=100, no query regressed >5%, 22/22 cell-exact both
scales, lance inherits (warm A/Bs).

## Non-Functional Requirements

- Memory-safe always; 22/22 cell-exact SF=10 + SF=100 after every
  lever; suites green (default, IPC, lance); fmt before commit;
  commit-or-revert; heavy runs through scripts/oomsafe.sh (no
  OOMSAFE_MEMHIGH on measurements).

## Success Criteria

- G1: attribution + ceiling verdict recorded (kill-switch).
- G2: Q9 ≤ 12.5s, Q18 ≤ 6.5s, SF=100 suite ≤ 62s warm, 22/22
  cell-exact both scales.
- G3: SF=10 ≤ 7.8s; lance warm A/Bs inherit.
- Stretch: suite ≤ 58s (≤1.45x like-for-like).

## Constraints & Assumptions

- The fused streaming aggregate (execute_fused_streaming), its
  worker/merge machinery, AggregationState, and the join-output pruning
  masks are the building blocks — no parallel operator stack (round-35
  lesson: fuse INTO the probe or don't bother).
- Scan-side residues (Q1/Q16 decode) are OUT of scope.

## Out of Scope

- Scan/decode work, IPC lifecycle, distributed M3, window functions.
- Fusing outer/filtered joins.

## Dependencies

- HJ_PROF/AGG_TIMING/QE_AGG_PROF diagnostics, join-output pruning,
  execute_fused_streaming, scripts/oomsafe.sh.
