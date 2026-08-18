---
name: streaming-fusion
status: completed
created: 2026-08-18T15:21:40Z
updated: 2026-08-18T15:21:40Z
progress: 100%
prd: .claude/prds/streaming-fusion.md
github: (will be set on sync)
---

# Epic: streaming-fusion

## Overview

Eliminate the write+read of final-join outputs for `Aggregate(Join)`
shapes: the probe's per-batch loop feeds thread-local AggregationStates
directly. Evidence-first with a kill-switch: task 001 re-attributes the
five join-residue queries POST-pruning and computes the traffic ceiling;
a ceiling under 1.5s stops the epic.

## Architecture Decisions

- **Fuse INTO the probe** (round-35 lesson). The planner marks a
  HashJoinExec with a `fused_agg: Option<FusedAggSpec>` when its ONLY
  consumer is a fused-streaming-eligible Aggregate; the join's execute()
  then RETURNS AGGREGATE PARTIALS (per-partition state batches) — no:
  simpler and safer — the SpillableHashAggregateExec keeps its role, but
  its fused drain hands each probe partition a SINK closure instead of a
  channel... DECISION DEFERRED to task 002's design note after 001's
  numbers; both variants must keep the aggregate's group-count budget
  and fallback (abort → re-execute materializing) semantics.
- **Thin evaluation**: group/agg expressions evaluate against a
  column-view batch assembled from Arc'd probe columns + gathered build
  columns — the same arrays create_joined_batch produces today, minus
  the RecordBatch shipping and the second read.
- **Kill-switch discipline**: 001's ceiling gates 002; every lever
  commit-or-revert with cell-exact validation (row counts are not
  answers).

## Task Breakdown Preview

- 001: Post-pruning attribution + fusion traffic ceiling (GO/STOP). [S]
- 002: Fusion design note + implementation behind QE_FUSE_AGG. [XL]
- 003: SF=100/SF=10 gates, lance warm A/Bs, default decision. [M]
- 004: QA close-out, docs, epic close. [M]

## Dependencies

001 → 002 → 003 → 004.

## Success Criteria (Technical)

G1 verdict; G2 Q9 ≤ 12.5s, Q18 ≤ 6.5s, suite ≤ 62s, cell-exact both
scales; G3 SF=10 ≤ 7.8s, lance inherits.

## Tasks Created
- [ ] 001.md - Post-pruning attribution + fusion ceiling (parallel: false)
- [ ] 002.md - Fusion implementation behind QE_FUSE_AGG (parallel: false)
- [ ] 003.md - Gates + default decision (parallel: false)
- [ ] 004.md - QA close-out (parallel: false)

Total tasks: 4 (sequential; kill-switch after 001)

## Epic close-out (2026-08-18)

001's re-attribution corrected the PRD's premise (aggregate expression
work only RELOCATES under fusion; the real removable cost was the
intermediate match/index vectors) and priced the ceiling at ~2.5-3.5s.
Lever A (direct u32 match emission) claimed ~1.8s of it: **SF=100
66.1→65.1s, Q9 13.6→12.1s (gate MET), Q9-lance 15.2→12.7s, everything
cell-exact, no regressions.** Lever B declined at the evidence. Suite
≤62s / Q18 ≤6.5s gates not met — both residues are at measured
bandwidth floors (PARITY-PLAN).

Session grand total (three epics, one day): **89.3 → 65.1s at SF=100
(−27%), 2.23x → 1.62x like-for-like, 0.97x vs DuckDB native.**
Commits: 609509d (+ scaffolding).
