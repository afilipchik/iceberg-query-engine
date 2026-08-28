---
name: runtime-filter-chaining
description: Extend the runtime join-filter bitmap mechanism to propagate/combine across multiple joins touching the same probe-side leaf
status: backlog
created: 2026-08-28T00:43:03Z
---

# PRD: runtime-filter-chaining

## Executive Summary

The engine's one proven, working "runtime feedback changes in-flight
execution" mechanism — a hash join's build side publishes its real key
set, and an already-in-flight probe-side Parquet scan prunes decode work
against it — is real, measured, and safe, but strictly limited to "one
join, one directly-adjacent scan leaf." A research pass grounding the
engine's first genuine adaptive-execution investment (following the
adaptive-join-order-reoptimization go/no-go investigation, which found
full replanning not justified — see `CLAUDE.md`'s "Adaptive join-order
re-optimization" section) found the single best next extension: when a
fact-table leaf (e.g. `lineitem`) is touched by two or more independently
eligible Inner joins, only the FIRST one's filter ever gets applied — a
second, equally real, independently selective join chain is silently
left unfiltered. This is confirmed live on TPC-H Q7 at SF=100, where
today's one linked filter is already worth ~150-200ms (11-13% of total
query time), and a second, currently-blocked, nation-restricted chain
touching the same `lineitem` leaf is a real, measurable opportunity.

## Problem Statement

`src/physical/planner.rs`'s runtime-filter wiring resolves a probe-side
leaf by unwrapping `Project` nodes only (never descending through an
already-built `Join`), and keys `streaming_scans` by that leaf's raw
pointer. The instant a leaf is consumed into one `HashJoinExec`, every
later join touching the same logical table — even on a different,
equally eligible Inner column — sees `probe_leaf=SpillableHashJoin`/
`Filter` and gets no link at all. Confirmed live on Q7 (SF=100) and Q9
(SF=10/100) via `RT_DEBUG=1`.

## User Stories

**As someone running a multi-join TPC-H-style query where a fact table
is restricted from more than one direction**, I want every applicable
runtime filter to apply, not just the first one the planner happens to
wire up.
- Acceptance: a query with ≥2 independently eligible Inner joins against
  the same leaf gets filter treatment from all of them (AND-combined, or
  chained through already-linked joins — implementation's call), with a
  measured wall-time improvement on the real TPC-H Q7 (SF=100) case this
  PRD is grounded in.

## Functional Requirements

1. Extend the planner's leaf-resolution logic so a leaf already consumed
   into one `HashJoinExec` can still receive a filter from a LATER,
   independently eligible Inner join touching the same logical table —
   either by resolving the leaf transitively through already-linked join
   nodes, or by allowing a leaf to accept multiple, AND-combined filter
   slots. Implementation's call which shape fits the existing
   `SharedRuntimeFilter`/`RuntimeFilterConfig` plumbing better.
2. Reuse the existing bitmap/set payload representation
   (`RuntimeFilterPayload`) — this PRD does not require a new bitmap
   representation, per the grounding research's own finding.
3. Every existing single-filter case (today's behavior) must keep
   working identically — this is additive, not a rewrite of the
   mechanism's core semantics (Inner-only, build-stays-left Left/Semi/
   Anti, Int64 keys, Parquet-backed probe leaves — none of that changes
   in this PRD).

## Non-Functional Requirements

- **Cell-exact correctness always** — a runtime filter is a performance
  optimization only; the full predicate must always still be re-applied
  downstream, exactly as the existing mechanism already guarantees.
- **No regression** to the existing single-filter cases' own measured
  wins (Q19, Q21, and Q7/Q9's own first-touch filters).
- Every build through `scripts/claude-safe-build.sh`.

## Success Criteria

- G1: Q7 at SF=100 measurably improves with the second filter applied,
  real before/after numbers reported.
- G2: cell-exact correctness preserved across every affected query.
- G3: no regression to any existing single-filter case's measured value.
- G4: full suite green.

## Constraints & Assumptions

- Builds on `.claude/plans/research/2026-08-27-modern-olap-research-
  synthesis.md` and the runtime-filter-chaining research pass (this
  session, recorded in this epic's own research trail) — read before
  starting, don't re-derive the eligibility-gate enumeration.
- Scope is strictly the leaf-resolution/chaining gap. Column-type
  eligibility (Int64-only), join-type eligibility, and probe-provider
  eligibility (Parquet-only) are unchanged by this PRD.

## Out of Scope

- **Native-table probe sides** (a second, real, independently-valuable
  extension the same research identified) — needs a new lazy
  `NativeTableScanExec` operator (native tables are currently scanned
  eagerly at plan time, before any join's build side exists) and a
  `benchmark-native`-class multi-table TPC-H harness that doesn't exist
  yet to measure real value first. Worth its own, separately-scoped PRD
  once this one ships.
- **Composite/packed key eligibility** (`PackedJoinKeys`-produced keys)
  — no currently-measurable standalone TPC-H win found; its one live
  example (Q9) is dominated by this PRD's own chaining gap. Revisit
  after this ships, once a query shape needing it independently can be
  identified.
- Any change to the mechanism's existing eligibility gates (join type,
  column type, probe provider).

## Dependencies

- `src/physical/operators/hash_join.rs` (build-side publish),
  `src/storage/streaming_parquet_scan.rs` (probe-side consumption),
  `src/physical/planner.rs` (the leaf-resolution/linking logic this PRD
  changes).
- `scripts/claude-safe-build.sh` for every build.
