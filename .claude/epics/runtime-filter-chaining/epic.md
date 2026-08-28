---
name: runtime-filter-chaining
status: in-progress
created: 2026-08-28T00:43:03Z
updated: 2026-08-28T02:40:00Z
progress: 67%
prd: .claude/prds/runtime-filter-chaining.md
github: (will be set on sync)
---

# Epic: runtime-filter-chaining

## Overview

The engine's one proven "runtime feedback changes in-flight execution"
mechanism (a hash join's build side publishes its real key set, an
already-in-flight probe-side scan prunes decode against it) is strictly
limited to one join → one directly-adjacent leaf. A dedicated research
pass found this is the single best next extension — reuses all existing
infrastructure, has a concrete measured example (Q7 SF=100: today's one
linked filter is worth ~150-200ms/11-13%, a second, independently
selective chain touching the same leaf is currently blocked) — ranked
ahead of two other real candidates (native-table probe sides, composite/
packed keys) that either need new infrastructure or have no measurable
standalone win yet.

## Architecture Decisions

- **Reuse the existing payload representation.** No new bitmap/set
  shape — `RuntimeFilterPayload` as it exists today is sufficient; the
  gap is purely in leaf-resolution/linking, not the filter's own data
  structure.
- **Additive, not a rewrite.** Every existing single-filter case must
  keep working identically. This PRD does not touch join-type,
  column-type, or probe-provider eligibility.
- **Implementation shape is the task's own call.** Either (a) resolve a
  leaf transitively through already-linked `HashJoinExec` nodes, or (b)
  let a leaf accept multiple AND-combined filter slots — task 001
  decides which fits the existing `SharedRuntimeFilter`/
  `RuntimeFilterConfig` plumbing better, based on what it finds reading
  the actual code, not assumed here.

## Technical Approach

### Chaining mechanism (task 001)
`src/physical/planner.rs`'s leaf-resolution logic currently unwraps only
`Project` nodes when finding a probe-side scan to link a filter to, and
keys `streaming_scans` by raw leaf pointer — so a leaf already consumed
into one join is invisible to a later, independently eligible join.
Extend this so a second (or later) eligible Inner join touching the same
logical leaf can still register/combine a filter.

### Validation (task 002)
Real before/after measurement on Q7 at SF=100 (the grounding example),
cell-exact correctness across affected queries, no regression to
existing single-filter cases (Q19, Q21, Q7/Q9's own first-touch
filters), full suite.

### QA close-out (task 003)
G1-G4 verdicts, docs, epic close.

## Task Breakdown Preview

- 001: Extend leaf-resolution/linking to chain or combine filters across
  multiple joins touching the same leaf (parallel: false, entry point)
- 002: Validation — real Q7 SF=100 measurement, cell-exact correctness,
  no-regression check (parallel: false, depends on 001)
- 003: QA close-out (parallel: false, depends on 002)

Total tasks: 3
Estimated total effort: S-M — the grounding research found this reuses
existing infrastructure end to end.

## Dependencies

- `src/physical/operators/hash_join.rs`, `src/storage/
  streaming_parquet_scan.rs`, `src/physical/planner.rs`.
- `scripts/claude-safe-build.sh` for every build.

## Success Criteria (Technical)

- G1: Q7 SF=100 measurably improves, real numbers reported.
- G2: cell-exact correctness preserved.
- G3: no regression to existing single-filter cases.
- G4: full suite green.

## Estimated Effort

- 001: S-M.
- 002: S.
- 003: S.

## Tasks Created
- [x] 001.md - Extend leaf-resolution/linking for multi-join chaining (parallel: false) — closed 2026-08-28
- [x] 002.md - Validation — real measurement, cell-exact, no-regression (parallel: false) — closed 2026-08-28
- [ ] 003.md - QA close-out (parallel: false)

Total tasks: 3
Parallel tasks: 0
Sequential tasks: 3
Estimated total effort: S-M

## Task 002 summary (honest, not a clean win)

Real measurement found G2 (cell-exact, both scale factors), G3 (no
regression to Q19/Q21) and G4 (full suite green, all 4 combinations)
cleanly MET. **G1 ("Q7 SF=100 measurably improves") is only partially
met**: task 001's second filter link is real (RT_DEBUG-confirmed), but
the direct before/after A/B (10 interleaved pairs) showed a small,
consistent SLOWDOWN (+1.8% avg, 10/10 pairs), and the `RT_DISABLE=1`
isolation of the whole mechanism was noisy enough (interleaved pairs:
+2.3% avg favoring the filter, 15/20 pairs; two 8-iteration block runs:
~5% favoring NO filter) that its sign could not be confidently pinned
down at the noise level available on this shared, heavily-loaded
machine this session. Full detail: `002.md`'s Outcome section. Task 003
(QA close-out) should weigh G1 as a genuine, small/inconclusive
performance result rather than an unambiguous win — the PRD's own
~150-200ms/11-13% grounding figure was for the ORIGINAL single filter,
not this epic's own second-filter addition.
