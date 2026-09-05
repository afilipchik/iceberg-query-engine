---
name: spill-boundaries
status: in-progress
created: 2026-09-05T01:39:08Z
updated: 2026-09-05T06:36:53Z
progress: 0%
prd: .claude/prds/spill-boundaries.md
github: (will be set on sync)
---

# Epic: spill-boundaries

## Overview

Three boundaries, three mechanisms, one certification. Planner routing
(native scans), per-candidate filter evaluation (ON clause), and
preserved-side match tracking with NULL-extended emission (outer joins).

## Architecture Decisions

1. **Native scans: a routing change, not a provider rewrite.** The
   streaming operator exists; the only question is which consumers may
   receive it. "Spill-covered" = every pipeline breaker between the scan
   and the root is spill-capable. Joins qualify only once the probe side
   streams (join-spill-streaming) — hence the ordering.
2. **Filter = a predicate on candidate pairs, evaluated where the pair
   is formed.** INNER filters the gathered joined batch; SEMI/ANTI/outer
   decide the match per pair before marking. One helper, used by all.
3. **Outer joins reuse the SEMI/ANTI bitmaps.** LEFT with build=right is
   SEMI-swapped's bitmap + NULL-extended emission of the unmatched probe
   rows; LEFT with build=left is SEMI-!swapped's bitmap + NULL-extended
   emission of unmatched build rows; RIGHT mirrors; FULL is both.
   Chunked read-back is exact for the same reasons documented in task
   004 of spill-join-correctness-3.
4. **Measure at the query that exposed each boundary** (Q02/Q10/Q11/Q15/
   Q20 native @1G; Q21 and Q20 parquet @256M) plus synthetic tests.

## Technical Approach

### Backend Services
- `planner.rs`: `collect_agg_covered_scans` → `collect_spill_covered_scans`.
- `spillable.rs`: filter evaluation helper on candidate pairs; outer
  join bitmaps + NULL-extended emitters; refusals removed.
- Harness: `left-join`, `filtered-join` scenarios.

### Infrastructure
- Same sweep drivers; DuckDB oracle.

## Implementation Strategy

001 native-scan routing (planner) ∥ 002 ON-filter spill (spillable.rs)
can run in parallel (different files); 003 outer-join spill after 002
(same file); 004 certification last.

## Task Breakdown Preview

- [ ] 001: Spill-covered native scan routing → native SF=100 @1G 22/22
- [ ] 002: ON-clause filter on the spill path → Q21 @256M
- [ ] 003: LEFT/RIGHT/FULL spill → Q20 @256M
- [ ] 004: Harness scenarios, SF=100 certification, docs, close-out

## Dependencies

- `join-spill-streaming` merged.

## Success Criteria (Technical)

PRD G1-G4.

## Estimated Effort

4 tasks, ~16-22 focused hours + SF=100 machine time.

## Tasks Created
- [ ] 001.md - Spill-covered native scan routing (parallel: true)
- [ ] 002.md - ON-clause filter on the join spill path (parallel: true, conflicts: 003)
- [ ] 003.md - LEFT/RIGHT/FULL outer-join spill (parallel: false, after 002)
- [ ] 004.md - Harness scenarios, SF=100 certification, docs, epic close-out (parallel: false, last)

Total tasks: 4
Parallel tasks: 2
Sequential tasks: 2
Estimated total effort: 21 hours + SF=100 machine time
