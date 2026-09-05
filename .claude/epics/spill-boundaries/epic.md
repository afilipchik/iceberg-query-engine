---
name: spill-boundaries
status: completed
created: 2026-09-05T01:39:08Z
updated: 2026-09-05T07:33:11Z
progress: 100%
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
- [x] 001.md - Spill-covered native scan routing (parallel: true)
- [x] 002.md - ON-clause filter on the join spill path (parallel: true, conflicts: 003)
- [x] 003.md - LEFT/RIGHT/FULL outer-join spill (parallel: false, after 002)
- [x] 004.md - Harness scenarios, SF=100 certification, docs, epic close-out (parallel: false, last)

Total tasks: 4
Parallel tasks: 2
Sequential tasks: 2
Estimated total effort: 21 hours + SF=100 machine time

## Close-out (2026-09-05)

Commits: 05b0213 (start), 84e2c62 (001), 8dab430 (002), 19b29c3 (003),
5ec9af1 (001 test follow-up), 8f38273 (001-003 close), plus the 004
close-out. Evidence: `00N.md` Outcomes, `updates/00N/`, `.scratch/sb/`.

- **G1 MET**: native SF=100 @1G **22/22 cell-exact** on the final binary
  (Q02/Q10/Q11/Q15/Q20 stream into their joins; all refused before).
- **G2 MET**: parquet SF=100 @256M **22/22 cell-exact** — Q20 through the
  LEFT-join spill path (54.9s) and Q21 through the filtered spill path
  (117.6s), real spill activity, 0 hash mismatches.
- **G3 MET**: 21 filtered + 31 outer-join fixtures vs a row-level naive
  oracle (both orientations, dense/sparse, NULL keys, filter, retained
  mask, K=3), planner cover/uncover tests, native_streaming_scan tests on
  the widened rule; harness `left-join` + `filtered-join` scenarios
  COMPLETED under the default 1G cap on both levers (18/18 with semi/anti)
  after the 003 follow-up bounded the gathered emissions (pre-fix 8/8
  clean refusals).
- **G4 MET**: chaos 300/300 (0 mismatch); suites default 1353/0/1,
  lance **1418/0/2**, gpu **1362/0/1**, pulsar **1356/0/1** (all `--no-fail-fast`, exit 0; identical to the pre-follow-up counts — the follow-up added no tests); M1/M2 PASS; timing legs SF=10 native **5,891 / 6,610ms** (22/22 OK) and parquet cache-off **7.52s then 7.24s** (22/22 PASS) on a machine under interactive load (load average 11-21 from the desktop shell, a browser and a peer session; the same native sweep measured 5,113ms at 8f38273 when the load was ~2.7) — an alternating A/B under identical load, pre-follow-up binary vs final binary, gave 6,163/6,113ms vs 6,226/5,916ms (within 2%, final not slower), so the band miss is load, not the code; INSERT RSS 1.57GB in band; chaos 300/300; M1/M2 PASS; CLAUDE.md updated
  (boundaries closed, re-certification table).
- Recorded honestly: the first post-fix harness run was killed at 1G on
  3 of 8 legs (1.0-1.36GB peaks) — root-caused with a 1s RSS sampler to a
  per-row `take`+`concat` gather in the outer/filtered emissions and
  fixed (one `interleave` per column + 8,192-row emission slices),
  not hidden behind a bigger cap; native @1G Q18 (aggregate spill) ran
  387-429s under shared load vs 230s on the quiet 09-03 machine.
