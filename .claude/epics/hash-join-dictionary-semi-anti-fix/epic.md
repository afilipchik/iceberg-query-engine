---
name: hash-join-dictionary-semi-anti-fix
status: backlog
created: 2026-09-05T01:36:45Z
updated: 2026-09-05T01:36:45Z
progress: 0%
prd: .claude/prds/hash-join-dictionary-semi-anti-fix.md
github: (will be set on sync)
---

# Epic: hash-join-dictionary-semi-anti-fix

## Overview

One confirmed mechanism, one small fix, tests that would have caught it,
and the fast-path gap that let the buggy fallback be reached. Three
tasks, strictly sequential (same file).

## Architecture Decisions

1. **Confirm, then fix minimally.** Task 001 first writes the failing
   test against `HashJoinExec` directly, then removes the `break` for
   the `!swapped` case in `probe_semi_anti_parallel` (and any sibling
   path the audit finds). No refactor of the probe loop.
2. **Close the fallback gap, don't just patch the fallback.** Dictionary
   keys should take the vectorized path like every other supported key
   type: decode once per batch at VHT build and probe (`compute::cast`
   to the value type — same helper shape as spillable.rs's
   `join_key_arrays`). The generic path stays correct AND stays as the
   safety net.
3. **Test at two levels.** Operator-level (the existing pinned fixture,
   un-ignored) and SQL-level over a real native table with a Dictionary
   key, compared to the same SQL over parquet — the shape that reaches
   this code in production.

## Technical Approach

### Backend Services
- `src/physical/operators/hash_join.rs`: `probe_semi_anti_parallel`
  entry loop; `VectorizedHashTable::build` / `probe_batch` /
  `probe_batch_semi` key evaluation; `vectorized_hash::can_vectorize_arrays`.
- Tests: `spillable.rs` test module (fixture), `tests/` SQL-level test.

### Infrastructure
- Safe-build wrapper for every cargo command; SF=10 native sweep for the
  band check.

## Implementation Strategy

001 (fix + pin) → 002 (Dictionary fast path) → 003 (verification,
docs, close-out).

## Task Breakdown Preview

- [ ] 001: Confirm root cause with a failing test; fix the build-side
      marking loop; un-ignore the findings fixture; SQL-level native test
- [ ] 002: Dictionary keys on the vectorized hash-table path (build +
      probe decode), asserted + timed
- [ ] 003: Verification (suite, spill_tests, SF=10 band, M1/M2), CLAUDE.md
      update, epic close-out

## Dependencies

- Merged `spill-join-correctness-3` (main @ 978e974).

## Success Criteria (Technical)

PRD G1-G3.

## Estimated Effort

3 tasks, ~6-8 focused hours.

## Tasks Created
- [ ] 001.md - Confirm root cause, fix build-side SEMI/ANTI marking, pin at operator and SQL level (parallel: false)
- [ ] 002.md - Dictionary keys on the vectorized hash-table path (parallel: false, after 001)
- [ ] 003.md - Verification + CLAUDE.md + epic close-out (parallel: false, last)

Total tasks: 3
Parallel tasks: 0
Sequential tasks: 3
Estimated total effort: 8 hours
