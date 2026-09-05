---
name: hash-join-dictionary-semi-anti-fix
status: completed
created: 2026-09-05T01:36:45Z
updated: 2026-09-05T03:03:39Z
progress: 100%
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
- [x] 001.md - Confirm root cause, fix build-side SEMI/ANTI marking, pin at operator and SQL level (parallel: false)
- [x] 002.md - Dictionary keys on the vectorized hash-table path (parallel: false, after 001)
- [x] 003.md - Verification + CLAUDE.md + epic close-out (parallel: false, last)

Total tasks: 3
Parallel tasks: 0
Sequential tasks: 3
Estimated total effort: 8 hours

## Close-out (2026-09-05)

Commits: 7fdc9a1 (start), bb6e557 (001), 0c04da9 (002), b13fecf (001/002
close), plus this close-out. Evidence: `001.md`/`002.md`/`003.md`
Outcomes, `updates/00N/stream-A.md`, `.scratch/hjdict/`.

- **G1 MET**: the task-004 fixture passes un-ignored (SEMI 30,000 / ANTI
  30,000 for Dictionary and Utf8); the SQL-level native-vs-parquet test
  (`tests/native_dictionary_semi_anti.rs`, 4/4) is cell-exact in both
  build orientations with the Dictionary encoding asserted on the join
  input. Pre-fix failing numbers observed first: 20 / 59,980.
- **G2 MET**: Dictionary keys build a `VectorizedHashTable` (asserted);
  the SF=10 native Dictionary-keyed Semi join went from 2.60s (wrong
  answer, generic path) to 2.18s (correct, `vht=true`), back to back.
- **G3 MET**: suite 1337/0/1, spill_tests 12/12, SF=10 native 5,398ms in
  band, M1/M2 PASS, CLAUDE.md updated.
- **Beyond the PRD, fixed in the same function**: filtered Semi/Anti
  over Utf8 keys (and Int64 with a non-compilable filter) matched
  NOTHING; Int64 + compiled filter build-side stopped on the first pass;
  swapped Semi/Anti emission over IPC-sidecar parquet errored on
  Dictionary arrays; and the naive fix's quadratic build-side walk under
  duplicated keys (replaced by an O(probe + build) marker).
