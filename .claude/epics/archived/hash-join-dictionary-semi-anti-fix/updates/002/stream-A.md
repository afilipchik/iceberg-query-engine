---
issue: 002
stream: dictionary-vht-fast-path
started: 2026-09-05T02:50:00Z
status: completed
---
## Scope
Dictionary(Int32, Utf8) join keys on the vectorized hash-table path:
decode ONCE per batch (`compute::cast` to the value type) at
`VectorizedHashTable::build` and at every probe-side key evaluation that
feeds the VHT; test that a Dictionary-keyed build yields a VHT;
cell-exact Inner/Semi/Anti/Left over Dictionary keys both orientations;
SF=10 native Dictionary-keyed Semi join timed pre-fix (7fdc9a1) vs 001
(bb6e557) vs 002, 3 runs each, same machine back to back.

## Progress
- 2026-09-05T02:50Z Starting after 001 (bb6e557). Pre-fix binary pinned
  at `.scratch/hjdict/bin/query_engine_7fdc9a1`; 001 binary building in a
  second detached worktree (`.scratch/hjdict/wt001`, own target dir).
- 2026-09-05T03:20Z Dictionary decode landed (`decode_dictionary_key` /
  `evaluate_join_keys` in hash_join.rs; used at `VectorizedHashTable::build`
  and every probe-side key evaluation: probe_vectorized x4,
  probe_semi_anti_parallel source 3/4, sequential probe_hash_table).
  `#[cfg(test)] HashJoinExec::build_used_vectorized_table()` accessor.
  hash_join lib tests 16/16 (new: `dictionary_keys_build_a_vectorized_hash_table`,
  `inner_semi_anti_left_over_dictionary_keys_are_cell_exact` — dict/dict,
  dict/utf8, utf8/dict; Inner/Semi/Anti/Left; both orientations; row-level
  vs naive truth).
- 2026-09-05T03:25Z **SF=10 finding**: pre-fix binary (7fdc9a1) answers
  the Dictionary-keyed build-left Semi in 3.89s with `n=1` per ship mode
  (the defect, in production shape: 30.6M-row build, vht=false,
  swapped=false). The 001 binary (correct marking, still generic path)
  did NOT finish in 10 minutes: with 7 distinct keys every probe row
  walks ~4.4M equal-key entries — O(probe x build/NDV) = ~1e14 visits.
  The unconditional `break` had been hiding a quadratic walk, and the
  vectorized path (`probe_one_semi_anti_batch` -> `probe_batch`) has the
  same shape for Utf8 keys, in BOTH orientations. Fix (this task):
  `VectorizedHashTable::mark_build_matches` — a probe row stops at the
  first already-marked equal entry (chains are always walked in order,
  bits are monotonic), O(probe + build); swapped uses first-match
  `probe_batch_semi`; the generic loops skip already-matched build rows
  (and stop the row when there is no filter). Guard test
  `build_side_semi_anti_over_heavily_duplicated_keys_is_linear`
  (400k x 400k over 5 keys, both encodings, 30s bound).
- 2026-09-05T03:55Z All green after the complexity fix: hash_join 17/17
  (linear guard: 400k x 400k over 5 keys in 6-13ms), spillable::tests
  31/31, native_dictionary_semi_anti 4/4, spill_tests 12/12. fmt clean.

## SF=10 native measurement (`.scratch/hjdict/time_semi.py`, back to back, quiet machine)

`serve --tables data/tpch-10gb-native --memory-limit 40G`, POST /sql,
`systemd-run --user --scope -p MemoryMax=48G`, `QE_MEM_CAP=44G`,
HJ_TIMING=1. Query (build = LEFT, 30,607,902-row Dictionary-keyed build):

    SELECT l_shipmode, COUNT(*) AS n, SUM(l_orderkey) AS k FROM lineitem
    WHERE l_quantity > 25 AND l_shipmode IN
      (SELECT l_shipmode FROM lineitem WHERE l_discount > 0.05)
    GROUP BY l_shipmode ORDER BY l_shipmode

| binary | path (`[hj]` line) | 3 runs | best | answer |
|---|---|---|---|---|
| 7fdc9a1 pre-fix | vht=false, generic map, swapped=false | 2.60 / 2.71 / 2.64s | **2.60s** | WRONG: n=1 per ship mode |
| bb6e557 (001 only) | generic map, correct marking | did not finish in 600s (sanity run, killed) | DNF | — |
| 002 (this task) | vht=true (VHT build 0.44-0.61s), swapped=false | 2.48 / 2.18 / 2.29s | **2.18s** | n = 4,371,617..4,373,956 per mode |

002 is not slower than the pre-fix generic path (2.18s vs 2.60s best,
~16% faster) AND correct. Oracle for the 002 answer, same binary: the
subquery yields all 7 ship modes, so the Semi must equal the plain
`... WHERE l_quantity > 25 GROUP BY l_shipmode` aggregate — byte-identical
CSV (`result_oracle_002.csv` == `result_timed_002.csv`).
Logs/CSVs: `.scratch/hjdict/serve_timed_*.log`, `result_timed_*.csv`.
