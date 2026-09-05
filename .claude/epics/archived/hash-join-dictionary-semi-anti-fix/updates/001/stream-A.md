---
issue: 001
stream: fix-and-pin
started: 2026-09-05T01:39:23Z
status: completed
---
## Scope
Failing test first, then the marking-loop fix in hash_join.rs, un-ignore
the findings fixture, SQL-level native-vs-parquet test.

## Progress
- Starting

## 2026-09-05T01:45Z — read-through, root cause confirmed from code before any test

- `probe_semi_anti_parallel` (hash_join.rs ~2235-2520): all three marking
  sites in the generic entry loop (compiled filter / expression filter /
  no filter) `break` after the FIRST marked entry regardless of `swapped`.
  Correct when `swapped` (probe row is the output; its status is decided);
  wrong when `!swapped` (build rows are the output; every build entry with
  the key must be marked). SIBLING in the same function: the VHT
  `for_each_i64_candidate` callback returns `pass` (= stop) — same defect
  for Int64 keys WITH a compiled ON filter and build-side output.
- Sequential `probe_hash_table` (~3430-3480) collects ALL candidates first
  and marks every `(build, probe)` pair — clean. `probe_vectorized`'s
  `probe_one_semi_anti_batch` marks every candidate — clean; its sequential
  Semi/Anti arm delegates to the same closure — clean.
- Why Dictionary keys reach the buggy path: `VectorizedHashTable::build`
  fails `can_vectorize_arrays` (no Dictionary arm) -> `vectorized_ht=None`
  -> generic `build_hash_table` (extract_join_key has a Dictionary arm) ->
  `probe_hash_table` skips `probe_vectorized` -> `probe_semi_anti_parallel`
  generic map path.
- Latent hazard noted for task 002 (NOT touched yet): with a VHT present the
  generic map is skipped (`hash_table = HashMap::new()` at ~1235) and the
  i64 map is None for non-integer keys, so a FILTERED Semi/Anti over
  Utf8 keys lands in `probe_semi_anti_parallel` with nothing to look up.
  Will probe empirically with a Utf8+filter test in this task's audit.

## 2026-09-05T02:05Z — PRE-FIX numbers observed (HEAD 7fdc9a1 + tests only; `.scratch/hjdict/prefix_run.log`)

Three new tests in hash_join.rs's module, all FAIL pre-fix, 3/3
(`cargo test --release --lib hash_join::tests::semi_anti_ -- --nocapture`):

| fixture (60k build / 40 keys vs 2k probe / 20 keys) | jt | build_right | got | truth |
|---|---|---|---|---|
| Dictionary(Int32,Utf8), no filter | Semi | false | **20** | 30,000 |
| Dictionary(Int32,Utf8), no filter | Anti | false | **59,980** | 30,000 |
| Dictionary(Int32,Utf8), no filter | Semi/Anti | true | 30,000 | 30,000 |
| Utf8 control, no filter | Semi/Anti | both | 30,000 | 30,000 |
| Int64 + compiled filter `lp > rp` | Semi | false | **20** | 30,000 |
| Int64 + compiled filter `lp > rp` | Anti | false | **59,980** | 30,000 |
| Int64 + compiled filter `lp > rp` | Semi/Anti | true | 30,000 | 30,000 |
| Int64 + expression filter `lp > -1` | Semi | BOTH | **0** | 30,000 |
| Int64 + expression filter `lp > -1` | Anti | BOTH | **60,000** | 30,000 |
| Utf8 + filter `lp >= 0` | Semi | BOTH | **0** | 30,000 |
| Utf8 + filter `lp >= 0` | Anti | BOTH | **60,000** | 30,000 |

- Rows 1-2: THE pinned defect, exactly as the PRD predicted (20 = one per
  distinct matched key; 59,980 = 60,000 - 20).
- Rows 5-6: the `for_each_i64_candidate` sibling confirmed (callback stops
  the walk on the first passing candidate regardless of `swapped`).
- Rows 8-9 and 10-11: two further Semi/Anti-with-ON-filter findings in the
  same function, BOTH orientations, where NO row is ever matched. Being
  root-caused next (suspects: the expression-filter single-row batch path;
  and the empty generic map when a VHT exists but the keys are strings).

## 2026-09-05T02:40Z — fix applied, everything green (`.scratch/hjdict/postfix_run2.log`)

Root cause of ALL four pre-fix rows, read from the code and confirmed by
the tests flipping to green:

1. **The pinned defect**: `probe_semi_anti_parallel`'s marking sites
   `break`-ed after the first marked entry regardless of `swapped`
   (and the `for_each_i64_candidate` callback returned `pass` = stop).
   Fix: one shared `consider(bb, br, pr)` closure applies the ON filter
   (compiled / expression / none) and marks the OUTPUT side; every walk
   stops only on `pass && swapped`. Doc comment at the site names the
   mechanism.
2. **Filtered Semi/Anti matched NOTHING** (Utf8 keys any filter; Int64
   keys with a non-compilable filter; both orientations): `execute()`
   skips the generic map whenever a VHT exists, and the function's
   "local i64 safety net" was built over the BUILD batches with the
   PROBE key expression (`build_i64_hash_table(build_batches,
   &probe_key_exprs[0])`) — `None` whenever the key names differ. Fix:
   removed that map; when neither i64 path serves a batch and a VHT
   exists, candidates come from `vht.probe_batch` (source 3 in the new
   precedence comment) with the filter applied per pair. This is ALSO the
   path Dictionary keys will need once task 002 gives them a VHT.
3. **Swapped Semi/Anti emission over IPC-sidecar parquet** errored with
   "column types must match schema types, expected Utf8 but found
   Dictionary" (surfaced by the SQL test's build-RIGHT control over
   `lineitem_src`): the probe batch was emitted under the declared output
   schema. Fix: all four Semi/Anti emission sites (3 swapped-probe, plus
   `create_semi_anti_batch`) go through the existing
   `batch_with_actual_types`, like every other emission in the file.

Post-fix: hash_join 14/14 (3 new), spillable::tests 31/31 (findings
fixture renamed `in_memory_hash_join_dictionary_build_side_semi_anti_is_exact`,
un-ignored, hard assertion), `tests/native_dictionary_semi_anti.rs` 4/4,
spill_tests 12/12. fmt clean.

SQL-level test notes: the native provider's DECLARED type is deliberately
Utf8 and `sql()` decodes output, so the Dictionary witness executes the
planned Semi/Anti join's LEFT child and inspects the arrays it emits (the
operator's real input). Orientation is asserted from the plan via a new
`build_right` field in `SpillableHashJoinExec`/`HashJoinExec` Debug
output (the only non-test change outside hash_join.rs). Build-left
shapes: filters of <3 conjuncts on both sides (flat 0.3 estimate each);
build-right controls: unfiltered outer vs filtered subquery.

Pre-fix timing binary for task 002 pinned: detached worktree
`.scratch/hjdict/wt` @ 7fdc9a1, own `CARGO_TARGET_DIR`
(`.scratch/hjdict/wt-target`, 3m44s) -> `.scratch/hjdict/bin/query_engine_7fdc9a1`.
