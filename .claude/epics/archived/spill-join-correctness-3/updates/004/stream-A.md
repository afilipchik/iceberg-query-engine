---
issue: 004
stream: operator-implementation
started: 2026-09-03T14:43:06Z
status: completed
---
## Scope
SEMI/ANTI spill support in `src/physical/operators/spillable.rs` (both
orientations), outer-join refusal message, unit + integration tests,
INNER non-regression, Q16/Q22 SF=10 check.

## Progress
- Starting implementation

## 2026-09-03T15:08:40Z — taken over by the coordinator session

- Both Stream A subagent launches (default model x3, Opus x1) died on
  server-side API 529 errors before writing a file; the coordinator is
  executing Stream A directly in the main session. Tree was clean at
  570a8cb at takeover.
- Design confirmed against code before editing (recorded in
  004-analysis.md facts 1-10): both orientations are needed; a partition
  is wholly resident or wholly on disk; two ANTI hazards (dropped probe
  batch for partitions with no build rows; build partitions with zero
  probe rows).
- Patch 1 applied (`.scratch/sjc3-004/streamA/patch1.py`): guard lifted
  for Semi/Anti with a new outer-join refusal message (still contains
  "INNER"); `probe_with_spilling` gains probe-side emission per batch
  (swapped), build-side bitmaps emitted after the probe loop (!swapped),
  and ANTI emission of probe batches whose partition has no build rows;
  `process_spilled_partition` gains a cross-chunk probe bitmap (swapped,
  emitted after the last chunk) and per-chunk build-side emission
  (!swapped); new helpers `join_key_arrays`, `probe_match_flags`,
  `accumulate_probe_matches`, `mark_build_matches`, `take_probe_rows`,
  `take_build_rows`.
- Patch 2 applied (`patch2_tests.py`): unit tests
  `semi_anti_spill_probe_side_output_is_cell_exact`,
  `semi_anti_spill_build_side_output_is_cell_exact`,
  `semi_anti_spill_dictionary_keys_are_cell_exact`,
  `outer_join_spill_is_still_refused_by_name`; each spill run is compared
  to the in-memory HashJoinExec delegate; chunked read-back asserted via
  the predicted-table-bytes > threshold precondition on the largest
  spilled partition.
- Build/test deferred until the Q4 SF=100 100G-limit probe (Stream B,
  110G cap) finishes, to keep total memory under the machine's 125G.

## 2026-09-03T15:34:04Z — unit-test iterations (runs 1-4, `.scratch/sjc3-004/streamA/unit_tests_run*.log`)

- Run 1: the four SEMI/ANTI orientation tests + outer refusal compiled;
  outer refusal passed; three failures, all "Failed to open parquet file
  .../build_N.parquet: No such file or directory" on the SPARSE fixtures,
  plus one degenerate fixture (my sparse keys 5/17/4000 are not multiples
  of 3, so the build-side SEMI baseline was empty).
- **Pre-existing spill-path bug found (not SEMI/ANTI-specific):**
  `evict_build_partition_to_disk` turned an EMPTY resident partition into
  a `SpilledPartition` whose file is only created by a later append. This
  happens when the first build batch alone exceeds the threshold
  (`find_largest_partition` runs while every partition is still empty).
  With dense keys every partition later receives rows, so the file gets
  created and nothing shows; with sparse keys ~60 partitions never do,
  and read-back fails. Fixed (patch3) by keeping an empty partition
  resident (the function's own doc comment already promised "no-op if
  empty"); pinned by
  `inner_spill_with_sparse_build_keys_survives_empty_partition_eviction`
  (INNER, both orientations, vs an independent naive-join ground truth).
- Run 2 (after patch3): all four SEMI/ANTI orientation/hazard tests PASS
  (dense + sparse, probe-side and build-side output, incl. the
  no-build-rows ANTI batch emission and zero-probe-rows partitions).
  Dictionary test: spill path returned 0 rows (in-memory 1,979) — the
  predicted `extract_join_key` gap (no Dictionary arm → every
  Dictionary key is `JoinValue::Null` → never inserted, never matched).
  **Pre-existing for INNER too.**
- Patch4: `join_key_arrays` decodes Dictionary keys (`compute::cast` to
  the value type, exactly as `partition_batch_by_group_hash` already does
  for the aggregate) and is now the single key-evaluation point for
  `build_hash_table`, `partition_batch_by_hash`, `probe_partition`,
  `batch_key_checksum` and all SEMI/ANTI probes. Non-Dictionary keys are
  evaluated exactly as before (routing byte-identical to what task 001
  recalibrated).
- Run 3/4: Dictionary keys now cell-exact on the spill path in both
  orientations (SEMI/ANTI) — but comparing against the in-memory
  delegate exposed TWO wrong-answer FINDINGS in `HashJoinExec`
  (hash_join.rs, out of this task's file scope), so patch5 switched every
  test to an independent `naive_join_count` ground truth (plain per-row
  key comparison, Dictionary decoded, NULL never matches):
  1. **INNER, 3 distinct build keys x ~13k duplicates, other side keys
     0..20000: in-memory returns 26,367 pairs; ground truth AND spill
     path 39,550** — exactly one key's pairs are missing, in BOTH
     orientations.
  2. **SEMI/ANTI with build = LEFT (output) side and Dictionary(Int32,
     Utf8) keys: in-memory SEMI returns 20 rows (one per distinct matched
     key) vs truth/spill 29,662; ANTI returns 59,980 vs truth/spill
     30,338.** Probe-side output (build = right) with the same Dictionary
     keys is correct in-memory.
  Both are pinned executable-but-`#[ignore]`d in
  `in_memory_hash_join_findings_from_task004_fixtures` (includes a
  duplicate-count sweep to bracket finding 1's onset) so the suite stays
  green on this task's spill-path claims while the findings remain
  visible. They need their own task; they are NOT masked by this one.
- Run 4 hung in my test helper (`render_join_row` re-cast the Dictionary
  column per output row; the INNER Dictionary case emits ~3M pairs) —
  patch6 decodes once per batch and shrinks that fixture; run 5 in flight.

## 2026-09-03T15:38:21Z — run 5 green; finding 1 RETRACTED; finding 2 sharpened

- Run 5: **30 passed / 0 failed / 1 ignored** in `spillable::tests`
  (`unit_tests_run5.log`) — every spill-path assertion vs the naive
  ground truth holds for SEMI/ANTI x {probe-side, build-side} x {dense,
  sparse}, Dictionary keys x {SEMI, ANTI, INNER} x both orientations,
  the empty-partition-eviction INNER pin, and the outer-join refusal.
- **Finding 1 (in-memory INNER losing one key's pairs) is RETRACTED — it
  was my test helper's bug, not the engine's.** INNER output is spread
  across the probe side's output partitions and `run_spillable_join`
  drained only partition 0; the ignored sweep's 1024/2048-row results
  (exactly one or two probe batches' worth) made the artifact obvious.
  SEMI/ANTI are single-partition, so every SEMI/ANTI comparison was
  complete. Patch7 drains all partitions and the INNER in-memory
  comparison is a hard assertion again.
- **Finding 2 stands and is now exact:** in-memory build-side (build =
  LEFT = output) SEMI/ANTI with Dictionary(Int32,Utf8) keys marks ONE
  build row per distinct matched key: SEMI 20 (truth 30,000), ANTI 59,980
  = 60,000 - 20 (truth 30,000). Probe-side output with the same keys is
  correct in-memory. The ignored test now also runs the same fixture with
  plain Utf8 keys as a control. hash_join.rs is out of this task's scope;
  this needs its own task.
- `cargo fmt --all` applied; run 6 + findings run 2 + `tests/spill_tests`
  in flight.

## 2026-09-03T15:50:33Z — committed 67f7cea; INNER non-regression chaos battery on the post-fix binary

- Run 6 (after the partition-drain fix): 30 passed / 0 failed / 1 ignored;
  `tests/spill_tests`: **12/12** incl. the three new SEMI/ANTI queries
  (EXISTS, NOT EXISTS, NOT IN at 8KB, cell-exact vs in-memory). Findings
  run 2: Dictionary build-side SEMI 20 / ANTI 59,980 vs truth 30,000;
  **Utf8 control correct (30,000 / 30,000)** — Dictionary-specific.
- Committed: 67f7cea (operator + tests), d378744 (harness scenarios).
- Chaos harness on `.scratch/sjc3-004/bin/spill_chaos_harness_fixed`
  (built from 67f7cea): batch A 200 trials @seed 20260903 tpch-10mb —
  **200/200 pass, 177 genuine-disk, 0 missed injection, 15,790
  hash-check-ok / 0 HASH-MISMATCH**; batch B 100 trials @seed 777
  tpch-100mb — **100/100, 92 genuine-disk, 9,398 hash-check-ok / 0
  mismatch**. Both under MemoryMax=8G / QE_MEM_CAP=6G.
  Logs: `.scratch/sjc3-004/streamA/chaos/batch_{A,B}.log`.

## Stream A result (2026-09-03T18:54:09Z)

See `004.md` "Outcome" for the consolidated evidence table. Status:
completed; all deliverables of this stream landed in 67f7cea / d378744
and the close-out commit.
