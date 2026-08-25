---
issue: 002
stream: main
started: 2026-08-24T23:00:00Z
status: completed
---

## Scope
See .claude/epics/spill-join-correctness/002.md

## Context gathered
- Read 002.md, epic.md's "Re-scope after task 001" section, 001.md's
  Outcome ("A separate, real finding along the way"), and
  updates/001/stream-A.md in full before writing any code.
- Read `src/physical/operators/spillable.rs` in full: `append_to_parquet`
  (the O(n^2) function), `build_with_partitioning`/`probe_with_spilling`
  (its two call sites), `process_spilled_partition`/`read_parquet` (the
  read-back path), and the sibling `merge_parquet_files` function used by
  the AGGREGATE spill path (a different function, deliberately untouched
  — task 001's finding and this task's scope are both specifically about
  `append_to_parquet`, which greps to exactly two call sites, both in the
  join build/probe spill path).
- Confirmed via repo-wide grep: `append_to_parquet` had exactly 2 call
  sites (both inside `SpillableHashJoinExec`), so removing it entirely
  and replacing both sites was safe and complete.

## Root cause (from task 001, re-confirmed by reading the code directly)

`append_to_parquet(path, batch)` on every call: opened the existing file,
read its schema, streamed EVERY existing row group into a brand new temp
file, wrote the ONE new batch, closed, then renamed the temp file over the
original. Cost per call was O(bytes already spilled for that partition).
With 64 hash partitions spilling almost immediately and hundreds of build
batches (plus the full probe side) appended progressively, the total cost
across a spill was O(n^2) in bytes read+written.

## Fix

Replaced the read-rewrite-rename-per-append pattern with a single
`ArrowWriter<File>` kept OPEN per partition for the whole build (or probe)
phase, appending each new batch as one more row group
(`append_batch_streaming`), closed exactly once when the phase's loop
ends (`close_spill_writers`). Cost per append is now O(batch), not O(prior
file size). The on-disk SHAPE is unchanged — still exactly one Parquet
file per partition, still one row group per appended batch — so
`process_spilled_partition`/`read_parquet` needed NO changes at all. This
is the "(b) true streaming Parquet writer" option from the task's own
technical details, chosen over "(a) many small files merged at read time"
specifically because it required zero changes to the read-back path,
matching the "least disruption" instruction explicitly.

`append_to_parquet` (the old function) was deleted entirely rather than
kept dead — confirmed via repo-wide grep it had no other callers.

Sole file touched: `src/physical/operators/spillable.rs` (matches the
task's own scope statement).

## Tests added

Two new unit tests in `spillable.rs`'s own test module:
- `append_batch_streaming_preserves_all_rows_across_many_appends`:
  correctness — 50 batches (37 rows each, deliberately not a round
  number) appended through one shared writer slot, read back, asserts
  every row survives in write order.
- `append_batch_streaming_cost_does_not_grow_with_prior_data`: direct
  evidence for the acceptance criterion's own language ("cost per append
  does not grow with the total data already spilled"). Times append #2
  vs append #301 through the same writer/file and asserts the late append
  is not dramatically more expensive (generous 20x + 25ms fudge factor to
  avoid flaking on scheduler jitter, while still failing clearly if the
  old O(n)-per-append cost regressed back in, which would make append
  #301 roughly 150x append #2's cost).

Both pass; both fast (~0.17s combined with the other 6 pre-existing tests
in this file's module).

## Wall-clock validation (task 001's exact repro,
`.scratch/spill_join_repro/repro.sh`, still present, reused unchanged)

Built `target/release/query_engine` with the fix
(`scripts/claude-safe-build.sh cargo build --release`), then ran the
saved repro script repeatedly against the pristine `data/tpch-10gb-native`
warehouse, `--memory-limit 40G`, exactly as task 001 did.

**Before (task 001's own recorded numbers, re-cited here, not
re-measured — see 001's Outcome/stream-A.md for the full 21-run record):
140-291s per run, 21/21 uniformly slow.**

**After (this task, 30 fresh-process trials, ports 18701-18730, run
sequentially): 30/30 correct. HTTP query wall time: min 3s, max 4s, avg
3.30s. Full script wall time (incl. ~1-2s server startup/readiness):
3-5s per trial.** Consistent, not run-to-run variable in any meaningful
way — every one of the 30 trials landed in the same narrow 3-5s band.

That is a ~40-90x speedup (141s/3.3s ~= 43x against task 001's "normal
correct run" baseline; 291s/3.3s ~= 88x against its one recorded slow/
wrong run) on the exact repro that motivated this task.

Also ran 10 additional WARM trials (one persistent server process,
repeated queries — task 001's own methodology included both fresh and
warm trials to rule out a per-process effect): 10/10 correct, 4-6s each
(the test suite was compiling concurrently in the background during this
batch, which plausibly explains the slightly wider 4-6s vs the isolated
3-4s band from the fresh-process batch — still nowhere near the old
140-291s regime).

**Total: 40/40 trials correct across this task's own validation.**

## Correctness / wrong-answer-bug status (explicit, per the task's own instruction)

Per this epic's own scope boundary, the wrong-answer bug (~4.8%, 1/21,
per task 001) was NOT investigated or touched by this task — this fix
targets only the O(n^2) append pattern, an independently-confirmed,
separate root cause. 0/40 of this task's own trials reproduced a wrong
answer. This does NOT mean the bug is fixed or gone: at an unchanged true
4.8% rate, the probability of observing zero wrong answers across 40
independent trials is 0.952^40 ~= 13.9% -- a real, non-negligible chance,
so 0/40 is fully consistent with "unaffected." Stated as instructed: the
wrong-answer bug was not observed in this task's 40 trials; it is not
claimed to be fixed, reduced, or worsened by this change, only that this
task's own change does not touch any code path relevant to its
(unconfirmed) mechanism -- `append_batch_streaming`/`close_spill_writers`
change how and when bytes are written to a spill file's underlying
`ArrowWriter`, not what rows are computed, matched, partitioned, or
counted.

## Full suite, all four feature combinations (final)

Ran `scripts/claude-safe-build.sh cargo test --release` (default),
`--features lance` (with `PROTOC=.scratch/tools/protoc/bin/protoc`),
`--features gpu` (with `LD_LIBRARY_PATH=$PWD/.venv/lib/python3.12/
site-packages/nvidia/cuda_nvrtc/lib`), `--features pulsar`. Cargo
serializes builds sharing one `target/` directory (each later one
printed "Blocking waiting for file lock on artifact directory" until
the previous one finished), so total wall time for all four was long
(each full-feature `cargo test --release` compiles the lib, all 17
integration test files, AND all 33 example binaries under fat LTO --
confirmed empirically, not just asserted, by watching `rustc
--crate-name <example>` invocations in `ps aux` throughout each build)
-- default alone took 9m40s just to finish compiling before any test
ran. All four completed with ZERO failures:

| combination | passed | failed | ignored | prior baseline (native-tables-mutation task 006) |
|---|---|---|---|---|
| default | 1190 | 0 | 1 | 1188/0/1 |
| lance | 1255 | 0 | 2 | 1253/0/2 |
| gpu | 1190 | 0 | 1 | 1188/0/1 |
| pulsar | 1193 | 0 | 1 | 1191/0/1 |

Every single combination is EXACTLY +2 passed over its own recorded
prior baseline -- precisely this task's own two new tests
(`append_batch_streaming_preserves_all_rows_across_many_appends`,
`append_batch_streaming_cost_does_not_grow_with_prior_data`), with zero
regressions and zero unexplained drift anywhere. `cargo fmt --all --
--check` clean throughout (one self-caught formatting issue in the new
cost test, fixed before the first suite run completed).

Also directly confirmed before the full-suite runs: `cargo test
--release --lib physical::operators::spillable` (8/8, including both
new tests and all three native-tables-mutation task 006 regression
tests) and `cargo test --release --test spill_tests` (7/7, the join
spill path's own end-to-end integration coverage) -- both green in
isolation, well before the ~35-minute full four-combination sweep
confirmed the same at full scale.

## Final summary

Root cause confirmed by direct code reading (matched task 001's
finding exactly): `append_to_parquet` read-rewrote-renamed the ENTIRE
spill file on every append. Fixed by keeping one `ArrowWriter<File>`
open per partition for the whole build/probe phase, appending each new
batch as a row group, closing once at the end
(`append_batch_streaming`/`close_spill_writers`). On-disk shape
unchanged, so the read-back path needed no changes. Task 001's repro:
140-291s -> 3-5s (30 fresh + 10 warm trials, all 40 correct, 0 wrong --
reported honestly as consistent-with-unaffected given the wrong-answer
bug's own 4.8% base rate, not as proof of a fix). Full suite green in
all four feature combinations, exactly +2 (this task's own tests) over
the prior baseline everywhere, zero regressions. Sole file changed:
`src/physical/operators/spillable.rs`.
