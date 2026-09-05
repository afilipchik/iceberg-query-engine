---
issue: 001
stream: spill-streaming
started: 2026-09-05T03:03:56Z
status: completed
---
## Scope
Tasks 001-003 as one stream (same file): probe streaming, output streaming, parallel read-back.

## Progress
- Starting

## 2026-09-05T03:20:00Z — read-in complete, pinned baseline build launched

- Read CLAUDE.md rules, PRD, epic, 001-003, sjc3 004/004-analysis/005 and
  stream-B, and the whole join section of `spillable.rs` (execute →
  compute_build_decision → execute_spill_path → probe_with_spilling →
  process_spilled_partition, the SEMI/ANTI helpers, Drop, tests).
- Baseline: detached worktree `.scratch/jss/wt` @ 5ea5e81 (pre-change
  HEAD), building `query_engine` + `--example oom_cap_harness` with its own
  `CARGO_TARGET_DIR=.scratch/jss/wt-target` under claude-safe-build.sh
  (48G/8 jobs) → will be copied to `.scratch/jss/bin/{query_engine,
  oom_cap_harness}_pre`. Edits to the main checkout cannot enter it.
- `.scratch/jss/q4_oracle.json` copied from sjc3-004 (run_q4.py needs it).
- Machine at start: 25G used / 100G available, 32 cores, no engine or
  harness processes running.
- Prior baseline numbers to beat (sjc3-005, 600M build / 300M probe /
  256MB budget, build_right=0 only, 12G cap): semi-join 5,285/5,289MB peak,
  5:41/5:54 wall; anti-join 8,069/7,957MB, 8:33/10:38 wall. Q9 SF=100
  parquet @1G: join_0_1 DONE total_matched=1,333,333,320 elapsed 1,396s.

### Design (before touching code)
- 001: `execute_spill_path` takes `stream_merge_input_partitions(probe)`
  directly; `probe_with_spilling` counts rows as they flow and the
  "probe collected" trace keeps its shape (now printed after the probe
  stream ends).
- 002: `BuildDecision::Spill(Arc<SpillState>)`; `SpillState` owns
  `partitions/tables/spilled/spill_dir` and removes the dir in ITS Drop
  (last Arc holder — operator or an in-flight producer — cleans up, so a
  producer can never read a directory the operator's Drop just removed).
  `execute_spill_path` spawns a producer task holding the Arc + a cloned
  `SpillJoinCtx` (keys, join type, schema, retained mask, threshold, pool)
  and returns a `ReceiverStream` over a bounded mpsc(8). Producer: phase A
  (probe batches → resident partitions probed + yielded, spilled-partition
  probe rows appended to PER-CALL probe files `probe_<call>_<idx>.parquet`
  so an abandoned earlier call can never share a writer with a repeat
  call), A' (build-side SEMI/ANTI resident emission), B (spilled
  partitions). `tx.is_closed()` polled per probe batch so an abandoned
  stream stops the producer early.
- 003: phase B drives up to K spilled partitions on `spawn_blocking`,
  chunk budget = threshold / K; read-back STREAMS both files row-group by
  row-group (build: chunk by chunk under the per-K budget, probe: re-opened
  per chunk) so a job's footprint is one chunk + one probe batch + bitmaps
  — never a whole partition file. K = min(available_parallelism, 8,
  spilled) further clamped by threshold / largest-partition-charge (a
  partition that is itself bigger than the budget gains nothing from
  K-way splitting: chunks × probe passes scale with K, so wall time is flat
  and CPU is K×); `QE_SPILL_JOIN_PARALLELISM` overrides.

## 2026-09-05T03:30:00Z — 001 code done, gates green, measurements running

- `execute_spill_path` now feeds `stream_merge_input_partitions(probe)`
  straight into `probe_with_spilling`, which counts probe rows as they
  flow (4th tuple element) — the "probe collected" trace keeps its shape
  and moves after the probe stream ends.
- Gates: `cargo fmt --check` clean; `spillable::tests` 31/31;
  `spill_tests` 12/12; `native_dictionary_semi_anti` 4/4.
- Pre-change Q4 SF=100 native @64M under MemoryMax=8G: **CELL-EXACT in
  61.6s** (spill_path_starts=1, hash_check_ok=122, mismatch 0, probe_rows
  452,534,946) — the brief's premise that pre-change is killed at 8G did
  NOT reproduce, so the Q4 comparison becomes a measured peak-RSS one
  (`run_q4.py` patched to print the serve process's VmHWM). Pre/post-001
  Q4 legs and pre/post-001 harness legs (600M build, both orientations,
  cgroup lever, 12G) launched.
