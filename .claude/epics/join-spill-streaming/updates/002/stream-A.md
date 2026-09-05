---
issue: 002
stream: spill-streaming
started: 2026-09-05T03:45:00Z
status: in_progress
---
## Scope
Stream the join spill path's OUTPUT (phase A per probe batch, A' resident
build-side SEMI/ANTI emission, B per spilled partition/chunk) instead of
materializing it; repeat-execution semantics preserved.

## 2026-09-05T03:45:00Z — code done, unit/integration gates green

- `BuildDecision::Spill(Arc<SpillState>)`; `SpillState` owns
  partitions/tables/spilled/spill_dir and removes the directory in ITS
  `Drop` (last holder: operator or an in-flight producer) — the
  operator-level `Drop` is gone.
- `execute_spill_path` → START trace, `stream_merge_input_partitions(probe)`,
  a `tokio::spawn`ed producer (`run_spill_join_producer`) holding the Arc
  + a cloned `SpillJoinCtx`, bounded `mpsc(8)` → `ReceiverStream`. Phase A
  (`probe_with_spilling`, now a free async fn) sends resident-partition
  output per batch and writes PER-CALL probe files
  `probe_<call_id>_<idx>.parquet`; `tx.is_closed()` per probe batch stops
  an abandoned call early. Phase B (`process_spilled_partitions`) runs each
  `process_spilled_partition` (now sync, sink-based) on `spawn_blocking`
  with `blocking_send`. Probe files removed on every producer exit path.
  DONE trace moves to the end of the stream and carries `call_id`; an
  `ABANDONED` trace is new. Hash-check lines unchanged (shared
  `report_key_checksum`).
- Tests added: `spill_path_repeat_execution_yields_identical_results`
  (two full executions identical + spill dir survives between them, probe
  files gone after a drained call, dir removed on drop) and
  `spill_path_abandoned_stream_does_not_disturb_a_repeat_execution`
  (drop after one batch, then a full execution = ground truth; abandoned
  call's probe files cleaned up).
- Gates: `spillable::tests` 33/33; `spill_tests` 12/12;
  `native_dictionary_semi_anti` 4/4; fmt clean.
- Harness-under-1G, Q9@1G/16G and chaos legs: pending the release build.

## 2026-09-05T04:15:00Z — first 002 measurements

- Chaos (002 binary, tpch-10mb, seed 20260905): **100/100 passed**, 89
  genuine-disk trials, 0 disk-expected-but-missing, 25.7 ms/trial.
- Harness @1G on the 002 binary, `semi-join` build_right=0, cgroup lever:
  **FAIL — exit 143, memcg kill** (`harness_002_1G_br0/semi-join_cgroup.log`).
  Output streaming alone is not enough for this shape: phase B still
  `read_parquet`s the WHOLE build partition (600M/64 rows ≈ 150MB) and
  probe partition (≈ 75MB) into memory on top of the resident set
  (≤ 205MB) and the chunk table (≤ 205MB predicted) — task 003's
  streaming read-back removes exactly that. Recorded honestly as a 002
  miss; the 1G target is re-measured on the 003 binary. Remaining 002
  legs (anti br0, semi/anti br1, both levers) left running for the
  record.

## 2026-09-05T05:20:00Z — 002 harness @1G complete; Q9 on the 002 binary still running

- Harness @1G, 002 binary, 600M build / 256MB budget (cgroup / rlimit):
  build_right=0 semi-join **memcg-killed / PASS 1,068MB**; anti-join
  **memcg-killed / (see table in 002.md)**; build_right=1 semi-join
  **682 / 707MB PASS**, anti-join **691MB PASS** (cgroup) — see logs
  `harness_002_1G_br{0,1}/`. The build_right=0 (build = output) shape is
  the one that reads the largest partition files back whole (SEMI:
  150MB build + 75MB probe per partition on top of the resident set and
  the chunk table) — closed by task 003's streaming read-back (003:
  838/839MB PASS on the same legs).
- Q9 SF=100 parquet @1G under 16G on the 002 binary: second join DONE in
  665.7s (the streamed output is now consumed at the pace of the first
  join's single-threaded phase A — the pipeline effect task 003 fixes);
  first join still running; recorded in 002.md when it finishes.
