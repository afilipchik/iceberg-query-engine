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
