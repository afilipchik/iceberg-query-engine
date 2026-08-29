# oom-safety-hardening task 007 — stream C progress

Agent: stream C (join spill path hash-table budgeting). Branch: epic/spill-size-estimate-fix.

## 2026-08-29 — session start

Read: 007.md, 001.md Outcome, updates/001/stream-A.md, spillable.rs
(compute_build_decision / finish_via_spill / build_with_partitioning /
execute_spill_path / probe_with_spilling / process_spilled_partition /
build_hash_table / estimate_batch_size / BuildDecision / evict_build_
partition_to_disk), both repros, ExecutionConfig (spill_threshold=0.8).

## Chosen mechanism (direct accounting + partition eviction)

Three coordinated changes, all inside the SPILL path only (the phase-1
InMemory/Spill crossing check is deliberately UNCHANGED — the in-memory
fast path's own denser HashJoinExec/RowStore table is the
already-ruled-in shape 001 named out of scope, and changing the crossing
point would alter when every join in the engine spills):

1. **Hash-table budgeting pass** at the end of `build_with_partitioning`
   (while `partitions`/`spilled`/`spill_writers` are still mutable, before
   `close_spill_writers`): for each still-resident partition, largest
   first, (a) a conservative PREDICTED table cost
   (`predicted_hash_table_bytes(rows, key_cols)` — documented per-row
   constants: 56B amortized hashbrown bucket + key-values Vec + entries
   Vec + 16B/alloc overhead ≈ 136B/row for a 1-column key) gates whether
   the table is even built; (b) if built, its REAL footprint is measured
   directly (`hash_table_memory_bytes` — walks capacity + per-key Vec
   capacities + String heap bytes) and added to a running total that
   already includes all resident batch bytes. Crossing
   `memory_limit * spill_threshold` ⇒ the partition is EVICTED to disk
   via the existing `evict_build_partition_to_disk` (same writer, same
   checksum path — zero new spill-write logic). Surviving tables are
   memoized in `BuildDecision::Spill { tables, .. }` so repeat
   `execute_spill_path` calls reuse instead of rebuilding — the resident
   set (batches + tables) is ≤ threshold by construction, for the
   operator's whole lifetime.

2. `execute_spill_path`'s unbudgeted in-memory table loop (old ~line 871)
   is DELETED — it borrows the memoized, already-budgeted tables.

3. `process_spilled_partition` (old ~line 1240) builds its read-back
   table in CHUNKS of whole batches sized so
   `predicted_hash_table_bytes(chunk_rows) <= memory_threshold`, probing
   the full probe file per chunk and dropping each chunk's table before
   the next. INNER-only (the only join type the spill path accepts —
   `finish_via_spill` refuses everything else), so chunking the build
   side and unioning matches is exactly equivalent: build rows are
   disjoint across chunks, each (build,probe) pair emitted exactly once.
   Probe routing / probe_partition internals untouched (hard boundary).

Documented residual constants (the "budget + constant" model): one
read-back partition's batch bytes (~build_bytes/64), one chunk table
(≤ threshold), the probe-side full collection in execute_spill_path
(pre-existing, tasks 002/003's streaming-reservation territory), and
accumulated results.

## Validation evidence (2026-08-29)

All runs wrapped (systemd-run scopes / claude-safe-build.sh); logs in
`.scratch/oom007/`.

- **Control repro** (`_control_int32_repro`, 500MB limit, 3G memcg cap,
  `sjctrl007` scope): exit 0, `RESULT: PASS`, matched_rows=3/3,
  **peak RSS 555MB** (was: kernel-OOM-killed at 3G, ≥7.3GB plateau
  uncapped — a 15-24x overshoot; now ~1.1x the 500MB limit).
  Trace: `budget_partition_hash_tables resident_partitions=1
  table_bytes=202333672 batch_bytes=11476422 running_total=213810094
  threshold=419430400 evicted_for_table_budget=2` — one partition kept
  with a directly-measured 202MB table, total 214MB ≤ 400MB threshold.
  16.3s wall. Log: `ctrl_postfix.log`.
- **Dict repro** (`spill_dictionary_oversized_build_repro`, 30MB limit,
  2G cap, `sjdict007` scope): exit 0, `RESULT: PASS`, **peak RSS 106MB**
  (was 738MB — now budget + the documented constants: ≤24MB chunk table
  + ~11MB read-back batches + runtime baseline). All 64 partitions
  spilled; read-back tables built in 5 chunks/partition under the 24MB
  budget. 11.9s wall. Log: `dict_postfix.log`.
- **Profiler after-evidence** (dict repro, `QE_ALLOC_PROFILE=1`, 2G cap):
  allocator `peak=69.8MB` (was 640.4MB), and the ONLY live >=256KB
  allocation at peak is ONE 12.3MB hashbrown table at
  `build_hash_table <- execute_spill_path` — the bounded chunk table,
  vs 343MB across 4 unbounded tables at the same site before. RSS 140MB
  profiled. Log: `dictprof_postfix.log`.
- **spill_tests**: 7/7 green.
- **spillable unit tests**: 19/19 green (4 new: sizing amplification
  band, conservative duplicate-key prediction, end-to-end budget
  invariant asserted on the memoized decision, chunk-straddling
  duplicate-key cell-exactness).
- **Chaos harness**: 100 trials, 100 passed, 93 genuine disk-spill
  trials, 0 disk-expected-but-missing. Log: `chaos_postfix.log`.

## Status

- [x] plan written
- [x] code implemented (commits 9565685, 50959e7)
- [x] unit tests
- [x] repro validation under caps
- [ ] Q12 native re-check
- [ ] harness re-run
- [x] chaos harness / spill_tests
- [ ] default suite
- [ ] Outcome appended
