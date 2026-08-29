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

## Status

- [x] plan written
- [ ] code implemented
- [ ] unit tests
- [ ] repro validation under caps
- [ ] Q12 native re-check
- [ ] harness re-run
- [ ] chaos harness / spill_tests / suite
- [ ] Outcome appended
