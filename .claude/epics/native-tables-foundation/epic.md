---
name: native-tables-foundation
status: in-progress
created: 2026-08-23T07:42:14Z
updated: 2026-08-23T19:17:32Z
progress: 63%
prd: .claude/prds/native-tables.md
github: (will be set on sync)
---

# Epic: native-tables-foundation

## Overview

Phase 1 of the `native-tables` program (see the PRD for the full four-phase
vision: foundation → mutation → GPU/RAM/disk tiering → materialized
rollups). This epic generalizes the engine's existing IPC sidecar cache
(`src/storage/ipc_cache.rs`) — already measured faster than DuckDB's own
native storage at SF=100 (0.72x, 22/22 cell-exact, three completed epics
of hardening) — from an opportunistic read-through cache tied to
shadowing a specific parquet file into a first-class, independently
persistent, writable, user-creatable table type. Deliberately NO mutation
(INSERT/UPDATE/DELETE) in this epic — bulk-load/replace only. That is
phase 2, sequenced after this format is proven, matching the "smallest
safe slice first" discipline every completed epic in this repo has used.

## Architecture Decisions

- **Reuse the sidecar's proven read mechanism wholesale; don't reinvent
  it.** mmap zero-copy via `Buffer::from_custom_allocation`, dictionary
  coercion for low-cardinality strings — three epics of hardening (v0's
  `File`+`BufReader` regression, v1's mmap fix, v2's dict-coercion +
  page-cache-contention fix) already paid for this. This epic's job is
  identity/lifecycle and the write path, not the read path.
- **New identity model, decoupled from source-file shadowing.** Today's
  `is_fresh()`/staleness check compares against a source parquet path's
  mtime/size — a native table has no such source once loaded. Replace with
  a table-owned manifest (schema, segment layout, per-segment statistics,
  a table-local version/snapshot marker) — the closest existing precedent
  for a self-describing versioned layout in this codebase is the Iceberg
  metadata/manifest/snapshot structure already used for reads
  (`src/storage/iceberg.rs`), not a from-scratch design.
- **Write path modeled on `src/storage/lance_write.rs`, not invented.**
  `write_batches`/`write_from_parquet`'s streaming-without-double-
  materializing pattern is the direct template for "move data from an
  existing source into the new format."
- **Zone-map statistics become table-owned, not parquet-footer-derived.**
  `morsel_agg.rs::try_execute_dense_direct` (the engine's fastest
  aggregation path) currently reads key-range bounds from parquet file
  footers specifically. A native table has no parquet footer once
  standalone — this epic must give it an equivalent, or that fast path
  silently stops firing for native tables, which is a real performance
  regression risk, not just an omission.
- **Memory safety from day one, no exceptions.** No unbounded
  `MemoryTable`-style load path. Either a hard, explicit size bound or
  real integration with the existing spillable/budget machinery — decided
  and built in this epic, not deferred, per CLAUDE.md's unconditional
  memory-safety rule.
- **SQL DDL scope decided by early attribution, not assumed.** `CREATE
  TABLE ... AS SELECT` is the target surface, but `src/planner/binder.rs`
  has zero DDL/DML parsing today. Task 001 spikes the actual size of that
  lift; if it's large relative to the storage-format work, a CLI-only
  bulk-load surface (mirroring the existing `write-lance`/`load-lance`
  commands) ships first as the safe minimum, with SQL DDL as a fast-follow
  — this mirrors every prior epic's "measure before committing to the
  bigger lift" discipline.
- **GPU-offload compatibility kept open, not built.** `GpuAggPlan::pid()`
  hashes `provider.parquet_files()`, and offload refuses any provider
  where that's `None`. This epic generalizes that identity/eligibility
  hook enough that phase 3 (GPU tiering) doesn't have to redo foundation
  work — it does NOT build the tiering itself.

## Technical Approach

### Storage format (`src/storage/` — new module or `ipc_cache.rs` evolved in place, decide in task 002)
Table-owned manifest carrying schema, segment/row-group layout, and
per-segment min/max statistics (the zone-map data `try_execute_dense_
direct` needs). Segments themselves stay Arrow IPC, mmap zero-copy,
dictionary-coerced — unchanged from today's sidecar.

### Write path
Bulk-load entrypoint accepting any `RecordBatch` stream — from a query
result, a parquet directory, an Iceberg table, or a Lance dataset — using
`lance_write.rs`'s streaming pattern. Full-table replace only in this
epic (a "load" always produces a new complete snapshot); no partial
append/update semantics yet.

### Read path / engine integration
Real `TableProvider` implementation (today's sidecar is a helper called
*from* `ParquetTable`, not a provider itself): `distributed_splits`/
`shard_by_splits` implemented for real (both default `None` today on
providers that don't specifically implement them), `statistics()`
returning the table's own zone-map data, registration alongside
parquet/Iceberg/Lance in `ExecutionContext`/`--tables`/metastore.

### Memory safety
Either a hard configurable size cap enforced at load time, or real
`MemoryConsumer`-style integration with the existing spill machinery —
decided in task 006 based on which is actually sufficient for the
RAM-resident target this phase serves (VRAM/disk tiering is phase 3, not
this epic).

### QA / benchmarking
Cell-exact validation (`tests/duckdb_validated.rs` pattern) at SF=10 and
SF=100. Per the standing project direction: report against DuckDB reading
plain parquet AND DuckDB reading Iceberg tables, and CPU vs GPU as
separate rows wherever GPU offload can plausibly engage (task 007's
generalized `pid()` may make this partially testable even though full
GPU-tier work is phase 3).

## Implementation Strategy

Sequenced by what gates what, not a fixed phase count:
1. Attribution spike (SQL DDL lift size, statistics/manifest format
   decision) — gates everything else's exact shape.
2. Storage format + manifest (identity/versioning/zone-maps).
3. Write path (bulk-load, streaming, `lance_write.rs`-modeled).
4. `TableProvider` integration (splits, statistics, registration).
5. Dense-direct-address fast-path compatibility (feed real zone-maps in).
6. Memory safety (cap or spill integration).
7. GPU-offload identity/eligibility generalization (keeps phase 3 open,
   doesn't build phase 3).
8. QA close-out (cell-exact, SF=10+SF=100, Iceberg comparison, CPU/GPU
   split, docs, epic close).

Every lever: implement → cell-exact validate → benchmark → commit-or-revert,
through `scripts/claude-safe-build.sh`, matching this repo's standing
discipline.

## Task Breakdown Preview

- 001: Attribution spike — SQL DDL lift size + statistics/manifest format
  decision (parallel: false, gates everything)
- 002: Storage format + manifest (identity, versioning, zone-maps)
  (parallel: false, depends on 001)
- 003: Write path — bulk-load from parquet/Iceberg/Lance/query results
  (parallel: true once 002 lands)
- 004: `TableProvider` integration — splits, statistics, registration
  (parallel: true once 002 lands, conflicts with 003 only if they touch
  the same new module — decide file layout in 001/002 to keep them
  separable)
- 005: Dense-direct-address fast-path compatibility (parallel: false,
  depends on 002's zone-map format)
- 006: Memory safety — cap or spill integration (parallel: false, depends
  on 002/004)
- 007: GPU-offload identity/eligibility generalization (parallel: true,
  independent of 003-006's specifics once 002's identity model is fixed)
- 008: QA close-out — cell-exact SF=10+SF=100, DuckDB-parquet AND
  DuckDB-iceberg comparison, CPU/GPU split, docs, epic close
  (parallel: false, depends on everything)

Total tasks: 8
Estimated total effort: matches or exceeds a single one of the six prior
performance epics — likely 2-4 focused working sessions, dominated by
the storage-format and TableProvider-integration tasks.

## Dependencies

- `src/storage/ipc_cache.rs`, `src/storage/lance_write.rs`,
  `src/storage/iceberg.rs` (metadata/manifest precedent),
  `src/physical/operators/scan.rs` (`TableProvider` trait, `MemoryTable`),
  `src/physical/gpu.rs` (`pid()`/eligibility hook), `src/execution/
  context.rs` (`register_*` family), `src/physical/operators/morsel_agg.rs`
  (`try_execute_dense_direct`'s footer-stats dependency).
- Should stay compatible with the concurrently-in-flight
  `duckdb-parity-2/006` dense-group-id remapping work: that work's flat,
  dense-id-indexed accumulator layout wants group keys in dense-integer or
  dictionary space — this epic's format should preserve that, not fight it.
- No new external crate dependency identified as required.

## Success Criteria (Technical)

- G1: a native table can be created (SQL or CLI) and loaded from an
  existing source; queries against it are cell-exact vs DuckDB.
- G2: SF=10 and SF=100 like-for-like numbers at or better than today's
  IPC-sidecar cache-on baseline (SF=10 ~1.3x, SF=100 historically 1.21x
  like-for-like / 0.72x DuckDB-native).
- G3: no uncontrolled-OOM path exists; memory safety validated the same
  way every other operator in this codebase is.
- G4: `try_execute_dense_direct` fires for native tables exactly as it
  does for parquet tables of equivalent shape — no silent fast-path loss.
- G5: full suite green in all feature combinations; distributed M1/M2
  gates unaffected (native tables need not participate in distributed
  queries yet, but must not break existing distributed behavior for other
  table types).

## Estimated Effort

- 001: S (0.5-1 day — a spike, not full implementation).
- 002: M-L (2-4 days — the core new format/manifest design).
- 003: M (1.5-3 days — write path, real streaming discipline required).
- 004: M (1.5-3 days — TableProvider surface, splits).
- 005: S-M (0.5-1.5 days — wiring existing stats consumers to a new source).
- 006: M (1.5-3 days — real design decision + implementation, not just a flag).
- 007: S (0.5-1 day — identity/eligibility hook generalization only).
- 008: S-M (1-2 days — both scales, both premises, CPU/GPU split, docs).
- Total: 2-4 focused working sessions, the largest single-epic effort in
  this program so far.

## Tasks Created
- [x] 001.md - Attribution spike — SQL DDL lift size + statistics/manifest format decision (parallel: false)
- [x] 002.md - Storage format + manifest — identity, versioning, zone-maps (parallel: false)
- [x] 003.md - Write path — bulk-load from parquet/Iceberg/Lance/query results (parallel: true)
- [x] 004.md - TableProvider integration — splits, statistics, registration (parallel: true)
- [ ] 005.md - Dense-direct-address fast-path compatibility (parallel: false)
- [ ] 006.md - Memory safety — cap or spill integration (parallel: false)
- [x] 007.md - GPU-offload identity/eligibility generalization (parallel: true)
- [ ] 008.md - QA close-out — cell-exact SF=10+SF=100, Iceberg comparison, CPU/GPU split, docs, epic close (parallel: false)

Total tasks: 8
Parallel tasks: 3
Sequential tasks: 5
Estimated total effort: 72-144 hours (2-4 focused working sessions)
