---
name: native-tables-foundation
status: completed
created: 2026-08-23T07:42:14Z
updated: 2026-08-23T23:50:00Z
progress: 100%
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
- [x] 005.md - Dense-direct-address fast-path compatibility (parallel: false)
- [x] 006.md - Memory safety — cap or spill integration (parallel: false)
- [x] 007.md - GPU-offload identity/eligibility generalization (parallel: true)
- [x] 008.md - QA close-out — cell-exact SF=10+SF=100, Iceberg comparison, CPU/GPU split, docs, epic close (parallel: false)

Total tasks: 8
Parallel tasks: 3
Sequential tasks: 5
Estimated total effort: 72-144 hours (2-4 focused working sessions)

## Epic close-out (2026-08-23)

All 8 tasks shipped and validated on branch `epic/native-tables-foundation`
(commits `f59eb2d`..`ea77142`, see Commits below). Full suite green in
**all four feature combinations** (default 1061/0/1, lance 1126/0/2, gpu
1061/0/1, pulsar 1064/0/1 — passed/failed/ignored, zero failures; lance
and pulsar's FULL suites ran for real in task 008, not just `cargo check`
as task 005 had explicitly deferred), `cargo fmt --all -- --check` clean,
M1 + M2 distributed gates PASS via real 3-separate-process clusters.

### Headline: what this epic actually delivered

A first-class, writable (bulk-load/replace, not yet mutable),
independently-persistent table format — `_manifest.json` + Arrow IPC
segments, mmap zero-copy, dictionary-coerced, reusing the existing IPC
sidecar's read mechanism wholesale (`src/storage/native_manifest.rs`,
`native_write.rs`, `native_table.rs`) — reachable via both a CLI
(`write-native`/`load-native`) and SQL (`CREATE TABLE ... AS SELECT`
through `ExecutionContext::create_table_as_select`, a new `&mut self`
method deliberately NOT wired through `sql()` or the distributed HTTP/
Flight endpoints). Registered exactly like Iceberg/Lance directories
(`serve --tables`, `register_native_table`), with a real `TableProvider`
implementation (`statistics()` never `None`, `distributed_splits`/
`shard_by_splits` real, `identity()` for GPU-cache correctness).

| scale | queries | engine total | vs DuckDB-parquet | vs DuckDB-iceberg | disk (native vs parquet) |
|---|---|---|---|---|---|
| SF=10 | 22/22 cell-exact | 5.324s | 4.321s → **1.23x** | 6.888s → **0.77x (engine faster)** | 6.5GB vs 9.6GB |
| SF=100 | 19/22 cell-exact + successful (Q4/Q12/Q13: see Residues) | 75.17s | 50.02s (same 19) → **1.50x** | n/a (no SF=100 Iceberg fixture) | 65GB vs 97GB |

Write throughput: SF=10's full 8-table warehouse in 23.5s; SF=100's
600,000,000-row `lineitem` alone in 173.9s (whole warehouse 209.6s).
Both scales land SMALLER on disk than their parquet source — dictionary
coercion of low-cardinality strings outweighs parquet's own compression
here, at both scales, not just one.

**CPU/GPU split found something genuinely new for this program**: every
prior GPU-offload measurement (all parquet-sourced, `gpu-acceleration`
and `duckdb-parity-2` both) found NO full-query win because scan/decode
dominates wall time. Native tables' simplest GPU-eligible shape (Q6: one
ungrouped `SUM`) breaks that pattern — **~18-20x end-to-end** (CPU
~140ms vs GPU warm ~7-8ms), VRAM-confirmed (1048→3858 MiB, RTX 5090),
reproduced twice — because a native table has no decode step to
dominate in the first place. Q1 (multi-aggregate + `GROUP BY`) stays
flat, matching the parquet-sourced finding for that same shape exactly.
This is the first shape in this program's entire GPU-offload
investigation history — parquet or native — to show a real full-query
win.

### Per-task attribution

- **001** (attribution spike): decided SQL DDL ships in-epic (not
  deferred), a JSON `_manifest.json` sidecar (not Arrow IPC/Parquet, not
  Iceberg's Avro), and three sibling modules under `src/storage/`
  (`native_manifest.rs`/`native_write.rs`/`native_table.rs`) so tasks
  003/004 could run in parallel without touching the same file. Every
  later task's technical shape traces back to this task's evidence, not
  assumption.
- **002** (manifest format): `native_manifest.rs` (~840 lines, 23 tests)
  — identity (`table_id` UUID, decoupled from any source file),
  versioning (`snapshot.version`), per-segment AND table-level zone-map
  statistics, atomic staging-then-publish. Confirmed `ipc_cache.rs`'s
  read mechanism (`read_row_group`/`sidecar_dict_cols`) reusable
  UNCHANGED — zero refactor of three-epics-hardened code.
- **003** (write path): `native_write.rs` (~1354 lines, 17 tests) +
  `write-native`/`load-native` CLI. Streaming, never materializing —
  measured, not just designed: SF=10 `lineitem` (60M rows) at ~406MB
  peak RSS regardless of source scale. Found (flagged for 006, not
  itself a fix) that `ExecutionContext::sql()`/`write-lance --sql`'s
  existing collect-then-write pattern was NOT memory-safe for CTAS, and
  built the write entrypoint to avoid inheriting that gap.
- **004** (`TableProvider` integration): `native_table.rs`, `CREATE
  TABLE ... AS SELECT` end to end. **Found and fixed 2 real, pre-existing
  engine bugs** during cell-exact validation: a bare `SELECT *`'s
  qualified-column-name asymmetry (harmless for a transient query,
  wrong to persist into a table's schema), and a `Dictionary`-typed
  logical schema breaking `GROUP BY` (no match arm existed anywhere in
  the engine for a provider that exposes Dictionary at the logical
  level, because none ever had before).
- **005** (dense-direct-address fast path): found the "fast path" was
  STRUCTURALLY unreachable for native tables (footer-stats dependency
  AND a parquet-only scan-and-accumulate loop, not just the bounds task
  004 flagged) and fixed both. Real timing proof: 6.1ms native vs 41.7ms
  parquet for a Q18-shaped SF=1 dense-direct `GROUP BY` — G4 met with
  evidence, not assumed, and re-confirmed fresh by task 008 (7.5ms vs
  46.75ms on an independent re-run).
- **006** (memory safety): independently re-verified the write path
  (confirmed safe, no gap) and, going one step further than the task's
  own framing, **found and fixed a real OOM**: `NativeTable::scan()` was
  not spill-aware, materializing the whole active segment set — measured
  SIGKILL (exit 137, ~1.6GB peak RSS) under a bare 1GiB cgroup cap where
  the identical query over identical data as plain Parquet finished in
  109ms. Fixed with a read-side admission-control cap
  (`memory_limit * spill_threshold`, reusing `spillable.rs`'s own
  formula) — G3 met with a real regression found AND fixed, not merely
  validated.
- **007** (GPU-offload identity): generalized `GpuAggPlan::pid()`/
  `plan_gpu_agg`'s eligibility from an inlined `parquet_files()`-only
  hash to a new `TableProvider::identity()` trait method — parquet gets
  correct behavior for free from the trait's own default, native tables
  opt in directly (`table_id` + `version` bytes, `None` for a shard —
  the GPU-cache-aliasing guard). Verified on real hardware for parquet
  (VRAM staircase, RTX 5090); the native-table live-query verification
  was explicitly left as follow-up for whoever validated task 004 next
  — task 008 closed that loop (below).
- **008** (this task, QA close-out): full suite re-verification in all 4
  feature combinations (the FIRST real lance/pulsar full-suite run post-
  epic, not just `cargo check`), cell-exact SF=10 (22/22) and SF=100
  (19/22 + 3 documented residuals) validation against a real DuckDB
  oracle, Iceberg comparison, M1/M2 real-cluster gate re-confirmation,
  the CPU/GPU split finding above, CLAUDE.md documentation, and epic
  close-out. **Found and fixed a real regression**: a `HashJoinExec`
  concurrency bug (see Residues) that task 008's own SF=10 sweep
  surfaced as Q13 losing its zero-order-customers bucket — root-caused
  with a live `gdb` thread dump, fixed with a one-line change, pinned
  with a new deterministic regression test that fails without the fix.

**Four real, pre-existing-or-newly-exposed engine bugs found and fixed
across this epic** (002/006/008 each independently verified rather than
trusted the prior task's self-report before finding their own issue):
two in task 004 (wildcard qualification, Dictionary-schema GROUP BY),
one in task 006 (native-table read-side OOM), one in task 008
(HashJoinExec multi-round concurrency). Every one shipped with its own
dedicated regression test.

### G1-G5 (this epic's own success criteria): G1/G3/G4/G5 MET, G2 PARTIALLY MET

- **G1** (a native table can be created and loaded; queries cell-exact
  vs DuckDB) — **MET**. Both surfaces validated at real scale: SQL DDL
  (`create_table_as_select`) and CLI (`write-native`). SF=10: 22/22
  cell-exact. SF=100: 19/22 cell-exact + successful, 3 residual (never a
  wrong answer — always a clean refusal or a slow-but-correct
  completion; see Residues).
- **G2** (SF=10/SF=100 like-for-like at or better than the IPC-sidecar
  cache-on baseline: SF=10 ~1.3x, SF=100 1.21x historically) — **PARTIALLY
  MET**. SF=10's 1.23x is at/better than the ~1.3x/1.36x baseline: MET.
  SF=100's 1.50x (on the 19 completing queries) is WORSE than the 1.21x
  historical baseline: NOT MET, directly attributable to the same
  documented root cause as the 3 residual queries — native tables have
  no scan-level pruning yet, so even the queries that DO complete pay
  more unpruned scan volume at this data size than parquet's row-group
  statistics let it pay. A well-evidenced partial result, not hidden.
- **G3** (no uncontrolled-OOM path; memory safety validated) — **MET**.
  Write path streaming/bounded (measured at both scales, not just
  designed for). Read path: task 006 found and fixed a real OOM with a
  hard admission-control cap, verified end to end (SIGKILL → clean HTTP
  400 citing the exact budget).
- **G4** (`try_execute_dense_direct` fires for native tables exactly as
  for parquet) — **MET**. Task 005's real measurement, independently
  re-confirmed fresh by task 008 on the same SF=1 shape (7.5ms native vs
  46.75ms parquet, "(native)" tag present).
- **G5** (full suite green in all feature combinations; M1/M2 gates
  unaffected) — **MET**. All 4 combinations green (1061/1126/1061/1064
  passed, 0 failed anywhere). M1 GATE PASS + M2 GATE PASS via real
  3-process clusters (`cluster_local.sh verify`/`verify-m2`), confirming
  nothing this epic touched broke existing parquet/Iceberg/Lance
  distributed behavior — native tables themselves correctly do NOT
  participate in distributed scatter/gather yet (out of scope, see
  Deferred below), by design, not by accident.

### Residues (named as one class, matching this program's convention)

1. **The `HashJoinExec` multi-round concurrency bug (found AND fixed,
   task 008)** — a real regression this epic's own QA found and fixed
   with a one-line change plus a new permanent regression test
   (`left_join_reemits_unmatched_build_rows_on_a_second_full_round`,
   `src/physical/operators/hash_join.rs`). Not native-table-specific: a
   latent bug in shared join code, general enough to hit any provider
   whenever `SpillableHashAggregateExec::execute_fused_streaming`
   attempts and abandons a LEFT/RIGHT/FULL join with a large-enough
   group-by cardinality. Worth naming precisely because this is the
   SECOND time this exact symptom class ("Q13's zero-order customers
   vanish") has been fixed in this codebase — `tests/spill_tests.rs`'s
   pre-existing `left_join_unmatched_build_rows_preserved` test already
   documents an EARLIER, different-trigger version of the same symptom
   (multi-PARTITION drops, fixed by the `build_matched` mechanism this
   epic's bug lived inside).
2. **No scan-level pruning for native tables (found, root-caused,
   explicitly NOT fixed — a real future task, not this epic's scope)**
   — `NativeTable::scan_with_filter` has no predicate pushdown at all;
   every query reads every active segment in full. At small-to-medium
   scale this costs nothing observable (SF=10: 22/22 cell-exact, only
   1.23x DuckDB-parquet). At larger scale it has a real, measured cost:
   3 of 22 TPC-H queries (Q4, Q12, Q13 — scale-dependent: only Q12 at
   SF=10, all three at SF=100) push a join's build side across
   `SpillableHashJoinExec`'s spill threshold, which (a pre-existing
   characteristic of that operator, not introduced by this epic —
   "materializes the build side before deciding to spill... known hole"
   is CLAUDE.md's own prior-epic language) then either refuses cleanly
   (SEMI/ANTI joins) or completes very slowly (LEFT/INNER joins spilling
   via many small Parquet files — root-caused with a live `gdb` thread
   dump catching a thread inside `parquet::column::writer` mid-query, and
   filesystem evidence of hundreds of actively-growing spill files).
   Closing this for real means either scan-level pruning for native
   tables (a materially larger feature) or a streaming rewrite of the
   join spill path (a separately-tracked, pre-existing ROADMAP item) —
   named precisely so a future epic can pick either lever with real
   evidence already in hand, rather than re-discovering this gap.
3. **Two CLI/GPU wiring gaps (found, not fixed)** — `load-native
   --query` neither calls `enable_gpu_offload()` (every other single-
   process CLI command does) nor registers a real `NativeTable` (it
   fully materializes via `native_write::read_back` into a plain
   `MemoryTable`, which has no `identity()` override and so could never
   pass GPU eligibility regardless). Both predate this task (task 003
   shipped `load-native` before task 007's GPU-identity work existed);
   worked around with a new permanent diagnostic
   (`examples/native_gpu_check.rs`) rather than fixed, since fixing
   `load-native` to use the real provider is a small-but-separate
   change outside this task's own scope boundary.
4. **`MemoryTable`-shaped read gap shared with Lance** — `NativeTable::
   scan()`'s lack of incremental/streaming reads (task 006's fix is an
   admission-control CAP, not a true streaming rewrite) is explicitly
   the same architectural gap `LanceTable::scan()` already has, not
   unique to this epic's new code. A genuinely streaming native-table
   scan comparable to `ParallelParquetSource` remains open, unclaimed
   follow-up work.

### Deferred to phase 2/3/4 (per the `native-tables` PRD — explicitly
NOT attempted this epic, so the next epic has a clean starting point)

- **Phase 2 — Mutation.** `INSERT`/`UPDATE`/`DELETE` are not
  implemented; `Statement::Insert` still falls through the binder's
  catch-all `NotImplemented` exactly as before this epic (confirmed
  parses fine via `sqlparser`, per task 001's own spike — the grammar
  is not the blocker). A "load" is always a full-table replace. This
  epic's manifest/versioning design (task 002) was built with mutation
  in mind (a `snapshot.version` marker exists specifically for a future
  mutation to bump) but no deletion/update strategy (tombstones, MVCC,
  rewrite-on-write) was designed or decided — that is genuinely open
  design work for phase 2, not a small addition.
- **Phase 3 — GPU/RAM/disk tiering.** This epic kept the GPU-offload
  identity hook open (task 007) and confirmed real engagement + a real
  full-query win for one shape (task 008), but built no promotion/
  eviction policy, no RAM-tier/disk-tier distinction beyond "mmap is
  OS-evictable," and no `QE_GPU_CACHE_MB`-style cap (already unbuilt
  even for the pre-existing parquet GPU cache, per the `gpu-acceleration`
  epic's own "Not done" list). Native tables are RAM/disk-resident via
  mmap only, exactly like the pre-existing IPC sidecar always was.
- **Phase 4 — Materialized rollups.** No query-rewrite/substitution
  mechanism of any kind exists anywhere in this codebase (confirmed
  still true, not re-derived — the PRD's own research already
  established this). Entirely unbuilt, as planned; its priority
  relative to other future work depends on the concurrency-model
  question the PRD itself flags as unsettled (one-query-at-a-time vs.
  many-concurrent-dashboard-viewers).
- **Distributed participation for native tables.** `distributed_splits`/
  `shard_by_splits` are real (task 004), but native tables do not
  participate in scatter/gather planning yet — this epic's own G5
  explicitly scoped that out, and task 008 confirmed (M1/M2 gates PASS)
  that existing distributed behavior for other table types is
  unaffected. Wiring native tables into scatter/gather is unclaimed
  future work, made easier by task 004's real (not stub) splits.

### Commits

`f59eb2d` (001) → `e0afa44` (001 done) → `5ffe1cb` (002) → `4b12697`
(002 done) → `6df5703`+`a34f460`+`84f064e` (003) → `db24535` (007) →
`a61e8a1`+`5c12923` (003/007 done) → `f8391fe`+`21e5162` (004) →
`aa4b033` (004 done) → `baf9e15`+`a0b10c6` (005) →
`ed78861`+`d64f968` (006) → `45328c4` (005 done) →
`d34ea0b` (008: HashJoinExec fix) → `ea77142` (008: benchmarks/docs/
archive) → this commit (008: epic close-out).

### Archival

Epic moved to `.claude/epics/archived/native-tables-foundation/` as
task 008's final step, mirroring `duckdb-parity-2`/
`dependency-modernization`'s archival pattern (`git mv`, this session).
Not merged to `main` — that decision and action is left to the user/
orchestrating session per this task's own instructions.
