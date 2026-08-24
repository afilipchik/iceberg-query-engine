---
name: native-tables-mutation
status: in-progress
created: 2026-08-24T04:45:55Z
updated: 2026-08-24T04:45:55Z
progress: 67%
prd: .claude/prds/native-tables.md
github: (will be set on sync)
---

# Epic: native-tables-mutation

## Overview

Phase 2 of the `native-tables` program. Phase 1 (`native-tables-foundation`,
archived) delivered a real, working, but strictly bulk-load/replace-only
table format: `CREATE TABLE ... AS SELECT` and a CLI write path both
produce one complete new snapshot every time, via `NativeWriteMode::
{Create, Overwrite}` — there is no `Append`, and no row-level mutation of
any kind. This epic adds `INSERT`, `DELETE`, and `UPDATE`. Unlike phase 1
— which was substantially "expose an already-proven mechanism as a
table" — this epic is genuinely new ground: the engine has zero DML
infrastructure of any kind today (phase 1 explicitly did not build
`Statement::Insert`, `Statement::Update`, or `Statement::Delete` handling;
all three currently hit the binder's catch-all `NotImplemented`), and the
storage format's segments are immutable Arrow IPC files by design — there
is no existing "modify a row in place" mechanism anywhere in this
codebase to extend.

## Architecture Decisions

**Decided by task 001's design spike** (`.claude/epics/
native-tables-mutation/001.md`'s Outcome section has the full evidence
behind every decision below — read it before implementing 002-006;
this section states the conclusions only) — matching phase 1's own
discipline (its task 001 decided SQL DDL scope and manifest format
before anything else proceeded; this epic's design stakes are higher,
so the same discipline applied with more force):

- **Deletion mechanism — DECIDED: a per-segment sorted deletion vector**
  (`Vec<u32>` of local row positions, a new field inline on the existing
  `Segment` struct in `_manifest.json`), consulted inside
  `NativeTable::scan()`/`scan_with_filter()` as a single choke point —
  merge-on-read, not copy-on-write. **Correction to this section's own
  prior claim**: `src/storage/iceberg.rs` is NOT existing read-side
  precedent for deletion-vector consultation — read in full, it
  REFUSES any Iceberg table containing delete files outright
  (`data_files_of`: `content != 0` is a hard `NotImplemented`, "row-level
  deletes are not supported — compact the table first"), never consults
  them. This epic's deletion-vector read-side logic is new, with no
  prior in-repo implementation to reuse. Representation is a plain
  `Vec<u32>`, not the `roaring` crate — segments are capped at
  ~1,000,000 rows by construction, below the scale where `roaring`'s
  compression wins over a flat structure, and a wholly-tombstoned
  segment is dropped from the manifest outright (see Compaction below),
  bounding the pathological case a plain list handles worst. No new
  Cargo dependency. Full reasoning, including composition with phase
  1's dense-direct-address fast path (verified needs ZERO changes) and
  the memory-budget mechanism (verified needs ZERO changes, with one
  named residual risk for task 005 to adversarially test): task 001's
  Outcome, Decision 1.
- **UPDATE semantics — DECIDED: DELETE + INSERT, confirmed**, no
  evidenced reason found for true in-place update to be better on this
  engine's immutable-segment shape. One load-bearing refinement beyond
  the "standard simplification" framing: task 004 must NOT implement
  this as two sequential calls to task 002/003's own public,
  self-publishing entrypoints (each would independently publish,
  leaving a real half-done window) — task 002 and task 003 must each
  expose a lower-level, non-publishing building block (write segments
  without publishing; identify+tombstone matched rows without
  publishing) that task 004 composes into ONE manifest edit and ONE
  atomic publish. Full reasoning: task 001's Outcome, Decision 2.
- **Compaction — DECIDED: explicitly deferred to a future epic**, not
  built here. A deletion vector alone is CORRECT indefinitely (reads
  stay right; they just do increasing amounts of filtering work as
  deletes accumulate) — confirmed, not just asserted. Honest, named
  downside: segment count grows by at least one per Append/INSERT
  forever (no merging of small segments), and disk space from deleted
  rows is never physically reclaimed — except one narrow, in-scope
  exception: a segment tombstoned to 100% (`deleted_rows.len() ==
  row_count`) is dropped from the manifest outright by task 003, which
  is not full compaction but does bound the deletion vector's own
  worst case. A future compaction task needs nothing this epic doesn't
  already build: candidate selection is its own new logic, but reading
  survivors (the existing deletion-aware `scan()`), writing them as
  fresh segments (the existing segment-writer), and publishing
  (the existing atomic single-file manifest rename + single-writer
  lock) are all direct reuse. Full reasoning: task 001's Outcome,
  Decision 3.
- **Atomic publish for incremental changes — DECIDED: single-FILE
  atomic rename of a freshly-written manifest**, generalizing (not
  reusing unchanged) phase 1's single-DIRECTORY rename
  (`native_manifest::publish_table_dir`). Confirmed by reading the
  actual implementation: phase 1's mechanism does
  `remove_dir_all(final_dir)` then `rename(staging, final_dir)` — safe
  only when the staging directory is a complete, self-sufficient copy
  of the whole new table state (true for Create/Overwrite, NOT true for
  Append/DELETE/UPDATE, which must preserve most of an existing
  directory's segment files unchanged). Confirms this section's own
  hint: "phase 1's atomicity only worked because full-replace writes
  have no partial-update window to protect against" — true, evidenced.
  The new mechanism: write new segment file(s) directly into the LIVE
  table directory under fresh non-colliding ids (harmless orphans if a
  crash happens before publish — no manifest references them yet),
  construct one new `Vec<Segment>` in memory, call the EXISTING
  `NativeManifest::build()` unchanged (it already derives
  `row_count`/`table_stats` fresh from whatever segments it's given —
  no new merge function needed), write the new manifest to a temp file
  in the same directory, then `rename()` it over the live
  `_manifest.json` — one atomic file-level rename, the same POSIX
  guarantee already trusted for directory-level renames elsewhere in
  this exact codebase. Explicit scope boundary: this provides
  read-atomicity and process-crash-safety, not `fsync`/power-loss
  durability beyond the OS's own page-cache behavior — matching, not
  narrowing, phase 1's own established scope (its own publish path
  doesn't `fsync` either). Full reasoning: task 001's Outcome, Decision 4.

**Not deferred — fixed by this epic's own non-negotiables:**

- **No new opt-out on memory safety.** Task 006 in the foundation epic
  found and fixed a real OOM (`NativeTable::scan()` was not spill-aware).
  Mutation introduces new unbounded-growth surfaces (a deletion vector
  that grows with every delete; concurrent-write buffering) that must be
  bounded by design, not discovered as a bug after the fact this time —
  though a dedicated verification task still exists here precisely
  because "by design" claims in this program have been wrong before and
  were only caught by adversarial testing, never by inspection alone.
- **Single-writer assumption stays explicit, not implicit — DECIDED:
  `std::fs::File::try_lock()`.** This engine has no lock manager, no
  WAL, no MVCC, and this epic does not build any of those. Task 001
  decided the concrete mechanism: `std::fs::File::{try_lock, unlock}` —
  STABLE standard-library methods (confirmed against this toolchain's
  shipped std source; this repo pins `rustc 1.93.0`, well past the
  1.89.0 stabilization), wrapping `flock(2)` on a sibling lock file per
  table directory, held for a mutation's full read-identify-write-publish
  span. Zero new Cargo dependency. Verified with a real, live
  cross-process test (not just documentation trust): a second process
  gets `TryLockError::WouldBlock` while the first holds the lock, and —
  critically for crash-safety — can immediately acquire it after the
  first is `SIGKILL`'d, with zero manual cleanup, because the kernel
  releases a `flock` the instant the holding process dies for any
  reason. This is why `File::try_lock()` was chosen over a simpler
  `create_new`-marker-file lock, which would NOT self-release after a
  crash. Readers never lock (writer-vs-writer only). Full reasoning and
  the live test transcript: task 001's Outcome, Decision 5.

## Technical Approach

### SQL surface
`INSERT INTO <table> SELECT ...` (source is a `Box<Query>`, same shape
CTAS already binds via the existing `bind_query()` — task 001 confirmed
this holds for `Insert` via a fresh spike, not assumed to transfer from
CreateTable's; `INSERT ... VALUES (...)` binds through the identical
path too, for free, since `Binder::bind_set_expr` already has a
`SetExpr::Values` arm). `DELETE FROM <table> WHERE <predicate>`.
`UPDATE <table> SET <assignments> WHERE <predicate>`. All three need new
`Binder::bind()` match arms (currently absent — everything but
`Statement::Query` and `Statement::CreateTable` hits `NotImplemented`)
and new `ExecutionContext` entrypoints following
`create_table_as_select`'s established shape (`&mut self`, streaming,
not `sql()`'s materializing path). DELETE/UPDATE's `WHERE`/`SET`
expressions bind via the existing `Binder::bind_expr` (the same
predicate-binding `bind_select`'s WHERE clause already uses) but are
NOT executed via the generic `LogicalPlan`/`PhysicalOperator` pipeline
CTAS/INSERT use for their source query — that pipeline has no way to
carry a matched row's (segment, local position) back out, which DELETE
and UPDATE both need. They use a bespoke per-segment scan+evaluate loop
instead (task 001's Outcome, Decision 1 and Decision 2).

### Storage
`NativeWriteMode::Append` (new) writes additional segments directly into
the live table directory (fresh, non-colliding segment ids continuing
from the existing max, not restarting at 0) and constructs one new
`Vec<Segment>` (existing + new) that the EXISTING `NativeManifest::build`
rolls up unchanged — no new merge function needed, confirmed by reading
`build`'s existing implementation (task 001's Outcome, Decision 4).
Append must inherit the target table's ALREADY-DECIDED dictionary
encoding and validate the source's schema (name+type, not just column
count — `SegmentWriter::accept` today checks only count) against the
target's existing manifest schema, not rediscover either from the new
data (task 001's Outcome, Decision 6). The deletion vector is a new
`deleted_rows: Vec<u32>` field inline on the EXISTING `Segment` struct
in `_manifest.json` — NOT a separate file or artifact (task 001's
Outcome, Decision 1 — corrects this section's own earlier "most likely
a new manifest-adjacent artifact" framing).

### Read path
`NativeTable::scan()`/`scan_with_filter()` gain a deletion-vector
consultation step (task 001's Outcome, Decision 1) — every row a
segment yields gets filtered against `deleted_rows` before reaching the
rest of the query, as a single choke point inside `scan()` itself, so
every existing caller (generic path, dense-direct-address, a future
distributed shard) composes correctly with zero changes at the call
site. Verified (not assumed) to compose correctly with task 005's
dense-direct-address fast path from phase 1 (which reads segment/table
stats directly) and with distributed splits (`shard_by_splits`) — a
deletion vector scoped to the wrong granularity could silently
miscount rows for a shard.

### Memory safety
Re-verify, don't assume: adversarially test INSERT/DELETE/UPDATE the same
way phase 1's task 006 found its real OOM — large deletion vectors, many
small mutations in sequence, concurrent-ish access patterns — under
`scripts/claude-safe-build.sh`'s memory-capped execution model.

### QA / benchmarking
Cell-exact validation: mutate a native table, independently recompute the
expected result (e.g., apply the equivalent SQL to the original parquet/
Iceberg source via DuckDB, or re-derive via a fresh `CREATE TABLE ... AS
SELECT` with the mutation's net effect expressed as a query), compare.
Per this program's standing convention: both DuckDB comparison premises
and a CPU/GPU split where relevant (mutation itself is not a GPU-offload
target, but post-mutation aggregate reads are, and should keep working).

## Implementation Strategy

1. Design spike (deletion mechanism, UPDATE semantics, compaction
   in/out, atomic-publish mechanism, single-writer enforcement, SQL lift
   sizing) — gates everything else, mirroring phase 1's own task 001.
2. INSERT (`Append` mode + SQL wiring) — the simplest, most
   self-contained piece; delivers standalone value (native tables can
   grow incrementally) even before DELETE/UPDATE exist.
3. Deletion vector mechanism + DELETE (shipped together — the mechanism
   has no independent value without the SQL surface that exercises it).
4. UPDATE (built on INSERT + DELETE's mechanisms, per task 001's
   semantics decision).
5. Memory safety + concurrency/crash-safety adversarial verification.
6. QA close-out (cell-exact, full suite, benchmarks, CLAUDE.md, archive).

Every lever: implement → cell-exact validate → benchmark → commit-or-revert,
through `scripts/claude-safe-build.sh`, matching this program's standing
discipline.

## Task Breakdown Preview

- 001: Design spike — deletion mechanism, UPDATE semantics, compaction
  scope, atomic-publish model, SQL lift sizing (parallel: false, gates
  everything)
- 002: INSERT — `NativeWriteMode::Append` + SQL wiring (parallel: false,
  depends on 001)
- 003: Deletion vector mechanism + DELETE (parallel: false, depends on
  001; can start alongside 002 only if task 001 confirms no shared-file
  conflict — default to sequential unless proven safe)
- 004: UPDATE (parallel: false, depends on 002 and 003)
- 005: Memory safety + concurrency/crash-safety adversarial verification
  (parallel: false, depends on 002/003/004)
- 006: QA close-out (parallel: false, depends on everything)

Total tasks: 6
Estimated total effort: likely the largest single epic in this program so
far — genuinely new infrastructure, not an extension of a proven
mechanism the way phase 1 substantially was.

## Dependencies

- Everything phase 1 built: `src/storage/native_manifest.rs`,
  `src/storage/native_write.rs`, `src/storage/native_table.rs`,
  `src/planner/binder.rs`'s `CreateTable` arm (the template for the new
  `Insert`/`Update`/`Delete` arms), `ExecutionContext::
  create_table_as_select` (the template for the new entrypoints).
- The general `HashJoinExec` multi-round bug phase 1's task 008 found and
  fixed — mutation's own testing may exercise re-execution paths in new
  ways; treat any anomaly as a possible new instance of a similar class
  of bug before assuming it's mutation-specific.
- No new external crate dependency identified as required (a roaring
  bitmap crate is a plausible addition if task 001 picks that deletion-
  vector representation — not assumed here).

## Success Criteria (Technical)

- G1: `INSERT INTO`, `DELETE FROM ... WHERE`, and `UPDATE ... SET ...
  WHERE` all work end-to-end through SQL, cell-exact vs an independently
  computed reference.
- G2: No performance cliff for the still-dominant read-only query shapes
  — phase 1's benchmarks (SF=10 1.23x, dense-direct-address firing,
  GPU-eligible shapes) must not regress for a table that has never been
  mutated.
- G3: Memory safety holds under adversarial testing (large deletion
  vectors, many sequential mutations) — not assumed, tested the way
  phase 1's task 006 tested it.
- G4: Full suite green in all feature combinations; M1/M2 distributed
  gates unaffected (mutation need not work inside a distributed cluster
  yet, matching phase 1's own G5 scope boundary for reads).
- G5: The single-writer assumption (or whatever task 001 decides) is
  enforced, not just documented — a concurrent-write attempt fails
  cleanly and namedly, never silently corrupts state.

## Estimated Effort

- 001: S-M (1-2 days — a spike, but a genuinely harder design question
  than phase 1's).
- 002: M (2-4 days).
- 003: M-L (3-5 days — new read-path mechanism, not just a write-side
  addition).
- 004: M (2-4 days, mostly composition of 002+003 per task 001's
  semantics decision).
- 005: M (2-4 days — adversarial testing, matching phase 1's task 006's
  own real find).
- 006: S-M (1-2 days).
- Total: likely 3-5 focused working sessions, the largest epic in this
  program so far.

## Tasks Created
- [x] 001.md - Design spike — deletion mechanism, UPDATE semantics, compaction scope, atomic-publish model (parallel: false)
- [x] 002.md - INSERT — Append write mode + SQL wiring (parallel: false)
- [x] 003.md - Deletion vector mechanism + DELETE (parallel: false)
- [x] 004.md - UPDATE (parallel: false)
- [ ] 005.md - Memory safety + concurrency/crash-safety adversarial verification (parallel: false)
- [ ] 006.md - QA close-out — cell-exact, full suite, benchmarks, docs, epic close (parallel: false)

Total tasks: 6
Parallel tasks: 0
Sequential tasks: 6
Estimated total effort: 84-152 hours (3-5 focused working sessions)
