---
name: native-tables-tiering
status: in-progress
created: 2026-08-26T16:23:07Z
updated: 2026-08-26T17:45:00Z
progress: 33%
prd: .claude/prds/native-tables.md
github: (will be set on sync)
---

# Epic: native-tables-tiering

## Overview

Phase 3 of the `native-tables` PRD: GPU/RAM/disk tiering, the last
remaining phase (foundation, mutation, and rollups are all shipped and
archived). A dedicated research pass — confirmed against a real RTX
5090 (32GB VRAM) available in this environment, so every claim below is
empirically checkable, not theoretical — found the PRD's own "3-tier
hierarchy" framing overstates how much needs building: two of the three
tiers already exist with real work already done elsewhere; the actual,
concrete gap is narrower and in a different place than the PRD's FR5
implies.

**What already exists, contradicting the PRD's own assumptions:**
- **RAM tier**: IS the native-table format itself — mmap + OS page
  cache, already shipped, already the entire "RAM-resident" story.
  Nothing to build.
- **Disk tier**: IS how native tables persist by default — nothing to
  build either. The PRD's "spill cold data to disk within the engine's
  existing spillable-operator machinery" is really about query-execution
  spilling staying safe, not a new table-data cold-tier to construct —
  see the correctness caveat below.
- **GPU cache**: the PRD describes it as "VRAM-resident, per-query,
  ephemeral... no eviction policy implemented despite one being
  designed." Research found this half-right: the routing DECISION
  (GPU vs. CPU) is per-query, but the underlying DATA cache
  (`GpuEngine`, `src/physical/gpu.rs`) is already a process-wide,
  cross-query, `(table, column)`-keyed persistent cache — the "hard
  part" of FR5 (a keyed, persistent device-memory cache) already exists
  and is in production use. What's actually, concretely missing: **zero
  VRAM byte accounting anywhere** (`QE_GPU_CACHE_MB` is a name in prose
  and three separate planning docs — zero occurrences in actual code),
  **zero eviction** (every cache structure — `resident`, `codes`,
  `columns`, `code_bufs` — is insert-only, confirmed by reading every
  call site), and **one failed upload permanently disables GPU offload
  for the rest of the process** (`GpuEngine::mark_unhealthy()` sets a
  single process-wide `AtomicBool`, never reset) — the opposite of
  graceful degradation.
- **A genuine, newly-identified leak**: `GpuAggPlan::pid()` keys the
  cache by a hash of `TableProvider::identity()`, and a native table's
  `identity()` is `table_id ++ version` — so every INSERT/DELETE/UPDATE
  against a native table (mutation epic, shipped) changes its identity
  hash, meaning the OLD version's uploaded columns become permanently
  unreachable dead weight in VRAM with no way to reclaim them short of
  a process restart. Every mutation against a GPU-queried native table
  leaks VRAM. Task 001 confirms this empirically before treating it as
  fact.

**Scope narrowed accordingly**: this epic is real, bounded GPU-cache
hardening (budget accounting, eviction, failure isolation, the leak
fix, observability) layered on the ALREADY-EXISTING persistent cache —
not building a new 3-tier hierarchy from scratch. This matches the
`native-tables-rollups` epic's own "narrow slice first" precedent:
ship the real, well-scoped gap; name what's explicitly not attempted.

**Also found**: the PRD's own dependency note points at
`larger-than-memory-support.md` for the memory-safety constraint this
phase depends on — that doc is stale (last touched 2026-01-26) and its
"disabled by default" claims are contradicted by current reality
(CLAUDE.md's own "ALWAYS used" mandate for spillable operators). The
REAL, current, operative constraint is `spill-join-correctness`'s own
finding: a real but low-rate (0.34% pooled, 95% CI [0.01%, 1.91%]),
root-cause-unconfirmed wrong-answer bug in `SpillableHashJoinExec`'s
spill path — confirmed NOT native-table-specific and confirmed
distributed-reachable. This epic does not touch that mechanism (it's
join-spill, not GPU-cache), but any claim this epic makes about "cold
data is safe on disk" should cite the real, current finding, not the
stale doc.

## Architecture Decisions

- **Harden the existing cache, don't build a new hierarchy.** RAM and
  disk tiers need no new code (see above) — effort goes entirely into
  the GPU tier's real gap: budget, eviction, failure isolation, the
  mutation-driven leak.
- **Confirm the leak empirically before fixing it.** Task 001 doesn't
  assume the mutation-driven VRAM leak is real from the research's
  code-reading alone — it reproduces it with a real measurement (VRAM
  usage before/after N mutations against a GPU-queried native table)
  before designing the fix, matching this program's own "instrument,
  don't just re-read" discipline (`spill-join-correctness` task 001's
  own precedent).
- **Reactive promotion stays reactive.** No hotness-tracking or
  automatic pre-promotion policy exists anywhere in this codebase
  (confirmed by research: zero LRU/access-timestamp/promotion
  infrastructure anywhere in `src/`), and building one from scratch is
  a much bigger, unprecedented, fuzzier undertaking than what the real
  gap calls for. This epic keeps promotion exactly as it is today (a
  query matching the GPU-eligible shape triggers upload) and makes THAT
  reactive path safe and bounded — it does not add a new "predict and
  pre-promote" system.
- **Type-coverage widening is explicitly out of scope.** The PRD's FR5
  mentions "wider type coverage beyond today's f64-only" — research
  found the type story is already less limited than that phrasing
  suggests (Float64/Int32/Date32/bounded-Int64 inputs, Utf8/Dictionary
  keys), and more importantly, the existing `gpu-acceleration` epic's
  own measured numbers show GROUP BY-shaped queries (the shapes that
  would benefit most from wider key-type coverage) see flat full-query
  improvement regardless — they're scan/decode-bound, not
  compute-bound. Widening type coverage would add real engineering
  effort for a shape class that doesn't clearly benefit. Named, not
  attempted.
- **Eviction correctness over eviction sophistication.** The cache
  lives inside one dedicated worker thread's single-threaded job-queue
  loop (`GpuEngine`'s worker) — eviction logic runs there too, avoiding
  new concurrency surface. A simple, correct LRU (or LRU-adjacent)
  policy is the target; a more sophisticated policy is not this epic's
  goal.
- **Always correct under eviction.** An evicted-then-requested-again
  column must transparently re-upload and produce a cell-exact answer,
  indistinguishable from a column that was never evicted — matches this
  program's "always correct, even if that means not fast" standing
  culture (`native-tables-rollups`'s own explicit Architecture Decision,
  applied here to cache state instead of rollup freshness).
- **No regression to the gpu-acceleration epic's own measured wins.**
  Q6-shape (single ungrouped SUM/COUNT, no GROUP BY) over native tables
  measured ~18-20x end-to-end warm — this epic's eviction/budget work
  must not regress that number meaningfully for the common case (one
  table's columns fitting comfortably inside budget).

## Technical Approach

### VRAM budget + eviction (task 001)
Real byte-size accounting for every resident `(table, column)` entry in
`GpuEngine`'s worker-thread state; a real `QE_GPU_CACHE_MB` env var
(finally implemented, not just named); LRU-based eviction when a new
upload would exceed budget, running inside the worker thread's existing
single-consumer loop. Confirm and fix the mutation-driven stale-version
leak as part of this — a superseded native-table version's columns
should become evictable (ideally proactively reclaimed, not just
"eventually LRU'd out" if nothing else ever touches the budget).

### Failure isolation (task 002)
Replace the single process-wide `healthy: AtomicBool` poison flag with
per-column/per-upload failure handling — one bad upload should evict/
skip that entry and fall back to CPU for that query, not permanently
disable GPU offload for the rest of the process's lifetime. Pairs with
observability: expose current cache state (resident columns, VRAM used
vs. budget, eviction count) via whatever this codebase's existing
diagnostic convention is (`QE_GPU_DEBUG`-style env-gated tracing,
matching `QE_SPILL_DEBUG`/`HJ_TIMING`/`AGG_TIMING` precedent).

### Validation (task 003)
Cell-exact correctness across eviction (a re-uploaded-after-eviction
column must match a never-evicted baseline); a real stress test proving
VRAM stays bounded across many mutations against a GPU-queried native
table (proving the leak is actually fixed, not just theoretically
addressed); no regression to `gpu-acceleration`'s own measured Q6-shape
win; full suite; docs.

## Task Breakdown Preview

- 001: VRAM budget accounting + LRU eviction, including confirming and
  fixing the mutation-driven stale-version leak (parallel: false, the
  epic's core piece)
- 002: Per-column failure isolation + cache-state observability
  (parallel: false, depends on 001's cache-state structures)
- 003: QA close-out — validation sweep, no-regression check, full
  suite, docs, epic close (parallel: false, depends on everything)

Total tasks: 3
Estimated total effort: smaller and better-bounded than
`native-tables-rollups` — this is "ordinary Rust cache-eviction
bookkeeping layered on existing structures" (the research's own
characterization), not a new algorithm class.

## Dependencies

- `src/physical/gpu.rs` — the sole file the core cache logic lives in
  (`GpuEngine`, `GpuAggExec`, `GpuAggPlan`).
- `src/execution/memory.rs` (`ExecutionConfig::gpu_offload`) and
  `src/physical/planner.rs:1698-1710` — the existing distributed-disable
  gate this epic must not weaken.
- `.claude/epics/archived/gpu-acceleration/` — prior art; read its
  CLAUDE.md section ("GPU Aggregate Offload") before starting, it has
  the real measured numbers this epic's no-regression check validates
  against.
- `scripts/claude-safe-build.sh` for every build.

## Success Criteria (Technical)

- G1: the mutation-driven VRAM leak is confirmed (empirically measured)
  and fixed — VRAM usage stays bounded across a real, repeated-mutation
  stress test, not just theoretically addressed.
- G2: a real, enforced VRAM budget exists (`QE_GPU_CACHE_MB` actually
  implemented) with LRU eviction when exceeded; correctness preserved
  across eviction (cell-exact, re-upload-after-eviction transparent).
- G3: a single failed upload no longer permanently disables GPU offload
  for the rest of the process — failure isolation is per-column.
- G4: cache state (resident columns, VRAM used vs. budget, eviction
  count) is observable via this codebase's existing diagnostic
  convention.
- G5: no regression to `gpu-acceleration`'s own measured wins (Q6-shape
  ~18-20x over native tables); no regression to the distributed-disable
  gate; full suite green; native-tables PRD's status updated to reflect
  this phase's actual outcome (the 4th and final phase).

## Estimated Effort

- 001: M-L — real work (byte accounting, LRU, the leak fix), but
  "ordinary Rust bookkeeping," not new-algorithm-class risk.
- 002: S-M.
- 003: S-M.

## Tasks Created
- [x] 001.md - VRAM budget + LRU eviction + confirm/fix the mutation leak (parallel: false) — CLOSED 2026-08-26
- [ ] 002.md - Per-column failure isolation + observability (parallel: false)
- [ ] 003.md - QA close-out — validation, no-regression check, full suite, docs, epic close (parallel: false)

Total tasks: 3
Parallel tasks: 0
Sequential tasks: 3
Estimated total effort: M-L overall, smaller than native-tables-rollups
