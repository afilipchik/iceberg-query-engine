---
name: native-tables
description: A first-class, writable, tiered (GPU/RAM/disk) native table format for blazingly fast analytical and data-viz queries
status: active
created: 2026-08-23T07:42:14Z
updated: 2026-08-25T00:00:00Z
---

> **Status note (2026-08-25).** 2 of 4 phases shipped and archived:
> `native-tables-foundation` (CREATE/read) and `native-tables-mutation`
> (INSERT/DELETE/UPDATE), both cell-exact validated at real SF=10 scale.
> Phase 3 (GPU/RAM/disk tiering) and phase 4 (materialized rollups) are
> not started. A correctness bug in the mutation epic's own QA close-out
> (`SpillableHashJoinExec`'s spill path, unrelated to native tables
> specifically — see `spill-join-correctness`) is tracked separately and
> does not block this PRD's own remaining phases.

# PRD: native-tables

## Executive Summary

Add a new storage mode — "native tables" — that users can `CREATE` and load
data into (from the engine's existing parquet/Iceberg/Lance sources),
optimized specifically for fast analytical and data-visualization query
workloads: general ad-hoc acceleration and precomputed rollups, full
read/write (INSERT/UPDATE/DELETE), and a tiered GPU/RAM/disk memory
hierarchy that puts hot data as close to the compute as the hardware allows.

Research this session (mining this engine's own six completed performance
epics, a 67-finding competitive study of ClickHouse/DataFusion/Velox/
Trino/Polars, and external research on DuckDB/ClickHouse/Druid/Pinot/
Arrow/RAPIDS cuDF) found the foundational piece is **already built and
already winning**: the engine's Arrow-IPC sidecar cache
(`src/storage/ipc_cache.rs`) is an owned, mmap-zero-copy columnar format
that at SF=100 measured **0.72x DuckDB's own native storage format** (i.e.
faster) — 22/22 cell-exact, three completed epics of hardening behind it.
It is not currently a first-class, user-creatable, persistent, writable
table type; it is an opportunistic read-through cache tied to mirroring an
existing parquet file. This PRD's foundation phase is substantially "expose
what already works as a real table," not "invent a format from scratch."

The full vision (general storage + rollups, mutable, GPU/RAM/disk tiered)
is a program on the scale of the six-epic performance effort already
completed in this repo, not a single epic. It is staged accordingly — see
Implementation Strategy.

## Problem Statement

The engine today has no writable table type at all: `src/planner/binder.rs`
binds exactly one statement shape, `Statement::Query`. There is no
`CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`, or `COPY` DDL/DML parsing
anywhere. Every table the engine queries is an external, read-only source
(parquet file, Iceberg table, Lance dataset) discovered at registration
time. For fast, iterative, viz-style analytical work — where a user wants
to materialize a working dataset once and then hit it repeatedly with
varied ad-hoc queries, or power a dashboard's known set of aggregate
queries — round-tripping through an external immutable source every time
is the wrong shape, and there is no "put this data somewhere fast and keep
querying it" primitive.

Separately, the engine already has three independent fast-column
mechanisms that each solve one piece of this problem but none is exposed
as a persistent, user-facing table: `MemoryTable` (RAM-resident, dies with
the process, no compression, not spill-aware), the IPC sidecar
(disk-resident via mmap, persistent, but existentially tied to shadowing a
specific parquet file's path/mtime/size — it cannot exist without a source
parquet table behind it), and the GPU aggregate cache (VRAM-resident,
per-query, ephemeral, numeric-columns-only, capped at 96 group×slot
combinations, no eviction policy implemented despite one being designed).

## User Stories

**As an analyst working interactively on this engine**, I want to load a
dataset into a native table once and then run many different ad-hoc
queries against it at speeds close to (or better than) DuckDB's native
format, without re-paying parquet decode cost on every query.
- Acceptance: `CREATE TABLE t AS SELECT ...` (or an equivalent CLI/API
  bulk-load path if SQL DDL isn't ready in the foundation phase) populates
  a native table from any existing source the engine can already read;
  subsequent `SELECT` queries against `t` measure at or better than the
  IPC-sidecar's existing SF=100 like-for-like numbers, cell-exact vs
  DuckDB.

**As someone building a dashboard on top of this engine**, I want either
raw-table queries fast enough to serve interactively, or precomputed
rollups that answer a known set of aggregate queries near-instantly, so
that the dashboard doesn't need its own separate caching layer.
- Acceptance: at minimum, raw-table native queries are fast (foundation
  phase); a later phase adds rollup definitions that the planner
  transparently substitutes for matching queries, with an explicit,
  documented staleness/refresh model (not silent, not assumed-instant).

**As someone whose working dataset changes over time**, I want to update a
native table incrementally (append new rows, correct existing ones,
remove stale ones) without re-loading the whole table from scratch.
- Acceptance: `INSERT INTO`, `UPDATE`, and `DELETE` against a native table
  produce correct, cell-exact results against a reference re-computation,
  each landing as its own phase after the foundation format is proven (see
  Implementation Strategy) — this explicitly is NOT foundation-phase scope,
  named here as the end-state the foundation format must not preclude.

**As the engine's maintainer**, I want native tables to inherit this
engine's existing memory-safety guarantee, not create a new way to OOM.
- Acceptance: no native table access path can be loaded/queried in a way
  that bypasses the engine's spill/budget machinery; an unbounded
  RAM-resident or VRAM-resident table has an explicit size bound or a real
  spill/eviction story before it ships, not an implicit "don't do that."

## Functional Requirements

1. **Table creation and load.** A way to create a native table and
   populate it from an existing engine-readable source (parquet, Iceberg,
   Lance, or a query result). SQL DDL (`CREATE TABLE ... AS SELECT`) is
   the target surface; a CLI-only bulk-load command (mirroring the
   existing `write-lance`/`load-lance` pattern in `src/main.rs`) is an
   acceptable, lower-risk first cut if SQL DDL parsing is a larger lift
   than the storage format itself.
2. **Storage format**, foundation phase: generalize the existing IPC
   sidecar mechanism (`src/storage/ipc_cache.rs`) from "read-through cache
   mirroring a parquet path" into a first-class, independently-persistent
   `TableProvider` — decoupled identity/staleness model (not tied to
   shadowing a source file's mtime/size), a real write entrypoint for
   arbitrary Arrow batches (the existing `src/storage/lance_write.rs`
   pattern — `write_batches`/`write_from_parquet`, streaming without
   double-materializing — is the template), and `distributed_splits`/
   `shard_by_splits` implemented for real (currently both default `None`
   on every `TableProvider` that doesn't specifically implement them).
3. **Read acceleration inherited, not rebuilt**: mmap zero-copy reads,
   dictionary-coerced low-cardinality string columns, footer/zone-map
   statistics feeding the engine's fastest aggregation path
   (`morsel_agg.rs::try_execute_dense_direct`) — this last one needs an
   explicit non-parquet equivalent, since that fast path currently reads
   its key-range bounds from parquet file footers specifically.
4. **Mutation** (later phase, not foundation): `INSERT`, then `UPDATE`/
   `DELETE` as a genuinely harder follow-on requiring a real deletion/
   update strategy (tombstones, MVCC, or rewrite-on-write — a real design
   decision for that phase, not assumed here).
5. **Tiered residency** (later phase): promote hot columns to GPU VRAM
   (generalizing the existing `src/physical/gpu.rs` cache — wider type
   coverage beyond today's f64-only/96-group cap, a real eviction policy
   using the already-designed-but-unbuilt `QE_GPU_CACHE_MB`), keep warm
   data RAM/mmap-resident (the foundation format), spill cold data to
   disk within the engine's existing spillable-operator machinery.
6. **Materialized rollups** (later phase, largest net-new infrastructure):
   register a table as a rollup of another; planner-level query matching
   that transparently substitutes the rollup for an equivalent base-table
   query (no such mechanism — no query-rewrite/substitution logic of any
   kind — exists anywhere in `src/optimizer/rules/` today); an explicit,
   documented refresh/staleness model.
7. **Distributed compatibility**: whatever ships must eventually work
   inside `serve`/sharded contexts (native tables should be nameable in
   `--tables`/metastore registration same as parquet/Iceberg/Lance today),
   though GPU-tier residency specifically may stay single-process-only for
   the same byte-exactness reasons the current GPU cache is already
   disabled in distributed contexts (float reduction order differs in the
   last bits; M1/M2 cluster gates demand byte-exact local answers).

## Non-Functional Requirements

- **Memory safety is never negotiable** (project mandate, unconditional
  for every phase of this feature): no native-table code path may load
  or hold data outside the engine's existing spill/budget machinery
  without an explicit, hard size bound. `MemoryTable`'s current lack of
  spill-awareness is a named gap this feature must not inherit uncritically
  for an unbounded user table.
- **Cell-exact correctness always**, validated the way every mechanism in
  this codebase already is (`tests/duckdb_validated.rs`-style comparison),
  including through mutation once that phase exists — this codebase has a
  specific, repeated history of subtle wrong-answer bugs in exactly the
  areas this feature touches (dictionary/perfect-hash group-key handling,
  cross-shard hashing) and the "row counts are not answers" lesson applies
  in full.
- **Benchmark-honesty discipline carries over**: report every native-table
  number against the SAME premises this program already insists on (both
  cache states where relevant, DuckDB-over-Iceberg AND DuckDB-native where
  relevant, CPU vs GPU reported separately per the current standing
  direction). If a rollup answers a query, that must be labeled distinctly
  from "the engine got faster at the real query" — flagged explicitly by
  this session's research as a real risk given how the industry's
  materialized-view story usually gets marketed.
- **No bare `cargo build`/`test`/`bench`**: every build in this program
  runs through `scripts/claude-safe-build.sh`.
- **No regression** to any existing table type (parquet/Iceberg/Lance) or
  to the IPC sidecar's existing behavior as an opportunistic read-through
  cache — generalizing it into a table type must be additive.

## Success Criteria

- G1 (foundation phase functional): a user can create a native table from
  an existing source and query it; reads are cell-exact vs DuckDB.
- G2 (foundation phase performance): native-table SF=10 and SF=100
  like-for-like numbers are at or better than the current IPC-sidecar
  cache-on numbers (today: SF=10 ~1.3x, SF=100 historically 1.21x
  like-for-like / 0.72x DuckDB-native) — this phase should not regress a
  mechanism that already works, and ideally improves on it now that it's
  not constrained to mirror an existing parquet file 1:1.
- G3 (memory safety): no native-table path can be driven into an
  uncontrolled OOM; either a hard size bound or real spill integration
  exists before any phase ships to users.
- G4 (staging discipline): each phase (foundation, mutation, GPU/tiering,
  rollups) ships independently, is independently useful, and is validated
  with the same "attribution before optimization, cell-exact after every
  lever, commit-or-revert" discipline as every completed epic in this
  repo — no phase blocks on a later phase's completion.
- G5 (rollups, when built): query-answer provenance (base table vs.
  rollup) is always distinguishable in output/logging, and staleness is
  bounded and documented, not silently assumed-fresh.

## Constraints & Assumptions

- **Concurrency model assumption, stated explicitly because it changes
  which later phases matter most**: this codebase's entire benchmark and
  design culture to date targets ONE query running at a time on a single
  box (the whole TPC-H suite, every completed epic, the distributed layer
  itself — which shards ONE query across nodes rather than serving many
  simultaneous independent queries). This PRD assumes that remains the
  primary target for the foundation and GPU-tiering phases. The
  materialized-rollup phase specifically exists to serve the OTHER
  pattern — many concurrent dashboard viewers hitting a small known query
  set — and its priority relative to the other phases should be revisited
  with the user once the foundation phase ships, rather than assumed now.
  If many-concurrent-viewers turns out to be the actual primary target,
  the rollup phase's priority likely rises relative to GPU-tiering.
- Builds on six completed performance epics
  (`duckdb-parity`/`radix-execution`/`streaming-fusion`/`decode-path`/
  `ipc-default`/`perf-marathon`) and this session's research
  (`.claude/plans/research/wave2-olap-engines.json` plus fresh external
  research, both cited in full in the epic).
- `arrow-rs` stays pinned at the current major (per the existing Lance
  FFI-free-compatibility constraint) — `GenericByteViewArray`
  (StringView/BinaryView) is already available at this pin, so dictionary/
  view-based encoding work is not blocked on an Arrow upgrade.
- The `larger-than-memory-support.md` plan document records several
  spillable operators as `PARTIAL ⚠️ / HAS BUGS / disabled by default` —
  this is a real, currently-open gap this feature's memory-safety
  requirement depends on, not a solved problem to build on top of blindly.

## Out of Scope (for this PRD; phased into later epics, not abandoned)

- **Mutation (INSERT/UPDATE/DELETE)** — explicitly the second phase, not
  the foundation. The engine has zero existing DML infrastructure; this is
  a substantial, separate piece of engineering and needs its own
  evidence-first design (tombstone vs. MVCC vs. rewrite-on-write) once the
  storage format itself is proven.
- **GPU/RAM/disk tiering with real promotion/eviction policy** — the third
  phase. Today's GPU cache has no eviction at all (a cap was designed,
  never built); building a genuine tiered hierarchy is real, separate work
  from making the base format fast.
- **Materialized rollups / query-rewrite substitution** — the fourth
  phase, and the largest net-new infrastructure of the whole program (no
  query-matching/rewrite mechanism exists anywhere in this codebase
  today). Sequenced last because its value depends on the
  concurrency-model question above, which is explicitly not yet settled.
- **Multi-tenant / many-concurrent-query serving** as a general engine
  capability — not attempted; the rollup phase serves ONE version of this
  need (precomputed answers), not general concurrent-query infrastructure.
- **Streaming/continuous ingest** (Druid/Pinot/ClickHouse-style) — this
  PRD's mutation phase targets discrete INSERT/UPDATE/DELETE, not a
  streaming ingest pipeline; no such pipeline exists in this engine today
  and building one is out of scope unless explicitly requested later.
- **A new on-disk compression codec family** (Delta/T64/FSST-class,
  identified by research as available headroom on the existing ~2.6x
  disk-footprint cost) — noted as a candidate future optimization, not
  committed to any phase of this PRD; revisit if the foundation phase's
  disk cost proves a real adoption blocker.

## Dependencies

- Builds directly on `src/storage/ipc_cache.rs`, `src/storage/lance_write.rs`,
  `src/physical/gpu.rs`, `src/physical/operators/scan.rs`
  (`MemoryTable`, `TableProvider` trait), and `src/execution/context.rs`
  (`register_*` family) — no new external dependency identified as
  required for the foundation phase.
- Depends on the `duckdb-parity-2` epic's dense-group-id remapping work
  (`.claude/epics/duckdb-parity-2/006.md`, in flight concurrently with this
  PRD's research) staying compatible: that work's flat, dense-id-indexed
  accumulator layout is naturally aligned with a native format's own
  columnar layout as long as group keys stay dense-integer or
  dictionary-shaped — the foundation phase's design should not fight it.
- Depends on `scripts/claude-safe-build.sh` for every build in this
  program, per this repo's standing sandboxed-build rule.
