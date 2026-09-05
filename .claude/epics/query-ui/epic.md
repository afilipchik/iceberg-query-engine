---
name: query-ui
status: backlog
created: 2026-09-05T04:12:00Z
updated: 2026-09-05T04:12:00Z
progress: 0%
prd: .claude/prds/query-ui.md
github: (will be set on sync)
---

# Epic: query-ui

## Overview

Add an in-memory query log to `serve` nodes, expose it plus derived
statistics as JSON, and ship a dependency-free web UI embedded in the
binary. The engine side is small and surgical: capture the plans and a
true memory high-water mark inside the code paths that already produce
them, then record every `execute_statement` outcome in a bounded ring.
The UI is plain HTML/CSS/JS served by the existing hyper router.

## Architecture Decisions

1. **Feed the log from `execute_statement`, nowhere else.** HTTP and
   Flight already share it; fragments get their own call site in the
   `/fragment` handler. A record is `begin()`-ed before the spawn and
   `finish()`-ed after, so running queries are visible.
2. **Plans are captured where they are built.** `QueryMetrics` gains
   `optimized_plan: Option<String>`, `physical_plan: Option<String>`,
   `tables: Vec<String>` and `statement_kind`, filled in
   `ExecutionContext::sql` from the objects it already has. No
   re-planning, no second parse. Strings are capped at 64KB.
3. **Peak memory is a `fetch_max` high-water mark on `MemoryPool`.**
   `allocate`, `try_allocate` and `resize` update it; `peak()` reads it.
   The query log reads the pool peak at finish and reports it together
   with `concurrent_at_start` so overlap is visible rather than hidden.
   `metrics.peak_memory_bytes` switches to the true peak; the CLI's
   `Memory: peak=` line becomes correct for free.
4. **Ring buffer = `Mutex<VecDeque<QueryRecord>>` + monotonic seq.**
   Records are small structs; running ones are near the back so
   `finish(id)` scans from the back. Eviction drops the oldest
   *finished* record so a long-running query is never evicted while
   running. Capacity from `ServeOptions::query_log_size`.
5. **Stats are computed on request** from the ring and the existing
   atomics; with ≤10k records that is microseconds. No background
   aggregator, no histograms to maintain.
6. **UI is embedded via `include_str!`** from `src/distributed/ui/`
   (`index.html`, `app.js`, `style.css`) and served with correct
   content types under `/ui`. A hash-router SPA with `fetch()` polling;
   charts are inline SVG built by hand (sparkline, bar histogram, phase
   bar). Zero external resources, so it works air-gapped.
7. **New endpoints answer before readiness.** They never touch tables;
   the UI shows the load error string from `/readyz` when not ready.

## Technical Approach

### Backend Services
- `src/execution/memory.rs`: `peak: AtomicUsize`, updated in
  `allocate`/`try_allocate`/`resize`; `peak()`; unit tests.
- `src/execution/context.rs`: extend `QueryMetrics` (plans, tables,
  statement kind, batches); fill in `sql()`; set `peak_memory_bytes`
  from `pool.peak()`; helper `collect_scan_tables(&LogicalPlan)`.
- `src/distributed/query_log.rs` (new): `QueryRecord`, `QueryLog`
  (begin/finish/fail/get/list/stats), `StatsView`, `ListFilter`,
  serde `Serialize` for all; percentile + per-minute bucketing.
- `src/distributed/server.rs`: `NodeState.query_log`, `ServeOptions
  ::query_log_size`; `execute_statement` gains `front_door` +
  `client_addr` params and does begin/finish; `/sql` adds
  `x-qe-query-id`; new routes `/queries`, `/queries/{id}`, `/stats`,
  `/tables`, `/ui*`; `index()` text updated.
- `src/distributed/flight.rs`: pass `FrontDoor::Flight` + peer addr;
  add `query_id` to the trailing metadata JSON.
- `src/main.rs`: `--query-log-size` (env `QE_QUERY_LOG_SIZE`).

### Frontend Components
- `src/distributed/ui/index.html` — shell, nav, view containers.
- `src/distributed/ui/style.css` — tokens (light/dark), table, pills,
  tiles, grid, code blocks.
- `src/distributed/ui/app.js` — router (`#/`, `#/queries`,
  `#/query/<id>`, `#/cluster`, `#/tables`, `#/sql`), `api.js`-style
  fetch helpers, formatters (bytes, ms, relative time), views, SVG
  charts, poller with visibility-aware pause.

### Infrastructure
- `tests/ui_tests.rs`: in-process `spawn()` server tests.
- `scripts/cluster_local.sh verify`: new step hitting the endpoints on
  every node.
- Docs: `CLAUDE.md` (endpoints table, file structure, the peak-memory
  fix), `README.md` (a "Web UI" section).

## Implementation Strategy

Three layers, in dependency order: engine facts → log + endpoints →
UI. The UI tasks develop against the JSON contract in the PRD and can
proceed as soon as the endpoints compile. Verification closes the epic
with the integration suite and the cluster gate. Risks: (a) the
`display_plan` strings for very wide plans — capped; (b) mutex
contention on the ring under concurrent load — held for a push only;
(c) `peak` semantics with concurrent queries — documented, exposed.

## Task Breakdown Preview

- [ ] 001 — Engine debug facts: plans, tables, statement kind in
      `QueryMetrics`; `MemoryPool` high-water mark. (parallel: true)
- [ ] 002 — Query log ring + `/queries`, `/queries/{id}`, `/stats`,
      `/tables`; wiring from HTTP, Flight and fragments; CLI flag.
      (depends 001)
- [ ] 003 — UI shell, static serving, Overview + Queries list + Query
      detail views. (depends 002)
- [ ] 004 — Statistics view: tiles, sparkline, latency histogram,
      per-table and error breakdowns. (depends 003, parallel with 005)
- [ ] 005 — Cluster, Tables and SQL console views. (depends 003,
      parallel with 004)
- [ ] 006 — Verification: `tests/ui_tests.rs`, cluster gate step,
      docs, TPC-H timing check. (depends 004, 005)

## Dependencies

- Internal only (see PRD). Builds/tests through
  `scripts/claude-safe-build.sh`.

## Success Criteria (Technical)

- PRD G1–G7 as written. In particular: plans non-null for every local
  query; peak ≥ 50% of limit on a spilling query; HTTP and Flight
  records identical modulo identity/timing; stats reconcile with the
  list; `cluster_local.sh verify` step passes on 3 real processes.

## Estimated Effort

- 001: 4h · 002: 8h · 003: 8h · 004: 4h · 005: 4h · 006: 6h
- Total ≈ 34h; critical path 001→002→003→(004|005)→006 ≈ 30h.

## Tasks Created
- [ ] 001.md - Engine debug facts: plans and statement facts in QueryMetrics, MemoryPool high-water mark (parallel: true)
- [ ] 002.md - Query log ring and JSON endpoints wired from HTTP, Flight and fragments (parallel: false)
- [ ] 003.md - UI shell, embedded static serving, Overview + Queries list + Query detail views (parallel: false)
- [ ] 004.md - Statistics view: tiles, sparkline, latency histogram, breakdowns (parallel: true)
- [ ] 005.md - Cluster, Tables and SQL console views (parallel: true)
- [ ] 006.md - Verification: integration suite, cluster gate step, docs, timing check (parallel: false)

Total tasks: 6
Parallel tasks: 3
Sequential tasks: 3
Estimated total effort: 34 hours
