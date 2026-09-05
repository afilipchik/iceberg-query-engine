---
name: query-ui
description: Built-in web UI for `serve` nodes — recent-query list, per-query debugging detail, and engine statistics — backed by an in-memory query log and JSON endpoints
status: backlog
created: 2026-09-05T04:05:24Z
---

# PRD: query-ui

## Executive Summary

Give every `query_engine serve` node a built-in web UI, reachable at
`http://<node>:7777/ui`, that shows (1) the last N queries the node ran,
(2) everything the engine knows about one query that helps debug it, and
(3) engine statistics over the recent window. The UI is a static,
dependency-free page embedded in the binary and served by the existing
hyper server; it reads three new JSON endpoints (`/queries`,
`/queries/{id}`, `/stats`) that are equally useful from `curl`.

Today the only observability a running node offers is `x-qe-*` response
headers on the request that produced them and `queries_total` /
`queries_failed` counters that nothing exposes. Once a response has been
consumed, the plan, timings, memory, spill and distribution facts are
gone. Debugging a slow or failed query means re-running it under
`QE_*` debug switches from a shell on the box. This PRD closes that gap.

## Problem Statement

- **No history.** A node cannot answer "what ran in the last ten minutes,
  what failed, and why." Both front doors (HTTP `/sql` and Arrow Flight)
  funnel through `execute_statement`, which counts queries but records
  nothing.
- **Per-query debug facts are produced and then discarded.**
  `QueryMetrics` already carries parse/plan/optimize/execute timings, peak
  memory, spill bytes, files pruned by stats and by partition, and the
  rollups that answered the query. `/sql` forwards a fraction of that as
  headers and drops the rest; Flight puts some into a trailing metadata
  batch. The optimized logical plan and the physical operator tree —
  the first things one looks at when a query is slow — are never exposed
  by `serve` at all (only the CLI `--plan` flag prints them).
- **"Peak memory" is not a peak.** `metrics.peak_memory_bytes` is
  `memory_pool.used()` read after the last batch, i.e. the residual after
  reservations were released. A query that spilled at 8GB reports ~0.
- **No statistics.** Throughput, latency distribution, error rate, spill
  volume, distributed-vs-local share and pool occupancy are not computed
  anywhere; the cluster testbed's `verify` step derives them by hand from
  logs.
- **Why now.** The engine is entering a phase of multi-process cluster
  runs (distributed M2/M2.5, Flight clients, Gravitino/Pulsar sources),
  where the person debugging is not the person who issued the query.

## Prior Art — what other engines expose, and what we take from each

| Engine | Query history | Per-query detail | Statistics | Delivery |
|---|---|---|---|---|
| **Trino / Presto Web UI** | Live list of queries: id, user, source, state, elapsed, CPU, peak memory, progress, SQL preview; filters by state and text; auto-refresh | Overview (submission/completion time, queued/analysis/planning/execution time, input/output rows and bytes, peak user/total memory, spilled data, error type/code/stack), Live Plan DAG, Stage performance, Splits timeline, References (tables/columns), raw JSON | Cluster overview tiles: running/queued/blocked queries, active workers, rows/s, bytes/s, memory reserved | React SPA bundled into the coordinator; JSON from `/v1/query` |
| **ClickHouse** | No list UI; `system.query_log` and `system.processes` tables queried by SQL (`play.html` console is embedded in the binary) | Very wide row: read/written/result rows and bytes, memory_usage, exception + stack, query_kind, tables/columns/projections touched, `normalized_query_hash`, used functions/storages, thread ids, ProfileEvents map, client/interface | Derived by querying the log; `system.metrics`, `system.events`, `dashboard.html` (embedded) | Single-file HTML embedded in the server, zero build |
| **Snowflake Query History** | Table: id, SQL text, status, user, warehouse, duration, rows, start time; rich filtering; last-N with pagination | Query Profile: operator DAG with per-node time %, rows, and **pruning stats (partitions scanned / total)**, spilling to local/remote, most expensive nodes | Account usage views | Hosted SaaS |
| **Spark UI** | Jobs/SQL tabs list completed & running queries with duration and status | SQL tab: physical plan DAG with per-operator metrics (rows, time, spill), stages/tasks, event timeline | Executors page: memory, GC, tasks | Server-side UI in the driver |
| **Dremio Jobs** | Jobs list: id, user, duration, status, query type, SQL | Job profile: phases → operators with setup/process/wait time, memory, rows; planning phases with timings | Cluster tiles | React SPA |
| **Doris / StarRocks** | `SHOW PROCESSLIST`, audit log table; FE Web UI query page | Query Profile: per-fragment/operator counters (rows, bytes, time, memory, spill) | FE metrics endpoint | Embedded FE UI |
| **Pinot** | Query console only; no history | Response stats per query: numServersQueried, numSegmentsProcessed/Matched, numDocsScanned, numEntriesScanned, timeUsedMs | Controller/broker metrics | React SPA in controller |
| **DuckDB / DataFusion** | None (embedded engines) | `EXPLAIN ANALYZE` / profiling tree with per-operator timing and cardinality; DuckDB's `ui` extension is a notebook, not a query log | None | n/a |

**Design conclusions drawn from the table:**

1. **The universal shape is list → detail → stats.** Every server-side
   engine converges on a recency-ordered list with state, duration,
   rows and memory; a detail page with SQL, phase timings, resource use,
   the plan and the error; and a set of headline tiles. This PRD builds
   exactly that and nothing exotic.
2. **Debug value is in the facts the engine already computes, exposed
   whole.** ClickHouse's log row and Trino's overview are wide precisely
   so nobody has to re-run a query to learn what happened. We expose
   every `QueryMetrics` field, plus the plans, plus the distribution
   record, per query.
3. **Pruning counters are first-class** (Snowflake, Pinot): this engine
   already counts files pruned by stats and by partition; the UI shows
   them as "scanned / pruned" the way Snowflake does.
4. **Embedding beats a frontend toolchain for a single-binary engine.**
   ClickHouse ships `play.html`/`dashboard.html` inside the server
   binary; this repo is Rust-only with no Node in CI (the box has an
   EOL Node 18). The UI is therefore hand-written HTML/CSS/ES-module JS
   compiled in with `include_str!`, so `cargo build` remains the only
   build step. Trino/Pinot-style React bundles are explicitly rejected.
5. **Per-operator runtime metrics are the next tier, not this one.**
   Trino/Spark/Doris/DuckDB all show rows and time per operator. This
   engine has no operator-level counters yet (`display_plan` prints
   names only). We show the operator tree now and leave per-operator
   counters to a follow-up epic so this one stays ≤10 tasks.
6. **Live state matters.** Trino shows running queries; a list that
   only shows finished queries hides the hang you are debugging. A
   record is inserted when the query starts and completed when it ends.

## User Stories

1. **Operator triaging a slow cluster.** "I open `/ui` on any node and
   see the last 200 queries with state, elapsed, rows and memory. I sort
   by elapsed, click the slow one, and see its phase timings, whether
   it distributed (and how the shards were divided), whether it spilled,
   and the physical plan."
   - *Acceptance*: list loads in <200ms with 1,000 records in the ring;
     detail shows every field named in Functional Requirements §2.
2. **Engineer debugging a failed query reported by someone else.** "A
   Flight client got `InvalidArgument`. I search the query list for the
   table name, open the failed entry, and read the exact error message,
   the front door it came through, the client address and the SQL."
   - *Acceptance*: failed queries carry error kind + full message; the
     list filter matches SQL text and error text; Flight-originated
     queries are listed with `front_door: flight`.
3. **Engineer confirming the memory story.** "After a run at
   `--memory-limit 1G` I want to see the pool's high-water mark during
   the query, bytes spilled and partitions spilled, without re-running
   under `QE_SPILL_DEBUG`."
   - *Acceptance*: `peak_memory_bytes` is a true high-water mark
     (a query that spills reports a peak near the limit, not ~0);
     spill bytes match the `QE_SPILL_DEBUG` stderr totals.
4. **Anyone checking that a rollup or pruning fired.** "The query got
   fast; I need to know whether a materialized rollup answered it or
   Iceberg pruning skipped files."
   - *Acceptance*: detail shows `rollup_answered` names and
     files pruned by stats / by partition.
5. **Operator glancing at the node's health.** "One screen with
   queries per minute, p50/p95/p99 latency, error rate, running count,
   spilled bytes, pool used vs. limit, cluster members and uptime."
   - *Acceptance*: `/stats` returns all of those; tiles update on a
     5-second refresh; numbers reconcile with the query list.
6. **Scripted user.** "I `curl /queries?limit=20&state=failed` in a CI
   step and get JSON I can assert on."
   - *Acceptance*: endpoints are plain JSON, documented on `GET /`, and
     the `cluster_local.sh verify` gate exercises them.

## Functional Requirements

### 1. Query log (per node, in memory)
- Every statement that reaches `execute_statement` (HTTP `/sql` and
  Flight `DoGet`) gets a record with a UUID `query_id`, inserted at
  start with `state: running` and finalized on completion.
- Bounded ring buffer, default 1,000 records, `--query-log-size N`
  (`QE_QUERY_LOG_SIZE`), floor 10. Oldest finished records evict first;
  the log never grows unbounded and never blocks execution (a mutex
  held for microseconds, no allocation on the hot path beyond the
  record itself).
- `/fragment` executions (worker side of a distributed query) are
  recorded with `kind: fragment` and the initiator's address so a
  worker's list explains its own load.

### 2. Per-query record (the debugging surface)
Identity and lifecycle: `query_id`, `node_id`, `front_door`
(`http`|`flight`|`fragment`), `client_addr`, `submitted_at`,
`finished_at`, `state` (`running`|`finished`|`failed`), `sql`,
`statement_kind` (`select`|`ddl`|`dml`|`other`), `result_format`,
`requested_mode` (`auto`|`force`|`off`).

Timing (ms, float): `elapsed_ms`, `parse_ms`, `plan_ms`, `optimize_ms`,
`execute_ms`.

Output: `rows`, `result_bytes` (encoded body size when known),
`batches`.

Resources: `peak_memory_bytes` (true high-water mark of the pool while
the query ran), `memory_limit_bytes`, `spill` {`bytes`, `partitions`,
`files`, `read_back_ms`} or null.

Storage facts: `files_pruned_by_stats`, `files_pruned_by_partition`,
`rollup_answered: [..]`, `tables: [..]` (base tables referenced,
derived from the plan).

Plans: `optimized_plan` (Display of the optimized `LogicalPlan`) and
`physical_plan` (`display_plan` of the operator tree), captured inside
`ExecutionContext::sql` at zero extra planning cost. Null for
distributed runs where the initiator did not plan locally.

Distribution: `distributed: bool`, `fallback_reason`, and when
distributed the full `distribution` record (`shards`, `imbalance`,
`wall_time_spread`, per-node assignments) that `/sql` today truncates
into a header.

Failure: `error: {kind, message}` where `kind` is the `QueryError`
variant name.

### 3. Endpoints (JSON, on the existing hyper server)
- `GET /queries?limit=N&state=running|finished|failed&q=<substring>` —
  newest first, `limit` default 100, max = ring size. Each element is
  the record minus plans and full distribution (list payload stays
  small); `sql` is truncated to 200 chars with `sql_truncated: true`.
- `GET /queries/{id}` — full record. 404 with a message when evicted.
- `GET /stats` — `uptime_s`, `node_id`, `queries: {total, running,
  finished, failed, distributed, local}`, `latency_ms: {p50, p95, p99,
  max}` over the ring, `per_minute: [{minute_start, count, failed,
  p95_ms}]` for the last 60 minutes, `rows_total`, `bytes_total`,
  `spilled_bytes_total`, `spill_queries`, `memory: {used, peak, max}`,
  `errors_by_kind: {kind: n}`, `tables: {name: query_count}`,
  `slowest: [{query_id, elapsed_ms, sql_preview}]` (top 5), `cluster:
  {members, ready}`.
- `GET /tables` — registered tables with column names and types (the
  Flight `ListFlights` information over HTTP).
- `GET /ui`, `GET /ui/` and `GET /ui/*` — the embedded assets.
  `GET /` keeps its text index and gains the new routes.
- All new endpoints answer before readiness (an empty log is a valid
  answer) so the UI works while tables load and shows the load error.

### 4. UI
- **Overview**: stat tiles (running, qpm, p95, error rate, spilled,
  pool used/limit, members, uptime), a 60-minute queries-per-minute
  sparkline, a latency histogram, and the 10 most recent queries.
- **Queries**: table of the last N (N selectable: 50/100/500/all)
  with state pill, submitted time, elapsed, rows, peak memory, spill
  flag, distributed flag, front door, SQL preview; filter box (SQL and
  error text); state filter; column sort; 2-second auto-refresh with a
  pause toggle; deep-linkable rows (`#/query/<id>`).
- **Query detail**: SQL (monospace, copy button), status/error banner,
  a phase timeline bar (parse/plan/optimize/execute proportional),
  a facts grid (all §2 fields), distribution table when present,
  optimized and physical plan blocks, raw JSON toggle.
- **Cluster**: `/cluster` rendered as a table (id, address, flight
  address, ready, last seen).
- **Tables**: `/tables` rendered with expandable schemas.
- **SQL console**: textarea → `POST /sql?format=json`, results grid,
  the produced `query_id` linked to its detail (uses the existing
  endpoint; adds nothing server-side beyond the `x-qe-query-id`
  header).
- Light and dark themes via `prefers-color-scheme`; no external fonts,
  scripts or network calls; readable on a 1280px laptop.

## Non-Functional Requirements

- **Zero cost when idle, negligible when busy**: recording a query is
  one short mutex hold and one string clone of the SQL; plans are
  rendered from objects the engine already built. Target: <50µs per
  query overhead, unmeasurable in the TPC-H suite.
- **Bounded memory**: with the default ring and 1MB SQL bodies the worst
  case is ~1GB; plans are capped at 64KB each and SQL at 64KB in the
  record (truncated with a marker), bringing the realistic ring to
  <10MB.
- **No new crates.** `hyper`, `serde_json`, `uuid`, `chrono` are
  already dependencies. No Node/npm anywhere in the build.
- **Memory-safety rule respected**: nothing here opts out of anything;
  the log is bounded; the peak tracker is an atomic `fetch_max`.
- **Same answers through every door**: the log is fed from
  `execute_statement`, so HTTP and Flight cannot disagree.
- **Tests run under the safe-build wrapper**, like everything else.

## Success Criteria

- G1. On a running node, `curl /queries` after 22 TPC-H queries lists
  22 finished records; `/queries/{id}` for each carries non-null
  physical and optimized plans, phase timings summing to within 5% of
  `elapsed_ms`, and rows equal to the `x-qe-rows` header.
- G2. A query run at `--memory-limit 64M` that spills reports
  `peak_memory_bytes` ≥ 50% of the limit and `spill.bytes` > 0 (the
  pre-fix value was ~0).
- G3. A Flight `DoGet` query and an HTTP `/sql` query of the same text
  produce records that differ only in `query_id`, `front_door`,
  `client_addr`, timing and `result_format`.
- G4. `/stats` tallies reconcile with the list: `queries.total` =
  finished + failed + running, and `per_minute` counts sum to the
  number of records in the window.
- G5. `cluster_local.sh verify` gains a step that hits `/queries`,
  `/stats` and `/ui` on every node and fails on non-200 or on a list
  that does not contain the verify step's own queries.
- G6. `tests/ui_tests.rs` (in-process server) covers ring eviction,
  running-state visibility during a slow query, filter/limit parsing,
  fragment records on a 3-node scatter, and static asset serving.
- G7. Full test suite green; TPC-H SF=1 timings unchanged within noise.

## Constraints & Assumptions

- The UI is served by the node it observes; there is no cross-node
  aggregation (the Cluster page links to each member's `/ui`). A
  cluster-wide view is a follow-up once `/queries` exists everywhere.
- The log is in-memory and lost on restart. Persisting to a native
  table (ClickHouse-style `system.query_log`) is a follow-up.
- `peak_memory_bytes` is the pool-wide high-water mark between the
  query's start and end; concurrent queries share a pool, so the number
  attributes the whole pool to each overlapping query. The record says
  so via `concurrent_at_start`.
- No authentication: the endpoints are as open as `/sql` already is.
- No query cancellation from the UI: the engine has no cancellation
  token; a "Cancel" button would lie.
- Assumed answers to the brainstorming questions, since this ran
  unattended: users are the engine's own developers and cluster
  operators; success is "never re-run a query just to see what it
  did"; out of scope is anything that needs new engine instrumentation
  beyond a peak tracker.

## Out of Scope

- Per-operator runtime metrics (rows/time per operator, EXPLAIN
  ANALYZE) — separate epic; the plan tree is shown without them.
- Cross-node aggregated history; persistence of the log; retention
  beyond the ring.
- Query cancellation, user/session identity, authentication, RBAC.
- Editing/saving queries, notebooks, charts over result data.
- The CLI/REPL (they already print metrics; unchanged).
- Any change to `/sql` response semantics beyond adding the
  `x-qe-query-id` header.

## Dependencies

- Internal: `execute_statement` (`src/distributed/server.rs`),
  `QueryMetrics` (`src/execution/context.rs`), `MemoryPool`
  (`src/execution/memory.rs`), `display_plan` (`src/physical/plan.rs`),
  `cluster_view`, Flight `DoGet` (`src/distributed/flight.rs`),
  `scripts/cluster_local.sh`.
- External: none new. Browser only needs ES2020.
