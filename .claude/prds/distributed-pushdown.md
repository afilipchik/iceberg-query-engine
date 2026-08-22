---
name: distributed-pushdown
description: ClickHouse/Trino-style computation pushdown so distributed TPC-H stops shipping tables and starts shipping answers
status: backlog
created: 2026-08-22T05:07:38Z
---

# PRD: distributed-pushdown

## Executive Summary

Distributed SF=1 runs 4.33s against 1.38s single-process — 3.1x SLOWER —
because 21 of 22 TPC-H queries take the gather path: workers stream their raw
table shards to the initiator, which does all the compute. That is the one
architecture both ClickHouse and Trino exist to avoid: **move the query to the
data, ship back partial states, never the table.** This epic generalizes the
existing (correct, digest-checked) scatter machinery from "single table, bare
aggregate" to the ClickHouse replicated-dimension model with Trino's
partial-final decomposition and TopN pushdown, targeting most of TPC-H on the
scatter path and total distributed time at or below single-process.

## Problem Statement

Measured (2026-08-21, 3 workers, one host): scatter handles exactly one query
(Q06). Everything with a join, ORDER BY, LIMIT, HAVING, or subquery falls to
gather, which re-streams ~an entire table's shards over TCP per query, per
run. Q05 733ms vs 61ms local; Q09 794ms vs 124ms. The restriction is
syntactic, not architectural: the partial/final SQL rewriter and the
digest-checked split assignment already work.

## How ClickHouse and Trino do it (analysis)

**ClickHouse** (shard-local execution):
- A distributed query is REWRITTEN and sent to each shard; the shard runs the
  full fragment — scans, joins against local/replicated dimension tables,
  partial aggregation — over its own data. The initiator merges partial
  aggregate STATES (sum of sums, sum of counts...), then applies HAVING /
  ORDER BY / LIMIT.
- Dimensions are expected to be replicated (or broadcast via GLOBAL JOIN /
  GLOBAL IN when they are not). The fact table is the only sharded one.
- LIMIT/TopN is pushed down (`distributed_push_down_limit`): shards return
  their local top-N, the initiator re-sorts the union.
- What it refuses to do implicitly, we refuse loudly: queries whose
  correctness needs a shard to see other shards' rows.

**Trino** (stage DAG + exchanges):
- Plans become stages: source stages scan+filter+PARTIAL aggregate, exchanges
  repartition, final stages FINAL-aggregate; AVG is split into SUM/COUNT at
  the partial stage — exactly our qe_a0s/qe_a0c split.
- Join distribution is cost-chosen: BROADCAST the small build side to every
  worker vs PARTITIONED (shuffle both). With replicated tables, broadcast is
  free — the build side is already everywhere.
- TopN is decomposed: partial TopN per worker, final TopN at the coordinator.
- Dynamic filters prune the fact scan from the build side (we already have
  single-node runtime filters; distributed DF is out of scope).

**The combination we take** (best of both, no shuffle required):
ClickHouse's *replicated-dimension, sharded-fact, run-the-whole-query-locally*
model — which fits this engine exactly, because every node in the testbed and
in metastore deployments already registers ALL tables — plus Trino's
partial/final decomposition discipline (which our rewriter already implements
for aggregates) extended with final-stage HAVING/ORDER BY/LIMIT and partial
TopN. Trino's shuffle (M3) remains the future answer for what this cannot
cover; nothing here blocks it.

## User Stories

1. **Operator**: "A TPC-H-shaped query with joins against a 3-node cluster is
   answered from partial states — network traffic per query is rows-of-answer,
   not rows-of-table."
   - *Acceptance*: >= 14 of the 22 TPC-H queries run the scatter path under
     `distributed=1`; the distribution header shows per-node result rows
     comparable to answer size, not shard size.
2. **Operator**: "Distributed SF=1 total is at least 2.5x faster than today's
   4.33s, at or below the 1.38s single-process time."
   - *Acceptance*: published benchmark, same methodology as 2026-08-21.
3. **Correctness owner**: "Every scatter answer is cell-exact vs DuckDB, and
   every shape scatter cannot prove correct is refused by name and falls to
   gather (auto) — never a wrong answer."
   - *Acceptance*: forced-distributed 22/22 cell-exact; refusal unit tests
     for each safety rule.

## Functional Requirements

- Scatter planning accepts: multi-table joins (incl. comma joins and JOIN
  syntax), derived tables in FROM, EXISTS/IN/scalar subqueries, GROUP BY +
  decomposable aggregates (COUNT/SUM/MIN/MAX/AVG), HAVING, ORDER BY,
  LIMIT/OFFSET — under the safety rules below.
- **Shard-table selection**: among tables referenced EXACTLY ONCE in the whole
  statement, in the main query's FROM tree (never inside a subquery
  expression), and on a shard-safe join side (inner joins anywhere; outer
  joins only with the candidate on the preserved side), pick the LARGEST by
  provider statistics. No eligible table -> gather, reason named.
- **Safety refusals** (each by name): DISTINCT / COUNT(DISTINCT) / other
  non-decomposable aggregates; window functions; more than one Aggregate
  level in the main plan; aggregates inside derived tables; set operations;
  CTEs (as today); sharded-table references inside subquery expressions.
- **Merge phase**: final SQL applies merged aggregates + HAVING (rewritten
  over merged states) + ORDER BY + LIMIT/OFFSET. Non-aggregate queries with
  ORDER BY [+LIMIT]: workers run partial TopN (local sort + LIMIT n+offset
  when a limit exists), initiator re-sorts and applies the true LIMIT/OFFSET.
- Fragment execution registers ALL base-context tables and overrides only the
  sharded one (today it registers only the sharded table).
- `distributed=auto` picks scatter for every newly-supported shape; gather
  stays the fallback and is unchanged.

## Non-Functional Requirements

- Correctness gates unchanged in spirit: splits digest agreement, no silent
  local fallback under distributed=1, cell-exact vs DuckDB (1e-6 float).
- Replicated-tables assumption is CHECKED, not assumed: scatter requires
  every referenced table to resolve on the worker; a worker missing a table
  fails the query loudly (existing behavior).
- No regression to single-process performance or to the existing scatter/
  gather shapes; all existing suites green.

## Success Criteria

1. >= 14/22 TPC-H queries on the scatter path (shape recorded per query).
2. Distributed SF=1 total <= 1.75s (from 4.33s); heavy queries (Q05, Q09)
   within 2x of single-process.
3. 22/22 cell-exact vs DuckDB under distributed=1, from every node.
4. Refusal-by-name unit tests for every safety rule; M1/M2 gates green;
   971+ tests green.
5. Results published to README + CLAUDE.md; DISTRIBUTED-DESIGN.md amended.

## Constraints & Assumptions

- Every node registers all tables (testbed `--data`, metastore mode). This is
  the ClickHouse replicated-dim assumption; deployments with true partitioned
  storage need M3 shuffle, out of scope.
- One host, 3 processes: shared memory bandwidth. The benchmark measures
  elimination of shipping + coordination, not linear scaling.

## Out of Scope

- M3 shuffle / repartitioned joins (the DISTRIBUTED-DESIGN.md plan stands).
- Distributed dynamic filters; distributed window functions (stay gather).
- COUNT(DISTINCT) via HLL-style mergeable sketches (future).
- Gather-path caching of shipped shards.
- Multi-level aggregate scatter (Q13-shape); stays gather.

## Dependencies

- Existing: `plan.rs` rewriter (qe_g/qe_a machinery), coordinator fragment
  execution + digest checks, splits/LPT assignment, `distributed_validate.py`
  and `cluster_local.sh` harnesses, provider statistics for size selection.
