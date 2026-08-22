---
name: distributed-pushdown
status: backlog
created: 2026-08-22T05:07:38Z
updated: 2026-08-22T05:07:38Z
progress: 0%
prd: .claude/prds/distributed-pushdown.md
github: (will be set on sync)
---

# Epic: distributed-pushdown

## Overview

Generalize scatter from "one table, bare aggregate" to the ClickHouse
sharded-fact/replicated-dims model with Trino's partial-final discipline:
workers run the WHOLE query over their shard of one chosen table (all other
tables read from their full local copies), ship partial aggregate states or
local TopN rows, and the initiator finishes with merge aggregation + HAVING +
ORDER BY + LIMIT. Gather remains the untouched fallback for what the safety
rules refuse.

## Architecture Decisions

1. **Shard exactly one table; correctness by union-decomposability.** Running
   query Q per shard of table T and UNION-merging is correct when every result
   row derives from exactly one T row and the top-level aggregates are
   mergeable. Enforced rules (refuse by name otherwise):
   - T referenced exactly once in the entire statement, in the main FROM tree
     (derived tables allowed), never inside a subquery expression;
   - every outer join on T's path keeps T on the preserved side;
   - at most one Aggregate node in the main plan; none inside derived tables;
   - no DISTINCT / COUNT(DISTINCT) / non-decomposable aggs / windows / set
     ops / CTEs.
   Subqueries (EXISTS/IN/scalar) are fine when they touch only non-sharded
   tables — on a worker they see full replicas, so correlated lookups and
   global scalar aggregates are shard-invariant.
2. **Selection by statistics**: among eligible tables, shard the largest
   (provider `statistics().total_byte_size`, rows as tiebreak). TPC-H picks
   lineitem where present-once, else orders/part/supplier — matching the
   fact-table intuition Trino's CBO applies.
3. **Rewriter extension, not rewrite**: the existing AST rewriter already
   splits aggregates (AVG -> qe_a0s/qe_a0c). It gains: FROM clause passed
   through VERBATIM (joins, derived tables, subqueries — they execute on
   workers); HAVING moved to the final SQL rewritten over merged aggregates;
   ORDER BY/LIMIT/OFFSET moved to the final SQL; for non-aggregate queries a
   new TopN shape (partial = original + local ORDER BY + LIMIT n+offset when
   limited; final = re-sort + exact LIMIT/OFFSET over the concatenation).
4. **Fragment contexts get the full catalog**: register every base-context
   provider, then override the sharded table. One-line semantic change,
   the enabling fix for multi-table fragments.
5. **Gather untouched**; auto mode ranks scatter > gather as today (scatter
   plan probe succeeds -> scatter).

## Technical Approach

### Backend Services

- `src/distributed/plan.rs`: Net-1 syntactic gate relaxed (joins, derived,
  subqueries, ORDER BY, LIMIT, HAVING pass); table-reference census over the
  AST (main FROM tree vs subquery expressions, join-side tracking) + logical
  plan walk for aggregate/window/distinct checks; shard-table election by
  stats; rewriter emits partial/final with the new clauses; `MergeShape::TopN`
  added (or Concat + final_sql).
- `src/distributed/coordinator.rs`: fragment ctx registers all tables then
  overrides the sharded one; execute-final path already runs final_sql over
  qe_dist_partial — unchanged except it now also runs for TopN/Concat-with-
  final shapes.
- `src/distributed/gather.rs`, membership, transport: untouched.

### Frontend Components

None.

### Infrastructure

- `scripts/distributed_validate.py` / new bench runs reuse cluster_local.sh.

## Implementation Strategy

Land the fragment-catalog fix first (tiny, unblocks everything), then the
planner/rewriter in one task with unit tests at each rule, then the
correctness harness, then benchmark + publish. Safety rules err toward
refusal: a query wrongly sent to gather costs milliseconds; one wrongly
scattered costs a wrong answer.

## Task Breakdown Preview

1. Fragment contexts carry the full catalog (override only the shard).
2. Planner: reference census, safety rules, shard election; refusals by name
   with unit tests.
3. Rewriter: verbatim FROM, final-stage HAVING/ORDER BY/LIMIT, TopN shape;
   unit tests on partial/final SQL text.
4. Correctness gate: 22 TPC-H forced-distributed cell-exact vs DuckDB with
   expected-shape assertions; refusal integration tests; M1/M2 gates green.
5. Benchmark 3 workers SF=1; publish README/CLAUDE.md; amend
   DISTRIBUTED-DESIGN.md with the CH/Trino analysis and what was taken.

## Dependencies

- Existing scatter/gather/split/digest machinery; provider statistics.

## Success Criteria (Technical)

- >= 14/22 scatter; SF=1 distributed total <= 1.75s; 22/22 cell-exact from
  every node; all suites + gates green; docs published.

## Estimated Effort

5 tasks. The planner safety rules (task 2) carry the correctness risk;
the DuckDB gate catches what reasoning misses.

## Tasks Created
- [ ] 001.md - Fragment contexts carry the full catalog (parallel: true)
- [ ] 002.md - Scatter planner: census, safety rules, shard election (parallel: false)
- [ ] 003.md - Rewriter: verbatim FROM, final HAVING/ORDER/LIMIT, TopN shape (parallel: false)
- [ ] 004.md - Correctness gate: 22 TPC-H forced-distributed vs DuckDB + shapes (parallel: false)
- [ ] 005.md - Benchmark 3 workers SF=1 and publish (parallel: false)

Total tasks: 5
Parallel tasks: 1
Sequential tasks: 4
Estimated total effort: 23 hours
