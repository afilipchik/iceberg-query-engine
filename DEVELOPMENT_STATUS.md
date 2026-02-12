# Development Status

## Current State

- **619 tests passing** (103 lib, 24+18+98 module, 225 function validation, 128 SQL comprehensive, 23 TPC-H)
- **All 22/22 TPC-H queries executing correctly**
- **Branch**: `ea/ui_and_fixups` (54+ commits ahead of main)
- **Key features**:
  - MCP app (Node.js-based UI)
  - Morsel-driven parallelism (DuckDB-style work-stealing scheduler)
  - Branching metastore REST API client
  - 100+ Trino-compatible SQL functions (math, string, date/time, JSON, regex, binary, bitwise, URL, conditional, aggregate)
  - Interactive CLI REPL with tab completion, syntax highlighting, and multiple output formats
  - DelimJoin infrastructure for efficient correlated subquery execution

## GitHub Issues Mapping

| Issue | Title | Status | Notes |
|-------|-------|--------|-------|
| #9 | Performance is low vs DuckDB | Open | Q21=2790x slower (nested EXISTS O(n^2)), Q7/Q9=50-130x (multi-way joins). Excluding Q21: 11x slower. Several queries faster than DuckDB (Q02, Q04, Q06, Q14) |
| #10 | Function parity with Trino | Mostly resolved | 100+ functions implemented across math, string, date/time, JSON, regex, binary, bitwise, URL, conditional. Window functions remaining. |
| #11 | LIMIT doesn't work | Needs investigation | Likely off-by-one in LimitExec stream truncation or OFFSET interaction |
| #5 | Parallelize agentic work | Partially addressed | AGENT_COORDINATION.md exists, morsel parallelism implemented |

## TPC-H Performance Gap Analysis (SF=0.1)

| Query | Our Engine | DuckDB | Ratio | Notes |
|-------|------------|--------|-------|-------|
| Q01   |     41ms   |   15ms |  2.7x | |
| Q02   |      9ms   |   11ms |  **0.8x** | Faster than DuckDB |
| Q03   |     52ms   |   12ms |  4.3x | |
| Q04   |      3ms   |   16ms |  **0.2x** | 5x faster than DuckDB |
| Q05   |    256ms   |   24ms | 10.7x | |
| Q06   |      8ms   |   25ms |  **0.3x** | 3x faster than DuckDB |
| Q07   |   2656ms   |   20ms |  133x | Multi-way join bottleneck |
| Q08   |     63ms   |   19ms |  3.3x | |
| Q09   |    792ms   |   15ms |   53x | Multi-way join bottleneck |
| Q10   |    132ms   |   21ms |  6.3x | |
| Q11   |    128ms   |   34ms |  3.8x | |
| Q12   |     43ms   |   32ms |  1.3x | |
| Q13   |     52ms   |   35ms |  1.5x | |
| Q14   |     12ms   |   31ms |  **0.4x** | 2.6x faster than DuckDB |
| Q15   |     59ms   |   30ms |  2.0x | |
| Q16   |     20ms   |   19ms |  1.1x | Nearly equal |
| Q17   |    199ms   |   10ms |   20x | Scalar subquery |
| Q18   |    364ms   |   12ms |   30x | IN subquery |
| Q19   |    305ms   |   20ms |   15x | |
| Q20   |    120ms   |   26ms |  4.6x | |
| Q21   |  61373ms   |   22ms | 2790x | **Main bottleneck** - nested EXISTS |
| Q22   |     51ms   |   20ms |  2.6x | |
| **TOTAL** | **66.7s** | **0.49s** | **136x** | |

**Key findings:**
- Q21 takes 92% of total time (61s out of 66.7s) due to nested EXISTS/NOT EXISTS still executing with O(n^2) row-by-row evaluation
- Q7 and Q9 need better join optimization for multi-way joins (133x and 53x slower respectively)
- Several queries (Q02, Q04, Q06, Q14) are **faster** than DuckDB, demonstrating competitive performance on simpler query shapes
- Excluding Q21: 5.3s total vs 0.47s DuckDB (11x slower)

## Architecture Overview

The query engine follows a classic SQL query processing pipeline:

```
SQL String
    |
    v
Parser (sqlparser-rs)          -- src/parser/
    |  SQL AST
    v
Binder (Planner)               -- src/planner/binder.rs
    |  LogicalPlan
    v
Optimizer (7 rules)            -- src/optimizer/
    |  Optimized LogicalPlan
    v
Physical Planner               -- src/physical/planner.rs
    |  PhysicalOperator tree
    v
Streaming Execution            -- src/physical/operators/
    |  Stream<RecordBatch>
    v
Query Results
```

**Optimizer rules** (applied in order, up to 10 fixed-point iterations):
1. **ConstantFolding** -- Evaluate constant expressions at plan time
2. **PredicatePushdown** -- Push filters closer to table scans (first pass, before decorrelation)
3. **FlattenDependentJoin** -- DelimJoin-based subquery flattening for simple single-EXISTS cases
4. **SubqueryDecorrelation** -- Transform correlated subqueries into regular joins
5. **JoinReorder** -- Eliminate cross joins, optimize join order and build/probe sides
6. **PredicatePushdown** -- Second pass for post-decorrelation opportunities
7. **ProjectionPushdown** -- Push column projections to reduce data flow
8. **PredicateReordering** -- Reorder conjunctive predicates by estimated selectivity

## Recent Integration

- **Integrated PredicateReordering** from upstream main (commit bfd416e on `ea/ui_and_fixups`). This rule reorders conjunctive predicates within Filter nodes by estimated selectivity to evaluate cheaper/more selective predicates first.
- **Upstream's `join_reordering.rs` was NOT integrated** -- it was superseded by the project's superior `join_reorder.rs` which already handles cross join elimination, join graph construction, greedy ordering, and build/probe side optimization.
- Upstream repo (`origin/main`) was reset to its original state after cherry-picking the predicate reordering rule.

## Next Steps

Reference `IMPROVEMENT_SUGGESTIONS.md` for priority-ranked improvements. Key areas include:

1. **Q21 performance**: The nested EXISTS/NOT EXISTS pattern needs DelimJoin-based set execution to eliminate O(n^2) behavior. The infrastructure (DelimJoinExec, DelimGetExec, FlattenDependentJoin) is already in place but only handles simple single-EXISTS cases.
2. **Multi-way join optimization**: Q7 (133x) and Q9 (53x) would benefit from better join ordering heuristics for 5+ table joins.
3. **Window functions**: ROW_NUMBER, RANK, DENSE_RANK, LEAD, LAG, and other window functions are not yet implemented (see `.claude/plans/trino-function-implementation.md` Phase 6).
4. **Subquery performance**: Q17 (20x) and Q18 (30x) still use row-by-row correlated execution with memoization rather than fully decorrelated set-based joins.
5. **Array/Map type support**: Complex nested data types and their associated functions (Phases 4-5 of the function implementation plan).
