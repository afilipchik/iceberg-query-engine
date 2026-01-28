# Query Engine Performance Optimization Plan

## Current State (SF=0.1, ~600K rows in lineitem)

### Latest Benchmark Results (2026-01-27)

| Query | Time (SF=0.01) | Time (SF=0.1) | Category | Status |
|-------|----------------|---------------|----------|--------|
| Q01   | 8ms            | ~40ms         | Aggregation | ✅ Optimized |
| Q02   | 3ms            | ~10ms         | Multi-join | ✅ Good |
| Q03   | 10ms           | ~50ms         | 3-way join | ✅ Good |
| Q04   | 0.3ms          | ~3ms          | Subquery | ✅ Fast |
| Q05   | 25ms           | ~250ms        | 6-way join | Needs work |
| Q06   | 0.6ms          | ~8ms          | Filter+Agg | ✅ Fast |
| Q07   | 227ms          | ~2.6s         | Multi-join | Needs work |
| Q08   | 20ms           | ~60ms         | 8-way join | ✅ Good |
| Q09   | 932ms          | ~10s          | 6-way join | **Bottleneck** |
| Q10   | 18ms           | ~130ms        | 4-way join | ✅ Good |
| Q17   | 7ms            | ~200ms        | Scalar subquery | ✅ Fixed |
| Q18   | 151ms          | ~400ms        | IN subquery | ✅ Working |
| Q21   | 945ms          | **10.2s**     | Nested EXISTS | **Main bottleneck** |
| Q22   | 1ms            | ~50ms         | NOT EXISTS | ✅ Fixed |

**Total (SF=0.01)**: ~2.5s for all 22 queries

### Recent Improvements

1. **Q21 Decorrelation Fix** (2026-01-27)
   - SubqueryDecorrelation now correctly converts EXISTS/NOT EXISTS to SEMI/ANTI joins
   - Q21 at SF=0.1: 61s → 10.2s (**6x faster**)
   - Still slow due to triple lineitem scan with hash joins

2. **DelimJoin Infrastructure** (2026-01-27)
   - Implemented DuckDB-style DelimJoin/DelimGet for O(n+m) subquery execution
   - Fixed column resolution (inner column names for schema)
   - Enabled for simple single-EXISTS cases
   - Complex patterns (Q21/Q22 with multiple EXISTS) fall through to SubqueryDecorrelation

3. **Previous Optimizations**
   - Morsel-driven parallelism (8x on Q1)
   - Parallel hash join build
   - Subquery result caching/memoization
   - Join reordering (158x improvement on Q8)

## Implementation Roadmap

### Phase 1: Quick Wins ✅ COMPLETE
1. [x] Fix filter partition propagation bug
2. [x] Add simple parallel scan with rayon (morsel-driven parallelism)
3. [x] Implement subquery result caching (memoization by correlation key)

### Phase 2: I/O Optimization ✅ COMPLETE
4. [x] Streaming Parquet reader (StreamingParquetReader)
5. [x] Row group pruning with statistics (vectorized_agg.rs)
6. [x] Async I/O for Parquet (AsyncParquetReader with tokio)

### Phase 3: Parallel Execution ✅ COMPLETE
7. [x] Partitioned aggregation (morsel_agg.rs)
8. [x] Parallel hash join build (rayon-based parallel build)
9. [x] Work-stealing scheduler (crossbeam work-stealing example)

### Phase 4: Subquery Decorrelation ✅ MOSTLY COMPLETE
10. [x] **DelimJoin Infrastructure**
    - `DelimJoinNode`, `DelimGetNode` logical plan nodes
    - `DelimJoinExec`, `DelimGetExec` physical operators
    - `FlattenDependentJoin` optimizer rule
    - Shared `DelimState` for efficient value passing
    - Fixed column resolution for inner column names
    - **ENABLED**: Simple single-EXISTS/NOT EXISTS flattened via DelimJoin
    - Complex patterns fall through to SubqueryDecorrelation

11. [x] **SubqueryDecorrelation improvements**
    - EXISTS/NOT EXISTS → SEMI/ANTI joins
    - Proper predicate extraction
    - Q21 now uses efficient hash joins (6x faster)

### Phase 5: Advanced Optimizations (TODO)
12. [ ] **Multiple EXISTS flattening** - Handle Q21 pattern with combined DelimJoin
13. [ ] **Parallel SEMI/ANTI joins** - Speed up Q21 nested join execution
14. [ ] **Expression vectorization** - CPU-bound query optimization
15. [ ] **Cost-based join ordering** - Better multi-way join plans (Q5, Q7, Q9)

## Current Architecture

```
SQL Query
    │
    ▼
┌─────────────────┐
│   Optimizer     │
│  - ConstantFolding
│  - PredicatePushdown
│  - FlattenDependentJoin (simple EXISTS → DelimJoin)
│  - SubqueryDecorrelation (complex → SEMI/ANTI joins)
│  - JoinReorder
│  - ProjectionPushdown
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Physical Plan   │
│  - DelimJoinExec (O(n+m) for simple EXISTS)
│  - HashJoinExec (SEMI/ANTI for complex patterns)
│  - HashAggregateExec (parallel)
│  - Morsel-driven scan
└─────────────────┘
```

## Key Files

| Component | File | Status |
|-----------|------|--------|
| DelimJoin logical | `src/planner/logical_plan.rs` | ✅ Complete |
| DelimJoin physical | `src/physical/operators/delim_join.rs` | ✅ Complete |
| FlattenDependentJoin | `src/optimizer/rules/flatten_dependent_join.rs` | ✅ Simple cases |
| SubqueryDecorrelation | `src/optimizer/rules/subquery_decorrelation.rs` | ✅ Complete |
| JoinReorder | `src/optimizer/rules/join_reorder.rs` | ✅ Complete |

## Success Criteria

- [x] All 22 TPC-H queries execute correctly
- [x] All 615+ tests pass
- [ ] Within 5x of DuckDB for Q1-Q16
- [ ] Within 20x of DuckDB for Q17-Q22
- [ ] Q21 under 1 second at SF=0.1

## Current Blockers

1. **Q21 Performance**: Still 10s at SF=0.1 due to triple lineitem scan
   - Solution: Extend FlattenDependentJoin for multiple EXISTS patterns
2. **Q9 Performance**: ~10s at SF=0.1 due to 6-way join
   - Solution: Better join ordering / parallel execution
