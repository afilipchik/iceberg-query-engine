# Query Engine Performance Optimization Plan

## Current State (SF=10, ~60M rows in lineitem)

### Latest Benchmark Results: Morsel-Driven Parallelism

| Version | Q01 Time | vs DuckDB | Improvement |
|---------|----------|-----------|-------------|
| DuckDB | 84ms | 1x | baseline |
| **Morsel + Projected** | **227ms** | **2.7x** | **8x faster than original** |
| Morsel + Specialized | 570ms | 6.8x | 3.3x faster |
| Morsel + Generic | 970ms | 11.5x | 1.9x faster |
| Original Engine | 1,860ms | 22x | baseline |

### Benchmark Results vs DuckDB (Standard Execution)

| Query | Our Engine | DuckDB | Slowdown | Category | Improvement |
|-------|-----------|--------|----------|----------|-------------|
| Q01   | 2,679ms   | 84ms   | 32x      | Aggregation | **Morsel: 2.7x** |
| Q02   | 154ms     | ~20ms  | ~8x      | Multi-join | - |
| Q03   | 993ms     | 166ms  | 6x       | 3-way join | - |
| Q04   | 120ms     | ~30ms  | ~4x      | Subquery | 1.5x faster |
| Q05   | 5,374ms   | 156ms  | 34x      | 6-way join | - |
| Q06   | 549ms     | 66ms   | 8x       | Filter+Agg | **1.9x faster** |
| Q08   | 2,217ms   | ~200ms | 11x      | 8-way join | - |
| Q09   | 2,350ms   | ~200ms | 12x      | 6-way join | - |
| Q10   | 1,210ms   | 248ms  | 4.9x     | 4-way join | - |
| Q17+  | >5min     | ~500ms | >600x    | Correlated subquery | - |

### Changes Made This Session

1. **Implemented Morsel-Driven Parallelism** (morsel.rs, morsel_agg.rs)
   - `ParallelParquetSource`: Parallel row-group reading with work-stealing
   - Thread-local hash tables for aggregation with final merge
   - Column projection pushdown to Parquet reader
   - **8x improvement on Q1** (1.86s → 227ms)

2. **Fixed Critical Partitioning Bug** (filter.rs, project.rs)
   - Filter and Project operators now propagate `output_partitions()` from input
   - Was returning only ~3% of data (one partition instead of all)
   - All queries now return correct row counts

3. **Implemented Parallel Aggregation** (hash_agg.rs)
   - Added `aggregate_batches_parallel()` using rayon
   - Parallel partition collection using tokio::spawn
   - Parallel hash table building with merge
   - 3-4x improvement on aggregation-heavy queries (Q01, Q06)

### Root Cause Analysis

1. **Single-threaded execution**: DuckDB uses ~20 cores, we use 1
2. **Memory materialization**: We load all data into memory before processing
3. **No I/O optimization**: No predicate pushdown to Parquet row groups
4. **Correlated subquery O(n²)**: Nested loop evaluation for Q17-Q22
5. **Sequential hash table**: No concurrent writes/reads

## Optimization Hypotheses (Prioritized by Impact)

### Hypothesis 1: Parallel Execution (Expected: 10-20x improvement)

**Theory**: Modern CPUs have 16-32 cores. If we parallelize scan, filter, and aggregation, we can achieve near-linear speedup for I/O-bound and CPU-bound operations.

**Evidence**: DuckDB shows 2000%+ CPU utilization on queries. Our engine shows 100%.

**Implementation Options**:

A. **Rayon-based parallel scan** (Simplest)
   - Use rayon's `par_iter` to process batches in parallel
   - Merge results at the end
   - Risk: Requires careful synchronization for aggregation
   - Complexity: Low

B. **Partitioned pipeline execution** (Most thorough)
   - Split data into partitions at scan level
   - Execute entire pipeline per partition
   - Final merge/repartition step
   - Complexity: Medium

C. **Morsel-driven parallelism** (DuckDB approach)
   - Work-stealing scheduler
   - Fine-grained morsels (64K rows)
   - Best load balancing
   - Complexity: High

**Recommendation**: Start with Option A for quick wins, evolve to Option B.

### Hypothesis 2: Streaming Parquet Scan (Expected: 2-5x improvement)

**Theory**: Currently we load entire Parquet files into memory before processing. Streaming row-group-by-row-group would:
- Reduce memory footprint
- Allow pipeline parallelism
- Enable better cache utilization

**Evidence**: Our Q1 timing shows ~1.5s for "plan" which is data loading.

**Implementation Options**:

A. **Row-group streaming with batch pipelining**
   - Read one row group at a time
   - Process immediately while reading next
   - Pro: Lower memory, overlapped I/O
   - Complexity: Medium

B. **Async I/O with tokio**
   - Use async Parquet reader
   - Overlap I/O with computation
   - Pro: Best for NVMe drives
   - Complexity: Medium

**Recommendation**: Option A first, then add async I/O.

### Hypothesis 3: Predicate Pushdown to Parquet (Expected: 2-10x improvement)

**Theory**: Parquet files have min/max statistics per row group. If we push predicates to the Parquet reader, we can skip entire row groups that don't match.

**Evidence**: Q6 filter `l_shipdate >= '1994-01-01' AND l_shipdate < '1995-01-01'` should skip ~6/7 of row groups.

**Implementation Options**:

A. **Row group pruning with statistics**
   - Read Parquet metadata
   - Compare filter predicates against min/max
   - Skip non-matching row groups
   - Complexity: Low-Medium

B. **Page-level predicate pushdown**
   - Use page statistics for finer granularity
   - More I/O savings
   - Complexity: High

**Recommendation**: Option A provides most benefit with lower complexity.

### Hypothesis 4: Subquery Decorrelation (Expected: 100x+ for Q17-Q22)

**Theory**: Correlated subqueries are evaluated once per outer row, leading to O(n²) complexity. Decorrelation transforms them into joins, achieving O(n log n).

**Evidence**: Q17-Q22 take >5 minutes while equivalent DuckDB queries complete in <1s.

**Implementation Options**:

A. **Magic set transformation**
   - Extract distinct values from outer query
   - Execute subquery once for all values
   - Join back to outer query
   - Complexity: High

B. **Lateral join rewrite**
   - Transform correlated subquery to lateral join
   - Use hash join for efficiency
   - Complexity: High

C. **Caching + memoization** (Quick win)
   - Cache subquery results by correlation key
   - Reuse for repeated values
   - Pro: Simple, works for many cases
   - Complexity: Low

**Recommendation**: Start with Option C for quick wins, then implement Option A.

### Hypothesis 5: Vectorized/SIMD Execution (Expected: 2-4x improvement)

**Theory**: Arrow already uses SIMD for many operations, but we can improve by:
- Batching expressions into larger chunks
- Using SIMD-friendly hash functions
- Avoiding per-row function calls

**Implementation Options**:

A. **Expression compilation to Arrow compute**
   - Replace per-row evaluation with batch kernels
   - Use Arrow's SIMD-optimized functions
   - Complexity: Medium

B. **JIT compilation with Cranelift**
   - Compile expressions to native code
   - Maximum performance
   - Complexity: Very High

**Recommendation**: Option A is sufficient for most gains.

## Proposed Implementation Roadmap

### Phase 1: Quick Wins (1-2 days)
1. [x] Fix filter partition propagation bug
2. [ ] Add simple parallel scan with rayon
3. [ ] Implement subquery result caching

### Phase 2: I/O Optimization (2-3 days)
4. [ ] Streaming Parquet reader
5. [ ] Row group pruning with statistics
6. [ ] Async I/O for Parquet

### Phase 3: Parallel Execution (3-5 days)
7. [ ] Partitioned aggregation
8. [ ] Parallel hash join build
9. [ ] Work-stealing scheduler

### Phase 4: Advanced Optimizations (5+ days)
10. [ ] Subquery decorrelation
11. [ ] Expression vectorization
12. [ ] Cost-based join ordering

## Metrics to Track

For each optimization, measure:
1. **Wall-clock time** for all 22 TPC-H queries
2. **CPU utilization** (target: >1500%)
3. **Memory peak usage**
4. **I/O throughput**

## Success Criteria

- Within 5x of DuckDB for Q1-Q16
- Within 20x of DuckDB for Q17-Q22
- All 22 queries complete in <30s total at SF=10

## Current Blockers

1. **Correlated subqueries**: Q17-Q22 are blocked until decorrelation implemented
2. **Single-threaded**: All queries blocked by lack of parallelism
3. **Memory**: Large datasets may OOM without streaming
