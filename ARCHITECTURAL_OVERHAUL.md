# ARCHITECTURAL OVERHAUL PLAN: Beat DuckDB on TPC-H

## Executive Summary

**Goal**: Reduce total TPC-H time from 14.2s to <1s (14x speedup) and eliminate critical regressions (Q09: 366x, Q18: 152x, Q21: 95x).

**Root Cause Analysis**:
- We have parallelism (rayon) in joins/aggregates but NOT in subquery execution
- Our `FlattenDependentJoin` is incomplete - falls back to row-by-row for multi-subquery patterns
- `JoinReorder` uses greedy heuristics instead of optimal DP-based ordering
- Missing: proper cost model, statistics collection, and DuckDB's `LogicalDependentJoin` framework

## Research Findings

### From DuckDB's `flatten_dependent_join.cpp`:
1. **Single unified framework** for ALL subqueries (EXISTS, IN, scalar) via `LogicalDependentJoin`
2. **`perform_delim` flag** - controls whether to use DelimJoin or window-based duplicate elimination
3. **Sophisticated handling**:
   - Window functions with row_number() OVER for duplicate elimination
   - Aggregates with correlation tracking
   - LIMIT pushdown with partitioned row_number
   - Recursive CTEs with correlation marking
   - MARK joins with proper rewriting
4. **Cross product optimization** - detects when correlation disappears and uses simple cross product

### From Join Ordering Papers:
1. **DuckDB uses dynamic programming** (Moerkotte & Neumann 2008, modern Selinger variant)
2. **POLAR (2024)**: Adaptive join order selection, 9x improvements, <7% overhead
3. **Cardinality estimation** via histograms and sampling (not just row counts)

### From Performance Analysis:
1. **Q21 bottleneck**: 95% of total slowness - EXISTS subqueries executed row-by-row (O(n²))
2. **Q09/Q18 regressions**: Our JoinReorder creates suboptimal plans vs original
3. **Aggregations**: Need thread-local hash tables with parallel merge (partially implemented)

## Implementation Plan

### Phase 1: Subquery Decorrelation Overhaul (HIGHEST ROI - fixes Q21)

**Target**: Q21: 2.1s → <100ms (21x speedup)

#### 1.1 Implement LogicalDependentJoin Framework

**Current state**: We have `DelimJoinExec` physical operator but incomplete logical planning

**Required changes**:
```
NEW FILES:
- src/planner/logical_dependent_join.rs  (based on DuckDB's design)
  - LogicalDependentJoin node (replaces Filter with EXISTS/IN/ScalarSubquery)
  - Correlation extraction and tracking
  - perform_delim flag logic

MODIFY:
- src/planner/logical_plan.rs
  - Add LogicalDependentJoin variant
  - Add LogicalDelimGet variant
  - Add logical comparison join conditions

- src/planner/binder.rs
  - Convert EXISTS/IN/ScalarSubquery → LogicalDependentJoin
  - Track correlated columns
  - Set perform_delim based on query analysis
```

**Key insight from DuckDB**:
- Use `row_number() OVER (PARTITION BY [correlated_columns])` when `!perform_delim`
- This enables duplicate elimination WITHOUT expensive DelimJoin for simple cases
- Only use DelimJoin when correlation benefits > overhead

#### 1.2 Fix Multi-EXISTS Handling (Q21)

**Problem**: Q21 has `EXISTS(...) AND NOT EXISTS(...)` - both correlate on same column

**DuckDB approach**: Process ALL correlated subqueries in single pass through `LogicalDependentJoin`

**Implementation**:
```rust
// In try_flatten_multiple_exists_subqueries:
// Don't try to handle sequentially - instead create ONE LogicalDependentJoin
// with multiple inner sides that all share the same DelimGet

// DuckDB pattern:
// - Create DelimJoin with:
//   left: outer query
//   right: first subquery
// - Then process additional EXISTS by extending the DelimJoin conditions
// - Key: The DelimGet is SHARED across all inner subqueries
```

#### 1.3 Enable IN Subquery → Mark Join

**Current state**: `flatten_in_subquery()` exists but uses Semi/Anti (incorrect for IN)

**Fix**: Use Mark join type which adds boolean marker column:
```rust
// IN subquery becomes:
// LEFT JOIN (SELECT DISTINCT col FROM inner) ON outer.col = inner.col
// WHERE inner.col IS NOT NULL  -- Mark indicates match
```

**Estimated effort**: 8-12 hours
**Expected speedup**: Q21: 2.1s → 50-100ms (21-42x)

---

### Phase 2: Join Ordering Overhaul (HIGH ROI - fixes Q09/Q18)

**Target**: Q09: 5.5s → <50ms (110x), Q18: 1.8s → <20ms (90x)

#### 2.1 Implement Dynamic Programming Join Reordering

**Current**: Greedy heuristic in `JoinReorder`

**Replace with**:
```rust
// src/optimizer/rules/join_reorder.rs

// Selinger-style DP for optimal join ordering
fn find_optimal_join_order(
    relations: Vec<JoinRelation>,
    edges: &[(Expr, Expr)],  // join conditions
) -> JoinOrder {
    // dp[bitmask] = (cost, join_order)
    // For each subset of tables, try all ways to build it from smaller subsets
    // Pick minimum cost
}
```

**Algorithm** (from DuckDB research):
1. For each subset size from 2 to n:
   - For each subset S:
     - For each bipartition (L, R) of S:
       - cost = dp[L].cost + dp[R].cost + join_cost(L, R)
       - Keep minimum
2. Return dp[all_tables]

#### 2.2 Implement Proper Cardinality Estimation

**Current**: Hardcoded row counts, SF=1 estimates used for SF=0.1

**Replace with**:
```rust
// src/optimizer/rules/join_reorder.rs

struct Statistics {
    row_count: usize,
    distinct_counts: HashMap<String, usize>,  // column -> distinct values
    histograms: HashMap<String, Histogram>,     // column -> value distribution
}

// Collect from:
// 1. Parquet metadata (already have row counts)
// 2. COUNT(DISTINCT) sampling for multi-file datasets
// 3. Histograms for filter selectivity estimation
```

**Estimated effort**: 12-16 hours
**Expected speedup**: Q09: 110x, Q18: 90x

---

### Phase 3: Hash Table Optimizations (MEDIUM ROI)

**Target**: Overall 2-3x speedup for hash-heavy operations

#### 3.1 Perfect Hashing for Low-Cardinality Joins

**From research**: "Is Perfect Hashing Practical for OLAP Systems?" (2024)

**Implementation**:
```rust
// src/physical/operators/hash_join.rs

fn build_perfect_hash(
    keys: &[ArrayRef],
    build_side: &[RecordBatch],
) -> PerfectHashTable {
    // Check if keys fit in perfect hash table
    // Build using perfect hash function (minimal perfect hash)
    // Probe in O(1) with no conflicts
}
```

#### 3.2 Cache-Friendly Data Layout

```rust
// Use soa_vec or similar for better cache utilization
// Align structs to cache line boundaries (64 bytes)
// Store join keys contiguously
```

**Estimated effort**: 8-12 hours
**Expected speedup**: 1.5-2x for joins

---

### Phase 4: Statistics Collection (FOUNDATION)

**Target**: Enable all cost-based optimizations

#### 4.1 Parquet Metadata Extraction

```rust
// src/storage/parquet.rs

pub fn collect_statistics(path: &Path) -> TableStatistics {
    // Read Parquet metadata:
    // - Row count per file
    // - Min/max values per column (for filter pushdown)
    // - Distinct count estimates (using sampling)
}
```

#### 4.2 Auto-ANALYZE Command

```bash
./query_engine analyze --tables ./data/tpch-100mb
```

**Estimated effort**: 4-6 hours

---

### Phase 5: Integration & Testing

#### 5.1 Update Optimizer Rule Order

```
Current:
1. ConstantFolding
2. PredicatePushdown
3. FlattenDependentJoin (partially implemented)
4. SubqueryDecorrelation
5. JoinReorder
6. PredicatePushdown
7. ProjectionPushdown

NEW:
1. ConstantFolding
2. PredicatePushdown
3. FlattenDependentJoin (FULL implementation - replaces SubqueryDecorrelation)
4. JoinReorder (DP-based)
5. CollectStatistics (if needed)
6. PredicatePushdown
7. ProjectionPushdown
```

#### 5.2 Regression Tests

```bash
# Before any changes, establish baseline
./scripts/baseline_performance.sh

# After each phase, run:
cargo test
./scripts/benchmark_mini.sh  # Must maintain DuckDB comparison
```

**Estimated effort**: 4-6 hours

---

## Success Criteria

| Metric | Before | Target | Stretch |
|--------|--------|--------|---------|
| Q21 | 2.1s | <100ms | <50ms |
| Q09 | 5.5s | <50ms | <20ms |
| Q18 | 1.8s | <20ms | <10ms |
| Q07 | 2.4s | <50ms | <20ms |
| **Total** | **14.2s** | **<1s** | **<0.5s** |

### DuckDB Comparison (SF=0.1):

| Query | DuckDB | Our Target | Ratio |
|-------|--------|------------|-------|
| Q21 | 22ms | <100ms | <4.5x |
| Q09 | 15ms | <50ms | <3.3x |
| Q18 | 12ms | <20ms | <1.7x |
| Q07 | 20ms | <50ms | <2.5x |

---

## Risk Mitigation

1. **Complexity risk**: LogicalDependentJoin is complex - implement incrementally
2. **Correctness risk**: Comprehensive test suite before/after each phase
3. **Performance risk**: Profile after each major change to ensure improvement
4. **Regression risk**: Keep SubqueryDecorrelation as fallback initially

---

## Implementation Order

### Sprint 1 (Week 1): Quick Wins
- [ ] Fix cardinality scaling bug (SF=0.1 vs SF=1)
- [ ] Enable IN subquery → Mark join (Q18)
- [ ] Fix multi-EXISTS without full LogicalDependentJoin
- [ ] Verify Q21 <500ms

**Expected**: Q21 2.1s → <500ms, Q18 1.8s → <500ms

### Sprint 2 (Week 1): DP Join Ordering
- [ ] Implement DP-based join reordering
- [ ] Add proper cardinality estimation
- [ ] Fix Q09/Q18 regressions
- [ **Verify**: Q09 <100ms, Q18 <50ms**

### Sprint 3 (Week 2): Full LogicalDependentJoin
- [ ] Implement complete LogicalDependentJoin framework
- [ ] Handle all subquery types uniformly
- [ ] Window-based duplicate elimination fallback
- [ ] **Verify**: Q21 <100ms

### Sprint 4 (Week 2): Final Polish
- [ ] Perfect hashing for joins
- [ ] Statistics collection
- [ ] Full benchmark suite
- [ ] **Verify**: Total <1s

---

## Open Questions for Research

1. **Recursive CTEs**: How does DuckDB handle Q19 which uses CTEs? Need to investigate
2. **LATERAL joins**: DuckDB has special handling - do we need it?
3. **OUTER joins**: Complex case - ensure we don't break these
4. **Histogram collection**: Sampling strategy for accurate distinct counts?

---

**Sources**:
- [DuckDB FlattenDependentJoin](https://github.com/cwida/duckdb/blob/master/src/planner/subquery/flatten_dependent_join.cpp)
- [Debunking Join Ordering Myth (SIGMOD 2025)](https://people.iiis.tsinghua.edu.cn/~huanchen/publications/rpt-sigmod25.pdf)
- [Join Order Optimization Thesis](https://blobs.duckdb.org/papers/tom-ebergen-msc-thesis-join-order-optimization-with-almost-no-statistics.pdf)
- [Is Perfect Hashing Practical? (2024)](https://db.cs.cmu.edu/papers/2024/p65-gaffney.pdf)
- [DuckDB Correlated Subqueries](https://duckdb.org/2023/05/26/correlated-subqueries-in-sql.html)
