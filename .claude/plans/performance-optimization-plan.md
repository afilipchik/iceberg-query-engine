# Query Engine Performance Optimization Plan

## Current State (SF=10, 3.1GB, 60M lineitem rows)

### Benchmark Results (2026-01-28)

**Overall**: 608x slower than DuckDB (17.6 min vs 1.74s)

| Query | Engine | DuckDB | Ratio | Status |
|-------|--------|--------|-------|--------|
| Q01 | 617ms | 89ms | 6.9x | ✅ Morsel integrated, needs vectorization |
| Q02 | 3.0s | 13ms | 224x | Join ordering |
| Q03 | 4.7s | 84ms | 56x | Join ordering |
| Q04 | 136ms | 80ms | **1.7x** | ✅ Best performer |
| Q05 | 33s | 49ms | 682x | Join ordering |
| Q06 | 563ms | 24ms | 24x | Needs morsel integration |
| Q07 | **437s** | 61ms | **7,203x** | 🔴 Join ordering critical |
| Q08 | 22s | 76ms | 296x | Join ordering |
| Q09 | **310s** | 8ms | **40,346x** | 🔴 Join ordering critical |
| Q10 | 24s | 98ms | 247x | Join ordering |
| Q11 | 14s | 10ms | 1,342x | 🔴 **Wrong results** |
| Q12 | 6.2s | 66ms | 94x | OK |
| Q13 | 25s | 131ms | 194x | ✅ LEFT JOIN fixed |
| Q14 | 1.4s | 35ms | 41x | OK |
| Q15 | 7.2s | 33ms | 219x | CTE optimization |
| Q16 | 2.2s | 40ms | 55x | OK |
| Q17 | 44s | 75ms | 593x | Subquery decorrelation |
| Q18 | 62s | 283ms | 219x | Subquery decorrelation |
| Q19 | 42s | 87ms | 485x | Complex OR predicates |
| Q20 | 11s | 161ms | 68x | OK |
| Q21 | **1.29s** | 201ms | 6.4x | ⚠️ Decorrelated but slow filter |
| Q22 | 4.1s | 36ms | 115x | ✅ Fixed |

### Root Cause Analysis

| Category | Queries | Root Cause | Solution |
|----------|---------|------------|----------|
| Join ordering | Q05,Q07,Q09 | Greedy heuristic picks wrong order | Cost-based optimization |
| Subquery | Q17,Q18,Q19 | Row-by-row execution | DelimJoin integration |
| Parallelism | Q01,Q06 | Single-threaded execution | Morsel integration |
| Bugs | Q11,Q21 | HAVING bug, column resolution | Code fixes |

---

## Priority 0: Critical Bugs

### P0.1: Q21 Crash (DelimJoin Column Resolution)

**File**: `src/optimizer/rules/flatten_dependent_join.rs`

**Problem**: Q21 has nested EXISTS/NOT EXISTS with table aliases (l1, l2, l3):
```sql
EXISTS (SELECT * FROM lineitem l2 WHERE l2.l_orderkey = l1.l_orderkey ...)
AND NOT EXISTS (SELECT * FROM lineitem l3 WHERE l3.l_orderkey = l1.l_orderkey ...)
```

When FlattenDependentJoin rewrites the subquery, column names don't match:
- Outer reference: `l1.l_orderkey`
- Inner column: `lineitem.l_orderkey` (alias stripped)

**Fix**:
```rust
// In flatten_dependent_join.rs, improve column matching:
fn columns_match(outer_ref: &str, inner_col: &str) -> bool {
    // Strip table qualifier and compare base names
    let outer_base = outer_ref.rsplit('.').next().unwrap_or(outer_ref);
    let inner_base = inner_col.rsplit('.').next().unwrap_or(inner_col);
    outer_base == inner_base
}
```

**Test**: Run Q21 at SF=10, should complete in <60s

### P0.2: Q11 Wrong Results (HAVING Scalar Subquery)

**File**: `src/physical/operators/hash_agg.rs` or `subquery.rs`

**Problem**: HAVING clause with scalar subquery:
```sql
HAVING SUM(ps_supplycost * ps_availqty) > (
    SELECT SUM(ps_supplycost * ps_availqty) * 0.0001 FROM ...
)
```

Expected threshold: 80,989,195 (from DuckDB)
Our threshold: Unknown (returning 100 rows instead of 0)

**Debug Steps**:
1. Add logging to scalar subquery evaluation in HAVING
2. Compare threshold value with DuckDB
3. Check if subquery is evaluated once or per-group

---

## Priority 1: Join Ordering (Q07, Q09)

### Current Approach

`JoinReorder` uses greedy edge selection:
1. Build graph of tables and join predicates
2. Greedily pick edges that add fewest rows
3. Uses heuristics like "prefer dimension tables over fact tables"

**Problem**: At SF=10, heuristics fail for 6+ table joins:
- Q07: supplier → lineitem → orders → customer → nation × 2
- Q09: part → supplier → lineitem → partsupp → orders → nation

### Solution: Cost-Based Join Ordering

```rust
// Add to src/optimizer/rules/join_reorder.rs

struct TableStats {
    row_count: usize,
    cardinality: HashMap<String, usize>,  // column -> distinct values
}

fn estimate_join_size(left: &TableStats, right: &TableStats, predicate: &Expr) -> usize {
    // Use selectivity estimation
    // join_size = left.rows * right.rows * selectivity
    // For equality: selectivity = 1 / max(left_card, right_card)
}

fn find_optimal_join_order(tables: &[TableStats], predicates: &[JoinPredicate]) -> Vec<usize> {
    // Dynamic programming or greedy with cost
    // Start with smallest table after filtering
    // Add tables that minimize intermediate result size
}
```

**Implementation Steps**:
1. Add `row_count()` method to `TableProvider` trait
2. Collect stats during Parquet scan (from footer metadata)
3. Implement cost estimation in `JoinReorder`
4. Use DP for small joins (<8 tables), greedy with cost for larger

---

## Priority 2: Morsel Integration (Q01, Q06) - ✅ COMPLETED (2026-01-29)

### Implementation Status

Morsel-driven execution has been integrated into the main query engine:

**What was done:**
1. Added `enable_morsel_execution` flag to `ExecutionConfig` (default: true)
2. Created `MorselAggregateExec` in `src/physical/operators/morsel_agg.rs`
3. Extended `TableProvider` trait with `parquet_files()` method
4. Updated `PhysicalPlanner` to route aggregation over Parquet to morsel execution

**Performance Results (TPC-H Q1 at SF=10):**
- With morsel execution: **617ms**
- Without morsel execution: 1830ms
- **Speedup: 3x faster**
- vs DuckDB (89ms): 6.9x (within 10x target)

**Files Modified:**
- `src/execution/memory.rs` - Added `enable_morsel_execution` config
- `src/physical/operators/morsel_agg.rs` - New MorselAggregateExec operator
- `src/physical/operators/scan.rs` - Added `parquet_files()` to TableProvider
- `src/storage/parquet.rs` - Implemented `parquet_files()` for ParquetTable
- `src/physical/planner.rs` - Routing logic for morsel execution

**Test examples:**
```bash
cargo run --release --example test_morsel_q1
cargo run --release --example test_morsel_q1_comparison
```

---

## Priority 2.5: Q1 Vectorization (617ms → <200ms target)

### Current Bottlenecks Analysis

| Component | Our Time | DuckDB Est. | Gap | Root Cause |
|-----------|----------|-------------|-----|------------|
| Parquet I/O + decode | ~300ms | ~50ms | 6x | Generic Arrow reader |
| Hash table lookups | ~200ms | ~5ms | 40x | HashMap for 4 groups |
| Value extraction | ~80ms | ~10ms | 8x | ScalarValue boxing |
| Expression eval | ~30ms | ~5ms | 6x | AST traversal |
| **Total** | **617ms** | **89ms** | **7x** | |

### Optimizations Implemented (2026-01-29)

All four optimizations were implemented and benchmarked:

| # | Optimization | Implemented In | Result |
|---|-------------|----------------|--------|
| 1 | Perfect hash for low cardinality | `morsel_agg.rs` | ✅ Fixed array + raw_key_maps |
| 2 | Direct primitive array access | `morsel_agg.rs` | ✅ TypedArrayAccessor + update_f64 |
| 3 | Vectorized batch updates | `morsel_agg.rs` | ✅ f64 slice fast path |
| 4 | Pre-evaluate column indices | N/A | Skipped (evaluate_expr overhead is per-batch, not per-row) |

**Results (TPC-H Q1 at SF=10):**

| Stage | Time | Improvement |
|-------|------|-------------|
| Before (HashAggregateExec) | 1830ms | baseline |
| Morsel integration | 617ms | 3.0x |
| + Direct primitive access | 527ms | 1.2x |
| + Perfect hash + raw keys | 502ms | 1.05x |
| + Vectorized f64 path | 502ms | - |
| **Final** | **502ms** | **3.6x total** |

**Remaining gap analysis:**
- Our engine: 502ms
- final_q1.rs (hand-coded): 420ms → 80ms gap is filter materialization + function call overhead
- DuckDB: 89ms → 330ms gap is custom Parquet reader with SIMD decompression

**Key implementation details:**
- `AggregationState` now has dual mode: perfect hash (fixed array) and HashMap fallback
- `TypedArrayAccessor` pre-downcasts arrays once per batch for typed access
- `raw_key()` extracts u64 keys without ScalarValue allocation
- `update_f64()` / `update_i64()` bypass ScalarValue in accumulator updates
- f64 fast path pre-extracts primitive slices for tight inner loop

---

## Implementation Roadmap

### Week 1: Critical Bugs
- [ ] Fix Q21 column resolution in `flatten_dependent_join.rs`
- [ ] Fix Q11 HAVING scalar subquery evaluation
- [ ] Re-run SF=10 benchmark to verify 22/22 correct

### Week 2: Join Ordering
- [ ] Add table statistics infrastructure
- [ ] Implement cost-based join ordering
- [ ] Target: Q07 < 30s, Q09 < 30s at SF=10

### Week 3: Parallelism - ✅ COMPLETED
- [x] Integrate morsel execution into main path
- [x] Target: Q01 < 890ms at SF=10 (achieved 617ms)

### Week 4: Subquery Optimization
- [ ] Enable DelimJoin for all subquery patterns
- [ ] Target: Q17-Q19 < 10s each at SF=10

---

## Success Criteria

| Metric | Current | Target |
|--------|---------|--------|
| Correctness | 20/22 | 22/22 |
| Total time (SF=10) | 17.6 min | < 2 min |
| Slowest query ratio | 40,346x | < 100x |
| Average ratio | 608x | < 50x |

---

## Key Files Reference

| Component | File | Status |
|-----------|------|--------|
| DelimJoin | `src/physical/operators/delim_join.rs` | ✅ Built |
| FlattenDependentJoin | `src/optimizer/rules/flatten_dependent_join.rs` | ⚠️ Bugs |
| JoinReorder | `src/optimizer/rules/join_reorder.rs` | ⚠️ Not cost-based |
| Morsel execution | `src/physical/morsel.rs` | ✅ Built |
| MorselAggregateExec | `src/physical/operators/morsel_agg.rs` | ✅ **Integrated (2026-01-29)** |
| Hash aggregation | `src/physical/operators/hash_agg.rs` | ⚠️ Q11 bug |
| Subquery execution | `src/physical/operators/subquery.rs` | ✅ Working |

---

## Previous Improvements (Reference)

1. **Parallel SEMI/ANTI Join** (2026-01-27): 5.8x faster Q21 at SF=0.01
2. **Q21 Decorrelation Fix** (2026-01-27): 6x faster at SF=0.1
3. **DelimJoin Infrastructure** (2026-01-27): Built, has bugs
4. **Morsel-driven parallelism**: 8x on Q1 in examples
5. **Parallel hash join build**: Using rayon
6. **Subquery memoization**: Caching by correlation key
7. **Join reordering**: 158x improvement on Q8
8. **LEFT OUTER JOIN fix** (2026-01-28): Q13 now correct
9. **Morsel Aggregation Integration** (2026-01-29): Q1 617ms (3x faster than HashAggregateExec)
