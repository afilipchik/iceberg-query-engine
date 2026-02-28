# Performance ROI Roadmap

## TL;DR - Priority Order for Fixes

| Priority | Query | Ratio | Time Savings | Fix Type | Est. Effort |
|----------|-------|-------|--------------|----------|-------------|
| 🔥 **1** | Q21 | 2790x | **61s** | Complete DelimJoin for nested EXISTS | 2-3 days |
| ⭐ **2** | Q07 | 133x | **2.5s** | 6-way join reordering + cardinality | 1-2 days |
| ⭐ **3** | Q09 | 53x | **770ms** | 6-way join reordering | 1 day |
| ⭐ **4** | Q18 | 30x | **350ms** | IN → Mark join decorrelation | 1 day |
| ⭐ **5** | Q19 | 15x | **285ms** | Multi-way join + agg pushdown | 1 day |
| 6 | Q05 | 10.7x | 235ms | 5-way join reordering | 0.5 day |
| 7 | Q17 | 20x | 189ms | Scalar → Single join | 0.5 day |
| 8 | Q03 | 4.3x | 39ms | Minor | 0.5 day |

**Total potential time savings: ~65 seconds** (92% from Q21 alone)

## How to Run the Mini Benchmark

```bash
# 1. Generate test data (first time only)
cargo run --release -- generate-parquet --sf 0.1 --output ./data/tpch-100mb

# 2. Install DuckDB (for comparison)
brew install duckdb

# 3. Run the lightweight benchmark (~30 seconds)
./scripts/benchmark_mini.sh
```

The mini benchmark runs 7 representative queries (vs all 22) focusing on:
- Baseline (Q01 - we're competitive)
- 3-way joins (Q03)
- 5-way joins (Q05)
- 6-way joins (Q07, Q09 - **high ROI**)
- Subqueries (Q17, Q18 - **high ROI**)

## Fix Details by Priority

### Priority 1: Q21 - Nested EXISTS (2790x slower)

**Current**: Row-by-row subquery execution (O(n²))
**Target**: DelimJoin (DuckDB-style deduplication join)

**Status**: Infrastructure exists (`DelimJoinExec`, `DelimGetExec`) but not fully enabled for nested EXISTS patterns.

**What's needed**:
1. Enable `FlattenDependentJoin` rule for nested EXISTS/NOT EXISTS
2. Fix column resolution for DelimGet in nested contexts
3. Test Q21 at larger scale factors

**Files**:
- `src/optimizer/rules/flatten_dependent_join.rs` - Already exists, needs enabling
- `src/physical/operators/delim_join.rs` - Already implemented
- `src/physical/planner.rs` - DelimJoin conversion already done

### Priority 2: Q07 - 6-way Join Reordering (133x slower)

**Current**: Suboptimal join order causing huge intermediate results
**Target**: Cost-based join ordering with cardinality estimation

**What's needed**:
1. Collect statistics (row counts, distinct counts) from Parquet metadata
2. Enhance `JoinReorder` rule to use cardinality estimates
3. Prefer selective filters early in join order

**Files**:
- `src/optimizer/rules/join_reorder.rs` - Greedy join reordering exists
- `src/optimizer/cost.rs` - Placeholder, needs real cardinality estimation

### Priority 3: Q09 - 6-way Join (53x slower)

**Current**: Similar to Q07 - poor join ordering
**Target**: Same as Q07

**Note**: Fixing join reordering (Priority 2) should also fix Q09.

### Priority 4: Q18 - IN Subquery (30x slower)

**Current**: IN subquery executed row-by-row
**Target**: Mark join (DuckDB pattern - adds boolean column)

**What's needed**:
1. Extend `SubqueryDecorrelation` to handle IN subqueries
2. Add `JoinType::Mark` support (already in codebase)
3. Convert `IN (SELECT...)` to `LEFT JOIN ... ON ... AND mark_column`

**Files**:
- `src/optimizer/rules/subquery_decorrelation.rs` - Needs IN subquery handling
- `src/planner/logical_plan.rs` - `JoinType::Mark` already exists

### Priority 5: Q19 - Multi-way Join + Aggregation (15x slower)

**Current**: Multiple issues: join order + aggregation pushdown
**Target**: Combine fixes from Q07/Q09 + aggregate pushdown

**What's needed**:
1. Join reordering (from Priority 2)
2. Push aggregations below joins where possible

### Priority 6-7: Scalar/IN Subqueries (Q17, Q05)

**Current**: Scalar subquery execution
**Target**: Single join (for scalar) / Mark join (for IN)

**Note**: These will benefit from the same fixes as Q18.

## Summary: Bang for the Buck

| Fix | Queries Affected | Total Time Saved | Effort |
|-----|-----------------|------------------|--------|
| Complete DelimJoin | Q21, Q17, Q18, Q20 | ~62s | 2-3 days |
| Join reordering + stats | Q05, Q07, Q08, Q09, Q19 | ~4s | 1-2 days |
| Mark join for IN | Q18, Q20 | ~450ms | 1 day |

**Highest ROI**: Complete DelimJoin infrastructure - single biggest impact (61s from one fix).
