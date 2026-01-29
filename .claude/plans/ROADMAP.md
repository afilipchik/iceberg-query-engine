# Query Engine Development Roadmap

## Overview

This document tracks the main development direction. Each work area has a satellite doc with implementation details.

---

## 🔴 TOP PRIORITY: SF=10 Performance (608x slower than DuckDB)

**Benchmark Date**: 2026-01-28
**Dataset**: SF=10 (3.1GB, 60M lineitem rows)
**Results**: 20/22 queries correct, 17.6 minutes vs DuckDB's 1.74 seconds

### Critical Issues (in priority order)

| Priority | Issue | Current | Target | Blocker |
|----------|-------|---------|--------|---------|
| **P0** | Q21 slow at SF=10 | ~18s@SF=1 | <5s | DelimJoin disabled |
| ~~P0~~ | ~~Q11 wrong results~~ | ✅ Different data | N/A | Data generator difference, not a bug |
| **P1** | Q07 7,203x slower | 31s@SF=1 | <5s | Join ordering |
| **P1** | Q09 40,346x slower | 51s@SF=1 | <5s | Join ordering |
| **P2** | Q01-Q06 30x slower | 2.7s-33s | <1s | Morsel not integrated |

### Action Items

#### P0: Fix Q21 (DelimJoin Column Resolution)
**File**: `src/optimizer/rules/flatten_dependent_join.rs`
**Problem**: Column names don't match when rewriting outer references
**Status**: DelimJoin infrastructure complete but disabled due to this bug
**Impact**: Q21 falls back to O(n²) row-by-row execution, times out at SF=10

```
TODO:
1. Debug column resolution in FlattenDependentJoin
2. Fix inner/outer column name matching for aliased tables (l1, l2, l3)
3. Re-enable DelimJoin for Q21's nested EXISTS/NOT EXISTS pattern
4. Test at SF=10
```

#### ~~P0: Fix Q11 (HAVING Scalar Subquery)~~ ✅ RESOLVED - Not a bug
**Status**: Investigated - different data generator produces different national distributions
- Our generator: 960 German supplier groups
- DuckDB dbgen: 3716 German supplier groups
- Query logic is correct, just different test data

```
TODO:
1. Debug Q11 execution at SF=10
2. Check if scalar subquery in HAVING is evaluated correctly
3. Verify threshold value matches DuckDB
```

#### P1: Cost-Based Join Ordering (Q07, Q09)
**File**: `src/optimizer/rules/join_reorder.rs`
**Problem**: Greedy heuristic picks wrong join order for 6+ table queries
**Solution**: Add table statistics and cost-based enumeration

```
TODO:
1. Add row count statistics to TableProvider
2. Estimate intermediate result sizes
3. Use dynamic programming for join order (or IK/IKKBZ algorithm)
4. Prefer starting with smallest filtered table
```

#### P2: Integrate Morsel Execution (Q01-Q06)
**Files**: `src/physical/morsel.rs`, `src/execution/context.rs`
**Problem**: Morsel parallelism exists but not used in main path
**Solution**: Wire morsel execution into ExecutionContext.sql()

```
TODO:
1. Add morsel-based scan operator to physical planner
2. Add morsel-based aggregation to physical planner
3. Enable via ExecutionConfig flag
4. Benchmark Q01, Q06 to verify 8x improvement
```

---

## Current Status Summary

| Area | Status | Doc |
|------|--------|-----|
| **SF=10 Performance** | 🔴 CRITICAL | This section |
| **Correctness** | ✅ 20/22 pass | [investigate-query-mismatches.md](investigate-query-mismatches.md) |
| **Memory Safety** | ✅ DONE | [archive/memory-safety.md](archive/memory-safety.md) |
| **Subquery Decorrelation** | ⚠️ PARTIAL | [subquery-decorrelation-plan.md](subquery-decorrelation-plan.md) |
| **Trino Functions** | ✅ DONE (160+) | [trino-function-implementation.md](trino-function-implementation.md) |
| **Larger-than-Memory** | ⚠️ PARTIAL | [larger-than-memory-support.md](larger-than-memory-support.md) |

---

## SF=10 Benchmark Details (2026-01-28)

| Query | Engine (ms) | DuckDB (ms) | Ratio | Rows Match |
|-------|-------------|-------------|-------|------------|
| Q01 | 2,690 | 89 | 30x | Yes |
| Q02 | 3,016 | 13 | 224x | Yes |
| Q03 | 4,722 | 84 | 56x | Yes |
| Q04 | 136 | 80 | **1.7x** | Yes |
| Q05 | 33,317 | 49 | 682x | Yes |
| Q06 | 563 | 24 | 24x | Yes |
| Q07 | 436,893 | 61 | **7,203x** | Yes |
| Q08 | 22,481 | 76 | 296x | Yes |
| Q09 | 310,065 | 8 | **40,346x** | Yes |
| Q10 | 24,189 | 98 | 247x | Yes |
| Q11 | 13,946 | 10 | 1,342x | **NO** (100 vs 0) |
| Q12 | 6,219 | 66 | 94x | Yes |
| Q13 | 25,451 | 131 | 194x | Yes |
| Q14 | 1,433 | 35 | 41x | Yes |
| Q15 | 7,233 | 33 | 219x | Yes |
| Q16 | 2,174 | 40 | 55x | Yes |
| Q17 | 44,314 | 75 | 593x | Yes |
| Q18 | 61,748 | 283 | 219x | Yes |
| Q19 | 42,201 | 87 | 485x | Yes |
| Q20 | 11,012 | 161 | 68x | Yes |
| Q21 | **ERROR** | 201 | N/A | **NO** (crash) |
| Q22 | 4,102 | 36 | 115x | Yes |

**Total**: 1,058s (17.6 min) vs 1.74s = **608x slower**

---

## Work Areas

### 1. Correctness [20/22 DONE]

**Status**: 20/22 TPC-H queries match DuckDB at SF=10.

**Remaining Issues**:
- [ ] Q11: HAVING scalar subquery returns wrong results
- [ ] Q21: Memory/timeout error on complex EXISTS

**Recent Fixes** (2026-01-28):
- [x] LEFT OUTER JOIN bug fixed - Q13 now correct
- [x] Data generator fixed - Q22 now correct

**Key Files**: `hash_join.rs`, `hash_agg.rs`

---

### 2. Memory Safety [DONE]

**Status**: Cross join no longer crashes. Graceful failure before OOM.

**Completed**:
- [x] Memory limit checks in HashJoinExec (100M rows / 4GB limit)
- [x] Checks happen DURING build collection (fail early)

**Key Files**: `hash_join.rs:164-195`

---

### 3. Performance [IN PROGRESS]

**Status**: Infrastructure exists, needs integration and fixes.

**What Works**:
- [x] Parallel SEMI/ANTI joins (5.8x on Q21 at SF=0.01)
- [x] Parallel hash join build with rayon
- [x] Subquery memoization
- [x] Join reordering (158x on Q8)

**What's Broken/Not Integrated**:
- [ ] DelimJoin disabled (column resolution bugs)
- [ ] Morsel execution not in main path
- [ ] Join ordering not cost-based

**Key Files**: `delim_join.rs`, `flatten_dependent_join.rs`, `morsel.rs`, `join_reorder.rs`

---

### 4. Subquery Decorrelation [PARTIAL]

**Status**: Basic decorrelation works, complex patterns fail.

**Working**:
- [x] EXISTS → Semi Join
- [x] NOT EXISTS → Anti Join
- [x] Simple scalar subqueries

**Not Working**:
- [ ] Multiple correlated EXISTS (Q21 pattern)
- [ ] DelimJoin for O(n) execution

**Key Files**: `subquery_decorrelation.rs`, `flatten_dependent_join.rs`

---

### 5. Trino Functions [DONE]

**Status**: 160+ functions implemented, 161 validation tests.

**Key Files**: `filter.rs`, `hash_agg.rs`

---

### 6. Larger-than-Memory [PARTIAL]

**Status**: Infrastructure exists but not fully integrated.

**What Exists**:
- [x] MemoryPool with RAII reservations
- [x] StreamingParquetReader
- [x] Spillable operator implementations

**What's Missing**:
- [ ] Fix SpillableHashJoinExec schema bugs
- [ ] Integration tests

---

### 7. Window Functions [NOT STARTED]

**Status**: Not implemented. Required for full SQL compatibility.

---

### 8. Cost-Based Optimizer [NOT STARTED]

**Status**: Placeholder exists in `cost.rs` but not functional.

**Required for**: Q07, Q09 performance

---

## Quick Reference

### Testing Commands

```bash
# All tests
cargo test --release

# TPC-H benchmark at SF=10
./target/release/query_engine benchmark-parquet --path data/tpch-10gb-new

# Compare with DuckDB
python benchmark_vs_duckdb.py

# Specific query debug
./target/release/query_engine query --num 21 --sf 0.1 --plan
```

### Key Directories

```
src/
├── physical/operators/
│   ├── hash_join.rs      # LEFT JOIN fix here
│   ├── hash_agg.rs       # Q11 HAVING bug likely here
│   ├── delim_join.rs     # Q21 fix needed here
│   └── subquery.rs       # Correlated subquery execution
├── optimizer/rules/
│   ├── join_reorder.rs   # Q07/Q09 fix needed here
│   └── flatten_dependent_join.rs  # Q21 column resolution bug
└── execution/
    └── context.rs        # Morsel integration needed here
```

---

## Benchmark Report

Full benchmark report: [docs/benchmark-report-sf10.md](../docs/benchmark-report-sf10.md)
