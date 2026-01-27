# Query Engine Development Roadmap

## Overview

This document tracks the main development direction. Each work area has a satellite doc with implementation details.

---

## Current Status Summary

| Area | Status | Doc |
|------|--------|-----|
| **Correctness** | ✅ DONE | [archive/correctness.md](archive/correctness.md) |
| **Memory Safety** | ✅ DONE | [archive/memory-safety.md](archive/memory-safety.md) |
| **Performance** | 🔄 IN PROGRESS | [performance-optimization-plan.md](performance-optimization-plan.md) |
| **Trino Functions** | ✅ DONE (160+) | [trino-function-implementation.md](trino-function-implementation.md) |
| **Larger-than-Memory** | ⚠️ PARTIAL | [larger-than-memory-support.md](larger-than-memory-support.md) |
| **Window Functions** | ❌ NOT STARTED | See below |
| **Cost-Based Optimizer** | ❌ NOT STARTED | See below |

---

## Work Areas

### 1. Correctness [DONE]

**Status**: All 128 SQL tests pass. 15/22 TPC-H match DuckDB.

**Completed**:
- [x] Hash join partition handling - collect ALL partitions for build side
- [x] Build side caching with OnceCell - reuse across partition executions
- [x] JoinReorder optimizer - preserves table qualifiers for aliased tables
- [x] Cross join detection - 10M row limit check

**Remaining Issues** (high priority):
- [ ] Value mismatches in Q2, Q7, Q13, Q17, Q21 (rounding/ordering differences)
- [ ] Q20 timeout (complex correlated subquery)

**Key Files**: `hash_join.rs`, `join_reorder.rs`

---

### 2. Memory Safety [DONE]

**Status**: Cross join no longer crashes. Graceful failure before OOM.

**Completed**:
- [x] Memory limit checks in HashJoinExec (50M rows / 4GB limit)
- [x] Checks happen DURING build collection (fail early)
- [x] PhysicalPlanner wired to ExecutionConfig
- [x] Spillable operators available (opt-in via `enable_spilling`)

**Infrastructure Ready** (needs bug fixes to enable):
- [ ] SpillableHashJoinExec - has schema issues
- [ ] SpillableHashAggregateExec - has Parquet I/O bugs
- [ ] ExternalSortExec - has schema issues

**Key Files**: `hash_join.rs:164-195`, `planner.rs`, `memory.rs`

**Usage**:
```rust
// Regular mode (default) - uses memory limits
let ctx = ExecutionContext::new();

// Spillable mode (opt-in, has bugs)
let config = ExecutionConfig::new().with_spilling(true);
let ctx = ExecutionContext::with_config(config);
```

---

### 3. Performance [IN PROGRESS]

**Status**: 8x improvement on Q1 via morsel-driven parallelism.

**Completed**:
- [x] Morsel-driven parallelism (morsel.rs, morsel_agg.rs)
- [x] Parallel hash join build with rayon
- [x] Subquery memoization
- [x] Async Parquet I/O
- [x] Row-group statistics filtering

**Benchmarks** (SF=10, ~60M rows):
| Query | Our Engine | DuckDB | Ratio |
|-------|-----------|--------|-------|
| Q1 | 227ms | 84ms | 2.7x |
| Q6 | 549ms | 66ms | 8x |

**Next Steps**:
- [ ] Integrate morsel execution into main path
- [ ] Subquery decorrelation (Q17-Q22)
- [ ] Cost-based join ordering

**Key Files**: `morsel.rs`, `morsel_agg.rs`, `vectorized_agg.rs`

---

### 4. Trino Functions [DONE]

**Status**: 160+ functions implemented, 161 validation tests.

**Categories Implemented**:
- Math: 40+ functions (ABS, ROUND, SQRT, SIN, COS, etc.)
- String: 35+ functions (UPPER, TRIM, SUBSTRING, CONCAT, etc.)
- Date/Time: 25+ functions (YEAR, DATE_TRUNC, DATE_ADD, etc.)
- Aggregate: 30+ functions (COUNT, SUM, AVG, STDDEV, CORR, etc.)
- JSON: 14 functions (JSON_EXTRACT, JSON_ARRAY_LENGTH, etc.)
- Regex: 6 functions (REGEXP_LIKE, REGEXP_EXTRACT, etc.)
- Binary/Encoding: 14 functions (MD5, SHA256, TO_HEX, etc.)
- Bitwise: 8 functions (BITWISE_AND, BIT_COUNT, etc.)
- URL: 9 functions (URL_EXTRACT_HOST, URL_ENCODE, etc.)

**Not Implemented** (lower priority):
- Window functions (requires new infrastructure)
- Array/Map functions (requires complex type support)
- Geospatial functions (not needed for TPC-H)

**Key Files**: `filter.rs`, `hash_agg.rs`, `function_validation_tests.rs`

---

### 5. Larger-than-Memory [PARTIAL]

**Status**: Infrastructure exists but not fully integrated.

**What Exists**:
- [x] MemoryPool with RAII reservations
- [x] ExecutionConfig with limits
- [x] StreamingParquetReader
- [x] Spillable operator implementations

**What's Missing**:
- [ ] Fix SpillableHashJoinExec schema bugs
- [ ] Fix SpillableHashAggregateExec Parquet I/O
- [ ] Fix ExternalSortExec
- [ ] Integration tests with memory limits

---

### 6. Window Functions [NOT STARTED]

**Status**: Not implemented. Required for full Trino SQL compatibility.

**Required Infrastructure**:
- [ ] `WindowExpr` - Window expression type in logical expressions
- [ ] `WindowNode` - Logical plan node for window operations
- [ ] `WindowExec` - Physical operator for window execution
- [ ] Frame specification parsing (ROWS/RANGE/GROUPS)
- [ ] PARTITION BY and ORDER BY handling

**Functions to Implement**:
- [ ] ROW_NUMBER()
- [ ] RANK(), DENSE_RANK()
- [ ] NTILE(n)
- [ ] LEAD(x, offset, default), LAG(x, offset, default)
- [ ] FIRST_VALUE(x), LAST_VALUE(x), NTH_VALUE(x, n)
- [ ] PERCENT_RANK(), CUME_DIST()

**Complexity**: High - requires new operator infrastructure

**Key Files**: Will need new files `window.rs` in planner and physical operators

---

### 7. Cost-Based Optimizer [NOT STARTED]

**Status**: Placeholder exists in `cost.rs` but not functional.

**Required Components**:
- [ ] Table statistics collection (row count, cardinality, min/max)
- [ ] Cost model for operators (scan, filter, join, aggregate)
- [ ] Cardinality estimation for predicates
- [ ] Join order enumeration (dynamic programming or greedy with cost)
- [ ] Physical plan selection (hash join vs sort-merge join)

**Benefits**:
- Better join ordering for complex queries (Q5, Q7, Q8, Q9)
- Automatic selection of hash vs sort-merge join
- More accurate memory estimation

**Current Workaround**: Greedy join reordering based on predicate structure

**Complexity**: High - requires statistics infrastructure

**Key Files**: `cost.rs`, `join_reorder.rs`

---

## Quick Reference

### How to Pick Work

1. Check status above
2. Go to satellite doc for details
3. Find specific implementation steps
4. Key files are listed for each area

### Testing Commands

```bash
# All tests
cargo test

# SQL correctness (128 tests)
cargo test --test sql_comprehensive

# Function validation (161 tests)
cargo test --test function_validation_tests

# TPC-H benchmark
cargo run --release --example benchmark_runner -- <query> ./data/tpch-100mb

# Q21 (previously crashed)
cargo run --release --example debug_q21
```

### Key Directories

```
src/
├── physical/operators/
│   ├── hash_join.rs      # Join with memory limits
│   ├── hash_agg.rs       # Aggregation
│   ├── spillable.rs      # Spillable operators (has bugs)
│   ├── filter.rs         # Expression evaluation, functions
│   └── subquery.rs       # Correlated subquery execution
├── optimizer/rules/
│   └── join_reorder.rs   # Join order optimization
├── execution/
│   ├── context.rs        # Main entry point
│   └── memory.rs         # Memory pool, config
└── storage/
    └── parquet.rs        # Parquet reading
```
