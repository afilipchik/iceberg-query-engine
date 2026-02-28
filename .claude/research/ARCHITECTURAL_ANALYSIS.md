# Iceberg Query Engine: Architectural Analysis & Improvement Roadmap

**Date**: 2026-02-12
**Status**: Research Complete, Implementation Pending

---

## Executive Summary

The iceberg-query-engine is 100% Rust, targeting TPC-H benchmark performance. Currently ~136x slower than DuckDB overall, but this is dominated by Q21 (2790x slower). Excluding Q21, we're ~11x slower.

**Key Insight**: We're not hitting diminishing returns on optimizations — we're hitting **missing architectural components**. The "manifold" problem: 2D optimizations (row-group filtering, parallel hash build) give 10-50% gains, but the 100x gap requires Z-dimension components.

---

## Current Performance Baseline (SF=0.1 / 100MB)

| Query | Our Engine | DuckDB | Ratio | Category |
|-------|------------|--------|-------|----------|
| Q01   | 41ms       | 15ms   | 2.7x  | OK |
| Q02   | 9ms        | 11ms   | **0.8x** | FASTER |
| Q03   | 52ms       | 12ms   | 4.3x  | OK |
| Q04   | 3ms        | 16ms   | **0.2x** | 5x FASTER |
| Q05   | 256ms      | 24ms   | 10.7x | Needs work |
| Q06   | 8ms        | 25ms   | **0.3x** | 3x FASTER |
| Q07   | 2656ms     | 20ms   | 133x  | **CRITICAL** |
| Q08   | 63ms       | 19ms   | 3.3x  | OK |
| Q09   | 792ms      | 15ms   | 53x   | **CRITICAL** |
| Q10   | 132ms      | 21ms   | 6.3x  | Needs work |
| Q11   | 128ms      | 34ms   | 3.8x  | OK |
| Q12   | 43ms       | 32ms   | 1.3x  | Good |
| Q13   | 52ms       | 35ms   | 1.5x  | Good |
| Q14   | 12ms       | 31ms   | **0.4x** | 2.6x FASTER |
| Q15   | 59ms       | 30ms   | 2.0x  | OK |
| Q16   | 20ms       | 19ms   | 1.1x  | Good |
| Q17   | 199ms      | 10ms   | 20x   | Subquery |
| Q18   | 364ms      | 12ms   | 30x   | Subquery |
| Q19   | 305ms      | 15ms   | 20x   | Complex predicates |
| Q20   | 120ms      | 26ms   | 4.6x  | OK |
| Q21   | 61373ms    | 22ms   | **2790x** | **MAIN BOTTLENECK** |
| Q22   | 51ms       | 20ms   | 2.6x  | OK |
| **TOTAL** | **66.7s** | **0.49s** | **136x** | |
| **Excl Q21** | **5.3s** | **0.47s** | **11x** | |

**Key findings:**
- Q21 takes 92% of total time (61s out of 66.7s)
- Q7, Q9 are multi-way join bottlenecks (133x, 53x)
- Several queries (Q02, Q04, Q06, Q14) are FASTER than DuckDB
- Q17, Q18, Q19, Q21 are subquery-related bottlenecks

---

## Existing Architectural Components

### ✅ Implemented & Working

| Component | Location | Description |
|-----------|----------|-------------|
| DelimJoin/DelimGet | `src/physical/operators/delim_join.rs` | DuckDB-style deduplicated join for correlated subqueries |
| FlattenDependentJoin | `src/optimizer/rules/flatten_dependent_join.rs` | Transforms correlated subqueries to joins |
| Join Reordering | `src/optimizer/rules/join_reorder.rs` | Greedy join ordering, eliminates cross joins |
| Predicate Pushdown | `src/optimizer/rules/predicate_pushdown.rs` | Pushes filters to table scans |
| Projection Pushdown | `src/optimizer/rules/projection_pushdown.rs` | Minimizes column reads |
| Parallel Hash Join Build | `src/physical/operators/hash_join.rs` | Uses rayon for parallel hash table building |
| Subquery Memoization | `src/physical/operators/subquery.rs` | Caches subquery results by correlation key |
| 100+ Trino Functions | Various | Comprehensive function library |

### ⚠️ Implemented but NOT WIRED

| Component | Location | Problem |
|-----------|----------|---------|
| **Morsel-Driven Parallelism** | `src/physical/morsel.rs` (312 lines) | Only used in `examples/`, not in main pipeline |
| **ParallelParquetSource** | `src/physical/morsel.rs` | Work-stealing row group reader, bypassed by ParquetScanExec |
| **Morsel Aggregation** | `src/physical/morsel_agg.rs` (703 lines) | Thread-local accumulators, not integrated |
| **Vectorized Aggregation** | `src/physical/vectorized_agg.rs` | Inline TypedGroupKey, FNV hasher, row-group filtering |

### ❌ Missing

| Component | DuckDB Has | We Need | Impact |
|-----------|------------|---------|--------|
| Radix-Partitioned Joins | Partitioned hash tables | hashbrown::HashMap | 2-3x on joins |
| Arena Allocators | Custom memory pools | Standard allocations | 1.5-2x |
| Custom Parquet Reader | Direct decode with SIMD | Arrow parquet crate | 1.5-2x |
| DP Join Ordering | Cost-based | Greedy only | Better multi-way joins |
| Window Functions | ROW_NUMBER, RANK, etc. | Not implemented | TPC-DS support |
| Histogram Statistics | Multi-column histograms | Simple heuristics | Better cardinality estimates |

---

## The "Why Isn't It Wired" Problem

### ParquetScanExec (Current - Slow)

```rust
// src/physical/operators/parquet.rs:90-131
async fn execute(&self, _partition: usize) -> Result<RecordBatchStream> {
    let file = File::open(&self.path)?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let mut reader = builder.build()?;  // Sequential, single-threaded

    loop {
        match reader.next() { ... }  // One batch at a time
    }
}
```

### ParallelParquetSource (Not Used - Fast)

```rust
// src/physical/morsel.rs:219-250
pub fn read_all_parallel(&self) -> Result<Vec<Morsel>> {
    (0..num_threads).into_par_iter().map(|_| {
        while let Some(work) = self.get_work() {  // Work-stealing queue
            let batches = self.read_row_group(&work)?;
            // Process in parallel
        }
    })
}
```

**The disconnect**: Two separate code paths. The fast path only exists in `examples/` as standalone benchmarks.

---

## ROI-Ranked Improvement Roadmap

### Tier 1: CRITICAL (10x+ overall improvement)

#### 1. Complete DelimJoin for Multi-EXISTS Patterns
- **Target**: Q21
- **Current**: 61s (2790x slower)
- **Expected**: <100ms (sub-5x)
- **Impact**: 12x overall improvement (66.7s → 5.3s)
- **Effort**: Medium (3-5 days)
- **Location**: `src/optimizer/rules/flatten_dependent_join.rs`
- **What's needed**:
  - Extend `FlattenDependentJoin` to handle `WHERE EXISTS(...) AND NOT EXISTS(...)`
  - Decompose into two Semi/Anti joins instead of row-by-row
  - Wire DelimGet correctly for multiple correlation columns

#### 2. Wire Morsel-Driven Parallelism into Main Pipeline
- **Target**: All scan-heavy queries (Q1, Q6, Q12, Q14)
- **Current**: Sequential Parquet reads
- **Expected**: 4-8x faster (proven in examples)
- **Impact**: 4-8x on affected queries
- **Effort**: Medium (2-4 days)
- **Location**: `src/physical/operators/parquet.rs`, `src/physical/operators/hash_agg.rs`
- **What's needed**:
  - Add `parallel: bool` flag to `ParquetScanExec`
  - Use `ParallelParquetSource` when parallel=true
  - Teach `HashAggregateExec` to use thread-local hash tables
  - Add merge phase for thread-local results

### Tier 2: HIGH IMPACT (2-5x improvement)

#### 3. Row-Group Min/Max Filtering
- **Target**: Queries with selective filters (Q6, Q12, Q14, Q19)
- **Current**: Read all row groups
- **Expected**: 2-5x faster (skip irrelevant row groups)
- **Impact**: 2-5x on selective queries
- **Effort**: Low (1-2 days)
- **Location**: `src/physical/operators/parquet.rs`
- **What's needed**:
  - Use Parquet row group metadata statistics
  - Skip row groups where filter predicates cannot match
  - Already partially implemented in `vectorized_agg.rs`

#### 4. Fix Multi-Way Join Performance (Q7, Q9)
- **Target**: Q7 (133x), Q9 (53x)
- **Current**: Suboptimal join ordering
- **Expected**: 10-20x faster
- **Impact**: Major on these queries
- **Effort**: Medium (3-5 days)
- **What's needed**:
  - Dynamic programming join ordering (beyond greedy)
  - Better cardinality estimation
  - Consider radix-partitioned joins

#### 5. Dictionary Encoding Preservation
- **Target**: String-heavy queries (Q16, Q19)
- **Current**: Decode DictionaryArray to StringArray during filter
- **Expected**: 2-4x faster on string operations
- **Impact**: 2-4x on affected queries
- **Effort**: Medium (2-3 days)
- **Location**: `src/physical/operators/filter.rs`
- **What's needed**:
  - Teach `evaluate_expr()` to operate on DictionaryArray keys
  - For equality, compare dictionary indices instead of strings

### Tier 3: MODERATE IMPACT (1.2-2x improvement)

#### 6. LowCardinality Global Dictionary
- **Target**: Aggregation with string group-by (Q1, Q4, Q13)
- **Current**: Per-batch dictionary or raw strings
- **Expected**: 2-3x faster
- **Effort**: Medium (2-3 days)
- **What's needed**:
  - Maintain global dictionary for low-cardinality columns
  - Aggregations operate on integer indices

#### 7. Histogram-Based Cost Model
- **Target**: All queries (better join ordering)
- **Current**: Simple heuristics (selectivity = 0.1)
- **Expected**: Better plans for complex queries
- **Effort**: High (5-7 days)
- **Location**: `src/optimizer/cost.rs`
- **What's needed**:
  - Build equi-depth histograms during first scan
  - Use for selectivity estimation

#### 8. Lazy Vectors
- **Target**: Wide tables with few columns accessed
- **Current**: Eagerly decode all columns
- **Expected**: 1.5-3x faster
- **Effort**: Medium (3-4 days)
- **What's needed**:
  - Defer Parquet column decode until accessed
  - Only decode columns in projection/filter

### Tier 4: RELIABILITY & MATURITY

#### 9. Pre-Allocated Morsel Buffers
- **Target**: Memory efficiency
- **Current**: New RecordBatch allocations per operator
- **Expected**: Reduced GC pressure, better latency
- **Effort**: Low (1-2 days)

#### 10. Deterministic Testing
- **Target**: Concurrency bug detection
- **Current**: No concurrency testing
- **Expected**: Catch race conditions in CI
- **Effort**: Medium (2-3 days)
- **What's needed**:
  - `DeterministicScheduler` that replays thread schedules
  - Use in CI for parallel hash join / aggregation

#### 11. io_uring Async I/O (Linux only)
- **Target**: I/O-bound queries
- **Current**: tokio::spawn_blocking
- **Expected**: 1.2-1.5x faster
- **Effort**: Low (1-2 days)

---

## Summary: Priority Order

| Rank | Improvement | Impact | Effort | ROI |
|------|-------------|--------|--------|-----|
| 1 | Complete DelimJoin for Q21 | 12x overall | Medium | **CRITICAL** |
| 2 | Wire morsel parallelism | 4-8x on scans | Medium | **CRITICAL** |
| 3 | Row-group filtering | 2-5x selective | Low | HIGH |
| 4 | Fix multi-way joins (Q7, Q9) | 10-20x on these | Medium | HIGH |
| 5 | Dictionary preservation | 2-4x string ops | Medium | MEDIUM |
| 6 | LowCardinality dict | 2-3x agg | Medium | MEDIUM |
| 7 | Histogram cost model | Better plans | High | MEDIUM |
| 8 | Lazy vectors | 1.5-3x wide tables | Medium | MEDIUM |
| 9 | Pre-allocated buffers | Latency | Low | LOW |
| 10 | Deterministic testing | Reliability | Medium | LOW |
| 11 | io_uring I/O | 1.2-1.5x I/O | Low | LOW |

---

## Two-Repo Setup

| Repo | Path | Branch | Status |
|------|------|--------|--------|
| User's fork | `otherdirs/iceberg-query-engine/` | `ea/ui_and_fixups` | Primary development, 54 commits |
| Upstream clone | `otherdirs/iceberg-query-engine-upstream/` | `main` | Reference, 1 unique commit (predicate_reordering.rs) |

**GitHub**: `afilipchik/iceberg-query-engine`
**Issues**:
- #9: Performance vs DuckDB (this doc addresses)
- #10: Trino function parity (100+ functions done)
- #11: LIMIT bug (needs investigation)
- #5: Parallelize agentic work (AGENT_COORDINATION.md exists)

---

## Key Files Reference

| Component | File |
|-----------|------|
| Morsel framework | `src/physical/morsel.rs` |
| Morsel aggregation | `src/physical/morsel_agg.rs` |
| Vectorized aggregation | `src/physical/vectorized_agg.rs` |
| DelimJoin operators | `src/physical/operators/delim_join.rs` |
| FlattenDependentJoin | `src/optimizer/rules/flatten_dependent_join.rs` |
| ParquetScanExec | `src/physical/operators/parquet.rs` |
| HashAggregateExec | `src/physical/operators/hash_agg.rs` |
| SubqueryExecutor | `src/physical/operators/subquery.rs` |
| TPC-H queries | `src/tpch/queries.rs` |
| Architectural Decisions | `otherdirs/Architectural Decisions.md` |

---

## Research Sources

- DuckDB: Morsel-driven parallelism, DelimJoin, zone maps
- Velox: Lazy vectors, dictionary preservation, memory arbitration
- ClickHouse: LowCardinality, approximate functions, data skipping indexes
- CockroachDB: Histogram-based cost model
- TigerBeetle: Pre-allocated buffers, deterministic testing, io_uring
