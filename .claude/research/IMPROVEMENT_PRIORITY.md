# Improvement Priority Quick Reference

**Last Updated**: 2026-02-12
**Full Analysis**: `.claude/research/ARCHITECTURAL_ANALYSIS.md`

---

## Top 5 Actions (Do These First)

### 1. Complete DelimJoin for Q21 (CRITICAL)
```
File: src/optimizer/rules/flatten_dependent_join.rs
Target: Q21 (currently 2790x slower, 61s vs 22ms)
Impact: 12x overall improvement (66.7s → 5.3s)
Effort: Medium (3-5 days)
What: Handle WHERE EXISTS(...) AND NOT EXISTS(...) patterns
      Decompose into Semi/Anti joins instead of row-by-row
```

### 2. Wire Morsel Parallelism (CRITICAL)
```
Files: src/physical/operators/parquet.rs, src/physical/operators/hash_agg.rs
Target: All scan queries (Q1, Q6, Q12, Q14)
Impact: 4-8x (proven in examples/morsel_test_projected.rs)
Effort: Medium (2-4 days)
What: Replace ParquetScanExec sequential read with ParallelParquetSource
      Add thread-local hash tables to HashAggregateExec + merge
```

### 3. Row-Group Min/Max Filtering (HIGH)
```
File: src/physical/operators/parquet.rs
Target: Selective queries (Q6, Q12, Q14, Q19)
Impact: 2-5x
Effort: Low (1-2 days)
What: Use Parquet row_group.column(i).statistics() to skip row groups
      Already partially done in vectorized_agg.rs
```

### 4. Fix Multi-Way Joins Q7/Q9 (HIGH)
```
Files: src/optimizer/rules/join_reorder.rs, src/optimizer/cost.rs
Target: Q7 (133x), Q9 (53x)
Impact: 10-20x on these queries
Effort: Medium (3-5 days)
What: Dynamic programming join ordering (beyond greedy)
      Better cardinality estimation
      Consider radix-partitioned joins
```

### 5. Dictionary Encoding Preservation (MEDIUM)
```
File: src/physical/operators/filter.rs
Target: String-heavy queries (Q16, Q19)
Impact: 2-4x on string ops
Effort: Medium (2-3 days)
What: Operate on DictionaryArray keys instead of decoding to StringArray
      Compare indices for equality instead of string comparison
```

---

## Key Performance Numbers

| Metric | Value |
|--------|-------|
| Total vs DuckDB | 136x slower |
| Excluding Q21 | 11x slower |
| Q21 alone | 92% of total time |
| Morsel example vs main | 8x faster |
| Vectorized Q1 vs DuckDB | 1.4x slower (close!) |
| Queries faster than DuckDB | Q02, Q04, Q06, Q14 |

---

## The "Not Wired" Problem

```
ParquetScanExec (parquet.rs:90-131)
  └── Uses sequential ParquetRecordBatchReaderBuilder
  └── NOT using ParallelParquetSource from morsel.rs

ParallelParquetSource (morsel.rs:219-250)
  └── Work-stealing row group reader
  └── Only used in examples/morsel_test_*.rs
  └── Needs to replace ParquetScanExec's reader
```

---

## File Locations Quick Map

```
Morsel framework        → src/physical/morsel.rs
Morsel aggregation      → src/physical/morsel_agg.rs
Vectorized aggregation  → src/physical/vectorized_agg.rs
DelimJoin operators     → src/physical/operators/delim_join.rs
FlattenDependentJoin    → src/optimizer/rules/flatten_dependent_join.rs
ParquetScanExec         → src/physical/operators/parquet.rs
HashAggregateExec       → src/physical/operators/hash_agg.rs
SubqueryExecutor        → src/physical/operators/subquery.rs
TPC-H queries           → src/tpch/queries.rs
Architectural Decisions → otherdirs/Architectural Decisions.md
```

---

## Verification Commands

```bash
# Run TPC-H benchmark
cargo run --release -- benchmark --sf 0.1

# Run morsel example (proves parallel code works)
cargo run --release --example morsel_test_projected

# Run all tests
cargo test

# Check specific query
cargo run --release -- query --num 21 --sf 0.1 --plan
```
