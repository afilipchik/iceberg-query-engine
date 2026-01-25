# TPC-H Benchmark: Query Engine vs DuckDB

**Date:** 2026-01-25
**System:** Linux 6.14.0-34-generic x86_64
**CPU:** 13th Gen Intel(R) Core(TM) i9-13900KF
**DuckDB Version:** v1.4.3

## TPC-H SF=0.1 (100MB Dataset)

| Query | Description | DuckDB (ms) | Query Engine (ms) | Ratio | Winner |
|-------|-------------|-------------|-------------------|-------|--------|
| Q1 | Pricing Summary | 52 | 79 | 1.5x | DuckDB |
| Q3 | Shipping Priority | 37 | 96 | 2.6x | DuckDB |
| Q4 | Order Priority (EXISTS) | 16 | 1 | **0.06x** | **Engine** |
| Q5 | Local Supplier Volume | 28 | 2,565 | 91.6x | DuckDB |
| Q6 | Revenue Forecast | 20 | 9 | **0.46x** | **Engine** |
| Q10 | Returned Item Report | 25 | 70 | 2.8x | DuckDB |
| Q12 | Shipping Modes | 32 | 203 | 6.3x | DuckDB |

## TPC-H SF=10 (10GB Dataset)

| Query | Description | DuckDB (ms) | Query Engine (ms) | Ratio |
|-------|-------------|-------------|-------------------|-------|
| Q1 | Pricing Summary | 115 | 3,050 | 26.5x |
| Q3 | Shipping Priority | 210 | 945 | 4.5x |
| Q4 | Order Priority | 25 | 133 | 5.3x |
| Q6 | Revenue Forecast | 94 | 570 | 6.1x |

## Analysis

### Query Engine Wins (SF=0.1)

- **Q4 (16x faster)**: Simple aggregation with EXISTS subquery - our optimizer handles this efficiently
- **Q6 (2x faster)**: Simple scan with filter and aggregation - benefits from our predicate pushdown

### DuckDB Advantages

- **Q1, Q3, Q5, Q10, Q12**: Multi-table joins and complex aggregations
- DuckDB has highly optimized vectorized execution and parallel processing
- Uses SIMD instructions for columnar operations

### Key Observations

1. **Small Dataset Performance**: On 100MB data, our engine is competitive on simple queries (Q4, Q6) but struggles with complex joins

2. **Scalability Gap**: As data size increases (10GB), the performance gap widens. DuckDB's vectorized execution and parallel processing scale better

3. **Memory Safety**: With the new spillable operators, our engine maintains stable memory usage (~87MB) even on complex queries like Q21 that previously grew to 12GB+

4. **Join Performance**: The main bottleneck is multi-table join handling. DuckDB uses advanced join algorithms and parallel execution

### Performance Ratio Interpretation

- **Ratio < 1.0**: Query Engine is faster than DuckDB
- **Ratio = 1.0**: Equal performance
- **Ratio > 1.0**: DuckDB is faster

### Recommendations for Improvement

1. **Parallel execution**: Implement morsel-driven parallelism for all operators
2. **Vectorized processing**: Process batches using SIMD operations
3. **Join optimization**: Implement partition-wise hash join with parallel build
4. **Subquery decorrelation**: Convert correlated subqueries to joins

## Dataset Information

### 100MB Dataset (SF=0.1)
- lineitem: ~600K rows
- orders: ~150K rows
- customer: ~15K rows

### 10GB Dataset (SF=10)
- lineitem: ~60M rows
- orders: ~15M rows
- customer: ~1.5M rows
