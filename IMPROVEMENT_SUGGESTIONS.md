# Improvement Suggestions

Research-based improvement suggestions for the iceberg-query-engine, a Rust SQL query engine targeting Apache Iceberg tables. These suggestions are informed by studying production-grade systems (DuckDB, Velox, ClickHouse, CockroachDB, TigerBeetle) and grounded in the current codebase architecture.

## Current Performance Baseline (TPC-H SF=0.1)

- **Total: 66.7s** vs DuckDB 0.49s (136x slower)
- **Q21 alone: 61.3s** (nested EXISTS = O(n^2)) -- 92% of total runtime
- **Q7: 2656ms** (133x slower), **Q9: 792ms** (53x slower) -- multi-way join bottleneck
- Several queries **faster** than DuckDB: Q02 (0.8x), Q04 (0.2x), Q06 (0.3x), Q14 (0.4x)
- Excluding Q21: 5.3s total vs 0.47s DuckDB (11x slower)

## Priority-Ranked Improvements

| # | Source System | Improvement | Expected Impact | Effort Level |
|---|---------------|-------------|-----------------|--------------|
| 1 | DuckDB | **Complete DelimJoin for Q21** -- Extend `FlattenDependentJoin` in `src/optimizer/rules/flatten_dependent_join.rs` to handle multiple correlated subqueries (`EXISTS(...) AND NOT EXISTS(...)`). Currently, the rule bails out when `subquery_exprs.len() > 1` (line 165) because sequential processing corrupts the schema. The fix requires either chaining DelimJoins with schema propagation or implementing a single multi-subquery DelimJoin node. | Q21: 61.3s to under 100ms (2790x improvement). Total: 66.7s to 5.3s. This single fix removes 92% of runtime. | High |
| 2 | DuckDB | **Integrate morsel parallelism into main pipeline** -- Wire `ParallelParquetSource` (in `src/physical/morsel.rs`) into `ParquetScanExec` (in `src/physical/operators/parquet.rs`). Currently the morsel framework exists but is only used in standalone examples (`examples/morsel_test*.rs`). The main `ParquetScanExec::execute()` still reads all row groups sequentially on a single thread (line 114). Integrating it requires making `ParquetScanExec` dispatch row groups across rayon workers and stream morsels back via tokio channels. | 4-8x improvement on scan-heavy queries (Q1, Q6, Q12). Demonstrated 8x speedup in `morsel_test_projected` example already. | Medium |
| 3 | DuckDB | **Row-group min/max filtering** -- Use Parquet row group statistics (min/max values per column) to skip irrelevant row groups at scan time. The `ParquetRecordBatchReaderBuilder` already provides access to `RowGroupMetaData` with column statistics. Add a `RowGroupPruner` that evaluates filter predicates against these statistics before opening each row group. The `IcebergScanExec` in `src/physical/operators/iceberg.rs` already has file-level statistics pruning; this extends it to the row-group level. | 2-5x on selective queries (Q6, Q12, Q14) that filter on range predicates like `l_shipdate BETWEEN '1994-01-01' AND '1995-01-01'`. Impact scales with data size since larger files have more row groups to skip. | Low |
| 4 | Velox/Meta | **Dictionary encoding preservation** -- When reading Parquet dictionary-encoded columns, preserve the `DictionaryArray` representation through `FilterExec` instead of immediately decoding to plain string arrays. Operate on integer dictionary keys in hash joins and aggregations. Currently, `evaluate_expr()` in `src/physical/operators/filter.rs` always operates on decoded arrays. Add `DictionaryArray<Int32Type>` match arms to key comparison paths. | 2-4x on string-heavy queries at large scale factors. Reduces memory bandwidth for group-by on low-cardinality string columns like `l_returnflag`, `l_linestatus`, `o_orderpriority`. | Medium |
| 5 | ClickHouse | **LowCardinality global dictionary** -- For known low-cardinality columns (status codes, flags, priorities), build a global dictionary at table registration time and use integer indices throughout the pipeline. This is similar to dictionary encoding but controlled at the engine level rather than relying on Parquet encoding. Implement as a new `LowCardinalityArray` wrapper that wraps an `Int32Array` of indices plus a shared `StringArray` dictionary. | 2-3x on aggregation queries with string group-by keys (Q1 groups by `l_returnflag`/`l_linestatus`, Q4 groups by `o_orderpriority`, Q13 groups by `c_count`). Hash table operations on 4-byte integers are much faster than variable-length strings. | Medium |
| 6 | CockroachDB | **Histogram-based cost model** -- Replace the current fixed-selectivity cost estimator in `src/optimizer/cost.rs` (which hardcodes 30% for filters and 10% for joins at lines 122 and 152) with equi-depth histograms built from Parquet column statistics. Collect min/max/distinct-count per column when registering tables, then use these to estimate selectivity for equality and range predicates. Feed accurate cardinality estimates into the `JoinReorder` rule for better join ordering. | Better join ordering for multi-way joins in Q5, Q7, Q8, Q9. The current join reorder rule in `src/optimizer/rules/join_reorder.rs` uses a greedy algorithm that doesn't account for intermediate result sizes. Accurate cardinality estimates would prevent the blowup seen in Q7 (2656ms) and Q9 (792ms). | High |
| 7 | Velox/Meta | **Lazy vectors** -- Defer column decoding in Parquet reads until the column is actually accessed by an operator. Currently `ParquetScanExec` reads all columns into memory even if downstream operators only use a subset (line 106 reads with no projection). Implement a `LazyBatch` wrapper that holds a `ParquetRowGroupReader` handle and decodes columns on first access. This avoids decoding columns that are pruned by early filters. | 1.5-3x on wide-table queries where filters eliminate most rows before all columns are needed. Particularly beneficial for queries that filter on one column but project many (e.g., Q19 filters on `p_brand`/`p_container` but projects `l_extendedprice`). | Medium |
| 8 | TigerBeetle | **Pre-allocated morsel buffers** -- Reuse `RecordBatch` allocations across morsel iterations instead of allocating new Arrow buffers for each batch. Create a `MorselPool` that maintains a free-list of pre-sized `MutableBuffer` objects. When an operator produces a batch, it draws buffers from the pool; when a batch is consumed, buffers return to the pool. This eliminates the per-batch allocation overhead visible in tight scan loops. | Latency reduction on short-running queries. Reduces allocator pressure and improves cache locality. Most visible on Q2 (9ms), Q4 (3ms), Q6 (8ms) where per-batch overhead is a significant fraction of total runtime. | Low |
| 9 | TigerBeetle | **io_uring async I/O** (Linux only) -- Replace synchronous `File::open` + `read` calls in `ParquetScanExec` and `ParallelParquetSource` with `tokio-uring` for true async kernel-level I/O. Currently, Parquet reads use `std::fs::File` (synchronous) even though the operator trait is async. The `AsyncParquetReader` in `src/storage/parquet.rs` uses `tokio::fs` which is just a thread-pool wrapper around blocking I/O. `io_uring` provides zero-copy reads with kernel-level scheduling. | 1.2-1.5x on I/O-bound queries when reading from NVMe storage. The improvement is larger at higher scale factors (SF=1 and above) where data does not fit in OS page cache. Limited to Linux; macOS would need a kqueue fallback. | Low |
| 10 | TigerBeetle | **Deterministic testing** -- Implement a deterministic simulation testing framework that replays thread schedules for reproducing concurrency bugs. Record the ordering of morsel dispatches, hash table merges, and async I/O completions, then replay them to reproduce failures. This is especially important because the engine uses `rayon` for data parallelism (hash join build, aggregation) and `tokio` for async I/O, creating complex interleaving. | Reliability improvement. Does not directly improve performance but prevents regression. Particularly important for the parallel hash table build in `src/physical/operators/hash_join.rs` and the morsel work-stealing in `src/physical/morsel.rs` where race conditions can produce silently incorrect results. | Medium |

## Issue #11 Fix Plan (LIMIT Bug)

The LIMIT operator implementation in `src/physical/operators/limit.rs` should be investigated for an off-by-one bug in stream truncation. Here is the investigation and fix plan:

### 1. Check `src/physical/operators/limit.rs` for Off-by-One in Stream Truncation

The current `LimitExec::execute()` implementation (lines 43-108) uses `filter_map` on the input stream with mutable `skipped` and `fetched` counters. The logic has two code paths:

- **Path A (lines 60-86):** When `skipped < skip`, handles partial batch skipping and then applies the fetch limit.
- **Path B (lines 89-98):** When skip is complete, applies the fetch limit to full batches.

Potential off-by-one issues to verify:
- When a batch boundary falls exactly at the skip count (`to_skip == num_rows`), the batch is correctly skipped (line 64-67), but the `fetched` counter is not incremented. Verify this is correct when `skip == 0` and `fetch == Some(0)`.
- After partial skip, the remaining rows are sliced and fetch-limited (lines 74-83). Check that `fetched` is correctly updated when `to_fetch == sliced.num_rows()` -- currently this case falls through to return the full sliced batch without incrementing `fetched` proportionally. **This is correct** because `fetched += to_fetch` on line 79 handles it.
- The stream does not call `fuse()` after the fetch limit is reached, meaning subsequent calls to `poll_next` will still invoke the closure even though `fetched >= limit`. This is harmless (returns `None`) but wastes CPU. Consider wrapping with `.take_while()` for early termination.

### 2. Check Interaction with OFFSET

The `skip` parameter implements SQL `OFFSET`. Test the following edge cases:
- `OFFSET` larger than total rows (should return 0 rows)
- `OFFSET 0 LIMIT 0` (should return 0 rows)
- `OFFSET` equal to total rows with `LIMIT 1` (should return 0 rows)
- Multi-batch input where the skip boundary falls mid-batch

### 3. Add Regression Test Cases

Add the following tests to the existing test module in `limit.rs`:

```rust
#[tokio::test]
async fn test_limit_zero_fetch() {
    // LIMIT 0 should return 0 rows
    let batch = create_test_batch(); // 5 rows
    let schema = batch.schema();
    let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));
    let limit = LimitExec::new(scan, 0, Some(0));
    let stream = limit.execute(0).await.unwrap();
    let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_limit_skip_exceeds_rows() {
    // OFFSET larger than input should return 0 rows
    let batch = create_test_batch(); // 5 rows
    let schema = batch.schema();
    let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));
    let limit = LimitExec::new(scan, 100, Some(5));
    let stream = limit.execute(0).await.unwrap();
    let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);
}

#[tokio::test]
async fn test_limit_multi_batch() {
    // Test LIMIT across multiple input batches
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch1 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    ).unwrap();
    let batch2 = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![4, 5, 6]))],
    ).unwrap();
    let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch1, batch2], None));
    let limit = LimitExec::new(scan, 2, Some(3));
    let stream = limit.execute(0).await.unwrap();
    let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();
    let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 3); // skip 2, fetch 3 from [1,2,3,4,5,6] = [3,4,5]
}
```

### 4. Verify with TPC-H Queries that Use LIMIT

Several TPC-H queries use `LIMIT` or `ORDER BY ... LIMIT`:
- **Q1**: No LIMIT (full aggregation)
- **Q2**: `ORDER BY ... LIMIT 100` -- verify correct number of output rows
- **Q3**: `ORDER BY ... LIMIT 10` -- verify correct row count and ordering
- **Q10**: `ORDER BY ... LIMIT 20`
- **Q18**: `ORDER BY ... LIMIT 100`
- **Q21**: `ORDER BY ... LIMIT 100`

Run these queries and verify that the output row count matches the LIMIT clause, and that the sort order is preserved after the limit is applied.

## Research References

### DuckDB: Morsel-Driven Parallelism and Vectorized Execution
- Leis et al., "Morsel-Driven Parallelism: A NUMA-Aware Query Evaluation Framework for the Many-Core Age" (SIGMOD 2014). Describes the morsel-driven execution model where data is split into fixed-size chunks processed by worker threads with work-stealing for load balancing. This directly inspired our `ParallelParquetSource` in `src/physical/morsel.rs`.
- Raasveldt and Muehleisen, "DuckDB: an Embeddable Analytical Database" (SIGMOD 2019). Covers DuckDB's vectorized execution engine, pipeline-based query processing, and the DelimJoin technique for efficient correlated subquery execution. The DelimJoin pattern is implemented in our `src/physical/operators/delim_join.rs` and `src/optimizer/rules/flatten_dependent_join.rs`.

### Velox/Meta: Lazy Evaluation and Dictionary Encoding
- Pedreira et al., "Velox: Meta's Unified Execution Engine" (VLDB 2022). Describes Velox's lazy vector materialization where columns are decoded on demand, and dictionary encoding is preserved through operator pipelines. Velox maintains `DictionaryVector<T>` through filters and projections, only decoding when necessary (e.g., for output serialization or complex expressions that cannot operate on indices).

### ClickHouse: LowCardinality Encoding
- ClickHouse documentation on LowCardinality data type. ClickHouse automatically detects columns with fewer than 10,000 distinct values and stores them as integer indices into a global dictionary. GROUP BY operations on LowCardinality columns operate on integer keys, reducing hash table size and improving cache performance. The technique is described in the ClickHouse blog post "LowCardinality Data Type" (2019).

### CockroachDB: Histogram-Based Optimizer
- CockroachDB documentation on table statistics and the cost-based optimizer. CockroachDB collects equi-depth histograms during `CREATE STATISTICS` and uses them for selectivity estimation in the optimizer. Histograms capture the distribution of values (not just min/max/NDV), enabling accurate cardinality estimates for range predicates and skewed data distributions. The approach is described in Ioannidis and Christodoulakis, "On the Propagation of Errors in the Size of Join Results" (SIGMOD 1991).

### TigerBeetle: Deterministic Simulation Testing
- TigerBeetle documentation on deterministic simulation testing. TigerBeetle records all sources of non-determinism (thread scheduling, I/O completion order, timer expirations) and replays them to reproduce failures. This approach, inspired by FoundationDB's simulation testing (described in Zhou et al., "FoundationDB: A Distributed Unbundled Transactional Key Value Store", SIGMOD 2021), catches concurrency bugs that are invisible to conventional testing. TigerBeetle also emphasizes pre-allocated buffers and zero-allocation steady-state operation for predictable performance.
