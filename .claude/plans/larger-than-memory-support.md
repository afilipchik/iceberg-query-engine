# Plan: Supporting Larger-Than-Memory Datasets

## Status: PARTIAL ⚠️

### Phase Status

| Phase | Status | Notes |
|-------|--------|-------|
| Phase 1: Memory Accounting | ✅ DONE | MemoryPool, ExecutionConfig, MemoryConsumer trait |
| Phase 2: Streaming Reads | ✅ DONE | StreamingParquetReader, AsyncParquetReader |
| Phase 3: Spillable Hash Join | ⚠️ HAS BUGS | Schema issues - disabled by default |
| Phase 4: Spillable Hash Aggregate | ⚠️ HAS BUGS | Parquet I/O bugs - disabled by default |
| Phase 5: External Sort | ⚠️ HAS BUGS | Schema issues - disabled by default |
| Phase 6: Sort-Merge Join | ❌ NOT STARTED | Alternative to hash join |
| Phase 7: Iceberg Optimizations | ❌ NOT STARTED | Statistics pruning, delete files |

### Current Workaround

Memory limits in HashJoinExec (50M rows / 4GB) prevent OOM. Spilling disabled by default.

---

## Executive Summary

This plan enables the query engine to process datasets that exceed available RAM by implementing:
1. **Memory-aware execution** using the existing `MemoryPool` infrastructure
2. **Disk-based spilling** for operators that exceed memory limits
3. **Streaming data access** leveraging Iceberg's metadata for intelligent data skipping
4. **Iceberg-native optimizations** using partition pruning, column statistics, and file-level filtering

## Current State Analysis

### What's Done ✅
- `MemoryPool` with `try_allocate()` / RAII reservations
- `MemoryConsumer` trait for spillable operators
- `ExecutionConfig` with memory limits and spill paths
- `StreamingParquetReader` for row-group streaming
- `AsyncParquetReader` for async I/O
- Spillable operator implementations (have bugs)

### What's Missing ❌
- Fix spillable operator bugs (schema, Parquet I/O)
- Iceberg statistics-based file pruning
- Sort-merge join alternative to hash join
- Integration tests with memory limits

---

## Phase 1: Memory Accounting Foundation

**Goal**: Track memory usage across all operators to enable spilling decisions.

### 1.1 Integrate MemoryPool into ExecutionContext

```rust
// src/execution/context.rs
pub struct ExecutionContext {
    tables: HashMap<String, Arc<dyn TableProvider>>,
    memory_pool: SharedMemoryPool,  // ADD THIS
}

impl ExecutionContext {
    pub fn with_memory_limit(max_bytes: usize) -> Self {
        Self {
            tables: HashMap::new(),
            memory_pool: create_memory_pool(max_bytes),
        }
    }
}
```

### 1.2 Add MemoryConsumer Trait

```rust
// src/execution/memory.rs
pub trait MemoryConsumer: Send + Sync {
    /// Name for debugging/metrics
    fn name(&self) -> &str;

    /// Current memory usage
    fn mem_used(&self) -> usize;

    /// Try to free memory by spilling; returns bytes freed
    fn spill(&self, target_bytes: usize) -> Result<usize>;
}
```

### 1.3 Pass MemoryPool to Physical Operators

Update `PhysicalOperator` trait:

```rust
#[async_trait]
pub trait PhysicalOperator: Send + Sync {
    // ... existing methods ...

    /// Execute with memory tracking
    async fn execute_with_memory(
        &self,
        partition: usize,
        memory_pool: &SharedMemoryPool,
    ) -> Result<RecordBatchStream>;
}
```

---

## Phase 2: Streaming Iceberg/Parquet Reads

**Goal**: Read data incrementally instead of loading entire tables.

### 2.1 Row-Group Level Streaming for Parquet

Replace current `ParquetTable::scan()` which loads all files at once:

```rust
// src/storage/parquet.rs
pub struct StreamingParquetReader {
    files: Vec<PathBuf>,
    projection: Option<Vec<usize>>,
    predicate: Option<Expr>,
    current_file_idx: usize,
    current_row_group_idx: usize,
    row_group_reader: Option<ArrowReaderBuilder<File>>,
}

impl Stream for StreamingParquetReader {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Read one row group at a time
        // Advance to next file when current exhausted
    }
}
```

### 2.2 Complete Iceberg Data File Reading

The current `IcebergScanExec::execute()` returns empty. Complete it:

```rust
// src/physical/operators/iceberg.rs
async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
    let metadata = self.load_metadata()?;
    let snapshot = self.load_snapshot(snapshot_id)?;
    let data_files = self.collect_data_files(&snapshot)?;

    // PHASE 2.2: Apply partition pruning
    let pruned_files = self.filter_data_files(data_files);

    // PHASE 2.3: Apply statistics-based pruning (see below)
    let stats_pruned = self.apply_stats_filter(pruned_files, &self.predicate)?;

    // Stream Parquet files
    let reader = StreamingIcebergReader::new(
        stats_pruned,
        self.schema.clone(),
        self.projection.clone(),
    );

    Ok(Box::pin(reader))
}
```

### 2.3 Iceberg Statistics-Based File Pruning

Leverage `lower_bounds` and `upper_bounds` in `IcebergDataFile`:

```rust
// src/physical/operators/iceberg.rs
impl IcebergScanExec {
    fn apply_stats_filter(
        &self,
        files: Vec<IcebergDataFile>,
        predicate: &Option<Expr>,
    ) -> Result<Vec<IcebergDataFile>> {
        let Some(pred) = predicate else {
            return Ok(files);
        };

        files.into_iter().filter(|file| {
            // Check if predicate could possibly match this file
            // based on column min/max statistics
            self.file_might_match(file, pred)
        }).collect()
    }

    fn file_might_match(&self, file: &IcebergDataFile, pred: &Expr) -> bool {
        match pred {
            Expr::BinaryExpr { left, op, right } => {
                // For col > value: check if file's max >= value
                // For col < value: check if file's min <= value
                // For col = value: check if min <= value <= max
                self.check_bounds(file, left, op, right)
            }
            Expr::And(exprs) => exprs.iter().all(|e| self.file_might_match(file, e)),
            Expr::Or(exprs) => exprs.iter().any(|e| self.file_might_match(file, e)),
            _ => true, // Conservative: include file if unsure
        }
    }
}
```

### 2.4 Iceberg Partition Pruning Enhancement

Current partition filter is basic. Enhance to extract from SQL predicates:

```rust
// src/optimizer/rules/iceberg_pushdown.rs (new file)
pub struct IcebergPartitionPushdown;

impl OptimizerRule for IcebergPartitionPushdown {
    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // Extract partition column predicates from Filter nodes
        // Push them to IcebergScan's partition_filters
    }
}
```

---

## Phase 3: Spillable Hash Join

**Goal**: Allow hash joins to spill partitions to disk when memory is exhausted.

### 3.1 Partitioned Build Phase

Instead of one giant hash table, partition data by hash:

```rust
// src/physical/operators/hash_join.rs
const NUM_PARTITIONS: usize = 64;

struct SpillableHashJoin {
    memory_pool: SharedMemoryPool,
    spill_path: PathBuf,

    // In-memory partitions
    partitions: Vec<Option<HashPartition>>,

    // Spilled partition files
    spilled: Vec<Option<SpilledPartition>>,
}

struct HashPartition {
    build_batches: Vec<RecordBatch>,
    hash_table: HashMap<JoinKey, Vec<HashEntry>>,
    memory_reservation: MemoryReservation,
}

struct SpilledPartition {
    build_file: PathBuf,
    probe_file: PathBuf,
}
```

### 3.2 Memory-Aware Build Phase

```rust
impl SpillableHashJoin {
    async fn build_phase(&mut self, build_stream: RecordBatchStream) -> Result<()> {
        while let Some(batch) = build_stream.try_next().await? {
            let batch_size = batch.get_array_memory_size();

            // Try to allocate memory
            match self.memory_pool.try_allocate(batch_size) {
                Some(reservation) => {
                    // Partition and insert into in-memory hash tables
                    self.insert_batch(batch, reservation)?;
                }
                None => {
                    // Memory pressure: spill largest partition
                    self.spill_largest_partition()?;

                    // Retry allocation
                    let reservation = self.memory_pool.allocate(batch_size);
                    self.insert_batch(batch, reservation)?;
                }
            }
        }
        Ok(())
    }

    fn spill_largest_partition(&mut self) -> Result<()> {
        // Find partition with most memory
        let (idx, partition) = self.partitions
            .iter_mut()
            .enumerate()
            .filter_map(|(i, p)| p.as_ref().map(|p| (i, p)))
            .max_by_key(|(_, p)| p.memory_reservation.size())
            .ok_or(QueryError::Execution("No partition to spill".into()))?;

        // Write to disk
        let path = self.spill_path.join(format!("build_{}.parquet", idx));
        write_batches_to_parquet(&path, &partition.build_batches)?;

        // Release memory
        let spilled = SpilledPartition {
            build_file: path,
            probe_file: PathBuf::new(), // Set during probe phase
        };

        self.spilled[idx] = Some(spilled);
        self.partitions[idx] = None;

        Ok(())
    }
}
```

### 3.3 Probe Phase with Spill Handling

```rust
impl SpillableHashJoin {
    async fn probe_phase(&mut self, probe_stream: RecordBatchStream) -> Result<Vec<RecordBatch>> {
        let mut results = Vec::new();

        while let Some(batch) = probe_stream.try_next().await? {
            // Partition probe batch
            for (partition_id, partition_batch) in self.partition_batch(&batch)? {
                if let Some(ref hash_partition) = self.partitions[partition_id] {
                    // Probe in-memory partition
                    let matched = probe_hash_table(
                        &hash_partition.build_batches,
                        &[partition_batch],
                        &hash_partition.hash_table,
                        // ...
                    )?;
                    results.extend(matched);
                } else if let Some(ref mut spilled) = self.spilled[partition_id] {
                    // Append to spilled probe file for later processing
                    append_to_parquet(&spilled.probe_file, &partition_batch)?;
                }
            }
        }

        // Process spilled partitions
        for spilled in self.spilled.iter().flatten() {
            let spilled_results = self.process_spilled_partition(spilled)?;
            results.extend(spilled_results);
        }

        Ok(results)
    }

    fn process_spilled_partition(&self, spilled: &SpilledPartition) -> Result<Vec<RecordBatch>> {
        // Read spilled build side, rebuild hash table
        let build_batches = read_parquet(&spilled.build_file)?;
        let hash_table = build_hash_table(&build_batches, &self.build_keys)?;

        // Read spilled probe side and probe
        let probe_batches = read_parquet(&spilled.probe_file)?;
        probe_hash_table(&build_batches, &probe_batches, &hash_table, ...)
    }
}
```

---

## Phase 4: Spillable Hash Aggregate

**Goal**: Spill aggregate hash table partitions when memory is exhausted.

### 4.1 Partitioned Aggregation

```rust
// src/physical/operators/hash_agg.rs
struct SpillableHashAggregate {
    memory_pool: SharedMemoryPool,
    spill_path: PathBuf,

    // Partitioned accumulators
    partitions: Vec<Option<AggPartition>>,
    spilled_partitions: Vec<Option<PathBuf>>,
}

struct AggPartition {
    groups: HashMap<GroupKey, Vec<AccumulatorState>>,
    memory_reservation: MemoryReservation,
}
```

### 4.2 Spilling Strategy

```rust
impl SpillableHashAggregate {
    fn update(&mut self, batch: &RecordBatch) -> Result<()> {
        for row in 0..batch.num_rows() {
            let key = extract_group_key(&group_arrays, row);
            let partition_id = key.hash() % NUM_PARTITIONS;

            // Check memory before inserting new group
            if !self.partitions[partition_id].as_ref()
                .map(|p| p.groups.contains_key(&key))
                .unwrap_or(false)
            {
                let new_group_size = estimate_accumulator_size(&self.aggregates);
                if self.memory_pool.try_allocate(new_group_size).is_none() {
                    self.spill_largest_partition()?;
                }
            }

            // Update accumulator
            self.update_group(partition_id, key, &agg_inputs, row)?;
        }
        Ok(())
    }

    fn spill_largest_partition(&mut self) -> Result<()> {
        // Serialize partition to Parquet
        // Use partial aggregates format:
        // [group_key_cols..., partial_state_cols...]
    }

    fn finalize(&mut self) -> Result<RecordBatch> {
        // Finalize in-memory partitions
        let mut results = self.finalize_in_memory()?;

        // Re-aggregate spilled partitions
        for spilled_path in self.spilled_partitions.iter().flatten() {
            let partial = read_parquet(spilled_path)?;
            let merged = self.merge_partial_aggregates(partial)?;
            results.extend(merged);
        }

        Ok(results)
    }
}
```

---

## Phase 5: External Sort

**Goal**: Sort datasets larger than memory using merge sort with disk.

### 5.1 Run Generation

```rust
// src/physical/operators/sort.rs
struct ExternalSort {
    memory_pool: SharedMemoryPool,
    spill_path: PathBuf,
    order_by: Vec<SortExpr>,

    // Current in-memory buffer
    buffer: Vec<RecordBatch>,
    buffer_memory: MemoryReservation,

    // Sorted runs on disk
    runs: Vec<PathBuf>,
}

impl ExternalSort {
    async fn sort_stream(&mut self, input: RecordBatchStream) -> Result<RecordBatchStream> {
        // Phase 1: Generate sorted runs
        while let Some(batch) = input.try_next().await? {
            let batch_size = batch.get_array_memory_size();

            match self.memory_pool.try_allocate(batch_size) {
                Some(reservation) => {
                    self.buffer_memory.resize(self.buffer_memory.size() + batch_size);
                    self.buffer.push(batch);
                }
                None => {
                    // Memory full: sort buffer and write run
                    self.flush_to_run()?;

                    // Reset buffer
                    self.buffer.clear();
                    self.buffer_memory.resize(0);

                    self.buffer.push(batch);
                }
            }
        }

        // Flush remaining buffer
        if !self.buffer.is_empty() {
            self.flush_to_run()?;
        }

        // Phase 2: Merge sorted runs
        self.merge_runs()
    }

    fn flush_to_run(&mut self) -> Result<()> {
        // Concatenate and sort in-memory
        let combined = concat_batches(&self.schema, &self.buffer)?;
        let sorted = sort_batch(&combined, &self.order_by)?;

        // Write to disk
        let run_path = self.spill_path.join(format!("run_{}.parquet", self.runs.len()));
        write_batches_to_parquet(&run_path, &[sorted])?;
        self.runs.push(run_path);

        Ok(())
    }
}
```

### 5.2 K-Way Merge

```rust
impl ExternalSort {
    fn merge_runs(&self) -> Result<RecordBatchStream> {
        if self.runs.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        if self.runs.len() == 1 {
            // Single run: stream directly
            return Ok(Box::pin(StreamingParquetReader::new(&self.runs[0])));
        }

        // K-way merge
        let merger = KWayMerger::new(
            self.runs.iter().map(|p| StreamingParquetReader::new(p)).collect(),
            self.order_by.clone(),
        );

        Ok(Box::pin(merger))
    }
}

struct KWayMerger {
    readers: Vec<StreamingParquetReader>,
    heap: BinaryHeap<HeapEntry>,
    order_by: Vec<SortExpr>,
}

impl Stream for KWayMerger {
    type Item = Result<RecordBatch>;

    fn poll_next(...) -> Poll<Option<Self::Item>> {
        // Use min-heap to merge sorted streams
        // Output in sorted order
    }
}
```

---

## Phase 6: Sort-Merge Join (Alternative to Hash Join)

**Goal**: Provide join algorithm that works well for pre-sorted data.

### 6.1 Sort-Merge Join Operator

```rust
// src/physical/operators/merge_join.rs
pub struct SortMergeJoinExec {
    left: Arc<dyn PhysicalOperator>,
    right: Arc<dyn PhysicalOperator>,
    on: Vec<(Expr, Expr)>,
    join_type: JoinType,
    schema: SchemaRef,
}

impl SortMergeJoinExec {
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        // Both inputs must be sorted on join keys
        let left_sorted = SortExec::new(self.left.clone(), self.left_sort_exprs());
        let right_sorted = SortExec::new(self.right.clone(), self.right_sort_exprs());

        let left_stream = left_sorted.execute(partition).await?;
        let right_stream = right_sorted.execute(partition).await?;

        // Stream-based merge join
        let merger = MergeJoinStream::new(
            left_stream,
            right_stream,
            self.on.clone(),
            self.join_type,
            self.schema.clone(),
        );

        Ok(Box::pin(merger))
    }
}
```

### 6.2 When to Use Sort-Merge vs Hash Join

Add to physical planner:

```rust
// src/physical/planner.rs
fn plan_join(&self, join: &LogicalJoin) -> Result<Arc<dyn PhysicalOperator>> {
    let left_size = estimate_size(&join.left);
    let right_size = estimate_size(&join.right);
    let available_memory = self.memory_pool.available();

    // Use sort-merge if:
    // 1. Inputs are already sorted on join keys, OR
    // 2. Neither side fits in memory
    let left_sorted = is_sorted_on(&join.left, &join.on);
    let right_sorted = is_sorted_on(&join.right, &join.on);

    if (left_sorted && right_sorted) ||
       (left_size > available_memory && right_size > available_memory) {
        Ok(Arc::new(SortMergeJoinExec::new(...)))
    } else {
        Ok(Arc::new(SpillableHashJoinExec::new(...)))
    }
}
```

---

## Phase 7: Iceberg-Specific Optimizations

**Goal**: Leverage Iceberg's unique features for query optimization.

### 7.1 Delete File Handling

Iceberg supports row-level deletes via delete files:

```rust
// src/physical/operators/iceberg.rs
struct IcebergDeleteHandler {
    position_deletes: HashMap<String, RoaringBitmap>,  // file -> deleted rows
    equality_deletes: Vec<EqualityDeleteFile>,
}

impl IcebergScanExec {
    fn apply_deletes(&self, batch: RecordBatch, file_path: &str) -> Result<RecordBatch> {
        // Apply position deletes (bitmap filter)
        let batch = self.delete_handler.apply_position_deletes(batch, file_path)?;

        // Apply equality deletes (anti-join)
        let batch = self.delete_handler.apply_equality_deletes(batch)?;

        Ok(batch)
    }
}
```

### 7.2 Sorted Iceberg Tables

Iceberg tables can declare sort order. Leverage for merge joins:

```rust
// src/physical/operators/iceberg.rs
impl IcebergTable {
    pub fn sort_order(&self) -> Option<Vec<SortExpr>> {
        // Parse from metadata.json sort-order field
    }

    pub fn is_sorted_on(&self, columns: &[&str]) -> bool {
        // Check if table's sort order is compatible
    }
}

// In physical planner
fn plan_iceberg_join(&self, left: IcebergTable, right: IcebergTable, on: &[(Expr, Expr)]) {
    let join_cols: Vec<&str> = on.iter().map(|(l, _)| l.column_name()).collect();

    if left.is_sorted_on(&join_cols) && right.is_sorted_on(&join_cols) {
        // Use merge join without additional sorting
        SortMergeJoinExec::new_presorted(...)
    }
}
```

### 7.3 Time Travel Queries

```rust
// src/physical/operators/iceberg.rs
impl IcebergScanExec {
    /// Read from a specific snapshot
    pub fn at_snapshot(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Read as of a timestamp
    pub fn as_of(mut self, timestamp_ms: i64) -> Result<Self> {
        let snapshot_id = self.find_snapshot_at(timestamp_ms)?;
        self.snapshot_id = Some(snapshot_id);
        Ok(self)
    }
}
```

---

## Implementation Priority & Dependencies

```
Phase 1 (Foundation) ──────────────────────────┐
│ Memory accounting                            │
└──────────────────────────────────────────────┘
            │
            ▼
Phase 2 (Streaming Reads) ─────────────────────┐
│ Streaming Parquet                            │
│ Complete Iceberg scan                        │
│ Statistics pruning                           │
└──────────────────────────────────────────────┘
            │
            ├───────────────┬───────────────┐
            ▼               ▼               ▼
Phase 3              Phase 4          Phase 5
Spillable Join       Spillable Agg    External Sort
            │               │               │
            └───────────────┴───────────────┘
                            │
                            ▼
Phase 6 ───────────────────────────────────────┐
│ Sort-Merge Join                              │
└──────────────────────────────────────────────┘
                            │
                            ▼
Phase 7 ───────────────────────────────────────┐
│ Iceberg optimizations                        │
│ (can be done incrementally)                  │
└──────────────────────────────────────────────┘
```

## Testing Strategy

### Unit Tests
- Memory pool allocation/spilling triggers
- Partition functions for join/aggregate
- K-way merge correctness
- Iceberg statistics filter logic

### Integration Tests
- Run TPC-H with 10MB memory limit on 100MB data
- Verify results match in-memory execution
- Measure performance degradation with spilling

### Stress Tests
```bash
# Generate 1GB TPC-H data
query_engine generate-parquet --sf 1.0 --output ./data/tpch-1gb

# Run with 256MB memory limit
query_engine benchmark-parquet \
    --path ./data/tpch-1gb \
    --memory-limit 256mb \
    --spill-dir /tmp/query-spill
```

## Configuration Options

```rust
pub struct ExecutionConfig {
    /// Maximum memory for query execution
    pub memory_limit: usize,

    /// Directory for spill files
    pub spill_path: PathBuf,

    /// Number of partitions for spilling operators
    pub spill_partitions: usize,

    /// Batch size for streaming reads
    pub batch_size: usize,

    /// Whether to use sort-merge join when beneficial
    pub enable_sort_merge_join: bool,

    /// Enable Iceberg statistics pruning
    pub enable_stats_pruning: bool,
}
```

## Metrics & Observability

Add metrics to track spilling behavior:

```rust
pub struct SpillMetrics {
    pub partitions_spilled: usize,
    pub bytes_spilled: usize,
    pub spill_time_ms: u64,
    pub merge_time_ms: u64,
}

pub struct QueryMetrics {
    // ... existing fields ...
    pub peak_memory_bytes: usize,
    pub spill_metrics: Option<SpillMetrics>,
    pub files_pruned_by_stats: usize,
    pub files_pruned_by_partition: usize,
}
```

## Summary

This plan transforms the query engine from an in-memory-only system to one capable of processing arbitrarily large datasets by:

1. **Tracking memory** using the existing `MemoryPool`
2. **Streaming data** from Iceberg/Parquet instead of loading all at once
3. **Spilling to disk** when operators exceed memory limits
4. **Leveraging Iceberg metadata** to skip irrelevant files entirely

The Iceberg-specific optimizations (partition pruning, statistics-based filtering, sort order awareness) can reduce the amount of data that needs to be read in the first place, making spilling less necessary for well-partitioned tables.
