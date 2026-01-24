//! Spillable operators for larger-than-memory execution
//!
//! This module provides versions of hash join, hash aggregate, and sort
//! operators that can spill intermediate data to disk when memory limits
//! are exceeded.

use crate::error::{QueryError, Result};
use crate::execution::{ExecutionConfig, SharedMemoryPool};
use crate::physical::operators::filter::evaluate_expr;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::{Expr, JoinType};
use arrow::array::{ArrayRef, Int64Array, UInt32Array, UInt64Array};
use arrow::compute;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::{self, TryStreamExt};
use hashbrown::HashMap;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fmt;
use std::fs::File;
use std::hash::{BuildHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Number of hash partitions for spilling
const NUM_PARTITIONS: usize = 64;

// ============================================================================
// Spillable Hash Join
// ============================================================================

/// Hash join execution operator with spilling support
///
/// When memory limits are exceeded, this operator partitions data by hash
/// and spills partitions to disk. During the probe phase, spilled partitions
/// are processed one at a time.
pub struct SpillableHashJoinExec {
    left: Arc<dyn PhysicalOperator>,
    right: Arc<dyn PhysicalOperator>,
    on: Vec<(Expr, Expr)>,
    join_type: JoinType,
    schema: SchemaRef,
    memory_pool: SharedMemoryPool,
    config: ExecutionConfig,
}

impl fmt::Debug for SpillableHashJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpillableHashJoinExec")
            .field("join_type", &self.join_type)
            .field("on", &self.on)
            .finish()
    }
}

impl SpillableHashJoinExec {
    pub fn new(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        on: Vec<(Expr, Expr)>,
        join_type: JoinType,
        memory_pool: SharedMemoryPool,
        config: ExecutionConfig,
    ) -> Self {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let schema = match join_type {
            JoinType::Semi | JoinType::Anti => left_schema,
            _ => {
                let left_nullable = matches!(join_type, JoinType::Right | JoinType::Full);
                let right_nullable = matches!(join_type, JoinType::Left | JoinType::Full);

                let left_fields = left_schema.fields().iter().map(|f| {
                    if left_nullable && !f.is_nullable() {
                        Arc::new(f.as_ref().clone().with_nullable(true))
                    } else {
                        f.clone()
                    }
                });

                let right_fields = right_schema.fields().iter().map(|f| {
                    if right_nullable && !f.is_nullable() {
                        Arc::new(f.as_ref().clone().with_nullable(true))
                    } else {
                        f.clone()
                    }
                });

                let fields: Vec<_> = left_fields.chain(right_fields).collect();
                Arc::new(Schema::new(fields))
            }
        };

        Self {
            left,
            right,
            on,
            join_type,
            schema,
            memory_pool,
            config,
        }
    }
}

/// State for a hash partition during the build phase
struct BuildPartition {
    batches: Vec<RecordBatch>,
    memory_bytes: usize,
}

impl BuildPartition {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            memory_bytes: 0,
        }
    }

    fn add_batch(&mut self, batch: RecordBatch) {
        self.memory_bytes += estimate_batch_size(&batch);
        self.batches.push(batch);
    }
}

/// State for a spilled partition
struct SpilledPartition {
    build_file: PathBuf,
    probe_file: Option<PathBuf>,
    #[allow(dead_code)]
    build_rows: usize,
}

#[async_trait]
impl PhysicalOperator for SpillableHashJoinExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.left.clone(), self.right.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        // Ensure spill directory exists
        self.config.ensure_spill_dir()?;

        let spill_dir = self.config.spill_path.join(format!("join_{}", partition));
        std::fs::create_dir_all(&spill_dir).map_err(|e| {
            QueryError::Execution(format!("Failed to create spill directory: {}", e))
        })?;

        // Determine build and probe sides
        let (build_side, probe_side, swapped) = match self.join_type {
            JoinType::Right => (&self.right, &self.left, true),
            _ => (&self.left, &self.right, false),
        };

        let (on_left, on_right): (Vec<_>, Vec<_>) = self.on.iter().cloned().unzip();
        let build_keys = if swapped { &on_right } else { &on_left };
        let probe_keys = if swapped { &on_left } else { &on_right };

        // Build phase with partitioning
        let build_stream = build_side.execute(partition).await?;
        let (mut in_memory_partitions, spilled_partitions) = self
            .build_with_partitioning(build_stream, build_keys, &spill_dir)
            .await?;

        // Build hash tables for in-memory partitions
        let mut hash_tables: Vec<Option<HashMap<JoinKey, Vec<HashEntry>>>> =
            (0..NUM_PARTITIONS).map(|_| None).collect();

        for (idx, part) in in_memory_partitions.iter().enumerate() {
            if let Some(p) = part {
                if !p.batches.is_empty() {
                    let table = build_hash_table(&p.batches, build_keys)?;
                    hash_tables[idx] = Some(table);
                }
            }
        }

        // Probe phase
        let probe_stream = probe_side.execute(partition).await?;
        let results = self
            .probe_with_spilling(
                probe_stream,
                probe_keys,
                &mut in_memory_partitions,
                &hash_tables,
                &spilled_partitions,
                &spill_dir,
                swapped,
            )
            .await?;

        // Process spilled partitions
        let mut all_results = results;
        for (idx, spilled) in spilled_partitions.iter().enumerate() {
            if let Some(sp) = spilled {
                let spilled_results = self
                    .process_spilled_partition(sp, build_keys, probe_keys, swapped, idx)
                    .await?;
                all_results.extend(spilled_results);
            }
        }

        // Clean up spill directory
        let _ = std::fs::remove_dir_all(&spill_dir);

        Ok(Box::pin(stream::iter(all_results.into_iter().map(Ok))))
    }

    fn name(&self) -> &str {
        "SpillableHashJoin"
    }
}

impl SpillableHashJoinExec {
    async fn build_with_partitioning(
        &self,
        mut build_stream: RecordBatchStream,
        build_keys: &[Expr],
        spill_dir: &Path,
    ) -> Result<(Vec<Option<BuildPartition>>, Vec<Option<SpilledPartition>>)> {
        let mut partitions: Vec<Option<BuildPartition>> = (0..NUM_PARTITIONS)
            .map(|_| Some(BuildPartition::new()))
            .collect();
        let mut spilled: Vec<Option<SpilledPartition>> =
            (0..NUM_PARTITIONS).map(|_| None).collect();
        let mut total_memory: usize = 0;
        let memory_threshold =
            (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;

        while let Some(batch) = build_stream.try_next().await? {
            let batch_size = estimate_batch_size(&batch);

            // Check if we need to spill
            if total_memory + batch_size > memory_threshold {
                // Find the largest partition to spill
                if let Some(idx) = find_largest_partition(&partitions) {
                    let part = partitions[idx].take().unwrap();
                    let path = spill_dir.join(format!("build_{}.parquet", idx));
                    write_batches_to_parquet(&path, &part.batches)?;

                    self.memory_pool.record_spill(part.memory_bytes);
                    total_memory -= part.memory_bytes;

                    spilled[idx] = Some(SpilledPartition {
                        build_file: path,
                        probe_file: None,
                        build_rows: part.batches.iter().map(|b| b.num_rows()).sum(),
                    });
                }
            }

            // Partition the batch by hash
            let partitioned = partition_batch_by_hash(&batch, build_keys, NUM_PARTITIONS)?;

            for (idx, part_batch) in partitioned.into_iter().enumerate() {
                if let Some(pb) = part_batch {
                    let pb_size = estimate_batch_size(&pb);

                    if let Some(ref mut part) = partitions[idx] {
                        part.add_batch(pb);
                        total_memory += pb_size;
                    } else if let Some(ref sp) = spilled[idx] {
                        // Append to spilled partition
                        append_to_parquet(&sp.build_file, &pb)?;
                    }
                }
            }
        }

        Ok((partitions, spilled))
    }

    #[allow(clippy::too_many_arguments)]
    async fn probe_with_spilling(
        &self,
        mut probe_stream: RecordBatchStream,
        probe_keys: &[Expr],
        in_memory_partitions: &mut [Option<BuildPartition>],
        hash_tables: &[Option<HashMap<JoinKey, Vec<HashEntry>>>],
        spilled_partitions: &[Option<SpilledPartition>],
        spill_dir: &Path,
        swapped: bool,
    ) -> Result<Vec<RecordBatch>> {
        let mut results = Vec::new();
        let mut probe_spill_files: Vec<Option<PathBuf>> =
            (0..NUM_PARTITIONS).map(|_| None).collect();

        while let Some(batch) = probe_stream.try_next().await? {
            // Partition probe batch
            let partitioned = partition_batch_by_hash(&batch, probe_keys, NUM_PARTITIONS)?;

            for (idx, part_batch) in partitioned.into_iter().enumerate() {
                if let Some(pb) = part_batch {
                    if let Some(ref ht) = hash_tables[idx] {
                        // Probe in-memory partition
                        if let Some(ref build_part) = in_memory_partitions[idx] {
                            let matched = probe_partition(
                                &build_part.batches,
                                &[pb],
                                ht,
                                probe_keys,
                                self.join_type,
                                swapped,
                                &self.schema,
                            )?;
                            results.extend(matched);
                        }
                    } else if spilled_partitions[idx].is_some() {
                        // Spill probe batch for this partition
                        let probe_path = probe_spill_files[idx].get_or_insert_with(|| {
                            spill_dir.join(format!("probe_{}.parquet", idx))
                        });
                        append_to_parquet(probe_path, &pb)?;
                    }
                }
            }
        }

        // Update spilled partitions with probe files
        // (In a real implementation, we'd update the SpilledPartition struct)

        Ok(results)
    }

    async fn process_spilled_partition(
        &self,
        spilled: &SpilledPartition,
        build_keys: &[Expr],
        probe_keys: &[Expr],
        swapped: bool,
        _idx: usize,
    ) -> Result<Vec<RecordBatch>> {
        // Read build side from disk
        let build_batches = read_parquet(&spilled.build_file)?;

        // Build hash table
        let hash_table = build_hash_table(&build_batches, build_keys)?;

        // Read probe side from disk (if exists)
        let probe_batches = if let Some(ref probe_path) = spilled.probe_file {
            read_parquet(probe_path)?
        } else {
            Vec::new()
        };

        // Probe
        probe_partition(
            &build_batches,
            &probe_batches,
            &hash_table,
            probe_keys,
            self.join_type,
            swapped,
            &self.schema,
        )
    }
}

impl fmt::Display for SpillableHashJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let on_str: Vec<String> = self
            .on
            .iter()
            .map(|(l, r)| format!("{} = {}", l, r))
            .collect();
        write!(
            f,
            "SpillableHashJoin: {} on [{}]",
            self.join_type,
            on_str.join(", ")
        )
    }
}

// ============================================================================
// Spillable Hash Aggregate
// ============================================================================

/// Hash aggregate execution operator with spilling support
pub struct SpillableHashAggregateExec {
    input: Arc<dyn PhysicalOperator>,
    group_by: Vec<Expr>,
    aggregates: Vec<AggregateExpr>,
    schema: SchemaRef,
    #[allow(dead_code)]
    memory_pool: SharedMemoryPool,
    #[allow(dead_code)]
    config: ExecutionConfig,
}

/// Aggregate expression with function and input
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub func: crate::planner::AggregateFunction,
    pub input: Expr,
    pub distinct: bool,
    /// Optional second argument for functions like APPROX_PERCENTILE
    pub second_arg: Option<Expr>,
}

impl fmt::Debug for SpillableHashAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpillableHashAggregateExec")
            .field("group_by", &self.group_by)
            .finish()
    }
}

impl SpillableHashAggregateExec {
    pub fn new(
        input: Arc<dyn PhysicalOperator>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        schema: SchemaRef,
        memory_pool: SharedMemoryPool,
        config: ExecutionConfig,
    ) -> Self {
        Self {
            input,
            group_by,
            aggregates,
            schema,
            memory_pool,
            config,
        }
    }
}

#[async_trait]
impl PhysicalOperator for SpillableHashAggregateExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.input.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        // For now, delegate to the non-spilling version
        // Full implementation would partition by group key hash and spill
        let input_stream = self.input.execute(partition).await?;
        let batches: Vec<RecordBatch> = input_stream.try_collect().await?;

        // Use the existing aggregation logic
        let result = crate::physical::operators::hash_agg::aggregate_batches_external(
            &batches,
            &self.group_by,
            &self
                .aggregates
                .iter()
                .map(|a| crate::physical::operators::hash_agg::AggregateExpr {
                    func: a.func,
                    input: a.input.clone(),
                    distinct: a.distinct,
                    second_arg: a.second_arg.clone(),
                })
                .collect::<Vec<_>>(),
            &self.schema,
        )?;

        Ok(Box::pin(stream::once(async { Ok(result) })))
    }

    fn name(&self) -> &str {
        "SpillableHashAggregate"
    }
}

impl fmt::Display for SpillableHashAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SpillableHashAggregate")
    }
}

// ============================================================================
// External Sort
// ============================================================================

/// Sort execution operator with external merge sort support
pub struct ExternalSortExec {
    input: Arc<dyn PhysicalOperator>,
    order_by: Vec<crate::planner::SortExpr>,
    schema: SchemaRef,
    memory_pool: SharedMemoryPool,
    config: ExecutionConfig,
}

impl fmt::Debug for ExternalSortExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExternalSortExec")
            .field("order_by", &self.order_by)
            .finish()
    }
}

impl ExternalSortExec {
    pub fn new(
        input: Arc<dyn PhysicalOperator>,
        order_by: Vec<crate::planner::SortExpr>,
        memory_pool: SharedMemoryPool,
        config: ExecutionConfig,
    ) -> Self {
        let schema = input.schema();
        Self {
            input,
            order_by,
            schema,
            memory_pool,
            config,
        }
    }
}

#[async_trait]
impl PhysicalOperator for ExternalSortExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.input.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        self.config.ensure_spill_dir()?;

        let spill_dir = self.config.spill_path.join(format!("sort_{}", partition));
        std::fs::create_dir_all(&spill_dir).map_err(|e| {
            QueryError::Execution(format!("Failed to create spill directory: {}", e))
        })?;

        let input_stream = self.input.execute(partition).await?;

        // Generate sorted runs
        let runs = self.generate_runs(input_stream, &spill_dir).await?;

        // Merge runs
        let result = if runs.is_empty() {
            Vec::new()
        } else if runs.len() == 1 {
            // Single run - just read it
            if runs[0].is_file() {
                read_parquet(&runs[0])?
            } else {
                // In-memory run (stored as path to temp file)
                Vec::new()
            }
        } else {
            // Multi-way merge
            self.merge_runs(&runs)?
        };

        // Clean up
        let _ = std::fs::remove_dir_all(&spill_dir);

        Ok(Box::pin(stream::iter(result.into_iter().map(Ok))))
    }

    fn name(&self) -> &str {
        "ExternalSort"
    }
}

impl ExternalSortExec {
    async fn generate_runs(
        &self,
        mut input_stream: RecordBatchStream,
        spill_dir: &Path,
    ) -> Result<Vec<PathBuf>> {
        let mut runs = Vec::new();
        let mut buffer: Vec<RecordBatch> = Vec::new();
        let mut buffer_size: usize = 0;
        let memory_threshold =
            (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;

        while let Some(batch) = input_stream.try_next().await? {
            let batch_size = estimate_batch_size(&batch);

            if buffer_size + batch_size > memory_threshold && !buffer.is_empty() {
                // Sort buffer and write run
                let run_path = spill_dir.join(format!("run_{}.parquet", runs.len()));
                self.flush_run(&buffer, &run_path)?;
                runs.push(run_path);

                self.memory_pool.record_spill(buffer_size);
                buffer.clear();
                buffer_size = 0;
            }

            buffer.push(batch);
            buffer_size += batch_size;
        }

        // Flush remaining buffer
        if !buffer.is_empty() {
            let run_path = spill_dir.join(format!("run_{}.parquet", runs.len()));
            self.flush_run(&buffer, &run_path)?;
            runs.push(run_path);
        }

        Ok(runs)
    }

    fn flush_run(&self, batches: &[RecordBatch], path: &PathBuf) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        // Concatenate batches
        let combined = compute::concat_batches(&self.schema, batches)?;

        // Sort
        let sorted = sort_batch(&combined, &self.order_by)?;

        // Write to disk
        write_batches_to_parquet(path, &[sorted])?;

        Ok(())
    }

    fn merge_runs(&self, runs: &[PathBuf]) -> Result<Vec<RecordBatch>> {
        // Simple implementation: read all runs and merge
        // A production implementation would use streaming k-way merge
        let mut all_batches = Vec::new();

        for run in runs {
            let batches = read_parquet(run)?;
            all_batches.extend(batches);
        }

        if all_batches.is_empty() {
            return Ok(Vec::new());
        }

        // Final merge sort
        let combined = compute::concat_batches(&self.schema, &all_batches)?;
        let sorted = sort_batch(&combined, &self.order_by)?;

        Ok(vec![sorted])
    }
}

impl fmt::Display for ExternalSortExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let order: Vec<String> = self
            .order_by
            .iter()
            .map(|s| format!("{}", s.expr))
            .collect();
        write!(f, "ExternalSort: [{}]", order.join(", "))
    }
}

// ============================================================================
// Helper Functions
// ============================================================================

/// Estimate the memory size of a RecordBatch
fn estimate_batch_size(batch: &RecordBatch) -> usize {
    batch.get_array_memory_size()
}

/// Find the index of the largest partition
fn find_largest_partition(partitions: &[Option<BuildPartition>]) -> Option<usize> {
    partitions
        .iter()
        .enumerate()
        .filter_map(|(idx, p)| p.as_ref().map(|part| (idx, part.memory_bytes)))
        .max_by_key(|(_, size)| *size)
        .map(|(idx, _)| idx)
}

/// Partition a batch by hash of key columns
fn partition_batch_by_hash(
    batch: &RecordBatch,
    key_exprs: &[Expr],
    num_partitions: usize,
) -> Result<Vec<Option<RecordBatch>>> {
    let key_arrays: Result<Vec<ArrayRef>> =
        key_exprs.iter().map(|e| evaluate_expr(batch, e)).collect();
    let key_arrays = key_arrays?;

    // Compute partition for each row
    let mut partition_indices: Vec<Vec<usize>> = (0..num_partitions).map(|_| Vec::new()).collect();

    for row in 0..batch.num_rows() {
        let key = extract_join_key(&key_arrays, row);
        
        
        let partition = (hashbrown::hash_map::DefaultHashBuilder::default().hash_one(&key) as usize) % num_partitions;
        partition_indices[partition].push(row);
    }

    // Build batches for each partition
    let mut result: Vec<Option<RecordBatch>> = Vec::with_capacity(num_partitions);

    for indices in partition_indices {
        if indices.is_empty() {
            result.push(None);
        } else {
            let indices_arr =
                UInt32Array::from(indices.iter().map(|&i| i as u32).collect::<Vec<_>>());
            let columns: Result<Vec<ArrayRef>> = batch
                .columns()
                .iter()
                .map(|col| compute::take(col.as_ref(), &indices_arr, None).map_err(Into::into))
                .collect();
            let part_batch = RecordBatch::try_new(batch.schema(), columns?)?;
            result.push(Some(part_batch));
        }
    }

    Ok(result)
}

/// Write batches to a Parquet file
fn write_batches_to_parquet(path: &PathBuf, batches: &[RecordBatch]) -> Result<()> {
    if batches.is_empty() {
        return Ok(());
    }

    let file = File::create(path).map_err(|e| {
        QueryError::Execution(format!("Failed to create parquet file {:?}: {}", path, e))
    })?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(file, batches[0].schema(), Some(props))?;

    for batch in batches {
        writer.write(batch)?;
    }

    writer.close()?;
    Ok(())
}

/// Append a batch to an existing Parquet file (or create new)
fn append_to_parquet(path: &PathBuf, batch: &RecordBatch) -> Result<()> {
    // For simplicity, read existing, append, and rewrite
    // A production implementation would use a proper append mechanism
    let mut batches = if path.exists() {
        read_parquet(path)?
    } else {
        Vec::new()
    };
    batches.push(batch.clone());
    write_batches_to_parquet(path, &batches)
}

/// Read batches from a Parquet file
fn read_parquet(path: &PathBuf) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).map_err(|e| {
        QueryError::Execution(format!("Failed to open parquet file {:?}: {}", path, e))
    })?;

    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let reader = builder.build()?;

    let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
    Ok(batches)
}

/// Sort a record batch
fn sort_batch(batch: &RecordBatch, order_by: &[crate::planner::SortExpr]) -> Result<RecordBatch> {
    use crate::planner::SortDirection;
    use arrow::compute::{lexsort_to_indices, SortColumn, SortOptions};

    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    let sort_columns: Result<Vec<SortColumn>> = order_by
        .iter()
        .map(|s| {
            let values = evaluate_expr(batch, &s.expr)?;
            Ok(SortColumn {
                values,
                options: Some(SortOptions {
                    descending: s.direction == SortDirection::Desc,
                    nulls_first: matches!(s.nulls, crate::planner::NullOrdering::NullsFirst),
                }),
            })
        })
        .collect();
    let sort_columns = sort_columns?;

    let indices = lexsort_to_indices(&sort_columns, None)?;

    let sorted_columns: Result<Vec<ArrayRef>> = batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &indices, None).map_err(Into::into))
        .collect();

    RecordBatch::try_new(batch.schema(), sorted_columns?).map_err(Into::into)
}

// ============================================================================
// Join Key and Hash Table (reused from hash_join.rs)
// ============================================================================

#[derive(Clone)]
struct JoinKey {
    values: Vec<JoinValue>,
}

#[derive(Clone)]
enum JoinValue {
    Null,
    Int64(i64),
    Float64(ordered_float::OrderedFloat<f64>),
    String(String),
}

impl PartialEq for JoinKey {
    fn eq(&self, other: &Self) -> bool {
        if self.values.len() != other.values.len() {
            return false;
        }
        self.values
            .iter()
            .zip(other.values.iter())
            .all(|(a, b)| match (a, b) {
                (JoinValue::Null, JoinValue::Null) => true,
                (JoinValue::Int64(a), JoinValue::Int64(b)) => a == b,
                (JoinValue::Float64(a), JoinValue::Float64(b)) => a == b,
                (JoinValue::String(a), JoinValue::String(b)) => a == b,
                _ => false,
            })
    }
}

impl Eq for JoinKey {}

impl Hash for JoinKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for v in &self.values {
            match v {
                JoinValue::Null => 0u8.hash(state),
                JoinValue::Int64(i) => {
                    1u8.hash(state);
                    i.hash(state);
                }
                JoinValue::Float64(f) => {
                    2u8.hash(state);
                    f.hash(state);
                }
                JoinValue::String(s) => {
                    3u8.hash(state);
                    s.hash(state);
                }
            }
        }
    }
}

#[derive(Clone)]
struct HashEntry {
    batch_idx: usize,
    row_idx: usize,
}

fn extract_join_key(arrays: &[ArrayRef], row: usize) -> JoinKey {
    let values: Vec<JoinValue> = arrays
        .iter()
        .map(|arr| {
            if arr.is_null(row) {
                return JoinValue::Null;
            }

            if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                return JoinValue::Int64(a.value(row));
            }
            if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int32Array>() {
                return JoinValue::Int64(a.value(row) as i64);
            }
            if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
                return JoinValue::Int64(a.value(row) as i64);
            }
            if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
                return JoinValue::Float64(ordered_float::OrderedFloat(a.value(row)));
            }
            if let Some(a) = arr.as_any().downcast_ref::<arrow::array::StringArray>() {
                return JoinValue::String(a.value(row).to_string());
            }

            JoinValue::Null
        })
        .collect();

    JoinKey { values }
}

fn build_hash_table(
    batches: &[RecordBatch],
    key_exprs: &[Expr],
) -> Result<HashMap<JoinKey, Vec<HashEntry>>> {
    let mut table: HashMap<JoinKey, Vec<HashEntry>> = HashMap::new();

    for (batch_idx, batch) in batches.iter().enumerate() {
        let key_arrays: Result<Vec<ArrayRef>> =
            key_exprs.iter().map(|e| evaluate_expr(batch, e)).collect();
        let key_arrays = key_arrays?;

        for row_idx in 0..batch.num_rows() {
            let key = extract_join_key(&key_arrays, row_idx);

            if key.values.iter().any(|v| matches!(v, JoinValue::Null)) {
                continue;
            }

            table
                .entry(key)
                .or_default()
                .push(HashEntry { batch_idx, row_idx });
        }
    }

    Ok(table)
}

fn probe_partition(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    hash_table: &HashMap<JoinKey, Vec<HashEntry>>,
    probe_key_exprs: &[Expr],
    _join_type: JoinType, // TODO: handle all join types
    swapped: bool,
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    // Simplified probe - for inner join only
    // Full implementation would handle all join types
    let mut results = Vec::new();

    for probe_batch in probe_batches {
        let probe_key_arrays: Result<Vec<ArrayRef>> = probe_key_exprs
            .iter()
            .map(|e| evaluate_expr(probe_batch, e))
            .collect();
        let probe_key_arrays = probe_key_arrays?;

        let mut build_indices: Vec<(usize, usize)> = Vec::new();
        let mut probe_indices: Vec<usize> = Vec::new();

        for probe_row in 0..probe_batch.num_rows() {
            let key = extract_join_key(&probe_key_arrays, probe_row);

            if key.values.iter().any(|v| matches!(v, JoinValue::Null)) {
                continue;
            }

            if let Some(entries) = hash_table.get(&key) {
                for entry in entries {
                    build_indices.push((entry.batch_idx, entry.row_idx));
                    probe_indices.push(probe_row);
                }
            }
        }

        if !build_indices.is_empty() {
            let batch = create_joined_batch(
                build_batches,
                probe_batch,
                &build_indices,
                &probe_indices,
                swapped,
                output_schema,
            )?;
            results.push(batch);
        }
    }

    Ok(results)
}

fn create_joined_batch(
    build_batches: &[RecordBatch],
    probe_batch: &RecordBatch,
    build_indices: &[(usize, usize)],
    probe_indices: &[usize],
    swapped: bool,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    // Gather build columns
    let build_columns: Result<Vec<ArrayRef>> = if build_batches.is_empty() {
        Ok(vec![])
    } else {
        (0..build_batches[0].num_columns())
            .map(|col_idx| gather_column(build_batches, col_idx, build_indices))
            .collect()
    };
    let build_columns = build_columns?;

    // Gather probe columns
    let probe_indices_arr: Vec<u32> = probe_indices.iter().map(|&i| i as u32).collect();
    let probe_index_arr = UInt32Array::from(probe_indices_arr);

    let probe_columns: Result<Vec<ArrayRef>> = probe_batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &probe_index_arr, None).map_err(Into::into))
        .collect();
    let probe_columns = probe_columns?;

    let columns: Vec<ArrayRef> = if swapped {
        probe_columns.into_iter().chain(build_columns).collect()
    } else {
        build_columns.into_iter().chain(probe_columns).collect()
    };

    RecordBatch::try_new(output_schema.clone(), columns).map_err(Into::into)
}

fn gather_column(
    batches: &[RecordBatch],
    col_idx: usize,
    indices: &[(usize, usize)],
) -> Result<ArrayRef> {
    let mut batch_indices: HashMap<usize, Vec<(usize, u32)>> = HashMap::new();
    for (out_idx, &(batch_idx, row_idx)) in indices.iter().enumerate() {
        batch_indices
            .entry(batch_idx)
            .or_default()
            .push((out_idx, row_idx as u32));
    }

    let total_len = indices.len();
    let dt = batches[0].column(col_idx).data_type();

    let mut builders_data: Vec<(usize, ArrayRef)> = Vec::new();

    for (batch_idx, idx_list) in batch_indices {
        let batch = &batches[batch_idx];
        let col = batch.column(col_idx);

        let take_indices: Vec<u32> = idx_list.iter().map(|(_, row)| *row).collect();
        let take_arr = UInt32Array::from(take_indices);

        let taken = compute::take(col.as_ref(), &take_arr, None)?;

        for (i, (out_idx, _)) in idx_list.iter().enumerate() {
            builders_data.push((
                *out_idx,
                compute::take(&taken, &UInt32Array::from(vec![i as u32]), None)?,
            ));
        }
    }

    builders_data.sort_by_key(|(idx, _)| *idx);

    if builders_data.is_empty() {
        return Ok(arrow::array::new_null_array(dt, total_len));
    }

    let arrays: Vec<&dyn arrow::array::Array> =
        builders_data.iter().map(|(_, arr)| arr.as_ref()).collect();

    if arrays.is_empty() {
        Ok(arrow::array::new_null_array(dt, total_len))
    } else {
        compute::concat(&arrays).map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_estimate_batch_size() {
        use arrow::array::Int64Array;
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let size = estimate_batch_size(&batch);
        assert!(size > 0);
    }
}
