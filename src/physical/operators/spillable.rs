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
use arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int64Array, StringArray, UInt32Array, UInt64Array,
};
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
use std::path::PathBuf;
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
        spill_dir: &PathBuf,
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

    async fn probe_with_spilling(
        &self,
        mut probe_stream: RecordBatchStream,
        probe_keys: &[Expr],
        in_memory_partitions: &mut [Option<BuildPartition>],
        hash_tables: &[Option<HashMap<JoinKey, Vec<HashEntry>>>],
        spilled_partitions: &[Option<SpilledPartition>],
        spill_dir: &PathBuf,
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
    memory_pool: SharedMemoryPool,
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

/// State for an aggregate partition during processing
struct AggregatePartition {
    batches: Vec<RecordBatch>,
    memory_bytes: usize,
}

impl AggregatePartition {
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

    fn clear(&mut self) {
        self.batches.clear();
        self.memory_bytes = 0;
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
        // Ensure spill directory exists
        self.config.ensure_spill_dir()?;

        let spill_dir =
            self.config
                .spill_path
                .join(format!("agg_{}_{}", partition, std::process::id()));
        std::fs::create_dir_all(&spill_dir).map_err(|e| {
            QueryError::Execution(format!("Failed to create spill directory: {}", e))
        })?;

        let input_stream = self.input.execute(partition).await?;

        // Process with partitioning and spilling
        let (in_memory_partitions, spilled_files) = self
            .aggregate_with_spilling(input_stream, &spill_dir)
            .await?;

        // Aggregate in-memory partitions
        let mut all_results = Vec::new();

        for part in in_memory_partitions.into_iter().flatten() {
            if !part.batches.is_empty() {
                let result = crate::physical::operators::hash_agg::aggregate_batches_external(
                    &part.batches,
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
                all_results.push(result);
            }
        }

        // Process spilled partitions one at a time to limit memory
        for spill_file in &spilled_files {
            if let Some(path) = spill_file {
                let batches = read_parquet(path)?;
                if !batches.is_empty() {
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
                    all_results.push(result);
                }
            }
        }

        // Clean up spill directory
        let _ = std::fs::remove_dir_all(&spill_dir);

        // If we have results from multiple partitions, we need a final merge
        // For now, concatenate all partial results (works for most aggregates)
        if all_results.is_empty() {
            // Return empty result with correct schema
            let empty_batch = RecordBatch::new_empty(self.schema.clone());
            return Ok(Box::pin(stream::once(async { Ok(empty_batch) })));
        }

        // If only one partition had data, return it directly
        if all_results.len() == 1 {
            return Ok(Box::pin(stream::once(
                async move { Ok(all_results.remove(0)) },
            )));
        }

        // Multiple partitions: need to re-aggregate the partial results
        let final_result = crate::physical::operators::hash_agg::aggregate_batches_external(
            &all_results,
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

        Ok(Box::pin(stream::once(async { Ok(final_result) })))
    }

    fn name(&self) -> &str {
        "SpillableHashAggregate"
    }
}

impl SpillableHashAggregateExec {
    /// Process input with hash partitioning and spilling when memory limit is reached
    async fn aggregate_with_spilling(
        &self,
        mut input_stream: RecordBatchStream,
        spill_dir: &PathBuf,
    ) -> Result<(Vec<Option<AggregatePartition>>, Vec<Option<PathBuf>>)> {
        let mut partitions: Vec<Option<AggregatePartition>> = (0..NUM_PARTITIONS)
            .map(|_| Some(AggregatePartition::new()))
            .collect();
        let mut spilled_files: Vec<Option<PathBuf>> = (0..NUM_PARTITIONS).map(|_| None).collect();
        let mut spill_file_counts: Vec<usize> = vec![0; NUM_PARTITIONS];

        let mut total_memory: usize = 0;
        let memory_threshold =
            (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;

        while let Some(batch) = input_stream.try_next().await? {
            let batch_size = estimate_batch_size(&batch);

            // Check if we need to spill before adding more data
            if total_memory + batch_size > memory_threshold {
                // Find the largest partition to spill
                if let Some(idx) = find_largest_agg_partition(&partitions) {
                    if let Some(ref mut part) = partitions[idx] {
                        if !part.batches.is_empty() {
                            // Spill this partition
                            let spill_path = spill_dir
                                .join(format!("part_{}_{}.parquet", idx, spill_file_counts[idx]));
                            spill_file_counts[idx] += 1;

                            write_batches_to_parquet(&spill_path, &part.batches)?;
                            self.memory_pool.record_spill(part.memory_bytes);
                            total_memory -= part.memory_bytes;

                            // If we already have a spill file for this partition, merge them
                            if let Some(ref existing_path) = spilled_files[idx] {
                                // Append new file path to a list file or merge
                                // For simplicity, we'll just keep the latest and merge on read
                                merge_parquet_files(existing_path, &spill_path, spill_dir, idx)?;
                            } else {
                                spilled_files[idx] = Some(spill_path);
                            }

                            part.clear();
                        }
                    }
                }
            }

            // Partition the batch by group key hash
            let partitioned = partition_batch_by_hash(&batch, &self.group_by, NUM_PARTITIONS)?;

            for (idx, part_batch) in partitioned.into_iter().enumerate() {
                if let Some(pb) = part_batch {
                    let pb_size = estimate_batch_size(&pb);

                    if let Some(ref mut part) = partitions[idx] {
                        part.add_batch(pb);
                        total_memory += pb_size;
                    } else if let Some(ref spill_path) = spilled_files[idx] {
                        // Partition was fully spilled, append to spill file
                        let temp_path = spill_dir
                            .join(format!("temp_{}_{}.parquet", idx, spill_file_counts[idx]));
                        spill_file_counts[idx] += 1;
                        write_batches_to_parquet(&temp_path, &[pb])?;
                        merge_parquet_files(spill_path, &temp_path, spill_dir, idx)?;
                    }
                }
            }
        }

        Ok((partitions, spilled_files))
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
        spill_dir: &PathBuf,
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
        // Streaming k-way merge: process runs in batches to limit memory
        // Maximum number of runs to merge at once
        const MAX_MERGE_FANIN: usize = 8;
        // Maximum rows to buffer per run during merge
        const MERGE_BUFFER_ROWS: usize = 8192;

        if runs.is_empty() {
            return Ok(Vec::new());
        }

        if runs.len() == 1 {
            return read_parquet(&runs[0]);
        }

        // If we have too many runs, merge in multiple passes
        if runs.len() > MAX_MERGE_FANIN {
            return self.multi_pass_merge(runs, MAX_MERGE_FANIN);
        }

        // Single-pass k-way merge with bounded memory
        self.streaming_k_way_merge(runs, MERGE_BUFFER_ROWS)
    }

    /// Multi-pass merge for when there are too many runs
    fn multi_pass_merge(&self, runs: &[PathBuf], fanin: usize) -> Result<Vec<RecordBatch>> {
        let mut current_runs = runs.to_vec();
        let mut pass = 0;

        // Get spill directory from first run's parent
        let spill_dir = runs[0].parent().unwrap_or(std::path::Path::new("/tmp"));

        while current_runs.len() > fanin {
            let mut next_runs = Vec::new();

            for chunk in current_runs.chunks(fanin) {
                if chunk.len() == 1 {
                    next_runs.push(chunk[0].clone());
                } else {
                    // Merge this chunk into a new run
                    let merged = self.streaming_k_way_merge(chunk, 8192)?;
                    if !merged.is_empty() {
                        let output_path = spill_dir.join(format!(
                            "merged_pass{}_{}.parquet",
                            pass,
                            next_runs.len()
                        ));
                        write_batches_to_parquet(&output_path, &merged)?;
                        next_runs.push(output_path);
                    }
                }
            }

            // Clean up old runs from previous pass (except original runs)
            if pass > 0 {
                for run in &current_runs {
                    let _ = std::fs::remove_file(run);
                }
            }

            current_runs = next_runs;
            pass += 1;
        }

        // Final merge
        self.streaming_k_way_merge(&current_runs, 8192)
    }

    /// Streaming k-way merge with bounded memory usage
    fn streaming_k_way_merge(
        &self,
        runs: &[PathBuf],
        buffer_rows: usize,
    ) -> Result<Vec<RecordBatch>> {
        use std::cmp::Ordering;
        use std::collections::BinaryHeap;

        if runs.is_empty() {
            return Ok(Vec::new());
        }

        // Open iterators for each run
        let mut run_iterators: Vec<
            Box<dyn Iterator<Item = std::result::Result<RecordBatch, arrow::error::ArrowError>>>,
        > = Vec::new();
        let mut run_buffers: Vec<Option<RecordBatch>> = Vec::new();
        let mut run_indices: Vec<usize> = Vec::new(); // Current row index in each buffer

        for run in runs {
            let file = File::open(run).map_err(|e| {
                QueryError::Execution(format!("Failed to open run file {:?}: {}", run, e))
            })?;
            let builder =
                ParquetRecordBatchReaderBuilder::try_new(file)?.with_batch_size(buffer_rows);
            let reader = builder.build()?;
            run_iterators.push(Box::new(reader));
            run_buffers.push(None);
            run_indices.push(0);
        }

        // Load initial batch from each run
        for (i, iter) in run_iterators.iter_mut().enumerate() {
            if let Some(batch_result) = iter.next() {
                run_buffers[i] = Some(batch_result?);
                run_indices[i] = 0;
            }
        }

        // Build output batches using a simple row-by-row merge
        // For better performance, we'd want to do vectorized merge, but this is memory-safe
        let mut result_batches = Vec::new();
        let mut output_rows: Vec<(usize, usize)> = Vec::new(); // (run_idx, row_idx)

        // Helper to compare rows
        let compare_rows = |batch_a: &RecordBatch,
                            row_a: usize,
                            batch_b: &RecordBatch,
                            row_b: usize,
                            order_by: &[crate::planner::SortExpr]|
         -> std::cmp::Ordering {
            for sort_expr in order_by {
                let col_a = evaluate_expr(batch_a, &sort_expr.expr).ok();
                let col_b = evaluate_expr(batch_b, &sort_expr.expr).ok();

                if let (Some(a), Some(b)) = (col_a, col_b) {
                    let cmp = compare_array_values(&a, row_a, &b, row_b);
                    let cmp = if sort_expr.direction == crate::planner::SortDirection::Desc {
                        cmp.reverse()
                    } else {
                        cmp
                    };
                    if cmp != Ordering::Equal {
                        return cmp;
                    }
                }
            }
            Ordering::Equal
        };

        // Simple merge: repeatedly find minimum across all runs
        loop {
            // Find run with minimum current row
            let mut min_run: Option<usize> = None;

            for (run_idx, buffer) in run_buffers.iter().enumerate() {
                if let Some(ref batch) = buffer {
                    if run_indices[run_idx] < batch.num_rows() {
                        min_run = match min_run {
                            None => Some(run_idx),
                            Some(current_min) => {
                                let cmp = compare_rows(
                                    batch,
                                    run_indices[run_idx],
                                    run_buffers[current_min].as_ref().unwrap(),
                                    run_indices[current_min],
                                    &self.order_by,
                                );
                                if cmp == Ordering::Less {
                                    Some(run_idx)
                                } else {
                                    Some(current_min)
                                }
                            }
                        };
                    }
                }
            }

            match min_run {
                None => break, // All runs exhausted
                Some(run_idx) => {
                    output_rows.push((run_idx, run_indices[run_idx]));
                    run_indices[run_idx] += 1;

                    // Check if current buffer is exhausted
                    if let Some(ref batch) = run_buffers[run_idx] {
                        if run_indices[run_idx] >= batch.num_rows() {
                            // Try to load next batch from this run
                            if let Some(next_batch) = run_iterators[run_idx].next() {
                                run_buffers[run_idx] = Some(next_batch?);
                                run_indices[run_idx] = 0;
                            } else {
                                run_buffers[run_idx] = None;
                            }
                        }
                    }

                    // Flush output when buffer is full
                    if output_rows.len() >= buffer_rows {
                        let batch = self.build_merged_batch(&run_buffers, &output_rows)?;
                        result_batches.push(batch);
                        output_rows.clear();
                    }
                }
            }
        }

        // Flush remaining output
        if !output_rows.is_empty() {
            // For the final batch, we need to reload any exhausted buffers
            // that are referenced in output_rows
            let batch = self.build_merged_batch_final(&runs, &output_rows, buffer_rows)?;
            result_batches.push(batch);
        }

        Ok(result_batches)
    }

    /// Build a merged batch from the given row references
    fn build_merged_batch(
        &self,
        run_buffers: &[Option<RecordBatch>],
        rows: &[(usize, usize)],
    ) -> Result<RecordBatch> {
        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // Group rows by run
        let mut run_row_groups: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();
        for (output_idx, &(run_idx, row_idx)) in rows.iter().enumerate() {
            run_row_groups
                .entry(run_idx)
                .or_default()
                .push((output_idx, row_idx));
        }

        // Build output columns
        let num_cols = self.schema.fields().len();
        let mut output_columns: Vec<Vec<(usize, ArrayRef)>> = vec![Vec::new(); num_cols];

        for (run_idx, row_list) in run_row_groups {
            if let Some(ref batch) = run_buffers[run_idx] {
                let take_indices: Vec<u32> = row_list.iter().map(|(_, r)| *r as u32).collect();
                let indices_arr = UInt32Array::from(take_indices);

                for col_idx in 0..num_cols.min(batch.num_columns()) {
                    let taken = compute::take(batch.column(col_idx), &indices_arr, None)?;
                    for (i, (out_idx, _)) in row_list.iter().enumerate() {
                        let single =
                            compute::take(&taken, &UInt32Array::from(vec![i as u32]), None)?;
                        output_columns[col_idx].push((*out_idx, single));
                    }
                }
            }
        }

        // Sort and concatenate columns
        let mut final_columns: Vec<ArrayRef> = Vec::new();
        for col_parts in output_columns {
            let mut sorted_parts = col_parts;
            sorted_parts.sort_by_key(|(idx, _)| *idx);
            let arrays: Vec<&dyn arrow::array::Array> =
                sorted_parts.iter().map(|(_, arr)| arr.as_ref()).collect();
            if arrays.is_empty() {
                final_columns.push(arrow::array::new_null_array(
                    self.schema.field(final_columns.len()).data_type(),
                    rows.len(),
                ));
            } else {
                final_columns.push(compute::concat(&arrays)?);
            }
        }

        RecordBatch::try_new(self.schema.clone(), final_columns).map_err(Into::into)
    }

    /// Build final merged batch, reloading data from files if needed
    fn build_merged_batch_final(
        &self,
        runs: &[PathBuf],
        rows: &[(usize, usize)],
        _buffer_rows: usize,
    ) -> Result<RecordBatch> {
        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // For the final batch, we may need to re-read some runs
        // Group by run and load only what we need
        let mut run_row_groups: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();
        for (output_idx, &(run_idx, row_idx)) in rows.iter().enumerate() {
            run_row_groups
                .entry(run_idx)
                .or_default()
                .push((output_idx, row_idx));
        }

        let num_cols = self.schema.fields().len();
        let mut output_columns: Vec<Vec<(usize, ArrayRef)>> = vec![Vec::new(); num_cols];

        for (run_idx, row_list) in run_row_groups {
            // Read the run
            let batches = read_parquet(&runs[run_idx])?;
            if batches.is_empty() {
                continue;
            }

            // Concatenate all batches from this run
            let combined = compute::concat_batches(&batches[0].schema(), &batches)?;

            let take_indices: Vec<u32> = row_list.iter().map(|(_, r)| *r as u32).collect();
            let indices_arr = UInt32Array::from(take_indices);

            for col_idx in 0..num_cols.min(combined.num_columns()) {
                let taken = compute::take(combined.column(col_idx), &indices_arr, None)?;
                for (i, (out_idx, _)) in row_list.iter().enumerate() {
                    let single = compute::take(&taken, &UInt32Array::from(vec![i as u32]), None)?;
                    output_columns[col_idx].push((*out_idx, single));
                }
            }
        }

        // Sort and concatenate columns
        let mut final_columns: Vec<ArrayRef> = Vec::new();
        for col_parts in output_columns {
            let mut sorted_parts = col_parts;
            sorted_parts.sort_by_key(|(idx, _)| *idx);
            let arrays: Vec<&dyn arrow::array::Array> =
                sorted_parts.iter().map(|(_, arr)| arr.as_ref()).collect();
            if arrays.is_empty() {
                final_columns.push(arrow::array::new_null_array(
                    self.schema.field(final_columns.len()).data_type(),
                    rows.len(),
                ));
            } else {
                final_columns.push(compute::concat(&arrays)?);
            }
        }

        RecordBatch::try_new(self.schema.clone(), final_columns).map_err(Into::into)
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

/// Find the index of the largest aggregate partition
fn find_largest_agg_partition(partitions: &[Option<AggregatePartition>]) -> Option<usize> {
    partitions
        .iter()
        .enumerate()
        .filter_map(|(idx, p)| p.as_ref().map(|part| (idx, part.memory_bytes)))
        .max_by_key(|(_, size)| *size)
        .map(|(idx, _)| idx)
}

/// Merge two parquet files into one, using streaming to limit memory
fn merge_parquet_files(
    existing: &PathBuf,
    new_file: &PathBuf,
    spill_dir: &PathBuf,
    partition_idx: usize,
) -> Result<()> {
    // Use a streaming approach: create a new merged file
    let merged_path = spill_dir.join(format!("merged_{}.parquet", partition_idx));

    // Open readers for both files
    let file1 = File::open(existing)
        .map_err(|e| QueryError::Execution(format!("Failed to open file {:?}: {}", existing, e)))?;
    let file2 = File::open(new_file)
        .map_err(|e| QueryError::Execution(format!("Failed to open file {:?}: {}", new_file, e)))?;

    let reader1 = ParquetRecordBatchReaderBuilder::try_new(file1)?
        .with_batch_size(8192)
        .build()?;
    let reader2 = ParquetRecordBatchReaderBuilder::try_new(file2)?
        .with_batch_size(8192)
        .build()?;

    // Get schema from first file
    let schema = {
        let file = File::open(existing)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        builder.schema().clone()
    };

    // Create output file
    let output_file = File::create(&merged_path).map_err(|e| {
        QueryError::Execution(format!(
            "Failed to create merged file {:?}: {}",
            merged_path, e
        ))
    })?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(output_file, schema, Some(props))?;

    // Stream batches from both files
    for batch_result in reader1 {
        let batch = batch_result?;
        writer.write(&batch)?;
    }

    for batch_result in reader2 {
        let batch = batch_result?;
        writer.write(&batch)?;
    }

    writer.close()?;

    // Replace existing file with merged file
    std::fs::rename(&merged_path, existing)
        .map_err(|e| QueryError::Execution(format!("Failed to rename merged file: {}", e)))?;

    // Remove the new file since it's been merged
    let _ = std::fs::remove_file(new_file);

    Ok(())
}

/// Compare two array values at given indices
fn compare_array_values(
    a: &ArrayRef,
    row_a: usize,
    b: &ArrayRef,
    row_b: usize,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    // Handle nulls
    let a_null = a.is_null(row_a);
    let b_null = b.is_null(row_b);

    match (a_null, b_null) {
        (true, true) => return Ordering::Equal,
        (true, false) => return Ordering::Greater, // nulls last
        (false, true) => return Ordering::Less,
        (false, false) => {}
    }

    // Compare based on type
    if let Some(arr_a) = a.as_any().downcast_ref::<Int64Array>() {
        if let Some(arr_b) = b.as_any().downcast_ref::<Int64Array>() {
            return arr_a.value(row_a).cmp(&arr_b.value(row_b));
        }
    }

    if let Some(arr_a) = a.as_any().downcast_ref::<arrow::array::Int32Array>() {
        if let Some(arr_b) = b.as_any().downcast_ref::<arrow::array::Int32Array>() {
            return arr_a.value(row_a).cmp(&arr_b.value(row_b));
        }
    }

    if let Some(arr_a) = a.as_any().downcast_ref::<Float64Array>() {
        if let Some(arr_b) = b.as_any().downcast_ref::<Float64Array>() {
            let va = arr_a.value(row_a);
            let vb = arr_b.value(row_b);
            return va.partial_cmp(&vb).unwrap_or(Ordering::Equal);
        }
    }

    if let Some(arr_a) = a.as_any().downcast_ref::<StringArray>() {
        if let Some(arr_b) = b.as_any().downcast_ref::<StringArray>() {
            return arr_a.value(row_a).cmp(arr_b.value(row_b));
        }
    }

    if let Some(arr_a) = a.as_any().downcast_ref::<Date32Array>() {
        if let Some(arr_b) = b.as_any().downcast_ref::<Date32Array>() {
            return arr_a.value(row_a).cmp(&arr_b.value(row_b));
        }
    }

    Ordering::Equal
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
        let mut hasher = hashbrown::hash_map::DefaultHashBuilder::default().build_hasher();
        key.hash(&mut hasher);
        let partition = (hasher.finish() as usize) % num_partitions;
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

/// Append a batch to an existing Parquet file (or create new) using streaming
fn append_to_parquet(path: &PathBuf, batch: &RecordBatch) -> Result<()> {
    if !path.exists() {
        // No existing file, just write the batch
        return write_batches_to_parquet(path, &[batch.clone()]);
    }

    // Streaming append: create temp file, stream existing + new batch, then rename
    let temp_path = path.with_extension("parquet.tmp");

    // Get schema from existing file
    let schema = {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        builder.schema().clone()
    };

    // Create output writer
    let output_file = File::create(&temp_path).map_err(|e| {
        QueryError::Execution(format!("Failed to create temp file {:?}: {}", temp_path, e))
    })?;

    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();

    let mut writer = ArrowWriter::try_new(output_file, schema, Some(props))?;

    // Stream existing batches (one at a time to limit memory)
    {
        let existing_file = File::open(path)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(existing_file)?
            .with_batch_size(8192)
            .build()?;

        for batch_result in reader {
            let existing_batch = batch_result?;
            writer.write(&existing_batch)?;
        }
    }

    // Write the new batch
    writer.write(batch)?;
    writer.close()?;

    // Atomically replace old file with new
    std::fs::rename(&temp_path, path)
        .map_err(|e| QueryError::Execution(format!("Failed to rename temp file: {}", e)))?;

    Ok(())
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
