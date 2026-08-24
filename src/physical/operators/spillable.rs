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
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Unique counter for spill directory names to avoid conflicts between concurrent operators
static SPILL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Number of hash partitions for spilling
const NUM_PARTITIONS: usize = 64;

/// Monotonic call-sequence id for `QE_SPILL_DEBUG` join/aggregate spill-path
/// tracing (task 001, spill-join-correctness epic). Diagnostic only — lets
/// log lines from repeated or overlapping invocations of
/// `execute_spill_path` / `execute_fused_streaming` (e.g. a fused-streaming
/// aggregate that aborts and falls back, re-executing its input) be told
/// apart in the log. Never read by any non-diagnostic code path.
static SJ_TRACE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Next id for `QE_SPILL_DEBUG` tracing. `Ordering::Relaxed` is enough: this
/// only needs distinct values for correlating log lines, never ordering
/// guarantees against other memory operations.
fn next_sj_trace_id() -> u64 {
    SJ_TRACE_SEQ.fetch_add(1, Ordering::Relaxed)
}

/// Drain every input partition concurrently and return the collected batches
/// together with their estimated in-memory size.
///
/// Pipeline-breaking operators (aggregate, sort) must consume all input partitions
/// before they can produce output. Draining them one-at-a-time in an `await` loop
/// serializes the whole subtree beneath the operator — a parallel scan/join feeding
/// an aggregate would execute on a single core. Spawning one task per partition lets
/// the producers run concurrently across the tokio worker pool.
///
/// The total size is returned so the caller can decide whether the data fits within
/// its memory budget or the spill path is required.
pub(crate) async fn collect_input_partitions_concurrently(
    input: &Arc<dyn PhysicalOperator>,
) -> Result<(Vec<RecordBatch>, usize)> {
    let input_partitions = input.output_partitions().max(1);

    if input_partitions == 1 {
        // Nothing to overlap — avoid the task-spawn round trip.
        let stream = input.execute(0).await?;
        let batches: Vec<RecordBatch> = stream.try_collect().await?;
        let size = batches.iter().map(estimate_batch_size).sum();
        return Ok((batches, size));
    }

    let mut handles = Vec::with_capacity(input_partitions);
    for part in 0..input_partitions {
        let input = input.clone();
        handles.push(tokio::spawn(async move {
            let stream = input.execute(part).await?;
            let batches: Vec<RecordBatch> = stream.try_collect().await?;
            let size: usize = batches.iter().map(estimate_batch_size).sum();
            Ok::<_, QueryError>((batches, size))
        }));
    }

    let mut all_batches = Vec::new();
    let mut total_size = 0usize;
    for handle in handles {
        let (batches, size) = handle
            .await
            .map_err(|e| QueryError::Execution(format!("Partition task join error: {}", e)))??;
        total_size += size;
        all_batches.extend(batches);
    }

    Ok((all_batches, total_size))
}

// ============================================================================
// Spillable Hash Join
// ============================================================================

/// Hash join execution operator with spilling support
///
/// When memory limits are exceeded, this operator partitions data by hash
/// and spills partitions to disk. During the probe phase, spilled partitions
/// are processed one at a time.
pub struct SpillableHashJoinExec {
    /// Runtime probe-scan key filter (Inner joins), passed to the delegate.
    pub probe_runtime_filter: Option<crate::physical::operators::SharedRuntimeFilter>,
    /// Which equi pair the runtime filter applies to.
    pub probe_runtime_filter_pair: usize,
    /// Join-output retention mask (see HashJoinExec::retained).
    pub retained: Option<Vec<bool>>,
    left: Arc<dyn PhysicalOperator>,
    right: Arc<dyn PhysicalOperator>,
    on: Vec<(Expr, Expr)>,
    join_type: JoinType,
    schema: SchemaRef,
    memory_pool: SharedMemoryPool,
    config: ExecutionConfig,
    /// When true, build hash table from right side (smaller) for Left joins.
    build_right: bool,
    /// Optional join filter (e.g., for Semi/Anti with additional predicates)
    filter: Option<Expr>,
    /// Cached build decision — computed once, shared across all partition executions
    build_decision: OnceCell<BuildDecision>,
}

impl fmt::Debug for SpillableHashJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpillableHashJoinExec")
            .field("join_type", &self.join_type)
            .field("on", &self.on)
            .finish()
    }
}

/// Cached decision about whether the build side fits in memory
enum BuildDecision {
    /// Build fits in memory — use cached HashJoinExec for parallel probe
    InMemory(Arc<crate::physical::operators::HashJoinExec>),
    /// Build exceeds memory — spill path (only partition 0 processes)
    Spill {
        /// Build batches, kept for EVERY execution of partition 0 — an
        /// operator above may legitimately execute its child more than once
        /// (the fused streaming aggregate drains, aborts, and re-executes).
        /// The previous `Mutex<Option<..>>::take()` shape handed the second
        /// execution an EMPTY build side, which joined to zero rows and
        /// returned them as the answer. Cloning is cheap: RecordBatch
        /// columns are Arc'd buffers.
        build_batches: Vec<RecordBatch>,
    },
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
            build_right: false,
            filter: None,
            build_decision: OnceCell::new(),
            probe_runtime_filter: None,
            probe_runtime_filter_pair: 0,
            retained: None,
        }
    }

    /// Set build_right flag: when true, build hash table from right side.
    pub fn with_build_right(mut self, build_right: bool) -> Self {
        self.build_right = build_right;
        self
    }

    /// Set join filter (for Semi/Anti joins with additional predicates).
    pub fn with_filter(mut self, filter: Option<Expr>) -> Self {
        self.filter = filter;
        self
    }

    /// Join-output retention mask over the FULL (left ++ right) column
    /// order: false = no ancestor references the column (ON-only keys, or a
    /// filter-only column nothing downstream selects), so it is dropped
    /// from the output schema and never gathered. Set by the planner's
    /// usage analysis for Inner/Left/Right/Full joins whose filter (if any)
    /// contains no subquery.
    ///
    /// Gate condition — MUST stay identical to `HashJoinExec::set_retained`
    /// and the planner's `analyze_join_output_usage` (three gates that must
    /// move in lockstep). This wrapper delegates to an inner `HashJoinExec`
    /// for the in-memory build path and forwards this exact mask to it
    /// (`hj.set_retained(self.retained.clone())`); if that gate ever
    /// disagreed with this one, `self.schema()` would report a narrower
    /// width than the delegate's stream actually produces — a silent
    /// schema/column-count mismatch a unit test on `HashJoinExec` alone
    /// cannot catch (see the dedicated Spillable-level regression test).
    pub fn set_retained(&mut self, mask: Option<Vec<bool>>) {
        if let Some(m) = &mask {
            let type_ok = matches!(
                self.join_type,
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
            );
            let filter_ok = self
                .filter
                .as_ref()
                .map(|f| !f.contains_subquery())
                .unwrap_or(true);
            if !type_ok || !filter_ok || m.len() != self.schema.fields().len() {
                return;
            }
            let fields: Vec<_> = self
                .schema
                .fields()
                .iter()
                .zip(m)
                .filter(|(_, keep)| **keep)
                .map(|(f, _)| f.clone())
                .collect();
            self.schema = Arc::new(Schema::new(fields));
        }
        self.retained = mask;
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

    fn output_partitions(&self) -> usize {
        match self.join_type {
            JoinType::Semi | JoinType::Anti => 1,
            _ => {
                let probe_side = if self.build_right || matches!(self.join_type, JoinType::Right) {
                    &self.left
                } else {
                    &self.right
                };
                probe_side.output_partitions().max(1)
            }
        }
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        crate::physical::check_partition(self, partition)?;

        // Determine build and probe sides
        let (build_side, probe_side, swapped) =
            if self.build_right || matches!(self.join_type, JoinType::Right) {
                (&self.right, &self.left, true)
            } else {
                (&self.left, &self.right, false)
            };

        // Get or compute the build decision (computed ONCE, shared across all partitions)
        let decision = self
            .build_decision
            .get_or_try_init(|| async {
                // Drain all build partitions concurrently — the sequential await
                // loop serialized a parallel scan beneath the build side onto one
                // core (same fix as the agg/sort pipeline breakers).
                let memory_threshold =
                    (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;
                let (build_batches, build_size) =
                    collect_input_partitions_concurrently(build_side).await?;
                let exceeded = build_size > memory_threshold;

                if !exceeded {
                    // Build side fits in memory — create a reusable HashJoinExec
                    let build_schema = build_batches
                        .first()
                        .map(|b| b.schema())
                        .unwrap_or_else(|| build_side.schema());
                    let build_mem = Arc::new(crate::physical::operators::MemoryTableExec::new(
                        "join_build",
                        build_schema,
                        build_batches,
                        None,
                    ));
                    let (left, right): (Arc<dyn PhysicalOperator>, Arc<dyn PhysicalOperator>) =
                        if swapped {
                            (self.left.clone(), build_mem as Arc<dyn PhysicalOperator>)
                        } else {
                            (build_mem as Arc<dyn PhysicalOperator>, self.right.clone())
                        };
                    let hash_join = if self.filter.is_some() {
                        let mut hj = crate::physical::operators::HashJoinExec::with_filter(
                            left,
                            right,
                            self.on.clone(),
                            self.join_type,
                            self.filter.clone(),
                        )
                        .with_build_right(self.build_right);
                        hj.probe_runtime_filter = self.probe_runtime_filter.clone();
                        hj.probe_runtime_filter_pair = self.probe_runtime_filter_pair;
                        hj.set_retained(self.retained.clone());
                        Arc::new(hj)
                    } else {
                        let mut hj = crate::physical::operators::HashJoinExec::new(
                            left,
                            right,
                            self.on.clone(),
                            self.join_type,
                        )
                        .with_build_right(self.build_right);
                        hj.probe_runtime_filter = self.probe_runtime_filter.clone();
                        hj.probe_runtime_filter_pair = self.probe_runtime_filter_pair;
                        hj.set_retained(self.retained.clone());
                        Arc::new(hj)
                    };
                    Ok::<_, QueryError>(BuildDecision::InMemory(hash_join))
                } else {
                    Ok(BuildDecision::Spill { build_batches })
                }
            })
            .await?;

        match decision {
            BuildDecision::InMemory(hash_join) => {
                // Delegate directly — HashJoinExec has its own OnceCell for the hash table
                hash_join.execute(partition).await
            }
            BuildDecision::Spill { build_batches } => {
                // Spill path runs everything through partition 0
                if partition > 0 {
                    return Ok(Box::pin(stream::empty()));
                }
                self.execute_spill_path(build_batches.clone(), probe_side, swapped)
                    .await
            }
        }
    }

    fn name(&self) -> &str {
        "SpillableHashJoin"
    }
}

impl SpillableHashJoinExec {
    /// Execute the spill path when build side exceeds memory limit.
    /// Only called from partition 0.
    async fn execute_spill_path(
        &self,
        build_batches: Vec<RecordBatch>,
        probe_side: &Arc<dyn PhysicalOperator>,
        swapped: bool,
    ) -> Result<RecordBatchStream> {
        // The partitioned spill path currently implements INNER join semantics
        // only (probe_partition ignores join_type). Producing silently-wrong
        // results for other join types is worse than failing, and falling back
        // to the in-memory join would risk OOM. Fail loudly until the streaming
        // spill rewrite implements the remaining join types.
        if !matches!(self.join_type, JoinType::Inner) {
            return Err(QueryError::Execution(format!(
                "{} join build side exceeds the memory budget, but the join spill \
                 path currently supports only INNER joins. Raise the memory limit \
                 for this query.",
                self.join_type
            )));
        }
        // The spill path also ignores the join filter (the non-equi part of an
        // ON clause). Inner joins are lowered with a post-filter above the join
        // so they never carry one, but never emit unfiltered rows if that ever
        // changes.
        if self.filter.is_some() {
            return Err(QueryError::Execution(
                "join build side exceeds the memory budget, but the join spill \
                 path cannot evaluate an ON-clause filter. Raise the memory \
                 limit for this query."
                    .to_string(),
            ));
        }

        self.config.ensure_spill_dir()?;

        let spill_id = SPILL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let spill_dir = self.config.spill_path.join(format!("join_0_{}", spill_id));
        std::fs::create_dir_all(&spill_dir).map_err(|e| {
            QueryError::Execution(format!("Failed to create spill directory: {}", e))
        })?;

        // QE_SPILL_DEBUG tracing (task 001, spill-join-correctness epic):
        // `execute_spill_path` recomputes its ENTIRE result from scratch on
        // every call and has no cache of its own output (unlike
        // `build_decision`, which IS memoized). If some caller ever invokes
        // it more than once for what should be a single logical query
        // execution (e.g. `SpillableHashAggregateExec`'s fused-streaming
        // path aborting and falling back to
        // `collect_input_partitions_concurrently`, which re-executes its
        // input), this prints TWO START/DONE pairs for the SAME join
        // instead of one — direct evidence, not inference. Zero cost when
        // unset beyond one env lookup per call.
        let sj_trace = std::env::var("QE_SPILL_DEBUG").is_ok();
        let sj_t0 = std::time::Instant::now();
        let build_rows_in: usize = build_batches.iter().map(|b| b.num_rows()).sum();
        if sj_trace {
            eprintln!(
                "[sj-trace] execute_spill_path START spill_id={} build_batches={} build_rows={}",
                spill_id,
                build_batches.len(),
                build_rows_in
            );
        }

        let (on_left, on_right): (Vec<_>, Vec<_>) = self.on.iter().cloned().unzip();
        let build_keys = if swapped { &on_right } else { &on_left };
        let probe_keys = if swapped { &on_left } else { &on_right };

        let build_stream: RecordBatchStream =
            Box::pin(stream::iter(build_batches.into_iter().map(Ok)));

        // Build phase with partitioning
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

        if sj_trace {
            let in_mem_parts = in_memory_partitions.iter().filter(|p| p.is_some()).count();
            let spilled_parts = spilled_partitions.iter().filter(|p| p.is_some()).count();
            let in_mem_build_rows: usize = in_memory_partitions
                .iter()
                .flatten()
                .flat_map(|p| p.batches.iter())
                .map(|b| b.num_rows())
                .sum();
            let spilled_build_rows: usize = spilled_partitions
                .iter()
                .flatten()
                .map(|sp| sp.build_rows)
                .sum();
            eprintln!(
                "[sj-trace] execute_spill_path spill_id={} build partitioned: in_memory_partitions={} (rows={}) spilled_partitions={} (rows={})",
                spill_id, in_mem_parts, in_mem_build_rows, spilled_parts, spilled_build_rows
            );
        }

        // Collect ALL probe-side partitions into a single stream
        let probe_partitions = probe_side.output_partitions().max(1);
        let mut probe_batches = Vec::new();
        for p in 0..probe_partitions {
            let probe_stream = probe_side.execute(p).await?;
            let batches: Vec<RecordBatch> = probe_stream.try_collect().await?;
            probe_batches.extend(batches);
        }
        let probe_rows_in: usize = probe_batches.iter().map(|b| b.num_rows()).sum();
        if sj_trace {
            eprintln!(
                "[sj-trace] execute_spill_path spill_id={} probe collected: probe_partitions={} probe_rows={}",
                spill_id, probe_partitions, probe_rows_in
            );
        }
        let probe_stream: RecordBatchStream =
            Box::pin(stream::iter(probe_batches.into_iter().map(Ok)));
        let (results, mut probe_spill_files) = self
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
        let in_memory_matched_rows: usize = results.iter().map(|b| b.num_rows()).sum();

        // Attach each partition's probe spill file before processing: without
        // this, spilled build partitions are probed against an EMPTY probe side
        // and every one of their matches is silently dropped.
        let mut spilled_partitions = spilled_partitions;
        for (idx, spilled) in spilled_partitions.iter_mut().enumerate() {
            if let Some(sp) = spilled {
                sp.probe_file = probe_spill_files[idx].take();
            }
        }

        // Process spilled partitions
        let mut all_results = results;
        let mut spilled_matched_rows: usize = 0;
        for (idx, spilled) in spilled_partitions.iter().enumerate() {
            if let Some(sp) = spilled {
                let spilled_results = self
                    .process_spilled_partition(sp, build_keys, probe_keys, swapped, idx)
                    .await?;
                spilled_matched_rows += spilled_results.iter().map(|b| b.num_rows()).sum::<usize>();
                all_results.extend(spilled_results);
            }
        }

        // Clean up spill directory
        let _ = std::fs::remove_dir_all(&spill_dir);

        if sj_trace {
            let total_matched: usize = all_results.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "[sj-trace] execute_spill_path DONE spill_id={} in_memory_matched={} spilled_matched={} total_matched={} elapsed={:?}",
                spill_id, in_memory_matched_rows, spilled_matched_rows, total_matched, sj_t0.elapsed()
            );
        }

        Ok(Box::pin(stream::iter(all_results.into_iter().map(Ok))))
    }

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
    ) -> Result<(Vec<RecordBatch>, Vec<Option<PathBuf>>)> {
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
                                self.retained.as_deref(),
                            )?;
                            results.extend(matched);
                        }
                    } else if spilled_partitions[idx].is_some() {
                        // Spill probe batch for this partition
                        self.memory_pool.record_spill(estimate_batch_size(&pb));
                        let probe_path = probe_spill_files[idx].get_or_insert_with(|| {
                            spill_dir.join(format!("probe_{}.parquet", idx))
                        });
                        append_to_parquet(probe_path, &pb)?;
                    }
                }
            }
        }

        Ok((results, probe_spill_files))
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
            self.retained.as_deref(),
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
    /// See [`Self::with_disjoint_groups`].
    disjoint_hint: bool,
    input: Arc<dyn PhysicalOperator>,
    group_by: Vec<Expr>,
    aggregates: Vec<AggregateExpr>,
    schema: SchemaRef,
    memory_pool: SharedMemoryPool,
    config: ExecutionConfig,
    /// HAVING predicate applied per output batch (references output columns)
    post_filter: Option<Expr>,
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
            post_filter: None,
            disjoint_hint: false,
        }
    }

    /// Planner hint: hash-partition the fused-agg input to per-worker
    /// channels (disjoint states, trivial finalize). Set only when the group
    /// key's statistics predict the shared-channel merge will pay a large
    /// overlap penalty — a DENSE integer key space (range ≈ NDV) with
    /// millions of groups, where every worker's partial state spans the
    /// whole range. Q13's c_custkey (NDV=range=15M): shared merge 4.3s,
    /// disjoint 0.1ms. Sparse keys (Q18's l_orderkey, range 4x NDV) LOSE
    /// under scatter (+1.3s), so the default stays shared.
    pub fn with_disjoint_groups(mut self, disjoint: bool) -> Self {
        self.disjoint_hint = disjoint;
        self
    }

    pub fn with_post_filter(mut self, post_filter: Option<Expr>) -> Self {
        self.post_filter = post_filter;
        self
    }

    /// The fused streaming path supports the simple accumulator functions the
    /// morsel aggregation core implements.
    fn fused_streaming_eligible(&self) -> bool {
        use crate::planner::AggregateFunction as F;
        !self.group_by.is_empty()
            && !self.aggregates.is_empty()
            && self.aggregates.iter().all(|a| {
                !a.distinct
                    && a.second_arg.is_none()
                    && matches!(
                        a.func,
                        F::Count | F::Sum | F::Min | F::Max | F::Avg | F::AnyValue | F::Arbitrary
                    )
            })
    }

    /// Stream every input partition into a bounded channel consumed by
    /// balanced aggregation worker threads (work distribution is by
    /// availability, not by partition, so skewed partitions don't serialize).
    /// Returns Ok(None) to fall back to the materializing path when a worker
    /// can't process a batch or the group-count budget trips — the input is
    /// re-executed there.
    async fn execute_fused_streaming(&self) -> Result<Option<RecordBatchStream>> {
        use crate::physical::morsel_agg::{merge_states_to_batches, AggregationState};
        use crate::planner::AggregateFunction;
        use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

        let plan_schema =
            crate::planner::PlanSchema::from_qualified_arrow(self.input.schema().as_ref());
        let input_types: Vec<arrow::datatypes::DataType> = self
            .aggregates
            .iter()
            .map(|a| {
                a.input
                    .data_type(&plan_schema)
                    .unwrap_or(arrow::datatypes::DataType::Float64)
            })
            .collect();
        let agg_funcs: Vec<AggregateFunction> = self.aggregates.iter().map(|a| a.func).collect();
        let agg_inputs: Vec<Expr> = self.aggregates.iter().map(|a| a.input.clone()).collect();
        let timing = std::env::var("AGG_TIMING").is_ok();
        // QE_SPILL_DEBUG tracing (task 001, spill-join-correctness epic): see
        // the identical-purpose comment on `execute_spill_path`. This
        // function's own doc comment already states it may fall back to
        // `Ok(None)` and have its input RE-EXECUTED by the caller
        // (`SpillableHashAggregateExec::execute`'s
        // `collect_input_partitions_concurrently` path) — tracing here
        // shows directly whether that fallback is actually taken, and why.
        let sj_trace = std::env::var("QE_SPILL_DEBUG").is_ok();
        let sj_call_id = next_sj_trace_id();
        let t_start = std::time::Instant::now();
        let busy_ns = Arc::new(std::sync::atomic::AtomicU64::new(0));

        // Group-state budget: a worker aborts the fused attempt if its state
        // estimate exceeds an equal share of the memory budget.
        let n_workers = rayon::current_num_threads().clamp(2, 32);
        let per_group_bytes = 64 + 48 * self.aggregates.len();
        let memory_threshold =
            (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;
        let group_limit = ((memory_threshold / n_workers) / per_group_bytes).max(64);

        // Grouped aggregates hash-partition batches to PER-WORKER channels so
        // every worker owns a disjoint key subset: the finalize is then a
        // parallel per-state build instead of a full shard merge. With one
        // shared channel, 32 workers each accumulated partials over the WHOLE
        // key space and the merge paid the overlap (Q13 at SF=100: 126M
        // partial slots for 15M real groups, 4.3s of a 7.5s query). Global
        // aggregates (no GROUP BY) keep the shared channel — every row is one
        // group, and partitioning would serialize onto one worker.
        let disjoint = self.disjoint_hint && !self.group_by.is_empty();
        let mut txs: Vec<crossbeam::channel::Sender<RecordBatch>> = Vec::with_capacity(n_workers);
        let mut rxs: Vec<crossbeam::channel::Receiver<RecordBatch>> = Vec::with_capacity(n_workers);
        if disjoint {
            for _ in 0..n_workers {
                let (t, r) = crossbeam::channel::bounded::<RecordBatch>(8);
                txs.push(t);
                rxs.push(r);
            }
        } else {
            let (t, r) = crossbeam::channel::bounded::<RecordBatch>(n_workers * 8);
            txs.push(t);
            rxs.push(r);
        }
        let abort = Arc::new(AtomicBool::new(false));
        // QE_SPILL_DEBUG tracing (task 001, spill-join-correctness epic):
        // `abort` alone doesn't say WHY — capture the first worker-side
        // error's message so a caught abort is diagnosable, not just
        // detected. `Mutex<Option<String>>` rather than a second atomic
        // flag/enum: the interesting content is the error text itself.
        let abort_reason: Arc<std::sync::Mutex<Option<String>>> =
            Arc::new(std::sync::Mutex::new(None));

        // Aggregation workers: dedicated OS threads pulling from the channel.
        let mut workers = Vec::with_capacity(n_workers);
        for w in 0..n_workers {
            let rx = if disjoint {
                rxs[w].clone()
            } else {
                rxs[0].clone()
            };
            let abort = Arc::clone(&abort);
            let abort_reason = Arc::clone(&abort_reason);
            let agg_funcs = agg_funcs.clone();
            let input_types = input_types.clone();
            let agg_inputs = agg_inputs.clone();
            let group_by = self.group_by.clone();
            let busy_ns = Arc::clone(&busy_ns);
            workers.push(std::thread::spawn(move || {
                let mut state = AggregationState::new(agg_funcs, input_types);
                let mut batches_seen = 0usize;
                while let Ok(batch) = rx.recv() {
                    if abort.load(AtomicOrdering::Relaxed) {
                        continue; // keep draining so senders never block forever
                    }
                    let t = std::time::Instant::now();
                    if let Err(e) = state.process_batch(&batch, &group_by, &agg_inputs) {
                        if sj_trace {
                            let mut guard = abort_reason.lock().unwrap();
                            if guard.is_none() {
                                *guard = Some(format!(
                                    "worker {} process_batch error after {} batches: {}",
                                    w, batches_seen, e
                                ));
                            }
                        }
                        abort.store(true, AtomicOrdering::Relaxed);
                        continue;
                    }
                    busy_ns.fetch_add(t.elapsed().as_nanos() as u64, AtomicOrdering::Relaxed);
                    batches_seen += 1;
                    if batches_seen % 16 == 0 && state.group_count() > group_limit {
                        abort.store(true, AtomicOrdering::Relaxed);
                    }
                }
                if state.group_count() > group_limit {
                    abort.store(true, AtomicOrdering::Relaxed);
                }
                if std::env::var("QE_WORKER_DEBUG").is_ok() {
                    eprintln!(
                        "[fused-worker] batches={} groups={}",
                        batches_seen,
                        state.group_count()
                    );
                }
                state
            }));
        }
        drop(rxs);

        // Drain tasks: one per input partition. Disjoint mode partitions each
        // batch by group-key hash and routes piece i to worker i's channel;
        // the scatter runs on the drain task, so it parallelizes across input
        // partitions.
        let input_partitions = self.input.output_partitions().max(1);
        if sj_trace {
            eprintln!(
                "[sj-trace] execute_fused_streaming START call_id={} input_partitions={} disjoint={}",
                sj_call_id, input_partitions, disjoint
            );
        }
        let mut drains = Vec::with_capacity(input_partitions);
        for p in 0..input_partitions {
            let input = self.input.clone();
            let txs = txs.clone();
            let abort = Arc::clone(&abort);
            let group_by = self.group_by.clone();
            drains.push(tokio::spawn(async move {
                let mut coalesce: Vec<(usize, Vec<RecordBatch>)> =
                    (0..txs.len()).map(|_| (0, Vec::new())).collect();
                let mut stream = input.execute(p).await?;
                while let Some(batch) = stream.try_next().await? {
                    if abort.load(AtomicOrdering::Relaxed) {
                        break;
                    }
                    let pieces: Vec<(usize, RecordBatch)> = if disjoint {
                        // Coalesce per-worker pieces before sending: an
                        // 8192-row batch split 32 ways is 256-row slivers,
                        // and per-batch costs in process_batch (expr eval
                        // setup, hash-table probes' setup) tripled worker
                        // busy time when slivers went out directly.
                        let mut out: Vec<(usize, RecordBatch)> = Vec::new();
                        for (i, b) in partition_batch_by_hash(&batch, &group_by, txs.len())?
                            .into_iter()
                            .enumerate()
                        {
                            let Some(b) = b else { continue };
                            if b.num_rows() == 0 {
                                continue;
                            }
                            let (rows, bufd) = &mut coalesce[i];
                            *rows += b.num_rows();
                            bufd.push(b);
                            if *rows >= 8_192 {
                                let merged =
                                    arrow::compute::concat_batches(&bufd[0].schema(), bufd.iter())
                                        .map_err(|e| QueryError::Execution(e.to_string()))?;
                                bufd.clear();
                                *rows = 0;
                                out.push((i, merged));
                            }
                        }
                        out
                    } else {
                        vec![(0, batch)]
                    };
                    if pieces.is_empty() {
                        continue;
                    }
                    // Bounded sends provide backpressure; run them off the
                    // async reactor so a full channel doesn't stall others.
                    let txs2 = txs.clone();
                    let send_res = tokio::task::spawn_blocking(move || {
                        for (i, piece) in pieces {
                            if txs2[i].send(piece).is_err() {
                                return true;
                            }
                        }
                        false
                    })
                    .await;
                    match send_res {
                        Ok(false) => {}
                        _ => break, // channel closed or join error
                    }
                }
                // Flush the per-worker coalescing buffers.
                let mut tail: Vec<(usize, RecordBatch)> = Vec::new();
                for (i, (rows, bufd)) in coalesce.iter_mut().enumerate() {
                    if *rows > 0 {
                        let merged = arrow::compute::concat_batches(&bufd[0].schema(), bufd.iter())
                            .map_err(|e| QueryError::Execution(e.to_string()))?;
                        bufd.clear();
                        tail.push((i, merged));
                    }
                }
                if !tail.is_empty() && !abort.load(AtomicOrdering::Relaxed) {
                    let txs2 = txs.clone();
                    let _ = tokio::task::spawn_blocking(move || {
                        for (i, piece) in tail {
                            if txs2[i].send(piece).is_err() {
                                return;
                            }
                        }
                    })
                    .await;
                }
                Ok::<_, QueryError>(())
            }));
        }
        drop(txs);

        let mut drain_failed = false;
        let mut first_drain_err: Option<String> = None;
        for (p, d) in drains.into_iter().enumerate() {
            match d.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    drain_failed = true;
                    if sj_trace && first_drain_err.is_none() {
                        first_drain_err = Some(format!("drain task p={} returned Err: {}", p, e));
                    }
                }
                Err(join_err) => {
                    drain_failed = true;
                    if sj_trace && first_drain_err.is_none() {
                        first_drain_err = Some(format!(
                            "drain task p={} join error (panic?): {}",
                            p, join_err
                        ));
                    }
                }
            }
        }
        let t_drained = t_start.elapsed();

        let mut states = Vec::with_capacity(workers.len());
        let mut first_worker_join_err: Option<String> = None;
        for (w, worker) in workers.into_iter().enumerate() {
            match worker.join() {
                Ok(state) => states.push(state),
                Err(e) => {
                    drain_failed = true;
                    if sj_trace && first_worker_join_err.is_none() {
                        let msg = e
                            .downcast_ref::<&str>()
                            .map(|s| s.to_string())
                            .or_else(|| e.downcast_ref::<String>().cloned())
                            .unwrap_or_else(|| "<non-string panic payload>".to_string());
                        first_worker_join_err = Some(format!("worker w={} panicked: {}", w, msg));
                    }
                }
            }
        }

        let aborted = abort.load(AtomicOrdering::Relaxed);
        if drain_failed || aborted {
            if sj_trace {
                let group_limit_exceeded = states.iter().any(|s| s.group_count() > group_limit);
                let total_groups_so_far: usize = states.iter().map(|s| s.group_count()).sum();
                let process_batch_reason = abort_reason.lock().unwrap().clone();
                eprintln!(
                    "[sj-trace] execute_fused_streaming ABORTED call_id={} drain_failed={} abort_flag={} \
                     group_limit_exceeded={} group_limit={} states_collected={} total_groups_so_far={} \
                     elapsed={:?} first_drain_err={:?} first_worker_join_err={:?} process_batch_reason={:?} \
                     -> falling back to Ok(None); CALLER WILL RE-EXECUTE THE INPUT \
                     (collect_input_partitions_concurrently) FROM SCRATCH",
                    sj_call_id,
                    drain_failed,
                    aborted,
                    group_limit_exceeded,
                    group_limit,
                    states.len(),
                    total_groups_so_far,
                    t_start.elapsed(),
                    first_drain_err,
                    first_worker_join_err,
                    process_batch_reason
                );
            }
            return Ok(None);
        }

        let t_workers = t_start.elapsed();
        let total_groups: usize = states.iter().map(|s| s.group_count()).sum();
        let mut batches = if disjoint {
            crate::physical::morsel_agg::finalize_disjoint_states(
                states,
                &agg_funcs,
                &input_types,
                &self.schema,
                self.post_filter.as_ref(),
            )?
        } else {
            merge_states_to_batches(states, &agg_funcs, &input_types, &self.schema)?
        };
        let t_merged = t_start.elapsed();
        if !disjoint {
            if let Some(pred) = &self.post_filter {
                batches = crate::physical::operators::filter_batches(batches, pred)?;
            }
        }
        if std::env::var("QE_AGG_PROF").is_ok() {
            use std::sync::atomic::Ordering as O;
            eprintln!(
                "[agg-prof] group-eval: {:.1}ms; agg-eval: {:.1}ms (cumulative across workers)",
                crate::physical::morsel_agg::AGG_PROF_GROUP_NS.load(O::Relaxed) as f64 / 1e6,
                crate::physical::morsel_agg::AGG_PROF_AGGEVAL_NS.load(O::Relaxed) as f64 / 1e6,
            );
        }
        if timing {
            eprintln!(
                "[fused-agg] drain(join+scan+send): {:?}; workers done: {:?}; merge {} state-groups -> out: {:?}; worker busy sum: {:.1}ms; total: {:?}",
                t_drained,
                t_workers,
                total_groups,
                t_merged - t_workers,
                busy_ns.load(AtomicOrdering::Relaxed) as f64 / 1e6,
                t_start.elapsed()
            );
        }
        if sj_trace {
            let out_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "[sj-trace] execute_fused_streaming OK call_id={} total_groups={} out_rows={} elapsed={:?}",
                sj_call_id,
                total_groups,
                out_rows,
                t_start.elapsed()
            );
        }
        Ok(Some(Box::pin(stream::iter(batches.into_iter().map(Ok)))))
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
        // Aggregation always produces a single partition by collecting from all input partitions
        crate::physical::check_partition(self, partition)?;

        // Fused streaming aggregation: input batches flow through a bounded
        // channel into balanced aggregation workers — the input is never
        // materialized (Q09 collected a 133M-row join output before a single
        // group was aggregated) and aggregation overlaps with join output
        // production. Bounded channel + group-count budget keep memory safe;
        // ineligible shapes or a tripped budget fall through to the
        // collect-then-decide path below.
        let fused_eligible = self.fused_streaming_eligible();
        if fused_eligible {
            if let Some(result) = self.execute_fused_streaming().await? {
                return Ok(result);
            }
        }

        // QE_SPILL_DEBUG tracing (task 001, spill-join-correctness epic):
        // reaching here after `fused_eligible` was true means
        // `execute_fused_streaming` returned `Ok(None)` (see its own DONE/
        // ABORTED trace lines) and its input is about to be driven a SECOND
        // time by `collect_input_partitions_concurrently` below — for a
        // child like `SpillableHashJoinExec`'s spill path, which has no
        // cache of its own output, this reruns the ENTIRE join computation
        // from scratch. Not proof of duplication by itself (this second run
        // is the one whose results are actually returned), but a query
        // whose log shows this line is a query where the join's expensive
        // work happened twice, and a wrong answer that also shows two
        // `execute_spill_path` DONE lines is direct evidence they share a
        // cause.
        if std::env::var("QE_SPILL_DEBUG").is_ok() {
            eprintln!(
                "[sj-trace] agg fallback: fused_eligible={} -> (re-)executing input via \
                 collect_input_partitions_concurrently",
                fused_eligible
            );
        }

        // Drain all input partitions concurrently so a parallel scan/join beneath this
        // aggregate is not serialized onto a single core.
        let memory_threshold =
            (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;
        let (all_batches, total_size) = collect_input_partitions_concurrently(&self.input).await?;
        let exceeded = total_size > memory_threshold;

        if !exceeded {
            // Data fits in memory — delegate to the proven HashAggregateExec
            let hash_aggs: Vec<crate::physical::operators::hash_agg::AggregateExpr> = self
                .aggregates
                .iter()
                .map(|a| crate::physical::operators::hash_agg::AggregateExpr {
                    func: a.func,
                    input: a.input.clone(),
                    distinct: a.distinct,
                    second_arg: a.second_arg.clone(),
                })
                .collect();
            let mem = crate::physical::operators::MemoryTableExec::new(
                "agg_input",
                all_batches
                    .first()
                    .map(|b| b.schema())
                    .unwrap_or_else(|| self.schema.clone()),
                all_batches,
                None,
            );
            let hash_agg = crate::physical::operators::HashAggregateExec::new(
                Arc::new(mem),
                self.group_by.clone(),
                hash_aggs,
                self.schema.clone(),
            );
            let stream = hash_agg.execute(0).await?;
            if let Some(pred) = &self.post_filter {
                let batches: Vec<RecordBatch> = stream.try_collect().await?;
                let batches = crate::physical::operators::filter_batches(batches, pred)?;
                return Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))));
            }
            return Ok(stream);
        }

        // Data exceeds memory — use spillable aggregation path
        self.config.ensure_spill_dir()?;

        let spill_id = SPILL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let spill_dir = self
            .config
            .spill_path
            .join(format!("agg_{}_{}", partition, spill_id));
        std::fs::create_dir_all(&spill_dir).map_err(|e| {
            QueryError::Execution(format!("Failed to create spill directory: {}", e))
        })?;

        if std::env::var("QE_SPILL_DEBUG").is_ok() {
            eprintln!(
                "[spill-agg] collected {} batches, {} rows, {} bytes",
                all_batches.len(),
                all_batches.iter().map(|b| b.num_rows()).sum::<usize>(),
                total_size
            );
        }
        let input_stream: RecordBatchStream =
            Box::pin(stream::iter(all_batches.into_iter().map(Ok)));

        // Process with partitioning and spilling
        let (in_memory_partitions, spilled_files) = self
            .aggregate_with_spilling(input_stream, &spill_dir)
            .await?;
        if std::env::var("QE_SPILL_DEBUG").is_ok() {
            let mem_rows: usize = in_memory_partitions
                .iter()
                .flatten()
                .map(|p| p.batches.iter().map(|b| b.num_rows()).sum::<usize>())
                .sum();
            eprintln!(
                "[spill-agg] in-memory rows {}, spilled files {}",
                mem_rows,
                spilled_files.iter().flatten().count()
            );
        }

        // Each partition holds RAW input rows hash-partitioned by group key, so
        // a given group lives in exactly one partition — possibly split between
        // a spill file and the in-memory remainder that accumulated after the
        // eviction. Aggregate each partition's spilled + in-memory rows TOGETHER,
        // once. Partition results are then disjoint group sets and are simply
        // concatenated. (Re-aggregating partial outputs with the original
        // functions is wrong: COUNT over partial counts returns the number of
        // partials, AVG over partial averages loses the weights.)
        //
        // With an empty GROUP BY every row hashes to the same partition, so the
        // single-result concatenation below is also correct for global aggregates.
        let agg_exprs: Vec<crate::physical::operators::hash_agg::AggregateExpr> = self
            .aggregates
            .iter()
            .map(|a| crate::physical::operators::hash_agg::AggregateExpr {
                func: a.func,
                input: a.input.clone(),
                distinct: a.distinct,
                second_arg: a.second_arg.clone(),
            })
            .collect();

        let mut all_results = Vec::new();
        for (idx, part) in in_memory_partitions.into_iter().enumerate() {
            let mut batches: Vec<RecordBatch> = Vec::new();
            if let Some(path) = &spilled_files[idx] {
                batches.extend(read_parquet(path)?);
            }
            if let Some(part) = part {
                batches.extend(part.batches);
            }
            if batches.is_empty() {
                continue;
            }
            let result = crate::physical::operators::hash_agg::aggregate_batches_external(
                &batches,
                &self.group_by,
                &agg_exprs,
                &self.schema,
            )?;
            if result.num_rows() > 0 {
                all_results.push(result);
            }
        }

        // Clean up spill directory
        let _ = std::fs::remove_dir_all(&spill_dir);

        if all_results.is_empty() {
            // Return empty result with correct schema
            let empty_batch = RecordBatch::new_empty(self.schema.clone());
            return Ok(Box::pin(stream::once(async { Ok(empty_batch) })));
        }

        let all_results = if let Some(pred) = &self.post_filter {
            crate::physical::operators::filter_batches(all_results, pred)?
        } else {
            all_results
        };
        Ok(Box::pin(stream::iter(all_results.into_iter().map(Ok))))
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

        while let Some(big) = input_stream.try_next().await? {
            // Re-chunk over-large batches (an mmap-backed IPC scan emits
            // 65536-row slabs) into zero-copy slices. Spill accounting
            // decides BEFORE each addition; fed one slab bigger than the
            // whole budget it could decide nothing — the answer stayed
            // correct but the budget became advisory.
            let mut chunks: Vec<RecordBatch> = Vec::new();
            const CHUNK_ROWS: usize = 8_192;
            if big.num_rows() > CHUNK_ROWS {
                let mut off = 0;
                while off < big.num_rows() {
                    let len = CHUNK_ROWS.min(big.num_rows() - off);
                    chunks.push(big.slice(off, len));
                    off += len;
                }
            } else {
                chunks.push(big);
            }

            for batch in chunks {
                let batch_size = estimate_batch_size(&batch);

                // Check if we need to spill before adding more data
                if total_memory + batch_size > memory_threshold {
                    // Find the largest partition to spill
                    if let Some(idx) = find_largest_agg_partition(&partitions) {
                        if let Some(ref mut part) = partitions[idx] {
                            if !part.batches.is_empty() {
                                // Spill this partition
                                let spill_path = spill_dir.join(format!(
                                    "part_{}_{}.parquet",
                                    idx, spill_file_counts[idx]
                                ));
                                spill_file_counts[idx] += 1;

                                write_batches_to_parquet(&spill_path, &part.batches)?;
                                self.memory_pool.record_spill(part.memory_bytes);
                                total_memory -= part.memory_bytes;

                                // If we already have a spill file for this partition, merge them
                                if let Some(ref existing_path) = spilled_files[idx] {
                                    // Append new file path to a list file or merge
                                    // For simplicity, we'll just keep the latest and merge on read
                                    merge_parquet_files(
                                        existing_path,
                                        &spill_path,
                                        spill_dir,
                                        idx,
                                    )?;
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
    /// Optional limit for Top-K optimization
    fetch: Option<usize>,
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
            fetch: None,
        }
    }

    /// Create an external sort with a fetch limit (Top-K optimization).
    pub fn with_fetch(
        input: Arc<dyn PhysicalOperator>,
        order_by: Vec<crate::planner::SortExpr>,
        memory_pool: SharedMemoryPool,
        config: ExecutionConfig,
        fetch: usize,
    ) -> Self {
        let schema = input.schema();
        Self {
            input,
            order_by,
            schema,
            memory_pool,
            config,
            fetch: Some(fetch),
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
        // Sort always produces a single partition
        crate::physical::check_partition(self, partition)?;

        // Drain all input partitions concurrently so a parallel scan/join beneath this
        // sort is not serialized onto a single core.
        let memory_threshold =
            (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;
        let (all_batches, total_size) = collect_input_partitions_concurrently(&self.input).await?;
        let exceeded = total_size > memory_threshold;

        if all_batches.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        if !exceeded {
            // Data fits in memory — use the regular SortExec path for correctness
            // Create a temporary MemoryTableExec with our already-collected data
            let mem = crate::physical::operators::MemoryTableExec::new(
                "sort_input",
                self.schema.clone(),
                all_batches,
                None,
            );
            let sort = if let Some(fetch) = self.fetch {
                crate::physical::operators::SortExec::with_fetch(
                    Arc::new(mem),
                    self.order_by.clone(),
                    fetch,
                )
            } else {
                crate::physical::operators::SortExec::new(Arc::new(mem), self.order_by.clone())
            };
            return sort.execute(0).await;
        }

        // Data exceeds memory — use external sort with spilling
        self.config.ensure_spill_dir()?;

        let spill_id = SPILL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let spill_dir = self
            .config
            .spill_path
            .join(format!("sort_{}_{}", partition, spill_id));
        std::fs::create_dir_all(&spill_dir).map_err(|e| {
            QueryError::Execution(format!("Failed to create spill directory: {}", e))
        })?;

        let input_stream: RecordBatchStream =
            Box::pin(stream::iter(all_batches.into_iter().map(Ok)));

        // Generate sorted runs
        let runs = self.generate_runs(input_stream, &spill_dir).await?;

        // Merge runs
        let result = if runs.is_empty() {
            Vec::new()
        } else if runs.len() == 1 {
            if runs[0].is_file() {
                read_parquet(&runs[0])?
            } else {
                Vec::new()
            }
        } else {
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

        // Flush remaining buffer. This is still a spill — the run is written to
        // disk — so it must be recorded, otherwise a sort whose input arrives in
        // a single batch spills without ever reporting it in QueryMetrics.
        if !buffer.is_empty() {
            let run_path = spill_dir.join(format!("run_{}.parquet", runs.len()));
            self.flush_run(&buffer, &run_path)?;
            runs.push(run_path);

            self.memory_pool.record_spill(buffer_size);
        }

        Ok(runs)
    }

    fn flush_run(&self, batches: &[RecordBatch], path: &PathBuf) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        // Concatenate under the batches' OWN actual schema, not the
        // declared/logical one (`self.schema`, derived from `PlanSchema` via
        // `plan_schema_to_arrow`, which has no Dictionary representation and
        // so always reports a string column as plain `Utf8`). A sort large
        // enough to spill over a Dictionary-encoded source (native tables;
        // also small-build join gathers, per the identical reasoning already
        // applied to the in-memory path) previously failed outright here
        // with "column types must match schema types, expected Utf8 but
        // found Dictionary(...)" — found by native-tables-mutation task
        // 006's real-scale (SF=10, 15M-row `orders`) cell-exact validation;
        // reproduces on an UNMUTATED native table too, so this is a
        // pre-existing gap in the spill path specifically, not something
        // mutation introduced. Mirrors the exact "if every batch agrees, use
        // their actual schema; otherwise cast Dictionary columns down to
        // their plain value type against the declared schema" pattern
        // `SortExec::execute()` (sort.rs) and `MemoryTableExec::execute()`
        // (scan.rs) already establish for the in-memory sort path.
        let first_schema = batches[0].schema();
        let all_agree = batches.iter().all(|b| b.schema() == first_schema);

        let combined = if all_agree {
            compute::concat_batches(&first_schema, batches)?
        } else {
            let normalized: Result<Vec<RecordBatch>> = batches
                .iter()
                .map(|b| {
                    let cols: std::result::Result<Vec<ArrayRef>, arrow::error::ArrowError> = b
                        .columns()
                        .iter()
                        .map(|c| match c.data_type() {
                            arrow::datatypes::DataType::Dictionary(_, v) => {
                                compute::cast(c.as_ref(), v)
                            }
                            _ => Ok(c.clone()),
                        })
                        .collect();
                    RecordBatch::try_new(self.schema.clone(), cols?).map_err(Into::into)
                })
                .collect();
            compute::concat_batches(&self.schema, &normalized?)?
        };

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
                            // `output_rows` holds (run_idx, row_idx) pairs
                            // where row_idx indexes into `run_buffers
                            // [run_idx]`'s CURRENT in-memory batch — a
                            // reference that goes stale (silently wrong, or
                            // out-of-bounds and panicking) the instant that
                            // slot is overwritten below. A run whose
                            // Parquet file needs more than one
                            // `buffer_rows`-sized read (any spill run
                            // larger than `MERGE_BUFFER_ROWS` = 8192 rows —
                            // the common case for a real spill, not an edge
                            // case) reloads mid-merge, and any row pushed
                            // from an EARLIER load of this exact slot that
                            // had not yet been flushed becomes a dangling
                            // reference: `build_merged_batch` would gather
                            // it from the NEW batch at the OLD row_idx —
                            // silently wrong data if the new batch happens
                            // to be at least that long, or an out-of-bounds
                            // panic ("the len is N but the index is N")
                            // otherwise. Found by native-tables-mutation
                            // task 006's real-scale (SF=10, 15M-row
                            // `orders`) `ORDER BY` validation — a spill
                            // large enough to need >1 run AND any run
                            // >8192 rows reaches this every time; TPC-H's
                            // own suite apparently never spills a sort this
                            // large. Flushing HERE, before the slot is
                            // overwritten, guarantees every row in
                            // `output_rows` always indexes into whichever
                            // batch is CURRENTLY loaded for its run — the
                            // invariant `build_merged_batch` requires.
                            if !output_rows.is_empty() {
                                let batch = self.build_merged_batch(&run_buffers, &output_rows)?;
                                result_batches.push(batch);
                                output_rows.clear();
                            }
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

        // Flush remaining output. With the flush-before-buffer-transition
        // fix above, every run's exhaustion (buffer -> None) flushes
        // `output_rows` first, and the loop only exits once every run has
        // exhausted — so in practice `output_rows` is always already empty
        // here. Kept as a defensive fallback (not proven unreachable by an
        // exhaustive proof, just by construction of the loop above) rather
        // than deleted outright.
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

        batch_with_actual_types(&self.schema, final_columns)
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

        batch_with_actual_types(&self.schema, final_columns)
    }
}

/// Build a `RecordBatch` from columns whose ACTUAL data types may not match
/// `declared`'s (a `plan_schema_to_arrow`-derived schema, which has no
/// Dictionary representation and so always claims a string column is plain
/// `Utf8` even when the real data — native table segments, small-build join
/// gathers, or either round-tripped through this operator's own Parquet
/// spill files, which faithfully preserve Dictionary via the embedded
/// `ARROW:schema` metadata `ArrowWriter`/`ParquetRecordBatchReaderBuilder`
/// already use — is `Dictionary(Int32, Utf8)`). Widens the declared field
/// type to the actual column's type wherever they disagree, mirroring the
/// identical fix already established for the in-memory sort path
/// (`SortExec::execute()` in sort.rs, `MemoryTableExec::execute()`'s
/// `rewrap` in scan.rs) and for joins (`hash_join.rs`'s own
/// `batch_with_actual_types`, not reused directly across the module
/// boundary — this file follows the same local-duplication convention
/// those two other sites already established rather than introducing a
/// new cross-module dependency for a three-line function). Found by
/// native-tables-mutation task 006's real-scale (SF=10, 15M-row `orders`)
/// cell-exact `ORDER BY` validation — reproduces on an unmutated native
/// table too, so this is a pre-existing gap in the external-sort spill
/// path specifically, not something mutation introduced.
fn batch_with_actual_types(declared: &SchemaRef, columns: Vec<ArrayRef>) -> Result<RecordBatch> {
    let types_match = columns
        .iter()
        .zip(declared.fields())
        .all(|(c, f)| c.data_type() == f.data_type());
    let schema = if types_match {
        declared.clone()
    } else {
        Arc::new(Schema::new(
            declared
                .fields()
                .iter()
                .zip(&columns)
                .map(|(f, c)| {
                    if f.data_type() == c.data_type() {
                        f.as_ref().clone()
                    } else {
                        arrow::datatypes::Field::new(f.name(), c.data_type().clone(), true)
                    }
                })
                .collect::<Vec<_>>(),
        ))
    };
    RecordBatch::try_new(schema, columns).map_err(Into::into)
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
/// Bytes a batch LOGICALLY holds, computed slice-aware.
///
/// `get_array_memory_size()` reports the CAPACITY of the underlying buffers,
/// which for an array sliced out of a larger allocation — every batch an
/// mmap-backed IPC read produces — counts the whole mapping per batch. Spill
/// decisions made on that number spill everything unconditionally (and the
/// spilled join's build-side estimate was off by ~50x). Offsets-based sizing
/// counts the rows the batch actually references; unknown types fall back to
/// the capacity number, which over- rather than under-counts (the safe
/// direction for a spill decision).
fn estimate_batch_size(batch: &RecordBatch) -> usize {
    use arrow::array::Array;
    batch
        .columns()
        .iter()
        .map(|c| {
            let rows = c.len();
            let null_bytes = rows.div_ceil(8);
            match c.data_type() {
                t if t.primitive_width().is_some() => {
                    rows * t.primitive_width().unwrap_or(8) + null_bytes
                }
                arrow::datatypes::DataType::Boolean => rows.div_ceil(8) + null_bytes,
                arrow::datatypes::DataType::Utf8 | arrow::datatypes::DataType::Binary => {
                    let data: usize = match c.as_any().downcast_ref::<arrow::array::StringArray>() {
                        Some(a) if rows > 0 => {
                            (a.value_offsets()[rows] - a.value_offsets()[0]) as usize
                        }
                        _ => match c.as_any().downcast_ref::<arrow::array::BinaryArray>() {
                            Some(a) if rows > 0 => {
                                (a.value_offsets()[rows] - a.value_offsets()[0]) as usize
                            }
                            _ => 0,
                        },
                    };
                    data + rows * 4 + null_bytes
                }
                _ => c.get_array_memory_size(),
            }
        })
        .sum()
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
        // EXPLICITLY seeded: partition routing must give the same answer for
        // the same key from every call site. hashbrown 0.14's default hasher
        // happened to be deterministic across instances (ahash with fixed
        // fallback keys); 0.17's foldhash seeds PER INSTANCE, which shattered
        // groups across partitions. Never rely on a default for this.
        let mut hasher = xxhash_rust::xxh64::Xxh64::new(0x517c_c1b7_2722_0a95);
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

#[allow(clippy::too_many_arguments)]
fn probe_partition(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    hash_table: &HashMap<JoinKey, Vec<HashEntry>>,
    probe_key_exprs: &[Expr],
    _join_type: JoinType, // TODO: handle all join types
    swapped: bool,
    output_schema: &SchemaRef,
    retained: Option<&[bool]>,
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
                retained,
            )?;
            results.push(batch);
        }
    }

    Ok(results)
}

#[allow(clippy::too_many_arguments)]
fn create_joined_batch(
    build_batches: &[RecordBatch],
    probe_batch: &RecordBatch,
    build_indices: &[(usize, usize)],
    probe_indices: &[usize],
    swapped: bool,
    output_schema: &SchemaRef,
    retained: Option<&[bool]>,
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

    // Retention mask (join-output pruning): the spill path gathers full
    // width and drops the unreferenced columns here — correctness only,
    // the delegate path is where pruning saves the gather itself.
    let columns: Vec<ArrayRef> = match retained {
        Some(mask) if mask.len() == columns.len() => columns
            .into_iter()
            .zip(mask)
            .filter(|(_, keep)| **keep)
            .map(|(c, _)| c)
            .collect(),
        _ => columns,
    };

    // Same "declared Utf8 vs actual Dictionary" class as
    // `ExternalSortExec`'s spill path above (`output_schema` is a
    // `plan_schema_to_arrow`-derived declared schema; `columns` are
    // GATHERED via `compute::take`, which preserves whatever the build/
    // probe batches' actual encoding is). Found the same way: a real
    // SF=10 query (Q12, whose join output must carry the low-cardinality,
    // Dictionary-coerced `l_shipmode`) spilling this hash join over a
    // native table crashed here with the identical error before this fix.
    batch_with_actual_types(output_schema, columns)
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

    /// Disjoint fused aggregation must produce EXACTLY what the plain
    /// aggregate produces, at a group count high enough that worker states
    /// abandon their perfect-hash arrays mid-build. The first disjoint
    /// finalize skipped the per-state raw prep and silently emitted only the
    /// raw-map groups — Q11 at SF=100 lost ~70% of every SUM while still
    /// returning the right ROW COUNT. Row counts are not answers.
    #[tokio::test]
    async fn disjoint_aggregation_matches_plain_aggregation_exactly() {
        use arrow::array::{Float64Array, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};

        // 3M rows over 300k dense keys: big enough for raw/perfect-hash
        // transitions inside worker states, keyed densely like c_custkey.
        let n_rows = 3_000_000usize;
        let n_keys = 300_000i64;
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Float64, false),
        ]));
        let mut batches = Vec::new();
        for chunk in 0..(n_rows / 8192) {
            let start = chunk * 8192;
            let keys: Vec<i64> = (start..start + 8192)
                .map(|i| (i as i64 * 2654435761i64.wrapping_abs()) % n_keys)
                .collect();
            let vals: Vec<f64> = keys.iter().map(|k| (*k % 97) as f64 + 0.5).collect();
            batches.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(keys)),
                        Arc::new(Float64Array::from(vals)),
                    ],
                )
                .unwrap(),
            );
        }
        let input: Arc<dyn PhysicalOperator> = Arc::new(
            crate::physical::operators::MemoryTableExec::new("t", schema.clone(), batches, None),
        );

        let group_by = vec![Expr::column("k")];
        let aggs = vec![AggregateExpr {
            func: crate::planner::AggregateFunction::Sum,
            input: Expr::column("v"),
            distinct: false,
            second_arg: None,
        }];
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("sum_v", DataType::Float64, true),
        ]));

        let pool = crate::execution::create_memory_pool(8 * 1024 * 1024 * 1024);
        let config = ExecutionConfig::default();

        let mut results = Vec::new();
        for disjoint in [false, true] {
            let agg = SpillableHashAggregateExec::new(
                input.clone(),
                group_by.clone(),
                aggs.clone(),
                out_schema.clone(),
                pool.clone(),
                config.clone(),
            )
            .with_disjoint_groups(disjoint);
            let mut stream = agg.execute(0).await.unwrap();
            let mut rows: Vec<(i64, f64)> = Vec::new();
            use futures::TryStreamExt;
            while let Some(b) = stream.try_next().await.unwrap() {
                let k = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                let v = b.column(1).as_any().downcast_ref::<Float64Array>().unwrap();
                for i in 0..b.num_rows() {
                    rows.push((k.value(i), v.value(i)));
                }
            }
            rows.sort_by(|a, b| a.0.cmp(&b.0));
            results.push(rows);
        }
        assert_eq!(results[0].len(), results[1].len(), "group count differs");
        for (a, b) in results[0].iter().zip(results[1].iter()) {
            assert_eq!(a.0, b.0, "group keys diverge");
            assert!(
                (a.1 - b.1).abs() < 1e-6,
                "sum for key {} differs: {} vs {}",
                a.0,
                a.1,
                b.1
            );
        }
    }

    /// Gate C (`SpillableHashJoinExec::set_retained`) must never diverge
    /// from Gate B (`HashJoinExec::set_retained`). The wrapper narrows its
    /// OWN `schema()` to the retained mask immediately in `set_retained`,
    /// then separately hands the exact same mask to the inner
    /// `HashJoinExec` it delegates to for the in-memory build path
    /// (`hj.set_retained(self.retained.clone())`). If that inner gate ever
    /// declined a mask this wrapper's gate accepted, `self.schema()` would
    /// promise fewer columns than the delegate's stream actually returns —
    /// a unit test on `HashJoinExec` alone can never see this, because it
    /// never goes through the wrapper's OWN (separate) gate check.
    #[tokio::test]
    async fn spillable_hash_join_retained_mask_matches_delegate_schema() {
        use arrow::array::Array;
        use arrow::datatypes::{DataType, Field, Schema};
        use futures::TryStreamExt;

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("keep_left", DataType::Int64, false),
            Field::new("filter_left", DataType::Int64, false),
            Field::new("drop_left", DataType::Int64, false),
        ]));
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50, 60])),
                // Odd ids pass the ON-filter (>5), even ids fail it — a
                // build-side (left) column referenced ONLY by the filter,
                // never selected downstream: without force-keep this would
                // be pruned away before the filter can evaluate it.
                Arc::new(Int64Array::from(vec![10, 3, 10, 3, 10, 3])),
                Arc::new(Int64Array::from(vec![111, 222, 333, 444, 555, 666])),
            ],
        )
        .unwrap();

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("rid", DataType::Int64, false),
            Field::new("keep_right", DataType::Int64, false),
            Field::new("drop_right", DataType::Int64, false),
        ]));
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5, 6])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 400, 500, 600])),
                Arc::new(Int64Array::from(vec![999, 998, 997, 996, 995, 994])),
            ],
        )
        .unwrap();

        let left: Arc<dyn PhysicalOperator> =
            Arc::new(crate::physical::operators::MemoryTableExec::new(
                "left_t",
                left_schema,
                vec![left_batch],
                None,
            ));
        let right: Arc<dyn PhysicalOperator> =
            Arc::new(crate::physical::operators::MemoryTableExec::new(
                "right_t",
                right_schema,
                vec![right_batch],
                None,
            ));

        let pool = crate::execution::create_memory_pool(64 * 1024 * 1024);
        let config = ExecutionConfig::default();
        let mut join = SpillableHashJoinExec::new(
            left,
            right,
            vec![(Expr::column("id"), Expr::column("rid"))],
            JoinType::Left,
            pool,
            config,
        )
        .with_filter(Some(
            Expr::column("filter_left").gt(Expr::literal(crate::planner::ScalarValue::Int64(5))),
        ));

        // Force-keep (filter_left, referenced only by the ON predicate) +
        // downstream need (keep_left, keep_right); drop the join keys and
        // the two never-referenced columns. Order: id, keep_left,
        // filter_left, drop_left, rid, keep_right, drop_right.
        let mask = vec![false, true, true, false, false, true, false];
        join.set_retained(Some(mask));

        assert_eq!(
            join.schema().fields().len(),
            3,
            "wrapper schema should already reflect the retained mask"
        );
        let join_schema = join.schema();
        let field_names: Vec<&str> = join_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(field_names, vec!["keep_left", "filter_left", "keep_right"]);

        let mut stream = join.execute(0).await.unwrap();
        let mut rows: Vec<(i64, i64, Option<i64>)> = Vec::new();
        while let Some(batch) = stream.try_next().await.unwrap() {
            // Gate B/Gate C lockstep check: the ACTUAL batch schema must
            // match the wrapper's advertised (already-narrowed) schema, in
            // both width and field order/names — this is what would fail
            // (column-count mismatch) if the two gates ever disagreed.
            assert_eq!(
                batch.schema().fields().len(),
                join.schema().fields().len(),
                "delegate produced a different column count than the wrapper's schema() promised"
            );
            for (a, b) in batch.schema().fields().iter().zip(join.schema().fields()) {
                assert_eq!(a.name(), b.name());
            }
            let keep_left = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let filter_left = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let keep_right = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                rows.push((
                    keep_left.value(i),
                    filter_left.value(i),
                    if keep_right.is_null(i) {
                        None
                    } else {
                        Some(keep_right.value(i))
                    },
                ));
            }
        }
        rows.sort_by_key(|r| r.0);
        assert_eq!(
            rows,
            vec![
                (10, 10, Some(100)),
                (20, 3, None),
                (30, 10, Some(300)),
                (40, 3, None),
                (50, 10, Some(500)),
                (60, 3, None),
            ],
            "Left join + force-kept filter column + pruned join keys/unused columns"
        );
    }

    /// A `Dictionary(Int32, Utf8)`-encoded column reaching `ExternalSortExec`'s
    /// SPILL path (not its in-memory `SortExec` delegate, already exercised
    /// elsewhere) previously failed outright: "column types must match
    /// schema types, expected Utf8 but found Dictionary(Int32, Utf8)".
    /// `flush_run`/`build_merged_batch`/`build_merged_batch_final` all
    /// constructed their output `RecordBatch` against `self.schema` (a
    /// `plan_schema_to_arrow`-derived DECLARED schema, which has no
    /// Dictionary representation and so always reports a string column as
    /// plain `Utf8`) instead of the batches' own ACTUAL type. Found by
    /// native-tables-mutation task 006's real-scale `ORDER BY` validation
    /// over a native table (whose low-cardinality string columns are always
    /// Dictionary-encoded) large enough to spill; reproduces on an
    /// unmutated native table too, so this is a pre-existing external-sort
    /// gap, not something mutation introduced.
    #[tokio::test]
    async fn external_sort_spill_path_handles_dictionary_encoded_columns() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::{DataType, Field, Int32Type, Schema};

        // DECLARED schema: plain Utf8 for the string column, mirroring
        // `plan_schema_to_arrow`'s own logical->physical mapping (it has no
        // Dictionary variant, so a string column is always declared Utf8
        // regardless of a provider's actual physical encoding).
        let declared_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Utf8, false),
        ]));
        // ACTUAL schema of the batches themselves -- what a native table's
        // segments genuinely are on disk. `MemoryTableExec` stores batches
        // and its own declared `schema` independently (reconciling only
        // inside `execute()`), so constructing a batch here must use its
        // OWN real type, exactly like a real provider's scan output would.
        let actual_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new(
                "v",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
        ]));

        // Several batches whose ACTUAL data is Dictionary-encoded (as a
        // native table's low-cardinality string columns always are), keys
        // in descending order so a correct sort must reverse them.
        let mut batches = Vec::new();
        for chunk in 0..4i64 {
            let start = chunk * 4;
            let keys: Vec<i64> = (start..start + 4).rev().collect();
            let dict: DictionaryArray<Int32Type> = keys
                .iter()
                .map(|k| Some(if k % 2 == 0 { "alpha" } else { "beta" }))
                .collect();
            batches.push(
                RecordBatch::try_new(
                    actual_schema.clone(),
                    vec![Arc::new(Int64Array::from(keys)), Arc::new(dict)],
                )
                .unwrap(),
            );
        }

        let input: Arc<dyn PhysicalOperator> = Arc::new(
            crate::physical::operators::MemoryTableExec::new("t", declared_schema, batches, None),
        );

        // A tiny memory budget forces the SPILL branch (not the in-memory
        // SortExec delegate) even for this handful of rows.
        let pool = crate::execution::create_memory_pool(1024);
        let mut config = ExecutionConfig::default();
        config.memory_limit = 1024;
        config.spill_threshold = 0.1;
        config.spill_path =
            std::env::temp_dir().join(format!("qe_sort_dict_spill_test_{}", std::process::id()));

        let sort = ExternalSortExec::new(
            input,
            vec![crate::planner::SortExpr::new(Expr::column("k"))],
            pool,
            config,
        );

        let mut stream = sort.execute(0).await.expect("spilled sort must not error");
        let mut keys = Vec::new();
        while let Some(batch) = stream.try_next().await.expect("collecting sorted output") {
            let k = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            keys.extend(k.values().iter().copied());
            assert!(
                matches!(batch.column(1).data_type(), DataType::Dictionary(_, _)),
                "sorted output column 1 should still be Dictionary-encoded, got {:?}",
                batch.column(1).data_type()
            );
        }
        assert_eq!(keys.len(), 16);
        assert!(
            keys.windows(2).all(|w| w[0] <= w[1]),
            "output must be sorted ascending by k: {keys:?}"
        );
    }

    /// The k-way merge's `output_rows` buffer held `(run_idx, row_idx)`
    /// pairs indexing into `run_buffers[run_idx]`'s CURRENT in-memory
    /// batch — a reference that went stale (silently wrong, or an
    /// out-of-bounds panic) the instant that slot was reloaded before the
    /// pending rows were flushed. Any run whose Parquet file needs more
    /// than one `buffer_rows`-sized read during merge (the common case for
    /// a real spill: any run over `MERGE_BUFFER_ROWS` = 8192 rows) hit
    /// this. Found by native-tables-mutation task 006's real-scale
    /// `ORDER BY` validation: a real 15M-row sort's spilled-run merge
    /// panicked with "index out of bounds: the len is 5329 but the index
    /// is 5329". This test reproduces the same shape at unit-test scale by
    /// calling `streaming_k_way_merge` directly with a tiny `buffer_rows`
    /// so a 10-row run needs multiple reloads to merge.
    #[tokio::test]
    async fn k_way_merge_survives_a_run_needing_more_than_one_buffer_load() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let dir = std::env::temp_dir().join(format!("qe_kway_merge_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // Run A: 10 rows, sorted -- bigger than the buffer_rows=4 this
        // test uses below, so merging it needs MULTIPLE `next()` reads
        // (the reload this bug loses track of).
        let run_a_vals: Vec<i64> = (0..10).map(|i| i * 2).collect(); // 0,2,4,...,18
        let run_a_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(run_a_vals.clone()))],
        )
        .unwrap();
        let run_a_path = dir.join("run_a.parquet");
        write_batches_to_parquet(&run_a_path, &[run_a_batch]).unwrap();

        // Run B: 5 rows, interleaved with (a prefix of) A's values.
        let run_b_vals: Vec<i64> = (0..5).map(|i| i * 2 + 1).collect(); // 1,3,5,7,9
        let run_b_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(run_b_vals.clone()))],
        )
        .unwrap();
        let run_b_path = dir.join("run_b.parquet");
        write_batches_to_parquet(&run_b_path, &[run_b_batch]).unwrap();

        let pool = crate::execution::create_memory_pool(64 * 1024 * 1024);
        let config = ExecutionConfig::default();
        let sort = ExternalSortExec::new(
            Arc::new(crate::physical::operators::MemoryTableExec::new(
                "t",
                schema,
                vec![],
                None,
            )),
            vec![crate::planner::SortExpr::new(Expr::column("k"))],
            pool,
            config,
        );

        // buffer_rows=4: run A (10 rows) needs 3 reloads to merge fully.
        let merged = sort
            .streaming_k_way_merge(&[run_a_path, run_b_path], 4)
            .expect("k-way merge must not crash or lose/misplace rows");

        let mut all_vals: Vec<i64> = Vec::new();
        for batch in &merged {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            all_vals.extend(arr.values().iter().copied());
        }
        let mut expected: Vec<i64> = run_a_vals.into_iter().chain(run_b_vals).collect();
        expected.sort_unstable();
        assert_eq!(
            all_vals, expected,
            "k-way merge must produce every value from both runs, in order, exactly once"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Same "declared Utf8 vs actual Dictionary" bug class as
    /// `ExternalSortExec`'s spill path, in `SpillableHashJoinExec`'s own
    /// spill-path join-output constructor (`create_joined_batch`). Found
    /// by native-tables-mutation task 006's real-scale SF=10 benchmark:
    /// Q12 (whose join output carries the Dictionary-coerced `l_shipmode`)
    /// crashed identically once its join spilled over a native table.
    #[test]
    fn create_joined_batch_handles_dictionary_encoded_columns() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::{DataType, Field, Int32Type, Schema};

        // DECLARED (plan_schema_to_arrow-shaped) output schema: plain
        // Utf8 for the string column.
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("tag", DataType::Utf8, false),
        ]));

        // ACTUAL build-side batch: Dictionary-encoded `tag`, as a native
        // table's low-cardinality string columns always are.
        let build_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "tag",
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                false,
            ),
        ]));
        let dict: DictionaryArray<Int32Type> = vec![Some("alpha"), Some("beta"), Some("alpha")]
            .into_iter()
            .collect();
        let build_batch = RecordBatch::try_new(
            build_schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3])), Arc::new(dict)],
        )
        .unwrap();

        // An empty-schema probe side keeps this test focused on the
        // build-side gather (`compute::take`), which is where the actual
        // Dictionary encoding survives into the joined output.
        let probe_schema = Arc::new(Schema::new(Vec::<Field>::new()));
        let probe_batch = RecordBatch::try_new_with_options(
            probe_schema,
            vec![],
            &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(3)),
        )
        .unwrap();

        let result = create_joined_batch(
            &[build_batch],
            &probe_batch,
            &[(0, 0), (0, 1), (0, 2)],
            &[0, 1, 2],
            false,
            &output_schema,
            None,
        )
        .expect("must not error on a Dictionary-encoded build column");

        assert_eq!(result.num_rows(), 3);
        assert!(
            matches!(result.column(1).data_type(), DataType::Dictionary(_, _)),
            "joined output column 1 should still be Dictionary-encoded, got {:?}",
            result.column(1).data_type()
        );
    }
}
