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
use futures::stream::{self, StreamExt, TryStreamExt};
use hashbrown::HashMap;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::fmt;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::OnceCell;

/// Unique counter for spill directory names to avoid conflicts between concurrent operators
static SPILL_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Number of hash partitions for spilling
const NUM_PARTITIONS: usize = 64;

/// Row granularity for re-chunking oversized batches during spill ingestion
/// AND for streaming a spilled aggregate partition back off disk
/// (`aggregate_partition_chunked`). Matches `append_batch_streaming`'s
/// `SPILL_WRITER_ROW_GROUP_ROWS` — one shared magic number, not three.
const AGG_CHUNK_ROWS: usize = 8_192;

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

// ============================================================================
// Fault injection: forced spill (spill-join-correctness-2 epic, task 003)
// ============================================================================
//
// A reusable, permanent mechanism to force `SpillableHashJoinExec`'s
// spill/unspill machinery to engage at a CHOSEN, controllable point in
// build/probe, decoupled from whether the data is actually oversized — the
// harness this enables (`examples/spill_chaos_harness.rs`) runs a query once
// with this forced and once without, and differentially compares the two
// results. Same style as this file's own `QE_SPILL_DEBUG` and the archived
// `spill-join-correctness` epic's own `QE_SPILL_CHAOS_FORCE_ABORT`: plain,
// explicitly-opt-in env vars, checked fresh (never cached) at each call site,
// zero cost and zero behavior change unless a caller sets them.
//
// Two independent, orthogonal levers:
//
// 1. `QE_SPILL_CHAOS_FORCE_SPILL` (`compute_build_decision`, WHEN): forces
//    the top-level build/no-build decision to take the disk-spill branch
//    (`BuildDecision::Spill`) after a CHOSEN number of build batches have
//    already been collected flat (0 = force on the very first batch),
//    regardless of the configured `memory_limit`/`spill_threshold` or how
//    much data is actually present. On its own this only guarantees the
//    `BuildDecision::Spill` CODE PATH is taken — whether any individual
//    partition's rows actually touch disk still depends on memory pressure
//    inside `build_with_partitioning`, since a small build side may still
//    fit every partition in memory once inside it.
// 2. `QE_SPILL_CHAOS_FORCE_SPILL_PARTITIONS` (`build_with_partitioning`,
//    WHICH): forces specific hash partitions — "all", or a comma-separated
//    list of indices in `0..NUM_PARTITIONS` — to write their resident
//    batches to disk the moment they receive their first one, regardless of
//    memory pressure. This is the lever that guarantees a REAL write +
//    read-back round trip happens for chosen rows, even when the whole
//    build side is a handful of rows that would otherwise trivially fit in
//    memory end to end. Because probe-side routing (`probe_with_spilling`)
//    decides whether to spill a probe batch purely from whether its build
//    partition already spilled, forcing build-side partitions here also
//    forces the PROBE-side disk round trip (through
//    `process_spilled_partition`) for those same partitions — one lever
//    covers both build and probe.
//
// Pure parsing logic is factored out (`parse_chaos_force_spill_after_batches`
// / `ChaosPartitionSpec::parse`) so it is unit-testable without mutating the
// real process environment — `cargo test` runs many tests from one binary
// concurrently, and a test calling `std::env::set_var` here would race every
// other test reading the same key (see `gpu.rs`'s `parse_cache_budget_mb` and
// `execution::context::parse_merge_concurrency` for this exact, established
// precedent in this codebase).

/// Pure parsing logic for `QE_SPILL_CHAOS_FORCE_SPILL` — see the module-level
/// "Fault injection" doc comment above. `None` (env var unset) means no
/// forcing at all; `Some(raw)` (env var set, to any string, including empty
/// or unparseable) means "force spill," at the batch count `raw` parses to,
/// defaulting to 0 (force on the very first batch) if `raw` doesn't parse.
fn parse_chaos_force_spill_after_batches(raw: Option<&str>) -> Option<usize> {
    raw.map(|v| v.trim().parse::<usize>().unwrap_or(0))
}

fn chaos_force_spill_after_batches() -> Option<usize> {
    parse_chaos_force_spill_after_batches(
        std::env::var("QE_SPILL_CHAOS_FORCE_SPILL").ok().as_deref(),
    )
}

/// Which hash partitions `build_with_partitioning` should force to disk,
/// independent of memory pressure — see the module-level "Fault injection"
/// doc comment above for why this is needed alongside
/// `chaos_force_spill_after_batches`.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ChaosPartitionSpec {
    /// Force every one of the `NUM_PARTITIONS` hash partitions to disk —
    /// maximum coverage in a single trial.
    All,
    /// Force exactly these partition indices to disk; every other
    /// partition still spills only if memory pressure demands it.
    Indices(std::collections::HashSet<usize>),
}

impl ChaosPartitionSpec {
    fn contains(&self, idx: usize) -> bool {
        match self {
            ChaosPartitionSpec::All => true,
            ChaosPartitionSpec::Indices(set) => set.contains(&idx),
        }
    }

    /// "all" (case-insensitive) forces every partition. Otherwise, a
    /// comma-separated list of partition indices; any comma-separated
    /// token that fails to parse as a `usize` is silently skipped (an
    /// empty or fully-unparseable spec is `Indices(<empty set>)`, i.e. "no
    /// partitions forced" — never a panic).
    fn parse(spec: &str) -> Self {
        let spec = spec.trim();
        if spec.eq_ignore_ascii_case("all") {
            return ChaosPartitionSpec::All;
        }
        let indices = spec
            .split(',')
            .filter_map(|s| s.trim().parse::<usize>().ok())
            .collect();
        ChaosPartitionSpec::Indices(indices)
    }
}

fn parse_chaos_force_spill_partitions(raw: Option<&str>) -> Option<ChaosPartitionSpec> {
    raw.map(ChaosPartitionSpec::parse)
}

fn chaos_force_spill_partitions() -> Option<ChaosPartitionSpec> {
    parse_chaos_force_spill_partitions(
        std::env::var("QE_SPILL_CHAOS_FORCE_SPILL_PARTITIONS")
            .ok()
            .as_deref(),
    )
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

/// Stream every output partition of `input` concurrently into ONE merged
/// stream, without collecting anything into a `Vec` first.
///
/// `collect_input_partitions_concurrently` (above) gives a pipeline-breaking
/// operator's collect side the same cross-core parallelism benefit, but by
/// fully draining every partition into memory before returning — exactly
/// what let `SpillableHashJoinExec`'s build side OOM before its own spill
/// decision could ever run (spill-join-correctness-2 epic, task 002: the
/// build side was compared against `memory_limit * spill_threshold` only
/// AFTER `collect_input_partitions_concurrently` had already fully
/// materialized it).
///
/// This function keeps the same "one task per input partition, drained
/// concurrently" parallelism, but hands batches to the caller as they arrive
/// via a small, FIXED-capacity channel instead of a growing `Vec`. The
/// callers (`SpillableHashJoinExec::compute_build_decision`,
/// `SpillableHashAggregateExec::execute` — oom-safety-hardening task 002 —
/// and `ExternalSortExec::execute` — task 003) can then track a
/// running size total and switch to a bounded, spill-capable structure the
/// moment it would exceed the threshold, without ever buffering more than a
/// bounded number of batches ahead of that check. The channel's own fixed
/// bound is what keeps this mechanism itself from becoming a second,
/// subtler unbounded-memory path.
async fn stream_merge_input_partitions(
    input: &Arc<dyn PhysicalOperator>,
) -> Result<RecordBatchStream> {
    let input_partitions = input.output_partitions().max(1);
    if input_partitions == 1 {
        // Nothing to merge — avoid the task-spawn/channel round trip.
        return input.execute(0).await;
    }

    // Small and FIXED per producer partition: enough that a fast producer
    // doesn't stall on every single batch, not enough to reintroduce
    // unbounded buffering — a handful of batches, never the whole build
    // side, can sit in this channel at once.
    const PER_PARTITION_CAPACITY: usize = 4;
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(
        PER_PARTITION_CAPACITY * input_partitions,
    );

    for part in 0..input_partitions {
        let input = input.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let mut stream = match input.execute(part).await {
                Ok(s) => s,
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            loop {
                match stream.try_next().await {
                    Ok(Some(batch)) => {
                        if tx.send(Ok(batch)).await.is_err() {
                            // Receiver gone (e.g. the consumer bailed on an
                            // earlier error) — stop pulling from our own
                            // upstream rather than spin producing into the
                            // void.
                            return;
                        }
                    }
                    Ok(None) => return,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }
            }
        });
    }
    // Drop this function's own sender handle so the channel closes once
    // every spawned producer task (each holds its own clone) finishes.
    drop(tx);

    Ok(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx)))
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
    /// Build exceeds memory — spill path (only partition 0 processes).
    ///
    /// spill-join-correctness-2 epic, task 002: `partitions`/`spilled` are
    /// the ALREADY hash-partitioned, already-spilled-where-needed build
    /// side, computed ONCE by `compute_build_decision`'s bounded,
    /// incremental collection — never a full, unbounded, unpartitioned
    /// copy of the build side (that was the collect-fully-then-decide OOM
    /// hole this task fixes). Kept for EVERY execution of partition 0 — an
    /// operator above may legitimately execute its child more than once
    /// (the fused streaming aggregate drains, aborts, and re-executes).
    /// The old `Mutex<Option<..>>::take()` shape handed the second
    /// execution an EMPTY build side, which joined to zero rows and
    /// returned them as the answer; this shape avoids that the same way
    /// the old flat-`Vec` one did, just without the flat Vec. `spill_dir`
    /// is REUSED, not recreated, across repeat calls, so
    /// `spilled[..].build_file` stays valid — it is removed exactly once,
    /// in `Drop`, not at the end of every `execute_spill_path` call (see
    /// that function's own comment).
    ///
    /// oom-safety-hardening task 007: `tables` are the in-memory
    /// partitions' join hash tables, built ONCE by
    /// `build_with_partitioning`'s hash-table budgeting pass under DIRECT
    /// memory accounting (`hash_table_memory_bytes`) — the resident set's
    /// batches PLUS tables together fit under `memory_limit *
    /// spill_threshold` by construction, with partitions evicted to disk
    /// when the running total would cross it. Memoizing them here (rather
    /// than rebuilding per `execute_spill_path` call, as before) means the
    /// bound holds for the operator's whole lifetime and a repeat
    /// execution never re-pays the build. Before this task these tables
    /// were rebuilt per call with ZERO accounting at ~10-20x the batch
    /// bytes the spill decision budgeted (task 001's root-caused hole:
    /// measured 15-24x budget overshoot, ≥7.3GB at a 500MB budget).
    Spill {
        partitions: Vec<Option<BuildPartition>>,
        tables: Vec<Option<HashMap<JoinKey, Vec<HashEntry>>>>,
        spilled: Vec<Option<SpilledPartition>>,
        spill_dir: PathBuf,
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
    /// Total rows across `batches` — kept incrementally so the spill
    /// path's hash-table accounting (`predicted_hash_table_bytes`,
    /// oom-safety-hardening task 007) never rescans batches to price the
    /// table this partition's rows imply.
    rows: usize,
}

impl BuildPartition {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            memory_bytes: 0,
            rows: 0,
        }
    }

    fn add_batch(&mut self, batch: RecordBatch) {
        self.memory_bytes += estimate_batch_size(&batch);
        self.rows += batch.num_rows();
        self.batches.push(batch);
    }

    /// What keeping this partition resident REALLY costs once its join
    /// hash table exists: batch bytes plus the (conservative, worst-case
    /// all-unique-keys) predicted table bytes. oom-safety-hardening task
    /// 007: this is the charge the streaming build loop accounts against
    /// the memory threshold — before it, only `memory_bytes` was charged,
    /// so resident partitions were kept AT the threshold in batch bytes
    /// and the 10-20x table amplification landed entirely on top,
    /// unbudgeted (the measured 15-24x overshoot of task 001's repros).
    fn charged_bytes(&self, key_cols: usize) -> usize {
        self.memory_bytes + predicted_hash_table_bytes(self.rows, key_cols)
    }
}

/// State for a spilled partition — build-side info only.
///
/// spill-join-correctness-2 epic, task 002: this struct is now memoized
/// ONCE (inside `BuildDecision::Spill`, computed by
/// `SpillableHashJoinExec::compute_build_decision`) and reused across
/// every call to `execute_spill_path` (an operator above may legitimately
/// execute its child more than once — see `BuildDecision::Spill`'s own
/// doc comment). Probe-side spill info is call-specific (each call
/// re-probes and writes fresh probe spill files, since the probe side is
/// re-executed every call), so it is threaded through
/// `execute_spill_path`/`process_spilled_partition` as separate, per-call
/// parameters instead of being stored here — storing it here would have
/// meant either clobbering it on a second call before the first call's
/// results were done using it, or leaking a `Mutex`/interior-mutability
/// dance this file doesn't otherwise need.
struct SpilledPartition {
    build_file: PathBuf,
    build_rows: usize,
    /// Diagnostic only (`QE_SPILL_DEBUG`), spill-join-correctness-2 epic
    /// task 001: a checksum of the join-key values as WRITTEN to
    /// `build_file`, computed from the ORIGINAL in-memory batch(es) at
    /// spill time via the exact same `extract_join_key` code path
    /// `build_hash_table`/`partition_batch_by_hash` use. Compared in
    /// `process_spilled_partition` against the checksum RECOMPUTED from
    /// the data read back off `build_file` -- directly testing whether the
    /// spill/unspill round trip preserves join-key data, the same shape of
    /// bug as Trino's PR #25892 (spill wrote with one hash generator,
    /// unspill read with a different one). `None` when `QE_SPILL_DEBUG` is
    /// unset (never computed, zero cost).
    build_key_checksum: Option<KeyChecksum>,
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

        // Get or compute the build decision (computed ONCE, shared across all
        // partitions). spill-join-correctness-2 epic, task 002:
        // `compute_build_decision` collects the build side INCREMENTALLY,
        // checking the running size against the spill threshold as batches
        // arrive, instead of the old `collect_input_partitions_concurrently`
        // -then-check order that could OOM on an oversized build side before
        // the spill decision ever ran. See that method's own doc comment.
        let decision = self
            .build_decision
            .get_or_try_init(|| self.compute_build_decision(build_side, swapped))
            .await?;

        match decision {
            BuildDecision::InMemory(hash_join) => {
                // Delegate directly — HashJoinExec has its own OnceCell for the hash table
                hash_join.execute(partition).await
            }
            BuildDecision::Spill {
                partitions,
                tables,
                spilled,
                spill_dir,
            } => {
                // Spill path runs everything through partition 0
                if partition > 0 {
                    return Ok(Box::pin(stream::empty()));
                }
                self.execute_spill_path(partitions, tables, spilled, spill_dir, probe_side, swapped)
                    .await
            }
        }
    }

    fn name(&self) -> &str {
        "SpillableHashJoin"
    }
}

impl SpillableHashJoinExec {
    /// Compute the (memoized-once) build decision: does the build side fit
    /// in memory, or must it spill?
    ///
    /// spill-join-correctness-2 epic, task 002: this is the actual fix for
    /// the collect-fully-then-decide OOM hole. The OLD code called
    /// `collect_input_partitions_concurrently`, which fully drains the
    /// ENTIRE build side into one flat `Vec<RecordBatch>`, and only THEN
    /// compared its total size against `memory_limit * spill_threshold` —
    /// so an oversized build side could exhaust real memory during that
    /// initial collection, before the spill decision (or anything it would
    /// have triggered) ever ran.
    ///
    /// This is Photon's (Databricks, SIGMOD'22) two-phase reservation
    /// pattern, adapted rather than ported literally: phase 1 below
    /// ("reserve, possibly spill") streams the build side in via
    /// `stream_merge_input_partitions` and tracks a running size total,
    /// batch by batch, checking it against `memory_threshold` as each batch
    /// arrives — never buffering more than ~`memory_threshold` bytes of
    /// flat, unpartitioned build-side data. The MOMENT the running total
    /// would cross the threshold, phase 2 ("guaranteed spill-free
    /// allocation" becomes "hand off to the bounded, spill-capable
    /// structure") takes over: everything collected so far, plus the
    /// crossing batch, plus the rest of the stream, feeds into
    /// `build_with_partitioning`'s existing hash-partition-and-spill-
    /// as-needed bookkeeping — the exact same mechanism the rest of the
    /// spill path already used, just entered mid-stream instead of only
    /// after a full prior collection. If the whole build side is consumed
    /// without ever crossing the threshold, this is the ordinary "fits in
    /// memory" case — nothing about that path's cost or shape changes
    /// versus before.
    /// Enter the disk-spill branch: validate join shape (INNER-only, no
    /// filter), create the spill directory, and hand `prefix` (already
    /// flat-collected batches) chained with `rest` (whatever remains of the
    /// build stream — empty if the stream was already exhausted) to
    /// `build_with_partitioning`. Factored out of `compute_build_decision`
    /// (spill-join-correctness-2 epic, task 003) so its two crossing sites
    /// — the ordinary mid-stream crossing (organic memory pressure or
    /// `QE_SPILL_CHAOS_FORCE_SPILL`, `rest` non-empty) and the
    /// end-of-stream chaos fallback (`rest` always empty, see that
    /// fallback's own comment) — share one copy of the INNER/filter guard
    /// and spill-directory setup rather than risking drift between two
    /// copies.
    async fn finish_via_spill(
        &self,
        prefix: Vec<RecordBatch>,
        rest: RecordBatchStream,
        build_keys: &[Expr],
        sj_trace: bool,
    ) -> Result<BuildDecision> {
        if !matches!(self.join_type, JoinType::Inner) {
            return Err(QueryError::Execution(format!(
                "{} join build side exceeds the memory budget, but the join spill \
                 path currently supports only INNER joins. Raise the memory limit \
                 for this query.",
                self.join_type
            )));
        }
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

        if sj_trace {
            let prefix_rows: usize = prefix.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "[sj-trace] compute_build_decision spill_dir={:?} entering build_with_partitioning prefix_rows={}",
                spill_dir, prefix_rows
            );
        }

        // Phase 2: guaranteed-spill-free (for what's ALREADY collected)
        // allocation becomes "hand off to the bounded, spill-capable
        // structure" — everything gathered so far (`prefix`) plus whatever
        // remains of the stream (`rest`), through the SAME
        // hash-partition-and-spill-as-needed bookkeeping the rest of the
        // spill path already used. Nothing collected in phase 1 is thrown
        // away or re-pulled from the source.
        let combined: RecordBatchStream =
            Box::pin(stream::iter(prefix.into_iter().map(Ok)).chain(rest));

        let (partitions, tables, spilled) = self
            .build_with_partitioning(combined, build_keys, &spill_dir)
            .await?;

        Ok(BuildDecision::Spill {
            partitions,
            tables,
            spilled,
            spill_dir,
        })
    }

    async fn compute_build_decision(
        &self,
        build_side: &Arc<dyn PhysicalOperator>,
        swapped: bool,
    ) -> Result<BuildDecision> {
        let memory_threshold =
            (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;
        let (on_left, on_right): (Vec<Expr>, Vec<Expr>) = self.on.iter().cloned().unzip();
        let build_keys: Vec<Expr> = if swapped { on_right } else { on_left };

        let sj_trace = std::env::var("QE_SPILL_DEBUG").is_ok();
        // spill-join-correctness-2 epic, task 003: see this file's own
        // "Fault injection: forced spill" module doc comment.
        let chaos_after_batches = chaos_force_spill_after_batches();

        // Phase 1: reserve, possibly spill. Stream in, tracking a running
        // total as batches arrive from every build-side input partition
        // concurrently (the streaming analog of
        // `collect_input_partitions_concurrently`'s own parallel-drain
        // benefit for a pipeline-breaking operator's collect side).
        let mut build_stream = stream_merge_input_partitions(build_side).await?;
        let mut flat_batches: Vec<RecordBatch> = Vec::new();
        let mut flat_size: usize = 0;

        while let Some(batch) = build_stream.try_next().await? {
            let batch_size = estimate_batch_size(&batch);
            let chaos_crossing = chaos_after_batches
                .map(|n| flat_batches.len() >= n)
                .unwrap_or(false);
            if flat_size + batch_size > memory_threshold || chaos_crossing {
                if chaos_crossing && sj_trace {
                    eprintln!(
                        "[sj-trace] compute_build_decision CHAOS forcing spill (QE_SPILL_CHAOS_FORCE_SPILL) at flat_batches={} flat_size={}",
                        flat_batches.len(),
                        flat_size
                    );
                }
                // Crossed the threshold: this build side must spill. Checked
                // HERE, at the crossing point, rather than only once a full
                // collection would have finished — an oversized non-INNER
                // or filtered join now fails loudly this much earlier too,
                // without ever needing to pull in the rest of the build
                // side first.
                let prefix: Vec<RecordBatch> = flat_batches
                    .into_iter()
                    .chain(std::iter::once(batch))
                    .collect();
                return self
                    .finish_via_spill(prefix, build_stream, &build_keys, sj_trace)
                    .await;
            }
            flat_size += batch_size;
            flat_batches.push(batch);
        }

        // spill-join-correctness-2 epic, task 003: the build stream is now
        // fully exhausted without ever organically crossing
        // `memory_threshold`. If `QE_SPILL_CHAOS_FORCE_SPILL` requested a
        // crossing point (a batch count) the stream never actually reached
        // — e.g. a small/single-batch build side, common for this
        // mechanism's own differential-testing harness fixtures — force it
        // HERE instead of silently falling through to "fits in memory,"
        // which would make the WHEN lever a no-op for exactly the small
        // inputs it's most useful against. `rest` is empty (nothing left
        // to chain; the stream is already drained).
        if chaos_after_batches.is_some() && !flat_batches.is_empty() {
            if sj_trace {
                eprintln!(
                    "[sj-trace] compute_build_decision CHAOS forcing spill at end-of-stream (requested crossing point never reached; flat_batches={} flat_size={})",
                    flat_batches.len(),
                    flat_size
                );
            }
            return self
                .finish_via_spill(
                    flat_batches,
                    Box::pin(stream::empty()),
                    &build_keys,
                    sj_trace,
                )
                .await;
        }

        // Never crossed the threshold: the build side genuinely fits.
        // Same in-memory HashJoinExec construction as before — reached
        // without ever risking an unbounded collection to get here.
        let build_schema = flat_batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| build_side.schema());
        let build_mem = Arc::new(crate::physical::operators::MemoryTableExec::new(
            "join_build",
            build_schema,
            flat_batches,
            None,
        ));
        let (left, right): (Arc<dyn PhysicalOperator>, Arc<dyn PhysicalOperator>) = if swapped {
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
        Ok(BuildDecision::InMemory(hash_join))
    }

    /// Execute the spill path when build side exceeds memory limit.
    /// Only called from partition 0. May be called MORE THAN ONCE for the
    /// same logical query execution (an operator above may legitimately
    /// execute its child more than once — see `BuildDecision::Spill`'s own
    /// doc comment); `partitions`/`spilled`/`spill_dir` are borrowed from
    /// the memoized `BuildDecision`, computed exactly once regardless of
    /// how many times this function runs.
    async fn execute_spill_path(
        &self,
        partitions: &[Option<BuildPartition>],
        hash_tables: &[Option<HashMap<JoinKey, Vec<HashEntry>>>],
        spilled_partitions: &[Option<SpilledPartition>],
        spill_dir: &PathBuf,
        probe_side: &Arc<dyn PhysicalOperator>,
        swapped: bool,
    ) -> Result<RecordBatchStream> {
        // QE_SPILL_DEBUG tracing (task 001, spill-join-correctness epic):
        // if some caller ever invokes this more than once for what should
        // be a single logical query execution (e.g.
        // `SpillableHashAggregateExec`'s fused-streaming path aborting and
        // falling back to `collect_input_partitions_concurrently`, which
        // re-executes its input), this prints TWO START/DONE pairs for the
        // SAME join instead of one — direct evidence, not inference. Zero
        // cost when unset beyond one env lookup per call.
        let sj_trace = std::env::var("QE_SPILL_DEBUG").is_ok();
        let sj_t0 = std::time::Instant::now();

        let (on_left, on_right): (Vec<_>, Vec<_>) = self.on.iter().cloned().unzip();
        let build_keys = if swapped { &on_right } else { &on_left };
        let probe_keys = if swapped { &on_left } else { &on_right };

        // oom-safety-hardening task 007: the in-memory partitions' hash
        // tables are NO LONGER built here. They are built exactly once, in
        // `build_with_partitioning`'s hash-table budgeting pass, under
        // direct memory accounting (resident batches + measured table
        // bytes <= memory_threshold, partitions evicted when the total
        // would cross), and memoized in the build decision alongside the
        // batches they index. This function's old unbudgeted rebuild loop
        // was task 001's root-caused accounting hole (~10-20x the batch
        // bytes the spill decision budgeted, measured 15-24x overshoot).

        if sj_trace {
            let in_mem_parts = partitions.iter().filter(|p| p.is_some()).count();
            let spilled_parts = spilled_partitions.iter().filter(|p| p.is_some()).count();
            let in_mem_build_rows: usize = partitions
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
                "[sj-trace] execute_spill_path START spill_dir={:?} in_memory_partitions={} (rows={}) spilled_partitions={} (rows={})",
                spill_dir, in_mem_parts, in_mem_build_rows, spilled_parts, spilled_build_rows
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
                "[sj-trace] execute_spill_path probe collected: probe_partitions={} probe_rows={}",
                probe_partitions, probe_rows_in
            );
        }
        let probe_stream: RecordBatchStream =
            Box::pin(stream::iter(probe_batches.into_iter().map(Ok)));
        let (results, probe_spill_files, probe_key_checksums) = self
            .probe_with_spilling(
                probe_stream,
                probe_keys,
                partitions,
                hash_tables,
                spilled_partitions,
                spill_dir,
                swapped,
            )
            .await?;
        let in_memory_matched_rows: usize = results.iter().map(|b| b.num_rows()).sum();

        // Process spilled partitions — this call's own freshly-written
        // probe spill file (if any) for each partition, paired with the
        // MEMOIZED build spill file from `spilled_partitions`.
        let mut all_results = results;
        let mut spilled_matched_rows: usize = 0;
        for (idx, spilled) in spilled_partitions.iter().enumerate() {
            if let Some(sp) = spilled {
                let probe_file = probe_spill_files[idx].as_ref();
                let probe_key_checksum = probe_key_checksums[idx];
                let spilled_results = self
                    .process_spilled_partition(
                        &sp.build_file,
                        sp.build_key_checksum,
                        probe_file,
                        probe_key_checksum,
                        build_keys,
                        probe_keys,
                        swapped,
                        idx,
                    )
                    .await?;
                spilled_matched_rows += spilled_results.iter().map(|b| b.num_rows()).sum::<usize>();
                all_results.extend(spilled_results);
            }
        }

        // Build-side spill files under `spill_dir` are NOT removed here —
        // they are memoized in `self.build_decision` and must survive a
        // possible repeat call to this same function (see this function's
        // own doc comment). Cleaned up exactly once, in `Drop`, when this
        // operator itself is no longer needed.

        if sj_trace {
            let total_matched: usize = all_results.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "[sj-trace] execute_spill_path DONE spill_dir={:?} in_memory_matched={} spilled_matched={} total_matched={} elapsed={:?}",
                spill_dir, in_memory_matched_rows, spilled_matched_rows, total_matched, sj_t0.elapsed()
            );
        }

        Ok(Box::pin(stream::iter(all_results.into_iter().map(Ok))))
    }

    /// Evict partition `idx`'s currently-resident batches to disk right now,
    /// opening/reusing its spill writer — the exact mechanism
    /// `build_with_partitioning`'s own memory-pressure eviction used
    /// inline before spill-join-correctness-2 epic task 003 pulled it out,
    /// so the fault-injection path (which forces a CHOSEN partition to disk
    /// regardless of memory pressure, see this file's own "Fault injection:
    /// forced spill" module doc comment) shares it byte-for-byte with the
    /// organic memory-pressure path rather than risking behavioral drift
    /// between two separate copies of the same spill-write logic. No-op
    /// (returns 0 freed bytes) if the partition is already empty/spilled.
    /// Only ever called on a still-resident partition by either caller, so
    /// `spilled[idx]` is always `None` on entry here in practice — asserted
    /// implicitly by simply overwriting it, matching the pre-refactor code's
    /// own assumption.
    fn evict_build_partition_to_disk(
        &self,
        idx: usize,
        partitions: &mut [Option<BuildPartition>],
        spilled: &mut [Option<SpilledPartition>],
        spill_writers: &mut [Option<ArrowWriter<File>>],
        spill_dir: &PathBuf,
        build_keys: &[Expr],
        sj_trace: bool,
    ) -> Result<usize> {
        let Some(part) = partitions[idx].take() else {
            return Ok(0);
        };
        let path = spill_dir.join(format!("build_{}.parquet", idx));
        let mut write_checksum = if sj_trace {
            Some(KeyChecksum::default())
        } else {
            None
        };
        for b in &part.batches {
            append_batch_streaming(&mut spill_writers[idx], &path, b)?;
            if let Some(cs) = write_checksum.as_mut() {
                cs.accumulate(batch_key_checksum(b, build_keys)?);
            }
        }

        self.memory_pool.record_spill(part.memory_bytes);
        let freed = part.memory_bytes;

        spilled[idx] = Some(SpilledPartition {
            build_file: path,
            build_rows: part.batches.iter().map(|b| b.num_rows()).sum(),
            build_key_checksum: write_checksum,
        });

        Ok(freed)
    }

    #[allow(clippy::type_complexity)]
    async fn build_with_partitioning(
        &self,
        mut build_stream: RecordBatchStream,
        build_keys: &[Expr],
        spill_dir: &PathBuf,
    ) -> Result<(
        Vec<Option<BuildPartition>>,
        Vec<Option<HashMap<JoinKey, Vec<HashEntry>>>>,
        Vec<Option<SpilledPartition>>,
    )> {
        // spill-join-correctness-2 epic, task 001: checked once per call,
        // matching this file's own established convention (no `OnceLock`
        // caching, re-read fresh — see `gpu.rs`'s `QE_GPU_DEBUG` precedent).
        let sj_trace = std::env::var("QE_SPILL_DEBUG").is_ok();
        // spill-join-correctness-2 epic, task 003: see this file's own
        // "Fault injection: forced spill" module doc comment.
        let chaos_partitions = chaos_force_spill_partitions();
        let mut partitions: Vec<Option<BuildPartition>> = (0..NUM_PARTITIONS)
            .map(|_| Some(BuildPartition::new()))
            .collect();
        let mut spilled: Vec<Option<SpilledPartition>> =
            (0..NUM_PARTITIONS).map(|_| None).collect();
        // One incrementally-written Parquet file per spilled partition, kept
        // OPEN for the whole build phase and closed exactly once at the end.
        // This replaces the old per-append read-entire-file+rewrite+rename
        // (`append_to_parquet`), whose cost grew with the total data already
        // spilled for that partition — see `append_batch_streaming`'s doc
        // comment (spill-join-correctness epic, task 002).
        let mut spill_writers: Vec<Option<ArrowWriter<File>>> =
            (0..NUM_PARTITIONS).map(|_| None).collect();
        // oom-safety-hardening task 007: `total_memory` charges each
        // resident partition its batch bytes PLUS the predicted bytes of
        // the join hash table those rows imply
        // (`BuildPartition::charged_bytes`), so residency settles where
        // batches AND tables genuinely fit — not where batch bytes alone
        // hit the threshold with the 10-20x table amplification unbudgeted
        // on top (task 001's root-caused hole). The later budgeting pass
        // (`budget_partition_hash_tables`) then replaces the prediction
        // with DIRECT measurement of the built tables and evicts any
        // residual overshoot.
        let key_cols = build_keys.len();
        let mut total_memory: usize = 0;
        let memory_threshold =
            (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;

        while let Some(batch) = build_stream.try_next().await? {
            let batch_charge = estimate_batch_size(&batch)
                + predicted_hash_table_bytes(batch.num_rows(), key_cols);

            // Check if we need to spill
            if total_memory + batch_charge > memory_threshold {
                // Find the largest partition to spill
                if let Some(idx) = find_largest_partition(&partitions) {
                    // Uncharge the partition's FULL charge (batches +
                    // predicted table), matching what was added below.
                    let charge = partitions[idx]
                        .as_ref()
                        .map(|p| p.charged_bytes(key_cols))
                        .unwrap_or(0);
                    self.evict_build_partition_to_disk(
                        idx,
                        &mut partitions,
                        &mut spilled,
                        &mut spill_writers,
                        spill_dir,
                        build_keys,
                        sj_trace,
                    )?;
                    total_memory = total_memory.saturating_sub(charge);
                }
            }

            // Partition the batch by hash
            let partitioned = partition_batch_by_hash(&batch, build_keys, NUM_PARTITIONS)?;

            for (idx, part_batch) in partitioned.into_iter().enumerate() {
                if let Some(pb) = part_batch {
                    let pb_charge = estimate_batch_size(&pb)
                        + predicted_hash_table_bytes(pb.num_rows(), key_cols);

                    if let Some(ref mut part) = partitions[idx] {
                        part.add_batch(pb);
                        total_memory += pb_charge;
                        // spill-join-correctness-2 epic, task 003: force
                        // this partition to disk right now if a fault-
                        // injection trial chose it, regardless of memory
                        // pressure — see `evict_build_partition_to_disk`'s
                        // own doc comment.
                        if chaos_partitions.as_ref().is_some_and(|s| s.contains(idx)) {
                            let charge = partitions[idx]
                                .as_ref()
                                .map(|p| p.charged_bytes(key_cols))
                                .unwrap_or(0);
                            self.evict_build_partition_to_disk(
                                idx,
                                &mut partitions,
                                &mut spilled,
                                &mut spill_writers,
                                spill_dir,
                                build_keys,
                                sj_trace,
                            )?;
                            total_memory = total_memory.saturating_sub(charge);
                        }
                    } else if let Some(ref mut sp) = spilled[idx] {
                        // Append to spilled partition as one more row group
                        // in the already-open writer — O(batch), never
                        // re-reads or rewrites this partition's prior data.
                        append_batch_streaming(&mut spill_writers[idx], &sp.build_file, &pb)?;
                        if sj_trace {
                            let cs = batch_key_checksum(&pb, build_keys)?;
                            sp.build_key_checksum
                                .get_or_insert_with(KeyChecksum::default)
                                .accumulate(cs);
                        }
                    }
                }
            }
        }

        // oom-safety-hardening task 007: hash-table budgeting pass. The
        // streaming loop above bounded resident batch bytes + PREDICTED
        // table bytes at `memory_threshold`; this pass builds the actual
        // tables, while `partitions`/`spilled`/`spill_writers` are still
        // mutable, and replaces the prediction with DIRECT measurement:
        // resident batch bytes + measured table bytes must stay under the
        // SAME `memory_threshold`, with partitions evicted to disk (the
        // existing eviction mechanism, byte-for-byte) when the running
        // total would cross it. Before this task the tables were built
        // later, in `execute_spill_path`, with zero accounting at ~10-20x
        // the batch bytes the spill decision budgeted (task 001's
        // root-caused hole, measured 15-24x budget overshoot).
        let tables = self.budget_partition_hash_tables(
            &mut partitions,
            &mut spilled,
            &mut spill_writers,
            spill_dir,
            build_keys,
            sj_trace,
        )?;

        close_spill_writers(spill_writers)?;
        Ok((partitions, tables, spilled))
    }

    /// oom-safety-hardening task 007: decide which resident build
    /// partitions may keep an in-memory join hash table, under DIRECT
    /// memory accounting, evicting the rest to disk.
    ///
    /// The running total starts at the sum of all resident partitions'
    /// batch bytes and must never cross `memory_limit * spill_threshold`
    /// — the same formula every other spill decision in this file uses.
    /// (The streaming loop already bounded batch bytes + PREDICTED table
    /// bytes at the same threshold; this pass swaps the prediction for
    /// direct measurement.) Per resident partition, largest first
    /// (keeping the largest resident minimizes both spill I/O and the
    /// biggest per-partition transient read-back cost later):
    ///
    /// 1. a conservative PREDICTED table cost
    ///    (`predicted_hash_table_bytes`, worst-case all-unique keys) gates
    ///    whether the table is even built — so a table that provably
    ///    cannot fit is never materialized at all;
    /// 2. if built, the table's REAL footprint is measured
    ///    (`hash_table_memory_bytes` — real capacity, real per-key Vec
    ///    buffers, real String bytes) and charged against the budget; a
    ///    measured crossing (possible when the prediction under-counts,
    ///    e.g. long String keys) drops the table and evicts the partition,
    ///    bounding the transient overshoot at one partition's table.
    ///
    /// Eviction reuses `evict_build_partition_to_disk` unchanged (same
    /// writer, same spill files, same checksum path), so probe routing is
    /// untouched: an evicted partition simply becomes one more
    /// `SpilledPartition`, exactly as if the streaming loop's own
    /// memory-pressure eviction had chosen it.
    #[allow(clippy::too_many_arguments, clippy::type_complexity)]
    fn budget_partition_hash_tables(
        &self,
        partitions: &mut [Option<BuildPartition>],
        spilled: &mut [Option<SpilledPartition>],
        spill_writers: &mut [Option<ArrowWriter<File>>],
        spill_dir: &PathBuf,
        build_keys: &[Expr],
        sj_trace: bool,
    ) -> Result<Vec<Option<HashMap<JoinKey, Vec<HashEntry>>>>> {
        let memory_threshold =
            (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;
        let mut tables: Vec<Option<HashMap<JoinKey, Vec<HashEntry>>>> =
            (0..NUM_PARTITIONS).map(|_| None).collect();

        // Resident batch bytes are already in memory and stay charged for
        // the operator's lifetime (memoized in the build decision).
        let mut running: usize = partitions
            .iter()
            .flatten()
            .map(|p| p.memory_bytes)
            .sum::<usize>();
        let mut table_bytes_total: usize = 0;
        let mut evicted_for_tables = 0usize;

        let mut order: Vec<usize> = (0..partitions.len())
            .filter(|&i| {
                partitions[i]
                    .as_ref()
                    .is_some_and(|p| !p.batches.is_empty())
            })
            .collect();
        order.sort_by_key(|&i| {
            std::cmp::Reverse(partitions[i].as_ref().map(|p| p.memory_bytes).unwrap_or(0))
        });

        for idx in order {
            let rows: usize = partitions[idx].as_ref().map(|p| p.rows).unwrap_or(0);
            let predicted = predicted_hash_table_bytes(rows, build_keys.len());
            if running + predicted > memory_threshold {
                running = running.saturating_sub(self.evict_build_partition_to_disk(
                    idx,
                    partitions,
                    spilled,
                    spill_writers,
                    spill_dir,
                    build_keys,
                    sj_trace,
                )?);
                evicted_for_tables += 1;
                continue;
            }
            let table = build_hash_table(
                &partitions[idx]
                    .as_ref()
                    .expect("resident partition filtered above")
                    .batches,
                build_keys,
            )?;
            let actual = hash_table_memory_bytes(&table);
            if running + actual > memory_threshold {
                drop(table);
                running = running.saturating_sub(self.evict_build_partition_to_disk(
                    idx,
                    partitions,
                    spilled,
                    spill_writers,
                    spill_dir,
                    build_keys,
                    sj_trace,
                )?);
                evicted_for_tables += 1;
                continue;
            }
            running += actual;
            table_bytes_total += actual;
            tables[idx] = Some(table);
        }

        if sj_trace {
            let resident = tables.iter().filter(|t| t.is_some()).count();
            let resident_batch_bytes: usize =
                partitions.iter().flatten().map(|p| p.memory_bytes).sum();
            eprintln!(
                "[sj-trace] budget_partition_hash_tables resident_partitions={} table_bytes={} batch_bytes={} running_total={} threshold={} evicted_for_table_budget={}",
                resident,
                table_bytes_total,
                resident_batch_bytes,
                running,
                memory_threshold,
                evicted_for_tables
            );
        }

        Ok(tables)
    }

    async fn probe_with_spilling(
        &self,
        mut probe_stream: RecordBatchStream,
        probe_keys: &[Expr],
        in_memory_partitions: &[Option<BuildPartition>],
        hash_tables: &[Option<HashMap<JoinKey, Vec<HashEntry>>>],
        spilled_partitions: &[Option<SpilledPartition>],
        spill_dir: &PathBuf,
        swapped: bool,
    ) -> Result<(
        Vec<RecordBatch>,
        Vec<Option<PathBuf>>,
        Vec<Option<KeyChecksum>>,
    )> {
        let sj_trace = std::env::var("QE_SPILL_DEBUG").is_ok();
        let mut results = Vec::new();
        let mut probe_spill_files: Vec<Option<PathBuf>> =
            (0..NUM_PARTITIONS).map(|_| None).collect();
        // spill-join-correctness-2 epic, task 001: write-time checksum of
        // each spilled partition's PROBE-side join keys, compared against
        // the read-back recomputation in `process_spilled_partition`.
        let mut probe_key_checksums: Vec<Option<KeyChecksum>> =
            (0..NUM_PARTITIONS).map(|_| None).collect();
        // Same fix as `build_with_partitioning`: one writer per partition,
        // kept open for the whole probe phase, instead of a read-rewrite
        // per appended batch.
        let mut spill_writers: Vec<Option<ArrowWriter<File>>> =
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
                        append_batch_streaming(&mut spill_writers[idx], probe_path, &pb)?;
                        if sj_trace {
                            let cs = batch_key_checksum(&pb, probe_keys)?;
                            probe_key_checksums[idx]
                                .get_or_insert_with(KeyChecksum::default)
                                .accumulate(cs);
                        }
                    }
                }
            }
        }

        close_spill_writers(spill_writers)?;
        Ok((results, probe_spill_files, probe_key_checksums))
    }

    async fn process_spilled_partition(
        &self,
        build_file: &PathBuf,
        build_key_checksum: Option<KeyChecksum>,
        probe_file: Option<&PathBuf>,
        probe_key_checksum: Option<KeyChecksum>,
        build_keys: &[Expr],
        probe_keys: &[Expr],
        swapped: bool,
        idx: usize,
    ) -> Result<Vec<RecordBatch>> {
        let sj_trace = std::env::var("QE_SPILL_DEBUG").is_ok();

        // Read build side from disk
        let build_batches = read_parquet(build_file)?;

        // spill-join-correctness-2 epic, task 001: directly compare the
        // join-key checksum recomputed from the data just read back off
        // disk against the checksum recorded when those SAME rows were
        // written to `build_file` (before the spill/unspill round
        // trip). A mismatch here would be direct, in-the-act evidence of a
        // Trino-PR#25892-shaped bug (spill-write and unspill-read
        // disagreeing about a row's join key) rather than inference from
        // reading the code.
        if sj_trace {
            if let Some(write_cs) = build_key_checksum {
                let mut read_cs = KeyChecksum::default();
                for b in &build_batches {
                    read_cs.accumulate(batch_key_checksum(b, build_keys)?);
                }
                if read_cs.rows != write_cs.rows || read_cs.xor_hash != write_cs.xor_hash {
                    eprintln!(
                        "[sj-trace] HASH-MISMATCH build partition idx={} write_rows={} write_xor={:016x} read_rows={} read_xor={:016x} write_unhandled={} read_unhandled={}",
                        idx,
                        write_cs.rows,
                        write_cs.xor_hash,
                        read_cs.rows,
                        read_cs.xor_hash,
                        write_cs.unhandled_type_rows,
                        read_cs.unhandled_type_rows
                    );
                } else {
                    eprintln!(
                        "[sj-trace] hash-check-ok build partition idx={} rows={} xor={:016x} unhandled={}",
                        idx, read_cs.rows, read_cs.xor_hash, read_cs.unhandled_type_rows
                    );
                }
            }
        }

        // Read probe side from disk (if exists)
        let probe_batches = if let Some(probe_path) = probe_file {
            read_parquet(probe_path)?
        } else {
            Vec::new()
        };

        if sj_trace {
            if let Some(write_cs) = probe_key_checksum {
                let mut read_cs = KeyChecksum::default();
                for b in &probe_batches {
                    read_cs.accumulate(batch_key_checksum(b, probe_keys)?);
                }
                if read_cs.rows != write_cs.rows || read_cs.xor_hash != write_cs.xor_hash {
                    eprintln!(
                        "[sj-trace] HASH-MISMATCH probe partition idx={} write_rows={} write_xor={:016x} read_rows={} read_xor={:016x} write_unhandled={} read_unhandled={}",
                        idx,
                        write_cs.rows,
                        write_cs.xor_hash,
                        read_cs.rows,
                        read_cs.xor_hash,
                        write_cs.unhandled_type_rows,
                        read_cs.unhandled_type_rows
                    );
                } else {
                    eprintln!(
                        "[sj-trace] hash-check-ok probe partition idx={} rows={} xor={:016x} unhandled={}",
                        idx, read_cs.rows, read_cs.xor_hash, read_cs.unhandled_type_rows
                    );
                }
            }
        }

        // oom-safety-hardening task 007: build the read-back partition's
        // hash table in CHUNKS of whole batches sized so the PREDICTED
        // table cost stays under `memory_limit * spill_threshold`, probing
        // the full probe set per chunk and dropping each chunk's table
        // before building the next. This bounds the previously-unbudgeted
        // per-read-back table (task 001's second unbudgeted call site) at
        // ~one threshold's worth of transient table instead of the whole
        // partition's rows x ~136B. Correctness: the spill path accepts
        // ONLY INNER joins (`finish_via_spill` refuses everything else),
        // and chunks partition the build rows disjointly, so each
        // (build row, probe row) match pair is emitted exactly once and
        // the union over chunks equals the single-table result. Probe
        // routing and `probe_partition` itself are untouched.
        let memory_threshold =
            (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;
        let key_cols = build_keys.len();
        let mut results: Vec<RecordBatch> = Vec::new();
        let mut chunk_start = 0usize;
        let mut chunk_count = 0usize;
        while chunk_start < build_batches.len() {
            let mut chunk_end = chunk_start;
            let mut chunk_rows = 0usize;
            while chunk_end < build_batches.len() {
                let next_rows = chunk_rows + build_batches[chunk_end].num_rows();
                // Always take at least one batch per chunk, then stop
                // BEFORE the predicted table cost would cross the budget.
                if chunk_end > chunk_start
                    && predicted_hash_table_bytes(next_rows, key_cols) > memory_threshold
                {
                    break;
                }
                chunk_rows = next_rows;
                chunk_end += 1;
            }
            let chunk = &build_batches[chunk_start..chunk_end];
            let hash_table = build_hash_table(chunk, build_keys)?;
            results.extend(probe_partition(
                chunk,
                &probe_batches,
                &hash_table,
                probe_keys,
                self.join_type,
                swapped,
                &self.schema,
                self.retained.as_deref(),
            )?);
            chunk_count += 1;
            chunk_start = chunk_end;
        }
        if sj_trace && chunk_count > 1 {
            eprintln!(
                "[sj-trace] process_spilled_partition idx={} build table chunked: {} chunks over {} batches (table budget {})",
                idx,
                chunk_count,
                build_batches.len(),
                memory_threshold
            );
        }
        Ok(results)
    }
}

/// spill-join-correctness-2 epic, task 002: build-side spill files
/// (`BuildDecision::Spill { spill_dir, .. }`) are now memoized ONCE, in
/// `compute_build_decision`, and deliberately NOT removed at the end of
/// every `execute_spill_path` call anymore — a repeat call (e.g. a
/// fused-streaming aggregate aborting and re-executing this join as its
/// input) must still be able to read them back. Clean up exactly once
/// here, when this operator itself is finally dropped (typically at the
/// end of the query that owns it). A join whose build side never spilled
/// (`BuildDecision::InMemory`, or `build_decision` never even
/// initialized, e.g. an unused partition) has nothing to clean up.
impl Drop for SpillableHashJoinExec {
    fn drop(&mut self) {
        if let Some(BuildDecision::Spill { spill_dir, .. }) = self.build_decision.get() {
            let _ = std::fs::remove_dir_all(spill_dir);
        }
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
    /// Total rows across `batches` — kept incrementally (mirroring
    /// `BuildPartition::rows`, oom-safety-hardening task 007) so the
    /// finalize step's aggregation-state accounting
    /// (`predicted_agg_state_bytes`, task 002) never rescans batches to
    /// price the state this partition's rows imply.
    rows: usize,
}

impl AggregatePartition {
    fn new() -> Self {
        Self {
            batches: Vec::new(),
            memory_bytes: 0,
            rows: 0,
        }
    }

    fn add_batch(&mut self, batch: RecordBatch) {
        self.memory_bytes += estimate_batch_size(&batch);
        self.rows += batch.num_rows();
        self.batches.push(batch);
    }

    fn clear(&mut self) {
        self.batches.clear();
        self.memory_bytes = 0;
        self.rows = 0;
    }
}

/// State for a spilled aggregate partition: the (single, incrementally
/// appended) Parquet file plus running row/byte totals of what was written
/// to it. The totals are what lets the finalize step price a partition's
/// read-back + aggregation-state cost BEFORE materializing anything
/// (oom-safety-hardening task 002, mirroring task 007's accounting
/// discipline for the join spill path's hash tables).
struct SpilledAggPartition {
    file: PathBuf,
    rows: usize,
    /// Estimated IN-MEMORY bytes of the batches written to `file`
    /// (accumulated `estimate_batch_size` at spill time — NOT the
    /// compressed on-disk size, which under-counts what read-back will
    /// actually materialize).
    raw_bytes: usize,
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
        // time by the streaming reservation below — for a child like
        // `SpillableHashJoinExec`'s spill path, which has no cache of its
        // own output, this reruns the ENTIRE join computation from scratch.
        // Not proof of duplication by itself (this second run is the one
        // whose results are actually returned), but a query whose log shows
        // this line is a query where the join's expensive work happened
        // twice, and a wrong answer that also shows two `execute_spill_path`
        // DONE lines is direct evidence they share a cause.
        if std::env::var("QE_SPILL_DEBUG").is_ok() {
            eprintln!(
                "[sj-trace] agg fallback: fused_eligible={} -> (re-)executing input via \
                 the streaming two-phase reservation (stream_merge_input_partitions)",
                fused_eligible
            );
        }

        // Phase 1: reserve, possibly spill (oom-safety-hardening task 002 —
        // the same Photon-style streaming two-phase reservation
        // `SpillableHashJoinExec::compute_build_decision` ships, mirrored
        // rather than reinvented). The OLD code called
        // `collect_input_partitions_concurrently`, which fully drained the
        // ENTIRE input into one flat `Vec<RecordBatch>` and only THEN
        // compared its total size against `memory_limit * spill_threshold`
        // — so an oversized input could exhaust real memory during that
        // initial collection, before the spill decision ever ran (the
        // harness `agg` scenario: 250M rows / ~4GB streamed into a 256MB
        // budget was kernel-OOM-killed here). Now the input streams in via
        // `stream_merge_input_partitions` (same one-task-per-partition
        // cross-core drain benefit, bounded channel instead of a growing
        // Vec) with a running size total checked as each batch arrives —
        // never buffering more than ~`memory_threshold` bytes of flat,
        // unpartitioned input. The MOMENT the total would cross the
        // threshold, everything collected so far plus the crossing batch
        // plus the rest of the stream feeds `aggregate_with_spilling`'s
        // hash-partition-and-spill-as-needed bookkeeping, entered
        // mid-stream instead of only after a full prior collection.
        let memory_threshold =
            (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;
        let mut input_stream = stream_merge_input_partitions(&self.input).await?;
        let mut flat_batches: Vec<RecordBatch> = Vec::new();
        let mut flat_size: usize = 0;
        let mut crossing_batch: Option<RecordBatch> = None;
        while let Some(batch) = input_stream.try_next().await? {
            let batch_size = estimate_batch_size(&batch);
            if flat_size + batch_size > memory_threshold {
                crossing_batch = Some(batch);
                break;
            }
            flat_size += batch_size;
            flat_batches.push(batch);
        }

        if crossing_batch.is_none() {
            // Never crossed the threshold: the input genuinely fits — the
            // ordinary in-memory case, reached without ever risking an
            // unbounded collection to get here.
            let all_batches = flat_batches;
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

        // Threshold crossed mid-stream — use the spillable aggregation path.
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
                "[spill-agg] threshold crossed mid-stream at {} buffered batches ({} bytes, \
                 threshold {}) — entering streaming spill path",
                flat_batches.len() + 1,
                flat_size,
                memory_threshold
            );
        }

        // Phase 2: everything gathered so far (`flat_batches`), plus the
        // crossing batch, plus whatever remains of the stream, feeds the
        // bounded, spill-capable structure. Nothing collected in phase 1 is
        // thrown away or re-pulled from the source (mirrors
        // `SpillableHashJoinExec::finish_via_spill`).
        let combined: RecordBatchStream = Box::pin(
            stream::iter(flat_batches.into_iter().chain(crossing_batch).map(Ok))
                .chain(input_stream),
        );

        // Process with partitioning and spilling
        let (mut in_memory_partitions, mut spilled_parts) =
            self.aggregate_with_spilling(combined, &spill_dir).await?;
        if std::env::var("QE_SPILL_DEBUG").is_ok() {
            let mem_rows: usize = in_memory_partitions.iter().flatten().map(|p| p.rows).sum();
            let spilled_rows: usize = spilled_parts.iter().flatten().map(|s| s.rows).sum();
            eprintln!(
                "[spill-agg] in-memory rows {}, spilled files {} (spilled rows {})",
                mem_rows,
                spilled_parts.iter().flatten().count(),
                spilled_rows
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

        // Finalize, one partition at a time, under the SAME accounting
        // discipline task 007 established for the join spill path's hash
        // tables: BEFORE materializing a partition's read-back + building
        // its aggregation hash state, price both (raw batch bytes actually
        // written/held for the partition + `predicted_agg_state_bytes`, the
        // conservative worst-case-all-unique-groups analogue of
        // `predicted_hash_table_bytes`) against the same
        // `memory_limit * spill_threshold` threshold. A partition that
        // provably cannot fit is aggregated via the chunked
        // (sub-partitioned) read-back instead
        // (`aggregate_partition_chunked`, the aggregate analogue of task
        // 007's chunked read-back in `process_spilled_partition`) — groups
        // stay whole because sub-partitioning re-hashes the SAME group key
        // at a finer modulus, so the per-sub aggregation still sees every
        // row of each of its groups.
        //
        // Documented residual constants on top of the threshold: the
        // not-yet-processed partitions' resident batches (bounded at
        // ~threshold total by the ingestion loop), one partition's (or
        // sub-partition's) read-back + aggregation state (bounded at
        // ~threshold by this gate), and the accumulated output batches
        // (irreducible — the operator returns a materialized result).
        let group_cols = self.group_by.len();
        let n_aggs = self.aggregates.len();
        let n_distinct = self
            .aggregates
            .iter()
            .filter(|a| {
                a.distinct || matches!(a.func, crate::planner::AggregateFunction::CountDistinct)
            })
            .count();

        let mut all_results = Vec::new();
        for idx in 0..NUM_PARTITIONS {
            let resident = in_memory_partitions[idx].take();
            let spilled = spilled_parts[idx].take();
            let resident_rows = resident.as_ref().map(|p| p.rows).unwrap_or(0);
            let resident_bytes = resident.as_ref().map(|p| p.memory_bytes).unwrap_or(0);
            let spilled_rows = spilled.as_ref().map(|s| s.rows).unwrap_or(0);
            let spilled_bytes = spilled.as_ref().map(|s| s.raw_bytes).unwrap_or(0);
            let rows = resident_rows + spilled_rows;
            if rows == 0 {
                continue;
            }
            let raw_bytes = resident_bytes + spilled_bytes;
            let predicted =
                raw_bytes + predicted_agg_state_bytes(rows, group_cols, n_aggs, n_distinct);
            // An empty GROUP BY cannot be sub-partitioned (one global group
            // — every row hashes identically, so a finer modulus would put
            // everything in one sub anyway); its state is O(1) except for
            // DISTINCT's value set, which is irreducible without a sorted
            // spill of the values themselves (documented residual).
            let result_batches = if !self.group_by.is_empty() && predicted > memory_threshold {
                self.aggregate_partition_chunked(
                    idx,
                    resident,
                    spilled,
                    predicted,
                    memory_threshold,
                    &spill_dir,
                    &agg_exprs,
                )?
            } else {
                let mut batches: Vec<RecordBatch> = Vec::new();
                if let Some(sp) = &spilled {
                    batches.extend(read_parquet(&sp.file)?);
                }
                if let Some(part) = resident {
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
                    vec![result]
                } else {
                    Vec::new()
                }
            };
            all_results.extend(result_batches);
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
    /// Evict one aggregate partition's resident batches to its (single,
    /// incrementally appended) spill file, leaving the partition resident
    /// and empty so it keeps accumulating in memory afterwards — an
    /// aggregate re-reads spilled + resident rows TOGETHER at finalize, so
    /// unlike the join's `evict_build_partition_to_disk` (which retires the
    /// partition entirely) there is no resident-vs-spilled routing
    /// dichotomy to maintain here.
    ///
    /// oom-safety-hardening task 002: this replaces the old
    /// `write_batches_to_parquet` + `merge_parquet_files` (read the whole
    /// existing file + rewrite + rename on EVERY eviction — the same
    /// O(n²)-in-bytes pattern `append_batch_streaming`'s doc comment
    /// records being fixed for the join in spill-join-correctness task
    /// 002) with the join's own open-writer plumbing, reused byte-for-byte:
    /// one `ArrowWriter` per partition, kept open across the whole
    /// ingestion phase, each eviction appending O(batch) row groups.
    fn evict_agg_partition_to_disk(
        &self,
        idx: usize,
        partitions: &mut [Option<AggregatePartition>],
        spilled: &mut [Option<SpilledAggPartition>],
        spill_writers: &mut [Option<ArrowWriter<File>>],
        spill_dir: &Path,
    ) -> Result<usize> {
        let Some(part) = partitions[idx].as_mut() else {
            return Ok(0);
        };
        if part.batches.is_empty() {
            return Ok(0);
        }
        let path = spill_dir.join(format!("agg_part_{}.parquet", idx));
        for b in &part.batches {
            append_batch_streaming(&mut spill_writers[idx], &path, b)?;
        }
        let freed = part.memory_bytes;
        let rows = part.rows;
        self.memory_pool.record_spill(freed);
        let sp = spilled[idx].get_or_insert_with(|| SpilledAggPartition {
            file: path,
            rows: 0,
            raw_bytes: 0,
        });
        sp.rows += rows;
        sp.raw_bytes += freed;
        part.clear();
        Ok(freed)
    }

    /// Process input with hash partitioning and spilling when memory limit
    /// is reached. Consumes `input_stream` batch by batch — combined with
    /// `execute()`'s streaming phase-1 reservation this is what keeps the
    /// spill path's ingestion memory bounded at ~`memory_threshold`
    /// regardless of input size (oom-safety-hardening task 002).
    async fn aggregate_with_spilling(
        &self,
        mut input_stream: RecordBatchStream,
        spill_dir: &PathBuf,
    ) -> Result<(
        Vec<Option<AggregatePartition>>,
        Vec<Option<SpilledAggPartition>>,
    )> {
        let mut partitions: Vec<Option<AggregatePartition>> = (0..NUM_PARTITIONS)
            .map(|_| Some(AggregatePartition::new()))
            .collect();
        let mut spilled: Vec<Option<SpilledAggPartition>> =
            (0..NUM_PARTITIONS).map(|_| None).collect();
        // One incrementally-written Parquet file per spilled partition,
        // kept OPEN for the whole ingestion phase and closed exactly once
        // at the end — the join's `build_with_partitioning` plumbing,
        // reused (see `evict_agg_partition_to_disk`).
        let mut spill_writers: Vec<Option<ArrowWriter<File>>> =
            (0..NUM_PARTITIONS).map(|_| None).collect();

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
            if big.num_rows() > AGG_CHUNK_ROWS {
                let mut off = 0;
                while off < big.num_rows() {
                    let len = AGG_CHUNK_ROWS.min(big.num_rows() - off);
                    chunks.push(big.slice(off, len));
                    off += len;
                }
            } else {
                chunks.push(big);
            }

            for batch in chunks {
                let batch_size = estimate_batch_size(&batch);

                // Check if we need to spill before adding more data.
                //
                // Ingestion charges resident partitions their BATCH bytes
                // only — deliberately NOT a predicted-state charge the way
                // the join's streaming loop charges predicted table bytes
                // (task 007): during aggregate ingestion no hash/accumulator
                // state exists yet (partitions hold raw rows), and the
                // state that eventually materializes at finalize covers
                // SPILLED rows too, so charging it against residency here
                // could not bound it. The state cost is priced where it
                // actually materializes: `execute()`'s finalize gate
                // (`predicted_agg_state_bytes` + `aggregate_partition_chunked`).
                if total_memory + batch_size > memory_threshold {
                    // Spill the largest partition (largest-first, matching
                    // the join: maximizes freed bytes per eviction).
                    if let Some(idx) = find_largest_agg_partition(&partitions) {
                        let freed = self.evict_agg_partition_to_disk(
                            idx,
                            &mut partitions,
                            &mut spilled,
                            &mut spill_writers,
                            spill_dir,
                        )?;
                        total_memory = total_memory.saturating_sub(freed);
                    }
                }

                // Partition the batch by group key hash
                let partitioned =
                    partition_batch_by_group_hash(&batch, &self.group_by, NUM_PARTITIONS)?;

                for (idx, part_batch) in partitioned.into_iter().enumerate() {
                    if let Some(pb) = part_batch {
                        let pb_size = estimate_batch_size(&pb);
                        if let Some(ref mut part) = partitions[idx] {
                            part.add_batch(pb);
                            total_memory += pb_size;
                        }
                    }
                }
            }
        }

        close_spill_writers(spill_writers)?;
        Ok((partitions, spilled))
    }

    /// Chunked (sub-partitioned) read-back for one aggregate partition
    /// whose priced finalize cost (raw batch bytes + predicted aggregation
    /// state, see `execute()`'s finalize gate) exceeds the memory
    /// threshold — the aggregate analogue of task 007's chunked read-back
    /// in the join's `process_spilled_partition`.
    ///
    /// A join can chunk its build rows arbitrarily (probing is per-row);
    /// an aggregate cannot — every row of a group must reach ONE hash
    /// state. So chunking here is BY GROUP: the partition's rows (spill
    /// file streamed in `AGG_CHUNK_ROWS` batches, then the resident
    /// remainder) are re-routed with the SAME group-key hash at modulus
    /// `NUM_PARTITIONS * fan`. Rows of top-level partition `idx` satisfy
    /// `hash % NUM_PARTITIONS == idx`, so exactly `fan` residues
    /// `{idx + NUM_PARTITIONS * j}` occur; sub-partition `j = i /
    /// NUM_PARTITIONS` therefore holds a group-disjoint subset and each
    /// sub-aggregation sees every row of each of its groups — the same
    /// disjointness argument the top-level partitioning already relies on.
    /// One level only: a sub-partition is aggregated directly (documented
    /// residual: one sub's real state, targeted at ~threshold by `fan`).
    #[allow(clippy::too_many_arguments)]
    fn aggregate_partition_chunked(
        &self,
        idx: usize,
        resident: Option<AggregatePartition>,
        spilled: Option<SpilledAggPartition>,
        predicted_bytes: usize,
        memory_threshold: usize,
        spill_dir: &Path,
        agg_exprs: &[crate::physical::operators::hash_agg::AggregateExpr],
    ) -> Result<Vec<RecordBatch>> {
        let fan = predicted_bytes
            .div_ceil(memory_threshold.max(1))
            .clamp(2, NUM_PARTITIONS);
        let sub_paths: Vec<PathBuf> = (0..fan)
            .map(|j| spill_dir.join(format!("agg_sub_{}_{}.parquet", idx, j)))
            .collect();
        let mut sub_writers: Vec<Option<ArrowWriter<File>>> = (0..fan).map(|_| None).collect();
        if std::env::var("QE_SPILL_DEBUG").is_ok() {
            eprintln!(
                "[spill-agg] partition {} finalize predicted {} bytes > threshold {} — \
                 chunked read-back with fan {}",
                idx, predicted_bytes, memory_threshold, fan
            );
        }

        let group_by = &self.group_by;
        let route =
            |batch: &RecordBatch, sub_writers: &mut [Option<ArrowWriter<File>>]| -> Result<()> {
                let pieces = partition_batch_by_group_hash(batch, group_by, NUM_PARTITIONS * fan)?;
                for (i, pb) in pieces.into_iter().enumerate() {
                    let Some(pb) = pb else { continue };
                    if i % NUM_PARTITIONS != idx {
                        // Would silently split a group across sub-partitions —
                        // the group-hash must route identically at write time
                        // and read-back time. Fail loudly instead of ever
                        // returning wrong aggregates.
                        return Err(QueryError::Execution(format!(
                            "aggregate spill sub-partitioning routed rows of partition {} to \
                         residue {} — group-hash routing diverged across the spill round trip",
                            idx,
                            i % NUM_PARTITIONS
                        )));
                    }
                    let j = i / NUM_PARTITIONS;
                    append_batch_streaming(&mut sub_writers[j], &sub_paths[j], &pb)?;
                }
                Ok(())
            };

        if let Some(sp) = &spilled {
            // Stream the spill file back in bounded batches — never the
            // whole partition at once (that materialization is exactly
            // what this path exists to avoid).
            let file = File::open(&sp.file).map_err(|e| {
                QueryError::Execution(format!(
                    "Failed to open agg spill file {:?}: {}",
                    sp.file, e
                ))
            })?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
                .with_batch_size(AGG_CHUNK_ROWS)
                .build()?;
            for batch in reader {
                route(&batch?, &mut sub_writers)?;
            }
        }
        if let Some(part) = resident {
            for b in &part.batches {
                route(b, &mut sub_writers)?;
            }
        }
        if let Some(sp) = &spilled {
            let _ = std::fs::remove_file(&sp.file);
        }
        close_spill_writers(sub_writers)?;

        let mut results = Vec::new();
        for path in &sub_paths {
            if !path.exists() {
                continue;
            }
            let batches = read_parquet(path)?;
            let _ = std::fs::remove_file(path);
            if batches.is_empty() {
                continue;
            }
            let result = crate::physical::operators::hash_agg::aggregate_batches_external(
                &batches,
                &self.group_by,
                agg_exprs,
                &self.schema,
            )?;
            if result.num_rows() > 0 {
                results.push(result);
            }
        }
        Ok(results)
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

        // Phase 1: reserve, possibly spill (oom-safety-hardening task 003 —
        // the same Photon-style streaming two-phase reservation the join's
        // `compute_build_decision` and the aggregate's `execute()` (task
        // 002) already ship, mirrored rather than reinvented). The OLD code
        // called `collect_input_partitions_concurrently`, which fully
        // drained the ENTIRE input into one flat `Vec<RecordBatch>` and
        // only THEN compared its total size against
        // `memory_limit * spill_threshold` — so an oversized input could
        // exhaust real memory during that initial collection, before the
        // spill decision ever ran (the harness `sort` scenario: 250M rows /
        // ~4GB streamed into a 256MB budget was kernel-OOM-killed under a
        // 1G cgroup cap, and aborted at ~1.9GB under the rlimit lever).
        // Now the input streams in via `stream_merge_input_partitions`
        // (same one-task-per-partition cross-core drain benefit, bounded
        // channel instead of a growing Vec) with a running size total
        // checked as each batch arrives.
        let memory_threshold =
            (self.config.memory_limit as f64 * self.config.spill_threshold) as usize;
        let mut input_stream = stream_merge_input_partitions(&self.input).await?;
        let mut flat_batches: Vec<RecordBatch> = Vec::new();
        let mut flat_size: usize = 0;
        let mut crossing_batch: Option<RecordBatch> = None;
        while let Some(batch) = input_stream.try_next().await? {
            let batch_size = estimate_batch_size(&batch);
            if flat_size + batch_size > memory_threshold {
                crossing_batch = Some(batch);
                break;
            }
            flat_size += batch_size;
            flat_batches.push(batch);
        }

        if crossing_batch.is_none() {
            // Never crossed the threshold: the input genuinely fits — the
            // ordinary in-memory case, reached without ever risking an
            // unbounded collection to get here.
            if flat_batches.is_empty() {
                return Ok(Box::pin(stream::empty()));
            }
            // Data fits in memory — use the regular SortExec path for
            // correctness. Create a temporary MemoryTableExec with our
            // already-collected data.
            let mem = crate::physical::operators::MemoryTableExec::new(
                "sort_input",
                self.schema.clone(),
                flat_batches,
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

        // Threshold crossed mid-stream — external sort with spilling.
        self.config.ensure_spill_dir()?;

        let spill_id = SPILL_COUNTER.fetch_add(1, Ordering::Relaxed);
        let spill_dir = self
            .config
            .spill_path
            .join(format!("sort_{}_{}", partition, spill_id));
        std::fs::create_dir_all(&spill_dir).map_err(|e| {
            QueryError::Execution(format!("Failed to create spill directory: {}", e))
        })?;

        if std::env::var("QE_SPILL_DEBUG").is_ok() {
            eprintln!(
                "[spill-sort] threshold crossed mid-stream at {} buffered batches ({} bytes, \
                 threshold {}) — entering streaming run-generation path",
                flat_batches.len() + 1,
                flat_size,
                memory_threshold
            );
        }

        // Phase 2: everything gathered so far (`flat_batches`), plus the
        // crossing batch, plus whatever remains of the stream, feeds
        // `generate_runs`'s bounded buffer-to-threshold / cut-sorted-run
        // ingestion loop. Nothing collected in phase 1 is thrown away or
        // re-pulled from the source (mirrors the aggregate's phase 2).
        let combined: RecordBatchStream = Box::pin(
            stream::iter(flat_batches.into_iter().chain(crossing_batch).map(Ok))
                .chain(input_stream),
        );

        // Generate sorted runs, batch by batch.
        let runs = self.generate_runs(combined, &spill_dir).await?;

        if std::env::var("QE_SPILL_DEBUG").is_ok() {
            eprintln!(
                "[spill-sort] {} sorted run(s) generated in {:?}",
                runs.len(),
                spill_dir
            );
        }

        if runs.is_empty() {
            let _ = std::fs::remove_dir_all(&spill_dir);
            return Ok(Box::pin(stream::empty()));
        }

        // Merge the runs on a blocking thread, delivering merged batches
        // through a small bounded channel that IS this operator's output
        // stream — the merged result is never re-materialized into one
        // Vec (the full sorted output is the size of the entire input,
        // exactly what this operator just spilled to AVOID holding). The
        // merge machinery itself is unchanged (same multi-pass reduction,
        // same k-way merge core, same carried-run bookkeeping and
        // buffer-transition flush) — only the DELIVERY of already-built
        // output batches changed from `Vec::push` to a channel send.
        //
        // The merge thread owns spill-dir cleanup: it removes the
        // directory when the merge finishes, errors, OR aborts early
        // because the receiver was dropped (query abandoned / LIMIT
        // satisfied below — the next sink send fails and the merge
        // returns).
        let merger = self.merger();
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<RecordBatch>>(4);
        let cleanup_dir = spill_dir.clone();
        tokio::task::spawn_blocking(move || {
            let batch_tx = tx.clone();
            let mut sink = move |batch: RecordBatch| -> Result<bool> {
                Ok(batch_tx.blocking_send(Ok(batch)).is_ok())
            };
            if let Err(e) = merger.merge_runs_into(&runs, &mut sink) {
                let _ = tx.blocking_send(Err(e));
            }
            let _ = std::fs::remove_dir_all(&cleanup_dir);
        });
        let merged = tokio_stream::wrappers::ReceiverStream::new(rx);

        // Apply the top-K `fetch` limit, mirroring the in-memory branch
        // above (`SortExec::with_fetch`). The top-k fusion rule in
        // `planner.rs` (`LogicalPlan::Limit` folding a `skip == 0` LIMIT
        // directly into `ExternalSortExec::with_fetch` instead of wrapping
        // a separate `LimitExec` around it) means THIS is the only place a
        // spilled `ORDER BY ... LIMIT N` query's row count is ever
        // truncated — the merge produces every row of the sort, not just
        // the top-K prefix (spill-join-correctness-2 task 004's fix,
        // preserved). Applied per arriving batch via the SAME
        // `truncate_batches_to_limit` helper (one implementation of the
        // slicing semantics, not two); once the limit is reached the
        // stream ends, the receiver drops, and the merge thread aborts
        // early instead of merging rows nobody will read.
        match self.fetch {
            Some(fetch) => {
                let limited = merged.scan(fetch, |remaining, item| {
                    futures::future::ready(match item {
                        Err(e) => {
                            *remaining = 0;
                            Some(Err(e))
                        }
                        Ok(batch) => {
                            if *remaining == 0 {
                                None
                            } else {
                                let mut kept = truncate_batches_to_limit(vec![batch], *remaining);
                                match kept.pop() {
                                    Some(b) => {
                                        *remaining -= b.num_rows();
                                        Some(Ok(b))
                                    }
                                    None => None,
                                }
                            }
                        }
                    })
                });
                Ok(Box::pin(limited))
            }
            None => Ok(Box::pin(merged)),
        }
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

    /// Build the owned, `'static` merge-machinery handle `execute()`'s
    /// spill branch hands to its blocking merge thread. The merge methods
    /// live on `SortRunMerger` (not `&self`) purely so they can be MOVED
    /// onto that thread — every algorithm is byte-for-byte the code that
    /// used to live on `ExternalSortExec` itself.
    fn merger(&self) -> SortRunMerger {
        SortRunMerger {
            order_by: self.order_by.clone(),
            schema: self.schema.clone(),
        }
    }

    /// Test-only Vec-collecting entry point (regression tests
    /// `external_sort_multi_pass_merge_survives_a_leftover_singleton_chunk`
    /// etc. call this); production merging goes through
    /// `SortRunMerger::merge_runs_into`'s sink form.
    #[cfg(test)]
    fn merge_runs(&self, runs: &[PathBuf]) -> Result<Vec<RecordBatch>> {
        let merger = self.merger();
        let mut out = Vec::new();
        let mut sink = |batch: RecordBatch| -> Result<bool> {
            out.push(batch);
            Ok(true)
        };
        merger.merge_runs_into(runs, &mut sink)?;
        Ok(out)
    }

    /// Test-only Vec-collecting entry point (regression test
    /// `k_way_merge_survives_a_run_needing_more_than_one_buffer_load`
    /// calls this).
    #[cfg(test)]
    fn streaming_k_way_merge(
        &self,
        runs: &[PathBuf],
        buffer_rows: usize,
    ) -> Result<Vec<RecordBatch>> {
        let merger = self.merger();
        let mut out = Vec::new();
        let mut sink = |batch: RecordBatch| -> Result<bool> {
            out.push(batch);
            Ok(true)
        };
        merger.streaming_k_way_merge_into(runs, buffer_rows, &mut sink)?;
        Ok(out)
    }
}

/// The run-merge machinery of `ExternalSortExec`, factored onto an owned,
/// `Send + 'static` value so `execute()`'s spill branch can run it on a
/// `spawn_blocking` thread that streams merged batches out through a
/// bounded channel instead of accumulating the ENTIRE sorted output into
/// one `Vec<RecordBatch>` (which is the size of the whole input — exactly
/// the materialization this operator spills to avoid;
/// oom-safety-hardening task 003). The algorithms themselves — multi-pass
/// reduction, carried-run bookkeeping, the k-way merge core with its
/// buffer-transition flush, `batch_with_actual_types` reconciliation —
/// are unchanged from when these methods lived on `ExternalSortExec`.
///
/// Every output-producing method takes a `sink: &mut dyn FnMut(RecordBatch)
/// -> Result<bool>`; a sink returning `Ok(false)` means "stop producing"
/// (the consumer is gone — query abandoned, or a fused `LIMIT` already
/// satisfied) and the merge returns early and cleanly.
struct SortRunMerger {
    order_by: Vec<crate::planner::SortExpr>,
    schema: SchemaRef,
}

impl SortRunMerger {
    /// Maximum number of runs to merge at once.
    const MAX_MERGE_FANIN: usize = 8;
    /// Maximum rows to buffer per run during merge.
    const MERGE_BUFFER_ROWS: usize = 8192;

    /// Merge `runs` (already individually sorted) into globally-sorted
    /// output batches, delivered through `sink`.
    fn merge_runs_into(
        &self,
        runs: &[PathBuf],
        sink: &mut dyn FnMut(RecordBatch) -> Result<bool>,
    ) -> Result<()> {
        if runs.is_empty() {
            return Ok(());
        }

        if runs.len() == 1 {
            // A single run is already globally sorted — stream it straight
            // off disk in bounded reads (the old code's `read_parquet`
            // one-shot load, made incremental).
            if !runs[0].is_file() {
                return Ok(());
            }
            let file = File::open(&runs[0]).map_err(|e| {
                QueryError::Execution(format!("Failed to open run file {:?}: {}", runs[0], e))
            })?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
                .with_batch_size(Self::MERGE_BUFFER_ROWS)
                .build()?;
            for batch in reader {
                if !sink(batch?)? {
                    return Ok(());
                }
            }
            return Ok(());
        }

        // If we have too many runs, reduce in multiple passes first.
        if runs.len() > Self::MAX_MERGE_FANIN {
            let reduced = self.reduce_runs_to_fanin(runs, Self::MAX_MERGE_FANIN)?;
            return self.streaming_k_way_merge_into(&reduced, Self::MERGE_BUFFER_ROWS, sink);
        }

        // Single-pass k-way merge with bounded memory.
        self.streaming_k_way_merge_into(runs, Self::MERGE_BUFFER_ROWS, sink)
    }

    /// Multi-pass reduction for when there are too many runs: repeatedly
    /// merge `fanin`-sized chunks of runs into new on-disk runs until at
    /// most `fanin` remain (the FINAL merge is the caller's, so it can
    /// stream). Each chunk's merged output is sunk straight into an open
    /// `ArrowWriter` (`append_batch_streaming`, the join/agg spill paths'
    /// own open-writer plumbing) instead of the previous
    /// collect-into-a-Vec-then-`write_batches_to_parquet` — a chunk of
    /// `fanin` runs each ~`memory_threshold`-sized would otherwise
    /// materialize `fanin * threshold` bytes per intermediate pass.
    fn reduce_runs_to_fanin(&self, runs: &[PathBuf], fanin: usize) -> Result<Vec<PathBuf>> {
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
                    // Merge this chunk into a new run, streamed batch by
                    // batch into an open writer (lazily created on the
                    // first batch, so an empty chunk merge creates no file
                    // and pushes no run — mirrors the old
                    // `if !merged.is_empty()` guard).
                    let output_path =
                        spill_dir.join(format!("merged_pass{}_{}.parquet", pass, next_runs.len()));
                    let mut writer_slot: Option<ArrowWriter<File>> = None;
                    {
                        let mut sink = |batch: RecordBatch| -> Result<bool> {
                            append_batch_streaming(&mut writer_slot, &output_path, &batch)?;
                            Ok(true)
                        };
                        self.streaming_k_way_merge_into(chunk, Self::MERGE_BUFFER_ROWS, &mut sink)?;
                    }
                    if let Some(writer) = writer_slot {
                        writer.close()?;
                        next_runs.push(output_path);
                    }
                }
            }

            // Clean up old runs from previous pass (except original runs).
            //
            // A chunk of exactly one leftover run (`chunk.len() == 1` above,
            // whenever `current_runs.len()` isn't an exact multiple of
            // `fanin`) is carried forward into `next_runs` UNCHANGED — same
            // path, not rewritten to a new `merged_pass{N}_*.parquet` file.
            // Unconditionally deleting every path in `current_runs` here
            // (the previous behavior) deleted that carried-forward file too,
            // even though `next_runs` (this iteration's own output, about to
            // become `current_runs` for the NEXT iteration or the final
            // merge below) still points at it — a real, deterministic crash
            // ("Failed to open run file ... No such file or directory") on
            // any multi-pass merge whose leftover-chunk arithmetic hits this
            // shape, not a rare/timing-dependent one. Skip deleting any path
            // that's still referenced by `next_runs`.
            if pass > 0 {
                let still_needed: std::collections::HashSet<&PathBuf> = next_runs.iter().collect();
                for run in &current_runs {
                    if !still_needed.contains(run) {
                        let _ = std::fs::remove_file(run);
                    }
                }
            }

            current_runs = next_runs;
            pass += 1;
        }

        Ok(current_runs)
    }

    /// Streaming k-way merge with bounded memory usage, delivering each
    /// built output batch through `sink` as soon as it exists instead of
    /// accumulating a `result_batches` Vec (whose total size is the whole
    /// merged output). The merge ALGORITHM — row-by-row min-scan, the
    /// buffer-transition flush, `build_merged_batch`(+`_final` fallback)
    /// construction — is unchanged.
    ///
    /// One hoist, semantics-preserving (oom-safety-hardening task 003):
    /// sort-key columns are evaluated ONCE per LOADED buffer
    /// (`eval_key_cols`), not twice per row-COMPARISON as the old
    /// `compare_rows` closure did — `evaluate_expr` is deterministic per
    /// batch, so per-comparison re-evaluation only cost time (billions of
    /// calls for a 250M-row merge: rows × fan-in × keys × 2), never
    /// changed a result. A key expression that fails to evaluate is
    /// skipped for the comparison (both sides), exactly as the old
    /// `.ok()`-based closure did.
    fn streaming_k_way_merge_into(
        &self,
        runs: &[PathBuf],
        buffer_rows: usize,
        sink: &mut dyn FnMut(RecordBatch) -> Result<bool>,
    ) -> Result<()> {
        use std::cmp::Ordering;

        if runs.is_empty() {
            return Ok(());
        }

        // Open iterators for each run
        let mut run_iterators: Vec<
            Box<dyn Iterator<Item = std::result::Result<RecordBatch, arrow::error::ArrowError>>>,
        > = Vec::new();
        let mut run_buffers: Vec<Option<RecordBatch>> = Vec::new();
        let mut run_indices: Vec<usize> = Vec::new(); // Current row index in each buffer
                                                      // Hoisted sort-key columns for each run's CURRENT buffer, one
                                                      // entry per `order_by` expression (None = that key failed to
                                                      // evaluate against this buffer — skipped in comparisons, matching
                                                      // the old per-comparison `.ok()` behavior).
        let mut run_keys: Vec<Vec<Option<ArrayRef>>> = Vec::new();

        let eval_key_cols = |batch: &RecordBatch| -> Vec<Option<ArrayRef>> {
            self.order_by
                .iter()
                .map(|s| evaluate_expr(batch, &s.expr).ok())
                .collect()
        };

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
            run_keys.push(Vec::new());
        }

        // Load initial batch from each run
        for (i, iter) in run_iterators.iter_mut().enumerate() {
            if let Some(batch_result) = iter.next() {
                let batch = batch_result?;
                run_keys[i] = eval_key_cols(&batch);
                run_buffers[i] = Some(batch);
                run_indices[i] = 0;
            }
        }

        // Build output batches using a simple row-by-row merge
        // For better performance, we'd want to do vectorized merge, but this is memory-safe
        let mut output_rows: Vec<(usize, usize)> = Vec::new(); // (run_idx, row_idx)

        // Helper to compare rows via the hoisted key columns.
        let compare_rows = |keys_a: &[Option<ArrayRef>],
                            row_a: usize,
                            keys_b: &[Option<ArrayRef>],
                            row_b: usize,
                            order_by: &[crate::planner::SortExpr]|
         -> std::cmp::Ordering {
            for (key_idx, sort_expr) in order_by.iter().enumerate() {
                if let (Some(a), Some(b)) = (&keys_a[key_idx], &keys_b[key_idx]) {
                    let cmp = compare_array_values(a, row_a, b, row_b);
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
                                    &run_keys[run_idx],
                                    run_indices[run_idx],
                                    &run_keys[current_min],
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
                                output_rows.clear();
                                if !sink(batch)? {
                                    return Ok(());
                                }
                            }
                            // Try to load next batch from this run
                            if let Some(next_batch) = run_iterators[run_idx].next() {
                                let batch = next_batch?;
                                run_keys[run_idx] = eval_key_cols(&batch);
                                run_buffers[run_idx] = Some(batch);
                                run_indices[run_idx] = 0;
                            } else {
                                run_buffers[run_idx] = None;
                                run_keys[run_idx] = Vec::new();
                            }
                        }
                    }

                    // Flush output when buffer is full
                    if output_rows.len() >= buffer_rows {
                        let batch = self.build_merged_batch(&run_buffers, &output_rows)?;
                        output_rows.clear();
                        if !sink(batch)? {
                            return Ok(());
                        }
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
            let batch = self.build_merged_batch_final(runs, &output_rows, buffer_rows)?;
            if !sink(batch)? {
                return Ok(());
            }
        }

        Ok(())
    }

    /// Build a merged batch from the given row references.
    ///
    /// Fast path (oom-safety-hardening task 003, semantics-preserving):
    /// when every referenced run buffer is present, wide enough, and the
    /// referenced buffers agree on column types, gather with ONE
    /// `arrow::compute::interleave` call per column — the kernel built for
    /// exactly this (row_idx, batch)-addressed k-way-merge output shape —
    /// instead of the generic path below, which materializes a fresh
    /// 1-ROW array per output row per column and then concatenates
    /// thousands of them (the dominant merge cost at real spill scale:
    /// 2 arrays/row/column). The generic path is KEPT verbatim as the
    /// fallback for any shape the fast path can't prove safe (missing
    /// buffer, short batch, cross-run type disagreement), and both paths
    /// end in the same `batch_with_actual_types` reconciliation.
    fn build_merged_batch(
        &self,
        run_buffers: &[Option<RecordBatch>],
        rows: &[(usize, usize)],
    ) -> Result<RecordBatch> {
        if rows.is_empty() {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        let num_cols = self.schema.fields().len();

        // ---- interleave fast path ------------------------------------
        let mut referenced: Vec<usize> = rows.iter().map(|&(run_idx, _)| run_idx).collect();
        referenced.sort_unstable();
        referenced.dedup();

        let mut fast_ok = referenced
            .iter()
            .all(|&r| matches!(&run_buffers[r], Some(b) if b.num_columns() >= num_cols));
        if fast_ok {
            if let Some((&first_ref, rest)) = referenced.split_first() {
                let first = run_buffers[first_ref].as_ref().unwrap();
                'cols: for col_idx in 0..num_cols {
                    let dt = first.column(col_idx).data_type();
                    for &r in rest {
                        if run_buffers[r].as_ref().unwrap().column(col_idx).data_type() != dt {
                            fast_ok = false;
                            break 'cols;
                        }
                    }
                }
            }
        }
        if fast_ok {
            // Dense position of each referenced run in the interleave
            // input array list.
            let mut dense_pos = vec![usize::MAX; run_buffers.len()];
            for (pos, &r) in referenced.iter().enumerate() {
                dense_pos[r] = pos;
            }
            let indices: Vec<(usize, usize)> = rows
                .iter()
                .map(|&(run_idx, row_idx)| (dense_pos[run_idx], row_idx))
                .collect();
            let mut final_columns: Vec<ArrayRef> = Vec::with_capacity(num_cols);
            for col_idx in 0..num_cols {
                let arrays: Vec<&dyn arrow::array::Array> = referenced
                    .iter()
                    .map(|&r| run_buffers[r].as_ref().unwrap().column(col_idx).as_ref())
                    .collect();
                final_columns.push(compute::interleave(&arrays, &indices)?);
            }
            return batch_with_actual_types(&self.schema, final_columns);
        }
        // ---- generic fallback (pre-existing path, unchanged) ---------

        // Group rows by run
        let mut run_row_groups: HashMap<usize, Vec<(usize, usize)>> = HashMap::new();
        for (output_idx, &(run_idx, row_idx)) in rows.iter().enumerate() {
            run_row_groups
                .entry(run_idx)
                .or_default()
                .push((output_idx, row_idx));
        }

        // Build output columns
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

/// Truncate a sequence of already-globally-sorted batches to the first
/// `limit` rows total, dropping/slicing batches as needed. Used by
/// `ExternalSortExec::execute()`'s spill branch to apply a `LIMIT` fused
/// into a `fetch` (see the top-k fusion rule in `planner.rs`) — the merge
/// step produces every row of the sort, not just the top-K prefix, so this
/// is the only place that prefix gets cut down to exactly `limit` rows.
fn truncate_batches_to_limit(batches: Vec<RecordBatch>, limit: usize) -> Vec<RecordBatch> {
    let mut remaining = limit;
    let mut out = Vec::new();
    for batch in batches {
        if remaining == 0 {
            break;
        }
        if batch.num_rows() <= remaining {
            remaining -= batch.num_rows();
            out.push(batch);
        } else {
            out.push(batch.slice(0, remaining));
            remaining = 0;
        }
    }
    out
}

/// Estimate the memory size of a RecordBatch
/// Bytes a batch LOGICALLY holds, computed slice-aware.
///
/// `get_array_memory_size()` reports the CAPACITY of the underlying buffers,
/// which for an array sliced out of a larger allocation — every batch an
/// mmap-backed IPC read produces — counts the whole mapping per batch. Spill
/// decisions made on that number spill everything unconditionally (and the
/// spilled join's build-side estimate was off by ~50x, and by ~4,000x for a
/// Dictionary-column-heavy build side — see below). Offsets-based sizing
/// counts the rows the batch actually references; unknown types fall back to
/// a slice-aware generic computation (see below), which over- rather than
/// under-counts (the safe direction for a spill decision) but never counts
/// an mmap segment's whole on-disk size.
///
/// `spill-size-estimate-fix` epic, task 001 (2026-08-28): the generic
/// fallback used to be `c.get_array_memory_size()`, which walks each
/// `Buffer`'s `capacity()` — and for a `Buffer` built by
/// `Buffer::from_custom_allocation` (every column read through
/// `ipc_cache.rs`'s mmap path, i.e. every native-table and IPC-sidecar
/// scan), `capacity()` unconditionally returns the FULL LENGTH PASSED AT
/// MMAP TIME, forever, regardless of how small a slice/sub-buffer is later
/// carved out of it (`Bytes::capacity()`'s `Deallocation::Custom(_, size)`
/// arm returns the original `size`, not the current view's length — see
/// arrow-buffer's `bytes.rs`). One mmap `Buffer` backs an ENTIRE segment
/// file and is shared by every column's every sub-buffer in that segment,
/// so ANY type reaching this fallback reported the whole segment file's
/// on-disk size as its own — confirmed via `examples/
/// spill_size_estimate_check.rs`: Q12's real ~42MB Dictionary-column-heavy
/// build side estimated at ~167.7GB (~4,000x over), which crossed the spill
/// threshold and forced an unnecessary spill.
///
/// Fix: fall back to `ArrayData::get_slice_memory_size()` (arrow-data)
/// instead. It computes size purely from the array's own LOGICAL length and
/// data-type layout — for `Dictionary(K, V)` specifically: `self.len *
/// size_of::<K>()` for the keys (the type's own `layout()` delegates to the
/// key type's fixed-width layout) PLUS the dictionary VALUES array's own
/// real content bytes, computed by recursing into `child_data[0]` (arrow
/// stores a `DictionaryArray`'s values as its first — and only — child
/// `ArrayData`) — i.e. exactly `keys.len() * key_width +
/// dictionary_values_actual_bytes`, this function's own target formula, but
/// generalized by arrow itself to ANY key/value type combination rather
/// than hand-rolled against one hard-coded `Int32Type`/`StringArray` shape.
/// It NEVER reads `Buffer::capacity()`. This generalizes the SAME fix to
/// every other type that previously reached this fallback and could carry
/// the identical mmap-capacity-vs-logical-content problem — `LargeUtf8`/
/// `LargeBinary` (content-aware via `typed_offsets::<i64>()`, the exact
/// same offsets-delta approach this function's own Utf8/Binary branch
/// above already uses by hand), `FixedSizeBinary`/`Union` (fixed-width,
/// scaled by `self.len`, not by buffer capacity), and `List`/`LargeList`/
/// `Struct`/`Map`/`FixedSizeList`/`RunEndEncoded` (offsets sized by
/// `self.len`, recursing into child `ArrayData` the same way Dictionary's
/// values do) — audited against this engine's actual mmap-backed write
/// surface: `NativeManifest::ManifestDataType::from_arrow` (`src/storage/
/// native_manifest.rs`) is the ONLY schema gate a native table (or an IPC
/// sidecar, which mirrors the same coercion) can be written through, and it
/// explicitly REJECTS every nested type (List/Struct/Map/Union/
/// FixedSizeList/RunEndEncoded) at write time — so those are not reachable
/// via this engine's own mmap-backed tables today, but are still fixed here
/// defensively (never worse than the old behavior, no correctness/
/// performance cost when unreached) rather than left as a live landmine for
/// a future writer that adds them. `.unwrap_or_else(|_| c.
/// get_array_memory_size())` is a defensive-only fallback for the
/// `Result`'s error path (not expected to fire for any type this function
/// can be called with) — if it ever does, it lands back on the OLD,
/// over-counting-safe behavior, never a new failure mode.
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
                _ => c
                    .to_data()
                    .get_slice_memory_size()
                    .unwrap_or_else(|_| c.get_array_memory_size()),
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

/// `SpillableHashAggregateExec`'s partition routing (oom-safety-hardening
/// task 002): the same fixed-seed `hash % num_partitions` loop as
/// `partition_batch_by_hash`, with ONE aggregate-specific difference — a
/// `Dictionary`-encoded group-key column is decoded to its value type
/// before key extraction. `extract_join_key`'s downcast chain has no
/// Dictionary arm (it falls through to `JoinValue::Null`), which for the
/// JOIN is merely degenerate routing, but for an aggregate would (a) route
/// EVERY row of a Dictionary-keyed GROUP BY into one partition (still
/// correct — groups never split — but the memory bounding this operator
/// exists for degenerates to nothing) and (b) make the finalize step's
/// sub-partitioning (`aggregate_partition_chunked`) structurally unable to
/// split such a partition. Decoding makes the routing value the SAME
/// string `hash_agg::extract_group_value` resolves a Dictionary key to,
/// and it is applied identically at ingestion time and read-back time
/// (Parquet spill files preserve Dictionary via the embedded Arrow
/// schema), so routing is stable across the spill round trip.
///
/// Deliberately a SEPARATE function rather than a change to
/// `partition_batch_by_hash`: that function is part of the join's
/// build/probe partition routing, which oom-safety-hardening task 002 is
/// hard-bounded from touching (the open ~0.34% duplicate-counting bug
/// lives somewhere in that logic; its routing must stay byte-identical).
fn partition_batch_by_group_hash(
    batch: &RecordBatch,
    group_by: &[Expr],
    num_partitions: usize,
) -> Result<Vec<Option<RecordBatch>>> {
    let key_arrays: Result<Vec<ArrayRef>> = group_by
        .iter()
        .map(|e| {
            let arr = evaluate_expr(batch, e)?;
            match arr.data_type() {
                arrow::datatypes::DataType::Dictionary(_, value_type) => {
                    compute::cast(arr.as_ref(), value_type).map_err(Into::into)
                }
                _ => Ok(arr),
            }
        })
        .collect();
    let key_arrays = key_arrays?;

    let mut partition_indices: Vec<Vec<usize>> = (0..num_partitions).map(|_| Vec::new()).collect();
    for row in 0..batch.num_rows() {
        let key = extract_join_key(&key_arrays, row);
        // Same EXPLICITLY seeded hasher as `partition_batch_by_hash` (see
        // its comment): routing must give the same answer for the same key
        // from every call site — including this function called at modulus
        // `NUM_PARTITIONS` (ingestion) and `NUM_PARTITIONS * fan`
        // (chunked read-back), whose residues must agree mod NUM_PARTITIONS.
        let mut hasher = xxhash_rust::xxh64::Xxh64::new(0x517c_c1b7_2722_0a95);
        key.hash(&mut hasher);
        let partition = (hasher.finish() as usize) % num_partitions;
        partition_indices[partition].push(row);
    }

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

/// Append one batch to a spilled partition's Parquet file as a new row
/// group, via an ALREADY-OPEN streaming writer kept alive across many calls
/// (one per spilled partition, for the whole build or probe phase — see
/// `build_with_partitioning`/`probe_with_spilling`).
///
/// Replaces the previous `append_to_parquet`, which reopened the file on
/// EVERY call: read the schema back off disk, streamed the ENTIRE existing
/// file into a fresh temp file, wrote the one new batch, then renamed the
/// temp file over the original. With `NUM_PARTITIONS` = 64 partitions
/// spilling almost immediately and hundreds of build batches (plus the full
/// probe side) appended progressively, that made each append cost
/// O(current file size) — i.e. the whole build/probe phase cost O(n^2) in
/// bytes read+written for a partition that accumulates n batches.
/// Confirmed (spill-join-correctness epic, task 001) to be a strong,
/// evidenced candidate for why even a CORRECT run of a large spilling join
/// took 140+ seconds, on top of the still-open, unrelated wrong-answer bug
/// investigated by that same epic. `writer_slot` is created lazily on the
/// first call for a given partition (mirrors the old function's "no
/// existing file yet" branch — nothing is written, and no file appears on
/// disk, for a partition that never receives a batch) and reused for every
/// subsequent one, so the cost of any single append no longer grows with
/// how much has already been spilled for that partition. The on-disk
/// SHAPE is unchanged (still exactly one Parquet file per partition) —
/// `read_parquet`/`process_spilled_partition` need no changes.
///
/// **`oom-safety-hardening` epic (2026-08-29): the "kept open for the
/// whole build/probe phase" design above has a real, confirmed memory-
/// accounting hole this fixes.** `ArrowWriter::write()` does NOT flush a
/// batch to disk per call — it buffers into the CURRENT row group's
/// in-progress column encoders until `parquet`'s own row-group limit is
/// hit (this file previously relied on the crate's row-COUNT default,
/// 1,048,576, via an unset `WriterProperties`) or `.close()` runs. With
/// `NUM_PARTITIONS` = 64 spilled partitions and a real build side's rows
/// hash-partitioned roughly evenly across them, MOST spilled partitions
/// never individually accumulate 1M+ rows before the build/probe phase
/// ends — so their data sits fully memory-resident inside the writer's
/// own internal buffer for the ENTIRE phase, invisible to `total_memory`
/// (which already decremented as if that data were freed the moment
/// `evict_build_partition_to_disk` ran). Confirmed live, not just by
/// reading the code: `examples/_control_int32_repro.rs` (a PLAIN Int32
/// build side, no Dictionary column at all — ruling out any connection
/// to this epic's own Dictionary-sizing fix) with a 500MB configured
/// `memory_limit` was genuinely OOM-killed by the kernel at ~3.1GB RSS
/// under a real `systemd-run -p MemoryMax=3G` cap (`journalctl -k`:
/// "Memory cgroup out of memory: Killed process ... (_control_int32_)");
/// `examples/spill_dictionary_oversized_build_repro.rs` at a 30MB limit
/// survived under a 900M cap but peaked at 723MB RSS — 24x its own
/// configured budget.
///
/// **First attempt (byte-based `set_max_row_group_bytes`) measured, and
/// REJECTED — a second instance of the exact bug class this epic exists
/// to fix.** `parquet`'s own doc comment on that setter says row groups
/// "are flushed when their ESTIMATED ENCODED SIZE exceeds this
/// threshold" — encoded/compressed, not raw. Both repros above use
/// deliberately low-cardinality, highly repetitive keys (a 7-entry
/// dictionary; `i % 7` for the control's plain Int32), so their
/// COMPRESSED footprint stays tiny no matter how many raw rows are
/// buffered — the estimate this setting flushes against can be
/// arbitrarily smaller than the actual uncompressed memory being held,
/// the identical "estimate vs. real content" failure mode
/// `estimate_batch_size` was fixed for elsewhere in this file. Measured,
/// not assumed: re-ran both repros with only that fix — peak RSS was
/// UNCHANGED (723MB → 740MB for the dictionary case, within noise; the
/// control case was STILL genuinely OOM-killed at the same ~3G cap).
///
/// Fix that actually measures bounded: `set_max_row_group_row_count`
/// (in addition to, not instead of, the byte cap above as a secondary
/// net for genuinely wide rows) — a ROW-count limit bounds RAW buffered
/// rows directly, immune to how well those rows happen to compress.
/// `SPILL_WRITER_ROW_GROUP_ROWS` (8,192) matches this same file's own
/// `CHUNK_ROWS` precedent (`SpillableHashAggregateExec::
/// aggregate_with_spilling`) rather than inventing a new magic number.
///
/// **CORRECTION (`oom-safety-hardening` task 001, 2026-08-29): the
/// paragraph this one replaced claimed the row-count cap made the
/// control repro "complete cleanly" — that was WRONG** (contradicted by
/// `spill-size-estimate-fix` 001's own Outcome section, which is the
/// honest record: peak RSS unchanged, control still OOM-killed at a 3G
/// cap). This writer-flush fix is real and worth keeping, but it was
/// never the dominant driver. The actual dominant driver, root-caused
/// with the (fixed) `QE_ALLOC_PROFILE` diagnostic allocator: the
/// UNBUDGETED `HashMap<JoinKey, Vec<HashEntry>>` hash tables
/// `execute_spill_path` builds over the in-memory partitions (and each
/// read-back spilled partition) cost ~120-200 bytes/row across three
/// allocations per key — ~10-20x the raw batch bytes the spill decision
/// budgeted — and are never checked against `memory_limit`. See
/// `.claude/epics/oom-safety-hardening/updates/001/stream-A.md` for the
/// full evidence (profiler call-site snapshots, cap-sweep numbers).
fn append_batch_streaming(
    writer_slot: &mut Option<ArrowWriter<File>>,
    path: &PathBuf,
    batch: &RecordBatch,
) -> Result<()> {
    /// Secondary net for genuinely wide rows whose compressed size tracks
    /// their raw size reasonably closely. Matches `DEFAULT_PAGE_SIZE`
    /// (1MB), already used elsewhere in this codebase as a natural unit.
    const SPILL_WRITER_ROW_GROUP_BYTES: usize = 1024 * 1024;
    /// The binding limit for narrow/repetitive rows (see this function's
    /// own doc comment for why the byte-based cap alone measured as a
    /// no-op): bounds RAW buffered rows directly, regardless of how well
    /// they compress. Matches this file's own `CHUNK_ROWS` precedent.
    const SPILL_WRITER_ROW_GROUP_ROWS: usize = 8_192;

    if writer_slot.is_none() {
        let file = File::create(path).map_err(|e| {
            QueryError::Execution(format!("Failed to create spill file {:?}: {}", path, e))
        })?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_bytes(Some(SPILL_WRITER_ROW_GROUP_BYTES))
            .set_max_row_group_row_count(Some(SPILL_WRITER_ROW_GROUP_ROWS))
            .build();
        *writer_slot = Some(ArrowWriter::try_new(file, batch.schema(), Some(props))?);
    }
    writer_slot
        .as_mut()
        .expect("just created above if it was None")
        .write(batch)?;
    Ok(())
}

/// Close every open spill writer exactly once, flushing the final row group
/// and Parquet footer so the file is valid and readable. Must run after a
/// build/probe phase's loop finishes — every `SpilledPartition` file has to
/// be complete before `process_spilled_partition` opens it.
fn close_spill_writers(writers: Vec<Option<ArrowWriter<File>>>) -> Result<()> {
    for writer in writers.into_iter().flatten() {
        writer.close()?;
    }
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

/// Diagnostic-only (`QE_SPILL_DEBUG`) checksum of a batch of join-key
/// values, spill-join-correctness-2 epic task 001. Order-independent (XOR
/// accumulation) so it survives any row reordering across a spill/unspill
/// round trip. Deliberately built on the SAME `extract_join_key` function
/// used by `build_hash_table`/`partition_batch_by_hash` themselves — a
/// mismatch caught by comparing two `KeyChecksum`s can only come from the
/// underlying ARRAY DATA differing between the two calls (e.g. a Parquet
/// round trip changing a column's concrete Arrow type or values), never
/// from the checksum's own logic disagreeing with the real join mechanism.
#[derive(Clone, Copy, Default)]
struct KeyChecksum {
    rows: usize,
    xor_hash: u64,
    /// Rows whose key extraction fell through to `JoinValue::Null` despite
    /// the underlying array being NON-null at that row — i.e. an unhandled
    /// `extract_join_key` array type (see its downcast chain), not a
    /// genuine SQL NULL. Tracked separately so a real mismatch reads as
    /// "spill round-trip changed the data", not conflated with this
    /// already-known, structurally different key-extraction gap.
    unhandled_type_rows: usize,
}

impl KeyChecksum {
    fn accumulate(&mut self, other: KeyChecksum) {
        self.rows += other.rows;
        self.xor_hash ^= other.xor_hash;
        self.unhandled_type_rows += other.unhandled_type_rows;
    }
}

/// Compute a [`KeyChecksum`] for one batch's join keys (`key_exprs`
/// evaluated against `batch`, then `extract_join_key` per row — the exact
/// path `build_hash_table` and `partition_batch_by_hash` use).
fn batch_key_checksum(batch: &RecordBatch, key_exprs: &[Expr]) -> Result<KeyChecksum> {
    let key_arrays: Result<Vec<ArrayRef>> =
        key_exprs.iter().map(|e| evaluate_expr(batch, e)).collect();
    let key_arrays = key_arrays?;
    let mut cs = KeyChecksum::default();
    for row in 0..batch.num_rows() {
        let key = extract_join_key(&key_arrays, row);
        let source_is_null = key_arrays.iter().any(|a| a.is_null(row));
        let key_is_all_null = key.values.iter().all(|v| matches!(v, JoinValue::Null));
        if key_is_all_null && !source_is_null {
            cs.unhandled_type_rows += 1;
        }
        // A dedicated, fixed seed — distinct from `partition_batch_by_hash`'s
        // own routing-hash seed, since this checksum only ever needs to
        // agree with ITSELF (write time vs. read time), never with the
        // routing function.
        let mut hasher = xxhash_rust::xxh64::Xxh64::new(0x9e37_79b9_7f4a_7c15);
        key.hash(&mut hasher);
        cs.xor_hash ^= hasher.finish();
        cs.rows += 1;
    }
    Ok(cs)
}

/// Approximate per-heap-allocation bookkeeping overhead (allocator bin
/// rounding + header amortization under mimalloc). Every 1-element
/// `Vec<JoinValue>` / `Vec<HashEntry>` a join hash table holds is its own
/// heap allocation, so at millions of keys this overhead is a first-order
/// term, not noise — oom-safety-hardening task 001's profiler evidence
/// measured ~300MB of exactly these untracked small allocations at the
/// dict repro's 640MB peak.
const HEAP_ALLOC_OVERHEAD_BYTES: usize = 16;

/// Amortized hashbrown bucket cost per OCCUPIED entry for
/// `HashMap<JoinKey, Vec<HashEntry>>`: `size_of::<(JoinKey,
/// Vec<HashEntry>)>() = 48` plus 1 control byte, times hashbrown's 8/7
/// inverse load factor ≈ 56 bytes. Used by the PREDICTED (pre-build)
/// estimate; the post-build measurement uses the table's real `capacity()`
/// instead.
const HASH_TABLE_BUCKET_BYTES_AMORTIZED: usize =
    (std::mem::size_of::<(JoinKey, Vec<HashEntry>)>() + 1) * 8 / 7;

/// Conservative PREDICTED heap footprint of the `HashMap<JoinKey,
/// Vec<HashEntry>>` that `build_hash_table` would produce over `rows` rows
/// with `key_cols` join-key columns — the worst (all-unique-keys) case,
/// which is exactly the shape both oom-safety-hardening repros (and any
/// PK-keyed build side) hit. Per row:
///   ~56B amortized hashbrown bucket
/// + key-values `Vec<JoinValue>` heap buffer (`key_cols` × 32B + overhead)
/// + entries `Vec<HashEntry>` heap buffer (16B + overhead)
/// ≈ 136B/row for a single-column key — vs the ~12B/row of raw batch data
/// `estimate_batch_size` accounts, i.e. the measured 10-20x amplification
/// task 001 root-caused. Duplicate-heavy key sets cost LESS than this
/// (shared buckets/entry vecs), so the prediction only ever errs toward
/// spilling more readily — the safe direction. String keys' character
/// bytes are NOT predicted here (unknowable pre-build); the post-build
/// `hash_table_memory_bytes` measurement catches them, at the cost of one
/// partition table's transient overshoot.
fn predicted_hash_table_bytes(rows: usize, key_cols: usize) -> usize {
    let per_row = HASH_TABLE_BUCKET_BYTES_AMORTIZED
        + (std::mem::size_of::<JoinValue>() * key_cols.max(1) + HEAP_ALLOC_OVERHEAD_BYTES)
        + (std::mem::size_of::<HashEntry>() + HEAP_ALLOC_OVERHEAD_BYTES);
    rows.saturating_mul(per_row)
}

/// Documented approximation of `hash_agg::AccumulatorState`'s size (private
/// to `hash_agg.rs`, so not `size_of`-able from here): ~30 fields — counts,
/// power sums, per-type min/max `Option`s, two `Option<String>`s, an
/// `Option<HashSet>`, correlation sums, a `Vec` header — land it in the
/// 350-400B range; 384 is the conservative round-up.
const AGG_ACCUMULATOR_STATE_BYTES: usize = 384;
/// `hash_agg::GroupValue` (enum whose largest variant holds a `String`
/// header): 8B discriminant + 24B String ≈ 32B.
const AGG_GROUP_VALUE_BYTES: usize = 32;
/// Amortized hashbrown SET entry for one `GroupValue` in a
/// `distinct_set: HashSet<GroupValue>`: 32B value + 1 control byte, times
/// the 8/7 inverse load factor, rounded up to 48 to cover growth slack.
const AGG_DISTINCT_ENTRY_BYTES: usize = 48;

/// Conservative PREDICTED heap footprint of the `HashMap<GroupKey,
/// Vec<AccumulatorState>>` (plus DISTINCT value sets) that
/// `aggregate_batches_external` would build over `rows` rows — the
/// aggregate analogue of `predicted_hash_table_bytes`, priced the same
/// worst-case way (task 007's discipline): every row is its own group.
/// Duplicate-heavy group keys cost LESS than this (shared buckets/states),
/// so the prediction only ever errs toward the chunked read-back
/// (`aggregate_partition_chunked`) — the safe direction, whose cost is
/// bounded extra I/O over one already-spilling partition, never a wrong
/// answer or an unbudgeted allocation. DISTINCT aggregates additionally
/// pay a real (not worst-case) per-row set entry — every row inserts into
/// its group's `distinct_set` regardless of group cardinality — so
/// `n_distinct` charges per row unconditionally. String contents
/// (group-key bytes, distinct String values) are NOT predicted here
/// (unknowable pre-build), mirroring `predicted_hash_table_bytes`'s own
/// documented String gap.
fn predicted_agg_state_bytes(
    rows: usize,
    group_cols: usize,
    n_aggs: usize,
    n_distinct: usize,
) -> usize {
    // (GroupKey, Vec<AccumulatorState>) is the same 48-byte
    // (24B Vec-holding struct + 24B Vec header) map-entry shape as the
    // join's (JoinKey, Vec<HashEntry>), so the amortized bucket constant
    // is shared deliberately.
    let per_group = HASH_TABLE_BUCKET_BYTES_AMORTIZED
        + (AGG_GROUP_VALUE_BYTES * group_cols.max(1) + HEAP_ALLOC_OVERHEAD_BYTES)
        + (AGG_ACCUMULATOR_STATE_BYTES * n_aggs.max(1) + HEAP_ALLOC_OVERHEAD_BYTES);
    let per_row = per_group + n_distinct * AGG_DISTINCT_ENTRY_BYTES;
    rows.saturating_mul(per_row)
}

/// DIRECT measurement of a built join hash table's heap footprint: the
/// hashbrown bucket array at its REAL `capacity()`, plus every key's
/// `Vec<JoinValue>` buffer (and any `String` key's character buffer), plus
/// every entry list's `Vec<HashEntry>` buffer, each with
/// `HEAP_ALLOC_OVERHEAD_BYTES` of allocator overhead. O(keys) walk —
/// negligible next to the O(rows) build that produced the table. This is
/// the number the spill path's memory accounting charges against the
/// budget (oom-safety-hardening task 007); `estimate_batch_size` only ever
/// covered the batches underneath.
fn hash_table_memory_bytes(table: &HashMap<JoinKey, Vec<HashEntry>>) -> usize {
    let bucket_bytes = std::mem::size_of::<(JoinKey, Vec<HashEntry>)>() + 1;
    let mut total = table.capacity().saturating_mul(bucket_bytes);
    for (key, entries) in table.iter() {
        if key.values.capacity() > 0 {
            total += key.values.capacity() * std::mem::size_of::<JoinValue>()
                + HEAP_ALLOC_OVERHEAD_BYTES;
        }
        for v in &key.values {
            if let JoinValue::String(s) = v {
                if s.capacity() > 0 {
                    total += s.capacity() + HEAP_ALLOC_OVERHEAD_BYTES;
                }
            }
        }
        if entries.capacity() > 0 {
            total +=
                entries.capacity() * std::mem::size_of::<HashEntry>() + HEAP_ALLOC_OVERHEAD_BYTES;
        }
    }
    total
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

    /// Regression test for `spill-size-estimate-fix` epic, task 001: builds
    /// a Dictionary column whose values array is sliced out of a single
    /// oversized shared allocation via `Buffer::from_custom_allocation` --
    /// exactly `ipc_cache.rs::read_row_group`'s own mmap mechanics, where
    /// ONE `Buffer` backs an entire native-table segment file and every
    /// column's sub-buffers are views into it. Before the fix,
    /// `estimate_batch_size` fell through to `c.get_array_memory_size()`
    /// for `Dictionary` columns, which sums `Buffer::capacity()` --
    /// and a `Buffer` built by `from_custom_allocation` reports the FULL
    /// length passed at construction time forever, regardless of how small
    /// a slice is later carved out of it (this is `Deallocation::Custom`'s
    /// documented behavior in arrow-buffer's `bytes.rs`). This test proves
    /// two things: (1) the oversized-allocation setup below genuinely
    /// reproduces the precondition (`get_array_memory_size()` on the raw
    /// column is tens of MB for 5 logical rows), and (2)
    /// `estimate_batch_size` on the very same batch is now content-aware --
    /// on the order of the real ~50-ish logical bytes, not the shared
    /// allocation's size.
    #[test]
    fn dictionary_column_estimate_is_content_aware_not_mmap_capacity() {
        use arrow::array::{Array, ArrayData, DictionaryArray};
        use arrow::buffer::Buffer;
        use arrow::datatypes::{DataType, Field, Int32Type, Schema};

        // A 32 MiB "mmap" standing in for one native-table segment file --
        // `ipc_cache.rs::read_row_group`'s single `Buffer::from_custom_allocation`
        // over the whole mapped file, which every column's every buffer in
        // that segment is then sliced out of.
        let oversized_len = 32 * 1024 * 1024;
        let raw: Arc<Box<[u8]>> = Arc::new(vec![0u8; oversized_len].into_boxed_slice());
        let ptr = std::ptr::NonNull::new(raw.as_ptr() as *mut u8).unwrap();
        // SAFETY: `raw` (kept alive via the `Arc` passed as the allocation
        // owner) is a `oversized_len`-byte heap allocation; `ptr`/`oversized_len`
        // describe exactly that allocation.
        let mmap_buffer = unsafe { Buffer::from_custom_allocation(ptr, oversized_len, raw) };

        // Dictionary VALUES ("MAIL"/"SHIP"/"RAIL", Q12's own l_shipmode
        // values): a small Utf8 array whose data buffer is a slice of the
        // oversized shared allocation above -- the same shape
        // `read_dictionary`'s `buffer.slice_with_length(...)` produces.
        let values_bytes = b"MAILSHIPRAIL";
        let values_data_buf = mmap_buffer.slice_with_length(0, values_bytes.len());
        let offsets_buf = Buffer::from_slice_ref(&[0i32, 4, 8, 12]);
        let values_data = ArrayData::builder(DataType::Utf8)
            .len(3)
            .add_buffer(offsets_buf)
            .add_buffer(values_data_buf)
            .build()
            .unwrap();

        // Keys: 5 rows referencing the 3 dictionary entries (a normal,
        // non-oversized buffer -- only the values buffer needs to be
        // mmap-backed to reproduce the bug, since the OLD fallback summed
        // buffer capacities across the Dictionary array AND its child
        // (values) data).
        let keys_buf = Buffer::from_slice_ref(&[0i32, 1, 2, 0, 1]);
        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let dict_data = ArrayData::builder(dict_type.clone())
            .len(5)
            .add_buffer(keys_buf)
            .child_data(vec![values_data])
            .build()
            .unwrap();
        let dict_array: DictionaryArray<Int32Type> = DictionaryArray::from(dict_data);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "l_shipmode",
            dict_type,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dict_array) as ArrayRef]).unwrap();

        let col = batch.column(0);
        let old_buggy_size = col.get_array_memory_size();
        assert!(
            old_buggy_size > 16 * 1024 * 1024,
            "test setup didn't reproduce the mmap-capacity precondition: \
             get_array_memory_size()={old_buggy_size}"
        );

        let fixed_size = estimate_batch_size(&batch);
        assert!(
            fixed_size < 1024,
            "estimate_batch_size should be content-aware (tens of bytes for \
             5 rows over a 3-entry dictionary), got {fixed_size} bytes -- \
             still reading the mmap-backed buffer's capacity instead of its \
             logical content"
        );
    }

    /// `append_batch_streaming` is the task 002 fix for `append_to_parquet`'s
    /// O(n^2) read-entire-file+rewrite-rename-per-append pattern (see its
    /// own doc comment for the full story). This appends many small batches
    /// to the SAME spill file — the exact call pattern
    /// `build_with_partitioning`/`probe_with_spilling` use — through one
    /// shared writer slot, then confirms every row survives the round trip
    /// exactly, in the order written (Parquet preserves row-group write
    /// order and `read_parquet` reads row groups in file order).
    #[test]
    fn append_batch_streaming_preserves_all_rows_across_many_appends() {
        use arrow::datatypes::{DataType, Field, Schema};

        let dir = std::env::temp_dir().join(format!(
            "qe_append_streaming_correctness_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("build_0.parquet");

        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let mut writer: Option<ArrowWriter<File>> = None;
        let n_batches = 50;
        let batch_len = 37; // deliberately not a round number
        let mut expected: Vec<i64> = Vec::new();

        for i in 0..n_batches {
            let start = (i * batch_len) as i64;
            let vals: Vec<i64> = (start..start + batch_len as i64).collect();
            expected.extend_from_slice(&vals);
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))])
                    .unwrap();
            append_batch_streaming(&mut writer, &path, &batch)
                .expect("streaming append must not fail");
        }
        close_spill_writers(vec![writer]).expect("closing the writer must not fail");

        let read_back = read_parquet(&path).expect("spill file must be readable after close");
        let mut actual: Vec<i64> = Vec::new();
        for batch in &read_back {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            actual.extend(arr.values().iter().copied());
        }
        assert_eq!(
            actual, expected,
            "every appended batch's rows must survive, in write order"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Direct evidence for task 002's own acceptance criterion: "cost per
    /// append does not grow with the total data already spilled for that
    /// partition." Times one append early (right after the file already has
    /// one batch in it) and one late (after ~300 more appends), through the
    /// SAME writer slot/file `build_with_partitioning` would use, and
    /// asserts the late append is not dramatically more expensive than the
    /// early one. A generous multiplier avoids flaking on scheduler jitter
    /// while still clearly failing if the old O(n)-per-append cost (which
    /// would make append #301 roughly 150x the cost of append #2, since the
    /// file being fully re-read+rewritten on every call would have grown
    /// ~150x between the two measurements) ever regresses back in.
    #[test]
    fn append_batch_streaming_cost_does_not_grow_with_prior_data() {
        use arrow::datatypes::{DataType, Field, Schema};
        use std::time::{Duration, Instant};

        let dir =
            std::env::temp_dir().join(format!("qe_append_streaming_cost_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("build_0.parquet");

        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Float64, false),
        ]));
        let make_batch = || {
            let n = 500usize;
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from((0..n as i64).collect::<Vec<_>>())),
                    Arc::new(Float64Array::from(vec![1.5f64; n])),
                ],
            )
            .unwrap()
        };

        let mut writer: Option<ArrowWriter<File>> = None;

        // Prime the file with one batch, then time append #2.
        append_batch_streaming(&mut writer, &path, &make_batch()).unwrap();
        let t0 = Instant::now();
        append_batch_streaming(&mut writer, &path, &make_batch()).unwrap();
        let early = t0.elapsed();

        // 300 more appends — under the OLD read-rewrite-rename
        // implementation, the file being re-read+rewritten on every call
        // here would have grown ~150x between the early and late
        // measurement below.
        for _ in 0..300 {
            append_batch_streaming(&mut writer, &path, &make_batch()).unwrap();
        }

        let t1 = Instant::now();
        append_batch_streaming(&mut writer, &path, &make_batch()).unwrap();
        let late = t1.elapsed();

        close_spill_writers(vec![writer]).unwrap();
        let total_rows: usize = read_parquet(&path)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();
        assert_eq!(total_rows, 500 * (2 + 300 + 1));

        assert!(
            late < early * 20 + Duration::from_millis(25),
            "append cost grew with prior data (early={:?}, late={:?}) — \
             the O(n^2) read-rewrite-rename pattern may have regressed",
            early,
            late
        );

        let _ = std::fs::remove_dir_all(&dir);
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

    /// spill-join-correctness-2 epic, task 001: `KeyChecksum` is the direct
    /// write-vs-read comparison mechanism this task added to catch a
    /// Trino-PR#25892-shaped bug (spill wrote a join key one way, unspill
    /// read it back differently) in the act. This test pins its two
    /// load-bearing properties: identical logical key data produces an
    /// IDENTICAL checksum regardless of how it's split across batches or
    /// what order the rows appear in (both are true of a real Parquet
    /// spill/unspill round trip — row-group boundaries and, in general,
    /// row order are not guaranteed to match the pre-spill batch shape),
    /// and genuinely DIFFERENT key data produces a different checksum (so
    /// the mechanism would actually catch a real mismatch, not just always
    /// report "ok").
    #[test]
    fn key_checksum_is_order_and_batch_split_independent_but_detects_real_differences() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let key_expr = vec![Expr::column("k")];

        let one_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50]))],
        )
        .unwrap();
        let cs_one = batch_key_checksum(&one_batch, &key_expr).unwrap();

        // Same logical rows, split across three batches AND reordered —
        // exactly what a spill (writes in arrival order, across many
        // batches) followed by an unspill (reads back in row-group order,
        // which need not match) can produce.
        let split_a = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![40, 10]))],
        )
        .unwrap();
        let split_b =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![50]))])
                .unwrap();
        let split_c = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![20, 30]))],
        )
        .unwrap();
        let mut cs_split = KeyChecksum::default();
        for b in [&split_a, &split_b, &split_c] {
            cs_split.accumulate(batch_key_checksum(b, &key_expr).unwrap());
        }

        assert_eq!(
            cs_one.rows, cs_split.rows,
            "row count must match regardless of batch splitting"
        );
        assert_eq!(
            cs_one.xor_hash, cs_split.xor_hash,
            "checksum must be identical for the same logical keys \
             regardless of batch splitting or row order"
        );
        assert_eq!(cs_one.unhandled_type_rows, 0);
        assert_eq!(cs_split.unhandled_type_rows, 0);

        // Genuinely different key data (one value changed) must NOT
        // checksum the same — otherwise this mechanism could never catch a
        // real mismatch.
        let different = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![10, 20, 30, 40, 999]))],
        )
        .unwrap();
        let cs_different = batch_key_checksum(&different, &key_expr).unwrap();
        assert_eq!(cs_different.rows, cs_one.rows);
        assert_ne!(
            cs_different.xor_hash, cs_one.xor_hash,
            "a real difference in key data must change the checksum"
        );
    }

    /// A NULL-valued key (real SQL NULL) must never be flagged as
    /// `unhandled_type_rows` — that counter exists specifically to isolate
    /// `extract_join_key`'s downcast-chain gap (an array type it doesn't
    /// recognize, e.g. a Dictionary-encoded key column) from an ordinary,
    /// expected NULL.
    #[test]
    fn key_checksum_does_not_confuse_real_null_with_an_unhandled_array_type() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, true)]));
        let key_expr = vec![Expr::column("k")];
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]))],
        )
        .unwrap();
        let cs = batch_key_checksum(&batch, &key_expr).unwrap();
        assert_eq!(cs.rows, 3);
        assert_eq!(
            cs.unhandled_type_rows, 0,
            "a genuine SQL NULL is a handled case, not an unhandled array type"
        );
    }

    /// spill-join-correctness-2 epic, task 003: pure-parsing unit tests for
    /// the two fault-injection env vars (see this file's own "Fault
    /// injection: forced spill" module doc comment). Deliberately test the
    /// `parse_*`/`ChaosPartitionSpec::parse` functions directly rather than
    /// `std::env::set_var` + the `chaos_force_spill_*` wrappers — this test
    /// binary runs many tests concurrently in one process, and mutating the
    /// real environment here would race any other test reading the same
    /// keys (same reasoning as `gpu.rs`'s `parse_cache_budget_mb` and
    /// `execution::context::parse_merge_concurrency`).
    #[test]
    fn chaos_force_spill_after_batches_parsing() {
        assert_eq!(parse_chaos_force_spill_after_batches(None), None);
        // Set but empty/unparseable: still "force," defaulting to batch 0.
        assert_eq!(parse_chaos_force_spill_after_batches(Some("")), Some(0));
        assert_eq!(
            parse_chaos_force_spill_after_batches(Some("not_a_number")),
            Some(0)
        );
        assert_eq!(parse_chaos_force_spill_after_batches(Some("0")), Some(0));
        assert_eq!(parse_chaos_force_spill_after_batches(Some("7")), Some(7));
        // Whitespace tolerated, matching this file's own `QE_SPILL_DEBUG`-
        // adjacent env-parsing conventions elsewhere.
        assert_eq!(parse_chaos_force_spill_after_batches(Some(" 3 ")), Some(3));
    }

    #[test]
    fn chaos_partition_spec_parsing() {
        assert!(matches!(
            ChaosPartitionSpec::parse("all"),
            ChaosPartitionSpec::All
        ));
        assert!(matches!(
            ChaosPartitionSpec::parse("ALL"),
            ChaosPartitionSpec::All
        ));
        assert!(matches!(
            ChaosPartitionSpec::parse("  all  "),
            ChaosPartitionSpec::All
        ));

        match ChaosPartitionSpec::parse("0,3,17") {
            ChaosPartitionSpec::Indices(set) => {
                assert_eq!(set, [0usize, 3, 17].into_iter().collect())
            }
            other => panic!("expected Indices, got {other:?}"),
        }

        // Whitespace around entries tolerated; unparseable tokens silently
        // skipped rather than panicking (a malformed harness invocation
        // should force nothing, not crash the query it's testing).
        match ChaosPartitionSpec::parse(" 2 , x , 9,") {
            ChaosPartitionSpec::Indices(set) => {
                assert_eq!(set, [2usize, 9].into_iter().collect())
            }
            other => panic!("expected Indices, got {other:?}"),
        }

        match ChaosPartitionSpec::parse("") {
            ChaosPartitionSpec::Indices(set) => assert!(set.is_empty()),
            other => panic!("expected empty Indices, got {other:?}"),
        }

        for idx in 0..NUM_PARTITIONS {
            assert!(ChaosPartitionSpec::All.contains(idx));
        }
        let some = ChaosPartitionSpec::Indices([1usize, 2, 3].into_iter().collect());
        assert!(some.contains(2));
        assert!(!some.contains(4));
    }

    /// spill-join-correctness-2 epic, task 004, bug 2: `ORDER BY ... LIMIT`
    /// under spill (Q2/Q3-shaped, per the archived epic's own `003.md`
    /// characterization). The top-k fusion rule in `planner.rs`
    /// (`LogicalPlan::Limit` with `skip == 0` over a `Sort` folds directly
    /// into `ExternalSortExec::with_fetch` instead of wrapping a separate
    /// `LimitExec` around it) means `ExternalSortExec::execute()`'s SPILL
    /// branch is the only place a spilled top-K query's row count is ever
    /// truncated — before the fix, `self.fetch` was stored but never
    /// consulted again anywhere in that branch, so the full, correctly
    /// globally-sorted output was returned instead of just its first
    /// `fetch` rows: right values, wrong (too large) row count, matching
    /// the archived epic's own exact symptom description.
    #[tokio::test]
    async fn external_sort_spill_path_enforces_limit_under_forced_spill() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));

        // 20 single-row batches, descending values 19..0 — enough rows that
        // a tiny memory budget forces several separate spill runs (not just
        // a trivial single-run passthrough), while staying under
        // `MAX_MERGE_FANIN` = 8 runs so this test stays focused on the
        // LIMIT bug alone, not entangled with the separate multi-pass-merge
        // bug covered by its own test below.
        let mut batches = Vec::new();
        for v in (0..20i64).rev() {
            batches.push(
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![v]))])
                    .unwrap(),
            );
        }

        let input: Arc<dyn PhysicalOperator> = Arc::new(
            crate::physical::operators::MemoryTableExec::new("t", schema.clone(), batches, None),
        );

        let pool = crate::execution::create_memory_pool(1024);
        let mut config = ExecutionConfig::default();
        config.memory_limit = 64;
        config.spill_threshold = 0.5;
        config.spill_path =
            std::env::temp_dir().join(format!("qe_sort_limit_spill_test_{}", std::process::id()));

        let fetch = 5usize;
        let sort = ExternalSortExec::with_fetch(
            input,
            vec![crate::planner::SortExpr::new(Expr::column("k"))],
            pool,
            config,
            fetch,
        );

        let mut stream = sort
            .execute(0)
            .await
            .expect("spilled sort with LIMIT must not error");
        let mut keys = Vec::new();
        while let Some(batch) = stream
            .try_next()
            .await
            .expect("collecting limited sorted output")
        {
            let k = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            keys.extend(k.values().iter().copied());
        }

        assert_eq!(
            keys.len(),
            fetch,
            "spilled ORDER BY ... LIMIT {fetch} must return exactly {fetch} rows, got {}: {keys:?}",
            keys.len()
        );
        assert_eq!(
            keys,
            (0..fetch as i64).collect::<Vec<_>>(),
            "the LIMIT-ed rows must be the correct top-K prefix (right values), \
             not just the right count: {keys:?}"
        );
    }

    /// spill-join-correctness-2 epic, task 004, bug 3: sort-spill
    /// run-file-not-found crash (Q10-shaped, per the archived epic's own
    /// `003.md`: `Failed to open run file ".../sort_0_27/
    /// merged_pass0_48.parquet": No such file or directory`).
    /// `multi_pass_merge`'s "clean up old runs from previous pass" step
    /// unconditionally deleted every path in that pass's `current_runs` —
    /// including any carried forward UNCHANGED into `next_runs` (a chunk of
    /// exactly one leftover run, whenever a pass's run count isn't an exact
    /// multiple of `MAX_MERGE_FANIN` = 8) — so a later pass, or the final
    /// merge, tried to open a file this same function had just deleted out
    /// from under it. Reproduces the exact arithmetic deterministically:
    /// 129 initial single-row runs -> pass 0's chunking (129 = 16*8 + 1)
    /// leaves a 1-run leftover chunk at the tail, carried into pass 1 as
    /// one of 17 runs; pass 1's own chunking (17 = 2*8 + 1) again leaves a
    /// 1-run leftover chunk at the tail — exactly the run this bug deleted
    /// while pass 1's own `next_runs` (and the final merge right after)
    /// still needed it.
    #[tokio::test]
    async fn external_sort_multi_pass_merge_survives_a_leftover_singleton_chunk() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let dir =
            std::env::temp_dir().join(format!("qe_multi_pass_merge_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        // 129 single-row sorted "runs", one per distinct value 0..129.
        const N: i64 = 129;
        let mut run_paths = Vec::new();
        for v in 0..N {
            let batch =
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vec![v]))])
                    .unwrap();
            let path = dir.join(format!("run_{v}.parquet"));
            write_batches_to_parquet(&path, &[batch]).unwrap();
            run_paths.push(path);
        }

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

        let merged = sort
            .merge_runs(&run_paths)
            .expect("multi-pass merge must not fail to open a run file it itself deleted");

        let mut vals: Vec<i64> = Vec::new();
        for batch in &merged {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            vals.extend(arr.values().iter().copied());
        }
        let expected: Vec<i64> = (0..N).collect();
        assert_eq!(
            vals, expected,
            "multi-pass merge must produce every run's row, in order, exactly once"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    // Deliberately NO unit test here that calls
    // `std::env::set_var("QE_SPILL_CHAOS_FORCE_SPILL"/"_PARTITIONS", ...)`.
    // An earlier version of this task's own work-in-progress added one
    // (guarded by a local `Mutex` around its own set/unset section) and it
    // genuinely broke an unrelated, pre-existing, otherwise-unmodified test
    // in this exact file
    // (`spillable_hash_join_retained_mask_matches_delegate_schema`, a LEFT
    // JOIN case) under a real `cargo test` run: `cargo test`'s default
    // concurrent-test-thread execution ran that LEFT JOIN test WHILE this
    // module's own chaos test held `QE_SPILL_CHAOS_FORCE_SPILL` set, so the
    // LEFT JOIN's `execute()` call was forced into the disk-spill branch's
    // INNER-only guard and errored/panicked — a real, observed instance of
    // exactly the hazard `gpu.rs`'s `parse_cache_budget_mb` and
    // `execution::context::parse_merge_concurrency` already document
    // ("a test that called `std::env::set_var` here would race every other
    // test reading the same key"). A local mutex only serializes against
    // OTHER tests that also acquire IT — it does nothing for the hundreds
    // of unrelated tests in this same shared binary that don't know it
    // exists. The end-to-end "forced spill produces the same result as an
    // unforced run" invariant this would have checked is instead verified
    // by `examples/spill_chaos_harness.rs` — a SEPARATE PROCESS, so no
    // shared-binary env var race is possible — at far greater scale
    // (thousands of trials; see that file's own module doc comment and
    // this task's Outcome in `003.md` for real run counts/results) than
    // any single unit test would exercise anyway.

    // ------------------------------------------------------------------
    // oom-safety-hardening task 007: join spill-path hash-table budgeting
    // ------------------------------------------------------------------

    fn int64_key_batch(schema: &SchemaRef, ids: Vec<i64>) -> RecordBatch {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(ids))]).unwrap()
    }

    /// The sizing helpers must reflect the REAL ~10-20x table-over-batch
    /// amplification task 001 measured (~120-200B/row for unique Int64
    /// keys vs ~12B/row of batch data), or the budgeting they drive is
    /// fiction. Bounds are deliberately loose (allocator/capacity
    /// granularity varies) but strict enough that a regression to "tables
    /// cost roughly what batches cost" fails loudly.
    #[test]
    fn hash_table_sizing_reflects_real_amplification() {
        use arrow::datatypes::{DataType, Field, Schema};
        let rows: usize = 10_000;
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = int64_key_batch(&schema, (0..rows as i64).collect());
        let table = build_hash_table(&[batch.clone()], &[Expr::column("id")]).unwrap();
        assert_eq!(table.len(), rows);

        let measured = hash_table_memory_bytes(&table);
        let predicted = predicted_hash_table_bytes(rows, 1);
        let batch_bytes = estimate_batch_size(&batch);

        // Real per-row cost band from task 001's profiler evidence.
        assert!(
            measured >= rows * 100 && measured <= rows * 300,
            "measured {} bytes for {} unique keys ({}B/row) — outside the \
             100-300B/row band the real allocations occupy",
            measured,
            rows,
            measured / rows
        );
        assert!(
            predicted >= rows * 100 && predicted <= rows * 200,
            "predicted {}B/row outside 100-200B/row",
            predicted / rows
        );
        // The amplification the budget must account for: table >= 8x the
        // batch bytes the old accounting covered.
        assert!(
            measured >= batch_bytes * 8,
            "measured table bytes ({measured}) should be many times the \
             batch bytes ({batch_bytes})"
        );
    }

    /// Duplicate-heavy keys must cost LESS than the all-unique prediction
    /// (shared buckets/entry vecs) — the prediction only ever errs toward
    /// spilling more readily, never toward under-budgeting.
    #[test]
    fn predicted_hash_table_bytes_is_conservative_for_duplicate_keys() {
        use arrow::datatypes::{DataType, Field, Schema};
        let rows: usize = 10_000;
        let schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        // 100 distinct keys, 100 occurrences each.
        let batch = int64_key_batch(&schema, (0..rows as i64).map(|i| i % 100).collect());
        let table = build_hash_table(&[batch], &[Expr::column("id")]).unwrap();
        assert_eq!(table.len(), 100);
        let measured = hash_table_memory_bytes(&table);
        let predicted = predicted_hash_table_bytes(rows, 1);
        assert!(
            measured < predicted,
            "duplicate-heavy table ({measured}) should cost less than the \
             all-unique prediction ({predicted})"
        );
        // Entries themselves (16B x 10_000) must still be counted.
        assert!(measured >= rows * std::mem::size_of::<HashEntry>());
    }

    /// End-to-end: a spilling INNER join over many unique keys must (1)
    /// stay cell-exact, and (2) hold the memoized resident set — batch
    /// bytes PLUS measured hash-table bytes — under `memory_limit *
    /// spill_threshold`. Before task 007 the tables were built with zero
    /// accounting at ~10-20x the budgeted batch bytes (the 15-24x overshoot
    /// of both oom repros); this asserts the budgeting invariant directly
    /// on the build decision, not just survival.
    #[tokio::test]
    async fn spill_path_hash_tables_are_budgeted_and_join_stays_exact() {
        use arrow::datatypes::{DataType, Field, Schema};
        let build_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let total_rows: i64 = 200_000;
        let mut build_batches = Vec::new();
        let mut start = 0i64;
        while start < total_rows {
            let end = (start + 8192).min(total_rows);
            build_batches.push(int64_key_batch(&build_schema, (start..end).collect()));
            start = end;
        }
        let build: Arc<dyn PhysicalOperator> =
            Arc::new(crate::physical::operators::MemoryTableExec::new(
                "build",
                build_schema,
                build_batches,
                None,
            ));

        let probe_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("pid", DataType::Int64, false)]));
        // 4 existing keys (one duplicated → 2 output rows) + 1 missing key.
        let probe_ids = vec![0i64, 12_345, 12_345, 199_999, 777_777_777];
        let probe_batch = int64_key_batch(&probe_schema, probe_ids);
        let probe: Arc<dyn PhysicalOperator> =
            Arc::new(crate::physical::operators::MemoryTableExec::new(
                "probe",
                probe_schema,
                vec![probe_batch],
                None,
            ));

        // 256KB limit: the ~1.6MB of build batches must spill, and the
        // ~27MB of worst-case table bytes must be budgeted, not built.
        let limit = 256 * 1024;
        let pool = crate::execution::create_memory_pool(limit);
        let spill_dir =
            std::env::temp_dir().join(format!("qe_join_table_budget_test_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&spill_dir);
        let config = ExecutionConfig::new()
            .with_memory_limit(limit)
            .with_spill_path(spill_dir.clone());
        let threshold = (limit as f64 * config.spill_threshold) as usize;

        let join = SpillableHashJoinExec::new(
            build,
            probe,
            vec![(Expr::column("id"), Expr::column("pid"))],
            JoinType::Inner,
            pool,
            config,
        );

        let mut matched: Vec<i64> = Vec::new();
        let mut stream = join.execute(0).await.unwrap();
        while let Some(batch) = stream.try_next().await.unwrap() {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                matched.push(ids.value(i));
            }
        }
        matched.sort_unstable();
        assert_eq!(
            matched,
            vec![0, 12_345, 12_345, 199_999],
            "spilling join with budgeted tables must stay cell-exact"
        );

        // The budgeting invariant, asserted on the memoized decision.
        match join.build_decision.get() {
            Some(BuildDecision::Spill {
                partitions, tables, ..
            }) => {
                let batch_bytes: usize = partitions.iter().flatten().map(|p| p.memory_bytes).sum();
                let table_bytes: usize = tables.iter().flatten().map(hash_table_memory_bytes).sum();
                assert!(
                    batch_bytes + table_bytes <= threshold,
                    "resident batches ({batch_bytes}) + measured tables \
                     ({table_bytes}) exceed the budget ({threshold})"
                );
            }
            _ => panic!("expected a memoized Spill decision"),
        }
        drop(join);
        let _ = std::fs::remove_dir_all(&spill_dir);
    }

    /// Read-back (spilled-partition) processing must chunk its transient
    /// hash table under the budget AND stay exact when a key's multiple
    /// build rows straddle a chunk boundary — every (build,probe) pair
    /// emitted exactly once, unioned across chunks. Build keys appear
    /// TWICE (in two well-separated row ranges, so the two occurrences
    /// land in different read-back batches/chunks); every probed key must
    /// come back exactly twice.
    #[tokio::test]
    async fn spilled_partition_chunked_table_build_is_cell_exact() {
        use arrow::datatypes::{DataType, Field, Schema};
        let build_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let distinct: i64 = 20_000;
        let mut build_batches = Vec::new();
        for _pass in 0..2 {
            let mut start = 0i64;
            while start < distinct {
                let end = (start + 1024).min(distinct);
                build_batches.push(int64_key_batch(&build_schema, (start..end).collect()));
                start = end;
            }
        }
        let build: Arc<dyn PhysicalOperator> =
            Arc::new(crate::physical::operators::MemoryTableExec::new(
                "build",
                build_schema,
                build_batches,
                None,
            ));

        let probe_schema: SchemaRef =
            Arc::new(Schema::new(vec![Field::new("pid", DataType::Int64, false)]));
        let probe_ids: Vec<i64> = (0..distinct).step_by(7).collect();
        let expected_rows = probe_ids.len() * 2;
        let probe_batch = int64_key_batch(&probe_schema, probe_ids.clone());
        let probe: Arc<dyn PhysicalOperator> =
            Arc::new(crate::physical::operators::MemoryTableExec::new(
                "probe",
                probe_schema,
                vec![probe_batch],
                None,
            ));

        // 64KB limit → ~51KB threshold: every partition's predicted table
        // (~625 rows x ~136B ≈ 85KB) exceeds it, so partitions evict to
        // disk and read-back chunking engages (multiple chunks per
        // partition).
        let limit = 64 * 1024;
        let pool = crate::execution::create_memory_pool(limit);
        let spill_dir = std::env::temp_dir().join(format!(
            "qe_join_chunked_readback_test_{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&spill_dir);
        let config = ExecutionConfig::new()
            .with_memory_limit(limit)
            .with_spill_path(spill_dir.clone());

        let join = SpillableHashJoinExec::new(
            build,
            probe,
            vec![(Expr::column("id"), Expr::column("pid"))],
            JoinType::Inner,
            pool,
            config,
        );

        let mut matched: Vec<i64> = Vec::new();
        let mut stream = join.execute(0).await.unwrap();
        while let Some(batch) = stream.try_next().await.unwrap() {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                matched.push(ids.value(i));
            }
        }
        matched.sort_unstable();
        let mut expected: Vec<i64> = probe_ids.iter().flat_map(|&k| [k, k]).collect();
        expected.sort_unstable();
        assert_eq!(matched.len(), expected_rows);
        assert_eq!(
            matched, expected,
            "each probed key must match exactly its two build occurrences, \
             even when they land in different read-back chunks"
        );
        drop(join);
        let _ = std::fs::remove_dir_all(&spill_dir);
    }
    // ------------------------------------------------------------------
    // oom-safety-hardening task 002: streaming two-phase reservation +
    // accounted finalize for SpillableHashAggregateExec.
    // ------------------------------------------------------------------

    /// Run one SpillableHashAggregateExec over `batches` with the given
    /// memory limit; returns (sorted rows, spilled bytes recorded by the
    /// pool). Rows are (group rendered as String, per-agg i64 values).
    async fn run_agg_collect(
        batches: Vec<RecordBatch>,
        schema: SchemaRef,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        out_schema: SchemaRef,
        memory_limit: usize,
        spill_tag: &str,
    ) -> (Vec<Vec<String>>, usize) {
        use futures::TryStreamExt;
        let input: Arc<dyn PhysicalOperator> = Arc::new(
            crate::physical::operators::MemoryTableExec::new("t", schema, batches, None),
        );
        let spill_path = std::env::temp_dir().join(format!(
            "qe_spillable_agg_test_{}_{}",
            spill_tag,
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&spill_path);
        let pool = crate::execution::create_memory_pool(memory_limit);
        let config = ExecutionConfig::new()
            .with_memory_limit(memory_limit)
            .with_spill_path(spill_path.clone());
        let agg = SpillableHashAggregateExec::new(
            input,
            group_by,
            aggregates,
            out_schema,
            pool.clone(),
            config,
        );
        let mut stream = agg.execute(0).await.unwrap();
        let mut rows: Vec<Vec<String>> = Vec::new();
        while let Some(b) = stream.try_next().await.unwrap() {
            for r in 0..b.num_rows() {
                let mut row = Vec::with_capacity(b.num_columns());
                for c in b.columns() {
                    row.push(render_cell(c, r));
                }
                rows.push(row);
            }
        }
        rows.sort();
        let spilled = pool.spilled();
        let _ = std::fs::remove_dir_all(&spill_path);
        (rows, spilled)
    }

    fn render_cell(col: &ArrayRef, row: usize) -> String {
        use arrow::array::Array;
        if col.is_null(row) {
            return "NULL".to_string();
        }
        if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            return a.value(row).to_string();
        }
        if let Some(a) = col.as_any().downcast_ref::<StringArray>() {
            return a.value(row).to_string();
        }
        if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
            return format!("{:.6}", a.value(row));
        }
        panic!("render_cell: unhandled type {:?}", col.data_type());
    }

    /// Cell-exact: a Dictionary(Int32, Utf8)-keyed GROUP BY with a DISTINCT
    /// aggregate (fused-streaming-INELIGIBLE by construction, so execution
    /// takes the streaming two-phase reservation path) must return results
    /// byte-identical to an unlimited-memory run when forced through the
    /// spill path — including the chunked (sub-partitioned) finalize, which
    /// the tiny limit's predicted-state gate necessarily trips. Covers the
    /// Dictionary spill round trip (Parquet preserves the Dictionary type
    /// via the embedded Arrow schema) and the dictionary-decoded group-hash
    /// routing staying stable across write/read-back.
    #[tokio::test]
    async fn agg_spill_dictionary_group_by_cell_exact_vs_in_memory() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::{DataType, Field, Int32Type, Schema};

        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", dict_type, false),
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let values = StringArray::from(
            (0..40)
                .map(|i| format!("group_{:02}", i))
                .collect::<Vec<_>>(),
        );
        let mut batches = Vec::new();
        let n_batches = 24usize;
        let rows_per_batch = 8_192usize;
        for b in 0..n_batches {
            let start = (b * rows_per_batch) as i64;
            let keys = Int32Array::from(
                (0..rows_per_batch)
                    .map(|i| (i % 40) as i32)
                    .collect::<Vec<_>>(),
            );
            let dict: DictionaryArray<Int32Type> =
                DictionaryArray::try_new(keys, Arc::new(values.clone())).unwrap();
            let ids: Vec<i64> = (start..start + rows_per_batch as i64).collect();
            let vals: Vec<i64> = ids.iter().map(|i| i % 91).collect();
            batches.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(dict),
                        Arc::new(Int64Array::from(ids)),
                        Arc::new(Int64Array::from(vals)),
                    ],
                )
                .unwrap(),
            );
        }

        let group_by = vec![Expr::column("g")];
        let aggregates = vec![
            AggregateExpr {
                func: crate::planner::AggregateFunction::CountDistinct,
                input: Expr::column("id"),
                distinct: true,
                second_arg: None,
            },
            AggregateExpr {
                func: crate::planner::AggregateFunction::Sum,
                input: Expr::column("v"),
                distinct: false,
                second_arg: None,
            },
        ];
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, false),
            Field::new("cnt", DataType::Int64, true),
            Field::new("sum_v", DataType::Int64, true),
        ]));

        let (baseline, base_spilled) = run_agg_collect(
            batches.clone(),
            schema.clone(),
            group_by.clone(),
            aggregates.clone(),
            out_schema.clone(),
            8 * 1024 * 1024 * 1024,
            "dict_base",
        )
        .await;
        let (spilled_rows, spilled_bytes) = run_agg_collect(
            batches,
            schema,
            group_by,
            aggregates,
            out_schema,
            256 * 1024,
            "dict_spill",
        )
        .await;

        assert_eq!(base_spilled, 0, "unlimited run must not spill");
        assert!(
            spilled_bytes > 0,
            "256KB limit over ~4MB of input must force a spill"
        );
        assert_eq!(baseline.len(), 40, "expected exactly 40 groups");
        assert_eq!(
            baseline, spilled_rows,
            "spilled Dictionary-keyed aggregate must be byte-identical to the in-memory run"
        );
    }

    /// Cell-exact through the chunked (sub-partitioned) finalize with a
    /// plain Int64 group key: high group cardinality + COUNT(DISTINCT)
    /// makes the per-partition predicted state cross the tiny threshold,
    /// so every partition takes `aggregate_partition_chunked`; groups whose
    /// rows straddle the resident/spilled boundary AND multiple read-back
    /// batches must still aggregate exactly once each (the aggregate
    /// analogue of the join's chunk-straddling duplicate-key test).
    #[tokio::test]
    async fn agg_spill_chunked_finalize_high_cardinality_cell_exact() {
        use arrow::datatypes::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let mut batches = Vec::new();
        let n_batches = 25usize;
        let rows_per_batch = 8_192usize;
        for b in 0..n_batches {
            let start = (b * rows_per_batch) as i64;
            let ids: Vec<i64> = (start..start + rows_per_batch as i64).collect();
            let groups: Vec<i64> = ids.iter().map(|i| i % 997).collect();
            let vals: Vec<i64> = ids.iter().map(|i| i % 13).collect();
            batches.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(groups)),
                        Arc::new(Int64Array::from(ids)),
                        Arc::new(Int64Array::from(vals)),
                    ],
                )
                .unwrap(),
            );
        }
        let total_rows = (n_batches * rows_per_batch) as i64;

        let group_by = vec![Expr::column("g")];
        let aggregates = vec![
            AggregateExpr {
                func: crate::planner::AggregateFunction::CountDistinct,
                input: Expr::column("id"),
                distinct: true,
                second_arg: None,
            },
            AggregateExpr {
                func: crate::planner::AggregateFunction::Sum,
                input: Expr::column("v"),
                distinct: false,
                second_arg: None,
            },
        ];
        let out_schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Int64, false),
            Field::new("cnt", DataType::Int64, true),
            Field::new("sum_v", DataType::Int64, true),
        ]));

        let (baseline, _) = run_agg_collect(
            batches.clone(),
            schema.clone(),
            group_by.clone(),
            aggregates.clone(),
            out_schema.clone(),
            8 * 1024 * 1024 * 1024,
            "chunk_base",
        )
        .await;
        let (spilled_rows, spilled_bytes) = run_agg_collect(
            batches,
            schema,
            group_by,
            aggregates,
            out_schema,
            256 * 1024,
            "chunk_spill",
        )
        .await;

        assert!(spilled_bytes > 0, "tiny limit must force a spill");
        assert_eq!(baseline.len(), 997, "expected exactly 997 groups");
        // Self-consistency: all ids are unique, so the distinct counts must
        // sum to the total row count — a duplication or loss across the
        // sub-partition boundary breaks this even if both runs drifted.
        let count_sum: i64 = baseline.iter().map(|r| r[1].parse::<i64>().unwrap()).sum();
        assert_eq!(count_sum, total_rows);
        assert_eq!(
            baseline, spilled_rows,
            "chunked-finalize aggregate must be byte-identical to the in-memory run"
        );
    }

    /// The chunked finalize's group-disjointness rests on this invariant:
    /// routing the same rows at modulus NUM_PARTITIONS (ingestion) and
    /// NUM_PARTITIONS * fan (sub-partitioning) must agree mod
    /// NUM_PARTITIONS for every row — same fixed-seed hash, coarser vs
    /// finer modulus.
    #[test]
    fn group_hash_routing_consistent_across_fanout() {
        use arrow::datatypes::{DataType, Field, Schema};
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let keys: Vec<i64> = (0..10_000).map(|i| i * 2654435761i64 % 5_000).collect();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(keys))]).unwrap();
        let exprs = vec![Expr::column("k")];

        let coarse = partition_batch_by_group_hash(&batch, &exprs, NUM_PARTITIONS).unwrap();
        let mut key_to_coarse: HashMap<i64, usize> = HashMap::new();
        for (idx, piece) in coarse.iter().enumerate() {
            let Some(piece) = piece else { continue };
            let col = piece
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for r in 0..piece.num_rows() {
                key_to_coarse.insert(col.value(r), idx);
            }
        }

        let fan = 7usize;
        let fine = partition_batch_by_group_hash(&batch, &exprs, NUM_PARTITIONS * fan).unwrap();
        let mut nonempty_fine = 0usize;
        for (i, piece) in fine.iter().enumerate() {
            let Some(piece) = piece else { continue };
            nonempty_fine += 1;
            let col = piece
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for r in 0..piece.num_rows() {
                assert_eq!(
                    key_to_coarse[&col.value(r)],
                    i % NUM_PARTITIONS,
                    "fine residue must agree with coarse routing mod NUM_PARTITIONS"
                );
            }
        }
        assert!(nonempty_fine > NUM_PARTITIONS, "fanout must actually split");
    }

    /// A Dictionary-encoded group key must route by its decoded VALUE (and
    /// therefore spread across partitions), identically to the same data as
    /// plain Utf8 — not fall through `extract_join_key`'s downcast chain to
    /// a constant Null key that lands every row in one partition.
    #[test]
    fn group_hash_routing_splits_dictionary_group_keys() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::{DataType, Field, Int32Type, Schema};

        let dict_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let schema = Arc::new(Schema::new(vec![Field::new("g", dict_type, false)]));
        let values = StringArray::from(
            (0..40)
                .map(|i| format!("group_{:02}", i))
                .collect::<Vec<_>>(),
        );
        let keys = Int32Array::from((0..4_000).map(|i| (i % 40) as i32).collect::<Vec<_>>());
        let dict: DictionaryArray<Int32Type> =
            DictionaryArray::try_new(keys, Arc::new(values)).unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(dict)]).unwrap();
        let exprs = vec![Expr::column("g")];

        let routed = partition_batch_by_group_hash(&batch, &exprs, NUM_PARTITIONS).unwrap();
        let nonempty = routed.iter().flatten().count();
        assert!(
            nonempty > 1,
            "40 distinct dictionary values must spread across partitions, got {}",
            nonempty
        );

        // And identically to the decoded plain-Utf8 rendering of the same data.
        let plain_schema = Arc::new(Schema::new(vec![Field::new("g", DataType::Utf8, false)]));
        let plain = StringArray::from(
            (0..4_000)
                .map(|i| format!("group_{:02}", i % 40))
                .collect::<Vec<_>>(),
        );
        let plain_batch = RecordBatch::try_new(plain_schema, vec![Arc::new(plain)]).unwrap();
        let routed_plain =
            partition_batch_by_group_hash(&plain_batch, &exprs, NUM_PARTITIONS).unwrap();
        let counts: Vec<usize> = routed
            .iter()
            .map(|p| p.as_ref().map(|b| b.num_rows()).unwrap_or(0))
            .collect();
        let counts_plain: Vec<usize> = routed_plain
            .iter()
            .map(|p| p.as_ref().map(|b| b.num_rows()).unwrap_or(0))
            .collect();
        assert_eq!(
            counts, counts_plain,
            "dictionary and plain encodings of the same values must route identically"
        );
    }

    /// The aggregate-state prediction must stay conservative (per-row cost
    /// at least covers the map-entry shape) and monotonic in every input,
    /// with DISTINCT charging strictly more — mirroring
    /// `predicted_hash_table_bytes_is_conservative_for_duplicate_keys`'s
    /// role for the join prediction.
    #[test]
    fn predicted_agg_state_bytes_is_conservative_and_monotonic() {
        let base = predicted_agg_state_bytes(1_000, 1, 1, 0);
        // At minimum the map-entry bucket + one accumulator per row.
        assert!(base >= 1_000 * (HASH_TABLE_BUCKET_BYTES_AMORTIZED + AGG_ACCUMULATOR_STATE_BYTES));
        assert!(predicted_agg_state_bytes(2_000, 1, 1, 0) > base);
        assert!(predicted_agg_state_bytes(1_000, 2, 1, 0) > base);
        assert!(predicted_agg_state_bytes(1_000, 1, 2, 0) > base);
        assert!(
            predicted_agg_state_bytes(1_000, 1, 1, 1) >= base + 1_000 * AGG_DISTINCT_ENTRY_BYTES
        );
        // Zero-arg floors: a global aggregate (0 group cols) still prices
        // its single group's state per row (worst case).
        assert!(predicted_agg_state_bytes(1_000, 0, 0, 0) > 0);
    }
}
