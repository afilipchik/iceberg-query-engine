//! Hash join operator

use crate::error::Result;
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
use rayon::prelude::*;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::OnceCell;

// Debug logging for crash investigation (disabled by default)
#[allow(dead_code)]
fn debug_log(_msg: &str) {
    // Uncomment to enable debug logging:
    // use std::fs::OpenOptions;
    // use std::io::Write;
    // if let Ok(mut file) = OpenOptions::new()
    //     .create(true)
    //     .append(true)
    //     .open("/tmp/hash_join_debug.log")
    // {
    //     let _ = writeln!(file, "[HashJoin] {}", msg);
    //     let _ = file.flush();
    // }
}

/// Cached build side data - collected once, reused across partitions
struct BuildSideCache {
    batches: Vec<RecordBatch>,
    hash_table: HashMap<JoinKey, Vec<HashEntry>>,
}

/// Hash join execution operator
pub struct HashJoinExec {
    left: Arc<dyn PhysicalOperator>,
    right: Arc<dyn PhysicalOperator>,
    on: Vec<(Expr, Expr)>,
    join_type: JoinType,
    schema: SchemaRef,
    /// Optional filter to evaluate during the join (required for Semi/Anti joins with filters)
    filter: Option<Expr>,
    /// Schema combining left and right for filter evaluation
    combined_schema: SchemaRef,
    /// Cached build side - computed once, shared across all partition executions
    build_cache: OnceCell<BuildSideCache>,
}

impl std::fmt::Debug for HashJoinExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HashJoinExec")
            .field("left", &self.left)
            .field("right", &self.right)
            .field("on", &self.on)
            .field("join_type", &self.join_type)
            .field("schema", &self.schema)
            .field("filter", &self.filter)
            .finish()
    }
}

impl HashJoinExec {
    pub fn new(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        on: Vec<(Expr, Expr)>,
        join_type: JoinType,
    ) -> Self {
        Self::with_filter(left, right, on, join_type, None)
    }

    pub fn with_filter(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        on: Vec<(Expr, Expr)>,
        join_type: JoinType,
        filter: Option<Expr>,
    ) -> Self {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let schema = match join_type {
            JoinType::Semi | JoinType::Anti => left_schema,
            _ => {
                // For outer joins, columns from the "outer" side can be null
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

        // Create combined schema for filter evaluation (left + right)
        let combined_fields: Vec<_> = left
            .schema()
            .fields()
            .iter()
            .chain(right.schema().fields().iter())
            .cloned()
            .collect();
        let combined_schema = Arc::new(Schema::new(combined_fields));

        Self {
            left,
            right,
            on,
            join_type,
            schema,
            filter,
            combined_schema,
            build_cache: OnceCell::new(),
        }
    }
}

#[async_trait]
impl PhysicalOperator for HashJoinExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.left.clone(), self.right.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        debug_log(&format!(
            "execute() partition={} join_type={:?}",
            partition, self.join_type
        ));

        // Determine build and probe sides
        let (build_side, probe_side, swapped) = match self.join_type {
            JoinType::Right => (&self.right, &self.left, true),
            _ => (&self.left, &self.right, false),
        };

        let (on_left, on_right): (Vec<_>, Vec<_>) = self.on.iter().cloned().unzip();
        let build_keys = if swapped {
            on_right.clone()
        } else {
            on_left.clone()
        };
        let probe_keys = if swapped { &on_left } else { &on_right };

        // Get or build the cached build side (computed ONCE, reused across all partitions)
        let cache = self
            .build_cache
            .get_or_try_init(|| async {
                debug_log(&format!(
                    "CACHE MISS: Building hash table for join_type={:?}",
                    self.join_type
                ));

                // Collect ALL partitions from the build side
                let build_partitions = build_side.output_partitions().max(1);
                debug_log(&format!(
                    "Collecting {} build partitions from {}",
                    build_partitions,
                    build_side.name()
                ));

                // Memory safety: limit build side size to prevent OOM crashes
                // Default 50M rows - enough for TPC-H SF=100 lineitem (600M) with proper joins
                const MAX_BUILD_ROWS: usize = 50_000_000;
                const MAX_BUILD_BYTES: usize = 4 * 1024 * 1024 * 1024; // 4GB

                let mut build_batches = Vec::new();
                let mut total_build_rows = 0usize;
                let mut total_build_bytes = 0usize;
                for p in 0..build_partitions {
                    let stream = build_side.execute(p).await?;
                    let batches: Vec<RecordBatch> = stream.try_collect().await?;
                    for b in &batches {
                        total_build_rows += b.num_rows();
                        // Estimate memory: ~50 bytes per row as rough average
                        total_build_bytes += b.get_array_memory_size();
                    }
                    build_batches.extend(batches);

                    // Check memory limits DURING collection to fail early
                    if total_build_rows > MAX_BUILD_ROWS {
                        return Err(crate::error::QueryError::Execution(format!(
                            "Hash join build side exceeds {} rows (at {} rows). \
                        This usually indicates a cross join or missing join condition. \
                        Consider using spillable operators for larger datasets.",
                            MAX_BUILD_ROWS, total_build_rows
                        )));
                    }
                    if total_build_bytes > MAX_BUILD_BYTES {
                        return Err(crate::error::QueryError::Execution(format!(
                            "Hash join build side exceeds {} bytes (at {} bytes). \
                        Consider using spillable operators for larger datasets.",
                            MAX_BUILD_BYTES, total_build_bytes
                        )));
                    }
                }
                debug_log(&format!(
                    "Build side collected: {} batches, {} total rows, {} bytes",
                    build_batches.len(),
                    total_build_rows,
                    total_build_bytes
                ));

                // Build hash table
                debug_log("Building hash table...");
                let hash_table = build_hash_table(&build_batches, &build_keys)?;
                debug_log(&format!(
                    "Hash table built with {} entries",
                    hash_table.len()
                ));

                Ok::<_, crate::error::QueryError>(BuildSideCache {
                    batches: build_batches,
                    hash_table,
                })
            })
            .await?;

        debug_log(&format!(
            "CACHE HIT: Reusing hash table with {} entries for partition {}",
            cache.hash_table.len(),
            partition
        ));

        // Probe with only THIS partition (allows parallel execution across partitions)
        debug_log(&format!(
            "Probing partition {} from {}",
            partition,
            probe_side.name()
        ));
        let probe_stream = probe_side.execute(partition).await?;
        let probe_batches: Vec<RecordBatch> = probe_stream.try_collect().await?;
        let probe_rows: usize = probe_batches.iter().map(|b| b.num_rows()).sum();
        debug_log(&format!(
            "Probe side collected: {} batches, {} rows",
            probe_batches.len(),
            probe_rows
        ));

        // Safety check: prevent cross join explosions
        // For Cross joins, the output size is build_rows * probe_rows
        let build_rows: usize = cache.batches.iter().map(|b| b.num_rows()).sum();
        if self.join_type == JoinType::Cross && build_rows > 0 && probe_rows > 0 {
            let max_output = build_rows.saturating_mul(probe_rows);
            const CROSS_JOIN_LIMIT: usize = 10_000_000; // 10 million rows max
            if max_output > CROSS_JOIN_LIMIT {
                debug_log(&format!(
                    "CROSS JOIN EXPLOSION DETECTED: {} x {} = {} rows (limit: {})",
                    build_rows, probe_rows, max_output, CROSS_JOIN_LIMIT
                ));
                return Err(crate::error::QueryError::Execution(format!(
                    "Cross join would produce {} rows ({} x {}), exceeding limit of {}. \
                    This usually indicates missing join conditions in the query. \
                    Check that all table joins have proper ON or WHERE conditions.",
                    max_output, build_rows, probe_rows, CROSS_JOIN_LIMIT
                )));
            }
        }

        debug_log("Starting probe_hash_table...");
        let result = probe_hash_table(
            &cache.batches,
            &probe_batches,
            &cache.hash_table,
            probe_keys,
            self.join_type,
            swapped,
            &self.schema,
            self.filter.as_ref(),
            &self.combined_schema,
        )?;

        let result_rows: usize = result.iter().map(|b| b.num_rows()).sum();
        debug_log(&format!(
            "Join produced {} result batches, {} rows",
            result.len(),
            result_rows
        ));

        Ok(Box::pin(stream::iter(result.into_iter().map(Ok))))
    }

    fn output_partitions(&self) -> usize {
        // Hash join can be parallelized by probe side partitions
        // The build side is fully collected, probe side is partitioned
        match self.join_type {
            JoinType::Right => self.left.output_partitions().max(1),
            _ => self.right.output_partitions().max(1),
        }
    }

    fn name(&self) -> &str {
        "HashJoin"
    }
}

impl fmt::Display for HashJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let on_str: Vec<String> = self
            .on
            .iter()
            .map(|(l, r)| format!("{} = {}", l, r))
            .collect();
        write!(f, "{} Join on [{}]", self.join_type, on_str.join(", "))
    }
}

/// Join key for hash table
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

/// Hash table entry pointing to batch and row indices
#[derive(Clone)]
struct HashEntry {
    batch_idx: usize,
    row_idx: usize,
}

/// Threshold for parallel build (use parallel for larger datasets)
const PARALLEL_BUILD_THRESHOLD: usize = 10_000;

fn build_hash_table(
    batches: &[RecordBatch],
    key_exprs: &[Expr],
) -> Result<HashMap<JoinKey, Vec<HashEntry>>> {
    // Count total rows to decide if parallel build is worth it
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    if total_rows < PARALLEL_BUILD_THRESHOLD || batches.len() < 2 {
        // Use sequential build for small datasets
        build_hash_table_sequential(batches, key_exprs)
    } else {
        // Use parallel build for large datasets
        build_hash_table_parallel(batches, key_exprs)
    }
}

/// Sequential hash table build (for small datasets)
fn build_hash_table_sequential(
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

            // Skip null keys (null != null in SQL)
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

/// Parallel hash table build using rayon
fn build_hash_table_parallel(
    batches: &[RecordBatch],
    key_exprs: &[Expr],
) -> Result<HashMap<JoinKey, Vec<HashEntry>>> {
    // Build partial hash tables in parallel, one per batch
    let partial_tables: Vec<Result<HashMap<JoinKey, Vec<HashEntry>>>> = batches
        .par_iter()
        .enumerate()
        .map(|(batch_idx, batch)| {
            let mut partial: HashMap<JoinKey, Vec<HashEntry>> = HashMap::new();

            let key_arrays: Result<Vec<ArrayRef>> =
                key_exprs.iter().map(|e| evaluate_expr(batch, e)).collect();
            let key_arrays = key_arrays?;

            for row_idx in 0..batch.num_rows() {
                let key = extract_join_key(&key_arrays, row_idx);

                // Skip null keys (null != null in SQL)
                if key.values.iter().any(|v| matches!(v, JoinValue::Null)) {
                    continue;
                }

                partial
                    .entry(key)
                    .or_default()
                    .push(HashEntry { batch_idx, row_idx });
            }

            Ok(partial)
        })
        .collect();

    // Merge partial tables into final table
    let mut final_table: HashMap<JoinKey, Vec<HashEntry>> = HashMap::new();

    for partial_result in partial_tables {
        let partial = partial_result?;
        for (key, entries) in partial {
            final_table.entry(key).or_default().extend(entries);
        }
    }

    Ok(final_table)
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
            if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Date32Array>() {
                return JoinValue::Int64(a.value(row) as i64);
            }

            JoinValue::Null
        })
        .collect();

    JoinKey { values }
}

/// Create a combined batch from multiple build/probe row pairs for batch filter evaluation
fn create_combined_batch(
    build_batches: &[RecordBatch],
    build_indices: &[(usize, usize)], // (batch_idx, row_idx)
    probe_batch: &RecordBatch,
    probe_indices: &[usize],
    swapped: bool,
    combined_schema: &SchemaRef,
) -> Result<RecordBatch> {
    if build_indices.is_empty() {
        return Ok(RecordBatch::new_empty(combined_schema.clone()));
    }

    // Gather columns from build side
    let build_columns: Result<Vec<ArrayRef>> = if build_batches.is_empty() {
        Ok(vec![])
    } else {
        (0..build_batches[0].num_columns())
            .map(|col_idx| gather_column(build_batches, col_idx, build_indices))
            .collect()
    };
    let build_columns = build_columns?;

    // Gather columns from probe side
    let probe_indices_u32: Vec<u32> = probe_indices.iter().map(|&i| i as u32).collect();
    let take_indices = UInt32Array::from(probe_indices_u32);
    let probe_columns: Vec<ArrayRef> = probe_batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &take_indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    // Combine in correct order (left then right, accounting for swap)
    let all_columns = if swapped {
        probe_columns.into_iter().chain(build_columns).collect()
    } else {
        build_columns.into_iter().chain(probe_columns).collect()
    };

    RecordBatch::try_new(combined_schema.clone(), all_columns).map_err(Into::into)
}

#[allow(clippy::needless_range_loop)] // Index needed for parallel array access
#[allow(clippy::too_many_arguments)]
fn probe_hash_table(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    hash_table: &HashMap<JoinKey, Vec<HashEntry>>,
    probe_key_exprs: &[Expr],
    join_type: JoinType,
    swapped: bool,
    output_schema: &SchemaRef,
    filter: Option<&Expr>,
    combined_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let mut results = Vec::new();

    // Track which build rows have been matched (for outer joins)
    let mut build_matched: Vec<Vec<bool>> = build_batches
        .iter()
        .map(|b| vec![false; b.num_rows()])
        .collect();

    for probe_batch in probe_batches {
        let probe_key_arrays: Result<Vec<ArrayRef>> = probe_key_exprs
            .iter()
            .map(|e| evaluate_expr(probe_batch, e))
            .collect();
        let probe_key_arrays = probe_key_arrays?;

        // First pass: collect all candidate pairs from hash table lookup
        let mut candidate_build_indices: Vec<(usize, usize)> = Vec::new();
        let mut candidate_probe_indices: Vec<usize> = Vec::new();

        for probe_row in 0..probe_batch.num_rows() {
            let key = extract_join_key(&probe_key_arrays, probe_row);

            // Skip null keys
            if key.values.iter().any(|v| matches!(v, JoinValue::Null)) {
                continue;
            }

            if let Some(entries) = hash_table.get(&key) {
                for entry in entries {
                    candidate_build_indices.push((entry.batch_idx, entry.row_idx));
                    candidate_probe_indices.push(probe_row);
                }
            }
        }

        // For Semi/Anti joins with filter, evaluate filter on all candidates at once
        let (build_indices, probe_indices) =
            if matches!(join_type, JoinType::Semi | JoinType::Anti) && filter.is_some() {
                if candidate_build_indices.is_empty() {
                    (vec![], vec![])
                } else {
                    // Create combined batch with all candidate pairs
                    let combined_batch = create_combined_batch(
                        build_batches,
                        &candidate_build_indices,
                        probe_batch,
                        &candidate_probe_indices,
                        swapped,
                        combined_schema,
                    )?;

                    // Evaluate filter on entire batch
                    let filter_result = evaluate_expr(&combined_batch, filter.unwrap())?;
                    let filter_mask = filter_result
                        .as_any()
                        .downcast_ref::<arrow::array::BooleanArray>();

                    // Filter the indices based on filter result
                    let mut filtered_build_indices = Vec::new();
                    let mut filtered_probe_indices = Vec::new();

                    if let Some(mask) = filter_mask {
                        for i in 0..mask.len() {
                            if mask.value(i) {
                                filtered_build_indices.push(candidate_build_indices[i]);
                                filtered_probe_indices.push(candidate_probe_indices[i]);
                            }
                        }
                    } else {
                        // If filter didn't return boolean array, keep all candidates
                        filtered_build_indices = candidate_build_indices;
                        filtered_probe_indices = candidate_probe_indices;
                    }

                    (filtered_build_indices, filtered_probe_indices)
                }
            } else {
                (candidate_build_indices, candidate_probe_indices)
            };

        // Update matched tracking
        let mut probe_matched = vec![false; probe_batch.num_rows()];
        for (i, (batch_idx, row_idx)) in build_indices.iter().enumerate() {
            probe_matched[probe_indices[i]] = true;
            if *batch_idx < build_matched.len() && *row_idx < build_matched[*batch_idx].len() {
                build_matched[*batch_idx][*row_idx] = true;
            }
        }

        match join_type {
            JoinType::Inner | JoinType::Cross => {
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
            JoinType::Left => {
                // Include all probe rows, with nulls for non-matches
                let (bi, pi) = if swapped {
                    // Build side is right, probe side is left
                    add_unmatched_probe(
                        &build_indices,
                        &probe_indices,
                        &probe_matched,
                        probe_batch.num_rows(),
                    )
                } else {
                    (build_indices.clone(), probe_indices.clone())
                };

                if !bi.is_empty() {
                    let batch = create_joined_batch_with_nulls(
                        build_batches,
                        probe_batch,
                        &bi,
                        &pi,
                        &probe_matched,
                        swapped,
                        output_schema,
                        true, // null for build side
                    )?;
                    results.push(batch);
                }
            }
            JoinType::Right => {
                // Similar to left but for build side
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
            JoinType::Semi | JoinType::Anti => {
                // Semi and Anti joins are handled after processing all probe batches
                // since we need to know which build rows matched across all probes
            }
            JoinType::Full => {
                // Implemented below after processing all probe batches
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
        }
    }

    // For outer joins, add unmatched build rows
    if matches!(join_type, JoinType::Right | JoinType::Full) && !swapped {
        let unmatched_build = collect_unmatched_build(&build_matched);
        if !unmatched_build.is_empty() {
            let batch =
                create_build_only_batch(build_batches, &unmatched_build, output_schema, swapped)?;
            results.push(batch);
        }
    }

    if matches!(join_type, JoinType::Left | JoinType::Full) && swapped {
        let unmatched_build = collect_unmatched_build(&build_matched);
        if !unmatched_build.is_empty() {
            let batch =
                create_build_only_batch(build_batches, &unmatched_build, output_schema, swapped)?;
            results.push(batch);
        }
    }

    // Handle Semi and Anti joins - return build (left) rows based on match status
    if matches!(join_type, JoinType::Semi) {
        // Semi join: return build rows that have at least one match
        let matched_build = collect_matched_build(&build_matched);
        if !matched_build.is_empty() {
            let batch = create_semi_anti_batch(build_batches, &matched_build, output_schema)?;
            results.push(batch);
        }
    }

    if matches!(join_type, JoinType::Anti) {
        // Anti join: return build rows that have no matches
        let unmatched_build = collect_unmatched_build(&build_matched);
        if !unmatched_build.is_empty() {
            let batch = create_semi_anti_batch(build_batches, &unmatched_build, output_schema)?;
            results.push(batch);
        }
    }

    Ok(results)
}

fn add_unmatched_probe(
    build_indices: &[(usize, usize)],
    probe_indices: &[usize],
    probe_matched: &[bool],
    probe_rows: usize,
) -> (Vec<(usize, usize)>, Vec<usize>) {
    let mut bi = build_indices.to_vec();
    let mut pi = probe_indices.to_vec();

    for (row, &matched) in probe_matched.iter().enumerate().take(probe_rows) {
        if !matched {
            bi.push((usize::MAX, 0)); // Sentinel for null
            pi.push(row);
        }
    }

    (bi, pi)
}

fn collect_unmatched_build(build_matched: &[Vec<bool>]) -> Vec<(usize, usize)> {
    let mut unmatched = Vec::new();
    for (batch_idx, matched) in build_matched.iter().enumerate() {
        for (row_idx, &m) in matched.iter().enumerate() {
            if !m {
                unmatched.push((batch_idx, row_idx));
            }
        }
    }
    unmatched
}

fn collect_matched_build(build_matched: &[Vec<bool>]) -> Vec<(usize, usize)> {
    let mut matched = Vec::new();
    for (batch_idx, match_vec) in build_matched.iter().enumerate() {
        for (row_idx, &m) in match_vec.iter().enumerate() {
            if m {
                matched.push((batch_idx, row_idx));
            }
        }
    }
    matched
}

fn create_semi_anti_batch(
    build_batches: &[RecordBatch],
    indices: &[(usize, usize)],
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    if build_batches.is_empty() || indices.is_empty() {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let columns: Result<Vec<ArrayRef>> = (0..build_batches[0].num_columns())
        .map(|col_idx| gather_column(build_batches, col_idx, indices))
        .collect();

    RecordBatch::try_new(output_schema.clone(), columns?).map_err(Into::into)
}

fn create_joined_batch(
    build_batches: &[RecordBatch],
    probe_batch: &RecordBatch,
    build_indices: &[(usize, usize)],
    probe_indices: &[usize],
    swapped: bool,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let _num_rows = build_indices.len();

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

    // Combine in correct order
    let columns: Vec<ArrayRef> = if swapped {
        probe_columns.into_iter().chain(build_columns).collect()
    } else {
        build_columns.into_iter().chain(probe_columns).collect()
    };

    RecordBatch::try_new(output_schema.clone(), columns).map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
fn create_joined_batch_with_nulls(
    build_batches: &[RecordBatch],
    probe_batch: &RecordBatch,
    build_indices: &[(usize, usize)],
    probe_indices: &[usize],
    _probe_matched: &[bool],
    swapped: bool,
    output_schema: &SchemaRef,
    _null_build: bool,
) -> Result<RecordBatch> {
    // For now, just use the regular join
    // A proper implementation would handle nulls for unmatched rows
    create_joined_batch(
        build_batches,
        probe_batch,
        build_indices,
        probe_indices,
        swapped,
        output_schema,
    )
}

fn create_build_only_batch(
    build_batches: &[RecordBatch],
    indices: &[(usize, usize)],
    output_schema: &SchemaRef,
    swapped: bool,
) -> Result<RecordBatch> {
    if build_batches.is_empty() {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let build_columns: Result<Vec<ArrayRef>> = (0..build_batches[0].num_columns())
        .map(|col_idx| gather_column(build_batches, col_idx, indices))
        .collect();
    let build_columns = build_columns?;

    // Create null arrays for probe side
    let num_rows = indices.len();
    let probe_num_cols = output_schema.fields().len() - build_batches[0].num_columns();

    let null_columns: Vec<ArrayRef> = (0..probe_num_cols)
        .map(|i| {
            let field_idx = if swapped {
                i
            } else {
                build_batches[0].num_columns() + i
            };
            let dt = output_schema.field(field_idx).data_type();
            arrow::array::new_null_array(dt, num_rows)
        })
        .collect();

    let columns: Vec<ArrayRef> = if swapped {
        null_columns.into_iter().chain(build_columns).collect()
    } else {
        build_columns.into_iter().chain(null_columns).collect()
    };

    RecordBatch::try_new(output_schema.clone(), columns).map_err(Into::into)
}

fn gather_column(
    batches: &[RecordBatch],
    col_idx: usize,
    indices: &[(usize, usize)],
) -> Result<ArrayRef> {
    // Build indices for each batch
    let mut batch_indices: HashMap<usize, Vec<(usize, u32)>> = HashMap::new();
    for (out_idx, &(batch_idx, row_idx)) in indices.iter().enumerate() {
        if batch_idx != usize::MAX {
            batch_indices
                .entry(batch_idx)
                .or_default()
                .push((out_idx, row_idx as u32));
        }
    }

    // Create output array
    let total_len = indices.len();
    let dt = batches[0].column(col_idx).data_type();

    // Gather from each batch
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
                arrow::compute::take(&taken, &UInt32Array::from(vec![i as u32]), None)?,
            ));
        }
    }

    // Sort by output index and concatenate
    builders_data.sort_by_key(|(idx, _)| *idx);

    if builders_data.is_empty() {
        return Ok(arrow::array::new_null_array(dt, total_len));
    }

    // Simple approach: gather one at a time
    let arrays: Vec<&dyn arrow::array::Array> =
        builders_data.iter().map(|(_, arr)| arr.as_ref()).collect();

    if arrays.is_empty() {
        Ok(arrow::array::new_null_array(dt, total_len))
    } else {
        compute::concat(&arrays).map_err(Into::into)
    }
}

#[allow(dead_code)] // Reserved for join filter optimization
fn filter_batch_by_indices(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
    let indices_arr = UInt32Array::from(indices.iter().map(|&i| i as u32).collect::<Vec<_>>());

    let columns: Result<Vec<ArrayRef>> = batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &indices_arr, None).map_err(Into::into))
        .collect();

    RecordBatch::try_new(batch.schema(), columns?).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::MemoryTableExec;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::TryStreamExt;

    fn create_left_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap()
    }

    fn create_right_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 2, 5])),
                Arc::new(Int64Array::from(vec![10, 20, 21, 50])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_inner_join() {
        let left = create_left_batch();
        let right = create_right_batch();

        let left_scan = Arc::new(MemoryTableExec::new(
            "left",
            left.schema(),
            vec![left],
            None,
        ));
        let right_scan = Arc::new(MemoryTableExec::new(
            "right",
            right.schema(),
            vec![right],
            None,
        ));

        let join = HashJoinExec::new(
            left_scan,
            right_scan,
            vec![(Expr::column("id"), Expr::column("id"))],
            JoinType::Inner,
        );

        let stream = join.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // id=1 matches once, id=2 matches twice
    }

    #[tokio::test]
    async fn test_semi_join() {
        let left = create_left_batch();
        let right = create_right_batch();

        let left_scan = Arc::new(MemoryTableExec::new(
            "left",
            left.schema(),
            vec![left],
            None,
        ));
        let right_scan = Arc::new(MemoryTableExec::new(
            "right",
            right.schema(),
            vec![right],
            None,
        ));

        let join = HashJoinExec::new(
            left_scan,
            right_scan,
            vec![(Expr::column("id"), Expr::column("id"))],
            JoinType::Semi,
        );

        let stream = join.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // ids 1 and 2 exist in right
    }

    #[tokio::test]
    async fn test_anti_join() {
        let left = create_left_batch();
        let right = create_right_batch();

        let left_scan = Arc::new(MemoryTableExec::new(
            "left",
            left.schema(),
            vec![left],
            None,
        ));
        let right_scan = Arc::new(MemoryTableExec::new(
            "right",
            right.schema(),
            vec![right],
            None,
        ));

        let join = HashJoinExec::new(
            left_scan,
            right_scan,
            vec![(Expr::column("id"), Expr::column("id"))],
            JoinType::Anti,
        );

        let stream = join.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // ids 3 and 4 don't exist in right
    }
}
