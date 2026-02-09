//! Hash join operator

use crate::error::Result;
use crate::physical::operators::filter::evaluate_expr;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::{BinaryOp, Expr, JoinType};
use arrow::array::{Array, ArrayRef, Int64Array, UInt32Array, UInt64Array};
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
    /// Fast path: specialized i64 hash table for single-key Int64 joins
    i64_hash_table: Option<HashMap<i64, Vec<HashEntry>>>,
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

        // For Semi/Anti joins, start probe collection early so it overlaps with build side
        let probe_prefetch_handle = if matches!(self.join_type, JoinType::Semi | JoinType::Anti) {
            let probe = probe_side.clone();
            Some(tokio::spawn(async move {
                let probe_partitions = probe.output_partitions().max(1);
                let handles: Vec<_> = (0..probe_partitions)
                    .map(|p| {
                        let probe = probe.clone();
                        tokio::spawn(async move {
                            let stream = probe.execute(p).await?;
                            let batches: Vec<RecordBatch> = stream.try_collect().await?;
                            Ok::<_, crate::error::QueryError>(batches)
                        })
                    })
                    .collect();
                let mut all_batches = Vec::new();
                for handle in handles {
                    let batches = handle.await.map_err(|e| {
                        crate::error::QueryError::Execution(format!(
                            "Probe partition task failed: {}",
                            e
                        ))
                    })??;
                    all_batches.extend(batches);
                }
                Ok::<_, crate::error::QueryError>(all_batches)
            }))
        } else {
            None
        };

        // All join types can skip the generic hash table when i64 fast path is available.
        // The generic probe loop has i64 fallback logic, and specialized parallel paths
        // (Semi/Anti, Inner) handle i64 directly.
        let can_skip_generic_ht = true;

        // Get or build the cached build side (computed ONCE, reused across all partitions)
        // For Semi/Anti, probe collection runs concurrently via probe_prefetch_handle
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

                // Collect all build partitions in parallel using tokio::spawn
                let handles: Vec<_> = (0..build_partitions)
                    .map(|p| {
                        let build = build_side.clone();
                        tokio::spawn(async move {
                            let stream = build.execute(p).await?;
                            let batches: Vec<RecordBatch> = stream.try_collect().await?;
                            Ok::<_, crate::error::QueryError>(batches)
                        })
                    })
                    .collect();
                let mut partition_results = Vec::with_capacity(handles.len());
                for handle in handles {
                    let batches = handle.await.map_err(|e| {
                        crate::error::QueryError::Execution(format!(
                            "Build partition task failed: {}",
                            e
                        ))
                    })??;
                    partition_results.push(batches);
                }

                let mut build_batches = Vec::new();
                let mut total_build_rows = 0usize;
                let mut total_build_bytes = 0usize;
                for batches in partition_results {
                    for b in &batches {
                        total_build_rows += b.num_rows();
                        total_build_bytes += b.get_array_memory_size();
                    }
                    build_batches.extend(batches);
                }
                debug_log(&format!(
                    "Build side collected: {} batches, {} total rows, {} bytes",
                    build_batches.len(),
                    total_build_rows,
                    total_build_bytes
                ));

                // Try specialized i64 hash table first (much faster for Int64 keys)
                let i64_hash_table = if build_keys.len() == 1 {
                    build_i64_hash_table(&build_batches, &build_keys[0])
                } else {
                    None
                };

                // Skip expensive generic hash table build when i64 fast path handles all lookups
                let hash_table = if i64_hash_table.is_some() && can_skip_generic_ht {
                    HashMap::new()
                } else {
                    build_hash_table(&build_batches, &build_keys)?
                };

                Ok::<_, crate::error::QueryError>(BuildSideCache {
                    batches: build_batches,
                    hash_table,
                    i64_hash_table,
                })
            })
            .await?;

        // Collect probe batches. For Semi/Anti, await the prefetched probe data
        // that was running concurrently with the build side.
        let probe_batches: Vec<RecordBatch> = if let Some(handle) = probe_prefetch_handle {
            handle.await.map_err(|e| {
                crate::error::QueryError::Execution(format!("Probe prefetch task failed: {}", e))
            })??
        } else {
            let probe_stream = probe_side.execute(partition).await?;
            probe_stream.try_collect().await?
        };

        // Safety check: prevent cross join explosions
        let build_rows: usize = cache.batches.iter().map(|b| b.num_rows()).sum();
        let probe_rows: usize = probe_batches.iter().map(|b| b.num_rows()).sum();
        if self.join_type == JoinType::Cross && build_rows > 0 && probe_rows > 0 {
            let max_output = build_rows.saturating_mul(probe_rows);
            const CROSS_JOIN_LIMIT: usize = 10_000_000;
            if max_output > CROSS_JOIN_LIMIT {
                return Err(crate::error::QueryError::Execution(format!(
                    "Cross join would produce {} rows ({} x {}), exceeding limit of {}. \
                    This usually indicates missing join conditions in the query.",
                    max_output, build_rows, probe_rows, CROSS_JOIN_LIMIT
                )));
            }
        }

        let result = probe_hash_table(
            &cache.batches,
            &probe_batches,
            &cache.hash_table,
            cache.i64_hash_table.as_ref(),
            probe_keys,
            self.join_type,
            swapped,
            &self.schema,
            self.filter.as_ref(),
            &self.combined_schema,
        )?;

        Ok(Box::pin(stream::iter(result.into_iter().map(Ok))))
    }

    fn output_partitions(&self) -> usize {
        // Semi/Anti joins must see ALL probe rows to correctly determine
        // matched/unmatched build rows, so they must use a single partition.
        // Other join types can be parallelized by probe side partitions.
        match self.join_type {
            JoinType::Semi | JoinType::Anti => 1,
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

/// Build a specialized i64 hash table for single-key Int64 joins.
/// Returns None if the key expression doesn't evaluate to Int64/Int32.
fn build_i64_hash_table(
    batches: &[RecordBatch],
    key_expr: &Expr,
) -> Option<HashMap<i64, Vec<HashEntry>>> {
    if batches.is_empty() {
        return Some(HashMap::new());
    }

    // Check if the key evaluates to an Int64-compatible type
    let first_key = evaluate_expr(&batches[0], key_expr).ok()?;
    let is_int = first_key.as_any().downcast_ref::<Int64Array>().is_some()
        || first_key
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .is_some()
        || first_key
            .as_any()
            .downcast_ref::<arrow::array::Date32Array>()
            .is_some();
    if !is_int {
        return None;
    }

    let mut table: HashMap<i64, Vec<HashEntry>> = HashMap::new();
    for (batch_idx, batch) in batches.iter().enumerate() {
        let key_arr = evaluate_expr(batch, key_expr).ok()?;
        if let Some(int_arr) = key_arr.as_any().downcast_ref::<Int64Array>() {
            for row_idx in 0..batch.num_rows() {
                if int_arr.is_null(row_idx) {
                    continue;
                }
                table
                    .entry(int_arr.value(row_idx))
                    .or_default()
                    .push(HashEntry { batch_idx, row_idx });
            }
        } else if let Some(int_arr) = key_arr.as_any().downcast_ref::<arrow::array::Int32Array>() {
            for row_idx in 0..batch.num_rows() {
                if int_arr.is_null(row_idx) {
                    continue;
                }
                table
                    .entry(int_arr.value(row_idx) as i64)
                    .or_default()
                    .push(HashEntry { batch_idx, row_idx });
            }
        } else if let Some(int_arr) = key_arr.as_any().downcast_ref::<arrow::array::Date32Array>() {
            for row_idx in 0..batch.num_rows() {
                if int_arr.is_null(row_idx) {
                    continue;
                }
                table
                    .entry(int_arr.value(row_idx) as i64)
                    .or_default()
                    .push(HashEntry { batch_idx, row_idx });
            }
        }
    }
    Some(table)
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

/// A pre-compiled filter for fast evaluation without per-row batch creation.
/// Recognizes patterns like `col_a != col_b` and evaluates directly from arrays.
struct CompiledFilter {
    build_col_idx: usize,
    probe_col_idx: usize,
    op: BinaryOp,
}

impl CompiledFilter {
    /// Try to compile a filter expression into a direct column comparison.
    /// Uses the combined schema (build columns first, then probe columns) to resolve indices.
    /// Returns None if the filter is too complex.
    fn try_compile(
        filter: &Expr,
        build_schema: &Schema,
        probe_schema: &Schema,
        swapped: bool,
    ) -> Option<Self> {
        if let Expr::BinaryExpr { left, op, right } = filter {
            if !matches!(
                op,
                BinaryOp::Eq
                    | BinaryOp::NotEq
                    | BinaryOp::Lt
                    | BinaryOp::LtEq
                    | BinaryOp::Gt
                    | BinaryOp::GtEq
            ) {
                return None;
            }
            let left_col = match left.as_ref() {
                Expr::Column(c) => c,
                _ => return None,
            };
            let right_col = match right.as_ref() {
                Expr::Column(c) => c,
                _ => return None,
            };

            // Resolve columns in the combined schema.
            // Combined schema = build fields first (or probe first if swapped), then the other.
            let (first_schema, second_schema) = if swapped {
                (probe_schema, build_schema)
            } else {
                (build_schema, probe_schema)
            };
            let first_len = first_schema.fields().len();

            let left_combined_idx =
                resolve_column_in_combined(left_col, first_schema, second_schema, first_len)?;
            let right_combined_idx =
                resolve_column_in_combined(right_col, first_schema, second_schema, first_len)?;

            // Determine which side each column is on
            let (left_side, left_local_idx) = if left_combined_idx < first_len {
                if swapped {
                    (ColumnSide::Probe, left_combined_idx)
                } else {
                    (ColumnSide::Build, left_combined_idx)
                }
            } else {
                if swapped {
                    (ColumnSide::Build, left_combined_idx - first_len)
                } else {
                    (ColumnSide::Probe, left_combined_idx - first_len)
                }
            };

            let (right_side, right_local_idx) = if right_combined_idx < first_len {
                if swapped {
                    (ColumnSide::Probe, right_combined_idx)
                } else {
                    (ColumnSide::Build, right_combined_idx)
                }
            } else {
                if swapped {
                    (ColumnSide::Build, right_combined_idx - first_len)
                } else {
                    (ColumnSide::Probe, right_combined_idx - first_len)
                }
            };

            // We need one build-side column and one probe-side column
            if left_side == ColumnSide::Build && right_side == ColumnSide::Probe {
                Some(CompiledFilter {
                    build_col_idx: left_local_idx,
                    probe_col_idx: right_local_idx,
                    op: *op,
                })
            } else if left_side == ColumnSide::Probe && right_side == ColumnSide::Build {
                let swapped_op = match op {
                    BinaryOp::Lt => BinaryOp::Gt,
                    BinaryOp::LtEq => BinaryOp::GtEq,
                    BinaryOp::Gt => BinaryOp::Lt,
                    BinaryOp::GtEq => BinaryOp::LtEq,
                    other => *other,
                };
                Some(CompiledFilter {
                    build_col_idx: right_local_idx,
                    probe_col_idx: left_local_idx,
                    op: swapped_op,
                })
            } else {
                None // Both on same side - can't optimize
            }
        } else {
            None
        }
    }

    /// Evaluate the filter for a single (build_row, probe_row) pair directly from arrays.
    #[inline(always)]
    fn evaluate(
        &self,
        build_batch: &RecordBatch,
        build_row: usize,
        probe_batch: &RecordBatch,
        probe_row: usize,
    ) -> bool {
        let build_col = build_batch.column(self.build_col_idx);
        let probe_col = probe_batch.column(self.probe_col_idx);

        // Fast path for Int64 (most common for join keys)
        if let (Some(b_arr), Some(p_arr)) = (
            build_col.as_any().downcast_ref::<Int64Array>(),
            probe_col.as_any().downcast_ref::<Int64Array>(),
        ) {
            let bv = b_arr.value(build_row);
            let pv = p_arr.value(probe_row);
            return match self.op {
                BinaryOp::Eq => bv == pv,
                BinaryOp::NotEq => bv != pv,
                BinaryOp::Lt => bv < pv,
                BinaryOp::LtEq => bv <= pv,
                BinaryOp::Gt => bv > pv,
                BinaryOp::GtEq => bv >= pv,
                _ => false,
            };
        }

        // Float64 path
        if let (Some(b_arr), Some(p_arr)) = (
            build_col
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>(),
            probe_col
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>(),
        ) {
            let bv = b_arr.value(build_row);
            let pv = p_arr.value(probe_row);
            return match self.op {
                BinaryOp::Eq => bv == pv,
                BinaryOp::NotEq => bv != pv,
                BinaryOp::Lt => bv < pv,
                BinaryOp::LtEq => bv <= pv,
                BinaryOp::Gt => bv > pv,
                BinaryOp::GtEq => bv >= pv,
                _ => false,
            };
        }

        // Utf8 path
        if let (Some(b_arr), Some(p_arr)) = (
            build_col
                .as_any()
                .downcast_ref::<arrow::array::StringArray>(),
            probe_col
                .as_any()
                .downcast_ref::<arrow::array::StringArray>(),
        ) {
            let bv = b_arr.value(build_row);
            let pv = p_arr.value(probe_row);
            return match self.op {
                BinaryOp::Eq => bv == pv,
                BinaryOp::NotEq => bv != pv,
                BinaryOp::Lt => bv < pv,
                BinaryOp::LtEq => bv <= pv,
                BinaryOp::Gt => bv > pv,
                BinaryOp::GtEq => bv >= pv,
                _ => false,
            };
        }

        // Int32 path
        if let (Some(b_arr), Some(p_arr)) = (
            build_col
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>(),
            probe_col
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>(),
        ) {
            let bv = b_arr.value(build_row);
            let pv = p_arr.value(probe_row);
            return match self.op {
                BinaryOp::Eq => bv == pv,
                BinaryOp::NotEq => bv != pv,
                BinaryOp::Lt => bv < pv,
                BinaryOp::LtEq => bv <= pv,
                BinaryOp::Gt => bv > pv,
                BinaryOp::GtEq => bv >= pv,
                _ => false,
            };
        }

        false // Unknown type - treat as no match
    }
}

#[derive(PartialEq)]
enum ColumnSide {
    Build,
    Probe,
}

/// Resolve a column to an index in the combined schema, using the same logic as find_column_index.
fn resolve_column_in_combined(
    col: &crate::planner::Column,
    first_schema: &Schema,
    second_schema: &Schema,
    _first_len: usize,
) -> Option<usize> {
    // Build combined schema (same as used for filter evaluation in the hash join)
    let combined_fields: Vec<_> = first_schema
        .fields()
        .iter()
        .chain(second_schema.fields().iter())
        .cloned()
        .collect();
    let combined = Schema::new(combined_fields);

    // Use the exact same resolution as find_column_index in filter.rs
    // 1. Try qualified name
    if let Some(relation) = &col.relation {
        let qualified = format!("{}.{}", relation, col.name);
        if let Ok(idx) = combined.index_of(&qualified) {
            return Some(idx);
        }
    }

    // 2. Try unqualified name
    if let Ok(idx) = combined.index_of(&col.name) {
        return Some(idx);
    }

    // 3. Try suffix match
    let suffix = format!(".{}", col.name);
    for (i, field) in combined.fields().iter().enumerate() {
        if field.name().ends_with(&suffix) || field.name() == &col.name {
            return Some(i);
        }
    }

    None
}

/// Parallel probe for INNER joins using specialized i64 hash table.
/// Processes probe rows in parallel chunks using rayon, with direct Int64 array access
/// to avoid per-row JoinKey allocation overhead.
fn probe_inner_i64_parallel(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    i64_hash_table: &HashMap<i64, Vec<HashEntry>>,
    probe_key_expr: &Expr,
    swapped: bool,
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    const CHUNK_SIZE: usize = 65536;

    let mut results = Vec::new();

    for probe_batch in probe_batches {
        // Evaluate key expression once for the whole batch
        let key_arr = evaluate_expr(probe_batch, probe_key_expr)?;
        let n_rows = probe_batch.num_rows();

        // Get direct access to the key array values (no per-row allocation)
        let key_values: &[i64];
        let _int32_values: Vec<i64>; // storage for converted i32 values
        if let Some(int_arr) = key_arr.as_any().downcast_ref::<Int64Array>() {
            key_values = int_arr.values();
            _int32_values = Vec::new();
        } else if let Some(int_arr) = key_arr.as_any().downcast_ref::<arrow::array::Int32Array>() {
            _int32_values = int_arr.values().iter().map(|v| *v as i64).collect();
            key_values = &_int32_values;
        } else {
            // Can't use i64 fast path for this type
            continue;
        }

        // Check null bitmap once (most join keys are NOT NULL so this is often empty)
        let null_bitmap = key_arr.nulls();

        // Split into chunks and process in parallel
        let chunks: Vec<std::ops::Range<usize>> = (0..n_rows)
            .step_by(CHUNK_SIZE)
            .map(|start| start..std::cmp::min(start + CHUNK_SIZE, n_rows))
            .collect();

        let chunk_results: Vec<(Vec<(usize, usize)>, Vec<usize>)> = chunks
            .par_iter()
            .map(|range| {
                let mut build_indices = Vec::new();
                let mut probe_indices = Vec::new();

                for probe_row in range.clone() {
                    // Fast null check via bitmap
                    if let Some(nb) = null_bitmap {
                        if !nb.is_valid(probe_row) {
                            continue;
                        }
                    }
                    let key_val = key_values[probe_row];
                    if let Some(entries) = i64_hash_table.get(&key_val) {
                        for entry in entries {
                            build_indices.push((entry.batch_idx, entry.row_idx));
                            probe_indices.push(probe_row);
                        }
                    }
                }

                (build_indices, probe_indices)
            })
            .collect();

        // Merge chunk results
        let mut all_build_indices = Vec::new();
        let mut all_probe_indices = Vec::new();
        for (bi, pi) in chunk_results {
            all_build_indices.extend(bi);
            all_probe_indices.extend(pi);
        }

        if !all_build_indices.is_empty() {
            let batch = create_joined_batch(
                build_batches,
                probe_batch,
                &all_build_indices,
                &all_probe_indices,
                swapped,
                output_schema,
            )?;
            results.push(batch);
        }
    }

    Ok(results)
}

/// Parallel probe for SEMI/ANTI joins - uses rayon for parallel execution
#[allow(clippy::too_many_arguments)]
fn probe_semi_anti_parallel(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    hash_table: &HashMap<JoinKey, Vec<HashEntry>>,
    cached_i64_ht: Option<&HashMap<i64, Vec<HashEntry>>>,
    probe_key_exprs: &[Expr],
    join_type: JoinType,
    swapped: bool,
    output_schema: &SchemaRef,
    filter: Option<&Expr>,
    combined_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    use std::sync::atomic::{AtomicBool, Ordering};

    // Track which build rows have been matched using atomic bools for parallel access
    let build_matched: Vec<Vec<AtomicBool>> = build_batches
        .iter()
        .map(|b| (0..b.num_rows()).map(|_| AtomicBool::new(false)).collect())
        .collect();

    // Try to compile the filter for fast direct evaluation
    let build_schema = if !build_batches.is_empty() {
        build_batches[0].schema()
    } else {
        Arc::new(Schema::empty())
    };
    let probe_schema = if !probe_batches.is_empty() {
        probe_batches[0].schema()
    } else {
        Arc::new(Schema::empty())
    };
    let compiled_filter =
        filter.and_then(|f| CompiledFilter::try_compile(f, &build_schema, &probe_schema, swapped));

    // Use cached i64 hash table if available, otherwise build one
    let local_i64_ht: Option<HashMap<i64, Vec<HashEntry>>> = if cached_i64_ht.is_some() {
        None // Using cached, no need to build local
    } else if probe_key_exprs.len() == 1 {
        build_i64_hash_table(build_batches, &probe_key_exprs[0])
    } else {
        None
    };
    let i64_ht_ref: Option<&HashMap<i64, Vec<HashEntry>>> = cached_i64_ht.or(local_i64_ht.as_ref());

    // Process all probe batches in parallel using chunked intra-batch parallelism
    const CHUNK_SIZE: usize = 65536;
    for probe_batch in probe_batches {
        let probe_key_arr = if probe_key_exprs.len() == 1 {
            Some(evaluate_expr(probe_batch, &probe_key_exprs[0])?)
        } else {
            None
        };

        // Get direct i64 values if available
        let i64_values: Option<&[i64]> = probe_key_arr.as_ref().and_then(|arr| {
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .map(|a| a.values().as_ref())
        });
        let null_bitmap = probe_key_arr.as_ref().and_then(|arr| arr.nulls().cloned());

        let n_rows = probe_batch.num_rows();
        let chunks: Vec<std::ops::Range<usize>> = (0..n_rows)
            .step_by(CHUNK_SIZE)
            .map(|start| start..std::cmp::min(start + CHUNK_SIZE, n_rows))
            .collect();

        // Fall back to full key extraction if no i64 fast path
        let probe_key_arrays: Option<Vec<ArrayRef>> =
            if i64_values.is_none() || i64_ht_ref.is_none() {
                let arrays: Result<Vec<ArrayRef>> = probe_key_exprs
                    .iter()
                    .map(|e| evaluate_expr(probe_batch, e))
                    .collect();
                Some(arrays?)
            } else {
                None
            };

        chunks.par_iter().try_for_each(|range| {
            for probe_row in range.clone() {
                // Fast i64 path: direct array access, no JoinKey allocation
                let entries_opt = if let (Some(vals), Some(ht)) = (i64_values, i64_ht_ref) {
                    if let Some(ref nb) = null_bitmap {
                        if !nb.is_valid(probe_row) {
                            continue;
                        }
                    }
                    ht.get(&vals[probe_row])
                } else if let Some(ref key_arrays) = probe_key_arrays {
                    let key = extract_join_key(key_arrays, probe_row);
                    if key.values.iter().any(|v| matches!(v, JoinValue::Null)) {
                        continue;
                    }
                    hash_table.get(&key)
                } else {
                    None
                };

                if let Some(entries) = entries_opt {
                    for entry in entries {
                        if let Some(ref cf) = compiled_filter {
                            let build_batch = &build_batches[entry.batch_idx];
                            if cf.evaluate(build_batch, entry.row_idx, probe_batch, probe_row) {
                                build_matched[entry.batch_idx][entry.row_idx]
                                    .store(true, Ordering::Relaxed);
                                break;
                            }
                        } else if let Some(filter_expr) = filter {
                            let build_row_batch = create_single_row_combined_batch(
                                build_batches,
                                entry.batch_idx,
                                entry.row_idx,
                                probe_batch,
                                probe_row,
                                swapped,
                                combined_schema,
                            )?;
                            let filter_result = evaluate_expr(&build_row_batch, filter_expr)?;
                            if let Some(bool_arr) = filter_result
                                .as_any()
                                .downcast_ref::<arrow::array::BooleanArray>()
                            {
                                if bool_arr.len() > 0 && bool_arr.value(0) {
                                    build_matched[entry.batch_idx][entry.row_idx]
                                        .store(true, Ordering::Relaxed);
                                    break;
                                }
                            }
                        } else {
                            build_matched[entry.batch_idx][entry.row_idx]
                                .store(true, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }
            Ok::<(), crate::error::QueryError>(())
        })?;
    }

    // Convert atomic bools to regular bools and create output
    let matched_rows: Vec<(usize, usize)> = build_matched
        .iter()
        .enumerate()
        .flat_map(|(batch_idx, rows)| {
            rows.iter()
                .enumerate()
                .filter_map(move |(row_idx, matched)| {
                    if matched.load(Ordering::Relaxed) {
                        Some((batch_idx, row_idx))
                    } else {
                        None
                    }
                })
        })
        .collect();

    let unmatched_rows: Vec<(usize, usize)> = build_matched
        .iter()
        .enumerate()
        .flat_map(|(batch_idx, rows)| {
            rows.iter()
                .enumerate()
                .filter_map(move |(row_idx, matched)| {
                    if !matched.load(Ordering::Relaxed) {
                        Some((batch_idx, row_idx))
                    } else {
                        None
                    }
                })
        })
        .collect();

    let mut results = Vec::new();
    if matches!(join_type, JoinType::Semi) && !matched_rows.is_empty() {
        let batch = create_semi_anti_batch(build_batches, &matched_rows, output_schema)?;
        results.push(batch);
    }
    if matches!(join_type, JoinType::Anti) && !unmatched_rows.is_empty() {
        let batch = create_semi_anti_batch(build_batches, &unmatched_rows, output_schema)?;
        results.push(batch);
    }

    Ok(results)
}

/// Create a combined batch with a single row pair for filter evaluation
fn create_single_row_combined_batch(
    build_batches: &[RecordBatch],
    build_batch_idx: usize,
    build_row_idx: usize,
    probe_batch: &RecordBatch,
    probe_row: usize,
    swapped: bool,
    combined_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let build_batch = &build_batches[build_batch_idx];

    // Extract single row from build side
    let build_indices = UInt32Array::from(vec![build_row_idx as u32]);
    let build_columns: Vec<ArrayRef> = build_batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &build_indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    // Extract single row from probe side
    let probe_indices = UInt32Array::from(vec![probe_row as u32]);
    let probe_columns: Vec<ArrayRef> = probe_batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &probe_indices, None))
        .collect::<std::result::Result<Vec<_>, _>>()?;

    // Combine in correct order
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
    i64_hash_table: Option<&HashMap<i64, Vec<HashEntry>>>,
    probe_key_exprs: &[Expr],
    join_type: JoinType,
    swapped: bool,
    output_schema: &SchemaRef,
    filter: Option<&Expr>,
    combined_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    // Use parallel path for SEMI/ANTI joins with sufficient data
    let total_probe_rows: usize = probe_batches.iter().map(|b| b.num_rows()).sum();
    if matches!(join_type, JoinType::Semi | JoinType::Anti) && total_probe_rows > 1000 {
        return probe_semi_anti_parallel(
            build_batches,
            probe_batches,
            hash_table,
            i64_hash_table,
            probe_key_exprs,
            join_type,
            swapped,
            output_schema,
            filter,
            combined_schema,
        );
    }

    // Use parallel i64 fast path for inner joins with sufficient data
    if matches!(join_type, JoinType::Inner) && filter.is_none() && total_probe_rows > 10_000 {
        if let Some(i64_ht) = i64_hash_table {
            return probe_inner_i64_parallel(
                build_batches,
                probe_batches,
                i64_ht,
                &probe_key_exprs[0],
                swapped,
                output_schema,
            );
        }
    }

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

            // Use i64 hash table if available, fall back to generic
            let entries: Option<&Vec<HashEntry>> = if let Some(i64_ht) = i64_hash_table {
                if let [JoinValue::Int64(val)] = key.values.as_slice() {
                    i64_ht.get(val)
                } else {
                    hash_table.get(&key)
                }
            } else {
                hash_table.get(&key)
            };

            if let Some(entries) = entries {
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
            JoinType::Single | JoinType::Mark => {
                // Single and Mark joins are similar to Semi/Anti - handle after all probes
                // Single: for scalar subqueries, returns one row per outer row
                // Mark: for IN subqueries, adds a boolean column for match status
                // For now, treat like Semi join (keep matched rows)
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
    if indices.is_empty() {
        let dt = batches[0].column(col_idx).data_type();
        return Ok(arrow::array::new_null_array(dt, 0));
    }

    if batches.len() == 1 {
        // Fast path: single batch - direct take()
        let col = batches[0].column(col_idx);
        let take_indices: Vec<u32> = indices.iter().map(|&(_, row_idx)| row_idx as u32).collect();
        let take_arr = UInt32Array::from(take_indices);
        return compute::take(col.as_ref(), &take_arr, None).map_err(Into::into);
    }

    // Multi-batch: compute batch offsets, then do a single take on concatenated array
    let mut offsets = Vec::with_capacity(batches.len());
    let mut offset = 0usize;
    for batch in batches {
        offsets.push(offset);
        offset += batch.num_rows();
    }

    // Check if we need LargeUtf8 promotion to avoid 2GB i32 offset overflow
    let dt = batches[0].column(col_idx).data_type().clone();
    let needs_large_utf8 = dt == arrow::datatypes::DataType::Utf8 && {
        let total_bytes: usize = batches
            .iter()
            .map(|b| b.column(col_idx).get_array_memory_size())
            .sum();
        total_bytes > 1_500_000_000 // 1.5GB threshold
    };

    let all_arrays: Vec<ArrayRef> = if needs_large_utf8 {
        batches
            .iter()
            .map(|b| {
                compute::cast(
                    b.column(col_idx).as_ref(),
                    &arrow::datatypes::DataType::LargeUtf8,
                )
                .map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?
    } else {
        batches.iter().map(|b| b.column(col_idx).clone()).collect()
    };

    let all_refs: Vec<&dyn arrow::array::Array> = all_arrays.iter().map(|a| a.as_ref()).collect();
    let concatenated = compute::concat(&all_refs)?;

    let take_indices: Vec<u32> = indices
        .iter()
        .map(|&(batch_idx, row_idx)| {
            if batch_idx == usize::MAX {
                0 // null row marker - will be handled by null bitmap
            } else {
                (offsets[batch_idx] + row_idx) as u32
            }
        })
        .collect();
    let take_arr = UInt32Array::from(take_indices);
    let result = compute::take(concatenated.as_ref(), &take_arr, None)?;

    // Cast back to Utf8 if we promoted
    if needs_large_utf8 {
        compute::cast(result.as_ref(), &arrow::datatypes::DataType::Utf8).map_err(Into::into)
    } else {
        Ok(result)
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
