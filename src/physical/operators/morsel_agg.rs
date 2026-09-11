//! Morsel-driven aggregate operator
//!
//! This operator implements parallel aggregation using morsel-driven parallelism
//! for Parquet data sources. It provides significant performance improvements over
//! the standard HashAggregateExec by:
//! - Reading Parquet files in parallel across row groups
//! - Using thread-local hash tables to avoid contention
//! - Final merge of all thread-local states

use crate::error::{QueryError, Result};
use crate::execution::reserved_vec::ReservedVec;
use crate::execution::{process_memory_pool, MemoryPool, MemoryReservation, SharedMemoryPool};
use crate::physical::dense_domain::{bounded_i64_width, checked_i64_index};
use crate::physical::morsel::{ParallelParquetSource, DEFAULT_MORSEL_SIZE};
use crate::physical::morsel_agg::AggregationState;
use crate::physical::operators::evaluate_expr;
use crate::physical::operators::hash_agg::AggregateExpr;
use crate::physical::operators::TableProvider;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::{AggregateFunction, Expr};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int32Array, Int64Array,
};
use arrow::compute;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;
use rayon::prelude::*;
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;

/// Morsel-driven aggregate execution operator
///
/// This operator is used for aggregations over Parquet data sources.
/// It uses parallel row-group reading and thread-local hash tables
/// for optimal performance.
pub struct MorselAggregateExec {
    /// Parquet file paths to read from. Empty when `native_provider` is set
    /// (task 005): a native table has no parquet files backing it.
    files: Vec<PathBuf>,
    /// Columns to project from Parquet (indices)
    projection: Option<Vec<usize>>,
    /// Optional filter to apply before aggregation
    filter: Option<Expr>,
    /// Group by expressions
    group_by: Vec<Expr>,
    /// Aggregate expressions
    aggregates: Vec<AggregateExpr>,
    /// Output schema
    schema: SchemaRef,
    /// Input schema from Parquet files
    input_schema: SchemaRef,
    /// HAVING predicate applied per output batch (references output columns)
    post_filter: Option<Expr>,
    /// Set only for a native-table source (task 005, native-tables-
    /// foundation epic): `files` stays empty and the dense-direct-address
    /// path (`try_execute_dense_direct`) reads key bounds via this
    /// provider's `TableProvider::statistics()` and scans via
    /// `TableProvider::scan_with_filter()` instead of opening parquet
    /// footers/row groups. There is no generic (non-dense) execution tier
    /// for a native-mode instance the way there is for Parquet — see
    /// `execute()` — so the physical planner only ever sets this once it
    /// has independently confirmed (`dense_direct_shape` +
    /// `dense_direct_key_bounds`) that the dense path will accept the
    /// group-by/aggregate shape.
    native_provider: Option<Arc<dyn TableProvider>>,
    /// Admission domain for the dense accumulator allocation. Other morsel
    /// representations and provider buffers are not yet reservation-backed.
    memory_pool: SharedMemoryPool,
}

impl fmt::Debug for MorselAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MorselAggregateExec")
            .field("files", &self.files.len())
            .field("native", &self.native_provider.is_some())
            .field("group_by", &self.group_by)
            .field("schema", &self.schema)
            .finish()
    }
}

impl MorselAggregateExec {
    /// Create a new MorselAggregateExec
    pub fn new(
        files: Vec<PathBuf>,
        input_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filter: Option<Expr>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            files,
            projection,
            filter,
            group_by,
            aggregates,
            schema,
            input_schema,
            post_filter: None,
            native_provider: None,
            memory_pool: process_memory_pool(),
        }
    }

    /// Use the query's admission domain for dense accumulator storage.
    pub fn with_memory_pool(mut self, memory_pool: SharedMemoryPool) -> Self {
        self.memory_pool = memory_pool;
        self
    }

    /// Attach a HAVING predicate applied to output batches before returning.
    pub fn with_post_filter(mut self, post_filter: Option<Expr>) -> Self {
        self.post_filter = post_filter;
        self
    }

    /// Switch this instance into native-table mode (task 005): the
    /// dense-direct-address path reads through `provider` instead of
    /// `files`. Only the physical planner should call this, and only after
    /// confirming (see `dense_direct_shape`/`dense_direct_key_bounds`) that
    /// the dense path will actually accept this instance's group-by/
    /// aggregate shape — there is no fallback tier once this is set.
    pub fn with_native_provider(mut self, provider: Arc<dyn TableProvider>) -> Self {
        self.native_provider = Some(provider);
        self
    }
}

#[async_trait]
impl PhysicalOperator for MorselAggregateExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![] // MorselAggregateExec reads directly from Parquet
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        // Morsel aggregate produces a single partition
        crate::physical::check_partition(self, partition)?;

        // Dense direct-address fast path: single bounded int group key with
        // simple aggregates skips hash tables AND the merge.
        if let Some(stream) = self.try_execute_dense_direct()? {
            return Ok(stream);
        }

        // A native-table instance has no generic (hash-table) fallback tier
        // below this point -- that tier is built entirely around
        // `ParallelParquetSource` over `self.files`, which is empty here.
        // The physical planner only ever constructs a native-mode instance
        // after independently confirming the dense-direct path will accept
        // it (see `dense_direct_shape`/`dense_direct_key_bounds` in
        // `src/physical/planner.rs`), so reaching here means that pre-check
        // and this operator's own eligibility check have drifted apart --
        // fail loudly rather than silently aggregating an empty file list.
        if self.native_provider.is_some() {
            return Err(QueryError::Execution(
                "MorselAggregateExec: native-table aggregate did not qualify \
                 for the dense-direct-address path at execution time even \
                 though the planner pre-checked eligibility -- this is a \
                 planner/executor eligibility mismatch, not a data problem"
                    .to_string(),
            ));
        }

        // Dictionary reads for string GROUP columns nothing else touches:
        // dictionaries then flow only into the group-key accessors.
        let mut other_cols: std::collections::HashSet<String> = std::collections::HashSet::new();
        if let Some(f) = &self.filter {
            let mut names = Vec::new();
            crate::physical::morsel::collect_expr_columns(f, &mut names);
            other_cols.extend(names.into_iter().map(|n| n.to_lowercase()));
        }
        for a in &self.aggregates {
            let mut names = Vec::new();
            crate::physical::morsel::collect_expr_columns(&a.input, &mut names);
            other_cols.extend(names.into_iter().map(|n| n.to_lowercase()));
        }
        let mut dict_cols: Vec<usize> = Vec::new();
        let mut all_string_groups = !self.group_by.is_empty();
        for g in &self.group_by {
            match g {
                Expr::Column(c) => {
                    match self
                        .input_schema
                        .fields()
                        .iter()
                        .position(|f| f.name().eq_ignore_ascii_case(&c.name))
                    {
                        Some(i)
                            if self.input_schema.field(i).data_type() == &DataType::Utf8
                                && !other_cols.contains(&c.name.to_lowercase()) =>
                        {
                            dict_cols.push(i)
                        }
                        _ => all_string_groups = false,
                    }
                }
                _ => all_string_groups = false,
            }
        }
        if !all_string_groups {
            dict_cols.clear();
        }

        // Create the parallel Parquet source with row group pruning
        let mut source = ParallelParquetSource::try_new_with_filter(
            self.files.clone(),
            self.input_schema.clone(),
            self.projection.clone(),
            DEFAULT_MORSEL_SIZE,
            self.filter.as_ref(),
        )?
        .with_dict_strings(dict_cols);
        // Drop filter-only columns from the output projection once the
        // decoder has taken the filter (see narrowed_projection).
        self.apply_narrowed_projection(&mut source);

        // Determine input types for aggregates
        let plan_schema =
            crate::planner::PlanSchema::from_qualified_arrow(source.schema().as_ref());
        let input_types: Vec<DataType> = self
            .aggregates
            .iter()
            .map(|a| a.input.data_type(&plan_schema).unwrap_or(DataType::Float64))
            .collect();

        // One worker per row group at most: after pruning, a selective scan can
        // leave three row groups, and 29 extra workers only allocate 29
        // aggregation states that never see a row.
        let num_threads = crate::execution::topology::workers_for(
            source.total_work(),
            rayon::current_num_threads(),
        );

        // Clone expressions for use in parallel closure
        let group_by_exprs = self.group_by.clone();
        let agg_input_exprs: Vec<Expr> = self.aggregates.iter().map(|a| a.input.clone()).collect();
        let agg_funcs: Vec<AggregateFunction> = self.aggregates.iter().map(|a| a.func).collect();
        // When the filter was pushed into the parquet decoder (RowFilter),
        // re-applying it here would be wasted work.
        let filter_expr = if source.filter_pushed_down() {
            None
        } else {
            self.filter.clone()
        };
        let output_schema = self.schema.clone();

        let timing = std::env::var("AGG_TIMING").is_ok();
        let t0 = std::time::Instant::now();
        // Execute in parallel - each thread processes morsels and maintains its own hash table
        let profile = std::env::var("QE_AGG_PROF").is_ok();
        let thread_states: Vec<Result<AggregationState>> = (0..num_threads)
            .into_par_iter()
            .map(|_thread_id| {
                let mut state = AggregationState::new_with_pool(
                    agg_funcs.clone(),
                    input_types.clone(),
                    &self.memory_pool,
                );

                // Keep processing morsels from the source
                while let Some(work) = source.get_work() {
                    let scan_timer = crate::physical::morsel_agg::AggProfileTimer::start(
                        profile,
                        &crate::physical::morsel_agg::AGG_PROF_SCAN_NS,
                    );
                    let batches = source.read_row_group(&work)?;
                    drop(scan_timer);

                    for batch in batches {
                        // Apply filter if present
                        let filtered_batch = if let Some(ref filter) = filter_expr {
                            let filter_result = evaluate_expr(&batch, filter)?;
                            let filter_array = filter_result
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .ok_or_else(|| {
                                    QueryError::Execution("Filter must return boolean".to_string())
                                })?;

                            // Use arrow's filter kernel
                            let filtered_columns: Vec<ArrayRef> = batch
                                .columns()
                                .iter()
                                .map(|col| compute::filter(col.as_ref(), filter_array))
                                .collect::<std::result::Result<Vec<_>, _>>()
                                .map_err(|e| {
                                    QueryError::Execution(format!("Filter failed: {}", e))
                                })?;

                            if filtered_columns.is_empty() || filtered_columns[0].len() == 0 {
                                continue;
                            }

                            RecordBatch::try_new(batch.schema(), filtered_columns).map_err(|e| {
                                QueryError::Execution(format!(
                                    "Failed to create filtered batch: {}",
                                    e
                                ))
                            })?
                        } else {
                            batch
                        };

                        // Process the batch
                        state.process_batch(&filtered_batch, &group_by_exprs, &agg_input_exprs)?;
                    }

                    source.complete_work();
                }

                Ok(state)
            })
            .collect();

        let states: Vec<AggregationState> =
            thread_states.into_iter().collect::<Result<Vec<_>>>()?;
        if timing {
            let groups: usize = states.iter().map(|s| s.group_count()).sum();
            eprintln!(
                "[AGG_TIMING] morsel scan+process: {:?} ({} thread-groups)",
                t0.elapsed(),
                groups
            );
        }
        let t1 = std::time::Instant::now();

        // Shared merge: parallel shard merge above 64K groups (raw-u64 pipeline
        // for single integer group columns), sequential fold below.
        let batches = crate::physical::morsel_agg::merge_states_to_batches_filtered(
            states,
            &agg_funcs,
            &input_types,
            &output_schema,
            self.post_filter.as_ref(),
            &self.memory_pool,
        )?;
        if timing {
            eprintln!(
                "[AGG_TIMING] morsel merge+output+having: {:?}",
                t1.elapsed()
            );
        }
        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn name(&self) -> &str {
        "MorselAggregate"
    }

    fn output_partitions(&self) -> usize {
        // Aggregation produces a single partition (all groups combined)
        1
    }
}

impl fmt::Display for MorselAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let groups: Vec<String> = self.group_by.iter().map(|e| e.to_string()).collect();
        let aggs: Vec<String> = self
            .aggregates
            .iter()
            .map(|a| format!("{}({})", a.func, a.input))
            .collect();
        write!(
            f,
            "MorselAggregate: files={}, group_by=[{}], aggs=[{}]",
            self.files.len(),
            groups.join(", "),
            aggs.join(", ")
        )
    }
}

pub(crate) enum DenseAgg {
    SumF64,
    SumI64,
    Count,
    Avg,
}

/// The reservation is declared last so accumulator buffers are destroyed before
/// their capacity is released. Output arrays copy values out of these buffers;
/// their separate ownership is not covered by this reservation.
struct DenseAccumulators {
    presence: Vec<AtomicU64>,
    acc_f64: Vec<Vec<AtomicU64>>,
    acc_i64: Vec<Vec<AtomicI64>>,
    seen: Vec<Vec<AtomicU64>>,
    _reservation: MemoryReservation,
}

impl DenseAccumulators {
    fn required_bytes(width: usize, kinds: &[(DenseAgg, Option<Expr>)]) -> Result<usize> {
        let arrays = kinds.iter().try_fold(0usize, |total, (kind, _)| {
            total.checked_add(if matches!(kind, DenseAgg::Avg) { 2 } else { 1 })
        });
        let bytes = arrays
            .and_then(|n| n.checked_mul(width))
            .and_then(|n| {
                let bitmaps = 1 + kinds
                    .iter()
                    .filter(|(kind, _)| matches!(kind, DenseAgg::SumF64 | DenseAgg::SumI64))
                    .count();
                width
                    .div_ceil(64)
                    .checked_mul(bitmaps)
                    .and_then(|bits| n.checked_add(bits))
            })
            .and_then(|n| n.checked_mul(std::mem::size_of::<AtomicU64>()))
            .and_then(|n| {
                // Outer vectors contain one inner Vec header per aggregate,
                // including the empty vectors for the other accumulator type.
                kinds
                    .len()
                    .checked_mul(3 * std::mem::size_of::<Vec<AtomicU64>>())
                    .and_then(|headers| n.checked_add(headers))
            });
        bytes.ok_or_else(|| {
            QueryError::Execution(
                "Dense aggregate accumulator allocation size overflow".to_string(),
            )
        })
    }

    fn try_new(
        width: usize,
        kinds: &[(DenseAgg, Option<Expr>)],
        pool: &MemoryPool,
    ) -> Result<Self> {
        let reservation = pool.allocate(Self::required_bytes(width, kinds)?)?;
        // try_reserve_exact prevents geometric capacity growth beyond the
        // charged allocation layout. Initialization cannot grow these vectors.
        let presence = dense_vec(width.div_ceil(64), || AtomicU64::new(0))?;
        let mut acc_f64 = dense_vec(kinds.len(), Vec::new)?;
        let mut acc_i64 = dense_vec(kinds.len(), Vec::new)?;
        let mut seen = dense_vec(kinds.len(), Vec::new)?;
        for (index, (kind, _)) in kinds.iter().enumerate() {
            if matches!(kind, DenseAgg::SumF64 | DenseAgg::SumI64) {
                seen[index] = dense_vec(width.div_ceil(64), || AtomicU64::new(0))?;
            }
            if matches!(kind, DenseAgg::SumF64 | DenseAgg::Avg) {
                acc_f64[index] = dense_vec(width, || AtomicU64::new(0))?;
            }
            if matches!(kind, DenseAgg::SumI64 | DenseAgg::Count | DenseAgg::Avg) {
                acc_i64[index] = dense_vec(width, || AtomicI64::new(0))?;
            }
        }
        // Keep the vectors immutable in shape for the entire execution.
        Ok(Self {
            presence,
            acc_f64,
            acc_i64,
            seen,
            _reservation: reservation,
        })
    }
}

fn dense_vec<T>(len: usize, init: impl FnMut() -> T) -> Result<Vec<T>> {
    let mut result = Vec::new();
    result.try_reserve_exact(len).map_err(|error| {
        QueryError::Execution(format!(
            "Dense aggregate accumulator allocation failed: {error}"
        ))
    })?;
    result.resize_with(len, init);
    Ok(result)
}

/// Pure, source-agnostic eligibility check for the dense-direct-address
/// path: is `group_by` a single plain Int64/Int32/Date32 column, and is
/// every aggregate a plain (non-DISTINCT, no second argument) COUNT/SUM/AVG
/// over a column type this path knows how to accumulate? Neither answer
/// depends on WHERE the data comes from (parquet footers vs. a native
/// table's manifest statistics), which is exactly why this is a free
/// function rather than a `&self` method: the physical planner
/// (`src/physical/planner.rs`) needs to run this SAME check, before it has
/// constructed a `MorselAggregateExec`, to decide whether a native-table
/// aggregate should be routed there at all -- a native-mode instance has no
/// generic fallback tier the way a Parquet-mode one does (see `execute()`),
/// so the planner must be exactly as strict as `try_execute_dense_direct`
/// itself, not an approximation of it. Returns the resolved group-key
/// column name/index and each aggregate's `DenseAgg` classification (plus
/// its input expression, if any), or `None` if this shape can never take
/// the dense-direct path regardless of source.
pub(crate) fn dense_direct_shape(
    group_by: &[Expr],
    aggregates: &[AggregateExpr],
    input_schema: &SchemaRef,
) -> Option<(String, usize, Vec<(DenseAgg, Option<Expr>)>)> {
    if group_by.len() != 1 {
        return None;
    }
    let key_name = match &group_by[0] {
        Expr::Column(c) => c.name.clone(),
        Expr::Alias { expr, .. } => match &**expr {
            Expr::Column(c) => c.name.clone(),
            _ => return None,
        },
        _ => return None,
    };
    let key_idx = input_schema
        .fields()
        .iter()
        .position(|f| f.name().eq_ignore_ascii_case(&key_name))?;
    if !matches!(
        input_schema.field(key_idx).data_type(),
        DataType::Int64 | DataType::Int32 | DataType::Date32
    ) {
        return None;
    }
    // Aggregate kinds: inputs are arbitrary expressions evaluated per
    // batch (Q15 sums l_extendedprice * (1 - l_discount)); the value
    // type comes from the plan schema.
    let plan_schema = crate::planner::PlanSchema::from_qualified_arrow(input_schema.as_ref());
    let mut kinds: Vec<(DenseAgg, Option<Expr>)> = Vec::new();
    for a in aggregates {
        if a.distinct || a.second_arg.is_some() {
            return None;
        }
        match a.func {
            AggregateFunction::Count => match &a.input {
                Expr::Wildcard => kinds.push((DenseAgg::Count, None)),
                e => kinds.push((DenseAgg::Count, Some(e.clone()))),
            },
            AggregateFunction::Sum | AggregateFunction::Avg => {
                let dt = a.input.data_type(&plan_schema).unwrap_or(DataType::Float64);
                let k = match (&a.func, &dt) {
                    (AggregateFunction::Sum, DataType::Float64) => DenseAgg::SumF64,
                    (AggregateFunction::Sum, DataType::Int64) => DenseAgg::SumI64,
                    (AggregateFunction::Avg, DataType::Float64) => DenseAgg::Avg,
                    _ => return None,
                };
                kinds.push((k, Some(a.input.clone())));
            }
            _ => return None,
        }
    }
    Some((key_name, key_idx, kinds))
}

/// `TableStatistics.column_stats[key_name].{min_i64,max_i64}` for
/// `provider` -- the SAME lookup `disjoint_group_hint`
/// (`src/physical/planner.rs`) already uses for its own, differently-tuned
/// dense-key heuristic -- already width-checked against the same
/// <=64,000,000-entry cap `try_execute_dense_direct` applies to its
/// accumulator arrays. `None` when the column has no int zone-map, either
/// bound is missing, or the range would blow that budget.
///
/// Shared by the physical planner (pre-checking a native-table aggregate
/// BEFORE committing to `MorselAggregateExec`'s native mode, which has no
/// generic fallback tier) and `try_execute_dense_direct` itself (the real
/// bounds lookup at execution time) so the two can never disagree.
/// `key_name` must already be lowercased (`TableStatistics.column_stats`'s
/// own key convention).
pub(crate) fn dense_direct_key_bounds(
    provider: &dyn TableProvider,
    key_name_lower: &str,
) -> Option<(i64, i64)> {
    let stats = provider.statistics()?;
    let cs = stats.column_stats.get(key_name_lower)?;
    let (min, max) = (cs.min_i64?, cs.max_i64?);
    if min > max {
        return None;
    }
    bounded_i64_width(min, max, 64_000_000)?;
    Some((min, max))
}

/// Accumulate one batch's rows into the shared dense accumulator arrays.
/// Pure function of its arguments (no `&self`) so it is identical whether
/// `batch` came from a Parquet row group (`ParallelParquetSource`) or a
/// native table's `TableProvider::scan_with_filter` -- extracted from
/// `try_execute_dense_direct`'s own per-batch loop body, unchanged, so the
/// Parquet path's behavior is byte-for-byte what it was before this
/// function existed.
#[allow(clippy::too_many_arguments)]
fn accumulate_dense_batch(
    batch: &RecordBatch,
    key_pos: usize,
    kmin: i64,
    width: usize,
    kinds: &[(DenseAgg, Option<Expr>)],
    acc_i64: &[Vec<AtomicI64>],
    acc_f64: &[Vec<AtomicU64>],
    presence: &[AtomicU64],
    seen: &[Vec<AtomicU64>],
    pool: &MemoryPool,
) -> Result<()> {
    let key_arr = batch.column(key_pos);
    let mut indices = ReservedVec::with_capacity(pool, batch.num_rows())?;
    let index = |row: usize, value: i64| {
        if key_arr.is_null(row) {
            Ok(width)
        } else {
            checked_i64_index(value, kmin, width)
        }
    };
    match key_arr.data_type() {
        DataType::Int64 => {
            let keys = key_arr.as_any().downcast_ref::<Int64Array>().unwrap();
            indices.try_extend_reserved(
                batch.num_rows(),
                keys.values()
                    .iter()
                    .enumerate()
                    .map(|(row, value)| index(row, *value)),
            )?;
        }
        DataType::Int32 => {
            let keys = key_arr.as_any().downcast_ref::<Int32Array>().unwrap();
            indices.try_extend_reserved(
                batch.num_rows(),
                keys.values()
                    .iter()
                    .enumerate()
                    .map(|(row, value)| index(row, *value as i64)),
            )?;
        }
        DataType::Date32 => {
            let keys = key_arr.as_any().downcast_ref::<Date32Array>().unwrap();
            indices.try_extend_reserved(
                batch.num_rows(),
                keys.values()
                    .iter()
                    .enumerate()
                    .map(|(row, value)| index(row, *value as i64)),
            )?;
        }
        _ => {
            return Err(QueryError::Execution(
                "dense agg: unexpected key type".into(),
            ))
        }
    }
    let offsets = indices.as_slice();
    for (ai, (kind, input)) in kinds.iter().enumerate() {
        let arr = match input {
            Some(e) => Some(evaluate_expr(batch, e)?),
            None => None,
        };
        match kind {
            DenseAgg::Count => {
                if let Some(arr) = &arr {
                    for (r, &k) in offsets.iter().enumerate() {
                        if !arr.is_null(r) {
                            acc_i64[ai][k].fetch_add(1, Ordering::Relaxed);
                        }
                    }
                } else {
                    for &k in offsets {
                        acc_i64[ai][k].fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            DenseAgg::SumI64 => {
                let arr = arr.as_ref().unwrap();
                let arr = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .ok_or_else(|| QueryError::Execution("dense agg: expected Int64".into()))?;
                let vals = arr.values();
                let has_nulls = arr.null_count() > 0;
                for (r, &k) in offsets.iter().enumerate() {
                    if has_nulls && arr.is_null(r) {
                        continue;
                    }
                    acc_i64[ai][k].fetch_add(vals[r], Ordering::Relaxed);
                    seen[ai][k >> 6].fetch_or(1u64 << (k & 63), Ordering::Relaxed);
                }
            }
            DenseAgg::SumF64 | DenseAgg::Avg => {
                let arr = arr.as_ref().unwrap();
                let arr = arr
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| QueryError::Execution("dense agg: expected Float64".into()))?;
                let vals = arr.values();
                let has_nulls = arr.null_count() > 0;
                for (r, &k) in offsets.iter().enumerate() {
                    if has_nulls && arr.is_null(r) {
                        continue;
                    }
                    let cell = &acc_f64[ai][k];
                    let mut cur = cell.load(Ordering::Relaxed);
                    loop {
                        let nv = f64::from_bits(cur) + vals[r];
                        match cell.compare_exchange_weak(
                            cur,
                            nv.to_bits(),
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(a) => cur = a,
                        }
                    }
                    if matches!(kind, DenseAgg::Avg) {
                        acc_i64[ai][k].fetch_add(1, Ordering::Relaxed);
                    } else {
                        seen[ai][k >> 6].fetch_or(1u64 << (k & 63), Ordering::Relaxed);
                    }
                }
            }
        }
    }
    for &k in offsets {
        let off = k;
        presence[off >> 6].fetch_or(1u64 << (off & 63), Ordering::Relaxed);
    }
    Ok(())
}

impl MorselAggregateExec {
    /// Columns the aggregation actually consumes: group keys plus aggregate
    /// inputs. The scan projection handed down by the planner also contains
    /// the filter's columns, because the logical scan needs them; but when
    /// the filter is served by the parquet decoder's RowFilter, those
    /// columns have already been decoded in the predicate phase and
    /// re-listing them in the output projection decodes them a SECOND time
    /// (Q01: l_shipdate over 60M rows, a filter that passes 98% of them).
    ///
    /// Returns None when nothing can be dropped, when a referenced column
    /// cannot be resolved, or when the IPC sidecar cache is on (that path
    /// applies the filter post-load and therefore needs its columns).
    fn narrowed_projection(&self) -> Option<Vec<usize>> {
        if crate::storage::ipc_cache::enabled() {
            return None;
        }
        let mut names: Vec<String> = Vec::new();
        for g in &self.group_by {
            crate::physical::morsel::collect_expr_columns(g, &mut names);
        }
        for a in &self.aggregates {
            crate::physical::morsel::collect_expr_columns(&a.input, &mut names);
        }
        let mut idxs: Vec<usize> = Vec::new();
        for n in &names {
            let i = self
                .input_schema
                .fields()
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(n))?;
            if !idxs.contains(&i) {
                idxs.push(i);
            }
        }
        if idxs.is_empty() {
            // COUNT(*) with no grouping: an empty projection would decode no
            // columns at all — leave the projection alone.
            return None;
        }
        idxs.sort_unstable();
        match &self.projection {
            Some(p) => {
                if idxs.len() >= p.len() || !idxs.iter().all(|i| p.contains(i)) {
                    None
                } else {
                    Some(idxs)
                }
            }
            None => Some(idxs),
        }
    }

    /// Narrow `source`'s projection to the columns the aggregation consumes,
    /// but only once the decoder has taken the filter (or there is none).
    /// Returns the projection actually in force.
    fn apply_narrowed_projection(&self, source: &mut ParallelParquetSource) -> Option<Vec<usize>> {
        if self.filter.is_some() && !source.filter_pushed_down() {
            return self.projection.clone();
        }
        match self.narrowed_projection() {
            Some(p) => {
                source.set_projection(Some(p.clone()));
                Some(p)
            }
            None => self.projection.clone(),
        }
    }

    /// Dense direct-address aggregation. Applies when the group key is one
    /// plain Int64/Int32/Date32 column whose min/max span at most 64M
    /// values, and every aggregate is COUNT / SUM / AVG over a plain
    /// null-free column (or COUNT(*)). One shared atomic accumulator array
    /// indexed by key-min replaces hash tables and the merge entirely.
    /// Key bounds come from parquet row-group footers for a Parquet source,
    /// or from `TableProvider::statistics()` for a native-table source
    /// (`self.native_provider`, task 005) — see `dense_direct_key_bounds`.
    /// Returns None to fall back to the generic morsel path for a Parquet
    /// source; for a native-table source there IS no fallback tier, so the
    /// planner never constructs one unless it has already confirmed this
    /// will return `Some` (see `dense_direct_shape`/`dense_direct_key_bounds`
    /// and `execute()`'s native safety-net check).
    fn try_execute_dense_direct(&self) -> Result<Option<RecordBatchStream>> {
        let Some((key_name, key_idx, kinds)) =
            dense_direct_shape(&self.group_by, &self.aggregates, &self.input_schema)
        else {
            return Ok(None);
        };

        // Key bounds: a native-table provider's statistics rollup (task
        // 005), already width-checked by `dense_direct_key_bounds`; or
        // (unchanged) parquet row-group footers, width-checked below.
        let (kmin, kmax): (i64, i64) = if let Some(provider) = &self.native_provider {
            // No pushdown into the IPC segment reader and this path never
            // re-evaluates a filter itself (see `accumulate_dense_batch`'s
            // callers) -- the planner already refuses to route a filtered
            // native scan here (`try_extract_native_dense_source`); this is
            // a defensive second gate, not the primary one.
            if self.filter.is_some() {
                return Ok(None);
            }
            match dense_direct_key_bounds(provider.as_ref(), &key_name.to_lowercase()) {
                Some(b) => b,
                None => return Ok(None),
            }
        } else {
            // Key bounds from parquet footers
            let mut kmin = i64::MAX;
            let mut kmax = i64::MIN;
            for f in &self.files {
                let file = match std::fs::File::open(f) {
                    Ok(f) => f,
                    Err(_) => return Ok(None),
                };
                let reader = match parquet::file::reader::SerializedFileReader::new(file) {
                    Ok(r) => r,
                    Err(_) => return Ok(None),
                };
                use parquet::file::reader::FileReader;
                let meta = reader.metadata();
                for rg in meta.row_groups() {
                    let col = rg.column(key_idx);
                    let Some(stats) = col.statistics() else {
                        return Ok(None);
                    };
                    use parquet::file::statistics::Statistics;
                    let (lo, hi) = match stats {
                        Statistics::Int64(s) => match (s.min_opt(), s.max_opt()) {
                            (Some(a), Some(b)) => (*a, *b),
                            _ => return Ok(None),
                        },
                        Statistics::Int32(s) => match (s.min_opt(), s.max_opt()) {
                            (Some(a), Some(b)) => (*a as i64, *b as i64),
                            _ => return Ok(None),
                        },
                        _ => return Ok(None),
                    };
                    kmin = kmin.min(lo);
                    kmax = kmax.max(hi);
                }
            }
            if kmin > kmax {
                return Ok(None);
            }
            (kmin, kmax)
        };
        let Some(width) = bounded_i64_width(kmin, kmax, 64_000_000) else {
            return Ok(None);
        };
        let slots = width
            .checked_add(1)
            .ok_or_else(|| QueryError::Execution("dense aggregate null slot overflow".into()))?;

        // Admit the fixed-capacity accumulator storage before allocating it.
        // A selected dense path refuses by name on denial; it must not silently
        // move the same work to an unbudgeted representation.
        let dense_pool = MemoryPool::new_child(
            &self.memory_pool,
            "dense aggregate accumulators",
            self.memory_pool.max(),
        );
        let storage = DenseAccumulators::try_new(slots, &kinds, &dense_pool)?;
        let presence = &storage.presence;
        let acc_f64 = &storage.acc_f64;
        let acc_i64 = &storage.acc_i64;

        // Column positions AFTER projection -- shared by both source kinds.
        let proj_pos = |eff: &Option<Vec<usize>>, idx: usize| -> Option<usize> {
            match eff {
                Some(p) => p.iter().position(|&i| i == idx),
                None => Some(idx),
            }
        };

        let timing = std::env::var("AGG_TIMING").is_ok();
        let t0 = std::time::Instant::now();

        if let Some(provider) = &self.native_provider {
            // Native table (task 005): no row-group work-stealing queue to
            // drive -- `TableProvider::scan_with_filter` already reads
            // every segment (`NativeTable::scan` loops its manifest's
            // segments through `ipc_cache::read_row_group`, the same
            // mmap-backed reader the Parquet IPC sidecar cache uses).
            // Accumulation is still parallelized, over the resulting
            // batches, by the SAME per-batch routine the Parquet branch
            // below uses.
            let eff_projection = self
                .narrowed_projection()
                .or_else(|| self.projection.clone());
            let Some(key_pos) = proj_pos(&eff_projection, key_idx) else {
                return Ok(None);
            };
            let batches = provider.scan_with_filter(eff_projection.as_deref(), None)?;
            let n_batches = batches.len();
            let results: Vec<Result<()>> = batches
                .par_iter()
                .map(|batch| {
                    accumulate_dense_batch(
                        batch,
                        key_pos,
                        kmin,
                        width,
                        &kinds,
                        &acc_i64,
                        &acc_f64,
                        &presence,
                        &storage.seen,
                        &dense_pool,
                    )
                })
                .collect();
            for r in results {
                r?;
            }
            if timing {
                eprintln!(
                    "[AGG_TIMING] dense-direct scan+accumulate (native): {:?} (key={}, width={}, aggs={}, batches={}, projection={:?})",
                    t0.elapsed(),
                    key_name,
                    width,
                    kinds.len(),
                    n_batches,
                    eff_projection,
                );
            }
        } else {
            let mut source = ParallelParquetSource::try_new_with_filter(
                self.files.clone(),
                self.input_schema.clone(),
                self.projection.clone(),
                DEFAULT_MORSEL_SIZE,
                self.filter.as_ref(),
            )?;
            // A filter is only safe here when the parquet decoder applies it
            // fully (RowFilter pushdown) — this path never re-evaluates it.
            if self.filter.is_some() && !source.filter_pushed_down() {
                return Ok(None);
            }
            // Filter-only columns were already decoded by the RowFilter; do not
            // decode them again for the output.
            let eff_projection = self.apply_narrowed_projection(&mut source);
            let Some(key_pos) = proj_pos(&eff_projection, key_idx) else {
                return Ok(None);
            };

            let num_threads = crate::execution::topology::workers_for(
                source.total_work(),
                rayon::current_num_threads(),
            );
            let results: Vec<Result<()>> = (0..num_threads)
                .into_par_iter()
                .map(|_| {
                    while let Some(work) = source.get_work() {
                        let batches = source.read_row_group(&work)?;
                        for batch in batches {
                            accumulate_dense_batch(
                                &batch,
                                key_pos,
                                kmin,
                                width,
                                &kinds,
                                &acc_i64,
                                &acc_f64,
                                &presence,
                                &storage.seen,
                                &dense_pool,
                            )?;
                        }
                        source.complete_work();
                    }
                    Ok(())
                })
                .collect();
            for r in results {
                r?;
            }
            if timing {
                eprintln!(
                    "[AGG_TIMING] dense-direct scan+accumulate: {:?} (key={}, width={}, aggs={}, files={}, threads={}, projection={:?}, filter_pushed={})",
                    t0.elapsed(),
                    key_name,
                    width,
                    kinds.len(),
                    self.files.len(),
                    num_threads,
                    eff_projection,
                    source.filter_pushed_down()
                );
            }
        }
        let t1 = std::time::Instant::now();

        // Parallel bitmap walk -> output batches per chunk of the key space
        let key_dt = self.schema.field(0).data_type().clone();
        let out_dt: Vec<DataType> = (1..self.schema.fields().len())
            .map(|i| self.schema.field(i).data_type().clone())
            .collect();
        const WORDS_PER_CHUNK: usize = 16_384;
        let n_chunks = presence.len().div_ceil(WORDS_PER_CHUNK);
        let mut batches: Vec<RecordBatch> = (0..n_chunks)
            .into_par_iter()
            .map(|ci| -> Result<Option<RecordBatch>> {
                let w0 = ci * WORDS_PER_CHUNK;
                let w1 = (w0 + WORDS_PER_CHUNK).min(presence.len());
                let mut keys: Vec<usize> = Vec::new();
                for w in w0..w1 {
                    let mut bits = presence[w].load(Ordering::Relaxed);
                    while bits != 0 {
                        let b = bits.trailing_zeros() as usize;
                        keys.push((w << 6) + b);
                        bits &= bits - 1;
                    }
                }
                if keys.is_empty() {
                    return Ok(None);
                }
                let mut arrays: Vec<ArrayRef> = Vec::with_capacity(1 + kinds.len());
                let key_array: ArrayRef = match key_dt {
                    DataType::Int32 => Arc::new(arrow::array::Int32Array::from_iter(
                        keys.iter()
                            .map(|&slot| (slot != width).then(|| (kmin + slot as i64) as i32)),
                    )),
                    DataType::Date32 => Arc::new(arrow::array::Date32Array::from_iter(
                        keys.iter()
                            .map(|&slot| (slot != width).then(|| (kmin + slot as i64) as i32)),
                    )),
                    _ => Arc::new(Int64Array::from_iter(
                        keys.iter()
                            .map(|&slot| (slot != width).then(|| kmin + slot as i64)),
                    )),
                };
                arrays.push(key_array);
                for (ai, (kind, _)) in kinds.iter().enumerate() {
                    let arr: ArrayRef = match kind {
                        DenseAgg::Count => Arc::new(Int64Array::from_iter_values(
                            keys.iter().map(|&k| acc_i64[ai][k].load(Ordering::Relaxed)),
                        )),
                        DenseAgg::SumI64 => {
                            let it = keys.iter().map(|&slot| {
                                let seen = storage.seen[ai][slot >> 6].load(Ordering::Relaxed)
                                    & (1u64 << (slot & 63))
                                    != 0;
                                seen.then(|| acc_i64[ai][slot].load(Ordering::Relaxed))
                            });
                            match &out_dt[ai] {
                                DataType::Float64 => Arc::new(Float64Array::from_iter(
                                    it.map(|v| v.map(|v| v as f64)),
                                )),
                                _ => Arc::new(Int64Array::from_iter(it)),
                            }
                        }
                        DenseAgg::SumF64 => {
                            Arc::new(Float64Array::from_iter(keys.iter().map(|&slot| {
                                let seen = storage.seen[ai][slot >> 6].load(Ordering::Relaxed)
                                    & (1u64 << (slot & 63))
                                    != 0;
                                seen.then(|| {
                                    f64::from_bits(acc_f64[ai][slot].load(Ordering::Relaxed))
                                })
                            })))
                        }
                        DenseAgg::Avg => {
                            Arc::new(Float64Array::from_iter(keys.iter().map(|&slot| {
                                let count = acc_i64[ai][slot].load(Ordering::Relaxed);
                                (count > 0).then(|| {
                                    f64::from_bits(acc_f64[ai][slot].load(Ordering::Relaxed))
                                        / count as f64
                                })
                            })))
                        }
                    };
                    arrays.push(arr);
                }
                Ok(Some(RecordBatch::try_new(self.schema.clone(), arrays)?))
            })
            .collect::<Result<Vec<Option<RecordBatch>>>>()?
            .into_iter()
            .flatten()
            .collect();

        if let Some(pred) = &self.post_filter {
            batches = crate::physical::operators::filter_batches(batches, pred)?;
        }
        if timing {
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            eprintln!(
                "[AGG_TIMING] dense-direct output: {:?} ({} rows)",
                t1.elapsed(),
                rows
            );
        }
        Ok(Some(Box::pin(stream::iter(batches.into_iter().map(Ok)))))
    }
}

#[cfg(test)]
mod dense_memory_tests {
    use super::*;
    use crate::physical::operators::{ColumnStatistics, TableStatistics};
    use arrow::datatypes::{Field, Schema};
    use futures::TryStreamExt;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn dense_layout_is_admitted_before_allocation_and_released_after_drop() {
        let kinds = vec![(DenseAgg::Avg, None), (DenseAgg::Count, None)];
        let bytes = DenseAccumulators::required_bytes(100, &kinds).unwrap();
        assert_eq!(bytes, 3 * 100 * 8 + 2 * 8 + 6 * 24);
        let pool = MemoryPool::new(bytes - 1);
        assert!(DenseAccumulators::try_new(100, &kinds, &pool).is_err());
        assert_eq!(pool.used(), 0);
        assert_eq!(pool.reserved_peak(), 0);
        let pool = MemoryPool::new(bytes);
        let storage = DenseAccumulators::try_new(100, &kinds, &pool).unwrap();
        let actual = storage.presence.capacity() * std::mem::size_of::<AtomicU64>()
            + storage.acc_f64.capacity() * std::mem::size_of::<Vec<AtomicU64>>()
            + storage.acc_i64.capacity() * std::mem::size_of::<Vec<AtomicI64>>()
            + storage.seen.capacity() * std::mem::size_of::<Vec<AtomicU64>>()
            + storage
                .seen
                .iter()
                .map(|v| v.capacity() * std::mem::size_of::<AtomicU64>())
                .sum::<usize>()
            + storage
                .acc_f64
                .iter()
                .map(|v| v.capacity() * std::mem::size_of::<AtomicU64>())
                .sum::<usize>()
            + storage
                .acc_i64
                .iter()
                .map(|v| v.capacity() * std::mem::size_of::<AtomicI64>())
                .sum::<usize>();
        assert_eq!(pool.used(), actual);
        assert!(storage.acc_i64[1]
            .iter()
            .all(|a| a.load(Ordering::Relaxed) == 0));
        drop(storage);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn dense_domains_share_parent_admission() {
        let kinds = vec![(DenseAgg::Count, None)];
        let bytes = DenseAccumulators::required_bytes(100, &kinds).unwrap();
        let parent = MemoryPool::new(bytes);
        let first = MemoryPool::new_child(&parent, "first dense aggregate", bytes);
        let second = MemoryPool::new_child(&parent, "second dense aggregate", bytes);
        let storage = DenseAccumulators::try_new(100, &kinds, &first).unwrap();
        assert!(DenseAccumulators::try_new(100, &kinds, &second).is_err());
        assert_eq!(second.used(), 0);
        drop(storage);
        let storage = DenseAccumulators::try_new(100, &kinds, &second).unwrap();
        assert_eq!(parent.used(), bytes);
        drop(storage);
        assert_eq!(parent.used(), 0);
    }

    #[tokio::test]
    async fn dense_owner_releases_when_cancelled() {
        let pool = Arc::new(MemoryPool::new(4096));
        let task_pool = Arc::clone(&pool);
        let (tx, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _storage =
                DenseAccumulators::try_new(100, &[(DenseAgg::Count, None)], &task_pool).unwrap();
            tx.send(()).unwrap();
            std::future::pending::<()>().await;
        });
        rx.await.unwrap();
        assert!(pool.used() > 0);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(pool.used(), 0);
    }

    #[derive(Debug)]
    struct ObservedProvider {
        batch: RecordBatch,
        pool: SharedMemoryPool,
        scans: AtomicUsize,
        fail_scan: bool,
    }

    impl TableProvider for ObservedProvider {
        fn schema(&self) -> SchemaRef {
            self.batch.schema()
        }
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }
        fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
            self.scans.fetch_add(1, Ordering::SeqCst);
            assert!(
                self.pool.used() > 0,
                "dense storage must stay admitted throughout scan"
            );
            if self.fail_scan {
                return Err(QueryError::Execution("injected scan failure".to_string()));
            }
            Ok(vec![match projection {
                Some(indices) => self.batch.project(indices)?,
                None => self.batch.clone(),
            }])
        }
        fn statistics(&self) -> Option<TableStatistics> {
            Some(TableStatistics {
                row_count: 3,
                total_byte_size: 24,
                column_stats: [(
                    "k".to_string(),
                    ColumnStatistics {
                        min_i64: Some(0),
                        max_i64: Some(99),
                        null_count: Some(0),
                        ..Default::default()
                    },
                )]
                .into_iter()
                .collect(),
            })
        }
    }

    fn observed_operator(
        limit: usize,
        fail_scan: bool,
    ) -> (MorselAggregateExec, Arc<ObservedProvider>) {
        let pool = Arc::new(MemoryPool::new(limit));
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![0, 99, 99]))],
        )
        .unwrap();
        let provider = Arc::new(ObservedProvider {
            batch,
            pool: Arc::clone(&pool),
            scans: AtomicUsize::new(0),
            fail_scan,
        });
        let output = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("count", DataType::Int64, false),
        ]));
        let op = MorselAggregateExec::new(
            Vec::new(),
            schema,
            None,
            None,
            vec![Expr::column("k")],
            vec![AggregateExpr {
                func: AggregateFunction::Count,
                input: Expr::Wildcard,
                distinct: false,
                second_arg: None,
            }],
            output,
        )
        .with_native_provider(provider.clone())
        .with_memory_pool(pool);
        (op, provider)
    }

    #[tokio::test]
    async fn selected_dense_path_refuses_before_scan_and_releases_on_error() {
        let (operator, provider) = observed_operator(1, false);
        let error = match operator.execute(0).await {
            Err(error) => error,
            Ok(_) => panic!("expected dense admission denial"),
        };
        assert!(error.to_string().contains("dense aggregate accumulators"));
        assert_eq!(provider.scans.load(Ordering::Relaxed), 0);
        assert_eq!(provider.pool.used(), 0);
        let (operator, provider) = observed_operator(4096, true);
        assert!(operator.execute(0).await.is_err());
        assert_eq!(provider.scans.load(Ordering::Relaxed), 1);
        assert_eq!(provider.pool.used(), 0);
    }

    #[tokio::test]
    async fn dense_execution_holds_admission_through_accumulation_and_preserves_values() {
        let (operator, provider) = observed_operator(4096, false);
        let batches: Vec<RecordBatch> = operator
            .execute(0)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut actual = Vec::new();
        for batch in batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            actual.extend((0..batch.num_rows()).map(|row| (keys.value(row), counts.value(row))));
        }
        assert_eq!(actual, vec![(0, 1), (99, 2)]);
        assert_eq!(provider.scans.load(Ordering::Relaxed), 1);
        assert!(provider.pool.reserved_peak() > 0);
        assert_eq!(provider.pool.used(), 0);
    }

    #[tokio::test]
    async fn sql_and_planning_time_cte_use_query_admission_domain() {
        use crate::ExecutionContext;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("dense.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![0, 99, 99]))],
        )
        .unwrap();
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = parquet::arrow::ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let mut denied = ExecutionContext::with_memory_limit(1);
        denied.register_parquet("dense", &path).unwrap();
        let error = denied
            .sql("SELECT k, COUNT(*) FROM dense GROUP BY k")
            .await
            .unwrap_err();
        assert!(error.to_string().contains("dense aggregate accumulators"));
        assert_eq!(denied.memory_used(), 0);

        let mut admitted = ExecutionContext::with_memory_limit(4096);
        admitted.register_parquet("dense", &path).unwrap();
        let query =
            "WITH c AS (SELECT k, COUNT(*) AS n FROM dense GROUP BY k) SELECT SUM(n) FROM c";
        let result = admitted.sql(query).await.unwrap();
        assert!(result.metrics.reserved_peak_memory_bytes > 0);
        assert!(result.metrics.reserved_peak_memory_bytes <= 4096);
        assert_eq!(admitted.memory_used(), 0);
        assert_eq!(result.row_count, 1);
        assert_eq!(
            result.batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            3
        );
    }
}
