//! Morsel-driven aggregate operator
//!
//! This operator implements parallel aggregation using morsel-driven parallelism
//! for Parquet data sources. It provides significant performance improvements over
//! the standard HashAggregateExec by:
//! - Reading Parquet files in parallel across row groups
//! - Using thread-local hash tables to avoid contention
//! - Final merge of all thread-local states

use crate::error::{QueryError, Result};
use crate::physical::morsel::{ParallelParquetSource, DEFAULT_MORSEL_SIZE};
use crate::physical::morsel_agg::AggregationState;
use crate::physical::operators::evaluate_expr;
use crate::physical::operators::hash_agg::AggregateExpr;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::{AggregateFunction, Expr};
use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;
use rayon::prelude::*;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

/// Morsel-driven aggregate execution operator
///
/// This operator is used for aggregations over Parquet data sources.
/// It uses parallel row-group reading and thread-local hash tables
/// for optimal performance.
pub struct MorselAggregateExec {
    /// Parquet file paths to read from
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
}

impl fmt::Debug for MorselAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MorselAggregateExec")
            .field("files", &self.files.len())
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
        }
    }

    /// Attach a HAVING predicate applied to output batches before returning.
    pub fn with_post_filter(mut self, post_filter: Option<Expr>) -> Self {
        self.post_filter = post_filter;
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
        if partition != 0 {
            return Ok(Box::pin(stream::empty()));
        }

        // Dense direct-address fast path: single bounded int group key with
        // simple aggregates skips hash tables AND the merge.
        if let Some(stream) = self.try_execute_dense_direct()? {
            return Ok(stream);
        }

        // Create the parallel Parquet source with row group pruning
        let source = ParallelParquetSource::try_new_with_filter(
            self.files.clone(),
            self.input_schema.clone(),
            self.projection.clone(),
            DEFAULT_MORSEL_SIZE,
            self.filter.as_ref(),
        )?;

        // Determine input types for aggregates
        let plan_schema = crate::planner::PlanSchema::from(source.schema().as_ref());
        let input_types: Vec<DataType> = self
            .aggregates
            .iter()
            .map(|a| a.input.data_type(&plan_schema).unwrap_or(DataType::Float64))
            .collect();

        let num_threads = rayon::current_num_threads();

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
        let thread_states: Vec<Result<AggregationState>> = (0..num_threads)
            .into_par_iter()
            .map(|_thread_id| {
                let mut state = AggregationState::new(agg_funcs.clone(), input_types.clone());

                // Keep processing morsels from the source
                while let Some(work) = source.get_work() {
                    let batches = source.read_row_group(&work)?;

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

enum DenseAgg {
    SumF64,
    SumI64,
    Count,
    Avg,
}

impl MorselAggregateExec {
    /// Dense direct-address aggregation. Applies when the group key is one
    /// plain Int64/Int32/Date32 column whose footer min/max span at most
    /// 64M values, and every aggregate is COUNT / SUM / AVG over a plain
    /// null-free column (or COUNT(*)). One shared atomic accumulator array
    /// indexed by key-min replaces hash tables and the merge entirely.
    /// Returns None to fall back to the generic morsel path.
    fn try_execute_dense_direct(&self) -> Result<Option<RecordBatchStream>> {
        use arrow::array::{Date32Array, Float64Array, Int32Array, Int64Array};
        use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

        if self.group_by.len() != 1 || self.filter.is_some() {
            return Ok(None);
        }
        let key_name = match &self.group_by[0] {
            Expr::Column(c) => c.name.clone(),
            Expr::Alias { expr, .. } => match &**expr {
                Expr::Column(c) => c.name.clone(),
                _ => return Ok(None),
            },
            _ => return Ok(None),
        };
        let key_idx = match self
            .input_schema
            .fields()
            .iter()
            .position(|f| f.name().eq_ignore_ascii_case(&key_name))
        {
            Some(i) => i,
            None => return Ok(None),
        };
        if !matches!(
            self.input_schema.field(key_idx).data_type(),
            DataType::Int64 | DataType::Int32 | DataType::Date32
        ) {
            return Ok(None);
        }
        // Aggregate kinds
        let mut kinds: Vec<(DenseAgg, Option<usize>)> = Vec::new();
        for a in &self.aggregates {
            if a.distinct || a.second_arg.is_some() {
                return Ok(None);
            }
            let col_idx = |e: &Expr| -> Option<usize> {
                match e {
                    Expr::Column(c) => self
                        .input_schema
                        .fields()
                        .iter()
                        .position(|f| f.name().eq_ignore_ascii_case(&c.name)),
                    _ => None,
                }
            };
            match a.func {
                AggregateFunction::Count => match &a.input {
                    Expr::Wildcard => kinds.push((DenseAgg::Count, None)),
                    e => match col_idx(e) {
                        Some(i) => kinds.push((DenseAgg::Count, Some(i))),
                        None => return Ok(None),
                    },
                },
                AggregateFunction::Sum | AggregateFunction::Avg => {
                    let Some(i) = col_idx(&a.input) else {
                        return Ok(None);
                    };
                    let k = match (&a.func, self.input_schema.field(i).data_type()) {
                        (AggregateFunction::Sum, DataType::Float64) => DenseAgg::SumF64,
                        (AggregateFunction::Sum, DataType::Int64) => DenseAgg::SumI64,
                        (AggregateFunction::Avg, DataType::Float64) => DenseAgg::Avg,
                        _ => return Ok(None),
                    };
                    kinds.push((k, Some(i)));
                }
                _ => return Ok(None),
            }
        }

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
        let width_u = (kmax as i128 - kmin as i128 + 1) as u64;
        if width_u > 64_000_000 {
            return Ok(None);
        }
        let width = width_u as usize;

        // Shared atomic accumulator arrays (zeroed lazily by the allocator)
        let presence: Vec<AtomicU64> = (0..width.div_ceil(64)).map(|_| AtomicU64::new(0)).collect();
        let acc_f64: Vec<Vec<AtomicU64>> = kinds
            .iter()
            .map(|(k, _)| match k {
                DenseAgg::SumF64 | DenseAgg::Avg => (0..width).map(|_| AtomicU64::new(0)).collect(),
                _ => Vec::new(),
            })
            .collect();
        let acc_i64: Vec<Vec<AtomicI64>> = kinds
            .iter()
            .map(|(k, _)| match k {
                DenseAgg::SumI64 | DenseAgg::Count | DenseAgg::Avg => {
                    (0..width).map(|_| AtomicI64::new(0)).collect()
                }
                _ => Vec::new(),
            })
            .collect();

        let source = ParallelParquetSource::try_new_with_filter(
            self.files.clone(),
            self.input_schema.clone(),
            self.projection.clone(),
            DEFAULT_MORSEL_SIZE,
            None,
        )?;
        // Column positions AFTER projection
        let proj_pos = |file_idx: usize| -> Option<usize> {
            match &self.projection {
                Some(p) => p.iter().position(|&i| i == file_idx),
                None => Some(file_idx),
            }
        };
        let Some(key_pos) = proj_pos(key_idx) else {
            return Ok(None);
        };
        let mut agg_pos: Vec<Option<usize>> = Vec::new();
        for (_, idx) in &kinds {
            match idx {
                Some(i) => match proj_pos(*i) {
                    Some(p) => agg_pos.push(Some(p)),
                    None => return Ok(None),
                },
                None => agg_pos.push(None),
            }
        }

        let num_threads = rayon::current_num_threads();
        let results: Vec<Result<()>> = (0..num_threads)
            .into_par_iter()
            .map(|_| {
                while let Some(work) = source.get_work() {
                    let batches = source.read_row_group(&work)?;
                    for batch in batches {
                        let key_arr = batch.column(key_pos);
                        if key_arr.null_count() > 0 {
                            return Err(QueryError::Execution(
                                "dense agg: null group keys unsupported".into(),
                            ));
                        }
                        let keys_i64: Vec<i64> = match key_arr.data_type() {
                            DataType::Int64 => key_arr
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .values()
                                .to_vec(),
                            DataType::Int32 => key_arr
                                .as_any()
                                .downcast_ref::<Int32Array>()
                                .unwrap()
                                .values()
                                .iter()
                                .map(|&v| v as i64)
                                .collect(),
                            DataType::Date32 => key_arr
                                .as_any()
                                .downcast_ref::<Date32Array>()
                                .unwrap()
                                .values()
                                .iter()
                                .map(|&v| v as i64)
                                .collect(),
                            _ => {
                                return Err(QueryError::Execution(
                                    "dense agg: unexpected key type".into(),
                                ))
                            }
                        };
                        for (ai, ((kind, _), pos)) in kinds.iter().zip(agg_pos.iter()).enumerate() {
                            match kind {
                                DenseAgg::Count => {
                                    if let Some(p) = pos {
                                        let arr = batch.column(*p);
                                        for (r, &k) in keys_i64.iter().enumerate() {
                                            if !arr.is_null(r) {
                                                acc_i64[ai][(k - kmin) as usize]
                                                    .fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                    } else {
                                        for &k in &keys_i64 {
                                            acc_i64[ai][(k - kmin) as usize]
                                                .fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                                DenseAgg::SumI64 => {
                                    let arr = batch
                                        .column(pos.unwrap())
                                        .as_any()
                                        .downcast_ref::<Int64Array>()
                                        .unwrap();
                                    if arr.null_count() > 0 {
                                        return Err(QueryError::Execution(
                                            "dense agg: null sum input".into(),
                                        ));
                                    }
                                    let vals = arr.values();
                                    for (r, &k) in keys_i64.iter().enumerate() {
                                        acc_i64[ai][(k - kmin) as usize]
                                            .fetch_add(vals[r], Ordering::Relaxed);
                                    }
                                }
                                DenseAgg::SumF64 | DenseAgg::Avg => {
                                    let arr = batch
                                        .column(pos.unwrap())
                                        .as_any()
                                        .downcast_ref::<Float64Array>()
                                        .unwrap();
                                    if arr.null_count() > 0 {
                                        return Err(QueryError::Execution(
                                            "dense agg: null sum input".into(),
                                        ));
                                    }
                                    let vals = arr.values();
                                    for (r, &k) in keys_i64.iter().enumerate() {
                                        let cell = &acc_f64[ai][(k - kmin) as usize];
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
                                            acc_i64[ai][(k - kmin) as usize]
                                                .fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                        }
                        for &k in &keys_i64 {
                            let off = (k - kmin) as usize;
                            presence[off >> 6].fetch_or(1u64 << (off & 63), Ordering::Relaxed);
                        }
                    }
                    source.complete_work();
                }
                Ok(())
            })
            .collect();
        for r in results {
            r?;
        }

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
                let mut keys: Vec<i64> = Vec::new();
                for w in w0..w1 {
                    let mut bits = presence[w].load(Ordering::Relaxed);
                    while bits != 0 {
                        let b = bits.trailing_zeros() as usize;
                        keys.push(kmin + ((w << 6) + b) as i64);
                        bits &= bits - 1;
                    }
                }
                if keys.is_empty() {
                    return Ok(None);
                }
                let mut arrays: Vec<ArrayRef> = Vec::with_capacity(1 + kinds.len());
                let key_array: ArrayRef = match key_dt {
                    DataType::Int32 => Arc::new(arrow::array::Int32Array::from_iter_values(
                        keys.iter().map(|&k| k as i32),
                    )),
                    DataType::Date32 => Arc::new(arrow::array::Date32Array::from_iter_values(
                        keys.iter().map(|&k| k as i32),
                    )),
                    _ => Arc::new(Int64Array::from_iter_values(keys.iter().copied())),
                };
                arrays.push(key_array);
                for (ai, (kind, _)) in kinds.iter().enumerate() {
                    let arr: ArrayRef = match kind {
                        DenseAgg::Count => {
                            Arc::new(Int64Array::from_iter_values(keys.iter().map(|&k| {
                                acc_i64[ai][(k - kmin) as usize].load(Ordering::Relaxed)
                            })))
                        }
                        DenseAgg::SumI64 => {
                            let it = keys
                                .iter()
                                .map(|&k| acc_i64[ai][(k - kmin) as usize].load(Ordering::Relaxed));
                            match &out_dt[ai] {
                                DataType::Float64 => {
                                    Arc::new(arrow::array::Float64Array::from_iter_values(
                                        it.map(|v| v as f64),
                                    ))
                                }
                                _ => Arc::new(Int64Array::from_iter_values(it)),
                            }
                        }
                        DenseAgg::SumF64 => Arc::new(arrow::array::Float64Array::from_iter_values(
                            keys.iter().map(|&k| {
                                f64::from_bits(
                                    acc_f64[ai][(k - kmin) as usize].load(Ordering::Relaxed),
                                )
                            }),
                        )),
                        DenseAgg::Avg => Arc::new(arrow::array::Float64Array::from_iter_values(
                            keys.iter().map(|&k| {
                                let off = (k - kmin) as usize;
                                let s = f64::from_bits(acc_f64[ai][off].load(Ordering::Relaxed));
                                let c = acc_i64[ai][off].load(Ordering::Relaxed);
                                s / c as f64
                            }),
                        )),
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
        Ok(Some(Box::pin(stream::iter(batches.into_iter().map(Ok)))))
    }
}
