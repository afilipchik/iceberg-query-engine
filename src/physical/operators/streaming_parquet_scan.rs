//! Streaming Parquet scan operator
//!
//! Reads Parquet files row-group by row-group instead of materializing
//! entire tables into memory. Supports row group pruning via filter predicates.

use crate::error::{QueryError, Result};
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::Expr;
use crate::storage::row_group_pruning;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fmt;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

/// A work unit: one row group from one file
#[derive(Debug, Clone)]
struct RowGroupWork {
    file_path: PathBuf,
    row_group_idx: usize,
}

/// Streaming Parquet scan operator that reads row groups on-demand.
///
/// Unlike `MemoryTableExec` which materializes all data before processing,
/// this operator lazily reads one row group at a time, keeping memory usage
/// bounded by `batch_size * num_partitions`.
/// Runtime join-key filter payload: a bitmap over a bounded key range when
/// the build keys span a small domain (one AND per probe test, L2-resident),
/// otherwise a hash set. A 222K-key HashSet probed 60M times cost as much as
/// the decode it saved; the bitmap version is ~30x cheaper per test.
#[derive(Debug)]
pub enum RuntimeFilterPayload {
    Bitmap { min: i64, bits: Vec<u64> },
    Set(hashbrown::HashSet<i64>),
}

impl RuntimeFilterPayload {
    #[inline]
    pub fn contains(&self, v: i64) -> bool {
        match self {
            RuntimeFilterPayload::Bitmap { min, bits } => {
                let off = v.wrapping_sub(*min);
                if off < 0 {
                    return false;
                }
                let off = off as usize;
                match bits.get(off >> 6) {
                    Some(w) => (w >> (off & 63)) & 1 == 1,
                    None => false,
                }
            }
            RuntimeFilterPayload::Set(set) => set.contains(&v),
        }
    }
}

/// Runtime join-key filter slot: a hash join publishes its build-side key set
/// here after building; the scan then decodes only matching rows.
pub type SharedRuntimeFilter =
    std::sync::Arc<parking_lot::Mutex<Option<std::sync::Arc<RuntimeFilterPayload>>>>;

/// Plan-time configuration handle: the planner links a join to a scan by
/// pushing (file column index, filter slot) here. A `Vec` rather than a
/// single slot: a probe-side leaf touched by more than one independently
/// eligible join (directly, or transitively through an already-linked join
/// above it -- see `PhysicalPlanner`'s `streaming_scans` registry) publishes
/// one entry per linking join, each on its own build side's schedule. Every
/// populated entry is applied as an independent, AND-combined predicate at
/// read time (parquet `RowFilter` ANDs its predicates in sequence; the IPC
/// sidecar path applies them as a fold) -- so a leaf that has already been
/// filtered by one join can only ever narrow further, never contradict.
pub type RuntimeFilterConfig =
    std::sync::Arc<parking_lot::Mutex<Vec<(usize, SharedRuntimeFilter)>>>;

pub struct StreamingParquetScanExec {
    table_name: String,
    /// Logical schema with proper qualified column names
    schema: SchemaRef,
    /// Projection column indices
    projection: Option<Vec<usize>>,
    /// Row groups to read, distributed across partitions
    partitioned_work: Vec<Vec<RowGroupWork>>,
    /// Batch size for reading
    batch_size: usize,
    /// Runtime filter configuration (written by the planner when a join links)
    runtime_filter: RuntimeFilterConfig,
    /// Static predicate applied at the decoder (expr, provider column indices)
    filter_spec: Option<(Expr, Vec<usize>)>,
    /// Schema override coercing dict-safe filter string columns
    dict_filter_schema: Option<SchemaRef>,
    /// Post-projection column positions to cast back to Utf8 on emission
    coerce_back: Vec<usize>,
    /// IPC sidecar dir per file (QE_IPC_CACHE=1 and the sidecar built).
    /// A file that has one is read decode-free; filters that the parquet
    /// path pushes into the decoder apply vectorized post-load instead.
    ipc_dirs: std::collections::HashMap<PathBuf, PathBuf>,
}

impl fmt::Debug for StreamingParquetScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingParquetScanExec")
            .field("table_name", &self.table_name)
            .field("partitions", &self.partitioned_work.len())
            .field(
                "total_row_groups",
                &self.partitioned_work.iter().map(|p| p.len()).sum::<usize>(),
            )
            .finish()
    }
}

impl StreamingParquetScanExec {
    /// Create a new streaming Parquet scan.
    ///
    /// Reads file footers, applies row group pruning based on the filter,
    /// and distributes work items across partitions.
    pub fn try_new(
        table_name: impl Into<String>,
        files: &[PathBuf],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filter: Option<&Expr>,
        provider_schema: &SchemaRef,
    ) -> Result<Self> {
        let table_name = table_name.into();
        let batch_size = 8_192;

        // Dictionary coercion for filter string columns: when every
        // reference to a Utf8 column in the predicate is dictionary-safe
        // (=, <>, LIKE, IN over literals), read it as Dictionary(Int32,Utf8)
        // — the RowFilter then evaluates over the values array and never
        // expands 60M shipmode strings (filter.rs has the dict fast paths).
        let dict_filter_schema: Option<SchemaRef> = filter.and_then(|expr| {
            let mut safe: Vec<String> = Vec::new();
            let mut unsafe_cols: Vec<String> = Vec::new();
            collect_dict_safety(expr, &mut safe, &mut unsafe_cols);
            let safe: Vec<String> = safe
                .into_iter()
                .filter(|c| !unsafe_cols.iter().any(|u| u.eq_ignore_ascii_case(c)))
                .collect();
            if safe.is_empty() {
                return None;
            }
            let mut changed = false;
            let fields: Vec<arrow::datatypes::Field> = provider_schema
                .fields()
                .iter()
                .map(|f| {
                    if f.data_type() == &arrow::datatypes::DataType::Utf8
                        && safe.iter().any(|c| c.eq_ignore_ascii_case(f.name()))
                    {
                        changed = true;
                        arrow::datatypes::Field::new(
                            f.name(),
                            arrow::datatypes::DataType::Dictionary(
                                Box::new(arrow::datatypes::DataType::Int32),
                                Box::new(arrow::datatypes::DataType::Utf8),
                            ),
                            f.is_nullable(),
                        )
                    } else {
                        f.as_ref().clone()
                    }
                })
                .collect();
            changed.then(|| std::sync::Arc::new(arrow::datatypes::Schema::new(fields)))
        });
        // Output positions (post-projection) of coerced columns: batches
        // cast these back to Utf8 after the RowFilter, so only surviving
        // rows materialize strings and the output schema stays Utf8.
        let coerce_back: Vec<usize> = match &dict_filter_schema {
            None => Vec::new(),
            Some(ds) => {
                let dict_positions: Vec<usize> = ds
                    .fields()
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| {
                        matches!(f.data_type(), arrow::datatypes::DataType::Dictionary(_, _))
                    })
                    .map(|(i, _)| i)
                    .collect();
                match &projection {
                    Some(p) => p
                        .iter()
                        .enumerate()
                        .filter(|(_, file_idx)| dict_positions.contains(file_idx))
                        .map(|(out_idx, _)| out_idx)
                        .collect(),
                    None => dict_positions,
                }
            }
        };

        // Static predicate pushdown: decode predicate columns first and
        // materialize the rest only for matching rows. Requires every
        // referenced column to resolve in the provider schema.
        let filter_spec: Option<(Expr, Vec<usize>)> = filter.and_then(|expr| {
            if expr.contains_subquery() {
                return None;
            }
            let mut cols: Vec<String> = Vec::new();
            crate::physical::morsel::collect_expr_columns(expr, &mut cols);
            if cols.is_empty() {
                return None;
            }
            let mut indices = Vec::with_capacity(cols.len());
            for c in &cols {
                let idx = provider_schema
                    .fields()
                    .iter()
                    .position(|f| f.name().eq_ignore_ascii_case(c))?;
                if !indices.contains(&idx) {
                    indices.push(idx);
                }
            }
            Some((expr.clone(), indices))
        });

        // Discover matching row groups from all files
        let mut all_work = Vec::new();
        for file_path in files {
            let file = File::open(file_path)?;
            let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
            let metadata = builder.metadata().clone();

            let matching_rgs =
                row_group_pruning::prune_row_groups(&metadata, provider_schema, filter);

            for rg_idx in matching_rgs {
                all_work.push(RowGroupWork {
                    file_path: file_path.clone(),
                    row_group_idx: rg_idx,
                });
            }
        }

        let ipc_dirs: std::collections::HashMap<PathBuf, PathBuf> =
            if crate::storage::ipc_cache::enabled() {
                files
                    .iter()
                    .filter_map(|f| {
                        crate::storage::ipc_cache::ensure_sidecar(f).map(|d| (f.clone(), d))
                    })
                    .collect()
            } else {
                Default::default()
            };

        // Distribute row groups round-robin across partitions
        let num_threads = rayon::current_num_threads();
        let num_partitions = if all_work.is_empty() {
            1
        } else {
            std::cmp::min(num_threads, all_work.len())
        };

        let mut partitioned_work = vec![Vec::new(); num_partitions];
        for (i, work) in all_work.into_iter().enumerate() {
            partitioned_work[i % num_partitions].push(work);
        }

        // Compute projected schema
        let projected_schema = match &projection {
            Some(indices) => {
                let fields: Vec<_> = indices.iter().map(|&i| schema.field(i).clone()).collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => schema,
        };

        Ok(Self {
            table_name,
            schema: projected_schema,
            projection,
            partitioned_work,
            batch_size,
            runtime_filter: RuntimeFilterConfig::default(),
            filter_spec,
            dict_filter_schema,
            coerce_back,
            ipc_dirs,
        })
    }

    /// Plan-time handle for linking a runtime join-key filter to this scan.
    pub fn runtime_filter_config(&self) -> RuntimeFilterConfig {
        std::sync::Arc::clone(&self.runtime_filter)
    }
}

#[async_trait]
impl PhysicalOperator for StreamingParquetScanExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        crate::physical::check_partition(self, partition)?;

        // `output_partitions()` floors at 1, so a scan with NO work items still
        // declares (and must answer) partition 0 — with nothing.
        if partition >= self.partitioned_work.len() {
            return Ok(Box::pin(futures::stream::empty()));
        }

        let work_items = self.partitioned_work[partition].clone();
        let projection = self.projection.clone();
        let batch_size = self.batch_size;
        let schema = self.schema.clone();
        // Runtime join-key filter handle: re-resolved at every row-group
        // open, because the driving join publishes the key set only after
        // its build side drains — a scan that started earlier (semi/anti
        // probes drained concurrently) would otherwise never see it.
        let runtime_cfg = std::sync::Arc::clone(&self.runtime_filter);
        let filter_spec = std::sync::Arc::new(self.filter_spec.clone());
        let dict_schema = self.dict_filter_schema.clone();
        let coerce_back = std::sync::Arc::new(self.coerce_back.clone());

        let ipc_dirs = std::sync::Arc::new(self.ipc_dirs.clone());

        // A stream that lazily reads row groups. The body is a LOOP over work
        // items on purpose: a row group whose every row is filtered out (a
        // selective runtime join-key filter can do that) must move on to the
        // NEXT row group. The previous shape returned `None` from the unfold
        // there, which does not mean "retry" — it ends the stream, silently
        // dropping every remaining row group of the partition.
        let stream = futures::stream::unfold(
            (
                work_items.into_iter(),
                projection,
                batch_size,
                schema,
                None::<parquet::arrow::arrow_reader::ParquetRecordBatchReader>,
                None::<std::vec::IntoIter<RecordBatch>>,
                (runtime_cfg, filter_spec, dict_schema, coerce_back, ipc_dirs),
            ),
            |(
                mut work_iter,
                projection,
                batch_size,
                schema,
                mut current_reader,
                mut ipc_iter,
                rt_state,
            )| async move {
                let (runtime_cfg, filter_spec, dict_schema, coerce_back, ipc_dirs) = rt_state;
                loop {
                    // Drain an open IPC row group first.
                    if let Some(mut it) = ipc_iter.take() {
                        if let Some(batch) = it.next() {
                            return Some((
                                Ok(batch),
                                (
                                    work_iter,
                                    projection,
                                    batch_size,
                                    schema,
                                    current_reader,
                                    Some(it),
                                    (runtime_cfg, filter_spec, dict_schema, coerce_back, ipc_dirs),
                                ),
                            ));
                        }
                    }
                    // Then an open parquet reader.
                    if let Some(mut reader) = current_reader.take() {
                        match reader.next() {
                            Some(Ok(batch)) => {
                                let result = wrap_batch(batch, &schema, &coerce_back);
                                return Some((
                                    result,
                                    (
                                        work_iter,
                                        projection,
                                        batch_size,
                                        schema,
                                        Some(reader),
                                        None,
                                        (
                                            runtime_cfg,
                                            filter_spec,
                                            dict_schema,
                                            coerce_back,
                                            ipc_dirs,
                                        ),
                                    ),
                                ));
                            }
                            Some(Err(e)) => {
                                return Some((
                                    Err(QueryError::Arrow(e)),
                                    (
                                        work_iter,
                                        projection,
                                        batch_size,
                                        schema,
                                        None,
                                        None,
                                        (
                                            runtime_cfg,
                                            filter_spec,
                                            dict_schema,
                                            coerce_back,
                                            ipc_dirs,
                                        ),
                                    ),
                                ));
                            }
                            None => {} // exhausted; open the next work item
                        }
                    }

                    let work = work_iter.next()?;

                    // Re-resolve the runtime filter(s) for this row group: each
                    // driving join publishes its own key set only after its own
                    // build side drains, independently -- so a leaf linked to
                    // more than one join may see them populate on different
                    // row groups. Every populated slot at this point is
                    // collected; unpopulated ones are simply skipped (that
                    // join hasn't finished building yet).
                    let runtime: Vec<(usize, std::sync::Arc<RuntimeFilterPayload>)> = runtime_cfg
                        .lock()
                        .iter()
                        .filter_map(|(idx, slot)| slot.lock().clone().map(|set| (*idx, set)))
                        .collect();

                    // IPC sidecar: decode-free, filters applied post-load.
                    // Dictionary-coercion scans keep the parquet path: their
                    // predicate (LIKE over a low-cardinality string) evaluates
                    // once per DICTIONARY VALUE there, and post-load evaluation
                    // over materialized strings measurably loses (Q13
                    // 425→538ms when this guard was missing).
                    let ipc_dir = ipc_dirs.get(&work.file_path).filter(|dir| {
                        // v2 sidecars store low-cardinality strings dict-
                        // encoded; a dict-coercion scan may take the IPC path
                        // when every column it wants coerced is stored dict
                        // (wrap_batch casts survivors back to Utf8 exactly
                        // like the parquet RowFilter path).
                        match &dict_schema {
                            None => true,
                            Some(ds) => {
                                let stored = crate::storage::ipc_cache::sidecar_dict_cols(dir);
                                ds.fields()
                                    .iter()
                                    .filter(|f| {
                                        matches!(
                                            f.data_type(),
                                            arrow::datatypes::DataType::Dictionary(_, _)
                                        )
                                    })
                                    .all(|f| stored.contains(&f.name().to_lowercase()))
                            }
                        }
                    });
                    if let Some(dir) = ipc_dir {
                        match ipc_read_work(
                            dir,
                            &work,
                            projection.as_deref(),
                            filter_spec.as_ref().as_ref(),
                            &runtime,
                            &schema,
                        ) {
                            Ok(batches) => {
                                ipc_iter = Some(batches.into_iter());
                                continue;
                            }
                            Err(e) => {
                                return Some((
                                    Err(e),
                                    (
                                        work_iter,
                                        projection,
                                        batch_size,
                                        schema,
                                        None,
                                        None,
                                        (
                                            runtime_cfg,
                                            filter_spec,
                                            dict_schema,
                                            coerce_back,
                                            ipc_dirs,
                                        ),
                                    ),
                                ));
                            }
                        }
                    }

                    let builder = match match &dict_schema {
                        Some(ds) => {
                            crate::storage::metadata_cache::cached_reader_builder_with_schema(
                                &work.file_path,
                                ds.clone(),
                            )
                        }
                        None => {
                            crate::storage::metadata_cache::cached_reader_builder(&work.file_path)
                        }
                    } {
                        Ok(b) => b,
                        Err(e) => {
                            return Some((
                                Err(e),
                                (
                                    work_iter,
                                    projection,
                                    batch_size,
                                    schema,
                                    None,
                                    None,
                                    (runtime_cfg, filter_spec, dict_schema, coerce_back, ipc_dirs),
                                ),
                            ))
                        }
                    };

                    let builder = builder
                        .with_batch_size(batch_size)
                        .with_row_groups(vec![work.row_group_idx]);

                    let mut predicates: Vec<Box<dyn parquet::arrow::arrow_reader::ArrowPredicate>> =
                        Vec::new();
                    if let Some((expr, indices)) = filter_spec.as_ref() {
                        let mask = parquet::arrow::ProjectionMask::roots(
                            builder.parquet_schema(),
                            indices.iter().copied(),
                        );
                        let expr = expr.clone();
                        predicates.push(Box::new(
                            parquet::arrow::arrow_reader::ArrowPredicateFn::new(
                                mask,
                                move |batch: RecordBatch| {
                                    let arr =
                                        crate::physical::operators::evaluate_expr(&batch, &expr)
                                            .map_err(|e| {
                                                arrow::error::ArrowError::ComputeError(
                                                    e.to_string(),
                                                )
                                            })?;
                                    arr.as_any()
                                        .downcast_ref::<arrow::array::BooleanArray>()
                                        .cloned()
                                        .ok_or_else(|| {
                                            arrow::error::ArrowError::ComputeError(
                                                "scan filter did not evaluate to boolean".into(),
                                            )
                                        })
                                },
                            ),
                        ));
                    }
                    // One predicate per LINKED join (see `RuntimeFilterConfig`'s
                    // own doc comment) -- `RowFilter` applies its predicates in
                    // order, ANDing them together, so N independently-populated
                    // filters on this leaf narrow the same way N `AND`-combined
                    // WHERE clauses would.
                    for (col_idx, set) in &runtime {
                        let mask = parquet::arrow::ProjectionMask::roots(
                            builder.parquet_schema(),
                            [*col_idx],
                        );
                        let set = std::sync::Arc::clone(set);
                        predicates.push(Box::new(
                            parquet::arrow::arrow_reader::ArrowPredicateFn::new(
                                mask,
                                move |batch: RecordBatch| {
                                    let arr = batch
                                        .column(0)
                                        .as_any()
                                        .downcast_ref::<arrow::array::Int64Array>()
                                        .ok_or_else(|| {
                                            arrow::error::ArrowError::ComputeError(
                                                "runtime filter column is not Int64".into(),
                                            )
                                        })?;
                                    use arrow::array::Array;
                                    let mut b =
                                        arrow::array::BooleanBuilder::with_capacity(arr.len());
                                    for i in 0..arr.len() {
                                        b.append_value(
                                            !arr.is_null(i) && set.contains(arr.value(i)),
                                        );
                                    }
                                    Ok(b.finish())
                                },
                            ),
                        ));
                    }
                    let builder = if predicates.is_empty() {
                        builder
                    } else {
                        builder.with_row_filter(parquet::arrow::arrow_reader::RowFilter::new(
                            predicates,
                        ))
                    };

                    let builder = if let Some(ref indices) = projection {
                        let mask = parquet::arrow::ProjectionMask::roots(
                            builder.parquet_schema(),
                            indices.iter().copied(),
                        );
                        builder.with_projection(mask)
                    } else {
                        builder
                    };

                    match builder.build() {
                        Ok(r) => {
                            current_reader = Some(r);
                            // Loop pulls the first batch (and moves on when the
                            // row group filters down to nothing).
                        }
                        Err(e) => {
                            return Some((
                                Err(QueryError::Parquet(e)),
                                (
                                    work_iter,
                                    projection,
                                    batch_size,
                                    schema,
                                    None,
                                    None,
                                    (runtime_cfg, filter_spec, dict_schema, coerce_back, ipc_dirs),
                                ),
                            ))
                        }
                    }
                }
            },
        );

        Ok(Box::pin(stream))
    }

    fn output_partitions(&self) -> usize {
        self.partitioned_work.len().max(1)
    }

    fn name(&self) -> &str {
        "StreamingParquetScan"
    }
}

impl fmt::Display for StreamingParquetScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let total_rgs: usize = self.partitioned_work.iter().map(|p| p.len()).sum();
        write!(
            f,
            "StreamingParquetScan: {} ({} row groups, {} partitions)",
            self.table_name,
            total_rgs,
            self.partitioned_work.len()
        )?;
        if let Some(ref proj) = self.projection {
            write!(f, " projection={:?}", proj)?;
        }
        Ok(())
    }
}

/// Classify Utf8 column references in a predicate: `safe` collects columns
/// whose reference is a dict-evaluable shape (col = lit, col <> lit,
/// col LIKE/NOT LIKE lit, col IN (literals)); `unsafe_cols` collects string
/// columns referenced any other way (SUBSTRING(col), comparisons between
/// columns, ...), which veto coercion.
fn collect_dict_safety(expr: &Expr, safe: &mut Vec<String>, unsafe_cols: &mut Vec<String>) {
    use crate::planner::BinaryOp;
    match expr {
        Expr::BinaryExpr { left, op, right } => match op {
            BinaryOp::And | BinaryOp::Or => {
                collect_dict_safety(left, safe, unsafe_cols);
                collect_dict_safety(right, safe, unsafe_cols);
            }
            BinaryOp::Eq | BinaryOp::NotEq | BinaryOp::Like | BinaryOp::NotLike => {
                match (&**left, &**right) {
                    (Expr::Column(c), Expr::Literal(crate::planner::ScalarValue::Utf8(_))) => {
                        safe.push(c.name.clone())
                    }
                    _ => {
                        mark_string_cols(left, unsafe_cols);
                        mark_string_cols(right, unsafe_cols);
                    }
                }
            }
            _ => {
                mark_string_cols(left, unsafe_cols);
                mark_string_cols(right, unsafe_cols);
            }
        },
        Expr::InList { expr: e, list, .. } => {
            let all_lit = list
                .iter()
                .all(|l| matches!(l, Expr::Literal(crate::planner::ScalarValue::Utf8(_))));
            match &**e {
                Expr::Column(c) if all_lit => safe.push(c.name.clone()),
                _ => mark_string_cols(e, unsafe_cols),
            }
        }
        Expr::Between {
            expr: e, low, high, ..
        } => {
            mark_string_cols(e, unsafe_cols);
            mark_string_cols(low, unsafe_cols);
            mark_string_cols(high, unsafe_cols);
        }
        other => mark_string_cols(other, unsafe_cols),
    }
}

/// Conservatively mark every column referenced by `expr` as dict-unsafe.
fn mark_string_cols(expr: &Expr, out: &mut Vec<String>) {
    let mut names = Vec::new();
    crate::physical::morsel::collect_expr_columns(expr, &mut names);
    out.extend(names);
}

/// Wrap a decoded batch with the logical schema, casting coerced
/// Read one row group from the IPC sidecar and apply, post-load, everything
/// the parquet path pushes into the decoder: the static predicate, the
/// runtime join-key filter, and the output projection.
///
/// Post-load filtering is not a downgrade here: the load is zero-copy mmap
/// references, so the "wasted" materialization the RowFilter exists to avoid
/// costs nothing — the filter kernel then copies survivors only, which is
/// the same work the decoder-side path does for its survivors.
#[allow(clippy::type_complexity)]
fn ipc_read_work(
    dir: &std::path::Path,
    work: &RowGroupWork,
    projection: Option<&[usize]>,
    filter_spec: Option<&(Expr, Vec<usize>)>,
    runtime: &[(usize, std::sync::Arc<RuntimeFilterPayload>)],
    out_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    // Read the union of output + filter + runtime-key columns; positions
    // within the read set are recovered by binary search (the set is sorted).
    let read_set: Option<Vec<usize>> = match projection {
        None => None,
        Some(p) => {
            let mut s: Vec<usize> = p.to_vec();
            if let Some((_, idxs)) = filter_spec {
                s.extend(idxs.iter().copied());
            }
            for (ridx, _) in runtime {
                s.push(*ridx);
            }
            s.sort_unstable();
            s.dedup();
            Some(s)
        }
    };
    let pos_of = |file_idx: usize| -> Result<usize> {
        match &read_set {
            None => Ok(file_idx),
            Some(s) => s.binary_search(&file_idx).map_err(|_| {
                QueryError::Execution(format!(
                    "IPC read set does not contain file column {file_idx}"
                ))
            }),
        }
    };

    let mut batches = crate::storage::ipc_cache::read_row_group(
        dir,
        work.row_group_idx,
        read_set.as_deref(),
        None,
    )?;

    if let Some((expr, _)) = filter_spec {
        batches = crate::physical::operators::filter_batches(batches, expr)?;
    }
    // One AND-combined pass per LINKED join (mirrors the parquet decoder
    // RowFilter path's own multi-predicate semantics above) -- each pass
    // only narrows `batches` further, so order among the entries never
    // matters for correctness.
    for (ridx, set) in runtime {
        let col = pos_of(*ridx)?;
        let mut kept = Vec::with_capacity(batches.len());
        for batch in batches {
            let arr = batch
                .column(col)
                .as_any()
                .downcast_ref::<arrow::array::Int64Array>()
                .ok_or_else(|| {
                    QueryError::Execution("runtime filter column is not Int64".into())
                })?;
            use arrow::array::Array;
            let mut b = arrow::array::BooleanBuilder::with_capacity(arr.len());
            for i in 0..arr.len() {
                b.append_value(!arr.is_null(i) && set.contains(arr.value(i)));
            }
            let mask = b.finish();
            let filtered = arrow::compute::filter_record_batch(&batch, &mask)
                .map_err(|e| QueryError::Execution(e.to_string()))?;
            if filtered.num_rows() > 0 {
                kept.push(filtered);
            }
        }
        batches = kept;
    }

    // Survivor-size-gated 8k re-slice (see ipc_cache::reslice_large).
    batches = crate::storage::ipc_cache::reslice_large(batches, 16384, 8192);

    // Project down to the requested output columns, in output order, and
    // re-wrap with the logical (qualified-name) schema.
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        let projected = match projection {
            None => batch,
            Some(p) => {
                let cols: Vec<usize> = p.iter().map(|&i| pos_of(i)).collect::<Result<_>>()?;
                batch
                    .project(&cols)
                    .map_err(|e| QueryError::Execution(e.to_string()))?
            }
        };
        out.push(wrap_batch(projected, out_schema, &[])?);
    }
    Ok(out)
}

/// dictionary filter columns back to Utf8 (survivors only — the RowFilter
/// already dropped non-matching rows).
fn wrap_batch(
    batch: RecordBatch,
    schema: &SchemaRef,
    coerce_back: &[usize],
) -> Result<RecordBatch> {
    if batch.schema() == *schema || batch.num_columns() != schema.fields().len() {
        return Ok(batch);
    }
    let mut cols = batch.columns().to_vec();
    for &i in coerce_back {
        if i < cols.len() {
            if matches!(
                cols[i].data_type(),
                arrow::datatypes::DataType::Dictionary(_, _)
            ) {
                cols[i] = arrow::compute::cast(&cols[i], &arrow::datatypes::DataType::Utf8)
                    .map_err(|e| QueryError::Execution(e.to_string()))?;
            }
        }
    }
    // v2 IPC sidecars store low-cardinality strings dictionary-encoded;
    // any remaining dict column whose declared field is Utf8 casts back
    // here (survivors only — filters already ran).
    for (i, f) in schema.fields().iter().enumerate() {
        if f.data_type() == &arrow::datatypes::DataType::Utf8
            && matches!(
                cols[i].data_type(),
                arrow::datatypes::DataType::Dictionary(_, _)
            )
        {
            cols[i] = arrow::compute::cast(&cols[i], &arrow::datatypes::DataType::Utf8)
                .map_err(|e| QueryError::Execution(e.to_string()))?;
        }
    }
    RecordBatch::try_new(schema.clone(), cols)
        .map_err(|e| QueryError::Execution(format!("Schema mismatch: {}", e)))
}
