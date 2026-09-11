//! Hash join operator

use crate::error::{QueryError, Result};
use crate::physical::operators::filter::evaluate_expr;
use crate::physical::operators::vectorized_hash;
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

mod outer_probe;

/// Own all partition work through completion or cancellation. Results retain
/// declared partition order even though failures are observed as tasks finish.
/// This fixes task lifetime, not admission of the collected build/probe payload.
async fn collect_join_partitions(
    input: Arc<dyn PhysicalOperator>,
    phase: &'static str,
) -> Result<Vec<Vec<RecordBatch>>> {
    let partitions = input.output_partitions();
    let mut tasks = tokio::task::JoinSet::new();
    let mut results: Vec<Option<Vec<RecordBatch>>> = (0..partitions).map(|_| None).collect();
    for partition in 0..partitions {
        let input = input.clone();
        tasks.spawn(async move {
            let stream = input.execute(partition).await?;
            let batches = stream.try_collect().await?;
            Ok::<_, crate::error::QueryError>((partition, batches))
        });
    }
    while let Some(result) = tasks.join_next().await {
        match result {
            Ok(Ok((partition, batches))) => results[partition] = Some(batches),
            Ok(Err(error)) => {
                tasks.shutdown().await;
                return Err(error);
            }
            Err(error) => {
                tasks.shutdown().await;
                return Err(crate::error::QueryError::Execution(format!(
                    "{phase} partition task failed: {error}"
                )));
            }
        }
    }
    results
        .into_iter()
        .enumerate()
        .map(|(partition, result)| {
            result.ok_or_else(|| {
                crate::error::QueryError::Internal(format!(
                    "{phase} partition {partition} completed without a result"
                ))
            })
        })
        .collect()
}

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

/// Row-major build-side payload for Inner-join gather: `data` packs every
/// build column of row r at `data[r * stride ..]`, each column at its fixed
/// byte offset (little-endian). For eligible builds this REPLACES the
/// columnar concat — the same single copy at build time, but the
/// joined-batch gather then touches ONE packed row per matched row instead
/// of doing one random read per matched row PER COLUMN (DuckDB TupleData /
/// Velox RowContainer pattern). Built exactly once inside the build-cache
/// init closure; never lazily, never keyed by pointer identity.
struct RowStore {
    stride: usize,
    data: Vec<u8>,
    cols: Vec<(usize, u8, arrow::datatypes::DataType)>,
    /// Includes terminal nrows; validates each original batch's row domain.
    row_offsets: Vec<usize>,
    nrows: usize,
    // Buffers/metadata above drop before their hierarchical reservation.
    _reservation: crate::execution::MemoryReservation,
}

#[derive(Clone, Copy)]
enum RowStoreSrc<'a> {
    I64(&'a [i64]),
    F64(&'a [f64]),
    I32(&'a [i32]),
}
fn row_store_error(message: &str) -> crate::error::QueryError {
    crate::error::QueryError::Execution(format!("row store: {message}"))
}
fn row_store_width(dt: &arrow::datatypes::DataType) -> Option<u8> {
    use arrow::datatypes::DataType;
    match dt {
        DataType::Int64 | DataType::Float64 => Some(8),
        DataType::Int32 | DataType::Date32 => Some(4),
        _ => None,
    }
}
fn row_store_charge(
    nrows: usize,
    stride: usize,
    columns: usize,
    batches: usize,
) -> Result<(usize, usize, usize)> {
    // Global-u32 gather uses u32::MAX as an invalid/sentinel domain elsewhere.
    if nrows > u32::MAX as usize {
        return Err(row_store_error("global row IDs exceed u32 domain"));
    }
    let data = nrows
        .checked_mul(stride)
        .ok_or_else(|| row_store_error("payload layout overflow"))?;
    let offsets = batches
        .checked_add(1)
        .and_then(|n| n.checked_mul(std::mem::size_of::<usize>()))
        .ok_or_else(|| row_store_error("offset layout overflow"))?;
    let cols = columns
        .checked_mul(std::mem::size_of::<(usize, u8, arrow::datatypes::DataType)>())
        .ok_or_else(|| row_store_error("column layout overflow"))?;
    let retained = std::mem::size_of::<RowStore>()
        .checked_add(data)
        .and_then(|n| n.checked_add(offsets))
        .and_then(|n| n.checked_add(cols))
        .ok_or_else(|| row_store_error("retained layout overflow"))?;
    let sources = batches
        .checked_mul(columns)
        .and_then(|n| n.checked_mul(std::mem::size_of::<RowStoreSrc<'_>>()))
        .ok_or_else(|| row_store_error("source view layout overflow"))?;
    let chunks = batches
        .checked_mul(std::mem::size_of::<(&[RowStoreSrc<'_>], &mut [u8])>())
        .ok_or_else(|| row_store_error("chunk layout overflow"))?;
    let temporary = sources
        .checked_add(chunks)
        .and_then(|n| n.checked_add(std::mem::size_of::<Vec<RowStoreSrc<'_>>>()))
        .and_then(|n| n.checked_add(std::mem::size_of::<Vec<(&[RowStoreSrc<'_>], &mut [u8])>>()))
        .ok_or_else(|| row_store_error("temporary layout overflow"))?;
    Ok((data, retained, temporary))
}
fn row_store_vec<T>(count: usize) -> Result<Vec<T>> {
    let mut result = Vec::new();
    result
        .try_reserve_exact(count)
        .map_err(|e| row_store_error(&format!("allocation refused: {e}")))?;
    Ok(result)
}
fn row_store_owned_vec<T>(
    count: usize,
    guard: &mut crate::execution::MemoryReservation,
) -> Result<Vec<T>> {
    let result = row_store_vec::<T>(count)?;
    let excess = result
        .capacity()
        .checked_sub(count)
        .and_then(|n| n.checked_mul(std::mem::size_of::<T>()))
        .ok_or_else(|| row_store_error("actual capacity overflow"))?;
    if excess != 0 {
        let total = guard
            .size()
            .checked_add(excess)
            .ok_or_else(|| row_store_error("actual admission overflow"))?;
        // Admit allocator excess before subsequent allocation or packing.
        guard.resize(total)?;
    }
    Ok(result)
}
impl RowStore {
    fn build(batches: &[RecordBatch], pool: &crate::execution::SharedMemoryPool) -> Result<Self> {
        use arrow::datatypes::DataType;
        let first = batches
            .first()
            .ok_or_else(|| row_store_error("no build batches"))?;
        if first.num_columns() == 0 {
            return Err(row_store_error("no build columns"));
        }
        let mut stride = 0usize;
        for array in first.columns() {
            let width = row_store_width(array.data_type()).ok_or_else(|| {
                crate::error::QueryError::NotImplemented("row-store physical type".into())
            })?;
            stride = stride
                .checked_add(width as usize)
                .ok_or_else(|| row_store_error("stride overflow"))?;
        }
        let mut nrows = 0usize;
        for batch in batches {
            if batch.num_columns() != first.num_columns()
                || batch
                    .columns()
                    .iter()
                    .zip(first.columns())
                    .any(|(a, b)| a.data_type() != b.data_type())
            {
                return Err(crate::error::QueryError::Type(
                    "row-store build batches have inconsistent physical schemas".into(),
                ));
            }
            if batch.columns().iter().any(|a| a.null_count() != 0) {
                return Err(crate::error::QueryError::NotImplemented(
                    "row-store nullable payload".into(),
                ));
            }
            nrows = nrows
                .checked_add(batch.num_rows())
                .ok_or_else(|| row_store_error("row count overflow"))?;
        }
        let (data_bytes, retained_bytes, temp_bytes) =
            row_store_charge(nrows, stride, first.num_columns(), batches.len())?;
        let mut reservation = pool.allocate(retained_bytes)?;
        // Declared before temporary vectors: those vectors drop first on errors.
        let mut temporary = pool.allocate(temp_bytes)?;
        let mut cols = row_store_owned_vec(first.num_columns(), &mut reservation)?;
        let mut offset = 0usize;
        for array in first.columns() {
            let width = row_store_width(array.data_type()).expect("validated width");
            cols.push((offset, width, array.data_type().clone()));
            offset += width as usize;
        }
        let mut row_offsets = row_store_owned_vec(
            batches
                .len()
                .checked_add(1)
                .ok_or_else(|| row_store_error("batch count overflow"))?,
            &mut reservation,
        )?;
        let mut base = 0usize;
        row_offsets.push(base);
        for batch in batches {
            base += batch.num_rows();
            row_offsets.push(base);
        }
        let mut data = row_store_owned_vec(data_bytes, &mut reservation)?;
        data.resize(data_bytes, 0u8);
        let source_count = batches
            .len()
            .checked_mul(first.num_columns())
            .ok_or_else(|| row_store_error("source count overflow"))?;
        let mut sources = row_store_owned_vec(source_count, &mut temporary)?;
        for batch in batches {
            for array in batch.columns() {
                let source = match array.data_type() {
                    DataType::Int64 => RowStoreSrc::I64(
                        array
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .ok_or_else(|| {
                                crate::error::QueryError::Type(
                                    "invalid row-store Int64 array".into(),
                                )
                            })?
                            .values(),
                    ),
                    DataType::Float64 => RowStoreSrc::F64(
                        array
                            .as_any()
                            .downcast_ref::<arrow::array::Float64Array>()
                            .ok_or_else(|| {
                                crate::error::QueryError::Type(
                                    "invalid row-store Float64 array".into(),
                                )
                            })?
                            .values(),
                    ),
                    DataType::Int32 => RowStoreSrc::I32(
                        array
                            .as_any()
                            .downcast_ref::<arrow::array::Int32Array>()
                            .ok_or_else(|| {
                                crate::error::QueryError::Type(
                                    "invalid row-store Int32 array".into(),
                                )
                            })?
                            .values(),
                    ),
                    DataType::Date32 => RowStoreSrc::I32(
                        array
                            .as_any()
                            .downcast_ref::<arrow::array::Date32Array>()
                            .ok_or_else(|| {
                                crate::error::QueryError::Type(
                                    "invalid row-store Date32 array".into(),
                                )
                            })?
                            .values(),
                    ),
                    _ => return Err(row_store_error("validated physical type changed")),
                };
                sources.push(source);
            }
        }
        let mut slices = row_store_owned_vec(batches.len(), &mut temporary)?;
        let mut rest = data.as_mut_slice();
        for (index, batch) in batches.iter().enumerate() {
            let bytes = batch
                .num_rows()
                .checked_mul(stride)
                .ok_or_else(|| row_store_error("batch extent overflow"))?;
            let (chunk, next) = std::mem::take(&mut rest).split_at_mut(bytes);
            rest = next;
            let start = index * first.num_columns();
            slices.push((&sources[start..start + first.num_columns()], chunk));
        }
        slices
            .par_iter_mut()
            .for_each(|(source, chunk)| fill_row_store_chunk(source, chunk, &cols, stride));
        drop(slices);
        drop(sources);
        drop(temporary);
        Ok(Self {
            stride,
            data,
            cols,
            row_offsets,
            nrows,
            _reservation: reservation,
        })
    }
    fn global_row(&self, batch: usize, row: usize) -> Result<usize> {
        let start = *self
            .row_offsets
            .get(batch)
            .ok_or_else(|| row_store_error("batch index out of range"))?;
        let end = *self
            .row_offsets
            .get(
                batch
                    .checked_add(1)
                    .ok_or_else(|| row_store_error("batch index overflow"))?,
            )
            .ok_or_else(|| row_store_error("batch index out of range"))?;
        if row >= end - start {
            return Err(row_store_error("batch row index out of range"));
        }
        start
            .checked_add(row)
            .ok_or_else(|| row_store_error("global row overflow"))
    }
}
fn fill_row_store_chunk(
    srcs: &[RowStoreSrc<'_>],
    chunk: &mut [u8],
    cols: &[(usize, u8, arrow::datatypes::DataType)],
    stride: usize,
) {
    for (row, destination) in chunk.chunks_exact_mut(stride).enumerate() {
        for (index, source) in srcs.iter().enumerate() {
            let off = cols[index].0;
            match source {
                RowStoreSrc::I64(v) => {
                    destination[off..off + 8].copy_from_slice(&v[row].to_le_bytes())
                }
                RowStoreSrc::F64(v) => {
                    destination[off..off + 8].copy_from_slice(&v[row].to_le_bytes())
                }
                RowStoreSrc::I32(v) => {
                    destination[off..off + 4].copy_from_slice(&v[row].to_le_bytes())
                }
            }
        }
    }
}

/// Gather build columns row-wise from the row store: one packed-row read per
/// matched row. Output column order matches the build batch column order.
/// Caller guarantees `build_indices` contains no usize::MAX null sentinels.
fn gather_build_from_row_store(
    store: &RowStore,
    row_offsets: &[usize],
    build_indices: &[(usize, usize)],
) -> Result<Vec<ArrayRef>> {
    if row_offsets != store.row_offsets.as_slice() {
        return Err(row_store_error("offset mapping does not belong to store"));
    }
    gather_row_store_rows(
        store,
        build_indices
            .iter()
            .map(|&(batch, row)| store.global_row(batch, row)),
    )
}
fn gather_row_store_rows(
    store: &RowStore,
    rows: impl ExactSizeIterator<Item = Result<usize>>,
) -> Result<Vec<ArrayRef>> {
    use arrow::datatypes::DataType;
    enum Buf {
        I64(Vec<i64>),
        F64(Vec<f64>),
        I32(Vec<i32>),
        D32(Vec<i32>),
    }
    let n = rows.len();
    let mut bufs = row_store_vec(store.cols.len())?;
    for (_, _, dt) in &store.cols {
        bufs.push(match dt {
            DataType::Int64 => Buf::I64(row_store_vec(n)?),
            DataType::Float64 => Buf::F64(row_store_vec(n)?),
            DataType::Int32 => Buf::I32(row_store_vec(n)?),
            DataType::Date32 => Buf::D32(row_store_vec(n)?),
            _ => return Err(row_store_error("unsupported stored type")),
        });
    }
    for row in rows {
        let row = row?;
        if row >= store.nrows {
            return Err(row_store_error("global row index out of range"));
        }
        let base = row
            .checked_mul(store.stride)
            .ok_or_else(|| row_store_error("gather offset overflow"))?;
        for (index, &(offset, _, _)) in store.cols.iter().enumerate() {
            let p = base + offset;
            match &mut bufs[index] {
                Buf::I64(v) => v.push(i64::from_le_bytes(store.data[p..p + 8].try_into().unwrap())),
                Buf::F64(v) => v.push(f64::from_le_bytes(store.data[p..p + 8].try_into().unwrap())),
                Buf::I32(v) | Buf::D32(v) => {
                    v.push(i32::from_le_bytes(store.data[p..p + 4].try_into().unwrap()))
                }
            }
        }
    }
    let mut result = row_store_vec(store.cols.len())?;
    for buf in bufs {
        result.push(match buf {
            Buf::I64(v) => Arc::new(Int64Array::from(v)) as ArrayRef,
            Buf::F64(v) => Arc::new(arrow::array::Float64Array::from(v)) as ArrayRef,
            Buf::I32(v) => Arc::new(arrow::array::Int32Array::from(v)) as ArrayRef,
            Buf::D32(v) => Arc::new(arrow::array::Date32Array::from(v)) as ArrayRef,
        });
    }
    Ok(result)
}

/// Cached build side data - collected once, reused across partitions
struct BuildSideCache {
    batches: Vec<RecordBatch>,
    hash_table: HashMap<JoinKey, Vec<HashEntry>>,
    /// Fast path: specialized i64 hash table for single-key Int64 joins
    i64_hash_table: Option<HashMap<i64, Vec<HashEntry>>>,
    /// Vectorized hash table for batch-level probing (multi-type support)
    vectorized_ht: Option<VectorizedHashTable>,
    /// Shared matched-bit per build row (indexed per batch), for join types
    /// that must emit unmatched BUILD rows exactly once across all probe
    /// partitions (Left build-left, Right build-right, Full). Without this,
    /// multi-partition probes silently dropped unmatched build rows — Q13's
    /// zero-order customers vanished at SF=10.
    build_matched: Option<Vec<Vec<std::sync::atomic::AtomicBool>>>,
    /// Number of probe partitions that finished; the last one emits the
    /// unmatched build rows.
    completed_partitions: std::sync::atomic::AtomicUsize,
    /// Row-major copy of the build side for Inner-join gather, plus the
    /// per-batch offsets mapping (batch_idx, row_idx) -> global row. Present
    /// only for eligible builds (Inner, no filter, fixed-width null-free
    /// columns); when present the columnar concat was skipped and `batches`
    /// keeps the unconcatenated originals (still needed for the hash tables'
    /// key buffers and fallback paths).
    row_store: Option<RowStore>,
}

/// Vectorized hash table using open addressing with batch-level operations.
/// Stores (batch_idx, row_idx) pairs indexed by hash of key columns.
/// Eliminates per-row JoinKey allocation during both build and probe.
/// Decode a Dictionary-encoded join-key array to its value type. The
/// vectorized hash table (`VectorizedHashTable`) hashes and compares the
/// physical value arrays; Dictionary(Int32, Utf8) keys — every native
/// table's low-cardinality string column, and IPC-sidecar parquet reads —
/// used to fail `can_vectorize_arrays` and fall back to the generic
/// `HashMap<JoinKey, _>` path (per-row String allocation, and the path
/// task 001's defects lived on). Decoding ONCE per batch here, on BOTH the
/// build and the probe side, means the table always sees one physical type
/// for a logical key (Dictionary build vs Utf8 probe included). Same
/// `compute::cast` the spill path and the aggregate use.
fn decode_dictionary_key(arr: ArrayRef) -> Result<ArrayRef> {
    match arr.data_type() {
        arrow::datatypes::DataType::Dictionary(_, value_type) => {
            compute::cast(arr.as_ref(), value_type).map_err(|e| {
                crate::error::QueryError::Execution(format!(
                    "decode Dictionary join key to {value_type:?}: {e}"
                ))
            })
        }
        _ => Ok(arr),
    }
}

/// Evaluate the join-key expressions of `batch` for the vectorized table,
/// Dictionary keys decoded to their value type (see `decode_dictionary_key`).
/// Every key evaluation that feeds `VectorizedHashTable` — its build and
/// every probe entry point — goes through here, so build and probe can
/// never disagree on the physical key type.
fn evaluate_join_keys(batch: &RecordBatch, exprs: &[Expr]) -> Result<Vec<ArrayRef>> {
    exprs
        .iter()
        .map(|e| evaluate_expr(batch, e).and_then(decode_dictionary_key))
        .collect()
}

fn join_index_bucket_count(rows: usize) -> Result<usize> {
    rows.checked_mul(2)
        .and_then(|n| n.max(16).checked_next_power_of_two())
        .ok_or_else(|| QueryError::Execution("join index bucket count overflow".into()))
}

fn join_index_bytes(buckets: usize, rows: usize) -> Result<usize> {
    buckets
        .checked_mul(std::mem::size_of::<u32>())
        .and_then(|b| {
            rows.checked_mul(std::mem::size_of::<u32>() + std::mem::size_of::<(u32, u32)>())
                .and_then(|r| b.checked_add(r))
        })
        .ok_or_else(|| QueryError::Execution("join index size overflow".into()))
}

/// Costing bound for heads/next/entries only. Direct addressing cannot exceed
/// this layout. Payload, evaluated keys and gather storage are separate, and
/// actual construction must still reserve its allocations in the query pool.
pub(crate) fn join_index_storage_bound(rows: usize) -> Result<usize> {
    join_index_bytes(join_index_bucket_count(rows)?, rows)
}

struct VectorizedHashTable {
    /// heads[hash & mask] = index of the first chain entry, or u32::MAX if empty.
    /// Flat chained layout: one contiguous allocation each for heads/next/entries
    /// instead of a Vec per bucket — with multi-million-row build sides the
    /// per-bucket-Vec layout meant one heap allocation per occupied bucket and a
    /// pointer chase per probe, which dominated large joins.
    heads: Vec<u32>,
    /// next[entry] = next entry index in the same bucket's chain, or u32::MAX
    next: Vec<u32>,
    /// entries[entry] = (build_batch_idx, build_row_idx)
    entries: Vec<(u32, u32)>,
    /// Mask for bucket index (power-of-2 bucket count - 1)
    mask: usize,
    /// Pre-evaluated key arrays for each build batch: build_key_arrays[batch_idx][key_col] = ArrayRef
    build_key_arrays: Vec<Vec<ArrayRef>>,
    /// Zero-copy i64 views of the build keys ([col][batch]) when every key
    /// column is non-null-free Int64-typed: chain-walk comparisons then use
    /// raw i64 loads instead of per-candidate dynamic downcasts.
    i64_key_bufs: Option<Vec<Vec<arrow::buffer::ScalarBuffer<i64>>>>,
    /// Direct-address mode: for a single i64 key whose build domain spans a
    /// dense range, `heads` is indexed by (key - min) instead of hash&mask.
    /// Chains then contain EXACTLY equal keys, so probing needs no hashing
    /// and no key comparison at all (dimension-table PK builds: customer,
    /// part, supplier).
    direct: Option<(i64, i64)>,
    // Private index buffers are dropped before their admission owner. This
    // covers heads/next/entries only, not key arrays or build payload.
    _index_reservation: crate::execution::MemoryReservation,
}

impl VectorizedHashTable {
    /// Build the vectorized hash table from build-side batches.
    fn build(
        batches: &[RecordBatch],
        key_exprs: &[Expr],
        pool: &crate::execution::SharedMemoryPool,
    ) -> Result<Option<Self>> {
        // Evaluate key expressions for each batch and check types
        let mut build_key_arrays: Vec<Vec<ArrayRef>> = Vec::with_capacity(batches.len());
        let mut total_rows = 0usize;

        for batch in batches {
            // Dictionary keys are decoded to their value type here, ONCE
            // per build batch, so `can_vectorize_arrays` sees Utf8 and the
            // probe side (decoded the same way) compares like with like.
            let key_arrays = evaluate_join_keys(batch, key_exprs)?;
            // Verify we can vectorize these types
            if !key_arrays.is_empty() && !vectorized_hash::can_vectorize_arrays(&key_arrays) {
                return Ok(None);
            }
            total_rows = total_rows.checked_add(batch.num_rows()).ok_or_else(|| {
                crate::error::QueryError::Execution("join index row count overflow".into())
            })?;
            build_key_arrays.push(key_arrays);
        }

        if total_rows >= u32::MAX as usize || batches.len() > u32::MAX as usize {
            return Err(crate::error::QueryError::Execution(
                "Build side too large for vectorized hash table".into(),
            ));
        }

        // Size buckets to next_power_of_2(total_rows * 2) for ~50% load factor
        let bucket_count = join_index_bucket_count(total_rows)?;
        let mask = bucket_count - 1;
        // Inserts happen AFTER the mode decision below: building the hash
        // layout and then rebuilding direct-address doubled build cost.
        let mut index_rows = total_rows;
        let mut index_buckets = bucket_count;

        // Zero-copy i64 buffers for fast chain comparisons (columns first,
        // batches second). Only when EVERY key column in EVERY batch is Int64.
        let n_cols = build_key_arrays.first().map(|k| k.len()).unwrap_or(0);
        let mut i64_key_bufs: Option<Vec<Vec<arrow::buffer::ScalarBuffer<i64>>>> = {
            let mut cols: Vec<Vec<arrow::buffer::ScalarBuffer<i64>>> =
                (0..n_cols).map(|_| Vec::new()).collect();
            let mut ok = n_cols > 0;
            'outer: for key_arrays in &build_key_arrays {
                for (col, arr) in key_arrays.iter().enumerate() {
                    match arr.as_any().downcast_ref::<Int64Array>() {
                        Some(a) => cols[col].push(a.values().clone()),
                        None => {
                            ok = false;
                            break 'outer;
                        }
                    }
                }
            }
            if ok {
                Some(cols)
            } else {
                None
            }
        };
        if build_key_arrays.is_empty() {
            i64_key_bufs = None;
        }

        // Direct-address upgrade: single i64 key over a dense domain.
        let mut direct: Option<(i64, i64)> = None;
        if n_cols == 1 {
            if let Some(bufs) = &i64_key_bufs {
                let mut kmin = i64::MAX;
                let mut kmax = i64::MIN;
                let mut n_keys = 0usize;
                for (batch_idx, key_arrays) in build_key_arrays.iter().enumerate() {
                    let arr = key_arrays[0].as_any().downcast_ref::<Int64Array>().unwrap();
                    let vals = &bufs[0][batch_idx];
                    for row in 0..vals.len() {
                        if !arr.is_null(row) {
                            let v = vals[row];
                            kmin = kmin.min(v);
                            kmax = kmax.max(v);
                            n_keys += 1;
                        }
                    }
                }
                const DIRECT_MAX_RANGE: i64 = 16_000_000;
                if n_keys > 0
                    && kmax.saturating_sub(kmin) < DIRECT_MAX_RANGE
                    && (kmax - kmin + 1) as usize <= bucket_count
                {
                    index_buckets = (kmax - kmin + 1) as usize;
                    index_rows = n_keys;
                    direct = Some((kmin, kmax));
                }
            }
        }

        // Choose the final representation before allocating; the old direct
        // path allocated and discarded a second set of next/entry vectors.
        let index_bytes = join_index_bytes(index_buckets, index_rows)?;
        let mut reservation = pool.allocate(index_bytes)?;
        fn index_vec<T>(
            count: usize,
            reservation: &mut crate::execution::MemoryReservation,
        ) -> Result<Vec<T>> {
            let mut values = Vec::new();
            values.try_reserve_exact(count).map_err(|e| {
                crate::error::QueryError::Execution(format!("join index allocation failed: {e}"))
            })?;
            // The allocator may report more capacity than requested. Charge
            // that excess before allocating any subsequent vector.
            let charged = values
                .capacity()
                .checked_sub(count)
                .and_then(|n| n.checked_mul(std::mem::size_of::<T>()))
                .and_then(|n| reservation.size().checked_add(n))
                .ok_or_else(|| {
                    crate::error::QueryError::Execution("join index capacity overflow".into())
                })?;
            reservation.resize(charged)?;
            Ok(values)
        }
        let mut heads: Vec<u32> = index_vec(index_buckets, &mut reservation)?;
        let mut next: Vec<u32> = index_vec(index_rows, &mut reservation)?;
        let mut entries: Vec<(u32, u32)> = index_vec(index_rows, &mut reservation)?;
        heads.resize(index_buckets, u32::MAX);
        if let Some((min, _)) = direct {
            for (batch_idx, key_arrays) in build_key_arrays.iter().enumerate() {
                let arr = key_arrays[0].as_any().downcast_ref::<Int64Array>().unwrap();
                for row in 0..arr.len() {
                    if arr.is_null(row) {
                        continue;
                    }
                    let slot = (arr.value(row) - min) as usize;
                    let entry_idx = entries.len() as u32;
                    entries.push((batch_idx as u32, row as u32));
                    next.push(heads[slot]);
                    heads[slot] = entry_idx;
                }
            }
        }
        if direct.is_none() {
            for (batch_idx, key_arrays) in build_key_arrays.iter().enumerate() {
                if key_arrays.is_empty() {
                    continue;
                }
                let num_rows = batches[batch_idx].num_rows();
                let hashes = vectorized_hash::hash_arrays(key_arrays, num_rows);
                for row_idx in 0..num_rows {
                    if vectorized_hash::has_null(key_arrays, row_idx) {
                        continue;
                    }
                    let bucket = hashes[row_idx] as usize & mask;
                    let entry_idx = entries.len() as u32;
                    entries.push((batch_idx as u32, row_idx as u32));
                    next.push(heads[bucket]);
                    heads[bucket] = entry_idx;
                }
            }
        }

        Ok(Some(VectorizedHashTable {
            heads,
            next,
            entries,
            mask,
            build_key_arrays,
            i64_key_bufs,
            direct,
            _index_reservation: reservation,
        }))
    }

    /// Probe the hash table with a batch of probe keys.
    /// Returns matched (build_batch_idx, build_row_idx, probe_row_idx) triples.
    #[inline]
    /// Closure-emitting probe for the fast key layouts (direct-address and
    /// zero-copy i64). Returns false when only the generic layout can serve
    /// — the caller then uses `probe_batch`. Exists because materializing
    /// matches as Vec<(u32,u32,u32)> and re-packing them into
    /// (usize,usize)/usize index vectors cost ~24GB of write+readback on
    /// Q9's two 604M-row joins (HJ_PROF idx-build ~37s cumulative).
    fn probe_batch_into(
        &self,
        probe_key_arrays: &[ArrayRef],
        num_rows: usize,
        mut emit: impl FnMut(u32, u32, u32),
    ) -> bool {
        if let Some((kmin, kmax)) = self.direct {
            if let Some(pa) = probe_key_arrays[0].as_any().downcast_ref::<Int64Array>() {
                let vals = pa.values();
                let nulls = pa.nulls();
                for probe_row in 0..num_rows {
                    if let Some(nb) = nulls {
                        if !nb.is_valid(probe_row) {
                            continue;
                        }
                    }
                    let k = vals[probe_row];
                    if k < kmin || k > kmax {
                        continue;
                    }
                    let mut entry = self.heads[(k - kmin) as usize];
                    while entry != u32::MAX {
                        let (bb, br) = self.entries[entry as usize];
                        emit(bb, br, probe_row as u32);
                        entry = self.next[entry as usize];
                    }
                }
                return true;
            }
        }
        if let Some(build_bufs) = &self.i64_key_bufs {
            let probe_bufs: Option<Vec<&Int64Array>> = probe_key_arrays
                .iter()
                .map(|a| a.as_any().downcast_ref::<Int64Array>())
                .collect();
            if let Some(probe_arrs) = probe_bufs {
                let hashes = vectorized_hash::hash_arrays(probe_key_arrays, num_rows);
                for probe_row in 0..num_rows {
                    if probe_arrs.iter().any(|a| a.is_null(probe_row)) {
                        continue;
                    }
                    let bucket = hashes[probe_row] as usize & self.mask;
                    let mut entry = self.heads[bucket];
                    while entry != u32::MAX {
                        let (bb, br) = self.entries[entry as usize];
                        let mut eq = true;
                        for (col, pa) in probe_arrs.iter().enumerate() {
                            if build_bufs[col][bb as usize][br as usize] != pa.value(probe_row) {
                                eq = false;
                                break;
                            }
                        }
                        if eq {
                            emit(bb, br, probe_row as u32);
                        }
                        entry = self.next[entry as usize];
                    }
                }
                return true;
            }
        }
        false
    }

    fn iter_matches<'a>(
        &'a self,
        keys: &'a [ArrayRef],
        num_rows: usize,
    ) -> impl Iterator<Item = (u32, u32, u32)> + 'a {
        let hashes = vectorized_hash::hash_arrays(keys, num_rows);
        let (mut row, mut entry, mut started) = (0usize, u32::MAX, false);
        std::iter::from_fn(move || {
            while row < num_rows {
                if !started {
                    started = true;
                    entry = if vectorized_hash::has_null(keys, row) {
                        u32::MAX
                    } else if let Some((min, max)) = self.direct {
                        match keys[0].as_any().downcast_ref::<Int64Array>() {
                            Some(values)
                                if values.value(row) >= min && values.value(row) <= max =>
                            {
                                self.heads[(values.value(row) - min) as usize]
                            }
                            _ => u32::MAX,
                        }
                    } else {
                        self.heads[hashes[row] as usize & self.mask]
                    };
                }
                if entry == u32::MAX {
                    row += 1;
                    started = false;
                    continue;
                }
                let (bb, br) = self.entries[entry as usize];
                entry = self.next[entry as usize];
                if self.direct.is_some()
                    || vectorized_hash::compare_row(
                        &self.build_key_arrays[bb as usize],
                        br as usize,
                        keys,
                        row,
                    )
                {
                    return Some((bb, br, row as u32));
                }
            }
            None
        })
    }

    fn probe_batch(
        &self,
        probe_key_arrays: &[ArrayRef],
        num_rows: usize,
    ) -> Result<Vec<(u32, u32, u32)>> {
        let mut matches = Vec::new();
        fn push(matches: &mut Vec<(u32, u32, u32)>, entry: (u32, u32, u32)) -> Result<()> {
            // Legacy callers materialize candidates. Keep this allocation
            // finite and fallible even when they bypass execute's admission.
            if matches.len() == 1_048_576 {
                return Err(crate::error::QueryError::Execution(
                    "HashJoin legacy candidate memory bound exceeded; bounded output support required".into(),
                ));
            }
            if matches.len() == matches.capacity() {
                matches.try_reserve(1).map_err(|error| {
                    crate::error::QueryError::Execution(format!(
                        "HashJoin legacy candidate allocation refused: {error}"
                    ))
                })?;
            }
            matches.push(entry);
            Ok(())
        }

        // Direct-address probe: bounds check + slot load; chain entries are
        // exactly equal keys, so no hashing and no comparisons.
        if let Some((kmin, kmax)) = self.direct {
            if let Some(pa) = probe_key_arrays[0].as_any().downcast_ref::<Int64Array>() {
                let vals = pa.values();
                let nulls = pa.nulls();
                for probe_row in 0..num_rows {
                    if let Some(nb) = nulls {
                        if !nb.is_valid(probe_row) {
                            continue;
                        }
                    }
                    let k = vals[probe_row];
                    if k < kmin || k > kmax {
                        continue;
                    }
                    let mut entry = self.heads[(k - kmin) as usize];
                    while entry != u32::MAX {
                        let (bb, br) = self.entries[entry as usize];
                        push(&mut matches, (bb, br, probe_row as u32))?;
                        entry = self.next[entry as usize];
                    }
                }
                return Ok(matches);
            }
        }

        let hashes = vectorized_hash::hash_arrays(probe_key_arrays, num_rows);

        // Fast path: raw i64 comparisons, no per-candidate downcasts.
        if let Some(build_bufs) = &self.i64_key_bufs {
            let probe_bufs: Option<Vec<&Int64Array>> = probe_key_arrays
                .iter()
                .map(|a| a.as_any().downcast_ref::<Int64Array>())
                .collect();
            if let Some(probe_arrs) = probe_bufs {
                for probe_row in 0..num_rows {
                    if probe_arrs.iter().any(|a| a.is_null(probe_row)) {
                        continue;
                    }
                    let bucket = hashes[probe_row] as usize & self.mask;
                    let mut entry = self.heads[bucket];
                    while entry != u32::MAX {
                        let (bb, br) = self.entries[entry as usize];
                        let mut eq = true;
                        for (col, pa) in probe_arrs.iter().enumerate() {
                            if build_bufs[col][bb as usize][br as usize] != pa.value(probe_row) {
                                eq = false;
                                break;
                            }
                        }
                        if eq {
                            push(&mut matches, (bb, br, probe_row as u32))?;
                        }
                        entry = self.next[entry as usize];
                    }
                }
                return Ok(matches);
            }
        }

        for probe_row in 0..num_rows {
            // Skip null probe keys
            if vectorized_hash::has_null(probe_key_arrays, probe_row) {
                continue;
            }

            let bucket = hashes[probe_row] as usize & self.mask;
            let mut entry = self.heads[bucket];
            while entry != u32::MAX {
                let (build_batch, build_row) = self.entries[entry as usize];
                // Verify equality (hash collision resolution)
                let build_keys = &self.build_key_arrays[build_batch as usize];
                if vectorized_hash::compare_row(
                    build_keys,
                    build_row as usize,
                    probe_key_arrays,
                    probe_row,
                ) {
                    push(&mut matches, (build_batch, build_row, probe_row as u32))?;
                }
                entry = self.next[entry as usize];
            }
        }

        Ok(matches)
    }

    /// Whether `for_each_i64_candidate` point lookups work on this table:
    /// a single Int64 key in direct-address mode, or hash mode with the
    /// zero-copy i64 key views available for verification.
    fn supports_i64_lookup(&self) -> bool {
        self.direct.is_some()
            || self
                .i64_key_bufs
                .as_ref()
                .map(|b| b.len() == 1)
                .unwrap_or(false)
    }

    /// Visit the (batch_idx, row_idx) candidates whose key equals `key`.
    /// The callback returns true to STOP the walk. Caller must have checked
    /// `supports_i64_lookup`. Filtered Semi/Anti probes use this instead of a
    /// separately built HashMap<i64, Vec<HashEntry>> — the chained VHT
    /// already holds the same candidates without per-key heap boxes (Q21
    /// spent ~70ms building i64 maps its probes barely touched).
    #[inline]
    fn for_each_i64_candidate(&self, key: i64, mut f: impl FnMut(u32, u32) -> bool) {
        if let Some((kmin, kmax)) = self.direct {
            if key < kmin || key > kmax {
                return;
            }
            let mut entry = self.heads[(key - kmin) as usize];
            while entry != u32::MAX {
                let (bb, br) = self.entries[entry as usize];
                if f(bb, br) {
                    return;
                }
                entry = self.next[entry as usize];
            }
            return;
        }
        let bufs = match &self.i64_key_bufs {
            Some(b) => b,
            None => return,
        };
        let bucket = vectorized_hash::hash_i64(key) as usize & self.mask;
        let mut entry = self.heads[bucket];
        while entry != u32::MAX {
            let (bb, br) = self.entries[entry as usize];
            if bufs[0][bb as usize][br as usize] == key && f(bb, br) {
                return;
            }
            entry = self.next[entry as usize];
        }
    }

    /// Probe for Semi/Anti joins: returns a boolean mask per probe row indicating match.
    #[inline]
    fn probe_batch_semi(&self, probe_key_arrays: &[ArrayRef], num_rows: usize) -> Vec<bool> {
        let mut matched = vec![false; num_rows];

        // Direct-address: membership = slot occupancy, no hash/compare.
        if let Some((kmin, kmax)) = self.direct {
            if let Some(pa) = probe_key_arrays[0].as_any().downcast_ref::<Int64Array>() {
                let vals = pa.values();
                let nulls = pa.nulls();
                for probe_row in 0..num_rows {
                    if let Some(nb) = nulls {
                        if !nb.is_valid(probe_row) {
                            continue;
                        }
                    }
                    let k = vals[probe_row];
                    if k >= kmin && k <= kmax {
                        matched[probe_row] = self.heads[(k - kmin) as usize] != u32::MAX;
                    }
                }
                return matched;
            }
        }

        let hashes = vectorized_hash::hash_arrays(probe_key_arrays, num_rows);

        for probe_row in 0..num_rows {
            if vectorized_hash::has_null(probe_key_arrays, probe_row) {
                continue;
            }

            let bucket = hashes[probe_row] as usize & self.mask;
            let mut entry = self.heads[bucket];
            while entry != u32::MAX {
                let (build_batch, build_row) = self.entries[entry as usize];
                let build_keys = &self.build_key_arrays[build_batch as usize];
                if vectorized_hash::compare_row(
                    build_keys,
                    build_row as usize,
                    probe_key_arrays,
                    probe_row,
                ) {
                    matched[probe_row] = true;
                    break; // One match is enough for Semi/Anti
                }
                entry = self.next[entry as usize];
            }
        }

        matched
    }

    /// Build-side-output Semi/Anti (`!swapped`, no ON filter): set the
    /// matched bit of EVERY build row whose key equals some probe key.
    ///
    /// Complexity matters here in a way it does not for the probe-side
    /// orientation: with heavily duplicated keys (a 30M-row native
    /// `lineitem` build over 7 `l_shipmode` values) enumerating every
    /// candidate per probe row is O(probe_rows x build_rows / NDV) — the
    /// task-001 correctness fix, done naively, turned a 4s query into one
    /// that did not finish in 10 minutes. The bits are monotonic and every
    /// walk marks a chain in the SAME order, so a probe row whose FIRST
    /// equal-key entry is already marked can stop: the prober that marked
    /// it walked (or is walking, on another thread) the rest of that
    /// chain. Cost becomes O(probe_rows + build_rows). Redundant walks by
    /// two threads racing on the same fresh key are harmless.
    fn mark_build_matches(
        &self,
        probe_key_arrays: &[ArrayRef],
        num_rows: usize,
        marked: &[Vec<std::sync::atomic::AtomicBool>],
    ) {
        use std::sync::atomic::Ordering;
        // Direct-address mode: the slot's chain holds exactly-equal keys.
        if let Some((kmin, kmax)) = self.direct {
            if let Some(pa) = probe_key_arrays[0].as_any().downcast_ref::<Int64Array>() {
                let vals = pa.values();
                let nulls = pa.nulls();
                for probe_row in 0..num_rows {
                    if let Some(nb) = nulls {
                        if !nb.is_valid(probe_row) {
                            continue;
                        }
                    }
                    let k = vals[probe_row];
                    if k < kmin || k > kmax {
                        continue;
                    }
                    let mut entry = self.heads[(k - kmin) as usize];
                    let mut first = true;
                    while entry != u32::MAX {
                        let (bb, br) = self.entries[entry as usize];
                        let cell = &marked[bb as usize][br as usize];
                        if first && cell.load(Ordering::Relaxed) {
                            break;
                        }
                        first = false;
                        cell.store(true, Ordering::Relaxed);
                        entry = self.next[entry as usize];
                    }
                }
                return;
            }
        }

        let hashes = vectorized_hash::hash_arrays(probe_key_arrays, num_rows);
        for probe_row in 0..num_rows {
            if vectorized_hash::has_null(probe_key_arrays, probe_row) {
                continue;
            }
            let bucket = hashes[probe_row] as usize & self.mask;
            let mut entry = self.heads[bucket];
            let mut first = true;
            while entry != u32::MAX {
                let (bb, br) = self.entries[entry as usize];
                let build_keys = &self.build_key_arrays[bb as usize];
                if vectorized_hash::compare_row(
                    build_keys,
                    br as usize,
                    probe_key_arrays,
                    probe_row,
                ) {
                    let cell = &marked[bb as usize][br as usize];
                    if first && cell.load(Ordering::Relaxed) {
                        break;
                    }
                    first = false;
                    cell.store(true, Ordering::Relaxed);
                }
                entry = self.next[entry as usize];
            }
        }
    }
}

/// Hash join execution operator
pub struct HashJoinExec {
    left: Arc<dyn PhysicalOperator>,
    right: Arc<dyn PhysicalOperator>,
    on: Vec<(Expr, Expr)>,
    join_type: JoinType,
    schema: SchemaRef,
    /// Runtime filter slot on the probe-side scan: after the build side is
    /// hashed, publish the key set so the scan decodes only matching rows.
    /// Inner joins only (planner-gated).
    pub probe_runtime_filter: Option<crate::physical::operators::SharedRuntimeFilter>,
    /// Which equi pair the runtime filter applies to (multi-key joins
    /// publish a partial filter on one pair — a correct superset).
    pub probe_runtime_filter_pair: usize,
    /// Optional filter to evaluate during the join (required for Semi/Anti joins with filters)
    filter: Option<Expr>,
    /// Schema combining left and right for filter evaluation
    combined_schema: SchemaRef,
    /// Cached build side - computed once, shared across all partition executions
    build_cache: OnceCell<Arc<BuildSideCache>>,
    outer_round: std::sync::Mutex<Option<Arc<outer_probe::Round>>>,
    memory_pool: crate::execution::SharedMemoryPool,
    /// When true, build hash table from right side (smaller) instead of left.
    /// Used for Left joins where the right side is much smaller than the left.
    build_right: bool,
    /// Join-output retention mask over the FULL (left ++ right) column
    /// order: false = referenced by NOTHING above this join (typically an
    /// ON-only key). Pruned columns are dropped from the output schema and
    /// never gathered — HJ_PROF put gather+batch at ~75% of Q9's probe
    /// pipeline, and 2 of the partsupp build's 3 columns were dead weight.
    /// Set only for Inner, unfiltered joins by the planner's usage pass.
    retained: Option<Vec<bool>>,
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
            .field("build_right", &self.build_right)
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
            outer_round: std::sync::Mutex::new(None),
            memory_pool: crate::execution::process_memory_pool(),
            build_right: false,
            probe_runtime_filter: None,
            probe_runtime_filter_pair: 0,
            retained: None,
        }
    }

    /// Set admission for future initialization. Existing initialized caches
    /// retain their original reservation owner; this never rebinds live memory.
    pub fn with_memory_pool(mut self, pool: crate::execution::SharedMemoryPool) -> Self {
        self.memory_pool = pool;
        self
    }

    /// Set build_right flag: when true, build hash table from right side.
    /// This is useful for Left joins where the right side is much smaller.
    /// Test-only: whether the (already executed) build side produced a
    /// `VectorizedHashTable`, i.e. the join took the vectorized path rather
    /// than the generic-map fallback. `None` before the build ran.
    #[cfg(test)]
    fn build_used_vectorized_table(&self) -> Option<bool> {
        self.build_cache.get().map(|c| c.vectorized_ht.is_some())
    }

    pub fn with_build_right(mut self, build_right: bool) -> Self {
        self.build_right = build_right;
        self
    }

    /// Apply a join-output retention mask (see the `retained` field). The
    /// output schema shrinks to the kept columns; a mask whose length does
    /// not match the full combined width is ignored (Semi/Anti schemas).
    ///
    /// Gate condition — MUST stay identical to `SpillableHashJoinExec::
    /// set_retained` and the planner's `analyze_join_output_usage` (three
    /// gates that must move in lockstep; `SpillableHashJoinExec` delegates
    /// to an inner `HashJoinExec` and calls this same method, so a gate that
    /// diverges here silently un-prunes what the wrapper already narrowed
    /// its own schema to — see the dedicated Spillable-level regression
    /// test). Semi/Anti/Cross are excluded: Semi/Anti's schema is left-only
    /// width (a mask sized for left++right never matches it) and Cross has
    /// no ON-clause. A filter containing a subquery can reference columns
    /// this operator's own filter-column bookkeeping never sees, so it
    /// declines too (the planner already never hands one down, but the
    /// operator-level gate stays independently correct).
    pub fn set_retained(&mut self, mask: Option<Vec<bool>>) {
        let Some(m) = mask else {
            return;
        };
        let type_ok = matches!(
            self.join_type,
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
        );
        let filter_ok = self
            .filter
            .as_ref()
            .map(|f| !f.contains_subquery())
            .unwrap_or(true);
        if !type_ok
            || !filter_ok
            || m.len() != self.combined_schema.fields().len()
            || m.len() != self.schema.fields().len()
        {
            return;
        }
        let fields: Vec<_> = self
            .schema
            .fields()
            .iter()
            .zip(&m)
            .filter(|(_, keep)| **keep)
            .map(|(f, _)| f.clone())
            .collect();
        self.schema = Arc::new(Schema::new(fields));
        self.retained = Some(m);
    }
}

impl HashJoinExec {
    /// Construct the shared pull-driven Inner stream without polling its input.
    fn inner_probe_stream(
        &self,
        cache: Arc<BuildSideCache>,
        input: RecordBatchStream,
        probe_keys: Vec<Expr>,
        swapped: bool,
    ) -> RecordBatchStream {
        let left_len = self.left.schema().fields().len();
        let probe_keep = self.retained.as_ref().map(|mask| {
            if swapped {
                mask[..left_len].to_vec()
            } else {
                mask[left_len..].to_vec()
            }
        });
        let profile = JoinProfile::new("inner_probe", &probe_keys);
        let state = InnerProbeStream {
            profile,
            cache,
            input,
            batch: None,
            keys: Vec::new(),
            hashes: Vec::new(),
            i64_keys: None,
            key_nulls: Vec::new(),
            row: 0,
            entry: u32::MAX,
            started: false,
            key_exprs: probe_keys,
            swapped,
            schema: self.schema.clone(),
            filter: self.filter.clone(),
            combined_schema: self.combined_schema.clone(),
            probe_keep,
        };
        Box::pin(stream::try_unfold(state, |mut state| async move {
            match state.next_batch().await? {
                Some(batch) => Ok(Some((batch, state))),
                None => Ok(None),
            }
        }))
    }

    /// Complete the shared build phase without starting any probe/output stream.
    /// Partition tasks are owned by the initialization future; cancelled attempts
    /// leave OnceCell retryable. This is not a prepared output-byte guarantee.
    async fn ensure_build_cache(
        &self,
        build_side: &Arc<dyn PhysicalOperator>,
        build_keys: &[Expr],
        swapped: bool,
    ) -> Result<&Arc<BuildSideCache>> {
        let can_skip_generic_ht = true;
        self
            .build_cache
            .get_or_try_init(|| async {
                let mut profile = JoinProfile::new("build_initialization", build_keys);
                JoinProfile::phase(&mut profile, 0);
                debug_log(&format!(
                    "CACHE MISS: Building hash table for join_type={:?}",
                    self.join_type
                ));

                // Collect ALL partitions from the build side
                let partition_results =
                    collect_join_partitions(build_side.clone(), "Build").await?;
                JoinProfile::phase(&mut profile, 7);

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

                if let Some(profile) = &mut profile {
                    profile.input_rows = total_build_rows;
                    profile.input_batches = build_batches.len();
                }

                // Join-output pruning: the build side's contribution to the
                // gather. `retained` indexes (left ++ right); the build side
                // is left or right per `swapped`.
                let left_len = self.left.schema().fields().len();
                let build_keep: Option<Vec<bool>> = self.retained.as_ref().map(|m| {
                    if swapped {
                        m[left_len..].to_vec()
                    } else {
                        m[..left_len].to_vec()
                    }
                });
                let prune = prune_batch_columns;
                // Row store and gather work over the PRUNED build columns:
                // dropping ON-only keys shrinks the row-store stride (Q9's
                // partsupp: 3 cols -> 1) and can even make string-carrying
                // builds row-store-eligible once the strings are dropped.
                let rs_batches: Vec<RecordBatch> = match &build_keep {
                    Some(keep) if keep.iter().any(|k| !k) => {
                        build_batches.iter().map(|b| prune(b, keep)).collect()
                    }
                    _ => build_batches.clone(),
                };

                // Row-store eligibility: Inner join, no join filter, build
                // side not swapped, every build column fixed-width
                // (Int64/Float64/Int32/Date32) and null-free across all
                // batches, and a build side big enough for gather locality to
                // matter. When eligible the columnar concat below is REPLACED
                // by a row-major store: the same single copy at build time,
                // but the joined-batch gather then costs one random read per
                // matched ROW instead of one per matched row PER COLUMN.
                let row_store_eligible = self.join_type == JoinType::Inner
                    && self.filter.is_none()
                    && !swapped
                    && total_build_rows >= 100_000
                    && !rs_batches.is_empty()
                    && rs_batches[0].num_columns() > 0
                    && rs_batches.iter().all(|b| {
                        b.columns().iter().all(|c| {
                            c.null_count() == 0
                                && matches!(
                                    c.data_type(),
                                    arrow::datatypes::DataType::Int64
                                        | arrow::datatypes::DataType::Float64
                                        | arrow::datatypes::DataType::Int32
                                        | arrow::datatypes::DataType::Date32
                                )
                        })
                    });

                // Concatenate the build side into a single batch ONCE.
                // gather_column's multi-batch path re-concatenates the ENTIRE
                // build side per output batch per column — with a 15M-row build
                // side and thousands of probe batches that alone accounted for
                // ~50s on lineitem x orders at SF=10. A single build batch also
                // sends every downstream gather through the direct-take fast
                // path. On concat failure (e.g. >2GB Utf8 offset overflow) keep
                // the chunked layout; the slow-but-correct path still works.
                // Row-store-eligible builds skip the concat entirely: the hash
                // tables index the unconcatenated batches and the row store
                // serves the build-column gather instead.
                let build_batches = if !row_store_eligible && build_batches.len() > 1 {
                    match arrow::compute::concat_batches(&build_batches[0].schema(), &build_batches)
                    {
                        Ok(single) => vec![single],
                        Err(_) => build_batches,
                    }
                } else {
                    build_batches
                };

                if std::env::var("RS_DEBUG").is_ok() && !row_store_eligible {
                    eprintln!(
                        "[rs] ineligible: inner={} filter_none={} swapped={} rows={} cols={:?} nulls={:?}",
                        self.join_type == JoinType::Inner,
                        self.filter.is_none(),
                        swapped,
                        total_build_rows,
                        build_batches
                            .first()
                            .map(|b| b
                                .columns()
                                .iter()
                                .map(|c| c.data_type().clone())
                                .collect::<Vec<_>>()),
                        build_batches
                            .first()
                            .map(|b| b
                                .columns()
                                .iter()
                                .map(|c| c.null_count())
                                .collect::<Vec<_>>()),
                    );
                }
                let row_store = if row_store_eligible {
                    Some(RowStore::build(&rs_batches, &self.memory_pool)?)
                } else {
                    None
                };

                // Try vectorized hash table first (supports all key types)
                // Don't build for empty keys (cross joins) — they use the generic cartesian path.
                let timing = std::env::var("HJ_TIMING").is_ok();
                let t_vht = std::time::Instant::now();
                let vectorized_ht = if !build_keys.is_empty() {
                    VectorizedHashTable::build(&build_batches, &build_keys, &self.memory_pool)?
                } else {
                    None
                };
                if timing {
                    eprintln!(
                        "[hj] build drain: {} rows, {} batches; vht build: {:?}; vht={} row_store={} build_keys={:?} on={:?} swapped={}",
                        total_build_rows,
                        build_batches.len(),
                        t_vht.elapsed(),
                        vectorized_ht.is_some(),
                        row_store.is_some(),
                        build_keys.iter().map(|k| k.to_string()).collect::<Vec<_>>(),
                        self.on.iter().map(|(l, r)| format!("{}={}", l, r)).collect::<Vec<_>>(),
                        swapped
                    );
                }

                // Specialized i64 hash table: only needed on paths the vectorized
                // table doesn't serve (VHT probe requires filter.is_none()). It is a
                // HashMap with one heap-allocated Vec per distinct key — on a 15M-row
                // build side that's tens of seconds of pure waste if never probed.
                // Filtered Semi/Anti probes can take candidates straight from
                // the VHT when it supports single-i64 point lookups — the i64
                // map would be built (per-key Vec allocs over the whole build)
                // and barely probed. probe_semi_anti_parallel rebuilds a local
                // map if its VHT path turns out unusable at probe time.
                let vht_serves_semi_anti = matches!(self.join_type, JoinType::Semi | JoinType::Anti)
                    && vectorized_ht
                        .as_ref()
                        .map(|v| v.supports_i64_lookup())
                        .unwrap_or(false);
                // Left/Right/Full apply the ON predicate to candidate pairs
                // inside the vectorized probe, so a filtered outer join has no
                // use for the i64 map either.
                let vht_serves_filtered_join = vht_serves_semi_anti
                    || (vectorized_ht.is_some()
                        && matches!(
                            self.join_type,
                            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
                        ));
                let i64_needed = vectorized_ht.is_none()
                    || (self.filter.is_some() && !vht_serves_filtered_join);
                let t_i64 = std::time::Instant::now();
                let i64_hash_table = if i64_needed && build_keys.len() == 1 {
                    build_i64_hash_table(&build_batches, &build_keys[0])
                } else {
                    None
                };
                if timing && i64_needed && build_keys.len() == 1 {
                    eprintln!(
                        "[hj] i64 ht build: {:?} ({} rows)",
                        t_i64.elapsed(),
                        total_build_rows
                    );
                }

                // Skip expensive generic hash table build when vectorized or i64 fast path is available
                let hash_table = if self.join_type == JoinType::Inner || vectorized_ht.is_some()
                    || (i64_hash_table.is_some() && can_skip_generic_ht)
                {
                    HashMap::new()
                } else {
                    build_hash_table(&build_batches, &build_keys)?
                };

                // Join types whose BUILD side rows must survive unmatched:
                // Left with build=left, Right with build=right, Full always.
                let needs_build_tracking = match self.join_type {
                    JoinType::Left => !swapped,
                    JoinType::Right => swapped,
                    JoinType::Full => true,
                    _ => false,
                };
                let build_matched = if needs_build_tracking {
                    Some(
                        build_batches
                            .iter()
                            .map(|b| {
                                (0..b.num_rows())
                                    .map(|_| std::sync::atomic::AtomicBool::new(false))
                                    .collect::<Vec<_>>()
                            })
                            .collect::<Vec<_>>(),
                    )
                } else {
                    None
                };

                // With a retention mask, the cached batches keep only the
                // gather-relevant columns — every consumer reachable for an
                // Inner, unfiltered join (the only shape the planner masks)
                // reads them for gather or row counts only; the hash tables
                // above were built from the full batches and are
                // self-contained.
                let build_batches = match &build_keep {
                    Some(keep) if keep.iter().any(|k| !k) => {
                        build_batches.iter().map(|b| prune(b, keep)).collect()
                    }
                    _ => build_batches,
                };
                // Publish only the exact arrays already used by the hash table.
                // Optional filter admission cannot replay computed/volatile keys.
                if let (Some(slot), Some(table)) = (&self.probe_runtime_filter, &vectorized_ht) {
                    if let Some(filter) = super::runtime_filter::prepare(
                        &table.build_key_arrays, self.probe_runtime_filter_pair, &self.memory_pool,
                    ) {
                        *slot.lock() = Some(Arc::new(
                            super::streaming_parquet_scan::RuntimeFilterPayload::Admitted(filter),
                        ));
                    }
                }
                if let Some(profile) = &mut profile {
                    profile.completed = true;
                }
                Ok::<_, crate::error::QueryError>(Arc::new(BuildSideCache {
                    batches: build_batches,
                    hash_table,
                    i64_hash_table,
                    vectorized_ht,
                    build_matched,
                    completed_partitions: std::sync::atomic::AtomicUsize::new(0),
                    row_store,
                }))
            })
            .await
    }
}

/// Only the actual nonpreserved probe output may carry a pruning target.
/// Retained output ordinals must be restored before choosing the child.
pub(crate) fn runtime_filter_probe_ordinal(
    join_type: JoinType,
    build_right: bool,
    retained: Option<&[bool]>,
    left_width: usize,
    right_width: usize,
    output: usize,
) -> Option<usize> {
    if !matches!(join_type, JoinType::Inner) && !(join_type == JoinType::Left && !build_right) {
        return None;
    }
    let width = left_width.checked_add(right_width)?;
    let original = match retained {
        Some(mask) if mask.len() == width => {
            mask.iter()
                .enumerate()
                .filter(|(_, keep)| **keep)
                .nth(output)?
                .0
        }
        Some(_) => return None,
        None if output < width => output,
        None => return None,
    };
    if build_right {
        (original < left_width).then_some(original)
    } else if original >= left_width {
        Some(original - left_width)
    } else {
        None
    }
}

#[async_trait]
impl PhysicalOperator for HashJoinExec {
    fn runtime_filter_target(
        &self,
        output: usize,
    ) -> Option<crate::physical::plan::RuntimeFilterTarget> {
        let index = runtime_filter_probe_ordinal(
            self.join_type,
            self.build_right,
            self.retained.as_deref(),
            self.left.schema().fields().len(),
            self.right.schema().fields().len(),
            output,
        )?;
        let probe = if self.build_right {
            &self.left
        } else {
            &self.right
        };
        probe.runtime_filter_target(index)
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.left.clone(), self.right.clone()]
    }

    async fn prepare_admitted_queue_input(
        &self,
        pool: crate::execution::SharedMemoryPool,
    ) -> Result<Option<crate::physical::PreparedAdmittedInput>> {
        if !outer_probe::admitted_eligible(self)
            || self.filter.is_some()
            || !Arc::ptr_eq(&pool, &self.memory_pool)
        {
            return Ok(None);
        }
        let swapped = self.build_right || self.join_type == JoinType::Right;
        let (build, probe) = if swapped {
            (&self.right, &self.left)
        } else {
            (&self.left, &self.right)
        };
        let (left, right): (Vec<_>, Vec<_>) = self.on.iter().cloned().unzip();
        let (build_keys, probe_keys) = if swapped {
            (right, left)
        } else {
            (left, right)
        };
        let cache = self.ensure_build_cache(build, &build_keys, swapped).await?;
        if cache.vectorized_ht.is_none() && cache.batches.iter().any(|b| b.num_rows() != 0) {
            return Ok(None);
        }
        // A copied-output bound alone does not prove ownership of future
        // child decoder/scratch allocations. Preserve the admitted capability
        // transitively; an unsupported child keeps ordinary serial consumption.
        let Some(prepared) = probe.prepare_admitted_queue_input(pool.clone()).await? else {
            return Ok(None);
        };
        if !prepared.pool.is_within(&pool)
            || prepared.streams.as_slice().len() != self.output_partitions()
        {
            return Err(QueryError::Execution(
                "bounded outer join child pool/partition mismatch".into(),
            ));
        }
        let runtime_pool = prepared.pool.clone();
        let mut streams = crate::execution::reserved_vec::ReservedVec::with_capacity(
            &pool,
            self.output_partitions(),
        )?;
        for (partition, input) in prepared.streams.into_owned_iter().enumerate() {
            streams.extend_reserved(
                1,
                [outer_probe::stream(
                    self,
                    cache.clone(),
                    input,
                    probe_keys.clone(),
                    swapped,
                    partition,
                    true,
                    runtime_pool.clone(),
                )?],
            )?;
        }
        Ok(Some(crate::physical::PreparedAdmittedInput {
            pool: runtime_pool,
            streams,
        }))
    }

    async fn prepare_queue_input(&self) -> Result<Option<crate::physical::PreparedQueueInput>> {
        if self.join_type != JoinType::Inner
            || self.filter.as_ref().is_some_and(Expr::contains_subquery)
            || self
                .on
                .iter()
                .any(|(left, right)| left.contains_subquery() || right.contains_subquery())
        {
            return Ok(None);
        }
        let (build, probe, swapped) = if self.build_right {
            (&self.right, &self.left, true)
        } else {
            (&self.left, &self.right, false)
        };
        if probe.output_partitions() != self.output_partitions() {
            return Ok(None);
        }
        let (left_keys, right_keys): (Vec<_>, Vec<_>) = self.on.iter().cloned().unzip();
        let (build_keys, probe_keys) = if swapped {
            (right_keys, left_keys)
        } else {
            (left_keys, right_keys)
        };
        let cache = self.ensure_build_cache(build, &build_keys, swapped).await?;
        // Ordinary multi-batch columnar gather remains unsupported. An owned,
        // validated row store has its own fixed-width physical output proof.
        if (cache.row_store.is_none() && cache.batches.len() != 1)
            || cache.vectorized_ht.is_none()
            || cache.batches.iter().all(|batch| batch.num_rows() == 0)
        {
            return Ok(None);
        }
        let row_store_schema = if let Some(store) = &cache.row_store {
            let Some(first) = cache.batches.first() else {
                return Ok(None);
            };
            if first.num_columns() != store.cols.len()
                || first
                    .columns()
                    .iter()
                    .zip(&store.cols)
                    .any(|(array, (_, _, dt))| array.data_type() != dt)
            {
                return Err(row_store_error(
                    "retained output schema differs from validated store",
                ));
            }
            Some(first.schema())
        } else {
            None
        };
        let left_len = self.left.schema().fields().len();
        let probe_keep = self.retained.as_ref().map(|mask| {
            if swapped {
                &mask[..left_len]
            } else {
                &mask[left_len..]
            }
        });
        // Finish our own build/factory checks before recursively initializing
        // the probe. A child Some owns initialized, unpulled streams even when
        // its output proof is Unknown; never drop it and execute again.
        let (probe_streams, output) = if let Some(prepared) = probe.prepare_queue_input().await? {
            if prepared.streams.len() != probe.output_partitions() {
                return Err(crate::error::QueryError::Execution(
                    "prepared join probe partition contract violated".into(),
                ));
            }
            let bound = match &prepared.output {
                crate::physical::PreparedOutputBound::Layouts(layouts) => {
                    if let Some(build_schema) = &row_store_schema {
                        crate::physical::queue_layout::row_store_prepared_output_copy_bound(
                            build_schema,
                            layouts,
                            probe_keep,
                            swapped,
                            &self.schema,
                            INNER_CANDIDATE_ROWS,
                        )
                    } else {
                        crate::physical::queue_layout::inner_prepared_output_copy_bound(
                            &cache.batches,
                            layouts,
                            probe_keep,
                            swapped,
                            &self.schema,
                            INNER_CANDIDATE_ROWS,
                        )
                    }
                }
                _ => None,
            };
            (
                prepared.streams,
                bound
                    .map(crate::physical::PreparedOutputBound::Layouts)
                    .unwrap_or(crate::physical::PreparedOutputBound::Unknown),
            )
        } else {
            let Some(probe_bound) = probe.pool_independent_gather_copy_bound() else {
                return Ok(None);
            };
            let bound = if let Some(build_schema) = &row_store_schema {
                crate::physical::queue_layout::row_store_output_copy_bound(
                    build_schema,
                    &probe_bound,
                    probe_keep,
                    swapped,
                    &self.schema,
                    INNER_CANDIDATE_ROWS,
                )
            } else {
                crate::physical::queue_layout::inner_output_copy_bound(
                    &cache.batches,
                    &probe_bound,
                    None,
                    probe_keep,
                    swapped,
                    &self.schema,
                    INNER_CANDIDATE_ROWS,
                )
            };
            let Some(bound) = bound else {
                return Ok(None);
            };
            let mut inputs = Vec::new();
            inputs
                .try_reserve_exact(probe.output_partitions())
                .map_err(|e| {
                    crate::error::QueryError::Execution(format!(
                        "prepared probe stream allocation failed: {e}"
                    ))
                })?;
            for partition in 0..probe.output_partitions() {
                inputs.push(probe.execute(partition).await?);
            }
            (inputs, crate::physical::PreparedOutputBound::Layouts(bound))
        };
        // No producer task or output pull. If initialization fails or is
        // cancelled, this owned Vec drops all previously initialized streams.
        let mut streams = Vec::new();
        streams
            .try_reserve_exact(probe.output_partitions())
            .map_err(|e| {
                crate::error::QueryError::Execution(format!(
                    "prepared join stream allocation failed: {e}"
                ))
            })?;
        for input in probe_streams {
            streams.push(self.inner_probe_stream(
                Arc::clone(cache),
                input,
                probe_keys.clone(),
                swapped,
            ));
        }
        Ok(Some(crate::physical::PreparedQueueInput {
            streams,
            output,
        }))
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        crate::physical::check_partition(self, partition)?;

        debug_log(&format!(
            "execute() partition={} join_type={:?}",
            partition, self.join_type
        ));

        // Determine build and probe sides
        // For Right join: always build from right.
        // For Left join with build_right=true: build from right (smaller side).
        // Otherwise: build from left.
        let (build_side, probe_side, swapped) =
            if self.build_right || matches!(self.join_type, JoinType::Right) {
                (&self.right, &self.left, true)
            } else {
                (&self.left, &self.right, false)
            };

        let (on_left, on_right): (Vec<_>, Vec<_>) = self.on.iter().cloned().unzip();
        let build_keys = if swapped {
            on_right.clone()
        } else {
            on_left.clone()
        };
        let probe_keys = if swapped { &on_left } else { &on_right };

        // For Semi/Anti joins, start probe collection early so it overlaps
        // with the build side — UNLESS this join publishes a runtime filter
        // into the probe-side scan. Prefetch would start decoding before the
        // build exists, so the filter (published only after the build drains)
        // could never prune anything: Q21's l2/l3 lineitem probes decoded
        // ~35M rows each where the bitmap admits ~2.5M. With a linked filter
        // the probe is collected AFTER the build instead (see below); the
        // bitmap-pruned decode is far cheaper than what overlap saved.
        let defer_probe_for_filter = self.probe_runtime_filter.is_some();
        let probe_prefetch_handle = if matches!(self.join_type, JoinType::Semi | JoinType::Anti)
            && !defer_probe_for_filter
        {
            let probe = probe_side.clone();
            let mut prefetch = tokio::task::JoinSet::new();
            prefetch.spawn(async move {
                Ok::<_, crate::error::QueryError>(
                    collect_join_partitions(probe, "Probe")
                        .await?
                        .into_iter()
                        .flatten()
                        .collect::<Vec<_>>(),
                )
            });
            Some(prefetch)
        } else {
            None
        };

        // Collected variants finish their work before returning a stream. Keep
        // cache waiting separate from the initializer's existing build trace.
        let mut collected_profile = match self.join_type {
            _ if outer_probe::eligible(self) => None,
            JoinType::Inner => None,
            JoinType::Left => JoinProfile::new("collected_left", probe_keys),
            JoinType::Right => JoinProfile::new("collected_right", probe_keys),
            JoinType::Full => JoinProfile::new("collected_full", probe_keys),
            JoinType::Cross => JoinProfile::new("collected_cross", probe_keys),
            JoinType::Semi => JoinProfile::new("collected_semi", probe_keys),
            JoinType::Anti => JoinProfile::new("collected_anti", probe_keys),
            JoinType::Single => JoinProfile::new("collected_single", probe_keys),
            JoinType::Mark => JoinProfile::new("collected_mark", probe_keys),
        };
        JoinProfile::phase(&mut collected_profile, 11);
        let cache = self
            .ensure_build_cache(build_side, &build_keys, swapped)
            .await?;

        // Inner joins are pull-driven: neither the probe input nor the output
        // cardinality is materialized before returning the first batch.
        if self.join_type == JoinType::Inner {
            if cache.batches.iter().all(|batch| batch.num_rows() == 0) {
                return Ok(Box::pin(stream::empty()));
            }
            if cache.vectorized_ht.is_none() {
                return Err(crate::error::QueryError::NotImplemented(
                    "HashJoin bounded inner probe requires supported exact vectorized keys".into(),
                ));
            }
            return Ok(self.inner_probe_stream(
                Arc::clone(cache),
                probe_side.execute(partition).await?,
                probe_keys.clone(),
                swapped,
            ));
        }

        if outer_probe::eligible(self)
            && (cache.vectorized_ht.is_some() || cache.batches.iter().all(|b| b.num_rows() == 0))
        {
            // The outer cursor owns completion, not construction of its stream.
            // Do not report a collected-route record for this lazy path.
            return outer_probe::stream(
                self,
                cache.clone(),
                probe_side.execute(partition).await?,
                probe_keys.clone(),
                swapped,
                partition,
                false,
                self.memory_pool.clone(),
            );
        }

        // Collect probe batches. For Semi/Anti, await the prefetched probe data
        // that was running concurrently with the build side — or, when the
        // prefetch was deferred so the published runtime filter can prune the
        // probe-side decode, collect ALL probe partitions now.
        JoinProfile::phase(&mut collected_profile, 0);
        let probe_batches: Vec<RecordBatch> = if let Some(mut prefetch) = probe_prefetch_handle {
            prefetch
                .join_next()
                .await
                .ok_or_else(|| {
                    crate::error::QueryError::Internal(
                        "Probe prefetch completed without a task".into(),
                    )
                })?
                .map_err(|error| {
                    crate::error::QueryError::Execution(format!(
                        "Probe prefetch task failed: {error}"
                    ))
                })??
        } else if matches!(self.join_type, JoinType::Semi | JoinType::Anti) {
            collect_join_partitions(probe_side.clone(), "Probe")
                .await?
                .into_iter()
                .flatten()
                .collect()
        } else {
            let probe_stream = probe_side.execute(partition).await?;
            probe_stream.try_collect().await?
        };

        if let Some(profile) = &mut collected_profile {
            profile.input_batches = probe_batches.len();
            profile.input_rows = probe_batches.iter().map(RecordBatch::num_rows).sum();
        }
        JoinProfile::phase(&mut collected_profile, 8);
        // Safety check: prevent cross join explosions
        let build_rows: usize = cache.batches.iter().map(|b| b.num_rows()).sum();
        let probe_rows: usize = probe_batches.iter().map(|b| b.num_rows()).sum();
        if matches!(
            self.join_type,
            JoinType::Left | JoinType::Right | JoinType::Full | JoinType::Cross
        ) {
            // These legacy variants still gather a complete probe result. Bound
            // their worst-case candidate count before allocating match vectors;
            // clean refusal is safer than trying to recover after allocation.
            const MAX_LEGACY_CANDIDATES: usize = 1_048_576;
            let candidates = if let Some(table) = &cache.vectorized_ht {
                let mut count = 0usize;
                'count: for batch in &probe_batches {
                    // probe_batch owns one candidate vector per input batch,
                    // not one per partition. A finite vector bound must not
                    // become a limit on valid total output cardinality.
                    count = 0;
                    let keys = evaluate_join_keys(batch, probe_keys)?;
                    let hashes = vectorized_hash::hash_arrays(&keys, batch.num_rows());
                    for row in 0..batch.num_rows() {
                        if vectorized_hash::has_null(&keys, row) {
                            continue;
                        }
                        let mut entry = if let Some((min, max)) = table.direct {
                            let Some(values) = keys[0].as_any().downcast_ref::<Int64Array>() else {
                                return Err(crate::error::QueryError::Execution(
                                    "HashJoin candidate admission key domain differs".into(),
                                ));
                            };
                            let key = values.value(row);
                            if key < min || key > max {
                                continue;
                            }
                            table.heads[(key - min) as usize]
                        } else {
                            table.heads[hashes[row] as usize & table.mask]
                        };
                        while entry != u32::MAX {
                            let (bb, br) = table.entries[entry as usize];
                            if table.direct.is_some()
                                || vectorized_hash::compare_row(
                                    &table.build_key_arrays[bb as usize],
                                    br as usize,
                                    &keys,
                                    row,
                                )
                            {
                                count += 1;
                                if count > MAX_LEGACY_CANDIDATES {
                                    break 'count;
                                }
                            }
                            entry = table.next[entry as usize];
                        }
                    }
                }
                count
            } else {
                probe_batches
                    .iter()
                    .map(|batch| build_rows.saturating_mul(batch.num_rows()))
                    .max()
                    .unwrap_or(0)
            };
            if candidates > MAX_LEGACY_CANDIDATES {
                return Err(crate::error::QueryError::Execution(format!(
                    "HashJoin {:?} requires bounded output support: potential candidate count exceeds {}",
                    self.join_type, MAX_LEGACY_CANDIDATES
                )));
            }
        }
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

        JoinProfile::phase(&mut collected_profile, 9);
        let t_probe = std::time::Instant::now();
        let row_store = cache
            .row_store
            .as_ref()
            .map(|rs| (rs, rs.row_offsets.as_slice()));
        // Join-output pruning: the probe side's keep flags (the build side's
        // were applied when the cache pruned its batches).
        let left_len = self.left.schema().fields().len();
        let probe_keep: Option<Vec<bool>> = self.retained.as_ref().map(|m| {
            if swapped {
                m[..left_len].to_vec()
            } else {
                m[left_len..].to_vec()
            }
        });
        let mut result = probe_hash_table(
            &cache.batches,
            &probe_batches,
            &cache.hash_table,
            cache.i64_hash_table.as_ref(),
            cache.vectorized_ht.as_ref(),
            probe_keys,
            self.join_type,
            swapped,
            &self.schema,
            self.filter.as_ref(),
            &self.combined_schema,
            cache.build_matched.as_deref(),
            row_store,
            probe_keep.as_deref(),
        )?;

        JoinProfile::phase(&mut collected_profile, 10);
        // Emit unmatched BUILD rows exactly once PER FULL ROUND: the last
        // probe partition of a round scans the shared matched bits.
        //
        // `build_cache` is a `OnceCell` — the build side (hash table +
        // `matched` bits + this counter) is intentionally computed ONCE and
        // SHARED across every call to `execute()`, however many there are.
        // Almost always that is exactly `output_partitions()` calls, one per
        // partition, and `done == target` on the last one. But
        // `SpillableHashAggregateExec::execute_fused_streaming` (task 008,
        // native-tables-foundation QA) can drive this SAME child through its
        // ENTIRE `0..output_partitions()` range, discover a tripped
        // group-count budget only after every partition already finished
        // (each partition's `execute(p).await` fully computes that
        // partition's probe synchronously, so the round always completes
        // before the caller can observe the abort), and then fall through to
        // `collect_input_partitions_concurrently`, which re-executes the
        // SAME `0..output_partitions()` range a SECOND time. Comparing for
        // exact equality only ever fires on the FIRST such round (`done`
        // sails past `target` on every later round and never lands on it
        // again), so every subsequent round's actually-used output silently
        // lost every unmatched build row — reproduced concretely as TPC-H
        // Q13 losing its "customers with zero orders" bucket (23 rows
        // instead of 24) against native tables at SF=10, the shape that
        // happens to trip the fused path's budget. Checking for a multiple
        // of `target` instead makes each round self-contained: the matched
        // bits reflect the same true match/no-match facts regardless of
        // which round is asking, so recomputing "unmatched" once per round
        // is correct (and free — an abandoned round's own output, unmatched
        // batch included, is simply discarded along with the rest of it).
        if let Some(matched) = &cache.build_matched {
            let done = cache
                .completed_partitions
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            let target = self.output_partitions().max(1);
            if done % target == 0 {
                let mut unmatched: Vec<(usize, usize)> = Vec::new();
                for (batch_idx, flags) in matched.iter().enumerate() {
                    for (row_idx, flag) in flags.iter().enumerate() {
                        if !flag.load(std::sync::atomic::Ordering::SeqCst) {
                            unmatched.push((batch_idx, row_idx));
                        }
                    }
                }
                if !unmatched.is_empty() {
                    result.push(create_build_only_batch(
                        &cache.batches,
                        &unmatched,
                        &self.schema,
                        swapped,
                    )?);
                }
            }
        }
        if let Some(profile) = &mut collected_profile {
            profile.output_batches = result.len();
            profile.output_rows = result.iter().map(RecordBatch::num_rows).sum();
            profile.completed = true;
        }
        drop(collected_profile);
        if std::env::var("HJ_TIMING").is_ok() {
            eprintln!(
                "[hj] partition {} probe: {} rows in {:?}",
                partition,
                probe_rows,
                t_probe.elapsed()
            );
        }

        Ok(Box::pin(stream::iter(result.into_iter().map(Ok))))
    }

    fn output_partitions(&self) -> usize {
        // Semi/Anti joins must see ALL probe rows to correctly determine
        // matched/unmatched build rows, so they must use a single partition.
        // Other join types are parallelized over the PROBE side's partitions:
        // execute(partition) forwards its argument to probe_side.execute(), so
        // this count must be the probe side's or partitions get skipped (rows
        // silently lost) or executed past the end. Which child is the probe
        // side is the same test execute() makes, and it is not always
        // self.right — a Left join with build_right probes self.left. This
        // mirrors SpillableHashJoinExec::output_partitions.
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

// A row bound limits candidate index/filter work; it is not a byte budget for
// arbitrarily wide payloads or ownership of the collected build side.
const INNER_CANDIDATE_ROWS: usize = 4096;
// A match-count limit alone does not bound scans of NULLs, misses or hash
// collisions. Cooperatively yield after this many cursor steps as well.
const INNER_PROBE_WORK_STEPS: usize = 4096;

// Opt-in wall attribution. Phases can overlap other streams and nested operators;
// they are not exclusive CPU measurements. Drop records early stop/error as an
// incomplete stream, including time spent awaiting input or downstream polling.
struct JoinProfile {
    route: &'static str,
    id: u64,
    keys: Vec<String>,
    elapsed: [std::time::Duration; 12],
    phase: usize,
    tick: std::time::Instant,
    input_rows: usize,
    input_batches: usize,
    candidates: usize,
    output_rows: usize,
    output_batches: usize,
    completed: bool,
}

impl JoinProfile {
    fn new(route: &'static str, keys: &[Expr]) -> Option<Self> {
        if std::env::var("QE_JOIN_STREAM_PROF").as_deref() != Ok("1") {
            return None;
        }
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        Some(Self {
            route,
            id: NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
            keys: keys.iter().map(ToString::to_string).collect(),
            elapsed: [std::time::Duration::ZERO; 12],
            phase: 6,
            tick: std::time::Instant::now(),
            input_rows: 0,
            input_batches: 0,
            candidates: 0,
            output_rows: 0,
            output_batches: 0,
            completed: false,
        })
    }

    fn phase(profile: &mut Option<Self>, next: usize) {
        if let Some(profile) = profile {
            let now = std::time::Instant::now();
            profile.elapsed[profile.phase] += now.duration_since(profile.tick);
            profile.tick = now;
            profile.phase = next;
        }
    }
}

impl Drop for JoinProfile {
    fn drop(&mut self) {
        use std::io::Write;
        self.elapsed[self.phase] += self.tick.elapsed();
        let ms = self.elapsed.map(|duration| duration.as_secs_f64() * 1000.0);
        let _ = writeln!(
            std::io::stderr().lock(),
            "[join-stream] {}",
            serde_json::json!({
                "route": self.route, "id": self.id, "keys": self.keys,
                "completed": self.completed, "unwinding": std::thread::panicking(),
                "input_rows": self.input_rows, "input_batches": self.input_batches,
                // Collected probing combines traversal, filtering and gathering;
                // it does not expose an exact candidate count to this observer.
                "candidates": if self.route.starts_with("collected_") { None } else { Some(self.candidates) },
                "output_rows": self.output_rows, "output_batches": self.output_batches,
                "input_wait_ms": ms[0], "key_prepare_ms": ms[1],
                "candidate_ms": ms[2], "filter_ms": ms[3], "gather_ms": ms[4],
                "yield_wait_ms": ms[5], "downstream_wait_ms": ms[6],
                "build_state_ms": ms[7],
                "preflight_ms": ms[8], "collected_probe_ms": ms[9],
                "unmatched_build_ms": ms[10], "build_cache_wait_ms": ms[11]
            })
        );
    }
}

struct InnerProbeStream {
    profile: Option<JoinProfile>,
    cache: Arc<BuildSideCache>,
    input: RecordBatchStream,
    batch: Option<RecordBatch>,
    keys: Vec<ArrayRef>,
    hashes: Vec<u64>,
    // Zero-copy typed handles and validity buffers, resolved once per batch.
    // Preserve the old i64 tuple fast path without per-candidate downcasts.
    i64_keys: Option<Vec<Int64Array>>,
    key_nulls: Vec<arrow::buffer::NullBuffer>,
    row: usize,
    entry: u32,
    started: bool,
    key_exprs: Vec<Expr>,
    swapped: bool,
    schema: SchemaRef,
    filter: Option<Expr>,
    combined_schema: SchemaRef,
    probe_keep: Option<Vec<bool>>,
}

impl InnerProbeStream {
    async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if self.batch.is_none() {
                JoinProfile::phase(&mut self.profile, 0);
                let Some(batch) = self.input.try_next().await? else {
                    if let Some(profile) = &mut self.profile {
                        profile.completed = true;
                    }
                    JoinProfile::phase(&mut self.profile, 6);
                    return Ok(None);
                };
                JoinProfile::phase(&mut self.profile, 1);
                if let Some(profile) = &mut self.profile {
                    profile.input_rows += batch.num_rows();
                    profile.input_batches += 1;
                }
                self.keys = evaluate_join_keys(&batch, &self.key_exprs)?;
                let table = self
                    .cache
                    .vectorized_ht
                    .as_ref()
                    .expect("validated inner keys");
                if !vectorized_hash::can_vectorize_arrays(&self.keys)
                    || table.build_key_arrays.first().is_some_and(|build| {
                        build.len() != self.keys.len()
                            || build
                                .iter()
                                .zip(&self.keys)
                                .any(|(left, right)| left.data_type() != right.data_type())
                    })
                {
                    return Err(crate::error::QueryError::Execution(
                        "HashJoin bounded probe requires matching exact key domains".into(),
                    ));
                }
                self.i64_keys = if table.i64_key_bufs.is_some() {
                    self.keys
                        .iter()
                        .map(|array| array.as_any().downcast_ref::<Int64Array>().cloned())
                        .collect()
                } else {
                    None
                };
                if table.direct.is_some() && self.i64_keys.is_none() {
                    return Err(crate::error::QueryError::Execution(
                        "HashJoin direct-key domain differs".into(),
                    ));
                }
                // Dictionary keys have already been decoded. The admitted
                // domains have ordinary Arrow validity, so these bitmaps retain
                // logical NULL checks without dynamic dispatch on every row.
                self.key_nulls = self
                    .keys
                    .iter()
                    .filter_map(|array| array.nulls())
                    .filter(|nulls| nulls.null_count() != 0)
                    .cloned()
                    .collect();
                self.hashes = if table.direct.is_some() {
                    Vec::new()
                } else {
                    vectorized_hash::hash_arrays(&self.keys, batch.num_rows())
                };
                self.row = 0;
                self.entry = u32::MAX;
                self.started = false;
                self.batch = Some(batch);
            }
            let batch = self.batch.as_ref().expect("loaded probe batch");
            let table = self
                .cache
                .vectorized_ht
                .as_ref()
                .expect("validated inner keys");
            JoinProfile::phase(&mut self.profile, 2);
            let mut build_indices = Vec::new();
            let mut probe_indices = Vec::new();
            build_indices
                .try_reserve_exact(INNER_CANDIDATE_ROWS)
                .map_err(|e| {
                    crate::error::QueryError::Execution(format!(
                        "HashJoin candidate allocation refused: {e}"
                    ))
                })?;
            probe_indices
                .try_reserve_exact(INNER_CANDIDATE_ROWS)
                .map_err(|e| {
                    crate::error::QueryError::Execution(format!(
                        "HashJoin candidate allocation refused: {e}"
                    ))
                })?;
            let mut work_steps = 0;
            while self.row < batch.num_rows() && build_indices.len() < INNER_CANDIDATE_ROWS {
                if work_steps == INNER_PROBE_WORK_STEPS {
                    // Cursor, current input and at most 4096 candidate pairs
                    // stay owned by this future; dropping it stops further work.
                    JoinProfile::phase(&mut self.profile, 5);
                    tokio::task::yield_now().await;
                    JoinProfile::phase(&mut self.profile, 2);
                    work_steps = 0;
                }
                work_steps += 1;
                if !self.started {
                    self.started = true;
                    if self.key_nulls.iter().any(|nulls| !nulls.is_valid(self.row)) {
                        self.entry = u32::MAX;
                    } else if let Some((min, max)) = table.direct {
                        let values = &self.i64_keys.as_ref().expect("validated direct keys")[0];
                        let value = values.value(self.row);
                        self.entry = if value >= min && value <= max {
                            table.heads[(value - min) as usize]
                        } else {
                            u32::MAX
                        };
                    } else {
                        self.entry = table.heads[self.hashes[self.row] as usize & table.mask];
                    }
                }
                if self.entry == u32::MAX {
                    self.row += 1;
                    self.started = false;
                    continue;
                }
                let (build_batch, build_row) = table.entries[self.entry as usize];
                self.entry = table.next[self.entry as usize];
                // Hashes locate candidates only; SQL equality always checks the
                // complete key tuple (direct chains already prove exact i64 keys).
                let equal = if table.direct.is_some() {
                    true
                } else if let (Some(build), Some(probe)) = (&table.i64_key_bufs, &self.i64_keys) {
                    probe.iter().enumerate().all(|(column, values)| {
                        build[column][build_batch as usize][build_row as usize]
                            == values.value(self.row)
                    })
                } else {
                    vectorized_hash::compare_row(
                        &table.build_key_arrays[build_batch as usize],
                        build_row as usize,
                        &self.keys,
                        self.row,
                    )
                };
                if equal {
                    build_indices.push((build_batch as usize, build_row as usize));
                    probe_indices.push(self.row);
                }
            }
            if let Some(profile) = &mut self.profile {
                profile.candidates += build_indices.len();
            }
            JoinProfile::phase(&mut self.profile, 3);
            if let Some(filter) = &self.filter {
                (build_indices, probe_indices) = filter_candidate_pairs(
                    &self.cache.batches,
                    batch,
                    build_indices,
                    probe_indices,
                    self.swapped,
                    &self.combined_schema,
                    filter,
                )?;
            }
            JoinProfile::phase(&mut self.profile, 4);
            let result = if build_indices.is_empty() {
                None
            } else {
                let pruned;
                let probe = match &self.probe_keep {
                    Some(keep) if keep.iter().any(|keep| !keep) => {
                        pruned = prune_batch_columns(batch, keep);
                        &pruned
                    }
                    _ => batch,
                };
                Some(create_joined_batch(
                    &self.cache.batches,
                    probe,
                    &build_indices,
                    &probe_indices,
                    self.swapped,
                    &self.schema,
                    self.cache
                        .row_store
                        .as_ref()
                        .map(|store| (store, store.row_offsets.as_slice())),
                )?)
            };
            if self.row == batch.num_rows() {
                self.batch = None;
                self.keys.clear();
                self.hashes.clear();
                self.i64_keys = None;
                self.key_nulls.clear();
            }
            if let Some(batch) = &result {
                if let Some(profile) = &mut self.profile {
                    profile.output_rows += batch.num_rows();
                    profile.output_batches += 1;
                }
                JoinProfile::phase(&mut self.profile, 6);
                return Ok(result);
            }
            // Filtered-out chunks should give cancellation/scheduling a chance,
            // without spawning an eager producer or retaining an output queue.
            JoinProfile::phase(&mut self.profile, 5);
            tokio::task::yield_now().await;
        }
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
            // Dictionary-encoded strings (small-build join gathers): resolve
            // key -> value. Falling through to Null would make every row of a
            // dict-keyed join compare equal — silent wrong matches.
            if let Some(a) = arr
                .as_any()
                .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>()
            {
                if let (Some(values), Some(key)) = (
                    a.values()
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>(),
                    a.key(row),
                ) {
                    return JoinValue::String(values.value(key).to_string());
                }
                return JoinValue::Null;
            }

            JoinValue::Null
        })
        .collect();

    JoinKey { values }
}

/// Create a combined batch from multiple build/probe row pairs for batch filter evaluation
pub(crate) fn create_combined_batch(
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

    // `combined_schema` is a field fixed once at HashJoinExec construction
    // time from the FULL (unpruned) left/right schemas — join-output
    // pruning can since have dropped columns from `build_batches` (the
    // build cache) without it ever being updated, so using it here after a
    // pruned build produces a column-count mismatch against `all_columns`.
    // Build the schema from the batches actually being gathered instead,
    // in the same build/probe order as `all_columns` above.
    let actual_schema: SchemaRef = {
        let build_fields: Vec<_> = build_batches
            .first()
            .map(|b| b.schema().fields().iter().cloned().collect())
            .unwrap_or_default();
        let probe_fields: Vec<_> = probe_batch.schema().fields().iter().cloned().collect();
        let fields: Vec<_> = if swapped {
            probe_fields.into_iter().chain(build_fields).collect()
        } else {
            build_fields.into_iter().chain(probe_fields).collect()
        };
        Arc::new(Schema::new(fields))
    };

    batch_with_actual_types(&actual_schema, all_columns)
}

/// Apply the non-equi part of an ON clause to the candidate (build row, probe
/// row) pairs produced by the hash lookup.
///
/// This has to happen INSIDE the join, not above it. A post-join filter also
/// sees the NULL-extended rows an outer join emits for unmatched rows; those
/// rows fail every comparison (NULL is not TRUE) and get dropped, which turns
/// the outer join into an inner join. Rejecting candidate pairs here instead
/// leaves an outer row that loses all of its matches genuinely unmatched, so
/// the normal null-extension path still emits it exactly once.
fn filter_candidate_pairs(
    build_batches: &[RecordBatch],
    probe_batch: &RecordBatch,
    build_indices: Vec<(usize, usize)>,
    probe_indices: Vec<usize>,
    swapped: bool,
    combined_schema: &SchemaRef,
    filter: &Expr,
) -> Result<(Vec<(usize, usize)>, Vec<usize>)> {
    if build_indices.is_empty() {
        return Ok((build_indices, probe_indices));
    }

    let combined_batch = create_combined_batch(
        build_batches,
        &build_indices,
        probe_batch,
        &probe_indices,
        swapped,
        combined_schema,
    )?;

    let filter_result = evaluate_expr(&combined_batch, filter)?;
    let mask = match filter_result
        .as_any()
        .downcast_ref::<arrow::array::BooleanArray>()
    {
        Some(mask) => mask,
        // Non-boolean filter result: keep every candidate (the fallback the
        // Semi/Anti path has always used).
        None => return Ok((build_indices, probe_indices)),
    };

    // Every candidate qualifies (TPC-H Q13's ON predicate matches every row):
    // skip rebuilding the index vectors entirely.
    if mask.null_count() == 0 && mask.true_count() == mask.len() {
        return Ok((build_indices, probe_indices));
    }

    let mut kept_build = Vec::with_capacity(build_indices.len());
    let mut kept_probe = Vec::with_capacity(probe_indices.len());
    for i in 0..mask.len() {
        // NULL is not TRUE: an unknown join condition rejects the pair.
        if mask.is_valid(i) && mask.value(i) {
            kept_build.push(build_indices[i]);
            kept_probe.push(probe_indices[i]);
        }
    }
    Ok((kept_build, kept_probe))
}

/// A pre-compiled filter for fast evaluation without per-row batch creation.
/// Recognizes patterns like `col_a != col_b` and evaluates directly from arrays.
///
/// `pub(crate)` (spill-boundaries task 002): the join spill path
/// (`spillable.rs`) reuses it as its own fast path for the same shapes.
#[derive(Clone, Debug)]
pub(crate) struct CompiledFilter {
    build_col_idx: usize,
    probe_col_idx: usize,
    op: BinaryOp,
}

impl CompiledFilter {
    /// The (build-side, probe-side) column indices this filter compares —
    /// so a caller can check the ACTUAL array types / null slots before
    /// trusting `evaluate` (which returns `false` for any type it does not
    /// handle and reads a NULL slot's placeholder value as if it were
    /// data).
    pub(crate) fn column_indices(&self) -> (usize, usize) {
        (self.build_col_idx, self.probe_col_idx)
    }

    /// Try to compile a filter expression into a direct column comparison.
    /// Uses the combined schema (build columns first, then probe columns) to resolve indices.
    /// Returns None if the filter is too complex.
    pub(crate) fn try_compile(
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
    pub(crate) fn evaluate(
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
            return crate::planner::numeric::sql_float_compare(bv, self.op, pv);
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

    crate::planner::resolve_arrow_column(&combined, col)
}

/// Parallel probe for INNER joins using specialized i64 hash table.
/// Processes probe rows in parallel chunks using rayon, with direct Int64 array access
/// to avoid per-row JoinKey allocation overhead.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
fn probe_inner_i64_parallel(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    i64_hash_table: &HashMap<i64, Vec<HashEntry>>,
    probe_key_expr: &Expr,
    swapped: bool,
    output_schema: &SchemaRef,
    row_store: Option<(&RowStore, &[usize])>,
    probe_keep: Option<&[bool]>,
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
            let pruned_probe;
            let gather_probe: &RecordBatch = match probe_keep {
                Some(keep) if keep.iter().any(|k| !k) => {
                    pruned_probe = prune_batch_columns(probe_batch, keep);
                    &pruned_probe
                }
                _ => probe_batch,
            };
            let batch = create_joined_batch(
                build_batches,
                gather_probe,
                &all_build_indices,
                &all_probe_indices,
                swapped,
                output_schema,
                row_store,
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
    vht: Option<&VectorizedHashTable>,
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

    // Serve candidate lookups straight from the vectorized hash table when
    // it supports single-i64 point lookups and the filter (if any) compiled:
    // the separately built HashMap<i64, Vec<HashEntry>> adds nothing then.
    let use_vht: Option<&VectorizedHashTable> = vht.filter(|v| {
        probe_key_exprs.len() == 1
            && v.supports_i64_lookup()
            && (filter.is_none() || compiled_filter.is_some())
    });
    let i64_ht_ref: Option<&HashMap<i64, Vec<HashEntry>>> = cached_i64_ht;

    // Candidate sources, in precedence order, per probe batch:
    //   1. VHT i64 point lookup (`use_vht`)            — Int64 probe keys
    //   2. cached i64 map (`i64_ht_ref`)               — Int64 probe keys
    //   3. VHT generic `probe_batch` enumeration       — any VHT-able keys
    //   4. generic `hash_table` (only built when NO VHT exists)
    // Source 3 is the safety net for everything a VHT was built for but
    // the i64 paths can't take (Utf8/Dictionary keys; Int64 keys whose ON
    // filter is not compilable): `execute()` skips the generic map whenever
    // a VHT exists, so before this task those shapes looked up an EMPTY
    // map and matched nothing (task 001 audit: Semi 0 / Anti every row,
    // both orientations). The former "local i64 map" safety net was built
    // over the BUILD batches with the PROBE key expression and so silently
    // evaluated to None whenever the two sides' key names differed.
    //
    // Match-marking stop rule (the task-001 defect): after a candidate
    // pair PASSES, stop scanning that probe row's candidates ONLY when
    // `swapped` — the probe row is the output and its Semi/Anti status is
    // decided by one match. When `!swapped` the BUILD rows are the output,
    // and every build row sharing the key is its own output row, so the
    // walk must continue and mark them all. Breaking unconditionally
    // emitted one build row per distinct matched key (Dictionary-keyed
    // build-side Semi: 20 rows instead of 30,000; Anti: 59,980 instead of
    // 30,000 on the pinned fixture).

    // Process probe BATCHES in parallel: 8K-row parquet batches are smaller
    // than any useful intra-batch chunk, so chunking within a batch left the
    // whole probe on one thread (Q21's 60M-row semi probe starved 32 fused
    // aggregation workers behind it). Both output modes are batch-independent:
    // non-swapped only sets shared atomic build_matched bits, swapped emits
    // one output batch per probe batch.
    let batch_results: Vec<Result<Option<RecordBatch>>> = probe_batches
        .par_iter()
        .map(|probe_batch| {
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

            let i64_point = i64_values.is_some() && use_vht.is_some();
            let i64_map = i64_values.is_some() && i64_ht_ref.is_some();

            // Full key arrays when no i64 fast path serves this batch
            // (sources 3 and 4).
            let probe_key_arrays: Option<Vec<ArrayRef>> = if !i64_point && !i64_map {
                Some(evaluate_join_keys(probe_batch, probe_key_exprs)?)
            } else {
                None
            };

            // Track probe-side matches when swapped (build=right, probe=left=output)
            let probe_matched_batch: Vec<AtomicBool> = if swapped {
                (0..n_rows).map(|_| AtomicBool::new(false)).collect()
            } else {
                vec![]
            };

            // Apply the ON filter (compiled, expression, or none) to one
            // candidate pair and record the match on the output side.
            // Returns whether the pair passed.
            let consider = |bb: usize, br: usize, pr: usize| -> Result<bool> {
                let pass = if let Some(cf) = &compiled_filter {
                    cf.evaluate(&build_batches[bb], br, probe_batch, pr)
                } else if let Some(filter_expr) = filter {
                    let one = create_single_row_combined_batch(
                        build_batches,
                        bb,
                        br,
                        probe_batch,
                        pr,
                        swapped,
                        combined_schema,
                    )?;
                    let res = evaluate_expr(&one, filter_expr)?;
                    res.as_any()
                        .downcast_ref::<arrow::array::BooleanArray>()
                        .map(|b| b.len() > 0 && b.is_valid(0) && b.value(0))
                        .unwrap_or(false)
                } else {
                    true
                };
                if pass {
                    if swapped {
                        probe_matched_batch[pr].store(true, Ordering::Relaxed);
                    } else {
                        build_matched[bb][br].store(true, Ordering::Relaxed);
                    }
                }
                Ok(pass)
            };

            if let (false, false, Some(v), Some(keys)) =
                (i64_point, i64_map, vht, probe_key_arrays.as_ref())
            {
                // Source 3: every candidate pair of the batch from the VHT
                // (null probe keys never produce candidates). Candidates
                // come grouped by probe row in row order.
                // `!swapped`: a build row already matched needs no further
                // filter evaluation; and with NO filter an already-marked
                // first entry means the whole key was marked by an earlier
                // prober (chains are walked in order), so the rest of this
                // probe row's candidates are skipped (`done_pr`) — keeps
                // duplicated-key builds O(probe + build), not O(pairs).
                let mut done_pr: Option<usize> = None;
                for (bb, br, pr) in v.iter_matches(keys, n_rows) {
                    let (bb, br, pr) = (bb as usize, br as usize, pr as usize);
                    if swapped {
                        if probe_matched_batch[pr].load(Ordering::Relaxed) {
                            continue; // this probe row is already decided
                        }
                    } else if done_pr == Some(pr) {
                        continue;
                    } else if build_matched[bb][br].load(Ordering::Relaxed) {
                        if filter.is_none() {
                            done_pr = Some(pr);
                        }
                        continue;
                    }
                    consider(bb, br, pr)?;
                }
            } else {
                for probe_row in 0..n_rows {
                    // Source 1: VHT point lookup; the filter is compiled or
                    // absent here by construction of `use_vht`.
                    if let (true, Some(vals), Some(v)) = (i64_point, i64_values, use_vht) {
                        if let Some(ref nb) = null_bitmap {
                            if !nb.is_valid(probe_row) {
                                continue;
                            }
                        }
                        v.for_each_i64_candidate(vals[probe_row], |bb, br| {
                            if !swapped
                                && build_matched[bb as usize][br as usize].load(Ordering::Relaxed)
                            {
                                // Already an output row: skip the filter; with
                                // no filter the whole key is already marked.
                                return compiled_filter.is_none();
                            }
                            let pass = match &compiled_filter {
                                Some(cf) => cf.evaluate(
                                    &build_batches[bb as usize],
                                    br as usize,
                                    probe_batch,
                                    probe_row,
                                ),
                                None => true,
                            };
                            if pass {
                                if swapped {
                                    probe_matched_batch[probe_row].store(true, Ordering::Relaxed);
                                } else {
                                    build_matched[bb as usize][br as usize]
                                        .store(true, Ordering::Relaxed);
                                }
                            }
                            // Stop the walk only once the PROBE row is decided.
                            pass && swapped
                        });
                        continue;
                    }
                    // Source 2: i64 map, direct array access, no JoinKey allocation.
                    let entries_opt =
                        if let (true, Some(vals), Some(ht)) = (i64_map, i64_values, i64_ht_ref) {
                            if let Some(ref nb) = null_bitmap {
                                if !nb.is_valid(probe_row) {
                                    continue;
                                }
                            }
                            ht.get(&vals[probe_row])
                        } else if let Some(ref key_arrays) = probe_key_arrays {
                            // Source 4: generic map (no VHT exists).
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
                            if !swapped
                                && build_matched[entry.batch_idx][entry.row_idx]
                                    .load(Ordering::Relaxed)
                            {
                                if filter.is_none() {
                                    break; // whole key already marked
                                }
                                continue;
                            }
                            if consider(entry.batch_idx, entry.row_idx, probe_row)? && swapped {
                                break;
                            }
                        }
                    }
                }
            }

            // Output probe rows per-batch when swapped
            if swapped {
                let is_semi = matches!(join_type, JoinType::Semi);
                let keep: Vec<u32> = (0..n_rows as u32)
                    .filter(|&i| probe_matched_batch[i as usize].load(Ordering::Relaxed) == is_semi)
                    .collect();
                if !keep.is_empty() {
                    let take_idx = UInt32Array::from(keep);
                    let columns: std::result::Result<Vec<ArrayRef>, arrow::error::ArrowError> =
                        probe_batch
                            .columns()
                            .iter()
                            .map(|col| arrow::compute::take(col, &take_idx, None))
                            .collect();
                    let batch = batch_with_actual_types(
                        output_schema,
                        columns.map_err(|e| crate::error::QueryError::Execution(e.to_string()))?,
                    )?;
                    return Ok(Some(batch));
                }
            }
            Ok(None)
        })
        .collect();

    let mut results = Vec::new();
    for r in batch_results {
        if let Some(b) = r? {
            results.push(b);
        }
    }

    // When swapped, probe-side output was already collected per-batch
    if swapped {
        return Ok(results);
    }

    // Convert atomic bools to regular bools and create output from build side
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

/// Vectorized probe: handles Inner, Left, Right, Semi, Anti, Full, Cross joins
/// without per-row JoinKey allocation. Uses VectorizedHashTable for batch-level
/// hash computation and comparison.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
fn probe_vectorized(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    vht: &VectorizedHashTable,
    probe_key_exprs: &[Expr],
    join_type: JoinType,
    swapped: bool,
    output_schema: &SchemaRef,
    filter: Option<&Expr>,
    combined_schema: &SchemaRef,
    shared_build_matched: Option<&[Vec<std::sync::atomic::AtomicBool>]>,
    row_store: Option<(&RowStore, &[usize])>,
    probe_keep: Option<&[bool]>,
) -> Result<Vec<RecordBatch>> {
    let mut results = Vec::new();

    use std::sync::atomic::{AtomicBool, Ordering};

    // Track build-side matches for Semi/Anti/Full/Right joins
    let needs_build_tracking = matches!(
        join_type,
        JoinType::Semi | JoinType::Anti | JoinType::Full | JoinType::Right
    );
    // Use atomic bools for Semi/Anti to enable parallel probe
    let build_matched_atomic: Vec<Vec<AtomicBool>> = if needs_build_tracking {
        build_batches
            .iter()
            .map(|b| (0..b.num_rows()).map(|_| AtomicBool::new(false)).collect())
            .collect()
    } else {
        Vec::new()
    };
    // Non-atomic version for Left/Right/Full (sequential per probe batch)
    let mut build_matched: Vec<Vec<bool>> =
        if needs_build_tracking && !matches!(join_type, JoinType::Semi | JoinType::Anti) {
            build_batches
                .iter()
                .map(|b| vec![false; b.num_rows()])
                .collect()
        } else {
            Vec::new()
        };

    // Probe exactly one batch for Semi/Anti against the VHT. Used by both
    // the batch-parallel branch below and the sequential fallback's own
    // Semi/Anti arm, so there is exactly ONE implementation of "how do we
    // probe a Semi/Anti batch", not two that can silently drift apart.
    //   swapped  (build=right, probe=left=output): a probe row's matched
    //     status is fully decided within THIS batch alone (the build side
    //     is already 100% built before any probing starts), so the output
    //     rows for this batch can be produced independently, in parallel.
    //   !swapped (build=left=output): a build row can be matched by ANY
    //     probe batch, so this only marks the SHARED `build_matched_atomic`
    //     bits — relaxed, monotonic false->true stores are race-free no
    //     matter how many threads write the same cell. The actual output
    //     (matched build rows for Semi, unmatched for Anti) is assembled
    //     once, after every batch has been probed, further below.
    let probe_one_semi_anti_batch = |probe_batch: &RecordBatch| -> Result<Option<RecordBatch>> {
        let probe_key_arrays = evaluate_join_keys(probe_batch, probe_key_exprs)?;
        let n_rows = probe_batch.num_rows();
        if swapped {
            let is_semi = matches!(join_type, JoinType::Semi);
            // One match decides a probe row: first-match probe, never the
            // full candidate enumeration (quadratic under duplicated keys).
            let matched = vht.probe_batch_semi(&probe_key_arrays, n_rows);
            let keep: Vec<u32> = (0..n_rows as u32)
                .filter(|&i| matched[i as usize] == is_semi)
                .collect();
            if keep.is_empty() {
                return Ok(None);
            }
            let take_idx = UInt32Array::from(keep);
            let columns: std::result::Result<Vec<ArrayRef>, arrow::error::ArrowError> = probe_batch
                .columns()
                .iter()
                .map(|col| arrow::compute::take(col, &take_idx, None))
                .collect();
            let batch = batch_with_actual_types(
                output_schema,
                columns.map_err(|e| crate::error::QueryError::Execution(e.to_string()))?,
            )?;
            Ok(Some(batch))
        } else {
            // Every build row sharing a probed key is an output row; see
            // `mark_build_matches` for why this is not `probe_batch`.
            vht.mark_build_matches(&probe_key_arrays, n_rows, &build_matched_atomic);
            Ok(None)
        }
    };

    // Batch-level parallel probing for Inner/Left/Semi/Anti joins with many
    // probe batches. With 8K-row Parquet batches, intra-batch chunk
    // parallelism is ineffective. Process entire batches in parallel across
    // rayon threads instead.
    //
    // Semi/Anti were excluded here historically. Investigated for task 004
    // and confirmed an oversight, not a correctness requirement:
    // `output_partitions()` (this operator's `execute()`) already forces
    // Semi/Anti onto a single ASYNC partition so every probe batch is
    // collected before this function ever runs — necessary, since a build
    // row's matched status isn't final until every probe batch has been
    // seen. That is a different axis from HOW the already-collected
    // `probe_batches` slice is scanned inside one call to this function.
    // The match-tracking above is already safe for concurrent writers
    // (atomic, relaxed, monotonic false->true), and the sibling
    // `probe_semi_anti_parallel` fallback (below, serves the non-VHT/
    // filtered case) has probed Semi/Anti batch-parallel this same way
    // since it was written, with the identical justification: "8K-row
    // parquet batches are smaller than any useful intra-batch chunk, so
    // chunking within a batch left the whole probe on one thread."
    // `QE_SEMI_ANTI_PARALLEL=0` forces the old sequential-batch behavior,
    // for A/B measurement.
    let semi_anti_parallel_enabled =
        !matches!(std::env::var("QE_SEMI_ANTI_PARALLEL").as_deref(), Ok("0"));
    let mut semi_anti_batch_parallel_done = false;
    const MIN_BATCHES_FOR_PARALLEL: usize = 32;
    if probe_batches.len() >= MIN_BATCHES_FOR_PARALLEL
        && matches!(
            join_type,
            JoinType::Inner | JoinType::Left | JoinType::Semi | JoinType::Anti
        )
    {
        if join_type == JoinType::Inner || join_type == JoinType::Cross {
            // HJ_PROF=1: per-phase wall-in-section accumulators across all
            // probe threads. The 001 microbench put the pure hash probe at
            // 3.8 ns/row; the in-engine partitions run 900-1500 ns/row —
            // this is the instrument that says where the other 99.6% goes.
            let prof = std::env::var("HJ_PROF").is_ok();
            use std::sync::atomic::{AtomicU64, Ordering as AOrd};
            let (t_key, t_probe, t_idx, t_filter, t_gather) = (
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
                AtomicU64::new(0),
            );
            let clk = |on: bool| on.then(std::time::Instant::now);
            let lap = |t: Option<std::time::Instant>, acc: &AtomicU64| {
                if let Some(t) = t {
                    acc.fetch_add(t.elapsed().as_nanos() as u64, AOrd::Relaxed);
                }
            };
            // Direct-emission fast path: Inner + no filter + (row store or a
            // single concat'd build batch) — matches go straight into u32
            // take vectors, skipping the tuple Vec and the usize re-pack
            // (Q9: ~24GB of intermediate traffic across its two big joins).
            let u32_path = filter.is_none()
                && (row_store
                    .map(|(st, _)| st.nrows < u32::MAX as usize)
                    .unwrap_or(false)
                    || build_batches.len() <= 1);
            let batch_results: Vec<Result<Option<RecordBatch>>> = probe_batches
                .par_iter()
                .map(|probe_batch| {
                    let t = clk(prof);
                    let probe_key_arrays = evaluate_join_keys(probe_batch, probe_key_exprs)?;
                    lap(t, &t_key);
                    let n_rows = probe_batch.num_rows();
                    if u32_path {
                        let t = clk(prof);
                        let mut build_rows: Vec<u32> = Vec::with_capacity(n_rows);
                        let mut probe_rows: Vec<u32> = Vec::with_capacity(n_rows);
                        let served = match row_store {
                            Some((_, offs)) => {
                                vht.probe_batch_into(&probe_key_arrays, n_rows, |bb, br, pr| {
                                    build_rows.push((offs[bb as usize] + br as usize) as u32);
                                    probe_rows.push(pr);
                                })
                            }
                            None => {
                                vht.probe_batch_into(&probe_key_arrays, n_rows, |bb, br, pr| {
                                    debug_assert_eq!(bb, 0);
                                    build_rows.push(br);
                                    probe_rows.push(pr);
                                })
                            }
                        };
                        lap(t, &t_probe);
                        if served {
                            if build_rows.is_empty() {
                                return Ok(None);
                            }
                            let t = clk(prof);
                            let pruned_probe;
                            let gather_probe: &RecordBatch = match probe_keep {
                                Some(keep) if keep.iter().any(|k| !k) => {
                                    pruned_probe = prune_batch_columns(probe_batch, keep);
                                    &pruned_probe
                                }
                                _ => probe_batch,
                            };
                            let batch = create_joined_batch_u32(
                                build_batches,
                                gather_probe,
                                build_rows,
                                probe_rows,
                                swapped,
                                output_schema,
                                row_store,
                            )?;
                            lap(t, &t_gather);
                            return Ok(Some(batch));
                        }
                        // Generic key layout: fall through to the tuple path.
                    }
                    let t = clk(prof);
                    let matches = vht.probe_batch(&probe_key_arrays, n_rows)?;
                    lap(t, &t_probe);
                    if matches.is_empty() {
                        return Ok(None);
                    }
                    let t = clk(prof);
                    let mut build_indices: Vec<(usize, usize)> = Vec::with_capacity(matches.len());
                    let mut probe_indices: Vec<usize> = Vec::with_capacity(matches.len());
                    for (bb, br, pr) in matches {
                        build_indices.push((bb as usize, br as usize));
                        probe_indices.push(pr as usize);
                    }
                    lap(t, &t_idx);
                    let t = clk(prof);
                    let (build_indices, probe_indices) = match filter {
                        Some(f) => filter_candidate_pairs(
                            build_batches,
                            probe_batch,
                            build_indices,
                            probe_indices,
                            swapped,
                            combined_schema,
                            f,
                        )?,
                        None => (build_indices, probe_indices),
                    };
                    lap(t, &t_filter);
                    if build_indices.is_empty() {
                        return Ok(None);
                    }
                    let t = clk(prof);
                    // Join-output pruning: drop ON-only probe columns now
                    // that the keys have been evaluated; the output schema
                    // was pruned to match at plan time.
                    let pruned_probe;
                    let gather_probe: &RecordBatch = match probe_keep {
                        Some(keep) if keep.iter().any(|k| !k) => {
                            pruned_probe = prune_batch_columns(probe_batch, keep);
                            &pruned_probe
                        }
                        _ => probe_batch,
                    };
                    let batch = create_joined_batch(
                        build_batches,
                        gather_probe,
                        &build_indices,
                        &probe_indices,
                        swapped,
                        output_schema,
                        row_store,
                    )?;
                    lap(t, &t_gather);
                    Ok(Some(batch))
                })
                .collect();
            if prof {
                eprintln!(
                    "[hj-prof] key-eval: {:.0}ms; vht-probe: {:.0}ms; idx-build: {:.0}ms; filter: {:.0}ms; gather+batch: {:.0}ms (cumulative across threads; build_keys={:?})",
                    t_key.load(AOrd::Relaxed) as f64 / 1e6,
                    t_probe.load(AOrd::Relaxed) as f64 / 1e6,
                    t_idx.load(AOrd::Relaxed) as f64 / 1e6,
                    t_filter.load(AOrd::Relaxed) as f64 / 1e6,
                    t_gather.load(AOrd::Relaxed) as f64 / 1e6,
                    probe_key_exprs.iter().map(|k| k.to_string()).collect::<Vec<_>>(),
                );
            }
            for result in batch_results {
                if let Some(batch) = result? {
                    results.push(batch);
                }
            }
            return Ok(results);
        } else if matches!(join_type, JoinType::Semi | JoinType::Anti) {
            if semi_anti_parallel_enabled {
                let batch_results: Vec<Result<Option<RecordBatch>>> = probe_batches
                    .par_iter()
                    .map(|probe_batch| probe_one_semi_anti_batch(probe_batch))
                    .collect();
                for r in batch_results {
                    if let Some(b) = r? {
                        results.push(b);
                    }
                }
                semi_anti_batch_parallel_done = true;
            }
            // else (QE_SEMI_ANTI_PARALLEL=0): fall through unchanged to the
            // sequential per-batch loop below — the A/B control arm.
        } else {
            // Left join: each probe batch independently tracks its own unmatched rows
            let batch_results: Vec<Result<Option<RecordBatch>>> = probe_batches
                .par_iter()
                .map(|probe_batch| {
                    let probe_key_arrays = evaluate_join_keys(probe_batch, probe_key_exprs)?;
                    let n_rows = probe_batch.num_rows();
                    let matches = vht.probe_batch(&probe_key_arrays, n_rows)?;
                    let mut build_indices: Vec<(usize, usize)> = Vec::with_capacity(matches.len());
                    let mut probe_indices: Vec<usize> = Vec::with_capacity(matches.len());
                    for (bb, br, pr) in &matches {
                        build_indices.push((*bb as usize, *br as usize));
                        probe_indices.push(*pr as usize);
                    }
                    // ON-clause predicate: reject non-qualifying pairs BEFORE
                    // match tracking, so a left row whose every match fails the
                    // predicate stays unmatched and is null-extended.
                    let (build_indices, probe_indices) = match filter {
                        Some(f) => filter_candidate_pairs(
                            build_batches,
                            probe_batch,
                            build_indices,
                            probe_indices,
                            swapped,
                            combined_schema,
                            f,
                        )?,
                        None => (build_indices, probe_indices),
                    };
                    let mut probe_matched = vec![false; n_rows];
                    for &pr in &probe_indices {
                        probe_matched[pr] = true;
                    }
                    // Build side is the preserved (left) side when !swapped:
                    // publish match bits so execute() can emit the left rows
                    // that never matched. Without this every build row looks
                    // unmatched and the matched ones are emitted twice.
                    if let Some(shared) = shared_build_matched {
                        for &(bb, br) in &build_indices {
                            shared[bb][br].store(true, Ordering::Relaxed);
                        }
                    }
                    let (bi, pi) = if swapped {
                        add_unmatched_probe(&build_indices, &probe_indices, &probe_matched, n_rows)
                    } else {
                        (build_indices, probe_indices)
                    };
                    if bi.is_empty() && !probe_matched.iter().any(|&m| !m) {
                        return Ok(None);
                    }
                    // Join-output pruning: drop ON-only/unneeded probe
                    // columns now that keys and filter have been evaluated
                    // (Q13's hot path: a filtered Left join with >=32 probe
                    // batches lands here).
                    let pruned_probe;
                    let gather_probe: &RecordBatch = match probe_keep {
                        Some(keep) if keep.iter().any(|k| !k) => {
                            pruned_probe = prune_batch_columns(probe_batch, keep);
                            &pruned_probe
                        }
                        _ => probe_batch,
                    };
                    let batch = create_joined_batch_with_nulls(
                        build_batches,
                        gather_probe,
                        &bi,
                        &pi,
                        &probe_matched,
                        swapped,
                        output_schema,
                        true,
                    )?;
                    Ok(Some(batch))
                })
                .collect();
            for result in batch_results {
                if let Some(batch) = result? {
                    results.push(batch);
                }
            }
            // Left join with swapped=true handles unmatched probe (left) rows
            // per-batch, so no post-processing is needed here. Unmatched
            // build (right) rows are NOT emitted for Left join (only Full
            // join needs that).
            return Ok(results);
        }
    }

    // Original sequential path for small batch counts and other join types.
    // Also reached for Semi/Anti when `semi_anti_batch_parallel_done` is
    // true, but with `sequential_probe_batches` forced empty just below:
    // join_type is fixed for the whole call, so once the batch-parallel
    // branch above has probed every batch there is nothing left for this
    // loop to do — running it again would double-count matches and (for
    // the swapped/output case) emit duplicate rows.
    // Chunk size for parallel processing
    const CHUNK_SIZE: usize = 65536;
    let sequential_probe_batches: &[RecordBatch] = if semi_anti_batch_parallel_done {
        &[]
    } else {
        probe_batches
    };

    for probe_batch in sequential_probe_batches {
        let probe_key_arrays = evaluate_join_keys(probe_batch, probe_key_exprs)?;

        let n_rows = probe_batch.num_rows();

        match join_type {
            JoinType::Inner | JoinType::Cross => {
                // Parallel chunked probing for inner join
                let chunks: Vec<std::ops::Range<usize>> = (0..n_rows)
                    .step_by(CHUNK_SIZE)
                    .map(|start| start..std::cmp::min(start + CHUNK_SIZE, n_rows))
                    .collect();

                let chunk_results: Result<Vec<Vec<(u32, u32, u32)>>> = chunks
                    .par_iter()
                    .map(|range| {
                        // Create sliced key arrays for this chunk
                        let chunk_len = range.end - range.start;
                        let chunk_keys: Vec<ArrayRef> = probe_key_arrays
                            .iter()
                            .map(|a| a.slice(range.start, chunk_len))
                            .collect();
                        let mut matches = vht.probe_batch(&chunk_keys, chunk_len)?;
                        // Adjust probe indices back to original batch coordinates
                        for m in &mut matches {
                            m.2 += range.start as u32;
                        }
                        Ok(matches)
                    })
                    .collect();

                // Merge all matches
                let mut all_build_indices: Vec<(usize, usize)> = Vec::new();
                let mut all_probe_indices: Vec<usize> = Vec::new();
                for chunk_matches in chunk_results? {
                    for (bb, br, pr) in chunk_matches {
                        all_build_indices.push((bb as usize, br as usize));
                        all_probe_indices.push(pr as usize);
                    }
                }

                let (all_build_indices, all_probe_indices) = match filter {
                    Some(f) => filter_candidate_pairs(
                        build_batches,
                        probe_batch,
                        all_build_indices,
                        all_probe_indices,
                        swapped,
                        combined_schema,
                        f,
                    )?,
                    None => (all_build_indices, all_probe_indices),
                };

                if !all_build_indices.is_empty() {
                    let pruned_probe;
                    let gather_probe: &RecordBatch = match probe_keep {
                        Some(keep) if keep.iter().any(|k| !k) => {
                            pruned_probe = prune_batch_columns(probe_batch, keep);
                            &pruned_probe
                        }
                        _ => probe_batch,
                    };
                    let batch = create_joined_batch(
                        build_batches,
                        gather_probe,
                        &all_build_indices,
                        &all_probe_indices,
                        swapped,
                        output_schema,
                        row_store,
                    )?;
                    results.push(batch);
                }
            }

            JoinType::Left => {
                // For Left join: collect matches, then add unmatched probe rows with nulls
                let matches = vht.probe_batch(&probe_key_arrays, n_rows)?;

                let mut build_indices: Vec<(usize, usize)> = Vec::with_capacity(matches.len());
                let mut probe_indices: Vec<usize> = Vec::with_capacity(matches.len());

                for (bb, br, pr) in &matches {
                    build_indices.push((*bb as usize, *br as usize));
                    probe_indices.push(*pr as usize);
                }

                // ON-clause predicate: applied to candidate pairs, before any
                // match is recorded (see filter_candidate_pairs).
                let (build_indices, probe_indices) = match filter {
                    Some(f) => filter_candidate_pairs(
                        build_batches,
                        probe_batch,
                        build_indices,
                        probe_indices,
                        swapped,
                        combined_schema,
                        f,
                    )?,
                    None => (build_indices, probe_indices),
                };

                let mut probe_matched = vec![false; n_rows];
                for (i, &pr) in probe_indices.iter().enumerate() {
                    probe_matched[pr] = true;
                    if let Some(shared) = shared_build_matched {
                        let (bb, br) = build_indices[i];
                        shared[bb][br].store(true, Ordering::Relaxed);
                    }
                }

                // Add unmatched probe rows
                let (bi, pi) = if swapped {
                    add_unmatched_probe(&build_indices, &probe_indices, &probe_matched, n_rows)
                } else {
                    (build_indices, probe_indices)
                };

                if !bi.is_empty() {
                    // Join-output pruning: probe columns nothing downstream
                    // needs were dropped from the output schema at plan time.
                    let pruned_probe;
                    let gather_probe: &RecordBatch = match probe_keep {
                        Some(keep) if keep.iter().any(|k| !k) => {
                            pruned_probe = prune_batch_columns(probe_batch, keep);
                            &pruned_probe
                        }
                        _ => probe_batch,
                    };
                    let batch = create_joined_batch_with_nulls(
                        build_batches,
                        gather_probe,
                        &bi,
                        &pi,
                        &probe_matched,
                        swapped,
                        output_schema,
                        true,
                    )?;
                    results.push(batch);
                }
            }

            JoinType::Right => {
                // Right join: collect matches and track build-side matches
                let matches = vht.probe_batch(&probe_key_arrays, n_rows)?;

                let mut build_indices: Vec<(usize, usize)> = Vec::with_capacity(matches.len());
                let mut probe_indices: Vec<usize> = Vec::with_capacity(matches.len());

                for (bb, br, pr) in &matches {
                    build_indices.push((*bb as usize, *br as usize));
                    probe_indices.push(*pr as usize);
                }

                let (build_indices, probe_indices) = match filter {
                    Some(f) => filter_candidate_pairs(
                        build_batches,
                        probe_batch,
                        build_indices,
                        probe_indices,
                        swapped,
                        combined_schema,
                        f,
                    )?,
                    None => (build_indices, probe_indices),
                };

                for &(bb, br) in &build_indices {
                    if needs_build_tracking {
                        build_matched[bb][br] = true;
                    }
                    if let Some(shared) = shared_build_matched {
                        shared[bb][br].store(true, Ordering::Relaxed);
                    }
                }

                if !build_indices.is_empty() {
                    let pruned_probe;
                    let gather_probe: &RecordBatch = match probe_keep {
                        Some(keep) if keep.iter().any(|k| !k) => {
                            pruned_probe = prune_batch_columns(probe_batch, keep);
                            &pruned_probe
                        }
                        _ => probe_batch,
                    };
                    let batch = create_joined_batch(
                        build_batches,
                        gather_probe,
                        &build_indices,
                        &probe_indices,
                        swapped,
                        output_schema,
                        None,
                    )?;
                    results.push(batch);
                }
            }

            JoinType::Full => {
                let matches = vht.probe_batch(&probe_key_arrays, n_rows)?;

                let mut build_indices: Vec<(usize, usize)> = Vec::with_capacity(matches.len());
                let mut probe_indices: Vec<usize> = Vec::with_capacity(matches.len());

                for (bb, br, pr) in &matches {
                    build_indices.push((*bb as usize, *br as usize));
                    probe_indices.push(*pr as usize);
                }

                let (build_indices, probe_indices) = match filter {
                    Some(f) => filter_candidate_pairs(
                        build_batches,
                        probe_batch,
                        build_indices,
                        probe_indices,
                        swapped,
                        combined_schema,
                        f,
                    )?,
                    None => (build_indices, probe_indices),
                };

                let mut probe_matched = vec![false; n_rows];
                for (i, &pr) in probe_indices.iter().enumerate() {
                    probe_matched[pr] = true;
                    let (bb, br) = build_indices[i];
                    if needs_build_tracking {
                        build_matched[bb][br] = true;
                    }
                    if let Some(shared) = shared_build_matched {
                        shared[bb][br].store(true, Ordering::Relaxed);
                    }
                }

                // FULL OUTER: probe rows with no surviving match are emitted
                // with NULLs on the build side. (Unmatched BUILD rows are
                // emitted once by the last probe partition, in execute().)
                let (bi, pi) =
                    add_unmatched_probe(&build_indices, &probe_indices, &probe_matched, n_rows);

                if !bi.is_empty() {
                    let pruned_probe;
                    let gather_probe: &RecordBatch = match probe_keep {
                        Some(keep) if keep.iter().any(|k| !k) => {
                            pruned_probe = prune_batch_columns(probe_batch, keep);
                            &pruned_probe
                        }
                        _ => probe_batch,
                    };
                    let batch = create_joined_batch(
                        build_batches,
                        gather_probe,
                        &bi,
                        &pi,
                        swapped,
                        output_schema,
                        None,
                    )?;
                    results.push(batch);
                }
            }

            JoinType::Semi | JoinType::Anti => {
                // Shared with the batch-parallel branch above
                // (`probe_one_semi_anti_batch`) — see its doc comment. Only
                // reached here when NOT already probed in parallel
                // (`sequential_probe_batches` is forced empty otherwise).
                if let Some(batch) = probe_one_semi_anti_batch(probe_batch)? {
                    results.push(batch);
                }
            }

            JoinType::Single | JoinType::Mark => {
                // Treat like Semi for now
                let matches = vht.probe_batch(&probe_key_arrays, n_rows)?;
                for (bb, br, _pr) in matches {
                    if needs_build_tracking {
                        build_matched[bb as usize][br as usize] = true;
                    }
                }
            }
        }
    }

    // Publish local matched bits for Right/Full into the SHARED tracker: the
    // last probe partition emits unmatched build rows exactly once in
    // execute(). (Per-call emission here duplicated unmatched rows whenever
    // the probe side had multiple partitions.)
    if matches!(join_type, JoinType::Right | JoinType::Full) {
        if let Some(shared) = shared_build_matched {
            for (batch_idx, flags) in build_matched.iter().enumerate() {
                for (row_idx, &m) in flags.iter().enumerate() {
                    if m {
                        shared[batch_idx][row_idx].store(true, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    if matches!(join_type, JoinType::Semi) && !swapped {
        // Read from atomic bools
        let matched_build: Vec<(usize, usize)> = build_matched_atomic
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
        if !matched_build.is_empty() {
            let batch = create_semi_anti_batch(build_batches, &matched_build, output_schema)?;
            results.push(batch);
        }
    }

    if matches!(join_type, JoinType::Anti) && !swapped {
        // Read from atomic bools
        let unmatched_build: Vec<(usize, usize)> = build_matched_atomic
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
        if !unmatched_build.is_empty() {
            let batch = create_semi_anti_batch(build_batches, &unmatched_build, output_schema)?;
            results.push(batch);
        }
    }

    Ok(results)
}

#[allow(clippy::needless_range_loop)] // Index needed for parallel array access
#[allow(clippy::too_many_arguments)]
#[allow(clippy::too_many_arguments)]
fn probe_hash_table(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    hash_table: &HashMap<JoinKey, Vec<HashEntry>>,
    i64_hash_table: Option<&HashMap<i64, Vec<HashEntry>>>,
    vectorized_ht: Option<&VectorizedHashTable>,
    probe_key_exprs: &[Expr],
    join_type: JoinType,
    swapped: bool,
    output_schema: &SchemaRef,
    filter: Option<&Expr>,
    combined_schema: &SchemaRef,
    shared_build_matched: Option<&[Vec<std::sync::atomic::AtomicBool>]>,
    row_store: Option<(&RowStore, &[usize])>,
    probe_keep: Option<&[bool]>,
) -> Result<Vec<RecordBatch>> {
    let total_probe_rows: usize = probe_batches.iter().map(|b| b.num_rows()).sum();

    // Try vectorized path first (handles all key types, all join types except Cross)
    // Cross joins have no join keys and must use the generic path.
    if let Some(vht) = vectorized_ht {
        // A join filter no longer forces the slow path for the join types whose
        // probe applies it to candidate pairs (see filter_candidate_pairs).
        // Semi/Anti keep their dedicated filtered path below.
        let filter_served = filter.is_none()
            || matches!(
                join_type,
                JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full
            );
        if filter_served && total_probe_rows > 0 && join_type != JoinType::Cross {
            return probe_vectorized(
                build_batches,
                probe_batches,
                vht,
                probe_key_exprs,
                join_type,
                swapped,
                output_schema,
                filter,
                combined_schema,
                shared_build_matched,
                row_store,
                probe_keep,
            );
        }
    }

    // Use parallel path for SEMI/ANTI joins with sufficient data
    if matches!(join_type, JoinType::Semi | JoinType::Anti) && total_probe_rows > 1000 {
        return probe_semi_anti_parallel(
            build_batches,
            probe_batches,
            hash_table,
            i64_hash_table,
            vectorized_ht,
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
                row_store,
                probe_keep,
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
        let probe_key_arrays = evaluate_join_keys(probe_batch, probe_key_exprs)?;

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

        // The ON-clause predicate is part of the join condition: reject
        // non-qualifying candidate pairs here, BEFORE match tracking, so outer
        // rows that lose every match are still emitted null-extended.
        let (build_indices, probe_indices) = match filter {
            Some(f) => filter_candidate_pairs(
                build_batches,
                probe_batch,
                candidate_build_indices,
                candidate_probe_indices,
                swapped,
                combined_schema,
                f,
            )?,
            None => (candidate_build_indices, candidate_probe_indices),
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
                    let pruned_probe;
                    let gather_probe: &RecordBatch = match probe_keep {
                        Some(keep) if keep.iter().any(|k| !k) => {
                            pruned_probe = prune_batch_columns(probe_batch, keep);
                            &pruned_probe
                        }
                        _ => probe_batch,
                    };
                    let batch = create_joined_batch(
                        build_batches,
                        gather_probe,
                        &build_indices,
                        &probe_indices,
                        swapped,
                        output_schema,
                        row_store,
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
                    let pruned_probe;
                    let gather_probe: &RecordBatch = match probe_keep {
                        Some(keep) if keep.iter().any(|k| !k) => {
                            pruned_probe = prune_batch_columns(probe_batch, keep);
                            &pruned_probe
                        }
                        _ => probe_batch,
                    };
                    let batch = create_joined_batch_with_nulls(
                        build_batches,
                        gather_probe,
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
                    let pruned_probe;
                    let gather_probe: &RecordBatch = match probe_keep {
                        Some(keep) if keep.iter().any(|k| !k) => {
                            pruned_probe = prune_batch_columns(probe_batch, keep);
                            &pruned_probe
                        }
                        _ => probe_batch,
                    };
                    let batch = create_joined_batch(
                        build_batches,
                        gather_probe,
                        &build_indices,
                        &probe_indices,
                        swapped,
                        output_schema,
                        None,
                    )?;
                    results.push(batch);
                }
            }
            JoinType::Semi | JoinType::Anti => {
                if swapped {
                    // When swapped: build=right, probe=left=output
                    // Output matching/unmatching probe rows per-batch
                    let is_semi = join_type == JoinType::Semi;
                    let keep: Vec<u32> = (0..probe_batch.num_rows() as u32)
                        .filter(|&i| probe_matched[i as usize] == is_semi)
                        .collect();
                    if !keep.is_empty() {
                        let take_idx = UInt32Array::from(keep);
                        let columns: std::result::Result<Vec<ArrayRef>, arrow::error::ArrowError> =
                            probe_batch
                                .columns()
                                .iter()
                                .map(|col| arrow::compute::take(col, &take_idx, None))
                                .collect();
                        let batch = batch_with_actual_types(
                            output_schema,
                            columns
                                .map_err(|e| crate::error::QueryError::Execution(e.to_string()))?,
                        )?;
                        results.push(batch);
                    }
                }
                // When !swapped, handled after processing all probe batches
            }
            JoinType::Single | JoinType::Mark => {
                // Single and Mark joins are similar to Semi/Anti - handle after all probes
                // Single: for scalar subqueries, returns one row per outer row
                // Mark: for IN subqueries, adds a boolean column for match status
                // For now, treat like Semi join (keep matched rows)
            }
            JoinType::Full => {
                // Unmatched BUILD rows are emitted after all probe batches (or
                // by execute() when a shared tracker exists). Unmatched PROBE
                // rows are emitted here, null-extended on the build side.
                let (bi, pi) = add_unmatched_probe(
                    &build_indices,
                    &probe_indices,
                    &probe_matched,
                    probe_batch.num_rows(),
                );
                if !bi.is_empty() {
                    let pruned_probe;
                    let gather_probe: &RecordBatch = match probe_keep {
                        Some(keep) if keep.iter().any(|k| !k) => {
                            pruned_probe = prune_batch_columns(probe_batch, keep);
                            &pruned_probe
                        }
                        _ => probe_batch,
                    };
                    let batch = create_joined_batch(
                        build_batches,
                        gather_probe,
                        &bi,
                        &pi,
                        swapped,
                        output_schema,
                        None,
                    )?;
                    results.push(batch);
                }
            }
        }
    }

    // Publish local build-match bits into the SHARED tracker when one exists;
    // execute()'s last probe partition then emits the unmatched build rows
    // exactly once. Emitting them here as well would duplicate them.
    if matches!(join_type, JoinType::Left | JoinType::Right | JoinType::Full) {
        if let Some(shared) = shared_build_matched {
            for (batch_idx, flags) in build_matched.iter().enumerate() {
                for (row_idx, &m) in flags.iter().enumerate() {
                    if m {
                        shared[batch_idx][row_idx]
                            .store(true, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        }
    }

    // For outer joins, add unmatched build rows
    if shared_build_matched.is_none() {
        if matches!(join_type, JoinType::Right | JoinType::Full) && !swapped {
            let unmatched_build = collect_unmatched_build(&build_matched);
            if !unmatched_build.is_empty() {
                let batch = create_build_only_batch(
                    build_batches,
                    &unmatched_build,
                    output_schema,
                    swapped,
                )?;
                results.push(batch);
            }
        }

        if matches!(join_type, JoinType::Full) && swapped {
            let unmatched_build = collect_unmatched_build(&build_matched);
            if !unmatched_build.is_empty() {
                let batch = create_build_only_batch(
                    build_batches,
                    &unmatched_build,
                    output_schema,
                    swapped,
                )?;
                results.push(batch);
            }
        }
    }

    // Handle Semi and Anti joins - return build (left) rows based on match status
    // When swapped, probe-side output was already handled per-batch above
    if matches!(join_type, JoinType::Semi) && !swapped {
        // Semi join: return build rows that have at least one match
        let matched_build = collect_matched_build(&build_matched);
        if !matched_build.is_empty() {
            let batch = create_semi_anti_batch(build_batches, &matched_build, output_schema)?;
            results.push(batch);
        }
    }

    if matches!(join_type, JoinType::Anti) && !swapped {
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

    // Semi/Anti output is the preserved side's rows verbatim; like every
    // other emission here, the arrays may be Dictionary-encoded where the
    // declared schema says Utf8 (IPC sidecar / native scans), so the batch
    // is tagged with the ACTUAL types rather than failing `try_new`.
    batch_with_actual_types(output_schema, columns?)
}

fn create_joined_batch(
    build_batches: &[RecordBatch],
    probe_batch: &RecordBatch,
    build_indices: &[(usize, usize)],
    probe_indices: &[usize],
    swapped: bool,
    output_schema: &SchemaRef,
    row_store: Option<(&RowStore, &[usize])>,
) -> Result<RecordBatch> {
    let _num_rows = build_indices.len();

    // Gather build columns. When a row store is present (eligible Inner
    // builds), gather all build columns row-wise from it: one packed-row
    // read per matched row instead of one arrow take per column. Otherwise:
    // single-batch builds (the concat-once cache) share ONE take-index array
    // across every column — gather_column was rebuilding the u32 index vec
    // per column.
    let build_columns: Result<Vec<ArrayRef>> = if build_batches.is_empty() {
        Ok(vec![])
    } else if let Some((store, row_offsets)) = row_store {
        if build_indices.iter().any(|&(b, _)| b == usize::MAX) {
            // Null sentinels (defensive: not produced on Inner paths) —
            // fall back to the columnar gather.
            (0..build_batches[0].num_columns())
                .map(|col_idx| gather_column(build_batches, col_idx, build_indices))
                .collect()
        } else {
            gather_build_from_row_store(store, row_offsets, build_indices)
        }
    } else if build_batches.len() == 1 {
        let has_null_sentinels = build_indices.iter().any(|&(b, _)| b == usize::MAX);
        let take_arr: UInt32Array = if has_null_sentinels {
            build_indices
                .iter()
                .map(|&(b, r)| {
                    if b == usize::MAX {
                        None
                    } else {
                        Some(r as u32)
                    }
                })
                .collect()
        } else {
            build_indices.iter().map(|&(_, r)| r as u32).collect()
        };
        // Small-build string columns come out DICTIONARY-encoded: the take
        // indices already ARE dictionary keys into the build column, so the
        // encoding is free, and it survives every downstream take-gather.
        // What it buys: a group key like n_name (25 nations decorated onto
        // 600M joined rows in Q9) reaches the aggregate as indices instead
        // of strings — the per-row hash+verify of the string path was 40%
        // of Q9's CPU at SF=100. 4096 keeps the values array trivially
        // small and Int32 keys exact.
        let dict_encode = build_batches[0].num_rows() <= 4096
            && !matches!(std::env::var("QE_DICT_GATHER").as_deref(), Ok("0"));
        build_batches[0]
            .columns()
            .iter()
            .map(|col| {
                if dict_encode && col.data_type() == &arrow::datatypes::DataType::Utf8 {
                    let keys: arrow::array::Int32Array =
                        take_arr.iter().map(|v| v.map(|u| u as i32)).collect();
                    arrow::array::DictionaryArray::try_new(keys, col.clone())
                        .map(|d| std::sync::Arc::new(d) as ArrayRef)
                        .map_err(Into::into)
                } else {
                    compute::take(col.as_ref(), &take_arr, None).map_err(Into::into)
                }
            })
            .collect()
    } else {
        (0..build_batches[0].num_columns())
            .map(|col_idx| gather_column(build_batches, col_idx, build_indices))
            .collect()
    };
    let build_columns = build_columns?;

    // Gather probe columns. FK-shaped joins (every probe row matches exactly
    // once, in order) produce identity indices — reuse the probe columns
    // as-is instead of take()-copying them (Q09 was re-copying a 133M-row
    // intermediate through this path).
    let identity = probe_indices.len() == probe_batch.num_rows()
        && probe_indices.iter().enumerate().all(|(i, &p)| p == i);
    let probe_columns: Vec<ArrayRef> = if identity {
        probe_batch.columns().to_vec()
    } else {
        let probe_indices_arr: Vec<u32> = probe_indices.iter().map(|&i| i as u32).collect();
        let probe_index_arr = UInt32Array::from(probe_indices_arr);
        probe_batch
            .columns()
            .iter()
            .map(|col| compute::take(col.as_ref(), &probe_index_arr, None).map_err(Into::into))
            .collect::<Result<Vec<ArrayRef>>>()?
    };

    // Combine in correct order
    let columns: Vec<ArrayRef> = if swapped {
        probe_columns.into_iter().chain(build_columns).collect()
    } else {
        build_columns.into_iter().chain(probe_columns).collect()
    };

    batch_with_actual_types(output_schema, columns)
}

/// Build a RecordBatch, adjusting declared field types to the columns'
/// ACTUAL types where they differ. Dictionary-encoded string columns (see
/// the small-build encoding in `create_joined_batch`) legitimately carry
/// `Dictionary(Int32, Utf8)` where the logical schema says `Utf8`;
/// consumers resolve columns by name/position and either read dictionaries
/// natively (the aggregation fast path) or normalize with a cast.
/// Row-store gather where the row ids are already GLOBAL (offsets applied
/// at emission). Mirror of `gather_build_from_row_store` minus the
/// per-match (batch_idx, row_idx) resolution.
fn gather_build_from_row_store_global(store: &RowStore, rows: &[u32]) -> Result<Vec<ArrayRef>> {
    gather_row_store_rows(store, rows.iter().map(|row| Ok(*row as usize)))
}

/// Joined-batch construction from u32 row ids emitted DIRECTLY by
/// `probe_batch_into` — no intermediate match tuples, no usize index
/// vectors, no take-array rebuild. Inner/unfiltered fast path only:
/// `build_rows` are single-batch row ids, or GLOBAL rows when a row
/// store serves the gather.
#[allow(clippy::too_many_arguments)]
fn create_joined_batch_u32(
    build_batches: &[RecordBatch],
    probe_batch: &RecordBatch,
    build_rows: Vec<u32>,
    probe_rows: Vec<u32>,
    swapped: bool,
    output_schema: &SchemaRef,
    row_store: Option<(&RowStore, &[usize])>,
) -> Result<RecordBatch> {
    let build_columns: Vec<ArrayRef> = if let Some((store, _)) = row_store {
        gather_build_from_row_store_global(store, &build_rows)?
    } else if build_batches.is_empty() || build_batches[0].num_columns() == 0 {
        Vec::new()
    } else {
        let dict_encode = build_batches[0].num_rows() <= 4096;
        let take_arr = UInt32Array::from(build_rows);
        build_batches[0]
            .columns()
            .iter()
            .map(|col| {
                if dict_encode && col.data_type() == &arrow::datatypes::DataType::Utf8 {
                    let keys: arrow::array::Int32Array =
                        take_arr.iter().map(|v| v.map(|u| u as i32)).collect();
                    arrow::array::DictionaryArray::try_new(keys, col.clone())
                        .map(|d| std::sync::Arc::new(d) as ArrayRef)
                        .map_err(Into::into)
                } else {
                    compute::take(col.as_ref(), &take_arr, None).map_err(Into::into)
                }
            })
            .collect::<Result<Vec<_>>>()?
    };

    let identity = probe_rows.len() == probe_batch.num_rows()
        && probe_rows.iter().enumerate().all(|(i, &p)| p as usize == i);
    let probe_columns: Vec<ArrayRef> = if identity {
        probe_batch.columns().to_vec()
    } else {
        let probe_index_arr = UInt32Array::from(probe_rows);
        probe_batch
            .columns()
            .iter()
            .map(|col| compute::take(col.as_ref(), &probe_index_arr, None).map_err(Into::into))
            .collect::<Result<Vec<ArrayRef>>>()?
    };

    let columns: Vec<ArrayRef> = if swapped {
        probe_columns.into_iter().chain(build_columns).collect()
    } else {
        build_columns.into_iter().chain(probe_columns).collect()
    };
    batch_with_actual_types(output_schema, columns)
}

/// Column-subset view of a batch (Arc-level, no data copy). Used by
/// join-output pruning to drop ON-only probe columns after key eval.
fn prune_batch_columns(b: &RecordBatch, keep: &[bool]) -> RecordBatch {
    let (fields, cols): (Vec<_>, Vec<_>) = b
        .schema()
        .fields()
        .iter()
        .zip(b.columns())
        .zip(keep)
        .filter(|(_, k)| **k)
        .map(|((f, c), _)| (f.clone(), c.clone()))
        .unzip();
    let opts = arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(b.num_rows()));
    RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), cols, &opts)
        .expect("pruned batch: same rows, subset of columns")
}

fn batch_with_actual_types(declared: &SchemaRef, columns: Vec<ArrayRef>) -> Result<RecordBatch> {
    let schema_matches = columns
        .iter()
        .zip(declared.fields())
        .all(|(c, f)| c.data_type() == f.data_type());
    let schema = if schema_matches {
        declared.clone()
    } else {
        let fields: Vec<arrow::datatypes::Field> = declared
            .fields()
            .iter()
            .zip(&columns)
            .map(|(f, c)| {
                if f.data_type() == c.data_type() {
                    f.as_ref().clone()
                } else {
                    f.as_ref()
                        .clone()
                        .with_data_type(c.data_type().clone())
                        .with_nullable(true)
                }
            })
            .collect();
        std::sync::Arc::new(arrow::datatypes::Schema::new(fields))
    };
    RecordBatch::try_new(schema, columns).map_err(Into::into)
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
        None,
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

    let build_columns: Result<Vec<ArrayRef>> = if build_batches.len() == 1 {
        let has_null_sentinels = indices.iter().any(|&(b, _)| b == usize::MAX);
        let take_arr: UInt32Array = if has_null_sentinels {
            indices
                .iter()
                .map(|&(b, r)| {
                    if b == usize::MAX {
                        None
                    } else {
                        Some(r as u32)
                    }
                })
                .collect()
        } else {
            indices.iter().map(|&(_, r)| r as u32).collect()
        };
        // Mirror create_joined_batch's small-build dictionary encoding so
        // matched and unmatched batches from the same join carry identical
        // schemas (a later concat of mixed encodings would fail loudly).
        let dict_encode = build_batches[0].num_rows() <= 4096
            && !matches!(std::env::var("QE_DICT_GATHER").as_deref(), Ok("0"));
        build_batches[0]
            .columns()
            .iter()
            .map(|col| {
                if dict_encode && col.data_type() == &arrow::datatypes::DataType::Utf8 {
                    let keys: arrow::array::Int32Array =
                        take_arr.iter().map(|v| v.map(|u| u as i32)).collect();
                    arrow::array::DictionaryArray::try_new(keys, col.clone())
                        .map(|d| std::sync::Arc::new(d) as ArrayRef)
                        .map_err(Into::into)
                } else {
                    compute::take(col.as_ref(), &take_arr, None).map_err(Into::into)
                }
            })
            .collect()
    } else {
        (0..build_batches[0].num_columns())
            .map(|col_idx| gather_column(build_batches, col_idx, indices))
            .collect()
    };
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

    batch_with_actual_types(output_schema, columns)
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

    // Check if any indices are NULL sentinels (usize::MAX = unmatched row in outer join)
    let has_null_sentinels = indices
        .iter()
        .any(|&(batch_idx, _)| batch_idx == usize::MAX);

    if batches.len() == 1 {
        // Fast path: single batch - direct take()
        let col = batches[0].column(col_idx);
        if has_null_sentinels {
            // Use nullable take indices so sentinel rows produce NULL output
            let take_indices: Vec<Option<u32>> = indices
                .iter()
                .map(|&(batch_idx, row_idx)| {
                    if batch_idx == usize::MAX {
                        None
                    } else {
                        Some(row_idx as u32)
                    }
                })
                .collect();
            let take_arr = UInt32Array::from(take_indices);
            return compute::take(col.as_ref(), &take_arr, None).map_err(Into::into);
        } else {
            let take_indices: Vec<u32> =
                indices.iter().map(|&(_, row_idx)| row_idx as u32).collect();
            let take_arr = UInt32Array::from(take_indices);
            return compute::take(col.as_ref(), &take_arr, None).map_err(Into::into);
        }
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

    let result = if has_null_sentinels {
        // Use nullable take indices so sentinel rows produce NULL output
        let take_indices: Vec<Option<u32>> = indices
            .iter()
            .map(|&(batch_idx, row_idx)| {
                if batch_idx == usize::MAX {
                    None
                } else {
                    Some((offsets[batch_idx] + row_idx) as u32)
                }
            })
            .collect();
        let take_arr = UInt32Array::from(take_indices);
        compute::take(concatenated.as_ref(), &take_arr, None)?
    } else {
        let take_indices: Vec<u32> = indices
            .iter()
            .map(|&(batch_idx, row_idx)| (offsets[batch_idx] + row_idx) as u32)
            .collect();
        let take_arr = UInt32Array::from(take_indices);
        compute::take(concatenated.as_ref(), &take_arr, None)?
    };

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

    #[test]
    fn index_cost_bound_covers_dense_null_and_extreme_domains() {
        assert!(join_index_storage_bound(usize::MAX).is_err());
        for (keys, direct) in [
            (vec![Some(0), Some(1), None, Some(1)], true),
            (vec![Some(i64::MIN), Some(i64::MAX)], false),
            (vec![None; 4], false),
        ] {
            let bound = join_index_storage_bound(keys.len()).unwrap();
            let pool = crate::execution::create_memory_pool(65536);
            let keys: ArrayRef = Arc::new(Int64Array::from(keys));
            let batch = RecordBatch::try_from_iter([("k", keys)]).unwrap();
            let table = VectorizedHashTable::build(&[batch], &[Expr::column("k")], &pool)
                .unwrap()
                .unwrap();
            assert_eq!(table.direct.is_some(), direct);
            assert!(pool.used() <= bound);
            drop(table);
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn sparse_integer_domain_uses_a_budgeted_hash_index() {
        let pool = crate::execution::create_memory_pool(65536);
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![
            Some(0),
            Some(15_000_000),
            Some(0),
            None,
        ]));
        let batch = RecordBatch::try_from_iter([("k", keys)]).unwrap();
        let table = VectorizedHashTable::build(&[batch], &[Expr::column("k")], &pool)
            .unwrap()
            .unwrap();
        assert!(table.direct.is_none());
        let probe: ArrayRef = Arc::new(Int64Array::from(vec![Some(0), Some(15_000_000), None]));
        let mut matches = table.probe_batch(&[probe], 3).unwrap();
        matches.sort_unstable();
        assert_eq!(matches, vec![(0, 0, 0), (0, 1, 1), (0, 2, 0)]);
        drop(table);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn join_index_declines_only_unsupported_domains() {
        let pool = crate::execution::create_memory_pool(0);
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Float32, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(arrow::array::Float32Array::from(vec![1.0]))],
        )
        .unwrap();
        assert!(
            VectorizedHashTable::build(&[batch.clone()], &[Expr::column("k")], &pool)
                .unwrap()
                .is_none()
        );
        let err = match VectorizedHashTable::build(&[batch], &[Expr::column("missing")], &pool) {
            Err(e) => e,
            Ok(_) => panic!("binding errors cannot masquerade as unsupported domains"),
        };
        assert_eq!(err.kind(), "ColumnNotFound");
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn join_index_reservation_lifetime_and_parent_contention() {
        use crate::execution::{create_memory_pool, MemoryPool};
        for keys in [vec![1, 2, 2, 3], vec![i64::MIN, 0, i64::MAX, 0]] {
            let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(keys))]).unwrap();
            let parent = create_memory_pool(1024);
            let a = Arc::new(MemoryPool::new_child(&parent, "join-a", 1024));
            let table = VectorizedHashTable::build(&[batch.clone()], &[Expr::column("k")], &a)
                .unwrap()
                .unwrap();
            let charged = table.heads.capacity() * 4
                + table.next.capacity() * 4
                + table.entries.capacity() * 8;
            assert_eq!(a.used(), charged);
            assert_eq!(parent.used(), charged);
            let blocker = parent.allocate(parent.available()).unwrap();
            let b = Arc::new(MemoryPool::new_child(&parent, "join-b", 1024));
            assert!(VectorizedHashTable::build(&[batch], &[Expr::column("k")], &b).is_err());
            assert_eq!(b.used(), 0);
            drop(blocker);
            assert_eq!(parent.used(), charged);
            drop(table);
            assert_eq!(parent.used(), 0);
        }
    }

    #[tokio::test]
    async fn join_index_refusal_is_not_swallowed_as_fallback() {
        let left = create_left_batch();
        let right = create_right_batch();
        let pool = crate::execution::create_memory_pool(1);
        let join = HashJoinExec::new(
            Arc::new(MemoryTableExec::new("l", left.schema(), vec![left], None)),
            Arc::new(MemoryTableExec::new("r", right.schema(), vec![right], None)),
            vec![(Expr::column("id"), Expr::column("id"))],
            JoinType::Inner,
        )
        .with_memory_pool(pool.clone());
        let err = match join.execute(0).await {
            Ok(_) => panic!("index admission must refuse"),
            Err(e) => e,
        };
        assert_eq!(err.kind(), "Execution");
        assert!(
            err.to_string().contains("Memory limit exceeded in"),
            "{err}"
        );
        assert!(join.build_cache.get().is_none());
        assert_eq!(pool.used(), 0);
    }

    #[tokio::test]
    async fn join_index_owner_survives_operator_until_prepared_streams_drop() {
        let left = create_left_batch();
        let right = create_right_batch();
        let pool = crate::execution::create_memory_pool(1024 * 1024);
        let join = HashJoinExec::new(
            Arc::new(MemoryTableExec::new("l", left.schema(), vec![left], None)),
            Arc::new(MemoryTableExec::new("r", right.schema(), vec![right], None)),
            vec![(Expr::column("id"), Expr::column("id"))],
            JoinType::Inner,
        )
        .with_memory_pool(pool.clone());
        let prepared = join
            .prepare_queue_input()
            .await
            .unwrap()
            .expect("resident Inner prepares");
        assert!(pool.used() > 0);
        drop(join);
        assert!(
            pool.used() > 0,
            "prepared streams retain the cached index owner"
        );
        drop(prepared);
        assert_eq!(pool.used(), 0);
    }

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

    /// Run an Anti/Semi join and return its single-i64-column output,
    /// sorted, allowing NULL (represented as `i64::MIN`, out of range of
    /// every value these tests use, so it can't collide with a real key).
    ///
    /// Parameters are named `left`/`right` (the operator's own terms), NOT
    /// `probe`/`build`: which physical side (left or right) ends up as
    /// probe is a function of `build_right` (see `execute()`), so a caller
    /// wanting to force the >=32-batch parallel gate must put the many
    /// batches on whichever side ends up as PROBE for the `build_right` it
    /// passes — LEFT is probe when `build_right=true` (swapped), RIGHT is
    /// probe when `build_right=false` (!swapped). Getting this backwards
    /// doesn't fail loudly: the join still runs, just through the old
    /// sequential path on both sides of the intended A/B, silently testing
    /// nothing new (caught during this task's own development — see
    /// `updates/004/stream-A.md`).
    async fn run_semi_anti_i64(
        left_schema: SchemaRef,
        left_batches: Vec<RecordBatch>,
        right_schema: SchemaRef,
        right_batches: Vec<RecordBatch>,
        join_type: JoinType,
        build_right: bool,
    ) -> Vec<i64> {
        let left_scan = Arc::new(MemoryTableExec::new("l", left_schema, left_batches, None));
        let right_scan = Arc::new(MemoryTableExec::new("r", right_schema, right_batches, None));
        let join = HashJoinExec::new(
            left_scan,
            right_scan,
            vec![(Expr::column("lk"), Expr::column("rk"))],
            join_type,
        )
        .with_build_right(build_right);

        let stream = join.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut out: Vec<i64> = Vec::new();
        for batch in &results {
            let col = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                out.push(if col.is_null(i) {
                    i64::MIN
                } else {
                    col.value(i)
                });
            }
        }
        out.sort_unstable();
        out
    }

    /// Task 004: the batch-parallel probe gate (`MIN_BATCHES_FOR_PARALLEL`,
    /// `probe_vectorized`) now admits Anti (and Semi) — this mirrors Q16's
    /// exact shape (`ps_suppkey NOT IN (SELECT s_suppkey FROM supplier
    /// WHERE ...)`): a small, heavily-filtered BUILD side (`build_right`,
    /// like the filtered `supplier` set) probed by a large LEFT side split
    /// across >=32 batches (like `partsupp`), i.e. the `swapped=true`
    /// branch of `probe_one_semi_anti_batch`. The build (right) side also
    /// carries a NULL key, exercising the classic `NOT IN` NULL corner
    /// independent of this change: `VectorizedHashTable::try_new` already
    /// skips inserting NULL-keyed build rows for every join type (see its
    /// `has_null` guard), so a NULL build key is never a match candidate
    /// for anything — it does not "poison" the whole Anti output the way
    /// SQL's `NOT IN` three-valued logic would for a `WHERE` clause. That
    /// pre-existing behavior is independent of this task and is pinned
    /// here only to confirm the parallel path didn't change it. The
    /// load-bearing assertion is `sequential == parallel`: identical
    /// logical join, scanned once via the pre-existing sequential fallback
    /// (left/probe as a single batch, below `MIN_BATCHES_FOR_PARALLEL`)
    /// and once via the new batch-parallel branch (left/probe split into
    /// 40 one-row batches) — the scheduling difference must not change a
    /// single row.
    #[tokio::test]
    async fn anti_join_batch_parallel_matches_sequential_swapped_with_null_build_key() {
        const N: i64 = 40;
        let ids: Vec<i64> = (1..=N).collect();
        let left_schema = Arc::new(Schema::new(vec![Field::new("lk", DataType::Int64, false)]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("rk", DataType::Int64, true)]));
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]))],
        )
        .unwrap();

        let left_one_batch = vec![RecordBatch::try_new(
            left_schema.clone(),
            vec![Arc::new(Int64Array::from(ids.clone()))],
        )
        .unwrap()];
        let left_many_batches: Vec<RecordBatch> = ids
            .iter()
            .map(|&i| {
                RecordBatch::try_new(
                    left_schema.clone(),
                    vec![Arc::new(Int64Array::from(vec![i]))],
                )
                .unwrap()
            })
            .collect();
        assert!(left_many_batches.len() >= 32);

        // build_right=true: build=right (small, NULL-bearing), probe=left.
        let sequential = run_semi_anti_i64(
            left_schema.clone(),
            left_one_batch,
            right_schema.clone(),
            vec![right_batch.clone()],
            JoinType::Anti,
            true,
        )
        .await;
        let parallel = run_semi_anti_i64(
            left_schema,
            left_many_batches,
            right_schema,
            vec![right_batch],
            JoinType::Anti,
            true,
        )
        .await;

        assert_eq!(
            sequential, parallel,
            "sequential and batch-parallel Anti probes must agree"
        );
        // All 40 left/probe ids except the two (10, 30) that match a
        // non-NULL build key; the NULL build key matches nothing.
        let expected: Vec<i64> = ids.into_iter().filter(|&i| i != 10 && i != 30).collect();
        assert_eq!(sequential, expected);
        assert_eq!(sequential.len(), 38);
    }

    /// Same as above but `!swapped` (`build_right=false`, build = LEFT =
    /// output side, probe = RIGHT): exercises the OTHER half of
    /// `probe_one_semi_anti_batch` (marking the shared `build_matched_atomic`
    /// bits rather than producing per-batch output directly) and the
    /// unchanged tail in `probe_vectorized` that turns those bits into the
    /// final Anti batch. Since probe is RIGHT here, RIGHT is what gets
    /// split into 40 batches to exercise the new gate; LEFT (build, small,
    /// with the NULL key) stays fixed across both sub-calls. The NULL-keyed
    /// build row can never be matched (see the sibling test's comment), so
    /// it is the one row Anti keeps.
    #[tokio::test]
    async fn anti_join_batch_parallel_matches_sequential_not_swapped_with_null_build_key() {
        const N: i64 = 40;
        let ids: Vec<i64> = (1..=N).collect();
        let left_schema = Arc::new(Schema::new(vec![Field::new("lk", DataType::Int64, true)]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("rk", DataType::Int64, false)]));
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]))],
        )
        .unwrap();

        let right_one_batch = vec![RecordBatch::try_new(
            right_schema.clone(),
            vec![Arc::new(Int64Array::from(ids.clone()))],
        )
        .unwrap()];
        let right_many_batches: Vec<RecordBatch> = ids
            .iter()
            .map(|&i| {
                RecordBatch::try_new(
                    right_schema.clone(),
                    vec![Arc::new(Int64Array::from(vec![i]))],
                )
                .unwrap()
            })
            .collect();
        assert!(right_many_batches.len() >= 32);

        // build_right=false: build=left (small, NULL-bearing), probe=right.
        let sequential = run_semi_anti_i64(
            left_schema.clone(),
            vec![left_batch.clone()],
            right_schema.clone(),
            right_one_batch,
            JoinType::Anti,
            false,
        )
        .await;
        let parallel = run_semi_anti_i64(
            left_schema,
            vec![left_batch],
            right_schema,
            right_many_batches,
            JoinType::Anti,
            false,
        )
        .await;

        assert_eq!(
            sequential, parallel,
            "sequential and batch-parallel Anti probes must agree"
        );
        // Left/build keys 10 and 30 both match a right/probe row and are
        // excluded; the NULL left/build key matches nothing and is the
        // sole Anti survivor.
        assert_eq!(sequential, vec![i64::MIN]);
    }

    /// Semi's turn through the same gate (task 004 widened it to both Semi
    /// and Anti): `swapped=true`, build = a small filtered right side.
    #[tokio::test]
    async fn semi_join_batch_parallel_matches_sequential_swapped() {
        const N: i64 = 40;
        let ids: Vec<i64> = (1..=N).collect();
        let left_schema = Arc::new(Schema::new(vec![Field::new("lk", DataType::Int64, false)]));
        let right_schema = Arc::new(Schema::new(vec![Field::new("rk", DataType::Int64, true)]));
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]))],
        )
        .unwrap();

        let left_one_batch = vec![RecordBatch::try_new(
            left_schema.clone(),
            vec![Arc::new(Int64Array::from(ids.clone()))],
        )
        .unwrap()];
        let left_many_batches: Vec<RecordBatch> = ids
            .iter()
            .map(|&i| {
                RecordBatch::try_new(
                    left_schema.clone(),
                    vec![Arc::new(Int64Array::from(vec![i]))],
                )
                .unwrap()
            })
            .collect();
        assert!(left_many_batches.len() >= 32);

        let sequential = run_semi_anti_i64(
            left_schema.clone(),
            left_one_batch,
            right_schema.clone(),
            vec![right_batch.clone()],
            JoinType::Semi,
            true,
        )
        .await;
        let parallel = run_semi_anti_i64(
            left_schema,
            left_many_batches,
            right_schema,
            vec![right_batch],
            JoinType::Semi,
            true,
        )
        .await;

        assert_eq!(
            sequential, parallel,
            "sequential and batch-parallel Semi probes must agree"
        );
        assert_eq!(sequential, vec![10, 30]);
    }

    /// output_partitions() must report the PROBE side's partition count,
    /// because execute(partition) forwards its argument to
    /// probe_side.execute(partition). A Left join with build_right probes
    /// self.LEFT, so reporting self.right's count under-counts whenever the
    /// left child is more partitioned than the right — and the caller then
    /// never asks for the missing partitions, silently dropping rows from the
    /// side a LEFT JOIN exists to preserve.
    #[tokio::test]
    async fn test_left_join_build_right_output_partitions_follow_probe_side() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        // 4 batches x 500 rows: MemoryTableExec partitions a >=1000-row table
        // across its batches, so the left child reports several partitions.
        let left_batches: Vec<RecordBatch> = (0..4)
            .map(|b: i64| {
                let vals: Vec<i64> = (0..500).map(|i| b * 500 + i).collect();
                RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(vals))])
                    .unwrap()
            })
            .collect();
        let right_schema = Arc::new(Schema::new(vec![Field::new("rk", DataType::Int64, false)]));
        let right_batch = RecordBatch::try_new(
            right_schema.clone(),
            vec![Arc::new(Int64Array::from((0..10).collect::<Vec<i64>>()))],
        )
        .unwrap();

        let left_scan = Arc::new(MemoryTableExec::new(
            "l",
            schema.clone(),
            left_batches,
            None,
        ));
        let right_scan = Arc::new(MemoryTableExec::new(
            "r",
            right_schema,
            vec![right_batch],
            None,
        ));
        let expected_partitions = left_scan.output_partitions();

        let join = HashJoinExec::new(
            left_scan,
            right_scan,
            vec![(Expr::column("k"), Expr::column("rk"))],
            JoinType::Left,
        )
        .with_build_right(true);

        assert_eq!(join.output_partitions(), expected_partitions);

        let mut total_rows = 0usize;
        for p in 0..join.output_partitions() {
            let stream = join.execute(p).await.unwrap();
            let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            total_rows += results.iter().map(|b| b.num_rows()).sum::<usize>();
        }
        // Every left row survives: 10 match a right row, 1990 NULL-extend.
        assert_eq!(total_rows, 2000);
    }

    /// Task 008 (native-tables-foundation QA) regression: the shared
    /// `build_matched`/`completed_partitions` state on a join's cached
    /// `BuildSideCache` must correctly re-emit unmatched BUILD rows on EVERY
    /// full round through `0..output_partitions()`, not just the first.
    /// `SpillableHashAggregateExec::execute_fused_streaming` can drive its
    /// child through that entire range, discover its own group-count budget
    /// tripped only after the round already finished, and fall through to
    /// `collect_input_partitions_concurrently`, which re-executes the SAME
    /// range a second time on the SAME `HashJoinExec` (its build cache is a
    /// `OnceCell`, intentionally computed once and shared). Comparing the
    /// completion counter for exact equality against `target` only ever
    /// fires once (the counter sails past `target` on every later round and
    /// never lands on it again) -- reproduced concretely as TPC-H Q13
    /// against native tables at SF=10 losing its "customers with zero
    /// orders" bucket (23 rows instead of 24) because the round whose
    /// output was actually used was the SECOND one. Fixed by checking
    /// `done % target == 0` instead, so each round is self-contained.
    #[tokio::test]
    async fn left_join_reemits_unmatched_build_rows_on_a_second_full_round() {
        let schema = Arc::new(Schema::new(vec![Field::new("k", DataType::Int64, false)]));
        // 1000 build-side rows, 300 of which have a matching probe row -- 700
        // must NULL-extend, both times the full partition range is driven.
        let left_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from((0..1000).collect::<Vec<i64>>()))],
        )
        .unwrap();
        let right_schema = Arc::new(Schema::new(vec![Field::new("rk", DataType::Int64, false)]));
        // 4 probe batches totalling 1200 rows (over MemoryTableExec's
        // <1000-rows "small table" single-partition threshold, so
        // output_partitions() > 1 -- the bug requires more than one
        // partition to manifest at all) but each repeating the SAME 300
        // distinct keys (0..299), so build keys 300..999 (700 of them)
        // never appear on the probe side and must NULL-extend.
        let right_batches: Vec<RecordBatch> = (0..4)
            .map(|_| {
                let vals: Vec<i64> = (0..300).collect();
                RecordBatch::try_new(right_schema.clone(), vec![Arc::new(Int64Array::from(vals))])
                    .unwrap()
            })
            .collect();

        let left_scan = Arc::new(MemoryTableExec::new("l", schema, vec![left_batch], None));
        let right_scan = Arc::new(MemoryTableExec::new("r", right_schema, right_batches, None));

        let join = HashJoinExec::new(
            left_scan,
            right_scan,
            vec![(Expr::column("k"), Expr::column("rk"))],
            JoinType::Left,
        );
        let partitions = join.output_partitions();
        assert!(
            partitions > 1,
            "test needs >1 probe partition to be meaningful"
        );

        for round in 0..2 {
            let mut total_rows = 0usize;
            for p in 0..partitions {
                let stream = join.execute(p).await.unwrap();
                let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();
                total_rows += results.iter().map(|b| b.num_rows()).sum::<usize>();
            }
            // Keys 0..299 each match all 4 probe batches (1200 matched
            // rows); keys 300..999 match nothing and NULL-extend (700 rows).
            assert_eq!(
                total_rows, 1900,
                "round {round}: expected 1200 matched + 700 NULL-extended = 1900 rows, \
                 got {total_rows} -- the unmatched-row emission did not fire on this round"
            );
        }
    }

    /// White-box: a Left join with a filter, a build-side column referenced
    /// ONLY by that filter (never selected downstream, so it survives
    /// pruning purely via the force-keep rule), and >=32 probe batches —
    /// the exact conditions that route through `probe_vectorized`'s Left
    /// PARALLEL-batch branch (`MIN_BATCHES_FOR_PARALLEL`; Q13's actual hot
    /// path at scale) rather than the small-input sequential fallback. This
    /// exercises a pruned build side flowing into `filter_candidate_pairs`
    /// through the real `execute()` path (using the real cached, pruned
    /// build side) together with the `create_combined_batch` schema fix:
    /// without the force-keep rule this mis-answers (the filter column is
    /// pruned away before the filter can see it); without the schema fix it
    /// hard-fails (stale `combined_schema` width vs the pruned build).
    #[tokio::test]
    async fn left_join_filtered_pruned_build_parallel_batch_path() {
        const N: i64 = 40;

        let left_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("keep_left", DataType::Int64, false),
            Field::new("filter_left", DataType::Int64, false),
            Field::new("drop_left", DataType::Int64, false),
        ]));
        let ids: Vec<i64> = (1..=N).collect();
        let left_batch = RecordBatch::try_new(
            left_schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.clone())),
                Arc::new(Int64Array::from(
                    ids.iter().map(|i| i * 10).collect::<Vec<_>>(),
                )),
                // Odd ids pass the ON-filter (>5); even ids fail it and
                // must NULL-extend on the right instead of matching.
                Arc::new(Int64Array::from(
                    ids.iter()
                        .map(|i| if i % 2 == 1 { 10 } else { 3 })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    ids.iter().map(|i| i * 1000).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("rid", DataType::Int64, false),
            Field::new("keep_right", DataType::Int64, false),
            Field::new("drop_right", DataType::Int64, false),
        ]));
        // One row per batch: >=32 batches forces the parallel-batch path
        // (MIN_BATCHES_FOR_PARALLEL) instead of the sequential fallback.
        let right_batches: Vec<RecordBatch> = ids
            .iter()
            .map(|&i| {
                RecordBatch::try_new(
                    right_schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(vec![i])),
                        Arc::new(Int64Array::from(vec![i * 100])),
                        Arc::new(Int64Array::from(vec![i * 9999])),
                    ],
                )
                .unwrap()
            })
            .collect();

        let left_scan = Arc::new(MemoryTableExec::new(
            "l",
            left_schema,
            vec![left_batch],
            None,
        ));
        let right_scan = Arc::new(MemoryTableExec::new("r", right_schema, right_batches, None));

        let mut join = HashJoinExec::with_filter(
            left_scan,
            right_scan,
            vec![(Expr::column("id"), Expr::column("rid"))],
            JoinType::Left,
            Some(
                Expr::column("filter_left")
                    .gt(Expr::literal(crate::planner::ScalarValue::Int64(5))),
            ),
        );

        // Force-keep (filter_left) + downstream need (keep_left,
        // keep_right); drop the join keys and the two never-referenced
        // columns. Order: id, keep_left, filter_left, drop_left, rid,
        // keep_right, drop_right.
        join.set_retained(Some(vec![false, true, true, false, false, true, false]));
        assert_eq!(join.schema().fields().len(), 3);

        let stream = join.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let mut rows: Vec<(i64, i64, Option<i64>)> = Vec::new();
        for batch in &results {
            assert_eq!(
                batch.num_columns(),
                3,
                "gathered batch must match the pruned (retained) schema width"
            );
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
            rows.len(),
            N as usize,
            "every left row must survive exactly once"
        );
        for (idx, (kl, fl, kr)) in rows.iter().enumerate() {
            let id = (idx + 1) as i64;
            assert_eq!(*kl, id * 10);
            if id % 2 == 1 {
                assert_eq!(*fl, 10);
                assert_eq!(
                    *kr,
                    Some(id * 100),
                    "odd id {id} should match its probe row"
                );
            } else {
                assert_eq!(*fl, 3);
                assert_eq!(
                    *kr, None,
                    "even id {id} fails the ON-filter and must NULL-extend"
                );
            }
        }
    }

    // ------------------------------------------------------------------
    // hash-join-dictionary-semi-anti-fix task 001: build-side-output
    // SEMI/ANTI must mark EVERY build row sharing a matched key.
    // ------------------------------------------------------------------

    /// `n` rows in 1024-row batches; column 0 is the key `key{i % modulo}`
    /// (Dictionary(Int32, Utf8) when `dict`, else plain Utf8), column 1 an
    /// Int64 payload `i`. Ported from spillable.rs's task-004 findings
    /// fixture so the operator-level pin and the wrapper-level pin agree
    /// on the exact data.
    fn keyed_string_batches(
        schema: &SchemaRef,
        n: i64,
        modulo: i64,
        dict: bool,
    ) -> Vec<RecordBatch> {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;
        let mut out = Vec::new();
        let mut start = 0i64;
        while start < n {
            let end = (start + 1024).min(n);
            let strings: Vec<String> = (start..end)
                .map(|i| format!("key{:02}", i % modulo))
                .collect();
            let key_col: ArrayRef = if dict {
                let d: DictionaryArray<Int32Type> =
                    strings.iter().map(|s| Some(s.as_str())).collect();
                Arc::new(d)
            } else {
                Arc::new(StringArray::from(strings))
            };
            out.push(
                RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        key_col,
                        Arc::new(Int64Array::from((start..end).collect::<Vec<i64>>())),
                    ],
                )
                .unwrap(),
            );
            start = end;
        }
        out
    }

    fn string_key_schema(key: &str, payload: &str, dict: bool) -> SchemaRef {
        let key_ty = if dict {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
        } else {
            DataType::Utf8
        };
        Arc::new(Schema::new(vec![
            Field::new(key, key_ty, true),
            Field::new(payload, DataType::Int64, false),
        ]))
    }

    /// Column 0 of every batch rendered as an optional string (Dictionary
    /// decoded to its value type; NULL -> None).
    fn key_strings(batches: &[RecordBatch]) -> Vec<Option<String>> {
        let mut out = Vec::new();
        for b in batches {
            let c = b.column(0);
            let c: ArrayRef = match c.data_type() {
                DataType::Dictionary(_, v) => compute::cast(c.as_ref(), v).unwrap(),
                _ => c.clone(),
            };
            for i in 0..c.len() {
                if c.is_null(i) {
                    out.push(None);
                } else if let Some(s) = c.as_any().downcast_ref::<StringArray>() {
                    out.push(Some(s.value(i).to_string()));
                } else if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
                    out.push(Some(a.value(i).to_string()));
                } else {
                    panic!("key_strings: unhandled key type {:?}", c.data_type());
                }
            }
        }
        out
    }

    /// Ground truth independent of the join: SEMI = left rows whose key
    /// occurs on the right, ANTI = left rows whose key does not (NULL keys
    /// never match, so they count as ANTI). INNER = pair count.
    fn naive_count(left: &[RecordBatch], right: &[RecordBatch], jt: JoinType) -> usize {
        let l = key_strings(left);
        let r = key_strings(right);
        let mut rc: HashMap<String, usize> = HashMap::new();
        for k in r.iter().flatten() {
            *rc.entry(k.clone()).or_default() += 1;
        }
        match jt {
            JoinType::Inner => l
                .iter()
                .flatten()
                .map(|k| rc.get(k).copied().unwrap_or(0))
                .sum(),
            JoinType::Semi => l.iter().flatten().filter(|k| rc.contains_key(*k)).count(),
            JoinType::Anti => l
                .iter()
                .filter(|k| match k {
                    None => true,
                    Some(k) => !rc.contains_key(k),
                })
                .count(),
            _ => unreachable!("naive_count: INNER/SEMI/ANTI only"),
        }
    }

    /// Drain EVERY output partition of the operator (Semi/Anti declare one,
    /// Inner declares the probe side's count; draining only partition 0 of
    /// a multi-partition result is the exact mistake retracted in the
    /// task-004 findings note).
    async fn drain_all_partitions(join: &HashJoinExec) -> Vec<RecordBatch> {
        let mut out = Vec::new();
        for p in 0..join.output_partitions() {
            let stream = join.execute(p).await.unwrap();
            let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
            out.extend(batches);
        }
        out
    }

    fn rows_in(batches: &[RecordBatch]) -> usize {
        batches.iter().map(|b| b.num_rows()).sum()
    }

    /// THE pinned defect (spill-join-correctness-3 task 004 finding,
    /// reproduced here directly against `HashJoinExec`, not the spillable
    /// wrapper): SEMI/ANTI with the BUILD side as the output side
    /// (`build_right=false`, `swapped=false`), keys `Dictionary(Int32,Utf8)`
    /// with heavy duplication on the build side (60,000 rows over 40
    /// values) probed by 2,000 rows over 20 of those values.
    ///
    /// Dictionary keys cannot build a `VectorizedHashTable` (task 002 fixes
    /// that), so this shape runs `probe_semi_anti_parallel`'s generic-map
    /// loop, which — before this task — `break`-ed after marking the FIRST
    /// build entry of each probe key. Observed pre-fix: SEMI 20 (one per
    /// distinct matched key) instead of 30,000; ANTI 59,980 instead of
    /// 30,000. Plain Utf8 keys are the control (they take the vectorized
    /// path, which marks every candidate) and pass both before and after.
    /// The probe-side orientation (`build_right=true`) is asserted too: it
    /// was never wrong and must stay that way.
    #[tokio::test]
    async fn semi_anti_build_side_output_dictionary_keys_marks_every_build_row() {
        let mut failures: Vec<String> = Vec::new();
        for (dict, label) in [(true, "Dictionary(Int32,Utf8)"), (false, "Utf8")] {
            let ls = string_key_schema("lk", "lp", dict);
            let rs = string_key_schema("rk", "rp", dict);
            for jt in [JoinType::Semi, JoinType::Anti] {
                for build_right in [false, true] {
                    let left = keyed_string_batches(&ls, 60_000, 40, dict);
                    let right = keyed_string_batches(&rs, 2_000, 20, dict);
                    let truth = naive_count(&left, &right, jt);
                    let join = HashJoinExec::new(
                        Arc::new(MemoryTableExec::new("l", ls.clone(), left, None)),
                        Arc::new(MemoryTableExec::new("r", rs.clone(), right, None)),
                        vec![(Expr::column("lk"), Expr::column("rk"))],
                        jt,
                    )
                    .with_build_right(build_right);
                    let got = rows_in(&drain_all_partitions(&join).await);
                    eprintln!(
                        "[hjdict-001] {label} {jt:?} build_right={build_right}: got={got} truth={truth}"
                    );
                    if got != truth {
                        failures.push(format!(
                            "{label} {jt:?} build_right={build_right}: got {got}, truth {truth}"
                        ));
                    }
                }
            }
        }
        assert!(failures.is_empty(), "wrong counts: {failures:#?}");
    }

    /// Audit sibling of the pin above, same function, different entry:
    /// Int64 keys WITH an ON filter. A compiled column-vs-column filter
    /// (`lp > rp`) routes through `for_each_i64_candidate`, whose callback
    /// returned `pass` (= stop walking) — the same first-entry-only marking
    /// when the build side is the output. A non-compilable filter (`lp >
    /// -1`, literal on one side) takes the expression-evaluated entry loop,
    /// whose marking site also `break`-ed. Both filters pass for every
    /// candidate pair, so truth is the unfiltered count.
    #[tokio::test]
    async fn semi_anti_build_side_output_with_on_filter_marks_every_build_row() {
        let ls: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("lk", DataType::Int64, true),
            Field::new("lp", DataType::Int64, false),
        ]));
        let rs: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("rk", DataType::Int64, true),
            Field::new("rp", DataType::Int64, false),
        ]));
        let mk = |schema: &SchemaRef, n: i64, modulo: i64, payload: i64| -> Vec<RecordBatch> {
            let mut out = Vec::new();
            let mut start = 0i64;
            while start < n {
                let end = (start + 1024).min(n);
                out.push(
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            Arc::new(Int64Array::from(
                                (start..end).map(|i| i % modulo).collect::<Vec<i64>>(),
                            )),
                            Arc::new(Int64Array::from(vec![payload; (end - start) as usize])),
                        ],
                    )
                    .unwrap(),
                );
                start = end;
            }
            out
        };
        let filters: Vec<(&str, Expr)> = vec![
            (
                "compiled lp > rp",
                Expr::column("lp").gt(Expr::column("rp")),
            ),
            (
                "expression lp > -1",
                Expr::column("lp").gt(Expr::literal(crate::planner::ScalarValue::Int64(-1))),
            ),
        ];
        let mut failures: Vec<String> = Vec::new();
        for (label, filter) in filters {
            for jt in [JoinType::Semi, JoinType::Anti] {
                for build_right in [false, true] {
                    // lp = 1 on the left, rp = 0 on the right: `lp > rp` and
                    // `lp > -1` both hold for every candidate pair.
                    let left = mk(&ls, 60_000, 40, 1);
                    let right = mk(&rs, 2_000, 20, 0);
                    let truth = naive_count(&left, &right, jt);
                    let join = HashJoinExec::with_filter(
                        Arc::new(MemoryTableExec::new("l", ls.clone(), left, None)),
                        Arc::new(MemoryTableExec::new("r", rs.clone(), right, None)),
                        vec![(Expr::column("lk"), Expr::column("rk"))],
                        jt,
                        Some(filter.clone()),
                    )
                    .with_build_right(build_right);
                    let got = rows_in(&drain_all_partitions(&join).await);
                    eprintln!(
                        "[hjdict-001] Int64+filter[{label}] {jt:?} build_right={build_right}: got={got} truth={truth}"
                    );
                    if got != truth {
                        failures.push(format!(
                            "Int64+filter[{label}] {jt:?} build_right={build_right}: got {got}, truth {truth}"
                        ));
                    }
                }
            }
        }
        assert!(failures.is_empty(), "wrong counts: {failures:#?}");
    }

    /// Audit probe: Utf8 keys (vectorized table present) WITH an ON filter.
    /// Semi/Anti with a filter bypass `probe_vectorized` and land in
    /// `probe_semi_anti_parallel`, where — with a VHT present — the generic
    /// map was never built and the i64 map does not exist for strings.
    /// Truth: the filter passes for every pair, so counts equal the
    /// unfiltered ones. Both orientations.
    #[tokio::test]
    async fn semi_anti_utf8_keys_with_on_filter_are_exact() {
        let ls = string_key_schema("lk", "lp", false);
        let rs = string_key_schema("rk", "rp", false);
        let mut failures: Vec<String> = Vec::new();
        for jt in [JoinType::Semi, JoinType::Anti] {
            for build_right in [false, true] {
                let left = keyed_string_batches(&ls, 60_000, 40, false);
                let right = keyed_string_batches(&rs, 2_000, 20, false);
                let truth = naive_count(&left, &right, jt);
                // lp in 0..60000, rp in 0..2000: `lp >= 0` always holds.
                let join = HashJoinExec::with_filter(
                    Arc::new(MemoryTableExec::new("l", ls.clone(), left, None)),
                    Arc::new(MemoryTableExec::new("r", rs.clone(), right, None)),
                    vec![(Expr::column("lk"), Expr::column("rk"))],
                    jt,
                    Some(
                        Expr::column("lp")
                            .gt_eq(Expr::literal(crate::planner::ScalarValue::Int64(0))),
                    ),
                )
                .with_build_right(build_right);
                let got = rows_in(&drain_all_partitions(&join).await);
                eprintln!(
                    "[hjdict-001] Utf8+filter {jt:?} build_right={build_right}: got={got} truth={truth}"
                );
                if got != truth {
                    failures.push(format!(
                        "Utf8+filter {jt:?} build_right={build_right}: got {got}, truth {truth}"
                    ));
                }
            }
        }
        assert!(failures.is_empty(), "wrong counts: {failures:#?}");
    }

    // ------------------------------------------------------------------
    // hash-join-dictionary-semi-anti-fix task 002: Dictionary keys take
    // the vectorized hash-table path.
    // ------------------------------------------------------------------

    /// A Dictionary(Int32, Utf8)-keyed build must produce a
    /// `VectorizedHashTable` (keys decoded once per batch), not fall back
    /// to the generic map — in both orientations, so both the build-side
    /// and probe-side Dictionary decode are exercised. The Utf8 control
    /// pins that the assertion itself is meaningful.
    #[tokio::test]
    async fn dictionary_keys_build_a_vectorized_hash_table() {
        for build_right in [false, true] {
            for (dict, label) in [(true, "Dictionary(Int32,Utf8)"), (false, "Utf8")] {
                let ls = string_key_schema("lk", "lp", dict);
                let rs = string_key_schema("rk", "rp", dict);
                let left = keyed_string_batches(&ls, 5_000, 40, dict);
                let right = keyed_string_batches(&rs, 2_000, 20, dict);
                let truth = naive_count(&left, &right, JoinType::Semi);
                let join = HashJoinExec::new(
                    Arc::new(MemoryTableExec::new("l", ls.clone(), left, None)),
                    Arc::new(MemoryTableExec::new("r", rs.clone(), right, None)),
                    vec![(Expr::column("lk"), Expr::column("rk"))],
                    JoinType::Semi,
                )
                .with_build_right(build_right);
                let got = rows_in(&drain_all_partitions(&join).await);
                assert_eq!(got, truth, "{label} build_right={build_right}");
                assert_eq!(
                    join.build_used_vectorized_table(),
                    Some(true),
                    "{label} build_right={build_right}: the build must yield a VectorizedHashTable"
                );
            }
        }
    }

    /// Every output row of a join rendered as strings (Dictionary columns
    /// decoded; NULL -> "NULL"), sorted.
    fn render_sorted(batches: &[RecordBatch]) -> Vec<Vec<String>> {
        let mut out = Vec::new();
        for b in batches {
            let cols: Vec<ArrayRef> = b
                .columns()
                .iter()
                .map(|c| match c.data_type() {
                    DataType::Dictionary(_, v) => compute::cast(c.as_ref(), v).unwrap(),
                    _ => c.clone(),
                })
                .collect();
            for row in 0..b.num_rows() {
                let mut r = Vec::with_capacity(cols.len());
                for c in &cols {
                    if c.is_null(row) {
                        r.push("NULL".to_string());
                    } else if let Some(a) = c.as_any().downcast_ref::<StringArray>() {
                        r.push(a.value(row).to_string());
                    } else if let Some(a) = c.as_any().downcast_ref::<Int64Array>() {
                        r.push(a.value(row).to_string());
                    } else {
                        panic!("render_sorted: unhandled {:?}", c.data_type());
                    }
                }
                out.push(r);
            }
        }
        out.sort();
        out
    }

    /// Ground truth rows for Inner/Semi/Anti/Left, independent of the
    /// join: per-row key comparison over (key, payload) two-column sides.
    /// NULL keys never match (they survive only through Anti and Left).
    fn naive_rows(left: &[RecordBatch], right: &[RecordBatch], jt: JoinType) -> Vec<Vec<String>> {
        fn side(batches: &[RecordBatch]) -> Vec<(Option<String>, String)> {
            let keys = key_strings(batches);
            let mut payloads = Vec::new();
            for b in batches {
                let p = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
                for i in 0..b.num_rows() {
                    payloads.push(p.value(i).to_string());
                }
            }
            keys.into_iter().zip(payloads).collect()
        }
        let l = side(left);
        let r = side(right);
        let mut by_key: HashMap<String, Vec<String>> = HashMap::new();
        for (k, p) in &r {
            if let Some(k) = k {
                by_key.entry(k.clone()).or_default().push(p.clone());
            }
        }
        let mut out = Vec::new();
        for (k, p) in &l {
            let ks = k.clone().unwrap_or_else(|| "NULL".to_string());
            let matches = k.as_ref().and_then(|k| by_key.get(k));
            match jt {
                JoinType::Inner => {
                    if let Some(ms) = matches {
                        for rp in ms {
                            out.push(vec![ks.clone(), p.clone(), ks.clone(), rp.clone()]);
                        }
                    }
                }
                JoinType::Left => match matches {
                    Some(ms) => {
                        for rp in ms {
                            out.push(vec![ks.clone(), p.clone(), ks.clone(), rp.clone()]);
                        }
                    }
                    None => out.push(vec![ks.clone(), p.clone(), "NULL".into(), "NULL".into()]),
                },
                JoinType::Semi => {
                    if matches.is_some() {
                        out.push(vec![ks.clone(), p.clone()]);
                    }
                }
                JoinType::Anti => {
                    if matches.is_none() {
                        out.push(vec![ks.clone(), p.clone()]);
                    }
                }
                _ => unreachable!(),
            }
        }
        out.sort();
        out
    }

    /// Cell-exact Inner / Semi / Anti / Left over Dictionary(Int32, Utf8)
    /// keys through `HashJoinExec`, both orientations, versus naive truth —
    /// with duplicate keys on both sides, keys present on only one side,
    /// and a NULL key on each side. Also a MIXED encoding (Dictionary build
    /// vs plain Utf8 probe and vice versa), which the per-batch decode
    /// makes a non-event: both sides reach the table as Utf8. Every
    /// combination must also have taken the vectorized path.
    #[tokio::test]
    async fn inner_semi_anti_left_over_dictionary_keys_are_cell_exact() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;
        fn mk(
            schema: &SchemaRef,
            keys: &[Option<&str>],
            base: i64,
            dict: bool,
        ) -> Vec<RecordBatch> {
            // Two batches so multi-batch build/probe are both covered.
            let mid = keys.len() / 2;
            [&keys[..mid], &keys[mid..]]
                .iter()
                .enumerate()
                .map(|(bi, ks)| {
                    let key_col: ArrayRef = if dict {
                        let d: DictionaryArray<Int32Type> = ks.iter().copied().collect();
                        Arc::new(d)
                    } else {
                        Arc::new(StringArray::from(ks.to_vec()))
                    };
                    let start = base + (bi * mid) as i64;
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![
                            key_col,
                            Arc::new(Int64Array::from(
                                (0..ks.len() as i64).map(|i| start + i).collect::<Vec<_>>(),
                            )),
                        ],
                    )
                    .unwrap()
                })
                .collect()
        }
        let lkeys: Vec<Option<&str>> = vec![
            Some("a"),
            Some("b"),
            Some("b"),
            Some("c"),
            None,
            Some("d"),
            Some("a"),
            Some("e"),
            Some("z"),
            Some("b"),
        ];
        let rkeys: Vec<Option<&str>> = vec![
            Some("b"),
            Some("a"),
            Some("a"),
            None,
            Some("d"),
            Some("q"),
            Some("b"),
            Some("d"),
        ];
        let mut failures: Vec<String> = Vec::new();
        for (ld, rd, label) in [
            (true, true, "dict/dict"),
            (true, false, "dict/utf8"),
            (false, true, "utf8/dict"),
        ] {
            let ls = string_key_schema("lk", "lp", ld);
            let rs = string_key_schema("rk", "rp", rd);
            for jt in [
                JoinType::Inner,
                JoinType::Semi,
                JoinType::Anti,
                JoinType::Left,
            ] {
                for build_right in [false, true] {
                    let left = mk(&ls, &lkeys, 100, ld);
                    let right = mk(&rs, &rkeys, 200, rd);
                    let truth = naive_rows(&left, &right, jt);
                    let join = HashJoinExec::new(
                        Arc::new(MemoryTableExec::new("l", ls.clone(), left, None)),
                        Arc::new(MemoryTableExec::new("r", rs.clone(), right, None)),
                        vec![(Expr::column("lk"), Expr::column("rk"))],
                        jt,
                    )
                    .with_build_right(build_right);
                    let got = render_sorted(&drain_all_partitions(&join).await);
                    if got != truth {
                        failures.push(format!(
                            "{label} {jt:?} build_right={build_right}: got {got:?}, truth {truth:?}"
                        ));
                    }
                    if join.build_used_vectorized_table() != Some(true) {
                        failures.push(format!(
                            "{label} {jt:?} build_right={build_right}: no VectorizedHashTable"
                        ));
                    }
                }
            }
        }
        assert!(failures.is_empty(), "{failures:#?}");
    }

    /// Complexity guard for the build-side-output marking (task 002):
    /// 400,000 build rows over only 5 distinct Dictionary keys, probed by
    /// 400,000 rows over 4 of them, no filter, both join types. Enumerating
    /// every candidate per probe row would be 400k x 80k = 3.2e10 pair
    /// visits (the shape that turned a 4s SF=10 query into one that did
    /// not finish in 10 minutes, see updates/002/stream-A.md);
    /// `mark_build_matches` makes it O(probe + build). Counts are checked
    /// against the naive truth; the wall-clock bound is deliberately loose
    /// (a 100x margin on this machine) — it exists to fail on a quadratic
    /// regression, not to benchmark.
    #[tokio::test]
    async fn build_side_semi_anti_over_heavily_duplicated_keys_is_linear() {
        for dict in [true, false] {
            let ls = string_key_schema("lk", "lp", dict);
            let rs = string_key_schema("rk", "rp", dict);
            for jt in [JoinType::Semi, JoinType::Anti] {
                let left = keyed_string_batches(&ls, 400_000, 5, dict);
                let right = keyed_string_batches(&rs, 400_000, 4, dict);
                let truth = naive_count(&left, &right, jt);
                let join = HashJoinExec::new(
                    Arc::new(MemoryTableExec::new("l", ls.clone(), left, None)),
                    Arc::new(MemoryTableExec::new("r", rs.clone(), right, None)),
                    vec![(Expr::column("lk"), Expr::column("rk"))],
                    jt,
                );
                let t0 = std::time::Instant::now();
                let got = rows_in(&drain_all_partitions(&join).await);
                let elapsed = t0.elapsed();
                assert_eq!(got, truth, "dict={dict} {jt:?}");
                assert_eq!(join.build_used_vectorized_table(), Some(true));
                assert!(
                    elapsed < std::time::Duration::from_secs(30),
                    "dict={dict} {jt:?}: build-side marking took {elapsed:?} — quadratic walk regressed?"
                );
                eprintln!("[hjdict-002] dict={dict} {jt:?}: {got} rows in {elapsed:?}");
            }
        }
    }
}

#[cfg(test)]
mod prepared_inner_bound_tests {
    use super::*;
    use crate::physical::operators::spillable::owned_input_batch_charge;
    use crate::physical::queue_layout::{inner_output_copy_bound, GatherCopyBound};
    use arrow::array::{DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::{Field, Int32Type};

    fn build(rows: usize, dictionary: bool) -> RecordBatch {
        let strings: ArrayRef = if dictionary {
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(
                        (0..rows)
                            .map(|i| {
                                if i % 3 == 0 {
                                    None
                                } else {
                                    Some((i % 2) as i32)
                                }
                            })
                            .collect::<Vec<_>>(),
                    ),
                    Arc::new(StringArray::from(vec![Some("long".repeat(1024)), None])),
                )
                .unwrap(),
            )
        } else {
            Arc::new(StringArray::from(
                (0..rows)
                    .map(|i| {
                        if i % 3 == 0 {
                            None
                        } else if i % 2 == 0 {
                            Some("long".repeat(1024))
                        } else {
                            Some("s".into())
                        }
                    })
                    .collect::<Vec<_>>(),
            ))
        };
        RecordBatch::try_from_iter(vec![
            (
                "bk",
                Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
            ),
            ("bv", strings),
        ])
        .unwrap()
    }
    fn probe() -> RecordBatch {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "pk",
                Arc::new(Int64Array::from(vec![10, 11, 12, 13])) as ArrayRef,
            ),
            (
                "pv",
                Arc::new(StringArray::from(vec![
                    "excluded".repeat(1000),
                    "x".repeat(1000),
                    "y".into(),
                    "z".into(),
                ])) as ArrayRef,
            ),
        ])
        .unwrap();
        batch.slice(1, 3)
    }
    #[test]
    fn actual_inner_gathers_masks_orientations_and_repeated_values_obey_maximum() {
        for (rows, dictionary) in [(3, false), (3, true), (4097, false)] {
            let build = build(rows, dictionary);
            let probe = probe();
            let probe_bound =
                GatherCopyBound::from_batches(&probe.schema(), &[probe.clone()]).unwrap();
            for swapped in [false, true] {
                for masked in [false, true] {
                    let bk = if masked {
                        vec![false, true]
                    } else {
                        vec![true, true]
                    };
                    let pk = if masked {
                        vec![true, false]
                    } else {
                        vec![true, true]
                    };
                    let mut fields = Vec::new();
                    for i in
                        0..bk.iter().filter(|v| **v).count() + pk.iter().filter(|v| **v).count()
                    {
                        // Intentionally logical types: actual gather rewrites the
                        // mismatches including its small-build dictionary branch.
                        fields.push(Field::new(
                            format!("{}_{i}", "long_alias".repeat(20)),
                            arrow::datatypes::DataType::Utf8,
                            true,
                        ));
                    }
                    let schema = Arc::new(Schema::new(fields));
                    let bound = inner_output_copy_bound(
                        &[build.clone()],
                        &probe_bound,
                        Some(&bk),
                        Some(&pk),
                        swapped,
                        &schema,
                        4096,
                    )
                    .unwrap();
                    let actual_build = prune_batch_columns(&build, &bk);
                    let actual_probe = prune_batch_columns(&probe, &pk);
                    for count in [0, 3, 4096] {
                        let build_indices = (0..count).map(|i| (0, i % rows)).collect::<Vec<_>>();
                        let probe_indices = (0..count).map(|i| i % 3).collect::<Vec<_>>();
                        let output = create_joined_batch(
                            &[actual_build.clone()],
                            &actual_probe,
                            &build_indices,
                            &probe_indices,
                            swapped,
                            &schema,
                            None,
                        )
                        .unwrap();
                        assert_eq!(output.num_rows(), count);
                        assert!(
                            owned_input_batch_charge(&output).unwrap()
                                <= bound.max_bytes().unwrap(),
                            "rows={rows} dict={dictionary} swapped={swapped} masked={masked}"
                        );
                    }
                }
            }
        }
    }
    #[test]
    fn explicit_small_build_dictionary_schema_and_full_child_are_bounded() {
        // Exercise the encoded alternative regardless of process QE_DICT_GATHER,
        // without mutating global environment or racing other tests.
        let source = build(3, false);
        let probe = probe();
        let schema = Arc::new(Schema::new(
            source
                .schema()
                .fields()
                .iter()
                .chain(probe.schema().fields())
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>(),
        ));
        let bound = inner_output_copy_bound(
            &[source.clone()],
            &GatherCopyBound::from_batches(&probe.schema(), &[probe.clone()]).unwrap(),
            None,
            None,
            false,
            &schema,
            4096,
        )
        .unwrap();
        let keys = Int32Array::from(vec![1; 4096]);
        let encoded: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(keys, source.column(1).clone()).unwrap(),
        );
        let indices = UInt32Array::from(vec![1; 4096]);
        let output = batch_with_actual_types(
            &schema,
            vec![
                compute::take(source.column(0).as_ref(), &indices, None).unwrap(),
                encoded,
                compute::take(probe.column(0).as_ref(), &indices, None).unwrap(),
                compute::take(probe.column(1).as_ref(), &indices, None).unwrap(),
            ],
        )
        .unwrap();
        assert!(owned_input_batch_charge(&output).unwrap() <= bound.max_bytes().unwrap());
    }
    #[test]
    fn unsupported_concat_masks_and_arithmetic_decline() {
        let build = build(3, false);
        let probe = probe();
        let gather = GatherCopyBound::from_batches(&probe.schema(), &[probe.clone()]).unwrap();
        let schema = Arc::new(Schema::new(
            build
                .schema()
                .fields()
                .iter()
                .chain(probe.schema().fields())
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>(),
        ));
        assert!(inner_output_copy_bound(
            &[build.clone(), build.clone()],
            &gather,
            None,
            None,
            false,
            &schema,
            4096
        )
        .is_none());
        assert!(inner_output_copy_bound(
            &[build.clone()],
            &gather,
            Some(&[true]),
            None,
            false,
            &schema,
            4096
        )
        .is_none());
        assert!(
            inner_output_copy_bound(&[build], &gather, None, None, false, &schema, usize::MAX)
                .is_none()
        );
    }
}

#[cfg(test)]
mod row_store_owned_tests {
    use super::*;
    use crate::execution::create_memory_pool;
    use arrow::array::{Date32Array, Float64Array, Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field};
    fn batch(rows: &[(i64, u64, i32, i32)]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("f", DataType::Float64, false),
            Field::new("i", DataType::Int32, false),
            Field::new("d", DataType::Date32, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| f64::from_bits(r.1)).collect::<Vec<_>>(),
                )),
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(Date32Array::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }
    #[test]
    fn row_store_exact_bits_signed_types_and_duplicate_multibatch_indices() {
        let a = (i64::MIN, 0x8000_0000_0000_0000, -1, -12345);
        let b = (7, 0x7ff8_0000_0000_0042, i32::MIN, -1);
        let c = (i64::MAX, 0xfff0_0000_0000_0000, 3, 0);
        let source = batch(&[c, a, b]).slice(1, 2);
        let batches = vec![source, batch(&[c, a])];
        let pool = create_memory_pool(1 << 20);
        let store = RowStore::build(&batches, &pool).unwrap();
        let indices = [(1, 1), (0, 0), (0, 1), (1, 0), (0, 1)];
        let output = gather_build_from_row_store(&store, &store.row_offsets, &indices).unwrap();
        let expected = [a, a, b, c, b];
        for (row, value) in expected.iter().enumerate() {
            assert_eq!(
                output[0]
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
                value.0
            );
            assert_eq!(
                output[1]
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row)
                    .to_bits(),
                value.1
            );
            assert_eq!(
                output[2]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(row),
                value.2
            );
            assert_eq!(
                output[3]
                    .as_any()
                    .downcast_ref::<Date32Array>()
                    .unwrap()
                    .value(row),
                value.3
            );
        }
        let global = gather_build_from_row_store_global(&store, &[3, 0, 1, 2, 1]).unwrap();
        for (a, b) in output.iter().zip(&global) {
            assert_eq!(a.to_data(), b.to_data());
        }
        assert!(
            gather_build_from_row_store(&store, &store.row_offsets, &[(0, 2)]).is_err(),
            "must not spill into next batch's row domain"
        );
        assert!(gather_build_from_row_store_global(&store, &[4]).is_err());
    }
    #[test]
    fn row_store_schema_and_null_rejection_precedes_admission() {
        let pool = create_memory_pool(1 << 20);
        let first = batch(&[(1, 0, -1, -1)]);
        let changed =
            RecordBatch::try_from_iter([("k", Arc::new(Int32Array::from(vec![-1])) as ArrayRef)])
                .unwrap();
        let error = RowStore::build(&[first.clone(), changed], &pool)
            .err()
            .unwrap();
        assert!(matches!(error, crate::error::QueryError::Type(_)));
        assert_eq!(pool.used(), 0);
        let nullable =
            RecordBatch::try_from_iter([("k", Arc::new(Int64Array::from(vec![None])) as ArrayRef)])
                .unwrap();
        assert!(matches!(
            RowStore::build(&[nullable], &pool),
            Err(crate::error::QueryError::NotImplemented(_))
        ));
        let unsupported =
            RecordBatch::try_from_iter([("k", Arc::new(StringArray::from(vec!["x"])) as ArrayRef)])
                .unwrap();
        assert!(matches!(
            RowStore::build(&[unsupported], &pool),
            Err(crate::error::QueryError::NotImplemented(_))
        ));
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn row_store_retained_and_temporary_admission_refusal_and_last_owner_drop() {
        let batches = vec![batch(&[(1, 0, 2, 3); 5]), batch(&[(2, 0, 4, 5); 7])];
        let pool = create_memory_pool(1 << 20);
        let store = Arc::new(RowStore::build(&batches, &pool).unwrap());
        let actual = std::mem::size_of::<RowStore>()
            + store.data.capacity()
            + store.row_offsets.capacity() * std::mem::size_of::<usize>()
            + store.cols.capacity() * std::mem::size_of::<(usize, u8, DataType)>();
        assert_eq!(pool.used(), actual);
        assert!(
            pool.reserved_peak() > pool.used(),
            "source/chunk temporary views were admitted too"
        );
        let peak = pool.reserved_peak();
        let clone = store.clone();
        drop(store);
        assert_eq!(pool.used(), actual);
        drop(clone);
        assert_eq!(pool.used(), 0);
        for limit in [actual - 1, peak - 1] {
            let tight = create_memory_pool(limit);
            let error = RowStore::build(&batches, &tight)
                .err()
                .expect("insufficient retained/transient admission");
            assert!(error.to_string().contains("Memory limit exceeded in '"));
            assert_eq!(tight.used(), 0);
        }
        let fitting = create_memory_pool(peak);
        let store = RowStore::build(&batches, &fitting).unwrap();
        assert_eq!(fitting.used(), actual);
        drop(store);
        assert_eq!(fitting.used(), 0);
    }
    #[test]
    fn row_store_checked_layout_rejects_overflow_and_global_u32_limit() {
        assert!(row_store_charge(2, usize::MAX, 1, 1).is_err());
        assert!(row_store_charge(1, 8, usize::MAX, 1).is_err());
        assert!(row_store_charge(1, 8, 1, usize::MAX).is_err());
        if let Some(n) = (u32::MAX as usize).checked_add(1) {
            assert!(row_store_charge(n, 1, 1, 1).is_err());
        }
    }
    #[test]
    fn row_store_real_gather_bound_covers_multibatch_and_nullable_probe_strings() {
        use crate::physical::operators::spillable::owned_input_batch_charge;
        use crate::physical::queue_layout::{row_store_output_copy_bound, GatherCopyBound};
        let builds = vec![
            batch(&[(1, 0, -1, -2)]),
            batch(&[(2, 0, -3, -4), (3, 0, -5, -6)]),
        ];
        let pool = create_memory_pool(1 << 20);
        let store = RowStore::build(&builds, &pool).unwrap();
        let text = "abc".repeat(100);
        let probe = RecordBatch::try_from_iter([(
            "p",
            Arc::new(StringArray::from(vec![
                Some(text.as_str()),
                None,
                Some("q"),
            ])) as ArrayRef,
        )])
        .unwrap();
        for swapped in [false, true] {
            let fields = if swapped {
                probe
                    .schema()
                    .fields()
                    .iter()
                    .cloned()
                    .chain(builds[0].schema().fields().iter().cloned())
                    .collect::<Vec<_>>()
            } else {
                builds[0]
                    .schema()
                    .fields()
                    .iter()
                    .cloned()
                    .chain(probe.schema().fields().iter().cloned())
                    .collect::<Vec<_>>()
            };
            let schema = Arc::new(Schema::new(fields));
            let metadata =
                GatherCopyBound::from_batches(&probe.schema(), std::slice::from_ref(&probe))
                    .unwrap();
            let bound = row_store_output_copy_bound(
                &builds[0].schema(),
                &metadata,
                None,
                swapped,
                &schema,
                4,
            )
            .unwrap();
            let output = create_joined_batch(
                &builds,
                &probe,
                &[(1, 1), (0, 0), (1, 0), (1, 1)],
                &[0, 0, 1, 2],
                swapped,
                &schema,
                Some((&store, &store.row_offsets)),
            )
            .unwrap();
            assert!(owned_input_batch_charge(&output).unwrap() <= bound.max_bytes().unwrap());
            assert!(
                bound.gather_max_bytes(7).is_some(),
                "fixed build proof composes downstream"
            );
        }
    }
}

#[cfg(test)]
mod compiled_float_residual_oracle {
    use super::*;

    #[test]
    fn compiled_float_residual_matches_independent_sql_oracle() {
        let oracle: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/float_comparison_duckdb_1_4_4.json"
        ))
        .unwrap();
        let ops = [
            BinaryOp::Eq,
            BinaryOp::NotEq,
            BinaryOp::Lt,
            BinaryOp::LtEq,
            BinaryOp::Gt,
            BinaryOp::GtEq,
        ];
        for case in oracle["cases"].as_array().unwrap() {
            let (Some(a), Some(b)) = (case["left"].as_str(), case["right"].as_str()) else {
                continue;
            };
            // Caller owns NULL rejection; this specialized evaluator receives
            // only non-NULL slots. Exercise it directly rather than a fallback.
            let build = RecordBatch::try_from_iter(vec![(
                "a",
                Arc::new(arrow::array::Float64Array::from(vec![a
                    .parse::<f64>()
                    .unwrap()])) as ArrayRef,
            )])
            .unwrap();
            let probe = RecordBatch::try_from_iter(vec![(
                "b",
                Arc::new(arrow::array::Float64Array::from(vec![b
                    .parse::<f64>()
                    .unwrap()])) as ArrayRef,
            )])
            .unwrap();
            for (i, op) in ops.into_iter().enumerate() {
                let filter = CompiledFilter {
                    build_col_idx: 0,
                    probe_col_idx: 0,
                    op,
                };
                assert_eq!(
                    filter.evaluate(&build, 0, &probe, 0),
                    case["expected"][i].as_bool().unwrap(),
                    "{a} {op:?} {b}"
                );
            }
        }
    }
}
