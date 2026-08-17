//! Arrow IPC sidecar cache: the engine's owned storage format.
//!
//! Parquet decode is the largest residual cost against DuckDB's native
//! tables (whose 2.94s prices in a decode-free format). Each parquet row
//! group gets a sidecar Arrow IPC file (`<parquet>.qeipc/rg_NNNNN.arrow`),
//! built once and read back with no decompression and no decode — a warm
//! read is a page-cache memcpy. Row-group alignment keeps the parquet
//! footer's min/max statistics valid for pruning and the ALWAYS_TRUE
//! zone-map proof.
//!
//! Memory safety: reads stream one row group at a time exactly like the
//! parquet path; sidecar building is bounded by one row group per thread.
//! Enabled via QE_IPC_CACHE=1 (default off until the gauntlet passes).

use crate::error::{QueryError, Result};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};

pub fn enabled() -> bool {
    static ON: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("QE_IPC_CACHE")
            .map(|v| v == "1")
            .unwrap_or(false)
    })
}

fn sidecar_dir(parquet_path: &Path) -> PathBuf {
    let mut name = parquet_path.file_name().unwrap_or_default().to_os_string();
    name.push(".qeipc");
    parquet_path.with_file_name(name)
}

fn rg_path(dir: &Path, rg_idx: usize) -> PathBuf {
    dir.join(format!("rg_{:05}.arrow", rg_idx))
}

/// Ensure the sidecar exists and is fresh; build it (parallel over row
/// groups) if not. Returns the sidecar dir, or None if building failed
/// (callers fall back to parquet).
///
/// # Concurrency
///
/// Many threads (parallel tests, parallel queries) and even many PROCESSES
/// (cluster nodes sharing a data directory) can race to build the same
/// sidecar. Two defenses, both required:
///
/// * an in-process mutex serializes builders, with a fresh-check after the
///   lock so losers reuse the winner's work instead of rebuilding;
/// * the build itself writes into a `.<pid>.building` staging dir that is
///   atomically RENAMED into place — a reader can never observe a
///   half-written sidecar, and if a rename loses a cross-process race the
///   loser deletes its staging dir and uses the winner's.
///
/// Without these, cold-start parallel tests failed nondeterministically
/// (readers mmapping files another builder was still writing).
pub fn ensure_sidecar(parquet_path: &Path) -> Option<PathBuf> {
    let dir = sidecar_dir(parquet_path);
    let src_meta = std::fs::metadata(parquet_path).ok()?;
    if is_fresh(&dir, &src_meta) {
        return Some(dir);
    }

    static BUILD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = BUILD_LOCK.lock().ok()?;
    if is_fresh(&dir, &src_meta) {
        return Some(dir);
    }
    build_sidecar(parquet_path, &dir, &src_meta).ok()?;
    Some(dir)
}

fn stamp_value(src_meta: &std::fs::Metadata) -> Option<String> {
    Some(format!(
        "{}:{}",
        src_meta.len(),
        src_meta
            .modified()
            .ok()?
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_secs()
    ))
}

fn is_fresh(dir: &Path, src_meta: &std::fs::Metadata) -> bool {
    match (
        std::fs::read_to_string(dir.join(".complete")),
        stamp_value(src_meta),
    ) {
        (Ok(s), Some(want)) => s == want,
        _ => false,
    }
}

fn build_sidecar(parquet_path: &Path, dir: &Path, src_meta: &std::fs::Metadata) -> Result<()> {
    let staging = dir.with_extension(format!("{}.building", std::process::id()));
    let _ = std::fs::remove_dir_all(&staging);
    std::fs::create_dir_all(&staging)?;
    let build_into = staging.clone();
    let dir = build_into.as_path();
    let md = crate::storage::metadata_cache::cached_metadata(parquet_path)?;
    let n_rg = md.metadata().num_row_groups();
    (0..n_rg).into_par_iter().try_for_each(|rg| -> Result<()> {
        let file = File::open(parquet_path)?;
        let builder =
            parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::new_with_metadata(
                file,
                md.clone(),
            )
            .with_row_groups(vec![rg])
            .with_batch_size(65536);
        let reader = builder.build()?;
        let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
        let f = File::create(rg_path(dir, rg))?;
        let mut w = arrow::ipc::writer::FileWriter::try_new(std::io::BufWriter::new(f), &schema)
            .map_err(|e| QueryError::Execution(e.to_string()))?;
        for b in &batches {
            w.write(b)
                .map_err(|e| QueryError::Execution(e.to_string()))?;
        }
        w.finish()
            .map_err(|e| QueryError::Execution(e.to_string()))?;
        Ok(())
    })?;
    let stamp = stamp_value(src_meta)
        .ok_or_else(|| QueryError::Execution("source mtime unavailable for stamp".into()))?;
    std::fs::write(dir.join(".complete"), stamp)?;

    // Atomic publication: rename the staging dir into place. If the final
    // dir exists (stale, or a cross-process racer won), remove-then-rename;
    // if the rename still loses, defer to whatever is there — the fresh
    // check on the next call decides.
    let final_dir = sidecar_dir(parquet_path);
    let _ = std::fs::remove_dir_all(&final_dir);
    if std::fs::rename(&staging, &final_dir).is_err() {
        let _ = std::fs::remove_dir_all(&staging);
    }
    Ok(())
}

/// Read one row group's batches from the sidecar, applying an optional
/// projection (file-schema column indices).
///
/// # Zero-copy, and why it is the whole point
///
/// v0 read sidecars through `File` + `BufReader`, which copies and
/// validates every buffer — for an UNCOMPRESSED format that is more memory
/// traffic than the parquet decode it replaced, and the benchmark showed it:
/// a net regression (Q06 97→558ms). This path instead mmaps the file and
/// hands the arrays `Buffer`s that point INTO the mapping
/// (`Buffer::from_custom_allocation` keeps the `Mmap` alive via its
/// allocation handle). A warm read is then page-cache references, no
/// allocation, no memcpy; alignment stays zero-copy because `FileWriter`
/// pads buffers and an mmap base is page-aligned (misalignment would fall
/// back to a copy inside `build_aligned`, not to an error).
///
/// Memory safety: the mapping is file-backed and read-only — reclaimable by
/// the OS under pressure, never anonymous memory, so the engine's no-OOM
/// guarantee is unaffected.
pub fn read_row_group(
    dir: &Path,
    rg_idx: usize,
    projection: Option<&[usize]>,
) -> Result<Vec<RecordBatch>> {
    use arrow::buffer::Buffer;
    use arrow::ipc::reader::{read_footer_length, FileDecoder};

    let path = rg_path(dir, rg_idx);
    let file = File::open(&path)?;
    // SAFETY: the sidecar is created atomically by build_sidecar (readers
    // only see it after `.complete` is stamped) and never mutated in place —
    // a source-parquet change rebuilds into a fresh directory. Mapping a
    // file that nothing rewrites is the documented sound use of Mmap.
    let mmap = unsafe { memmap2::Mmap::map(&file) }
        .map_err(|e| QueryError::Execution(format!("mmap {}: {e}", path.display())))?;
    let len = mmap.len();
    if len < 10 {
        return Err(QueryError::Execution(format!(
            "{} is truncated ({len} bytes)",
            path.display()
        )));
    }
    let mmap = std::sync::Arc::new(mmap);
    let ptr = std::ptr::NonNull::new(mmap.as_ptr() as *mut u8)
        .ok_or_else(|| QueryError::Execution("mmap returned a null mapping".into()))?;
    // SAFETY: ptr/len describe the live mapping, and the Arc<Mmap> passed as
    // the allocation keeps it alive for as long as any Buffer (and therefore
    // any RecordBatch column) references it.
    let buffer = unsafe { Buffer::from_custom_allocation(ptr, len, mmap) };

    let trailer_start = len - 10;
    let footer_len = read_footer_length(
        buffer.as_slice()[trailer_start..]
            .try_into()
            .expect("10-byte trailer"),
    )
    .map_err(|e| QueryError::Execution(format!("{}: {e}", path.display())))?;
    let footer = arrow::ipc::root_as_footer(
        &buffer.as_slice()[trailer_start - footer_len..trailer_start],
    )
    .map_err(|e| QueryError::Execution(format!("{}: bad IPC footer: {e}", path.display())))?;

    let schema = arrow::ipc::convert::fb_to_schema(
        footer
            .schema()
            .ok_or_else(|| QueryError::Execution("IPC footer has no schema".into()))?,
    );
    let mut decoder = FileDecoder::new(std::sync::Arc::new(schema), footer.version());
    if let Some(p) = projection {
        decoder = decoder.with_projection(p.to_vec());
    }

    for block in footer.dictionaries().iter().flat_map(|d| d.iter()) {
        let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
        let data = buffer.slice_with_length(block.offset() as usize, block_len);
        decoder
            .read_dictionary(&block, &data)
            .map_err(|e| QueryError::Execution(format!("{}: {e}", path.display())))?;
    }

    let mut out = Vec::new();
    for block in footer.recordBatches().iter().flat_map(|b| b.iter()) {
        let block_len = block.bodyLength() as usize + block.metaDataLength() as usize;
        let data = buffer.slice_with_length(block.offset() as usize, block_len);
        if let Some(batch) = decoder
            .read_record_batch(&block, &data)
            .map_err(|e| QueryError::Execution(format!("{}: {e}", path.display())))?
        {
            out.push(batch);
        }
    }
    Ok(out)
}
