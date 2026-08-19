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

#[derive(Clone, Copy, PartialEq)]
pub enum Mode {
    /// QE_IPC_CACHE=0: never touch sidecars.
    Off,
    /// QE_IPC_CACHE unset (the default): USE a sidecar that already exists
    /// and is fresh, but never build one — the cache costs ~2.6x the
    /// parquet footprint on disk, which no one should pay by surprise.
    Auto,
    /// QE_IPC_CACHE=1: build missing/stale sidecars and use them.
    Build,
}

pub fn mode() -> Mode {
    static M: std::sync::OnceLock<Mode> = std::sync::OnceLock::new();
    *M.get_or_init(|| match std::env::var("QE_IPC_CACHE").as_deref() {
        Ok("0") => Mode::Off,
        Ok("1") => Mode::Build,
        _ => Mode::Auto,
    })
}

pub fn enabled() -> bool {
    mode() != Mode::Off
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
    if mode() != Mode::Build {
        // Auto mode uses what exists but never builds (and never serves a
        // STALE sidecar — the fresh check above already declined it).
        return None;
    }

    static BUILD_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    let _guard = BUILD_LOCK.lock().ok()?;
    if is_fresh(&dir, &src_meta) {
        return Some(dir);
    }
    if let Err(e) = build_sidecar(parquet_path, &dir, &src_meta) {
        // A failing build is not silent: every query would re-attempt it
        // and eat the cost while quietly running parquet instead.
        eprintln!(
            "[ipc-cache] sidecar build FAILED for {}: {e}",
            parquet_path.display()
        );
        return None;
    }
    Some(dir)
}

fn stamp_value(src_meta: &std::fs::Metadata) -> Option<String> {
    // v2: sidecars store low-cardinality string columns DICTIONARY-encoded
    // (see build_sidecar) — the version prefix retires every v1 sidecar so
    // mixed formats can never be served.
    Some(format!(
        "v2:{}:{}",
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
    // Canonical dictionary coercion: every Utf8 column whose chunks are
    // fully dictionary-encoded in parquet is STORED as
    // Dictionary(Int32, Utf8) — IPC round-trips dictionaries natively, so
    // a group-key or filter scan that wants the dict form reads it
    // zero-copy instead of falling back to parquet decode (the v1 guard
    // that kept Q1/Q13/Q16 on the parquet path — and kept 32GB of parquet
    // pages competing with the sidecars for page cache). Columns whose
    // dictionaries turn out wide (> 4096 values in any row group) are
    // demoted to plain before writing: a huge dictionary loses the point.
    let file_schema = parquet::arrow::parquet_to_arrow_schema(
        md.metadata().file_metadata().schema_descr(),
        md.metadata().file_metadata().key_value_metadata(),
    )?;
    let dict_candidates: Vec<usize> = file_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(i, f)| {
            f.data_type() == &arrow::datatypes::DataType::Utf8
                && (0..n_rg).all(|rg| {
                    let col = md.metadata().row_group(rg).column(*i);
                    col.dictionary_page_offset().is_some()
                        && col.encodings().iter().all(|e| {
                            matches!(
                                e,
                                parquet::basic::Encoding::PLAIN_DICTIONARY
                                    | parquet::basic::Encoding::RLE_DICTIONARY
                                    | parquet::basic::Encoding::RLE
                                    | parquet::basic::Encoding::PLAIN
                            )
                        })
                })
        })
        .map(|(i, _)| i)
        .collect();
    let coerced_schema: Option<std::sync::Arc<arrow::datatypes::Schema>> =
        (!dict_candidates.is_empty()).then(|| {
            std::sync::Arc::new(arrow::datatypes::Schema::new(
                file_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, f)| {
                        if dict_candidates.contains(&i) {
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
                    .collect::<Vec<_>>(),
            ))
        });

    (0..n_rg).into_par_iter().try_for_each(|rg| -> Result<()> {
        let file = File::open(parquet_path)?;
        let builder = match &coerced_schema {
            Some(cs) => {
                let opts = parquet::arrow::arrow_reader::ArrowReaderOptions::new()
                    .with_schema(cs.clone());
                match parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new_with_options(
                    file, opts,
                ) {
                    Ok(b) => b,
                    Err(_) => {
                        let file = File::open(parquet_path)?;
                        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::new_with_metadata(
                            file,
                            md.clone(),
                        )
                    }
                }
            }
            None => {
                parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::new_with_metadata(
                    file,
                    md.clone(),
                )
            }
        }
        .with_row_groups(vec![rg])
        .with_batch_size(65536);
        let reader = builder.build()?;
        let mut batches: Vec<RecordBatch> =
            reader.collect::<std::result::Result<Vec<_>, _>>()?;
        // Demote any column whose dictionary is wide back to plain Utf8.
        if coerced_schema.is_some() && !batches.is_empty() {
            let wide: Vec<usize> = batches[0]
                .columns()
                .iter()
                .enumerate()
                .filter(|(_, c): &(usize, &arrow::array::ArrayRef)| {
                    c.as_any()
                        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>()
                        .map(|d: &arrow::array::DictionaryArray<arrow::datatypes::Int32Type>| {
                            d.values().len() > 4096
                        })
                        .unwrap_or(false)
                })
                .map(|(i, _)| i)
                .collect();
            if !wide.is_empty() {
                batches = batches
                    .into_iter()
                    .map(|b| -> Result<RecordBatch> {
                        let cols: Vec<arrow::array::ArrayRef> = b
                            .columns()
                            .iter()
                            .enumerate()
                            .map(|(i, c)| {
                                if wide.contains(&i) {
                                    arrow::compute::cast(
                                        c.as_ref(),
                                        &arrow::datatypes::DataType::Utf8,
                                    )
                                    .map_err(|e| QueryError::Execution(e.to_string()))
                                } else {
                                    Ok(c.clone())
                                }
                            })
                            .collect::<Result<_>>()?;
                        let fields: Vec<arrow::datatypes::Field> = b
                            .schema()
                            .fields()
                            .iter()
                            .zip(&cols)
                            .map(|(f, c)| {
                                arrow::datatypes::Field::new(
                                    f.name(),
                                    c.data_type().clone(),
                                    f.is_nullable(),
                                )
                            })
                            .collect();
                        RecordBatch::try_new(
                            std::sync::Arc::new(arrow::datatypes::Schema::new(fields)),
                            cols,
                        )
                        .map_err(|e| QueryError::Execution(e.to_string()))
                    })
                    .collect::<Result<_>>()?;
            }
        }
        // Arrow IPC files allow ONE dictionary per field across the whole
        // file, but the parquet reader emits a fresh dictionary per batch.
        // Concat the row group (concat unifies dictionaries), then re-slice
        // to 64k zero-copy views that all share the unified dictionary.
        if !batches.is_empty()
            && batches[0]
                .columns()
                .iter()
                .any(|c| matches!(c.data_type(), arrow::datatypes::DataType::Dictionary(_, _)))
        {
            let unified =
                arrow::compute::concat_batches(&batches[0].schema(), batches.iter())
                    .map_err(|e| QueryError::Execution(e.to_string()))?;
            let mut sliced = Vec::with_capacity(unified.num_rows().div_ceil(65536));
            let mut off = 0;
            while off < unified.num_rows() {
                let len = (unified.num_rows() - off).min(65536);
                sliced.push(unified.slice(off, len));
                off += len;
            }
            batches = sliced;
        }
        let schema = batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| std::sync::Arc::new(arrow::datatypes::Schema::empty()));
        let f = File::create(rg_path(dir, rg))?;
        // Parquet-derived schemas give EVERY dictionary field dict_id 0; the
        // IPC writer tracks dictionaries by id, so preserving schema ids
        // makes the second dict field look like a forbidden replacement.
        // preserve_dict_id(false) has the writer assign unique ids.
        #[allow(deprecated)]
        let opts = arrow::ipc::writer::IpcWriteOptions::default().with_preserve_dict_id(false);
        let mut w = arrow::ipc::writer::FileWriter::try_new_with_options(
            std::io::BufWriter::new(f),
            &schema,
            opts,
        )
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
/// Lowercased names of the DICTIONARY-typed columns a sidecar stores
/// (from rg_00000's footer schema). Cached per directory. Read gates use
/// it: a scan that wants dict coercion may take the IPC path only when
/// every requested column is stored dict.
pub fn sidecar_dict_cols(dir: &Path) -> std::collections::HashSet<String> {
    static CACHE: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<PathBuf, std::collections::HashSet<String>>>,
    > = std::sync::OnceLock::new();
    let cache = CACHE.get_or_init(Default::default);
    if let Some(hit) = cache.lock().unwrap().get(dir) {
        return hit.clone();
    }
    let cols = (|| -> Option<std::collections::HashSet<String>> {
        let f = File::open(rg_path(dir, 0)).ok()?;
        let reader = arrow::ipc::reader::FileReader::try_new(f, None).ok()?;
        Some(
            reader
                .schema()
                .fields()
                .iter()
                .filter(|fl| matches!(fl.data_type(), arrow::datatypes::DataType::Dictionary(_, _)))
                .map(|fl| fl.name().to_lowercase())
                .collect(),
        )
    })()
    .unwrap_or_default();
    cache
        .lock()
        .unwrap()
        .insert(dir.to_path_buf(), cols.clone());
    cols
}

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
