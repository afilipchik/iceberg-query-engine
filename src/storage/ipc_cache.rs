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
pub fn ensure_sidecar(parquet_path: &Path) -> Option<PathBuf> {
    let dir = sidecar_dir(parquet_path);
    let src_meta = std::fs::metadata(parquet_path).ok()?;
    let stamp = dir.join(".complete");
    if let Ok(s) = std::fs::read_to_string(&stamp) {
        let want = format!(
            "{}:{}",
            src_meta.len(),
            src_meta
                .modified()
                .ok()?
                .duration_since(std::time::UNIX_EPOCH)
                .ok()?
                .as_secs()
        );
        if s == want {
            return Some(dir);
        }
    }
    build_sidecar(parquet_path, &dir, &src_meta).ok()?;
    Some(dir)
}

fn build_sidecar(parquet_path: &Path, dir: &Path, src_meta: &std::fs::Metadata) -> Result<()> {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir)?;
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
    let stamp = format!(
        "{}:{}",
        src_meta.len(),
        src_meta
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| QueryError::Execution(e.to_string()))?
            .as_secs()
    );
    std::fs::write(dir.join(".complete"), stamp)?;
    Ok(())
}

/// Read one row group's batches from the sidecar, applying an optional
/// projection (file-schema column indices).
pub fn read_row_group(
    dir: &Path,
    rg_idx: usize,
    projection: Option<&[usize]>,
) -> Result<Vec<RecordBatch>> {
    let f = File::open(rg_path(dir, rg_idx))?;
    let reader = arrow::ipc::reader::FileReader::try_new(
        std::io::BufReader::with_capacity(1 << 20, f),
        projection.map(|p| p.to_vec()),
    )
    .map_err(|e| QueryError::Execution(e.to_string()))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| QueryError::Execution(e.to_string()))
}
