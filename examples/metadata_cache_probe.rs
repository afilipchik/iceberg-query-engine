//! Inspect footer estimates and cache reuse without decoding table data.
//! Run through scripts/claude-safe-build.sh, like other engine diagnostics.
use query_engine::storage::metadata_cache::cached_metadata;
use serde_json::json;
use std::{path::PathBuf, sync::Arc};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let paths: Vec<PathBuf> = std::env::args_os().skip(1).map(PathBuf::from).collect();
    if paths.is_empty() {
        return Err("provide one or more Parquet file paths".into());
    }
    let metadata = paths
        .iter()
        .map(|path| cached_metadata(path))
        .collect::<Result<Vec<_>, _>>()?;
    let mut reports = Vec::new();
    for (path, first) in paths.iter().zip(&metadata) {
        let again = cached_metadata(path)?;
        reports.push(json!({
            "path": path,
            "rows": first.metadata().file_metadata().num_rows(),
            "row_groups": first.metadata().num_row_groups(),
            "parquet_metadata_estimated_bytes": first.metadata().memory_size(),
            "retained_after_working_set_load": Arc::ptr_eq(first.metadata(), again.metadata()),
        }));
    }
    println!("{}", serde_json::to_string_pretty(&reports)?);
    Ok(())
}
