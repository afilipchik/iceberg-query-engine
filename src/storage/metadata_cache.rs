//! Global parquet footer metadata cache.
//!
//! Every row-group read used to reopen the file and re-parse the footer
//! (arrow's `try_new` path): lineitem at SF=10 has ~900 row groups, so a
//! single scan parsed the same multi-column footer ~900 times. The cache
//! parses once per (path, mtime) and hands out cheap clones
//! (`ArrowReaderMetadata` is Arc-backed).

use crate::error::Result;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

static CACHE: parking_lot::RwLock<Option<HashMap<PathBuf, (SystemTime, ArrowReaderMetadata)>>> =
    parking_lot::RwLock::new(None);

/// Cached footer metadata for `path` (plain reader options).
pub fn cached_metadata(path: &Path) -> Result<ArrowReaderMetadata> {
    let mtime = std::fs::metadata(path)?.modified()?;
    {
        let guard = CACHE.read();
        if let Some(map) = guard.as_ref() {
            if let Some((t, md)) = map.get(path) {
                if *t == mtime {
                    return Ok(md.clone());
                }
            }
        }
    }
    let mut file = File::open(path)?;
    let md = ArrowReaderMetadata::load(&mut file, ArrowReaderOptions::new())?;
    let mut guard = CACHE.write();
    guard
        .get_or_insert_with(HashMap::new)
        .insert(path.to_path_buf(), (mtime, md.clone()));
    Ok(md)
}

/// Reader builder over `path` without re-parsing the footer.
pub fn cached_reader_builder(path: &Path) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    let md = cached_metadata(path)?;
    let file = File::open(path)?;
    Ok(ParquetRecordBatchReaderBuilder::new_with_metadata(file, md))
}

static SCHEMA_CACHE: parking_lot::RwLock<
    Option<HashMap<(PathBuf, usize), (SystemTime, ArrowReaderMetadata)>>,
> = parking_lot::RwLock::new(None);

/// Reader builder with a coercion schema override (e.g. dictionary string
/// columns), footer parsed once per (path, schema-identity).
pub fn cached_reader_builder_with_schema(
    path: &Path,
    schema: arrow::datatypes::SchemaRef,
) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    let key = (path.to_path_buf(), std::sync::Arc::as_ptr(&schema) as usize);
    let mtime = std::fs::metadata(path)?.modified()?;
    {
        let guard = SCHEMA_CACHE.read();
        if let Some(map) = guard.as_ref() {
            if let Some((t, md)) = map.get(&key) {
                if *t == mtime {
                    let file = File::open(path)?;
                    return Ok(ParquetRecordBatchReaderBuilder::new_with_metadata(
                        file,
                        md.clone(),
                    ));
                }
            }
        }
    }
    let mut file = File::open(path)?;
    let md = ArrowReaderMetadata::load(
        &mut file,
        ArrowReaderOptions::new().with_schema(schema.clone()),
    )?;
    SCHEMA_CACHE
        .write()
        .get_or_insert_with(HashMap::new)
        .insert(key, (mtime, md.clone()));
    let file = File::open(path)?;
    Ok(ParquetRecordBatchReaderBuilder::new_with_metadata(file, md))
}
