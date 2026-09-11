//! Parsed Parquet metadata cached by opened-file identity and schema owner.
//! Builders use the same file handle as lookup/parse, avoiding a second open
//! after pathname replacement. This is not a lock against later in-place writes.
use crate::{error::Result, QueryError};
use arrow::datatypes::SchemaRef;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use std::{
    collections::HashMap,
    fs::{File, Metadata},
    path::{Path, PathBuf},
    time::SystemTime,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Fingerprint {
    length: u64,
    modified: SystemTime,
    #[cfg(unix)]
    identity: (u64, u64, i64, i64),
}
impl Fingerprint {
    fn read(metadata: &Metadata) -> std::io::Result<Self> {
        Ok(Self {
            length: metadata.len(),
            modified: metadata.modified()?,
            #[cfg(unix)]
            identity: {
                use std::os::unix::fs::MetadataExt;
                (
                    metadata.dev(),
                    metadata.ino(),
                    metadata.ctime(),
                    metadata.ctime_nsec(),
                )
            },
        })
    }
}
struct Entry {
    metadata: ArrowReaderMetadata,
    // Explicitly retain the schema whose address keys this entry, even if the
    // reader library normalizes/replaces the supplied schema internally.
    _schema: Option<SchemaRef>,
    reported_bytes: usize,
    last_used: std::sync::atomic::AtomicU64,
}
struct FileCache {
    fingerprint: Fingerprint,
    entries: HashMap<Option<usize>, Entry>,
}
// Retention limits, not a claim about parser peak or exact process RSS.
const MAX_CACHE_ENTRIES: usize = 256;
const MAX_REPORTED_BYTES: usize = 256 * 1024 * 1024;
static CLOCK: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
fn tick() -> u64 {
    CLOCK.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}
fn reported_size(metadata: &ArrowReaderMetadata, schema: Option<&SchemaRef>) -> usize {
    fn schema_size(schema: &SchemaRef) -> usize {
        schema
            .fields()
            .iter()
            .fold(512usize, |n, f| n.saturating_add(f.size()))
            .saturating_add(schema.metadata().iter().fold(0usize, |n, (k, v)| {
                n.saturating_add(k.capacity()).saturating_add(v.capacity())
            }))
    }
    metadata
        .metadata()
        .memory_size()
        .saturating_add(schema_size(metadata.schema()))
        .saturating_add(schema.map_or(0, schema_size))
        .saturating_add(512)
}
fn trim(map: &mut HashMap<PathBuf, FileCache>, entries_limit: usize, reported_limit: usize) {
    loop {
        let (entries, reported) = map
            .values()
            .flat_map(|f| f.entries.values())
            .fold((0usize, 0usize), |(n, b), e| {
                (n.saturating_add(1), b.saturating_add(e.reported_bytes))
            });
        if entries <= entries_limit && reported <= reported_limit {
            break;
        }
        let oldest = map
            .iter()
            .flat_map(|(path, f)| {
                f.entries.iter().map(move |(key, e)| {
                    (
                        path,
                        key,
                        e.last_used.load(std::sync::atomic::Ordering::Relaxed),
                    )
                })
            })
            .min_by_key(|(_, _, age)| *age)
            .map(|(path, key, _)| (path.clone(), *key));
        let Some((path, key)) = oldest else {
            break;
        };
        let file = map.get_mut(&path).unwrap();
        file.entries.remove(&key);
        if file.entries.is_empty() {
            map.remove(&path);
        }
    }
}

static CACHE: parking_lot::RwLock<Option<HashMap<PathBuf, FileCache>>> =
    parking_lot::RwLock::new(None);

fn opened_metadata(
    path: &Path,
    file: &mut File,
    schema: Option<SchemaRef>,
) -> Result<ArrowReaderMetadata> {
    let before = Fingerprint::read(&file.metadata()?)?;
    let key = schema.as_ref().map(|s| std::sync::Arc::as_ptr(s) as usize);
    // Without stable file identity, bypass cache reuse rather than accepting a
    // same-size/same-mtime replacement as the same file.
    let cached = if cfg!(unix) {
        let guard = CACHE.read();
        guard
            .as_ref()
            .and_then(|map| map.get(path))
            .filter(|entry| entry.fingerprint == before)
            .and_then(|entry| entry.entries.get(&key))
            .map(|entry| {
                entry
                    .last_used
                    .store(tick(), std::sync::atomic::Ordering::Relaxed);
                entry.metadata.clone()
            })
    } else {
        None
    };
    let cache_hit = cached.is_some();
    let metadata = if let Some(metadata) = cached {
        metadata
    } else {
        let options = match &schema {
            Some(schema) => ArrowReaderOptions::new().with_schema(schema.clone()),
            None => ArrowReaderOptions::new(),
        };
        ArrowReaderMetadata::load(&mut *file, options)?
    };
    if Fingerprint::read(&file.metadata()?)? != before {
        return Err(QueryError::Storage(
            "Parquet file changed while loading metadata".into(),
        ));
    }
    if cfg!(unix) && !cache_hit {
        let mut guard = CACHE.write();
        let map = guard.get_or_insert_with(HashMap::new);
        let reported_bytes = reported_size(&metadata, schema.as_ref());
        if reported_bytes > MAX_REPORTED_BYTES {
            // An entry that cannot fit alone must not flush unrelated hot data.
            // Still remove obsolete variants for this replaced pathname.
            if map
                .get(path)
                .is_some_and(|entry| entry.fingerprint != before)
            {
                map.remove(path);
            }
            return Ok(metadata);
        }
        // A changed version drops all stale schema variants from the cache;
        // active readers keep their own Arc-backed metadata and open handle.
        if map
            .get(path)
            .is_none_or(|entry| entry.fingerprint != before)
        {
            map.insert(
                path.to_path_buf(),
                FileCache {
                    fingerprint: before,
                    entries: HashMap::new(),
                },
            );
        }
        map.get_mut(path)
            .unwrap()
            .entries
            .entry(key)
            .or_insert_with(|| Entry {
                metadata: metadata.clone(),
                reported_bytes,
                last_used: std::sync::atomic::AtomicU64::new(tick()),
                _schema: schema,
            });
        trim(map, MAX_CACHE_ENTRIES, MAX_REPORTED_BYTES);
    }
    Ok(metadata)
}

/// Matched opened file and metadata for consumers constructing their own reader.
/// Footer/cache allocation and immutable-snapshot policy remain caller contracts.
pub(crate) fn cached_open_file(path: &Path) -> Result<(File, ArrowReaderMetadata)> {
    let mut file = File::open(path)?;
    let metadata = opened_metadata(path, &mut file, None)?;
    Ok((file, metadata))
}
pub fn cached_metadata(path: &Path) -> Result<ArrowReaderMetadata> {
    Ok(cached_open_file(path)?.1)
}
pub fn cached_reader_builder(path: &Path) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    let (file, metadata) = cached_open_file(path)?;
    Ok(ParquetRecordBatchReaderBuilder::new_with_metadata(
        file, metadata,
    ))
}
pub fn cached_reader_builder_with_schema(
    path: &Path,
    schema: SchemaRef,
) -> Result<ParquetRecordBatchReaderBuilder<File>> {
    let mut file = File::open(path)?;
    let metadata = opened_metadata(path, &mut file, Some(schema))?;
    Ok(ParquetRecordBatchReaderBuilder::new_with_metadata(
        file, metadata,
    ))
}

/// Query-owned metadata, independent of global-cache retention. A later reader
/// either opens the version used for pruning or refuses before consuming data.
/// File contents must still remain immutable while an opened reader consumes them.
#[derive(Clone, Debug)]
pub(crate) struct ParquetSnapshot {
    path: PathBuf,
    fingerprint: Fingerprint,
    metadata: ArrowReaderMetadata,
    plain_metadata: ArrowReaderMetadata,
}

impl ParquetSnapshot {
    pub(crate) fn open(path: &Path, schema: Option<SchemaRef>) -> Result<Self> {
        if !cfg!(unix) {
            return Err(QueryError::NotImplemented(
                "Version-checked Parquet scan requires Unix file identity".into(),
            ));
        }
        let mut file = File::open(path)?;
        let fingerprint = Fingerprint::read(&file.metadata()?)?;
        let plain_metadata = opened_metadata(path, &mut file, None)?;
        let metadata = if let Some(schema) = schema {
            ArrowReaderMetadata::try_new(
                plain_metadata.metadata().clone(),
                ArrowReaderOptions::new().with_schema(schema),
            )?
        } else {
            plain_metadata.clone()
        };
        if Fingerprint::read(&file.metadata()?)? != fingerprint {
            return Err(QueryError::Storage(
                "Parquet file changed while preparing scan metadata".into(),
            ));
        }
        Ok(Self {
            path: path.to_path_buf(),
            fingerprint,
            metadata,
            plain_metadata,
        })
    }

    pub(crate) fn metadata(&self) -> &ArrowReaderMetadata {
        &self.metadata
    }

    pub(crate) fn plain_metadata(&self) -> &ArrowReaderMetadata {
        &self.plain_metadata
    }

    pub(crate) fn reader_builder(&self) -> Result<ParquetRecordBatchReaderBuilder<File>> {
        Ok(ParquetRecordBatchReaderBuilder::new_with_metadata(
            self.open_file()?,
            self.metadata.clone(),
        ))
    }

    pub(crate) fn open_file(&self) -> Result<File> {
        let file = File::open(&self.path)?;
        if Fingerprint::read(&file.metadata()?)? != self.fingerprint {
            return Err(QueryError::Storage(format!(
                "Parquet file changed after scan planning: {}",
                self.path.display()
            )));
        }
        Ok(file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Array, Int64Array},
        record_batch::RecordBatch,
    };
    fn write(path: &Path, values: Vec<i64>) {
        let batch = RecordBatch::try_from_iter(vec![(
            "v",
            std::sync::Arc::new(Int64Array::from(values)) as std::sync::Arc<dyn Array>,
        )])
        .unwrap();
        let mut writer =
            parquet::arrow::ArrowWriter::try_new(File::create(path).unwrap(), batch.schema(), None)
                .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
    #[test]
    fn replacement_with_preserved_mtime_invalidates_both_caches() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.parquet");
        write(&path, vec![1, 2]);
        let old_time = File::open(&path)
            .unwrap()
            .metadata()
            .unwrap()
            .modified()
            .unwrap();
        let schema = cached_metadata(&path).unwrap().schema().clone();
        assert_eq!(
            cached_reader_builder_with_schema(&path, schema.clone())
                .unwrap()
                .metadata()
                .file_metadata()
                .num_rows(),
            2
        );
        let replacement = directory.path().join("replacement.parquet");
        write(&replacement, vec![10, 20, 30, 40, 50]);
        File::open(&replacement)
            .unwrap()
            .set_times(std::fs::FileTimes::new().set_modified(old_time))
            .unwrap();
        std::fs::rename(replacement, &path).unwrap();
        assert_eq!(
            cached_metadata(&path)
                .unwrap()
                .metadata()
                .file_metadata()
                .num_rows(),
            5
        );
        let mut reader = cached_reader_builder_with_schema(&path, schema)
            .unwrap()
            .build()
            .unwrap();
        let actual = reader
            .by_ref()
            .flat_map(|b| {
                b.unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![10, 20, 30, 40, 50]);
    }
    #[test]
    fn same_size_replacement_refreshes_statistics_and_keeps_open_builder_pinned() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.parquet");
        write(&path, vec![1, 2]);
        let old_file = File::open(&path).unwrap();
        let stamp = old_file.metadata().unwrap();
        let old_metadata = cached_metadata(&path).unwrap();
        let pinned = cached_reader_builder(&path).unwrap();
        let replacement = directory.path().join("replacement.parquet");
        write(&replacement, vec![99, 100]);
        let new_file = File::open(&replacement).unwrap();
        assert_eq!(new_file.metadata().unwrap().len(), stamp.len());
        new_file
            .set_times(std::fs::FileTimes::new().set_modified(stamp.modified().unwrap()))
            .unwrap();
        std::fs::rename(replacement, &path).unwrap();
        let new_metadata = cached_metadata(&path).unwrap();
        assert!(!std::sync::Arc::ptr_eq(
            old_metadata.metadata(),
            new_metadata.metadata()
        ));
        assert_ne!(
            old_metadata.metadata().row_group(0).column(0).statistics(),
            new_metadata.metadata().row_group(0).column(0).statistics()
        );
        let original = pinned
            .build()
            .unwrap()
            .flat_map(|b| {
                b.unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(original, vec![1, 2]);
        let current = cached_reader_builder(&path)
            .unwrap()
            .build()
            .unwrap()
            .flat_map(|b| {
                b.unwrap()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect::<Vec<_>>();
        assert_eq!(current, vec![99, 100]);
    }
    #[cfg(unix)]
    #[test]
    fn schema_identity_is_retained_and_unchanged_files_reuse_metadata() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.parquet");
        write(&path, vec![1, 2]);
        let first = cached_metadata(&path).unwrap();
        let second = cached_metadata(&path).unwrap();
        assert!(std::sync::Arc::ptr_eq(first.metadata(), second.metadata()));
        let schema = std::sync::Arc::new(first.schema().as_ref().clone());
        let weak = std::sync::Arc::downgrade(&schema);
        let identity = std::sync::Arc::as_ptr(&schema) as usize;
        drop(cached_reader_builder_with_schema(&path, schema).unwrap());
        assert!(weak.upgrade().is_some());
        let guard = CACHE.read();
        let cached = &guard.as_ref().unwrap().get(&path).unwrap().entries[&Some(identity)];
        assert!(std::sync::Arc::ptr_eq(
            cached._schema.as_ref().unwrap(),
            &weak.upgrade().unwrap()
        ));
    }
    #[test]
    fn retention_limits_evict_old_entries_without_invalidating_readers() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.parquet");
        write(&path, vec![1, 2]);
        let (file, metadata) = cached_open_file(&path).unwrap();
        let stamp = Fingerprint::read(&file.metadata().unwrap()).unwrap();
        let mut map = HashMap::new();
        let mut entries = HashMap::new();
        for (key, age, weight) in [(None, 1, 40), (Some(1), 3, 40), (Some(2), 2, 40)] {
            entries.insert(
                key,
                Entry {
                    metadata: metadata.clone(),
                    _schema: None,
                    reported_bytes: weight,
                    last_used: std::sync::atomic::AtomicU64::new(age),
                },
            );
        }
        map.insert(
            path.clone(),
            FileCache {
                fingerprint: stamp,
                entries,
            },
        );
        trim(&mut map, 2, 1000);
        assert!(!map[&path].entries.contains_key(&None));
        trim(&mut map, 2, 40);
        assert_eq!(map[&path].entries.len(), 1);
        assert!(map[&path].entries.contains_key(&Some(1)));
        trim(&mut map, 0, 0);
        assert!(map.is_empty());
        let rows = ParquetRecordBatchReaderBuilder::new_with_metadata(file, metadata)
            .build()
            .unwrap()
            .map(|b| b.unwrap().num_rows())
            .sum::<usize>();
        assert_eq!(rows, 2);
    }

    #[test]
    fn query_snapshot_survives_cache_eviction_without_reparsing() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("data.parquet");
        write(&path, vec![7, 7, 11]);
        let schema = cached_metadata(&path).unwrap().schema().clone();
        for override_schema in [None, Some(schema)] {
            let snapshot = ParquetSnapshot::open(&path, override_schema).unwrap();
            CACHE.write().as_mut().unwrap().remove(&path);
            let builder = snapshot.reader_builder().unwrap();
            assert!(std::sync::Arc::ptr_eq(
                snapshot.metadata.metadata(),
                builder.metadata()
            ));
            let actual: Vec<_> = builder
                .build()
                .unwrap()
                .flat_map(|batch| {
                    batch
                        .unwrap()
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .to_vec()
                })
                .collect();
            assert_eq!(actual, vec![7, 7, 11]);
        }
    }
}
