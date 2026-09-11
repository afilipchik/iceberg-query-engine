//! Private query-local spill files. Publication is the transfer of a completed
//! run capability, not a durable catalog entry. The outer query owns the parent
//! directory; this owner removes only the unique file it successfully created.
use super::{
    group_rows::{GroupLayout, GroupRows},
    spill_frames::{FrameCursor, FrameScratch, RunIdentity, FRAME_OVERHEAD},
    spill_io::{Reader, Writer},
};
use crate::{
    execution::{
        reserved_vec::{ReservedIntoIter, ReservedVec},
        MemoryReservation,
    },
    QueryError, Result,
};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    mem::size_of,
    path::{Path, PathBuf},
    sync::Arc,
};

fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("aggregate spill file: {message}"))
}

fn admitted_path(
    layout: &GroupLayout,
    directory: &Path,
    prefix: &[u8; 5],
    owner_bytes: usize,
) -> Result<(PathBuf, MemoryReservation)> {
    // Fixed filename formatting uses stack storage, before any path copy.
    let mut name = [0u8; 47];
    name[..5].copy_from_slice(prefix);
    uuid::Uuid::new_v4()
        .hyphenated()
        .encode_lower(&mut name[5..41]);
    name[41..].copy_from_slice(b".spill");
    let path_bytes = directory
        .as_os_str()
        .as_encoded_bytes()
        .len()
        .checked_add(1 + name.len())
        .ok_or_else(|| invalid("path size overflow"))?;
    let charge = path_bytes
        .checked_add(512 + owner_bytes)
        .ok_or_else(|| invalid("owner size overflow"))?;
    let reservation = layout.pool().allocate(charge)?;
    let mut path = PathBuf::new();
    path.try_reserve_exact(path_bytes)
        .map_err(|e| invalid(&format!("path allocation refused: {e}")))?;
    if path.capacity() != path_bytes {
        return Err(invalid("path capacity differs from admission"));
    }
    path.push(directory);
    path.push(std::str::from_utf8(&name).unwrap());
    Ok((path, reservation))
}

pub(super) struct RunDirectory {
    path: PathBuf,
    layout: Arc<GroupLayout>,
    _reservation: MemoryReservation,
}
impl RunDirectory {
    pub(super) fn create(layout: Arc<GroupLayout>, parent: &Path) -> Result<Arc<Self>> {
        let (path, reservation) = admitted_path(&layout, parent, b"qe-d-", size_of::<Self>())?;
        // Create exactly one new directory; never adopt an existing directory.
        std::fs::create_dir(&path)?;
        Ok(Arc::new(Self {
            path,
            layout,
            _reservation: reservation,
        }))
    }
}
impl Drop for RunDirectory {
    fn drop(&mut self) {
        // Never recursively remove foreign entries. Run owners remove their files
        // before releasing the final directory reference.
        if let Err(error) = std::fs::remove_dir(&self.path) {
            if error.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(path = %self.path.display(), %error, "spill directory cleanup failed");
            }
        }
    }
}

/// The collection itself is admitted, not just each file's path/handle owner.
/// Prepare the next flush before filling resident state to its working budget.
pub(super) struct RunCollection {
    runs: ReservedVec<SpillRun>,
    layout: Arc<GroupLayout>,
}
impl RunCollection {
    fn share(&self) -> Result<Self> {
        let mut runs = ReservedVec::with_capacity(self.layout.pool(), self.runs.as_slice().len())?;
        runs.extend_reserved(
            self.runs.as_slice().len(),
            self.runs.as_slice().iter().map(|run| SpillRun {
                owner: run.owner.clone(),
                identity: run.identity,
                count: run.count,
                bytes: run.bytes,
            }),
        )?;
        Ok(Self {
            runs,
            layout: self.layout.clone(),
        })
    }

    /// Compact without dropping original run capabilities on failure. Shared
    /// copies have separately admitted collection metadata, not cloned payloads.
    pub(super) fn compact(
        &mut self,
        directory: &Arc<RunDirectory>,
        frame_capacity: usize,
        group_limit: usize,
    ) -> Result<usize> {
        self.compact_with(directory, frame_capacity, group_limit, |_| Ok(()))
    }
    fn compact_with(
        &mut self,
        directory: &Arc<RunDirectory>,
        frame_capacity: usize,
        group_limit: usize,
        before_finish: impl FnOnce(&mut RunWriter) -> Result<()>,
    ) -> Result<usize> {
        if group_limit == 0 {
            return Err(invalid("compaction group limit must be positive"));
        }
        if self.runs.as_slice().is_empty() {
            return Ok(0);
        }
        let mut writer = RunWriter::create(self.layout.clone(), directory)?;
        let scheduler = super::partition_scheduler::PartitionScheduler::new(
            self.layout.clone(),
            directory.clone(),
            self.share()?,
            frame_capacity,
            group_limit,
        )?;
        let splits = scheduler.visit(|groups| {
            for row in 0..groups.len() {
                writer.append(groups, row)?;
            }
            Ok(())
        })?;
        before_finish(&mut writer)?;
        let completed = writer.finish()?;
        // Capacity for the existing first element is already owned. Publication
        // needs no admission or fallible writes; replaced owners clean up on drop.
        self.runs.as_mut_slice()[0] = completed;
        self.runs.truncate(1);
        Ok(splits)
    }

    /// Call between batches, before filling resident state. The resulting flush
    /// cannot grow the published collection beyond max_runs.
    pub(super) fn prepare_flush_bounded(
        &mut self,
        directory: &Arc<RunDirectory>,
        max_runs: usize,
        frame_capacity: usize,
        group_limit: usize,
    ) -> Result<PreparedFlush<'_>> {
        if max_runs < 2 {
            return Err(invalid("run limit must be at least two"));
        }
        if group_limit == 0 {
            return Err(invalid("compaction group limit must be positive"));
        }
        if self.runs.as_slice().len() >= max_runs {
            self.compact(directory, frame_capacity, group_limit)?;
        }
        self.prepare_flush(directory)
    }
    #[cfg(test)]
    fn prepare_flush_in(&mut self, parent: &Path) -> Result<PreparedFlush<'_>> {
        let directory = RunDirectory::create(self.layout.clone(), parent)?;
        self.prepare_flush(&directory)
    }

    pub(super) fn new(layout: Arc<GroupLayout>) -> Result<Self> {
        Ok(Self {
            runs: ReservedVec::with_capacity(layout.pool(), 0)?,
            layout,
        })
    }
    pub(super) fn reserve_slots(&mut self, additional: usize) -> Result<()> {
        self.runs.reserve(additional)
    }
    pub(super) fn adopt_run(&mut self, run: SpillRun) -> Result<()> {
        if run.layout_identity() != self.layout.identity() {
            return Err(invalid("adopted run layout mismatch"));
        }
        self.runs.extend_reserved(1, std::iter::once(run))
    }
    pub(super) fn runs(&self) -> &[SpillRun] {
        self.runs.as_slice()
    }
    pub(super) fn prepare_flush(
        &mut self,
        directory: &Arc<RunDirectory>,
    ) -> Result<PreparedFlush<'_>> {
        self.runs.reserve(1)?;
        let writer = RunWriter::create(self.layout.clone(), directory)?;
        Ok(PreparedFlush {
            collection: self,
            writer,
        })
    }
    pub(super) fn flush_writer(&mut self, writer: RunWriter, groups: &mut GroupRows) -> Result<()> {
        PreparedFlush {
            collection: self,
            writer,
        }
        .flush(groups)
    }
    /// Publish a writer whose slot was admitted by prepare_flush. The caller
    /// retains all borrowed source state until append has succeeded.
    pub(super) fn publish_writer(&mut self, writer: RunWriter) -> Result<u64> {
        if writer.owner.layout.identity() != self.layout.identity() {
            return Err(invalid("published writer layout mismatch"));
        }
        let run = writer.finish()?;
        let bytes = run.bytes;
        self.runs.extend_reserved(1, std::iter::once(run))?;
        Ok(bytes)
    }

    pub(super) fn last_run_bytes(&self) -> u64 {
        self.runs.as_slice().last().map_or(0, |run| run.bytes)
    }
    pub(super) fn into_runs(self) -> ReservedIntoIter<SpillRun> {
        self.runs.into_owned_iter()
    }
}

pub(super) struct PreparedFlush<'a> {
    collection: &'a mut RunCollection,
    writer: RunWriter,
}
impl PreparedFlush<'_> {
    /// Detach the prepared writer for an owning ingestion controller. The slot
    /// remains reserved in the collection; publication still checks capacity.
    pub(super) fn into_writer(self) -> RunWriter {
        self.writer
    }

    pub(super) fn flush(self, groups: &mut GroupRows) -> Result<()> {
        self.flush_with(groups, |_| Ok(()))
    }
    fn flush_with(
        mut self,
        groups: &mut GroupRows,
        before_finish: impl FnOnce(&mut RunWriter) -> Result<()>,
    ) -> Result<()> {
        if groups.layout_identity() != self.collection.layout.identity() {
            return Err(invalid("flush source layout mismatch"));
        }
        if groups.len() == 0 {
            return Ok(());
        }
        for row in 0..groups.len() {
            self.writer.append(groups, row)?;
        }
        before_finish(&mut self.writer)?;
        let run = self.writer.finish()?;
        // Exclusive collection ownership preserves the pre-admitted slot. If
        // publication ever fails, its iterator drops the file; source survives.
        self.collection
            .runs
            .extend_reserved(1, std::iter::once(run))?;
        groups.clear();
        Ok(())
    }
}

struct RunFile {
    _directory: Arc<RunDirectory>,
    path: PathBuf,
    created: bool,
    layout: Arc<GroupLayout>,
    _reservation: MemoryReservation,
}
impl Drop for RunFile {
    fn drop(&mut self) {
        if self.created {
            if let Err(error) = std::fs::remove_file(&self.path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = %self.path.display(), %error, "spill file cleanup failed");
                }
            }
        }
    }
}

pub(super) struct RunWriter {
    // Close the writer before dropping the last file owner.
    file: Writer<File>,
    owner: Arc<RunFile>,
    cursor: FrameCursor,
    identity: RunIdentity,
    count: u64,
    bytes: u64,
    poisoned: bool,
}

impl RunWriter {
    pub(super) fn append_frame(&mut self, frame: &FrameScratch) -> Result<()> {
        if self.poisoned {
            return Err(invalid("writer previously failed"));
        }
        self.poisoned = true;
        let count = self
            .count
            .checked_add(1)
            .ok_or_else(|| invalid("row count overflow"))?;
        let length =
            u64::try_from(frame.payload()?.len()).map_err(|_| invalid("row size overflow"))?;
        let bytes = self
            .bytes
            .checked_add(length)
            .and_then(|n| n.checked_add(FRAME_OVERHEAD))
            .ok_or_else(|| invalid("file size overflow"))?;
        self.cursor.copy(frame, &mut self.file)?;
        self.count = count;
        self.bytes = bytes;
        self.poisoned = false;
        Ok(())
    }
    #[cfg(test)]
    pub(super) fn create_in(layout: Arc<GroupLayout>, parent: &Path) -> Result<Self> {
        let directory = RunDirectory::create(layout.clone(), parent)?;
        Self::create(layout, &directory)
    }

    pub(super) fn create(layout: Arc<GroupLayout>, directory: &Arc<RunDirectory>) -> Result<Self> {
        if layout.identity() != directory.layout.identity() {
            return Err(invalid("directory layout mismatch"));
        }
        let (path, reservation) = admitted_path(
            &layout,
            &directory.path,
            b"qe-r-",
            size_of::<RunFile>() + size_of::<RunWriter>() + size_of::<SpillRun>(),
        )?;
        let identity = RunIdentity::new(&layout);
        let mut owner = RunFile {
            _directory: directory.clone(),
            path,
            created: false,
            layout,
            _reservation: reservation,
        };
        // A failed create_new never adopts or removes an existing file.
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&owner.path)?;
        owner.created = true;
        let file = Writer::new(file, owner.layout.pool())?;
        Ok(Self {
            file,
            owner: Arc::new(owner),
            cursor: FrameCursor::new(identity),
            identity,
            count: 0,
            bytes: 0,
            poisoned: false,
        })
    }

    pub(super) fn append(&mut self, groups: &GroupRows, row: usize) -> Result<()> {
        if self.poisoned {
            return Err(invalid("writer previously failed"));
        }
        self.poisoned = true;
        let count = self
            .count
            .checked_add(1)
            .ok_or_else(|| invalid("row count overflow"))?;
        let row_bytes =
            u64::try_from(groups.encoded_size(row)?).map_err(|_| invalid("row size overflow"))?;
        let bytes = self
            .bytes
            .checked_add(row_bytes)
            .and_then(|n| n.checked_add(FRAME_OVERHEAD))
            .ok_or_else(|| invalid("file size overflow"))?;
        self.cursor.write(groups, row, &mut self.file)?;
        self.count = count;
        self.bytes = bytes;
        self.poisoned = false;
        Ok(())
    }

    /// Only successful completion produces a readable capability. No fsync is
    /// required for ephemeral query spill; this is not crash-durable storage.
    pub(super) fn finish(mut self) -> Result<SpillRun> {
        if self.poisoned {
            return Err(invalid("cannot publish failed writer"));
        }
        self.file.flush()?;
        if self.file.inner().metadata()?.len() != self.bytes {
            return Err(invalid("completed file length mismatch"));
        }
        drop(self.file);
        Ok(SpillRun {
            owner: self.owner,
            identity: self.identity,
            count: self.count,
            bytes: self.bytes,
        })
    }
}

pub(super) struct SpillRun {
    owner: Arc<RunFile>,
    identity: RunIdentity,
    count: u64,
    bytes: u64,
}
impl SpillRun {
    pub(super) fn row_count(&self) -> u64 {
        self.count
    }
    pub(super) fn layout_identity(&self) -> uuid::Uuid {
        self.owner.layout.identity()
    }
    pub(super) fn reader(&self) -> Result<RunReader> {
        let reservation = self
            .owner
            .layout
            .pool()
            .allocate(512 + size_of::<RunReader>())?;
        // Open separately: File::try_clone would share the seek offset on Unix.
        let file = File::open(&self.owner.path)?;
        if file.metadata()?.len() != self.bytes {
            return Err(invalid("published file length mismatch"));
        }
        let file = Reader::new(file, self.owner.layout.pool())?;
        Ok(RunReader {
            file,
            _owner: self.owner.clone(),
            cursor: FrameCursor::new(self.identity),
            remaining: self.count,
            poisoned: false,
            _reservation: reservation,
        })
    }
}

pub(super) struct RunReader {
    file: Reader<File>,
    _owner: Arc<RunFile>,
    cursor: FrameCursor,
    remaining: u64,
    poisoned: bool,
    _reservation: MemoryReservation,
}
impl RunReader {
    /// Reuse this query-owned reader slot only after verified completion. Open
    /// the new handle before replacing the old one; no new reservation is needed.
    pub(super) fn reset(&mut self, run: &SpillRun) -> Result<()> {
        if self.poisoned || self.remaining != 0 {
            return Err(invalid("reader reset before complete run"));
        }
        if self._owner.layout.identity() != run.owner.layout.identity() {
            return Err(invalid("reader reset layout mismatch"));
        }
        self.poisoned = true;
        let mut byte = [0];
        if self.file.read(&mut byte)? != 0 {
            return Err(invalid("extra bytes before reader reset"));
        }
        let file = File::open(&run.owner.path)?;
        if file.metadata()?.len() != run.bytes {
            return Err(invalid("reset file length mismatch"));
        }
        self.file.reset(file);
        self._owner = run.owner.clone();
        self.cursor = FrameCursor::new(run.identity);
        self.remaining = run.count;
        self.poisoned = false;
        Ok(())
    }
    pub(super) fn next<'a>(&mut self, scratch: &'a mut FrameScratch) -> Result<Option<&'a [u8]>> {
        if self.poisoned {
            return Err(invalid("reader previously failed"));
        }
        if self.remaining == 0 {
            self.poisoned = true;
            let mut byte = [0];
            if self.file.read(&mut byte)? != 0 {
                return Err(invalid("extra bytes after declared frame count"));
            }
            self.poisoned = false;
            return Ok(None);
        }
        let payload = match self.cursor.read(&mut self.file, scratch) {
            Ok(payload) => payload,
            Err(error) => {
                self.poisoned = !error.is_memory_limit();
                return Err(error);
            }
        };
        if self.remaining == 1 {
            self.poisoned = true;
            let mut byte = [0];
            if self.file.read(&mut byte)? != 0 {
                return Err(invalid("extra bytes after final frame"));
            }
            self.poisoned = false;
        }
        self.remaining -= 1;
        Ok(Some(payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_run_accumulation_preserves_partial_weights_through_repeated_compaction() {
        use crate::planner::DecimalValue;
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("bounded runs", 262144);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[
                (AggregateFunction::Count, DataType::Int64, false),
                (AggregateFunction::Avg, DataType::Float64, false),
                (AggregateFunction::Sum, DataType::Decimal128(38, 2), false),
            ],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
        let mut runs = RunCollection::new(layout.clone()).unwrap();
        let mut groups = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        let exact = (1i128 << 80) + 17;
        for run in 1..=12 {
            let flush = runs.prepare_flush_bounded(&directory, 3, 1024, 2).unwrap();
            for k in 0..7 {
                key.encode(&[if k == 6 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Int64(k)
                }])
                .unwrap();
                for _ in 0..run {
                    groups
                        .prepare_update(
                            &key,
                            &mut row,
                            &[
                                ScalarValue::Int64(1),
                                ScalarValue::Float64((run as f64 * 10.0).into()),
                                ScalarValue::Decimal128(DecimalValue::new(
                                    exact * i128::from(run),
                                    2,
                                )),
                            ],
                        )
                        .unwrap()
                        .commit();
                }
            }
            flush.flush(&mut groups).unwrap();
            assert!(runs.runs().len() <= 3);
        }
        runs.compact(&directory, 1024, 2).unwrap();
        assert_eq!(runs.runs().len(), 1);
        assert_eq!(runs.runs()[0].row_count(), 7);
        let mut merge =
            super::super::run_merge::RunMerge::new(&runs.runs()[0], layout.clone(), 1024).unwrap();
        while merge.step(&mut groups).unwrap() {}
        assert_eq!(groups.len(), 7);
        for row in 0..7 {
            assert_eq!(
                groups.value(row, 0).unwrap().as_ref(),
                &ScalarValue::Int64(78)
            );
            assert_eq!(
                groups.value(row, 1).unwrap().as_ref(),
                &ScalarValue::Float64((6500.0 / 78.0).into())
            );
            assert_eq!(
                groups.value(row, 2).unwrap().as_ref(),
                &ScalarValue::Decimal128(DecimalValue::new(exact * 650, 2))
            );
        }
        drop((merge, runs, groups, key, row, directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn compaction_failure_keeps_original_runs_and_cleans_temporary_files() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("compact rollback", 131072);
        let (layout, mut groups) = fixture(&pool);
        let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
        let mut runs = RunCollection::new(layout.clone()).unwrap();
        runs.prepare_flush(&directory)
            .unwrap()
            .flush(&mut groups)
            .unwrap();
        let original = runs.runs()[0].owner.path.clone();
        let before = pool.used();
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(runs
            .compact(&directory, 256, 1)
            .unwrap_err()
            .is_memory_limit());
        drop(pressure);
        assert_eq!(pool.used(), before);
        assert!(runs
            .compact_with(&directory, 256, 1, |writer| {
                writer.file.flush()?;
                writer.file.inner_mut().set_len(0)?;
                Ok(())
            })
            .is_err());
        assert_eq!(runs.runs().len(), 1);
        assert_eq!(runs.runs()[0].owner.path, original);
        assert!(original.exists());
        assert_eq!(std::fs::read_dir(&directory.path).unwrap().count(), 1);
        assert_eq!(pool.used(), before);
        let mut reader = runs.runs()[0].reader().unwrap();
        let mut scratch = FrameScratch::new(&layout, 256).unwrap();
        let mut rows = 0;
        while reader.next(&mut scratch).unwrap().is_some() {
            rows += 1;
        }
        assert_eq!(rows, 2);
        drop((reader, scratch));
        runs.compact(&directory, 256, 1).unwrap();
        assert!(!original.exists());
        assert_eq!(runs.runs()[0].row_count(), 2);
        drop((runs, groups, directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn reader_slot_cannot_skip_an_unread_run_and_resets_without_free_memory() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("reader slot", 131072);
        let (layout, groups) = fixture(&pool);
        let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
        let mut writer = RunWriter::create(layout.clone(), &directory).unwrap();
        writer.append(&groups, 0).unwrap();
        let first = writer.finish().unwrap();
        let mut writer = RunWriter::create(layout.clone(), &directory).unwrap();
        writer.append(&groups, 1).unwrap();
        let second = writer.finish().unwrap();
        let mut reader = first.reader().unwrap();
        let mut scratch = FrameScratch::new(&layout, 256).unwrap();
        assert!(reader.reset(&second).is_err());
        assert_eq!(reader.remaining, 1);
        assert!(reader.next(&mut scratch).unwrap().is_some());
        let pressure = pool.allocate(pool.available()).unwrap();
        reader.reset(&second).unwrap();
        assert!(reader.next(&mut scratch).unwrap().is_some());
        assert!(reader.next(&mut scratch).unwrap().is_none());
        drop(pressure);
        // Even an already-completed run must not acquire a new trailing suffix.
        let mut edit = OpenOptions::new()
            .append(true)
            .open(&second.owner.path)
            .unwrap();
        edit.write_all(b"extra").unwrap();
        drop(edit);
        assert!(reader.reset(&first).is_err());
        assert!(reader.poisoned);
        drop((reader, scratch, first, second, groups, directory, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn directory_lives_until_the_last_run_reader_and_cleans_up_afterward() {
        let parent = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("directory lifetime", 131072);
        let (layout, groups) = fixture(&pool);
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let path = directory.path.clone();
        let mut writer = RunWriter::create(layout.clone(), &directory).unwrap();
        writer.append(&groups, 0).unwrap();
        let run = writer.finish().unwrap();
        let mut reader = run.reader().unwrap();
        drop((directory, run));
        assert!(path.is_dir());
        let mut scratch = FrameScratch::new(&layout, 256).unwrap();
        assert!(reader.next(&mut scratch).unwrap().is_some());
        assert!(reader.next(&mut scratch).unwrap().is_none());
        drop(reader);
        assert!(!path.exists());
        drop((scratch, groups, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn directory_admission_and_layout_refusal_preserve_foreign_paths() {
        let parent = tempfile::tempdir().unwrap();
        let sentinel = parent.path().join("keep");
        std::fs::write(&sentinel, b"foreign").unwrap();
        let pool = MemoryPool::new_named("directory refusal", 131072);
        let (layout, groups) = fixture(&pool);
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(RunDirectory::create(layout.clone(), parent.path())
            .err()
            .unwrap()
            .is_memory_limit());
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 1);
        drop(pressure);
        let before = pool.used();
        assert!(RunDirectory::create(layout.clone(), &sentinel).is_err());
        assert_eq!(pool.used(), before);
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let (other, other_groups) = fixture(&pool);
        let before = pool.used();
        assert!(RunWriter::create(other.clone(), &directory).is_err());
        assert_eq!(pool.used(), before);
        assert_eq!(std::fs::read_dir(&directory.path).unwrap().count(), 0);
        drop((directory, other, other_groups, groups, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read(&sentinel).unwrap(), b"foreign");
    }

    #[test]
    fn prepared_flush_publishes_before_releasing_groups_even_with_a_full_pool() {
        let directory = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("prepared flush", 131072);
        let (layout, mut groups) = fixture(&pool);
        let mut runs = RunCollection::new(layout.clone()).unwrap();
        let prepared = runs.prepare_flush_in(directory.path()).unwrap();
        let pressure = pool.allocate(pool.available()).unwrap();
        prepared.flush(&mut groups).unwrap();
        assert_eq!(groups.len(), 0);
        assert_eq!(runs.runs().len(), 1);
        drop(pressure);
        let path = runs.runs()[0].owner.path.clone();
        let mut scratch = FrameScratch::new(&layout, 256).unwrap();
        let mut reader = runs.runs()[0].reader().unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        while let Some(bytes) = reader.next(&mut scratch).unwrap() {
            groups
                .prepare_restore(&mut key, &mut row, bytes)
                .unwrap()
                .commit();
        }
        assert_eq!(groups.len(), 2);
        for index in 0..2 {
            assert_eq!(
                groups.value(index, 0).unwrap().as_ref(),
                &ScalarValue::Int64(1)
            );
        }
        let mut owned_runs = runs.into_runs();
        let run = owned_runs.next().unwrap();
        assert!(owned_runs.next().is_none());
        drop((run, owned_runs));
        assert!(path.exists());
        drop(reader);
        assert!(!path.exists());
        drop((scratch, key, row, groups, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn failed_or_abandoned_flush_keeps_source_and_preexisting_runs() {
        let directory = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("flush rollback", 131072);
        let (layout, mut groups) = fixture(&pool);
        let mut runs = RunCollection::new(layout.clone()).unwrap();
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(runs
            .prepare_flush_in(directory.path())
            .err()
            .unwrap()
            .is_memory_limit());
        assert_eq!(groups.len(), 2);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        drop(pressure);
        drop(runs.prepare_flush_in(directory.path()).unwrap());
        assert_eq!(groups.len(), 2);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        let failure =
            runs.prepare_flush_in(directory.path())
                .unwrap()
                .flush_with(&mut groups, |writer| {
                    writer.file.flush()?;
                    writer.file.inner_mut().set_len(0)?;
                    Ok(())
                });
        assert!(failure.is_err());
        assert_eq!(groups.len(), 2);
        assert!(runs.runs().is_empty());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        runs.prepare_flush_in(directory.path())
            .unwrap()
            .flush(&mut groups)
            .unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        key.encode(&[ScalarValue::Int64(7)]).unwrap();
        groups
            .prepare_update(&key, &mut row, &[ScalarValue::Int64(1)])
            .unwrap()
            .commit();
        let mut prepared = runs.prepare_flush_in(directory.path()).unwrap();
        *prepared.writer.file.inner_mut() = File::open(&prepared.writer.owner.path).unwrap();
        assert!(prepared.flush(&mut groups).is_err());
        assert_eq!(groups.len(), 1);
        assert_eq!(runs.runs().len(), 1);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
        assert_eq!(groups.value(0, 0).unwrap().as_ref(), &ScalarValue::Int64(1));
        drop((runs, groups, key, row, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
    use crate::{
        execution::MemoryPool,
        planner::{AggregateFunction, ScalarValue},
    };
    use arrow::datatypes::DataType;
    use std::io::{Seek, SeekFrom};

    fn fixture(pool: &MemoryPool) -> (Arc<GroupLayout>, GroupRows) {
        let layout = GroupLayout::bind(
            pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let mut groups = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        for value in [0, 1] {
            key.encode(&[ScalarValue::Int64(value)]).unwrap();
            groups
                .prepare_update(&key, &mut row, &[ScalarValue::Int64(1)])
                .unwrap()
                .commit();
        }
        (layout, groups)
    }

    #[test]
    fn published_run_survives_handle_drop_and_readers_have_independent_offsets() {
        let directory = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("run lifetime", 131072);
        let (layout, groups) = fixture(&pool);
        let mut writer = RunWriter::create_in(layout.clone(), directory.path()).unwrap();
        let path = writer.owner.path.clone();
        writer.append(&groups, 0).unwrap();
        writer.append(&groups, 1).unwrap();
        let run = writer.finish().unwrap();
        let mut first = run.reader().unwrap();
        let mut second = run.reader().unwrap();
        let mut a = FrameScratch::new(&layout, 256).unwrap();
        let mut b = FrameScratch::new(&layout, 256).unwrap();
        drop(run);
        assert!(path.exists());
        assert_eq!(
            first.next(&mut a).unwrap().unwrap(),
            second.next(&mut b).unwrap().unwrap()
        );
        assert_eq!(
            first.next(&mut a).unwrap().unwrap(),
            second.next(&mut b).unwrap().unwrap()
        );
        assert!(first.next(&mut a).unwrap().is_none());
        assert!(second.next(&mut b).unwrap().is_none());
        drop(first);
        assert!(path.exists());
        drop(second);
        assert!(!path.exists());
        drop((a, b, groups, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn incomplete_and_failed_writers_remove_only_their_owned_files() {
        let directory = tempfile::tempdir().unwrap();
        let sentinel = directory.path().join("keep");
        std::fs::write(&sentinel, b"foreign").unwrap();
        let pool = MemoryPool::new_named("run cleanup", 131072);
        let (layout, groups) = fixture(&pool);
        let baseline = pool.used();
        let mut writer = RunWriter::create_in(layout.clone(), directory.path()).unwrap();
        let path = writer.owner.path.clone();
        writer.append(&groups, 0).unwrap();
        drop(writer);
        assert!(!path.exists());
        assert_eq!(pool.used(), baseline);
        let mut writer = RunWriter::create_in(layout.clone(), directory.path()).unwrap();
        let path = writer.owner.path.clone();
        *writer.file.inner_mut() = File::open(&path).unwrap(); // Inject a real read-only descriptor write failure.
        writer.append(&groups, 0).unwrap();
        assert!(matches!(writer.finish(), Err(QueryError::Io(_))));
        assert!(!path.exists());
        assert_eq!(pool.used(), baseline);
        assert_eq!(std::fs::read(&sentinel).unwrap(), b"foreign");
        assert_eq!(groups.len(), 2);
        let pressure = layout.pool().allocate(layout.pool().available()).unwrap();
        assert!(RunWriter::create_in(layout.clone(), directory.path())
            .err()
            .unwrap()
            .is_memory_limit());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
        drop((pressure, groups, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn complete_suffix_loss_and_extra_frames_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("run lengths", 131072);
        let (layout, groups) = fixture(&pool);
        for extra in [false, true] {
            let mut writer = RunWriter::create_in(layout.clone(), directory.path()).unwrap();
            writer.append(&groups, 0).unwrap();
            let boundary = writer.bytes;
            writer.append(&groups, 1).unwrap();
            let run = writer.finish().unwrap();
            let mut existing = run.reader().unwrap();
            let mut edit = OpenOptions::new()
                .write(true)
                .open(&run.owner.path)
                .unwrap();
            if extra {
                edit.seek(SeekFrom::End(0)).unwrap();
                edit.write_all(b"extra").unwrap();
            } else {
                edit.set_len(boundary).unwrap();
            }
            assert!(run.reader().is_err());
            let mut scratch = FrameScratch::new(&layout, 256).unwrap();
            assert!(existing.next(&mut scratch).unwrap().is_some());
            assert!(existing.next(&mut scratch).is_err());
            assert!(existing.next(&mut scratch).is_err());
            drop((edit, existing, run, scratch));
            assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
        }
        let writer = RunWriter::create_in(layout.clone(), directory.path()).unwrap();
        let empty = writer.finish().unwrap();
        let mut reader = empty.reader().unwrap();
        let mut scratch = FrameScratch::new(&layout, 0).unwrap();
        assert!(reader.next(&mut scratch).unwrap().is_none());
        drop((reader, empty, scratch, groups, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn file_reader_retries_budget_denial_without_consuming_a_frame() {
        let directory = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("run pressure", 131072);
        let (layout, groups) = fixture(&pool);
        let mut writer = RunWriter::create_in(layout.clone(), directory.path()).unwrap();
        writer.append(&groups, 0).unwrap();
        let run = writer.finish().unwrap();
        let mut reader = run.reader().unwrap();
        let mut scratch = FrameScratch::new(&layout, 0).unwrap();
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(reader.next(&mut scratch).unwrap_err().is_memory_limit());
        assert_eq!(reader.remaining, 1);
        assert!(!reader.poisoned);
        assert!(run.reader().err().unwrap().is_memory_limit());
        drop(pressure);
        assert!(reader.next(&mut scratch).unwrap().is_some());
        assert!(reader.next(&mut scratch).unwrap().is_none());
        drop((reader, run, scratch, groups, layout));
        assert_eq!(pool.used(), 0);
    }
}
