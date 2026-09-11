//! Two-way exact-key repartition, independent of key hash collisions. Uses one
//! admitted frame and two prepared output writers, never decoded selected values.
use super::{
    group_rows::GroupLayout,
    spill_files::{RunDirectory, RunReader, RunWriter, SpillRun},
    spill_frames::FrameScratch,
};
use crate::{execution::reserved_vec::ReservedVec, QueryError, Result};
use std::sync::Arc;

/// Each byte has a leading presence bit, followed by its eight bits. The end
/// marker distinguishes a key from a longer key with that complete prefix.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct KeySplit {
    byte: usize,
    bit: u8,
}
impl KeySplit {
    pub(super) fn first_difference(a: &[u8], b: &[u8]) -> Option<Self> {
        for (byte, (a, b)) in a.iter().zip(b).enumerate() {
            if a != b {
                return Some(Self {
                    byte,
                    bit: (a ^ b).leading_zeros() as u8 + 1,
                });
            }
        }
        (a.len() != b.len()).then_some(Self {
            byte: a.len().min(b.len()),
            bit: 0,
        })
    }
    pub(super) fn side(self, key: &[u8]) -> usize {
        match key.get(self.byte) {
            None => 0,
            Some(_) if self.bit == 0 => 1,
            Some(byte) => usize::from((byte >> (8 - self.bit)) & 1),
        }
    }
}

pub(super) struct PreparedRepartition<'a> {
    source: &'a SpillRun,
    reader: RunReader,
    writers: [RunWriter; 2],
    scratch: FrameScratch,
    split: KeySplit,
    expected: Option<[u64; 2]>,
}

/// A full, checked scan proves that this exact source has two nonempty children.
/// The source borrow prevents replacing the run between planning and execution.
pub(super) struct SplitPlan<'a> {
    source: &'a SpillRun,
    split: KeySplit,
    counts: [u64; 2],
}
impl<'a> SplitPlan<'a> {
    pub(super) fn scan(
        source: &'a SpillRun,
        layout: &Arc<GroupLayout>,
        frame_capacity: usize,
    ) -> Result<Option<Self>> {
        Ok(
            scan_sources(std::slice::from_ref(source), layout, frame_capacity)?.map(
                |(split, counts)| Self {
                    source,
                    split,
                    counts,
                },
            ),
        )
    }
    pub(super) fn counts(&self) -> [u64; 2] {
        self.counts
    }
}

fn scan_sources(
    sources: &[SpillRun],
    layout: &Arc<GroupLayout>,
    frame_capacity: usize,
) -> Result<Option<(KeySplit, [u64; 2])>> {
    let Some(first) = sources.first() else {
        return Ok(None);
    };
    let mut total = 0u64;
    for source in sources {
        if source.layout_identity() != layout.identity() {
            return Err(QueryError::Execution("split scan layout mismatch".into()));
        }
        total = total
            .checked_add(source.row_count())
            .ok_or_else(|| QueryError::Execution("partition row count overflow".into()))?;
    }
    let mut reader = first.reader()?;
    let mut frame = FrameScratch::new(layout, frame_capacity)?;
    let mut representative = Some(ReservedVec::with_capacity(layout.pool(), 0)?);
    let mut chosen: Option<KeySplit> = None;
    let mut counts = [0u64; 2];
    let mut seen = 0u64;
    for (index, source) in sources.iter().enumerate() {
        if index != 0 {
            reader.reset(source)?;
        }
        while reader.next(&mut frame)?.is_some() {
            let key = frame.key_bytes()?;
            if let Some(split) = chosen {
                counts[split.side(key)] = counts[split.side(key)]
                    .checked_add(1)
                    .ok_or_else(|| QueryError::Execution("split count overflow".into()))?;
            } else if seen == 0 {
                let first = representative.as_mut().unwrap();
                first.reserve(key.len())?;
                first.extend_reserved(key.len(), key.iter().copied())?;
            } else if let Some(split) =
                KeySplit::first_difference(representative.as_ref().unwrap().as_slice(), key)
            {
                counts[split.side(representative.as_ref().unwrap().as_slice())] = seen;
                counts[split.side(key)] = 1;
                chosen = Some(split);
                drop(representative.take());
            }
            seen = seen
                .checked_add(1)
                .ok_or_else(|| QueryError::Execution("split count overflow".into()))?;
        }
    }
    if seen != total {
        return Err(QueryError::Execution(
            "partition split scan count mismatch".into(),
        ));
    }
    Ok(chosen.map(|split| (split, counts)))
}

pub(super) struct PartitionPlan<'a> {
    sources: &'a [SpillRun],
    split: KeySplit,
    counts: [u64; 2],
}
impl<'a> PartitionPlan<'a> {
    pub(super) fn scan(
        sources: &'a [SpillRun],
        layout: &Arc<GroupLayout>,
        frame_capacity: usize,
    ) -> Result<Option<Self>> {
        Ok(
            scan_sources(sources, layout, frame_capacity)?.map(|(split, counts)| Self {
                sources,
                split,
                counts,
            }),
        )
    }
    pub(super) fn prepare(
        self,
        layout: Arc<GroupLayout>,
        directory: &Arc<RunDirectory>,
        frame_capacity: usize,
    ) -> Result<PreparedPartition<'a>> {
        if self
            .sources
            .iter()
            .any(|source| source.layout_identity() != layout.identity())
        {
            return Err(QueryError::Execution(
                "partition output layout mismatch".into(),
            ));
        }
        Ok(PreparedPartition {
            reader: self.sources[0].reader()?,
            writers: [
                RunWriter::create(layout.clone(), directory)?,
                RunWriter::create(layout.clone(), directory)?,
            ],
            scratch: FrameScratch::new(&layout, frame_capacity)?,
            plan: self,
        })
    }
}

pub(super) struct PreparedPartition<'a> {
    reader: RunReader,
    writers: [RunWriter; 2],
    scratch: FrameScratch,
    plan: PartitionPlan<'a>,
}
impl PreparedPartition<'_> {
    pub(super) fn finish(mut self) -> Result<[SpillRun; 2]> {
        for (index, source) in self.plan.sources.iter().enumerate() {
            if index != 0 {
                self.reader.reset(source)?;
            }
            while self.reader.next(&mut self.scratch)?.is_some() {
                let side = self.plan.split.side(self.scratch.key_bytes()?);
                self.writers[side].append_frame(&self.scratch)?;
            }
        }
        let [left, right] = self.writers;
        let children = [left.finish()?, right.finish()?];
        let actual = [children[0].row_count(), children[1].row_count()];
        if actual != self.plan.counts || actual.contains(&0) {
            return Err(QueryError::Execution(
                "partition children failed global progress check".into(),
            ));
        }
        Ok(children)
    }
}

impl<'a> PreparedRepartition<'a> {
    pub(super) fn from_plan(
        plan: SplitPlan<'a>,
        layout: Arc<GroupLayout>,
        directory: &Arc<RunDirectory>,
        frame_capacity: usize,
    ) -> Result<Self> {
        let mut prepared = Self::new(plan.source, layout, directory, frame_capacity, plan.split)?;
        prepared.expected = Some(plan.counts);
        Ok(prepared)
    }
    fn new(
        source: &'a SpillRun,
        layout: Arc<GroupLayout>,
        directory: &Arc<RunDirectory>,
        frame_capacity: usize,
        split: KeySplit,
    ) -> Result<Self> {
        if source.layout_identity() != layout.identity() {
            return Err(QueryError::Execution(
                "repartition source layout mismatch".into(),
            ));
        }
        Ok(Self {
            source,
            reader: source.reader()?,
            writers: [
                RunWriter::create(layout.clone(), directory)?,
                RunWriter::create(layout.clone(), directory)?,
            ],
            scratch: FrameScratch::new(&layout, frame_capacity)?,
            split,
            expected: None,
        })
    }
    pub(super) fn finish(mut self) -> Result<[SpillRun; 2]> {
        let mut count = 0u64;
        while self.reader.next(&mut self.scratch)?.is_some() {
            let side = self.split.side(self.scratch.key_bytes()?);
            self.writers[side].append_frame(&self.scratch)?;
            count = count
                .checked_add(1)
                .ok_or_else(|| QueryError::Execution("repartition row count overflow".into()))?;
        }
        if count != self.source.row_count() {
            return Err(QueryError::Execution("repartition lost source rows".into()));
        }
        let [left, right] = self.writers;
        let children = [left.finish()?, right.finish()?];
        if let Some(expected) = self.expected {
            let actual = [children[0].row_count(), children[1].row_count()];
            if actual != expected || actual.contains(&0) {
                return Err(QueryError::Execution(
                    "repartition did not preserve proven split progress".into(),
                ));
            }
        }
        Ok(children)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{group_rows::GroupRows, run_merge::RunMerge};
    use super::*;
    use crate::{
        execution::MemoryPool,
        planner::{AggregateFunction, ScalarValue},
    };
    use arrow::datatypes::DataType;
    use std::io::{Read, Seek, SeekFrom, Write};

    #[test]
    fn global_split_finds_keys_between_individually_unsplittable_runs() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("global split", 262144);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
        let mut groups = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        let empty = RunWriter::create(layout.clone(), &directory)
            .unwrap()
            .finish()
            .unwrap();
        let mut make_run = |k, repeats| {
            groups.clear();
            key.encode(&[ScalarValue::Int64(k)]).unwrap();
            groups
                .prepare_update(&key, &mut row, &[ScalarValue::Int64(1)])
                .unwrap()
                .commit();
            let mut writer = RunWriter::create(layout.clone(), &directory).unwrap();
            for _ in 0..repeats {
                writer.append(&groups, 0).unwrap();
            }
            writer.finish().unwrap()
        };
        let sources = [empty, make_run(0, 3), make_run(1, 5)];
        let dir_path = std::fs::read_dir(root.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let last_source_path = std::fs::read_dir(&dir_path)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .max_by_key(|path| std::fs::metadata(path).unwrap().len())
            .unwrap();
        for source in &sources {
            assert!(SplitPlan::scan(source, &layout, 256).unwrap().is_none());
        }
        let baseline = pool.used();
        let plan = PartitionPlan::scan(&sources, &layout, 256)
            .unwrap()
            .unwrap();
        assert_eq!(plan.counts, [3, 5]);
        assert_eq!(pool.used(), baseline);
        let prepared = plan.prepare(layout.clone(), &directory, 256).unwrap();
        let pressure = pool.allocate(pool.available()).unwrap();
        let children = prepared.finish().unwrap();
        drop(pressure);
        assert_eq!([children[0].row_count(), children[1].row_count()], [3, 5]);
        groups.clear();
        for child in &children {
            let mut merge = RunMerge::new(child, layout.clone(), 256).unwrap();
            while merge.step(&mut groups).unwrap() {}
        }
        assert_eq!(groups.len(), 2);
        for (k, count) in [(0, 3), (1, 5)] {
            key.encode(&[ScalarValue::Int64(k)]).unwrap();
            let index = groups.lookup(&key).unwrap().unwrap();
            assert_eq!(
                groups.value(index, 0).unwrap().as_ref(),
                &ScalarValue::Int64(count)
            );
        }
        let plan = PartitionPlan::scan(&sources, &layout, 256)
            .unwrap()
            .unwrap();
        let existing = std::fs::read_dir(&dir_path).unwrap().count();
        let prepared = plan.prepare(layout.clone(), &directory, 256).unwrap();
        let mut edit = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&last_source_path)
            .unwrap();
        edit.seek(SeekFrom::End(-1)).unwrap();
        let mut byte = [0];
        edit.read_exact(&mut byte).unwrap();
        edit.seek(SeekFrom::End(-1)).unwrap();
        edit.write_all(&[byte[0] ^ 1]).unwrap();
        drop(edit);
        assert!(prepared.finish().is_err());
        assert_eq!(std::fs::read_dir(&dir_path).unwrap().count(), existing);
        assert!(last_source_path.exists());
        drop((sources, children, groups, key, row, directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn checked_split_plan_proves_two_nonempty_children_and_releases_scan_memory() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("split progress", 262144);
        let (layout, directory, source, _) = fixture(&pool, root.path());
        let dir_path = std::fs::read_dir(root.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let source_path = std::fs::read_dir(dir_path)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let before = pool.used();
        let plan = SplitPlan::scan(&source, &layout, 8192).unwrap().unwrap();
        assert_eq!(plan.counts(), [4, 4]);
        assert_eq!(pool.used(), before);
        let children = PreparedRepartition::from_plan(plan, layout.clone(), &directory, 8192)
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!([children[0].row_count(), children[1].row_count()], [4, 4]);
        // A split discovered early must not bypass verification of a later frame.
        let mut edit = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(source_path)
            .unwrap();
        edit.seek(SeekFrom::End(-1)).unwrap();
        let mut byte = [0];
        edit.read_exact(&mut byte).unwrap();
        edit.seek(SeekFrom::End(-1)).unwrap();
        edit.write_all(&[byte[0] ^ 1]).unwrap();
        let retained = pool.used();
        assert!(SplitPlan::scan(&source, &layout, 8192).is_err());
        assert_eq!(pool.used(), retained);
        drop(edit);
        drop((children, source, directory, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn equal_key_runs_and_empty_runs_decline_splitting_but_check_all_frames() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("unsplittable", 131072);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
        let mut source = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        key.encode(&[ScalarValue::Null]).unwrap();
        source
            .prepare_update(&key, &mut row, &[ScalarValue::Int64(1)])
            .unwrap()
            .commit();
        let empty = RunWriter::create(layout.clone(), &directory)
            .unwrap()
            .finish()
            .unwrap();
        assert!(SplitPlan::scan(&empty, &layout, 256).unwrap().is_none());
        drop(empty);
        let mut writer = RunWriter::create(layout.clone(), &directory).unwrap();
        for _ in 0..20 {
            writer.append(&source, 0).unwrap();
        }
        let run = writer.finish().unwrap();
        assert!(SplitPlan::scan(&run, &layout, 256).unwrap().is_none());
        let dir = std::fs::read_dir(root.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let path = std::fs::read_dir(dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mut edit = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        edit.seek(SeekFrom::End(-1)).unwrap();
        let mut byte = [0];
        edit.read_exact(&mut byte).unwrap();
        edit.seek(SeekFrom::End(-1)).unwrap();
        edit.write_all(&[byte[0] ^ 1]).unwrap();
        assert!(SplitPlan::scan(&run, &layout, 256).is_err());
        drop(edit);
        drop((run, source, key, row, directory, layout));
        assert_eq!(pool.used(), 0);
    }

    fn fixture(
        pool: &MemoryPool,
        root: &std::path::Path,
    ) -> (Arc<GroupLayout>, Arc<RunDirectory>, SpillRun, KeySplit) {
        let layout = GroupLayout::bind(
            pool,
            &[DataType::Int64],
            &[
                (AggregateFunction::Count, DataType::Int64, false),
                (AggregateFunction::Avg, DataType::Float64, false),
                (AggregateFunction::Max, DataType::Utf8, false),
            ],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), root).unwrap();
        let mut groups = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        for k in 0..4 {
            key.encode(&[ScalarValue::Int64(k)]).unwrap();
            groups
                .prepare_update(
                    &key,
                    &mut row,
                    &[
                        ScalarValue::Int64(1),
                        ScalarValue::Float64((k as f64 + 10.0).into()),
                        ScalarValue::Utf8("z".repeat(4096)),
                    ],
                )
                .unwrap()
                .commit();
        }
        let split = KeySplit::first_difference(
            groups.key(0).unwrap().bytes(),
            groups.key(1).unwrap().bytes(),
        )
        .unwrap();
        let mut writer = RunWriter::create(layout.clone(), &directory).unwrap();
        for _ in 0..2 {
            for row in 0..4 {
                writer.append(&groups, row).unwrap();
            }
        }
        (layout, directory, writer.finish().unwrap(), split)
    }

    #[test]
    fn repartition_preserves_every_partial_and_exact_group_routing_with_a_full_pool() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("repartition", 262144);
        let (layout, directory, source, split) = fixture(&pool, root.path());
        let prepared =
            PreparedRepartition::new(&source, layout.clone(), &directory, 8192, split).unwrap();
        let pressure = pool.allocate(pool.available()).unwrap();
        let children = prepared.finish().unwrap();
        drop(pressure);
        assert_eq!(source.row_count(), 8);
        assert_eq!(children[0].row_count(), 4);
        assert_eq!(children[1].row_count(), 4);
        let mut scratch = FrameScratch::new(&layout, 8192).unwrap();
        let mut reader = source.reader().unwrap();
        let mut before = Vec::new();
        while let Some(bytes) = reader.next(&mut scratch).unwrap() {
            before.push(bytes.to_vec());
        }
        drop(reader);
        let mut after = Vec::new();
        let mut target = GroupRows::new(layout.clone()).unwrap();
        for (side, child) in children.iter().enumerate() {
            let mut reader = child.reader().unwrap();
            while reader.next(&mut scratch).unwrap().is_some() {
                assert_eq!(split.side(scratch.key_bytes().unwrap()), side);
                after.push(scratch.payload().unwrap().to_vec());
            }
            let mut merge = RunMerge::new(child, layout.clone(), 8192).unwrap();
            while merge.step(&mut target).unwrap() {}
        }
        before.sort();
        after.sort();
        assert_eq!(before, after);
        let mut key = layout.key_workspace().unwrap();
        assert_eq!(target.len(), 4);
        for k in 0..4 {
            key.encode(&[ScalarValue::Int64(k)]).unwrap();
            let row = target.lookup(&key).unwrap().unwrap();
            assert_eq!(
                target.value(row, 0).unwrap().as_ref(),
                &ScalarValue::Int64(2)
            );
            assert_eq!(
                target.value(row, 1).unwrap().as_ref(),
                &ScalarValue::Float64((k as f64 + 10.0).into())
            );
        }
        drop((key, target, scratch, children, source, directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn failed_repartition_removes_children_and_preserves_source_for_retry() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("partition rollback", 262144);
        let (layout, directory, source, split) = fixture(&pool, root.path());
        let path = std::fs::read_dir(root.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let source_path = std::fs::read_dir(&path)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let prepared =
            PreparedRepartition::new(&source, layout.clone(), &directory, 0, split).unwrap();
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(prepared.finish().err().unwrap().is_memory_limit());
        drop(pressure);
        assert_eq!(std::fs::read_dir(&path).unwrap().count(), 1);
        let prepared =
            PreparedRepartition::new(&source, layout.clone(), &directory, 8192, split).unwrap();
        let mut edit = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&source_path)
            .unwrap();
        edit.seek(SeekFrom::End(-1)).unwrap();
        let mut byte = [0];
        edit.read_exact(&mut byte).unwrap();
        edit.seek(SeekFrom::End(-1)).unwrap();
        edit.write_all(&[byte[0] ^ 1]).unwrap();
        assert!(prepared.finish().is_err());
        assert_eq!(std::fs::read_dir(&path).unwrap().count(), 1);
        assert!(source_path.exists());
        edit.seek(SeekFrom::End(-1)).unwrap();
        edit.write_all(&byte).unwrap();
        drop(edit);
        let children = PreparedRepartition::new(&source, layout.clone(), &directory, 8192, split)
            .unwrap()
            .finish()
            .unwrap();
        assert_eq!(children.iter().map(SpillRun::row_count).sum::<u64>(), 8);
        drop((children, source, directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn split_is_prefix_safe_and_does_not_depend_on_hash_uniqueness() {
        for (a, b) in [
            (b"".as_slice(), b"\0".as_slice()),
            (b"a".as_slice(), b"ab".as_slice()),
            (b"ab".as_slice(), b"ac".as_slice()),
            (&[0, 128], &[0, 0]),
        ] {
            let split = KeySplit::first_difference(a, b).unwrap();
            assert_ne!(split.side(a), split.side(b));
            assert_eq!(
                KeySplit::first_difference(a, b),
                KeySplit::first_difference(b, a)
            );
            assert!(KeySplit::first_difference(a, a).is_none());
        }
    }
}
