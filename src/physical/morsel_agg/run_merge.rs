//! One-frame/one-partial-row merge cursor. The partition scheduler can flush
//! the destination on admission pressure, then retry without reading a frame or
//! applying a partial twice. This component does not schedule repartitioning.
use super::{
    group_rows::{GroupLayout, GroupRows},
    key_rows::KeyWorkspace,
    spill_files::{RunReader, SpillRun},
    spill_frames::FrameScratch,
    state_rows::RowWorkspace,
};
use crate::{QueryError, Result};
use std::sync::Arc;

pub(super) struct RunMerge {
    reader: RunReader,
    frame: FrameScratch,
    key: KeyWorkspace,
    workspace: RowWorkspace,
    stage: GroupRows,
    pending: bool,
    ended: bool,
    poisoned: bool,
}
#[derive(Debug, PartialEq, Eq)]
pub(super) enum MergeStep {
    Merged,
    End,
    GroupLimit,
}
impl RunMerge {
    pub(super) fn new(
        run: &SpillRun,
        layout: Arc<GroupLayout>,
        frame_capacity: usize,
    ) -> Result<Self> {
        Ok(Self {
            reader: run.reader()?,
            frame: FrameScratch::new(&layout, frame_capacity)?,
            key: layout.key_workspace()?,
            workspace: layout.row_workspace()?,
            stage: GroupRows::new(layout)?,
            pending: false,
            ended: false,
            poisoned: false,
        })
    }
    /// True means exactly one partial row committed. False means verified EOF.
    /// Only typed memory denial is retryable; all other errors poison the cursor.
    pub(super) fn step(&mut self, target: &mut GroupRows) -> Result<bool> {
        match self.step_with_group_limit(target, usize::MAX)? {
            MergeStep::Merged => Ok(true),
            MergeStep::End => Ok(false),
            MergeStep::GroupLimit => Err(QueryError::Execution(
                "aggregate group count exhausted".into(),
            )),
        }
    }
    pub(super) fn step_with_group_limit(
        &mut self,
        target: &mut GroupRows,
        limit: usize,
    ) -> Result<MergeStep> {
        if self.poisoned {
            return Err(QueryError::Execution(
                "aggregate run merge cursor previously failed".into(),
            ));
        }
        let result = self.step_inner(target, limit);
        if result.as_ref().is_err_and(|error| !error.is_memory_limit()) {
            self.poisoned = true;
        }
        result
    }
    fn stage_next(&mut self) -> Result<bool> {
        if self.ended {
            return Ok(false);
        }
        if self.stage.len() == 0 {
            if !self.pending {
                if self.reader.next(&mut self.frame)?.is_none() {
                    self.ended = true;
                    return Ok(false);
                }
                self.pending = true;
            }
            self.stage
                .prepare_restore(&mut self.key, &mut self.workspace, self.frame.payload()?)?
                .commit();
            self.pending = false;
            self.frame.invalidate();
        }
        Ok(true)
    }
    fn step_inner(&mut self, target: &mut GroupRows, limit: usize) -> Result<MergeStep> {
        if target.layout_identity() != self.stage.layout_identity() {
            return Err(QueryError::Execution(
                "aggregate run merge destination layout mismatch".into(),
            ));
        }
        if !self.stage_next()? {
            return Ok(MergeStep::End);
        }
        if target.len() >= limit && target.lookup(&self.key)?.is_none() {
            return Ok(MergeStep::GroupLimit);
        }
        target
            .prepare_merge_update(&self.key, &mut self.workspace, &self.stage, 0)?
            .commit();
        self.stage.clear();
        Ok(MergeStep::Merged)
    }
}

#[cfg(test)]
mod tests {
    use super::super::spill_files::RunWriter;
    use super::*;
    use crate::{
        execution::MemoryPool,
        planner::{AggregateFunction, DecimalValue, ScalarValue},
    };
    use arrow::datatypes::DataType;

    #[test]
    fn arithmetic_failure_is_terminal_and_does_not_modify_the_destination() {
        let directory = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("merge overflow", 131072);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let mut source = GroupRows::new(layout.clone()).unwrap();
        let mut target = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        key.encode(&[ScalarValue::Int64(0)]).unwrap();
        source
            .prepare_update(&key, &mut row, &[ScalarValue::Int64(1)])
            .unwrap()
            .commit();
        let mut writer = RunWriter::create_in(layout.clone(), directory.path()).unwrap();
        writer.append(&source, 0).unwrap();
        let run = writer.finish().unwrap();
        let mut wire = Vec::new();
        source.write_row(0, &mut wire).unwrap();
        let end = wire.len();
        wire[end - 8..].copy_from_slice(&i64::MAX.to_le_bytes());
        target
            .prepare_restore(&mut key, &mut row, &wire)
            .unwrap()
            .commit();
        let mut merge = RunMerge::new(&run, layout.clone(), 256).unwrap();
        let error = merge.step(&mut target).unwrap_err();
        assert!(!error.is_memory_limit());
        assert!(error.to_string().contains("overflow"));
        assert!(merge.poisoned);
        assert_eq!(
            target.value(0, 0).unwrap().as_ref(),
            &ScalarValue::Int64(i64::MAX)
        );
        assert!(merge.step(&mut target).is_err());
        assert_eq!(target.len(), 1);
        drop((merge, run, target, source, key, row, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn decode_denial_retains_the_verified_frame_and_merge_is_exactly_once() {
        let directory = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("run merge", 262144);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[
                (AggregateFunction::Count, DataType::Int64, false),
                (AggregateFunction::Avg, DataType::Float64, false),
                (AggregateFunction::Sum, DataType::Decimal128(38, 2), false),
                (AggregateFunction::Max, DataType::Utf8, false),
            ],
        )
        .unwrap()
        .unwrap();
        let mut source = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        key.encode(&[ScalarValue::Int64(7)]).unwrap();
        let exact = (1i128 << 100) + 17;
        for v in [10.0, 30.0, 30.0, 30.0] {
            source
                .prepare_update(
                    &key,
                    &mut row,
                    &[
                        ScalarValue::Int64(1),
                        ScalarValue::Float64(v.into()),
                        ScalarValue::Decimal128(DecimalValue::new(exact, 2)),
                        ScalarValue::Utf8("z".repeat(4096)),
                    ],
                )
                .unwrap()
                .commit();
        }
        let mut writer = RunWriter::create_in(layout.clone(), directory.path()).unwrap();
        writer.append(&source, 0).unwrap();
        writer.append(&source, 0).unwrap();
        let run = writer.finish().unwrap();
        drop((source, key, row));
        let mut merge = RunMerge::new(&run, layout.clone(), 8192).unwrap();
        let mut target = GroupRows::new(layout.clone()).unwrap();
        let pressure = pool.allocate(pool.available()).unwrap();
        for _ in 0..3 {
            assert!(merge.step(&mut target).unwrap_err().is_memory_limit());
            assert!(merge.pending);
            assert_eq!(merge.stage.len(), 0);
            assert_eq!(target.len(), 0);
        }
        drop(pressure);
        assert!(merge.step(&mut target).unwrap());
        assert_eq!(target.value(0, 0).unwrap().as_ref(), &ScalarValue::Int64(4));
        assert!(merge.step(&mut target).unwrap());
        assert_eq!(target.value(0, 0).unwrap().as_ref(), &ScalarValue::Int64(8));
        assert_eq!(
            target.value(0, 1).unwrap().as_ref(),
            &ScalarValue::Float64(25.0.into())
        );
        assert_eq!(
            target.value(0, 2).unwrap().as_ref(),
            &ScalarValue::Decimal128(DecimalValue::new(8 * exact, 2))
        );
        assert!(!merge.step(&mut target).unwrap());
        assert!(!merge.step(&mut target).unwrap());
        drop((merge, run, target, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }

    #[test]
    fn destination_denial_retains_staged_state_and_source_file_lifetime() {
        let directory = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("staged retry", 131072);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let mut source = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        key.encode(&[ScalarValue::Int64(0)]).unwrap();
        source
            .prepare_update(&key, &mut row, &[ScalarValue::Int64(1)])
            .unwrap()
            .commit();
        let mut writer = RunWriter::create_in(layout.clone(), directory.path()).unwrap();
        writer.append(&source, 0).unwrap();
        let run = writer.finish().unwrap();
        let mut merge = RunMerge::new(&run, layout.clone(), 256).unwrap();
        let mut target = GroupRows::new(layout.clone()).unwrap();
        assert!(merge.stage_next().unwrap());
        assert_eq!(merge.stage.len(), 1);
        drop(run);
        let pressure = pool.allocate(pool.available()).unwrap();
        for _ in 0..3 {
            assert!(merge.step(&mut target).unwrap_err().is_memory_limit());
            assert_eq!(merge.stage.len(), 1);
            assert!(!merge.pending);
            assert_eq!(target.len(), 0);
        }
        drop(pressure);
        assert!(merge.step(&mut target).unwrap());
        assert!(!merge.step(&mut target).unwrap());
        assert_eq!(target.value(0, 0).unwrap().as_ref(), &ScalarValue::Int64(1));
        drop((merge, target, source, key, row, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}
