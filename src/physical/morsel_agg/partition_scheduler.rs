//! Serial depth-first partition scheduling. Smaller children are processed first
//! to bound pending siblings; every task/file remains query-owned. Output is a
//! borrowed complete partition, whose consumer must admit any retained copies.
use super::{
    group_rows::{GroupLayout, GroupRows},
    repartition::PartitionPlan,
    run_merge::{MergeStep, RunMerge},
    spill_files::{RunCollection, RunDirectory, SpillRun},
};
use crate::{execution::reserved_vec::ReservedVec, QueryError, Result};
use std::sync::Arc;

enum Task {
    Root(RunCollection),
    Child(SpillRun),
}
impl Task {
    fn runs(&self) -> &[SpillRun] {
        match self {
            Self::Root(runs) => runs.runs(),
            Self::Child(run) => std::slice::from_ref(run),
        }
    }
}
pub(super) struct PartitionScheduler {
    tasks: ReservedVec<Task>,
    layout: Arc<GroupLayout>,
    directory: Arc<RunDirectory>,
    frame_capacity: usize,
    group_limit: usize,
    splits: usize,
}
impl PartitionScheduler {
    pub(super) fn new(
        layout: Arc<GroupLayout>,
        directory: Arc<RunDirectory>,
        runs: RunCollection,
        frame_capacity: usize,
        group_limit: usize,
    ) -> Result<Self> {
        if group_limit == 0 {
            return Err(QueryError::Execution(
                "partition group limit must be positive".into(),
            ));
        }
        let mut tasks = ReservedVec::with_capacity(layout.pool(), 1)?;
        tasks.extend_reserved(1, std::iter::once(Task::Root(runs)))?;
        Ok(Self {
            tasks,
            layout,
            directory,
            frame_capacity,
            group_limit,
            splits: 0,
        })
    }
    /// The group limit is a working-set control, never a proof of byte usage.
    /// Query reservations enforce memory independently of this limit.
    fn merge(&self, runs: &[SpillRun]) -> Result<Option<GroupRows>> {
        let mut target = GroupRows::new(self.layout.clone())?;
        for run in runs {
            let mut cursor = RunMerge::new(run, self.layout.clone(), self.frame_capacity)?;
            loop {
                match cursor.step_with_group_limit(&mut target, self.group_limit) {
                    Ok(MergeStep::Merged) => (),
                    Ok(MergeStep::End) => break,
                    Ok(MergeStep::GroupLimit) => return Ok(None),
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(Some(target))
    }
    pub(super) fn visit(
        mut self,
        mut output: impl FnMut(&GroupRows) -> Result<()>,
    ) -> Result<usize> {
        while let Some(task) = self.tasks.pop() {
            let (merged, pressure) = match self.merge(task.runs()) {
                Err(error) if error.is_memory_limit() => (None, Some(error)),
                result => (result?, None),
            };
            if let Some(groups) = merged {
                if groups.len() != 0 {
                    output(&groups)?;
                }
                continue;
            }
            // Merge temporaries have dropped before admitting split work. Keep
            // original runs alive until both children and task slots are owned.
            self.tasks.reserve(2)?;
            let plan = PartitionPlan::scan(task.runs(), &self.layout, self.frame_capacity)?
                .ok_or_else(|| {
                    // Split feasibility does not replace the allocator's typed
                    // cause. Preserve the exact refusal from the failed merge
                    // after its temporary state has been released.
                    pressure.unwrap_or_else(|| {
                        QueryError::Execution(
                            "aggregate partition cannot fit its single-key working set".into(),
                        )
                    })
                })?;
            let [a, b] = plan
                .prepare(self.layout.clone(), &self.directory, self.frame_capacity)?
                .finish()?;
            let (large, small) = if a.row_count() >= b.row_count() {
                (a, b)
            } else {
                (b, a)
            };
            self.tasks
                .extend_reserved(2, [Task::Child(large), Task::Child(small)])?;
            self.splits = self
                .splits
                .checked_add(1)
                .ok_or_else(|| QueryError::Execution("partition split count overflow".into()))?;
        }
        Ok(self.splits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::MemoryPool,
        planner::{AggregateFunction, DecimalValue, ScalarValue},
    };
    use arrow::datatypes::DataType;
    use std::collections::BTreeMap;

    #[test]
    fn actual_query_budget_pressure_splits_without_an_artificial_group_limit() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("scheduler 256KiB", 256 * 1024);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
        let mut runs = RunCollection::new(layout.clone()).unwrap();
        let mut source = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        // Compact fixed cells need more distinct groups to exceed the same budget.
        for run in 0..32 {
            let flush = runs.prepare_flush(&directory).unwrap();
            for k in run * 128..(run + 1) * 128 {
                key.encode(&[ScalarValue::Int64(k)]).unwrap();
                source
                    .prepare_update(&key, &mut row, &[ScalarValue::Int64(1)])
                    .unwrap()
                    .commit();
            }
            flush.flush(&mut source).unwrap();
        }
        drop((source, key, row));
        let scheduler =
            PartitionScheduler::new(layout.clone(), directory.clone(), runs, 256, usize::MAX)
                .unwrap();
        let mut seen = std::collections::BTreeSet::new();
        let splits = scheduler
            .visit(|groups| {
                for row in 0..groups.len() {
                    let key = groups.key(row)?;
                    let k = i64::from_le_bytes(key.bytes()[1..9].try_into().unwrap());
                    assert!((0..4096).contains(&k));
                    assert!(seen.insert(k));
                    assert_eq!(groups.value(row, 0)?.as_ref(), &ScalarValue::Int64(1));
                }
                Ok(())
            })
            .unwrap();
        assert!(splits > 0, "must exercise actual byte-admission pressure");
        assert_eq!(seen.len(), 4096);
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn scheduler_splits_and_emits_every_exact_group_once_across_runs() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("partition scheduler", 262144);
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
        let mut source = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        for run in 0..3 {
            let flush = runs.prepare_flush(&directory).unwrap();
            for k in 0..17 {
                key.encode(&[if k == 16 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Int64(k)
                }])
                .unwrap();
                let exact = (1i128 << 80) + i128::from(k) * 17;
                for _ in 0..=run {
                    let values = if k % 7 == 0 {
                        [const { ScalarValue::Null }; 3]
                    } else {
                        [
                            ScalarValue::Int64(1),
                            ScalarValue::Float64((10.0 * (run + 1) as f64).into()),
                            ScalarValue::Decimal128(DecimalValue::new(
                                exact * (run + 1) as i128,
                                2,
                            )),
                        ]
                    };
                    source
                        .prepare_update(&key, &mut row, &values)
                        .unwrap()
                        .commit();
                }
            }
            flush.flush(&mut source).unwrap();
        }
        drop((source, key, row));
        let scheduler =
            PartitionScheduler::new(layout.clone(), directory.clone(), runs, 1024, 2).unwrap();
        let mut seen = BTreeMap::new();
        let splits = scheduler
            .visit(|groups| {
                assert!(groups.len() <= 2);
                for row in 0..groups.len() {
                    let key = groups.key(row)?;
                    let bytes = key.bytes();
                    let k = if bytes == [0] {
                        16
                    } else {
                        i64::from_le_bytes(bytes[1..9].try_into().unwrap())
                    };
                    assert!(seen.insert(k, ()).is_none(), "group emitted twice");
                    let null = k % 7 == 0;
                    assert_eq!(
                        groups.value(row, 0)?.as_ref(),
                        &ScalarValue::Int64(if null { 0 } else { 6 })
                    );
                    assert_eq!(
                        groups.value(row, 1)?.as_ref(),
                        &if null {
                            ScalarValue::Null
                        } else {
                            ScalarValue::Float64((140.0 / 6.0).into())
                        }
                    );
                    assert_eq!(
                        groups.value(row, 2)?.as_ref(),
                        &if null {
                            ScalarValue::Null
                        } else {
                            ScalarValue::Decimal128(DecimalValue::new(
                                14 * ((1i128 << 80) + i128::from(k) * 17),
                                2,
                            ))
                        }
                    );
                }
                Ok(())
            })
            .unwrap();
        assert!(splits >= 8);
        assert_eq!(seen.len(), 17);
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn one_group_keeps_merging_duplicates_at_the_group_limit_and_output_error_cleans_up() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("scheduler leaf", 131072);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
        for fail_output in [false, true] {
            let mut runs = RunCollection::new(layout.clone()).unwrap();
            let mut source = GroupRows::new(layout.clone()).unwrap();
            let mut key = layout.key_workspace().unwrap();
            let mut row = layout.row_workspace().unwrap();
            key.encode(&[ScalarValue::Int64(0)]).unwrap();
            for _ in 0..3 {
                let flush = runs.prepare_flush(&directory).unwrap();
                source
                    .prepare_update(&key, &mut row, &[ScalarValue::Int64(1)])
                    .unwrap()
                    .commit();
                flush.flush(&mut source).unwrap();
            }
            drop((source, key, row));
            let baseline = pool.used();
            let scheduler =
                PartitionScheduler::new(layout.clone(), directory.clone(), runs, 256, 1).unwrap();
            let result = scheduler.visit(|groups| {
                assert_eq!(groups.len(), 1);
                assert_eq!(groups.value(0, 0)?.as_ref(), &ScalarValue::Int64(3));
                if fail_output {
                    Err(QueryError::Execution("injected output failure".into()))
                } else {
                    Ok(())
                }
            });
            if fail_output {
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("injected output failure"));
            } else {
                assert_eq!(result.unwrap(), 0);
            }
            assert!(pool.used() < baseline);
        }
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn unsplittable_partition_preserves_the_typed_memory_refusal() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("single-key scheduler", 131072);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
        let mut runs = RunCollection::new(layout.clone()).unwrap();
        let mut source = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        key.encode(&[ScalarValue::Int64(7)]).unwrap();
        let flush = runs.prepare_flush(&directory).unwrap();
        source
            .prepare_update(&key, &mut row, &[ScalarValue::Int64(1)])
            .unwrap()
            .commit();
        flush.flush(&mut source).unwrap();
        drop((source, key, row));
        let scheduler =
            PartitionScheduler::new(layout.clone(), directory.clone(), runs, 256, 1).unwrap();
        let pressure = pool.allocate(pool.available() - 8192).unwrap();
        assert!(
            PartitionPlan::scan(scheduler.tasks.as_slice()[0].runs(), &layout, 256)
                .unwrap()
                .is_none(),
            "split planning must fit and prove there is only one exact key"
        );
        let error = scheduler
            .visit(|_| panic!("working state cannot fit"))
            .unwrap_err();
        assert!(
            error.is_memory_limit(),
            "the original admission cause was lost: {error}"
        );
        assert!(matches!(
            error.root(),
            QueryError::MemoryLimit { limit: 131072, .. }
        ));
        drop((pressure, directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}
