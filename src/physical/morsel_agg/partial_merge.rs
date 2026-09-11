//! Cross-owner merge of partial states, before any final values or HAVING.
//! One pre-admitted writer can spool all remaining rows without allocating a
//! replacement while source owners fill the query pool. Fitting merges stay in
//! memory. After spooling begins, final merge waits until local owners drop.
use super::{
    group_rows::{GroupLayout, GroupRows},
    ingestion_controller::IngestionStats,
    key_rows::KeyWorkspace,
    partition_scheduler::PartitionScheduler,
    spill_files::{RunCollection, RunDirectory, RunWriter},
    state_rows::RowWorkspace,
};
use crate::{QueryError, Result};
use std::sync::Arc;

struct Resident {
    groups: GroupRows,
    key: KeyWorkspace,
    row: RowWorkspace,
}

pub(super) struct PartialMerge {
    resident: Option<Resident>,
    writer: Option<RunWriter>,
    runs: RunCollection,
    layout: Arc<GroupLayout>,
    directory: Arc<RunDirectory>,
    frame_capacity: usize,
    group_limit: usize,
    spilled_rows: u64,
    poisoned: bool,
}

fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("aggregate partial merge: {message}"))
}

impl PartialMerge {
    /// Construct before filling local worker state, from the same query pool.
    pub(super) fn new(
        layout: Arc<GroupLayout>,
        directory: Arc<RunDirectory>,
        frame_capacity: usize,
        group_limit: usize,
    ) -> Result<Self> {
        if group_limit == 0 {
            return Err(invalid("group limit must be positive"));
        }
        let mut runs = RunCollection::new(layout.clone())?;
        let writer = runs.prepare_flush(&directory)?.into_writer();
        let resident = Resident {
            groups: GroupRows::new(layout.clone())?,
            key: layout.key_workspace()?,
            row: layout.row_workspace()?,
        };
        Ok(Self {
            resident: Some(resident),
            writer: Some(writer),
            runs,
            layout,
            directory,
            frame_capacity,
            group_limit,
            spilled_rows: 0,
            poisoned: false,
        })
    }

    /// Reserve all bounded worker run handles plus this consumer's own writer
    /// before opening input. Adoption never grows this ledger while workers live.
    pub(super) fn reserve_input_runs(&mut self, maximum: usize) -> Result<()> {
        self.runs.reserve_slots(
            maximum
                .checked_add(1)
                .ok_or_else(|| invalid("run ledger overflow"))?,
        )
    }
    pub(super) fn adopt_runs(&mut self, runs: RunCollection) -> Result<()> {
        if self.poisoned {
            return Err(invalid("previous merge failed"));
        }
        for run in runs.into_runs() {
            if let Err(error) = self.runs.adopt_run(run) {
                self.poisoned = true;
                return Err(error);
            }
        }
        Ok(())
    }

    fn append(&mut self, source: &GroupRows, row: usize) -> Result<()> {
        let count = self
            .spilled_rows
            .checked_add(1)
            .ok_or_else(|| invalid("row count overflow"))?;
        self.writer
            .as_mut()
            .ok_or_else(|| invalid("missing prepared writer"))?
            .append(source, row)?;
        self.spilled_rows = count;
        Ok(())
    }

    /// Source rows remain borrowed until every transaction or partial-row write
    /// succeeds. A terminal failure poisons this consumer; callers cannot replay
    /// a prefix. Typed target pressure switches to the prepared spool instead.
    pub(super) fn ingest(&mut self, source: &GroupRows) -> Result<()> {
        if self.poisoned {
            return Err(invalid("previous merge failed"));
        }
        let result = self.ingest_inner(source, 0..source.len());
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    /// Validate the entire borrowed selection before applying any prefix.
    pub(super) fn ingest_selected(&mut self, source: &GroupRows, rows: &[usize]) -> Result<()> {
        if self.poisoned {
            return Err(invalid("previous merge failed"));
        }
        let result = super::row_selection::RowSelection::new(rows, source.len())
            .and_then(|selection| self.ingest_inner(source, selection.rows().iter().copied()));
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    pub(super) fn has_spooled(&self) -> bool {
        self.resident.is_none() || !self.runs.runs().is_empty()
    }

    /// Release resident capacities using the already prepared writer, before a
    /// sibling reducer needs space to decode its own spill. No input is replayed.
    pub(super) fn spool_for_finish(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(invalid("previous merge failed"));
        }
        let result = (|| {
            if let Some(resident) = self.resident.take() {
                for row in 0..resident.groups.len() {
                    self.append(&resident.groups, row)?;
                }
            }
            Ok(())
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn ingest_inner(
        &mut self,
        source: &GroupRows,
        rows: impl Iterator<Item = usize>,
    ) -> Result<()> {
        if source.layout_identity() != self.layout.identity() {
            return Err(invalid("source layout mismatch"));
        }
        for row in rows {
            let merged = if let Some(resident) = &mut self.resident {
                let result: Result<bool> = (|| {
                    resident.key.load_encoded(source.key(row)?.bytes())?;
                    if resident.groups.len() >= self.group_limit
                        && resident.groups.lookup(&resident.key)?.is_none()
                    {
                        return Ok(false);
                    }
                    resident
                        .groups
                        .prepare_merge_update(&resident.key, &mut resident.row, source, row)?
                        .commit();
                    Ok(true)
                })();
                match result {
                    Err(error) if error.is_memory_limit() => false,
                    result => result?,
                }
            } else {
                false
            };
            if merged {
                continue;
            }
            if let Some(resident) = self.resident.take() {
                // Each prior source row exists exactly once in resident state.
                // No final values are serialized, and no failed row was committed.
                for prior in 0..resident.groups.len() {
                    self.append(&resident.groups, prior)?;
                }
                drop(resident);
            }
            self.append(source, row)?;
        }
        Ok(())
    }

    /// Call after all source controllers and their borrowed callbacks finish.
    /// No callback is made while cross-owner partial rows remain unmerged.
    pub(super) fn finish(
        mut self,
        mut output: impl FnMut(&GroupRows) -> Result<()>,
    ) -> Result<IngestionStats> {
        if self.poisoned {
            return Err(invalid("cannot finalize failed merge"));
        }
        if self.runs.runs().is_empty() && self.resident.is_some() {
            let mut resident = self.resident.take().unwrap();
            drop(self.writer.take());
            resident.groups.ensure_empty_global(&mut resident.key)?;
            if resident.groups.len() != 0 {
                output(&resident.groups)?;
            }
            return Ok(IngestionStats::default());
        }
        if let Some(resident) = self.resident.take() {
            for row in 0..resident.groups.len() {
                self.append(&resident.groups, row)?;
            }
        }
        let writer = self
            .writer
            .take()
            .ok_or_else(|| invalid("missing prepared writer"))?;
        let bytes = if self.spilled_rows > 0 {
            self.runs.publish_writer(writer)?
        } else {
            drop(writer);
            0
        };
        let splits = PartitionScheduler::new(
            self.layout,
            self.directory,
            self.runs,
            self.frame_capacity,
            self.group_limit,
        )?
        .visit(output)?;
        Ok(IngestionStats {
            flushes: usize::from(self.spilled_rows > 0),
            spilled_rows: self.spilled_rows,
            spilled_bytes: bytes,
            merge_splits: splits,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::MemoryPool,
        planner::{AggregateFunction as A, DecimalValue, ScalarValue as V},
    };
    use arrow::datatypes::DataType;

    fn layout(pool: &MemoryPool) -> Arc<GroupLayout> {
        GroupLayout::bind(
            pool,
            &[DataType::Int64],
            &[
                (A::Count, DataType::Int64, false),
                (A::Sum, DataType::Decimal128(38, 2), false),
                (A::Avg, DataType::Float64, false),
            ],
        )
        .unwrap()
        .unwrap()
    }
    fn partial(layout: &Arc<GroupLayout>, weight: i128) -> GroupRows {
        let mut groups = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        let exact = (1i128 << 80) + 17;
        for _ in 0..weight {
            for k in 0..64 {
                key.encode(&[if k == 63 { V::Null } else { V::Int64(k) }])
                    .unwrap();
                groups
                    .prepare_update(
                        &key,
                        &mut row,
                        &[
                            V::Int64(1),
                            if k == 62 {
                                V::Null
                            } else {
                                V::Decimal128(DecimalValue::new(exact * weight, 2))
                            },
                            if k == 62 {
                                V::Null
                            } else {
                                V::Float64((10.0 * weight as f64).into())
                            },
                        ],
                    )
                    .unwrap()
                    .commit();
            }
        }
        groups
    }

    #[test]
    fn overlapping_partials_merge_exact_states_in_memory_and_through_spool() {
        for (limit, pressure) in [(usize::MAX, false), (7, false), (usize::MAX, true)] {
            let root = tempfile::tempdir().unwrap();
            let pool = MemoryPool::new_named("cross owner merge", 2 * 1024 * 1024);
            let layout = layout(&pool);
            let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
            let mut merge =
                PartialMerge::new(layout.clone(), directory.clone(), 1024, limit).unwrap();
            let sources = (1..=4).map(|w| partial(&layout, w)).collect::<Vec<_>>();
            let held = pressure.then(|| pool.allocate(pool.available()).unwrap());
            for source in &sources {
                merge.ingest(source).unwrap();
            }
            let expected_spill = pressure || limit != usize::MAX;
            assert_eq!(merge.resident.is_none(), expected_spill);
            drop((sources, held));
            let mut seen = [false; 64];
            let stats = merge
                .finish(|groups| {
                    for row in 0..groups.len() {
                        let key = groups.key(row)?;
                        let k = if key.bytes()[0] == 0 {
                            63
                        } else {
                            i64::from_le_bytes(key.bytes()[1..9].try_into().unwrap()) as usize
                        };
                        assert!(k < 64 && !seen[k]);
                        seen[k] = true;
                        assert_eq!(*groups.value(row, 0)?, V::Int64(10));
                        assert_eq!(
                            *groups.value(row, 1)?,
                            if k == 62 {
                                V::Null
                            } else {
                                V::Decimal128(DecimalValue::new(((1i128 << 80) + 17) * 30, 2))
                            }
                        );
                        assert_eq!(
                            *groups.value(row, 2)?,
                            if k == 62 {
                                V::Null
                            } else {
                                V::Float64(30.0.into())
                            }
                        );
                    }
                    Ok(())
                })
                .unwrap();
            assert!(seen.into_iter().all(|v| v));
            assert_eq!(stats.spilled_bytes > 0, expected_spill);
            assert_eq!(stats.flushes, usize::from(expected_spill));
            drop((directory, layout));
            assert_eq!(pool.used(), 0);
            assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
        }
    }

    #[test]
    fn invalid_selection_rejects_before_applying_a_valid_prefix() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("partial selection validation", 1048576);
        let layout = layout(&pool);
        let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
        let mut merge =
            PartialMerge::new(layout.clone(), directory.clone(), 1024, usize::MAX).unwrap();
        let groups = partial(&layout, 1);
        assert!(merge.ingest_selected(&groups, &[0, groups.len()]).is_err());
        assert_eq!(merge.resident.as_ref().unwrap().groups.len(), 0);
        assert!(merge.ingest(&groups).is_err());
        assert!(merge
            .finish(|_| panic!("invalid selection published"))
            .is_err());
        drop((groups, directory, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn foreign_partial_poison_prevents_any_final_publication() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("foreign merge", 1048576);
        let local = layout(&pool);
        let foreign = layout(&pool);
        let directory = RunDirectory::create(local.clone(), root.path()).unwrap();
        let mut merge = PartialMerge::new(local.clone(), directory.clone(), 1024, 7).unwrap();
        let valid = partial(&local, 1);
        merge.ingest(&valid).unwrap();
        let wrong = partial(&foreign, 2);
        assert!(!merge.ingest(&wrong).unwrap_err().is_memory_limit());
        assert!(merge.ingest(&valid).is_err());
        assert!(merge
            .finish(|_| panic!("failed merge must not publish"))
            .is_err());
        drop((valid, wrong, local, foreign, directory));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }

    #[test]
    fn inconsistent_spool_extent_cannot_publish_partial_results() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("corrupt merge spool", 1048576);
        let layout = layout(&pool);
        let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
        let mut merge = PartialMerge::new(layout.clone(), directory.clone(), 1024, 7).unwrap();
        let source = partial(&layout, 1);
        merge.ingest(&source).unwrap();
        drop(source);
        // Extend the file past its admitted writer's logical extent. Publication
        // must detect length mismatch even when its small IO buffer flushes later.
        let dir = std::fs::read_dir(root.path())
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let file = std::fs::read_dir(dir)
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        std::fs::OpenOptions::new()
            .write(true)
            .open(file)
            .unwrap()
            .set_len(1 << 20)
            .unwrap();
        let error = merge
            .finish(|_| panic!("corrupt spool must not publish"))
            .unwrap_err();
        assert!(!error.is_memory_limit());
        assert!(error.to_string().contains("completed file length mismatch"));
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
    #[test]
    fn startup_denial_and_empty_global_merge_release_all_owners() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("partial startup", 1048576);
        let layout = GroupLayout::bind(
            &pool,
            &[],
            &[
                (A::Count, DataType::Int64, false),
                (A::Avg, DataType::Float64, false),
            ],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
        let baseline = pool.used();
        let held = pool.allocate(pool.available()).unwrap();
        let failed = PartialMerge::new(layout.clone(), directory.clone(), 1024, 64);
        assert!(matches!(failed, Err(ref error) if error.is_memory_limit()));
        drop(held);
        assert_eq!(pool.used(), baseline);
        let merge = PartialMerge::new(layout.clone(), directory.clone(), 1024, 64).unwrap();
        let mut seen = 0;
        let stats = merge
            .finish(|groups| {
                seen += groups.len();
                assert_eq!(groups.len(), 1);
                assert_eq!(*groups.value(0, 0)?, V::Int64(0));
                assert_eq!(*groups.value(0, 1)?, V::Null);
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, 1);
        assert_eq!(stats.spilled_bytes, 0);
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}
