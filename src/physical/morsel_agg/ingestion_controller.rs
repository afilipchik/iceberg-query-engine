//! Own the spill transition while the caller retains evaluated input arrays.
use super::{
    group_rows::{GroupLayout, GroupRows},
    key_rows::KeyWorkspace,
    partition_scheduler::PartitionScheduler,
    spill_files::{RunCollection, RunDirectory, RunWriter},
    state_rows::RowWorkspace,
};
use crate::{QueryError, Result};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

struct Resident {
    groups: GroupRows,
    key: KeyWorkspace,
    row: RowWorkspace,
}
impl Resident {
    fn new(layout: &Arc<GroupLayout>) -> Result<Self> {
        Ok(Self {
            groups: GroupRows::new(layout.clone())?,
            key: layout.key_workspace()?,
            row: layout.row_workspace()?,
        })
    }
}

#[derive(Default, Debug)]
pub(super) struct IngestionStats {
    pub(super) flushes: usize,
    pub(super) spilled_rows: u64,
    pub(super) spilled_bytes: u64,
    pub(super) merge_splits: usize,
}

#[derive(Default)]
pub(super) struct IngestionProgress {
    pub(super) next_row: usize,
    pub(super) done: bool,
    pub(super) pressure: Option<QueryError>,
}

pub(super) struct IngestionController {
    // Prepared file ownership exists before resident state fills. Option allows
    // consuming finish without creating a replacement while memory is exhausted.
    writer: Option<RunWriter>,
    resident: Option<Resident>,
    runs: RunCollection,
    layout: Arc<GroupLayout>,
    directory: Arc<RunDirectory>,
    max_runs: usize,
    frame_capacity: usize,
    group_limit: usize,
    stats: IngestionStats,
    poisoned: bool,
}

fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("aggregate ingestion controller: {message}"))
}

impl IngestionController {
    pub(super) fn new(
        layout: Arc<GroupLayout>,
        directory: Arc<RunDirectory>,
        max_runs: usize,
        frame_capacity: usize,
        group_limit: usize,
    ) -> Result<Self> {
        let mut runs = RunCollection::new(layout.clone())?;
        let writer = runs
            .prepare_flush_bounded(&directory, max_runs, frame_capacity, group_limit)?
            .into_writer();
        let resident = Resident::new(&layout)?;
        Ok(Self {
            writer: Some(writer),
            resident: Some(resident),
            runs,
            layout,
            directory,
            max_runs,
            frame_capacity,
            group_limit,
            stats: IngestionStats::default(),
            poisoned: false,
        })
    }

    /// The batch's owner and expression evaluation stay outside this controller.
    /// This call does not return until all rows apply or a terminal error occurs;
    /// it can flush and retry only the exact first unapplied row of these arrays.
    pub(super) fn ingest(&mut self, batch: &RecordBatch, group_count: usize) -> Result<()> {
        if self.poisoned {
            return Err(invalid("previous ingestion failed"));
        }
        let result = self.ingest_inner(batch, group_count, None, None);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    /// Apply a retained selection without gathering or re-evaluating columns.
    /// Invalid indices are rejected before this call changes aggregate state.
    pub(super) fn ingest_selected(
        &mut self,
        batch: &RecordBatch,
        group_count: usize,
        rows: &[usize],
    ) -> Result<()> {
        if self.poisoned {
            return Err(invalid("previous ingestion failed"));
        }
        let result = super::row_selection::RowSelection::new(rows, batch.num_rows())
            .and_then(|selection| self.ingest_inner(batch, group_count, Some(selection), None));
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    pub(super) fn ingest_prepared_selected(
        &mut self,
        batch: &RecordBatch,
        group_count: usize,
        rows: &[usize],
        prepared: Option<&super::prepared_keys::PreparedKeys<'_>>,
    ) -> Result<()> {
        if self.poisoned {
            return Err(invalid("previous ingestion failed"));
        }
        let result = (|| {
            if let Some(keys) = prepared {
                keys.validate(&self.layout, batch, group_count)?;
            }
            let selection = super::row_selection::RowSelection::new(rows, batch.num_rows())?;
            self.ingest_inner(batch, group_count, Some(selection), prepared)
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    /// Stop at the exact unapplied selected row so the parent can release peer
    /// state before compaction. Evaluated arrays and selection stay borrowed.
    pub(super) fn ingest_prepared_step(
        &mut self,
        batch: &RecordBatch,
        group_count: usize,
        rows: &[usize],
        prepared: Option<&super::prepared_keys::PreparedKeys<'_>>,
        next_row: usize,
    ) -> Result<IngestionProgress> {
        if self.poisoned {
            return Err(invalid("previous ingestion failed"));
        }
        let result = (|| {
            if let Some(keys) = prepared {
                keys.validate(&self.layout, batch, group_count)?;
            }
            let selection = super::row_selection::RowSelection::new(rows, batch.num_rows())?;
            let resident = self
                .resident
                .as_mut()
                .ok_or_else(|| invalid("missing resident state"))?;
            match resident.groups.process_selected_with_limit(
                batch,
                group_count,
                selection,
                next_row,
                &mut resident.key,
                &mut resident.row,
                self.group_limit,
                prepared,
            ) {
                Ok(end) => Ok(IngestionProgress {
                    next_row: end,
                    done: end == rows.len(),
                    pressure: None,
                }),
                Err(failure) if failure.error.is_memory_limit() => Ok(IngestionProgress {
                    next_row: failure.next_row,
                    done: false,
                    pressure: Some(failure.error),
                }),
                Err(failure) => Err(failure.error),
            }
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    pub(super) fn has_resident_groups(&self) -> bool {
        self.resident.as_ref().is_some_and(|r| r.groups.len() != 0)
    }

    /// All workers stop before this phase. Existing prepared writers permit
    /// publishing partial states without allocating a replacement working set.
    pub(super) fn park_for_spill(&mut self) -> Result<()> {
        if self.poisoned {
            return Err(invalid("previous ingestion failed"));
        }
        if self.has_resident_groups() {
            self.flush()?;
        }
        drop(self.resident.take());
        drop(self.writer.take());
        Ok(())
    }

    pub(super) fn prepare_parked_writer(&mut self) -> Result<()> {
        if self.poisoned || self.resident.is_some() || self.writer.is_some() {
            return Err(invalid("worker is not parked"));
        }
        self.writer = Some(
            self.runs
                .prepare_flush_bounded(
                    &self.directory,
                    self.max_runs,
                    self.frame_capacity,
                    self.group_limit,
                )?
                .into_writer(),
        );
        Ok(())
    }

    pub(super) fn restore_parked_resident(&mut self) -> Result<()> {
        if self.poisoned || self.resident.is_some() || self.writer.is_none() {
            return Err(invalid("parked writer is not prepared"));
        }
        self.resident = Some(Resident::new(&self.layout)?);
        Ok(())
    }

    /// Restore the global scheduling limit when unused parallel workers are
    /// removed. The startup writer is not sized by this group-count limit;
    /// prepare_flush_bounded uses it only when compacting published runs.
    pub(super) fn restore_unused_group_limit(&mut self, group_limit: usize) -> Result<()> {
        if self.poisoned
            || group_limit == 0
            || self.stats.flushes != 0
            || !self.runs.runs().is_empty()
            || !self.resident.as_ref().is_some_and(|r| r.groups.len() == 0)
            || self.writer.is_none()
        {
            return Err(invalid("cannot change the limit of a used worker"));
        }
        self.group_limit = group_limit;
        Ok(())
    }

    /// Free resident state before the caller admits another retained input.
    /// No source is pulled or evaluated here. An empty resident cannot free more.
    pub(super) fn release_for_input(&mut self) -> Result<bool> {
        if self.poisoned {
            return Err(invalid("previous ingestion failed"));
        }
        if !self.resident.as_ref().is_some_and(|r| r.groups.len() != 0) {
            return Ok(false);
        }
        let result = (|| {
            self.flush()?;
            drop(self.resident.take());
            self.writer = Some(
                self.runs
                    .prepare_flush_bounded(
                        &self.directory,
                        self.max_runs,
                        self.frame_capacity,
                        self.group_limit,
                    )?
                    .into_writer(),
            );
            self.resident = Some(Resident::new(&self.layout)?);
            Ok(true)
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn ingest_inner(
        &mut self,
        batch: &RecordBatch,
        group_count: usize,
        selection: Option<super::row_selection::RowSelection<'_>>,
        prepared: Option<&super::prepared_keys::PreparedKeys<'_>>,
    ) -> Result<()> {
        let mut next_row = 0;
        let end_position = selection.map_or(batch.num_rows(), |s| s.len());
        loop {
            let resident = self
                .resident
                .as_mut()
                .ok_or_else(|| invalid("missing resident state"))?;
            let progress = if let Some(selection) = selection {
                resident.groups.process_selected_with_limit(
                    batch,
                    group_count,
                    selection,
                    next_row,
                    &mut resident.key,
                    &mut resident.row,
                    self.group_limit,
                    prepared,
                )
            } else {
                resident.groups.process_evaluated_with_limit(
                    batch,
                    group_count,
                    next_row,
                    &mut resident.key,
                    &mut resident.row,
                    self.group_limit,
                )
            };
            match progress {
                Ok(end) if end == end_position => return Ok(()),
                Ok(end) => next_row = end,
                Err(failure) if failure.error.is_memory_limit() => {
                    if resident.groups.len() == 0 {
                        return Err(failure.error);
                    }
                    next_row = failure.next_row;
                }
                Err(failure) => return Err(failure.error),
            }
            self.flush()?;
            // Release all old capacities, including failed key/winner scratch,
            // before compaction and admitting the next resident working set.
            drop(self.resident.take());
            self.writer = Some(
                self.runs
                    .prepare_flush_bounded(
                        &self.directory,
                        self.max_runs,
                        self.frame_capacity,
                        self.group_limit,
                    )?
                    .into_writer(),
            );
            self.resident = Some(Resident::new(&self.layout)?);
        }
    }

    fn flush(&mut self) -> Result<()> {
        let resident = self
            .resident
            .as_mut()
            .ok_or_else(|| invalid("missing resident state"))?;
        let count = resident.groups.len();
        if count == 0 {
            return Err(invalid("flush cannot make progress on empty state"));
        }
        let writer = self
            .writer
            .take()
            .ok_or_else(|| invalid("missing prepared spill writer"))?;
        self.runs.flush_writer(writer, &mut resident.groups)?;
        self.stats.flushes = self
            .stats
            .flushes
            .checked_add(1)
            .ok_or_else(|| invalid("flush count overflow"))?;
        self.stats.spilled_rows = self
            .stats
            .spilled_rows
            .checked_add(count as u64)
            .ok_or_else(|| invalid("spill row count overflow"))?;
        self.stats.spilled_bytes = self
            .stats
            .spilled_bytes
            .checked_add(self.runs.last_run_bytes())
            .ok_or_else(|| invalid("spill byte count overflow"))?;
        Ok(())
    }

    pub(super) fn has_spilled_runs(&self) -> bool {
        !self.runs.runs().is_empty()
    }

    /// Transfer unfinalized resident state and existing partial spill runs.
    /// The cross-owner consumer performs the only final group merge.
    pub(super) fn into_partials(
        mut self,
    ) -> Result<(Option<GroupRows>, RunCollection, IngestionStats)> {
        if self.poisoned {
            return Err(invalid("cannot transfer failed ingestion"));
        }
        drop(self.writer.take());
        let groups = self
            .resident
            .take()
            .map(|r| r.groups)
            .filter(|g| g.len() != 0);
        Ok((groups, self.runs, self.stats))
    }

    /// Complete groups are borrowed for admitted output construction. A later
    /// callback failure invalidates the query's entire partial output collection.
    pub(super) fn finish(
        mut self,
        mut output: impl FnMut(&GroupRows) -> Result<()>,
    ) -> Result<IngestionStats> {
        if self.poisoned {
            return Err(invalid("cannot finalize failed ingestion"));
        }
        if self.runs.runs().is_empty() {
            drop(self.writer.take());
            let resident = self
                .resident
                .as_mut()
                .ok_or_else(|| invalid("missing resident state"))?;
            resident.groups.ensure_empty_global(&mut resident.key)?;
            if resident.groups.len() != 0 {
                output(&resident.groups)?;
            }
        } else {
            if self.resident.as_ref().is_some_and(|r| r.groups.len() != 0) {
                self.flush()?;
            }
            drop(self.writer.take());
            drop(self.resident.take());
            self.stats.merge_splits = PartitionScheduler::new(
                self.layout,
                self.directory,
                self.runs,
                self.frame_capacity,
                self.group_limit,
            )?
            .visit(output)?;
        }
        Ok(self.stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::MemoryPool,
        planner::{AggregateFunction, DecimalValue, ScalarValue},
    };
    use arrow::{
        array::*,
        datatypes::{DataType, Field, Schema},
    };

    fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
        let fields: Vec<_> = columns
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn selected_rows_preserve_order_multiplicity_nulls_and_spill_cursor() {
        for (group_limit, reuse) in [
            (16, false),
            (usize::MAX, false),
            (16, true),
            (usize::MAX, true),
        ] {
            let parent = tempfile::tempdir().unwrap();
            let pool =
                MemoryPool::new_named("selected controller", if reuse { 1048576 } else { 262144 });
            let layout = GroupLayout::bind(
                &pool,
                &[DataType::Int64],
                &[
                    (AggregateFunction::Count, DataType::Int64, false),
                    (AggregateFunction::Sum, DataType::Decimal128(38, 2), false),
                ],
            )
            .unwrap()
            .unwrap();
            let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
            let mut controller =
                IngestionController::new(layout.clone(), directory.clone(), 3, 1024, group_limit)
                    .unwrap();
            let exact = (1i128 << 80) + 17;
            let input = batch(vec![
                Arc::new(Int64Array::from(
                    (0..1000)
                        .map(|i| if i == 999 { None } else { Some(i) })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    (0..1000)
                        .map(|i| if i % 7 == 0 { None } else { Some(1) })
                        .collect::<Vec<_>>(),
                )),
                Arc::new(
                    Decimal128Array::from((0..1000).map(|i| Some(exact + i)).collect::<Vec<_>>())
                        .with_precision_and_scale(38, 2)
                        .unwrap(),
                ),
            ]);
            let selection: Vec<usize> = (0..1000)
                .rev()
                .filter(|i| i % 2 == 0)
                .chain([998, 1, 999, 1])
                .collect();
            let held = pool
                .allocate(
                    input.get_array_memory_size() + selection.len() * std::mem::size_of::<usize>(),
                )
                .unwrap();
            if reuse {
                let prepared =
                    super::super::prepared_keys::PreparedKeys::try_new(&layout, &input, 1)
                        .unwrap()
                        .expect("prepared keys must be exercised");
                controller
                    .ingest_prepared_selected(&input, 1, &[], Some(&prepared))
                    .unwrap();
                controller
                    .ingest_prepared_selected(&input, 1, &selection, Some(&prepared))
                    .unwrap();
            } else {
                controller.ingest_selected(&input, 1, &[]).unwrap();
                controller.ingest_selected(&input, 1, &selection).unwrap();
            }
            let mut expected = std::collections::BTreeMap::new();
            for &i in &selection {
                let key = if i == 999 { None } else { Some(i as i64) };
                let value = expected.entry(key).or_insert((0i64, 0i128));
                value.0 += i64::from(i % 7 != 0);
                value.1 += exact + i as i128;
            }
            drop((input, selection, held));
            let stats = controller
                .finish(|groups| {
                    for row in 0..groups.len() {
                        let key = groups.key(row)?;
                        let bytes = key.bytes();
                        let key = if bytes[0] == 0 {
                            None
                        } else {
                            Some(i64::from_le_bytes(bytes[1..9].try_into().unwrap()))
                        };
                        let (count, sum) =
                            expected.remove(&key).expect("unexpected or repeated group");
                        assert_eq!(*groups.value(row, 0)?, ScalarValue::Int64(count));
                        assert_eq!(
                            *groups.value(row, 1)?,
                            ScalarValue::Decimal128(DecimalValue::new(sum, 2))
                        );
                    }
                    Ok(())
                })
                .unwrap();
            assert!(expected.is_empty());
            if group_limit == 16 {
                assert!(stats.flushes > 1 && stats.spilled_bytes > 0);
            }
            drop((directory, layout));
            assert_eq!(pool.used(), 0);
            assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
        }
    }

    #[test]
    fn invalid_selection_is_rejected_before_applying_its_valid_prefix() {
        let parent = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("invalid selection", 65536);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let mut controller =
            IngestionController::new(layout.clone(), directory.clone(), 3, 1024, 16).unwrap();
        let input = batch(vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![1, 1])),
        ]);
        let error = controller.ingest_selected(&input, 1, &[1, 2]).unwrap_err();
        assert!(error.to_string().contains("selection row out of bounds"));
        assert_eq!(controller.resident.as_ref().unwrap().groups.len(), 0);
        assert!(controller
            .finish(|_| panic!("failed ingestion must not finalize"))
            .is_err());
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn retained_batches_complete_through_compaction_and_real_pool_pressure() {
        for group_limit in [32, usize::MAX] {
            let parent = tempfile::tempdir().unwrap();
            let pool = MemoryPool::new_named("controller 256KiB", 262144);
            let layout = GroupLayout::bind(
                &pool,
                &[DataType::Int64],
                &[
                    (AggregateFunction::Count, DataType::Int64, false),
                    (AggregateFunction::Sum, DataType::Decimal128(38, 2), false),
                    (AggregateFunction::Avg, DataType::Float64, false),
                ],
            )
            .unwrap()
            .unwrap();
            let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
            let mut controller =
                IngestionController::new(layout.clone(), directory.clone(), 3, 1024, group_limit)
                    .unwrap();
            let exact = (1i128 << 80) + 17;
            let mut input_pulls = 0;
            // Preserve actual spilling after reducing the per-group fixed footprint.
            for weight in 1..=4 {
                let input = batch(vec![
                    Arc::new(Int64Array::from(
                        (0..2000)
                            .map(|key| if key == 1999 { None } else { Some(key) })
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(vec![Some(1); 2000])),
                    Arc::new(
                        Decimal128Array::from(vec![Some(exact * weight); 2000])
                            .with_precision_and_scale(38, 2)
                            .unwrap(),
                    ),
                    Arc::new(Float64Array::from(vec![Some(weight as f64 * 10.0); 2000])),
                ]);
                // The caller owns and accounts for its retained evaluated batch.
                let input_lease = pool.allocate(input.get_array_memory_size()).unwrap();
                input_pulls += 1;
                controller.ingest(&input, 1).unwrap();
                drop(input);
                drop(input_lease);
            }
            let mut seen = vec![false; 2000];
            let stats = controller
                .finish(|groups| {
                    for row in 0..groups.len() {
                        let key_ref = groups.key(row)?;
                        let encoded = key_ref.bytes();
                        let key = if encoded[0] == 0 {
                            1999
                        } else {
                            i64::from_le_bytes(encoded[1..9].try_into().unwrap()) as usize
                        };
                        assert!(!seen[key]);
                        seen[key] = true;
                        assert_eq!(*groups.value(row, 0)?, ScalarValue::Int64(4));
                        assert_eq!(
                            *groups.value(row, 1)?,
                            ScalarValue::Decimal128(DecimalValue::new(exact * 10, 2))
                        );
                        assert_eq!(*groups.value(row, 2)?, ScalarValue::Float64(25.0.into()));
                    }
                    Ok(())
                })
                .unwrap();
            assert_eq!(input_pulls, 4);
            assert!(seen.into_iter().all(|s| s));
            assert!(stats.flushes > 3, "{stats:?}");
            assert!(stats.spilled_rows >= 2000);
            assert!(stats.spilled_bytes > 0);
            drop((directory, layout));
            assert_eq!(pool.used(), 0);
            assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
        }
    }

    #[test]
    fn unsplittable_row_refuses_once_and_failed_ingestion_cannot_finalize() {
        let parent = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("oversized controller input", 65536);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Max, DataType::Utf8, false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let mut controller =
            IngestionController::new(layout.clone(), directory.clone(), 3, 1024, usize::MAX)
                .unwrap();
        let seed = batch(vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["a"])),
        ]);
        controller.ingest(&seed, 1).unwrap();
        let too_large = batch(vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(StringArray::from(vec!["z".repeat(65536)])),
        ]);
        let error = controller.ingest(&too_large, 1).unwrap_err();
        assert!(error.is_memory_limit());
        assert_eq!(controller.stats.flushes, 1);
        assert!(controller.ingest(&seed, 1).is_err());
        let mut called = false;
        assert!(controller
            .finish(|_| {
                called = true;
                Ok(())
            })
            .is_err());
        assert!(!called);
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn in_memory_finish_does_not_spill_duplicates_at_the_group_limit() {
        let parent = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("controller no spill", 65536);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let mut controller =
            IngestionController::new(layout.clone(), directory.clone(), 3, 1024, 1).unwrap();
        let input = batch(vec![
            Arc::new(Int64Array::from(vec![1, 1, 1])),
            Arc::new(Int64Array::from(vec![Some(1), None, Some(1)])),
        ]);
        controller.ingest(&input, 1).unwrap();
        let stats = controller
            .finish(|groups| {
                assert_eq!(groups.len(), 1);
                assert_eq!(*groups.value(0, 0)?, ScalarValue::Int64(2));
                Ok(())
            })
            .unwrap();
        assert_eq!(stats.flushes, 0);
        let mut controller =
            IngestionController::new(layout.clone(), directory.clone(), 3, 1024, 1).unwrap();
        controller.ingest(&input, 1).unwrap();
        assert!(controller
            .finish(|_| Err(invalid("injected output failure")))
            .is_err());
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }
}
