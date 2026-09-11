//! Bounded canonical-hash partitioning of resident partial states. Each reducer
//! owns disjoint keys, so only complete reducer results may concatenate.
use super::{
    group_rows::{GroupLayout, GroupRows},
    ingestion_controller::IngestionStats,
    partial_merge::PartialMerge,
    spill_files::RunDirectory,
};
use crate::{execution::reserved_vec::ReservedVec, QueryError, Result};
use rayon::prelude::*;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

const WINDOW: usize = 8192;
const MAX_REDUCERS: usize = 16;

pub(super) struct ParallelMerge {
    reducers: ReservedVec<PartialMerge>,
    indices: ReservedVec<usize>,
    owners: ReservedVec<u8>,
    counts: ReservedVec<usize>,
    offsets: ReservedVec<usize>,
    cursors: ReservedVec<usize>,
    layout: Arc<GroupLayout>,
    poisoned: bool,
    profile: bool,
    routing: Duration,
    processing: Duration,
    rows: usize,
    windows: usize,
    parallel_windows: usize,
}
fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("parallel partial merge: {message}"))
}
impl ParallelMerge {
    /// Optional startup admission. Failure drops all partial construction before
    /// returning None; the caller already owns the serial progress path.
    pub(super) fn try_new(
        layout: Arc<GroupLayout>,
        directory: Arc<RunDirectory>,
        frame_capacity: usize,
        group_limit: usize,
        workers: usize,
    ) -> Result<Option<Self>> {
        let workers = workers.min(MAX_REDUCERS);
        if workers < 2 || layout.key_layout().len() == 0 {
            return Ok(None);
        }
        let create = || -> Result<Self> {
            let mut reducers = ReservedVec::with_capacity(layout.pool(), workers)?;
            reducers.try_extend_reserved(
                workers,
                (0..workers).map(|_| {
                    PartialMerge::new(
                        layout.clone(),
                        directory.clone(),
                        frame_capacity,
                        group_limit.div_ceil(workers).max(1),
                    )
                }),
            )?;
            let mut indices = ReservedVec::with_capacity(layout.pool(), WINDOW)?;
            indices.extend_reserved(WINDOW, std::iter::repeat_n(0, WINDOW))?;
            let mut owners = ReservedVec::with_capacity(layout.pool(), WINDOW)?;
            owners.extend_reserved(WINDOW, std::iter::repeat_n(0, WINDOW))?;
            let mut counts = ReservedVec::with_capacity(layout.pool(), workers)?;
            counts.extend_reserved(workers, std::iter::repeat_n(0, workers))?;
            let mut offsets = ReservedVec::with_capacity(layout.pool(), workers + 1)?;
            offsets.extend_reserved(workers + 1, std::iter::repeat_n(0, workers + 1))?;
            let mut cursors = ReservedVec::with_capacity(layout.pool(), workers)?;
            cursors.extend_reserved(workers, std::iter::repeat_n(0, workers))?;
            Ok(Self {
                reducers,
                indices,
                owners,
                counts,
                offsets,
                cursors,
                layout: layout.clone(),
                poisoned: false,
                profile: std::env::var_os("QE_AGG_PROF").is_some(),
                routing: Duration::ZERO,
                processing: Duration::ZERO,
                rows: 0,
                windows: 0,
                parallel_windows: 0,
            })
        };
        match create() {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.is_memory_limit() => Ok(None),
            Err(error) => Err(error),
        }
    }
    pub(super) fn ingest(&mut self, source: &GroupRows) -> Result<()> {
        if self.poisoned {
            return Err(invalid("previous merge failed"));
        }
        let result = self.ingest_inner(source);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn ingest_inner(&mut self, source: &GroupRows) -> Result<()> {
        if source.layout_identity() != self.layout.identity() {
            return Err(invalid("source layout mismatch"));
        }
        let workers = self.reducers.as_slice().len();
        let mut start = 0;
        while start < source.len() {
            let len = (source.len() - start).min(WINDOW);
            let time = self.profile.then(Instant::now);
            self.counts.as_mut_slice().fill(0);
            for i in 0..len {
                let owner = (source.key(start + i)?.hash64() % workers as u64) as usize;
                self.owners.as_mut_slice()[i] = owner as u8;
                self.counts.as_mut_slice()[owner] += 1;
            }
            self.offsets.as_mut_slice()[0] = 0;
            for owner in 0..workers {
                self.offsets.as_mut_slice()[owner + 1] =
                    self.offsets.as_slice()[owner] + self.counts.as_slice()[owner];
            }
            self.cursors
                .as_mut_slice()
                .copy_from_slice(&self.offsets.as_slice()[..workers]);
            for i in 0..len {
                let owner = self.owners.as_slice()[i] as usize;
                let position = self.cursors.as_slice()[owner];
                self.indices.as_mut_slice()[position] = start + i;
                self.cursors.as_mut_slice()[owner] += 1;
            }
            if let Some(time) = time {
                self.routing += time.elapsed();
            }
            let time = self.profile.then(Instant::now);
            let indices = self.indices.as_slice();
            let offsets = self.offsets.as_slice();
            let apply = |(owner, reducer): (usize, &mut PartialMerge)| {
                let rows = &indices[offsets[owner]..offsets[owner + 1]];
                if rows.is_empty() {
                    Ok(())
                } else {
                    reducer.ingest_selected(source, rows)
                }
            };
            let active = self.counts.as_slice().iter().filter(|&&n| n != 0).count();
            if active > 1 && len / active >= 256 {
                self.reducers
                    .as_mut_slice()
                    .par_iter_mut()
                    .enumerate()
                    .try_for_each(apply)?;
                self.parallel_windows += 1;
            } else {
                self.reducers
                    .as_mut_slice()
                    .iter_mut()
                    .enumerate()
                    .try_for_each(apply)?;
            }
            if let Some(time) = time {
                self.processing += time.elapsed();
            }
            self.rows = self
                .rows
                .checked_add(len)
                .ok_or_else(|| invalid("partial row count overflow"))?;
            self.windows += 1;
            start += len;
        }
        Ok(())
    }
    pub(super) fn finish(
        mut self,
        mut output: impl FnMut(&GroupRows) -> Result<()>,
    ) -> Result<IngestionStats> {
        if self.poisoned {
            return Err(invalid("cannot finalize failed merge"));
        }
        if self.profile {
            eprintln!("live_aggregate_reduction workers={} method=parallel partial_rows={} windows={} parallel_windows={} routing_ms={:.3} merging_wall_ms={:.3}",
                self.reducers.as_slice().len(),self.rows,self.windows,self.parallel_windows,self.routing.as_secs_f64()*1000.0,self.processing.as_secs_f64()*1000.0);
        }
        // Free routing storage before any reducer needs spill-decoding space.
        drop((
            self.indices,
            self.owners,
            self.counts,
            self.offsets,
            self.cursors,
        ));
        if self
            .reducers
            .as_slice()
            .iter()
            .any(PartialMerge::has_spooled)
        {
            // Prepared writers let resident siblings release capacities without
            // emergency allocation. Reducer key domains remain disjoint.
            for reducer in self.reducers.as_mut_slice() {
                reducer.spool_for_finish()?;
            }
        }
        let mut total = IngestionStats::default();
        for reducer in self.reducers.into_owned_iter() {
            let stats = reducer.finish(&mut output)?;
            macro_rules! add {
                ($field:ident) => {
                    total.$field = total
                        .$field
                        .checked_add(stats.$field)
                        .ok_or_else(|| invalid("metric overflow"))?;
                };
            }
            add!(flushes);
            add!(spilled_rows);
            add!(spilled_bytes);
            add!(merge_splits);
        }
        Ok(total)
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
    fn source(layout: &Arc<GroupLayout>, n: usize, weight: i64) -> GroupRows {
        let mut groups = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        for k in 0..n {
            key.encode(&[if k == n - 1 {
                V::Null
            } else {
                V::Int64(k as i64)
            }])
            .unwrap();
            for _ in 0..weight {
                groups
                    .prepare_update(
                        &key,
                        &mut row,
                        &[
                            V::Int64(1),
                            if k == n - 2 {
                                V::Null
                            } else {
                                V::Decimal128(DecimalValue::new(
                                    ((1i128 << 80) + 17) * weight as i128,
                                    2,
                                ))
                            },
                            if k == n - 2 {
                                V::Null
                            } else {
                                V::Float64((weight as f64 * 10.0).into())
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
    fn parallel_reduction_routes_every_partial_once_and_preserves_exact_states() {
        for (workers, n, limit, pressure) in [
            (4, 9001, usize::MAX, false),
            (16, 9001, usize::MAX, false),
            (4, 128, 32, false),
            (4, 128, 128, false),
            (4, 128, usize::MAX, true),
        ] {
            let root = tempfile::tempdir().unwrap();
            let pool = MemoryPool::new_named("parallel reduction semantic gate", 32 * 1024 * 1024);
            let layout = layout(&pool);
            let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
            let mut merge =
                ParallelMerge::try_new(layout.clone(), directory.clone(), 1024, limit, workers)
                    .unwrap()
                    .unwrap();
            let inputs = [source(&layout, n, 1), source(&layout, n, 2)];
            let hold = pressure.then(|| pool.allocate(pool.available()).unwrap());
            for input in &inputs {
                merge.ingest(input).unwrap();
            }
            assert_eq!(merge.rows, 2 * n);
            if n > WINDOW {
                assert!(merge.parallel_windows > 0);
            }
            drop((inputs, hold));
            let mut seen = vec![false; n];
            let stats = merge
                .finish(|groups| {
                    for row in 0..groups.len() {
                        let key = groups.key(row)?;
                        let k = if key.bytes()[0] == 0 {
                            n - 1
                        } else {
                            i64::from_le_bytes(key.bytes()[1..9].try_into().unwrap()) as usize
                        };
                        assert!(k < n && !seen[k]);
                        seen[k] = true;
                        assert_eq!(*groups.value(row, 0)?, V::Int64(3));
                        assert_eq!(
                            *groups.value(row, 1)?,
                            if k == n - 2 {
                                V::Null
                            } else {
                                V::Decimal128(DecimalValue::new(((1i128 << 80) + 17) * 5, 2))
                            }
                        );
                        assert_eq!(
                            *groups.value(row, 2)?,
                            if k == n - 2 {
                                V::Null
                            } else {
                                V::Float64((50.0 / 3.0).into())
                            }
                        );
                    }
                    Ok(())
                })
                .unwrap();
            assert!(seen.into_iter().all(|v| v));
            assert_eq!(stats.spilled_bytes > 0, pressure || limit != usize::MAX);
            drop((directory, layout));
            assert_eq!(pool.used(), 0);
            assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
        }
    }
    #[test]
    fn parallel_reduction_denial_poison_and_output_failure_release_owners() {
        let root = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("parallel reduction lifecycle", 4 * 1024 * 1024);
        let layout = layout(&pool);
        let directory = RunDirectory::create(layout.clone(), root.path()).unwrap();
        let baseline = pool.used();
        let hold = pool.allocate(pool.available() - 4096).unwrap();
        let held = pool.used();
        assert!(
            ParallelMerge::try_new(layout.clone(), directory.clone(), 1024, usize::MAX, 4)
                .unwrap()
                .is_none()
        );
        assert_eq!(pool.used(), held);
        drop(hold);
        assert_eq!(pool.used(), baseline);
        let foreign = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(A::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let foreign_groups = GroupRows::new(foreign.clone()).unwrap();
        let mut merge =
            ParallelMerge::try_new(layout.clone(), directory.clone(), 1024, usize::MAX, 4)
                .unwrap()
                .unwrap();
        assert!(merge.ingest(&foreign_groups).is_err());
        assert!(merge
            .finish(|_| panic!("poisoned merge published output"))
            .is_err());
        drop((foreign_groups, foreign));
        assert_eq!(pool.used(), baseline);
        let empty = ParallelMerge::try_new(layout.clone(), directory.clone(), 1024, usize::MAX, 4)
            .unwrap()
            .unwrap();
        empty
            .finish(|_| panic!("empty grouped input emitted a group"))
            .unwrap();
        assert_eq!(pool.used(), baseline);
        for limit in [usize::MAX, 4] {
            let mut merge =
                ParallelMerge::try_new(layout.clone(), directory.clone(), 1024, limit, 4)
                    .unwrap()
                    .unwrap();
            let groups = source(&layout, 128, 1);
            merge.ingest(&groups).unwrap();
            drop(groups);
            let error = merge
                .finish(|_| Err(QueryError::Execution("injected consumer failure".into())))
                .unwrap_err();
            assert!(error.to_string().contains("injected consumer failure"));
            assert_eq!(pool.used(), baseline);
        }
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    }
}
