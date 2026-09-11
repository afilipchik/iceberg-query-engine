//! Disjoint-key or balanced-row owners sharing a query pool and retained batch.
use super::{
    group_rows::{GroupLayout, GroupRows},
    ingestion_controller::{IngestionController, IngestionStats},
    parallel_merge::ParallelMerge,
    partial_merge::PartialMerge,
    row_router,
    spill_files::RunDirectory,
};
use crate::{execution::reserved_vec::ReservedVec, QueryError, Result};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub(super) struct ParallelControllers {
    controllers: ReservedVec<IngestionController>,
    layout: Arc<GroupLayout>,
    merger: Option<PartialMerge>,
    parallel_merger: Option<ParallelMerge>,
    // Empty batches publish no group rows. Once a nonempty input is attempted,
    // ownership cannot be changed, even if a later spill empties resident state.
    ingestion_started: bool,
    group_limit: usize,
    poisoned: bool,
    profile: bool,
    routing_time: Duration,
    processing_time: Duration,
    serial_batches: usize,
    parallel_batches: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::MemoryPool,
        planner::{AggregateFunction, DecimalValue, ScalarValue},
    };
    use arrow::{array::*, datatypes::DataType};

    #[test]
    fn unused_partial_workers_release_headroom_for_same_retained_input() {
        for partial in [false, true] {
            for empty_prefix in [false, true] {
                check_unused_workers(partial, empty_prefix);
            }
        }
    }

    fn check_unused_workers(partial: bool, empty_prefix: bool) {
        let parent = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("startup query", 256 * 1024);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[
                (AggregateFunction::Count, DataType::Int64, false),
                (AggregateFunction::Sum, DataType::Int64, false),
            ],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let workers = if partial { 16 } else { 4 };
        let constructor = if partial {
            ParallelControllers::new_partial
        } else {
            ParallelControllers::new
        };
        let mut controllers =
            constructor(layout.clone(), directory.clone(), 8, 1024, 1024, workers).unwrap();
        assert_eq!(controllers.controllers.as_slice().len(), workers);
        let rows = if partial { 8192 } else { 14000 };
        let keys: Vec<Option<i64>> = (0..rows).map(|i| (i % 17 != 0).then_some(i % 5)).collect();
        let values: Vec<Option<i64>> = (0..rows).map(|i| (i % 11 != 0).then_some(i)).collect();
        let mut expected = std::collections::BTreeMap::<Option<i64>, (i64, i64)>::new();
        for (&key, &value) in keys.iter().zip(&values) {
            let state = expected.entry(key).or_default();
            if let Some(value) = value {
                state.0 += 1;
                state.1 += value;
            }
        }
        let values = Arc::new(Int64Array::from(values)) as ArrayRef;
        let batch = RecordBatch::try_from_iter([
            ("g", Arc::new(Int64Array::from(keys)) as ArrayRef),
            ("c", values.clone()),
            ("s", values),
        ])
        .unwrap();
        if empty_prefix {
            controllers.ingest(&batch.slice(0, 0), 1).unwrap();
        }
        let bytes = crate::execution::retained_batch::retained_batch_bytes(&batch, &pool).unwrap();
        assert!(pool.allocate(bytes).unwrap_err().is_memory_limit());
        assert!(
            controllers.release_for_input().unwrap(),
            "unused startup owners must release headroom"
        );
        assert_eq!(controllers.controllers.as_slice().len(), 1);
        let held = pool.allocate(bytes).unwrap();
        controllers.ingest(&batch, 1).unwrap();
        drop((held, batch));
        controllers
            .finish(|groups| {
                for row in 0..groups.len() {
                    let key = groups.key(row)?;
                    let bytes = key.bytes();
                    let key = (bytes[0] != 0)
                        .then(|| i64::from_le_bytes(bytes[1..9].try_into().unwrap()));
                    let (count, sum) = expected
                        .remove(&key)
                        .expect("duplicate or unexpected group");
                    assert_eq!(*groups.value(row, 0)?, ScalarValue::Int64(count));
                    assert_eq!(*groups.value(row, 1)?, ScalarValue::Int64(sum));
                }
                Ok(())
            })
            .unwrap();
        assert!(expected.is_empty());
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn populated_then_spilled_workers_never_revert_to_startup_ownership() {
        let parent = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new(2 * 1024 * 1024);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[
                (AggregateFunction::Count, DataType::Int64, false),
                (AggregateFunction::Sum, DataType::Int64, false),
            ],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let mut controllers =
            ParallelControllers::new_partial(layout.clone(), directory.clone(), 8, 1024, 1024, 4)
                .unwrap();
        let values =
            Arc::new(Int64Array::from(vec![Some(10), Some(20), None, Some(30)])) as ArrayRef;
        let batch = RecordBatch::try_from_iter([
            (
                "g",
                Arc::new(Int64Array::from(vec![Some(1), None, Some(1), None])) as ArrayRef,
            ),
            ("c", values.clone()),
            ("s", values),
        ])
        .unwrap();
        controllers.ingest(&batch, 1).unwrap();
        assert!(controllers.release_for_input().unwrap());
        assert!(!controllers.release_for_input().unwrap());
        assert_eq!(controllers.controllers.as_slice().len(), 4);
        assert!(controllers.merger.is_some());
        controllers.ingest(&batch, 1).unwrap();
        let mut seen = [false; 2];
        let stats = controllers
            .finish(|groups| {
                for row in 0..groups.len() {
                    let key = groups.key(row)?;
                    let is_null = key.bytes()[0] == 0;
                    let index = usize::from(is_null);
                    assert!(!seen[index]);
                    seen[index] = true;
                    if !is_null {
                        assert_eq!(i64::from_le_bytes(key.bytes()[1..9].try_into().unwrap()), 1);
                    }
                    assert_eq!(
                        *groups.value(row, 0)?,
                        ScalarValue::Int64(if is_null { 4 } else { 2 })
                    );
                    assert_eq!(
                        *groups.value(row, 1)?,
                        ScalarValue::Int64(if is_null { 100 } else { 20 })
                    );
                }
                Ok(())
            })
            .unwrap();
        assert!(seen.into_iter().all(|v| v));
        assert!(stats.spilled_bytes > 0);
        drop((batch, directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn peer_resident_state_cannot_starve_spill_compaction() {
        let parent = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("peer spill progress", 256 * 1024);
        let retained_input = pool.allocate(136 * 1024).unwrap();
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Sum, DataType::Decimal128(38, 2), false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let mut controllers =
            ParallelControllers::new(layout.clone(), directory.clone(), 8, 1024, 1872, 4).unwrap();
        assert_eq!(controllers.controllers.as_slice().len(), 4);
        let coefficient = (1i128 << 80) + 17;
        let batch = RecordBatch::try_from_iter(vec![
            (
                "key",
                Arc::new(Int64Array::from_iter_values(0..1000)) as ArrayRef,
            ),
            (
                "value",
                Arc::new(
                    Decimal128Array::from(vec![coefficient; 1000])
                        .with_precision_and_scale(38, 2)
                        .unwrap(),
                ) as ArrayRef,
            ),
        ])
        .unwrap();
        for _ in 0..20 {
            controllers.ingest(&batch, 1).unwrap();
        }
        drop(retained_input);
        let mut seen = vec![false; 1000];
        let stats = controllers
            .finish(|groups| {
                for row in 0..groups.len() {
                    let key = groups.key(row)?;
                    let index = i64::from_le_bytes(key.bytes()[1..9].try_into().unwrap()) as usize;
                    assert!(!seen[index]);
                    seen[index] = true;
                    assert_eq!(
                        *groups.value(row, 0)?,
                        ScalarValue::Decimal128(DecimalValue::new(coefficient * 20, 2))
                    );
                }
                Ok(())
            })
            .unwrap();
        assert!(seen.into_iter().all(|v| v));
        assert!(stats.flushes > 8 && stats.spilled_bytes > 0);
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn four_disjoint_workers_spill_exact_partial_states_without_final_value_merge() {
        let parent = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("parallel controller", 1048576);
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
        let mut controllers =
            ParallelControllers::new(layout.clone(), directory.clone(), 3, 1024, 16, 4).unwrap();
        assert_eq!(controllers.controllers.as_slice().len(), 4);
        let exact = (1i128 << 80) + 17;
        controllers.profile = true;
        for (weight, row_count) in [(1i128, 128usize), (2, 2048), (3, 128), (4, 2048)] {
            let batch = RecordBatch::try_from_iter(vec![
                (
                    "key",
                    Arc::new(Int64Array::from(
                        (0..row_count)
                            .map(|i| {
                                if i % 128 == 127 {
                                    None
                                } else {
                                    Some((i % 128) as i64)
                                }
                            })
                            .collect::<Vec<_>>(),
                    )) as ArrayRef,
                ),
                (
                    "count",
                    Arc::new(Int64Array::from(vec![1; row_count])) as ArrayRef,
                ),
                (
                    "sum",
                    Arc::new(
                        Decimal128Array::from(vec![exact * weight; row_count])
                            .with_precision_and_scale(38, 2)
                            .unwrap(),
                    ) as ArrayRef,
                ),
                (
                    "avg",
                    Arc::new(Float64Array::from(vec![weight as f64 * 10.0; row_count])) as ArrayRef,
                ),
            ])
            .unwrap();
            let held = pool.allocate(batch.get_array_memory_size()).unwrap();
            controllers.ingest(&batch, 1).unwrap();
            drop((batch, held));
        }
        assert_eq!(
            (controllers.serial_batches, controllers.parallel_batches),
            (2, 2)
        );
        let mut seen = [false; 128];
        let stats = controllers
            .finish(|groups| {
                for row in 0..groups.len() {
                    let key = groups.key(row)?;
                    let bytes = key.bytes();
                    let index = if bytes[0] == 0 {
                        127
                    } else {
                        i64::from_le_bytes(bytes[1..9].try_into().unwrap()) as usize
                    };
                    assert!(!seen[index]);
                    seen[index] = true;
                    assert_eq!(*groups.value(row, 0)?, ScalarValue::Int64(34));
                    assert_eq!(
                        *groups.value(row, 1)?,
                        ScalarValue::Decimal128(DecimalValue::new(exact * 100, 2))
                    );
                    assert_eq!(
                        *groups.value(row, 2)?,
                        ScalarValue::Float64((1000.0 / 34.0).into())
                    );
                }
                Ok(())
            })
            .unwrap();
        assert!(seen.into_iter().all(|v| v));
        assert!(stats.flushes > 4 && stats.spilled_bytes > 0);
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn balanced_workers_merge_hot_keys_and_adopt_local_spills() {
        for (workers, groups, rows, limit) in [
            (1, 4, 4096, usize::MAX),
            (4, 4, 4096, usize::MAX),
            (16, 4, 4096, usize::MAX),
            (4, 128, 2048, 256),
        ] {
            let parent = tempfile::tempdir().unwrap();
            let pool = MemoryPool::new_named("balanced partial workers", 16 * 1024 * 1024);
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
            let mut controllers = ParallelControllers::new_partial(
                layout.clone(),
                directory.clone(),
                8,
                1024,
                limit,
                workers,
            )
            .unwrap();
            assert_eq!(controllers.controllers.as_slice().len(), workers);
            assert_eq!(controllers.merger.is_some(), workers > 1);
            assert_eq!(controllers.parallel_merger.is_some(), workers > 1);
            controllers.profile = true;
            let exact = (1i128 << 80) + 17;
            for weight in 1..=4 {
                let batch = RecordBatch::try_from_iter(vec![
                    (
                        "g",
                        Arc::new(Int64Array::from(
                            (0..rows)
                                .map(|i| (i % groups != groups - 1).then_some((i % groups) as i64))
                                .collect::<Vec<_>>(),
                        )) as ArrayRef,
                    ),
                    ("c", Arc::new(Int64Array::from(vec![1; rows])) as ArrayRef),
                    (
                        "s",
                        Arc::new(
                            Decimal128Array::from(
                                (0..rows)
                                    .map(|i| (i % groups != groups - 2).then_some(exact * weight))
                                    .collect::<Vec<_>>(),
                            )
                            .with_precision_and_scale(38, 2)
                            .unwrap(),
                        ) as ArrayRef,
                    ),
                    (
                        "a",
                        Arc::new(Float64Array::from(
                            (0..rows)
                                .map(|i| (i % groups != groups - 2).then_some(weight as f64 * 10.0))
                                .collect::<Vec<_>>(),
                        )) as ArrayRef,
                    ),
                ])
                .unwrap();
                let held = pool.allocate(batch.get_array_memory_size()).unwrap();
                controllers.ingest(&batch, 1).unwrap();
                drop((batch, held));
            }
            if workers > 1 {
                assert_eq!(controllers.parallel_batches, 4);
            }
            let mut seen = vec![false; groups];
            let stats = controllers
                .finish(|state| {
                    for row in 0..state.len() {
                        let key = state.key(row)?;
                        let k = if key.bytes()[0] == 0 {
                            groups - 1
                        } else {
                            i64::from_le_bytes(key.bytes()[1..9].try_into().unwrap()) as usize
                        };
                        assert!(k < groups && !seen[k]);
                        seen[k] = true;
                        assert_eq!(
                            *state.value(row, 0)?,
                            ScalarValue::Int64((4 * rows / groups) as i64)
                        );
                        assert_eq!(
                            *state.value(row, 1)?,
                            if k == groups - 2 {
                                ScalarValue::Null
                            } else {
                                ScalarValue::Decimal128(DecimalValue::new(
                                    exact * 10 * (rows / groups) as i128,
                                    2,
                                ))
                            }
                        );
                        assert_eq!(
                            *state.value(row, 2)?,
                            if k == groups - 2 {
                                ScalarValue::Null
                            } else {
                                ScalarValue::Float64(25.0.into())
                            }
                        );
                    }
                    Ok(())
                })
                .unwrap();
            assert!(seen.into_iter().all(|v| v));
            assert_eq!(stats.spilled_bytes > 0, limit != usize::MAX);
            drop((directory, layout));
            assert_eq!(pool.used(), 0);
            assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
        }
    }

    #[test]
    fn partial_owners_merge_dictionary_values_across_codebooks() {
        use arrow::datatypes::Int32Type;
        let parent = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("partial dictionary", 4 * 1024 * 1024);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Utf8],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let mut controllers = ParallelControllers::new_partial(
            layout.clone(),
            directory.clone(),
            8,
            1024,
            usize::MAX,
            4,
        )
        .unwrap();
        for reversed in [false, true] {
            let values = if reversed {
                vec![Some("b"), Some("é"), None]
            } else {
                vec![Some("é"), Some("b"), None]
            };
            let codes = (0..4096)
                .map(|i| match i % 4 {
                    0 => Some(i32::from(reversed)),
                    1 => Some(i32::from(!reversed)),
                    2 => Some(2),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let keys = DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(codes),
                Arc::new(StringArray::from(values)),
            )
            .unwrap();
            let batch = RecordBatch::try_from_iter(vec![
                ("g", Arc::new(keys) as ArrayRef),
                ("c", Arc::new(Int64Array::from(vec![1; 4096])) as ArrayRef),
            ])
            .unwrap();
            let held = pool.allocate(batch.get_array_memory_size()).unwrap();
            controllers.ingest(&batch, 1).unwrap();
            drop((batch, held));
        }
        let mut seen = std::collections::BTreeMap::new();
        controllers
            .finish(|groups| {
                for row in 0..groups.len() {
                    let key = groups.key(row)?;
                    let label = if key.bytes()[0] == 0 {
                        "NULL"
                    } else {
                        std::str::from_utf8(&key.bytes()[9..]).unwrap()
                    };
                    assert!(seen
                        .insert(label.to_string(), groups.value(row, 0)?.into_owned())
                        .is_none());
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(
            seen,
            std::collections::BTreeMap::from([
                ("NULL".to_string(), ScalarValue::Int64(4096)),
                ("b".to_string(), ScalarValue::Int64(2048)),
                ("é".to_string(), ScalarValue::Int64(2048))
            ])
        );
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn partial_startup_fallback_releases_abandoned_merge_and_workers() {
        let parent = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("partial startup fallback", 1048576);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let baseline = pool.used();
        let mut seen = [false; 3];
        let mut merge_refusal_fallback = false;
        for headroom in (0..65536).step_by(512) {
            let held = pool.allocate(pool.available() - headroom).unwrap();
            let before = pool.used();
            let ordinary =
                ParallelControllers::new(layout.clone(), directory.clone(), 8, 1024, 64, 4)
                    .map(|c| c.controllers.as_slice().len())
                    .ok();
            assert_eq!(pool.used(), before);
            match ParallelControllers::new_partial(
                layout.clone(),
                directory.clone(),
                8,
                1024,
                64,
                4,
            ) {
                Err(error) => {
                    assert!(error.is_memory_limit());
                    seen[0] = true;
                }
                Ok(c) => {
                    let workers = c.controllers.as_slice().len();
                    if workers == 1 {
                        seen[1] = true;
                        assert!(c.merger.is_none());
                        merge_refusal_fallback |= ordinary == Some(4);
                    } else {
                        assert_eq!(workers, 4);
                        seen[2] = true;
                        assert!(c.merger.is_some());
                    }
                }
            }
            assert_eq!(pool.used(), before);
            drop(held);
            assert_eq!(pool.used(), baseline);
        }
        assert!(seen.into_iter().all(|v| v));
        assert!(
            merge_refusal_fallback,
            "extra merge admission must be covered separately from worker admission"
        );
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn empty_and_large_single_owner_batches_avoid_parallel_dispatch() {
        let parent = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("single active owner", 1048576);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let mut controllers =
            ParallelControllers::new(layout.clone(), directory.clone(), 3, 1024, 32, 4).unwrap();
        controllers.profile = true;
        let batch = RecordBatch::try_from_iter(vec![
            (
                "key",
                Arc::new(Int64Array::from(vec![None; 4096])) as ArrayRef,
            ),
            (
                "value",
                Arc::new(Int64Array::from(
                    (0..4096)
                        .map(|i| if i % 5 == 0 { None } else { Some(1) })
                        .collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
        ])
        .unwrap();
        let held = pool.allocate(batch.get_array_memory_size()).unwrap();
        controllers.ingest(&batch.slice(0, 0), 1).unwrap();
        controllers.ingest(&batch, 1).unwrap();
        assert_eq!(
            (controllers.serial_batches, controllers.parallel_batches),
            (2, 0)
        );
        let mut seen = 0;
        controllers
            .finish(|groups| {
                seen += groups.len();
                for row in 0..groups.len() {
                    assert_eq!(groups.key(row)?.bytes(), &[0]);
                    assert_eq!(*groups.value(row, 0)?, ScalarValue::Int64(3276));
                }
                Ok(())
            })
            .unwrap();
        assert_eq!(seen, 1);
        drop((batch, held, directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }

    #[test]
    fn startup_admission_can_choose_one_worker_and_cleans_abandoned_sets() {
        let parent = tempfile::tempdir().unwrap();
        let pool = MemoryPool::new_named("parallel startup", 1048576);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let directory = RunDirectory::create(layout.clone(), parent.path()).unwrap();
        let baseline = pool.used();
        let mut observed = [false; 3];
        for headroom in (0..32768).step_by(256) {
            let hold = pool.allocate(pool.available() - headroom).unwrap();
            let used = pool.used();
            match ParallelControllers::new(layout.clone(), directory.clone(), 3, 1024, 32, 4) {
                Ok(controllers) => match controllers.controllers.as_slice().len() {
                    1 => observed[1] = true,
                    4 => observed[2] = true,
                    n => panic!("unexpected worker count {n}"),
                },
                Err(error) => {
                    assert!(error.is_memory_limit(), "{error}");
                    observed[0] = true;
                }
            }
            assert_eq!(pool.used(), used);
            drop(hold);
            assert_eq!(pool.used(), baseline);
        }
        assert!(observed.into_iter().all(|v| v));
        drop((directory, layout));
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(parent.path()).unwrap().count(), 0);
    }
}

impl ParallelControllers {
    /// Admit the complete worker set before any aggregate rows are applied.
    /// The caller may already own a prepared input and its first batch. Only a
    /// startup admission denial selects fewer workers; input is never replayed.
    pub(super) fn new(
        layout: Arc<GroupLayout>,
        directory: Arc<RunDirectory>,
        max_runs: usize,
        frame_capacity: usize,
        group_limit: usize,
        workers: usize,
    ) -> Result<Self> {
        let create = |workers: usize| -> Result<ReservedVec<IngestionController>> {
            let mut controllers = ReservedVec::with_capacity(layout.pool(), workers)?;
            controllers.try_extend_reserved(
                workers,
                (0..workers).map(|_| {
                    IngestionController::new(
                        layout.clone(),
                        directory.clone(),
                        max_runs,
                        frame_capacity,
                        group_limit.div_ceil(workers).max(1),
                    )
                }),
            )?;
            Ok(controllers)
        };
        let workers = workers.max(1);
        let controllers = match create(workers) {
            Err(error) if error.is_memory_limit() && workers > 1 => create(1)?,
            result => result?,
        };
        Ok(Self {
            controllers,
            layout,
            merger: None,
            parallel_merger: None,
            ingestion_started: false,
            group_limit: group_limit.max(1),
            poisoned: false,
            profile: std::env::var_os("QE_AGG_PROF").is_some(),
            routing_time: Duration::ZERO,
            processing_time: Duration::ZERO,
            serial_batches: 0,
            parallel_batches: 0,
        })
    }

    /// Reserve worker state and final merge before applying aggregate rows. An
    /// admission denial can select one ordinary owner, never replay input.
    pub(super) fn new_partial(
        layout: Arc<GroupLayout>,
        directory: Arc<RunDirectory>,
        max_runs: usize,
        frame_capacity: usize,
        group_limit: usize,
        workers: usize,
    ) -> Result<Self> {
        let attempt: Result<Self> = (|| {
            let mut result = Self::new(
                layout.clone(),
                directory.clone(),
                max_runs,
                frame_capacity,
                group_limit,
                workers,
            )?;
            let count = result.controllers.as_slice().len();
            if count > 1 {
                let mut merge = PartialMerge::new(
                    layout.clone(),
                    directory.clone(),
                    frame_capacity,
                    group_limit,
                )?;
                merge.reserve_input_runs(count.checked_mul(max_runs).ok_or_else(|| {
                    QueryError::Execution("partial run ledger overflow".into())
                })?)?;
                result.merger = Some(merge);
                result.parallel_merger = ParallelMerge::try_new(
                    layout.clone(),
                    directory.clone(),
                    frame_capacity,
                    group_limit,
                    count,
                )?;
            }
            Ok(result)
        })();
        match attempt {
            Err(error) if error.is_memory_limit() => {
                Self::new(layout, directory, max_runs, frame_capacity, group_limit, 1)
            }
            result => result,
        }
    }

    pub(super) fn release_for_input(&mut self) -> Result<bool> {
        if self.poisoned {
            return Err(QueryError::Execution(
                "parallel aggregate previously failed".into(),
            ));
        }
        if !self.ingestion_started && self.controllers.as_slice().len() > 1 {
            // The caller still owns the same incoming batch and prepared input.
            // Reuse the first admitted worker; rebuilding would allocate while
            // memory is exhausted and must never reopen a consuming source.
            self.controllers.as_mut_slice()[0].restore_unused_group_limit(self.group_limit)?;
            drop(self.parallel_merger.take());
            drop(self.merger.take());
            self.controllers.truncate(1);
            return Ok(true);
        }
        let mut released = false;
        for controller in self.controllers.as_mut_slice() {
            match controller.release_for_input() {
                Ok(value) => released |= value,
                Err(error) => {
                    self.poisoned = true;
                    return Err(error);
                }
            }
        }
        Ok(released)
    }

    fn route(
        &self,
        batch: &RecordBatch,
        group_count: usize,
        prepared: Option<&super::prepared_keys::PreparedKeys<'_>>,
    ) -> Result<ReservedVec<ReservedVec<usize>>> {
        if self.merger.is_some() {
            row_router::route_balanced(
                &self.layout,
                batch,
                group_count,
                self.controllers.as_slice().len(),
            )
        } else {
            row_router::route_prepared(
                &self.layout,
                batch,
                group_count,
                self.controllers.as_slice().len(),
                prepared,
            )
        }
    }

    pub(super) fn ingest(&mut self, batch: &RecordBatch, group_count: usize) -> Result<()> {
        if self.poisoned {
            return Err(QueryError::Execution(
                "parallel aggregate previously failed".into(),
            ));
        }
        let result = (|| {
            if self.controllers.as_slice().len() == 1 {
                self.ingestion_started |= batch.num_rows() != 0;
                return self.controllers.as_mut_slice()[0].ingest(batch, group_count);
            }
            let routing_start = self.profile.then(Instant::now);
            let prepared =
                super::prepared_keys::PreparedKeys::try_new(&self.layout, batch, group_count)?;
            let routes = match self.route(batch, group_count, prepared.as_ref()) {
                Err(error) if error.is_memory_limit() => {
                    if !self.release_for_input()? {
                        return Err(error);
                    }
                    self.route(batch, group_count, prepared.as_ref())?
                }
                result => result?,
            };
            // Routing has not applied any rows. Until it succeeds, startup
            // admission may still reclaim unused owners using these same arrays.
            // Once workers can apply rows, the ownership map is irreversible.
            self.ingestion_started |= batch.num_rows() != 0;
            if let Some(start) = routing_start {
                self.routing_time += start.elapsed();
            }
            let processing_start = self.profile.then(Instant::now);
            // Rayon returns only after all scoped tasks have stopped, including
            // on error. Batch, indices and controller owners stay borrowed here.
            // Measured small batches spend more time dispatching than updating
            // states. This is a scheduling choice only: the selected ownership policy never
            // changes between serial and parallel dispatch. Keep a provisional
            // minimum of 256 rows per active owner before paying Rayon overhead.
            let active = routes
                .as_slice()
                .iter()
                .filter(|r| !r.as_slice().is_empty())
                .count();
            let parallel = active > 1 && batch.num_rows() / active >= 256;
            let mut progress =
                ReservedVec::with_capacity(self.layout.pool(), self.controllers.as_slice().len())?;
            progress.extend_reserved(
                self.controllers.as_slice().len(),
                std::iter::repeat_with(super::ingestion_controller::IngestionProgress::default),
            )?;
            if self.profile {
                if parallel {
                    self.parallel_batches += 1;
                } else {
                    self.serial_batches += 1;
                }
            }
            let result = (|| {
                loop {
                    let apply = |((controller, rows), cursor): (
                        (&mut IngestionController, &ReservedVec<usize>),
                        &mut super::ingestion_controller::IngestionProgress,
                    )|
                     -> Result<()> {
                        if cursor.done {
                            return Ok(());
                        }
                        *cursor = controller.ingest_prepared_step(
                            batch,
                            group_count,
                            rows.as_slice(),
                            prepared.as_ref(),
                            cursor.next_row,
                        )?;
                        Ok(())
                    };
                    if parallel {
                        self.controllers
                            .as_mut_slice()
                            .par_iter_mut()
                            .zip(routes.as_slice().par_iter())
                            .zip(progress.as_mut_slice().par_iter_mut())
                            .try_for_each(apply)?;
                    } else {
                        self.controllers
                            .as_mut_slice()
                            .iter_mut()
                            .zip(routes.as_slice())
                            .zip(progress.as_mut_slice())
                            .try_for_each(apply)?;
                    }
                    if progress.as_slice().iter().all(|p| p.done) {
                        break;
                    }
                    if !self
                        .controllers
                        .as_slice()
                        .iter()
                        .any(IngestionController::has_resident_groups)
                    {
                        return Err(progress
                            .as_mut_slice()
                            .iter_mut()
                            .find_map(|p| p.pressure.take())
                            .unwrap_or_else(|| {
                                QueryError::Execution(
                                    "aggregate ingestion made no spill progress".into(),
                                )
                            }));
                    }
                    // Join every worker before flushing peers. No worker starts
                    // compaction while another still owns a populated resident.
                    for controller in self.controllers.as_mut_slice() {
                        controller.park_for_spill()?;
                    }
                    for controller in self.controllers.as_mut_slice() {
                        controller.prepare_parked_writer()?;
                    }
                    for controller in self.controllers.as_mut_slice() {
                        controller.restore_parked_resident()?;
                    }
                }
                Ok(())
            })();
            if let Some(start) = processing_start {
                self.processing_time += start.elapsed();
            }
            result
        })();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    pub(super) fn finish(
        self,
        mut output: impl FnMut(&GroupRows) -> Result<()>,
    ) -> Result<IngestionStats> {
        if self.poisoned {
            return Err(QueryError::Execution(
                "cannot finalize failed parallel aggregate".into(),
            ));
        }
        if self.profile {
            eprintln!(
                "live_aggregate_workers workers={} ownership={} routing_ms={:.3} processing_wall_ms={:.3} serial_batches={} parallel_batches={}",
                self.controllers.as_slice().len(),
                if self.merger.is_some() { "partial" } else { "disjoint" },
                self.routing_time.as_secs_f64() * 1000.0,
                self.processing_time.as_secs_f64() * 1000.0,
                self.serial_batches,
                self.parallel_batches
            );
        }
        let mut total = IngestionStats::default();
        let mut add = |stats: IngestionStats| -> Result<()> {
            macro_rules! sum {
                ($field:ident) => {
                    total.$field = total.$field.checked_add(stats.$field).ok_or_else(|| {
                        QueryError::Execution("parallel aggregate metric overflow".into())
                    })?;
                };
            }
            sum!(flushes);
            sum!(spilled_rows);
            sum!(spilled_bytes);
            sum!(merge_splits);
            Ok(())
        };
        let local_spill = self
            .controllers
            .as_slice()
            .iter()
            .any(IngestionController::has_spilled_runs);
        let mut parallel_merger = self.parallel_merger;
        let mut merger = self.merger;
        if local_spill {
            // Drop optional reducers before decoding local spill runs. The serial
            // consumer and its adoption ledger were admitted before input opened.
            drop(parallel_merger.take());
        } else if parallel_merger.is_some() {
            drop(merger.take());
        }
        if self.profile && parallel_merger.is_none() && merger.is_some() {
            eprintln!(
                "live_aggregate_reduction method=serial reason={}",
                if local_spill {
                    "local_spill"
                } else {
                    "startup_admission"
                }
            );
        }
        for controller in self.controllers.into_owned_iter() {
            let stats = if let Some(merge) = &mut parallel_merger {
                let (groups, runs, stats) = controller.into_partials()?;
                if !runs.runs().is_empty() {
                    return Err(QueryError::Execution(
                        "resident reduction received local spill runs".into(),
                    ));
                }
                drop(runs);
                if let Some(groups) = groups {
                    merge.ingest(&groups)?;
                }
                stats
            } else if let Some(merge) = &mut merger {
                let (groups, runs, stats) = controller.into_partials()?;
                if let Some(groups) = groups {
                    merge.ingest(&groups)?;
                }
                merge.adopt_runs(runs)?;
                stats
            } else {
                controller.finish(&mut output)?
            };
            add(stats)?;
        }
        if let Some(merge) = parallel_merger {
            add(merge.finish(&mut output)?)?;
        }
        if let Some(merge) = merger {
            add(merge.finish(output)?)?;
        }
        Ok(total)
    }
}
