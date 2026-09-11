//! Query-admitted canonical-key routing. No rows are dispatched until complete.
use super::group_rows::GroupLayout;
use crate::{execution::reserved_vec::ReservedVec, QueryError, Result};
use arrow::record_batch::RecordBatch;

/// Preserve physical input order within each owner. Hash equality selects an
/// owner only; the worker still checks complete canonical group-key equality.
/// The caller retains the evaluated batch while these borrowed-row indices live.
pub(super) fn route(
    layout: &GroupLayout,
    batch: &RecordBatch,
    group_count: usize,
    workers: usize,
) -> Result<ReservedVec<ReservedVec<usize>>> {
    route_prepared(layout, batch, group_count, workers, None)
}

pub(super) fn route_prepared(
    layout: &GroupLayout,
    batch: &RecordBatch,
    group_count: usize,
    workers: usize,
    prepared: Option<&super::prepared_keys::PreparedKeys<'_>>,
) -> Result<ReservedVec<ReservedVec<usize>>> {
    if workers == 0 || group_count == 0 || group_count > batch.num_columns() {
        return Err(QueryError::Execution(
            "invalid grouped row routing dimensions".into(),
        ));
    }
    let keys = &batch.columns()[..group_count];
    layout.validate_key_arrays(keys)?;
    if let Some(prepared) = prepared {
        prepared.validate(layout, batch, group_count)?;
    }
    let mut routes = ReservedVec::with_capacity(layout.pool(), workers)?;
    routes.try_extend_reserved(
        workers,
        (0..workers).map(|_| ReservedVec::with_capacity(layout.pool(), 0)),
    )?;
    let mut key = layout.key_workspace()?;
    for row in 0..batch.num_rows() {
        let hash = if let Some(prepared) = prepared {
            prepared.key(row)?.1
        } else {
            key.encode_arrays(keys, row)?;
            key.key()?.hash64()
        };
        let owner = (hash % workers as u64) as usize;
        let rows = &mut routes.as_mut_slice()[owner];
        rows.reserve(1)?;
        rows.extend_reserved(1, std::iter::once(row))?;
    }
    Ok(routes)
}

/// Balanced contiguous row ranges preserve each worker's physical row order.
/// Equal keys may have several owners: caller MUST merge their partial states.
pub(super) fn route_balanced(
    layout: &GroupLayout,
    batch: &RecordBatch,
    group_count: usize,
    workers: usize,
) -> Result<ReservedVec<ReservedVec<usize>>> {
    if workers == 0 || group_count == 0 || group_count > batch.num_columns() {
        return Err(QueryError::Execution(
            "invalid balanced routing dimensions".into(),
        ));
    }
    layout.validate_key_arrays(&batch.columns()[..group_count])?;
    let mut routes = ReservedVec::with_capacity(layout.pool(), workers)?;
    let quotient = batch.num_rows() / workers;
    let remainder = batch.num_rows() % workers;
    let mut start = 0;
    for owner in 0..workers {
        let len = quotient + usize::from(owner < remainder);
        let mut rows = ReservedVec::with_capacity(layout.pool(), len)?;
        rows.extend_reserved(len, start..start + len)?;
        start += len;
        routes.extend_reserved(1, std::iter::once(rows))?;
    }
    Ok(routes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{execution::MemoryPool, planner::AggregateFunction};
    use arrow::{array::*, datatypes::*};
    use std::sync::Arc;

    #[test]
    fn equal_logical_keys_route_together_across_dictionary_codebooks() {
        let pool = MemoryPool::new_named("row routing", 65536);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Utf8],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let mut owners = std::collections::HashMap::new();
        for reversed in [false, true] {
            let values = if reversed {
                vec![Some("b"), Some("é\0"), None]
            } else {
                vec![Some("é\0"), Some("b"), None]
            };
            let keys = if reversed {
                vec![Some(1), Some(0), Some(1), None, Some(2)]
            } else {
                vec![Some(0), Some(1), Some(0), None, Some(2)]
            };
            let array = Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(keys),
                    Arc::new(StringArray::from(values)),
                )
                .unwrap(),
            ) as ArrayRef;
            let batch = RecordBatch::try_from_iter(vec![("key", array)]).unwrap();
            for workers in [1, 2, 4] {
                let routed = route(&layout, &batch, 1, workers).unwrap();
                let mut seen = vec![false; 5];
                for (owner, rows) in routed.as_slice().iter().enumerate() {
                    assert!(rows.as_slice().windows(2).all(|w| w[0] < w[1]));
                    for &row in rows.as_slice() {
                        assert!(!seen[row]);
                        seen[row] = true;
                        let logical = match row {
                            0 | 2 => Some("é\0"),
                            1 => Some("b"),
                            _ => None,
                        };
                        assert_eq!(*owners.entry((workers, logical)).or_insert(owner), owner);
                    }
                }
                assert!(seen.into_iter().all(|v| v));
            }
        }
        drop(layout);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn canonical_float_ownership_and_refusal_cleanup() {
        let pool = MemoryPool::new_named("float routing", 65536);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Float64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "key",
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(-0.0),
                Some(f64::NAN),
                Some(f64::from_bits(0x7ff0000000000001)),
                None,
            ])) as ArrayRef,
        )])
        .unwrap();
        let before = pool.used();
        let mut denied = 0;
        let mut admitted = 0;
        for headroom in (0..8192).step_by(128) {
            let hold = pool.allocate(pool.available() - headroom).unwrap();
            let held = pool.used();
            match route(&layout, &batch, 1, 4) {
                Ok(routes) => {
                    admitted += 1;
                    let mut owners = [usize::MAX; 5];
                    for (owner, rows) in routes.as_slice().iter().enumerate() {
                        for &row in rows.as_slice() {
                            owners[row] = owner;
                        }
                    }
                    assert_eq!(owners[0], owners[1]);
                    assert_eq!(owners[2], owners[3]);
                    assert!(owners.into_iter().all(|v| v < 4));
                }
                Err(error) => {
                    assert!(error.is_memory_limit(), "{error}");
                    denied += 1;
                }
            }
            assert_eq!(pool.used(), held);
            drop(hold);
            assert_eq!(pool.used(), before);
        }
        assert!(denied > 0 && admitted > 0);
        assert!(route(&layout, &batch, 1, 0).is_err());
        let empty = batch.slice(0, 0);
        assert!(route(&layout, &empty, 1, 4)
            .unwrap()
            .as_slice()
            .iter()
            .all(|r| r.as_slice().is_empty()));
        drop(layout);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn balanced_ranges_cover_all_rows_and_refuse_atomically() {
        let pool = MemoryPool::new_named("balanced ranges", 1048576);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        for count in [0, 1, 3, 2049] {
            let batch = RecordBatch::try_from_iter(vec![(
                "g",
                Arc::new(Int64Array::from(vec![Some(7); count])) as ArrayRef,
            )])
            .unwrap();
            for workers in [1, 4, 16] {
                let baseline = pool.used();
                let routes = route_balanced(&layout, &batch, 1, workers).unwrap();
                let lengths = routes
                    .as_slice()
                    .iter()
                    .map(|r| r.as_slice().len())
                    .collect::<Vec<_>>();
                assert!(lengths.iter().max().unwrap() - lengths.iter().min().unwrap() <= 1);
                let rows = routes
                    .as_slice()
                    .iter()
                    .flat_map(|r| r.as_slice().iter().copied())
                    .collect::<Vec<_>>();
                assert_eq!(rows, (0..count).collect::<Vec<_>>());
                drop(routes);
                assert_eq!(pool.used(), baseline);
                let held = pool.allocate(pool.available()).unwrap();
                assert!(route_balanced(&layout, &batch, 1, workers)
                    .err()
                    .unwrap()
                    .is_memory_limit());
                drop(held);
                assert_eq!(pool.used(), baseline);
            }
            assert!(route_balanced(&layout, &batch, 1, 0).is_err());
        }
        drop(layout);
        assert_eq!(pool.used(), 0);
    }
}
