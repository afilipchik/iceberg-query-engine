//! Canonical keys and hashes bound to the exact retained evaluated batch.
use super::{
    group_rows::GroupLayout,
    key_rows::{KeyRef, KeyRows, KeyWorkspace},
};
use crate::{
    execution::{reserved_vec::ReservedVec, MemoryPool},
    QueryError, Result,
};
use arrow::record_batch::RecordBatch;
use rayon::prelude::*;

pub(super) struct PreparedKeys<'a> {
    batch: &'a RecordBatch,
    groups: usize,
    layout: uuid::Uuid,
    chunk_rows: usize,
    chunks: ReservedVec<Option<KeyChunk>>,
}

struct KeyChunk {
    keys: KeyRows,
    hashes: ReservedVec<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::AggregateFunction;
    use arrow::{array::*, datatypes::*};
    use std::sync::Arc;

    #[test]
    fn parallel_chunks_preserve_every_row_and_release_on_pressure() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap()
            .install(|| {
                let pool = MemoryPool::new_named("parallel prepared keys", 8 * 1024 * 1024);
                let layout = GroupLayout::bind(
                    &pool,
                    &[DataType::Int64],
                    &[(AggregateFunction::Count, DataType::Int64, false)],
                )
                .unwrap()
                .unwrap();
                let batch = RecordBatch::try_from_iter(vec![(
                    "k",
                    Arc::new(Int64Array::from_iter(
                        (0..8193).map(|i| (i % 17 != 0).then_some(i as i64 % 137)),
                    )) as ArrayRef,
                )])
                .unwrap();
                let source = pool.allocate(batch.get_array_memory_size()).unwrap();
                let baseline = pool.used();
                let prepared = PreparedKeys::try_new(&layout, &batch, 1).unwrap().unwrap();
                assert_eq!(prepared.chunks.as_slice().len(), 4);
                for row in 0..batch.num_rows() {
                    let mut expected = if row % 17 == 0 { vec![0] } else { vec![1] };
                    if row % 17 != 0 {
                        expected.extend((row as i64 % 137).to_le_bytes());
                    }
                    let (key, hash) = prepared.key(row).unwrap();
                    assert_eq!(key.bytes(), expected);
                    assert_eq!(hash, key.hash64());
                }
                drop(prepared);
                assert_eq!(pool.used(), baseline);
                // Permit partial chunk construction before the shared child denies
                // growth. Scoped workers must release everything before fallback.
                let pressure = pool.allocate(pool.available() - 256 * 1024).unwrap();
                let pressured = pool.used();
                assert!(PreparedKeys::try_new(&layout, &batch, 1).unwrap().is_none());
                assert_eq!(pool.used(), pressured);
                drop((pressure, source, batch, layout));
                assert_eq!(pool.used(), 0);
            });
    }

    #[test]
    fn exact_identity_empty_input_and_admission_fallback_release_owners() {
        let pool = MemoryPool::new_named("prepared key test", 1048576);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let batch = RecordBatch::try_from_iter(vec![(
            "k",
            Arc::new(Int64Array::from(vec![Some(7), None, Some(7)])) as ArrayRef,
        )])
        .unwrap();
        let held = pool.allocate(batch.get_array_memory_size()).unwrap();
        let baseline = pool.used();
        let prepared = PreparedKeys::try_new(&layout, &batch, 1).unwrap().unwrap();
        let mut expected = vec![1];
        expected.extend(7i64.to_le_bytes());
        assert_eq!(prepared.key(0).unwrap().0.bytes(), expected);
        assert_eq!(prepared.key(1).unwrap().0.bytes(), &[0]);
        assert!(prepared.key(0).unwrap() == prepared.key(2).unwrap());
        assert!(prepared.key(3).is_err());
        assert!(prepared.validate(&layout, &batch.clone(), 1).is_err());
        let other = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        assert!(prepared.validate(&other, &batch, 1).is_err());
        drop((other, prepared));
        assert_eq!(pool.used(), baseline);
        let pressure = pool.allocate(pool.available() - 1024).unwrap();
        let pressured = pool.used();
        assert!(PreparedKeys::try_new(&layout, &batch, 1).unwrap().is_none());
        assert_eq!(pool.used(), pressured);
        drop(pressure);
        let empty = batch.slice(0, 0);
        let prepared = PreparedKeys::try_new(&layout, &empty, 1).unwrap().unwrap();
        assert!(prepared.key(0).is_err());
        drop(prepared);
        drop((empty, held, batch, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn dictionary_codebooks_nulls_and_float_keys_keep_canonical_owners() {
        let pool = MemoryPool::new_named("prepared logical keys", 1048576);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Utf8, DataType::Float64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let mut expected: Vec<(Vec<u8>, u64)> = Vec::new();
        for reversed in [false, true] {
            let values = Arc::new(StringArray::from(if reversed {
                vec![Some("b"), Some("a"), None]
            } else {
                vec![Some("a"), Some("b"), None]
            }));
            let codes = Int32Array::from(if reversed {
                vec![Some(1), Some(0), None, Some(2), Some(1)]
            } else {
                vec![Some(0), Some(1), None, Some(2), Some(0)]
            });
            let strings = DictionaryArray::<Int32Type>::try_new(codes, values).unwrap();
            let batch = RecordBatch::try_from_iter(vec![
                ("s", Arc::new(strings) as ArrayRef),
                (
                    "f",
                    Arc::new(Float64Array::from(vec![
                        0.0,
                        1.0,
                        f64::NAN,
                        f64::from_bits(0x7ff8_0000_0000_0011),
                        -0.0,
                    ])) as ArrayRef,
                ),
            ])
            .unwrap();
            let held = pool.allocate(batch.get_array_memory_size()).unwrap();
            let prepared = PreparedKeys::try_new(&layout, &batch, 2).unwrap().unwrap();
            assert!(prepared.key(0).unwrap() == prepared.key(4).unwrap());
            assert!(prepared.key(2).unwrap() == prepared.key(3).unwrap());
            for row in 0..batch.num_rows() {
                let (key, hash) = prepared.key(row).unwrap();
                if reversed {
                    assert_eq!(
                        (key.bytes(), hash),
                        (expected[row].0.as_slice(), expected[row].1)
                    );
                } else {
                    expected.push((key.bytes().to_vec(), hash));
                }
            }
            let ordinary = super::super::row_router::route(&layout, &batch, 2, 4).unwrap();
            let reused =
                super::super::row_router::route_prepared(&layout, &batch, 2, 4, Some(&prepared))
                    .unwrap();
            for (a, b) in ordinary.as_slice().iter().zip(reused.as_slice()) {
                assert_eq!(a.as_slice(), b.as_slice());
            }
            drop((ordinary, reused, prepared));
            drop((held, batch));
        }
        drop(layout);
        assert_eq!(pool.used(), 0);
    }
}

impl<'a> PreparedKeys<'a> {
    /// Optional optimization, completed before routing or aggregate mutation.
    /// Only admission denial declines it. Arrays are already evaluated; fallback
    /// never reruns expressions or a source. Cap scratch at one eighth of current
    /// available memory, leaving headroom for routing and state/spill work.
    pub(super) fn try_new(
        layout: &GroupLayout,
        batch: &'a RecordBatch,
        groups: usize,
    ) -> Result<Option<Self>> {
        if groups == 0 || groups > batch.num_columns() {
            return Err(QueryError::Execution("prepared key arity mismatch".into()));
        }
        layout.validate_key_arrays(&batch.columns()[..groups])?;
        let pool = MemoryPool::new_child(
            layout.pool(),
            "prepared aggregate keys",
            layout.pool().available() / 8,
        );
        let create = || -> Result<Self> {
            // Scheduling only: every chunk shares the same admitted child
            // budget and uses the unchanged canonical encoder. Small batches
            // stay serial. No worker outlives this scoped parallel iterator.
            let workers = rayon::current_num_threads()
                .min(4)
                .min((batch.num_rows() / 1024).max(1));
            let chunk_rows = batch.num_rows().div_ceil(workers).max(1);
            let count = batch.num_rows().div_ceil(chunk_rows).max(1);
            let mut chunks = ReservedVec::with_capacity(&pool, count)?;
            chunks.extend_reserved(count, (0..count).map(|_| None))?;
            let prepare = |(index, slot): (usize, &mut Option<KeyChunk>)| -> Result<()> {
                let start = index * chunk_rows;
                let end = start.saturating_add(chunk_rows).min(batch.num_rows());
                let mut keys = KeyRows::new_in_pool(layout.key_layout().clone(), &pool)?;
                let mut hashes = ReservedVec::with_capacity(&pool, end - start)?;
                let mut workspace = KeyWorkspace::new_in_pool(layout.key_layout().clone(), &pool)?;
                let bound = super::key_rows::bound_arrays::BoundKeyArrays::bind(
                    layout.key_layout().clone(),
                    &batch.columns()[..groups],
                    batch.num_rows(),
                    &pool,
                )?;
                for row in start..end {
                    bound.encode(&mut workspace, row)?;
                    let hash = workspace.key()?.hash64();
                    keys.append(&workspace)?;
                    hashes.extend_reserved(1, std::iter::once(hash))?;
                }
                *slot = Some(KeyChunk { keys, hashes });
                Ok(())
            };
            if count > 1 {
                chunks
                    .as_mut_slice()
                    .par_iter_mut()
                    .enumerate()
                    .try_for_each(prepare)?;
            } else {
                chunks
                    .as_mut_slice()
                    .iter_mut()
                    .enumerate()
                    .try_for_each(prepare)?;
            }
            Ok(Self {
                batch,
                groups,
                layout: layout.identity(),
                chunk_rows,
                chunks,
            })
        };
        match create() {
            Ok(prepared) => Ok(Some(prepared)),
            Err(error) if error.is_memory_limit() => Ok(None),
            Err(error) => Err(error),
        }
    }

    pub(super) fn validate(
        &self,
        layout: &GroupLayout,
        batch: &RecordBatch,
        groups: usize,
    ) -> Result<()> {
        if self.layout != layout.identity()
            || !std::ptr::eq(self.batch, batch)
            || self.groups != groups
        {
            return Err(QueryError::Execution(
                "prepared key batch/layout mismatch".into(),
            ));
        }
        Ok(())
    }

    pub(super) fn key(&self, row: usize) -> Result<(KeyRef<'_>, u64)> {
        if row >= self.batch.num_rows() {
            return Err(QueryError::Execution(
                "prepared key row out of bounds".into(),
            ));
        }
        let (index, local) = if self.chunks.as_slice().len() == 1 {
            (0, row)
        } else {
            (row / self.chunk_rows, row % self.chunk_rows)
        };
        let chunk = self
            .chunks
            .as_slice()
            .get(index)
            .and_then(Option::as_ref)
            .ok_or_else(|| QueryError::Execution("incomplete prepared key chunk".into()))?;
        let hash = *chunk
            .hashes
            .as_slice()
            .get(local)
            .ok_or_else(|| QueryError::Execution("prepared key row out of bounds".into()))?;
        Ok((chunk.keys.key(local)?, hash))
    }
}
