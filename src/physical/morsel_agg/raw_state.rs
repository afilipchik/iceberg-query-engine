//! Raw-key aggregate rows: one key/index table and contiguous fixed-arity states.
//! Reservations cover outer storage, not heap allocations inside an accumulator.
use super::AccumulatorState;
use crate::error::{QueryError, Result};
use crate::execution::reserved_vec::ReservedVec;
use crate::execution::{MemoryPool, MemoryReservation};
use hashbrown::HashMap;

fn overflow() -> QueryError {
    QueryError::Execution("aggregate state arena size overflow".into())
}

/// Shards retain flat storage; they never allocate an accumulator Vec per key.
pub(super) struct RawRows {
    keys: Option<ReservedVec<u64>>,
    states: Option<ReservedVec<AccumulatorState>>,
    arity: usize,
    pool: MemoryPool,
}

impl RawRows {
    pub(super) fn new(pool: &MemoryPool, arity: usize) -> Self {
        Self {
            keys: None,
            states: None,
            arity,
            pool: pool.clone(),
        }
    }

    pub(super) fn len(&self) -> usize {
        self.keys.as_ref().map_or(0, |keys| keys.as_slice().len())
    }

    fn reserve(&mut self, additional: usize) -> Result<()> {
        let values = additional.checked_mul(self.arity).ok_or_else(overflow)?;
        if self.keys.is_none() {
            self.keys = Some(ReservedVec::with_capacity(&self.pool, additional)?);
        } else {
            self.keys.as_mut().unwrap().reserve(additional)?;
        }
        if self.states.is_none() {
            self.states = Some(ReservedVec::with_capacity(&self.pool, values)?);
        } else {
            self.states.as_mut().unwrap().reserve(values)?;
        }
        Ok(())
    }

    pub(super) fn push(
        &mut self,
        key: u64,
        values: impl IntoIterator<Item = AccumulatorState>,
    ) -> Result<usize> {
        let index = self.len();
        self.reserve(1)?;
        self.states
            .as_mut()
            .unwrap()
            .extend_reserved(self.arity, values)?;
        self.keys.as_mut().unwrap().extend_reserved(1, [key])?;
        Ok(index)
    }

    pub(super) fn row_mut(&mut self, index: usize) -> &mut [AccumulatorState] {
        let start = index * self.arity;
        &mut self.states.as_mut().unwrap().as_mut_slice()[start..start + self.arity]
    }

    pub(super) fn iter(
        &self,
    ) -> impl ExactSizeIterator<Item = (&u64, &[AccumulatorState])> + Clone {
        let keys = self.keys.as_ref().map_or(&[][..], |v| v.as_slice());
        let states = self.states.as_ref().map_or(&[][..], |v| v.as_slice());
        keys.iter().enumerate().map(move |(i, key)| {
            let start = i * self.arity;
            (key, &states[start..start + self.arity])
        })
    }

    /// Pre-size every shard before moving any row. Old payload leases remain
    /// attached to consuming iterators until their storage is destroyed.
    pub(super) fn shard(mut self, p: usize, route: impl Fn(u64) -> usize) -> Result<Vec<Self>> {
        if p == 0 {
            return Err(QueryError::Execution(
                "aggregate requires nonzero shards".into(),
            ));
        }
        let mut counts = vec![0usize; p];
        for (key, _) in self.iter() {
            let index = route(*key);
            let count = counts
                .get_mut(index)
                .ok_or_else(|| QueryError::Execution("aggregate shard out of range".into()))?;
            *count = count.checked_add(1).ok_or_else(overflow)?;
        }
        let mut shards = (0..p)
            .map(|_| Self::new(&self.pool, self.arity))
            .collect::<Vec<_>>();
        for (shard, count) in shards.iter_mut().zip(counts) {
            if count > 0 {
                shard.reserve(count)?;
            }
        }
        if let Some(keys) = self.keys.take() {
            let mut values = self.states.take().unwrap().into_owned_iter();
            for key in keys.into_owned_iter() {
                shards[route(key)].push(key, values.by_ref().take(self.arity))?;
            }
        }
        Ok(shards)
    }
}

pub(super) struct RawStateMap {
    index: HashMap<u64, usize>,
    rows: RawRows,
    // Free hash metadata before releasing its accounting owner.
    index_reservation: Option<MemoryReservation>,
}

/// Upper bound for the pinned hashbrown 0.17 table layout: 7/8 load,
/// power-of-two buckets, data alignment padding and trailing control bytes.
/// 64 covers the control group and alignment on supported CPU targets.
pub(super) fn index_bound(capacity: usize) -> Result<usize> {
    let buckets = if capacity < 4 {
        4
    } else if capacity < 8 {
        8
    } else {
        capacity
            .checked_mul(8)
            .ok_or_else(overflow)?
            .checked_div(7)
            .unwrap()
            .checked_next_power_of_two()
            .ok_or_else(overflow)?
    };
    let bytes = buckets
        .checked_mul(std::mem::size_of::<(u64, usize)>() + 1)
        .and_then(|v| v.checked_add(64))
        .ok_or_else(overflow)?;
    if bytes > isize::MAX as usize {
        return Err(overflow());
    }
    Ok(bytes)
}

impl RawStateMap {
    pub(super) fn new(pool: &MemoryPool, arity: usize) -> Self {
        Self {
            index: HashMap::new(),
            rows: RawRows::new(pool, arity),
            index_reservation: None,
        }
    }
    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }
    pub(super) fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub(super) fn iter(
        &self,
    ) -> impl ExactSizeIterator<Item = (&u64, &[AccumulatorState])> + Clone {
        self.rows.iter()
    }
    pub(super) fn keys(&self) -> impl Iterator<Item = &u64> {
        self.iter().map(|(k, _)| k)
    }
    pub(super) fn values(&self) -> impl Iterator<Item = &[AccumulatorState]> {
        self.iter().map(|(_, v)| v)
    }

    #[cfg(test)]
    pub(super) fn get_mut(&mut self, key: &u64) -> Option<&mut [AccumulatorState]> {
        self.index.get(key).copied().map(|i| self.rows.row_mut(i))
    }

    fn reserve_index(&mut self) -> Result<()> {
        if self.index.len() < self.index.capacity() {
            return Ok(());
        }
        let required = self.index.len().checked_add(1).ok_or_else(overflow)?;
        let bound = index_bound(required)?;
        if self.index_reservation.is_none() {
            self.index_reservation = Some(self.rows.pool.allocate(512)?);
        }
        let lease = self.index_reservation.as_mut().unwrap();
        let old = lease.size();
        lease.resize(old.checked_add(bound).ok_or_else(overflow)?)?;
        if let Err(error) = self.index.try_reserve(1) {
            lease.resize(old)?;
            return Err(QueryError::Execution(format!(
                "aggregate hash index allocation refused: {error}"
            )));
        }
        // Audited upper bound; preserve the admitted charge on mismatch and
        // return an error instead of permitting subsequent unaccounted growth.
        let actual = self.index.allocation_size();
        if actual > bound {
            return Err(QueryError::Execution(
                "aggregate hash index exceeds admitted layout bound".into(),
            ));
        }
        lease.resize(512usize.checked_add(actual).ok_or_else(overflow)?)?;
        Ok(())
    }

    pub(super) fn get_or_insert_with<I: IntoIterator<Item = AccumulatorState>>(
        &mut self,
        key: u64,
        values: impl FnOnce() -> I,
    ) -> Result<&mut [AccumulatorState]> {
        if let Some(index) = self.index.get(&key).copied() {
            return Ok(self.rows.row_mut(index));
        }
        self.reserve_index()?;
        self.rows.reserve(1)?;
        let index = self.rows.push(key, values())?;
        self.index.insert(key, index);
        Ok(self.rows.row_mut(index))
    }

    pub(super) fn insert_or_merge(&mut self, key: u64, values: &[AccumulatorState]) -> Result<()> {
        if values.len() != self.rows.arity {
            return Err(QueryError::Execution(
                "aggregate state arity mismatch".into(),
            ));
        }
        if let Some(index) = self.index.get(&key).copied() {
            for (a, b) in self.rows.row_mut(index).iter_mut().zip(values) {
                a.merge(b);
            }
        } else {
            self.reserve_index()?;
            let index = self.rows.push(key, values.iter().cloned())?;
            self.index.insert(key, index);
        }
        Ok(())
    }

    pub(super) fn clear(&mut self) {
        *self = Self::new(&self.rows.pool, self.rows.arity);
    }

    pub(super) fn take_rows(&mut self) -> RawRows {
        let replacement = Self::new(&self.rows.pool, self.rows.arity);
        let old = std::mem::replace(self, replacement);
        old.rows
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{AggregateFunction, DecimalValue, ScalarValue};
    use std::collections::BTreeMap;

    fn exact(value: i128) -> AccumulatorState {
        AccumulatorState::SumDecimal {
            coefficient: Some(value),
            scale: -2,
            seen: true,
        }
    }

    #[test]
    fn hash_metadata_bound_covers_real_pinned_growth() {
        let pool = MemoryPool::new(32 << 20);
        let mut state = RawStateMap::new(&pool, 0);
        for key in 0..20000 {
            let old_capacity = state.index.capacity();
            state.get_or_insert_with(key, std::iter::empty).unwrap();
            if state.index.capacity() != old_capacity {
                assert!(state.index.allocation_size() <= index_bound(state.len()).unwrap());
                assert_eq!(
                    state.index_reservation.as_ref().unwrap().size(),
                    512 + state.index.allocation_size()
                );
            }
        }
        assert_eq!(state.len(), 20000);
        assert!(state.iter().all(|(_, values)| values.is_empty()));
        drop(state);
        assert_eq!(pool.used(), 0);
        assert!(index_bound(usize::MAX).is_err());
    }

    #[test]
    fn arena_merges_exact_values_and_keeps_heap_bearing_states() {
        let pool = MemoryPool::new(8 << 20);
        let mut state = RawStateMap::new(&pool, 2);
        let large = (1i128 << 80) + 3;
        for key in [0, u64::MAX, i64::MIN as u64, 8191] {
            state
                .insert_or_merge(
                    key,
                    &[
                        exact(large),
                        AccumulatorState::Min(Some(ScalarValue::Utf8("zebra".into()))),
                    ],
                )
                .unwrap();
            state
                .insert_or_merge(
                    key,
                    &[
                        exact(-1),
                        AccumulatorState::Min(Some(ScalarValue::Utf8("ant".into()))),
                    ],
                )
                .unwrap();
        }
        for (_, values) in state.iter() {
            assert_eq!(
                values[0].finalize(&AggregateFunction::Sum).unwrap(),
                ScalarValue::Decimal128(DecimalValue::new(large - 1, -2))
            );
            assert_eq!(
                values[1].finalize(&AggregateFunction::Min).unwrap(),
                ScalarValue::Utf8("ant".into())
            );
        }
        assert_eq!(state.len(), 4);
        drop(state);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn repeated_keys_need_no_new_reservation_or_initializer() {
        let pool = MemoryPool::new(1 << 20);
        let mut state = RawStateMap::new(&pool, 1);
        state
            .get_or_insert_with(7, || [AccumulatorState::Count(3)])
            .unwrap();
        let fill = pool.allocate(pool.available()).unwrap();
        let peak = pool.reserved_peak();
        let values = state
            .get_or_insert_with(7, || -> [AccumulatorState; 1] {
                panic!("existing group must not initialize")
            })
            .unwrap();
        assert!(matches!(values[0], AccumulatorState::Count(3)));
        assert_eq!(pool.reserved_peak(), peak);
        assert!(state
            .get_or_insert_with(8, || [AccumulatorState::Count(9)])
            .is_err());
        assert_eq!(state.len(), 1);
        drop(fill);
        drop(state);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn child_and_parent_refusals_release_all_arena_storage() {
        let parent = MemoryPool::new_named("shared query", 12000);
        let child = MemoryPool::new_child(&parent, "worker", 12000);
        let sibling = parent.allocate(4000).unwrap();
        let mut state = RawStateMap::new(&child, 1);
        let mut inserted = 0;
        let error = loop {
            match state.get_or_insert_with(inserted, || [exact(inserted as i128)]) {
                Ok(_) => inserted += 1,
                Err(error) => break error,
            }
        };
        assert!(error.to_string().contains("shared query"));
        assert!(inserted > 3);
        assert_eq!(state.len(), inserted as usize);
        assert!(parent.used() <= parent.max());
        drop(child);
        drop(state);
        assert_eq!(parent.used(), 4000);
        drop(sibling);
        assert_eq!(parent.used(), 0);
    }

    #[test]
    fn flat_shards_preserve_each_key_state_and_ownership() {
        let parent = MemoryPool::new(8 << 20);
        let child = MemoryPool::new_child(&parent, "arena shards", 8 << 20);
        let mut state = RawStateMap::new(&child, 2);
        for key in 0..4097 {
            state
                .insert_or_merge(key, &[exact(key as i128), AccumulatorState::Count(2)])
                .unwrap();
        }
        let rows = state.take_rows();
        drop(state);
        let shards = rows.shard(7, |key| key as usize % 7).unwrap();
        let mut actual = BTreeMap::new();
        for (i, shard) in shards.iter().enumerate() {
            for (key, values) in shard.iter() {
                assert_eq!(*key as usize % 7, i);
                assert!(matches!(values[1], AccumulatorState::Count(2)));
                assert!(actual
                    .insert(*key, values[0].finalize(&AggregateFunction::Sum).unwrap())
                    .is_none());
            }
        }
        assert_eq!(actual.len(), 4097);
        for (key, value) in actual {
            assert_eq!(
                value,
                ScalarValue::Decimal128(DecimalValue::new(key as i128, -2))
            );
        }
        drop(child);
        assert!(parent.used() > 0);
        drop(shards);
        assert_eq!(parent.used(), 0);
    }

    #[test]
    fn failed_shard_admission_cleans_up_both_original_and_partial_shards() {
        let pool = MemoryPool::new(12000);
        let mut state = RawStateMap::new(&pool, 1);
        for key in 0..32 {
            state.insert_or_merge(key, &[exact(1)]).unwrap();
        }
        let held = pool.allocate(pool.available()).unwrap();
        assert!(state.take_rows().shard(4, |key| key as usize % 4).is_err());
        drop(state);
        assert_eq!(pool.used(), held.size());
        drop(held);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn malformed_arity_refuses_without_publishing_group() {
        let pool = MemoryPool::new(4096);
        let mut state = RawStateMap::new(&pool, 2);
        assert!(state.insert_or_merge(1, &[exact(1)]).is_err());
        assert_eq!(state.len(), 0);
        assert!(state.get_or_insert_with(1, || [exact(1)]).is_err());
        assert_eq!(state.len(), 0);
        state.insert_or_merge(1, &[exact(2), exact(3)]).unwrap();
        assert_eq!(state.len(), 1);
        drop(state);
        assert_eq!(pool.used(), 0);
    }
}
