//! Owned exact Int64 membership only; no provider/planner/executor capability.
use crate::{
    error::{QueryError, Result},
    execution::{MemoryReservation, SharedMemoryPool},
};
use arrow::array::{Array, Int64Array};
use std::mem::size_of;

/// Immutable after finish. Place in Arc to share; keys drop before their guard.
/// Charged metadata covers this value, not a caller's Arc allocation/control block.
#[derive(Debug)]
pub(crate) struct Int64Membership {
    keys: Vec<i64>,
    has_rows: bool,
    has_null: bool,
    reservation: MemoryReservation,
}
/// Consuming mutations make failed admission destroy, not publish, partial state.
pub(crate) struct Int64MembershipBuilder(Int64Membership);
impl Int64MembershipBuilder {
    pub(crate) fn new(pool: &SharedMemoryPool) -> Result<Self> {
        let reservation = pool.allocate(size_of::<Int64Membership>())?;
        Ok(Self(Int64Membership {
            keys: Vec::new(),
            has_rows: false,
            has_null: false,
            reservation,
        }))
    }
    pub(crate) fn extend(mut self, array: &Int64Array) -> Result<Self> {
        let additional = array
            .len()
            .checked_sub(array.null_count())
            .ok_or_else(|| size_error("invalid NULL count"))?;
        let needed = self
            .0
            .keys
            .len()
            .checked_add(additional)
            .ok_or_else(|| size_error("key count overflow"))?;
        if needed > self.0.keys.capacity() {
            // Vec may realloc through a second allocation: reserve BOTH old and
            // complete requested replacement payload until the old buffer is gone.
            let old_capacity = self.0.keys.capacity();
            let preferred = old_capacity.checked_mul(2).map(|n| n.max(needed));
            // Prefer amortized growth. Overflow or denied preferred admission
            // retries only the minimum required capacity. Failed resize leaves
            // the original reservation intact; actual allocator errors do not
            // trigger retries or discard their failure.
            let target = match preferred.and_then(|count| {
                growth_charge(old_capacity, count)
                    .ok()
                    .map(|bytes| (count, bytes))
            }) {
                Some((count, bytes)) if count > needed => match self.0.reservation.resize(bytes) {
                    Ok(()) => count,
                    Err(_) => {
                        self.0
                            .reservation
                            .resize(growth_charge(old_capacity, needed)?)?;
                        needed
                    }
                },
                _ => {
                    self.0
                        .reservation
                        .resize(growth_charge(old_capacity, needed)?)?;
                    needed
                }
            };
            self.0
                .keys
                .try_reserve_exact(target - self.0.keys.len())
                .map_err(|error| {
                    QueryError::Execution(format!("Int64 membership allocation failed: {error}"))
                })?;
            // try_reserve_exact can report more than requested. Admit excess
            // immediately, before copying any input/further allocation; on failure
            // consuming self drops its now-owned Vec before releasing the guard.
            self.0
                .reservation
                .resize(retained_charge(self.0.keys.capacity())?)?;
        }
        self.0.keys.extend(array.iter().flatten());
        self.0.has_rows |= !array.is_empty();
        self.0.has_null |= array.null_count() != 0;
        Ok(self)
    }
    pub(crate) fn finish(mut self) -> Int64Membership {
        // sort_unstable is in-place; dedup does not shrink the retained capacity.
        self.0.keys.sort_unstable();
        self.0.keys.dedup();
        self.0
    }
}
impl Int64Membership {
    pub(crate) fn is_empty(&self) -> bool {
        !self.has_rows
    }

    /// SQL IN/NOT IN, with no allocation, lock, planner or pool operation.
    pub(crate) fn lookup(&self, value: Option<i64>, negated: bool) -> Option<bool> {
        if !self.has_rows {
            return Some(negated);
        }
        let value = value?;
        if self.keys.binary_search(&value).is_ok() {
            Some(!negated)
        } else if self.has_null {
            None
        } else {
            Some(negated)
        }
    }
    pub(crate) fn reserved_bytes(&self) -> usize {
        self.reservation.size()
    }
}
fn size_error(message: &str) -> QueryError {
    QueryError::Execution(format!("Int64 membership {message}"))
}
fn retained_charge(capacity: usize) -> Result<usize> {
    capacity
        .checked_mul(size_of::<i64>())
        .and_then(|v| v.checked_add(size_of::<Int64Membership>()))
        .ok_or_else(|| size_error("retained size overflow"))
}
fn growth_charge(old_capacity: usize, new_capacity: usize) -> Result<usize> {
    retained_charge(new_capacity)?
        .checked_add(
            old_capacity
                .checked_mul(size_of::<i64>())
                .ok_or_else(|| size_error("growth size overflow"))?,
        )
        .ok_or_else(|| size_error("growth size overflow"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::{create_memory_pool, MemoryPool};
    use std::sync::Arc;
    fn oracle(rhs: &[Option<i64>], lhs: Option<i64>, negated: bool) -> Option<bool> {
        if rhs.is_empty() {
            return Some(negated);
        }
        let lhs = lhs?;
        if rhs.iter().any(|v| *v == Some(lhs)) {
            Some(!negated)
        } else if rhs.iter().any(Option::is_none) {
            None
        } else {
            Some(negated)
        }
    }
    #[test]
    fn independent_membership_truth_table_empty_null_duplicates_full_i64_domain() {
        for rhs in [
            vec![],
            vec![None],
            vec![None, None],
            vec![Some(1), Some(1)],
            vec![None, Some(1), Some(i64::MIN), Some(i64::MAX)],
        ] {
            let pool = create_memory_pool(4096);
            let state = Int64MembershipBuilder::new(&pool)
                .unwrap()
                .extend(&Int64Array::from(rhs.clone()))
                .unwrap()
                .finish();
            for lhs in [
                None,
                Some(i64::MIN),
                Some(-1),
                Some(0),
                Some(1),
                Some(2),
                Some(i64::MAX),
            ] {
                for negated in [false, true] {
                    assert_eq!(
                        state.lookup(lhs, negated),
                        oracle(&rhs, lhs, negated),
                        "rhs={rhs:?}, lhs={lhs:?}, negated={negated}"
                    );
                }
            }
            assert_eq!(pool.used(), retained_charge(state.keys.capacity()).unwrap());
            drop(state);
            assert_eq!(pool.used(), 0);
        }
    }
    #[test]
    fn sliced_multibatch_input_is_copied_and_deduplicated_without_retaining_array() {
        let pool = create_memory_pool(4096);
        let source = Int64Array::from(vec![
            Some(99),
            None,
            Some(i64::MIN),
            Some(2),
            Some(2),
            Some(i64::MAX),
            Some(100),
        ]);
        let builder = Int64MembershipBuilder::new(&pool)
            .unwrap()
            .extend(&source.slice(1, 4))
            .unwrap()
            .extend(&Int64Array::from(Vec::<Option<i64>>::new()))
            .unwrap()
            .extend(&source.slice(3, 3))
            .unwrap();
        let state = builder.finish();
        drop(source);
        assert_eq!(state.keys, vec![i64::MIN, 2, i64::MAX]);
        assert!(state.has_null);
        assert_eq!(state.lookup(Some(99), false), None);
        assert_eq!(state.lookup(Some(2), false), Some(true));
        drop(state);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn lookup_needs_no_budget_and_owner_lives_until_last_arc() {
        let parent = create_memory_pool(4096);
        let child = Arc::new(MemoryPool::new_child(&parent, "membership", 4096));
        let state = Arc::new(
            Int64MembershipBuilder::new(&child)
                .unwrap()
                .extend(&Int64Array::from(vec![1, 2, 2]))
                .unwrap()
                .finish(),
        );
        let bytes = state.reserved_bytes();
        let clone = state.clone();
        drop(state);
        assert_eq!(child.used(), bytes);
        let blocker = parent.allocate(parent.available()).unwrap();
        let peak = parent.reserved_peak();
        for _ in 0..1000 {
            assert_eq!(clone.lookup(Some(2), false), Some(true));
            assert_eq!(clone.lookup(Some(3), true), Some(true));
        }
        assert_eq!(parent.reserved_peak(), peak);
        drop(blocker);
        drop(clone);
        assert_eq!(parent.used(), 0);
    }
    #[test]
    fn insufficient_growth_refuses_before_copy_and_releases_partial_state() {
        let parent = create_memory_pool(4096);
        let child = Arc::new(MemoryPool::new_child(&parent, "membership-child", 4096));
        let builder = Int64MembershipBuilder::new(&child)
            .unwrap()
            .extend(&Int64Array::from(vec![1, 2]))
            .unwrap();
        let bytes = child.used();
        let blocker = parent.allocate(parent.available()).unwrap();
        let result = builder.extend(&Int64Array::from(vec![3, 4, 5]));
        let error = match result {
            Ok(_) => panic!("growth must be denied"),
            Err(e) => e,
        };
        assert!(error.is_memory_limit(), "{error}");
        assert_eq!(child.used(), 0);
        assert_eq!(parent.used(), 4096 - bytes);
        drop(blocker);
        assert_eq!(parent.used(), 0);
    }
    #[test]
    fn growth_peak_includes_old_and_replacement_capacity() {
        let pool = create_memory_pool(4096);
        let builder = Int64MembershipBuilder::new(&pool)
            .unwrap()
            .extend(&Int64Array::from(vec![1, 2]))
            .unwrap();
        let old = builder.0.keys.capacity();
        let needed = old + 1;
        let array = Int64Array::from_iter_values(0..(needed - builder.0.keys.len()) as i64);
        let state = builder.extend(&array).unwrap().finish();
        assert!(pool.reserved_peak() >= growth_charge(old, needed).unwrap());
        assert_eq!(pool.used(), retained_charge(state.keys.capacity()).unwrap());
        drop(state);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn metadata_denial_and_checked_overflow_are_explicit() {
        let pool = create_memory_pool(size_of::<Int64Membership>() - 1);
        assert!(Int64MembershipBuilder::new(&pool).is_err());
        assert_eq!(pool.used(), 0);
        assert!(retained_charge(usize::MAX).is_err());
        assert!(growth_charge(usize::MAX, 1).is_err());
    }
    #[test]
    fn many_single_row_batches_have_logarithmically_many_capacity_changes() {
        let pool = create_memory_pool(1024 * 1024);
        let mut builder = Int64MembershipBuilder::new(&pool).unwrap();
        let mut capacity_changes = 0;
        let mut previous = 0;
        for row in 0..4096 {
            builder = builder.extend(&Int64Array::from(vec![row % 17])).unwrap();
            let capacity = builder.0.keys.capacity();
            if capacity != previous {
                capacity_changes += 1;
                if previous > 0 {
                    assert!(capacity >= previous * 2);
                }
                previous = capacity;
            }
        }
        assert!(
            capacity_changes <= 13,
            "4096 singleton batches must not cause linear reallocations: {capacity_changes}"
        );
        let state = builder.finish();
        assert_eq!(state.keys, (0..17).collect::<Vec<i64>>());
        assert_eq!(pool.used(), retained_charge(state.keys.capacity()).unwrap());
        drop(state);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn tight_parent_budget_falls_back_to_minimum_transient_growth() {
        let parent = create_memory_pool(4096);
        let child = Arc::new(MemoryPool::new_child(&parent, "membership-tight", 4096));
        let builder = Int64MembershipBuilder::new(&child)
            .unwrap()
            .extend(&Int64Array::from(vec![1, 2, 3, 4]))
            .unwrap();
        let old = builder.0.keys.capacity();
        let needed = old + 1;
        let retained = builder.0.reservation.size();
        let minimum_extra = growth_charge(old, needed).unwrap() - retained;
        let blocker = parent.allocate(parent.available() - minimum_extra).unwrap();
        assert_eq!(parent.available(), minimum_extra);
        assert!(growth_charge(old, old * 2).unwrap() - retained > parent.available());
        let extra = Int64Array::from_iter_values(10..10 + (needed - builder.0.keys.len()) as i64);
        let state = builder
            .extend(&extra)
            .expect("minimum fits although doubling is denied")
            .finish();
        assert_eq!(state.lookup(Some(1), false), Some(true));
        assert_eq!(state.lookup(Some(10), false), Some(true));
        assert_eq!(parent.reserved_peak(), parent.max());
        drop(state);
        drop(blocker);
        assert_eq!(parent.used(), 0);
    }
}
