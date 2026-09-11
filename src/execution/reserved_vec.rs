//! Reservation-owned storage for values that can require destruction.
//!
//! Only this vector's allocation is charged. Allocations owned by individual
//! elements require separate leases. No mutable Vec escapes this interface.
use super::{MemoryPool, MemoryReservation};
use crate::error::{QueryError, Result};

const OWNER_BYTES: usize = 512;

#[derive(Debug)]
pub(crate) struct ReservedVec<T> {
    // Drop values (including their destructors) before releasing the lease.
    values: Vec<T>,
    reservation: MemoryReservation,
}

fn overflow() -> QueryError {
    QueryError::Execution("reserved vector size overflow".into())
}

fn payload<T>(capacity: usize) -> Result<usize> {
    let bytes = capacity
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(overflow)?;
    if bytes > isize::MAX as usize {
        return Err(overflow());
    }
    Ok(bytes)
}

impl<T> ReservedVec<T> {
    /// Move the last element out while retaining the admitted backing capacity.
    pub(crate) fn pop(&mut self) -> Option<T> {
        self.values.pop()
    }
    /// Initial payload plus owner charge; reservations remain authoritative.
    pub(crate) fn initial_allocation_bytes(capacity: usize) -> Result<usize> {
        payload::<T>(capacity)?
            .checked_add(OWNER_BYTES)
            .ok_or_else(overflow)
    }

    pub(crate) fn with_capacity(pool: &MemoryPool, capacity: usize) -> Result<Self> {
        let reservation = pool.allocate(Self::initial_allocation_bytes(capacity)?)?;
        let mut values = Vec::new();
        values.try_reserve_exact(capacity).map_err(|error| {
            QueryError::Execution(format!("reserved vector allocation refused: {error}"))
        })?;
        if std::mem::size_of::<T>() != 0 && values.capacity() != capacity {
            return Err(QueryError::Execution(
                "reserved vector capacity differs from admission".into(),
            ));
        }
        Ok(Self {
            values,
            reservation,
        })
    }

    pub(crate) fn as_slice(&self) -> &[T] {
        &self.values
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [T] {
        &mut self.values
    }

    /// Drop initialized suffix values while retaining the admitted capacity.
    pub(crate) fn truncate(&mut self, len: usize) {
        self.values.truncate(len);
    }

    pub(crate) fn reserve(&mut self, additional: usize) -> Result<()> {
        let required = self
            .values
            .len()
            .checked_add(additional)
            .ok_or_else(overflow)?;
        if required <= self.values.capacity() {
            return Ok(());
        }
        let target = self
            .values
            .capacity()
            .checked_mul(2)
            .unwrap_or(required)
            .max(required);
        let bytes = payload::<T>(target)?;
        let old_charge = self.reservation.size();
        let final_charge = OWNER_BYTES.checked_add(bytes).ok_or_else(overflow)?;
        // One owner, but both live payloads, until movement and deallocation finish.
        self.reservation
            .resize(old_charge.checked_add(bytes).ok_or_else(overflow)?)?;
        let mut replacement = Vec::new();
        if let Err(error) = replacement.try_reserve_exact(target) {
            self.reservation.resize(old_charge)?;
            return Err(QueryError::Execution(format!(
                "reserved vector allocation refused: {error}"
            )));
        }
        if replacement.capacity() != target {
            drop(replacement);
            self.reservation.resize(old_charge)?;
            return Err(QueryError::Execution(
                "reserved vector capacity differs from admission".into(),
            ));
        }
        // Moves T; never invokes Clone or drops an element during relocation.
        replacement.append(&mut self.values);
        let old = std::mem::replace(&mut self.values, replacement);
        drop(old);
        self.reservation.resize(final_charge)?;
        Ok(())
    }

    /// Initialize only within admitted capacity. Errors restore the old length
    /// and destroy partial values while retaining the admitted allocation.
    pub(crate) fn extend_reserved(
        &mut self,
        count: usize,
        values: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        let start = self.values.len();
        let end = start.checked_add(count).ok_or_else(overflow)?;
        if end > self.values.capacity() {
            return Err(QueryError::Execution(
                "reserved output extent exceeds admitted capacity".into(),
            ));
        }
        self.values.extend(values.into_iter().take(count));
        if self.values.len() != end {
            self.values.truncate(start);
            return Err(QueryError::Execution(
                "reserved output iterator ended before declared extent".into(),
            ));
        }
        Ok(())
    }

    /// Fallible element construction with the same admitted-extent contract.
    pub(crate) fn try_extend_reserved(
        &mut self,
        count: usize,
        values: impl IntoIterator<Item = Result<T>>,
    ) -> Result<()> {
        let start = self.values.len();
        let end = start.checked_add(count).ok_or_else(overflow)?;
        if end > self.values.capacity() {
            return Err(QueryError::Execution(
                "reserved output extent exceeds admitted capacity".into(),
            ));
        }
        for value in values.into_iter().take(count) {
            match value {
                Ok(value) => self.values.push(value),
                Err(error) => {
                    self.values.truncate(start);
                    return Err(error);
                }
            }
        }
        if self.values.len() != end {
            self.values.truncate(start);
            return Err(QueryError::Execution(
                "reserved output iterator ended before declared extent".into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn into_parts(self) -> (Vec<T>, MemoryReservation) {
        (self.values, self.reservation)
    }

    /// Preserve the old payload's charge while the consuming iterator retains
    /// its allocation, including after its last element has been moved out.
    pub(crate) fn into_owned_iter(self) -> ReservedIntoIter<T> {
        ReservedIntoIter {
            values: self.values.into_iter(),
            _reservation: self.reservation,
        }
    }
}

pub(crate) struct ReservedIntoIter<T> {
    values: std::vec::IntoIter<T>,
    _reservation: MemoryReservation,
}
impl<T> Iterator for ReservedIntoIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<T> {
        self.values.next()
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.values.size_hint()
    }
}
impl<T> ExactSizeIterator for ReservedIntoIter<T> {}

impl<T: Copy> ReservedVec<T> {
    pub(crate) fn extend_from_slice(&mut self, values: &[T]) -> Result<()> {
        self.reserve(values.len())?;
        self.values.extend_from_slice(values);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    struct Dropped(Arc<AtomicUsize>, usize);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn moves_drop_bearing_values_and_charges_growth_peak() {
        let parent = MemoryPool::new(4096);
        let child = MemoryPool::new_child(&parent, "state arena", 4096);
        let drops = Arc::new(AtomicUsize::new(0));
        let mut values = ReservedVec::with_capacity(&child, 1).unwrap();
        values
            .try_extend_reserved(1, [Ok(Dropped(drops.clone(), 7))])
            .unwrap();
        values.reserve(1).unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(values.as_slice()[0].1, 7);
        let width = std::mem::size_of::<Dropped>();
        assert_eq!(child.used(), OWNER_BYTES + 2 * width);
        assert_eq!(parent.reserved_peak(), OWNER_BYTES + 3 * width);
        drop(child);
        drop(values);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(parent.used(), 0);
    }

    #[test]
    fn refusal_does_not_initialize_or_move_existing_values() {
        let width = std::mem::size_of::<Dropped>();
        let pool = MemoryPool::new_named("state arena", OWNER_BYTES + 2 * width);
        let drops = Arc::new(AtomicUsize::new(0));
        let mut values = ReservedVec::with_capacity(&pool, 1).unwrap();
        values
            .try_extend_reserved(1, [Ok(Dropped(drops.clone(), 19))])
            .unwrap();
        let ptr = values.as_slice().as_ptr();
        let error = values.reserve(1).unwrap_err().to_string();
        assert!(error.contains("state arena"), "{error}");
        assert_eq!(values.as_slice().as_ptr(), ptr);
        assert_eq!(values.as_slice()[0].1, 19);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(pool.used(), OWNER_BYTES + width);
        drop(values);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn failed_initialization_destroys_only_partial_append() {
        let pool = MemoryPool::new(4096);
        let drops = Arc::new(AtomicUsize::new(0));
        let mut values = ReservedVec::with_capacity(&pool, 4).unwrap();
        values
            .try_extend_reserved(1, [Ok(Dropped(drops.clone(), 7))])
            .unwrap();
        let used = pool.used();
        assert!(values
            .try_extend_reserved(3, [Ok(Dropped(drops.clone(), 8)), Err(overflow())])
            .is_err());
        assert_eq!(values.as_slice().len(), 1);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(pool.used(), used);
        assert!(values
            .try_extend_reserved(2, [Ok(Dropped(drops.clone(), 9))])
            .is_err());
        assert_eq!(drops.load(Ordering::SeqCst), 2);
        drop(values);
        assert_eq!(drops.load(Ordering::SeqCst), 3);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn capacity_refusal_does_not_consume_lazy_initializer() {
        let pool = MemoryPool::new(4096);
        let mut values = ReservedVec::<u64>::with_capacity(&pool, 0).unwrap();
        assert!(values
            .try_extend_reserved(1, std::iter::once_with(|| panic!("must not initialize")))
            .is_err());
        assert_eq!(pool.used(), OWNER_BYTES);
        assert!(values.reserve(usize::MAX).is_err());
        assert_eq!(pool.used(), OWNER_BYTES);
        assert!(ReservedVec::<u64>::with_capacity(&pool, usize::MAX).is_err());
        assert_eq!(pool.used(), OWNER_BYTES);
    }

    #[test]
    fn consuming_iterator_retains_payload_lease_through_partial_and_full_drain() {
        let pool = MemoryPool::new(4096);
        let drops = Arc::new(AtomicUsize::new(0));
        let mut values = ReservedVec::with_capacity(&pool, 2).unwrap();
        values
            .extend_reserved(2, [Dropped(drops.clone(), 1), Dropped(drops.clone(), 2)])
            .unwrap();
        let used = pool.used();
        let mut iter = values.into_owned_iter();
        drop(iter.next());
        assert_eq!(pool.used(), used);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let last = iter.next().unwrap();
        assert_eq!(iter.len(), 0);
        assert_eq!(pool.used(), used);
        drop(iter);
        assert_eq!(pool.used(), 0);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        drop(last);
        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn zero_sized_values_have_no_payload_charge_and_checked_length() {
        let pool = MemoryPool::new(OWNER_BYTES);
        let mut values = ReservedVec::<()>::with_capacity(&pool, 0).unwrap();
        values.reserve(3).unwrap();
        values
            .try_extend_reserved(3, [(); 3].into_iter().map(Ok))
            .unwrap();
        assert_eq!(values.as_slice().len(), 3);
        assert_eq!(pool.used(), OWNER_BYTES);
        assert!(values.reserve(usize::MAX).is_err());
        drop(values);
        assert_eq!(pool.used(), 0);
    }
}
