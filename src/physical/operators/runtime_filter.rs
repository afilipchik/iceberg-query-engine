//! Optional membership filters over the hash table's already-evaluated keys.
//! No expression evaluation, probe pull, or unadmitted key-staging allocation.
use crate::execution::{reserved_vec::ReservedVec, MemoryPool, MemoryReservation};
use arrow::array::{Array, ArrayRef, Int64Array};

const OWNER_BYTES: usize = 1024;
#[derive(Debug)]
enum Data {
    Bitmap {
        min: i64,
        bits: ReservedVec<u64>,
    },
    Set {
        values: hashbrown::HashSet<i64>,
        _reservation: MemoryReservation,
    },
}
/// Construction is internal; scans can retain membership after the join drops.
#[derive(Debug)]
pub struct AdmittedRuntimeFilter {
    data: Data,
    _metadata: MemoryReservation,
}
impl AdmittedRuntimeFilter {
    #[inline]
    pub(crate) fn contains(&self, value: i64) -> bool {
        match &self.data {
            Data::Bitmap { min, bits } => {
                let Some(offset) = value
                    .checked_sub(*min)
                    .and_then(|n| usize::try_from(n).ok())
                else {
                    return false;
                };
                bits.as_slice()
                    .get(offset / 64)
                    .is_some_and(|word| (word >> (offset % 64)) & 1 == 1)
            }
            Data::Set { values, .. } => values.contains(&value),
        }
    }
}
// Pinned hashbrown 0.17, i64 entries: 4/8/16 buckets for small tables,
// then 7/8 load. Reserve 64 control/alignment bytes (current groups <=16).
// Validate actual allocation and capacity before inserting; no growth follows.
fn set_bound(count: usize) -> Option<usize> {
    if count == 0 {
        return Some(0);
    }
    let buckets = match count {
        1..=3 => 4,
        4..=7 => 8,
        8..=14 => 16,
        _ => (count.checked_mul(8)? / 7).checked_next_power_of_two()?,
    };
    buckets
        .checked_mul(std::mem::size_of::<i64>() + 1)?
        .checked_add(64)
}
fn visit(keys: &[Vec<ArrayRef>], column: usize, mut f: impl FnMut(i64)) -> Option<()> {
    for batch in keys {
        let array = batch.get(column)?.as_any().downcast_ref::<Int64Array>()?;
        for value in array.iter().flatten() {
            f(value);
        }
    }
    Some(())
}
/// A costing decision can decline, but must never weaken membership semantics.
/// Existing hash-table arrays are borrowed; NULLs contribute no matching key.
pub(crate) fn prepare(
    keys: &[Vec<ArrayRef>],
    column: usize,
    parent: &MemoryPool,
) -> Option<AdmittedRuntimeFilter> {
    let mut count = 0usize;
    let (mut min, mut max) = (i64::MAX, i64::MIN);
    for batch in keys {
        let array = batch.get(column)?.as_any().downcast_ref::<Int64Array>()?;
        count = count.checked_add(array.len() - array.null_count())?;
        for value in array.iter().flatten() {
            min = min.min(value);
            max = max.max(value);
        }
    }
    if count == 0 {
        return None;
    }
    let set_bytes = set_bound(count)?;
    let words = max
        .abs_diff(min)
        .checked_add(1)
        .and_then(|width| usize::try_from(width).ok())
        .map(|width| width.div_ceil(64));
    let bitmap_bytes = words.and_then(|words| words.checked_mul(8));
    let budget = parent.available() / 8;
    let bitmap_fits = bitmap_bytes
        .and_then(|n| n.checked_add(512 + OWNER_BYTES))
        .is_some_and(|n| n <= budget);
    let set_fits = set_bytes
        .checked_add(OWNER_BYTES)
        .is_some_and(|n| n <= budget);
    if !bitmap_fits && !set_fits {
        return None;
    }
    // Bitmap probes avoid hashing. Permit modest extra bytes for that saving,
    // but never a huge sparse domain or a representation exceeding admission.
    let bitmap = bitmap_fits && (!set_fits || bitmap_bytes? <= set_bytes.saturating_mul(4));
    let pool = MemoryPool::new_child(parent, "runtime join filter", budget);
    let metadata = pool.allocate(OWNER_BYTES).ok()?;
    let data = if bitmap {
        let words = words?;
        let mut bits = ReservedVec::with_capacity(&pool, words).ok()?;
        bits.extend_reserved(words, std::iter::repeat(0)).ok()?;
        visit(keys, column, |value| {
            let offset = value.abs_diff(min) as usize;
            bits.as_mut_slice()[offset / 64] |= 1 << (offset % 64);
        })?;
        Data::Bitmap { min, bits }
    } else {
        let mut reservation = pool.allocate(set_bytes).ok()?;
        let mut values = hashbrown::HashSet::new();
        values.try_reserve(count).ok()?;
        if values.capacity() < count || values.allocation_size() > set_bytes {
            return None;
        }
        reservation.resize(values.allocation_size()).ok()?;
        visit(keys, column, |value| {
            values.insert(value);
        })?;
        Data::Set {
            values,
            _reservation: reservation,
        }
    };
    Some(AdmittedRuntimeFilter {
        data,
        _metadata: metadata,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn hashbrown_allocation_bound_covers_capacity_transitions() {
        for count in (1..128).chain([255, 256, 257, 1023, 1024, 1025, 65535, 65536]) {
            let mut set = hashbrown::HashSet::<i64>::new();
            set.try_reserve(count).unwrap();
            assert!(set.capacity() >= count);
            assert!(
                set.allocation_size() <= set_bound(count).unwrap(),
                "count={count}"
            );
        }
    }
    #[test]
    fn optional_refusal_is_transactional_and_signed_membership_is_exact() {
        let keys = vec![vec![Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            None,
            Some(0),
            Some(i64::MAX),
            Some(0),
        ])) as ArrayRef]];
        let small = MemoryPool::new(8192);
        assert!(prepare(&keys, 0, &small).is_none());
        assert_eq!(small.used(), 0);
        assert_eq!(small.reserved_peak(), 0);
        let pool = MemoryPool::new(128 * 1024);
        let filter = prepare(&keys, 0, &pool).unwrap();
        for value in [i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX] {
            assert_eq!(
                filter.contains(value),
                [i64::MIN, 0, i64::MAX].contains(&value)
            );
        }
        assert!(pool.used() > 0);
        drop(filter);
        assert_eq!(pool.used(), 0);
        let nulls = vec![vec![
            Arc::new(Int64Array::from(vec![None, None])) as ArrayRef
        ]];
        assert!(prepare(&nulls, 0, &pool).is_none());
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn selected_evaluated_key_column_is_complete_or_declines() {
        use arrow::array::Float64Array;
        let pool = MemoryPool::new(128 * 1024);
        let keys = vec![
            vec![
                Arc::new(Int64Array::from(vec![99, 99])) as ArrayRef,
                Arc::new(Int64Array::from(vec![Some(-4), None])) as ArrayRef,
            ],
            vec![
                Arc::new(Int64Array::from(vec![99])) as ArrayRef,
                Arc::new(Int64Array::from(vec![6])) as ArrayRef,
            ],
        ];
        let filter = prepare(&keys, 1, &pool).unwrap();
        assert!(filter.contains(-4) && filter.contains(6));
        assert!(!filter.contains(99) && !filter.contains(0));
        drop(filter);
        assert_eq!(pool.used(), 0);
        assert!(prepare(&keys, 2, &pool).is_none());
        let mut incompatible = keys;
        incompatible[1][1] = Arc::new(Float64Array::from(vec![6.0]));
        assert!(
            prepare(&incompatible, 1, &pool).is_none(),
            "must not publish a partial key domain"
        );
        assert_eq!(pool.used(), 0);
        assert!(set_bound(usize::MAX).is_none());
    }
    use std::sync::Arc;
}
