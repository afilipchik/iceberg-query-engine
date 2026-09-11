use arrow::array::{Array, Int64Array};
use arrow::buffer::ScalarBuffer;
use query_engine::execution::{MemoryPool, ReservedBufferBuilder};

#[test]
fn admitted_buffer_remains_charged_through_arrow_slice_escape() {
    let parent = MemoryPool::new(4096);
    let pool = MemoryPool::new_child(&parent, "computed output", 4096);
    let mut builder = ReservedBufferBuilder::<i64>::new(&pool).unwrap();
    let base = pool.used();
    builder.extend_from_slice(&[11, 22, 33]).unwrap();
    builder.as_mut_slice()[1] = 44;
    let ptr = builder.as_slice().as_ptr();
    let buffer = builder.finish();
    assert_eq!(buffer.as_ptr(), ptr.cast::<u8>());
    let array = Int64Array::new(ScalarBuffer::new(buffer, 0, 3), None);
    let slice = array.slice(1, 1);
    drop(array);
    assert_eq!(slice.value(0), 44);
    let escaped = slice.to_data().buffers()[0].clone();
    drop(slice);
    assert_eq!(parent.used(), base + 24);
    drop(pool);
    std::thread::spawn(move || drop(escaped)).join().unwrap();
    assert_eq!(parent.used(), 0);
}

#[test]
fn growth_reserves_both_allocations_and_is_transactional_on_refusal() {
    let probe = MemoryPool::new(4096);
    let empty = ReservedBufferBuilder::<i64>::new(&probe).unwrap();
    let base = probe.used();
    drop(empty);
    // Final two-value payload would fit, but old + replacement would not.
    let pool = MemoryPool::new(base + 16);
    let mut builder = ReservedBufferBuilder::<i64>::new(&pool).unwrap();
    builder.extend_from_slice(&[7]).unwrap();
    let ptr = builder.as_slice().as_ptr();
    let peak = pool.reserved_peak();
    assert!(builder.extend_from_slice(&[8]).is_err());
    assert_eq!(builder.as_slice(), &[7]);
    assert_eq!(builder.as_slice().as_ptr(), ptr);
    assert_eq!(pool.used(), base + 8);
    assert_eq!(pool.reserved_peak(), peak);
    drop(builder);
    assert_eq!(pool.used(), 0);
}

#[test]
fn geometric_growth_accounts_for_copy_peak_and_releases_old_storage() {
    let pool = MemoryPool::new(4096);
    let mut builder = ReservedBufferBuilder::<i64>::new(&pool).unwrap();
    let base = pool.used();
    builder.extend_from_slice(&[1, 2, 3]).unwrap();
    builder.extend_from_slice(&[4]).unwrap();
    assert_eq!(pool.reserved_peak(), base + 24 + 48);
    assert_eq!(pool.used(), base + 48);
    builder.extend_from_slice(&[5, 6]).unwrap();
    assert_eq!(pool.reserved_peak(), base + 24 + 48);
    assert_eq!(builder.as_slice(), &[1, 2, 3, 4, 5, 6]);
    let buffer = builder.finish();
    assert_eq!(pool.used(), base + 48);
    drop(buffer);
    assert_eq!(pool.used(), 0);
}

#[test]
fn empty_buffer_keeps_owner_envelope_until_final_drop() {
    let pool = MemoryPool::new(4096);
    let buffer = ReservedBufferBuilder::<u8>::new(&pool).unwrap().finish();
    assert!(buffer.is_empty());
    assert!(pool.used() > 0);
    drop(buffer);
    assert_eq!(pool.used(), 0);
}

#[test]
fn parent_refuses_sibling_builder_without_leaking_its_admission() {
    let parent = MemoryPool::new(1024);
    let first = MemoryPool::new_child(&parent, "first", 1024);
    let second = MemoryPool::new_child(&parent, "second", 1024);
    let mut builder = ReservedBufferBuilder::<i64>::new(&first).unwrap();
    builder.extend_from_slice(&[1; 32]).unwrap();
    let used = parent.used();
    assert!(ReservedBufferBuilder::<i64>::new(&second).is_err());
    assert_eq!(parent.used(), used);
    assert_eq!(second.used(), 0);
    drop(builder);
    assert_eq!(parent.used(), 0);
}

#[test]
fn direct_fill_is_bounded_even_when_iterator_size_hint_lies() {
    struct Misleading(usize);
    impl Iterator for Misleading {
        type Item = i64;
        fn next(&mut self) -> Option<i64> {
            let v = self.0;
            self.0 += 1;
            Some(v as i64)
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            (usize::MAX, Some(usize::MAX))
        }
    }
    let pool = MemoryPool::new(4096);
    let mut builder = ReservedBufferBuilder::<i64>::with_capacity(&pool, 3).unwrap();
    let used = pool.used();
    builder.extend_reserved(3, Misleading(0)).unwrap();
    assert_eq!(builder.as_slice(), &[0, 1, 2]);
    assert_eq!(pool.used(), used);
    assert_eq!(pool.reserved_peak(), used);
    assert!(builder.extend_reserved(1, [4]).is_err());
    assert_eq!(builder.as_slice(), &[0, 1, 2]);
}

#[test]
fn short_direct_fill_rolls_back_without_releasing_the_admitted_capacity() {
    let pool = MemoryPool::new(4096);
    let mut builder = ReservedBufferBuilder::<i64>::with_capacity(&pool, 4).unwrap();
    builder.extend_reserved(1, [9]).unwrap();
    let used = pool.used();
    assert!(builder.extend_reserved(3, [1, 2]).is_err());
    assert_eq!(builder.as_slice(), &[9]);
    assert_eq!(pool.used(), used);
    builder.extend_reserved(3, [3, 4, 5]).unwrap();
    assert_eq!(builder.as_slice(), &[9, 3, 4, 5]);
}

#[test]
fn fallible_fill_rolls_back_values_and_keeps_capacity_on_conversion_error() {
    let pool = MemoryPool::new(4096);
    let mut builder = ReservedBufferBuilder::<i64>::with_capacity(&pool, 4).unwrap();
    builder.extend_reserved(1, [9]).unwrap();
    let used = pool.used();
    let error = query_engine::error::QueryError::Type("injected conversion".into());
    assert!(builder
        .try_extend_reserved(3, [Ok(1), Err(error), Ok(3)])
        .is_err());
    assert_eq!(builder.as_slice(), &[9]);
    assert_eq!(pool.used(), used);
    assert!(builder.try_extend_reserved(3, [Ok(1)]).is_err());
    assert_eq!(builder.as_slice(), &[9]);
    builder
        .try_extend_reserved(3, std::iter::repeat_with(|| Ok(7)))
        .unwrap();
    assert_eq!(builder.as_slice(), &[9, 7, 7, 7]);
    assert_eq!(pool.used(), used);
    assert!(builder.try_extend_reserved(1, [Ok(1)]).is_err());
}
