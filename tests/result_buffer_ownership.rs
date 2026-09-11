//! Ownership feasibility gate. This is not production admission or a claim that
//! the current query API enforces its budget. New buffers must be admitted before
//! allocation; this mechanism only preserves a previously acquired lease.
use std::sync::Arc;

use arrow::array::{make_array, Array, ArrayData, ArrayRef, Int64Array, StringArray, StructArray};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::datatypes::{DataType, Field};
use query_engine::execution::{MemoryPool, MemoryReservation};

struct Owner {
    // Release the original allocation before releasing the reservation.
    buffer: Buffer,
    _reservation: Arc<MemoryReservation>,
}
impl AsRef<[u8]> for Owner {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}
fn leased_buffer(buffer: Buffer, reservation: &Arc<MemoryReservation>) -> Buffer {
    Buffer::from(bytes::Bytes::from_owner(Owner {
        buffer,
        _reservation: reservation.clone(),
    }))
}
fn leased_data(data: ArrayData, reservation: &Arc<MemoryReservation>) -> ArrayData {
    let buffers = data
        .buffers()
        .iter()
        .cloned()
        .map(|b| leased_buffer(b, reservation))
        .collect();
    let children = data
        .child_data()
        .iter()
        .cloned()
        .map(|d| leased_data(d, reservation))
        .collect();
    let nulls = data.nulls().map(|n| {
        NullBuffer::new(BooleanBuffer::new(
            leased_buffer(n.buffer().clone(), reservation),
            n.inner().offset(),
            n.len(),
        ))
    });
    data.into_builder()
        .buffers(buffers)
        .child_data(children)
        .nulls(nulls)
        .build()
        .unwrap()
}

#[test]
fn escaped_slice_and_buffer_keep_the_preallocation_reservation() {
    let pool = MemoryPool::new(4096);
    let lease = Arc::new(pool.allocate(4096).unwrap());
    let original = Int64Array::from(vec![10, 20, 30, 40]);
    let original_ptr = original.values().as_ptr();
    let array = make_array(leased_data(original.into_data(), &lease));
    drop(lease);
    let slice = array.slice(1, 2);
    drop(array);
    let data = slice.to_data();
    assert_eq!(
        slice
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .as_ref(),
        &[20, 30]
    );
    assert_eq!(
        data.buffers()[0].as_ptr(),
        original_ptr.wrapping_add(1).cast::<u8>()
    );
    let escaped = data.buffers()[0].clone();
    drop(data);
    drop(slice);
    assert_eq!(pool.used(), 4096);
    std::thread::spawn(move || drop(escaped)).join().unwrap();
    assert_eq!(pool.used(), 0);
}

#[test]
fn nullable_string_validity_escape_retains_lease_and_offsets() {
    let pool = MemoryPool::new(4096);
    let lease = Arc::new(pool.allocate(4096).unwrap());
    let original = StringArray::from(vec![Some("prefix"), None, Some("é"), Some("tail")]);
    let array = make_array(leased_data(original.slice(1, 2).to_data(), &lease));
    drop(original);
    drop(lease);
    let strings = array.as_any().downcast_ref::<StringArray>().unwrap();
    assert!(strings.is_null(0));
    assert_eq!(strings.value(1), "é");
    let validity = strings.nulls().unwrap().buffer().clone();
    drop(array);
    assert_eq!(pool.used(), 4096);
    drop(validity);
    assert_eq!(pool.used(), 0);
}

#[test]
fn nested_child_escape_keeps_parent_pool_charged_once() {
    let parent = MemoryPool::new(4096);
    let pool = MemoryPool::new_child(&parent, "result", 4096);
    let lease = Arc::new(pool.allocate(4096).unwrap());
    let child: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None]));
    let original = StructArray::from(vec![(
        Arc::new(Field::new("v", DataType::Int64, true)),
        child,
    )]);
    let array = make_array(leased_data(original.into_data(), &lease));
    drop(lease);
    let child = array
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap()
        .column(0)
        .clone();
    drop(array);
    assert_eq!(parent.used(), 4096);
    assert_eq!(pool.used(), 4096);
    drop(child);
    assert_eq!(parent.used(), 0);
}

#[test]
fn rejected_admission_never_constructs_output() {
    let pool = MemoryPool::new(64);
    let allocation = pool.allocate(128).map(|lease| {
        let values = vec![1_i64; 16];
        leased_buffer(Buffer::from_vec(values), &Arc::new(lease))
    });
    assert!(allocation.is_err());
    assert_eq!(pool.used(), 0);
    assert_eq!(pool.reserved_peak(), 0);
}

#[test]
fn wrapped_capacity_cannot_certify_retained_allocation_size() {
    let pool = MemoryPool::new(16384);
    let lease = Arc::new(pool.allocate(16384).unwrap());
    let mut values = Vec::<i64>::with_capacity(1024);
    values.extend_from_slice(&[1, 2]);
    let original = Buffer::from_vec(values);
    assert!(original.capacity() >= 8192);
    let wrapped = leased_buffer(original, &lease);
    drop(lease);
    // The opaque owner retains the full original capacity. Consumers cannot
    // rederive that charge from this new view's Arrow capacity or length.
    assert_eq!(wrapped.capacity(), 16);
    assert_eq!(pool.used(), 16384);
    drop(wrapped);
    assert_eq!(pool.used(), 0);
}
