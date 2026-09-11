//! Retained input accounting, distinct from logical rows or compact-copy size.
//! Deduplication is local to a batch. Unknown array layouts remain conservative.
use super::{reserved_vec::ReservedVec, MemoryPool};
use crate::{QueryError, Result};
use arrow::{array::*, buffer::Buffer, record_batch::RecordBatch};

fn overflow() -> QueryError {
    QueryError::Execution("retained batch accounting overflow".into())
}

fn primitive_buffers(array: &dyn Array) -> Option<[Option<&Buffer>; 2]> {
    macro_rules! types {
        ($($ty:ty),+ $(,)?) => {$(
            if let Some(a) = array.as_any().downcast_ref::<$ty>() {
                return Some([Some(a.values().inner()), a.nulls().map(|n| n.buffer())]);
            }
        )+};
    }
    types!(
        Int8Array,
        Int16Array,
        Int32Array,
        Int64Array,
        UInt8Array,
        UInt16Array,
        UInt32Array,
        UInt64Array,
        Float16Array,
        Float32Array,
        Float64Array,
        Decimal128Array,
        Decimal256Array,
        Date32Array,
        Date64Array,
        Time32SecondArray,
        Time32MillisecondArray,
        Time64MicrosecondArray,
        Time64NanosecondArray,
        TimestampSecondArray,
        TimestampMillisecondArray,
        TimestampMicrosecondArray,
        TimestampNanosecondArray,
        DurationSecondArray,
        DurationMillisecondArray,
        DurationMicrosecondArray,
        DurationNanosecondArray
    );
    None
}

/// Charges full backing capacities once for recognized shared primitive buffers,
/// plus each array's metadata. It never substitutes slice length for capacity.
/// The admitted temporary ledger is released before the caller admits the total.
/// This does not account for source queues, schema ownership, or other batches.
pub(crate) fn retained_batch_bytes(batch: &RecordBatch, pool: &MemoryPool) -> Result<usize> {
    let mut roots = ReservedVec::with_capacity(
        pool,
        batch.num_columns().checked_mul(2).ok_or_else(overflow)?,
    )?;
    let mut total = 0usize;
    for array in batch.columns() {
        if let Some(buffers) = primitive_buffers(array.as_ref()) {
            let metadata = array
                .get_array_memory_size()
                .checked_sub(array.get_buffer_memory_size())
                .ok_or_else(overflow)?;
            total = total.checked_add(metadata).ok_or_else(overflow)?;
            for buffer in buffers.into_iter().flatten() {
                // Pointer alone is insufficient for independently created custom
                // owners describing differing extents at the same base address.
                let root = (buffer.data_ptr().as_ptr() as usize, buffer.capacity());
                if !roots.as_slice().contains(&root) {
                    total = total.checked_add(root.1).ok_or_else(overflow)?;
                    roots.extend_from_slice(&[root])?;
                }
            }
        } else {
            total = total
                .checked_add(array.get_array_memory_size())
                .ok_or_else(overflow)?;
        }
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn shared_slices_keep_full_backing_charge_once() {
        let values = Int64Array::from(vec![1; 1024]);
        let capacity = values.values().inner().capacity();
        let batch = RecordBatch::try_from_iter(vec![
            ("a", Arc::new(values.slice(0, 1)) as ArrayRef),
            ("b", Arc::new(values.slice(700, 1)) as ArrayRef),
        ])
        .unwrap();
        let pool = MemoryPool::new(64 * 1024);
        assert_eq!(
            retained_batch_bytes(&batch, &pool).unwrap(),
            capacity + 2 * std::mem::size_of::<Int64Array>()
        );
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn independent_allocations_and_shared_validity_are_distinguished() {
        let a = Int64Array::from(vec![Some(1), None]);
        let b = Int64Array::from(vec![Some(1), None]);
        let batch = RecordBatch::try_from_iter(vec![
            ("a", Arc::new(a) as ArrayRef),
            ("b", Arc::new(b) as ArrayRef),
        ])
        .unwrap();
        let pool = MemoryPool::new(64 * 1024);
        assert_eq!(
            retained_batch_bytes(&batch, &pool).unwrap(),
            batch.get_array_memory_size()
        );
        let duplicated = batch.project(&[0, 0]).unwrap();
        assert_eq!(
            retained_batch_bytes(&duplicated, &pool).unwrap(),
            batch.column(0).get_array_memory_size() + std::mem::size_of::<Int64Array>()
        );
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn real_mapped_ipc_columns_share_one_full_file_extent() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("rg_00000.arrow");
        let batch = RecordBatch::try_from_iter(vec![
            ("a", Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef),
            ("b", Arc::new(Int64Array::from(vec![4, 5, 6])) as ArrayRef),
        ])
        .unwrap();
        let mut writer = arrow::ipc::writer::FileWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            &batch.schema(),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let file_bytes = std::fs::metadata(&path).unwrap().len() as usize;
        let loaded = crate::storage::ipc_cache::read_row_group(dir.path(), 0, None, None).unwrap();
        let pool = MemoryPool::new(64 * 1024);
        assert_eq!(
            retained_batch_bytes(&loaded[0].slice(1, 1), &pool).unwrap(),
            file_bytes + 2 * std::mem::size_of::<Int64Array>()
        );
        let a = loaded[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            a.iter().collect::<Vec<_>>(),
            vec![Some(1), Some(2), Some(3)]
        );
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn ledger_refuses_before_allocation_and_unknown_layout_stays_conservative() {
        let a = Arc::new(StringArray::from(vec![Some("abc"), None])) as ArrayRef;
        let batch = RecordBatch::try_from_iter(vec![("a", a.clone()), ("b", a)]).unwrap();
        let small = MemoryPool::new(1);
        assert!(retained_batch_bytes(&batch, &small)
            .unwrap_err()
            .is_memory_limit());
        assert_eq!(small.used(), 0);
        let pool = MemoryPool::new(4096);
        assert_eq!(
            retained_batch_bytes(&batch, &pool).unwrap(),
            batch.get_array_memory_size()
        );
    }
}
