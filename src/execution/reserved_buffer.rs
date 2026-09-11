//! Fallible, pre-admitted typed buffer construction.
//!
//! This does not intercept allocations made by ordinary Arrow kernels. Callers
//! must use this builder for every new buffer they want covered by its contract.
use super::reserved_vec::ReservedVec;
use super::{MemoryPool, MemoryReservation};
use crate::error::Result;
use arrow::buffer::Buffer;
use arrow::datatypes::ArrowNativeType;

/// A buffer whose payload capacity is reserved before allocation or growth.
/// Both old and new payloads remain charged until old storage is freed.
#[derive(Debug)]
pub struct ReservedBufferBuilder<T: ArrowNativeType> {
    values: ReservedVec<T>,
}

impl<T: ArrowNativeType> ReservedBufferBuilder<T> {
    pub fn new(pool: &MemoryPool) -> Result<Self> {
        Self::with_capacity(pool, 0)
    }

    /// Checked initial payload and owner charge, without allocating or reserving.
    pub(crate) fn initial_allocation_bytes(capacity: usize) -> Result<usize> {
        ReservedVec::<T>::initial_allocation_bytes(capacity)
    }

    pub fn with_capacity(pool: &MemoryPool, capacity: usize) -> Result<Self> {
        Ok(Self {
            values: ReservedVec::with_capacity(pool, capacity)?,
        })
    }

    pub fn as_slice(&self) -> &[T] {
        self.values.as_slice()
    }
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        self.values.as_mut_slice()
    }

    /// Fill admitted storage without growing it. A short iterator rolls back
    /// the append; misleading size hints cannot bypass the capacity limit.
    pub fn extend_reserved(
        &mut self,
        count: usize,
        values: impl IntoIterator<Item = T>,
    ) -> Result<()> {
        self.values.extend_reserved(count, values)
    }

    /// Fallible direct fill; conversion errors restore the original length.
    pub fn try_extend_reserved(
        &mut self,
        count: usize,
        values: impl IntoIterator<Item = Result<T>>,
    ) -> Result<()> {
        self.values.try_extend_reserved(count, values)
    }

    /// Failure to admit growth preserves existing values and their lease.
    pub fn extend_from_slice(&mut self, values: &[T]) -> Result<()> {
        self.values.extend_from_slice(values)
    }

    /// Transfer storage and its lease to Arrow without copying. Clones and
    /// slices retain ownership after the query/context or builder is dropped.
    pub fn finish(self) -> Buffer {
        let (values, reservation) = self.values.into_parts();
        Buffer::from(bytes::Bytes::from_owner(ReservedOwner {
            buffer: Buffer::from_vec(values),
            _reservation: reservation,
        }))
    }
}

struct ReservedOwner {
    // Field order frees data before releasing its budget.
    buffer: Buffer,
    _reservation: MemoryReservation,
}
impl AsRef<[u8]> for ReservedOwner {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}
