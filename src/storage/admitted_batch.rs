//! Aligned flat batches with retained column prefixes and admitted handoff metadata.
use super::{admitted_flat_column::AdmittedFlatColumn, admitted_page_read::PageSource};
use crate::{
    execution::{reserved_vec::ReservedVec, MemoryPool, MemoryReservation},
    QueryError, Result,
};
use arrow::{
    array::{make_array, ArrayRef},
    buffer::{BooleanBuffer, Buffer, NullBuffer},
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchOptions},
};
use std::sync::Arc;
fn invalid(message: &str) -> QueryError {
    QueryError::Storage(format!("admitted batch: {message}"))
}
pub(crate) trait ColumnSource {
    type Checkpoint: Clone;
    const COORDINATED: bool = false;
    fn prepare_output(&mut self, _pool: &MemoryPool) -> Result<bool> {
        unreachable!("uncoordinated source")
    }
    fn checkpoint(&self) -> Self::Checkpoint {
        unreachable!("uncoordinated source")
    }
    fn restore(&mut self, _checkpoint: &Self::Checkpoint) {
        unreachable!("uncoordinated source")
    }
    fn next_prepared(
        &mut self,
        _rows: usize,
        _bytes: usize,
        _pool: &MemoryPool,
    ) -> Result<Option<ArrayRef>> {
        unreachable!("uncoordinated source")
    }
    fn next(&mut self, rows: usize, bytes: usize, pool: &MemoryPool) -> Result<Option<ArrayRef>>;
}
impl<S: PageSource> ColumnSource for AdmittedFlatColumn<S> {
    type Checkpoint = super::admitted_flat_column::OutputCheckpoint;
    const COORDINATED: bool = true;
    fn prepare_output(&mut self, pool: &MemoryPool) -> Result<bool> {
        AdmittedFlatColumn::prepare_output(self, pool)
    }
    fn checkpoint(&self) -> Self::Checkpoint {
        AdmittedFlatColumn::checkpoint(self)
    }
    fn restore(&mut self, checkpoint: &Self::Checkpoint) {
        AdmittedFlatColumn::restore(self, checkpoint)
    }
    fn next_prepared(
        &mut self,
        rows: usize,
        bytes: usize,
        pool: &MemoryPool,
    ) -> Result<Option<ArrayRef>> {
        AdmittedFlatColumn::next_prepared(self, rows, bytes, pool)
    }

    fn next(&mut self, rows: usize, bytes: usize, pool: &MemoryPool) -> Result<Option<ArrayRef>> {
        AdmittedFlatColumn::next(self, rows, bytes, pool)
    }
}
struct Pending {
    array: Option<ArrayRef>,
    offset: usize,
    ended: bool,
}
struct Handoff {
    _vector: MemoryReservation,
    _metadata: MemoryReservation,
}
struct OwnedBuffer {
    buffer: Buffer,
    _handoff: Arc<Handoff>,
}
impl AsRef<[u8]> for OwnedBuffer {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}
fn retain(buffer: Buffer, owner: &Arc<Handoff>) -> Buffer {
    Buffer::from(bytes::Bytes::from_owner(OwnedBuffer {
        buffer,
        _handoff: owner.clone(),
    }))
}
/// Flat arrays always have a values/offset buffer. Attaching the handoff to
/// buffers keeps it alive through typed downcasts, ArrayData extraction and slices.
fn slice(array: &ArrayRef, offset: usize, rows: usize, owner: &Arc<Handoff>) -> Result<ArrayRef> {
    let data = array.to_data().slice(offset, rows);
    if !data.child_data().is_empty() || data.buffers().is_empty() || data.buffers().len() > 2 {
        return Err(invalid("non-flat array at handoff"));
    }
    let buffers = data
        .buffers()
        .iter()
        .map(|b| retain(b.clone(), owner))
        .collect();
    let nulls = data.nulls().map(|n| {
        NullBuffer::new(BooleanBuffer::new(
            retain(n.buffer().clone(), owner),
            n.offset(),
            n.len(),
        ))
    });
    Ok(make_array(
        data.into_builder().buffers(buffers).nulls(nulls).build()?,
    ))
}
/// Complete an already admitted flat-column vector, retaining handoff metadata
/// through every output buffer. Caller-owned schema storage is not charged here.
pub(crate) fn finish(
    schema: SchemaRef,
    rows: usize,
    arrays: ReservedVec<ArrayRef>,
    pool: &MemoryPool,
) -> Result<RecordBatch> {
    if arrays.as_slice().iter().any(|a| a.len() != rows) {
        return Err(invalid("gathered column length mismatch"));
    }
    let metadata = pool.allocate(
        arrays
            .as_slice()
            .len()
            .checked_mul(4096)
            .ok_or_else(|| invalid("metadata extent overflow"))?,
    )?;
    finish_reserved(schema, rows, arrays, metadata)
}

/// A constructor may reserve final handoff storage before consuming input rows.
pub(crate) fn finish_reserved(
    schema: SchemaRef,
    rows: usize,
    arrays: ReservedVec<ArrayRef>,
    metadata: MemoryReservation,
) -> Result<RecordBatch> {
    let required = arrays
        .as_slice()
        .len()
        .checked_mul(4096)
        .ok_or_else(|| invalid("metadata extent overflow"))?;
    if metadata.size() < required || arrays.as_slice().iter().any(|a| a.len() != rows) {
        return Err(invalid("invalid reserved handoff extent"));
    }
    let (mut values, vector) = arrays.into_parts();
    let owner = Arc::new(Handoff {
        _vector: vector,
        _metadata: metadata,
    });
    for array in &mut values {
        *array = slice(array, 0, rows, &owner)?;
    }
    Ok(RecordBatch::try_new_with_options(
        schema,
        values,
        &RecordBatchOptions::new().with_row_count(Some(rows)),
    )?)
}
pub(crate) struct AdmittedBatchReader<C> {
    columns: ReservedVec<C>,
    pending: ReservedVec<Pending>,
    schema: SchemaRef,
    remaining: usize,
    failed: bool,
}
impl<C: ColumnSource> AdmittedBatchReader<C> {
    pub(crate) fn new(
        columns: ReservedVec<C>,
        schema: SchemaRef,
        rows: usize,
        pool: &MemoryPool,
    ) -> Result<Self> {
        if columns.as_slice().len() != schema.fields().len() {
            return Err(invalid("schema/column count mismatch"));
        }
        let mut pending = ReservedVec::with_capacity(pool, columns.as_slice().len())?;
        pending.extend_reserved(
            columns.as_slice().len(),
            std::iter::repeat_with(|| Pending {
                array: None,
                offset: 0,
                ended: false,
            }),
        )?;
        Ok(Self {
            columns,
            pending,
            schema,
            remaining: rows,
            failed: false,
        })
    }
    pub(crate) fn next(
        &mut self,
        max_rows: usize,
        value_bytes: usize,
        pool: &MemoryPool,
    ) -> Result<Option<RecordBatch>> {
        if self.failed {
            return Err(invalid("reader is poisoned"));
        }
        if max_rows == 0 || value_bytes == 0 {
            return Err(invalid("positive output targets required"));
        }
        let result = self.next_batch(max_rows, value_bytes, pool);
        if result.as_ref().is_err_and(|e| !e.is_memory_limit()) {
            self.failed = true;
        }
        result
    }
    /// Page preparation may perform source I/O, but output trials never do.
    /// Checkpoints and provisional arrays stay reserved until all columns agree.
    fn fill_coordinated(&mut self, max_rows: usize, bytes: usize, pool: &MemoryPool) -> Result<()> {
        let missing = self
            .pending
            .as_slice()
            .iter()
            .filter(|p| p.array.is_none() && !p.ended)
            .count();
        if missing == 0 {
            return Ok(());
        }
        let mut checkpoints = ReservedVec::with_capacity(pool, missing)?;
        for (index, (column, pending)) in self
            .columns
            .as_mut_slice()
            .iter_mut()
            .zip(self.pending.as_mut_slice())
            .enumerate()
        {
            if pending.array.is_none() && !pending.ended {
                if column.prepare_output(pool)? {
                    checkpoints.extend_reserved(1, [(index, column.checkpoint())])?;
                } else {
                    pending.ended = true;
                }
            }
        }
        let mut rows = max_rows;
        loop {
            let mut failure = None;
            for (index, _) in checkpoints.as_slice() {
                let index = *index;
                let column = &mut self.columns.as_mut_slice()[index];
                let pending = &mut self.pending.as_mut_slice()[index];
                match column.next_prepared(rows, bytes, pool) {
                    Ok(Some(array)) => {
                        if array.is_empty()
                            || array.data_type() != self.schema.field(index).data_type()
                            || (!self.schema.field(index).is_nullable() && array.null_count() != 0)
                        {
                            return Err(invalid(
                                "prepared output type, nullability or extent differs",
                            ));
                        }
                        pending.array = Some(array);
                        pending.offset = 0;
                    }
                    Ok(None) => return Err(invalid("prepared column ended without output")),
                    Err(error) => {
                        failure = Some(error);
                        break;
                    }
                }
            }
            let Some(error) = failure else {
                return Ok(());
            };
            if !error.is_memory_limit() {
                return Err(error);
            }
            // Restore decoder cursors from retained pages and discard only this
            // trial's arrays. Existing pending prefixes are untouched. Dictionary
            // IDs decoded by this trial are provisional too; no source is replayed.
            for (index, checkpoint) in checkpoints.as_slice() {
                let pending = &mut self.pending.as_mut_slice()[*index];
                pending.array = None;
                pending.offset = 0;
                self.columns.as_mut_slice()[*index].restore(checkpoint);
            }
            if rows == 1 {
                return Err(error);
            }
            rows = (rows / 2).max(1);
        }
    }

    fn next_batch(
        &mut self,
        max_rows: usize,
        value_bytes: usize,
        pool: &MemoryPool,
    ) -> Result<Option<RecordBatch>> {
        // Reserve the eventual batch handoff before columns consume the free
        // budget with output. A refusal here must not pull any column.
        let handoff = if self.remaining > 0 && !self.columns.as_slice().is_empty() {
            let arrays =
                ReservedVec::<ArrayRef>::with_capacity(pool, self.columns.as_slice().len())?;
            // Conservative bounded allowance for flat ArrayData/array headers,
            // buffer-owner wrappers and their small buffer vectors; payloads already
            // have distinct decoder leases. Schema ownership remains with the caller.
            let metadata = pool.allocate(
                self.columns
                    .as_slice()
                    .len()
                    .checked_mul(4096)
                    .ok_or_else(|| invalid("metadata extent overflow"))?,
            )?;
            Some((arrays, metadata))
        } else {
            None
        };
        if C::COORDINATED {
            self.fill_coordinated(max_rows, value_bytes, pool)?;
        } else {
            for (index, (column, pending)) in self
                .columns
                .as_mut_slice()
                .iter_mut()
                .zip(self.pending.as_mut_slice())
                .enumerate()
            {
                if pending.array.is_none() && !pending.ended {
                    match column.next(max_rows, value_bytes, pool)? {
                        Some(array) => {
                            if array.is_empty() {
                                return Err(invalid("column returned an empty nonterminal chunk"));
                            }
                            if array.data_type() != self.schema.field(index).data_type() {
                                return Err(invalid("column type differs from schema"));
                            }
                            if !self.schema.field(index).is_nullable() && array.null_count() != 0 {
                                return Err(invalid("NULLs in required column"));
                            }
                            pending.array = Some(array);
                            pending.offset = 0;
                        }
                        None => pending.ended = true,
                    }
                }
            }
        }
        if self.remaining == 0 {
            if self.pending.as_slice().iter().any(|p| p.array.is_some()) {
                return Err(invalid("column exceeds declared row count"));
            }
            return Ok(None);
        }
        if self.pending.as_slice().iter().any(|p| p.ended) {
            return Err(invalid("column ended before declared row count"));
        }
        let mut rows = max_rows.min(self.remaining);
        for pending in self.pending.as_slice() {
            rows = rows.min(pending.array.as_ref().unwrap().len() - pending.offset);
        }
        if self.columns.as_slice().is_empty() {
            let batch = RecordBatch::try_new_with_options(
                self.schema.clone(),
                vec![],
                &RecordBatchOptions::new().with_row_count(Some(rows)),
            )?;
            self.remaining -= rows;
            return Ok(Some(batch));
        }
        let (arrays, metadata) = handoff.expect("nonempty batch reserved its handoff");
        let (values, vector) = arrays.into_parts();
        let owner = Arc::new(Handoff {
            _vector: vector,
            _metadata: metadata,
        });
        let mut values = values;
        for pending in self.pending.as_slice() {
            values.push(slice(
                pending.array.as_ref().unwrap(),
                pending.offset,
                rows,
                &owner,
            )?);
        }
        let batch = RecordBatch::try_new(self.schema.clone(), values)?;
        // Commit only after all handoff buffers and batch validation succeed.
        for pending in self.pending.as_mut_slice() {
            pending.offset += rows;
            if pending.offset == pending.array.as_ref().unwrap().len() {
                pending.array = None;
                pending.offset = 0;
            }
        }
        self.remaining -= rows;
        Ok(Some(batch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
    };
    use std::{
        collections::VecDeque,
        sync::atomic::{AtomicUsize, Ordering},
    };
    struct Mock {
        chunks: VecDeque<ArrayRef>,
        calls: Arc<AtomicUsize>,
        fail_once: bool,
    }
    impl ColumnSource for Mock {
        type Checkpoint = ();
        fn next(&mut self, _: usize, _: usize, pool: &MemoryPool) -> Result<Option<ArrayRef>> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_once {
                self.fail_once = false;
                return Err(pool.allocate(pool.max() + 1).unwrap_err());
            }
            Ok(self.chunks.pop_front())
        }
    }
    fn mock(chunks: Vec<ArrayRef>, fail_once: bool) -> (Mock, Arc<AtomicUsize>) {
        let calls = Arc::new(AtomicUsize::new(0));
        (
            Mock {
                chunks: chunks.into(),
                calls: calls.clone(),
                fail_once,
            },
            calls,
        )
    }
    fn columns(pool: &MemoryPool, sources: Vec<Mock>) -> ReservedVec<Mock> {
        let mut columns = ReservedVec::with_capacity(pool, sources.len()).unwrap();
        columns.extend_reserved(sources.len(), sources).unwrap();
        columns
    }
    #[test]
    fn unequal_chunks_and_denials_preserve_alignment_and_buffer_owners() {
        let pool = MemoryPool::new(65536);
        let (left, left_calls) = mock(
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![4, 5, 6, 7])),
            ],
            false,
        );
        let (right, right_calls) = mock(
            vec![
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(StringArray::from(vec!["c"])),
                Arc::new(StringArray::from(vec!["d", "e", "f", "g"])),
            ],
            true,
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("n", DataType::Int64, false),
            Field::new("s", DataType::Utf8, true),
        ]));
        let mut reader =
            AdmittedBatchReader::new(columns(&pool, vec![left, right]), schema, 7, &pool).unwrap();
        assert!(reader.next(8, 100, &pool).unwrap_err().is_memory_limit());
        assert_eq!(left_calls.load(Ordering::SeqCst), 1);
        let held = pool.allocate(pool.max() - pool.used()).unwrap();
        assert!(reader.next(8, 100, &pool).unwrap_err().is_memory_limit());
        assert_eq!(left_calls.load(Ordering::SeqCst), 1);
        assert_eq!(right_calls.load(Ordering::SeqCst), 1);
        drop(held);
        let first = reader.next(8, 100, &pool).unwrap().unwrap();
        assert_eq!(first.num_rows(), 2);
        let retained = first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .clone();
        let mut actual_left = Vec::new();
        let mut actual_right = Vec::new();
        let mut sizes = Vec::new();
        let mut next = Some(first);
        while let Some(batch) = next {
            sizes.push(batch.num_rows());
            actual_left.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied(),
            );
            actual_right.extend(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(str::to_owned)),
            );
            drop(batch);
            next = reader.next(8, 100, &pool).unwrap();
        }
        assert_eq!(sizes, vec![2, 1, 4]);
        assert_eq!(actual_left, vec![1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(
            actual_right,
            [
                Some("a"),
                None,
                Some("c"),
                Some("d"),
                Some("e"),
                Some("f"),
                Some("g")
            ]
            .map(|v| v.map(str::to_owned))
        );
        let calls = (
            left_calls.load(Ordering::SeqCst),
            right_calls.load(Ordering::SeqCst),
        );
        assert!(reader.next(8, 100, &pool).unwrap().is_none());
        assert_eq!(
            calls,
            (
                left_calls.load(Ordering::SeqCst),
                right_calls.load(Ordering::SeqCst)
            )
        );
        drop(reader);
        assert!(pool.used() >= 8192);
        let data = retained.to_data();
        drop(retained);
        assert!(pool.used() > 0);
        drop(data);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn unequal_lengths_poison_without_replay() {
        for long in [false, true] {
            let pool = MemoryPool::new(65536);
            let (source, calls) = mock(vec![Arc::new(Int64Array::from(vec![1, 2]))], false);
            let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int64, false)]));
            let mut reader = AdmittedBatchReader::new(
                columns(&pool, vec![source]),
                schema,
                if long { 1 } else { 3 },
                &pool,
            )
            .unwrap();
            drop(reader.next(8, 100, &pool).unwrap());
            assert!(reader.next(8, 100, &pool).is_err());
            let before = calls.load(Ordering::SeqCst);
            assert!(reader
                .next(8, 100, &pool)
                .unwrap_err()
                .to_string()
                .contains("poisoned"));
            assert_eq!(calls.load(Ordering::SeqCst), before);
            drop(reader);
            assert_eq!(pool.used(), 0);
        }
    }
    #[test]
    fn zero_column_projection_keeps_declared_rows() {
        let pool = MemoryPool::new(8192);
        let mut reader = AdmittedBatchReader::<Mock>::new(
            ReservedVec::with_capacity(&pool, 0).unwrap(),
            Arc::new(Schema::empty()),
            5,
            &pool,
        )
        .unwrap();
        assert_eq!(reader.next(3, 100, &pool).unwrap().unwrap().num_rows(), 3);
        assert_eq!(reader.next(3, 100, &pool).unwrap().unwrap().num_rows(), 2);
        assert!(reader.next(3, 100, &pool).unwrap().is_none());
        drop(reader);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn provider_type_and_required_null_mismatches_refuse_before_handoff() {
        for data_type in [DataType::Int64, DataType::Utf8] {
            let pool = MemoryPool::new(65536);
            let (source, calls) =
                mock(vec![Arc::new(Int64Array::from(vec![None, Some(7)]))], false);
            let schema = Arc::new(Schema::new(vec![Field::new("v", data_type, false)]));
            let mut reader =
                AdmittedBatchReader::new(columns(&pool, vec![source]), schema, 2, &pool).unwrap();
            assert!(reader.next(8, 100, &pool).is_err());
            assert!(reader
                .next(8, 100, &pool)
                .unwrap_err()
                .to_string()
                .contains("poisoned"));
            assert_eq!(calls.load(Ordering::SeqCst), 1);
            drop(reader);
            assert_eq!(pool.used(), 0);
        }
    }
}
