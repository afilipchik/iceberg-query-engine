//! Fixed-capacity, admitted flat output accumulation across input batches.
//! Retains column buffers, not an expanding list of input batch owners.
use crate::{
    execution::{reserved_vec::ReservedVec, MemoryPool, MemoryReservation, ReservedBufferBuilder},
    QueryError, Result,
};
use arrow::{
    array::*,
    buffer::{BooleanBuffer, Buffer, NullBuffer},
    datatypes::*,
    record_batch::RecordBatch,
};
use std::sync::Arc;

fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("admitted coalesce: {message}"))
}

macro_rules! fixed_values {
    ($($name:ident : $ty:ty => $pattern:pat),* $(,)?) => {
        enum Values {
            $($name(ReservedBufferBuilder<<$ty as ArrowPrimitiveType>::Native>),)*
            Boolean(ReservedBufferBuilder<u8>),
            Utf8 { offsets: ReservedBufferBuilder<i32>, bytes: ReservedBufferBuilder<u8>, limit: usize },
        }
        impl Values {
            fn new(dtype: &DataType, rows: usize, bytes: usize, pool: &MemoryPool) -> Result<Self> {
                Ok(match dtype {
                    $($pattern => Self::$name(ReservedBufferBuilder::with_capacity(pool, rows)?),)*
                    DataType::Boolean => {
                        let mut values = ReservedBufferBuilder::with_capacity(pool, rows.div_ceil(8))?;
                        values.extend_reserved(rows.div_ceil(8), std::iter::repeat(0))?;
                        Self::Boolean(values)
                    },
                    DataType::Utf8 => {
                        let mut offsets = ReservedBufferBuilder::with_capacity(pool, rows.checked_add(1).ok_or_else(|| invalid("row extent overflow"))?)?;
                        offsets.extend_reserved(1, [0])?;
                        Self::Utf8 { offsets, bytes: ReservedBufferBuilder::with_capacity(pool, bytes)?, limit: bytes }
                    },
                    other => return Err(QueryError::NotImplemented(format!("admitted coalesce type {other:?}"))),
                })
            }
            fn accepts(&self, array: &ArrayRef) -> bool {
                match self {
                    $(Self::$name(_) => array.as_any().is::<PrimitiveArray<$ty>>(),)*
                    Self::Boolean(_) => array.as_any().is::<BooleanArray>(),
                    Self::Utf8 { .. } => array.as_any().is::<StringArray>(),
                }
            }
            fn fit(&self, array: &ArrayRef, offset: usize, count: usize) -> usize {
                if let Self::Utf8 { bytes, limit, .. } = self {
                    let input = array.as_any().downcast_ref::<StringArray>().unwrap();
                    let mut remaining = limit - bytes.as_slice().len();
                    for i in 0..count {
                        let length = if input.is_null(offset+i) { 0 } else { input.value(offset+i).len() };
                        if length > remaining { return i; }
                        remaining -= length;
                    }
                }
                count
            }
            fn append(&mut self, array: &ArrayRef, offset: usize, count: usize, output_offset: usize) -> Result<()> {
                match self {
                    $(Self::$name(values) => {
                        let input = array.as_any().downcast_ref::<PrimitiveArray<$ty>>().unwrap();
                        values.extend_reserved(count, input.values()[offset..offset+count].iter().copied())
                    },)*
                    Self::Boolean(values) => {
                        let input = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                        for i in 0..count {
                            if input.value(offset+i) { let row=output_offset+i; values.as_mut_slice()[row/8] |= 1 << (row%8); }
                        }
                        Ok(())
                    },
                    Self::Utf8 { offsets, bytes, .. } => {
                        let input = array.as_any().downcast_ref::<StringArray>().unwrap();
                        for i in offset..offset+count {
                            if input.is_valid(i) { let v=input.value(i).as_bytes(); bytes.extend_reserved(v.len(), v.iter().copied())?; }
                            offsets.extend_reserved(1, [i32::try_from(bytes.as_slice().len()).map_err(|_| invalid("UTF8 extent overflow"))?])?;
                        }
                        Ok(())
                    },
                }
            }
            fn buffers(self) -> (Buffer, Option<Buffer>) {
                match self {
                    $(Self::$name(values) => (values.finish(), None),)*
                    Self::Boolean(values) => (values.finish(), None),
                    Self::Utf8 { offsets, bytes, .. } => (offsets.finish(), Some(bytes.finish())),
                }
            }
        }
    }
}
fixed_values! {
    I8: Int8Type => DataType::Int8,
    I16: Int16Type => DataType::Int16,
    I32: Int32Type => DataType::Int32,
    I64: Int64Type => DataType::Int64,
    U8: UInt8Type => DataType::UInt8,
    U16: UInt16Type => DataType::UInt16,
    U32: UInt32Type => DataType::UInt32,
    U64: UInt64Type => DataType::UInt64,
    F32: Float32Type => DataType::Float32,
    F64: Float64Type => DataType::Float64,
    D32: Date32Type => DataType::Date32,
    D64: Date64Type => DataType::Date64,
    Decimal: Decimal128Type => DataType::Decimal128(_, _),
    TS: TimestampSecondType => DataType::Timestamp(TimeUnit::Second, _),
    TM: TimestampMillisecondType => DataType::Timestamp(TimeUnit::Millisecond, _),
    TU: TimestampMicrosecondType => DataType::Timestamp(TimeUnit::Microsecond, _),
    TN: TimestampNanosecondType => DataType::Timestamp(TimeUnit::Nanosecond, _),
}
struct Column {
    values: Values,
    validity: Option<ReservedBufferBuilder<u8>>,
}

pub(crate) struct BatchAccumulator {
    columns: ReservedVec<Column>,
    output: ReservedVec<ArrayRef>,
    schema: SchemaRef,
    capacity: usize,
    rows: usize,
    failed: bool,
    handoff: MemoryReservation,
    _construction: MemoryReservation,
}
impl BatchAccumulator {
    pub(crate) fn new(
        schema: SchemaRef,
        rows: usize,
        bytes: usize,
        pool: &MemoryPool,
    ) -> Result<Self> {
        if rows == 0 || bytes == 0 || bytes > i32::MAX as usize {
            return Err(invalid("invalid capacity"));
        }
        let metadata = schema
            .fields()
            .len()
            .checked_mul(4096)
            .ok_or_else(|| invalid("metadata extent overflow"))?;
        let construction = pool.allocate(metadata)?;
        let handoff = pool.allocate(metadata)?;
        let output = ReservedVec::with_capacity(pool, schema.fields().len())?;
        let mut columns = ReservedVec::with_capacity(pool, schema.fields().len())?;
        for field in schema.fields() {
            let values = Values::new(field.data_type(), rows, bytes, pool)?;
            let validity = if field.is_nullable() {
                let mut bits = ReservedBufferBuilder::with_capacity(pool, rows.div_ceil(8))?;
                bits.extend_reserved(rows.div_ceil(8), std::iter::repeat(0))?;
                Some(bits)
            } else {
                None
            };
            columns.extend_reserved(1, [Column { values, validity }])?;
        }
        Ok(Self {
            columns,
            output,
            schema,
            capacity: rows,
            rows: 0,
            failed: false,
            handoff,
            _construction: construction,
        })
    }
    pub(crate) fn rows(&self) -> usize {
        self.rows
    }
    pub(crate) fn full(&self) -> bool {
        self.rows == self.capacity
    }
    /// Returns the exact consumed prefix. Zero means a byte/row boundary, not EOF.
    /// All input representations and capacities are checked before any append.
    pub(crate) fn append(&mut self, batch: &RecordBatch, offset: usize) -> Result<usize> {
        if self.failed {
            return Err(invalid("accumulator is poisoned"));
        }
        if offset > batch.num_rows() || batch.num_columns() != self.columns.as_slice().len() {
            return Err(invalid("input extent differs"));
        }
        for ((column, array), field) in self
            .columns
            .as_slice()
            .iter()
            .zip(batch.columns())
            .zip(self.schema.fields())
        {
            if array.data_type() != field.data_type()
                || !column.values.accepts(array)
                || (!field.is_nullable() && array.null_count() != 0)
            {
                return Err(invalid("input type or nullability differs"));
            }
        }
        let mut count = (self.capacity - self.rows).min(batch.num_rows() - offset);
        for (column, array) in self.columns.as_slice().iter().zip(batch.columns()) {
            count = column.values.fit(array, offset, count);
        }
        if count == 0 {
            return Ok(0);
        }
        let result = (|| {
            for (column, array) in self.columns.as_mut_slice().iter_mut().zip(batch.columns()) {
                column.values.append(array, offset, count, self.rows)?;
                if let Some(bits) = &mut column.validity {
                    for i in 0..count {
                        if array.is_valid(offset + i) {
                            let row = self.rows + i;
                            bits.as_mut_slice()[row / 8] |= 1 << (row % 8);
                        }
                    }
                }
            }
            Ok(())
        })();
        if let Err(e) = result {
            self.failed = true;
            return Err(e);
        }
        self.rows += count;
        Ok(count)
    }
    pub(crate) fn finish(mut self) -> Result<RecordBatch> {
        if self.failed {
            return Err(invalid("accumulator is poisoned"));
        }
        let (columns, _column_owner) = self.columns.into_parts();
        for (column, field) in columns.into_iter().zip(self.schema.fields()) {
            let (first, second) = column.values.buffers();
            let nulls = column
                .validity
                .map(|v| NullBuffer::new(BooleanBuffer::new(v.finish(), 0, self.rows)));
            let mut data = ArrayData::builder(field.data_type().clone())
                .len(self.rows)
                .add_buffer(first)
                .nulls(nulls);
            if let Some(second) = second {
                data = data.add_buffer(second);
            }
            self.output
                .extend_reserved(1, [make_array(data.build()?)])?;
        }
        super::admitted_batch::finish_reserved(self.schema, self.rows, self.output, self.handoff)
    }
}

#[cfg(test)]
#[path = "admitted_coalesce_tests.rs"]
mod tests;
