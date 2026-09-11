//! Enforced fixed-width exposed-buffer bounds, not source-owner/RSS admission.
use crate::error::{QueryError, Result};
use crate::physical::queue_layout::{GatherCopyBound, QueueCopyBound};
use arrow::{
    array::{make_array, ArrayData},
    buffer::{BooleanBuffer, Buffer, NullBuffer},
    datatypes::{DataType, SchemaRef},
    record_batch::{RecordBatch, RecordBatchOptions},
};

#[derive(Clone, Copy, Debug)]
pub(crate) enum FixedWidthKind {
    Null,
    Boolean,
    Bytes(usize),
}
pub(crate) fn fixed_width_kind(data_type: &DataType) -> Option<FixedWidthKind> {
    use FixedWidthKind::*;
    Some(match data_type {
        DataType::Null => Null,
        DataType::Boolean => Boolean,
        DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Date32 => Bytes(4),
        DataType::Int64 | DataType::UInt64 | DataType::Float64 | DataType::Date64 => Bytes(8),
        DataType::Decimal128(p, s) if (1..=38).contains(p) && *s <= *p as i8 => Bytes(16),
        DataType::Decimal256(p, s) if (1..=76).contains(p) && *s <= *p as i8 => Bytes(32),
        _ => return None,
    })
}
fn contract(message: &str) -> QueryError {
    QueryError::Execution(format!("fixed-width output contract: {message}"))
}
fn extent(buffer: &Buffer, start: usize, len: usize) -> Result<Buffer> {
    let end = start
        .checked_add(len)
        .ok_or_else(|| contract("buffer extent overflow"))?;
    if end > buffer.len() {
        return Err(contract("buffer extent out of range"));
    }
    Ok(buffer.slice_with_length(start, len))
}
fn bit_extent(buffer: &Buffer, offset: usize, rows: usize) -> Result<(Buffer, usize)> {
    let residual = offset % 8;
    let bytes = residual
        .checked_add(rows)
        .and_then(|n| n.checked_add(7))
        .map(|n| n / 8)
        .ok_or_else(|| contract("bitmap extent overflow"))?;
    Ok((extent(buffer, offset / 8, bytes)?, residual))
}
#[derive(Clone, Debug)]
pub(crate) struct FixedWidthOutputLayout {
    schema: SchemaRef,
    max_rows: usize,
    gather: GatherCopyBound,
}
impl FixedWidthOutputLayout {
    pub(crate) fn try_new(schema: SchemaRef, max_rows: usize) -> Option<Self> {
        let gather = GatherCopyBound::from_compact_fixed_width(&schema, max_rows)?;
        Some(Self {
            schema,
            max_rows,
            gather,
        })
    }
    pub(crate) fn queue_bound(&self) -> QueueCopyBound {
        self.gather.input_copy_bound()
    }
    pub(crate) fn gather_bound(&self) -> GatherCopyBound {
        self.gather.clone()
    }
    pub(crate) fn normalize(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let rows = batch.num_rows();
        if rows > self.max_rows {
            return Err(contract("reader exceeded declared row quantum"));
        }
        if batch.num_columns() != self.schema.fields().len() {
            return Err(contract("column count differs"));
        }
        let mut columns = Vec::with_capacity(batch.num_columns());
        for (array, field) in batch.columns().iter().zip(self.schema.fields()) {
            if array.data_type() != field.data_type() {
                return Err(contract("physical type differs"));
            }
            let data = array.to_data();
            if data.len() != rows || !data.child_data().is_empty() {
                return Err(contract("unexpected array shape"));
            }
            let kind = fixed_width_kind(field.data_type())
                .ok_or_else(|| contract("unsupported physical layout"))?;
            let (buffers, offset) = match kind {
                FixedWidthKind::Null => {
                    if !data.buffers().is_empty() || data.nulls().is_some() {
                        return Err(contract("unexpected Null buffers"));
                    }
                    (vec![], 0)
                }
                FixedWidthKind::Boolean => {
                    if data.buffers().len() != 1 {
                        return Err(contract("Boolean buffer count differs"));
                    }
                    let (buffer, offset) = bit_extent(&data.buffers()[0], data.offset(), rows)?;
                    // ArrayData validation also checks validity byte length
                    // using the VALUE offset, although NullBuffer has its own
                    // independent offset. Make values offset-zero safely; do
                    // not move validity bits or bypass build validation.
                    let buffer = if offset == 0 {
                        buffer
                    } else {
                        BooleanBuffer::new(buffer, offset, rows).sliced()
                    };
                    (vec![buffer], 0)
                }
                FixedWidthKind::Bytes(width) => {
                    if data.buffers().len() != 1 {
                        return Err(contract("primitive buffer count differs"));
                    }
                    let start = data
                        .offset()
                        .checked_mul(width)
                        .ok_or_else(|| contract("value offset overflow"))?;
                    let len = rows
                        .checked_mul(width)
                        .ok_or_else(|| contract("value length overflow"))?;
                    (vec![extent(&data.buffers()[0], start, len)?], 0)
                }
            };
            let nulls = data
                .nulls()
                .map(|nulls| {
                    if nulls.len() != rows {
                        return Err(contract("validity length differs"));
                    }
                    let (buffer, offset) =
                        bit_extent(nulls.buffer(), nulls.inner().offset(), rows)?;
                    Ok(NullBuffer::new(BooleanBuffer::new(buffer, offset, rows)))
                })
                .transpose()?;
            let data = ArrayData::builder(field.data_type().clone())
                .len(rows)
                .offset(offset)
                .buffers(buffers)
                .nulls(nulls)
                .build()
                .map_err(|e| contract(&format!("invalid normalized array: {e}")))?;
            columns.push(make_array(data));
        }
        RecordBatch::try_new_with_options(
            self.schema.clone(),
            columns,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )
        .map_err(|e| contract(&format!("invalid normalized batch: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::operators::spillable::owned_input_batch_charge;
    use arrow::array::{
        Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
        Int64Array, StringArray, UInt32Array,
    };
    use arrow::buffer::ScalarBuffer;
    use arrow::datatypes::i256;
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;
    fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .enumerate()
                .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, columns).unwrap()
    }
    fn check(input: RecordBatch) -> RecordBatch {
        let layout = FixedWidthOutputLayout::try_new(input.schema(), input.num_rows()).unwrap();
        let output = layout.normalize(input.clone()).unwrap();
        assert_eq!(output.num_rows(), input.num_rows());
        assert_eq!(output.columns(), input.columns());
        assert!(
            owned_input_batch_charge(&output).unwrap() <= layout.queue_bound().max_bytes().unwrap()
        );
        let indices = UInt32Array::from(
            (0..if output.num_rows() == 0 { 0 } else { 31 })
                .map(|i| (i % output.num_rows()) as u32)
                .collect::<Vec<_>>(),
        );
        let taken = RecordBatch::try_new(
            output.schema(),
            output
                .columns()
                .iter()
                .map(|a| arrow::compute::take(a.as_ref(), &indices, None).unwrap())
                .collect(),
        )
        .unwrap();
        assert!(
            owned_input_batch_charge(&taken).unwrap()
                <= layout
                    .gather_bound()
                    .gather(indices.len())
                    .unwrap()
                    .max_bytes()
                    .unwrap()
        );
        output
    }
    #[test]
    fn independent_value_and_validity_offsets_trim_large_parents() {
        for value_offset in 0..16 {
            for null_offset in 16..32 {
                let values = BooleanBuffer::collect_bool(100_000, |i| i % 3 == 0);
                let nulls = NullBuffer::new(
                    BooleanBuffer::collect_bool(100_000, |i| i % 2 == 0).slice(null_offset, 11),
                );
                let boolean =
                    BooleanArray::new(values.slice(value_offset, 11), Some(nulls.clone()));
                let numeric = Int64Array::new(
                    ScalarBuffer::from((0..1000).map(|i| i as i64).collect::<Vec<_>>())
                        .slice(127, 11),
                    Some(nulls),
                );
                let source = batch(vec![Arc::new(boolean), Arc::new(numeric)]);
                let before = owned_input_batch_charge(&source).unwrap();
                let output = check(source);
                assert!(owned_input_batch_charge(&output).unwrap() < before);
                let boolean = output
                    .column(0)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap();
                let numeric = output
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..11 {
                    assert_eq!(boolean.is_valid(row), (null_offset + row) % 2 == 0);
                    assert_eq!(numeric.is_valid(row), (null_offset + row) % 2 == 0);
                    assert_eq!(boolean.value(row), (value_offset + row) % 3 == 0);
                    assert_eq!(numeric.value(row), 127 + row as i64);
                }
                let data = output.column(0).to_data();
                assert_eq!(data.offset(), 0);
                assert_eq!(data.buffers()[0].len(), 2);
                assert!(data.nulls().unwrap().buffer().len() <= 3);
            }
        }
    }
    #[test]
    fn exact_decimal_date_null_and_empty_values_survive() {
        let source = batch(vec![
            Arc::new(
                Decimal128Array::from(vec![Some(-1234567890123), None, Some(1)])
                    .with_precision_and_scale(24, 7)
                    .unwrap(),
            ),
            Arc::new(
                Decimal256Array::from(vec![
                    Some(i256::from_i128(i128::MIN)),
                    None,
                    Some(i256::from_i128(i128::MAX)),
                ])
                .with_precision_and_scale(70, 12)
                .unwrap(),
            ),
            Arc::new(Date32Array::from(vec![Some(-365), None, Some(20000)])),
            Arc::new(Date64Array::from(vec![
                Some(-86400000),
                None,
                Some(86400000),
            ])),
            Arc::new(arrow::array::NullArray::new(3)),
            Arc::new(Int64Array::from(vec![None::<i64>; 3])),
        ]);
        check(source.clone());
        check(source.slice(1, 1));
        check(source.slice(2, 0));
    }
    #[test]
    fn exact_schema_owner_is_used_and_invalid_contracts_decline() {
        let mut name = String::with_capacity(10000);
        name.push_str("x");
        let input_schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]));
        let target = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
        let input =
            RecordBatch::try_new(input_schema, vec![Arc::new(Int64Array::from(vec![1, 2]))])
                .unwrap();
        let layout = FixedWidthOutputLayout::try_new(target.clone(), 2).unwrap();
        assert!(Arc::ptr_eq(
            &layout.normalize(input.clone()).unwrap().schema(),
            &target
        ));
        assert!(FixedWidthOutputLayout::try_new(target.clone(), 1)
            .unwrap()
            .normalize(input)
            .is_err());
        assert!(layout
            .normalize(batch(vec![Arc::new(StringArray::from(vec!["a"]))]))
            .is_err());
        assert!(layout
            .normalize(batch(vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![2]))
            ]))
            .is_err());
        assert!(FixedWidthOutputLayout::try_new(target, usize::MAX).is_none());
        for data_type in [
            DataType::Utf8,
            DataType::Binary,
            DataType::Utf8View,
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            DataType::Decimal128(0, 0),
        ] {
            assert!(FixedWidthOutputLayout::try_new(
                Arc::new(Schema::new(vec![Field::new("x", data_type, true)])),
                10
            )
            .is_none());
        }
        let buffer = Buffer::from(vec![0u8; 8]);
        assert!(extent(&buffer, usize::MAX, 1).is_err());
        assert!(extent(&buffer, 7, 2).is_err());
        assert!(bit_extent(&buffer, 0, usize::MAX).is_err());
    }
}
