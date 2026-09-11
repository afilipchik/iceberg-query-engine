//! Contiguous, admission-owned copies without an identity row-index vector.
use super::*;
fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("admitted range copy: {message}"))
}
fn bits(
    input: &BooleanBuffer,
    start: usize,
    rows: usize,
    pool: &MemoryPool,
) -> Result<BooleanBuffer> {
    if start.checked_add(rows).is_none_or(|end| end > input.len()) {
        return Err(invalid("bitmap range outside input"));
    }
    let first = input
        .offset()
        .checked_add(start)
        .ok_or_else(|| invalid("bitmap offset overflow"))?;
    let end = first
        .checked_add(rows)
        .ok_or_else(|| invalid("bitmap extent overflow"))?;
    if end.div_ceil(8) > input.values().len() {
        return Err(invalid("bitmap storage is truncated"));
    }
    let bytes = rows.div_ceil(8);
    let mut output = ReservedBufferBuilder::<u8>::with_capacity(pool, bytes)?;
    if first % 8 == 0 {
        output.extend_reserved(
            bytes,
            input.values()[first / 8..first / 8 + bytes].iter().copied(),
        )?;
    } else {
        output.extend_reserved(
            bytes,
            (0..bytes).map(|i| {
                let index = first / 8 + i;
                let word = u16::from(input.values()[index])
                    | (u16::from(input.values().get(index + 1).copied().unwrap_or(0)) << 8);
                (word >> (first % 8)) as u8
            }),
        )?;
    }
    if rows % 8 != 0 {
        *output.as_mut_slice().last_mut().unwrap() &= (1 << (rows % 8)) - 1;
    }
    Ok(BooleanBuffer::new(output.finish(), 0, rows))
}
fn nulls(
    input: &dyn Array,
    start: usize,
    rows: usize,
    pool: &MemoryPool,
) -> Result<Option<NullBuffer>> {
    input
        .nulls()
        .filter(|n| n.null_count() != 0)
        .map(|n| bits(n.inner(), start, rows, pool).map(NullBuffer::new))
        .transpose()
}
fn primitive<T: ArrowPrimitiveType>(
    array: &ArrayRef,
    start: usize,
    rows: usize,
    pool: &MemoryPool,
) -> Result<ArrayRef> {
    let input = array
        .as_any()
        .downcast_ref::<PrimitiveArray<T>>()
        .ok_or_else(|| invalid("primitive representation differs"))?;
    let nulls = nulls(input, start, rows, pool)?;
    let values = input
        .values()
        .get(start..start + rows)
        .ok_or_else(|| invalid("primitive range outside input"))?;
    let mut output = ReservedBufferBuilder::<T::Native>::with_capacity(pool, rows)?;
    output.extend_reserved(rows, values.iter().copied())?;
    Ok(Arc::new(
        PrimitiveArray::<T>::new(ScalarBuffer::new(output.finish(), 0, rows), nulls)
            .with_data_type(input.data_type().clone()),
    ))
}
fn column(array: &ArrayRef, start: usize, rows: usize, pool: &MemoryPool) -> Result<ArrayRef> {
    macro_rules! p {
        ($ty:ty) => {
            primitive::<$ty>(array, start, rows, pool)
        };
    }
    match array.data_type() {
        DataType::Int8 => p!(Int8Type),
        DataType::Int16 => p!(Int16Type),
        DataType::Int32 => p!(Int32Type),
        DataType::Int64 => p!(Int64Type),
        DataType::UInt8 => p!(UInt8Type),
        DataType::UInt16 => p!(UInt16Type),
        DataType::UInt32 => p!(UInt32Type),
        DataType::UInt64 => p!(UInt64Type),
        DataType::Float32 => p!(Float32Type),
        DataType::Float64 => p!(Float64Type),
        DataType::Date32 => p!(Date32Type),
        DataType::Date64 => p!(Date64Type),
        DataType::Decimal128(_, _) => p!(Decimal128Type),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => p!(TimestampSecondType),
            TimeUnit::Millisecond => p!(TimestampMillisecondType),
            TimeUnit::Microsecond => p!(TimestampMicrosecondType),
            TimeUnit::Nanosecond => p!(TimestampNanosecondType),
        },
        DataType::Boolean => {
            let input = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| invalid("Boolean representation differs"))?;
            Ok(Arc::new(BooleanArray::new(
                bits(input.values(), start, rows, pool)?,
                nulls(input, start, rows, pool)?,
            )))
        }
        DataType::Utf8 => {
            let input = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| invalid("UTF8 representation differs"))?;
            let end = start
                .checked_add(rows)
                .ok_or_else(|| invalid("offset extent overflow"))?;
            let offsets = input
                .value_offsets()
                .get(start..=end)
                .ok_or_else(|| invalid("UTF8 offsets outside input"))?;
            let first = usize::try_from(offsets[0]).map_err(|_| invalid("negative UTF8 offset"))?;
            let last =
                usize::try_from(offsets[rows]).map_err(|_| invalid("negative UTF8 offset"))?;
            let values = input
                .value_data()
                .get(first..last)
                .ok_or_else(|| invalid("UTF8 values outside input"))?;
            let mut copied_offsets = ReservedBufferBuilder::<i32>::with_capacity(
                pool,
                rows.checked_add(1)
                    .ok_or_else(|| invalid("offset count overflow"))?,
            )?;
            copied_offsets.try_extend_reserved(
                rows + 1,
                offsets.iter().map(|offset| {
                    offset
                        .checked_sub(offsets[0])
                        .filter(|n| *n >= 0)
                        .ok_or_else(|| invalid("invalid UTF8 offset"))
                }),
            )?;
            let mut copied_values = ReservedBufferBuilder::<u8>::with_capacity(pool, values.len())?;
            copied_values.extend_reserved(values.len(), values.iter().copied())?;
            Ok(Arc::new(StringArray::try_new(
                OffsetBuffer::new(ScalarBuffer::new(copied_offsets.finish(), 0, rows + 1)),
                copied_values.finish(),
                nulls(input, start, rows, pool)?,
            )?))
        }
        _ => Err(invalid("unsupported contiguous type")),
    }
}
pub(crate) fn copy_range(
    source: &RecordBatch,
    start: usize,
    rows: usize,
    columns: &[usize],
    schema: SchemaRef,
    pool: &MemoryPool,
) -> Result<RecordBatch> {
    if start
        .checked_add(rows)
        .is_none_or(|end| end > source.num_rows())
    {
        return Err(invalid("range outside source"));
    }
    if columns.len() != schema.fields().len() {
        return Err(invalid("schema width differs"));
    }
    let mut encoded = false;
    for (column, field) in columns.iter().zip(schema.fields()) {
        let array = source
            .columns()
            .get(*column)
            .ok_or_else(|| invalid("column outside source"))?;
        let mut actual = array.data_type();
        while let DataType::Dictionary(_, value) = actual {
            encoded = true;
            actual = value.as_ref();
        }
        if actual != field.data_type() || !supported(actual) {
            return Err(invalid("logical type differs"));
        }
    }
    if encoded {
        let mut selection = ReservedVec::with_capacity(pool, rows)?;
        selection.extend_reserved(rows, (start..start + rows).map(|row| Some((0, row))))?;
        return gather(
            std::slice::from_ref(source),
            selection.as_slice(),
            columns,
            schema,
            pool,
        );
    }
    let mut arrays = ReservedVec::with_capacity(pool, columns.len())?;
    for index in columns {
        arrays.extend_reserved(1, [column(source.column(*index), start, rows, pool)?])?;
    }
    crate::storage::admitted_batch::finish(schema, rows, arrays, pool)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn packed_ranges_preserve_sliced_bits_and_empty_extents() {
        let pool = MemoryPool::new(128 * 1024);
        let original = BooleanBuffer::from_iter((0..80).map(|i| i % 3 == 0));
        let sliced = original.slice(3, 65);
        for start in 0..=65 {
            for rows in 0..=65 - start {
                let copied = bits(&sliced, start, rows, &pool).unwrap();
                assert_eq!(
                    copied.iter().collect::<Vec<_>>(),
                    (0..rows)
                        .map(|i| (3 + start + i) % 3 == 0)
                        .collect::<Vec<_>>()
                );
                if rows % 8 != 0 {
                    assert_eq!(copied.values().last().unwrap() >> (rows % 8), 0);
                }
            }
        }
        assert!(bits(&sliced, usize::MAX, 2, &pool).is_err());
        assert!(bits(&sliced, 65, 1, &pool).is_err());
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn contiguous_typed_values_nulls_and_buffer_ownership() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int8Array::from(vec![
                Some(7),
                None,
                Some(i8::MIN),
                Some(i8::MAX),
            ])),
            Arc::new(UInt64Array::from(vec![
                Some(0),
                Some(u64::MAX),
                None,
                Some(1),
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(0), Some(-1234567), None, Some(i128::MAX / 100)])
                    .with_precision_and_scale(38, 7)
                    .unwrap(),
            ),
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(f64::from_bits(0x7ff8000000000042)),
                Some(-0.0),
                None,
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(false),
                Some(true),
                None,
                Some(false),
            ])),
            Arc::new(StringArray::from(vec![
                Some("prefix"),
                Some("é🦆"),
                None,
                Some(""),
            ])),
            Arc::new(
                TimestampNanosecondArray::from(vec![Some(0), Some(-1), None, Some(42)])
                    .with_timezone("UTC"),
            ),
        ];
        let schema = Arc::new(Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        let input = RecordBatch::try_new(schema.clone(), arrays)
            .unwrap()
            .slice(1, 3);
        let pool = MemoryPool::new(128 * 1024);
        let out = copy_range(&input, 0, 3, &[0, 1, 2, 3, 4, 5, 6], schema, &pool).unwrap();
        macro_rules! values {
            ($col:expr, $ty:ty, $expected:expr) => {
                assert_eq!(
                    out.column($col)
                        .as_any()
                        .downcast_ref::<$ty>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                    $expected
                );
            };
        }
        values!(0, Int8Array, vec![None, Some(i8::MIN), Some(i8::MAX)]);
        values!(1, UInt64Array, vec![Some(u64::MAX), None, Some(1)]);
        values!(
            2,
            Decimal128Array,
            vec![Some(-1234567), None, Some(i128::MAX / 100)]
        );
        let floats = out
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(
            floats
                .iter()
                .map(|v| v.map(f64::to_bits))
                .collect::<Vec<_>>(),
            vec![Some(0x7ff8000000000042), Some(1u64 << 63), None]
        );
        values!(4, BooleanArray, vec![Some(true), None, Some(false)]);
        values!(5, StringArray, vec![Some("é🦆"), None, Some("")]);
        values!(6, TimestampNanosecondArray, vec![Some(-1), None, Some(42)]);
        assert_eq!(out.schema(), input.schema());
        let retained = out.column(5).slice(0, 1);
        drop(out);
        assert!(pool.used() > 0);
        drop(retained);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn dictionary_range_and_refusal_leave_no_reservations() {
        let dictionary = DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), None, Some(1), Some(2)]),
            Arc::new(StringArray::from(vec![Some("a"), None, Some("z")])),
        )
        .unwrap();
        let input =
            RecordBatch::try_from_iter(vec![("s", Arc::new(dictionary) as ArrayRef)]).unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let pool = MemoryPool::new(128 * 1024);
        let output = copy_range(&input, 1, 3, &[0], schema.clone(), &pool).unwrap();
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, None, Some("z")]
        );
        drop(output);
        assert_eq!(pool.used(), 0);
        let long = "x".repeat(20000);
        let input = RecordBatch::try_from_iter(vec![(
            "s",
            Arc::new(StringArray::from(vec![long.as_str()])) as ArrayRef,
        )])
        .unwrap();
        let small = MemoryPool::new(4096);
        assert!(matches!(
            copy_range(&input, 0, 1, &[0], schema.clone(), &small),
            Err(QueryError::MemoryLimit { .. })
        ));
        assert_eq!(small.used(), 0);
        assert!(copy_range(&input, usize::MAX, 2, &[0], schema.clone(), &pool).is_err());
        assert!(copy_range(&input, 0, 1, &[1], schema, &pool).is_err());
        assert_eq!(pool.used(), 0);
    }
}
