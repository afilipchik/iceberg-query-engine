use super::*;

fn fixture() -> RecordBatch {
    macro_rules! ints {
        ($ty:ty) => {
            Arc::new(PrimitiveArray::<$ty>::from(vec![
                Some(0),
                Some(3),
                None,
                Some(7),
                Some(9),
                Some(0),
            ])) as ArrayRef
        };
    }
    let arrays: Vec<ArrayRef> = vec![
        ints!(Int8Type),
        ints!(Int16Type),
        ints!(Int32Type),
        ints!(Int64Type),
        ints!(UInt8Type),
        ints!(UInt16Type),
        ints!(UInt32Type),
        ints!(UInt64Type),
        Arc::new(Float32Array::from(vec![
            Some(0.0),
            Some(-0.0),
            None,
            Some(f32::INFINITY),
            Some(1.5),
            Some(0.0),
        ])),
        Arc::new(Float64Array::from(vec![
            Some(0.0),
            Some(-0.0),
            Some(f64::from_bits(0x7ff8000000000042)),
            Some(f64::INFINITY),
            None,
            Some(0.0),
        ])),
        ints!(Date32Type),
        ints!(Date64Type),
        Arc::new(
            Decimal128Array::from(vec![
                Some(0),
                Some(10i128.pow(30)),
                None,
                Some(-10i128.pow(30)),
                Some(3),
                Some(0),
            ])
            .with_precision_and_scale(38, -2)
            .unwrap(),
        ),
        ints!(TimestampSecondType),
        ints!(TimestampMillisecondType),
        ints!(TimestampMicrosecondType),
        Arc::new(
            TimestampNanosecondArray::from(vec![
                Some(0),
                Some(-1),
                None,
                Some(i64::MAX),
                Some(3),
                Some(0),
            ])
            .with_timezone("UTC"),
        ),
        Arc::new(BooleanArray::from(vec![
            Some(false),
            Some(true),
            None,
            Some(false),
            Some(true),
            Some(false),
        ])),
        Arc::new(StringArray::from(vec![
            Some("skip"),
            Some("é"),
            None,
            Some(""),
            Some("abc"),
            Some("skip"),
        ])),
    ];
    let schema = Arc::new(Schema::new(
        arrays
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
            .collect::<Vec<_>>(),
    ));
    RecordBatch::try_new(schema, arrays).unwrap().slice(1, 4)
}

#[test]
fn typed_sliced_chunks_preserve_values_and_pre_reserved_finish() {
    let batch = fixture();
    let pool = MemoryPool::new(1 << 20);
    let mut accumulator = BatchAccumulator::new(batch.schema(), 3, 16, &pool).unwrap();
    let first = batch.slice(0, 2);
    let second = batch.slice(2, 2);
    assert_eq!(accumulator.append(&first, 0).unwrap(), 2);
    assert_eq!(accumulator.append(&second, 0).unwrap(), 1);
    assert!(accumulator.full());
    assert_eq!(accumulator.append(&second, 1).unwrap(), 0);
    // Filling the rest of the pool proves finish needs no additional pool lease.
    let blocker = pool.allocate((1 << 20) - pool.used()).unwrap();
    let output = accumulator.finish().unwrap();
    assert_eq!(output.schema(), batch.schema());
    for (i, a) in output.columns().iter().enumerate() {
        let tail = second.column(i).slice(0, 1);
        let expected = arrow::compute::concat(&[first.column(i).as_ref(), tail.as_ref()]).unwrap();
        assert_eq!(a.to_data(), expected.to_data(), "column {i}");
    }
    let floats = output
        .column(9)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    assert_eq!(floats.value(0).to_bits(), (-0.0f64).to_bits());
    assert_eq!(floats.value(1).to_bits(), 0x7ff8000000000042);
    let held = output.column(18).slice(0, 1);
    drop(blocker);
    drop(output);
    assert!(pool.used() > 0);
    drop(held);
    assert_eq!(pool.used(), 0);
}

#[test]
fn utf8_byte_boundary_preserves_exact_unconsumed_prefix() {
    let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(StringArray::from(vec![
            Some("aa"),
            None,
            Some("bbb"),
            Some("oversized"),
        ]))],
    )
    .unwrap();
    let pool = MemoryPool::new(1 << 20);
    let mut first = BatchAccumulator::new(schema.clone(), 8, 4, &pool).unwrap();
    assert_eq!(first.append(&batch, 0).unwrap(), 2);
    assert_eq!(first.append(&batch, 2).unwrap(), 0);
    assert_eq!(first.rows(), 2);
    let output = first.finish().unwrap();
    assert_eq!(
        output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap(),
        &StringArray::from(vec![Some("aa"), None])
    );
    drop(output);
    let mut second = BatchAccumulator::new(schema.clone(), 8, 4, &pool).unwrap();
    assert_eq!(second.append(&batch, 2).unwrap(), 1);
    drop(second.finish().unwrap());
    let mut oversized = BatchAccumulator::new(schema, 8, 4, &pool).unwrap();
    assert_eq!(oversized.append(&batch, 3).unwrap(), 0);
    assert_eq!(oversized.rows(), 0);
    drop(oversized);
    assert_eq!(pool.used(), 0);
}

#[test]
fn refusal_and_invalid_input_leave_ownership_and_cursor_intact() {
    let batch = fixture();
    let tiny = MemoryPool::new(8192);
    assert!(
        matches!(BatchAccumulator::new(batch.schema(),32,16,&tiny), Err(e) if e.is_memory_limit())
    );
    assert_eq!(tiny.used(), 0);
    let pool = MemoryPool::new(1 << 20);
    let mut accumulator = BatchAccumulator::new(batch.schema(), 4, 16, &pool).unwrap();
    let charge = pool.used();
    assert!(accumulator.append(&batch, 5).is_err());
    assert_eq!(accumulator.rows(), 0);
    assert_eq!(pool.used(), charge);
    assert_eq!(accumulator.append(&batch, 0).unwrap(), 4);
    drop(accumulator.finish().unwrap());
    assert_eq!(pool.used(), 0);
}

#[test]
fn zero_column_rows_and_zero_length_finish_are_exact() {
    let pool = MemoryPool::new(65536);
    let schema = Arc::new(Schema::empty());
    let batch = RecordBatch::try_new_with_options(
        schema.clone(),
        vec![],
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(5)),
    )
    .unwrap();
    let mut accumulator = BatchAccumulator::new(schema.clone(), 3, 4, &pool).unwrap();
    assert_eq!(accumulator.append(&batch, 1).unwrap(), 3);
    let output = accumulator.finish().unwrap();
    assert_eq!(output.num_rows(), 3);
    assert_eq!(output.num_columns(), 0);
    drop(output);
    let empty = BatchAccumulator::new(schema, 3, 4, &pool)
        .unwrap()
        .finish()
        .unwrap();
    assert_eq!(empty.num_rows(), 0);
    drop(empty);
    assert_eq!(pool.used(), 0);
}
