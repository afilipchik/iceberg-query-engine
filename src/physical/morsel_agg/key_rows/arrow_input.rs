//! Encode retained evaluated Arrow values without intermediate owned scalars.
use super::*;
use arrow::array::*;
use arrow::datatypes::TimeUnit;

/// Dictionary encoding is physical; the bound logical value domain is stable.
/// Strip only dictionary wrappers, without weakening decimal/temporal identity.
pub(in crate::physical::morsel_agg) fn type_matches(
    mut actual: &DataType,
    expected: &DataType,
) -> bool {
    for _ in 0..=64 {
        match actual {
            DataType::Dictionary(_, value) => actual = value,
            _ => return actual == expected,
        }
    }
    false
}

/// Borrow the logical cell, checking each code before touching dictionary values.
/// NULL codes and NULL dictionary values both represent SQL NULL.
pub(in crate::physical::morsel_agg) fn resolve(
    mut array: &dyn Array,
    mut row: usize,
    mut depth: usize,
) -> Result<Option<(&dyn Array, usize, usize)>> {
    loop {
        if depth > 64 {
            return Err(invalid("dictionary nesting exceeds encoding limit"));
        }
        if row >= array.len() {
            return Err(invalid("array row out of bounds"));
        }
        if array.data_type() == &DataType::Null || array.is_null(row) {
            return Ok(None);
        }
        let DataType::Dictionary(key, _) = array.data_type() else {
            return Ok(Some((array, row, depth)));
        };
        macro_rules! dictionary {
            ($t:ty) => {{
                let dictionary = downcast::<DictionaryArray<$t>>(array)?;
                let index = usize::try_from(dictionary.keys().value(row))
                    .map_err(|_| invalid("dictionary code out of range"))?;
                (dictionary.values().as_ref(), index)
            }};
        }
        (array, row) = match key.as_ref() {
            DataType::Int8 => dictionary!(arrow::datatypes::Int8Type),
            DataType::Int16 => dictionary!(arrow::datatypes::Int16Type),
            DataType::Int32 => dictionary!(arrow::datatypes::Int32Type),
            DataType::Int64 => dictionary!(arrow::datatypes::Int64Type),
            DataType::UInt8 => dictionary!(arrow::datatypes::UInt8Type),
            DataType::UInt16 => dictionary!(arrow::datatypes::UInt16Type),
            DataType::UInt32 => dictionary!(arrow::datatypes::UInt32Type),
            DataType::UInt64 => dictionary!(arrow::datatypes::UInt64Type),
            _ => return Err(invalid("unsupported dictionary index type")),
        };
        depth += 1;
    }
}

impl KeyLayout {
    pub(in crate::physical::morsel_agg) fn validate_arrays(
        &self,
        arrays: &[ArrayRef],
    ) -> Result<()> {
        if arrays.len() != self.types.as_slice().len() {
            return Err(invalid("array arity differs from bound layout"));
        }
        for (column, (array, ty)) in arrays.iter().zip(self.types.as_slice()).enumerate() {
            if !type_matches(array.data_type(), ty.as_type()) {
                return Err(invalid(&format!(
                    "column {column}: expected {:?}, received {:?}",
                    ty.as_type(),
                    array.data_type()
                )));
            }
        }
        Ok(())
    }
}

impl KeyWorkspace {
    /// Arrays remain borrowed from the evaluated batch. The first pass validates
    /// the entire row and measures bytes; only then is destination growth admitted.
    /// Dictionary codes are resolved as borrowed logical cells; view encodings
    /// remain outside this normalized boundary.
    pub(crate) fn encode_arrays(&mut self, arrays: &[ArrayRef], row: usize) -> Result<()> {
        self.valid = false;
        if arrays.len() != self.layout.types.as_slice().len() {
            return Err(invalid("array arity differs from bound layout"));
        }
        let mut size = 0;
        for (array, ty) in arrays.iter().zip(self.layout.types.as_slice()) {
            if !type_matches(array.data_type(), ty.as_type()) {
                return Err(invalid("array differs from bound type"));
            }
            visit(array.as_ref(), row, true, 0, true, &mut |bytes| {
                size = add(size, bytes.len())?;
                Ok(())
            })?;
        }
        self.bytes.truncate(0);
        self.bytes.reserve(size)?;
        self.bytes
            .extend_reserved(size, std::iter::repeat_n(0, size))?;
        let mut writer = KeyWriter {
            bytes: self.bytes.as_mut_slice(),
            position: 0,
        };
        for array in arrays {
            visit(array.as_ref(), row, true, 0, true, &mut |bytes| {
                writer.put(bytes);
                Ok(())
            })?;
        }
        debug_assert_eq!(writer.position, size);
        self.valid = true;
        Ok(())
    }
}

fn downcast<T: Array + 'static>(array: &dyn Array) -> Result<&T> {
    array
        .as_any()
        .downcast_ref()
        .ok_or_else(|| invalid("Arrow representation differs from type"))
}

pub(in crate::physical::morsel_agg) fn visit(
    array: &dyn Array,
    row: usize,
    nullable: bool,
    depth: usize,
    canonical: bool,
    put: &mut impl FnMut(&[u8]) -> Result<()>,
) -> Result<()> {
    let Some((array, row, depth)) = resolve(array, row, depth)? else {
        if !nullable {
            return Err(invalid("NULL in non-nullable list element"));
        }
        return put(&[0]);
    };
    put(&[1])?;
    macro_rules! primitive {
        ($t:ty) => {
            put(&downcast::<$t>(array)?.value(row).to_le_bytes())
        };
    }
    match array.data_type() {
        DataType::Boolean => put(&[u8::from(downcast::<BooleanArray>(array)?.value(row))]),
        DataType::Int8 => primitive!(Int8Array),
        DataType::Int16 => primitive!(Int16Array),
        DataType::Int32 => primitive!(Int32Array),
        DataType::Int64 => primitive!(Int64Array),
        DataType::UInt8 => primitive!(UInt8Array),
        DataType::UInt16 => primitive!(UInt16Array),
        DataType::UInt32 => primitive!(UInt32Array),
        DataType::UInt64 => primitive!(UInt64Array),
        DataType::Date32 => primitive!(Date32Array),
        DataType::Date64 => primitive!(Date64Array),
        DataType::Decimal128(..) => primitive!(Decimal128Array),
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => primitive!(TimestampSecondArray),
            TimeUnit::Millisecond => primitive!(TimestampMillisecondArray),
            TimeUnit::Microsecond => primitive!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => primitive!(TimestampNanosecondArray),
        },
        DataType::Float32 => {
            let value = downcast::<Float32Array>(array)?.value(row);
            let bits = if !canonical {
                value.to_bits()
            } else if value == 0.0 {
                0
            } else if value.is_nan() {
                0x7fc0_0000
            } else {
                value.to_bits()
            };
            put(&bits.to_le_bytes())
        }
        DataType::Float64 => {
            let value = downcast::<Float64Array>(array)?.value(row);
            put(&if canonical {
                sql_float_key(value)
            } else {
                value.to_bits()
            }
            .to_le_bytes())
        }
        DataType::Utf8 => {
            let value = downcast::<StringArray>(array)?.value(row);
            put(&(value.len() as u64).to_le_bytes())?;
            put(value.as_bytes())
        }
        DataType::List(field) => {
            let array = downcast::<ListArray>(array)?;
            let offsets = array.value_offsets();
            let start =
                usize::try_from(offsets[row]).map_err(|_| invalid("negative list offset"))?;
            let end =
                usize::try_from(offsets[row + 1]).map_err(|_| invalid("negative list offset"))?;
            if start > end || end > array.values().len() {
                return Err(invalid("invalid list offsets"));
            }
            put(&((end - start) as u64).to_le_bytes())?;
            for child in start..end {
                visit(
                    array.values().as_ref(),
                    child,
                    field.is_nullable(),
                    depth + 1,
                    canonical,
                    put,
                )?;
            }
            Ok(())
        }
        _ => Err(invalid("unsupported Arrow key representation")),
    }
}

/// Read an already evaluated fixed-width logical cell directly. Variable
/// payloads and timestamp metadata stay on the selected state's admitted path.
/// This is aggregate input, so float bits must not use key canonicalization.
/// Consume a decoded value without returning a wide owning scalar through
/// Result/Option. The borrow cannot outlive this call; decoding stays shared.
pub(in crate::physical::morsel_agg) fn with_inline<T>(
    array: &dyn Array,
    row: usize,
    consume: impl FnOnce(Option<&ScalarValue>) -> Result<T>,
) -> Result<T> {
    let Some((array, row, _)) = resolve(array, row, 0)? else {
        return consume(Some(&ScalarValue::Null));
    };
    macro_rules! primitive {
        ($array:ty, $variant:ident) => {
            ScalarValue::$variant(downcast::<$array>(array)?.value(row).into())
        };
    }
    let value = match array.data_type() {
        DataType::Boolean => primitive!(BooleanArray, Boolean),
        DataType::Int8 => primitive!(Int8Array, Int8),
        DataType::Int16 => primitive!(Int16Array, Int16),
        DataType::Int32 => primitive!(Int32Array, Int32),
        DataType::Int64 => primitive!(Int64Array, Int64),
        DataType::UInt8 => primitive!(UInt8Array, UInt8),
        DataType::UInt16 => primitive!(UInt16Array, UInt16),
        DataType::UInt32 => primitive!(UInt32Array, UInt32),
        DataType::UInt64 => primitive!(UInt64Array, UInt64),
        DataType::Float32 => primitive!(Float32Array, Float32),
        DataType::Float64 => primitive!(Float64Array, Float64),
        DataType::Date32 => primitive!(Date32Array, Date32),
        DataType::Date64 => primitive!(Date64Array, Date64),
        DataType::Decimal128(_, scale) => {
            ScalarValue::Decimal128(crate::planner::DecimalValue::new(
                downcast::<Decimal128Array>(array)?.value(row),
                *scale,
            ))
        }
        DataType::Utf8 | DataType::List(_) | DataType::Timestamp(..) => return consume(None),
        _ => return Err(invalid("unsupported Arrow key representation")),
    };
    consume(Some(&value))
}

pub(in crate::physical::morsel_agg) fn inline(
    array: &dyn Array,
    row: usize,
) -> Result<Option<ScalarValue>> {
    with_inline(array, row, |value| Ok(value.cloned()))
}

pub(in crate::physical::morsel_agg) fn encode_scalar(
    array: &dyn Array,
    row: usize,
    scratch: &mut ReservedVec<u8>,
) -> Result<()> {
    let mut size = 0;
    visit(array, row, true, 0, false, &mut |part| {
        size = add(size, part.len())?;
        Ok(())
    })?;
    scratch.truncate(0);
    scratch.reserve(size)?;
    scratch.extend_reserved(size, std::iter::repeat_n(0, size))?;
    let mut writer = KeyWriter {
        bytes: scratch.as_mut_slice(),
        position: 0,
    };
    visit(array, row, true, 0, false, &mut |part| {
        writer.put(part);
        Ok(())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::DecimalValue;
    use arrow::datatypes::Int64Type;

    #[test]
    fn borrowed_inline_checks_before_consuming_and_never_replays_errors() {
        let array = Float64Array::from(vec![Some(-0.0), None, Some(1.25)]);
        let mut calls = 0;
        let mut seen = Vec::new();
        for row in [2, 0, 2, 1] {
            with_inline(&array, row, |value| {
                calls += 1;
                seen.push(match value.unwrap() {
                    ScalarValue::Float64(v) => Some(v.into_inner().to_bits()),
                    ScalarValue::Null => None,
                    _ => panic!("unexpected numeric domain"),
                });
                Ok(())
            })
            .unwrap();
        }
        assert_eq!(
            seen,
            [
                Some(1.25f64.to_bits()),
                Some((-0.0f64).to_bits()),
                Some(1.25f64.to_bits()),
                None
            ]
        );
        assert_eq!(calls, 4);
        assert!(with_inline(&array, 3, |_| {
            calls += 1;
            Ok(())
        })
        .is_err());
        assert_eq!(calls, 4, "invalid rows cannot reach the consumer");
        let error = with_inline(&array, 0, |_| -> Result<()> {
            calls += 1;
            Err(QueryError::Execution("consumer failure sentinel".into()))
        })
        .unwrap_err();
        assert_eq!(calls, 5, "a consumer failure must never replay its input");
        assert!(error.to_string().contains("consumer failure sentinel"));
        let strings = StringArray::from(vec!["selected payload"]);
        with_inline(&strings, 0, |value| {
            assert!(value.is_none());
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn inline_fixed_inputs_preserve_widths_nulls_and_slices() {
        macro_rules! check {
            ($array:ty, $variant:ident, $lo:expr, $hi:expr) => {{
                let array = <$array>::from(vec![Some($hi), Some($lo), None, Some($hi)]);
                let sliced = array.slice(1, 3);
                let expected = [
                    ScalarValue::$variant(($lo).into()),
                    ScalarValue::Null,
                    ScalarValue::$variant(($hi).into()),
                ];
                for row in [2, 0, 2, 1] {
                    assert_eq!(inline(&sliced, row).unwrap(), Some(expected[row].clone()));
                }
                assert!(inline(&sliced, 3).is_err());
            }};
        }
        check!(BooleanArray, Boolean, false, true);
        check!(Int8Array, Int8, i8::MIN, i8::MAX);
        check!(Int16Array, Int16, i16::MIN, i16::MAX);
        check!(Int32Array, Int32, i32::MIN, i32::MAX);
        check!(Int64Array, Int64, i64::MIN, i64::MAX);
        check!(UInt8Array, UInt8, 0u8, u8::MAX);
        check!(UInt16Array, UInt16, 0u16, u16::MAX);
        check!(UInt32Array, UInt32, 0u32, u32::MAX);
        check!(UInt64Array, UInt64, 0u64, u64::MAX);
        check!(Date32Array, Date32, i32::MIN, i32::MAX);
        check!(Date64Array, Date64, i64::MIN, i64::MAX);
        let coefficient = 10i128.pow(38) - 1;
        for scale in [-3, 0, 9] {
            let array = Decimal128Array::from(vec![Some(coefficient), None, Some(-coefficient)])
                .with_precision_and_scale(38, scale)
                .unwrap();
            assert_eq!(
                inline(&array, 0).unwrap(),
                Some(ScalarValue::Decimal128(DecimalValue::new(
                    coefficient,
                    scale
                )))
            );
            assert_eq!(inline(&array, 1).unwrap(), Some(ScalarValue::Null));
            assert_eq!(
                inline(&array, 2).unwrap(),
                Some(ScalarValue::Decimal128(DecimalValue::new(
                    -coefficient,
                    scale
                )))
            );
        }
        assert_eq!(
            inline(&NullArray::new(1), 0).unwrap(),
            Some(ScalarValue::Null)
        );
        assert!(inline(&NullArray::new(0), 0).is_err());
    }

    #[test]
    fn inline_float_values_keep_noncanonical_bits() {
        let bits32 = [
            0x8000_0000,
            0x7f80_0000,
            0xff80_0000,
            0x7fc0_0017,
            0xffc0_0029,
            1,
        ];
        let array32 = Float32Array::from(bits32.map(f32::from_bits).to_vec());
        for (row, expected) in bits32.into_iter().enumerate() {
            let Some(ScalarValue::Float32(value)) = inline(&array32, row).unwrap() else {
                panic!("expected Float32")
            };
            assert_eq!(value.0.to_bits(), expected);
        }
        let bits64 = [
            0x8000_0000_0000_0000,
            0x7ff0_0000_0000_0000,
            0xfff0_0000_0000_0000,
            0x7ff8_0000_0000_0017,
            0xfff8_0000_0000_0029,
            1,
        ];
        let array64 = Float64Array::from(bits64.map(f64::from_bits).to_vec());
        for (row, expected) in bits64.into_iter().enumerate() {
            let Some(ScalarValue::Float64(value)) = inline(&array64, row).unwrap() else {
                panic!("expected Float64")
            };
            assert_eq!(value.0.to_bits(), expected);
        }
        assert_eq!(
            inline(&Float64Array::from(vec![None]), 0).unwrap(),
            Some(ScalarValue::Null)
        );
    }

    #[test]
    fn inline_dictionary_values_and_admitted_fallbacks_stay_distinct() {
        macro_rules! check {
            ($key:ty, $native:ty) => {{
                let array = DictionaryArray::<$key>::try_new(
                    PrimitiveArray::<$key>::from(vec![
                        Some(1 as $native),
                        None,
                        Some(0 as $native),
                        Some(1 as $native),
                    ]),
                    Arc::new(Int64Array::from(vec![None, Some(i64::MIN)])),
                )
                .unwrap();
                let expected = [
                    ScalarValue::Int64(i64::MIN),
                    ScalarValue::Null,
                    ScalarValue::Null,
                    ScalarValue::Int64(i64::MIN),
                ];
                for (row, value) in expected.into_iter().enumerate() {
                    assert_eq!(inline(&array, row).unwrap(), Some(value));
                }
                assert!(inline(&array, 4).is_err());
            }};
        }
        check!(arrow::datatypes::Int8Type, i8);
        check!(arrow::datatypes::Int16Type, i16);
        check!(arrow::datatypes::Int32Type, i32);
        check!(arrow::datatypes::Int64Type, i64);
        check!(arrow::datatypes::UInt8Type, u8);
        check!(arrow::datatypes::UInt16Type, u16);
        check!(arrow::datatypes::UInt32Type, u32);
        check!(arrow::datatypes::UInt64Type, u64);
        assert!(inline(&StringArray::from(vec!["value"]), 0)
            .unwrap()
            .is_none());
        assert!(inline(
            &TimestampNanosecondArray::from(vec![1]).with_timezone("UTC"),
            0
        )
        .unwrap()
        .is_none());
        assert!(inline(
            &ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(1)])]),
            0
        )
        .unwrap()
        .is_none());
        assert!(inline(&BinaryArray::from(vec![b"unsupported".as_slice()]), 0).is_err());
    }

    #[test]
    fn dictionary_index_widths_resolve_values_and_both_null_kinds() {
        macro_rules! check {
            ($t:ty,$native:ty) => {{
                let pool = MemoryPool::new_named("dictionary key", 65536);
                let layout = KeyLayout::bind(&pool, &[DataType::Utf8]).unwrap().unwrap();
                let mut key = KeyWorkspace::new(layout.clone()).unwrap();
                let array: ArrayRef = Arc::new(
                    DictionaryArray::<$t>::try_new(
                        PrimitiveArray::<$t>::from(vec![
                            Some(0 as $native),
                            Some(1 as $native),
                            None,
                        ]),
                        Arc::new(StringArray::from(vec![Some("value"), None])),
                    )
                    .unwrap(),
                );
                for row in 0..3 {
                    key.encode_arrays(std::slice::from_ref(&array), row)
                        .unwrap();
                    let mut expected = vec![0];
                    if row == 0 {
                        expected = vec![1];
                        expected.extend(5u64.to_le_bytes());
                        expected.extend(b"value");
                    }
                    assert_eq!(key.key().unwrap().bytes(), expected);
                }
                drop((key, layout));
                assert_eq!(pool.used(), 0);
            }};
        }
        check!(arrow::datatypes::Int8Type, i8);
        check!(arrow::datatypes::Int16Type, i16);
        check!(arrow::datatypes::Int32Type, i32);
        check!(arrow::datatypes::Int64Type, i64);
        check!(arrow::datatypes::UInt8Type, u8);
        check!(arrow::datatypes::UInt16Type, u16);
        check!(arrow::datatypes::UInt32Type, u32);
        check!(arrow::datatypes::UInt64Type, u64);
    }

    #[test]
    fn borrowed_arrow_keys_match_scalar_wire_values_and_sliced_offsets() {
        let pool = MemoryPool::new_named("Arrow keys", 131072);
        let list = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
            Some(vec![Some(99)]),
            Some(vec![Some(4), None, Some(-7)]),
            Some(vec![]),
            None,
        ])) as ArrayRef;
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(vec![
                Some("skip"),
                Some("hello"),
                Some(""),
                None,
            ])),
            list,
            Arc::new(Float64Array::from(vec![
                Some(99.0),
                Some(-0.0),
                Some(f64::from_bits(0xfff8_0000_0000_0017)),
                None,
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(0), Some((1i128 << 80) + 17), Some(-17), None])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
        ];
        let arrays: Vec<ArrayRef> = arrays.into_iter().map(|a| a.slice(1, 3)).collect();
        let types: Vec<_> = arrays.iter().map(|a| a.data_type().clone()).collect();
        let layout = KeyLayout::bind(&pool, &types).unwrap().unwrap();
        let mut actual = KeyWorkspace::new(layout.clone()).unwrap();
        let mut expected = KeyWorkspace::new(layout.clone()).unwrap();
        let field = Box::new(DataType::Int64);
        let values = [
            vec![
                ScalarValue::Utf8("hello".into()),
                ScalarValue::List(
                    vec![
                        ScalarValue::Int64(4),
                        ScalarValue::Null,
                        ScalarValue::Int64(-7),
                    ],
                    field.clone(),
                ),
                ScalarValue::Float64((-0.0).into()),
                ScalarValue::Decimal128(DecimalValue::new((1i128 << 80) + 17, 2)),
            ],
            vec![
                ScalarValue::Utf8("".into()),
                ScalarValue::List(vec![], field),
                ScalarValue::Float64(f64::NAN.into()),
                ScalarValue::Decimal128(DecimalValue::new(-17, 2)),
            ],
            vec![ScalarValue::Null; 4],
        ];
        for (row, scalars) in values.iter().enumerate() {
            actual.encode_arrays(&arrays, row).unwrap();
            expected.encode(scalars).unwrap();
            assert_eq!(
                actual.key().unwrap().bytes(),
                expected.key().unwrap().bytes()
            );
            layout
                .validate_encoded(actual.key().unwrap().bytes())
                .unwrap();
        }
        drop((actual, expected, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn logical_null_arrays_and_timestamp_domains_are_preserved() {
        let pool = MemoryPool::new_named("Arrow logical domains", 65536);
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(NullArray::new(2)),
            Arc::new(TimestampNanosecondArray::from(vec![Some(-17), None]).with_timezone("UTC")),
            Arc::new(Float32Array::from(vec![-0.0, f32::from_bits(0xffc0_0017)])),
        ];
        let types: Vec<_> = arrays.iter().map(|a| a.data_type().clone()).collect();
        let layout = KeyLayout::bind(&pool, &types).unwrap().unwrap();
        let mut key = KeyWorkspace::new(layout.clone()).unwrap();
        key.encode_arrays(&arrays, 0).unwrap();
        let mut expected = vec![0, 1];
        expected.extend((-17i64).to_le_bytes());
        expected.push(1);
        expected.extend(0u32.to_le_bytes());
        assert_eq!(key.key().unwrap().bytes(), expected);
        key.encode_arrays(&arrays, 1).unwrap();
        let mut expected = vec![0, 0, 1];
        expected.extend(0x7fc0_0000u32.to_le_bytes());
        assert_eq!(key.key().unwrap().bytes(), expected);
        let wrong: Vec<ArrayRef> = vec![
            arrays[0].clone(),
            Arc::new(TimestampNanosecondArray::from(vec![0, 1])),
            arrays[2].clone(),
        ];
        assert!(key.encode_arrays(&wrong, 0).is_err());
        assert!(key.key().is_err());
        drop((key, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn arrow_key_admission_and_invalid_rows_never_expose_previous_key() {
        let pool = MemoryPool::new_named("Arrow key refusal", 65536);
        let layout = KeyLayout::bind(&pool, &[DataType::Utf8]).unwrap().unwrap();
        let mut key = KeyWorkspace::new(layout.clone()).unwrap();
        key.encode(&[ScalarValue::Null]).unwrap();
        let arrays: [ArrayRef; 1] = [Arc::new(StringArray::from(vec!["x".repeat(4096)]))];
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(key.encode_arrays(&arrays, 0).unwrap_err().is_memory_limit());
        assert!(key.key().is_err());
        // Invalid input is diagnosed before attempting destination admission.
        assert!(!key.encode_arrays(&arrays, 1).unwrap_err().is_memory_limit());
        assert!(key.key().is_err());
        drop(pressure);
        key.encode_arrays(&arrays, 0).unwrap();
        let used = pool.used();
        let pressure = pool.allocate(pool.available()).unwrap();
        key.encode_arrays(&arrays, 0).unwrap(); // reuse does not need a fresh lease
        assert!(key.encode_arrays(&[], 0).is_err());
        assert!(key.key().is_err());
        drop(pressure);
        assert_eq!(pool.used(), used);
        drop((key, layout));
        assert_eq!(pool.used(), 0);
    }
}
