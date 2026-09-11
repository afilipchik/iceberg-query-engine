//! Borrowed key bindings for a retained evaluated batch. Bytes stay canonical.
use super::*;
use arrow::array::*;
use arrow::buffer::NullBuffer;
use arrow::datatypes::TimeUnit;

macro_rules! primitives {
    ($($variant:ident: $array:ty => $width:expr),+ $(,)?) => {
        enum Fixed<'a> { $($variant(&'a $array)),+, Boolean(&'a BooleanArray),
            Float32(&'a Float32Array), Float64(&'a Float64Array), Null }
        impl Fixed<'_> {
            fn width(&self) -> usize {
                match self {
                    $(Self::$variant(_) => $width),+,
                    Self::Boolean(_) => 1, Self::Float32(_) => 4, Self::Float64(_) => 8,
                    Self::Null => 0,
                }
            }
            fn put(&self, row: usize, put: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
                match self {
                    $(Self::$variant(a) => put(&a.value(row).to_le_bytes())),+,
                    Self::Boolean(a) => put(&[u8::from(a.value(row))]),
                    Self::Float32(a) => {
                        let value = a.value(row);
                        let bits = if value == 0.0 { 0 } else if value.is_nan() { 0x7fc0_0000 } else { value.to_bits() };
                        put(&bits.to_le_bytes())
                    }
                    Self::Float64(a) => put(&sql_float_key(a.value(row)).to_le_bytes()),
                    Self::Null => Ok(()),
                }
            }
        }
    }
}
primitives! {
    Int8: Int8Array => 1, Int16: Int16Array => 2, Int32: Int32Array => 4, Int64: Int64Array => 8,
    UInt8: UInt8Array => 1, UInt16: UInt16Array => 2, UInt32: UInt32Array => 4, UInt64: UInt64Array => 8,
    Date32: Date32Array => 4, Date64: Date64Array => 8, Decimal128: Decimal128Array => 16,
    TimestampSecond: TimestampSecondArray => 8, TimestampMillisecond: TimestampMillisecondArray => 8,
    TimestampMicrosecond: TimestampMicrosecondArray => 8, TimestampNanosecond: TimestampNanosecondArray => 8,
}

enum Column<'a> {
    Fixed {
        value: Fixed<'a>,
        nulls: Option<&'a NullBuffer>,
    },
    Checked(&'a dyn Array),
}
impl<'a> Column<'a> {
    fn bind(array: &'a dyn Array) -> Result<Self> {
        fn cast<T: Array + 'static>(a: &dyn Array) -> Result<&T> {
            a.as_any()
                .downcast_ref::<T>()
                .ok_or_else(|| invalid("Arrow representation differs from type"))
        }
        macro_rules! bind {
            ($variant:ident, $array:ty) => {
                Fixed::$variant(cast::<$array>(array)?)
            };
        }
        let value = match array.data_type() {
            DataType::Null => Fixed::Null,
            DataType::Boolean => bind!(Boolean, BooleanArray),
            DataType::Int8 => bind!(Int8, Int8Array),
            DataType::Int16 => bind!(Int16, Int16Array),
            DataType::Int32 => bind!(Int32, Int32Array),
            DataType::Int64 => bind!(Int64, Int64Array),
            DataType::UInt8 => bind!(UInt8, UInt8Array),
            DataType::UInt16 => bind!(UInt16, UInt16Array),
            DataType::UInt32 => bind!(UInt32, UInt32Array),
            DataType::UInt64 => bind!(UInt64, UInt64Array),
            DataType::Float32 => bind!(Float32, Float32Array),
            DataType::Float64 => bind!(Float64, Float64Array),
            DataType::Date32 => bind!(Date32, Date32Array),
            DataType::Date64 => bind!(Date64, Date64Array),
            DataType::Decimal128(..) => bind!(Decimal128, Decimal128Array),
            DataType::Timestamp(unit, _) => match unit {
                TimeUnit::Second => bind!(TimestampSecond, TimestampSecondArray),
                TimeUnit::Millisecond => bind!(TimestampMillisecond, TimestampMillisecondArray),
                TimeUnit::Microsecond => bind!(TimestampMicrosecond, TimestampMicrosecondArray),
                TimeUnit::Nanosecond => bind!(TimestampNanosecond, TimestampNanosecondArray),
            },
            _ => return Ok(Self::Checked(array)),
        };
        Ok(Self::Fixed {
            value,
            nulls: array.nulls(),
        })
    }
    fn is_null(value: &Fixed<'_>, nulls: Option<&NullBuffer>, row: usize) -> bool {
        matches!(value, Fixed::Null) || nulls.is_some_and(|n| n.is_null(row))
    }
    fn size(&self, row: usize) -> Result<usize> {
        match self {
            Self::Fixed { value, nulls } => Ok(if Self::is_null(value, *nulls, row) {
                1
            } else {
                1 + value.width()
            }),
            Self::Checked(array) => {
                let mut size = 0;
                arrow_input::visit(*array, row, true, 0, true, &mut |bytes| {
                    size = add(size, bytes.len())?;
                    Ok(())
                })?;
                Ok(size)
            }
        }
    }
    fn put(&self, row: usize, put: &mut impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        match self {
            Self::Fixed { value, nulls } => {
                if Self::is_null(value, *nulls, row) {
                    return put(&[0]);
                }
                put(&[1])?;
                value.put(row, put)
            }
            Self::Checked(array) => arrow_input::visit(*array, row, true, 0, true, put),
        }
    }
}

pub(in crate::physical::morsel_agg) struct BoundKeyArrays<'a> {
    layout: Arc<KeyLayout>,
    columns: ReservedVec<Column<'a>>,
    rows: usize,
}
impl<'a> BoundKeyArrays<'a> {
    pub(in crate::physical::morsel_agg) fn bind(
        layout: Arc<KeyLayout>,
        arrays: &'a [ArrayRef],
        rows: usize,
        pool: &MemoryPool,
    ) -> Result<Self> {
        layout.validate_arrays(arrays)?;
        if arrays.iter().any(|a| a.len() != rows) {
            return Err(invalid("bound key array length differs from batch"));
        }
        let mut columns = ReservedVec::with_capacity(pool, arrays.len())?;
        columns.try_extend_reserved(
            arrays.len(),
            arrays.iter().map(|a| Column::bind(a.as_ref())),
        )?;
        Ok(Self {
            layout,
            columns,
            rows,
        })
    }
    pub(in crate::physical::morsel_agg) fn encode(
        &self,
        workspace: &mut KeyWorkspace,
        row: usize,
    ) -> Result<()> {
        workspace.valid = false;
        if !Arc::ptr_eq(&self.layout, &workspace.layout) {
            return Err(invalid("bound keys belong to another layout"));
        }
        if row >= self.rows {
            return Err(invalid("bound key row out of bounds"));
        }
        let size = self
            .columns
            .as_slice()
            .iter()
            .try_fold(0, |size, column| add(size, column.size(row)?))?;
        workspace.bytes.truncate(0);
        workspace.bytes.reserve(size)?;
        workspace
            .bytes
            .extend_reserved(size, std::iter::repeat_n(0, size))?;
        let mut writer = KeyWriter {
            bytes: workspace.bytes.as_mut_slice(),
            position: 0,
        };
        for column in self.columns.as_slice() {
            column.put(row, &mut |bytes| {
                writer.put(bytes);
                Ok(())
            })?;
        }
        debug_assert_eq!(writer.position, size);
        workspace.valid = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bound_primitive_keys_preserve_independent_bytes_and_sliced_nulls() {
        let pool = MemoryPool::new(1024 * 1024);
        let mut arrays: Vec<ArrayRef> = Vec::new();
        let mut expected = Vec::new();
        macro_rules! column {
            ($array:ty, $value:expr) => {{
                let value = $value;
                arrays.push(Arc::new(
                    <$array>::from(vec![Some(value), None, Some(value)]).slice(1, 2),
                ));
                expected.push(1);
                expected.extend(value.to_le_bytes());
            }};
        }
        column!(Int8Array, -17i8);
        column!(Int16Array, -1234i16);
        column!(Int32Array, -123456i32);
        column!(Int64Array, i64::MIN);
        column!(UInt8Array, 255u8);
        column!(UInt16Array, 65535u16);
        column!(UInt32Array, u32::MAX);
        column!(UInt64Array, u64::MAX);
        column!(Date32Array, -123i32);
        column!(Date64Array, -86400000i64);
        column!(TimestampSecondArray, -123i64);
        column!(TimestampMillisecondArray, -123i64);
        column!(TimestampMicrosecondArray, -123i64);
        column!(TimestampNanosecondArray, -123i64);
        arrays.push(Arc::new(
            Decimal128Array::from(vec![None, Some(-123456i128)])
                .with_precision_and_scale(30, 4)
                .unwrap(),
        ));
        expected.push(1);
        expected.extend((-123456i128).to_le_bytes());
        arrays.push(Arc::new(BooleanArray::from(vec![None, Some(true)])));
        expected.extend([1, 1]);
        arrays.push(Arc::new(NullArray::new(2)));
        expected.push(0);
        let types = arrays
            .iter()
            .map(|a| a.data_type().clone())
            .collect::<Vec<_>>();
        let layout = KeyLayout::bind(&pool, &types).unwrap().unwrap();
        let binding = BoundKeyArrays::bind(layout.clone(), &arrays, 2, &pool).unwrap();
        let mut key = KeyWorkspace::new(layout.clone()).unwrap();
        binding.encode(&mut key, 0).unwrap();
        assert_eq!(key.key().unwrap().bytes(), vec![0; arrays.len()]);
        binding.encode(&mut key, 1).unwrap();
        assert_eq!(key.key().unwrap().bytes(), expected);
        drop((binding, key, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn bound_keys_canonicalize_float_bits_and_check_dictionary_values() {
        use arrow::datatypes::Int8Type;
        let pool = MemoryPool::new(1024 * 1024);
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![
                -0.0,
                f32::from_bits(0xffc00017),
                f32::INFINITY,
            ])),
            Arc::new(Float64Array::from(vec![
                0.0,
                f64::from_bits(0xfff8000000000042),
                f64::NEG_INFINITY,
            ])),
            Arc::new(
                DictionaryArray::<Int8Type>::try_new(
                    Int8Array::from(vec![Some(1), None, Some(0)]),
                    Arc::new(StringArray::from(vec![Some("é"), None])),
                )
                .unwrap(),
            ),
        ];
        let layout = KeyLayout::bind(
            &pool,
            &[DataType::Float32, DataType::Float64, DataType::Utf8],
        )
        .unwrap()
        .unwrap();
        let bound = BoundKeyArrays::bind(layout.clone(), &arrays, 3, &pool).unwrap();
        let mut key = KeyWorkspace::new(layout.clone()).unwrap();
        for (row, f32bits, f64bits, text) in [
            (0, 0u32, 0u64, None),
            (1, 0x7fc00000, 0x7ff8000000000000, None),
            (2, 0x7f800000, 0xfff0000000000000, Some("é")),
        ] {
            let mut expected = vec![1];
            expected.extend(f32bits.to_le_bytes());
            expected.push(1);
            expected.extend(f64bits.to_le_bytes());
            if let Some(text) = text {
                expected.push(1);
                expected.extend((text.len() as u64).to_le_bytes());
                expected.extend(text.as_bytes());
            } else {
                expected.push(0);
            }
            bound.encode(&mut key, row).unwrap();
            assert_eq!(key.key().unwrap().bytes(), expected);
        }
        drop((bound, key, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn bound_key_admission_identity_and_errors_do_not_publish_stale_keys() {
        let pool = MemoryPool::new(65536);
        let layout = KeyLayout::bind(&pool, &[DataType::Utf8]).unwrap().unwrap();
        let arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec!["x".repeat(4096)]))];
        let bound = BoundKeyArrays::bind(layout.clone(), &arrays, 1, &pool).unwrap();
        let mut key = KeyWorkspace::new(layout.clone()).unwrap();
        key.encode(&[ScalarValue::Null]).unwrap();
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(BoundKeyArrays::bind(layout.clone(), &arrays, 1, &pool)
            .err()
            .unwrap()
            .is_memory_limit());
        assert!(bound.encode(&mut key, 0).unwrap_err().is_memory_limit());
        assert!(key.key().is_err());
        assert!(!bound.encode(&mut key, 1).unwrap_err().is_memory_limit());
        drop(pressure);
        bound.encode(&mut key, 0).unwrap();
        let other = KeyLayout::bind(&pool, &[DataType::Utf8]).unwrap().unwrap();
        let mut foreign = KeyWorkspace::new(other.clone()).unwrap();
        foreign.encode(&[ScalarValue::Null]).unwrap();
        assert!(bound.encode(&mut foreign, 0).is_err());
        assert!(foreign.key().is_err());
        assert!(BoundKeyArrays::bind(layout.clone(), &arrays, 2, &pool).is_err());
        assert!(BoundKeyArrays::bind(layout.clone(), &[], 0, &pool).is_err());
        let wrong: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![1]))];
        assert!(BoundKeyArrays::bind(layout.clone(), &wrong, 1, &pool).is_err());
        drop((bound, key, layout, foreign, other));
        assert_eq!(pool.used(), 0);
    }
}
