//! Typed selected-state payload restoration. The enclosing row/file owns version
//! and layout identity; this codec validates one value and returns its byte count.
use super::*;
use crate::planner::{DecimalValue, TimestampValue};

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }
    fn take(&mut self, count: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .ok_or_else(|| invalid("wire size overflow"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| invalid("truncated scalar wire value"))?;
        self.position = end;
        Ok(value)
    }
    fn word<const N: usize>(&mut self) -> Result<[u8; N]> {
        Ok(self.take(N)?.try_into().unwrap())
    }
    fn length(&mut self) -> Result<usize> {
        usize::try_from(u64::from_le_bytes(self.word()?))
            .ok()
            .filter(|n| *n <= self.bytes.len() - self.position)
            .ok_or_else(|| invalid("scalar wire length exceeds remaining bytes"))
    }

    /// BUILD=false performs either allocation-free validation or admission.
    /// BUILD=true runs only after both passes succeed on the same immutable bytes.
    fn value<const BUILD: bool>(
        &mut self,
        data_type: &DataType,
        nullable: bool,
        depth: usize,
        mut admission: Option<&mut Admission>,
        before_allocation: &mut impl FnMut() -> Result<()>,
    ) -> Result<Option<ScalarValue>> {
        if depth > MAX_DEPTH {
            return Err(invalid("scalar wire nesting exceeds limit"));
        }
        match self.take(1)?[0] {
            0 if nullable => return Ok(BUILD.then_some(ScalarValue::Null)),
            0 => return Err(invalid("NULL scalar wire list element is forbidden")),
            1 => (),
            _ => return Err(invalid("invalid scalar wire validity")),
        }
        macro_rules! primitive {
            ($variant:ident, $ty:ty) => {{
                let value = <$ty>::from_le_bytes(self.word()?);
                BUILD.then(|| ScalarValue::$variant(value.into()))
            }};
        }
        Ok(match data_type {
            DataType::Boolean => {
                let byte = self.take(1)?[0];
                if byte > 1 {
                    return Err(invalid("invalid scalar wire Boolean"));
                }
                BUILD.then_some(ScalarValue::Boolean(byte != 0))
            }
            DataType::Int8 => primitive!(Int8, i8),
            DataType::Int16 => primitive!(Int16, i16),
            DataType::Int32 => primitive!(Int32, i32),
            DataType::Int64 => primitive!(Int64, i64),
            DataType::UInt8 => primitive!(UInt8, u8),
            DataType::UInt16 => primitive!(UInt16, u16),
            DataType::UInt32 => primitive!(UInt32, u32),
            DataType::UInt64 => primitive!(UInt64, u64),
            DataType::Float32 => primitive!(Float32, f32),
            DataType::Float64 => primitive!(Float64, f64),
            DataType::Date32 => primitive!(Date32, i32),
            DataType::Date64 => primitive!(Date64, i64),
            DataType::Decimal128(_, scale) => {
                let value = i128::from_le_bytes(self.word()?);
                BUILD.then_some(ScalarValue::Decimal128(DecimalValue::new(value, *scale)))
            }
            DataType::Timestamp(unit, zone) => {
                let ticks = i64::from_le_bytes(self.word()?);
                if let (Some(admission), Some(zone)) = (admission, zone) {
                    admission.allocation(zone.len())?;
                }
                BUILD.then(|| {
                    ScalarValue::Timestamp(TimestampValue::new(ticks, *unit, zone.clone()))
                })
            }
            DataType::Utf8 => {
                let count = self.length()?;
                let text = std::str::from_utf8(self.take(count)?)
                    .map_err(|_| invalid("invalid scalar wire UTF-8"))?;
                if let Some(admission) = admission {
                    admission.allocation(count)?;
                }
                if BUILD {
                    before_allocation()?;
                    let mut value = String::new();
                    value
                        .try_reserve_exact(count)
                        .map_err(|e| invalid(&format!("decoded string allocation refused: {e}")))?;
                    if value.capacity() != count {
                        return Err(invalid("decoded string capacity exceeds admission"));
                    }
                    value.push_str(text);
                    Some(ScalarValue::Utf8(value))
                } else {
                    None
                }
            }
            DataType::List(field) => {
                let count = self.length()?;
                let bytes = count
                    .checked_mul(size_of::<ScalarValue>())
                    .filter(|n| *n <= isize::MAX as usize)
                    .ok_or_else(|| invalid("decoded list size overflow"))?;
                if let Some(admission) = admission.as_deref_mut() {
                    admission.allocation(bytes)?;
                    admission.data_type(field.data_type(), depth + 1)?;
                }
                let mut values = Vec::new();
                if BUILD {
                    before_allocation()?;
                    values
                        .try_reserve_exact(count)
                        .map_err(|e| invalid(&format!("decoded list allocation refused: {e}")))?;
                    if values.capacity() != count {
                        return Err(invalid("decoded list capacity exceeds admission"));
                    }
                }
                for _ in 0..count {
                    if let Some(value) = self.value::<BUILD>(
                        field.data_type(),
                        field.is_nullable(),
                        depth + 1,
                        admission.as_deref_mut(),
                        before_allocation,
                    )? {
                        values.push(value);
                    }
                }
                if BUILD {
                    Some(ScalarValue::List(
                        values,
                        Box::new(field.data_type().clone()),
                    ))
                } else {
                    None
                }
            }
            _ => return Err(invalid("unsupported non-null scalar wire type")),
        })
    }
}

impl ReservedScalar {
    pub(crate) fn validate_encoded(data_type: &DataType, bytes: &[u8]) -> Result<usize> {
        let mut reader = Reader::new(bytes);
        reader.value::<false>(data_type, true, 0, None, &mut || Ok(()))?;
        Ok(reader.position)
    }
    /// Only allocation-free scalar variants may leave without an owner. A
    /// variable payload returns None for the caller to use the admitted decoder.
    pub(crate) fn try_decode_inline(
        data_type: &DataType,
        bytes: &[u8],
    ) -> Result<Option<(ScalarValue, usize)>> {
        if bytes.first() != Some(&0)
            && !matches!(
                data_type,
                DataType::Null
                    | DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Float32
                    | DataType::Float64
                    | DataType::Decimal128(..)
                    | DataType::Date32
                    | DataType::Date64
            )
        {
            return Ok(None);
        }
        let mut reader = Reader::new(bytes);
        let value = reader
            .value::<true>(data_type, true, 0, None, &mut || {
                Err(invalid("inline decoder attempted allocation"))
            })?
            .ok_or_else(|| invalid("inline decoder did not construct a value"))?;
        Ok(Some((value, reader.position)))
    }

    pub(crate) fn try_decode(
        pool: &MemoryPool,
        data_type: &DataType,
        bytes: &[u8],
    ) -> Result<(Self, usize)> {
        Self::decode_with(pool, data_type, bytes, &mut || Ok(()))
    }

    fn decode_with(
        pool: &MemoryPool,
        data_type: &DataType,
        bytes: &[u8],
        before_allocation: &mut impl FnMut() -> Result<()>,
    ) -> Result<(Self, usize)> {
        let mut validator = Reader::new(bytes);
        validator.value::<false>(data_type, true, 0, None, before_allocation)?;
        let consumed = validator.position;
        let mut admission = Admission {
            lease: pool.allocate(OWNER_BYTES + size_of::<Self>())?,
        };
        Reader::new(bytes).value::<false>(
            data_type,
            true,
            0,
            Some(&mut admission),
            before_allocation,
        )?;
        let value = Reader::new(bytes)
            .value::<true>(data_type, true, 0, None, before_allocation)?
            .ok_or_else(|| invalid("decoder did not construct a value"))?;
        Ok((
            Self {
                value,
                _reservation: admission.lease,
            },
            consumed,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{Field, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn decoder_accounts_for_retained_schema_and_bounds_nested_input() {
        let pool = MemoryPool::new_named("decode schema", 8192);
        let field = Field::new("leaf", DataType::Utf8, true).with_metadata(
            std::collections::HashMap::from([("large".into(), "x".repeat(16384))]),
        );
        let child = DataType::List(Arc::new(field));
        let data_type = DataType::List(Arc::new(Field::new("outer", child, true)));
        let mut attempts = 0;
        // Empty values still retain the child type, including its shared fields.
        let error = ReservedScalar::decode_with(&pool, &data_type, &list(&[]), &mut || {
            attempts += 1;
            Ok(())
        })
        .unwrap_err();
        assert!(error.is_memory_limit());
        assert_eq!(attempts, 0);
        assert_eq!(pool.used(), 0);
        let mut data_type = DataType::Int64;
        let mut bytes = vec![0];
        for _ in 0..70 {
            data_type = DataType::List(Arc::new(Field::new("nested", data_type, true)));
            bytes = list(&[bytes]);
        }
        let error = ReservedScalar::decode_with(&pool, &data_type, &bytes, &mut || {
            attempts += 1;
            Ok(())
        })
        .unwrap_err();
        assert!(!error.is_memory_limit());
        assert_eq!(attempts, 0);
        assert_eq!(pool.used(), 0);
    }

    fn string(value: &[u8]) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend((value.len() as u64).to_le_bytes());
        bytes.extend(value);
        bytes
    }
    fn list(values: &[Vec<u8>]) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend((values.len() as u64).to_le_bytes());
        for value in values {
            bytes.extend(value);
        }
        bytes
    }

    #[test]
    fn decoded_payload_owns_nested_values_and_retained_type_metadata() {
        let pool = MemoryPool::new_named("decode ownership", 1 << 20);
        let child = DataType::List(Arc::new(Field::new("text", DataType::Utf8, true)));
        let data_type = DataType::List(Arc::new(Field::new("nested", child.clone(), true)));
        let bytes = list(&[
            vec![0],
            list(&[]),
            list(&[vec![0], string(b""), string("é\0owned".as_bytes())]),
        ]);
        let mut framed = bytes.clone();
        framed.extend([1, 255]);
        let (value, consumed) = ReservedScalar::try_decode(&pool, &data_type, &framed).unwrap();
        assert_eq!(consumed, bytes.len());
        let expected = ScalarValue::List(
            vec![
                ScalarValue::Null,
                ScalarValue::List(vec![], Box::new(DataType::Utf8)),
                ScalarValue::List(
                    vec![
                        ScalarValue::Null,
                        ScalarValue::Utf8(String::new()),
                        ScalarValue::Utf8("é\0owned".into()),
                    ],
                    Box::new(DataType::Utf8),
                ),
            ],
            Box::new(child),
        );
        drop((bytes, framed, data_type));
        assert_eq!(value.as_scalar(), &expected);
        assert!(pool.used() > 1000);
        let shared = Arc::new(value);
        let other = shared.clone();
        drop(shared);
        assert_eq!(other.as_scalar(), &expected);
        drop(other);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn malformed_wire_is_validated_before_admission_or_payload_construction() {
        let pool = MemoryPool::new_named("decode invalid", 65536);
        let data_type = DataType::List(Arc::new(Field::new("s", DataType::Utf8, true)));
        let bytes = list(&[string(b"first"), string(b"second")]);
        let pressure = pool.allocate(pool.available()).unwrap();
        let mut allocations = 0;
        for end in 0..bytes.len() {
            let error = ReservedScalar::decode_with(&pool, &data_type, &bytes[..end], &mut || {
                allocations += 1;
                Ok(())
            })
            .unwrap_err();
            assert!(!error.is_memory_limit());
        }
        let mut huge = vec![1];
        huge.extend(u64::MAX.to_le_bytes());
        for (data_type, malformed) in [
            (DataType::Boolean, vec![1, 2]),
            (DataType::Int64, vec![2]),
            (DataType::Null, vec![1]),
            (DataType::Utf8, string(&[0xc0, 0xaf])),
            (data_type.clone(), list(&[string(b"valid"), string(&[255])])),
            (DataType::Utf8, huge.clone()),
            (data_type.clone(), huge),
            (
                DataType::List(Arc::new(Field::new("s", DataType::Utf8, false))),
                list(&[vec![0]]),
            ),
        ] {
            let error = ReservedScalar::decode_with(&pool, &data_type, &malformed, &mut || {
                allocations += 1;
                Ok(())
            })
            .unwrap_err();
            assert!(!error.is_memory_limit());
        }
        assert_eq!(allocations, 0);
        assert_eq!(pool.available(), 0);
        drop(pressure);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn decode_admission_and_partial_allocator_failure_release_every_owner() {
        let pool = MemoryPool::new_named("decode failure", 65536);
        let data_type = DataType::List(Arc::new(Field::new("s", DataType::Utf8, true)));
        let bytes = list(&[string(&vec![b'a'; 4096]), string(&vec![b'b'; 4096])]);
        let pressure = pool.allocate(pool.available() - 3000).unwrap();
        let before = pool.used();
        let mut calls = 0;
        let error = ReservedScalar::decode_with(&pool, &data_type, &bytes, &mut || {
            calls += 1;
            Ok(())
        })
        .unwrap_err();
        assert!(error.is_memory_limit());
        assert_eq!(calls, 0);
        assert_eq!(pool.used(), before);
        drop(pressure);
        let error = ReservedScalar::decode_with(&pool, &data_type, &bytes, &mut || {
            calls += 1;
            assert!(pool.used() > 8192);
            if calls == 3 {
                Err(invalid("injected decoder allocation failure"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert!(!error.is_memory_limit());
        assert_eq!(calls, 3);
        assert_eq!(pool.used(), 0);
        let (value, used) = ReservedScalar::try_decode(&pool, &data_type, &bytes).unwrap();
        assert_eq!(used, bytes.len());
        drop(value);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn decoding_keeps_selected_float_bits_full_decimal_and_timestamp_domain() {
        let pool = MemoryPool::new_named("decode bits", 65536);
        let nan32 = 0xffc0_0042u32;
        let nan64 = 0xfff8_0000_0000_0042u64;
        for (data_type, payload) in [
            (DataType::Float32, nan32.to_le_bytes().to_vec()),
            (DataType::Float64, nan64.to_le_bytes().to_vec()),
            (DataType::Float64, (-0.0f64).to_le_bytes().to_vec()),
            (
                DataType::Decimal128(38, -3),
                i128::MAX.to_le_bytes().to_vec(),
            ),
            (DataType::UInt64, u64::MAX.to_le_bytes().to_vec()),
            (
                DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
                i64::MIN.to_le_bytes().to_vec(),
            ),
        ] {
            let mut bytes = vec![1];
            bytes.extend(&payload);
            let (value, consumed) = ReservedScalar::try_decode(&pool, &data_type, &bytes).unwrap();
            assert_eq!(consumed, bytes.len());
            match value.as_scalar() {
                ScalarValue::Float32(v) => {
                    assert_eq!(v.into_inner().to_bits().to_le_bytes().as_slice(), payload)
                }
                ScalarValue::Float64(v) => {
                    assert_eq!(v.into_inner().to_bits().to_le_bytes().as_slice(), payload)
                }
                ScalarValue::Decimal128(v) => {
                    assert_eq!(v.mantissa(), i128::MAX);
                    assert_eq!(v.scale(), -3);
                }
                ScalarValue::UInt64(v) => assert_eq!(*v, u64::MAX),
                ScalarValue::Timestamp(v) => {
                    assert_eq!(v.ticks, i64::MIN);
                    assert_eq!(v.unit, TimeUnit::Nanosecond);
                    assert_eq!(v.timezone.as_deref(), Some("UTC"));
                }
                _ => panic!("wrong scalar domain"),
            }
            drop(value);
            assert_eq!(pool.used(), 0);
        }
    }
}
