//! Canonical group keys in flat reservation-owned storage. Hashes only select
//! candidate groups: equality still checks the full bytes and bound layout.
pub(super) mod arrow_input;
pub(super) mod bound_arrays;

#[cfg(test)]
mod preparation_benchmark;

use super::selected_state::{matches_type, supported_type};
use crate::execution::{reserved_scalar::ReservedDataType, reserved_vec::ReservedVec, MemoryPool};
use crate::planner::{numeric::sql_float_key, ScalarValue};
use crate::{QueryError, Result};
use arrow::datatypes::DataType;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    io::Read,
    sync::Arc,
};

pub(super) struct KeyLayout {
    types: ReservedVec<ReservedDataType>,
    pool: MemoryPool,
}

fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("canonical group key: {message}"))
}

impl KeyLayout {
    /// Borrow a validated UTF8 field directly from its canonical key owner.
    /// The returned string cannot outlive the key storage; no scalar is allocated.
    pub(super) fn utf8_field<'a>(&self, bytes: &'a [u8], column: usize) -> Result<Option<&'a str>> {
        if self.data_type(column)? != &DataType::Utf8 {
            return Err(invalid("UTF8 field requested for another type"));
        }
        let payload = self.field_payload(bytes, column)?;
        if payload.first() == Some(&0) {
            return Ok(None);
        }
        // field_payload already validates presence, length, UTF8 and bounds.
        let value = payload
            .get(9..)
            .ok_or_else(|| invalid("truncated UTF8 field"))?;
        Ok(Some(
            std::str::from_utf8(value).map_err(|_| invalid("invalid UTF8 field"))?,
        ))
    }

    pub(super) fn len(&self) -> usize {
        self.types.as_slice().len()
    }
    pub(super) fn data_type(&self, column: usize) -> Result<&DataType> {
        self.types
            .as_slice()
            .get(column)
            .map(|t| t.as_type())
            .ok_or_else(|| invalid("key column out of bounds"))
    }
    pub(super) fn field_payload<'a>(&self, bytes: &'a [u8], column: usize) -> Result<&'a [u8]> {
        self.data_type(column)?;
        let mut reader = KeyReader { remaining: bytes };
        for (index, ty) in self.types.as_slice().iter().enumerate() {
            let before = reader.remaining;
            reader.value(ty.as_type(), true, 0)?;
            if index == column {
                return Ok(&before[..before.len() - reader.remaining.len()]);
            }
        }
        Err(invalid("key column out of bounds"))
    }

    fn validate_encoded(&self, bytes: &[u8]) -> Result<()> {
        let mut reader = KeyReader { remaining: bytes };
        for data_type in self.types.as_slice() {
            reader.value(data_type.as_type(), true, 0)?;
        }
        if !reader.remaining.is_empty() {
            return Err(invalid("trailing encoded bytes"));
        }
        Ok(())
    }

    pub(super) fn bind(pool: &MemoryPool, types: &[DataType]) -> Result<Option<Arc<Self>>> {
        if types
            .iter()
            .any(|data_type| !supported_type(data_type, true, 0))
        {
            return Ok(None);
        }
        let mut owned = ReservedVec::with_capacity(pool, types.len())?;
        owned.try_extend_reserved(
            types.len(),
            types
                .iter()
                .map(|data_type| ReservedDataType::try_copy(pool, data_type)),
        )?;
        Ok(Some(Arc::new(Self {
            types: owned,
            pool: pool.clone(),
        })))
    }
}

/// Reusable scratch for lookup/admission. A failed encoding invalidates the view;
/// callers cannot accidentally publish the preceding key as a new row's key.
pub(super) struct KeyWorkspace {
    layout: Arc<KeyLayout>,
    bytes: ReservedVec<u8>,
    valid: bool,
}

impl KeyWorkspace {
    pub(super) fn new(layout: Arc<KeyLayout>) -> Result<Self> {
        Self::new_in_pool(layout.clone(), &layout.pool)
    }

    pub(super) fn new_in_pool(layout: Arc<KeyLayout>, pool: &MemoryPool) -> Result<Self> {
        if !pool.is_within(&layout.pool) {
            return Err(invalid("workspace pool is outside bound query"));
        }
        let bytes = ReservedVec::with_capacity(pool, 0)?;
        Ok(Self {
            layout,
            bytes,
            valid: false,
        })
    }

    pub(super) fn encode(&mut self, values: &[ScalarValue]) -> Result<()> {
        self.valid = false;
        if values.len() != self.layout.types.as_slice().len() {
            return Err(invalid("arity differs from bound layout"));
        }
        let mut size = 0usize;
        for (value, data_type) in values.iter().zip(self.layout.types.as_slice()) {
            if !matches_type(value, data_type.as_type(), 0) {
                return Err(invalid("value differs from bound type"));
            }
            size = add(size, value_size(value, 0)?)?;
        }
        self.bytes.truncate(0);
        self.bytes.reserve(size)?;
        self.bytes
            .extend_reserved(size, std::iter::repeat_n(0, size))?;
        let mut writer = KeyWriter {
            bytes: self.bytes.as_mut_slice(),
            position: 0,
        };
        for value in values {
            writer.value(value);
        }
        debug_assert_eq!(writer.position, size);
        self.valid = true;
        Ok(())
    }

    pub(super) fn key(&self) -> Result<KeyRef<'_>> {
        if !self.valid {
            return Err(invalid("workspace has no completed encoding"));
        }
        Ok(KeyRef {
            layout: &self.layout,
            bytes: self.bytes.as_slice(),
        })
    }

    /// Import a complete encoded key after the file owner has verified its
    /// version and layout identity. Validate without allocation before copying
    /// into admitted scratch. An error invalidates the preceding key view.
    pub(super) fn load_encoded(&mut self, bytes: &[u8]) -> Result<()> {
        self.valid = false;
        self.layout.validate_encoded(bytes)?;
        self.bytes.truncate(0);
        self.bytes.reserve(bytes.len())?;
        self.bytes
            .extend_reserved(bytes.len(), bytes.iter().copied())?;
        self.valid = true;
        Ok(())
    }

    /// The enclosing file reader verifies version/layout and retains the frame
    /// length. Admission failure consumes no payload bytes; IO/validation errors
    /// are terminal for that file and never expose partially read keys.
    pub(super) fn read_encoded(&mut self, reader: &mut impl Read, length: usize) -> Result<()> {
        self.valid = false;
        add(0, length)?;
        self.bytes.truncate(0);
        self.bytes.reserve(length)?;
        self.bytes
            .extend_reserved(length, std::iter::repeat_n(0, length))?;
        reader.read_exact(self.bytes.as_mut_slice())?;
        self.layout.validate_encoded(self.bytes.as_slice())?;
        self.valid = true;
        Ok(())
    }
}

/// Every recursive value consumes at least one byte. List counts are bounded
/// by remaining bytes before traversal, so hostile lengths cannot amplify work
/// beyond the input size. No decoded String/List allocation is needed for keys.
struct KeyReader<'a> {
    remaining: &'a [u8],
}

impl<'a> KeyReader<'a> {
    fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        let (head, tail) = self
            .remaining
            .split_at_checked(size)
            .ok_or_else(|| invalid("truncated encoded value"))?;
        self.remaining = tail;
        Ok(head)
    }

    fn length(&mut self) -> Result<usize> {
        let length = u64::from_le_bytes(self.take(8)?.try_into().unwrap());
        usize::try_from(length)
            .ok()
            .filter(|length| *length <= self.remaining.len())
            .ok_or_else(|| invalid("encoded length exceeds remaining bytes"))
    }

    fn value(&mut self, data_type: &DataType, nullable: bool, depth: usize) -> Result<()> {
        if depth > 64 {
            return Err(invalid("value nesting exceeds encoding limit"));
        }
        match self.take(1)?[0] {
            0 if nullable => return Ok(()),
            0 => return Err(invalid("NULL in non-nullable list element")),
            1 => (),
            _ => return Err(invalid("invalid encoded validity")),
        }
        match data_type {
            DataType::Boolean => {
                if self.take(1)?[0] > 1 {
                    return Err(invalid("invalid encoded Boolean"));
                }
            }
            DataType::Int8 | DataType::UInt8 => {
                self.take(1)?;
            }
            DataType::Int16 | DataType::UInt16 => {
                self.take(2)?;
            }
            DataType::Int32 | DataType::UInt32 | DataType::Date32 => {
                self.take(4)?;
            }
            DataType::Int64 | DataType::UInt64 | DataType::Date64 | DataType::Timestamp(..) => {
                self.take(8)?;
            }
            DataType::Decimal128(..) => {
                self.take(16)?;
            }
            DataType::Float32 => {
                let bits = u32::from_le_bytes(self.take(4)?.try_into().unwrap());
                let value = f32::from_bits(bits);
                if (value == 0.0 && bits != 0) || (value.is_nan() && bits != 0x7fc0_0000) {
                    return Err(invalid("noncanonical Float32 key"));
                }
            }
            DataType::Float64 => {
                let bits = u64::from_le_bytes(self.take(8)?.try_into().unwrap());
                if sql_float_key(f64::from_bits(bits)) != bits {
                    return Err(invalid("noncanonical Float64 key"));
                }
            }
            DataType::Utf8 => {
                let size = self.length()?;
                std::str::from_utf8(self.take(size)?)
                    .map_err(|_| invalid("invalid encoded UTF-8"))?;
            }
            DataType::List(field) => {
                let count = self.length()?;
                for _ in 0..count {
                    self.value(field.data_type(), field.is_nullable(), depth + 1)?;
                }
            }
            _ => return Err(invalid("non-null value for unsupported bound type")),
        }
        Ok(())
    }
}

/// Offset metadata and payload each have a flat allocation, not one Vec per key.
pub(super) struct KeyRows {
    layout: Arc<KeyLayout>,
    bytes: ReservedVec<u8>,
    ends: ReservedVec<usize>,
}

impl KeyRows {
    pub(super) fn new(layout: Arc<KeyLayout>) -> Result<Self> {
        Self::new_in_pool(layout.clone(), &layout.pool)
    }

    pub(super) fn new_in_pool(layout: Arc<KeyLayout>, pool: &MemoryPool) -> Result<Self> {
        if !pool.is_within(&layout.pool) {
            return Err(invalid("key rows pool is outside bound query"));
        }
        let bytes = ReservedVec::with_capacity(pool, 0)?;
        let ends = ReservedVec::with_capacity(pool, 0)?;
        Ok(Self {
            layout,
            bytes,
            ends,
        })
    }
    pub(super) fn len(&self) -> usize {
        self.ends.as_slice().len()
    }

    pub(super) fn append(&mut self, workspace: &KeyWorkspace) -> Result<usize> {
        self.append_ref(workspace.key()?)
    }

    pub(super) fn append_ref(&mut self, key: KeyRef<'_>) -> Result<usize> {
        if !Arc::ptr_eq(&self.layout, key.layout) {
            return Err(invalid("append requires the same bound layout"));
        }
        let start = self.bytes.as_slice().len();
        let end = add(start, key.bytes.len())?;
        self.bytes.reserve(key.bytes.len())?;
        self.ends.reserve(1)?;
        self.bytes
            .extend_reserved(key.bytes.len(), key.bytes.iter().copied())?;
        if let Err(error) = self.ends.extend_reserved(1, std::iter::once(end)) {
            self.bytes.truncate(start);
            return Err(error);
        }
        Ok(self.len() - 1)
    }

    pub(super) fn key(&self, row: usize) -> Result<KeyRef<'_>> {
        let end = *self
            .ends
            .as_slice()
            .get(row)
            .ok_or_else(|| invalid("row index out of range"))?;
        let start = if row == 0 {
            0
        } else {
            self.ends.as_slice()[row - 1]
        };
        Ok(KeyRef {
            layout: &self.layout,
            bytes: &self.bytes.as_slice()[start..end],
        })
    }

    /// Used by a future grouped-row transaction to roll back key insertion if
    /// state admission fails before the key/state index is published.
    pub(super) fn truncate(&mut self, rows: usize) {
        if rows >= self.len() {
            return;
        }
        let bytes = if rows == 0 {
            0
        } else {
            self.ends.as_slice()[rows - 1]
        };
        self.bytes.truncate(bytes);
        self.ends.truncate(rows);
    }
}

#[derive(Clone, Copy)]
pub(super) struct KeyRef<'a> {
    layout: &'a Arc<KeyLayout>,
    bytes: &'a [u8],
}

impl PartialEq for KeyRef<'_> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(self.layout, other.layout) && self.bytes == other.bytes
    }
}
impl Eq for KeyRef<'_> {}
impl Hash for KeyRef<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.bytes.hash(state);
    }
}
impl<'a> KeyRef<'a> {
    pub(super) fn belongs_to(&self, layout: &Arc<KeyLayout>) -> bool {
        Arc::ptr_eq(self.layout, layout)
    }
    pub(super) fn bytes(&self) -> &'a [u8] {
        self.bytes
    }
    /// Stable across workers in this executable/query, not a cross-version file
    /// format promise. A hash match is never proof of group equality.
    pub(super) fn hash64(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

fn add(left: usize, right: usize) -> Result<usize> {
    left.checked_add(right)
        .filter(|size| *size <= isize::MAX as usize)
        .ok_or_else(|| invalid("encoded size overflow"))
}

fn value_size(value: &ScalarValue, depth: usize) -> Result<usize> {
    if depth > 64 {
        return Err(invalid("value nesting exceeds encoding limit"));
    }
    let payload = match value {
        ScalarValue::Null => 0,
        ScalarValue::Boolean(_) | ScalarValue::Int8(_) | ScalarValue::UInt8(_) => 1,
        ScalarValue::Int16(_) | ScalarValue::UInt16(_) => 2,
        ScalarValue::Int32(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::Float32(_)
        | ScalarValue::Date32(_) => 4,
        ScalarValue::Int64(_)
        | ScalarValue::UInt64(_)
        | ScalarValue::Float64(_)
        | ScalarValue::Date64(_)
        | ScalarValue::Timestamp(_)
        | ScalarValue::Interval(_) => 8,
        ScalarValue::Decimal128(_) => 16,
        ScalarValue::Utf8(value) => add(8, value.len())?,
        ScalarValue::List(values, _) => {
            let mut size = 8;
            for value in values {
                size = add(size, value_size(value, depth + 1)?)?;
            }
            size
        }
    };
    add(1, payload)
}

struct KeyWriter<'a> {
    bytes: &'a mut [u8],
    position: usize,
}
impl KeyWriter<'_> {
    fn put(&mut self, value: &[u8]) {
        let end = self.position + value.len();
        self.bytes[self.position..end].copy_from_slice(value);
        self.position = end;
    }
    fn value(&mut self, value: &ScalarValue) {
        if matches!(value, ScalarValue::Null) {
            self.put(&[0]);
            return;
        }
        self.put(&[1]);
        match value {
            ScalarValue::Null => unreachable!(),
            ScalarValue::Boolean(value) => self.put(&[u8::from(*value)]),
            ScalarValue::Int8(value) => self.put(&value.to_le_bytes()),
            ScalarValue::UInt8(value) => self.put(&value.to_le_bytes()),
            ScalarValue::Int16(value) => self.put(&value.to_le_bytes()),
            ScalarValue::UInt16(value) => self.put(&value.to_le_bytes()),
            ScalarValue::Int32(value) | ScalarValue::Date32(value) => {
                self.put(&value.to_le_bytes())
            }
            ScalarValue::UInt32(value) => self.put(&value.to_le_bytes()),
            ScalarValue::Int64(value)
            | ScalarValue::Date64(value)
            | ScalarValue::Interval(value) => self.put(&value.to_le_bytes()),
            ScalarValue::UInt64(value) => self.put(&value.to_le_bytes()),
            ScalarValue::Float32(value) => {
                let value = value.into_inner();
                let bits = if value == 0.0 {
                    0
                } else if value.is_nan() {
                    0x7fc0_0000
                } else {
                    value.to_bits()
                };
                self.put(&bits.to_le_bytes());
            }
            ScalarValue::Float64(value) => {
                self.put(&sql_float_key(value.into_inner()).to_le_bytes())
            }
            ScalarValue::Decimal128(value) => self.put(&value.mantissa().to_le_bytes()),
            ScalarValue::Timestamp(value) => self.put(&value.ticks.to_le_bytes()),
            ScalarValue::Utf8(value) => {
                self.put(&(value.len() as u64).to_le_bytes());
                self.put(value.as_bytes());
            }
            ScalarValue::List(values, _) => {
                self.put(&(values.len() as u64).to_le_bytes());
                for value in values {
                    self.value(value);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{DecimalValue, TimestampValue};
    use arrow::datatypes::{Field, TimeUnit};

    #[test]
    fn framed_read_admits_before_io_and_never_publishes_a_partial_key() {
        use std::io::{Cursor, Error, ErrorKind};
        let pool = MemoryPool::new_named("framed key", 65536);
        let layout = KeyLayout::bind(&pool, &[DataType::Utf8]).unwrap().unwrap();
        let mut workspace = KeyWorkspace::new(layout.clone()).unwrap();
        workspace.encode(&[ScalarValue::Null]).unwrap();
        let mut bytes = vec![1];
        bytes.extend(4096u64.to_le_bytes());
        bytes.extend(std::iter::repeat_n(b'x', 4096));
        // Adjacent frames prove read_exact consumes only the declared payload.
        let mut adjacent = bytes.clone();
        adjacent.push(0);
        let mut input = Cursor::new(adjacent);
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(workspace
            .read_encoded(&mut input, bytes.len())
            .unwrap_err()
            .is_memory_limit());
        assert_eq!(input.position(), 0);
        assert!(workspace.key().is_err());
        assert!(workspace.read_encoded(&mut input, usize::MAX).is_err());
        assert_eq!(input.position(), 0);
        drop(pressure);
        workspace.read_encoded(&mut input, bytes.len()).unwrap();
        assert_eq!(workspace.key().unwrap().bytes(), bytes);
        assert_eq!(input.position(), bytes.len() as u64);
        let pressure = pool.allocate(pool.available()).unwrap();
        workspace.read_encoded(&mut input, 1).unwrap();
        assert_eq!(workspace.key().unwrap().bytes(), &[0]);
        // A short read after overwriting scratch must invalidate the old NULL.
        let error = workspace
            .read_encoded(&mut Cursor::new(&bytes[..100]), bytes.len())
            .unwrap_err();
        assert!(matches!(error, QueryError::Io(ref e) if e.kind() == ErrorKind::UnexpectedEof));
        assert!(workspace.key().is_err());
        struct Failing {
            calls: usize,
        }
        impl Read for Failing {
            fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
                self.calls += 1;
                if self.calls == 1 {
                    out[0] = 1;
                    Ok(1)
                } else {
                    Err(Error::from_raw_os_error(5))
                }
            }
        }
        let error = workspace
            .read_encoded(&mut Failing { calls: 0 }, bytes.len())
            .unwrap_err();
        assert!(matches!(error, QueryError::Io(ref e) if e.raw_os_error() == Some(5)));
        assert!(workspace.key().is_err());
        let mut invalid = bytes.clone();
        invalid[9] = 0xff;
        assert!(workspace
            .read_encoded(&mut Cursor::new(invalid), bytes.len())
            .is_err());
        assert!(workspace.key().is_err());
        workspace
            .read_encoded(&mut Cursor::new(&bytes), bytes.len())
            .unwrap();
        assert_eq!(workspace.key().unwrap().bytes(), bytes);
        drop((pressure, workspace, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn encoded_import_preserves_exact_keys_without_free_pool_memory() {
        let pool = MemoryPool::new_named("key reader", 131072);
        let values = [
            ScalarValue::Null,
            ScalarValue::Boolean(true),
            ScalarValue::Int8(i8::MIN),
            ScalarValue::UInt8(u8::MAX),
            ScalarValue::Int16(i16::MIN),
            ScalarValue::UInt16(u16::MAX),
            ScalarValue::Int32(i32::MIN),
            ScalarValue::UInt32(u32::MAX),
            ScalarValue::Int64(i64::MIN),
            ScalarValue::UInt64(u64::MAX),
            ScalarValue::Float32(f32::from_bits(0xffc0_0042).into()),
            ScalarValue::Float64((-0.0).into()),
            ScalarValue::Decimal128(DecimalValue::new((1i128 << 110) + 17, -3)),
            ScalarValue::Date32(i32::MIN),
            ScalarValue::Date64(i64::MAX),
            ScalarValue::Timestamp(TimestampValue::new(
                i64::MIN,
                TimeUnit::Nanosecond,
                Some(Arc::from("UTC")),
            )),
            ScalarValue::Utf8("日本語\0é".into()),
            ScalarValue::List(
                vec![
                    ScalarValue::Null,
                    ScalarValue::Float64(f64::NEG_INFINITY.into()),
                    ScalarValue::Float64(f64::NAN.into()),
                ],
                Box::new(DataType::Float64),
            ),
        ];
        let types = [
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::UInt8,
            DataType::Int16,
            DataType::UInt16,
            DataType::Int32,
            DataType::UInt32,
            DataType::Int64,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal128(38, -3),
            DataType::Date32,
            DataType::Date64,
            DataType::Timestamp(TimeUnit::Nanosecond, Some(Arc::from("UTC"))),
            DataType::Utf8,
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
        ];
        let layout = KeyLayout::bind(&pool, &types).unwrap().unwrap();
        let mut writer = KeyWorkspace::new(layout.clone()).unwrap();
        let mut reader = KeyWorkspace::new(layout.clone()).unwrap();
        writer.encode(&values).unwrap();
        let key = writer.key().unwrap();
        let encoded = key.bytes();
        reader.load_encoded(encoded).unwrap();
        let pressure = pool.allocate(pool.available()).unwrap();
        reader.load_encoded(encoded).unwrap();
        assert!(reader.key().unwrap() == writer.key().unwrap());
        assert_eq!(
            reader.key().unwrap().hash64(),
            writer.key().unwrap().hash64()
        );
        // Every truncation is invalid for this complete nonempty key.
        for length in 0..encoded.len() {
            let error = reader.load_encoded(&encoded[..length]).unwrap_err();
            assert!(!error.is_memory_limit());
            assert!(reader.key().is_err());
        }
        reader.load_encoded(encoded).unwrap();
        drop((pressure, reader, writer, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn encoded_import_rejects_noncanonical_and_malformed_values_before_admission() {
        fn reject(data_type: DataType, payload: &[u8]) {
            let pool = MemoryPool::new_named("invalid key", 65536);
            let layout = KeyLayout::bind(&pool, &[data_type]).unwrap().unwrap();
            let mut reader = KeyWorkspace::new(layout.clone()).unwrap();
            reader.encode(&[ScalarValue::Null]).unwrap();
            let pressure = pool.allocate(pool.available()).unwrap();
            let error = reader.load_encoded(payload).unwrap_err();
            assert!(!error.is_memory_limit(), "{error}");
            assert!(reader.key().is_err());
            assert_eq!(pool.available(), 0);
            drop((pressure, reader, layout));
            assert_eq!(pool.used(), 0);
        }
        reject(DataType::Boolean, &[2]);
        reject(DataType::Boolean, &[1, 2]);
        reject(DataType::Null, &[1]);
        reject(DataType::Int64, &[0, 0]);
        for bits in [(-0.0f32).to_bits(), 0xffc0_0042, 0x7f80_0001] {
            let mut bytes = vec![1];
            bytes.extend(bits.to_le_bytes());
            reject(DataType::Float32, &bytes);
        }
        for bits in [
            (-0.0f64).to_bits(),
            0xfff8_0000_0000_0042,
            0x7ff0_0000_0000_0001,
        ] {
            let mut bytes = vec![1];
            bytes.extend(bits.to_le_bytes());
            reject(DataType::Float64, &bytes);
        }
        let mut huge = vec![1];
        huge.extend(u64::MAX.to_le_bytes());
        reject(DataType::Utf8, &huge);
        reject(
            DataType::List(Arc::new(Field::new("x", DataType::Null, true))),
            &huge,
        );
        let mut bad_utf8 = vec![1];
        bad_utf8.extend(2u64.to_le_bytes());
        bad_utf8.extend([0xc0, 0xaf]);
        reject(DataType::Utf8, &bad_utf8);
        let mut bad_list = vec![1];
        bad_list.extend(1u64.to_le_bytes());
        bad_list.push(0);
        reject(
            DataType::List(Arc::new(Field::new("x", DataType::Int64, false))),
            &bad_list,
        );
        // Enough bytes for the count, but not enough for a non-null child.
        bad_list[9] = 1;
        reject(
            DataType::List(Arc::new(Field::new("x", DataType::Int64, true))),
            &bad_list,
        );
    }

    #[test]
    fn encoded_import_denial_invalidates_old_key_and_nested_empty_values_round_trip() {
        let pool = MemoryPool::new_named("read growth", 65536);
        let child = DataType::List(Arc::new(Field::new("s", DataType::Utf8, true)));
        let layout = KeyLayout::bind(
            &pool,
            &[DataType::List(Arc::new(Field::new(
                "nested",
                child.clone(),
                true,
            )))],
        )
        .unwrap()
        .unwrap();
        let mut source = KeyWorkspace::new(layout.clone()).unwrap();
        let mut reader = KeyWorkspace::new(layout.clone()).unwrap();
        let mut rows = KeyRows::new(layout.clone()).unwrap();
        reader.encode(&[ScalarValue::Null]).unwrap();
        rows.append(&reader).unwrap();
        source
            .encode(&[ScalarValue::List(
                vec![
                    ScalarValue::Null,
                    ScalarValue::List(vec![], Box::new(DataType::Utf8)),
                    ScalarValue::List(
                        vec![
                            ScalarValue::Null,
                            ScalarValue::Utf8(String::new()),
                            ScalarValue::Utf8("x".repeat(4096)),
                        ],
                        Box::new(DataType::Utf8),
                    ),
                ],
                Box::new(child),
            )])
            .unwrap();
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(reader
            .load_encoded(source.key().unwrap().bytes())
            .unwrap_err()
            .is_memory_limit());
        assert!(reader.key().is_err());
        assert!(rows.append(&reader).is_err());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.key(0).unwrap().bytes(), &[0]);
        drop(pressure);
        reader.load_encoded(source.key().unwrap().bytes()).unwrap();
        assert!(reader.key().unwrap() == source.key().unwrap());
        rows.append(&reader).unwrap();
        assert_eq!(rows.len(), 2);
        drop((rows, reader, source, layout));
        assert_eq!(pool.used(), 0);
        let layout = KeyLayout::bind(&pool, &[]).unwrap().unwrap();
        let mut reader = KeyWorkspace::new(layout.clone()).unwrap();
        reader.load_encoded(&[]).unwrap();
        assert!(reader.key().unwrap().bytes().is_empty());
        assert!(reader.load_encoded(&[0]).is_err());
        assert!(reader.key().is_err());
        drop((reader, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn composite_boundaries_nulls_and_sql_float_equivalence_are_exact() {
        let pool = MemoryPool::new_named("canonical keys", 65536);
        let types = [DataType::Utf8, DataType::Utf8, DataType::Float64];
        let layout = KeyLayout::bind(&pool, &types).unwrap().unwrap();
        let mut workspace = KeyWorkspace::new(layout.clone()).unwrap();
        let mut rows = KeyRows::new(layout.clone()).unwrap();
        for (a, b, number) in [
            (Some("a"), Some("bc"), -0.0),
            (Some("ab"), Some("c"), 0.0),
            (Some("a"), Some("bc"), 0.0),
            (Some("a"), Some("bc"), f64::NAN),
            (Some("a"), Some("bc"), f64::from_bits(0xfff8_0000_0000_0042)),
            (None, Some(""), 0.0),
            (Some(""), None, 0.0),
            (Some(""), Some(""), 0.0),
        ] {
            let string = |value: Option<&str>| {
                value
                    .map(|s| ScalarValue::Utf8(s.into()))
                    .unwrap_or(ScalarValue::Null)
            };
            workspace
                .encode(&[string(a), string(b), ScalarValue::Float64(number.into())])
                .unwrap();
            rows.append(&workspace).unwrap();
        }
        assert!(rows.key(0).unwrap() == rows.key(2).unwrap());
        assert_eq!(rows.key(0).unwrap().hash64(), rows.key(2).unwrap().hash64());
        assert!(rows.key(3).unwrap() == rows.key(4).unwrap());
        assert_eq!(rows.key(3).unwrap().hash64(), rows.key(4).unwrap().hash64());
        for (a, b) in [(0, 1), (0, 3), (5, 6), (5, 7), (6, 7)] {
            assert!(rows.key(a).unwrap() != rows.key(b).unwrap());
        }
        // An independent byte oracle includes validity and each field's length.
        let mut expected = vec![1];
        expected.extend(1u64.to_le_bytes());
        expected.push(b'a');
        expected.push(1);
        expected.extend(2u64.to_le_bytes());
        expected.extend(b"bc");
        expected.push(1);
        expected.extend(0u64.to_le_bytes());
        assert_eq!(rows.key(0).unwrap().bytes(), expected);
        struct Collision;
        impl Hasher for Collision {
            fn finish(&self) -> u64 {
                0
            }
            fn write(&mut self, _: &[u8]) {}
        }
        let (mut a, mut b) = (Collision, Collision);
        rows.key(0).unwrap().hash(&mut a);
        rows.key(1).unwrap().hash(&mut b);
        assert_eq!(a.finish(), b.finish());
        assert!(
            rows.key(0).unwrap() != rows.key(1).unwrap(),
            "hash collision must not prove equality"
        );
        drop((rows, workspace, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn exact_domains_and_nested_float_keys_preserve_bound_identity() {
        let pool = MemoryPool::new_named("typed keys", 65536);
        let zone: Arc<str> = Arc::from("UTC");
        let types = [
            DataType::Decimal128(38, -3),
            DataType::UInt64,
            DataType::Int64,
            DataType::Timestamp(TimeUnit::Nanosecond, Some(zone.clone())),
            DataType::List(Arc::new(Field::new("element", DataType::Float32, true))),
        ];
        let layout = KeyLayout::bind(&pool, &types).unwrap().unwrap();
        let mut workspace = KeyWorkspace::new(layout.clone()).unwrap();
        let mut rows = KeyRows::new(layout.clone()).unwrap();
        let exact = (1i128 << 110) + 17;
        for (coefficient, zero, nan) in [
            (exact, -0.0, f32::NAN),
            (exact + 1, 0.0, f32::NAN),
            (exact, 0.0, f32::from_bits(0xffc0_0042)),
        ] {
            let values = [
                ScalarValue::Decimal128(DecimalValue::new(coefficient, -3)),
                ScalarValue::UInt64(u64::MAX),
                ScalarValue::Int64(i64::MIN),
                ScalarValue::Timestamp(TimestampValue::new(
                    i64::MAX,
                    TimeUnit::Nanosecond,
                    Some(zone.clone()),
                )),
                ScalarValue::List(
                    vec![
                        ScalarValue::Float32(zero.into()),
                        ScalarValue::Float32(nan.into()),
                        ScalarValue::Null,
                    ],
                    Box::new(DataType::Float32),
                ),
            ];
            workspace.encode(&values).unwrap();
            rows.append(&workspace).unwrap();
        }
        assert!(rows.key(0).unwrap() != rows.key(1).unwrap());
        assert!(rows.key(0).unwrap() == rows.key(2).unwrap());
        assert_eq!(&rows.key(0).unwrap().bytes()[1..17], exact.to_le_bytes());
        assert_eq!(
            &rows.key(0).unwrap().bytes()[18..26],
            u64::MAX.to_le_bytes()
        );
        assert_eq!(
            &rows.key(0).unwrap().bytes()[27..35],
            i64::MIN.to_le_bytes()
        );
        assert_eq!(
            &rows.key(0).unwrap().bytes()[36..44],
            i64::MAX.to_le_bytes()
        );
        let another = KeyLayout::bind(&pool, &types).unwrap().unwrap();
        let mut other_workspace = KeyWorkspace::new(another.clone()).unwrap();
        other_workspace
            .encode(&[const { ScalarValue::Null }; 5])
            .unwrap();
        workspace.encode(&[const { ScalarValue::Null }; 5]).unwrap();
        assert!(other_workspace.key().unwrap() != workspace.key().unwrap());
        assert!(rows.append(&other_workspace).is_err());
        assert!(workspace
            .encode(&[
                ScalarValue::Decimal128(DecimalValue::new(exact, 0)),
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null,
                ScalarValue::Null
            ])
            .is_err());
        assert!(workspace.key().is_err());
        drop((rows, workspace, layout, other_workspace, another));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn partial_growth_and_failed_encoding_do_not_publish_a_key() {
        let pool = MemoryPool::new_named("key growth", 16384);
        let layout = KeyLayout::bind(&pool, &[DataType::Boolean])
            .unwrap()
            .unwrap();
        let mut workspace = KeyWorkspace::new(layout.clone()).unwrap();
        let mut rows = KeyRows::new(layout.clone()).unwrap();
        workspace.encode(&[ScalarValue::Boolean(true)]).unwrap();
        rows.append(&workspace).unwrap();
        // Four bytes allow payload growth, but not the second offset allocation.
        let pressure = pool.allocate(pool.available() - 4).unwrap();
        assert!(rows.append(&workspace).unwrap_err().is_memory_limit());
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.key(0).unwrap().bytes(), &[1, 1]);
        drop(pressure);
        workspace.encode(&[ScalarValue::Boolean(false)]).unwrap();
        rows.append(&workspace).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows.key(1).unwrap().bytes(), &[1, 0]);
        rows.truncate(1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.key(0).unwrap().bytes(), &[1, 1]);
        rows.truncate(0);
        assert_eq!(rows.len(), 0);
        assert!(rows.key(0).is_err());
        drop((rows, workspace, layout));
        assert_eq!(pool.used(), 0);

        let layout = KeyLayout::bind(&pool, &[DataType::Utf8]).unwrap().unwrap();
        let mut workspace = KeyWorkspace::new(layout.clone()).unwrap();
        let mut rows = KeyRows::new(layout.clone()).unwrap();
        workspace
            .encode(&[ScalarValue::Utf8("old".into())])
            .unwrap();
        rows.append(&workspace).unwrap();
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(workspace
            .encode(&[ScalarValue::Utf8("x".repeat(1000))])
            .unwrap_err()
            .is_memory_limit());
        assert!(workspace.key().is_err());
        assert!(rows.append(&workspace).is_err());
        assert_eq!(rows.len(), 1);
        drop(pressure);
        workspace
            .encode(&[ScalarValue::Utf8("old".into())])
            .unwrap();
        assert!(rows.key(0).unwrap() == workspace.key().unwrap());
        drop((rows, workspace, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn global_and_null_keys_are_valid_and_unsupported_encodings_refuse_binding() {
        let pool = MemoryPool::new_named("empty key", 16384);
        assert!(KeyLayout::bind(
            &pool,
            &[DataType::Dictionary(
                Box::new(DataType::Int32),
                Box::new(DataType::Utf8)
            )]
        )
        .unwrap()
        .is_none());
        assert_eq!(pool.used(), 0);
        let layout = KeyLayout::bind(&pool, &[]).unwrap().unwrap();
        let mut workspace = KeyWorkspace::new(layout.clone()).unwrap();
        let mut rows = KeyRows::new(layout.clone()).unwrap();
        workspace.encode(&[]).unwrap();
        rows.append(&workspace).unwrap();
        rows.append(&workspace).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows.key(0).unwrap().bytes().is_empty());
        assert!(rows.key(0).unwrap() == rows.key(1).unwrap());
        assert!(workspace.encode(&[ScalarValue::Null]).is_err());
        assert!(workspace.key().is_err());
        drop((rows, workspace, layout));
        assert_eq!(pool.used(), 0);
    }
}
