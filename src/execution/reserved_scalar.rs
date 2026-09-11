//! An owned scalar payload whose reservation cannot be detached from its value.
//!
//! Intended for selected aggregate values and state transfer. Numeric slots can
//! remain inline; this owner is for payloads that must outlive their input.
use super::{MemoryPool, MemoryReservation};
use crate::{planner::ScalarValue, QueryError, Result};
use arrow::datatypes::{DataType, Field};
use std::mem::size_of;

mod decode;

const OWNER_BYTES: usize = 512;
const MAX_DEPTH: usize = 64;

#[derive(Debug)]
pub(crate) struct ReservedScalar {
    // Drop the payload before releasing its lease. Sharing the owner (rather
    // than cloning as_scalar()) shares the same storage and reservation.
    value: ScalarValue,
    _reservation: MemoryReservation,
}

/// Bind type metadata once per aggregate slot layout, not once per group.
#[derive(Debug)]
pub(crate) struct ReservedDataType {
    value: DataType,
    _reservation: MemoryReservation,
}

impl ReservedDataType {
    pub(crate) fn try_copy(pool: &MemoryPool, value: &DataType) -> Result<Self> {
        let mut admission = Admission {
            lease: pool.allocate(OWNER_BYTES + size_of::<Self>())?,
        };
        admission.data_type(value, 0)?;
        Ok(Self {
            value: value.clone(),
            _reservation: admission.lease,
        })
    }

    pub(crate) fn as_type(&self) -> &DataType {
        &self.value
    }
}

fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("selected scalar ownership: {message}"))
}

struct Admission {
    lease: MemoryReservation,
}

impl Admission {
    fn add(&mut self, bytes: usize) -> Result<()> {
        let total = self
            .lease
            .size()
            .checked_add(bytes)
            .ok_or_else(|| invalid("size overflow"))?;
        self.lease.resize(total)
    }

    fn allocation(&mut self, bytes: usize) -> Result<()> {
        if bytes > isize::MAX as usize {
            return Err(invalid("allocation exceeds addressable size"));
        }
        self.add(
            bytes
                .checked_add(OWNER_BYTES)
                .ok_or_else(|| invalid("size overflow"))?,
        )
    }

    fn field(&mut self, field: &Field, depth: usize) -> Result<()> {
        // Retained Arc metadata is charged conservatively per occurrence, even
        // when different fields share an allocation. Admission bounds traversal;
        // never call Arrow's unchecked recursively summing size() helpers.
        self.allocation(size_of::<Field>())?;
        self.allocation(field.name().capacity())?;
        let metadata = field.metadata();
        // Hash table slots, load-factor slack and control bytes, plus the owner
        // allowance. This is an admission bound, not allocator-exact RSS.
        self.allocation(
            metadata
                .capacity()
                .checked_mul(2 * (size_of::<(String, String)>() + 1))
                .ok_or_else(|| invalid("metadata size overflow"))?,
        )?;
        for (key, value) in metadata {
            self.allocation(key.capacity())?;
            self.allocation(value.capacity())?;
        }
        self.data_type(field.data_type(), depth + 1)
    }

    fn data_type(&mut self, data_type: &DataType, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            return Err(invalid("type nesting exceeds ownership traversal limit"));
        }
        self.allocation(size_of::<DataType>())?;
        match data_type {
            DataType::Timestamp(_, Some(zone)) => self.allocation(zone.len())?,
            DataType::List(field)
            | DataType::ListView(field)
            | DataType::LargeList(field)
            | DataType::LargeListView(field)
            | DataType::FixedSizeList(field, _)
            | DataType::Map(field, _) => self.field(field, depth)?,
            DataType::Struct(fields) => {
                self.allocation(
                    fields
                        .len()
                        .checked_mul(size_of::<std::sync::Arc<Field>>())
                        .ok_or_else(|| invalid("field size overflow"))?,
                )?;
                for field in fields {
                    self.field(field, depth)?;
                }
            }
            DataType::Union(fields, _) => {
                self.allocation(
                    fields
                        .len()
                        .checked_mul(size_of::<(i8, std::sync::Arc<Field>)>())
                        .ok_or_else(|| invalid("union size overflow"))?,
                )?;
                for (_, field) in fields.iter() {
                    self.field(field, depth)?;
                }
            }
            DataType::Dictionary(key, value) => {
                self.data_type(key, depth + 1)?;
                self.data_type(value, depth + 1)?;
            }
            DataType::RunEndEncoded(ends, values) => {
                self.field(ends, depth)?;
                self.field(values, depth)?;
            }
            // Every remaining Arrow58 type is inline or has no owned payload.
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
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Timestamp(_, None)
            | DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::Binary
            | DataType::FixedSizeBinary(_)
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Decimal32(..)
            | DataType::Decimal64(..)
            | DataType::Decimal128(..)
            | DataType::Decimal256(..) => {}
        }
        Ok(())
    }

    fn scalar(&mut self, value: &ScalarValue, depth: usize) -> Result<()> {
        if depth > MAX_DEPTH {
            return Err(invalid("value nesting exceeds ownership traversal limit"));
        }
        match value {
            ScalarValue::Utf8(value) => self.allocation(value.len())?,
            ScalarValue::Timestamp(value) => {
                if let Some(zone) = &value.timezone {
                    self.allocation(zone.len())?;
                }
            }
            ScalarValue::List(values, data_type) => {
                self.allocation(
                    values
                        .len()
                        .checked_mul(size_of::<ScalarValue>())
                        .ok_or_else(|| invalid("list size overflow"))?,
                )?;
                self.data_type(data_type, depth + 1)?;
                for value in values {
                    self.scalar(value, depth + 1)?;
                }
            }
            ScalarValue::Null
            | ScalarValue::Boolean(_)
            | ScalarValue::Int8(_)
            | ScalarValue::Int16(_)
            | ScalarValue::Int32(_)
            | ScalarValue::Int64(_)
            | ScalarValue::UInt8(_)
            | ScalarValue::UInt16(_)
            | ScalarValue::UInt32(_)
            | ScalarValue::UInt64(_)
            | ScalarValue::Float32(_)
            | ScalarValue::Float64(_)
            | ScalarValue::Decimal128(_)
            | ScalarValue::Date32(_)
            | ScalarValue::Date64(_)
            | ScalarValue::Interval(_) => {}
        }
        Ok(())
    }
}

fn copy_payload(
    value: &ScalarValue,
    before_allocation: &mut impl FnMut() -> Result<()>,
) -> Result<ScalarValue> {
    Ok(match value {
        ScalarValue::Utf8(value) => {
            before_allocation()?;
            let mut copy = String::new();
            copy.try_reserve_exact(value.len())
                .map_err(|e| invalid(&format!("string allocation refused: {e}")))?;
            if copy.capacity() != value.len() {
                return Err(invalid("string capacity differs from admission"));
            }
            copy.push_str(value);
            ScalarValue::Utf8(copy)
        }
        ScalarValue::List(values, data_type) => {
            before_allocation()?;
            let mut copy = Vec::new();
            copy.try_reserve_exact(values.len())
                .map_err(|e| invalid(&format!("list allocation refused: {e}")))?;
            if copy.capacity() != values.len() {
                return Err(invalid("list capacity differs from admission"));
            }
            for value in values {
                copy.push(copy_payload(value, before_allocation)?);
            }
            // Arrow58 metadata clone shares Fields/FieldRef/timezone allocations;
            // only bounded dictionary boxes are recursively copied. Every box
            // and all retained shared metadata were admitted before this call.
            ScalarValue::List(copy, data_type.clone())
        }
        // All other payloads are inline, except the shallow timestamp Arc clone.
        ScalarValue::Null
        | ScalarValue::Boolean(_)
        | ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt64(_)
        | ScalarValue::Float32(_)
        | ScalarValue::Float64(_)
        | ScalarValue::Decimal128(_)
        | ScalarValue::Date32(_)
        | ScalarValue::Date64(_)
        | ScalarValue::Interval(_)
        | ScalarValue::Timestamp(_) => value.clone(),
    })
}

impl ReservedScalar {
    pub(crate) fn try_copy(pool: &MemoryPool, value: &ScalarValue) -> Result<Self> {
        Self::copy_with(pool, value, &mut || Ok(()))
    }

    fn copy_with(
        pool: &MemoryPool,
        value: &ScalarValue,
        before_allocation: &mut impl FnMut() -> Result<()>,
    ) -> Result<Self> {
        let mut admission = Admission {
            lease: pool.allocate(OWNER_BYTES + size_of::<Self>())?,
        };
        admission.scalar(value, 0)?;
        let value = copy_payload(value, before_allocation)?;
        Ok(Self {
            value,
            _reservation: admission.lease,
        })
    }

    pub(crate) fn as_scalar(&self) -> &ScalarValue {
        &self.value
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn payload_failure_drops_partial_copy_before_releasing_reservation() {
        let pool = MemoryPool::new_named("selected copy failure", 1 << 20);
        let value = ScalarValue::List(
            vec![
                ScalarValue::Utf8("first".repeat(100)),
                ScalarValue::Utf8("second".repeat(100)),
            ],
            Box::new(DataType::Utf8),
        );
        let mut attempts = 0;
        let error = ReservedScalar::copy_with(&pool, &value, &mut || {
            attempts += 1;
            assert!(pool.used() > 1000, "all ownership must precede copying");
            if attempts == 3 {
                Err(invalid("injected allocation refusal"))
            } else {
                Ok(())
            }
        })
        .unwrap_err();
        assert_eq!(attempts, 3);
        assert!(error.to_string().contains("injected allocation refusal"));
        assert!(
            !error.is_memory_limit(),
            "allocator errors must not trigger spill recovery"
        );
        assert_eq!(pool.used(), 0);
        let recovered = ReservedScalar::try_copy(&pool, &value).unwrap();
        assert_eq!(recovered.as_scalar(), &value);
        drop(recovered);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn exact_scalar_bits_and_timestamp_metadata_survive_copy() {
        use crate::planner::{DecimalValue, TimestampValue};
        use arrow::datatypes::TimeUnit;
        let pool = MemoryPool::new_named("selected exact values", 1 << 20);
        let timestamp = ScalarValue::Timestamp(TimestampValue::new(
            i64::MIN + 1,
            TimeUnit::Nanosecond,
            Some(Arc::from("America/Los_Angeles")),
        ));
        for value in [
            ScalarValue::Null,
            ScalarValue::UInt64(u64::MAX),
            ScalarValue::Decimal128(DecimalValue::new(i128::MAX, -3)),
            timestamp,
            ScalarValue::Float64(f64::from_bits(0xfff8_0000_0000_0042).into()),
            ScalarValue::Utf8(String::new()),
            ScalarValue::List(vec![], Box::new(DataType::Int64)),
        ] {
            let copy = ReservedScalar::try_copy(&pool, &value).unwrap();
            assert_eq!(copy.as_scalar(), &value);
            if let ScalarValue::Float64(value) = copy.as_scalar() {
                assert_eq!(value.into_inner().to_bits(), 0xfff8_0000_0000_0042);
            }
            drop(copy);
            assert_eq!(pool.used(), 0);
        }
    }

    #[test]
    fn selected_payload_owner_survives_source_and_shared_transfer() {
        let pool = MemoryPool::new_named("selected values", 1 << 20);
        let value = ScalarValue::List(
            vec![
                ScalarValue::Utf8("a long selected string".repeat(100)),
                ScalarValue::Null,
            ],
            Box::new(DataType::Utf8),
        );
        let owner = Arc::new(ReservedScalar::try_copy(&pool, &value).unwrap());
        assert_eq!(owner.as_scalar(), &value);
        let used = pool.used();
        assert!(used > 2000);
        drop(value);
        let transferred = owner.clone();
        drop(owner);
        assert_eq!(pool.used(), used);
        match transferred.as_scalar() {
            ScalarValue::List(values, data_type) => {
                assert_eq!(data_type.as_ref(), &DataType::Utf8);
                assert_eq!(
                    values[0],
                    ScalarValue::Utf8("a long selected string".repeat(100))
                );
                assert_eq!(values[1], ScalarValue::Null);
            }
            _ => panic!("lost list payload"),
        }
        drop(transferred);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn replacement_denial_keeps_old_value_and_releases_partial_admission() {
        let pool = MemoryPool::new_named("selected values", 8192);
        let old = ReservedScalar::try_copy(&pool, &ScalarValue::Utf8("old".into())).unwrap();
        let used = pool.used();
        let replacement = ScalarValue::List(
            vec![
                ScalarValue::Utf8("x".repeat(100)),
                ScalarValue::Utf8("y".repeat(8192)),
            ],
            Box::new(DataType::Utf8),
        );
        let mut allocation_attempts = 0;
        let error = ReservedScalar::copy_with(&pool, &replacement, &mut || {
            allocation_attempts += 1;
            Ok(())
        })
        .unwrap_err();
        assert_eq!(
            allocation_attempts, 0,
            "denied admission must precede payload copying"
        );
        assert!(error.is_memory_limit());
        assert_eq!(old.as_scalar(), &ScalarValue::Utf8("old".into()));
        assert_eq!(pool.used(), used);
        drop(old);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn retained_type_metadata_and_excessive_depth_are_accounted() {
        let pool = MemoryPool::new_named("selected metadata", 8192);
        let field = Field::new("x".repeat(8192), DataType::Int64, true);
        let value = ScalarValue::List(vec![], Box::new(DataType::Struct(vec![field].into())));
        assert!(ReservedScalar::try_copy(&pool, &value)
            .unwrap_err()
            .is_memory_limit());
        assert_eq!(pool.used(), 0);
        let mut nested = ScalarValue::Null;
        for _ in 0..70 {
            nested = ScalarValue::List(vec![nested], Box::new(DataType::Null));
        }
        let pool = MemoryPool::new_named("selected depth", 1 << 20);
        let error = ReservedScalar::try_copy(&pool, &nested).unwrap_err();
        assert!(error.to_string().contains("nesting"));
        assert_eq!(pool.used(), 0);
        let mut data_type = DataType::Int64;
        for _ in 0..70 {
            data_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(data_type));
        }
        let value = ScalarValue::List(vec![], Box::new(data_type));
        let error = ReservedScalar::try_copy(&pool, &value).unwrap_err();
        assert!(error.to_string().contains("type nesting"));
        assert_eq!(pool.used(), 0);
    }
}
