//! Exact scalar timestamp domain. Ticks never pass through floating point.
use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use std::{fmt, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TimestampValue {
    pub ticks: i64,
    pub unit: TimeUnit,
    pub timezone: Option<Arc<str>>,
}

impl TimestampValue {
    pub fn new(ticks: i64, unit: TimeUnit, timezone: Option<Arc<str>>) -> Self {
        Self {
            ticks,
            unit,
            timezone,
        }
    }

    pub fn data_type(&self) -> DataType {
        DataType::Timestamp(self.unit.clone(), self.timezone.clone())
    }

    pub fn from_array(array: &dyn Array, row: usize) -> Option<Self> {
        if row >= array.len() || array.is_null(row) {
            return None;
        }
        let DataType::Timestamp(unit, timezone) = array.data_type() else {
            return None;
        };
        let ticks = match unit {
            TimeUnit::Second => array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()?
                .value(row),
            TimeUnit::Millisecond => array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()?
                .value(row),
            TimeUnit::Microsecond => array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()?
                .value(row),
            TimeUnit::Nanosecond => array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()?
                .value(row),
        };
        Some(Self::new(ticks, unit.clone(), timezone.clone()))
    }

    /// Reinterpret already constructed raw counts without conversion or copying.
    /// Shared buffers retain their original reservation owners.
    pub(crate) fn wrap_counts(&self, counts: &Int64Array) -> ArrayRef {
        macro_rules! wrap {
            ($ty:ty) => {
                Arc::new(
                    <$ty>::new(counts.values().clone(), counts.nulls().cloned())
                        .with_timezone_opt(self.timezone.clone()),
                ) as ArrayRef
            };
        }
        match self.unit {
            TimeUnit::Second => wrap!(TimestampSecondArray),
            TimeUnit::Millisecond => wrap!(TimestampMillisecondArray),
            TimeUnit::Microsecond => wrap!(TimestampMicrosecondArray),
            TimeUnit::Nanosecond => wrap!(TimestampNanosecondArray),
        }
    }

    pub(crate) fn expand(&self, rows: usize) -> ArrayRef {
        self.wrap_counts(&Int64Array::from(vec![self.ticks; rows]))
    }
}

impl From<i64> for TimestampValue {
    fn from(ticks: i64) -> Self {
        Self::new(ticks, TimeUnit::Microsecond, None)
    }
}

impl fmt::Display for TimestampValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {:?} {:?}", self.ticks, self.unit, self.timezone)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::MemoryPool,
        planner::{reserved_literal, ScalarValue},
    };

    #[test]
    fn exact_timestamp_expansion_retains_domain_ticks_and_buffer_lease() {
        for unit in [
            TimeUnit::Second,
            TimeUnit::Millisecond,
            TimeUnit::Microsecond,
            TimeUnit::Nanosecond,
        ] {
            for timezone in [
                None,
                Some(Arc::<str>::from("UTC")),
                Some(Arc::<str>::from("America/Los_Angeles")),
            ] {
                for ticks in [i64::MIN, -1, 0, 1_700_000_000_123_456_789, i64::MAX] {
                    let value = TimestampValue::new(ticks, unit.clone(), timezone.clone());
                    let pool = MemoryPool::new(1024);
                    let array =
                        reserved_literal::expand(&pool, &ScalarValue::Timestamp(value.clone()), 9)
                            .unwrap();
                    assert_eq!(
                        array.data_type(),
                        &DataType::Timestamp(unit.clone(), timezone.clone())
                    );
                    for row in 0..9 {
                        assert_eq!(
                            TimestampValue::from_array(array.as_ref(), row),
                            Some(value.clone())
                        );
                    }
                    let raw = arrow::compute::cast(array.as_ref(), &DataType::Int64).unwrap();
                    assert!(raw
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .values()
                        .iter()
                        .all(|v| *v == ticks));
                    let escaped = array.to_data().buffers()[0].slice(8);
                    drop(raw);
                    drop(array);
                    assert!(pool.used() >= 9 * 8);
                    drop(escaped);
                    assert_eq!(pool.used(), 0);
                    assert!(
                        reserved_literal::expand(&pool, &ScalarValue::Timestamp(value), 65)
                            .is_err()
                    );
                    assert_eq!(pool.used(), 0);
                }
            }
        }
    }
}
