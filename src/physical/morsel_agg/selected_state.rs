//! Bound selected partial states. Preparation may allocate; commit and sharing
//! within a bound layout do not. Numeric values remain inline.
use super::compare_scalar_values;
use crate::execution::{
    reserved_scalar::{ReservedDataType, ReservedScalar},
    MemoryPool,
};
use crate::planner::{AggregateFunction, ScalarValue};
use crate::{QueryError, Result};
use arrow::datatypes::DataType;
use std::{cmp::Ordering, sync::Arc};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Selection {
    Min,
    Max,
    First,
}

#[derive(Debug)]
pub(super) struct BoundSelection {
    selection: Selection,
    input_type: ReservedDataType,
    pool: MemoryPool,
}

fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("selected aggregate state: {message}"))
}

pub(super) fn supported_type(data_type: &DataType, lists: bool, depth: usize) -> bool {
    if depth > 64 {
        return false;
    }
    match data_type {
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
        | DataType::Utf8
        | DataType::Date32
        | DataType::Date64
        | DataType::Timestamp(..) => true,
        DataType::List(field) if lists => supported_type(field.data_type(), true, depth + 1),
        _ => false,
    }
}

impl BoundSelection {
    pub(super) fn input_type(&self) -> &DataType {
        self.input_type.as_type()
    }
    pub(super) fn validate_payload(&self, bytes: &[u8]) -> Result<usize> {
        ReservedScalar::validate_encoded(self.input_type.as_type(), bytes)
    }
    /// Restore the selected value, not an input to selection or a finalized
    /// aggregate. The enclosing row transaction owns this pending payload.
    pub(super) fn decode_payload(&self, bytes: &[u8]) -> Result<(Option<Payload>, usize)> {
        if let Some((value, consumed)) =
            ReservedScalar::try_decode_inline(self.input_type.as_type(), bytes)?
        {
            let payload = if matches!(value, ScalarValue::Null) {
                None
            } else {
                Some(Payload::Inline(value))
            };
            return Ok((payload, consumed));
        }
        let (value, consumed) =
            ReservedScalar::try_decode(&self.pool, self.input_type.as_type(), bytes)?;
        Ok((Some(Payload::Owned(Arc::new(value))), consumed))
    }

    pub(super) fn bind(
        pool: &MemoryPool,
        function: AggregateFunction,
        input_type: &DataType,
        distinct: bool,
    ) -> Result<Option<Arc<Self>>> {
        if distinct {
            return Ok(None);
        }
        let selection = match function {
            AggregateFunction::Min => Selection::Min,
            AggregateFunction::Max => Selection::Max,
            AggregateFunction::AnyValue | AggregateFunction::Arbitrary => Selection::First,
            _ => return Ok(None),
        };
        let supported = supported_type(input_type, selection == Selection::First, 0);
        if !supported {
            return Ok(None);
        }
        // Large type payloads are admitted before cloning; the owner allowance
        // also covers this fixed-size Arc/layout allocation.
        let input_type = ReservedDataType::try_copy(pool, input_type)?;
        Ok(Some(Arc::new(Self {
            selection,
            input_type,
            pool: pool.clone(),
        })))
    }
}

#[derive(Debug)]
pub(super) enum Payload {
    // Constructed only by prepare_payload: no String/List/timezone allocation
    // can enter this variant. Therefore clone during merge is allocation-free.
    Inline(ScalarValue),
    Owned(Arc<ReservedScalar>),
}

impl Payload {
    fn value(&self) -> &ScalarValue {
        match self {
            Self::Inline(value) => value,
            Self::Owned(value) => value.as_scalar(),
        }
    }
    fn share(&self) -> Self {
        match self {
            Self::Inline(value) => Self::Inline(value.clone()),
            Self::Owned(value) => Self::Owned(value.clone()),
        }
    }
}

fn prepare_payload(pool: &MemoryPool, value: &ScalarValue) -> Result<Payload> {
    match value {
        ScalarValue::Utf8(_) | ScalarValue::List(..) | ScalarValue::Timestamp(_) => Ok(
            Payload::Owned(Arc::new(ReservedScalar::try_copy(pool, value)?)),
        ),
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
        | ScalarValue::Interval(_) => Ok(Payload::Inline(value.clone())),
    }
}

pub(super) fn matches_type(value: &ScalarValue, data_type: &DataType, depth: usize) -> bool {
    if depth > 64 {
        return false;
    }
    match (value, data_type) {
        (ScalarValue::Null, _) => true,
        (ScalarValue::Boolean(_), DataType::Boolean)
        | (ScalarValue::Int8(_), DataType::Int8)
        | (ScalarValue::Int16(_), DataType::Int16)
        | (ScalarValue::Int32(_), DataType::Int32)
        | (ScalarValue::Int64(_), DataType::Int64)
        | (ScalarValue::UInt8(_), DataType::UInt8)
        | (ScalarValue::UInt16(_), DataType::UInt16)
        | (ScalarValue::UInt32(_), DataType::UInt32)
        | (ScalarValue::UInt64(_), DataType::UInt64)
        | (ScalarValue::Float32(_), DataType::Float32)
        | (ScalarValue::Float64(_), DataType::Float64)
        | (ScalarValue::Utf8(_), DataType::Utf8)
        | (ScalarValue::Date32(_), DataType::Date32)
        | (ScalarValue::Date64(_), DataType::Date64) => true,
        (ScalarValue::Decimal128(value), DataType::Decimal128(_, scale)) => value.scale() == *scale,
        (ScalarValue::Timestamp(value), DataType::Timestamp(unit, zone)) => {
            value.unit == *unit && value.timezone == *zone
        }
        (ScalarValue::List(values, element_type), DataType::List(field)) => {
            element_type.as_ref() == field.data_type()
                && values.iter().all(|value| {
                    (!matches!(value, ScalarValue::Null) || field.is_nullable())
                        && matches_type(value, field.data_type(), depth + 1)
                })
        }
        _ => false,
    }
}

#[derive(Debug)]
pub(super) struct SelectedState {
    layout: Arc<BoundSelection>,
    value: Option<Payload>,
}

/// Borrowing the destination prevents stale prepared replacements. Dropping a
/// token rolls back by destroying only its uncommitted replacement. A row can
/// prepare every selected slot before committing any selected/fixed state.
pub(super) struct PreparedSelection<'a> {
    destination: &'a mut SelectedState,
    replacement: Option<Payload>,
}

impl PreparedSelection<'_> {
    pub(super) fn commit(self) {
        if let Some(value) = self.replacement {
            self.destination.value = Some(value);
        }
    }
}

impl SelectedState {
    pub(super) fn new(layout: Arc<BoundSelection>) -> Self {
        Self {
            layout,
            value: None,
        }
    }
    pub(super) fn value(&self) -> Option<&ScalarValue> {
        self.value.as_ref().map(Payload::value)
    }

    fn wants(&self, value: &ScalarValue) -> Result<bool> {
        if !matches_type(value, self.layout.input_type.as_type(), 0) {
            return Err(invalid("value differs from bound input type"));
        }
        if matches!(value, ScalarValue::Null) {
            return Ok(false);
        }
        let Some(current) = self.value() else {
            return Ok(true);
        };
        Ok(match self.layout.selection {
            Selection::First => false,
            Selection::Min => compare_scalar_values(value, current) == Ordering::Less,
            Selection::Max => compare_scalar_values(value, current) == Ordering::Greater,
        })
    }

    /// Compare borrowed Arrow values before admitting a retained replacement.
    /// Numeric candidates stay inline; variable winners decode exactly once into
    /// their final reservation owner, then transfer into the row transaction.
    pub(super) fn prepare_array_replacement(
        &self,
        array: &dyn arrow::array::Array,
        row: usize,
        scratch: &mut crate::execution::reserved_vec::ReservedVec<u8>,
    ) -> Result<Option<Payload>> {
        use super::key_rows::arrow_input;
        if !arrow_input::type_matches(array.data_type(), self.layout.input_type.as_type())
            || row >= array.len()
        {
            return Err(invalid("Arrow input type or extent mismatch"));
        }
        let Some((array, row, _)) = arrow_input::resolve(array, row, 0)? else {
            return Ok(None);
        };
        if self.value.is_some() && self.layout.selection == Selection::First {
            return Ok(None);
        }
        if let Some(value) = arrow_input::inline(array, row)? {
            return self.prepare_replacement(&value);
        }
        if let Some(current) = self.value() {
            let comparison = match current {
                ScalarValue::Utf8(value) => {
                    let array = array
                        .as_any()
                        .downcast_ref::<arrow::array::StringArray>()
                        .ok_or_else(|| invalid("Arrow string representation mismatch"))?;
                    array.value(row).cmp(value.as_str())
                }
                ScalarValue::Timestamp(value) => {
                    let mut bytes = [0u8; 9];
                    let mut offset = 0;
                    arrow_input::visit(array, row, true, 0, false, &mut |part| {
                        let end = offset + part.len();
                        bytes
                            .get_mut(offset..end)
                            .ok_or_else(|| invalid("timestamp frame overflow"))?
                            .copy_from_slice(part);
                        offset = end;
                        Ok(())
                    })?;
                    i64::from_le_bytes(bytes[1..].try_into().unwrap()).cmp(&value.ticks)
                }
                _ => return Err(invalid("unsupported borrowed selection comparison")),
            };
            if !matches!(
                (self.layout.selection, comparison),
                (Selection::Min, Ordering::Less) | (Selection::Max, Ordering::Greater)
            ) {
                return Ok(None);
            }
        }
        arrow_input::encode_scalar(array, row, scratch)?;
        let (payload, consumed) = self.layout.decode_payload(scratch.as_slice())?;
        if consumed != scratch.as_slice().len() {
            return Err(invalid("trailing Arrow scalar payload"));
        }
        Ok(payload)
    }

    pub(super) fn prepare_replacement(&self, value: &ScalarValue) -> Result<Option<Payload>> {
        if self.wants(value)? {
            Ok(Some(prepare_payload(&self.layout.pool, value)?))
        } else {
            Ok(None)
        }
    }

    pub(super) fn commit_replacement(&mut self, replacement: Option<Payload>) {
        if let Some(value) = replacement {
            self.value = Some(value);
        }
    }

    pub(super) fn prepare_merge_replacement(&self, source: &Self) -> Result<Option<Payload>> {
        if !Arc::ptr_eq(&self.layout, &source.layout) {
            return Err(invalid("merge requires the same bound layout"));
        }
        match &source.value {
            Some(value) if self.wants(value.value())? => Ok(Some(value.share())),
            _ => Ok(None),
        }
    }

    pub(super) fn prepare(&mut self, value: &ScalarValue) -> Result<PreparedSelection<'_>> {
        let replacement = self.prepare_replacement(value)?;
        Ok(PreparedSelection {
            destination: self,
            replacement,
        })
    }

    pub(super) fn prepare_merge(&mut self, source: &Self) -> Result<PreparedSelection<'_>> {
        let replacement = self.prepare_merge_replacement(source)?;
        Ok(PreparedSelection {
            destination: self,
            replacement,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{DecimalValue, TimestampValue};
    use arrow::datatypes::{Field, TimeUnit};

    #[test]
    fn decoded_selected_payloads_keep_inline_and_owned_admission_contracts() {
        let pool = MemoryPool::new_named("selected decode", 65536);
        let numeric = bound(&pool, AggregateFunction::Min, DataType::Float64);
        let text = bound(&pool, AggregateFunction::Max, DataType::Utf8);
        let mut bytes = vec![1];
        bytes.extend((-0.0f64).to_le_bytes());
        let mut string = vec![1];
        string.extend(4096u64.to_le_bytes());
        string.extend(std::iter::repeat_n(b'x', 4096));
        let pressure = pool.allocate(pool.available()).unwrap();
        let (value, count) = numeric.decode_payload(&bytes).unwrap();
        assert_eq!(count, 9);
        let Some(Payload::Inline(ScalarValue::Float64(value))) = value else {
            panic!("numeric payload acquired an owner");
        };
        assert_eq!(value.into_inner().to_bits(), (-0.0f64).to_bits());
        assert!(matches!(text.decode_payload(&[0]).unwrap(), (None, 1)));
        assert!(text.decode_payload(&string).unwrap_err().is_memory_limit());
        drop(pressure);
        let (value, count) = text.decode_payload(&string).unwrap();
        assert_eq!(count, string.len());
        drop(string);
        let Some(Payload::Owned(value)) = value else {
            panic!("string lost owner");
        };
        assert_eq!(value.as_scalar(), &ScalarValue::Utf8("x".repeat(4096)));
        drop((value, numeric, text));
        assert_eq!(pool.used(), 0);
    }

    fn bound(
        pool: &MemoryPool,
        function: AggregateFunction,
        data_type: DataType,
    ) -> Arc<BoundSelection> {
        BoundSelection::bind(pool, function, &data_type, false)
            .unwrap()
            .unwrap()
    }

    #[test]
    fn later_slot_pressure_rolls_back_all_prepared_replacements() {
        let pool = MemoryPool::new_named("selected row", 65536);
        let layout = bound(&pool, AggregateFunction::Max, DataType::Utf8);
        let mut first = SelectedState::new(layout.clone());
        let mut second = SelectedState::new(layout.clone());
        for state in [&mut first, &mut second] {
            state
                .prepare(&ScalarValue::Utf8("b".into()))
                .unwrap()
                .commit();
        }
        let pressure = pool.allocate(pool.available() - 2000).unwrap();
        let baseline = pool.used();
        let prepared = first.prepare(&ScalarValue::Utf8("c".repeat(100))).unwrap();
        assert!(pool.used() > baseline);
        let error = second
            .prepare(&ScalarValue::Utf8("z".repeat(4096)))
            .err()
            .unwrap();
        assert!(error.is_memory_limit());
        drop(prepared);
        assert_eq!(pool.used(), baseline);
        for state in [&first, &second] {
            assert_eq!(state.value(), Some(&ScalarValue::Utf8("b".into())));
        }
        drop(pressure);
        first
            .prepare(&ScalarValue::Utf8("c".repeat(100)))
            .unwrap()
            .commit();
        second
            .prepare(&ScalarValue::Utf8("z".repeat(4096)))
            .unwrap()
            .commit();
        assert_eq!(first.value(), Some(&ScalarValue::Utf8("c".repeat(100))));
        assert_eq!(second.value(), Some(&ScalarValue::Utf8("z".repeat(4096))));
        drop((first, second, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn merge_shares_winner_without_allocation_and_keeps_its_lease() {
        let pool = MemoryPool::new_named("selected merge", 65536);
        let layout = bound(&pool, AggregateFunction::Min, DataType::Utf8);
        let mut source = SelectedState::new(layout.clone());
        let mut target = SelectedState::new(layout.clone());
        source
            .prepare(&ScalarValue::Utf8("alpha".repeat(100)))
            .unwrap()
            .commit();
        let before = pool.used();
        let pressure = pool.allocate(pool.available()).unwrap();
        target.prepare_merge(&source).unwrap().commit();
        assert_eq!(pool.available(), 0);
        drop(source);
        drop(pressure);
        assert_eq!(pool.used(), before);
        assert_eq!(
            target.value(),
            Some(&ScalarValue::Utf8("alpha".repeat(100)))
        );
        let another_pool = MemoryPool::new_named("selected merge", 65536);
        let other_layout = bound(&another_pool, AggregateFunction::Min, DataType::Utf8);
        let other = SelectedState::new(other_layout.clone());
        assert!(target
            .prepare_merge(&other)
            .err()
            .unwrap()
            .to_string()
            .contains("same bound layout"));
        drop((target, layout));
        assert_eq!(pool.used(), 0);
        drop((other, other_layout));
        assert_eq!(another_pool.used(), 0);
    }

    #[test]
    fn numeric_slots_update_and_merge_with_no_available_pool_memory() {
        let pool = MemoryPool::new_named("selected inline", 65536);
        for (function, expected) in [
            (AggregateFunction::Min, -7.0),
            (AggregateFunction::Max, f64::NAN),
        ] {
            let layout = bound(&pool, function, DataType::Float64);
            let mut first = SelectedState::new(layout.clone());
            let mut second = SelectedState::new(layout.clone());
            let pressure = pool.allocate(pool.available()).unwrap();
            for value in [
                ScalarValue::Null,
                ScalarValue::Float64(f64::NAN.into()),
                ScalarValue::Float64((-7.0).into()),
            ] {
                first.prepare(&value).unwrap().commit();
            }
            second
                .prepare(&ScalarValue::Float64(f64::INFINITY.into()))
                .unwrap()
                .commit();
            first.prepare_merge(&second).unwrap().commit();
            let Some(ScalarValue::Float64(actual)) = first.value() else {
                panic!("missing float result")
            };
            assert!(if expected.is_nan() {
                actual.is_nan()
            } else {
                actual.into_inner() == expected
            });
            assert!(first.prepare(&ScalarValue::Int64(1)).is_err());
            assert_eq!(pool.available(), 0);
            drop((first, second, layout, pressure));
            assert_eq!(pool.used(), 0);
        }
        let layout = bound(&pool, AggregateFunction::Max, DataType::Decimal128(38, -3));
        let mut state = SelectedState::new(layout.clone());
        let pressure = pool.allocate(pool.available()).unwrap();
        let exact = ScalarValue::Decimal128(DecimalValue::new(i128::MAX, -3));
        state.prepare(&exact).unwrap().commit();
        assert_eq!(state.value(), Some(&exact));
        assert!(state
            .prepare(&ScalarValue::Decimal128(DecimalValue::new(1, 0)))
            .is_err());
        drop((state, layout, pressure));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn first_preserves_nested_nulls_and_binding_rejects_unsupported_ordering() {
        let pool = MemoryPool::new_named("selected nested", 65536);
        let data_type = DataType::List(Arc::new(Field::new("element", DataType::Utf8, true)));
        assert!(
            BoundSelection::bind(&pool, AggregateFunction::Min, &data_type, false)
                .unwrap()
                .is_none()
        );
        assert!(
            BoundSelection::bind(&pool, AggregateFunction::AnyValue, &data_type, true)
                .unwrap()
                .is_none()
        );
        let layout = bound(&pool, AggregateFunction::AnyValue, data_type);
        let mut state = SelectedState::new(layout.clone());
        state.prepare(&ScalarValue::Null).unwrap().commit();
        assert!(state.value().is_none());
        let malformed = ScalarValue::List(vec![ScalarValue::Int64(1)], Box::new(DataType::Utf8));
        assert!(state.prepare(&malformed).is_err());
        let value = ScalarValue::List(
            vec![ScalarValue::Utf8("chosen".into()), ScalarValue::Null],
            Box::new(DataType::Utf8),
        );
        state.prepare(&value).unwrap().commit();
        let pressure = pool.allocate(pool.available()).unwrap();
        state
            .prepare(&ScalarValue::List(
                vec![ScalarValue::Utf8("ignored".into())],
                Box::new(DataType::Utf8),
            ))
            .unwrap()
            .commit();
        assert_eq!(state.value(), Some(&value));
        drop((state, layout, pressure));
        assert_eq!(pool.used(), 0);

        let zone: Arc<str> = Arc::from("UTC");
        let layout = bound(
            &pool,
            AggregateFunction::Min,
            DataType::Timestamp(TimeUnit::Nanosecond, Some(zone.clone())),
        );
        let mut state = SelectedState::new(layout.clone());
        assert!(state
            .prepare(&ScalarValue::Timestamp(TimestampValue::new(
                1,
                TimeUnit::Second,
                Some(zone.clone())
            )))
            .is_err());
        let exact = ScalarValue::Timestamp(TimestampValue::new(
            i64::MIN,
            TimeUnit::Nanosecond,
            Some(zone),
        ));
        state.prepare(&exact).unwrap().commit();
        assert_eq!(state.value(), Some(&exact));
        drop((state, layout));
        assert_eq!(pool.used(), 0);
    }
}
