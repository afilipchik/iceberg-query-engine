//! Checked borrowed views for fixed aggregate inputs. No normalization or ownership.
use crate::planner::{DecimalValue, ScalarValue};
use arrow::{array::*, buffer::NullBuffer, datatypes::DataType};

#[derive(Clone, Copy)]
pub(super) enum FixedArrayView<'a> {
    Count {
        nulls: Option<&'a NullBuffer>,
        all_null: bool,
    },
    Boolean(&'a BooleanArray),
    Int8(&'a Int8Array),
    Int16(&'a Int16Array),
    Int32(&'a Int32Array),
    Int64(&'a Int64Array),
    UInt8(&'a UInt8Array),
    UInt16(&'a UInt16Array),
    UInt32(&'a UInt32Array),
    UInt64(&'a UInt64Array),
    Float32(&'a Float32Array),
    Float64(&'a Float64Array),
    Decimal128(&'a Decimal128Array, i8),
}
impl<'a> FixedArrayView<'a> {
    pub(super) fn bind(array: &'a dyn Array, count: bool) -> Option<Self> {
        // Dictionary key validity does not prove logical value validity. Keep
        // recursive codebook resolution in the existing adapter for every code.
        if matches!(array.data_type(), DataType::Dictionary(..)) {
            return None;
        }
        // COUNT can read physical validity only for checked concrete arrays.
        // Unknown layouts keep resolve(), including their original error timing.
        if count && (array.as_any().is::<NullArray>() || array.as_any().is::<StringArray>()) {
            return Some(Self::Count {
                nulls: array.nulls(),
                all_null: array.as_any().is::<NullArray>(),
            });
        }
        let view = match array.data_type() {
            DataType::Boolean => array.as_any().downcast_ref().map(Self::Boolean),
            DataType::Int8 => array.as_any().downcast_ref().map(Self::Int8),
            DataType::Int16 => array.as_any().downcast_ref().map(Self::Int16),
            DataType::Int32 => array.as_any().downcast_ref().map(Self::Int32),
            DataType::Int64 => array.as_any().downcast_ref().map(Self::Int64),
            DataType::UInt8 => array.as_any().downcast_ref().map(Self::UInt8),
            DataType::UInt16 => array.as_any().downcast_ref().map(Self::UInt16),
            DataType::UInt32 => array.as_any().downcast_ref().map(Self::UInt32),
            DataType::UInt64 => array.as_any().downcast_ref().map(Self::UInt64),
            DataType::Float32 => array.as_any().downcast_ref().map(Self::Float32),
            DataType::Float64 => array.as_any().downcast_ref().map(Self::Float64),
            DataType::Decimal128(_, scale) => array
                .as_any()
                .downcast_ref()
                .map(|a| Self::Decimal128(a, *scale)),
            _ => None,
        }?;
        if count {
            Some(Self::Count {
                nulls: array.nulls(),
                all_null: false,
            })
        } else {
            Some(view)
        }
    }
    /// Caller checked the row against the bound common array extent.
    #[inline]
    pub(super) fn value(self, row: usize) -> ScalarValue {
        macro_rules! primitive {
            ($array:expr,$variant:ident) => {
                if $array.is_null(row) {
                    ScalarValue::Null
                } else {
                    ScalarValue::$variant($array.value(row).into())
                }
            };
        }
        match self {
            Self::Count { nulls, all_null } => {
                if all_null || nulls.is_some_and(|n| n.is_null(row)) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Int64(1)
                }
            }
            Self::Boolean(a) => primitive!(a, Boolean),
            Self::Int8(a) => primitive!(a, Int8),
            Self::Int16(a) => primitive!(a, Int16),
            Self::Int32(a) => primitive!(a, Int32),
            Self::Int64(a) => primitive!(a, Int64),
            Self::UInt8(a) => primitive!(a, UInt8),
            Self::UInt16(a) => primitive!(a, UInt16),
            Self::UInt32(a) => primitive!(a, UInt32),
            Self::UInt64(a) => primitive!(a, UInt64),
            Self::Float32(a) => primitive!(a, Float32),
            Self::Float64(a) => primitive!(a, Float64),
            Self::Decimal128(a, scale) => {
                if a.is_null(row) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Decimal128(DecimalValue::new(a.value(row), scale))
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn views_preserve_extreme_scalar_representations_and_decline_dictionaries() {
        for bits in [
            0u64,
            1u64 << 63,
            0x7ff8_0000_0000_0123,
            f64::INFINITY.to_bits(),
            f64::NEG_INFINITY.to_bits(),
        ] {
            let array = Float64Array::from(vec![Some(f64::from_bits(bits)), None]);
            let view = FixedArrayView::bind(&array, false).unwrap();
            let ScalarValue::Float64(value) = view.value(0) else {
                panic!("Float64 domain changed")
            };
            assert_eq!(value.to_bits(), bits);
            assert!(matches!(view.value(1), ScalarValue::Null));
        }
        let array = UInt64Array::from(vec![u64::MAX]);
        assert_eq!(
            FixedArrayView::bind(&array, false).unwrap().value(0),
            ScalarValue::UInt64(u64::MAX)
        );
        for scale in [-3, 0, 3] {
            let coefficient = (1i128 << 100) + 1;
            let array = Decimal128Array::from(vec![coefficient])
                .with_precision_and_scale(38, scale)
                .unwrap();
            let ScalarValue::Decimal128(value) =
                FixedArrayView::bind(&array, false).unwrap().value(0)
            else {
                panic!("decimal domain changed")
            };
            assert_eq!(value.mantissa(), coefficient);
            assert_eq!(value.scale(), scale);
        }
        let array = BooleanArray::from(vec![Some(false), None, Some(true)]);
        let view = FixedArrayView::bind(&array, false).unwrap();
        assert_eq!(view.value(0), ScalarValue::Boolean(false));
        assert_eq!(view.value(2), ScalarValue::Boolean(true));
        assert!(matches!(view.value(1), ScalarValue::Null));
        let nulls = NullArray::new(2);
        let view = FixedArrayView::bind(&nulls, true).unwrap();
        assert!(matches!(view.value(0), ScalarValue::Null));
        let strings = StringArray::from(vec![Some("x"), None]);
        let view = FixedArrayView::bind(&strings, true).unwrap();
        assert_eq!(view.value(0), ScalarValue::Int64(1));
        assert!(matches!(view.value(1), ScalarValue::Null));
        let date = Date32Array::from(vec![None, Some(2)]);
        assert!(FixedArrayView::bind(&date, true).is_none());
        let dictionary = DictionaryArray::<arrow::datatypes::Int32Type>::try_new(
            Int32Array::from(vec![Some(0), None]),
            std::sync::Arc::new(Int64Array::from(vec![None])),
        )
        .unwrap();
        assert!(FixedArrayView::bind(&dictionary, true).is_none());
        assert!(FixedArrayView::bind(&dictionary, false).is_none());
    }
}
