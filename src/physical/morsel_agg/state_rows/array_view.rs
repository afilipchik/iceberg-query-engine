//! Checked borrowed inputs; floating conversion is bound to the state codec.
use super::FixedStateCodec;
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
    DecimalFloat(&'a Decimal128Array, f64),
}
impl<'a> FixedArrayView<'a> {
    pub(super) fn bind_for_codec(array: &'a dyn Array, codec: FixedStateCodec) -> Option<Self> {
        let view = Self::bind(array, matches!(codec, FixedStateCodec::Count))?;
        match (view, codec) {
            (
                Self::Decimal128(array, scale),
                FixedStateCodec::Sum | FixedStateCodec::Avg | FixedStateCodec::Variance,
            ) => {
                // Floating states already convert each decimal through this exact
                // multiplication. Hoist only scale metadata, preserving row order
                // and leaving exact decimal states and dictionary fallback alone.
                Some(Self::DecimalFloat(array, 10_f64.powi(-i32::from(scale))))
            }
            _ => Some(view),
        }
    }
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
            Self::DecimalFloat(a, factor) => {
                if a.is_null(row) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float64((a.value(row) as f64 * factor).into())
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn decimal_floating_views_preserve_slices_extremes_and_dictionary_fallback() {
        let maximum = 10i128.pow(38) - 1;
        for scale in [-128, -3, 0, 3, 38] {
            let array = Decimal128Array::from(vec![
                Some(99),
                Some(maximum),
                None,
                Some(-maximum),
                Some(99),
            ])
            .with_precision_and_scale(38, scale)
            .unwrap()
            .slice(1, 3);
            for codec in [FixedStateCodec::Avg, FixedStateCodec::Variance] {
                let view = FixedArrayView::bind_for_codec(&array, codec).unwrap();
                for (row, coefficient) in [(0, maximum), (2, -maximum)] {
                    let ScalarValue::Float64(value) = view.value(row) else {
                        panic!("float input")
                    };
                    let expected = std::hint::black_box(coefficient) as f64
                        * 10_f64.powi(-i32::from(std::hint::black_box(scale)));
                    assert_eq!(value.to_bits(), expected.to_bits());
                }
                assert!(matches!(view.value(1), ScalarValue::Null));
                let dictionary = DictionaryArray::<arrow::datatypes::Int32Type>::try_new(
                    Int32Array::from(vec![Some(0), Some(1), None, Some(2)]),
                    std::sync::Arc::new(array.clone()),
                )
                .unwrap();
                assert!(FixedArrayView::bind_for_codec(&dictionary, codec).is_none());
                let empty = array.slice(0, 0);
                assert!(FixedArrayView::bind_for_codec(&empty, codec).is_some());
            }
        }
    }
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
