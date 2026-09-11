//! Compact numeric state, separate from the owning ScalarValue accumulator enum.
//! Payload words preserve native numeric bits; this is not an IPC/spill frame.
use super::{state_codec::FixedStateCodec, AccumulatorState};
use crate::{
    planner::{AggregateFunction, ScalarValue},
    QueryError, Result,
};

#[derive(Clone, Copy, Debug)]
pub(super) struct FixedCell {
    words: [u64; 3],
    codec: FixedStateCodec,
    flags: u8,
}

impl FixedCell {
    pub(super) fn empty(codec: FixedStateCodec) -> Self {
        Self {
            words: [0; 3],
            codec,
            flags: 0,
        }
    }

    /// Update the compact words directly; no owning enum is built per input.
    /// StateRows still stages the entire row before publishing any slot.
    #[inline]
    pub(super) fn update(&mut self, value: &ScalarValue) -> Result<()> {
        use super::{
            add_decimal_value, scalar_to_f64, state_rows::checked_add, update_average,
            update_variance,
        };
        if matches!(value, ScalarValue::Null) {
            return Ok(());
        }
        match self.codec {
            FixedStateCodec::Count => {
                self.words[0] = checked_add(self.words[0] as i64, 1)? as u64;
            }
            FixedStateCodec::Sum => {
                if let Some(value) = scalar_to_f64(value) {
                    self.words[0] = (f64::from_bits(self.words[0]) + value).to_bits();
                    self.flags = 1;
                }
            }
            FixedStateCodec::SumInt => {
                let value = match value {
                    ScalarValue::Int8(v) => *v as i64,
                    ScalarValue::Int16(v) => *v as i64,
                    ScalarValue::Int32(v) => *v as i64,
                    ScalarValue::Int64(v) => *v,
                    _ => {
                        return Err(QueryError::Execution(
                            "partial aggregate row: integer SUM input mismatch".into(),
                        ))
                    }
                };
                self.words[0] = checked_add(self.words[0] as i64, value)? as u64;
                self.flags = 1;
            }
            FixedStateCodec::SumDecimal(scale) => {
                let sum = (self.flags & 2 == 0)
                    .then_some((self.words[0] as u128 | (self.words[1] as u128) << 64) as i128);
                let unsigned = if scale == 0 {
                    match value {
                        ScalarValue::UInt8(v) => Some(*v as i128),
                        ScalarValue::UInt16(v) => Some(*v as i128),
                        ScalarValue::UInt32(v) => Some(*v as i128),
                        ScalarValue::UInt64(v) => Some(*v as i128),
                        _ => None,
                    }
                } else {
                    None
                };
                let next = if let Some(value) = unsigned {
                    sum.and_then(|sum| sum.checked_add(value))
                } else {
                    add_decimal_value(sum, scale, value)
                };
                let bits = next.unwrap_or(0) as u128;
                self.words[0] = bits as u64;
                self.words[1] = (bits >> 64) as u64;
                self.flags = 1 | (u8::from(next.is_none()) << 1);
            }
            FixedStateCodec::Avg => {
                let mut count = self.words[1] as i64;
                checked_add(count, 1)?;
                if let Some(value) = scalar_to_f64(value) {
                    let mut sum = f64::from_bits(self.words[0]);
                    update_average(&mut sum, &mut count, value);
                    self.words[0] = sum.to_bits();
                    self.words[1] = count as u64;
                }
            }
            FixedStateCodec::Variance => {
                let mut count = self.words[0] as i64;
                checked_add(count, 1)?;
                if let Some(value) = scalar_to_f64(value) {
                    let mut mean = f64::from_bits(self.words[1]);
                    let mut m2 = f64::from_bits(self.words[2]);
                    update_variance(&mut count, &mut mean, &mut m2, value);
                    self.words = [count as u64, mean.to_bits(), m2.to_bits()];
                }
            }
            FixedStateCodec::BoolAnd | FixedStateCodec::BoolOr => {
                if let ScalarValue::Boolean(value) = value {
                    let seen = self.flags & 1 != 0;
                    let old = self.flags & 2 != 0;
                    let next = if matches!(self.codec, FixedStateCodec::BoolAnd) {
                        (!seen || old) && *value
                    } else {
                        (seen && old) || *value
                    };
                    self.flags = 1 | (u8::from(next) << 1);
                }
            }
        }
        Ok(())
    }

    pub(super) fn state(self) -> AccumulatorState {
        let w = self.words;
        let seen = self.flags & 1 != 0;
        match self.codec {
            FixedStateCodec::Count => AccumulatorState::Count(w[0] as i64),
            FixedStateCodec::Sum => AccumulatorState::Sum(f64::from_bits(w[0]), seen),
            FixedStateCodec::SumInt => AccumulatorState::SumInt(w[0] as i64, seen),
            FixedStateCodec::SumDecimal(scale) => AccumulatorState::SumDecimal {
                coefficient: (self.flags & 2 == 0)
                    .then_some(((w[0] as u128) | ((w[1] as u128) << 64)) as i128),
                scale,
                seen,
            },
            FixedStateCodec::Avg => AccumulatorState::Avg {
                sum: f64::from_bits(w[0]),
                count: w[1] as i64,
            },
            FixedStateCodec::Variance => AccumulatorState::Variance {
                count: w[0] as i64,
                mean: f64::from_bits(w[1]),
                m2: f64::from_bits(w[2]),
            },
            FixedStateCodec::BoolAnd => {
                AccumulatorState::BoolAnd(seen.then_some(self.flags & 2 != 0))
            }
            FixedStateCodec::BoolOr => {
                AccumulatorState::BoolOr(seen.then_some(self.flags & 2 != 0))
            }
        }
    }

    pub(super) fn finalize(self, function: &AggregateFunction) -> Result<ScalarValue> {
        self.state().finalize(function)
    }
}

impl TryFrom<AccumulatorState> for FixedCell {
    type Error = QueryError;
    fn try_from(state: AccumulatorState) -> Result<Self> {
        let (codec, words, flags) = match state {
            AccumulatorState::Count(n) => (FixedStateCodec::Count, [n as u64, 0, 0], 0),
            AccumulatorState::Sum(n, seen) => {
                (FixedStateCodec::Sum, [n.to_bits(), 0, 0], u8::from(seen))
            }
            AccumulatorState::SumInt(n, seen) => {
                (FixedStateCodec::SumInt, [n as u64, 0, 0], u8::from(seen))
            }
            AccumulatorState::SumDecimal {
                coefficient,
                scale,
                seen,
            } => {
                let bits = coefficient.unwrap_or(0) as u128;
                (
                    FixedStateCodec::SumDecimal(scale),
                    [bits as u64, (bits >> 64) as u64, 0],
                    u8::from(seen) | (u8::from(coefficient.is_none()) << 1),
                )
            }
            AccumulatorState::Avg { sum, count } => {
                (FixedStateCodec::Avg, [sum.to_bits(), count as u64, 0], 0)
            }
            AccumulatorState::Variance { count, mean, m2 } => (
                FixedStateCodec::Variance,
                [count as u64, mean.to_bits(), m2.to_bits()],
                0,
            ),
            AccumulatorState::BoolAnd(value) => (
                FixedStateCodec::BoolAnd,
                [0; 3],
                u8::from(value.is_some()) | (u8::from(value.unwrap_or(false)) << 1),
            ),
            AccumulatorState::BoolOr(value) => (
                FixedStateCodec::BoolOr,
                [0; 3],
                u8::from(value.is_some()) | (u8::from(value.unwrap_or(false)) << 1),
            ),
            _ => {
                return Err(QueryError::Execution(
                    "selected aggregate cannot enter fixed cell".into(),
                ))
            }
        };
        Ok(Self {
            words,
            codec,
            flags,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn direct_updates_match_checked_general_state_after_every_input() {
        use super::super::state_rows::update_fixed_state;
        use crate::planner::DecimalValue;
        let inputs = [
            ScalarValue::Null,
            ScalarValue::Int8(-7),
            ScalarValue::Int16(19),
            ScalarValue::Int32(-103),
            ScalarValue::Int64(i64::MAX),
            ScalarValue::Int64(1),
            ScalarValue::UInt8(255),
            ScalarValue::UInt16(65535),
            ScalarValue::UInt32(u32::MAX),
            ScalarValue::UInt64(u64::MAX),
            ScalarValue::Float64((-0.0).into()),
            ScalarValue::Float64(1.25.into()),
            ScalarValue::Float64(f64::INFINITY.into()),
            ScalarValue::Float64(f64::from_bits(0x7ff8_0000_0000_0042).into()),
            ScalarValue::Decimal128(DecimalValue::new(12345, 2)),
            ScalarValue::Decimal128(DecimalValue::new(i128::MAX, 0)),
            ScalarValue::Decimal128(DecimalValue::new(1, 0)),
            ScalarValue::Boolean(true),
            ScalarValue::Boolean(false),
            ScalarValue::Null,
        ];
        let mut states: Vec<_> = [
            FixedStateCodec::Count,
            FixedStateCodec::Sum,
            FixedStateCodec::SumInt,
            FixedStateCodec::SumDecimal(0),
            FixedStateCodec::SumDecimal(2),
            FixedStateCodec::SumDecimal(-3),
            FixedStateCodec::Avg,
            FixedStateCodec::Variance,
            FixedStateCodec::BoolAnd,
            FixedStateCodec::BoolOr,
        ]
        .into_iter()
        .map(|codec| FixedCell::empty(codec).state())
        .collect();
        states.extend([
            AccumulatorState::Count(i64::MAX),
            AccumulatorState::SumInt(i64::MIN, true),
            AccumulatorState::Avg {
                sum: -0.0,
                count: i64::MAX,
            },
            AccumulatorState::Variance {
                count: i64::MAX,
                mean: 2.0,
                m2: 3.0,
            },
            AccumulatorState::SumDecimal {
                coefficient: Some(i128::MAX),
                scale: 0,
                seen: true,
            },
        ]);
        for initial in states {
            // Rotate inputs so overflow, rescaling and unsigned values also reach
            // pristine states instead of always following a sticky decimal error.
            for start in 0..inputs.len() {
                let mut expected = initial.clone();
                let mut actual = FixedCell::try_from(initial.clone()).unwrap();
                for value in inputs[start..].iter().chain(&inputs[..start]) {
                    let before = actual.codec.encode(&actual.state()).unwrap();
                    let reference = update_fixed_state(&mut expected, value);
                    let result = actual.update(value);
                    assert_eq!(
                        result.as_ref().err().map(ToString::to_string),
                        reference.as_ref().err().map(ToString::to_string)
                    );
                    assert_eq!(
                        actual.codec.encode(&actual.state()).unwrap(),
                        actual.codec.encode(&expected).unwrap(),
                        "input {value:?}"
                    );
                    if result.is_err() {
                        assert_eq!(
                            actual.codec.encode(&actual.state()).unwrap(),
                            before,
                            "a refused cell must remain unchanged"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn compact_cells_preserve_fixed_frames_and_float_bits() {
        assert_eq!(std::mem::size_of::<FixedCell>(), 32);
        fn copyable<T: Copy>() {}
        copyable::<FixedCell>();
        let nan = f64::from_bits(0x7ff8_0000_0000_0042);
        let states = [
            AccumulatorState::Count(i64::MAX),
            AccumulatorState::Sum(-0.0, true),
            AccumulatorState::Sum(nan, true),
            AccumulatorState::Sum(0.0, false),
            AccumulatorState::SumInt(i64::MIN, true),
            AccumulatorState::SumDecimal {
                coefficient: Some(i128::MIN),
                scale: -3,
                seen: true,
            },
            AccumulatorState::SumDecimal {
                coefficient: None,
                scale: 38,
                seen: true,
            },
            AccumulatorState::Avg {
                sum: -0.0,
                count: 7,
            },
            AccumulatorState::Variance {
                count: 9,
                mean: nan,
                m2: -0.0,
            },
            AccumulatorState::BoolAnd(None),
            AccumulatorState::BoolAnd(Some(false)),
            AccumulatorState::BoolOr(Some(true)),
        ];
        for state in states {
            let cell = FixedCell::try_from(state.clone()).unwrap();
            // Existing independently defined spill representation must remain bit-exact.
            assert_eq!(
                cell.codec.encode(&state).unwrap(),
                cell.codec.encode(&cell.state()).unwrap()
            );
        }
        assert!(FixedCell::try_from(AccumulatorState::Min(None)).is_err());
    }
}
