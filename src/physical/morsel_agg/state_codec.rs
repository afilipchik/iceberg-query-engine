//! Fixed-size partial accumulator frames. These preserve internal state, not
//! finalized SQL output. A complete spill row also needs its bound layout/key
//! identity, variable payload ownership, framing and IO lifecycle.
use super::AccumulatorState;
use crate::error::{QueryError, Result};
use crate::execution::{reserved_vec::ReservedVec, MemoryPool};
use crate::planner::AggregateFunction as F;
use arrow::datatypes::DataType;
use std::io::{Read, Write};

pub(super) const FRAME_BYTES: usize = 32;
const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FixedStateCodec {
    Count,
    Sum,
    SumInt,
    SumDecimal(i8),
    Avg,
    BoolAnd,
    BoolOr,
    Variance,
}

fn invalid() -> QueryError {
    QueryError::Execution("invalid or incompatible partial aggregate state frame".into())
}

impl FixedStateCodec {
    pub(super) fn empty(self) -> AccumulatorState {
        match self {
            Self::Count => AccumulatorState::Count(0),
            Self::Sum => AccumulatorState::Sum(0.0, false),
            Self::SumInt => AccumulatorState::SumInt(0, false),
            Self::SumDecimal(scale) => AccumulatorState::SumDecimal {
                coefficient: Some(0),
                scale,
                seen: false,
            },
            Self::Avg => AccumulatorState::Avg { sum: 0.0, count: 0 },
            Self::BoolAnd => AccumulatorState::BoolAnd(None),
            Self::BoolOr => AccumulatorState::BoolOr(None),
            Self::Variance => AccumulatorState::Variance {
                count: 0,
                mean: 0.0,
                m2: 0.0,
            },
        }
    }

    /// Select from the bound operation/type, never from observed values.
    /// None means this component cannot represent the state. The eventual row
    /// layout must resolve that before pulling input, not retry after ingestion.
    pub(super) fn bind(function: &F, input: &DataType, distinct: bool) -> Option<Self> {
        if distinct {
            return None;
        }
        Some(match function {
            F::Count => Self::Count,
            F::Sum => match input {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    Self::SumInt
                }
                // Unsigned SUM uses an exact wider coefficient. Final UInt64
                // conversion checks range; floating accumulation loses low bits.
                DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                    Self::SumDecimal(0)
                }
                DataType::Decimal128(_, scale) => Self::SumDecimal(*scale),
                _ => Self::Sum,
            },
            F::Avg => Self::Avg,
            F::BoolAnd => Self::BoolAnd,
            F::BoolOr => Self::BoolOr,
            F::Stddev | F::StddevPop | F::StddevSamp | F::Variance | F::VarPop | F::VarSamp => {
                Self::Variance
            }
            _ => return None,
        })
    }

    fn tag(self) -> u8 {
        match self {
            Self::Count => 1,
            Self::Sum => 2,
            Self::SumInt => 3,
            Self::SumDecimal(_) => 4,
            Self::Avg => 5,
            Self::BoolAnd => 6,
            Self::BoolOr => 7,
            Self::Variance => 8,
        }
    }

    pub(super) fn width(self) -> usize {
        match self {
            Self::Count | Self::Sum | Self::SumInt => 12,
            Self::SumDecimal(_) | Self::Avg => 20,
            Self::BoolAnd | Self::BoolOr => 4,
            Self::Variance => 28,
        }
    }

    /// Stack-only encoding: callers can write directly into already-admitted
    /// codec/IO scratch. Unused bytes are zero and checked on read-back.
    pub(super) fn encode(self, state: &AccumulatorState) -> Result<[u8; FRAME_BYTES]> {
        let mut bytes = [0; FRAME_BYTES];
        bytes[0] = VERSION;
        bytes[1] = self.tag();
        match (self, state) {
            (Self::Count, AccumulatorState::Count(count)) if *count >= 0 => {
                bytes[4..12].copy_from_slice(&count.to_le_bytes());
            }
            (Self::Sum, AccumulatorState::Sum(sum, seen)) => {
                bytes[2] = u8::from(*seen);
                bytes[4..12].copy_from_slice(&sum.to_bits().to_le_bytes());
            }
            (Self::SumInt, AccumulatorState::SumInt(sum, seen)) => {
                bytes[2] = u8::from(*seen);
                bytes[4..12].copy_from_slice(&sum.to_le_bytes());
            }
            (
                Self::SumDecimal(expected),
                AccumulatorState::SumDecimal {
                    coefficient,
                    scale,
                    seen,
                },
            ) if expected == *scale => {
                bytes[2] = u8::from(*seen) | (u8::from(coefficient.is_some()) << 1);
                bytes[3] = *scale as u8;
                if let Some(coefficient) = coefficient {
                    bytes[4..20].copy_from_slice(&coefficient.to_le_bytes());
                }
            }
            (Self::Avg, AccumulatorState::Avg { sum, count }) if *count >= 0 => {
                bytes[4..12].copy_from_slice(&sum.to_bits().to_le_bytes());
                bytes[12..20].copy_from_slice(&count.to_le_bytes());
            }
            (Self::BoolAnd, AccumulatorState::BoolAnd(value))
            | (Self::BoolOr, AccumulatorState::BoolOr(value)) => {
                bytes[2] = match value {
                    None => 0,
                    Some(false) => 1,
                    Some(true) => 2,
                };
            }
            (Self::Variance, AccumulatorState::Variance { count, mean, m2 }) if *count >= 0 => {
                bytes[4..12].copy_from_slice(&count.to_le_bytes());
                bytes[12..20].copy_from_slice(&mean.to_bits().to_le_bytes());
                bytes[20..28].copy_from_slice(&m2.to_bits().to_le_bytes());
            }
            _ => return Err(invalid()),
        }
        Ok(bytes)
    }

    /// No heap allocation or final precision check. Decimal coefficients retain
    /// every i128 bit, signed scale, seen flag and sticky overflow marker.
    pub(super) fn decode(self, bytes: &[u8]) -> Result<AccumulatorState> {
        if bytes.len() != FRAME_BYTES || bytes[0] != VERSION || bytes[1] != self.tag() {
            return Err(invalid());
        }
        let word = |start: usize| u64::from_le_bytes(bytes[start..start + 8].try_into().unwrap());
        let state = match self {
            Self::Count => AccumulatorState::Count(word(4) as i64),
            Self::Sum => AccumulatorState::Sum(f64::from_bits(word(4)), bytes[2] != 0),
            Self::SumInt => AccumulatorState::SumInt(word(4) as i64, bytes[2] != 0),
            Self::SumDecimal(scale) => AccumulatorState::SumDecimal {
                coefficient: (bytes[2] & 2 != 0)
                    .then(|| i128::from_le_bytes(bytes[4..20].try_into().unwrap())),
                scale,
                seen: bytes[2] & 1 != 0,
            },
            Self::Avg => AccumulatorState::Avg {
                sum: f64::from_bits(word(4)),
                count: word(12) as i64,
            },
            Self::BoolAnd | Self::BoolOr => {
                let value = match bytes[2] {
                    0 => None,
                    1 => Some(false),
                    2 => Some(true),
                    _ => return Err(invalid()),
                };
                if self == Self::BoolAnd {
                    AccumulatorState::BoolAnd(value)
                } else {
                    AccumulatorState::BoolOr(value)
                }
            }
            Self::Variance => AccumulatorState::Variance {
                count: word(4) as i64,
                mean: f64::from_bits(word(12)),
                m2: f64::from_bits(word(20)),
            },
        };
        // Re-encoding enforces flags, padding, scale and count validity without
        // duplicating the wire invariants or normalizing floating-point bits.
        if self.encode(&state)?.as_slice() != bytes {
            return Err(invalid());
        }
        Ok(state)
    }
}

/// Query-owned working space for one fixed-state fragment. Allocate it before
/// filling aggregate storage. There is no growth during encoding/read-back;
/// returned state borrows keep its storage owner alive. Variable-size selected
/// values and group keys require additional layout/storage components.
pub(super) struct FixedStateWorkspace {
    codecs: ReservedVec<FixedStateCodec>,
    frames: ReservedVec<u8>,
    states: ReservedVec<AccumulatorState>,
}

impl FixedStateWorkspace {
    pub(super) fn new(pool: &MemoryPool, codecs: &[FixedStateCodec]) -> Result<Self> {
        if codecs.is_empty() {
            return Err(invalid());
        }
        let count = codecs.len();
        let mut owned_codecs = ReservedVec::with_capacity(pool, count)?;
        owned_codecs.extend_reserved(count, codecs.iter().copied())?;
        let bytes = codecs.iter().try_fold(0usize, |total, codec| {
            total.checked_add(codec.width()).ok_or_else(invalid)
        })?;
        let mut frames = ReservedVec::with_capacity(pool, bytes)?;
        frames.extend_reserved(bytes, std::iter::repeat_n(0, bytes))?;
        let mut states = ReservedVec::with_capacity(pool, count)?;
        states.extend_reserved(count, codecs.iter().map(|codec| codec.empty()))?;
        Ok(Self {
            codecs: owned_codecs,
            frames,
            states,
        })
    }

    fn encode(&mut self, states: &[AccumulatorState]) -> Result<&[u8]> {
        if states.len() != self.codecs.as_slice().len() {
            return Err(invalid());
        }
        let mut offset = 0;
        for (codec, state) in self.codecs.as_slice().iter().zip(states) {
            let frame = codec.encode(state)?;
            let end = offset + codec.width();
            self.frames.as_mut_slice()[offset..end].copy_from_slice(&frame[..codec.width()]);
            offset = end;
        }
        Ok(self.frames.as_slice())
    }

    /// Validate/encode the complete fragment before publishing any bytes. A
    /// writer failure can leave a partial file; its lifecycle owner must remove
    /// or reject that file. The original accumulators are only borrowed.
    pub(super) fn write_to(
        &mut self,
        writer: &mut impl Write,
        states: &[AccumulatorState],
    ) -> Result<()> {
        writer.write_all(self.encode(states)?)?;
        Ok(())
    }

    /// Read exactly one fixed fragment into existing admitted scratch. No state
    /// reference escapes if IO or a slot validation fails partway through.
    pub(super) fn read_from(&mut self, reader: &mut impl Read) -> Result<&[AccumulatorState]> {
        reader.read_exact(self.frames.as_mut_slice())?;
        let mut offset = 0;
        for (state, codec) in self
            .states
            .as_mut_slice()
            .iter_mut()
            .zip(self.codecs.as_slice())
        {
            let mut frame = [0; FRAME_BYTES];
            let end = offset + codec.width();
            frame[..codec.width()].copy_from_slice(&self.frames.as_slice()[offset..end]);
            *state = codec.decode(&frame)?;
            offset = end;
        }
        Ok(self.states.as_slice())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{DecimalValue, ScalarValue};

    #[test]
    fn decimal_intermediate_precision_and_overflow_survive_frames() {
        let codec = FixedStateCodec::bind(&F::Sum, &DataType::Decimal128(38, 2), false).unwrap();
        let large: i128 = "80000000000000000000000000000000000000".parse().unwrap();
        let partial = AccumulatorState::SumDecimal {
            coefficient: Some(large * 2),
            scale: 2,
            seen: true,
        };
        assert!(
            partial.finalize(&F::Sum).is_err(),
            "partial exceeds final SQL precision"
        );
        let mut restored = codec.decode(&codec.encode(&partial).unwrap()).unwrap();
        restored.merge(&AccumulatorState::SumDecimal {
            coefficient: Some(-large),
            scale: 2,
            seen: true,
        });
        assert_eq!(
            restored.finalize(&F::Sum).unwrap(),
            ScalarValue::Decimal128(DecimalValue::new(large, 2))
        );
        for coefficient in [Some(i128::MIN), Some(i128::MAX), Some(0), None] {
            for seen in [false, true] {
                let original = AccumulatorState::SumDecimal {
                    coefficient,
                    scale: -3,
                    seen,
                };
                let c = FixedStateCodec::SumDecimal(-3);
                let recovered = c.decode(&c.encode(&original).unwrap()).unwrap();
                assert!(
                    matches!(recovered, AccumulatorState::SumDecimal { coefficient: v, scale: -3, seen: s } if v == coefficient && s == seen)
                );
            }
        }
        let overflow = AccumulatorState::SumDecimal {
            coefficient: None,
            scale: 2,
            seen: true,
        };
        let mut recovered = codec.decode(&codec.encode(&overflow).unwrap()).unwrap();
        recovered.merge(&AccumulatorState::SumDecimal {
            coefficient: Some(1),
            scale: 2,
            seen: true,
        });
        assert!(
            recovered.finalize(&F::Sum).is_err(),
            "arithmetic overflow must remain sticky"
        );
    }

    #[test]
    fn average_frames_merge_weights_before_division() {
        let codec = FixedStateCodec::Avg;
        let mut result = codec
            .decode(
                &codec
                    .encode(&AccumulatorState::Avg {
                        sum: 10.0,
                        count: 1,
                    })
                    .unwrap(),
            )
            .unwrap();
        let other = codec
            .decode(
                &codec
                    .encode(&AccumulatorState::Avg {
                        sum: 90.0,
                        count: 3,
                    })
                    .unwrap(),
            )
            .unwrap();
        result.merge(&other);
        assert_eq!(
            result.finalize(&F::Avg).unwrap(),
            ScalarValue::Float64(25.0.into())
        );
    }

    #[test]
    fn frames_preserve_float_bits_and_null_state() {
        for bits in [
            0,
            1 << 63,
            f64::INFINITY.to_bits(),
            f64::NEG_INFINITY.to_bits(),
            0x7ff8_0000_0000_0042,
        ] {
            for seen in [false, true] {
                let value = AccumulatorState::Sum(f64::from_bits(bits), seen);
                let encoded = FixedStateCodec::Sum.encode(&value).unwrap();
                let AccumulatorState::Sum(sum, actual_seen) =
                    FixedStateCodec::Sum.decode(&encoded).unwrap()
                else {
                    panic!()
                };
                assert_eq!(sum.to_bits(), bits);
                assert_eq!(actual_seen, seen);
            }
        }
        for (codec, value) in [
            (FixedStateCodec::Count, AccumulatorState::Count(i64::MAX)),
            (
                FixedStateCodec::SumInt,
                AccumulatorState::SumInt(i64::MIN, true),
            ),
            (FixedStateCodec::SumInt, AccumulatorState::SumInt(0, false)),
            (FixedStateCodec::BoolAnd, AccumulatorState::BoolAnd(None)),
            (
                FixedStateCodec::BoolAnd,
                AccumulatorState::BoolAnd(Some(false)),
            ),
            (
                FixedStateCodec::BoolOr,
                AccumulatorState::BoolOr(Some(true)),
            ),
            (
                FixedStateCodec::Variance,
                AccumulatorState::Variance {
                    count: 7,
                    mean: -0.0,
                    m2: 12.5,
                },
            ),
        ] {
            let bytes = codec.encode(&value).unwrap();
            let recovered = codec.decode(&bytes).unwrap();
            match (&value, &recovered) {
                (AccumulatorState::Count(a), AccumulatorState::Count(b)) => assert_eq!(a, b),
                (AccumulatorState::SumInt(a, sa), AccumulatorState::SumInt(b, sb)) => {
                    assert_eq!((a, sa), (b, sb))
                }
                (AccumulatorState::BoolAnd(a), AccumulatorState::BoolAnd(b))
                | (AccumulatorState::BoolOr(a), AccumulatorState::BoolOr(b)) => assert_eq!(a, b),
                (
                    AccumulatorState::Variance {
                        count: a,
                        mean: ma,
                        m2: va,
                    },
                    AccumulatorState::Variance {
                        count: b,
                        mean: mb,
                        m2: vb,
                    },
                ) => {
                    assert_eq!(a, b);
                    assert_eq!(ma.to_bits(), mb.to_bits());
                    assert_eq!(va.to_bits(), vb.to_bits());
                }
                _ => panic!("state variant changed"),
            }
            assert_eq!(codec.encode(&recovered).unwrap(), bytes);
        }
    }

    #[test]
    fn malformed_frames_and_incompatible_layouts_refuse() {
        let bytes = FixedStateCodec::Count
            .encode(&AccumulatorState::Count(3))
            .unwrap();
        for len in 0..FRAME_BYTES {
            assert!(FixedStateCodec::Count.decode(&bytes[..len]).is_err());
        }
        let mut extended = bytes.to_vec();
        extended.push(0);
        assert!(FixedStateCodec::Count.decode(&extended).is_err());
        for offset in [0, 1, 2, 3, 12, 31] {
            let mut invalid = bytes;
            invalid[offset] ^= 0x80;
            assert!(
                FixedStateCodec::Count.decode(&invalid).is_err(),
                "byte {offset}"
            );
        }
        assert!(FixedStateCodec::SumInt.decode(&bytes).is_err());
        assert!(FixedStateCodec::Count
            .encode(&AccumulatorState::Count(-1))
            .is_err());
        assert!(FixedStateCodec::Avg
            .encode(&AccumulatorState::Avg {
                sum: 0.0,
                count: -1
            })
            .is_err());
        let decimal = FixedStateCodec::SumDecimal(-2)
            .encode(&AccumulatorState::SumDecimal {
                coefficient: Some(1),
                scale: -2,
                seen: true,
            })
            .unwrap();
        assert!(FixedStateCodec::SumDecimal(2).decode(&decimal).is_err());
        assert!(FixedStateCodec::bind(&F::CountDistinct, &DataType::Int64, false).is_none());
        assert!(FixedStateCodec::bind(&F::Count, &DataType::Int64, true).is_none());
        assert!(FixedStateCodec::bind(&F::Min, &DataType::Utf8, false).is_none());
    }

    #[test]
    fn admitted_workspace_operates_with_no_remaining_pool_capacity() {
        let pool = MemoryPool::new_named("codec query", 16 << 10);
        let codecs = [
            FixedStateCodec::Count,
            FixedStateCodec::SumDecimal(-2),
            FixedStateCodec::Avg,
            FixedStateCodec::Sum,
        ];
        let mut workspace = FixedStateWorkspace::new(&pool, &codecs).unwrap();
        let owned = pool.used();
        assert!(owned >= 4 * (FRAME_BYTES + std::mem::size_of::<AccumulatorState>()));
        let blocker = pool.allocate(pool.available()).unwrap();
        let peak = pool.reserved_peak();
        let states = [
            AccumulatorState::Count(7),
            AccumulatorState::SumDecimal {
                coefficient: Some(i128::MAX),
                scale: -2,
                seen: true,
            },
            AccumulatorState::Avg {
                sum: 90.0,
                count: 3,
            },
            AccumulatorState::Sum(-0.0, true),
        ];
        let mut bytes = [0; 4 * FRAME_BYTES];
        let mut writer = std::io::Cursor::new(bytes.as_mut_slice());
        workspace.write_to(&mut writer, &states).unwrap();
        assert_eq!(
            writer.position(),
            64,
            "12 + 20 + 20 + 12 bytes; no unused slot padding on disk"
        );
        let decoded = workspace
            .read_from(&mut std::io::Cursor::new(bytes.as_slice()))
            .unwrap();
        for ((codec, original), actual) in codecs.iter().zip(&states).zip(decoded) {
            assert_eq!(
                codec.encode(original).unwrap(),
                codec.encode(actual).unwrap()
            );
        }
        assert_eq!(pool.reserved_peak(), peak);
        assert_eq!(pool.used(), pool.max());
        drop(blocker);
        assert_eq!(pool.used(), owned);
        drop(workspace);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn workspace_denial_and_failed_io_preserve_ownership_and_errors() {
        let pool = MemoryPool::new_named("codec query", 8192);
        let held = pool.allocate(pool.max() - 600).unwrap();
        let error = match FixedStateWorkspace::new(&pool, &[FixedStateCodec::Count; 4]) {
            Err(error) => error,
            Ok(_) => panic!("partial workspace must refuse"),
        };
        assert!(error.is_memory_limit());
        assert_eq!(pool.used(), held.size());
        drop(held);
        let mut workspace = FixedStateWorkspace::new(&pool, &[FixedStateCodec::Count]).unwrap();
        let error = workspace
            .read_from(&mut std::io::Cursor::new([0u8; 11]))
            .unwrap_err();
        assert!(
            matches!(error, QueryError::Io(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof)
        );
        struct RefuseWrite;
        impl Write for RefuseWrite {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::from_raw_os_error(28))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let error = workspace
            .write_to(&mut RefuseWrite, &[AccumulatorState::Count(1)])
            .unwrap_err();
        assert!(matches!(error, QueryError::Io(ref e) if e.raw_os_error() == Some(28)));
        let mut bytes = [0; FRAME_BYTES];
        workspace
            .write_to(
                &mut std::io::Cursor::new(bytes.as_mut_slice()),
                &[AccumulatorState::Count(9)],
            )
            .unwrap();
        assert!(matches!(
            workspace
                .read_from(&mut std::io::Cursor::new(bytes))
                .unwrap(),
            [AccumulatorState::Count(9)]
        ));
        drop(workspace);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn whole_fragment_validation_precedes_publication_and_recovers_scratch() {
        let pool = MemoryPool::new(8192);
        let mut workspace =
            FixedStateWorkspace::new(&pool, &[FixedStateCodec::Count, FixedStateCodec::Sum])
                .unwrap();
        let owned = pool.used();
        let mut bytes = [0; 24];
        let mut writer = std::io::Cursor::new(bytes.as_mut_slice());
        let error = workspace
            .write_to(
                &mut writer,
                &[
                    AccumulatorState::Count(7),
                    AccumulatorState::SumInt(9, true),
                ],
            )
            .unwrap_err();
        assert!(!error.is_memory_limit());
        assert_eq!(
            writer.position(),
            0,
            "a valid slot prefix must not be published before the whole fragment validates"
        );
        workspace
            .write_to(
                &mut writer,
                &[AccumulatorState::Count(7), AccumulatorState::Sum(9.0, true)],
            )
            .unwrap();
        assert_eq!(writer.position(), 24);
        let mut corrupt = bytes;
        corrupt[14] = 0x80; // Invalid flags in the second 12-byte state.
        assert!(workspace
            .read_from(&mut std::io::Cursor::new(corrupt))
            .is_err());
        let states = workspace
            .read_from(&mut std::io::Cursor::new(bytes))
            .unwrap();
        assert!(
            matches!(states, [AccumulatorState::Count(7), AccumulatorState::Sum(value, true)] if *value == 9.0)
        );
        assert_eq!(pool.used(), owned);
        drop(workspace);
        assert_eq!(pool.used(), 0);
    }
}
