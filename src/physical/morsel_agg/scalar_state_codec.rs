//! Borrowed selected-value serialization. Unlike group-key encoding, preserve
//! floating bits: a selected -0 or NaN payload must survive a spill unchanged.
use crate::{planner::ScalarValue, QueryError, Result};
use std::io::Write;

pub(super) fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b)
        .filter(|n| *n <= isize::MAX as usize)
        .ok_or_else(|| QueryError::Execution("selected state encoded size overflow".into()))
}

pub(super) fn size(value: &ScalarValue, depth: usize) -> Result<usize> {
    if depth > 64 {
        return Err(QueryError::Execution(
            "selected state nesting exceeds codec limit".into(),
        ));
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
        ScalarValue::Utf8(s) => add(8, s.len())?,
        ScalarValue::List(values, _) => values
            .iter()
            .try_fold(8, |n, v| add(n, size(v, depth + 1)?))?,
    };
    add(1, payload)
}

/// Caller validates size/depth and bound type before starting the enclosing row.
/// This writes borrowed payloads and stack words without allocating a buffer.
pub(super) fn write(value: &ScalarValue, writer: &mut impl Write) -> Result<()> {
    writer.write_all(&[u8::from(!matches!(value, ScalarValue::Null))])?;
    match value {
        ScalarValue::Null => (),
        ScalarValue::Boolean(v) => writer.write_all(&[u8::from(*v)])?,
        ScalarValue::Int8(v) => writer.write_all(&v.to_le_bytes())?,
        ScalarValue::UInt8(v) => writer.write_all(&v.to_le_bytes())?,
        ScalarValue::Int16(v) => writer.write_all(&v.to_le_bytes())?,
        ScalarValue::UInt16(v) => writer.write_all(&v.to_le_bytes())?,
        ScalarValue::Int32(v) | ScalarValue::Date32(v) => writer.write_all(&v.to_le_bytes())?,
        ScalarValue::UInt32(v) => writer.write_all(&v.to_le_bytes())?,
        ScalarValue::Int64(v) | ScalarValue::Date64(v) | ScalarValue::Interval(v) => {
            writer.write_all(&v.to_le_bytes())?
        }
        ScalarValue::UInt64(v) => writer.write_all(&v.to_le_bytes())?,
        ScalarValue::Float32(v) => writer.write_all(&v.into_inner().to_bits().to_le_bytes())?,
        ScalarValue::Float64(v) => writer.write_all(&v.into_inner().to_bits().to_le_bytes())?,
        ScalarValue::Decimal128(v) => writer.write_all(&v.mantissa().to_le_bytes())?,
        ScalarValue::Timestamp(v) => writer.write_all(&v.ticks.to_le_bytes())?,
        ScalarValue::Utf8(v) => {
            writer.write_all(&(v.len() as u64).to_le_bytes())?;
            writer.write_all(v.as_bytes())?;
        }
        ScalarValue::List(values, _) => {
            writer.write_all(&(values.len() as u64).to_le_bytes())?;
            for value in values {
                write(value, writer)?;
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{DecimalValue, TimestampValue};
    use arrow::datatypes::{DataType, TimeUnit};

    #[test]
    fn selected_payload_wire_has_exact_width_and_retains_every_scalar_domain() {
        let nan = 0xffc0_0042u32;
        let cases = [
            (ScalarValue::Null, vec![0]),
            (ScalarValue::Boolean(false), vec![1, 0]),
            (ScalarValue::Int8(-128), vec![1, 128]),
            (ScalarValue::UInt8(255), vec![1, 255]),
            (ScalarValue::Int16(-32768), vec![1, 0, 128]),
            (ScalarValue::UInt16(65535), vec![1, 255, 255]),
            (ScalarValue::Int32(-1), vec![1, 255, 255, 255, 255]),
            (ScalarValue::UInt32(u32::MAX), vec![1, 255, 255, 255, 255]),
            (
                ScalarValue::Float32(f32::from_bits(nan).into()),
                vec![1, 0x42, 0, 0xc0, 0xff],
            ),
            (
                ScalarValue::Int64(i64::MIN),
                [vec![1], i64::MIN.to_le_bytes().to_vec()].concat(),
            ),
            (
                ScalarValue::UInt64(u64::MAX),
                [vec![1], vec![255; 8]].concat(),
            ),
            (
                ScalarValue::Float64((-0.0).into()),
                vec![1, 0, 0, 0, 0, 0, 0, 0, 128],
            ),
            (
                ScalarValue::Decimal128(DecimalValue::new(i128::MAX, -3)),
                [vec![1], i128::MAX.to_le_bytes().to_vec()].concat(),
            ),
            (ScalarValue::Date32(-1), vec![1, 255, 255, 255, 255]),
            (ScalarValue::Date64(-1), [vec![1], vec![255; 8]].concat()),
            (
                ScalarValue::Timestamp(TimestampValue::new(
                    -1,
                    TimeUnit::Nanosecond,
                    Some("UTC".into()),
                )),
                [vec![1], vec![255; 8]].concat(),
            ),
            (ScalarValue::Interval(-1), [vec![1], vec![255; 8]].concat()),
            (
                ScalarValue::Utf8(String::new()),
                vec![1, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
            (
                ScalarValue::List(vec![], Box::new(DataType::Utf8)),
                vec![1, 0, 0, 0, 0, 0, 0, 0, 0],
            ),
        ];
        for (value, expected) in cases {
            let mut output = vec![0; expected.len()];
            assert_eq!(size(&value, 0).unwrap(), expected.len());
            write(&value, &mut std::io::Cursor::new(output.as_mut_slice())).unwrap();
            assert_eq!(output, expected);
            // Interval is not a bound selected-state capability. Every supported
            // domain must also agree with the independently owned reader.
            if !matches!(value, ScalarValue::Interval(_)) {
                let pool = crate::execution::MemoryPool::new_named("wire round trip", 65536);
                let (decoded, consumed) =
                    crate::execution::reserved_scalar::ReservedScalar::try_decode(
                        &pool,
                        &value.data_type(),
                        &output,
                    )
                    .unwrap();
                assert_eq!(consumed, output.len());
                let mut encoded = Vec::new();
                write(decoded.as_scalar(), &mut encoded).unwrap();
                assert_eq!(encoded, expected);
                drop(decoded);
                assert_eq!(pool.used(), 0);
            }
        }
        assert!(add(isize::MAX as usize, 1).is_err());
        assert!(size(&ScalarValue::Null, 65).is_err());
    }
}
