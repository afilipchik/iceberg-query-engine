//! Date32 extraction writes admitted output; unsupported representations keep
//! their existing ordinary evaluator and are not advertised as admitted input.
use crate::{
    execution::{MemoryPool, MemoryReservation, ReservedBufferBuilder},
    QueryError, Result,
};
use arrow::{
    array::{Array, ArrayRef, Date32Array, Int32Array},
    buffer::{Buffer, ScalarBuffer},
    compute::DatePart,
};
use chrono::{Datelike, NaiveDateTime};
use std::sync::Arc;

pub(crate) fn part(field: &str) -> Result<DatePart> {
    for (name, part) in [
        ("YEAR", DatePart::Year),
        ("QUARTER", DatePart::Quarter),
        ("MONTH", DatePart::Month),
        ("WEEK", DatePart::WeekISO),
        ("DAY", DatePart::Day),
        ("DOY", DatePart::DayOfYear),
        ("DAYOFYEAR", DatePart::DayOfYear),
        ("DOW", DatePart::DayOfWeekSunday0),
        ("DAYOFWEEK", DatePart::DayOfWeekSunday0),
        ("HOUR", DatePart::Hour),
        ("MINUTE", DatePart::Minute),
        ("SECOND", DatePart::Second),
    ] {
        if field.eq_ignore_ascii_case(name) {
            return Ok(part);
        }
    }
    Err(QueryError::NotImplemented(format!(
        "EXTRACT field {field} is unsupported"
    )))
}
fn outside_range() -> QueryError {
    QueryError::Execution("EXTRACT input is outside the supported temporal range".into())
}
struct Values {
    buffer: Buffer,
    _metadata: MemoryReservation,
}
impl AsRef<[u8]> for Values {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}
pub(crate) fn date32(pool: &MemoryPool, input: &Date32Array, part: DatePart) -> Result<ArrayRef> {
    let map: fn(NaiveDateTime) -> i32 = match part {
        DatePart::Year => |d| d.year(),
        DatePart::Quarter => |d| (d.month0() / 3 + 1) as i32,
        DatePart::Month => |d| d.month() as i32,
        DatePart::WeekISO => |d| d.iso_week().week() as i32,
        DatePart::Day => |d| d.day() as i32,
        DatePart::DayOfYear => |d| d.ordinal() as i32,
        DatePart::DayOfWeekSunday0 => |d| d.weekday().num_days_from_sunday() as i32,
        DatePart::Hour | DatePart::Minute | DatePart::Second => |_| 0,
        _ => {
            return Err(QueryError::NotImplemented(
                "unsupported admitted Date32 part".into(),
            ))
        }
    };
    // Match Arrow: time-of-day fields of Date32 are zero even outside chrono's
    // calendar range. Calendar fields reject newly introduced NULL results.
    let zero = matches!(part, DatePart::Hour | DatePart::Minute | DatePart::Second);
    let metadata = pool.allocate(1024)?;
    let mut values = ReservedBufferBuilder::<i32>::with_capacity(pool, input.len())?;
    values.try_extend_reserved(
        input.len(),
        (0..input.len()).map(|row| {
            if zero || input.is_null(row) {
                return Ok(0);
            }
            arrow_array::temporal_conversions::date32_to_datetime(input.value(row))
                .map(map)
                .ok_or_else(outside_range)
        }),
    )?;
    let buffer = Buffer::from(bytes::Bytes::from_owner(Values {
        buffer: values.finish(),
        _metadata: metadata,
    }));
    Ok(Arc::new(Int32Array::new(
        ScalarBuffer::new(buffer, 0, input.len()),
        input.nulls().cloned(),
    )))
}

pub(super) fn extract(
    batch: &arrow::record_batch::RecordBatch,
    args: &[crate::planner::Expr],
    executor: Option<&super::SubqueryExecutor>,
) -> Result<ArrayRef> {
    use crate::planner::{Expr, ScalarValue};
    if args.len() != 2 {
        return Err(QueryError::InvalidArgument(
            "EXTRACT requires 2 arguments".into(),
        ));
    }
    let Expr::Literal(ScalarValue::Utf8(field)) = &args[0] else {
        return Err(QueryError::NotImplemented(
            "EXTRACT requires a constant field name".into(),
        ));
    };
    let part = part(field)?;
    // Field name is metadata, not a column-sized literal; evaluate the value
    // expression exactly once, even if the runtime representation uses fallback.
    let input = super::evaluate_expr_internal(batch, &args[1], executor)?;
    let input = if let arrow::datatypes::DataType::Dictionary(_, value) = input.data_type() {
        arrow::compute::cast(input.as_ref(), value)?
    } else {
        input
    };
    if let Some(pool) = crate::execution::expression_memory::expression_pool() {
        if let Some(array) = input.as_any().downcast_ref::<Date32Array>() {
            return date32(&pool, array, part);
        }
    }
    let result = arrow::compute::date_part(input.as_ref(), part)?;
    if result.null_count() != input.null_count() {
        return Err(outside_range());
    }
    Ok(result)
}
#[cfg(test)]
mod tests;
