//! SQL substring bounds are character intervals, clipped after signed arithmetic.
use crate::error::{QueryError, Result};
use crate::planner::{Expr, ScalarValue};
use arrow::array::{Array, ArrayRef, Int64Array, StringArray};
use arrow::datatypes::DataType;
use std::sync::Arc;

fn value_type(mut ty: &DataType) -> &DataType {
    while let DataType::Dictionary(_, value) = ty {
        ty = value;
    }
    ty
}

fn integer(array: &ArrayRef) -> Result<ArrayRef> {
    if !matches!(
        value_type(array.data_type()),
        DataType::Null
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
    ) {
        return Err(QueryError::Type(
            "SUBSTRING position and length require integer arguments".into(),
        ));
    }
    crate::planner::numeric::cast_strict(array, &DataType::Int64)
}

fn constant(expr: &Expr, array: &Int64Array) -> Option<i64> {
    if array.is_empty() || array.null_count() != 0 {
        return None;
    }
    if array.len() == 1
        || matches!(
            expr,
            Expr::Literal(ScalarValue::Int64(_) | ScalarValue::Int32(_))
        )
    {
        Some(array.value(0))
    } else {
        None
    }
}

pub(super) fn evaluate(exprs: &[Expr], args: &[ArrayRef]) -> Result<ArrayRef> {
    if !(2..=3).contains(&args.len()) || exprs.len() != args.len() {
        return Err(QueryError::InvalidArgument(
            "SUBSTRING requires 2 or 3 arguments".into(),
        ));
    }
    if !matches!(
        value_type(args[0].data_type()),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null
    ) {
        return Err(QueryError::Type(
            "SUBSTRING requires string argument".into(),
        ));
    }
    let strings = crate::planner::numeric::cast_strict(&args[0], &DataType::Utf8)?;
    let starts = integer(&args[1])?;
    let lengths = args.get(2).map(integer).transpose()?;
    let strings = strings
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| QueryError::Type("SUBSTRING string normalization failed".into()))?;
    let starts = starts
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| QueryError::Type("SUBSTRING position normalization failed".into()))?;
    let lengths = lengths
        .as_ref()
        .map(|a| {
            a.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::Type("SUBSTRING length normalization failed".into()))
        })
        .transpose()?;
    let rows = if args.iter().any(|a| a.is_empty()) {
        0
    } else {
        args.iter().map(|a| a.len()).max().unwrap_or(0)
    };
    if args.iter().any(|a| a.len() != rows && a.len() != 1) {
        return Err(QueryError::Execution(
            "SUBSTRING argument lengths do not match".into(),
        ));
    }
    if rows == 0 {
        return Ok(Arc::new(StringArray::from(Vec::<Option<&str>>::new())));
    }

    // Preserve the vectorized common path. NULL constants never enter it.
    let start = constant(&exprs[1], starts);
    let length = match lengths {
        Some(a) => constant(&exprs[2], a).map(Some),
        None => Some(None),
    };
    if let (Some(start), Some(length)) = (start, length) {
        if strings.len() == rows && start >= 1 && length.is_none_or(|n| n >= 0) {
            return Ok(Arc::new(
                arrow::compute::kernels::substring::substring_by_char(
                    strings,
                    start - 1,
                    length.map(|n| n as u64),
                )?,
            ));
        }
    }
    let index = |len: usize, row: usize| if len == 1 { 0 } else { row };
    let output: StringArray = (0..rows)
        .map(|row| {
            let s = index(strings.len(), row);
            let p = index(starts.len(), row);
            let n = lengths.map(|a| index(a.len(), row));
            if strings.is_null(s)
                || starts.is_null(p)
                || lengths.zip(n).is_some_and(|(a, i)| a.is_null(i))
            {
                return None;
            }
            Some(slice(
                strings.value(s),
                starts.value(p),
                lengths.zip(n).map(|(a, i)| a.value(i)),
            ))
        })
        .collect();
    Ok(Arc::new(output))
}

fn slice(input: &str, position: i64, length: Option<i64>) -> &str {
    let count = input.chars().count() as i128;
    let anchor = if position < 0 {
        count + i128::from(position)
    } else {
        i128::from(position) - 1
    };
    let (start, end) = match length {
        None => (anchor, count),
        Some(n) if n >= 0 => (anchor, anchor + i128::from(n)),
        Some(n) => (anchor + i128::from(n), anchor),
    };
    let start = start.clamp(0, count) as usize;
    let end = end.clamp(0, count) as usize;
    let byte = |character| {
        input
            .char_indices()
            .map(|(i, _)| i)
            .nth(character)
            .unwrap_or(input.len())
    };
    &input[byte(start)..byte(end)]
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn signed_bounds_match_independent_unicode_oracle() {
        let oracle: serde_json::Value = serde_json::from_str(include_str!(
            "../../../tests/fixtures/substring_duckdb_1_4_4.json"
        ))
        .unwrap();
        for case in oracle["cases"].as_array().unwrap() {
            let args = case["input"].as_array().unwrap();
            if let (Some(s), Some(p), Some(n)) =
                (args[0].as_str(), args[1].as_i64(), args[2].as_i64())
            {
                assert_eq!(
                    slice(s, p, Some(n)),
                    case["expected"].as_str().unwrap(),
                    "{case}"
                );
            }
        }
    }
    #[test]
    fn extreme_bounds_are_clipped_without_overflow() {
        assert_eq!(slice("aé🙂z", i64::MIN, Some(i64::MAX)), "aé🙂");
        assert_eq!(slice("aé🙂z", i64::MAX, Some(i64::MIN)), "aé🙂z");
        assert_eq!(slice("aé🙂z", 0, None), "aé🙂z");
        assert_eq!(slice("aé🙂z", -2, None), "🙂z");
    }
}
