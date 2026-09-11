//! Bounded, batch-local REGEXP_REPLACE evaluation. Three arguments retain
//! global/$capture behavior; four select explicit first/global and \\capture mode.
use crate::error::{QueryError, Result};
use arrow::array::{Array, ArrayRef, StringArray, StringBuilder};
use arrow::datatypes::DataType;
use regex::{Regex, RegexBuilder};
use std::sync::Arc;

fn invalid(message: impl Into<String>) -> QueryError {
    QueryError::InvalidArgument(format!("REGEXP_REPLACE: {}", message.into()))
}

fn compile(pattern: &str, options: &str) -> Result<(Regex, bool)> {
    let (mut insensitive, mut dot_newline, mut literal, mut global) = (false, false, false, false);
    for option in options.chars() {
        match option {
            'c' => insensitive = false,
            'i' => insensitive = true,
            's' => dot_newline = true,
            'l' => literal = true,
            'g' => global = true,
            other => return Err(invalid(format!("unsupported option {other:?}"))),
        }
    }
    let pattern = if literal {
        regex::escape(pattern)
    } else {
        pattern.to_owned()
    };
    let regex = RegexBuilder::new(&pattern)
        .case_insensitive(insensitive)
        .dot_matches_new_line(dot_newline)
        .build()
        .map_err(|error| invalid(format!("invalid pattern: {error}")))?;
    Ok((regex, global))
}

fn replacement_template(input: &str, regex: &Regex, duckdb: bool) -> Result<String> {
    let mut result = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            let next = chars
                .next()
                .ok_or_else(|| invalid("trailing replacement escape"))?;
            if duckdb {
                if let Some(index) = next.to_digit(10) {
                    if index as usize >= regex.captures_len() {
                        return Err(invalid("replacement capture does not exist"));
                    }
                    result.push_str(&format!("${{{index}}}"));
                } else if next == '\\' {
                    result.push('\\');
                } else {
                    return Err(invalid(format!("invalid replacement escape \\{next}")));
                }
            } else {
                if next == '$' {
                    result.push('$');
                }
                result.push(next);
            }
        } else if ch == '$' && !duckdb {
            if chars.peek() == Some(&'$') {
                chars.next();
                result.push_str("$$");
                continue;
            }
            let mut capture = String::new();
            if chars.peek() == Some(&'{') {
                chars.next();
                loop {
                    match chars.next() {
                        Some('}') => break,
                        Some(ch) => capture.push(ch),
                        None => return Err(invalid("unterminated replacement capture")),
                    }
                }
            } else {
                while chars.peek().is_some_and(|ch| ch.is_ascii_digit()) {
                    capture.push(chars.next().unwrap());
                }
            }
            let valid = if let Ok(index) = capture.parse::<usize>() {
                index < regex.captures_len()
            } else {
                !capture.is_empty() && regex.capture_names().flatten().any(|name| name == capture)
            };
            if !valid {
                return Err(invalid("replacement capture does not exist"));
            }
            result.push_str("${");
            result.push_str(&capture);
            result.push('}');
        } else {
            if ch == '$' {
                result.push('$');
            }
            result.push(ch);
        }
    }
    Ok(result)
}

pub(super) fn evaluate(args: &[ArrayRef]) -> Result<ArrayRef> {
    if !matches!(args.len(), 3 | 4) {
        return Err(invalid("requires 3 or 4 arguments"));
    }
    let arrays = args
        .iter()
        .map(|array| {
            fn string_type(dt: &DataType) -> bool {
                match dt {
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Null => {
                        true
                    }
                    DataType::Dictionary(_, value) => string_type(value),
                    _ => false,
                }
            }
            if !string_type(array.data_type()) {
                return Err(invalid("arguments must be strings"));
            }
            crate::planner::numeric::cast_strict(array, &DataType::Utf8)
        })
        .collect::<Result<Vec<_>>>()?;
    let arrays = arrays
        .iter()
        .map(|a| a.as_any().downcast_ref::<StringArray>().unwrap())
        .collect::<Vec<_>>();
    let rows = arrays[0].len();
    if arrays.iter().any(|array| array.len() != rows) {
        return Err(invalid("argument length mismatch"));
    }
    let duckdb = args.len() == 4;
    // Exactly one pattern/options and one replacement are retained per batch.
    let mut cached: Option<(String, String, Regex, bool)> = None;
    let mut replacement: Option<(String, String)> = None;
    let mut output = StringBuilder::with_capacity(rows, arrays[0].value_data().len());
    for row in 0..rows {
        if arrays.iter().any(|array| array.is_null(row)) {
            output.append_null();
            continue;
        }
        let pattern = arrays[1].value(row);
        let options = if duckdb { arrays[3].value(row) } else { "g" };
        if cached
            .as_ref()
            .is_none_or(|(p, o, _, _)| p != pattern || o != options)
        {
            let (regex, global) = compile(pattern, options)?;
            cached = Some((pattern.to_owned(), options.to_owned(), regex, global));
            replacement = None;
        }
        let (_, _, regex, global) = cached.as_ref().unwrap();
        let original = arrays[2].value(row);
        if replacement.as_ref().is_none_or(|(old, _)| old != original) {
            replacement = Some((
                original.to_owned(),
                replacement_template(original, regex, duckdb)?,
            ));
        }
        let template = replacement.as_ref().unwrap().1.as_str();
        let value = if *global {
            regex.replace_all(arrays[0].value(row), template)
        } else {
            regex.replace(arrays[0].value(row), template)
        };
        output.append_value(value.as_ref());
    }
    Ok(Arc::new(output.finish()))
}
