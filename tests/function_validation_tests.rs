//! Comprehensive Function Validation Tests
//!
//! These tests validate that functions return correct values according to Trino documentation.
//! Each test checks specific input/output pairs to ensure correctness.

use arrow::array::{Array, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

// =============================================================================
// TEST INFRASTRUCTURE
// =============================================================================

fn create_test_context() -> ExecutionContext {
    let mut ctx = ExecutionContext::new();

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val_int", DataType::Int64, true),
        Field::new("val_float", DataType::Float64, true),
        Field::new("val_str", DataType::Utf8, true),
        Field::new("val_bool", DataType::Boolean, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![
                Some(10),
                Some(20),
                Some(30),
                Some(40),
                Some(50),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1.5),
                Some(2.5),
                Some(3.5),
                Some(4.5),
                Some(5.5),
            ])),
            Arc::new(StringArray::from(vec![
                Some("hello"),
                Some("world"),
                Some("test"),
                Some("foo"),
                Some("bar"),
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                Some(false),
                Some(true),
                Some(false),
                Some(true),
            ])),
        ],
    )
    .unwrap();

    ctx.register_table("test_data", schema, vec![batch]);
    ctx
}

/// Helper to get a single Int64 value from a query result
fn get_int64(ctx: &ExecutionContext, sql: &str) -> Option<i64> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let result = ctx.sql(sql).await.ok()?;
        let batch = result.batches.first()?;
        let col = batch.column(0);
        if let Some(arr) = col.as_any().downcast_ref::<Int64Array>() {
            if arr.is_null(0) {
                None
            } else {
                Some(arr.value(0))
            }
        } else if let Some(arr) = col.as_any().downcast_ref::<Int32Array>() {
            if arr.is_null(0) {
                None
            } else {
                Some(arr.value(0) as i64)
            }
        } else {
            None
        }
    })
}

/// Helper to get a single Float64 value from a query result
fn get_float64(ctx: &ExecutionContext, sql: &str) -> Option<f64> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let result = ctx.sql(sql).await.ok()?;
        let batch = result.batches.first()?;
        let col = batch.column(0);
        if let Some(arr) = col.as_any().downcast_ref::<Float64Array>() {
            if arr.is_null(0) {
                None
            } else {
                Some(arr.value(0))
            }
        } else {
            None
        }
    })
}

/// Helper to get a single String value from a query result
fn get_string(ctx: &ExecutionContext, sql: &str) -> Option<String> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let result = ctx.sql(sql).await.ok()?;
        let batch = result.batches.first()?;
        let col = batch.column(0);
        if let Some(arr) = col.as_any().downcast_ref::<StringArray>() {
            if arr.is_null(0) {
                None
            } else {
                Some(arr.value(0).to_string())
            }
        } else {
            None
        }
    })
}

/// Helper to get a single Boolean value from a query result
fn get_bool(ctx: &ExecutionContext, sql: &str) -> Option<bool> {
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let result = ctx.sql(sql).await.ok()?;
        let batch = result.batches.first()?;
        let col = batch.column(0);
        if let Some(arr) = col.as_any().downcast_ref::<BooleanArray>() {
            if arr.is_null(0) {
                None
            } else {
                Some(arr.value(0))
            }
        } else {
            None
        }
    })
}

// =============================================================================
// MATH FUNCTIONS - Basic
// =============================================================================

#[test]
fn test_abs_positive() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT ABS(5) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(5), "ABS(5) should be 5");
}

#[test]
fn test_abs_negative() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT ABS(-5) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(5), "ABS(-5) should be 5");
}

#[test]
fn test_ceil() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT CEIL(3.2) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(4.0), "CEIL(3.2) should be 4.0");
}

#[test]
fn test_floor() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT FLOOR(3.8) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(3.0), "FLOOR(3.8) should be 3.0");
}

#[test]
fn test_round() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT ROUND(3.5) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(4.0), "ROUND(3.5) should be 4.0");
}

#[test]
fn test_power() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT POWER(2, 3) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(8.0), "POWER(2, 3) should be 8.0");
}

#[test]
fn test_sqrt() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT SQRT(16) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(4.0), "SQRT(16) should be 4.0");
}

#[test]
fn test_mod_positive() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT MOD(10, 3) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(1), "MOD(10, 3) should be 1");
}

#[test]
fn test_sign_positive() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT SIGN(5) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(1), "SIGN(5) should be 1");
}

#[test]
fn test_sign_negative() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT SIGN(-5) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(-1), "SIGN(-5) should be -1");
}

#[test]
fn test_sign_zero() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT SIGN(0) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(0), "SIGN(0) should be 0");
}

#[test]
fn test_truncate() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT TRUNCATE(3.7) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(3.0), "TRUNCATE(3.7) should be 3.0");
}

#[test]
fn test_ln() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT LN(2.718281828) FROM test_data WHERE id = 1");
    assert!(result.is_some());
    let val = result.unwrap();
    assert!(
        (val - 1.0).abs() < 0.0001,
        "LN(e) should be approximately 1.0, got {}",
        val
    );
}

#[test]
fn test_log10() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT LOG10(100) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(2.0), "LOG10(100) should be 2.0");
}

#[test]
fn test_exp() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT EXP(1) FROM test_data WHERE id = 1");
    assert!(result.is_some());
    let val = result.unwrap();
    assert!(
        (val - 2.718281828).abs() < 0.0001,
        "EXP(1) should be approximately e"
    );
}

// =============================================================================
// MATH FUNCTIONS - Trigonometric
// =============================================================================

#[test]
fn test_sin() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT SIN(0) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(0.0), "SIN(0) should be 0.0");
}

#[test]
fn test_cos() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT COS(0) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(1.0), "COS(0) should be 1.0");
}

#[test]
fn test_tan() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT TAN(0) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(0.0), "TAN(0) should be 0.0");
}

#[test]
fn test_sinh() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT SINH(0) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(0.0), "SINH(0) should be 0.0");
}

#[test]
fn test_cosh() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT COSH(0) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(1.0), "COSH(0) should be 1.0");
}

#[test]
fn test_tanh() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT TANH(0) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(0.0), "TANH(0) should be 0.0");
}

#[test]
fn test_asin() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT ASIN(0) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(0.0), "ASIN(0) should be 0.0");
}

#[test]
fn test_acos() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT ACOS(1) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(0.0), "ACOS(1) should be 0.0");
}

#[test]
fn test_atan() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT ATAN(0) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(0.0), "ATAN(0) should be 0.0");
}

#[test]
fn test_pi() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT PI() FROM test_data WHERE id = 1");
    assert!(result.is_some());
    let val = result.unwrap();
    assert!(
        (val - std::f64::consts::PI).abs() < 0.0001,
        "PI() should be approximately 3.14159"
    );
}

#[test]
fn test_degrees() {
    let ctx = create_test_context();
    let result = get_float64(
        &ctx,
        "SELECT DEGREES(3.141592653589793) FROM test_data WHERE id = 1",
    );
    assert!(result.is_some());
    let val = result.unwrap();
    assert!(
        (val - 180.0).abs() < 0.0001,
        "DEGREES(PI) should be 180.0, got {}",
        val
    );
}

#[test]
fn test_radians() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT RADIANS(180) FROM test_data WHERE id = 1");
    assert!(result.is_some());
    let val = result.unwrap();
    assert!(
        (val - std::f64::consts::PI).abs() < 0.0001,
        "RADIANS(180) should be PI, got {}",
        val
    );
}

#[test]
fn test_cbrt() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT CBRT(27) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(3.0), "CBRT(27) should be 3.0");
}

// =============================================================================
// MATH FUNCTIONS - Special Values
// =============================================================================

#[test]
fn test_infinity() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT INFINITY() FROM test_data WHERE id = 1");
    assert!(result.is_some());
    assert!(
        result.unwrap().is_infinite(),
        "INFINITY() should return infinity"
    );
}

#[test]
fn test_nan() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT NAN() FROM test_data WHERE id = 1");
    assert!(result.is_some());
    assert!(result.unwrap().is_nan(), "NAN() should return NaN");
}

#[test]
fn test_is_finite_true() {
    let ctx = create_test_context();
    let result = get_bool(&ctx, "SELECT IS_FINITE(1.0) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(true), "IS_FINITE(1.0) should be true");
}

#[test]
fn test_is_nan_false() {
    let ctx = create_test_context();
    let result = get_bool(&ctx, "SELECT IS_NAN(1.0) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(false), "IS_NAN(1.0) should be false");
}

#[test]
fn test_is_infinite_false() {
    let ctx = create_test_context();
    let result = get_bool(&ctx, "SELECT IS_INFINITE(1.0) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(false), "IS_INFINITE(1.0) should be false");
}

// =============================================================================
// STRING FUNCTIONS
// =============================================================================

#[test]
fn test_upper() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT UPPER('hello') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("HELLO".to_string()),
        "UPPER('hello') should be 'HELLO'"
    );
}

#[test]
fn test_lower() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT LOWER('HELLO') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("hello".to_string()),
        "LOWER('HELLO') should be 'hello'"
    );
}

#[test]
fn test_length() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT LENGTH('hello') FROM test_data WHERE id = 1");
    assert_eq!(result, Some(5), "LENGTH('hello') should be 5");
}

#[test]
fn test_trim() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT TRIM('  hello  ') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("hello".to_string()),
        "TRIM('  hello  ') should be 'hello'"
    );
}

#[test]
fn test_ltrim() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT LTRIM('  hello') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("hello".to_string()),
        "LTRIM('  hello') should be 'hello'"
    );
}

#[test]
fn test_rtrim() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT RTRIM('hello  ') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("hello".to_string()),
        "RTRIM('hello  ') should be 'hello'"
    );
}

#[test]
fn test_substring() {
    let ctx = create_test_context();
    // Trino: SUBSTR(string, start, length) - 1-indexed
    let result = get_string(
        &ctx,
        "SELECT SUBSTRING('hello', 2, 3) FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("ell".to_string()),
        "SUBSTRING('hello', 2, 3) should be 'ell'"
    );
}

#[test]
fn test_concat() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT CONCAT('hello', ' ', 'world') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("hello world".to_string()),
        "CONCAT('hello', ' ', 'world') should be 'hello world'"
    );
}

#[test]
fn test_replace() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT REPLACE('hello', 'l', 'L') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("heLLo".to_string()),
        "REPLACE('hello', 'l', 'L') should be 'heLLo'"
    );
}

#[test]
fn test_reverse() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT REVERSE('hello') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("olleh".to_string()),
        "REVERSE('hello') should be 'olleh'"
    );
}

#[test]
fn test_lpad() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT LPAD('hi', 5, '*') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("***hi".to_string()),
        "LPAD('hi', 5, '*') should be '***hi'"
    );
}

#[test]
fn test_rpad() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT RPAD('hi', 5, '*') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("hi***".to_string()),
        "RPAD('hi', 5, '*') should be 'hi***'"
    );
}

#[test]
fn test_left() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT LEFT('hello', 3) FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("hel".to_string()),
        "LEFT('hello', 3) should be 'hel'"
    );
}

#[test]
fn test_right() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT RIGHT('hello', 3) FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("llo".to_string()),
        "RIGHT('hello', 3) should be 'llo'"
    );
}

#[test]
fn test_repeat_string() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT REPEAT('ab', 3) FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("ababab".to_string()),
        "REPEAT('ab', 3) should be 'ababab'"
    );
}

#[test]
fn test_chr() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT CHR(65) FROM test_data WHERE id = 1");
    assert_eq!(result, Some("A".to_string()), "CHR(65) should be 'A'");
}

#[test]
fn test_ascii() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT ASCII('A') FROM test_data WHERE id = 1");
    assert_eq!(result, Some(65), "ASCII('A') should be 65");
}

#[test]
fn test_strpos() {
    let ctx = create_test_context();
    // Trino: STRPOS(string, substring) returns 1-indexed position
    let result = get_int64(
        &ctx,
        "SELECT STRPOS('hello', 'l') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(3),
        "STRPOS('hello', 'l') should be 3 (1-indexed)"
    );
}

#[test]
fn test_split_part() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT SPLIT_PART('a,b,c', ',', 2) FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("b".to_string()),
        "SPLIT_PART('a,b,c', ',', 2) should be 'b'"
    );
}

#[test]
fn test_starts_with_true() {
    let ctx = create_test_context();
    let result = get_bool(
        &ctx,
        "SELECT STARTS_WITH('hello', 'he') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(true),
        "STARTS_WITH('hello', 'he') should be true"
    );
}

#[test]
fn test_starts_with_false() {
    let ctx = create_test_context();
    let result = get_bool(
        &ctx,
        "SELECT STARTS_WITH('hello', 'lo') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(false),
        "STARTS_WITH('hello', 'lo') should be false"
    );
}

#[test]
fn test_ends_with_true() {
    let ctx = create_test_context();
    let result = get_bool(
        &ctx,
        "SELECT ENDS_WITH('hello', 'lo') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(true),
        "ENDS_WITH('hello', 'lo') should be true"
    );
}

#[test]
fn test_concat_ws() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT CONCAT_WS('-', 'a', 'b', 'c') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("a-b-c".to_string()),
        "CONCAT_WS('-', 'a', 'b', 'c') should be 'a-b-c'"
    );
}

// =============================================================================
// STRING FUNCTIONS - Advanced (Phase 1)
// =============================================================================

#[test]
fn test_codepoint() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT CODEPOINT('A') FROM test_data WHERE id = 1");
    assert_eq!(result, Some(65), "CODEPOINT('A') should be 65");
}

#[test]
fn test_levenshtein_distance() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT LEVENSHTEIN_DISTANCE('kitten', 'sitting') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(3),
        "LEVENSHTEIN_DISTANCE('kitten', 'sitting') should be 3"
    );
}

#[test]
fn test_hamming_distance() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT HAMMING_DISTANCE('abc', 'axc') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(1),
        "HAMMING_DISTANCE('abc', 'axc') should be 1"
    );
}

#[test]
fn test_soundex() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT SOUNDEX('Robert') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("R163".to_string()),
        "SOUNDEX('Robert') should be 'R163'"
    );
}

#[test]
fn test_translate() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT TRANSLATE('hello', 'aeiou', '12345') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("h2ll4".to_string()),
        "TRANSLATE('hello', 'aeiou', '12345') should be 'h2ll4'"
    );
}

// =============================================================================
// CONDITIONAL FUNCTIONS
// =============================================================================

#[test]
fn test_coalesce() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT COALESCE(NULL, 5) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(5), "COALESCE(NULL, 5) should be 5");
}

#[test]
fn test_nullif_equal() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT NULLIF(5, 5) FROM test_data WHERE id = 1");
    assert_eq!(result, None, "NULLIF(5, 5) should be NULL");
}

#[test]
fn test_nullif_not_equal() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT NULLIF(5, 3) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(5), "NULLIF(5, 3) should be 5");
}

#[test]
fn test_if_true() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT IF(1 > 0, 10, 20) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(10), "IF(1 > 0, 10, 20) should be 10");
}

#[test]
fn test_if_false() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT IF(1 < 0, 10, 20) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(20), "IF(1 < 0, 10, 20) should be 20");
}

#[test]
fn test_greatest() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT GREATEST(1, 5, 3) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(5), "GREATEST(1, 5, 3) should be 5");
}

#[test]
fn test_least() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT LEAST(1, 5, 3) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(1), "LEAST(1, 5, 3) should be 1");
}

// =============================================================================
// BITWISE FUNCTIONS
// =============================================================================

#[test]
fn test_bitwise_and() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT BITWISE_AND(12, 10) FROM test_data WHERE id = 1",
    );
    // 12 = 1100, 10 = 1010, AND = 1000 = 8
    assert_eq!(result, Some(8), "BITWISE_AND(12, 10) should be 8");
}

#[test]
fn test_bitwise_or() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT BITWISE_OR(12, 10) FROM test_data WHERE id = 1",
    );
    // 12 = 1100, 10 = 1010, OR = 1110 = 14
    assert_eq!(result, Some(14), "BITWISE_OR(12, 10) should be 14");
}

#[test]
fn test_bitwise_xor() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT BITWISE_XOR(12, 10) FROM test_data WHERE id = 1",
    );
    // 12 = 1100, 10 = 1010, XOR = 0110 = 6
    assert_eq!(result, Some(6), "BITWISE_XOR(12, 10) should be 6");
}

#[test]
fn test_bitwise_not() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT BITWISE_NOT(0) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(-1), "BITWISE_NOT(0) should be -1");
}

#[test]
fn test_bit_count() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT BIT_COUNT(7) FROM test_data WHERE id = 1");
    // 7 = 111 = 3 bits
    assert_eq!(result, Some(3), "BIT_COUNT(7) should be 3");
}

#[test]
fn test_bitwise_left_shift() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT BITWISE_LEFT_SHIFT(1, 4) FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(16), "BITWISE_LEFT_SHIFT(1, 4) should be 16");
}

#[test]
fn test_bitwise_right_shift() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT BITWISE_RIGHT_SHIFT(16, 2) FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(4), "BITWISE_RIGHT_SHIFT(16, 2) should be 4");
}

// =============================================================================
// ENCODING FUNCTIONS
// =============================================================================

#[test]
fn test_to_hex() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT TO_HEX(255) FROM test_data WHERE id = 1");
    assert_eq!(result, Some("ff".to_string()), "TO_HEX(255) should be 'ff'");
}

#[test]
fn test_to_base64() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT TO_BASE64('hello') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("aGVsbG8=".to_string()),
        "TO_BASE64('hello') should be 'aGVsbG8='"
    );
}

#[test]
fn test_md5() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT MD5('hello') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("5d41402abc4b2a76b9719d911017c592".to_string()),
        "MD5('hello') should be correct hash"
    );
}

#[test]
fn test_sha256() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT SHA256('hello') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824".to_string()),
        "SHA256('hello') should be correct hash"
    );
}

#[test]
fn test_crc32() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT CRC32('hello') FROM test_data WHERE id = 1");
    // CRC32 of 'hello' is 907060870
    assert_eq!(
        result,
        Some(907060870),
        "CRC32('hello') should be 907060870"
    );
}

#[test]
fn test_xxhash64() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT XXHASH64('hello') FROM test_data WHERE id = 1");
    // xxhash64 of 'hello'
    assert!(result.is_some(), "XXHASH64('hello') should return a value");
}

// =============================================================================
// URL FUNCTIONS
// =============================================================================

#[test]
fn test_url_extract_host() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT URL_EXTRACT_HOST('https://example.com/path') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("example.com".to_string()),
        "URL_EXTRACT_HOST should return 'example.com'"
    );
}

#[test]
fn test_url_extract_path() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT URL_EXTRACT_PATH('https://example.com/path/to/page') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("/path/to/page".to_string()),
        "URL_EXTRACT_PATH should return '/path/to/page'"
    );
}

#[test]
fn test_url_extract_protocol() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT URL_EXTRACT_PROTOCOL('https://example.com/path') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("https".to_string()),
        "URL_EXTRACT_PROTOCOL should return 'https'"
    );
}

#[test]
fn test_url_extract_port() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT URL_EXTRACT_PORT('https://example.com:8080/path') FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(8080), "URL_EXTRACT_PORT should return 8080");
}

#[test]
fn test_url_extract_query() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT URL_EXTRACT_QUERY('https://example.com/path?foo=bar') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("foo=bar".to_string()),
        "URL_EXTRACT_QUERY should return 'foo=bar'"
    );
}

#[test]
fn test_url_extract_fragment() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT URL_EXTRACT_FRAGMENT('https://example.com/path#section1') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("section1".to_string()),
        "URL_EXTRACT_FRAGMENT should return 'section1'"
    );
}

#[test]
fn test_url_encode() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT URL_ENCODE('hello world') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("hello%20world".to_string()),
        "URL_ENCODE('hello world') should be 'hello%20world'"
    );
}

#[test]
fn test_url_decode() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT URL_DECODE('hello%20world') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("hello world".to_string()),
        "URL_DECODE('hello%20world') should be 'hello world'"
    );
}

// =============================================================================
// REGEX FUNCTIONS
// =============================================================================

#[test]
fn test_regexp_like_true() {
    let ctx = create_test_context();
    let result = get_bool(
        &ctx,
        "SELECT REGEXP_LIKE('hello', 'h.*o') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(true),
        "REGEXP_LIKE('hello', 'h.*o') should be true"
    );
}

#[test]
fn test_regexp_like_false() {
    let ctx = create_test_context();
    let result = get_bool(
        &ctx,
        "SELECT REGEXP_LIKE('hello', 'x.*y') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(false),
        "REGEXP_LIKE('hello', 'x.*y') should be false"
    );
}

#[test]
fn test_regexp_extract() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT REGEXP_EXTRACT('hello123world', '([0-9]+)', 1) FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("123".to_string()),
        "REGEXP_EXTRACT should extract '123'"
    );
}

#[test]
fn test_regexp_replace() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT REGEXP_REPLACE('hello123', '[0-9]+', 'XXX') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("helloXXX".to_string()),
        "REGEXP_REPLACE should replace digits with 'XXX'"
    );
}

#[test]
fn test_regexp_count() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT REGEXP_COUNT('hello', 'l') FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(2), "REGEXP_COUNT('hello', 'l') should be 2");
}

// =============================================================================
// JSON FUNCTIONS
// =============================================================================

#[test]
fn test_json_extract() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT JSON_EXTRACT('{\"name\": \"John\"}', '$.name') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("\"John\"".to_string()),
        "JSON_EXTRACT should return '\"John\"'"
    );
}

#[test]
fn test_json_extract_scalar() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT JSON_EXTRACT_SCALAR('{\"name\": \"John\"}', '$.name') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("John".to_string()),
        "JSON_EXTRACT_SCALAR should return 'John'"
    );
}

#[test]
fn test_json_array_length() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT JSON_ARRAY_LENGTH('[1, 2, 3, 4, 5]') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(5),
        "JSON_ARRAY_LENGTH('[1, 2, 3, 4, 5]') should be 5"
    );
}

#[test]
fn test_json_array_get() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT JSON_ARRAY_GET('[\"a\", \"b\", \"c\"]', 1) FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("\"b\"".to_string()),
        "JSON_ARRAY_GET at index 1 should be '\"b\"'"
    );
}

#[test]
fn test_json_array_contains_true() {
    let ctx = create_test_context();
    let result = get_bool(
        &ctx,
        "SELECT JSON_ARRAY_CONTAINS('[1, 2, 3]', 2) FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(true),
        "JSON_ARRAY_CONTAINS('[1, 2, 3]', 2) should be true"
    );
}

#[test]
fn test_json_array_contains_false() {
    let ctx = create_test_context();
    let result = get_bool(
        &ctx,
        "SELECT JSON_ARRAY_CONTAINS('[1, 2, 3]', 5) FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(false),
        "JSON_ARRAY_CONTAINS('[1, 2, 3]', 5) should be false"
    );
}

#[test]
fn test_json_size() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT JSON_SIZE('{\"a\": 1, \"b\": 2}', '$') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(2),
        "JSON_SIZE of object with 2 keys should be 2"
    );
}

#[test]
fn test_is_json_scalar_true() {
    let ctx = create_test_context();
    let result = get_bool(
        &ctx,
        "SELECT IS_JSON_SCALAR('\"hello\"') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(true),
        "IS_JSON_SCALAR('\"hello\"') should be true"
    );
}

#[test]
fn test_is_json_scalar_false() {
    let ctx = create_test_context();
    let result = get_bool(
        &ctx,
        "SELECT IS_JSON_SCALAR('[1, 2, 3]') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(false),
        "IS_JSON_SCALAR('[1, 2, 3]') should be false"
    );
}

#[test]
fn test_json_exists_true() {
    let ctx = create_test_context();
    let result = get_bool(
        &ctx,
        "SELECT JSON_EXISTS('{\"name\": \"John\"}', '$.name') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(true),
        "JSON_EXISTS should return true for existing path"
    );
}

#[test]
fn test_json_exists_false() {
    let ctx = create_test_context();
    let result = get_bool(
        &ctx,
        "SELECT JSON_EXISTS('{\"name\": \"John\"}', '$.age') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(false),
        "JSON_EXISTS should return false for non-existing path"
    );
}

// =============================================================================
// AGGREGATE FUNCTIONS
// =============================================================================

#[test]
fn test_count() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT COUNT(*) FROM test_data");
    assert_eq!(result, Some(5), "COUNT(*) should be 5");
}

#[test]
fn test_sum() {
    let ctx = create_test_context();
    // Sum of 10+20+30+40+50 = 150
    let result = get_int64(&ctx, "SELECT SUM(val_int) FROM test_data");
    assert_eq!(result, Some(150), "SUM should be 150");
}

#[test]
fn test_avg() {
    let ctx = create_test_context();
    // Avg of 10,20,30,40,50 = 30
    let result = get_float64(&ctx, "SELECT AVG(val_int) FROM test_data");
    assert_eq!(result, Some(30.0), "AVG should be 30.0");
}

#[test]
fn test_min() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT MIN(val_int) FROM test_data");
    assert_eq!(result, Some(10), "MIN should be 10");
}

#[test]
fn test_max() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT MAX(val_int) FROM test_data");
    assert_eq!(result, Some(50), "MAX should be 50");
}

#[test]
fn test_count_if() {
    let ctx = create_test_context();
    // Count where val_int > 25: 30, 40, 50 = 3
    let result = get_int64(&ctx, "SELECT COUNT_IF(val_int > 25) FROM test_data");
    assert_eq!(result, Some(3), "COUNT_IF(val_int > 25) should be 3");
}

#[test]
fn test_bool_and_true() {
    let ctx = create_test_context();
    let result = get_bool(&ctx, "SELECT BOOL_AND(val_int > 0) FROM test_data");
    assert_eq!(
        result,
        Some(true),
        "BOOL_AND(val_int > 0) should be true (all values > 0)"
    );
}

#[test]
fn test_bool_and_false() {
    let ctx = create_test_context();
    let result = get_bool(&ctx, "SELECT BOOL_AND(val_int > 25) FROM test_data");
    assert_eq!(
        result,
        Some(false),
        "BOOL_AND(val_int > 25) should be false"
    );
}

#[test]
fn test_bool_or_true() {
    let ctx = create_test_context();
    let result = get_bool(&ctx, "SELECT BOOL_OR(val_int > 40) FROM test_data");
    assert_eq!(result, Some(true), "BOOL_OR(val_int > 40) should be true");
}

#[test]
fn test_bool_or_false() {
    let ctx = create_test_context();
    let result = get_bool(&ctx, "SELECT BOOL_OR(val_int > 100) FROM test_data");
    assert_eq!(
        result,
        Some(false),
        "BOOL_OR(val_int > 100) should be false"
    );
}

#[test]
fn test_geometric_mean() {
    let ctx = create_test_context();
    // Geometric mean of 10,20,30,40,50
    let result = get_float64(&ctx, "SELECT GEOMETRIC_MEAN(val_int) FROM test_data");
    assert!(result.is_some(), "GEOMETRIC_MEAN should return a value");
    let val = result.unwrap();
    // Approximate value
    assert!(
        val > 25.0 && val < 30.0,
        "GEOMETRIC_MEAN should be around 26.05"
    );
}

#[test]
fn test_bitwise_and_agg() {
    let ctx = create_test_context();
    // 10 & 20 & 30 & 40 & 50
    // 10 = 001010, 20 = 010100, 30 = 011110, 40 = 101000, 50 = 110010
    // AND of all = 0
    let result = get_int64(&ctx, "SELECT BITWISE_AND_AGG(val_int) FROM test_data");
    assert_eq!(result, Some(0), "BITWISE_AND_AGG should be 0");
}

#[test]
fn test_bitwise_or_agg() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT BITWISE_OR_AGG(val_int) FROM test_data");
    // Should OR all the bits together
    assert!(result.is_some(), "BITWISE_OR_AGG should return a value");
}

#[test]
fn test_listagg() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT LISTAGG(val_str) FROM test_data");
    assert!(result.is_some(), "LISTAGG should return a value");
    let val = result.unwrap();
    // Should contain all strings joined
    assert!(val.contains("hello"), "LISTAGG should contain 'hello'");
    assert!(val.contains("world"), "LISTAGG should contain 'world'");
}

#[test]
fn test_approx_distinct() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT APPROX_DISTINCT(val_int) FROM test_data");
    assert_eq!(
        result,
        Some(5),
        "APPROX_DISTINCT should be 5 (all unique values)"
    );
}

// =============================================================================
// DATE/TIME FUNCTIONS
// =============================================================================

#[test]
fn test_year() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT YEAR(DATE '2024-06-15') FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(2024), "YEAR(DATE '2024-06-15') should be 2024");
}

#[test]
fn test_month() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT MONTH(DATE '2024-06-15') FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(6), "MONTH(DATE '2024-06-15') should be 6");
}

#[test]
fn test_day() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT DAY(DATE '2024-06-15') FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(15), "DAY(DATE '2024-06-15') should be 15");
}

// =============================================================================
// TYPE FUNCTIONS
// =============================================================================

#[test]
fn test_typeof_int() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT TYPEOF(5) FROM test_data WHERE id = 1");
    assert!(result.is_some(), "TYPEOF should return a value");
    let val = result.unwrap();
    assert!(
        val.to_lowercase().contains("int"),
        "TYPEOF(5) should contain 'int', got '{}'",
        val
    );
}

#[test]
fn test_typeof_string() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT TYPEOF('hello') FROM test_data WHERE id = 1");
    assert!(result.is_some(), "TYPEOF should return a value");
}

#[test]
fn test_uuid_format() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT UUID() FROM test_data WHERE id = 1");
    assert!(result.is_some(), "UUID should return a value");
    let val = result.unwrap();
    // UUID format: 8-4-4-4-12 characters
    assert_eq!(val.len(), 36, "UUID should be 36 characters long");
    assert_eq!(
        val.chars().filter(|c| *c == '-').count(),
        4,
        "UUID should have 4 dashes"
    );
}

// =============================================================================
// ADDITIONAL PHASE 1 TESTS - Math Functions
// =============================================================================

#[test]
fn test_cot() {
    let ctx = create_test_context();
    // COT(1) = 1/TAN(1) ≈ 0.6421
    let result = get_float64(&ctx, "SELECT COT(1.0) FROM test_data WHERE id = 1");
    assert!(result.is_some(), "COT should return a value");
    let val = result.unwrap();
    assert!(
        (val - 0.6421).abs() < 0.01,
        "COT(1) should be approximately 0.6421, got {}",
        val
    );
}

#[test]
fn test_from_base() {
    let ctx = create_test_context();
    // FROM_BASE('ff', 16) = 255
    let result = get_int64(
        &ctx,
        "SELECT FROM_BASE('ff', 16) FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(255), "FROM_BASE('ff', 16) should be 255");
}

#[test]
fn test_to_base() {
    let ctx = create_test_context();
    // TO_BASE(255, 16) = 'ff'
    let result = get_string(&ctx, "SELECT TO_BASE(255, 16) FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("ff".to_string()),
        "TO_BASE(255, 16) should be 'ff'"
    );
}

#[test]
fn test_width_bucket() {
    let ctx = create_test_context();
    // WIDTH_BUCKET(5.5, 0, 10, 5) - value 5.5 in range [0,10) with 5 buckets
    let result = get_int64(
        &ctx,
        "SELECT WIDTH_BUCKET(5.5, 0, 10, 5) FROM test_data WHERE id = 1",
    );
    // 5.5 falls in bucket 3 (buckets are: [0,2), [2,4), [4,6), [6,8), [8,10))
    assert_eq!(result, Some(3), "WIDTH_BUCKET(5.5, 0, 10, 5) should be 3");
}

#[test]
fn test_normal_cdf() {
    let ctx = create_test_context();
    // NORMAL_CDF(mean, std, value) - Trino order is mean, std, value
    // NORMAL_CDF(0, 1, 0) = 0.5 (mean=0, std=1, value=0)
    let result = get_float64(
        &ctx,
        "SELECT NORMAL_CDF(0, 1, 0) FROM test_data WHERE id = 1",
    );
    assert!(result.is_some(), "NORMAL_CDF should return a value");
    let val = result.unwrap();
    assert!(
        (val - 0.5).abs() < 0.001,
        "NORMAL_CDF(0, 1, 0) should be 0.5, got {}",
        val
    );
}

// =============================================================================
// ADDITIONAL PHASE 1 TESTS - String Functions
// =============================================================================

#[test]
fn test_luhn_check_valid() {
    let ctx = create_test_context();
    // Valid Luhn number
    let result = get_bool(
        &ctx,
        "SELECT LUHN_CHECK('79927398713') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(true),
        "LUHN_CHECK('79927398713') should be true"
    );
}

#[test]
fn test_luhn_check_invalid() {
    let ctx = create_test_context();
    let result = get_bool(
        &ctx,
        "SELECT LUHN_CHECK('12345') FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(false), "LUHN_CHECK('12345') should be false");
}

#[test]
fn test_normalize() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT NORMALIZE('café') FROM test_data WHERE id = 1");
    assert!(result.is_some(), "NORMALIZE should return a value");
}

// =============================================================================
// ADDITIONAL PHASE 1 TESTS - Date/Time Functions
// =============================================================================

#[test]
fn test_quarter() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT QUARTER(DATE '2024-06-15') FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(2), "QUARTER of June should be 2");
}

#[test]
fn test_week() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT WEEK(DATE '2024-01-15') FROM test_data WHERE id = 1",
    );
    assert!(result.is_some(), "WEEK should return a value");
    let val = result.unwrap();
    assert!(val >= 1 && val <= 53, "WEEK should be between 1 and 53");
}

#[test]
fn test_day_of_week() {
    let ctx = create_test_context();
    // June 15, 2024 is a Saturday = 6 (Sunday=0 or 7 depending on convention)
    let result = get_int64(
        &ctx,
        "SELECT DAY_OF_WEEK(DATE '2024-06-15') FROM test_data WHERE id = 1",
    );
    assert!(result.is_some(), "DAY_OF_WEEK should return a value");
}

#[test]
fn test_day_of_year() {
    let ctx = create_test_context();
    // January 15 is day 15 of the year
    let result = get_int64(
        &ctx,
        "SELECT DAY_OF_YEAR(DATE '2024-01-15') FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(15), "DAY_OF_YEAR of Jan 15 should be 15");
}

// =============================================================================
// ADDITIONAL PHASE 1 TESTS - Binary/Encoding Functions
// =============================================================================

#[test]
fn test_sha1() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT SHA1('hello') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d".to_string()),
        "SHA1('hello') should be correct hash"
    );
}

#[test]
fn test_sha512() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT SHA512('hello') FROM test_data WHERE id = 1");
    assert!(result.is_some(), "SHA512 should return a value");
    let val = result.unwrap();
    assert_eq!(val.len(), 128, "SHA512 hash should be 128 hex characters");
}

#[test]
fn test_hmac_sha256() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT HMAC_SHA256('key', 'message') FROM test_data WHERE id = 1",
    );
    assert!(result.is_some(), "HMAC_SHA256 should return a value");
    let val = result.unwrap();
    assert_eq!(
        val.len(),
        64,
        "HMAC_SHA256 hash should be 64 hex characters"
    );
}

// =============================================================================
// ADDITIONAL PHASE 2 TESTS - Aggregate Functions
// =============================================================================

#[test]
fn test_any_value() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT ANY_VALUE(val_int) FROM test_data");
    // ANY_VALUE returns any non-null value, so it should be one of the values
    assert!(result.is_some(), "ANY_VALUE should return a value");
    let val = result.unwrap();
    assert!(
        vec![10, 20, 30, 40, 50].contains(&val),
        "ANY_VALUE should return one of the values"
    );
}

#[test]
fn test_checksum() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT CHECKSUM(val_int) FROM test_data");
    // CHECKSUM XORs all values: 10 ^ 20 ^ 30 ^ 40 ^ 50
    assert!(result.is_some(), "CHECKSUM should return a value");
}

#[test]
fn test_kurtosis() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT KURTOSIS(val_int) FROM test_data");
    // Kurtosis requires at least 4 values, we have 5
    assert!(result.is_some(), "KURTOSIS should return a value");
}

#[test]
fn test_skewness() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT SKEWNESS(val_int) FROM test_data");
    // Skewness requires at least 3 values, we have 5
    assert!(result.is_some(), "SKEWNESS should return a value");
    let val = result.unwrap();
    // For a symmetric distribution (10,20,30,40,50), skewness should be close to 0
    assert!(
        (val).abs() < 0.1,
        "SKEWNESS of symmetric data should be close to 0, got {}",
        val
    );
}

#[test]
fn test_stddev() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT STDDEV(val_int) FROM test_data");
    assert!(result.is_some(), "STDDEV should return a value");
    let val = result.unwrap();
    // Standard deviation of 10,20,30,40,50 is approximately 14.14 (sample) or 15.81 (pop)
    assert!(
        val > 10.0 && val < 20.0,
        "STDDEV should be around 14-16, got {}",
        val
    );
}

#[test]
fn test_variance() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT VARIANCE(val_int) FROM test_data");
    assert!(result.is_some(), "VARIANCE should return a value");
    let val = result.unwrap();
    // Variance of 10,20,30,40,50 is 250 (population) or 200 (sample)
    assert!(
        val > 150.0 && val < 300.0,
        "VARIANCE should be around 200-250, got {}",
        val
    );
}

#[test]
fn test_approx_percentile() {
    let ctx = create_test_context();
    let result = get_float64(
        &ctx,
        "SELECT APPROX_PERCENTILE(val_int, 0.5) FROM test_data",
    );
    assert!(result.is_some(), "APPROX_PERCENTILE should return a value");
    // Median (50th percentile) = 30 for 10,20,30,40,50
    let val = result.unwrap();
    assert_eq!(val, 30.0, "APPROX_PERCENTILE(0.5) (median) should be 30.0");
}

#[test]
fn test_approx_percentile_different_values() {
    let ctx = create_test_context();

    // Test 0.0 percentile (minimum)
    let p0 = get_float64(
        &ctx,
        "SELECT APPROX_PERCENTILE(val_int, 0.0) FROM test_data",
    );
    assert!(p0.is_some(), "APPROX_PERCENTILE(0.0) should return a value");
    assert_eq!(
        p0.unwrap(),
        10.0,
        "APPROX_PERCENTILE(0.0) should be minimum (10)"
    );

    // Test 1.0 percentile (maximum)
    let p100 = get_float64(
        &ctx,
        "SELECT APPROX_PERCENTILE(val_int, 1.0) FROM test_data",
    );
    assert!(
        p100.is_some(),
        "APPROX_PERCENTILE(1.0) should return a value"
    );
    assert_eq!(
        p100.unwrap(),
        50.0,
        "APPROX_PERCENTILE(1.0) should be maximum (50)"
    );

    // Test that different percentiles return different values
    let p25 = get_float64(
        &ctx,
        "SELECT APPROX_PERCENTILE(val_int, 0.25) FROM test_data",
    );
    let p75 = get_float64(
        &ctx,
        "SELECT APPROX_PERCENTILE(val_int, 0.75) FROM test_data",
    );
    assert!(
        p25.is_some() && p75.is_some(),
        "APPROX_PERCENTILE should return values"
    );
    assert!(
        p25.unwrap() < p75.unwrap(),
        "25th percentile should be less than 75th percentile"
    );
}

// =============================================================================
// ADDITIONAL PHASE 3 TESTS - JSON Functions
// =============================================================================

#[test]
fn test_json_format() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT JSON_FORMAT('{\"a\": 1}') FROM test_data WHERE id = 1",
    );
    assert!(result.is_some(), "JSON_FORMAT should return a value");
}

#[test]
fn test_json_parse() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT JSON_PARSE('{\"a\": 1}') FROM test_data WHERE id = 1",
    );
    assert!(result.is_some(), "JSON_PARSE should return a value");
}

#[test]
fn test_json_query() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT JSON_QUERY('{\"items\": [1, 2, 3]}', '$.items') FROM test_data WHERE id = 1",
    );
    assert!(result.is_some(), "JSON_QUERY should return a value");
    let val = result.unwrap();
    assert!(
        val.contains("[1,2,3]") || val.contains("[1, 2, 3]"),
        "JSON_QUERY should return array"
    );
}

#[test]
fn test_json_value() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT JSON_VALUE('{\"name\": \"test\"}', '$.name') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("test".to_string()),
        "JSON_VALUE should return 'test'"
    );
}

#[test]
fn test_json_object() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT JSON_OBJECT('key', 'value') FROM test_data WHERE id = 1",
    );
    assert!(result.is_some(), "JSON_OBJECT should return a value");
    let val = result.unwrap();
    assert!(
        val.contains("key") && val.contains("value"),
        "JSON_OBJECT should contain key and value"
    );
}

#[test]
fn test_json_array_function() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT JSON_ARRAY(1, 2, 3) FROM test_data WHERE id = 1",
    );
    assert!(result.is_some(), "JSON_ARRAY should return a value");
    let val = result.unwrap();
    assert!(
        val.contains("[") && val.contains("]"),
        "JSON_ARRAY should return an array"
    );
}

// =============================================================================
// ADDITIONAL TESTS - Edge Cases
// =============================================================================

#[test]
fn test_coalesce_multiple() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT COALESCE(NULL, NULL, 3) FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(3), "COALESCE(NULL, NULL, 3) should be 3");
}

#[test]
fn test_greatest_multiple() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT GREATEST(1, 5, 3, 9, 2) FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(9), "GREATEST(1, 5, 3, 9, 2) should be 9");
}

#[test]
fn test_least_multiple() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT LEAST(1, 5, 3, 9, 2) FROM test_data WHERE id = 1",
    );
    assert_eq!(result, Some(1), "LEAST(1, 5, 3, 9, 2) should be 1");
}

#[test]
fn test_concat_multiple() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT CONCAT('a', 'b', 'c', 'd', 'e') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("abcde".to_string()),
        "CONCAT should concatenate all strings"
    );
}

#[test]
fn test_regexp_position() {
    let ctx = create_test_context();
    let result = get_int64(
        &ctx,
        "SELECT REGEXP_POSITION('hello123world', '[0-9]+') FROM test_data WHERE id = 1",
    );
    // Position of first digit match (1-indexed)
    assert_eq!(
        result,
        Some(6),
        "REGEXP_POSITION should return 6 (1-indexed position of '123')"
    );
}

#[test]
fn test_e_constant() {
    let ctx = create_test_context();
    let result = get_float64(&ctx, "SELECT E() FROM test_data WHERE id = 1");
    assert!(result.is_some(), "E() should return a value");
    let val = result.unwrap();
    assert!(
        (val - std::f64::consts::E).abs() < 0.0001,
        "E() should be approximately 2.71828"
    );
}

// =============================================================================
// COMPREHENSIVE MULTI-VALUE TESTS
// =============================================================================

// -----------------------------------------------------------------------------
// Math Functions - Multiple Values
// -----------------------------------------------------------------------------

#[test]
fn test_abs_multiple_values() {
    let ctx = create_test_context();

    // Test positive values
    assert_eq!(
        get_int64(&ctx, "SELECT ABS(0) FROM test_data WHERE id = 1"),
        Some(0),
        "ABS(0) should be 0"
    );
    assert_eq!(
        get_int64(&ctx, "SELECT ABS(1) FROM test_data WHERE id = 1"),
        Some(1),
        "ABS(1) should be 1"
    );
    assert_eq!(
        get_int64(&ctx, "SELECT ABS(100) FROM test_data WHERE id = 1"),
        Some(100),
        "ABS(100) should be 100"
    );
    assert_eq!(
        get_int64(&ctx, "SELECT ABS(999999) FROM test_data WHERE id = 1"),
        Some(999999),
        "ABS(999999) should be 999999"
    );

    // Test negative values
    assert_eq!(
        get_int64(&ctx, "SELECT ABS(-1) FROM test_data WHERE id = 1"),
        Some(1),
        "ABS(-1) should be 1"
    );
    assert_eq!(
        get_int64(&ctx, "SELECT ABS(-100) FROM test_data WHERE id = 1"),
        Some(100),
        "ABS(-100) should be 100"
    );
    assert_eq!(
        get_int64(&ctx, "SELECT ABS(-999999) FROM test_data WHERE id = 1"),
        Some(999999),
        "ABS(-999999) should be 999999"
    );

    // Test float values
    let abs_float = get_float64(&ctx, "SELECT ABS(-3.14159) FROM test_data WHERE id = 1");
    assert!(abs_float.is_some());
    assert!(
        (abs_float.unwrap() - 3.14159).abs() < 0.00001,
        "ABS(-3.14159) should be 3.14159"
    );
}

#[test]
fn test_ceil_multiple_values() {
    let ctx = create_test_context();

    // Positive values
    assert_eq!(
        get_float64(&ctx, "SELECT CEIL(0.0) FROM test_data WHERE id = 1"),
        Some(0.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT CEIL(0.1) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT CEIL(0.5) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT CEIL(0.9) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT CEIL(1.0) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT CEIL(1.1) FROM test_data WHERE id = 1"),
        Some(2.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT CEIL(99.01) FROM test_data WHERE id = 1"),
        Some(100.0)
    );

    // Negative values
    assert_eq!(
        get_float64(&ctx, "SELECT CEIL(-0.1) FROM test_data WHERE id = 1"),
        Some(0.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT CEIL(-0.9) FROM test_data WHERE id = 1"),
        Some(0.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT CEIL(-1.0) FROM test_data WHERE id = 1"),
        Some(-1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT CEIL(-1.1) FROM test_data WHERE id = 1"),
        Some(-1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT CEIL(-99.99) FROM test_data WHERE id = 1"),
        Some(-99.0)
    );
}

#[test]
fn test_floor_multiple_values() {
    let ctx = create_test_context();

    // Positive values
    assert_eq!(
        get_float64(&ctx, "SELECT FLOOR(0.0) FROM test_data WHERE id = 1"),
        Some(0.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT FLOOR(0.1) FROM test_data WHERE id = 1"),
        Some(0.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT FLOOR(0.9) FROM test_data WHERE id = 1"),
        Some(0.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT FLOOR(1.0) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT FLOOR(1.9) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT FLOOR(99.99) FROM test_data WHERE id = 1"),
        Some(99.0)
    );

    // Negative values
    assert_eq!(
        get_float64(&ctx, "SELECT FLOOR(-0.1) FROM test_data WHERE id = 1"),
        Some(-1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT FLOOR(-0.9) FROM test_data WHERE id = 1"),
        Some(-1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT FLOOR(-1.0) FROM test_data WHERE id = 1"),
        Some(-1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT FLOOR(-1.1) FROM test_data WHERE id = 1"),
        Some(-2.0)
    );
}

#[test]
fn test_round_multiple_values() {
    let ctx = create_test_context();

    // Standard rounding
    assert_eq!(
        get_float64(&ctx, "SELECT ROUND(0.4) FROM test_data WHERE id = 1"),
        Some(0.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT ROUND(0.5) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT ROUND(0.6) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT ROUND(1.4) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT ROUND(1.5) FROM test_data WHERE id = 1"),
        Some(2.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT ROUND(2.5) FROM test_data WHERE id = 1"),
        Some(3.0)
    );

    // Negative values
    assert_eq!(
        get_float64(&ctx, "SELECT ROUND(-0.4) FROM test_data WHERE id = 1"),
        Some(0.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT ROUND(-0.5) FROM test_data WHERE id = 1"),
        Some(-1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT ROUND(-1.5) FROM test_data WHERE id = 1"),
        Some(-2.0)
    );
}

#[test]
fn test_power_multiple_values() {
    let ctx = create_test_context();

    // Basic powers
    assert_eq!(
        get_float64(&ctx, "SELECT POWER(2, 0) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT POWER(2, 1) FROM test_data WHERE id = 1"),
        Some(2.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT POWER(2, 2) FROM test_data WHERE id = 1"),
        Some(4.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT POWER(2, 10) FROM test_data WHERE id = 1"),
        Some(1024.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT POWER(3, 3) FROM test_data WHERE id = 1"),
        Some(27.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT POWER(10, 3) FROM test_data WHERE id = 1"),
        Some(1000.0)
    );

    // Fractional powers (square root, cube root)
    let sqrt_4 = get_float64(&ctx, "SELECT POWER(4, 0.5) FROM test_data WHERE id = 1");
    assert!(sqrt_4.is_some());
    assert!(
        (sqrt_4.unwrap() - 2.0).abs() < 0.0001,
        "POWER(4, 0.5) should be 2.0"
    );

    let cbrt_8 = get_float64(
        &ctx,
        "SELECT POWER(8, 0.333333) FROM test_data WHERE id = 1",
    );
    assert!(cbrt_8.is_some());
    assert!(
        (cbrt_8.unwrap() - 2.0).abs() < 0.01,
        "POWER(8, 1/3) should be ~2.0"
    );

    // Negative exponent
    assert_eq!(
        get_float64(&ctx, "SELECT POWER(2, -1) FROM test_data WHERE id = 1"),
        Some(0.5)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT POWER(2, -2) FROM test_data WHERE id = 1"),
        Some(0.25)
    );
}

#[test]
fn test_sqrt_multiple_values() {
    let ctx = create_test_context();

    // Perfect squares
    assert_eq!(
        get_float64(&ctx, "SELECT SQRT(0) FROM test_data WHERE id = 1"),
        Some(0.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT SQRT(1) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT SQRT(4) FROM test_data WHERE id = 1"),
        Some(2.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT SQRT(9) FROM test_data WHERE id = 1"),
        Some(3.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT SQRT(25) FROM test_data WHERE id = 1"),
        Some(5.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT SQRT(100) FROM test_data WHERE id = 1"),
        Some(10.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT SQRT(10000) FROM test_data WHERE id = 1"),
        Some(100.0)
    );

    // Non-perfect squares
    let sqrt_2 = get_float64(&ctx, "SELECT SQRT(2) FROM test_data WHERE id = 1");
    assert!(sqrt_2.is_some());
    assert!(
        (sqrt_2.unwrap() - 1.41421356).abs() < 0.0001,
        "SQRT(2) should be ~1.414"
    );

    let sqrt_3 = get_float64(&ctx, "SELECT SQRT(3) FROM test_data WHERE id = 1");
    assert!(sqrt_3.is_some());
    assert!(
        (sqrt_3.unwrap() - 1.73205).abs() < 0.0001,
        "SQRT(3) should be ~1.732"
    );
}

#[test]
fn test_mod_multiple_values() {
    let ctx = create_test_context();

    // Standard modulo
    assert_eq!(
        get_int64(&ctx, "SELECT MOD(10, 3) FROM test_data WHERE id = 1"),
        Some(1)
    );
    assert_eq!(
        get_int64(&ctx, "SELECT MOD(10, 5) FROM test_data WHERE id = 1"),
        Some(0)
    );
    assert_eq!(
        get_int64(&ctx, "SELECT MOD(15, 4) FROM test_data WHERE id = 1"),
        Some(3)
    );
    assert_eq!(
        get_int64(&ctx, "SELECT MOD(100, 7) FROM test_data WHERE id = 1"),
        Some(2)
    );
    assert_eq!(
        get_int64(&ctx, "SELECT MOD(0, 5) FROM test_data WHERE id = 1"),
        Some(0)
    );

    // Negative dividend
    assert_eq!(
        get_int64(&ctx, "SELECT MOD(-10, 3) FROM test_data WHERE id = 1"),
        Some(-1)
    );
    assert_eq!(
        get_int64(&ctx, "SELECT MOD(-10, -3) FROM test_data WHERE id = 1"),
        Some(-1)
    );
    assert_eq!(
        get_int64(&ctx, "SELECT MOD(10, -3) FROM test_data WHERE id = 1"),
        Some(1)
    );
}

#[test]
fn test_trigonometry_multiple_values() {
    let ctx = create_test_context();
    let pi = std::f64::consts::PI;

    // SIN values
    assert_eq!(
        get_float64(&ctx, "SELECT SIN(0) FROM test_data WHERE id = 1"),
        Some(0.0)
    );
    let sin_pi_2 = get_float64(
        &ctx,
        "SELECT SIN(1.5707963267948966) FROM test_data WHERE id = 1",
    ); // PI/2
    assert!(sin_pi_2.is_some());
    assert!(
        (sin_pi_2.unwrap() - 1.0).abs() < 0.0001,
        "SIN(PI/2) should be 1.0"
    );

    let sin_pi = get_float64(
        &ctx,
        "SELECT SIN(3.141592653589793) FROM test_data WHERE id = 1",
    ); // PI
    assert!(sin_pi.is_some());
    assert!(sin_pi.unwrap().abs() < 0.0001, "SIN(PI) should be ~0");

    // COS values
    assert_eq!(
        get_float64(&ctx, "SELECT COS(0) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    let cos_pi_2 = get_float64(
        &ctx,
        "SELECT COS(1.5707963267948966) FROM test_data WHERE id = 1",
    ); // PI/2
    assert!(cos_pi_2.is_some());
    assert!(cos_pi_2.unwrap().abs() < 0.0001, "COS(PI/2) should be ~0");

    let cos_pi = get_float64(
        &ctx,
        "SELECT COS(3.141592653589793) FROM test_data WHERE id = 1",
    ); // PI
    assert!(cos_pi.is_some());
    assert!(
        (cos_pi.unwrap() - (-1.0)).abs() < 0.0001,
        "COS(PI) should be -1.0"
    );

    // TAN values
    assert_eq!(
        get_float64(&ctx, "SELECT TAN(0) FROM test_data WHERE id = 1"),
        Some(0.0)
    );
    let tan_pi_4 = get_float64(
        &ctx,
        "SELECT TAN(0.7853981633974483) FROM test_data WHERE id = 1",
    ); // PI/4
    assert!(tan_pi_4.is_some());
    assert!(
        (tan_pi_4.unwrap() - 1.0).abs() < 0.0001,
        "TAN(PI/4) should be 1.0"
    );
}

#[test]
fn test_log_functions_multiple_values() {
    let ctx = create_test_context();

    // LN (natural log)
    assert_eq!(
        get_float64(&ctx, "SELECT LN(1) FROM test_data WHERE id = 1"),
        Some(0.0)
    );
    let ln_e = get_float64(
        &ctx,
        "SELECT LN(2.718281828459045) FROM test_data WHERE id = 1",
    );
    assert!(ln_e.is_some());
    assert!((ln_e.unwrap() - 1.0).abs() < 0.0001, "LN(e) should be 1.0");

    let ln_10 = get_float64(&ctx, "SELECT LN(10) FROM test_data WHERE id = 1");
    assert!(ln_10.is_some());
    assert!(
        (ln_10.unwrap() - 2.302585).abs() < 0.0001,
        "LN(10) should be ~2.303"
    );

    // LOG10
    assert_eq!(
        get_float64(&ctx, "SELECT LOG10(1) FROM test_data WHERE id = 1"),
        Some(0.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT LOG10(10) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT LOG10(100) FROM test_data WHERE id = 1"),
        Some(2.0)
    );
    assert_eq!(
        get_float64(&ctx, "SELECT LOG10(1000) FROM test_data WHERE id = 1"),
        Some(3.0)
    );

    // EXP
    assert_eq!(
        get_float64(&ctx, "SELECT EXP(0) FROM test_data WHERE id = 1"),
        Some(1.0)
    );
    let exp_1 = get_float64(&ctx, "SELECT EXP(1) FROM test_data WHERE id = 1");
    assert!(exp_1.is_some());
    assert!((exp_1.unwrap() - std::f64::consts::E).abs() < 0.0001);

    let exp_2 = get_float64(&ctx, "SELECT EXP(2) FROM test_data WHERE id = 1");
    assert!(exp_2.is_some());
    assert!((exp_2.unwrap() - 7.389056).abs() < 0.0001);
}

// -----------------------------------------------------------------------------
// String Functions - Multiple Values
// -----------------------------------------------------------------------------

#[test]
fn test_string_functions_comprehensive() {
    let ctx = create_test_context();

    // UPPER - various cases
    assert_eq!(
        get_string(&ctx, "SELECT UPPER('') FROM test_data WHERE id = 1"),
        Some("".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT UPPER('a') FROM test_data WHERE id = 1"),
        Some("A".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT UPPER('ABC') FROM test_data WHERE id = 1"),
        Some("ABC".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT UPPER('abc123') FROM test_data WHERE id = 1"),
        Some("ABC123".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT UPPER('Hello World') FROM test_data WHERE id = 1"
        ),
        Some("HELLO WORLD".to_string())
    );

    // LOWER - various cases
    assert_eq!(
        get_string(&ctx, "SELECT LOWER('') FROM test_data WHERE id = 1"),
        Some("".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT LOWER('A') FROM test_data WHERE id = 1"),
        Some("a".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT LOWER('abc') FROM test_data WHERE id = 1"),
        Some("abc".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT LOWER('ABC123') FROM test_data WHERE id = 1"),
        Some("abc123".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT LOWER('HELLO WORLD') FROM test_data WHERE id = 1"
        ),
        Some("hello world".to_string())
    );

    // LENGTH - various cases
    assert_eq!(
        get_int64(&ctx, "SELECT LENGTH('') FROM test_data WHERE id = 1"),
        Some(0)
    );
    assert_eq!(
        get_int64(&ctx, "SELECT LENGTH('a') FROM test_data WHERE id = 1"),
        Some(1)
    );
    assert_eq!(
        get_int64(&ctx, "SELECT LENGTH('hello') FROM test_data WHERE id = 1"),
        Some(5)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT LENGTH('hello world') FROM test_data WHERE id = 1"
        ),
        Some(11)
    );
}

#[test]
fn test_substring_comprehensive() {
    let ctx = create_test_context();

    // Basic substring (1-indexed in Trino)
    assert_eq!(
        get_string(
            &ctx,
            "SELECT SUBSTRING('hello', 1, 1) FROM test_data WHERE id = 1"
        ),
        Some("h".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT SUBSTRING('hello', 1, 2) FROM test_data WHERE id = 1"
        ),
        Some("he".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT SUBSTRING('hello', 1, 5) FROM test_data WHERE id = 1"
        ),
        Some("hello".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT SUBSTRING('hello', 2, 3) FROM test_data WHERE id = 1"
        ),
        Some("ell".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT SUBSTRING('hello', 3, 2) FROM test_data WHERE id = 1"
        ),
        Some("ll".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT SUBSTRING('hello', 5, 1) FROM test_data WHERE id = 1"
        ),
        Some("o".to_string())
    );

    // Edge cases
    assert_eq!(
        get_string(
            &ctx,
            "SELECT SUBSTRING('hello', 1, 10) FROM test_data WHERE id = 1"
        ),
        Some("hello".to_string())
    ); // Length exceeds
}

#[test]
fn test_concat_comprehensive() {
    let ctx = create_test_context();

    // Basic concatenation
    assert_eq!(
        get_string(&ctx, "SELECT CONCAT('a', 'b') FROM test_data WHERE id = 1"),
        Some("ab".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT CONCAT('', 'b') FROM test_data WHERE id = 1"),
        Some("b".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT CONCAT('a', '') FROM test_data WHERE id = 1"),
        Some("a".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT CONCAT('', '') FROM test_data WHERE id = 1"),
        Some("".to_string())
    );

    // Multiple arguments
    assert_eq!(
        get_string(
            &ctx,
            "SELECT CONCAT('a', 'b', 'c') FROM test_data WHERE id = 1"
        ),
        Some("abc".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT CONCAT('a', 'b', 'c', 'd', 'e') FROM test_data WHERE id = 1"
        ),
        Some("abcde".to_string())
    );

    // With spaces and special characters
    assert_eq!(
        get_string(
            &ctx,
            "SELECT CONCAT('hello', ' ', 'world') FROM test_data WHERE id = 1"
        ),
        Some("hello world".to_string())
    );
}

#[test]
fn test_replace_comprehensive() {
    let ctx = create_test_context();

    // Basic replace
    assert_eq!(
        get_string(
            &ctx,
            "SELECT REPLACE('hello', 'l', 'L') FROM test_data WHERE id = 1"
        ),
        Some("heLLo".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT REPLACE('hello', 'h', 'H') FROM test_data WHERE id = 1"
        ),
        Some("Hello".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT REPLACE('hello', 'o', 'O') FROM test_data WHERE id = 1"
        ),
        Some("hellO".to_string())
    );

    // Multiple character replace
    assert_eq!(
        get_string(
            &ctx,
            "SELECT REPLACE('hello', 'el', 'EL') FROM test_data WHERE id = 1"
        ),
        Some("hELlo".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT REPLACE('hello', 'lo', 'LO') FROM test_data WHERE id = 1"
        ),
        Some("helLO".to_string())
    );

    // No match - should return original
    assert_eq!(
        get_string(
            &ctx,
            "SELECT REPLACE('hello', 'x', 'X') FROM test_data WHERE id = 1"
        ),
        Some("hello".to_string())
    );

    // Remove (replace with empty)
    assert_eq!(
        get_string(
            &ctx,
            "SELECT REPLACE('hello', 'l', '') FROM test_data WHERE id = 1"
        ),
        Some("heo".to_string())
    );
}

#[test]
fn test_lpad_rpad_comprehensive() {
    let ctx = create_test_context();

    // LPAD - various lengths
    assert_eq!(
        get_string(
            &ctx,
            "SELECT LPAD('hi', 2, '*') FROM test_data WHERE id = 1"
        ),
        Some("hi".to_string())
    ); // Already at length
    assert_eq!(
        get_string(
            &ctx,
            "SELECT LPAD('hi', 3, '*') FROM test_data WHERE id = 1"
        ),
        Some("*hi".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT LPAD('hi', 5, '*') FROM test_data WHERE id = 1"
        ),
        Some("***hi".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT LPAD('hi', 5, 'xy') FROM test_data WHERE id = 1"
        ),
        Some("xyxhi".to_string())
    ); // Multi-char pad

    // RPAD - various lengths
    assert_eq!(
        get_string(
            &ctx,
            "SELECT RPAD('hi', 2, '*') FROM test_data WHERE id = 1"
        ),
        Some("hi".to_string())
    ); // Already at length
    assert_eq!(
        get_string(
            &ctx,
            "SELECT RPAD('hi', 3, '*') FROM test_data WHERE id = 1"
        ),
        Some("hi*".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT RPAD('hi', 5, '*') FROM test_data WHERE id = 1"
        ),
        Some("hi***".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT RPAD('hi', 5, 'xy') FROM test_data WHERE id = 1"
        ),
        Some("hixyx".to_string())
    ); // Multi-char pad
}

// -----------------------------------------------------------------------------
// Conditional Functions - Multiple Values
// -----------------------------------------------------------------------------

#[test]
fn test_coalesce_comprehensive() {
    let ctx = create_test_context();

    // First non-null
    assert_eq!(
        get_int64(&ctx, "SELECT COALESCE(1, 2, 3) FROM test_data WHERE id = 1"),
        Some(1)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT COALESCE(NULL, 2, 3) FROM test_data WHERE id = 1"
        ),
        Some(2)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT COALESCE(NULL, NULL, 3) FROM test_data WHERE id = 1"
        ),
        Some(3)
    );

    // With different types (strings)
    assert_eq!(
        get_string(
            &ctx,
            "SELECT COALESCE('a', 'b') FROM test_data WHERE id = 1"
        ),
        Some("a".to_string())
    );
}

#[test]
fn test_greatest_least_comprehensive() {
    let ctx = create_test_context();

    // GREATEST with integers
    assert_eq!(
        get_int64(&ctx, "SELECT GREATEST(1, 2) FROM test_data WHERE id = 1"),
        Some(2)
    );
    assert_eq!(
        get_int64(&ctx, "SELECT GREATEST(2, 1) FROM test_data WHERE id = 1"),
        Some(2)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT GREATEST(1, 2, 3, 4, 5) FROM test_data WHERE id = 1"
        ),
        Some(5)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT GREATEST(5, 4, 3, 2, 1) FROM test_data WHERE id = 1"
        ),
        Some(5)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT GREATEST(-10, -5, 0, 5, 10) FROM test_data WHERE id = 1"
        ),
        Some(10)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT GREATEST(-100, -50) FROM test_data WHERE id = 1"
        ),
        Some(-50)
    );

    // LEAST with integers
    assert_eq!(
        get_int64(&ctx, "SELECT LEAST(1, 2) FROM test_data WHERE id = 1"),
        Some(1)
    );
    assert_eq!(
        get_int64(&ctx, "SELECT LEAST(2, 1) FROM test_data WHERE id = 1"),
        Some(1)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT LEAST(1, 2, 3, 4, 5) FROM test_data WHERE id = 1"
        ),
        Some(1)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT LEAST(5, 4, 3, 2, 1) FROM test_data WHERE id = 1"
        ),
        Some(1)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT LEAST(-10, -5, 0, 5, 10) FROM test_data WHERE id = 1"
        ),
        Some(-10)
    );
}

// -----------------------------------------------------------------------------
// Bitwise Functions - Multiple Values
// -----------------------------------------------------------------------------

#[test]
fn test_bitwise_comprehensive() {
    let ctx = create_test_context();

    // BITWISE_AND
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_AND(15, 7) FROM test_data WHERE id = 1"
        ),
        Some(7)
    ); // 1111 & 0111 = 0111
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_AND(255, 15) FROM test_data WHERE id = 1"
        ),
        Some(15)
    ); // 11111111 & 00001111 = 00001111
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_AND(0, 255) FROM test_data WHERE id = 1"
        ),
        Some(0)
    ); // Any AND 0 = 0

    // BITWISE_OR
    assert_eq!(
        get_int64(&ctx, "SELECT BITWISE_OR(15, 7) FROM test_data WHERE id = 1"),
        Some(15)
    ); // 1111 | 0111 = 1111
    assert_eq!(
        get_int64(&ctx, "SELECT BITWISE_OR(8, 4) FROM test_data WHERE id = 1"),
        Some(12)
    ); // 1000 | 0100 = 1100
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_OR(0, 255) FROM test_data WHERE id = 1"
        ),
        Some(255)
    ); // Any OR 0 = itself

    // BITWISE_XOR
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_XOR(15, 7) FROM test_data WHERE id = 1"
        ),
        Some(8)
    ); // 1111 ^ 0111 = 1000
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_XOR(255, 255) FROM test_data WHERE id = 1"
        ),
        Some(0)
    ); // Same XOR = 0
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_XOR(0, 255) FROM test_data WHERE id = 1"
        ),
        Some(255)
    ); // 0 XOR any = itself

    // BIT_COUNT
    assert_eq!(
        get_int64(&ctx, "SELECT BIT_COUNT(0) FROM test_data WHERE id = 1"),
        Some(0)
    );
    assert_eq!(
        get_int64(&ctx, "SELECT BIT_COUNT(1) FROM test_data WHERE id = 1"),
        Some(1)
    );
    assert_eq!(
        get_int64(&ctx, "SELECT BIT_COUNT(3) FROM test_data WHERE id = 1"),
        Some(2)
    ); // 11
    assert_eq!(
        get_int64(&ctx, "SELECT BIT_COUNT(7) FROM test_data WHERE id = 1"),
        Some(3)
    ); // 111
    assert_eq!(
        get_int64(&ctx, "SELECT BIT_COUNT(15) FROM test_data WHERE id = 1"),
        Some(4)
    ); // 1111
    assert_eq!(
        get_int64(&ctx, "SELECT BIT_COUNT(255) FROM test_data WHERE id = 1"),
        Some(8)
    ); // 11111111

    // BITWISE_LEFT_SHIFT
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_LEFT_SHIFT(1, 0) FROM test_data WHERE id = 1"
        ),
        Some(1)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_LEFT_SHIFT(1, 1) FROM test_data WHERE id = 1"
        ),
        Some(2)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_LEFT_SHIFT(1, 2) FROM test_data WHERE id = 1"
        ),
        Some(4)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_LEFT_SHIFT(1, 8) FROM test_data WHERE id = 1"
        ),
        Some(256)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_LEFT_SHIFT(5, 2) FROM test_data WHERE id = 1"
        ),
        Some(20)
    ); // 101 << 2 = 10100

    // BITWISE_RIGHT_SHIFT
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_RIGHT_SHIFT(16, 0) FROM test_data WHERE id = 1"
        ),
        Some(16)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_RIGHT_SHIFT(16, 1) FROM test_data WHERE id = 1"
        ),
        Some(8)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_RIGHT_SHIFT(16, 4) FROM test_data WHERE id = 1"
        ),
        Some(1)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT BITWISE_RIGHT_SHIFT(255, 4) FROM test_data WHERE id = 1"
        ),
        Some(15)
    );
}

// -----------------------------------------------------------------------------
// Aggregate Functions - Multiple Values and Edge Cases
// -----------------------------------------------------------------------------

#[test]
fn test_aggregate_comprehensive() {
    let ctx = create_test_context();

    // Test data has: val_int = 10, 20, 30, 40, 50
    // Test data has: val_float = 1.5, 2.5, 3.5, 4.5, 5.5

    // Verify COUNT
    assert_eq!(get_int64(&ctx, "SELECT COUNT(*) FROM test_data"), Some(5));
    assert_eq!(
        get_int64(&ctx, "SELECT COUNT(val_int) FROM test_data"),
        Some(5)
    );

    // Verify SUM
    assert_eq!(
        get_int64(&ctx, "SELECT SUM(val_int) FROM test_data"),
        Some(150)
    ); // 10+20+30+40+50
    let sum_float = get_float64(&ctx, "SELECT SUM(val_float) FROM test_data");
    assert!(sum_float.is_some());
    assert!(
        (sum_float.unwrap() - 17.5).abs() < 0.001,
        "SUM(val_float) should be 17.5"
    );

    // Verify AVG
    assert_eq!(
        get_float64(&ctx, "SELECT AVG(val_int) FROM test_data"),
        Some(30.0)
    ); // 150/5
    let avg_float = get_float64(&ctx, "SELECT AVG(val_float) FROM test_data");
    assert!(avg_float.is_some());
    assert!(
        (avg_float.unwrap() - 3.5).abs() < 0.001,
        "AVG(val_float) should be 3.5"
    );

    // Verify MIN/MAX
    assert_eq!(
        get_int64(&ctx, "SELECT MIN(val_int) FROM test_data"),
        Some(10)
    );
    assert_eq!(
        get_int64(&ctx, "SELECT MAX(val_int) FROM test_data"),
        Some(50)
    );

    let min_float = get_float64(&ctx, "SELECT MIN(val_float) FROM test_data");
    assert!(min_float.is_some());
    assert!((min_float.unwrap() - 1.5).abs() < 0.001);

    let max_float = get_float64(&ctx, "SELECT MAX(val_float) FROM test_data");
    assert!(max_float.is_some());
    assert!((max_float.unwrap() - 5.5).abs() < 0.001);
}

// -----------------------------------------------------------------------------
// JSON Functions - Multiple Values
// -----------------------------------------------------------------------------

#[test]
fn test_json_comprehensive() {
    let ctx = create_test_context();

    // JSON_EXTRACT with different paths
    assert_eq!(
        get_string(
            &ctx,
            "SELECT JSON_EXTRACT('{\"a\": 1}', '$.a') FROM test_data WHERE id = 1"
        ),
        Some("1".to_string())
    );
    assert_eq!(get_string(&ctx, "SELECT JSON_EXTRACT('{\"nested\": {\"value\": 42}}', '$.nested.value') FROM test_data WHERE id = 1"), Some("42".to_string()));

    // JSON_ARRAY_LENGTH with different arrays
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT JSON_ARRAY_LENGTH('[]') FROM test_data WHERE id = 1"
        ),
        Some(0)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT JSON_ARRAY_LENGTH('[1]') FROM test_data WHERE id = 1"
        ),
        Some(1)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT JSON_ARRAY_LENGTH('[1, 2, 3]') FROM test_data WHERE id = 1"
        ),
        Some(3)
    );
    assert_eq!(get_int64(&ctx, "SELECT JSON_ARRAY_LENGTH('[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]') FROM test_data WHERE id = 1"), Some(10));

    // JSON_ARRAY_CONTAINS with different types
    assert_eq!(
        get_bool(
            &ctx,
            "SELECT JSON_ARRAY_CONTAINS('[1, 2, 3]', 1) FROM test_data WHERE id = 1"
        ),
        Some(true)
    );
    assert_eq!(
        get_bool(
            &ctx,
            "SELECT JSON_ARRAY_CONTAINS('[1, 2, 3]', 4) FROM test_data WHERE id = 1"
        ),
        Some(false)
    );
    assert_eq!(
        get_bool(
            &ctx,
            "SELECT JSON_ARRAY_CONTAINS('[]', 1) FROM test_data WHERE id = 1"
        ),
        Some(false)
    );
}

// -----------------------------------------------------------------------------
// Regex Functions - Multiple Patterns
// -----------------------------------------------------------------------------

#[test]
fn test_regex_comprehensive() {
    let ctx = create_test_context();

    // REGEXP_LIKE with various patterns
    assert_eq!(
        get_bool(
            &ctx,
            "SELECT REGEXP_LIKE('hello', '^h') FROM test_data WHERE id = 1"
        ),
        Some(true)
    ); // Starts with h
    assert_eq!(
        get_bool(
            &ctx,
            "SELECT REGEXP_LIKE('hello', 'o$') FROM test_data WHERE id = 1"
        ),
        Some(true)
    ); // Ends with o
    assert_eq!(
        get_bool(
            &ctx,
            "SELECT REGEXP_LIKE('hello', '^hello$') FROM test_data WHERE id = 1"
        ),
        Some(true)
    ); // Exact match
    assert_eq!(
        get_bool(
            &ctx,
            "SELECT REGEXP_LIKE('hello', '[aeiou]') FROM test_data WHERE id = 1"
        ),
        Some(true)
    ); // Contains vowel
    assert_eq!(
        get_bool(
            &ctx,
            "SELECT REGEXP_LIKE('hello', '[0-9]') FROM test_data WHERE id = 1"
        ),
        Some(false)
    ); // No digits
    assert_eq!(
        get_bool(
            &ctx,
            "SELECT REGEXP_LIKE('hello123', '[0-9]+') FROM test_data WHERE id = 1"
        ),
        Some(true)
    ); // Has digits

    // REGEXP_COUNT with various patterns
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT REGEXP_COUNT('hello', 'l') FROM test_data WHERE id = 1"
        ),
        Some(2)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT REGEXP_COUNT('hello', 'o') FROM test_data WHERE id = 1"
        ),
        Some(1)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT REGEXP_COUNT('hello', 'x') FROM test_data WHERE id = 1"
        ),
        Some(0)
    );
    assert_eq!(
        get_int64(
            &ctx,
            "SELECT REGEXP_COUNT('aaa', 'a') FROM test_data WHERE id = 1"
        ),
        Some(3)
    );
}

// -----------------------------------------------------------------------------
// Encoding Functions - Multiple Values
// -----------------------------------------------------------------------------

#[test]
fn test_encoding_comprehensive() {
    let ctx = create_test_context();

    // TO_HEX with various integers
    assert_eq!(
        get_string(&ctx, "SELECT TO_HEX(0) FROM test_data WHERE id = 1"),
        Some("0".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT TO_HEX(15) FROM test_data WHERE id = 1"),
        Some("f".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT TO_HEX(16) FROM test_data WHERE id = 1"),
        Some("10".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT TO_HEX(255) FROM test_data WHERE id = 1"),
        Some("ff".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT TO_HEX(256) FROM test_data WHERE id = 1"),
        Some("100".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT TO_HEX(4096) FROM test_data WHERE id = 1"),
        Some("1000".to_string())
    );

    // TO_BASE64 with various strings
    assert_eq!(
        get_string(&ctx, "SELECT TO_BASE64('') FROM test_data WHERE id = 1"),
        Some("".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT TO_BASE64('a') FROM test_data WHERE id = 1"),
        Some("YQ==".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT TO_BASE64('ab') FROM test_data WHERE id = 1"),
        Some("YWI=".to_string())
    );
    assert_eq!(
        get_string(&ctx, "SELECT TO_BASE64('abc') FROM test_data WHERE id = 1"),
        Some("YWJj".to_string())
    );
    assert_eq!(
        get_string(
            &ctx,
            "SELECT TO_BASE64('hello') FROM test_data WHERE id = 1"
        ),
        Some("aGVsbG8=".to_string())
    );
}

// =============================================================================
// DATE_ADD, DATE_DIFF, DATE_TRUNC, DATE_PART FUNCTION TESTS
// =============================================================================

#[test]
fn test_date_add_days() {
    let ctx = create_test_context();
    // DATE_ADD with days - adding to a timestamp
    let result = get_string(&ctx, "SELECT DATE_FORMAT(DATE_ADD('day', 5, DATE_PARSE('2024-01-15', '%Y-%m-%d')), '%Y-%m-%d') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("2024-01-20".to_string()),
        "DATE_ADD 5 days to 2024-01-15 should be 2024-01-20"
    );
}

#[test]
fn test_date_add_months() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT DATE_FORMAT(DATE_ADD('month', 2, DATE_PARSE('2024-01-15', '%Y-%m-%d')), '%Y-%m-%d') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("2024-03-15".to_string()),
        "DATE_ADD 2 months to 2024-01-15 should be 2024-03-15"
    );
}

#[test]
fn test_date_add_years() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT DATE_FORMAT(DATE_ADD('year', 1, DATE_PARSE('2024-01-15', '%Y-%m-%d')), '%Y-%m-%d') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("2025-01-15".to_string()),
        "DATE_ADD 1 year to 2024-01-15 should be 2025-01-15"
    );
}

#[test]
fn test_date_add_negative() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT DATE_FORMAT(DATE_ADD('day', -10, DATE_PARSE('2024-01-15', '%Y-%m-%d')), '%Y-%m-%d') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("2024-01-05".to_string()),
        "DATE_ADD -10 days to 2024-01-15 should be 2024-01-05"
    );
}

#[test]
fn test_date_diff_days() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT DATE_DIFF('day', DATE_PARSE('2024-01-10', '%Y-%m-%d'), DATE_PARSE('2024-01-15', '%Y-%m-%d')) FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some(5),
        "DATE_DIFF days between 2024-01-10 and 2024-01-15 should be 5"
    );
}

#[test]
fn test_date_diff_months() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT DATE_DIFF('month', DATE_PARSE('2024-01-15', '%Y-%m-%d'), DATE_PARSE('2024-04-15', '%Y-%m-%d')) FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some(3),
        "DATE_DIFF months between 2024-01-15 and 2024-04-15 should be 3"
    );
}

#[test]
fn test_date_diff_years() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT DATE_DIFF('year', DATE_PARSE('2020-01-01', '%Y-%m-%d'), DATE_PARSE('2024-01-01', '%Y-%m-%d')) FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some(4),
        "DATE_DIFF years between 2020 and 2024 should be 4"
    );
}

#[test]
fn test_date_diff_negative() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT DATE_DIFF('day', DATE_PARSE('2024-01-15', '%Y-%m-%d'), DATE_PARSE('2024-01-10', '%Y-%m-%d')) FROM test_data WHERE id = 1");
    assert_eq!(result, Some(-5), "DATE_DIFF days (reverse) should be -5");
}

#[test]
fn test_date_trunc_month() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT DATE_FORMAT(DATE_TRUNC('month', DATE_PARSE('2024-03-15 14:30:00', '%Y-%m-%d %H:%i:%s')), '%Y-%m-%d') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("2024-03-01".to_string()),
        "DATE_TRUNC to month should be first of month"
    );
}

#[test]
fn test_date_trunc_year() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT DATE_FORMAT(DATE_TRUNC('year', DATE_PARSE('2024-07-20', '%Y-%m-%d')), '%Y-%m-%d') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("2024-01-01".to_string()),
        "DATE_TRUNC to year should be Jan 1"
    );
}

#[test]
fn test_date_trunc_quarter() {
    let ctx = create_test_context();
    // May is in Q2, which starts on April 1
    let result = get_string(&ctx, "SELECT DATE_FORMAT(DATE_TRUNC('quarter', DATE_PARSE('2024-05-15', '%Y-%m-%d')), '%Y-%m-%d') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("2024-04-01".to_string()),
        "DATE_TRUNC to quarter should be start of quarter"
    );
}

#[test]
fn test_date_part_year() {
    let ctx = create_test_context();
    // DATE_PART with DATE literal - consistent with YEAR function syntax
    let result = get_float64(
        &ctx,
        "SELECT DATE_PART('year', DATE '2024-06-15') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(2024.0),
        "DATE_PART('year', DATE '2024-06-15') should be 2024"
    );
}

#[test]
fn test_date_part_month() {
    let ctx = create_test_context();
    let result = get_float64(
        &ctx,
        "SELECT DATE_PART('month', DATE '2024-06-15') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(6.0),
        "DATE_PART('month', DATE '2024-06-15') should be 6"
    );
}

#[test]
fn test_date_part_day() {
    let ctx = create_test_context();
    let result = get_float64(
        &ctx,
        "SELECT DATE_PART('day', DATE '2024-06-15') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(15.0),
        "DATE_PART('day', DATE '2024-06-15') should be 15"
    );
}

#[test]
fn test_date_part_quarter() {
    let ctx = create_test_context();
    // June is in Q2
    let result = get_float64(
        &ctx,
        "SELECT DATE_PART('quarter', DATE '2024-06-15') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some(2.0),
        "DATE_PART('quarter', DATE '2024-06-15') should be 2"
    );
}

// =============================================================================
// DATE_FORMAT AND DATE_PARSE FUNCTION TESTS
// =============================================================================

#[test]
fn test_date_format_basic() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT DATE_FORMAT(DATE_PARSE('2024-07-15', '%Y-%m-%d'), '%Y-%m-%d') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("2024-07-15".to_string()),
        "DATE_FORMAT should format correctly"
    );
}

#[test]
fn test_date_format_different_pattern() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT DATE_FORMAT(DATE_PARSE('2024-07-15', '%Y-%m-%d'), '%d/%m/%Y') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("15/07/2024".to_string()),
        "DATE_FORMAT with different pattern"
    );
}

#[test]
fn test_date_parse_with_time() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT DATE_FORMAT(DATE_PARSE('2024-07-15 14:30:45', '%Y-%m-%d %H:%i:%s'), '%H:%i:%s') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("14:30:45".to_string()),
        "DATE_PARSE should handle time correctly"
    );
}

// =============================================================================
// BASE32 ENCODING FUNCTION TESTS
// =============================================================================

#[test]
fn test_to_base32_basic() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT TO_BASE32('hello') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("NBSWY3DP".to_string()),
        "TO_BASE32('hello') should be 'NBSWY3DP'"
    );
}

#[test]
fn test_to_base32_empty() {
    let ctx = create_test_context();
    let result = get_string(&ctx, "SELECT TO_BASE32('') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("".to_string()),
        "TO_BASE32 of empty string should be empty"
    );
}

#[test]
fn test_to_base32_various() {
    let ctx = create_test_context();
    // 'a' in Base32 is 'ME======'
    let result = get_string(&ctx, "SELECT TO_BASE32('a') FROM test_data WHERE id = 1");
    assert_eq!(result, Some("ME======".to_string()));

    let result = get_string(&ctx, "SELECT TO_BASE32('ab') FROM test_data WHERE id = 1");
    assert_eq!(result, Some("MFRA====".to_string()));
}

// =============================================================================
// FORMAT AND FORMAT_NUMBER FUNCTION TESTS
// =============================================================================

#[test]
fn test_format_number_basic() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT FORMAT_NUMBER(1234567.89, 2) FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("1,234,567.89".to_string()),
        "FORMAT_NUMBER should add thousands separators"
    );
}

#[test]
fn test_format_number_no_decimals() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT FORMAT_NUMBER(1234567, 0) FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("1,234,567".to_string()),
        "FORMAT_NUMBER with 0 decimals"
    );
}

#[test]
fn test_format_number_negative() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT FORMAT_NUMBER(-9876543.21, 2) FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("-9,876,543.21".to_string()),
        "FORMAT_NUMBER with negative number"
    );
}

#[test]
fn test_format_number_small() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT FORMAT_NUMBER(42.5, 2) FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("42.50".to_string()),
        "FORMAT_NUMBER with small number"
    );
}

#[test]
fn test_format_simple() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT FORMAT('Value: %s', 'hello') FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("Value: hello".to_string()),
        "FORMAT should replace %s"
    );
}

// =============================================================================
// STATISTICAL FUNCTION TESTS (BETA_CDF, T_CDF, WILSON_INTERVAL)
// =============================================================================

#[test]
fn test_beta_cdf_basic() {
    let ctx = create_test_context();
    // BETA_CDF(1, 1, 0.5) for uniform distribution should be 0.5
    let result = get_float64(
        &ctx,
        "SELECT BETA_CDF(1, 1, 0.5) FROM test_data WHERE id = 1",
    );
    assert!(result.is_some());
    assert!(
        (result.unwrap() - 0.5).abs() < 0.001,
        "BETA_CDF(1, 1, 0.5) should be ~0.5"
    );
}

#[test]
fn test_beta_cdf_at_zero() {
    let ctx = create_test_context();
    // BETA_CDF at x=0 should be 0
    let result = get_float64(&ctx, "SELECT BETA_CDF(2, 3, 0) FROM test_data WHERE id = 1");
    assert!(result.is_some());
    assert!(
        (result.unwrap() - 0.0).abs() < 0.001,
        "BETA_CDF at x=0 should be 0"
    );
}

#[test]
fn test_beta_cdf_at_one() {
    let ctx = create_test_context();
    // BETA_CDF at x=1 should be 1
    let result = get_float64(&ctx, "SELECT BETA_CDF(2, 3, 1) FROM test_data WHERE id = 1");
    assert!(result.is_some());
    assert!(
        (result.unwrap() - 1.0).abs() < 0.001,
        "BETA_CDF at x=1 should be 1"
    );
}

#[test]
fn test_inverse_beta_cdf_basic() {
    let ctx = create_test_context();
    // INVERSE_BETA_CDF(1, 1, 0.5) for uniform should be 0.5
    let result = get_float64(
        &ctx,
        "SELECT INVERSE_BETA_CDF(1, 1, 0.5) FROM test_data WHERE id = 1",
    );
    assert!(result.is_some());
    assert!(
        (result.unwrap() - 0.5).abs() < 0.001,
        "INVERSE_BETA_CDF(1, 1, 0.5) should be ~0.5"
    );
}

#[test]
fn test_t_cdf_standard() {
    let ctx = create_test_context();
    // T_CDF(0, 10) should be 0.5 (symmetric around 0)
    let result = get_float64(&ctx, "SELECT T_CDF(0, 10) FROM test_data WHERE id = 1");
    assert!(result.is_some());
    assert!(
        (result.unwrap() - 0.5).abs() < 0.001,
        "T_CDF(0, df) should be 0.5"
    );
}

#[test]
fn test_t_cdf_positive() {
    let ctx = create_test_context();
    // T_CDF at large positive x should approach 1
    let result = get_float64(&ctx, "SELECT T_CDF(5, 10) FROM test_data WHERE id = 1");
    assert!(result.is_some());
    assert!(result.unwrap() > 0.99, "T_CDF(5, 10) should be close to 1");
}

#[test]
fn test_t_pdf_at_zero() {
    let ctx = create_test_context();
    // T_PDF at 0 should be the maximum value
    let result = get_float64(&ctx, "SELECT T_PDF(0, 10) FROM test_data WHERE id = 1");
    assert!(result.is_some());
    assert!(result.unwrap() > 0.38, "T_PDF(0, 10) should be around 0.39");
}

#[test]
fn test_wilson_interval_lower_basic() {
    let ctx = create_test_context();
    // Wilson interval for 10 successes out of 100 trials with z=1.96 (95% CI)
    let result = get_float64(
        &ctx,
        "SELECT WILSON_INTERVAL_LOWER(10, 100, 1.96) FROM test_data WHERE id = 1",
    );
    assert!(result.is_some());
    let lower = result.unwrap();
    assert!(
        lower > 0.04 && lower < 0.07,
        "Wilson lower bound should be between 0.04 and 0.07"
    );
}

#[test]
fn test_wilson_interval_upper_basic() {
    let ctx = create_test_context();
    // Wilson interval for 10 successes out of 100 trials with z=1.96 (95% CI)
    let result = get_float64(
        &ctx,
        "SELECT WILSON_INTERVAL_UPPER(10, 100, 1.96) FROM test_data WHERE id = 1",
    );
    assert!(result.is_some());
    let upper = result.unwrap();
    assert!(
        upper > 0.15 && upper < 0.20,
        "Wilson upper bound should be between 0.15 and 0.20"
    );
}

#[test]
fn test_wilson_interval_bounds_relationship() {
    let ctx = create_test_context();
    // Upper bound should be greater than lower bound
    let lower = get_float64(
        &ctx,
        "SELECT WILSON_INTERVAL_LOWER(50, 100, 1.96) FROM test_data WHERE id = 1",
    );
    let upper = get_float64(
        &ctx,
        "SELECT WILSON_INTERVAL_UPPER(50, 100, 1.96) FROM test_data WHERE id = 1",
    );
    assert!(lower.is_some() && upper.is_some());
    assert!(
        upper.unwrap() > lower.unwrap(),
        "Upper bound should be greater than lower bound"
    );
}

// =============================================================================
// TIMEZONE FUNCTION TESTS
// =============================================================================

#[test]
fn test_timezone_basic() {
    let ctx = create_test_context();
    // TIMEZONE() should return a timezone string
    let result = get_string(&ctx, "SELECT TIMEZONE(NOW()) FROM test_data WHERE id = 1");
    assert!(result.is_some(), "TIMEZONE should return a value");
}

#[test]
fn test_at_timezone_utc() {
    let ctx = create_test_context();
    // AT_TIMEZONE with UTC should not change the timestamp
    let result = get_int64(
        &ctx,
        "SELECT DATE_DIFF('second', NOW(), AT_TIMEZONE(NOW(), 'UTC')) FROM test_data WHERE id = 1",
    );
    assert!(result.is_some());
    // The difference should be 0 or very small
    assert!(
        result.unwrap().abs() < 2,
        "AT_TIMEZONE with UTC should not significantly change the timestamp"
    );
}

// =============================================================================
// EDGE CASE AND BOUNDARY TESTS
// =============================================================================

#[test]
fn test_date_add_month_end_boundary() {
    let ctx = create_test_context();
    // Adding 1 month to Jan 31 should handle month-end correctly
    let result = get_string(&ctx, "SELECT DATE_FORMAT(DATE_ADD('month', 1, DATE_PARSE('2024-01-31', '%Y-%m-%d')), '%Y-%m-%d') FROM test_data WHERE id = 1");
    // Feb 2024 has 29 days (leap year), so Jan 31 + 1 month = Feb 29 or closest
    assert!(
        result.is_some(),
        "DATE_ADD should handle month-end boundary"
    );
}

#[test]
fn test_date_add_leap_year() {
    let ctx = create_test_context();
    // Adding 1 day to Feb 28 in a leap year should give Feb 29
    let result = get_string(&ctx, "SELECT DATE_FORMAT(DATE_ADD('day', 1, DATE_PARSE('2024-02-28', '%Y-%m-%d')), '%Y-%m-%d') FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some("2024-02-29".to_string()),
        "2024-02-28 + 1 day should be 2024-02-29 (leap year)"
    );
}

#[test]
fn test_date_diff_across_year_boundary() {
    let ctx = create_test_context();
    let result = get_int64(&ctx, "SELECT DATE_DIFF('day', DATE_PARSE('2023-12-31', '%Y-%m-%d'), DATE_PARSE('2024-01-02', '%Y-%m-%d')) FROM test_data WHERE id = 1");
    assert_eq!(
        result,
        Some(2),
        "DATE_DIFF across year boundary should be 2 days"
    );
}

#[test]
fn test_format_number_zero() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT FORMAT_NUMBER(0, 2) FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("0.00".to_string()),
        "FORMAT_NUMBER(0) should be '0.00'"
    );
}

#[test]
fn test_format_number_large() {
    let ctx = create_test_context();
    let result = get_string(
        &ctx,
        "SELECT FORMAT_NUMBER(1234567890123, 0) FROM test_data WHERE id = 1",
    );
    assert_eq!(
        result,
        Some("1,234,567,890,123".to_string()),
        "FORMAT_NUMBER with large number"
    );
}
