//! Trino SQL Function Test Harness
//!
//! This file contains test definitions for all Trino SQL functions.
//! Remove any functions you don't need before implementing.
//!
//! Status Legend:
//! - ✅ SUPPORTED: Function is implemented and tested
//! - ❌ NOT_SUPPORTED: Function is not yet implemented
//! - 🔧 PARTIAL: Function is partially implemented

use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

// =============================================================================
// TEST INFRASTRUCTURE
// =============================================================================

/// Create a test context with sample data for function testing
fn create_function_test_context() -> ExecutionContext {
    let mut ctx = ExecutionContext::new();

    // Register a simple test_data table with various columns for testing functions
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val_int", DataType::Int64, true),
        Field::new("val_float", DataType::Float64, true),
        Field::new("val_str", DataType::Utf8, true),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            Arc::new(Int64Array::from(vec![
                Some(-5),
                Some(10),
                Some(0),
                None,
                Some(100),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(std::f64::consts::PI),
                Some(-2.5),
                Some(0.0),
                None,
                Some(100.99),
            ])),
            Arc::new(StringArray::from(vec![
                Some("hello"),
                Some("WORLD"),
                Some("  trim me  "),
                None,
                Some("test@example.com"),
            ])),
        ],
    )
    .unwrap();

    ctx.register_table("test_data", schema, vec![batch]);
    ctx
}

/// Helper macro for testing scalar functions with FROM clause
macro_rules! test_scalar_function {
    ($name:ident, $sql:expr) => {
        #[tokio::test]
        async fn $name() {
            let ctx = create_function_test_context();
            let result = ctx.sql($sql).await;
            match result {
                Ok(r) => {
                    assert!(r.row_count > 0, "Expected results for: {}", $sql);
                }
                Err(e) => panic!("Query failed: {} - Error: {:?}", $sql, e),
            }
        }
    };
}

#[allow(unused_macros)]
/// Macro for documenting unsupported functions (tests that should fail gracefully)
macro_rules! test_unsupported_function {
    ($name:ident, $sql:expr, $func_name:expr) => {
        #[tokio::test]
        #[ignore = "Function not yet implemented"]
        async fn $name() {
            let ctx = create_function_test_context();
            let result = ctx.sql($sql).await;
            // Should return NotImplemented error
            assert!(
                result.is_err(),
                "{} should not be implemented yet",
                $func_name
            );
        }
    };
}

// =============================================================================
// MATH FUNCTIONS
// =============================================================================

mod math_functions {
    use super::*;

    // ✅ SUPPORTED - Basic Math
    test_scalar_function!(test_abs, "SELECT ABS(val_int) FROM test_data");
    test_scalar_function!(test_ceil, "SELECT CEIL(val_float) FROM test_data");
    test_scalar_function!(test_ceiling, "SELECT CEILING(val_float) FROM test_data");
    test_scalar_function!(test_floor, "SELECT FLOOR(val_float) FROM test_data");
    test_scalar_function!(test_round, "SELECT ROUND(val_float) FROM test_data");
    test_scalar_function!(
        test_power,
        "SELECT POWER(val_int, 2) FROM test_data WHERE val_int > 0"
    );
    test_scalar_function!(test_sqrt, "SELECT SQRT(ABS(val_int)) FROM test_data");

    // ✅ SUPPORTED - New Math Functions
    test_scalar_function!(
        test_mod,
        "SELECT MOD(val_int, 3) FROM test_data WHERE val_int IS NOT NULL"
    );
    test_scalar_function!(test_sign, "SELECT SIGN(val_int) FROM test_data");
    test_scalar_function!(test_truncate, "SELECT TRUNCATE(val_float) FROM test_data");
    test_scalar_function!(test_ln, "SELECT LN(ABS(val_float) + 1) FROM test_data");
    test_scalar_function!(test_log, "SELECT LOG(ABS(val_float) + 1) FROM test_data");
    test_scalar_function!(test_log2, "SELECT LOG2(ABS(val_int) + 1) FROM test_data");
    test_scalar_function!(test_log10, "SELECT LOG10(ABS(val_int) + 1) FROM test_data");
    test_scalar_function!(test_exp, "SELECT EXP(1) FROM test_data WHERE id = 1");
    test_scalar_function!(test_random, "SELECT RANDOM() FROM test_data WHERE id = 1");
    test_scalar_function!(test_sin, "SELECT SIN(val_float) FROM test_data");
    test_scalar_function!(test_cos, "SELECT COS(val_float) FROM test_data");
    test_scalar_function!(test_tan, "SELECT TAN(val_float) FROM test_data");
    test_scalar_function!(
        test_asin,
        "SELECT ASIN(val_float / 100) FROM test_data WHERE val_float IS NOT NULL"
    );
    test_scalar_function!(
        test_acos,
        "SELECT ACOS(val_float / 100) FROM test_data WHERE val_float IS NOT NULL"
    );
    test_scalar_function!(test_atan, "SELECT ATAN(val_float) FROM test_data");
    test_scalar_function!(test_atan2, "SELECT ATAN2(val_float, 1) FROM test_data");
    test_scalar_function!(test_degrees, "SELECT DEGREES(val_float) FROM test_data");
    test_scalar_function!(test_radians, "SELECT RADIANS(val_float) FROM test_data");
    test_scalar_function!(test_pi, "SELECT PI() FROM test_data WHERE id = 1");
    test_scalar_function!(test_e, "SELECT E() FROM test_data WHERE id = 1");
    test_scalar_function!(test_cbrt, "SELECT CBRT(val_int) FROM test_data");
}

// =============================================================================
// STRING FUNCTIONS
// =============================================================================

mod string_functions {
    use super::*;

    // ✅ SUPPORTED - Basic String Functions
    test_scalar_function!(test_upper, "SELECT UPPER(val_str) FROM test_data");
    test_scalar_function!(test_lower, "SELECT LOWER(val_str) FROM test_data");
    test_scalar_function!(test_trim, "SELECT TRIM(val_str) FROM test_data");
    test_scalar_function!(test_ltrim, "SELECT LTRIM(val_str) FROM test_data");
    test_scalar_function!(test_rtrim, "SELECT RTRIM(val_str) FROM test_data");
    test_scalar_function!(test_length, "SELECT LENGTH(val_str) FROM test_data");
    test_scalar_function!(
        test_substring,
        "SELECT SUBSTRING(val_str, 1, 3) FROM test_data"
    );
    test_scalar_function!(test_concat, "SELECT CONCAT(val_str, '!') FROM test_data");
    test_scalar_function!(
        test_replace,
        "SELECT REPLACE(val_str, 'e', 'E') FROM test_data"
    );

    // ✅ SUPPORTED - New String Functions
    test_scalar_function!(
        test_position,
        "SELECT POSITION('l' IN val_str) FROM test_data WHERE val_str = 'hello'"
    );
    test_scalar_function!(test_strpos, "SELECT STRPOS(val_str, 'l') FROM test_data");
    test_scalar_function!(test_reverse, "SELECT REVERSE(val_str) FROM test_data");
    test_scalar_function!(test_lpad, "SELECT LPAD(val_str, 10, '*') FROM test_data");
    test_scalar_function!(test_rpad, "SELECT RPAD(val_str, 10, '*') FROM test_data");
    test_scalar_function!(
        test_split_part,
        "SELECT SPLIT_PART(val_str, '@', 1) FROM test_data WHERE val_str LIKE '%@%'"
    );
    test_scalar_function!(
        test_starts_with,
        "SELECT STARTS_WITH(val_str, 'he') FROM test_data"
    );
    test_scalar_function!(
        test_ends_with,
        "SELECT ENDS_WITH(val_str, 'lo') FROM test_data"
    );
    test_scalar_function!(test_chr, "SELECT CHR(65) FROM test_data WHERE id = 1");
    test_scalar_function!(test_ascii, "SELECT ASCII(val_str) FROM test_data");
    test_scalar_function!(
        test_concat_ws,
        "SELECT CONCAT_WS('-', val_str, 'suffix') FROM test_data"
    );
    test_scalar_function!(test_left, "SELECT LEFT(val_str, 3) FROM test_data");
    test_scalar_function!(test_right, "SELECT RIGHT(val_str, 3) FROM test_data");
    test_scalar_function!(
        test_repeat,
        "SELECT REPEAT(val_str, 2) FROM test_data WHERE id = 1"
    );
}

// =============================================================================
// DATE/TIME FUNCTIONS
// =============================================================================

mod datetime_functions {
    use super::*;

    // ✅ SUPPORTED - Basic Date/Time
    test_scalar_function!(
        test_current_date,
        "SELECT CURRENT_DATE() FROM test_data WHERE id = 1"
    );
    test_scalar_function!(
        test_current_timestamp,
        "SELECT CURRENT_TIMESTAMP() FROM test_data WHERE id = 1"
    );
    test_scalar_function!(test_now, "SELECT NOW() FROM test_data WHERE id = 1");
}

// =============================================================================
// CONDITIONAL FUNCTIONS
// =============================================================================

mod conditional_functions {
    use super::*;

    // ✅ SUPPORTED
    test_scalar_function!(test_coalesce, "SELECT COALESCE(val_int, 0) FROM test_data");
    test_scalar_function!(test_nullif, "SELECT NULLIF(val_int, 0) FROM test_data");
    test_scalar_function!(
        test_case,
        "SELECT CASE WHEN val_int > 0 THEN 'positive' ELSE 'non-positive' END FROM test_data"
    );
    test_scalar_function!(
        test_if,
        "SELECT IF(val_int > 0, 1, 0) FROM test_data WHERE val_int IS NOT NULL"
    );
    test_scalar_function!(
        test_greatest,
        "SELECT GREATEST(val_int, 0) FROM test_data WHERE val_int IS NOT NULL"
    );
    test_scalar_function!(
        test_least,
        "SELECT LEAST(val_int, 50) FROM test_data WHERE val_int IS NOT NULL"
    );
}

// =============================================================================
// REGEX FUNCTIONS
// =============================================================================

mod regex_functions {
    use super::*;

    // ✅ SUPPORTED
    test_scalar_function!(
        test_regexp_like,
        "SELECT REGEXP_LIKE(val_str, 'e') FROM test_data"
    );
    test_scalar_function!(
        test_regexp_extract,
        "SELECT REGEXP_EXTRACT(val_str, '([a-z]+)', 1) FROM test_data"
    );
    test_scalar_function!(
        test_regexp_replace,
        "SELECT REGEXP_REPLACE(val_str, '[aeiou]', '*') FROM test_data"
    );
    test_scalar_function!(
        test_regexp_count,
        "SELECT REGEXP_COUNT(val_str, 'l') FROM test_data"
    );
}

// =============================================================================
// BINARY/ENCODING FUNCTIONS
// =============================================================================

mod binary_functions {
    use super::*;

    // ✅ SUPPORTED
    test_scalar_function!(
        test_to_hex,
        "SELECT TO_HEX(val_int) FROM test_data WHERE val_int IS NOT NULL AND val_int > 0"
    );
    test_scalar_function!(test_to_base64, "SELECT TO_BASE64(val_str) FROM test_data");
    test_scalar_function!(test_md5, "SELECT MD5(val_str) FROM test_data");
    test_scalar_function!(test_sha1, "SELECT SHA1(val_str) FROM test_data");
    test_scalar_function!(test_sha256, "SELECT SHA256(val_str) FROM test_data");
    test_scalar_function!(test_sha512, "SELECT SHA512(val_str) FROM test_data");
}

// =============================================================================
// BITWISE FUNCTIONS
// =============================================================================

mod bitwise_functions {
    use super::*;

    // ✅ SUPPORTED
    test_scalar_function!(
        test_bitwise_and,
        "SELECT BITWISE_AND(val_int, 15) FROM test_data WHERE val_int IS NOT NULL"
    );
    test_scalar_function!(
        test_bitwise_or,
        "SELECT BITWISE_OR(val_int, 15) FROM test_data WHERE val_int IS NOT NULL"
    );
    test_scalar_function!(
        test_bitwise_xor,
        "SELECT BITWISE_XOR(val_int, 15) FROM test_data WHERE val_int IS NOT NULL"
    );
    test_scalar_function!(
        test_bitwise_not,
        "SELECT BITWISE_NOT(val_int) FROM test_data WHERE val_int IS NOT NULL"
    );
    test_scalar_function!(
        test_bit_count,
        "SELECT BIT_COUNT(val_int) FROM test_data WHERE val_int IS NOT NULL AND val_int > 0"
    );
}

// =============================================================================
// URL FUNCTIONS
// =============================================================================

mod url_functions {
    use super::*;

    #[tokio::test]
    async fn test_url_extract_host() {
        let ctx = create_function_test_context();
        let result = ctx.sql("SELECT URL_EXTRACT_HOST('https://example.com/path?query=1') FROM test_data WHERE id = 1").await;
        assert!(
            result.is_ok(),
            "URL_EXTRACT_HOST failed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_url_extract_path() {
        let ctx = create_function_test_context();
        let result = ctx.sql("SELECT URL_EXTRACT_PATH('https://example.com/path?query=1') FROM test_data WHERE id = 1").await;
        assert!(
            result.is_ok(),
            "URL_EXTRACT_PATH failed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_url_extract_protocol() {
        let ctx = create_function_test_context();
        let result = ctx.sql("SELECT URL_EXTRACT_PROTOCOL('https://example.com/path') FROM test_data WHERE id = 1").await;
        assert!(
            result.is_ok(),
            "URL_EXTRACT_PROTOCOL failed: {:?}",
            result.err()
        );
    }

    #[tokio::test]
    async fn test_url_encode() {
        let ctx = create_function_test_context();
        let result = ctx.sql("SELECT URL_ENCODE(val_str) FROM test_data").await;
        assert!(result.is_ok(), "URL_ENCODE failed: {:?}", result.err());
    }

    #[tokio::test]
    async fn test_url_decode() {
        let ctx = create_function_test_context();
        let result = ctx
            .sql("SELECT URL_DECODE('hello%20world') FROM test_data WHERE id = 1")
            .await;
        assert!(result.is_ok(), "URL_DECODE failed: {:?}", result.err());
    }
}

// =============================================================================
// OTHER FUNCTIONS
// =============================================================================

mod other_functions {
    use super::*;

    // ✅ SUPPORTED
    test_scalar_function!(
        test_typeof,
        "SELECT TYPEOF(val_int) FROM test_data WHERE id = 1"
    );
    test_scalar_function!(test_uuid, "SELECT UUID() FROM test_data WHERE id = 1");
}

// =============================================================================
// AGGREGATE FUNCTIONS
// =============================================================================

mod aggregate_functions {
    use super::*;

    // ✅ SUPPORTED - Basic Aggregates (already in engine)
    test_scalar_function!(test_count, "SELECT COUNT(*) FROM test_data");
    test_scalar_function!(
        test_count_distinct,
        "SELECT COUNT(DISTINCT val_int) FROM test_data"
    );
    test_scalar_function!(test_sum, "SELECT SUM(val_int) FROM test_data");
    test_scalar_function!(test_avg, "SELECT AVG(val_int) FROM test_data");
    test_scalar_function!(test_min, "SELECT MIN(val_int) FROM test_data");
    test_scalar_function!(test_max, "SELECT MAX(val_int) FROM test_data");

    // ✅ SUPPORTED - Statistical Aggregates
    test_scalar_function!(test_stddev, "SELECT STDDEV(val_int) FROM test_data");
    test_scalar_function!(test_stddev_pop, "SELECT STDDEV_POP(val_int) FROM test_data");
    test_scalar_function!(
        test_stddev_samp,
        "SELECT STDDEV_SAMP(val_int) FROM test_data"
    );
    test_scalar_function!(test_variance, "SELECT VARIANCE(val_int) FROM test_data");
    test_scalar_function!(test_var_pop, "SELECT VAR_POP(val_int) FROM test_data");
    test_scalar_function!(test_var_samp, "SELECT VAR_SAMP(val_int) FROM test_data");
    test_scalar_function!(
        test_bool_and,
        "SELECT BOOL_AND(val_int > 0) FROM test_data WHERE val_int IS NOT NULL"
    );
    test_scalar_function!(
        test_bool_or,
        "SELECT BOOL_OR(val_int > 0) FROM test_data WHERE val_int IS NOT NULL"
    );
}

// =============================================================================
// CONVERSION FUNCTIONS
// =============================================================================

mod conversion_functions {
    use super::*;

    // ✅ SUPPORTED
    test_scalar_function!(
        test_cast_int,
        "SELECT CAST(val_float AS BIGINT) FROM test_data"
    );
    test_scalar_function!(
        test_cast_varchar,
        "SELECT CAST(val_int AS VARCHAR) FROM test_data"
    );
}
