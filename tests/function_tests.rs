//! Trino SQL Function Test Harness
//!
//! This file contains test definitions for all Trino SQL functions.
//! Remove any functions you don't need before implementing.
//!
//! Status Legend:
//! - ✅ SUPPORTED: Function is implemented and tested
//! - ❌ NOT_SUPPORTED: Function is not yet implemented
//! - 🔧 PARTIAL: Function is partially implemented

use query_engine::ExecutionContext;

// =============================================================================
// TEST INFRASTRUCTURE
// =============================================================================

/// Create a test context with sample data for function testing
async fn create_function_test_context() -> ExecutionContext {
    let mut ctx = ExecutionContext::new();

    // Register a simple numbers table for testing
    // In real tests, add proper schema and data
    ctx
}

/// Helper macro for testing scalar functions
macro_rules! test_scalar_function {
    ($name:ident, $sql:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() {
            let ctx = create_function_test_context().await;
            let result = ctx.sql($sql).await;
            match result {
                Ok(r) => {
                    // Add assertion logic based on expected value
                    assert!(r.row_count > 0, "Expected results for: {}", $sql);
                }
                Err(e) => panic!("Query failed: {} - Error: {:?}", $sql, e),
            }
        }
    };
}

/// Helper macro for testing aggregate functions
macro_rules! test_aggregate_function {
    ($name:ident, $sql:expr) => {
        #[tokio::test]
        async fn $name() {
            let ctx = create_function_test_context().await;
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

/// Macro for documenting unsupported functions (tests that should fail gracefully)
macro_rules! test_unsupported_function {
    ($name:ident, $sql:expr, $func_name:expr) => {
        #[tokio::test]
        #[ignore = "Function not yet implemented"]
        async fn $name() {
            let ctx = create_function_test_context().await;
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

    // ✅ SUPPORTED
    test_scalar_function!(test_abs, "SELECT ABS(-5)", 5);
    test_scalar_function!(test_ceil, "SELECT CEIL(4.3)", 5);
    test_scalar_function!(test_ceiling, "SELECT CEILING(4.3)", 5);
    test_scalar_function!(test_floor, "SELECT FLOOR(4.7)", 4);
    test_scalar_function!(test_round, "SELECT ROUND(4.5)", 5);
    test_scalar_function!(test_round_precision, "SELECT ROUND(4.567, 2)", 4.57);
    test_scalar_function!(test_power, "SELECT POWER(2, 3)", 8);
    test_scalar_function!(test_sqrt, "SELECT SQRT(16)", 4);

    // ❌ NOT_SUPPORTED - Basic Math
    test_unsupported_function!(test_cbrt, "SELECT CBRT(27)", "cbrt");
    test_unsupported_function!(test_degrees, "SELECT DEGREES(3.14159)", "degrees");
    test_unsupported_function!(test_radians, "SELECT RADIANS(180)", "radians");
    test_unsupported_function!(test_exp, "SELECT EXP(1)", "exp");
    test_unsupported_function!(test_ln, "SELECT LN(2.718)", "ln");
    test_unsupported_function!(test_log, "SELECT LOG(2, 8)", "log");
    test_unsupported_function!(test_log2, "SELECT LOG2(8)", "log2");
    test_unsupported_function!(test_log10, "SELECT LOG10(100)", "log10");
    test_unsupported_function!(test_mod, "SELECT MOD(10, 3)", "mod");
    test_unsupported_function!(test_sign, "SELECT SIGN(-5)", "sign");
    test_unsupported_function!(test_truncate, "SELECT TRUNCATE(4.567)", "truncate");
    test_unsupported_function!(test_pi, "SELECT PI()", "pi");
    test_unsupported_function!(test_e, "SELECT E()", "e");

    // ❌ NOT_SUPPORTED - Trigonometric
    test_unsupported_function!(test_sin, "SELECT SIN(0)", "sin");
    test_unsupported_function!(test_cos, "SELECT COS(0)", "cos");
    test_unsupported_function!(test_tan, "SELECT TAN(0)", "tan");
    test_unsupported_function!(test_asin, "SELECT ASIN(0)", "asin");
    test_unsupported_function!(test_acos, "SELECT ACOS(1)", "acos");
    test_unsupported_function!(test_atan, "SELECT ATAN(0)", "atan");
    test_unsupported_function!(test_atan2, "SELECT ATAN2(1, 1)", "atan2");
    test_unsupported_function!(test_sinh, "SELECT SINH(0)", "sinh");
    test_unsupported_function!(test_cosh, "SELECT COSH(0)", "cosh");
    test_unsupported_function!(test_tanh, "SELECT TANH(0)", "tanh");

    // ❌ NOT_SUPPORTED - Random
    test_unsupported_function!(test_random, "SELECT RANDOM()", "random");
    test_unsupported_function!(test_rand, "SELECT RAND()", "rand");

    // ❌ NOT_SUPPORTED - Floating Point
    test_unsupported_function!(test_infinity, "SELECT INFINITY()", "infinity");
    test_unsupported_function!(test_nan, "SELECT NAN()", "nan");
    test_unsupported_function!(test_is_finite, "SELECT IS_FINITE(1.0)", "is_finite");
    test_unsupported_function!(test_is_infinite, "SELECT IS_INFINITE(1.0)", "is_infinite");
    test_unsupported_function!(test_is_nan, "SELECT IS_NAN(1.0)", "is_nan");

    // ❌ NOT_SUPPORTED - Base Conversion
    test_unsupported_function!(test_from_base, "SELECT FROM_BASE('ff', 16)", "from_base");
    test_unsupported_function!(test_to_base, "SELECT TO_BASE(255, 16)", "to_base");

    // ❌ NOT_SUPPORTED - Statistical
    test_unsupported_function!(
        test_width_bucket,
        "SELECT WIDTH_BUCKET(5.5, 0, 10, 5)",
        "width_bucket"
    );
}

// =============================================================================
// STRING FUNCTIONS
// =============================================================================

mod string_functions {
    use super::*;

    // ✅ SUPPORTED
    test_scalar_function!(test_upper, "SELECT UPPER('hello')", "HELLO");
    test_scalar_function!(test_lower, "SELECT LOWER('HELLO')", "hello");
    test_scalar_function!(test_trim, "SELECT TRIM('  hello  ')", "hello");
    test_scalar_function!(test_ltrim, "SELECT LTRIM('  hello')", "hello");
    test_scalar_function!(test_rtrim, "SELECT RTRIM('hello  ')", "hello");
    test_scalar_function!(test_length, "SELECT LENGTH('hello')", 5);
    test_scalar_function!(test_substring, "SELECT SUBSTRING('hello', 2, 3)", "ell");
    test_scalar_function!(test_concat, "SELECT CONCAT('a', 'b', 'c')", "abc");
    test_scalar_function!(test_replace, "SELECT REPLACE('hello', 'l', 'x')", "hexxo");

    // ❌ NOT_SUPPORTED
    test_unsupported_function!(test_chr, "SELECT CHR(65)", "chr");
    test_unsupported_function!(test_codepoint, "SELECT CODEPOINT('A')", "codepoint");
    test_unsupported_function!(test_concat_ws, "SELECT CONCAT_WS(',', 'a', 'b')", "concat_ws");
    test_unsupported_function!(
        test_hamming_distance,
        "SELECT HAMMING_DISTANCE('abc', 'abd')",
        "hamming_distance"
    );
    test_unsupported_function!(
        test_levenshtein_distance,
        "SELECT LEVENSHTEIN_DISTANCE('abc', 'abd')",
        "levenshtein_distance"
    );
    test_unsupported_function!(test_lpad, "SELECT LPAD('hi', 5, '*')", "lpad");
    test_unsupported_function!(test_rpad, "SELECT RPAD('hi', 5, '*')", "rpad");
    test_unsupported_function!(test_luhn_check, "SELECT LUHN_CHECK('79927398713')", "luhn_check");
    test_unsupported_function!(test_position, "SELECT POSITION('l' IN 'hello')", "position");
    test_unsupported_function!(test_reverse, "SELECT REVERSE('hello')", "reverse");
    test_unsupported_function!(test_soundex, "SELECT SOUNDEX('Robert')", "soundex");
    test_unsupported_function!(test_split, "SELECT SPLIT('a,b,c', ',')", "split");
    test_unsupported_function!(test_split_part, "SELECT SPLIT_PART('a,b,c', ',', 2)", "split_part");
    test_unsupported_function!(test_strpos, "SELECT STRPOS('hello', 'l')", "strpos");
    test_unsupported_function!(test_starts_with, "SELECT STARTS_WITH('hello', 'he')", "starts_with");
    test_unsupported_function!(test_translate, "SELECT TRANSLATE('hello', 'el', 'ip')", "translate");
    test_unsupported_function!(test_word_stem, "SELECT WORD_STEM('running')", "word_stem");

    // ❌ NOT_SUPPORTED - Unicode
    test_unsupported_function!(test_normalize, "SELECT NORMALIZE('café')", "normalize");
    test_unsupported_function!(test_to_utf8, "SELECT TO_UTF8('hello')", "to_utf8");
    test_unsupported_function!(test_from_utf8, "SELECT FROM_UTF8(X'68656c6c6f')", "from_utf8");
}

// =============================================================================
// DATE/TIME FUNCTIONS
// =============================================================================

mod datetime_functions {
    use super::*;

    // ✅ SUPPORTED
    test_scalar_function!(test_year, "SELECT YEAR(DATE '2023-06-15')", 2023);
    test_scalar_function!(test_month, "SELECT MONTH(DATE '2023-06-15')", 6);
    test_scalar_function!(test_day, "SELECT DAY(DATE '2023-06-15')", 15);
    test_scalar_function!(
        test_date_trunc,
        "SELECT DATE_TRUNC('month', DATE '2023-06-15')",
        "2023-06-01"
    );
    test_scalar_function!(
        test_extract_year,
        "SELECT EXTRACT(YEAR FROM DATE '2023-06-15')",
        2023
    );

    // ❌ NOT_SUPPORTED - Current Time
    test_unsupported_function!(test_current_date, "SELECT CURRENT_DATE", "current_date");
    test_unsupported_function!(test_current_time, "SELECT CURRENT_TIME", "current_time");
    test_unsupported_function!(
        test_current_timestamp,
        "SELECT CURRENT_TIMESTAMP",
        "current_timestamp"
    );
    test_unsupported_function!(
        test_current_timezone,
        "SELECT CURRENT_TIMEZONE()",
        "current_timezone"
    );
    test_unsupported_function!(test_localtime, "SELECT LOCALTIME", "localtime");
    test_unsupported_function!(test_localtimestamp, "SELECT LOCALTIMESTAMP", "localtimestamp");
    test_unsupported_function!(test_now, "SELECT NOW()", "now");

    // ❌ NOT_SUPPORTED - Conversion
    test_unsupported_function!(test_date_fn, "SELECT DATE('2023-06-15')", "date");
    test_unsupported_function!(
        test_from_iso8601_date,
        "SELECT FROM_ISO8601_DATE('2023-06-15')",
        "from_iso8601_date"
    );
    test_unsupported_function!(
        test_from_unixtime,
        "SELECT FROM_UNIXTIME(1686787200)",
        "from_unixtime"
    );
    test_unsupported_function!(
        test_to_unixtime,
        "SELECT TO_UNIXTIME(TIMESTAMP '2023-06-15 00:00:00')",
        "to_unixtime"
    );

    // ❌ NOT_SUPPORTED - Arithmetic
    test_unsupported_function!(
        test_date_add,
        "SELECT DATE_ADD('day', 1, DATE '2023-06-15')",
        "date_add"
    );
    test_unsupported_function!(
        test_date_diff,
        "SELECT DATE_DIFF('day', DATE '2023-06-15', DATE '2023-06-20')",
        "date_diff"
    );
    test_unsupported_function!(
        test_last_day_of_month,
        "SELECT LAST_DAY_OF_MONTH(DATE '2023-06-15')",
        "last_day_of_month"
    );

    // ❌ NOT_SUPPORTED - Format/Parse
    test_unsupported_function!(
        test_date_format,
        "SELECT DATE_FORMAT(TIMESTAMP '2023-06-15', '%Y-%m-%d')",
        "date_format"
    );
    test_unsupported_function!(
        test_date_parse,
        "SELECT DATE_PARSE('2023-06-15', '%Y-%m-%d')",
        "date_parse"
    );

    // ❌ NOT_SUPPORTED - Extraction
    test_unsupported_function!(
        test_hour,
        "SELECT HOUR(TIMESTAMP '2023-06-15 14:30:00')",
        "hour"
    );
    test_unsupported_function!(
        test_minute,
        "SELECT MINUTE(TIMESTAMP '2023-06-15 14:30:00')",
        "minute"
    );
    test_unsupported_function!(
        test_second,
        "SELECT SECOND(TIMESTAMP '2023-06-15 14:30:45')",
        "second"
    );
    test_unsupported_function!(test_quarter, "SELECT QUARTER(DATE '2023-06-15')", "quarter");
    test_unsupported_function!(test_week, "SELECT WEEK(DATE '2023-06-15')", "week");
    test_unsupported_function!(
        test_day_of_week,
        "SELECT DAY_OF_WEEK(DATE '2023-06-15')",
        "day_of_week"
    );
    test_unsupported_function!(
        test_day_of_year,
        "SELECT DAY_OF_YEAR(DATE '2023-06-15')",
        "day_of_year"
    );
}

// =============================================================================
// AGGREGATE FUNCTIONS
// =============================================================================

mod aggregate_functions {
    use super::*;

    // Note: These tests require a table with data.
    // Uncomment and modify once you have proper test tables.

    // ✅ SUPPORTED
    // test_aggregate_function!(test_count, "SELECT COUNT(*) FROM test_table");
    // test_aggregate_function!(test_count_distinct, "SELECT COUNT(DISTINCT col) FROM test_table");
    // test_aggregate_function!(test_sum, "SELECT SUM(num_col) FROM test_table");
    // test_aggregate_function!(test_avg, "SELECT AVG(num_col) FROM test_table");
    // test_aggregate_function!(test_min, "SELECT MIN(num_col) FROM test_table");
    // test_aggregate_function!(test_max, "SELECT MAX(num_col) FROM test_table");

    // ❌ NOT_SUPPORTED - General
    test_unsupported_function!(
        test_any_value,
        "SELECT ANY_VALUE(col) FROM test_table",
        "any_value"
    );
    test_unsupported_function!(
        test_array_agg,
        "SELECT ARRAY_AGG(col) FROM test_table",
        "array_agg"
    );
    test_unsupported_function!(
        test_bool_and,
        "SELECT BOOL_AND(bool_col) FROM test_table",
        "bool_and"
    );
    test_unsupported_function!(
        test_bool_or,
        "SELECT BOOL_OR(bool_col) FROM test_table",
        "bool_or"
    );
    test_unsupported_function!(
        test_count_if,
        "SELECT COUNT_IF(col > 5) FROM test_table",
        "count_if"
    );
    test_unsupported_function!(
        test_geometric_mean,
        "SELECT GEOMETRIC_MEAN(num_col) FROM test_table",
        "geometric_mean"
    );
    test_unsupported_function!(
        test_listagg,
        "SELECT LISTAGG(col, ',') FROM test_table",
        "listagg"
    );
    test_unsupported_function!(
        test_max_by,
        "SELECT MAX_BY(a, b) FROM test_table",
        "max_by"
    );
    test_unsupported_function!(
        test_min_by,
        "SELECT MIN_BY(a, b) FROM test_table",
        "min_by"
    );

    // ❌ NOT_SUPPORTED - Statistical
    test_unsupported_function!(test_stddev, "SELECT STDDEV(num_col) FROM test_table", "stddev");
    test_unsupported_function!(
        test_stddev_pop,
        "SELECT STDDEV_POP(num_col) FROM test_table",
        "stddev_pop"
    );
    test_unsupported_function!(
        test_stddev_samp,
        "SELECT STDDEV_SAMP(num_col) FROM test_table",
        "stddev_samp"
    );
    test_unsupported_function!(
        test_variance,
        "SELECT VARIANCE(num_col) FROM test_table",
        "variance"
    );
    test_unsupported_function!(
        test_var_pop,
        "SELECT VAR_POP(num_col) FROM test_table",
        "var_pop"
    );
    test_unsupported_function!(
        test_var_samp,
        "SELECT VAR_SAMP(num_col) FROM test_table",
        "var_samp"
    );
    test_unsupported_function!(test_corr, "SELECT CORR(y, x) FROM test_table", "corr");
    test_unsupported_function!(
        test_covar_pop,
        "SELECT COVAR_POP(y, x) FROM test_table",
        "covar_pop"
    );
    test_unsupported_function!(
        test_covar_samp,
        "SELECT COVAR_SAMP(y, x) FROM test_table",
        "covar_samp"
    );

    // ❌ NOT_SUPPORTED - Approximate
    test_unsupported_function!(
        test_approx_distinct,
        "SELECT APPROX_DISTINCT(col) FROM test_table",
        "approx_distinct"
    );
    test_unsupported_function!(
        test_approx_percentile,
        "SELECT APPROX_PERCENTILE(num_col, 0.5) FROM test_table",
        "approx_percentile"
    );
}

// =============================================================================
// WINDOW FUNCTIONS
// =============================================================================

mod window_functions {
    use super::*;

    // ❌ NOT_SUPPORTED - All window functions require OVER clause support

    // Ranking Functions
    test_unsupported_function!(
        test_row_number,
        "SELECT ROW_NUMBER() OVER (ORDER BY col) FROM test_table",
        "row_number"
    );
    test_unsupported_function!(
        test_rank,
        "SELECT RANK() OVER (ORDER BY col) FROM test_table",
        "rank"
    );
    test_unsupported_function!(
        test_dense_rank,
        "SELECT DENSE_RANK() OVER (ORDER BY col) FROM test_table",
        "dense_rank"
    );
    test_unsupported_function!(
        test_ntile,
        "SELECT NTILE(4) OVER (ORDER BY col) FROM test_table",
        "ntile"
    );
    test_unsupported_function!(
        test_percent_rank,
        "SELECT PERCENT_RANK() OVER (ORDER BY col) FROM test_table",
        "percent_rank"
    );
    test_unsupported_function!(
        test_cume_dist,
        "SELECT CUME_DIST() OVER (ORDER BY col) FROM test_table",
        "cume_dist"
    );

    // Value Functions
    test_unsupported_function!(
        test_lag,
        "SELECT LAG(col) OVER (ORDER BY col) FROM test_table",
        "lag"
    );
    test_unsupported_function!(
        test_lead,
        "SELECT LEAD(col) OVER (ORDER BY col) FROM test_table",
        "lead"
    );
    test_unsupported_function!(
        test_first_value,
        "SELECT FIRST_VALUE(col) OVER (ORDER BY col) FROM test_table",
        "first_value"
    );
    test_unsupported_function!(
        test_last_value,
        "SELECT LAST_VALUE(col) OVER (ORDER BY col) FROM test_table",
        "last_value"
    );
    test_unsupported_function!(
        test_nth_value,
        "SELECT NTH_VALUE(col, 2) OVER (ORDER BY col) FROM test_table",
        "nth_value"
    );
}

// =============================================================================
// REGULAR EXPRESSION FUNCTIONS
// =============================================================================

mod regexp_functions {
    use super::*;

    // ❌ NOT_SUPPORTED
    test_unsupported_function!(
        test_regexp_like,
        "SELECT REGEXP_LIKE('hello', 'h.*o')",
        "regexp_like"
    );
    test_unsupported_function!(
        test_regexp_count,
        "SELECT REGEXP_COUNT('hello world', 'o')",
        "regexp_count"
    );
    test_unsupported_function!(
        test_regexp_extract,
        "SELECT REGEXP_EXTRACT('hello', '(h.*)(o)')",
        "regexp_extract"
    );
    test_unsupported_function!(
        test_regexp_extract_all,
        "SELECT REGEXP_EXTRACT_ALL('hello', 'l')",
        "regexp_extract_all"
    );
    test_unsupported_function!(
        test_regexp_position,
        "SELECT REGEXP_POSITION('hello', 'l')",
        "regexp_position"
    );
    test_unsupported_function!(
        test_regexp_replace,
        "SELECT REGEXP_REPLACE('hello', 'l', 'x')",
        "regexp_replace"
    );
    test_unsupported_function!(
        test_regexp_split,
        "SELECT REGEXP_SPLIT('a-b-c', '-')",
        "regexp_split"
    );
}

// =============================================================================
// JSON FUNCTIONS
// =============================================================================

mod json_functions {
    use super::*;

    // ❌ NOT_SUPPORTED
    test_unsupported_function!(
        test_json_extract,
        "SELECT JSON_EXTRACT('{\"a\":1}', '$.a')",
        "json_extract"
    );
    test_unsupported_function!(
        test_json_extract_scalar,
        "SELECT JSON_EXTRACT_SCALAR('{\"a\":1}', '$.a')",
        "json_extract_scalar"
    );
    test_unsupported_function!(
        test_json_array_contains,
        "SELECT JSON_ARRAY_CONTAINS('[1,2,3]', 2)",
        "json_array_contains"
    );
    test_unsupported_function!(
        test_json_array_get,
        "SELECT JSON_ARRAY_GET('[1,2,3]', 1)",
        "json_array_get"
    );
    test_unsupported_function!(
        test_json_array_length,
        "SELECT JSON_ARRAY_LENGTH('[1,2,3]')",
        "json_array_length"
    );
    test_unsupported_function!(
        test_json_format,
        "SELECT JSON_FORMAT(JSON '{\"a\":1}')",
        "json_format"
    );
    test_unsupported_function!(
        test_json_parse,
        "SELECT JSON_PARSE('{\"a\":1}')",
        "json_parse"
    );
    test_unsupported_function!(
        test_json_size,
        "SELECT JSON_SIZE('{\"a\":1}', '$.a')",
        "json_size"
    );
}

// =============================================================================
// ARRAY FUNCTIONS
// =============================================================================

mod array_functions {
    use super::*;

    // ❌ NOT_SUPPORTED - Requires ARRAY type support
    test_unsupported_function!(
        test_array_distinct,
        "SELECT ARRAY_DISTINCT(ARRAY[1, 1, 2])",
        "array_distinct"
    );
    test_unsupported_function!(
        test_array_intersect,
        "SELECT ARRAY_INTERSECT(ARRAY[1, 2], ARRAY[2, 3])",
        "array_intersect"
    );
    test_unsupported_function!(
        test_array_union,
        "SELECT ARRAY_UNION(ARRAY[1, 2], ARRAY[2, 3])",
        "array_union"
    );
    test_unsupported_function!(
        test_array_join,
        "SELECT ARRAY_JOIN(ARRAY['a', 'b'], ',')",
        "array_join"
    );
    test_unsupported_function!(
        test_array_max,
        "SELECT ARRAY_MAX(ARRAY[1, 2, 3])",
        "array_max"
    );
    test_unsupported_function!(
        test_array_min,
        "SELECT ARRAY_MIN(ARRAY[1, 2, 3])",
        "array_min"
    );
    test_unsupported_function!(
        test_array_position,
        "SELECT ARRAY_POSITION(ARRAY['a', 'b'], 'b')",
        "array_position"
    );
    test_unsupported_function!(
        test_array_remove,
        "SELECT ARRAY_REMOVE(ARRAY[1, 2, 3], 2)",
        "array_remove"
    );
    test_unsupported_function!(
        test_array_sort,
        "SELECT ARRAY_SORT(ARRAY[3, 1, 2])",
        "array_sort"
    );
    test_unsupported_function!(
        test_cardinality,
        "SELECT CARDINALITY(ARRAY[1, 2, 3])",
        "cardinality"
    );
    test_unsupported_function!(
        test_contains,
        "SELECT CONTAINS(ARRAY[1, 2, 3], 2)",
        "contains"
    );
    test_unsupported_function!(
        test_element_at,
        "SELECT ELEMENT_AT(ARRAY[1, 2, 3], 2)",
        "element_at"
    );
    test_unsupported_function!(
        test_flatten,
        "SELECT FLATTEN(ARRAY[ARRAY[1], ARRAY[2]])",
        "flatten"
    );
    test_unsupported_function!(test_sequence, "SELECT SEQUENCE(1, 5)", "sequence");
    test_unsupported_function!(test_slice, "SELECT SLICE(ARRAY[1, 2, 3, 4], 2, 2)", "slice");
}

// =============================================================================
// BINARY FUNCTIONS
// =============================================================================

mod binary_functions {
    use super::*;

    // ❌ NOT_SUPPORTED
    test_unsupported_function!(test_to_hex, "SELECT TO_HEX(CAST('hello' AS VARBINARY))", "to_hex");
    test_unsupported_function!(test_from_hex, "SELECT FROM_HEX('68656c6c6f')", "from_hex");
    test_unsupported_function!(
        test_to_base64,
        "SELECT TO_BASE64(CAST('hello' AS VARBINARY))",
        "to_base64"
    );
    test_unsupported_function!(test_from_base64, "SELECT FROM_BASE64('aGVsbG8=')", "from_base64");
    test_unsupported_function!(
        test_md5,
        "SELECT MD5(CAST('hello' AS VARBINARY))",
        "md5"
    );
    test_unsupported_function!(
        test_sha256,
        "SELECT SHA256(CAST('hello' AS VARBINARY))",
        "sha256"
    );
    test_unsupported_function!(
        test_sha512,
        "SELECT SHA512(CAST('hello' AS VARBINARY))",
        "sha512"
    );
    test_unsupported_function!(
        test_crc32,
        "SELECT CRC32(CAST('hello' AS VARBINARY))",
        "crc32"
    );
    test_unsupported_function!(
        test_xxhash64,
        "SELECT XXHASH64(CAST('hello' AS VARBINARY))",
        "xxhash64"
    );
}

// =============================================================================
// BITWISE FUNCTIONS
// =============================================================================

mod bitwise_functions {
    use super::*;

    // ❌ NOT_SUPPORTED
    test_unsupported_function!(test_bitwise_and, "SELECT BITWISE_AND(5, 3)", "bitwise_and");
    test_unsupported_function!(test_bitwise_or, "SELECT BITWISE_OR(5, 3)", "bitwise_or");
    test_unsupported_function!(test_bitwise_xor, "SELECT BITWISE_XOR(5, 3)", "bitwise_xor");
    test_unsupported_function!(test_bitwise_not, "SELECT BITWISE_NOT(5)", "bitwise_not");
    test_unsupported_function!(test_bit_count, "SELECT BIT_COUNT(7)", "bit_count");
    test_unsupported_function!(
        test_bitwise_left_shift,
        "SELECT BITWISE_LEFT_SHIFT(1, 3)",
        "bitwise_left_shift"
    );
    test_unsupported_function!(
        test_bitwise_right_shift,
        "SELECT BITWISE_RIGHT_SHIFT(8, 2)",
        "bitwise_right_shift"
    );
}

// =============================================================================
// URL FUNCTIONS
// =============================================================================

mod url_functions {
    use super::*;

    // ❌ NOT_SUPPORTED
    test_unsupported_function!(
        test_url_extract_host,
        "SELECT URL_EXTRACT_HOST('https://example.com/path')",
        "url_extract_host"
    );
    test_unsupported_function!(
        test_url_extract_path,
        "SELECT URL_EXTRACT_PATH('https://example.com/path')",
        "url_extract_path"
    );
    test_unsupported_function!(
        test_url_extract_port,
        "SELECT URL_EXTRACT_PORT('https://example.com:8080/path')",
        "url_extract_port"
    );
    test_unsupported_function!(
        test_url_extract_protocol,
        "SELECT URL_EXTRACT_PROTOCOL('https://example.com/path')",
        "url_extract_protocol"
    );
    test_unsupported_function!(
        test_url_extract_query,
        "SELECT URL_EXTRACT_QUERY('https://example.com?a=1')",
        "url_extract_query"
    );
    test_unsupported_function!(
        test_url_extract_parameter,
        "SELECT URL_EXTRACT_PARAMETER('https://example.com?a=1', 'a')",
        "url_extract_parameter"
    );
    test_unsupported_function!(test_url_encode, "SELECT URL_ENCODE('hello world')", "url_encode");
    test_unsupported_function!(test_url_decode, "SELECT URL_DECODE('hello%20world')", "url_decode");
}

// =============================================================================
// MAP FUNCTIONS
// =============================================================================

mod map_functions {
    use super::*;

    // ❌ NOT_SUPPORTED - Requires MAP type support
    test_unsupported_function!(
        test_map,
        "SELECT MAP(ARRAY['a', 'b'], ARRAY[1, 2])",
        "map"
    );
    test_unsupported_function!(
        test_map_keys,
        "SELECT MAP_KEYS(MAP(ARRAY['a'], ARRAY[1]))",
        "map_keys"
    );
    test_unsupported_function!(
        test_map_values,
        "SELECT MAP_VALUES(MAP(ARRAY['a'], ARRAY[1]))",
        "map_values"
    );
    test_unsupported_function!(
        test_map_entries,
        "SELECT MAP_ENTRIES(MAP(ARRAY['a'], ARRAY[1]))",
        "map_entries"
    );
    test_unsupported_function!(
        test_map_concat,
        "SELECT MAP_CONCAT(MAP(ARRAY['a'], ARRAY[1]), MAP(ARRAY['b'], ARRAY[2]))",
        "map_concat"
    );
}

// =============================================================================
// CONDITIONAL FUNCTIONS
// =============================================================================

mod conditional_functions {
    use super::*;

    // ✅ SUPPORTED
    test_scalar_function!(test_coalesce, "SELECT COALESCE(NULL, 'a', 'b')", "a");
    test_scalar_function!(test_nullif, "SELECT NULLIF(1, 1)", "NULL");
    test_scalar_function!(
        test_case,
        "SELECT CASE WHEN 1=1 THEN 'yes' ELSE 'no' END",
        "yes"
    );

    // ❌ NOT_SUPPORTED
    test_unsupported_function!(test_if, "SELECT IF(1=1, 'yes', 'no')", "if");
    test_unsupported_function!(test_try, "SELECT TRY(1/0)", "try");
}

// =============================================================================
// COMPARISON FUNCTIONS
// =============================================================================

mod comparison_functions {
    use super::*;

    // ❌ NOT_SUPPORTED
    test_unsupported_function!(test_greatest, "SELECT GREATEST(1, 2, 3)", "greatest");
    test_unsupported_function!(test_least, "SELECT LEAST(1, 2, 3)", "least");
    test_unsupported_function!(
        test_is_distinct_from,
        "SELECT 1 IS DISTINCT FROM NULL",
        "is_distinct_from"
    );
}

// =============================================================================
// CONVERSION FUNCTIONS
// =============================================================================

mod conversion_functions {
    use super::*;

    // ✅ SUPPORTED
    test_scalar_function!(test_cast_int, "SELECT CAST('123' AS INTEGER)", 123);
    test_scalar_function!(test_cast_varchar, "SELECT CAST(123 AS VARCHAR)", "123");

    // ❌ NOT_SUPPORTED
    test_unsupported_function!(test_try_cast, "SELECT TRY_CAST('abc' AS INTEGER)", "try_cast");
    test_unsupported_function!(
        test_format,
        "SELECT FORMAT('%s %d', 'hello', 42)",
        "format"
    );
    test_unsupported_function!(test_typeof, "SELECT TYPEOF(123)", "typeof");
}

// =============================================================================
// UUID FUNCTIONS
// =============================================================================

mod uuid_functions {
    use super::*;

    // ❌ NOT_SUPPORTED
    test_unsupported_function!(test_uuid, "SELECT UUID()", "uuid");
}
