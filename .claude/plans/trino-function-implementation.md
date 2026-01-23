# Trino Function Implementation Plan

## Overview

This plan covers implementing Trino-compatible SQL functions in the query engine.
Total estimated functions to implement: **~200+**

---

## Phase 1: Test Harness Architecture

### 1.1 Test Framework Design

Create a macro-based test harness that can:
1. Define expected function behavior declaratively
2. Auto-generate test cases for each function
3. Support multiple input types and edge cases
4. Track implementation status

**File Structure:**
```
tests/
├── functions/
│   ├── mod.rs                 # Test module exports
│   ├── test_harness.rs        # Core test infrastructure
│   ├── math_functions.rs      # Math function tests
│   ├── string_functions.rs    # String function tests
│   ├── datetime_functions.rs  # Date/time function tests
│   ├── aggregate_functions.rs # Aggregate function tests
│   ├── window_functions.rs    # Window function tests
│   ├── regexp_functions.rs    # Regex function tests
│   ├── json_functions.rs      # JSON function tests
│   ├── array_functions.rs     # Array function tests
│   ├── binary_functions.rs    # Binary/hash function tests
│   ├── bitwise_functions.rs   # Bitwise function tests
│   ├── url_functions.rs       # URL function tests
│   └── map_functions.rs       # Map function tests
```

### 1.2 Test Harness Implementation

```rust
// tests/functions/test_harness.rs

/// Macro for defining function tests
macro_rules! function_test {
    ($name:ident, $sql:expr, $expected:expr) => {
        #[tokio::test]
        async fn $name() {
            let ctx = create_test_context();
            let result = ctx.sql($sql).await;
            assert_function_result(result, $expected);
        }
    };
}

/// Macro for defining unsupported function tests (should error gracefully)
macro_rules! unsupported_function_test {
    ($name:ident, $sql:expr) => {
        #[tokio::test]
        async fn $name() {
            let ctx = create_test_context();
            let result = ctx.sql($sql).await;
            assert!(result.is_err() || is_not_implemented_error(&result));
        }
    };
}

/// Trait for function test suites
pub trait FunctionTestSuite {
    fn name() -> &'static str;
    fn run_tests() -> Vec<TestResult>;
}
```

### 1.3 Test Data Setup

Create a standard test context with tables containing various data types:

```rust
fn create_function_test_context() -> ExecutionContext {
    let mut ctx = ExecutionContext::new();

    // Numbers table for math functions
    ctx.register_table("numbers", schema![
        ("i8_val", Int8),
        ("i16_val", Int16),
        ("i32_val", Int32),
        ("i64_val", Int64),
        ("f32_val", Float32),
        ("f64_val", Float64),
        ("decimal_val", Decimal128),
    ], test_number_data());

    // Strings table for string functions
    ctx.register_table("strings", schema![
        ("s", Utf8),
        ("s2", Utf8),
        ("pattern", Utf8),
    ], test_string_data());

    // Dates table for datetime functions
    ctx.register_table("dates", schema![
        ("d", Date32),
        ("ts", Timestamp),
        ("interval_val", Interval),
    ], test_date_data());

    ctx
}
```

---

## Phase 2: Function Implementation Priority

### Priority 1: High-Value Functions (Essential for TPC-H and common queries)

#### 2.1 Math Functions (Week 1-2)
| Function | Complexity | Notes |
|----------|------------|-------|
| `mod(n, m)` | Low | Already have `%` operator |
| `sign(x)` | Low | Return -1, 0, or 1 |
| `truncate(x)` | Low | Similar to floor/ceil |
| `ln(x)` | Low | Natural log |
| `log(b, x)` | Low | Log with base |
| `log10(x)` | Low | Common log |
| `exp(x)` | Low | e^x |
| `random()` | Medium | RNG state management |

#### 2.2 String Functions (Week 2-3)
| Function | Complexity | Notes |
|----------|------------|-------|
| `position(sub IN str)` | Low | Already have strpos logic |
| `strpos(str, sub)` | Low | Alias for position |
| `reverse(str)` | Low | String reversal |
| `lpad(str, len, pad)` | Low | Left padding |
| `rpad(str, len, pad)` | Low | Right padding |
| `split_part(str, delim, idx)` | Medium | Split and index |
| `starts_with(str, prefix)` | Low | Prefix check |
| `chr(n)` | Low | ASCII to char |
| `concat_ws(sep, ...)` | Medium | Concat with separator |

#### 2.3 Date/Time Functions (Week 3-4)
| Function | Complexity | Notes |
|----------|------------|-------|
| `current_date` | Low | System date |
| `current_timestamp` | Low | System timestamp |
| `now()` | Low | Alias for current_timestamp |
| `date_add(unit, value, ts)` | Medium | Add interval |
| `date_diff(unit, ts1, ts2)` | Medium | Difference calculation |
| `hour(ts)` | Low | Extract hour |
| `minute(ts)` | Low | Extract minute |
| `second(ts)` | Low | Extract second |
| `quarter(date)` | Low | Extract quarter |
| `week(date)` | Low | Extract week number |
| `day_of_week(date)` | Low | 1-7 day number |
| `day_of_year(date)` | Low | 1-366 day number |

#### 2.4 Conditional Functions (Week 4)
| Function | Complexity | Notes |
|----------|------------|-------|
| `IF(cond, t, f)` | Low | Syntactic sugar for CASE |
| `TRY(expr)` | Medium | Error handling |
| `greatest(v1, v2, ...)` | Low | Max of values |
| `least(v1, v2, ...)` | Low | Min of values |

### Priority 2: Window Functions (Week 5-7)

This requires architectural changes to support the OVER clause.

#### 2.5 Window Function Infrastructure
1. Add `WindowExpr` to logical expressions
2. Add `WindowNode` to logical plan
3. Add `WindowExec` physical operator
4. Implement frame specification parsing

#### 2.6 Window Functions
| Function | Complexity | Notes |
|----------|------------|-------|
| `row_number()` | Medium | Sequential numbering |
| `rank()` | Medium | Ranking with gaps |
| `dense_rank()` | Medium | Ranking without gaps |
| `ntile(n)` | Medium | Divide into n buckets |
| `lag(x, offset, default)` | High | Previous row value |
| `lead(x, offset, default)` | High | Next row value |
| `first_value(x)` | Medium | First in window |
| `last_value(x)` | Medium | Last in window |

### Priority 3: Regular Expression Functions (Week 8-9)

#### 2.7 Regex Functions
| Function | Complexity | Notes |
|----------|------------|-------|
| `regexp_like(str, pattern)` | Medium | Use regex crate |
| `regexp_extract(str, pattern, group)` | Medium | Capture groups |
| `regexp_replace(str, pattern, repl)` | Medium | Pattern replacement |
| `regexp_split(str, pattern)` | Medium | Split by pattern |
| `regexp_count(str, pattern)` | Low | Count matches |

### Priority 4: Aggregate Functions (Week 10-12)

#### 2.8 Statistical Aggregates
| Function | Complexity | Notes |
|----------|------------|-------|
| `stddev(x)` | Medium | Standard deviation |
| `stddev_pop(x)` | Medium | Population stddev |
| `stddev_samp(x)` | Medium | Sample stddev |
| `variance(x)` | Medium | Variance |
| `var_pop(x)` | Medium | Population variance |
| `var_samp(x)` | Medium | Sample variance |
| `corr(y, x)` | High | Correlation |
| `covar_pop(y, x)` | High | Covariance |

#### 2.9 General Aggregates
| Function | Complexity | Notes |
|----------|------------|-------|
| `bool_and(x)` | Low | Logical AND |
| `bool_or(x)` | Low | Logical OR |
| `count_if(x)` | Low | Conditional count |
| `array_agg(x)` | Medium | Collect to array |
| `listagg(x, sep)` | Medium | String aggregation |
| `max_by(x, y)` | Medium | X where Y is max |
| `min_by(x, y)` | Medium | X where Y is min |

### Priority 5: JSON Functions (Week 13-15)

Requires JSON type support in Arrow.

#### 2.10 JSON Functions
| Function | Complexity | Notes |
|----------|------------|-------|
| `json_extract(json, path)` | High | JSONPath support |
| `json_extract_scalar(json, path)` | High | Scalar extraction |
| `json_array_contains(json, val)` | Medium | Array membership |
| `json_array_length(json)` | Low | Array length |
| `json_parse(str)` | Medium | Parse string to JSON |
| `json_format(json)` | Medium | JSON to string |

### Priority 6: Array & Map Functions (Week 16-18)

Requires Array and Map type support.

#### 2.11 Array Functions
| Function | Complexity | Notes |
|----------|------------|-------|
| `cardinality(arr)` | Low | Array length |
| `element_at(arr, idx)` | Low | Array indexing |
| `contains(arr, val)` | Low | Membership check |
| `array_distinct(arr)` | Medium | Remove duplicates |
| `array_sort(arr)` | Medium | Sort array |
| `array_join(arr, sep)` | Medium | Join to string |
| `array_position(arr, val)` | Low | Find index |
| `filter(arr, lambda)` | High | Lambda support |
| `transform(arr, lambda)` | High | Lambda support |

#### 2.12 Map Functions
| Function | Complexity | Notes |
|----------|------------|-------|
| `map_keys(map)` | Low | Extract keys |
| `map_values(map)` | Low | Extract values |
| `element_at(map, key)` | Low | Map lookup |
| `map_concat(m1, m2)` | Medium | Merge maps |
| `cardinality(map)` | Low | Map size |

### Priority 7: Binary & Bitwise Functions (Week 19-20)

#### 2.13 Binary Functions
| Function | Complexity | Notes |
|----------|------------|-------|
| `to_hex(binary)` | Low | Hex encoding |
| `from_hex(str)` | Low | Hex decoding |
| `to_base64(binary)` | Low | Base64 encoding |
| `from_base64(str)` | Low | Base64 decoding |
| `md5(binary)` | Low | MD5 hash |
| `sha256(binary)` | Low | SHA256 hash |

#### 2.14 Bitwise Functions
| Function | Complexity | Notes |
|----------|------------|-------|
| `bitwise_and(a, b)` | Low | AND operation |
| `bitwise_or(a, b)` | Low | OR operation |
| `bitwise_xor(a, b)` | Low | XOR operation |
| `bitwise_not(a)` | Low | NOT operation |
| `bit_count(x)` | Low | Count set bits |

### Priority 8: URL & Miscellaneous (Week 21)

#### 2.15 URL Functions
| Function | Complexity | Notes |
|----------|------------|-------|
| `url_extract_host(url)` | Medium | Parse URL |
| `url_extract_path(url)` | Medium | Parse URL |
| `url_extract_port(url)` | Medium | Parse URL |
| `url_encode(str)` | Low | URL encoding |
| `url_decode(str)` | Low | URL decoding |

---

## Phase 3: Implementation Pattern

### 3.1 Adding a New Scalar Function

1. **Add to ScalarFunction enum** ([logical_expr.rs:186](src/planner/logical_expr.rs#L186))
```rust
pub enum ScalarFunction {
    // existing...
    NewFunction,
}
```

2. **Update Display impl** ([logical_expr.rs:220](src/planner/logical_expr.rs#L220))
```rust
ScalarFunction::NewFunction => write!(f, "NEW_FUNCTION"),
```

3. **Add type inference** ([logical_expr.rs:603](src/planner/logical_expr.rs#L603))
```rust
ScalarFunction::NewFunction => Ok(ArrowDataType::...),
```

4. **Add parsing** ([binder.rs:1486](src/planner/binder.rs#L1486))
```rust
"NEW_FUNCTION" => Ok(Expr::ScalarFunc {
    func: ScalarFunction::NewFunction,
    args,
}),
```

5. **Add evaluation** ([filter.rs](src/physical/operators/filter.rs))
```rust
ScalarFunction::NewFunction => {
    // Implementation
}
```

6. **Add tests**
```rust
function_test!(test_new_function, "SELECT new_function(1)", 42);
```

### 3.2 Adding a New Aggregate Function

1. Add to `AggregateFunction` enum
2. Update parsing in binder
3. Add accumulator state in hash_agg.rs
4. Implement update/merge/finalize logic
5. Add tests

### 3.3 Adding Window Function Support

This is a larger architectural change:

1. Add `WindowExpr` to Expr enum
2. Add `WindowNode` to LogicalPlan enum
3. Implement window parsing in binder
4. Add `WindowExec` physical operator
5. Implement partitioning and frame logic

---

## Phase 4: Test Categories

### 4.1 Basic Function Tests
- Correct result for valid inputs
- NULL handling
- Edge cases (empty strings, zero, negative numbers)

### 4.2 Type Coercion Tests
- Function works with different numeric types
- Implicit casting behavior

### 4.3 Error Handling Tests
- Invalid argument count
- Invalid argument types
- Out of range values

### 4.4 Integration Tests
- Functions in WHERE clauses
- Functions in GROUP BY
- Functions in ORDER BY
- Nested function calls
- Functions with aggregates

---

## Appendix: Function Inventory

### Currently Supported (24 functions)
```
Aggregates: COUNT, COUNT(DISTINCT), SUM, AVG, MIN, MAX
Math: ABS, CEIL, FLOOR, ROUND, POWER, SQRT
String: UPPER, LOWER, TRIM, LTRIM, RTRIM, LENGTH, SUBSTRING, CONCAT, REPLACE
DateTime: YEAR, MONTH, DAY, DATE_TRUNC, DATE_PART, EXTRACT
Conditional: COALESCE, NULLIF, CASE
```

### To Implement (~200 functions)
```
Math: ~35 functions
String: ~20 functions
DateTime: ~35 functions
Aggregate: ~40 functions
Window: ~11 functions
Regexp: ~7 functions
JSON: ~15 functions
Array: ~38 functions
Binary: ~35 functions
Bitwise: ~8 functions
URL: ~9 functions
Map: ~13 functions
Other: ~15 functions
```

---

## Timeline Estimate

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| Phase 1 | 1 week | Test harness infrastructure |
| Priority 1 | 4 weeks | Math, String, DateTime, Conditional |
| Priority 2 | 3 weeks | Window functions |
| Priority 3 | 2 weeks | Regex functions |
| Priority 4 | 3 weeks | Aggregate functions |
| Priority 5 | 3 weeks | JSON functions |
| Priority 6 | 3 weeks | Array & Map functions |
| Priority 7 | 2 weeks | Binary & Bitwise functions |
| Priority 8 | 1 week | URL & Misc functions |

**Total: ~22 weeks** for full Trino function compatibility
