# Trino Function Implementation Plan

## Overview

This plan covers implementing Trino-compatible SQL functions in the query engine.
**Total Trino functions: ~350+**

Based on Trino 479 documentation.

---

## Current Implementation Status

### Implemented Functions (~80 functions)

#### Math (25 implemented)
```
ABS, CEIL, CEILING, FLOOR, ROUND, POWER, SQRT, MOD, SIGN, TRUNCATE,
LN, LOG, LOG2, LOG10, EXP, RANDOM, SIN, COS, TAN, ASIN, ACOS, ATAN,
ATAN2, DEGREES, RADIANS, PI, E, CBRT
```

#### String (24 implemented)
```
UPPER, LOWER, TRIM, LTRIM, RTRIM, LENGTH, SUBSTRING, CONCAT, REPLACE,
POSITION, STRPOS, REVERSE, LPAD, RPAD, SPLIT_PART, STARTS_WITH, ENDS_WITH,
CHR, ASCII, CONCAT_WS, LEFT, RIGHT, REPEAT
```

#### Date/Time (10 implemented)
```
YEAR, MONTH, DAY, DATE_TRUNC, DATE_PART, EXTRACT, CURRENT_DATE,
CURRENT_TIMESTAMP, NOW
```

#### Aggregate (14 implemented)
```
COUNT, COUNT(DISTINCT), SUM, AVG, MIN, MAX, STDDEV, STDDEV_POP,
STDDEV_SAMP, VARIANCE, VAR_POP, VAR_SAMP, BOOL_AND, BOOL_OR
```

#### Conditional (6 implemented)
```
COALESCE, NULLIF, CASE, IF, GREATEST, LEAST
```

#### Regex (4 implemented)
```
REGEXP_LIKE, REGEXP_EXTRACT, REGEXP_REPLACE, REGEXP_COUNT
```

#### Binary/Encoding (6 implemented)
```
TO_HEX, TO_BASE64, MD5, SHA1, SHA256, SHA512
```

#### Bitwise (5 implemented)
```
BITWISE_AND, BITWISE_OR, BITWISE_XOR, BITWISE_NOT, BIT_COUNT
```

#### URL (5 implemented)
```
URL_EXTRACT_HOST, URL_EXTRACT_PATH, URL_EXTRACT_PROTOCOL, URL_ENCODE, URL_DECODE
```

#### Other (2 implemented)
```
TYPEOF, UUID
```

---

## Complete Trino Function Inventory (Not Yet Implemented)

### Priority 1: Math Functions

#### Not Implemented (~20 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `rand()` | `→ double` | Low | Alias for random() |
| `random(n)` | `→ [0, n)` | Low | Random in range |
| `random(m, n)` | `→ [m, n)` | Low | Random in range |
| `pow(x, p)` | `→ double` | Low | Alias for power() |
| `width_bucket(x, bound1, bound2, n)` | `→ bigint` | Medium | Histogram bucket |
| `width_bucket(x, bins)` | `→ bigint` | Medium | Array-based buckets |
| `cosh(x)` | `→ double` | Low | Hyperbolic cosine |
| `sinh(x)` | `→ double` | Low | Hyperbolic sine |
| `tanh(x)` | `→ double` | Low | Hyperbolic tangent |
| `cosine_distance(a, b)` | `→ double` | Medium | Vector distance |
| `cosine_similarity(a, b)` | `→ double` | Medium | Vector similarity |
| `infinity()` | `→ double` | Low | Infinity constant |
| `is_finite(x)` | `→ boolean` | Low | Finite check |
| `is_infinite(x)` | `→ boolean` | Low | Infinite check |
| `is_nan(x)` | `→ boolean` | Low | NaN check |
| `nan()` | `→ double` | Low | NaN constant |
| `from_base(string, radix)` | `→ bigint` | Low | Base conversion |
| `to_base(x, radix)` | `→ varchar` | Low | Base conversion |
| `beta_cdf(a, b, v)` | `→ double` | High | Beta distribution CDF |
| `inverse_beta_cdf(a, b, p)` | `→ double` | High | Inverse beta CDF |
| `inverse_normal_cdf(mean, sd, p)` | `→ double` | High | Inverse normal CDF |
| `normal_cdf(mean, sd, v)` | `→ double` | High | Normal distribution CDF |
| `t_cdf(x, df)` | `→ double` | High | Student's t CDF |
| `t_pdf(x, df)` | `→ double` | High | Student's t PDF |
| `wilson_interval_lower(s, t, z)` | `→ double` | Medium | Wilson score lower |
| `wilson_interval_upper(s, t, z)` | `→ double` | Medium | Wilson score upper |

---

### Priority 2: String Functions

#### Not Implemented (~15 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `codepoint(string)` | `→ integer` | Low | Unicode codepoint |
| `hamming_distance(s1, s2)` | `→ bigint` | Low | String distance |
| `levenshtein_distance(s1, s2)` | `→ bigint` | Medium | Edit distance |
| `soundex(char)` | `→ string` | Medium | Phonetic encoding |
| `split(string, delimiter)` | `→ array` | Medium | Split to array |
| `split(string, delimiter, limit)` | `→ array` | Medium | Split with limit |
| `split_to_map(s, ed, kvd)` | `→ map` | Medium | Split to map |
| `split_to_multimap(s, ed, kvd)` | `→ map` | Medium | Split to multimap |
| `translate(source, from, to)` | `→ varchar` | Medium | Character translation |
| `luhn_check(string)` | `→ boolean` | Low | Luhn algorithm |
| `word_stem(word)` | `→ varchar` | High | Stemming (English) |
| `word_stem(word, lang)` | `→ varchar` | High | Stemming (multi-lang) |
| `normalize(string)` | `→ varchar` | Medium | Unicode normalize |
| `normalize(string, form)` | `→ varchar` | Medium | Unicode normalize |
| `to_utf8(string)` | `→ varbinary` | Low | UTF-8 encoding |
| `from_utf8(binary)` | `→ varchar` | Low | UTF-8 decoding |
| `from_utf8(binary, replace)` | `→ varchar` | Low | UTF-8 with replacement |

---

### Priority 3: Date/Time Functions

#### Not Implemented (~30 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `current_time` | `→ time with tz` | Low | Current time |
| `current_timezone()` | `→ varchar` | Low | Session timezone |
| `localtime` | `→ time` | Low | Local time |
| `localtimestamp` | `→ timestamp` | Low | Local timestamp |
| `date(x)` | `→ date` | Low | Convert to date |
| `from_iso8601_timestamp(s)` | `→ timestamp` | Medium | ISO 8601 parse |
| `from_iso8601_timestamp_nanos(s)` | `→ timestamp(9)` | Medium | Nano precision |
| `from_iso8601_date(s)` | `→ date` | Medium | ISO 8601 date parse |
| `from_unixtime(t)` | `→ timestamp` | Low | Unix time convert |
| `from_unixtime(t, zone)` | `→ timestamp` | Low | With timezone |
| `from_unixtime(t, h, m)` | `→ timestamp` | Low | With offset |
| `from_unixtime_nanos(t)` | `→ timestamp(9)` | Low | Nano precision |
| `to_iso8601(x)` | `→ varchar` | Low | ISO 8601 format |
| `to_unixtime(ts)` | `→ double` | Low | To Unix time |
| `to_milliseconds(interval)` | `→ bigint` | Low | Interval to ms |
| `date_format(ts, fmt)` | `→ varchar` | Medium | Custom format |
| `format_datetime(ts, fmt)` | `→ varchar` | Medium | Joda format |
| `date_parse(s, fmt)` | `→ timestamp` | Medium | Parse with format |
| `parse_datetime(s, fmt)` | `→ timestamp` | Medium | Joda parse |
| `parse_duration(s)` | `→ interval` | Medium | Duration parse |
| `at_timezone(ts, zone)` | `→ timestamp` | Medium | Timezone convert |
| `with_timezone(ts, zone)` | `→ timestamp` | Medium | Add timezone |
| `timezone(ts)` | `→ varchar` | Low | Get timezone |
| `date_add(unit, value, ts)` | `→ same` | Medium | Add interval |
| `date_diff(unit, ts1, ts2)` | `→ bigint` | Medium | Date difference |
| `last_day_of_month(x)` | `→ date` | Low | Month end |
| `human_readable_seconds(d)` | `→ varchar` | Low | Duration format |
| `hour(x)` | `→ bigint` | Low | Extract hour |
| `minute(x)` | `→ bigint` | Low | Extract minute |
| `second(x)` | `→ bigint` | Low | Extract second |
| `millisecond(x)` | `→ bigint` | Low | Extract millisecond |
| `day_of_week(x)` / `dow(x)` | `→ bigint` | Low | Day of week |
| `day_of_year(x)` / `doy(x)` | `→ bigint` | Low | Day of year |
| `quarter(x)` | `→ bigint` | Low | Extract quarter |
| `week(x)` / `week_of_year(x)` | `→ bigint` | Low | Week number |
| `year_of_week(x)` / `yow(x)` | `→ bigint` | Low | ISO week year |
| `timezone_hour(ts)` | `→ bigint` | Low | TZ hour offset |
| `timezone_minute(ts)` | `→ bigint` | Low | TZ minute offset |

---

### Priority 4: Aggregate Functions

#### Not Implemented (~30 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `any_value(x)` | `→ same` | Low | Any non-null value |
| `arbitrary(x)` | `→ same` | Low | Alias for any_value |
| `array_agg(x)` | `→ array` | Medium | Collect to array |
| `checksum(x)` | `→ varbinary` | Medium | Order-insensitive hash |
| `count_if(x)` | `→ bigint` | Low | Conditional count |
| `every(boolean)` | `→ boolean` | Low | Alias for bool_and |
| `geometric_mean(x)` | `→ double` | Medium | Geometric mean |
| `listagg(x, sep)` | `→ varchar` | Medium | String aggregation |
| `max(x, n)` | `→ array` | Medium | Top N values |
| `max_by(x, y)` | `→ same as x` | Medium | X where Y is max |
| `max_by(x, y, n)` | `→ array` | Medium | Top N by Y |
| `min(x, n)` | `→ array` | Medium | Bottom N values |
| `min_by(x, y)` | `→ same as x` | Medium | X where Y is min |
| `min_by(x, y, n)` | `→ array` | Medium | Bottom N by Y |
| `bitwise_and_agg(x)` | `→ bigint` | Low | Aggregate AND |
| `bitwise_or_agg(x)` | `→ bigint` | Low | Aggregate OR |
| `bitwise_xor_agg(x)` | `→ bigint` | Low | Aggregate XOR |
| `histogram(x)` | `→ map` | Medium | Value histogram |
| `map_agg(key, value)` | `→ map` | Medium | Aggregate to map |
| `map_union(x)` | `→ map` | Medium | Union of maps |
| `multimap_agg(k, v)` | `→ map` | Medium | Multi-value map |
| `approx_distinct(x)` | `→ bigint` | Medium | HyperLogLog count |
| `approx_distinct(x, e)` | `→ bigint` | Medium | With error bound |
| `approx_most_frequent(b, v, c)` | `→ map` | High | Frequent items |
| `approx_percentile(x, p)` | `→ same` | High | Approximate percentile |
| `approx_percentile(x, ps)` | `→ array` | High | Multiple percentiles |
| `approx_percentile(x, w, p)` | `→ same` | High | Weighted percentile |
| `approx_percentile(x, w, ps)` | `→ array` | High | Weighted multi-percentile |
| `numeric_histogram(b, v)` | `→ map` | High | Numeric histogram |
| `numeric_histogram(b, v, w)` | `→ map` | High | Weighted histogram |
| `corr(y, x)` | `→ double` | Medium | Correlation |
| `covar_pop(y, x)` | `→ double` | Medium | Population covariance |
| `covar_samp(y, x)` | `→ double` | Medium | Sample covariance |
| `kurtosis(x)` | `→ double` | Medium | Kurtosis |
| `regr_intercept(y, x)` | `→ double` | Medium | Regression intercept |
| `regr_slope(y, x)` | `→ double` | Medium | Regression slope |
| `skewness(x)` | `→ double` | Medium | Skewness |
| `reduce_agg(v, s, inp, comb)` | `→ S` | High | Lambda aggregate |

---

### Priority 5: Window Functions (Requires Architecture Change)

#### Not Implemented (~11 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `row_number()` | `→ bigint` | Medium | Sequential number |
| `rank()` | `→ bigint` | Medium | Rank with gaps |
| `dense_rank()` | `→ bigint` | Medium | Rank without gaps |
| `ntile(n)` | `→ bigint` | Medium | Bucket assignment |
| `percent_rank()` | `→ double` | Medium | Relative rank |
| `cume_dist()` | `→ double` | Medium | Cumulative distribution |
| `first_value(x)` | `→ same` | Medium | First in window |
| `last_value(x)` | `→ same` | Medium | Last in window |
| `nth_value(x, offset)` | `→ same` | Medium | Nth value |
| `lead(x[, offset[, default]])` | `→ same` | High | Next row value |
| `lag(x[, offset[, default]])` | `→ same` | High | Previous row value |

**Infrastructure Required:**
1. Add `WindowExpr` to logical expressions
2. Add `WindowNode` to logical plan
3. Add `WindowExec` physical operator
4. Implement frame specification parsing (ROWS/RANGE/GROUPS)
5. Implement PARTITION BY and ORDER BY handling

---

### Priority 6: JSON Functions (Requires JSON Type)

#### Not Implemented (~15 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `json_exists(json, path)` | `→ boolean` | High | Path existence |
| `json_query(json, path)` | `→ json` | High | Extract JSON |
| `json_value(json, path)` | `→ varchar` | High | Extract scalar |
| `json_table(...)` | `→ table` | Very High | JSON to table |
| `json_array(...)` | `→ json` | Medium | Create array |
| `json_object(...)` | `→ json` | Medium | Create object |
| `is_json_scalar(json)` | `→ boolean` | Low | Scalar check |
| `json_array_contains(json, v)` | `→ boolean` | Medium | Array membership |
| `json_array_get(json, idx)` | `→ json` | Medium | Array index |
| `json_array_length(json)` | `→ bigint` | Low | Array length |
| `json_extract(json, path)` | `→ json` | High | JSONPath extract |
| `json_extract_scalar(json, path)` | `→ varchar` | High | Extract as string |
| `json_format(json)` | `→ varchar` | Low | Serialize to string |
| `json_parse(string)` | `→ json` | Medium | Parse JSON |
| `json_size(json, path)` | `→ bigint` | Medium | Size of extracted |

---

### Priority 7: Array Functions (Requires Array Type)

#### Not Implemented (~41 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `all_match(arr, func)` | `→ boolean` | High | Lambda predicate |
| `any_match(arr, func)` | `→ boolean` | High | Lambda predicate |
| `array_distinct(x)` | `→ array` | Medium | Remove duplicates |
| `array_intersect(x, y)` | `→ array` | Medium | Set intersection |
| `array_union(x, y)` | `→ array` | Medium | Set union |
| `array_except(x, y)` | `→ array` | Medium | Set difference |
| `array_first(arr)` | `→ E` | Low | First element |
| `array_histogram(x)` | `→ map` | Medium | Value counts |
| `array_join(x, delim)` | `→ varchar` | Low | Join to string |
| `array_join(x, d, null)` | `→ varchar` | Low | With null replacement |
| `array_last(arr)` | `→ E` | Low | Last element |
| `array_max(x)` | `→ same` | Low | Maximum |
| `array_min(x)` | `→ same` | Low | Minimum |
| `array_position(x, elem)` | `→ bigint` | Low | Find index |
| `array_remove(x, elem)` | `→ array` | Low | Remove element |
| `array_sort(x)` | `→ array` | Medium | Sort array |
| `array_sort(arr, func)` | `→ array` | High | Custom comparator |
| `arrays_overlap(x, y)` | `→ boolean` | Medium | Check overlap |
| `cardinality(x)` | `→ bigint` | Low | Array length |
| `combinations(arr, n)` | `→ array` | Medium | N-combinations |
| `concat(arr1, ..., arrN)` | `→ array` | Low | Concatenate arrays |
| `contains(x, elem)` | `→ boolean` | Low | Membership check |
| `contains_sequence(x, seq)` | `→ boolean` | Medium | Subsequence check |
| `element_at(arr, idx)` | `→ E` | Low | Get element |
| `filter(arr, func)` | `→ array` | High | Lambda filter |
| `flatten(x)` | `→ array` | Medium | Flatten nested |
| `ngrams(arr, n)` | `→ array` | Medium | N-grams |
| `none_match(arr, func)` | `→ boolean` | High | Lambda predicate |
| `reduce(arr, s, inp, out)` | `→ R` | High | Lambda reduce |
| `repeat(elem, count)` | `→ array` | Low | Repeat element |
| `reverse(x)` | `→ array` | Low | Reverse array |
| `sequence(start, stop)` | `→ array` | Medium | Generate sequence |
| `sequence(start, stop, step)` | `→ array` | Medium | With step |
| `shuffle(x)` | `→ array` | Low | Random shuffle |
| `slice(x, start, length)` | `→ array` | Low | Subarray |
| `trim_array(x, n)` | `→ array` | Low | Remove last N |
| `transform(arr, func)` | `→ array` | High | Lambda map |
| `euclidean_distance(a, b)` | `→ double` | Medium | Vector distance |
| `dot_product(a, b)` | `→ double` | Medium | Vector dot product |
| `zip(arr1, arr2, ...)` | `→ array(row)` | Medium | Zip arrays |
| `zip_with(a, b, func)` | `→ array` | High | Lambda zip |

---

### Priority 8: Map Functions (Requires Map Type)

#### Not Implemented (~14 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `cardinality(map)` | `→ bigint` | Low | Map size |
| `element_at(map, key)` | `→ V` | Low | Get value |
| `map()` | `→ map` | Low | Empty map |
| `map(keys, values)` | `→ map` | Low | From arrays |
| `map_from_entries(arr)` | `→ map` | Medium | From entries |
| `multimap_from_entries(arr)` | `→ map` | Medium | Multi-value map |
| `map_entries(map)` | `→ array(row)` | Low | To entries |
| `map_concat(m1, ..., mN)` | `→ map` | Medium | Merge maps |
| `map_filter(map, func)` | `→ map` | High | Lambda filter |
| `map_keys(x)` | `→ array` | Low | Extract keys |
| `map_values(x)` | `→ array` | Low | Extract values |
| `map_zip_with(m1, m2, func)` | `→ map` | High | Lambda zip |
| `transform_keys(map, func)` | `→ map` | High | Lambda transform |
| `transform_values(map, func)` | `→ map` | High | Lambda transform |

---

### Priority 9: Binary Functions

#### Not Implemented (~25 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `from_hex(string)` | `→ varbinary` | Low | Hex decode |
| `from_base64(string)` | `→ varbinary` | Low | Base64 decode |
| `from_base64url(string)` | `→ varbinary` | Low | URL-safe base64 |
| `to_base64url(binary)` | `→ varchar` | Low | URL-safe base64 |
| `from_base32(string)` | `→ varbinary` | Low | Base32 decode |
| `to_base32(binary)` | `→ varchar` | Low | Base32 encode |
| `from_big_endian_32(binary)` | `→ integer` | Low | Decode int |
| `to_big_endian_32(integer)` | `→ varbinary` | Low | Encode int |
| `from_big_endian_64(binary)` | `→ bigint` | Low | Decode bigint |
| `to_big_endian_64(bigint)` | `→ varbinary` | Low | Encode bigint |
| `from_ieee754_32(binary)` | `→ real` | Low | Decode float |
| `to_ieee754_32(real)` | `→ varbinary` | Low | Encode float |
| `from_ieee754_64(binary)` | `→ double` | Low | Decode double |
| `to_ieee754_64(double)` | `→ varbinary` | Low | Encode double |
| `crc32(binary)` | `→ bigint` | Low | CRC32 checksum |
| `spooky_hash_v2_32(binary)` | `→ varbinary` | Low | SpookyHash 32 |
| `spooky_hash_v2_64(binary)` | `→ varbinary` | Low | SpookyHash 64 |
| `xxhash64(binary)` | `→ varbinary` | Low | xxHash64 |
| `murmur3(binary)` | `→ varbinary` | Low | MurmurHash3 |
| `hmac_md5(binary, key)` | `→ varbinary` | Medium | HMAC-MD5 |
| `hmac_sha1(binary, key)` | `→ varbinary` | Medium | HMAC-SHA1 |
| `hmac_sha256(binary, key)` | `→ varbinary` | Medium | HMAC-SHA256 |
| `hmac_sha512(binary, key)` | `→ varbinary` | Medium | HMAC-SHA512 |
| `lpad(binary, size, pad)` | `→ varbinary` | Low | Binary lpad |
| `rpad(binary, size, pad)` | `→ varbinary` | Low | Binary rpad |

---

### Priority 10: Bitwise Functions

#### Not Implemented (~3 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `bitwise_left_shift(v, shift)` | `→ same` | Low | Left shift |
| `bitwise_right_shift(v, shift)` | `→ same` | Low | Logical right shift |
| `bitwise_right_shift_arithmetic(v, s)` | `→ same` | Low | Arithmetic shift |

---

### Priority 11: URL Functions

#### Not Implemented (~4 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `url_extract_fragment(url)` | `→ varchar` | Low | Extract fragment |
| `url_extract_parameter(url, name)` | `→ varchar` | Medium | Query parameter |
| `url_extract_port(url)` | `→ bigint` | Low | Extract port |
| `url_extract_query(url)` | `→ varchar` | Low | Extract query string |

---

### Priority 12: Regex Functions

#### Not Implemented (~5 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `regexp_extract_all(s, p)` | `→ array` | Medium | All matches |
| `regexp_extract_all(s, p, g)` | `→ array` | Medium | All group matches |
| `regexp_position(s, p)` | `→ integer` | Low | First match position |
| `regexp_position(s, p, start)` | `→ integer` | Low | From position |
| `regexp_position(s, p, s, o)` | `→ integer` | Low | Nth occurrence |
| `regexp_split(string, pattern)` | `→ array` | Medium | Split by regex |

---

### Priority 13: Conditional Functions

#### Not Implemented (~1 function)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `TRY(expr)` | `→ same or null` | Medium | Error to null |

---

### Priority 14: Conversion Functions

#### Not Implemented (~4 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `try_cast(value AS type)` | `→ type` | Medium | Safe cast |
| `format(format, args...)` | `→ varchar` | Medium | Printf-style |
| `format_number(number)` | `→ varchar` | Low | Human readable |
| `parse_data_size(string)` | `→ bigint` | Low | Parse "1MB" etc |

---

### Priority 15: Specialized Functions

#### Color Functions (~8 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `bar(x, width)` | `→ varchar` | Low | ANSI bar chart |
| `bar(x, w, low, high)` | `→ varchar` | Low | Custom colors |
| `color(string)` | `→ color` | Low | Parse color |
| `color(x, low, high, lc, hc)` | `→ color` | Low | Interpolate |
| `color(x, low_color, high_color)` | `→ color` | Low | Simple interpolate |
| `render(x, color)` | `→ varchar` | Low | ANSI colored text |
| `render(b)` | `→ varchar` | Low | Boolean rendering |
| `rgb(r, g, b)` | `→ color` | Low | RGB color |

#### Geospatial Functions (~80+ functions)
Extensive ST_* functions for geometry operations. High complexity, requires geometry types.

**Key functions include:**
- Constructors: ST_Point, ST_LineString, ST_Polygon, ST_GeometryFromText
- Relationships: ST_Contains, ST_Intersects, ST_Within, ST_Crosses
- Operations: ST_Buffer, ST_Union, ST_Intersection, ST_Difference
- Accessors: ST_Area, ST_Length, ST_Distance, ST_Centroid
- Bing Tiles: bing_tile, bing_tile_polygon, geometry_to_bing_tiles

#### HyperLogLog Functions (~4 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `approx_set(x)` | `→ HyperLogLog` | Medium | Create HLL |
| `cardinality(hll)` | `→ bigint` | Low | HLL cardinality |
| `empty_approx_set()` | `→ HyperLogLog` | Low | Empty HLL |
| `merge(HyperLogLog)` | `→ HyperLogLog` | Medium | Merge HLLs |

#### T-Digest Functions (~5 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `tdigest_agg(x)` | `→ tdigest` | High | Create T-Digest |
| `tdigest_agg(x, w)` | `→ tdigest` | High | Weighted |
| `merge(tdigest)` | `→ tdigest` | Medium | Merge digests |
| `value_at_quantile(td, q)` | `→ double` | Medium | Get percentile |
| `values_at_quantiles(td, qs)` | `→ array` | Medium | Multiple percentiles |

#### Quantile Digest Functions (~7 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `qdigest_agg(x)` | `→ qdigest` | High | Create Q-Digest |
| `qdigest_agg(x, w)` | `→ qdigest` | High | Weighted |
| `qdigest_agg(x, w, acc)` | `→ qdigest` | High | With accuracy |
| `merge(qdigest)` | `→ qdigest` | Medium | Merge digests |
| `value_at_quantile(qd, q)` | `→ T` | Medium | Get percentile |
| `quantile_at_value(qd, v)` | `→ double` | Medium | Inverse |
| `values_at_quantiles(qd, qs)` | `→ array` | Medium | Multiple percentiles |

#### Set Digest Functions (~6 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `make_set_digest(x)` | `→ setdigest` | Medium | Create digest |
| `merge_set_digest(sd)` | `→ setdigest` | Medium | Merge digests |
| `cardinality(setdigest)` | `→ long` | Low | Estimated size |
| `intersection_cardinality(x, y)` | `→ long` | Medium | Intersection size |
| `jaccard_index(x, y)` | `→ double` | Medium | Similarity |
| `hash_counts(x)` | `→ map` | Medium | Hash frequencies |

#### IP Address Functions (~1 function)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `contains(network, address)` | `→ boolean` | Medium | CIDR membership |

#### Session Functions (~4 functions)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `current_user` | `→ varchar` | Low | Session user |
| `current_groups()` | `→ array` | Low | User groups |
| `current_catalog` | `→ varchar` | Low | Current catalog |
| `current_schema` | `→ varchar` | Low | Current schema |

#### System Functions (~1 function)
| Function | Signature | Complexity | Notes |
|----------|-----------|------------|-------|
| `version()` | `→ varchar` | Low | Engine version |

---

## Implementation Summary

| Category | Implemented | Not Implemented | Total |
|----------|-------------|-----------------|-------|
| Math | 25 | ~26 | ~51 |
| String | 24 | ~17 | ~41 |
| Date/Time | 10 | ~38 | ~48 |
| Aggregate | 14 | ~38 | ~52 |
| Window | 0 | 11 | 11 |
| JSON | 0 | ~15 | ~15 |
| Array | 0 | ~41 | ~41 |
| Map | 0 | ~14 | ~14 |
| Binary | 6 | ~25 | ~31 |
| Bitwise | 5 | 3 | 8 |
| URL | 5 | 4 | 9 |
| Regex | 4 | ~6 | ~10 |
| Conditional | 6 | 1 | 7 |
| Conversion | 2 | 4 | 6 |
| Color | 0 | 8 | 8 |
| Geospatial | 0 | ~80 | ~80 |
| HyperLogLog | 0 | 4 | 4 |
| T-Digest | 0 | 5 | 5 |
| Quantile Digest | 0 | 7 | 7 |
| Set Digest | 0 | 6 | 6 |
| IP Address | 0 | 1 | 1 |
| Session | 0 | 4 | 4 |
| System | 0 | 1 | 1 |
| **TOTAL** | **~101** | **~349** | **~450** |

---

## Phase 3: Implementation Pattern

### 3.1 Adding a New Scalar Function

1. **Add to ScalarFunction enum** (`src/planner/logical_expr.rs`)
2. **Update Display impl** for the new function
3. **Add type inference** in `Expr::data_type()`
4. **Add parsing** in `src/planner/binder.rs`
5. **Add evaluation** in `src/physical/operators/filter.rs`
6. **Add tests**

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

## References

- [Trino 479 Functions Documentation](https://trino.io/docs/current/functions.html)
- [Trino Aggregate Functions](https://trino.io/docs/current/functions/aggregate.html)
- [Trino Window Functions](https://trino.io/docs/current/functions/window.html)
- [Trino String Functions](https://trino.io/docs/current/functions/string.html)
- [Trino Date/Time Functions](https://trino.io/docs/current/functions/datetime.html)
