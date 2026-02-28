# Architecture Decision Record: Decimal128 Type Support in Aggregation

## Status
Proposed

## Context
The TPC-H benchmark queries Q08, Q10, Q15, and Q18 fail due to incomplete Decimal128 type support in the query engine:

1. **Q08**: Arithmetic expression `l_extendedprice * (1 - l_discount)` fails with type coercion error
   - `l_extendedprice` is `Decimal128(15,2)`
   - `(1 - l_discount)` produces `Int64`
   - The `coerce_numeric_types` function doesn't handle Decimal128

2. **Q10, Q18**: GROUP BY on decimal columns (`o_totalprice`, `c_acctbal`) fails
   - `GroupValue` enum has no `Decimal128` variant
   - `extract_group_value` can't extract Decimal128 values
   - `build_group_array` can't build Decimal128 arrays

3. **Q15**: MAX aggregation on `Decimal128(38,10)` fails
   - `compute_single_agg` doesn't handle Decimal128Array for MAX/MIN

## Decision

### 1. GroupValue Enum Extension
Add `Decimal128(i128, u8)` variant to store raw decimal value and scale:

```rust
enum GroupValue {
    // ... existing variants
    Decimal128(i128, u8),  // (raw value, scale)
}
```

**Rationale**: Using raw `i128` + scale instead of `rust_decimal::Decimal`:
- Direct extraction from Arrow's `Decimal128Array`
- Preserves exact precision and scale
- Simple `Hash` implementation (tuples hash automatically)

### 2. Decimal128 Extraction
Add extraction in `extract_group_value`:

```rust
if let Some(a) = arr.as_any().downcast_ref::<Decimal128Array>() {
    return GroupValue::Decimal128(a.value(row), a.scale());
}
```

### 3. Decimal128 Array Building
Add case in `build_group_array` for `DataType::Decimal128(precision, scale)`:

```rust
DataType::Decimal128(precision, scale) => {
    let mut builder = Decimal128Builder::with_capacity(num_groups);
    for key in groups.keys() {
        match &key.values[col_idx] {
            GroupValue::Decimal128(v, _) => builder.append_value(*v),
            GroupValue::Null => builder.append_null(),
            _ => builder.append_null(),
        }
    }
    let array = builder.finish()
        .with_precision_and_scale(*precision, *scale)?;
    Ok(Arc::new(array))
}
```

### 4. MAX/MIN Aggregation Support
Add `Decimal128Array` handling in `compute_single_agg`:

```rust
} else if let Some(a) = input.as_any().downcast_ref::<Decimal128Array>() {
    let max = a.iter().flatten().max().unwrap_or(i128::MIN);
    let precision = a.precision();
    let scale = a.scale();
    Arc::new(Decimal128Array::from(vec![max])
        .with_precision_and_scale(precision, scale)?)
}
```

### 5. Type Coercion for Arithmetic
Add Decimal128 handling in `coerce_numeric_types` (filter.rs):

```rust
// Decimal128 preservation
(DataType::Decimal128(_, _), _) | (_, DataType::Decimal128(_, _)) => {
    // Keep the decimal type - cast the other operand to decimal
    // For now, use the larger decimal type
    match (left, right) {
        (DataType::Decimal128(p1, s1), DataType::Decimal128(_, _)) => {
            Ok(DataType::Decimal128(*p1, *s1))
        }
        (DataType::Decimal128(p, s), _) => Ok(DataType::Decimal128(*p, *s)),
        (_, DataType::Decimal128(p, s)) => Ok(DataType::Decimal128(*p, *s)),
    }
}
```

**Rationale**: When mixing Decimal128 with Int64, preserve Decimal128 to avoid precision loss.

## Consequences

### Positive
- Fixes 4 TPC-H query failures (Q08, Q10, Q15, Q18)
- Moves benchmark from 17/22 to 21/22 passing queries
- Maintains exact decimal precision throughout aggregation

### Negative
- Minor performance overhead from Decimal128 handling
- May need to extend to Decimal256 for future scale

### Risks
- Need to ensure consistent scale handling across operations
- Type coercion edge cases may need additional testing

## Implementation Order
1. GroupValue enum + extraction (Q10, Q18)
2. build_group_array (Q10, Q18)
3. MAX/MIN support (Q15)
4. Type coercion (Q08)

## Related Files
- `src/physical/operators/hash_agg.rs` - GROUP BY and aggregation
- `src/physical/operators/filter.rs` - Type coercion for arithmetic
- `src/planner/logical_expr.rs` - ScalarValue::Decimal128 definition
