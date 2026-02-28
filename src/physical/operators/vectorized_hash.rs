//! Vectorized hash computation for batch-level operations.
//!
//! Provides batch-level hashing of Arrow arrays for use in hash joins and aggregations.
//! Eliminates per-row allocation overhead by processing entire columns at a time.

use arrow::array::{Array, ArrayRef, Int64Array, StringArray};

/// Compute hash values for all rows across multiple key columns.
/// Processes column-at-a-time for cache-friendly access.
pub fn hash_arrays(key_arrays: &[ArrayRef], num_rows: usize) -> Vec<u64> {
    let mut hashes = vec![0x517cc1b727220a95u64; num_rows]; // FNV offset basis

    for array in key_arrays {
        hash_array_into(array, &mut hashes);
    }

    hashes
}

/// Hash a single array column, combining with existing hash values.
fn hash_array_into(array: &ArrayRef, hashes: &mut [u64]) {
    let num_rows = hashes.len();

    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        let values = arr.values();
        let nulls = arr.nulls();
        for i in 0..num_rows {
            if nulls.map_or(true, |n| n.is_valid(i)) {
                hashes[i] = combine_hash(hashes[i], values[i] as u64);
            }
        }
    } else if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Int32Array>() {
        let values = arr.values();
        let nulls = arr.nulls();
        for i in 0..num_rows {
            if nulls.map_or(true, |n| n.is_valid(i)) {
                hashes[i] = combine_hash(hashes[i], values[i] as u64);
            }
        }
    } else if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Float64Array>() {
        let values = arr.values();
        let nulls = arr.nulls();
        for i in 0..num_rows {
            if nulls.map_or(true, |n| n.is_valid(i)) {
                hashes[i] = combine_hash(hashes[i], values[i].to_bits());
            }
        }
    } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        let nulls = arr.nulls();
        for i in 0..num_rows {
            if nulls.map_or(true, |n| n.is_valid(i)) {
                hashes[i] = combine_hash(hashes[i], hash_bytes(arr.value(i).as_bytes()));
            }
        }
    } else if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Date32Array>() {
        let values = arr.values();
        let nulls = arr.nulls();
        for i in 0..num_rows {
            if nulls.map_or(true, |n| n.is_valid(i)) {
                hashes[i] = combine_hash(hashes[i], values[i] as u64);
            }
        }
    } else if let Some(arr) = array.as_any().downcast_ref::<arrow::array::UInt64Array>() {
        let values = arr.values();
        let nulls = arr.nulls();
        for i in 0..num_rows {
            if nulls.map_or(true, |n| n.is_valid(i)) {
                hashes[i] = combine_hash(hashes[i], values[i]);
            }
        }
    } else if let Some(arr) = array
        .as_any()
        .downcast_ref::<arrow::array::Decimal128Array>()
    {
        let nulls = arr.nulls();
        for i in 0..num_rows {
            if nulls.map_or(true, |n| n.is_valid(i)) {
                hashes[i] = combine_hash(hashes[i], arr.value(i) as u64);
            }
        }
    } else if let Some(arr) = array.as_any().downcast_ref::<arrow::array::BooleanArray>() {
        let nulls = arr.nulls();
        for i in 0..num_rows {
            if nulls.map_or(true, |n| n.is_valid(i)) {
                hashes[i] = combine_hash(hashes[i], arr.value(i) as u64);
            }
        }
    }
    // Unknown types: hash stays unchanged (will match on equality check)
}

/// FNV-1a-style hash combine: fast and produces good distribution.
#[inline(always)]
fn combine_hash(seed: u64, value: u64) -> u64 {
    // Fibonacci hashing: multiply by golden ratio and XOR
    seed.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(value)
}

/// Hash a byte slice (for string values).
#[inline(always)]
fn hash_bytes(data: &[u8]) -> u64 {
    // FNV-1a hash
    let mut hash: u64 = 0xcbf29ce484222325;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Compare two rows across key arrays for equality (hash collision resolution).
/// Returns true if all key columns match between the two rows.
#[inline]
pub fn compare_row(
    arrays_a: &[ArrayRef],
    row_a: usize,
    arrays_b: &[ArrayRef],
    row_b: usize,
) -> bool {
    for (a, b) in arrays_a.iter().zip(arrays_b.iter()) {
        if !compare_array_values(a, row_a, b, row_b) {
            return false;
        }
    }
    true
}

/// Compare a single value between two arrays at given rows.
#[inline]
fn compare_array_values(a: &ArrayRef, row_a: usize, b: &ArrayRef, row_b: usize) -> bool {
    // Handle nulls: null != null in SQL semantics
    if a.is_null(row_a) || b.is_null(row_b) {
        return false;
    }

    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<Int64Array>(),
        b.as_any().downcast_ref::<Int64Array>(),
    ) {
        return aa.value(row_a) == bb.value(row_b);
    }

    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<arrow::array::Int32Array>(),
        b.as_any().downcast_ref::<arrow::array::Int32Array>(),
    ) {
        return aa.value(row_a) == bb.value(row_b);
    }

    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<arrow::array::Float64Array>(),
        b.as_any().downcast_ref::<arrow::array::Float64Array>(),
    ) {
        return aa.value(row_a) == bb.value(row_b);
    }

    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<StringArray>(),
        b.as_any().downcast_ref::<StringArray>(),
    ) {
        return aa.value(row_a) == bb.value(row_b);
    }

    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<arrow::array::Date32Array>(),
        b.as_any().downcast_ref::<arrow::array::Date32Array>(),
    ) {
        return aa.value(row_a) == bb.value(row_b);
    }

    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<arrow::array::UInt64Array>(),
        b.as_any().downcast_ref::<arrow::array::UInt64Array>(),
    ) {
        return aa.value(row_a) == bb.value(row_b);
    }

    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<arrow::array::Decimal128Array>(),
        b.as_any().downcast_ref::<arrow::array::Decimal128Array>(),
    ) {
        return aa.value(row_a) == bb.value(row_b);
    }

    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<arrow::array::BooleanArray>(),
        b.as_any().downcast_ref::<arrow::array::BooleanArray>(),
    ) {
        return aa.value(row_a) == bb.value(row_b);
    }

    // Cross-type comparison: Int64 vs Int32
    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<Int64Array>(),
        b.as_any().downcast_ref::<arrow::array::Int32Array>(),
    ) {
        return aa.value(row_a) == bb.value(row_b) as i64;
    }
    if let (Some(aa), Some(bb)) = (
        a.as_any().downcast_ref::<arrow::array::Int32Array>(),
        b.as_any().downcast_ref::<Int64Array>(),
    ) {
        return aa.value(row_a) as i64 == bb.value(row_b);
    }

    false
}

/// Check if any key column has a null value at the given row.
#[inline]
pub fn has_null(arrays: &[ArrayRef], row: usize) -> bool {
    arrays.iter().any(|a| a.is_null(row))
}

/// Check if all key expression types are supported for vectorized hashing.
pub fn can_vectorize_arrays(arrays: &[ArrayRef]) -> bool {
    arrays.iter().all(|arr| {
        arr.as_any().downcast_ref::<Int64Array>().is_some()
            || arr
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .is_some()
            || arr
                .as_any()
                .downcast_ref::<arrow::array::Float64Array>()
                .is_some()
            || arr.as_any().downcast_ref::<StringArray>().is_some()
            || arr
                .as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .is_some()
            || arr
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .is_some()
            || arr
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .is_some()
            || arr
                .as_any()
                .downcast_ref::<arrow::array::BooleanArray>()
                .is_some()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StringArray};
    use std::sync::Arc;

    #[test]
    fn test_hash_int64() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3, 1]));
        let hashes = hash_arrays(&[arr], 4);
        assert_eq!(hashes[0], hashes[3]); // same value → same hash
        assert_ne!(hashes[0], hashes[1]); // different values → different hash
    }

    #[test]
    fn test_hash_string() {
        let arr: ArrayRef = Arc::new(StringArray::from(vec!["hello", "world", "hello"]));
        let hashes = hash_arrays(&[arr], 3);
        assert_eq!(hashes[0], hashes[2]);
        assert_ne!(hashes[0], hashes[1]);
    }

    #[test]
    fn test_hash_multi_column() {
        let a1: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 2]));
        let a2: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "a"]));
        let hashes = hash_arrays(&[a1, a2], 3);
        // (1,"a") != (1,"b") != (2,"a")
        assert_ne!(hashes[0], hashes[1]);
        assert_ne!(hashes[0], hashes[2]);
        assert_ne!(hashes[1], hashes[2]);
    }

    #[test]
    fn test_compare_row_same() {
        let a: ArrayRef = Arc::new(Int64Array::from(vec![10, 20, 10]));
        let b: ArrayRef = Arc::new(Int64Array::from(vec![10, 30, 10]));
        assert!(compare_row(&[a.clone()], 0, &[b.clone()], 0));
        assert!(!compare_row(&[a.clone()], 1, &[b.clone()], 1));
        assert!(compare_row(&[a], 2, &[b], 2));
    }

    #[test]
    fn test_compare_row_string() {
        let a: ArrayRef = Arc::new(StringArray::from(vec!["foo", "bar"]));
        let b: ArrayRef = Arc::new(StringArray::from(vec!["foo", "baz"]));
        assert!(compare_row(&[a.clone()], 0, &[b.clone()], 0));
        assert!(!compare_row(&[a], 1, &[b], 1));
    }

    #[test]
    fn test_compare_row_multi_column() {
        let a1: ArrayRef = Arc::new(Int64Array::from(vec![1, 1]));
        let a2: ArrayRef = Arc::new(StringArray::from(vec!["x", "y"]));
        let b1: ArrayRef = Arc::new(Int64Array::from(vec![1, 1]));
        let b2: ArrayRef = Arc::new(StringArray::from(vec!["x", "z"]));
        assert!(compare_row(
            &[a1.clone(), a2.clone()],
            0,
            &[b1.clone(), b2.clone()],
            0
        ));
        assert!(!compare_row(&[a1, a2], 1, &[b1, b2], 1));
    }

    #[test]
    fn test_has_null() {
        let arr: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(3)]));
        assert!(!has_null(&[arr.clone()], 0));
        assert!(has_null(&[arr.clone()], 1));
        assert!(!has_null(&[arr], 2));
    }

    #[test]
    fn test_can_vectorize() {
        let i: ArrayRef = Arc::new(Int64Array::from(vec![1]));
        let f: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        let s: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
        let i32: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        assert!(can_vectorize_arrays(&[i, f, s, i32]));
    }
}
