//! Vector distance kernels and the k-NN pushdown request type.
//!
//! # Sign conventions (fixed, and relied on by the pushdown rule)
//!
//! | SQL function        | formula                       | closer means |
//! |---------------------|-------------------------------|--------------|
//! | `l2_distance`       | `sqrt(sum((a-b)^2))`          | **smaller**  |
//! | `cosine_distance`   | `1 - dot(a,b)/(|a|*|b|)`      | **smaller**  |
//! | `cosine_similarity` | `dot(a,b)/(|a|*|b|)`          | larger       |
//! | `dot_product`       | `sum(a*b)`                    | larger       |
//!
//! `dot_product` is deliberately *not* dressed up as a distance by negating it
//! or subtracting it from one. Lance internally stores "dot distance" as
//! `1 - dot`, and pgvector exposes `<#>` as `-dot`; both are conventions that
//! silently change the meaning of the number a user sees. Here the function
//! returns the inner product, and the pushdown rule handles the mapping to
//! Lance's convention itself (see `VectorMetric::from_distance_fn`).
//!
//! `l2_distance` takes the square root, unlike Lance's internal L2 which is the
//! *squared* distance. Ordering is unaffected (sqrt is monotone on the
//! non-negatives) so pushdown is still valid, but any distance value surfaced
//! from an index must be square-rooted before it is shown to the user.
//!
//! # Why the kernels read the values buffer directly
//!
//! A `FixedSizeList<Float32, 384>` column stores its 384 floats per row in one
//! contiguous child buffer. Building a `Vec<f32>` per row to compute a distance
//! would allocate once per row — 200,000 allocations for one scan of the test
//! dataset — and destroy the auto-vectorization. Every kernel below takes
//! `&[f32]` slices carved out of that single buffer.

use crate::error::{QueryError, Result};
use arrow::array::{Array, ArrayRef, FixedSizeListArray, Float32Array, Float64Array};
use arrow::datatypes::DataType;

/// Borrow a fixed-size-list column as `(flat values, dimension)`.
///
/// Returns `None` when the array is not a float vector column. The returned
/// slice is the whole contiguous child buffer: row `i` occupies
/// `values[i * dim .. (i + 1) * dim]`.
pub fn as_f32_vectors(array: &ArrayRef) -> Option<(&[f32], usize)> {
    let list = array.as_any().downcast_ref::<FixedSizeListArray>()?;
    let dim = list.value_length() as usize;
    // `values()` is already offset-adjusted: FixedSizeListArray::slice slices
    // the child buffer alongside the parent.
    let child = list.values();
    let floats = child.as_any().downcast_ref::<Float32Array>()?;
    Some((floats.values(), dim))
}

/// Same as [`as_f32_vectors`] but for `Float64` element type.
fn as_f64_vectors(array: &ArrayRef) -> Option<(&[f64], usize)> {
    let list = array.as_any().downcast_ref::<FixedSizeListArray>()?;
    let dim = list.value_length() as usize;
    let child = list.values();
    let floats = child.as_any().downcast_ref::<Float64Array>()?;
    Some((floats.values(), dim))
}

/// Which distance the caller wants from [`distance_column`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DistanceKind {
    L2,
    Cosine,
    CosineSimilarity,
    Dot,
}

#[inline]
fn dot(a: &[f32], b: &[f32]) -> f64 {
    // Chunked accumulation so LLVM can keep several independent FMA chains in
    // flight; a single `sum` accumulator serializes on the add latency.
    let mut acc = [0f32; 8];
    let mut ai = a.chunks_exact(8);
    let mut bi = b.chunks_exact(8);
    for (x, y) in ai.by_ref().zip(bi.by_ref()) {
        for i in 0..8 {
            acc[i] += x[i] * y[i];
        }
    }
    let mut s: f64 = acc.iter().map(|v| *v as f64).sum();
    for (x, y) in ai.remainder().iter().zip(bi.remainder()) {
        s += (*x * *y) as f64;
    }
    s
}

#[inline]
fn l2_sq(a: &[f32], b: &[f32]) -> f64 {
    let mut acc = [0f32; 8];
    let mut ai = a.chunks_exact(8);
    let mut bi = b.chunks_exact(8);
    for (x, y) in ai.by_ref().zip(bi.by_ref()) {
        for i in 0..8 {
            let d = x[i] - y[i];
            acc[i] += d * d;
        }
    }
    let mut s: f64 = acc.iter().map(|v| *v as f64).sum();
    for (x, y) in ai.remainder().iter().zip(bi.remainder()) {
        let d = *x - *y;
        s += (d * d) as f64;
    }
    s
}

#[inline]
fn norm(a: &[f32]) -> f64 {
    dot(a, a).sqrt()
}

/// Compute `kind(column_row_i, query)` for every row, as Float64.
///
/// Rows whose vector is NULL produce NULL.
pub fn distance_column(
    column: &ArrayRef,
    query: &[f32],
    kind: DistanceKind,
    column_name: &str,
) -> Result<ArrayRef> {
    let list = column
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| {
            QueryError::Type(format!(
                "vector distance requires a fixed-size vector column, but `{}` has type {:?}",
                column_name,
                column.data_type()
            ))
        })?;

    let dim = list.value_length() as usize;
    if dim != query.len() {
        return Err(QueryError::Type(format!(
            "vector dimension mismatch: column `{}` has {} dimensions but the \
             query vector has {}",
            column_name,
            dim,
            query.len()
        )));
    }

    // Materialize the column's floats once. Float64 columns are converted here
    // (a rare case), Float32 columns are borrowed with zero copies.
    let owned_f32: Option<Vec<f32>> = match list.values().data_type() {
        DataType::Float32 => None,
        DataType::Float64 => as_f64_vectors(column)
            .map(|(vals, _)| vals.iter().map(|v| *v as f32).collect::<Vec<f32>>()),
        other => {
            return Err(QueryError::Type(format!(
                "vector distance requires float elements, but `{}` holds {:?}",
                column_name, other
            )))
        }
    };
    let flat: &[f32] = match &owned_f32 {
        Some(v) => v.as_slice(),
        None => as_f32_vectors(column).map(|(v, _)| v).ok_or_else(|| {
            QueryError::Internal(format!("vector column `{}` lost its buffer", column_name))
        })?,
    };

    let n = list.len();
    if flat.len() < n * dim {
        return Err(QueryError::Internal(format!(
            "vector column `{}`: buffer holds {} floats, expected {}",
            column_name,
            flat.len(),
            n * dim
        )));
    }

    let q_norm = match kind {
        DistanceKind::Cosine | DistanceKind::CosineSimilarity => norm(query),
        _ => 0.0,
    };

    let mut out: Vec<Option<f64>> = Vec::with_capacity(n);
    for i in 0..n {
        if list.is_null(i) {
            out.push(None);
            continue;
        }
        let row = &flat[i * dim..(i + 1) * dim];
        let v = match kind {
            DistanceKind::L2 => l2_sq(row, query).sqrt(),
            DistanceKind::Dot => dot(row, query),
            DistanceKind::Cosine | DistanceKind::CosineSimilarity => {
                let denom = norm(row) * q_norm;
                let sim = if denom == 0.0 {
                    0.0
                } else {
                    dot(row, query) / denom
                };
                if kind == DistanceKind::Cosine {
                    1.0 - sim
                } else {
                    sim
                }
            }
        };
        out.push(Some(v));
    }
    Ok(std::sync::Arc::new(Float64Array::from(out)))
}

/// Row-wise distance between two vector columns of the same dimension.
pub fn distance_columns(left: &ArrayRef, right: &ArrayRef, kind: DistanceKind) -> Result<ArrayRef> {
    let (lv, ldim) = as_f32_vectors(left).ok_or_else(|| {
        QueryError::Type(format!(
            "vector distance requires Float32 vector columns, got {:?}",
            left.data_type()
        ))
    })?;
    let (rv, rdim) = as_f32_vectors(right).ok_or_else(|| {
        QueryError::Type(format!(
            "vector distance requires Float32 vector columns, got {:?}",
            right.data_type()
        ))
    })?;
    if ldim != rdim {
        return Err(QueryError::Type(format!(
            "vector dimension mismatch: {} vs {}",
            ldim, rdim
        )));
    }
    let n = left.len().min(right.len());
    let mut out: Vec<Option<f64>> = Vec::with_capacity(n);
    for i in 0..n {
        if left.is_null(i) || right.is_null(i) {
            out.push(None);
            continue;
        }
        let a = &lv[i * ldim..(i + 1) * ldim];
        let b = &rv[i * rdim..(i + 1) * rdim];
        let v = match kind {
            DistanceKind::L2 => l2_sq(a, b).sqrt(),
            DistanceKind::Dot => dot(a, b),
            DistanceKind::Cosine | DistanceKind::CosineSimilarity => {
                let denom = norm(a) * norm(b);
                let sim = if denom == 0.0 { 0.0 } else { dot(a, b) / denom };
                if kind == DistanceKind::Cosine {
                    1.0 - sim
                } else {
                    sim
                }
            }
        };
        out.push(Some(v));
    }
    Ok(std::sync::Arc::new(Float64Array::from(out)))
}

/// Extract a `f32` query vector from a `ScalarValue::List` literal.
pub fn query_vector_from_scalar(v: &crate::planner::ScalarValue) -> Option<Vec<f32>> {
    use crate::planner::ScalarValue as SV;
    let SV::List(values, _) = v else {
        return None;
    };
    let mut out = Vec::with_capacity(values.len());
    for e in values {
        let f = match e {
            SV::Float32(f) => f.0 as f64,
            SV::Float64(f) => f.0,
            SV::Int8(i) => *i as f64,
            SV::Int16(i) => *i as f64,
            SV::Int32(i) => *i as f64,
            SV::Int64(i) => *i as f64,
            SV::UInt8(i) => *i as f64,
            SV::UInt16(i) => *i as f64,
            SV::UInt32(i) => *i as f64,
            SV::UInt64(i) => *i as f64,
            SV::Decimal128(d) => {
                use rust_decimal::prelude::ToPrimitive;
                d.to_f64()?
            }
            _ => return None,
        };
        out.push(f as f32);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Float32Builder;
    use arrow::datatypes::Field;
    use std::sync::Arc;

    fn vec_array(rows: &[&[f32]]) -> ArrayRef {
        let dim = rows[0].len();
        let mut b = Float32Builder::new();
        for r in rows {
            for v in *r {
                b.append_value(*v);
            }
        }
        let child = Arc::new(b.finish());
        Arc::new(
            FixedSizeListArray::try_new(
                Arc::new(Field::new("item", DataType::Float32, true)),
                dim as i32,
                child,
                None,
            )
            .unwrap(),
        )
    }

    fn vals(a: &ArrayRef) -> Vec<f64> {
        a.as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap())
            .collect()
    }

    #[test]
    fn l2_matches_hand_computation() {
        let col = vec_array(&[&[0.0, 0.0], &[3.0, 4.0]]);
        let d = distance_column(&col, &[0.0, 0.0], DistanceKind::L2, "v").unwrap();
        let got = vals(&d);
        assert!((got[0] - 0.0).abs() < 1e-9);
        assert!((got[1] - 5.0).abs() < 1e-6, "{:?}", got);
    }

    #[test]
    fn cosine_distance_is_one_minus_similarity() {
        let col = vec_array(&[&[1.0, 0.0], &[0.0, 1.0], &[-1.0, 0.0]]);
        let d = vals(&distance_column(&col, &[1.0, 0.0], DistanceKind::Cosine, "v").unwrap());
        assert!((d[0] - 0.0).abs() < 1e-6, "{:?}", d);
        assert!((d[1] - 1.0).abs() < 1e-6, "{:?}", d);
        assert!((d[2] - 2.0).abs() < 1e-6, "{:?}", d);
        let s =
            vals(&distance_column(&col, &[1.0, 0.0], DistanceKind::CosineSimilarity, "v").unwrap());
        for i in 0..3 {
            assert!((s[i] - (1.0 - d[i])).abs() < 1e-9);
        }
    }

    #[test]
    fn dot_product_is_not_negated() {
        let col = vec_array(&[&[1.0, 2.0], &[-1.0, -2.0]]);
        let d = vals(&distance_column(&col, &[3.0, 4.0], DistanceKind::Dot, "v").unwrap());
        assert!((d[0] - 11.0).abs() < 1e-6, "{:?}", d);
        assert!((d[1] + 11.0).abs() < 1e-6, "{:?}", d);
    }

    #[test]
    fn dimension_mismatch_is_an_error_naming_the_column() {
        let col = vec_array(&[&[1.0, 2.0]]);
        let err = distance_column(&col, &[1.0, 2.0, 3.0], DistanceKind::L2, "embedding")
            .unwrap_err()
            .to_string();
        assert!(err.contains("embedding"), "{}", err);
        assert!(err.contains("dimension"), "{}", err);
    }

    #[test]
    fn sliced_batches_read_the_right_rows() {
        // The values buffer is shared; a sliced batch must not read row 0.
        let col = vec_array(&[&[1.0, 0.0], &[0.0, 1.0], &[10.0, 10.0]]);
        let sliced = col.slice(2, 1);
        let d = vals(&distance_column(&sliced, &[10.0, 10.0], DistanceKind::L2, "v").unwrap());
        assert_eq!(d.len(), 1);
        assert!(d[0].abs() < 1e-6, "sliced read wrong row: {:?}", d);
    }

    #[test]
    fn long_vectors_use_the_chunked_path() {
        // 384 is not a multiple of 8's remainder path; 385 exercises it.
        for dim in [384usize, 385] {
            let a: Vec<f32> = (0..dim).map(|i| (i % 7) as f32).collect();
            let b: Vec<f32> = (0..dim).map(|i| (i % 5) as f32).collect();
            let expected: f64 = a.iter().zip(&b).map(|(x, y)| (*x * *y) as f64).sum();
            assert!((dot(&a, &b) - expected).abs() < 1e-3, "dim {}", dim);
        }
    }
}
