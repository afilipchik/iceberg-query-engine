//! Window function execution.
//!
//! `WindowExec` computes each window expression independently: sort a
//! permutation of the input by (partition keys, order keys), evaluate the
//! function over each partition in sorted order, then scatter the results
//! back to the ORIGINAL row order. Output = every input column unchanged, in
//! input order, plus one appended column per window expression — which is
//! exactly the contract the binder's Window node declares, and it means
//! window expressions with different PARTITION BY / ORDER BY specs never
//! constrain each other.
//!
//! v1 executes on a single partition (window queries are a correctness
//! feature first; the morsel treatment is a later perf epic) and supports the
//! function set the epic has reached — everything else is refused BY NAME at
//! plan time, never silently mis-evaluated.

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Int64Array, UInt32Array};
use arrow::compute::{self, SortColumn, SortOptions};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;

use crate::error::{QueryError, Result};
use crate::physical::plan::{PhysicalOperator, RecordBatchStream};
use crate::planner::{AggregateFunction, Expr, FrameBound, FrameUnits, WindowExpr, WindowFunc};
use crate::planner::{NullOrdering, SortDirection};

use super::filter::evaluate_expr;

/// Physical window operator. See the module docs for the evaluation model.
pub struct WindowExec {
    input: Arc<dyn PhysicalOperator>,
    /// (output column name, window expression), appended in this order.
    window_exprs: Vec<(String, WindowExpr)>,
    schema: SchemaRef,
}

impl WindowExec {
    pub fn try_new(
        input: Arc<dyn PhysicalOperator>,
        window_exprs: Vec<(String, WindowExpr)>,
        schema: SchemaRef,
    ) -> Result<Self> {
        // Validate argument counts at plan time, by name.
        for (_, w) in &window_exprs {
            let arity_ok = match &w.func {
                WindowFunc::RowNumber
                | WindowFunc::Rank
                | WindowFunc::DenseRank
                | WindowFunc::PercentRank
                | WindowFunc::CumeDist => w.args.is_empty(),
                WindowFunc::Ntile => w.args.len() == 1,
                WindowFunc::Lag | WindowFunc::Lead => (1..=3).contains(&w.args.len()),
                WindowFunc::FirstValue | WindowFunc::LastValue => w.args.len() == 1,
                WindowFunc::NthValue => w.args.len() == 2,
                WindowFunc::Aggregate(_) => w.args.len() == 1,
            };
            if !arity_ok {
                return Err(QueryError::Bind(format!(
                    "wrong number of arguments for window function {}",
                    w.func
                )));
            }
        }
        Ok(Self {
            input,
            window_exprs,
            schema,
        })
    }

    /// Evaluate one window expression against the whole input, returning the
    /// result column in ORIGINAL row order.
    fn evaluate_window(batch: &RecordBatch, w: &WindowExpr) -> Result<ArrayRef> {
        let n = batch.num_rows();

        // Sort permutation: partition keys first (ascending, arbitrary but
        // consistent), then the window's ORDER BY keys.
        let mut sort_columns: Vec<SortColumn> = Vec::new();
        let mut partition_arrays: Vec<ArrayRef> = Vec::new();
        for p in &w.partition_by {
            let values = evaluate_expr(batch, p)?;
            partition_arrays.push(values.clone());
            sort_columns.push(SortColumn {
                values,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            });
        }
        let mut order_arrays: Vec<ArrayRef> = Vec::new();
        for o in &w.order_by {
            let values = evaluate_expr(batch, &o.expr)?;
            order_arrays.push(values.clone());
            sort_columns.push(SortColumn {
                values,
                options: Some(SortOptions {
                    descending: o.direction == SortDirection::Desc,
                    nulls_first: matches!(o.nulls, NullOrdering::NullsFirst),
                }),
            });
        }

        // indices[i] = original row at sorted position i. With no keys at
        // all (OVER ()), the identity permutation avoids a useless sort.
        let indices: UInt32Array = if sort_columns.is_empty() {
            (0..n as u32).collect::<Vec<_>>().into()
        } else {
            compute::lexsort_to_indices(&sort_columns, None)?
        };
        let take_sorted = |a: &ArrayRef| -> Result<ArrayRef> {
            compute::take(a.as_ref(), &indices, None).map_err(Into::into)
        };

        // Partition ranges over the SORTED order, and peer ranges (rows equal
        // on partition AND order keys — the standard's "peers").
        let sorted_partition: Vec<ArrayRef> = partition_arrays
            .iter()
            .map(take_sorted)
            .collect::<Result<_>>()?;
        let sorted_order: Vec<ArrayRef> = order_arrays
            .iter()
            .map(take_sorted)
            .collect::<Result<_>>()?;
        let partitions: Vec<std::ops::Range<usize>> = if sorted_partition.is_empty() {
            vec![0..n]
        } else {
            arrow::compute::kernels::partition::partition(&sorted_partition)?
                .ranges()
                .to_vec()
        };
        let peer_keys: Vec<ArrayRef> = sorted_partition
            .iter()
            .chain(sorted_order.iter())
            .cloned()
            .collect();
        let peers: Vec<std::ops::Range<usize>> = if peer_keys.is_empty() {
            vec![0..n]
        } else {
            arrow::compute::kernels::partition::partition(&peer_keys)?
                .ranges()
                .to_vec()
        };
        // peer_of[i] = index into `peers` for sorted row i.
        let mut peer_of = vec![0usize; n];
        for (gi, r) in peers.iter().enumerate() {
            for slot in &mut peer_of[r.clone()] {
                *slot = gi;
            }
        }

        // Argument columns in sorted order. `None` marks COUNT(*)'s wildcard.
        let sorted_args: Vec<Option<ArrayRef>> = w
            .args
            .iter()
            .map(|a| match a {
                Expr::Wildcard => Ok(None),
                e => evaluate_expr(batch, e)
                    .and_then(|v| take_sorted(&v))
                    .map(Some),
            })
            .collect::<Result<_>>()?;

        let ctx = SortedInput {
            n,
            partitions: &partitions,
            peers: &peers,
            peer_of: &peer_of,
            args: &sorted_args,
            order: &sorted_order,
            w,
        };
        let sorted_values = evaluate_sorted(&ctx)?;

        // Scatter back: out[indices[i]] = v[i].
        let mut inverse = vec![0u32; n];
        for (sorted_pos, orig) in indices.values().iter().enumerate() {
            inverse[*orig as usize] = sorted_pos as u32;
        }
        let inverse = UInt32Array::from(inverse);
        compute::take(sorted_values.as_ref(), &inverse, None).map_err(Into::into)
    }
}

/// Everything an evaluator needs, all in SORTED order.
struct SortedInput<'a> {
    n: usize,
    partitions: &'a [std::ops::Range<usize>],
    peers: &'a [std::ops::Range<usize>],
    /// peer group index of each sorted row
    peer_of: &'a [usize],
    /// argument columns (None = `*`)
    args: &'a [Option<ArrayRef>],
    /// ORDER BY key columns (used by RANGE offset frames)
    order: &'a [ArrayRef],
    w: &'a WindowExpr,
}

/// The frame of sorted row `i` within its partition, as a sorted-index range.
/// Empty ranges are legal (frame entirely outside the partition).
fn frame_range(
    ctx: &SortedInput<'_>,
    part: &std::ops::Range<usize>,
    i: usize,
) -> Result<std::ops::Range<usize>> {
    let f = &ctx.w.frame;
    let (ps, pe) = (part.start, part.end);
    let peer = &ctx.peers[ctx.peer_of[i]];
    let (start, end) = match f.units {
        FrameUnits::Rows => {
            let start = match &f.start {
                FrameBound::UnboundedPreceding => ps,
                FrameBound::Preceding(k) => i.saturating_sub(*k as usize).max(ps),
                FrameBound::CurrentRow => i,
                FrameBound::Following(k) => (i + *k as usize).min(pe),
                FrameBound::UnboundedFollowing => unreachable!("rejected at bind"),
            };
            let end = match &f.end {
                FrameBound::UnboundedPreceding => unreachable!("rejected at bind"),
                FrameBound::Preceding(k) => (i + 1).saturating_sub(*k as usize).max(ps),
                FrameBound::CurrentRow => i + 1,
                FrameBound::Following(k) => (i + 1 + *k as usize).min(pe),
                FrameBound::UnboundedFollowing => pe,
            };
            (start, end)
        }
        FrameUnits::Range => {
            let start = match &f.start {
                FrameBound::UnboundedPreceding => ps,
                FrameBound::CurrentRow => peer.start,
                FrameBound::Preceding(k) => range_offset_bound(ctx, part, i, *k, true)?,
                FrameBound::Following(k) => range_offset_bound(ctx, part, i, *k, false)?,
                FrameBound::UnboundedFollowing => unreachable!("rejected at bind"),
            };
            let end = match &f.end {
                FrameBound::UnboundedPreceding => unreachable!("rejected at bind"),
                FrameBound::CurrentRow => peer.end,
                FrameBound::Preceding(k) => range_offset_end(ctx, part, i, *k, true)?,
                FrameBound::Following(k) => range_offset_end(ctx, part, i, *k, false)?,
                FrameBound::UnboundedFollowing => pe,
            };
            (start, end)
        }
    };
    Ok(start.max(ps)..end.clamp(ps, pe).max(start.max(ps)))
}

/// The order key of sorted row `i` as f64, for RANGE offset arithmetic.
/// Supported key types are numeric and date; anything else was refused.
fn range_key(arr: &ArrayRef, i: usize) -> Option<f64> {
    use arrow::array::*;
    use arrow::datatypes::DataType;
    if arr.is_null(i) {
        return None;
    }
    match arr.data_type() {
        DataType::Int8 => Some(arr.as_any().downcast_ref::<Int8Array>()?.value(i) as f64),
        DataType::Int16 => Some(arr.as_any().downcast_ref::<Int16Array>()?.value(i) as f64),
        DataType::Int32 => Some(arr.as_any().downcast_ref::<Int32Array>()?.value(i) as f64),
        DataType::Int64 => Some(arr.as_any().downcast_ref::<Int64Array>()?.value(i) as f64),
        DataType::UInt32 => Some(arr.as_any().downcast_ref::<UInt32Array>()?.value(i) as f64),
        DataType::UInt64 => Some(arr.as_any().downcast_ref::<UInt64Array>()?.value(i) as f64),
        DataType::Float32 => Some(arr.as_any().downcast_ref::<Float32Array>()?.value(i) as f64),
        DataType::Float64 => Some(arr.as_any().downcast_ref::<Float64Array>()?.value(i)),
        DataType::Date32 => Some(arr.as_any().downcast_ref::<Date32Array>()?.value(i) as f64),
        _ => None,
    }
}

/// Start bound for `k PRECEDING/FOLLOWING` in RANGE mode: the first sorted
/// index in the partition whose key is inside the bound.
fn range_offset_bound(
    ctx: &SortedInput<'_>,
    part: &std::ops::Range<usize>,
    i: usize,
    k: u64,
    preceding: bool,
) -> Result<usize> {
    let (arr, desc) = range_frame_key(ctx)?;
    let Some(cur) = range_key(arr, i) else {
        // NULL order key: the standard makes NULLs peers; the frame is the
        // peer group of NULLs.
        return Ok(ctx.peers[ctx.peer_of[i]].start);
    };
    // Threshold value in sort direction; scan from partition start for the
    // first row satisfying it. O(partition) worst case — correctness first.
    let limit = if preceding == !desc {
        cur - k as f64
    } else {
        cur + k as f64
    };
    for j in part.clone() {
        match range_key(arr, j) {
            None => continue,
            Some(v) => {
                let inside = if !desc { v >= limit } else { v <= limit };
                if inside {
                    return Ok(j);
                }
            }
        }
    }
    Ok(part.end)
}

/// End bound (exclusive) for `k PRECEDING/FOLLOWING` in RANGE mode.
fn range_offset_end(
    ctx: &SortedInput<'_>,
    part: &std::ops::Range<usize>,
    i: usize,
    k: u64,
    preceding: bool,
) -> Result<usize> {
    let (arr, desc) = range_frame_key(ctx)?;
    let Some(cur) = range_key(arr, i) else {
        return Ok(ctx.peers[ctx.peer_of[i]].end);
    };
    let limit = if preceding == !desc {
        cur - k as f64
    } else {
        cur + k as f64
    };
    let mut end = part.start;
    for j in part.clone() {
        match range_key(arr, j) {
            None => continue,
            Some(v) => {
                let inside = if !desc { v <= limit } else { v >= limit };
                if inside {
                    end = j + 1;
                }
            }
        }
    }
    Ok(end)
}

/// The single numeric/date ORDER BY key a RANGE-with-offset frame requires.
fn range_frame_key<'a>(ctx: &'a SortedInput<'_>) -> Result<(&'a ArrayRef, bool)> {
    if ctx.order.len() != 1 {
        return Err(QueryError::NotImplemented(
            "RANGE frames with offsets require exactly one ORDER BY key".into(),
        ));
    }
    let arr = &ctx.order[0];
    if range_key_supported(arr) {
        Ok((arr, ctx.w.order_by[0].direction == SortDirection::Desc))
    } else {
        Err(QueryError::NotImplemented(format!(
            "RANGE frames with offsets over a {:?} ORDER BY key",
            arr.data_type()
        )))
    }
}

fn range_key_supported(arr: &ArrayRef) -> bool {
    use arrow::datatypes::DataType::*;
    matches!(
        arr.data_type(),
        Int8 | Int16 | Int32 | Int64 | UInt32 | UInt64 | Float32 | Float64 | Date32
    )
}

/// A positive integer literal argument (NTILE bucket count, LAG offset...).
fn literal_int_arg(w: &WindowExpr, idx: usize, what: &str) -> Result<i64> {
    match w.args.get(idx) {
        Some(Expr::Literal(v)) => {
            let n = match v {
                crate::planner::ScalarValue::Int8(x) => *x as i64,
                crate::planner::ScalarValue::Int16(x) => *x as i64,
                crate::planner::ScalarValue::Int32(x) => *x as i64,
                crate::planner::ScalarValue::Int64(x) => *x,
                other => {
                    return Err(QueryError::Bind(format!(
                        "{what} must be an integer literal, got {other}"
                    )))
                }
            };
            Ok(n)
        }
        Some(other) => Err(QueryError::NotImplemented(format!(
            "non-literal {what} ({other})"
        ))),
        None => unreachable!("arity validated"),
    }
}

/// Build an output column by TAKING per-row source indices from `values` —
/// the generic path for the navigation family, typed for free by arrow.
/// `idx[i] = None` produces NULL.
fn take_output(values: &ArrayRef, idx: Vec<Option<u32>>) -> Result<ArrayRef> {
    let indices = UInt32Array::from(idx);
    compute::take(values.as_ref(), &indices, None).map_err(Into::into)
}

/// Compute the window function over each partition of the sorted input,
/// producing values in SORTED order.
fn evaluate_sorted(ctx: &SortedInput<'_>) -> Result<ArrayRef> {
    use arrow::array::Float64Array;
    let n = ctx.n;
    let w = ctx.w;
    match &w.func {
        WindowFunc::RowNumber => {
            let mut out = vec![0i64; n];
            for range in ctx.partitions {
                for (i, slot) in out[range.clone()].iter_mut().enumerate() {
                    *slot = (i + 1) as i64;
                }
            }
            Ok(Arc::new(Int64Array::from(out)))
        }
        WindowFunc::Rank => {
            let mut out = vec![0i64; n];
            for part in ctx.partitions {
                for i in part.clone() {
                    out[i] = (ctx.peers[ctx.peer_of[i]].start - part.start) as i64 + 1;
                }
            }
            Ok(Arc::new(Int64Array::from(out)))
        }
        WindowFunc::DenseRank => {
            let mut out = vec![0i64; n];
            for part in ctx.partitions {
                let mut dense = 0i64;
                let mut last_peer = usize::MAX;
                for i in part.clone() {
                    if ctx.peer_of[i] != last_peer {
                        dense += 1;
                        last_peer = ctx.peer_of[i];
                    }
                    out[i] = dense;
                }
            }
            Ok(Arc::new(Int64Array::from(out)))
        }
        WindowFunc::PercentRank => {
            let mut out = vec![0f64; n];
            for part in ctx.partitions {
                let rows = part.len();
                for i in part.clone() {
                    let rank = (ctx.peers[ctx.peer_of[i]].start - part.start) as f64 + 1.0;
                    out[i] = if rows <= 1 {
                        0.0
                    } else {
                        (rank - 1.0) / (rows as f64 - 1.0)
                    };
                }
            }
            Ok(Arc::new(Float64Array::from(out)))
        }
        WindowFunc::CumeDist => {
            let mut out = vec![0f64; n];
            for part in ctx.partitions {
                let rows = part.len() as f64;
                for i in part.clone() {
                    out[i] = (ctx.peers[ctx.peer_of[i]].end - part.start) as f64 / rows;
                }
            }
            Ok(Arc::new(Float64Array::from(out)))
        }
        WindowFunc::Ntile => {
            let buckets = literal_int_arg(w, 0, "NTILE bucket count")?;
            if buckets <= 0 {
                return Err(QueryError::Bind(format!(
                    "NTILE bucket count must be positive, got {buckets}"
                )));
            }
            let buckets = buckets as usize;
            let mut out = vec![0i64; n];
            for part in ctx.partitions {
                let m = part.len();
                let size = m / buckets;
                let rem = m % buckets;
                for (pos, i) in part.clone().enumerate() {
                    let big = rem * (size + 1);
                    out[i] = if size == 0 {
                        (pos + 1) as i64
                    } else if pos < big {
                        (pos / (size + 1)) as i64 + 1
                    } else {
                        (rem + (pos - big) / size) as i64 + 1
                    };
                }
            }
            Ok(Arc::new(Int64Array::from(out)))
        }
        WindowFunc::Lag | WindowFunc::Lead => {
            let values = ctx.args[0]
                .as_ref()
                .ok_or_else(|| QueryError::Bind("LAG/LEAD needs a value argument".into()))?;
            let offset = if w.args.len() >= 2 {
                let o = literal_int_arg(w, 1, "LAG/LEAD offset")?;
                if o < 0 {
                    return Err(QueryError::Bind(format!(
                        "LAG/LEAD offset must be non-negative, got {o}"
                    )));
                }
                o as usize
            } else {
                1
            };
            let lead = matches!(w.func, WindowFunc::Lead);
            let mut idx: Vec<Option<u32>> = vec![None; n];
            for part in ctx.partitions {
                for i in part.clone() {
                    let src = if lead {
                        let j = i + offset;
                        (j < part.end).then_some(j)
                    } else {
                        i.checked_sub(offset).filter(|j| *j >= part.start)
                    };
                    idx[i] = src.map(|j| j as u32);
                }
            }
            let taken = take_output(values, idx.clone())?;
            if w.args.len() == 3 {
                // Default value where the offset fell outside the partition.
                let default_arr = ctx.args[2]
                    .as_ref()
                    .ok_or_else(|| QueryError::Bind("LAG/LEAD default cannot be *".into()))?;
                let default_arr = compute::cast(default_arr.as_ref(), taken.data_type())?;
                let in_partition: arrow::array::BooleanArray =
                    idx.iter().map(|s| Some(s.is_some())).collect();
                return arrow::compute::kernels::zip::zip(&in_partition, &taken, &default_arr)
                    .map_err(Into::into);
            }
            Ok(taken)
        }
        WindowFunc::FirstValue | WindowFunc::LastValue | WindowFunc::NthValue => {
            let values = ctx.args[0].as_ref().ok_or_else(|| {
                QueryError::Bind("value window function needs an argument".into())
            })?;
            let nth = if matches!(w.func, WindowFunc::NthValue) {
                let k = literal_int_arg(w, 1, "NTH_VALUE position")?;
                if k <= 0 {
                    return Err(QueryError::Bind(format!(
                        "NTH_VALUE position must be positive, got {k}"
                    )));
                }
                Some(k as usize)
            } else {
                None
            };
            let mut idx: Vec<Option<u32>> = vec![None; n];
            for part in ctx.partitions {
                for i in part.clone() {
                    let f = frame_range(ctx, part, i)?;
                    let src = if f.is_empty() {
                        None
                    } else {
                        match (&w.func, nth) {
                            (WindowFunc::FirstValue, _) => Some(f.start),
                            (WindowFunc::LastValue, _) => Some(f.end - 1),
                            (WindowFunc::NthValue, Some(k)) => {
                                let j = f.start + (k - 1);
                                (j < f.end).then_some(j)
                            }
                            _ => unreachable!(),
                        }
                    };
                    idx[i] = src.map(|j| j as u32);
                }
            }
            take_output(values, idx)
        }
        WindowFunc::Aggregate(func) => evaluate_window_aggregate(ctx, func),
    }
}

/// Framed aggregates. COUNT/SUM/AVG run on O(1)-per-row prefix sums; MIN/MAX
/// recompute their frame slice through arrow's kernels.
fn evaluate_window_aggregate(ctx: &SortedInput<'_>, func: &AggregateFunction) -> Result<ArrayRef> {
    use arrow::array::Float64Array;
    let n = ctx.n;
    let values = ctx.args[0].as_ref();

    match func {
        AggregateFunction::Count => {
            let mut out = vec![0i64; n];
            // prefix[i] = non-null count (or row count for *) before sorted i.
            let mut prefix = vec![0i64; n + 1];
            for i in 0..n {
                let c = match values {
                    None => 1,
                    Some(v) => (!v.is_null(i)) as i64,
                };
                prefix[i + 1] = prefix[i] + c;
            }
            for part in ctx.partitions {
                for i in part.clone() {
                    let f = frame_range(ctx, part, i)?;
                    out[i] = prefix[f.end] - prefix[f.start];
                }
            }
            Ok(Arc::new(Int64Array::from(out)))
        }
        AggregateFunction::Sum | AggregateFunction::Avg => {
            let values = values.ok_or_else(|| {
                QueryError::Bind(format!("{func} window aggregate needs a column argument"))
            })?;
            let as_f64 = compute::cast(values.as_ref(), &arrow::datatypes::DataType::Float64)?;
            let f64s = as_f64
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("cast to f64");
            let mut prefix_sum = vec![0f64; n + 1];
            let mut prefix_cnt = vec![0i64; n + 1];
            for i in 0..n {
                let (s, c) = if f64s.is_null(i) {
                    (0.0, 0)
                } else {
                    (f64s.value(i), 1)
                };
                prefix_sum[i + 1] = prefix_sum[i] + s;
                prefix_cnt[i + 1] = prefix_cnt[i] + c;
            }
            let mut out: Vec<Option<f64>> = vec![None; n];
            for part in ctx.partitions {
                for i in part.clone() {
                    let f = frame_range(ctx, part, i)?;
                    let cnt = prefix_cnt[f.end] - prefix_cnt[f.start];
                    if cnt > 0 {
                        let sum = prefix_sum[f.end] - prefix_sum[f.start];
                        out[i] = Some(if matches!(func, AggregateFunction::Avg) {
                            sum / cnt as f64
                        } else {
                            sum
                        });
                    }
                }
            }
            let arr: ArrayRef = Arc::new(Float64Array::from(out));
            // SUM keeps the engine's aggregate output type (e.g. Int64 sums);
            // AVG is always Float64.
            if matches!(func, AggregateFunction::Sum) {
                let target = match values.data_type() {
                    t if t.is_integer() => arrow::datatypes::DataType::Int64,
                    _ => arrow::datatypes::DataType::Float64,
                };
                return compute::cast(arr.as_ref(), &target).map_err(Into::into);
            }
            Ok(arr)
        }
        AggregateFunction::Min | AggregateFunction::Max => {
            let values = values.ok_or_else(|| {
                QueryError::Bind(format!("{func} window aggregate needs a column argument"))
            })?;
            // The frame slice re-sorted per row would be wasteful; instead
            // pick the extreme via take: for each row find argmin/argmax by
            // scanning the frame. O(n * frame) — correctness first.
            let mut idx: Vec<Option<u32>> = vec![None; n];
            for part in ctx.partitions {
                for i in part.clone() {
                    let f = frame_range(ctx, part, i)?;
                    if f.is_empty() {
                        continue;
                    }
                    let slice = values.slice(f.start, f.end - f.start);
                    let extreme = if matches!(func, AggregateFunction::Min) {
                        arrow::compute::kernels::sort::sort_to_indices(
                            slice.as_ref(),
                            Some(SortOptions {
                                descending: false,
                                nulls_first: false,
                            }),
                            Some(1),
                        )?
                    } else {
                        arrow::compute::kernels::sort::sort_to_indices(
                            slice.as_ref(),
                            Some(SortOptions {
                                descending: true,
                                nulls_first: false,
                            }),
                            Some(1),
                        )?
                    };
                    if extreme.len() == 1 {
                        let j = f.start + extreme.value(0) as usize;
                        if !values.is_null(j) {
                            idx[i] = Some(j as u32);
                        }
                    }
                }
            }
            take_output(values, idx)
        }
        other => Err(QueryError::NotImplemented(format!(
            "window aggregate {other} OVER (...)"
        ))),
    }
}

impl std::fmt::Debug for WindowExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WindowExec [{}]",
            self.window_exprs
                .iter()
                .map(|(n, w)| format!("{n}: {w}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[async_trait]
impl PhysicalOperator for WindowExec {
    fn name(&self) -> &str {
        "WindowExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.input.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        crate::physical::check_partition(self, partition)?;

        let (all_batches, _) =
            crate::physical::operators::spillable::collect_input_partitions_concurrently(
                &self.input,
            )
            .await?;

        if all_batches.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        // One contiguous batch; dictionary-encoded columns are normalized to
        // plain when batches disagree (same situation SortExec handles — the
        // declared schema is what the plan promised upward).
        let input_arrow_fields =
            self.schema.fields()[..self.schema.fields().len() - self.window_exprs.len()].to_vec();
        let input_schema = Arc::new(arrow::datatypes::Schema::new(input_arrow_fields));
        let normalized: Vec<RecordBatch> = all_batches
            .into_iter()
            .map(|b| {
                let cols: std::result::Result<Vec<ArrayRef>, arrow::error::ArrowError> = b
                    .columns()
                    .iter()
                    .map(|c| match c.data_type() {
                        arrow::datatypes::DataType::Dictionary(_, v) => {
                            compute::cast(c.as_ref(), v)
                        }
                        _ => Ok(c.clone()),
                    })
                    .collect();
                RecordBatch::try_new(input_schema.clone(), cols?).map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = if normalized.len() == 1 {
            normalized.into_iter().next().expect("checked")
        } else {
            compute::concat_batches(&input_schema, &normalized)?
        };

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        for (_, w) in &self.window_exprs {
            columns.push(Self::evaluate_window(&batch, w)?);
        }
        let out = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(Box::pin(stream::iter(vec![Ok(out)])))
    }
}
