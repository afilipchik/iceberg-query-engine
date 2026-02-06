//! Morsel-driven aggregation execution
//!
//! Implements parallel aggregation using morsel-driven parallelism:
//! - Data is read in parallel from Parquet files
//! - Each thread maintains its own hash table
//! - Final merge combines all thread-local hash tables

use crate::error::{QueryError, Result};
use crate::physical::morsel::{ParallelParquetSource, DEFAULT_MORSEL_SIZE};
use crate::physical::operators::evaluate_expr;
use crate::planner::{AggregateFunction, Expr, ScalarValue};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Decimal128Builder, Float64Array,
    Float64Builder, Int32Builder, Int64Array, Int64Builder, StringArray, StringBuilder,
};
use arrow::compute;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;
use rayon::prelude::*;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;

/// Group key for hash table
#[derive(Clone)]
struct GroupKey {
    values: Vec<ScalarValue>,
}

impl std::fmt::Debug for GroupKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupKey")
            .field("values", &self.values)
            .finish()
    }
}

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl Eq for GroupKey {}

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for value in &self.values {
            hash_scalar_value(value, state);
        }
    }
}

fn hash_scalar_value<H: Hasher>(value: &ScalarValue, state: &mut H) {
    std::mem::discriminant(value).hash(state);
    match value {
        ScalarValue::Null => {}
        ScalarValue::Boolean(v) => v.hash(state),
        ScalarValue::Int8(v) => v.hash(state),
        ScalarValue::Int16(v) => v.hash(state),
        ScalarValue::Int32(v) => v.hash(state),
        ScalarValue::Int64(v) => v.hash(state),
        ScalarValue::UInt8(v) => v.hash(state),
        ScalarValue::UInt16(v) => v.hash(state),
        ScalarValue::UInt32(v) => v.hash(state),
        ScalarValue::UInt64(v) => v.hash(state),
        ScalarValue::Float32(v) => v.hash(state), // OrderedFloat implements Hash
        ScalarValue::Float64(v) => v.hash(state), // OrderedFloat implements Hash
        ScalarValue::Utf8(v) => v.hash(state),
        ScalarValue::Date32(v) => v.hash(state),
        ScalarValue::Date64(v) => v.hash(state),
        ScalarValue::Timestamp(v) => v.hash(state),
        ScalarValue::Decimal128(v) => v.hash(state),
        ScalarValue::Interval(v) => v.hash(state),
        ScalarValue::List(values, _) => {
            values.len().hash(state);
            for v in values {
                hash_scalar_value(v, state);
            }
        }
    }
}

/// Accumulator state for a single aggregate
#[derive(Clone, Debug)]
enum AccumulatorState {
    Count(i64),
    Sum(f64),
    SumInt(i64),
    Avg {
        sum: f64,
        count: i64,
    },
    Min(Option<ScalarValue>),
    Max(Option<ScalarValue>),
    BoolAnd(Option<bool>),
    BoolOr(Option<bool>),
    /// Online variance using Welford's algorithm: (count, mean, M2)
    /// Finalize: population variance = M2/count, sample variance = M2/(count-1)
    Variance {
        count: i64,
        mean: f64,
        m2: f64,
    },
}

impl AccumulatorState {
    fn new(func: &AggregateFunction, input_type: &DataType) -> Self {
        match func {
            AggregateFunction::Count | AggregateFunction::CountDistinct => {
                AccumulatorState::Count(0)
            }
            AggregateFunction::Sum => match input_type {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    AccumulatorState::SumInt(0)
                }
                _ => AccumulatorState::Sum(0.0),
            },
            AggregateFunction::Avg => AccumulatorState::Avg { sum: 0.0, count: 0 },
            AggregateFunction::Min => AccumulatorState::Min(None),
            AggregateFunction::Max => AccumulatorState::Max(None),
            AggregateFunction::BoolAnd => AccumulatorState::BoolAnd(None),
            AggregateFunction::BoolOr => AccumulatorState::BoolOr(None),
            AggregateFunction::Stddev
            | AggregateFunction::StddevPop
            | AggregateFunction::StddevSamp
            | AggregateFunction::Variance
            | AggregateFunction::VarPop
            | AggregateFunction::VarSamp => AccumulatorState::Variance {
                count: 0,
                mean: 0.0,
                m2: 0.0,
            },
            _ => AccumulatorState::Count(0), // Default for unsupported
        }
    }

    /// Update with a ScalarValue (slow path, used for MIN/MAX with non-numeric types)
    fn update(&mut self, value: &ScalarValue) {
        match self {
            AccumulatorState::Count(c) => {
                if !matches!(value, ScalarValue::Null) {
                    *c += 1;
                }
            }
            AccumulatorState::Sum(s) => {
                if let Some(v) = scalar_to_f64(value) {
                    *s += v;
                }
            }
            AccumulatorState::SumInt(s) => {
                if let Some(v) = scalar_to_i64(value) {
                    *s += v;
                }
            }
            AccumulatorState::Avg { sum, count } => {
                if let Some(v) = scalar_to_f64(value) {
                    *sum += v;
                    *count += 1;
                }
            }
            AccumulatorState::Min(min) => {
                if !matches!(value, ScalarValue::Null) {
                    match min {
                        None => *min = Some(value.clone()),
                        Some(current) => {
                            if compare_scalar_values(value, current) == std::cmp::Ordering::Less {
                                *min = Some(value.clone());
                            }
                        }
                    }
                }
            }
            AccumulatorState::Max(max) => {
                if !matches!(value, ScalarValue::Null) {
                    match max {
                        None => *max = Some(value.clone()),
                        Some(current) => {
                            if compare_scalar_values(value, current) == std::cmp::Ordering::Greater
                            {
                                *max = Some(value.clone());
                            }
                        }
                    }
                }
            }
            AccumulatorState::BoolAnd(state) => {
                if let ScalarValue::Boolean(v) = value {
                    *state = Some(state.unwrap_or(true) && *v);
                }
            }
            AccumulatorState::BoolOr(state) => {
                if let ScalarValue::Boolean(v) = value {
                    *state = Some(state.unwrap_or(false) || *v);
                }
            }
            AccumulatorState::Variance { count, mean, m2 } => {
                if let Some(x) = scalar_to_f64(value) {
                    *count += 1;
                    let delta = x - *mean;
                    *mean += delta / *count as f64;
                    let delta2 = x - *mean;
                    *m2 += delta * delta2;
                }
            }
        }
    }

    /// Fast path: update with f64 value directly (no ScalarValue allocation)
    #[inline]
    fn update_f64(&mut self, value: f64) {
        match self {
            AccumulatorState::Count(c) => *c += 1,
            AccumulatorState::Sum(s) => *s += value,
            AccumulatorState::SumInt(s) => *s += value as i64,
            AccumulatorState::Avg { sum, count } => {
                *sum += value;
                *count += 1;
            }
            AccumulatorState::Min(min) => {
                let new_val = ScalarValue::Float64(ordered_float::OrderedFloat(value));
                match min {
                    None => *min = Some(new_val),
                    Some(ScalarValue::Float64(current)) => {
                        if value < current.into_inner() {
                            *min = Some(new_val);
                        }
                    }
                    _ => {}
                }
            }
            AccumulatorState::Max(max) => {
                let new_val = ScalarValue::Float64(ordered_float::OrderedFloat(value));
                match max {
                    None => *max = Some(new_val),
                    Some(ScalarValue::Float64(current)) => {
                        if value > current.into_inner() {
                            *max = Some(new_val);
                        }
                    }
                    _ => {}
                }
            }
            AccumulatorState::BoolAnd(state) => {
                *state = Some(state.unwrap_or(true) && (value != 0.0));
            }
            AccumulatorState::BoolOr(state) => {
                *state = Some(state.unwrap_or(false) || (value != 0.0));
            }
            AccumulatorState::Variance { count, mean, m2 } => {
                *count += 1;
                let delta = value - *mean;
                *mean += delta / *count as f64;
                let delta2 = value - *mean;
                *m2 += delta * delta2;
            }
        }
    }

    /// Fast path: update with i64 value directly (no ScalarValue allocation)
    #[inline]
    fn update_i64(&mut self, value: i64) {
        match self {
            AccumulatorState::Count(c) => *c += 1,
            AccumulatorState::Sum(s) => *s += value as f64,
            AccumulatorState::SumInt(s) => *s += value,
            AccumulatorState::Avg { sum, count } => {
                *sum += value as f64;
                *count += 1;
            }
            AccumulatorState::Min(min) => {
                let new_val = ScalarValue::Int64(value);
                match min {
                    None => *min = Some(new_val),
                    Some(ScalarValue::Int64(current)) => {
                        if value < *current {
                            *min = Some(new_val);
                        }
                    }
                    _ => {}
                }
            }
            AccumulatorState::Max(max) => {
                let new_val = ScalarValue::Int64(value);
                match max {
                    None => *max = Some(new_val),
                    Some(ScalarValue::Int64(current)) => {
                        if value > *current {
                            *max = Some(new_val);
                        }
                    }
                    _ => {}
                }
            }
            AccumulatorState::BoolAnd(state) => {
                *state = Some(state.unwrap_or(true) && (value != 0));
            }
            AccumulatorState::BoolOr(state) => {
                *state = Some(state.unwrap_or(false) || (value != 0));
            }
            AccumulatorState::Variance { count, mean, m2 } => {
                let x = value as f64;
                *count += 1;
                let delta = x - *mean;
                *mean += delta / *count as f64;
                let delta2 = x - *mean;
                *m2 += delta * delta2;
            }
        }
    }

    /// Fast path: increment count only
    #[inline]
    fn update_count(&mut self) {
        if let AccumulatorState::Count(c) = self {
            *c += 1;
        }
    }

    fn merge(&mut self, other: &AccumulatorState) {
        match (self, other) {
            (AccumulatorState::Count(a), AccumulatorState::Count(b)) => *a += b,
            (AccumulatorState::Sum(a), AccumulatorState::Sum(b)) => *a += b,
            (AccumulatorState::SumInt(a), AccumulatorState::SumInt(b)) => *a += b,
            (
                AccumulatorState::Avg { sum: s1, count: c1 },
                AccumulatorState::Avg { sum: s2, count: c2 },
            ) => {
                *s1 += s2;
                *c1 += c2;
            }
            (AccumulatorState::Min(a), AccumulatorState::Min(b)) => {
                if let Some(b_val) = b {
                    match a {
                        None => *a = Some(b_val.clone()),
                        Some(a_val) => {
                            if compare_scalar_values(b_val, a_val) == std::cmp::Ordering::Less {
                                *a = Some(b_val.clone());
                            }
                        }
                    }
                }
            }
            (AccumulatorState::Max(a), AccumulatorState::Max(b)) => {
                if let Some(b_val) = b {
                    match a {
                        None => *a = Some(b_val.clone()),
                        Some(a_val) => {
                            if compare_scalar_values(b_val, a_val) == std::cmp::Ordering::Greater {
                                *a = Some(b_val.clone());
                            }
                        }
                    }
                }
            }
            (AccumulatorState::BoolAnd(a), AccumulatorState::BoolAnd(b)) => {
                if let Some(b_val) = b {
                    *a = Some(a.unwrap_or(true) && *b_val);
                }
            }
            (AccumulatorState::BoolOr(a), AccumulatorState::BoolOr(b)) => {
                if let Some(b_val) = b {
                    *a = Some(a.unwrap_or(false) || *b_val);
                }
            }
            (
                AccumulatorState::Variance {
                    count: ca,
                    mean: ma,
                    m2: m2a,
                },
                AccumulatorState::Variance {
                    count: cb,
                    mean: mb,
                    m2: m2b,
                },
            ) => {
                if *cb > 0 {
                    if *ca == 0 {
                        *ca = *cb;
                        *ma = *mb;
                        *m2a = *m2b;
                    } else {
                        let total = *ca + *cb;
                        let delta = *mb - *ma;
                        *m2a += *m2b + delta * delta * (*ca as f64) * (*cb as f64) / (total as f64);
                        *ma = (*ma * (*ca as f64) + *mb * (*cb as f64)) / (total as f64);
                        *ca = total;
                    }
                }
            }
            _ => {}
        }
    }

    fn finalize(&self, func: &AggregateFunction) -> ScalarValue {
        match self {
            AccumulatorState::Count(c) => ScalarValue::Int64(*c),
            AccumulatorState::Sum(s) => ScalarValue::Float64(ordered_float::OrderedFloat(*s)),
            AccumulatorState::SumInt(s) => ScalarValue::Int64(*s),
            AccumulatorState::Avg { sum, count } => {
                if *count == 0 {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float64(ordered_float::OrderedFloat(*sum / *count as f64))
                }
            }
            AccumulatorState::Min(v) => v.clone().unwrap_or(ScalarValue::Null),
            AccumulatorState::Max(v) => v.clone().unwrap_or(ScalarValue::Null),
            AccumulatorState::BoolAnd(v) => match v {
                Some(val) => ScalarValue::Boolean(*val),
                None => ScalarValue::Null,
            },
            AccumulatorState::BoolOr(v) => match v {
                Some(val) => ScalarValue::Boolean(*val),
                None => ScalarValue::Null,
            },
            AccumulatorState::Variance { count, m2, .. } => {
                if *count == 0 {
                    return ScalarValue::Null;
                }
                let result = match func {
                    AggregateFunction::VarPop => *m2 / *count as f64,
                    AggregateFunction::Variance | AggregateFunction::VarSamp => {
                        if *count < 2 {
                            return ScalarValue::Null;
                        }
                        *m2 / (*count - 1) as f64
                    }
                    AggregateFunction::StddevPop => (*m2 / *count as f64).sqrt(),
                    AggregateFunction::Stddev | AggregateFunction::StddevSamp => {
                        if *count < 2 {
                            return ScalarValue::Null;
                        }
                        (*m2 / (*count - 1) as f64).sqrt()
                    }
                    _ => *m2 / *count as f64,
                };
                ScalarValue::Float64(ordered_float::OrderedFloat(result))
            }
        }
    }
}

fn compare_scalar_values(a: &ScalarValue, b: &ScalarValue) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match (a, b) {
        (ScalarValue::Null, ScalarValue::Null) => Ordering::Equal,
        (ScalarValue::Null, _) => Ordering::Less,
        (_, ScalarValue::Null) => Ordering::Greater,
        (ScalarValue::Int8(a), ScalarValue::Int8(b)) => a.cmp(b),
        (ScalarValue::Int16(a), ScalarValue::Int16(b)) => a.cmp(b),
        (ScalarValue::Int32(a), ScalarValue::Int32(b)) => a.cmp(b),
        (ScalarValue::Int64(a), ScalarValue::Int64(b)) => a.cmp(b),
        (ScalarValue::UInt8(a), ScalarValue::UInt8(b)) => a.cmp(b),
        (ScalarValue::UInt16(a), ScalarValue::UInt16(b)) => a.cmp(b),
        (ScalarValue::UInt32(a), ScalarValue::UInt32(b)) => a.cmp(b),
        (ScalarValue::UInt64(a), ScalarValue::UInt64(b)) => a.cmp(b),
        (ScalarValue::Float32(a), ScalarValue::Float32(b)) => a.cmp(b),
        (ScalarValue::Float64(a), ScalarValue::Float64(b)) => a.cmp(b),
        (ScalarValue::Utf8(a), ScalarValue::Utf8(b)) => a.cmp(b),
        (ScalarValue::Date32(a), ScalarValue::Date32(b)) => a.cmp(b),
        (ScalarValue::Date64(a), ScalarValue::Date64(b)) => a.cmp(b),
        (ScalarValue::Timestamp(a), ScalarValue::Timestamp(b)) => a.cmp(b),
        (ScalarValue::Decimal128(a), ScalarValue::Decimal128(b)) => a.cmp(b),
        _ => Ordering::Equal,
    }
}

fn scalar_to_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Int8(v) => Some(*v as f64),
        ScalarValue::Int16(v) => Some(*v as f64),
        ScalarValue::Int32(v) => Some(*v as f64),
        ScalarValue::Int64(v) => Some(*v as f64),
        ScalarValue::UInt8(v) => Some(*v as f64),
        ScalarValue::UInt16(v) => Some(*v as f64),
        ScalarValue::UInt32(v) => Some(*v as f64),
        ScalarValue::UInt64(v) => Some(*v as f64),
        ScalarValue::Float32(v) => Some(v.into_inner() as f64),
        ScalarValue::Float64(v) => Some(v.into_inner()),
        ScalarValue::Decimal128(v) => {
            use rust_decimal::prelude::ToPrimitive;
            v.to_f64()
        }
        _ => None,
    }
}

/// Convert ScalarValue to a raw u64 key (matches TypedArrayAccessor::raw_key)
fn scalar_to_raw_key(value: &ScalarValue) -> u64 {
    match value {
        ScalarValue::Null => u64::MAX,
        ScalarValue::Int64(v) => *v as u64,
        ScalarValue::Float64(v) => v.into_inner().to_bits(),
        ScalarValue::Date32(v) => *v as u64,
        ScalarValue::Utf8(s) => {
            let bytes = s.as_bytes();
            let len = bytes.len().min(8);
            let mut key = 0u64;
            for i in 0..len {
                key |= (bytes[i] as u64) << (i * 8);
            }
            key | ((bytes.len() as u64) << 56)
        }
        _ => {
            let mut hasher = std::collections::hash_map::DefaultHasher::new();
            hash_scalar_value(value, &mut hasher);
            std::hash::Hasher::finish(&hasher)
        }
    }
}

fn scalar_to_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(v) => Some(*v as i64),
        ScalarValue::Int16(v) => Some(*v as i64),
        ScalarValue::Int32(v) => Some(*v as i64),
        ScalarValue::Int64(v) => Some(*v),
        _ => None,
    }
}

/// Typed array accessor for fast value extraction without ScalarValue allocation
enum TypedArrayAccessor<'a> {
    Int64(&'a Int64Array),
    Float64(&'a Float64Array),
    String(&'a StringArray),
    Date32(&'a Date32Array),
    Other(ArrayRef),
}

impl<'a> TypedArrayAccessor<'a> {
    fn from_array(array: &'a ArrayRef) -> Self {
        match array.data_type() {
            DataType::Int64 => {
                TypedArrayAccessor::Int64(array.as_any().downcast_ref::<Int64Array>().unwrap())
            }
            DataType::Float64 => {
                TypedArrayAccessor::Float64(array.as_any().downcast_ref::<Float64Array>().unwrap())
            }
            DataType::Utf8 => {
                TypedArrayAccessor::String(array.as_any().downcast_ref::<StringArray>().unwrap())
            }
            DataType::Date32 => {
                TypedArrayAccessor::Date32(array.as_any().downcast_ref::<Date32Array>().unwrap())
            }
            _ => TypedArrayAccessor::Other(array.clone()),
        }
    }

    /// Update accumulator directly without creating ScalarValue
    #[inline]
    fn update_accumulator(&self, row: usize, acc: &mut AccumulatorState) {
        match self {
            TypedArrayAccessor::Float64(arr) => {
                if !arr.is_null(row) {
                    acc.update_f64(arr.value(row));
                }
            }
            TypedArrayAccessor::Int64(arr) => {
                if !arr.is_null(row) {
                    acc.update_i64(arr.value(row));
                }
            }
            TypedArrayAccessor::String(_) | TypedArrayAccessor::Date32(_) => {
                // For non-numeric types, fall back to ScalarValue path
                let value = self.extract_scalar(row);
                acc.update(&value);
            }
            TypedArrayAccessor::Other(arr) => {
                let value = extract_scalar(arr, row);
                acc.update(&value);
            }
        }
    }

    /// Extract a u64 key for perfect hash indexing (no allocation).
    /// Different values map to different u64 values.
    /// For strings, we hash the first 8 bytes plus length for a fast key.
    #[inline]
    fn raw_key(&self, row: usize) -> u64 {
        match self {
            TypedArrayAccessor::Int64(arr) => {
                if arr.is_null(row) {
                    u64::MAX
                } else {
                    arr.value(row) as u64
                }
            }
            TypedArrayAccessor::Float64(arr) => {
                if arr.is_null(row) {
                    u64::MAX
                } else {
                    arr.value(row).to_bits()
                }
            }
            TypedArrayAccessor::String(arr) => {
                if arr.is_null(row) {
                    u64::MAX
                } else {
                    // For short strings (group keys like "A", "N", "R", "F", "O"),
                    // pack bytes into u64 directly for perfect uniqueness
                    let bytes = arr.value(row).as_bytes();
                    let len = bytes.len().min(8);
                    let mut key = 0u64;
                    for i in 0..len {
                        key |= (bytes[i] as u64) << (i * 8);
                    }
                    // Include length to disambiguate short-prefix matches
                    key | ((bytes.len() as u64) << 56)
                }
            }
            TypedArrayAccessor::Date32(arr) => {
                if arr.is_null(row) {
                    u64::MAX
                } else {
                    arr.value(row) as u64
                }
            }
            TypedArrayAccessor::Other(arr) => {
                // Fallback: use hash of ScalarValue
                let val = extract_scalar(arr, row);
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                hash_scalar_value(&val, &mut hasher);
                std::hash::Hasher::finish(&hasher)
            }
        }
    }

    /// Extract ScalarValue (slow path, needed for group keys)
    fn extract_scalar(&self, row: usize) -> ScalarValue {
        match self {
            TypedArrayAccessor::Int64(arr) => {
                if arr.is_null(row) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Int64(arr.value(row))
                }
            }
            TypedArrayAccessor::Float64(arr) => {
                if arr.is_null(row) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Float64(ordered_float::OrderedFloat(arr.value(row)))
                }
            }
            TypedArrayAccessor::String(arr) => {
                if arr.is_null(row) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Utf8(arr.value(row).to_string())
                }
            }
            TypedArrayAccessor::Date32(arr) => {
                if arr.is_null(row) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Date32(arr.value(row))
                }
            }
            TypedArrayAccessor::Other(arr) => extract_scalar(arr, row),
        }
    }
}

/// Maximum number of groups for perfect hash mode.
/// If groups exceed this, falls back to HashMap.
const PERFECT_HASH_MAX_GROUPS: usize = 256;

/// Thread-local aggregation state.
///
/// Uses two strategies:
/// - **Perfect hash** (default): Fixed array indexed by group key, no hashing overhead.
///   Activated when the number of distinct groups ≤ PERFECT_HASH_MAX_GROUPS.
/// - **HashMap fallback**: Standard hash table for high-cardinality groups.
#[derive(Clone)]
pub struct AggregationState {
    /// Fixed-array accumulators indexed by perfect hash (low cardinality fast path)
    perfect_accs: Vec<Vec<AccumulatorState>>,
    /// Map from raw key (u64) → perfect hash index (one per group-by column)
    /// Uses u64 keys to avoid ScalarValue allocation in the hot path
    raw_key_maps: Vec<HashMap<u64, u8>>,
    /// Map from ScalarValue → perfect hash index (for merge operations)
    key_maps: Vec<HashMap<ScalarValue, u8>>,
    /// Strides for computing the flat index from per-column indices
    key_strides: Vec<usize>,
    /// Group keys in order of first insertion (for output)
    key_order: Vec<GroupKey>,
    /// Total number of slots in perfect_accs
    perfect_capacity: usize,
    /// Whether we overflowed and fell back to HashMap
    overflowed: bool,

    /// HashMap fallback: group key -> accumulator states
    groups: HashMap<GroupKey, Vec<AccumulatorState>>,
    /// Aggregate functions
    agg_funcs: Vec<AggregateFunction>,
    /// Input types for aggregates
    input_types: Vec<DataType>,
    /// Number of group-by columns
    num_group_cols: usize,
}

impl Default for AggregationState {
    fn default() -> Self {
        Self {
            perfect_accs: Vec::new(),
            raw_key_maps: Vec::new(),
            key_maps: Vec::new(),
            key_strides: Vec::new(),
            key_order: Vec::new(),
            perfect_capacity: 0,
            overflowed: false,
            groups: HashMap::new(),
            agg_funcs: Vec::new(),
            input_types: Vec::new(),
            num_group_cols: 0,
        }
    }
}

impl AggregationState {
    pub fn new(agg_funcs: Vec<AggregateFunction>, input_types: Vec<DataType>) -> Self {
        Self {
            agg_funcs,
            input_types,
            ..Default::default()
        }
    }

    /// Allocate perfect hash slots once we know the number of group-by columns
    fn init_perfect_hash(&mut self, num_group_cols: usize) {
        self.num_group_cols = num_group_cols;
        self.raw_key_maps = (0..num_group_cols).map(|_| HashMap::new()).collect();
        self.key_maps = (0..num_group_cols).map(|_| HashMap::new()).collect();
        self.key_strides = vec![1; num_group_cols];
    }

    /// Try to assign a perfect hash index for a group key.
    /// Returns the index, or None if we exceeded capacity and must fall back.
    ///
    /// Uses raw byte keys to avoid ScalarValue allocation in the hot path.
    #[inline]
    fn get_or_assign_perfect_index(
        &mut self,
        group_accessors: &[TypedArrayAccessor],
        row: usize,
    ) -> Option<usize> {
        if self.overflowed {
            return None;
        }

        let n = group_accessors.len();

        // Phase 1: Register all keys and collect ids.
        // We must do this BEFORE computing flat_idx because discovering a new
        // key in column j changes strides for columns 0..j-1.
        let mut ids = [0u8; 8]; // max 8 group-by columns
        let mut any_new = false;
        for (col, accessor) in group_accessors.iter().enumerate() {
            let raw_key = accessor.raw_key(row);
            let next_id = self.raw_key_maps[col].len() as u8;
            let id = *self.raw_key_maps[col].entry(raw_key).or_insert(next_id);
            ids[col] = id;
            if id == next_id {
                any_new = true;
            }
        }

        if any_new {
            // Recompute capacity
            let mut cap = 1usize;
            for km in &self.raw_key_maps {
                cap = cap.saturating_mul(km.len());
            }
            if cap > PERFECT_HASH_MAX_GROUPS {
                self.overflowed = true;
                return None;
            }

            // Save old strides and capacity before recomputing
            let old_strides = self.key_strides.clone();
            let old_capacity = self.perfect_capacity;

            // Recompute strides with new cardinalities
            self.key_strides = vec![1; n];
            for i in (0..n - 1).rev() {
                self.key_strides[i] = self.key_strides[i + 1] * self.raw_key_maps[i + 1].len();
            }

            // Check if strides actually changed and we have existing entries to rehash
            let needs_rehash = old_capacity > 0 && old_strides != self.key_strides;

            if needs_rehash {
                // Rehash: move existing accumulators from old positions to new positions.
                // This is needed because old entries were placed using old strides.
                let mut new_accs: Vec<Vec<AccumulatorState>> = (0..cap)
                    .map(|_| {
                        self.agg_funcs
                            .iter()
                            .zip(&self.input_types)
                            .map(|(func, dt)| AccumulatorState::new(func, dt))
                            .collect()
                    })
                    .collect();
                let mut new_key_order: Vec<GroupKey> = (0..cap)
                    .map(|_| GroupKey {
                        values: vec![ScalarValue::Null; n],
                    })
                    .collect();

                for old_idx in 0..old_capacity.min(self.perfect_accs.len()) {
                    if old_idx >= self.key_order.len() {
                        continue;
                    }
                    // Check if this slot has data
                    let has_data = !self.key_order[old_idx]
                        .values
                        .iter()
                        .all(|v| matches!(v, ScalarValue::Null));
                    if !has_data {
                        continue;
                    }

                    // Decode old ids from old_idx using old strides
                    let mut new_idx = 0usize;
                    let mut remainder = old_idx;
                    for col in 0..n {
                        let old_stride = old_strides[col];
                        let col_id = if old_stride > 0 {
                            remainder / old_stride
                        } else {
                            0
                        };
                        if old_stride > 0 {
                            remainder %= old_stride;
                        }
                        new_idx += col_id * self.key_strides[col];
                    }

                    // Move accumulators and key_order to new position
                    std::mem::swap(&mut new_accs[new_idx], &mut self.perfect_accs[old_idx]);
                    new_key_order[new_idx] = std::mem::replace(
                        &mut self.key_order[old_idx],
                        GroupKey {
                            values: vec![ScalarValue::Null; n],
                        },
                    );
                }

                self.perfect_accs = new_accs;
                self.key_order = new_key_order;
            } else {
                // No rehash needed — just extend arrays
                while self.perfect_accs.len() < cap {
                    self.perfect_accs.push(
                        self.agg_funcs
                            .iter()
                            .zip(&self.input_types)
                            .map(|(func, dt)| AccumulatorState::new(func, dt))
                            .collect(),
                    );
                }
                while self.key_order.len() < cap {
                    self.key_order.push(GroupKey {
                        values: vec![ScalarValue::Null; n],
                    });
                }
            }
            self.perfect_capacity = cap;
        }

        // Phase 2: Compute flat_idx using final (correct) strides
        let mut flat_idx = 0usize;
        for col in 0..n {
            flat_idx += ids[col] as usize * self.key_strides[col];
        }

        // Record key values for output (only on first assignment)
        if flat_idx < self.key_order.len()
            && self.key_order[flat_idx]
                .values
                .iter()
                .all(|v| matches!(v, ScalarValue::Null))
        {
            for (col, accessor) in group_accessors.iter().enumerate() {
                let val = accessor.extract_scalar(row);
                if !matches!(val, ScalarValue::Null) {
                    self.key_order[flat_idx].values[col] = val;
                }
            }
        }

        Some(flat_idx)
    }

    /// Process a batch and update the aggregation state
    pub fn process_batch(
        &mut self,
        batch: &RecordBatch,
        group_by_exprs: &[Expr],
        agg_input_exprs: &[Expr],
    ) -> Result<()> {
        let num_rows = batch.num_rows();
        if num_rows == 0 {
            return Ok(());
        }

        // Initialize perfect hash on first batch
        if self.key_maps.is_empty() && !group_by_exprs.is_empty() {
            self.init_perfect_hash(group_by_exprs.len());
        }

        // Evaluate expressions once per batch
        let group_arrays: Vec<ArrayRef> = group_by_exprs
            .iter()
            .map(|expr| evaluate_expr(batch, expr))
            .collect::<Result<Vec<_>>>()?;
        let agg_arrays: Vec<ArrayRef> = agg_input_exprs
            .iter()
            .map(|expr| evaluate_expr(batch, expr))
            .collect::<Result<Vec<_>>>()?;

        // Pre-downcast for typed access
        let group_accessors: Vec<TypedArrayAccessor> = group_arrays
            .iter()
            .map(TypedArrayAccessor::from_array)
            .collect();
        let agg_accessors: Vec<TypedArrayAccessor> = agg_arrays
            .iter()
            .map(TypedArrayAccessor::from_array)
            .collect();

        if !self.overflowed && !group_by_exprs.is_empty() {
            // Perfect hash fast path
            // Check if all aggregate inputs are f64 for the fastest possible path
            let all_f64_inputs = agg_accessors
                .iter()
                .all(|a| matches!(a, TypedArrayAccessor::Float64(_)));

            if all_f64_inputs && !agg_accessors.is_empty() {
                // Ultra-fast path: pre-extract f64 slices and group key raw arrays
                let f64_slices: Vec<&[f64]> = agg_accessors
                    .iter()
                    .map(|a| match a {
                        TypedArrayAccessor::Float64(arr) => arr.values().as_ref(),
                        _ => unreachable!(),
                    })
                    .collect();

                for row in 0..num_rows {
                    if let Some(idx) = self.get_or_assign_perfect_index(&group_accessors, row) {
                        let accs = &mut self.perfect_accs[idx];
                        for (i, acc) in accs.iter_mut().enumerate() {
                            acc.update_f64(f64_slices[i][row]);
                        }
                    } else {
                        self.drain_perfect_to_hashmap();
                        self.process_rows_hashmap(row, num_rows, &group_accessors, &agg_accessors);
                        break;
                    }
                }
            } else {
                // Generic perfect hash path
                for row in 0..num_rows {
                    if let Some(idx) = self.get_or_assign_perfect_index(&group_accessors, row) {
                        let accs = &mut self.perfect_accs[idx];
                        for (i, acc) in accs.iter_mut().enumerate() {
                            agg_accessors[i].update_accumulator(row, acc);
                        }
                    } else {
                        self.drain_perfect_to_hashmap();
                        self.process_rows_hashmap(row, num_rows, &group_accessors, &agg_accessors);
                        break;
                    }
                }
            }
        } else if group_by_exprs.is_empty() {
            // No group-by: single accumulator
            if self.perfect_accs.is_empty() {
                self.perfect_accs.push(
                    self.agg_funcs
                        .iter()
                        .zip(&self.input_types)
                        .map(|(func, dt)| AccumulatorState::new(func, dt))
                        .collect(),
                );
                self.perfect_capacity = 1;
                self.key_order.push(GroupKey { values: vec![] });
            }
            let accs = &mut self.perfect_accs[0];
            for row in 0..num_rows {
                for (i, acc) in accs.iter_mut().enumerate() {
                    agg_accessors[i].update_accumulator(row, acc);
                }
            }
        } else {
            // HashMap fallback
            self.process_rows_hashmap(0, num_rows, &group_accessors, &agg_accessors);
        }

        Ok(())
    }

    /// Process rows using HashMap (slow path)
    fn process_rows_hashmap(
        &mut self,
        start_row: usize,
        end_row: usize,
        group_accessors: &[TypedArrayAccessor],
        agg_accessors: &[TypedArrayAccessor],
    ) {
        for row in start_row..end_row {
            let key = GroupKey {
                values: group_accessors
                    .iter()
                    .map(|accessor| accessor.extract_scalar(row))
                    .collect(),
            };

            let accumulators = self.groups.entry(key).or_insert_with(|| {
                self.agg_funcs
                    .iter()
                    .zip(&self.input_types)
                    .map(|(func, dt)| AccumulatorState::new(func, dt))
                    .collect()
            });

            for (i, acc) in accumulators.iter_mut().enumerate() {
                agg_accessors[i].update_accumulator(row, acc);
            }
        }
    }

    /// Drain perfect hash accumulators into the HashMap fallback
    fn drain_perfect_to_hashmap(&mut self) {
        for (idx, accs) in self.perfect_accs.drain(..).enumerate() {
            if idx < self.key_order.len() {
                let key = self.key_order[idx].clone();
                // Skip empty slots (all-null keys that were never assigned)
                let has_data = accs.iter().any(|a| match a {
                    AccumulatorState::Count(c) => *c > 0,
                    AccumulatorState::Sum(s) => *s != 0.0,
                    AccumulatorState::SumInt(s) => *s != 0,
                    AccumulatorState::Avg { count, .. } => *count > 0,
                    AccumulatorState::Min(v) => v.is_some(),
                    AccumulatorState::Max(v) => v.is_some(),
                    AccumulatorState::BoolAnd(v) => v.is_some(),
                    AccumulatorState::BoolOr(v) => v.is_some(),
                    AccumulatorState::Variance { count, .. } => *count > 0,
                });
                if has_data {
                    self.groups.insert(key, accs);
                }
            }
        }
        self.key_order.clear();
    }

    /// Merge another state into this one
    pub fn merge(&mut self, other: &AggregationState) {
        // If other used perfect hash, merge into our perfect hash or groups
        if !other.overflowed && !other.perfect_accs.is_empty() {
            for (idx, other_accs) in other.perfect_accs.iter().enumerate() {
                // Check if this slot has data
                let has_data = other_accs.iter().any(|a| match a {
                    AccumulatorState::Count(c) => *c > 0,
                    AccumulatorState::Sum(s) => *s != 0.0,
                    AccumulatorState::SumInt(s) => *s != 0,
                    AccumulatorState::Avg { count, .. } => *count > 0,
                    AccumulatorState::Min(v) => v.is_some(),
                    AccumulatorState::Max(v) => v.is_some(),
                    AccumulatorState::BoolAnd(v) => v.is_some(),
                    AccumulatorState::BoolOr(v) => v.is_some(),
                    AccumulatorState::Variance { count, .. } => *count > 0,
                });
                if !has_data {
                    continue;
                }

                if !self.overflowed && idx < other.key_order.len() {
                    let key = &other.key_order[idx];
                    // Try to find the same key in our perfect hash
                    if let Some(our_idx) = self.find_perfect_index(key) {
                        // Ensure our array is large enough
                        while self.perfect_accs.len() <= our_idx {
                            self.perfect_accs.push(
                                self.agg_funcs
                                    .iter()
                                    .zip(&self.input_types)
                                    .map(|(func, dt)| AccumulatorState::new(func, dt))
                                    .collect(),
                            );
                        }
                        while self.key_order.len() <= our_idx {
                            self.key_order.push(GroupKey {
                                values: vec![
                                    ScalarValue::Null;
                                    self.num_group_cols.max(key.values.len())
                                ],
                            });
                        }
                        self.key_order[our_idx] = key.clone();

                        for (acc, other_acc) in
                            self.perfect_accs[our_idx].iter_mut().zip(other_accs.iter())
                        {
                            acc.merge(other_acc);
                        }
                    } else {
                        // Cannot fit in perfect hash, use HashMap
                        let accs = self.groups.entry(key.clone()).or_insert_with(|| {
                            self.agg_funcs
                                .iter()
                                .zip(&self.input_types)
                                .map(|(func, dt)| AccumulatorState::new(func, dt))
                                .collect()
                        });
                        for (acc, other_acc) in accs.iter_mut().zip(other_accs.iter()) {
                            acc.merge(other_acc);
                        }
                    }
                } else if idx < other.key_order.len() {
                    let key = &other.key_order[idx];
                    let accs = self.groups.entry(key.clone()).or_insert_with(|| {
                        self.agg_funcs
                            .iter()
                            .zip(&self.input_types)
                            .map(|(func, dt)| AccumulatorState::new(func, dt))
                            .collect()
                    });
                    for (acc, other_acc) in accs.iter_mut().zip(other_accs.iter()) {
                        acc.merge(other_acc);
                    }
                }
            }
        }

        // Merge HashMap entries
        for (key, other_accs) in &other.groups {
            if !self.overflowed {
                if let Some(our_idx) = self.find_perfect_index(key) {
                    while self.perfect_accs.len() <= our_idx {
                        self.perfect_accs.push(
                            self.agg_funcs
                                .iter()
                                .zip(&self.input_types)
                                .map(|(func, dt)| AccumulatorState::new(func, dt))
                                .collect(),
                        );
                    }
                    for (acc, other_acc) in
                        self.perfect_accs[our_idx].iter_mut().zip(other_accs.iter())
                    {
                        acc.merge(other_acc);
                    }
                    continue;
                }
            }

            let accs = self.groups.entry(key.clone()).or_insert_with(|| {
                self.agg_funcs
                    .iter()
                    .zip(&self.input_types)
                    .map(|(func, dt)| AccumulatorState::new(func, dt))
                    .collect()
            });
            for (acc, other_acc) in accs.iter_mut().zip(other_accs.iter()) {
                acc.merge(other_acc);
            }
        }
    }

    /// Try to find the perfect hash index for a key, assigning new IDs if needed.
    /// Used during merge operations (ScalarValue-based keys).
    fn find_perfect_index(&mut self, key: &GroupKey) -> Option<usize> {
        if self.overflowed || key.values.is_empty() {
            if key.values.is_empty() && !self.perfect_accs.is_empty() {
                return Some(0);
            }
            return None;
        }

        // Ensure key_maps is initialized
        if self.key_maps.is_empty() {
            self.init_perfect_hash(key.values.len());
        }

        let n = key.values.len();

        // Phase 1: Register all keys and collect ids
        let mut ids = [0u8; 8];
        let mut any_new = false;
        for (col, val) in key.values.iter().enumerate() {
            let next_id = self.key_maps[col].len() as u8;
            let id = *self.key_maps[col].entry(val.clone()).or_insert(next_id);
            ids[col] = id;

            if id == next_id {
                any_new = true;
                let raw = scalar_to_raw_key(val);
                self.raw_key_maps[col].entry(raw).or_insert(id);
            }
        }

        if any_new {
            let mut cap = 1usize;
            for km in &self.key_maps {
                cap = cap.saturating_mul(km.len());
            }
            if cap > PERFECT_HASH_MAX_GROUPS {
                self.overflowed = true;
                return None;
            }

            let old_strides = self.key_strides.clone();
            let old_capacity = self.perfect_capacity;

            self.key_strides = vec![1; n];
            for i in (0..n - 1).rev() {
                self.key_strides[i] = self.key_strides[i + 1] * self.key_maps[i + 1].len();
            }

            // Rehash existing entries if strides changed
            if old_capacity > 0 && old_strides != self.key_strides {
                let mut new_accs: Vec<Vec<AccumulatorState>> = (0..cap)
                    .map(|_| {
                        self.agg_funcs
                            .iter()
                            .zip(&self.input_types)
                            .map(|(func, dt)| AccumulatorState::new(func, dt))
                            .collect()
                    })
                    .collect();
                let mut new_key_order: Vec<GroupKey> = (0..cap)
                    .map(|_| GroupKey {
                        values: vec![ScalarValue::Null; n],
                    })
                    .collect();

                for old_idx in 0..old_capacity.min(self.perfect_accs.len()) {
                    if old_idx >= self.key_order.len() {
                        continue;
                    }
                    let has_data = !self.key_order[old_idx]
                        .values
                        .iter()
                        .all(|v| matches!(v, ScalarValue::Null));
                    if !has_data {
                        continue;
                    }

                    let mut new_idx = 0usize;
                    let mut remainder = old_idx;
                    for col in 0..n {
                        let old_stride = old_strides[col];
                        let col_id = if old_stride > 0 {
                            remainder / old_stride
                        } else {
                            0
                        };
                        if old_stride > 0 {
                            remainder %= old_stride;
                        }
                        new_idx += col_id * self.key_strides[col];
                    }

                    std::mem::swap(&mut new_accs[new_idx], &mut self.perfect_accs[old_idx]);
                    new_key_order[new_idx] = std::mem::replace(
                        &mut self.key_order[old_idx],
                        GroupKey {
                            values: vec![ScalarValue::Null; n],
                        },
                    );
                }

                self.perfect_accs = new_accs;
                self.key_order = new_key_order;
            } else {
                while self.perfect_accs.len() < cap {
                    self.perfect_accs.push(
                        self.agg_funcs
                            .iter()
                            .zip(&self.input_types)
                            .map(|(func, dt)| AccumulatorState::new(func, dt))
                            .collect(),
                    );
                }
                while self.key_order.len() < cap {
                    self.key_order.push(GroupKey {
                        values: vec![ScalarValue::Null; n],
                    });
                }
            }
            self.perfect_capacity = cap;
        }

        // Phase 2: Compute flat_idx with final strides
        let mut flat_idx = 0usize;
        for col in 0..n {
            flat_idx += ids[col] as usize * self.key_strides[col];
        }

        Some(flat_idx)
    }

    /// Build the output RecordBatch
    pub fn build_output(&self, schema: &SchemaRef) -> Result<RecordBatch> {
        let num_group_cols = schema.fields().len() - self.agg_funcs.len();

        // Collect all groups from both perfect hash and HashMap
        let mut all_groups: Vec<(&GroupKey, &Vec<AccumulatorState>)> = Vec::new();

        // From perfect hash
        if !self.overflowed {
            for (idx, accs) in self.perfect_accs.iter().enumerate() {
                if idx >= self.key_order.len() {
                    continue;
                }
                let has_data = accs.iter().any(|a| match a {
                    AccumulatorState::Count(c) => *c > 0,
                    AccumulatorState::Sum(s) => *s != 0.0,
                    AccumulatorState::SumInt(s) => *s != 0,
                    AccumulatorState::Avg { count, .. } => *count > 0,
                    AccumulatorState::Min(v) => v.is_some(),
                    AccumulatorState::Max(v) => v.is_some(),
                    AccumulatorState::BoolAnd(v) => v.is_some(),
                    AccumulatorState::BoolOr(v) => v.is_some(),
                    AccumulatorState::Variance { count, .. } => *count > 0,
                });
                if has_data {
                    all_groups.push((&self.key_order[idx], accs));
                }
            }
        }

        // From HashMap
        for (key, accs) in &self.groups {
            all_groups.push((key, accs));
        }

        let num_groups = all_groups.len();

        let mut arrays: Vec<ArrayRef> = Vec::new();

        // Group-by columns
        for col_idx in 0..num_group_cols {
            let field = schema.field(col_idx);
            let array = build_group_array(
                all_groups.iter().map(|(k, _)| &k.values[col_idx]),
                field.data_type(),
                num_groups,
            )?;
            arrays.push(array);
        }

        // Aggregate columns
        for agg_idx in 0..self.agg_funcs.len() {
            let func = &self.agg_funcs[agg_idx];
            let values: Vec<ScalarValue> = all_groups
                .iter()
                .map(|(_, accs)| accs[agg_idx].finalize(func))
                .collect();

            let field = schema.field(num_group_cols + agg_idx);
            let array = build_scalar_array(&values, field.data_type())?;
            arrays.push(array);
        }

        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| QueryError::Execution(format!("Failed to build output batch: {}", e)))
    }
}

fn extract_scalar(array: &ArrayRef, row: usize) -> ScalarValue {
    if array.is_null(row) {
        return ScalarValue::Null;
    }

    match array.data_type() {
        DataType::Int8 => ScalarValue::Int8(
            array
                .as_any()
                .downcast_ref::<arrow::array::Int8Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Int16 => ScalarValue::Int16(
            array
                .as_any()
                .downcast_ref::<arrow::array::Int16Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Int32 => ScalarValue::Int32(
            array
                .as_any()
                .downcast_ref::<arrow::array::Int32Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Int64 => ScalarValue::Int64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::UInt8 => ScalarValue::UInt8(
            array
                .as_any()
                .downcast_ref::<arrow::array::UInt8Array>()
                .unwrap()
                .value(row),
        ),
        DataType::UInt16 => ScalarValue::UInt16(
            array
                .as_any()
                .downcast_ref::<arrow::array::UInt16Array>()
                .unwrap()
                .value(row),
        ),
        DataType::UInt32 => ScalarValue::UInt32(
            array
                .as_any()
                .downcast_ref::<arrow::array::UInt32Array>()
                .unwrap()
                .value(row),
        ),
        DataType::UInt64 => ScalarValue::UInt64(
            array
                .as_any()
                .downcast_ref::<arrow::array::UInt64Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Float32 => {
            let val = array
                .as_any()
                .downcast_ref::<arrow::array::Float32Array>()
                .unwrap()
                .value(row);
            ScalarValue::Float32(ordered_float::OrderedFloat(val))
        }
        DataType::Float64 => {
            let val = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            ScalarValue::Float64(ordered_float::OrderedFloat(val))
        }
        DataType::Utf8 => ScalarValue::Utf8(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        DataType::Date32 => ScalarValue::Date32(
            array
                .as_any()
                .downcast_ref::<Date32Array>()
                .unwrap()
                .value(row),
        ),
        DataType::Boolean => ScalarValue::Boolean(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row),
        ),
        DataType::Decimal128(_p, s) => {
            let arr = array
                .as_any()
                .downcast_ref::<arrow::array::Decimal128Array>()
                .unwrap();
            let val = arr.value(row);
            // Convert i128 to Decimal using scale
            let decimal = rust_decimal::Decimal::from_i128_with_scale(val, *s as u32);
            ScalarValue::Decimal128(decimal)
        }
        _ => ScalarValue::Null,
    }
}

fn build_group_array<'a>(
    values: impl Iterator<Item = &'a ScalarValue>,
    data_type: &DataType,
    _capacity: usize,
) -> Result<ArrayRef> {
    let values: Vec<&ScalarValue> = values.collect();
    build_scalar_array_ref(&values, data_type)
}

fn build_scalar_array(values: &[ScalarValue], data_type: &DataType) -> Result<ArrayRef> {
    let refs: Vec<&ScalarValue> = values.iter().collect();
    build_scalar_array_ref(&refs, data_type)
}

fn build_scalar_array_ref(values: &[&ScalarValue], data_type: &DataType) -> Result<ArrayRef> {
    match data_type {
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Int64(val) => builder.append_value(*val),
                    ScalarValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Float64(val) => builder.append_value(val.into_inner()),
                    ScalarValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 16);
            for v in values {
                match v {
                    ScalarValue::Utf8(val) => builder.append_value(val),
                    ScalarValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let vals: Vec<Option<i32>> = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Date32(val) => Some(*val),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(Date32Array::from(vals)))
        }
        DataType::Decimal128(p, s) => {
            let mut builder = Decimal128Builder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Decimal128(val) => {
                        // Convert Decimal to i128 by scaling
                        let scaled = val.mantissa();
                        builder.append_value(scaled)
                    }
                    ScalarValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(
                builder
                    .finish()
                    .with_precision_and_scale(*p, *s)
                    .map_err(|e| {
                        QueryError::Execution(format!("Invalid decimal precision/scale: {}", e))
                    })?,
            ))
        }
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Boolean(val) => builder.append_value(*val),
                    ScalarValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Int32(val) => builder.append_value(*val),
                    ScalarValue::Int64(val) => builder.append_value(*val as i32),
                    ScalarValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(QueryError::NotImplemented(format!(
            "Unsupported data type for group array: {:?}",
            data_type
        ))),
    }
}

/// Execute a parallel aggregation query using morsel-driven parallelism
pub fn execute_morsel_aggregation(
    path: impl AsRef<Path>,
    filter_expr: Option<&Expr>,
    group_by_exprs: &[Expr],
    agg_funcs: &[AggregateFunction],
    agg_input_exprs: &[Expr],
    output_schema: SchemaRef,
    projection: Option<Vec<usize>>,
) -> Result<RecordBatch> {
    // Create the parallel Parquet source
    let source = ParallelParquetSource::try_from_path(&path, projection, DEFAULT_MORSEL_SIZE)?;
    let input_schema = source.schema();

    // Determine input types for aggregates
    let plan_schema = crate::planner::PlanSchema::from(input_schema.as_ref());
    let input_types: Vec<DataType> = agg_input_exprs
        .iter()
        .map(|e| e.data_type(&plan_schema).unwrap_or(DataType::Float64))
        .collect();

    let num_threads = rayon::current_num_threads();

    // Clone expressions for use in parallel closure
    let group_by_exprs = group_by_exprs.to_vec();
    let agg_input_exprs = agg_input_exprs.to_vec();
    let agg_funcs = agg_funcs.to_vec();
    let filter_expr = filter_expr.cloned();

    // Execute in parallel - each thread processes morsels and maintains its own hash table
    let thread_states: Vec<Result<AggregationState>> = (0..num_threads)
        .into_par_iter()
        .map(|_thread_id| {
            let mut state = AggregationState::new(agg_funcs.clone(), input_types.clone());

            // Keep processing morsels from the source
            while let Some(work) = source.get_work() {
                let batches = source.read_row_group(&work)?;

                for batch in batches {
                    // Apply filter if present
                    let filtered_batch = if let Some(ref filter) = filter_expr {
                        let filter_result = evaluate_expr(&batch, filter)?;
                        let filter_array = filter_result
                            .as_any()
                            .downcast_ref::<BooleanArray>()
                            .ok_or_else(|| {
                                QueryError::Execution("Filter must return boolean".to_string())
                            })?;

                        // Use arrow's filter kernel
                        let filtered_columns: Vec<ArrayRef> = batch
                            .columns()
                            .iter()
                            .map(|col| compute::filter(col.as_ref(), filter_array))
                            .collect::<std::result::Result<Vec<_>, _>>()
                            .map_err(|e| QueryError::Execution(format!("Filter failed: {}", e)))?;

                        if filtered_columns.is_empty() || filtered_columns[0].len() == 0 {
                            continue;
                        }

                        RecordBatch::try_new(batch.schema(), filtered_columns).map_err(|e| {
                            QueryError::Execution(format!("Failed to create filtered batch: {}", e))
                        })?
                    } else {
                        batch
                    };

                    // Process the batch
                    state.process_batch(&filtered_batch, &group_by_exprs, &agg_input_exprs)?;
                }

                source.complete_work();
            }

            Ok(state)
        })
        .collect();

    // Merge all thread states
    let mut final_state = AggregationState::new(agg_funcs.clone(), input_types);
    for result in thread_states {
        let state = result?;
        final_state.merge(&state);
    }

    // Build output
    final_state.build_output(&output_schema)
}
