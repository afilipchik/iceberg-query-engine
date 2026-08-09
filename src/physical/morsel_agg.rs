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
use std::hash::{BuildHasher, Hash, Hasher};
use std::path::Path;
use std::sync::Arc;

/// Merge per-thread entry lists (all belonging to the same key shard) into a
/// single groups map. Duplicate keys across threads have their accumulator
/// states merged pairwise.
pub(crate) fn merge_entries_into_map(
    lists: Vec<Vec<(GroupKey, Vec<AccumulatorState>)>>,
) -> HashMap<GroupKey, Vec<AccumulatorState>> {
    let cap: usize = lists.iter().map(|l| l.len()).sum();
    let mut map: HashMap<GroupKey, Vec<AccumulatorState>> = HashMap::with_capacity(cap);
    for list in lists {
        for (key, accs) in list {
            match map.entry(key) {
                hashbrown::hash_map::Entry::Occupied(mut e) => {
                    for (a, b) in e.get_mut().iter_mut().zip(accs.iter()) {
                        a.merge(b);
                    }
                }
                hashbrown::hash_map::Entry::Vacant(v) => {
                    v.insert(accs);
                }
            }
        }
    }
    map
}

/// Merge thread-local aggregation states into output batches, in parallel
/// when the group count is large. Shared by MorselAggregateExec,
/// HashAggregateExec's morsel-parallel path, and the fused streaming path in
/// SpillableHashAggregateExec.
pub(crate) fn merge_states_to_batches(
    states: Vec<AggregationState>,
    agg_funcs: &[AggregateFunction],
    input_types: &[DataType],
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    merge_states_to_batches_filtered(states, agg_funcs, input_types, schema, None)
}

/// Like `merge_states_to_batches`, but applies a HAVING-style predicate to
/// each shard's output batch INSIDE the parallel merge. Filtering per shard
/// keeps only the surviving rows and drops the full-size shard arrays on the
/// worker thread — materializing (and later freeing) the complete group set
/// on one thread measured 550ms of munmap stalls alone on Q18's 15M groups.
pub(crate) fn merge_states_to_batches_filtered(
    states: Vec<AggregationState>,
    agg_funcs: &[AggregateFunction],
    input_types: &[DataType],
    schema: &SchemaRef,
    post_filter: Option<&Expr>,
) -> Result<Vec<RecordBatch>> {
    const PARALLEL_MERGE_MIN_GROUPS: usize = 65_536;
    let total_groups: usize = states.iter().map(|s| s.group_count()).sum();

    // Full-raw pipeline for a single integer group column: shard, merge, and
    // build output on raw u64 keys. The GroupKey pipeline converts every group
    // to Vec<ScalarValue> and re-hashes it during the shard merge — profiling
    // Q18's 15M-group aggregate showed merge_entries_into_map +
    // build_scalar_array_ref dominating the whole query.
    let num_group_cols = schema.fields().len() - agg_funcs.len();
    let raw_dt = if num_group_cols == 1 {
        match schema.field(0).data_type() {
            DataType::Int64 | DataType::Int32 | DataType::Date32 => {
                Some(schema.field(0).data_type().clone())
            }
            _ => None,
        }
    } else {
        None
    };
    if let (Some(dt), true) = (raw_dt, total_groups > PARALLEL_MERGE_MIN_GROUPS) {
        let mut prepared = Vec::with_capacity(states.len());
        let mut all_raw = true;
        for mut st in states {
            st.raw_type = Some(dt.clone());
            st.drain_perfect_to_hashmap();
            st.normalize_raw();
            if !st.groups.is_empty() {
                all_raw = false;
            }
            prepared.push(st);
        }
        if all_raw {
            // Pair-based merge when the shape is exactly [Sum(Float64)]:
            // (u64, f64) entries end to end, no boxed accumulators anywhere.
            let sum_shape = agg_funcs.len() == 1
                && matches!(agg_funcs[0], AggregateFunction::Sum)
                && matches!(input_types.first(), Some(DataType::Float64))
                && schema.fields().len() == 2
                && schema.field(1).data_type() == &DataType::Float64;
            if sum_shape
                && prepared
                    .iter_mut()
                    .all(|st| st.absorb_raw_groups_into_sums())
            {
                return merge_raw_sum_states_to_batches(
                    prepared,
                    agg_funcs,
                    input_types,
                    &dt,
                    schema,
                    post_filter,
                );
            }
            return merge_raw_states_to_batches(
                prepared,
                agg_funcs,
                input_types,
                &dt,
                schema,
                post_filter,
            );
        }
        // Fall through: into_shards handles mixed raw/GroupKey states.
        return merge_states_groupkey(prepared, agg_funcs, input_types, schema, post_filter);
    }

    if states.len() > 1 && total_groups > PARALLEL_MERGE_MIN_GROUPS {
        return merge_states_groupkey(states, agg_funcs, input_types, schema, post_filter);
    }

    let mut final_state = AggregationState::new(agg_funcs.to_vec(), input_types.to_vec());
    for state in states {
        final_state.merge(&state);
    }
    let batches = vec![final_state.build_output(schema)?];
    match post_filter {
        Some(pred) => crate::physical::operators::filter_batches(batches, pred),
        None => Ok(batches),
    }
}

/// Build a shard's output batch and apply the optional HAVING predicate while
/// still on the merging worker thread.
fn build_filtered_output(
    state: &AggregationState,
    schema: &SchemaRef,
    post_filter: Option<&Expr>,
) -> Result<Option<RecordBatch>> {
    let batch = state.build_output(schema)?;
    match post_filter {
        Some(pred) => {
            let mut filtered = crate::physical::operators::filter_batches(vec![batch], pred)?;
            Ok(filtered.pop())
        }
        None => Ok(Some(batch)),
    }
}

/// GroupKey-based parallel shard merge (multi-column or non-integer keys).
fn merge_states_groupkey(
    states: Vec<AggregationState>,
    agg_funcs: &[AggregateFunction],
    input_types: &[DataType],
    schema: &SchemaRef,
    post_filter: Option<&Expr>,
) -> Result<Vec<RecordBatch>> {
    let p = rayon::current_num_threads().clamp(2, 64);
    let per_state_shards: Vec<_> = states
        .into_par_iter()
        .map(|s| s.into_shards(p))
        .collect::<Vec<_>>();

    let mut shard_major: Vec<Vec<_>> = (0..p).map(|_| Vec::new()).collect();
    for state_shards in per_state_shards {
        for (pi, shard) in state_shards.into_iter().enumerate() {
            shard_major[pi].push(shard);
        }
    }

    let batches: Vec<RecordBatch> = shard_major
        .into_par_iter()
        .map(|lists| {
            let map = merge_entries_into_map(lists);
            if map.is_empty() {
                return Ok(None);
            }
            let state =
                AggregationState::from_groups(agg_funcs.to_vec(), input_types.to_vec(), map);
            build_filtered_output(&state, schema, post_filter)
        })
        .collect::<Result<Vec<Option<RecordBatch>>>>()?
        .into_iter()
        .flatten()
        .collect();
    Ok(batches)
}

/// Raw-u64 parallel shard merge: no GroupKey materialization anywhere.
fn merge_raw_states_to_batches(
    states: Vec<AggregationState>,
    agg_funcs: &[AggregateFunction],
    input_types: &[DataType],
    raw_type: &DataType,
    schema: &SchemaRef,
    post_filter: Option<&Expr>,
) -> Result<Vec<RecordBatch>> {
    let p = rayon::current_num_threads().clamp(2, 64);
    let timing = std::env::var("AGG_TIMING").is_ok();
    let t0 = std::time::Instant::now();

    // Null-group accumulators merged across states up front; also gather the
    // global key range and group count to pick the merge strategy.
    let mut raw_null: Option<Vec<AccumulatorState>> = None;
    let mut gmin = i64::MAX;
    let mut gmax = i64::MIN;
    let mut total = 0usize;
    let mut prepared: Vec<AggregationState> = Vec::with_capacity(states.len());
    for mut st in states {
        st.demote_raw_sums();
        if let Some(n) = st.raw_null.take() {
            match &mut raw_null {
                Some(existing) => {
                    for (a, b) in existing.iter_mut().zip(n.iter()) {
                        a.merge(b);
                    }
                }
                None => raw_null = Some(n),
            }
        }
        total += st.raw_groups.len();
        for k in st.raw_groups.keys() {
            let v = *k as i64;
            gmin = gmin.min(v);
            gmax = gmax.max(v);
        }
        prepared.push(st);
    }

    // Dense key domain (e.g. l_orderkey, c_custkey): range-partition the
    // entries and merge each shard with a direct-address table — the
    // hash-shard merge re-inserts every group into a fresh HashMap even
    // though almost no keys span threads.
    let range = (gmax as i128 - gmin as i128 + 1).max(1) as u64;
    let dense =
        total > 0 && range <= 512_000_000 && (range as u128) <= 6 * total as u128 && gmax > gmin;

    let sharded_states: Vec<Vec<Vec<(u64, Vec<AccumulatorState>)>>> = if dense {
        let w = range.div_ceil(p as u64).max(1);
        prepared
            .into_iter()
            .map(|st| st.into_range_shards(p, gmin, w))
            .collect()
    } else {
        prepared
            .into_iter()
            .map(|st| st.into_raw_shards(p))
            .collect()
    };

    let mut shard_major: Vec<Vec<_>> = (0..p).map(|_| Vec::new()).collect();
    for state_shards in sharded_states {
        for (pi, shard) in state_shards.into_iter().enumerate() {
            shard_major[pi].push(shard);
        }
    }
    let t_sharded = t0.elapsed();

    let w = range.div_ceil(p as u64).max(1);
    let mut batches: Vec<RecordBatch> = shard_major
        .into_par_iter()
        .enumerate()
        .map(|(pi, lists)| {
            let cap: usize = lists.iter().map(|l| l.len()).sum();
            if cap == 0 {
                return Ok(None);
            }
            if dense {
                // Direct-address merge: slot index -> dense entry position
                let lo = gmin + (pi as u64 * w) as i64;
                let hi_w = if pi == lists.len().max(1) - 1 {
                    u64::MAX
                } else {
                    w
                };
                let width = if hi_w == u64::MAX {
                    (gmax - lo + 1).max(1) as usize
                } else {
                    w as usize
                };
                let mut slots: Vec<u32> = vec![u32::MAX; width];
                let mut dense_entries: Vec<(u64, Vec<AccumulatorState>)> = Vec::with_capacity(cap);
                for list in lists {
                    for (key, accs) in list {
                        let idx = ((key as i64).wrapping_sub(lo)) as usize;
                        let slot = slots[idx];
                        if slot == u32::MAX {
                            slots[idx] = dense_entries.len() as u32;
                            dense_entries.push((key, accs));
                        } else {
                            let target = &mut dense_entries[slot as usize].1;
                            for (a, b) in target.iter_mut().zip(accs.iter()) {
                                a.merge(b);
                            }
                        }
                    }
                }
                let refs: Vec<(u64, &Vec<AccumulatorState>)> =
                    dense_entries.iter().map(|(k, v)| (*k, v)).collect();
                let batch = build_output_raw_entries(&refs, None, agg_funcs, schema)?;
                return match post_filter {
                    Some(pred) => {
                        Ok(crate::physical::operators::filter_batches(vec![batch], pred)?.pop())
                    }
                    None => Ok(Some(batch)),
                };
            }
            let mut map: HashMap<u64, Vec<AccumulatorState>> = HashMap::with_capacity(cap);
            for list in lists {
                for (key, accs) in list {
                    match map.entry(key) {
                        hashbrown::hash_map::Entry::Occupied(mut e) => {
                            for (a, b) in e.get_mut().iter_mut().zip(accs.iter()) {
                                a.merge(b);
                            }
                        }
                        hashbrown::hash_map::Entry::Vacant(v) => {
                            v.insert(accs);
                        }
                    }
                }
            }
            let state = AggregationState::from_raw_groups(
                agg_funcs.to_vec(),
                input_types.to_vec(),
                raw_type.clone(),
                map,
                None,
            );
            build_filtered_output(&state, schema, post_filter)
        })
        .collect::<Result<Vec<Option<RecordBatch>>>>()?
        .into_iter()
        .flatten()
        .collect();

    if timing {
        eprintln!(
            "[raw-merge] {} groups dense={} shard: {:?}; merge+build: {:?}; total: {:?}",
            total,
            dense,
            t_sharded,
            t0.elapsed() - t_sharded,
            t0.elapsed()
        );
    }
    if let Some(null_accs) = raw_null {
        let state = AggregationState::from_raw_groups(
            agg_funcs.to_vec(),
            input_types.to_vec(),
            raw_type.clone(),
            HashMap::new(),
            Some(null_accs),
        );
        if let Some(b) = build_filtered_output(&state, schema, post_filter)? {
            batches.push(b);
        }
    }
    Ok(batches)
}

/// Pair-based parallel merge for the bare-f64 sum representation: (u64, f64)
/// entries end to end — shard, merge, and build output without ever
/// materializing a boxed accumulator (Q20: 4.4M groups, exactly [Sum(F64)]).
fn merge_raw_sum_states_to_batches(
    states: Vec<AggregationState>,
    agg_funcs: &[AggregateFunction],
    input_types: &[DataType],
    raw_type: &DataType,
    schema: &SchemaRef,
    post_filter: Option<&Expr>,
) -> Result<Vec<RecordBatch>> {
    let p = rayon::current_num_threads().clamp(2, 64);
    let timing = std::env::var("AGG_TIMING").is_ok();
    let t0 = std::time::Instant::now();

    // Null-group accumulators merged across states up front; also gather the
    // global key range and group count to pick the merge strategy.
    let mut raw_null: Option<Vec<AccumulatorState>> = None;
    let mut gmin = i64::MAX;
    let mut gmax = i64::MIN;
    let mut total = 0usize;
    let mut prepared: Vec<AggregationState> = Vec::with_capacity(states.len());
    for mut st in states {
        if let Some(n) = st.raw_null.take() {
            match &mut raw_null {
                Some(existing) => {
                    for (a, b) in existing.iter_mut().zip(n.iter()) {
                        a.merge(b);
                    }
                }
                None => raw_null = Some(n),
            }
        }
        total += st.raw_sums.len();
        for k in st.raw_sums.keys() {
            let v = *k as i64;
            gmin = gmin.min(v);
            gmax = gmax.max(v);
        }
        prepared.push(st);
    }

    let range = (gmax as i128 - gmin as i128 + 1).max(1) as u64;
    let dense =
        total > 0 && range <= 512_000_000 && (range as u128) <= 6 * total as u128 && gmax > gmin;
    let w = range.div_ceil(p as u64).max(1);

    let sharded: Vec<Vec<Vec<(u64, f64)>>> = if dense {
        prepared
            .into_iter()
            .map(|st| st.into_range_sum_shards(p, gmin, w))
            .collect()
    } else {
        prepared
            .into_iter()
            .map(|st| st.into_raw_sum_shards(p))
            .collect()
    };

    let mut shard_major: Vec<Vec<Vec<(u64, f64)>>> = (0..p).map(|_| Vec::new()).collect();
    for state_shards in sharded {
        for (pi, shard) in state_shards.into_iter().enumerate() {
            shard_major[pi].push(shard);
        }
    }
    let t_sharded = t0.elapsed();

    let mut batches: Vec<RecordBatch> = shard_major
        .into_par_iter()
        .enumerate()
        .map(|(pi, lists)| {
            let cap: usize = lists.iter().map(|l| l.len()).sum();
            if cap == 0 {
                return Ok(None);
            }
            let mut keys: Vec<u64> = Vec::with_capacity(cap);
            let mut sums: Vec<f64> = Vec::with_capacity(cap);
            if dense {
                // Direct-address merge: slot index -> dense entry position
                let lo = gmin + (pi as u64 * w) as i64;
                let width = if pi == p - 1 {
                    (gmax - lo + 1).max(1) as usize
                } else {
                    w as usize
                };
                let mut slots: Vec<u32> = vec![u32::MAX; width];
                for list in lists {
                    for (key, v) in list {
                        let idx = ((key as i64).wrapping_sub(lo)) as usize;
                        let slot = slots[idx];
                        if slot == u32::MAX {
                            slots[idx] = keys.len() as u32;
                            keys.push(key);
                            sums.push(v);
                        } else {
                            sums[slot as usize] += v;
                        }
                    }
                }
            } else {
                let mut map: HashMap<u64, u32> = HashMap::with_capacity(cap);
                for list in lists {
                    for (key, v) in list {
                        match map.entry(key) {
                            hashbrown::hash_map::Entry::Occupied(e) => {
                                sums[*e.get() as usize] += v;
                            }
                            hashbrown::hash_map::Entry::Vacant(slot) => {
                                slot.insert(keys.len() as u32);
                                keys.push(key);
                                sums.push(v);
                            }
                        }
                    }
                }
            }
            let key_array: ArrayRef = match schema.field(0).data_type() {
                DataType::Int32 => Arc::new(arrow::array::Int32Array::from(
                    keys.iter().map(|&k| k as i64 as i32).collect::<Vec<_>>(),
                )),
                DataType::Date32 => Arc::new(arrow::array::Date32Array::from(
                    keys.iter().map(|&k| k as i64 as i32).collect::<Vec<_>>(),
                )),
                _ => Arc::new(arrow::array::Int64Array::from(
                    keys.iter().map(|&k| k as i64).collect::<Vec<_>>(),
                )),
            };
            let sum_array: ArrayRef = Arc::new(arrow::array::Float64Array::from(sums));
            let batch =
                RecordBatch::try_new(schema.clone(), vec![key_array, sum_array]).map_err(|e| {
                    QueryError::Execution(format!("Failed to build output batch: {}", e))
                })?;
            match post_filter {
                Some(pred) => {
                    Ok(crate::physical::operators::filter_batches(vec![batch], pred)?.pop())
                }
                None => Ok(Some(batch)),
            }
        })
        .collect::<Result<Vec<Option<RecordBatch>>>>()?
        .into_iter()
        .flatten()
        .collect();

    if timing {
        eprintln!(
            "[raw-sum-merge] {} groups dense={} shard: {:?}; merge+build: {:?}; total: {:?}",
            total,
            dense,
            t_sharded,
            t0.elapsed() - t_sharded,
            t0.elapsed()
        );
    }

    if let Some(null_accs) = raw_null {
        let state = AggregationState::from_raw_groups(
            agg_funcs.to_vec(),
            input_types.to_vec(),
            raw_type.clone(),
            HashMap::new(),
            Some(null_accs),
        );
        if let Some(b) = build_filtered_output(&state, schema, post_filter)? {
            batches.push(b);
        }
    }
    Ok(batches)
}

/// Raw-key classification for normalize_raw / scalar_to_raw.
enum RawKey {
    Value(u64),
    Null,
}

/// Group key for hash table
#[derive(Clone)]
pub(crate) struct GroupKey {
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
pub(crate) enum AccumulatorState {
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
    /// ANY_VALUE / ARBITRARY: first non-null value wins
    First(Option<ScalarValue>),
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
            AggregateFunction::AnyValue | AggregateFunction::Arbitrary => {
                AccumulatorState::First(None)
            }
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
            AccumulatorState::First(v) => {
                if v.is_none() && !matches!(value, ScalarValue::Null) {
                    *v = Some(value.clone());
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
            AccumulatorState::First(v) => {
                if v.is_none() {
                    *v = Some(ScalarValue::Float64(value.into()));
                }
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
            AccumulatorState::First(v) => {
                if v.is_none() {
                    *v = Some(ScalarValue::Int64(value));
                }
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
            (AccumulatorState::First(a), AccumulatorState::First(b)) => {
                if a.is_none() {
                    *a = b.clone();
                }
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
            AccumulatorState::First(v) => v.clone().unwrap_or(ScalarValue::Null),
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
        ScalarValue::Int32(v) => *v as u64,
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
    Int32(&'a arrow::array::Int32Array),
    Float64(&'a Float64Array),
    String(&'a StringArray),
    Date32(&'a Date32Array),
    /// Dictionary-encoded strings: group keys use the dictionary INDEX
    /// (values only touched when a group's scalar must materialize).
    DictString(&'a arrow::array::DictionaryArray<arrow::datatypes::Int32Type>),
    Other(ArrayRef),
}

impl<'a> TypedArrayAccessor<'a> {
    fn from_array(array: &'a ArrayRef) -> Self {
        match array.data_type() {
            DataType::Int64 => {
                TypedArrayAccessor::Int64(array.as_any().downcast_ref::<Int64Array>().unwrap())
            }
            DataType::Int32 => TypedArrayAccessor::Int32(
                array
                    .as_any()
                    .downcast_ref::<arrow::array::Int32Array>()
                    .unwrap(),
            ),
            DataType::Float64 => {
                TypedArrayAccessor::Float64(array.as_any().downcast_ref::<Float64Array>().unwrap())
            }
            DataType::Utf8 => {
                TypedArrayAccessor::String(array.as_any().downcast_ref::<StringArray>().unwrap())
            }
            DataType::Date32 => {
                TypedArrayAccessor::Date32(array.as_any().downcast_ref::<Date32Array>().unwrap())
            }
            DataType::Dictionary(k, v) if **k == DataType::Int32 && **v == DataType::Utf8 => {
                TypedArrayAccessor::DictString(
                    array
                        .as_any()
                        .downcast_ref::<arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>(
                        )
                        .unwrap(),
                )
            }
            _ => TypedArrayAccessor::Other(array.clone()),
        }
    }

    /// Update accumulator directly without creating ScalarValue
    #[inline]
    fn update_accumulator(&self, row: usize, acc: &mut AccumulatorState) {
        // ANY_VALUE short-circuit: once set, skip value extraction entirely
        // (a per-row ScalarValue for a string column is an allocation).
        if let AccumulatorState::First(v) = acc {
            if v.is_none() {
                let value = self.extract_scalar(row);
                if !matches!(value, ScalarValue::Null) {
                    *v = Some(value);
                }
            }
            return;
        }
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
            TypedArrayAccessor::Int32(arr) => {
                if !arr.is_null(row) {
                    acc.update_i64(arr.value(row) as i64);
                }
            }
            TypedArrayAccessor::String(_)
            | TypedArrayAccessor::Date32(_)
            | TypedArrayAccessor::DictString(_) => {
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
            TypedArrayAccessor::DictString(arr) => {
                if arr.is_null(row) {
                    u64::MAX
                } else {
                    // Key on the VALUE bytes, not the index: different row
                    // groups/files may carry different dictionaries. Value
                    // lookup is one indirection; bytes-pack matches the
                    // String arm so mixed dict/plain states merge cleanly.
                    let values = arr.values().as_any().downcast_ref::<StringArray>().unwrap();
                    let s = values.value(arr.key(row).unwrap_or(0));
                    let b = s.as_bytes();
                    let mut key = (b.len() as u64) << 56;
                    for (i, &byte) in b.iter().take(7).enumerate() {
                        key |= (byte as u64) << (i * 8);
                    }
                    key
                }
            }
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
            TypedArrayAccessor::Int32(arr) => {
                if arr.is_null(row) {
                    u64::MAX
                } else {
                    arr.value(row) as i64 as u64
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

    /// Compare the value at `row` with a ScalarValue without allocating.
    fn value_equals_scalar(&self, row: usize, expected: &ScalarValue) -> bool {
        match self {
            TypedArrayAccessor::String(arr) => match expected {
                ScalarValue::Utf8(s) => !arr.is_null(row) && arr.value(row) == s.as_str(),
                ScalarValue::Null => arr.is_null(row),
                _ => false,
            },
            _ => self.extract_scalar(row) == *expected,
        }
    }

    /// Extract ScalarValue (slow path, needed for group keys)
    fn extract_scalar(&self, row: usize) -> ScalarValue {
        match self {
            TypedArrayAccessor::DictString(arr) => {
                if arr.is_null(row) {
                    ScalarValue::Null
                } else {
                    let values = arr.values().as_any().downcast_ref::<StringArray>().unwrap();
                    ScalarValue::Utf8(values.value(arr.key(row).unwrap_or(0)).to_string())
                }
            }
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
            TypedArrayAccessor::Int32(arr) => {
                if arr.is_null(row) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Int32(arr.value(row))
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
    /// Actual values per column, id-indexed — used to VERIFY raw-key hits for
    /// collision-prone key encodings (strings pack only 8 bytes + length:
    /// "Supplier#000000001" and "...002" collide and would merge groups).
    raw_key_values: Vec<Vec<ScalarValue>>,
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
    /// Raw-key fallback for a single Int64/Date32 group column: avoids the
    /// per-row GroupKey/ScalarValue allocation of `groups` (u64 is the
    /// bit-pattern of the value; nulls tracked separately).
    raw_groups: HashMap<u64, Vec<AccumulatorState>>,
    /// Specialized raw path for EXACTLY [Sum] over a Float64 input with a
    /// single raw-encodable group column: 16-byte (u64, f64) entries instead
    /// of a heap-boxed Vec<AccumulatorState> per group. Sum(f64) starts at
    /// 0.0 and ignores nulls, so a bare f64 is an exact drop-in. Q20's
    /// 4.4M-group packed-key aggregate spent most of its merge moving and
    /// freeing the boxes.
    raw_sums: HashMap<u64, f64>,
    raw_null: Option<Vec<AccumulatorState>>,
    raw_type: Option<DataType>,
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
            raw_key_values: Vec::new(),
            key_maps: Vec::new(),
            key_strides: Vec::new(),
            key_order: Vec::new(),
            perfect_capacity: 0,
            overflowed: false,
            groups: HashMap::new(),
            raw_groups: HashMap::new(),
            raw_sums: HashMap::new(),
            raw_null: None,
            raw_type: None,
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
        self.raw_key_values = (0..num_group_cols).map(|_| Vec::new()).collect();
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
                self.raw_key_values[col].push(accessor.extract_scalar(row));
            } else if matches!(
                accessor,
                TypedArrayAccessor::String(_)
                    | TypedArrayAccessor::Other(_)
                    | TypedArrayAccessor::DictString(_)
            ) {
                // Raw keys for strings/other types are lossy encodings — verify
                // the hit against the registered value; on collision, fall back
                // to the exact HashMap path for this whole state.
                if !accessor.value_equals_scalar(row, &self.raw_key_values[col][id as usize]) {
                    self.overflowed = true;
                    return None;
                }
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

        // Combined-dictionary fast path: when every group column is a
        // dictionary-encoded string with a small dictionary, resolve the
        // perfect-hash index ONCE per distinct key combination per batch
        // (packed table lookup per row afterwards).
        if !self.overflowed && !group_by_exprs.is_empty() {
            let dicts: Option<Vec<&arrow::array::DictionaryArray<arrow::datatypes::Int32Type>>> =
                group_accessors
                    .iter()
                    .map(|a| match a {
                        TypedArrayAccessor::DictString(d)
                            if d.values().len() <= 64 && d.null_count() == 0 =>
                        {
                            Some(*d)
                        }
                        _ => None,
                    })
                    .collect();
            if let Some(dicts) = dicts {
                if dicts.len() <= 2 {
                    let shift = 6 * (dicts.len() - 1);
                    let table_size = 1usize << (6 * dicts.len());
                    let mut table: Vec<u16> = vec![u16::MAX; table_size];
                    let keys0 = dicts[0].keys().values();
                    let keys1 = dicts.get(1).map(|d| d.keys().values());
                    let all_f64_inputs = agg_accessors
                        .iter()
                        .all(|a| matches!(a, TypedArrayAccessor::Float64(_)));
                    let f64_slices: Option<Vec<&[f64]>> = if all_f64_inputs {
                        Some(
                            agg_accessors
                                .iter()
                                .map(|a| match a {
                                    TypedArrayAccessor::Float64(arr) => arr.values().as_ref(),
                                    _ => unreachable!(),
                                })
                                .collect(),
                        )
                    } else {
                        None
                    };
                    for row in 0..num_rows {
                        let packed = match keys1 {
                            Some(k1) => ((keys0[row] as usize) << shift) | (k1[row] as usize),
                            None => keys0[row] as usize,
                        };
                        let mut idx = table[packed] as usize;
                        if idx == u16::MAX as usize {
                            match self.get_or_assign_perfect_index(&group_accessors, row) {
                                Some(i) => {
                                    table[packed] = i as u16;
                                    idx = i;
                                }
                                None => {
                                    self.drain_perfect_to_hashmap();
                                    self.process_rows_hashmap(
                                        row,
                                        num_rows,
                                        &group_accessors,
                                        &agg_accessors,
                                    );
                                    return Ok(());
                                }
                            }
                        }
                        let accs = &mut self.perfect_accs[idx];
                        if let Some(slices) = &f64_slices {
                            for (i, acc) in accs.iter_mut().enumerate() {
                                acc.update_f64(slices[i][row]);
                            }
                        } else {
                            for (i, acc) in accs.iter_mut().enumerate() {
                                agg_accessors[i].update_accumulator(row, acc);
                            }
                        }
                    }
                    return Ok(());
                }
            }
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
        // Raw fast path for a single Int64/Date32 group column: key the map by
        // the value's bit pattern instead of allocating a GroupKey per ROW.
        if group_accessors.len() == 1 {
            let raw_type = match &group_accessors[0] {
                TypedArrayAccessor::Int64(_) => Some(DataType::Int64),
                TypedArrayAccessor::Int32(_) => Some(DataType::Int32),
                TypedArrayAccessor::Date32(_) => Some(DataType::Date32),
                _ => None,
            };
            if let Some(rt) = raw_type {
                if self.raw_type.is_none() {
                    self.raw_type = Some(rt);
                    self.normalize_raw();
                }
                // Run detection: clustered keys (lineitem is ordered by
                // l_orderkey) produce runs of equal values — one map lookup
                // per RUN instead of per row. Unclustered data degrades
                // gracefully to runs of length 1.
                let key_at = |row: usize| -> (bool, u64) {
                    match &group_accessors[0] {
                        TypedArrayAccessor::Int64(a) => (a.is_null(row), a.value(row) as u64),
                        TypedArrayAccessor::Int32(a) => {
                            (a.is_null(row), a.value(row) as i64 as u64)
                        }
                        TypedArrayAccessor::Date32(a) => {
                            (a.is_null(row), a.value(row) as i64 as u64)
                        }
                        _ => unreachable!(),
                    }
                };
                // Pre-extract f64 value slices when every aggregate input is
                // a null-free Float64 column: the per-row update is then a
                // direct slice load + accumulator add.
                let f64_slices: Option<Vec<&[f64]>> = agg_accessors
                    .iter()
                    .map(|a| match a {
                        TypedArrayAccessor::Float64(arr) if arr.null_count() == 0 => {
                            Some(arr.values().as_ref())
                        }
                        _ => None,
                    })
                    .collect();

                // Bare-f64 fast path: exactly [Sum] over a Float64 input keeps
                // groups as (u64, f64) map entries — no per-group heap box.
                let use_raw_sums = self.agg_funcs.len() == 1
                    && matches!(self.agg_funcs[0], AggregateFunction::Sum)
                    && matches!(self.input_types.first(), Some(DataType::Float64))
                    && matches!(agg_accessors[0], TypedArrayAccessor::Float64(_));

                let mut row = start_row;
                while row < end_row {
                    let (is_null, raw) = key_at(row);
                    // Find the end of this run of identical keys
                    let mut run_end = row + 1;
                    while run_end < end_row {
                        let (n2, r2) = key_at(run_end);
                        if n2 != is_null || (!is_null && r2 != raw) {
                            break;
                        }
                        run_end += 1;
                    }
                    if use_raw_sums && !is_null {
                        let mut s = 0.0;
                        if let Some(slices) = &f64_slices {
                            let sl = slices[0];
                            for r in row..run_end {
                                s += sl[r];
                            }
                        } else if let TypedArrayAccessor::Float64(a) = &agg_accessors[0] {
                            for r in row..run_end {
                                if !a.is_null(r) {
                                    s += a.value(r);
                                }
                            }
                        }
                        *self.raw_sums.entry(raw).or_insert(0.0) += s;
                        row = run_end;
                        continue;
                    }
                    let accumulators = if is_null {
                        self.raw_null.get_or_insert_with(|| {
                            self.agg_funcs
                                .iter()
                                .zip(&self.input_types)
                                .map(|(func, dt)| AccumulatorState::new(func, dt))
                                .collect()
                        })
                    } else {
                        self.raw_groups.entry(raw).or_insert_with(|| {
                            self.agg_funcs
                                .iter()
                                .zip(&self.input_types)
                                .map(|(func, dt)| AccumulatorState::new(func, dt))
                                .collect()
                        })
                    };
                    if let Some(slices) = &f64_slices {
                        for r in row..run_end {
                            for (i, acc) in accumulators.iter_mut().enumerate() {
                                acc.update_f64(slices[i][r]);
                            }
                        }
                    } else {
                        for r in row..run_end {
                            for (i, acc) in accumulators.iter_mut().enumerate() {
                                agg_accessors[i].update_accumulator(r, acc);
                            }
                        }
                    }
                    row = run_end;
                }
                return;
            }
        }

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

    /// Convert a raw u64 key back into the GroupKey scalar it encodes.
    fn raw_key_to_scalar(&self, raw: u64) -> ScalarValue {
        match self.raw_type {
            Some(DataType::Date32) => ScalarValue::Date32(raw as i64 as i32),
            Some(DataType::Int32) => ScalarValue::Int32(raw as i64 as i32),
            _ => ScalarValue::Int64(raw as i64),
        }
    }

    /// Encode a single-scalar GroupKey as a raw u64 (when it matches raw_type).
    fn scalar_to_raw(&self, key: &GroupKey) -> Option<RawKey> {
        if key.values.len() != 1 {
            return None;
        }
        match (&key.values[0], &self.raw_type) {
            (ScalarValue::Null, _) => Some(RawKey::Null),
            (ScalarValue::Int64(v), Some(DataType::Int64)) => Some(RawKey::Value(*v as u64)),
            (ScalarValue::Int32(v), Some(DataType::Int32)) => Some(RawKey::Value(*v as i64 as u64)),
            (ScalarValue::Date32(v), Some(DataType::Date32)) => {
                Some(RawKey::Value(*v as i64 as u64))
            }
            _ => None,
        }
    }

    /// Move any GroupKey-keyed entries that encode raw-compatible keys into the
    /// raw maps so a group never lives in both maps at once.
    fn normalize_raw(&mut self) {
        if self.raw_type.is_none() || self.groups.is_empty() {
            return;
        }
        let keys: Vec<GroupKey> = self.groups.keys().cloned().collect();
        for key in keys {
            match self.scalar_to_raw(&key) {
                Some(RawKey::Value(raw)) => {
                    let accs = self.groups.remove(&key).unwrap();
                    match self.raw_groups.entry(raw) {
                        hashbrown::hash_map::Entry::Occupied(mut e) => {
                            for (a, b) in e.get_mut().iter_mut().zip(accs.iter()) {
                                a.merge(b);
                            }
                        }
                        hashbrown::hash_map::Entry::Vacant(v) => {
                            v.insert(accs);
                        }
                    }
                }
                Some(RawKey::Null) => {
                    let accs = self.groups.remove(&key).unwrap();
                    match &mut self.raw_null {
                        Some(existing) => {
                            for (a, b) in existing.iter_mut().zip(accs.iter()) {
                                a.merge(b);
                            }
                        }
                        None => self.raw_null = Some(accs),
                    }
                }
                None => {}
            }
        }
    }

    /// Check if a perfect hash slot has data.
    /// For GROUP BY without aggregates (DISTINCT-like), check key_order instead.
    fn slot_has_data(key: &GroupKey, accs: &[AccumulatorState]) -> bool {
        // If there are no accumulators (GROUP BY without aggregates),
        // check if the key slot was assigned (non-null key values)
        if accs.is_empty() {
            return !key.values.is_empty()
                && !key.values.iter().all(|v| matches!(v, ScalarValue::Null));
        }
        accs.iter().any(|a| match a {
            AccumulatorState::Count(c) => *c > 0,
            AccumulatorState::Sum(s) => *s != 0.0,
            AccumulatorState::SumInt(s) => *s != 0,
            AccumulatorState::Avg { count, .. } => *count > 0,
            AccumulatorState::Min(v) => v.is_some(),
            AccumulatorState::Max(v) => v.is_some(),
            AccumulatorState::BoolAnd(v) => v.is_some(),
            AccumulatorState::BoolOr(v) => v.is_some(),
            AccumulatorState::First(v) => v.is_some(),
            AccumulatorState::Variance { count, .. } => *count > 0,
        })
    }

    /// Drain perfect hash accumulators into the HashMap fallback
    fn drain_perfect_to_hashmap(&mut self) {
        for (idx, accs) in self.perfect_accs.drain(..).enumerate() {
            if idx < self.key_order.len() {
                let key = &self.key_order[idx];
                if Self::slot_has_data(key, &accs) {
                    self.groups.insert(key.clone(), accs);
                }
            }
        }
        self.key_order.clear();
    }

    /// Number of distinct groups currently held (for merge-strategy selection).
    pub fn group_count(&self) -> usize {
        let perfect = if !self.overflowed {
            self.perfect_accs
                .iter()
                .enumerate()
                .filter(|(idx, accs)| {
                    *idx < self.key_order.len() && Self::slot_has_data(&self.key_order[*idx], accs)
                })
                .count()
        } else {
            0
        };
        perfect
            + self.groups.len()
            + self.raw_groups.len()
            + self.raw_sums.len()
            + usize::from(self.raw_null.is_some())
    }

    /// Fold the bare-f64 sum groups back into boxed raw_groups entries. Used
    /// by every consumer that doesn't understand raw_sums, so the fast
    /// representation can never silently drop groups.
    pub(crate) fn demote_raw_sums(&mut self) {
        if self.raw_sums.is_empty() {
            return;
        }
        for (k, v) in self.raw_sums.drain() {
            match self.raw_groups.entry(k) {
                hashbrown::hash_map::Entry::Occupied(mut e) => {
                    if let AccumulatorState::Sum(s) = &mut e.get_mut()[0] {
                        *s += v;
                    }
                }
                hashbrown::hash_map::Entry::Vacant(slot) => {
                    slot.insert(vec![AccumulatorState::Sum(v)]);
                }
            }
        }
    }

    /// Inverse of demote_raw_sums: absorb boxed raw_groups (the pre-overflow
    /// perfect-hash residue) into the bare-f64 map. Returns false (leaving
    /// state unchanged) if any accumulator isn't a plain Sum.
    fn absorb_raw_groups_into_sums(&mut self) -> bool {
        if self
            .raw_groups
            .values()
            .any(|accs| accs.len() != 1 || !matches!(accs[0], AccumulatorState::Sum(_)))
        {
            return false;
        }
        for (k, accs) in self.raw_groups.drain() {
            if let AccumulatorState::Sum(s) = accs[0] {
                *self.raw_sums.entry(k).or_insert(0.0) += s;
            }
        }
        true
    }

    /// Shard the bare-f64 sum groups by multiplicative hash of the u64 key.
    fn into_raw_sum_shards(mut self, p: usize) -> Vec<Vec<(u64, f64)>> {
        let mut shards: Vec<Vec<(u64, f64)>> = (0..p).map(|_| Vec::new()).collect();
        for (raw, v) in self.raw_sums.drain() {
            let h = raw.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            shards[(h >> 33) as usize % p].push((raw, v));
        }
        shards
    }

    /// Shard the bare-f64 sum groups by key RANGE (dense direct-address merge).
    fn into_range_sum_shards(mut self, p: usize, min: i64, w: u64) -> Vec<Vec<(u64, f64)>> {
        let mut shards: Vec<Vec<(u64, f64)>> = (0..p).map(|_| Vec::new()).collect();
        for (raw, v) in self.raw_sums.drain() {
            let off = (raw as i64).wrapping_sub(min) as u64;
            shards[((off / w) as usize).min(p - 1)].push((raw, v));
        }
        shards
    }

    /// Consume this state, sharding its groups by key hash into `p` buckets.
    /// Used by the parallel partitioned merge: entries for the same key always
    /// land in the same bucket regardless of which thread produced them.
    pub(crate) fn into_shards(mut self, p: usize) -> Vec<Vec<(GroupKey, Vec<AccumulatorState>)>> {
        self.demote_raw_sums();
        self.drain_perfect_to_hashmap();
        let mut shards: Vec<Vec<(GroupKey, Vec<AccumulatorState>)>> =
            (0..p).map(|_| Vec::new()).collect();
        let mut push = |key: GroupKey, accs: Vec<AccumulatorState>| {
            let mut hasher = hashbrown::hash_map::DefaultHashBuilder::default().build_hasher();
            key.hash(&mut hasher);
            shards[(hasher.finish() as usize) % p].push((key, accs));
        };
        let raw_entries: Vec<(u64, Vec<AccumulatorState>)> = self.raw_groups.drain().collect();
        for (raw, accs) in raw_entries {
            push(
                GroupKey {
                    values: vec![self.raw_key_to_scalar(raw)],
                },
                accs,
            );
        }
        if let Some(accs) = self.raw_null.take() {
            push(
                GroupKey {
                    values: vec![ScalarValue::Null],
                },
                accs,
            );
        }
        for (key, accs) in self.groups.drain() {
            push(key, accs);
        }
        shards
    }

    /// Shard raw-keyed groups by multiplicative hash of the u64 key.
    /// Caller must have drained perfect entries and normalized to raw first.
    /// Partition raw groups by key RANGE: shard i owns [min + i*w, ...).
    /// With clustered keys each state's entries land in few shards, and the
    /// per-shard merge can use direct addressing instead of a hash map.
    pub(crate) fn into_range_shards(
        mut self,
        p: usize,
        min: i64,
        w: u64,
    ) -> Vec<Vec<(u64, Vec<AccumulatorState>)>> {
        self.demote_raw_sums();
        let mut shards: Vec<Vec<(u64, Vec<AccumulatorState>)>> =
            (0..p).map(|_| Vec::new()).collect();
        for (raw, accs) in self.raw_groups.drain() {
            let off = (raw as i64).wrapping_sub(min) as u64;
            shards[((off / w) as usize).min(p - 1)].push((raw, accs));
        }
        shards
    }

    pub(crate) fn into_raw_shards(mut self, p: usize) -> Vec<Vec<(u64, Vec<AccumulatorState>)>> {
        self.demote_raw_sums();
        let mut shards: Vec<Vec<(u64, Vec<AccumulatorState>)>> =
            (0..p).map(|_| Vec::new()).collect();
        for (raw, accs) in self.raw_groups.drain() {
            let h = raw.wrapping_mul(0x9E37_79B9_7F4A_7C15);
            shards[(h >> 33) as usize % p].push((raw, accs));
        }
        shards
    }

    /// Build a state holding pre-merged RAW groups (raw merge output path).
    pub(crate) fn from_raw_groups(
        agg_funcs: Vec<AggregateFunction>,
        input_types: Vec<DataType>,
        raw_type: DataType,
        raw_groups: HashMap<u64, Vec<AccumulatorState>>,
        raw_null: Option<Vec<AccumulatorState>>,
    ) -> Self {
        let mut state = Self::new(agg_funcs, input_types);
        state.overflowed = true;
        state.raw_type = Some(raw_type);
        state.raw_groups = raw_groups;
        state.raw_null = raw_null;
        state
    }

    /// Build a state that holds pre-merged groups (parallel merge output path).
    pub(crate) fn from_groups(
        agg_funcs: Vec<AggregateFunction>,
        input_types: Vec<DataType>,
        groups: HashMap<GroupKey, Vec<AccumulatorState>>,
    ) -> Self {
        let mut state = Self::new(agg_funcs, input_types);
        state.overflowed = true;
        state.groups = groups;
        state
    }

    /// Merge another state into this one
    pub fn merge(&mut self, other: &AggregationState) {
        // Raw-mode reconciliation: if either side holds raw-keyed groups, both
        // sides must abandon the perfect-hash arrays (a key must never live in
        // two places) and converge into the raw maps via normalize_raw below.
        let raw_involved = self.raw_type.is_some() || other.raw_type.is_some();
        if raw_involved {
            if self.raw_type.is_none() {
                self.raw_type = other.raw_type.clone();
            }
            if !self.overflowed {
                self.drain_perfect_to_hashmap();
                self.overflowed = true;
            }
        }
        // If other used perfect hash, merge into our perfect hash or groups
        if !other.overflowed && !other.perfect_accs.is_empty() {
            for (idx, other_accs) in other.perfect_accs.iter().enumerate() {
                if idx >= other.key_order.len() {
                    continue;
                }
                if !Self::slot_has_data(&other.key_order[idx], other_accs) {
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
                        // Overflow just happened - drain perfect hash entries to HashMap
                        // so they are not lost when build_output skips perfect_accs
                        if !self.perfect_accs.is_empty() {
                            self.drain_perfect_to_hashmap();
                        }
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
                    continue;
                }
                // Overflow just happened - drain perfect hash entries to HashMap
                if !self.perfect_accs.is_empty() {
                    self.drain_perfect_to_hashmap();
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

        // Merge raw-keyed entries
        for (raw, other_accs) in &other.raw_groups {
            match self.raw_groups.entry(*raw) {
                hashbrown::hash_map::Entry::Occupied(mut e) => {
                    for (a, b) in e.get_mut().iter_mut().zip(other_accs.iter()) {
                        a.merge(b);
                    }
                }
                hashbrown::hash_map::Entry::Vacant(v) => {
                    v.insert(other_accs.clone());
                }
            }
        }
        // Merge bare-f64 sum entries (folded into boxed raw_groups here; the
        // fast pair-based merge path never goes through this function).
        for (raw, v) in &other.raw_sums {
            match self.raw_groups.entry(*raw) {
                hashbrown::hash_map::Entry::Occupied(mut e) => {
                    if let AccumulatorState::Sum(s) = &mut e.get_mut()[0] {
                        *s += v;
                    }
                }
                hashbrown::hash_map::Entry::Vacant(slot) => {
                    slot.insert(vec![AccumulatorState::Sum(*v)]);
                }
            }
        }
        self.demote_raw_sums();
        if let Some(other_null) = &other.raw_null {
            match &mut self.raw_null {
                Some(existing) => {
                    for (a, b) in existing.iter_mut().zip(other_null.iter()) {
                        a.merge(b);
                    }
                }
                None => self.raw_null = Some(other_null.clone()),
            }
        }
        // Unify any GroupKey-shaped entries that encode raw keys
        if raw_involved {
            self.normalize_raw();
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

        // Raw-direct fast path: single integer group column with all groups in
        // the raw map — build the key array straight from the u64 keys and the
        // aggregate arrays from the accumulators, no ScalarValue per group.
        if num_group_cols == 1
            && !self.raw_groups.is_empty()
            && self.groups.is_empty()
            && (self.overflowed || self.perfect_accs.is_empty())
        {
            return self.build_output_raw(schema);
        }

        // Collect all groups from perfect hash, HashMap, and raw maps.
        // Raw keys are materialized as GroupKeys once, here at output time.
        let raw_keys: Vec<GroupKey> = self
            .raw_groups
            .keys()
            .map(|raw| GroupKey {
                values: vec![self.raw_key_to_scalar(*raw)],
            })
            .collect();
        let null_key = GroupKey {
            values: vec![ScalarValue::Null],
        };
        let mut all_groups: Vec<(&GroupKey, &Vec<AccumulatorState>)> = Vec::new();

        // From perfect hash
        if !self.overflowed {
            for (idx, accs) in self.perfect_accs.iter().enumerate() {
                if idx >= self.key_order.len() {
                    continue;
                }
                if Self::slot_has_data(&self.key_order[idx], accs) {
                    all_groups.push((&self.key_order[idx], accs));
                }
            }
        }

        // From HashMap
        for (key, accs) in &self.groups {
            all_groups.push((key, accs));
        }

        // From raw maps (raw_keys is ordered identically to the iteration here)
        for (gk, (_raw, accs)) in raw_keys.iter().zip(self.raw_groups.iter()) {
            all_groups.push((gk, accs));
        }
        if let Some(accs) = &self.raw_null {
            all_groups.push((&null_key, accs));
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

    /// build_output specialization for raw-keyed single-int-column states.
    fn build_output_raw(&self, schema: &SchemaRef) -> Result<RecordBatch> {
        let entries: Vec<(u64, &Vec<AccumulatorState>)> =
            self.raw_groups.iter().map(|(k, v)| (*k, v)).collect();
        build_output_raw_entries(&entries, self.raw_null.as_ref(), &self.agg_funcs, schema)
    }
}

/// Build the raw-mode output batch from an entry slice — shared by
/// AggregationState::build_output_raw and the range-partitioned dense merge
/// (which never materializes a HashMap).
pub(crate) fn build_output_raw_entries(
    entries: &[(u64, &Vec<AccumulatorState>)],
    raw_null: Option<&Vec<AccumulatorState>>,
    agg_funcs: &[AggregateFunction],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    {
        let has_null = raw_null.is_some();
        let num_groups = entries.len() + usize::from(has_null);

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        // Key column directly from raw bit patterns
        let key_array: ArrayRef = match schema.field(0).data_type() {
            DataType::Int32 => {
                let mut b = Int32Builder::with_capacity(num_groups);
                for (raw, _) in entries {
                    b.append_value(*raw as i64 as i32);
                }
                if has_null {
                    b.append_null();
                }
                Arc::new(b.finish())
            }
            DataType::Date32 => {
                let mut b = arrow::array::Date32Builder::with_capacity(num_groups);
                for (raw, _) in entries {
                    b.append_value(*raw as i64 as i32);
                }
                if has_null {
                    b.append_null();
                }
                Arc::new(b.finish())
            }
            _ => {
                let mut b = Int64Builder::with_capacity(num_groups);
                for (raw, _) in entries {
                    b.append_value(*raw as i64);
                }
                if has_null {
                    b.append_null();
                }
                Arc::new(b.finish())
            }
        };
        arrays.push(key_array);

        // Aggregate columns: build typed arrays directly from finalize()
        // results, skipping the intermediate Vec<ScalarValue> +
        // build_scalar_array double-dispatch (visible in Q18 profiles).
        for agg_idx in 0..agg_funcs.len() {
            let func = &agg_funcs[agg_idx];
            let field = schema.field(1 + agg_idx);
            let finalize_iter = entries
                .iter()
                .map(|(_, accs)| accs[agg_idx].finalize(func))
                .chain(raw_null.map(|accs| accs[agg_idx].finalize(func)));
            let array: ArrayRef = match field.data_type() {
                DataType::Int64 => {
                    let mut b = Int64Builder::with_capacity(num_groups);
                    for v in finalize_iter {
                        match v {
                            ScalarValue::Int64(x) => b.append_value(x),
                            ScalarValue::Null => b.append_null(),
                            other => b.append_option(scalar_to_i64(&other)),
                        }
                    }
                    Arc::new(b.finish())
                }
                DataType::Float64 => {
                    let mut b = Float64Builder::with_capacity(num_groups);
                    for v in finalize_iter {
                        match v {
                            ScalarValue::Float64(x) => b.append_value(x.into_inner()),
                            ScalarValue::Null => b.append_null(),
                            other => b.append_option(scalar_to_f64(&other)),
                        }
                    }
                    Arc::new(b.finish())
                }
                _ => {
                    let values: Vec<ScalarValue> = finalize_iter.collect();
                    build_scalar_array(&values, field.data_type())?
                }
            };
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

#[cfg(test)]
mod dict_accessor_tests {
    use super::*;
    use arrow::array::{DictionaryArray, Float64Array, StringArray};
    use arrow::datatypes::{DataType as ADataType, Field, Int32Type, Schema};
    use std::sync::Arc as StdArc;

    /// Dictionary-encoded group keys aggregate identically to plain strings,
    /// including across batches with DIFFERENT dictionaries for equal values.
    #[test]
    fn dictionary_group_keys_match_plain_strings() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new(
                "flag",
                ADataType::Dictionary(Box::new(ADataType::Int32), Box::new(ADataType::Utf8)),
                true,
            ),
            Field::new("v", ADataType::Float64, true),
        ]));

        // Batch 1: dict ["A", "B"], keys A,B,A
        let d1: DictionaryArray<Int32Type> =
            vec![Some("A"), Some("B"), Some("A")].into_iter().collect();
        let b1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(d1),
                StdArc::new(Float64Array::from(vec![1.0, 10.0, 2.0])),
            ],
        )
        .unwrap();
        // Batch 2: dict ["B", "A"] (reversed indices!), keys B,A
        let values = StringArray::from(vec!["B", "A"]);
        let keys = arrow::array::Int32Array::from(vec![0, 1]);
        let d2 = DictionaryArray::<Int32Type>::try_new(keys, StdArc::new(values)).unwrap();
        let b2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(d2),
                StdArc::new(Float64Array::from(vec![100.0, 4.0])),
            ],
        )
        .unwrap();

        let group_exprs = vec![Expr::column("flag")];
        let agg_exprs = vec![Expr::column("v")];
        let mut state =
            AggregationState::new(vec![AggregateFunction::Sum], vec![DataType::Float64]);
        state.process_batch(&b1, &group_exprs, &agg_exprs).unwrap();
        state.process_batch(&b2, &group_exprs, &agg_exprs).unwrap();

        let out_schema = StdArc::new(Schema::new(vec![
            Field::new("flag", ADataType::Utf8, true),
            Field::new("SUM(v)", ADataType::Float64, true),
        ]));
        let batch = state.build_output(&out_schema).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let flags = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut got: Vec<(String, f64)> = (0..2)
            .map(|i| (flags.value(i).to_string(), sums.value(i)))
            .collect();
        got.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(got[0].0, "A");
        assert!((got[0].1 - 7.0).abs() < 1e-9);
        assert_eq!(got[1].0, "B");
        assert!((got[1].1 - 110.0).abs() < 1e-9);
    }
}
