//! Morsel-driven aggregation execution
//!
//! Implements parallel aggregation using morsel-driven parallelism:
//! - Data is read in parallel from Parquet files
//! - Each thread maintains its own hash table
//! - Final merge combines all thread-local hash tables

mod admitted_output;
mod fixed_cell;
#[cfg(test)]
mod float_extrema_tests;
mod group_rows;
mod ingestion_controller;
#[cfg(test)]
mod ingestion_cursor_tests;
mod input_frontier;
mod key_rows;
pub(crate) mod live_spill;
#[cfg(test)]
mod migration_admission_tests;
mod output_quantum;
mod parallel_controllers;
mod parallel_merge;
mod partial_merge;
mod partition_scheduler;
#[cfg(test)]
mod prepared_input_tests;
mod prepared_keys;
mod raw_state;
mod repartition;
mod row_router;
mod row_selection;
mod run_merge;
mod scalar_state_codec;
mod selected_state;
mod spill_files;
mod spill_frames;
mod spill_io;
mod state_codec;
mod state_rows;
use crate::execution::reserved_vec::ReservedVec;
use raw_state::{RawRows, RawStateMap};

use crate::error::{QueryError, Result};
use crate::execution::{process_memory_pool, MemoryPool, ReservedBufferBuilder};
use crate::physical::morsel::{ParallelParquetSource, DEFAULT_MORSEL_SIZE};
use crate::physical::operators::evaluate_expr;
use crate::planner::{AggregateFunction, DecimalValue, Expr, ScalarValue};
use arrow::array::{
    Array, ArrayRef, BooleanArray, BooleanBuilder, Date32Array, Float64Array, Float64Builder,
    Int32Builder, Int64Array, Int64Builder, StringArray, StringBuilder,
};
use arrow::compute;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use hashbrown::HashMap;
use rayon::prelude::*;
use std::hash::{Hash, Hasher};
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
/// QE_AGG_PROF=1 section timers (ns), printed by the fused-agg driver.
pub static AGG_PROF_GROUP_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static AGG_PROF_AGGEVAL_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static AGG_PROF_PROCESS_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
pub static AGG_PROF_SCAN_NS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Opt-in worker-time accounting. These process-global counters are suitable
/// for serialized diagnostic queries, not concurrent per-query attribution.
pub(crate) struct AggProfileTimer {
    started: std::time::Instant,
    counter: &'static std::sync::atomic::AtomicU64,
}

impl AggProfileTimer {
    pub(crate) fn start(
        enabled: bool,
        counter: &'static std::sync::atomic::AtomicU64,
    ) -> Option<Self> {
        enabled.then(|| Self {
            started: std::time::Instant::now(),
            counter,
        })
    }
}

impl Drop for AggProfileTimer {
    fn drop(&mut self) {
        self.counter.fetch_add(
            self.started.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
}

pub(crate) fn merge_states_to_batches(
    states: Vec<AggregationState>,
    agg_funcs: &[AggregateFunction],
    input_types: &[DataType],
    schema: &SchemaRef,
    pool: &MemoryPool,
) -> Result<Vec<RecordBatch>> {
    merge_states_to_batches_filtered(states, agg_funcs, input_types, schema, None, pool)
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
    pool: &MemoryPool,
) -> Result<Vec<RecordBatch>> {
    const PARALLEL_MERGE_MIN_GROUPS: usize = 65_536;
    let total_groups: usize = states.iter().map(|s| s.group_count()).sum();

    // A single state has no cross-state duplicate keys to reconcile, so the
    // shard-then-parallel-merge machinery below (built to combine MULTIPLE
    // threads' overlapping group sets) is pure overhead for it, at any group
    // count. This matters most for disjoint-aggregation finalize
    // (`finalize_disjoint_states` calls in here with exactly one already-
    // disjoint worker state at a time): before this fix, a single oversized
    // (>65,536-group) worker state still paid real shard/rehash cost to
    // "merge" with nothing -- ~205ms/iteration measured on Q13 at SF=100
    // (469K groups/worker; SF=10's 46.9K groups/worker stays under the
    // threshold, so this never fired there). The worker's keys are HASH-
    // scattered across the full key range by the disjoint scatter, so the
    // `dense` range check below reads false even though the true key domain
    // is dense -- see duckdb-parity-2 tasks 002 and 006. `demote_raw_sums`
    // is the only prep genuinely needed first: `AggregationState::
    // build_output` already unions the perfect-hash, GroupKey and raw_groups
    // representations correctly on its own, but — unlike every other
    // consumer in this file — does not know about the bare-f64 `raw_sums`
    // representation, so skipping this step would silently drop any group
    // that took the bare-sum ingest fast path.
    if states.len() == 1 {
        let mut state = states.into_iter().next().unwrap();
        state.demote_raw_sums()?;
        return Ok(build_filtered_output(&state, schema, post_filter, pool)?
            .into_iter()
            .collect());
    }

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
            st.normalize_raw()?;
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
                    pool,
                );
            }
            return merge_raw_states_to_batches(
                prepared,
                agg_funcs,
                input_types,
                &dt,
                schema,
                post_filter,
                pool,
            );
        }
        // Fall through: into_shards handles mixed raw/GroupKey states.
        return merge_states_groupkey(prepared, agg_funcs, input_types, schema, post_filter, pool);
    }

    if states.len() > 1 && total_groups > PARALLEL_MERGE_MIN_GROUPS {
        return merge_states_groupkey(states, agg_funcs, input_types, schema, post_filter, pool);
    }

    let mut final_state =
        AggregationState::new_with_pool(agg_funcs.to_vec(), input_types.to_vec(), pool);
    for state in states {
        final_state.merge(&state)?;
    }
    let batches = vec![final_state.build_output_with_pool(schema, pool)?];
    match post_filter {
        Some(pred) => crate::physical::operators::filter_batches(batches, pred),
        None => Ok(batches),
    }
}

/// Finalize states whose group-key sets are DISJOINT by construction (the
/// fused streaming aggregate hash-partitions its input to per-worker
/// channels). No cross-state merge exists to do: each state builds its own
/// output in parallel and the batches concatenate. Running these through the
/// shard merge instead re-hashed every group to prove a disjointness the
/// producer already guaranteed — 4.3s of Q13's 7.5s at SF=100.
pub(crate) fn finalize_disjoint_states(
    states: Vec<AggregationState>,
    agg_funcs: &[AggregateFunction],
    input_types: &[DataType],
    schema: &SchemaRef,
    post_filter: Option<&Expr>,
    pool: &MemoryPool,
) -> Result<Vec<RecordBatch>> {
    use rayon::prelude::*;
    // Each state runs the SAME single-state pipeline the shared merge uses —
    // raw-type prep, perfect-hash drain, normalization — because
    // `build_output` alone is NOT sufficient for a state that abandoned its
    // perfect-hash array mid-build: its raw fast path emits only the raw
    // groups (Q11 at SF=100 lost ~70% of every sum this way, invisibly to a
    // row-count check). Disjointness removes the CROSS-state combine, not
    // the per-state finalize.
    let out: Result<Vec<Vec<RecordBatch>>> = states
        .into_par_iter()
        .filter(|s| s.group_count() > 0)
        .map(|state| {
            merge_states_to_batches_filtered(
                vec![state],
                agg_funcs,
                input_types,
                schema,
                post_filter,
                pool,
            )
        })
        .collect();
    Ok(out?
        .into_iter()
        .flatten()
        .filter(|b| b.num_rows() > 0)
        .collect())
}

/// Build a shard's output batch and apply the optional HAVING predicate while
/// still on the merging worker thread.
fn build_filtered_output(
    state: &AggregationState,
    schema: &SchemaRef,
    post_filter: Option<&Expr>,
    pool: &MemoryPool,
) -> Result<Option<RecordBatch>> {
    let batch = state.build_output_with_pool(schema, pool)?;
    match post_filter {
        Some(pred) => {
            let mut filtered = crate::physical::operators::filter_batches(vec![batch], pred)?;
            Ok(filtered.pop())
        }
        None => Ok(Some(batch)),
    }
}

/// Group entries a merge shard should hold before another shard is worth
/// creating.
///
/// The parallel merge splits every thread-local state into `p` shards, then
/// merges shard `i` of all states on one worker. With `p` fixed at the thread
/// count, a 4-group aggregate built 32 states x 32 shards = 1024 vectors and
/// 32 output batches to combine 128 entries. Sizing `p` by the entry count
/// keeps each shard worth a worker's while.
const MERGE_ENTRIES_PER_SHARD: usize = 4096;

/// Shard count for a parallel merge over `total_entries` group entries.
fn merge_shard_count(total_entries: usize) -> usize {
    let max = rayon::current_num_threads().clamp(2, 64);
    (total_entries / MERGE_ENTRIES_PER_SHARD).clamp(2, max)
}

/// GroupKey-based parallel shard merge (multi-column or non-integer keys).
fn merge_states_groupkey(
    states: Vec<AggregationState>,
    agg_funcs: &[AggregateFunction],
    input_types: &[DataType],
    schema: &SchemaRef,
    post_filter: Option<&Expr>,
    pool: &MemoryPool,
) -> Result<Vec<RecordBatch>> {
    let p = merge_shard_count(states.iter().map(|s| s.approx_group_count()).sum());
    let per_state_shards: Vec<_> = states
        .into_par_iter()
        .map(|s| s.into_shards(p))
        .collect::<Result<Vec<_>>>()?;

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
            build_filtered_output(&state, schema, post_filter, pool)
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
    pool: &MemoryPool,
) -> Result<Vec<RecordBatch>> {
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
        st.demote_raw_sums()?;
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

    // Shard count follows the entry count, now that it is known.
    let p = merge_shard_count(total);

    // Dense key domain (e.g. l_orderkey, c_custkey): range-partition the
    // entries and merge each shard with a direct-address table — the
    // hash-shard merge re-inserts every group into a fresh HashMap even
    // though almost no keys span threads.
    let candidate_range = crate::physical::dense_domain::bounded_i64_width(gmin, gmax, 512_000_000);
    let dense = total > 0
        && gmax > gmin
        && candidate_range.is_some_and(|range| (range as u128) <= 6 * total as u128);
    // The value is used for range routing only when dense is true. Invalid or
    // oversized domains must take the hash route, never narrow modulo 2^64.
    let range = candidate_range.unwrap_or(1) as u64;

    let sharded_states: Vec<Vec<RawRows>> = if dense {
        let w = range.div_ceil(p as u64).max(1);
        prepared
            .into_iter()
            .map(|st| st.into_range_shards(p, gmin, w))
            .collect::<Result<_>>()?
    } else {
        prepared
            .into_iter()
            .map(|st| st.into_raw_shards(p))
            .collect::<Result<_>>()?
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
            let mut rows = RawRows::new(pool, agg_funcs.len());
            if dense {
                let lo = gmin + (pi as u64 * w) as i64;
                let width = if pi == p - 1 {
                    (gmax - lo + 1).max(1) as usize
                } else {
                    w as usize
                };
                let mut slots = ReservedVec::<u32>::with_capacity(pool, width)?;
                slots.extend_reserved(width, std::iter::repeat_n(u32::MAX, width))?;
                for list in lists {
                    for (key, accs) in list.iter() {
                        let idx = ((*key as i64).wrapping_sub(lo)) as usize;
                        let slot = slots.as_mut_slice().get_mut(idx).ok_or_else(|| {
                            QueryError::Execution("aggregate dense key outside shard".into())
                        })?;
                        if *slot == u32::MAX {
                            *slot = u32::try_from(rows.push(*key, accs.iter().cloned())?).map_err(
                                |_| QueryError::Execution("aggregate dense index overflow".into()),
                            )?;
                        } else {
                            for (a, b) in rows.row_mut(*slot as usize).iter_mut().zip(accs) {
                                a.merge(b);
                            }
                        }
                    }
                }
            } else {
                let mut map = RawStateMap::new(pool, agg_funcs.len());
                for list in lists {
                    for (key, accs) in list.iter() {
                        map.insert_or_merge(*key, accs)?;
                    }
                }
                rows = map.take_rows();
            }
            let batch = build_output_raw_entries(
                rows.iter().map(|(key, values)| (*key, values)),
                None,
                agg_funcs,
                schema,
                pool,
            )?;
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
            pool,
        )?;
        if let Some(b) = build_filtered_output(&state, schema, post_filter, pool)? {
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
    pool: &MemoryPool,
) -> Result<Vec<RecordBatch>> {
    let timing = std::env::var("AGG_TIMING").is_ok();
    let t0 = std::time::Instant::now();

    // Null-group accumulators merged across states up front; also gather the
    // global key range and group count to pick the merge strategy.
    let mut raw_null: Option<Vec<AccumulatorState>> = None;
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
        prepared.push(st);
    }

    // Range/count probe and the shard scatter both iterate EVERY map entry;
    // sequential passes cost 1.4s of Q18's subquery aggregate at SF=100
    // (150M groups). Per-state work is independent — parallelize both.
    let per_state: Vec<(usize, i64, i64)> = prepared
        .par_iter()
        .map(|st| {
            let mut mn = i64::MAX;
            let mut mx = i64::MIN;
            for k in st.raw_sums.keys() {
                let v = *k as i64;
                mn = mn.min(v);
                mx = mx.max(v);
            }
            (st.raw_sums.len(), mn, mx)
        })
        .collect();
    let total: usize = per_state.iter().map(|(n, _, _)| n).sum();
    let gmin = per_state
        .iter()
        .map(|(_, mn, _)| *mn)
        .min()
        .unwrap_or(i64::MAX);
    let gmax = per_state
        .iter()
        .map(|(_, _, mx)| *mx)
        .max()
        .unwrap_or(i64::MIN);

    let p = merge_shard_count(total);
    let candidate_range = crate::physical::dense_domain::bounded_i64_width(gmin, gmax, 512_000_000);
    let dense = total > 0
        && gmax > gmin
        && candidate_range.is_some_and(|range| (range as u128) <= 6 * total as u128);
    // The value is used for range routing only when dense is true. Invalid or
    // oversized domains must take the hash route, never narrow modulo 2^64.
    let range = candidate_range.unwrap_or(1) as u64;
    let w = range.div_ceil(p as u64).max(1);

    let sharded: Vec<Vec<Vec<(u64, f64)>>> = if dense {
        prepared
            .into_par_iter()
            .map(|st| st.into_range_sum_shards(p, gmin, w))
            .collect()
    } else {
        prepared
            .into_par_iter()
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
                DataType::Date64 | DataType::Timestamp(_, _) => {
                    let values = keys
                        .iter()
                        .map(|&key| ScalarValue::Int64(key as i64))
                        .collect::<Vec<_>>();
                    build_scalar_array(&values, schema.field(0).data_type())?
                }
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
            pool,
        )?;
        if let Some(b) = build_filtered_output(&state, schema, post_filter, pool)? {
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
    /// SUM over a floating input. The bool is the "saw a non-NULL input"
    /// flag: SQL says SUM over a set with no non-NULL values is NULL, not 0,
    /// so the running total alone cannot be finalized (a real sum of 0.0 and
    /// an empty sum are indistinguishable otherwise).
    Sum(f64, bool),
    /// SUM over an integer input; same NULL semantics as `Sum`.
    SumInt(i64, bool),
    /// Exact coefficient, with sticky overflow carried through every merge.
    SumDecimal {
        coefficient: Option<i128>,
        scale: i8,
        seen: bool,
    },
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
    pub(crate) fn new(func: &AggregateFunction, input_type: &DataType) -> Self {
        match func {
            AggregateFunction::Count | AggregateFunction::CountDistinct => {
                AccumulatorState::Count(0)
            }
            AggregateFunction::Sum => match input_type {
                DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => {
                    AccumulatorState::SumInt(0, false)
                }
                DataType::Decimal128(_, scale) => AccumulatorState::SumDecimal {
                    coefficient: Some(0),
                    scale: *scale,
                    seen: false,
                },
                _ => AccumulatorState::Sum(0.0, false),
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
    pub(crate) fn update(&mut self, value: &ScalarValue) {
        match self {
            AccumulatorState::SumDecimal {
                coefficient,
                scale,
                seen,
            } => {
                if !matches!(value, ScalarValue::Null) {
                    *seen = true;
                    *coefficient = add_decimal_value(*coefficient, *scale, value);
                }
            }
            AccumulatorState::Count(c) => {
                if !matches!(value, ScalarValue::Null) {
                    *c += 1;
                }
            }
            AccumulatorState::Sum(s, seen) => {
                if let Some(v) = scalar_to_f64(value) {
                    *s += v;
                    *seen = true;
                }
            }
            AccumulatorState::SumInt(s, seen) => {
                if let Some(v) = scalar_to_i64(value) {
                    *s += v;
                    *seen = true;
                }
            }
            AccumulatorState::Avg { sum, count } => {
                if let Some(v) = scalar_to_f64(value) {
                    update_average(sum, count, v);
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
                    update_variance(count, mean, m2, x);
                }
            }
        }
    }

    /// Fast path: update with f64 value directly (no ScalarValue allocation)
    #[inline]
    fn update_f64(&mut self, value: f64) {
        match self {
            AccumulatorState::SumDecimal {
                coefficient, seen, ..
            } => {
                // Typed dispatch reaching this branch is a planner/type error.
                *coefficient = None;
                *seen = true;
            }
            AccumulatorState::Count(c) => *c += 1,
            AccumulatorState::Sum(s, seen) => {
                *s += value;
                *seen = true;
            }
            AccumulatorState::SumInt(s, seen) => {
                *s += value as i64;
                *seen = true;
            }
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
                        if crate::planner::numeric::sql_float_compare(
                            value,
                            crate::planner::BinaryOp::Lt,
                            current.into_inner(),
                        ) {
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
                        if crate::planner::numeric::sql_float_compare(
                            value,
                            crate::planner::BinaryOp::Gt,
                            current.into_inner(),
                        ) {
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
            AccumulatorState::SumDecimal {
                coefficient, seen, ..
            } => {
                // Typed dispatch reaching this branch is a planner/type error.
                *coefficient = None;
                *seen = true;
            }
            AccumulatorState::Count(c) => *c += 1,
            AccumulatorState::Sum(s, seen) => {
                *s += value as f64;
                *seen = true;
            }
            AccumulatorState::SumInt(s, seen) => {
                *s += value;
                *seen = true;
            }
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

    pub(crate) fn merge(&mut self, other: &AccumulatorState) {
        match (self, other) {
            (
                AccumulatorState::SumDecimal {
                    coefficient: a,
                    scale: sa,
                    seen: va,
                },
                AccumulatorState::SumDecimal {
                    coefficient: b,
                    scale: sb,
                    seen: vb,
                },
            ) => {
                *a = a.zip(*b).and_then(|(x, y)| {
                    DecimalValue::new(y, *sb)
                        .rescale(*sa)
                        .ok()
                        .and_then(|y| x.checked_add(y))
                });
                *va |= *vb;
            }
            (AccumulatorState::Count(a), AccumulatorState::Count(b)) => *a += b,
            (AccumulatorState::Sum(a, sa), AccumulatorState::Sum(b, sb)) => {
                *a += b;
                *sa |= *sb;
            }
            (AccumulatorState::SumInt(a, sa), AccumulatorState::SumInt(b, sb)) => {
                *a += b;
                *sa |= *sb;
            }
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

    pub(crate) fn finalize(&self, func: &AggregateFunction) -> Result<ScalarValue> {
        Ok(match self {
            AccumulatorState::SumDecimal {
                coefficient,
                scale,
                seen,
            } => {
                let value = coefficient.ok_or_else(|| {
                    QueryError::Execution("decimal SUM overflow or invalid input type".into())
                })?;
                if *seen {
                    DecimalValue::validate_precision(value, 38)?;
                    ScalarValue::Decimal128(DecimalValue::new(value, *scale))
                } else {
                    ScalarValue::Null
                }
            }
            AccumulatorState::Count(c) => ScalarValue::Int64(*c),
            // SUM over zero non-NULL inputs is NULL (SQL:2016 10.9 / DuckDB).
            AccumulatorState::Sum(s, seen) => {
                if *seen {
                    ScalarValue::Float64(ordered_float::OrderedFloat(*s))
                } else {
                    ScalarValue::Null
                }
            }
            AccumulatorState::SumInt(s, seen) => {
                if *seen {
                    ScalarValue::Int64(*s)
                } else {
                    ScalarValue::Null
                }
            }
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
                    return Ok(ScalarValue::Null);
                }
                let result = match func {
                    AggregateFunction::VarPop => *m2 / *count as f64,
                    AggregateFunction::Variance | AggregateFunction::VarSamp => {
                        if *count < 2 {
                            return Ok(ScalarValue::Null);
                        }
                        *m2 / (*count - 1) as f64
                    }
                    AggregateFunction::StddevPop => (*m2 / *count as f64).sqrt(),
                    AggregateFunction::Stddev | AggregateFunction::StddevSamp => {
                        if *count < 2 {
                            return Ok(ScalarValue::Null);
                        }
                        (*m2 / (*count - 1) as f64).sqrt()
                    }
                    _ => *m2 / *count as f64,
                };
                ScalarValue::Float64(ordered_float::OrderedFloat(result))
            }
        })
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
        (ScalarValue::Boolean(a), ScalarValue::Boolean(b)) => a.cmp(b),
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

// Shared arithmetic keeps compact live rows and general accumulators in the
// same operation order. Live callers check count overflow before entering.
#[inline]
fn add_decimal_value(sum: Option<i128>, scale: i8, value: &ScalarValue) -> Option<i128> {
    sum.and_then(|sum| match value {
        ScalarValue::Decimal128(v) => v.rescale(scale).ok().and_then(|v| sum.checked_add(v)),
        _ => None,
    })
}

#[inline]
fn update_average(sum: &mut f64, count: &mut i64, value: f64) {
    *sum += value;
    *count += 1;
}

#[inline]
fn update_variance(count: &mut i64, mean: &mut f64, m2: &mut f64, value: f64) {
    *count += 1;
    let delta = value - *mean;
    *mean += delta / *count as f64;
    let delta2 = value - *mean;
    *m2 += delta * delta2;
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
        ScalarValue::Decimal128(v) => Some(v.to_f64()),
        _ => None,
    }
}

/// Convert ScalarValue to a raw u64 key (matches TypedArrayAccessor::raw_key)
fn scalar_to_raw_key(value: &ScalarValue) -> u64 {
    match value {
        ScalarValue::Null => u64::MAX,
        ScalarValue::Int64(v) => *v as u64,
        ScalarValue::Int32(v) => *v as u64,
        ScalarValue::Float64(v) => crate::planner::numeric::sql_float_key(v.into_inner()),
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
    Decimal128(&'a arrow::array::Decimal128Array, i8, f64),
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
            DataType::Decimal128(_, scale) => TypedArrayAccessor::Decimal128(
                array
                    .as_any()
                    .downcast_ref::<arrow::array::Decimal128Array>()
                    .unwrap(),
                *scale,
                10_f64.powi(-(*scale as i32)),
            ),
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
            TypedArrayAccessor::Decimal128(arr, scale, factor) => {
                if !arr.is_null(row) {
                    let value = arr.value(row);
                    match acc {
                        AccumulatorState::SumDecimal {
                            coefficient,
                            scale: target,
                            seen,
                        } if target == scale => {
                            *seen = true;
                            *coefficient = coefficient.and_then(|sum| sum.checked_add(value));
                        }
                        AccumulatorState::Count(count) => *count += 1,
                        AccumulatorState::Avg { .. }
                        | AccumulatorState::Variance { .. }
                        | AccumulatorState::Sum(..) => acc.update_f64(value as f64 * factor),
                        _ => acc.update(&ScalarValue::Decimal128(DecimalValue::new(value, *scale))),
                    }
                }
            }
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
    /// Encodings may collide, including the NULL sentinel and signed -1.
    /// For strings, we hash the first 8 bytes plus length for a fast key.
    #[inline]
    fn raw_key(&self, row: usize) -> u64 {
        match self {
            TypedArrayAccessor::DictString(arr) => {
                if arr.is_null(row) {
                    u64::MAX
                } else {
                    // Key on the VALUE bytes, not the index: different row
                    // groups/files may carry different dictionaries. The
                    // pack must be BYTE-IDENTICAL to the String arm below or
                    // mixed dict/plain batches feeding one state split the
                    // same string into two groups (it packed take(7) while
                    // String packs min(8): every >=8-char value diverged —
                    // caught as a distributed-Q9 wrong answer when join
                    // gathers started emitting dictionaries).
                    let values = arr.values().as_any().downcast_ref::<StringArray>().unwrap();
                    let key_index = arr.key(row).unwrap();
                    if values.is_null(key_index) {
                        return u64::MAX;
                    }
                    let s = values.value(key_index);
                    let bytes = s.as_bytes();
                    let len = bytes.len().min(8);
                    let mut key = 0u64;
                    for (i, &byte) in bytes.iter().take(len).enumerate() {
                        key |= (byte as u64) << (i * 8);
                    }
                    key | ((bytes.len() as u64) << 56)
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
                    crate::planner::numeric::sql_float_key(arr.value(row))
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
            TypedArrayAccessor::Decimal128(..) => {
                let val = self.extract_scalar(row);
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                hash_scalar_value(&val, &mut hasher);
                std::hash::Hasher::finish(&hasher)
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
            TypedArrayAccessor::Decimal128(arr, scale, _) => {
                if arr.is_null(row) {
                    ScalarValue::Null
                } else {
                    ScalarValue::Decimal128(DecimalValue::new(arr.value(row), *scale))
                }
            }
            TypedArrayAccessor::DictString(arr) => {
                if arr.is_null(row) {
                    ScalarValue::Null
                } else {
                    let values = arr.values().as_any().downcast_ref::<StringArray>().unwrap();
                    let key_index = arr.key(row).unwrap();
                    if values.is_null(key_index) {
                        ScalarValue::Null
                    } else {
                        ScalarValue::Utf8(values.value(key_index).to_string())
                    }
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

/// Evaluate the input once, retaining normalized grouping and aggregate arrays.
/// The internal schema is positional: group columns, then aggregate inputs.
/// Routing, slicing and updates must preserve these arrays instead of evaluating
/// the source expressions again. This is not an aggregate-state spill format.
pub(crate) fn prepare_aggregate_batch(
    batch: &RecordBatch,
    group_by_exprs: &[Expr],
    agg_input_exprs: &[Expr],
) -> Result<RecordBatch> {
    let prof = std::env::var("QE_AGG_PROF").is_ok();
    let _process_timer = AggProfileTimer::start(prof, &AGG_PROF_PROCESS_NS);
    let t0 = prof.then(std::time::Instant::now);
    // Evaluate expressions once per batch
    let group_arrays: Vec<ArrayRef> = group_by_exprs
        .iter()
        .map(|expr| evaluate_expr(batch, expr).and_then(normalize_morsel_group_array))
        .collect::<Result<Vec<_>>>()?;
    if let Some(t) = t0 {
        AGG_PROF_GROUP_NS.fetch_add(
            t.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }
    let t1 = prof.then(std::time::Instant::now);
    let agg_arrays = crate::physical::operators::evaluate_aggregate_inputs(
        batch,
        agg_input_exprs.len(),
        |i| &agg_input_exprs[i],
        normalize_aggregate_array,
    )?;
    if let Some(t) = t1 {
        AGG_PROF_AGGEVAL_NS.fetch_add(
            t.elapsed().as_nanos() as u64,
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    let fields: Vec<arrow::datatypes::Field> = group_arrays
        .iter()
        .enumerate()
        .map(|(i, a)| {
            arrow::datatypes::Field::new(format!("group_{i}"), a.data_type().clone(), true)
        })
        .chain(agg_arrays.iter().enumerate().map(|(i, a)| {
            arrow::datatypes::Field::new(format!("aggregate_{i}"), a.data_type().clone(), true)
        }))
        .collect();
    let arrays = group_arrays.into_iter().chain(agg_arrays).collect();
    Ok(RecordBatch::try_new_with_options(
        Arc::new(arrow::datatypes::Schema::new(fields)),
        arrays,
        &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

/// Failure while applying retained evaluated input. Only a typed admission
/// denial permits resumption, and only at `next_row`; other errors remain terminal.
/// The cursor counts applied logical rows, not migration progress or groups.
#[derive(Debug)]
pub(crate) struct IngestionFailure {
    pub(crate) next_row: usize,
    pub(crate) error: QueryError,
}

impl IngestionFailure {
    fn at(next_row: usize, error: QueryError) -> Self {
        Self { next_row, error }
    }
}

type IngestionResult<T> = std::result::Result<T, IngestionFailure>;

/// Thread-local aggregation state.
///
/// Uses two strategies:
/// - **Perfect hash** (default): Fixed array indexed by group key, no hashing overhead.
///   Activated when the number of distinct groups ≤ PERFECT_HASH_MAX_GROUPS.
/// - **HashMap fallback**: Standard hash table for high-cardinality groups.
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
    /// Explicit occupancy: all-NULL keys and all-NULL aggregate inputs are real groups.
    perfect_occupied: Vec<bool>,
    /// Total number of slots in perfect_accs
    perfect_capacity: usize,
    /// Whether we overflowed and fell back to HashMap
    overflowed: bool,

    /// HashMap fallback: group key -> accumulator states
    groups: HashMap<GroupKey, Vec<AccumulatorState>>,
    /// Query-owned key-to-index table and flat accumulator arena for one
    /// Int64/Int32/Date32 group column. Nulls remain a separate single group.
    /// Outer key/state/hash capacity is admitted; nested scalar payloads are
    /// not yet independently reservation-owned.
    raw_groups: RawStateMap,
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
            perfect_occupied: Vec::new(),
            perfect_capacity: 0,
            overflowed: false,
            groups: HashMap::new(),
            raw_groups: RawStateMap::new(&process_memory_pool(), 0),
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
        Self::new_with_pool(agg_funcs, input_types, &process_memory_pool())
    }

    pub fn new_with_pool(
        agg_funcs: Vec<AggregateFunction>,
        input_types: Vec<DataType>,
        pool: &MemoryPool,
    ) -> Self {
        Self {
            raw_groups: RawStateMap::new(pool, agg_funcs.len()),
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
            } else if raw_key == u64::MAX
                || matches!(
                    accessor,
                    TypedArrayAccessor::String(_)
                        | TypedArrayAccessor::Decimal128(..)
                        | TypedArrayAccessor::Other(_)
                        | TypedArrayAccessor::DictString(_)
                )
            {
                // NULL shares its sentinel with valid numeric bit patterns.
                // Raw keys for strings/other types are also lossy — verify
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
                let mut new_occupied = vec![false; cap];
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
                    let has_data = self.perfect_occupied[old_idx];
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
                    new_occupied[new_idx] = true;
                    new_key_order[new_idx] = std::mem::replace(
                        &mut self.key_order[old_idx],
                        GroupKey {
                            values: vec![ScalarValue::Null; n],
                        },
                    );
                }

                self.perfect_accs = new_accs;
                self.key_order = new_key_order;
                self.perfect_occupied = new_occupied;
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
            self.perfect_occupied.resize(cap, false);
            self.perfect_capacity = cap;
        }

        // Phase 2: Compute flat_idx using final (correct) strides
        let mut flat_idx = 0usize;
        for col in 0..n {
            flat_idx += ids[col] as usize * self.key_strides[col];
        }

        if !self.perfect_occupied[flat_idx] {
            self.key_order[flat_idx] = GroupKey {
                values: group_accessors
                    .iter()
                    .map(|accessor| accessor.extract_scalar(row))
                    .collect(),
            };
            self.perfect_occupied[flat_idx] = true;
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

        let prepared = prepare_aggregate_batch(batch, group_by_exprs, agg_input_exprs)?;
        self.process_evaluated_batch(&prepared, group_by_exprs.len())
    }

    /// Apply already-evaluated arrays; ordinary callers retain terminal-error
    /// behavior. Spill-aware callers use the cursor API with the retained batch.
    pub(crate) fn process_evaluated_batch(
        &mut self,
        batch: &RecordBatch,
        group_count: usize,
    ) -> Result<()> {
        self.process_evaluated_from(batch, group_count, 0)
            .map(|_| ())
            .map_err(|failure| failure.error)
    }

    /// Resume at the first unapplied row of the same evaluated batch. Admission
    /// errors preserve every applied row and return the exact next-row cursor.
    /// This does not flush state or make unadmitted allocation paths spillable.
    pub(crate) fn process_evaluated_from(
        &mut self,
        batch: &RecordBatch,
        group_count: usize,
        start_row: usize,
    ) -> IngestionResult<usize> {
        if start_row > batch.num_rows()
            || group_count.checked_add(self.agg_funcs.len()) != Some(batch.num_columns())
        {
            return Err(IngestionFailure::at(
                start_row,
                QueryError::Execution("prepared aggregate input extent or arity mismatch".into()),
            ));
        }
        let num_rows = batch.num_rows();
        if start_row == num_rows {
            return Ok(num_rows);
        }
        if self.key_maps.is_empty() && group_count != 0 {
            self.init_perfect_hash(group_count);
        }
        let prof = std::env::var("QE_AGG_PROF").is_ok();
        let _process_timer = AggProfileTimer::start(prof, &AGG_PROF_PROCESS_NS);
        let (group_arrays, agg_arrays) = batch.columns().split_at(group_count);

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
        if !self.overflowed && group_count != 0 {
            // Fast combined-key components: a dictionary-encoded string's KEY
            // INDEX, or a null-free integer column whose per-batch range fits
            // 6 bits ((v - min) is the component — covers EXTRACT(YEAR ...)
            // and other small derived ints, which is how join-decorated
            // shapes like Q9's (n_name, o_year) reach this path).
            enum FastComp<'k> {
                Dict(&'k [i32]),
                IntOff { vals: Vec<i64>, min: i64 },
            }
            impl FastComp<'_> {
                #[inline]
                fn component(&self, row: usize) -> usize {
                    match self {
                        FastComp::Dict(keys) => keys[row] as usize,
                        FastComp::IntOff { vals, min } => (vals[row] - min) as usize,
                    }
                }
            }
            let int_comp = |vals: Vec<i64>| -> Option<FastComp<'static>> {
                let (mut lo, mut hi) = (i64::MAX, i64::MIN);
                for &v in &vals {
                    lo = lo.min(v);
                    hi = hi.max(v);
                }
                // Range metadata controls indexing only after a checked proof.
                // MIN..MAX spans overflow i64 subtraction and must use the
                // generic exact grouping path, never wrapped packed indices.
                if vals.is_empty() || hi.checked_sub(lo).is_some_and(|width| width <= 63) {
                    let min = if vals.is_empty() { 0 } else { lo };
                    Some(FastComp::IntOff { vals, min })
                } else {
                    None
                }
            };
            let comps: Option<Vec<FastComp>> = group_accessors
                .iter()
                .map(|a| match a {
                    TypedArrayAccessor::DictString(d)
                        if d.values().len() <= 64 && d.null_count() == 0 =>
                    {
                        Some(FastComp::Dict(d.keys().values()))
                    }
                    TypedArrayAccessor::Int64(arr) if arr.null_count() == 0 => {
                        int_comp(arr.values().to_vec())
                    }
                    TypedArrayAccessor::Int32(arr) if arr.null_count() == 0 => {
                        int_comp(arr.values().iter().map(|&v| v as i64).collect())
                    }
                    TypedArrayAccessor::Date32(arr) if arr.null_count() == 0 => {
                        int_comp(arr.values().iter().map(|&v| v as i64).collect())
                    }
                    _ => None,
                })
                .collect();
            // At least one DICTIONARY column must be present: an all-int key
            // set is already served well by the raw-key path, and requiring a
            // dict keeps this from hijacking shapes it wasn't measured on.
            let comps = comps.filter(|c| {
                c.iter().any(|k| matches!(k, FastComp::Dict(_)))
                    && !matches!(std::env::var("QE_AGG_FAST").as_deref(), Ok("0"))
            });
            if std::env::var("QE_AGG_PATH").is_ok() {
                eprintln!(
                    "[agg-path] combined={} types={:?}",
                    comps.as_ref().map(|c| c.len()).unwrap_or(0),
                    group_arrays
                        .iter()
                        .map(|a| a.data_type().clone())
                        .collect::<Vec<_>>()
                );
            }
            if let Some(comps) = comps {
                if comps.len() <= 2 {
                    let shift = 6 * (comps.len() - 1);
                    let table_size = 1usize << (6 * comps.len());
                    let mut table: Vec<u16> = vec![u16::MAX; table_size];
                    let keys0 = &comps[0];
                    let keys1 = comps.get(1);
                    let all_f64_inputs = agg_accessors.iter().all(
                        |a| matches!(a, TypedArrayAccessor::Float64(arr) if arr.null_count() == 0),
                    );
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
                    for row in start_row..num_rows {
                        let packed = match keys1 {
                            Some(k1) => (keys0.component(row) << shift) | k1.component(row),
                            None => keys0.component(row),
                        };
                        let mut idx = table[packed] as usize;
                        if idx == u16::MAX as usize {
                            // A new key id can change the perfect-hash
                            // STRIDES, which MOVES every existing group's
                            // flat index — cached table entries then point
                            // at other groups' accumulators. Detect the
                            // stride change and drop the cache. (This was
                            // latent in the dict-only version of this path:
                            // it survived only when discovery order never
                            // rehashed after the first cache fills.)
                            let strides_before = self.key_strides.clone();
                            match self.get_or_assign_perfect_index(&group_accessors, row) {
                                Some(i) => {
                                    if self.key_strides != strides_before {
                                        table.iter_mut().for_each(|t| *t = u16::MAX);
                                    }
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
                                    )?;
                                    return Ok(num_rows);
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
                    return Ok(num_rows);
                }
            }
            // Perfect hash fast path
            // Check if all aggregate inputs are f64 for the fastest possible path.
            // The slice load below reads arr.values() directly, which holds
            // arbitrary bytes under a NULL slot — so this path is only valid
            // for NULL-free inputs. A NULL-extended outer-join column reaches
            // here with real nulls, and counting those rows both corrupts the
            // total and marks the group as having seen data.
            let all_f64_inputs = agg_accessors
                .iter()
                .all(|a| matches!(a, TypedArrayAccessor::Float64(arr) if arr.null_count() == 0));

            if all_f64_inputs && !agg_accessors.is_empty() {
                // Ultra-fast path: pre-extract f64 slices and group key raw arrays
                let f64_slices: Vec<&[f64]> = agg_accessors
                    .iter()
                    .map(|a| match a {
                        TypedArrayAccessor::Float64(arr) => arr.values().as_ref(),
                        _ => unreachable!(),
                    })
                    .collect();

                for row in start_row..num_rows {
                    if let Some(idx) = self.get_or_assign_perfect_index(&group_accessors, row) {
                        let accs = &mut self.perfect_accs[idx];
                        for (i, acc) in accs.iter_mut().enumerate() {
                            acc.update_f64(f64_slices[i][row]);
                        }
                    } else {
                        self.drain_perfect_to_hashmap();
                        self.process_rows_hashmap(row, num_rows, &group_accessors, &agg_accessors)?;
                        break;
                    }
                }
            } else {
                // Generic perfect hash path
                for row in start_row..num_rows {
                    if let Some(idx) = self.get_or_assign_perfect_index(&group_accessors, row) {
                        let accs = &mut self.perfect_accs[idx];
                        for (i, acc) in accs.iter_mut().enumerate() {
                            agg_accessors[i].update_accumulator(row, acc);
                        }
                    } else {
                        self.drain_perfect_to_hashmap();
                        self.process_rows_hashmap(row, num_rows, &group_accessors, &agg_accessors)?;
                        break;
                    }
                }
            }
        } else if group_count == 0 {
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
                self.perfect_occupied.push(true);
            }
            let accs = &mut self.perfect_accs[0];
            for row in start_row..num_rows {
                for (i, acc) in accs.iter_mut().enumerate() {
                    agg_accessors[i].update_accumulator(row, acc);
                }
            }
        } else {
            // HashMap fallback
            self.process_rows_hashmap(start_row, num_rows, &group_accessors, &agg_accessors)?;
        }

        Ok(num_rows)
    }

    /// Process rows using HashMap (slow path)
    fn process_rows_hashmap(
        &mut self,
        start_row: usize,
        end_row: usize,
        group_accessors: &[TypedArrayAccessor],
        agg_accessors: &[TypedArrayAccessor],
    ) -> IngestionResult<()> {
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
                }
                // A previous migration may have failed after raw_type was set.
                // Reconcile its retained source entries before applying another row.
                self.normalize_raw()
                    .map_err(|e| IngestionFailure::at(start_row, e))?;
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
                // Requires a NULL-free input column: a bare f64 cannot encode
                // "this group saw no non-NULL value", which SQL finalizes as
                // NULL rather than 0. With nulls present the batch falls back
                // to the boxed Sum(f64, seen) accumulator, so every raw_sums
                // entry is a group that provably saw data.
                let use_raw_sums = self.agg_funcs.len() == 1
                    && matches!(self.agg_funcs[0], AggregateFunction::Sum)
                    && matches!(self.input_types.first(), Some(DataType::Float64))
                    && matches!(&agg_accessors[0],
                        TypedArrayAccessor::Float64(a) if a.null_count() == 0);

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
                    let accumulators: &mut [AccumulatorState] = if is_null {
                        self.raw_null.get_or_insert_with(|| {
                            self.agg_funcs
                                .iter()
                                .zip(&self.input_types)
                                .map(|(func, dt)| AccumulatorState::new(func, dt))
                                .collect()
                        })
                    } else {
                        self.raw_groups
                            .get_or_insert_with(raw, || {
                                self.agg_funcs
                                    .iter()
                                    .zip(&self.input_types)
                                    .map(|(func, dt)| AccumulatorState::new(func, dt))
                            })
                            .map_err(|e| IngestionFailure::at(row, e))?
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
                return Ok(());
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
        Ok(())
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
        Self::scalar_to_raw_type(key, &self.raw_type)
    }

    fn scalar_to_raw_type(key: &GroupKey, raw_type: &Option<DataType>) -> Option<RawKey> {
        if key.values.len() != 1 {
            return None;
        }
        match (&key.values[0], raw_type) {
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
    fn normalize_raw(&mut self) -> Result<()> {
        if self.raw_type.is_none() || self.groups.is_empty() {
            return Ok(());
        }
        let raw_type = &self.raw_type;
        let raw_groups = &mut self.raw_groups;
        let raw_null = &mut self.raw_null;
        let mut failure = None;
        // Remove a source entry only after its destination accepts it. On
        // refusal retain that entry and the suffix, while already transferred
        // entries belong solely to their destination. No cloned key list.
        self.groups.retain(|key, accs| {
            if failure.is_some() {
                return true;
            }
            match Self::scalar_to_raw_type(key, raw_type) {
                Some(RawKey::Value(raw)) => match raw_groups.insert_or_merge(raw, accs) {
                    Ok(()) => false,
                    Err(error) => {
                        failure = Some(error);
                        true
                    }
                },
                Some(RawKey::Null) => {
                    match raw_null {
                        Some(existing) => {
                            for (a, b) in existing.iter_mut().zip(accs.iter()) {
                                a.merge(b);
                            }
                        }
                        None => *raw_null = Some(std::mem::take(accs)),
                    }
                    false
                }
                None => true,
            }
        });
        failure.map_or(Ok(()), Err)
    }

    /// Drain perfect hash accumulators into the HashMap fallback
    fn drain_perfect_to_hashmap(&mut self) {
        for (idx, accs) in self.perfect_accs.drain(..).enumerate() {
            if idx < self.key_order.len() {
                let key = &self.key_order[idx];
                if self.perfect_occupied[idx] {
                    self.groups.insert(key.clone(), accs);
                }
            }
        }
        self.key_order.clear();
        self.perfect_occupied.clear();
    }

    /// Number of distinct groups currently held (for merge-strategy selection).
    pub fn group_count(&self) -> usize {
        let perfect = if !self.overflowed {
            self.perfect_accs
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx < self.key_order.len() && self.perfect_occupied[*idx])
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

    /// Fold bare-f64 sum groups into the fixed-arity raw state arena. Used
    /// by every consumer that doesn't understand raw_sums, so the fast
    /// representation can never silently drop groups.
    ///
    /// Every raw_sums entry is by construction a group that saw at least one
    /// non-NULL input (see `use_raw_sums`), so the demoted state is `seen`.
    pub(crate) fn demote_raw_sums(&mut self) -> Result<()> {
        let raw_groups = &mut self.raw_groups;
        let mut failure = None;
        self.raw_sums.retain(|&key, &mut value| {
            if failure.is_some() {
                return true;
            }
            match raw_groups.insert_or_merge(key, &[AccumulatorState::Sum(value, true)]) {
                Ok(()) => false,
                Err(error) => {
                    failure = Some(error);
                    true
                }
            }
        });
        failure.map_or(Ok(()), Err)
    }

    /// Inverse of demote_raw_sums: absorb arena raw_groups (the pre-overflow
    /// perfect-hash residue) into the bare-f64 map. Returns false (leaving
    /// state unchanged) if any accumulator isn't a plain Sum that already saw
    /// a non-NULL value — a bare f64 cannot represent "SUM is NULL", so a
    /// group that has only seen NULLs must stay boxed.
    fn absorb_raw_groups_into_sums(&mut self) -> bool {
        if self
            .raw_groups
            .values()
            .any(|accs| accs.len() != 1 || !matches!(accs[0], AccumulatorState::Sum(_, true)))
        {
            return false;
        }
        for (k, accs) in self.raw_groups.iter() {
            if let AccumulatorState::Sum(s, _) = accs[0] {
                *self.raw_sums.entry(*k).or_insert(0.0) += s;
            }
        }
        self.raw_groups.clear();
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

    /// Number of group entries this state holds, across every representation
    /// it might be using. Cheap (four `len()` calls) and used only to size the
    /// merge fan-out, so an over-count from a state that is mid-promotion is
    /// harmless.
    pub(crate) fn approx_group_count(&self) -> usize {
        self.groups.len() + self.raw_groups.len() + self.raw_sums.len() + self.key_order.len()
    }

    /// Consume this state, sharding its groups by key hash into `p` buckets.
    /// Used by the parallel partitioned merge: entries for the same key always
    /// land in the same bucket regardless of which thread produced them.
    pub(crate) fn into_shards(
        mut self,
        p: usize,
    ) -> Result<Vec<Vec<(GroupKey, Vec<AccumulatorState>)>>> {
        self.demote_raw_sums()?;
        self.drain_perfect_to_hashmap();
        let mut shards: Vec<Vec<(GroupKey, Vec<AccumulatorState>)>> =
            (0..p).map(|_| Vec::new()).collect();
        let mut push = |key: GroupKey, accs: Vec<AccumulatorState>| {
            // EXPLICITLY seeded: partition routing must give the same answer for
            // the same key from every call site. hashbrown 0.14's default hasher
            // happened to be deterministic across instances (ahash with fixed
            // fallback keys); 0.17's foldhash seeds PER INSTANCE, which shattered
            // groups across partitions. Never rely on a default for this.
            let mut hasher = xxhash_rust::xxh64::Xxh64::new(0x517c_c1b7_2722_0a95);
            key.hash(&mut hasher);
            shards[(hasher.finish() as usize) % p].push((key, accs));
        };
        let raw_entries = self.raw_groups.take_rows();
        for (raw, accs) in raw_entries.iter() {
            push(
                GroupKey {
                    values: vec![self.raw_key_to_scalar(*raw)],
                },
                accs.to_vec(),
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
        Ok(shards)
    }

    /// Shard raw-keyed groups by multiplicative hash of the u64 key.
    /// Caller must have drained perfect entries and normalized to raw first.
    /// Partition raw groups by key RANGE: shard i owns [min + i*w, ...).
    /// With clustered keys each state's entries land in few shards, and the
    /// per-shard merge can use direct addressing instead of a hash map.
    pub(crate) fn into_range_shards(mut self, p: usize, min: i64, w: u64) -> Result<Vec<RawRows>> {
        self.demote_raw_sums()?;
        self.raw_groups.take_rows().shard(p, |raw| {
            let off = (raw as i64).wrapping_sub(min) as u64;
            ((off / w) as usize).min(p - 1)
        })
    }

    pub(crate) fn into_raw_shards(mut self, p: usize) -> Result<Vec<RawRows>> {
        self.demote_raw_sums()?;
        self.raw_groups.take_rows().shard(p, |raw| {
            (raw.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 33) as usize % p
        })
    }

    /// Build a state holding pre-merged RAW groups (raw merge output path).
    pub(crate) fn from_raw_groups(
        agg_funcs: Vec<AggregateFunction>,
        input_types: Vec<DataType>,
        raw_type: DataType,
        raw_groups: HashMap<u64, Vec<AccumulatorState>>,
        raw_null: Option<Vec<AccumulatorState>>,
        pool: &MemoryPool,
    ) -> Result<Self> {
        let mut state = Self::new_with_pool(agg_funcs, input_types, pool);
        state.overflowed = true;
        state.raw_type = Some(raw_type);
        for (key, values) in raw_groups {
            state.raw_groups.insert_or_merge(key, &values)?;
        }
        state.raw_null = raw_null;
        Ok(state)
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
    pub fn merge(&mut self, other: &AggregationState) -> Result<()> {
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
                if !other.perfect_occupied[idx] {
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
                        self.perfect_occupied.resize(self.key_order.len(), false);
                        self.perfect_occupied[our_idx] = true;

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
                    self.perfect_occupied.resize(self.key_order.len(), false);
                    self.perfect_occupied[our_idx] = true;
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

        for (raw, values) in other.raw_groups.iter() {
            self.raw_groups.insert_or_merge(*raw, values)?;
        }
        for (raw, value) in &other.raw_sums {
            self.raw_groups
                .insert_or_merge(*raw, &[AccumulatorState::Sum(*value, true)])?;
        }
        self.demote_raw_sums()?;
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
            self.normalize_raw()?;
        }
        Ok(())
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
                let mut new_occupied = vec![false; cap];
                let mut new_key_order: Vec<GroupKey> = (0..cap)
                    .map(|_| GroupKey {
                        values: vec![ScalarValue::Null; n],
                    })
                    .collect();

                for old_idx in 0..old_capacity.min(self.perfect_accs.len()) {
                    if old_idx >= self.key_order.len() {
                        continue;
                    }
                    let has_data = self.perfect_occupied[old_idx];
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
                    new_occupied[new_idx] = true;
                    new_key_order[new_idx] = std::mem::replace(
                        &mut self.key_order[old_idx],
                        GroupKey {
                            values: vec![ScalarValue::Null; n],
                        },
                    );
                }

                self.perfect_accs = new_accs;
                self.key_order = new_key_order;
                self.perfect_occupied = new_occupied;
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
            self.perfect_occupied.resize(cap, false);
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
        self.build_output_with_pool(schema, &process_memory_pool())
    }

    /// Decimal payloads retain their explicit pool reservation through Arrow clones/slices.
    /// Other state, key and kernel allocations retain their existing accounting contracts.
    pub(crate) fn build_output_with_pool(
        &self,
        schema: &SchemaRef,
        pool: &MemoryPool,
    ) -> Result<RecordBatch> {
        let num_group_cols = schema.fields().len() - self.agg_funcs.len();

        // Raw-direct fast path: single integer group column with all groups in
        // the raw map — build the key array straight from the u64 keys and the
        // aggregate arrays from the accumulators, no ScalarValue per group.
        if num_group_cols == 1
            && !self.raw_groups.is_empty()
            && self.groups.is_empty()
            && (self.overflowed || self.perfect_accs.is_empty())
        {
            return self.build_output_raw(schema, pool);
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
        let mut all_groups: Vec<(&GroupKey, &[AccumulatorState])> = Vec::new();

        // From perfect hash
        if !self.overflowed {
            for (idx, accs) in self.perfect_accs.iter().enumerate() {
                if idx >= self.key_order.len() {
                    continue;
                }
                if self.perfect_occupied[idx] {
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

        // A GLOBAL (ungrouped) aggregate always produces exactly one row, even
        // over an empty input: SQL evaluates the set functions over the empty
        // multiset, giving 0 for COUNT and NULL for SUM/MIN/MAX/AVG. (With a
        // GROUP BY, an empty input correctly produces zero rows — the grouping
        // set is empty. That asymmetry is why this is keyed on
        // num_group_cols == 0.) The state never allocated an accumulator
        // because process_batch was never reached with a non-empty batch, so
        // synthesize the initial accumulators here.
        let empty_key = GroupKey { values: vec![] };
        let empty_accs: Vec<AccumulatorState>;
        if num_group_cols == 0 && all_groups.is_empty() {
            empty_accs = self
                .agg_funcs
                .iter()
                .enumerate()
                .map(|(i, func)| {
                    let dt = self
                        .input_types
                        .get(i)
                        .cloned()
                        .unwrap_or(DataType::Float64);
                    AccumulatorState::new(func, &dt)
                })
                .collect();
            all_groups.push((&empty_key, &empty_accs));
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
            let values = all_groups
                .iter()
                .map(|(_, accs)| accs[agg_idx].finalize(func));
            let field = schema.field(num_group_cols + agg_idx);
            let array = if let DataType::Decimal128(p, s) = field.data_type() {
                build_decimal_output(values, num_groups, *p, *s, pool)?
            } else {
                let values: Vec<ScalarValue> = values.collect::<Result<_>>()?;
                build_scalar_array(&values, field.data_type())?
            };
            arrays.push(array);
        }

        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| QueryError::Execution(format!("Failed to build output batch: {}", e)))
    }

    /// build_output specialization for raw-keyed single-int-column states.
    fn build_output_raw(&self, schema: &SchemaRef, pool: &MemoryPool) -> Result<RecordBatch> {
        build_output_raw_entries(
            self.raw_groups.iter().map(|(k, v)| (*k, v)),
            self.raw_null.as_deref(),
            &self.agg_funcs,
            schema,
            pool,
        )
    }
}

/// Build raw output by replaying a borrowed iterator without collecting row references.
/// Shared by
/// AggregationState::build_output_raw and the range-partitioned dense merge
/// (which never materializes a HashMap).
pub(crate) fn build_output_raw_entries<'a>(
    entries: impl ExactSizeIterator<Item = (u64, &'a [AccumulatorState])> + Clone,
    raw_null: Option<&[AccumulatorState]>,
    agg_funcs: &[AggregateFunction],
    schema: &SchemaRef,
    pool: &MemoryPool,
) -> Result<RecordBatch> {
    {
        let has_null = raw_null.is_some();
        let num_groups = entries.len() + usize::from(has_null);

        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.fields().len());

        // Key column directly from raw bit patterns
        let key_array: ArrayRef = match schema.field(0).data_type() {
            DataType::Int32 => {
                let mut b = Int32Builder::with_capacity(num_groups);
                for (raw, _) in entries.clone() {
                    b.append_value(raw as i64 as i32);
                }
                if has_null {
                    b.append_null();
                }
                Arc::new(b.finish())
            }
            DataType::Date32 => {
                let mut b = arrow::array::Date32Builder::with_capacity(num_groups);
                for (raw, _) in entries.clone() {
                    b.append_value(raw as i64 as i32);
                }
                if has_null {
                    b.append_null();
                }
                Arc::new(b.finish())
            }
            DataType::Date64 | DataType::Timestamp(_, _) => {
                let values: Vec<ScalarValue> = entries
                    .clone()
                    .map(|(raw, _)| ScalarValue::Int64(raw as i64))
                    .chain(has_null.then_some(ScalarValue::Null))
                    .collect();
                build_scalar_array(&values, schema.field(0).data_type())?
            }
            _ => {
                let mut b = Int64Builder::with_capacity(num_groups);
                for (raw, _) in entries.clone() {
                    b.append_value(raw as i64);
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
                .clone()
                .map(|(_, accs)| accs[agg_idx].finalize(func))
                .chain(raw_null.map(|accs| accs[agg_idx].finalize(func)));
            let array: ArrayRef = match field.data_type() {
                DataType::Decimal128(p, s) => {
                    build_decimal_output(finalize_iter, num_groups, *p, *s, pool)?
                }
                DataType::Int64 => {
                    let mut b = Int64Builder::with_capacity(num_groups);
                    for v in finalize_iter {
                        match v? {
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
                        match v? {
                            ScalarValue::Float64(x) => b.append_value(x.into_inner()),
                            ScalarValue::Null => b.append_null(),
                            other => b.append_option(scalar_to_f64(&other)),
                        }
                    }
                    Arc::new(b.finish())
                }
                _ => {
                    let values: Vec<ScalarValue> = finalize_iter.collect::<Result<_>>()?;
                    build_scalar_array(&values, field.data_type())?
                }
            };
            arrays.push(array);
        }

        RecordBatch::try_new(schema.clone(), arrays)
            .map_err(|e| QueryError::Execution(format!("Failed to build output batch: {}", e)))
    }
}

/// Retain the exact dictionary encoding supported by the morsel key accessor.
/// This preserves the combined-key cache without weakening normalization for
/// generic hash consumers or aggregate inputs. DictString checks both key and
/// dictionary-value validity and compares logical values across codebooks.
fn normalize_morsel_group_array(array: ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Dictionary(key, value)
            if **key == DataType::Int32 && **value == DataType::Utf8 =>
        {
            Ok(array)
        }
        _ => normalize_aggregate_array(array),
    }
}

/// Normalize only evaluated aggregate/key arrays, never the entire input batch.
/// Unsupported scalar domains fail here rather than becoming fabricated NULLs.
pub(crate) fn normalize_aggregate_array(array: ArrayRef) -> Result<ArrayRef> {
    match array.data_type() {
        DataType::Dictionary(_, value_type) => {
            let decoded = compute::cast_with_options(
                array.as_ref(),
                value_type,
                &compute::CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )?;
            normalize_aggregate_array(decoded)
        }
        // Keep temporal counts in their original unit. The output schema owns
        // unit/timezone metadata; no conversion through microsecond scalars occurs.
        DataType::Date64 | DataType::Timestamp(_, _) => Ok(compute::cast_with_options(
            array.as_ref(),
            &DataType::Int64,
            &compute::CastOptions {
                safe: false,
                ..Default::default()
            },
        )?),
        DataType::LargeUtf8 | DataType::Utf8View => Ok(compute::cast_with_options(
            array.as_ref(),
            &DataType::Utf8,
            &compute::CastOptions {
                safe: false,
                ..Default::default()
            },
        )?),
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::Date32
        | DataType::Decimal128(..) => Ok(array),
        other => Err(QueryError::NotImplemented(format!(
            "Aggregate scalar domain unsupported: {other:?}"
        ))),
    }
}

pub(crate) fn extract_scalar(array: &ArrayRef, row: usize) -> ScalarValue {
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
            let decimal = DecimalValue::new(val, *s);
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

/// Finalize exact decimal values straight into admitted Arrow buffers. The
/// iterator can fail; both partial buffers are released on any conversion error.
/// Admission precedes iteration/allocation and returned buffers own the leases.
fn build_decimal_output(
    values: impl IntoIterator<Item = Result<ScalarValue>>,
    rows: usize,
    precision: u8,
    scale: i8,
    pool: &MemoryPool,
) -> Result<ArrayRef> {
    use arrow::array::Decimal128Array;
    use arrow::buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
    let mut coefficients = ReservedBufferBuilder::<i128>::with_capacity(pool, rows)?;
    let bytes = rows.div_ceil(8);
    let mut validity = ReservedBufferBuilder::<u8>::with_capacity(pool, bytes)?;
    validity.extend_reserved(bytes, std::iter::repeat(0u8))?;
    let mut has_null = false;
    coefficients.try_extend_reserved(
        rows,
        values
            .into_iter()
            .enumerate()
            .map(|(i, value)| match value? {
                ScalarValue::Decimal128(value) => {
                    let scaled = value.rescale(scale)?;
                    DecimalValue::validate_precision(scaled, precision)?;
                    validity.as_mut_slice()[i / 8] |= 1 << (i % 8);
                    Ok(scaled)
                }
                ScalarValue::Null => {
                    has_null = true;
                    Ok(0)
                }
                other => Err(QueryError::Type(format!(
                    "non-decimal aggregate value {other:?} for {:?}",
                    DataType::Decimal128(precision, scale)
                ))),
            }),
    )?;
    let nulls = if has_null {
        Some(NullBuffer::new(BooleanBuffer::new(
            validity.finish(),
            0,
            rows,
        )))
    } else {
        None
    };
    let array = Decimal128Array::new(ScalarBuffer::new(coefficients.finish(), 0, rows), nulls)
        .with_precision_and_scale(precision, scale)
        .map_err(|e| QueryError::Execution(format!("Invalid decimal precision/scale: {e}")))?;
    Ok(Arc::new(array))
}

pub(crate) fn build_scalar_array(values: &[ScalarValue], data_type: &DataType) -> Result<ArrayRef> {
    let refs: Vec<&ScalarValue> = values.iter().collect();
    build_scalar_array_ref(&refs, data_type)
}

fn build_scalar_array_ref(values: &[&ScalarValue], data_type: &DataType) -> Result<ArrayRef> {
    macro_rules! integer_array {
        ($array:ty, $native:ty) => {{
            let mut output = Vec::with_capacity(values.len());
            for value in values {
                let value = match value {
                    ScalarValue::Null => {
                        output.push(None);
                        continue;
                    }
                    ScalarValue::Int8(v) => *v as i128,
                    ScalarValue::Int16(v) => *v as i128,
                    ScalarValue::Int32(v) => *v as i128,
                    ScalarValue::Int64(v) => *v as i128,
                    ScalarValue::UInt8(v) => *v as i128,
                    ScalarValue::UInt16(v) => *v as i128,
                    ScalarValue::UInt32(v) => *v as i128,
                    ScalarValue::UInt64(v) => *v as i128,
                    other => {
                        return Err(QueryError::Type(format!(
                            "aggregate value {other:?} does not match {data_type:?}"
                        )))
                    }
                };
                output.push(Some(<$native>::try_from(value).map_err(|_| {
                    QueryError::Execution(format!("aggregate {data_type:?} overflow"))
                })?));
            }
            Ok(Arc::new(<$array>::from(output)) as ArrayRef)
        }};
    }
    match data_type {
        DataType::Null if values.iter().all(|v| matches!(v, ScalarValue::Null)) => {
            Ok(arrow::array::new_null_array(data_type, values.len()))
        }
        DataType::Date64 | DataType::Timestamp(_, _) => {
            // Int64 <-> temporal casts preserve raw counts, unlike casts between
            // timestamp units. Preserve the declared unit and timezone verbatim.
            let counts = build_scalar_array_ref(values, &DataType::Int64)?;
            Ok(compute::cast_with_options(
                counts.as_ref(),
                data_type,
                &compute::CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )?)
        }
        DataType::Int8 => integer_array!(arrow::array::Int8Array, i8),
        DataType::Int16 => integer_array!(arrow::array::Int16Array, i16),
        DataType::UInt8 => integer_array!(arrow::array::UInt8Array, u8),
        DataType::UInt16 => integer_array!(arrow::array::UInt16Array, u16),
        DataType::UInt32 => integer_array!(arrow::array::UInt32Array, u32),
        DataType::UInt64 => integer_array!(arrow::array::UInt64Array, u64),
        DataType::Float32 => {
            let output = values
                .iter()
                .map(|value| match value {
                    ScalarValue::Float32(value) => Ok(Some(value.into_inner())),
                    ScalarValue::Null => Ok(None),
                    other => Err(QueryError::Type(format!(
                        "aggregate value {other:?} does not match {data_type:?}"
                    ))),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(arrow::array::Float32Array::from(output)))
        }
        DataType::Dictionary(_, value_type) => {
            let decoded = build_scalar_array_ref(values, value_type)?;
            Ok(compute::cast_with_options(
                decoded.as_ref(),
                data_type,
                &compute::CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )?)
        }
        DataType::LargeUtf8 | DataType::Utf8View => {
            let decoded = build_scalar_array_ref(values, &DataType::Utf8)?;
            Ok(compute::cast_with_options(
                decoded.as_ref(),
                data_type,
                &compute::CastOptions {
                    safe: false,
                    ..Default::default()
                },
            )?)
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Int64(val) => builder.append_value(*val),
                    ScalarValue::Null => builder.append_null(),
                    other => {
                        return Err(QueryError::Type(format!(
                            "aggregate value {other:?} does not match {data_type:?}"
                        )))
                    }
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
                    other => {
                        return Err(QueryError::Type(format!(
                            "aggregate value {other:?} does not match {data_type:?}"
                        )))
                    }
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
                    other => {
                        return Err(QueryError::Type(format!(
                            "aggregate value {other:?} does not match {data_type:?}"
                        )))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let vals: Vec<Option<i32>> = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Date32(val) => Ok(Some(*val)),
                    ScalarValue::Null => Ok(None),
                    other => Err(QueryError::Type(format!(
                        "aggregate value {other:?} does not match {data_type:?}"
                    ))),
                })
                .collect::<Result<_>>()?;
            Ok(Arc::new(Date32Array::from(vals)))
        }
        DataType::Decimal128(p, s) => build_decimal_output(
            values.iter().map(|v| Ok((**v).clone())),
            values.len(),
            *p,
            *s,
            &process_memory_pool(),
        ),
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Boolean(val) => builder.append_value(*val),
                    ScalarValue::Null => builder.append_null(),
                    other => {
                        return Err(QueryError::Type(format!(
                            "aggregate value {other:?} does not match {data_type:?}"
                        )))
                    }
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::with_capacity(values.len());
            for v in values {
                match v {
                    ScalarValue::Int32(val) => builder.append_value(*val),
                    ScalarValue::Int64(val) => {
                        builder.append_value(i32::try_from(*val).map_err(|_| {
                            QueryError::Execution("aggregate Int32 overflow".into())
                        })?)
                    }
                    ScalarValue::Null => builder.append_null(),
                    other => {
                        return Err(QueryError::Type(format!(
                            "aggregate value {other:?} does not match {data_type:?}"
                        )))
                    }
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
    let plan_schema = crate::planner::PlanSchema::from_qualified_arrow(input_schema.as_ref());
    let input_types: Vec<DataType> = agg_input_exprs
        .iter()
        .map(|e| e.data_type(&plan_schema).unwrap_or(DataType::Float64))
        .collect();

    let num_threads =
        crate::execution::topology::workers_for(source.total_work(), rayon::current_num_threads());

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
        final_state.merge(&state)?;
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

    #[test]
    fn morsel_group_normalization_preserves_only_supported_dictionary_encoding() {
        let dictionary: DictionaryArray<Int32Type> =
            [Some("a"), None, Some("b")].into_iter().collect();
        let source: ArrayRef = StdArc::new(dictionary);
        let group = normalize_morsel_group_array(source.clone()).unwrap();
        assert!(
            StdArc::ptr_eq(&source, &group),
            "group normalization must retain backing buffers"
        );
        assert!(matches!(
            TypedArrayAccessor::from_array(&group),
            TypedArrayAccessor::DictString(_)
        ));
        assert_eq!(
            normalize_aggregate_array(source).unwrap().data_type(),
            &DataType::Utf8,
            "shared aggregate/hash normalization must continue to decode"
        );
        let other: DictionaryArray<arrow::datatypes::Int8Type> =
            [Some("a"), None].into_iter().collect();
        let normalized = normalize_morsel_group_array(StdArc::new(other)).unwrap();
        assert_eq!(normalized.data_type(), &DataType::Utf8);
        assert!(normalized.is_null(1));
    }

    #[test]
    fn all_null_group_is_present_even_when_aggregates_saw_no_values() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", DataType::Float64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(Int64Array::from(vec![None])),
                StdArc::new(Float64Array::from(vec![None])),
            ],
        )
        .unwrap();
        for (functions, inputs, types) in [
            (
                vec![AggregateFunction::Sum],
                vec![Expr::column("v")],
                vec![DataType::Float64],
            ),
            (
                vec![AggregateFunction::Count],
                vec![Expr::column("v")],
                vec![DataType::Float64],
            ),
            (vec![], vec![], vec![]),
        ] {
            let mut state = AggregationState::new(functions.clone(), types.clone());
            state
                .process_batch(&batch, &[Expr::column("k")], &inputs)
                .unwrap();
            assert_eq!(state.group_count(), 1);
            let mut fields = vec![Field::new("k", DataType::Int64, true)];
            if let Some(function) = functions.first() {
                fields.push(Field::new(
                    "a",
                    if *function == AggregateFunction::Count {
                        DataType::Int64
                    } else {
                        DataType::Float64
                    },
                    true,
                ));
            }
            let output_schema = StdArc::new(Schema::new(fields));
            let output = state.build_output(&output_schema).unwrap();
            assert_eq!(output.num_rows(), 1);
            assert!(output.column(0).is_null(0));
            if functions.first() == Some(&AggregateFunction::Sum) {
                assert!(output.column(1).is_null(0));
            }
            if functions.first() == Some(&AggregateFunction::Count) {
                assert_eq!(
                    output
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(0),
                    0
                );
            }
            let mut merged = AggregationState::new(functions, types);
            merged.merge(&state).unwrap();
            assert_eq!(merged.build_output(&output_schema).unwrap().num_rows(), 1);
            state.drain_perfect_to_hashmap();
            state.overflowed = true;
            assert_eq!(state.build_output(&output_schema).unwrap().num_rows(), 1);
        }
    }

    #[test]
    fn all_null_group_survives_ingest_and_merge_stride_changes() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                StdArc::new(Int64Array::from(vec![None, None, Some(2)])),
                StdArc::new(Int64Array::from(vec![None, Some(1), Some(2)])),
                StdArc::new(Int64Array::from(vec![7, 3, 4])),
            ],
        )
        .unwrap();
        for merge in [false, true] {
            let mut state =
                AggregationState::new(vec![AggregateFunction::Sum], vec![DataType::Int64]);
            for row in 0..3 {
                let mut partial =
                    AggregationState::new(vec![AggregateFunction::Sum], vec![DataType::Int64]);
                let target = if merge { &mut partial } else { &mut state };
                target
                    .process_batch(
                        &batch.slice(row, 1),
                        &[Expr::column("a"), Expr::column("b")],
                        &[Expr::column("v")],
                    )
                    .unwrap();
                if merge {
                    state.merge(&partial).unwrap();
                }
            }
            let output = state.build_output(&schema).unwrap();
            assert_eq!(output.num_rows(), 3);
            let null_row = (0..output.num_rows())
                .find(|&row| output.column(0).is_null(row) && output.column(1).is_null(row))
                .unwrap();
            assert_eq!(
                output
                    .column(2)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(null_row),
                7
            );
        }
    }

    #[test]
    fn dictionary_value_nulls_group_with_key_nulls_and_are_not_counted() {
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            arrow::array::Int32Array::from(vec![0, 1, 2, 0]),
            StdArc::new(StringArray::from(vec![None, Some(""), Some("a")])),
        )
        .unwrap();
        let array: ArrayRef = StdArc::new(dictionary);
        let accessor = TypedArrayAccessor::from_array(&array);
        assert_eq!(accessor.extract_scalar(0), ScalarValue::Null);
        assert_eq!(accessor.raw_key(0), u64::MAX);
        let mut state = AggregationState::new(
            vec![AggregateFunction::Count],
            vec![array.data_type().clone()],
        );
        let batch = RecordBatch::try_new(
            StdArc::new(Schema::new(vec![Field::new(
                "k",
                array.data_type().clone(),
                true,
            )])),
            vec![array],
        )
        .unwrap();
        // No NULL dictionary keys: exercises combined-dictionary fast path.
        state
            .process_batch(&batch, &[Expr::column("k")], &[Expr::column("k")])
            .unwrap();
        let null_keys: DictionaryArray<Int32Type> = [None::<&str>].into_iter().collect();
        let batch2 = RecordBatch::try_new(batch.schema(), vec![StdArc::new(null_keys)]).unwrap();
        state
            .process_batch(&batch2, &[Expr::column("k")], &[Expr::column("k")])
            .unwrap();
        let schema = StdArc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, true),
            Field::new("n", DataType::Int64, false),
        ]));
        let output = state.build_output(&schema).unwrap();
        assert_eq!(output.num_rows(), 3);
        for row in 0..3 {
            let count = output
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row);
            assert_eq!(count, if output.column(0).is_null(row) { 0 } else { 1 });
        }
    }

    #[test]
    fn dictionary_combined_keys_with_extreme_integers_fall_back_exactly() {
        let dictionary: DictionaryArray<Int32Type> = [Some("a"), Some("a")].into_iter().collect();
        let schema = StdArc::new(Schema::new(vec![
            Field::new("d", dictionary.data_type().clone(), false),
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(dictionary),
                StdArc::new(Int64Array::from(vec![i64::MIN, i64::MAX])),
                StdArc::new(Int64Array::from(vec![3, 7])),
            ],
        )
        .unwrap();
        let mut state = AggregationState::new(vec![AggregateFunction::Sum], vec![DataType::Int64]);
        state
            .process_batch(
                &batch,
                &[Expr::column("d"), Expr::column("k")],
                &[Expr::column("v")],
            )
            .unwrap();
        let output_schema = StdArc::new(Schema::new(vec![
            Field::new("d", DataType::Utf8, false),
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let output = state.build_output(&output_schema).unwrap();
        let mut rows = (0..output.num_rows())
            .map(|row| {
                (
                    output
                        .column(1)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row),
                    output
                        .column(2)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .value(row),
                )
            })
            .collect::<Vec<_>>();
        rows.sort();
        assert_eq!(rows, vec![(i64::MIN, 3), (i64::MAX, 7)]);
    }

    #[test]
    fn dictionary_combined_logical_null_tuples_match_generic_groups() {
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            arrow::array::Int32Array::from(vec![0, 1, 2, 0, 1]),
            StdArc::new(StringArray::from(vec![None, Some("a"), None])),
        )
        .unwrap();
        // Physical keys are all valid, but two dictionary entries mean SQL NULL.
        assert_eq!(dictionary.null_count(), 0);
        let schema = StdArc::new(Schema::new(vec![
            Field::new("d", dictionary.data_type().clone(), true),
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                StdArc::new(dictionary),
                StdArc::new(Int64Array::from(vec![1, 2, 1, 3, 3])),
                StdArc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap();
        let output_schema = StdArc::new(Schema::new(vec![
            Field::new("d", DataType::Utf8, true),
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let expected = vec![
            (None, 1, 4),
            (None, 3, 4),
            (Some("a".to_string()), 2, 2),
            (Some("a".to_string()), 3, 5),
        ];
        for generic in [false, true] {
            let mut state =
                AggregationState::new(vec![AggregateFunction::Sum], vec![DataType::Int64]);
            state.overflowed = generic;
            state
                .process_batch(
                    &batch,
                    &[Expr::column("d"), Expr::column("k")],
                    &[Expr::column("v")],
                )
                .unwrap();
            let output = state.build_output(&output_schema).unwrap();
            let strings = output
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let keys = output
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let sums = output
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let mut rows = (0..output.num_rows())
                .map(|row| {
                    (
                        (!strings.is_null(row)).then(|| strings.value(row).to_string()),
                        keys.value(row),
                        sums.value(row),
                    )
                })
                .collect::<Vec<_>>();
            rows.sort();
            assert_eq!(rows, expected, "generic={generic}");
        }
    }

    #[test]
    fn raw_sum_nullable_batches_keep_unseen_groups_boxed() {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, false),
            Field::new("v", DataType::Float64, true),
        ]));
        let mut state =
            AggregationState::new(vec![AggregateFunction::Sum], vec![DataType::Float64]);
        state.overflowed = true; // isolate the representation entered after perfect overflow
        for value in [None, Some(5.0), None] {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    StdArc::new(Int64Array::from(vec![7])),
                    StdArc::new(Float64Array::from(vec![value])),
                ],
            )
            .unwrap();
            state
                .process_batch(&batch, &[Expr::column("k")], &[Expr::column("v")])
                .unwrap();
            if value.is_none() && state.raw_sums.is_empty() {
                assert!(!state.absorb_raw_groups_into_sums());
                assert!(state.build_output(&schema).unwrap().column(1).is_null(0));
            }
        }
        state.demote_raw_sums().unwrap();
        let output = state.build_output(&schema).unwrap();
        assert_eq!(output.num_rows(), 1);
        assert_eq!(
            output
                .column(1)
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(0),
            5.0
        );
    }

    #[test]
    fn decimal_accessor_keeps_exact_sum_and_null_state() {
        for scale in [-4, 2, 38] {
            let coefficient = 10_i128.pow(36) + 17;
            let array: ArrayRef = StdArc::new(
                arrow::array::Decimal128Array::from(vec![
                    None,
                    Some(coefficient),
                    Some(-coefficient + 23),
                    None,
                ])
                .with_precision_and_scale(38, scale)
                .unwrap(),
            );
            let accessor = TypedArrayAccessor::from_array(&array);
            let mut sum = AccumulatorState::new(&AggregateFunction::Sum, array.data_type());
            accessor.update_accumulator(0, &mut sum);
            assert!(matches!(
                sum,
                AccumulatorState::SumDecimal { seen: false, .. }
            ));
            for row in 1..4 {
                accessor.update_accumulator(row, &mut sum);
            }
            assert!(matches!(
                sum,
                AccumulatorState::SumDecimal {
                    coefficient: Some(23),
                    seen: true,
                    ..
                }
            ));
            let mut count = AccumulatorState::Count(0);
            for row in 0..4 {
                accessor.update_accumulator(row, &mut count);
            }
            assert!(matches!(count, AccumulatorState::Count(2)));
            let mut overflow = AccumulatorState::SumDecimal {
                coefficient: Some(i128::MAX),
                scale,
                seen: true,
            };
            accessor.update_accumulator(1, &mut overflow);
            accessor.update_accumulator(2, &mut overflow);
            assert!(matches!(
                overflow,
                AccumulatorState::SumDecimal {
                    coefficient: None,
                    seen: true,
                    ..
                }
            ));
            assert_eq!(
                accessor.extract_scalar(1),
                ScalarValue::Decimal128(DecimalValue::new(coefficient, scale))
            );
        }
    }

    #[test]
    fn null_sentinel_does_not_merge_negative_integer_groups() {
        for data_type in [DataType::Int64, DataType::Int32, DataType::Date32] {
            for null_first in [false, true] {
                let values = if null_first {
                    vec![None, Some(-1)]
                } else {
                    vec![Some(-1), None]
                };
                let keys: ArrayRef = match data_type {
                    DataType::Int64 => StdArc::new(Int64Array::from(values.clone())),
                    DataType::Int32 => StdArc::new(arrow::array::Int32Array::from(
                        values
                            .iter()
                            .map(|v| v.map(|x| x as i32))
                            .collect::<Vec<_>>(),
                    )),
                    _ => StdArc::new(Date32Array::from(
                        values
                            .iter()
                            .map(|v| v.map(|x| x as i32))
                            .collect::<Vec<_>>(),
                    )),
                };
                let schema = StdArc::new(Schema::new(vec![
                    Field::new("k", data_type.clone(), true),
                    Field::new("v", DataType::Int64, false),
                ]));
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![keys, StdArc::new(Int64Array::from(vec![3, 7]))],
                )
                .unwrap();
                let mut state =
                    AggregationState::new(vec![AggregateFunction::Sum], vec![DataType::Int64]);
                // Separate batches exercise an existing raw-key hit, not just insertion.
                for row in 0..2 {
                    state
                        .process_batch(
                            &batch.slice(row, 1),
                            &[Expr::column("k")],
                            &[Expr::column("v")],
                        )
                        .unwrap();
                }
                let output = state.build_output(&schema).unwrap();
                assert_eq!(
                    output.num_rows(),
                    2,
                    "{data_type:?}, null_first={null_first}"
                );
                let sums = output
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..2 {
                    let expected = if output.column(0).is_null(row) == null_first {
                        3
                    } else {
                        7
                    };
                    assert_eq!(sums.value(row), expected);
                }
            }
        }
    }

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

#[cfg(test)]
mod decimal_output_tests;

#[cfg(test)]
mod domain_tests;
