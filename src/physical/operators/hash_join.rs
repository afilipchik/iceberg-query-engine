//! Hash join operator

use crate::error::Result;
use crate::physical::operators::filter::evaluate_expr;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::{Expr, JoinType};
use arrow::array::{ArrayRef, Int64Array, UInt32Array, UInt64Array};
use arrow::compute;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::{self, TryStreamExt};
use hashbrown::HashMap;
use rayon::prelude::*;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Hash join execution operator
#[derive(Debug)]
pub struct HashJoinExec {
    left: Arc<dyn PhysicalOperator>,
    right: Arc<dyn PhysicalOperator>,
    on: Vec<(Expr, Expr)>,
    join_type: JoinType,
    schema: SchemaRef,
}

impl HashJoinExec {
    pub fn new(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        on: Vec<(Expr, Expr)>,
        join_type: JoinType,
    ) -> Self {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let schema = match join_type {
            JoinType::Semi | JoinType::Anti => left_schema,
            _ => {
                // For outer joins, columns from the "outer" side can be null
                let left_nullable = matches!(join_type, JoinType::Right | JoinType::Full);
                let right_nullable = matches!(join_type, JoinType::Left | JoinType::Full);

                let left_fields = left_schema.fields().iter().map(|f| {
                    if left_nullable && !f.is_nullable() {
                        Arc::new(f.as_ref().clone().with_nullable(true))
                    } else {
                        f.clone()
                    }
                });

                let right_fields = right_schema.fields().iter().map(|f| {
                    if right_nullable && !f.is_nullable() {
                        Arc::new(f.as_ref().clone().with_nullable(true))
                    } else {
                        f.clone()
                    }
                });

                let fields: Vec<_> = left_fields.chain(right_fields).collect();
                Arc::new(Schema::new(fields))
            }
        };

        Self {
            left,
            right,
            on,
            join_type,
            schema,
        }
    }
}

#[async_trait]
impl PhysicalOperator for HashJoinExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.left.clone(), self.right.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        // Collect build side (left for inner/left join, right for right join)
        let (build_side, probe_side, swapped) = match self.join_type {
            JoinType::Right => (&self.right, &self.left, true),
            _ => (&self.left, &self.right, false),
        };

        let build_stream = build_side.execute(partition).await?;
        let build_batches: Vec<RecordBatch> = build_stream.try_collect().await?;

        // Build hash table
        let (on_left, on_right): (Vec<_>, Vec<_>) = self.on.iter().cloned().unzip();
        let build_keys = if swapped { &on_right } else { &on_left };
        let probe_keys = if swapped { &on_left } else { &on_right };

        let hash_table = build_hash_table(&build_batches, build_keys)?;

        // Probe
        let probe_stream = probe_side.execute(partition).await?;
        let probe_batches: Vec<RecordBatch> = probe_stream.try_collect().await?;

        let result = probe_hash_table(
            &build_batches,
            &probe_batches,
            &hash_table,
            probe_keys,
            self.join_type,
            swapped,
            &self.schema,
        )?;

        Ok(Box::pin(stream::iter(result.into_iter().map(Ok))))
    }

    fn name(&self) -> &str {
        "HashJoin"
    }
}

impl fmt::Display for HashJoinExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let on_str: Vec<String> = self
            .on
            .iter()
            .map(|(l, r)| format!("{} = {}", l, r))
            .collect();
        write!(f, "{} Join on [{}]", self.join_type, on_str.join(", "))
    }
}

/// Join key for hash table
#[derive(Clone)]
struct JoinKey {
    values: Vec<JoinValue>,
}

#[derive(Clone)]
enum JoinValue {
    Null,
    Int64(i64),
    Float64(ordered_float::OrderedFloat<f64>),
    String(String),
}

impl PartialEq for JoinKey {
    fn eq(&self, other: &Self) -> bool {
        if self.values.len() != other.values.len() {
            return false;
        }
        self.values
            .iter()
            .zip(other.values.iter())
            .all(|(a, b)| match (a, b) {
                (JoinValue::Null, JoinValue::Null) => true,
                (JoinValue::Int64(a), JoinValue::Int64(b)) => a == b,
                (JoinValue::Float64(a), JoinValue::Float64(b)) => a == b,
                (JoinValue::String(a), JoinValue::String(b)) => a == b,
                _ => false,
            })
    }
}

impl Eq for JoinKey {}

impl Hash for JoinKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for v in &self.values {
            match v {
                JoinValue::Null => 0u8.hash(state),
                JoinValue::Int64(i) => {
                    1u8.hash(state);
                    i.hash(state);
                }
                JoinValue::Float64(f) => {
                    2u8.hash(state);
                    f.hash(state);
                }
                JoinValue::String(s) => {
                    3u8.hash(state);
                    s.hash(state);
                }
            }
        }
    }
}

/// Hash table entry pointing to batch and row indices
#[derive(Clone)]
struct HashEntry {
    batch_idx: usize,
    row_idx: usize,
}

/// Threshold for parallel build (use parallel for larger datasets)
const PARALLEL_BUILD_THRESHOLD: usize = 10_000;

fn build_hash_table(
    batches: &[RecordBatch],
    key_exprs: &[Expr],
) -> Result<HashMap<JoinKey, Vec<HashEntry>>> {
    // Count total rows to decide if parallel build is worth it
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    if total_rows < PARALLEL_BUILD_THRESHOLD || batches.len() < 2 {
        // Use sequential build for small datasets
        build_hash_table_sequential(batches, key_exprs)
    } else {
        // Use parallel build for large datasets
        build_hash_table_parallel(batches, key_exprs)
    }
}

/// Sequential hash table build (for small datasets)
fn build_hash_table_sequential(
    batches: &[RecordBatch],
    key_exprs: &[Expr],
) -> Result<HashMap<JoinKey, Vec<HashEntry>>> {
    let mut table: HashMap<JoinKey, Vec<HashEntry>> = HashMap::new();

    for (batch_idx, batch) in batches.iter().enumerate() {
        let key_arrays: Result<Vec<ArrayRef>> =
            key_exprs.iter().map(|e| evaluate_expr(batch, e)).collect();
        let key_arrays = key_arrays?;

        for row_idx in 0..batch.num_rows() {
            let key = extract_join_key(&key_arrays, row_idx);

            // Skip null keys (null != null in SQL)
            if key.values.iter().any(|v| matches!(v, JoinValue::Null)) {
                continue;
            }

            table
                .entry(key)
                .or_default()
                .push(HashEntry { batch_idx, row_idx });
        }
    }

    Ok(table)
}

/// Parallel hash table build using rayon
fn build_hash_table_parallel(
    batches: &[RecordBatch],
    key_exprs: &[Expr],
) -> Result<HashMap<JoinKey, Vec<HashEntry>>> {
    // Build partial hash tables in parallel, one per batch
    let partial_tables: Vec<Result<HashMap<JoinKey, Vec<HashEntry>>>> = batches
        .par_iter()
        .enumerate()
        .map(|(batch_idx, batch)| {
            let mut partial: HashMap<JoinKey, Vec<HashEntry>> = HashMap::new();

            let key_arrays: Result<Vec<ArrayRef>> =
                key_exprs.iter().map(|e| evaluate_expr(batch, e)).collect();
            let key_arrays = key_arrays?;

            for row_idx in 0..batch.num_rows() {
                let key = extract_join_key(&key_arrays, row_idx);

                // Skip null keys (null != null in SQL)
                if key.values.iter().any(|v| matches!(v, JoinValue::Null)) {
                    continue;
                }

                partial
                    .entry(key)
                    .or_default()
                    .push(HashEntry { batch_idx, row_idx });
            }

            Ok(partial)
        })
        .collect();

    // Merge partial tables into final table
    let mut final_table: HashMap<JoinKey, Vec<HashEntry>> = HashMap::new();

    for partial_result in partial_tables {
        let partial = partial_result?;
        for (key, entries) in partial {
            final_table.entry(key).or_default().extend(entries);
        }
    }

    Ok(final_table)
}

fn extract_join_key(arrays: &[ArrayRef], row: usize) -> JoinKey {
    let values: Vec<JoinValue> = arrays
        .iter()
        .map(|arr| {
            if arr.is_null(row) {
                return JoinValue::Null;
            }

            if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
                return JoinValue::Int64(a.value(row));
            }
            if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int32Array>() {
                return JoinValue::Int64(a.value(row) as i64);
            }
            if let Some(a) = arr.as_any().downcast_ref::<UInt64Array>() {
                return JoinValue::Int64(a.value(row) as i64);
            }
            if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
                return JoinValue::Float64(ordered_float::OrderedFloat(a.value(row)));
            }
            if let Some(a) = arr.as_any().downcast_ref::<arrow::array::StringArray>() {
                return JoinValue::String(a.value(row).to_string());
            }
            if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Date32Array>() {
                return JoinValue::Int64(a.value(row) as i64);
            }

            JoinValue::Null
        })
        .collect();

    JoinKey { values }
}

#[allow(clippy::needless_range_loop)] // Index needed for parallel array access
fn probe_hash_table(
    build_batches: &[RecordBatch],
    probe_batches: &[RecordBatch],
    hash_table: &HashMap<JoinKey, Vec<HashEntry>>,
    probe_key_exprs: &[Expr],
    join_type: JoinType,
    swapped: bool,
    output_schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let mut results = Vec::new();

    // Track which build rows have been matched (for outer joins)
    let mut build_matched: Vec<Vec<bool>> = build_batches
        .iter()
        .map(|b| vec![false; b.num_rows()])
        .collect();

    for probe_batch in probe_batches {
        let probe_key_arrays: Result<Vec<ArrayRef>> = probe_key_exprs
            .iter()
            .map(|e| evaluate_expr(probe_batch, e))
            .collect();
        let probe_key_arrays = probe_key_arrays?;

        let mut build_indices: Vec<(usize, usize)> = Vec::new(); // (batch_idx, row_idx)
        let mut probe_indices: Vec<usize> = Vec::new();
        let mut probe_matched = vec![false; probe_batch.num_rows()];

        for probe_row in 0..probe_batch.num_rows() {
            let key = extract_join_key(&probe_key_arrays, probe_row);

            // Skip null keys
            if key.values.iter().any(|v| matches!(v, JoinValue::Null)) {
                continue;
            }

            if let Some(entries) = hash_table.get(&key) {
                probe_matched[probe_row] = true;
                for entry in entries {
                    build_matched[entry.batch_idx][entry.row_idx] = true;
                    build_indices.push((entry.batch_idx, entry.row_idx));
                    probe_indices.push(probe_row);
                }
            }
        }

        match join_type {
            JoinType::Inner | JoinType::Cross => {
                if !build_indices.is_empty() {
                    let batch = create_joined_batch(
                        build_batches,
                        probe_batch,
                        &build_indices,
                        &probe_indices,
                        swapped,
                        output_schema,
                    )?;
                    results.push(batch);
                }
            }
            JoinType::Left => {
                // Include all probe rows, with nulls for non-matches
                let (bi, pi) = if swapped {
                    // Build side is right, probe side is left
                    add_unmatched_probe(
                        &build_indices,
                        &probe_indices,
                        &probe_matched,
                        probe_batch.num_rows(),
                    )
                } else {
                    (build_indices.clone(), probe_indices.clone())
                };

                if !bi.is_empty() {
                    let batch = create_joined_batch_with_nulls(
                        build_batches,
                        probe_batch,
                        &bi,
                        &pi,
                        &probe_matched,
                        swapped,
                        output_schema,
                        true, // null for build side
                    )?;
                    results.push(batch);
                }
            }
            JoinType::Right => {
                // Similar to left but for build side
                if !build_indices.is_empty() {
                    let batch = create_joined_batch(
                        build_batches,
                        probe_batch,
                        &build_indices,
                        &probe_indices,
                        swapped,
                        output_schema,
                    )?;
                    results.push(batch);
                }
            }
            JoinType::Semi | JoinType::Anti => {
                // Semi and Anti joins are handled after processing all probe batches
                // since we need to know which build rows matched across all probes
            }
            JoinType::Full => {
                // Implemented below after processing all probe batches
                if !build_indices.is_empty() {
                    let batch = create_joined_batch(
                        build_batches,
                        probe_batch,
                        &build_indices,
                        &probe_indices,
                        swapped,
                        output_schema,
                    )?;
                    results.push(batch);
                }
            }
        }
    }

    // For outer joins, add unmatched build rows
    if matches!(join_type, JoinType::Right | JoinType::Full) && !swapped {
        let unmatched_build = collect_unmatched_build(&build_matched);
        if !unmatched_build.is_empty() {
            let batch =
                create_build_only_batch(build_batches, &unmatched_build, output_schema, swapped)?;
            results.push(batch);
        }
    }

    if matches!(join_type, JoinType::Left | JoinType::Full) && swapped {
        let unmatched_build = collect_unmatched_build(&build_matched);
        if !unmatched_build.is_empty() {
            let batch =
                create_build_only_batch(build_batches, &unmatched_build, output_schema, swapped)?;
            results.push(batch);
        }
    }

    // Handle Semi and Anti joins - return build (left) rows based on match status
    if matches!(join_type, JoinType::Semi) {
        // Semi join: return build rows that have at least one match
        let matched_build = collect_matched_build(&build_matched);
        if !matched_build.is_empty() {
            let batch = create_semi_anti_batch(build_batches, &matched_build, output_schema)?;
            results.push(batch);
        }
    }

    if matches!(join_type, JoinType::Anti) {
        // Anti join: return build rows that have no matches
        let unmatched_build = collect_unmatched_build(&build_matched);
        if !unmatched_build.is_empty() {
            let batch = create_semi_anti_batch(build_batches, &unmatched_build, output_schema)?;
            results.push(batch);
        }
    }

    Ok(results)
}

fn add_unmatched_probe(
    build_indices: &[(usize, usize)],
    probe_indices: &[usize],
    probe_matched: &[bool],
    probe_rows: usize,
) -> (Vec<(usize, usize)>, Vec<usize>) {
    let mut bi = build_indices.to_vec();
    let mut pi = probe_indices.to_vec();

    for (row, &matched) in probe_matched.iter().enumerate().take(probe_rows) {
        if !matched {
            bi.push((usize::MAX, 0)); // Sentinel for null
            pi.push(row);
        }
    }

    (bi, pi)
}

fn collect_unmatched_build(build_matched: &[Vec<bool>]) -> Vec<(usize, usize)> {
    let mut unmatched = Vec::new();
    for (batch_idx, matched) in build_matched.iter().enumerate() {
        for (row_idx, &m) in matched.iter().enumerate() {
            if !m {
                unmatched.push((batch_idx, row_idx));
            }
        }
    }
    unmatched
}

fn collect_matched_build(build_matched: &[Vec<bool>]) -> Vec<(usize, usize)> {
    let mut matched = Vec::new();
    for (batch_idx, match_vec) in build_matched.iter().enumerate() {
        for (row_idx, &m) in match_vec.iter().enumerate() {
            if m {
                matched.push((batch_idx, row_idx));
            }
        }
    }
    matched
}

fn create_semi_anti_batch(
    build_batches: &[RecordBatch],
    indices: &[(usize, usize)],
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    if build_batches.is_empty() || indices.is_empty() {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let columns: Result<Vec<ArrayRef>> = (0..build_batches[0].num_columns())
        .map(|col_idx| gather_column(build_batches, col_idx, indices))
        .collect();

    RecordBatch::try_new(output_schema.clone(), columns?).map_err(Into::into)
}

fn create_joined_batch(
    build_batches: &[RecordBatch],
    probe_batch: &RecordBatch,
    build_indices: &[(usize, usize)],
    probe_indices: &[usize],
    swapped: bool,
    output_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let _num_rows = build_indices.len();

    // Gather build columns
    let build_columns: Result<Vec<ArrayRef>> = if build_batches.is_empty() {
        Ok(vec![])
    } else {
        (0..build_batches[0].num_columns())
            .map(|col_idx| gather_column(build_batches, col_idx, build_indices))
            .collect()
    };
    let build_columns = build_columns?;

    // Gather probe columns
    let probe_indices_arr: Vec<u32> = probe_indices.iter().map(|&i| i as u32).collect();
    let probe_index_arr = UInt32Array::from(probe_indices_arr);

    let probe_columns: Result<Vec<ArrayRef>> = probe_batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &probe_index_arr, None).map_err(Into::into))
        .collect();
    let probe_columns = probe_columns?;

    // Combine in correct order
    let columns: Vec<ArrayRef> = if swapped {
        probe_columns.into_iter().chain(build_columns).collect()
    } else {
        build_columns.into_iter().chain(probe_columns).collect()
    };

    RecordBatch::try_new(output_schema.clone(), columns).map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
fn create_joined_batch_with_nulls(
    build_batches: &[RecordBatch],
    probe_batch: &RecordBatch,
    build_indices: &[(usize, usize)],
    probe_indices: &[usize],
    _probe_matched: &[bool],
    swapped: bool,
    output_schema: &SchemaRef,
    _null_build: bool,
) -> Result<RecordBatch> {
    // For now, just use the regular join
    // A proper implementation would handle nulls for unmatched rows
    create_joined_batch(
        build_batches,
        probe_batch,
        build_indices,
        probe_indices,
        swapped,
        output_schema,
    )
}

fn create_build_only_batch(
    build_batches: &[RecordBatch],
    indices: &[(usize, usize)],
    output_schema: &SchemaRef,
    swapped: bool,
) -> Result<RecordBatch> {
    if build_batches.is_empty() {
        return Ok(RecordBatch::new_empty(output_schema.clone()));
    }

    let build_columns: Result<Vec<ArrayRef>> = (0..build_batches[0].num_columns())
        .map(|col_idx| gather_column(build_batches, col_idx, indices))
        .collect();
    let build_columns = build_columns?;

    // Create null arrays for probe side
    let num_rows = indices.len();
    let probe_num_cols = output_schema.fields().len() - build_batches[0].num_columns();

    let null_columns: Vec<ArrayRef> = (0..probe_num_cols)
        .map(|i| {
            let field_idx = if swapped {
                i
            } else {
                build_batches[0].num_columns() + i
            };
            let dt = output_schema.field(field_idx).data_type();
            arrow::array::new_null_array(dt, num_rows)
        })
        .collect();

    let columns: Vec<ArrayRef> = if swapped {
        null_columns.into_iter().chain(build_columns).collect()
    } else {
        build_columns.into_iter().chain(null_columns).collect()
    };

    RecordBatch::try_new(output_schema.clone(), columns).map_err(Into::into)
}

fn gather_column(
    batches: &[RecordBatch],
    col_idx: usize,
    indices: &[(usize, usize)],
) -> Result<ArrayRef> {
    // Build indices for each batch
    let mut batch_indices: HashMap<usize, Vec<(usize, u32)>> = HashMap::new();
    for (out_idx, &(batch_idx, row_idx)) in indices.iter().enumerate() {
        if batch_idx != usize::MAX {
            batch_indices
                .entry(batch_idx)
                .or_default()
                .push((out_idx, row_idx as u32));
        }
    }

    // Create output array
    let total_len = indices.len();
    let dt = batches[0].column(col_idx).data_type();

    // Gather from each batch
    let mut builders_data: Vec<(usize, ArrayRef)> = Vec::new();

    for (batch_idx, idx_list) in batch_indices {
        let batch = &batches[batch_idx];
        let col = batch.column(col_idx);

        let take_indices: Vec<u32> = idx_list.iter().map(|(_, row)| *row).collect();
        let take_arr = UInt32Array::from(take_indices);

        let taken = compute::take(col.as_ref(), &take_arr, None)?;

        for (i, (out_idx, _)) in idx_list.iter().enumerate() {
            builders_data.push((
                *out_idx,
                arrow::compute::take(&taken, &UInt32Array::from(vec![i as u32]), None)?,
            ));
        }
    }

    // Sort by output index and concatenate
    builders_data.sort_by_key(|(idx, _)| *idx);

    if builders_data.is_empty() {
        return Ok(arrow::array::new_null_array(dt, total_len));
    }

    // Simple approach: gather one at a time
    let arrays: Vec<&dyn arrow::array::Array> =
        builders_data.iter().map(|(_, arr)| arr.as_ref()).collect();

    if arrays.is_empty() {
        Ok(arrow::array::new_null_array(dt, total_len))
    } else {
        compute::concat(&arrays).map_err(Into::into)
    }
}

#[allow(dead_code)] // Reserved for join filter optimization
fn filter_batch_by_indices(batch: &RecordBatch, indices: &[usize]) -> Result<RecordBatch> {
    let indices_arr = UInt32Array::from(indices.iter().map(|&i| i as u32).collect::<Vec<_>>());

    let columns: Result<Vec<ArrayRef>> = batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &indices_arr, None).map_err(Into::into))
        .collect();

    RecordBatch::try_new(batch.schema(), columns?).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::MemoryTableExec;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::TryStreamExt;

    fn create_left_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
            ],
        )
        .unwrap()
    }

    fn create_right_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 2, 5])),
                Arc::new(Int64Array::from(vec![10, 20, 21, 50])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_inner_join() {
        let left = create_left_batch();
        let right = create_right_batch();

        let left_scan = Arc::new(MemoryTableExec::new(
            "left",
            left.schema(),
            vec![left],
            None,
        ));
        let right_scan = Arc::new(MemoryTableExec::new(
            "right",
            right.schema(),
            vec![right],
            None,
        ));

        let join = HashJoinExec::new(
            left_scan,
            right_scan,
            vec![(Expr::column("id"), Expr::column("id"))],
            JoinType::Inner,
        );

        let stream = join.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // id=1 matches once, id=2 matches twice
    }

    #[tokio::test]
    async fn test_semi_join() {
        let left = create_left_batch();
        let right = create_right_batch();

        let left_scan = Arc::new(MemoryTableExec::new(
            "left",
            left.schema(),
            vec![left],
            None,
        ));
        let right_scan = Arc::new(MemoryTableExec::new(
            "right",
            right.schema(),
            vec![right],
            None,
        ));

        let join = HashJoinExec::new(
            left_scan,
            right_scan,
            vec![(Expr::column("id"), Expr::column("id"))],
            JoinType::Semi,
        );

        let stream = join.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // ids 1 and 2 exist in right
    }

    #[tokio::test]
    async fn test_anti_join() {
        let left = create_left_batch();
        let right = create_right_batch();

        let left_scan = Arc::new(MemoryTableExec::new(
            "left",
            left.schema(),
            vec![left],
            None,
        ));
        let right_scan = Arc::new(MemoryTableExec::new(
            "right",
            right.schema(),
            vec![right],
            None,
        ));

        let join = HashJoinExec::new(
            left_scan,
            right_scan,
            vec![(Expr::column("id"), Expr::column("id"))],
            JoinType::Anti,
        );

        let stream = join.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // ids 3 and 4 don't exist in right
    }
}
