//! DelimJoin and DelimGet physical operators for efficient correlated subquery execution
//!
//! DelimJoin is a specialized join that:
//! 1. Collects all rows from the outer (left) side
//! 2. Extracts DISTINCT values of the correlation columns
//! 3. Passes these distinct values to DelimGet nodes inside the inner (right) side
//! 4. Executes the inner side ONCE with all distinct values
//! 5. Builds a hash table from the results and joins with all outer rows
//!
//! This transforms O(n * m) correlated subquery execution into O(n + m).

use crate::error::{QueryError, Result};
use crate::physical::PhysicalOperator;
use crate::planner::{Expr, JoinType};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray,
    UInt64Builder,
};
use arrow::compute;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, BoxStream};
use futures::{StreamExt, TryStreamExt};
use hashbrown::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, RwLock};

/// Shared state between DelimJoin and its child DelimGet nodes
#[derive(Debug, Default)]
pub struct DelimState {
    /// Distinct correlation values extracted from the outer side
    /// Format: Vec of rows, each row is a Vec of column values as ScalarValue
    distinct_values: RwLock<Option<RecordBatch>>,
    /// Schema of the distinct values
    schema: RwLock<Option<SchemaRef>>,
}

impl DelimState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the distinct values (called by DelimJoinExec after collecting outer side)
    pub fn set_distinct_values(&self, batch: RecordBatch, schema: SchemaRef) {
        *self.distinct_values.write().unwrap() = Some(batch);
        *self.schema.write().unwrap() = Some(schema);
    }

    /// Get the distinct values (called by DelimGetExec during execution)
    pub fn get_distinct_values(&self) -> Option<RecordBatch> {
        self.distinct_values.read().unwrap().clone()
    }

    /// Get the schema
    pub fn get_schema(&self) -> Option<SchemaRef> {
        self.schema.read().unwrap().clone()
    }
}

/// DelimJoinExec - Physical operator for DelimJoin
///
/// Executes correlated subqueries efficiently by:
/// 1. Collecting all outer rows and extracting distinct correlation values
/// 2. Executing the inner side once with all distinct values via DelimGet
/// 3. Building a hash table and probing for matches
#[derive(Debug)]
pub struct DelimJoinExec {
    /// Left input (outer query)
    left: Arc<dyn PhysicalOperator>,
    /// Right input (inner subquery with DelimGet)
    right: Arc<dyn PhysicalOperator>,
    /// Join type (Semi, Anti, Single, Mark)
    join_type: JoinType,
    /// Expressions for correlation columns (from outer)
    delim_columns: Vec<Expr>,
    /// Join conditions (outer_col, inner_col)
    on: Vec<(Expr, Expr)>,
    /// Output schema
    schema: SchemaRef,
    /// Shared state with DelimGet
    delim_state: Arc<DelimState>,
}

impl DelimJoinExec {
    pub fn new(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        join_type: JoinType,
        delim_columns: Vec<Expr>,
        on: Vec<(Expr, Expr)>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            delim_columns,
            on,
            schema,
            delim_state: Arc::new(DelimState::new()),
        }
    }

    /// Create with a shared DelimState (for connecting to child DelimGet nodes)
    pub fn with_delim_state(
        left: Arc<dyn PhysicalOperator>,
        right: Arc<dyn PhysicalOperator>,
        join_type: JoinType,
        delim_columns: Vec<Expr>,
        on: Vec<(Expr, Expr)>,
        schema: SchemaRef,
        delim_state: Arc<DelimState>,
    ) -> Self {
        Self {
            left,
            right,
            join_type,
            delim_columns,
            on,
            schema,
            delim_state,
        }
    }

    /// Get the shared delim state (for passing to DelimGet)
    pub fn delim_state(&self) -> Arc<DelimState> {
        Arc::clone(&self.delim_state)
    }
}

#[async_trait]
impl PhysicalOperator for DelimJoinExec {
    fn name(&self) -> &str {
        "DelimJoinExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![Arc::clone(&self.left), Arc::clone(&self.right)]
    }

    fn output_partitions(&self) -> usize {
        1 // DelimJoin materializes everything, outputs single partition
    }

    async fn execute(&self, _partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        // Step 1: Collect all rows from the outer side
        let mut outer_batches = Vec::new();
        let outer_stream = self.left.execute(0).await?;
        let collected: Vec<Result<RecordBatch>> = outer_stream.collect().await;
        for batch_result in collected {
            outer_batches.push(batch_result?);
        }

        if outer_batches.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        // Step 2: Extract distinct correlation values
        // Pass the `on` conditions so we can name the columns correctly for the inner side
        let distinct_batch =
            extract_distinct_values(&outer_batches, &self.delim_columns, &self.on)?;

        // Step 3: Store in shared state for DelimGet to use
        self.delim_state
            .set_distinct_values(distinct_batch.clone(), distinct_batch.schema());

        // Step 4: Execute the inner side (which will use DelimGet with our values)
        let mut inner_batches = Vec::new();
        let inner_stream = self.right.execute(0).await?;
        let collected: Vec<Result<RecordBatch>> = inner_stream.collect().await;
        for batch_result in collected {
            inner_batches.push(batch_result?);
        }

        // Step 5: Build hash table from inner results
        let inner_hash = build_hash_table(&inner_batches, &self.on)?;

        // Step 6: Probe and produce output based on join type
        let result_batches = match self.join_type {
            JoinType::Semi => {
                produce_semi_output(&outer_batches, &inner_hash, &self.on, &self.schema)?
            }
            JoinType::Anti => {
                produce_anti_output(&outer_batches, &inner_hash, &self.on, &self.schema)?
            }
            JoinType::Single => produce_single_output(
                &outer_batches,
                &inner_batches,
                &inner_hash,
                &self.on,
                &self.schema,
            )?,
            JoinType::Mark => {
                produce_mark_output(&outer_batches, &inner_hash, &self.on, &self.schema)?
            }
            _ => {
                return Err(QueryError::NotImplemented(format!(
                    "DelimJoin with {:?} not supported",
                    self.join_type
                )))
            }
        };

        Ok(Box::pin(stream::iter(result_batches.into_iter().map(Ok))))
    }
}

/// DelimGetExec - Physical operator that receives distinct values from parent DelimJoin
#[derive(Debug)]
pub struct DelimGetExec {
    /// Shared state with parent DelimJoin
    delim_state: Arc<DelimState>,
    /// Output schema
    schema: SchemaRef,
}

impl DelimGetExec {
    pub fn new(delim_state: Arc<DelimState>, schema: SchemaRef) -> Self {
        Self {
            delim_state,
            schema,
        }
    }
}

#[async_trait]
impl PhysicalOperator for DelimGetExec {
    fn name(&self) -> &str {
        "DelimGetExec"
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![] // Leaf node
    }

    fn output_partitions(&self) -> usize {
        1
    }

    async fn execute(&self, _partition: usize) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        // Get distinct values from parent DelimJoin's state
        let batch = self
            .delim_state
            .get_distinct_values()
            .ok_or_else(|| QueryError::Execution("DelimGet: no values from parent".into()))?;

        Ok(Box::pin(stream::once(async move { Ok(batch) })))
    }
}

// Helper functions

/// Extract distinct values from the outer side for the correlation columns
/// The `on` parameter contains (outer_expr, inner_expr) pairs - we use inner_expr names for the output schema
fn extract_distinct_values(
    batches: &[RecordBatch],
    delim_columns: &[Expr],
    on: &[(Expr, Expr)],
) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Err(QueryError::Execution("No batches to extract from".into()));
    }

    // Evaluate delim columns and collect all values
    let mut column_values: Vec<Vec<ArrayRef>> =
        (0..delim_columns.len()).map(|_| Vec::new()).collect();

    for batch in batches {
        for (i, expr) in delim_columns.iter().enumerate() {
            let array = evaluate_expr_to_array(expr, batch)?;
            column_values[i].push(array);
        }
    }

    // Concatenate arrays for each column
    let concat_columns: Vec<ArrayRef> = column_values
        .into_iter()
        .map(|arrays| {
            let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
            compute::concat(&refs).map_err(|e| QueryError::Execution(e.to_string()))
        })
        .collect::<Result<Vec<_>>>()?;

    if concat_columns.is_empty() {
        return Err(QueryError::Execution("No columns to deduplicate".into()));
    }

    // Build a schema for the delim columns using INNER column names from the `on` conditions
    // This is crucial: the inner side expects columns named by inner_expr, not outer_expr
    let delim_fields: Vec<arrow::datatypes::Field> = on
        .iter()
        .zip(concat_columns.iter())
        .enumerate()
        .map(|(i, ((_, inner_expr), array))| {
            let name = match inner_expr {
                Expr::Column(col) => col.name.clone(),
                _ => format!("__delim_col_{}", i),
            };
            arrow::datatypes::Field::new(name, array.data_type().clone(), true)
        })
        .collect();
    let delim_schema = Arc::new(arrow::datatypes::Schema::new(delim_fields));

    // Create batch with all values
    let all_batch = RecordBatch::try_new(delim_schema.clone(), concat_columns)?;

    // Remove duplicates using hash-based approach
    let distinct_batch = deduplicate_batch(&all_batch)?;

    Ok(distinct_batch)
}

use crate::planner::ScalarValue;

/// Evaluate an expression to an Arrow array
fn evaluate_expr_to_array(expr: &Expr, batch: &RecordBatch) -> Result<ArrayRef> {
    match expr {
        Expr::Column(col) => {
            // Find column by name
            for (i, field) in batch.schema().fields().iter().enumerate() {
                if field.name() == &col.name
                    || col
                        .relation
                        .as_ref()
                        .map(|r| format!("{}.{}", r, col.name) == *field.name())
                        .unwrap_or(false)
                {
                    return Ok(Arc::clone(batch.column(i)));
                }
            }
            // Try just by name without relation
            for (i, field) in batch.schema().fields().iter().enumerate() {
                if field.name().ends_with(&col.name) {
                    return Ok(Arc::clone(batch.column(i)));
                }
            }
            Err(QueryError::ColumnNotFound(col.name.clone()))
        }
        Expr::Literal(scalar) => {
            // Create constant array
            let array = scalar_to_array(scalar, batch.num_rows())?;
            Ok(array)
        }
        _ => Err(QueryError::NotImplemented(format!(
            "Complex expression in delim column: {:?}",
            expr
        ))),
    }
}

fn scalar_to_array(scalar: &ScalarValue, num_rows: usize) -> Result<ArrayRef> {
    match scalar {
        ScalarValue::Int64(v) => {
            let array = Int64Array::from(vec![*v; num_rows]);
            Ok(Arc::new(array))
        }
        ScalarValue::Float64(v) => {
            let array = Float64Array::from(vec![v.into_inner(); num_rows]);
            Ok(Arc::new(array))
        }
        ScalarValue::Utf8(v) => {
            let array = StringArray::from(vec![v.as_str(); num_rows]);
            Ok(Arc::new(array))
        }
        _ => Err(QueryError::NotImplemented(format!(
            "Scalar type not supported: {:?}",
            scalar
        ))),
    }
}

/// Deduplicate a batch by removing duplicate rows
fn deduplicate_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    // Use a HashSet to track unique row hashes
    let mut seen: HashMap<u64, usize> = HashMap::new();
    let mut indices = Vec::new();

    for row_idx in 0..batch.num_rows() {
        let hash = hash_row(batch, row_idx);
        if seen.insert(hash, row_idx).is_none() {
            indices.push(row_idx as u64);
        }
    }

    // Take unique rows
    let mut index_builder = UInt64Builder::with_capacity(indices.len());
    for idx in indices {
        index_builder.append_value(idx);
    }
    let index_array = index_builder.finish();

    let new_columns: Vec<ArrayRef> = batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &index_array, None).map(Arc::from))
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| QueryError::Execution(e.to_string()))?;

    let result = RecordBatch::try_new(batch.schema(), new_columns)?;
    Ok(result)
}

/// Hash a row for deduplication
fn hash_row(batch: &RecordBatch, row_idx: usize) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();

    for col in batch.columns() {
        hash_array_value(col.as_ref(), row_idx, &mut hasher);
    }

    hasher.finish()
}

fn hash_array_value<H: Hasher>(array: &dyn Array, idx: usize, hasher: &mut H) {
    if array.is_null(idx) {
        0_u8.hash(hasher);
        return;
    }

    if let Some(arr) = array.as_any().downcast_ref::<Int64Array>() {
        arr.value(idx).hash(hasher);
    } else if let Some(arr) = array.as_any().downcast_ref::<Int32Array>() {
        arr.value(idx).hash(hasher);
    } else if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        arr.value(idx).hash(hasher);
    } else if let Some(arr) = array.as_any().downcast_ref::<Float64Array>() {
        arr.value(idx).to_bits().hash(hasher);
    } else {
        // For unsupported types, hash the index (not ideal but works)
        idx.hash(hasher);
    }
}

/// Hash table entry for the inner side
#[derive(Clone)]
struct InnerEntry {
    batch_idx: usize,
    row_idx: usize,
}

/// Build a hash table from inner batches keyed by join columns
fn build_hash_table(
    batches: &[RecordBatch],
    on: &[(Expr, Expr)],
) -> Result<HashMap<u64, Vec<InnerEntry>>> {
    let mut hash_table: HashMap<u64, Vec<InnerEntry>> = HashMap::new();

    for (batch_idx, batch) in batches.iter().enumerate() {
        for row_idx in 0..batch.num_rows() {
            let hash = hash_join_key(batch, row_idx, on, false)?;
            hash_table
                .entry(hash)
                .or_default()
                .push(InnerEntry { batch_idx, row_idx });
        }
    }

    Ok(hash_table)
}

/// Hash join key columns for a row
fn hash_join_key(
    batch: &RecordBatch,
    row_idx: usize,
    on: &[(Expr, Expr)],
    is_outer: bool,
) -> Result<u64> {
    use std::collections::hash_map::DefaultHasher;
    let mut hasher = DefaultHasher::new();

    for (outer_expr, inner_expr) in on {
        let expr = if is_outer { outer_expr } else { inner_expr };

        if let Expr::Column(col) = expr {
            // Find the column
            let mut found = false;
            for (i, field) in batch.schema().fields().iter().enumerate() {
                if field.name() == &col.name || field.name().ends_with(&col.name) {
                    hash_array_value(batch.column(i).as_ref(), row_idx, &mut hasher);
                    found = true;
                    break;
                }
            }
            if !found {
                return Err(QueryError::ColumnNotFound(col.name.clone()));
            }
        }
    }

    Ok(hasher.finish())
}

/// Produce output for Semi join (rows that have a match)
fn produce_semi_output(
    outer_batches: &[RecordBatch],
    inner_hash: &HashMap<u64, Vec<InnerEntry>>,
    on: &[(Expr, Expr)],
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let mut results = Vec::new();

    for batch in outer_batches {
        let mut matched_indices = Vec::new();

        for row_idx in 0..batch.num_rows() {
            let hash = hash_join_key(batch, row_idx, on, true)?;
            if inner_hash.contains_key(&hash) {
                matched_indices.push(row_idx as u64);
            }
        }

        if !matched_indices.is_empty() {
            let mut index_builder = UInt64Builder::with_capacity(matched_indices.len());
            for idx in matched_indices {
                index_builder.append_value(idx);
            }
            let index_array = index_builder.finish();

            let new_columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| compute::take(col.as_ref(), &index_array, None).map(Arc::from))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| QueryError::Execution(e.to_string()))?;

            let result = RecordBatch::try_new(schema.clone(), new_columns)?;
            results.push(result);
        }
    }

    Ok(results)
}

/// Produce output for Anti join (rows that don't have a match)
fn produce_anti_output(
    outer_batches: &[RecordBatch],
    inner_hash: &HashMap<u64, Vec<InnerEntry>>,
    on: &[(Expr, Expr)],
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let mut results = Vec::new();

    for batch in outer_batches {
        let mut unmatched_indices = Vec::new();

        for row_idx in 0..batch.num_rows() {
            let hash = hash_join_key(batch, row_idx, on, true)?;
            if !inner_hash.contains_key(&hash) {
                unmatched_indices.push(row_idx as u64);
            }
        }

        if !unmatched_indices.is_empty() {
            let mut index_builder = UInt64Builder::with_capacity(unmatched_indices.len());
            for idx in unmatched_indices {
                index_builder.append_value(idx);
            }
            let index_array = index_builder.finish();

            let new_columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|col| compute::take(col.as_ref(), &index_array, None).map(Arc::from))
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| QueryError::Execution(e.to_string()))?;

            let result = RecordBatch::try_new(schema.clone(), new_columns)?;
            results.push(result);
        }
    }

    Ok(results)
}

/// Produce output for Single join (scalar subquery - one value per outer row)
/// This joins each outer row with the single matching inner row (scalar result)
fn produce_single_output(
    outer_batches: &[RecordBatch],
    inner_batches: &[RecordBatch],
    inner_hash: &HashMap<u64, Vec<InnerEntry>>,
    on: &[(Expr, Expr)],
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let mut results = Vec::new();

    // Get the number of columns to take from outer and inner
    let outer_cols = if !outer_batches.is_empty() {
        outer_batches[0].num_columns()
    } else {
        0
    };
    let inner_cols = if !inner_batches.is_empty() {
        inner_batches[0].num_columns()
    } else {
        0
    };

    for outer_batch in outer_batches {
        // Build output arrays - one per output column
        let mut output_builders: Vec<Vec<ArrayRef>> = vec![Vec::new(); schema.fields().len()];
        let mut matched_outer_indices: Vec<u64> = Vec::new();
        let mut matched_inner_entries: Vec<InnerEntry> = Vec::new();

        for row_idx in 0..outer_batch.num_rows() {
            let hash = hash_join_key(outer_batch, row_idx, on, true)?;
            if let Some(inner_entries) = inner_hash.get(&hash) {
                // For Single join, there should be exactly one match per outer row
                // If multiple, take the first
                if let Some(entry) = inner_entries.first() {
                    matched_outer_indices.push(row_idx as u64);
                    matched_inner_entries.push(entry.clone());
                }
            }
        }

        if !matched_outer_indices.is_empty() {
            // Take matched outer rows
            let mut outer_index_builder = UInt64Builder::with_capacity(matched_outer_indices.len());
            for idx in &matched_outer_indices {
                outer_index_builder.append_value(*idx);
            }
            let outer_index_array = outer_index_builder.finish();

            let mut output_columns: Vec<ArrayRef> = Vec::new();

            // Take outer columns
            for col in outer_batch.columns() {
                let taken = compute::take(col.as_ref(), &outer_index_array, None)
                    .map_err(|e| QueryError::Execution(e.to_string()))?;
                output_columns.push(Arc::from(taken));
            }

            // Add inner columns (scalar results)
            // We need to gather values from inner batches based on matched_inner_entries
            if !inner_batches.is_empty() {
                // Only add inner columns that are expected in the output schema
                // (skip correlation columns that are already represented in the outer)
                for inner_col_idx in 0..inner_cols {
                    let inner_col_name = inner_batches[0]
                        .schema()
                        .field(inner_col_idx)
                        .name()
                        .to_string();

                    // Only add if this column is in the output schema (after the outer columns)
                    let in_output_schema = schema.fields().iter().skip(outer_cols).any(|f| {
                        f.name() == &inner_col_name
                            || f.name().ends_with(&format!(".{}", inner_col_name))
                    });
                    if !in_output_schema {
                        continue;
                    }

                    // Gather values from inner batches
                    let mut values: Vec<ArrayRef> = Vec::new();
                    for entry in &matched_inner_entries {
                        let inner_batch = &inner_batches[entry.batch_idx];
                        let inner_col = inner_batch.column(inner_col_idx);
                        // Take single row
                        let mut idx_builder = UInt64Builder::with_capacity(1);
                        idx_builder.append_value(entry.row_idx as u64);
                        let idx_array = idx_builder.finish();
                        let taken = compute::take(inner_col.as_ref(), &idx_array, None)
                            .map_err(|e| QueryError::Execution(e.to_string()))?;
                        values.push(Arc::from(taken));
                    }

                    // Concatenate all gathered values
                    if !values.is_empty() {
                        let refs: Vec<&dyn Array> = values.iter().map(|a| a.as_ref()).collect();
                        let concat = compute::concat(&refs)
                            .map_err(|e| QueryError::Execution(e.to_string()))?;
                        output_columns.push(concat);
                    }
                }
            }

            let result = RecordBatch::try_new(schema.clone(), output_columns)?;
            results.push(result);
        }
    }

    Ok(results)
}

/// Produce output for Mark join (adds boolean column for match status)
fn produce_mark_output(
    outer_batches: &[RecordBatch],
    inner_hash: &HashMap<u64, Vec<InnerEntry>>,
    on: &[(Expr, Expr)],
    schema: &SchemaRef,
) -> Result<Vec<RecordBatch>> {
    let mut results = Vec::new();

    for batch in outer_batches {
        let mut marks = Vec::with_capacity(batch.num_rows());

        for row_idx in 0..batch.num_rows() {
            let hash = hash_join_key(batch, row_idx, on, true)?;
            marks.push(inner_hash.contains_key(&hash));
        }

        // Add mark column to batch
        let mark_array: ArrayRef = Arc::new(BooleanArray::from(marks));
        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        columns.push(mark_array);

        let result = RecordBatch::try_new(schema.clone(), columns)?;
        results.push(result);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::operators::MemoryTableExec;
    use arrow::datatypes::{DataType, Field, Schema};

    #[tokio::test]
    async fn test_delim_state() {
        let state = DelimState::new();

        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        state.set_distinct_values(batch.clone(), schema.clone());

        let retrieved = state.get_distinct_values().unwrap();
        assert_eq!(retrieved.num_rows(), 3);
    }

    #[tokio::test]
    async fn test_deduplicate_batch() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 1, 3, 2, 1]))],
        )
        .unwrap();

        let deduped = deduplicate_batch(&batch).unwrap();
        assert_eq!(deduped.num_rows(), 3); // Only 1, 2, 3
    }
}
