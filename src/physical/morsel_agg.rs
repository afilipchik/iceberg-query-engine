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
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Builder, Float64Array, Float64Builder,
    Int64Array, Int64Builder, StringArray, StringBuilder,
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
    Avg { sum: f64, count: i64 },
    Min(Option<ScalarValue>),
    Max(Option<ScalarValue>),
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
            _ => AccumulatorState::Count(0), // Default for unsupported
        }
    }

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
            _ => {}
        }
    }

    fn finalize(&self) -> ScalarValue {
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

fn scalar_to_i64(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int8(v) => Some(*v as i64),
        ScalarValue::Int16(v) => Some(*v as i64),
        ScalarValue::Int32(v) => Some(*v as i64),
        ScalarValue::Int64(v) => Some(*v),
        _ => None,
    }
}

/// Thread-local aggregation state
#[derive(Clone)]
pub struct AggregationState {
    /// Hash table: group key -> accumulator states
    groups: HashMap<GroupKey, Vec<AccumulatorState>>,
    /// Aggregate functions
    agg_funcs: Vec<AggregateFunction>,
    /// Input types for aggregates
    input_types: Vec<DataType>,
}

impl Default for AggregationState {
    fn default() -> Self {
        Self {
            groups: HashMap::new(),
            agg_funcs: Vec::new(),
            input_types: Vec::new(),
        }
    }
}

impl AggregationState {
    pub fn new(agg_funcs: Vec<AggregateFunction>, input_types: Vec<DataType>) -> Self {
        Self {
            groups: HashMap::new(),
            agg_funcs,
            input_types,
        }
    }

    /// Process a batch and update the hash table
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

        // Evaluate group-by expressions
        let group_arrays: Vec<ArrayRef> = group_by_exprs
            .iter()
            .map(|expr| evaluate_expr(batch, expr))
            .collect::<Result<Vec<_>>>()?;

        // Evaluate aggregate input expressions
        let agg_arrays: Vec<ArrayRef> = agg_input_exprs
            .iter()
            .map(|expr| evaluate_expr(batch, expr))
            .collect::<Result<Vec<_>>>()?;

        // Process each row
        for row in 0..num_rows {
            // Extract group key
            let key = GroupKey {
                values: group_arrays
                    .iter()
                    .map(|arr| extract_scalar(arr, row))
                    .collect(),
            };

            // Get or create accumulators for this group
            let accumulators = self.groups.entry(key).or_insert_with(|| {
                self.agg_funcs
                    .iter()
                    .zip(&self.input_types)
                    .map(|(func, dt)| AccumulatorState::new(func, dt))
                    .collect()
            });

            // Update each accumulator
            for (i, acc) in accumulators.iter_mut().enumerate() {
                let value = extract_scalar(&agg_arrays[i], row);
                acc.update(&value);
            }
        }

        Ok(())
    }

    /// Merge another state into this one
    pub fn merge(&mut self, other: &AggregationState) {
        for (key, other_accs) in &other.groups {
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

    /// Build the output RecordBatch
    pub fn build_output(&self, schema: &SchemaRef) -> Result<RecordBatch> {
        let num_groups = self.groups.len();
        let num_group_cols = schema.fields().len() - self.agg_funcs.len();

        // Collect all groups and their values
        let groups: Vec<(&GroupKey, &Vec<AccumulatorState>)> = self.groups.iter().collect();

        // Build arrays for each column
        let mut arrays: Vec<ArrayRef> = Vec::new();

        // Group-by columns
        for col_idx in 0..num_group_cols {
            let field = schema.field(col_idx);
            let array = build_group_array(
                groups.iter().map(|(k, _)| &k.values[col_idx]),
                field.data_type(),
                num_groups,
            )?;
            arrays.push(array);
        }

        // Aggregate columns
        for agg_idx in 0..self.agg_funcs.len() {
            let values: Vec<ScalarValue> = groups
                .iter()
                .map(|(_, accs)| accs[agg_idx].finalize())
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
        DataType::Decimal128(p, s) => {
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
    capacity: usize,
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
            use rust_decimal::prelude::ToPrimitive;
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
