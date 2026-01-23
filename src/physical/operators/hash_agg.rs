//! Hash aggregate operator

use crate::error::{QueryError, Result};
use crate::physical::operators::filter::evaluate_expr;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::{AggregateFunction, Expr};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Builder, Float64Array, Float64Builder,
    Int64Array, Int64Builder, StringArray, StringBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::{self, TryStreamExt};
use hashbrown::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Hash aggregate execution operator
#[derive(Debug)]
pub struct HashAggregateExec {
    input: Arc<dyn PhysicalOperator>,
    group_by: Vec<Expr>,
    aggregates: Vec<AggregateExpr>,
    schema: SchemaRef,
}

/// Aggregate expression with function and input
#[derive(Debug, Clone)]
pub struct AggregateExpr {
    pub func: AggregateFunction,
    pub input: Expr,
    pub distinct: bool,
}

impl HashAggregateExec {
    pub fn new(
        input: Arc<dyn PhysicalOperator>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            input,
            group_by,
            aggregates,
            schema,
        }
    }

    pub fn try_new(
        input: Arc<dyn PhysicalOperator>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let plan_schema = crate::planner::PlanSchema::from(input_schema.as_ref());

        let mut fields = Vec::new();

        // Group by columns
        for expr in &group_by {
            let name = expr.output_name();
            let dt = expr.data_type(&plan_schema)?;
            fields.push(Field::new(name, dt, true));
        }

        // Aggregate columns
        for agg in &aggregates {
            let name = format!("{}({})", agg.func, agg.input.output_name());
            let dt = agg.output_type(&plan_schema)?;
            fields.push(Field::new(name, dt, true));
        }

        let schema = Arc::new(Schema::new(fields));

        Ok(Self {
            input,
            group_by,
            aggregates,
            schema,
        })
    }
}

impl AggregateExpr {
    pub fn output_type(&self, schema: &crate::planner::PlanSchema) -> Result<DataType> {
        match self.func {
            AggregateFunction::Count | AggregateFunction::CountDistinct => Ok(DataType::Int64),
            AggregateFunction::Sum => {
                let input_type = self.input.data_type(schema)?;
                Ok(promote_sum_type(&input_type))
            }
            AggregateFunction::Avg => Ok(DataType::Float64),
            AggregateFunction::Min | AggregateFunction::Max => self.input.data_type(schema),
        }
    }
}

fn promote_sum_type(input: &DataType) -> DataType {
    match input {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => DataType::Int64,
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            DataType::UInt64
        }
        DataType::Float32 | DataType::Float64 => DataType::Float64,
        DataType::Decimal128(p, s) => DataType::Decimal128(*p, *s),
        _ => DataType::Float64,
    }
}

#[async_trait]
impl PhysicalOperator for HashAggregateExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.input.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        let input_stream = self.input.execute(partition).await?;

        // Collect all input batches
        let batches: Vec<RecordBatch> = input_stream.try_collect().await?;

        // Build hash table
        let result = aggregate_batches(&batches, &self.group_by, &self.aggregates, &self.schema)?;

        Ok(Box::pin(stream::once(async { Ok(result) })))
    }

    fn name(&self) -> &str {
        "HashAggregate"
    }
}

impl fmt::Display for HashAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let groups: Vec<String> = self.group_by.iter().map(|e| e.to_string()).collect();
        let aggs: Vec<String> = self
            .aggregates
            .iter()
            .map(|a| format!("{}({})", a.func, a.input))
            .collect();
        write!(
            f,
            "HashAggregate: group_by=[{}], aggs=[{}]",
            groups.join(", "),
            aggs.join(", ")
        )
    }
}

/// Group key for hash map
#[derive(Clone)]
struct GroupKey {
    values: Vec<GroupValue>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
enum GroupValue {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(ordered_float::OrderedFloat<f64>),
    String(String),
    Date32(i32),
}

impl PartialEq for GroupKey {
    fn eq(&self, other: &Self) -> bool {
        if self.values.len() != other.values.len() {
            return false;
        }
        self.values
            .iter()
            .zip(other.values.iter())
            .all(|(a, b)| match (a, b) {
                (GroupValue::Null, GroupValue::Null) => true,
                (GroupValue::Bool(a), GroupValue::Bool(b)) => a == b,
                (GroupValue::Int64(a), GroupValue::Int64(b)) => a == b,
                (GroupValue::Float64(a), GroupValue::Float64(b)) => a == b,
                (GroupValue::String(a), GroupValue::String(b)) => a == b,
                (GroupValue::Date32(a), GroupValue::Date32(b)) => a == b,
                _ => false,
            })
    }
}

impl Eq for GroupKey {}

impl Hash for GroupKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for v in &self.values {
            match v {
                GroupValue::Null => 0u8.hash(state),
                GroupValue::Bool(b) => {
                    1u8.hash(state);
                    b.hash(state);
                }
                GroupValue::Int64(i) => {
                    2u8.hash(state);
                    i.hash(state);
                }
                GroupValue::Float64(f) => {
                    3u8.hash(state);
                    f.hash(state);
                }
                GroupValue::String(s) => {
                    4u8.hash(state);
                    s.hash(state);
                }
                GroupValue::Date32(d) => {
                    5u8.hash(state);
                    d.hash(state);
                }
            }
        }
    }
}

/// Accumulator state for each group
struct AccumulatorState {
    count: i64,
    sum: f64,
    sum_i64: i64,
    min_f64: Option<f64>,
    max_f64: Option<f64>,
    min_i64: Option<i64>,
    max_i64: Option<i64>,
    min_str: Option<String>,
    max_str: Option<String>,
    distinct_set: Option<std::collections::HashSet<GroupValue>>,
}

impl Default for AccumulatorState {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_i64: 0,
            min_f64: None,
            max_f64: None,
            min_i64: None,
            max_i64: None,
            min_str: None,
            max_str: None,
            distinct_set: None,
        }
    }
}

fn aggregate_batches(
    batches: &[RecordBatch],
    group_by: &[Expr],
    aggregates: &[AggregateExpr],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    // Special case: scalar aggregate (no GROUP BY) - use Arrow's SIMD kernels
    // Note: COUNT DISTINCT requires the hash-based path for tracking distinct values
    let has_count_distinct = aggregates
        .iter()
        .any(|a| a.func == AggregateFunction::CountDistinct);
    if group_by.is_empty() && batches.len() == 1 && aggregates.len() == 1 && !has_count_distinct {
        return aggregate_scalar_simd(&batches[0], &aggregates[0], schema);
    }

    // Regular hash-based aggregation for grouped queries
    aggregate_batches_hash(batches, group_by, aggregates, schema)
}

/// Fast scalar aggregate using optimized iterators (for queries without GROUP BY)
fn aggregate_scalar_simd(
    batch: &RecordBatch,
    aggregate: &AggregateExpr,
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let input = evaluate_expr(batch, &aggregate.input)?;

    let result: ArrayRef = match aggregate.func {
        AggregateFunction::Count => {
            let count = input.len() - input.null_count();
            Arc::new(Int64Array::from(vec![count as i64]))
        }
        AggregateFunction::Sum => {
            if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                // Use values() for iterator over non-null values
                let sum = a
                    .values()
                    .iter()
                    .fold(0i64, |acc, &x| acc.saturating_add(x));
                Arc::new(Int64Array::from(vec![sum]))
            } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                let sum = a.values().iter().fold(0.0f64, |acc, &x| acc + x);
                Arc::new(Float64Array::from(vec![sum]))
            } else if let Some(a) = input.as_any().downcast_ref::<arrow::array::Int32Array>() {
                let sum = a
                    .values()
                    .iter()
                    .fold(0i64, |acc, &x| acc.saturating_add(x as i64));
                Arc::new(Int64Array::from(vec![sum]))
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "SUM not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::Avg => {
            if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let sum = a
                    .values()
                    .iter()
                    .fold(0i64, |acc, &x| acc.saturating_add(x));
                let count = (a.len() - a.null_count()) as f64;
                Arc::new(Float64Array::from(vec![sum as f64 / count.max(1.0)]))
            } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                let sum = a.values().iter().fold(0.0f64, |acc, &x| acc + x);
                let count = (a.len() - a.null_count()) as f64;
                Arc::new(Float64Array::from(vec![sum / count.max(1.0)]))
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "AVG not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::Min => {
            if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let min = a.iter().filter_map(|x| x).min().unwrap_or(i64::MAX);
                Arc::new(Int64Array::from(vec![min]))
            } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                let min = a
                    .iter()
                    .filter_map(|x| x)
                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap_or(f64::MAX);
                Arc::new(Float64Array::from(vec![min]))
            } else if let Some(a) = input.as_any().downcast_ref::<StringArray>() {
                let min = a.iter().filter_map(|x| x).min();
                match min {
                    Some(val) => Arc::new(StringArray::from(vec![Some(val)])),
                    None => Arc::new(StringArray::from(vec![Option::<&str>::None])),
                }
            } else if let Some(a) = input.as_any().downcast_ref::<Date32Array>() {
                let min = a.iter().filter_map(|x| x).min().unwrap_or(i32::MAX);
                Arc::new(Date32Array::from(vec![min]))
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "MIN not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::Max => {
            if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let max = a.iter().filter_map(|x| x).max().unwrap_or(i64::MIN);
                Arc::new(Int64Array::from(vec![max]))
            } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                let max = a
                    .iter()
                    .filter_map(|x| x)
                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap_or(f64::MIN);
                Arc::new(Float64Array::from(vec![max]))
            } else if let Some(a) = input.as_any().downcast_ref::<StringArray>() {
                let max = a.iter().filter_map(|x| x).max();
                match max {
                    Some(val) => Arc::new(StringArray::from(vec![Some(val)])),
                    None => Arc::new(StringArray::from(vec![Option::<&str>::None])),
                }
            } else if let Some(a) = input.as_any().downcast_ref::<Date32Array>() {
                let max = a.iter().filter_map(|x| x).max().unwrap_or(i32::MIN);
                Arc::new(Date32Array::from(vec![max]))
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "MAX not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::CountDistinct => {
            return Err(QueryError::NotImplemented(
                "COUNT DISTINCT not implemented for SIMD aggregation".into(),
            ));
        }
    };

    RecordBatch::try_new(schema.clone(), vec![result]).map_err(Into::into)
}

/// Hash-based aggregation for grouped queries
fn aggregate_batches_hash(
    batches: &[RecordBatch],
    group_by: &[Expr],
    aggregates: &[AggregateExpr],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    // Map from group key to accumulator states (one per aggregate)
    let mut groups: HashMap<GroupKey, Vec<AccumulatorState>> = HashMap::new();

    for batch in batches {
        // Evaluate group by expressions
        let group_arrays: Result<Vec<ArrayRef>> =
            group_by.iter().map(|e| evaluate_expr(batch, e)).collect();
        let group_arrays = group_arrays?;

        // Evaluate aggregate inputs
        let agg_inputs: Result<Vec<ArrayRef>> = aggregates
            .iter()
            .map(|a| evaluate_expr(batch, &a.input))
            .collect();
        let agg_inputs = agg_inputs?;

        // Process each row
        for row in 0..batch.num_rows() {
            let key = extract_group_key(&group_arrays, row);

            let states = groups.entry(key).or_insert_with(|| {
                (0..aggregates.len())
                    .map(|_| AccumulatorState::default())
                    .collect()
            });

            // Update each accumulator
            for (i, agg) in aggregates.iter().enumerate() {
                let input = &agg_inputs[i];
                update_accumulator(&mut states[i], agg.func, input, row);
            }
        }
    }

    // Handle empty input with no groups (scalar aggregates)
    if batches.iter().all(|b| b.num_rows() == 0) || (batches.is_empty() && group_by.is_empty()) {
        if group_by.is_empty() {
            // Return a single row with default aggregate values
            groups.insert(
                GroupKey { values: vec![] },
                (0..aggregates.len())
                    .map(|_| AccumulatorState::default())
                    .collect(),
            );
        }
    }

    // Build output arrays
    let num_groups = groups.len();
    let mut output_arrays: Vec<ArrayRef> = Vec::new();

    // Group by columns
    for (i, _) in group_by.iter().enumerate() {
        let field = schema.field(i);
        let arr = build_group_array(&groups, i, num_groups, field.data_type())?;
        output_arrays.push(arr);
    }

    // Aggregate columns
    for (i, agg) in aggregates.iter().enumerate() {
        let field = schema.field(group_by.len() + i);
        let arr = build_agg_array(&groups, i, agg.func, num_groups, field.data_type())?;
        output_arrays.push(arr);
    }

    RecordBatch::try_new(schema.clone(), output_arrays).map_err(Into::into)
}

fn extract_group_key(arrays: &[ArrayRef], row: usize) -> GroupKey {
    let values: Vec<GroupValue> = arrays
        .iter()
        .map(|arr| extract_group_value(arr, row))
        .collect();
    GroupKey { values }
}

fn extract_group_value(arr: &ArrayRef, row: usize) -> GroupValue {
    if arr.is_null(row) {
        return GroupValue::Null;
    }

    if let Some(a) = arr.as_any().downcast_ref::<Int64Array>() {
        return GroupValue::Int64(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return GroupValue::Int64(a.value(row) as i64);
    }
    if let Some(a) = arr.as_any().downcast_ref::<Float64Array>() {
        return GroupValue::Float64(ordered_float::OrderedFloat(a.value(row)));
    }
    if let Some(a) = arr.as_any().downcast_ref::<arrow::array::StringArray>() {
        return GroupValue::String(a.value(row).to_string());
    }
    if let Some(a) = arr.as_any().downcast_ref::<Date32Array>() {
        return GroupValue::Date32(a.value(row));
    }
    if let Some(a) = arr.as_any().downcast_ref::<BooleanArray>() {
        return GroupValue::Bool(a.value(row));
    }

    GroupValue::Null
}

fn update_accumulator(
    state: &mut AccumulatorState,
    func: AggregateFunction,
    input: &ArrayRef,
    row: usize,
) {
    match func {
        AggregateFunction::Count => {
            if !input.is_null(row) {
                state.count += 1;
            }
        }
        AggregateFunction::CountDistinct => {
            if !input.is_null(row) {
                let value = extract_group_value(input, row);
                let set = state
                    .distinct_set
                    .get_or_insert_with(std::collections::HashSet::new);
                set.insert(value);
            }
        }
        AggregateFunction::Sum => {
            if !input.is_null(row) {
                state.count += 1;
                if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                    state.sum_i64 += a.value(row);
                    state.sum += a.value(row) as f64;
                } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                    state.sum += a.value(row);
                } else if let Some(a) = input.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    state.sum_i64 += a.value(row) as i64;
                    state.sum += a.value(row) as f64;
                } else if let Some(a) = input
                    .as_any()
                    .downcast_ref::<arrow::array::Decimal128Array>()
                {
                    state.sum_i64 += a.value(row) as i64;
                    state.sum += a.value(row) as f64;
                }
            }
        }
        AggregateFunction::Avg => {
            if !input.is_null(row) {
                state.count += 1;
                if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                    state.sum += a.value(row) as f64;
                } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                    state.sum += a.value(row);
                } else if let Some(a) = input.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    state.sum += a.value(row) as f64;
                }
            }
        }
        AggregateFunction::Min => {
            if !input.is_null(row) {
                if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                    let val = a.value(row);
                    state.min_i64 = Some(state.min_i64.map_or(val, |m| m.min(val)));
                } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                    let val = a.value(row);
                    state.min_f64 = Some(state.min_f64.map_or(val, |m| m.min(val)));
                } else if let Some(a) = input.as_any().downcast_ref::<arrow::array::StringArray>() {
                    let val = a.value(row).to_string();
                    state.min_str = Some(state.min_str.as_ref().map_or(val.clone(), |m| {
                        if val < *m {
                            val
                        } else {
                            m.clone()
                        }
                    }));
                } else if let Some(a) = input.as_any().downcast_ref::<Date32Array>() {
                    let val = a.value(row) as i64;
                    state.min_i64 = Some(state.min_i64.map_or(val, |m| m.min(val)));
                }
            }
        }
        AggregateFunction::Max => {
            if !input.is_null(row) {
                if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                    let val = a.value(row);
                    state.max_i64 = Some(state.max_i64.map_or(val, |m| m.max(val)));
                } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                    let val = a.value(row);
                    state.max_f64 = Some(state.max_f64.map_or(val, |m| m.max(val)));
                } else if let Some(a) = input.as_any().downcast_ref::<arrow::array::StringArray>() {
                    let val = a.value(row).to_string();
                    state.max_str = Some(state.max_str.as_ref().map_or(val.clone(), |m| {
                        if val > *m {
                            val
                        } else {
                            m.clone()
                        }
                    }));
                } else if let Some(a) = input.as_any().downcast_ref::<Date32Array>() {
                    let val = a.value(row) as i64;
                    state.max_i64 = Some(state.max_i64.map_or(val, |m| m.max(val)));
                }
            }
        }
    }
}

fn build_group_array(
    groups: &HashMap<GroupKey, Vec<AccumulatorState>>,
    col_idx: usize,
    num_groups: usize,
    data_type: &DataType,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Int32 => {
            let mut builder = arrow::array::Int32Builder::with_capacity(num_groups);
            for key in groups.keys() {
                match &key.values[col_idx] {
                    GroupValue::Int64(v) => builder.append_value(*v as i32),
                    GroupValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Int64 => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for key in groups.keys() {
                match &key.values[col_idx] {
                    GroupValue::Int64(v) => builder.append_value(*v),
                    GroupValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for key in groups.keys() {
                match &key.values[col_idx] {
                    GroupValue::Float64(v) => builder.append_value(v.0),
                    GroupValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(num_groups, num_groups * 16);
            for key in groups.keys() {
                match &key.values[col_idx] {
                    GroupValue::String(v) => builder.append_value(v),
                    GroupValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        DataType::Date32 => {
            let mut builder = arrow::array::Date32Builder::with_capacity(num_groups);
            for key in groups.keys() {
                match &key.values[col_idx] {
                    GroupValue::Date32(v) => builder.append_value(*v),
                    GroupValue::Int64(v) => builder.append_value(*v as i32),
                    GroupValue::Null => builder.append_null(),
                    _ => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(QueryError::NotImplemented(format!(
            "Group by type not supported: {:?}",
            data_type
        ))),
    }
}

fn build_agg_array(
    groups: &HashMap<GroupKey, Vec<AccumulatorState>>,
    agg_idx: usize,
    func: AggregateFunction,
    num_groups: usize,
    data_type: &DataType,
) -> Result<ArrayRef> {
    match (func, data_type) {
        (AggregateFunction::Count, DataType::Int64) => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for states in groups.values() {
                builder.append_value(states[agg_idx].count);
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::CountDistinct, DataType::Int64) => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for states in groups.values() {
                let count = states[agg_idx]
                    .distinct_set
                    .as_ref()
                    .map(|s| s.len() as i64)
                    .unwrap_or(0);
                builder.append_value(count);
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Sum, DataType::Int64) => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for states in groups.values() {
                builder.append_value(states[agg_idx].sum_i64);
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Sum, DataType::UInt64) => {
            let mut builder = UInt64Builder::with_capacity(num_groups);
            for states in groups.values() {
                builder.append_value(states[agg_idx].sum_i64 as u64);
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Sum, DataType::Float64) => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for states in groups.values() {
                builder.append_value(states[agg_idx].sum);
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Sum, DataType::Decimal128(p, s)) => {
            let mut builder = Decimal128Builder::with_capacity(num_groups);
            for states in groups.values() {
                builder.append_value(states[agg_idx].sum_i64 as i128);
            }
            Ok(Arc::new(builder.finish().with_precision_and_scale(*p, *s)?))
        }
        (AggregateFunction::Avg, DataType::Float64) => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for states in groups.values() {
                let state = &states[agg_idx];
                if state.count > 0 {
                    builder.append_value(state.sum / state.count as f64);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Min, DataType::Int64) => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for states in groups.values() {
                match states[agg_idx].min_i64 {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Max, DataType::Int64) => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for states in groups.values() {
                match states[agg_idx].max_i64 {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Min, DataType::Float64) => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for states in groups.values() {
                match states[agg_idx].min_f64 {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Max, DataType::Float64) => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for states in groups.values() {
                match states[agg_idx].max_f64 {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Min | AggregateFunction::Max, DataType::Utf8) => {
            let mut builder = StringBuilder::with_capacity(num_groups, num_groups * 16);
            for states in groups.values() {
                let val = if func == AggregateFunction::Min {
                    &states[agg_idx].min_str
                } else {
                    &states[agg_idx].max_str
                };
                match val {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Min | AggregateFunction::Max, DataType::Date32) => {
            let mut builder = arrow::array::Date32Builder::with_capacity(num_groups);
            for states in groups.values() {
                let val = if func == AggregateFunction::Min {
                    states[agg_idx].min_i64
                } else {
                    states[agg_idx].max_i64
                };
                match val {
                    Some(v) => builder.append_value(v as i32),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        _ => Err(QueryError::NotImplemented(format!(
            "Aggregate {:?} with type {:?} not supported",
            func, data_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::MemoryTableExec;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::TryStreamExt;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("category", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "a", "b", "a"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_hash_aggregate_sum() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));

        let group_by = vec![Expr::column("category")];
        let aggregates = vec![AggregateExpr {
            func: AggregateFunction::Sum,
            input: Expr::column("value"),
            distinct: false,
        }];

        let agg = HashAggregateExec::try_new(scan, group_by, aggregates).unwrap();

        let stream = agg.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2); // Two groups: a and b
    }

    #[tokio::test]
    async fn test_hash_aggregate_count() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));

        let group_by = vec![Expr::column("category")];
        let aggregates = vec![AggregateExpr {
            func: AggregateFunction::Count,
            input: Expr::column("value"),
            distinct: false,
        }];

        let agg = HashAggregateExec::try_new(scan, group_by, aggregates).unwrap();

        let stream = agg.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 2);
    }

    #[tokio::test]
    async fn test_scalar_aggregate() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));

        // No group by - scalar aggregate
        let group_by = vec![];
        let aggregates = vec![
            AggregateExpr {
                func: AggregateFunction::Sum,
                input: Expr::column("value"),
                distinct: false,
            },
            AggregateExpr {
                func: AggregateFunction::Count,
                input: Expr::column("value"),
                distinct: false,
            },
        ];

        let agg = HashAggregateExec::try_new(scan, group_by, aggregates).unwrap();

        let stream = agg.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1); // Single row result

        let sum = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(sum, 150); // 10 + 20 + 30 + 40 + 50

        let count = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 5);
    }
}
