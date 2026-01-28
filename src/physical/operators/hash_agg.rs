//! Hash aggregate operator

use crate::error::{QueryError, Result};
use crate::physical::operators::filter::evaluate_expr;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::{AggregateFunction, Expr, ScalarValue};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Decimal128Builder, Float64Array, Float64Builder,
    Int64Array, Int64Builder, StringArray, StringBuilder, UInt64Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::{self, TryStreamExt};
use hashbrown::HashMap;
use rayon::prelude::*;
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
    /// Optional second argument for functions like APPROX_PERCENTILE
    pub second_arg: Option<Expr>,
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
            // Statistical aggregates always return Float64
            AggregateFunction::Stddev
            | AggregateFunction::StddevPop
            | AggregateFunction::StddevSamp
            | AggregateFunction::Variance
            | AggregateFunction::VarPop
            | AggregateFunction::VarSamp => Ok(DataType::Float64),
            // Boolean aggregates return Boolean
            AggregateFunction::BoolAnd | AggregateFunction::BoolOr => Ok(DataType::Boolean),
            // New aggregate functions
            AggregateFunction::CountIf => Ok(DataType::Int64),
            AggregateFunction::AnyValue | AggregateFunction::Arbitrary => {
                self.input.data_type(schema)
            }
            AggregateFunction::GeometricMean => Ok(DataType::Float64),
            AggregateFunction::Checksum => Ok(DataType::Int64),
            AggregateFunction::BitwiseAndAgg
            | AggregateFunction::BitwiseOrAgg
            | AggregateFunction::BitwiseXorAgg => Ok(DataType::Int64),
            AggregateFunction::Listagg => Ok(DataType::Utf8),
            // Correlation and covariance aggregates
            AggregateFunction::Corr
            | AggregateFunction::CovarPop
            | AggregateFunction::CovarSamp
            | AggregateFunction::Kurtosis
            | AggregateFunction::Skewness
            | AggregateFunction::RegrSlope
            | AggregateFunction::RegrIntercept
            | AggregateFunction::RegrAvgx
            | AggregateFunction::RegrAvgy => Ok(DataType::Float64),
            AggregateFunction::RegrCount => Ok(DataType::Int64),
            // Approximate aggregates
            AggregateFunction::ApproxPercentile => Ok(DataType::Float64),
            AggregateFunction::ApproxDistinct => Ok(DataType::Int64),
            // Multi-value aggregates
            AggregateFunction::MaxBy | AggregateFunction::MinBy => self.input.data_type(schema),
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
        // Aggregation always produces a single partition by collecting from all input partitions
        if partition != 0 {
            return Ok(Box::pin(stream::empty()));
        }

        // Collect from all input partitions in parallel using tokio
        let input_partitions = self.input.output_partitions().max(1);

        // Spawn concurrent tasks to collect from each partition
        let mut handles = Vec::with_capacity(input_partitions);
        for part in 0..input_partitions {
            let input = self.input.clone();
            handles.push(tokio::spawn(async move {
                let input_stream = input.execute(part).await?;
                let batches: Vec<RecordBatch> = input_stream.try_collect().await?;
                Ok::<_, QueryError>(batches)
            }));
        }

        // Collect results from all partitions
        let mut all_batches = Vec::new();
        for handle in handles {
            let batches = handle
                .await
                .map_err(|e| QueryError::Execution(format!("Task join error: {}", e)))??;
            all_batches.extend(batches);
        }

        // Build hash table from all collected batches
        // For large batch counts, use parallel aggregation
        let result = if all_batches.len() > 4 {
            aggregate_batches_parallel(
                &all_batches,
                &self.group_by,
                &self.aggregates,
                &self.schema,
            )?
        } else {
            aggregate_batches(&all_batches, &self.group_by, &self.aggregates, &self.schema)?
        };

        Ok(Box::pin(stream::once(async { Ok(result) })))
    }

    fn name(&self) -> &str {
        "HashAggregate"
    }

    fn output_partitions(&self) -> usize {
        // Aggregation produces a single partition (all groups combined)
        1
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
    sum_squares: f64, // For variance/stddev calculation
    min_f64: Option<f64>,
    max_f64: Option<f64>,
    min_i64: Option<i64>,
    max_i64: Option<i64>,
    min_str: Option<String>,
    max_str: Option<String>,
    distinct_set: Option<std::collections::HashSet<GroupValue>>,
    // Boolean aggregate states
    bool_and: Option<bool>,
    bool_or: Option<bool>,
    // New aggregate states
    count_if: i64,                 // For COUNT_IF
    any_value: Option<GroupValue>, // For ANY_VALUE/ARBITRARY
    log_sum: f64,                  // For GEOMETRIC_MEAN (sum of logs)
    log_count: i64,                // For GEOMETRIC_MEAN (count for log)
    bitwise_and: Option<i64>,      // For BITWISE_AND_AGG
    bitwise_or: Option<i64>,       // For BITWISE_OR_AGG
    bitwise_xor: i64,              // For BITWISE_XOR_AGG
    string_list: Vec<String>,      // For LISTAGG
    // Correlation/covariance states
    sum_x: f64,         // For correlation
    sum_y: f64,         // For correlation
    sum_xy: f64,        // For correlation
    sum_x_squares: f64, // For correlation
    sum_y_squares: f64, // For correlation
    // For kurtosis/skewness
    sum_cubes: f64,  // Sum of (x - mean)^3
    sum_fourth: f64, // Sum of (x - mean)^4
    // For max_by/min_by
    max_by_value: Option<f64>,         // The max value of the second arg
    max_by_result: Option<GroupValue>, // The value to return
    min_by_value: Option<f64>,         // The min value of the second arg
    min_by_result: Option<GroupValue>, // The value to return
    // Approximate percentile - stores values for sorting
    approx_values: Vec<f64>,
    // Percentile value for APPROX_PERCENTILE (0.0 to 1.0)
    percentile: f64,
}

impl Default for AccumulatorState {
    fn default() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            sum_i64: 0,
            sum_squares: 0.0,
            min_f64: None,
            max_f64: None,
            min_i64: None,
            max_i64: None,
            min_str: None,
            max_str: None,
            distinct_set: None,
            bool_and: None,
            bool_or: None,
            // New aggregate states
            count_if: 0,
            any_value: None,
            log_sum: 0.0,
            log_count: 0,
            bitwise_and: None,
            bitwise_or: None,
            bitwise_xor: 0,
            string_list: Vec::new(),
            sum_x: 0.0,
            sum_y: 0.0,
            sum_xy: 0.0,
            sum_x_squares: 0.0,
            sum_y_squares: 0.0,
            sum_cubes: 0.0,
            sum_fourth: 0.0,
            max_by_value: None,
            max_by_result: None,
            min_by_value: None,
            min_by_result: None,
            approx_values: Vec::new(),
            percentile: 0.5, // Default to median
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

/// Parallel aggregation using rayon for multi-core performance
fn aggregate_batches_parallel(
    batches: &[RecordBatch],
    group_by: &[Expr],
    aggregates: &[AggregateExpr],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    // Use scalar SIMD path for simple cases
    let has_count_distinct = aggregates
        .iter()
        .any(|a| a.func == AggregateFunction::CountDistinct);
    if group_by.is_empty() && batches.len() == 1 && aggregates.len() == 1 && !has_count_distinct {
        return aggregate_scalar_simd(&batches[0], &aggregates[0], schema);
    }

    // Determine number of threads
    let num_threads = rayon::current_num_threads().min(batches.len());
    if num_threads <= 1 {
        return aggregate_batches_hash(batches, group_by, aggregates, schema);
    }

    // Split batches into chunks for parallel processing
    let chunk_size = (batches.len() + num_threads - 1) / num_threads;
    let chunks: Vec<_> = batches.chunks(chunk_size).collect();

    // Build partial hash tables in parallel
    let partial_results: Vec<Result<HashMap<GroupKey, Vec<AccumulatorState>>>> = chunks
        .par_iter()
        .map(|chunk| build_partial_hash_table(chunk, group_by, aggregates))
        .collect();

    // Check for errors and collect successful results
    let mut merged_groups: HashMap<GroupKey, Vec<AccumulatorState>> = HashMap::new();
    for result in partial_results {
        let partial = result?;
        for (key, states) in partial {
            merged_groups
                .entry(key)
                .and_modify(|existing| {
                    for (i, state) in states.iter().enumerate() {
                        merge_accumulator_states(&mut existing[i], state, &aggregates[i].func);
                    }
                })
                .or_insert(states);
        }
    }

    // Handle empty input with no groups (scalar aggregates)
    if merged_groups.is_empty() && group_by.is_empty() {
        merged_groups.insert(
            GroupKey { values: vec![] },
            aggregates
                .iter()
                .map(|_| AccumulatorState::default())
                .collect(),
        );
    }

    // Build output arrays
    build_output_from_groups(&merged_groups, group_by, aggregates, schema)
}

/// Build a partial hash table from a subset of batches
fn build_partial_hash_table(
    batches: &[RecordBatch],
    group_by: &[Expr],
    aggregates: &[AggregateExpr],
) -> Result<HashMap<GroupKey, Vec<AccumulatorState>>> {
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
                aggregates
                    .iter()
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

    Ok(groups)
}

/// Merge two accumulator states
fn merge_accumulator_states(
    target: &mut AccumulatorState,
    source: &AccumulatorState,
    func: &AggregateFunction,
) {
    match func {
        AggregateFunction::Count | AggregateFunction::CountDistinct => {
            target.count += source.count;
        }
        AggregateFunction::Sum => {
            target.sum += source.sum;
            target.sum_i64 = target.sum_i64.saturating_add(source.sum_i64);
        }
        AggregateFunction::Avg => {
            // For AVG, we need sum and count
            target.sum += source.sum;
            target.count += source.count;
        }
        AggregateFunction::Min => {
            if let (Some(t), Some(s)) = (&mut target.min_f64, &source.min_f64) {
                *t = t.min(*s);
            } else if source.min_f64.is_some() {
                target.min_f64 = source.min_f64;
            }
            if let (Some(t), Some(s)) = (&mut target.min_i64, &source.min_i64) {
                *t = (*t).min(*s);
            } else if source.min_i64.is_some() {
                target.min_i64 = source.min_i64;
            }
            if let (Some(t), Some(s)) = (&mut target.min_str, &source.min_str) {
                if s < t {
                    *t = s.clone();
                }
            } else if source.min_str.is_some() {
                target.min_str = source.min_str.clone();
            }
        }
        AggregateFunction::Max => {
            if let (Some(t), Some(s)) = (&mut target.max_f64, &source.max_f64) {
                *t = t.max(*s);
            } else if source.max_f64.is_some() {
                target.max_f64 = source.max_f64;
            }
            if let (Some(t), Some(s)) = (&mut target.max_i64, &source.max_i64) {
                *t = (*t).max(*s);
            } else if source.max_i64.is_some() {
                target.max_i64 = source.max_i64;
            }
            if let (Some(t), Some(s)) = (&mut target.max_str, &source.max_str) {
                if s > t {
                    *t = s.clone();
                }
            } else if source.max_str.is_some() {
                target.max_str = source.max_str.clone();
            }
        }
        AggregateFunction::Stddev
        | AggregateFunction::StddevSamp
        | AggregateFunction::StddevPop
        | AggregateFunction::Variance
        | AggregateFunction::VarSamp
        | AggregateFunction::VarPop => {
            // Use Welford's online algorithm for parallel merge
            let n1 = target.count as f64;
            let n2 = source.count as f64;
            let n = n1 + n2;
            if n > 0.0 {
                let delta = source.sum / n2.max(1.0) - target.sum / n1.max(1.0);
                target.sum += source.sum;
                target.sum_squares += source.sum_squares + delta * delta * n1 * n2 / n;
                target.count += source.count;
            }
        }
        _ => {
            // For other functions, just merge counts and sums
            target.count += source.count;
            target.sum += source.sum;
        }
    }
}

/// Build output RecordBatch from grouped hash map
fn build_output_from_groups(
    groups: &HashMap<GroupKey, Vec<AccumulatorState>>,
    group_by: &[Expr],
    aggregates: &[AggregateExpr],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    let num_groups = groups.len();
    let mut output_arrays: Vec<ArrayRef> = Vec::new();

    // Group by columns
    for (i, _) in group_by.iter().enumerate() {
        let field = schema.field(i);
        let arr = build_group_array(groups, i, num_groups, field.data_type())?;
        output_arrays.push(arr);
    }

    // Aggregate columns
    for (i, agg) in aggregates.iter().enumerate() {
        let field = schema.field(group_by.len() + i);
        let arr = build_agg_array(groups, i, agg.func, num_groups, field.data_type())?;
        output_arrays.push(arr);
    }

    RecordBatch::try_new(schema.clone(), output_arrays).map_err(Into::into)
}

/// Public interface for aggregate_batches used by spillable operators
pub fn aggregate_batches_external(
    batches: &[RecordBatch],
    group_by: &[Expr],
    aggregates: &[AggregateExpr],
    schema: &SchemaRef,
) -> Result<RecordBatch> {
    aggregate_batches(batches, group_by, aggregates, schema)
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
                let min = a.iter().flatten().min().unwrap_or(i64::MAX);
                Arc::new(Int64Array::from(vec![min]))
            } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                let min = a
                    .iter()
                    .flatten()
                    .min_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap_or(f64::MAX);
                Arc::new(Float64Array::from(vec![min]))
            } else if let Some(a) = input.as_any().downcast_ref::<StringArray>() {
                let min = a.iter().flatten().min();
                match min {
                    Some(val) => Arc::new(StringArray::from(vec![Some(val)])),
                    None => Arc::new(StringArray::from(vec![Option::<&str>::None])),
                }
            } else if let Some(a) = input.as_any().downcast_ref::<Date32Array>() {
                let min = a.iter().flatten().min().unwrap_or(i32::MAX);
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
                let max = a.iter().flatten().max().unwrap_or(i64::MIN);
                Arc::new(Int64Array::from(vec![max]))
            } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                let max = a
                    .iter()
                    .flatten()
                    .max_by(|a, b| a.partial_cmp(b).unwrap())
                    .unwrap_or(f64::MIN);
                Arc::new(Float64Array::from(vec![max]))
            } else if let Some(a) = input.as_any().downcast_ref::<StringArray>() {
                let max = a.iter().flatten().max();
                match max {
                    Some(val) => Arc::new(StringArray::from(vec![Some(val)])),
                    None => Arc::new(StringArray::from(vec![Option::<&str>::None])),
                }
            } else if let Some(a) = input.as_any().downcast_ref::<Date32Array>() {
                let max = a.iter().flatten().max().unwrap_or(i32::MIN);
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
        // Statistical aggregates
        AggregateFunction::Stddev | AggregateFunction::StddevSamp => {
            if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let values: Vec<f64> = a.iter().flatten().map(|v| v as f64).collect();
                let stddev = compute_stddev_sample(&values);
                Arc::new(Float64Array::from(vec![stddev]))
            } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                let values: Vec<f64> = a.iter().flatten().collect();
                let stddev = compute_stddev_sample(&values);
                Arc::new(Float64Array::from(vec![stddev]))
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "STDDEV not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::StddevPop => {
            if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let values: Vec<f64> = a.iter().flatten().map(|v| v as f64).collect();
                let stddev = compute_stddev_pop(&values);
                Arc::new(Float64Array::from(vec![stddev]))
            } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                let values: Vec<f64> = a.iter().flatten().collect();
                let stddev = compute_stddev_pop(&values);
                Arc::new(Float64Array::from(vec![stddev]))
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "STDDEV_POP not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::Variance | AggregateFunction::VarSamp => {
            if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let values: Vec<f64> = a.iter().flatten().map(|v| v as f64).collect();
                let var = compute_variance_sample(&values);
                Arc::new(Float64Array::from(vec![var]))
            } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                let values: Vec<f64> = a.iter().flatten().collect();
                let var = compute_variance_sample(&values);
                Arc::new(Float64Array::from(vec![var]))
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "VARIANCE not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::VarPop => {
            if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let values: Vec<f64> = a.iter().flatten().map(|v| v as f64).collect();
                let var = compute_variance_pop(&values);
                Arc::new(Float64Array::from(vec![var]))
            } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                let values: Vec<f64> = a.iter().flatten().collect();
                let var = compute_variance_pop(&values);
                Arc::new(Float64Array::from(vec![var]))
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "VAR_POP not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::BoolAnd => {
            if let Some(a) = input.as_any().downcast_ref::<BooleanArray>() {
                let result = a.iter().flatten().all(|v| v);
                Arc::new(BooleanArray::from(vec![result]))
            } else {
                return Err(QueryError::Type(
                    "BOOL_AND requires boolean argument".into(),
                ));
            }
        }
        AggregateFunction::BoolOr => {
            if let Some(a) = input.as_any().downcast_ref::<BooleanArray>() {
                let result = a.iter().flatten().any(|v| v);
                Arc::new(BooleanArray::from(vec![result]))
            } else {
                return Err(QueryError::Type("BOOL_OR requires boolean argument".into()));
            }
        }
        // New aggregate functions
        AggregateFunction::CountIf => {
            if let Some(a) = input.as_any().downcast_ref::<BooleanArray>() {
                let count = a.iter().flatten().filter(|v| *v).count();
                Arc::new(Int64Array::from(vec![count as i64]))
            } else {
                return Err(QueryError::Type(
                    "COUNT_IF requires boolean argument".into(),
                ));
            }
        }
        AggregateFunction::AnyValue | AggregateFunction::Arbitrary => {
            // Return first non-null value
            if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let first = a.iter().flatten().next();
                match first {
                    Some(v) => Arc::new(Int64Array::from(vec![v])),
                    None => Arc::new(Int64Array::from(vec![Option::<i64>::None])),
                }
            } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                let first = a.iter().flatten().next();
                match first {
                    Some(v) => Arc::new(Float64Array::from(vec![v])),
                    None => Arc::new(Float64Array::from(vec![Option::<f64>::None])),
                }
            } else if let Some(a) = input.as_any().downcast_ref::<StringArray>() {
                let first = a.iter().flatten().next();
                match first {
                    Some(v) => Arc::new(StringArray::from(vec![Some(v)])),
                    None => Arc::new(StringArray::from(vec![Option::<&str>::None])),
                }
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "ANY_VALUE not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::GeometricMean => {
            if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                let values: Vec<f64> = a.iter().flatten().filter(|v| *v > 0.0).collect();
                if values.is_empty() {
                    Arc::new(Float64Array::from(vec![Option::<f64>::None]))
                } else {
                    let log_sum: f64 = values.iter().map(|v| v.ln()).sum();
                    let geom_mean = (log_sum / values.len() as f64).exp();
                    Arc::new(Float64Array::from(vec![geom_mean]))
                }
            } else if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let values: Vec<f64> = a
                    .iter()
                    .flatten()
                    .map(|v| v as f64)
                    .filter(|v| *v > 0.0)
                    .collect();
                if values.is_empty() {
                    Arc::new(Float64Array::from(vec![Option::<f64>::None]))
                } else {
                    let log_sum: f64 = values.iter().map(|v| v.ln()).sum();
                    let geom_mean = (log_sum / values.len() as f64).exp();
                    Arc::new(Float64Array::from(vec![geom_mean]))
                }
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "GEOMETRIC_MEAN not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::Checksum => {
            if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let xor = a.iter().flatten().fold(0i64, |acc, v| acc ^ v);
                Arc::new(Int64Array::from(vec![xor]))
            } else if let Some(a) = input.as_any().downcast_ref::<arrow::array::Int32Array>() {
                let xor = a.iter().flatten().fold(0i64, |acc, v| acc ^ (v as i64));
                Arc::new(Int64Array::from(vec![xor]))
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "CHECKSUM not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::BitwiseAndAgg => {
            if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let result = a
                    .iter()
                    .flatten()
                    .fold(None, |acc: Option<i64>, v| Some(acc.map_or(v, |a| a & v)));
                match result {
                    Some(v) => Arc::new(Int64Array::from(vec![v])),
                    None => Arc::new(Int64Array::from(vec![Option::<i64>::None])),
                }
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "BITWISE_AND_AGG not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::BitwiseOrAgg => {
            if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let result = a
                    .iter()
                    .flatten()
                    .fold(None, |acc: Option<i64>, v| Some(acc.map_or(v, |a| a | v)));
                match result {
                    Some(v) => Arc::new(Int64Array::from(vec![v])),
                    None => Arc::new(Int64Array::from(vec![Option::<i64>::None])),
                }
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "BITWISE_OR_AGG not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::BitwiseXorAgg => {
            if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let xor = a.iter().flatten().fold(0i64, |acc, v| acc ^ v);
                Arc::new(Int64Array::from(vec![xor]))
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "BITWISE_XOR_AGG not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::Listagg => {
            if let Some(a) = input.as_any().downcast_ref::<StringArray>() {
                let joined: String = a.iter().flatten().collect::<Vec<_>>().join(",");
                Arc::new(StringArray::from(vec![Some(joined)]))
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "LISTAGG not implemented for type {:?}",
                    input.data_type()
                )));
            }
        }
        AggregateFunction::Kurtosis => {
            let values: Vec<f64> = if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                a.iter().flatten().collect()
            } else if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                a.iter().flatten().map(|v| v as f64).collect()
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "KURTOSIS not implemented for type {:?}",
                    input.data_type()
                )));
            };
            if values.len() < 4 {
                Arc::new(Float64Array::from(vec![Option::<f64>::None]))
            } else {
                let n = values.len() as f64;
                let mean = values.iter().sum::<f64>() / n;
                let variance = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
                if variance > 0.0 {
                    let m4 = values.iter().map(|x| (x - mean).powi(4)).sum::<f64>() / n;
                    let kurtosis = m4 / variance.powi(2) - 3.0;
                    Arc::new(Float64Array::from(vec![kurtosis]))
                } else {
                    Arc::new(Float64Array::from(vec![Option::<f64>::None]))
                }
            }
        }
        AggregateFunction::Skewness => {
            let values: Vec<f64> = if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                a.iter().flatten().collect()
            } else if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                a.iter().flatten().map(|v| v as f64).collect()
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "SKEWNESS not implemented for type {:?}",
                    input.data_type()
                )));
            };
            if values.len() < 3 {
                Arc::new(Float64Array::from(vec![Option::<f64>::None]))
            } else {
                let n = values.len() as f64;
                let mean = values.iter().sum::<f64>() / n;
                let variance = values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n;
                if variance > 0.0 {
                    let std_dev = variance.sqrt();
                    let m3 = values.iter().map(|x| (x - mean).powi(3)).sum::<f64>() / n;
                    let skewness = m3 / std_dev.powi(3);
                    Arc::new(Float64Array::from(vec![skewness]))
                } else {
                    Arc::new(Float64Array::from(vec![Option::<f64>::None]))
                }
            }
        }
        AggregateFunction::ApproxPercentile => {
            // Extract percentile from second argument (default to 0.5 for median)
            let percentile = if let Some(ref second_arg) = aggregate.second_arg {
                match second_arg {
                    Expr::Literal(ScalarValue::Float64(p)) => (*p).into(),
                    Expr::Literal(ScalarValue::Int64(p)) => *p as f64,
                    _ => 0.5, // Default to median
                }
            } else {
                0.5
            };

            let values: Vec<f64> = if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                a.iter().flatten().collect()
            } else if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                a.iter().flatten().map(|v| v as f64).collect()
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "APPROX_PERCENTILE not implemented for type {:?}",
                    input.data_type()
                )));
            };
            if values.is_empty() {
                Arc::new(Float64Array::from(vec![Option::<f64>::None]))
            } else {
                let mut sorted = values;
                sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                // Use the percentile value to calculate index
                let idx = ((sorted.len() - 1) as f64 * percentile).round() as usize;
                let idx = idx.min(sorted.len() - 1); // Clamp to valid range
                Arc::new(Float64Array::from(vec![sorted[idx]]))
            }
        }
        AggregateFunction::ApproxDistinct => {
            // For SIMD path, just use exact count distinct
            let count = if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                let set: std::collections::HashSet<_> = a.iter().flatten().collect();
                set.len() as i64
            } else if let Some(a) = input.as_any().downcast_ref::<StringArray>() {
                let set: std::collections::HashSet<_> = a.iter().flatten().collect();
                set.len() as i64
            } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                let set: std::collections::HashSet<_> = a
                    .iter()
                    .flatten()
                    .map(|v| ordered_float::OrderedFloat(v))
                    .collect();
                set.len() as i64
            } else {
                return Err(QueryError::NotImplemented(format!(
                    "APPROX_DISTINCT not implemented for type {:?}",
                    input.data_type()
                )));
            };
            Arc::new(Int64Array::from(vec![count]))
        }
        // Two-argument aggregates - not supported in SIMD path
        AggregateFunction::Corr
        | AggregateFunction::CovarPop
        | AggregateFunction::CovarSamp
        | AggregateFunction::RegrSlope
        | AggregateFunction::RegrIntercept
        | AggregateFunction::RegrCount
        | AggregateFunction::RegrAvgx
        | AggregateFunction::RegrAvgy
        | AggregateFunction::MaxBy
        | AggregateFunction::MinBy => {
            return Err(QueryError::NotImplemented(format!(
                "{:?} requires two arguments and is not supported in SIMD aggregation",
                aggregate.func
            )));
        }
    };

    RecordBatch::try_new(schema.clone(), vec![result]).map_err(Into::into)
}

// Helper functions for statistical calculations
fn compute_variance_pop(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / n
}

fn compute_variance_sample(values: &[f64]) -> f64 {
    if values.len() < 2 {
        return 0.0;
    }
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    values.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0)
}

fn compute_stddev_pop(values: &[f64]) -> f64 {
    compute_variance_pop(values).sqrt()
}

fn compute_stddev_sample(values: &[f64]) -> f64 {
    compute_variance_sample(values).sqrt()
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
                aggregates
                    .iter()
                    .map(|agg| {
                        let mut state = AccumulatorState::default();
                        // Set percentile for APPROX_PERCENTILE from second_arg
                        if agg.func == AggregateFunction::ApproxPercentile {
                            if let Some(ref second_arg) = agg.second_arg {
                                state.percentile = match second_arg {
                                    Expr::Literal(ScalarValue::Float64(p)) => (*p).into(),
                                    Expr::Literal(ScalarValue::Int64(p)) => *p as f64,
                                    _ => 0.5,
                                };
                            }
                        }
                        state
                    })
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
    if (batches.iter().all(|b| b.num_rows() == 0) || (batches.is_empty() && group_by.is_empty()))
        && group_by.is_empty()
    {
        // Return a single row with default aggregate values
        groups.insert(
            GroupKey { values: vec![] },
            aggregates
                .iter()
                .map(|agg| {
                    let mut state = AccumulatorState::default();
                    // Set percentile for APPROX_PERCENTILE from second_arg
                    if agg.func == AggregateFunction::ApproxPercentile {
                        if let Some(ref second_arg) = agg.second_arg {
                            state.percentile = match second_arg {
                                Expr::Literal(ScalarValue::Float64(p)) => (*p).into(),
                                Expr::Literal(ScalarValue::Int64(p)) => *p as f64,
                                _ => 0.5,
                            };
                        }
                    }
                    state
                })
                .collect(),
        );
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
        // Statistical aggregates - we track sum and sum_squares for online calculation
        AggregateFunction::Stddev
        | AggregateFunction::StddevPop
        | AggregateFunction::StddevSamp
        | AggregateFunction::Variance
        | AggregateFunction::VarPop
        | AggregateFunction::VarSamp => {
            if !input.is_null(row) {
                let val = if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                    a.value(row) as f64
                } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                    a.value(row)
                } else if let Some(a) = input.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    a.value(row) as f64
                } else {
                    return;
                };
                state.count += 1;
                state.sum += val;
                state.sum_squares += val * val;
            }
        }
        AggregateFunction::BoolAnd => {
            if !input.is_null(row) {
                if let Some(a) = input.as_any().downcast_ref::<BooleanArray>() {
                    let val = a.value(row);
                    state.bool_and = Some(state.bool_and.map_or(val, |v| v && val));
                }
            }
        }
        AggregateFunction::BoolOr => {
            if !input.is_null(row) {
                if let Some(a) = input.as_any().downcast_ref::<BooleanArray>() {
                    let val = a.value(row);
                    state.bool_or = Some(state.bool_or.map_or(val, |v| v || val));
                }
            }
        }
        // New aggregate functions
        AggregateFunction::CountIf => {
            if !input.is_null(row) {
                if let Some(a) = input.as_any().downcast_ref::<BooleanArray>() {
                    if a.value(row) {
                        state.count_if += 1;
                    }
                }
            }
        }
        AggregateFunction::AnyValue | AggregateFunction::Arbitrary => {
            // Take the first non-null value
            if !input.is_null(row) && state.any_value.is_none() {
                state.any_value = Some(extract_group_value(input, row));
            }
        }
        AggregateFunction::GeometricMean => {
            if !input.is_null(row) {
                let val = if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                    a.value(row) as f64
                } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                    a.value(row)
                } else if let Some(a) = input.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    a.value(row) as f64
                } else {
                    return;
                };
                if val > 0.0 {
                    state.log_sum += val.ln();
                    state.log_count += 1;
                }
            }
        }
        AggregateFunction::Checksum => {
            // Simple XOR-based checksum
            if !input.is_null(row) {
                if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                    state.bitwise_xor ^= a.value(row);
                } else if let Some(a) = input.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    state.bitwise_xor ^= a.value(row) as i64;
                }
            }
        }
        AggregateFunction::BitwiseAndAgg => {
            if !input.is_null(row) {
                if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                    let val = a.value(row);
                    state.bitwise_and = Some(state.bitwise_and.map_or(val, |v| v & val));
                } else if let Some(a) = input.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    let val = a.value(row) as i64;
                    state.bitwise_and = Some(state.bitwise_and.map_or(val, |v| v & val));
                }
            }
        }
        AggregateFunction::BitwiseOrAgg => {
            if !input.is_null(row) {
                if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                    let val = a.value(row);
                    state.bitwise_or = Some(state.bitwise_or.map_or(val, |v| v | val));
                } else if let Some(a) = input.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    let val = a.value(row) as i64;
                    state.bitwise_or = Some(state.bitwise_or.map_or(val, |v| v | val));
                }
            }
        }
        AggregateFunction::BitwiseXorAgg => {
            if !input.is_null(row) {
                if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                    state.bitwise_xor ^= a.value(row);
                } else if let Some(a) = input.as_any().downcast_ref::<arrow::array::Int32Array>() {
                    state.bitwise_xor ^= a.value(row) as i64;
                }
            }
        }
        AggregateFunction::Listagg => {
            if !input.is_null(row) {
                if let Some(a) = input.as_any().downcast_ref::<arrow::array::StringArray>() {
                    state.string_list.push(a.value(row).to_string());
                }
            }
        }
        // Correlation/covariance - these need two inputs but we only get one here
        // They need special handling with multiple inputs
        AggregateFunction::Corr
        | AggregateFunction::CovarPop
        | AggregateFunction::CovarSamp
        | AggregateFunction::RegrSlope
        | AggregateFunction::RegrIntercept
        | AggregateFunction::RegrCount
        | AggregateFunction::RegrAvgx
        | AggregateFunction::RegrAvgy => {
            // These require special handling for two-argument aggregates
            // For now, just count
            if !input.is_null(row) {
                state.count += 1;
            }
        }
        AggregateFunction::Kurtosis | AggregateFunction::Skewness => {
            // Track higher moments - would need running mean calculation
            if !input.is_null(row) {
                let val = if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                    a.value(row) as f64
                } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                    a.value(row)
                } else {
                    return;
                };
                state.count += 1;
                state.sum += val;
                state.sum_squares += val * val;
                state.sum_cubes += val * val * val;
                state.sum_fourth += val * val * val * val;
            }
        }
        AggregateFunction::ApproxPercentile => {
            if !input.is_null(row) {
                let val = if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                    a.value(row) as f64
                } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                    a.value(row)
                } else {
                    return;
                };
                state.approx_values.push(val);
            }
        }
        AggregateFunction::ApproxDistinct => {
            // Use distinct set for approximate distinct
            if !input.is_null(row) {
                let value = extract_group_value(input, row);
                let set = state
                    .distinct_set
                    .get_or_insert_with(std::collections::HashSet::new);
                set.insert(value);
            }
        }
        AggregateFunction::MaxBy | AggregateFunction::MinBy => {
            // These need special handling with two inputs
            // For single input, just track values
            if !input.is_null(row) {
                let val = if let Some(a) = input.as_any().downcast_ref::<Int64Array>() {
                    a.value(row) as f64
                } else if let Some(a) = input.as_any().downcast_ref::<Float64Array>() {
                    a.value(row)
                } else {
                    return;
                };
                state.approx_values.push(val);
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
        // Statistical aggregates
        (AggregateFunction::Variance | AggregateFunction::VarSamp, DataType::Float64) => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for states in groups.values() {
                let state = &states[agg_idx];
                if state.count >= 2 {
                    let n = state.count as f64;
                    let mean = state.sum / n;
                    let variance = (state.sum_squares / n) - (mean * mean);
                    // Bessel's correction for sample variance
                    let sample_variance = variance * n / (n - 1.0);
                    builder.append_value(sample_variance);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::VarPop, DataType::Float64) => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for states in groups.values() {
                let state = &states[agg_idx];
                if state.count > 0 {
                    let n = state.count as f64;
                    let mean = state.sum / n;
                    let variance = (state.sum_squares / n) - (mean * mean);
                    builder.append_value(variance);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Stddev | AggregateFunction::StddevSamp, DataType::Float64) => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for states in groups.values() {
                let state = &states[agg_idx];
                if state.count >= 2 {
                    let n = state.count as f64;
                    let mean = state.sum / n;
                    let variance = (state.sum_squares / n) - (mean * mean);
                    // Bessel's correction for sample stddev
                    let sample_variance = variance * n / (n - 1.0);
                    builder.append_value(sample_variance.sqrt());
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::StddevPop, DataType::Float64) => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for states in groups.values() {
                let state = &states[agg_idx];
                if state.count > 0 {
                    let n = state.count as f64;
                    let mean = state.sum / n;
                    let variance = (state.sum_squares / n) - (mean * mean);
                    builder.append_value(variance.sqrt());
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        // Boolean aggregates
        (AggregateFunction::BoolAnd, DataType::Boolean) => {
            let mut builder = arrow::array::BooleanBuilder::with_capacity(num_groups);
            for states in groups.values() {
                match states[agg_idx].bool_and {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::BoolOr, DataType::Boolean) => {
            let mut builder = arrow::array::BooleanBuilder::with_capacity(num_groups);
            for states in groups.values() {
                match states[agg_idx].bool_or {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        // New aggregate functions
        (AggregateFunction::CountIf, DataType::Int64) => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for states in groups.values() {
                builder.append_value(states[agg_idx].count_if);
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::AnyValue | AggregateFunction::Arbitrary, _) => {
            // Build array from any_value
            match data_type {
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(num_groups);
                    for states in groups.values() {
                        match &states[agg_idx].any_value {
                            Some(GroupValue::Int64(v)) => builder.append_value(*v),
                            _ => builder.append_null(),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(num_groups);
                    for states in groups.values() {
                        match &states[agg_idx].any_value {
                            Some(GroupValue::Float64(v)) => builder.append_value(v.0),
                            _ => builder.append_null(),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                DataType::Utf8 => {
                    let mut builder = StringBuilder::with_capacity(num_groups, num_groups * 16);
                    for states in groups.values() {
                        match &states[agg_idx].any_value {
                            Some(GroupValue::String(v)) => builder.append_value(v),
                            _ => builder.append_null(),
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                _ => Err(QueryError::NotImplemented(format!(
                    "ANY_VALUE with type {:?} not supported",
                    data_type
                ))),
            }
        }
        (AggregateFunction::GeometricMean, DataType::Float64) => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for states in groups.values() {
                let state = &states[agg_idx];
                if state.log_count > 0 {
                    let geom_mean = (state.log_sum / state.log_count as f64).exp();
                    builder.append_value(geom_mean);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Checksum, DataType::Int64) => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for states in groups.values() {
                builder.append_value(states[agg_idx].bitwise_xor);
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::BitwiseAndAgg, DataType::Int64) => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for states in groups.values() {
                match states[agg_idx].bitwise_and {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::BitwiseOrAgg, DataType::Int64) => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for states in groups.values() {
                match states[agg_idx].bitwise_or {
                    Some(v) => builder.append_value(v),
                    None => builder.append_null(),
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::BitwiseXorAgg, DataType::Int64) => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for states in groups.values() {
                builder.append_value(states[agg_idx].bitwise_xor);
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Listagg, DataType::Utf8) => {
            let mut builder = StringBuilder::with_capacity(num_groups, num_groups * 64);
            for states in groups.values() {
                let joined = states[agg_idx].string_list.join(",");
                builder.append_value(&joined);
            }
            Ok(Arc::new(builder.finish()))
        }
        // Approximate percentile
        (AggregateFunction::ApproxPercentile, DataType::Float64) => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for states in groups.values() {
                let state = &states[agg_idx];
                if !state.approx_values.is_empty() {
                    let mut sorted = state.approx_values.clone();
                    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                    // Use the percentile value from state
                    let idx = ((sorted.len() - 1) as f64 * state.percentile).round() as usize;
                    let idx = idx.min(sorted.len() - 1); // Clamp to valid range
                    builder.append_value(sorted[idx]);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::ApproxDistinct, DataType::Int64) => {
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
        // Kurtosis and skewness
        (AggregateFunction::Kurtosis, DataType::Float64) => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for states in groups.values() {
                let state = &states[agg_idx];
                if state.count >= 4 {
                    let n = state.count as f64;
                    let mean = state.sum / n;
                    let variance = (state.sum_squares / n) - (mean * mean);
                    if variance > 0.0 {
                        let _std_dev = variance.sqrt();
                        // Excess kurtosis
                        let m4 = state.sum_fourth / n - 4.0 * mean * state.sum_cubes / n
                            + 6.0 * mean * mean * state.sum_squares / n
                            - 3.0 * mean.powi(4);
                        let kurtosis = m4 / variance.powi(2) - 3.0;
                        builder.append_value(kurtosis);
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::Skewness, DataType::Float64) => {
            let mut builder = Float64Builder::with_capacity(num_groups);
            for states in groups.values() {
                let state = &states[agg_idx];
                if state.count >= 3 {
                    let n = state.count as f64;
                    let mean = state.sum / n;
                    let variance = (state.sum_squares / n) - (mean * mean);
                    if variance > 0.0 {
                        let m3 = state.sum_cubes / n - 3.0 * mean * state.sum_squares / n
                            + 2.0 * mean.powi(3);
                        let skewness = m3 / variance.sqrt().powi(3);
                        builder.append_value(skewness);
                    } else {
                        builder.append_null();
                    }
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        // Correlation and regression - placeholder implementations
        (AggregateFunction::Corr, DataType::Float64)
        | (AggregateFunction::CovarPop, DataType::Float64)
        | (AggregateFunction::CovarSamp, DataType::Float64)
        | (AggregateFunction::RegrSlope, DataType::Float64)
        | (AggregateFunction::RegrIntercept, DataType::Float64)
        | (AggregateFunction::RegrAvgx, DataType::Float64)
        | (AggregateFunction::RegrAvgy, DataType::Float64) => {
            // These require two-argument handling
            let mut builder = Float64Builder::with_capacity(num_groups);
            for states in groups.values() {
                let state = &states[agg_idx];
                if state.count > 0 {
                    // Placeholder - just return average for now
                    builder.append_value(state.sum / state.count as f64);
                } else {
                    builder.append_null();
                }
            }
            Ok(Arc::new(builder.finish()))
        }
        (AggregateFunction::RegrCount, DataType::Int64) => {
            let mut builder = Int64Builder::with_capacity(num_groups);
            for states in groups.values() {
                builder.append_value(states[agg_idx].count);
            }
            Ok(Arc::new(builder.finish()))
        }
        // MaxBy and MinBy placeholders
        (AggregateFunction::MaxBy | AggregateFunction::MinBy, _) => {
            // These require special two-argument handling
            match data_type {
                DataType::Float64 => {
                    let mut builder = Float64Builder::with_capacity(num_groups);
                    for states in groups.values() {
                        let vals = &states[agg_idx].approx_values;
                        if !vals.is_empty() {
                            let val = if func == AggregateFunction::MaxBy {
                                vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
                            } else {
                                vals.iter().cloned().fold(f64::INFINITY, f64::min)
                            };
                            builder.append_value(val);
                        } else {
                            builder.append_null();
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::with_capacity(num_groups);
                    for states in groups.values() {
                        let vals = &states[agg_idx].approx_values;
                        if !vals.is_empty() {
                            let val = if func == AggregateFunction::MaxBy {
                                vals.iter().cloned().fold(f64::NEG_INFINITY, f64::max)
                            } else {
                                vals.iter().cloned().fold(f64::INFINITY, f64::min)
                            };
                            builder.append_value(val as i64);
                        } else {
                            builder.append_null();
                        }
                    }
                    Ok(Arc::new(builder.finish()))
                }
                _ => Err(QueryError::NotImplemented(format!(
                    "MAX_BY/MIN_BY with type {:?} not supported",
                    data_type
                ))),
            }
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
            second_arg: None,
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
            second_arg: None,
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
                second_arg: None,
            },
            AggregateExpr {
                func: AggregateFunction::Count,
                input: Expr::column("value"),
                distinct: false,
                second_arg: None,
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
