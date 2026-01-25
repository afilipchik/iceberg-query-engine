//! Filter operator

use crate::error::{QueryError, Result};
use crate::physical::operators::subquery::{evaluate_subquery_expr, SubqueryExecutor};
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::{BinaryOp, Column, Expr, ScalarValue, UnaryOp};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Datum, Float32Array, Int32Array, Int64Array,
    StringArray, Time64MicrosecondArray, TimestampMicrosecondArray,
};
use arrow::compute;
use arrow::compute::kernels::boolean;
use arrow::compute::kernels::cmp;
use arrow::compute::kernels::numeric;
use arrow::datatypes::{DataType, IntervalDayTime, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_select::zip::zip;
use async_trait::async_trait;
use futures::stream::TryStreamExt;
use std::fmt;
use std::sync::Arc;

/// Filter execution operator
pub struct FilterExec {
    input: Arc<dyn PhysicalOperator>,
    predicate: Expr,
    schema: SchemaRef,
    /// Optional subquery executor for handling subqueries in predicates
    subquery_executor: Option<SubqueryExecutor>,
}

impl fmt::Debug for FilterExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FilterExec")
            .field("predicate", &self.predicate)
            .field("has_subquery_executor", &self.subquery_executor.is_some())
            .finish()
    }
}

impl FilterExec {
    pub fn new(input: Arc<dyn PhysicalOperator>, predicate: Expr) -> Self {
        let schema = input.schema();
        Self {
            input,
            predicate,
            schema,
            subquery_executor: None,
        }
    }

    /// Set the subquery executor for this filter
    pub fn with_subquery_executor(mut self, executor: SubqueryExecutor) -> Self {
        self.subquery_executor = Some(executor);
        self
    }

    /// Get a reference to the subquery executor (for nested filters)
    pub fn subquery_executor(&self) -> Option<&SubqueryExecutor> {
        self.subquery_executor.as_ref()
    }
}

#[async_trait]
impl PhysicalOperator for FilterExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.input.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        let input_stream = self.input.execute(partition).await?;
        let predicate = self.predicate.clone();
        let schema = self.schema.clone();
        let has_subqueries = predicate.contains_subquery();
        let subquery_exec = self.subquery_executor.clone();

        let filtered_stream = input_stream.and_then(move |batch| {
            let pred = predicate.clone();
            let schema = schema.clone();
            let subquery_exec = subquery_exec.clone();
            async move {
                if has_subqueries {
                    if let Some(exec) = subquery_exec {
                        evaluate_filter_with_subquery(&batch, &pred, &schema, &exec)
                    } else {
                        Err(QueryError::Execution(
                            "Subquery in filter but no executor available".into(),
                        ))
                    }
                } else {
                    evaluate_filter(&batch, &pred, &schema)
                }
            }
        });

        Ok(Box::pin(filtered_stream))
    }

    fn name(&self) -> &str {
        "Filter"
    }

    fn output_partitions(&self) -> usize {
        // Propagate partitions from input - filter preserves partitioning
        self.input.output_partitions()
    }
}

impl fmt::Display for FilterExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Filter: {}", self.predicate)
    }
}

/// Evaluate a filter predicate on a batch
fn evaluate_filter(
    batch: &RecordBatch,
    predicate: &Expr,
    _schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mask = evaluate_expr_internal(batch, predicate, None)?;

    let boolean_array = mask
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| QueryError::Execution("Filter predicate must evaluate to boolean".into()))?;

    let filtered_columns: Result<Vec<ArrayRef>> = batch
        .columns()
        .iter()
        .map(|col| compute::filter(col.as_ref(), boolean_array).map_err(Into::into))
        .collect();

    RecordBatch::try_new(batch.schema(), filtered_columns?).map_err(Into::into)
}

/// Evaluate a filter predicate on a batch with subquery support
fn evaluate_filter_with_subquery(
    batch: &RecordBatch,
    predicate: &Expr,
    _schema: &SchemaRef,
    executor: &SubqueryExecutor,
) -> Result<RecordBatch> {
    let mask = evaluate_expr_internal(batch, predicate, Some(executor))?;

    let boolean_array = mask
        .as_any()
        .downcast_ref::<BooleanArray>()
        .ok_or_else(|| QueryError::Execution("Filter predicate must evaluate to boolean".into()))?;

    let filtered_columns: Result<Vec<ArrayRef>> = batch
        .columns()
        .iter()
        .map(|col| compute::filter(col.as_ref(), boolean_array).map_err(Into::into))
        .collect();

    RecordBatch::try_new(batch.schema(), filtered_columns?).map_err(Into::into)
}

/// Evaluate an expression on a batch (public API, no subquery support)
pub fn evaluate_expr(batch: &RecordBatch, expr: &Expr) -> Result<ArrayRef> {
    evaluate_expr_internal(batch, expr, None)
}

/// Internal expression evaluation with optional subquery support
fn evaluate_expr_internal(
    batch: &RecordBatch,
    expr: &Expr,
    subquery_executor: Option<&SubqueryExecutor>,
) -> Result<ArrayRef> {
    match expr {
        Expr::Column(col) => {
            let idx = find_column_index(batch, col)?;
            Ok(batch.column(idx).clone())
        }

        Expr::Literal(value) => Ok(scalar_to_array(value, batch.num_rows())),

        Expr::BinaryExpr { left, op, right } => {
            let left_arr = evaluate_expr_internal(batch, left, subquery_executor)?;
            let right_arr = evaluate_expr_internal(batch, right, subquery_executor)?;
            evaluate_binary_op(&left_arr, *op, &right_arr)
        }

        Expr::UnaryExpr { op, expr } => {
            let arr = evaluate_expr_internal(batch, expr, subquery_executor)?;
            evaluate_unary_op(*op, &arr)
        }

        Expr::Cast { expr, data_type } => {
            let arr = evaluate_expr_internal(batch, expr, subquery_executor)?;
            arrow::compute::cast(&arr, data_type).map_err(Into::into)
        }

        Expr::Alias { expr, .. } => evaluate_expr_internal(batch, expr, subquery_executor),

        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => evaluate_case(
            batch,
            operand.as_deref(),
            when_then,
            else_expr.as_deref(),
            subquery_executor,
        ),

        Expr::InList {
            expr,
            list,
            negated,
        } => {
            let value = evaluate_expr_internal(batch, expr, subquery_executor)?;
            let list_values: Result<Vec<ArrayRef>> = list
                .iter()
                .map(|e| evaluate_expr_internal(batch, e, subquery_executor))
                .collect();
            let list_values = list_values?;
            evaluate_in_list(&value, &list_values, *negated)
        }

        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => {
            let value = evaluate_expr_internal(batch, expr, subquery_executor)?;
            let low_val = evaluate_expr_internal(batch, low, subquery_executor)?;
            let high_val = evaluate_expr_internal(batch, high, subquery_executor)?;

            let ge_low = evaluate_binary_op(&value, BinaryOp::GtEq, &low_val)?;
            let le_high = evaluate_binary_op(&value, BinaryOp::LtEq, &high_val)?;

            let ge_low_bool = ge_low.as_any().downcast_ref::<BooleanArray>().unwrap();
            let le_high_bool = le_high.as_any().downcast_ref::<BooleanArray>().unwrap();
            let result = boolean::and(ge_low_bool, le_high_bool)?;

            if *negated {
                Ok(Arc::new(boolean::not(&result)?))
            } else {
                Ok(Arc::new(result))
            }
        }

        Expr::ScalarFunc { func, args } => {
            evaluate_scalar_func(batch, func, args, subquery_executor)
        }

        Expr::Wildcard => {
            // For COUNT(*), return an array of 1s to count
            let arr = Int64Array::from(vec![1i64; batch.num_rows()]);
            Ok(Arc::new(arr))
        }

        Expr::QualifiedWildcard(_) => {
            // Same as Wildcard
            let arr = Int64Array::from(vec![1i64; batch.num_rows()]);
            Ok(Arc::new(arr))
        }

        Expr::ScalarSubquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => {
            // Handle subquery expressions
            if let Some(exec) = subquery_executor {
                evaluate_subquery_expr(batch, expr, exec)
            } else {
                Err(QueryError::Execution(
                    "Subquery expression but no executor available".into(),
                ))
            }
        }

        _ => Err(QueryError::NotImplemented(format!(
            "Expression not supported in filter: {:?}",
            expr
        ))),
    }
}

fn find_column_index(batch: &RecordBatch, col: &Column) -> Result<usize> {
    let schema = batch.schema();

    // Try qualified name first (e.g., "n1.n_nationkey")
    if let Some(relation) = &col.relation {
        let qualified = format!("{}.{}", relation, col.name);
        if let Ok(idx) = schema.index_of(&qualified) {
            return Ok(idx);
        }
    }

    // Try unqualified name exactly (e.g., "n_nationkey")
    if let Ok(idx) = schema.index_of(&col.name) {
        return Ok(idx);
    }

    // Try to find a field that ends with ".{column_name}" (for unqualified lookups on qualified schema)
    let suffix = format!(".{}", col.name);
    for (i, field) in schema.fields().iter().enumerate() {
        if field.name().ends_with(&suffix) || field.name() == &col.name {
            return Ok(i);
        }
    }

    Err(QueryError::ColumnNotFound(col.qualified_name()))
}

fn scalar_to_array(value: &ScalarValue, num_rows: usize) -> ArrayRef {
    use arrow::array::*;

    match value {
        ScalarValue::Null => Arc::new(NullArray::new(num_rows)),
        ScalarValue::Boolean(v) => Arc::new(BooleanArray::from(vec![*v; num_rows])),
        ScalarValue::Int8(v) => Arc::new(Int8Array::from(vec![*v; num_rows])),
        ScalarValue::Int16(v) => Arc::new(Int16Array::from(vec![*v; num_rows])),
        ScalarValue::Int32(v) => Arc::new(Int32Array::from(vec![*v; num_rows])),
        ScalarValue::Int64(v) => Arc::new(Int64Array::from(vec![*v; num_rows])),
        ScalarValue::UInt8(v) => Arc::new(UInt8Array::from(vec![*v; num_rows])),
        ScalarValue::UInt16(v) => Arc::new(UInt16Array::from(vec![*v; num_rows])),
        ScalarValue::UInt32(v) => Arc::new(UInt32Array::from(vec![*v; num_rows])),
        ScalarValue::UInt64(v) => Arc::new(UInt64Array::from(vec![*v; num_rows])),
        ScalarValue::Float32(v) => Arc::new(Float32Array::from(vec![v.0; num_rows])),
        ScalarValue::Float64(v) => Arc::new(Float64Array::from(vec![v.0; num_rows])),
        ScalarValue::Utf8(v) => Arc::new(StringArray::from(vec![v.as_str(); num_rows])),
        ScalarValue::Date32(v) => Arc::new(Date32Array::from(vec![*v; num_rows])),
        ScalarValue::Date64(v) => Arc::new(arrow::array::Date64Array::from(vec![*v; num_rows])),
        ScalarValue::Timestamp(v) => Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![
                *v;
                num_rows
            ])),
        ScalarValue::Decimal128(d) => {
            let scaled = d.mantissa();
            Arc::new(
                arrow::array::Decimal128Array::from(vec![scaled; num_rows])
                    .with_precision_and_scale(38, 10)
                    .unwrap(),
            )
        }
        ScalarValue::Interval(v) => {
            let interval = IntervalDayTime::new(*v as i32, (*v >> 32) as i32);
            Arc::new(arrow::array::IntervalDayTimeArray::from(vec![
                interval;
                num_rows
            ]))
        }
        ScalarValue::List(values, elem_type) => {
            // Convert list to JSON string representation for now
            let json_arr: Vec<serde_json::Value> = values
                .iter()
                .map(|v| match v {
                    ScalarValue::Null => serde_json::Value::Null,
                    ScalarValue::Boolean(b) => serde_json::Value::Bool(*b),
                    ScalarValue::Int8(i) => serde_json::json!(*i),
                    ScalarValue::Int16(i) => serde_json::json!(*i),
                    ScalarValue::Int32(i) => serde_json::json!(*i),
                    ScalarValue::Int64(i) => serde_json::json!(*i),
                    ScalarValue::UInt8(i) => serde_json::json!(*i),
                    ScalarValue::UInt16(i) => serde_json::json!(*i),
                    ScalarValue::UInt32(i) => serde_json::json!(*i),
                    ScalarValue::UInt64(i) => serde_json::json!(*i),
                    ScalarValue::Float32(f) => serde_json::json!(f.0),
                    ScalarValue::Float64(f) => serde_json::json!(f.0),
                    ScalarValue::Utf8(s) => serde_json::Value::String(s.clone()),
                    _ => serde_json::Value::Null,
                })
                .collect();
            let json_str = serde_json::to_string(&json_arr).unwrap_or_else(|_| "[]".to_string());
            Arc::new(StringArray::from(vec![json_str.as_str(); num_rows]))
        }
    }
}

fn evaluate_binary_op(left: &ArrayRef, op: BinaryOp, right: &ArrayRef) -> Result<ArrayRef> {
    // Handle type coercion
    let (left, right) = coerce_arrays(left, right)?;

    match op {
        BinaryOp::Eq => compare_arrays(&left, &right, |l, r| cmp::eq(l, r)),
        BinaryOp::NotEq => compare_arrays(&left, &right, |l, r| cmp::neq(l, r)),
        BinaryOp::Lt => compare_arrays(&left, &right, |l, r| cmp::lt(l, r)),
        BinaryOp::LtEq => compare_arrays(&left, &right, |l, r| cmp::lt_eq(l, r)),
        BinaryOp::Gt => compare_arrays(&left, &right, |l, r| cmp::gt(l, r)),
        BinaryOp::GtEq => compare_arrays(&left, &right, |l, r| cmp::gt_eq(l, r)),
        BinaryOp::And => {
            let l = left
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Type("AND requires boolean operands".into()))?;
            let r = right
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Type("AND requires boolean operands".into()))?;
            Ok(Arc::new(boolean::and(l, r)?))
        }
        BinaryOp::Or => {
            let l = left
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Type("OR requires boolean operands".into()))?;
            let r = right
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Type("OR requires boolean operands".into()))?;
            Ok(Arc::new(boolean::or(l, r)?))
        }
        BinaryOp::Add => arithmetic_op(&left, &right, |a, b| numeric::add(a, b)),
        BinaryOp::Subtract => arithmetic_op(&left, &right, |a, b| numeric::sub(a, b)),
        BinaryOp::Multiply => arithmetic_op(&left, &right, |a, b| numeric::mul(a, b)),
        BinaryOp::Divide => arithmetic_op(&left, &right, |a, b| numeric::div(a, b)),
        BinaryOp::Modulo => arithmetic_op(&left, &right, |a, b| numeric::rem(a, b)),
        BinaryOp::Like => {
            let l = left
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("LIKE requires string operands".into()))?;
            let r = right
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("LIKE requires string operands".into()))?;
            // Implement proper LIKE pattern matching with % and _
            let result: BooleanArray = (0..l.len())
                .map(|i| {
                    if l.is_null(i) || r.is_null(i) {
                        None
                    } else {
                        Some(like_match(l.value(i), r.value(i)))
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }
        BinaryOp::NotLike => {
            let l = left
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("NOT LIKE requires string operands".into()))?;
            let r = right
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("NOT LIKE requires string operands".into()))?;
            // Implement NOT LIKE as negation of LIKE
            let result: BooleanArray = (0..l.len())
                .map(|i| {
                    if l.is_null(i) || r.is_null(i) {
                        None
                    } else {
                        Some(!like_match(l.value(i), r.value(i)))
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }
        BinaryOp::StringConcat => {
            let l = left
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("|| requires string operands".into()))?;
            let r = right
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("|| requires string operands".into()))?;
            // Manually concatenate strings
            let result: StringArray = (0..l.len())
                .map(|i| {
                    if l.is_null(i) || r.is_null(i) {
                        None
                    } else {
                        Some(format!("{}{}", l.value(i), r.value(i)))
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }
    }
}

fn coerce_arrays(left: &ArrayRef, right: &ArrayRef) -> Result<(ArrayRef, ArrayRef)> {
    let left_type = left.data_type();
    let right_type = right.data_type();

    if left_type == right_type {
        return Ok((left.clone(), right.clone()));
    }

    // Numeric type coercion
    let common_type = coerce_numeric_types(left_type, right_type)?;

    let left = if left_type != &common_type {
        compute::cast(left, &common_type)?
    } else {
        left.clone()
    };

    let right = if right_type != &common_type {
        compute::cast(right, &common_type)?
    } else {
        right.clone()
    };

    Ok((left, right))
}

fn coerce_numeric_types(left: &DataType, right: &DataType) -> Result<DataType> {
    use DataType::*;

    match (left, right) {
        // Same types
        (a, b) if a == b => Ok(a.clone()),

        // Float64 dominates
        (Float64, _) | (_, Float64) => Ok(Float64),
        (Float32, _) | (_, Float32) => Ok(Float64),

        // Int64 for integers
        (Int64, _) | (_, Int64) => Ok(Int64),
        (Int32, _) | (_, Int32) => Ok(Int64),
        (Int16, _) | (_, Int16) => Ok(Int32),
        (Int8, _) | (_, Int8) => Ok(Int16),

        // UInt64 for unsigned
        (UInt64, _) | (_, UInt64) => Ok(UInt64),
        (UInt32, _) | (_, UInt32) => Ok(UInt64),

        // Date/String coercion
        (Date32, Utf8) | (Utf8, Date32) => Ok(Date32),

        // Default to string
        (Utf8, _) | (_, Utf8) => Ok(Utf8),

        _ => Err(QueryError::Type(format!(
            "Cannot coerce {:?} and {:?}",
            left, right
        ))),
    }
}

fn compare_arrays<F>(left: &ArrayRef, right: &ArrayRef, f: F) -> Result<ArrayRef>
where
    F: Fn(&dyn Datum, &dyn Datum) -> std::result::Result<BooleanArray, arrow::error::ArrowError>,
{
    let result = f(left, right)?;
    Ok(Arc::new(result))
}

fn arithmetic_op<F>(left: &ArrayRef, right: &ArrayRef, f: F) -> Result<ArrayRef>
where
    F: Fn(&dyn Datum, &dyn Datum) -> std::result::Result<ArrayRef, arrow::error::ArrowError>,
{
    f(left, right).map_err(Into::into)
}

fn evaluate_unary_op(op: UnaryOp, arr: &ArrayRef) -> Result<ArrayRef> {
    match op {
        UnaryOp::Not => {
            let bool_arr = arr
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Type("NOT requires boolean operand".into()))?;
            Ok(Arc::new(boolean::not(bool_arr)?))
        }
        UnaryOp::Negate => Ok(Arc::new(numeric::neg(arr.as_ref())?)),
        UnaryOp::IsNull => Ok(Arc::new(compute::is_null(arr.as_ref())?)),
        UnaryOp::IsNotNull => Ok(Arc::new(compute::is_not_null(arr.as_ref())?)),
    }
}

fn evaluate_case(
    batch: &RecordBatch,
    _operand: Option<&Expr>,
    when_then: &[(Expr, Expr)],
    else_expr: Option<&Expr>,
    subquery_executor: Option<&SubqueryExecutor>,
) -> Result<ArrayRef> {
    let num_rows = batch.num_rows();

    // Start with else value or null
    let mut result: Option<ArrayRef> = else_expr
        .map(|e| evaluate_expr_internal(batch, e, subquery_executor))
        .transpose()?;

    // Process WHEN clauses in reverse order
    for (when, then) in when_then.iter().rev() {
        let condition = evaluate_expr_internal(batch, when, subquery_executor)?;
        let condition = condition
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| QueryError::Type("CASE WHEN requires boolean condition".into()))?;

        let then_value = evaluate_expr_internal(batch, then, subquery_executor)?;

        result = Some(match result {
            Some(else_val) => {
                // Use zip to select between then_value and else_val based on condition
                zip(condition, &then_value, &else_val)?
            }
            None => {
                // No else, use null for false conditions
                let null_arr = arrow::array::new_null_array(then_value.data_type(), num_rows);
                zip(condition, &then_value, &null_arr)?
            }
        });
    }

    result.ok_or_else(|| QueryError::Execution("CASE must have at least one WHEN clause".into()))
}

fn evaluate_in_list(value: &ArrayRef, list: &[ArrayRef], negated: bool) -> Result<ArrayRef> {
    if list.is_empty() {
        // Empty list - no matches
        let result = BooleanArray::from(vec![negated; value.len()]);
        return Ok(Arc::new(result));
    }

    // Compare with each list value and OR the results
    let mut result: Option<BooleanArray> = None;

    for list_val in list {
        let eq_result = evaluate_binary_op(value, BinaryOp::Eq, list_val)?;
        let eq_bool = eq_result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| QueryError::Type("IN comparison must return boolean".into()))?;

        result = Some(match result {
            Some(prev) => boolean::or(&prev, eq_bool)?,
            None => eq_bool.clone(),
        });
    }

    let result = result.unwrap();

    if negated {
        Ok(Arc::new(boolean::not(&result)?))
    } else {
        Ok(Arc::new(result))
    }
}

fn evaluate_scalar_func(
    batch: &RecordBatch,
    func: &crate::planner::ScalarFunction,
    args: &[Expr],
    subquery_executor: Option<&SubqueryExecutor>,
) -> Result<ArrayRef> {
    use crate::planner::ScalarFunction;
    use arrow::array::{BinaryArray, Float64Array};

    let evaluated_args: Result<Vec<ArrayRef>> = args
        .iter()
        .map(|a| evaluate_expr_internal(batch, a, subquery_executor))
        .collect();
    let evaluated_args = evaluated_args?;

    match func {
        ScalarFunction::Upper => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("UPPER requires 1 argument".into()))?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("UPPER requires string argument".into()))?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| opt.map(|s| s.to_uppercase()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Lower => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("LOWER requires 1 argument".into()))?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("LOWER requires string argument".into()))?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| opt.map(|s| s.to_lowercase()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Length => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("LENGTH requires 1 argument".into()))?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("LENGTH requires string argument".into()))?;

            let result: Int64Array = str_arr
                .iter()
                .map(|opt| opt.map(|s| s.len() as i64))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Substring => {
            if evaluated_args.len() < 2 {
                return Err(QueryError::InvalidArgument(
                    "SUBSTRING requires at least 2 arguments".into(),
                ));
            }

            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("SUBSTRING requires string argument".into()))?;

            let start_arr = &evaluated_args[1];
            let len_arr = evaluated_args.get(2);

            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    let s = str_arr.value(i);
                    let start = get_int_value(start_arr, i).unwrap_or(1) as usize;
                    let start = start.saturating_sub(1); // SQL is 1-indexed

                    match len_arr {
                        Some(len) => {
                            let len = get_int_value(len, i).unwrap_or(s.len() as i64) as usize;
                            Some(s.chars().skip(start).take(len).collect::<String>())
                        }
                        None => Some(s.chars().skip(start).collect::<String>()),
                    }
                })
                .collect();

            Ok(Arc::new(result))
        }

        ScalarFunction::Coalesce => {
            if evaluated_args.is_empty() {
                return Err(QueryError::InvalidArgument(
                    "COALESCE requires at least 1 argument".into(),
                ));
            }

            // Find the first non-null data type
            let target_type = evaluated_args
                .iter()
                .map(|arr| arr.data_type().clone())
                .find(|dt| *dt != DataType::Null)
                .unwrap_or(DataType::Null);

            // If all arguments are null type, return the first one
            if target_type == DataType::Null {
                return Ok(evaluated_args[0].clone());
            }

            // Convert all arguments to the target type
            let num_rows = evaluated_args[0].len();
            let converted_args: Vec<ArrayRef> = evaluated_args
                .iter()
                .map(|arr| {
                    if arr.data_type() == &DataType::Null {
                        // Create a null array of the target type
                        arrow::array::new_null_array(&target_type, num_rows)
                    } else {
                        arr.clone()
                    }
                })
                .collect();

            // Start with the first argument
            let mut result = converted_args.first().unwrap().clone();

            // Work forwards, replacing nulls with subsequent values
            for arr in converted_args.iter().skip(1) {
                let is_null = compute::is_null(result.as_ref())?;
                result = zip(&is_null, arr, &result)?;
            }

            Ok(result)
        }

        ScalarFunction::Extract => {
            // EXTRACT(field FROM date)
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "EXTRACT requires 2 arguments".into(),
                ));
            }

            let field = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("EXTRACT field must be string".into()))?;

            let date_arr = &evaluated_args[1];

            if let Some(date32) = date_arr.as_any().downcast_ref::<Date32Array>() {
                let field_name = field.value(0).to_uppercase();
                let result: Int32Array = date32
                    .iter()
                    .map(|opt| {
                        opt.map(|days| {
                            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                                .unwrap_or_default();
                            match field_name.as_str() {
                                "YEAR" => date.year(),
                                "MONTH" => date.month() as i32,
                                "DAY" => date.day() as i32,
                                _ => 0,
                            }
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            Err(QueryError::NotImplemented(
                "EXTRACT for this type not implemented".into(),
            ))
        }

        ScalarFunction::Year | ScalarFunction::Month | ScalarFunction::Day => {
            let date_arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("Date function requires 1 argument".into())
            })?;

            if let Some(date32) = date_arr.as_any().downcast_ref::<Date32Array>() {
                use chrono::Datelike;
                let result: Int32Array = date32
                    .iter()
                    .map(|opt| {
                        opt.map(|days| {
                            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                                .unwrap_or_default();
                            match func {
                                ScalarFunction::Year => date.year(),
                                ScalarFunction::Month => date.month() as i32,
                                ScalarFunction::Day => date.day() as i32,
                                _ => 0,
                            }
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            Err(QueryError::NotImplemented(
                "Date function for this type not implemented".into(),
            ))
        }

        ScalarFunction::Abs => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("ABS requires 1 argument".into()))?;
            apply_math_unary_preserve_int(arr, |x| x.abs(), |x| x.abs())
        }

        ScalarFunction::Ceil => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("CEIL requires 1 argument".into()))?;
            // For integers, ceil is identity; for floats, use ceil
            apply_math_unary_preserve_int(arr, |x| x.ceil(), |x| x)
        }

        ScalarFunction::Floor => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("FLOOR requires 1 argument".into()))?;
            // For integers, floor is identity; for floats, use floor
            apply_math_unary_preserve_int(arr, |x| x.floor(), |x| x)
        }

        ScalarFunction::Round => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("ROUND requires 1 argument".into()))?;
            // For integers, round is identity; for floats, use round
            apply_math_unary_preserve_int(arr, |x| x.round(), |x| x)
        }

        ScalarFunction::Power => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "POWER requires 2 arguments".into(),
                ));
            }
            apply_math_binary(&evaluated_args[0], &evaluated_args[1], |a, b| a.powf(b))
        }

        ScalarFunction::Sqrt => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("SQRT requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.sqrt())
        }

        ScalarFunction::Trim => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("TRIM requires 1 argument".into()))?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("TRIM requires string argument".into()))?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| opt.map(|s| s.trim().to_string()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Ltrim => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("LTRIM requires 1 argument".into()))?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("LTRIM requires string argument".into()))?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| opt.map(|s| s.trim_start().to_string()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Rtrim => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("RTRIM requires 1 argument".into()))?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("RTRIM requires string argument".into()))?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| opt.map(|s| s.trim_end().to_string()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Replace => {
            if evaluated_args.len() != 3 {
                return Err(QueryError::InvalidArgument(
                    "REPLACE requires 3 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("REPLACE requires string argument".into()))?;
            let from_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("REPLACE requires string argument for pattern".into())
                })?;
            let to_arr = evaluated_args[2]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("REPLACE requires string argument for replacement".into())
                })?;

            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || from_arr.is_null(i) || to_arr.is_null(i) {
                        None
                    } else {
                        Some(str_arr.value(i).replace(from_arr.value(i), to_arr.value(i)))
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::NullIf => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "NULLIF requires 2 arguments".into(),
                ));
            }
            // NULLIF(a, b) returns NULL if a = b, otherwise returns a
            let eq_result =
                evaluate_binary_op(&evaluated_args[0], BinaryOp::Eq, &evaluated_args[1])?;
            let is_equal = eq_result
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Type("NULLIF comparison must return boolean".into()))?;

            // Create null array of same type
            let null_arr = arrow::array::new_null_array(
                evaluated_args[0].data_type(),
                evaluated_args[0].len(),
            );
            // Use zip: if equal, return null; otherwise return first arg
            Ok(zip(is_equal, &null_arr, &evaluated_args[0])?)
        }

        ScalarFunction::Concat => {
            if evaluated_args.is_empty() {
                return Err(QueryError::InvalidArgument(
                    "CONCAT requires at least 1 argument".into(),
                ));
            }
            let num_rows = evaluated_args[0].len();
            let result: StringArray = (0..num_rows)
                .map(|i| {
                    let mut s = String::new();
                    for arr in &evaluated_args {
                        if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                            if !str_arr.is_null(i) {
                                s.push_str(str_arr.value(i));
                            }
                        }
                    }
                    Some(s)
                })
                .collect();
            Ok(Arc::new(result))
        }

        // ========== NEW MATH FUNCTIONS ==========
        ScalarFunction::Mod => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "MOD requires 2 arguments".into(),
                ));
            }
            arithmetic_op(&evaluated_args[0], &evaluated_args[1], |a, b| {
                arrow::compute::kernels::numeric::rem(a, b)
            })
        }

        ScalarFunction::Sign => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("SIGN requires 1 argument".into()))?;

            if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>() {
                let result: Int32Array = float_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|v| {
                            if v > 0.0 {
                                1
                            } else if v < 0.0 {
                                -1
                            } else {
                                0
                            }
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                let result: Int32Array = int_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|v| {
                            if v > 0 {
                                1
                            } else if v < 0 {
                                -1
                            } else {
                                0
                            }
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type("SIGN requires numeric argument".into()))
        }

        ScalarFunction::Truncate => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("TRUNCATE requires 1 argument".into())
            })?;
            apply_math_unary(arr, |x| x.trunc())
        }

        ScalarFunction::Ln => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("LN requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.ln())
        }

        ScalarFunction::Log => {
            if evaluated_args.is_empty() {
                return Err(QueryError::InvalidArgument(
                    "LOG requires at least 1 argument".into(),
                ));
            }
            if evaluated_args.len() == 1 {
                // LOG(x) = natural log
                apply_math_unary(&evaluated_args[0], |x| x.ln())
            } else {
                // LOG(base, x)
                apply_math_binary(&evaluated_args[0], &evaluated_args[1], |base, x| {
                    x.log(base)
                })
            }
        }

        ScalarFunction::Log2 => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("LOG2 requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.log2())
        }

        ScalarFunction::Log10 => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("LOG10 requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.log10())
        }

        ScalarFunction::Exp => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("EXP requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.exp())
        }

        ScalarFunction::Random => {
            use rand::Rng;
            let num_rows = batch.num_rows();
            let mut rng = rand::thread_rng();
            let values: Vec<f64> = (0..num_rows).map(|_| rng.gen::<f64>()).collect();
            Ok(Arc::new(Float64Array::from(values)))
        }

        ScalarFunction::Sin => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("SIN requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.sin())
        }

        ScalarFunction::Cos => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("COS requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.cos())
        }

        ScalarFunction::Tan => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("TAN requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.tan())
        }

        ScalarFunction::Asin => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("ASIN requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.asin())
        }

        ScalarFunction::Acos => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("ACOS requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.acos())
        }

        ScalarFunction::Atan => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("ATAN requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.atan())
        }

        ScalarFunction::Atan2 => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "ATAN2 requires 2 arguments".into(),
                ));
            }
            apply_math_binary(&evaluated_args[0], &evaluated_args[1], |y, x| y.atan2(x))
        }

        ScalarFunction::Degrees => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("DEGREES requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.to_degrees())
        }

        ScalarFunction::Radians => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("RADIANS requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.to_radians())
        }

        ScalarFunction::Pi => {
            let num_rows = batch.num_rows();
            Ok(Arc::new(Float64Array::from(vec![
                std::f64::consts::PI;
                num_rows
            ])))
        }

        ScalarFunction::E => {
            let num_rows = batch.num_rows();
            Ok(Arc::new(Float64Array::from(vec![
                std::f64::consts::E;
                num_rows
            ])))
        }

        ScalarFunction::Cbrt => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("CBRT requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.cbrt())
        }

        // ========== NEW STRING FUNCTIONS ==========
        ScalarFunction::Position | ScalarFunction::Strpos => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "POSITION/STRPOS requires 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("POSITION/STRPOS requires string arguments".into())
                })?;
            let substr_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("POSITION/STRPOS requires string arguments".into())
                })?;

            let result: Int64Array = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || substr_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let sub = substr_arr.value(i);
                        Some(s.find(sub).map(|pos| pos as i64 + 1).unwrap_or(0))
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Reverse => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("REVERSE requires 1 argument".into()))?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("REVERSE requires string argument".into()))?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| opt.map(|s| s.chars().rev().collect::<String>()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Lpad => {
            if evaluated_args.len() < 2 {
                return Err(QueryError::InvalidArgument(
                    "LPAD requires at least 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("LPAD requires string argument".into()))?;
            let len_arr = &evaluated_args[1];
            let pad_char = if evaluated_args.len() > 2 {
                if let Some(pad_arr) = evaluated_args[2].as_any().downcast_ref::<StringArray>() {
                    pad_arr.value(0).to_string()
                } else {
                    " ".to_string()
                }
            } else {
                " ".to_string()
            };

            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let target_len = get_int_value(len_arr, i).unwrap_or(0) as usize;
                        let current_len = s.chars().count();
                        if current_len >= target_len {
                            Some(s.chars().take(target_len).collect::<String>())
                        } else {
                            let padding_needed = target_len - current_len;
                            let pad_chars: String =
                                pad_char.chars().cycle().take(padding_needed).collect();
                            Some(format!("{}{}", pad_chars, s))
                        }
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Rpad => {
            if evaluated_args.len() < 2 {
                return Err(QueryError::InvalidArgument(
                    "RPAD requires at least 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("RPAD requires string argument".into()))?;
            let len_arr = &evaluated_args[1];
            let pad_char = if evaluated_args.len() > 2 {
                if let Some(pad_arr) = evaluated_args[2].as_any().downcast_ref::<StringArray>() {
                    pad_arr.value(0).to_string()
                } else {
                    " ".to_string()
                }
            } else {
                " ".to_string()
            };

            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let target_len = get_int_value(len_arr, i).unwrap_or(0) as usize;
                        let current_len = s.chars().count();
                        if current_len >= target_len {
                            Some(s.chars().take(target_len).collect::<String>())
                        } else {
                            let padding_needed = target_len - current_len;
                            let pad_chars: String =
                                pad_char.chars().cycle().take(padding_needed).collect();
                            Some(format!("{}{}", s, pad_chars))
                        }
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::SplitPart => {
            if evaluated_args.len() != 3 {
                return Err(QueryError::InvalidArgument(
                    "SPLIT_PART requires 3 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("SPLIT_PART requires string argument".into()))?;
            let delim_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("SPLIT_PART requires string delimiter".into()))?;
            let idx_arr = &evaluated_args[2];

            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || delim_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let delim = delim_arr.value(i);
                        let idx = get_int_value(idx_arr, i).unwrap_or(1) as usize;
                        let parts: Vec<&str> = s.split(delim).collect();
                        if idx > 0 && idx <= parts.len() {
                            Some(parts[idx - 1].to_string())
                        } else {
                            Some(String::new())
                        }
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::StartsWith => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "STARTS_WITH requires 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("STARTS_WITH requires string argument".into()))?;
            let prefix_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("STARTS_WITH requires string prefix".into()))?;

            let result: BooleanArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || prefix_arr.is_null(i) {
                        None
                    } else {
                        Some(str_arr.value(i).starts_with(prefix_arr.value(i)))
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::EndsWith => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "ENDS_WITH requires 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("ENDS_WITH requires string argument".into()))?;
            let suffix_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("ENDS_WITH requires string suffix".into()))?;

            let result: BooleanArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || suffix_arr.is_null(i) {
                        None
                    } else {
                        Some(str_arr.value(i).ends_with(suffix_arr.value(i)))
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Chr => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("CHR requires 1 argument".into()))?;

            let result: StringArray = if let Some(int_arr) =
                arr.as_any().downcast_ref::<Int64Array>()
            {
                int_arr
                    .iter()
                    .map(|opt| opt.and_then(|v| char::from_u32(v as u32).map(|c| c.to_string())))
                    .collect()
            } else if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
                int_arr
                    .iter()
                    .map(|opt| opt.and_then(|v| char::from_u32(v as u32).map(|c| c.to_string())))
                    .collect()
            } else {
                return Err(QueryError::Type("CHR requires integer argument".into()));
            };
            Ok(Arc::new(result))
        }

        ScalarFunction::Ascii => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("ASCII requires 1 argument".into()))?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("ASCII requires string argument".into()))?;

            let result: Int64Array = str_arr
                .iter()
                .map(|opt| opt.map(|s| s.chars().next().map(|c| c as i64).unwrap_or(0)))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::ConcatWs => {
            if evaluated_args.len() < 2 {
                return Err(QueryError::InvalidArgument(
                    "CONCAT_WS requires at least 2 arguments".into(),
                ));
            }
            let sep_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("CONCAT_WS requires string separator".into()))?;

            let num_rows = sep_arr.len();
            let result: StringArray = (0..num_rows)
                .map(|i| {
                    if sep_arr.is_null(i) {
                        None
                    } else {
                        let sep = sep_arr.value(i);
                        let parts: Vec<String> = evaluated_args[1..]
                            .iter()
                            .filter_map(|arr| {
                                arr.as_any().downcast_ref::<StringArray>().and_then(|sa| {
                                    if sa.is_null(i) {
                                        None
                                    } else {
                                        Some(sa.value(i).to_string())
                                    }
                                })
                            })
                            .collect();
                        Some(parts.join(sep))
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Left => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "LEFT requires 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("LEFT requires string argument".into()))?;
            let len_arr = &evaluated_args[1];

            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let n = get_int_value(len_arr, i).unwrap_or(0) as usize;
                        Some(s.chars().take(n).collect::<String>())
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Right => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "RIGHT requires 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("RIGHT requires string argument".into()))?;
            let len_arr = &evaluated_args[1];

            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let n = get_int_value(len_arr, i).unwrap_or(0) as usize;
                        let chars: Vec<char> = s.chars().collect();
                        let start = chars.len().saturating_sub(n);
                        Some(chars[start..].iter().collect::<String>())
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Repeat => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "REPEAT requires 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("REPEAT requires string argument".into()))?;
            let count_arr = &evaluated_args[1];

            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let n = get_int_value(count_arr, i).unwrap_or(0).max(0) as usize;
                        Some(s.repeat(n))
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        // ========== DATE/TIME FUNCTIONS ==========
        ScalarFunction::CurrentDate => {
            let num_rows = batch.num_rows();
            let today = chrono::Local::now().date_naive();
            let days = today
                .signed_duration_since(chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                .num_days() as i32;
            Ok(Arc::new(Date32Array::from(vec![days; num_rows])))
        }

        ScalarFunction::CurrentTimestamp | ScalarFunction::Now => {
            let num_rows = batch.num_rows();
            let now = chrono::Utc::now();
            let micros = now.timestamp_micros();
            Ok(Arc::new(arrow::array::TimestampMicrosecondArray::from(
                vec![micros; num_rows],
            )))
        }

        ScalarFunction::Hour => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("HOUR requires 1 argument".into()))?;

            if let Some(ts_arr) = arr
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            {
                use chrono::Timelike;
                let result: Int32Array = ts_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|micros| {
                            chrono::DateTime::from_timestamp_micros(micros)
                                .map(|dt| dt.hour() as i32)
                                .unwrap_or(0)
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::NotImplemented(
                "HOUR for this type not implemented".into(),
            ))
        }

        ScalarFunction::Minute => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("MINUTE requires 1 argument".into()))?;

            if let Some(ts_arr) = arr
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            {
                use chrono::Timelike;
                let result: Int32Array = ts_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|micros| {
                            chrono::DateTime::from_timestamp_micros(micros)
                                .map(|dt| dt.minute() as i32)
                                .unwrap_or(0)
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::NotImplemented(
                "MINUTE for this type not implemented".into(),
            ))
        }

        ScalarFunction::Second => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("SECOND requires 1 argument".into()))?;

            if let Some(ts_arr) = arr
                .as_any()
                .downcast_ref::<arrow::array::TimestampMicrosecondArray>()
            {
                use chrono::Timelike;
                let result: Int32Array = ts_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|micros| {
                            chrono::DateTime::from_timestamp_micros(micros)
                                .map(|dt| dt.second() as i32)
                                .unwrap_or(0)
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::NotImplemented(
                "SECOND for this type not implemented".into(),
            ))
        }

        ScalarFunction::Quarter => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("QUARTER requires 1 argument".into()))?;

            if let Some(date32) = arr.as_any().downcast_ref::<Date32Array>() {
                let result: Int32Array = date32
                    .iter()
                    .map(|opt| {
                        opt.map(|days| {
                            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                                .unwrap_or_default();
                            ((date.month() - 1) / 3 + 1) as i32
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::NotImplemented(
                "QUARTER for this type not implemented".into(),
            ))
        }

        ScalarFunction::Week => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("WEEK requires 1 argument".into()))?;

            if let Some(date32) = arr.as_any().downcast_ref::<Date32Array>() {
                use chrono::Datelike;
                let result: Int32Array = date32
                    .iter()
                    .map(|opt| {
                        opt.map(|days| {
                            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                                .unwrap_or_default();
                            date.iso_week().week() as i32
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::NotImplemented(
                "WEEK for this type not implemented".into(),
            ))
        }

        ScalarFunction::DayOfWeek => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("DAY_OF_WEEK requires 1 argument".into())
            })?;

            if let Some(date32) = arr.as_any().downcast_ref::<Date32Array>() {
                use chrono::Datelike;
                let result: Int32Array = date32
                    .iter()
                    .map(|opt| {
                        opt.map(|days| {
                            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                                .unwrap_or_default();
                            date.weekday().num_days_from_sunday() as i32 + 1
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::NotImplemented(
                "DAY_OF_WEEK for this type not implemented".into(),
            ))
        }

        ScalarFunction::DayOfYear => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("DAY_OF_YEAR requires 1 argument".into())
            })?;

            if let Some(date32) = arr.as_any().downcast_ref::<Date32Array>() {
                use chrono::Datelike;
                let result: Int32Array = date32
                    .iter()
                    .map(|opt| {
                        opt.map(|days| {
                            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                                .unwrap_or_default();
                            date.ordinal() as i32
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::NotImplemented(
                "DAY_OF_YEAR for this type not implemented".into(),
            ))
        }

        ScalarFunction::DateAdd => {
            // DATE_ADD(unit, value, date/timestamp)
            // In Trino: date_add(unit, value, timestamp) -> timestamp
            if evaluated_args.len() != 3 {
                return Err(QueryError::InvalidArgument(
                    "DATE_ADD requires 3 arguments (unit, value, date/timestamp)".into(),
                ));
            }
            let unit_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("DATE_ADD unit must be string".into()))?;
            let value_arr = &evaluated_args[1];
            let date_arr = &evaluated_args[2];

            // Handle Date32 input
            if let Some(date32_arr) = date_arr.as_any().downcast_ref::<Date32Array>() {
                use chrono::{Datelike, Duration, Months, NaiveDate};
                let result: Date32Array = (0..date32_arr.len())
                    .map(|i| {
                        if date32_arr.is_null(i) || unit_arr.is_null(i) {
                            return None;
                        }
                        let days = date32_arr.value(i);
                        let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)?;
                        let unit = unit_arr.value(i).to_lowercase();
                        let value = get_int_value(value_arr, i)? as i64;

                        let new_date = match unit.as_str() {
                            "day" | "days" => date.checked_add_signed(Duration::days(value))?,
                            "week" | "weeks" => date.checked_add_signed(Duration::weeks(value))?,
                            "month" | "months" => {
                                if value >= 0 {
                                    date.checked_add_months(Months::new(value as u32))?
                                } else {
                                    date.checked_sub_months(Months::new((-value) as u32))?
                                }
                            }
                            "year" | "years" => {
                                let months = value * 12;
                                if months >= 0 {
                                    date.checked_add_months(Months::new(months as u32))?
                                } else {
                                    date.checked_sub_months(Months::new((-months) as u32))?
                                }
                            }
                            _ => return None,
                        };
                        let days_since_epoch = new_date
                            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                            .num_days() as i32;
                        Some(days_since_epoch)
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            // Handle Timestamp input
            if let Some(ts_arr) = date_arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
            {
                use chrono::{Duration, Months, TimeZone, Utc};
                let result: TimestampMicrosecondArray = (0..ts_arr.len())
                    .map(|i| {
                        if ts_arr.is_null(i) || unit_arr.is_null(i) {
                            return None;
                        }
                        let micros = ts_arr.value(i);
                        let dt = Utc.timestamp_micros(micros).single()?;
                        let unit = unit_arr.value(i).to_lowercase();
                        let value = get_int_value(value_arr, i)? as i64;

                        let new_dt = match unit.as_str() {
                            "second" | "seconds" => {
                                dt.checked_add_signed(Duration::seconds(value))?
                            }
                            "minute" | "minutes" => {
                                dt.checked_add_signed(Duration::minutes(value))?
                            }
                            "hour" | "hours" => dt.checked_add_signed(Duration::hours(value))?,
                            "day" | "days" => dt.checked_add_signed(Duration::days(value))?,
                            "week" | "weeks" => dt.checked_add_signed(Duration::weeks(value))?,
                            "month" | "months" => {
                                if value >= 0 {
                                    dt.checked_add_months(Months::new(value as u32))?
                                } else {
                                    dt.checked_sub_months(Months::new((-value) as u32))?
                                }
                            }
                            "year" | "years" => {
                                let months = value * 12;
                                if months >= 0 {
                                    dt.checked_add_months(Months::new(months as u32))?
                                } else {
                                    dt.checked_sub_months(Months::new((-months) as u32))?
                                }
                            }
                            _ => return None,
                        };
                        Some(new_dt.timestamp_micros())
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            Err(QueryError::Type(
                "DATE_ADD requires date or timestamp argument".into(),
            ))
        }

        ScalarFunction::DateDiff => {
            // DATE_DIFF(unit, date1, date2) -> bigint
            // Returns date2 - date1 in the specified unit
            if evaluated_args.len() != 3 {
                return Err(QueryError::InvalidArgument(
                    "DATE_DIFF requires 3 arguments (unit, date1, date2)".into(),
                ));
            }
            let unit_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("DATE_DIFF unit must be string".into()))?;
            let date1_arr = &evaluated_args[1];
            let date2_arr = &evaluated_args[2];

            // Handle Date32 inputs
            if let (Some(d1_arr), Some(d2_arr)) = (
                date1_arr.as_any().downcast_ref::<Date32Array>(),
                date2_arr.as_any().downcast_ref::<Date32Array>(),
            ) {
                use chrono::{Datelike, NaiveDate};
                let result: Int64Array = (0..d1_arr.len())
                    .map(|i| {
                        if d1_arr.is_null(i) || d2_arr.is_null(i) || unit_arr.is_null(i) {
                            return None;
                        }
                        let days1 = d1_arr.value(i);
                        let days2 = d2_arr.value(i);
                        let date1 = NaiveDate::from_num_days_from_ce_opt(days1 + 719163)?;
                        let date2 = NaiveDate::from_num_days_from_ce_opt(days2 + 719163)?;
                        let unit = unit_arr.value(i).to_lowercase();

                        Some(match unit.as_str() {
                            "day" | "days" => (days2 - days1) as i64,
                            "week" | "weeks" => ((days2 - days1) / 7) as i64,
                            "month" | "months" => {
                                let months1 = date1.year() * 12 + date1.month() as i32;
                                let months2 = date2.year() * 12 + date2.month() as i32;
                                (months2 - months1) as i64
                            }
                            "year" | "years" => (date2.year() - date1.year()) as i64,
                            _ => return None,
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            // Handle Timestamp inputs
            if let (Some(ts1_arr), Some(ts2_arr)) = (
                date1_arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>(),
                date2_arr
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>(),
            ) {
                use chrono::{Datelike, TimeZone, Utc};
                let result: Int64Array = (0..ts1_arr.len())
                    .map(|i| {
                        if ts1_arr.is_null(i) || ts2_arr.is_null(i) || unit_arr.is_null(i) {
                            return None;
                        }
                        let micros1 = ts1_arr.value(i);
                        let micros2 = ts2_arr.value(i);
                        let dt1 = Utc.timestamp_micros(micros1).single()?;
                        let dt2 = Utc.timestamp_micros(micros2).single()?;
                        let unit = unit_arr.value(i).to_lowercase();

                        Some(match unit.as_str() {
                            "millisecond" | "milliseconds" => (micros2 - micros1) / 1000,
                            "second" | "seconds" => (micros2 - micros1) / 1_000_000,
                            "minute" | "minutes" => (micros2 - micros1) / 60_000_000,
                            "hour" | "hours" => (micros2 - micros1) / 3_600_000_000,
                            "day" | "days" => (micros2 - micros1) / 86_400_000_000,
                            "week" | "weeks" => (micros2 - micros1) / (7 * 86_400_000_000),
                            "month" | "months" => {
                                let months1 = dt1.year() * 12 + dt1.month() as i32;
                                let months2 = dt2.year() * 12 + dt2.month() as i32;
                                (months2 - months1) as i64
                            }
                            "year" | "years" => (dt2.year() - dt1.year()) as i64,
                            _ => return None,
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            Err(QueryError::Type(
                "DATE_DIFF requires date or timestamp arguments".into(),
            ))
        }

        ScalarFunction::DateTrunc => {
            // DATE_TRUNC(unit, date/timestamp) -> same type
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "DATE_TRUNC requires 2 arguments (unit, date/timestamp)".into(),
                ));
            }
            let unit_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("DATE_TRUNC unit must be string".into()))?;
            let date_arr = &evaluated_args[1];

            // Handle Date32 input
            if let Some(date32_arr) = date_arr.as_any().downcast_ref::<Date32Array>() {
                use chrono::{Datelike, NaiveDate};
                let result: Date32Array = (0..date32_arr.len())
                    .map(|i| {
                        if date32_arr.is_null(i) || unit_arr.is_null(i) {
                            return None;
                        }
                        let days = date32_arr.value(i);
                        let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)?;
                        let unit = unit_arr.value(i).to_lowercase();

                        let truncated = match unit.as_str() {
                            "day" | "days" => date,
                            "week" | "weeks" => {
                                // Start of ISO week (Monday)
                                let weekday = date.weekday().num_days_from_monday();
                                date - chrono::Duration::days(weekday as i64)
                            }
                            "month" | "months" => {
                                NaiveDate::from_ymd_opt(date.year(), date.month(), 1)?
                            }
                            "quarter" | "quarters" => {
                                let quarter_start_month = ((date.month() - 1) / 3) * 3 + 1;
                                NaiveDate::from_ymd_opt(date.year(), quarter_start_month, 1)?
                            }
                            "year" | "years" => NaiveDate::from_ymd_opt(date.year(), 1, 1)?,
                            _ => return None,
                        };
                        let days_since_epoch = truncated
                            .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
                            .num_days() as i32;
                        Some(days_since_epoch)
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            // Handle Timestamp input
            if let Some(ts_arr) = date_arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
            {
                use chrono::{Datelike, TimeZone, Timelike, Utc};
                let result: TimestampMicrosecondArray = (0..ts_arr.len())
                    .map(|i| {
                        if ts_arr.is_null(i) || unit_arr.is_null(i) {
                            return None;
                        }
                        let micros = ts_arr.value(i);
                        let dt = Utc.timestamp_micros(micros).single()?;
                        let unit = unit_arr.value(i).to_lowercase();

                        let truncated = match unit.as_str() {
                            "second" | "seconds" => dt.with_nanosecond(0)?,
                            "minute" | "minutes" => dt.with_second(0)?.with_nanosecond(0)?,
                            "hour" | "hours" => {
                                dt.with_minute(0)?.with_second(0)?.with_nanosecond(0)?
                            }
                            "day" | "days" => dt
                                .with_hour(0)?
                                .with_minute(0)?
                                .with_second(0)?
                                .with_nanosecond(0)?,
                            "week" | "weeks" => {
                                let weekday = dt.weekday().num_days_from_monday();
                                let day_start = dt
                                    .with_hour(0)?
                                    .with_minute(0)?
                                    .with_second(0)?
                                    .with_nanosecond(0)?;
                                day_start - chrono::Duration::days(weekday as i64)
                            }
                            "month" | "months" => Utc
                                .with_ymd_and_hms(dt.year(), dt.month(), 1, 0, 0, 0)
                                .single()?,
                            "quarter" | "quarters" => {
                                let quarter_start_month = ((dt.month() - 1) / 3) * 3 + 1;
                                Utc.with_ymd_and_hms(dt.year(), quarter_start_month, 1, 0, 0, 0)
                                    .single()?
                            }
                            "year" | "years" => {
                                Utc.with_ymd_and_hms(dt.year(), 1, 1, 0, 0, 0).single()?
                            }
                            _ => return None,
                        };
                        Some(truncated.timestamp_micros())
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            Err(QueryError::Type(
                "DATE_TRUNC requires date or timestamp argument".into(),
            ))
        }

        ScalarFunction::DatePart => {
            // DATE_PART(unit, date/timestamp) -> double
            // Similar to EXTRACT but returns double
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "DATE_PART requires 2 arguments (unit, date/timestamp)".into(),
                ));
            }
            let unit_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("DATE_PART unit must be string".into()))?;
            let date_arr = &evaluated_args[1];

            // Handle Date32 input
            if let Some(date32_arr) = date_arr.as_any().downcast_ref::<Date32Array>() {
                use chrono::{Datelike, NaiveDate};
                let result: Float64Array = (0..date32_arr.len())
                    .map(|i| {
                        if date32_arr.is_null(i) || unit_arr.is_null(i) {
                            return None;
                        }
                        let days = date32_arr.value(i);
                        let date = NaiveDate::from_num_days_from_ce_opt(days + 719163)?;
                        let unit = unit_arr.value(i).to_lowercase();

                        Some(match unit.as_str() {
                            "year" | "years" => date.year() as f64,
                            "month" | "months" => date.month() as f64,
                            "day" | "days" => date.day() as f64,
                            "quarter" | "quarters" => ((date.month() - 1) / 3 + 1) as f64,
                            "week" | "weeks" => date.iso_week().week() as f64,
                            "day_of_week" | "dayofweek" | "dow" => {
                                (date.weekday().num_days_from_sunday() + 1) as f64
                            }
                            "day_of_year" | "dayofyear" | "doy" => date.ordinal() as f64,
                            _ => return None,
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            // Handle Timestamp input
            if let Some(ts_arr) = date_arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
            {
                use chrono::{Datelike, TimeZone, Timelike, Utc};
                let result: Float64Array = (0..ts_arr.len())
                    .map(|i| {
                        if ts_arr.is_null(i) || unit_arr.is_null(i) {
                            return None;
                        }
                        let micros = ts_arr.value(i);
                        let dt = Utc.timestamp_micros(micros).single()?;
                        let unit = unit_arr.value(i).to_lowercase();

                        Some(match unit.as_str() {
                            "year" | "years" => dt.year() as f64,
                            "month" | "months" => dt.month() as f64,
                            "day" | "days" => dt.day() as f64,
                            "hour" | "hours" => dt.hour() as f64,
                            "minute" | "minutes" => dt.minute() as f64,
                            "second" | "seconds" => dt.second() as f64,
                            "millisecond" | "milliseconds" => (dt.nanosecond() / 1_000_000) as f64,
                            "microsecond" | "microseconds" => (dt.nanosecond() / 1_000) as f64,
                            "quarter" | "quarters" => ((dt.month() - 1) / 3 + 1) as f64,
                            "week" | "weeks" => dt.iso_week().week() as f64,
                            "day_of_week" | "dayofweek" | "dow" => {
                                (dt.weekday().num_days_from_sunday() + 1) as f64
                            }
                            "day_of_year" | "dayofyear" | "doy" => dt.ordinal() as f64,
                            _ => return None,
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            Err(QueryError::Type(
                "DATE_PART requires date or timestamp argument".into(),
            ))
        }

        // ========== CONDITIONAL FUNCTIONS ==========
        ScalarFunction::If => {
            if evaluated_args.len() != 3 {
                return Err(QueryError::InvalidArgument(
                    "IF requires 3 arguments".into(),
                ));
            }
            let condition = evaluated_args[0]
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Type("IF condition must be boolean".into()))?;

            Ok(zip(condition, &evaluated_args[1], &evaluated_args[2])?)
        }

        ScalarFunction::Greatest => {
            if evaluated_args.is_empty() {
                return Err(QueryError::InvalidArgument(
                    "GREATEST requires at least 1 argument".into(),
                ));
            }
            let mut result = evaluated_args[0].clone();
            for arr in &evaluated_args[1..] {
                let (left, right) = coerce_arrays(&result, arr)?;
                let gt = compare_arrays(&left, &right, |l, r| cmp::gt(l, r))?;
                let gt_bool = gt
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| QueryError::Type("GREATEST comparison failed".into()))?;
                result = zip(gt_bool, &left, &right)?;
            }
            Ok(result)
        }

        ScalarFunction::Least => {
            if evaluated_args.is_empty() {
                return Err(QueryError::InvalidArgument(
                    "LEAST requires at least 1 argument".into(),
                ));
            }
            let mut result = evaluated_args[0].clone();
            for arr in &evaluated_args[1..] {
                let (left, right) = coerce_arrays(&result, arr)?;
                let lt = compare_arrays(&left, &right, |l, r| cmp::lt(l, r))?;
                let lt_bool = lt
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| QueryError::Type("LEAST comparison failed".into()))?;
                result = zip(lt_bool, &left, &right)?;
            }
            Ok(result)
        }

        // ========== REGEX FUNCTIONS ==========
        ScalarFunction::RegexpLike => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "REGEXP_LIKE requires 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("REGEXP_LIKE requires string argument".into()))?;
            let pattern_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("REGEXP_LIKE requires string pattern".into()))?;

            let result: BooleanArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || pattern_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let pattern = pattern_arr.value(i);
                        match regex::Regex::new(pattern) {
                            Ok(re) => Some(re.is_match(s)),
                            Err(_) => Some(false),
                        }
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::RegexpExtract => {
            if evaluated_args.len() < 2 {
                return Err(QueryError::InvalidArgument(
                    "REGEXP_EXTRACT requires at least 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("REGEXP_EXTRACT requires string argument".into())
                })?;
            let pattern_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("REGEXP_EXTRACT requires string pattern".into()))?;
            let group_idx = if evaluated_args.len() > 2 {
                get_int_value(&evaluated_args[2], 0).unwrap_or(0) as usize
            } else {
                0
            };

            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || pattern_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let pattern = pattern_arr.value(i);
                        match regex::Regex::new(pattern) {
                            Ok(re) => re.captures(s).and_then(|caps| {
                                caps.get(group_idx).map(|m| m.as_str().to_string())
                            }),
                            Err(_) => None,
                        }
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::RegexpReplace => {
            if evaluated_args.len() != 3 {
                return Err(QueryError::InvalidArgument(
                    "REGEXP_REPLACE requires 3 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("REGEXP_REPLACE requires string argument".into())
                })?;
            let pattern_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("REGEXP_REPLACE requires string pattern".into()))?;
            let replacement_arr = evaluated_args[2]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("REGEXP_REPLACE requires string replacement".into())
                })?;

            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || pattern_arr.is_null(i) || replacement_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let pattern = pattern_arr.value(i);
                        let replacement = replacement_arr.value(i);
                        match regex::Regex::new(pattern) {
                            Ok(re) => Some(re.replace_all(s, replacement).to_string()),
                            Err(_) => Some(s.to_string()),
                        }
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::RegexpCount => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "REGEXP_COUNT requires 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("REGEXP_COUNT requires string argument".into()))?;
            let pattern_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("REGEXP_COUNT requires string pattern".into()))?;

            let result: Int64Array = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || pattern_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let pattern = pattern_arr.value(i);
                        match regex::Regex::new(pattern) {
                            Ok(re) => Some(re.find_iter(s).count() as i64),
                            Err(_) => Some(0),
                        }
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::RegexpSplit => {
            // For simplicity, return first split part only
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "REGEXP_SPLIT requires 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("REGEXP_SPLIT requires string argument".into()))?;
            let pattern_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("REGEXP_SPLIT requires string pattern".into()))?;

            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || pattern_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let pattern = pattern_arr.value(i);
                        match regex::Regex::new(pattern) {
                            Ok(re) => {
                                let parts: Vec<&str> = re.split(s).collect();
                                Some(parts.join(","))
                            }
                            Err(_) => Some(s.to_string()),
                        }
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        // ========== BINARY/ENCODING FUNCTIONS ==========
        ScalarFunction::ToHex => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("TO_HEX requires 1 argument".into()))?;

            if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                let result: StringArray = int_arr
                    .iter()
                    .map(|opt| opt.map(|v| format!("{:x}", v)))
                    .collect();
                return Ok(Arc::new(result));
            }
            if let Some(bin_arr) = arr.as_any().downcast_ref::<BinaryArray>() {
                let result: StringArray = bin_arr.iter().map(|opt| opt.map(hex::encode)).collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type(
                "TO_HEX requires integer or binary argument".into(),
            ))
        }

        ScalarFunction::FromHex => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("FROM_HEX requires 1 argument".into())
            })?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("FROM_HEX requires string argument".into()))?;

            let result: BinaryArray = str_arr
                .iter()
                .map(|opt| opt.and_then(|s| hex::decode(s).ok()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::ToBase64 => {
            use base64::Engine;
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("TO_BASE64 requires 1 argument".into())
            })?;

            if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                let result: StringArray = str_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|s| base64::engine::general_purpose::STANDARD.encode(s.as_bytes()))
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            if let Some(bin_arr) = arr.as_any().downcast_ref::<BinaryArray>() {
                let result: StringArray = bin_arr
                    .iter()
                    .map(|opt| opt.map(|b| base64::engine::general_purpose::STANDARD.encode(b)))
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type(
                "TO_BASE64 requires string or binary argument".into(),
            ))
        }

        ScalarFunction::FromBase64 => {
            use base64::Engine;
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("FROM_BASE64 requires 1 argument".into())
            })?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("FROM_BASE64 requires string argument".into()))?;

            let result: BinaryArray = str_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| base64::engine::general_purpose::STANDARD.decode(s).ok())
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Md5 => {
            use md5::{Digest, Md5};
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("MD5 requires 1 argument".into()))?;

            if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                let result: StringArray = str_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|s| {
                            let mut hasher = Md5::new();
                            hasher.update(s.as_bytes());
                            format!("{:x}", hasher.finalize())
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type("MD5 requires string argument".into()))
        }

        ScalarFunction::Sha1 => {
            use sha1::{Digest, Sha1};
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("SHA1 requires 1 argument".into()))?;

            if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                let result: StringArray = str_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|s| {
                            let mut hasher = Sha1::new();
                            hasher.update(s.as_bytes());
                            format!("{:x}", hasher.finalize())
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type("SHA1 requires string argument".into()))
        }

        ScalarFunction::Sha256 => {
            use sha2::{Digest, Sha256};
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("SHA256 requires 1 argument".into()))?;

            if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                let result: StringArray = str_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|s| {
                            let mut hasher = Sha256::new();
                            hasher.update(s.as_bytes());
                            format!("{:x}", hasher.finalize())
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type("SHA256 requires string argument".into()))
        }

        ScalarFunction::Sha512 => {
            use sha2::{Digest, Sha512};
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("SHA512 requires 1 argument".into()))?;

            if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                let result: StringArray = str_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|s| {
                            let mut hasher = Sha512::new();
                            hasher.update(s.as_bytes());
                            format!("{:x}", hasher.finalize())
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type("SHA512 requires string argument".into()))
        }

        // ========== BITWISE FUNCTIONS ==========
        ScalarFunction::BitwiseAnd => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "BITWISE_AND requires 2 arguments".into(),
                ));
            }
            let left = evaluated_args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::Type("BITWISE_AND requires integer arguments".into()))?;
            let right = evaluated_args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::Type("BITWISE_AND requires integer arguments".into()))?;

            let result: Int64Array = left
                .iter()
                .zip(right.iter())
                .map(|(l, r)| match (l, r) {
                    (Some(lv), Some(rv)) => Some(lv & rv),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::BitwiseOr => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "BITWISE_OR requires 2 arguments".into(),
                ));
            }
            let left = evaluated_args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::Type("BITWISE_OR requires integer arguments".into()))?;
            let right = evaluated_args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::Type("BITWISE_OR requires integer arguments".into()))?;

            let result: Int64Array = left
                .iter()
                .zip(right.iter())
                .map(|(l, r)| match (l, r) {
                    (Some(lv), Some(rv)) => Some(lv | rv),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::BitwiseXor => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "BITWISE_XOR requires 2 arguments".into(),
                ));
            }
            let left = evaluated_args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::Type("BITWISE_XOR requires integer arguments".into()))?;
            let right = evaluated_args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::Type("BITWISE_XOR requires integer arguments".into()))?;

            let result: Int64Array = left
                .iter()
                .zip(right.iter())
                .map(|(l, r)| match (l, r) {
                    (Some(lv), Some(rv)) => Some(lv ^ rv),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::BitwiseNot => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("BITWISE_NOT requires 1 argument".into())
            })?;
            let int_arr = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::Type("BITWISE_NOT requires integer argument".into()))?;

            let result: Int64Array = int_arr.iter().map(|opt| opt.map(|v| !v)).collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::BitCount => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("BIT_COUNT requires 1 argument".into())
            })?;
            let int_arr = arr
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::Type("BIT_COUNT requires integer argument".into()))?;

            let result: Int64Array = int_arr
                .iter()
                .map(|opt| opt.map(|v| v.count_ones() as i64))
                .collect();
            Ok(Arc::new(result))
        }

        // ========== URL FUNCTIONS ==========
        ScalarFunction::UrlExtractHost => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("URL_EXTRACT_HOST requires 1 argument".into())
            })?;
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                QueryError::Type("URL_EXTRACT_HOST requires string argument".into())
            })?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        url::Url::parse(s)
                            .ok()
                            .and_then(|u| u.host_str().map(|h| h.to_string()))
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::UrlExtractPath => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("URL_EXTRACT_PATH requires 1 argument".into())
            })?;
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                QueryError::Type("URL_EXTRACT_PATH requires string argument".into())
            })?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| opt.and_then(|s| url::Url::parse(s).ok().map(|u| u.path().to_string())))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::UrlExtractPort => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("URL_EXTRACT_PORT requires 1 argument".into())
            })?;
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                QueryError::Type("URL_EXTRACT_PORT requires string argument".into())
            })?;

            let result: Int32Array = str_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        url::Url::parse(s)
                            .ok()
                            .and_then(|u| u.port().map(|p| p as i32))
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::UrlExtractProtocol => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("URL_EXTRACT_PROTOCOL requires 1 argument".into())
            })?;
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                QueryError::Type("URL_EXTRACT_PROTOCOL requires string argument".into())
            })?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| url::Url::parse(s).ok().map(|u| u.scheme().to_string()))
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::UrlExtractQuery => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("URL_EXTRACT_QUERY requires 1 argument".into())
            })?;
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                QueryError::Type("URL_EXTRACT_QUERY requires string argument".into())
            })?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        url::Url::parse(s)
                            .ok()
                            .and_then(|u| u.query().map(|q| q.to_string()))
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::UrlEncode => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("URL_ENCODE requires 1 argument".into())
            })?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("URL_ENCODE requires string argument".into()))?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| {
                    opt.map(|s| {
                        percent_encoding::utf8_percent_encode(s, percent_encoding::NON_ALPHANUMERIC)
                            .to_string()
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::UrlDecode => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("URL_DECODE requires 1 argument".into())
            })?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("URL_DECODE requires string argument".into()))?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| {
                    opt.map(|s| {
                        percent_encoding::percent_decode_str(s)
                            .decode_utf8_lossy()
                            .to_string()
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        // ========== OTHER FUNCTIONS ==========
        ScalarFunction::Typeof => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("TYPEOF requires 1 argument".into()))?;
            let type_name = format!("{:?}", arr.data_type());
            let num_rows = arr.len();
            Ok(Arc::new(StringArray::from(vec![
                type_name.as_str();
                num_rows
            ])))
        }

        ScalarFunction::Uuid => {
            let num_rows = batch.num_rows();
            let result: StringArray = (0..num_rows)
                .map(|_| Some(uuid::Uuid::new_v4().to_string()))
                .collect();
            Ok(Arc::new(result))
        }

        // ========== NEW MATH FUNCTIONS - TRIGONOMETRIC ==========
        ScalarFunction::Sinh => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("SINH requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.sinh())
        }

        ScalarFunction::Cosh => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("COSH requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.cosh())
        }

        ScalarFunction::Tanh => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("TANH requires 1 argument".into()))?;
            apply_math_unary(arr, |x| x.tanh())
        }

        ScalarFunction::Cot => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("COT requires 1 argument".into()))?;
            apply_math_unary(arr, |x| 1.0 / x.tan())
        }

        // ========== NEW MATH FUNCTIONS - SPECIAL VALUES ==========
        ScalarFunction::Infinity => {
            let num_rows = batch.num_rows();
            let result: Float64Array = (0..num_rows).map(|_| Some(f64::INFINITY)).collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Nan => {
            let num_rows = batch.num_rows();
            let result: Float64Array = (0..num_rows).map(|_| Some(f64::NAN)).collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::IsFinite => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("IS_FINITE requires 1 argument".into())
            })?;
            if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>() {
                let result: BooleanArray = float_arr
                    .iter()
                    .map(|opt| opt.map(|v| v.is_finite()))
                    .collect();
                return Ok(Arc::new(result));
            }
            // For integers, always finite
            let num_rows = arr.len();
            Ok(Arc::new(BooleanArray::from(vec![true; num_rows])))
        }

        ScalarFunction::IsNan => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("IS_NAN requires 1 argument".into()))?;
            if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>() {
                let result: BooleanArray = float_arr
                    .iter()
                    .map(|opt| opt.map(|v| v.is_nan()))
                    .collect();
                return Ok(Arc::new(result));
            }
            // For integers, never NaN
            let num_rows = arr.len();
            Ok(Arc::new(BooleanArray::from(vec![false; num_rows])))
        }

        ScalarFunction::IsInfinite => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("IS_INFINITE requires 1 argument".into())
            })?;
            if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>() {
                let result: BooleanArray = float_arr
                    .iter()
                    .map(|opt| opt.map(|v| v.is_infinite()))
                    .collect();
                return Ok(Arc::new(result));
            }
            // For integers, never infinite
            let num_rows = arr.len();
            Ok(Arc::new(BooleanArray::from(vec![false; num_rows])))
        }

        // ========== NEW MATH FUNCTIONS - BASE CONVERSION ==========
        ScalarFunction::FromBase => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "FROM_BASE requires 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("FROM_BASE requires string argument".into()))?;
            let radix = get_int_value(&evaluated_args[1], 0).unwrap_or(10) as u32;

            let result: Int64Array = str_arr
                .iter()
                .map(|opt| opt.and_then(|s| i64::from_str_radix(s, radix).ok()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::ToBase => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "TO_BASE requires 2 arguments".into(),
                ));
            }
            let int_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::Type("TO_BASE requires integer argument".into()))?;
            let radix = get_int_value(&evaluated_args[1], 0).unwrap_or(10) as u32;

            let result: StringArray = int_arr
                .iter()
                .map(|opt| {
                    opt.map(|v| {
                        match radix {
                            2 => format!("{:b}", v),
                            8 => format!("{:o}", v),
                            16 => format!("{:x}", v),
                            _ => format!("{}", v), // Only support common bases
                        }
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::WidthBucket => {
            if evaluated_args.len() != 4 {
                return Err(QueryError::InvalidArgument(
                    "WIDTH_BUCKET requires 4 arguments".into(),
                ));
            }
            let operand = get_float_array(&evaluated_args[0])?;
            let low = get_float_array(&evaluated_args[1])?;
            let high = get_float_array(&evaluated_args[2])?;
            let count = get_int_value(&evaluated_args[3], 0).unwrap_or(1) as f64;

            let result: Int64Array = operand
                .iter()
                .zip(low.iter())
                .zip(high.iter())
                .map(|((op, lo), hi)| match (op, lo, hi) {
                    (Some(v), Some(l), Some(h)) => {
                        if v < l {
                            Some(0)
                        } else if v >= h {
                            Some(count as i64 + 1)
                        } else {
                            Some(((v - l) / (h - l) * count).floor() as i64 + 1)
                        }
                    }
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        // ========== NEW MATH FUNCTIONS - STATISTICAL DISTRIBUTIONS ==========
        ScalarFunction::NormalCdf => {
            use statrs::distribution::{ContinuousCDF, Normal};
            if evaluated_args.len() < 3 {
                return Err(QueryError::InvalidArgument(
                    "NORMAL_CDF requires 3 arguments (mean, std, value)".into(),
                ));
            }
            let mean = get_float_array(&evaluated_args[0])?;
            let std = get_float_array(&evaluated_args[1])?;
            let value = get_float_array(&evaluated_args[2])?;

            let result: Float64Array = mean
                .iter()
                .zip(std.iter())
                .zip(value.iter())
                .map(|((m, s), v)| match (m, s, v) {
                    (Some(m), Some(s), Some(v)) => Normal::new(*m, *s).ok().map(|n| n.cdf(*v)),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::InverseNormalCdf => {
            use statrs::distribution::{ContinuousCDF, Normal};
            if evaluated_args.len() < 3 {
                return Err(QueryError::InvalidArgument(
                    "INVERSE_NORMAL_CDF requires 3 arguments (mean, std, p)".into(),
                ));
            }
            let mean = get_float_array(&evaluated_args[0])?;
            let std = get_float_array(&evaluated_args[1])?;
            let p = get_float_array(&evaluated_args[2])?;

            let result: Float64Array = mean
                .iter()
                .zip(std.iter())
                .zip(p.iter())
                .map(|((m, s), prob)| match (m, s, prob) {
                    (Some(m), Some(s), Some(p)) => {
                        Normal::new(*m, *s).ok().map(|n| n.inverse_cdf(*p))
                    }
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::BetaCdf => {
            // BETA_CDF(a, b, x) -> double
            // Cumulative distribution function of the Beta distribution
            use statrs::distribution::{Beta, ContinuousCDF};
            if evaluated_args.len() != 3 {
                return Err(QueryError::InvalidArgument(
                    "BETA_CDF requires 3 arguments (a, b, x)".into(),
                ));
            }
            let a = get_float_array(&evaluated_args[0])?;
            let b = get_float_array(&evaluated_args[1])?;
            let x = get_float_array(&evaluated_args[2])?;

            let result: Float64Array = a
                .iter()
                .zip(b.iter())
                .zip(x.iter())
                .map(|((a_opt, b_opt), x_opt)| match (a_opt, b_opt, x_opt) {
                    (Some(a), Some(b), Some(x)) => Beta::new(*a, *b).ok().map(|dist| dist.cdf(*x)),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::InverseBetaCdf => {
            // INVERSE_BETA_CDF(a, b, p) -> double
            // Inverse cumulative distribution function of the Beta distribution
            use statrs::distribution::{Beta, ContinuousCDF};
            if evaluated_args.len() != 3 {
                return Err(QueryError::InvalidArgument(
                    "INVERSE_BETA_CDF requires 3 arguments (a, b, p)".into(),
                ));
            }
            let a = get_float_array(&evaluated_args[0])?;
            let b = get_float_array(&evaluated_args[1])?;
            let p = get_float_array(&evaluated_args[2])?;

            let result: Float64Array = a
                .iter()
                .zip(b.iter())
                .zip(p.iter())
                .map(|((a_opt, b_opt), p_opt)| match (a_opt, b_opt, p_opt) {
                    (Some(a), Some(b), Some(p)) => {
                        Beta::new(*a, *b).ok().map(|dist| dist.inverse_cdf(*p))
                    }
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::TCdf => {
            // T_CDF(x, df) -> double
            // Cumulative distribution function of Student's t-distribution
            use statrs::distribution::{ContinuousCDF, StudentsT};
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "T_CDF requires 2 arguments (x, df)".into(),
                ));
            }
            let x = get_float_array(&evaluated_args[0])?;
            let df = get_float_array(&evaluated_args[1])?;

            let result: Float64Array = x
                .iter()
                .zip(df.iter())
                .map(|(x_opt, df_opt)| match (x_opt, df_opt) {
                    (Some(x), Some(df)) => {
                        StudentsT::new(0.0, 1.0, *df).ok().map(|dist| dist.cdf(*x))
                    }
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::TPdf => {
            // T_PDF(x, df) -> double
            // Probability density function of Student's t-distribution
            use statrs::distribution::{Continuous, StudentsT};
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "T_PDF requires 2 arguments (x, df)".into(),
                ));
            }
            let x = get_float_array(&evaluated_args[0])?;
            let df = get_float_array(&evaluated_args[1])?;

            let result: Float64Array = x
                .iter()
                .zip(df.iter())
                .map(|(x_opt, df_opt)| match (x_opt, df_opt) {
                    (Some(x), Some(df)) => {
                        StudentsT::new(0.0, 1.0, *df).ok().map(|dist| dist.pdf(*x))
                    }
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::WilsonIntervalLower => {
            // WILSON_INTERVAL_LOWER(successes, trials, z) -> double
            // Lower bound of Wilson score confidence interval
            if evaluated_args.len() != 3 {
                return Err(QueryError::InvalidArgument(
                    "WILSON_INTERVAL_LOWER requires 3 arguments (successes, trials, z)".into(),
                ));
            }
            let s = get_float_array(&evaluated_args[0])?;
            let n = get_float_array(&evaluated_args[1])?;
            let z = get_float_array(&evaluated_args[2])?;

            let result: Float64Array = s
                .iter()
                .zip(n.iter())
                .zip(z.iter())
                .map(|((s_opt, n_opt), z_opt)| match (s_opt, n_opt, z_opt) {
                    (Some(s), Some(n), Some(z)) if *n > 0.0 => {
                        let p = s / n;
                        let z2 = z * z;
                        let denominator = 1.0 + z2 / n;
                        let center = p + z2 / (2.0 * n);
                        let margin = z * ((p * (1.0 - p) / n + z2 / (4.0 * n * n)).sqrt());
                        Some((center - margin) / denominator)
                    }
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::WilsonIntervalUpper => {
            // WILSON_INTERVAL_UPPER(successes, trials, z) -> double
            // Upper bound of Wilson score confidence interval
            if evaluated_args.len() != 3 {
                return Err(QueryError::InvalidArgument(
                    "WILSON_INTERVAL_UPPER requires 3 arguments (successes, trials, z)".into(),
                ));
            }
            let s = get_float_array(&evaluated_args[0])?;
            let n = get_float_array(&evaluated_args[1])?;
            let z = get_float_array(&evaluated_args[2])?;

            let result: Float64Array = s
                .iter()
                .zip(n.iter())
                .zip(z.iter())
                .map(|((s_opt, n_opt), z_opt)| match (s_opt, n_opt, z_opt) {
                    (Some(s), Some(n), Some(z)) if *n > 0.0 => {
                        let p = s / n;
                        let z2 = z * z;
                        let denominator = 1.0 + z2 / n;
                        let center = p + z2 / (2.0 * n);
                        let margin = z * ((p * (1.0 - p) / n + z2 / (4.0 * n * n)).sqrt());
                        Some((center + margin) / denominator)
                    }
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        // ========== NEW MATH FUNCTIONS - VECTOR OPERATIONS ==========
        ScalarFunction::CosineSimilarity | ScalarFunction::CosineDistance => Err(
            QueryError::NotImplemented("Vector operations require array type support".into()),
        ),

        // ========== NEW STRING FUNCTIONS ==========
        ScalarFunction::Split => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "SPLIT requires 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("SPLIT requires string argument".into()))?;
            let delim_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("SPLIT requires string delimiter".into()))?;

            // Return comma-separated result until array type is supported
            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || delim_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let d = delim_arr.value(i);
                        let parts: Vec<&str> = s.split(d).collect();
                        Some(parts.join(","))
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Codepoint => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("CODEPOINT requires 1 argument".into())
            })?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("CODEPOINT requires string argument".into()))?;

            let result: Int64Array = str_arr
                .iter()
                .map(|opt| opt.and_then(|s| s.chars().next().map(|c| c as i64)))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::HammingDistance => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "HAMMING_DISTANCE requires 2 arguments".into(),
                ));
            }
            let str1 = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("HAMMING_DISTANCE requires string arguments".into())
                })?;
            let str2 = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("HAMMING_DISTANCE requires string arguments".into())
                })?;

            let result: Int64Array = (0..str1.len())
                .map(|i| {
                    if str1.is_null(i) || str2.is_null(i) {
                        None
                    } else {
                        let s1 = str1.value(i);
                        let s2 = str2.value(i);
                        if s1.len() != s2.len() {
                            None
                        } else {
                            Some(
                                s1.chars()
                                    .zip(s2.chars())
                                    .filter(|(c1, c2)| c1 != c2)
                                    .count() as i64,
                            )
                        }
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::LevenshteinDistance => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "LEVENSHTEIN_DISTANCE requires 2 arguments".into(),
                ));
            }
            let str1 = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("LEVENSHTEIN_DISTANCE requires string arguments".into())
                })?;
            let str2 = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("LEVENSHTEIN_DISTANCE requires string arguments".into())
                })?;

            let result: Int64Array = (0..str1.len())
                .map(|i| {
                    if str1.is_null(i) || str2.is_null(i) {
                        None
                    } else {
                        let s1 = str1.value(i);
                        let s2 = str2.value(i);
                        Some(levenshtein_distance(s1, s2) as i64)
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Soundex => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("SOUNDEX requires 1 argument".into()))?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("SOUNDEX requires string argument".into()))?;

            let result: StringArray = str_arr.iter().map(|opt| opt.map(|s| soundex(s))).collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Translate => {
            if evaluated_args.len() != 3 {
                return Err(QueryError::InvalidArgument(
                    "TRANSLATE requires 3 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("TRANSLATE requires string argument".into()))?;
            let from_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("TRANSLATE requires string from argument".into())
                })?;
            let to_arr = evaluated_args[2]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("TRANSLATE requires string to argument".into()))?;

            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || from_arr.is_null(i) || to_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let from: Vec<char> = from_arr.value(i).chars().collect();
                        let to: Vec<char> = to_arr.value(i).chars().collect();
                        Some(
                            s.chars()
                                .map(|c| {
                                    from.iter()
                                        .position(|&fc| fc == c)
                                        .and_then(|pos| to.get(pos).copied())
                                        .unwrap_or(c)
                                })
                                .collect::<String>(),
                        )
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::LuhnCheck => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("LUHN_CHECK requires 1 argument".into())
            })?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("LUHN_CHECK requires string argument".into()))?;

            let result: BooleanArray = str_arr
                .iter()
                .map(|opt| opt.map(|s| luhn_check(s)))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Normalize => {
            use unicode_normalization::UnicodeNormalization;
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("NORMALIZE requires 1 argument".into())
            })?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("NORMALIZE requires string argument".into()))?;

            // Default to NFC normalization
            let result: StringArray = str_arr
                .iter()
                .map(|opt| opt.map(|s| s.nfc().collect::<String>()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::ToUtf8 => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("TO_UTF8 requires 1 argument".into()))?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("TO_UTF8 requires string argument".into()))?;

            let result: BinaryArray = str_arr
                .iter()
                .map(|opt| opt.map(|s| s.as_bytes().to_vec()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::FromUtf8 => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("FROM_UTF8 requires 1 argument".into())
            })?;
            let bin_arr = arr
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| QueryError::Type("FROM_UTF8 requires binary argument".into()))?;

            let result: StringArray = bin_arr
                .iter()
                .map(|opt| opt.and_then(|b| String::from_utf8(b.to_vec()).ok()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::WordStem => {
            // Simple implementation - just return the word unchanged
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("WORD_STEM requires 1 argument".into())
            })?;
            Ok(arr.clone())
        }

        // ========== NEW DATE/TIME FUNCTIONS ==========
        ScalarFunction::Millisecond => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("MILLISECOND requires 1 argument".into())
            })?;
            if let Some(ts_arr) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                let result: Int32Array = ts_arr
                    .iter()
                    .map(|opt| opt.map(|us| ((us % 1_000_000) / 1_000) as i32))
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::NotImplemented(
                "MILLISECOND for this type not implemented".into(),
            ))
        }

        ScalarFunction::YearOfWeek => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("YEAR_OF_WEEK requires 1 argument".into())
            })?;
            if let Some(date32) = arr.as_any().downcast_ref::<Date32Array>() {
                use chrono::Datelike;
                let result: Int32Array = date32
                    .iter()
                    .map(|opt| {
                        opt.map(|days| {
                            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                                .unwrap_or_default();
                            date.iso_week().year()
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::NotImplemented(
                "YEAR_OF_WEEK for this type not implemented".into(),
            ))
        }

        ScalarFunction::TimezoneHour | ScalarFunction::TimezoneMinute => {
            // These require timezone-aware timestamps, return 0 for now
            let num_rows = batch.num_rows();
            Ok(Arc::new(Int32Array::from(vec![0; num_rows])))
        }

        ScalarFunction::CurrentTime => {
            use chrono::Timelike;
            let now = chrono::Local::now();
            let micros = now.hour() as i64 * 3600_000_000
                + now.minute() as i64 * 60_000_000
                + now.second() as i64 * 1_000_000
                + now.nanosecond() as i64 / 1000;
            let num_rows = batch.num_rows();
            Ok(Arc::new(Time64MicrosecondArray::from(vec![
                micros;
                num_rows
            ])))
        }

        ScalarFunction::CurrentTimezone => {
            let tz = chrono::Local::now().format("%:z").to_string();
            let num_rows = batch.num_rows();
            Ok(Arc::new(StringArray::from(vec![tz.as_str(); num_rows])))
        }

        ScalarFunction::Localtime => {
            use chrono::Timelike;
            let now = chrono::Local::now();
            let micros = now.hour() as i64 * 3600_000_000
                + now.minute() as i64 * 60_000_000
                + now.second() as i64 * 1_000_000;
            let num_rows = batch.num_rows();
            Ok(Arc::new(Time64MicrosecondArray::from(vec![
                micros;
                num_rows
            ])))
        }

        ScalarFunction::Localtimestamp => {
            let now = chrono::Local::now();
            let micros = now.timestamp_micros();
            let num_rows = batch.num_rows();
            Ok(Arc::new(TimestampMicrosecondArray::from(vec![
                micros;
                num_rows
            ])))
        }

        ScalarFunction::LastDayOfMonth => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("LAST_DAY_OF_MONTH requires 1 argument".into())
            })?;
            if let Some(date32) = arr.as_any().downcast_ref::<Date32Array>() {
                use chrono::Datelike;
                let result: Date32Array = date32
                    .iter()
                    .map(|opt| {
                        opt.map(|days| {
                            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                                .unwrap_or_default();
                            let last =
                                chrono::NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
                                    .unwrap()
                                    .with_day(1)
                                    .unwrap()
                                    + chrono::Months::new(1)
                                    - chrono::Duration::days(1);
                            last.num_days_from_ce() - 719163
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::NotImplemented(
                "LAST_DAY_OF_MONTH for this type not implemented".into(),
            ))
        }

        ScalarFunction::FromUnixtime => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("FROM_UNIXTIME requires 1 argument".into())
            })?;
            if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
                let result: TimestampMicrosecondArray = int_arr
                    .iter()
                    .map(|opt| opt.map(|secs| secs * 1_000_000))
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type(
                "FROM_UNIXTIME requires integer argument".into(),
            ))
        }

        ScalarFunction::ToUnixtime => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("TO_UNIXTIME requires 1 argument".into())
            })?;
            if let Some(ts_arr) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                let result: Int64Array = ts_arr
                    .iter()
                    .map(|opt| opt.map(|micros| micros / 1_000_000))
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type(
                "TO_UNIXTIME requires timestamp argument".into(),
            ))
        }

        ScalarFunction::FromIso8601Timestamp => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("FROM_ISO8601_TIMESTAMP requires 1 argument".into())
            })?;
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                QueryError::Type("FROM_ISO8601_TIMESTAMP requires string argument".into())
            })?;

            let result: TimestampMicrosecondArray = str_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        chrono::DateTime::parse_from_rfc3339(s)
                            .ok()
                            .map(|dt| dt.timestamp_micros())
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::FromIso8601Date => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("FROM_ISO8601_DATE requires 1 argument".into())
            })?;
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                QueryError::Type("FROM_ISO8601_DATE requires string argument".into())
            })?;

            let result: Date32Array = str_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                            .ok()
                            .map(|d| d.num_days_from_ce() - 719163)
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::ToIso8601 => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("TO_ISO8601 requires 1 argument".into())
            })?;
            if let Some(ts_arr) = arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                let result: StringArray = ts_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|micros| {
                            let dt =
                                chrono::DateTime::from_timestamp_micros(micros).unwrap_or_default();
                            dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string()
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            if let Some(date32) = arr.as_any().downcast_ref::<Date32Array>() {
                let result: StringArray = date32
                    .iter()
                    .map(|opt| {
                        opt.map(|days| {
                            let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)
                                .unwrap_or_default();
                            date.format("%Y-%m-%d").to_string()
                        })
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type(
                "TO_ISO8601 requires timestamp or date argument".into(),
            ))
        }

        ScalarFunction::DateFormat => {
            // DATE_FORMAT(timestamp, format) -> varchar
            // Uses MySQL/Trino format patterns
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "DATE_FORMAT requires 2 arguments (timestamp, format)".into(),
                ));
            }
            let ts_arr = &evaluated_args[0];
            let fmt_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("DATE_FORMAT format must be string".into()))?;

            // Convert Trino/MySQL format to chrono format
            fn convert_format(mysql_fmt: &str) -> String {
                // Note: Order matters! We use a placeholder to avoid conflicts
                // MySQL %i = minutes, MySQL %M = month name
                // Chrono %M = minutes, Chrono %B = month name
                mysql_fmt
                    .replace("%M", "{{MONTH_NAME}}") // Month name -> placeholder first
                    .replace("%i", "%M") // Minutes (00-59) - MySQL %i -> Chrono %M
                    .replace("{{MONTH_NAME}}", "%B") // Month name -> chrono %B
                    .replace("%Y", "%Y") // 4-digit year
                    .replace("%y", "%y") // 2-digit year
                    .replace("%m", "%m") // Month (01-12)
                    .replace("%c", "%-m") // Month (1-12) without leading zero
                    .replace("%d", "%d") // Day (01-31)
                    .replace("%e", "%-d") // Day (1-31) without leading zero
                    .replace("%H", "%H") // Hour 24h (00-23)
                    .replace("%h", "%I") // Hour 12h (01-12)
                    .replace("%I", "%I") // Hour 12h (01-12)
                    .replace("%k", "%-H") // Hour 24h (0-23) without leading zero
                    .replace("%l", "%-I") // Hour 12h (1-12) without leading zero
                    .replace("%s", "%S") // Seconds (00-59)
                    .replace("%S", "%S") // Seconds (00-59)
                    .replace("%p", "%p") // AM/PM
                    .replace("%W", "%A") // Weekday name
                    .replace("%w", "%u") // Day of week (0-6)
                    .replace("%a", "%a") // Abbreviated weekday
                    .replace("%b", "%b") // Abbreviated month name
                    .replace("%j", "%j") // Day of year
                    .replace("%U", "%U") // Week number (Sunday start)
                    .replace("%u", "%W") // Week number (Monday start)
                    .replace("%f", "%6f") // Microseconds
            }

            if let Some(ts_arr) = ts_arr.as_any().downcast_ref::<TimestampMicrosecondArray>() {
                let result: StringArray = (0..ts_arr.len())
                    .map(|i| {
                        if ts_arr.is_null(i) || fmt_arr.is_null(i) {
                            return None;
                        }
                        let micros = ts_arr.value(i);
                        let dt = chrono::DateTime::from_timestamp_micros(micros)?;
                        let fmt = fmt_arr.value(i);
                        let chrono_fmt = convert_format(fmt);
                        Some(dt.format(&chrono_fmt).to_string())
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            if let Some(date_arr) = ts_arr.as_any().downcast_ref::<Date32Array>() {
                let result: StringArray = (0..date_arr.len())
                    .map(|i| {
                        if date_arr.is_null(i) || fmt_arr.is_null(i) {
                            return None;
                        }
                        let days = date_arr.value(i);
                        let date = chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163)?;
                        let fmt = fmt_arr.value(i);
                        let chrono_fmt = convert_format(fmt);
                        Some(date.format(&chrono_fmt).to_string())
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            Err(QueryError::Type(
                "DATE_FORMAT requires timestamp or date argument".into(),
            ))
        }

        ScalarFunction::DateParse => {
            // DATE_PARSE(string, format) -> timestamp
            // Parses a date string using MySQL/Trino format
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "DATE_PARSE requires 2 arguments (string, format)".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("DATE_PARSE requires string argument".into()))?;
            let fmt_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("DATE_PARSE format must be string".into()))?;

            // Convert Trino/MySQL format to chrono format for parsing
            fn convert_parse_format(mysql_fmt: &str) -> String {
                mysql_fmt
                    .replace("%Y", "%Y") // 4-digit year
                    .replace("%y", "%y") // 2-digit year
                    .replace("%m", "%m") // Month (01-12)
                    .replace("%c", "%m") // Month (1-12) - chrono uses %m for both
                    .replace("%d", "%d") // Day (01-31)
                    .replace("%e", "%d") // Day (1-31) - chrono handles both
                    .replace("%H", "%H") // Hour 24h (00-23)
                    .replace("%h", "%I") // Hour 12h (01-12)
                    .replace("%I", "%I") // Hour 12h (01-12)
                    .replace("%k", "%H") // Hour 24h (0-23)
                    .replace("%l", "%I") // Hour 12h (1-12)
                    .replace("%i", "%M") // Minutes (00-59)
                    .replace("%s", "%S") // Seconds (00-59)
                    .replace("%S", "%S") // Seconds (00-59)
                    .replace("%p", "%p") // AM/PM
                    .replace("%f", "%6f") // Microseconds
            }

            let result: TimestampMicrosecondArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || fmt_arr.is_null(i) {
                        return None;
                    }
                    let s = str_arr.value(i);
                    let fmt = fmt_arr.value(i);
                    let chrono_fmt = convert_parse_format(fmt);

                    // Try parsing as datetime first
                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, &chrono_fmt) {
                        return Some(dt.and_utc().timestamp_micros());
                    }
                    // Try parsing as date only
                    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, &chrono_fmt) {
                        return Some(date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros());
                    }
                    None
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::ParseDatetime => {
            // PARSE_DATETIME is an alias for DATE_PARSE with Joda-style patterns
            // For now, support the same functionality as DATE_PARSE
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "PARSE_DATETIME requires 2 arguments (string, format)".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("PARSE_DATETIME requires string argument".into())
                })?;
            let fmt_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("PARSE_DATETIME format must be string".into()))?;

            // Convert Joda-style format to chrono format
            fn convert_joda_format(joda_fmt: &str) -> String {
                joda_fmt
                    .replace("yyyy", "%Y") // 4-digit year
                    .replace("yy", "%y") // 2-digit year
                    .replace("MM", "%m") // Month (01-12)
                    .replace("M", "%-m") // Month (1-12)
                    .replace("dd", "%d") // Day (01-31)
                    .replace("d", "%-d") // Day (1-31)
                    .replace("HH", "%H") // Hour 24h (00-23)
                    .replace("H", "%-H") // Hour 24h (0-23)
                    .replace("hh", "%I") // Hour 12h (01-12)
                    .replace("h", "%-I") // Hour 12h (1-12)
                    .replace("mm", "%M") // Minutes (00-59)
                    .replace("ss", "%S") // Seconds (00-59)
                    .replace("SSS", "%3f") // Milliseconds
                    .replace("a", "%p") // AM/PM
            }

            let result: TimestampMicrosecondArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || fmt_arr.is_null(i) {
                        return None;
                    }
                    let s = str_arr.value(i);
                    let fmt = fmt_arr.value(i);
                    let chrono_fmt = convert_joda_format(fmt);

                    // Try parsing as datetime first
                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, &chrono_fmt) {
                        return Some(dt.and_utc().timestamp_micros());
                    }
                    // Try parsing as date only
                    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, &chrono_fmt) {
                        return Some(date.and_hms_opt(0, 0, 0)?.and_utc().timestamp_micros());
                    }
                    None
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::ParseDuration => Err(QueryError::NotImplemented(
            "PARSE_DURATION not fully implemented".into(),
        )),

        ScalarFunction::HumanReadableSeconds => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("HUMAN_READABLE_SECONDS requires 1 argument".into())
            })?;
            let float_arr = get_float_array(arr)?;

            let result: StringArray = float_arr
                .iter()
                .map(|opt| match opt {
                    Some(secs) => {
                        let s = *secs;
                        if s < 60.0 {
                            Some(format!("{:.2} seconds", s))
                        } else if s < 3600.0 {
                            Some(format!("{:.2} minutes", s / 60.0))
                        } else if s < 86400.0 {
                            Some(format!("{:.2} hours", s / 3600.0))
                        } else {
                            Some(format!("{:.2} days", s / 86400.0))
                        }
                    }
                    None => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::AtTimezone => {
            // AT_TIMEZONE(timestamp, timezone) -> timestamp with time zone
            // Interprets timestamp in given timezone and converts to UTC
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "AT_TIMEZONE requires 2 arguments (timestamp, timezone)".into(),
                ));
            }
            let ts_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    QueryError::Type("AT_TIMEZONE requires timestamp argument".into())
                })?;
            let tz_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("AT_TIMEZONE requires string timezone".into()))?;

            // For now, just return the timestamp unchanged - proper timezone conversion
            // would require parsing timezone names and adjusting offsets
            let result: TimestampMicrosecondArray = (0..ts_arr.len())
                .map(|i| {
                    if ts_arr.is_null(i) || tz_arr.is_null(i) {
                        return None;
                    }
                    let micros = ts_arr.value(i);
                    let tz_str = tz_arr.value(i);

                    // Handle simple offset timezones like "+00:00", "-05:00", "UTC"
                    if tz_str == "UTC" || tz_str == "+00:00" || tz_str == "Z" {
                        return Some(micros);
                    }

                    // Parse offset like "+05:00" or "-08:00"
                    if let Some(offset) = parse_timezone_offset(tz_str) {
                        // Adjust by offset (convert from local to UTC)
                        return Some(micros - offset * 1_000_000);
                    }

                    // Unsupported timezone, return unchanged
                    Some(micros)
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::WithTimezone => {
            // WITH_TIMEZONE(timestamp, timezone) -> timestamp with time zone
            // Associates a timezone with a timestamp without changing the instant
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "WITH_TIMEZONE requires 2 arguments (timestamp, timezone)".into(),
                ));
            }
            let ts_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| {
                    QueryError::Type("WITH_TIMEZONE requires timestamp argument".into())
                })?;

            // For simplicity, just return the timestamp unchanged
            // The timezone would be associated as metadata in a full implementation
            Ok(Arc::new(ts_arr.clone()))
        }

        ScalarFunction::Timezone => {
            // TIMEZONE(timestamp_with_tz) -> varchar
            // Extracts the timezone string from a timestamp with timezone
            // Since we don't have timezone metadata, return "UTC"
            let num_rows = batch.num_rows();
            Ok(Arc::new(StringArray::from(vec!["UTC"; num_rows])))
        }

        // ========== NEW CONDITIONAL/FORMATTING FUNCTIONS ==========
        ScalarFunction::TryCast => {
            // For now, just return the input (actual casting requires type info)
            evaluated_args
                .first()
                .cloned()
                .ok_or_else(|| QueryError::InvalidArgument("TRY_CAST requires 1 argument".into()))
        }

        ScalarFunction::Try => {
            // For now, just return the input
            evaluated_args
                .first()
                .cloned()
                .ok_or_else(|| QueryError::InvalidArgument("TRY requires 1 argument".into()))
        }

        ScalarFunction::Format => {
            // FORMAT(format_string, args...) -> varchar
            // Simple printf-style formatting
            if evaluated_args.is_empty() {
                return Err(QueryError::InvalidArgument(
                    "FORMAT requires at least 1 argument".into(),
                ));
            }
            let fmt_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("FORMAT first argument must be string".into()))?;

            let num_rows = fmt_arr.len();
            let result: StringArray = (0..num_rows)
                .map(|i| {
                    if fmt_arr.is_null(i) {
                        return None;
                    }
                    let fmt = fmt_arr.value(i);
                    let mut result = fmt.to_string();

                    // Replace %s, %d, %f with corresponding arguments
                    for (arg_idx, arg) in evaluated_args.iter().skip(1).enumerate() {
                        let value = if let Some(s_arr) = arg.as_any().downcast_ref::<StringArray>()
                        {
                            if s_arr.is_null(i) {
                                "null".to_string()
                            } else {
                                s_arr.value(i).to_string()
                            }
                        } else if let Some(i_arr) = arg.as_any().downcast_ref::<Int64Array>() {
                            if i_arr.is_null(i) {
                                "null".to_string()
                            } else {
                                i_arr.value(i).to_string()
                            }
                        } else if let Some(f_arr) = arg.as_any().downcast_ref::<Float64Array>() {
                            if f_arr.is_null(i) {
                                "null".to_string()
                            } else {
                                format!("{}", f_arr.value(i))
                            }
                        } else {
                            "?".to_string()
                        };

                        // Replace first occurrence of %s, %d, or %f
                        if result.contains("%s") || result.contains("%d") || result.contains("%f") {
                            result = result.replacen("%s", &value, 1);
                            result = result.replacen("%d", &value, 1);
                            result = result.replacen("%f", &value, 1);
                        }
                    }
                    Some(result)
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::FormatNumber => {
            // FORMAT_NUMBER(number, decimal_places) -> varchar
            // Formats a number with thousands separators and specified decimal places
            if evaluated_args.is_empty() {
                return Err(QueryError::InvalidArgument(
                    "FORMAT_NUMBER requires at least 1 argument".into(),
                ));
            }
            let num_arr = &evaluated_args[0];
            let decimals = if evaluated_args.len() > 1 {
                evaluated_args.get(1)
            } else {
                None
            };

            fn format_with_separators(n: f64, decimals: usize) -> String {
                let formatted = format!("{:.prec$}", n, prec = decimals);
                let parts: Vec<&str> = formatted.split('.').collect();
                let int_part = parts[0];

                // Add thousands separators
                let negative = int_part.starts_with('-');
                let digits: String = if negative {
                    int_part[1..].to_string()
                } else {
                    int_part.to_string()
                };
                let mut with_sep = String::new();
                for (i, c) in digits.chars().rev().enumerate() {
                    if i > 0 && i % 3 == 0 {
                        with_sep.push(',');
                    }
                    with_sep.push(c);
                }
                let int_with_sep: String = with_sep.chars().rev().collect();

                if parts.len() > 1 {
                    if negative {
                        format!("-{}.{}", int_with_sep, parts[1])
                    } else {
                        format!("{}.{}", int_with_sep, parts[1])
                    }
                } else {
                    if negative {
                        format!("-{}", int_with_sep)
                    } else {
                        int_with_sep
                    }
                }
            }

            if let Some(f_arr) = num_arr.as_any().downcast_ref::<Float64Array>() {
                let result: StringArray = (0..f_arr.len())
                    .map(|i| {
                        if f_arr.is_null(i) {
                            return None;
                        }
                        let n = f_arr.value(i);
                        let dec = decimals.and_then(|d| get_int_value(d, i)).unwrap_or(0) as usize;
                        Some(format_with_separators(n, dec))
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            if let Some(i_arr) = num_arr.as_any().downcast_ref::<Int64Array>() {
                let result: StringArray = (0..i_arr.len())
                    .map(|i| {
                        if i_arr.is_null(i) {
                            return None;
                        }
                        let n = i_arr.value(i) as f64;
                        let dec = decimals.and_then(|d| get_int_value(d, i)).unwrap_or(0) as usize;
                        Some(format_with_separators(n, dec))
                    })
                    .collect();
                return Ok(Arc::new(result));
            }

            Err(QueryError::Type(
                "FORMAT_NUMBER requires numeric argument".into(),
            ))
        }

        ScalarFunction::ParseDataSize => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("PARSE_DATA_SIZE requires 1 argument".into())
            })?;
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                QueryError::Type("PARSE_DATA_SIZE requires string argument".into())
            })?;

            let result: Int64Array = str_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        let s = s.trim().to_uppercase();
                        if s.ends_with("TB") {
                            s[..s.len() - 2]
                                .trim()
                                .parse::<f64>()
                                .ok()
                                .map(|v| (v * 1024.0 * 1024.0 * 1024.0 * 1024.0) as i64)
                        } else if s.ends_with("GB") {
                            s[..s.len() - 2]
                                .trim()
                                .parse::<f64>()
                                .ok()
                                .map(|v| (v * 1024.0 * 1024.0 * 1024.0) as i64)
                        } else if s.ends_with("MB") {
                            s[..s.len() - 2]
                                .trim()
                                .parse::<f64>()
                                .ok()
                                .map(|v| (v * 1024.0 * 1024.0) as i64)
                        } else if s.ends_with("KB") {
                            s[..s.len() - 2]
                                .trim()
                                .parse::<f64>()
                                .ok()
                                .map(|v| (v * 1024.0) as i64)
                        } else if s.ends_with("B") {
                            s[..s.len() - 1].trim().parse::<i64>().ok()
                        } else {
                            s.parse::<i64>().ok()
                        }
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        // ========== NEW REGEX FUNCTIONS ==========
        ScalarFunction::RegexpExtractAll => {
            // Return all matches as comma-separated string (until array support)
            if evaluated_args.len() < 2 {
                return Err(QueryError::InvalidArgument(
                    "REGEXP_EXTRACT_ALL requires at least 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("REGEXP_EXTRACT_ALL requires string argument".into())
                })?;
            let pattern_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("REGEXP_EXTRACT_ALL requires string pattern".into())
                })?;

            let result: StringArray = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || pattern_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let pattern = pattern_arr.value(i);
                        match regex::Regex::new(pattern) {
                            Ok(re) => {
                                let matches: Vec<&str> =
                                    re.find_iter(s).map(|m| m.as_str()).collect();
                                Some(matches.join(","))
                            }
                            Err(_) => None,
                        }
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::RegexpPosition => {
            if evaluated_args.len() < 2 {
                return Err(QueryError::InvalidArgument(
                    "REGEXP_POSITION requires at least 2 arguments".into(),
                ));
            }
            let str_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("REGEXP_POSITION requires string argument".into())
                })?;
            let pattern_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("REGEXP_POSITION requires string pattern".into())
                })?;

            let result: Int64Array = (0..str_arr.len())
                .map(|i| {
                    if str_arr.is_null(i) || pattern_arr.is_null(i) {
                        None
                    } else {
                        let s = str_arr.value(i);
                        let pattern = pattern_arr.value(i);
                        match regex::Regex::new(pattern) {
                            Ok(re) => re.find(s).map(|m| m.start() as i64 + 1),
                            Err(_) => None,
                        }
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        // ========== NEW BINARY/ENCODING FUNCTIONS ==========
        ScalarFunction::ToBase64Url => {
            use base64::Engine;
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("TO_BASE64URL requires 1 argument".into())
            })?;

            if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                let result: StringArray = str_arr
                    .iter()
                    .map(|opt| {
                        opt.map(|s| base64::engine::general_purpose::URL_SAFE.encode(s.as_bytes()))
                    })
                    .collect();
                return Ok(Arc::new(result));
            }
            if let Some(bin_arr) = arr.as_any().downcast_ref::<BinaryArray>() {
                let result: StringArray = bin_arr
                    .iter()
                    .map(|opt| opt.map(|b| base64::engine::general_purpose::URL_SAFE.encode(b)))
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type(
                "TO_BASE64URL requires string or binary argument".into(),
            ))
        }

        ScalarFunction::FromBase64Url => {
            use base64::Engine;
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("FROM_BASE64URL requires 1 argument".into())
            })?;
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                QueryError::Type("FROM_BASE64URL requires string argument".into())
            })?;

            let result: BinaryArray = str_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| base64::engine::general_purpose::URL_SAFE.decode(s).ok())
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::ToBase32 => {
            use data_encoding::BASE32;
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("TO_BASE32 requires 1 argument".into())
            })?;

            if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                let result: StringArray = str_arr
                    .iter()
                    .map(|opt| opt.map(|s| BASE32.encode(s.as_bytes())))
                    .collect();
                return Ok(Arc::new(result));
            }
            if let Some(bin_arr) = arr.as_any().downcast_ref::<BinaryArray>() {
                let result: StringArray = bin_arr
                    .iter()
                    .map(|opt| opt.map(|b| BASE32.encode(b)))
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type(
                "TO_BASE32 requires string or binary argument".into(),
            ))
        }

        ScalarFunction::FromBase32 => {
            use data_encoding::BASE32;
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("FROM_BASE32 requires 1 argument".into())
            })?;
            let str_arr = arr
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("FROM_BASE32 requires string argument".into()))?;

            let result: BinaryArray = str_arr
                .iter()
                .map(|opt| opt.and_then(|s| BASE32.decode(s.as_bytes()).ok()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::Crc32 => {
            let arr = evaluated_args
                .first()
                .ok_or_else(|| QueryError::InvalidArgument("CRC32 requires 1 argument".into()))?;

            if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                let result: Int32Array = str_arr
                    .iter()
                    .map(|opt| opt.map(|s| crc32fast::hash(s.as_bytes()) as i32))
                    .collect();
                return Ok(Arc::new(result));
            }
            if let Some(bin_arr) = arr.as_any().downcast_ref::<BinaryArray>() {
                let result: Int32Array = bin_arr
                    .iter()
                    .map(|opt| opt.map(|b| crc32fast::hash(b) as i32))
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type(
                "CRC32 requires string or binary argument".into(),
            ))
        }

        ScalarFunction::Xxhash64 => {
            use xxhash_rust::xxh64::xxh64;
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("XXHASH64 requires 1 argument".into())
            })?;

            if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
                let result: Int64Array = str_arr
                    .iter()
                    .map(|opt| opt.map(|s| xxh64(s.as_bytes(), 0) as i64))
                    .collect();
                return Ok(Arc::new(result));
            }
            if let Some(bin_arr) = arr.as_any().downcast_ref::<BinaryArray>() {
                let result: Int64Array = bin_arr
                    .iter()
                    .map(|opt| opt.map(|b| xxh64(b, 0) as i64))
                    .collect();
                return Ok(Arc::new(result));
            }
            Err(QueryError::Type(
                "XXHASH64 requires string or binary argument".into(),
            ))
        }

        ScalarFunction::Murmur3
        | ScalarFunction::SpookyHashV2_32
        | ScalarFunction::SpookyHashV2_64 => Err(QueryError::NotImplemented(format!(
            "Hash function {:?} not implemented",
            func
        ))),

        ScalarFunction::HmacMd5
        | ScalarFunction::HmacSha1
        | ScalarFunction::HmacSha256
        | ScalarFunction::HmacSha512 => {
            use hmac::{Hmac, Mac};

            if evaluated_args.len() < 2 {
                return Err(QueryError::InvalidArgument(
                    "HMAC functions require 2 arguments (key, message)".into(),
                ));
            }

            let key_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("HMAC key must be string".into()))?;

            let msg_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("HMAC message must be string".into()))?;

            let result: StringArray = key_arr
                .iter()
                .zip(msg_arr.iter())
                .map(|(key_opt, msg_opt)| match (key_opt, msg_opt) {
                    (Some(key), Some(msg)) => {
                        let hash = match func {
                            ScalarFunction::HmacMd5 => {
                                type HmacMd5 = Hmac<md5::Md5>;
                                let mut mac = HmacMd5::new_from_slice(key.as_bytes()).unwrap();
                                mac.update(msg.as_bytes());
                                hex::encode(mac.finalize().into_bytes())
                            }
                            ScalarFunction::HmacSha1 => {
                                type HmacSha1 = Hmac<sha1::Sha1>;
                                let mut mac = HmacSha1::new_from_slice(key.as_bytes()).unwrap();
                                mac.update(msg.as_bytes());
                                hex::encode(mac.finalize().into_bytes())
                            }
                            ScalarFunction::HmacSha256 => {
                                type HmacSha256 = Hmac<sha2::Sha256>;
                                let mut mac = HmacSha256::new_from_slice(key.as_bytes()).unwrap();
                                mac.update(msg.as_bytes());
                                hex::encode(mac.finalize().into_bytes())
                            }
                            ScalarFunction::HmacSha512 => {
                                type HmacSha512 = Hmac<sha2::Sha512>;
                                let mut mac = HmacSha512::new_from_slice(key.as_bytes()).unwrap();
                                mac.update(msg.as_bytes());
                                hex::encode(mac.finalize().into_bytes())
                            }
                            _ => unreachable!(),
                        };
                        Some(hash)
                    }
                    _ => None,
                })
                .collect();

            Ok(Arc::new(result))
        }

        ScalarFunction::FromBigEndian32 => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("FROM_BIG_ENDIAN_32 requires 1 argument".into())
            })?;
            let bin_arr = arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                QueryError::Type("FROM_BIG_ENDIAN_32 requires binary argument".into())
            })?;

            let result: Int32Array = bin_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|b| {
                        if b.len() >= 4 {
                            Some(i32::from_be_bytes([b[0], b[1], b[2], b[3]]))
                        } else {
                            None
                        }
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::ToBigEndian32 => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("TO_BIG_ENDIAN_32 requires 1 argument".into())
            })?;
            let int_arr = arr.as_any().downcast_ref::<Int32Array>().ok_or_else(|| {
                QueryError::Type("TO_BIG_ENDIAN_32 requires integer argument".into())
            })?;

            let result: BinaryArray = int_arr
                .iter()
                .map(|opt| opt.map(|v| v.to_be_bytes().to_vec()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::FromBigEndian64 => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("FROM_BIG_ENDIAN_64 requires 1 argument".into())
            })?;
            let bin_arr = arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                QueryError::Type("FROM_BIG_ENDIAN_64 requires binary argument".into())
            })?;

            let result: Int64Array = bin_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|b| {
                        if b.len() >= 8 {
                            Some(i64::from_be_bytes([
                                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                            ]))
                        } else {
                            None
                        }
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::ToBigEndian64 => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("TO_BIG_ENDIAN_64 requires 1 argument".into())
            })?;
            let int_arr = arr.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                QueryError::Type("TO_BIG_ENDIAN_64 requires integer argument".into())
            })?;

            let result: BinaryArray = int_arr
                .iter()
                .map(|opt| opt.map(|v| v.to_be_bytes().to_vec()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::FromIeee754_32 => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("FROM_IEEE754_32 requires 1 argument".into())
            })?;
            let bin_arr = arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                QueryError::Type("FROM_IEEE754_32 requires binary argument".into())
            })?;

            let result: Float32Array = bin_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|b| {
                        if b.len() >= 4 {
                            Some(f32::from_be_bytes([b[0], b[1], b[2], b[3]]))
                        } else {
                            None
                        }
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::ToIeee754_32 => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("TO_IEEE754_32 requires 1 argument".into())
            })?;
            let float_arr = arr
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| QueryError::Type("TO_IEEE754_32 requires float argument".into()))?;

            let result: BinaryArray = float_arr
                .iter()
                .map(|opt| opt.map(|v| v.to_be_bytes().to_vec()))
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::FromIeee754_64 => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("FROM_IEEE754_64 requires 1 argument".into())
            })?;
            let bin_arr = arr.as_any().downcast_ref::<BinaryArray>().ok_or_else(|| {
                QueryError::Type("FROM_IEEE754_64 requires binary argument".into())
            })?;

            let result: Float64Array = bin_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|b| {
                        if b.len() >= 8 {
                            Some(f64::from_be_bytes([
                                b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                            ]))
                        } else {
                            None
                        }
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::ToIeee754_64 => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("TO_IEEE754_64 requires 1 argument".into())
            })?;
            let float_arr = arr
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| QueryError::Type("TO_IEEE754_64 requires float argument".into()))?;

            let result: BinaryArray = float_arr
                .iter()
                .map(|opt| opt.map(|v| v.to_be_bytes().to_vec()))
                .collect();
            Ok(Arc::new(result))
        }

        // ========== NEW BITWISE FUNCTIONS ==========
        ScalarFunction::BitwiseLeftShift => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "BITWISE_LEFT_SHIFT requires 2 arguments".into(),
                ));
            }
            let left = evaluated_args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    QueryError::Type("BITWISE_LEFT_SHIFT requires integer arguments".into())
                })?;
            let right = evaluated_args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    QueryError::Type("BITWISE_LEFT_SHIFT requires integer arguments".into())
                })?;

            let result: Int64Array = left
                .iter()
                .zip(right.iter())
                .map(|(l, r)| match (l, r) {
                    (Some(lv), Some(rv)) => Some(lv << (rv as u32)),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::BitwiseRightShift => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "BITWISE_RIGHT_SHIFT requires 2 arguments".into(),
                ));
            }
            let left = evaluated_args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    QueryError::Type("BITWISE_RIGHT_SHIFT requires integer arguments".into())
                })?;
            let right = evaluated_args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    QueryError::Type("BITWISE_RIGHT_SHIFT requires integer arguments".into())
                })?;

            let result: Int64Array = left
                .iter()
                .zip(right.iter())
                .map(|(l, r)| match (l, r) {
                    (Some(lv), Some(rv)) => Some((lv as u64 >> (rv as u32)) as i64),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::BitwiseRightShiftArithmetic => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "BITWISE_RIGHT_SHIFT_ARITHMETIC requires 2 arguments".into(),
                ));
            }
            let left = evaluated_args[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    QueryError::Type(
                        "BITWISE_RIGHT_SHIFT_ARITHMETIC requires integer arguments".into(),
                    )
                })?;
            let right = evaluated_args[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    QueryError::Type(
                        "BITWISE_RIGHT_SHIFT_ARITHMETIC requires integer arguments".into(),
                    )
                })?;

            let result: Int64Array = left
                .iter()
                .zip(right.iter())
                .map(|(l, r)| match (l, r) {
                    (Some(lv), Some(rv)) => Some(lv >> (rv as u32)),
                    _ => None,
                })
                .collect();
            Ok(Arc::new(result))
        }

        // ========== NEW URL FUNCTIONS ==========
        ScalarFunction::UrlExtractFragment => {
            let arr = evaluated_args.first().ok_or_else(|| {
                QueryError::InvalidArgument("URL_EXTRACT_FRAGMENT requires 1 argument".into())
            })?;
            let str_arr = arr.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                QueryError::Type("URL_EXTRACT_FRAGMENT requires string argument".into())
            })?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        url::Url::parse(s)
                            .ok()
                            .and_then(|u| u.fragment().map(|f| f.to_string()))
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::UrlExtractParameter => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "URL_EXTRACT_PARAMETER requires 2 arguments".into(),
                ));
            }
            let url_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("URL_EXTRACT_PARAMETER requires string URL".into())
                })?;
            let param_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("URL_EXTRACT_PARAMETER requires string parameter name".into())
                })?;

            let result: StringArray = (0..url_arr.len())
                .map(|i| {
                    if url_arr.is_null(i) || param_arr.is_null(i) {
                        None
                    } else {
                        let url_str = url_arr.value(i);
                        let param = param_arr.value(i);
                        url::Url::parse(url_str).ok().and_then(|u| {
                            u.query_pairs()
                                .find(|(k, _)| k == param)
                                .map(|(_, v)| v.to_string())
                        })
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        // Aliases
        ScalarFunction::Pow => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "POW requires 2 arguments".into(),
                ));
            }
            apply_math_binary(&evaluated_args[0], &evaluated_args[1], |a, b| a.powf(b))
        }

        ScalarFunction::Rand => {
            let num_rows = batch.num_rows();
            let result: Float64Array = (0..num_rows).map(|_| Some(rand::random::<f64>())).collect();
            Ok(Arc::new(result))
        }

        // ========== JSON FUNCTIONS ==========
        ScalarFunction::JsonExtract => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "JSON_EXTRACT requires 2 arguments".into(),
                ));
            }
            let json_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("JSON_EXTRACT requires string JSON".into()))?;
            let path_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("JSON_EXTRACT requires string path".into()))?;

            let result: StringArray = (0..json_arr.len())
                .map(|i| {
                    if json_arr.is_null(i) || path_arr.is_null(i) {
                        None
                    } else {
                        let json_str = json_arr.value(i);
                        let path = path_arr.value(i);
                        json_extract_impl(json_str, path)
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::JsonExtractScalar => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "JSON_EXTRACT_SCALAR requires 2 arguments".into(),
                ));
            }
            let json_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("JSON_EXTRACT_SCALAR requires string JSON".into())
                })?;
            let path_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("JSON_EXTRACT_SCALAR requires string path".into())
                })?;

            let result: StringArray = (0..json_arr.len())
                .map(|i| {
                    if json_arr.is_null(i) || path_arr.is_null(i) {
                        None
                    } else {
                        let json_str = json_arr.value(i);
                        let path = path_arr.value(i);
                        json_extract_scalar_impl(json_str, path)
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::JsonSize => {
            if evaluated_args.len() < 1 {
                return Err(QueryError::InvalidArgument(
                    "JSON_SIZE requires at least 1 argument".into(),
                ));
            }
            let json_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("JSON_SIZE requires string JSON".into()))?;
            let path_arr = evaluated_args.get(1);

            let result: Int64Array = (0..json_arr.len())
                .map(|i| {
                    if json_arr.is_null(i) {
                        None
                    } else {
                        let json_str = json_arr.value(i);
                        let path = path_arr
                            .and_then(|a| a.as_any().downcast_ref::<StringArray>())
                            .map(|a| if a.is_null(i) { "$" } else { a.value(i) })
                            .unwrap_or("$");
                        json_size_impl(json_str, path)
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::JsonArrayLength => {
            let json_arr = evaluated_args
                .first()
                .ok_or_else(|| {
                    QueryError::InvalidArgument("JSON_ARRAY_LENGTH requires 1 argument".into())
                })?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("JSON_ARRAY_LENGTH requires string JSON".into()))?;

            let result: Int64Array = json_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|json_str| {
                        serde_json::from_str::<serde_json::Value>(json_str)
                            .ok()
                            .and_then(|v| v.as_array().map(|a| a.len() as i64))
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::JsonArrayGet => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "JSON_ARRAY_GET requires 2 arguments".into(),
                ));
            }
            let json_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("JSON_ARRAY_GET requires string JSON".into()))?;
            let idx_arr = &evaluated_args[1];

            let result: StringArray = (0..json_arr.len())
                .map(|i| {
                    if json_arr.is_null(i) {
                        None
                    } else {
                        let json_str = json_arr.value(i);
                        let idx = get_int_value(idx_arr, i).unwrap_or(0) as usize;
                        serde_json::from_str::<serde_json::Value>(json_str)
                            .ok()
                            .and_then(|v| {
                                v.as_array().and_then(|a| a.get(idx).map(|e| e.to_string()))
                            })
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::JsonArrayContains => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "JSON_ARRAY_CONTAINS requires 2 arguments".into(),
                ));
            }
            let json_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    QueryError::Type("JSON_ARRAY_CONTAINS requires string JSON".into())
                })?;
            let value_arr = &evaluated_args[1];

            let result: BooleanArray = (0..json_arr.len())
                .map(|i| {
                    if json_arr.is_null(i) {
                        None
                    } else {
                        let json_str = json_arr.value(i);
                        let search_value = get_scalar_value(value_arr, i);
                        Some(json_array_contains_impl(json_str, &search_value))
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::IsJsonScalar => {
            let json_arr = evaluated_args
                .first()
                .ok_or_else(|| {
                    QueryError::InvalidArgument("IS_JSON_SCALAR requires 1 argument".into())
                })?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("IS_JSON_SCALAR requires string JSON".into()))?;

            let result: BooleanArray = json_arr
                .iter()
                .map(|opt| {
                    opt.map(|json_str| {
                        serde_json::from_str::<serde_json::Value>(json_str)
                            .ok()
                            .map(|v| !v.is_array() && !v.is_object())
                            .unwrap_or(false)
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::JsonFormat => {
            let json_arr = evaluated_args
                .first()
                .ok_or_else(|| {
                    QueryError::InvalidArgument("JSON_FORMAT requires 1 argument".into())
                })?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("JSON_FORMAT requires string JSON".into()))?;

            let result: StringArray = json_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|json_str| {
                        serde_json::from_str::<serde_json::Value>(json_str)
                            .ok()
                            .map(|v| {
                                serde_json::to_string_pretty(&v)
                                    .unwrap_or_else(|_| json_str.to_string())
                            })
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::JsonParse => {
            // JSON_PARSE validates and returns the JSON string
            let str_arr = evaluated_args
                .first()
                .ok_or_else(|| {
                    QueryError::InvalidArgument("JSON_PARSE requires 1 argument".into())
                })?
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("JSON_PARSE requires string argument".into()))?;

            let result: StringArray = str_arr
                .iter()
                .map(|opt| {
                    opt.and_then(|s| {
                        serde_json::from_str::<serde_json::Value>(s)
                            .ok()
                            .map(|_| s.to_string())
                    })
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::JsonQuery => {
            // JSON_QUERY is similar to JSON_EXTRACT but returns NULL for scalar values
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "JSON_QUERY requires 2 arguments".into(),
                ));
            }
            let json_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("JSON_QUERY requires string JSON".into()))?;
            let path_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("JSON_QUERY requires string path".into()))?;

            let result: StringArray = (0..json_arr.len())
                .map(|i| {
                    if json_arr.is_null(i) || path_arr.is_null(i) {
                        None
                    } else {
                        let json_str = json_arr.value(i);
                        let path = path_arr.value(i);
                        json_query_impl(json_str, path)
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::JsonValue => {
            // JSON_VALUE returns a scalar value as a string, NULL for objects/arrays
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "JSON_VALUE requires 2 arguments".into(),
                ));
            }
            let json_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("JSON_VALUE requires string JSON".into()))?;
            let path_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("JSON_VALUE requires string path".into()))?;

            let result: StringArray = (0..json_arr.len())
                .map(|i| {
                    if json_arr.is_null(i) || path_arr.is_null(i) {
                        None
                    } else {
                        let json_str = json_arr.value(i);
                        let path = path_arr.value(i);
                        json_value_impl(json_str, path)
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::JsonExists => {
            if evaluated_args.len() != 2 {
                return Err(QueryError::InvalidArgument(
                    "JSON_EXISTS requires 2 arguments".into(),
                ));
            }
            let json_arr = evaluated_args[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("JSON_EXISTS requires string JSON".into()))?;
            let path_arr = evaluated_args[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("JSON_EXISTS requires string path".into()))?;

            let result: BooleanArray = (0..json_arr.len())
                .map(|i| {
                    if json_arr.is_null(i) || path_arr.is_null(i) {
                        None
                    } else {
                        let json_str = json_arr.value(i);
                        let path = path_arr.value(i);
                        Some(json_exists_impl(json_str, path))
                    }
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::JsonObject => {
            // Creates a JSON object from key-value pairs
            // JSON_OBJECT('key1', value1, 'key2', value2, ...)
            if evaluated_args.is_empty() || evaluated_args.len() % 2 != 0 {
                return Err(QueryError::InvalidArgument(
                    "JSON_OBJECT requires an even number of arguments".into(),
                ));
            }
            let num_rows = evaluated_args[0].len();
            let result: StringArray = (0..num_rows)
                .map(|i| {
                    let mut obj = serde_json::Map::new();
                    for pair in evaluated_args.chunks(2) {
                        let key = get_string_value(&pair[0], i).unwrap_or_default();
                        let value = get_json_value(&pair[1], i);
                        obj.insert(key, value);
                    }
                    Some(serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_default())
                })
                .collect();
            Ok(Arc::new(result))
        }

        ScalarFunction::JsonArray => {
            // Creates a JSON array from values
            if evaluated_args.is_empty() {
                let num_rows = batch.num_rows();
                let result: StringArray = (0..num_rows).map(|_| Some("[]".to_string())).collect();
                return Ok(Arc::new(result));
            }
            let num_rows = evaluated_args[0].len();
            let result: StringArray = (0..num_rows)
                .map(|i| {
                    let arr: Vec<serde_json::Value> = evaluated_args
                        .iter()
                        .map(|arg| get_json_value(arg, i))
                        .collect();
                    Some(serde_json::to_string(&serde_json::Value::Array(arr)).unwrap_or_default())
                })
                .collect();
            Ok(Arc::new(result))
        }

        _ => Err(QueryError::NotImplemented(format!(
            "Scalar function not implemented: {:?}",
            func
        ))),
    }
}

fn apply_math_unary_preserve_int<F, G>(arr: &ArrayRef, f_float: F, f_int: G) -> Result<ArrayRef>
where
    F: Fn(f64) -> f64,
    G: Fn(i64) -> i64,
{
    use arrow::array::Float64Array;

    // Preserve int types for functions like ABS
    if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
        let result: Int64Array = int_arr.iter().map(|opt| opt.map(&f_int)).collect();
        return Ok(Arc::new(result));
    }

    if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
        let result: Int32Array = int_arr
            .iter()
            .map(|opt| opt.map(|v| f_int(v as i64) as i32))
            .collect();
        return Ok(Arc::new(result));
    }

    if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>() {
        let result: Float64Array = float_arr.iter().map(|opt| opt.map(&f_float)).collect();
        return Ok(Arc::new(result));
    }

    Err(QueryError::Type(
        "Math function requires numeric argument".into(),
    ))
}

fn apply_math_unary<F>(arr: &ArrayRef, f: F) -> Result<ArrayRef>
where
    F: Fn(f64) -> f64,
{
    use arrow::array::Float64Array;

    // Always return Float64 for functions like SQRT, CEIL, FLOOR, ROUND
    if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>() {
        let result: Float64Array = float_arr.iter().map(|opt| opt.map(&f)).collect();
        return Ok(Arc::new(result));
    }

    if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
        let result: Float64Array = int_arr.iter().map(|opt| opt.map(|v| f(v as f64))).collect();
        return Ok(Arc::new(result));
    }

    if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
        let result: Float64Array = int_arr.iter().map(|opt| opt.map(|v| f(v as f64))).collect();
        return Ok(Arc::new(result));
    }

    Err(QueryError::Type(
        "Math function requires numeric argument".into(),
    ))
}

fn apply_math_binary<F>(left: &ArrayRef, right: &ArrayRef, f: F) -> Result<ArrayRef>
where
    F: Fn(f64, f64) -> f64,
{
    use arrow::array::Float64Array;

    let left_vals = get_float_array(left)?;
    let right_vals = get_float_array(right)?;

    let result: Float64Array = left_vals
        .iter()
        .zip(right_vals.iter())
        .map(|(l, r)| match (l, r) {
            (Some(lv), Some(rv)) => Some(f(*lv, *rv)),
            _ => None,
        })
        .collect();

    Ok(Arc::new(result))
}

fn get_float_array(arr: &ArrayRef) -> Result<Vec<Option<f64>>> {
    use arrow::array::Float64Array;

    if let Some(float_arr) = arr.as_any().downcast_ref::<Float64Array>() {
        return Ok(float_arr.iter().collect());
    }

    if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
        return Ok(int_arr.iter().map(|opt| opt.map(|v| v as f64)).collect());
    }

    if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
        return Ok(int_arr.iter().map(|opt| opt.map(|v| v as f64)).collect());
    }

    Err(QueryError::Type("Expected numeric array".into()))
}

fn get_int_value(arr: &ArrayRef, idx: usize) -> Option<i64> {
    if let Some(i64_arr) = arr.as_any().downcast_ref::<Int64Array>() {
        return Some(i64_arr.value(idx));
    }
    if let Some(i32_arr) = arr.as_any().downcast_ref::<Int32Array>() {
        return Some(i32_arr.value(idx) as i64);
    }
    None
}

/// Parse timezone offset strings like "+05:00", "-08:00", "Z"
/// Returns the offset in seconds from UTC
fn parse_timezone_offset(tz: &str) -> Option<i64> {
    let tz = tz.trim();
    if tz == "Z" || tz == "UTC" || tz == "+00:00" {
        return Some(0);
    }

    // Parse format like "+05:00" or "-08:00"
    if (tz.starts_with('+') || tz.starts_with('-')) && tz.len() >= 5 {
        let sign = if tz.starts_with('-') { -1 } else { 1 };
        let rest = &tz[1..];

        // Try format "+05:00" or "+0500"
        if rest.contains(':') {
            let parts: Vec<&str> = rest.split(':').collect();
            if parts.len() >= 2 {
                let hours: i64 = parts[0].parse().ok()?;
                let mins: i64 = parts[1].parse().ok()?;
                return Some(sign * (hours * 3600 + mins * 60));
            }
        } else if rest.len() >= 4 {
            let hours: i64 = rest[..2].parse().ok()?;
            let mins: i64 = rest[2..4].parse().ok()?;
            return Some(sign * (hours * 3600 + mins * 60));
        }
    }

    None
}

/// SQL LIKE pattern matching
/// `%` matches any sequence of characters (including empty)
/// `_` matches exactly one character
fn like_match(text: &str, pattern: &str) -> bool {
    let t_chars: Vec<char> = text.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();

    like_match_recursive(&t_chars, &p_chars)
}

fn like_match_recursive(text: &[char], pattern: &[char]) -> bool {
    if pattern.is_empty() {
        return text.is_empty();
    }

    match pattern[0] {
        '%' => {
            // Try matching zero or more characters
            // First try matching zero characters
            if like_match_recursive(text, &pattern[1..]) {
                return true;
            }
            // Then try matching one or more characters
            if !text.is_empty() && like_match_recursive(&text[1..], pattern) {
                return true;
            }
            false
        }
        '_' => {
            // Match exactly one character
            if text.is_empty() {
                false
            } else {
                like_match_recursive(&text[1..], &pattern[1..])
            }
        }
        c => {
            // Match exact character
            if text.is_empty() || text[0] != c {
                false
            } else {
                like_match_recursive(&text[1..], &pattern[1..])
            }
        }
    }
}

use chrono::Datelike;

/// Compute Levenshtein edit distance between two strings
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    let m = s1_chars.len();
    let n = s2_chars.len();

    if m == 0 {
        return n;
    }
    if n == 0 {
        return m;
    }

    let mut prev = (0..=n).collect::<Vec<_>>();
    let mut curr = vec![0; n + 1];

    for i in 1..=m {
        curr[0] = i;
        for j in 1..=n {
            let cost = if s1_chars[i - 1] == s2_chars[j - 1] {
                0
            } else {
                1
            };
            curr[j] = (prev[j] + 1).min(curr[j - 1] + 1).min(prev[j - 1] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[n]
}

/// Compute Soundex code for a string
fn soundex(s: &str) -> String {
    if s.is_empty() {
        return String::new();
    }

    let s = s.to_uppercase();
    let mut result = String::new();
    let mut prev_code = '0';

    for (i, c) in s.chars().enumerate() {
        if i == 0 {
            result.push(c);
            prev_code = soundex_code(c);
            continue;
        }

        let code = soundex_code(c);
        if code != '0' && code != prev_code {
            result.push(code);
            if result.len() >= 4 {
                break;
            }
        }
        if code != '0' {
            prev_code = code;
        }
    }

    while result.len() < 4 {
        result.push('0');
    }
    result
}

fn soundex_code(c: char) -> char {
    match c {
        'B' | 'F' | 'P' | 'V' => '1',
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => '2',
        'D' | 'T' => '3',
        'L' => '4',
        'M' | 'N' => '5',
        'R' => '6',
        _ => '0',
    }
}

/// Validate a number using the Luhn algorithm
fn luhn_check(s: &str) -> bool {
    let digits: Vec<u32> = s
        .chars()
        .filter(|c| c.is_ascii_digit())
        .map(|c| c.to_digit(10).unwrap())
        .collect();

    if digits.is_empty() {
        return false;
    }

    let mut sum = 0;
    for (i, &d) in digits.iter().rev().enumerate() {
        if i % 2 == 1 {
            let doubled = d * 2;
            sum += if doubled > 9 { doubled - 9 } else { doubled };
        } else {
            sum += d;
        }
    }
    sum % 10 == 0
}

// ========== JSON HELPER FUNCTIONS ==========

/// Navigate a JSON path and return the extracted value as a JSON string
fn json_extract_impl(json_str: &str, path: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let result = navigate_json_path(&value, path)?;
    Some(result.to_string())
}

/// Navigate a JSON path and return a scalar value as a string (no quotes)
fn json_extract_scalar_impl(json_str: &str, path: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let result = navigate_json_path(&value, path)?;
    match result {
        serde_json::Value::Null => Some("null".to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::String(s) => Some(s.clone()),
        _ => None, // Return None for objects and arrays
    }
}

/// Get the size of a JSON value at the given path
fn json_size_impl(json_str: &str, path: &str) -> Option<i64> {
    let value: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let result = navigate_json_path(&value, path)?;
    match result {
        serde_json::Value::Array(a) => Some(a.len() as i64),
        serde_json::Value::Object(o) => Some(o.len() as i64),
        serde_json::Value::String(s) => Some(s.len() as i64),
        _ => Some(0),
    }
}

/// Check if a JSON array contains a value
fn json_array_contains_impl(json_str: &str, search_value: &ScalarValue) -> bool {
    let value: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => return false,
    };
    let arr = match value.as_array() {
        Some(a) => a,
        None => return false,
    };
    let search_json = scalar_to_json_value(search_value);
    arr.iter().any(|v| json_values_equal(v, &search_json))
}

/// JSON_QUERY returns the extracted value only if it's an object or array
fn json_query_impl(json_str: &str, path: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let result = navigate_json_path(&value, path)?;
    match result {
        serde_json::Value::Array(_) | serde_json::Value::Object(_) => Some(result.to_string()),
        _ => None,
    }
}

/// JSON_VALUE returns the scalar value at the path as a string
fn json_value_impl(json_str: &str, path: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(json_str).ok()?;
    let result = navigate_json_path(&value, path)?;
    match result {
        serde_json::Value::Null => Some("null".to_string()),
        serde_json::Value::Bool(b) => Some(b.to_string()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::String(s) => Some(s.clone()),
        _ => None, // Return None for objects and arrays
    }
}

/// Check if a path exists in the JSON
fn json_exists_impl(json_str: &str, path: &str) -> bool {
    let value: serde_json::Value = match serde_json::from_str(json_str) {
        Ok(v) => v,
        Err(_) => return false,
    };
    navigate_json_path(&value, path).is_some()
}

/// Navigate a JSON path like "$.key[0].nested"
fn navigate_json_path<'a>(
    value: &'a serde_json::Value,
    path: &str,
) -> Option<&'a serde_json::Value> {
    let path = path.trim();

    // Handle root reference
    if path == "$" || path.is_empty() {
        return Some(value);
    }

    // Remove leading $. if present
    let path = if path.starts_with("$.") {
        &path[2..]
    } else if path.starts_with("$") {
        &path[1..]
    } else {
        path
    };

    let mut current = value;

    for part in split_json_path(path) {
        match part {
            JsonPathPart::Key(key) => {
                current = current.get(&key)?;
            }
            JsonPathPart::Index(idx) => {
                current = current.get(idx)?;
            }
        }
    }

    Some(current)
}

enum JsonPathPart {
    Key(String),
    Index(usize),
}

fn split_json_path(path: &str) -> Vec<JsonPathPart> {
    let mut parts = Vec::new();
    let mut current_key = String::new();
    let mut chars = path.chars().peekable();
    let mut in_bracket = false;
    let mut bracket_content = String::new();

    while let Some(c) = chars.next() {
        if in_bracket {
            if c == ']' {
                in_bracket = false;
                let content = bracket_content.trim_matches(|c| c == '\'' || c == '"');
                if let Ok(idx) = content.parse::<usize>() {
                    parts.push(JsonPathPart::Index(idx));
                } else {
                    parts.push(JsonPathPart::Key(content.to_string()));
                }
                bracket_content.clear();
            } else {
                bracket_content.push(c);
            }
        } else {
            match c {
                '.' => {
                    if !current_key.is_empty() {
                        parts.push(JsonPathPart::Key(current_key.clone()));
                        current_key.clear();
                    }
                }
                '[' => {
                    if !current_key.is_empty() {
                        parts.push(JsonPathPart::Key(current_key.clone()));
                        current_key.clear();
                    }
                    in_bracket = true;
                }
                _ => {
                    current_key.push(c);
                }
            }
        }
    }

    if !current_key.is_empty() {
        parts.push(JsonPathPart::Key(current_key));
    }

    parts
}

/// Convert a ScalarValue to a serde_json Value
fn scalar_to_json_value(scalar: &ScalarValue) -> serde_json::Value {
    match scalar {
        ScalarValue::Null => serde_json::Value::Null,
        ScalarValue::Boolean(b) => serde_json::Value::Bool(*b),
        ScalarValue::Int8(i) => serde_json::json!(*i),
        ScalarValue::Int16(i) => serde_json::json!(*i),
        ScalarValue::Int32(i) => serde_json::json!(*i),
        ScalarValue::Int64(i) => serde_json::json!(*i),
        ScalarValue::UInt8(i) => serde_json::json!(*i),
        ScalarValue::UInt16(i) => serde_json::json!(*i),
        ScalarValue::UInt32(i) => serde_json::json!(*i),
        ScalarValue::UInt64(i) => serde_json::json!(*i),
        ScalarValue::Float32(f) => serde_json::json!(f.0),
        ScalarValue::Float64(f) => serde_json::json!(f.0),
        ScalarValue::Utf8(s) => serde_json::Value::String(s.clone()),
        _ => serde_json::Value::Null,
    }
}

/// Compare two JSON values for equality
fn json_values_equal(a: &serde_json::Value, b: &serde_json::Value) -> bool {
    match (a, b) {
        (serde_json::Value::Null, serde_json::Value::Null) => true,
        (serde_json::Value::Bool(a), serde_json::Value::Bool(b)) => a == b,
        (serde_json::Value::Number(a), serde_json::Value::Number(b)) => {
            // Compare as f64 for flexibility
            a.as_f64() == b.as_f64()
        }
        (serde_json::Value::String(a), serde_json::Value::String(b)) => a == b,
        // Also handle comparing string to number
        (serde_json::Value::String(a), serde_json::Value::Number(b)) => {
            a.parse::<f64>().ok() == b.as_f64()
        }
        (serde_json::Value::Number(a), serde_json::Value::String(b)) => {
            a.as_f64() == b.parse::<f64>().ok()
        }
        _ => false,
    }
}

/// Get a JSON value from an array at a given row index
fn get_json_value(arr: &ArrayRef, row: usize) -> serde_json::Value {
    if arr.is_null(row) {
        return serde_json::Value::Null;
    }

    if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
        return serde_json::Value::String(str_arr.value(row).to_string());
    }
    if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
        return serde_json::json!(int_arr.value(row));
    }
    if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
        return serde_json::json!(int_arr.value(row));
    }
    if let Some(float_arr) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return serde_json::json!(float_arr.value(row));
    }
    if let Some(bool_arr) = arr.as_any().downcast_ref::<BooleanArray>() {
        return serde_json::Value::Bool(bool_arr.value(row));
    }

    serde_json::Value::Null
}

/// Get a string value from an array at a given row index
fn get_string_value(arr: &ArrayRef, row: usize) -> Option<String> {
    if arr.is_null(row) {
        return None;
    }
    if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
        return Some(str_arr.value(row).to_string());
    }
    if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
        return Some(int_arr.value(row).to_string());
    }
    if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
        return Some(int_arr.value(row).to_string());
    }
    None
}

/// Get a scalar value from an array at a given row index
fn get_scalar_value(arr: &ArrayRef, row: usize) -> ScalarValue {
    if arr.is_null(row) {
        return ScalarValue::Null;
    }

    if let Some(str_arr) = arr.as_any().downcast_ref::<StringArray>() {
        return ScalarValue::Utf8(str_arr.value(row).to_string());
    }
    if let Some(int_arr) = arr.as_any().downcast_ref::<Int64Array>() {
        return ScalarValue::Int64(int_arr.value(row));
    }
    if let Some(int_arr) = arr.as_any().downcast_ref::<Int32Array>() {
        return ScalarValue::Int32(int_arr.value(row));
    }
    if let Some(float_arr) = arr.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return ScalarValue::Float64(ordered_float::OrderedFloat(float_arr.value(row)));
    }
    if let Some(bool_arr) = arr.as_any().downcast_ref::<BooleanArray>() {
        return ScalarValue::Boolean(bool_arr.value(row));
    }

    ScalarValue::Null
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::MemoryTableExec;
    use arrow::array::Int64Array;
    use arrow::datatypes::{Field, Schema};
    use futures::TryStreamExt;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_filter_simple() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));

        // Filter: value > 25
        let predicate = Expr::column("value").gt(Expr::literal(ScalarValue::Int64(25)));

        let filter = FilterExec::new(scan, predicate);

        let stream = filter.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3); // 30, 40, 50
    }

    #[tokio::test]
    async fn test_filter_and() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));

        // Filter: value > 15 AND value < 45
        let predicate = Expr::column("value")
            .gt(Expr::literal(ScalarValue::Int64(15)))
            .and(Expr::column("value").lt(Expr::literal(ScalarValue::Int64(45))));

        let filter = FilterExec::new(scan, predicate);

        let stream = filter.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3); // 20, 30, 40
    }
}
