//! Filter operator

use crate::error::{QueryError, Result};
use crate::physical::operators::subquery::{evaluate_subquery_expr, SubqueryExecutor};
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::{BinaryOp, Column, Expr, ScalarValue, UnaryOp};
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Datum, Int32Array, Int64Array, StringArray,
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

            // Start with the last argument
            let mut result = evaluated_args.last().unwrap().clone();

            // Work backwards, replacing nulls with previous values
            for arr in evaluated_args.iter().rev().skip(1) {
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
        let result: Int64Array = int_arr.iter().map(|opt| opt.map(|v| f_int(v))).collect();
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
        let result: Float64Array = float_arr
            .iter()
            .map(|opt| opt.map(|v| f_float(v)))
            .collect();
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
        let result: Float64Array = float_arr.iter().map(|opt| opt.map(|v| f(v))).collect();
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
