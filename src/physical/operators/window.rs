//! Window function execution operator
//!
//! Implements SQL window functions (ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD)
//! with OVER (PARTITION BY ... ORDER BY ...) semantics.
//!
//! Algorithm:
//! 1. Collect all input rows into a single combined RecordBatch
//! 2. Sort by PARTITION BY columns, then ORDER BY columns
//! 3. Iterate sorted rows, tracking partition boundaries
//! 4. Assign window function values per partition
//! 5. Output batches with window columns appended

use crate::error::{QueryError, Result};
use crate::physical::operators::filter::evaluate_expr;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::{Expr, NullOrdering, ScalarValue, SortDirection, SortExpr, WindowFunction};
use arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow::compute::{lexsort_to_indices, SortColumn, SortOptions};
use arrow::datatypes::{DataType, SchemaRef};
use async_trait::async_trait;
use futures::stream;
use std::fmt;
use std::sync::Arc;

/// Window function physical operator
pub struct WindowExec {
    input: Arc<dyn PhysicalOperator>,
    window_exprs: Vec<Expr>,
    output_names: Vec<String>,
    schema: SchemaRef,
}

impl fmt::Debug for WindowExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WindowExec({} window exprs)", self.window_exprs.len())
    }
}

impl WindowExec {
    pub fn new(
        input: Arc<dyn PhysicalOperator>,
        window_exprs: Vec<Expr>,
        output_names: Vec<String>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            input,
            window_exprs,
            output_names,
            schema,
        }
    }
}

#[async_trait]
impl PhysicalOperator for WindowExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.input.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        use futures::TryStreamExt;

        let input_stream = self.input.execute(partition).await?;
        let batches: Vec<RecordBatch> = input_stream.try_collect().await?;

        if batches.is_empty() {
            let stream = stream::iter(std::iter::empty::<Result<RecordBatch>>());
            return Ok(Box::pin(stream));
        }

        let input_batch = concat_batches(&batches)?;
        let num_rows = input_batch.num_rows();

        if num_rows == 0 {
            let stream = stream::iter(std::iter::empty::<Result<RecordBatch>>());
            return Ok(Box::pin(stream));
        }

        let input_schema = input_batch.schema();

        let mut window_arrays: Vec<ArrayRef> = Vec::with_capacity(self.window_exprs.len());
        for wf_expr in &self.window_exprs {
            let arr = compute_window_function(wf_expr, &input_batch, &input_schema)?;
            window_arrays.push(arr);
        }

        let mut output_columns: Vec<ArrayRef> = input_batch.columns().to_vec();
        output_columns.extend(window_arrays);

        let output_batch = RecordBatch::try_new(self.schema.clone(), output_columns)
            .map_err(|e| QueryError::Execution(format!("WindowExec output batch error: {}", e)))?;

        let stream = stream::iter(vec![Ok(output_batch)]);
        Ok(Box::pin(stream))
    }

    fn name(&self) -> &str {
        "WindowExec"
    }
}

impl fmt::Display for WindowExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WindowExec: [")?;
        for (i, name) in self.output_names.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", name)?;
        }
        write!(f, "]")
    }
}

fn concat_batches(batches: &[RecordBatch]) -> Result<RecordBatch> {
    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }
    let schema = batches[0].schema();
    let arrays: Vec<Vec<ArrayRef>> = (0..schema.fields().len())
        .map(|col_idx| batches.iter().map(|b| b.column(col_idx).clone()).collect())
        .collect();
    let concatenated: Vec<ArrayRef> = arrays
        .iter()
        .map(|col_arrays| {
            let refs: Vec<&dyn arrow::array::Array> =
                col_arrays.iter().map(|a| a.as_ref()).collect();
            arrow::compute::concat(&refs)
                .map_err(|e| QueryError::Execution(format!("Concat error: {}", e)))
        })
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(schema, concatenated)
        .map_err(|e| QueryError::Execution(format!("Concat batch error: {}", e)))
}

enum WindowValues {
    Int64(Vec<i64>),
    Float64(Vec<Option<f64>>),
}

fn compute_window_function(
    expr: &Expr,
    batch: &RecordBatch,
    schema: &SchemaRef,
) -> Result<ArrayRef> {
    let (func, args, partition_by, order_by) = match expr {
        Expr::WindowFunc {
            func,
            args,
            partition_by,
            order_by,
        } => (func, args, partition_by, order_by),
        _ => {
            return Err(QueryError::Execution(
                "Expected WindowFunc expression".to_string(),
            ))
        }
    };

    let num_rows = batch.num_rows();
    let sort_indices = compute_sort_indices(batch, schema, partition_by, order_by)?;

    let sorted_values = match func {
        WindowFunction::RowNumber => {
            compute_row_number(&sort_indices, batch, schema, partition_by)?
        }
        WindowFunction::Rank => {
            compute_rank(&sort_indices, batch, schema, partition_by, order_by, false)?
        }
        WindowFunction::DenseRank => {
            compute_rank(&sort_indices, batch, schema, partition_by, order_by, true)?
        }
        WindowFunction::Lag => {
            compute_lag_lead(&sort_indices, batch, schema, partition_by, args, false)?
        }
        WindowFunction::Lead => {
            compute_lag_lead(&sort_indices, batch, schema, partition_by, args, true)?
        }
        WindowFunction::Sum
        | WindowFunction::Avg
        | WindowFunction::Min
        | WindowFunction::Max
        | WindowFunction::Count => {
            compute_aggregate_window(func, &sort_indices, batch, schema, partition_by, args)?
        }
    };

    // Rearrange from sorted order back to original row order
    match &sorted_values {
        WindowValues::Int64(vals) => {
            let mut result = vec![0i64; num_rows];
            for (sorted_pos, &orig_row) in sort_indices.iter().enumerate() {
                result[orig_row] = vals[sorted_pos];
            }
            Ok(Arc::new(Int64Array::from(result)))
        }
        WindowValues::Float64(vals) => {
            let mut result = vec![None::<f64>; num_rows];
            for (sorted_pos, &orig_row) in sort_indices.iter().enumerate() {
                result[orig_row] = vals[sorted_pos];
            }
            Ok(Arc::new(Float64Array::from(result)))
        }
    }
}

fn compute_sort_indices(
    batch: &RecordBatch,
    schema: &SchemaRef,
    partition_by: &[Expr],
    order_by: &[SortExpr],
) -> Result<Vec<usize>> {
    let num_rows = batch.num_rows();
    let mut sort_columns: Vec<SortColumn> = Vec::new();

    for pb_expr in partition_by {
        let arr = evaluate_expr(batch, pb_expr)?;
        sort_columns.push(SortColumn {
            values: arr,
            options: Some(SortOptions {
                descending: false,
                nulls_first: true,
            }),
        });
    }

    for ob in order_by {
        let arr = evaluate_expr(batch, &ob.expr)?;
        let descending = ob.direction == SortDirection::Desc;
        let nulls_first = ob.nulls == NullOrdering::NullsFirst;
        sort_columns.push(SortColumn {
            values: arr,
            options: Some(SortOptions {
                descending,
                nulls_first,
            }),
        });
    }

    if sort_columns.is_empty() {
        return Ok((0..num_rows).collect());
    }

    let indices = lexsort_to_indices(&sort_columns, None)
        .map_err(|e| QueryError::Execution(format!("Window sort error: {}", e)))?;

    Ok(indices.values().iter().map(|&i| i as usize).collect())
}

fn same_partition(
    batch: &RecordBatch,
    schema: &SchemaRef,
    partition_by: &[Expr],
    row_i: usize,
    row_j: usize,
) -> Result<bool> {
    if partition_by.is_empty() {
        return Ok(true);
    }
    for pb_expr in partition_by {
        let arr = evaluate_expr(batch, pb_expr)?;
        if array_scalar_at(&arr, row_i) != array_scalar_at(&arr, row_j) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn same_order(
    batch: &RecordBatch,
    schema: &SchemaRef,
    order_by: &[SortExpr],
    row_i: usize,
    row_j: usize,
) -> Result<bool> {
    if order_by.is_empty() {
        return Ok(true);
    }
    for ob in order_by {
        let arr = evaluate_expr(batch, &ob.expr)?;
        if array_scalar_at(&arr, row_i) != array_scalar_at(&arr, row_j) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn compute_row_number(
    sort_indices: &[usize],
    batch: &RecordBatch,
    schema: &SchemaRef,
    partition_by: &[Expr],
) -> Result<WindowValues> {
    let n = sort_indices.len();
    let mut result = vec![0i64; n];
    let mut row_num: i64 = 1;
    for i in 0..n {
        if i > 0
            && !same_partition(
                batch,
                schema,
                partition_by,
                sort_indices[i - 1],
                sort_indices[i],
            )?
        {
            row_num = 1;
        }
        result[i] = row_num;
        row_num += 1;
    }
    Ok(WindowValues::Int64(result))
}

fn compute_rank(
    sort_indices: &[usize],
    batch: &RecordBatch,
    schema: &SchemaRef,
    partition_by: &[Expr],
    order_by: &[SortExpr],
    dense: bool,
) -> Result<WindowValues> {
    let n = sort_indices.len();
    let mut result = vec![0i64; n];
    let mut rank: i64 = 1;
    let mut dense_rank: i64 = 1;
    let mut count: i64 = 0;

    for i in 0..n {
        let new_partition = i > 0
            && !same_partition(
                batch,
                schema,
                partition_by,
                sort_indices[i - 1],
                sort_indices[i],
            )?;

        if new_partition {
            rank = 1;
            dense_rank = 1;
            count = 0;
        }

        let new_tie_group = i > 0
            && !new_partition
            && !same_order(
                batch,
                schema,
                order_by,
                sort_indices[i - 1],
                sort_indices[i],
            )?;

        if new_tie_group {
            rank += count;
            dense_rank += 1;
            count = 0;
        }

        result[i] = if dense { dense_rank } else { rank };
        count += 1;
    }
    Ok(WindowValues::Int64(result))
}

fn compute_lag_lead(
    sort_indices: &[usize],
    batch: &RecordBatch,
    schema: &SchemaRef,
    partition_by: &[Expr],
    args: &[Expr],
    is_lead: bool,
) -> Result<WindowValues> {
    let n = sort_indices.len();

    let offset: usize = if args.len() > 1 {
        match &args[1] {
            Expr::Literal(ScalarValue::Int64(v)) => *v as usize,
            Expr::Literal(ScalarValue::Int32(v)) => *v as usize,
            _ => 1,
        }
    } else {
        1
    };

    let value_arr = if args.is_empty() {
        None
    } else {
        Some(evaluate_expr(batch, &args[0])?)
    };

    let mut result = vec![None::<f64>; n];

    for i in 0..n {
        let target_pos = if is_lead {
            if i + offset < n {
                Some(i + offset)
            } else {
                None
            }
        } else {
            i.checked_sub(offset)
        };

        if let Some(pos) = target_pos {
            let same_part = same_partition(
                batch,
                schema,
                partition_by,
                sort_indices[i],
                sort_indices[pos],
            )?;
            if same_part {
                if let Some(ref arr) = value_arr {
                    result[i] = array_f64_at(arr, sort_indices[pos]);
                }
            }
        }
    }

    Ok(WindowValues::Float64(result))
}

fn compute_aggregate_window(
    func: &WindowFunction,
    sort_indices: &[usize],
    batch: &RecordBatch,
    schema: &SchemaRef,
    partition_by: &[Expr],
    args: &[Expr],
) -> Result<WindowValues> {
    let n = sort_indices.len();

    let value_arr = if args.is_empty() {
        None
    } else {
        Some(evaluate_expr(batch, &args[0])?)
    };

    // Find partition boundaries
    let mut partition_starts: Vec<usize> = vec![0];
    for i in 1..n {
        if !same_partition(
            batch,
            schema,
            partition_by,
            sort_indices[i - 1],
            sort_indices[i],
        )? {
            partition_starts.push(i);
        }
    }
    partition_starts.push(n);

    let mut result = vec![None::<f64>; n];

    for w in partition_starts.windows(2) {
        let start = w[0];
        let end = w[1];

        let agg_val = match func {
            WindowFunction::Count => Some((end - start) as f64),
            WindowFunction::Sum | WindowFunction::Avg => {
                let mut sum = 0.0f64;
                let mut cnt = 0usize;
                for pos in start..end {
                    if let Some(ref arr) = value_arr {
                        if let Some(v) = array_f64_at(arr, sort_indices[pos]) {
                            sum += v;
                            cnt += 1;
                        }
                    }
                }
                if cnt == 0 {
                    None
                } else if matches!(func, WindowFunction::Avg) {
                    Some(sum / cnt as f64)
                } else {
                    Some(sum)
                }
            }
            WindowFunction::Min => {
                let mut min_val: Option<f64> = None;
                for pos in start..end {
                    if let Some(ref arr) = value_arr {
                        if let Some(v) = array_f64_at(arr, sort_indices[pos]) {
                            min_val = Some(min_val.map_or(v, |m: f64| m.min(v)));
                        }
                    }
                }
                min_val
            }
            WindowFunction::Max => {
                let mut max_val: Option<f64> = None;
                for pos in start..end {
                    if let Some(ref arr) = value_arr {
                        if let Some(v) = array_f64_at(arr, sort_indices[pos]) {
                            max_val = Some(max_val.map_or(v, |m: f64| m.max(v)));
                        }
                    }
                }
                max_val
            }
            _ => None,
        };

        for pos in start..end {
            result[pos] = agg_val;
        }
    }

    Ok(WindowValues::Float64(result))
}

fn array_scalar_at(arr: &ArrayRef, idx: usize) -> ScalarValue {
    use arrow::array::*;
    if arr.is_null(idx) {
        return ScalarValue::Null;
    }
    match arr.data_type() {
        DataType::Int32 => ScalarValue::Int32(
            arr.as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(idx),
        ),
        DataType::Int64 => ScalarValue::Int64(
            arr.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(idx),
        ),
        DataType::Float64 => ScalarValue::Float64(ordered_float::OrderedFloat(
            arr.as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(idx),
        )),
        DataType::Utf8 => ScalarValue::Utf8(
            arr.as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(idx)
                .to_string(),
        ),
        DataType::Date32 => ScalarValue::Date32(
            arr.as_any()
                .downcast_ref::<arrow::array::Date32Array>()
                .unwrap()
                .value(idx),
        ),
        _ => ScalarValue::Null,
    }
}

fn array_f64_at(arr: &ArrayRef, idx: usize) -> Option<f64> {
    use arrow::array::*;
    if arr.is_null(idx) {
        return None;
    }
    match arr.data_type() {
        DataType::Int32 => arr
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|a| a.value(idx) as f64),
        DataType::Int64 => arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .map(|a| a.value(idx) as f64),
        DataType::Float32 => arr
            .as_any()
            .downcast_ref::<Float32Array>()
            .map(|a| a.value(idx) as f64),
        DataType::Float64 => arr
            .as_any()
            .downcast_ref::<Float64Array>()
            .map(|a| a.value(idx)),
        _ => None,
    }
}
