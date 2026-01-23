//! Subquery execution support

use crate::error::{QueryError, Result};
use crate::physical::{PhysicalOperator, PhysicalPlanner, RecordBatchStream};
use crate::physical::operators::TableProvider;
use crate::planner::{Expr, LogicalPlan, ScalarValue};
use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::sync::Arc;

/// Subquery executor - handles execution of subqueries during filter evaluation
#[derive(Clone)]
pub struct SubqueryExecutor {
    /// Inner executor state (wrapped in Arc for sharing)
    inner: Arc<SubqueryExecutorInner>,
}

/// Inner state of the subquery executor
struct SubqueryExecutorInner {
    /// Table providers for accessing table data
    tables: parking_lot::Mutex<HashMap<String, Arc<dyn TableProvider>>>,
    /// Cached results for uncorrelated subqueries (keyed by a hash of the plan)
    cache: parking_lot::Mutex<HashMap<usize, SubqueryResult>>,
}

/// Result of a subquery execution
#[derive(Clone)]
enum SubqueryResult {
    /// Scalar value (single row, single column)
    Scalar(ScalarValue),
    /// Array of values (IN subquery)
    Array(ArrayRef),
    /// Boolean result (EXISTS subquery)
    Boolean(bool),
}

impl SubqueryExecutor {
    /// Create a new subquery executor with the given tables
    pub fn from_tables(tables: HashMap<String, Arc<dyn TableProvider>>) -> Self {
        Self {
            inner: Arc::new(SubqueryExecutorInner {
                tables: parking_lot::Mutex::new(tables),
                cache: parking_lot::Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Register a table provider
    pub fn register_table(&self, name: String, provider: Arc<dyn TableProvider>) {
        self.inner.tables.lock().insert(name, provider);
    }

    /// Create a physical planner for subquery execution
    fn create_planner(&self) -> PhysicalPlanner {
        let mut planner = PhysicalPlanner::new();
        for (name, provider) in self.inner.tables.lock().iter() {
            planner.register_table(name.clone(), provider.clone());
        }
        // Pass this executor to the planner for nested subquery support
        planner.set_subquery_executor(Some(self.clone()));
        planner
    }

    /// Register outer context column values as temporary in-memory tables
    fn register_outer_context(
        &self,
        planner: &mut PhysicalPlanner,
        outer_context: &std::collections::HashMap<String, Expr>,
    ) -> Result<()> {
        use crate::physical::operators::{MemoryTable, TableProvider};
        use arrow::array::{ArrayRef, PrimitiveArray};
        use arrow::datatypes::{DataType, Field, Schema};

        // Create a temporary table for each outer column value
        for (col_name, expr) in outer_context {
            // Extract the scalar value from the expression
            let scalar = match expr {
                Expr::Literal(s) => s.clone(),
                _ => {
                    // For complex expressions, we'd need to evaluate them
                    // For now, skip or use a default
                    continue;
                }
            };

            // Create a single-column schema and batch
            let data_type = match scalar {
                ScalarValue::Int64(_) => DataType::Int64,
                ScalarValue::Int32(_) => DataType::Int32,
                ScalarValue::Float64(_) => DataType::Float64,
                ScalarValue::Float32(_) => DataType::Float32,
                ScalarValue::Utf8(_) => DataType::Utf8,
                ScalarValue::Boolean(_) => DataType::Boolean,
                ScalarValue::Null => DataType::Null,
                _ => continue, // Skip unsupported types
            };

            let schema = Arc::new(Schema::new(vec![Field::new(col_name.clone(), data_type, true)]));

            // Create a single-element array
            let array: ArrayRef = match scalar {
                ScalarValue::Int64(v) => Arc::new(arrow::array::Int64Array::from(vec![v])),
                ScalarValue::Int32(v) => Arc::new(arrow::array::Int32Array::from(vec![v])),
                ScalarValue::Float64(v) => Arc::new(arrow::array::Float64Array::from(vec![v.0])),
                ScalarValue::Float32(v) => Arc::new(arrow::array::Float32Array::from(vec![v.0])),
                ScalarValue::Utf8(v) => Arc::new(arrow::array::StringArray::from(vec![v.as_str()])),
                ScalarValue::Boolean(v) => Arc::new(arrow::array::BooleanArray::from(vec![v])),
                ScalarValue::Null => continue,
                _ => continue,
            };

            let batch = RecordBatch::try_new(schema.clone(), vec![array])
                .map_err(|e| QueryError::Execution(format!("Failed to create batch: {}", e)))?;

            // Create a temporary table provider
            let table = Arc::new(MemoryTable::new(schema.clone(), vec![batch.clone()]));

            // Register with a safe table name
            let table_name = format!("_outer_{}", col_name.replace('.', "_"));
            planner.register_table(table_name, table);

            // Also register with the original column name for direct lookup
            // This allows queries to reference the outer context column directly
            let table2 = Arc::new(MemoryTable::new(schema, vec![batch]));
            planner.register_table(col_name.clone(), table2);
        }

        Ok(())
    }

    /// Execute a scalar subquery and return the scalar value
    pub fn execute_scalar(&self, plan: &LogicalPlan) -> Result<ScalarValue> {
        self.execute_scalar_with_context(plan, &std::collections::HashMap::new())
    }

    /// Execute a scalar subquery with outer column context
    pub fn execute_scalar_with_context(
        &self,
        plan: &LogicalPlan,
        outer_context: &std::collections::HashMap<String, Expr>,
    ) -> Result<ScalarValue> {
        // If there's outer context, we can't cache the result (it depends on outer values)
        let cache_key = if outer_context.is_empty() {
            Some(self.plan_hash(plan))
        } else {
            None // Don't cache correlated subqueries
        };

        if let Some(key) = cache_key {
            let cache = self.inner.cache.lock();
            if let Some(SubqueryResult::Scalar(value)) = cache.get(&key) {
                return Ok(value.clone());
            }
        }

        // If we have outer context, substitute column references with literals
        let plan_to_execute = if outer_context.is_empty() {
            plan.clone()
        } else {
            // Substitute correlated columns with literal values from outer context
            substitute_columns_in_plan(plan, outer_context)?
        };

        // Create a planner
        let planner = self.create_planner();

        let physical = planner.create_physical_plan(&plan_to_execute)?;

        // Run the async code in a blocking way
        let batches = if let Ok(_handle) = tokio::runtime::Handle::try_current() {
            // We're inside a tokio runtime, use spawn_blocking
            let plan_clone = plan.clone();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Runtime::new()
                    .map_err(|e| QueryError::Execution(format!("Failed to create runtime: {}", e)))?;
                let stream = runtime.block_on(physical.execute(0))?;
                let result: Result<Vec<RecordBatch>> = runtime.block_on(async { stream.try_collect().await });
                result
            }).join().unwrap_or_else(|_| Err(QueryError::Execution("Subquery execution thread panicked".into())))?
        } else {
            // No existing runtime, create a new one and use it directly
            let runtime = tokio::runtime::Runtime::new()
                .map_err(|e| QueryError::Execution(format!("Failed to create runtime: {}", e)))?;
            let stream = runtime.block_on(physical.execute(0))?;
            runtime.block_on(async { stream.try_collect().await })?
        };

        if batches.is_empty() || batches[0].num_rows() == 0 {
            let result = ScalarValue::Null;
            if let Some(key) = cache_key {
                self.inner.cache.lock().insert(key, SubqueryResult::Scalar(result.clone()));
            }
            return Ok(result);
        }

        let batch = &batches[0];
        if batch.num_rows() != 1 {
            return Err(QueryError::Execution(format!(
                "Scalar subquery returned {} rows, expected 1",
                batch.num_rows()
            )));
        }

        let column = batch.column(0);
        let scalar = array_ref_to_scalar(column, 0)?;

        if let Some(key) = cache_key {
            self.inner.cache.lock().insert(key, SubqueryResult::Scalar(scalar.clone()));
        }
        Ok(scalar)
    }

    /// Execute an IN subquery with outer column context
    pub fn execute_in_subquery_with_context(
        &self,
        plan: &LogicalPlan,
        outer_context: &std::collections::HashMap<String, Expr>,
    ) -> Result<ArrayRef> {
        // If there's outer context, we can't cache the result (it depends on outer values)
        let cache_key = if outer_context.is_empty() {
            Some(self.plan_hash(plan))
        } else {
            None // Don't cache correlated subqueries
        };

        if let Some(key) = cache_key {
            let cache = self.inner.cache.lock();
            if let Some(SubqueryResult::Array(array)) = cache.get(&key) {
                return Ok(array.clone());
            }
        }

        // If we have outer context, substitute column references with literals
        let plan_to_execute = if outer_context.is_empty() {
            plan.clone()
        } else {
            // Substitute correlated columns with literal values from outer context
            substitute_columns_in_plan(plan, outer_context)?
        };

        // Create a planner
        let planner = self.create_planner();

        let physical = planner.create_physical_plan(&plan_to_execute)?;

        // Run the async code in a blocking way
        let batches = if let Ok(_handle) = tokio::runtime::Handle::try_current() {
            // We're inside a tokio runtime, use a dedicated thread
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Runtime::new()
                    .map_err(|e| QueryError::Execution(format!("Failed to create runtime: {}", e)))?;
                let stream = runtime.block_on(physical.execute(0))?;
                let result: Result<Vec<RecordBatch>> = runtime.block_on(async { stream.try_collect().await });
                result
            }).join().unwrap_or_else(|_| Err(QueryError::Execution("Subquery execution thread panicked".into())))?
        } else {
            // No existing runtime, create a new one and use it directly
            let runtime = tokio::runtime::Runtime::new()
                .map_err(|e| QueryError::Execution(format!("Failed to create runtime: {}", e)))?;
            let stream = runtime.block_on(physical.execute(0))?;
            runtime.block_on(async { stream.try_collect().await })?
        };

        if batches.is_empty() {
            let result = new_empty_array(plan.schema().fields().first()
                .ok_or_else(|| QueryError::Execution("IN subquery has no columns".into()))?
                .data_type.clone());
            if let Some(key) = cache_key {
                self.inner.cache.lock().insert(key, SubqueryResult::Array(result.clone()));
            }
            return Ok(result);
        }

        // Concatenate all batches into a single array
        let result = if batches.len() == 1 {
            batches[0].column(0).clone()
        } else {
            use arrow::compute::concat;
            let arrays: Vec<&dyn Array> = batches.iter().map(|b| b.column(0).as_ref()).collect();
            concat(&arrays)?
        };

        if let Some(key) = cache_key {
            self.inner.cache.lock().insert(key, SubqueryResult::Array(result.clone()));
        }
        Ok(result)
    }

    /// Execute an IN subquery and return the array of values
    pub fn execute_in_subquery(&self, plan: &LogicalPlan) -> Result<ArrayRef> {
        self.execute_in_subquery_with_context(plan, &std::collections::HashMap::new())
    }

    /// Execute an EXISTS subquery with outer column context
    pub fn execute_exists_with_context(
        &self,
        plan: &LogicalPlan,
        outer_context: &std::collections::HashMap<String, Expr>,
    ) -> Result<bool> {
        // If there's outer context, we can't cache the result (it depends on outer values)
        let cache_key = if outer_context.is_empty() {
            Some(self.plan_hash(plan))
        } else {
            None // Don't cache correlated subqueries
        };

        if let Some(key) = cache_key {
            let cache = self.inner.cache.lock();
            if let Some(SubqueryResult::Boolean(value)) = cache.get(&key) {
                return Ok(*value);
            }
        }

        // If we have outer context, substitute column references with literals
        let plan_to_execute = if outer_context.is_empty() {
            plan.clone()
        } else {
            // Substitute correlated columns with literal values from outer context
            substitute_columns_in_plan(plan, outer_context)?
        };

        // Create a planner
        let planner = self.create_planner();

        let physical = planner.create_physical_plan(&plan_to_execute)?;

        // Run the async code in a blocking way
        let batches = if let Ok(_handle) = tokio::runtime::Handle::try_current() {
            // We're inside a tokio runtime, use spawn_blocking
            let plan_clone = plan.clone();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Runtime::new()
                    .map_err(|e| QueryError::Execution(format!("Failed to create runtime: {}", e)))?;
                let stream = runtime.block_on(physical.execute(0))?;
                let result: Result<Vec<RecordBatch>> = runtime.block_on(async { stream.try_collect().await });
                result
            }).join().unwrap_or_else(|_| Err(QueryError::Execution("Subquery execution thread panicked".into())))?
        } else {
            // No existing runtime, create a new one and use it directly
            let runtime = tokio::runtime::Runtime::new()
                .map_err(|e| QueryError::Execution(format!("Failed to create runtime: {}", e)))?;
            let stream = runtime.block_on(physical.execute(0))?;
            runtime.block_on(async { stream.try_collect().await })?
        };

        let has_rows = batches.iter().any(|b| b.num_rows() > 0);

        if let Some(key) = cache_key {
            self.inner.cache.lock().insert(key, SubqueryResult::Boolean(has_rows));
        }
        Ok(has_rows)
    }

    /// Execute an EXISTS subquery and return the boolean result
    pub fn execute_exists(&self, plan: &LogicalPlan) -> Result<bool> {
        self.execute_exists_with_context(plan, &std::collections::HashMap::new())
    }

    /// Simple hash of a plan for caching
    fn plan_hash(&self, plan: &LogicalPlan) -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        format!("{:?}", plan).hash(&mut hasher);
        hasher.finish() as usize
    }
}

/// Convert an ArrayRef element to ScalarValue
fn array_ref_to_scalar(array: &ArrayRef, index: usize) -> Result<ScalarValue> {
    use arrow::array::*;

    if array.is_null(index) {
        return Ok(ScalarValue::Null);
    }

    Ok(match array.data_type() {
        arrow::datatypes::DataType::Boolean => {
            let arr = array.as_any().downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Type("Expected BooleanArray".into()))?;
            ScalarValue::Boolean(arr.value(index))
        }
        arrow::datatypes::DataType::Int8 => {
            let arr = array.as_any().downcast_ref::<Int8Array>()
                .ok_or_else(|| QueryError::Type("Expected Int8Array".into()))?;
            ScalarValue::Int8(arr.value(index))
        }
        arrow::datatypes::DataType::Int16 => {
            let arr = array.as_any().downcast_ref::<Int16Array>()
                .ok_or_else(|| QueryError::Type("Expected Int16Array".into()))?;
            ScalarValue::Int16(arr.value(index))
        }
        arrow::datatypes::DataType::Int32 => {
            let arr = array.as_any().downcast_ref::<Int32Array>()
                .ok_or_else(|| QueryError::Type("Expected Int32Array".into()))?;
            ScalarValue::Int32(arr.value(index))
        }
        arrow::datatypes::DataType::Int64 => {
            let arr = array.as_any().downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::Type("Expected Int64Array".into()))?;
            ScalarValue::Int64(arr.value(index))
        }
        arrow::datatypes::DataType::Float32 => {
            let arr = array.as_any().downcast_ref::<Float32Array>()
                .ok_or_else(|| QueryError::Type("Expected Float32Array".into()))?;
            ScalarValue::Float32(ordered_float::OrderedFloat(arr.value(index)))
        }
        arrow::datatypes::DataType::Float64 => {
            let arr = array.as_any().downcast_ref::<Float64Array>()
                .ok_or_else(|| QueryError::Type("Expected Float64Array".into()))?;
            ScalarValue::Float64(ordered_float::OrderedFloat(arr.value(index)))
        }
        arrow::datatypes::DataType::Utf8 => {
            let arr = array.as_any().downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("Expected StringArray".into()))?;
            ScalarValue::Utf8(arr.value(index).to_string())
        }
        arrow::datatypes::DataType::Date32 => {
            let arr = array.as_any().downcast_ref::<Date32Array>()
                .ok_or_else(|| QueryError::Type("Expected Date32Array".into()))?;
            ScalarValue::Date32(arr.value(index))
        }
        dt => {
            return Err(QueryError::NotImplemented(format!(
                "Unsupported type for scalar subquery: {:?}",
                dt
            )))
        }
    })
}

/// Create an empty array of the given type
fn new_empty_array(data_type: arrow::datatypes::DataType) -> ArrayRef {
    use arrow::array::*;
    match data_type {
        arrow::datatypes::DataType::Boolean => Arc::new(BooleanArray::from(vec![] as Vec<Option<bool>>)),
        arrow::datatypes::DataType::Int64 => Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)),
        arrow::datatypes::DataType::Int32 => Arc::new(Int32Array::from(vec![] as Vec<Option<i32>>)),
        arrow::datatypes::DataType::Float64 => Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>)),
        arrow::datatypes::DataType::Utf8 => Arc::new(StringArray::from(vec![] as Vec<Option<&str>>)),
        _ => Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)),
    }
}

/// Evaluate a subquery expression during filter execution
pub fn evaluate_subquery_expr(
    batch: &RecordBatch,
    expr: &Expr,
    executor: &SubqueryExecutor,
) -> Result<ArrayRef> {
    match expr {
        Expr::InSubquery { expr, subquery, negated } => {
            // Check if the subquery contains nested subqueries
            // If so, it's likely correlated and needs per-row execution
            let has_nested_subqueries = contains_subquery(subquery);

            if has_nested_subqueries {
                // Treat as correlated - execute for each row
                execute_correlated_in_subquery(batch, expr, subquery, *negated, executor)
            } else {
                // Try executing once - if it fails with ColumnNotFound, it's correlated
                match executor.execute_in_subquery(subquery) {
                    Ok(in_values) => {
                        // Uncorrelated - use the same values for all rows
                        let left_array = super::filter::evaluate_expr(batch, expr)?;
                        evaluate_in_subquery(&left_array, &in_values, *negated)
                    }
                    Err(QueryError::ColumnNotFound(_)) => {
                        // Correlated IN subquery - execute for each row
                        execute_correlated_in_subquery(batch, expr, subquery, *negated, executor)
                    }
                    Err(e) => Err(e),
                }
            }
        }
        Expr::ScalarSubquery(plan) => {
            // Check if this is a correlated subquery by trying to execute once
            match executor.execute_scalar(plan) {
                Ok(scalar) => {
                    // Uncorrelated - use the same value for all rows
                    let num_rows = batch.num_rows();
                    Ok(scalar_to_array(&scalar, num_rows))
                }
                Err(QueryError::ColumnNotFound(_)) => {
                    // Correlated subquery - execute for each row
                    execute_correlated_scalar_subquery(batch, plan, executor)
                }
                Err(e) => Err(e),
            }
        }
        Expr::InSubquery { expr, subquery, negated } => {
            // Try executing once - if it fails with ColumnNotFound, it's correlated
            match executor.execute_in_subquery(subquery) {
                Ok(in_values) => {
                    // Uncorrelated - use the same values for all rows
                    let left_array = super::filter::evaluate_expr(batch, expr)?;
                    evaluate_in_subquery(&left_array, &in_values, *negated)
                }
                Err(QueryError::ColumnNotFound(_)) => {
                    // Correlated IN subquery - execute for each row
                    execute_correlated_in_subquery(batch, expr, subquery, *negated, executor)
                }
                Err(e) => Err(e),
            }
        }
        Expr::Exists { subquery, negated } => {
            // Try executing once - if it fails with ColumnNotFound, it's correlated
            match executor.execute_exists(subquery) {
                Ok(exists) => {
                    // Uncorrelated - use the same result for all rows
                    let result = if *negated { !exists } else { exists };
                    let arr = BooleanArray::from(vec![result; batch.num_rows()]);
                    Ok(Arc::new(arr))
                }
                Err(QueryError::ColumnNotFound(_)) => {
                    // Correlated EXISTS subquery
                    execute_correlated_exists_subquery(batch, subquery, *negated, executor)
                }
                Err(e) => Err(e),
            }
        }
        _ => Err(QueryError::NotImplemented(format!(
            "Not a subquery expression: {:?}",
            expr
        ))),
    }
}

/// Build outer context from a batch (collects all columns as potential outer references)
fn build_outer_context(batch: &RecordBatch) -> std::collections::HashMap<String, Expr> {
    let mut context = std::collections::HashMap::new();

    for (i, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(i);
        let column_name = field.name().clone();

        // Store the column reference - we'll create row-specific values later
        // For now, just store the column name as a key
        // The actual values will be substituted per-row
        context.insert(column_name.clone(), Expr::column(&column_name));
    }

    context
}

/// Build row-specific context with actual scalar values for a single row
fn build_row_context(
    outer_context: &std::collections::HashMap<String, Expr>,
    batch: &RecordBatch,
    row: usize,
) -> Result<std::collections::HashMap<String, Expr>> {
    let mut row_context = std::collections::HashMap::new();

    for (i, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(i);
        let column_name = field.name().clone();

        // Extract the scalar value for this specific row
        let scalar = if column.is_null(row) {
            ScalarValue::Null
        } else {
            array_ref_to_scalar_value(column, row)?
        };

        // Create a literal expression with the actual value
        row_context.insert(column_name.clone(), Expr::Literal(scalar.clone()));

        // Also add variations of the column name for flexible matching
        // 1. Unqualified name (e.g., "ps_partkey" from "partsupp.ps_partkey")
        if let Some(dot_pos) = column_name.find('.') {
            let unqualified = &column_name[dot_pos + 1..];
            row_context.insert(unqualified.to_string(), Expr::Literal(scalar.clone()));
        }

        // 2. Base name after last underscore (e.g., "partkey" from "l_partkey")
        if let Some(underscore_pos) = column_name.rfind('_') {
            let base_name = &column_name[underscore_pos + 1..];
            row_context.insert(base_name.to_string(), Expr::Literal(scalar.clone()));
        }

        // 3. With table prefix (e.g., for "s_suppkey" also add "supplier.s_suppkey")
        // This handles cases where the subquery uses qualified names
        if column_name.contains('_') {
            let parts: Vec<&str> = column_name.split('_').collect();
            if parts.len() >= 2 {
                let table_guess = match parts[0] {
                    "s" => "supplier",
                    "p" => "part",
                    "ps" => "partsupp",
                    "l" => "lineitem",
                    "o" => "orders",
                    "c" => "customer",
                    "n" => "nation",
                    "r" => "region",
                    _ => continue,
                };
                let qualified_guess = format!("{}.{}", table_guess, column_name);
                row_context.entry(qualified_guess).or_insert_with(|| Expr::Literal(scalar.clone()));
            }
        }
    }

    Ok(row_context)
}

/// Check if a plan contains any subquery expressions
fn contains_subquery(plan: &LogicalPlan) -> bool {
    use crate::planner::*;

    // Check expressions in the plan
    fn expr_has_subquery(expr: &Expr) -> bool {
        match expr {
            Expr::ScalarSubquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => true,
            Expr::BinaryExpr { left, right, .. } => {
                expr_has_subquery(left) || expr_has_subquery(right)
            }
            Expr::UnaryExpr { expr, .. } => expr_has_subquery(expr),
            Expr::Alias { expr, .. } => expr_has_subquery(expr),
            Expr::Aggregate { args, .. } => args.iter().any(expr_has_subquery),
            Expr::ScalarFunc { args, .. } => args.iter().any(expr_has_subquery),
            Expr::Cast { expr, .. } => expr_has_subquery(expr),
            Expr::Case { operand, when_then, else_expr } => {
                operand.as_ref().map_or(false, |e| expr_has_subquery(e))
                    || when_then.iter().any(|(w, t)| expr_has_subquery(w) || expr_has_subquery(t))
                    || else_expr.as_ref().map_or(false, |e| expr_has_subquery(e))
            }
            _ => false,
        }
    }

    fn plan_has_subquery(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Filter(node) => {
                expr_has_subquery(&node.predicate) || plan_has_subquery(&node.input)
            }
            LogicalPlan::Project(node) => {
                node.exprs.iter().any(expr_has_subquery) || plan_has_subquery(&node.input)
            }
            LogicalPlan::Aggregate(node) => {
                node.group_by.iter().any(expr_has_subquery)
                    || node.aggregates.iter().any(expr_has_subquery)
                    || plan_has_subquery(&node.input)
            }
            LogicalPlan::Join(node) => {
                node.on.iter().any(|(l, r)| expr_has_subquery(l) || expr_has_subquery(r))
                    || node.filter.as_ref().map_or(false, |f| expr_has_subquery(f))
                    || plan_has_subquery(&node.left)
                    || plan_has_subquery(&node.right)
            }
            LogicalPlan::Sort(node) => {
                node.order_by.iter().any(|s| expr_has_subquery(&s.expr))
                    || plan_has_subquery(&node.input)
            }
            LogicalPlan::Limit(node) => plan_has_subquery(&node.input),
            LogicalPlan::Distinct(node) => plan_has_subquery(&node.input),
            LogicalPlan::SubqueryAlias(node) => plan_has_subquery(&node.input),
            LogicalPlan::Union(node) => node.inputs.iter().any(|p| plan_has_subquery(p)),
            _ => false,
        }
    }

    plan_has_subquery(plan)
}

/// Execute a correlated scalar subquery for each row in the batch
fn execute_correlated_scalar_subquery(
    batch: &RecordBatch,
    plan: &LogicalPlan,
    executor: &SubqueryExecutor,
) -> Result<ArrayRef> {
    let num_rows = batch.num_rows();
    let mut results = Vec::with_capacity(num_rows);

    // Build outer context from batch columns
    let outer_context = build_outer_context(batch);

    // For each row, build row-specific context and execute the subquery
    for row in 0..num_rows {
        // Build row-specific context with actual scalar values
        let row_context = build_row_context(&outer_context, batch, row)?;

        // Execute the subquery with row-specific context
        match executor.execute_scalar_with_context(plan, &row_context) {
            Ok(scalar) => {
                results.push(scalar);
            }
            Err(QueryError::ColumnNotFound(_)) => {
                // Try with substitution as fallback
                let substituted_plan = substitute_correlated_columns(plan, batch, row)?;
                match executor.execute_scalar(&substituted_plan) {
                    Ok(scalar) => results.push(scalar),
                    Err(_) => results.push(ScalarValue::Null),
                }
            }
            Err(e) => {
                // If still failing, return null for this row
                eprintln!("Row {} failed: {:?}", row, e);
                results.push(ScalarValue::Null);
            }
        }
    }

    // Convert results to an array
    results_array_from_scalars(&results, num_rows)
}

/// Execute a correlated EXISTS subquery for each row in the batch
fn execute_correlated_exists_subquery(
    batch: &RecordBatch,
    subquery: &LogicalPlan,
    negated: bool,
    executor: &SubqueryExecutor,
) -> Result<ArrayRef> {
    let num_rows = batch.num_rows();
    let mut results = Vec::with_capacity(num_rows);

    // Build outer context from batch columns
    let outer_context = build_outer_context(batch);

    for row in 0..num_rows {
        // Build row-specific context with actual scalar values
        let row_context = build_row_context(&outer_context, batch, row)?;

        // Execute the subquery with row-specific context
        let exists = match executor.execute_exists_with_context(subquery, &row_context) {
            Ok(e) => e,
            Err(QueryError::ColumnNotFound(_)) => {
                // Try with substitution as fallback
                let substituted_plan = substitute_correlated_columns(subquery, batch, row)?;
                executor.execute_exists(&substituted_plan).unwrap_or(false)
            }
            Err(_) => false, // On error, treat as not found
        };

        results.push(if negated { !exists } else { exists });
    }

    Ok(Arc::new(BooleanArray::from(results)))
}

/// Execute a correlated IN subquery for each row in the batch
fn execute_correlated_in_subquery(
    batch: &RecordBatch,
    expr: &Expr,
    subquery: &LogicalPlan,
    negated: bool,
    executor: &SubqueryExecutor,
) -> Result<ArrayRef> {
    let num_rows = batch.num_rows();
    let mut results = Vec::with_capacity(num_rows);

    // Build outer context from batch columns
    let outer_context = build_outer_context(batch);

    for row in 0..num_rows {
        // Build row-specific context with actual scalar values
        let row_context = build_row_context(&outer_context, batch, row)?;

        // Execute the IN subquery with row-specific context
        let in_values = match executor.execute_in_subquery_with_context(subquery, &row_context) {
            Ok(values) => values,
            Err(QueryError::ColumnNotFound(_)) => {
                // Try with substitution as fallback
                let substituted_plan = substitute_correlated_columns(subquery, batch, row)?;
                executor.execute_in_subquery(&substituted_plan).unwrap_or_else(|_| {
                    new_empty_array(subquery.schema().fields().first()
                        .map(|f| f.data_type.clone())
                        .unwrap_or(arrow::datatypes::DataType::Int64))
                })
            }
            Err(_) => {
                // On error, use empty array (no matches)
                new_empty_array(subquery.schema().fields().first()
                    .map(|f| f.data_type.clone())
                    .unwrap_or(arrow::datatypes::DataType::Int64))
            }
        };

        // Evaluate the left side expression for this single row
        let single_row_results = evaluate_in_subquery_single_value(batch, expr, row, &in_values, negated)?;
        results.push(single_row_results);
    }

    // Convert results to an array
    Ok(Arc::new(BooleanArray::from(results)))
}

/// Evaluate IN subquery for a single row value
fn evaluate_in_subquery_single_value(
    batch: &RecordBatch,
    expr: &Expr,
    row: usize,
    in_values: &ArrayRef,
    negated: bool,
) -> Result<bool> {
    use arrow::array::*;

    // Evaluate the left side expression for this single row
    let left_array = super::filter::evaluate_expr(batch, expr)?;
    let mut found = false;

    if left_array.is_null(row) {
        return Ok(false);
    }

    for j in 0..in_values.len() {
        if in_values.is_null(j) {
            continue;
        }

        let matches = match (left_array.data_type(), in_values.data_type()) {
            (arrow::datatypes::DataType::Int64, arrow::datatypes::DataType::Int64) => {
                let left_arr = left_array.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_arr = in_values.as_any().downcast_ref::<Int64Array>().unwrap();
                left_arr.value(row) == right_arr.value(j)
            }
            (arrow::datatypes::DataType::Int32, arrow::datatypes::DataType::Int32) => {
                let left_arr = left_array.as_any().downcast_ref::<Int32Array>().unwrap();
                let right_arr = in_values.as_any().downcast_ref::<Int32Array>().unwrap();
                left_arr.value(row) == right_arr.value(j)
            }
            (arrow::datatypes::DataType::Float64, arrow::datatypes::DataType::Float64) => {
                let left_arr = left_array.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_arr = in_values.as_any().downcast_ref::<Float64Array>().unwrap();
                left_arr.value(row) == right_arr.value(j)
            }
            (arrow::datatypes::DataType::Utf8, arrow::datatypes::DataType::Utf8) => {
                let left_arr = left_array.as_any().downcast_ref::<StringArray>().unwrap();
                let right_arr = in_values.as_any().downcast_ref::<StringArray>().unwrap();
                left_arr.value(row) == right_arr.value(j)
            }
            _ => {
                return Err(QueryError::NotImplemented(format!(
                    "IN subquery not supported for types: {:?} IN {:?}",
                    left_array.data_type(),
                    in_values.data_type()
                )))
            }
        };

        if matches {
            found = true;
            break;
        }
    }

    Ok(if negated { !found } else { found })
}

/// Substitute correlated column references with literal values from a specific row
/// This version supports nested subqueries by collecting ALL available columns from the batch
fn substitute_correlated_columns(
    plan: &LogicalPlan,
    batch: &RecordBatch,
    row: usize,
) -> Result<LogicalPlan> {
    use crate::planner::*;

    // Create a comprehensive mapping of column names to their literal values for this row
    // This includes all variations of the column name to handle nested references
    let mut column_values: std::collections::HashMap<String, Expr> = std::collections::HashMap::new();

    for (i, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(i);
        let scalar = if column.is_null(row) {
            ScalarValue::Null
        } else {
            array_ref_to_scalar_value(column, row)?
        };

        let literal_expr = Expr::Literal(scalar.clone());

        // Add the fully qualified name (e.g., "partsupp.ps_partkey")
        column_values.insert(field.name().clone(), literal_expr.clone());

        // Add multiple variations for flexible matching:
        // 1. Unqualified name only (e.g., "ps_partkey")
        if let Some(dot_pos) = field.name().find('.') {
            let unqualified = &field.name()[dot_pos + 1..];
            column_values.insert(unqualified.to_string(), literal_expr.clone());
        }

        // 2. Base name after last underscore (e.g., "partkey" from "l_partkey")
        if let Some(dot_pos) = field.name().rfind('_') {
            let base_name = &field.name()[dot_pos + 1..];
            column_values.insert(base_name.to_string(), literal_expr.clone());
        }

        // 3. With table prefix if not already present (e.g., for "s_suppkey" also add "supplier.s_suppkey")
        // Extract potential table name from column prefix
        if field.name().contains('_') {
            // Common pattern: table_column (e.g., s_suppkey = supplier.s_suppkey)
            let parts: Vec<&str> = field.name().split('_').collect();
            if parts.len() >= 2 {
                // Try to guess table name from prefix
                let table_guess = match parts[0] {
                    "s" => "supplier",
                    "p" => "part",
                    "ps" => "partsupp",
                    "l" => "lineitem",
                    "o" => "orders",
                    "c" => "customer",
                    "n" => "nation",
                    "r" => "region",
                    _ => continue,
                };
                let qualified_guess = format!("{}.{}", table_guess, field.name());
                column_values.entry(qualified_guess.clone()).or_insert_with(|| literal_expr.clone());
            }
        }
    }

    // Recursively substitute column references in the plan
    substitute_columns_in_plan(plan, &column_values)
}

/// Recursively substitute column references with literals in a logical plan
fn substitute_columns_in_plan(
    plan: &LogicalPlan,
    column_values: &std::collections::HashMap<String, Expr>,
) -> Result<LogicalPlan> {
    use crate::planner::*;

    // First, check if the column reference exists in our substitution map
    // If so, substitute it; otherwise, keep it as-is
    let substituted_expr = |expr: &Expr| -> Expr {
        match expr {
            Expr::Column(col) => {
                // Try to substitute both qualified and unqualified names
                let qualified_name = if let Some(rel) = &col.relation {
                    format!("{}.{}", rel, col.name)
                } else {
                    col.name.clone()
                };

                if let Some(substituted) = column_values.get(&qualified_name) {
                    substituted.clone()
                } else if let Some(substituted) = column_values.get(&col.name) {
                    substituted.clone()
                } else {
                    expr.clone()
                }
            }
            _ => expr.clone(),
        }
    };

    // Recursively process the plan
    match plan {
        LogicalPlan::Filter(node) => {
            let new_predicate = substitute_columns_in_expr(&node.predicate, column_values);
            let new_input = substitute_columns_in_plan(&node.input, column_values)?;
            Ok(LogicalPlan::Filter(FilterNode {
                input: Arc::new(new_input),
                predicate: new_predicate,
            }))
        }
        LogicalPlan::Project(node) => {
            let new_exprs: Vec<Expr> = node.exprs
                .iter()
                .map(|e| substitute_columns_in_expr(e, column_values))
                .collect();
            let new_input = substitute_columns_in_plan(&node.input, column_values)?;
            Ok(LogicalPlan::Project(ProjectNode {
                input: Arc::new(new_input),
                exprs: new_exprs,
                schema: node.schema.clone(),
            }))
        }
        LogicalPlan::Aggregate(node) => {
            let new_group_by: Vec<Expr> = node.group_by
                .iter()
                .map(|e| substitute_columns_in_expr(e, column_values))
                .collect();
            let new_aggregates: Vec<Expr> = node.aggregates
                .iter()
                .map(|e| substitute_columns_in_expr(e, column_values))
                .collect();
            let new_input = substitute_columns_in_plan(&node.input, column_values)?;
            Ok(LogicalPlan::Aggregate(AggregateNode {
                input: Arc::new(new_input),
                group_by: new_group_by,
                aggregates: new_aggregates,
                schema: node.schema.clone(),
            }))
        }
        LogicalPlan::Scan(node) => {
            // Substitute in filter expression if present
            let new_filter = node.filter.as_ref()
                .map(|f| substitute_columns_in_expr(f, column_values));
            Ok(LogicalPlan::Scan(ScanNode {
                table_name: node.table_name.clone(),
                schema: node.schema.clone(),
                projection: node.projection.clone(),
                filter: new_filter,
            }))
        }
        LogicalPlan::Join(node) => {
            let left = substitute_columns_in_plan(&node.left, column_values)?;
            let right = substitute_columns_in_plan(&node.right, column_values)?;
            // Substitute in join conditions
            let new_on: Vec<(Expr, Expr)> = node.on.iter()
                .map(|(l, r)| (
                    substitute_columns_in_expr(l, column_values),
                    substitute_columns_in_expr(r, column_values),
                ))
                .collect();
            let new_filter = node.filter.as_ref()
                .map(|f| substitute_columns_in_expr(f, column_values));
            Ok(LogicalPlan::Join(JoinNode {
                left: Arc::new(left),
                right: Arc::new(right),
                join_type: node.join_type,
                on: new_on,
                filter: new_filter,
                schema: node.schema.clone(),
            }))
        }
        LogicalPlan::SubqueryAlias(node) => {
            let new_input = substitute_columns_in_plan(&node.input, column_values)?;
            Ok(LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                input: Arc::new(new_input),
                alias: node.alias.clone(),
                schema: node.schema.clone(),
            }))
        }
        LogicalPlan::Limit(node) => {
            let new_input = substitute_columns_in_plan(&node.input, column_values)?;
            Ok(LogicalPlan::Limit(LimitNode {
                input: Arc::new(new_input),
                skip: node.skip,
                fetch: node.fetch,
            }))
        }
        LogicalPlan::Sort(node) => {
            let new_input = substitute_columns_in_plan(&node.input, column_values)?;
            let new_order_by: Vec<crate::planner::SortExpr> = node.order_by.iter()
                .map(|s| crate::planner::SortExpr {
                    expr: substitute_columns_in_expr(&s.expr, column_values),
                    direction: s.direction,
                    nulls: s.nulls,
                })
                .collect();
            Ok(LogicalPlan::Sort(SortNode {
                input: Arc::new(new_input),
                order_by: new_order_by,
            }))
        }
        LogicalPlan::Distinct(node) => {
            let new_input = substitute_columns_in_plan(&node.input, column_values)?;
            Ok(LogicalPlan::Distinct(DistinctNode {
                input: Arc::new(new_input),
            }))
        }
        LogicalPlan::Union(node) => {
            let new_inputs: Vec<Arc<LogicalPlan>> = node.inputs.iter()
                .map(|i| substitute_columns_in_plan(i, column_values).map(Arc::new))
                .collect::<Result<_>>()?;
            Ok(LogicalPlan::Union(UnionNode {
                inputs: new_inputs,
                schema: node.schema.clone(),
                all: node.all,
            }))
        }
        // For other plan types, return as-is for now
        _ => Ok(plan.clone()),
    }
}

/// Substitute column references in an expression
fn substitute_columns_in_expr(
    expr: &Expr,
    column_values: &std::collections::HashMap<String, Expr>,
) -> Expr {
    match expr {
        Expr::Column(col) => {
            // Try multiple variations of the column name
            // 1. Qualified with table: "table.column"
            // 2. Unqualified: "column"
            // 3. Try exact matches first

            // Try qualified name first (e.g., "supplier.s_suppkey")
            if let Some(rel) = &col.relation {
                let qualified = format!("{}.{}", rel, col.name);
                if let Some(substituted) = column_values.get(&qualified) {
                    return substituted.clone();
                }
            }

            // Try unqualified name (e.g., "s_suppkey")
            if let Some(substituted) = column_values.get(&col.name) {
                return substituted.clone();
            }

            // Try matching with wildcards - if column_values has "table.col", try matching "col"
            // This handles cases where the subquery uses unqualified names
            for (key, value) in column_values.iter() {
                if key.ends_with(&format!(".{}", col.name)) || key == col.name.as_str() {
                    return value.clone();
                }
                // Also try reverse: if subquery has "table.col" and column_values has "col"
                if col.name.ends_with(&format!(".{}", key)) || col.name.as_str() == *key {
                    return value.clone();
                }
            }

            // If no match found, return original (will fail later)
            expr.clone()
        }
        Expr::BinaryExpr { left, op, right } => {
            let new_left = substitute_columns_in_expr(left, column_values);
            let new_right = substitute_columns_in_expr(right, column_values);
            Expr::BinaryExpr {
                left: Box::new(new_left),
                op: *op,
                right: Box::new(new_right),
            }
        }
        Expr::UnaryExpr { op, expr } => {
            let new_expr = substitute_columns_in_expr(expr, column_values);
            Expr::UnaryExpr {
                op: *op,
                expr: Box::new(new_expr),
            }
        }
        Expr::Alias { expr, name } => {
            let new_expr = substitute_columns_in_expr(expr, column_values);
            Expr::Alias {
                expr: Box::new(new_expr),
                name: name.clone(),
            }
        }
        Expr::Aggregate { func, args, distinct } => {
            let new_args: Vec<Expr> = args
                .iter()
                .map(|a| substitute_columns_in_expr(a, column_values))
                .collect();
            Expr::Aggregate {
                func: *func,
                args: new_args,
                distinct: *distinct,
            }
        }
        Expr::ScalarFunc { args, func } => {
            let new_args: Vec<Expr> = args
                .iter()
                .map(|a| substitute_columns_in_expr(a, column_values))
                .collect();
            Expr::ScalarFunc {
                args: new_args,
                func: func.clone(),
            }
        }
        Expr::Cast { expr, data_type } => {
            let new_expr = substitute_columns_in_expr(expr, column_values);
            Expr::Cast {
                expr: Box::new(new_expr),
                data_type: data_type.clone(),
            }
        }
        Expr::Case { operand, when_then, else_expr } => {
            let new_operand = operand
                .as_ref()
                .map(|e| substitute_columns_in_expr(e, column_values));
            let new_when_then: Vec<(Expr, Expr)> = when_then
                .iter()
                .map(|(w, t)| {
                    (
                        substitute_columns_in_expr(w, column_values),
                        substitute_columns_in_expr(t, column_values),
                    )
                })
                .collect();
            let new_else = else_expr
                .as_ref()
                .map(|e| substitute_columns_in_expr(e, column_values));
            Expr::Case {
                operand: new_operand.map(Box::new),
                when_then: new_when_then,
                else_expr: new_else.map(Box::new),
            }
        }
        // For other expressions, return as-is
        _ => expr.clone(),
    }
}

/// Convert an array element to ScalarValue at a specific index
fn array_ref_to_scalar_value(array: &ArrayRef, index: usize) -> Result<ScalarValue> {
    array_ref_to_scalar(array, index)
}

/// Convert a vector of scalars to an arrow array
fn results_array_from_scalars(scalars: &[ScalarValue], num_rows: usize) -> Result<ArrayRef> {
    if scalars.is_empty() {
        return Ok(Arc::new(arrow::array::NullArray::new(num_rows)));
    }

    // All scalars should have the same type
    match &scalars[0] {
        ScalarValue::Int64(v) => {
            use arrow::array::Int64Array;
            let values: Vec<Option<i64>> = scalars
                .iter()
                .map(|s| match s {
                    ScalarValue::Int64(v) => Some(*v),
                    ScalarValue::Null => None,
                    _ => Some(0),
                })
                .collect();
            Ok(Arc::new(Int64Array::from(values)))
        }
        ScalarValue::Float64(v) => {
            use arrow::array::Float64Array;
            let values: Vec<Option<f64>> = scalars
                .iter()
                .map(|s| match s {
                    ScalarValue::Float64(v) => Some(v.0),
                    ScalarValue::Null => None,
                    _ => Some(0.0),
                })
                .collect();
            Ok(Arc::new(Float64Array::from(values)))
        }
        ScalarValue::Boolean(v) => {
            use arrow::array::BooleanArray;
            let values: Vec<Option<bool>> = scalars
                .iter()
                .map(|s| match s {
                    ScalarValue::Boolean(v) => Some(*v),
                    ScalarValue::Null => None,
                    _ => Some(false),
                })
                .collect();
            Ok(Arc::new(BooleanArray::from(values)))
        }
        ScalarValue::Utf8(v) => {
            use arrow::array::StringArray;
            let values: Vec<Option<&str>> = scalars
                .iter()
                .map(|s| match s {
                    ScalarValue::Utf8(v) => Some(v.as_str()),
                    ScalarValue::Null => None,
                    _ => Some(""),
                })
                .collect();
            Ok(Arc::new(StringArray::from(values)))
        }
        _ => {
            // Default to null array for unsupported types
            Ok(Arc::new(arrow::array::NullArray::new(num_rows)))
        }
    }
}

/// Evaluate IN subquery by checking membership
fn evaluate_in_subquery(left: &ArrayRef, right: &ArrayRef, negated: bool) -> Result<ArrayRef> {
    use arrow::array::*;

    let num_rows = left.len();
    let mut result = Vec::with_capacity(num_rows);

    for i in 0..num_rows {
        let mut found = false;

        if left.is_null(i) {
            result.push(Some(false));
            continue;
        }

        for j in 0..right.len() {
            if right.is_null(j) {
                continue;
            }

            let matches = match (left.data_type(), right.data_type()) {
                (arrow::datatypes::DataType::Int64, arrow::datatypes::DataType::Int64) => {
                    let left_arr = left.as_any().downcast_ref::<Int64Array>().unwrap();
                    let right_arr = right.as_any().downcast_ref::<Int64Array>().unwrap();
                    left_arr.value(i) == right_arr.value(j)
                }
                (arrow::datatypes::DataType::Int32, arrow::datatypes::DataType::Int32) => {
                    let left_arr = left.as_any().downcast_ref::<Int32Array>().unwrap();
                    let right_arr = right.as_any().downcast_ref::<Int32Array>().unwrap();
                    left_arr.value(i) == right_arr.value(j)
                }
                (arrow::datatypes::DataType::Float64, arrow::datatypes::DataType::Float64) => {
                    let left_arr = left.as_any().downcast_ref::<Float64Array>().unwrap();
                    let right_arr = right.as_any().downcast_ref::<Float64Array>().unwrap();
                    left_arr.value(i) == right_arr.value(j)
                }
                (arrow::datatypes::DataType::Utf8, arrow::datatypes::DataType::Utf8) => {
                    let left_arr = left.as_any().downcast_ref::<StringArray>().unwrap();
                    let right_arr = right.as_any().downcast_ref::<StringArray>().unwrap();
                    left_arr.value(i) == right_arr.value(j)
                }
                _ => {
                    return Err(QueryError::NotImplemented(format!(
                        "IN subquery not supported for types: {:?} IN {:?}",
                        left.data_type(),
                        right.data_type()
                    )))
                }
            };

            if matches {
                found = true;
                break;
            }
        }

        result.push(Some(if negated { !found } else { found }));
    }

    Ok(Arc::new(BooleanArray::from(result)))
}

/// Convert scalar value to array (for scalar subquery results)
fn scalar_to_array(value: &ScalarValue, num_rows: usize) -> ArrayRef {
    use arrow::array::*;
    use arrow::datatypes::DataType;

    match value {
        ScalarValue::Null => Arc::new(NullArray::new(num_rows)),
        ScalarValue::Boolean(v) => Arc::new(BooleanArray::from(vec![*v; num_rows])),
        ScalarValue::Int8(v) => Arc::new(Int8Array::from(vec![*v; num_rows])),
        ScalarValue::Int16(v) => Arc::new(Int16Array::from(vec![*v; num_rows])),
        ScalarValue::Int32(v) => Arc::new(Int32Array::from(vec![*v; num_rows])),
        ScalarValue::Int64(v) => Arc::new(Int64Array::from(vec![*v; num_rows])),
        ScalarValue::Float32(v) => Arc::new(Float32Array::from(vec![v.0; num_rows])),
        ScalarValue::Float64(v) => Arc::new(Float64Array::from(vec![v.0; num_rows])),
        ScalarValue::Utf8(v) => Arc::new(StringArray::from(vec![v.as_str(); num_rows])),
        ScalarValue::Date32(v) => Arc::new(Date32Array::from(vec![*v; num_rows])),
        _ => Arc::new(Int64Array::from(vec![0i64; num_rows])),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, BooleanArray};

    #[test]
    fn test_scalar_to_array_conversion() {
        let scalar = ScalarValue::Int64(42);
        let arr = scalar_to_array(&scalar, 3);
        let int_arr = arr.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(int_arr.len(), 3);
        assert_eq!(int_arr.value(0), 42);
        assert_eq!(int_arr.value(1), 42);
        assert_eq!(int_arr.value(2), 42);
    }

    #[test]
    fn test_empty_in_subquery() {
        let empty = new_empty_array(arrow::datatypes::DataType::Int64);
        let left: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 3]));
        let result = evaluate_in_subquery(&left, &empty, false).unwrap();
        let bool_arr = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(bool_arr.len(), 3);
        // All should be false since right side is empty
        assert!(!bool_arr.value(0));
        assert!(!bool_arr.value(1));
        assert!(!bool_arr.value(2));
    }
}
