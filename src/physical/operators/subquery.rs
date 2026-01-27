//! Subquery execution support

use crate::error::{QueryError, Result};
use crate::physical::operators::TableProvider;
use crate::physical::PhysicalPlanner;
use crate::planner::{Expr, LogicalPlan, ScalarValue};
use arrow::array::{Array, ArrayRef, BooleanArray};
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
    /// Cached results for correlated subqueries (keyed by plan hash + correlation key values)
    correlated_cache: parking_lot::Mutex<HashMap<CorrelatedCacheKey, SubqueryResult>>,
}

/// Cache key for correlated subqueries: plan hash + correlation values
#[derive(Clone, Hash, PartialEq, Eq)]
struct CorrelatedCacheKey {
    plan_hash: usize,
    correlation_values: Vec<CorrelationValue>,
}

/// A hashable representation of correlation column values
#[derive(Clone, Hash, PartialEq, Eq)]
enum CorrelationValue {
    Null,
    Int64(i64),
    Int32(i32),
    Float64Bits(u64), // Store f64 as bits for hashing
    Utf8(String),
    Boolean(bool),
    Date32(i32),
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
                correlated_cache: parking_lot::Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Extract correlation values from a batch row for cache key
    fn extract_correlation_values(&self, batch: &RecordBatch, row: usize) -> Vec<CorrelationValue> {
        use arrow::array::*;

        batch
            .columns()
            .iter()
            .map(|col| {
                if col.is_null(row) {
                    return CorrelationValue::Null;
                }
                match col.data_type() {
                    arrow::datatypes::DataType::Int64 => {
                        let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                        CorrelationValue::Int64(arr.value(row))
                    }
                    arrow::datatypes::DataType::Int32 => {
                        let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                        CorrelationValue::Int32(arr.value(row))
                    }
                    arrow::datatypes::DataType::Float64 => {
                        let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                        CorrelationValue::Float64Bits(arr.value(row).to_bits())
                    }
                    arrow::datatypes::DataType::Utf8 => {
                        let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                        CorrelationValue::Utf8(arr.value(row).to_string())
                    }
                    arrow::datatypes::DataType::Boolean => {
                        let arr = col.as_any().downcast_ref::<BooleanArray>().unwrap();
                        CorrelationValue::Boolean(arr.value(row))
                    }
                    arrow::datatypes::DataType::Date32 => {
                        let arr = col.as_any().downcast_ref::<Date32Array>().unwrap();
                        CorrelationValue::Date32(arr.value(row))
                    }
                    _ => CorrelationValue::Null,
                }
            })
            .collect()
    }

    /// Try to get cached result for correlated subquery
    fn get_correlated_cache(
        &self,
        plan_hash: usize,
        correlation_values: &[CorrelationValue],
    ) -> Option<SubqueryResult> {
        let cache = self.inner.correlated_cache.lock();
        let key = CorrelatedCacheKey {
            plan_hash,
            correlation_values: correlation_values.to_vec(),
        };
        cache.get(&key).cloned()
    }

    /// Maximum number of entries in the correlated cache to prevent unbounded memory growth
    const MAX_CORRELATED_CACHE_SIZE: usize = 100_000;

    /// Cache result for correlated subquery
    fn set_correlated_cache(
        &self,
        plan_hash: usize,
        correlation_values: Vec<CorrelationValue>,
        result: SubqueryResult,
    ) {
        let mut cache = self.inner.correlated_cache.lock();

        // If cache is too large, clear it to prevent unbounded memory growth
        // This is a simple strategy - a more sophisticated approach would use LRU eviction
        if cache.len() >= Self::MAX_CORRELATED_CACHE_SIZE {
            cache.clear();
        }

        let key = CorrelatedCacheKey {
            plan_hash,
            correlation_values,
        };
        cache.insert(key, result);
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

    /// Execute a scalar subquery and return the scalar value
    pub fn execute_scalar(&self, plan: &LogicalPlan) -> Result<ScalarValue> {
        let key = self.plan_hash(plan);

        {
            let cache = self.inner.cache.lock();
            if let Some(SubqueryResult::Scalar(value)) = cache.get(&key) {
                return Ok(value.clone());
            }
        }

        let planner = self.create_planner();
        let physical = planner.create_physical_plan(plan)?;
        let _plan_clone = plan.clone();

        // Run the async code in a blocking way
        let batches = if let Ok(_handle) = tokio::runtime::Handle::try_current() {
            // We're inside a tokio runtime, use spawn_blocking
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Runtime::new().map_err(|e| {
                    QueryError::Execution(format!("Failed to create runtime: {}", e))
                })?;
                let stream = runtime.block_on(physical.execute(0))?;
                let result: Result<Vec<RecordBatch>> =
                    runtime.block_on(async { stream.try_collect().await });
                result
            })
            .join()
            .unwrap_or_else(|_| {
                Err(QueryError::Execution(
                    "Subquery execution thread panicked".into(),
                ))
            })?
        } else {
            // No existing runtime, create a new one and use it directly
            let runtime = tokio::runtime::Runtime::new()
                .map_err(|e| QueryError::Execution(format!("Failed to create runtime: {}", e)))?;
            let stream = runtime.block_on(physical.execute(0))?;
            runtime.block_on(async { stream.try_collect().await })?
        };

        if batches.is_empty() || batches[0].num_rows() == 0 {
            let result = ScalarValue::Null;
            self.inner
                .cache
                .lock()
                .insert(key, SubqueryResult::Scalar(result.clone()));
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

        self.inner
            .cache
            .lock()
            .insert(key, SubqueryResult::Scalar(scalar.clone()));
        Ok(scalar)
    }

    /// Execute an IN subquery and return the array of values
    pub fn execute_in_subquery(&self, plan: &LogicalPlan) -> Result<ArrayRef> {
        let key = self.plan_hash(plan);

        {
            let cache = self.inner.cache.lock();
            if let Some(SubqueryResult::Array(array)) = cache.get(&key) {
                return Ok(array.clone());
            }
        }

        let planner = self.create_planner();
        let physical = planner.create_physical_plan(plan)?;

        // Run the async code in a blocking way
        let batches = if let Ok(_handle) = tokio::runtime::Handle::try_current() {
            // We're inside a tokio runtime, use a dedicated thread
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Runtime::new().map_err(|e| {
                    QueryError::Execution(format!("Failed to create runtime: {}", e))
                })?;
                let stream = runtime.block_on(physical.execute(0))?;
                let result: Result<Vec<RecordBatch>> =
                    runtime.block_on(async { stream.try_collect().await });
                result
            })
            .join()
            .unwrap_or_else(|_| {
                Err(QueryError::Execution(
                    "Subquery execution thread panicked".into(),
                ))
            })?
        } else {
            // No existing runtime, create a new one and use it directly
            let runtime = tokio::runtime::Runtime::new()
                .map_err(|e| QueryError::Execution(format!("Failed to create runtime: {}", e)))?;
            let stream = runtime.block_on(physical.execute(0))?;
            runtime.block_on(async { stream.try_collect().await })?
        };

        if batches.is_empty() {
            let result = new_empty_array(
                plan.schema()
                    .fields()
                    .first()
                    .ok_or_else(|| QueryError::Execution("IN subquery has no columns".into()))?
                    .data_type
                    .clone(),
            );
            self.inner
                .cache
                .lock()
                .insert(key, SubqueryResult::Array(result.clone()));
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

        self.inner
            .cache
            .lock()
            .insert(key, SubqueryResult::Array(result.clone()));
        Ok(result)
    }

    /// Execute an EXISTS subquery and return the boolean result
    pub fn execute_exists(&self, plan: &LogicalPlan) -> Result<bool> {
        let key = self.plan_hash(plan);

        {
            let cache = self.inner.cache.lock();
            if let Some(SubqueryResult::Boolean(value)) = cache.get(&key) {
                return Ok(*value);
            }
        }

        let planner = self.create_planner();
        let physical = planner.create_physical_plan(plan)?;

        // Run the async code in a blocking way
        let batches = if let Ok(_handle) = tokio::runtime::Handle::try_current() {
            // We're inside a tokio runtime, use a dedicated thread
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Runtime::new().map_err(|e| {
                    QueryError::Execution(format!("Failed to create runtime: {}", e))
                })?;
                let stream = runtime.block_on(physical.execute(0))?;
                let result: Result<Vec<RecordBatch>> =
                    runtime.block_on(async { stream.try_collect().await });
                result
            })
            .join()
            .unwrap_or_else(|_| {
                Err(QueryError::Execution(
                    "Subquery execution thread panicked".into(),
                ))
            })?
        } else {
            // No existing runtime, create a new one and use it directly
            let runtime = tokio::runtime::Runtime::new()
                .map_err(|e| QueryError::Execution(format!("Failed to create runtime: {}", e)))?;
            let stream = runtime.block_on(physical.execute(0))?;
            runtime.block_on(async { stream.try_collect().await })?
        };

        let has_rows = batches.iter().any(|b| b.num_rows() > 0);

        self.inner
            .cache
            .lock()
            .insert(key, SubqueryResult::Boolean(has_rows));
        Ok(has_rows)
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
            let arr = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| QueryError::Type("Expected BooleanArray".into()))?;
            ScalarValue::Boolean(arr.value(index))
        }
        arrow::datatypes::DataType::Int8 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| QueryError::Type("Expected Int8Array".into()))?;
            ScalarValue::Int8(arr.value(index))
        }
        arrow::datatypes::DataType::Int16 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| QueryError::Type("Expected Int16Array".into()))?;
            ScalarValue::Int16(arr.value(index))
        }
        arrow::datatypes::DataType::Int32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| QueryError::Type("Expected Int32Array".into()))?;
            ScalarValue::Int32(arr.value(index))
        }
        arrow::datatypes::DataType::Int64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| QueryError::Type("Expected Int64Array".into()))?;
            ScalarValue::Int64(arr.value(index))
        }
        arrow::datatypes::DataType::Float32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| QueryError::Type("Expected Float32Array".into()))?;
            ScalarValue::Float32(ordered_float::OrderedFloat(arr.value(index)))
        }
        arrow::datatypes::DataType::Float64 => {
            let arr = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| QueryError::Type("Expected Float64Array".into()))?;
            ScalarValue::Float64(ordered_float::OrderedFloat(arr.value(index)))
        }
        arrow::datatypes::DataType::Utf8 => {
            let arr = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| QueryError::Type("Expected StringArray".into()))?;
            ScalarValue::Utf8(arr.value(index).to_string())
        }
        arrow::datatypes::DataType::Date32 => {
            let arr = array
                .as_any()
                .downcast_ref::<Date32Array>()
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
        arrow::datatypes::DataType::Boolean => {
            Arc::new(BooleanArray::from(vec![] as Vec<Option<bool>>))
        }
        arrow::datatypes::DataType::Int64 => Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)),
        arrow::datatypes::DataType::Int32 => Arc::new(Int32Array::from(vec![] as Vec<Option<i32>>)),
        arrow::datatypes::DataType::Float64 => {
            Arc::new(Float64Array::from(vec![] as Vec<Option<f64>>))
        }
        arrow::datatypes::DataType::Utf8 => {
            Arc::new(StringArray::from(vec![] as Vec<Option<&str>>))
        }
        _ => Arc::new(Int64Array::from(vec![] as Vec<Option<i64>>)),
    }
}

/// Collect all table names/aliases defined in a logical plan's FROM clause
fn collect_table_aliases(plan: &LogicalPlan) -> std::collections::HashSet<String> {
    let mut aliases = std::collections::HashSet::new();
    collect_table_aliases_recursive(plan, &mut aliases);
    aliases
}

fn collect_table_aliases_recursive(
    plan: &LogicalPlan,
    aliases: &mut std::collections::HashSet<String>,
) {
    use crate::planner::*;
    match plan {
        LogicalPlan::Scan(node) => {
            aliases.insert(node.table_name.clone());
        }
        LogicalPlan::SubqueryAlias(node) => {
            aliases.insert(node.alias.clone());
            // Don't recurse into the aliased input - the alias shadows it
        }
        LogicalPlan::Join(node) => {
            collect_table_aliases_recursive(&node.left, aliases);
            collect_table_aliases_recursive(&node.right, aliases);
        }
        LogicalPlan::Filter(node) => {
            collect_table_aliases_recursive(&node.input, aliases);
        }
        LogicalPlan::Project(node) => {
            collect_table_aliases_recursive(&node.input, aliases);
        }
        LogicalPlan::Aggregate(node) => {
            collect_table_aliases_recursive(&node.input, aliases);
        }
        LogicalPlan::Sort(node) => {
            collect_table_aliases_recursive(&node.input, aliases);
        }
        LogicalPlan::Limit(node) => {
            collect_table_aliases_recursive(&node.input, aliases);
        }
        LogicalPlan::Distinct(node) => {
            collect_table_aliases_recursive(&node.input, aliases);
        }
        _ => {}
    }
}

/// Check if an expression contains any column references to tables not in the given set
fn has_outer_references(expr: &Expr, local_tables: &std::collections::HashSet<String>) -> bool {
    use crate::planner::*;
    match expr {
        Expr::Column(col) => {
            if let Some(rel) = &col.relation {
                // This column references a specific table - check if it's local
                !local_tables.contains(rel)
            } else {
                // Unqualified column - assume local
                false
            }
        }
        Expr::BinaryExpr { left, right, .. } => {
            has_outer_references(left, local_tables) || has_outer_references(right, local_tables)
        }
        Expr::UnaryExpr { expr, .. } => has_outer_references(expr, local_tables),
        Expr::ScalarFunc { args, .. } | Expr::Aggregate { args, .. } => {
            args.iter().any(|a| has_outer_references(a, local_tables))
        }
        Expr::Cast { expr, .. } | Expr::Alias { expr, .. } => {
            has_outer_references(expr, local_tables)
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            operand
                .as_ref()
                .is_some_and(|o| has_outer_references(o, local_tables))
                || when_then.iter().any(|(w, t)| {
                    has_outer_references(w, local_tables) || has_outer_references(t, local_tables)
                })
                || else_expr
                    .as_ref()
                    .is_some_and(|e| has_outer_references(e, local_tables))
        }
        Expr::InList { expr, list, .. } => {
            has_outer_references(expr, local_tables)
                || list.iter().any(|e| has_outer_references(e, local_tables))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            has_outer_references(expr, local_tables)
                || has_outer_references(low, local_tables)
                || has_outer_references(high, local_tables)
        }
        _ => false,
    }
}

/// Check if a plan contains any outer (correlated) column references
fn plan_has_outer_references(
    plan: &LogicalPlan,
    local_tables: &std::collections::HashSet<String>,
) -> bool {
    use crate::planner::*;
    match plan {
        LogicalPlan::Filter(node) => {
            has_outer_references(&node.predicate, local_tables)
                || plan_has_outer_references(&node.input, local_tables)
        }
        LogicalPlan::Project(node) => {
            node.exprs
                .iter()
                .any(|e| has_outer_references(e, local_tables))
                || plan_has_outer_references(&node.input, local_tables)
        }
        LogicalPlan::Join(node) => {
            node.on.iter().any(|(l, r)| {
                has_outer_references(l, local_tables) || has_outer_references(r, local_tables)
            }) || node
                .filter
                .as_ref()
                .is_some_and(|f| has_outer_references(f, local_tables))
                || plan_has_outer_references(&node.left, local_tables)
                || plan_has_outer_references(&node.right, local_tables)
        }
        LogicalPlan::Aggregate(node) => {
            node.group_by
                .iter()
                .any(|e| has_outer_references(e, local_tables))
                || node
                    .aggregates
                    .iter()
                    .any(|e| has_outer_references(e, local_tables))
                || plan_has_outer_references(&node.input, local_tables)
        }
        LogicalPlan::Sort(node) => {
            node.order_by
                .iter()
                .any(|s| has_outer_references(&s.expr, local_tables))
                || plan_has_outer_references(&node.input, local_tables)
        }
        LogicalPlan::Limit(node) => plan_has_outer_references(&node.input, local_tables),
        LogicalPlan::Distinct(node) => plan_has_outer_references(&node.input, local_tables),
        LogicalPlan::SubqueryAlias(node) => plan_has_outer_references(&node.input, local_tables),
        LogicalPlan::Scan(node) => node
            .filter
            .as_ref()
            .is_some_and(|f| has_outer_references(f, local_tables)),
        _ => false,
    }
}

/// Check if a subquery is correlated (references columns from outer scope)
fn is_correlated_subquery(subquery: &LogicalPlan) -> bool {
    let local_tables = collect_table_aliases(subquery);
    plan_has_outer_references(subquery, &local_tables)
}

/// Evaluate a subquery expression during filter execution
pub fn evaluate_subquery_expr(
    batch: &RecordBatch,
    expr: &Expr,
    executor: &SubqueryExecutor,
) -> Result<ArrayRef> {
    match expr {
        Expr::ScalarSubquery(plan) => {
            // Check if this is a correlated subquery by analyzing the plan structure
            if is_correlated_subquery(plan) {
                // Correlated subquery - execute for each row
                return execute_correlated_scalar_subquery(batch, plan, executor);
            }

            // Uncorrelated - execute once
            match executor.execute_scalar(plan) {
                Ok(scalar) => {
                    // Use the same value for all rows
                    let num_rows = batch.num_rows();
                    Ok(scalar_to_array(&scalar, num_rows))
                }
                Err(QueryError::ColumnNotFound(_)) => {
                    // Fallback: Correlated subquery (shouldn't happen with plan analysis)
                    execute_correlated_scalar_subquery(batch, plan, executor)
                }
                Err(e) => Err(e),
            }
        }
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => {
            // Execute the IN subquery once to get the set of values
            let in_values = executor.execute_in_subquery(subquery)?;

            // Evaluate the left side expression
            let left_array = super::filter::evaluate_expr(batch, expr)?;

            // Check membership
            evaluate_in_subquery(&left_array, &in_values, *negated)
        }
        Expr::Exists { subquery, negated } => {
            // Check if this is a correlated subquery by analyzing the plan structure
            if is_correlated_subquery(subquery) {
                // Correlated EXISTS subquery - execute row-by-row
                return execute_correlated_exists_subquery(batch, subquery, *negated, executor);
            }

            // Uncorrelated - execute once
            match executor.execute_exists(subquery) {
                Ok(exists) => {
                    // Use the same result for all rows
                    let result = if *negated { !exists } else { exists };
                    let arr = BooleanArray::from(vec![result; batch.num_rows()]);
                    Ok(Arc::new(arr))
                }
                Err(QueryError::ColumnNotFound(_)) => {
                    // Fallback: Correlated EXISTS subquery (shouldn't happen with plan analysis)
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

/// Execute a correlated scalar subquery for each row in the batch
fn execute_correlated_scalar_subquery(
    batch: &RecordBatch,
    plan: &LogicalPlan,
    executor: &SubqueryExecutor,
) -> Result<ArrayRef> {
    let num_rows = batch.num_rows();
    let mut results = Vec::with_capacity(num_rows);

    // Compute plan hash once for cache key
    let plan_hash = executor.plan_hash(plan);

    // For each row, we need to:
    // 1. Extract correlation key values
    // 2. Check cache for existing result
    // 3. If not cached, substitute columns and execute
    // 4. Cache the result

    for row in 0..num_rows {
        // Extract correlation values for this row (used as cache key)
        let correlation_values = executor.extract_correlation_values(batch, row);

        // Check cache first
        if let Some(SubqueryResult::Scalar(cached)) =
            executor.get_correlated_cache(plan_hash, &correlation_values)
        {
            results.push(cached);
            continue;
        }

        // Cache miss - execute the subquery
        let substituted_plan = substitute_correlated_columns(plan, batch, row)?;

        let scalar = match executor.execute_scalar(&substituted_plan) {
            Ok(scalar) => scalar,
            Err(_e) => crate::planner::ScalarValue::Null,
        };

        // Cache the result
        executor.set_correlated_cache(
            plan_hash,
            correlation_values,
            SubqueryResult::Scalar(scalar.clone()),
        );
        results.push(scalar);
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

    // Compute plan hash once for cache key
    let plan_hash = executor.plan_hash(subquery);

    for row in 0..num_rows {
        // Extract correlation values for this row (used as cache key)
        let correlation_values = executor.extract_correlation_values(batch, row);

        // Check cache first
        if let Some(SubqueryResult::Boolean(cached)) =
            executor.get_correlated_cache(plan_hash, &correlation_values)
        {
            results.push(if negated { !cached } else { cached });
            continue;
        }

        // Cache miss - execute the subquery
        let substituted_plan = substitute_correlated_columns(subquery, batch, row)?;

        let exists = executor
            .execute_exists(&substituted_plan)
            .unwrap_or_default();

        // Cache the result (before applying negation)
        executor.set_correlated_cache(
            plan_hash,
            correlation_values,
            SubqueryResult::Boolean(exists),
        );
        results.push(if negated { !exists } else { exists });
    }

    Ok(Arc::new(BooleanArray::from(results)))
}

/// Substitute correlated column references with literal values from a specific row
fn substitute_correlated_columns(
    plan: &LogicalPlan,
    batch: &RecordBatch,
    row: usize,
) -> Result<LogicalPlan> {
    use crate::planner::*;

    // First, collect which table aliases are local to this subquery
    // We should ONLY substitute columns that reference OUTER tables (not in this set)
    let local_tables = collect_table_aliases(plan);

    // Create a mapping of column names to their literal values for this row
    let mut column_values: std::collections::HashMap<String, Expr> =
        std::collections::HashMap::new();

    for (i, field) in batch.schema().fields().iter().enumerate() {
        let column = batch.column(i);
        let scalar = if column.is_null(row) {
            ScalarValue::Null
        } else {
            array_ref_to_scalar_value(column, row)?
        };

        let literal_expr = Expr::Literal(scalar);

        // Add the fully qualified name (e.g., "partsupp.ps_partkey")
        column_values.insert(field.name().clone(), literal_expr.clone());

        // Also add just the column name without qualifier (e.g., "ps_partkey")
        // This handles cases where the subquery uses unqualified references
        if let Some(dot_pos) = field.name().find('.') {
            let unqualified = &field.name()[dot_pos + 1..];
            column_values.insert(unqualified.to_string(), literal_expr.clone());
        }

        // Also add with potential table qualifiers from common patterns
        // E.g., "l_partkey" could be referenced as just "partkey" if context is clear
        if let Some(dot_pos) = field.name().rfind('_') {
            // Extract base name after last underscore (common pattern: table_column)
            let base_name = &field.name()[dot_pos + 1..];
            column_values.insert(base_name.to_string(), literal_expr);
        }
    }

    // Recursively substitute column references in the plan
    substitute_columns_in_plan(plan, &column_values, &local_tables)
}

/// Recursively substitute column references with literals in a logical plan
/// Only substitutes columns that reference tables NOT in local_tables (i.e., outer references)
fn substitute_columns_in_plan(
    plan: &LogicalPlan,
    column_values: &std::collections::HashMap<String, Expr>,
    local_tables: &std::collections::HashSet<String>,
) -> Result<LogicalPlan> {
    use crate::planner::*;

    // Recursively process the plan
    match plan {
        LogicalPlan::Filter(node) => {
            let new_predicate =
                substitute_columns_in_expr(&node.predicate, column_values, local_tables);
            let new_input = substitute_columns_in_plan(&node.input, column_values, local_tables)?;
            Ok(LogicalPlan::Filter(FilterNode {
                input: Arc::new(new_input),
                predicate: new_predicate,
            }))
        }
        LogicalPlan::Project(node) => {
            let new_exprs: Vec<Expr> = node
                .exprs
                .iter()
                .map(|e| substitute_columns_in_expr(e, column_values, local_tables))
                .collect();
            let new_input = substitute_columns_in_plan(&node.input, column_values, local_tables)?;
            Ok(LogicalPlan::Project(ProjectNode {
                input: Arc::new(new_input),
                exprs: new_exprs,
                schema: node.schema.clone(),
            }))
        }
        LogicalPlan::Aggregate(node) => {
            let new_group_by: Vec<Expr> = node
                .group_by
                .iter()
                .map(|e| substitute_columns_in_expr(e, column_values, local_tables))
                .collect();
            let new_aggregates: Vec<Expr> = node
                .aggregates
                .iter()
                .map(|e| substitute_columns_in_expr(e, column_values, local_tables))
                .collect();
            let new_input = substitute_columns_in_plan(&node.input, column_values, local_tables)?;
            Ok(LogicalPlan::Aggregate(AggregateNode {
                input: Arc::new(new_input),
                group_by: new_group_by,
                aggregates: new_aggregates,
                schema: node.schema.clone(),
            }))
        }
        LogicalPlan::Scan(node) => {
            // Substitute in filter expression if present
            let new_filter = node
                .filter
                .as_ref()
                .map(|f| substitute_columns_in_expr(f, column_values, local_tables));
            Ok(LogicalPlan::Scan(ScanNode {
                table_name: node.table_name.clone(),
                schema: node.schema.clone(),
                projection: node.projection.clone(),
                filter: new_filter,
            }))
        }
        LogicalPlan::Join(node) => {
            let new_on: Vec<(Expr, Expr)> = node
                .on
                .iter()
                .map(|(l, r)| {
                    (
                        substitute_columns_in_expr(l, column_values, local_tables),
                        substitute_columns_in_expr(r, column_values, local_tables),
                    )
                })
                .collect();
            let new_filter = node
                .filter
                .as_ref()
                .map(|f| substitute_columns_in_expr(f, column_values, local_tables));
            let new_left = substitute_columns_in_plan(&node.left, column_values, local_tables)?;
            let new_right = substitute_columns_in_plan(&node.right, column_values, local_tables)?;
            Ok(LogicalPlan::Join(JoinNode {
                left: Arc::new(new_left),
                right: Arc::new(new_right),
                join_type: node.join_type,
                on: new_on,
                filter: new_filter,
                schema: node.schema.clone(),
            }))
        }
        LogicalPlan::Sort(node) => {
            let new_order_by: Vec<SortExpr> = node
                .order_by
                .iter()
                .map(|s| SortExpr {
                    expr: substitute_columns_in_expr(&s.expr, column_values, local_tables),
                    direction: s.direction,
                    nulls: s.nulls,
                })
                .collect();
            let new_input = substitute_columns_in_plan(&node.input, column_values, local_tables)?;
            Ok(LogicalPlan::Sort(SortNode {
                input: Arc::new(new_input),
                order_by: new_order_by,
            }))
        }
        LogicalPlan::Limit(node) => {
            let new_input = substitute_columns_in_plan(&node.input, column_values, local_tables)?;
            Ok(LogicalPlan::Limit(LimitNode {
                input: Arc::new(new_input),
                skip: node.skip,
                fetch: node.fetch,
            }))
        }
        LogicalPlan::Distinct(node) => {
            let new_input = substitute_columns_in_plan(&node.input, column_values, local_tables)?;
            Ok(LogicalPlan::Distinct(DistinctNode {
                input: Arc::new(new_input),
            }))
        }
        LogicalPlan::SubqueryAlias(node) => {
            let new_input = substitute_columns_in_plan(&node.input, column_values, local_tables)?;
            Ok(LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                input: Arc::new(new_input),
                alias: node.alias.clone(),
                schema: node.schema.clone(),
            }))
        }
        LogicalPlan::Union(node) => {
            let new_inputs: Result<Vec<Arc<LogicalPlan>>> = node
                .inputs
                .iter()
                .map(|i| substitute_columns_in_plan(i, column_values, local_tables).map(Arc::new))
                .collect();
            Ok(LogicalPlan::Union(UnionNode {
                inputs: new_inputs?,
                schema: node.schema.clone(),
                all: node.all,
            }))
        }
        // Pass through unchanged for leaf nodes
        LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => Ok(plan.clone()),

        LogicalPlan::DelimJoin(node) => {
            let new_on: Vec<(Expr, Expr)> = node
                .on
                .iter()
                .map(|(l, r)| {
                    (
                        substitute_columns_in_expr(l, column_values, local_tables),
                        substitute_columns_in_expr(r, column_values, local_tables),
                    )
                })
                .collect();
            let new_left = substitute_columns_in_plan(&node.left, column_values, local_tables)?;
            let new_right = substitute_columns_in_plan(&node.right, column_values, local_tables)?;
            Ok(LogicalPlan::DelimJoin(DelimJoinNode {
                left: Arc::new(new_left),
                right: Arc::new(new_right),
                join_type: node.join_type,
                delim_columns: node.delim_columns.clone(),
                on: new_on,
                schema: node.schema.clone(),
            }))
        }

        LogicalPlan::DelimGet(node) => Ok(LogicalPlan::DelimGet(node.clone())),
    }
}

/// Substitute column references in an expression
/// Only substitutes columns that reference tables NOT in local_tables
fn substitute_columns_in_expr(
    expr: &Expr,
    column_values: &std::collections::HashMap<String, Expr>,
    local_tables: &std::collections::HashSet<String>,
) -> Expr {
    use crate::planner::*;
    match expr {
        Expr::Column(col) => {
            // Only substitute if the column's relation is NOT a local table
            // If relation is Some and it's in local_tables, DON'T substitute
            if let Some(rel) = &col.relation {
                if local_tables.contains(rel) {
                    // This is a local table column - don't substitute
                    return expr.clone();
                }
            }

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
        Expr::BinaryExpr { left, op, right } => {
            let new_left = substitute_columns_in_expr(left, column_values, local_tables);
            let new_right = substitute_columns_in_expr(right, column_values, local_tables);
            Expr::BinaryExpr {
                left: Box::new(new_left),
                op: *op,
                right: Box::new(new_right),
            }
        }
        Expr::UnaryExpr { op, expr: inner } => {
            let new_expr = substitute_columns_in_expr(inner, column_values, local_tables);
            Expr::UnaryExpr {
                op: *op,
                expr: Box::new(new_expr),
            }
        }
        Expr::Alias { expr: inner, name } => {
            let new_expr = substitute_columns_in_expr(inner, column_values, local_tables);
            Expr::Alias {
                expr: Box::new(new_expr),
                name: name.clone(),
            }
        }
        Expr::Aggregate {
            func,
            args,
            distinct,
        } => {
            let new_args: Vec<Expr> = args
                .iter()
                .map(|a| substitute_columns_in_expr(a, column_values, local_tables))
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
                .map(|a| substitute_columns_in_expr(a, column_values, local_tables))
                .collect();
            Expr::ScalarFunc {
                args: new_args,
                func: func.clone(),
            }
        }
        Expr::Cast {
            expr: inner,
            data_type,
        } => {
            let new_expr = substitute_columns_in_expr(inner, column_values, local_tables);
            Expr::Cast {
                expr: Box::new(new_expr),
                data_type: data_type.clone(),
            }
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let new_operand = operand
                .as_ref()
                .map(|e| substitute_columns_in_expr(e, column_values, local_tables));
            let new_when_then: Vec<(Expr, Expr)> = when_then
                .iter()
                .map(|(w, t)| {
                    (
                        substitute_columns_in_expr(w, column_values, local_tables),
                        substitute_columns_in_expr(t, column_values, local_tables),
                    )
                })
                .collect();
            let new_else = else_expr
                .as_ref()
                .map(|e| substitute_columns_in_expr(e, column_values, local_tables));
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
        ScalarValue::Int64(_) => {
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
        ScalarValue::Float64(_) => {
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
        ScalarValue::Boolean(_) => {
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
        ScalarValue::Utf8(_) => {
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
    use arrow::array::{BooleanArray, Int64Array};

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
