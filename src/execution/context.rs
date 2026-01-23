//! Execution context - main entry point for query execution

use crate::error::Result;
use crate::optimizer::Optimizer;
use crate::parser;
use crate::physical::operators::{MemoryTable, TableProvider};
use crate::physical::{PhysicalOperator, PhysicalPlanner};
use crate::planner::{Binder, InMemoryCatalog, LogicalPlan, PlanSchema, SchemaField};
use crate::storage::ParquetTable;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Query execution result
#[derive(Debug)]
pub struct QueryResult {
    /// Output schema
    pub schema: SchemaRef,
    /// Result batches
    pub batches: Vec<RecordBatch>,
    /// Total row count
    pub row_count: usize,
    /// Execution metrics
    pub metrics: QueryMetrics,
}

/// Query execution metrics
#[derive(Debug, Default)]
pub struct QueryMetrics {
    /// Time spent parsing
    pub parse_time: Duration,
    /// Time spent planning
    pub plan_time: Duration,
    /// Time spent optimizing
    pub optimize_time: Duration,
    /// Time spent executing
    pub execute_time: Duration,
    /// Total time
    pub total_time: Duration,
}

/// Execution context - manages tables and executes queries
pub struct ExecutionContext {
    catalog: InMemoryCatalog,
    tables: HashMap<String, Arc<dyn TableProvider>>,
    optimizer: Optimizer,
    /// Number of parallel partitions for execution (defaults to CPU count)
    parallel_partitions: usize,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionContext {
    pub fn new() -> Self {
        Self {
            catalog: InMemoryCatalog::new(),
            tables: HashMap::new(),
            optimizer: Optimizer::new(),
            parallel_partitions: rayon::current_num_threads(),
        }
    }

    /// Set the number of parallel partitions for execution
    pub fn with_parallel_partitions(mut self, partitions: usize) -> Self {
        self.parallel_partitions = partitions.max(1);
        self
    }

    /// Get the number of parallel partitions
    pub fn parallel_partitions(&self) -> usize {
        self.parallel_partitions
    }

    /// Register a table from record batches
    pub fn register_table(
        &mut self,
        name: impl Into<String>,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) {
        let name = name.into();

        // Register with catalog for planning
        let plan_schema = arrow_schema_to_plan_schema(&schema);
        self.catalog.register_table(name.clone(), plan_schema);

        // Register table provider for execution
        let table = Arc::new(MemoryTable::new(schema, batches));
        self.tables.insert(name, table);
    }

    /// Register a table from a single batch
    pub fn register_batch(&mut self, name: impl Into<String>, batch: RecordBatch) {
        let schema = batch.schema();
        self.register_table(name, schema, vec![batch]);
    }

    /// Register a custom table provider
    ///
    /// This allows registering any type that implements TableProvider,
    /// such as ParquetTable or IcebergTable.
    pub fn register_table_provider(
        &mut self,
        name: impl Into<String>,
        provider: Arc<dyn TableProvider>,
    ) {
        let name = name.into();

        // Register schema with catalog for planning
        let plan_schema = arrow_schema_to_plan_schema(&provider.schema());
        self.catalog.register_table(name.clone(), plan_schema);

        // Store provider for execution
        self.tables.insert(name, provider);
    }

    /// Register a table from Parquet file(s)
    ///
    /// If path points to a file, loads that single Parquet file.
    /// If path points to a directory, loads all .parquet files in it.
    pub fn register_parquet(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let table = ParquetTable::try_new(path)?;
        self.register_table_provider(name, Arc::new(table));
        Ok(())
    }

    /// Execute a SQL query and return results
    pub async fn sql(&self, query: &str) -> Result<QueryResult> {
        let start = Instant::now();
        let mut metrics = QueryMetrics::default();

        // Parse
        let parse_start = Instant::now();
        let stmt = parser::parse_sql(query)?;
        metrics.parse_time = parse_start.elapsed();

        // Plan
        let plan_start = Instant::now();
        let mut binder = Binder::new(&self.catalog);
        let logical = binder.bind(&stmt)?;
        metrics.plan_time = plan_start.elapsed();

        // Optimize
        let optimize_start = Instant::now();
        let optimized = self.optimizer.optimize(logical)?;
        metrics.optimize_time = optimize_start.elapsed();

        // Physical planning
        let physical_start = Instant::now();
        let mut planner = PhysicalPlanner::new();
        for (name, provider) in &self.tables {
            planner.register_table(name.clone(), provider.clone());
        }
        // Enable subquery execution support
        planner.enable_subquery_execution();
        let physical = planner.create_physical_plan(&optimized)?;
        metrics.plan_time += physical_start.elapsed();

        // Execute
        let execute_start = Instant::now();

        // Determine number of output partitions
        let num_partitions = physical.output_partitions().max(1);

        // Execute all partitions concurrently
        // Use futures to execute partitions in parallel
        let partition_futures: Vec<_> = (0..num_partitions)
            .map(|partition_id| {
                let physical = physical.clone();
                async move {
                    let stream = physical.execute(partition_id).await.map_err(|e| {
                        crate::error::QueryError::Execution(format!(
                            "Partition {} execution failed: {}",
                            partition_id, e
                        ))
                    })?;
                    stream.try_collect().await.map_err(|e| {
                        crate::error::QueryError::Execution(format!(
                            "Partition {} collection failed: {}",
                            partition_id, e
                        ))
                    })
                }
            })
            .collect();

        // Execute all partitions concurrently and collect results
        let partition_results: Vec<Result<Vec<RecordBatch>>> =
            futures::future::join_all(partition_futures).await;

        // Check for errors in any partition
        let mut all_batches = Vec::new();
        for partition_result in partition_results {
            all_batches.extend(partition_result?);
        }

        metrics.execute_time = execute_start.elapsed();

        metrics.total_time = start.elapsed();

        let schema = physical.schema();
        let row_count: usize = all_batches.iter().map(|b| b.num_rows()).sum();

        Ok(QueryResult {
            schema,
            batches: all_batches,
            row_count,
            metrics,
        })
    }

    /// Get the logical plan for a query (for debugging)
    pub fn logical_plan(&self, query: &str) -> Result<LogicalPlan> {
        let stmt = parser::parse_sql(query)?;
        let mut binder = Binder::new(&self.catalog);
        binder.bind(&stmt)
    }

    /// Get the optimized logical plan for a query (for debugging)
    pub fn optimized_plan(&self, query: &str) -> Result<LogicalPlan> {
        let logical = self.logical_plan(query)?;
        self.optimizer.optimize(logical)
    }

    /// Get the physical plan for a query (for debugging)
    pub fn physical_plan(&self, query: &str) -> Result<Arc<dyn PhysicalOperator>> {
        let optimized = self.optimized_plan(query)?;

        let mut planner = PhysicalPlanner::new();
        for (name, provider) in &self.tables {
            planner.register_table(name.clone(), provider.clone());
        }
        planner.enable_subquery_execution();

        planner.create_physical_plan(&optimized)
    }

    /// List registered tables
    pub fn table_names(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Get table schema
    pub fn table_schema(&self, name: &str) -> Option<SchemaRef> {
        self.tables.get(name).map(|t| t.schema())
    }
}

/// Convert Arrow schema to PlanSchema
fn arrow_schema_to_plan_schema(schema: &Schema) -> PlanSchema {
    let fields: Vec<SchemaField> = schema
        .fields()
        .iter()
        .map(|f| {
            SchemaField::new(f.name().clone(), f.data_type().clone()).with_nullable(f.is_nullable())
        })
        .collect();
    PlanSchema::new(fields)
}

/// Utility to print a record batch
pub fn print_batch(batch: &RecordBatch) {
    use arrow::util::pretty::print_batches;
    let _ = print_batches(&[batch.clone()]);
}

/// Utility to print query results
pub fn print_results(result: &QueryResult) {
    use arrow::util::pretty::print_batches;

    println!("Schema: {:?}", result.schema);
    println!("Row count: {}", result.row_count);
    println!(
        "Timing: parse={:?}, plan={:?}, opt={:?}, exec={:?}, total={:?}",
        result.metrics.parse_time,
        result.metrics.plan_time,
        result.metrics.optimize_time,
        result.metrics.execute_time,
        result.metrics.total_time
    );
    println!();

    if !result.batches.is_empty() {
        let _ = print_batches(&result.batches);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    fn create_test_context() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        ctx.register_table("test", schema, vec![batch]);
        ctx
    }

    #[tokio::test]
    async fn test_simple_query() {
        let ctx = create_test_context();
        let result = ctx.sql("SELECT id, value FROM test").await.unwrap();

        assert_eq!(result.row_count, 5);
        assert_eq!(result.schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn test_filter_query() {
        let ctx = create_test_context();
        let result = ctx
            .sql("SELECT id FROM test WHERE value > 25")
            .await
            .unwrap();

        assert_eq!(result.row_count, 3);
    }

    #[tokio::test]
    async fn test_aggregate_query() {
        let ctx = create_test_context();
        let result = ctx
            .sql("SELECT SUM(value), COUNT(*) FROM test")
            .await
            .unwrap();

        assert_eq!(result.row_count, 1);

        let sum = result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(sum, 150);
    }

    #[tokio::test]
    async fn test_sort_query() {
        let ctx = create_test_context();
        // Note: ORDER BY columns must be in SELECT (planner limitation)
        let result = ctx
            .sql("SELECT id, value FROM test ORDER BY value DESC")
            .await
            .unwrap();

        assert_eq!(result.row_count, 5);

        let ids = result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 5);
        assert_eq!(ids.value(1), 4);
        assert_eq!(ids.value(2), 3);
    }

    #[tokio::test]
    async fn test_limit_query() {
        let ctx = create_test_context();
        let result = ctx.sql("SELECT id FROM test LIMIT 2").await.unwrap();

        assert_eq!(result.row_count, 2);
    }

    #[tokio::test]
    async fn test_group_by_query() {
        let ctx = create_test_context();
        let result = ctx
            .sql("SELECT name, SUM(value) FROM test GROUP BY name")
            .await
            .unwrap();

        assert_eq!(result.row_count, 5); // 5 unique names
    }
}
