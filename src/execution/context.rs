//! Execution context - main entry point for query execution

use crate::error::Result;
use crate::execution::{create_memory_pool, ExecutionConfig, SharedMemoryPool, SpillMetrics};
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
    /// Peak memory usage during execution (bytes)
    pub peak_memory_bytes: usize,
    /// Spill metrics if any spilling occurred
    pub spill_metrics: Option<SpillMetrics>,
    /// Number of files pruned by Iceberg statistics
    pub files_pruned_by_stats: usize,
    /// Number of files pruned by partition filter
    pub files_pruned_by_partition: usize,
}

/// Execution context - manages tables and executes queries
pub struct ExecutionContext {
    catalog: InMemoryCatalog,
    tables: HashMap<String, Arc<dyn TableProvider>>,
    optimizer: Optimizer,
    /// Number of parallel partitions for execution (defaults to CPU count)
    parallel_partitions: usize,
    /// Memory pool for tracking and limiting memory usage
    memory_pool: SharedMemoryPool,
    /// Execution configuration
    config: ExecutionConfig,
}

impl Default for ExecutionContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ExecutionContext {
    pub fn new() -> Self {
        let config = ExecutionConfig::default();
        let memory_pool = create_memory_pool(config.memory_limit);
        Self {
            catalog: InMemoryCatalog::new(),
            tables: HashMap::new(),
            optimizer: Optimizer::new(),
            parallel_partitions: rayon::current_num_threads(),
            memory_pool,
            config,
        }
    }

    /// Create a new context with a specific memory limit
    pub fn with_memory_limit(max_bytes: usize) -> Self {
        let config = ExecutionConfig::default().with_memory_limit(max_bytes);
        let memory_pool = create_memory_pool(max_bytes);
        Self {
            catalog: InMemoryCatalog::new(),
            tables: HashMap::new(),
            optimizer: Optimizer::new(),
            parallel_partitions: rayon::current_num_threads(),
            memory_pool,
            config,
        }
    }

    /// Create a new context with custom configuration
    pub fn with_config(config: ExecutionConfig) -> Self {
        let memory_pool = create_memory_pool(config.memory_limit);
        Self {
            catalog: InMemoryCatalog::new(),
            tables: HashMap::new(),
            optimizer: Optimizer::new(),
            parallel_partitions: rayon::current_num_threads(),
            memory_pool,
            config,
        }
    }

    /// Collect table statistics from all registered table providers
    fn collect_table_statistics(
        &self,
    ) -> HashMap<String, crate::physical::operators::TableStatistics> {
        let mut stats = HashMap::new();
        for (name, provider) in &self.tables {
            if let Some(table_stats) = provider.statistics() {
                stats.insert(name.clone(), table_stats);
            }
        }
        stats
    }

    /// Get the memory pool
    pub fn memory_pool(&self) -> &SharedMemoryPool {
        &self.memory_pool
    }

    /// Get the execution config
    pub fn config(&self) -> &ExecutionConfig {
        &self.config
    }

    /// Get current memory usage
    pub fn memory_used(&self) -> usize {
        self.memory_pool.used()
    }

    /// Get available memory
    pub fn memory_available(&self) -> usize {
        self.memory_pool.available()
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

    /// Register a table from a Lance dataset directory (e.g. `orders.lance`).
    ///
    /// Requires the `lance` cargo feature. Column projection is pushed into
    /// Lance; see `storage::LanceTable` for what the Lance path does and does
    /// not get relative to the Parquet path.
    #[cfg(feature = "lance")]
    pub fn register_lance(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let table = crate::storage::LanceTable::try_new(path)?;
        self.register_table_provider(name, Arc::new(table));
        Ok(())
    }

    /// Register a HISTORICAL version of a Lance dataset — time travel.
    ///
    /// Lance commits a new manifest on every append, overwrite, delete and
    /// index build, keeping the old ones, so `version` selects a past snapshot
    /// of the same path. The table is immutable at that version.
    ///
    /// Registering one path twice under two names and two versions is a
    /// supported way to diff snapshots in a single query:
    ///
    /// ```ignore
    /// ctx.register_lance_version("t_old", "./data/t.lance", 1)?;
    /// ctx.register_lance("t_new", "./data/t.lance")?;
    /// ctx.sql("SELECT COUNT(*) FROM t_new WHERE id NOT IN (SELECT id FROM t_old)").await?;
    /// ```
    ///
    /// An unknown version is an error, never a silent fall back to the latest.
    #[cfg(feature = "lance")]
    pub fn register_lance_version(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
        version: u64,
    ) -> Result<()> {
        let table = crate::storage::LanceTable::try_new_at_version(path, version)?;
        self.register_table_provider(name, Arc::new(table));
        Ok(())
    }

    /// Register a Lance dataset and immediately compute its column statistics.
    ///
    /// Same as [`register_lance`](Self::register_lance) except the statistics
    /// scan (which Lance, unlike Parquet, cannot answer from metadata) is paid
    /// here rather than inside whichever query first invokes the optimizer.
    /// Benchmarks use this so load cost is reported as load cost.
    #[cfg(feature = "lance")]
    pub fn register_lance_warm(
        &mut self,
        name: impl Into<String>,
        path: impl AsRef<Path>,
    ) -> Result<()> {
        let table = crate::storage::LanceTable::try_new(path)?;
        table.warm_statistics();
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

        // Optimize (with table statistics for better join planning)
        let optimize_start = Instant::now();
        let table_stats = self.collect_table_statistics();
        let optimized = if table_stats.is_empty() {
            self.optimizer.optimize(logical)?
        } else {
            // Create a temporary optimizer with stats for this query
            let optimizer = Optimizer::new().with_table_statistics(table_stats);
            optimizer.optimize(logical)?
        };
        metrics.optimize_time = optimize_start.elapsed();
        if std::env::var("PLAN_DEBUG").is_ok() {
            eprintln!("[plan]\n{}", optimized);
        }

        // Physical planning with spillable operators for memory safety
        let physical_start = Instant::now();
        let mut planner =
            PhysicalPlanner::with_config(self.memory_pool.clone(), self.config.clone());
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

        // Capture memory metrics
        metrics.peak_memory_bytes = self.memory_pool.used();
        let spilled = self.memory_pool.spilled();
        if spilled > 0 {
            metrics.spill_metrics = Some(SpillMetrics {
                bytes_spilled: spilled,
                ..Default::default()
            });
        }

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
        // Use the same statistics-aware optimizer as sql(), otherwise the
        // debug view shows a different (stats-blind) plan than execution uses.
        let table_stats = self.collect_table_statistics();
        if table_stats.is_empty() {
            self.optimizer.optimize(logical)
        } else {
            let optimizer = Optimizer::new().with_table_statistics(table_stats);
            optimizer.optimize(logical)
        }
    }

    /// Get the physical plan for a query (for debugging)
    pub fn physical_plan(&self, query: &str) -> Result<Arc<dyn PhysicalOperator>> {
        let optimized = self.optimized_plan(query)?;

        let mut planner =
            PhysicalPlanner::with_config(self.memory_pool.clone(), self.config.clone());
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

    /// The provider backing `name`, if it is registered.
    ///
    /// Distributed execution needs this to ask a table what files it is made
    /// of before replacing it with the shard of those files this node owns.
    pub fn table_provider(&self, name: &str) -> Option<Arc<dyn TableProvider>> {
        self.tables.get(name).cloned()
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
    let _ = print_batches(std::slice::from_ref(batch));
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

    // Print memory metrics
    if result.metrics.peak_memory_bytes > 0 {
        println!(
            "Memory: peak={}",
            format_bytes(result.metrics.peak_memory_bytes)
        );
    }

    // Print spill metrics if any
    if let Some(ref spill) = result.metrics.spill_metrics {
        println!(
            "Spill: {} spilled, {} partitions, {} files",
            format_bytes(spill.bytes_spilled),
            spill.partitions_spilled,
            spill.spill_files_created
        );
    }

    // Print pruning stats
    if result.metrics.files_pruned_by_stats > 0 || result.metrics.files_pruned_by_partition > 0 {
        println!(
            "Pruning: {} by stats, {} by partition",
            result.metrics.files_pruned_by_stats, result.metrics.files_pruned_by_partition
        );
    }

    println!();

    if !result.batches.is_empty() {
        let display: Vec<_> = result
            .batches
            .iter()
            .map(summarize_vector_columns)
            .collect();
        let _ = print_batches(&display);
    }
}

/// Replace vector/list columns with a short text summary for display.
///
/// Arrow's `print_batches` renders a `FixedSizeList<Float32, 384>` cell as all
/// 384 numbers, which produces a 5,000-character-wide table for a single
/// embedding and hides every other column. Only the *printed* copy is
/// rewritten; `QueryResult::batches` still holds the real vectors.
fn summarize_vector_columns(batch: &RecordBatch) -> RecordBatch {
    use arrow::array::{Array, ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    let needs_summary = batch
        .schema()
        .fields()
        .iter()
        .any(|f| crate::planner::vector_types::is_opaque_nested(f.data_type()));
    if !needs_summary {
        return batch.clone();
    }

    let mut fields: Vec<Field> = Vec::with_capacity(batch.num_columns());
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(batch.num_columns());
    for (i, field) in batch.schema().fields().iter().enumerate() {
        let col = batch.column(i);
        if !crate::planner::vector_types::is_opaque_nested(field.data_type()) {
            fields.push(field.as_ref().clone());
            columns.push(col.clone());
            continue;
        }
        let summary: StringArray = (0..col.len())
            .map(|r| {
                if col.is_null(r) {
                    None
                } else {
                    Some(summarize_nested_cell(col, r))
                }
            })
            .collect();
        fields.push(Field::new(field.name(), DataType::Utf8, true));
        columns.push(Arc::new(summary));
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap_or_else(|_| batch.clone())
}

/// Render one nested cell as a short, human-readable string for the printed
/// result table.
///
/// Uses Arrow's own value formatter for the leaves rather than a hand-rolled
/// per-type match: this runs on display output only, so correctness of the
/// rendering matters more than speed, and Arrow already knows how to print
/// every leaf type the reader accepts. Vectors are truncated to a head plus a
/// dimension; structs print all their fields, since a struct is a handful of
/// named values rather than a thousand anonymous ones.
fn summarize_nested_cell(col: &arrow::array::ArrayRef, row: usize) -> String {
    use arrow::array::{Array, FixedSizeListArray, LargeListArray, ListArray, StructArray};
    use arrow::datatypes::DataType;
    use arrow::util::display::{ArrayFormatter, FormatOptions};

    const HEAD: usize = 3;

    fn leaf(arr: &arrow::array::ArrayRef, i: usize) -> String {
        let opts = FormatOptions::default().with_null("NULL");
        match ArrayFormatter::try_new(arr.as_ref(), &opts) {
            Ok(f) => f.value(i).to_string(),
            Err(_) => format!("<{}>", arr.data_type()),
        }
    }

    // Recurse one level so a struct containing a vector still gets truncated.
    fn cell(arr: &arrow::array::ArrayRef, i: usize, depth: usize) -> String {
        if arr.is_null(i) {
            return "NULL".to_string();
        }
        if depth > 2 {
            return leaf(arr, i);
        }
        match arr.data_type() {
            DataType::Struct(fields) => {
                let Some(s) = arr.as_any().downcast_ref::<StructArray>() else {
                    return leaf(arr, i);
                };
                let body: Vec<String> = fields
                    .iter()
                    .enumerate()
                    .map(|(c, f)| format!("{}: {}", f.name(), cell(s.column(c), i, depth + 1)))
                    .collect();
                format!("{{{}}}", body.join(", "))
            }
            DataType::FixedSizeList(_, _) | DataType::List(_) | DataType::LargeList(_) => {
                let child = match arr.data_type() {
                    DataType::FixedSizeList(_, _) => arr
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .map(|a| a.value(i)),
                    DataType::List(_) => {
                        arr.as_any().downcast_ref::<ListArray>().map(|a| a.value(i))
                    }
                    _ => arr
                        .as_any()
                        .downcast_ref::<LargeListArray>()
                        .map(|a| a.value(i)),
                };
                let Some(child) = child else {
                    return leaf(arr, i);
                };
                let n = child.len();
                let head: Vec<String> = (0..n.min(HEAD))
                    .map(|k| cell(&child, k, depth + 1))
                    .collect();
                if n > HEAD {
                    format!("[{}, ...] ({})", head.join(", "), n)
                } else {
                    format!("[{}]", head.join(", "))
                }
            }
            _ => leaf(arr, i),
        }
    }

    cell(col, row, 0)
}

/// Format bytes into human-readable string
fn format_bytes(bytes: usize) -> String {
    const KB: usize = 1024;
    const MB: usize = KB * 1024;
    const GB: usize = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
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
