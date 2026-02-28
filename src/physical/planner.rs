//! Physical planner - converts logical plans to physical plans

use crate::error::{QueryError, Result};
use crate::execution::{ExecutionConfig, SharedMemoryPool};
use crate::physical::operators::{
    AggregateExpr, ExternalSortExec, FilterExec, HashAggregateExec, HashJoinExec, LimitExec,
    MemoryTableExec, MorselAggregateExec, ProjectExec, SortExec, SpillableHashAggregateExec,
    SpillableHashJoinExec, SubqueryExecutor, TableProvider, UnionExec,
};
use crate::physical::PhysicalOperator;
use crate::planner::{BinaryOp, Expr, JoinType, LogicalPlan, PlanSchema};
use arrow::datatypes::{Field, Schema, SchemaRef};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

/// Physical planner that converts logical plans to physical execution plans
pub struct PhysicalPlanner {
    /// Table providers for accessing table data
    tables: HashMap<String, Arc<dyn TableProvider>>,
    /// Optional subquery executor for handling subqueries in filters
    subquery_executor: Option<SubqueryExecutor>,
    /// Memory pool for spillable operators
    memory_pool: Option<SharedMemoryPool>,
    /// Execution configuration for spillable operators
    config: Option<ExecutionConfig>,
    /// Cache of full table scans to avoid re-reading the same table multiple times.
    /// Key is table name, value is (full schema, all batches with no projection).
    scan_cache: RefCell<HashMap<String, (SchemaRef, Vec<arrow::record_batch::RecordBatch>)>>,
}

impl Default for PhysicalPlanner {
    fn default() -> Self {
        Self::new()
    }
}

impl PhysicalPlanner {
    /// Create a new physical planner without memory management (uses regular operators)
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            subquery_executor: None,
            memory_pool: None,
            config: None,
            scan_cache: RefCell::new(HashMap::new()),
        }
    }

    /// Create a physical planner with memory management (uses spillable operators)
    pub fn with_config(memory_pool: SharedMemoryPool, config: ExecutionConfig) -> Self {
        Self {
            tables: HashMap::new(),
            subquery_executor: None,
            memory_pool: Some(memory_pool),
            config: Some(config),
            scan_cache: RefCell::new(HashMap::new()),
        }
    }

    /// Check if spillable operators should be used (always true when memory pool is configured)
    fn use_spillable(&self) -> bool {
        self.memory_pool.is_some() && self.config.is_some()
    }

    /// Check if morsel execution should be used
    fn use_morsel_execution(&self) -> bool {
        self.config.is_some() && self.config.as_ref().unwrap().enable_morsel_execution
    }

    /// Try to extract Parquet files and filter from a logical plan for morsel execution
    /// Returns (files, input_schema, filter, projection) if the plan is suitable for morsel execution
    fn try_extract_parquet_source(
        &self,
        plan: &LogicalPlan,
    ) -> Option<(
        Vec<std::path::PathBuf>,
        arrow::datatypes::SchemaRef,
        Option<Expr>,
        Option<Vec<usize>>,
    )> {
        match plan {
            LogicalPlan::Scan(node) => {
                let provider = self.tables.get(&node.table_name)?;
                let files = provider.parquet_files()?;
                let input_schema = provider.schema();
                let filter = node.filter.clone();
                let projection = node.projection.clone();
                Some((files, input_schema, filter, projection))
            }
            LogicalPlan::Filter(node) => {
                // Check if input is a Scan over ParquetTable
                if let LogicalPlan::Scan(scan_node) = node.input.as_ref() {
                    let provider = self.tables.get(&scan_node.table_name)?;
                    let files = provider.parquet_files()?;
                    let input_schema = provider.schema();
                    // Combine filters if scan also has a filter
                    let filter = match &scan_node.filter {
                        Some(scan_filter) => Some(Expr::BinaryExpr {
                            left: Box::new(scan_filter.clone()),
                            op: crate::planner::BinaryOp::And,
                            right: Box::new(node.predicate.clone()),
                        }),
                        None => Some(node.predicate.clone()),
                    };
                    let projection = scan_node.projection.clone();
                    Some((files, input_schema, filter, projection))
                } else {
                    None
                }
            }
            LogicalPlan::Project(node) => {
                // Project can be handled if input is Scan or Filter->Scan
                self.try_extract_parquet_source(&node.input)
            }
            _ => None,
        }
    }

    /// Helper to create a FilterExec with subquery executor if needed
    fn create_filter(&self, input: Arc<dyn PhysicalOperator>, predicate: Expr) -> FilterExec {
        let has_subquery = predicate.contains_subquery();
        // Pre-compute uncorrelated scalar subqueries as literal values
        let predicate = if has_subquery {
            if let Some(ref executor) = self.subquery_executor {
                Self::precompute_uncorrelated_scalars(predicate, executor)
            } else {
                predicate
            }
        } else {
            predicate
        };
        let still_has_subquery = predicate.contains_subquery();
        let filter = FilterExec::new(input, predicate);
        if still_has_subquery {
            if let Some(ref executor) = self.subquery_executor {
                return filter.with_subquery_executor(executor.clone());
            }
        }
        filter
    }

    /// Replace uncorrelated scalar subqueries with their computed literal values.
    fn precompute_uncorrelated_scalars(expr: Expr, executor: &SubqueryExecutor) -> Expr {
        match expr {
            Expr::ScalarSubquery(ref plan) => {
                let is_correlated = crate::physical::operators::is_correlated_subquery_plan(plan);
                if !is_correlated {
                    match executor.execute_scalar(plan) {
                        Ok(scalar) => Expr::Literal(scalar),
                        Err(_) => expr,
                    }
                } else {
                    expr
                }
            }
            Expr::BinaryExpr { left, op, right } => {
                let left = Self::precompute_uncorrelated_scalars(*left, executor);
                let right = Self::precompute_uncorrelated_scalars(*right, executor);
                Expr::BinaryExpr {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                }
            }
            Expr::UnaryExpr { op, expr: inner } => {
                let inner = Self::precompute_uncorrelated_scalars(*inner, executor);
                Expr::UnaryExpr {
                    op,
                    expr: Box::new(inner),
                }
            }
            Expr::Cast {
                expr: inner,
                data_type,
            } => {
                let inner = Self::precompute_uncorrelated_scalars(*inner, executor);
                Expr::Cast {
                    expr: Box::new(inner),
                    data_type,
                }
            }
            Expr::Alias { expr: inner, name } => {
                let inner = Self::precompute_uncorrelated_scalars(*inner, executor);
                Expr::Alias {
                    expr: Box::new(inner),
                    name,
                }
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                let operand =
                    operand.map(|o| Box::new(Self::precompute_uncorrelated_scalars(*o, executor)));
                let when_then = when_then
                    .into_iter()
                    .map(|(w, t)| {
                        (
                            Self::precompute_uncorrelated_scalars(w, executor),
                            Self::precompute_uncorrelated_scalars(t, executor),
                        )
                    })
                    .collect();
                let else_expr = else_expr
                    .map(|e| Box::new(Self::precompute_uncorrelated_scalars(*e, executor)));
                Expr::Case {
                    operand,
                    when_then,
                    else_expr,
                }
            }
            Expr::ScalarFunc { func, args } => {
                let args = args
                    .into_iter()
                    .map(|a| Self::precompute_uncorrelated_scalars(a, executor))
                    .collect();
                Expr::ScalarFunc { func, args }
            }
            Expr::InList {
                expr: inner,
                list,
                negated,
            } => {
                let inner = Self::precompute_uncorrelated_scalars(*inner, executor);
                let list = list
                    .into_iter()
                    .map(|e| Self::precompute_uncorrelated_scalars(e, executor))
                    .collect();
                Expr::InList {
                    expr: Box::new(inner),
                    list,
                    negated,
                }
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => {
                let inner = Self::precompute_uncorrelated_scalars(*inner, executor);
                let low = Self::precompute_uncorrelated_scalars(*low, executor);
                let high = Self::precompute_uncorrelated_scalars(*high, executor);
                Expr::Between {
                    expr: Box::new(inner),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated,
                }
            }
            Expr::Exists { subquery, negated } => {
                if !crate::physical::operators::is_correlated_subquery_plan(&subquery) {
                    match executor.execute_exists(&subquery) {
                        Ok(exists) => {
                            let result = if negated { !exists } else { exists };
                            Expr::Literal(crate::planner::ScalarValue::Boolean(result))
                        }
                        Err(_) => Expr::Exists { subquery, negated },
                    }
                } else {
                    Expr::Exists { subquery, negated }
                }
            }
            Expr::InSubquery {
                expr: inner,
                subquery,
                negated,
            } => {
                if !crate::physical::operators::is_correlated_subquery_plan(&subquery) {
                    // Precompute the inner expression first
                    let inner = Self::precompute_uncorrelated_scalars(*inner, executor);
                    // Keep InSubquery as-is but with precomputed inner expr
                    Expr::InSubquery {
                        expr: Box::new(inner),
                        subquery,
                        negated,
                    }
                } else {
                    Expr::InSubquery {
                        expr: inner,
                        subquery,
                        negated,
                    }
                }
            }
            _ => expr,
        }
    }

    /// Register a table provider
    pub fn register_table(&mut self, name: impl Into<String>, provider: Arc<dyn TableProvider>) {
        let name = name.into();
        self.tables.insert(name.clone(), provider.clone());

        // Also register with subquery executor if it exists
        if let Some(ref executor) = self.subquery_executor {
            executor.register_table(name, provider);
        }
    }

    /// Enable subquery execution support
    pub fn enable_subquery_execution(&mut self) {
        // Clone the tables HashMap for the subquery executor
        let tables = self.tables.clone();
        self.subquery_executor = Some(SubqueryExecutor::from_tables(tables));
    }

    /// Set the subquery executor (used by subquery executor to pass itself for nested subqueries)
    pub fn set_subquery_executor(&mut self, executor: Option<SubqueryExecutor>) {
        self.subquery_executor = executor;
    }

    /// Collect all scan projections for each table name in the plan.
    /// Used to compute union projections for scan caching.
    fn collect_scan_projections(
        plan: &LogicalPlan,
        table_scans: &mut HashMap<String, Vec<Option<Vec<usize>>>>,
    ) {
        match plan {
            LogicalPlan::Scan(node) => {
                table_scans
                    .entry(node.table_name.clone())
                    .or_default()
                    .push(node.projection.clone());
            }
            _ => {
                for child in plan.children() {
                    Self::collect_scan_projections(child, table_scans);
                }
            }
        }
    }

    /// Compute the union projection for a set of projections.
    /// Returns None if any scan needs all columns (no projection).
    fn union_projection(projections: &[Option<Vec<usize>>]) -> Option<Vec<usize>> {
        let mut union_set = std::collections::BTreeSet::new();
        for proj in projections {
            match proj {
                None => return None, // One scan needs all columns
                Some(indices) => {
                    for &i in indices {
                        union_set.insert(i);
                    }
                }
            }
        }
        Some(union_set.into_iter().collect())
    }

    /// Pre-scan tables that are accessed multiple times and cache the results.
    fn prescan_shared_tables(&self, logical: &LogicalPlan) {
        use rayon::prelude::*;

        let mut table_scans: HashMap<String, Vec<Option<Vec<usize>>>> = HashMap::new();
        Self::collect_scan_projections(logical, &mut table_scans);

        // Build scan tasks: only prescan tables that are accessed 2+ times.
        // Single-use tables will be read on-demand, reducing peak memory.
        let scan_tasks: Vec<_> = table_scans
            .iter()
            .filter(|(_, projections)| projections.len() > 1) // Only shared tables
            .filter_map(|(table_name, projections)| {
                let provider = self.tables.get(table_name)?;
                let proj = Self::union_projection(projections);
                Some((table_name.clone(), provider.clone(), proj))
            })
            .collect();

        // Execute all scans in parallel using rayon
        let results: Vec<_> = scan_tasks
            .par_iter()
            .filter_map(|(table_name, provider, proj)| {
                let batches = provider.scan(proj.as_deref()).ok()?;
                let schema = match proj {
                    Some(indices) => {
                        let base_schema = provider.schema();
                        let fields: Vec<_> = indices
                            .iter()
                            .map(|&i| base_schema.field(i).clone())
                            .collect();
                        Arc::new(Schema::new(fields))
                    }
                    None => provider.schema(),
                };
                Some((table_name.clone(), schema, batches))
            })
            .collect();

        let mut cache = self.scan_cache.borrow_mut();
        for (table_name, schema, batches) in results {
            cache.insert(table_name, (schema, batches));
        }
    }

    /// Check if a plan subtree contains an Aggregate (looking through Project/SubqueryAlias).
    fn plan_contains_aggregate(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Aggregate(_) => true,
            LogicalPlan::Project(p) => Self::plan_contains_aggregate(&p.input),
            LogicalPlan::SubqueryAlias(s) => Self::plan_contains_aggregate(&s.input),
            _ => false,
        }
    }

    /// Estimate the output row count of a logical plan using table statistics.
    /// Returns None if statistics are not available.
    fn estimate_output_rows(&self, plan: &LogicalPlan) -> Option<usize> {
        match plan {
            LogicalPlan::Scan(node) => {
                let provider = self.tables.get(&node.table_name)?;
                let stats = provider.statistics()?;
                let rows = stats.row_count;
                if let Some(ref filter) = node.filter {
                    let sel = Self::estimate_leaf_selectivity(filter);
                    Some(std::cmp::max((rows as f64 * sel) as usize, 1))
                } else {
                    Some(rows)
                }
            }
            LogicalPlan::Filter(node) => {
                let input_rows = self.estimate_output_rows(&node.input)?;
                let sel = if matches!(*node.input, LogicalPlan::Scan(_)) {
                    Self::estimate_leaf_selectivity(&node.predicate)
                } else {
                    0.3
                };
                Some(std::cmp::max((input_rows as f64 * sel) as usize, 1))
            }
            LogicalPlan::Project(node) => self.estimate_output_rows(&node.input),
            LogicalPlan::SubqueryAlias(node) => self.estimate_output_rows(&node.input),
            LogicalPlan::Join(node) => {
                let left = self.estimate_output_rows(&node.left).unwrap_or(10_000);
                let right = self.estimate_output_rows(&node.right).unwrap_or(10_000);
                match node.join_type {
                    JoinType::Semi | JoinType::Anti => Some(left / 2),
                    JoinType::Inner => Some(std::cmp::max(left, right) / 10),
                    JoinType::Left => Some(left),
                    JoinType::Right => Some(right),
                    _ => Some(std::cmp::max(left, right)),
                }
            }
            LogicalPlan::Aggregate(node) => {
                let input_rows = self.estimate_output_rows(&node.input).unwrap_or(10_000);
                if node.group_by.is_empty() {
                    Some(1)
                } else {
                    Some(input_rows / 10)
                }
            }
            LogicalPlan::Limit(node) => node
                .fetch
                .or_else(|| self.estimate_output_rows(&node.input)),
            _ => None,
        }
    }

    /// Estimate selectivity for leaf-level filters (on scans).
    /// Only uses compound selectivity for 3+ AND conjuncts to avoid
    /// underestimating common 2-condition range filters.
    fn estimate_leaf_selectivity(expr: &Expr) -> f64 {
        let n = Self::count_and_conjuncts(expr);
        if n >= 3 {
            Self::estimate_expr_selectivity(expr).max(0.01)
        } else {
            0.3
        }
    }

    fn count_and_conjuncts(expr: &Expr) -> usize {
        match expr {
            Expr::BinaryExpr {
                op: BinaryOp::And,
                left,
                right,
            } => Self::count_and_conjuncts(left) + Self::count_and_conjuncts(right),
            _ => 1,
        }
    }

    fn estimate_expr_selectivity(expr: &Expr) -> f64 {
        match expr {
            Expr::BinaryExpr {
                op: BinaryOp::And,
                left,
                right,
            } => Self::estimate_expr_selectivity(left) * Self::estimate_expr_selectivity(right),
            Expr::BinaryExpr {
                op: BinaryOp::Or,
                left,
                right,
            } => {
                let l = Self::estimate_expr_selectivity(left);
                let r = Self::estimate_expr_selectivity(right);
                (l + r - l * r).min(1.0)
            }
            Expr::BinaryExpr {
                op: BinaryOp::Eq, ..
            } => 0.1,
            Expr::BinaryExpr {
                op: BinaryOp::Gt | BinaryOp::Lt | BinaryOp::GtEq | BinaryOp::LtEq,
                ..
            } => 0.33,
            Expr::BinaryExpr {
                op: BinaryOp::Like, ..
            } => 0.1,
            Expr::BinaryExpr {
                op: BinaryOp::NotEq,
                ..
            } => 0.9,
            Expr::InList { list, negated, .. } => {
                if *negated {
                    0.9
                } else {
                    (list.len() as f64 * 0.05).min(0.5)
                }
            }
            Expr::Between { negated, .. } => {
                if *negated {
                    0.75
                } else {
                    0.25
                }
            }
            _ => 0.3,
        }
    }

    /// Convert a logical plan to a physical plan
    pub fn create_physical_plan(&self, logical: &LogicalPlan) -> Result<Arc<dyn PhysicalOperator>> {
        // Pre-scan tables that are accessed multiple times to avoid redundant parquet reads
        self.prescan_shared_tables(logical);
        self.create_physical_plan_inner(logical)
    }

    fn create_physical_plan_inner(
        &self,
        logical: &LogicalPlan,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        match logical {
            LogicalPlan::Scan(node) => {
                let provider = self
                    .tables
                    .get(&node.table_name)
                    .ok_or_else(|| QueryError::TableNotFound(node.table_name.clone()))?;

                // Use the logical schema (with aliases) instead of the provider schema
                let logical_schema = plan_schema_to_arrow(&node.schema);

                // Check scan cache for pre-scanned tables (shared across multiple aliases)
                let cache = self.scan_cache.borrow();
                let exec = if let Some((cached_schema, cached_batches)) =
                    cache.get(&node.table_name)
                {
                    // Cache hit: project from cached union-projected scan
                    let batches = if let Some(ref requested_indices) = node.projection {
                        // Map requested projection indices to positions in cached batches
                        // Cached batches use union projection indices
                        let cached_fields: Vec<&str> = cached_schema
                            .fields()
                            .iter()
                            .map(|f| f.name().as_str())
                            .collect();
                        let provider_schema = provider.schema();

                        cached_batches
                            .iter()
                            .map(|batch| {
                                let columns: Vec<arrow::array::ArrayRef> = requested_indices
                                    .iter()
                                    .map(|&orig_idx| {
                                        // Find the position of this original column in the cached batches
                                        let col_name = provider_schema.field(orig_idx).name();
                                        let cached_pos = cached_fields
                                            .iter()
                                            .position(|&n| n == col_name.as_str())
                                            .unwrap_or(orig_idx);
                                        batch.column(cached_pos).clone()
                                    })
                                    .collect();
                                let fields: Vec<_> = requested_indices
                                    .iter()
                                    .map(|&i| logical_schema.field(i).clone())
                                    .collect();
                                let schema = Arc::new(Schema::new(fields));
                                arrow::record_batch::RecordBatch::try_new(schema, columns).map_err(
                                    |e| QueryError::Execution(format!("Projection failed: {}", e)),
                                )
                            })
                            .collect::<Result<Vec<_>>>()?
                    } else {
                        cached_batches.clone()
                    };

                    let schema = match &node.projection {
                        Some(indices) => {
                            let fields: Vec<_> = indices
                                .iter()
                                .map(|&i| logical_schema.field(i).clone())
                                .collect();
                            Arc::new(Schema::new(fields))
                        }
                        None => logical_schema,
                    };
                    MemoryTableExec::new(&node.table_name, schema, batches, None)
                } else {
                    // No cache: use scan_with_filter for Parquet row group pruning
                    drop(cache);
                    let batches = provider
                        .scan_with_filter(node.projection.as_deref(), node.filter.as_ref())?;
                    let schema = match &node.projection {
                        Some(indices) => {
                            let fields: Vec<_> = indices
                                .iter()
                                .map(|&i| logical_schema.field(i).clone())
                                .collect();
                            Arc::new(Schema::new(fields))
                        }
                        None => logical_schema,
                    };
                    MemoryTableExec::new(&node.table_name, schema, batches, None)
                };

                // If there's a filter on the scan, wrap with FilterExec
                match &node.filter {
                    Some(predicate) => {
                        let filter = self.create_filter(Arc::new(exec), predicate.clone());
                        Ok(Arc::new(filter))
                    }
                    None => Ok(Arc::new(exec)),
                }
            }

            LogicalPlan::Filter(node) => {
                let input = self.create_physical_plan_inner(&node.input)?;
                let filter = self.create_filter(input, node.predicate.clone());
                Ok(Arc::new(filter))
            }

            LogicalPlan::Project(node) => {
                let input = self.create_physical_plan_inner(&node.input)?;
                let schema = plan_schema_to_arrow(&node.schema);
                let mut project = ProjectExec::new(input, node.exprs.clone(), schema);
                // If any projection expression contains a subquery, attach executor
                let has_subquery = node.exprs.iter().any(|e| e.contains_subquery());
                if has_subquery {
                    if let Some(ref executor) = self.subquery_executor {
                        project = project.with_subquery_executor(executor.clone());
                    }
                }
                Ok(Arc::new(project))
            }

            LogicalPlan::Join(node) => {
                // For Inner joins, swap build/probe sides so smaller side is the build side.
                // The left side of HashJoinExec is the build side (builds the hash table).
                let should_swap = matches!(node.join_type, JoinType::Inner) && {
                    let left_rows = self.estimate_output_rows(&node.left);
                    let right_rows = self.estimate_output_rows(&node.right);
                    match (left_rows, right_rows) {
                        (Some(l), Some(r)) => l > r * 2, // Swap if left is 2x larger
                        _ => false,
                    }
                };

                // For Left/Semi/Anti joins, we can't swap children (it would change semantics),
                // but we CAN build the hash table from the right (smaller) side.
                // This is especially important for decorrelated Left joins where:
                //   left = large join result, right = grouped aggregate (much smaller)
                // For Semi/Anti: when right is small (e.g., filtered dimension table),
                // building from right and probing with left outputs matching/unmatching
                // probe (left) rows directly per-batch.
                let build_right_for_left = matches!(
                    node.join_type,
                    JoinType::Left | JoinType::Semi | JoinType::Anti
                ) && {
                    let left_rows = self.estimate_output_rows(&node.left);
                    let right_rows = self.estimate_output_rows(&node.right);
                    let right_is_aggregate = Self::plan_contains_aggregate(&node.right);
                    match (left_rows, right_rows) {
                        (Some(l), Some(r)) => {
                            if right_is_aggregate {
                                l > (r / 10).max(1) * 2
                            } else {
                                l > r * 2
                            }
                        }
                        _ => false,
                    }
                };

                let (left_plan, right_plan, on) = if should_swap {
                    // Swap: right becomes build side (left), left becomes probe side (right)
                    let swapped_on: Vec<(Expr, Expr)> = node
                        .on
                        .iter()
                        .map(|(l, r)| (r.clone(), l.clone()))
                        .collect();
                    (&node.right, &node.left, swapped_on)
                } else {
                    (&node.left, &node.right, node.on.clone())
                };

                let left = self.create_physical_plan_inner(left_plan)?;
                let right = self.create_physical_plan_inner(right_plan)?;

                // For Semi/Anti joins, the filter must be evaluated inside the join
                // because the output doesn't include right-side columns
                let is_semi_anti = matches!(node.join_type, JoinType::Semi | JoinType::Anti);

                if is_semi_anti && node.filter.is_some() {
                    // SpillableHashJoinExec doesn't support with_filter yet,
                    // use regular HashJoinExec for this case
                    let join = HashJoinExec::with_filter(
                        left,
                        right,
                        on,
                        node.join_type,
                        node.filter.clone(),
                    );
                    Ok(Arc::new(join))
                } else if self.use_spillable() {
                    // Use spillable hash join with memory management
                    let join = SpillableHashJoinExec::new(
                        left,
                        right,
                        on,
                        node.join_type,
                        self.memory_pool.clone().unwrap(),
                        self.config.clone().unwrap(),
                    )
                    .with_build_right(build_right_for_left);

                    // Apply additional filter if present
                    match &node.filter {
                        Some(predicate) => {
                            let filter = self.create_filter(Arc::new(join), predicate.clone());
                            Ok(Arc::new(filter))
                        }
                        None => Ok(Arc::new(join)),
                    }
                } else {
                    // Use regular hash join (no memory management)
                    let join = HashJoinExec::new(left, right, on, node.join_type)
                        .with_build_right(build_right_for_left);

                    // Apply additional filter if present (for non-Semi/Anti joins)
                    match &node.filter {
                        Some(predicate) => {
                            let filter = self.create_filter(Arc::new(join), predicate.clone());
                            Ok(Arc::new(filter))
                        }
                        None => Ok(Arc::new(join)),
                    }
                }
            }

            LogicalPlan::Aggregate(node) => {
                // Convert logical aggregate expressions to physical
                let aggregates = extract_aggregates(&node.aggregates);

                let schema = plan_schema_to_arrow(&node.schema);

                // Try morsel execution for Parquet-based aggregations
                // Skip morsel path for DISTINCT aggregates (not yet supported)
                let has_distinct = aggregates.iter().any(|a| a.distinct);
                if self.use_morsel_execution() && !has_distinct {
                    if let Some((files, input_schema, filter, projection)) =
                        self.try_extract_parquet_source(&node.input)
                    {
                        // Use morsel-driven parallel aggregation
                        let morsel_agg = MorselAggregateExec::new(
                            files,
                            input_schema,
                            projection,
                            filter,
                            node.group_by.clone(),
                            aggregates,
                            schema,
                        );
                        return Ok(Arc::new(morsel_agg));
                    }
                }

                // Fall back to regular execution
                let input = self.create_physical_plan_inner(&node.input)?;

                if self.use_spillable() {
                    // Convert to spillable AggregateExpr type
                    let spillable_aggs: Vec<crate::physical::operators::spillable::AggregateExpr> =
                        aggregates
                            .into_iter()
                            .map(|a| crate::physical::operators::spillable::AggregateExpr {
                                func: a.func,
                                input: a.input,
                                distinct: a.distinct,
                                second_arg: a.second_arg,
                            })
                            .collect();

                    let agg = SpillableHashAggregateExec::new(
                        input,
                        node.group_by.clone(),
                        spillable_aggs,
                        schema,
                        self.memory_pool.clone().unwrap(),
                        self.config.clone().unwrap(),
                    );
                    Ok(Arc::new(agg))
                } else {
                    let agg =
                        HashAggregateExec::new(input, node.group_by.clone(), aggregates, schema);
                    Ok(Arc::new(agg))
                }
            }

            LogicalPlan::Sort(node) => {
                let input = self.create_physical_plan_inner(&node.input)?;
                if self.use_spillable() {
                    let sort = ExternalSortExec::new(
                        input,
                        node.order_by.clone(),
                        self.memory_pool.clone().unwrap(),
                        self.config.clone().unwrap(),
                    );
                    Ok(Arc::new(sort))
                } else {
                    let sort = SortExec::new(input, node.order_by.clone());
                    Ok(Arc::new(sort))
                }
            }

            LogicalPlan::Limit(node) => {
                // Top-K optimization: fuse Sort+Limit into a single SortExec with fetch
                if node.skip == 0 {
                    if let Some(fetch) = node.fetch {
                        if let LogicalPlan::Sort(sort_node) = node.input.as_ref() {
                            // Fuse: create SortExec with fetch limit
                            let sort_input = self.create_physical_plan_inner(&sort_node.input)?;
                            if self.use_spillable() {
                                let sort = ExternalSortExec::with_fetch(
                                    sort_input,
                                    sort_node.order_by.clone(),
                                    self.memory_pool.clone().unwrap(),
                                    self.config.clone().unwrap(),
                                    fetch,
                                );
                                return Ok(Arc::new(sort));
                            } else {
                                let sort = SortExec::with_fetch(
                                    sort_input,
                                    sort_node.order_by.clone(),
                                    fetch,
                                );
                                return Ok(Arc::new(sort));
                            }
                        }
                    }
                }
                let input = self.create_physical_plan_inner(&node.input)?;
                let limit = LimitExec::new(input, node.skip, node.fetch);
                Ok(Arc::new(limit))
            }

            LogicalPlan::Distinct(node) => {
                // Implement distinct as group by all columns
                let input = self.create_physical_plan_inner(&node.input)?;
                let input_schema = input.schema();

                let group_by: Vec<Expr> = input_schema
                    .fields()
                    .iter()
                    .map(|f| Expr::column(f.name().clone()))
                    .collect();

                if self.use_spillable() {
                    let agg = SpillableHashAggregateExec::new(
                        input,
                        group_by,
                        vec![],
                        input_schema,
                        self.memory_pool.clone().unwrap(),
                        self.config.clone().unwrap(),
                    );
                    Ok(Arc::new(agg))
                } else {
                    let agg = HashAggregateExec::new(input, group_by, vec![], input_schema);
                    Ok(Arc::new(agg))
                }
            }

            LogicalPlan::Union(node) => {
                if node.inputs.is_empty() {
                    return Err(QueryError::Plan("Union with no inputs".to_string()));
                }

                let physical_inputs: Result<Vec<_>> = node
                    .inputs
                    .iter()
                    .map(|input| self.create_physical_plan_inner(input))
                    .collect();
                let physical_inputs = physical_inputs?;

                let union_exec: Arc<dyn PhysicalOperator> =
                    Arc::new(UnionExec::new(physical_inputs));

                // If not UNION ALL, we need to remove duplicates using GROUP BY on all columns
                if !node.all {
                    // Create aggregate for distinct - group by all columns with no aggregates
                    let schema = plan_schema_to_arrow(&node.schema);
                    let group_by: Vec<Expr> = node
                        .schema
                        .fields()
                        .iter()
                        .map(|f| Expr::Column(crate::planner::Column::new(f.name.clone())))
                        .collect();

                    if self.use_spillable() {
                        let agg = SpillableHashAggregateExec::new(
                            union_exec,
                            group_by,
                            vec![],
                            schema,
                            self.memory_pool.clone().unwrap(),
                            self.config.clone().unwrap(),
                        );
                        Ok(Arc::new(agg))
                    } else {
                        let agg = HashAggregateExec::new(
                            union_exec,
                            group_by,
                            vec![], // No aggregates, just grouping for distinct
                            schema,
                        );
                        Ok(Arc::new(agg))
                    }
                } else {
                    Ok(union_exec)
                }
            }

            LogicalPlan::SubqueryAlias(node) => {
                // Just pass through to input
                self.create_physical_plan_inner(&node.input)
            }

            LogicalPlan::EmptyRelation(node) => {
                let schema = plan_schema_to_arrow(&node.schema);
                let batches = if node.produce_one_row {
                    // Create a single empty row
                    vec![arrow::record_batch::RecordBatch::new_empty(schema.clone())]
                } else {
                    vec![]
                };
                let exec = MemoryTableExec::new("empty", schema, batches, None);
                Ok(Arc::new(exec))
            }

            LogicalPlan::Values(node) => {
                // Evaluate constant expressions and create a batch
                let schema = plan_schema_to_arrow(&node.schema);
                // For now, return empty - proper implementation needs expression evaluation
                let exec = MemoryTableExec::new("values", schema, vec![], None);
                Ok(Arc::new(exec))
            }

            LogicalPlan::DelimJoin(node) => {
                use crate::physical::operators::DelimJoinExec;
                use std::sync::Arc as StdArc;

                // Create shared delim state
                let delim_state = StdArc::new(crate::physical::operators::DelimState::new());

                // Create the left (outer) side
                let left = self.create_physical_plan_inner(&node.left)?;

                // For the right side, we need to find DelimGet nodes and connect them
                // to the shared state.
                let right =
                    self.create_physical_plan_with_delim_state(&node.right, &delim_state)?;

                let schema = plan_schema_to_arrow(&node.schema);
                // Use with_delim_state to share the state with child DelimGet nodes
                let delim_join = DelimJoinExec::with_delim_state(
                    left,
                    right,
                    node.join_type,
                    node.delim_columns.clone(),
                    node.on.clone(),
                    schema,
                    delim_state,
                );

                Ok(Arc::new(delim_join))
            }

            LogicalPlan::DelimGet(_node) => {
                // DelimGet without a parent DelimJoin is an error
                Err(QueryError::Execution(
                    "DelimGet encountered without parent DelimJoin. \
                     Ensure the logical plan is correctly structured."
                        .to_string(),
                ))
            }
        }
    }

    /// Create physical plan for the inner side of a DelimJoin, connecting DelimGet nodes
    fn create_physical_plan_with_delim_state(
        &self,
        logical: &LogicalPlan,
        delim_state: &std::sync::Arc<crate::physical::operators::DelimState>,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        use crate::physical::operators::DelimGetExec;

        match logical {
            LogicalPlan::DelimGet(node) => {
                // Create DelimGetExec connected to the shared state
                let schema = plan_schema_to_arrow(&node.schema);
                let delim_get = DelimGetExec::new(std::sync::Arc::clone(delim_state), schema);
                Ok(Arc::new(delim_get))
            }
            LogicalPlan::Filter(node) => {
                let input = self.create_physical_plan_with_delim_state(&node.input, delim_state)?;
                let filter = self.create_filter(input, node.predicate.clone());
                Ok(Arc::new(filter))
            }
            LogicalPlan::Project(node) => {
                let input = self.create_physical_plan_with_delim_state(&node.input, delim_state)?;
                let schema = plan_schema_to_arrow(&node.schema);
                let project = ProjectExec::new(input, node.exprs.clone(), schema);
                Ok(Arc::new(project))
            }
            LogicalPlan::Join(node) => {
                let left = self.create_physical_plan_with_delim_state(&node.left, delim_state)?;
                let right = self.create_physical_plan_with_delim_state(&node.right, delim_state)?;
                let join = HashJoinExec::new(left, right, node.on.clone(), node.join_type);
                match &node.filter {
                    Some(predicate) => {
                        let filter = self.create_filter(Arc::new(join), predicate.clone());
                        Ok(Arc::new(filter))
                    }
                    None => Ok(Arc::new(join)),
                }
            }
            LogicalPlan::Aggregate(node) => {
                let input = self.create_physical_plan_with_delim_state(&node.input, delim_state)?;
                let aggregates = extract_aggregates(&node.aggregates);
                let schema = plan_schema_to_arrow(&node.schema);
                let agg = HashAggregateExec::new(input, node.group_by.clone(), aggregates, schema);
                Ok(Arc::new(agg))
            }
            LogicalPlan::SubqueryAlias(node) => {
                self.create_physical_plan_with_delim_state(&node.input, delim_state)
            }
            // For other node types, fall back to regular planning
            _ => self.create_physical_plan_inner(logical),
        }
    }
}

/// Convert PlanSchema to Arrow Schema
fn plan_schema_to_arrow(plan_schema: &PlanSchema) -> SchemaRef {
    let fields: Vec<Field> = plan_schema
        .fields()
        .iter()
        .map(|f| f.to_arrow_field())
        .collect();
    Arc::new(Schema::new(fields))
}

/// Extract aggregate expressions from logical expressions
fn extract_aggregates(exprs: &[Expr]) -> Vec<AggregateExpr> {
    let mut aggregates = Vec::new();

    for expr in exprs {
        collect_aggregates(expr, &mut aggregates);
    }

    aggregates
}

fn collect_aggregates(expr: &Expr, aggregates: &mut Vec<AggregateExpr>) {
    match expr {
        Expr::Aggregate {
            func,
            args,
            distinct,
        } => {
            let input = args.first().cloned().unwrap_or(Expr::Wildcard);
            // Capture second argument for functions like APPROX_PERCENTILE
            let second_arg = args.get(1).cloned();
            aggregates.push(AggregateExpr {
                func: *func,
                input,
                distinct: *distinct,
                second_arg,
            });
        }
        Expr::BinaryExpr { left, right, .. } => {
            collect_aggregates(left, aggregates);
            collect_aggregates(right, aggregates);
        }
        Expr::UnaryExpr { expr, .. } => {
            collect_aggregates(expr, aggregates);
        }
        Expr::Cast { expr, .. } => {
            collect_aggregates(expr, aggregates);
        }
        Expr::Alias { expr, .. } => {
            collect_aggregates(expr, aggregates);
        }
        Expr::ScalarFunc { args, .. } => {
            for arg in args {
                collect_aggregates(arg, aggregates);
            }
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(op) = operand {
                collect_aggregates(op, aggregates);
            }
            for (w, t) in when_then {
                collect_aggregates(w, aggregates);
                collect_aggregates(t, aggregates);
            }
            if let Some(e) = else_expr {
                collect_aggregates(e, aggregates);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::operators::MemoryTable;
    use crate::planner::{Binder, InMemoryCatalog, SchemaField};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::DataType;
    use arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;

    fn create_test_table() -> Arc<MemoryTable> {
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

        Arc::new(MemoryTable::new(schema, vec![batch]))
    }

    fn create_catalog_and_planner() -> (InMemoryCatalog, PhysicalPlanner) {
        let mut catalog = InMemoryCatalog::new();
        catalog.register_table(
            "test",
            PlanSchema::new(vec![
                SchemaField::new("id", DataType::Int64),
                SchemaField::new("name", DataType::Utf8),
                SchemaField::new("value", DataType::Int64),
            ]),
        );

        let mut planner = PhysicalPlanner::new();
        planner.register_table("test", create_test_table());

        (catalog, planner)
    }

    #[tokio::test]
    async fn test_simple_select() {
        let (catalog, planner) = create_catalog_and_planner();
        let mut binder = Binder::new(&catalog);

        let logical = binder.bind_sql("SELECT id, value FROM test").unwrap();
        let physical = planner.create_physical_plan(&logical).unwrap();

        let stream = physical.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert!(!results.is_empty());
        assert_eq!(results[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn test_filter() {
        let (catalog, planner) = create_catalog_and_planner();
        let mut binder = Binder::new(&catalog);

        let logical = binder
            .bind_sql("SELECT id FROM test WHERE value > 25")
            .unwrap();
        let physical = planner.create_physical_plan(&logical).unwrap();

        let stream = physical.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // values 30, 40, 50
    }

    #[tokio::test]
    async fn test_aggregate() {
        let (catalog, planner) = create_catalog_and_planner();
        let mut binder = Binder::new(&catalog);

        let logical = binder
            .bind_sql("SELECT SUM(value), COUNT(*) FROM test")
            .unwrap();
        let physical = planner.create_physical_plan(&logical).unwrap();

        let stream = physical.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 1);

        let sum = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(sum, 150);

        let count = results[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 5);
    }

    #[tokio::test]
    async fn test_sort() {
        let (catalog, planner) = create_catalog_and_planner();
        let mut binder = Binder::new(&catalog);

        // Note: ORDER BY columns must be in SELECT (planner limitation)
        let logical = binder
            .bind_sql("SELECT id, value FROM test ORDER BY value DESC")
            .unwrap();
        let physical = planner.create_physical_plan(&logical).unwrap();

        let stream = physical.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let ids = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(ids.value(0), 5);
        assert_eq!(ids.value(1), 4);
        assert_eq!(ids.value(2), 3);
    }

    #[tokio::test]
    async fn test_limit() {
        let (catalog, planner) = create_catalog_and_planner();
        let mut binder = Binder::new(&catalog);

        let logical = binder.bind_sql("SELECT id FROM test LIMIT 3").unwrap();
        let physical = planner.create_physical_plan(&logical).unwrap();

        let stream = physical.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }
}
