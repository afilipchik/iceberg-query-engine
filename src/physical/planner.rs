//! Physical planner - converts logical plans to physical plans

use crate::error::{QueryError, Result};
use crate::execution::{ExecutionConfig, SharedMemoryPool};
use crate::physical::operators::{
    evaluate_expr, run_subquery_plan, AggregateExpr, ExternalSortExec, FilterExec,
    HashAggregateExec, HashJoinExec, LimitExec, MemoryTableExec, MorselAggregateExec, ProjectExec,
    SortExec, SpillableHashAggregateExec, SpillableHashJoinExec, SubqueryExecutor, TableProvider,
    UnionExec, VectorSearchExec,
};
use crate::physical::PhysicalOperator;
use crate::planner::{BinaryOp, Expr, JoinType, LogicalPlan, PlanSchema};
use arrow::datatypes::{Field, Schema, SchemaRef};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

/// Shared CTE materialization cache. Allows SubqueryExecutor planners to access
/// CTEs materialized by the main planner.
pub type SharedCteCache =
    Arc<parking_lot::Mutex<HashMap<usize, (SchemaRef, Vec<arrow::record_batch::RecordBatch>)>>>;

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
    /// Cache for materialized CTEs. When a CTE is referenced multiple times (same Arc pointer),
    /// we materialize it once to ensure identical results (avoids floating-point non-determinism).
    /// Key is the raw pointer of the Arc<LogicalPlan>, value is materialized batches.
    /// Shared via Arc so SubqueryExecutor planners can access the same cache.
    cte_cache: SharedCteCache,
    /// HAVING-style predicate handed from a Filter node to its immediate
    /// Aggregate child so the aggregate can filter per shard BEFORE
    /// materializing output arrays (Q18 builds 650K rows instead of 15M).
    pending_agg_filter: RefCell<Option<Expr>>,
    /// Streaming scans created in this plan, keyed by operator address:
    /// (runtime-filter config handle, provider schema). Joins link runtime
    /// key filters to their probe-side scans through this registry.
    streaming_scans: RefCell<
        HashMap<
            usize,
            (
                crate::physical::operators::streaming_parquet_scan::RuntimeFilterConfig,
                SchemaRef,
            ),
        >,
    >,
    /// Per-Inner-join ancestor reference sets keyed by `&JoinNode` address,
    /// computed by `analyze_join_output_usage` before planning. At join
    /// construction the refs become a retention mask over the PHYSICAL
    /// (left ++ right) output columns: unmatched columns (typically
    /// ON-only keys) are dropped from the join's output and never gathered.
    join_retained: RefCell<HashMap<usize, Vec<crate::planner::Column>>>,
    /// Scan nodes (keyed by `&ScanNode` address) that are SPILL-COVERED,
    /// computed by `collect_spill_covered_scans` before planning
    /// (oom-safety-hardening task 004, generalized by spill-boundaries task
    /// 001). An over-budget native table's scan streams
    /// (`NativeStreamingScanExec`) ONLY when its Scan node is in this set.
    /// Covered = a spill-capable pipeline breaker sits between the scan
    /// and the root (a spillable aggregate / DISTINCT, a
    /// `SpillableHashJoinExec` on EITHER side, an `ExternalSortExec`) and
    /// no materializing consumer (a Window, a DelimJoin, a vector-search
    /// fallback, a non-spillable join/sort, or a shared-CTE
    /// materialization) sits between the scan and that breaker without an
    /// output-bounding aggregate in between. Every other shape (raw
    /// `SELECT *`, filter/project-only, LIMIT-only dumps) still takes the
    /// materializing scan, whose `check_scan_budget` refusal remains the
    /// correct, documented answer — streaming those would only move the
    /// OOM from the scan to the `QueryResult` collection at the root.
    spill_covered_scans: RefCell<std::collections::HashSet<usize>>,
}

/// State carried top-down by `PhysicalPlanner::collect_spill_covered_scans`
/// (spill-boundaries task 001): `covered` = a spill-capable pipeline
/// breaker has been passed on the way down; `blocked` = a materializing
/// consumer has been passed with no output-bounding aggregate below it yet.
#[derive(Clone, Copy, Debug)]
struct CoverWalk {
    covered: bool,
    blocked: bool,
}

impl CoverWalk {
    fn root() -> Self {
        Self {
            covered: false,
            blocked: false,
        }
    }
    fn block(self) -> Self {
        Self {
            covered: self.covered,
            blocked: true,
        }
    }
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
            cte_cache: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            pending_agg_filter: RefCell::new(None),
            streaming_scans: RefCell::new(HashMap::new()),
            join_retained: RefCell::new(HashMap::new()),
            spill_covered_scans: RefCell::new(std::collections::HashSet::new()),
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
            cte_cache: Arc::new(parking_lot::Mutex::new(HashMap::new())),
            pending_agg_filter: RefCell::new(None),
            streaming_scans: RefCell::new(HashMap::new()),
            join_retained: RefCell::new(HashMap::new()),
            spill_covered_scans: RefCell::new(std::collections::HashSet::new()),
        }
    }

    /// Check if spillable operators should be used (always true when memory pool is configured)
    fn use_spillable(&self) -> bool {
        self.memory_pool.is_some() && self.config.is_some()
    }

    /// Check if morsel execution should be used
    ///
    /// `QE_MORSEL=0` forces the generic aggregate path on, a diagnostic switch
    /// in the mould of `RT_DISABLE`. It exists to answer "what is
    /// `MorselAggregateExec` actually worth on this query", which is the only
    /// honest way to price porting it to a non-Parquet source: the morsel path
    /// is reachable only through `parquet_files()`, so on any other provider
    /// the measured delta IS the ceiling.
    fn use_morsel_execution(&self) -> bool {
        self.config.is_some()
            && self.config.as_ref().unwrap().enable_morsel_execution
            && !matches!(std::env::var("QE_MORSEL").as_deref(), Ok("0"))
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
                // A predicate containing a subquery must NOT be fused into the
                // scan: the scan evaluates its predicate itself, without a
                // subquery executor, and fails at execution ("no executor
                // available"). Left unfused, the generic path plans a
                // FilterExec, which precomputes uncorrelated scalars and
                // carries the executor for whatever remains.
                if node.predicate.contains_subquery() {
                    return None;
                }
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

    /// Mirror of `try_extract_parquet_source`, scoped to the dense-direct-
    /// address aggregate fast path's native-table support (task 005 of the
    /// native-tables-foundation epic): a native table has no parquet
    /// footer/row-group structure for `try_execute_dense_direct`'s existing
    /// machinery to read, but it DOES implement the same generic
    /// `TableProvider::{statistics, scan_with_filter}` surface that
    /// machinery can drive instead once threaded through
    /// (`MorselAggregateExec::with_native_provider`).
    ///
    /// Deliberately narrower than the parquet extractor:
    /// - No `Filter` arm, and a Scan-level `filter` is refused: unlike the
    ///   Parquet branch (which only trusts a filter the parquet decoder's
    ///   own RowFilter has fully applied), `NativeTable::scan_with_filter`
    ///   has NO pushdown at all (see its doc comment) and
    ///   `try_execute_dense_direct` never re-evaluates a filter itself —
    ///   accepting a filtered native scan here would silently aggregate
    ///   unfiltered rows.
    /// - Only matches a provider that downcasts to `NativeTable` — NOT
    ///   every non-Parquet provider. Widening the parquet-only
    ///   dense-direct fast path to arbitrary providers (Lance, MemoryTable,
    ///   ...) was investigated for Lance specifically and rejected (see
    ///   CLAUDE.md's "Tried, measured, REJECTED" table); this keeps that
    ///   boundary intentional rather than incidental.
    fn try_extract_native_dense_source(
        &self,
        plan: &LogicalPlan,
    ) -> Option<(
        Arc<dyn TableProvider>,
        arrow::datatypes::SchemaRef,
        Option<Vec<usize>>,
    )> {
        match plan {
            LogicalPlan::Scan(node) => {
                if node.filter.is_some() {
                    return None;
                }
                let provider = self.tables.get(&node.table_name)?;
                let native = provider
                    .as_any()
                    .downcast_ref::<crate::storage::NativeTable>()?;
                // An over-budget table must NOT take the dense-direct
                // route: its execution-time `scan_with_filter` call would
                // hit `check_scan_budget`'s refusal. Declining here lets it
                // fall through to the generic spillable aggregate over the
                // STREAMING scan path instead (oom-safety-hardening task
                // 004). In-budget tables are unaffected.
                if native.scan_budget_exceeded() {
                    return None;
                }
                let input_schema = provider.schema();
                let projection = node.projection.clone();
                Some((provider.clone(), input_schema, projection))
            }
            LogicalPlan::Project(node) => self.try_extract_native_dense_source(&node.input),
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

    /// Should the fused streaming aggregate hash-partition its input to
    /// per-worker channels (disjoint states)?
    ///
    /// True only for a single INTEGER group key whose footer statistics show
    /// a DENSE key space: NDV >= 1M and range <= 2x NDV. That is the regime
    /// where every worker's shared-channel partial state spans the whole key
    /// range and the merge pays workers-fold overlap (Q13's c_custkey,
    /// NDV=range=15M: merge 4.3s shared vs 0.1ms disjoint at SF=100 — an old
    /// generic-merge measurement, kept for the shape it documents; the
    /// CURRENT dense range-shard merge costs ~223ms at SF=10's 1.5M-range
    /// c_custkey, `[raw-merge]` profiling, see task 002).
    /// Sparse keys (l_orderkey, range 4x NDV) measured a net LOSS under the
    /// scatter, and low-NDV keys pay scatter for a merge that was already
    /// trivial — both stay on the shared channel.
    ///
    /// Lower bound 1,000,000 (task 002, duckdb-parity-2 epic, 2026-08-22):
    /// SF=10's c_custkey range is 1,500,000, under the floor this replaced
    /// (2,000,000) — that floor was set from the SF=100 case alone with no
    /// SF=10-scale measurement. `examples/disjoint_merge_bench.rs` isolates
    /// the merge step at range/mult combinations bracketing 1.5M (mult =
    /// TPC-H's fixed 10:1 orders:customer ratio) and found disjoint mode a
    /// net win EVERYWHERE tested, 500K through 5M range and mult 3 through
    /// 40, 1.55x-2.85x faster end to end (scatter cost included) with no
    /// sign of a crossover approaching from below — at SF=10's exact shape
    /// (range=1.5M, mult=10): ~1.9-2.0x. The bench's structural prediction
    /// (8.59x worker-state duplication at range=15M, mult=10) matches this
    /// function's own SF=100 doc citation (126M partial slots for 15M
    /// groups) to within 2%, and its absolute SF=10 merge-step timing
    /// (~150-200ms) matches the real measured 223ms `[raw-merge]` number
    /// closely enough to trust its verdict at the untested 1.5M point.
    /// 1,000,000 (not the lower bracket points also measured, e.g. 500K) was
    /// chosen to stay inside the directly-tested range rather than
    /// extrapolate below it.
    fn disjoint_group_hint(&self, group_by: &[Expr]) -> bool {
        if group_by.len() != 1 {
            return false;
        }
        let Expr::Column(c) = &group_by[0] else {
            return false;
        };
        let name = c.name.to_lowercase();
        for provider in self.tables.values() {
            if let Some(stats) = provider.statistics() {
                if let Some(cs) = stats.column_stats.get(&name) {
                    if let (Some(min), Some(max)) = (cs.min_i64, cs.max_i64) {
                        // Mirror the merge's dense-path trigger from HARD
                        // stats only. ndv_est is min(rows, range), which made
                        // a range-vs-ndv test circular: sparse l_orderkey
                        // (range 600M) passed it and Q18 paid a 32-way
                        // scatter for a merge that was already cheap. The
                        // pathological shared-merge case is a dense DIRECT-
                        // ADDRESS key domain, and that is a RANGE property:
                        // c_custkey (range 15M at SF=100, 1.5M at SF=10)
                        // qualifies, l_orderkey does not.
                        let range = max.saturating_sub(min).saturating_add(1).max(1) as u64;
                        return (1_000_000..=64_000_000).contains(&range);
                    }
                }
            }
        }
        false
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
        self.subquery_executor = Some(match (&self.memory_pool, &self.config) {
            (Some(pool), Some(config)) => {
                SubqueryExecutor::from_tables_with_config(tables, pool.clone(), config.clone())
            }
            _ => SubqueryExecutor::from_tables(tables),
        });
    }

    /// Set the subquery executor (used by subquery executor to pass itself for nested subqueries)
    pub fn set_subquery_executor(&mut self, executor: Option<SubqueryExecutor>) {
        self.subquery_executor = executor;
    }

    /// Share the CTE cache with this planner (used by SubqueryExecutor planners).
    pub fn set_cte_cache(&mut self, cache: SharedCteCache) {
        self.cte_cache = cache;
    }

    /// Get a clone of the CTE cache Arc for sharing with subquery planners.
    pub fn cte_cache_ref(&self) -> SharedCteCache {
        Arc::clone(&self.cte_cache)
    }

    /// Collect all scan projections for each table name in the plan.
    /// Used to compute union projections for scan caching.
    fn collect_scan_projections(
        &self,
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
            // Subtrees under a materialized CTE alias never execute (they are
            // replaced by the cached batches) — prescanning their tables reads
            // whole parquet files for nothing (Q15 pre-read all of lineitem).
            LogicalPlan::SubqueryAlias(node)
                if node
                    .cte_name
                    .as_ref()
                    .map(|n| self.cte_cache.lock().contains_key(&Self::cte_name_key(n)))
                    .unwrap_or(false) => {}
            _ => {
                for child in plan.children() {
                    self.collect_scan_projections(child, table_scans);
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
        self.collect_scan_projections(logical, &mut table_scans);

        // Build scan tasks: only prescan tables that are accessed 2+ times.
        // Single-use tables will be read on-demand, reducing peak memory.
        // Large tables are ALSO skipped: their scans stay lazy/streaming
        // where runtime join filters can prune decode at the parquet layer,
        // and morsel aggregation reads the files directly regardless — the
        // shared decode was frequently pure waste (Q17 decoded 60M lineitem
        // rows into cache while both consumers needed ~66K of them).
        const PRESCAN_MAX_BYTES: u64 = 400_000_000;
        let scan_tasks: Vec<_> = table_scans
            .iter()
            .filter(|(_, projections)| projections.len() > 1) // Only shared tables
            .filter_map(|(table_name, projections)| {
                let provider = self.tables.get(table_name)?;
                // Size the table from its files when it is Parquet, else from
                // provider statistics. Keying this off `parquet_files()` alone
                // left every non-Parquet provider (e.g. Lance) exempt from the
                // cap, so a multi-GB shared table would be decoded into the
                // cache unconditionally. Providers that report neither (e.g.
                // MemoryTable, already in memory) stay ungated as before.
                let total_bytes: Option<u64> = match provider.parquet_files() {
                    Some(files) => Some(
                        files
                            .iter()
                            .filter_map(|f| std::fs::metadata(f).ok())
                            .map(|m| m.len())
                            .sum(),
                    ),
                    None => provider.statistics().map(|s| s.total_byte_size),
                };
                if total_bytes.is_some_and(|total| total > PRESCAN_MAX_BYTES) {
                    return None;
                }
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

    /// Pre-materialize CTEs that are referenced multiple times. This ensures both
    /// references get identical data, avoiding floating-point non-determinism from
    /// parallel aggregation computing slightly different sums.
    fn materialize_shared_ctes(&self, logical: &LogicalPlan) -> Result<()> {
        // Count how many times each CTE name appears as a SubqueryAlias input
        let mut cte_counts: HashMap<String, usize> = HashMap::new();
        Self::count_cte_refs(logical, &mut cte_counts);
        if std::env::var("CTE_DEBUG").is_ok() {
            eprintln!("[cte] ref counts: {:?}", cte_counts);
        }

        // Materialize CTEs referenced 2+ times
        if cte_counts.values().all(|&c| c < 2) {
            return Ok(());
        }

        // Collect every SubqueryAlias node for each CTE that needs materialization.
        let mut cte_candidates: HashMap<String, Vec<&LogicalPlan>> = HashMap::new();
        Self::collect_cte_plans(logical, &cte_counts, &mut cte_candidates);

        // All copies of one CTE compute the same ROWS (PredicatePushdown treats
        // a named CTE as a materialization boundary and never pushes a
        // consumer's predicate inside), but they are not equally optimized: a
        // copy that lives inside a subquery EXPRESSION is never reached by
        // ProjectionPushdown, so its scan still reads every column of the
        // table. Materialize the widest output schema — so every consumer
        // finds its columns in the shared result — and break ties with
        // traversal order, which visits the main plan tree before expressions.
        let cte_plans: HashMap<String, &LogicalPlan> = cte_candidates
            .iter()
            .filter_map(|(name, cands)| {
                let max_fields = cands.iter().map(|p| p.schema().fields().len()).max()?;
                let best = cands
                    .iter()
                    .find(|p| p.schema().fields().len() == max_fields)?;
                Some((name.clone(), *best))
            })
            .collect();

        for (name, plan) in &cte_plans {
            if std::env::var("CTE_DEBUG").is_ok() {
                eprintln!(
                    "[cte] materializing {} ({} candidates) from node: {}",
                    name,
                    cte_candidates.get(name).map(|c| c.len()).unwrap_or(0),
                    &format!("{}", plan).lines().next().unwrap_or("?")
                );
            }
            let t0 = std::time::Instant::now();
            let physical = self.create_physical_plan_inner(plan)?;
            let schema = physical.schema();
            let batches: Vec<arrow::record_batch::RecordBatch> = run_subquery_plan(physical)?;
            // Use a hash of the CTE name as the key
            let key = Self::cte_name_key(name);
            if std::env::var("CTE_DEBUG").is_ok() {
                let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
                eprintln!(
                    "[cte] materialized {} (key {}) in {:?} -> {} rows",
                    name,
                    key,
                    t0.elapsed(),
                    rows
                );
            }
            self.cte_cache.lock().insert(key, (schema, batches));
        }
        Ok(())
    }

    /// Hash a CTE name to a usize key for the cache.
    fn cte_name_key(name: &str) -> usize {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        name.hash(&mut hasher);
        hasher.finish() as usize
    }

    /// Walk the logical plan tree and count CTE references by name.
    /// Also walks into subquery expressions (ScalarSubquery, Exists, InSubquery).
    fn count_cte_refs(plan: &LogicalPlan, counts: &mut HashMap<String, usize>) {
        match plan {
            LogicalPlan::SubqueryAlias(node) => {
                if let Some(ref cte_name) = node.cte_name {
                    *counts.entry(cte_name.clone()).or_insert(0) += 1;
                }
                Self::count_cte_refs(&node.input, counts);
            }
            LogicalPlan::Filter(node) => {
                Self::count_cte_refs_in_expr(&node.predicate, counts);
                Self::count_cte_refs(&node.input, counts);
            }
            LogicalPlan::Project(node) => {
                for expr in &node.exprs {
                    Self::count_cte_refs_in_expr(expr, counts);
                }
                Self::count_cte_refs(&node.input, counts);
            }
            LogicalPlan::Join(node) => {
                if let Some(ref filter) = node.filter {
                    Self::count_cte_refs_in_expr(filter, counts);
                }
                Self::count_cte_refs(&node.left, counts);
                Self::count_cte_refs(&node.right, counts);
            }
            LogicalPlan::Aggregate(node) => {
                for expr in &node.aggregates {
                    Self::count_cte_refs_in_expr(expr, counts);
                }
                Self::count_cte_refs(&node.input, counts);
            }
            _ => {
                for child in plan.children() {
                    Self::count_cte_refs(child, counts);
                }
            }
        }
    }

    /// Walk an expression tree to find subquery plans that contain CTE references.
    fn count_cte_refs_in_expr(expr: &Expr, counts: &mut HashMap<String, usize>) {
        match expr {
            Expr::ScalarSubquery(plan) => {
                Self::count_cte_refs(plan, counts);
            }
            Expr::Exists { subquery, .. } => {
                Self::count_cte_refs(subquery, counts);
            }
            Expr::InSubquery { subquery, expr, .. } => {
                Self::count_cte_refs(subquery, counts);
                Self::count_cte_refs_in_expr(expr, counts);
            }
            Expr::BinaryExpr { left, right, .. } => {
                Self::count_cte_refs_in_expr(left, counts);
                Self::count_cte_refs_in_expr(right, counts);
            }
            Expr::UnaryExpr { expr: inner, .. }
            | Expr::Cast { expr: inner, .. }
            | Expr::Alias { expr: inner, .. } => {
                Self::count_cte_refs_in_expr(inner, counts);
            }
            _ => {}
        }
    }

    /// Collect the first logical plan for each CTE that needs materialization.
    fn collect_cte_plans<'a>(
        plan: &'a LogicalPlan,
        needed: &HashMap<String, usize>,
        plans: &mut HashMap<String, Vec<&'a LogicalPlan>>,
    ) {
        match plan {
            LogicalPlan::SubqueryAlias(node) => {
                if let Some(ref cte_name) = node.cte_name {
                    if needed.get(cte_name).copied().unwrap_or(0) >= 2 {
                        plans.entry(cte_name.clone()).or_default().push(&node.input);
                    }
                }
                Self::collect_cte_plans(&node.input, needed, plans);
            }
            // Plan inputs are visited BEFORE subquery expressions: copies of a
            // CTE that sit in the main tree have been through the full
            // optimizer, copies nested in an expression have not.
            LogicalPlan::Filter(node) => {
                Self::collect_cte_plans(&node.input, needed, plans);
                Self::collect_cte_plans_in_expr(&node.predicate, needed, plans);
            }
            LogicalPlan::Project(node) => {
                Self::collect_cte_plans(&node.input, needed, plans);
                for expr in &node.exprs {
                    Self::collect_cte_plans_in_expr(expr, needed, plans);
                }
            }
            _ => {
                for child in plan.children() {
                    Self::collect_cte_plans(child, needed, plans);
                }
            }
        }
    }

    fn collect_cte_plans_in_expr<'a>(
        expr: &'a Expr,
        needed: &HashMap<String, usize>,
        plans: &mut HashMap<String, Vec<&'a LogicalPlan>>,
    ) {
        match expr {
            Expr::ScalarSubquery(plan) => Self::collect_cte_plans(plan, needed, plans),
            Expr::Exists { subquery, .. } => Self::collect_cte_plans(subquery, needed, plans),
            Expr::InSubquery { subquery, expr, .. } => {
                Self::collect_cte_plans(subquery, needed, plans);
                Self::collect_cte_plans_in_expr(expr, needed, plans);
            }
            Expr::BinaryExpr { left, right, .. } => {
                Self::collect_cte_plans_in_expr(left, needed, plans);
                Self::collect_cte_plans_in_expr(right, needed, plans);
            }
            Expr::UnaryExpr { expr: inner, .. }
            | Expr::Cast { expr: inner, .. }
            | Expr::Alias { expr: inner, .. } => {
                Self::collect_cte_plans_in_expr(inner, needed, plans);
            }
            _ => {}
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
    /// Top-down walk computing, per Inner/Left/Right/Full join, which of its
    /// output columns any ANCESTOR references. `needed == None` means
    /// "assume everything" (the safe default for shapes this walk doesn't
    /// model). Conservative by construction: a wrong mask can only KEEP too
    /// much (no pruning benefit), never drop a referenced column — every
    /// reference set is a superset of true usage, and unmodelled nodes reset
    /// to None. Semi/Anti (left-only-width schema) and Cross (no ON-clause
    /// to force-keep against) are out of scope and never get a mask.
    fn analyze_join_output_usage(
        &self,
        plan: &LogicalPlan,
        needed: Option<Vec<crate::planner::Column>>,
    ) {
        use crate::optimizer::rules::eager_aggregation::collect_columns;
        use crate::planner::Column;
        let refs_of = |exprs: &[&Expr]| -> Option<Vec<Column>> {
            // A subquery inside any expression can reference columns this
            // walk cannot see; give up on the whole subtree.
            if exprs.iter().any(|e| e.contains_subquery()) {
                return None;
            }
            let mut out = Vec::new();
            for e in exprs {
                collect_columns(e, &mut out);
            }
            Some(out)
        };
        let extend =
            |base: &Option<Vec<Column>>, more: Option<Vec<Column>>| -> Option<Vec<Column>> {
                match (base, more) {
                    (Some(b), Some(mut m)) => {
                        m.extend(b.iter().cloned());
                        Some(m)
                    }
                    _ => None,
                }
            };
        match plan {
            LogicalPlan::Project(n) => {
                let child = refs_of(&n.exprs.iter().collect::<Vec<_>>());
                self.analyze_join_output_usage(&n.input, child);
            }
            LogicalPlan::Aggregate(n) => {
                let exprs: Vec<&Expr> = n.group_by.iter().chain(n.aggregates.iter()).collect();
                let child = refs_of(&exprs);
                self.analyze_join_output_usage(&n.input, child);
            }
            LogicalPlan::Filter(n) => {
                let child = extend(&needed, refs_of(&[&n.predicate]));
                self.analyze_join_output_usage(&n.input, child);
            }
            LogicalPlan::Sort(n) => {
                let keys: Vec<&Expr> = n.order_by.iter().map(|s| &s.expr).collect();
                let child = extend(&needed, refs_of(&keys));
                self.analyze_join_output_usage(&n.input, child);
            }
            LogicalPlan::Limit(n) => {
                self.analyze_join_output_usage(&n.input, needed);
            }
            LogicalPlan::Join(n) => {
                // Pruning is safe for the join types whose output width and
                // NULL-extension semantics the physical probe paths fully
                // implement (Inner/Left/Right/Full — see HashJoinExec's and
                // SpillableHashJoinExec's set_retained). Semi/Anti keep a
                // left-only-width schema that needs separate handling and
                // Cross has no ON-clause; both stay excluded. A filter
                // containing a subquery can reference columns this walk
                // cannot see, so it bails to "keep everything" — the same
                // safe default `refs_of` uses below for ancestor exprs.
                let prune_eligible = matches!(
                    n.join_type,
                    crate::planner::JoinType::Inner
                        | crate::planner::JoinType::Left
                        | crate::planner::JoinType::Right
                        | crate::planner::JoinType::Full
                ) && n
                    .filter
                    .as_ref()
                    .map(|f| !f.contains_subquery())
                    .unwrap_or(true);
                if prune_eligible {
                    if let Some(need) = &needed {
                        // Force-keep: any column the ON-clause filter itself
                        // references stays in the join's output regardless
                        // of downstream need. The filter is evaluated INSIDE
                        // the join on candidate (build, probe) pairs — using
                        // the already-pruned build cache — before NULL
                        // extension is decided, so it needs the column even
                        // when nothing above the join ever selects it.
                        let mut retained = need.clone();
                        if let Some(f) = &n.filter {
                            collect_columns(f, &mut retained);
                        }
                        self.join_retained
                            .borrow_mut()
                            .insert(n as *const _ as usize, retained);
                    }
                }
                let mut on_refs: Vec<&Expr> = Vec::new();
                for (l, r) in &n.on {
                    on_refs.push(l);
                    on_refs.push(r);
                }
                if let Some(f) = &n.filter {
                    on_refs.push(f);
                }
                let child = extend(&needed, refs_of(&on_refs));
                self.analyze_join_output_usage(&n.left, child.clone());
                self.analyze_join_output_usage(&n.right, child);
            }
            LogicalPlan::Scan(_)
            | LogicalPlan::EmptyRelation(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::DelimGet(_) => {}
            // Distinct/Union/SubqueryAlias/DelimJoin/VectorSearch and anything
            // else: semantics not modelled — everything below is needed.
            other => {
                for child in other.children() {
                    self.analyze_join_output_usage(child, None);
                }
            }
        }
    }

    pub fn create_physical_plan(&self, logical: &LogicalPlan) -> Result<Arc<dyn PhysicalOperator>> {
        // Pre-materialize CTEs that are referenced multiple times to ensure
        // identical results (avoids floating-point non-determinism from parallel aggregation).
        // Must run BEFORE the shared-table prescan so tables that only appear
        // inside a now-materialized CTE aren't pointlessly pre-read.
        // Share the CTE cache with the SubqueryExecutor FIRST — subquery
        // planners created at any later point must see CTEs materialized
        // below (gating on non-empty raced: an executor set up before
        // materialization kept its own empty cache and re-ran the shared
        // pipeline).
        if let Some(ref executor) = self.subquery_executor {
            executor.set_cte_cache(self.cte_cache_ref());
        }
        // Streaming-scan eligibility pre-pass (oom-safety-hardening task
        // 004; spill-boundaries task 001): which Scan nodes are spill-
        // covered? See the `spill_covered_scans` field doc. Runs BEFORE
        // the shared-CTE materialization below: a CTE referenced twice is
        // lowered through `create_physical_plan_inner` right there, and
        // with the set still empty its scans were routed onto the
        // materializing path — Q11's CSE'd HAVING subquery and Q15's
        // `revenue` CTE refused this way at SF=100 native @1G even though
        // both sit under an aggregate.
        {
            let mut covered = self.spill_covered_scans.borrow_mut();
            covered.clear();
            let mut cte_counts: HashMap<String, usize> = HashMap::new();
            Self::count_cte_refs(logical, &mut cte_counts);
            self.collect_spill_covered_scans(logical, CoverWalk::root(), &cte_counts, &mut covered);
        }
        self.materialize_shared_ctes(logical)?;
        // Pre-scan tables that are accessed multiple times to avoid redundant parquet reads
        self.prescan_shared_tables(logical);
        // Join-output pruning analysis: which columns of each Inner join's
        // output do its ANCESTORS actually reference? ON-only columns (the
        // usual case: surrogate keys) are dead the instant the probe
        // completes, yet the join gathers, materializes and ships them
        // through every downstream batch. Q9 at SF=100 dragged
        // ps_partkey+ps_suppkey (2 of the partsupp build's 3 columns) and
        // o_orderkey through ~604M-row gathers this way — HJ_PROF measured
        // gather+batch at ~75% of the probe pipeline.
        self.analyze_join_output_usage(logical, None);
        self.create_physical_plan_inner(logical)
    }

    /// Walk `plan`, recording the address of every `ScanNode` that is
    /// spill-covered (spill-boundaries task 001, generalizing
    /// oom-safety-hardening task 004's "has an Aggregate ancestor"). Keyed
    /// by node ADDRESS, not table name: the same table may legitimately
    /// appear both under a breaker and in a raw-dump position within one
    /// query (e.g. a UNION branch), and only the covered occurrence is safe
    /// to stream. Addresses are stable for the duration of planning —
    /// `create_physical_plan_inner` (and `materialize_shared_ctes`, which
    /// lowers `&SubqueryAliasNode::input` of the SAME tree) recurse over
    /// the tree object this pre-pass walked.
    ///
    /// Top-down, carrying a `CoverWalk`:
    /// - Aggregate / Distinct: covered, and the output is BOUNDED, so any
    ///   materializing consumer above no longer matters (`blocked` resets).
    /// - Join / Sort: `SpillableHashJoinExec` (the build side spills, the
    ///   probe side streams — either child may be the over-budget scan) /
    ///   `ExternalSortExec` when spillable → covered; `blocked` unchanged.
    ///   Without a memory config they are plain in-memory operators →
    ///   blocked.
    /// - Window, DelimJoin, VectorSearch: materialize their input → blocked.
    /// - A shared CTE's root (`SubqueryAlias` with a `cte_name` referenced
    ///   twice or more): `materialize_shared_ctes` collects its OUTPUT into
    ///   memory, so it is a materializing consumer and a fresh root for
    ///   everything below it (its own aggregate, if any, re-covers).
    /// - Filter / Project / Limit / Union / plain SubqueryAlias: pass
    ///   through unchanged.
    /// A Scan is recorded when `covered && !blocked`.
    fn collect_spill_covered_scans(
        &self,
        plan: &LogicalPlan,
        walk: CoverWalk,
        cte_counts: &HashMap<String, usize>,
        out: &mut std::collections::HashSet<usize>,
    ) {
        let spillable = self.use_spillable();
        let next = match plan {
            LogicalPlan::Scan(node) => {
                if walk.covered && !walk.blocked {
                    out.insert(node as *const _ as usize);
                }
                return;
            }
            // An aggregate bounds its output whichever operator lowers it
            // (spillable, morsel, or the plain in-memory hash aggregate a
            // config-less planner builds — the pre-001 rule covered these
            // too), so it always re-covers.
            LogicalPlan::Aggregate(_) | LogicalPlan::Distinct(_) => CoverWalk {
                covered: true,
                blocked: false,
            },
            LogicalPlan::Join(_) | LogicalPlan::Sort(_) => {
                if spillable {
                    CoverWalk {
                        covered: true,
                        blocked: walk.blocked,
                    }
                } else {
                    walk.block()
                }
            }
            LogicalPlan::Window(_) | LogicalPlan::DelimJoin(_) | LogicalPlan::VectorSearch(_) => {
                walk.block()
            }
            LogicalPlan::SubqueryAlias(node)
                if node
                    .cte_name
                    .as_ref()
                    .is_some_and(|n| cte_counts.get(n).copied().unwrap_or(0) >= 2) =>
            {
                CoverWalk::root().block()
            }
            _ => walk,
        };
        for child in plan.children() {
            self.collect_spill_covered_scans(child, next, cte_counts, out);
        }
    }

    /// The normal (CPU) lowering of an Aggregate node: morsel path for
    /// parquet sources, spillable or plain hash aggregation otherwise.
    /// Split out so the GPU wrapper can delegate to it.
    fn lower_aggregate_cpu(
        &self,
        node: &crate::planner::AggregateNode,
    ) -> Result<Arc<dyn PhysicalOperator>> {
        // Take any pending HAVING predicate up front so aggregates
        // nested inside this one's input can never consume it.
        let post_filter = self.pending_agg_filter.borrow_mut().take();
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
                )
                .with_post_filter(post_filter);
                return Ok(Arc::new(morsel_agg));
            } else if let Some((provider, input_schema, projection)) =
                self.try_extract_native_dense_source(&node.input)
            {
                // Native-table dense-direct-address routing (task 005).
                // `MorselAggregateExec`'s native mode has no generic
                // (hash-table) fallback tier the way its Parquet mode does
                // (that tier is built entirely around
                // `ParallelParquetSource` over a file list, which a native
                // table doesn't have) — so this planner-time check must be
                // exactly as strict as `try_execute_dense_direct` itself,
                // not an approximation of it. Both halves are the SAME
                // functions `try_execute_dense_direct` uses at execution
                // time (`dense_direct_shape`/`dense_direct_key_bounds`), so
                // the two can never disagree.
                let eligible = crate::physical::operators::dense_direct_shape(
                    &node.group_by,
                    &aggregates,
                    &input_schema,
                )
                .is_some_and(|(key_name, _, _)| {
                    crate::physical::operators::dense_direct_key_bounds(
                        provider.as_ref(),
                        &key_name.to_lowercase(),
                    )
                    .is_some()
                });
                if eligible {
                    let morsel_agg = MorselAggregateExec::new(
                        Vec::new(),
                        input_schema,
                        projection,
                        None,
                        node.group_by.clone(),
                        aggregates,
                        schema,
                    )
                    .with_post_filter(post_filter)
                    .with_native_provider(provider);
                    return Ok(Arc::new(morsel_agg));
                }
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

            let disjoint = self.disjoint_group_hint(&node.group_by);
            let agg = SpillableHashAggregateExec::new(
                input,
                node.group_by.clone(),
                spillable_aggs,
                schema,
                self.memory_pool.clone().unwrap(),
                self.config.clone().unwrap(),
            )
            .with_post_filter(post_filter)
            .with_disjoint_groups(disjoint);
            Ok(Arc::new(agg))
        } else {
            let agg = HashAggregateExec::new(input, node.group_by.clone(), aggregates, schema);
            match post_filter {
                Some(pred) => {
                    let filter = self.create_filter(Arc::new(agg), pred);
                    Ok(Arc::new(filter))
                }
                None => Ok(Arc::new(agg)),
            }
        }
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

                // Unfiltered single-use parquet scans stream lazily, partitioned
                // by row group: decode overlaps with the consumer (join probes,
                // sorts) instead of materializing the whole table at plan time.
                // Filtered scans keep the eager path (decoder-level RowFilter);
                // prescanned shared tables use the cache below.
                // Filtered scans also stream when the predicate is fully
                // decodable at the parquet layer (subquery-free, all columns
                // resolve in the provider schema) — the decoder RowFilter
                // applies it completely, so no FilterExec is needed and the
                // scan can additionally take runtime join-key filters.
                let filter_streams = node.filter.as_ref().is_none_or(|f| {
                    if f.contains_subquery() {
                        return false;
                    }
                    // Small tables keep the eager path: it already pushes a
                    // decoder RowFilter and reads row groups with better
                    // parallelism than a lazy stream (Q16's part scan lost
                    // 180ms as a filtered stream).
                    let big = provider
                        .parquet_files()
                        .map(|files| {
                            files
                                .iter()
                                .filter_map(|f| std::fs::metadata(f).ok())
                                .map(|m| m.len())
                                .sum::<u64>()
                                > 400_000_000
                        })
                        .unwrap_or(false);
                    if !big {
                        return false;
                    }
                    let mut cols: Vec<String> = Vec::new();
                    crate::physical::morsel::collect_expr_columns(f, &mut cols);
                    !cols.is_empty()
                        && cols.iter().all(|c| {
                            provider
                                .schema()
                                .fields()
                                .iter()
                                .any(|pf| pf.name().eq_ignore_ascii_case(c))
                        })
                });
                if filter_streams && !self.scan_cache.borrow().contains_key(&node.table_name) {
                    if let Some(files) = provider.parquet_files() {
                        let exec = crate::physical::operators::StreamingParquetScanExec::try_new(
                            &node.table_name,
                            &files,
                            logical_schema.clone(),
                            node.projection.clone(),
                            node.filter.as_ref(),
                            &provider.schema(),
                        )?;
                        let cfg = exec.runtime_filter_config();
                        let arc: Arc<dyn PhysicalOperator> = Arc::new(exec);
                        self.streaming_scans.borrow_mut().insert(
                            Arc::as_ptr(&arc) as *const () as usize,
                            (cfg, provider.schema()),
                        );
                        return Ok(arc);
                    }
                }

                // Over-budget native tables stream into spill-capable
                // consumers instead of refusing (oom-safety-hardening task
                // 004; epic Architecture Decision 4; spill-boundaries task
                // 001 widened "feeds an Aggregate" to "spill-covered": a
                // spillable join on either side or an external sort covers
                // too). Fires ONLY when the materializing scan WOULD refuse
                // (`scan_budget_exceeded`) AND the Scan is in
                // `spill_covered_scans` — in-budget tables take the exact
                // pre-existing path below (dense-direct-address and
                // GPU-offload eligibility unchanged), and materializing
                // shapes (raw `SELECT *`, LIMIT-only) still reach
                // `check_scan_budget`'s named refusal.
                if !self.scan_cache.borrow().contains_key(&node.table_name)
                    && self
                        .spill_covered_scans
                        .borrow()
                        .contains(&(node as *const _ as usize))
                {
                    if let Some(native) = provider
                        .as_any()
                        .downcast_ref::<crate::storage::NativeTable>()
                    {
                        if native.scan_budget_exceeded() {
                            let exec = crate::physical::operators::NativeStreamingScanExec::new(
                                &node.table_name,
                                native,
                                logical_schema.clone(),
                                node.projection.clone(),
                                node.filter.as_ref(),
                            );
                            let arc: Arc<dyn PhysicalOperator> = Arc::new(exec);
                            // The operator prunes segments but never
                            // evaluates predicates — always re-apply the
                            // full filter above it.
                            return match &node.filter {
                                Some(predicate) => {
                                    let filter = self.create_filter(arc, predicate.clone());
                                    Ok(Arc::new(filter))
                                }
                                None => Ok(arc),
                            };
                        }
                    }
                }

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
                                // Field TYPES follow the actual columns: v2
                                // IPC sidecars serve low-cardinality strings
                                // dictionary-encoded, and the logical schema
                                // still says Utf8.
                                let fields: Vec<_> = requested_indices
                                    .iter()
                                    .zip(columns.iter())
                                    .map(|(&i, c)| {
                                        let f = logical_schema.field(i);
                                        if f.data_type() == c.data_type() {
                                            f.clone()
                                        } else {
                                            arrow::datatypes::Field::new(
                                                f.name(),
                                                c.data_type().clone(),
                                                true,
                                            )
                                        }
                                    })
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
                // HAVING pushdown: a subquery-free predicate directly above an
                // Aggregate is applied to the aggregate's output batches with
                // vectorized kernels instead of a row-wise FilterExec.
                if matches!(&*node.input, LogicalPlan::Aggregate(_))
                    && !node.predicate.contains_subquery()
                {
                    let prev = self
                        .pending_agg_filter
                        .borrow_mut()
                        .replace(node.predicate.clone());
                    let agg = self.create_physical_plan_inner(&node.input)?;
                    let leftover = {
                        let mut slot = self.pending_agg_filter.borrow_mut();
                        std::mem::replace(&mut *slot, prev)
                    };
                    if leftover.is_none() {
                        // consumed by the aggregate operator
                        return Ok(agg);
                    }
                    let filter = self.create_filter(agg, node.predicate.clone());
                    return Ok(Arc::new(filter));
                }
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
                // Trust the optimizer's orientation for Inner joins: both the
                // DPsize enumerator and the greedy fallback deliberately place
                // the intended build side LEFT using footer statistics. The
                // planner's own estimate_output_rows() guesses join outputs as
                // max(l,r)/10, and re-swapping based on that undid the
                // optimizer's choice (Q09: re-swapped an 8M-row partsupp build
                // into a 133M-row intermediate build -> 65GB peak).
                let should_swap = false;

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

                // An ON-clause predicate is part of the JOIN CONDITION, not a
                // post-join filter. The two are equivalent for Inner joins, but
                // for Left/Right/Full a post-filter also sees the NULL-extended
                // rows the outer join exists to produce; those rows fail every
                // comparison (NULL is not TRUE) and get dropped, silently
                // degrading the outer join to an inner join. Such predicates
                // must be applied to candidate (build, probe) pairs INSIDE the
                // join, before match tracking. Semi/Anti need this too, for the
                // separate reason that their output drops the other side's
                // columns entirely.
                let filter_inside_join = is_semi_anti
                    || matches!(
                        node.join_type,
                        JoinType::Left | JoinType::Right | JoinType::Full
                    );

                // Runtime join-key filter: joins with a single plain
                // probe-key column over a streaming parquet scan decode only
                // rows whose key exists in the (small) build side. Safe for
                // Inner; also for Semi, Anti and Left when the build is the
                // LEFT side (probe rows outside the build key set can never
                // match a build row, and for Left the PRESERVED side is the
                // build side, so a dropped probe row was never going to
                // reach the output either way). NOT safe when the build
                // flips to the right: for swapped Semi/Anti the probe rows
                // ARE the output (Anti would drop exactly the rows it must
                // keep), and for Left the probe side would then be the
                // preserved one (unmatched rows must still NULL-extend).
                // Right/Full are excluded too: Right always builds from its
                // own (preserved) right side, so the wiring below — which
                // targets the physical RIGHT child as "the probe scan" —
                // would target the wrong side; Full preserves both sides, so
                // no side may be dropped from the scan at all.
                let build_prefers_left = matches!(
                    node.join_type,
                    JoinType::Left | JoinType::Semi | JoinType::Anti
                ) && !build_right_for_left;
                let rt_eligible = matches!(node.join_type, JoinType::Inner) || build_prefers_left;
                // Multi-key joins publish a partial filter on the first
                // column pair (a correct superset of matching rows).
                let rt_pair = on
                    .iter()
                    .position(|(_, r)| matches!(r, Expr::Column(_)))
                    .unwrap_or(0);
                // `linked_scan_cfg`, when set, is the (multi-slot filter
                // config, provider schema) this join successfully linked to
                // -- either a DIRECT probe-side streaming scan, or,
                // transitively, an already-linked ANCESTOR join's own probe
                // scan (see the re-registration below the branch that builds
                // `result`). Captured here so it can be re-published under
                // THIS join's own resulting operator pointer once that
                // pointer exists, letting a LATER join up the tree chain
                // through this one exactly as if it were a plain scan.
                let mut linked_scan_cfg: Option<(
                    crate::physical::operators::streaming_parquet_scan::RuntimeFilterConfig,
                    SchemaRef,
                )> = None;
                let probe_rt_filter = if rt_eligible && !on.is_empty() {
                    if let Some(Expr::Column(c)) = on.get(rt_pair).map(|(_, r)| r) {
                        // The probe-side streaming scan may sit under column
                        // pass-through Projects (decorrelated subquery
                        // shapes); the filter column is resolved by NAME in
                        // the provider schema, so digging through is safe.
                        // It may ALSO, after unwrapping Projects, land
                        // directly on an already-linked ANCESTOR join's own
                        // output (registered below): a leaf touched by two or
                        // more independently eligible joins is reached this
                        // way without walking back down into that ancestor's
                        // children, which would risk resolving its BUILD
                        // side instead of its probe side for a join this
                        // code didn't itself gate as build-stays-left.
                        let mut probe_leaf = Arc::clone(&right);
                        while probe_leaf.name() == "Project" {
                            let ch = probe_leaf.children();
                            if ch.len() != 1 {
                                break;
                            }
                            probe_leaf = ch.into_iter().next().unwrap();
                        }
                        let key = Arc::as_ptr(&probe_leaf) as *const () as usize;
                        let scans = self.streaming_scans.borrow();
                        if std::env::var("RT_DEBUG").is_ok() && !scans.contains_key(&key) {
                            eprintln!(
                                "[rt] no link: jt={:?} probe_leaf={} col={}",
                                node.join_type,
                                probe_leaf.name(),
                                c.name
                            );
                        }
                        let linked = scans.get(&key).and_then(|(cfg, pschema)| {
                            pschema
                                .fields()
                                .iter()
                                .position(|f| f.name().eq_ignore_ascii_case(&c.name))
                                .filter(|_| std::env::var("RT_DISABLE").is_err())
                                .map(|idx| {
                                    let slot: crate::physical::operators::SharedRuntimeFilter =
                                        Default::default();
                                    // Push, never overwrite: a leaf already
                                    // linked to an earlier join's filter gets
                                    // ANOTHER, independent, AND-combined slot
                                    // rather than losing the first one.
                                    cfg.lock().push((idx, Arc::clone(&slot)));
                                    if std::env::var("RT_DEBUG").is_ok() {
                                        eprintln!(
                                            "[rt] linked col {} ({}) slots_now={}",
                                            idx,
                                            c.name,
                                            cfg.lock().len()
                                        );
                                    }
                                    (slot, Arc::clone(cfg), Arc::clone(pschema))
                                })
                        });
                        drop(scans);
                        match linked {
                            Some((slot, cfg, pschema)) => {
                                linked_scan_cfg = Some((cfg, pschema));
                                Some(slot)
                            }
                            None => None,
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };

                let result: Arc<dyn PhysicalOperator> = if self.use_spillable() {
                    // Use spillable hash join with memory management
                    let mut join = SpillableHashJoinExec::new(
                        left,
                        right,
                        on,
                        node.join_type,
                        self.memory_pool.clone().unwrap(),
                        self.config.clone().unwrap(),
                    )
                    .with_build_right(build_right_for_left);
                    join.probe_runtime_filter = probe_rt_filter.clone();
                    join.probe_runtime_filter_pair = rt_pair;
                    // Join-output pruning: match the ancestors' reference set
                    // against the PHYSICAL (left ++ right) columns; unmatched
                    // ones are ON-only and never gathered. Physical field
                    // names may carry a dotted qualifier ("n1.n_name").
                    let retained_mask: Option<Vec<bool>> = self
                        .join_retained
                        .borrow()
                        .get(&(node as *const _ as usize))
                        .filter(|_| !matches!(std::env::var("QE_JOIN_PRUNE").as_deref(), Ok("0")))
                        .map(|need| {
                            let schemas: Vec<SchemaRef> =
                                join.children().iter().map(|c| c.schema()).collect();
                            schemas
                                .iter()
                                .flat_map(|sc| sc.fields().iter())
                                .map(|f| {
                                    let (frel, fname) = match f.name().split_once('.') {
                                        Some((r, n)) => (Some(r), n),
                                        None => (None, f.name().as_str()),
                                    };
                                    need.iter().any(|c| {
                                        c.name.eq_ignore_ascii_case(fname)
                                            && match (&c.relation, frel) {
                                                (Some(a), Some(b)) => a.eq_ignore_ascii_case(b),
                                                _ => true,
                                            }
                                    })
                                })
                                .collect()
                        })
                        .filter(|m: &Vec<bool>| m.iter().any(|k| !k) && m.iter().any(|k| *k));
                    if std::env::var("QE_PRUNE_DEBUG").is_ok() {
                        if let Some(m) = &retained_mask {
                            eprintln!(
                                "[prune] join wired: {} of {} cols kept (on: {:?})",
                                m.iter().filter(|k| **k).count(),
                                m.len(),
                                node.on
                                    .iter()
                                    .map(|(l, r)| format!("{l}={r}"))
                                    .collect::<Vec<_>>()
                            );
                        }
                    }
                    join.set_retained(retained_mask);

                    // Semi/Anti and the outer join types evaluate the ON
                    // predicate inside the join; only Inner/Cross may post-filter
                    // (for them ON and WHERE are equivalent).
                    if filter_inside_join && node.filter.is_some() {
                        join = join.with_filter(node.filter.clone());
                        Arc::new(join)
                    } else {
                        match &node.filter {
                            Some(predicate) => {
                                let filter = self.create_filter(Arc::new(join), predicate.clone());
                                Arc::new(filter)
                            }
                            None => Arc::new(join),
                        }
                    }
                } else if filter_inside_join && node.filter.is_some() {
                    // Use regular hash join (no memory management), evaluating
                    // the ON predicate as part of the join condition.
                    let join = HashJoinExec::with_filter(
                        left,
                        right,
                        on,
                        node.join_type,
                        node.filter.clone(),
                    )
                    .with_build_right(build_right_for_left);
                    Arc::new(join)
                } else {
                    // Use regular hash join (no memory management)
                    let join = HashJoinExec::new(left, right, on, node.join_type)
                        .with_build_right(build_right_for_left);

                    // Inner/Cross only: ON is equivalent to WHERE here.
                    match &node.filter {
                        Some(predicate) => {
                            let filter = self.create_filter(Arc::new(join), predicate.clone());
                            Arc::new(filter)
                        }
                        None => Arc::new(join),
                    }
                };

                // Chain runtime-filter linking: a join that itself linked to
                // a probe-side scan (directly, or transitively through an
                // earlier-linked join below it) re-publishes that SAME
                // (config, schema) pair under its OWN resulting operator's
                // pointer. A LATER, independently eligible join whose probe
                // side is exactly THIS join's output then finds it through
                // the ordinary Project-unwrap-only lookup above, with no
                // extra downcasting or children()-walking needed -- and,
                // being a fresh `push` onto the SAME multi-slot config
                // rather than a new one, both joins' filters apply
                // AND-combined at the original scan.
                if let Some(scan_link) = linked_scan_cfg {
                    self.streaming_scans
                        .borrow_mut()
                        .insert(Arc::as_ptr(&result) as *const () as usize, scan_link);
                }

                Ok(result)
            }

            LogicalPlan::Aggregate(node) => {
                // GPU offload wrapper: when the shape is describable, the
                // normal operator is built anyway and the wrapper delegates
                // to it until the columns are device-resident (never slower).
                #[cfg(feature = "gpu")]
                {
                    let offload_ok = self.config.as_ref().map_or(true, |c| c.gpu_offload);
                    if offload_ok && self.pending_agg_filter.borrow().is_none() {
                        if let Some(gplan) = crate::physical::gpu::plan_gpu_agg(node, &self.tables)
                        {
                            let inner = self.lower_aggregate_cpu(node)?;
                            return Ok(Arc::new(crate::physical::gpu::GpuAggExec::new(
                                gplan, inner,
                            )));
                        }
                    }
                }
                self.lower_aggregate_cpu(node)
            }

            LogicalPlan::Window(node) => {
                let input = self.create_physical_plan_inner(&node.input)?;
                let schema = plan_schema_to_arrow(&node.schema);
                let exec = crate::physical::operators::WindowExec::try_new(
                    input,
                    node.window_exprs.clone(),
                    schema,
                )?;
                Ok(Arc::new(exec))
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

            LogicalPlan::VectorSearch(node) => {
                // The fallback IS the plan the optimizer replaced: build it
                // exactly as if the rule had never fired, so the exact path is
                // the same code that would have run otherwise.
                let sorted = LogicalPlan::Sort(crate::planner::SortNode {
                    input: node.input.clone(),
                    order_by: vec![node.sort_key.clone()],
                });
                let limited = LogicalPlan::Limit(crate::planner::LimitNode {
                    input: Arc::new(sorted),
                    skip: node.skip,
                    fetch: Some(node.k),
                });
                let fallback = self.create_physical_plan_inner(&limited)?;

                let provider = self.tables.get(&node.table_name).cloned();
                // Map output columns to indices in the PROVIDER's schema.
                // If any column cannot be resolved there, hand the operator no
                // provider at all — it will use the exact path.
                let projection: Option<Vec<usize>> = provider.as_ref().and_then(|p| {
                    let s = p.schema();
                    node.outputs
                        .iter()
                        .map(|(src, _)| {
                            s.fields()
                                .iter()
                                .position(|f| f.name().eq_ignore_ascii_case(src))
                        })
                        .collect()
                });
                let (provider, projection) = match projection {
                    Some(p) => (provider, p),
                    None => (None, Vec::new()),
                };

                Ok(Arc::new(VectorSearchExec::new(
                    fallback,
                    provider,
                    projection,
                    node.outputs.iter().map(|(_, f)| f.clone()).collect(),
                    node.column.clone(),
                    node.query.clone(),
                    node.k,
                    node.skip,
                    node.metric,
                    node.filter.clone(),
                    plan_schema_to_arrow(&node.schema),
                    self.config.clone().unwrap_or_default(),
                )))
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
                // Check if this CTE was pre-materialized
                if let Some(ref cte_name) = node.cte_name {
                    let key = Self::cte_name_key(cte_name);
                    let cache = self.cte_cache.lock();
                    if std::env::var("CTE_DEBUG").is_ok() {
                        eprintln!(
                            "[cte] alias {} (key {}) cache {} (len {})",
                            cte_name,
                            key,
                            if cache.contains_key(&key) {
                                "HIT"
                            } else {
                                "MISS"
                            },
                            cache.len()
                        );
                    }
                    if let Some((schema, batches)) = cache.get(&key) {
                        let exec = MemoryTableExec::new(
                            &node.alias,
                            schema.clone(),
                            batches.clone(),
                            None,
                        );
                        return Ok(Arc::new(exec));
                    }
                }
                // Not cached, pass through to input
                self.create_physical_plan_inner(&node.input)
            }

            LogicalPlan::EmptyRelation(node) => {
                let schema = plan_schema_to_arrow(&node.schema);
                let batches = if node.produce_one_row {
                    // A table-less SELECT (`SELECT 1`) is one row of no
                    // columns: the projection above evaluates its expressions
                    // once against it. RecordBatch::new_empty produced a
                    // ZERO-row batch, so those queries returned nothing.
                    // A batch with no columns cannot infer its length, so the
                    // row count has to be stated explicitly.
                    let columns: Vec<arrow::array::ArrayRef> = schema
                        .fields()
                        .iter()
                        .map(|f| arrow::array::new_null_array(f.data_type(), 1))
                        .collect();
                    let options =
                        arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1usize));
                    vec![arrow::record_batch::RecordBatch::try_new_with_options(
                        schema.clone(),
                        columns,
                        &options,
                    )?]
                } else {
                    vec![]
                };
                let exec = MemoryTableExec::new("empty", schema, batches, None);
                Ok(Arc::new(exec))
            }

            LogicalPlan::Values(node) => {
                // Evaluate each row's constant expressions into a real
                // RecordBatch — this used to be a stub returning an always-
                // empty batch ("proper implementation needs expression
                // evaluation"), silently making every `VALUES (...)` a
                // no-op no matter where it was reached from (a bare
                // `VALUES` query, and — the case that surfaced this,
                // native-tables-mutation epic task 002 — `INSERT INTO
                // <table> VALUES (...)`, which task 001's design spike
                // confirmed BINDS via the pre-existing `SetExpr::Values`
                // arm but never actually got this far correctly before).
                //
                // Reuses the SAME "1-row, 0-column dummy batch +
                // evaluate_expr" trick `LogicalPlan::EmptyRelation` above
                // already uses for a table-less `SELECT <literal-expr>`:
                // each `Expr` in a VALUES row is, per the SQL grammar, a
                // constant expression with no column references, so
                // evaluating it against a schema-less 1-row batch is
                // sufficient and needs no new evaluator machinery.
                let schema = plan_schema_to_arrow(&node.schema);
                let batches = if node.values.is_empty() {
                    vec![]
                } else {
                    let dummy_options =
                        arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(1usize));
                    let dummy_row = arrow::record_batch::RecordBatch::try_new_with_options(
                        Arc::new(Schema::empty()),
                        vec![],
                        &dummy_options,
                    )?;

                    let num_cols = schema.fields().len();
                    let mut per_column: Vec<Vec<arrow::array::ArrayRef>> = (0..num_cols)
                        .map(|_| Vec::with_capacity(node.values.len()))
                        .collect();
                    for row in &node.values {
                        if row.len() != num_cols {
                            return Err(QueryError::Plan(format!(
                                "VALUES row has {} expression(s) but the inferred schema has \
                                 {} column(s)",
                                row.len(),
                                num_cols
                            )));
                        }
                        for (i, expr) in row.iter().enumerate() {
                            per_column[i].push(evaluate_expr(&dummy_row, expr)?);
                        }
                    }
                    let columns: Vec<arrow::array::ArrayRef> = per_column
                        .into_iter()
                        .map(|parts| {
                            let refs: Vec<&dyn arrow::array::Array> =
                                parts.iter().map(|a| a.as_ref()).collect();
                            arrow::compute::concat(&refs).map_err(QueryError::from)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    vec![arrow::record_batch::RecordBatch::try_new(
                        schema.clone(),
                        columns,
                    )?]
                };
                let exec = MemoryTableExec::new("values", schema, batches, None);
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
                // HAVING pushdown: a subquery-free predicate directly above an
                // Aggregate filters per shard inside the aggregate instead of
                // materializing every group first.
                if matches!(&*node.input, LogicalPlan::Aggregate(_))
                    && !node.predicate.contains_subquery()
                {
                    *self.pending_agg_filter.borrow_mut() = Some(node.predicate.clone());
                    let agg =
                        self.create_physical_plan_with_delim_state(&node.input, delim_state)?;
                    if self.pending_agg_filter.borrow_mut().take().is_none() {
                        // consumed by the aggregate operator
                        return Ok(agg);
                    }
                    let filter = self.create_filter(agg, node.predicate.clone());
                    return Ok(Arc::new(filter));
                }
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
pub(crate) fn plan_schema_to_arrow(plan_schema: &PlanSchema) -> SchemaRef {
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
    use crate::planner::{Binder, InMemoryCatalog, ScalarValue, SchemaField};
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
    // ------------------------------------------------------------------
    // spill-boundaries task 001: spill-covered scan routing pre-pass.
    // ------------------------------------------------------------------

    fn scan_addrs(plan: &LogicalPlan, out: &mut Vec<(String, usize)>) {
        if let LogicalPlan::Scan(node) = plan {
            out.push((node.table_name.clone(), node as *const _ as usize));
        }
        for c in plan.children() {
            scan_addrs(c, out);
        }
    }

    /// `(table -> covered?)` for every Scan in `plan`, through the SAME
    /// pre-pass `create_physical_plan` runs (CTE ref counts included).
    fn coverage(planner: &PhysicalPlanner, plan: &LogicalPlan) -> Vec<(String, bool)> {
        let mut covered = std::collections::HashSet::new();
        let mut cte_counts: HashMap<String, usize> = HashMap::new();
        PhysicalPlanner::count_cte_refs(plan, &mut cte_counts);
        planner.collect_spill_covered_scans(plan, CoverWalk::root(), &cte_counts, &mut covered);
        let mut scans = Vec::new();
        scan_addrs(plan, &mut scans);
        scans
            .into_iter()
            .map(|(t, a)| (t, covered.contains(&a)))
            .collect()
    }

    fn cover_schema() -> PlanSchema {
        PlanSchema::new(vec![
            SchemaField::new("id", DataType::Int64),
            SchemaField::new("value", DataType::Int64),
        ])
    }

    fn spillable_planner() -> PhysicalPlanner {
        let config = ExecutionConfig::new().with_memory_limit(1 << 20);
        PhysicalPlanner::with_config(crate::execution::create_memory_pool(1 << 20), config)
    }

    fn scan(name: &str) -> crate::planner::LogicalPlanBuilder {
        crate::planner::LogicalPlanBuilder::scan(name, cover_schema())
    }

    fn count_star() -> Expr {
        Expr::Aggregate {
            func: crate::planner::AggregateFunction::Count,
            args: vec![Expr::column("id")],
            distinct: false,
        }
    }

    fn sort_by_value() -> Vec<crate::planner::SortExpr> {
        vec![crate::planner::SortExpr::new(Expr::column("value"))]
    }

    fn cte_alias(name: &str, input: LogicalPlan) -> LogicalPlan {
        let schema = input.schema();
        LogicalPlan::SubqueryAlias(crate::planner::SubqueryAliasNode {
            input: Arc::new(input),
            alias: name.to_string(),
            schema,
            cte_name: Some(name.to_string()),
        })
    }

    #[test]
    fn spill_covered_scans_materializing_shapes_stay_uncovered() {
        let planner = spillable_planner();
        // Raw dump.
        assert_eq!(
            coverage(&planner, &scan("big").build()),
            vec![("big".to_string(), false)]
        );
        // Filter/project-only.
        let plan = scan("big")
            .filter(Expr::column("value").gt(Expr::literal(ScalarValue::Int64(1))))
            .project(vec![Expr::column("id")])
            .unwrap()
            .build();
        assert_eq!(coverage(&planner, &plan), vec![("big".to_string(), false)]);
        // LIMIT-only.
        let plan = scan("big").limit(0, Some(10)).build();
        assert_eq!(coverage(&planner, &plan), vec![("big".to_string(), false)]);
        // A Window materializes its input.
        let input = scan("big").build();
        let plan = LogicalPlan::Window(crate::planner::WindowNode {
            schema: input.schema(),
            input: Arc::new(input),
            window_exprs: vec![],
        });
        assert_eq!(coverage(&planner, &plan), vec![("big".to_string(), false)]);
        // A bare join dump: both sides stream into the join, but the join's
        // own output is dumped — covered by the coordinator's rule (the
        // join IS a spill-capable breaker); a Window ABOVE the join blocks.
        let joined = scan("l")
            .join(
                scan("r").build(),
                JoinType::Inner,
                vec![(Expr::column("id"), Expr::column("id"))],
                None,
            )
            .build();
        let plan = LogicalPlan::Window(crate::planner::WindowNode {
            schema: joined.schema(),
            input: Arc::new(joined),
            window_exprs: vec![],
        });
        assert_eq!(
            coverage(&planner, &plan),
            vec![("l".to_string(), false), ("r".to_string(), false)]
        );
    }

    #[test]
    fn spill_covered_scans_breakers_cover_and_aggregates_rebound() {
        let planner = spillable_planner();
        // Aggregate (the pre-001 rule).
        let plan = scan("big")
            .aggregate(vec![Expr::column("id")], vec![count_star()])
            .unwrap()
            .build();
        assert_eq!(coverage(&planner, &plan), vec![("big".to_string(), true)]);
        // Join: BOTH sides, with a sort + limit above (Q02/Q10's shape).
        let plan = scan("l")
            .join(
                scan("r").build(),
                JoinType::Left,
                vec![(Expr::column("id"), Expr::column("id"))],
                None,
            )
            .sort(sort_by_value())
            .limit(0, Some(100))
            .build();
        assert_eq!(
            coverage(&planner, &plan),
            vec![("l".to_string(), true), ("r".to_string(), true)]
        );
        // Sort alone, with a filter and projection in between.
        let plan = scan("big")
            .filter(Expr::column("value").gt(Expr::literal(ScalarValue::Int64(1))))
            .project(vec![Expr::column("id"), Expr::column("value")])
            .unwrap()
            .sort(sort_by_value())
            .build();
        assert_eq!(coverage(&planner, &plan), vec![("big".to_string(), true)]);
        // Window above an aggregate: the aggregate bounds what the window
        // materializes, so the scan under it is covered.
        let agg = scan("big")
            .aggregate(vec![Expr::column("id")], vec![count_star()])
            .unwrap()
            .build();
        let plan = LogicalPlan::Window(crate::planner::WindowNode {
            schema: agg.schema(),
            input: Arc::new(agg),
            window_exprs: vec![],
        });
        assert_eq!(coverage(&planner, &plan), vec![("big".to_string(), true)]);
        // Address-keyed: the same table covered in one branch and not the
        // other of a UNION ALL.
        let covered = scan("big")
            .aggregate(vec![Expr::column("id")], vec![count_star()])
            .unwrap()
            .project(vec![Expr::column("id")])
            .unwrap()
            .build();
        let dumped = scan("big")
            .project(vec![Expr::column("id")])
            .unwrap()
            .build();
        let plan = LogicalPlan::Union(crate::planner::UnionNode {
            schema: covered.schema(),
            inputs: vec![Arc::new(covered), Arc::new(dumped)],
            all: true,
        });
        assert_eq!(
            coverage(&planner, &plan),
            vec![("big".to_string(), true), ("big".to_string(), false)]
        );
    }

    #[test]
    fn spill_covered_scans_shared_cte_is_a_materializing_root() {
        let planner = spillable_planner();
        // A twice-referenced CTE is materialized whole: a raw scan inside it
        // is NOT covered by the join above the CTE...
        let cte_body = || scan("big").build();
        let plan = crate::planner::LogicalPlanBuilder::scan("s", cover_schema())
            .join(
                cte_alias("c", cte_body()),
                JoinType::Inner,
                vec![(Expr::column("id"), Expr::column("id"))],
                None,
            )
            .join(
                cte_alias("c", cte_body()),
                JoinType::Inner,
                vec![(Expr::column("id"), Expr::column("id"))],
                None,
            )
            .aggregate(vec![], vec![count_star()])
            .unwrap()
            .build();
        assert_eq!(
            coverage(&planner, &plan),
            vec![
                ("s".to_string(), true),
                ("big".to_string(), false),
                ("big".to_string(), false)
            ]
        );
        // ...but an aggregate INSIDE the CTE re-covers it (Q15's `revenue`,
        // Q11's CSE'd HAVING subquery).
        let cte_body = || {
            scan("big")
                .aggregate(vec![Expr::column("id")], vec![count_star()])
                .unwrap()
                .build()
        };
        let plan = crate::planner::LogicalPlanBuilder::scan("s", cover_schema())
            .join(
                cte_alias("c", cte_body()),
                JoinType::Inner,
                vec![(Expr::column("id"), Expr::column("id"))],
                None,
            )
            .join(
                cte_alias("c", cte_body()),
                JoinType::Inner,
                vec![(Expr::column("id"), Expr::column("id"))],
                None,
            )
            .build();
        assert_eq!(
            coverage(&planner, &plan),
            vec![
                ("s".to_string(), true),
                ("big".to_string(), true),
                ("big".to_string(), true)
            ]
        );
        // A once-referenced CTE alias is a plain pass-through.
        let plan = crate::planner::LogicalPlanBuilder::scan("s", cover_schema())
            .join(
                cte_alias("once", scan("big").build()),
                JoinType::Inner,
                vec![(Expr::column("id"), Expr::column("id"))],
                None,
            )
            .build();
        assert_eq!(
            coverage(&planner, &plan),
            vec![("s".to_string(), true), ("big".to_string(), true)]
        );
    }

    #[test]
    fn spill_covered_scans_without_a_memory_config_only_aggregates_cover() {
        let planner = PhysicalPlanner::new();
        let plan = scan("l")
            .join(
                scan("r").build(),
                JoinType::Inner,
                vec![(Expr::column("id"), Expr::column("id"))],
                None,
            )
            .sort(sort_by_value())
            .build();
        assert_eq!(
            coverage(&planner, &plan),
            vec![("l".to_string(), false), ("r".to_string(), false)]
        );
        let plan = scan("big")
            .aggregate(vec![Expr::column("id")], vec![count_star()])
            .unwrap()
            .build();
        assert_eq!(coverage(&planner, &plan), vec![("big".to_string(), true)]);
    }
}
