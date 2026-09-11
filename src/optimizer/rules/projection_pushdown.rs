//! Projection pushdown optimization rule

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::planner::{Column, Expr, LogicalPlan, PlanSchema, ProjectNode, ScanNode};
use std::collections::HashSet;
use std::sync::Arc;

/// Projection pushdown rule - pushes column requirements to scans
pub struct ProjectionPushdown;

impl OptimizerRule for ProjectionPushdown {
    fn name(&self) -> &str {
        "ProjectionPushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // Collect all required columns from top and push down. Columns a
        // scan's own pushed-down filter reads are NOT in this set: the scan
        // reads them but a Project drops them from its output, so filter-only
        // columns never ride through joins (Q21 gathered a Utf8
        // o_orderstatus per matched row and carried two filter-only date
        // columns through two joins; the shared column-name matching below
        // also over-projected them onto every other lineitem alias).
        let required = self.collect_required_columns(plan);
        let keep_all = required.is_empty() && !Self::any_scan_filter(plan);
        self.pushdown(plan, &required, keep_all)
    }
}

impl ProjectionPushdown {
    fn collect_required_columns(&self, plan: &LogicalPlan) -> HashSet<Column> {
        let mut required = HashSet::new();
        self.collect_recursive(plan, &mut required);
        required
    }

    /// Whether any scan in the plan carries a pushed-down filter.
    fn any_scan_filter(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Scan(node) => node.filter.is_some(),
            _ => plan.children().iter().any(|c| Self::any_scan_filter(c)),
        }
    }

    fn collect_recursive(&self, plan: &LogicalPlan, required: &mut HashSet<Column>) {
        match plan {
            LogicalPlan::Scan(_) => {
                // A scan's own pushed-down filter columns are handled at the
                // scan itself (read but projected away); registering them
                // here forced them into the scan OUTPUT and, through the
                // name-based alias matching, into every other scan of the
                // same table.
            }
            LogicalPlan::Filter(node) => {
                self.extract_columns_from_expr(&node.predicate, required);
                self.collect_recursive(&node.input, required);
            }
            LogicalPlan::Project(node) => {
                for expr in &node.exprs {
                    self.extract_columns_from_expr(expr, required);
                }
                self.collect_recursive(&node.input, required);
            }
            LogicalPlan::Join(node) => {
                for (l, r) in &node.on {
                    self.extract_columns_from_expr(l, required);
                    self.extract_columns_from_expr(r, required);
                }
                if let Some(filter) = &node.filter {
                    self.extract_columns_from_expr(filter, required);
                }
                self.collect_recursive(&node.left, required);
                self.collect_recursive(&node.right, required);
            }
            LogicalPlan::Aggregate(node) => {
                for expr in &node.group_by {
                    self.extract_columns_from_expr(expr, required);
                }
                for expr in &node.aggregates {
                    self.extract_columns_from_expr(expr, required);
                }
                self.collect_recursive(&node.input, required);
            }
            LogicalPlan::Window(node) => {
                for (_, w) in &node.window_exprs {
                    for e in &w.args {
                        self.extract_columns_from_expr(e, required);
                    }
                    for e in &w.partition_by {
                        self.extract_columns_from_expr(e, required);
                    }
                    for o in &w.order_by {
                        self.extract_columns_from_expr(&o.expr, required);
                    }
                }
                self.collect_recursive(&node.input, required);
            }
            LogicalPlan::Sort(node) => {
                for sort_expr in &node.order_by {
                    self.extract_columns_from_expr(&sort_expr.expr, required);
                }
                self.collect_recursive(&node.input, required);
            }
            LogicalPlan::Limit(node) => {
                self.collect_recursive(&node.input, required);
            }
            LogicalPlan::Distinct(node) => {
                self.collect_recursive(&node.input, required);
            }
            LogicalPlan::Union(node) => {
                for input in &node.inputs {
                    self.collect_recursive(input, required);
                }
            }
            LogicalPlan::SubqueryAlias(node) => {
                self.collect_recursive(&node.input, required);
            }
            LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => {}
            LogicalPlan::DelimJoin(node) => {
                self.collect_recursive(&node.left, required);
                self.collect_recursive(&node.right, required);
            }
            // Opaque; its projection list is already fixed by the rule that
            // built it, and its input must not be re-pruned underneath it.
            LogicalPlan::VectorSearch(_) => {}
            LogicalPlan::DelimGet(_) => {}
        }
    }

    fn extract_columns_from_expr(&self, expr: &Expr, required: &mut HashSet<Column>) {
        match expr {
            Expr::Column(col) => {
                required.insert(col.clone());
            }
            Expr::BinaryExpr { left, right, .. } => {
                self.extract_columns_from_expr(left, required);
                self.extract_columns_from_expr(right, required);
            }
            Expr::UnaryExpr { expr, .. } => {
                self.extract_columns_from_expr(expr, required);
            }
            Expr::ScalarFunc { args, .. } | Expr::Aggregate { args, .. } => {
                for arg in args {
                    self.extract_columns_from_expr(arg, required);
                }
            }
            Expr::Cast { expr, .. } | Expr::Alias { expr, .. } => {
                self.extract_columns_from_expr(expr, required);
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    self.extract_columns_from_expr(op, required);
                }
                for (w, t) in when_then {
                    self.extract_columns_from_expr(w, required);
                    self.extract_columns_from_expr(t, required);
                }
                if let Some(e) = else_expr {
                    self.extract_columns_from_expr(e, required);
                }
            }
            Expr::InList { expr, list, .. } => {
                self.extract_columns_from_expr(expr, required);
                for item in list {
                    self.extract_columns_from_expr(item, required);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.extract_columns_from_expr(expr, required);
                self.extract_columns_from_expr(low, required);
                self.extract_columns_from_expr(high, required);
            }
            Expr::InSubquery { expr, subquery, .. } => {
                // Extract columns from the left side of the IN expression
                self.extract_columns_from_expr(expr, required);
                // Inner-local columns have their own scope, but correlated
                // references still require columns from this input, just as
                // they do for EXISTS and scalar subqueries.
                self.extract_outer_columns_from_subquery(subquery, required);
            }
            Expr::Exists { subquery, .. } => {
                // For EXISTS, we need to extract outer column references from the subquery
                // These are columns that reference tables NOT defined in the subquery
                self.extract_outer_columns_from_subquery(subquery, required);
            }
            Expr::ScalarSubquery(subquery) => {
                // Same for scalar subqueries - extract outer column references
                self.extract_outer_columns_from_subquery(subquery, required);
            }
            Expr::Wildcard => {
                // Wildcard means all columns - we can't push projection
            }
            Expr::QualifiedWildcard(_) => {
                // Same as wildcard for specific table
            }
            _ => {}
        }
    }

    /// Extract outer column references from a subquery
    /// These are columns that reference tables NOT defined within the subquery
    fn extract_outer_columns_from_subquery(
        &self,
        subquery: &LogicalPlan,
        required: &mut HashSet<Column>,
    ) {
        // Collect table aliases defined in the subquery
        let local_tables = self.collect_subquery_tables(subquery);
        // Extract columns from the subquery that reference outer tables
        self.extract_outer_columns_recursive(subquery, &local_tables, required);
    }

    /// Collect table names/aliases defined in a subquery
    fn collect_subquery_tables(&self, plan: &LogicalPlan) -> HashSet<String> {
        let mut tables = HashSet::new();
        self.collect_tables_recursive(plan, &mut tables);
        tables
    }

    fn collect_tables_recursive(&self, plan: &LogicalPlan, tables: &mut HashSet<String>) {
        match plan {
            LogicalPlan::Scan(node) => {
                tables.insert(node.table_name.clone());
            }
            LogicalPlan::SubqueryAlias(node) => {
                tables.insert(node.alias.clone());
                // Don't recurse - the alias shadows the input
            }
            LogicalPlan::Join(node) => {
                self.collect_tables_recursive(&node.left, tables);
                self.collect_tables_recursive(&node.right, tables);
            }
            LogicalPlan::Filter(node) => {
                self.collect_tables_recursive(&node.input, tables);
            }
            LogicalPlan::Project(node) => {
                self.collect_tables_recursive(&node.input, tables);
            }
            LogicalPlan::Aggregate(node) => {
                self.collect_tables_recursive(&node.input, tables);
            }
            LogicalPlan::Sort(node) => {
                self.collect_tables_recursive(&node.input, tables);
            }
            LogicalPlan::Limit(node) => {
                self.collect_tables_recursive(&node.input, tables);
            }
            LogicalPlan::Distinct(node) => {
                self.collect_tables_recursive(&node.input, tables);
            }
            _ => {}
        }
    }

    /// Extract columns from a plan that reference tables NOT in local_tables (i.e., outer references)
    fn extract_outer_columns_recursive(
        &self,
        plan: &LogicalPlan,
        local_tables: &HashSet<String>,
        required: &mut HashSet<Column>,
    ) {
        match plan {
            LogicalPlan::Filter(node) => {
                self.extract_outer_columns_from_expr(&node.predicate, local_tables, required);
                self.extract_outer_columns_recursive(&node.input, local_tables, required);
            }
            LogicalPlan::Project(node) => {
                for expr in &node.exprs {
                    self.extract_outer_columns_from_expr(expr, local_tables, required);
                }
                self.extract_outer_columns_recursive(&node.input, local_tables, required);
            }
            LogicalPlan::Aggregate(node) => {
                for expr in &node.group_by {
                    self.extract_outer_columns_from_expr(expr, local_tables, required);
                }
                for expr in &node.aggregates {
                    self.extract_outer_columns_from_expr(expr, local_tables, required);
                }
                self.extract_outer_columns_recursive(&node.input, local_tables, required);
            }
            LogicalPlan::Join(node) => {
                for (l, r) in &node.on {
                    self.extract_outer_columns_from_expr(l, local_tables, required);
                    self.extract_outer_columns_from_expr(r, local_tables, required);
                }
                if let Some(filter) = &node.filter {
                    self.extract_outer_columns_from_expr(filter, local_tables, required);
                }
                self.extract_outer_columns_recursive(&node.left, local_tables, required);
                self.extract_outer_columns_recursive(&node.right, local_tables, required);
            }
            LogicalPlan::Sort(node) => {
                for sort_expr in &node.order_by {
                    self.extract_outer_columns_from_expr(&sort_expr.expr, local_tables, required);
                }
                self.extract_outer_columns_recursive(&node.input, local_tables, required);
            }
            LogicalPlan::Limit(node) => {
                self.extract_outer_columns_recursive(&node.input, local_tables, required);
            }
            LogicalPlan::Distinct(node) => {
                self.extract_outer_columns_recursive(&node.input, local_tables, required);
            }
            LogicalPlan::SubqueryAlias(node) => {
                self.extract_outer_columns_recursive(&node.input, local_tables, required);
            }
            LogicalPlan::Scan(node) => {
                if let Some(filter) = &node.filter {
                    self.extract_outer_columns_from_expr(filter, local_tables, required);
                }
            }
            _ => {}
        }
    }

    /// Extract columns from an expression that reference tables NOT in local_tables
    fn extract_outer_columns_from_expr(
        &self,
        expr: &Expr,
        local_tables: &HashSet<String>,
        required: &mut HashSet<Column>,
    ) {
        match expr {
            Expr::Column(col) => {
                // Only add if this column references an outer table
                if let Some(rel) = &col.relation {
                    if !local_tables.contains(rel) {
                        // This is an outer reference
                        required.insert(col.clone());
                    }
                }
                // For unqualified columns, we assume they're local (can't determine definitively)
            }
            Expr::BinaryExpr { left, right, .. } => {
                self.extract_outer_columns_from_expr(left, local_tables, required);
                self.extract_outer_columns_from_expr(right, local_tables, required);
            }
            Expr::UnaryExpr { expr, .. } => {
                self.extract_outer_columns_from_expr(expr, local_tables, required);
            }
            Expr::ScalarFunc { args, .. } | Expr::Aggregate { args, .. } => {
                for arg in args {
                    self.extract_outer_columns_from_expr(arg, local_tables, required);
                }
            }
            Expr::Cast { expr, .. } | Expr::Alias { expr, .. } => {
                self.extract_outer_columns_from_expr(expr, local_tables, required);
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    self.extract_outer_columns_from_expr(op, local_tables, required);
                }
                for (w, t) in when_then {
                    self.extract_outer_columns_from_expr(w, local_tables, required);
                    self.extract_outer_columns_from_expr(t, local_tables, required);
                }
                if let Some(e) = else_expr {
                    self.extract_outer_columns_from_expr(e, local_tables, required);
                }
            }
            Expr::InList { expr, list, .. } => {
                self.extract_outer_columns_from_expr(expr, local_tables, required);
                for item in list {
                    self.extract_outer_columns_from_expr(item, local_tables, required);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.extract_outer_columns_from_expr(expr, local_tables, required);
                self.extract_outer_columns_from_expr(low, local_tables, required);
                self.extract_outer_columns_from_expr(high, local_tables, required);
            }
            _ => {}
        }
    }

    fn pushdown(
        &self,
        plan: &LogicalPlan,
        required: &HashSet<Column>,
        keep_all: bool,
    ) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Scan(node) => {
                // Compute projection indices for scan: the columns consumers
                // ABOVE the scan read (`required` excludes this scan's own
                // filter columns).
                let schema = &node.schema;
                let mut projection = Vec::new();

                for (i, field) in schema.fields().iter().enumerate() {
                    // Include column if it's required
                    let col = Column {
                        relation: field.relation.clone(),
                        name: field.name.clone(),
                    };
                    let unqualified = Column {
                        relation: None,
                        name: field.name.clone(),
                    };

                    // Check for exact match or unqualified match
                    let mut is_required =
                        required.contains(&col) || required.contains(&unqualified) || keep_all;

                    // Also check if any required column has the same name (handles table aliases)
                    // e.g., required has "l1.l_orderkey" but schema has "lineitem.l_orderkey"
                    if !is_required {
                        for req_col in required.iter() {
                            if req_col.name == field.name {
                                is_required = true;
                                break;
                            }
                        }
                    }

                    if is_required {
                        projection.push(i);
                    }
                }

                // Columns the pushed-down scan filter reads (loose name
                // matching, same as above).
                let mut read = projection.clone();
                if let Some(filter) = &node.filter {
                    let mut filter_cols: HashSet<Column> = HashSet::new();
                    self.extract_columns_from_expr(filter, &mut filter_cols);
                    for (i, field) in schema.fields().iter().enumerate() {
                        if read.contains(&i) {
                            continue;
                        }
                        let needed = filter_cols.iter().any(|c| {
                            c.name == field.name
                                || c.name.ends_with(&format!(".{}", field.name))
                                || field.name.ends_with(&format!(".{}", c.name))
                        });
                        if needed {
                            read.push(i);
                        }
                    }
                    read.sort_unstable();
                }

                let make_scan = |proj: Vec<usize>| {
                    let projection = if proj.len() == schema.len() {
                        None
                    } else if proj.is_empty() {
                        // Need at least one column
                        Some(vec![0])
                    } else {
                        Some(proj)
                    };
                    LogicalPlan::Scan(ScanNode {
                        table_name: node.table_name.clone(),
                        schema: node.schema.clone(),
                        projection,
                        filter: node.filter.clone(),
                    })
                };

                // No filter-only columns, or nothing above needs this scan's
                // columns at all: keep the legacy single-projection shape
                // (reading exactly what the filter needs in the latter case).
                if projection.is_empty() || read.len() == projection.len() {
                    return Ok(make_scan(read));
                }

                // Filter-only columns present: the scan reads them (the
                // filter runs inside the scan) and a Project drops them from
                // the output, so they are never carried through joins.
                let kept: Vec<crate::planner::SchemaField> = projection
                    .iter()
                    .map(|&i| schema.fields()[i].clone())
                    .collect();
                let exprs: Vec<Expr> = kept
                    .iter()
                    .map(|f| {
                        Expr::Column(Column {
                            relation: f.relation.clone(),
                            name: f.name.clone(),
                        })
                    })
                    .collect();
                Ok(LogicalPlan::Project(ProjectNode {
                    input: Arc::new(make_scan(read)),
                    exprs,
                    schema: PlanSchema::new(kept),
                }))
            }

            LogicalPlan::Filter(node) => {
                let input = self.pushdown(&node.input, required, keep_all)?;
                Ok(LogicalPlan::Filter(crate::planner::FilterNode {
                    input: Arc::new(input),
                    predicate: node.predicate.clone(),
                }))
            }

            LogicalPlan::Project(node) => {
                let input = self.pushdown(&node.input, required, keep_all)?;
                // Collapse only an actual outer identity mapping. Equal Exprs
                // are not a composition proof: repeated x+1 applies twice, and
                // a renamed/reordered inner schema changes column resolution.
                if let LogicalPlan::Project(inner) = &input {
                    let identity = node.schema == inner.schema
                        && node.exprs.len() == inner.schema.fields().len()
                        && node.exprs.iter().enumerate().all(|(i, expr)| {
                            let Expr::Column(column) = expr else {
                                return false;
                            };
                            inner
                                .schema
                                .resolve_column(column)
                                .is_some_and(|(resolved, _)| resolved == i)
                        });
                    if identity {
                        return Ok(input);
                    }
                }
                Ok(LogicalPlan::Project(crate::planner::ProjectNode {
                    input: Arc::new(input),
                    exprs: node.exprs.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Join(node) => {
                let left = self.pushdown(&node.left, required, keep_all)?;
                let right = self.pushdown(&node.right, required, keep_all)?;
                Ok(LogicalPlan::Join(crate::planner::JoinNode {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    join_type: node.join_type,
                    on: node.on.clone(),
                    filter: node.filter.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Aggregate(node) => {
                let input = self.pushdown(&node.input, required, keep_all)?;
                Ok(LogicalPlan::Aggregate(crate::planner::AggregateNode {
                    input: Arc::new(input),
                    group_by: node.group_by.clone(),
                    aggregates: node.aggregates.clone(),
                    schema: node.schema.clone(),
                }))
            }

            // Window output = ALL input columns + the window columns (schema
            // fixed at bind time), so nothing may be pruned beneath it.
            LogicalPlan::Window(node) => {
                let input = self.pushdown(&node.input, required, true)?;
                Ok(LogicalPlan::Window(crate::planner::WindowNode {
                    input: Arc::new(input),
                    window_exprs: node.window_exprs.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Sort(node) => {
                let input = self.pushdown(&node.input, required, keep_all)?;
                Ok(LogicalPlan::Sort(crate::planner::SortNode {
                    input: Arc::new(input),
                    order_by: node.order_by.clone(),
                }))
            }

            LogicalPlan::Limit(node) => {
                let input = self.pushdown(&node.input, required, keep_all)?;
                Ok(LogicalPlan::Limit(crate::planner::LimitNode {
                    input: Arc::new(input),
                    skip: node.skip,
                    fetch: node.fetch,
                }))
            }

            LogicalPlan::Distinct(node) => {
                let input = self.pushdown(&node.input, required, keep_all)?;
                Ok(LogicalPlan::Distinct(crate::planner::DistinctNode {
                    input: Arc::new(input),
                }))
            }

            LogicalPlan::Union(node) => {
                let inputs: Result<Vec<Arc<LogicalPlan>>> = node
                    .inputs
                    .iter()
                    .map(|i| self.pushdown(i, required, keep_all).map(Arc::new))
                    .collect();
                Ok(LogicalPlan::Union(crate::planner::UnionNode {
                    inputs: inputs?,
                    schema: node.schema.clone(),
                    all: node.all,
                }))
            }

            LogicalPlan::SubqueryAlias(node) => {
                let input = self.pushdown(&node.input, required, keep_all)?;
                Ok(LogicalPlan::SubqueryAlias(
                    crate::planner::SubqueryAliasNode {
                        input: Arc::new(input),
                        alias: node.alias.clone(),
                        schema: node.schema.clone(),
                        cte_name: node.cte_name.clone(),
                    },
                ))
            }

            LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => Ok(plan.clone()),

            LogicalPlan::DelimJoin(node) => {
                // Recursively push projections but don't modify DelimJoin structure
                let left = self.pushdown(&node.left, required, keep_all)?;
                let right = self.pushdown(&node.right, required, keep_all)?;
                Ok(LogicalPlan::DelimJoin(crate::planner::DelimJoinNode {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    join_type: node.join_type,
                    delim_columns: node.delim_columns.clone(),
                    on: node.on.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::VectorSearch(node) => Ok(LogicalPlan::VectorSearch(node.clone())),
            LogicalPlan::DelimGet(node) => Ok(LogicalPlan::DelimGet(node.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{LogicalPlanBuilder, PlanSchema, SchemaField};
    use arrow::datatypes::DataType;

    fn sample_schema() -> PlanSchema {
        PlanSchema::new(vec![
            SchemaField::new("id", DataType::Int64),
            SchemaField::new("name", DataType::Utf8),
            SchemaField::new("amount", DataType::Float64),
            SchemaField::new("status", DataType::Utf8),
        ])
    }

    #[test]
    fn test_projection_pushdown() {
        let rule = ProjectionPushdown;

        let plan = LogicalPlanBuilder::scan("orders", sample_schema())
            .project(vec![Expr::column("id"), Expr::column("amount")])
            .unwrap()
            .build();

        let optimized = rule.optimize(&plan).unwrap();

        // Find the scan and check projection
        fn find_scan_projection(plan: &LogicalPlan) -> Option<&Vec<usize>> {
            match plan {
                LogicalPlan::Scan(scan) => scan.projection.as_ref(),
                LogicalPlan::Project(p) => find_scan_projection(&p.input),
                _ => None,
            }
        }

        let proj = find_scan_projection(&optimized);
        assert!(proj.is_some());
        // Should only include id (0) and amount (2)
        let proj = proj.unwrap();
        assert_eq!(proj.len(), 2);
    }
}

#[cfg(test)]
mod projection_composition_contract_tests {
    use super::*;
    use crate::physical::{FilterExec, MemoryTableExec, PhysicalOperator, ProjectExec};
    use crate::planner::{BinaryOp, ScalarValue, SchemaField};
    use arrow::{
        array::{Array, Int64Array},
        datatypes::DataType,
        record_batch::RecordBatch,
    };
    use futures::TryStreamExt;
    fn schema(names: &[&str]) -> PlanSchema {
        PlanSchema::new(
            names
                .iter()
                .map(|n| SchemaField::new(*n, DataType::Int64))
                .collect(),
        )
    }
    fn scan(names: &[&str]) -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            table_name: "fixture".into(),
            schema: schema(names),
            projection: None,
            filter: None,
        })
    }
    fn project(input: LogicalPlan, exprs: Vec<Expr>, names: &[&str]) -> LogicalPlan {
        LogicalPlan::Project(ProjectNode {
            input: Arc::new(input),
            exprs,
            schema: schema(names),
        })
    }
    fn add_one() -> Expr {
        Expr::Alias {
            name: "x".into(),
            expr: Box::new(Expr::BinaryExpr {
                left: Box::new(Expr::column("x")),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(ScalarValue::Int64(1))),
            }),
        }
    }
    fn physical(plan: &LogicalPlan, batch: &RecordBatch) -> Arc<dyn PhysicalOperator> {
        match plan {
            LogicalPlan::Scan(node) => {
                let mut input: Arc<dyn PhysicalOperator> = Arc::new(MemoryTableExec::new(
                    "fixture",
                    batch.schema(),
                    vec![batch.clone()],
                    None,
                ));
                if let Some(predicate) = &node.filter {
                    input = Arc::new(FilterExec::new(input, predicate.clone()));
                }
                if let Some(projection) = &node.projection {
                    let exprs = projection
                        .iter()
                        .map(|i| Expr::column(&node.schema.fields()[*i].name))
                        .collect();
                    let output =
                        Arc::new(node.schema.to_arrow_schema().project(projection).unwrap());
                    input = Arc::new(ProjectExec::new(input, exprs, output));
                }
                input
            }
            LogicalPlan::Project(node) => Arc::new(ProjectExec::new(
                physical(&node.input, batch),
                node.exprs.clone(),
                node.schema.to_arrow_schema_ref(),
            )),
            other => panic!("unexpected fixture plan {other:?}"),
        }
    }
    async fn rows(
        plan: &LogicalPlan,
        names: &[&str],
        values: Vec<Vec<i64>>,
    ) -> (Vec<String>, Vec<Vec<i64>>) {
        let arrays = names
            .iter()
            .zip(values)
            .map(|(name, v)| {
                (
                    *name,
                    Arc::new(Int64Array::from(v)) as arrow::array::ArrayRef,
                )
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_from_iter(arrays).unwrap();
        let op = physical(plan, &batch);
        let mut result = Vec::new();
        let mut fields = None;
        for part in 0..op.output_partitions() {
            let batches = op
                .execute(part)
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            for b in batches {
                let names = b
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>();
                if let Some(prior) = &fields {
                    assert_eq!(prior, &names);
                } else {
                    fields = Some(names);
                }
                for row in 0..b.num_rows() {
                    result.push(
                        b.columns()
                            .iter()
                            .map(|a| {
                                let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
                                assert!(!a.is_null(row));
                                a.value(row)
                            })
                            .collect(),
                    );
                }
            }
        }
        (fields.unwrap(), result)
    }
    #[tokio::test]
    async fn repeated_computed_projection_is_not_idempotent() {
        let plan = project(
            project(scan(&["x"]), vec![add_one()], &["x"]),
            vec![add_one()],
            &["x"],
        );
        let optimized = ProjectionPushdown.optimize(&plan).unwrap();
        let (fields, actual) = rows(&optimized, &["x"], vec![vec![1]]).await;
        assert_eq!(fields, vec!["x"]);
        assert_eq!(actual, vec![vec![3]]);
    }
    #[tokio::test]
    async fn equal_column_expressions_require_identity_indices_and_exact_schema() {
        for output_names in [["b", "a"], ["a", "b"]] {
            let exprs = vec![Expr::column("a"), Expr::column("b")];
            let inner = project(scan(&["a", "b"]), exprs.clone(), &["b", "a"]);
            let plan = project(inner, exprs, &output_names);
            let optimized = ProjectionPushdown.optimize(&plan).unwrap();
            let (fields, actual) = rows(&optimized, &["a", "b"], vec![vec![10], vec![20]]).await;
            assert_eq!(fields, output_names);
            assert_eq!(actual, vec![vec![20, 10]]);
        }
    }
    #[tokio::test]
    async fn repeated_filter_scan_pruning_still_collapses_passthrough() {
        let mut source = scan(&["x", "hidden"]);
        let LogicalPlan::Scan(ref mut node) = source else {
            unreachable!()
        };
        node.filter = Some(Expr::BinaryExpr {
            left: Box::new(Expr::column("hidden")),
            op: BinaryOp::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Int64(0))),
        });
        let mut plan = project(source, vec![Expr::column("x")], &["x"]);
        for _ in 0..4 {
            plan = ProjectionPushdown.optimize(&plan).unwrap();
        }
        fn counts(plan: &LogicalPlan) -> usize {
            match plan {
                LogicalPlan::Project(p) => 1 + counts(&p.input),
                _ => 0,
            }
        }
        assert_eq!(counts(&plan), 1, "scan-pruning identity must not stack");
        let (fields, actual) = rows(&plan, &["x", "hidden"], vec![vec![7, 9], vec![1, 0]]).await;
        assert_eq!(fields, vec!["x"]);
        assert_eq!(actual, vec![vec![7]]);
    }
    #[test]
    fn ambiguous_identity_is_not_collapsed() {
        let inner = project(
            scan(&["x"]),
            vec![Expr::column("x"), Expr::column("x")],
            &["x", "x"],
        );
        let plan = project(
            inner,
            vec![Expr::column("x"), Expr::column("x")],
            &["x", "x"],
        );
        let optimized = ProjectionPushdown.optimize(&plan).unwrap();
        let LogicalPlan::Project(outer) = optimized else {
            panic!("outer required")
        };
        assert!(
            matches!(outer.input.as_ref(), LogicalPlan::Project(_)),
            "ambiguous passthrough is not a proof"
        );
    }
}
