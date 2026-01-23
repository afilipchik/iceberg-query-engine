//! Projection pushdown optimization rule

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::planner::{Column, Expr, LogicalPlan, ScanNode};
use std::collections::HashSet;
use std::sync::Arc;

/// Projection pushdown rule - pushes column requirements to scans
pub struct ProjectionPushdown;

impl OptimizerRule for ProjectionPushdown {
    fn name(&self) -> &str {
        "ProjectionPushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // Collect all required columns from top and push down
        let required = self.collect_required_columns(plan);
        self.pushdown(plan, &required)
    }
}

impl ProjectionPushdown {
    fn collect_required_columns(&self, plan: &LogicalPlan) -> HashSet<Column> {
        let mut required = HashSet::new();
        self.collect_recursive(plan, &mut required);
        required
    }

    fn collect_recursive(&self, plan: &LogicalPlan, required: &mut HashSet<Column>) {
        match plan {
            LogicalPlan::Scan(node) => {
                // If scan has a filter (from predicate pushdown), extract its columns
                if let Some(filter) = &node.filter {
                    self.extract_columns_from_expr(filter, required);
                }
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
            Expr::Between { expr, low, high, .. } => {
                self.extract_columns_from_expr(expr, required);
                self.extract_columns_from_expr(low, required);
                self.extract_columns_from_expr(high, required);
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

    fn pushdown(&self, plan: &LogicalPlan, required: &HashSet<Column>) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Scan(node) => {
                // Compute projection indices for scan
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

                    if required.contains(&col) || required.contains(&unqualified) || required.is_empty() {
                        projection.push(i);
                    }
                }

                // If all columns are needed, don't set projection
                let projection = if projection.len() == schema.len() {
                    None
                } else if projection.is_empty() {
                    // Need at least one column
                    Some(vec![0])
                } else {
                    Some(projection)
                };

                Ok(LogicalPlan::Scan(ScanNode {
                    table_name: node.table_name.clone(),
                    schema: node.schema.clone(),
                    projection,
                    filter: node.filter.clone(),
                }))
            }

            LogicalPlan::Filter(node) => {
                let input = self.pushdown(&node.input, required)?;
                Ok(LogicalPlan::Filter(crate::planner::FilterNode {
                    input: Arc::new(input),
                    predicate: node.predicate.clone(),
                }))
            }

            LogicalPlan::Project(node) => {
                let input = self.pushdown(&node.input, required)?;
                Ok(LogicalPlan::Project(crate::planner::ProjectNode {
                    input: Arc::new(input),
                    exprs: node.exprs.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Join(node) => {
                let left = self.pushdown(&node.left, required)?;
                let right = self.pushdown(&node.right, required)?;
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
                let input = self.pushdown(&node.input, required)?;
                Ok(LogicalPlan::Aggregate(crate::planner::AggregateNode {
                    input: Arc::new(input),
                    group_by: node.group_by.clone(),
                    aggregates: node.aggregates.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Sort(node) => {
                let input = self.pushdown(&node.input, required)?;
                Ok(LogicalPlan::Sort(crate::planner::SortNode {
                    input: Arc::new(input),
                    order_by: node.order_by.clone(),
                }))
            }

            LogicalPlan::Limit(node) => {
                let input = self.pushdown(&node.input, required)?;
                Ok(LogicalPlan::Limit(crate::planner::LimitNode {
                    input: Arc::new(input),
                    skip: node.skip,
                    fetch: node.fetch,
                }))
            }

            LogicalPlan::Distinct(node) => {
                let input = self.pushdown(&node.input, required)?;
                Ok(LogicalPlan::Distinct(crate::planner::DistinctNode {
                    input: Arc::new(input),
                }))
            }

            LogicalPlan::Union(node) => {
                let inputs: Result<Vec<Arc<LogicalPlan>>> = node
                    .inputs
                    .iter()
                    .map(|i| self.pushdown(i, required).map(Arc::new))
                    .collect();
                Ok(LogicalPlan::Union(crate::planner::UnionNode {
                    inputs: inputs?,
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::SubqueryAlias(node) => {
                let input = self.pushdown(&node.input, required)?;
                Ok(LogicalPlan::SubqueryAlias(crate::planner::SubqueryAliasNode {
                    input: Arc::new(input),
                    alias: node.alias.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => Ok(plan.clone()),
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
