//! Predicate pushdown optimization rule

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::planner::{
    BinaryOp, Column, Expr, FilterNode, JoinType, LogicalPlan, ScanNode,
};
use std::collections::HashSet;
use std::sync::Arc;

/// Predicate pushdown rule - pushes filters closer to data sources
pub struct PredicatePushdown;

impl OptimizerRule for PredicatePushdown {
    fn name(&self) -> &str {
        "PredicatePushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.pushdown(plan, vec![])
    }
}

impl PredicatePushdown {
    fn pushdown(&self, plan: &LogicalPlan, predicates: Vec<Expr>) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(node) => {
                // Collect this filter's predicates and push down
                let mut all_predicates = predicates;
                self.split_conjunction(&node.predicate, &mut all_predicates);
                self.pushdown(&node.input, all_predicates)
            }

            LogicalPlan::Scan(node) => {
                // Push predicates into scan
                if predicates.is_empty() {
                    return Ok(plan.clone());
                }

                let schema = &node.schema;
                let scan_cols = self.collect_columns(schema);

                // Separate predicates that can be pushed to scan vs those that can't
                let (pushable, remaining): (Vec<Expr>, Vec<Expr>) = predicates
                    .into_iter()
                    .partition(|p| self.can_push_to_scan(p, &scan_cols));

                let scan_filter = if !pushable.is_empty() {
                    let combined = self.combine_predicates(pushable);
                    match &node.filter {
                        Some(existing) => Some(existing.clone().and(combined)),
                        None => Some(combined),
                    }
                } else {
                    node.filter.clone()
                };

                let scan = LogicalPlan::Scan(ScanNode {
                    table_name: node.table_name.clone(),
                    schema: node.schema.clone(),
                    projection: node.projection.clone(),
                    filter: scan_filter,
                });

                // Apply remaining predicates
                if remaining.is_empty() {
                    Ok(scan)
                } else {
                    let combined = self.combine_predicates(remaining);
                    Ok(LogicalPlan::Filter(FilterNode {
                        input: Arc::new(scan),
                        predicate: combined,
                    }))
                }
            }

            LogicalPlan::Project(node) => {
                // Push through projection if predicates only reference projected columns
                let result = self.pushdown(&node.input, predicates)?;
                Ok(LogicalPlan::Project(crate::planner::ProjectNode {
                    input: Arc::new(result),
                    exprs: node.exprs.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Join(node) => {
                // Get ACTUAL base table columns by recursing through joins
                // This is crucial for correctly partitioning predicates in nested joins
                let left_base_tables = self.collect_base_table_columns(&node.left);
                let right_base_tables = self.collect_base_table_columns(&node.right);

                let mut left_predicates = Vec::new();
                let mut right_predicates = Vec::new();
                let mut remaining = Vec::new();

                // Clone predicates before iterating (since we need them later for CROSS join case)
                let all_predicates = predicates.clone();
                let has_predicates = !predicates.is_empty();

                for pred in predicates {
                    let pred_cols = self.extract_columns(&pred);

                    // Check if predicate uses ONLY columns from left side's base tables
                    if self.columns_subset(&pred_cols, &left_base_tables)
                        && !self.columns_overlap(&pred_cols, &right_base_tables)
                    {
                        left_predicates.push(pred);
                    } else if self.columns_subset(&pred_cols, &right_base_tables)
                        && !self.columns_overlap(&pred_cols, &left_base_tables)
                    {
                        right_predicates.push(pred);
                    } else {
                        remaining.push(pred);
                    }
                }

                // For inner joins, we can push more aggressively
                // For outer joins, we need to be careful
                match node.join_type {
                    JoinType::Inner | JoinType::Cross => {
                        // TEMPORARY FIX: For CROSS joins, skip predicate pushdown entirely
                        // to preserve join tree structure. This prevents tables from being dropped.
                        if node.join_type == JoinType::Cross && node.on.is_empty() {
                            // Don't push down predicates for CROSS joins
                            // Just reconstruct the join as-is
                            let left = self.pushdown(&node.left, vec![])?;
                            let right = self.pushdown(&node.right, vec![])?;

                            let left_schema = left.schema();
                            let right_schema = right.schema();
                            let join_schema = left_schema.merge(&right_schema);

                            let join = LogicalPlan::Join(crate::planner::JoinNode {
                                left: Arc::new(left),
                                right: Arc::new(right),
                                join_type: node.join_type,
                                on: node.on.clone(),
                                filter: node.filter.clone(),
                                schema: join_schema,
                            });

                            // Apply all predicates as a filter above the join
                            if !has_predicates {
                                Ok(join)
                            } else {
                                let combined = self.combine_predicates(all_predicates);
                                Ok(LogicalPlan::Filter(FilterNode {
                                    input: Arc::new(join),
                                    predicate: combined,
                                }))
                            }
                        } else {
                            // For INNER joins with existing conditions, use the normal logic
                            let mut join_conditions: Vec<(Expr, Expr)> = node.on.clone();
                            let mut non_join_remaining = Vec::new();

                            for pred in &remaining {
                                if let Some((left_expr, right_expr)) =
                                    self.extract_join_condition(pred, &left_base_tables, &right_base_tables)
                                {
                                    join_conditions.push((left_expr, right_expr));
                                } else {
                                    non_join_remaining.push(pred.clone());
                                }
                            }

                            let left = self.pushdown(&node.left, left_predicates)?;
                            let right = self.pushdown(&node.right, right_predicates)?;

                            let left_schema = left.schema();
                            let right_schema = right.schema();
                            let join_schema = left_schema.merge(&right_schema);

                            let join = LogicalPlan::Join(crate::planner::JoinNode {
                                left: Arc::new(left),
                                right: Arc::new(right),
                                join_type: node.join_type,
                                on: join_conditions,
                                filter: node.filter.clone(),
                                schema: join_schema,
                            });

                            if non_join_remaining.is_empty() {
                                Ok(join)
                            } else {
                                let combined = self.combine_predicates(non_join_remaining);
                                Ok(LogicalPlan::Filter(FilterNode {
                                    input: Arc::new(join),
                                    predicate: combined,
                                }))
                            }
                        }
                    }
                    JoinType::Left => {
                        // Can only push to left side
                        let left = self.pushdown(&node.left, left_predicates)?;
                        let right = self.pushdown(&node.right, vec![])?;
                        remaining.extend(right_predicates);

                        // Compute schema from optimized children before moving them
                        let left_schema = left.schema();
                        let right_schema = right.schema();
                        let join_schema = left_schema.merge(&right_schema);

                        let join = LogicalPlan::Join(crate::planner::JoinNode {
                            left: Arc::new(left),
                            right: Arc::new(right),
                            join_type: node.join_type,
                            on: node.on.clone(),
                            filter: node.filter.clone(),
                            schema: join_schema,
                        });

                        if remaining.is_empty() {
                            Ok(join)
                        } else {
                            let combined = self.combine_predicates(remaining);
                            Ok(LogicalPlan::Filter(FilterNode {
                                input: Arc::new(join),
                                predicate: combined,
                            }))
                        }
                    }
                    JoinType::Right => {
                        // Can only push to right side
                        let left = self.pushdown(&node.left, vec![])?;
                        let right = self.pushdown(&node.right, right_predicates)?;
                        remaining.extend(left_predicates);

                        // Compute schema from optimized children before moving them
                        let left_schema = left.schema();
                        let right_schema = right.schema();
                        let join_schema = left_schema.merge(&right_schema);

                        let join = LogicalPlan::Join(crate::planner::JoinNode {
                            left: Arc::new(left),
                            right: Arc::new(right),
                            join_type: node.join_type,
                            on: node.on.clone(),
                            filter: node.filter.clone(),
                            schema: join_schema,
                        });

                        if remaining.is_empty() {
                            Ok(join)
                        } else {
                            let combined = self.combine_predicates(remaining);
                            Ok(LogicalPlan::Filter(FilterNode {
                                input: Arc::new(join),
                                predicate: combined,
                            }))
                        }
                    }
                    _ => {
                        // Full, Semi, Anti - keep predicates above
                        remaining.extend(left_predicates);
                        remaining.extend(right_predicates);

                        let left = self.pushdown(&node.left, vec![])?;
                        let right = self.pushdown(&node.right, vec![])?;

                        // Compute schema from optimized children before moving them
                        let left_schema = left.schema();
                        let right_schema = right.schema();
                        let join_schema = left_schema.merge(&right_schema);

                        let join = LogicalPlan::Join(crate::planner::JoinNode {
                            left: Arc::new(left),
                            right: Arc::new(right),
                            join_type: node.join_type,
                            on: node.on.clone(),
                            filter: node.filter.clone(),
                            schema: join_schema,
                        });

                        if remaining.is_empty() {
                            Ok(join)
                        } else {
                            let combined = self.combine_predicates(remaining);
                            Ok(LogicalPlan::Filter(FilterNode {
                                input: Arc::new(join),
                                predicate: combined,
                            }))
                        }
                    }
                }
            }

            LogicalPlan::Aggregate(node) => {
                // Cannot push predicates through aggregation in general
                // Could push predicates on group-by columns
                let input = self.pushdown(&node.input, vec![])?;

                let agg = LogicalPlan::Aggregate(crate::planner::AggregateNode {
                    input: Arc::new(input),
                    group_by: node.group_by.clone(),
                    aggregates: node.aggregates.clone(),
                    schema: node.schema.clone(),
                });

                if predicates.is_empty() {
                    Ok(agg)
                } else {
                    let combined = self.combine_predicates(predicates);
                    Ok(LogicalPlan::Filter(FilterNode {
                        input: Arc::new(agg),
                        predicate: combined,
                    }))
                }
            }

            LogicalPlan::Sort(node) => {
                // Push through sort
                let input = self.pushdown(&node.input, predicates)?;
                Ok(LogicalPlan::Sort(crate::planner::SortNode {
                    input: Arc::new(input),
                    order_by: node.order_by.clone(),
                }))
            }

            LogicalPlan::Limit(node) => {
                // Push through limit
                let input = self.pushdown(&node.input, predicates)?;
                Ok(LogicalPlan::Limit(crate::planner::LimitNode {
                    input: Arc::new(input),
                    skip: node.skip,
                    fetch: node.fetch,
                }))
            }

            LogicalPlan::Distinct(node) => {
                let input = self.pushdown(&node.input, predicates)?;
                Ok(LogicalPlan::Distinct(crate::planner::DistinctNode {
                    input: Arc::new(input),
                }))
            }

            LogicalPlan::SubqueryAlias(node) => {
                let input = self.pushdown(&node.input, predicates)?;
                Ok(LogicalPlan::SubqueryAlias(crate::planner::SubqueryAliasNode {
                    input: Arc::new(input),
                    alias: node.alias.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Union(node) => {
                // Cannot push through union without pushing to all branches
                // For now, just apply predicates above
                let inputs: Result<Vec<Arc<LogicalPlan>>> = node
                    .inputs
                    .iter()
                    .map(|i| self.pushdown(i, vec![]).map(Arc::new))
                    .collect();

                let union = LogicalPlan::Union(crate::planner::UnionNode {
                    inputs: inputs?,
                    schema: node.schema.clone(),
                });

                if predicates.is_empty() {
                    Ok(union)
                } else {
                    let combined = self.combine_predicates(predicates);
                    Ok(LogicalPlan::Filter(FilterNode {
                        input: Arc::new(union),
                        predicate: combined,
                    }))
                }
            }

            LogicalPlan::EmptyRelation(node) => {
                if predicates.is_empty() {
                    Ok(plan.clone())
                } else {
                    let combined = self.combine_predicates(predicates);
                    Ok(LogicalPlan::Filter(FilterNode {
                        input: Arc::new(LogicalPlan::EmptyRelation(node.clone())),
                        predicate: combined,
                    }))
                }
            }

            LogicalPlan::Values(node) => {
                if predicates.is_empty() {
                    Ok(plan.clone())
                } else {
                    let combined = self.combine_predicates(predicates);
                    Ok(LogicalPlan::Filter(FilterNode {
                        input: Arc::new(LogicalPlan::Values(node.clone())),
                        predicate: combined,
                    }))
                }
            }
        }
    }

    fn split_conjunction(&self, expr: &Expr, predicates: &mut Vec<Expr>) {
        match expr {
            Expr::BinaryExpr {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.split_conjunction(left, predicates);
                self.split_conjunction(right, predicates);
            }
            _ => {
                predicates.push(expr.clone());
            }
        }
    }

    fn combine_predicates(&self, predicates: Vec<Expr>) -> Expr {
        predicates
            .into_iter()
            .reduce(|a, b| a.and(b))
            .expect("predicates should not be empty")
    }

    fn can_push_to_scan(&self, expr: &Expr, scan_cols: &[(Option<String>, String)]) -> bool {
        let cols = self.extract_columns(expr);
        self.columns_subset(&cols, scan_cols)
    }

    fn extract_columns(&self, expr: &Expr) -> HashSet<Column> {
        let mut cols = HashSet::new();
        self.extract_columns_recursive(expr, &mut cols);
        cols
    }

    fn extract_columns_recursive(&self, expr: &Expr, cols: &mut HashSet<Column>) {
        match expr {
            Expr::Column(col) => {
                cols.insert(col.clone());
            }
            Expr::BinaryExpr { left, right, .. } => {
                self.extract_columns_recursive(left, cols);
                self.extract_columns_recursive(right, cols);
            }
            Expr::UnaryExpr { expr, .. } => {
                self.extract_columns_recursive(expr, cols);
            }
            Expr::ScalarFunc { args, .. } | Expr::Aggregate { args, .. } => {
                for arg in args {
                    self.extract_columns_recursive(arg, cols);
                }
            }
            Expr::Cast { expr, .. } | Expr::Alias { expr, .. } => {
                self.extract_columns_recursive(expr, cols);
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    self.extract_columns_recursive(op, cols);
                }
                for (w, t) in when_then {
                    self.extract_columns_recursive(w, cols);
                    self.extract_columns_recursive(t, cols);
                }
                if let Some(e) = else_expr {
                    self.extract_columns_recursive(e, cols);
                }
            }
            Expr::InList { expr, list, .. } => {
                self.extract_columns_recursive(expr, cols);
                for item in list {
                    self.extract_columns_recursive(item, cols);
                }
            }
            Expr::Between { expr, low, high, .. } => {
                self.extract_columns_recursive(expr, cols);
                self.extract_columns_recursive(low, cols);
                self.extract_columns_recursive(high, cols);
            }
            _ => {}
        }
    }

    /// Collect columns from schema as (relation, name) pairs for proper qualified matching
    fn collect_columns(&self, schema: &crate::planner::PlanSchema) -> Vec<(Option<String>, String)> {
        schema.fields().iter().map(|f| (f.relation.clone(), f.name.clone())).collect()
    }

    /// Collect columns only from immediate base tables (scans), not from joins
    /// This is used to correctly determine which predicates can be pushed to which side of a join
    fn collect_base_table_columns(&self, plan: &LogicalPlan) -> Vec<(Option<String>, String)> {
        match plan {
            LogicalPlan::Scan(node) => {
                // Scan node - return all its columns
                self.collect_columns(&node.schema)
            }
            LogicalPlan::Filter(node) => {
                // Filter node - recurse to input
                self.collect_base_table_columns(&node.input)
            }
            LogicalPlan::Project(node) => {
                // Project node - recurse to input
                self.collect_base_table_columns(&node.input)
            }
            LogicalPlan::Aggregate(node) => {
                // Aggregate node - recurse to input
                self.collect_base_table_columns(&node.input)
            }
            LogicalPlan::Sort(node) => {
                // Sort node - recurse to input
                self.collect_base_table_columns(&node.input)
            }
            LogicalPlan::Limit(node) => {
                // Limit node - recurse to input
                self.collect_base_table_columns(&node.input)
            }
            LogicalPlan::Distinct(node) => {
                // Distinct node - recurse to input
                self.collect_base_table_columns(&node.input)
            }
            LogicalPlan::SubqueryAlias(node) => {
                // SubqueryAlias node - recurse to input
                self.collect_base_table_columns(&node.input)
            }
            // For Join nodes, recursively collect from BOTH children
            // This gives us the actual base tables on each side, not merged schemas
            LogicalPlan::Join(node) => {
                let mut cols = self.collect_base_table_columns(&node.left);
                cols.extend(self.collect_base_table_columns(&node.right));
                cols
            }
            // For other nodes, return empty - predicates referencing these will remain as "remaining"
            _ => vec![],
        }
    }

    /// Check if a column matches any schema column
    /// - Qualified columns (with relation) must match both relation and name
    /// - Unqualified columns match by name only
    fn column_in_schema(&self, col: &Column, schema_cols: &[(Option<String>, String)]) -> bool {
        match &col.relation {
            Some(rel) => {
                // Qualified column - must match both relation and name
                schema_cols.iter().any(|(r, n)| r.as_ref() == Some(rel) && n == &col.name)
            }
            None => {
                // Unqualified column - match by name only
                schema_cols.iter().any(|(_, n)| n == &col.name)
            }
        }
    }

    fn columns_subset(&self, cols: &HashSet<Column>, target: &[(Option<String>, String)]) -> bool {
        cols.iter().all(|c| self.column_in_schema(c, target))
    }

    fn columns_overlap(&self, cols: &HashSet<Column>, target: &[(Option<String>, String)]) -> bool {
        cols.iter().any(|c| self.column_in_schema(c, target))
    }

    /// Extract join condition from an equality predicate.
    /// Returns (left_expr, right_expr) if the predicate is an equality between
    /// expressions from left and right sides of the join.
    fn extract_join_condition(
        &self,
        pred: &Expr,
        left_cols: &[(Option<String>, String)],
        right_cols: &[(Option<String>, String)],
    ) -> Option<(Expr, Expr)> {
        match pred {
            Expr::BinaryExpr {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                let left_pred_cols = self.extract_columns(left);
                let right_pred_cols = self.extract_columns(right);

                // Check if left side of equality references left table, right side references right table
                let left_from_left = self.columns_subset(&left_pred_cols, left_cols)
                    && !self.columns_overlap(&left_pred_cols, right_cols);
                let right_from_right = self.columns_subset(&right_pred_cols, right_cols)
                    && !self.columns_overlap(&right_pred_cols, left_cols);

                if left_from_left && right_from_right {
                    return Some((left.as_ref().clone(), right.as_ref().clone()));
                }

                // Check the reverse - right side of equality references left table, left side references right table
                let left_from_right = self.columns_subset(&left_pred_cols, right_cols)
                    && !self.columns_overlap(&left_pred_cols, left_cols);
                let right_from_left = self.columns_subset(&right_pred_cols, left_cols)
                    && !self.columns_overlap(&right_pred_cols, right_cols);

                if left_from_right && right_from_left {
                    return Some((right.as_ref().clone(), left.as_ref().clone()));
                }

                None
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{LogicalPlanBuilder, PlanSchema, ScalarValue, SchemaField};
    use arrow::datatypes::DataType;

    fn sample_schema() -> PlanSchema {
        PlanSchema::new(vec![
            SchemaField::new("id", DataType::Int64),
            SchemaField::new("name", DataType::Utf8),
            SchemaField::new("amount", DataType::Float64),
        ])
    }

    #[test]
    fn test_pushdown_to_scan() {
        let rule = PredicatePushdown;

        let plan = LogicalPlanBuilder::scan("orders", sample_schema())
            .filter(Expr::column("amount").gt(Expr::literal(ScalarValue::Float64(100.0.into()))))
            .build();

        let optimized = rule.optimize(&plan).unwrap();

        // Filter should be pushed to scan
        match optimized {
            LogicalPlan::Scan(scan) => {
                assert!(scan.filter.is_some());
            }
            _ => panic!("Expected Scan with pushed filter"),
        }
    }

    #[test]
    fn test_pushdown_through_project() {
        let rule = PredicatePushdown;

        let plan = LogicalPlanBuilder::scan("orders", sample_schema())
            .project(vec![Expr::column("id"), Expr::column("amount")])
            .unwrap()
            .filter(Expr::column("amount").gt(Expr::literal(ScalarValue::Float64(100.0.into()))))
            .build();

        let optimized = rule.optimize(&plan).unwrap();

        // Should be Project -> Scan with filter pushed to scan
        fn find_scan_filter(plan: &LogicalPlan) -> Option<&Expr> {
            match plan {
                LogicalPlan::Scan(scan) => scan.filter.as_ref(),
                LogicalPlan::Project(p) => find_scan_filter(&p.input),
                LogicalPlan::Filter(f) => find_scan_filter(&f.input),
                _ => None,
            }
        }

        assert!(find_scan_filter(&optimized).is_some());
    }
}
