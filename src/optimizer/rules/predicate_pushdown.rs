//! Predicate pushdown optimization rule

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::planner::{BinaryOp, Column, Expr, FilterNode, JoinType, LogicalPlan, ScanNode};
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
                // Only push predicates that reference columns that exist in the input schema
                let input_schema = node.input.schema();
                let input_cols = self.collect_columns(&input_schema);

                // Check if predicates can be pushed (all referenced columns exist in input)
                let (pushable, remaining): (Vec<Expr>, Vec<Expr>) =
                    predicates.into_iter().partition(|p| {
                        let pred_cols = self.extract_columns(p);
                        self.columns_subset(&pred_cols, &input_cols)
                    });

                let result = self.pushdown(&node.input, pushable)?;
                let project = LogicalPlan::Project(crate::planner::ProjectNode {
                    input: Arc::new(result),
                    exprs: node.exprs.clone(),
                    schema: node.schema.clone(),
                });

                // Apply remaining predicates that couldn't be pushed
                if remaining.is_empty() {
                    Ok(project)
                } else {
                    let combined = self.combine_predicates(remaining);
                    Ok(LogicalPlan::Filter(FilterNode {
                        input: Arc::new(project),
                        predicate: combined,
                    }))
                }
            }

            LogicalPlan::Join(node) => {
                let left_cols = self.collect_columns(&node.left.schema());
                let right_cols = self.collect_columns(&node.right.schema());

                let mut left_predicates = Vec::new();
                let mut right_predicates = Vec::new();
                let mut remaining = Vec::new();

                for pred in predicates {
                    // Never push subquery-containing predicates through joins.
                    // They must stay at Filter level for SubqueryDecorrelation to
                    // create Semi/Anti joins at the correct (top) level of the join tree.
                    if pred.contains_subquery() {
                        remaining.push(pred);
                        continue;
                    }

                    let pred_cols = self.extract_columns(&pred);

                    if self.columns_subset(&pred_cols, &left_cols)
                        && !self.columns_overlap(&pred_cols, &right_cols)
                    {
                        left_predicates.push(pred);
                    } else if self.columns_subset(&pred_cols, &right_cols)
                        && !self.columns_overlap(&pred_cols, &left_cols)
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
                        // Extract equality predicates that span both sides as join conditions
                        let mut join_conditions: Vec<(Expr, Expr)> = node.on.clone();
                        let mut non_join_remaining = Vec::new();

                        for pred in &remaining {
                            if let Some((left_expr, right_expr)) =
                                self.extract_join_condition(pred, &left_cols, &right_cols)
                            {
                                join_conditions.push((left_expr, right_expr));
                            } else {
                                non_join_remaining.push(pred.clone());
                            }
                        }

                        let left = self.pushdown(&node.left, left_predicates)?;
                        let right = self.pushdown(&node.right, right_predicates)?;

                        // Convert Cross join to Inner join if we have join conditions
                        let new_join_type =
                            if node.join_type == JoinType::Cross && !join_conditions.is_empty() {
                                JoinType::Inner
                            } else {
                                node.join_type
                            };

                        let join = LogicalPlan::Join(crate::planner::JoinNode {
                            left: Arc::new(left),
                            right: Arc::new(right),
                            join_type: new_join_type,
                            on: join_conditions,
                            filter: node.filter.clone(),
                            schema: node.schema.clone(),
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
                    JoinType::Left => {
                        // Can only push to left side
                        let left = self.pushdown(&node.left, left_predicates)?;
                        let right = self.pushdown(&node.right, vec![])?;
                        remaining.extend(right_predicates);

                        let join = LogicalPlan::Join(crate::planner::JoinNode {
                            left: Arc::new(left),
                            right: Arc::new(right),
                            join_type: node.join_type,
                            on: node.on.clone(),
                            filter: node.filter.clone(),
                            schema: node.schema.clone(),
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

                        let join = LogicalPlan::Join(crate::planner::JoinNode {
                            left: Arc::new(left),
                            right: Arc::new(right),
                            join_type: node.join_type,
                            on: node.on.clone(),
                            filter: node.filter.clone(),
                            schema: node.schema.clone(),
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

                        let join = LogicalPlan::Join(crate::planner::JoinNode {
                            left: Arc::new(left),
                            right: Arc::new(right),
                            join_type: node.join_type,
                            on: node.on.clone(),
                            filter: node.filter.clone(),
                            schema: node.schema.clone(),
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
                Ok(LogicalPlan::SubqueryAlias(
                    crate::planner::SubqueryAliasNode {
                        input: Arc::new(input),
                        alias: node.alias.clone(),
                        schema: node.schema.clone(),
                    },
                ))
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
                    all: node.all,
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

            LogicalPlan::DelimJoin(node) => {
                // Don't push predicates into DelimJoin - handle like a regular join
                let left = self.pushdown(&node.left, vec![])?;
                let right = self.pushdown(&node.right, vec![])?;
                let plan = LogicalPlan::DelimJoin(crate::planner::DelimJoinNode {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    join_type: node.join_type,
                    delim_columns: node.delim_columns.clone(),
                    on: node.on.clone(),
                    schema: node.schema.clone(),
                });
                if predicates.is_empty() {
                    Ok(plan)
                } else {
                    let combined = self.combine_predicates(predicates);
                    Ok(LogicalPlan::Filter(FilterNode {
                        input: Arc::new(plan),
                        predicate: combined,
                    }))
                }
            }

            LogicalPlan::DelimGet(node) => {
                // DelimGet is a leaf node - can't push predicates into it
                if predicates.is_empty() {
                    Ok(plan.clone())
                } else {
                    let combined = self.combine_predicates(predicates);
                    Ok(LogicalPlan::Filter(FilterNode {
                        input: Arc::new(LogicalPlan::DelimGet(node.clone())),
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
                // Try to extract common factors from OR expressions
                // e.g. (A AND X) OR (B AND X) OR (C AND X) → X AND (A OR B OR C)
                if let Some(factored) = self.extract_common_or_factors(expr) {
                    for p in factored {
                        self.split_conjunction(&p, predicates);
                    }
                } else {
                    predicates.push(expr.clone());
                }
            }
        }
    }

    /// Extract common factors from OR expressions.
    /// Transforms `(A AND B) OR (A AND C)` into `[A, (B OR C)]`.
    /// Returns None if no common factors found or expression is not an OR.
    fn extract_common_or_factors(&self, expr: &Expr) -> Option<Vec<Expr>> {
        // Collect OR branches
        let mut branches = Vec::new();
        self.collect_or_branches(expr, &mut branches);
        if branches.len() < 2 {
            return None;
        }

        // For each branch, split into AND conditions
        let branch_conditions: Vec<Vec<Expr>> = branches
            .iter()
            .map(|b| {
                let mut conds = Vec::new();
                self.split_conjunction_into(b, &mut conds);
                conds
            })
            .collect();

        // Find conditions from the first branch that appear in ALL other branches
        let mut common = Vec::new();
        let mut first_remaining = Vec::new();

        for cond in &branch_conditions[0] {
            let cond_str = format!("{:?}", cond);
            let in_all = branch_conditions[1..]
                .iter()
                .all(|branch| branch.iter().any(|c| format!("{:?}", c) == cond_str));
            if in_all {
                common.push(cond.clone());
            } else {
                first_remaining.push(cond.clone());
            }
        }

        if common.is_empty() {
            return None;
        }

        // Build remaining OR branches (remove common conditions from each)
        let mut remaining_branches = Vec::new();
        remaining_branches.push(first_remaining);

        for branch in &branch_conditions[1..] {
            let remaining: Vec<Expr> = branch
                .iter()
                .filter(|c| {
                    let c_str = format!("{:?}", c);
                    !common
                        .iter()
                        .any(|common_c| format!("{:?}", common_c) == c_str)
                })
                .cloned()
                .collect();
            remaining_branches.push(remaining);
        }

        // Build result: common conditions + simplified OR
        let mut result = common;

        // Only add the OR if branches have remaining conditions
        let non_empty_branches: Vec<Expr> = remaining_branches
            .into_iter()
            .filter_map(|branch| {
                if branch.is_empty() {
                    None
                } else {
                    Some(self.combine_predicates(branch))
                }
            })
            .collect();

        if !non_empty_branches.is_empty() && non_empty_branches.len() >= 2 {
            let or_expr = non_empty_branches
                .into_iter()
                .reduce(|a, b| Expr::BinaryExpr {
                    left: Box::new(a),
                    op: BinaryOp::Or,
                    right: Box::new(b),
                })
                .unwrap();
            result.push(or_expr);
        } else if non_empty_branches.len() == 1 {
            result.push(non_empty_branches.into_iter().next().unwrap());
        }
        // If all branches become empty, common conditions alone suffice

        Some(result)
    }

    fn collect_or_branches(&self, expr: &Expr, branches: &mut Vec<Expr>) {
        match expr {
            Expr::BinaryExpr {
                left,
                op: BinaryOp::Or,
                right,
            } => {
                self.collect_or_branches(left, branches);
                self.collect_or_branches(right, branches);
            }
            _ => {
                branches.push(expr.clone());
            }
        }
    }

    fn split_conjunction_into(&self, expr: &Expr, conditions: &mut Vec<Expr>) {
        match expr {
            Expr::BinaryExpr {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.split_conjunction_into(left, conditions);
                self.split_conjunction_into(right, conditions);
            }
            _ => {
                conditions.push(expr.clone());
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
        // Never push predicates containing subqueries to scans
        // They need to remain in Filter nodes for SubqueryDecorrelation to process
        if expr.contains_subquery() {
            return false;
        }
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
            Expr::Between {
                expr, low, high, ..
            } => {
                self.extract_columns_recursive(expr, cols);
                self.extract_columns_recursive(low, cols);
                self.extract_columns_recursive(high, cols);
            }
            // For subquery expressions, we need to extract outer column references
            // to prevent them from being incorrectly pushed down to individual scans
            Expr::Exists { subquery, .. } | Expr::ScalarSubquery(subquery) => {
                // Extract outer references from the subquery
                self.extract_outer_columns_from_subquery(subquery, cols);
            }
            Expr::InSubquery { expr, subquery, .. } => {
                self.extract_columns_recursive(expr, cols);
                self.extract_outer_columns_from_subquery(subquery, cols);
            }
            _ => {}
        }
    }

    /// Extract outer column references from a subquery
    /// These are columns that reference tables not defined in the subquery
    fn extract_outer_columns_from_subquery(
        &self,
        subquery: &LogicalPlan,
        cols: &mut HashSet<Column>,
    ) {
        let local_tables = self.collect_subquery_tables(subquery);
        self.extract_outer_refs_recursive(subquery, &local_tables, cols);
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
            }
            LogicalPlan::Join(node) => {
                self.collect_tables_recursive(&node.left, tables);
                self.collect_tables_recursive(&node.right, tables);
            }
            LogicalPlan::Filter(node) => self.collect_tables_recursive(&node.input, tables),
            LogicalPlan::Project(node) => self.collect_tables_recursive(&node.input, tables),
            LogicalPlan::Aggregate(node) => self.collect_tables_recursive(&node.input, tables),
            LogicalPlan::Sort(node) => self.collect_tables_recursive(&node.input, tables),
            LogicalPlan::Limit(node) => self.collect_tables_recursive(&node.input, tables),
            LogicalPlan::Distinct(node) => self.collect_tables_recursive(&node.input, tables),
            _ => {}
        }
    }

    /// Extract columns from a plan that reference tables NOT in local_tables
    fn extract_outer_refs_recursive(
        &self,
        plan: &LogicalPlan,
        local_tables: &HashSet<String>,
        cols: &mut HashSet<Column>,
    ) {
        match plan {
            LogicalPlan::Filter(node) => {
                self.extract_outer_refs_from_expr(&node.predicate, local_tables, cols);
                self.extract_outer_refs_recursive(&node.input, local_tables, cols);
            }
            LogicalPlan::Project(node) => {
                for expr in &node.exprs {
                    self.extract_outer_refs_from_expr(expr, local_tables, cols);
                }
                self.extract_outer_refs_recursive(&node.input, local_tables, cols);
            }
            LogicalPlan::Join(node) => {
                for (l, r) in &node.on {
                    self.extract_outer_refs_from_expr(l, local_tables, cols);
                    self.extract_outer_refs_from_expr(r, local_tables, cols);
                }
                if let Some(f) = &node.filter {
                    self.extract_outer_refs_from_expr(f, local_tables, cols);
                }
                self.extract_outer_refs_recursive(&node.left, local_tables, cols);
                self.extract_outer_refs_recursive(&node.right, local_tables, cols);
            }
            LogicalPlan::SubqueryAlias(node) => {
                self.extract_outer_refs_recursive(&node.input, local_tables, cols);
            }
            _ => {}
        }
    }

    /// Extract columns from an expression that reference tables NOT in local_tables
    fn extract_outer_refs_from_expr(
        &self,
        expr: &Expr,
        local_tables: &HashSet<String>,
        cols: &mut HashSet<Column>,
    ) {
        match expr {
            Expr::Column(col) => {
                // Only add if this column references an outer table
                if let Some(rel) = &col.relation {
                    if !local_tables.contains(rel) {
                        cols.insert(col.clone());
                    }
                }
                // For unqualified columns, assume they might be outer references
                // (conservative approach - prevents incorrect pushdown)
                else {
                    cols.insert(col.clone());
                }
            }
            Expr::BinaryExpr { left, right, .. } => {
                self.extract_outer_refs_from_expr(left, local_tables, cols);
                self.extract_outer_refs_from_expr(right, local_tables, cols);
            }
            Expr::UnaryExpr { expr, .. } => {
                self.extract_outer_refs_from_expr(expr, local_tables, cols);
            }
            Expr::ScalarFunc { args, .. } | Expr::Aggregate { args, .. } => {
                for arg in args {
                    self.extract_outer_refs_from_expr(arg, local_tables, cols);
                }
            }
            Expr::Cast { expr, .. } | Expr::Alias { expr, .. } => {
                self.extract_outer_refs_from_expr(expr, local_tables, cols);
            }
            _ => {}
        }
    }

    /// Collect columns from schema as (relation, name) pairs for proper qualified matching
    fn collect_columns(
        &self,
        schema: &crate::planner::PlanSchema,
    ) -> Vec<(Option<String>, String)> {
        schema
            .fields()
            .iter()
            .map(|f| (f.relation.clone(), f.name.clone()))
            .collect()
    }

    /// Check if a column matches any schema column
    /// - Qualified columns (with relation) must match both relation and name
    /// - Unqualified columns match by name only
    fn column_in_schema(&self, col: &Column, schema_cols: &[(Option<String>, String)]) -> bool {
        match &col.relation {
            Some(rel) => {
                // Qualified column - must match both relation and name
                schema_cols
                    .iter()
                    .any(|(r, n)| r.as_ref() == Some(rel) && n == &col.name)
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
