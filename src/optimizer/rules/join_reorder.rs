//! Join reordering optimization rule
//!
//! This rule reorders joins to avoid Cartesian products by ensuring every join
//! has at least one equality condition. It builds a join graph and finds an
//! ordering that minimizes intermediate result sizes.

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::planner::{BinaryOp, Expr, JoinNode, JoinType, LogicalPlan};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Join reordering optimization rule
pub struct JoinReorder;

impl OptimizerRule for JoinReorder {
    fn name(&self) -> &str {
        "JoinReorder"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.reorder(plan)
    }
}

/// Represents a table/scan in the join graph
#[derive(Debug, Clone)]
struct JoinRelation {
    plan: LogicalPlan,
    /// Table name or alias for identification
    name: String,
    /// All column names available from this relation
    columns: HashSet<String>,
}

/// Represents a join edge in the graph
#[derive(Debug, Clone)]
struct JoinEdge {
    /// Indices of the two relations being joined
    left_idx: usize,
    right_idx: usize,
    /// The equality conditions for this join
    conditions: Vec<(Expr, Expr)>,
}

impl JoinReorder {
    fn reorder(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(node) => {
                // Check if input contains joins that need reordering
                // If so, we need to pass filter predicates down to help with join ordering
                if self.needs_reordering(&node.input) {
                    return self.reorder_filter_with_join(node);
                }
                let input = self.reorder(&node.input)?;
                Ok(LogicalPlan::Filter(crate::planner::FilterNode {
                    input: Arc::new(input),
                    predicate: node.predicate.clone(),
                }))
            }

            LogicalPlan::Project(node) => {
                let input = self.reorder(&node.input)?;
                Ok(LogicalPlan::Project(crate::planner::ProjectNode {
                    input: Arc::new(input),
                    exprs: node.exprs.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Join(node) => {
                // Check if this is a join tree that needs reordering
                // Only reorder if we have cross joins or inner joins without proper conditions
                if self.needs_reordering(plan) {
                    self.reorder_join_tree(plan)
                } else {
                    // Recursively optimize children
                    let left = self.reorder(&node.left)?;
                    let right = self.reorder(&node.right)?;
                    Ok(LogicalPlan::Join(JoinNode {
                        left: Arc::new(left),
                        right: Arc::new(right),
                        join_type: node.join_type,
                        on: node.on.clone(),
                        filter: node.filter.clone(),
                        schema: node.schema.clone(),
                    }))
                }
            }

            LogicalPlan::Aggregate(node) => {
                let input = self.reorder(&node.input)?;
                Ok(LogicalPlan::Aggregate(crate::planner::AggregateNode {
                    input: Arc::new(input),
                    group_by: node.group_by.clone(),
                    aggregates: node.aggregates.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Sort(node) => {
                let input = self.reorder(&node.input)?;
                Ok(LogicalPlan::Sort(crate::planner::SortNode {
                    input: Arc::new(input),
                    order_by: node.order_by.clone(),
                }))
            }

            LogicalPlan::Limit(node) => {
                let input = self.reorder(&node.input)?;
                Ok(LogicalPlan::Limit(crate::planner::LimitNode {
                    input: Arc::new(input),
                    skip: node.skip,
                    fetch: node.fetch,
                }))
            }

            LogicalPlan::Distinct(node) => {
                let input = self.reorder(&node.input)?;
                Ok(LogicalPlan::Distinct(crate::planner::DistinctNode {
                    input: Arc::new(input),
                }))
            }

            LogicalPlan::SubqueryAlias(node) => {
                let input = self.reorder(&node.input)?;
                Ok(LogicalPlan::SubqueryAlias(
                    crate::planner::SubqueryAliasNode {
                        input: Arc::new(input),
                        alias: node.alias.clone(),
                        schema: node.schema.clone(),
                    },
                ))
            }

            LogicalPlan::Union(node) => {
                let inputs = node
                    .inputs
                    .iter()
                    .map(|input| self.reorder(input).map(Arc::new))
                    .collect::<Result<Vec<_>>>()?;
                Ok(LogicalPlan::Union(crate::planner::UnionNode {
                    inputs,
                    schema: node.schema.clone(),
                    all: node.all,
                }))
            }

            // Leaf nodes - no reordering needed
            LogicalPlan::Scan(_) | LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => {
                Ok(plan.clone())
            }

            // DelimJoin/DelimGet - recursively optimize children but don't reorder
            LogicalPlan::DelimJoin(node) => {
                let left = self.reorder(&node.left)?;
                let right = self.reorder(&node.right)?;
                Ok(LogicalPlan::DelimJoin(crate::planner::DelimJoinNode {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    join_type: node.join_type,
                    delim_columns: node.delim_columns.clone(),
                    on: node.on.clone(),
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::DelimGet(node) => Ok(LogicalPlan::DelimGet(node.clone())),
        }
    }

    /// Check if this join tree needs reordering (has cross joins)
    fn needs_reordering(&self, plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Join(node) => {
                // If this is a cross join, it needs reordering
                if node.join_type == JoinType::Cross {
                    return true;
                }
                // Also check children
                self.needs_reordering(&node.left) || self.needs_reordering(&node.right)
            }
            _ => false,
        }
    }

    /// Handle Filter node when its input contains joins needing reorder
    /// This includes filter predicates in the join reordering process
    fn reorder_filter_with_join(&self, filter: &crate::planner::FilterNode) -> Result<LogicalPlan> {
        // Collect all relations and conditions from the join tree
        let mut relations: Vec<JoinRelation> = Vec::new();
        let mut all_conditions: Vec<(Expr, Expr)> = Vec::new();

        // First, extract join conditions from the filter predicate
        self.extract_join_conditions(&filter.predicate, &mut all_conditions);

        // Then collect from the join tree
        self.collect_relations_and_conditions(&filter.input, &mut relations, &mut all_conditions);

        if relations.len() <= 1 {
            // Not a multi-table join, just recurse normally
            let input = self.reorder(&filter.input)?;
            return Ok(LogicalPlan::Filter(crate::planner::FilterNode {
                input: Arc::new(input),
                predicate: filter.predicate.clone(),
            }));
        }

        // Build the optimized join tree using all conditions (including filter predicates)
        let (join_result, used_conditions) =
            self.build_optimized_join_tree(&relations, &all_conditions)?;

        // Rebuild the filter with remaining (non-join) predicates
        let remaining_filter =
            self.rebuild_filter_without_join_conditions(&filter.predicate, &used_conditions);

        if let Some(remaining) = remaining_filter {
            Ok(LogicalPlan::Filter(crate::planner::FilterNode {
                input: Arc::new(join_result),
                predicate: remaining,
            }))
        } else {
            Ok(join_result)
        }
    }

    /// Build an optimized join tree and return which conditions were used as join conditions
    fn build_optimized_join_tree(
        &self,
        relations: &[JoinRelation],
        conditions: &[(Expr, Expr)],
    ) -> Result<(LogicalPlan, HashSet<usize>)> {
        // Build column to relation mapping
        let mut column_to_relation: HashMap<String, Vec<usize>> = HashMap::new();
        for (idx, rel) in relations.iter().enumerate() {
            for col in &rel.columns {
                column_to_relation.entry(col.clone()).or_default().push(idx);
                let qualified = format!("{}.{}", rel.name, col);
                column_to_relation.entry(qualified).or_default().push(idx);
            }
        }

        // Build join edges from conditions
        let mut edges: Vec<JoinEdge> = Vec::new();
        let mut used_condition_indices: HashSet<usize> = HashSet::new();

        for (cond_idx, (left_expr, right_expr)) in conditions.iter().enumerate() {
            let left_cols = self.extract_columns(left_expr);
            let right_cols = self.extract_columns(right_expr);

            let left_rels = self.find_relations(&left_cols, &column_to_relation);
            let right_rels = self.find_relations(&right_cols, &column_to_relation);

            if left_rels.len() == 1 && right_rels.len() == 1 {
                let left_idx = left_rels[0];
                let right_idx = right_rels[0];

                if left_idx != right_idx {
                    used_condition_indices.insert(cond_idx);

                    let existing = edges.iter_mut().find(|e| {
                        (e.left_idx == left_idx && e.right_idx == right_idx)
                            || (e.left_idx == right_idx && e.right_idx == left_idx)
                    });

                    if let Some(edge) = existing {
                        if edge.left_idx == left_idx {
                            edge.conditions
                                .push((left_expr.clone(), right_expr.clone()));
                        } else {
                            edge.conditions
                                .push((right_expr.clone(), left_expr.clone()));
                        }
                    } else {
                        edges.push(JoinEdge {
                            left_idx,
                            right_idx,
                            conditions: vec![(left_expr.clone(), right_expr.clone())],
                        });
                    }
                }
            }
        }

        // Greedy join ordering
        let mut joined: HashSet<usize> = HashSet::new();
        let mut result_plan: Option<LogicalPlan> = None;
        let mut used_edges: HashSet<usize> = HashSet::new();

        let start_idx = self.select_start_relation(relations, &edges);
        joined.insert(start_idx);
        result_plan = Some(relations[start_idx].plan.clone());

        while joined.len() < relations.len() {
            let mut best_edge: Option<(usize, usize)> = None;
            let mut best_score: i32 = i32::MIN;

            for (edge_idx, edge) in edges.iter().enumerate() {
                if used_edges.contains(&edge_idx) {
                    continue;
                }

                let left_in = joined.contains(&edge.left_idx);
                let right_in = joined.contains(&edge.right_idx);

                if left_in && !right_in {
                    // Score based on: prefer small tables (dimension tables) to join early
                    // This reduces intermediate result sizes
                    let size_score = self.estimate_relation_size_score(&relations[edge.right_idx]);
                    let cond_score = edge.conditions.len() as i32 * 100;
                    let score = size_score + cond_score;
                    if score > best_score {
                        best_score = score;
                        best_edge = Some((edge_idx, edge.right_idx));
                    }
                } else if !left_in && right_in {
                    // Score based on: prefer small tables (dimension tables) to join early
                    let size_score = self.estimate_relation_size_score(&relations[edge.left_idx]);
                    let cond_score = edge.conditions.len() as i32 * 100;
                    let score = size_score + cond_score;
                    if score > best_score {
                        best_score = score;
                        best_edge = Some((edge_idx, edge.left_idx));
                    }
                }
            }

            if let Some((edge_idx, new_rel_idx)) = best_edge {
                let edge = &edges[edge_idx];
                used_edges.insert(edge_idx);
                joined.insert(new_rel_idx);

                let current = result_plan.take().unwrap();
                let new_rel = relations[new_rel_idx].plan.clone();

                let (left, right, on) = if edge.left_idx == new_rel_idx {
                    (new_rel, current, edge.conditions.clone())
                } else {
                    (current, new_rel, edge.conditions.clone())
                };

                let mut schema_fields = left.schema().fields().to_vec();
                schema_fields.extend(right.schema().fields().iter().cloned());
                let schema = crate::planner::PlanSchema::new(schema_fields);

                result_plan = Some(LogicalPlan::Join(JoinNode {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    join_type: JoinType::Inner,
                    on,
                    filter: None,
                    schema,
                }));

                // Check for additional edges from the newly joined relation to already-joined relations
                // These need to be added as filters
                for (other_edge_idx, other_edge) in edges.iter().enumerate() {
                    if used_edges.contains(&other_edge_idx) {
                        continue;
                    }

                    let connects_new =
                        other_edge.left_idx == new_rel_idx || other_edge.right_idx == new_rel_idx;
                    let other_side = if other_edge.left_idx == new_rel_idx {
                        other_edge.right_idx
                    } else {
                        other_edge.left_idx
                    };
                    let other_in_result = joined.contains(&other_side);

                    if connects_new && other_in_result {
                        // This edge adds more conditions between already-joined relations
                        // Add as filter on top
                        used_edges.insert(other_edge_idx);
                        used_condition_indices.insert(other_edge_idx);
                        let conditions = &other_edge.conditions;
                        for (l, r) in conditions {
                            let filter_expr = Expr::BinaryExpr {
                                left: Box::new(l.clone()),
                                op: BinaryOp::Eq,
                                right: Box::new(r.clone()),
                            };
                            result_plan = Some(LogicalPlan::Filter(crate::planner::FilterNode {
                                input: Arc::new(result_plan.take().unwrap()),
                                predicate: filter_expr,
                            }));
                        }
                    }
                }
            } else {
                // No edge found - need cross join (shouldn't happen if conditions exist)
                let next_rel = (0..relations.len()).find(|i| !joined.contains(i)).unwrap();
                joined.insert(next_rel);

                let current = result_plan.take().unwrap();
                let new_rel = relations[next_rel].plan.clone();

                let mut schema_fields = current.schema().fields().to_vec();
                schema_fields.extend(new_rel.schema().fields().iter().cloned());
                let schema = crate::planner::PlanSchema::new(schema_fields);

                result_plan = Some(LogicalPlan::Join(JoinNode {
                    left: Arc::new(current),
                    right: Arc::new(new_rel),
                    join_type: JoinType::Cross,
                    on: vec![],
                    filter: None,
                    schema,
                }));
            }
        }

        Ok((result_plan.unwrap(), used_condition_indices))
    }

    /// Rebuild filter predicate without the conditions used as join conditions
    fn rebuild_filter_without_join_conditions(
        &self,
        predicate: &Expr,
        used_indices: &HashSet<usize>,
    ) -> Option<Expr> {
        let mut remaining = Vec::new();
        let mut idx = 0;
        self.collect_non_join_predicates(predicate, used_indices, &mut idx, &mut remaining);

        if remaining.is_empty() {
            None
        } else {
            Some(
                remaining
                    .into_iter()
                    .reduce(|acc, p| Expr::BinaryExpr {
                        left: Box::new(acc),
                        op: BinaryOp::And,
                        right: Box::new(p),
                    })
                    .unwrap(),
            )
        }
    }

    /// Collect predicates that weren't used as join conditions
    fn collect_non_join_predicates(
        &self,
        expr: &Expr,
        used_indices: &HashSet<usize>,
        current_idx: &mut usize,
        result: &mut Vec<Expr>,
    ) {
        match expr {
            Expr::BinaryExpr {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.collect_non_join_predicates(left, used_indices, current_idx, result);
                self.collect_non_join_predicates(right, used_indices, current_idx, result);
            }
            Expr::BinaryExpr {
                op: BinaryOp::Eq, ..
            } => {
                // This is a potential join condition
                if !used_indices.contains(current_idx) {
                    result.push(expr.clone());
                }
                *current_idx += 1;
            }
            _ => {
                // Non-equality predicates are never join conditions
                result.push(expr.clone());
            }
        }
    }

    /// Reorder a join tree to minimize Cartesian products
    fn reorder_join_tree(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // Step 1: Collect all base relations and join conditions
        let mut relations: Vec<JoinRelation> = Vec::new();
        let mut all_conditions: Vec<(Expr, Expr)> = Vec::new();

        self.collect_relations_and_conditions(plan, &mut relations, &mut all_conditions);

        if relations.len() <= 1 {
            return Ok(plan.clone());
        }

        // Step 2: Build a mapping from column names to relation indices
        // Include both unqualified (col_name) and qualified (table.col_name) mappings
        let mut column_to_relation: HashMap<String, Vec<usize>> = HashMap::new();
        for (idx, rel) in relations.iter().enumerate() {
            for col in &rel.columns {
                // Unqualified column name
                column_to_relation.entry(col.clone()).or_default().push(idx);
                // Qualified column name (table.column)
                let qualified = format!("{}.{}", rel.name, col);
                column_to_relation.entry(qualified).or_default().push(idx);
            }
        }

        // Step 3: Build join edges from conditions
        let mut edges: Vec<JoinEdge> = Vec::new();
        let mut remaining_conditions: Vec<(Expr, Expr)> = Vec::new();

        for (left_expr, right_expr) in &all_conditions {
            let left_cols = self.extract_columns(left_expr);
            let right_cols = self.extract_columns(right_expr);

            let left_rels = self.find_relations(&left_cols, &column_to_relation);
            let right_rels = self.find_relations(&right_cols, &column_to_relation);

            if left_rels.len() == 1 && right_rels.len() == 1 {
                let left_idx = left_rels[0];
                let right_idx = right_rels[0];

                if left_idx != right_idx {
                    // This condition connects two different relations
                    // Check if we already have an edge for this pair
                    let existing = edges.iter_mut().find(|e| {
                        (e.left_idx == left_idx && e.right_idx == right_idx)
                            || (e.left_idx == right_idx && e.right_idx == left_idx)
                    });

                    if let Some(edge) = existing {
                        if edge.left_idx == left_idx {
                            edge.conditions
                                .push((left_expr.clone(), right_expr.clone()));
                        } else {
                            edge.conditions
                                .push((right_expr.clone(), left_expr.clone()));
                        }
                    } else {
                        edges.push(JoinEdge {
                            left_idx,
                            right_idx,
                            conditions: vec![(left_expr.clone(), right_expr.clone())],
                        });
                    }
                } else {
                    // Same relation - this is a filter condition, keep it
                    remaining_conditions.push((left_expr.clone(), right_expr.clone()));
                }
            } else {
                // Condition spans more than 2 relations or none - keep for later
                remaining_conditions.push((left_expr.clone(), right_expr.clone()));
            }
        }

        // Step 4: Greedy join ordering - always pick a join that connects to the current result
        let mut joined: HashSet<usize> = HashSet::new();
        let mut result_plan: Option<LogicalPlan> = None;
        let mut result_columns: HashSet<String> = HashSet::new();
        let mut used_edges: HashSet<usize> = HashSet::new();

        // Start selection: prefer relations with filters (selective), then most connected
        // This is a simple heuristic that works well for star/snowflake schemas
        let start_idx = self.select_start_relation(&relations, &edges);

        joined.insert(start_idx);
        result_plan = Some(relations[start_idx].plan.clone());
        result_columns = relations[start_idx].columns.clone();

        // Keep joining until all relations are included
        while joined.len() < relations.len() {
            // Find the best edge to use (connects to current result, with join condition)
            let mut best_edge: Option<(usize, usize)> = None; // (edge_idx, new_relation_idx)
            let mut best_score: i32 = i32::MIN;

            for (edge_idx, edge) in edges.iter().enumerate() {
                if used_edges.contains(&edge_idx) {
                    continue;
                }

                let left_in = joined.contains(&edge.left_idx);
                let right_in = joined.contains(&edge.right_idx);

                if left_in && !right_in {
                    // Can add right relation
                    let base_score = edge.conditions.len() as i32 * 100;
                    let size_score = self.estimate_relation_size_score(&relations[edge.right_idx]);
                    let score = base_score + size_score;
                    if score > best_score {
                        best_score = score;
                        best_edge = Some((edge_idx, edge.right_idx));
                    }
                } else if !left_in && right_in {
                    // Can add left relation
                    let base_score = edge.conditions.len() as i32 * 100;
                    let size_score = self.estimate_relation_size_score(&relations[edge.left_idx]);
                    let score = base_score + size_score;
                    if score > best_score {
                        best_score = score;
                        best_edge = Some((edge_idx, edge.left_idx));
                    }
                }
            }

            if let Some((edge_idx, new_idx)) = best_edge {
                // Add this relation with join conditions
                let edge = &edges[edge_idx];
                used_edges.insert(edge_idx);
                joined.insert(new_idx);

                let new_rel = &relations[new_idx];
                let current = result_plan.take().unwrap();

                // Determine which side of the condition goes where
                let base_conditions = if edge.right_idx == new_idx {
                    edge.conditions.clone()
                } else {
                    // Swap left/right in conditions
                    edge.conditions
                        .iter()
                        .map(|(l, r)| (r.clone(), l.clone()))
                        .collect()
                };

                // Hash join optimization: smaller table should be BUILD (left) side
                // New relation is likely smaller (dimension table), so put it as BUILD
                // This means: new_rel (left/build) JOIN current (right/probe)
                // Swap the conditions to match the new order
                let conditions: Vec<(Expr, Expr)> = base_conditions
                    .iter()
                    .map(|(l, r)| (r.clone(), l.clone()))
                    .collect();

                // Build the combined schema with new relation first
                let new_schema = new_rel.plan.schema();
                let current_schema = current.schema();
                let combined_schema = new_schema.merge(&current_schema);

                result_plan = Some(LogicalPlan::Join(JoinNode {
                    left: Arc::new(new_rel.plan.clone()), // New (smaller) as build
                    right: Arc::new(current),             // Current (larger) as probe
                    join_type: JoinType::Inner,
                    on: conditions,
                    filter: None,
                    schema: combined_schema,
                }));

                result_columns.extend(new_rel.columns.iter().cloned());

                // Check if there are additional edges from the new relation to existing relations
                for (other_edge_idx, other_edge) in edges.iter().enumerate() {
                    if used_edges.contains(&other_edge_idx) {
                        continue;
                    }

                    let connects_new =
                        other_edge.left_idx == new_idx || other_edge.right_idx == new_idx;
                    let other_side = if other_edge.left_idx == new_idx {
                        other_edge.right_idx
                    } else {
                        other_edge.left_idx
                    };
                    let other_in_result = joined.contains(&other_side);

                    if connects_new && other_in_result {
                        // This edge adds more conditions between already-joined relations
                        // Add as filter on top
                        used_edges.insert(other_edge_idx);
                        let conditions = &other_edge.conditions;
                        for (l, r) in conditions {
                            let filter_expr = Expr::BinaryExpr {
                                left: Box::new(l.clone()),
                                op: BinaryOp::Eq,
                                right: Box::new(r.clone()),
                            };
                            result_plan = Some(LogicalPlan::Filter(crate::planner::FilterNode {
                                input: Arc::new(result_plan.take().unwrap()),
                                predicate: filter_expr,
                            }));
                        }
                    }
                }
            } else {
                // No edge found - need to do a cross join with an unjoined relation
                // Pick the smallest unjoined relation (heuristic)
                let next_idx = (0..relations.len())
                    .filter(|i| !joined.contains(i))
                    .next()
                    .unwrap();

                joined.insert(next_idx);
                let new_rel = &relations[next_idx];
                let current = result_plan.take().unwrap();

                let current_schema = current.schema();
                let new_schema = new_rel.plan.schema();
                let combined_schema = current_schema.merge(&new_schema);

                result_plan = Some(LogicalPlan::Join(JoinNode {
                    left: Arc::new(current),
                    right: Arc::new(new_rel.plan.clone()),
                    join_type: JoinType::Cross,
                    on: vec![],
                    filter: None,
                    schema: combined_schema,
                }));

                result_columns.extend(new_rel.columns.iter().cloned());
            }
        }

        Ok(result_plan.unwrap())
    }

    /// Collect all base relations and join conditions from a join tree
    fn collect_relations_and_conditions(
        &self,
        plan: &LogicalPlan,
        relations: &mut Vec<JoinRelation>,
        conditions: &mut Vec<(Expr, Expr)>,
    ) {
        match plan {
            LogicalPlan::Join(node) => {
                // Only flatten Cross and Inner joins - other join types have specific semantics
                // and shouldn't be reordered (e.g., LeftJoin from subquery decorrelation)
                if node.join_type == JoinType::Cross || node.join_type == JoinType::Inner {
                    // Collect conditions from this join
                    conditions.extend(node.on.iter().cloned());

                    // Recursively collect from children
                    self.collect_relations_and_conditions(&node.left, relations, conditions);
                    self.collect_relations_and_conditions(&node.right, relations, conditions);
                } else {
                    // For LeftJoin/RightJoin/Semi/Anti, we still need to process the left child
                    // to find any Cross joins there, but NOT include the join's ON conditions
                    // in the reorder pool (those are for the outer join semantics)
                    //
                    // Check if left child has Cross joins that need reordering
                    if self.needs_reordering(&node.left) {
                        // Collect from left child only - we'll keep this join structure
                        self.collect_relations_and_conditions(&node.left, relations, conditions);
                        // Right child becomes an opaque relation
                        let right_schema = node.right.schema();
                        let right_columns: HashSet<String> = right_schema
                            .fields()
                            .iter()
                            .map(|f| f.name.clone())
                            .collect();
                        relations.push(JoinRelation {
                            plan: (*node.right).clone(),
                            name: format!("{:?}_right", node.join_type),
                            columns: right_columns,
                        });
                    } else {
                        // Treat the whole join as an opaque relation
                        let schema = plan.schema();
                        let columns: HashSet<String> =
                            schema.fields().iter().map(|f| f.name.clone()).collect();
                        relations.push(JoinRelation {
                            plan: plan.clone(),
                            name: format!("{:?}_join", node.join_type),
                            columns,
                        });
                    }
                }
            }

            LogicalPlan::Scan(node) => {
                let columns: HashSet<String> = node
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name.clone())
                    .collect();

                relations.push(JoinRelation {
                    plan: plan.clone(),
                    name: node.table_name.clone(),
                    columns,
                });
            }

            LogicalPlan::SubqueryAlias(node) => {
                let columns: HashSet<String> = node
                    .schema
                    .fields()
                    .iter()
                    .map(|f| f.name.clone())
                    .collect();

                relations.push(JoinRelation {
                    plan: plan.clone(),
                    name: node.alias.clone(),
                    columns,
                });
            }

            LogicalPlan::Filter(node) => {
                // For filters, we need to keep them attached to their input
                // But also extract any equality conditions that might be join conditions
                self.extract_join_conditions(&node.predicate, conditions);

                // Collect from input
                self.collect_relations_and_conditions(&node.input, relations, conditions);
            }

            _ => {
                // Other node types - treat as a single relation
                let schema = plan.schema();
                let columns: HashSet<String> =
                    schema.fields().iter().map(|f| f.name.clone()).collect();

                let name = match plan {
                    LogicalPlan::Project(_n) => "project".to_string(),
                    LogicalPlan::Aggregate(_n) => "aggregate".to_string(),
                    _ => "relation".to_string(),
                };

                relations.push(JoinRelation {
                    plan: plan.clone(),
                    name,
                    columns,
                });
            }
        }
    }

    /// Extract join conditions from a predicate expression
    fn extract_join_conditions(&self, expr: &Expr, conditions: &mut Vec<(Expr, Expr)>) {
        match expr {
            Expr::BinaryExpr {
                left,
                op: BinaryOp::And,
                right,
            } => {
                self.extract_join_conditions(left, conditions);
                self.extract_join_conditions(right, conditions);
            }
            Expr::BinaryExpr {
                left,
                op: BinaryOp::Eq,
                right,
            } => {
                // This might be a join condition
                conditions.push((*left.clone(), *right.clone()));
            }
            _ => {}
        }
    }

    /// Extract column names from an expression
    fn extract_columns(&self, expr: &Expr) -> HashSet<String> {
        let mut columns = HashSet::new();
        self.extract_columns_recursive(expr, &mut columns);
        columns
    }

    fn extract_columns_recursive(&self, expr: &Expr, columns: &mut HashSet<String>) {
        match expr {
            Expr::Column(col) => {
                // Include table qualifier if present, otherwise just the column name
                if let Some(ref relation) = col.relation {
                    columns.insert(format!("{}.{}", relation, col.name));
                } else {
                    columns.insert(col.name.clone());
                }
            }
            Expr::BinaryExpr { left, right, .. } => {
                self.extract_columns_recursive(left, columns);
                self.extract_columns_recursive(right, columns);
            }
            Expr::UnaryExpr { expr, .. } => {
                self.extract_columns_recursive(expr, columns);
            }
            Expr::Cast { expr, .. } => {
                self.extract_columns_recursive(expr, columns);
            }
            Expr::Alias { expr, .. } => {
                self.extract_columns_recursive(expr, columns);
            }
            Expr::ScalarFunc { args, .. } => {
                for arg in args {
                    self.extract_columns_recursive(arg, columns);
                }
            }
            Expr::Aggregate { args, .. } => {
                for arg in args {
                    self.extract_columns_recursive(arg, columns);
                }
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                if let Some(op) = operand {
                    self.extract_columns_recursive(op, columns);
                }
                for (when, then) in when_then {
                    self.extract_columns_recursive(when, columns);
                    self.extract_columns_recursive(then, columns);
                }
                if let Some(else_e) = else_expr {
                    self.extract_columns_recursive(else_e, columns);
                }
            }
            Expr::InList { expr, list, .. } => {
                self.extract_columns_recursive(expr, columns);
                for item in list {
                    self.extract_columns_recursive(item, columns);
                }
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                self.extract_columns_recursive(expr, columns);
                self.extract_columns_recursive(low, columns);
                self.extract_columns_recursive(high, columns);
            }
            _ => {}
        }
    }

    /// Find which relations a set of columns belong to
    fn find_relations(
        &self,
        columns: &HashSet<String>,
        column_to_relation: &HashMap<String, Vec<usize>>,
    ) -> Vec<usize> {
        let mut relations: HashSet<usize> = HashSet::new();
        for col in columns {
            if let Some(rels) = column_to_relation.get(col) {
                relations.extend(rels.iter().cloned());
            }
        }
        relations.into_iter().collect()
    }

    /// Get the underlying table name from a relation plan (handles SubqueryAlias)
    fn get_underlying_table_name(&self, plan: &LogicalPlan) -> Option<String> {
        match plan {
            LogicalPlan::Scan(node) => Some(node.table_name.clone()),
            LogicalPlan::SubqueryAlias(node) => self.get_underlying_table_name(&node.input),
            LogicalPlan::Filter(node) => self.get_underlying_table_name(&node.input),
            LogicalPlan::Project(node) => self.get_underlying_table_name(&node.input),
            _ => None,
        }
    }

    /// Select the best starting relation for join ordering
    /// For hash joins, we want to start with SMALL dimension tables and join to larger tables.
    /// Prefers: small tables with filters > small tables > any
    fn select_start_relation(&self, relations: &[JoinRelation], edges: &[JoinEdge]) -> usize {
        // Priority 1: Small dimension tables (nation, region, etc.)
        // Priority 2: Tables with filters (selective)
        // Priority 3: Avoid fact tables (lineitem, orders) as starting point

        let mut best_idx = 0;
        let mut best_score = i32::MIN;

        for (idx, rel) in relations.iter().enumerate() {
            let mut score = 0i32;

            // Estimate table size based on table name heuristics
            // Smaller tables get HIGHER scores (we want to start with small tables)
            // Check both the alias name and the underlying table name
            let name_lower = rel.name.to_lowercase();
            let underlying_name = self
                .get_underlying_table_name(&rel.plan)
                .map(|s| s.to_lowercase())
                .unwrap_or_default();

            // Fact tables are large - penalize them heavily
            // Check both alias name and underlying table name
            let is_lineitem =
                name_lower.contains("lineitem") || underlying_name.contains("lineitem");
            let is_orders = name_lower.contains("orders")
                || underlying_name.contains("orders")
                || name_lower.contains("sales")
                || underlying_name.contains("sales");
            let is_partsupp =
                name_lower.contains("partsupp") || underlying_name.contains("partsupp");
            let is_customer =
                name_lower.contains("customer") || underlying_name.contains("customer");
            let is_part = name_lower.contains("part") || underlying_name.contains("part");
            let is_supplier =
                name_lower.contains("supplier") || underlying_name.contains("supplier");
            let is_nation = name_lower.contains("nation") || underlying_name.contains("nation");
            let is_region = name_lower.contains("region") || underlying_name.contains("region");

            if is_lineitem {
                score -= 10000; // Largest table - avoid starting with it
            } else if is_orders {
                score -= 5000; // Large fact tables
            } else if is_partsupp {
                score -= 3000; // Medium-large
            } else if is_customer || (is_part && !is_partsupp) {
                score -= 1000; // Medium
            } else if is_supplier {
                score += 2000; // Small dimension table
            } else if is_nation || is_region {
                score += 5000; // Very small dimension tables - great starting point
            }

            // Check if this relation has a filter (indicates selectivity)
            let has_filter = self.relation_has_filter(&rel.plan);
            if has_filter {
                score += 3000; // Filtered relations are smaller - good starting point
            }

            // Number of edges (connectivity) - slightly prefer connected tables
            // but not as much as size consideration
            let edge_count = edges
                .iter()
                .filter(|e| e.left_idx == idx || e.right_idx == idx)
                .count() as i32;
            score += edge_count * 10;

            // Number of columns - fewer columns often means dimension table
            let col_count = rel.columns.len() as i32;
            score -= col_count * 5; // Penalize tables with many columns

            if score > best_score {
                best_score = score;
                best_idx = idx;
            }
        }

        best_idx
    }

    /// Check if a logical plan has a filter predicate
    fn relation_has_filter(&self, plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Scan(node) => node.filter.is_some(),
            LogicalPlan::Filter(_) => true,
            LogicalPlan::SubqueryAlias(node) => self.relation_has_filter(&node.input),
            _ => false,
        }
    }

    /// Estimate a score for relation size (higher score = prefer to join next)
    /// Small tables get higher scores because they're better as hash table build sides
    fn estimate_relation_size_score(&self, rel: &JoinRelation) -> i32 {
        let name_lower = rel.name.to_lowercase();
        let underlying_name = self
            .get_underlying_table_name(&rel.plan)
            .map(|s| s.to_lowercase())
            .unwrap_or_default();
        let mut score = 0i32;

        // Check both alias name and underlying table name
        let is_lineitem = name_lower.contains("lineitem") || underlying_name.contains("lineitem");
        let is_orders = name_lower.contains("orders")
            || underlying_name.contains("orders")
            || name_lower.contains("sales")
            || underlying_name.contains("sales");
        let is_partsupp = name_lower.contains("partsupp") || underlying_name.contains("partsupp");
        let is_customer = name_lower.contains("customer") || underlying_name.contains("customer");
        let is_part = name_lower.contains("part") || underlying_name.contains("part");
        let is_supplier = name_lower.contains("supplier") || underlying_name.contains("supplier");
        let is_nation = name_lower.contains("nation") || underlying_name.contains("nation");
        let is_region = name_lower.contains("region") || underlying_name.contains("region");

        // Fact tables are large - prefer to join them later (lower score)
        if is_lineitem {
            score -= 5000;
        } else if is_orders {
            score -= 3000;
        } else if is_partsupp {
            score -= 2000;
        } else if is_customer || (is_part && !is_partsupp) {
            score -= 500;
        } else if is_supplier {
            score += 1000;
        } else if is_nation || is_region {
            score += 2000;
        }

        // Tables with filters are smaller (more selective)
        if self.relation_has_filter(&rel.plan) {
            score += 1500;
        }

        score
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{PlanSchema, ScanNode, SchemaField};
    use arrow::datatypes::DataType as ArrowDataType;

    fn make_scan(name: &str, columns: Vec<&str>) -> LogicalPlan {
        let fields: Vec<SchemaField> = columns
            .into_iter()
            .map(|c| SchemaField::new(c.to_string(), ArrowDataType::Int64).with_relation(name))
            .collect();
        LogicalPlan::Scan(ScanNode {
            table_name: name.to_string(),
            schema: PlanSchema::new(fields),
            projection: None,
            filter: None,
        })
    }

    #[test]
    fn test_reorder_simple() {
        // Create: A CROSS B INNER C ON a.id = c.a_id AND b.id = c.b_id
        let a = make_scan("a", vec!["a_id"]);
        let b = make_scan("b", vec!["b_id"]);
        let c = make_scan("c", vec!["c_a_id", "c_b_id"]);

        let a_schema = a.schema();
        let b_schema = b.schema();
        let ab_schema = a_schema.merge(&b_schema);
        let c_schema = c.schema();
        let abc_schema = ab_schema.merge(&c_schema);

        let cross_ab = LogicalPlan::Join(JoinNode {
            left: Arc::new(a),
            right: Arc::new(b),
            join_type: JoinType::Cross,
            on: vec![],
            filter: None,
            schema: ab_schema,
        });

        let inner_abc = LogicalPlan::Join(JoinNode {
            left: Arc::new(cross_ab),
            right: Arc::new(c),
            join_type: JoinType::Inner,
            on: vec![
                (Expr::column("a_id"), Expr::column("c_a_id")),
                (Expr::column("b_id"), Expr::column("c_b_id")),
            ],
            filter: None,
            schema: abc_schema,
        });

        let rule = JoinReorder;
        assert!(rule.needs_reordering(&inner_abc));

        let optimized = rule.optimize(&inner_abc).unwrap();

        // After optimization, there should be no cross joins
        assert!(!rule.needs_reordering(&optimized));
    }
}
