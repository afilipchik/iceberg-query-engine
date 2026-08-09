//! Join reordering optimization rule
//!
//! This rule reorders joins to avoid Cartesian products by ensuring every join
//! has at least one equality condition. It builds a join graph and finds an
//! ordering that minimizes intermediate result sizes.

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::physical::operators::TableStatistics;
use crate::planner::{BinaryOp, Expr, JoinNode, JoinType, LogicalPlan};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Join reordering optimization rule
pub struct JoinReorder {
    /// Table statistics for statistics-based join ordering
    table_stats: HashMap<String, TableStatistics>,
}

impl JoinReorder {
    /// Create a new JoinReorder rule without statistics (uses heuristics)
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
        }
    }

    /// Create a new JoinReorder rule with table statistics
    pub fn with_table_statistics(stats: HashMap<String, TableStatistics>) -> Self {
        Self { table_stats: stats }
    }
}

impl Default for JoinReorder {
    fn default() -> Self {
        Self::new()
    }
}

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

/// DPsize memo entry: best plan found for a subset of relations (bitmask).
#[derive(Clone, Copy)]
struct DpEntry {
    /// Cumulative C_out cost (sum of intermediate cardinalities)
    cost: f64,
    /// Estimated output cardinality of this subset's join
    rows: f64,
    /// Child subset masks (0 for base relations)
    left: u32,
    right: u32,
}

impl JoinReorder {
    /// Recursively reorder subquery plans inside an expression.
    /// This ensures cross joins within scalar subqueries, EXISTS, and IN subqueries
    /// are also optimized.
    fn reorder_expr(&self, expr: &Expr) -> Result<Expr> {
        match expr {
            Expr::ScalarSubquery(subquery) => {
                let reordered = self.reorder(subquery)?;
                Ok(Expr::ScalarSubquery(Arc::new(reordered)))
            }
            Expr::Exists { subquery, negated } => {
                let reordered = self.reorder(subquery)?;
                Ok(Expr::Exists {
                    subquery: Arc::new(reordered),
                    negated: *negated,
                })
            }
            Expr::InSubquery {
                expr: inner_expr,
                subquery,
                negated,
            } => {
                let reordered_expr = self.reorder_expr(inner_expr)?;
                let reordered_plan = self.reorder(subquery)?;
                Ok(Expr::InSubquery {
                    expr: Box::new(reordered_expr),
                    subquery: Arc::new(reordered_plan),
                    negated: *negated,
                })
            }
            Expr::BinaryExpr { left, op, right } => {
                let new_left = self.reorder_expr(left)?;
                let new_right = self.reorder_expr(right)?;
                Ok(Expr::BinaryExpr {
                    left: Box::new(new_left),
                    op: *op,
                    right: Box::new(new_right),
                })
            }
            Expr::UnaryExpr { op, expr: inner } => {
                let new_inner = self.reorder_expr(inner)?;
                Ok(Expr::UnaryExpr {
                    op: *op,
                    expr: Box::new(new_inner),
                })
            }
            Expr::Alias { expr: inner, name } => {
                let new_inner = self.reorder_expr(inner)?;
                Ok(Expr::Alias {
                    expr: Box::new(new_inner),
                    name: name.clone(),
                })
            }
            Expr::Cast {
                expr: inner,
                data_type,
            } => {
                let new_inner = self.reorder_expr(inner)?;
                Ok(Expr::Cast {
                    expr: Box::new(new_inner),
                    data_type: data_type.clone(),
                })
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                let new_operand = operand
                    .as_ref()
                    .map(|e| self.reorder_expr(e).map(Box::new))
                    .transpose()?;
                let new_when_then = when_then
                    .iter()
                    .map(|(w, t)| Ok((self.reorder_expr(w)?, self.reorder_expr(t)?)))
                    .collect::<Result<Vec<_>>>()?;
                let new_else = else_expr
                    .as_ref()
                    .map(|e| self.reorder_expr(e).map(Box::new))
                    .transpose()?;
                Ok(Expr::Case {
                    operand: new_operand,
                    when_then: new_when_then,
                    else_expr: new_else,
                })
            }
            Expr::ScalarFunc { func, args } => {
                let new_args = args
                    .iter()
                    .map(|a| self.reorder_expr(a))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::ScalarFunc {
                    func: func.clone(),
                    args: new_args,
                })
            }
            Expr::Aggregate {
                func,
                args,
                distinct,
            } => {
                let new_args = args
                    .iter()
                    .map(|a| self.reorder_expr(a))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::Aggregate {
                    func: func.clone(),
                    args: new_args,
                    distinct: *distinct,
                })
            }
            Expr::InList {
                expr: inner,
                list,
                negated,
            } => {
                let new_inner = self.reorder_expr(inner)?;
                let new_list = list
                    .iter()
                    .map(|e| self.reorder_expr(e))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::InList {
                    expr: Box::new(new_inner),
                    list: new_list,
                    negated: *negated,
                })
            }
            Expr::Between {
                expr: inner,
                low,
                high,
                negated,
            } => {
                let new_inner = self.reorder_expr(inner)?;
                let new_low = self.reorder_expr(low)?;
                let new_high = self.reorder_expr(high)?;
                Ok(Expr::Between {
                    expr: Box::new(new_inner),
                    low: Box::new(new_low),
                    high: Box::new(new_high),
                    negated: *negated,
                })
            }
            // Leaf expressions — no subqueries possible
            Expr::Column(_) | Expr::Literal(_) | Expr::Wildcard | Expr::QualifiedWildcard(_) => {
                Ok(expr.clone())
            }
        }
    }

    /// Reorder expressions in a vec, recursing into subqueries
    fn reorder_exprs(&self, exprs: &[Expr]) -> Result<Vec<Expr>> {
        exprs.iter().map(|e| self.reorder_expr(e)).collect()
    }

    fn reorder(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        match plan {
            LogicalPlan::Filter(node) => {
                // Check if input contains joins that need reordering
                // If so, we need to pass filter predicates down to help with join ordering
                if self.needs_reordering(&node.input) {
                    return self.reorder_filter_with_join(node);
                }
                let input = self.reorder(&node.input)?;
                let predicate = self.reorder_expr(&node.predicate)?;
                Ok(LogicalPlan::Filter(crate::planner::FilterNode {
                    input: Arc::new(input),
                    predicate,
                }))
            }

            LogicalPlan::Project(node) => {
                let input = self.reorder(&node.input)?;
                let exprs = self.reorder_exprs(&node.exprs)?;
                Ok(LogicalPlan::Project(crate::planner::ProjectNode {
                    input: Arc::new(input),
                    exprs,
                    schema: node.schema.clone(),
                }))
            }

            LogicalPlan::Join(node) => {
                // Semi/Anti/Single/Mark: preserve structure, reorder children independently
                if matches!(
                    node.join_type,
                    JoinType::Semi | JoinType::Anti | JoinType::Single | JoinType::Mark
                ) {
                    let left = self.reorder(&node.left)?;
                    let right = self.reorder(&node.right)?;
                    return Ok(LogicalPlan::Join(JoinNode {
                        left: Arc::new(left),
                        right: Arc::new(right),
                        join_type: node.join_type,
                        on: node.on.clone(),
                        filter: node.filter.clone(),
                        schema: node.schema.clone(),
                    }));
                }

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
                let group_by = self.reorder_exprs(&node.group_by)?;
                let aggregates = self.reorder_exprs(&node.aggregates)?;
                Ok(LogicalPlan::Aggregate(crate::planner::AggregateNode {
                    input: Arc::new(input),
                    group_by,
                    aggregates,
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
                        cte_name: node.cte_name.clone(),
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

    /// Check if this join tree needs reordering (has cross joins or multi-way inner joins)
    fn needs_reordering(&self, plan: &LogicalPlan) -> bool {
        // Even a single Inner join goes through the enumerator: it decides the
        // BUILD side (smaller estimated side goes left), and the physical
        // planner deliberately trusts that orientation instead of re-guessing.
        // Without this, `FROM lineitem, part WHERE ...` kept SQL order and
        // built a 60M-row hash table probed by 30K rows (Q17).
        self.count_flattenable_joins(plan) >= 1
    }

    /// Count how many flattenable (Cross/Inner) joins exist in a chain.
    /// Stops at Semi/Anti/Left/Right/Full boundaries.
    fn count_flattenable_joins(&self, plan: &LogicalPlan) -> usize {
        match plan {
            LogicalPlan::Join(node)
                if matches!(node.join_type, JoinType::Cross | JoinType::Inner) =>
            {
                1 + self.count_flattenable_joins(&node.left)
                    + self.count_flattenable_joins(&node.right)
            }
            _ => 0,
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
        let mut extra_join_filters: Vec<Expr> = Vec::new();
        self.collect_relations_and_conditions(
            &filter.input,
            &mut relations,
            &mut all_conditions,
            &mut extra_join_filters,
        );

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

        let mut plan = join_result;
        // Join-node filter expressions collected during flattening must be
        // re-applied — dropping them turns joins into cross products.
        for f in &extra_join_filters {
            plan = LogicalPlan::Filter(crate::planner::FilterNode {
                input: Arc::new(plan),
                predicate: f.clone(),
            });
        }
        if let Some(remaining) = remaining_filter {
            Ok(LogicalPlan::Filter(crate::planner::FilterNode {
                input: Arc::new(plan),
                predicate: remaining,
            }))
        } else {
            Ok(plan)
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

        // Cost-based enumeration first: DPsize over the join graph, driven by
        // footer statistics. Falls back to the greedy heuristic below for wide
        // joins (>12 relations) or disconnected graphs (cross joins).
        if relations.len() >= 2 && relations.len() <= 12 {
            if let Some(plan) = self.build_join_tree_dpsize(relations, &edges) {
                return Ok((plan, used_condition_indices));
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
                    // Penalize M:N same-dimension FK joins heavily
                    let mn_penalty = if self.is_same_dimension_fk_edge(edge, relations) {
                        -5000
                    } else {
                        0
                    };
                    let score = size_score + cond_score + mn_penalty;
                    if score > best_score {
                        best_score = score;
                        best_edge = Some((edge_idx, edge.right_idx));
                    }
                } else if !left_in && right_in {
                    // Score based on: prefer small tables (dimension tables) to join early
                    let size_score = self.estimate_relation_size_score(&relations[edge.left_idx]);
                    let cond_score = edge.conditions.len() as i32 * 100;
                    // Penalize M:N same-dimension FK joins heavily
                    let mn_penalty = if self.is_same_dimension_fk_edge(edge, relations) {
                        -5000
                    } else {
                        0
                    };
                    let score = size_score + cond_score + mn_penalty;
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

                // Orient conditions so left=current, right=new_rel
                let on_current_left = if edge.right_idx == new_rel_idx {
                    edge.conditions.clone()
                } else {
                    edge.conditions
                        .iter()
                        .map(|(l, r)| (r.clone(), l.clone()))
                        .collect()
                };

                // Decide build/probe: prefer the smaller side as build (left).
                // If the accumulated result has 2+ tables and the new relation is
                // a small dimension table, swap so new_rel is build.
                let accumulated_tables = self.count_joined_tables(&current);
                let new_rel_is_small = !self.is_large_table(&relations[new_rel_idx]);
                let swap_build_probe = accumulated_tables >= 2 && new_rel_is_small;

                let (left, right, on) = if swap_build_probe {
                    // new_rel as build (left), accumulated as probe (right)
                    let swapped_on: Vec<(Expr, Expr)> = on_current_left
                        .iter()
                        .map(|(l, r)| (r.clone(), l.clone()))
                        .collect();
                    (new_rel, current, swapped_on)
                } else {
                    (current, new_rel, on_current_left)
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

    /// DPsize join enumeration: find the cheapest join tree under a C_out cost
    /// model (sum of intermediate result cardinalities), using footer
    /// statistics for base cardinalities and join-key NDVs.
    ///
    /// Returns None when the graph is disconnected (a cross join would be
    /// required) or when there are too many relations — callers fall back to
    /// the greedy heuristic.
    fn build_join_tree_dpsize(
        &self,
        relations: &[JoinRelation],
        edges: &[JoinEdge],
    ) -> Option<LogicalPlan> {
        let n = relations.len();
        debug_assert!(n <= 12);
        let full: u32 = (1u32 << n) - 1;

        // Base cardinality estimates
        let base_rows: Vec<f64> = relations
            .iter()
            .map(|rel| self.estimate_base_rows(rel))
            .collect();

        // Per-edge equi-join selectivity: 1 / max(combined_ndv_left, combined_ndv_right)
        // where each side's combined NDV over all the edge's key columns is the
        // MAX of per-column NDVs (full-correlation assumption), capped by the
        // side's row count. Multiplying per-column NDVs assumes independence
        // and overestimates composite-key NDV whenever the key columns are
        // correlated: partsupp's (suppkey, partkey) pairs are 4x duplicated,
        // so product-NDV said |lineitem x partsupp| = 8M when the true output
        // is 240M — the DP then buried a 240M-row intermediate at the bottom
        // of Q09's plan. Full correlation is the conservative direction: it
        // can only overestimate join outputs, never hide a blow-up.
        let side_combined_ndv = |rel: usize, exprs: &[&Expr]| -> f64 {
            let mut max_ndv = 1.0f64;
            for e in exprs {
                let ndv = self
                    .column_ndv(&relations[rel], e)
                    .unwrap_or(base_rows[rel].max(10.0));
                max_ndv = max_ndv.max(ndv);
            }
            max_ndv.min(base_rows[rel].max(1.0))
        };

        // Precompute each edge's endpoint masks and combined selectivity
        struct EdgeInfo {
            mask_a: u32,
            mask_b: u32,
            selectivity: f64,
        }
        let edge_infos: Vec<EdgeInfo> = edges
            .iter()
            .map(|e| {
                let left_exprs: Vec<&Expr> = e.conditions.iter().map(|(l, _)| l).collect();
                let right_exprs: Vec<&Expr> = e.conditions.iter().map(|(_, r)| r).collect();
                let ndv_l = side_combined_ndv(e.left_idx, &left_exprs);
                let ndv_r = side_combined_ndv(e.right_idx, &right_exprs);
                EdgeInfo {
                    mask_a: 1 << e.left_idx,
                    mask_b: 1 << e.right_idx,
                    selectivity: (1.0 / ndv_l.max(ndv_r).max(1.0)).clamp(1e-12, 1.0),
                }
            })
            .collect();

        let mut dp: Vec<Option<DpEntry>> = vec![None; (full as usize) + 1];
        for (i, &rows) in base_rows.iter().enumerate() {
            dp[1usize << i] = Some(DpEntry {
                cost: 0.0,
                rows,
                left: 0,
                right: 0,
            });
        }

        // Joined cardinality of two disjoint subsets: product of the sides'
        // rows times the selectivity of every edge crossing the split.
        // Returns None if no edge crosses (would be a cross join).
        let join_rows = |s1: u32, s2: u32, r1: f64, r2: f64| -> Option<f64> {
            let mut sel = 1.0f64;
            let mut connected = false;
            for info in &edge_infos {
                let a_in_1 = info.mask_a & s1 != 0;
                let a_in_2 = info.mask_a & s2 != 0;
                let b_in_1 = info.mask_b & s1 != 0;
                let b_in_2 = info.mask_b & s2 != 0;
                if (a_in_1 && b_in_2) || (a_in_2 && b_in_1) {
                    connected = true;
                    sel *= info.selectivity;
                }
            }
            if connected {
                Some((r1 * r2 * sel).max(1.0))
            } else {
                None
            }
        };

        for s in 2..=(full as usize) {
            let s = s as u32;
            if s.count_ones() < 2 || (s & full) != s {
                continue;
            }
            // Enumerate proper subset splits; s1 < s2 avoids double-counting.
            let mut s1 = (s - 1) & s;
            while s1 > 0 {
                let s2 = s ^ s1;
                if s1 < s2 {
                    if let (Some(e1), Some(e2)) = (dp[s1 as usize], dp[s2 as usize]) {
                        if let Some(rows) = join_rows(s1, s2, e1.rows, e2.rows) {
                            let cost = e1.cost + e2.cost + rows;
                            let better = match dp[s as usize] {
                                None => true,
                                Some(cur) => cost < cur.cost,
                            };
                            if better {
                                dp[s as usize] = Some(DpEntry {
                                    cost,
                                    rows,
                                    left: s1,
                                    right: s2,
                                });
                            }
                        }
                    }
                }
                s1 = (s1 - 1) & s;
            }
        }

        dp[full as usize]?;
        if std::env::var("DP_DEBUG").is_ok() {
            for (s, entry) in dp.iter().enumerate() {
                if let Some(e) = entry {
                    eprintln!(
                        "[dp] memo {:b}: rows={:.0} cost={:.0} split={:b}|{:b}",
                        s, e.rows, e.cost, e.left, e.right
                    );
                }
            }
        }
        self.dp_build_plan(full, &dp, relations, edges)
    }

    /// Materialize the DP solution into a LogicalPlan join tree.
    /// The smaller estimated side goes LEFT (the engine's default build side).
    fn dp_build_plan(
        &self,
        mask: u32,
        dp: &[Option<DpEntry>],
        relations: &[JoinRelation],
        edges: &[JoinEdge],
    ) -> Option<LogicalPlan> {
        let entry = dp[mask as usize]?;
        if mask.count_ones() == 1 {
            let idx = mask.trailing_zeros() as usize;
            return Some(relations[idx].plan.clone());
        }

        let (l_mask, r_mask) = (entry.left, entry.right);
        let l_entry = dp[l_mask as usize]?;
        let r_entry = dp[r_mask as usize]?;

        // Smaller side becomes the LEFT child (hash join build side).
        // Cardinality estimates for multi-join subtrees are far less reliable
        // than base-table row counts (correlated keys break the independence
        // assumption badly — Q09's partsupp join is 16x off). When one side is
        // a base relation and the estimates are within 4x, trust the base
        // relation as the build side: materializing an underestimated
        // intermediate as build (concat + hash table) is how Q09 hit 65GB.
        // Discount confidence in derived estimates by 2x per additional joined
        // relation (capped): a base table's row count is exact, a 5-way join
        // estimate can be orders of magnitude off. (4x/rel over-corrected: Q10
        // chose a 20M-row filtered lineitem build over a 539K intermediate.)
        let uncertainty = |mask: u32, rows: f64| -> f64 {
            let extra_rels = mask.count_ones().saturating_sub(1);
            rows * 2f64.powi(extra_rels.min(6) as i32)
        };
        let l_score = uncertainty(l_mask, l_entry.rows);
        let r_score = uncertainty(r_mask, r_entry.rows);
        let (build_mask, probe_mask) = if l_score <= r_score {
            (l_mask, r_mask)
        } else {
            (r_mask, l_mask)
        };
        if std::env::var("DP_DEBUG").is_ok() {
            eprintln!(
                "[dp] split {:b}: l={:b} rows={:.0} score={:.0} | r={:b} rows={:.0} score={:.0} -> build={:b}",
                mask, l_mask, l_entry.rows, l_score, r_mask, r_entry.rows, r_score, build_mask
            );
        }

        let build = self.dp_build_plan(build_mask, dp, relations, edges)?;
        let probe = self.dp_build_plan(probe_mask, dp, relations, edges)?;

        // Collect every edge condition crossing this split, oriented as
        // (build_expr, probe_expr).
        let mut on: Vec<(Expr, Expr)> = Vec::new();
        for edge in edges {
            let a = 1u32 << edge.left_idx;
            let b = 1u32 << edge.right_idx;
            if a & build_mask != 0 && b & probe_mask != 0 {
                on.extend(edge.conditions.iter().cloned());
            } else if b & build_mask != 0 && a & probe_mask != 0 {
                on.extend(edge.conditions.iter().map(|(l, r)| (r.clone(), l.clone())));
            }
        }
        if on.is_empty() {
            return None; // should not happen: DP only joins connected splits
        }

        let mut schema_fields = build.schema().fields().to_vec();
        schema_fields.extend(probe.schema().fields().iter().cloned());
        let schema = crate::planner::PlanSchema::new(schema_fields);

        Some(LogicalPlan::Join(JoinNode {
            left: Arc::new(build),
            right: Arc::new(probe),
            join_type: JoinType::Inner,
            on,
            filter: None,
            schema,
        }))
    }

    /// Estimated cardinality of a base relation: footer row count scaled by a
    /// heuristic selectivity for any attached filter predicates.
    fn estimate_base_rows(&self, rel: &JoinRelation) -> f64 {
        let base = self.get_relation_row_count(rel).unwrap_or(10_000).max(1) as f64;
        let stats = self
            .get_underlying_table_name(&rel.plan)
            .and_then(|t| self.table_stats.get(&t));
        (base * Self::plan_filter_selectivity_with_stats(&rel.plan, stats)).max(1.0)
    }

    /// Range-aware selectivity for a comparison of an integer/date column
    /// against a literal, using footer min/max. Returns None when stats or
    /// literal types don't allow it.
    fn range_selectivity(
        stats: Option<&TableStatistics>,
        col: &Expr,
        op: &BinaryOp,
        lit: &Expr,
        flipped: bool,
    ) -> Option<f64> {
        let stats = stats?;
        let name = match col {
            Expr::Column(c) => c.name.to_lowercase(),
            _ => return None,
        };
        let cs = stats.column_stats.get(&name)?;
        // Equality against any literal: NDV-based selectivity needs no
        // numeric min/max, so it must not sit behind that gate — string
        // dimension filters (o_orderstatus, n_name, c_mktsegment, p_brand)
        // otherwise fell to the generic 10% guess. String NDVs come from the
        // dictionary-page probe in the parquet stats collector.
        if *op == BinaryOp::Eq && matches!(lit, Expr::Literal(_)) {
            if let Some(ndv) = cs.ndv_est {
                return Some((1.0 / ndv.max(1) as f64).clamp(0.0005, 1.0));
            }
        }
        let (min, max) = (cs.min_i64? as f64, cs.max_i64? as f64);
        if max <= min {
            return None;
        }
        use crate::planner::ScalarValue;
        let v = match lit {
            Expr::Literal(ScalarValue::Int8(v)) => *v as f64,
            Expr::Literal(ScalarValue::Int16(v)) => *v as f64,
            Expr::Literal(ScalarValue::Int32(v)) => *v as f64,
            Expr::Literal(ScalarValue::Int64(v)) => *v as f64,
            Expr::Literal(ScalarValue::Date32(v)) => *v as f64,
            Expr::Literal(ScalarValue::Date64(v)) => *v as f64,
            Expr::Literal(ScalarValue::Float64(v)) => v.into_inner(),
            _ => return None,
        };
        let width = max - min;
        // Normalize direction: `flipped` means the literal was on the left.
        let op = if flipped {
            match op {
                BinaryOp::Lt => BinaryOp::Gt,
                BinaryOp::LtEq => BinaryOp::GtEq,
                BinaryOp::Gt => BinaryOp::Lt,
                BinaryOp::GtEq => BinaryOp::LtEq,
                other => *other,
            }
        } else {
            *op
        };
        let sel = match op {
            BinaryOp::Lt | BinaryOp::LtEq => (v - min) / width,
            BinaryOp::Gt | BinaryOp::GtEq => (max - v) / width,
            BinaryOp::Eq => 1.0 / cs.ndv_est.unwrap_or(100).max(1) as f64,
            _ => return None,
        };
        Some(sel.clamp(0.0005, 1.0))
    }

    /// Multiply the selectivities of all filters attached beneath a relation
    /// (both Filter nodes and predicates pushed into ScanNode.filter).
    fn plan_filter_selectivity_with_stats(
        plan: &LogicalPlan,
        stats: Option<&TableStatistics>,
    ) -> f64 {
        match plan {
            LogicalPlan::Filter(node) => {
                Self::predicate_selectivity(&node.predicate, stats)
                    * Self::plan_filter_selectivity_with_stats(&node.input, stats)
            }
            LogicalPlan::Scan(node) => node
                .filter
                .as_ref()
                .map(|f| Self::predicate_selectivity(f, stats))
                .unwrap_or(1.0),
            LogicalPlan::SubqueryAlias(node) => {
                Self::plan_filter_selectivity_with_stats(&node.input, stats)
            }
            LogicalPlan::Project(node) => {
                Self::plan_filter_selectivity_with_stats(&node.input, stats)
            }
            _ => 1.0,
        }
    }

    /// Per-predicate selectivity: exact range fractions from footer min/max
    /// where possible, System-R-style guesses otherwise.
    fn predicate_selectivity(expr: &Expr, stats: Option<&TableStatistics>) -> f64 {
        let sel = match expr {
            Expr::BinaryExpr { left, op, right } => match op {
                BinaryOp::And => {
                    // Band-aware conjunction: `col >= a AND col < b` is one
                    // band of width (b-a)/(max-min), not the product of two
                    // one-sided fractions (a 3-month order-date window came
                    // out at 22% instead of 3.8%, flipping build sides).
                    return Self::conjunction_selectivity(expr, stats);
                }
                BinaryOp::Or => (Self::predicate_selectivity(left, stats)
                    + Self::predicate_selectivity(right, stats))
                .min(1.0),
                BinaryOp::Eq | BinaryOp::Lt | BinaryOp::LtEq | BinaryOp::Gt | BinaryOp::GtEq => {
                    // Try footer-stats range estimation for col-vs-literal
                    if let Some(sel) = Self::range_selectivity(stats, left, op, right, false)
                        .or_else(|| Self::range_selectivity(stats, right, op, left, true))
                    {
                        sel
                    } else if *op == BinaryOp::Eq {
                        0.1
                    } else {
                        0.3
                    }
                }
                BinaryOp::NotEq => 0.9,
                BinaryOp::Like => 0.25,
                BinaryOp::NotLike => 0.75,
                _ => 0.25,
            },
            Expr::Between { negated, .. } => {
                if *negated {
                    0.75
                } else {
                    0.25
                }
            }
            Expr::InList { list, .. } => (0.1 * list.len() as f64).min(0.5),
            Expr::UnaryExpr {
                op: crate::planner::UnaryOp::IsNull,
                ..
            } => 0.1,
            Expr::UnaryExpr { .. } => 0.9,
            // Subquery predicates and anything opaque: assume moderate
            _ => 0.5,
        };
        sel.clamp(1e-4, 1.0)
    }

    /// Literal numeric value for range estimation.
    fn range_literal_value(e: &Expr) -> Option<f64> {
        use crate::planner::ScalarValue;
        match e {
            Expr::Literal(ScalarValue::Int8(v)) => Some(*v as f64),
            Expr::Literal(ScalarValue::Int16(v)) => Some(*v as f64),
            Expr::Literal(ScalarValue::Int32(v)) => Some(*v as f64),
            Expr::Literal(ScalarValue::Int64(v)) => Some(*v as f64),
            Expr::Literal(ScalarValue::Date32(v)) => Some(*v as f64),
            Expr::Literal(ScalarValue::Date64(v)) => Some(*v as f64),
            Expr::Literal(ScalarValue::Float64(v)) => Some(v.into_inner()),
            _ => None,
        }
    }

    /// AND-conjunction selectivity with per-column band intersection.
    fn conjunction_selectivity(expr: &Expr, stats: Option<&TableStatistics>) -> f64 {
        fn flatten<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
            if let Expr::BinaryExpr { left, op, right } = e {
                if *op == BinaryOp::And {
                    flatten(left, out);
                    flatten(right, out);
                    return;
                }
            }
            out.push(e);
        }
        let mut conjuncts = Vec::new();
        flatten(expr, &mut conjuncts);

        // col -> (lo, hi) bounds accumulated across the conjunction
        let mut bands: std::collections::HashMap<String, (f64, f64)> =
            std::collections::HashMap::new();
        let mut sel = 1.0f64;
        for c in conjuncts {
            let mut banded = false;
            if let (Some(st), Expr::BinaryExpr { left, op, right }) = (stats, c) {
                // Normalize to col-op-literal
                let norm = match (&**left, &**right) {
                    (Expr::Column(col), _) => {
                        Self::range_literal_value(right).map(|v| (col, *op, v))
                    }
                    (_, Expr::Column(col)) => Self::range_literal_value(left).map(|v| {
                        let flipped = match op {
                            BinaryOp::Lt => BinaryOp::Gt,
                            BinaryOp::LtEq => BinaryOp::GtEq,
                            BinaryOp::Gt => BinaryOp::Lt,
                            BinaryOp::GtEq => BinaryOp::LtEq,
                            other => *other,
                        };
                        (col, flipped, v)
                    }),
                    _ => None,
                };
                if let Some((col, op, v)) = norm {
                    let name = col.name.to_lowercase();
                    if let Some(cs) = st.column_stats.get(&name) {
                        if let (Some(min), Some(max)) = (cs.min_i64, cs.max_i64) {
                            if max > min {
                                let entry = bands.entry(name).or_insert((min as f64, max as f64));
                                match op {
                                    BinaryOp::Gt | BinaryOp::GtEq => {
                                        entry.0 = entry.0.max(v);
                                        banded = true;
                                    }
                                    BinaryOp::Lt | BinaryOp::LtEq => {
                                        entry.1 = entry.1.min(v);
                                        banded = true;
                                    }
                                    BinaryOp::Eq => {
                                        entry.0 = entry.0.max(v);
                                        entry.1 = entry.1.min(v);
                                        banded = true;
                                    }
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
            if !banded {
                sel *= Self::predicate_selectivity(c, stats);
            }
        }
        if let Some(st) = stats {
            for (name, (lo, hi)) in bands {
                if let Some(cs) = st.column_stats.get(&name) {
                    if let (Some(min), Some(max)) = (cs.min_i64, cs.max_i64) {
                        let width = (max - min) as f64;
                        let band = ((hi - lo) / width).clamp(1e-4, 1.0);
                        sel *= band;
                    }
                }
            }
        }
        sel.clamp(1e-4, 1.0)
    }

    /// NDV estimate for a join-key expression on a relation, from footer
    /// column statistics of the underlying base table.
    fn column_ndv(&self, rel: &JoinRelation, expr: &Expr) -> Option<f64> {
        let col = match expr {
            Expr::Column(c) => c.name.to_lowercase(),
            // Expression keys (e.g. packed composite keys from eager
            // aggregation): NDV = max over the referenced columns' NDVs —
            // the same full-correlation assumption as multi-column keys.
            // Returning None here made the caller substitute the relation's
            // row count, which UNDERestimated the join output and buried
            // fanout joins at the bottom of the tree.
            _ => {
                let mut names: Vec<String> = Vec::new();
                crate::physical::morsel::collect_expr_columns(expr, &mut names);
                let mut best: Option<f64> = None;
                for name in names {
                    let col_expr = Expr::Column(crate::planner::Column {
                        relation: None,
                        name,
                    });
                    if let Some(v) = self.column_ndv(rel, &col_expr) {
                        best = Some(best.map_or(v, |b: f64| b.max(v)));
                    }
                }
                return best;
            }
        };
        let table = self.get_underlying_table_name(&rel.plan)?;
        let stats = self.table_stats.get(&table)?;
        let col_stats = stats.column_stats.get(&col)?;
        col_stats.ndv_est.map(|v| v as f64)
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

        let mut extra_join_filters: Vec<Expr> = Vec::new();
        self.collect_relations_and_conditions(
            plan,
            &mut relations,
            &mut all_conditions,
            &mut extra_join_filters,
        );

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

        // Step 4a: Cost-based enumeration first (same as build_optimized_join_tree).
        // Falls back to the greedy heuristic for wide or disconnected graphs.
        if relations.len() >= 2 && relations.len() <= 12 {
            if let Some(mut plan) = self.build_join_tree_dpsize(&relations, &edges) {
                // Re-apply conditions that didn't become join edges as filters
                for (l, r) in &remaining_conditions {
                    plan = LogicalPlan::Filter(crate::planner::FilterNode {
                        input: Arc::new(plan),
                        predicate: Expr::BinaryExpr {
                            left: Box::new(l.clone()),
                            op: BinaryOp::Eq,
                            right: Box::new(r.clone()),
                        },
                    });
                }
                for f in &extra_join_filters {
                    plan = LogicalPlan::Filter(crate::planner::FilterNode {
                        input: Arc::new(plan),
                        predicate: f.clone(),
                    });
                }
                return Ok(plan);
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
                    // Penalize M:N same-dimension FK joins heavily
                    let mn_penalty = if self.is_same_dimension_fk_edge(edge, &relations) {
                        -5000
                    } else {
                        0
                    };
                    let score = base_score + size_score + mn_penalty;
                    if score > best_score {
                        best_score = score;
                        best_edge = Some((edge_idx, edge.right_idx));
                    }
                } else if !left_in && right_in {
                    // Can add left relation
                    let base_score = edge.conditions.len() as i32 * 100;
                    let size_score = self.estimate_relation_size_score(&relations[edge.left_idx]);
                    // Penalize M:N same-dimension FK joins heavily
                    let mn_penalty = if self.is_same_dimension_fk_edge(edge, &relations) {
                        -5000
                    } else {
                        0
                    };
                    let score = base_score + size_score + mn_penalty;
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

                // Orient conditions so left=current, right=new_rel
                let on_current_left = if edge.right_idx == new_idx {
                    edge.conditions.clone()
                } else {
                    edge.conditions
                        .iter()
                        .map(|(l, r)| (r.clone(), l.clone()))
                        .collect()
                };

                // Decide build/probe: prefer the smaller side as build (left).
                let accumulated_tables = self.count_joined_tables(&current);
                let new_rel_is_small = !self.is_large_table(new_rel);
                let swap_build_probe = accumulated_tables >= 2 && new_rel_is_small;

                let (left, right, on) = if swap_build_probe {
                    let swapped_on: Vec<(Expr, Expr)> = on_current_left
                        .iter()
                        .map(|(l, r)| (r.clone(), l.clone()))
                        .collect();
                    (new_rel.plan.clone(), current, swapped_on)
                } else {
                    (current, new_rel.plan.clone(), on_current_left)
                };

                let left_schema = left.schema();
                let right_schema = right.schema();
                let combined_schema = left_schema.merge(&right_schema);

                result_plan = Some(LogicalPlan::Join(JoinNode {
                    left: Arc::new(left),
                    right: Arc::new(right),
                    join_type: JoinType::Inner,
                    on,
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

        // Re-apply conditions that didn't become join edges (e.g. expression
        // conditions like `a.n + 1 = b.n`, or ambiguous self-join columns).
        // Dropping them silently turns a join into a cross product.
        let mut plan = result_plan.unwrap();
        for (l, r) in &remaining_conditions {
            plan = LogicalPlan::Filter(crate::planner::FilterNode {
                input: Arc::new(plan),
                predicate: Expr::BinaryExpr {
                    left: Box::new(l.clone()),
                    op: BinaryOp::Eq,
                    right: Box::new(r.clone()),
                },
            });
        }
        for f in &extra_join_filters {
            plan = LogicalPlan::Filter(crate::planner::FilterNode {
                input: Arc::new(plan),
                predicate: f.clone(),
            });
        }
        Ok(plan)
    }

    /// Collect all base relations and join conditions from a join tree
    fn collect_relations_and_conditions(
        &self,
        plan: &LogicalPlan,
        relations: &mut Vec<JoinRelation>,
        conditions: &mut Vec<(Expr, Expr)>,
        extra_filters: &mut Vec<Expr>,
    ) {
        match plan {
            LogicalPlan::Join(node) => {
                // Only flatten Cross and Inner joins - other join types have specific semantics
                // and shouldn't be reordered (e.g., LeftJoin from subquery decorrelation)
                if node.join_type == JoinType::Cross || node.join_type == JoinType::Inner {
                    // Collect conditions from this join
                    conditions.extend(node.on.iter().cloned());
                    // A join filter (e.g. expression conditions like a.n + 1 = b.n
                    // that the binder can't split into an equi pair) must survive
                    // flattening or the join silently becomes a cross product.
                    if let Some(f) = &node.filter {
                        extra_filters.push(f.clone());
                    }

                    // Recursively collect from children
                    self.collect_relations_and_conditions(
                        &node.left,
                        relations,
                        conditions,
                        extra_filters,
                    );
                    self.collect_relations_and_conditions(
                        &node.right,
                        relations,
                        conditions,
                        extra_filters,
                    );
                } else {
                    // Left/Right/Full/Semi/Anti/Single/Mark: treat entire join as opaque relation
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
                let over_flattenable_join = matches!(
                    &*node.input,
                    LogicalPlan::Join(j)
                        if j.join_type == JoinType::Cross || j.join_type == JoinType::Inner
                );
                if over_flattenable_join {
                    // Filter over a join being flattened: equality conjuncts may
                    // become join edges; everything else must be re-applied by
                    // the caller (extra_filters) or it is silently dropped —
                    // Q20's l_shipdate range vanished exactly this way.
                    self.extract_join_conditions(&node.predicate, conditions);
                    Self::collect_non_eq_conjuncts(&node.predicate, extra_filters);
                    self.collect_relations_and_conditions(
                        &node.input,
                        relations,
                        conditions,
                        extra_filters,
                    );
                } else {
                    // Filter over a base relation: keep it glued so selectivity
                    // stays with the relation (and nothing is lost).
                    let columns: HashSet<String> = node
                        .input
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name.clone())
                        .collect();
                    let name = self
                        .get_underlying_table_name(&node.input)
                        .unwrap_or_else(|| "relation".to_string());
                    relations.push(JoinRelation {
                        plan: plan.clone(),
                        name,
                        columns,
                    });
                }
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

    /// Collect the non-equality conjuncts of a predicate (the parts that can
    /// never become join edges and must be re-applied as filters).
    fn collect_non_eq_conjuncts(expr: &Expr, out: &mut Vec<Expr>) {
        match expr {
            Expr::BinaryExpr {
                left,
                op: BinaryOp::And,
                right,
            } => {
                Self::collect_non_eq_conjuncts(left, out);
                Self::collect_non_eq_conjuncts(right, out);
            }
            Expr::BinaryExpr {
                op: BinaryOp::Eq, ..
            } => {}
            other => out.push(other.clone()),
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

    /// Get the row count for a relation from statistics, if available.
    /// An Aggregate relation (e.g. an eager pre-aggregate or a decorrelated
    /// subquery aggregate over a single table) is estimated as its group-key
    /// NDV, capped by the underlying table's row count. Without this, such
    /// relations fell back to the 10K default and the DP scheduled fanout
    /// joins against them far too early.
    fn get_relation_row_count(&self, rel: &JoinRelation) -> Option<usize> {
        self.plan_row_estimate(&rel.plan)
    }

    fn plan_row_estimate(&self, plan: &LogicalPlan) -> Option<usize> {
        match plan {
            LogicalPlan::SubqueryAlias(node) => self.plan_row_estimate(&node.input),
            LogicalPlan::Project(node) => self.plan_row_estimate(&node.input),
            LogicalPlan::Aggregate(node) => {
                let input_rows = self.plan_row_estimate(&node.input)?;
                if node.group_by.is_empty() {
                    return Some(1);
                }
                let table = self.get_underlying_table_name(&node.input)?;
                let stats = self.table_stats.get(&table)?;
                // NDV per group expr: max over referenced columns (full
                // correlation); independence ACROSS group exprs (product).
                let mut groups = 1f64;
                for g in &node.group_by {
                    let mut names: Vec<String> = Vec::new();
                    crate::physical::morsel::collect_expr_columns(g, &mut names);
                    let mut ndv = 0f64;
                    for name in names {
                        if let Some(cs) = stats.column_stats.get(&name.to_lowercase()) {
                            if let Some(v) = cs.ndv_est {
                                ndv = ndv.max(v as f64);
                            }
                        }
                    }
                    if ndv == 0.0 {
                        return Some(input_rows); // unknown: assume no collapse
                    }
                    groups *= ndv;
                    if groups > input_rows as f64 {
                        return Some(input_rows);
                    }
                }
                Some((groups as usize).min(input_rows).max(1))
            }
            _ => {
                let table_name = self.get_underlying_table_name(plan)?;
                let stats = self.table_stats.get(&table_name)?;
                Some(stats.row_count)
            }
        }
    }

    /// Select the best starting relation for join ordering
    /// For hash joins, we want to start with SMALL dimension tables and join to larger tables.
    /// Prefers: small tables with filters > small tables > any
    fn select_start_relation(&self, relations: &[JoinRelation], edges: &[JoinEdge]) -> usize {
        // If we have statistics, use row counts directly
        if !self.table_stats.is_empty() {
            let mut best_idx = 0;
            let mut best_score = i64::MIN;

            for (idx, rel) in relations.iter().enumerate() {
                let mut score = 0i64;

                // Use actual row counts: smaller tables get higher scores
                if let Some(row_count) = self.get_relation_row_count(rel) {
                    // Invert: fewer rows = higher score
                    // Use log scale to avoid extreme differences
                    score = -(row_count as i64);
                }

                // Check if this relation has a filter (indicates selectivity)
                let has_filter = self.relation_has_filter(&rel.plan);
                if has_filter {
                    // Filtered relation likely smaller — boost heavily
                    score = score / 3 + 1_000_000;
                }

                // Number of edges (connectivity) - slightly prefer connected tables
                let edge_count = edges
                    .iter()
                    .filter(|e| e.left_idx == idx || e.right_idx == idx)
                    .count() as i64;
                score += edge_count * 10;

                if score > best_score {
                    best_score = score;
                    best_idx = idx;
                }
            }

            return best_idx;
        }

        // Fall back to name-based heuristics when no statistics available
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

    /// Check if an edge's conditions are all "same-dimension FK" patterns.
    /// E.g., `c_nationkey = s_nationkey` — both reference the `nation` dimension
    /// but neither table IS the nation table. This creates M:N join explosions.
    fn is_same_dimension_fk_edge(&self, edge: &JoinEdge, relations: &[JoinRelation]) -> bool {
        if edge.conditions.is_empty() {
            return false;
        }
        // All conditions on this edge must be same-dimension FK
        edge.conditions.iter().all(|(left_expr, right_expr)| {
            self.is_same_dimension_fk_condition(left_expr, right_expr, relations, edge)
        })
    }

    /// Check if a single condition is a same-dimension FK pattern.
    /// Detects `X.foo_key = Y.foo_key` where both columns reference the same
    /// dimension (e.g., nationkey → nation) but neither relation IS that dimension.
    fn is_same_dimension_fk_condition(
        &self,
        left_expr: &Expr,
        right_expr: &Expr,
        relations: &[JoinRelation],
        edge: &JoinEdge,
    ) -> bool {
        // Extract the raw column names
        let left_name = self.extract_column_name(left_expr);
        let right_name = self.extract_column_name(right_expr);
        let (left_name, right_name) = match (left_name, right_name) {
            (Some(l), Some(r)) => (l, r),
            _ => return false,
        };

        // Strip single-char prefix + underscore: c_nationkey → nationkey, s_nationkey → nationkey
        let left_base = strip_prefix(&left_name);
        let right_base = strip_prefix(&right_name);

        // Both base names must match
        if left_base != right_base {
            return false;
        }

        // The base name should end with "key" to indicate a foreign key pattern
        if !left_base.ends_with("key") {
            return false;
        }

        // Extract dimension name: "nationkey" → "nation", "partkey" → "part"
        let dimension = left_base.trim_end_matches("key");
        if dimension.is_empty() {
            return false;
        }

        // Check if neither relation IS the dimension table
        let left_rel = &relations[edge.left_idx];
        let right_rel = &relations[edge.right_idx];
        let left_table = self
            .get_underlying_table_name(&left_rel.plan)
            .unwrap_or_else(|| left_rel.name.clone())
            .to_lowercase();
        let right_table = self
            .get_underlying_table_name(&right_rel.plan)
            .unwrap_or_else(|| right_rel.name.clone())
            .to_lowercase();

        // If either table IS the dimension table, this is a normal FK→PK join (fine)
        // Use starts_with to handle naming conventions: "supp" matches "supplier",
        // "cust" matches "customer", "order" matches "orders", etc.
        if left_table == dimension
            || right_table == dimension
            || left_table.starts_with(dimension)
            || right_table.starts_with(dimension)
        {
            return false;
        }

        // Neither table is the dimension → same-dimension FK → M:N explosion risk
        true
    }

    /// Extract the unqualified column name from a simple column expression
    fn extract_column_name(&self, expr: &Expr) -> Option<String> {
        match expr {
            Expr::Column(col) => Some(col.name.clone()),
            _ => None,
        }
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
        // If we have statistics, use actual row counts
        if !self.table_stats.is_empty() {
            if let Some(row_count) = self.get_relation_row_count(rel) {
                // Smaller tables get higher scores (better build sides)
                // Scale: 25 rows -> ~8000, 150K rows -> ~3000, 6M rows -> ~800, 600M -> -3200
                let score = 10000 - (row_count as f64).log2() as i32 * 500;
                let score = if self.relation_has_filter(&rel.plan) {
                    score + 1500
                } else {
                    score
                };
                return score;
            }
        }

        // Fall back to name-based heuristics
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

    /// Count the number of leaf tables in a join tree
    fn count_joined_tables(&self, plan: &LogicalPlan) -> usize {
        match plan {
            LogicalPlan::Join(node) => {
                self.count_joined_tables(&node.left) + self.count_joined_tables(&node.right)
            }
            LogicalPlan::Filter(node) => self.count_joined_tables(&node.input),
            LogicalPlan::Project(node) => self.count_joined_tables(&node.input),
            LogicalPlan::SubqueryAlias(node) => self.count_joined_tables(&node.input),
            _ => 1,
        }
    }

    /// Check if a relation involves a large table (lineitem, orders, partsupp)
    fn is_large_table(&self, rel: &JoinRelation) -> bool {
        let name_lower = rel.name.to_lowercase();
        let underlying_name = self
            .get_underlying_table_name(&rel.plan)
            .map(|s| s.to_lowercase())
            .unwrap_or_default();

        let is_lineitem = name_lower.contains("lineitem") || underlying_name.contains("lineitem");
        let is_orders = name_lower.contains("orders") || underlying_name.contains("orders");
        let is_partsupp = name_lower.contains("partsupp") || underlying_name.contains("partsupp");

        is_lineitem || is_orders || is_partsupp
    }
}

/// Strip a single-char prefix + underscore from a column name.
/// E.g., "c_nationkey" → "nationkey", "s_nationkey" → "nationkey",
/// "ps_partkey" → "partkey", "l_orderkey" → "orderkey"
fn strip_prefix(name: &str) -> &str {
    // Handle 2-char prefixes like "ps_"
    if name.len() > 3 && name.as_bytes()[2] == b'_' {
        return &name[3..];
    }
    // Handle 1-char prefixes like "c_", "s_", "l_", "o_", "n_", "r_"
    if name.len() > 2 && name.as_bytes()[1] == b'_' {
        return &name[2..];
    }
    name
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

        let rule = JoinReorder::new();
        assert!(rule.needs_reordering(&inner_abc));

        let optimized = rule.optimize(&inner_abc).unwrap();

        // After optimization, there should be no cross joins
        assert!(!has_cross_joins(&optimized));
    }

    fn has_cross_joins(plan: &LogicalPlan) -> bool {
        match plan {
            LogicalPlan::Join(node) => {
                if node.join_type == JoinType::Cross {
                    return true;
                }
                has_cross_joins(&node.left) || has_cross_joins(&node.right)
            }
            _ => false,
        }
    }
}
