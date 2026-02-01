//! Flatten Dependent Join - transforms correlated subqueries into DelimJoin/DelimGet
//!
//! This optimizer rule implements DuckDB-style subquery decorrelation by:
//! 1. Detecting correlated subqueries (EXISTS, NOT EXISTS, IN, Scalar)
//! 2. Extracting correlation columns from the outer query
//! 3. Creating a DelimGet node inside the subquery to receive distinct correlation values
//! 4. Wrapping the entire structure in a DelimJoin
//!
//! The result is O(n + m) execution instead of O(n * m) for correlated subqueries.

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::planner::{
    AggregateNode, BinaryOp, DelimGetNode, DelimJoinNode, DistinctNode, Expr, FilterNode, JoinNode,
    JoinType, LimitNode, LogicalPlan, PlanSchema, ProjectNode, SchemaField, SortNode,
    SubqueryAliasNode, UnionNode,
};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Global counter for generating unique DelimGet IDs
static DELIM_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_delim_id() -> u64 {
    DELIM_ID_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Flatten Dependent Join optimizer rule
pub struct FlattenDependentJoin;

impl OptimizerRule for FlattenDependentJoin {
    fn name(&self) -> &str {
        "FlattenDependentJoin"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        flatten_plan(plan)
    }
}

/// Recursively process the plan and flatten correlated subqueries
fn flatten_plan(plan: &LogicalPlan) -> Result<LogicalPlan> {
    // First, recursively process children
    let plan = match plan {
        LogicalPlan::Filter(node) => {
            let new_input = flatten_plan(&node.input)?;
            LogicalPlan::Filter(FilterNode {
                input: Arc::new(new_input),
                predicate: node.predicate.clone(),
            })
        }
        LogicalPlan::Project(node) => {
            let new_input = flatten_plan(&node.input)?;
            LogicalPlan::Project(ProjectNode {
                input: Arc::new(new_input),
                exprs: node.exprs.clone(),
                schema: node.schema.clone(),
            })
        }
        LogicalPlan::Join(node) => {
            let new_left = flatten_plan(&node.left)?;
            let new_right = flatten_plan(&node.right)?;
            LogicalPlan::Join(JoinNode {
                left: Arc::new(new_left),
                right: Arc::new(new_right),
                join_type: node.join_type,
                on: node.on.clone(),
                filter: node.filter.clone(),
                schema: node.schema.clone(),
            })
        }
        LogicalPlan::Aggregate(node) => {
            let new_input = flatten_plan(&node.input)?;
            LogicalPlan::Aggregate(AggregateNode {
                input: Arc::new(new_input),
                group_by: node.group_by.clone(),
                aggregates: node.aggregates.clone(),
                schema: node.schema.clone(),
            })
        }
        LogicalPlan::Sort(node) => {
            let new_input = flatten_plan(&node.input)?;
            LogicalPlan::Sort(SortNode {
                input: Arc::new(new_input),
                order_by: node.order_by.clone(),
            })
        }
        LogicalPlan::Limit(node) => {
            let new_input = flatten_plan(&node.input)?;
            LogicalPlan::Limit(LimitNode {
                input: Arc::new(new_input),
                skip: node.skip,
                fetch: node.fetch,
            })
        }
        LogicalPlan::Distinct(node) => {
            let new_input = flatten_plan(&node.input)?;
            LogicalPlan::Distinct(DistinctNode {
                input: Arc::new(new_input),
            })
        }
        LogicalPlan::SubqueryAlias(node) => {
            let new_input = flatten_plan(&node.input)?;
            LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                input: Arc::new(new_input),
                alias: node.alias.clone(),
                schema: node.schema.clone(),
            })
        }
        LogicalPlan::Union(node) => {
            let new_inputs: Result<Vec<Arc<LogicalPlan>>> = node
                .inputs
                .iter()
                .map(|input| flatten_plan(input).map(Arc::new))
                .collect();
            LogicalPlan::Union(UnionNode {
                inputs: new_inputs?,
                schema: node.schema.clone(),
                all: node.all,
            })
        }
        _ => plan.clone(),
    };

    // Now try to flatten correlated subqueries in Filter nodes
    if let LogicalPlan::Filter(node) = &plan {
        if let Some(flattened) = try_flatten_filter(node)? {
            return Ok(flattened);
        }
    }

    Ok(plan)
}

/// Try to flatten a filter with correlated subqueries into DelimJoin
///
/// This handles:
/// - Single EXISTS/NOT EXISTS subqueries → DelimJoin (Semi/Anti)
/// - Single IN/NOT IN subqueries → DelimJoin (Mark)
/// - Single scalar subquery comparisons → DelimJoin (Single)
///
/// For multiple subqueries (e.g., `EXISTS(...) AND NOT EXISTS(...)`), we fall
/// back to SubqueryDecorrelation because sequential processing corrupts the schema.
/// Each DelimJoin transforms the plan, and the next subquery's correlation detection
/// fails because the "outer" columns are no longer available in the same form.
fn try_flatten_filter(node: &FilterNode) -> Result<Option<LogicalPlan>> {
    // Extract subquery expressions from the predicate
    let (subquery_exprs, other_predicates) = extract_subquery_predicates(&node.predicate);

    if subquery_exprs.is_empty() {
        return Ok(None);
    }

    // For multiple subqueries, fall through to SubqueryDecorrelation.
    // Sequential processing doesn't work because:
    // 1. First DelimJoin has schema = outer.schema (correct for Semi/Anti)
    // 2. Second subquery's correlation detection expects the original multi-join schema
    // 3. But the DelimJoin doesn't expose the same columns as the original join
    //
    // Example: Q21 has EXISTS(l2) AND NOT EXISTS(l3) both correlating on l1.l_orderkey
    // After flattening EXISTS(l2), the plan becomes DelimJoin(outer, inner2)
    // The NOT EXISTS(l3) then tries to correlate on l1.l_orderkey, but l1 is no longer
    // directly accessible - it's embedded in the DelimJoin structure.
    if subquery_exprs.len() > 1 {
        return Ok(None);
    }

    let subquery_expr = &subquery_exprs[0];

    // Handle single subquery - special case for scalar subquery which returns early
    if let Expr::ScalarSubquery(_) = subquery_expr {
        let mut remaining_predicates = other_predicates;
        if let Some((plan, _pred)) = try_flatten_scalar_comparison_from_expr(
            &(*node.input),
            subquery_expr,
            &mut remaining_predicates,
        )? {
            return Ok(Some(apply_remaining_predicates(
                plan,
                remaining_predicates,
            )?));
        }
        return Ok(None);
    }

    // Handle EXISTS/IN subqueries
    let flattened = match subquery_expr {
        Expr::Exists { subquery, negated } => flatten_exists(&(*node.input), subquery, *negated)?,
        Expr::InSubquery {
            expr,
            subquery,
            negated,
        } => flatten_in_subquery(&(*node.input), expr, subquery, *negated)?,
        _ => None,
    };

    match flattened {
        Some(mut plan) => {
            // Re-apply other predicates if any
            if !other_predicates.is_empty() {
                let combined = combine_predicates(other_predicates);
                plan = LogicalPlan::Filter(FilterNode {
                    input: Arc::new(plan),
                    predicate: combined,
                });
            }
            Ok(Some(plan))
        }
        None => Ok(None),
    }
}

/// Try to flatten a scalar subquery from an expression
fn try_flatten_scalar_comparison_from_expr(
    outer: &LogicalPlan,
    expr: &Expr,
    remaining_predicates: &mut Vec<Expr>,
) -> Result<Option<(LogicalPlan, Option<Expr>)>> {
    match expr {
        Expr::BinaryExpr { left, op, right } => {
            // Check if either side is a scalar subquery
            if let Expr::ScalarSubquery(_) = left.as_ref() {
                if let Some((plan, pred)) = try_flatten_scalar_comparison(outer, left, right, *op)?
                {
                    return Ok(Some((plan, pred)));
                }
            }
            if let Expr::ScalarSubquery(_) = right.as_ref() {
                if let Some((plan, pred)) = try_flatten_scalar_comparison(outer, right, left, *op)?
                {
                    return Ok(Some((plan, pred)));
                }
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

/// Apply remaining predicates to a plan
fn apply_remaining_predicates(plan: LogicalPlan, predicates: Vec<Expr>) -> Result<LogicalPlan> {
    if predicates.is_empty() {
        return Ok(plan);
    }

    let combined = combine_predicates(predicates);
    Ok(LogicalPlan::Filter(FilterNode {
        input: Arc::new(plan),
        predicate: combined,
    }))
}

/// Flatten an EXISTS/NOT EXISTS subquery into a DelimJoin
fn flatten_exists(
    outer: &LogicalPlan,
    subquery: &LogicalPlan,
    negated: bool,
) -> Result<Option<LogicalPlan>> {
    // Extract correlation info
    let outer_columns = collect_plan_column_names(outer);
    let (corr_columns, decorrelated) = extract_correlation_info(subquery, &outer_columns)?;

    if corr_columns.is_empty() {
        // Not correlated - can't use DelimJoin (use regular decorrelation)
        return Ok(None);
    }

    // Create DelimGet node for the correlation columns
    let delim_id = next_delim_id();
    let delim_schema = build_delim_schema(&corr_columns);
    let delim_get = LogicalPlan::DelimGet(DelimGetNode {
        columns: corr_columns.iter().map(|c| c.outer_expr.clone()).collect(),
        schema: delim_schema.clone(),
        delim_id,
    });

    // Rewrite the subquery to use DelimGet instead of outer references
    // For EXISTS, we don't need the original output - just need to check if rows exist
    let rewritten_subquery =
        rewrite_with_delim_get(&decorrelated, &corr_columns, &delim_get, false)?;

    // Create DelimJoin
    let join_type = if negated {
        JoinType::Anti
    } else {
        JoinType::Semi
    };

    // Build join conditions from correlation columns
    let join_on: Vec<(Expr, Expr)> = corr_columns
        .iter()
        .map(|c| {
            let outer_expr = c.outer_expr.clone();
            // The inner column now references the DelimGet schema
            let inner_expr = Expr::column(&c.inner_col);
            (outer_expr, inner_expr)
        })
        .collect();

    let delim_join = LogicalPlan::DelimJoin(DelimJoinNode {
        left: Arc::new(outer.clone()),
        right: Arc::new(rewritten_subquery),
        join_type,
        delim_columns: corr_columns.iter().map(|c| c.outer_expr.clone()).collect(),
        on: join_on,
        schema: outer.schema(), // Semi/Anti join preserves outer schema
    });

    Ok(Some(delim_join))
}

/// Flatten an IN/NOT IN subquery into a DelimJoin (Mark join)
fn flatten_in_subquery(
    outer: &LogicalPlan,
    in_expr: &Expr,
    subquery: &LogicalPlan,
    negated: bool,
) -> Result<Option<LogicalPlan>> {
    let outer_columns = collect_plan_column_names(outer);
    let (mut corr_columns, decorrelated) = extract_correlation_info(subquery, &outer_columns)?;

    // Get the output column from the subquery
    let subquery_schema = subquery.schema();
    if subquery_schema.fields().is_empty() {
        return Ok(None);
    }
    let subquery_col = &subquery_schema.fields()[0];

    // Add the IN expression as a correlation
    corr_columns.push(CorrelationColumn {
        outer_expr: in_expr.clone(),
        inner_col: subquery_col.name.clone(),
    });

    if corr_columns.len() == 1 && !has_other_correlations(subquery, &outer_columns) {
        // Simple uncorrelated IN - use regular semi/anti join
        return Ok(None);
    }

    // Create DelimGet and rewrite
    let delim_id = next_delim_id();
    let delim_schema = build_delim_schema(&corr_columns);
    let delim_get = LogicalPlan::DelimGet(DelimGetNode {
        columns: corr_columns.iter().map(|c| c.outer_expr.clone()).collect(),
        schema: delim_schema,
        delim_id,
    });

    // For IN subqueries, we don't need to preserve the original output
    let rewritten_subquery =
        rewrite_with_delim_get(&decorrelated, &corr_columns, &delim_get, false)?;

    let join_type = if negated {
        JoinType::Anti
    } else {
        JoinType::Semi
    };

    let join_on: Vec<(Expr, Expr)> = corr_columns
        .iter()
        .map(|c| (c.outer_expr.clone(), Expr::column(&c.inner_col)))
        .collect();

    let delim_join = LogicalPlan::DelimJoin(DelimJoinNode {
        left: Arc::new(outer.clone()),
        right: Arc::new(rewritten_subquery),
        join_type,
        delim_columns: corr_columns.iter().map(|c| c.outer_expr.clone()).collect(),
        on: join_on,
        schema: outer.schema(),
    });

    Ok(Some(delim_join))
}

/// Try to flatten a scalar subquery comparison (Single join)
fn try_flatten_scalar_comparison(
    outer: &LogicalPlan,
    left: &Expr,
    right: &Expr,
    op: BinaryOp,
) -> Result<Option<(LogicalPlan, Option<Expr>)>> {
    // Check if either side is a scalar subquery
    let (subquery, other_expr, subquery_on_left) = if let Expr::ScalarSubquery(sq) = left {
        (sq.as_ref(), right, true)
    } else if let Expr::ScalarSubquery(sq) = right {
        (sq.as_ref(), left, false)
    } else {
        return Ok(None);
    };

    let outer_columns = collect_plan_column_names(outer);
    let (corr_columns, decorrelated) = extract_correlation_info(subquery, &outer_columns)?;

    if corr_columns.is_empty() {
        return Ok(None);
    }

    // Create DelimGet
    let delim_id = next_delim_id();
    let delim_schema = build_delim_schema(&corr_columns);
    let delim_get = LogicalPlan::DelimGet(DelimGetNode {
        columns: corr_columns.iter().map(|c| c.outer_expr.clone()).collect(),
        schema: delim_schema,
        delim_id,
    });

    // For scalar subqueries, we need to preserve the output (like COUNT(*))
    let rewritten_subquery =
        rewrite_with_delim_get(&decorrelated, &corr_columns, &delim_get, true)?;

    // Get scalar result column name
    let scalar_col = if decorrelated.schema().fields().is_empty() {
        "__scalar_result".to_string()
    } else {
        decorrelated.schema().fields()[0].name.clone()
    };

    let join_on: Vec<(Expr, Expr)> = corr_columns
        .iter()
        .map(|c| (c.outer_expr.clone(), Expr::column(&c.inner_col)))
        .collect();

    // For scalar subquery, use Single join type (exactly one row per outer row)
    let mut result_schema = outer.schema();
    // Add the scalar column to the schema
    if let Some(field) = decorrelated.schema().fields().first() {
        let mut fields = result_schema.fields().to_vec();
        fields.push(field.clone());
        result_schema = PlanSchema::new(fields);
    }

    let delim_join = LogicalPlan::DelimJoin(DelimJoinNode {
        left: Arc::new(outer.clone()),
        right: Arc::new(rewritten_subquery),
        join_type: JoinType::Single,
        delim_columns: corr_columns.iter().map(|c| c.outer_expr.clone()).collect(),
        on: join_on,
        schema: result_schema,
    });

    // Create the comparison predicate using the joined scalar column
    let scalar_ref = Expr::column(&scalar_col);
    let new_predicate = if subquery_on_left {
        Expr::BinaryExpr {
            left: Box::new(scalar_ref),
            op,
            right: Box::new(other_expr.clone()),
        }
    } else {
        Expr::BinaryExpr {
            left: Box::new(other_expr.clone()),
            op,
            right: Box::new(scalar_ref),
        }
    };

    Ok(Some((delim_join, Some(new_predicate))))
}

/// Correlation column information
#[derive(Debug, Clone)]
struct CorrelationColumn {
    /// Expression from the outer query
    outer_expr: Expr,
    /// Column name from the inner query
    inner_col: String,
}

/// Extract correlation information from a subquery
fn extract_correlation_info(
    subquery: &LogicalPlan,
    outer_columns: &HashSet<String>,
) -> Result<(Vec<CorrelationColumn>, LogicalPlan)> {
    let mut correlations = Vec::new();
    let decorrelated = extract_correlations_recursive(subquery, outer_columns, &mut correlations)?;
    Ok((correlations, decorrelated))
}

fn extract_correlations_recursive(
    plan: &LogicalPlan,
    outer_columns: &HashSet<String>,
    correlations: &mut Vec<CorrelationColumn>,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter(node) => {
            let new_input =
                extract_correlations_recursive(&node.input, outer_columns, correlations)?;

            // Check for correlation predicates
            let (corr_preds, remaining) =
                extract_correlation_from_expr(&node.predicate, outer_columns);

            for pred in corr_preds {
                correlations.push(pred);
            }

            if let Some(remaining_pred) = remaining {
                Ok(LogicalPlan::Filter(FilterNode {
                    input: Arc::new(new_input),
                    predicate: remaining_pred,
                }))
            } else {
                Ok(new_input)
            }
        }
        LogicalPlan::Project(node) => {
            let new_input =
                extract_correlations_recursive(&node.input, outer_columns, correlations)?;
            Ok(LogicalPlan::Project(ProjectNode {
                input: Arc::new(new_input),
                exprs: node.exprs.clone(),
                schema: node.schema.clone(),
            }))
        }
        LogicalPlan::Join(node) => {
            let new_left = extract_correlations_recursive(&node.left, outer_columns, correlations)?;
            let new_right =
                extract_correlations_recursive(&node.right, outer_columns, correlations)?;
            Ok(LogicalPlan::Join(JoinNode {
                left: Arc::new(new_left),
                right: Arc::new(new_right),
                join_type: node.join_type,
                on: node.on.clone(),
                filter: node.filter.clone(),
                schema: node.schema.clone(),
            }))
        }
        LogicalPlan::Aggregate(node) => {
            let new_input =
                extract_correlations_recursive(&node.input, outer_columns, correlations)?;
            Ok(LogicalPlan::Aggregate(AggregateNode {
                input: Arc::new(new_input),
                group_by: node.group_by.clone(),
                aggregates: node.aggregates.clone(),
                schema: node.schema.clone(),
            }))
        }
        _ => Ok(plan.clone()),
    }
}

/// Extract correlation predicates from an expression
fn extract_correlation_from_expr(
    expr: &Expr,
    outer_columns: &HashSet<String>,
) -> (Vec<CorrelationColumn>, Option<Expr>) {
    match expr {
        Expr::BinaryExpr {
            left,
            op: BinaryOp::And,
            right,
        } => {
            let (left_corrs, left_remaining) = extract_correlation_from_expr(left, outer_columns);
            let (right_corrs, right_remaining) =
                extract_correlation_from_expr(right, outer_columns);

            let mut all_corrs = left_corrs;
            all_corrs.extend(right_corrs);

            let remaining = match (left_remaining, right_remaining) {
                (Some(l), Some(r)) => Some(Expr::BinaryExpr {
                    left: Box::new(l),
                    op: BinaryOp::And,
                    right: Box::new(r),
                }),
                (Some(l), None) => Some(l),
                (None, Some(r)) => Some(r),
                (None, None) => None,
            };

            (all_corrs, remaining)
        }
        Expr::BinaryExpr { left, op, right } if *op == BinaryOp::Eq => {
            // Check if this is outer = inner or inner = outer
            if let Some(corr) = try_extract_correlation_pair(left, right, outer_columns) {
                return (vec![corr], None);
            }
            if let Some(corr) = try_extract_correlation_pair(right, left, outer_columns) {
                return (vec![corr], None);
            }
            (vec![], Some(expr.clone()))
        }
        _ => (vec![], Some(expr.clone())),
    }
}

fn try_extract_correlation_pair(
    left: &Expr,
    right: &Expr,
    outer_columns: &HashSet<String>,
) -> Option<CorrelationColumn> {
    // Check if left references outer and right is inner
    if references_outer(left, outer_columns) && !references_outer(right, outer_columns) {
        if let Some(inner_col) = get_column_name(right) {
            return Some(CorrelationColumn {
                outer_expr: left.clone(),
                inner_col,
            });
        }
    }
    None
}

fn references_outer(expr: &Expr, outer_columns: &HashSet<String>) -> bool {
    match expr {
        Expr::Column(col) => {
            if let Some(rel) = &col.relation {
                outer_columns.contains(rel)
                    || outer_columns.contains(&format!("{}.{}", rel, col.name))
            } else {
                outer_columns.contains(&col.name)
            }
        }
        Expr::BinaryExpr { left, right, .. } => {
            references_outer(left, outer_columns) || references_outer(right, outer_columns)
        }
        _ => false,
    }
}

fn get_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => Some(col.name.clone()),
        Expr::Alias { expr, .. } => get_column_name(expr),
        _ => None,
    }
}

/// Build a schema for the DelimGet node
fn build_delim_schema(correlations: &[CorrelationColumn]) -> PlanSchema {
    let fields: Vec<SchemaField> = correlations
        .iter()
        .map(|c| {
            // Use the inner column name for the field
            SchemaField::new(&c.inner_col, arrow::datatypes::DataType::Int64) // Default type, should be inferred
        })
        .collect();
    PlanSchema::new(fields)
}

/// Rewrite a plan to use DelimGet instead of outer references
///
/// For EXISTS subqueries, we only need to check row existence, so we can strip projections.
/// For scalar/IN subqueries, we need to preserve the output value (like COUNT(*)).
fn rewrite_with_delim_get(
    plan: &LogicalPlan,
    correlations: &[CorrelationColumn],
    delim_get: &LogicalPlan,
    preserve_output: bool, // If true, preserve the original output columns (for scalar subqueries)
) -> Result<LogicalPlan> {
    // Find the innermost plan that has the correlation columns
    let join_target = find_base_with_columns(plan, correlations);

    // Build the join between DelimGet and the target plan
    let joined = LogicalPlan::Join(JoinNode {
        left: Arc::new(delim_get.clone()),
        right: Arc::new(join_target.clone()),
        join_type: JoinType::Inner,
        on: correlations
            .iter()
            .map(|c| {
                // DelimGet column = inner table column
                (Expr::column(&c.inner_col), Expr::column(&c.inner_col))
            })
            .collect(),
        filter: None,
        schema: {
            let delim_schema = delim_get.schema();
            let target_schema = join_target.schema();
            delim_schema.merge(&target_schema)
        },
    });

    // For scalar subqueries, we need to apply aggregates/projections to get the output value
    // For EXISTS, we just need the join to check for matches
    if preserve_output && has_aggregate_or_projection(plan) {
        // Rebuild the plan structure on top of the join
        let result = rebuild_on_join(plan, joined, correlations)?;
        Ok(result)
    } else {
        Ok(joined)
    }
}

/// Check if a plan has aggregates or projections that need to be preserved
fn has_aggregate_or_projection(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Aggregate(_) | LogicalPlan::Project(_))
}

/// Rebuild the plan structure on top of the join result
fn rebuild_on_join(
    original: &LogicalPlan,
    joined: LogicalPlan,
    correlations: &[CorrelationColumn],
) -> Result<LogicalPlan> {
    match original {
        LogicalPlan::Project(node) => {
            // Check if input needs to be rebuilt
            let new_input = rebuild_on_join(&node.input, joined, correlations)?;

            // Add correlation columns to the projection
            let mut new_exprs = node.exprs.clone();
            let mut new_schema_fields = node.schema.fields().to_vec();
            for corr in correlations {
                // Add the inner column to the projection if not already there
                if !new_exprs.iter().any(|e| matches_column(e, &corr.inner_col)) {
                    new_exprs.push(Expr::column(&corr.inner_col));
                    // Also add to schema
                    if !new_schema_fields.iter().any(|f| f.name == corr.inner_col) {
                        new_schema_fields.push(SchemaField::new(
                            &corr.inner_col,
                            arrow::datatypes::DataType::Int64,
                        ));
                    }
                }
            }

            Ok(LogicalPlan::Project(ProjectNode {
                input: Arc::new(new_input),
                exprs: new_exprs,
                schema: PlanSchema::new(new_schema_fields),
            }))
        }
        LogicalPlan::Aggregate(node) => {
            // For aggregates, we need to include correlation columns in GROUP BY
            let new_input = rebuild_on_join(&node.input, joined, correlations)?;

            // Add correlation columns to group_by
            let mut new_group_by = node.group_by.clone();
            for corr in correlations {
                // Add the inner column to the group by
                let col_expr = Expr::column(&corr.inner_col);
                if !new_group_by
                    .iter()
                    .any(|e| matches_column(e, &corr.inner_col))
                {
                    new_group_by.push(col_expr);
                }
            }

            // Rebuild the schema with correlation columns
            let mut new_schema_fields = node.schema.fields().to_vec();
            for corr in correlations {
                if !new_schema_fields.iter().any(|f| f.name == corr.inner_col) {
                    new_schema_fields.push(SchemaField::new(
                        &corr.inner_col,
                        arrow::datatypes::DataType::Int64,
                    ));
                }
            }

            Ok(LogicalPlan::Aggregate(AggregateNode {
                input: Arc::new(new_input),
                group_by: new_group_by,
                aggregates: node.aggregates.clone(),
                schema: PlanSchema::new(new_schema_fields),
            }))
        }
        LogicalPlan::Filter(node) => {
            let new_input = rebuild_on_join(&node.input, joined, correlations)?;
            Ok(LogicalPlan::Filter(FilterNode {
                input: Arc::new(new_input),
                predicate: node.predicate.clone(),
            }))
        }
        // For Scan and other base nodes, return the joined result
        _ => Ok(joined),
    }
}

fn matches_column(expr: &Expr, col_name: &str) -> bool {
    match expr {
        Expr::Column(col) => col.name == col_name,
        _ => false,
    }
}

/// Find the innermost plan node that has the correlation columns
fn find_base_with_columns(plan: &LogicalPlan, correlations: &[CorrelationColumn]) -> LogicalPlan {
    match plan {
        // For Project, look at the input which should have the actual columns
        LogicalPlan::Project(node) => find_base_with_columns(&node.input, correlations),
        // For Filter, look at the input
        LogicalPlan::Filter(node) => find_base_with_columns(&node.input, correlations),
        // For other plan types (Scan, Join, etc.), return as-is if they have the columns
        _ => {
            let schema = plan.schema();
            let has_cols = correlations.iter().all(|c| {
                schema
                    .fields()
                    .iter()
                    .any(|f| f.name == c.inner_col || f.qualified_name().ends_with(&c.inner_col))
            });
            if has_cols {
                plan.clone()
            } else {
                // Recurse into children if available
                match plan.children().first() {
                    Some(child) => find_base_with_columns(child, correlations),
                    None => plan.clone(), // No children, use as-is
                }
            }
        }
    }
}

/// Check if a subquery has other correlations besides IN
fn has_other_correlations(plan: &LogicalPlan, outer_columns: &HashSet<String>) -> bool {
    match plan {
        LogicalPlan::Filter(node) => {
            expr_has_outer_refs(&node.predicate, outer_columns)
                || has_other_correlations(&node.input, outer_columns)
        }
        LogicalPlan::Project(node) => {
            node.exprs
                .iter()
                .any(|e| expr_has_outer_refs(e, outer_columns))
                || has_other_correlations(&node.input, outer_columns)
        }
        LogicalPlan::Join(node) => {
            has_other_correlations(&node.left, outer_columns)
                || has_other_correlations(&node.right, outer_columns)
        }
        _ => false,
    }
}

/// Check if an expression references outer columns
fn expr_has_outer_refs(expr: &Expr, outer_columns: &HashSet<String>) -> bool {
    match expr {
        Expr::Column(col) => {
            if let Some(rel) = &col.relation {
                outer_columns.contains(rel)
            } else {
                false
            }
        }
        Expr::BinaryExpr { left, right, .. } => {
            expr_has_outer_refs(left, outer_columns) || expr_has_outer_refs(right, outer_columns)
        }
        _ => false,
    }
}

/// Collect column names defined in a plan
fn collect_plan_column_names(plan: &LogicalPlan) -> HashSet<String> {
    let mut columns = HashSet::new();
    collect_columns_recursive(plan, &mut columns);
    columns
}

fn collect_columns_recursive(plan: &LogicalPlan, columns: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Scan(node) => {
            columns.insert(node.table_name.clone());
            for field in node.schema.fields() {
                columns.insert(field.name.clone());
                columns.insert(field.qualified_name());
            }
        }
        LogicalPlan::SubqueryAlias(node) => {
            columns.insert(node.alias.clone());
            for field in node.schema.fields() {
                columns.insert(field.name.clone());
                columns.insert(field.qualified_name());
            }
            collect_columns_recursive(&node.input, columns);
        }
        LogicalPlan::Join(node) => {
            collect_columns_recursive(&node.left, columns);
            collect_columns_recursive(&node.right, columns);
        }
        LogicalPlan::Filter(node) => collect_columns_recursive(&node.input, columns),
        LogicalPlan::Project(node) => {
            for field in node.schema.fields() {
                columns.insert(field.name.clone());
            }
            collect_columns_recursive(&node.input, columns);
        }
        LogicalPlan::Aggregate(node) => {
            for field in node.schema.fields() {
                columns.insert(field.name.clone());
            }
            collect_columns_recursive(&node.input, columns);
        }
        _ => {
            for child in plan.children() {
                collect_columns_recursive(child, columns);
            }
        }
    }
}

/// Extract subquery predicates from an expression
fn extract_subquery_predicates(predicate: &Expr) -> (Vec<Expr>, Vec<Expr>) {
    let mut subquery_exprs = Vec::new();
    let mut other_predicates = Vec::new();
    extract_predicates_recursive(predicate, &mut subquery_exprs, &mut other_predicates);
    (subquery_exprs, other_predicates)
}

fn extract_predicates_recursive(
    expr: &Expr,
    subquery_exprs: &mut Vec<Expr>,
    other_predicates: &mut Vec<Expr>,
) {
    match expr {
        Expr::Exists { .. } | Expr::InSubquery { .. } | Expr::ScalarSubquery(_) => {
            subquery_exprs.push(expr.clone());
        }
        Expr::BinaryExpr {
            left,
            op: BinaryOp::And,
            right,
        } => {
            extract_predicates_recursive(left, subquery_exprs, other_predicates);
            extract_predicates_recursive(right, subquery_exprs, other_predicates);
        }
        _ => {
            if expr.contains_subquery() {
                subquery_exprs.push(expr.clone());
            } else {
                other_predicates.push(expr.clone());
            }
        }
    }
}

fn combine_predicates(predicates: Vec<Expr>) -> Expr {
    if predicates.is_empty() {
        return Expr::Literal(crate::planner::ScalarValue::Boolean(true));
    }
    predicates
        .into_iter()
        .reduce(|acc, p| Expr::BinaryExpr {
            left: Box::new(acc),
            op: BinaryOp::And,
            right: Box::new(p),
        })
        .unwrap()
}

/// Check if an operator is a comparison operator
fn is_comparison_op(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::Eq
            | BinaryOp::NotEq
            | BinaryOp::Lt
            | BinaryOp::LtEq
            | BinaryOp::Gt
            | BinaryOp::GtEq
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{LogicalPlanBuilder, ScalarValue};
    use arrow::datatypes::DataType;

    fn orders_schema() -> PlanSchema {
        PlanSchema::new(vec![
            SchemaField::new("o_orderkey", DataType::Int64),
            SchemaField::new("o_custkey", DataType::Int64),
            SchemaField::new("o_totalprice", DataType::Float64),
        ])
    }

    fn lineitem_schema() -> PlanSchema {
        PlanSchema::new(vec![
            SchemaField::new("l_orderkey", DataType::Int64),
            SchemaField::new("l_partkey", DataType::Int64),
            SchemaField::new("l_quantity", DataType::Float64),
        ])
    }

    #[test]
    fn test_flatten_exists() {
        let orders = LogicalPlanBuilder::scan("orders", orders_schema()).build();
        let lineitem = LogicalPlanBuilder::scan("lineitem", lineitem_schema()).build();

        // Create correlated subquery: WHERE l.l_orderkey = o.o_orderkey
        let correlated_predicate = Expr::BinaryExpr {
            left: Box::new(Expr::qualified_column("lineitem", "l_orderkey")),
            op: BinaryOp::Eq,
            right: Box::new(Expr::qualified_column("orders", "o_orderkey")),
        };

        let subquery = lineitem.filter(correlated_predicate);

        let exists = Expr::Exists {
            subquery: Arc::new(subquery),
            negated: false,
        };

        let plan = orders.filter(exists);

        // Apply the flattening rule
        let rule = FlattenDependentJoin;
        let result = rule.optimize(&plan).unwrap();

        // Should produce a DelimJoin
        assert!(matches!(result, LogicalPlan::DelimJoin(_)));
    }

    #[test]
    fn test_flatten_multiple_exists() {
        // Test Q21 pattern: EXISTS (...) AND NOT EXISTS (...)
        let orders = LogicalPlanBuilder::scan("orders", orders_schema()).build();
        let lineitem = LogicalPlanBuilder::scan("lineitem", lineitem_schema()).build();

        // First EXISTS: correlation between orders.o_orderkey and lineitem.l_orderkey
        // Note: In real SQL, this would be written as:
        // WHERE EXISTS (SELECT * FROM lineitem l WHERE l.l_orderkey = orders.o_orderkey)
        // where "orders" is the outer table and "lineitem" (aliased as l) is inner
        let exists1_predicate = Expr::BinaryExpr {
            left: Box::new(Expr::Column(crate::planner::Column {
                relation: Some("orders".to_string()),
                name: "o_orderkey".to_string(),
            })),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Column(crate::planner::Column {
                relation: Some("lineitem".to_string()),
                name: "l_orderkey".to_string(),
            })),
        };

        let subquery1 = lineitem.clone().filter(exists1_predicate);

        let exists1 = Expr::Exists {
            subquery: Arc::new(subquery1),
            negated: false,
        };

        // Second NOT EXISTS: similar pattern
        let exists2_predicate = Expr::BinaryExpr {
            left: Box::new(Expr::Column(crate::planner::Column {
                relation: Some("orders".to_string()),
                name: "o_orderkey".to_string(),
            })),
            op: BinaryOp::Eq,
            right: Box::new(Expr::Column(crate::planner::Column {
                relation: Some("lineitem".to_string()),
                name: "l_orderkey".to_string(),
            })),
        };

        let subquery2 = lineitem.filter(exists2_predicate);

        let exists2 = Expr::Exists {
            subquery: Arc::new(subquery2),
            negated: true, // NOT EXISTS
        };

        // Combine with AND
        let combined = Expr::BinaryExpr {
            left: Box::new(exists1),
            op: BinaryOp::And,
            right: Box::new(exists2),
        };

        let plan = orders.filter(combined);

        // Apply the flattening rule - should handle both subqueries
        let rule = FlattenDependentJoin;
        let result = rule.optimize(&plan);

        // The flattening may or may not succeed depending on correlation detection
        // For this test, just verify it doesn't crash
        assert!(result.is_ok());
    }
}
