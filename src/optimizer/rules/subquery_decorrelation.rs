//! Subquery decorrelation optimizer rule
//!
//! Transforms correlated subqueries into joins for O(n) instead of O(n²) execution.
//!
//! Transformations:
//! - EXISTS subquery → Semi Join
//! - NOT EXISTS subquery → Anti Join
//! - IN subquery → Semi Join
//! - NOT IN subquery → Anti Join
//! - Scalar subquery → Left Join with aggregation

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::planner::{
    AggregateNode, BinaryOp, DistinctNode, Expr, FilterNode, JoinNode, JoinType, LimitNode,
    LogicalPlan, PlanSchema, ProjectNode, SortNode, SubqueryAliasNode,
};
use std::collections::HashSet;
use std::sync::Arc;

/// Subquery decorrelation rule
pub struct SubqueryDecorrelation;

impl OptimizerRule for SubqueryDecorrelation {
    fn name(&self) -> &str {
        "SubqueryDecorrelation"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        decorrelate_plan(plan)
    }
}

/// Recursively decorrelate subqueries in a plan
fn decorrelate_plan(plan: &LogicalPlan) -> Result<LogicalPlan> {
    // First, recursively process children
    let plan = match plan {
        LogicalPlan::Filter(node) => {
            let new_input = decorrelate_plan(&node.input)?;
            LogicalPlan::Filter(FilterNode {
                input: Arc::new(new_input),
                predicate: node.predicate.clone(),
            })
        }
        LogicalPlan::Project(node) => {
            let new_input = decorrelate_plan(&node.input)?;
            LogicalPlan::Project(ProjectNode {
                input: Arc::new(new_input),
                exprs: node.exprs.clone(),
                schema: node.schema.clone(),
            })
        }
        LogicalPlan::Join(node) => {
            let new_left = decorrelate_plan(&node.left)?;
            let new_right = decorrelate_plan(&node.right)?;
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
            let new_input = decorrelate_plan(&node.input)?;
            LogicalPlan::Aggregate(AggregateNode {
                input: Arc::new(new_input),
                group_by: node.group_by.clone(),
                aggregates: node.aggregates.clone(),
                schema: node.schema.clone(),
            })
        }
        LogicalPlan::Sort(node) => {
            let new_input = decorrelate_plan(&node.input)?;
            LogicalPlan::Sort(SortNode {
                input: Arc::new(new_input),
                order_by: node.order_by.clone(),
            })
        }
        LogicalPlan::Limit(node) => {
            let new_input = decorrelate_plan(&node.input)?;
            LogicalPlan::Limit(LimitNode {
                input: Arc::new(new_input),
                skip: node.skip,
                fetch: node.fetch,
            })
        }
        LogicalPlan::Distinct(node) => {
            let new_input = decorrelate_plan(&node.input)?;
            LogicalPlan::Distinct(DistinctNode {
                input: Arc::new(new_input),
            })
        }
        LogicalPlan::SubqueryAlias(node) => {
            let new_input = decorrelate_plan(&node.input)?;
            LogicalPlan::SubqueryAlias(SubqueryAliasNode {
                input: Arc::new(new_input),
                alias: node.alias.clone(),
                schema: node.schema.clone(),
            })
        }
        _ => plan.clone(),
    };

    // Now try to decorrelate subqueries in Filter nodes
    if let LogicalPlan::Filter(node) = &plan {
        if let Some(decorrelated) = try_decorrelate_filter(node)? {
            return Ok(decorrelated);
        }
    }

    Ok(plan)
}

/// Try to decorrelate a filter with subquery predicates
fn try_decorrelate_filter(node: &FilterNode) -> Result<Option<LogicalPlan>> {
    // Check if the predicate contains a correlated subquery
    let (subquery_exprs, other_predicates) = extract_subquery_predicates(&node.predicate);

    if subquery_exprs.is_empty() {
        return Ok(None);
    }

    let mut current_plan = (*node.input).clone();
    let mut unhandled_subquery_exprs = Vec::new();
    let mut any_decorrelated = false;

    for subquery_expr in subquery_exprs {
        match &subquery_expr {
            Expr::Exists { subquery, negated } => {
                if let Some(decorrelated) = decorrelate_exists(&current_plan, subquery, *negated)? {
                    current_plan = decorrelated;
                    any_decorrelated = true;
                } else {
                    // Couldn't decorrelate - need to re-apply as filter
                    unhandled_subquery_exprs.push(subquery_expr.clone());
                }
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                if let Some(decorrelated) =
                    decorrelate_in_subquery(&current_plan, expr, subquery, *negated)?
                {
                    current_plan = decorrelated;
                    any_decorrelated = true;
                } else {
                    // Couldn't decorrelate - need to re-apply as filter
                    unhandled_subquery_exprs.push(subquery_expr.clone());
                }
            }
            // Handle comparisons involving scalar subqueries: expr op (SELECT ...)
            Expr::BinaryExpr { left, op, right } if is_comparison_op(*op) => {
                // Check if one side is a scalar subquery
                if let Some((decorrelated, new_predicate)) =
                    try_decorrelate_scalar_comparison(&current_plan, left, right, *op)?
                {
                    current_plan = decorrelated;
                    any_decorrelated = true;
                    // Add the transformed predicate
                    unhandled_subquery_exprs.push(new_predicate);
                } else {
                    unhandled_subquery_exprs.push(subquery_expr.clone());
                }
            }
            _ => {
                // Other expressions with subqueries can't be decorrelated
                unhandled_subquery_exprs.push(subquery_expr.clone());
            }
        }
    }

    // If nothing was decorrelated, don't transform the plan
    if !any_decorrelated {
        return Ok(None);
    }

    // Re-apply unhandled subquery expressions and other predicates
    let mut remaining_predicates = unhandled_subquery_exprs;
    remaining_predicates.extend(other_predicates);

    if !remaining_predicates.is_empty() {
        let combined_predicate = combine_predicates(remaining_predicates);
        current_plan = LogicalPlan::Filter(FilterNode {
            input: Arc::new(current_plan),
            predicate: combined_predicate,
        });
    }

    Ok(Some(current_plan))
}

/// Extract subquery expressions from a predicate
fn extract_subquery_predicates(predicate: &Expr) -> (Vec<Expr>, Vec<Expr>) {
    let mut subquery_exprs = Vec::new();
    let mut other_predicates = Vec::new();

    extract_subquery_predicates_recursive(predicate, &mut subquery_exprs, &mut other_predicates);

    (subquery_exprs, other_predicates)
}

fn extract_subquery_predicates_recursive(
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
            extract_subquery_predicates_recursive(left, subquery_exprs, other_predicates);
            extract_subquery_predicates_recursive(right, subquery_exprs, other_predicates);
        }
        _ => {
            // Check if this expression contains a subquery
            if expr.contains_subquery() {
                subquery_exprs.push(expr.clone());
            } else {
                other_predicates.push(expr.clone());
            }
        }
    }
}

/// Combine multiple predicates with AND
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

/// Decorrelate an EXISTS subquery into a Semi/Anti Join
fn decorrelate_exists(
    outer: &LogicalPlan,
    subquery: &LogicalPlan,
    negated: bool,
) -> Result<Option<LogicalPlan>> {
    // Extract correlation predicates from the subquery
    let (correlation_predicates, decorrelated_subquery) =
        extract_correlation_predicates(subquery, outer)?;

    if correlation_predicates.is_empty() {
        // Not a correlated subquery - can't decorrelate
        return Ok(None);
    }

    // For EXISTS, strip top-level projections since we don't care about the output,
    // we only need the base tables to be available for the join condition
    let join_right = strip_projection(&decorrelated_subquery);

    // Separate equality predicates (for join ON) from non-equality (for join filter)
    let (eq_predicates, non_eq_predicates): (Vec<_>, Vec<_>) = correlation_predicates
        .into_iter()
        .partition(|p| p.op == BinaryOp::Eq);

    // Build the equi-join conditions
    let join_on = build_join_conditions(&eq_predicates, outer, &join_right)?;

    if join_on.is_empty() {
        // Couldn't build equi-join conditions
        return Ok(None);
    }

    // Build join filter from non-equality predicates
    let join_filter = if non_eq_predicates.is_empty() {
        None
    } else {
        let filter_exprs: Vec<Expr> = non_eq_predicates
            .iter()
            .filter_map(|pred| build_filter_expr(pred, outer, &join_right))
            .collect();

        if filter_exprs.is_empty() {
            None
        } else {
            Some(combine_predicates(filter_exprs))
        }
    };

    // Create the appropriate join type
    let join_type = if negated {
        JoinType::Anti
    } else {
        JoinType::Semi
    };

    // Build the join schema (for Semi/Anti, it's just the left schema)
    let schema = outer.schema();

    let join = LogicalPlan::Join(JoinNode {
        left: Arc::new(outer.clone()),
        right: Arc::new(join_right),
        join_type,
        on: join_on,
        filter: join_filter,
        schema,
    });

    Ok(Some(join))
}

/// Build a filter expression from a non-equality correlation predicate
fn build_filter_expr(
    pred: &CorrelationPredicate,
    outer: &LogicalPlan,
    inner: &LogicalPlan,
) -> Option<Expr> {
    let outer_schema = outer.schema();
    let inner_schema = inner.schema();

    // Validate outer expression
    if !expr_references_schema(&pred.outer_expr, &outer_schema) {
        return None;
    }

    // Find inner column
    let inner_col_name = &pred.inner_col;
    let unqualified_name = if let Some(dot_pos) = inner_col_name.find('.') {
        &inner_col_name[dot_pos + 1..]
    } else {
        inner_col_name.as_str()
    };

    let inner_field = inner_schema.fields().iter().find(|f| {
        f.name == *inner_col_name
            || f.name == unqualified_name
            || inner_col_name.ends_with(&format!(".{}", f.name))
            || f.name.ends_with(&format!(".{}", unqualified_name))
            || f.qualified_name() == *inner_col_name
    });

    let inner_field = inner_field?;

    // Use the relation qualifier if available for proper column resolution
    let inner_expr = if inner_field.relation.is_some() {
        Expr::qualified_column(inner_field.relation.as_ref().unwrap(), &inner_field.name)
    } else {
        Expr::column(&inner_field.name)
    };

    Some(Expr::BinaryExpr {
        left: Box::new(inner_expr),
        op: pred.op,
        right: Box::new(pred.outer_expr.clone()),
    })
}

/// Strip top-level projection from a plan (used for EXISTS subqueries)
fn strip_projection(plan: &LogicalPlan) -> LogicalPlan {
    match plan {
        LogicalPlan::Project(node) => (*node.input).clone(),
        _ => plan.clone(),
    }
}

/// Decorrelate an IN subquery into a Semi/Anti Join
fn decorrelate_in_subquery(
    outer: &LogicalPlan,
    in_expr: &Expr,
    subquery: &LogicalPlan,
    negated: bool,
) -> Result<Option<LogicalPlan>> {
    // Get the output column from the subquery
    let subquery_schema = subquery.schema();
    if subquery_schema.fields().is_empty() {
        return Ok(None);
    }

    let subquery_col = &subquery_schema.fields()[0];

    // Extract correlation predicates
    let (mut correlation_predicates, decorrelated_subquery) =
        extract_correlation_predicates(subquery, outer)?;

    // Add the IN condition as a correlation predicate
    // The IN expr should match the first column of the subquery
    let in_predicate = CorrelationPredicate {
        outer_expr: in_expr.clone(),
        inner_col: subquery_col.name.clone(),
        op: BinaryOp::Eq,
    };
    correlation_predicates.push(in_predicate);

    // Build join conditions
    let join_on = build_join_conditions(&correlation_predicates, outer, &decorrelated_subquery)?;

    if join_on.is_empty() {
        return Ok(None);
    }

    let join_type = if negated {
        JoinType::Anti
    } else {
        JoinType::Semi
    };

    let schema = outer.schema();

    let join = LogicalPlan::Join(JoinNode {
        left: Arc::new(outer.clone()),
        right: Arc::new(decorrelated_subquery),
        join_type,
        on: join_on,
        filter: None,
        schema,
    });

    Ok(Some(join))
}

/// Try to decorrelate a scalar subquery comparison like `expr op (SELECT ...)`
/// Returns (decorrelated plan, new predicate) if successful
fn try_decorrelate_scalar_comparison(
    outer: &LogicalPlan,
    left: &Expr,
    right: &Expr,
    op: BinaryOp,
) -> Result<Option<(LogicalPlan, Expr)>> {
    // Check if left is a scalar subquery
    if let Expr::ScalarSubquery(subquery) = left {
        return decorrelate_scalar_subquery(outer, subquery, right, op, true);
    }

    // Check if right is a scalar subquery
    if let Expr::ScalarSubquery(subquery) = right {
        return decorrelate_scalar_subquery(outer, subquery, left, op, false);
    }

    Ok(None)
}

/// Decorrelate a scalar subquery into a Left Join
/// `subquery_on_left` indicates if the subquery was on the left of the comparison
fn decorrelate_scalar_subquery(
    outer: &LogicalPlan,
    subquery: &LogicalPlan,
    other_expr: &Expr,
    op: BinaryOp,
    subquery_on_left: bool,
) -> Result<Option<(LogicalPlan, Expr)>> {
    // Extract correlation predicates from the subquery
    let (correlation_predicates, decorrelated_subquery) =
        extract_correlation_predicates(subquery, outer)?;

    if correlation_predicates.is_empty() {
        // Not a correlated subquery - can't decorrelate with this method
        return Ok(None);
    }

    // Get the output column from the subquery (this is what the scalar subquery returns)
    let subquery_schema = decorrelated_subquery.schema();
    if subquery_schema.fields().is_empty() {
        return Ok(None);
    }

    // The scalar value column - this is what the subquery computes
    let scalar_col_name = subquery_schema.fields()[0].name.clone();

    // For scalar subqueries, we need to add the correlation columns to the group-by
    // if the subquery has aggregation, to ensure we get one result per outer row
    // This MUST happen before build_join_conditions so the schema includes correlation columns
    let join_right =
        ensure_grouped_by_correlation(&decorrelated_subquery, &correlation_predicates)?;

    // Semi-join reduction: if the outer plan has a filtered small table providing the
    // correlation key, add a Semi Join to reduce the aggregate's input.
    // E.g., for Q17: aggregate scans ALL lineitem (60M rows) but only ~200 filtered parts
    // are relevant. Adding Semi Join(lineitem, Filter(part)) reduces input to ~60K rows.
    let join_right = add_semi_join_reduction(join_right, outer, &correlation_predicates);

    // Build the join conditions using the updated schema
    let join_on = build_join_conditions(&correlation_predicates, outer, &join_right)?;

    if join_on.is_empty() {
        return Ok(None);
    }

    // Get the updated schema from join_right (after ensure_grouped_by_correlation)
    let join_right_schema = join_right.schema();

    // Find the scalar result column - the original scalar column from the subquery
    // After ensure_grouped_by_correlation, correlation columns are prepended
    let scalar_field_idx = join_right_schema
        .fields()
        .iter()
        .position(|f| {
            f.name == scalar_col_name
                || f.name.contains("AVG")
                || f.name.contains("SUM")
                || f.name.contains("COUNT")
                || f.name.contains("MAX")
                || f.name.contains("MIN")
        })
        .unwrap_or(join_right_schema.fields().len() - 1);
    let scalar_field = &join_right_schema.fields()[scalar_field_idx];

    // Create a sanitized name for the result column
    let result_col_name = "__scalar_result".to_string();

    // Wrap join_right with a projection that renames the scalar column to a safe name
    let mut wrapper_exprs = Vec::new();
    let mut wrapper_fields = Vec::new();

    for (i, field) in join_right_schema.fields().iter().enumerate() {
        if i == scalar_field_idx {
            // Rename the scalar result column
            wrapper_exprs.push(Expr::Alias {
                expr: Box::new(Expr::column(&field.name)),
                name: result_col_name.clone(),
            });
            wrapper_fields.push(crate::planner::SchemaField::new(
                &result_col_name,
                field.data_type.clone(),
            ));
        } else {
            // Keep other columns (like correlation columns) as-is
            wrapper_exprs.push(Expr::column(&field.name));
            wrapper_fields.push(field.clone());
        }
    }

    let wrapped_right = LogicalPlan::Project(ProjectNode {
        input: Arc::new(join_right),
        exprs: wrapper_exprs,
        schema: PlanSchema::new(wrapper_fields.clone()),
    });

    // Build the join schema - outer columns + wrapped subquery columns
    let mut join_fields: Vec<crate::planner::SchemaField> = outer.schema().fields().to_vec();
    join_fields.extend(wrapper_fields);
    let join_schema = PlanSchema::new(join_fields);

    // Create Left Join (to preserve outer rows even if subquery has no match)
    let join = LogicalPlan::Join(JoinNode {
        left: Arc::new(outer.clone()),
        right: Arc::new(wrapped_right),
        join_type: JoinType::Left,
        on: join_on,
        filter: None,
        schema: join_schema,
    });

    // Create the new comparison predicate using the join result column
    let scalar_col_expr = Expr::column(&result_col_name);

    let new_predicate = if subquery_on_left {
        Expr::BinaryExpr {
            left: Box::new(scalar_col_expr),
            op,
            right: Box::new(other_expr.clone()),
        }
    } else {
        Expr::BinaryExpr {
            left: Box::new(other_expr.clone()),
            op,
            right: Box::new(scalar_col_expr),
        }
    };

    Ok(Some((join, new_predicate)))
}

/// Ensure the subquery is properly grouped by correlation columns
/// This is needed for scalar subqueries with aggregates to return one row per outer row
fn ensure_grouped_by_correlation(
    subquery: &LogicalPlan,
    correlation_predicates: &[CorrelationPredicate],
) -> Result<LogicalPlan> {
    // Handle Project wrapping Aggregate (common pattern: SELECT expr * AGG(...))
    if let LogicalPlan::Project(proj_node) = subquery {
        if let LogicalPlan::Aggregate(_) = proj_node.input.as_ref() {
            // Recursively process the aggregate
            let new_agg = ensure_grouped_by_correlation(&proj_node.input, correlation_predicates)?;
            let new_agg_schema = new_agg.schema();

            // Update project schema to include correlation columns
            let mut new_proj_fields = Vec::new();

            // Add correlation columns from the aggregate's output
            for pred in correlation_predicates {
                let inner_col = &pred.inner_col;
                let unqualified_name = if let Some(dot_pos) = inner_col.find('.') {
                    &inner_col[dot_pos + 1..]
                } else {
                    inner_col.as_str()
                };

                // Find in the new aggregate schema
                if let Some(field) = new_agg_schema.fields().iter().find(|f| {
                    f.name == *inner_col
                        || f.name == unqualified_name
                        || inner_col.ends_with(&format!(".{}", f.name))
                        || f.name.ends_with(&format!(".{}", unqualified_name))
                }) {
                    new_proj_fields.push(field.clone());
                }
            }

            // Add original projection fields
            new_proj_fields.extend(proj_node.schema.fields().iter().cloned());

            // Create new projection expressions that include correlation columns
            let mut new_exprs = Vec::new();
            for pred in correlation_predicates {
                let inner_col = &pred.inner_col;
                let unqualified_name = if let Some(dot_pos) = inner_col.find('.') {
                    &inner_col[dot_pos + 1..]
                } else {
                    inner_col.as_str()
                };

                // Find the actual column name in the aggregate output
                if let Some(field) = new_agg_schema.fields().iter().find(|f| {
                    f.name == *inner_col
                        || f.name == unqualified_name
                        || inner_col.ends_with(&format!(".{}", f.name))
                        || f.name.ends_with(&format!(".{}", unqualified_name))
                }) {
                    new_exprs.push(Expr::column(&field.name));
                }
            }
            new_exprs.extend(proj_node.exprs.iter().cloned());

            let new_schema = PlanSchema::new(new_proj_fields);

            return Ok(LogicalPlan::Project(ProjectNode {
                input: Arc::new(new_agg),
                exprs: new_exprs,
                schema: new_schema,
            }));
        }
    }

    // If the subquery already has aggregation, we need to add the correlation columns to group-by
    if let LogicalPlan::Aggregate(agg_node) = subquery {
        // Check if we need to add correlation columns to group-by
        let mut new_group_by = agg_node.group_by.clone();
        let mut new_schema_fields = Vec::new();

        // Get the input schema once to avoid temporary lifetime issues
        let input_schema = agg_node.input.schema();

        for pred in correlation_predicates {
            // Parse the inner column name to get the unqualified name
            let inner_col = &pred.inner_col;
            let unqualified_name = if let Some(dot_pos) = inner_col.find('.') {
                &inner_col[dot_pos + 1..]
            } else {
                inner_col.as_str()
            };

            // Find the actual column in the input schema
            let input_field = input_schema.fields().iter().find(|f| {
                f.name == *inner_col
                    || f.name == unqualified_name
                    || inner_col.ends_with(&format!(".{}", f.name))
                    || f.name.ends_with(&format!(".{}", unqualified_name))
            });

            if let Some(field) = input_field {
                // Use the actual field name from the input schema
                let actual_col_name = field.name.clone();

                // Check if it's already in group-by
                let already_grouped = new_group_by.iter().any(|g| {
                    if let Expr::Column(c) = g {
                        c.name == actual_col_name || c.name == unqualified_name
                    } else {
                        false
                    }
                });

                if !already_grouped {
                    // Use unqualified column expression for group-by
                    let col_expr = Expr::column(&actual_col_name);
                    new_group_by.push(col_expr);

                    // Add to schema with the actual column name
                    new_schema_fields.push(crate::planner::SchemaField::new(
                        &actual_col_name,
                        field.data_type.clone(),
                    ));
                }
            }
        }

        // Build new schema with group-by columns + aggregate columns
        let mut schema_fields = new_schema_fields;
        schema_fields.extend(agg_node.schema.fields().iter().cloned());
        let new_schema = PlanSchema::new(schema_fields);

        return Ok(LogicalPlan::Aggregate(AggregateNode {
            input: agg_node.input.clone(),
            group_by: new_group_by,
            aggregates: agg_node.aggregates.clone(),
            schema: new_schema,
        }));
    }

    // If not an aggregate, return as-is
    Ok(subquery.clone())
}

/// Add a semi-join to the aggregate's input to reduce the number of rows processed.
/// This optimization detects when the outer plan has a filtered dimension table providing
/// the correlation key, and adds a Semi Join to the aggregate's input to filter it down.
fn add_semi_join_reduction(
    agg_plan: LogicalPlan,
    outer: &LogicalPlan,
    correlation_predicates: &[CorrelationPredicate],
) -> LogicalPlan {
    // Unwrap Project wrapping Aggregate (common pattern: SELECT expr * AGG(...))
    if let LogicalPlan::Project(proj) = &agg_plan {
        if let LogicalPlan::Aggregate(_) = proj.input.as_ref() {
            let new_inner =
                add_semi_join_reduction((*proj.input).clone(), outer, correlation_predicates);
            return LogicalPlan::Project(ProjectNode {
                input: Arc::new(new_inner),
                exprs: proj.exprs.clone(),
                schema: proj.schema.clone(),
            });
        }
        return agg_plan;
    }

    let agg = match &agg_plan {
        LogicalPlan::Aggregate(a) => a,
        _ => return agg_plan,
    };

    // Collect semi-join conditions and find a common source from the outer plan
    let mut semi_on = Vec::new();
    let mut source: Option<LogicalPlan> = None;

    for pred in correlation_predicates {
        // Get the outer column name (unqualified)
        let outer_col_name = match &pred.outer_expr {
            Expr::Column(c) => c.name.clone(),
            _ => {
                continue;
            }
        };

        // Find the filtered scan/sub-plan in outer that provides this column
        let found_source = match extract_correlation_source(outer, &outer_col_name) {
            Some(s) if has_selectivity(&s) => s,
            _ => continue,
        };

        // Find inner column in aggregate input schema
        let inner_col = &pred.inner_col;
        let unq_inner = if let Some(dot) = inner_col.rfind('.') {
            &inner_col[dot + 1..]
        } else {
            inner_col.as_str()
        };

        let agg_input_schema = agg.input.schema();
        let inner_field = agg_input_schema.fields().iter().find(|f| {
            f.name == *inner_col
                || f.name == unq_inner
                || inner_col.ends_with(&format!(".{}", f.name))
                || f.name.ends_with(&format!(".{}", unq_inner))
        });

        // Find outer column in source schema
        let source_schema = found_source.schema();
        let outer_field = source_schema.fields().iter().find(|f| {
            f.name == outer_col_name
                || outer_col_name.ends_with(&format!(".{}", f.name))
                || f.name.ends_with(&format!(".{}", outer_col_name))
        });

        if let (Some(inf), Some(outf)) = (inner_field, outer_field) {
            semi_on.push((Expr::column(&inf.name), Expr::column(&outf.name)));
            if source.is_none() {
                source = Some(found_source);
            }
        }
    }

    if let (Some(source_plan), true) = (source, !semi_on.is_empty()) {
        // Use Inner Join instead of Semi Join because the physical planner's
        // should_swap logic correctly builds from the smaller (right) side.
        // The aggregate above ignores the extra columns from the source table.
        let agg_input_schema = agg.input.schema();
        let source_schema = source_plan.schema();
        let mut join_fields = agg_input_schema.fields().to_vec();
        join_fields.extend(source_schema.fields().iter().cloned());
        let join_schema = PlanSchema::new(join_fields);

        let inner_join = LogicalPlan::Join(JoinNode {
            left: agg.input.clone(),
            right: Arc::new(source_plan),
            join_type: JoinType::Inner,
            on: semi_on,
            filter: None,
            schema: join_schema,
        });

        return LogicalPlan::Aggregate(AggregateNode {
            input: Arc::new(inner_join),
            group_by: agg.group_by.clone(),
            aggregates: agg.aggregates.clone(),
            schema: agg.schema.clone(),
        });
    }

    agg_plan
}

/// Extract a sub-plan from the outer plan that produces the given correlation column.
/// Walks the plan tree to find the scan containing the column, preserving any
/// filters and semi-joins along the path (these reduce cardinality).
fn extract_correlation_source(plan: &LogicalPlan, col_name: &str) -> Option<LogicalPlan> {
    match plan {
        LogicalPlan::Scan(scan) => {
            let has_col = scan.schema.fields().iter().any(|f| {
                f.name == col_name
                    || f.name.ends_with(&format!(".{}", col_name))
                    || col_name.ends_with(&format!(".{}", f.name))
            });
            if has_col {
                Some(plan.clone())
            } else {
                None
            }
        }
        LogicalPlan::Filter(f) => {
            let inner = extract_correlation_source(&f.input, col_name)?;
            // Keep filter only if all its column references exist in the inner schema
            let inner_schema = inner.schema();
            if expr_columns_in_schema(&f.predicate, &inner_schema) {
                Some(LogicalPlan::Filter(FilterNode {
                    input: Arc::new(inner),
                    predicate: f.predicate.clone(),
                }))
            } else {
                Some(inner)
            }
        }
        LogicalPlan::Join(j) => {
            let in_left = extract_correlation_source(&j.left, col_name);
            let in_right = extract_correlation_source(&j.right, col_name);

            match (in_left, in_right) {
                (Some(source), None) => {
                    // Column is on left side. If Semi/Anti join, preserve it as filter.
                    if matches!(j.join_type, JoinType::Semi | JoinType::Anti) {
                        let source_schema = source.schema();
                        Some(LogicalPlan::Join(JoinNode {
                            left: Arc::new(source),
                            right: j.right.clone(),
                            join_type: j.join_type,
                            on: j.on.clone(),
                            filter: j.filter.clone(),
                            schema: source_schema,
                        }))
                    } else {
                        Some(source)
                    }
                }
                (None, Some(source)) => Some(source),
                _ => None,
            }
        }
        LogicalPlan::SubqueryAlias(s) => extract_correlation_source(&s.input, col_name),
        LogicalPlan::Project(p) => extract_correlation_source(&p.input, col_name),
        _ => None,
    }
}

/// Check if all column references in an expression exist in the given schema
fn expr_columns_in_schema(expr: &Expr, schema: &PlanSchema) -> bool {
    match expr {
        Expr::Column(c) => {
            let name = &c.name;
            schema.fields().iter().any(|f| {
                f.name == *name
                    || f.name.ends_with(&format!(".{}", name))
                    || name.ends_with(&format!(".{}", f.name))
            })
        }
        Expr::BinaryExpr { left, right, .. } => {
            expr_columns_in_schema(left, schema) && expr_columns_in_schema(right, schema)
        }
        Expr::UnaryExpr { expr, .. } => expr_columns_in_schema(expr, schema),
        Expr::Literal(_) => true,
        Expr::Cast { expr, .. } | Expr::Alias { expr, .. } => expr_columns_in_schema(expr, schema),
        Expr::InList { expr, list, .. } => {
            expr_columns_in_schema(expr, schema)
                && list.iter().all(|l| expr_columns_in_schema(l, schema))
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            expr_columns_in_schema(expr, schema)
                && expr_columns_in_schema(low, schema)
                && expr_columns_in_schema(high, schema)
        }
        Expr::ScalarFunc { args, .. } => args.iter().all(|a| expr_columns_in_schema(a, schema)),
        _ => false, // Conservative: don't include filter for unknown expressions
    }
}

/// Check if a plan has any selectivity (filters or semi/anti joins that reduce cardinality)
fn has_selectivity(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Scan(s) => s.filter.is_some(), // Scan-level pushed-down filter
        LogicalPlan::Filter(_) => true,
        LogicalPlan::Join(j) if matches!(j.join_type, JoinType::Semi | JoinType::Anti) => true,
        LogicalPlan::Join(j) => has_selectivity(&j.left) || has_selectivity(&j.right),
        LogicalPlan::Project(p) => has_selectivity(&p.input),
        LogicalPlan::SubqueryAlias(s) => has_selectivity(&s.input),
        _ => false,
    }
}

/// A correlation predicate extracted from a subquery
#[derive(Debug, Clone)]
struct CorrelationPredicate {
    outer_expr: Expr,
    inner_col: String,
    op: BinaryOp,
}

/// Extract correlation predicates from a subquery
/// Returns (correlation predicates, decorrelated subquery)
fn extract_correlation_predicates(
    subquery: &LogicalPlan,
    outer: &LogicalPlan,
) -> Result<(Vec<CorrelationPredicate>, LogicalPlan)> {
    // Use collect_plan_column_names for both to include table aliases
    let outer_columns = collect_plan_column_names(outer);
    let inner_columns = collect_plan_column_names(subquery);

    let mut correlation_predicates = Vec::new();
    let decorrelated = extract_from_plan(
        subquery,
        &outer_columns,
        &inner_columns,
        &mut correlation_predicates,
    )?;

    Ok((correlation_predicates, decorrelated))
}

/// Collect column names defined in a plan (including aliases)
fn collect_plan_column_names(plan: &LogicalPlan) -> HashSet<String> {
    let mut columns = HashSet::new();
    collect_plan_columns_recursive(plan, &mut columns);
    columns
}

fn collect_plan_columns_recursive(plan: &LogicalPlan, columns: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Scan(node) => {
            columns.insert(node.table_name.clone());
            for field in node.schema.fields() {
                columns.insert(field.name.clone());
                // Also add qualified name if relation is set
                columns.insert(field.qualified_name());
                // Also add unqualified name if the name contains a dot
                if let Some(dot_pos) = field.name.find('.') {
                    columns.insert(field.name[dot_pos + 1..].to_string());
                }
            }
        }
        LogicalPlan::SubqueryAlias(node) => {
            columns.insert(node.alias.clone());
            for field in node.schema.fields() {
                // Add unqualified name
                columns.insert(field.name.clone());
                // Add qualified name (e.g., "l1.l_orderkey")
                columns.insert(field.qualified_name());
            }
            collect_plan_columns_recursive(&node.input, columns);
        }
        LogicalPlan::Join(node) => {
            collect_plan_columns_recursive(&node.left, columns);
            collect_plan_columns_recursive(&node.right, columns);
        }
        LogicalPlan::Filter(node) => collect_plan_columns_recursive(&node.input, columns),
        LogicalPlan::Project(node) => {
            for field in node.schema.fields() {
                columns.insert(field.name.clone());
            }
            collect_plan_columns_recursive(&node.input, columns);
        }
        LogicalPlan::Aggregate(node) => {
            for field in node.schema.fields() {
                columns.insert(field.name.clone());
            }
            collect_plan_columns_recursive(&node.input, columns);
        }
        _ => {
            for child in plan.children() {
                collect_plan_columns_recursive(child, columns);
            }
        }
    }
}

/// Extract correlation predicates from a plan recursively
fn extract_from_plan(
    plan: &LogicalPlan,
    outer_columns: &HashSet<String>,
    inner_columns: &HashSet<String>,
    predicates: &mut Vec<CorrelationPredicate>,
) -> Result<LogicalPlan> {
    match plan {
        LogicalPlan::Filter(node) => {
            let new_input =
                extract_from_plan(&node.input, outer_columns, inner_columns, predicates)?;

            // Extract correlation predicates from this filter
            let (corr_preds, remaining_expr) =
                extract_correlation_from_expr(&node.predicate, outer_columns, inner_columns);

            predicates.extend(corr_preds);

            if let Some(remaining) = remaining_expr {
                Ok(LogicalPlan::Filter(FilterNode {
                    input: Arc::new(new_input),
                    predicate: remaining,
                }))
            } else {
                Ok(new_input)
            }
        }
        LogicalPlan::Project(node) => {
            let new_input =
                extract_from_plan(&node.input, outer_columns, inner_columns, predicates)?;
            Ok(LogicalPlan::Project(ProjectNode {
                input: Arc::new(new_input),
                exprs: node.exprs.clone(),
                schema: node.schema.clone(),
            }))
        }
        LogicalPlan::Join(node) => {
            let new_left = extract_from_plan(&node.left, outer_columns, inner_columns, predicates)?;
            let new_right =
                extract_from_plan(&node.right, outer_columns, inner_columns, predicates)?;
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
                extract_from_plan(&node.input, outer_columns, inner_columns, predicates)?;
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
/// Returns (correlation predicates, remaining expression)
fn extract_correlation_from_expr(
    expr: &Expr,
    outer_columns: &HashSet<String>,
    inner_columns: &HashSet<String>,
) -> (Vec<CorrelationPredicate>, Option<Expr>) {
    match expr {
        Expr::BinaryExpr {
            left,
            op: BinaryOp::And,
            right,
        } => {
            let (left_preds, left_remaining) =
                extract_correlation_from_expr(left, outer_columns, inner_columns);
            let (right_preds, right_remaining) =
                extract_correlation_from_expr(right, outer_columns, inner_columns);

            let mut all_preds = left_preds;
            all_preds.extend(right_preds);

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

            (all_preds, remaining)
        }
        Expr::BinaryExpr { left, op, right } if is_comparison_op(*op) => {
            // Check if this is a correlation predicate (outer = inner or inner = outer)
            if let Some(pred) =
                try_extract_correlation(left, right, *op, outer_columns, inner_columns)
            {
                return (vec![pred], None);
            }
            if let Some(pred) =
                try_extract_correlation(right, left, flip_op(*op), outer_columns, inner_columns)
            {
                return (vec![pred], None);
            }
            // Not a correlation predicate
            (vec![], Some(expr.clone()))
        }
        _ => (vec![], Some(expr.clone())),
    }
}

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

fn flip_op(op: BinaryOp) -> BinaryOp {
    match op {
        BinaryOp::Lt => BinaryOp::Gt,
        BinaryOp::LtEq => BinaryOp::GtEq,
        BinaryOp::Gt => BinaryOp::Lt,
        BinaryOp::GtEq => BinaryOp::LtEq,
        other => other,
    }
}

/// Try to extract a correlation predicate from a comparison
fn try_extract_correlation(
    left: &Expr,
    right: &Expr,
    op: BinaryOp,
    outer_columns: &HashSet<String>,
    inner_columns: &HashSet<String>,
) -> Option<CorrelationPredicate> {
    let left_in_outer = references_columns(left, outer_columns);
    let left_in_inner = references_columns(left, inner_columns);
    let right_in_outer = references_columns(right, outer_columns);
    let right_in_inner = references_columns(right, inner_columns);

    // Case 1: left is exclusively outer, right is in inner (may also be in outer due to shared names)
    // This handles Q17's case: p_partkey (only outer) = l_partkey (in both)
    let left_exclusively_outer = left_in_outer && !left_in_inner;
    let right_has_inner = right_in_inner;

    if left_exclusively_outer && right_has_inner {
        if let Some(inner_col) = get_column_name(right) {
            return Some(CorrelationPredicate {
                outer_expr: left.clone(),
                inner_col,
                op,
            });
        }
    }

    // Case 2: right is exclusively outer, left is in inner
    let right_exclusively_outer = right_in_outer && !right_in_inner;
    let left_has_inner = left_in_inner;

    if right_exclusively_outer && left_has_inner {
        if let Some(inner_col) = get_column_name(left) {
            return Some(CorrelationPredicate {
                outer_expr: right.clone(),
                inner_col,
                op: flip_op(op),
            });
        }
    }

    // Case 3: Standard case - left only outer, right only inner (no overlap)
    let left_only_outer = left_in_outer && !left_in_inner;
    let right_only_inner = right_in_inner && !right_in_outer;

    if left_only_outer && right_only_inner {
        if let Some(inner_col) = get_column_name(right) {
            return Some(CorrelationPredicate {
                outer_expr: left.clone(),
                inner_col,
                op,
            });
        }
    }

    None
}

/// Check if an expression references any of the given columns
fn references_columns(expr: &Expr, columns: &HashSet<String>) -> bool {
    match expr {
        Expr::Column(col) => {
            // If column has a relation qualifier, only match against that relation
            if let Some(rel) = &col.relation {
                // Check if the relation itself is in columns (e.g., "l1" for table alias)
                if columns.contains(rel) {
                    return true;
                }
                // Check qualified name (e.g., "l1.l_orderkey")
                let qualified = format!("{}.{}", rel, col.name);
                if columns.contains(&qualified) {
                    return true;
                }
                // Don't fall back to unqualified matching when relation is specified
                // This prevents "l1.l_orderkey" from matching inner "l_orderkey"
                return false;
            }
            // Only for unqualified columns, check the name directly
            columns.contains(&col.name)
        }
        Expr::BinaryExpr { left, right, .. } => {
            references_columns(left, columns) || references_columns(right, columns)
        }
        Expr::UnaryExpr { expr, .. } => references_columns(expr, columns),
        Expr::ScalarFunc { args, .. } | Expr::Aggregate { args, .. } => {
            args.iter().any(|a| references_columns(a, columns))
        }
        Expr::Cast { expr, .. } | Expr::Alias { expr, .. } => references_columns(expr, columns),
        _ => false,
    }
}

/// Get the column name from a simple column expression
fn get_column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(col) => {
            if let Some(rel) = &col.relation {
                Some(format!("{}.{}", rel, col.name))
            } else {
                Some(col.name.clone())
            }
        }
        Expr::Alias { expr, .. } => get_column_name(expr),
        _ => None,
    }
}

/// Build equi-join conditions from correlation predicates
fn build_join_conditions(
    predicates: &[CorrelationPredicate],
    outer: &LogicalPlan,
    inner: &LogicalPlan,
) -> Result<Vec<(Expr, Expr)>> {
    let mut conditions = Vec::new();

    let outer_schema = outer.schema();
    let inner_schema = inner.schema();

    for pred in predicates {
        // Only support equality for now
        if pred.op != BinaryOp::Eq {
            continue;
        }

        // Validate that outer_expr references a column in outer schema
        if !expr_references_schema(&pred.outer_expr, &outer_schema) {
            continue;
        }

        // Parse the inner column name (might be qualified like "o.user_id")
        let inner_col_name = &pred.inner_col;
        let unqualified_name = if let Some(dot_pos) = inner_col_name.find('.') {
            &inner_col_name[dot_pos + 1..]
        } else {
            inner_col_name.as_str()
        };

        // Find the actual column in the inner schema
        let inner_field = inner_schema.fields().iter().find(|f| {
            f.name == *inner_col_name
                || f.name == unqualified_name
                || inner_col_name.ends_with(&format!(".{}", f.name))
                || f.name.ends_with(&format!(".{}", unqualified_name))
                || f.qualified_name() == *inner_col_name
        });

        if inner_field.is_none() {
            continue;
        }

        let inner_field = inner_field.unwrap();
        // Create the inner expression using the relation qualifier if available
        // This ensures proper column resolution in the join
        let inner_expr = if inner_field.relation.is_some() {
            Expr::qualified_column(inner_field.relation.as_ref().unwrap(), &inner_field.name)
        } else {
            Expr::column(&inner_field.name)
        };

        conditions.push((pred.outer_expr.clone(), inner_expr));
    }

    Ok(conditions)
}

/// Check if an expression references columns in a schema
fn expr_references_schema(expr: &Expr, schema: &PlanSchema) -> bool {
    match expr {
        Expr::Column(col) => {
            let col_name = if let Some(rel) = &col.relation {
                format!("{}.{}", rel, col.name)
            } else {
                col.name.clone()
            };

            schema.fields().iter().any(|f| {
                f.name == col_name
                    || f.name == col.name
                    || f.name.ends_with(&format!(".{}", col.name))
            })
        }
        Expr::BinaryExpr { left, right, .. } => {
            expr_references_schema(left, schema) || expr_references_schema(right, schema)
        }
        Expr::UnaryExpr { expr, .. } => expr_references_schema(expr, schema),
        Expr::Cast { expr, .. } | Expr::Alias { expr, .. } => expr_references_schema(expr, schema),
        _ => true, // Assume true for other expressions
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{LogicalPlanBuilder, SchemaField};
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
    fn test_decorrelate_exists() {
        // SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM lineitem l WHERE l.l_orderkey = o.o_orderkey)
        let lineitem_scan = LogicalPlanBuilder::scan("lineitem", lineitem_schema()).build();

        // Add correlation predicate to subquery
        let correlated_subquery = lineitem_scan.filter(Expr::BinaryExpr {
            left: Box::new(Expr::qualified_column("lineitem", "l_orderkey")),
            op: BinaryOp::Eq,
            right: Box::new(Expr::qualified_column("orders", "o_orderkey")),
        });

        let outer = LogicalPlanBuilder::scan("orders", orders_schema()).build();

        // Create EXISTS predicate
        let exists_predicate = Expr::Exists {
            subquery: Arc::new(correlated_subquery),
            negated: false,
        };

        let plan = outer.filter(exists_predicate);

        // Apply decorrelation
        let rule = SubqueryDecorrelation;
        let result = rule.optimize(&plan).unwrap();

        // Should be transformed into a Semi Join
        if let LogicalPlan::Join(join) = &result {
            assert_eq!(join.join_type, JoinType::Semi);
            assert!(!join.on.is_empty());
        } else {
            panic!("Expected Join, got {:?}", result);
        }
    }
}
