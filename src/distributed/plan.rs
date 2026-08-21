//! What M2 can distribute, and — much more importantly — what it refuses to.
//!
//! # The rule
//!
//! A query is distributable when it is a **single-table scan with filters and
//! projections, and any aggregation on top of it has an exact partial/final
//! split**: `COUNT`, `SUM`, `MIN`, `MAX`, and `AVG` carried as `(sum, count)`
//! and divided only at the end. Everything else returns
//! `NotImplemented("<reason>; distributed execution supports ...")`.
//!
//! Rejecting is not a limitation reluctantly documented — it is the feature. A
//! shard-local answer presented as a cluster answer is indistinguishable from a
//! correct one at the call site, and `DISTRIBUTED-READINESS.md` records three
//! live single-node defects (partition-0-only callers, an unenforced memory
//! budget, sub-plans executed at *planning* time — 87% of Q15) that would make
//! several of the tempting shapes actively wrong rather than merely slow. So
//! the shapes that need those fixes are refused, by name, with a reason.
//!
//! # Three independent nets, because one is not enough
//!
//! 1. **Structural check on the parsed AST** — CTEs, set operations, joins,
//!    `ORDER BY`, `LIMIT`, `DISTINCT`. These get the clearest messages because
//!    the syntax is right there.
//! 2. **Capability check on the bound `LogicalPlan`** — the authority. It uses
//!    the engine's own semantics, so an aggregate the AST layer failed to
//!    recognise by name is still caught here as an `Expr::Aggregate` whose
//!    function is not one of the five.
//! 3. **Alias closure on the rewritten final projection** — every leaf
//!    identifier in the merge query must be an alias this module generated. An
//!    aggregate that slipped through both earlier nets would leave a raw column
//!    reference behind, and that fails here. This is what makes "we forgot to
//!    blacklist `KURTOSIS`" a rejection instead of a wrong number.
//!
//! # `AVG` is the one that is easy to get silently wrong
//!
//! The average of per-node averages is not the average, unless every node holds
//! exactly the same number of non-null rows — which is precisely what a
//! byte-balanced assignment does *not* guarantee. `AVG(x)` is therefore
//! transported as `SUM(x)` and `COUNT(x)` and divided once, on the initiator.

use crate::error::{QueryError, Result};
use crate::execution::ExecutionContext;
use crate::planner::{AggregateFunction, Expr, LogicalPlan};
use sqlparser::ast as sa;

/// Name of the in-memory table the initiator registers the workers' partial
/// results under. Prefixed so it cannot collide with a user table.
pub const PARTIAL_TABLE: &str = "qe_dist_partial";
const GROUP_PREFIX: &str = "qe_g";
const AGG_PREFIX: &str = "qe_a";

const SUPPORTED: &str = "distributed execution supports a single-table scan with \
                         WHERE/SELECT and COUNT/SUM/MIN/MAX/AVG (with or without GROUP BY)";

fn unsupported(reason: impl std::fmt::Display) -> QueryError {
    QueryError::NotImplemented(format!("{reason}; {SUPPORTED}"))
}

/// How the initiator must combine what the workers return.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeShape {
    /// No aggregation: the answer is the concatenation of the shards.
    ///
    /// Row ORDER is unspecified — it depends on which node answered first.
    /// That is why `ORDER BY` is rejected rather than silently ignored.
    Concat,
    /// Two-phase: workers compute partial aggregates, the initiator merges them
    /// with [`DistributedPlan::final_sql`].
    TwoPhase,
    /// General path: workers stream their shard of every referenced table to
    /// the initiator, which runs the original statement over the gathered
    /// columns. Chosen only when neither exact shape above applies. See
    /// [`crate::distributed::gather`].
    Gather,
}

/// A query, split into the part every worker runs over its own shard and the
/// part the initiator runs over the collected partials.
#[derive(Clone, Debug, serde::Serialize)]
pub struct DistributedPlan {
    /// The single table the query scans. M2 has no shuffle, so there is
    /// exactly one.
    pub table: String,
    /// SQL each worker executes against its shard.
    pub partial_sql: String,
    /// SQL the initiator executes over [`PARTIAL_TABLE`]. `None` for
    /// [`MergeShape::Concat`].
    pub final_sql: Option<String>,
    pub shape: MergeShape,
    /// Output column names of the ORIGINAL query, so the distributed answer has
    /// the same schema as the single-node one rather than a rewritten one.
    pub output_names: Vec<String>,
}

/// Decide whether `sql` can be answered by fan-out + merge, and if so how.
///
/// Returns `Err(QueryError::NotImplemented(..))` — never a partial answer, and
/// never a quiet fallback — for anything outside the supported set.
pub fn plan_distributed(ctx: &ExecutionContext, sql: &str) -> Result<DistributedPlan> {
    let stmt = crate::parser::parse_sql(sql)?;
    let select = structural_check(&stmt)?;

    // Bind with the engine's own binder. A NotImplemented from here (window
    // functions, unsupported syntax) is re-flavoured so the caller is told what
    // *distributed* supports, not only what failed.
    let logical = ctx.logical_plan(sql).map_err(|e| match e {
        QueryError::NotImplemented(msg) => unsupported(msg),
        other => other,
    })?;
    let caps = capability_check(&logical)?;

    let output_names: Vec<String> = logical
        .schema()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect();

    let group_exprs: Vec<sa::Expr> = match &select.group_by {
        sa::GroupByExpr::Expressions(exprs, modifiers) => {
            if !modifiers.is_empty() {
                return Err(unsupported("GROUP BY with ROLLUP/CUBE/GROUPING SETS"));
            }
            exprs.clone()
        }
        sa::GroupByExpr::All(_) => return Err(unsupported("GROUP BY ALL")),
    };

    // No aggregation and no grouping: the shards concatenate. Nothing to
    // rewrite, so nothing can be rewritten wrongly.
    if !caps.has_aggregate && group_exprs.is_empty() {
        return Ok(DistributedPlan {
            table: caps.table,
            partial_sql: sql.trim().trim_end_matches(';').to_string(),
            final_sql: None,
            shape: MergeShape::Concat,
            output_names,
        });
    }

    if select.projection.len() != output_names.len() {
        // A wildcard expanded, or something else changed the arity. Rewriting
        // by position would silently mislabel columns.
        return Err(unsupported(
            "wildcard (SELECT *) in an aggregated or grouped query",
        ));
    }

    let mut rw = Rewriter::new(&group_exprs);
    let mut final_projection = Vec::with_capacity(select.projection.len());
    for (i, item) in select.projection.iter().enumerate() {
        let expr = match item {
            sa::SelectItem::UnnamedExpr(e) => e,
            sa::SelectItem::ExprWithAlias { expr, .. } => expr,
            sa::SelectItem::Wildcard(_) | sa::SelectItem::QualifiedWildcard(..) => {
                return Err(unsupported(
                    "wildcard (SELECT *) in an aggregated or grouped query",
                ))
            }
        };
        let rewritten = rw.rewrite(expr)?;
        final_projection.push(sa::SelectItem::ExprWithAlias {
            expr: rewritten,
            // Quoted, because the engine's own name for an unaliased aggregate
            // is `SUM(l_quantity)` — parentheses and all — and the distributed
            // answer must carry the same header as the single-node one.
            alias: sa::Ident::with_quote('"', output_names[i].clone()),
        });
    }
    let final_having = match &select.having {
        Some(h) => Some(rw.rewrite(h)?),
        None => None,
    };

    let partial_sql = rw.partial_sql(select, &group_exprs);
    let final_sql = rw.final_sql(final_projection, final_having);

    // Net 3: every leaf identifier of the merge query must be an alias this
    // module generated. See the module docs for why this is load-bearing.
    verify_alias_closure(&final_sql)?;

    Ok(DistributedPlan {
        table: caps.table,
        partial_sql,
        final_sql: Some(final_sql),
        shape: MergeShape::TwoPhase,
        output_names,
    })
}

// ---------------------------------------------------------------------------
// Net 1: structure of the parsed statement
// ---------------------------------------------------------------------------

fn structural_check(stmt: &sa::Statement) -> Result<&sa::Select> {
    let query = match stmt {
        sa::Statement::Query(q) => q,
        other => {
            return Err(unsupported(format!(
                "only SELECT can be distributed, not `{}`",
                first_word(&other.to_string())
            )))
        }
    };

    if query.with.is_some() {
        return Err(unsupported("common table expressions (WITH)"));
    }
    if query.order_by.is_some() {
        // Rejected rather than merged-then-sorted, deliberately: pushing
        // ORDER BY to the workers sorts each shard independently, and the
        // concatenation of sorted shards is not sorted. Doing it correctly
        // means a final global sort, which is M3's job.
        return Err(unsupported("global ORDER BY"));
    }
    if query.limit.is_some() || query.offset.is_some() || query.fetch.is_some() {
        // A LIMIT evaluated per shard returns N rows per node, not N rows.
        return Err(unsupported("global LIMIT/OFFSET/FETCH"));
    }
    if !query.limit_by.is_empty() {
        return Err(unsupported("LIMIT ... BY"));
    }

    let select = match query.body.as_ref() {
        sa::SetExpr::Select(s) => s.as_ref(),
        sa::SetExpr::SetOperation { op, .. } => {
            return Err(unsupported(format!("set operations ({op})")))
        }
        sa::SetExpr::Values(_) => return Err(unsupported("VALUES")),
        _ => return Err(unsupported("this query form")),
    };

    if select.distinct.is_some() {
        return Err(unsupported("SELECT DISTINCT"));
    }
    if select.top.is_some() {
        return Err(unsupported("TOP"));
    }
    if select.into.is_some() {
        return Err(unsupported("SELECT ... INTO"));
    }
    if select.qualify.is_some() {
        return Err(unsupported("QUALIFY"));
    }
    if !select.named_window.is_empty() {
        return Err(unsupported("WINDOW clauses"));
    }
    if select.prewhere.is_some() {
        return Err(unsupported("PREWHERE"));
    }
    if !select.lateral_views.is_empty() {
        return Err(unsupported("LATERAL VIEW"));
    }
    if !select.cluster_by.is_empty()
        || !select.distribute_by.is_empty()
        || !select.sort_by.is_empty()
    {
        return Err(unsupported("CLUSTER BY / DISTRIBUTE BY / SORT BY"));
    }
    if select.connect_by.is_some() {
        return Err(unsupported("CONNECT BY"));
    }

    match select.from.len() {
        1 => {}
        0 => return Err(unsupported("a SELECT with no FROM clause")),
        n => {
            return Err(unsupported(format!(
                "cross-shard joins (FROM lists {n} tables)"
            )))
        }
    }
    if !select.from[0].joins.is_empty() {
        return Err(unsupported("cross-shard joins (JOIN)"));
    }
    match &select.from[0].relation {
        sa::TableFactor::Table { args: Some(_), .. } => {
            return Err(unsupported("table-valued functions"))
        }
        sa::TableFactor::Table { .. } => {}
        sa::TableFactor::Derived { .. } => {
            return Err(unsupported("subqueries in FROM (derived tables)"))
        }
        sa::TableFactor::NestedJoin { .. } => return Err(unsupported("cross-shard joins (JOIN)")),
        other => {
            return Err(unsupported(format!(
                "this FROM item ({})",
                first_word(&other.to_string())
            )))
        }
    }

    Ok(select)
}

fn first_word(s: &str) -> String {
    s.split_whitespace().next().unwrap_or("?").to_string()
}

// ---------------------------------------------------------------------------
// Net 2: capability of the bound logical plan
// ---------------------------------------------------------------------------

struct Capabilities {
    table: String,
    has_aggregate: bool,
}

fn capability_check(plan: &LogicalPlan) -> Result<Capabilities> {
    let mut tables: Vec<String> = Vec::new();
    let mut has_aggregate = false;
    walk_plan(plan, &mut tables, &mut has_aggregate)?;

    match tables.len() {
        1 => Ok(Capabilities {
            table: tables.remove(0),
            has_aggregate,
        }),
        0 => Err(unsupported("a query that scans no table")),
        n => Err(unsupported(format!(
            "cross-shard joins ({n} tables scanned)"
        ))),
    }
}

fn walk_plan(plan: &LogicalPlan, tables: &mut Vec<String>, has_agg: &mut bool) -> Result<()> {
    match plan {
        LogicalPlan::Scan(node) => {
            tables.push(node.table_name.clone());
            if let Some(f) = &node.filter {
                check_expr(f)?;
            }
        }
        LogicalPlan::Filter(node) => check_expr(&node.predicate)?,
        LogicalPlan::Window(_) => return Err(unsupported("window functions")),
        LogicalPlan::Project(node) => {
            for e in &node.exprs {
                check_expr(e)?;
            }
        }
        LogicalPlan::Aggregate(node) => {
            *has_agg = true;
            for e in node.group_by.iter().chain(node.aggregates.iter()) {
                check_expr(e)?;
            }
        }
        LogicalPlan::Join(_) | LogicalPlan::DelimJoin(_) => {
            return Err(unsupported("cross-shard joins"))
        }
        LogicalPlan::Sort(_) => return Err(unsupported("global ORDER BY")),
        LogicalPlan::Limit(_) => return Err(unsupported("global LIMIT/OFFSET")),
        LogicalPlan::Distinct(_) => return Err(unsupported("SELECT DISTINCT")),
        LogicalPlan::Union(_) => {
            return Err(unsupported("set operations (UNION/EXCEPT/INTERSECT)"))
        }
        LogicalPlan::SubqueryAlias(_) => {
            return Err(unsupported("subqueries in FROM (derived tables)"))
        }
        LogicalPlan::Values(_) | LogicalPlan::EmptyRelation(_) => {
            return Err(unsupported("VALUES / an empty relation"))
        }
        LogicalPlan::DelimGet(_) => return Err(unsupported("decorrelated subqueries")),
        LogicalPlan::VectorSearch(_) => {
            return Err(unsupported("vector search (ORDER BY distance)"))
        }
    }
    for child in plan.children() {
        walk_plan(child, tables, has_agg)?;
    }
    Ok(())
}

/// Reject subqueries and unsupported aggregates anywhere in an expression.
fn check_expr(expr: &Expr) -> Result<()> {
    if expr.contains_subquery() {
        return Err(unsupported(
            "correlated or uncorrelated subqueries (EXISTS / IN / scalar)",
        ));
    }
    let mut bad: Option<String> = None;
    visit_expr(expr, &mut |e| {
        if bad.is_some() {
            return;
        }
        if let Expr::Aggregate {
            func,
            args,
            distinct,
        } = e
        {
            let arg = args
                .first()
                .map(|a| a.to_string())
                .unwrap_or_else(|| "*".into());
            match func {
                AggregateFunction::CountDistinct => {
                    bad = Some(format!("COUNT(DISTINCT {arg})"));
                }
                AggregateFunction::Count
                | AggregateFunction::Sum
                | AggregateFunction::Avg
                | AggregateFunction::Min
                | AggregateFunction::Max => {
                    if *distinct {
                        bad = Some(format!("{}(DISTINCT {arg})", upper(func)));
                    }
                }
                other => {
                    bad = Some(format!("{}({arg})", upper(other)));
                }
            }
        }
    });
    match bad {
        None => Ok(()),
        Some(what) => Err(unsupported(format!(
            "{what} has no exact partial/final split"
        ))),
    }
}

/// SQL spelling of an aggregate function, for error messages: the enum's
/// `CamelCase` name as `SCREAMING_SNAKE_CASE`, so `ApproxDistinct` is reported
/// as `APPROX_DISTINCT` — the name the user actually typed — rather than
/// `APPROXDISTINCT`, which they cannot search for.
///
/// Only the function name is transformed; the argument keeps the user's own
/// spelling instead of being shouted back at them.
fn upper(func: &AggregateFunction) -> String {
    let debug = format!("{func:?}");
    let mut out = String::with_capacity(debug.len() + 4);
    for (i, c) in debug.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.extend(c.to_uppercase());
    }
    out
}

/// Pre-order walk of an expression tree.
fn visit_expr(expr: &Expr, f: &mut impl FnMut(&Expr)) {
    f(expr);
    match expr {
        Expr::Column(_) | Expr::Literal(_) | Expr::Wildcard | Expr::QualifiedWildcard(_) => {}
        Expr::BinaryExpr { left, right, .. } => {
            visit_expr(left, f);
            visit_expr(right, f);
        }
        Expr::UnaryExpr { expr, .. } | Expr::Cast { expr, .. } | Expr::Alias { expr, .. } => {
            visit_expr(expr, f)
        }
        Expr::WindowFunction(w) => {
            for a in &w.args {
                visit_expr(a, f);
            }
            for p in &w.partition_by {
                visit_expr(p, f);
            }
            for o in &w.order_by {
                visit_expr(&o.expr, f);
            }
        }
        Expr::Aggregate { args, .. } | Expr::ScalarFunc { args, .. } => {
            for a in args {
                visit_expr(a, f);
            }
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(o) = operand {
                visit_expr(o, f);
            }
            for (w, t) in when_then {
                visit_expr(w, f);
                visit_expr(t, f);
            }
            if let Some(e) = else_expr {
                visit_expr(e, f);
            }
        }
        Expr::InList { expr, list, .. } => {
            visit_expr(expr, f);
            for l in list {
                visit_expr(l, f);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            visit_expr(expr, f);
            visit_expr(low, f);
            visit_expr(high, f);
        }
        // Subqueries were already rejected by `contains_subquery`; their inner
        // plans are deliberately not descended into here.
        Expr::ScalarSubquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {}
    }
}

// ---------------------------------------------------------------------------
// The rewrite
// ---------------------------------------------------------------------------

struct Rewriter {
    /// Normalized text of each GROUP BY expression, in order.
    group_keys: Vec<String>,
    /// SELECT list of the partial query.
    partial_items: Vec<sa::SelectItem>,
    next_agg: usize,
}

impl Rewriter {
    fn new(group_exprs: &[sa::Expr]) -> Self {
        let mut partial_items = Vec::with_capacity(group_exprs.len());
        for (i, g) in group_exprs.iter().enumerate() {
            partial_items.push(sa::SelectItem::ExprWithAlias {
                expr: g.clone(),
                alias: sa::Ident::new(format!("{GROUP_PREFIX}{i}")),
            });
        }
        Self {
            group_keys: group_exprs.iter().map(|g| g.to_string()).collect(),
            partial_items,
            next_agg: 0,
        }
    }

    fn add_partial(&mut self, expr: sa::Expr, alias: &str) {
        self.partial_items.push(sa::SelectItem::ExprWithAlias {
            expr,
            alias: sa::Ident::new(alias.to_string()),
        });
    }

    /// Rewrite one expression of the original SELECT/HAVING into its merge form.
    fn rewrite(&mut self, e: &sa::Expr) -> Result<sa::Expr> {
        // A GROUP BY key, at any depth, becomes its transported alias.
        let text = e.to_string();
        if let Some(i) = self.group_keys.iter().position(|k| *k == text) {
            return Ok(ident(&format!("{GROUP_PREFIX}{i}")));
        }

        Ok(match e {
            sa::Expr::Value(_) | sa::Expr::TypedString { .. } | sa::Expr::Interval(_) => e.clone(),
            sa::Expr::Identifier(id) => {
                return Err(unsupported(format!(
                    "column `{id}` is neither grouped nor aggregated"
                )))
            }
            sa::Expr::CompoundIdentifier(parts) => {
                return Err(unsupported(format!(
                    "column `{}` is neither grouped nor aggregated",
                    parts
                        .iter()
                        .map(|p| p.value.clone())
                        .collect::<Vec<_>>()
                        .join(".")
                )))
            }
            sa::Expr::Nested(inner) => sa::Expr::Nested(Box::new(self.rewrite(inner)?)),
            sa::Expr::BinaryOp { left, op, right } => sa::Expr::BinaryOp {
                left: Box::new(self.rewrite(left)?),
                op: op.clone(),
                right: Box::new(self.rewrite(right)?),
            },
            sa::Expr::UnaryOp { op, expr } => sa::Expr::UnaryOp {
                op: *op,
                expr: Box::new(self.rewrite(expr)?),
            },
            sa::Expr::Cast {
                kind,
                expr,
                data_type,
                format,
            } => sa::Expr::Cast {
                kind: kind.clone(),
                expr: Box::new(self.rewrite(expr)?),
                data_type: data_type.clone(),
                format: format.clone(),
            },
            sa::Expr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => sa::Expr::Case {
                operand: match operand {
                    Some(o) => Some(Box::new(self.rewrite(o)?)),
                    None => None,
                },
                conditions: conditions
                    .iter()
                    .map(|c| self.rewrite(c))
                    .collect::<Result<Vec<_>>>()?,
                results: results
                    .iter()
                    .map(|r| self.rewrite(r))
                    .collect::<Result<Vec<_>>>()?,
                else_result: match else_result {
                    Some(x) => Some(Box::new(self.rewrite(x)?)),
                    None => None,
                },
            },
            sa::Expr::IsNull(x) => sa::Expr::IsNull(Box::new(self.rewrite(x)?)),
            sa::Expr::IsNotNull(x) => sa::Expr::IsNotNull(Box::new(self.rewrite(x)?)),
            sa::Expr::IsTrue(x) => sa::Expr::IsTrue(Box::new(self.rewrite(x)?)),
            sa::Expr::IsFalse(x) => sa::Expr::IsFalse(Box::new(self.rewrite(x)?)),
            sa::Expr::IsNotTrue(x) => sa::Expr::IsNotTrue(Box::new(self.rewrite(x)?)),
            sa::Expr::IsNotFalse(x) => sa::Expr::IsNotFalse(Box::new(self.rewrite(x)?)),
            sa::Expr::Between {
                expr,
                negated,
                low,
                high,
            } => sa::Expr::Between {
                expr: Box::new(self.rewrite(expr)?),
                negated: *negated,
                low: Box::new(self.rewrite(low)?),
                high: Box::new(self.rewrite(high)?),
            },
            sa::Expr::InList {
                expr,
                list,
                negated,
            } => sa::Expr::InList {
                expr: Box::new(self.rewrite(expr)?),
                list: list
                    .iter()
                    .map(|l| self.rewrite(l))
                    .collect::<Result<Vec<_>>>()?,
                negated: *negated,
            },
            sa::Expr::Function(func) => self.rewrite_function(func)?,
            other => {
                return Err(unsupported(format!(
                    "expression `{other}` cannot be split into partial and final phases"
                )))
            }
        })
    }

    fn rewrite_function(&mut self, func: &sa::Function) -> Result<sa::Expr> {
        let name = func.name.to_string().to_uppercase();

        if func.over.is_some() {
            return Err(unsupported(format!("window function {name}(...) OVER ...")));
        }
        if func.filter.is_some() {
            return Err(unsupported(format!("{name}(...) FILTER (WHERE ...)")));
        }
        if !matches!(func.parameters, sa::FunctionArguments::None) {
            return Err(unsupported(format!("parameterized function {name}")));
        }

        let (distinct, args, clauses) = match &func.args {
            sa::FunctionArguments::None => (false, Vec::new(), Vec::new()),
            sa::FunctionArguments::Subquery(_) => {
                return Err(unsupported("subquery function arguments"))
            }
            sa::FunctionArguments::List(list) => (
                matches!(
                    list.duplicate_treatment,
                    Some(sa::DuplicateTreatment::Distinct)
                ),
                list.args.clone(),
                list.clauses.clone(),
            ),
        };

        if is_supported_aggregate(&name) {
            if distinct {
                return Err(unsupported(format!(
                    "{name}(DISTINCT ...) — a distinct aggregate needs a shuffle to be exact"
                )));
            }
            if !clauses.is_empty() {
                return Err(unsupported(format!("{name}(... ORDER BY/LIMIT ...)")));
            }
            let k = self.next_agg;
            self.next_agg += 1;
            return Ok(match name.as_str() {
                // COUNT of counts is a SUM. `SUM(BIGINT)` stays BIGINT in this
                // engine, so the merged type matches the single-node one.
                "COUNT" => {
                    let alias = format!("{AGG_PREFIX}{k}");
                    self.add_partial(call("COUNT", args), &alias);
                    call("SUM", vec![unnamed(ident(&alias))])
                }
                "SUM" | "MIN" | "MAX" => {
                    let alias = format!("{AGG_PREFIX}{k}");
                    self.add_partial(call(&name, args), &alias);
                    call(&name, vec![unnamed(ident(&alias))])
                }
                // AVG travels as (sum, count) and is divided exactly once, at
                // the end. The casts keep the division floating rather than
                // integral (AVG of a BIGINT column is a DOUBLE).
                //
                // The all-NULL group needs no CASE guard: a group with no
                // non-null values has `SUM = NULL` and `COUNT = 0`, and
                // `NULL / 0.0` is NULL in Arrow — which is exactly what
                // `AVG` returns there. Verified against the single-node engine
                // by `distributed_answers_match_the_single_node_answer`, whose
                // query list includes a filter that matches nothing.
                "AVG" => {
                    let sum_alias = format!("{AGG_PREFIX}{k}s");
                    let cnt_alias = format!("{AGG_PREFIX}{k}c");
                    self.add_partial(call("SUM", args.clone()), &sum_alias);
                    self.add_partial(call("COUNT", args), &cnt_alias);
                    sa::Expr::BinaryOp {
                        left: Box::new(cast_double(call("SUM", vec![unnamed(ident(&sum_alias))]))),
                        op: sa::BinaryOperator::Divide,
                        right: Box::new(cast_double(call("SUM", vec![unnamed(ident(&cnt_alias))]))),
                    }
                }
                _ => unreachable!("is_supported_aggregate covers exactly these"),
            });
        }

        if is_known_aggregate(&name) {
            return Err(unsupported(format!(
                "{name}() has no exact partial/final split"
            )));
        }

        // A scalar function: it stays where it is, with its arguments rewritten.
        let mut new_args = Vec::with_capacity(args.len());
        for a in &args {
            new_args.push(match a {
                sa::FunctionArg::Unnamed(sa::FunctionArgExpr::Expr(e)) => unnamed(self.rewrite(e)?),
                sa::FunctionArg::Named {
                    name,
                    arg: sa::FunctionArgExpr::Expr(e),
                    operator,
                } => sa::FunctionArg::Named {
                    name: name.clone(),
                    arg: sa::FunctionArgExpr::Expr(self.rewrite(e)?),
                    operator: operator.clone(),
                },
                other => {
                    return Err(unsupported(format!(
                        "function argument `{other}` in {name}()"
                    )))
                }
            });
        }
        let mut out = func.clone();
        out.args = sa::FunctionArguments::List(sa::FunctionArgumentList {
            duplicate_treatment: None,
            args: new_args,
            clauses,
        });
        Ok(sa::Expr::Function(out))
    }

    /// `SELECT <groups>, <partial aggregates> FROM t [WHERE ...] [GROUP BY ...]`
    fn partial_sql(&self, select: &sa::Select, group_exprs: &[sa::Expr]) -> String {
        let mut out = String::from("SELECT ");
        out.push_str(
            &self
                .partial_items
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", "),
        );
        out.push_str(" FROM ");
        out.push_str(&select.from[0].to_string());
        if let Some(w) = &select.selection {
            out.push_str(" WHERE ");
            out.push_str(&w.to_string());
        }
        if !group_exprs.is_empty() {
            out.push_str(" GROUP BY ");
            out.push_str(
                &group_exprs
                    .iter()
                    .map(|g| g.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
            );
        }
        out
    }

    /// `SELECT <merge exprs> FROM qe_dist_partial [GROUP BY ...] [HAVING ...]`
    fn final_sql(&self, projection: Vec<sa::SelectItem>, having: Option<sa::Expr>) -> String {
        let groups: Vec<String> = (0..self.group_keys.len())
            .map(|i| format!("{GROUP_PREFIX}{i}"))
            .collect();
        let mut out = String::from("SELECT ");
        out.push_str(
            &projection
                .iter()
                .map(|i| i.to_string())
                .collect::<Vec<_>>()
                .join(", "),
        );
        out.push_str(" FROM ");
        out.push_str(PARTIAL_TABLE);
        if !groups.is_empty() {
            out.push_str(" GROUP BY ");
            out.push_str(&groups.join(", "));
        }
        if let Some(h) = having {
            out.push_str(" HAVING ");
            out.push_str(&h.to_string());
        }
        out
    }
}

/// Net 3: re-parse the merge SQL and insist that every bare identifier it
/// mentions is an alias this module invented.
///
/// If the rewriter ever fails to consume something — an aggregate it did not
/// recognise, a column it did not map — the leftover reference is to a base
/// table column that does not exist in the partials, and it is caught here
/// rather than becoming a wrong number.
fn verify_alias_closure(final_sql: &str) -> Result<()> {
    let stmt = crate::parser::parse_sql(final_sql).map_err(|e| {
        QueryError::Internal(format!(
            "distributed rewrite produced SQL that does not parse: {e}\n  {final_sql}"
        ))
    })?;
    let text = stmt.to_string();
    // Cheap and sufficient: the merge query's identifiers are exactly the
    // generated aliases, the partial table name, and quoted output labels.
    let mut ident = String::new();
    let mut in_quotes = false;
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '"' {
            in_quotes = !in_quotes;
            continue;
        }
        if in_quotes {
            continue;
        }
        if c.is_alphanumeric() || c == '_' {
            ident.push(c);
            continue;
        }
        let done = std::mem::take(&mut ident);
        // A bare word followed by `(` is a function name, not a column.
        if !done.is_empty() && c != '(' {
            check_identifier(&done, final_sql)?;
        }
        let _ = chars.peek();
    }
    if !ident.is_empty() {
        check_identifier(&ident, final_sql)?;
    }
    Ok(())
}

fn check_identifier(word: &str, final_sql: &str) -> Result<()> {
    if word.chars().next().is_some_and(|c| c.is_ascii_digit()) {
        return Ok(());
    }
    if word.starts_with(GROUP_PREFIX) || word.starts_with(AGG_PREFIX) || word == PARTIAL_TABLE {
        return Ok(());
    }
    if SQL_KEYWORDS.contains(&word.to_uppercase().as_str()) {
        return Ok(());
    }
    Err(QueryError::Internal(format!(
        "distributed rewrite left `{word}` unresolved in the merge query, which \
         means an aggregate or column was not accounted for; refusing to run it\n  {final_sql}"
    )))
}

/// Words that may legitimately appear bare in a merge query. Anything else is a
/// column reference that should not have survived the rewrite.
const SQL_KEYWORDS: &[&str] = &[
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP",
    "BY",
    "HAVING",
    "AS",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    "AND",
    "OR",
    "NOT",
    "NULL",
    "IS",
    "IN",
    "BETWEEN",
    "CAST",
    "DOUBLE",
    "BIGINT",
    "INT",
    "INTEGER",
    "VARCHAR",
    "TEXT",
    "DECIMAL",
    "FLOAT",
    "REAL",
    "BOOLEAN",
    "DATE",
    "TIMESTAMP",
    "TRUE",
    "FALSE",
    "LIKE",
    "PRECISION",
];

fn is_supported_aggregate(name: &str) -> bool {
    matches!(name, "COUNT" | "SUM" | "MIN" | "MAX" | "AVG")
}

/// Aggregates this engine knows but M2 cannot split exactly. Kept explicit so
/// the rejection names the function instead of failing somewhere downstream.
fn is_known_aggregate(name: &str) -> bool {
    matches!(
        name,
        "STDDEV"
            | "STDDEV_POP"
            | "STDDEV_SAMP"
            | "VARIANCE"
            | "VAR"
            | "VAR_POP"
            | "VAR_SAMP"
            | "BOOL_AND"
            | "BOOL_OR"
            | "EVERY"
            | "ANY"
            | "COUNT_IF"
            | "ANY_VALUE"
            | "ARBITRARY"
            | "GEOMETRIC_MEAN"
            | "CHECKSUM"
            | "BITWISE_AND_AGG"
            | "BITWISE_OR_AGG"
            | "BITWISE_XOR_AGG"
            | "LISTAGG"
            | "STRING_AGG"
            | "GROUP_CONCAT"
            | "ARRAY_AGG"
            | "CORR"
            | "COVAR_POP"
            | "COVAR_SAMP"
            | "KURTOSIS"
            | "SKEWNESS"
            | "REGR_SLOPE"
            | "REGR_INTERCEPT"
            | "REGR_COUNT"
            | "REGR_AVGX"
            | "REGR_AVGY"
            | "APPROX_PERCENTILE"
            | "APPROX_DISTINCT"
            | "APPROX_COUNT_DISTINCT"
            | "MAX_BY"
            | "MIN_BY"
            | "MEDIAN"
            | "MODE"
            | "PERCENTILE_CONT"
            | "PERCENTILE_DISC"
            | "HISTOGRAM"
    )
}

// --- small AST constructors -------------------------------------------------

fn ident(name: &str) -> sa::Expr {
    sa::Expr::Identifier(sa::Ident::new(name.to_string()))
}

fn unnamed(e: sa::Expr) -> sa::FunctionArg {
    sa::FunctionArg::Unnamed(sa::FunctionArgExpr::Expr(e))
}

fn call(name: &str, args: Vec<sa::FunctionArg>) -> sa::Expr {
    sa::Expr::Function(sa::Function {
        name: sa::ObjectName(vec![sa::Ident::new(name.to_string())]),
        parameters: sa::FunctionArguments::None,
        args: sa::FunctionArguments::List(sa::FunctionArgumentList {
            duplicate_treatment: None,
            args,
            clauses: Vec::new(),
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: Vec::new(),
    })
}

fn cast_double(e: sa::Expr) -> sa::Expr {
    sa::Expr::Cast {
        kind: sa::CastKind::Cast,
        expr: Box::new(e),
        data_type: sa::DataType::Double,
        format: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> ExecutionContext {
        let mut c = ExecutionContext::new();
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/data/tpch-1mb");
        for t in ["lineitem", "orders", "nation"] {
            c.register_parquet(t, format!("{dir}/{t}.parquet"))
                .unwrap_or_else(|e| panic!("cannot load {t}: {e}"));
        }
        c
    }

    fn reject(sql: &str) -> String {
        match plan_distributed(&ctx(), sql) {
            Err(QueryError::NotImplemented(m)) => m,
            Err(other) => panic!("expected NotImplemented for `{sql}`, got {other:?}"),
            Ok(p) => panic!("expected `{sql}` to be REJECTED, got {p:?}"),
        }
    }

    #[test]
    fn count_star_becomes_a_sum_of_counts() {
        let p = plan_distributed(&ctx(), "SELECT COUNT(*) FROM lineitem").unwrap();
        assert_eq!(p.shape, MergeShape::TwoPhase);
        assert_eq!(p.table, "lineitem");
        assert_eq!(p.partial_sql, "SELECT COUNT(*) AS qe_a0 FROM lineitem");
        assert_eq!(
            p.final_sql.unwrap(),
            "SELECT SUM(qe_a0) AS \"COUNT(*)\" FROM qe_dist_partial"
        );
    }

    #[test]
    fn avg_travels_as_sum_and_count_never_as_an_average() {
        let p = plan_distributed(&ctx(), "SELECT AVG(l_quantity) AS q FROM lineitem").unwrap();
        assert_eq!(
            p.partial_sql,
            "SELECT SUM(l_quantity) AS qe_a0s, COUNT(l_quantity) AS qe_a0c FROM lineitem"
        );
        let f = p.final_sql.unwrap();
        assert!(
            f.contains("CAST(SUM(qe_a0s) AS DOUBLE) / CAST(SUM(qe_a0c) AS DOUBLE)"),
            "{f}"
        );
        assert!(
            !f.contains("AVG("),
            "the merge must never average an average: {f}"
        );
    }

    #[test]
    fn group_by_keys_are_transported_and_regrouped() {
        let p = plan_distributed(
            &ctx(),
            "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS s, COUNT(*) AS c \
             FROM lineitem WHERE l_shipdate <= '1998-09-01' \
             GROUP BY l_returnflag, l_linestatus",
        )
        .unwrap();
        assert_eq!(
            p.partial_sql,
            "SELECT l_returnflag AS qe_g0, l_linestatus AS qe_g1, SUM(l_quantity) AS qe_a0, \
             COUNT(*) AS qe_a1 FROM lineitem WHERE l_shipdate <= '1998-09-01' \
             GROUP BY l_returnflag, l_linestatus"
        );
        assert_eq!(
            p.final_sql.unwrap(),
            "SELECT qe_g0 AS \"l_returnflag\", qe_g1 AS \"l_linestatus\", SUM(qe_a0) AS \"s\", \
             SUM(qe_a1) AS \"c\" FROM qe_dist_partial GROUP BY qe_g0, qe_g1"
        );
    }

    #[test]
    fn arithmetic_over_aggregates_is_merged_per_aggregate() {
        let p = plan_distributed(
            &ctx(),
            "SELECT SUM(l_extendedprice * (1 - l_discount)) AS rev, \
                    SUM(l_quantity) / COUNT(*) AS ratio FROM lineitem",
        )
        .unwrap();
        // The heavy expression stays on the worker; only its SUM travels.
        assert!(p
            .partial_sql
            .contains("SUM(l_extendedprice * (1 - l_discount)) AS qe_a0"));
        let f = p.final_sql.unwrap();
        assert!(f.contains("SUM(qe_a1) / SUM(qe_a2)"), "{f}");
    }

    #[test]
    fn having_is_evaluated_after_the_merge_not_per_shard() {
        let p = plan_distributed(
            &ctx(),
            "SELECT l_returnflag, COUNT(*) AS c FROM lineitem GROUP BY l_returnflag HAVING COUNT(*) > 100",
        )
        .unwrap();
        assert!(
            !p.partial_sql.contains("HAVING"),
            "a per-shard HAVING would drop groups that only qualify once merged: {}",
            p.partial_sql
        );
        assert!(p.final_sql.unwrap().contains("HAVING SUM(qe_a1) > 100"));
    }

    #[test]
    fn a_plain_projection_is_a_concatenation() {
        let p = plan_distributed(
            &ctx(),
            "SELECT l_orderkey, l_quantity FROM lineitem WHERE l_quantity > 40",
        )
        .unwrap();
        assert_eq!(p.shape, MergeShape::Concat);
        assert!(p.final_sql.is_none());
    }

    #[test]
    fn every_unsupported_shape_is_named_in_its_rejection() {
        let cases: &[(&str, &str)] = &[
            (
                "SELECT COUNT(*) FROM lineitem, orders WHERE l_orderkey = o_orderkey",
                "cross-shard joins",
            ),
            (
                "SELECT COUNT(*) FROM lineitem JOIN orders ON l_orderkey = o_orderkey",
                "cross-shard joins",
            ),
            (
                "SELECT COUNT(DISTINCT l_orderkey) FROM lineitem",
                "COUNT(DISTINCT",
            ),
            (
                "SELECT SUM(DISTINCT l_quantity) FROM lineitem",
                "DISTINCT",
            ),
            (
                "SELECT COUNT(*) FROM lineitem WHERE l_orderkey IN (SELECT o_orderkey FROM orders)",
                "subqueries",
            ),
            (
                "SELECT COUNT(*) FROM lineitem WHERE EXISTS (SELECT 1 FROM orders)",
                "subqueries",
            ),
            (
                "SELECT COUNT(*) FROM lineitem WHERE l_quantity > (SELECT AVG(l_quantity) FROM lineitem)",
                "subqueries",
            ),
            (
                "SELECT l_orderkey FROM lineitem ORDER BY l_orderkey LIMIT 10",
                "global ORDER BY",
            ),
            ("SELECT l_orderkey FROM lineitem LIMIT 10", "global LIMIT"),
            (
                "SELECT STDDEV(l_quantity) FROM lineitem",
                "STDDEV(l_quantity) has no exact partial/final split",
            ),
            (
                "SELECT APPROX_DISTINCT(l_orderkey) FROM lineitem",
                "APPROX_DISTINCT",
            ),
            (
                "SELECT SUM(l_quantity) OVER () FROM lineitem",
                "Window function",
            ),
            (
                "SELECT DISTINCT l_returnflag FROM lineitem",
                "SELECT DISTINCT",
            ),
            (
                "SELECT COUNT(*) FROM lineitem UNION ALL SELECT COUNT(*) FROM orders",
                "set operations",
            ),
            (
                "WITH x AS (SELECT * FROM lineitem) SELECT COUNT(*) FROM x",
                "common table expressions",
            ),
            (
                "SELECT COUNT(*) FROM (SELECT * FROM lineitem) t",
                "subqueries in FROM",
            ),
        ];
        for (sql, needle) in cases {
            let msg = reject(sql);
            assert!(
                msg.to_lowercase().contains(&needle.to_lowercase()),
                "rejection for `{sql}` should mention `{needle}`, said: {msg}"
            );
            assert!(
                msg.contains("distributed execution supports"),
                "rejection for `{sql}` must say what IS supported, said: {msg}"
            );
        }
    }

    /// A column that is neither grouped nor aggregated must fail by name. The
    /// engine's binder gets there first with `ColumnNotFound`, which is a
    /// perfectly good rejection — what matters is that it is an error naming
    /// the column, never a silent answer over an arbitrary row.
    #[test]
    fn an_ungrouped_column_is_rejected_by_name() {
        let err = plan_distributed(
            &ctx(),
            "SELECT l_orderkey, COUNT(*) FROM lineitem GROUP BY l_returnflag",
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("l_orderkey"), "{err}");
    }

    /// ...and the rewriter's own version of that check, reached when the
    /// binder is happy but the merge query would reference a raw column.
    #[test]
    fn the_rewriter_refuses_a_column_it_cannot_map() {
        let mut rw = Rewriter::new(&[ident("l_returnflag")]);
        let err = rw.rewrite(&ident("l_orderkey")).unwrap_err().to_string();
        assert!(
            err.contains("l_orderkey") && err.contains("neither grouped nor aggregated"),
            "{err}"
        );
    }

    #[test]
    fn a_wildcard_over_a_group_by_is_rejected() {
        let msg = reject("SELECT * FROM lineitem GROUP BY l_returnflag");
        assert!(msg.to_lowercase().contains("wildcard"), "{msg}");
    }

    #[test]
    fn scalar_functions_over_group_keys_survive_the_rewrite() {
        let p = plan_distributed(
            &ctx(),
            "SELECT UPPER(l_returnflag) AS f, COUNT(*) AS c FROM lineitem GROUP BY l_returnflag",
        )
        .unwrap();
        assert!(p.final_sql.unwrap().contains("UPPER(qe_g0)"));
    }

    #[test]
    fn alias_closure_catches_a_leftover_column_reference() {
        // Directly exercise net 3 with SQL the rewriter would never emit.
        let err =
            verify_alias_closure("SELECT SUM(qe_a0), l_quantity FROM qe_dist_partial").unwrap_err();
        assert!(err.to_string().contains("l_quantity"), "{err}");
        // ...and accepts a well-formed merge query.
        verify_alias_closure(
            "SELECT qe_g0 AS \"l_returnflag\", CASE WHEN SUM(qe_a0c) = 0 THEN NULL \
             ELSE CAST(SUM(qe_a0s) AS DOUBLE) / CAST(SUM(qe_a0c) AS DOUBLE) END AS \"avg\" \
             FROM qe_dist_partial GROUP BY qe_g0",
        )
        .unwrap();
    }
}
