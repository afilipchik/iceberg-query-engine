//! Gather execution (M2.5): the general distributed path.
//!
//! [`plan_distributed`](crate::distributed::plan_distributed) covers the
//! shapes whose partial/final split is *exact* — and refuses everything else.
//! This module is the everything-else: joins, subqueries, `DISTINCT`,
//! `COUNT(DISTINCT)`, global `ORDER BY`/`LIMIT`, CTEs, set operations, and
//! every scalar function the engine has.
//!
//! # How it works
//!
//! ```text
//!   POST /sql?distributed=1        SELECT ... FROM a JOIN b ...
//!            |
//!   plan_gather(): base tables {a, b}, pruned to the columns the
//!                  optimized plan actually reads
//!            |
//!   per table, concurrently: enumerate splits -> assign_lpt -> fan
//!   `SELECT <cols> FROM t` out over /fragment, one shard per node
//!            |
//!   initiator registers the gathered columns as in-memory tables and
//!   runs the ORIGINAL query over them with the ordinary local engine
//! ```
//!
//! Every node scans only its own shard, so the scan work and the read
//! bandwidth are divided; the join/sort/distinct itself then runs on the
//! initiator over the moved columns. That is a real distributed scan with a
//! single-node finish — the shape Presto calls a single-fragment plan with
//! remote sources. It is NOT a shuffle: cross-node partitioned joins remain
//! M3, blocked on the defects in `.claude/plans/DISTRIBUTED-READINESS.md`.
//!
//! # Why this is correct
//!
//! The initiator sees every surviving row of every referenced table, then
//! executes the unmodified statement with the same engine a single process
//! would use. There is no rewritten aggregate, no merge arithmetic, nothing
//! to disagree with the local answer. The only differences a caller can
//! observe are row order (when the statement has no `ORDER BY`, concatenation
//! order depends on which shard answered first — exactly as it already does
//! for [`MergeShape::Concat`](crate::distributed::MergeShape)) and speed.
//!
//! # Why this does not OOM
//!
//! Gathering materializes the moved columns in initiator memory, so a bound
//! is enforced *before* any fragment is sent: the total compressed bytes
//! assigned across all gathered tables must fit in half the context's memory
//! budget. Over it, the query is refused with the numbers in the message —
//! slow-but-refused beats fast-but-dead. Column pruning is what keeps real
//! queries under the bound: only the columns the optimized plan reads are
//! moved, never `SELECT *` of a wide table because one column was needed.

use crate::error::{QueryError, Result};
use crate::execution::ExecutionContext;
use crate::planner::LogicalPlan;
use std::collections::{BTreeMap, BTreeSet};

/// One table the initiator must gather before it can run the statement.
#[derive(Clone, Debug, serde::Serialize)]
pub struct GatherTable {
    /// Base table name, as registered in the catalog.
    pub name: String,
    /// Columns to move, in table-schema order. `None` means every column
    /// (the plan reads the whole width, or the width could not be proven
    /// smaller — over-gathering is safe, under-gathering is a wrong answer).
    pub columns: Option<Vec<String>>,
    /// The exact SQL each worker runs over its shard of this table.
    pub gather_sql: String,
}

/// The general distributed plan: which tables to gather, then run `sql`.
#[derive(Clone, Debug, serde::Serialize)]
pub struct GatherPlan {
    /// Tables to gather, sorted by name so the plan (and its digest of work)
    /// is deterministic for a given statement.
    pub tables: Vec<GatherTable>,
    /// The original statement, run unmodified on the initiator afterwards.
    pub sql: String,
}

/// Decide how to answer `sql` by gathering sharded scans.
///
/// Errors with `NotImplemented` when the statement references no base table
/// (`SELECT 1` — there is nothing to distribute), and propagates the binder's
/// own error for anything the engine cannot run at all: gather widens
/// distributed support to exactly what the local engine supports, never past
/// it.
pub fn plan_gather(ctx: &ExecutionContext, sql: &str) -> Result<GatherPlan> {
    let stmt = crate::parser::parse_sql(sql)?;
    if !matches!(stmt, sqlparser::ast::Statement::Query(_)) {
        return Err(QueryError::NotImplemented(format!(
            "only SELECT can be distributed, not `{}`",
            stmt.to_string()
                .split_whitespace()
                .next()
                .unwrap_or("")
                .to_uppercase()
        )));
    }

    // The OPTIMIZED plan, because that is where projection pushdown has
    // already worked out which columns each scan reads. Using it means the
    // gather moves what execution would read, not what the parser mentions.
    let plan = ctx.optimized_plan(sql)?;

    // table -> required column names; None = all columns.
    let mut required: BTreeMap<String, Option<BTreeSet<String>>> = BTreeMap::new();
    collect_scans(ctx, &plan, &mut required)?;

    if required.is_empty() {
        return Err(QueryError::NotImplemented(
            "the statement references no base table, so there is nothing to distribute; \
             run it without distributed=1"
                .into(),
        ));
    }

    let tables = required
        .into_iter()
        .map(|(name, cols)| {
            let columns: Option<Vec<String>> = cols.map(|set| {
                // Schema order, not BTreeSet order: the gathered table should
                // look like the original one with columns removed, so a human
                // diffing the two sees omissions rather than a reshuffle.
                let provider = ctx
                    .table_provider(&name)
                    .expect("collect_scans verified the provider exists");
                provider
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .filter(|n| set.contains(n))
                    .collect()
            });
            // NEVER `SELECT *`: a wildcard's output schema carries QUALIFIED
            // field names (`supplier.s_suppkey`), and registering those
            // batches as a table makes every plain column reference in the
            // re-bound statement unresolvable. An explicit list projects
            // plain names. (Found via TPC-H Q2; see the regression test.)
            let all_columns = || {
                let provider = ctx
                    .table_provider(&name)
                    .expect("collect_scans verified the provider exists");
                provider
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>()
            };
            let projection = columns
                .clone()
                .unwrap_or_else(all_columns)
                .iter()
                .map(|c| quote_ident(c))
                .collect::<Vec<_>>()
                .join(", ");
            let gather_sql = format!("SELECT {projection} FROM {}", quote_ident(&name));
            GatherTable {
                name,
                columns,
                gather_sql,
            }
        })
        .collect();

    Ok(GatherPlan {
        tables,
        sql: sql.trim().trim_end_matches(';').to_string(),
    })
}

/// Walk the plan, recording every base-table scan and the columns it reads.
fn collect_scans(
    ctx: &ExecutionContext,
    plan: &LogicalPlan,
    required: &mut BTreeMap<String, Option<BTreeSet<String>>>,
) -> Result<()> {
    if let LogicalPlan::Scan(scan) = plan {
        // The binder resolved this name, but resolution and having a
        // provider are different facts; a scan of anything synthetic must
        // fail here, not inside a worker.
        let provider = ctx.table_provider(&scan.table_name).ok_or_else(|| {
            QueryError::NotImplemented(format!(
                "`{}` resolves to no registered table provider; only catalog tables can be \
                 gathered",
                scan.table_name
            ))
        })?;
        let full = provider.schema();

        let cols = match &scan.projection {
            // No projection recorded: the scan reads the whole width.
            None => None,
            Some(indices) => {
                let mut set: BTreeSet<String> = indices
                    .iter()
                    .filter_map(|&i| full.fields().get(i).map(|f| f.name().clone()))
                    .collect();
                // A filter pushed INTO the scan reads columns the projection
                // may exclude, and re-binding the original statement over the
                // gathered table needs them. Same loose name matching the
                // pushdown rule itself uses.
                if let Some(filter) = &scan.filter {
                    let mut filter_cols = Vec::new();
                    collect_expr_columns(filter, &mut filter_cols);
                    for field in full.fields() {
                        if filter_cols.iter().any(|c| {
                            c == field.name()
                                || c.ends_with(&format!(".{}", field.name()))
                                || field.name().ends_with(&format!(".{c}"))
                        }) {
                            set.insert(field.name().clone());
                        }
                    }
                }
                if set.is_empty() {
                    // `SELECT COUNT(*)`-shaped scans read no column but every
                    // row; one column carries the row count.
                    if let Some(f) = full.fields().first() {
                        set.insert(f.name().clone());
                    }
                }
                Some(set)
            }
        };

        // Merge with what other scans of the same table (self-joins,
        // subqueries) already require. Widest wins: None absorbs everything.
        match (required.get_mut(&scan.table_name), cols) {
            (None, cols) => {
                required.insert(scan.table_name.clone(), cols);
            }
            (Some(existing @ Some(_)), Some(more)) => {
                if let Some(set) = existing.as_mut() {
                    set.extend(more);
                }
            }
            (Some(existing), None) => *existing = None,
            (Some(None), Some(_)) => {}
        }
        return Ok(());
    }

    for child in plan.children() {
        collect_scans(ctx, child, required)?;
    }
    Ok(())
}

/// Column names an expression mentions.
///
/// Scan-level filters are simple predicates — the pushdown rule never embeds a
/// subquery in one — so a walk over the value-expression variants is complete.
/// If a variant is ever missed, the failure mode is a missing column at
/// re-bind (`ColumnNotFound`, loud), never a wrong answer.
fn collect_expr_columns(e: &crate::planner::Expr, out: &mut Vec<String>) {
    use crate::planner::Expr;
    match e {
        Expr::Column(c) => out.push(c.name.clone()),
        Expr::BinaryExpr { left, right, .. } => {
            collect_expr_columns(left, out);
            collect_expr_columns(right, out);
        }
        Expr::UnaryExpr { expr, .. } | Expr::Cast { expr, .. } | Expr::Alias { expr, .. } => {
            collect_expr_columns(expr, out)
        }
        Expr::Aggregate { args, .. } | Expr::ScalarFunc { args, .. } => {
            for a in args {
                collect_expr_columns(a, out);
            }
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            if let Some(o) = operand {
                collect_expr_columns(o, out);
            }
            for (w, t) in when_then {
                collect_expr_columns(w, out);
                collect_expr_columns(t, out);
            }
            if let Some(el) = else_expr {
                collect_expr_columns(el, out);
            }
        }
        Expr::InList { expr, list, .. } => {
            collect_expr_columns(expr, out);
            for i in list {
                collect_expr_columns(i, out);
            }
        }
        Expr::Between {
            expr, low, high, ..
        } => {
            collect_expr_columns(expr, out);
            collect_expr_columns(low, out);
            collect_expr_columns(high, out);
        }
        _ => {}
    }
}

/// Double-quote an identifier, escaping embedded quotes.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    fn ctx_with_tables() -> ExecutionContext {
        let mut ctx = ExecutionContext::new();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("price", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        ctx.register_table("items", schema.clone(), vec![batch.clone()]);
        ctx.register_table("others", schema, vec![batch]);
        ctx
    }

    #[test]
    fn a_join_lists_both_tables() {
        let ctx = ctx_with_tables();
        let p = plan_gather(
            &ctx,
            "SELECT i.name FROM items i JOIN others o ON i.id = o.id",
        )
        .unwrap();
        let names: Vec<_> = p.tables.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["items", "others"]);
    }

    #[test]
    fn columns_are_pruned_to_what_the_plan_reads() {
        let ctx = ctx_with_tables();
        let p = plan_gather(&ctx, "SELECT name FROM items WHERE id > 1").unwrap();
        let t = &p.tables[0];
        let cols = t.columns.as_ref().expect("pruned");
        assert!(cols.contains(&"id".to_string()), "filter column: {cols:?}");
        assert!(cols.contains(&"name".to_string()), "projected: {cols:?}");
        assert!(
            !cols.contains(&"price".to_string()),
            "price is unreferenced and must not be moved: {cols:?}"
        );
    }

    /// TPC-H Q2 regression: every column the statement mentions must survive
    /// pruning — Q2's mix of a correlated MIN subquery, five tables and an
    /// ORDER BY is where a projection-based collector is most likely to lose
    /// one (s_acctbal, historically).
    #[test]
    fn q2_pruning_keeps_every_mentioned_column() {
        let mut ctx = ExecutionContext::new();
        for t in [
            "part", "supplier", "partsupp", "nation", "region", "customer", "orders", "lineitem",
        ] {
            let path = format!("{}/data/tpch-1mb/{t}.parquet", env!("CARGO_MANIFEST_DIR"));
            ctx.register_parquet(t, &path).expect("fixture data");
        }
        let sql = crate::tpch::get_query(2).unwrap();
        let p = plan_gather(&ctx, sql).unwrap();
        let supplier = p
            .tables
            .iter()
            .find(|t| t.name == "supplier")
            .expect("supplier is gathered");
        if let Some(cols) = &supplier.columns {
            for c in ["s_acctbal", "s_name", "s_address", "s_phone", "s_comment"] {
                assert!(
                    cols.contains(&c.to_string()),
                    "Q2 mentions supplier.{c}; pruned to {cols:?}"
                );
            }
        }
        // And no gather may use a wildcard: `SELECT *` projects QUALIFIED
        // output names (supplier.s_suppkey), which cannot be re-bound once
        // registered as a table. Q2's supplier scan is exactly the case that
        // hits it (its scan keeps every column, so pruning yields "all").
        for t in &p.tables {
            assert!(
                !t.gather_sql.contains('*'),
                "gather of `{}` uses a wildcard: {}",
                t.name,
                t.gather_sql
            );
        }
    }

    #[test]
    fn a_statement_with_no_table_is_refused() {
        let ctx = ctx_with_tables();
        let err = plan_gather(&ctx, "SELECT 1").unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
    }

    #[test]
    fn non_select_is_refused() {
        let ctx = ctx_with_tables();
        let err = plan_gather(&ctx, "DROP TABLE items").unwrap_err();
        assert!(matches!(err, QueryError::NotImplemented(_)), "{err:?}");
    }

    #[test]
    fn self_join_unions_the_column_sets() {
        let ctx = ctx_with_tables();
        let p = plan_gather(
            &ctx,
            "SELECT a.name FROM items a JOIN items b ON a.id = b.price",
        )
        .unwrap();
        assert_eq!(p.tables.len(), 1);
        // Both sides' columns must be present. Whether that is expressed as
        // the explicit union or as `None` (all columns) is the optimizer's
        // call — over-gathering is safe, under-gathering never is.
        if let Some(cols) = p.tables[0].columns.as_ref() {
            for c in ["id", "name", "price"] {
                assert!(cols.contains(&c.to_string()), "{c} missing from {cols:?}");
            }
        }
    }
}
