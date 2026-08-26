//! Rollup matching/substitution: the mechanism task 001 of the
//! native-tables-rollups epic exists to prove. Given a query's freshly
//! BOUND (never optimized) `LogicalPlan`, decide whether it exactly
//! matches a registered rollup's defining shape and, if so, rewrite the
//! plan to scan the rollup's own native-table storage instead of the
//! base table -- transparently, and only when provably correct.
//!
//! # Why this cannot be an `OptimizerRule`
//!
//! `OptimizerRule::optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan>`
//! has no catalog/registry parameter by design (`src/optimizer/mod.rs`) --
//! every existing rule is deliberately plan-local, closed over nothing but
//! the plan tree in front of it. Answering "is there a registered rollup
//! for this base table with this exact GROUP BY/aggregate set, and is it
//! still fresh" needs to consult the CURRENT set of registered tables
//! (`ExecutionContext::tables`) at the moment a query runs, which is
//! exactly the kind of external, mutable, session-scoped state an
//! `OptimizerRule` cannot see. This module is therefore plain functions
//! operating on plans and an explicit `&[RollupCandidate]` snapshot handed
//! in by the caller -- `ExecutionContext` (`src/execution/context.rs`)
//! builds that snapshot from `self.tables` and calls
//! [`substitute`] itself, positioned BEFORE `Optimizer::optimize()` in its
//! own execution flow (see that file's `substitute_rollups`/
//! `rollup_candidates` methods and their doc for exactly why before, not
//! after or inside, the optimizer pipeline).
//!
//! # The shape this task recognizes
//!
//! ```text
//! Project(exprs = bare, optionally-aliased column refs into the Aggregate's own schema)
//!   Aggregate(group_by: Vec<Expr>, aggregates: Vec<Expr>)
//!     Scan(table_name)
//! ```
//!
//! This is exactly what `Binder::bind_select` produces, unmodified, for a
//! plain `SELECT <cols/aggs> FROM <table> GROUP BY <cols>` query with no
//! WHERE/JOIN/HAVING (`bind_select` always wraps a GROUP BY query's output
//! in a trailing `Project`, even when the SELECT list is nothing but the
//! aggregate's own columns -- see that function's own step 5). Matching
//! against the RAW bound plan, before the ordinary optimizer pipeline
//! runs, is a deliberate choice: several optimizer rules (`GroupKeyReduction`,
//! `PackedGroupKeys`, `EagerAggregation`, `ProjectionPushdown`, ...) rewrite
//! an `AggregateNode`'s own `group_by`/`aggregates` fields or push work into
//! the `Scan` node, any of which could make an incoming query's optimized
//! shape diverge from a rollup's recorded shape in ways that have nothing to
//! do with whether the two queries are actually equivalent. Matching on the
//! bound plan sidesteps that coupling entirely: the shape a rollup's own
//! definition is recorded from (see [`require_rollup_defining_shape`]) and
//! the shape an incoming query is matched against are both "straight out of
//! the binder," so nothing but the two SQL texts' own structure can make
//! them differ.
//!
//! `recognize` deliberately requires the `Aggregate`'s `input` to be a bare
//! `Scan` -- never a `Filter(Scan)`. A WHERE clause anywhere (in either the
//! rollup's defining query or an incoming candidate query) therefore always
//! misses this shape and correctly falls back to the base-table plan; this
//! task ships single-table, unfiltered, exact-match rollups only (per the
//! epic's own "narrow slice first" architecture decision) and does not
//! attempt to reason about whether a filter is compatible with a rollup's
//! own row set. A `HAVING` clause has the identical effect for the same
//! reason (`bind_select` wraps a `HAVING`-bearing aggregate in an extra
//! `Filter` between the `Aggregate` and the outer `Project`).
//!
//! `substitute` recurses through every OTHER plan shape unchanged
//! (`ORDER BY`/`LIMIT`/`DISTINCT`/`UNION` wrapping a matchable aggregate all
//! keep working, exactly like `VectorSearchPushdown`'s own recursive walk),
//! with one honest limitation: a subquery embedded inside an `Expr` (e.g. a
//! correlated `EXISTS`/scalar subquery in a `WHERE`/`SELECT` expression) is
//! never visited, since `LogicalPlan::children()` does not expose the plans
//! `Expr::ScalarSubquery`/`Exists`/`InSubquery` carry internally. A rollup
//! opportunity nested inside such a subquery is therefore never matched --
//! named here as a known, deliberate gap, not a silent one.
//!
//! # Matching semantics -- decided explicitly, not left implicit
//!
//! * **Base table**: the incoming query's `Scan.table_name` must
//!   case-insensitively equal the rollup's own recorded `base_table`.
//! * **GROUP BY / aggregate SETs, order-independent**: `GROUP BY a, b`
//!   matches a rollup defined with `GROUP BY b, a` -- both canonicalize to
//!   the SAME *sorted* list of shape keys (see [`RollupMeta::group_by_key_set`]).
//!   This is a sorted-MULTISET comparison, not a `HashSet` one: element
//!   COUNT matters too (`GROUP BY a, a` would need a rollup also listing
//!   `a` twice), so nothing is silently collapsed. The aggregate SET
//!   (`SELECT SUM(x), COUNT(*)` vs `SELECT COUNT(*), SUM(x)`) is compared
//!   the identical way.
//! * **Column aliasing never affects matching**: `SUM(l_quantity) AS
//!   sum_qty` and `SUM(l_quantity) AS total_quantity` canonicalize
//!   IDENTICALLY (`canonical_expr_key` strips every `Expr::Alias` wrapper
//!   before rendering a key) -- the alias only ever affects the OUTPUT
//!   column name, which the substituted plan preserves verbatim from the
//!   ORIGINAL query regardless of which rollup (if any) answered it.
//! * **Table qualification is stripped, safely**: `l_returnflag`,
//!   `lineitem.l_returnflag` and `l.l_returnflag` (an alias) all
//!   canonicalize to the same `col:l_returnflag` key. This is only safe
//!   because task 001's scope is single-base-table, no-JOIN rollups --
//!   `recognize` structurally guarantees exactly one `Scan` is in play, so
//!   there is no ambiguity a qualifier could ever be resolving.
//! * **The requested aggregate SET must match exactly, not be a subset**:
//!   a query wanting `SUM(x)` alone does not match a rollup that only
//!   stores `SUM(x), COUNT(*)` (or vice versa) -- both directions are a
//!   miss. Coarser/subsumption matching (a rollup covering MORE than a
//!   query needs) is explicitly out of scope for this task, named by the
//!   epic itself as later, harder work ("a genuinely new algorithm class").
//! * **The SELECT list may reorder/alias/omit trailing columns freely, but
//!   every item must be a bare column reference** into the `Aggregate`'s
//!   own output columns -- never a computed expression over them. A query
//!   whose SELECT list only projects SOME of the GROUP BY/aggregate
//!   columns (e.g. `SELECT l_returnflag, SUM(l_quantity) FROM lineitem
//!   GROUP BY l_returnflag, l_linestatus`, omitting `l_linestatus` from
//!   the output while still grouping by it) still matches correctly: the
//!   GROUP BY/aggregate SET equality check is against the `Aggregate`
//!   node's OWN `group_by`/`aggregates` fields, entirely independent of
//!   what the outer `Project` happens to expose.
//!
//! # Staleness
//!
//! A candidate rollup is excluded from matching entirely (as if it were
//! never registered) whenever the base table's CURRENT `(table_id,
//! version)` differs from what was recorded in `RollupMeta` at
//! registration/last-refresh time -- see `rollup_candidates` in
//! `execution/context.rs`, which builds the `&[RollupCandidate]` slice
//! this module's functions receive. Task 001 only needs this fact to
//! EXIST and be enforced (falling back correctly for a stale rollup); the
//! refresh model that keeps `RollupMeta` current automatically on every
//! base-table mutation is task 003's job, not attempted here.

use crate::error::{QueryError, Result};
use crate::planner::{Column, Expr, LogicalPlan, PlanSchema, ProjectNode, ScanNode};
use crate::storage::native_manifest::RollupColumn;
use arrow::datatypes::Schema;
use std::sync::Arc;

// ============================================================================
// Canonicalization: turn an `Expr` into an order/alias/qualification-blind
// shape key.
// ============================================================================

/// Strip every `Expr::Alias` wrapper -- an alias is a display name for an
/// OUTPUT column, never part of what makes two expressions the same
/// underlying computation.
fn strip_alias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias { expr, .. } => strip_alias(expr),
        other => other,
    }
}

/// Render `e` as a deterministic, alias-blind, table-qualification-blind
/// string key: two expressions with the same key are, for this task's
/// purposes, "the same GROUP BY column" or "the same requested aggregate."
///
/// This is this task's own answer to a question `.claude/epics/
/// native-tables-rollups/001.md` poses explicitly: the codebase's two
/// existing plan-comparison precedents (`optimizer/mod.rs`'s fixpoint-loop
/// change detection, `physical/operators/subquery.rs`'s `plan_hash`) both
/// use a Debug-string hash for EXACT identity, never semantic equivalence.
/// Decision: that precedent is reused, but not blindly -- a bare
/// `format!("{e:?}")` would treat `SUM(x) AS a` and `SUM(x) AS b` as
/// different keys (the alias is part of `Expr::Alias`'s own `Debug` output)
/// and would treat `l_returnflag` and `lineitem.l_returnflag` as different
/// keys (the `Column.relation` field differs). This function is therefore a
/// small, explicit NORMALIZATION pass (strip aliases, drop column relation
/// qualifiers) applied recursively BEFORE falling back to a Debug-based
/// rendering of each node's own "tag" (its enum variant plus any operator/
/// function/type value, all of which already derive `Debug` and are
/// deterministic) -- combining the codebase's existing precedent with the
/// semantic normalization this task's matching requires, rather than
/// picking one or the other.
pub fn canonical_expr_key(e: &Expr) -> String {
    match e {
        Expr::Alias { expr, .. } => canonical_expr_key(expr),
        Expr::Column(c) => format!("col:{}", c.name.to_lowercase()),
        Expr::Literal(v) => format!("lit:{v:?}"),
        Expr::BinaryExpr { left, op, right } => format!(
            "bin:{op:?}({},{})",
            canonical_expr_key(left),
            canonical_expr_key(right)
        ),
        Expr::UnaryExpr { op, expr } => format!("un:{op:?}({})", canonical_expr_key(expr)),
        Expr::Aggregate {
            func,
            args,
            distinct,
        } => format!(
            "agg:{func:?}:distinct={distinct}({})",
            args.iter()
                .map(canonical_expr_key)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Expr::ScalarFunc { func, args } => format!(
            "fn:{func:?}({})",
            args.iter()
                .map(canonical_expr_key)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Expr::Cast { expr, data_type } => {
            format!("cast:{data_type:?}({})", canonical_expr_key(expr))
        }
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let op = operand
                .as_ref()
                .map(|e| canonical_expr_key(e))
                .unwrap_or_default();
            let wt = when_then
                .iter()
                .map(|(w, t)| format!("{}=>{}", canonical_expr_key(w), canonical_expr_key(t)))
                .collect::<Vec<_>>()
                .join(";");
            let el = else_expr
                .as_ref()
                .map(|e| canonical_expr_key(e))
                .unwrap_or_default();
            format!("case:{op}[{wt}]else:{el}")
        }
        Expr::InList {
            expr,
            list,
            negated,
        } => format!(
            "in:negated={negated}:{}[{}]",
            canonical_expr_key(expr),
            list.iter()
                .map(canonical_expr_key)
                .collect::<Vec<_>>()
                .join(",")
        ),
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => format!(
            "between:negated={negated}:{}[{},{}]",
            canonical_expr_key(expr),
            canonical_expr_key(low),
            canonical_expr_key(high)
        ),
        // Subqueries and window functions cannot appear inside the narrow
        // GROUP-BY/aggregate shape `recognize` accepts (a bare Scan has no
        // outer rows for a correlated subquery to reference, and window
        // functions live in the SELECT list, not GROUP BY/aggregate args).
        // Rendered via a stable Debug fallback so this function stays
        // TOTAL (never panics on an unexpected shape) rather than because
        // any test is expected to exercise this arm.
        other => format!("dbg:{other:?}"),
    }
}

// ============================================================================
// Recognizing the shape, on either the rollup-definition side or the
// incoming-query side -- the SAME function serves both (see module doc).
// ============================================================================

/// One SELECT-list position of a recognized aggregate query: which
/// GROUP BY/aggregate expression (by canonical key) it exposes, and
/// whether that expression is a GROUP BY column (`true`) or an aggregate
/// (`false`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollupSlot {
    pub canonical_key: String,
    pub is_group_by: bool,
}

/// The result of successfully recognizing the `Project(Aggregate(Scan))`
/// shape at some node of a plan -- everything both registration
/// (`require_rollup_defining_shape`) and matching (`substitute`) need.
#[derive(Debug, Clone)]
pub struct RecognizedAggregateQuery {
    /// The `Scan` node's table name, verbatim (not lowercased -- callers
    /// compare case-insensitively where that matters).
    pub table_name: String,
    /// `AggregateNode.group_by`'s canonical keys, in SOURCE order (i.e.
    /// the order the SQL's `GROUP BY` clause listed them).
    pub group_by_keys: Vec<String>,
    /// `AggregateNode.aggregates`'s canonical keys, in SOURCE order (the
    /// order aggregate expressions were first encountered in the SELECT
    /// list, per `Binder::extract_aggregates`).
    pub aggregate_keys: Vec<String>,
    /// One entry per top-level `Project.exprs` position, in order.
    pub proj_slots: Vec<RollupSlot>,
    /// The top-level `Project`'s own output schema, verbatim -- reused
    /// unchanged as the substituted plan's schema so a matched query's
    /// output shape (field names/types/nullability) is bit-for-bit
    /// identical to what the unsubstituted base-table plan would have
    /// produced.
    pub proj_schema: PlanSchema,
}

impl RecognizedAggregateQuery {
    /// The order-independent GROUP BY key SET (sorted; see module doc on
    /// why sorted-multiset, not a true set).
    pub fn sorted_group_by_keys(&self) -> Vec<String> {
        let mut keys = self.group_by_keys.clone();
        keys.sort();
        keys
    }

    /// The order-independent aggregate key SET, same reasoning.
    pub fn sorted_aggregate_keys(&self) -> Vec<String> {
        let mut keys = self.aggregate_keys.clone();
        keys.sort();
        keys
    }
}

/// Recognize the `Project(Aggregate(Scan))` shape AT `plan`'s own top
/// node (never recurses -- callers that want to search a whole tree use
/// [`substitute`], which walks down through every other shape). Returns
/// `None` for anything else: a `Filter`/`Join` between `Scan` and
/// `Aggregate`, a computed (non-bare-column) SELECT item, more than one
/// `Scan` (impossible here structurally, since `Aggregate.input` is a
/// single child), or any other plan shape entirely.
pub fn recognize(plan: &LogicalPlan) -> Option<RecognizedAggregateQuery> {
    let LogicalPlan::Project(proj) = plan else {
        return None;
    };
    let LogicalPlan::Aggregate(agg) = proj.input.as_ref() else {
        return None;
    };
    let LogicalPlan::Scan(scan) = agg.input.as_ref() else {
        return None;
    };
    // Defensive: on the RAW bound plan (what this module always matches
    // against -- see module doc) a Scan's own `filter` is always `None`
    // anyway, since predicate pushdown is an optimizer rule that has not
    // run yet. A WHERE clause instead shows up as an explicit
    // `LogicalPlan::Filter` between Scan and Aggregate, already refused
    // by the match above.
    if scan.filter.is_some() {
        return None;
    }

    let group_by_keys: Vec<String> = agg.group_by.iter().map(canonical_expr_key).collect();
    let aggregate_keys: Vec<String> = agg.aggregates.iter().map(canonical_expr_key).collect();

    // `agg.schema`'s fields are GROUP BY fields (0..g) followed by
    // aggregate fields (g..g+a), exactly the order `Binder::bind_select`
    // builds `agg_fields` in -- see that function's own construction.
    let mut proj_slots = Vec::with_capacity(proj.exprs.len());
    for e in &proj.exprs {
        let Expr::Column(c) = strip_alias(e) else {
            return None;
        };
        let slot = agg
            .schema
            .fields()
            .iter()
            .position(|f| f.name.eq_ignore_ascii_case(&c.name))?;
        let (canonical_key, is_group_by) = if slot < agg.group_by.len() {
            (group_by_keys[slot].clone(), true)
        } else {
            (aggregate_keys[slot - agg.group_by.len()].clone(), false)
        };
        proj_slots.push(RollupSlot {
            canonical_key,
            is_group_by,
        });
    }

    Some(RecognizedAggregateQuery {
        table_name: scan.table_name.clone(),
        group_by_keys,
        aggregate_keys,
        proj_slots,
        proj_schema: proj.schema.clone(),
    })
}

/// [`recognize`], turned into a descriptive `Err` for registration
/// (`ExecutionContext::register_rollup`) instead of a silent `None` --
/// registering a rollup whose defining SQL isn't this shape should fail
/// loudly, naming exactly what is required, not just decline to match
/// later. Also checks the recognized `Scan` targets `expected_base_table`
/// (case-insensitive) and that there is at least one GROUP BY column or
/// aggregate (a rollup with neither has nothing to roll up).
pub fn require_rollup_defining_shape(
    plan: &LogicalPlan,
    expected_base_table: &str,
) -> Result<RecognizedAggregateQuery> {
    let recognized = recognize(plan).ok_or_else(|| {
        QueryError::NotImplemented(format!(
            "register_rollup: the defining query must bind to exactly `SELECT <GROUP BY \
             column(s) and/or aggregate(s), any order, any aliases, each referenced at most \
             once> FROM {expected_base_table} GROUP BY <...>` — no WHERE/JOIN/HAVING/ORDER \
             BY/LIMIT/DISTINCT, and every SELECT item must be a bare (optionally aliased) \
             column reference to one of the GROUP BY columns or aggregates, never a computed \
             expression over them. This epic ships exact-match, single-base-table rollups \
             only (native-tables-rollups epic, task 001's deliberate narrow scope)."
        ))
    })?;
    if !recognized
        .table_name
        .eq_ignore_ascii_case(expected_base_table)
    {
        return Err(QueryError::InvalidArgument(format!(
            "register_rollup: the defining query scans `{}`, but the rollup is being \
             registered against base table `{expected_base_table}` — they must match",
            recognized.table_name
        )));
    }
    if recognized.group_by_keys.is_empty() && recognized.aggregate_keys.is_empty() {
        return Err(QueryError::InvalidArgument(
            "register_rollup: the defining query has neither a GROUP BY column nor an \
             aggregate — nothing to roll up"
                .to_string(),
        ));
    }
    Ok(recognized)
}

/// Build a rollup's `RollupColumn` list from its (already validated)
/// recognized defining-query shape and the schema it was actually WRITTEN
/// under (`ExecutionContext::register_rollup`'s `write_schema`, the same
/// qualification-stripped/dictionary-normalized schema
/// `create_table_as_select` computes via `output_schema_for_native_write`
/// for the identical reason). `recognized.proj_slots` and `write_schema`'s
/// fields are the SAME SELECT-list positions in the SAME order (writing
/// never reorders or drops columns), so this is a plain positional zip —
/// the length-mismatch check exists only to turn a future refactor bug
/// into a named `Err` instead of a panic or a silently misaligned mapping.
pub fn build_rollup_columns(
    recognized: &RecognizedAggregateQuery,
    write_schema: &Schema,
) -> Result<Vec<RollupColumn>> {
    if recognized.proj_slots.len() != write_schema.fields().len() {
        return Err(QueryError::Internal(format!(
            "register_rollup: defining query has {} projected column(s) but the written \
             table has {} — internal shape mismatch",
            recognized.proj_slots.len(),
            write_schema.fields().len()
        )));
    }
    Ok(recognized
        .proj_slots
        .iter()
        .zip(write_schema.fields())
        .map(|(slot, field)| RollupColumn {
            canonical_key: slot.canonical_key.clone(),
            physical_name: field.name().clone(),
            is_group_by: slot.is_group_by,
        })
        .collect())
}

// ============================================================================
// Matching + substitution against a live registry snapshot.
// ============================================================================

/// One registered, non-stale rollup, as a self-contained snapshot handed
/// to this module's pure functions by `ExecutionContext` (which alone has
/// real registry access — see module doc). Building this list IS where
/// staleness is enforced: `ExecutionContext::rollup_candidates` excludes a
/// rollup whose recorded `(base_table_id, base_table_version)` no longer
/// matches the base table's CURRENT identity, so by the time a
/// `RollupCandidate` reaches this module it is always eligible to match —
/// this module itself does not re-check staleness.
#[derive(Debug, Clone)]
pub struct RollupCandidate {
    /// The name this rollup is registered under in `ExecutionContext::
    /// tables` — becomes the substituted `Scan`'s own `table_name`.
    pub registered_name: String,
    pub meta: crate::storage::native_manifest::RollupMeta,
    /// The rollup's own LOGICAL schema (`TableProvider::schema()`,
    /// dictionary-decoded — see `NativeTable::schema`'s own doc), used to
    /// build the substituted `Scan` node's declared schema.
    pub schema: PlanSchema,
}

/// Find the first candidate whose base table, GROUP BY key set, and
/// aggregate key set all match `recognized` exactly. Returns the FIRST
/// match in `candidates`' order — task 001 does not attempt to choose
/// among multiple equally-valid rollups (a real question a future task
/// could revisit; with exact-match semantics only, two candidates
/// matching the SAME query would have to be functionally interchangeable
/// duplicates of each other in the first place).
fn find_match<'a>(
    recognized: &RecognizedAggregateQuery,
    candidates: &'a [RollupCandidate],
) -> Option<&'a RollupCandidate> {
    let want_group_by = recognized.sorted_group_by_keys();
    let want_aggregates = recognized.sorted_aggregate_keys();
    candidates.iter().find(|c| {
        c.meta
            .base_table
            .eq_ignore_ascii_case(&recognized.table_name)
            && c.meta.group_by_key_set() == want_group_by
            && c.meta.aggregate_key_set() == want_aggregates
    })
}

/// Build the substituted plan for a confirmed match: a `Scan` against the
/// rollup's own native-table storage, reshaped by a `Project` so the
/// output is bit-for-bit the same shape (field names/types/order) the
/// ORIGINAL, unsubstituted query would have produced. Returns `None`
/// (decline, fall back — never a wrong answer) if any output column's
/// TYPE, as actually stored in the rollup, disagrees with what the
/// original query's own `Aggregate`/`Project` inferred — a defensive
/// check that should never fire in practice (both sides compute the
/// identical aggregate expression) but costs nothing to keep as a second
/// line of defense against ever silently mis-typing a result.
fn build_substituted_plan(
    recognized: &RecognizedAggregateQuery,
    candidate: &RollupCandidate,
) -> Option<LogicalPlan> {
    let mut exprs = Vec::with_capacity(recognized.proj_slots.len());
    for (slot, out_field) in recognized
        .proj_slots
        .iter()
        .zip(recognized.proj_schema.fields())
    {
        let physical_name = candidate.meta.physical_name_for(&slot.canonical_key)?;
        let (_, src_field) = candidate
            .schema
            .resolve_column(&Column::new(physical_name))?;
        if src_field.data_type != out_field.data_type {
            return None;
        }
        let expr =
            Expr::Column(Column::new(physical_name.to_string())).alias(out_field.name.clone());
        exprs.push(expr);
    }

    let scan = LogicalPlan::Scan(ScanNode {
        table_name: candidate.registered_name.clone(),
        schema: candidate.schema.clone(),
        projection: None,
        filter: None,
    });

    Some(LogicalPlan::Project(ProjectNode {
        input: Arc::new(scan),
        exprs,
        schema: recognized.proj_schema.clone(),
    }))
}

fn debug_enabled() -> bool {
    std::env::var("QE_DEBUG_ROLLUP").is_ok()
}

fn try_match_and_substitute(
    plan: &LogicalPlan,
    candidates: &[RollupCandidate],
    matched: &mut Vec<String>,
) -> Option<LogicalPlan> {
    let recognized = recognize(plan)?;
    let debug = debug_enabled();
    match find_match(&recognized, candidates) {
        Some(candidate) => match build_substituted_plan(&recognized, candidate) {
            Some(new_plan) => {
                if debug {
                    eprintln!(
                        "[rollup] MATCH: query over `{}` (group_by={:?}, aggregates={:?}) \
                         answered by rollup `{}`",
                        recognized.table_name,
                        recognized.sorted_group_by_keys(),
                        recognized.sorted_aggregate_keys(),
                        candidate.registered_name
                    );
                }
                matched.push(candidate.registered_name.clone());
                Some(new_plan)
            }
            None => {
                if debug {
                    eprintln!(
                        "[rollup] DECLINE: rollup `{}` matched the key sets for `{}` but a \
                         reshape column/type check failed — falling back to the base table",
                        candidate.registered_name, recognized.table_name
                    );
                }
                None
            }
        },
        None => {
            if debug {
                eprintln!(
                    "[rollup] NO MATCH: query over `{}` (group_by={:?}, aggregates={:?}) — no \
                     registered, non-stale rollup matches ({} candidate(s) considered)",
                    recognized.table_name,
                    recognized.sorted_group_by_keys(),
                    recognized.sorted_aggregate_keys(),
                    candidates.len()
                );
            }
            None
        }
    }
}

/// Walk `plan` looking for a subtree matching the shape in the module
/// doc, substituting the FIRST (outermost) match found at each independent
/// branch and recursing unchanged through everything else — the same
/// recursive shape `VectorSearchPushdown` uses (`src/optimizer/rules/
/// vector_search.rs`), adapted to this task's registry-snapshot signature.
/// Every rollup name that actually answered some subtree is appended to
/// `matched`, in the order encountered — this is the provenance record
/// `ExecutionContext::sql()` surfaces as `QueryMetrics::rollup_answered`
/// (G3/PRD G5: never silently indistinguishable from the base-table path).
pub fn substitute(
    plan: &LogicalPlan,
    candidates: &[RollupCandidate],
    matched: &mut Vec<String>,
) -> Result<LogicalPlan> {
    if let Some(new_plan) = try_match_and_substitute(plan, candidates, matched) {
        return Ok(new_plan);
    }
    let children: Result<Vec<Arc<LogicalPlan>>> = plan
        .children()
        .iter()
        .map(|c| substitute(c, candidates, matched).map(Arc::new))
        .collect();
    Ok(plan.with_new_children(children?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{AggregateFunction, AggregateNode, FilterNode, SchemaField};
    use crate::storage::native_manifest::RollupMeta;
    use arrow::datatypes::DataType;

    fn lineitem_schema() -> PlanSchema {
        PlanSchema::new(vec![
            SchemaField::new("l_returnflag", DataType::Utf8),
            SchemaField::new("l_linestatus", DataType::Utf8),
            SchemaField::new("l_quantity", DataType::Float64),
        ])
    }

    fn scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            table_name: "lineitem".into(),
            schema: lineitem_schema(),
            projection: None,
            filter: None,
        })
    }

    /// `SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty,
    /// COUNT(*) AS count_order FROM lineitem GROUP BY l_returnflag,
    /// l_linestatus` — the PRD's own worked example, hand-built the same
    /// way `Binder::bind_select` would (agg schema = group_by fields then
    /// aggregate fields; outer Project = bare column refs, aliased).
    fn worked_example(group_by_order: [&str; 2]) -> LogicalPlan {
        let group_by = vec![
            Expr::column(group_by_order[0]),
            Expr::column(group_by_order[1]),
        ];
        let aggregates = vec![
            Expr::Aggregate {
                func: AggregateFunction::Sum,
                args: vec![Expr::column("l_quantity")],
                distinct: false,
            },
            Expr::Aggregate {
                func: AggregateFunction::Count,
                args: vec![],
                distinct: false,
            },
        ];
        let agg_schema = PlanSchema::new(vec![
            SchemaField::new(group_by_order[0], DataType::Utf8),
            SchemaField::new(group_by_order[1], DataType::Utf8),
            SchemaField::new("sum_qty", DataType::Float64),
            SchemaField::new("count_order", DataType::Int64),
        ]);
        let agg = LogicalPlan::Aggregate(AggregateNode {
            input: Arc::new(scan()),
            group_by,
            aggregates,
            schema: agg_schema,
        });
        let proj_schema = PlanSchema::new(vec![
            SchemaField::new("l_returnflag", DataType::Utf8),
            SchemaField::new("l_linestatus", DataType::Utf8),
            SchemaField::new("sum_qty", DataType::Float64),
            SchemaField::new("count_order", DataType::Int64),
        ]);
        LogicalPlan::Project(ProjectNode {
            input: Arc::new(agg),
            exprs: vec![
                Expr::column("l_returnflag"),
                Expr::column("l_linestatus"),
                Expr::column("sum_qty"),
                Expr::column("count_order"),
            ],
            schema: proj_schema,
        })
    }

    fn rollup_meta_for_worked_example() -> RollupMeta {
        RollupMeta {
            base_table: "lineitem".into(),
            defining_sql: "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, \
                            COUNT(*) AS count_order FROM lineitem GROUP BY l_returnflag, \
                            l_linestatus"
                .into(),
            base_table_id: "tid-1".into(),
            base_table_version: 1,
            columns: vec![
                RollupColumn {
                    canonical_key: "col:l_returnflag".into(),
                    physical_name: "l_returnflag".into(),
                    is_group_by: true,
                },
                RollupColumn {
                    canonical_key: "col:l_linestatus".into(),
                    physical_name: "l_linestatus".into(),
                    is_group_by: true,
                },
                RollupColumn {
                    canonical_key: canonical_expr_key(&Expr::Aggregate {
                        func: AggregateFunction::Sum,
                        args: vec![Expr::column("l_quantity")],
                        distinct: false,
                    }),
                    physical_name: "sum_qty".into(),
                    is_group_by: false,
                },
                RollupColumn {
                    canonical_key: canonical_expr_key(&Expr::Aggregate {
                        func: AggregateFunction::Count,
                        args: vec![],
                        distinct: false,
                    }),
                    physical_name: "count_order".into(),
                    is_group_by: false,
                },
            ],
        }
    }

    fn rollup_candidate() -> RollupCandidate {
        RollupCandidate {
            registered_name: "lineitem_rollup".into(),
            meta: rollup_meta_for_worked_example(),
            schema: PlanSchema::new(vec![
                SchemaField::new("l_returnflag", DataType::Utf8),
                SchemaField::new("l_linestatus", DataType::Utf8),
                SchemaField::new("sum_qty", DataType::Float64),
                SchemaField::new("count_order", DataType::Int64),
            ]),
        }
    }

    // ---------- canonicalization ----------

    #[test]
    fn alias_never_affects_the_canonical_key() {
        let sum_a = Expr::Aggregate {
            func: AggregateFunction::Sum,
            args: vec![Expr::column("l_quantity")],
            distinct: false,
        }
        .alias("sum_qty");
        let sum_b = Expr::Aggregate {
            func: AggregateFunction::Sum,
            args: vec![Expr::column("l_quantity")],
            distinct: false,
        }
        .alias("total_quantity");
        assert_eq!(canonical_expr_key(&sum_a), canonical_expr_key(&sum_b));
    }

    #[test]
    fn table_qualification_is_stripped() {
        let bare = Expr::column("l_returnflag");
        let qualified = Expr::qualified_column("lineitem", "l_returnflag");
        let aliased_table = Expr::qualified_column("l", "l_returnflag");
        assert_eq!(canonical_expr_key(&bare), canonical_expr_key(&qualified));
        assert_eq!(
            canonical_expr_key(&bare),
            canonical_expr_key(&aliased_table)
        );
    }

    #[test]
    fn different_aggregate_functions_or_args_get_different_keys() {
        let sum_qty = Expr::Aggregate {
            func: AggregateFunction::Sum,
            args: vec![Expr::column("l_quantity")],
            distinct: false,
        };
        let sum_price = Expr::Aggregate {
            func: AggregateFunction::Sum,
            args: vec![Expr::column("l_extendedprice")],
            distinct: false,
        };
        let avg_qty = Expr::Aggregate {
            func: AggregateFunction::Avg,
            args: vec![Expr::column("l_quantity")],
            distinct: false,
        };
        let sum_distinct_qty = Expr::Aggregate {
            func: AggregateFunction::Sum,
            args: vec![Expr::column("l_quantity")],
            distinct: true,
        };
        let keys = [
            canonical_expr_key(&sum_qty),
            canonical_expr_key(&sum_price),
            canonical_expr_key(&avg_qty),
            canonical_expr_key(&sum_distinct_qty),
        ];
        for i in 0..keys.len() {
            for j in (i + 1)..keys.len() {
                assert_ne!(keys[i], keys[j], "keys[{i}] and keys[{j}] must differ");
            }
        }
    }

    // ---------- recognize() ----------

    #[test]
    fn recognizes_the_worked_example_and_is_order_independent_on_group_by() {
        let ab = recognize(&worked_example(["l_returnflag", "l_linestatus"])).unwrap();
        let ba = recognize(&worked_example(["l_linestatus", "l_returnflag"])).unwrap();
        assert_eq!(ab.table_name, "lineitem");
        assert_eq!(ab.sorted_group_by_keys(), ba.sorted_group_by_keys());
        assert_eq!(ab.sorted_aggregate_keys(), ba.sorted_aggregate_keys());
        // Source order DOES differ, confirming the test is exercising two
        // genuinely different orderings, not two identical plans.
        assert_ne!(ab.group_by_keys, ba.group_by_keys);
    }

    #[test]
    fn does_not_recognize_a_filter_between_scan_and_aggregate() {
        let with_filter = LogicalPlan::Filter(FilterNode {
            input: Arc::new(scan()),
            predicate: Expr::column("l_quantity").gt(Expr::literal(
                crate::planner::ScalarValue::Float64(10.0.into()),
            )),
        });
        let agg = LogicalPlan::Aggregate(AggregateNode {
            input: Arc::new(with_filter),
            group_by: vec![Expr::column("l_returnflag")],
            aggregates: vec![],
            schema: PlanSchema::new(vec![SchemaField::new("l_returnflag", DataType::Utf8)]),
        });
        let plan = LogicalPlan::Project(ProjectNode {
            input: Arc::new(agg),
            exprs: vec![Expr::column("l_returnflag")],
            schema: PlanSchema::new(vec![SchemaField::new("l_returnflag", DataType::Utf8)]),
        });
        assert!(recognize(&plan).is_none());
    }

    #[test]
    fn does_not_recognize_a_computed_projection_item() {
        let agg = LogicalPlan::Aggregate(AggregateNode {
            input: Arc::new(scan()),
            group_by: vec![Expr::column("l_returnflag")],
            aggregates: vec![],
            schema: PlanSchema::new(vec![SchemaField::new("l_returnflag", DataType::Utf8)]),
        });
        let plan = LogicalPlan::Project(ProjectNode {
            input: Arc::new(agg),
            exprs: vec![Expr::column("l_returnflag")
                .alias("x")
                .eq(Expr::column("l_returnflag"))],
            schema: PlanSchema::new(vec![SchemaField::new("x", DataType::Boolean)]),
        });
        assert!(recognize(&plan).is_none());
    }

    // ---------- require_rollup_defining_shape ----------

    #[test]
    fn require_rollup_defining_shape_accepts_the_worked_example() {
        let plan = worked_example(["l_returnflag", "l_linestatus"]);
        let recognized = require_rollup_defining_shape(&plan, "lineitem").unwrap();
        assert_eq!(recognized.group_by_keys.len(), 2);
        assert_eq!(recognized.aggregate_keys.len(), 2);
    }

    #[test]
    fn require_rollup_defining_shape_refuses_a_base_table_mismatch() {
        let plan = worked_example(["l_returnflag", "l_linestatus"]);
        let err = require_rollup_defining_shape(&plan, "orders").unwrap_err();
        assert!(err.to_string().contains("lineitem"));
    }

    #[test]
    fn require_rollup_defining_shape_refuses_a_non_matching_shape() {
        let with_filter = LogicalPlan::Filter(FilterNode {
            input: Arc::new(scan()),
            predicate: Expr::column("l_quantity").gt(Expr::literal(
                crate::planner::ScalarValue::Float64(10.0.into()),
            )),
        });
        let err = require_rollup_defining_shape(&with_filter, "lineitem").unwrap_err();
        assert!(err.to_string().contains("register_rollup"));
    }

    // ---------- build_rollup_columns ----------

    #[test]
    fn build_rollup_columns_zips_slots_with_the_written_schema() {
        let plan = worked_example(["l_returnflag", "l_linestatus"]);
        let recognized = require_rollup_defining_shape(&plan, "lineitem").unwrap();
        let write_schema = Schema::new(vec![
            arrow::datatypes::Field::new("l_returnflag", DataType::Utf8, true),
            arrow::datatypes::Field::new("l_linestatus", DataType::Utf8, true),
            arrow::datatypes::Field::new("sum_qty", DataType::Float64, true),
            arrow::datatypes::Field::new("count_order", DataType::Int64, false),
        ]);
        let columns = build_rollup_columns(&recognized, &write_schema).unwrap();
        assert_eq!(columns.len(), 4);
        assert_eq!(columns[0].physical_name, "l_returnflag");
        assert!(columns[0].is_group_by);
        assert_eq!(columns[2].physical_name, "sum_qty");
        assert!(!columns[2].is_group_by);
    }

    // ---------- substitute(): matching, order-independence, fallback ----------

    #[test]
    fn substitute_matches_and_reshapes_to_a_rollup_scan() {
        let plan = worked_example(["l_returnflag", "l_linestatus"]);
        let candidates = vec![rollup_candidate()];
        let mut matched = Vec::new();
        let out = substitute(&plan, &candidates, &mut matched).unwrap();
        assert_eq!(matched, vec!["lineitem_rollup".to_string()]);
        let LogicalPlan::Project(proj) = &out else {
            panic!("expected a substituted Project, got {out:?}");
        };
        let LogicalPlan::Scan(scan) = proj.input.as_ref() else {
            panic!("expected the substituted Project's input to be a Scan");
        };
        assert_eq!(scan.table_name, "lineitem_rollup");
        // Output shape must be BIT-FOR-BIT the original query's own shape.
        assert_eq!(
            proj.schema,
            worked_example(["l_returnflag", "l_linestatus"]).schema()
        );
    }

    #[test]
    fn substitute_matches_regardless_of_group_by_order_in_the_query() {
        let plan = worked_example(["l_linestatus", "l_returnflag"]); // reversed vs the rollup
        let candidates = vec![rollup_candidate()];
        let mut matched = Vec::new();
        let out = substitute(&plan, &candidates, &mut matched).unwrap();
        assert_eq!(matched, vec!["lineitem_rollup".to_string()]);
        assert!(matches!(out, LogicalPlan::Project(_)));
    }

    #[test]
    fn substitute_recurses_through_a_wrapping_sort() {
        use crate::planner::{SortDirection, SortExpr, SortNode};
        let plan = LogicalPlan::Sort(SortNode {
            input: Arc::new(worked_example(["l_returnflag", "l_linestatus"])),
            order_by: vec![SortExpr {
                expr: Expr::column("l_returnflag"),
                direction: SortDirection::Asc,
                nulls: crate::planner::NullOrdering::NullsLast,
            }],
        });
        let candidates = vec![rollup_candidate()];
        let mut matched = Vec::new();
        let out = substitute(&plan, &candidates, &mut matched).unwrap();
        assert_eq!(matched, vec!["lineitem_rollup".to_string()]);
        let LogicalPlan::Sort(sort) = &out else {
            panic!("Sort must survive substitution, wrapping the rewritten plan");
        };
        let LogicalPlan::Project(proj) = sort.input.as_ref() else {
            panic!("expected Sort's input to be the substituted Project");
        };
        assert!(matches!(proj.input.as_ref(), LogicalPlan::Scan(_)));
    }

    #[test]
    fn substitute_falls_back_on_a_different_group_by_set() {
        // Rollup is grouped by (l_returnflag, l_linestatus); query groups
        // by l_returnflag alone -- a real, different aggregate SET.
        let agg = LogicalPlan::Aggregate(AggregateNode {
            input: Arc::new(scan()),
            group_by: vec![Expr::column("l_returnflag")],
            aggregates: vec![Expr::Aggregate {
                func: AggregateFunction::Sum,
                args: vec![Expr::column("l_quantity")],
                distinct: false,
            }],
            schema: PlanSchema::new(vec![
                SchemaField::new("l_returnflag", DataType::Utf8),
                SchemaField::new("sum_qty", DataType::Float64),
            ]),
        });
        let plan = LogicalPlan::Project(ProjectNode {
            input: Arc::new(agg),
            exprs: vec![Expr::column("l_returnflag"), Expr::column("sum_qty")],
            schema: PlanSchema::new(vec![
                SchemaField::new("l_returnflag", DataType::Utf8),
                SchemaField::new("sum_qty", DataType::Float64),
            ]),
        });
        let candidates = vec![rollup_candidate()];
        let mut matched = Vec::new();
        let out = substitute(&plan, &candidates, &mut matched).unwrap();
        assert!(
            matched.is_empty(),
            "no rollup should have answered this query"
        );
        assert_eq!(
            out, plan,
            "plan must be returned completely unchanged on a miss"
        );
    }

    #[test]
    fn substitute_falls_back_on_an_added_filter() {
        let with_filter = LogicalPlan::Filter(FilterNode {
            input: Arc::new(scan()),
            predicate: Expr::column("l_quantity").gt(Expr::literal(
                crate::planner::ScalarValue::Float64(10.0.into()),
            )),
        });
        let agg = LogicalPlan::Aggregate(AggregateNode {
            input: Arc::new(with_filter),
            group_by: vec![Expr::column("l_returnflag"), Expr::column("l_linestatus")],
            aggregates: vec![
                Expr::Aggregate {
                    func: AggregateFunction::Sum,
                    args: vec![Expr::column("l_quantity")],
                    distinct: false,
                },
                Expr::Aggregate {
                    func: AggregateFunction::Count,
                    args: vec![],
                    distinct: false,
                },
            ],
            schema: PlanSchema::new(vec![
                SchemaField::new("l_returnflag", DataType::Utf8),
                SchemaField::new("l_linestatus", DataType::Utf8),
                SchemaField::new("sum_qty", DataType::Float64),
                SchemaField::new("count_order", DataType::Int64),
            ]),
        });
        let plan = LogicalPlan::Project(ProjectNode {
            input: Arc::new(agg),
            exprs: vec![
                Expr::column("l_returnflag"),
                Expr::column("l_linestatus"),
                Expr::column("sum_qty"),
                Expr::column("count_order"),
            ],
            schema: PlanSchema::new(vec![
                SchemaField::new("l_returnflag", DataType::Utf8),
                SchemaField::new("l_linestatus", DataType::Utf8),
                SchemaField::new("sum_qty", DataType::Float64),
                SchemaField::new("count_order", DataType::Int64),
            ]),
        });
        let candidates = vec![rollup_candidate()];
        let mut matched = Vec::new();
        let out = substitute(&plan, &candidates, &mut matched).unwrap();
        assert!(
            matched.is_empty(),
            "a query with an added filter must never match"
        );
        assert_eq!(out, plan);
    }

    #[test]
    fn substitute_falls_back_on_a_different_aggregate() {
        let agg = LogicalPlan::Aggregate(AggregateNode {
            input: Arc::new(scan()),
            group_by: vec![Expr::column("l_returnflag"), Expr::column("l_linestatus")],
            aggregates: vec![Expr::Aggregate {
                func: AggregateFunction::Avg, // AVG, not SUM/COUNT -- a different aggregate SET
                args: vec![Expr::column("l_quantity")],
                distinct: false,
            }],
            schema: PlanSchema::new(vec![
                SchemaField::new("l_returnflag", DataType::Utf8),
                SchemaField::new("l_linestatus", DataType::Utf8),
                SchemaField::new("avg_qty", DataType::Float64),
            ]),
        });
        let plan = LogicalPlan::Project(ProjectNode {
            input: Arc::new(agg),
            exprs: vec![
                Expr::column("l_returnflag"),
                Expr::column("l_linestatus"),
                Expr::column("avg_qty"),
            ],
            schema: PlanSchema::new(vec![
                SchemaField::new("l_returnflag", DataType::Utf8),
                SchemaField::new("l_linestatus", DataType::Utf8),
                SchemaField::new("avg_qty", DataType::Float64),
            ]),
        });
        let candidates = vec![rollup_candidate()];
        let mut matched = Vec::new();
        let out = substitute(&plan, &candidates, &mut matched).unwrap();
        assert!(matched.is_empty(), "a different aggregate must never match");
        assert_eq!(out, plan);
    }

    #[test]
    fn substitute_falls_back_on_a_different_base_table() {
        let other_scan = LogicalPlan::Scan(ScanNode {
            table_name: "orders".into(),
            schema: PlanSchema::new(vec![SchemaField::new("o_orderstatus", DataType::Utf8)]),
            projection: None,
            filter: None,
        });
        let agg = LogicalPlan::Aggregate(AggregateNode {
            input: Arc::new(other_scan),
            group_by: vec![Expr::column("o_orderstatus")],
            aggregates: vec![],
            schema: PlanSchema::new(vec![SchemaField::new("o_orderstatus", DataType::Utf8)]),
        });
        let plan = LogicalPlan::Project(ProjectNode {
            input: Arc::new(agg),
            exprs: vec![Expr::column("o_orderstatus")],
            schema: PlanSchema::new(vec![SchemaField::new("o_orderstatus", DataType::Utf8)]),
        });
        let candidates = vec![rollup_candidate()];
        let mut matched = Vec::new();
        let out = substitute(&plan, &candidates, &mut matched).unwrap();
        assert!(matched.is_empty());
        assert_eq!(out, plan);
    }

    #[test]
    fn substitute_is_a_no_op_with_zero_candidates() {
        let plan = worked_example(["l_returnflag", "l_linestatus"]);
        let mut matched = Vec::new();
        let out = substitute(&plan, &[], &mut matched).unwrap();
        assert!(matched.is_empty());
        assert_eq!(out, plan);
    }

    #[test]
    fn substitute_is_untouched_by_an_ordinary_non_aggregate_query() {
        let plan = LogicalPlan::Project(ProjectNode {
            input: Arc::new(scan()),
            exprs: vec![Expr::column("l_returnflag")],
            schema: PlanSchema::new(vec![SchemaField::new("l_returnflag", DataType::Utf8)]),
        });
        let candidates = vec![rollup_candidate()];
        let mut matched = Vec::new();
        let out = substitute(&plan, &candidates, &mut matched).unwrap();
        assert!(matched.is_empty());
        assert_eq!(out, plan);
    }
}
