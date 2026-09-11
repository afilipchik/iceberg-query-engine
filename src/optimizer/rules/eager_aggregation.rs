//! Eager aggregation (Yan & Larson): pre-aggregate a fanout join input.
//!
//! Pattern: `Aggregate(SUM...) over Inner Join(R, S)` where relation R's
//! columns appear only in the join keys and as direct floating SUM inputs
//! arguments. R is replaced by `Aggregate(R, group_by = join keys,
//! SUM(factor)..., COUNT(*))`, the join becomes key-unique (1:1 per probe
//! key group), and the outer SUM terms are rewritten:
//!
//!   SUM(r)  ->  SUM(SUM_r)
//!
//! where `SUM_r` comes from the pre-aggregate. Scalar multiplication is not
//! distributed across SUM: even non-null finite inputs can overflow a partial
//! sum before multiplication, changing zero into NaN for a zero multiplier.
//!
//! When R joins on two integer key columns, the pre-aggregate groups by a
//! single packed expression `k0 * K + k1` (K a power of two derived from
//! structural type/predicate proofs) so it stays on the fast single-int raw aggregation
//! path, and the join condition is rewritten to compare packed keys.
//!
//! Correctness gates (plan shape, schema nullability and structural domains):
//! - all output aggregates are non-DISTINCT floating SUMs of a direct R column,
//! - R-side factors are direct floating columns; exact arithmetic is not reassociated,
//! - group_by and join filter reference no R columns,
//! - each SUM term has at most one factor over R columns, that factor's
//!   columns are null-free (an R-side NULL would drop the entire term row
//!   in the original but not in the rewrite),
//! - packed keys require both key columns non-negative with known maxima.

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::physical::operators::TableStatistics;
use crate::planner::{
    AggregateFunction, AggregateNode, BinaryOp, Expr, JoinNode, JoinType, LogicalPlan, PlanSchema,
    ScanNode, SchemaField,
};
use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::sync::Arc;

pub struct EagerAggregation {
    table_stats: HashMap<String, TableStatistics>,
}

impl EagerAggregation {
    pub fn new() -> Self {
        Self {
            table_stats: HashMap::new(),
        }
    }

    pub fn with_table_statistics(table_stats: HashMap<String, TableStatistics>) -> Self {
        Self { table_stats }
    }
}

impl Default for EagerAggregation {
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerRule for EagerAggregation {
    fn name(&self) -> &str {
        "EagerAggregation"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        self.rewrite(plan)
    }
}

/// One additive term of a SUM argument: `sign * f0 * f1 * ...`
struct SumTerm {
    negated: bool,
    factors: Vec<Expr>,
}

impl EagerAggregation {
    fn rewrite(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        // Recurse into children first
        let children: Vec<Arc<LogicalPlan>> = plan
            .children()
            .iter()
            .map(|c| self.rewrite(c).map(Arc::new))
            .collect::<Result<Vec<_>>>()?;
        let plan = plan.with_new_children(children);

        if let LogicalPlan::Aggregate(agg) = &plan {
            if let LogicalPlan::Join(join) = &*agg.input {
                if let Some(rewritten) = self.try_rewrite_agg_join(agg, join) {
                    return Ok(rewritten);
                }
            }
        }
        Ok(plan)
    }

    fn try_rewrite_agg_join(&self, agg: &AggregateNode, join: &JoinNode) -> Option<LogicalPlan> {
        if join.join_type == JoinType::Left && join.filter.is_none() {
            if let Some(p) = self.try_rewrite_left_count(agg, join) {
                return Some(p);
            }
        }
        if join.join_type != JoinType::Inner || join.filter.is_some() {
            return None;
        }
        // Try each side as the pre-aggregation candidate R
        for r_is_left in [true, false] {
            if let Some(p) = self.try_rewrite_side(agg, join, r_is_left) {
                return Some(p);
            }
        }
        None
    }

    /// LEFT-join count pushdown: `Aggregate(group=[k], COUNT(r_col)) over
    /// LEFT Join(L, R) on k = fk` with k a structurally unique grouping key of L becomes
    ///
    ///   Project [k, COALESCE(cnt, 0)]
    ///     LEFT Join on k = fk
    ///       L
    ///       Aggregate(R, group=[fk], COUNT(r_col) AS cnt)
    ///
    /// With unknown left uniqueness, retain a final SUM of each left row's
    /// count. Statistics may select this route, but cannot erase duplicates.
    fn try_rewrite_left_count(&self, agg: &AggregateNode, join: &JoinNode) -> Option<LogicalPlan> {
        if agg.group_by.len() != 1 || agg.aggregates.len() != 1 || join.on.len() != 1 {
            return None;
        }
        let Expr::Column(k) = &agg.group_by[0] else {
            return None;
        };
        let (l_on, r_on) = &join.on[0];
        let (Expr::Column(l_col), Expr::Column(r_col)) = (l_on, r_on) else {
            return None;
        };
        // Group key must be the left join key and proven unique in the
        // exact left subtree, not merely estimated unique in a base table.
        if crate::optimizer::properties::column_index(&join.left.schema(), l_col)
            != crate::optimizer::properties::column_index(&join.left.schema(), k)
        {
            return None;
        }
        let l_schema = join.left.schema();
        let r_schema = join.right.schema();
        if !column_in_schema(l_col, &l_schema) || !column_in_schema(r_col, &r_schema) {
            return None;
        }
        // A base-table NDV estimate is not a uniqueness proof. Derive the
        // property from this exact left subtree (including projections).
        let unique_left = crate::optimizer::properties::proves_unique_column(&join.left, k);
        if !unique_left {
            // Costing may select preaggregation, but cannot remove the final
            // aggregate: every duplicate left row must contribute its count.
            let index = crate::optimizer::properties::column_index(&r_schema, r_col)?;
            let (rows, groups) = self.left_count_key_estimate(&join.right, index)?;
            if groups.saturating_mul(10) > rows.saturating_mul(7) || rows == 0 {
                return None;
            }
        }
        // The single aggregate must be COUNT over an R-side column
        let (out_alias, count_arg) = match strip_alias(&agg.aggregates[0]) {
            Expr::Aggregate {
                func: AggregateFunction::Count,
                args,
                distinct: false,
            } => match args.first() {
                Some(Expr::Column(c)) if column_in_schema(c, &r_schema) => {
                    let alias = match &agg.aggregates[0] {
                        Expr::Alias { name, .. } => Some(name.clone()),
                        _ => None,
                    };
                    (alias, c.clone())
                }
                _ => return None,
            },
            _ => return None,
        };

        // Internal output names must not capture either input's bound columns.
        // Checking names regardless of relation also keeps unqualified lookup unique.
        let mut count_name = "__ea_cnt".to_string();
        while l_schema
            .fields()
            .iter()
            .chain(r_schema.fields())
            .chain(agg.schema.fields())
            .any(|field| field.name == count_name)
        {
            count_name.push('_');
        }

        // Pre-aggregate R by its join key
        let pre_fields = vec![
            r_on.to_field(&r_schema).ok()?,
            SchemaField {
                name: count_name.clone(),
                data_type: DataType::Int64,
                nullable: true,
                relation: None,
            },
        ];
        let pre_agg = LogicalPlan::Aggregate(AggregateNode {
            input: join.right.clone(),
            group_by: vec![r_on.clone()],
            aggregates: vec![Expr::Alias {
                expr: Box::new(Expr::Aggregate {
                    func: AggregateFunction::Count,
                    args: vec![Expr::Column(count_arg)],
                    distinct: false,
                }),
                name: count_name.clone(),
            }],
            schema: PlanSchema::new(pre_fields),
        });

        let pre_schema = pre_agg.schema().clone();
        let new_join_schema = join.left.schema().merge(&pre_schema);
        let new_join = LogicalPlan::Join(JoinNode {
            left: join.left.clone(),
            right: Arc::new(pre_agg),
            join_type: JoinType::Left,
            on: join.on.clone(),
            filter: None,
            schema: new_join_schema,
        });

        // Restore the original aggregate output schema
        let count_expr = Expr::ScalarFunc {
            func: crate::planner::ScalarFunction::Coalesce,
            args: vec![
                Expr::Column(crate::planner::Column {
                    relation: None,
                    name: count_name.clone(),
                }),
                Expr::Literal(crate::planner::ScalarValue::Int64(0)),
            ],
        };
        if !unique_left {
            return Some(LogicalPlan::Aggregate(AggregateNode {
                input: Arc::new(new_join),
                group_by: agg.group_by.clone(),
                aggregates: vec![Expr::Alias {
                    expr: Box::new(Expr::Aggregate {
                        func: AggregateFunction::Sum,
                        args: vec![count_expr],
                        distinct: false,
                    }),
                    name: agg.schema.fields()[1].name.clone(),
                }],
                schema: agg.schema.clone(),
            }));
        }
        let count_out = match out_alias {
            Some(name) => Expr::Alias {
                expr: Box::new(count_expr),
                name,
            },
            None => Expr::Alias {
                expr: Box::new(count_expr),
                name: agg.schema.fields()[1].name.clone(),
            },
        };
        Some(LogicalPlan::Project(crate::planner::ProjectNode {
            input: Arc::new(new_join),
            exprs: vec![agg.group_by[0].clone(), count_out],
            schema: agg.schema.clone(),
        }))
    }

    // Follow only identity-preserving column lineage for a costing estimate.
    // Filters can change the estimate's accuracy, never the rewrite's semantics.
    fn left_count_key_estimate(&self, plan: &LogicalPlan, index: usize) -> Option<(u64, u64)> {
        match plan {
            LogicalPlan::Scan(scan) => {
                let stats = self.table_stats.get(&scan.table_name)?;
                let field = scan.schema.fields().get(index)?;
                let groups = stats.column_stats.get(&field.name)?.ndv_est?;
                Some((stats.row_count as u64, groups))
            }
            LogicalPlan::Filter(node) => self.left_count_key_estimate(&node.input, index),
            LogicalPlan::SubqueryAlias(node) => self.left_count_key_estimate(&node.input, index),
            LogicalPlan::Project(node) => {
                let Expr::Column(column) = strip_alias(node.exprs.get(index)?) else {
                    return None;
                };
                let child_index =
                    crate::optimizer::properties::column_index(&node.input.schema(), column)?;
                self.left_count_key_estimate(&node.input, child_index)
            }
            _ => None,
        }
    }

    fn try_rewrite_side(
        &self,
        agg: &AggregateNode,
        join: &JoinNode,
        r_is_left: bool,
    ) -> Option<LogicalPlan> {
        let (r_plan, s_plan) = if r_is_left {
            (&join.left, &join.right)
        } else {
            (&join.right, &join.left)
        };

        // R must be a bare Scan (filters already pushed into ScanNode.filter)
        let r_scan = match &**r_plan {
            LogicalPlan::Scan(s) => s,
            _ => return None,
        };
        let r_schema = r_scan.schema.clone();

        // Join keys on the R side must be plain R columns (1 or 2 of them)
        let mut r_keys: Vec<Expr> = Vec::new();
        for (l, r) in &join.on {
            let r_side = if r_is_left { l } else { r };
            match r_side {
                Expr::Column(c) if column_in_schema(c, &r_schema) => r_keys.push(r_side.clone()),
                _ => return None,
            }
        }
        if r_keys.is_empty() || r_keys.len() > 2 {
            return None;
        }

        // group_by must not touch R
        if agg
            .group_by
            .iter()
            .any(|e| expr_references_schema(e, &r_schema))
        {
            return None;
        }

        // Decompose every aggregate: all must be plain SUMs
        let mut all_terms: Vec<Vec<SumTerm>> = Vec::new();
        for a in &agg.aggregates {
            let (func, args, distinct) = match strip_alias(a) {
                Expr::Aggregate {
                    func,
                    args,
                    distinct,
                } => (func, args, distinct),
                _ => return None,
            };
            if *func != AggregateFunction::Sum || *distinct || args.len() != 1 {
                return None;
            }
            // Reassociation of exact arithmetic changes intermediate overflow
            // and decimal scale/rounding behavior. This rule currently builds
            // float count factors and cannot preserve those contracts.
            if !matches!(
                args[0].data_type(&join.schema).ok()?,
                DataType::Float32 | DataType::Float64
            ) {
                return None;
            }
            let mut terms = Vec::new();
            flatten_terms(&args[0], false, &mut terms);
            // SUM(a*r + b) cannot become SUM(a*SUM(r) + b*COUNT): a NULL
            // in either additive term originally discards the entire row.
            if terms.len() != 1 {
                return None;
            }
            all_terms.push(terms);
        }

        // Classify factors; collect distinct R-factor expressions
        let mut r_factors: Vec<Expr> = Vec::new();
        for terms in &all_terms {
            for term in terms {
                // Preserve scalar evaluation before aggregation. For example,
                // SUM(0 * x) is zero for finite x, but 0 * SUM(x) can be NaN
                // when the partial sum overflows. Null counts and ranges used
                // for costing do not prove this reassociation safe.
                if term.negated || term.factors.len() != 1 {
                    return None;
                }
                let mut r_count = 0;
                for f in &term.factors {
                    let refs_r = expr_references_schema(f, &r_schema);
                    if refs_r {
                        // Factor must be entirely over R columns
                        if !expr_only_references_schema(f, &r_schema) {
                            return None;
                        }
                        // Only a direct floating column has a supported
                        // partial SUM type and no hidden per-row arithmetic
                        // overflow, cast failure or NULL-producing operation.
                        if !matches!(f, Expr::Column(_))
                            || !matches!(
                                f.data_type(&r_schema).ok()?,
                                DataType::Float32 | DataType::Float64
                            )
                        {
                            return None;
                        }
                        r_count += 1;
                        if !r_factors.contains(f) {
                            r_factors.push(f.clone());
                        }
                    }
                }
                if r_count != 1 {
                    return None;
                }
            }
        }

        // Statistics are costing hints, not semantic nullability proofs.
        // Require the actual input schema's non-null contract instead.
        for f in &r_factors {
            let mut cols = Vec::new();
            collect_columns(f, &mut cols);
            for c in cols {
                let index = crate::optimizer::properties::column_index(&r_schema, &c)?;
                if r_schema.fields()[index].nullable {
                    return None;
                }
            }
        }

        // Only worthwhile when R actually duplicates its key combination:
        // estimate the distinct key count as max over per-column NDVs (full
        // correlation, consistent with the join cost model) and require real
        // fanout. Without duplication the rewrite only adds an aggregation.
        {
            let stats = self.table_stats.get(&r_scan.table_name)?;
            let mut ndv_max = 0u64;
            for kexpr in &r_keys {
                let cs = self.column_stats_for(&r_scan.table_name, kexpr)?;
                ndv_max = ndv_max.max(cs.ndv_est?);
            }
            let rows = stats.row_count as u64;
            if r_scan.filter.is_some() || ndv_max.saturating_mul(10) > rows.saturating_mul(7) {
                return None;
            }
            // The pre-aggregate reads ALL of R before the join can filter
            // it, and footer statistics cannot see the S side's selectivity
            // (Q18's semi-join keeps 4% of lineitem — pre-aggregating all
            // 60M rows there costs more than the fanout it avoids). Bound
            // the up-front cost instead: R must be moderately sized.
            const MAX_PREAGG_ROWS: u64 = 16_000_000;
            const MAX_PREAGG_GROUPS: u64 = 4_000_000;
            if rows > MAX_PREAGG_ROWS || ndv_max > MAX_PREAGG_GROUPS {
                return None;
            }
        }

        // Build the pre-aggregate group key (packing dual int keys)
        let (r_group_expr, s_key_expr) = self.build_keys(&r_keys, join, r_is_left, r_scan)?;

        // Pre-aggregate node over R
        let key_field = SchemaField {
            name: "__ea_key".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            relation: None,
        };
        let mut pre_fields = vec![key_field];
        let mut pre_aggs: Vec<Expr> = Vec::new();
        for (i, f) in r_factors.iter().enumerate() {
            let name = format!("__ea_sum_{}", i);
            let dt = f.data_type(&r_schema).ok()?;
            pre_aggs.push(Expr::Alias {
                expr: Box::new(Expr::Aggregate {
                    func: AggregateFunction::Sum,
                    args: vec![f.clone()],
                    distinct: false,
                }),
                name: name.clone(),
            });
            pre_fields.push(SchemaField {
                name,
                data_type: sum_output_type(&dt),
                nullable: true,
                relation: None,
            });
        }
        pre_aggs.push(Expr::Alias {
            expr: Box::new(Expr::Aggregate {
                func: AggregateFunction::Count,
                args: vec![Expr::Wildcard],
                distinct: false,
            }),
            name: "__ea_cnt".to_string(),
        });
        pre_fields.push(SchemaField {
            name: "__ea_cnt".to_string(),
            data_type: DataType::Int64,
            nullable: true,
            relation: None,
        });
        let pre_schema = PlanSchema::new(pre_fields);
        let pre_agg = LogicalPlan::Aggregate(AggregateNode {
            input: Arc::new(LogicalPlan::Scan(r_scan.clone())),
            group_by: vec![Expr::Alias {
                expr: Box::new(r_group_expr),
                name: "__ea_key".to_string(),
            }],
            aggregates: pre_aggs,
            schema: pre_schema.clone(),
        });

        // New join: pre-agg side keeps R's original position
        let ea_key_col = Expr::Column(crate::planner::Column {
            relation: None,
            name: "__ea_key".to_string(),
        });
        let new_on = if r_is_left {
            vec![(ea_key_col, s_key_expr)]
        } else {
            vec![(s_key_expr, ea_key_col)]
        };
        let (new_left, new_right) = if r_is_left {
            (Arc::new(pre_agg), s_plan.clone())
        } else {
            (s_plan.clone(), Arc::new(pre_agg))
        };
        let s_schema = s_plan.schema().clone();
        let new_join_schema = if r_is_left {
            pre_schema.merge(&s_schema)
        } else {
            s_schema.merge(&pre_schema)
        };
        let new_join = LogicalPlan::Join(JoinNode {
            left: new_left,
            right: new_right,
            join_type: JoinType::Inner,
            on: new_on,
            filter: None,
            schema: new_join_schema,
        });

        // Rewrite outer aggregates term by term
        let mut new_aggregates = Vec::with_capacity(agg.aggregates.len());
        for (a, terms) in agg.aggregates.iter().zip(all_terms.iter()) {
            let new_arg = rebuild_sum_arg(terms, &r_factors)?;
            let new_sum = Expr::Aggregate {
                func: AggregateFunction::Sum,
                args: vec![new_arg],
                distinct: false,
            };
            new_aggregates.push(match a {
                Expr::Alias { name, .. } => Expr::Alias {
                    expr: Box::new(new_sum),
                    name: name.clone(),
                },
                _ => new_sum,
            });
        }

        Some(LogicalPlan::Aggregate(AggregateNode {
            input: Arc::new(new_join),
            group_by: agg.group_by.clone(),
            aggregates: new_aggregates,
            schema: agg.schema.clone(),
        }))
    }

    /// Column statistics for a plain-column expression of a known table.
    fn column_stats_for(
        &self,
        table: &str,
        e: &Expr,
    ) -> Option<&crate::physical::operators::ColumnStatistics> {
        let name = match e {
            Expr::Column(c) => c.name.to_lowercase(),
            _ => return None,
        };
        self.table_stats.get(table)?.column_stats.get(&name)
    }

    /// Build the R-side group expression and the matching S-side key
    /// expression. Single key: identity. Dual int keys: packed `k0*K + k1`.
    fn build_keys(
        &self,
        r_keys: &[Expr],
        join: &JoinNode,
        r_is_left: bool,
        r_scan: &ScanNode,
    ) -> Option<(Expr, Expr)> {
        // S-side expressions paired with each R key
        let s_keys: Vec<Expr> = join
            .on
            .iter()
            .map(|(l, r)| if r_is_left { r.clone() } else { l.clone() })
            .collect();

        let int_like = |e: &Expr, schema: &PlanSchema| -> bool {
            matches!(
                e.data_type(schema),
                Ok(DataType::Int64) | Ok(DataType::Int32)
            )
        };

        if r_keys.len() == 1 {
            let s_schema = if r_is_left {
                join.right.schema()
            } else {
                join.left.schema()
            };
            if !int_like(&r_keys[0], &r_scan.schema) || !int_like(&s_keys[0], &s_schema) {
                return None;
            }
            // The pre-aggregate key schema is Int64. Widen Int32 keys
            // explicitly rather than emitting an Int32 array under that schema.
            let widen = |expr: &Expr, schema: &PlanSchema| -> Option<Expr> {
                Some(if expr.data_type(schema).ok()? == DataType::Int32 {
                    cast_i64(expr.clone())
                } else {
                    expr.clone()
                })
            };
            return Some((
                widen(&r_keys[0], &r_scan.schema)?,
                widen(&s_keys[0], &s_schema)?,
            ));
        }

        // Derive both domains from their exact input subtrees. A name-matched
        // footer/sample range cannot prove that a projected or joined key
        // remains in that range. Share the ordinary packed-join proof.
        use super::packed_group_keys::{integer_domain, packing_radix, IntegerDomain};
        let (r_plan, s_plan) = if r_is_left {
            (&join.left, &join.right)
        } else {
            (&join.right, &join.left)
        };
        let mut domains = Vec::new();
        for (i, k) in r_keys.iter().enumerate() {
            let (Expr::Column(r_col), Expr::Column(s_col)) = (k, &s_keys[i]) else {
                return None;
            };
            let r = integer_domain(r_plan, r_col)?;
            let s = integer_domain(s_plan, s_col)?;
            domains.push(IntegerDomain {
                min: r.min.min(s.min),
                max: r.max.max(s.max),
                nullable: r.nullable || s.nullable,
            });
        }
        let k = packing_radix(domains[0], domains[1])?;
        let pack = |a: &Expr, b: &Expr| -> Expr {
            Expr::BinaryExpr {
                left: Box::new(Expr::BinaryExpr {
                    left: Box::new(cast_i64(a.clone())),
                    op: BinaryOp::Multiply,
                    right: Box::new(Expr::Literal(crate::planner::ScalarValue::Int64(k))),
                }),
                op: BinaryOp::Add,
                right: Box::new(cast_i64(b.clone())),
            }
        };
        Some((pack(&r_keys[0], &r_keys[1]), pack(&s_keys[0], &s_keys[1])))
    }
}

fn cast_i64(e: Expr) -> Expr {
    Expr::Cast {
        expr: Box::new(e),
        data_type: DataType::Int64,
        mode: crate::planner::CastMode::Strict,
    }
}

fn sum_output_type(input: &DataType) -> DataType {
    match input {
        DataType::Float64 | DataType::Float32 => DataType::Float64,
        _ => DataType::Int64,
    }
}

fn strip_alias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias { expr, .. } => strip_alias(expr),
        _ => e,
    }
}

/// Flatten `a + b - c` into signed terms.
fn flatten_terms(e: &Expr, negated: bool, out: &mut Vec<SumTerm>) {
    match e {
        Expr::BinaryExpr { left, op, right } if *op == BinaryOp::Add => {
            flatten_terms(left, negated, out);
            flatten_terms(right, negated, out);
        }
        Expr::BinaryExpr { left, op, right } if *op == BinaryOp::Subtract => {
            flatten_terms(left, negated, out);
            flatten_terms(right, !negated, out);
        }
        _ => {
            let mut factors = Vec::new();
            flatten_factors(e, &mut factors);
            out.push(SumTerm { negated, factors });
        }
    }
}

/// Flatten `a * b * c` into factors.
fn flatten_factors(e: &Expr, out: &mut Vec<Expr>) {
    match e {
        Expr::BinaryExpr { left, op, right } if *op == BinaryOp::Multiply => {
            flatten_factors(left, out);
            flatten_factors(right, out);
        }
        _ => out.push(e.clone()),
    }
}

/// Rebuild a SUM argument from rewritten terms: R factors become their
/// pre-aggregated sum columns; R-free terms get multiplied by `__ea_cnt`.
fn rebuild_sum_arg(terms: &[SumTerm], r_factors: &[Expr]) -> Option<Expr> {
    let cnt_col = Expr::Column(crate::planner::Column {
        relation: None,
        name: "__ea_cnt".to_string(),
    });
    let mut result: Option<Expr> = None;
    for term in terms {
        let mut new_factors: Vec<Expr> = Vec::new();
        let mut had_r = false;
        for f in &term.factors {
            match r_factors.iter().position(|rf| rf == f) {
                Some(i) => {
                    had_r = true;
                    new_factors.push(Expr::Column(crate::planner::Column {
                        relation: None,
                        name: format!("__ea_sum_{}", i),
                    }));
                }
                None => new_factors.push(f.clone()),
            }
        }
        if !had_r {
            new_factors.push(cast_f64_if_needed(cnt_col.clone(), &term.factors));
        }
        let mut term_expr = new_factors.pop()?;
        while let Some(f) = new_factors.pop() {
            term_expr = Expr::BinaryExpr {
                left: Box::new(f),
                op: BinaryOp::Multiply,
                right: Box::new(term_expr),
            };
        }
        result = Some(match result {
            None => {
                if term.negated {
                    Expr::BinaryExpr {
                        left: Box::new(Expr::Literal(crate::planner::ScalarValue::Int64(0))),
                        op: BinaryOp::Subtract,
                        right: Box::new(term_expr),
                    }
                } else {
                    term_expr
                }
            }
            Some(prev) => Expr::BinaryExpr {
                left: Box::new(prev),
                op: if term.negated {
                    BinaryOp::Subtract
                } else {
                    BinaryOp::Add
                },
                right: Box::new(term_expr),
            },
        });
    }
    result
}

/// Multiply-by-count must not change the term's float typing.
fn cast_f64_if_needed(cnt: Expr, _factors: &[Expr]) -> Expr {
    Expr::Cast {
        expr: Box::new(cnt),
        data_type: DataType::Float64,
        mode: crate::planner::CastMode::Strict,
    }
}

fn column_in_schema(c: &crate::planner::Column, schema: &PlanSchema) -> bool {
    match &c.relation {
        Some(rel) => schema.index_of_qualified(Some(rel), &c.name).is_some(),
        None => schema.index_of(&c.name).is_some(),
    }
}

pub(crate) fn collect_columns(e: &Expr, out: &mut Vec<crate::planner::Column>) {
    match e {
        Expr::Column(c) => out.push(c.clone()),
        _ => {
            for child in expr_children(e) {
                collect_columns(child, out);
            }
        }
    }
}

fn expr_references_schema(e: &Expr, schema: &PlanSchema) -> bool {
    let mut cols = Vec::new();
    collect_columns(e, &mut cols);
    cols.iter().any(|c| column_in_schema(c, schema))
}

fn expr_only_references_schema(e: &Expr, schema: &PlanSchema) -> bool {
    let mut cols = Vec::new();
    collect_columns(e, &mut cols);
    !cols.is_empty() && cols.iter().all(|c| column_in_schema(c, schema))
}

pub(crate) fn expr_children(e: &Expr) -> Vec<&Expr> {
    match e {
        Expr::BinaryExpr { left, right, .. } => vec![left, right],
        Expr::UnaryExpr { expr, .. } => vec![expr],
        Expr::Cast { expr, .. } => vec![expr],
        Expr::Alias { expr, .. } => vec![expr],
        Expr::Aggregate { args, .. } => args.iter().collect(),
        Expr::ScalarFunc { args, .. } => args.iter().collect(),
        Expr::Case {
            operand,
            when_then,
            else_expr,
        } => {
            let mut v: Vec<&Expr> = Vec::new();
            if let Some(o) = operand {
                v.push(o);
            }
            for (w, t) in when_then {
                v.push(w);
                v.push(t);
            }
            if let Some(el) = else_expr {
                v.push(el);
            }
            v
        }
        Expr::InList { expr, list, .. } => {
            let mut v = vec![&**expr];
            v.extend(list.iter());
            v
        }
        Expr::Between {
            expr, low, high, ..
        } => vec![expr, low, high],
        _ => vec![],
    }
}

#[cfg(test)]
mod semantic_domain_tests {
    use super::*;
    use crate::physical::operators::ColumnStatistics;
    use crate::planner::{Column, ScalarValue};

    fn col(name: &str) -> Expr {
        Expr::Column(Column::new(name))
    }
    fn scan(table: &str, key_type: DataType, factor_nullable: bool) -> Arc<LogicalPlan> {
        Arc::new(LogicalPlan::Scan(ScanNode {
            table_name: table.into(),
            schema: PlanSchema::new(vec![
                SchemaField::new(format!("{table}0"), key_type.clone()),
                SchemaField::new(format!("{table}1"), key_type),
                SchemaField::new(format!("{table}v"), DataType::Float64)
                    .with_nullable(factor_nullable),
            ]),
            projection: None,
            filter: None,
        }))
    }
    fn join(left: Arc<LogicalPlan>, right: Arc<LogicalPlan>, two_keys: bool) -> JoinNode {
        JoinNode {
            schema: left.schema().merge(&right.schema()),
            left,
            right,
            join_type: JoinType::Inner,
            on: if two_keys {
                vec![(col("r0"), col("s0")), (col("r1"), col("s1"))]
            } else {
                vec![(col("r0"), col("s0"))]
            },
            filter: None,
        }
    }
    fn estimated_bounds() -> HashMap<String, TableStatistics> {
        ["r", "s"]
            .into_iter()
            .map(|table| {
                (
                    table.into(),
                    TableStatistics {
                        row_count: 1000,
                        total_byte_size: 24000,
                        column_stats: ["0", "1", "v"]
                            .into_iter()
                            .map(|suffix| {
                                (
                                    format!("{table}{suffix}"),
                                    ColumnStatistics {
                                        min_i64: Some(0),
                                        max_i64: Some(1),
                                        null_count: Some(0),
                                        ndv_est: Some(2),
                                        ..Default::default()
                                    },
                                )
                            })
                            .collect(),
                    },
                )
            })
            .collect()
    }
    #[test]
    fn estimated_key_ranges_never_establish_packing_or_alias_lineage() {
        let rule = EagerAggregation::with_table_statistics(estimated_bounds());
        let node = join(
            scan("r", DataType::Int64, false),
            scan("s", DataType::Int64, false),
            true,
        );
        let LogicalPlan::Scan(r_scan) = node.left.as_ref() else {
            unreachable!()
        };
        assert!(rule
            .build_keys(&[col("r0"), col("r1")], &node, true, r_scan)
            .is_none());

        let right = scan("s", DataType::UInt16, false);
        let projected = right
            .as_ref()
            .clone()
            .project(vec![
                col("s0"),
                Expr::Alias {
                    expr: Box::new(Expr::BinaryExpr {
                        left: Box::new(col("s1")),
                        op: BinaryOp::Multiply,
                        right: Box::new(Expr::Literal(ScalarValue::UInt16(2))),
                    }),
                    name: "s1".into(),
                },
                col("sv"),
            ])
            .unwrap();
        let node = join(
            scan("r", DataType::UInt16, false),
            Arc::new(projected),
            true,
        );
        let LogicalPlan::Scan(r_scan) = node.left.as_ref() else {
            unreachable!()
        };
        assert!(rule
            .build_keys(&[col("r0"), col("r1")], &node, true, r_scan)
            .is_none());
    }
    #[test]
    fn structural_domains_pack_and_single_int32_key_is_widened() {
        let rule = EagerAggregation::new();
        let node = join(
            scan("r", DataType::UInt16, false),
            scan("s", DataType::UInt16, false),
            true,
        );
        let LogicalPlan::Scan(r_scan) = node.left.as_ref() else {
            unreachable!()
        };
        let pair = rule
            .build_keys(&[col("r0"), col("r1")], &node, true, r_scan)
            .unwrap();
        assert!(pair.0.to_string().contains("65536"));
        let node = join(
            scan("r", DataType::Int32, false),
            scan("s", DataType::Int32, false),
            false,
        );
        let LogicalPlan::Scan(r_scan) = node.left.as_ref() else {
            unreachable!()
        };
        let pair = rule.build_keys(&[col("r0")], &node, true, r_scan).unwrap();
        assert_eq!(
            pair.0.data_type(&node.left.schema()).unwrap(),
            DataType::Int64
        );
        assert_eq!(
            pair.1.data_type(&node.right.schema()).unwrap(),
            DataType::Int64
        );
    }
    #[test]
    fn zero_null_estimate_does_not_override_nullable_factor_schema() {
        let rule = EagerAggregation::with_table_statistics(estimated_bounds());
        for nullable in [true, false] {
            let node = join(
                scan("r", DataType::Int64, nullable),
                scan("s", DataType::Int64, false),
                false,
            );
            let plan = LogicalPlan::Aggregate(AggregateNode {
                input: Arc::new(LogicalPlan::Join(node)),
                group_by: vec![col("s1")],
                aggregates: vec![Expr::Alias {
                    name: "total".into(),
                    expr: Box::new(Expr::Aggregate {
                        func: AggregateFunction::Sum,
                        args: vec![col("rv")],
                        distinct: false,
                    }),
                }],
                schema: PlanSchema::new(vec![
                    SchemaField::new("s1", DataType::Int64),
                    SchemaField::new("total", DataType::Float64),
                ]),
            });
            let rewritten = rule.optimize(&plan).unwrap();
            if nullable {
                assert_eq!(
                    rewritten, plan,
                    "a zero-null estimate is not a schema contract"
                );
            } else {
                assert_ne!(
                    rewritten, plan,
                    "schema-proven non-null factors remain eligible"
                );
            }
        }
    }
    #[test]
    fn floating_scalar_multiplication_is_not_moved_after_sum() {
        let rule = EagerAggregation::with_table_statistics(estimated_bounds());
        let node = join(
            scan("r", DataType::Int64, false),
            scan("s", DataType::Int64, false),
            false,
        );
        let plan = LogicalPlan::Aggregate(AggregateNode {
            input: Arc::new(LogicalPlan::Join(node)),
            group_by: vec![col("s1")],
            aggregates: vec![Expr::Alias {
                name: "total".into(),
                expr: Box::new(Expr::Aggregate {
                    func: AggregateFunction::Sum,
                    distinct: false,
                    args: vec![Expr::BinaryExpr {
                        left: Box::new(col("rv")),
                        op: BinaryOp::Multiply,
                        right: Box::new(Expr::Literal(ScalarValue::Float64(0.0.into()))),
                    }],
                }),
            }],
            schema: PlanSchema::new(vec![
                SchemaField::new("s1", DataType::Int64),
                SchemaField::new("total", DataType::Float64),
            ]),
        });
        assert_eq!(rule.optimize(&plan).unwrap(), plan);
        assert_eq!(f64::MAX * 0.0 + f64::MAX * 0.0, 0.0);
        assert!(((f64::MAX + f64::MAX) * 0.0).is_nan());
    }
}
