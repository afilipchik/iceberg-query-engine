//! Recognize a top-k vector search and mark it for index pushdown.
//!
//! # The shape
//!
//! ```sql
//! SELECT id, category, text FROM vectors
//! ORDER BY cosine_distance(embedding, [0.1, ...])
//! LIMIT 10
//! ```
//!
//! which the binder and the earlier rules leave as
//!
//! ```text
//! Limit(skip=0, fetch=10)
//!   Sort([COSINE_DISTANCE(embedding, ARRAY[..]) ASC])
//!     Project*([id, category, text, embedding])      (pure column projections)
//!       Scan(vectors, filter=..)
//! ```
//!
//! and becomes a single `VectorSearch` node that keeps the `Project*(Scan)`
//! subtree as its exact fallback.
//!
//! # Why this rule is written to refuse
//!
//! A missed pushdown costs latency. A wrong pushdown returns different rows
//! than the SQL asked for. Every condition below is therefore a hard gate, and
//! anything unrecognized leaves the plan byte-for-byte unchanged:
//!
//! * exactly one ORDER BY key, and it is a distance call on `(column, literal)`
//! * the sort direction agrees with the metric's sign convention — ASC for the
//!   distances, DESC for `dot_product` / `cosine_similarity`, which are
//!   similarities. `ORDER BY cosine_distance(...) DESC` asks for the k
//!   *furthest* rows, which no vector index can answer.
//! * a `LIMIT` is present with a finite fetch (an unbounded sort is not a k-NN)
//! * the chain from the Sort down to the Scan consists only of projections that
//!   rename or reorder columns; anything computed and anything with a Filter,
//!   Join or Aggregate in it is left alone
//! * the vector column is a `FixedSizeList` whose width equals the literal's
//!   length
//!
//! The rule does not know whether the table's provider actually has an index —
//! the optimizer has no access to providers. `VectorSearchExec` asks the
//! provider at execution time and runs the fallback when the answer is no.

use crate::error::Result;
use crate::optimizer::OptimizerRule;
use crate::planner::vector_types::{as_float_vector, VectorMetric};
use crate::planner::{
    Expr, LogicalPlan, ScalarFunction, ScalarValue, SchemaField, SortDirection, SortExpr,
    VectorSearchNode,
};
use std::sync::Arc;

pub struct VectorSearchPushdown;

impl OptimizerRule for VectorSearchPushdown {
    fn name(&self) -> &str {
        "VectorSearchPushdown"
    }

    fn optimize(&self, plan: &LogicalPlan) -> Result<LogicalPlan> {
        rewrite(plan)
    }
}

fn rewrite(plan: &LogicalPlan) -> Result<LogicalPlan> {
    // Try to match at this node first.
    if let Some(node) = try_match(plan) {
        return Ok(LogicalPlan::VectorSearch(node));
    }
    // Otherwise recurse. `VectorSearch` is opaque: never descend into one.
    if matches!(plan, LogicalPlan::VectorSearch(_)) {
        return Ok(plan.clone());
    }
    let children: Result<Vec<Arc<LogicalPlan>>> = plan
        .children()
        .iter()
        .map(|c| rewrite(c).map(Arc::new))
        .collect();
    Ok(plan.with_new_children(children?))
}

/// The metric a distance function implies, together with the sort direction
/// that means "nearest first" for it.
fn metric_of(func: &ScalarFunction) -> Option<(VectorMetric, SortDirection)> {
    match func {
        ScalarFunction::L2Distance => Some((VectorMetric::L2, SortDirection::Asc)),
        ScalarFunction::CosineDistance => Some((VectorMetric::Cosine, SortDirection::Asc)),
        // Similarities: larger is closer, so nearest-first is DESC.
        ScalarFunction::CosineSimilarity => Some((VectorMetric::Cosine, SortDirection::Desc)),
        ScalarFunction::DotProduct => Some((VectorMetric::Dot, SortDirection::Desc)),
        _ => None,
    }
}

fn try_match(plan: &LogicalPlan) -> Option<VectorSearchNode> {
    let LogicalPlan::Limit(limit) = plan else {
        return None;
    };
    // An unbounded LIMIT (pure OFFSET) is not a k-NN.
    let fetch = limit.fetch?;
    if fetch == 0 {
        return None;
    }
    let LogicalPlan::Sort(sort) = limit.input.as_ref() else {
        return None;
    };
    // Multiple sort keys mean the distance is only a primary key; an index
    // cannot honour the tiebreakers.
    if sort.order_by.len() != 1 {
        return None;
    }
    let key: &SortExpr = &sort.order_by[0];

    // NULLS FIRST would put NULL-vector rows ahead of the true nearest
    // neighbours; the index never returns them at all.
    if key.nulls == crate::planner::NullOrdering::NullsFirst {
        return None;
    }

    let Expr::ScalarFunc { func, args } = strip_alias(&key.expr) else {
        return None;
    };
    let (metric, want_dir) = metric_of(func)?;
    if key.direction != want_dir {
        return None;
    }
    if args.len() != 2 {
        return None;
    }

    // One side a column, the other a constant vector. Distances are symmetric,
    // so accept either order.
    let (col_expr, query) = match (constant_vector(&args[1]), constant_vector(&args[0])) {
        (Some(q), _) => (&args[0], q),
        (None, Some(q)) => (&args[1], q),
        _ => return None,
    };
    let Expr::Column(vec_col) = strip_alias(col_expr) else {
        return None;
    };

    // Walk down through pure column projections to the scan.
    let mut cursor: &LogicalPlan = sort.input.as_ref();
    // Maps an output field of the current level to the underlying scan column.
    let out_schema = sort.input.schema();
    let mut alias_to_source: Vec<(String, String)> = out_schema
        .fields()
        .iter()
        .map(|f| (f.name.clone(), f.name.clone()))
        .collect();

    loop {
        match cursor {
            LogicalPlan::Project(p) => {
                // Every expression must be a bare (possibly aliased) column;
                // a computed projection would have to be re-evaluated on top of
                // the index results, which this node does not do.
                let mut level: Vec<(String, String)> = Vec::with_capacity(p.exprs.len());
                for (i, e) in p.exprs.iter().enumerate() {
                    let Expr::Column(c) = strip_alias(e) else {
                        return None;
                    };
                    let out_name = p.schema.fields().get(i)?.name.clone();
                    level.push((out_name, c.name.clone()));
                }
                // Compose: current alias -> this level's source.
                for entry in alias_to_source.iter_mut() {
                    let src = &entry.1;
                    let next = level.iter().find(|(o, _)| o.eq_ignore_ascii_case(src))?;
                    entry.1 = next.1.clone();
                }
                cursor = p.input.as_ref();
            }
            LogicalPlan::Scan(scan) => {
                // The vector column must exist in the scan and be a float
                // vector of exactly the literal's width.
                let scan_col = alias_to_source
                    .iter()
                    .find(|(o, _)| o.eq_ignore_ascii_case(&vec_col.name))
                    .map(|(_, s)| s.clone())
                    // The sort key may reference the scan column directly even
                    // when it is not in the output (the binder widens the
                    // projection, so normally it is).
                    .or_else(|| Some(vec_col.name.clone()))?;

                let (_, field) = scan
                    .schema
                    .resolve_column(&crate::planner::Column::new(scan_col.clone()))?;
                let (_, dim) = as_float_vector(&field.data_type)?;
                if dim != query.len() {
                    return None;
                }

                // Output mapping: (scan column name, output field).
                let outputs: Vec<(String, SchemaField)> = out_schema
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(i, f)| (alias_to_source[i].1.clone(), f.clone()))
                    .collect();

                return Some(VectorSearchNode {
                    input: sort.input.clone(),
                    table_name: scan.table_name.clone(),
                    column: scan_col,
                    query,
                    k: fetch,
                    skip: limit.skip,
                    metric,
                    filter: scan.filter.clone(),
                    outputs,
                    sort_key: key.clone(),
                    schema: out_schema,
                });
            }
            // A Filter, Join, Aggregate, Distinct or anything else below the
            // Sort takes the plan out of the recognized shape.
            _ => return None,
        }
    }
}

fn strip_alias(e: &Expr) -> &Expr {
    match e {
        Expr::Alias { expr, .. } => strip_alias(expr),
        other => other,
    }
}

/// A literal array of numbers, as `f32`.
fn constant_vector(e: &Expr) -> Option<Vec<f32>> {
    let Expr::Literal(ScalarValue::List(_, _)) = strip_alias(e) else {
        return None;
    };
    let Expr::Literal(v) = strip_alias(e) else {
        return None;
    };
    let out = crate::physical::vector::query_vector_from_scalar(v)?;
    (!out.is_empty()).then_some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{Column, LimitNode, PlanSchema, ProjectNode, ScanNode, SortNode};
    use arrow::datatypes::{DataType, Field};

    fn schema() -> PlanSchema {
        PlanSchema::new(vec![
            SchemaField::new("id", DataType::Int64),
            SchemaField::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
            ),
        ])
    }

    fn scan() -> LogicalPlan {
        LogicalPlan::Scan(ScanNode {
            table_name: "t".into(),
            schema: schema(),
            projection: None,
            filter: None,
        })
    }

    fn qvec() -> Expr {
        Expr::Literal(ScalarValue::List(
            vec![
                ScalarValue::Float64(1.0.into()),
                ScalarValue::Float64(0.0.into()),
                ScalarValue::Float64(0.0.into()),
            ],
            Box::new(DataType::Float64),
        ))
    }

    fn dist(func: ScalarFunction) -> Expr {
        Expr::ScalarFunc {
            func,
            args: vec![Expr::column("embedding"), qvec()],
        }
    }

    fn build(key: SortExpr, fetch: Option<usize>) -> LogicalPlan {
        let project = LogicalPlan::Project(ProjectNode {
            input: Arc::new(scan()),
            exprs: vec![Expr::column("id"), Expr::column("embedding")],
            schema: schema(),
        });
        LogicalPlan::Limit(LimitNode {
            input: Arc::new(LogicalPlan::Sort(SortNode {
                input: Arc::new(project),
                order_by: vec![key],
            })),
            skip: 0,
            fetch,
        })
    }

    #[test]
    fn nulls_first_is_refused() {
        // `SortExpr::new` defaults to NULLS FIRST (the binder overrides it to
        // NULLS LAST), and NULLS FIRST would put rows with a NULL embedding
        // ahead of the true nearest neighbours — rows the index never returns.
        let plan = build(
            SortExpr::new(dist(ScalarFunction::CosineDistance)).asc(),
            Some(10),
        );
        assert!(matches!(
            VectorSearchPushdown.optimize(&plan).unwrap(),
            LogicalPlan::Limit(_)
        ));
    }

    #[test]
    fn matches_canonical_cosine_shape() {
        let plan = build(
            SortExpr::new(dist(ScalarFunction::CosineDistance))
                .asc()
                .nulls_last(),
            Some(10),
        );
        let out = VectorSearchPushdown.optimize(&plan).unwrap();
        let LogicalPlan::VectorSearch(n) = out else {
            panic!("expected VectorSearch, got {}", out);
        };
        assert_eq!(n.k, 10);
        assert_eq!(n.column, "embedding");
        assert_eq!(n.metric, VectorMetric::Cosine);
        assert_eq!(n.query, vec![1.0, 0.0, 0.0]);
        assert_eq!(n.schema.fields().len(), 2);
    }

    #[test]
    fn dot_product_needs_desc_and_l2_needs_asc() {
        // dot_product is a similarity: nearest-first is DESC.
        let desc = build(
            SortExpr::new(dist(ScalarFunction::DotProduct))
                .desc()
                .nulls_last(),
            Some(5),
        );
        assert!(matches!(
            VectorSearchPushdown.optimize(&desc).unwrap(),
            LogicalPlan::VectorSearch(_)
        ));
        // ASC on a similarity asks for the FURTHEST rows. Must not match.
        let asc = build(
            SortExpr::new(dist(ScalarFunction::DotProduct))
                .asc()
                .nulls_last(),
            Some(5),
        );
        assert!(matches!(
            VectorSearchPushdown.optimize(&asc).unwrap(),
            LogicalPlan::Limit(_)
        ));
        // Symmetrically, DESC on a distance asks for the furthest rows.
        let l2_desc = build(
            SortExpr::new(dist(ScalarFunction::L2Distance))
                .desc()
                .nulls_last(),
            Some(5),
        );
        assert!(matches!(
            VectorSearchPushdown.optimize(&l2_desc).unwrap(),
            LogicalPlan::Limit(_)
        ));
    }

    #[test]
    fn refuses_without_limit_or_with_extra_keys() {
        let no_limit = build(
            SortExpr::new(dist(ScalarFunction::CosineDistance))
                .asc()
                .nulls_last(),
            None,
        );
        assert!(matches!(
            VectorSearchPushdown.optimize(&no_limit).unwrap(),
            LogicalPlan::Limit(_)
        ));

        let project = LogicalPlan::Project(ProjectNode {
            input: Arc::new(scan()),
            exprs: vec![Expr::column("id"), Expr::column("embedding")],
            schema: schema(),
        });
        let two_keys = LogicalPlan::Limit(LimitNode {
            input: Arc::new(LogicalPlan::Sort(SortNode {
                input: Arc::new(project),
                order_by: vec![
                    SortExpr::new(dist(ScalarFunction::CosineDistance))
                        .asc()
                        .nulls_last(),
                    SortExpr::new(Expr::column("id")).asc(),
                ],
            })),
            skip: 0,
            fetch: Some(10),
        });
        assert!(matches!(
            VectorSearchPushdown.optimize(&two_keys).unwrap(),
            LogicalPlan::Limit(_)
        ));
    }

    #[test]
    fn refuses_when_a_filter_sits_between_sort_and_scan() {
        // A Filter here is not the same as a scan-level pushed predicate: it
        // may reference computed columns the index cannot see.
        let filtered = LogicalPlan::Filter(crate::planner::FilterNode {
            input: Arc::new(scan()),
            predicate: Expr::column("id").gt(Expr::literal(ScalarValue::Int64(3))),
        });
        let project = LogicalPlan::Project(ProjectNode {
            input: Arc::new(filtered),
            exprs: vec![Expr::column("id"), Expr::column("embedding")],
            schema: schema(),
        });
        let plan = LogicalPlan::Limit(LimitNode {
            input: Arc::new(LogicalPlan::Sort(SortNode {
                input: Arc::new(project),
                order_by: vec![SortExpr::new(dist(ScalarFunction::CosineDistance))
                    .asc()
                    .nulls_last()],
            })),
            skip: 0,
            fetch: Some(10),
        });
        assert!(matches!(
            VectorSearchPushdown.optimize(&plan).unwrap(),
            LogicalPlan::Limit(_)
        ));
    }

    #[test]
    fn refuses_on_dimension_mismatch() {
        let bad = Expr::ScalarFunc {
            func: ScalarFunction::CosineDistance,
            args: vec![
                Expr::column("embedding"),
                Expr::Literal(ScalarValue::List(
                    vec![ScalarValue::Float64(1.0.into())],
                    Box::new(DataType::Float64),
                )),
            ],
        };
        let plan = build(SortExpr::new(bad).asc().nulls_last(), Some(10));
        assert!(matches!(
            VectorSearchPushdown.optimize(&plan).unwrap(),
            LogicalPlan::Limit(_)
        ));
    }

    #[test]
    fn scan_filter_becomes_the_prefilter() {
        let mut s = match scan() {
            LogicalPlan::Scan(n) => n,
            _ => unreachable!(),
        };
        s.filter = Some(Expr::column("id").gt(Expr::literal(ScalarValue::Int64(3))));
        let project = LogicalPlan::Project(ProjectNode {
            input: Arc::new(LogicalPlan::Scan(s)),
            exprs: vec![Expr::column("id"), Expr::column("embedding")],
            schema: schema(),
        });
        let plan = LogicalPlan::Limit(LimitNode {
            input: Arc::new(LogicalPlan::Sort(SortNode {
                input: Arc::new(project),
                order_by: vec![SortExpr::new(dist(ScalarFunction::CosineDistance))
                    .asc()
                    .nulls_last()],
            })),
            skip: 0,
            fetch: Some(10),
        });
        let LogicalPlan::VectorSearch(n) = VectorSearchPushdown.optimize(&plan).unwrap() else {
            panic!("expected VectorSearch");
        };
        assert!(n.filter.is_some());
    }

    #[test]
    fn ordinary_top_n_is_untouched() {
        let project = LogicalPlan::Project(ProjectNode {
            input: Arc::new(scan()),
            exprs: vec![Expr::column("id")],
            schema: PlanSchema::new(vec![SchemaField::new("id", DataType::Int64)]),
        });
        let plan = LogicalPlan::Limit(LimitNode {
            input: Arc::new(LogicalPlan::Sort(SortNode {
                input: Arc::new(project),
                order_by: vec![SortExpr::new(Expr::column("id")).asc()],
            })),
            skip: 0,
            fetch: Some(10),
        });
        assert_eq!(VectorSearchPushdown.optimize(&plan).unwrap(), plan);
        // And the rule is idempotent on a plan it already rewrote.
        let vs = build(
            SortExpr::new(dist(ScalarFunction::CosineDistance))
                .asc()
                .nulls_last(),
            Some(10),
        );
        let once = VectorSearchPushdown.optimize(&vs).unwrap();
        let twice = VectorSearchPushdown.optimize(&once).unwrap();
        assert_eq!(once, twice);
    }

    #[test]
    fn unused_column_helper_is_exercised() {
        // `Column` import is used by the scan-column resolution path.
        assert_eq!(Column::new("x").name, "x");
    }
}
