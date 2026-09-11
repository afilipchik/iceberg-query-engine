//! Positive local-binding proof only. Not provider immutability or execution admission.
use crate::planner::{BinaryOp, Column, Expr, LogicalPlan, PlanSchema, ScalarValue, UnaryOp};
use arrow::datatypes::{DataType, SchemaRef};
use std::sync::Arc;

#[derive(Debug, Clone)]
pub(crate) struct ClosedInt64Membership {
    pub left_index: usize,
    pub left_column: Column,
    pub negated: bool,
    pub subquery: Arc<LogicalPlan>,
}

/// `provider_schema` must inspect the exact provider registry later used for execution.
/// The certificate expires if that registry/schema/plan is replaced. It is NOT a
/// license for eager errors or a claim the provider itself is immutable/deterministic.
pub(crate) fn prove_closed_int64_membership(
    predicate: &Expr,
    outer: &SchemaRef,
    provider_schema: impl Fn(&str) -> Option<SchemaRef>,
) -> Option<ClosedInt64Membership> {
    let Expr::InSubquery {
        expr,
        subquery,
        negated,
    } = predicate
    else {
        return None;
    };
    let Expr::Column(column) = expr.as_ref() else {
        return None;
    };
    let outer_plan = PlanSchema::from_qualified_arrow(outer);
    let left_index = resolved(column, &outer_plan)?.0;
    // The runtime's more permissive suffix lookup must resolve the SAME ordinal.
    if crate::physical::operators::find_column_index_in_schema(outer, column).ok()? != left_index
        || outer.field(left_index).data_type() != &DataType::Int64
    {
        return None;
    }
    let output = closed_plan(subquery, &provider_schema)?;
    if output.len() != 1
        || output.fields()[0].data_type != DataType::Int64
        || subquery.schema() != output
    {
        return None;
    }
    Some(ClosedInt64Membership {
        left_index,
        left_column: column.clone(),
        negated: *negated,
        subquery: subquery.clone(),
    })
}
fn unique_schema(schema: &PlanSchema) -> bool {
    schema.fields().iter().enumerate().all(|(i, f)| {
        !f.name.is_empty()
            && schema.fields().iter().enumerate().all(|(j, g)| {
                i == j || (f.name != g.name && f.qualified_name() != g.qualified_name())
            })
    })
}
fn resolved<'a>(
    column: &Column,
    schema: &'a PlanSchema,
) -> Option<(usize, &'a crate::planner::SchemaField)> {
    if !unique_schema(schema) {
        return None;
    }
    schema.resolve_column(column)
}
fn supported_scalar(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Null | DataType::Boolean | DataType::Int64 | DataType::Utf8
    )
}
fn scalar_type(expr: &Expr, schema: &PlanSchema) -> Option<DataType> {
    match expr {
        Expr::Column(c) => {
            let dt = resolved(c, schema)?.1.data_type.clone();
            supported_scalar(&dt).then_some(dt)
        }
        Expr::Literal(ScalarValue::Null) => Some(DataType::Null),
        Expr::Literal(ScalarValue::Boolean(_)) => Some(DataType::Boolean),
        Expr::Literal(ScalarValue::Int64(_)) => Some(DataType::Int64),
        Expr::Literal(ScalarValue::Utf8(_)) => Some(DataType::Utf8),
        Expr::UnaryExpr { op, expr } => {
            let t = scalar_type(expr, schema)?;
            match op {
                UnaryOp::IsNull | UnaryOp::IsNotNull => Some(DataType::Boolean),
                UnaryOp::Not if matches!(t, DataType::Boolean | DataType::Null) => {
                    Some(DataType::Boolean)
                }
                UnaryOp::Not | UnaryOp::Negate => None,
            }
        }
        Expr::BinaryExpr { left, op, right } => {
            let l = scalar_type(left, schema)?;
            let r = scalar_type(right, schema)?;
            let ok = match op {
                BinaryOp::And | BinaryOp::Or => {
                    matches!(l, DataType::Boolean | DataType::Null)
                        && matches!(r, DataType::Boolean | DataType::Null)
                }
                BinaryOp::Eq
                | BinaryOp::NotEq
                | BinaryOp::Lt
                | BinaryOp::LtEq
                | BinaryOp::Gt
                | BinaryOp::GtEq => l == r && l != DataType::Null,
                BinaryOp::Like | BinaryOp::NotLike => {
                    l == DataType::Utf8
                        && matches!(right.as_ref(),Expr::Literal(ScalarValue::Utf8(pattern)) if !pattern.contains('\\'))
                }
                BinaryOp::Add
                | BinaryOp::Subtract
                | BinaryOp::Multiply
                | BinaryOp::Divide
                | BinaryOp::Modulo
                | BinaryOp::StringConcat => false,
            };
            ok.then_some(DataType::Boolean)
        }
        // Explicit rejection list: new Expr variants require review at compile time.
        Expr::Literal(_)
        | Expr::Aggregate { .. }
        | Expr::ScalarFunc { .. }
        | Expr::Cast { .. }
        | Expr::Case { .. }
        | Expr::InList { .. }
        | Expr::Between { .. }
        | Expr::ScalarSubquery(_)
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::Alias { .. }
        | Expr::WindowFunction(_)
        | Expr::Wildcard
        | Expr::QualifiedWildcard(_) => None,
    }
}
fn projected_column<'a>(expr: &'a Expr) -> Option<&'a Column> {
    match expr {
        Expr::Column(c) => Some(c),
        Expr::Alias { expr, .. } if matches!(expr.as_ref(), Expr::Column(_)) => {
            projected_column(expr)
        }
        _ => None,
    }
}
fn closed_plan(
    plan: &LogicalPlan,
    providers: &impl Fn(&str) -> Option<SchemaRef>,
) -> Option<PlanSchema> {
    match plan {
        LogicalPlan::Scan(scan) => {
            let provider = providers(&scan.table_name)?;
            if provider.fields().len() != scan.schema.len() || !unique_schema(&scan.schema) {
                return None;
            }
            for (actual, bound) in provider.fields().iter().zip(scan.schema.fields()) {
                // No alias guessing: aliased scans/subquery aliases are outside this slice.
                if actual.name() != &bound.name
                    || actual.data_type() != &bound.data_type
                    || actual.is_nullable() != bound.nullable
                    || bound.relation.as_deref() != Some(scan.table_name.as_str())
                {
                    return None;
                }
            }
            if let Some(filter) = &scan.filter {
                if scalar_type(filter, &scan.schema)? != DataType::Boolean {
                    return None;
                }
            }
            // Scan.schema remains the complete provider schema even when projected.
            // Validate every root index rather than PlanSchema::project's filter_map.
            let output = if let Some(projection) = &scan.projection {
                let fields = projection
                    .iter()
                    .map(|i| scan.schema.fields().get(*i).cloned())
                    .collect::<Option<Vec<_>>>()?;
                PlanSchema::new(fields)
            } else {
                scan.schema.clone()
            };
            unique_schema(&output).then_some(output)
        }
        LogicalPlan::Filter(filter) => {
            let input = closed_plan(&filter.input, providers)?;
            (scalar_type(&filter.predicate, &input)? == DataType::Boolean).then_some(input)
        }
        LogicalPlan::Project(project) => {
            let input = closed_plan(&project.input, providers)?;
            if project.exprs.len() != project.schema.len() || !unique_schema(&project.schema) {
                return None;
            }
            for (expr, output) in project.exprs.iter().zip(project.schema.fields()) {
                let source = resolved(projected_column(expr)?, &input)?.1;
                if output.data_type != source.data_type || (!output.nullable && source.nullable) {
                    return None;
                }
                let expected = expr.to_field(&input).ok()?;
                if output.name != expected.name || output.relation != expected.relation {
                    return None;
                }
            }
            Some(project.schema.clone())
        }
        LogicalPlan::Join(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Window(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::Distinct(_)
        | LogicalPlan::Union(_)
        | LogicalPlan::SubqueryAlias(_)
        | LogicalPlan::EmptyRelation(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::DelimJoin(_)
        | LogicalPlan::DelimGet(_)
        | LogicalPlan::VectorSearch(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::{
        FilterNode, LimitNode, ProjectNode, ScalarFunction, ScanNode, SchemaField,
    };
    use arrow::datatypes::{Field, Schema};
    fn fixture() -> (Expr, SchemaRef, SchemaRef) {
        let provider = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, true),
            Field::new("comment", DataType::Utf8, true),
        ]));
        let scan_schema = PlanSchema::new(vec![
            SchemaField::new("key", DataType::Int64).with_relation("supplier"),
            SchemaField::new("comment", DataType::Utf8).with_relation("supplier"),
        ]);
        let filter = Expr::BinaryExpr {
            left: Box::new(Expr::qualified_column("supplier", "comment")),
            op: BinaryOp::Like,
            right: Box::new(Expr::Literal(ScalarValue::Utf8(
                "%Customer%Complaints%".into(),
            ))),
        };
        let scan = LogicalPlan::Scan(ScanNode {
            table_name: "supplier".into(),
            schema: scan_schema,
            projection: Some(vec![0]),
            filter: Some(filter),
        });
        let project = LogicalPlan::Project(ProjectNode {
            input: Arc::new(scan),
            exprs: vec![Expr::qualified_column("supplier", "key").alias("k")],
            schema: PlanSchema::new(vec![SchemaField::new("k", DataType::Int64)]),
        });
        let predicate = Expr::InSubquery {
            expr: Box::new(Expr::column("outer_key")),
            subquery: Arc::new(project),
            negated: true,
        };
        let outer = Arc::new(Schema::new(vec![Field::new(
            "outer_key",
            DataType::Int64,
            true,
        )]));
        (predicate, outer, provider)
    }
    fn prove(p: &Expr, o: &SchemaRef, s: &SchemaRef) -> Option<ClosedInt64Membership> {
        prove_closed_int64_membership(p, o, |name| (name == "supplier").then(|| s.clone()))
    }
    fn project_mut(p: &mut Expr) -> &mut ProjectNode {
        let Expr::InSubquery { subquery, .. } = p else {
            panic!()
        };
        let LogicalPlan::Project(project) = Arc::make_mut(subquery) else {
            panic!()
        };
        project
    }
    #[test]
    fn closes_local_projection_and_pushed_filter_using_non_emitted_provider_root() {
        let (p, o, s) = fixture();
        let proof = prove(&p, &o, &s).expect("closed scan/filter/project");
        assert_eq!(proof.left_index, 0);
        assert!(proof.negated);
        assert_eq!(
            proof.subquery.schema().fields()[0].data_type,
            DataType::Int64
        );
        assert_eq!(proof.subquery.schema().fields()[0].name, "k");
    }
    #[test]
    fn unqualified_rhs_is_resolved_not_assumed_local() {
        let (mut p, o, s) = fixture();
        project_mut(&mut p).exprs[0] = Expr::column("key").alias("k");
        assert!(prove(&p, &o, &s).is_some());
        project_mut(&mut p).exprs[0] = Expr::column("outer_key").alias("k");
        assert!(prove(&p, &o, &s).is_none());
        project_mut(&mut p).exprs[0] = Expr::qualified_column("outer", "key").alias("k");
        assert!(prove(&p, &o, &s).is_none());
    }
    #[test]
    fn missing_projected_root_and_bad_projection_or_schema_decline() {
        for bad_index in [1, usize::MAX] {
            let (mut p, o, s) = fixture();
            let LogicalPlan::Scan(scan) = Arc::make_mut(&mut project_mut(&mut p).input) else {
                panic!()
            };
            scan.projection = Some(vec![bad_index]);
            assert!(prove(&p, &o, &s).is_none());
        }
        let (mut p, o, s) = fixture();
        project_mut(&mut p).schema =
            PlanSchema::new(vec![SchemaField::new("k", DataType::Float64)]);
        assert!(prove(&p, &o, &s).is_none());
        let (p, o, _) = fixture();
        let wrong = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("comment", DataType::Utf8, true),
        ]));
        assert!(prove(&p, &o, &wrong).is_none());
    }
    #[test]
    fn volatile_nested_subquery_and_unknown_plan_nodes_decline() {
        let (mut p, o, s) = fixture();
        let project = project_mut(&mut p);
        project.input = Arc::new(LogicalPlan::Filter(FilterNode {
            input: project.input.clone(),
            predicate: Expr::ScalarFunc {
                func: ScalarFunction::Random,
                args: vec![],
            },
        }));
        assert!(prove(&p, &o, &s).is_none());
        let (mut p, o, s) = fixture();
        let project = project_mut(&mut p);
        project.input = Arc::new(LogicalPlan::Limit(LimitNode {
            input: project.input.clone(),
            skip: 0,
            fetch: Some(1),
        }));
        assert!(prove(&p, &o, &s).is_none());
        let (mut p, o, s) = fixture();
        let nested = p.clone();
        let project = project_mut(&mut p);
        project.input = Arc::new(LogicalPlan::Filter(FilterNode {
            input: project.input.clone(),
            predicate: nested,
        }));
        assert!(prove(&p, &o, &s).is_none());
    }
    #[test]
    fn outer_domain_ambiguity_and_computed_lhs_decline() {
        let (mut p, _, s) = fixture();
        for dtype in [
            DataType::Float64,
            DataType::UInt64,
            DataType::Decimal128(38, 0),
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int64)),
        ] {
            let o = Arc::new(Schema::new(vec![Field::new("outer_key", dtype, true)]));
            assert!(prove(&p, &o, &s).is_none());
        }
        let o = Arc::new(Schema::new(vec![
            Field::new("outer_key", DataType::Int64, true),
            Field::new("outer_key", DataType::Int64, true),
        ]));
        assert!(prove(&p, &o, &s).is_none());
        let Expr::InSubquery { expr, .. } = &mut p else {
            panic!()
        };
        *expr = Box::new(Expr::column("outer_key").add(Expr::Literal(ScalarValue::Int64(0))));
        let (_, o, _) = fixture();
        assert!(prove(&p, &o, &s).is_none());
    }
    #[test]
    fn comparison_requires_exact_types_and_no_error_prone_casts() {
        let schema = PlanSchema::new(vec![SchemaField::new("v", DataType::Int64)]);
        let eq = Expr::column("v").eq(Expr::Literal(ScalarValue::Int64(2)));
        assert_eq!(scalar_type(&eq, &schema), Some(DataType::Boolean));
        let mixed = Expr::column("v").eq(Expr::Literal(ScalarValue::Utf8("2".into())));
        assert!(scalar_type(&mixed, &schema).is_none());
        let cast = Expr::Cast {
            expr: Box::new(Expr::Literal(ScalarValue::Utf8("bad".into()))),
            data_type: DataType::Int64,
            mode: crate::planner::CastMode::Strict,
        };
        assert!(scalar_type(&cast, &schema).is_none());
    }
}
