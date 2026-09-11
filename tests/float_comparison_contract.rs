//! Independent SQL oracle for interpreted, folded and compiled comparisons.
use arrow::array::{Array, BooleanArray, Float32Array, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use serde::Deserialize;
use std::sync::Arc;

#[derive(Deserialize)]
struct Case {
    left: Option<String>,
    right: Option<String>,
    expected: Vec<Option<bool>>,
}
#[derive(Deserialize)]
struct Oracle {
    operators: Vec<String>,
    cases: Vec<Case>,
}
fn oracle() -> Oracle {
    serde_json::from_str(include_str!("fixtures/float_comparison_duckdb_1_4_4.json")).unwrap()
}
fn number(value: &Option<String>) -> Option<f64> {
    value.as_ref().map(|v| v.parse().unwrap())
}

#[tokio::test]
async fn array_comparison_matches_independent_float_domain_oracle() {
    let o = oracle();
    for encoding in ["string_cast", "float64", "float32"] {
        let (left, right): (Arc<dyn Array>, Arc<dyn Array>) = match encoding {
            "string_cast" => (
                Arc::new(StringArray::from(
                    o.cases
                        .iter()
                        .map(|c| c.left.as_deref())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    o.cases
                        .iter()
                        .map(|c| c.right.as_deref())
                        .collect::<Vec<_>>(),
                )),
            ),
            "float32" => (
                Arc::new(Float32Array::from(
                    o.cases
                        .iter()
                        .map(|c| number(&c.left).map(|n| n as f32))
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Float32Array::from(
                    o.cases
                        .iter()
                        .map(|c| number(&c.right).map(|n| n as f32))
                        .collect::<Vec<_>>(),
                )),
            ),
            _ => (
                Arc::new(Float64Array::from(
                    o.cases.iter().map(|c| number(&c.left)).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    o.cases.iter().map(|c| number(&c.right)).collect::<Vec<_>>(),
                )),
            ),
        };
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..o.cases.len() as i64)) as Arc<dyn Array>,
            ),
            ("a", left),
            ("b", right),
        ])
        .unwrap();
        let mut ctx = ExecutionContext::new().with_parallel_partitions(3);
        ctx.register_table(
            "t",
            batch.schema(),
            vec![batch.slice(0, 17), batch.slice(17, o.cases.len() - 17)],
        );
        let (a, b) = if encoding == "string_cast" {
            ("CAST(a AS DOUBLE)", "CAST(b AS DOUBLE)")
        } else {
            ("a", "b")
        };
        let expressions = o
            .operators
            .iter()
            .enumerate()
            .map(|(i, op)| format!("{a} {op} {b} AS v{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let result = ctx
            .sql(&format!("SELECT {expressions} FROM t ORDER BY id"))
            .await
            .unwrap();
        for column in 0..o.operators.len() {
            let values = result
                .batches
                .iter()
                .flat_map(|b| {
                    b.column(column)
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .iter()
                })
                .collect::<Vec<_>>();
            assert_eq!(
                values,
                o.cases
                    .iter()
                    .map(|c| c.expected[column])
                    .collect::<Vec<_>>(),
                "{encoding}: {}",
                o.operators[column]
            );
        }
    }
}

#[tokio::test]
async fn constant_comparison_matches_the_same_float_domain_oracle() {
    let o = oracle();
    let ctx = ExecutionContext::new();
    for c in &o.cases {
        let literal = |v: &Option<String>| v.as_ref().map_or("NULL".into(), |s| format!("'{s}'"));
        let a = literal(&c.left);
        let b = literal(&c.right);
        let expressions = o
            .operators
            .iter()
            .enumerate()
            .map(|(i, op)| format!("CAST({a} AS DOUBLE) {op} CAST({b} AS DOUBLE) AS v{i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let result = ctx.sql(&format!("SELECT {expressions}")).await.unwrap();
        for column in 0..o.operators.len() {
            let value = result.batches[0]
                .column(column)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap();
            assert_eq!(
                value.iter().next().unwrap(),
                c.expected[column],
                "{a} {} {b}",
                o.operators[column]
            );
        }
    }
}

#[test]
fn compiled_float_masks_match_independent_oracle_including_nulls() {
    use query_engine::physical::compiled_expr::CompiledPredicate;
    use query_engine::planner::{BinaryOp, Expr};
    let o = oracle();
    let batch = RecordBatch::try_from_iter(vec![
        (
            "a",
            Arc::new(Float64Array::from(
                o.cases.iter().map(|c| number(&c.left)).collect::<Vec<_>>(),
            )) as Arc<dyn Array>,
        ),
        (
            "b",
            Arc::new(Float64Array::from(
                o.cases.iter().map(|c| number(&c.right)).collect::<Vec<_>>(),
            )) as Arc<dyn Array>,
        ),
    ])
    .unwrap();
    for (column, op) in [
        BinaryOp::Eq,
        BinaryOp::NotEq,
        BinaryOp::Lt,
        BinaryOp::LtEq,
        BinaryOp::Gt,
        BinaryOp::GtEq,
    ]
    .into_iter()
    .enumerate()
    {
        let expr = Expr::BinaryExpr {
            left: Box::new(Expr::column("a")),
            op,
            right: Box::new(Expr::column("b")),
        };
        let compiled =
            CompiledPredicate::compile(&expr, &batch.schema()).expect("float comparisons compile");
        for batch in [batch.clone(), batch.slice(1, 30)] {
            let start = if batch.num_rows() == 30 { 1 } else { 0 };
            let result = compiled
                .evaluate(&batch)
                .expect("compiled path must execute");
            assert_eq!(
                result.iter().collect::<Vec<_>>(),
                o.cases[start..start + batch.num_rows()]
                    .iter()
                    .map(|c| c.expected[column])
                    .collect::<Vec<_>>()
            );
        }
    }
}

#[tokio::test]
async fn broadcasts_case_membership_and_between_use_sql_float_equality() {
    let values = Float64Array::from(vec![Some(-0.0), Some(0.0), Some(f64::NAN), None]);
    let batch =
        RecordBatch::try_from_iter(vec![("a", Arc::new(values) as Arc<dyn Array>)]).unwrap();
    let mut ctx = ExecutionContext::new();
    ctx.register_table("t", batch.schema(), vec![batch]);
    for expression in [
        "a = CAST('0.0' AS DOUBLE)",
        "CAST('0.0' AS DOUBLE) = a",
        "a IN (CAST('0.0' AS DOUBLE))",
        "a BETWEEN CAST('-0.0' AS DOUBLE) AND CAST('0.0' AS DOUBLE)",
    ] {
        let result = ctx
            .sql(&format!("SELECT {expression} AS v FROM t"))
            .await
            .unwrap();
        let actual = result
            .batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
            })
            .collect::<Vec<_>>();
        assert_eq!(
            actual,
            vec![Some(true), Some(true), Some(false), None],
            "{expression}"
        );
    }
    let result = ctx
        .sql("SELECT CASE a WHEN CAST('0.0' AS DOUBLE) THEN true ELSE false END AS v FROM t")
        .await
        .unwrap();
    let actual = result
        .batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
        })
        .collect::<Vec<_>>();
    assert_eq!(
        actual,
        vec![Some(true), Some(true), Some(false), Some(false)]
    );
}
