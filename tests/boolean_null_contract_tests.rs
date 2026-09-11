use arrow::array::{ArrayRef, BooleanArray, Int64Array};
use arrow::record_batch::RecordBatch;
use query_engine::physical::compiled_expr::{CompiledPredicate, PredicateEvaluator};
use query_engine::physical::operators::evaluate_expr;
use query_engine::planner::{BinaryOp, Expr, ScalarValue};
use query_engine::ExecutionContext;
use std::sync::Arc;

fn bin(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

fn truth_batch() -> RecordBatch {
    let values = [Some(false), Some(true), None];
    RecordBatch::try_from_iter(vec![
        (
            "a",
            Arc::new(BooleanArray::from(
                values.iter().flat_map(|a| [*a; 3]).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "b",
            Arc::new(BooleanArray::from(values.repeat(3))) as ArrayRef,
        ),
    ])
    .unwrap()
}

const AND: [Option<bool>; 9] = [
    Some(false),
    Some(false),
    Some(false),
    Some(false),
    Some(true),
    None,
    Some(false),
    None,
    None,
];
const OR: [Option<bool>; 9] = [
    Some(false),
    Some(true),
    None,
    Some(true),
    Some(true),
    Some(true),
    None,
    Some(true),
    None,
];

fn bools(array: &ArrayRef) -> Vec<Option<bool>> {
    array
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn direct_boolean_truth_tables_and_empty_batches() {
    let batch = truth_batch();
    for (op, expected) in [(BinaryOp::And, AND), (BinaryOp::Or, OR)] {
        let expr = bin(Expr::column("a"), op, Expr::column("b"));
        assert_eq!(bools(&evaluate_expr(&batch, &expr).unwrap()), expected);
        assert!(bools(&evaluate_expr(&batch.slice(0, 0), &expr).unwrap()).is_empty());
    }
}

#[tokio::test]
async fn sql_boolean_truth_tables_across_batches() {
    let batch = truth_batch();
    let mut ctx = ExecutionContext::new();
    ctx.register_table(
        "truths",
        batch.schema(),
        vec![batch.slice(0, 0), batch.slice(0, 4), batch.slice(4, 5)],
    );
    let result = ctx.sql("SELECT a AND b, a OR b FROM truths").await.unwrap();
    for (column, expected) in [AND, OR].iter().enumerate() {
        let actual: Vec<_> = result
            .batches
            .iter()
            .flat_map(|b| bools(b.column(column)))
            .collect();
        assert_eq!(&actual, expected);
    }
}

fn between_batch() -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "v",
            Arc::new(Int64Array::from(vec![
                Some(5),
                Some(5),
                Some(5),
                Some(5),
                Some(5),
                None,
            ])) as ArrayRef,
        ),
        (
            "lo",
            Arc::new(Int64Array::from(vec![
                Some(10),
                None,
                Some(0),
                None,
                Some(0),
                Some(0),
            ])) as ArrayRef,
        ),
        (
            "hi",
            Arc::new(Int64Array::from(vec![
                None,
                Some(0),
                None,
                Some(10),
                Some(10),
                Some(10),
            ])) as ArrayRef,
        ),
    ])
    .unwrap()
}

const BETWEEN: [Option<bool>; 6] = [Some(false), Some(false), None, None, Some(true), None];

#[test]
fn direct_between_and_compiled_boolean_fallback() {
    let batch = between_batch();
    for negated in [false, true] {
        let expr = Expr::Between {
            expr: Box::new(Expr::column("v")),
            low: Box::new(Expr::column("lo")),
            high: Box::new(Expr::column("hi")),
            negated,
        };
        let expected: Vec<_> = BETWEEN.iter().map(|v| v.map(|v| v != negated)).collect();
        assert_eq!(bools(&evaluate_expr(&batch, &expr).unwrap()), expected);
        assert_eq!(
            PredicateEvaluator::new(expr.clone())
                .evaluate(&batch)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
        assert!(bools(&evaluate_expr(&batch.slice(0, 0), &expr).unwrap()).is_empty());
    }
}

#[test]
fn compiled_numeric_boolean_truth_tables() {
    let values = [Some(0), Some(1), None];
    let batch = RecordBatch::try_from_iter(vec![
        (
            "a",
            Arc::new(Int64Array::from(
                values.iter().flat_map(|a| [*a; 3]).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "b",
            Arc::new(Int64Array::from(values.repeat(3))) as ArrayRef,
        ),
    ])
    .unwrap();
    for (op, expected) in [(BinaryOp::And, AND), (BinaryOp::Or, OR)] {
        let expr = bin(
            bin(
                Expr::column("a"),
                BinaryOp::Eq,
                Expr::literal(ScalarValue::Int64(1)),
            ),
            op,
            bin(
                Expr::column("b"),
                BinaryOp::Eq,
                Expr::literal(ScalarValue::Int64(1)),
            ),
        );
        let compiled = CompiledPredicate::compile(&expr, &batch.schema()).unwrap();
        assert!(
            compiled.evaluate(&batch).is_none(),
            "nullable Boolean program must fall back"
        );
        assert!(
            compiled.evaluate(&batch.slice(0, 2)).is_some(),
            "null-free batch must remain fused"
        );
        assert_eq!(
            PredicateEvaluator::new(expr)
                .evaluate(&batch)
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[tokio::test]
async fn sql_between_projection_and_filter_preserve_unknown() {
    let batch = between_batch();
    let mut ctx = ExecutionContext::new();
    ctx.register_table(
        "bounds",
        batch.schema(),
        vec![batch.slice(0, 2), batch.slice(2, 4)],
    );
    let result = ctx
        .sql("SELECT v BETWEEN lo AND hi, v NOT BETWEEN lo AND hi FROM bounds")
        .await
        .unwrap();
    for column in 0..2 {
        let expected: Vec<_> = BETWEEN
            .iter()
            .map(|v| v.map(|v| v != (column == 1)))
            .collect();
        let actual: Vec<_> = result
            .batches
            .iter()
            .flat_map(|b| bools(b.column(column)))
            .collect();
        assert_eq!(actual, expected);
    }
    let result = ctx
        .sql("SELECT v FROM bounds WHERE v NOT BETWEEN lo AND hi")
        .await
        .unwrap();
    assert_eq!(
        result.batches.iter().map(|b| b.num_rows()).sum::<usize>(),
        2
    );
}
