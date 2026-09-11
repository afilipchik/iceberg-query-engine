use super::*;
use crate::execution::MemoryPool;
use arrow::array::{ArrayRef, DictionaryArray, Int8Array};
use arrow::datatypes::{Field, Int8Type, Schema};
use std::sync::Arc;

fn col(name: &str) -> Expr {
    Expr::Column(Column::new(name))
}
fn lit(v: f64) -> Expr {
    Expr::Literal(ScalarValue::Float64(v.into()))
}
fn cmp(a: Expr, op: BinaryOp, b: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(a),
        op,
        right: Box::new(b),
    }
}
fn check(batch: &RecordBatch, expr: &Expr, expected: &[Option<bool>]) {
    let normal = CompiledPredicate::compile(expr, &batch.schema())
        .expect("ordinary mixed numeric compilation");
    if let Some(mask) = normal.evaluate(batch) {
        assert_eq!(mask.iter().collect::<Vec<_>>(), expected);
    } else {
        assert!(normal
            .prog
            .iter()
            .any(|instruction| matches!(instruction, Instr::And { .. } | Instr::Or { .. })));
        assert!(batch.columns().iter().any(|array| array.null_count() > 0));
        // Existing ordinary nullable Boolean fallback remains part of the contract.
        let mask = PredicateEvaluator::new(expr.clone())
            .evaluate(batch)
            .unwrap();
        assert_eq!(mask.iter().collect::<Vec<_>>(), expected);
    }
    let pool = MemoryPool::new(8 << 20);
    let admitted = CompiledPredicate::compile_reserved(expr, &batch.schema(), &pool)
        .unwrap()
        .expect("reserved mixed numeric compilation");
    let mask = admitted
        .evaluate_admitted(batch, &pool)
        .unwrap()
        .expect("supported physical binding");
    assert_eq!(mask.iter().collect::<Vec<_>>(), expected);
    drop(mask);
    drop(admitted);
    assert_eq!(pool.used(), 0);
}
fn decimal(values: Vec<Option<i128>>, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(38, scale)
            .unwrap(),
    )
}
fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(
            columns
                .iter()
                .map(|(name, a)| Field::new(*name, a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        )),
        columns.into_iter().map(|(_, a)| a).collect(),
    )
    .unwrap()
}
#[test]
fn rounded_decimal_float_equality_is_not_exact_decimal_ordering() {
    let n = 1i128 << 53;
    let b = batch(vec![(
        "d",
        decimal(
            vec![
                Some(n),
                Some(n + 1),
                Some(n + 2),
                Some(-n - 1),
                Some(0),
                None,
            ],
            0,
        ),
    )]);
    for reverse in [false, true] {
        let (a, z) = if reverse {
            (lit(n as f64), col("d"))
        } else {
            (col("d"), lit(n as f64))
        };
        check(
            &b,
            &cmp(a, BinaryOp::Eq, z),
            &[
                Some(true),
                Some(true),
                Some(false),
                Some(false),
                Some(false),
                None,
            ],
        );
    }
    let b = batch(vec![(
        "i",
        Arc::new(Int64Array::from(vec![
            Some(n as i64 + 1),
            Some(n as i64 + 2),
            None,
        ])),
    )]);
    check(
        &b,
        &cmp(col("i"), BinaryOp::Eq, lit(n as f64)),
        &[Some(true), Some(false), None],
    );
}
#[test]
fn mixed_float_domains_match_independent_arrow_coercion() {
    let max = 10i128.pow(38) - 1;
    for scale in [-128, -6, 0, 2, 18, 38] {
        let d = decimal(
            vec![
                Some(-max),
                Some(-(1i128 << 53) - 1),
                Some(-7),
                Some(-1),
                Some(0),
                Some(1),
                Some(5),
                Some(7),
                Some((1i128 << 53) + 1),
                Some(max),
                None,
            ],
            scale,
        );
        let f: ArrayRef = Arc::new(Float64Array::from(vec![
            Some(f64::NEG_INFINITY),
            Some(-9007199254740992.0),
            Some(-0.07),
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            Some(0.05),
            Some(0.07),
            Some(9007199254740992.0),
            Some(f64::INFINITY),
            Some(1.0),
        ]));
        let b = batch(vec![("d", d), ("f", f)]);
        for op in [
            BinaryOp::Eq,
            BinaryOp::NotEq,
            BinaryOp::Lt,
            BinaryOp::LtEq,
            BinaryOp::Gt,
            BinaryOp::GtEq,
        ] {
            for reverse in [false, true] {
                let (a, z) = if reverse {
                    (col("f"), col("d"))
                } else {
                    (col("d"), col("f"))
                };
                let expr = cmp(a, op, z);
                let oracle = crate::physical::operators::evaluate_expr(&b, &expr).unwrap();
                let expected = oracle
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();
                check(&b, &expr, &expected);
            }
        }
    }
    for integer in [
        Arc::new(Int32Array::from(vec![
            Some(i32::MIN),
            Some(0),
            Some(i32::MAX),
            None,
        ])) as ArrayRef,
        Arc::new(Int64Array::from(vec![
            Some(i64::MIN),
            Some(0),
            Some(i64::MAX),
            None,
        ])),
    ] {
        let b = batch(vec![("i", integer)]);
        for value in [
            f64::NEG_INFINITY,
            -0.0,
            0.0,
            16777217.0,
            9007199254740992.0,
            f64::INFINITY,
            f64::NAN,
        ] {
            let expr = cmp(col("i"), BinaryOp::LtEq, lit(value));
            let oracle = crate::physical::operators::evaluate_expr(&b, &expr).unwrap();
            check(
                &b,
                &expr,
                &oracle
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
            );
        }
    }
}
#[test]
fn between_reuses_conversion_and_preserves_kleene_logic_across_chunks() {
    let pattern = [Some(4), Some(5), Some(6), Some(7), Some(8), None];
    let b = batch(vec![(
        "d",
        decimal(pattern.into_iter().cycle().take(2 * CHUNK + 7).collect(), 2),
    )]);
    for negated in [false, true] {
        let range = Expr::Between {
            expr: Box::new(col("d")),
            low: Box::new(lit(0.05)),
            high: Box::new(lit(0.07)),
            negated,
        };
        let compiled = CompiledPredicate::compile(&range, &b.schema()).expect("range must compile");
        assert_eq!(
            compiled.f_regs, 1,
            "one conversion register for the shared bound column"
        );
        let expected = b
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .map(|v| v.map(|x| (5..=7).contains(&x) ^ negated))
            .collect::<Vec<_>>();
        check(&b, &range, &expected);
        for op in [BinaryOp::And, BinaryOp::Or] {
            let expr = cmp(range.clone(), op, cmp(col("d"), BinaryOp::Eq, lit(0.04)));
            let oracle = crate::physical::operators::evaluate_expr(&b, &expr).unwrap();
            check(
                &b,
                &expr,
                &oracle
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
            );
        }
        let empty = b.slice(0, 0);
        check(&empty, &range, &[]);
        let sliced = b.slice(3, 7);
        let oracle = crate::physical::operators::evaluate_expr(&sliced, &range).unwrap();
        check(
            &sliced,
            &range,
            &oracle
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
        );
    }
}
#[test]
fn unsupported_numeric_domains_and_physical_encodings_decline() {
    let schema = Schema::new(vec![Field::new("d", DataType::Decimal128(38, 38), true)]);
    let expr = cmp(col("d"), BinaryOp::Lt, Expr::Literal(ScalarValue::Int64(1)));
    assert!(
        crate::planner::numeric::common_type(&DataType::Decimal128(38, 38), &DataType::Int64)
            .is_err()
    );
    assert!(
        CompiledPredicate::compile_for_admitted_evaluation(&expr, &schema).is_none(),
        "unrepresentable common decimal type must decline"
    );
    let schema = Schema::new(vec![Field::new("d", DataType::Date32, true)]);
    assert!(CompiledPredicate::compile(&cmp(col("d"), BinaryOp::Eq, lit(1.0)), &schema).is_none());
    let b = batch(vec![("d", decimal(vec![Some(5), None], 2))]);
    let expr = cmp(col("d"), BinaryOp::Eq, lit(0.05));
    let c = CompiledPredicate::compile(&expr, &b.schema()).expect("logical comparison compiles");
    let values = decimal(vec![Some(5), None], 2);
    let dictionary =
        DictionaryArray::<Int8Type>::try_new(Int8Array::from(vec![Some(0), None, Some(1)]), values)
            .unwrap();
    let encoded = batch(vec![("d", Arc::new(dictionary))]);
    assert!(c.evaluate(&encoded).is_none());
    let pool = MemoryPool::new(8 << 20);
    assert!(c.evaluate_admitted(&encoded, &pool).unwrap().is_none());
    assert_eq!(pool.used(), 0);
}
