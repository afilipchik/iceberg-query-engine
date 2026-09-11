use arrow::array::{
    ArrayRef, BooleanArray, Decimal128Array, DictionaryArray, Float64Array, Int32Array, Int64Array,
    StringArray, UInt64Array,
};
use arrow::datatypes::{DataType, Int32Type};
use arrow::record_batch::RecordBatch;
use query_engine::physical::operators::evaluate_expr;
use query_engine::planner::{BinaryOp, CastMode, DecimalValue, Expr, ScalarValue};
use std::{cmp::Ordering, sync::Arc};

const OPS: [BinaryOp; 6] = [
    BinaryOp::Eq,
    BinaryOp::NotEq,
    BinaryOp::Lt,
    BinaryOp::LtEq,
    BinaryOp::Gt,
    BinaryOp::GtEq,
];
fn bin(left: Expr, op: BinaryOp, right: Expr) -> Expr {
    Expr::BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}
fn lit(value: ScalarValue) -> Expr {
    Expr::Literal(value)
}
fn cast(expr: Expr, data_type: DataType, mode: CastMode) -> Expr {
    Expr::Cast {
        expr: Box::new(expr),
        data_type,
        mode,
    }
}
fn evaluate(batch: &RecordBatch, expr: &Expr) -> Vec<Option<bool>> {
    evaluate_expr(batch, expr)
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap()
        .iter()
        .collect()
}
fn expected(order: Option<Ordering>, op: BinaryOp) -> Option<bool> {
    order.map(|o| match op {
        BinaryOp::Eq => o == Ordering::Equal,
        BinaryOp::NotEq => o != Ordering::Equal,
        BinaryOp::Lt => o == Ordering::Less,
        BinaryOp::LtEq => o != Ordering::Greater,
        BinaryOp::Gt => o == Ordering::Greater,
        BinaryOp::GtEq => o != Ordering::Less,
        _ => unreachable!(),
    })
}
fn check_six(array: ArrayRef, scalar: Expr, orders: &[Option<Ordering>]) {
    let batch = RecordBatch::try_from_iter(vec![("v", array)]).unwrap();
    for op in OPS {
        for reverse in [false, true] {
            let expr = if reverse {
                bin(scalar.clone(), op, Expr::column("v"))
            } else {
                bin(Expr::column("v"), op, scalar.clone())
            };
            let want: Vec<_> = orders
                .iter()
                .map(|o| expected(o.map(|o| if reverse { o.reverse() } else { o }), op))
                .collect();
            assert_eq!(evaluate(&batch, &expr), want, "{op:?} reverse={reverse}");
            assert!(evaluate(&batch.slice(0, 0), &expr).is_empty());
        }
    }
}

#[test]
fn decimal_scales_all_comparisons_and_directions() {
    let array = Decimal128Array::from(vec![Some(-100), Some(123), Some(124), None])
        .with_precision_and_scale(10, 2)
        .unwrap();
    // Scale 3 literal must compare by exact decimal value, not raw coefficient.
    check_six(
        Arc::new(array),
        lit(ScalarValue::Decimal128(DecimalValue::new(1230, 3))),
        &[
            Some(Ordering::Less),
            Some(Ordering::Equal),
            Some(Ordering::Greater),
            None,
        ],
    );
}

#[test]
fn integer_signed_unsigned_boundaries_remain_exact() {
    check_six(
        Arc::new(Int64Array::from(vec![Some(-1), Some(i64::MAX), None])),
        lit(ScalarValue::UInt64(u64::MAX)),
        &[Some(Ordering::Less), Some(Ordering::Less), None],
    );
    check_six(
        Arc::new(UInt64Array::from(vec![
            Some(0),
            Some(i64::MAX as u64),
            Some(u64::MAX),
            None,
        ])),
        lit(ScalarValue::Int64(i64::MAX)),
        &[
            Some(Ordering::Less),
            Some(Ordering::Equal),
            Some(Ordering::Greater),
            None,
        ],
    );
}

#[test]
fn float_sql_order_nan_and_signed_zero() {
    let values = [
        Some(f64::NEG_INFINITY),
        Some(-0.0),
        Some(0.0),
        Some(f64::INFINITY),
        Some(f64::NAN),
        None,
    ];
    // Independent DuckDB fixture in float_comparison_duckdb_1_4_4.json:
    // signed zeros are equal; NaN equals NaN and is above finite/infinite values.
    for (scalar, orders) in [
        (
            0.0,
            [
                Some(Ordering::Less),
                Some(Ordering::Equal),
                Some(Ordering::Equal),
                Some(Ordering::Greater),
                Some(Ordering::Greater),
                None,
            ],
        ),
        (
            f64::NAN,
            [
                Some(Ordering::Less),
                Some(Ordering::Less),
                Some(Ordering::Less),
                Some(Ordering::Less),
                Some(Ordering::Equal),
                None,
            ],
        ),
    ] {
        check_six(
            Arc::new(Float64Array::from(values.to_vec())),
            lit(ScalarValue::Float64(scalar.into())),
            &orders,
        );
    }
}

#[test]
fn dictionary_keys_and_values_both_contribute_nulls() {
    let array = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), Some(1), None, Some(2), Some(3)]),
        Arc::new(StringArray::from(vec![
            Some("a"),
            Some("m"),
            None,
            Some("z"),
        ])),
    )
    .unwrap();
    check_six(
        Arc::new(array),
        lit(ScalarValue::Utf8("m".into())),
        &[
            Some(Ordering::Less),
            Some(Ordering::Equal),
            None,
            None,
            Some(Ordering::Greater),
        ],
    );
}

#[test]
fn scalar_null_cast_modes_both_scalars_and_empty_batches() {
    let batch = RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
    )])
    .unwrap();
    for scalar in [
        lit(ScalarValue::Null),
        cast(
            lit(ScalarValue::Utf8("bad".into())),
            DataType::Int64,
            CastMode::Try,
        ),
    ] {
        for op in OPS {
            assert_eq!(
                evaluate(&batch, &bin(Expr::column("v"), op, scalar.clone())),
                vec![None; 3]
            );
            assert_eq!(
                evaluate(&batch, &bin(scalar.clone(), op, lit(ScalarValue::Int64(1)))),
                vec![None; 3]
            );
        }
    }
    let strict = cast(
        lit(ScalarValue::Utf8("bad".into())),
        DataType::Int64,
        CastMode::Strict,
    );
    let expr = bin(Expr::column("v"), BinaryOp::Eq, strict);
    assert!(evaluate_expr(&batch, &expr).is_err());
    assert!(evaluate(&batch.slice(0, 0), &expr).is_empty());
    let good = cast(
        lit(ScalarValue::Utf8("2".into())),
        DataType::Int64,
        CastMode::Strict,
    );
    let both = bin(good, BinaryOp::Gt, lit(ScalarValue::Int64(1)));
    assert_eq!(evaluate(&batch, &both), vec![Some(true); 3]);
    assert!(evaluate(&batch.slice(0, 0), &both).is_empty());
}

#[test]
fn between_scalar_bounds_and_selected_case_keep_sql_semantics() {
    let batch = RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(Int64Array::from(vec![Some(0), Some(1), Some(2), None])) as ArrayRef,
    )])
    .unwrap();
    for negated in [false, true] {
        let expr = Expr::Between {
            expr: Box::new(Expr::column("v")),
            low: Box::new(lit(ScalarValue::Int64(1))),
            high: Box::new(lit(ScalarValue::Int64(2))),
            negated,
        };
        assert_eq!(
            evaluate(&batch, &expr),
            vec![Some(negated), Some(!negated), Some(!negated), None]
        );
    }
    let invalid = bin(
        cast(
            lit(ScalarValue::Utf8("bad".into())),
            DataType::Int64,
            CastMode::Strict,
        ),
        BinaryOp::Eq,
        lit(ScalarValue::Int64(0)),
    );
    let expr = Expr::Case {
        operand: None,
        when_then: vec![(lit(ScalarValue::Boolean(false)), invalid)],
        else_expr: Some(Box::new(bin(
            lit(ScalarValue::Int64(1)),
            BinaryOp::Eq,
            lit(ScalarValue::Int64(1)),
        ))),
    };
    assert_eq!(evaluate(&batch, &expr), vec![Some(true); 4]);
    assert!(evaluate(&batch.slice(0, 0), &expr).is_empty());
}

#[test]
fn both_untyped_nulls_and_aliased_literal_casts() {
    let batch = RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(Int64Array::from(vec![0, 1, 2])) as ArrayRef,
    )])
    .unwrap();
    for op in OPS {
        let expr = bin(lit(ScalarValue::Null), op, lit(ScalarValue::Null));
        assert_eq!(evaluate(&batch, &expr), vec![None; 3]);
        assert!(evaluate(&batch.slice(0, 0), &expr).is_empty());
    }
    let alias = Expr::Alias {
        expr: Box::new(cast(
            lit(ScalarValue::Utf8("1".into())),
            DataType::Int64,
            CastMode::Strict,
        )),
        name: "constant".into(),
    };
    assert_eq!(
        evaluate(&batch, &bin(Expr::column("v"), BinaryOp::Eq, alias)),
        vec![Some(false), Some(true), Some(false)]
    );
}

#[test]
fn dictionary_scalar_cast_preserves_full_result_length() {
    let batch = RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(Int64Array::from(vec![0, 1, 2])) as ArrayRef,
    )])
    .unwrap();
    let constant = cast(
        lit(ScalarValue::Utf8("m".into())),
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
        CastMode::Strict,
    );
    for op in OPS {
        let expr = bin(constant.clone(), op, lit(ScalarValue::Utf8("m".into())));
        assert_eq!(
            evaluate(&batch, &expr),
            vec![expected(Some(Ordering::Equal), op); 3]
        );
        assert!(evaluate(&batch.slice(0, 0), &expr).is_empty());
    }
}
