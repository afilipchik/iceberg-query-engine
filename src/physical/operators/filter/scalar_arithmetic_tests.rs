use super::*;
use crate::execution::{expression_memory::with_expression_pool, MemoryPool};
use arrow::array::Decimal128Array;

fn decimal_batch(values: Vec<Option<i128>>, precision: u8, scale: i8) -> RecordBatch {
    let values: ArrayRef = Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    );
    RecordBatch::try_from_iter(vec![("v", values)]).unwrap()
}

fn expression(op: BinaryOp, scalar_left: bool, literal: i64) -> Expr {
    let scalar = Expr::literal(ScalarValue::Int64(literal));
    let column = Expr::column("v");
    let (left, right) = if scalar_left {
        (scalar, column)
    } else {
        (column, scalar)
    };
    Expr::BinaryExpr {
        left: Box::new(left),
        op,
        right: Box::new(right),
    }
}

#[test]
fn decimal_literal_arithmetic_fits_output_sized_workspace() {
    let len = 4096;
    let batch = decimal_batch(
        (0..len + 3)
            .map(|i| {
                if i % 7 == 0 {
                    None
                } else {
                    Some((i % 11) as i128 - 5)
                }
            })
            .collect(),
        10,
        2,
    )
    .slice(3, len);
    for aggregate_root in [false, true] {
        for scalar_left in [false, true] {
            for op in [BinaryOp::Subtract, BinaryOp::Multiply] {
                // Input ownership is external to this expression scope. Allow
                // output values/validity and metadata, not full literal/coercion
                // arrays. This applies to any batch, not a named SQL query.
                let pool = Arc::new(MemoryPool::new(len * 16 + len.div_ceil(8) + 8192));
                let expr = expression(op, scalar_left, 2);
                let result = with_expression_pool(&pool, || {
                    if aggregate_root {
                        evaluate_aggregate_inputs(&batch, 1, |_| &expr, Ok)
                            .map(|mut arrays| arrays.remove(0))
                    } else {
                        evaluate_expr(&batch, &expr)
                    }
                })
                .expect("scalar arithmetic must not expand and coerce a full literal array");
                let output = result.as_any().downcast_ref::<Decimal128Array>().unwrap();
                let precision = if op == BinaryOp::Multiply { 30 } else { 22 };
                assert_eq!(output.data_type(), &DataType::Decimal128(precision, 2));
                let input = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap();
                let expected: Vec<_> = input
                    .iter()
                    .map(|value| {
                        value.map(|v| match op {
                            BinaryOp::Multiply => v * 2,
                            BinaryOp::Subtract if scalar_left => 200 - v,
                            BinaryOp::Subtract => v - 200,
                            _ => unreachable!(),
                        })
                    })
                    .collect();
                assert_eq!(output.iter().collect::<Vec<_>>(), expected);
                let escaped = result.slice(1, 3).to_data().buffers()[0].clone();
                drop(result);
                assert!(pool.used() > 0, "escaped output lost its admission owner");
                drop(escaped);
                assert_eq!(pool.used(), 0);
            }
        }
    }
}

#[test]
fn decimal_literal_scaling_does_no_value_work_for_empty_or_null_rows() {
    for len in [0, 17] {
        let batch = decimal_batch(vec![None; len], 38, 38);
        let pool = Arc::new(MemoryPool::new(1024 * 1024));
        for scalar_left in [false, true] {
            let expr = expression(BinaryOp::Add, scalar_left, i64::MAX);
            let result = with_expression_pool(&pool, || evaluate_expr(&batch, &expr)).unwrap();
            assert_eq!(result.data_type(), &DataType::Decimal128(38, 38));
            assert_eq!(result.len(), len);
            assert_eq!(result.null_count(), len);
            drop(result);
            assert_eq!(pool.used(), 0);
        }
    }
}

#[test]
fn decimal_literal_scaling_preserves_late_overflow_and_releases_allocations() {
    let batch = decimal_batch(vec![None, Some(0)], 38, 38);
    let pool = Arc::new(MemoryPool::new(1024 * 1024));
    for scalar_left in [false, true] {
        let expr = expression(BinaryOp::Add, scalar_left, i64::MAX);
        let error = with_expression_pool(&pool, || evaluate_expr(&batch, &expr)).unwrap_err();
        assert!(!error.is_memory_limit(), "{error}");
        assert!(
            error.to_string().to_lowercase().contains("overflow"),
            "{error}"
        );
        assert_eq!(pool.used(), 0);
    }
}

#[test]
fn scalar_decimal_all_exact_operators_match_independent_arrow_arrays() {
    use arrow::compute::kernels::numeric;
    let batch = decimal_batch(vec![Some(-3), Some(7), None, Some(7), Some(-11)], 10, 2);
    let expanded: ArrayRef = Arc::new(
        Decimal128Array::from(vec![2_i128; batch.num_rows()])
            .with_precision_and_scale(19, 0)
            .unwrap(),
    );
    for op in [
        BinaryOp::Add,
        BinaryOp::Subtract,
        BinaryOp::Multiply,
        BinaryOp::Modulo,
    ] {
        for scalar_left in [false, true] {
            let (l, r) = if scalar_left {
                (&expanded, batch.column(0))
            } else {
                (batch.column(0), &expanded)
            };
            // Construct independently coerced Arrow operands, outside the
            // engine expression scope. No engine-versus-itself oracle.
            let expected = match op {
                BinaryOp::Add => numeric::add(l, r),
                BinaryOp::Subtract => numeric::sub(l, r),
                BinaryOp::Multiply => numeric::mul(l, r),
                BinaryOp::Modulo => numeric::rem(l, r),
                _ => unreachable!(),
            }
            .unwrap();
            for aggregate_root in [false, true] {
                let pool = Arc::new(MemoryPool::new(64 * 1024));
                let expr = expression(op, scalar_left, 2);
                let actual = with_expression_pool(&pool, || {
                    if aggregate_root {
                        evaluate_aggregate_inputs(&batch, 1, |_| &expr, Ok)
                            .map(|mut arrays| arrays.remove(0))
                    } else {
                        evaluate_expr(&batch, &expr)
                    }
                })
                .unwrap();
                assert_eq!(actual.data_type(), expected.data_type());
                let coefficients = |a: &ArrayRef| {
                    a.as_any()
                        .downcast_ref::<Decimal128Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>()
                };
                assert_eq!(
                    coefficients(&actual),
                    coefficients(&expected),
                    "{op:?}, left={scalar_left}"
                );
                drop(actual);
                assert_eq!(pool.used(), 0);
            }
        }
    }
}

#[test]
fn scalar_decimal_nested_roots_reuse_without_changing_cardinality() {
    let batch = decimal_batch(vec![Some(7), None, Some(-3), Some(7)], 10, 2);
    let inner = Expr::BinaryExpr {
        left: Box::new(Expr::column("v")),
        op: BinaryOp::Add,
        right: Box::new(Expr::column("v")),
    };
    let outer = Expr::BinaryExpr {
        left: Box::new(inner.clone()),
        op: BinaryOp::Add,
        right: Box::new(Expr::literal(ScalarValue::Int64(2)).alias("constant")),
    };
    let roots = [inner, outer.clone(), outer];
    let pool = Arc::new(MemoryPool::new(64 * 1024));
    let result = with_expression_pool(&pool, || {
        evaluate_aggregate_inputs(&batch, roots.len(), |i| &roots[i], Ok)
    })
    .unwrap();
    assert!(
        Arc::ptr_eq(&result[1], &result[2]),
        "successful prior root should still be shared"
    );
    assert_eq!(result[1].data_type(), &DataType::Decimal128(22, 2));
    assert_eq!(
        result[1]
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(214), None, Some(194), Some(214)]
    );
    drop(result);
    assert_eq!(pool.used(), 0);

    // Two scalar operands must emit the current batch cardinality, not one row.
    let expr = Expr::BinaryExpr {
        left: Box::new(Expr::literal(ScalarValue::Decimal128(
            crate::planner::DecimalValue::new(123, 2),
        ))),
        op: BinaryOp::Add,
        right: Box::new(Expr::literal(ScalarValue::Int64(2))),
    };
    for input in [batch.slice(0, 0), batch] {
        let result = with_expression_pool(&pool, || evaluate_expr(&input, &expr)).unwrap();
        assert_eq!(result.data_type(), &DataType::Decimal128(38, 2));
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(323); input.num_rows()]
        );
        drop(result);
        assert_eq!(pool.used(), 0);
    }
}

#[test]
fn scalar_decimal_refuses_real_output_pressure_and_preserves_modulo_errors() {
    let batch = decimal_batch(vec![Some(7); 4096], 10, 2);
    let pool = Arc::new(MemoryPool::new(4096));
    let expr = expression(BinaryOp::Multiply, false, 2);
    let error = with_expression_pool(&pool, || evaluate_expr(&batch, &expr)).unwrap_err();
    assert!(error.is_memory_limit(), "{error}");
    assert_eq!(pool.used(), 0);
    let pool = Arc::new(MemoryPool::new(64 * 1024));
    for values in [vec![None, None], vec![None, Some(7)]] {
        let batch = decimal_batch(values.clone(), 10, 2);
        let expr = expression(BinaryOp::Modulo, false, 0);
        let result = with_expression_pool(&pool, || evaluate_expr(&batch, &expr));
        if values.iter().all(Option::is_none) {
            let array = result.unwrap();
            assert_eq!(array.null_count(), 2);
            drop(array);
        } else {
            let error = result.unwrap_err();
            assert!(error.to_string().to_lowercase().contains("zero"), "{error}");
        }
        assert_eq!(pool.used(), 0);
    }
}

#[test]
fn scalar_decimal_integer_domains_and_negative_scales_match_arrow() {
    use arrow::compute::kernels::numeric;
    for scale in [-2, 0, 2] {
        let batch = decimal_batch(vec![Some(-3), None, Some(7)], 10, scale);
        for (literal, coefficient, precision, literal_scale) in [
            (ScalarValue::Int8(-2), -2_i128, 3, 0),
            (ScalarValue::UInt64(u64::MAX), i128::from(u64::MAX), 20, 0),
            (
                ScalarValue::Decimal128(crate::planner::DecimalValue::new(125, 2)),
                125,
                38,
                2,
            ),
        ] {
            let expanded: ArrayRef = Arc::new(
                Decimal128Array::from(vec![coefficient; batch.num_rows()])
                    .with_precision_and_scale(precision, literal_scale)
                    .unwrap(),
            );
            for op in [
                BinaryOp::Add,
                BinaryOp::Subtract,
                BinaryOp::Multiply,
                BinaryOp::Modulo,
            ] {
                for scalar_left in [false, true] {
                    let (l, r) = if scalar_left {
                        (&expanded, batch.column(0))
                    } else {
                        (batch.column(0), &expanded)
                    };
                    let expected = match op {
                        BinaryOp::Add => numeric::add(l, r),
                        BinaryOp::Subtract => numeric::sub(l, r),
                        BinaryOp::Multiply => numeric::mul(l, r),
                        BinaryOp::Modulo => numeric::rem(l, r),
                        _ => unreachable!(),
                    }
                    .unwrap();
                    let scalar = Expr::literal(literal.clone());
                    let column = Expr::column("v");
                    let (left, right) = if scalar_left {
                        (scalar, column)
                    } else {
                        (column, scalar)
                    };
                    let expr = Expr::BinaryExpr {
                        left: Box::new(left),
                        op,
                        right: Box::new(right),
                    };
                    let pool = Arc::new(MemoryPool::new(64 * 1024));
                    let actual =
                        with_expression_pool(&pool, || evaluate_expr(&batch, &expr)).unwrap();
                    assert_eq!(actual.data_type(), expected.data_type());
                    let coefficients = |a: &ArrayRef| {
                        a.as_any()
                            .downcast_ref::<Decimal128Array>()
                            .unwrap()
                            .iter()
                            .collect::<Vec<_>>()
                    };
                    assert_eq!(
                        coefficients(&actual),
                        coefficients(&expected),
                        "{op:?}, scale={scale}, left={scalar_left}, literal={literal:?}"
                    );
                    drop(actual);
                    assert_eq!(pool.used(), 0);
                }
            }
        }
    }
}
