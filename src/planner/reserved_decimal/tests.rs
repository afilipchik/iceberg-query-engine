use crate::execution::{create_memory_pool, expression_memory::with_expression_pool};
use crate::planner::{numeric, BinaryOp};
use arrow::{
    array::{Array, ArrayRef, Decimal128Array},
    datatypes::DataType,
};
use std::sync::Arc;
fn array(values: Vec<Option<i128>>, precision: u8, scale: i8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, scale)
            .unwrap(),
    )
}
#[test]
fn decimal_arithmetic_refuses_before_output_and_retains_extracted_buffers() {
    let l = array(vec![Some(123); 4096], 10, 2);
    let r = array(vec![Some(4567); 4096], 11, 3);
    for op in [
        BinaryOp::Add,
        BinaryOp::Subtract,
        BinaryOp::Multiply,
        BinaryOp::Modulo,
    ] {
        let small = create_memory_pool(4096);
        let result = with_expression_pool(&small, || numeric::arithmetic(op, &l, &r));
        assert!(
            result.is_err(),
            "{op:?} allocated an unadmitted decimal output"
        );
        assert!(result.unwrap_err().is_memory_limit());
        assert_eq!(small.used(), 0);
        let pool = create_memory_pool(1024 * 1024);
        let output = with_expression_pool(&pool, || numeric::arithmetic(op, &l, &r)).unwrap();
        let escaped = output.slice(7, 19).to_data().buffers()[0].clone();
        drop(output);
        assert!(pool.used() >= 4096 * 16, "{op:?} lost output ownership");
        drop(escaped);
        assert_eq!(pool.used(), 0);
    }
}
#[test]
fn decimal_arithmetic_independent_coefficients_scales_nulls_and_slices() {
    let l = array(vec![Some(999), Some(123), None, Some(-456), Some(0)], 10, 2).slice(1, 4);
    let r = array(vec![None, Some(4567), Some(7), None, Some(1)], 11, 3).slice(1, 4);
    let pool = create_memory_pool(1024 * 1024);
    for (op, datatype, expected) in [
        (
            BinaryOp::Add,
            DataType::Decimal128(12, 3),
            vec![Some(5797), None, None, Some(1)],
        ),
        (
            BinaryOp::Subtract,
            DataType::Decimal128(12, 3),
            vec![Some(-3337), None, None, Some(-1)],
        ),
        (
            BinaryOp::Multiply,
            DataType::Decimal128(22, 5),
            vec![Some(561741), None, None, Some(0)],
        ),
        (
            BinaryOp::Modulo,
            DataType::Decimal128(11, 3),
            vec![Some(1230), None, None, Some(0)],
        ),
    ] {
        let output = with_expression_pool(&pool, || numeric::arithmetic(op, &l, &r)).unwrap();
        assert_eq!(output.data_type(), &datatype);
        assert_eq!(
            output
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
        drop(output);
        assert_eq!(pool.used(), 0);
    }
}
#[test]
fn decimal_negative_scales_empty_inputs_and_validity_owners() {
    let l = array(vec![Some(12), Some(-12), None], 3, -2);
    let r = array(vec![Some(3), Some(3), Some(0)], 2, 1);
    let pool = create_memory_pool(1024 * 1024);
    for (op, datatype, expected) in [
        (
            BinaryOp::Add,
            DataType::Decimal128(7, 1),
            vec![Some(12003), Some(-11997), None],
        ),
        (
            BinaryOp::Subtract,
            DataType::Decimal128(7, 1),
            vec![Some(11997), Some(-12003), None],
        ),
        (
            BinaryOp::Multiply,
            DataType::Decimal128(6, -1),
            vec![Some(36), Some(-36), None],
        ),
        (
            BinaryOp::Modulo,
            DataType::Decimal128(2, 1),
            vec![Some(0), Some(0), None],
        ),
    ] {
        let result = with_expression_pool(&pool, || numeric::arithmetic(op, &l, &r)).unwrap();
        assert_eq!(result.data_type(), &datatype);
        assert_eq!(
            result
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            expected
        );
        let validity = result.to_data().nulls().unwrap().buffer().clone();
        drop(result);
        assert!(pool.used() > 0);
        drop(validity);
        assert_eq!(pool.used(), 0);
        let empty = with_expression_pool(&pool, || {
            numeric::arithmetic(op, &l.slice(0, 0), &r.slice(0, 0))
        })
        .unwrap();
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.data_type(), &datatype);
        drop(empty);
        assert_eq!(pool.used(), 0);
    }
}
#[test]
fn decimal_late_errors_release_prefix_without_confusing_resource_errors() {
    let pool = create_memory_pool(1024 * 1024);
    let max = 10i128.pow(38) - 1;
    for (op, l, r) in [
        (
            BinaryOp::Add,
            array(vec![Some(1), Some(max)], 38, 0),
            array(vec![Some(1), Some(1)], 1, 0),
        ),
        (
            BinaryOp::Multiply,
            array(vec![Some(1), Some(max)], 38, 0),
            array(vec![Some(2), Some(2)], 1, 0),
        ),
        (
            BinaryOp::Modulo,
            array(vec![Some(1), Some(2)], 2, 0),
            array(vec![Some(1), Some(0)], 1, 0),
        ),
    ] {
        let error = with_expression_pool(&pool, || numeric::arithmetic(op, &l, &r)).unwrap_err();
        assert!(!error.is_memory_limit());
        assert_eq!(pool.used(), 0);
    }
    let a = array(vec![Some(1), None], 2, 0);
    assert!(with_expression_pool(&pool, || numeric::arithmetic(
        BinaryOp::Add,
        &a,
        &a.slice(0, 1)
    ))
    .is_err());
    assert_eq!(pool.used(), 0);
}

#[test]
fn decimal_metadata_inference_is_allocation_independent_and_matches_arrow() {
    let pool = create_memory_pool(4096);
    let pressure = pool.allocate(pool.available()).unwrap();
    let ty = with_expression_pool(&pool, || {
        numeric::arithmetic_type(
            BinaryOp::Add,
            &DataType::Decimal128(10, 2),
            &DataType::Decimal128(11, 3),
        )
    })
    .unwrap();
    assert_eq!(ty, DataType::Decimal128(12, 3));
    drop(pressure);
    for lp in [1, 10, 38] {
        for rp in [1, 10, 38] {
            for ls in [-89, -10, 0, 1, 10, 38] {
                for rs in [-89, -10, 0, 1, 10, 38] {
                    if ls > lp as i8 || rs > rp as i8 {
                        continue;
                    }
                    let l = array(vec![], lp, ls);
                    let r = array(vec![], rp, rs);
                    for op in [
                        BinaryOp::Add,
                        BinaryOp::Subtract,
                        BinaryOp::Multiply,
                        BinaryOp::Modulo,
                    ] {
                        let expected = numeric::arithmetic(op, &l, &r);
                        let actual =
                            with_expression_pool(&pool, || numeric::arithmetic(op, &l, &r));
                        match (expected, actual) {
                            (Ok(e), Ok(a)) => assert_eq!(a.data_type(), e.data_type()),
                            (Err(_), Err(_)) => {}
                            other => {
                                panic!("metadata mismatch {lp}/{ls} {rp}/{rs} {op:?}: {other:?}")
                            }
                        }
                        assert_eq!(pool.used(), 0);
                    }
                }
            }
        }
    }
}

#[test]
fn mixed_signed_unsigned_division_has_the_declared_float_contract() {
    use arrow::array::{Float64Array, Int64Array, UInt64Array};
    let l: ArrayRef = Arc::new(Int64Array::from(vec![Some(-1), Some(7), None]));
    let r: ArrayRef = Arc::new(UInt64Array::from(vec![Some(2), Some(2), Some(u64::MAX)]));
    assert_eq!(
        numeric::arithmetic_type(BinaryOp::Divide, l.data_type(), r.data_type()).unwrap(),
        DataType::Float64
    );
    for admitted in [false, true] {
        let pool = create_memory_pool(65536);
        let run = || numeric::arithmetic(BinaryOp::Divide, &l, &r);
        let value = if admitted {
            with_expression_pool(&pool, run)
        } else {
            run()
        }
        .unwrap();
        assert_eq!(value.data_type(), &DataType::Float64);
        assert_eq!(
            value
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-0.5), Some(3.5), None]
        );
        drop(value);
        assert_eq!(pool.used(), 0);
    }
}
