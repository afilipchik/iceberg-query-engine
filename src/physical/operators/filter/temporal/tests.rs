use crate::{
    execution::{create_memory_pool, expression_memory::with_expression_pool},
    physical::operators::filter::evaluate_expr,
    planner::{Expr, ScalarFunction, ScalarValue},
};
use arrow::{
    array::{Array, ArrayRef, Date32Array, Int32Array},
    record_batch::RecordBatch,
};
use std::sync::Arc;
fn expression(field: &str) -> Expr {
    Expr::ScalarFunc {
        func: ScalarFunction::Extract,
        args: vec![
            Expr::literal(ScalarValue::Utf8(field.into())),
            Expr::column("d"),
        ],
    }
}
#[test]
fn extract_retains_admission_after_array_and_expression_drop() {
    let batch = RecordBatch::try_from_iter(vec![(
        "d",
        Arc::new(Date32Array::from(vec![0; 16384])) as ArrayRef,
    )])
    .unwrap();
    let pool = create_memory_pool(1024 * 1024);
    let result =
        with_expression_pool(&pool, || evaluate_expr(&batch, &expression("YEAR"))).unwrap();
    let values = result.slice(7, 3).to_data().buffers()[0].clone();
    drop(result);
    assert!(
        pool.used() >= 16384 * 4,
        "temporal values escaped without admission"
    );
    drop(values);
    assert_eq!(pool.used(), 0);
}
#[test]
fn extract_metadata_literal_is_not_expanded_per_row() {
    let batch = RecordBatch::try_from_iter(vec![(
        "d",
        Arc::new(Date32Array::from(vec![0; 16384])) as ArrayRef,
    )])
    .unwrap();
    let pool = create_memory_pool(70 * 1024);
    let result =
        with_expression_pool(&pool, || evaluate_expr(&batch, &expression("YEAR"))).unwrap();
    assert!(result
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .values()
        .iter()
        .all(|v| *v == 1970));
    drop(result);
    assert_eq!(pool.used(), 0);
    let small = create_memory_pool(4096);
    assert!(
        with_expression_pool(&small, || evaluate_expr(&batch, &expression("YEAR")))
            .unwrap_err()
            .is_memory_limit()
    );
    assert_eq!(small.used(), 0);
}
#[test]
fn extract_preserves_independent_calendar_values_nulls_and_slices() {
    let dates = Date32Array::from(vec![
        Some(123),
        Some(0),
        Some(11016),
        Some(-1),
        None,
        Some(10956),
    ])
    .slice(1, 5);
    let batch = RecordBatch::try_from_iter(vec![("d", Arc::new(dates) as ArrayRef)]).unwrap();
    for (field, expected) in [
        ("YEAR", [1970, 2000, 1969, 0, 1999]),
        ("MONTH", [1, 2, 12, 0, 12]),
        ("DAY", [1, 29, 31, 0, 31]),
        ("QUARTER", [1, 1, 4, 0, 4]),
        ("DOY", [1, 60, 365, 0, 365]),
        ("DOW", [4, 2, 3, 0, 5]),
        ("WEEK", [1, 9, 1, 0, 52]),
        ("HOUR", [0; 5]),
        ("MINUTE", [0; 5]),
        ("SECOND", [0; 5]),
    ] {
        let pool = create_memory_pool(65536);
        let result =
            with_expression_pool(&pool, || evaluate_expr(&batch, &expression(field))).unwrap();
        let actual = result.as_any().downcast_ref::<Int32Array>().unwrap();
        for row in 0..5 {
            if row == 3 {
                assert!(actual.is_null(row));
            } else {
                assert_eq!(actual.value(row), expected[row], "{field}/{row}");
            }
        }
        drop(result);
        assert_eq!(pool.used(), 0);
    }
}

#[test]
fn extract_calendar_range_errors_and_dictionary_fallback_preserve_null_semantics() {
    use arrow::array::{DictionaryArray, Int8Array};
    use arrow::datatypes::Int8Type;
    let pool = create_memory_pool(65536);
    let invalid = RecordBatch::try_from_iter(vec![(
        "d",
        Arc::new(Date32Array::from(vec![Some(0), Some(i32::MAX), None])) as ArrayRef,
    )])
    .unwrap();
    assert!(
        with_expression_pool(&pool, || evaluate_expr(&invalid, &expression("YEAR")))
            .unwrap_err()
            .to_string()
            .contains("supported temporal range")
    );
    assert_eq!(pool.used(), 0);
    let hour =
        with_expression_pool(&pool, || evaluate_expr(&invalid, &expression("HOUR"))).unwrap();
    assert_eq!(
        hour.as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(0), Some(0), None]
    );
    drop(hour);
    assert_eq!(pool.used(), 0);
    let dictionary = DictionaryArray::<Int8Type>::try_new(
        Int8Array::from(vec![Some(1), None, Some(0), Some(2)]),
        Arc::new(Date32Array::from(vec![Some(-1), Some(11016), None])),
    )
    .unwrap();
    let batch = RecordBatch::try_from_iter(vec![("d", Arc::new(dictionary) as ArrayRef)]).unwrap();
    let output =
        with_expression_pool(&pool, || evaluate_expr(&batch, &expression("year"))).unwrap();
    assert_eq!(
        output
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some(2000), None, Some(1969), None]
    );
    drop(output);
    assert_eq!(pool.used(), 0);
}
