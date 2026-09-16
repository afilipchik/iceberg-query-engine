//! Independent expected values for batch binding, including its generic fallback.
use super::*;
use arrow::{array::*, datatypes::Int32Type};

#[test]
fn decimal_floating_states_bind_conversion_without_changing_exact_sum() {
    for scale in i8::MIN..=38 {
        let pool = MemoryPool::new_named("decimal conversion binding", 65536);
        {
            let array: ArrayRef = Arc::new(
                Decimal128Array::from(vec![Some(123), None, Some(-321)])
                    .with_precision_and_scale(38, scale)
                    .unwrap(),
            );
            let ty = array.data_type().clone();
            let layout = StateRowLayout::bind(
                &pool,
                &[
                    (AggregateFunction::Avg, ty.clone(), false),
                    (AggregateFunction::Sum, ty, false),
                ],
            )
            .unwrap()
            .unwrap();
            let mut rows = StateRows::new(layout.clone()).unwrap();
            let mut workspace = RowWorkspace::new(layout).unwrap();
            let arrays = [array.clone(), array];
            let used = pool.used();
            let inputs = rows.bind_arrays(&arrays, 3).unwrap();
            assert_eq!(pool.used(), used, "binding must not allocate");
            let factor = 10_f64.powi(-i32::from(std::hint::black_box(scale)));
            for (row, coefficient) in [(0, 123i128), (2, -321)] {
                let ScalarValue::Float64(value) = inputs.fixed_views[0].unwrap().value(row) else {
                    panic!("floating state must bind decimal conversion once per batch");
                };
                assert_eq!(value.to_bits(), (coefficient as f64 * factor).to_bits());
                let ScalarValue::Decimal128(value) = inputs.fixed_views[1].unwrap().value(row)
                else {
                    panic!("exact SUM must keep its coefficient and scale");
                };
                assert_eq!((value.mantissa(), value.scale()), (coefficient, scale));
            }
            assert!(matches!(
                inputs.fixed_views[0].unwrap().value(1),
                ScalarValue::Null
            ));
            rows.push_empty().unwrap();
            for input in [0, 1, 2] {
                rows.prepare_arrays_indexed(0, &mut workspace, &inputs, input, false)
                    .unwrap()
                    .commit();
            }
            let ScalarValue::Float64(average) = rows.value(0, 0).unwrap().into_owned() else {
                panic!("average type")
            };
            assert_eq!(
                average.to_bits(),
                ((123.0 * factor + -321.0 * factor) / 2.0).to_bits()
            );
            let ScalarValue::Decimal128(sum) = rows.value(0, 1).unwrap().into_owned() else {
                panic!("sum type")
            };
            assert_eq!((sum.mantissa(), sum.scale()), (-198, scale));
        }
        assert_eq!(pool.used(), 0);
    }
}

#[test]
fn fixed_array_batches_preserve_numeric_domains_and_dictionary_nulls() {
    let mut cases: Vec<(ArrayRef, ScalarValue)> = Vec::new();
    macro_rules! signed {
        ($t:ty) => {
            cases.push((
                Arc::new(<$t>::from(vec![Some(2), None, Some(5)])),
                ScalarValue::Int64(9),
            ));
        };
    }
    macro_rules! unsigned {
        ($t:ty) => {
            cases.push((
                Arc::new(<$t>::from(vec![Some(2), None, Some(5)])),
                ScalarValue::Decimal128(crate::planner::DecimalValue::new(9, 0)),
            ));
        };
    }
    signed!(Int8Array);
    signed!(Int16Array);
    signed!(Int32Array);
    signed!(Int64Array);
    unsigned!(UInt8Array);
    unsigned!(UInt16Array);
    unsigned!(UInt32Array);
    unsigned!(UInt64Array);
    cases.push((
        Arc::new(Float32Array::from(vec![Some(2.0), None, Some(5.0)])),
        ScalarValue::Float64(9.0.into()),
    ));
    cases.push((
        Arc::new(Float64Array::from(vec![Some(2.0), None, Some(5.0)])),
        ScalarValue::Float64(9.0.into()),
    ));
    cases.push((
        Arc::new(
            Decimal128Array::from(vec![Some(2), None, Some(5)])
                .with_precision_and_scale(38, 3)
                .unwrap(),
        ),
        ScalarValue::Decimal128(crate::planner::DecimalValue::new(9, 3)),
    ));
    for (values, expected) in cases {
        for dictionary in [false, true] {
            let ty = values.data_type().clone();
            let array: ArrayRef = if dictionary {
                Arc::new(
                    DictionaryArray::<Int32Type>::try_new(
                        Int32Array::from(vec![Some(0), Some(1), None, Some(2), Some(0)]),
                        values.clone(),
                    )
                    .unwrap(),
                )
            } else {
                arrow::compute::take(
                    values.as_ref(),
                    &UInt32Array::from(vec![Some(0), Some(1), None, Some(2), Some(0)]),
                    None,
                )
                .unwrap()
            };
            let pool = MemoryPool::new_named("batch numeric binding", 65536);
            {
                let layout = StateRowLayout::bind(
                    &pool,
                    &[
                        (AggregateFunction::Count, ty.clone(), false),
                        (AggregateFunction::Sum, ty.clone(), false),
                        (AggregateFunction::Min, DataType::Utf8, false),
                    ],
                )
                .unwrap()
                .unwrap();
                let mut rows = StateRows::new(layout.clone()).unwrap();
                let mut workspace = RowWorkspace::new(layout).unwrap();
                let arrays = [
                    array.clone(),
                    array,
                    Arc::new(StringArray::from(vec![
                        Some("z"),
                        None,
                        Some("b"),
                        Some("a"),
                        Some("c"),
                    ])) as ArrayRef,
                ];
                let before = pool.used();
                let inputs = rows.bind_arrays(&arrays, 5).unwrap();
                assert_eq!(inputs.fixed_views[1].is_some(), !dictionary);
                assert_eq!(
                    pool.used(),
                    before,
                    "binding must not add unreserved descriptor allocations"
                );
                rows.push_empty().unwrap();
                for input in [4, 2, 0, 3, 1] {
                    rows.prepare_arrays_indexed(0, &mut workspace, &inputs, input, false)
                        .unwrap()
                        .commit();
                }
                assert_eq!(
                    rows.value(0, 0).unwrap().as_ref(),
                    &ScalarValue::Int64(3),
                    "{ty:?}, dictionary={dictionary}"
                );
                assert_eq!(
                    rows.value(0, 1).unwrap().as_ref(),
                    &expected,
                    "{ty:?}, dictionary={dictionary}"
                );
                assert_eq!(
                    rows.value(0, 2).unwrap().as_ref(),
                    &ScalarValue::Utf8("a".into())
                );
            }
            assert_eq!(pool.used(), 0);
        }
    }
}

#[test]
fn wide_array_binding_keeps_late_slot_overflow_transactional() {
    let pool = MemoryPool::new_named("wide batch binding", 65536);
    {
        let mut slots = vec![(AggregateFunction::Count, DataType::Int64, false); 20];
        slots.push((AggregateFunction::Sum, DataType::Int64, false));
        let layout = StateRowLayout::bind(&pool, &slots).unwrap().unwrap();
        let mut rows = StateRows::new(layout.clone()).unwrap();
        let mut workspace = RowWorkspace::new(layout).unwrap();
        rows.push_empty().unwrap();
        let mut seed = vec![ScalarValue::Int64(1); 20];
        seed.push(ScalarValue::Int64(i64::MAX));
        rows.prepare(0, &mut workspace, &seed).unwrap().commit();
        let arrays = vec![Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef; 21];
        let before = pool.used();
        let inputs = rows.bind_arrays(&arrays, 2).unwrap();
        assert_eq!(pool.used(), before);
        assert!(rows
            .prepare_arrays_indexed(0, &mut workspace, &inputs, 0, false)
            .is_err());
        for slot in 0..20 {
            assert_eq!(
                rows.value(0, slot).unwrap().as_ref(),
                &ScalarValue::Int64(1)
            );
        }
        assert_eq!(
            rows.value(0, 20).unwrap().as_ref(),
            &ScalarValue::Int64(i64::MAX)
        );
        rows.prepare_arrays_indexed(0, &mut workspace, &inputs, 1, false)
            .unwrap()
            .commit();
        for slot in 0..20 {
            assert_eq!(
                rows.value(0, slot).unwrap().as_ref(),
                &ScalarValue::Int64(1)
            );
        }
    }
    assert_eq!(pool.used(), 0);
}
