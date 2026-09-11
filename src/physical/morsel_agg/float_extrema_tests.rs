use super::*;

fn assert_value(state: &AccumulatorState, expected: f64) {
    let value = match state {
        AccumulatorState::Min(Some(ScalarValue::Float64(value)))
        | AccumulatorState::Max(Some(ScalarValue::Float64(value))) => value.into_inner(),
        _ => panic!("unexpected state: {state:?}"),
    };
    assert!(
        if expected.is_nan() {
            value.is_nan()
        } else {
            value == expected
        },
        "{state:?}, expected {expected}"
    );
}

#[test]
fn float_extrema_update_and_merge_obey_the_same_sql_order() {
    // Explicit SQL-domain oracle: NaN is above +infinity; zeros compare equal.
    // Test every split, both merge orders, and both scalar/typed ingestion.
    for (values, min, max) in [
        (vec![f64::NAN, 3.0, -7.0, f64::NAN], -7.0, f64::NAN),
        (vec![3.0, f64::NAN, -7.0], -7.0, f64::NAN),
        (
            vec![f64::NEG_INFINITY, f64::NAN, f64::INFINITY],
            f64::NEG_INFINITY,
            f64::NAN,
        ),
        (
            vec![f64::NAN, f64::from_bits(0xfff8_0000_0000_0042)],
            f64::NAN,
            f64::NAN,
        ),
        (vec![-0.0, 0.0, -0.0], 0.0, 0.0),
        (vec![3.0, -7.0, 3.0], -7.0, 3.0),
    ] {
        for (function, expected) in [(AggregateFunction::Min, min), (AggregateFunction::Max, max)] {
            for typed in [false, true] {
                for split in 0..=values.len() {
                    let mut left = AccumulatorState::new(&function, &DataType::Float64);
                    let mut right = AccumulatorState::new(&function, &DataType::Float64);
                    left.update(&ScalarValue::Null);
                    right.update(&ScalarValue::Null);
                    for (state, values) in [
                        (&mut left, &values[..split]),
                        (&mut right, &values[split..]),
                    ] {
                        for &value in values {
                            if typed {
                                state.update_f64(value);
                            } else {
                                state.update(&ScalarValue::Float64(value.into()));
                            }
                        }
                    }
                    let mut reverse = right.clone();
                    reverse.merge(&left);
                    left.merge(&right);
                    assert_value(&left, expected);
                    assert_value(&reverse, expected);
                }
            }
        }
    }
}
