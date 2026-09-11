use super::*;
use arrow::datatypes::{Field, Schema};

#[test]
fn full_signed_domain_shared_decimal_and_bare_float_merges_use_hash_routing() {
    let pool = MemoryPool::new_named("full signed merge", 1 << 20);
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Decimal128(38, 0), true),
    ]));
    let make = || {
        AggregationState::from_raw_groups(
            vec![AggregateFunction::Sum],
            vec![DataType::Decimal128(38, 0)],
            DataType::Int64,
            [
                (
                    i64::MIN as u64,
                    vec![AccumulatorState::SumDecimal {
                        coefficient: Some((1i128 << 80) + 1),
                        scale: 0,
                        seen: true,
                    }],
                ),
                (
                    i64::MAX as u64,
                    vec![AccumulatorState::SumDecimal {
                        coefficient: Some(-7),
                        scale: 0,
                        seen: true,
                    }],
                ),
            ]
            .into_iter()
            .collect(),
            None,
            &pool,
        )
        .unwrap()
    };
    let batches = merge_raw_states_to_batches(
        vec![make(), make()],
        &[AggregateFunction::Sum],
        &[DataType::Decimal128(38, 0)],
        &DataType::Int64,
        &schema,
        None,
        &pool,
    )
    .unwrap();
    let mut actual = std::collections::BTreeMap::new();
    for batch in &batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        for (key, sum) in keys.iter().zip(sums.iter()) {
            assert!(actual.insert(key.unwrap(), sum.unwrap()).is_none());
        }
    }
    assert_eq!(
        actual,
        [(i64::MIN, (1i128 << 81) + 2), (i64::MAX, -14)]
            .into_iter()
            .collect()
    );
    drop(batches);
    assert_eq!(pool.used(), 0);

    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Float64, true),
    ]));
    let make = || {
        let mut state = AggregationState::new_with_pool(
            vec![AggregateFunction::Sum],
            vec![DataType::Float64],
            &pool,
        );
        state.raw_type = Some(DataType::Int64);
        state.raw_sums.insert(i64::MIN as u64, 2.0);
        state.raw_sums.insert(i64::MAX as u64, 3.0);
        state
    };
    let batches = merge_raw_sum_states_to_batches(
        vec![make(), make()],
        &[AggregateFunction::Sum],
        &[DataType::Float64],
        &DataType::Int64,
        &schema,
        None,
        &pool,
    )
    .unwrap();
    let mut actual = std::collections::BTreeMap::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for (key, sum) in keys.iter().zip(sums.iter()) {
            assert!(actual.insert(key.unwrap(), sum.unwrap()).is_none());
        }
    }
    assert_eq!(
        actual,
        [(i64::MIN, 4.0), (i64::MAX, 6.0)].into_iter().collect()
    );
    assert_eq!(pool.used(), 0);
}
