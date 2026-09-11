use super::*;
use std::collections::BTreeMap;

fn snapshot(state: &AggregationState) -> BTreeMap<i64, f64> {
    let mut values = BTreeMap::new();
    for (key, accs) in state.raw_groups.iter() {
        let AccumulatorState::Sum(value, true) = accs[0] else {
            panic!()
        };
        *values.entry(*key as i64).or_default() += value;
    }
    for (key, value) in &state.raw_sums {
        *values.entry(*key as i64).or_default() += value;
    }
    for (key, accs) in &state.groups {
        let [ScalarValue::Int64(key)] = key.values.as_slice() else {
            panic!()
        };
        let AccumulatorState::Sum(value, true) = accs[0] else {
            panic!()
        };
        *values.entry(*key).or_default() += value;
    }
    values
}

fn check(normalize: bool, headroom: usize) {
    let pool = MemoryPool::new_named("migration query", 1 << 20);
    let mut state = AggregationState::new_with_pool(
        vec![AggregateFunction::Sum],
        vec![DataType::Float64],
        &pool,
    );
    state.overflowed = true;
    state.raw_type = Some(DataType::Int64);
    // An overlapping destination group also detects double application when
    // retrying a migration after a successfully moved prefix.
    state
        .raw_groups
        .insert_or_merge(0, &[AccumulatorState::Sum(10.0, true)])
        .unwrap();
    for key in 0..256 {
        let value = key as f64 + 0.25;
        if normalize {
            state.groups.insert(
                GroupKey {
                    values: vec![ScalarValue::Int64(key)],
                },
                vec![AccumulatorState::Sum(value, true)],
            );
        } else {
            state.raw_sums.insert(key as u64, value);
        }
    }
    let expected = snapshot(&state);
    let blocker = pool.allocate(pool.available() - headroom).unwrap();
    let error = if normalize {
        state.normalize_raw()
    } else {
        state.demote_raw_sums()
    }
    .unwrap_err();
    assert!(error.is_memory_limit(), "{error}");
    assert_eq!(
        snapshot(&state),
        expected,
        "admission denial discarded aggregate state"
    );
    if headroom > 0 {
        assert!(
            state.raw_groups.len() > 1,
            "fixture must admit a prefix before denial"
        );
    }
    drop(blocker);
    if normalize {
        state.normalize_raw().unwrap();
    } else {
        state.demote_raw_sums().unwrap();
    }
    assert!(state.groups.is_empty());
    assert!(state.raw_sums.is_empty());
    assert_eq!(state.raw_groups.len(), 256);
    assert_eq!(
        snapshot(&state),
        expected,
        "resuming migration duplicated a prefix"
    );
    drop(state);
    assert_eq!(pool.used(), 0);
}

#[test]
fn generic_to_raw_migration_survives_admission_denial() {
    for headroom in [0, 6000] {
        check(true, headroom);
    }
}

#[test]
fn bare_sum_to_arena_migration_survives_admission_denial() {
    for headroom in [0, 6000] {
        check(false, headroom);
    }
}
