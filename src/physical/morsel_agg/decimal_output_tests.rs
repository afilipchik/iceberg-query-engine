use super::*;
use arrow::array::Decimal128Array;
use arrow::datatypes::{Field, Schema};
use std::collections::BTreeMap;

fn decimal(v: i128, scale: i8) -> ScalarValue {
    ScalarValue::Decimal128(DecimalValue::new(v, scale))
}

#[test]
fn decimal_output_exact_rescaling_nulls_and_lease_lifetime() {
    for (source_scale, target_scale, coefficient, expected) in [
        (2, 4, 9_007_199_254_740_993i128, 900_719_925_474_099_300i128),
        (-3, -1, -9_007_199_254_740_993, -900_719_925_474_099_300),
        (4, 2, 123400, 1234),
    ] {
        let pool = MemoryPool::new_named("decimal output", 8192);
        let output = build_decimal_output(
            [
                Ok(decimal(coefficient, source_scale)),
                Ok(ScalarValue::Null),
                Ok(decimal(0, source_scale)),
            ],
            3,
            38,
            target_scale,
            &pool,
        )
        .unwrap();
        let values = output.as_any().downcast_ref::<Decimal128Array>().unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(expected), None, Some(0)]
        );
        assert_eq!(output.data_type(), &DataType::Decimal128(38, target_scale));
        let charged = pool.used();
        assert!(charged >= 3 * 16 + 1);
        let clone = output.clone();
        let slice = output.slice(1, 2);
        drop(output);
        drop(clone);
        assert_eq!(pool.used(), charged);
        assert!(slice.is_null(0));
        drop(slice);
        assert_eq!(pool.used(), 0);
    }
}

#[test]
fn decimal_output_refuses_before_iteration_and_releases_partial_admission() {
    let pool = MemoryPool::new_named("decimal test budget", 1024);
    let visited = std::cell::Cell::new(false);
    let error = build_decimal_output(
        std::iter::repeat_with(|| {
            visited.set(true);
            Ok(decimal(1, 2))
        }),
        128,
        38,
        2,
        &pool,
    )
    .unwrap_err();
    assert!(error.to_string().contains("decimal test budget"), "{error}");
    assert!(!visited.get());
    assert_eq!(pool.used(), 0);
    // Coefficients fit, but simultaneous validity ownership does not.
    assert!(build_decimal_output([Ok(decimal(1, 2))], 1, 38, 2, &pool).is_err());
    assert_eq!(pool.used(), 0);
}

#[test]
fn decimal_output_conversion_errors_are_terminal_and_release_buffers() {
    let pool = MemoryPool::new(65536);
    for (values, rows, precision, scale) in [
        (vec![Ok(decimal(100, 0))], 1, 2, 0),
        (vec![Ok(decimal(i128::MAX, 0))], 1, 38, 1),
        (vec![Ok(ScalarValue::Int64(1))], 1, 38, 2),
        (vec![Ok(ScalarValue::Null)], 1, 0, 2),
        (vec![Ok(ScalarValue::Null)], 1, 38, 100),
        (vec![Ok(decimal(1, 2))], 2, 38, 2),
        (
            vec![
                Ok(decimal(1, 2)),
                Err(QueryError::Execution("injected finalize failure".into())),
            ],
            2,
            38,
            2,
        ),
    ] {
        assert!(build_decimal_output(values, rows, precision, scale, &pool).is_err());
        assert_eq!(pool.used(), 0);
    }
    let empty = build_decimal_output([], 0, 38, -2, &pool).unwrap();
    assert_eq!(empty.len(), 0);
    drop(empty);
    assert_eq!(pool.used(), 0);
}

fn raw_state(groups: usize, start: usize) -> AggregationState {
    let map = (start..start + groups)
        .map(|key| {
            (
                key as u64,
                vec![AccumulatorState::SumDecimal {
                    coefficient: Some(key as i128 * 100),
                    scale: 2,
                    seen: true,
                }],
            )
        })
        .collect();
    AggregationState::from_raw_groups(
        vec![AggregateFunction::Sum],
        vec![DataType::Decimal128(38, 2)],
        DataType::Int64,
        map,
        None,
        &process_memory_pool(),
    )
    .unwrap()
}
fn output_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, true),
        Field::new("s", DataType::Decimal128(38, 2), true),
    ]))
}

#[test]
fn decimal_output_parallel_finalizers_share_query_budget_and_cleanup() {
    let pool = MemoryPool::new_named("parallel decimal budget", 12000);
    let schema = output_schema();
    let first = raw_state(512, 0)
        .build_output_with_pool(&schema, &pool)
        .unwrap();
    let retained = first.slice(0, 1);
    drop(first);
    assert!(pool.used() >= 512 * 16);
    assert!(raw_state(512, 512)
        .build_output_with_pool(&schema, &pool)
        .is_err());
    drop(retained);
    assert_eq!(pool.used(), 0);
    let error = finalize_disjoint_states(
        vec![raw_state(512, 0), raw_state(512, 512)],
        &[AggregateFunction::Sum],
        &[DataType::Decimal128(38, 2)],
        &schema,
        None,
        &pool,
    )
    .unwrap_err();
    assert!(
        error.to_string().contains("parallel decimal budget"),
        "{error}"
    );
    assert_eq!(pool.used(), 0);
}

#[test]
fn decimal_output_overflow_is_not_hidden_by_discarding_having() {
    let mut state = raw_state(512, 0);
    state.raw_groups.get_mut(&0).unwrap()[0] = AccumulatorState::SumDecimal {
        coefficient: None,
        scale: 2,
        seen: true,
    };
    let pool = MemoryPool::new(65536);
    let predicate = Expr::Literal(ScalarValue::Boolean(false));
    assert!(merge_states_to_batches_filtered(
        vec![state],
        &[AggregateFunction::Sum],
        &[DataType::Decimal128(38, 2)],
        &output_schema(),
        Some(&predicate),
        &pool,
    )
    .is_err());
    assert_eq!(pool.used(), 0);
}

#[test]
fn decimal_output_raw_general_and_perfect_paths_match_integer_oracle() {
    for scale in [-3, 0, 4] {
        let ty = DataType::Decimal128(38, scale);
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("v", ty.clone(), true),
        ]));
        let functions = vec![AggregateFunction::Sum, AggregateFunction::Min];
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int64, true),
            Field::new("s", ty.clone(), true).with_metadata(std::collections::HashMap::from([(
                "query_engine.bound_column.name".into(),
                "s".into(),
            )])),
            Field::new("m", ty.clone(), true),
        ]));
        let pool = MemoryPool::new(1024 * 1024);
        for groups in [8, 513] {
            let mut state = AggregationState::new(functions.clone(), vec![ty.clone(), ty.clone()]);
            let mut expected: BTreeMap<Option<i64>, (Option<i128>, Option<i128>)> = BTreeMap::new();
            for repetition in 0..3 {
                let mut keys = Vec::new();
                let mut values = Vec::new();
                for i in 0..=groups {
                    let key = (i < groups).then_some(i as i64);
                    let value = (i % 17 != 0)
                        .then_some((i as i128 - 256) * 9_007_199_254_740_993i128 + repetition);
                    keys.push(key);
                    values.push(value);
                    let entry = expected.entry(key).or_default();
                    if let Some(value) = value {
                        entry.0 = Some(entry.0.unwrap_or(0) + value);
                        entry.1 = Some(entry.1.map_or(value, |old| old.min(value)));
                    }
                }
                let batch = RecordBatch::try_new(
                    input_schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(keys)),
                        Arc::new(
                            Decimal128Array::from(values)
                                .with_precision_and_scale(38, scale)
                                .unwrap(),
                        ),
                    ],
                )
                .unwrap();
                state
                    .process_batch(
                        &batch,
                        &[Expr::column("k")],
                        &[Expr::column("v"), Expr::column("v")],
                    )
                    .unwrap();
            }
            if groups == 513 {
                assert!(
                    !state.raw_groups.is_empty(),
                    "fixture must exercise raw path"
                );
            }
            let check = |batch: RecordBatch| {
                assert_eq!(batch.schema(), output_schema);
                let keys = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let sums = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap();
                let mins = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap();
                let actual: BTreeMap<_, _> =
                    keys.iter().zip(sums.iter().zip(mins.iter())).collect();
                assert_eq!(batch.num_rows(), expected.len());
                assert_eq!(actual, expected);
            };
            check(state.build_output_with_pool(&output_schema, &pool).unwrap());
            assert_eq!(pool.used(), 0);
            // Explicitly exercise general group output with the same actual ingest states.
            let groups = state.into_shards(1).unwrap().pop().unwrap();
            let map = merge_entries_into_map(vec![groups]);
            let general =
                AggregationState::from_groups(functions.clone(), vec![ty.clone(), ty.clone()], map);
            assert!(general.raw_groups.is_empty());
            check(
                general
                    .build_output_with_pool(&output_schema, &pool)
                    .unwrap(),
            );
            assert_eq!(pool.used(), 0);
        }
    }
}

#[test]
fn decimal_output_empty_global_and_grouped_keep_sql_cardinality() {
    let pool = MemoryPool::new(65536);
    let state = AggregationState::new(
        vec![AggregateFunction::Sum],
        vec![DataType::Decimal128(38, 2)],
    );
    let grouped = state
        .build_output_with_pool(&output_schema(), &pool)
        .unwrap();
    assert_eq!(grouped.num_rows(), 0);
    drop(grouped);
    assert_eq!(pool.used(), 0);
    let global_schema = Arc::new(Schema::new(vec![Field::new(
        "s",
        DataType::Decimal128(38, 2),
        true,
    )]));
    let global = state.build_output_with_pool(&global_schema, &pool).unwrap();
    assert_eq!(global.num_rows(), 1);
    assert!(global.column(0).is_null(0));
    drop(global);
    assert_eq!(pool.used(), 0);
}

#[test]
fn decimal_output_raw_shard_merge_preserves_values_and_explicit_pool() {
    let schema = output_schema();
    // Cross-state duplicate keys require actual merging; >65,536 thread-local
    // groups selects shared raw sharding instead of the single-state shortcut.
    let states = || vec![raw_state(40000, 0), raw_state(40000, 20000)];
    let pool = MemoryPool::new_named("raw shard output", 4096);
    let error = merge_states_to_batches(
        states(),
        &[AggregateFunction::Sum],
        &[DataType::Decimal128(38, 2)],
        &schema,
        &pool,
    )
    .unwrap_err();
    assert!(error.to_string().contains("raw shard output"), "{error}");
    assert_eq!(pool.used(), 0);
    // The old 4 MiB output-only budget now also owns merged state. Parallel
    // scheduling determines peak overlap: accept exact completion or a named,
    // clean refusal there. The 16 MiB case must complete the same value oracle.
    for limit in [4 * 1024 * 1024, 16 * 1024 * 1024] {
        let pool = MemoryPool::new_named("raw shard complete budget", limit);
        let batches = match merge_states_to_batches(
            states(),
            &[AggregateFunction::Sum],
            &[DataType::Decimal128(38, 2)],
            &schema,
            &pool,
        ) {
            Ok(batches) => batches,
            Err(error) => {
                assert_eq!(limit, 4 * 1024 * 1024, "{error}");
                assert!(
                    error.to_string().contains("raw shard complete budget"),
                    "{error}"
                );
                assert_eq!(pool.used(), 0);
                continue;
            }
        };
        let mut seen = std::collections::BTreeSet::new();
        for batch in &batches {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let sums = batch
                .column(1)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            for (key, sum) in keys.iter().zip(sums.iter()) {
                let key = key.unwrap();
                assert!(seen.insert(key));
                let copies = if (20000..40000).contains(&key) { 2 } else { 1 };
                assert_eq!(sum, Some(key as i128 * 100 * copies));
            }
        }
        assert_eq!(seen.len(), 60000);
        assert!(pool.used() >= 60000 * 16);
        drop(batches);
        assert_eq!(pool.used(), 0);
    }
}
