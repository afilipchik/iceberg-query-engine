use super::*;
use arrow::array::Decimal128Array;
use arrow::datatypes::{Field, Schema};
use std::collections::BTreeMap;

#[derive(Debug, Default, PartialEq)]
struct Expected {
    count: i64,
    coefficient: Option<i128>,
    avg_sum: f64,
    avg_count: i64,
}

fn key(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Null => None,
        ScalarValue::Int64(v) => Some(*v),
        other => panic!("unexpected group: {other:?}"),
    }
}

fn snapshot(state: &AggregationState) -> BTreeMap<Option<i64>, Expected> {
    let mut result = BTreeMap::<Option<i64>, Expected>::new();
    let mut add = |key, accs: &[AccumulatorState]| {
        let entry = result.entry(key).or_default();
        let [AccumulatorState::Count(count), AccumulatorState::SumDecimal {
            coefficient: Some(sum),
            scale: 2,
            seen,
        }, AccumulatorState::Avg {
            sum: avg_sum,
            count: avg_count,
        }] = accs
        else {
            panic!("unexpected states {accs:?}")
        };
        entry.count += count;
        if *seen {
            entry.coefficient = Some(entry.coefficient.unwrap_or(0) + sum);
        }
        entry.avg_sum += avg_sum;
        entry.avg_count += avg_count;
    };
    for (raw, values) in state.raw_groups.iter() {
        add(Some(*raw as i64), values);
    }
    if let Some(values) = &state.raw_null {
        add(None, values);
    }
    for (group, values) in &state.groups {
        add(key(&group.values[0]), values);
    }
    for (i, values) in state.perfect_accs.iter().enumerate() {
        if state.perfect_occupied.get(i) == Some(&true) {
            add(key(&state.key_order[i].values[0]), values);
        }
    }
    assert!(state.raw_sums.is_empty());
    result
}

fn batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, true),
        Field::new("one", DataType::Int64, false),
        Field::new("d", DataType::Decimal128(38, 2), true),
        Field::new("v", DataType::Float64, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                (0..1024)
                    .map(|i| ((i + 1) % 19 != 0).then_some(i % 257))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(vec![1; 1024])),
            Arc::new(
                Decimal128Array::from(
                    (0..1024)
                        .map(|i| (i % 7 != 0).then_some((1i128 << 80) + i))
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(38, 2)
                .unwrap(),
            ),
            Arc::new(Float64Array::from(
                (0..1024)
                    .map(|i| (i % 5 != 0).then_some((i % 17) as f64))
                    .collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

fn oracle(end: usize) -> BTreeMap<Option<i64>, Expected> {
    let mut result = BTreeMap::<Option<i64>, Expected>::new();
    for i in 0..end {
        let key = ((i + 1) % 19 != 0).then_some((i % 257) as i64);
        let e = result.entry(key).or_default();
        e.count += 1;
        if i % 7 != 0 {
            e.coefficient = Some(e.coefficient.unwrap_or(0) + (1i128 << 80) + i as i128);
        }
        if i % 5 != 0 {
            e.avg_sum += (i % 17) as f64;
            e.avg_count += 1;
        }
    }
    result
}

fn check(raw: bool, headroom: usize) {
    let pool = MemoryPool::new_named("cursor query", 4 << 20);
    let prepared = batch();
    let mut state = AggregationState::new_with_pool(
        vec![
            AggregateFunction::Count,
            AggregateFunction::Sum,
            AggregateFunction::Avg,
        ],
        vec![
            DataType::Int64,
            DataType::Decimal128(38, 2),
            DataType::Float64,
        ],
        &pool,
    );
    if raw {
        state.overflowed = true;
        state.raw_type = Some(DataType::Int64);
    }
    let blocker = pool.allocate(pool.available() - headroom).unwrap();
    let failure = state.process_evaluated_from(&prepared, 1, 0).unwrap_err();
    assert!(failure.error.is_memory_limit(), "{:?}", failure);
    assert!(failure.next_row < prepared.num_rows());
    if raw && headroom == 0 {
        assert_eq!(failure.next_row, 0);
    } else {
        assert!(failure.next_row > 0, "fixture must apply a nonempty prefix");
    }
    assert_eq!(snapshot(&state), oracle(failure.next_row));
    // Repeating the denied admission cannot advance the logical cursor or lose
    // values, even if the previous attempt moved some generic state to raw.
    let again = state
        .process_evaluated_from(&prepared, 1, failure.next_row)
        .unwrap_err();
    assert!(again.error.is_memory_limit());
    assert_eq!(again.next_row, failure.next_row);
    assert_eq!(snapshot(&state), oracle(failure.next_row));
    drop(blocker);
    assert_eq!(
        state
            .process_evaluated_from(&prepared, 1, failure.next_row)
            .unwrap(),
        1024
    );
    assert_eq!(snapshot(&state), oracle(1024));
    assert_eq!(
        state.process_evaluated_from(&prepared, 1, 1024).unwrap(),
        1024
    );
    assert_eq!(snapshot(&state), oracle(1024));
    let invalid = state
        .process_evaluated_from(&prepared, 1, 1025)
        .unwrap_err();
    assert!(!invalid.error.is_memory_limit());
    assert_eq!(snapshot(&state), oracle(1024));
    drop(state);
    assert_eq!(pool.used(), 0);
}

#[test]
fn raw_admission_returns_exact_cursor_before_first_row_and_inside_batch() {
    check(true, 0);
    check(true, 8192);
}

#[test]
fn perfect_to_raw_migration_resumes_without_replaying_applied_rows() {
    check(false, 8192);
}

#[test]
fn resumed_offsets_cover_perfect_dictionary_generic_and_global_updates() {
    use arrow::array::{DictionaryArray, StringArray};
    use arrow::datatypes::Int32Type;
    for case in 0..4 {
        let pool = MemoryPool::new(4 << 20);
        let mut columns: Vec<ArrayRef> = vec![];
        let mut output_fields = vec![];
        match case {
            0 => {
                columns.push(Arc::new(Int64Array::from(
                    (0..512).map(|i| i % 3).collect::<Vec<_>>(),
                )));
                output_fields.push(Field::new("g", DataType::Int64, false));
            }
            1 => {
                let dictionary: DictionaryArray<Int32Type> =
                    (0..512).map(|i| Some(["a", "b", "c"][i % 3])).collect();
                columns.push(Arc::new(dictionary));
                columns.push(Arc::new(Int64Array::from(
                    (0..512).map(|i| i % 2).collect::<Vec<_>>(),
                )));
                output_fields.push(Field::new("g", DataType::Utf8, false));
                output_fields.push(Field::new("h", DataType::Int64, false));
            }
            2 => {
                columns.push(Arc::new(StringArray::from(
                    (0..512).map(|i| ["a", "b", "c"][i % 3]).collect::<Vec<_>>(),
                )));
                output_fields.push(Field::new("g", DataType::Utf8, false));
            }
            _ => {}
        }
        let groups = columns.len();
        columns.push(Arc::new(Float64Array::from(vec![1.0; 512])));
        columns.push(Arc::new(Float64Array::from(
            (0..512).map(|i| i as f64).collect::<Vec<_>>(),
        )));
        let schema = Arc::new(Schema::new(
            columns
                .iter()
                .enumerate()
                .map(|(i, c)| Field::new(format!("c{i}"), c.data_type().clone(), false))
                .collect::<Vec<_>>(),
        ));
        let prepared = RecordBatch::try_new(schema, columns).unwrap();
        let mut state = AggregationState::new_with_pool(
            vec![AggregateFunction::Count, AggregateFunction::Sum],
            vec![DataType::Float64, DataType::Float64],
            &pool,
        );
        state.overflowed = case == 2;
        assert_eq!(
            state
                .process_evaluated_from(&prepared.slice(0, 37), groups, 0)
                .unwrap(),
            37
        );
        assert_eq!(
            state.process_evaluated_from(&prepared, groups, 37).unwrap(),
            512
        );
        output_fields.push(Field::new("count", DataType::Int64, false));
        output_fields.push(Field::new("sum", DataType::Float64, false));
        let output = state
            .build_output_with_pool(&Arc::new(Schema::new(output_fields)), &pool)
            .unwrap();
        let mut expected = BTreeMap::<i64, (i64, f64)>::new();
        for i in 0..512 {
            let key = match case {
                0 | 2 => i % 3,
                1 => (i % 3) * 2 + i % 2,
                _ => 0,
            };
            let entry = expected.entry(key).or_default();
            entry.0 += 1;
            entry.1 += i as f64;
        }
        assert_eq!(output.num_rows(), expected.len());
        let counts = output
            .column(groups)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let sums = output
            .column(groups + 1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for row in 0..output.num_rows() {
            let key = match case {
                0 => output
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .value(row),
                1 | 2 => {
                    let value = output
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .value(row);
                    let k = ["a", "b", "c"].iter().position(|s| *s == value).unwrap() as i64;
                    if case == 1 {
                        k * 2
                            + output
                                .column(1)
                                .as_any()
                                .downcast_ref::<Int64Array>()
                                .unwrap()
                                .value(row)
                    } else {
                        k
                    }
                }
                _ => 0,
            };
            assert_eq!(
                expected.remove(&key).unwrap(),
                (counts.value(row), sums.value(row)),
                "case {case}, group {key}"
            );
        }
        assert!(expected.is_empty());
        drop(output);
        drop(state);
        assert_eq!(pool.used(), 0);
    }
}
