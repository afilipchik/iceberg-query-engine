//! Exact-value regressions for aggregate logical types and physical encodings.
use arrow::array::{Array, ArrayRef, Int16Array, Int32Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use query_engine::{ExecutionConfig, ExecutionContext};
use std::sync::Arc;

async fn aggregate(
    values: Vec<ArrayRef>,
    types: DataType,
    grouped: bool,
    distinct: bool,
) -> query_engine::Result<query_engine::QueryResult> {
    let fields = if grouped {
        vec![
            Field::new("g", DataType::Int16, true),
            Field::new("v", types, true),
        ]
    } else {
        vec![Field::new("v", types, true)]
    };
    let schema = Arc::new(Schema::new(fields));
    let batches = values
        .into_iter()
        .map(|value| {
            let arrays = if grouped {
                vec![
                    Arc::new(Int16Array::from(vec![Some(7); value.len()])) as ArrayRef,
                    value,
                ]
            } else {
                vec![value]
            };
            RecordBatch::try_new(schema.clone(), arrays).unwrap()
        })
        .collect();
    let mut context =
        ExecutionContext::with_config(ExecutionConfig::default().with_morsel_execution(false));
    context.register_table("t", schema, batches);
    let modifier = if distinct { "DISTINCT " } else { "" };
    let sql = format!(
        "SELECT {}MIN({modifier}v), MAX({modifier}v) FROM t{}",
        if grouped { "g, " } else { "" },
        if grouped { " GROUP BY g" } else { "" }
    );
    context.sql(&sql).await
}

#[tokio::test]
async fn extrema_int32_preserves_type_and_null_for_all_null_input() {
    for values in [vec![Some(9), None, Some(-7)], vec![None, None, None]] {
        let result = aggregate(
            vec![Arc::new(Int32Array::from(values.clone()))],
            DataType::Int32,
            false,
            false,
        )
        .await
        .unwrap();
        assert_eq!(result.schema.field(0).data_type(), &DataType::Int32);
        for (index, expected) in [values.iter().flatten().min(), values.iter().flatten().max()]
            .into_iter()
            .enumerate()
        {
            let array = result.batches[0]
                .column(index)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            assert_eq!(array.is_null(0), expected.is_none());
            if let Some(expected) = expected {
                assert_eq!(array.value(0), *expected);
            }
        }
    }
}

#[tokio::test]
async fn dictionary_extrema_use_values_across_different_codebooks() {
    let data_type = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let first = arrow::compute::cast(
        &StringArray::from(vec![Some("zeta"), None, Some("omega")]),
        &data_type,
    )
    .unwrap();
    let second = arrow::compute::cast(
        &StringArray::from(vec![Some("beta"), Some("alpha"), None]),
        &data_type,
    )
    .unwrap();
    for grouped in [false, true] {
        for distinct in [false, true] {
            let result = aggregate(
                vec![first.clone(), second.clone()],
                data_type.clone(),
                grouped,
                distinct,
            )
            .await
            .unwrap();
            let offset = usize::from(grouped);
            let min =
                arrow::compute::cast(result.batches[0].column(offset), &DataType::Utf8).unwrap();
            let max = arrow::compute::cast(result.batches[0].column(offset + 1), &DataType::Utf8)
                .unwrap();
            assert_eq!(
                min.as_any().downcast_ref::<StringArray>().unwrap().value(0),
                "alpha"
            );
            assert_eq!(
                max.as_any().downcast_ref::<StringArray>().unwrap().value(0),
                "zeta"
            );
        }
    }
}

#[tokio::test]
async fn extrema_unsigned_full_domain_and_small_group_keys_remain_exact() {
    let result = aggregate(
        vec![Arc::new(UInt64Array::from(vec![u64::MAX, u64::MAX - 1]))],
        DataType::UInt64,
        true,
        false,
    )
    .await
    .unwrap();
    assert_eq!(result.schema.field(0).data_type(), &DataType::Int16);
    let batch = &result.batches[0];
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        7
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        u64::MAX - 1
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        u64::MAX
    );
}

#[tokio::test]
async fn scalar_single_min_all_null_is_not_numeric_sentinel() {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![None, None]))],
    )
    .unwrap();
    let mut context = ExecutionContext::new();
    context.register_table("t", schema, vec![batch]);
    let result = context.sql("SELECT MIN(v) FROM t").await.unwrap();
    assert!(result.batches[0].column(0).is_null(0));
}

#[tokio::test]
async fn large_and_view_string_extrema_keep_logical_values() {
    for data_type in [DataType::LargeUtf8, DataType::Utf8View] {
        let input = arrow::compute::cast(
            &StringArray::from(vec![Some("zeta"), None, Some("alpha"), Some("alpha")]),
            &data_type,
        )
        .unwrap();
        let result = aggregate(vec![input], data_type, true, false)
            .await
            .unwrap();
        for (index, expected) in [(1, "alpha"), (2, "zeta")] {
            let array =
                arrow::compute::cast(result.batches[0].column(index), &DataType::Utf8).unwrap();
            assert_eq!(
                array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
    }
}

#[tokio::test]
async fn mixed_distinct_preserves_small_integer_average_and_full_unsigned_identity() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int16, true),
        Field::new("v", DataType::Int16, true),
        Field::new("id", DataType::UInt64, false),
    ]));
    let batches = (0..2)
        .map(|_| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int16Array::from(vec![Some(4), Some(4), Some(4)])),
                    Arc::new(Int16Array::from(vec![Some(100), None, Some(300)])),
                    Arc::new(UInt64Array::from(vec![u64::MAX, u64::MAX - 1, u64::MAX])),
                ],
            )
            .unwrap()
        })
        .collect();
    let mut context = ExecutionContext::new();
    context.register_table("t", schema, batches);
    let result = context
        .sql("SELECT g, MIN(v), MAX(v), AVG(v), COUNT(DISTINCT id) FROM t GROUP BY g")
        .await
        .unwrap();
    let batch = &result.batches[0];
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        4
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        100
    );
    assert_eq!(
        batch
            .column(2)
            .as_any()
            .downcast_ref::<Int16Array>()
            .unwrap()
            .value(0),
        300
    );
    assert_eq!(
        batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::Float64Array>()
            .unwrap()
            .value(0),
        200.0
    );
    assert_eq!(
        batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
}

#[tokio::test]
async fn temporal_groups_and_extrema_preserve_raw_counts_units_and_timezone() {
    use arrow::datatypes::TimeUnit;
    let mut types = vec![DataType::Date64];
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for timezone in [None, Some(Arc::<str>::from("America/Los_Angeles"))] {
            types.push(DataType::Timestamp(unit.clone(), timezone));
        }
    }
    for data_type in types {
        // More than 256 distinct groups force the raw/hash fallback. Adjacent
        // nanosecond counts must remain distinct, even across batch merges.
        let keys = (0..300)
            .map(|i| Some(1_000_000_000_001_i64 + i))
            .chain([None])
            .collect::<Vec<_>>();
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", data_type.clone(), true),
            Field::new("v", data_type.clone(), true),
            Field::new("id", DataType::Int64, false),
        ]));
        let batches = [-1, 1]
            .into_iter()
            .map(|delta| {
                let g = arrow::compute::cast(&Int64Array::from(keys.clone()), &data_type).unwrap();
                let v = arrow::compute::cast(
                    &Int64Array::from(
                        keys.iter()
                            .map(|key| key.map(|k| k + delta))
                            .collect::<Vec<_>>(),
                    ),
                    &data_type,
                )
                .unwrap();
                RecordBatch::try_new(
                    schema.clone(),
                    vec![g, v, Arc::new(Int64Array::from(vec![delta; keys.len()]))],
                )
                .unwrap()
            })
            .collect::<Vec<_>>();
        let mut context = ExecutionContext::new();
        context.register_table("t", schema, batches);
        for suffix in ["", ", COUNT(DISTINCT id)"] {
            let result = context
                .sql(&format!(
                    "SELECT g, MIN(v), MAX(v){suffix} FROM t GROUP BY g"
                ))
                .await
                .unwrap();
            let mut seen = std::collections::BTreeSet::new();
            let mut nulls = 0;
            for batch in &result.batches {
                for col in 0..3 {
                    assert_eq!(batch.column(col).data_type(), &data_type);
                }
                let arrays = (0..3)
                    .map(|i| arrow::compute::cast(batch.column(i), &DataType::Int64).unwrap())
                    .collect::<Vec<_>>();
                for row in 0..batch.num_rows() {
                    if arrays[0].is_null(row) {
                        nulls += 1;
                        assert!(arrays[1].is_null(row));
                        assert!(arrays[2].is_null(row));
                    } else {
                        let key = arrays[0]
                            .as_any()
                            .downcast_ref::<Int64Array>()
                            .unwrap()
                            .value(row);
                        assert!(seen.insert(key), "duplicate temporal group");
                        for (col, delta) in [(1, -1), (2, 1)] {
                            assert_eq!(
                                arrays[col]
                                    .as_any()
                                    .downcast_ref::<Int64Array>()
                                    .unwrap()
                                    .value(row),
                                key + delta
                            );
                        }
                    }
                }
            }
            assert_eq!(seen.len(), 300);
            assert_eq!(nulls, 1);
        }
    }
}

#[tokio::test]
async fn spilled_extrema_merge_matches_exact_typed_oracle() {
    let directory = tempfile::tempdir().unwrap();
    let dictionary = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int16, false),
        Field::new("s", dictionary.clone(), true),
        Field::new("u", DataType::UInt64, true),
        Field::new("v", DataType::Int16, true),
    ]));
    let batches = (0..20)
        .map(|batch| {
            let text = if batch % 2 == 0 { "zeta" } else { "alpha" };
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int16Array::from_iter_values(0..1000)),
                    arrow::compute::cast(
                        &StringArray::from(
                            (0..1000)
                                .map(|g| (g != 0).then_some(text))
                                .collect::<Vec<_>>(),
                        ),
                        &dictionary,
                    )
                    .unwrap(),
                    Arc::new(UInt64Array::from(
                        (0..1000)
                            .map(|g| (g != 0).then_some(u64::MAX - batch))
                            .collect::<Vec<_>>(),
                    )),
                    Arc::new(Int16Array::from(
                        (0..1000)
                            .map(|g| (g != 0).then_some(batch as i16 - 10))
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut context = ExecutionContext::with_config(
        ExecutionConfig::new()
            .with_memory_limit(256 * 1024)
            .with_spill_path(directory.path().join("spill")),
    );
    context.register_table("t", schema, batches);
    let result = context
        .sql("SELECT g, MIN(s), MAX(s), MIN(u), MAX(u), MIN(v), MAX(v) FROM t GROUP BY g")
        .await
        .unwrap();
    assert!(
        result
            .metrics
            .spill_metrics
            .as_ref()
            .expect("aggregate must spill")
            .bytes_spilled
            > 0
    );
    let mut seen = std::collections::BTreeSet::new();
    for batch in &result.batches {
        let min_s = arrow::compute::cast(batch.column(1), &DataType::Utf8).unwrap();
        let max_s = arrow::compute::cast(batch.column(2), &DataType::Utf8).unwrap();
        for row in 0..batch.num_rows() {
            let group = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row);
            assert!(seen.insert(group));
            if group == 0 {
                for column in 1..7 {
                    assert!(batch.column(column).is_null(row));
                }
                continue;
            }
            assert_eq!(
                min_s
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row),
                "alpha"
            );
            assert_eq!(
                max_s
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(row),
                "zeta"
            );
            assert_eq!(
                batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(row),
                u64::MAX - 19
            );
            assert_eq!(
                batch
                    .column(4)
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .unwrap()
                    .value(row),
                u64::MAX
            );
            assert_eq!(
                batch
                    .column(5)
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .unwrap()
                    .value(row),
                -10
            );
            assert_eq!(
                batch
                    .column(6)
                    .as_any()
                    .downcast_ref::<Int16Array>()
                    .unwrap()
                    .value(row),
                9
            );
        }
    }
    assert_eq!(seen, (0..1000).collect());
}
