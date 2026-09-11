//! Independent expected values for dense grouping: NULL keys, unseen inputs,
//! real zero sums, count semantics, and a NULL slot next to a maximum key.
use arrow::{
    array::{Array, ArrayRef, Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use query_engine::{
    execution::MemoryPool,
    physical::{operators::hash_agg::AggregateExpr, MorselAggregateExec, PhysicalOperator},
    planner::{AggregateFunction, Expr},
};
use std::{collections::BTreeMap, sync::Arc};

#[tokio::test]
async fn dense_nullable_groups_and_sum_avg_validity_match_exact_expected_values() {
    for (key_type, min) in [
        (DataType::Int64, -1),
        (DataType::Int32, -1),
        (DataType::Date32, -1),
        (DataType::Int64, i64::MAX - 2),
    ] {
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("k", key_type.clone(), true),
            Field::new("f", DataType::Float64, true),
            Field::new("i", DataType::Int64, true),
        ]));
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![
            None,
            None,
            Some(min),
            Some(min),
            Some(min + 1),
            Some(min + 1),
            Some(min + 2),
            Some(min + 2),
        ]));
        let keys = arrow::compute::cast(&keys, &key_type).unwrap();
        let batch = RecordBatch::try_new(
            input_schema.clone(),
            vec![
                keys,
                Arc::new(Float64Array::from(vec![
                    None,
                    None,
                    None,
                    None,
                    Some(2.0),
                    None,
                    Some(0.0),
                    Some(0.0),
                ])),
                Arc::new(Int64Array::from(vec![
                    None,
                    None,
                    None,
                    None,
                    Some(7),
                    None,
                    Some(0),
                    Some(0),
                ])),
            ],
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("nullable.parquet");
        let properties = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(4))
            .build();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            input_schema.clone(),
            Some(properties),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let aggregates = [
            (AggregateFunction::Sum, Expr::column("f")),
            (AggregateFunction::Sum, Expr::column("i")),
            (AggregateFunction::Avg, Expr::column("f")),
            (AggregateFunction::Count, Expr::column("f")),
            (AggregateFunction::Count, Expr::Wildcard),
        ]
        .into_iter()
        .map(|(func, input)| AggregateExpr {
            func,
            input,
            distinct: false,
            second_arg: None,
        })
        .collect();
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("k", key_type.clone(), true),
            Field::new("sf", DataType::Float64, true),
            Field::new("si", DataType::Int64, true),
            Field::new("av", DataType::Float64, true),
            Field::new("n", DataType::Int64, false),
            Field::new("rows", DataType::Int64, false),
        ]));
        let pool = Arc::new(MemoryPool::new_named("dense NULL semantics", 1 << 20));
        let operator = MorselAggregateExec::new(
            vec![path],
            input_schema,
            None,
            None,
            vec![Expr::column("k")],
            aggregates,
            output_schema,
        )
        .with_memory_pool(pool.clone());
        let batches = operator
            .execute(0)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut actual = BTreeMap::new();
        for batch in &batches {
            let keys = arrow::compute::cast(batch.column(0), &DataType::Int64).unwrap();
            let keys = keys.as_any().downcast_ref::<Int64Array>().unwrap();
            let floats = |col| {
                batch
                    .column(col)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
            };
            let ints = |col| {
                batch
                    .column(col)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
            };
            for row in 0..batch.num_rows() {
                let f = |col| (!floats(col).is_null(row)).then(|| floats(col).value(row));
                let i = |col| (!ints(col).is_null(row)).then(|| ints(col).value(row));
                let key = (!keys.is_null(row)).then(|| keys.value(row));
                assert!(actual.insert(key, (f(1), i(2), f(3), i(4), i(5))).is_none());
            }
        }
        let expected = [
            (None, (None, None, None, Some(0), Some(2))),
            (Some(min), (None, None, None, Some(0), Some(2))),
            (
                Some(min + 1),
                (Some(2.0), Some(7), Some(2.0), Some(1), Some(2)),
            ),
            (
                Some(min + 2),
                (Some(0.0), Some(0), Some(0.0), Some(2), Some(2)),
            ),
        ]
        .into_iter()
        .collect();
        assert_eq!(actual, expected, "{key_type:?} minimum {min}");
        drop(batches);
        assert_eq!(pool.used(), 0);
    }
}
