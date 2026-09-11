//! Aggregates in ORDER BY reference scalar group outputs, including hidden ones.
use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use query_engine::{ExecutionContext, QueryResult};
use std::sync::Arc;

fn groups(result: &QueryResult) -> Vec<i64> {
    result
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect()
}

fn context(workers: usize) -> ExecutionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 1, 2, 2, 2, 3])),
            Arc::new(Int64Array::from(vec![
                Some(2),
                Some(3),
                Some(1),
                None,
                Some(6),
                Some(9),
            ])),
        ],
    )
    .unwrap();
    let mut ctx = ExecutionContext::new().with_parallel_partitions(workers);
    ctx.register_table(
        "t",
        schema,
        vec![batch.slice(0, 2), batch.slice(2, 2), batch.slice(4, 2)],
    );
    ctx
}

#[tokio::test]
async fn order_by_aggregate_reuses_outputs_and_preserves_aliases() {
    for workers in [1, 2, 4] {
        let ctx = context(workers);
        for sql in [
            "SELECT g, COUNT(*) FROM t GROUP BY g ORDER BY COUNT(*) DESC, g",
            "SELECT g, COUNT(*) AS n FROM t GROUP BY g ORDER BY COUNT(*) DESC, g",
            "SELECT g, COUNT(*) + 1 AS n FROM t GROUP BY g ORDER BY COUNT(*) + 1 DESC, g",
            "SELECT g, COUNT(*) AS n FROM t GROUP BY g ORDER BY n DESC, g",
            "SELECT g, COUNT(*) AS n FROM t GROUP BY g ORDER BY 2 DESC, g",
        ] {
            let result = ctx
                .sql(sql)
                .await
                .unwrap_or_else(|error| panic!("{sql}: {error}"));
            assert_eq!(groups(&result), vec![2, 1, 3], "{sql}; workers={workers}");
            assert_eq!(result.schema.fields().len(), 2, "hidden keys must not leak");
        }
    }
}

#[tokio::test]
async fn hidden_aggregate_sort_keys_survive_having_limit_and_offset() {
    let ctx = context(2);
    for (sql, expected) in [
        (
            "SELECT g FROM t GROUP BY g ORDER BY COUNT(*) DESC, g",
            vec![2, 1, 3],
        ),
        (
            "SELECT g FROM t GROUP BY g ORDER BY SUM(v) DESC, g",
            vec![3, 2, 1],
        ),
        (
            "SELECT g FROM t GROUP BY g HAVING SUM(v) > 5 ORDER BY COUNT(*) DESC, g",
            vec![2, 3],
        ),
        (
            "SELECT g FROM t GROUP BY g ORDER BY COUNT(*) DESC, g LIMIT 1 OFFSET 1",
            vec![1],
        ),
        (
            "SELECT g FROM t GROUP BY g ORDER BY CASE WHEN COUNT(*) > 2 THEN 0 ELSE 1 END, g",
            vec![2, 1, 3],
        ),
    ] {
        let result = ctx
            .sql(sql)
            .await
            .unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(groups(&result), expected, "{sql}");
        assert_eq!(result.schema.fields().len(), 1, "{sql}");
    }
}

#[tokio::test]
async fn grouped_empty_input_retains_empty_result_with_aggregate_order() {
    let ctx = context(1);
    let result = ctx
        .sql("SELECT g, COUNT(*) FROM t WHERE g < 0 GROUP BY g ORDER BY COUNT(*) DESC")
        .await
        .unwrap();
    assert!(groups(&result).is_empty());
}

#[tokio::test]
async fn grouped_expression_sort_uses_group_output_before_alias_projection() {
    let ctx = context(2);
    for (sql, expected) in [
        ("SELECT g + 10 AS bucket, COUNT(*) FROM t GROUP BY g + 10 ORDER BY g + 10", vec![11, 12, 13]),
        ("SELECT g + 10 AS bucket, COUNT(*) FROM t GROUP BY g + 10 HAVING COUNT(*) > 1 ORDER BY g + 10 LIMIT 1 OFFSET 1", vec![12]),
        ("SELECT g + 10 AS bucket, COUNT(*) FROM t GROUP BY g + 10 HAVING COUNT(*) > 10 ORDER BY g + 10 LIMIT 1 OFFSET 1", vec![]),
        ("SELECT g + 10 AS bucket, COUNT(*) FROM t WHERE g < 0 GROUP BY g + 10 ORDER BY g + 10", vec![]),
        ("SELECT g + 10 AS bucket, COUNT(*) FROM t GROUP BY g + 10 ORDER BY bucket DESC LIMIT 1 OFFSET 1", vec![12]),
    ] {
        let result = ctx.sql(sql).await.unwrap_or_else(|error| panic!("{sql}: {error}"));
        assert_eq!(groups(&result), expected, "{sql}");
        assert_eq!(result.schema.fields().len(), 2, "group-sort keys must be trimmed");
    }
}

#[tokio::test]
async fn grouped_scalar_function_sort_reuses_temporal_group_key() {
    use arrow::array::{Array, TimestampMicrosecondArray};
    use arrow::datatypes::TimeUnit;
    let schema = Arc::new(Schema::new(vec![Field::new(
        "event_time",
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(TimestampMicrosecondArray::from(vec![
            Some(61_000_000),
            Some(125_000_000),
            Some(181_000_000),
            Some(126_000_000),
            None,
        ]))],
    )
    .unwrap();
    let mut ctx = ExecutionContext::new();
    ctx.register_table("events", schema, vec![batch.slice(0, 2), batch.slice(2, 3)]);
    let result = ctx.sql("SELECT DATE_TRUNC('minute', event_time) AS minute_bucket, COUNT(*) AS n FROM events GROUP BY DATE_TRUNC('minute', event_time) ORDER BY DATE_TRUNC('minute', event_time) LIMIT 2 OFFSET 1").await.unwrap();
    let mut values = vec![];
    for batch in result.batches {
        let array = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        for row in 0..array.len() {
            values.push((!array.is_null(row)).then(|| array.value(row)));
        }
    }
    assert_eq!(values, vec![Some(120_000_000), Some(180_000_000)]);
}
