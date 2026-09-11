use arrow::array::{Array, BooleanArray, Decimal128Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

#[tokio::test]
async fn explicit_cast_errors_while_try_cast_nulls_only_invalid_rows() {
    let batch = RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(StringArray::from(vec![Some("42"), Some("bad"), None])) as _,
    )])
    .unwrap();
    let mut ctx = ExecutionContext::new();
    ctx.register_table("casts", batch.schema(), vec![batch]);
    assert!(
        ctx.sql("SELECT CAST(v AS BIGINT) FROM casts")
            .await
            .is_err(),
        "CAST must not silently turn invalid input into NULL"
    );
    let result = ctx
        .sql("SELECT TRY_CAST(v AS BIGINT) FROM casts")
        .await
        .unwrap();
    let values: Vec<_> = result
        .batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
        })
        .collect();
    assert_eq!(values, vec![Some(42), None, None]);
}

#[tokio::test]
async fn mixed_integer_domains_remain_exact_in_comparisons_and_case() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("s", DataType::Int64, false),
        Field::new("u", DataType::UInt64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![-1, i64::MAX, 7])),
            Arc::new(UInt64Array::from(vec![u64::MAX, (i64::MAX as u64) + 1, 7])),
        ],
    )
    .unwrap();
    let mut ctx = ExecutionContext::new();
    ctx.register_table("ints", schema, vec![batch]);
    let result = ctx
        .sql("SELECT s < u, s = u, CASE WHEN s < 0 THEN u ELSE s END FROM ints")
        .await
        .unwrap();
    let less: Vec<_> = result
        .batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
        })
        .collect();
    assert_eq!(less, vec![Some(true), Some(true), Some(false)]);
    let equal: Vec<_> = result
        .batches
        .iter()
        .flat_map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
        })
        .collect();
    assert_eq!(equal, vec![Some(false), Some(false), Some(true)]);
    let values: Vec<_> = result
        .batches
        .iter()
        .flat_map(|b| {
            b.column(2)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .iter()
        })
        .collect();
    assert_eq!(
        values,
        vec![Some(u64::MAX as i128), Some(i64::MAX as i128), Some(7)]
    );
}

#[tokio::test]
async fn case_evaluates_only_selected_rows_and_preserves_simple_operand() {
    let batch = RecordBatch::try_from_iter(vec![(
        "v",
        Arc::new(StringArray::from(vec!["42", "bad", "7"])) as _,
    )])
    .unwrap();
    let mut ctx = ExecutionContext::new();
    ctx.register_table("casts", batch.schema(), vec![batch]);
    for sql in [
        "SELECT CASE WHEN v = 'bad' THEN 0 ELSE CAST(v AS BIGINT) END FROM casts",
        "SELECT CASE v WHEN 'bad' THEN 0 ELSE CAST(v AS BIGINT) END FROM casts",
        "SELECT CASE WHEN v <> 'bad' THEN CAST(v AS BIGINT) WHEN TRUE THEN 0 ELSE CAST('bad' AS BIGINT) END FROM casts",
    ] {
        let result = ctx.sql(sql).await.unwrap();
        let values: Vec<_> = result.batches.iter().flat_map(|b| b.column(0).as_any().downcast_ref::<Int64Array>().unwrap().iter()).collect();
        assert_eq!(values, vec![Some(42), Some(0), Some(7)], "{sql}");
    }
    assert!(ctx
        .sql("SELECT CASE WHEN TRUE THEN CAST('bad' AS BIGINT) ELSE 0 END")
        .await
        .is_err());
}

#[tokio::test]
async fn cast_policy_survives_aggregation_subqueries_and_overflow() {
    let ctx = ExecutionContext::new();
    for sql in [
        "SELECT CAST('9223372036854775808' AS BIGINT)",
        "SELECT CAST((SELECT 'bad') AS BIGINT)",
    ] {
        assert!(ctx.sql(sql).await.is_err(), "{sql}");
    }
    for sql in [
        "SELECT TRY_CAST('9223372036854775808' AS BIGINT)",
        "SELECT TRY_CAST((SELECT 'bad') AS BIGINT)",
        "SELECT TRY_CAST(MAX(v) AS BIGINT) FROM (SELECT 'bad' AS v) t",
    ] {
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(result.batches[0].column(0).data_type(), &DataType::Int64);
        assert!(result.batches[0].column(0).is_null(0), "{sql}");
    }
}
