use arrow::array::{Array, Date32Array, Int64Array};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

fn context() -> ExecutionContext {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "id",
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as Arc<dyn Array>,
        ),
        (
            "d",
            Arc::new(Date32Array::from(vec![
                Some(9373),
                Some(9374),
                Some(9403),
                Some(9404),
                None,
            ])) as Arc<dyn Array>,
        ),
    ])
    .unwrap();
    let mut ctx = ExecutionContext::new().with_parallel_partitions(3);
    ctx.register_table(
        "t",
        batch.schema(),
        vec![batch.slice(0, 2), batch.slice(2, 3)],
    );
    ctx
}

#[tokio::test]
async fn literal_cast_and_nested_cast_ranges_have_identical_rows_and_normalized_plans() {
    let ctx = context();
    for predicate in [
        "d >= DATE '1995-09-01' AND d < DATE '1995-10-01'",
        "d >= CAST('1995-09-01' AS DATE) AND d < CAST('1995-10-01' AS DATE)",
        "d >= CAST(CAST('1995-09-01' AS VARCHAR) AS DATE) AND d < CAST('1995-10-01' AS DATE)",
        "d BETWEEN CAST('1995-09-01' AS DATE) AND CAST('1995-09-30' AS DATE)",
        "d IN (CAST('1995-09-01' AS DATE), CAST('1995-09-30' AS DATE))",
    ] {
        let result = ctx
            .sql(&format!("SELECT id FROM t WHERE {predicate} ORDER BY id"))
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
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(values, vec![2, 3], "{predicate}");
        assert!(
            !result
                .metrics
                .optimized_plan
                .as_ref()
                .unwrap()
                .contains("CAST("),
            "{predicate}: {:?}",
            result.metrics.optimized_plan
        );
    }
}

#[tokio::test]
async fn failing_and_null_casts_keep_execution_semantics() {
    let ctx = context();
    assert!(ctx
        .sql("SELECT CAST('bad' AS BIGINT) FROM t")
        .await
        .is_err());
    let empty = ctx
        .sql("SELECT CAST('bad' AS BIGINT) FROM t WHERE id < 0")
        .await
        .unwrap();
    assert_eq!(empty.row_count, 0);
    let result = ctx
        .sql("SELECT TRY_CAST('bad' AS BIGINT) AS v, CAST(NULL AS DATE) AS d FROM t")
        .await
        .unwrap();
    assert_eq!(result.row_count, 5);
    for b in result.batches {
        assert_eq!(b.column(0).data_type(), &arrow::datatypes::DataType::Int64);
        assert_eq!(b.column(1).data_type(), &arrow::datatypes::DataType::Date32);
        assert_eq!(b.column(0).null_count(), b.num_rows());
        assert_eq!(b.column(1).null_count(), b.num_rows());
    }
    let result = ctx
        .sql("SELECT CASE WHEN id > 0 THEN 7 ELSE CAST('bad' AS BIGINT) END AS v FROM t")
        .await
        .unwrap();
    assert_eq!(result.row_count, 5);
}

#[tokio::test]
async fn constant_cast_aggregate_and_order_references_keep_bound_identity() {
    let ctx = context();
    let result = ctx.sql("SELECT SUM(CAST('2' AS BIGINT)) AS s FROM t HAVING SUM(CAST('2' AS BIGINT)) > 0 ORDER BY s").await.unwrap();
    assert_eq!(
        result.batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        10
    );
    let result = ctx
        .sql("SELECT CAST('2' AS BIGINT) AS x FROM t ORDER BY CAST('2' AS BIGINT)")
        .await
        .unwrap();
    assert_eq!(result.row_count, 5);
}
