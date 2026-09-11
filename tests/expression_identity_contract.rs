//! Numeric equality must not erase physical decimal result representation.
use arrow::array::{Array, Decimal128Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use query_engine::{ExecutionContext, QueryResult};
use std::collections::BTreeMap;
use std::sync::Arc;

fn context() -> ExecutionContext {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "g",
            Arc::new(StringArray::from(vec!["a", "a", "b"])) as Arc<dyn Array>,
        ),
        (
            "x",
            Arc::new(Int64Array::from(vec![Some(2), None, Some(-1)])) as Arc<dyn Array>,
        ),
    ])
    .unwrap();
    let mut context = ExecutionContext::new().with_parallel_partitions(2);
    context.register_table(
        "t",
        batch.schema(),
        vec![batch.slice(0, 2), batch.slice(2, 1)],
    );
    context
}

fn decimals(result: &QueryResult, column: usize, scale: i8) -> Vec<Option<i128>> {
    result
        .batches
        .iter()
        .flat_map(|batch| {
            let a = batch
                .column(column)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap();
            assert_eq!(a.precision(), 38);
            assert_eq!(a.scale(), scale);
            a.iter().collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn scalar_aggregate_outputs_preserve_distinct_decimal_literal_scales() {
    let result = context().sql("SELECT SUM(x * CAST('1.0' AS DECIMAL(38,1))) AS a, SUM(x * CAST('1.00' AS DECIMAL(38,2))) AS b FROM t").await.unwrap();
    assert_eq!(decimals(&result, 0, 1), vec![Some(10)]);
    assert_eq!(decimals(&result, 1, 2), vec![Some(100)]);
}

#[tokio::test]
async fn grouped_aggregate_outputs_preserve_distinct_decimal_literal_scales() {
    let result = context().sql("SELECT g, SUM(x * CAST('1.0' AS DECIMAL(38,1))) AS a, SUM(x * CAST('1.00' AS DECIMAL(38,2))) AS b FROM t GROUP BY g ORDER BY g").await.unwrap();
    assert_eq!(decimals(&result, 1, 1), vec![Some(20), Some(-10)]);
    assert_eq!(decimals(&result, 2, 2), vec![Some(200), Some(-100)]);
}

#[tokio::test]
async fn grouping_sets_preserve_distinct_decimal_literal_scales() {
    let result = context().sql("SELECT g, SUM(x * CAST('1.0' AS DECIMAL(38,1))) AS a, SUM(x * CAST('1.00' AS DECIMAL(38,2))) AS b FROM t GROUP BY GROUPING SETS ((g), ())").await.unwrap();
    let a = decimals(&result, 1, 1);
    let b = decimals(&result, 2, 2);
    assert_eq!(a.len(), 3);
    let keys: Vec<_> = result
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.map(str::to_owned))
                .collect::<Vec<_>>()
        })
        .collect();
    let rows: BTreeMap<_, _> = keys.into_iter().zip(a.into_iter().zip(b)).collect();
    assert_eq!(
        rows,
        BTreeMap::from([
            (None, (Some(10), Some(100))),
            (Some("a".into()), (Some(20), Some(200))),
            (Some("b".into()), (Some(-10), Some(-100))),
        ])
    );
}
