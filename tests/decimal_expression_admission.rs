use arrow::{
    array::{Array, ArrayRef, Decimal128Array},
    datatypes::DataType,
    record_batch::RecordBatch,
};
use query_engine::ExecutionContext;
use std::sync::Arc;
fn column(values: Vec<Option<i128>>, precision: u8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, 2)
            .unwrap(),
    )
}
#[tokio::test]
async fn nested_decimal_projection_preserves_duplicates_nulls_and_retained_slices() {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "price",
            column(vec![Some(10000), Some(10000), None, Some(-10000)], 10),
        ),
        (
            "discount",
            column(vec![Some(10), Some(10), Some(0), Some(10)], 4),
        ),
        ("cost", column(vec![Some(200); 4], 10)),
        ("quantity", column(vec![Some(300); 4], 10)),
    ])
    .unwrap();
    let mut ctx = ExecutionContext::with_memory_limit(4 * 1024 * 1024);
    ctx.register_table(
        "t",
        batch.schema(),
        vec![batch.slice(0, 0), batch.clone(), batch],
    );
    let pool = ctx.memory_pool().clone();
    let result = ctx
        .sql("SELECT price*(1-discount)-cost*quantity AS amount FROM t")
        .await
        .unwrap();
    let mut actual = vec![];
    for batch in &result.batches {
        assert_eq!(batch.column(0).data_type(), &DataType::Decimal128(34, 4));
        actual.extend(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .iter(),
        );
    }
    actual.sort();
    let mut expected = vec![
        Some(840000),
        Some(840000),
        None,
        Some(-960000),
        Some(840000),
        Some(840000),
        None,
        Some(-960000),
    ];
    expected.sort();
    assert_eq!(actual, expected);
    let escaped = result
        .batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .unwrap()
        .column(0)
        .slice(0, 1)
        .to_data()
        .buffers()[0]
        .clone();
    drop(result);
    drop(ctx);
    assert!(pool.used() > 0);
    drop(escaped);
    assert_eq!(pool.used(), 0);
}
#[tokio::test]
async fn decimal_expression_pressure_and_late_overflow_release_the_query() {
    for overflow in [false, true] {
        let values = if overflow {
            vec![Some(1), Some(10i128.pow(38) - 1)]
        } else {
            vec![Some(1); 16384]
        };
        let array: ArrayRef = Arc::new(
            Decimal128Array::from(values)
                .with_precision_and_scale(38, 0)
                .unwrap(),
        );
        let batch = RecordBatch::try_from_iter(vec![("v", array)]).unwrap();
        let mut ctx =
            ExecutionContext::with_memory_limit(if overflow { 1024 * 1024 } else { 64 * 1024 });
        ctx.register_table("t", batch.schema(), vec![batch]);
        let error = ctx.sql("SELECT v + v AS x FROM t").await.unwrap_err();
        assert_eq!(error.is_memory_limit(), !overflow, "{error}");
        assert_eq!(ctx.memory_pool().used(), 0);
    }
}
