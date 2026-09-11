use arrow::array::{Array, ArrayRef, Int64Array, Int8Array};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

fn context(limit: usize, rows: usize) -> ExecutionContext {
    let input = RecordBatch::try_from_iter(vec![(
        "i",
        Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
    )])
    .unwrap();
    let mut ctx = ExecutionContext::with_memory_limit(limit);
    ctx.register_table("t", input.schema(), vec![input]);
    ctx
}
#[tokio::test]
async fn numeric_and_decimal_coercions_refuse_before_allocating_over_budget_even_for_try_cast() {
    for expr in [
        "CAST(i AS DOUBLE)",
        "CAST(i AS DECIMAL(20,0))",
        "TRY_CAST(i AS DOUBLE)",
        "TRY_CAST(i AS DECIMAL(20,0))",
    ] {
        let ctx = context(65536, 16384);
        let error = ctx
            .sql(&format!("SELECT {expr} FROM t"))
            .await
            .unwrap_err()
            .to_string();
        assert!(
            error.to_lowercase().contains("memory") && error.contains("query"),
            "{expr}: {error}"
        );
        assert_eq!(ctx.memory_pool().used(), 0);
    }
}
#[tokio::test]
async fn returned_coercion_buffers_retain_query_reservations() {
    for expr in ["CAST(i AS DOUBLE)", "CAST(i AS DECIMAL(20,0))"] {
        let ctx = context(1024 * 1024, 16384);
        let pool = ctx.memory_pool().clone();
        let result = ctx.sql(&format!("SELECT {expr} FROM t")).await.unwrap();
        assert_eq!(result.row_count, 16384);
        let escaped = result.batches[0].column(0).to_data().buffers()[0].clone();
        drop(result);
        drop(ctx);
        assert!(pool.used() >= 16384 * 8);
        drop(escaped);
        assert_eq!(pool.used(), 0);
    }
}
#[tokio::test]
async fn strict_conversion_errors_and_try_nulls_preserve_non_null_values() {
    let input = RecordBatch::try_from_iter(vec![(
        "i",
        Arc::new(Int64Array::from(vec![
            Some(-129),
            Some(127),
            Some(128),
            None,
        ])) as ArrayRef,
    )])
    .unwrap();
    let mut ctx = ExecutionContext::with_memory_limit(65536);
    ctx.register_table("t", input.schema(), vec![input]);
    assert!(ctx.sql("SELECT CAST(i AS TINYINT) FROM t").await.is_err());
    assert_eq!(ctx.memory_pool().used(), 0);
    let result = ctx
        .sql("SELECT TRY_CAST(i AS TINYINT) FROM t")
        .await
        .unwrap();
    let values = result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int8Array>()
        .unwrap();
    assert_eq!(
        values.iter().collect::<Vec<_>>(),
        vec![None, Some(127), None, None]
    );
    drop(result);
    assert_eq!(ctx.memory_pool().used(), 0);
}
