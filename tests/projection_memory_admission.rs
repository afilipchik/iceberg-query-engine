use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray, UInt64Array};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

fn context(limit: usize, rows: usize, batches: usize) -> ExecutionContext {
    let input = RecordBatch::try_from_iter(vec![(
        "i",
        Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
    )])
    .unwrap();
    let mut ctx = ExecutionContext::with_memory_limit(limit);
    ctx.register_table("t", input.schema(), vec![input; batches]);
    ctx
}
#[tokio::test]
async fn computed_fixed_and_variable_outputs_refuse_the_reproduced_budget_violation() {
    for sql in [
        "SELECT i + 1 AS v FROM t",
        "SELECT CAST(i AS VARCHAR) AS v FROM t",
    ] {
        let ctx = context(64 * 1024, 16384, 1);
        let error = ctx.sql(sql).await.unwrap_err().to_string();
        assert!(
            error.to_lowercase().contains("memory") && error.contains("query"),
            "{sql}: {error}"
        );
        assert_eq!(
            ctx.memory_pool().used(),
            0,
            "error must release all reservations"
        );
    }
}
#[tokio::test]
async fn returned_arrays_keep_their_query_charges_after_result_and_context_drop() {
    let ctx = context(1024 * 1024, 16384, 1);
    let pool = ctx.memory_pool().clone();
    let result = ctx
        .sql("SELECT i + 1 AS v, CAST(i AS VARCHAR) AS s FROM t")
        .await
        .unwrap();
    assert_eq!(result.row_count, 16384);
    assert!(result.metrics.reserved_peak_memory_bytes >= 131072);
    let values = result.batches[0].column(0).clone();
    let strings = result.batches[0].column(1).clone();
    assert_eq!(
        values
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(16383),
        16384
    );
    assert_eq!(
        strings
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(16383),
        "16383"
    );
    drop(result);
    drop(ctx);
    assert!(pool.used() >= 131072);
    let buffer = values.to_data().buffers()[0].clone();
    drop(values);
    drop(strings);
    assert!(pool.used() >= 131072);
    drop(buffer);
    assert_eq!(pool.used(), 0);
}
#[tokio::test]
async fn collected_batches_share_one_budget_and_error_releases_them() {
    let ctx = context(64 * 1024, 1024, 32);
    assert!(ctx.sql("SELECT i + 1 FROM t").await.is_err());
    assert_eq!(ctx.memory_pool().used(), 0);
}
#[tokio::test]
async fn nullable_extremes_and_arithmetic_errors_preserve_semantics_and_release() {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "s",
            Arc::new(Int64Array::from(vec![Some(i64::MIN), None, Some(i64::MAX)])) as ArrayRef,
        ),
        (
            "u",
            Arc::new(UInt64Array::from(vec![Some(0), None, Some(u64::MAX)])) as ArrayRef,
        ),
        (
            "f",
            Arc::new(Float64Array::from(vec![Some(-0.0), None, Some(2.5)])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut ctx = ExecutionContext::with_memory_limit(1024 * 1024);
    ctx.register_table("t", batch.schema(), vec![batch]);
    let result = ctx
        .sql("SELECT CAST(s AS VARCHAR), CAST(u AS VARCHAR), f + f FROM t")
        .await
        .unwrap();
    let b = &result.batches[0];
    assert_eq!(
        b.column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![
            Some("-9223372036854775808"),
            None,
            Some("9223372036854775807")
        ]
    );
    assert_eq!(
        b.column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>(),
        vec![Some("0"), None, Some("18446744073709551615")]
    );
    let f = b.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
    assert_eq!(f.value(0).to_bits(), (-0.0_f64).to_bits());
    assert!(f.is_null(1));
    assert_eq!(f.value(2), 5.0);
    drop(result);
    assert_eq!(ctx.memory_pool().used(), 0);
    assert!(ctx.sql("SELECT s + 1 FROM t").await.is_err());
    assert_eq!(ctx.memory_pool().used(), 0);
    assert!(ctx.sql("SELECT s / 0 FROM t").await.is_err());
    assert_eq!(ctx.memory_pool().used(), 0);
}

#[tokio::test]
async fn every_integer_width_uses_the_same_exact_admitted_contract() {
    use arrow::array::*;
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(Int8Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(Int16Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(Int32Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(Int64Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(UInt8Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(UInt16Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(UInt32Array::from(vec![Some(1), None, Some(2)])),
        Arc::new(UInt64Array::from(vec![Some(1), None, Some(2)])),
    ];
    for array in arrays {
        let datatype = array.data_type().clone();
        let batch = RecordBatch::try_from_iter(vec![("a", array)]).unwrap();
        let mut ctx = ExecutionContext::with_memory_limit(1024 * 1024);
        ctx.register_table("t", batch.schema(), vec![batch]);
        let result = ctx
            .sql("SELECT a + a, CAST(a AS VARCHAR) FROM t")
            .await
            .unwrap();
        let b = &result.batches[0];
        assert_eq!(b.column(0).data_type(), &datatype);
        let values = arrow::compute::cast(b.column(0), &arrow::datatypes::DataType::Int64).unwrap();
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(2), None, Some(4)]
        );
        assert_eq!(
            b.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("1"), None, Some("2")]
        );
        assert!(ctx.memory_pool().used() > 0);
        drop(values);
        drop(result);
        assert_eq!(ctx.memory_pool().used(), 0);
    }
}

#[tokio::test]
async fn non_null_float_kernels_preserve_ieee_special_values_for_both_widths() {
    use arrow::array::Float32Array;
    for wide in [false, true] {
        let a = [-0.0, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 1.0, 0.0];
        let b = [1.0, 1.0, 1.0, 1.0, 0.0, 0.0];
        let arrays: Vec<ArrayRef> = if wide {
            vec![
                Arc::new(Float64Array::from(a.to_vec())),
                Arc::new(Float64Array::from(b.to_vec())),
            ]
        } else {
            vec![
                Arc::new(Float32Array::from(a.map(|v| v as f32).to_vec())),
                Arc::new(Float32Array::from(b.map(|v| v as f32).to_vec())),
            ]
        };
        let batch =
            RecordBatch::try_from_iter(vec![("a", arrays[0].clone()), ("b", arrays[1].clone())])
                .unwrap();
        let mut ctx = ExecutionContext::with_memory_limit(1024 * 1024);
        ctx.register_table("t", batch.schema(), vec![batch]);
        let result = ctx
            .sql("SELECT a * b AS product, a / b AS quotient FROM t")
            .await
            .unwrap();
        let b = &result.batches[0];
        let value = |col: usize, row: usize| {
            if wide {
                b.column(col)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row)
            } else {
                b.column(col)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .unwrap()
                    .value(row) as f64
            }
        };
        assert_eq!(value(0, 0).to_bits(), (-0.0_f64).to_bits());
        assert!(value(0, 1).is_nan());
        assert_eq!(value(0, 2), f64::INFINITY);
        assert_eq!(value(0, 3), f64::NEG_INFINITY);
        assert_eq!(value(1, 4), f64::INFINITY);
        assert!(value(1, 5).is_nan());
        assert!(ctx.memory_pool().used() > 0);
        drop(result);
        assert_eq!(ctx.memory_pool().used(), 0);
    }
}

#[tokio::test]
async fn expanded_literals_refuse_before_payload_allocation_and_keep_escaped_leases() {
    for sql in ["SELECT 1 AS v FROM t", "SELECT 'é🦀' AS v FROM t"] {
        let ctx = context(64 * 1024, 16384, 1);
        let error = ctx.sql(sql).await.unwrap_err().to_string();
        assert!(
            error.to_lowercase().contains("memory") && error.contains("query"),
            "{sql}: {error}"
        );
        assert_eq!(ctx.memory_pool().used(), 0);
    }
    let ctx = context(1024 * 1024, 16384, 1);
    let pool = ctx.memory_pool().clone();
    let result = ctx.sql("SELECT 7 AS v FROM t").await.unwrap();
    assert_eq!(result.row_count, 16384);
    let escaped = result.batches[0].column(0).to_data().buffers()[0].clone();
    drop(result);
    drop(ctx);
    assert!(pool.used() >= 16384 * 8);
    drop(escaped);
    assert_eq!(pool.used(), 0);
}
