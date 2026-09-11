//! Physical batch variants must never be interpreted with another batch's stride.
use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array};
use arrow::record_batch::RecordBatch;
use futures::TryStreamExt;
use query_engine::{
    execution::create_memory_pool,
    physical::{HashJoinExec, MemoryTableExec, PhysicalOperator},
    planner::{Expr, JoinType},
    QueryError,
};
use std::sync::Arc;

async fn check_payload(variant: u8) {
    // Exercise the actual row-store admission threshold with a small fixed fixture.
    let rows = 50_000;
    let first = RecordBatch::try_from_iter([
        ("bk", Arc::new(Int64Array::from(vec![0; rows])) as ArrayRef),
        (
            "payload",
            Arc::new(Int64Array::from(vec![7; rows])) as ArrayRef,
        ),
    ])
    .unwrap();
    let payload: ArrayRef = match variant {
        0 => Arc::new(Int64Array::from(vec![-1; rows])),
        1 => Arc::new(Int32Array::from(vec![-1; rows])),
        2 => Arc::new(Float64Array::from(vec![-1.0; rows])),
        _ => unreachable!(),
    };
    let second = RecordBatch::try_from_iter([
        ("bk", Arc::new(Int64Array::from(vec![1; rows])) as ArrayRef),
        ("payload", payload),
    ])
    .unwrap();
    let probe =
        RecordBatch::try_from_iter([("pk", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap();
    let pool = create_memory_pool(32 << 20);
    let join = HashJoinExec::new(
        Arc::new(MemoryTableExec::new(
            "build",
            first.schema(),
            vec![first, second],
            None,
        )),
        Arc::new(MemoryTableExec::new(
            "probe",
            probe.schema(),
            vec![probe],
            None,
        )),
        vec![(Expr::column("bk"), Expr::column("pk"))],
        JoinType::Inner,
    )
    .with_memory_pool(pool.clone());
    let result: query_engine::Result<Vec<RecordBatch>> = async {
        let mut batches = Vec::new();
        for partition in 0..join.output_partitions() {
            batches.extend(
                join.execute(partition)
                    .await?
                    .try_collect::<Vec<_>>()
                    .await?,
            );
        }
        Ok(batches)
    }
    .await;
    match result {
        Err(error) if variant != 0 => assert!(
            matches!(error.root(), QueryError::Type(_) | QueryError::Arrow(_)),
            "unsupported physical variants must be explicit, not hidden by another failure: {error}"
        ),
        Err(error) => panic!("uniform supported layout must execute: {error}"),
        Ok(batches) => {
            let mut count = 0;
            for batch in batches {
                let values = batch.column(1);
                assert_eq!(values.null_count(), 0);
                for row in 0..values.len() {
                    let value = if let Some(a) = values.as_any().downcast_ref::<Int64Array>() {
                        a.value(row) as f64
                    } else if let Some(a) = values.as_any().downcast_ref::<Int32Array>() {
                        f64::from(a.value(row))
                    } else if let Some(a) = values.as_any().downcast_ref::<Float64Array>() {
                        a.value(row)
                    } else {
                        panic!("unexpected physical payload type {:?}", values.data_type());
                    };
                    assert_eq!(
                        value, -1.0,
                        "packed row must preserve the actual signed value"
                    );
                    count += 1;
                }
            }
            assert_eq!(count, rows, "duplicate multiplicity must be preserved");
        }
    }
    drop(join);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn row_store_rejects_or_correctly_handles_mixed_physical_widths() {
    check_payload(1).await;
}

#[tokio::test]
async fn row_store_uniform_signed_payload_and_duplicate_control() {
    check_payload(0).await;
}

#[tokio::test]
async fn row_store_rejects_or_correctly_handles_same_width_different_types() {
    check_payload(2).await;
}
