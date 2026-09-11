//! The scanner's decode window must also permit bounded refinement work.
#![cfg(feature = "lance")]

use arrow::{
    array::{Array, BooleanArray, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::{RecordBatch, RecordBatchIterator},
};
use futures::TryStreamExt;
use lance::deps::datafusion::logical_expr::{col, create_udf, ColumnarValue, Volatility};
use lance::{dataset::scanner::MaterializationStyle, Dataset};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Condvar, Mutex,
};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refinement_batches_share_the_bounded_decode_window() {
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("refinement.lance");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let input: Vec<Option<i64>> = (0..8192)
        .map(|i| if i % 5 == 0 { None } else { Some(i % 17) })
        .collect();
    let expected: Vec<Option<i64>> = input
        .iter()
        .copied()
        .filter(|v| v.is_some_and(|v| v % 2 == 0))
        .collect();
    let batch =
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(input))]).unwrap();
    let ds = Dataset::write(
        RecordBatchIterator::new(vec![Ok(batch)], schema),
        uri.to_str().unwrap(),
        None,
    )
    .await
    .unwrap();
    let active = Arc::new(AtomicUsize::new(0));
    let peak = Arc::new(AtomicUsize::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let rendezvous = Arc::new((Mutex::new(false), Condvar::new()));
    let overlapped = Arc::new(AtomicUsize::new(0));
    let udf = create_udf(
        "refinement_concurrency_probe",
        vec![DataType::Int64],
        DataType::Boolean,
        Volatility::Volatile,
        {
            let (active, peak, calls, rendezvous, overlapped) = (
                active.clone(),
                peak.clone(),
                calls.clone(),
                rendezvous.clone(),
                overlapped.clone(),
            );
            Arc::new(move |args: &[ColumnarValue]| {
                let ColumnarValue::Array(array) = &args[0] else {
                    panic!("probe requires a batch");
                };
                let values = array.as_any().downcast_ref::<Int64Array>().unwrap();
                let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(current, Ordering::SeqCst);
                match calls.fetch_add(1, Ordering::SeqCst) {
                    0 => {
                        let (lock, wake) = &*rendezvous;
                        let (ready, _) = wake
                            .wait_timeout_while(
                                lock.lock().unwrap(),
                                Duration::from_secs(1),
                                |ready| !*ready,
                            )
                            .unwrap();
                        if *ready {
                            overlapped.store(1, Ordering::SeqCst);
                        }
                    }
                    1 => {
                        let (lock, wake) = &*rendezvous;
                        *lock.lock().unwrap() = true;
                        wake.notify_one();
                    }
                    _ => {}
                }
                let result = BooleanArray::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v % 2 == 0))
                        .collect::<Vec<_>>(),
                );
                active.fetch_sub(1, Ordering::SeqCst);
                Ok(ColumnarValue::Array(Arc::new(result)))
            })
        },
    );
    let mut scanner = ds.scan();
    scanner.project(&["value"]).unwrap();
    scanner.filter_expr(udf.call(vec![col("value")]));
    scanner.materialization_style(MaterializationStyle::AllEarly);
    scanner.batch_size(512);
    scanner.batch_readahead(4);
    scanner.scan_in_order(true);
    let batches: Vec<RecordBatch> = scanner
        .try_into_stream()
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    let actual: Vec<Option<i64>> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
        })
        .collect();
    assert_eq!(
        actual, expected,
        "filter must preserve duplicates, NULL semantics and order"
    );
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert!(calls.load(Ordering::SeqCst) >= 4);
    assert!(
        peak.load(Ordering::SeqCst) <= 4,
        "refinement escaped the configured window"
    );
    assert_eq!(
        overlapped.load(Ordering::SeqCst),
        1,
        "refinement is serialized despite a four-batch decode window"
    );
}
