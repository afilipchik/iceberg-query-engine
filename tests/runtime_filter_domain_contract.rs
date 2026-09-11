//! Runtime key filtering must preserve the full SQL Int64 domain.
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use futures::TryStreamExt;
use query_engine::{
    physical::{HashJoinExec, MemoryTableExec, PhysicalOperator},
    planner::{Expr, JoinType},
};
use std::sync::Arc;

async fn check_domain(keys: Vec<Option<i64>>) {
    let make = |name: &str, values: Vec<Option<i64>>| {
        let schema = Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, true)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(values))]).unwrap()
    };
    let split = keys.len() / 2;
    let left = vec![
        make("bk", keys[..split].to_vec()),
        make("bk", keys[split..].to_vec()),
    ];
    let probe = vec![
        Some(i64::MIN),
        Some(i64::MIN + 1),
        Some(-1),
        Some(0),
        Some(1),
        Some(i64::MAX - 1),
        Some(i64::MAX),
        None,
    ];
    let right = make("pk", probe.clone());
    let slot = Arc::new(parking_lot::Mutex::new(None));
    let mut join = HashJoinExec::new(
        Arc::new(MemoryTableExec::new("b", left[0].schema(), left, None)),
        Arc::new(MemoryTableExec::new("p", right.schema(), vec![right], None)),
        vec![(Expr::column("bk"), Expr::column("pk"))],
        JoinType::Inner,
    );
    join.probe_runtime_filter = Some(slot.clone());
    let mut actual = Vec::new();
    for partition in 0..join.output_partitions() {
        for batch in join
            .execute(partition)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
        {
            let a = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let b = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(!a.is_null(row) && !b.is_null(row));
                actual.push((a.value(row), b.value(row)));
            }
        }
    }
    let mut expected = Vec::new();
    for a in &keys {
        for b in &probe {
            if let (Some(a), Some(b)) = (a, b) {
                if a == b {
                    expected.push((*a, *b));
                }
            }
        }
    }
    actual.sort_unstable();
    expected.sort_unstable();
    assert_eq!(actual, expected);
    let payload = slot.lock();
    let payload = payload
        .as_ref()
        .expect("small nonempty key set publishes a filter");
    for value in probe.into_iter().flatten() {
        assert_eq!(
            payload.contains(value),
            keys.contains(&Some(value)),
            "key {value}"
        );
    }
}

#[tokio::test]
async fn runtime_filter_full_signed_domain() {
    check_domain(vec![
        Some(i64::MIN),
        Some(i64::MAX),
        Some(i64::MIN),
        None,
        Some(0),
    ])
    .await;
}
#[tokio::test]
async fn runtime_filter_bitmap_at_signed_boundaries() {
    check_domain(vec![
        Some(i64::MIN),
        None,
        Some(i64::MIN + 1),
        Some(i64::MIN),
    ])
    .await;
    check_domain(vec![
        Some(i64::MAX - 1),
        None,
        Some(i64::MAX),
        Some(i64::MAX),
    ])
    .await;
}

#[tokio::test]
async fn published_filter_retains_query_admission_after_join_is_dropped() {
    use query_engine::execution::MemoryPool;
    for keys in [vec![1, 3], (0..4000).map(|i| i * 262144).collect()] {
        let batch = |name: &str, values: Vec<i64>| {
            RecordBatch::try_from_iter([(
                name,
                Arc::new(Int64Array::from(values)) as arrow::array::ArrayRef,
            )])
            .unwrap()
        };
        let pool = Arc::new(MemoryPool::new(4 * 1024 * 1024));
        let left = batch("bk", keys.clone());
        let right = batch("pk", vec![keys[0], *keys.last().unwrap(), -1]);
        let slot = Arc::new(parking_lot::Mutex::new(None));
        let mut join = HashJoinExec::new(
            Arc::new(MemoryTableExec::new("b", left.schema(), vec![left], None)),
            Arc::new(MemoryTableExec::new("p", right.schema(), vec![right], None)),
            vec![(Expr::column("bk"), Expr::column("pk"))],
            JoinType::Inner,
        )
        .with_memory_pool(pool.clone());
        join.probe_runtime_filter = Some(slot.clone());
        let outputs = join
            .execute(0)
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(outputs.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
        let payload = slot.lock().as_ref().unwrap().clone();
        assert!(payload.contains(keys[0]));
        assert!(payload.contains(*keys.last().unwrap()));
        assert!(!payload.contains(-1));
        drop(outputs);
        drop(join);
        drop(slot);
        assert!(
            pool.used() > 0,
            "live published filter has no query reservation"
        );
        assert!(
            pool.used() < 256 * 1024,
            "sparse keys must not retain a domain-sized bitmap"
        );
        drop(payload);
        assert_eq!(pool.used(), 0);
    }
}

#[tokio::test]
async fn optional_filter_refusal_keeps_the_normal_join_exact() {
    use query_engine::execution::MemoryPool;
    let keys: Vec<i64> = (0..10000).map(|i| i * (1i64 << 40)).collect();
    let last = *keys.last().unwrap();
    let left = RecordBatch::try_from_iter([(
        "bk",
        Arc::new(Int64Array::from(keys)) as arrow::array::ArrayRef,
    )])
    .unwrap();
    let right = RecordBatch::try_from_iter([(
        "pk",
        Arc::new(Int64Array::from(vec![Some(0), Some(last), Some(-1), None]))
            as arrow::array::ArrayRef,
    )])
    .unwrap();
    let pool = Arc::new(MemoryPool::new(1024 * 1024));
    let slot = Arc::new(parking_lot::Mutex::new(None));
    let mut join = HashJoinExec::new(
        Arc::new(MemoryTableExec::new("b", left.schema(), vec![left], None)),
        Arc::new(MemoryTableExec::new("p", right.schema(), vec![right], None)),
        vec![(Expr::column("bk"), Expr::column("pk"))],
        JoinType::Inner,
    )
    .with_memory_pool(pool.clone());
    join.probe_runtime_filter = Some(slot.clone());
    let batches = join
        .execute(0)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    let mut actual = Vec::new();
    for batch in &batches {
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!a.is_null(row) && !b.is_null(row));
            actual.push((a.value(row), b.value(row)));
        }
    }
    actual.sort_unstable();
    assert_eq!(actual, vec![(0, 0), (last, last)]);
    assert!(
        slot.lock().is_none(),
        "optional filter must decline its insufficient budget"
    );
    assert!(pool.reserved_peak() <= pool.max());
    drop(batches);
    drop(join);
    drop(slot);
    assert_eq!(pool.used(), 0);
}
