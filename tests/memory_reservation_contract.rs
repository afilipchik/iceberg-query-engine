//! Contract tests for admission accounting, independent of operator estimates.
use query_engine::execution::MemoryPool;
use std::sync::{Arc, Barrier};

#[test]
fn failed_growth_is_atomic_at_every_ancestor() {
    let process = MemoryPool::new_named("process", 100);
    let query = MemoryPool::new_child(&process, "query", 80);
    let operator = MemoryPool::new_child(&query, "hash aggregate", 70);
    let other = process.allocate(40).unwrap();
    let mut reservation = operator.allocate(50).unwrap();
    let error = reservation.resize(65).unwrap_err().to_string();
    assert!(error.contains("process"));
    assert_eq!(reservation.size(), 50);
    for (pool, expected) in [(&operator, 50), (&query, 50), (&process, 90)] {
        assert_eq!(pool.used(), expected);
        assert_eq!(pool.reserved_peak(), expected);
    }
    assert_eq!(operator.available(), 10);
    drop(other);
    reservation.resize(65).unwrap();
    assert_eq!(process.used(), 65);
    reservation.resize(10).unwrap();
    assert_eq!(query.used(), 10);
    drop(reservation);
    assert_eq!(process.used(), 0);
    assert_eq!(query.used(), 0);
    assert_eq!(operator.used(), 0);
}

#[test]
fn child_limit_cannot_be_bypassed_by_parent_capacity() {
    let process = MemoryPool::new_named("process", 1000);
    let query = MemoryPool::new_child(&process, "query-7", 50);
    assert!(query
        .allocate(51)
        .unwrap_err()
        .to_string()
        .contains("query-7"));
    assert_eq!(query.used(), 0);
    assert_eq!(process.used(), 0);
    assert_eq!(process.reserved_peak(), 0);
}

#[test]
fn concurrent_siblings_cannot_over_admit_parent() {
    let parent = Arc::new(MemoryPool::new(400));
    let ready = Arc::new(Barrier::new(17));
    let acquired = Arc::new(Barrier::new(17));
    let release = Arc::new(Barrier::new(17));
    let handles: Vec<_> = (0..16)
        .map(|id| {
            let parent = Arc::clone(&parent);
            let ready = Arc::clone(&ready);
            let acquired = Arc::clone(&acquired);
            let release = Arc::clone(&release);
            std::thread::spawn(move || {
                let query = MemoryPool::new_child(&parent, format!("query-{id}"), 200);
                ready.wait();
                let reservation = query.allocate(100).ok();
                acquired.wait();
                release.wait();
                reservation.is_some()
            })
        })
        .collect();
    ready.wait();
    acquired.wait();
    assert_eq!(parent.used(), 400);
    assert_eq!(parent.reserved_peak(), 400);
    release.wait();
    let admitted = handles
        .into_iter()
        .map(|h| usize::from(h.join().unwrap()))
        .sum::<usize>();
    assert_eq!(admitted, 4);
    assert_eq!(parent.used(), 0);
}

#[test]
fn reservation_owns_pool_and_releases_after_move() {
    let parent = MemoryPool::new(100);
    let reservation = {
        let child = MemoryPool::new_child(&parent, "temporary query", 100);
        child.allocate(80).unwrap()
    };
    assert_eq!(parent.used(), 80);
    std::thread::spawn(move || drop(reservation))
        .join()
        .unwrap();
    assert_eq!(parent.used(), 0);
}

#[test]
fn errors_and_unwind_release_capacity() {
    let pool = MemoryPool::new(100);
    // This fresh root has no opaque lifetime owner. Exercise reservation Drop
    // across the intentional panic without asserting unwind safety for every
    // possible owner that a different pool could retain.
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _reservation = pool.allocate(80).unwrap();
        panic!("simulated consumer failure");
    }));
    assert!(result.is_err());
    assert_eq!(pool.used(), 0);
    let fail = || -> query_engine::error::Result<()> {
        let _reservation = pool.allocate(80)?;
        let _denied = pool.allocate(21)?;
        Ok(())
    };
    assert!(fail().is_err());
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn cancelled_task_releases_query_and_parent() {
    let parent = Arc::new(MemoryPool::new(100));
    let query = Arc::new(MemoryPool::new_child(&parent, "cancelled query", 80));
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let task_query = Arc::clone(&query);
    let task = tokio::spawn(async move {
        let _reservation = task_query.allocate(80).unwrap();
        ready_tx.send(()).unwrap();
        std::future::pending::<()>().await;
    });
    ready_rx.await.unwrap();
    assert_eq!(parent.used(), 80);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_eq!(parent.used(), 0);
    assert_eq!(query.used(), 0);
}

#[test]
fn zero_and_overflow_requests_do_not_corrupt_counters() {
    let zero = MemoryPool::new(0);
    let mut reservation = zero.allocate(0).unwrap();
    assert!(reservation.resize(1).is_err());
    assert_eq!(zero.used(), 0);
    let pool = MemoryPool::new(usize::MAX);
    let mut largest = pool.allocate(usize::MAX - 1).unwrap();
    assert!(pool.allocate(2).is_err());
    assert_eq!(pool.used(), usize::MAX - 1);
    largest.resize(usize::MAX).unwrap();
    assert!(pool.allocate(1).is_err());
    drop(largest);
    assert_eq!(pool.used(), 0);
}

#[test]
fn estimates_are_separate_from_reservations() {
    let pool = MemoryPool::new(100);
    pool.observe(1000);
    let reservation = pool.allocate(50).unwrap();
    assert_eq!(pool.used(), 50);
    assert_eq!(pool.available(), 50);
    assert_eq!(pool.reserved_peak(), 50);
    assert_eq!(pool.observed_peak(), 1000);
    assert_eq!(pool.peak(), 1000);
    pool.reset_peak();
    assert_eq!(pool.observed_peak(), 0);
    assert_eq!(pool.reserved_peak(), 50);
    drop(reservation);
}

#[test]
fn contexts_share_explicit_parent_and_cannot_bypass_process_root() {
    use query_engine::execution::{process_memory_pool, ExecutionConfig, ExecutionContext};
    let budget = Arc::new(MemoryPool::new_child(
        &process_memory_pool(),
        "server budget",
        100,
    ));
    let config = ExecutionConfig::default().with_memory_limit(80);
    let a =
        ExecutionContext::with_config_and_parent_pool(config.clone(), Arc::clone(&budget)).unwrap();
    let b =
        ExecutionContext::with_config_and_parent_pool(config.clone(), Arc::clone(&budget)).unwrap();
    let reservation = a.memory_pool().allocate(60).unwrap();
    assert!(b
        .memory_pool()
        .allocate(50)
        .unwrap_err()
        .to_string()
        .contains("server budget"));
    assert_eq!(b.memory_available(), 40);
    drop(reservation);
    assert_eq!(budget.used(), 0);
    assert!(
        ExecutionContext::with_config_and_parent_pool(config, Arc::new(MemoryPool::new(100)),)
            .is_err()
    );
}

#[tokio::test]
async fn sql_metrics_are_query_local_and_do_not_reset_context() {
    use query_engine::ExecutionContext;
    let context = ExecutionContext::new();
    context.memory_pool().observe(12345);
    context.memory_pool().record_spill(67890);
    let (a, b) = tokio::join!(context.sql("SELECT 1"), context.sql("SELECT 2"));
    let a = a.unwrap();
    let b = b.unwrap();
    assert_ne!(a.metrics.query_id, b.metrics.query_id);
    assert_ne!(a.metrics.query_id, 0);
    assert_eq!(a.metrics.observed_peak_memory_bytes, 0);
    assert_eq!(b.metrics.observed_peak_memory_bytes, 0);
    assert!(a.metrics.spill_metrics.is_none());
    assert!(b.metrics.spill_metrics.is_none());
    assert_eq!(context.memory_pool().observed_peak(), 12345);
    assert_eq!(context.memory_pool().spilled(), 67890);
}
