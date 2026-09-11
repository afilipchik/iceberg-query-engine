use query_engine::{execution::MemoryPool, QueryError};
use std::sync::Arc;

#[test]
fn hierarchical_denial_preserves_snapshot_and_atomic_reservation() {
    let parent = MemoryPool::new_named("query", 1024);
    let child = MemoryPool::new_child(&parent, "worker", 4096);
    let sibling = parent.allocate(700).unwrap();
    let mut reservation = child.allocate(100).unwrap();
    let error = reservation.resize(500).unwrap_err();
    assert!(error.is_memory_limit());
    match &error {
        QueryError::MemoryLimit {
            pool,
            requested,
            used,
            limit,
        } => {
            assert_eq!(pool, "query");
            assert_eq!((*requested, *used, *limit), (400, 800, 1024));
        }
        other => panic!("expected typed ancestor pressure: {other}"),
    }
    assert_eq!(reservation.size(), 100);
    assert_eq!((child.used(), parent.used()), (100, 800));
    assert_eq!((child.reserved_peak(), parent.reserved_peak()), (100, 800));
    assert_eq!(error.kind(), "Execution");
    assert_eq!(error.to_string(), "Execution error: Memory limit exceeded in 'query': requested 400 additional bytes, used 800, limit 1024");
    let original = Arc::new(error);
    let shared = QueryError::Shared(Arc::new(QueryError::Shared(original.clone())));
    assert!(shared.is_memory_limit());
    assert!(std::ptr::eq(shared.root(), original.as_ref()));
    drop(sibling);
    reservation.resize(500).unwrap();
    assert_eq!((child.used(), parent.used()), (500, 500));
    drop(reservation);
    assert_eq!((child.used(), parent.used()), (0, 0));
    // The error retains the admission snapshot, not live mutable counters.
    assert!(matches!(
        shared.root(),
        QueryError::MemoryLimit { used: 800, .. }
    ));
}

#[test]
fn child_denial_is_distinct_from_lookalike_execution_and_allocator_errors() {
    let parent = MemoryPool::new_named("query", 4096);
    let child = MemoryPool::new_child(&parent, "worker", 128);
    let error = match child.allocate(129) {
        Err(error) => error,
        Ok(_) => panic!("child limit must refuse"),
    };
    assert!(
        matches!(error.root(), QueryError::MemoryLimit { pool, requested: 129, used: 0, limit: 128 } if pool == "worker")
    );
    assert_eq!((child.used(), parent.used()), (0, 0));
    assert_eq!((child.reserved_peak(), parent.reserved_peak()), (0, 0));
    let lookalike = QueryError::Execution(
        "Memory limit exceeded in 'worker': requested 129 additional bytes, used 0, limit 128"
            .into(),
    );
    assert_eq!(lookalike.to_string(), error.to_string());
    assert!(!lookalike.is_memory_limit());
    let allocator = QueryError::Io(std::io::Error::from(std::io::ErrorKind::OutOfMemory));
    assert!(!allocator.is_memory_limit());
    assert!(!QueryError::Shared(Arc::new(allocator)).is_memory_limit());
}
