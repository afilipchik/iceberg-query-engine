//! Synchronous expression allocation scope. Scope guards restore nested callers;
//! they never span an await. Rayon/other evaluator entry points must opt in with
//! their own query pool rather than inheriting a thread's previous scope.
use super::SharedMemoryPool;
use std::cell::RefCell;
thread_local! {
    static POOL: RefCell<Option<SharedMemoryPool>> = const { RefCell::new(None) };
}
/// Read once per process; diagnostics run in fresh workers and never in paired
/// latency samples. No JSON or formatting allocation occurs when disabled.
pub(crate) fn trace_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("QE_TRACE_RESERVED_EXPRESSIONS").is_ok_and(|v| v == "1"))
}

pub(crate) fn expression_pool() -> Option<SharedMemoryPool> {
    POOL.with(|p| p.borrow().clone())
}
pub(crate) fn with_expression_pool<T>(pool: &SharedMemoryPool, f: impl FnOnce() -> T) -> T {
    struct Restore(Option<SharedMemoryPool>);
    impl Drop for Restore {
        fn drop(&mut self) {
            POOL.with(|p| *p.borrow_mut() = self.0.take());
        }
    }
    let _restore = Restore(POOL.with(|p| p.replace(Some(pool.clone()))));
    f()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::MemoryPool;
    use std::sync::Arc;
    #[test]
    fn expression_scope_restores_nested_and_unwinding_callers() {
        assert!(expression_pool().is_none());
        let outer = Arc::new(MemoryPool::new(4096));
        let inner = Arc::new(MemoryPool::new(2048));
        with_expression_pool(&outer, || {
            assert!(Arc::ptr_eq(&expression_pool().unwrap(), &outer));
            let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                with_expression_pool(&inner, || {
                    assert!(Arc::ptr_eq(&expression_pool().unwrap(), &inner));
                    panic!("injected evaluator unwind");
                });
            }));
            assert!(failed.is_err());
            assert!(Arc::ptr_eq(&expression_pool().unwrap(), &outer));
        });
        assert!(expression_pool().is_none());
    }
    #[test]
    fn expression_scope_never_leaks_into_another_thread() {
        let pool = Arc::new(MemoryPool::new(4096));
        with_expression_pool(&pool, || {
            std::thread::spawn(|| assert!(expression_pool().is_none()))
                .join()
                .unwrap()
        });
        assert!(expression_pool().is_none());
    }
}
