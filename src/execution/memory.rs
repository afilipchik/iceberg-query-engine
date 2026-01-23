//! Memory management for query execution

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Memory pool for tracking memory usage
#[derive(Debug)]
pub struct MemoryPool {
    /// Maximum memory allowed
    max_memory: usize,
    /// Current memory usage
    used: AtomicUsize,
}

impl MemoryPool {
    pub fn new(max_memory: usize) -> Self {
        Self {
            max_memory,
            used: AtomicUsize::new(0),
        }
    }

    /// Create a pool with no limit
    pub fn unbounded() -> Self {
        Self::new(usize::MAX)
    }

    /// Try to allocate memory
    pub fn try_allocate(&self, size: usize) -> Option<MemoryReservation<'_>> {
        let mut current = self.used.load(Ordering::Relaxed);
        loop {
            let new_usage = current.checked_add(size)?;
            if new_usage > self.max_memory {
                return None;
            }

            match self.used.compare_exchange_weak(
                current,
                new_usage,
                Ordering::SeqCst,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Some(MemoryReservation { pool: self, size });
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Force allocate memory (may exceed limit)
    pub fn allocate(&self, size: usize) -> MemoryReservation<'_> {
        self.used.fetch_add(size, Ordering::SeqCst);
        MemoryReservation { pool: self, size }
    }

    /// Current memory usage
    pub fn used(&self) -> usize {
        self.used.load(Ordering::Relaxed)
    }

    /// Maximum memory
    pub fn max(&self) -> usize {
        self.max_memory
    }

    /// Available memory
    pub fn available(&self) -> usize {
        self.max_memory.saturating_sub(self.used())
    }

    fn release(&self, size: usize) {
        self.used.fetch_sub(size, Ordering::SeqCst);
    }
}

/// RAII guard for memory reservation
pub struct MemoryReservation<'a> {
    pool: &'a MemoryPool,
    size: usize,
}

impl<'a> MemoryReservation<'a> {
    /// Size of this reservation
    pub fn size(&self) -> usize {
        self.size
    }

    /// Resize the reservation
    pub fn resize(&mut self, new_size: usize) {
        if new_size > self.size {
            let diff = new_size - self.size;
            self.pool.used.fetch_add(diff, Ordering::SeqCst);
        } else {
            let diff = self.size - new_size;
            self.pool.used.fetch_sub(diff, Ordering::SeqCst);
        }
        self.size = new_size;
    }
}

impl<'a> Drop for MemoryReservation<'a> {
    fn drop(&mut self) {
        self.pool.release(self.size);
    }
}

/// Shared memory pool
pub type SharedMemoryPool = Arc<MemoryPool>;

/// Create a shared memory pool
pub fn create_memory_pool(max_memory: usize) -> SharedMemoryPool {
    Arc::new(MemoryPool::new(max_memory))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_pool() {
        let pool = MemoryPool::new(1000);

        assert_eq!(pool.used(), 0);
        assert_eq!(pool.available(), 1000);

        let r1 = pool.try_allocate(500).unwrap();
        assert_eq!(pool.used(), 500);
        assert_eq!(pool.available(), 500);

        let r2 = pool.try_allocate(400).unwrap();
        assert_eq!(pool.used(), 900);

        // This should fail
        assert!(pool.try_allocate(200).is_none());

        drop(r1);
        assert_eq!(pool.used(), 400);

        drop(r2);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn test_resize_reservation() {
        let pool = MemoryPool::new(1000);

        let mut r = pool.allocate(100);
        assert_eq!(pool.used(), 100);

        r.resize(200);
        assert_eq!(pool.used(), 200);

        r.resize(50);
        assert_eq!(pool.used(), 50);

        drop(r);
        assert_eq!(pool.used(), 0);
    }
}
