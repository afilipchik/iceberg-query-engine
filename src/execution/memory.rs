//! Memory management for query execution
//!
//! This module provides memory tracking and management for query execution,
//! enabling operators to track their memory usage and spill to disk when
//! memory limits are exceeded.

use crate::error::Result;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Memory pool for tracking memory usage
#[derive(Debug)]
pub struct MemoryPool {
    /// Maximum memory allowed
    max_memory: usize,
    /// Current memory usage
    used: AtomicUsize,
    /// Total bytes that have been spilled to disk
    spilled: AtomicUsize,
}

impl MemoryPool {
    pub fn new(max_memory: usize) -> Self {
        Self {
            max_memory,
            used: AtomicUsize::new(0),
            spilled: AtomicUsize::new(0),
        }
    }

    /// Create a pool with no limit
    pub fn unbounded() -> Self {
        Self::new(usize::MAX)
    }

    /// Record that bytes were spilled to disk
    pub fn record_spill(&self, bytes: usize) {
        self.spilled.fetch_add(bytes, Ordering::SeqCst);
    }

    /// Get total bytes spilled
    pub fn spilled(&self) -> usize {
        self.spilled.load(Ordering::Relaxed)
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

/// Trait for operators that consume memory and can spill to disk
pub trait MemoryConsumer: Send + Sync {
    /// Name of this consumer (for debugging/metrics)
    fn name(&self) -> &str;

    /// Current memory usage in bytes
    fn mem_used(&self) -> usize;

    /// Try to free memory by spilling to disk
    /// Returns the number of bytes freed
    fn spill(&mut self, target_bytes: usize) -> Result<usize>;

    /// Check if this consumer supports spilling
    fn can_spill(&self) -> bool {
        true
    }
}

/// Metrics for tracking spill operations
#[derive(Debug, Default, Clone)]
pub struct SpillMetrics {
    /// Number of partitions that were spilled
    pub partitions_spilled: usize,
    /// Total bytes written to disk during spill
    pub bytes_spilled: usize,
    /// Time spent spilling to disk
    pub spill_time_ms: u64,
    /// Time spent reading spilled data back
    pub read_back_time_ms: u64,
    /// Number of spill files created
    pub spill_files_created: usize,
}

impl SpillMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn merge(&mut self, other: &SpillMetrics) {
        self.partitions_spilled += other.partitions_spilled;
        self.bytes_spilled += other.bytes_spilled;
        self.spill_time_ms += other.spill_time_ms;
        self.read_back_time_ms += other.read_back_time_ms;
        self.spill_files_created += other.spill_files_created;
    }
}

/// Configuration for query execution
#[derive(Debug, Clone)]
pub struct ExecutionConfig {
    /// Maximum memory for query execution (bytes)
    pub memory_limit: usize,

    /// Directory for spill files
    pub spill_path: PathBuf,

    /// Number of partitions for spillable operators (hash join, hash agg)
    pub spill_partitions: usize,

    /// Batch size for streaming reads
    pub batch_size: usize,

    /// Whether to prefer sort-merge join over hash join for large tables
    pub prefer_sort_merge_join: bool,

    /// Enable Iceberg statistics-based file pruning
    pub enable_stats_pruning: bool,

    /// Memory threshold (0.0-1.0) at which to start spilling
    pub spill_threshold: f64,
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            // Default to 1GB memory limit
            memory_limit: 1024 * 1024 * 1024,
            spill_path: std::env::temp_dir().join("query_engine_spill"),
            spill_partitions: 64,
            batch_size: 8192,
            prefer_sort_merge_join: false,
            enable_stats_pruning: true,
            spill_threshold: 0.8,
        }
    }
}

impl ExecutionConfig {
    pub fn new() -> Self {
        Self::default()
    }

    /// Set memory limit in bytes
    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.memory_limit = bytes;
        self
    }

    /// Set memory limit from a string like "1GB", "512MB", "1024KB"
    pub fn with_memory_limit_str(mut self, limit: &str) -> Result<Self> {
        self.memory_limit = parse_memory_size(limit)?;
        Ok(self)
    }

    /// Set spill directory
    pub fn with_spill_path(mut self, path: PathBuf) -> Self {
        self.spill_path = path;
        self
    }

    /// Set number of spill partitions
    pub fn with_spill_partitions(mut self, partitions: usize) -> Self {
        self.spill_partitions = partitions.max(1);
        self
    }

    /// Set batch size for streaming
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size.max(1);
        self
    }

    /// Prefer sort-merge join for large tables
    pub fn with_sort_merge_join(mut self, enabled: bool) -> Self {
        self.prefer_sort_merge_join = enabled;
        self
    }

    /// Enable/disable Iceberg stats pruning
    pub fn with_stats_pruning(mut self, enabled: bool) -> Self {
        self.enable_stats_pruning = enabled;
        self
    }

    /// Create the spill directory if it doesn't exist
    pub fn ensure_spill_dir(&self) -> Result<()> {
        if !self.spill_path.exists() {
            std::fs::create_dir_all(&self.spill_path).map_err(|e| {
                crate::error::QueryError::Execution(format!(
                    "Failed to create spill directory {:?}: {}",
                    self.spill_path, e
                ))
            })?;
        }
        Ok(())
    }
}

/// Parse a memory size string like "1GB", "512MB", "1024KB", "1048576"
fn parse_memory_size(s: &str) -> Result<usize> {
    let s = s.trim().to_uppercase();

    if let Ok(bytes) = s.parse::<usize>() {
        return Ok(bytes);
    }

    let (num_str, multiplier) = if s.ends_with("GB") {
        (&s[..s.len() - 2], 1024 * 1024 * 1024)
    } else if s.ends_with("MB") {
        (&s[..s.len() - 2], 1024 * 1024)
    } else if s.ends_with("KB") {
        (&s[..s.len() - 2], 1024)
    } else if s.ends_with('G') {
        (&s[..s.len() - 1], 1024 * 1024 * 1024)
    } else if s.ends_with('M') {
        (&s[..s.len() - 1], 1024 * 1024)
    } else if s.ends_with('K') {
        (&s[..s.len() - 1], 1024)
    } else if s.ends_with('B') {
        (&s[..s.len() - 1], 1)
    } else {
        return Err(crate::error::QueryError::Execution(format!(
            "Invalid memory size format: {}. Use formats like '1GB', '512MB', '1024KB', or bytes",
            s
        )));
    };

    let num: f64 = num_str.trim().parse().map_err(|_| {
        crate::error::QueryError::Execution(format!("Invalid memory size number: {}", num_str))
    })?;

    Ok((num * multiplier as f64) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_pool() {
        let pool = MemoryPool::new(1000);

        assert_eq!(pool.used(), 0);
        assert_eq!(pool.available(), 1000);
        assert_eq!(pool.spilled(), 0);

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

    #[test]
    fn test_spill_tracking() {
        let pool = MemoryPool::new(1000);
        assert_eq!(pool.spilled(), 0);

        pool.record_spill(500);
        assert_eq!(pool.spilled(), 500);

        pool.record_spill(300);
        assert_eq!(pool.spilled(), 800);
    }

    #[test]
    fn test_parse_memory_size() {
        assert_eq!(parse_memory_size("1024").unwrap(), 1024);
        assert_eq!(parse_memory_size("1KB").unwrap(), 1024);
        assert_eq!(parse_memory_size("1K").unwrap(), 1024);
        assert_eq!(parse_memory_size("1MB").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_size("1M").unwrap(), 1024 * 1024);
        assert_eq!(parse_memory_size("1GB").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("1G").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_memory_size("512mb").unwrap(), 512 * 1024 * 1024);
        assert_eq!(
            parse_memory_size("2.5GB").unwrap(),
            (2.5 * 1024.0 * 1024.0 * 1024.0) as usize
        );
    }

    #[test]
    fn test_execution_config() {
        let config = ExecutionConfig::new()
            .with_memory_limit(512 * 1024 * 1024)
            .with_spill_partitions(32)
            .with_batch_size(4096);

        assert_eq!(config.memory_limit, 512 * 1024 * 1024);
        assert_eq!(config.spill_partitions, 32);
        assert_eq!(config.batch_size, 4096);
    }
}
