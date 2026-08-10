//! Memory management for query execution
//!
//! This module provides memory tracking and management for query execution,
//! enabling operators to track their memory usage and spill to disk when
//! memory limits are exceeded.

use crate::error::Result;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// Opt this process out of transparent huge pages (2MB), keeping 4KB pages.
///
/// # Why the engine does NOT want huge pages
///
/// The intuition says otherwise — a multi-GB hash table probed at random costs
/// ~262,000 TLB entries at 4KB versus 512 at 2MB — so this was measured rather
/// than assumed, at SF=10 on 22 TPC-H queries, 5-7 interleaved A/B pairs each:
///
/// * A standalone random-probe microbenchmark says 2MB pages ARE worth
///   8-11% over a 1GB table, and ~7% over 64MB. The TLB win is real.
/// * The engine still runs FASTER on 4KB pages: **suite total 7.98s -> 7.48s
///   (-6.3%)**, 16 of 22 queries faster — Q01 -22%, Q06 -18%, Q13 -14%,
///   Q18 -13%, Q14 -12%, Q11 -11%, Q04 -11%.
/// * Of the 6 that were not faster, three (Q02/Q03/Q10) are sub-1% ties, and
///   Q19 (+8.4%) and Q21 (+2.2%) both flipped to FASTER on 4KB when
///   re-measured at 7 pairs (-2.7%, -4.8%) — they were noise. Only Q16 has no
///   consistent direction (+2.5% one run, -2.2% another). No query reliably
///   prefers 2MB pages.
///
/// The reason the microbenchmark does not transfer: the engine's hot memory is
/// *streamed*, not randomly probed. Morsel-driven scans allocate, fill, drain
/// and free large Arrow buffers continuously, so sequential prefetch already
/// hides the TLB cost that huge pages would remove. What huge pages add instead
/// is fault-time cost — on Q01, 2MB pages raised kernel time 2.64s -> 3.94s and
/// user time 6.27s -> 7.80s, because the kernel must zero a full 2MB on every
/// fault and the engine touches ~16% more physical memory as a result
/// (RSS 1.75GB -> 1.94GB). We pay to zero memory we never read.
///
/// Dropping to 4KB therefore both speeds the engine up and shrinks its peak
/// RSS, which is the direction the memory-safety rule wants anyway.
///
/// mimalloc explicitly asks for huge pages (`madvise(MADV_HUGEPAGE)`) on its
/// large regions, so without this call the engine gets 2MB backing for ~97-99%
/// of its RSS on any machine whose THP mode is `always` or `madvise`.
/// `PR_SET_THP_DISABLE` is used rather than mimalloc's `allow_thp` option
/// because it takes effect immediately for every subsequent fault, regardless
/// of whether mimalloc's one-shot OS-layer init has already run.
///
/// Set `QUERY_ENGINE_ALLOW_THP=1` to keep huge pages (for re-measuring this).
/// Call once, early in `main`. No-op off Linux.
pub fn disable_transparent_hugepages() {
    // Only an explicit affirmative keeps huge pages. An empty or unrecognised
    // value means "unset", so a stray `FOO=` in a shell script cannot silently
    // switch the engine back onto the slower path.
    let allow = std::env::var("QUERY_ENGINE_ALLOW_THP")
        .map(|v| matches!(v.trim(), "1" | "true" | "yes" | "on"))
        .unwrap_or(false);
    if allow {
        return;
    }
    #[cfg(target_os = "linux")]
    unsafe {
        // PR_SET_THP_DISABLE == 41. Unprivileged, inherited by children, and
        // advisory: a kernel without it just returns EINVAL, which we ignore.
        libc::prctl(41, 1, 0, 0, 0);
    }
}

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

/// Configuration for query execution.
///
/// Spillable operators (SpillableHashJoinExec, SpillableHashAggregateExec, ExternalSortExec)
/// are always active. The engine is memory-safe by default — being slow on larger-than-memory
/// datasets is acceptable, but OOM is not.
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

    /// Enable morsel-driven parallel execution for aggregations over Parquet
    /// When true (default), uses optimized parallel aggregation for Parquet scans
    pub enable_morsel_execution: bool,

    /// How `ORDER BY <distance> LIMIT k` is answered. See [`VectorSearchMode`].
    pub vector_search_mode: VectorSearchMode,

    /// Candidates to re-rank with exact distances, as a multiple of `k`.
    ///
    /// This is the recall lever: the index proposes `k * factor` rows, then
    /// exact distances decide the final k. `None` means no refinement.
    pub vector_refine_factor: Option<u32>,

    /// IVF partitions to probe. `None` uses the index's own default.
    pub vector_nprobes: Option<usize>,
}

/// Which semantics `ORDER BY <distance> LIMIT k` gets.
///
/// # Why this is a user-visible choice and not an optimizer decision
///
/// An IVF_PQ / HNSW index is **approximate**. Answering a SQL query from it can
/// return different rows than the query literally specifies. Every other
/// optimization in this engine preserves the answer exactly; this one does not,
/// so it is not the optimizer's call to make silently. The mode is explicit,
/// and the default is documented in CLAUDE.md with the measured recall that
/// justifies it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSearchMode {
    /// Always compute exact distances for every row. Slowest, always right.
    Exact,
    /// Let the storage layer serve the search from its index when it has one,
    /// falling back to `Exact` when it does not. Results may differ from
    /// `Exact` — see `vector_refine_factor`.
    Indexed,
}

impl VectorSearchMode {
    /// Parse from a config string (`exact` / `indexed`); anything else is None.
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "exact" | "brute" | "brute_force" | "off" | "false" | "0" => Some(Self::Exact),
            "indexed" | "index" | "approx" | "approximate" | "on" | "true" | "1" => {
                Some(Self::Indexed)
            }
            _ => None,
        }
    }
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
            // Morsel execution enabled by default for better performance
            enable_morsel_execution: true,
            // DEFAULT: Exact. Measured on data/vectors.lance (200k x 384,
            // IVF_PQ 447 partitions / 48 sub-vectors, cosine), 10 natural
            // language queries, k=10:
            //
            //   exact                109 ms   recall 1.000
            //   indexed, no refine     5 ms   recall 0.590
            //   indexed, refine=10     6 ms   recall 0.910
            //   indexed, refine=50    15 ms   recall 0.940   <- plateau
            //
            // 21x faster is a real prize, but recall does not reach 1.0 at any
            // refine factor, so the indexed path answers a measurably
            // DIFFERENT question than the SQL asked. Making that the default
            // would mean `ORDER BY distance LIMIT 10` quietly dropping about
            // one true neighbour in ten because an index file happened to
            // exist. Users who want that trade set `QE_VECTOR_SEARCH=indexed`
            // (or `ExecutionConfig::vector_search_mode`) and get it.
            vector_search_mode: std::env::var("QE_VECTOR_SEARCH")
                .ok()
                .and_then(|v| VectorSearchMode::parse(&v))
                .unwrap_or(VectorSearchMode::Exact),
            // refine=10 is the knee: 0.59 -> 0.91 recall for ~1 ms. Going to
            // 50 buys 0.94 for 9 ms more, and nothing buys 1.0.
            vector_refine_factor: match std::env::var("QE_VECTOR_REFINE") {
                Ok(v) => v.parse::<u32>().ok().filter(|f| *f > 0),
                Err(_) => Some(10),
            },
            // Left at Lance's own default on purpose. MEASURED IN LANCE
            // 0.23.2: raising nprobes makes recall WORSE, monotonically —
            // 0.91 at the default, 0.38 at nprobes=20, 0.16 at nprobes=447
            // (all with refine=10). That is backwards for IVF and reproduces
            // in raw pylance, so it is a defect in the pinned version, not in
            // this engine. The knob is kept for the day it is fixed.
            vector_nprobes: std::env::var("QE_VECTOR_NPROBES")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .filter(|n| *n > 0),
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

    /// Enable morsel-driven parallel execution for aggregations over Parquet
    pub fn with_morsel_execution(mut self, enabled: bool) -> Self {
        self.enable_morsel_execution = enabled;
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
