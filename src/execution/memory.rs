//! Memory management for query execution
//!
//! This module provides memory tracking and management for query execution,
//! enabling operators to track their memory usage and spill to disk when
//! memory limits are exceeded.

use crate::error::Result;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

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

/// Default hard process cap: 64GiB, matching `scripts/claude-safe-build.sh`'s
/// own cgroup cap on this program's 128GB development box.
const DEFAULT_PROCESS_MEM_CAP: u64 = 64 * 1024 * 1024 * 1024;
/// Floor below which a configured cap is treated as a typo (e.g. a bare "64"
/// parsing as 64 BYTES) rather than an intent the engine could even start
/// under. The floor is applied with a warning, never silently.
const MIN_PROCESS_MEM_CAP: u64 = 256 * 1024 * 1024;

/// Resolve the process-wide memory cap from `QE_MEM_CAP` (same size grammar
/// as `--memory-limit`: "48G", "512MB", raw bytes). Returns the cap plus an
/// optional warning describing why the input was overridden. There is no
/// "unlimited" spelling on purpose: an unparseable value falls back to the
/// default WITH a warning instead of removing the cap.
fn resolve_process_mem_cap(raw: Option<&str>) -> (u64, Option<String>) {
    let raw = match raw {
        Some(v) if !v.trim().is_empty() => v,
        _ => return (DEFAULT_PROCESS_MEM_CAP, None),
    };
    match parse_memory_size(raw) {
        Ok(bytes) if bytes as u64 >= MIN_PROCESS_MEM_CAP => (bytes as u64, None),
        Ok(bytes) => (
            MIN_PROCESS_MEM_CAP,
            Some(format!(
                "QE_MEM_CAP={} ({} bytes) is below the {}MB floor; using the floor",
                raw,
                bytes,
                MIN_PROCESS_MEM_CAP / (1024 * 1024)
            )),
        ),
        Err(_) => (
            DEFAULT_PROCESS_MEM_CAP,
            Some(format!(
                "QE_MEM_CAP={} is not a valid size; using the {}G default",
                raw,
                DEFAULT_PROCESS_MEM_CAP / (1024 * 1024 * 1024)
            )),
        ),
    }
}

/// Kernel-enforced hard cap on this process's memory. Call once, first thing
/// in `main`. No-op off Linux.
///
/// # Why this exists (2026-08-29)
///
/// Documented rules and the safe-build wrapper both failed twice: a bare
/// engine run inside the terminal's cgroup peaked over 100G and systemd-oomd
/// killed the whole terminal scope — session, remote-control bridge,
/// everything. This is the layer that cannot be forgotten or bypassed,
/// because it lives in the binary itself: the ENGINE fails when it exceeds
/// its budget; the terminal never does.
///
/// # Mechanism
///
/// `setrlimit(RLIMIT_DATA)` — since Linux 4.7 it covers brk AND private
/// anonymous mmap, which is where mimalloc (and thread stacks) get every
/// byte. When the engine crosses the cap, mimalloc's mmap fails, the global
/// allocator returns null, and Rust aborts THIS process with "memory
/// allocation of N bytes failed" (fallible paths like `try_reserve` get a
/// clean `Err` instead). `RLIMIT_DATA` rather than `RLIMIT_AS` on purpose:
/// AS also counts file-backed mmaps, and the IPC sidecar / native-table read
/// path maps tens of GB of page-cache-backed segments that pose no OOM risk.
///
/// Note the cap counts MAPPED anonymous bytes, not resident ones. mimalloc
/// frees with MADV_DONTNEED while keeping regions mapped, so the accounted
/// figure can exceed RSS — meaning the cap can only trip EARLY, never late.
/// That is the safe direction.
///
/// Both the soft and hard limits are lowered, and lowering the hard limit is
/// irreversible without CAP_SYS_RESOURCE — so the cap must be sized BEFORE
/// startup via `QE_MEM_CAP` (e.g. `QE_MEM_CAP=110G` for the SF=100 native
/// benchmarks that pass `--memory-limit 100G`); it cannot be raised later in
/// the process's life. That irreversibility is the point.
pub fn enforce_process_memory_cap() {
    #[cfg(target_os = "linux")]
    {
        let raw = std::env::var("QE_MEM_CAP").ok();
        let (mut cap, warning) = resolve_process_mem_cap(raw.as_deref());
        if let Some(w) = warning {
            eprintln!("[mem-cap] WARNING: {}", w);
        }
        unsafe {
            // Never try to RAISE an already-lower hard limit (EPERM without
            // CAP_SYS_RESOURCE) — take the minimum instead.
            let mut current = libc::rlimit {
                rlim_cur: libc::RLIM_INFINITY,
                rlim_max: libc::RLIM_INFINITY,
            };
            if libc::getrlimit(libc::RLIMIT_DATA, &mut current) == 0
                && current.rlim_max != libc::RLIM_INFINITY
            {
                cap = cap.min(current.rlim_max);
            }
            let lim = libc::rlimit {
                rlim_cur: cap,
                rlim_max: cap,
            };
            if libc::setrlimit(libc::RLIMIT_DATA, &lim) != 0 {
                eprintln!(
                    "[mem-cap] WARNING: setrlimit(RLIMIT_DATA) failed ({}); \
                     process memory is NOT capped",
                    std::io::Error::last_os_error()
                );
            } else {
                eprintln!(
                    "[mem-cap] process hard-capped at {:.1}G anonymous memory \
                     (RLIMIT_DATA; the engine aborts if exceeded, the terminal \
                     survives; size with QE_MEM_CAP)",
                    cap as f64 / (1024.0 * 1024.0 * 1024.0)
                );
            }
        }
    }
}

/// A bounded accounting domain. Clones of the internal state share ownership;
/// reservations keep it alive independently of operator or query lifetimes.
#[derive(Debug, Clone)]
pub struct MemoryPool {
    state: Arc<PoolState>,
}

#[derive(Debug)]
struct PoolState {
    name: String,
    max_memory: usize,
    used: AtomicUsize,
    reserved_peak: AtomicUsize,
    observed_peak: AtomicUsize,
    spilled: AtomicUsize,
    parent: Option<Arc<PoolState>>,
    // One lock per hierarchy makes admission transactional across all ancestors.
    // No callbacks or allocation occur while accounting is being changed.
    admission: Arc<Mutex<()>>,
    // Hard prepaid domains stop accounting here. Progress-credit domains
    // propagate usage above the prepaid floor to their parent.
    // Drop the capacity BEFORE returning an optional scheduling credit.
    prepaid: Option<MemoryReservation>,
    grow_past_prepaid: bool,
    _owner: Option<PoolOwner>,
}
struct PoolOwner {
    _value: Box<dyn Send + Sync>,
}
impl std::fmt::Debug for PoolOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("retained pool owner")
    }
}

impl MemoryPool {
    pub fn new(max_memory: usize) -> Self {
        Self::new_named("memory", max_memory)
    }

    pub fn new_named(name: impl Into<String>, max_memory: usize) -> Self {
        Self::build(name.into(), max_memory, None)
    }

    /// A query/operator domain constrained by both its own limit and every
    /// ancestor's limit. Sibling reservations consume the same parent budget.
    pub fn new_child(parent: &MemoryPool, name: impl Into<String>, max_memory: usize) -> Self {
        Self::build(name.into(), max_memory, Some(Arc::clone(&parent.state)))
    }

    fn build(name: String, max_memory: usize, parent: Option<Arc<PoolState>>) -> Self {
        Self::build_owned(name, max_memory, parent, None, None)
    }

    /// Reserve the whole child capacity now. Descendant buffers keep that
    /// reservation alive; sibling allocations cannot steal future input space.
    pub(crate) fn prepaid_child(
        parent: &MemoryPool,
        name: impl Into<String>,
        max_memory: usize,
    ) -> Result<Self> {
        Self::prepaid_child_with_owner(parent, name, max_memory, ())
    }
    pub(crate) fn prepaid_child_with_owner(
        parent: &MemoryPool,
        name: impl Into<String>,
        max_memory: usize,
        owner: impl Send + Sync + 'static,
    ) -> Result<Self> {
        let reservation = parent.allocate(max_memory)?;
        Ok(Self::build_owned(
            name.into(),
            max_memory,
            Some(parent.state.clone()),
            Some(reservation),
            Some(PoolOwner {
                _value: Box::new(owner),
            }),
        ))
    }

    /// Preserve a minimum input-progress reservation while allowing retained
    /// output to grow against the same query budget. Parent charge is exactly
    /// max(credit, child usage); unused credit survives sibling allocations.
    pub(crate) fn child_with_progress_credit(
        parent: &MemoryPool,
        name: impl Into<String>,
        credit: usize,
    ) -> Result<Self> {
        let reservation = parent.allocate(credit)?;
        let mut pool = Self::build_owned(
            name.into(),
            parent.max(),
            Some(parent.state.clone()),
            Some(reservation),
            None,
        );
        Arc::get_mut(&mut pool.state).unwrap().grow_past_prepaid = true;
        Ok(pool)
    }
    fn build_owned(
        name: String,
        max_memory: usize,
        parent: Option<Arc<PoolState>>,
        prepaid: Option<MemoryReservation>,
        owner: Option<PoolOwner>,
    ) -> Self {
        let admission = parent
            .as_ref()
            .map(|p| Arc::clone(&p.admission))
            .unwrap_or_else(|| Arc::new(Mutex::new(())));
        Self {
            state: Arc::new(PoolState {
                name,
                max_memory,
                used: AtomicUsize::new(0),
                reserved_peak: AtomicUsize::new(0),
                observed_peak: AtomicUsize::new(0),
                spilled: AtomicUsize::new(0),
                parent,
                admission,
                prepaid,
                grow_past_prepaid: false,
                _owner: owner,
            }),
        }
    }

    pub(crate) fn is_within(&self, ancestor: &MemoryPool) -> bool {
        let mut node = Some(&self.state);
        while let Some(pool) = node {
            if Arc::ptr_eq(pool, &ancestor.state) {
                return true;
            }
            node = pool.parent.as_ref();
        }
        false
    }

    /// Record bytes written to spill storage (not a memory reservation).
    pub fn record_spill(&self, bytes: usize) {
        self.state.spilled.fetch_add(bytes, Ordering::SeqCst);
    }

    pub fn spilled(&self) -> usize {
        self.state.spilled.load(Ordering::Relaxed)
    }

    /// Reserve before allocating. Failure leaves usage and peaks unchanged.
    /// The returned guard owns its pool reference and can cross async/thread
    /// boundaries. This reserves accounting capacity, not an allocator buffer.
    pub fn allocate(&self, size: usize) -> Result<MemoryReservation> {
        self.state.grow(size)?;
        Ok(MemoryReservation {
            pool: Arc::clone(&self.state),
            size,
        })
    }

    /// Convenience admission probe for consumers that spill on failure.
    pub fn try_allocate(&self, size: usize) -> Option<MemoryReservation> {
        self.allocate(size).ok()
    }

    pub fn used(&self) -> usize {
        self.state.used.load(Ordering::Relaxed)
    }

    /// Legacy high-water telemetry: the larger of reserved usage and the
    /// largest reported local estimate. This is not a query-wide RSS measure.
    pub fn peak(&self) -> usize {
        self.reserved_peak().max(self.observed_peak())
    }

    /// High-water mark of admitted reservations only.
    pub fn reserved_peak(&self) -> usize {
        self.state.reserved_peak.load(Ordering::Relaxed)
    }

    /// High-water mark of local footprint estimates, not reservations.
    pub fn observed_peak(&self) -> usize {
        self.state.observed_peak.load(Ordering::Relaxed)
    }

    pub fn observe(&self, bytes: usize) {
        self.state.observed_peak.fetch_max(bytes, Ordering::SeqCst);
    }

    /// Reset telemetry for this domain. Call only at the domain's query/window
    /// boundary; a shared parent's window is not a per-query metric.
    pub fn reset_peak(&self) {
        let _guard = self
            .state
            .admission
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.state
            .reserved_peak
            .store(self.used(), Ordering::SeqCst);
        self.state.observed_peak.store(0, Ordering::SeqCst);
    }

    pub fn max(&self) -> usize {
        self.state.max_memory
    }

    /// Capacity currently available across this domain and all ancestors.
    pub fn available(&self) -> usize {
        let _guard = self
            .state
            .admission
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        self.state.available_locked()
    }
}

impl PoolState {
    fn accounting_parent(&self) -> Option<&PoolState> {
        if self.prepaid.is_some() && !self.grow_past_prepaid {
            None
        } else {
            self.parent.as_deref()
        }
    }
    fn credit(&self) -> usize {
        if self.grow_past_prepaid {
            self.prepaid.as_ref().unwrap().size()
        } else {
            0
        }
    }
    fn available_locked(&self) -> usize {
        let used = self.used.load(Ordering::Relaxed);
        let local = self.max_memory - used;
        match self.accounting_parent() {
            Some(parent) => local.min(
                parent
                    .available_locked()
                    .saturating_add(self.credit().saturating_sub(used)),
            ),
            None => local,
        }
    }
    fn grow(&self, size: usize) -> Result<()> {
        let guard = self.admission.lock().unwrap_or_else(|e| e.into_inner());
        let mut node = Some(self);
        let mut increment = size;
        while let Some(pool) = node {
            let used = pool.used.load(Ordering::Relaxed);
            if increment > pool.max_memory - used {
                // Build the owned error outside the critical section. Keep the
                // limiting ancestor and its admission snapshot as typed data.
                drop(guard);
                return Err(crate::error::QueryError::MemoryLimit {
                    pool: pool.name.clone(),
                    requested: increment,
                    used,
                    limit: pool.max_memory,
                });
            }
            increment = (used + increment).saturating_sub(pool.credit())
                - used.saturating_sub(pool.credit());
            node = pool.accounting_parent();
        }
        // All levels fit; no counter or peak changes on failed admission.
        let mut node = Some(self);
        let mut increment = size;
        while let Some(pool) = node {
            let used = pool.used.load(Ordering::Relaxed);
            let new_usage = used + increment;
            pool.used.store(new_usage, Ordering::SeqCst);
            pool.reserved_peak.fetch_max(new_usage, Ordering::SeqCst);
            increment =
                new_usage.saturating_sub(pool.credit()) - used.saturating_sub(pool.credit());
            node = pool.accounting_parent();
        }
        Ok(())
    }

    fn release(&self, size: usize) {
        let _guard = self.admission.lock().unwrap_or_else(|e| e.into_inner());
        let mut node = Some(self);
        let mut decrement = size;
        while let Some(pool) = node {
            let used = pool.used.load(Ordering::Relaxed);
            let new_usage = used - decrement;
            pool.used.store(new_usage, Ordering::SeqCst);
            decrement =
                used.saturating_sub(pool.credit()) - new_usage.saturating_sub(pool.credit());
            node = pool.accounting_parent();
        }
    }
}

/// Owned RAII accounting guard. Dropping it releases capacity at every level,
/// including when a query errors, a task is cancelled, or a consumer unwinds.
#[derive(Debug)]
pub struct MemoryReservation {
    pool: Arc<PoolState>,
    size: usize,
}

impl MemoryReservation {
    pub fn size(&self) -> usize {
        self.size
    }

    /// Grow before allocating; shrink after releasing the corresponding buffer.
    /// Failed growth leaves the guard, every counter and every peak unchanged.
    pub fn resize(&mut self, new_size: usize) -> Result<()> {
        if new_size > self.size {
            self.pool.grow(new_size - self.size)?;
        } else {
            self.pool.release(self.size - new_size);
        }
        self.size = new_size;
        Ok(())
    }
}

impl Drop for MemoryReservation {
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

/// Shared process accounting root. Its limit follows the startup process-cap
/// configuration, but this tracks explicit reservations, not OS allocations.
/// Initialize the environment before creating any execution context.
pub fn process_memory_pool() -> SharedMemoryPool {
    static PROCESS_POOL: OnceLock<SharedMemoryPool> = OnceLock::new();
    Arc::clone(PROCESS_POOL.get_or_init(|| {
        let raw = std::env::var("QE_MEM_CAP").ok();
        let (limit, _) = resolve_process_mem_cap(raw.as_deref());
        Arc::new(MemoryPool::new_named(
            "process",
            usize::try_from(limit).unwrap_or(usize::MAX),
        ))
    }))
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

    /// Allow GPU aggregate offload (`--features gpu`). OFF by default and
    /// opted in only by the single-process CLI paths: GPU float reductions
    /// differ from the CPU in the last bits, and the serve/distributed
    /// gates demand byte-exact local answers.
    pub gpu_offload: bool,

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
            // PID-disambiguated: spill-join-correctness-2 epic, task 001.
            // Every spilling operator (join/aggregate/sort) names its own
            // per-query spill directory as `join_0_<id>`/`agg_..._<id>`/
            // `sort_..._<id>`, where `<id>` comes from a PER-PROCESS-LOCAL
            // counter that always starts at 0 in a fresh process. Without a
            // PID (or other process-unique) component in the shared parent
            // directory itself, two DIFFERENT `query_engine` processes on
            // the SAME host (any multi-node-per-host deployment, this
            // repo's own local multi-process cluster test harness, or
            // simply two concurrently-running `serve`/benchmark
            // invocations) landed on the IDENTICAL default path
            // (`$TMPDIR/query_engine_spill/join_0_0/...`) for their FIRST
            // spill. `spill-join-correctness` task 003 characterized this
            // as a same-host collision that "fails loudly, never silently
            // wrong" (`Parquet error`, HTTP 400). Direct instrumented
            // evidence from task 001 of the follow-on epic
            // (`spill-join-correctness-2`) shows that conclusion was
            // INCOMPLETE: under a collision between two DIFFERENTLY-SIZED
            // concurrent queries, one process's spilled build partition
            // (e.g. SF=10) can be silently, partially overwritten with
            // read-back data from an unrelated, concurrently-writing
            // process (e.g. SF=100) sharing the identical file path —
            // caught in the act via a per-partition join-key checksum
            // comparing what was WRITTEN against what was READ BACK
            // (`KeyChecksum`/`batch_key_checksum` in `spillable.rs`),
            // directly correlated with an observed silently-WRONG final
            // query answer, not just a crash. This is very likely the
            // same mechanism behind this engine's own still-open,
            // low-rate (~0.34% pooled) silent wrong-answer bug in
            // `SpillableHashJoinExec`'s spill path (see CLAUDE.md's
            // "Mutation: QA close-out" section) — a background process
            // sharing this host's `$TMPDIR` and ALSO landing on `spill_id
            // 0` at just the wrong moment.
            spill_path: std::env::temp_dir()
                .join(format!("query_engine_spill_{}", std::process::id())),
            spill_partitions: 64,
            batch_size: 8192,
            gpu_offload: false,
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
    fn peak_is_a_high_water_mark_not_the_residual() {
        let pool = MemoryPool::new(1000);
        assert_eq!(pool.peak(), 0);
        {
            let mut a = pool.try_allocate(300).expect("fits");
            assert_eq!(pool.peak(), 300);
            let _b = pool.allocate(500).unwrap();
            assert_eq!(pool.peak(), 800);
            a.resize(400).unwrap();
            assert_eq!(pool.peak(), 900);
            a.resize(100).unwrap();
            assert_eq!(pool.used(), 600);
            assert_eq!(pool.peak(), 900, "shrinking never lowers the peak");
        }
        assert_eq!(pool.used(), 0);
        assert_eq!(pool.peak(), 900, "release never lowers the peak");
        pool.reset_peak();
        assert_eq!(pool.peak(), 0, "reset starts a new window at used()");
        let _c = pool.allocate(50).unwrap();
        pool.reset_peak();
        assert_eq!(pool.peak(), 50);
    }

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

        let mut r = pool.allocate(100).unwrap();
        assert_eq!(pool.used(), 100);

        r.resize(200).unwrap();
        assert_eq!(pool.used(), 200);

        r.resize(50).unwrap();
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

    /// spill-join-correctness-2 epic, task 001: regression test for the
    /// spill-directory-collision fix. FAILS without the fix (the old
    /// default was the literal, PID-less `$TMPDIR/query_engine_spill`,
    /// identical for every `query_engine` process on a host) and PASSES
    /// with it (the default now embeds this process's own PID, so two
    /// DIFFERENT processes can never compute the same default spill
    /// directory — see `Default for ExecutionConfig`'s doc comment for
    /// the full evidence trail, including a real, caught-in-the-act
    /// silent wrong-answer this collision produced).
    #[test]
    fn default_spill_path_is_disambiguated_by_pid() {
        let path = ExecutionConfig::default().spill_path;
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .expect("spill_path must have a final path component");
        let pid = std::process::id().to_string();
        assert!(
            file_name.contains(&pid),
            "default spill_path ({:?}) must embed this process's own PID ({}) \
             so two concurrent query_engine processes on the same host can \
             never compute the identical default spill directory",
            path,
            pid
        );
        // Also pin the exact prefix so a future refactor can't accidentally
        // satisfy the PID check via an unrelated coincidence (e.g. the
        // whole path just happening to contain the PID's digits somewhere
        // in an unrelated segment).
        assert!(
            file_name.starts_with("query_engine_spill_"),
            "default spill_path's file name ({:?}) should still start with \
             the established `query_engine_spill_` prefix",
            file_name
        );
    }

    #[test]
    fn test_resolve_process_mem_cap() {
        // Unset / empty -> default, no warning.
        assert_eq!(
            resolve_process_mem_cap(None),
            (DEFAULT_PROCESS_MEM_CAP, None)
        );
        assert_eq!(
            resolve_process_mem_cap(Some("")),
            (DEFAULT_PROCESS_MEM_CAP, None)
        );

        // Ordinary sizes pass through.
        assert_eq!(
            resolve_process_mem_cap(Some("48G")),
            (48 * 1024 * 1024 * 1024, None)
        );
        assert_eq!(
            resolve_process_mem_cap(Some("512MB")),
            (512 * 1024 * 1024, None)
        );

        // A bare small number is bytes — clamped up to the floor with a
        // warning, so a typo can't make the engine unable to start.
        let (cap, warn) = resolve_process_mem_cap(Some("64"));
        assert_eq!(cap, MIN_PROCESS_MEM_CAP);
        assert!(warn.is_some());

        // Garbage (including any "unlimited" spelling) falls back to the
        // default WITH a warning — there is no way to remove the cap.
        let (cap, warn) = resolve_process_mem_cap(Some("unlimited"));
        assert_eq!(cap, DEFAULT_PROCESS_MEM_CAP);
        assert!(warn.is_some());
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

#[cfg(test)]
mod prepaid_tests {
    use super::*;
    #[test]
    fn progress_credit_grows_releases_and_preserves_minimum() {
        let parent = MemoryPool::new(1024);
        let input = MemoryPool::child_with_progress_credit(&parent, "input", 384).unwrap();
        assert_eq!(parent.used(), 384);
        assert_eq!(input.available(), 1024);
        let sibling = parent.allocate(640).unwrap();
        let mut output = input.allocate(300).unwrap();
        assert_eq!(input.available(), 84);
        let peak = input.reserved_peak();
        let error = output.resize(385).unwrap_err();
        assert!(matches!(
            error,
            crate::QueryError::MemoryLimit {
                requested: 1,
                used: 1024,
                limit: 1024,
                ..
            }
        ));
        assert_eq!(output.size(), 300);
        assert_eq!(input.reserved_peak(), peak);
        drop(sibling);
        output.resize(700).unwrap();
        assert_eq!(parent.used(), 700);
        output.resize(200).unwrap();
        assert_eq!(parent.used(), 384);
        drop(input);
        assert_eq!(parent.used(), 384);
        drop(output);
        assert_eq!(parent.used(), 0);
    }

    #[test]
    fn nested_progress_credit_composes_with_hard_prepaid_limits() {
        let root = MemoryPool::new(2048);
        let outer = MemoryPool::child_with_progress_credit(&root, "outer", 512).unwrap();
        let inner = MemoryPool::child_with_progress_credit(&outer, "inner", 256).unwrap();
        let hard = MemoryPool::prepaid_child(&inner, "hard", 128).unwrap();
        let held = hard.allocate(128).unwrap();
        assert!(hard.allocate(1).unwrap_err().is_memory_limit());
        let mut output = inner.allocate(700).unwrap();
        assert_eq!(inner.used(), 828);
        assert_eq!(outer.used(), 828);
        assert_eq!(root.used(), 828);
        output.resize(1900).unwrap();
        assert_eq!(inner.available(), 20);
        assert!(output.resize(1921).unwrap_err().is_memory_limit());
        assert_eq!(root.used(), 2028);
        drop(output);
        drop(outer);
        drop(inner);
        drop(hard);
        assert_eq!(root.used(), 512);
        drop(held);
        assert_eq!(root.used(), 0);
    }

    #[test]
    fn progress_credit_concurrent_growth_and_release_is_exact() {
        let root = MemoryPool::new(8192);
        let input = Arc::new(MemoryPool::child_with_progress_credit(&root, "input", 512).unwrap());
        std::thread::scope(|scope| {
            for _ in 0..8 {
                let input = input.clone();
                scope.spawn(move || {
                    let child = MemoryPool::new_child(&input, "output", 900);
                    for _ in 0..2000 {
                        let mut first = child.allocate(200).unwrap();
                        let second = child.allocate(300).unwrap();
                        first.resize(600).unwrap();
                        drop(second);
                        first.resize(1).unwrap();
                    }
                });
            }
        });
        assert_eq!(input.used(), 0);
        assert_eq!(root.used(), 512);
        assert!(root.reserved_peak() <= root.max());
        drop(input);
        assert_eq!(root.used(), 0);
    }
    #[test]
    fn prepaid_input_capacity_survives_full_sibling_usage_and_buffer_lifetime() {
        let parent = MemoryPool::new(1024);
        let input = MemoryPool::prepaid_child(&parent, "input", 384).unwrap();
        assert!(input.is_within(&parent));
        assert_eq!(parent.used(), 384);
        let sibling = parent.allocate(640).unwrap();
        assert_eq!(parent.available(), 0);
        assert_eq!(input.available(), 384);
        let mut values = input.allocate(300).unwrap();
        let nested = MemoryPool::new_child(&input, "nested", 128);
        let other = nested.allocate(50).unwrap();
        assert_eq!(parent.used(), 1024);
        assert_eq!(input.used(), 350);
        assert!(values.resize(335).unwrap_err().is_memory_limit());
        assert_eq!(values.size(), 300);
        assert_eq!(input.used(), 350);
        assert_eq!(parent.reserved_peak(), 1024);
        drop(sibling);
        drop(input);
        drop(nested);
        assert_eq!(
            parent.used(),
            384,
            "descendant reservations retain the prepaid domain"
        );
        drop(other);
        assert_eq!(parent.used(), 384);
        drop(values);
        assert_eq!(parent.used(), 0);
    }
    #[test]
    fn prepaid_refusal_is_transactional_and_releases_the_lifetime_owner() {
        struct Owner(Arc<AtomicUsize>);
        impl Drop for Owner {
            fn drop(&mut self) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }
        let parent = MemoryPool::new(100);
        let dropped = Arc::new(AtomicUsize::new(0));
        assert!(MemoryPool::prepaid_child_with_owner(
            &parent,
            "too large",
            101,
            Owner(dropped.clone())
        )
        .unwrap_err()
        .is_memory_limit());
        assert_eq!(parent.used(), 0);
        assert_eq!(parent.reserved_peak(), 0);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn output_credit_returns_only_after_prepaid_capacity_is_available() {
        let parent = Arc::new(MemoryPool::new(1024));
        let credits = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = credits.clone().acquire_owned().await.unwrap();
        let input = MemoryPool::prepaid_child_with_owner(&parent, "frame", 1024, permit).unwrap();
        let mut values =
            crate::execution::ReservedBufferBuilder::<i64>::with_capacity(&input, 8).unwrap();
        values.extend_reserved(8, 0..8).unwrap();
        let buffer = values.finish();
        drop(input);
        let parent2 = parent.clone();
        let waiter = tokio::spawn(async move {
            let _permit = credits.acquire_owned().await.unwrap();
            parent2
                .allocate(1024)
                .expect("credit returned before capacity")
        });
        tokio::task::yield_now().await;
        assert_eq!(parent.used(), 1024);
        drop(buffer);
        let next = tokio::time::timeout(std::time::Duration::from_secs(2), waiter)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(parent.used(), 1024);
        drop(next);
        assert_eq!(parent.used(), 0);
    }
}
