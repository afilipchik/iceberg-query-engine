//! TEMPORARY diagnostic allocator wrapper for the `oom-safety-hardening`
//! investigation (2026-08-29). Not wired into `lib.rs` by default — see
//! that file's own `#[global_allocator]` comment for how to enable it.
//!
//! No profiling tool (heaptrack, valgrind) is installable here without a
//! sudo password, and `perf record` is blocked by this box's
//! `perf_event_paranoid=4`. This is a from-scratch, tool-free substitute:
//! a `GlobalAlloc` wrapper around the real allocator (mimalloc) that, when
//! `QE_ALLOC_PROFILE=1` is set, tracks every LARGE (>= 256KB) live
//! allocation's call site via `std::backtrace::Backtrace`, and snapshots
//! "who owns the live bytes" every time a new global peak is reached.
//! Zero behavior change when the env var is unset (one atomic load, one
//! branch, then straight through to the inner allocator).
//!
//! Usage: temporarily swap `lib.rs`'s `#[global_allocator]` to
//! `ProfilingAlloc(mimalloc::MiMalloc)`, rebuild, run with
//! `RUST_BACKTRACE=1 QE_ALLOC_PROFILE=1`, then call
//! `alloc_profile::print_peak_snapshot()` at the end of the program (or
//! from a `Drop` guard) to see the top call sites by live bytes at the
//! moment of the process's peak allocation.

use std::alloc::{GlobalAlloc, Layout};
use std::backtrace::Backtrace;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock};

/// Only allocations at least this large get backtrace-tracked. Keeps
/// overhead bounded — the millions of small per-row/per-cell allocations
/// this engine makes are not what a few-hundred-MB overshoot is made of;
/// large buffers (RecordBatch column buffers, Vec<RecordBatch> growth,
/// ArrowWriter internal encoders) are.
const LARGE_THRESHOLD: usize = 256 * 1024;

fn enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var("QE_ALLOC_PROFILE").as_deref() == Ok("1"))
}

static CURRENT_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);
/// Guards against re-entrant backtrace capture (backtrace capture itself
/// allocates) causing infinite recursion/deadlock.
thread_local! {
    static IN_PROFILER: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

struct LiveAlloc {
    size: usize,
    site: u32,
}

static LIVE: OnceLock<Mutex<HashMap<usize, LiveAlloc>>> = OnceLock::new();
static SITES: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
static SITE_IDS: OnceLock<Mutex<HashMap<String, u32>>> = OnceLock::new();
static PEAK_SNAPSHOT: OnceLock<Mutex<HashMap<u32, (usize, usize)>>> = OnceLock::new();

fn live() -> &'static Mutex<HashMap<usize, LiveAlloc>> {
    LIVE.get_or_init(|| Mutex::new(HashMap::new()))
}
fn sites() -> &'static Mutex<Vec<String>> {
    SITES.get_or_init(|| Mutex::new(Vec::new()))
}
fn site_ids() -> &'static Mutex<HashMap<String, u32>> {
    SITE_IDS.get_or_init(|| Mutex::new(HashMap::new()))
}
fn peak_snapshot_store() -> &'static Mutex<HashMap<u32, (usize, usize)>> {
    PEAK_SNAPSHOT.get_or_init(|| Mutex::new(HashMap::new()))
}

fn site_id_for(bt: &Backtrace) -> u32 {
    // Keep only a handful of frames -- LTO inlines aggressively, so the
    // full backtrace is long and mostly noise; the first few real frames
    // above the allocator are what distinguish call sites here.
    let s = format!("{bt}");
    let key: String = s.lines().take(24).collect::<Vec<_>>().join("\n");
    let mut ids = site_ids().lock().unwrap();
    if let Some(&id) = ids.get(&key) {
        return id;
    }
    let mut list = sites().lock().unwrap();
    let id = list.len() as u32;
    list.push(key.clone());
    ids.insert(key, id);
    id
}

fn record_alloc(ptr: *mut u8, size: usize) {
    let current = CURRENT_BYTES.fetch_add(size, Ordering::Relaxed) + size;
    let mut new_peak = false;
    let mut prev = PEAK_BYTES.load(Ordering::Relaxed);
    while current > prev {
        match PEAK_BYTES.compare_exchange_weak(prev, current, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => {
                new_peak = true;
                break;
            }
            Err(actual) => prev = actual,
        }
    }

    if size >= LARGE_THRESHOLD {
        let reentrant = IN_PROFILER.with(|f| f.get());
        if !reentrant {
            IN_PROFILER.with(|f| f.set(true));
            let bt = Backtrace::capture();
            let site = site_id_for(&bt);
            live()
                .lock()
                .unwrap()
                .insert(ptr as usize, LiveAlloc { size, site });
            IN_PROFILER.with(|f| f.set(false));
        }
    }

    if new_peak {
        snapshot_peak(current);
    }
}

fn record_dealloc(ptr: *mut u8, size: usize) {
    CURRENT_BYTES.fetch_sub(size, Ordering::Relaxed);
    if size >= LARGE_THRESHOLD {
        live().lock().unwrap().remove(&(ptr as usize));
    }
}

/// Last peak (bytes) a full top-N breakdown was printed for. Rate-limits
/// the expensive full-scan print so a monotonically-growing phase 1
/// collection doesn't spam stderr with one print per allocation -- but
/// still guarantees a print close to the true peak even if the process is
/// SIGKILLed before reaching a clean exit (this is the whole point: the
/// control repro's own peak is never seen by `print_peak_snapshot` below,
/// since that process dies before returning from `main`).
static LAST_PRINTED_PEAK: AtomicUsize = AtomicUsize::new(0);

fn snapshot_peak(current_total: usize) {
    let reentrant = IN_PROFILER.with(|f| f.get());
    if reentrant {
        return;
    }
    IN_PROFILER.with(|f| f.set(true));
    let map = live().lock().unwrap();
    let mut by_site: HashMap<u32, (usize, usize)> = HashMap::new();
    for alloc in map.values() {
        let e = by_site.entry(alloc.site).or_insert((0, 0));
        e.0 += 1;
        e.1 += alloc.size;
    }
    drop(map);
    let tracked_bytes: usize = by_site.values().map(|(_, b)| *b).sum();
    *peak_snapshot_store().lock().unwrap() = by_site.clone();

    let last = LAST_PRINTED_PEAK.load(Ordering::Relaxed);
    let should_print_full =
        current_total > last + 8 * 1024 * 1024 || current_total as f64 > last as f64 * 1.05;
    if should_print_full {
        LAST_PRINTED_PEAK.store(current_total, Ordering::Relaxed);
        print_snapshot(&by_site, current_total, tracked_bytes, 8);
    } else {
        eprintln!(
            "[alloc-profile] new peak: current_total={:.1}MB tracked_large_live_bytes={:.1}MB",
            current_total as f64 / (1024.0 * 1024.0),
            tracked_bytes as f64 / (1024.0 * 1024.0)
        );
    }
    IN_PROFILER.with(|f| f.set(false));
}

fn print_snapshot(
    by_site: &HashMap<u32, (usize, usize)>,
    current_total: usize,
    tracked_bytes: usize,
    top_n: usize,
) {
    let names = sites().lock().unwrap();
    let mut entries: Vec<(u32, usize, usize)> = by_site
        .iter()
        .map(|(&id, &(count, bytes))| (id, count, bytes))
        .collect();
    entries.sort_by(|a, b| b.2.cmp(&a.2));
    eprintln!(
        "\n=== [alloc-profile] top {} call sites at peak ({:.1}MB tracked, current_total={:.1}MB, global_peak={:.1}MB) ===",
        top_n.min(entries.len()),
        tracked_bytes as f64 / (1024.0 * 1024.0),
        current_total as f64 / (1024.0 * 1024.0),
        PEAK_BYTES.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0),
    );
    for (rank, (id, count, bytes)) in entries.into_iter().take(top_n).enumerate() {
        eprintln!(
            "--- #{} : {:.1}MB across {} live allocation(s) ---\n{}",
            rank + 1,
            bytes as f64 / (1024.0 * 1024.0),
            count,
            names
                .get(id as usize)
                .map(|s| s.as_str())
                .unwrap_or("<unknown>")
        );
    }
}

/// Print the top call sites (by live bytes) as of the last recorded
/// global peak. Call this at the end of `main()` -- note this NEVER runs
/// for a process that gets SIGKILLed (see `snapshot_peak`'s own rate-limit
/// comment for how that case is still covered).
pub fn print_peak_snapshot(top_n: usize) {
    if !enabled() {
        return;
    }
    let snap = peak_snapshot_store().lock().unwrap();
    let names = sites().lock().unwrap();
    let mut entries: Vec<(u32, usize, usize)> = snap
        .iter()
        .map(|(&id, &(count, bytes))| (id, count, bytes))
        .collect();
    entries.sort_by(|a, b| b.2.cmp(&a.2));
    eprintln!(
        "\n=== [alloc-profile] FINAL top {} call sites at peak ({:.1}MB tracked, current={:.1}MB, peak={:.1}MB) ===",
        top_n.min(entries.len()),
        entries.iter().map(|e| e.2).sum::<usize>() as f64 / (1024.0 * 1024.0),
        CURRENT_BYTES.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0),
        PEAK_BYTES.load(Ordering::Relaxed) as f64 / (1024.0 * 1024.0),
    );
    for (rank, (id, count, bytes)) in entries.into_iter().take(top_n).enumerate() {
        eprintln!(
            "--- #{} : {:.1}MB across {} live allocation(s) ---\n{}",
            rank + 1,
            bytes as f64 / (1024.0 * 1024.0),
            count,
            names
                .get(id as usize)
                .map(|s| s.as_str())
                .unwrap_or("<unknown>")
        );
    }
}

/// Wraps any `GlobalAlloc` with the tracking above. Zero-cost pass-through
/// when `QE_ALLOC_PROFILE` is unset.
pub struct ProfilingAlloc<A>(pub A);

unsafe impl<A: GlobalAlloc> GlobalAlloc for ProfilingAlloc<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { self.0.alloc(layout) };
        if enabled() && !ptr.is_null() {
            record_alloc(ptr, layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if enabled() {
            record_dealloc(ptr, layout.size());
        }
        unsafe { self.0.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if enabled() {
            record_dealloc(ptr, layout.size());
        }
        let new_ptr = unsafe { self.0.realloc(ptr, layout, new_size) };
        if enabled() && !new_ptr.is_null() {
            record_alloc(new_ptr, new_size);
        }
        new_ptr
    }
}
