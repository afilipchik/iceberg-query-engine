//! High-performance SQL Query Engine
//!
//! A custom SQL query engine built from scratch for Apache Iceberg,
//! targeting top performance on TPC-H benchmarks.

/// Global allocator: mimalloc. The engine frees tens of millions of small
/// allocations at pipeline boundaries (per-group accumulators, batch arrays);
/// glibc malloc serializes those frees behind arena locks and munmap
/// consolidation stalls (measured 550ms on a single Q18 aggregate teardown).
///
/// TEMPORARY (`oom-safety-hardening` investigation, 2026-08-29): wrapped in
/// `execution::alloc_profile::ProfilingAlloc`, a zero-cost-when-disabled
/// diagnostic layer (see that module's own doc comment) used because no
/// external profiler (heaptrack/valgrind/perf) is usable on this box
/// without root. Revert to bare `mimalloc::MiMalloc` once the
/// investigation concludes.
#[global_allocator]
static GLOBAL: execution::alloc_profile::ProfilingAlloc<mimalloc::MiMalloc> =
    execution::alloc_profile::ProfilingAlloc(mimalloc::MiMalloc);

pub mod arrow_ffi;
pub mod distributed;
pub mod error;
pub mod execution;
pub mod metastore;
pub mod optimizer;
pub mod parser;
pub mod physical;
pub mod planner;
pub mod storage;
pub mod tpch;

// Re-export main types
pub use arrow_ffi::*;
pub use error::{QueryError, Result};
pub use execution::{ExecutionConfig, ExecutionContext, QueryResult};
pub use metastore::{BranchingMetastoreClient, MetastoreCatalog};
pub use planner::{Binder, InMemoryCatalog, LogicalPlan};
#[cfg(feature = "lance")]
pub use storage::LanceTable;
pub use storage::ParquetTable;
