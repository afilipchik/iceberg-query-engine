//! High-performance SQL Query Engine
//!
//! A custom SQL query engine built from scratch for Apache Iceberg,
//! targeting top performance on TPC-H benchmarks.

/// Global allocator: mimalloc. The engine frees tens of millions of small
/// allocations at pipeline boundaries (per-group accumulators, batch arrays);
/// glibc malloc serializes those frees behind arena locks and munmap
/// consolidation stalls (measured 550ms on a single Q18 aggregate teardown).
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

pub mod arrow_ffi;
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
pub use storage::ParquetTable;
