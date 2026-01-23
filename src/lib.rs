//! High-performance SQL Query Engine
//!
//! A custom SQL query engine built from scratch for Apache Iceberg,
//! targeting top performance on TPC-H benchmarks.

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
pub use execution::{ExecutionContext, QueryResult};
pub use metastore::{BranchingMetastoreClient, MetastoreCatalog};
pub use planner::{Binder, InMemoryCatalog, LogicalPlan};
pub use storage::ParquetTable;
