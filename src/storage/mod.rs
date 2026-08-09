//! Storage providers for external data sources
//!
//! This module provides table providers for reading data from external storage:
//! - Parquet files and directories (batch and streaming)
//! - Apache Iceberg tables (see `physical::operators::iceberg`)

mod parquet;
pub mod row_group_pruning;

pub use parquet::{
    ParquetFileInfo, ParquetTable, StreamingParquetReader, StreamingParquetScanBuilder,
};

/// Lance datasets. Requires `--features lance`; see `src/storage/lance.rs`.
#[cfg(feature = "lance")]
mod lance;
#[cfg(feature = "lance")]
pub use lance::LanceTable;

/// Writing Lance datasets. Requires `--features lance`.
#[cfg(feature = "lance")]
pub mod lance_write;
#[cfg(feature = "lance")]
pub use lance_write::{LanceWriteMode, LanceWriteResult};

pub mod ipc_cache;
pub mod metadata_cache;
