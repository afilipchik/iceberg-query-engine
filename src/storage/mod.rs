//! Storage providers for external data sources
//!
//! This module provides table providers for reading data from external storage:
//! - Parquet files and directories
//! - Apache Iceberg tables (planned)

mod parquet;

pub use parquet::ParquetTable;
