//! Storage providers for external data sources
//!
//! This module provides table providers for reading data from external storage:
//! - Parquet files and directories (batch and streaming)
//! - Apache Iceberg tables (see `physical::operators::iceberg`)

mod parquet;

pub use parquet::{
    ParquetFileInfo, ParquetTable, StreamingParquetReader, StreamingParquetScanBuilder,
};
