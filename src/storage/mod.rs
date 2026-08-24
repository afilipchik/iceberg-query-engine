//! Storage providers for external data sources
//!
//! This module provides table providers for reading data from external storage:
//! - Parquet files and directories (batch and streaming)
//! - Apache Iceberg tables (`storage::iceberg`): spec metadata + Avro
//!   manifests resolved to a `ParquetTable`

pub mod iceberg;
mod parquet;
pub mod row_group_pruning;

pub use iceberg::{open_table as open_iceberg_table, IcebergTable};
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

/// Apache Pulsar topics as tables. Requires `--features pulsar`.
#[cfg(feature = "pulsar")]
pub mod pulsar;
#[cfg(feature = "pulsar")]
pub use pulsar::{register_pulsar_namespace, PulsarSource, PulsarTable};

pub mod ipc_cache;
pub mod metadata_cache;

/// Native table manifest (identity/versioning/statistics format). Task 002
/// of the native-tables-foundation epic; sibling to `native_write.rs` (003)
/// and `native_table.rs` (004).
pub mod native_manifest;

/// Native table write path (bulk-load from parquet/Iceberg/Lance/query
/// results). Task 003 of the native-tables-foundation epic; calls into
/// `native_manifest` (002) for the format. A module is otherwise never
/// compiled, so this registration line is unavoidable.
pub mod native_write;

/// Native table `TableProvider` (read/registration/distributed splits).
/// Task 004 of the native-tables-foundation epic; calls into
/// `native_manifest` (004) for the format and `ipc_cache` for segment
/// reads. A module is otherwise never compiled, so this registration line
/// is unavoidable — same reasoning as `native_manifest`'s own line above.
pub mod native_table;
pub use native_table::NativeTable;

/// Native table DELETE (row-identification + deletion-vector editing).
/// Task 003 of the native-tables-mutation epic; calls into
/// `native_manifest` for the `Segment::deleted_rows` field and
/// `native_write` for the single-writer lock and atomic-publish
/// primitives it reuses unchanged. A module is otherwise never compiled,
/// so this registration line is unavoidable — same reasoning as
/// `native_manifest`'s own line above.
pub mod native_delete;
