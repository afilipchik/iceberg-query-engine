//! Physical operators

mod delim_join;
mod filter;
pub mod hash_agg;
mod hash_join;
mod iceberg;
mod limit;
mod morsel_agg;
mod parquet;
mod project;
mod scan;
mod sort;
pub mod spillable;
mod streaming_parquet_scan;
mod subquery;
mod union;
pub mod vectorized_hash;

pub use delim_join::{DelimGetExec, DelimJoinExec, DelimState};
pub use filter::{evaluate_expr, FilterExec};
pub use hash_agg::{AggregateExpr, HashAggregateExec};
pub use hash_join::HashJoinExec;
pub use iceberg::{IcebergScanExec, PartitionFilter};
pub use limit::LimitExec;
pub use morsel_agg::MorselAggregateExec;
pub use parquet::{ParquetScanExec, ParquetTable, ParquetWriter};
pub use project::ProjectExec;
pub use scan::{MemoryTable, MemoryTableExec, TableProvider, TableStatistics};
pub use sort::SortExec;
pub use spillable::{ExternalSortExec, SpillableHashAggregateExec, SpillableHashJoinExec};
pub use streaming_parquet_scan::StreamingParquetScanExec;
pub use subquery::{evaluate_subquery_expr, SubqueryExecutor};
pub use union::UnionExec;
