//! Physical operators

mod filter;
mod hash_agg;
mod hash_join;
mod iceberg;
mod limit;
mod parquet;
mod project;
mod scan;
mod sort;
mod subquery;
mod union;

pub use filter::{evaluate_expr, FilterExec};
pub use hash_agg::{AggregateExpr, HashAggregateExec};
pub use hash_join::HashJoinExec;
pub use iceberg::{IcebergScanExec, PartitionFilter};
pub use limit::LimitExec;
pub use parquet::{ParquetScanExec, ParquetTable, ParquetWriter};
pub use project::ProjectExec;
pub use scan::{MemoryTable, MemoryTableExec, TableProvider};
pub use sort::SortExec;
pub use subquery::{evaluate_subquery_expr, SubqueryExecutor};
pub use union::UnionExec;
