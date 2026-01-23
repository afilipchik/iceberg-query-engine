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

pub use filter::{FilterExec, evaluate_expr};
pub use hash_agg::{HashAggregateExec, AggregateExpr};
pub use hash_join::HashJoinExec;
pub use iceberg::{IcebergScanExec, PartitionFilter};
pub use limit::LimitExec;
pub use parquet::{ParquetScanExec, ParquetTable, ParquetWriter};
pub use project::ProjectExec;
pub use scan::{MemoryTable, MemoryTableExec, TableProvider};
pub use sort::SortExec;
pub use subquery::{SubqueryExecutor, evaluate_subquery_expr};
