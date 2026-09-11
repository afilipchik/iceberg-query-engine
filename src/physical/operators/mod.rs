//! Physical operators

mod delim_join;
mod filter;
pub(crate) use filter::{classify_like, AdmittedBatchFilter};
pub mod hash_agg;
mod hash_join;
mod iceberg;
mod limit;
mod morsel_agg;
mod native_scan;
mod parquet;
mod project;
mod regex_replace;
pub mod runtime_filter;
mod scan;
mod sort;
pub mod spillable;
pub mod streaming_parquet_scan;
mod subquery;
mod substring;
mod union;
pub mod vector_search;
pub mod vectorized_hash;
mod window;

pub use delim_join::{DelimGetExec, DelimJoinExec, DelimState};
pub(crate) use filter::{evaluate_aggregate_inputs, find_column_index_in_schema, scalar_to_array};
pub use filter::{evaluate_expr, filter_batches, FilterExec};
pub use hash_agg::{AggregateExpr, HashAggregateExec};
pub use hash_join::HashJoinExec;
pub use iceberg::{IcebergScanExec, PartitionFilter};
pub use limit::LimitExec;
pub use morsel_agg::MorselAggregateExec;
pub(crate) use morsel_agg::{dense_direct_key_bounds, dense_direct_shape};
pub use native_scan::NativeStreamingScanExec;
pub use parquet::{ParquetScanExec, ParquetTable, ParquetWriter};
pub use project::ProjectExec;
pub use scan::{ColumnStatistics, MemoryTable, MemoryTableExec, TableProvider, TableStatistics};
pub use sort::SortExec;
pub use spillable::{ExternalSortExec, SpillableHashAggregateExec, SpillableHashJoinExec};
pub use streaming_parquet_scan::{SharedRuntimeFilter, StreamingParquetScanExec};
pub use subquery::{
    evaluate_subquery_expr, is_correlated_subquery_plan, run_subquery_plan, SubqueryExecutor,
};
pub use union::UnionExec;
pub use vector_search::VectorSearchExec;
pub use window::WindowExec;

pub(crate) mod closed_subquery;
pub(crate) mod initialized_membership;
pub(crate) mod int64_membership;
