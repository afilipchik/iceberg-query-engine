//! Physical operators

mod filter;
mod hash_agg;
mod hash_join;
mod limit;
mod project;
mod scan;
mod sort;

pub use filter::{FilterExec, evaluate_expr};
pub use hash_agg::{HashAggregateExec, AggregateExpr};
pub use hash_join::HashJoinExec;
pub use limit::LimitExec;
pub use project::ProjectExec;
pub use scan::{MemoryTable, MemoryTableExec, TableProvider};
pub use sort::SortExec;
