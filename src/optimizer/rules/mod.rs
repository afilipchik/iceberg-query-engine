//! Optimizer rules

mod constant_folding;
mod predicate_pushdown;
mod projection_pushdown;
mod join_reordering;
mod predicate_reordering;

pub use constant_folding::ConstantFolding;
pub use predicate_pushdown::PredicatePushdown;
pub use projection_pushdown::ProjectionPushdown;
pub use join_reordering::JoinReordering;
pub use predicate_reordering::PredicateReordering;
