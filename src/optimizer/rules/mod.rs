//! Optimizer rules

mod constant_folding;
mod predicate_pushdown;
mod projection_pushdown;

pub use constant_folding::ConstantFolding;
pub use predicate_pushdown::PredicatePushdown;
pub use projection_pushdown::ProjectionPushdown;
