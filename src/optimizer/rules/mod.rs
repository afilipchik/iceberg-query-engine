//! Optimizer rules

mod constant_folding;
mod derive_or_predicates;
mod flatten_dependent_join;
mod join_reorder;
mod predicate_pushdown;
mod projection_pushdown;
mod semi_join_pushdown;
mod subquery_decorrelation;

pub use constant_folding::ConstantFolding;
pub use derive_or_predicates::DeriveOrPredicates;
pub use flatten_dependent_join::FlattenDependentJoin;
pub use join_reorder::JoinReorder;
pub use predicate_pushdown::PredicatePushdown;
pub use projection_pushdown::ProjectionPushdown;
pub use semi_join_pushdown::SemiJoinPushdown;
pub use subquery_decorrelation::SubqueryDecorrelation;
