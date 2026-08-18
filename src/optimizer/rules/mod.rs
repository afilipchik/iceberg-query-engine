//! Optimizer rules

mod constant_folding;
mod derive_or_predicates;
pub(crate) mod eager_aggregation;
mod flatten_dependent_join;
mod group_key_reduction;
mod having_total_cse;
mod join_reorder;
mod packed_group_keys;
mod packed_join_keys;
mod predicate_pushdown;
mod projection_pushdown;
mod semi_join_pushdown;
mod subquery_decorrelation;
mod vector_search;

pub use constant_folding::ConstantFolding;
pub use derive_or_predicates::DeriveOrPredicates;
pub use eager_aggregation::EagerAggregation;
pub use flatten_dependent_join::FlattenDependentJoin;
pub use group_key_reduction::GroupKeyReduction;
pub use having_total_cse::HavingTotalCse;
pub use join_reorder::JoinReorder;
pub use packed_group_keys::PackedGroupKeys;
pub use packed_join_keys::PackedJoinKeys;
pub use predicate_pushdown::PredicatePushdown;
pub use projection_pushdown::ProjectionPushdown;
pub use semi_join_pushdown::SemiJoinPushdown;
pub use subquery_decorrelation::SubqueryDecorrelation;
pub use vector_search::VectorSearchPushdown;
