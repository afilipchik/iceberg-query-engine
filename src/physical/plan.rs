//! Physical plan types

use crate::error::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream::BoxStream;
use std::fmt::Debug;
use std::sync::Arc;

/// Stream of record batches
pub type RecordBatchStream = BoxStream<'static, Result<RecordBatch>>;

/// Physical operator trait
#[async_trait]
pub trait PhysicalOperator: Debug + Send + Sync {
    /// Get the output schema
    fn schema(&self) -> SchemaRef;

    /// Get child operators
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>>;

    /// Execute the operator and return a stream of batches
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream>;

    /// Number of output partitions
    fn output_partitions(&self) -> usize {
        1
    }

    /// Name of this operator for display
    fn name(&self) -> &str;
}

/// Reject an `execute(partition)` call whose index lies outside the operator's
/// own `output_partitions()`.
///
/// **Every `PhysicalOperator::execute` implementation must call this first.**
///
/// `output_partitions()` is an advisory integer: a parent drives
/// `0..child.output_partitions()` and each operator decides for itself how many
/// partitions it produces. Before this guard existed, an out-of-range partition
/// index returned an *empty stream* in every single implementation, so any
/// disagreement between a parent's loop bound and a child's declared count was
/// never an error — it was silently a wrong row count. That is the shared root
/// cause of three shipped wrong-answer bugs: `UnionExec` (declared 1, drained
/// only partition 0 of each input), `LimitExec` (declared 1 while forwarding
/// `partition` to a multi-partition child), and the `HashJoinExec` Left +
/// `build_right` case where `output_partitions()` returned the build side's
/// count so probe partitions were never requested.
///
/// Turning the disagreement into a loud `Internal` error is the whole point:
/// a partition-count contract that is only checked by the row count of a
/// TPC-H query is not checked at all. Do not "helpfully" restore the empty
/// stream — an operator that legitimately produces nothing for a partition it
/// *declared* (e.g. `SpillableHashJoinExec`'s spill path, which funnels all
/// work through partition 0) returns `stream::empty()` **after** this guard,
/// not instead of it.
pub fn check_partition(op: &dyn PhysicalOperator, partition: usize) -> Result<()> {
    let declared = op.output_partitions();
    if partition >= declared {
        return Err(crate::error::QueryError::Internal(format!(
            "{}: partition {} out of range (output_partitions={})",
            op.name(),
            partition,
            declared
        )));
    }
    Ok(())
}

/// Display helper for physical plans
pub fn display_plan(plan: &dyn PhysicalOperator, indent: usize) -> String {
    let mut result = format!("{}{}\n", "  ".repeat(indent), plan.name());
    for child in plan.children() {
        result.push_str(&display_plan(child.as_ref(), indent + 1));
    }
    result
}
