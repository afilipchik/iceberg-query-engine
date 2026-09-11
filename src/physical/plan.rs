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

/// Internal audited capability: buffers remain charged to this pool after queue
/// handoff; future decoder/scratch allocation uses the supplied pool as well.
/// Unlike PreparedOutputBound, this does not promise pool-independent polling.
pub struct PreparedAdmittedInput {
    pub(crate) pool: crate::execution::SharedMemoryPool,
    pub(crate) streams: crate::execution::reserved_vec::ReservedVec<RecordBatchStream>,
}

/// Admit both boxes and retain the lease until the inner future/stream is dropped.
pub(crate) fn admit_stream<S>(
    stream: S,
    pool: &crate::execution::MemoryPool,
) -> Result<RecordBatchStream>
where
    S: futures::Stream<Item = Result<RecordBatch>> + Send + 'static,
{
    struct Owner<S> {
        stream: std::pin::Pin<Box<S>>,
        _lease: crate::execution::MemoryReservation,
    }
    impl<S: futures::Stream> futures::Stream for Owner<S> {
        type Item = S::Item;
        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            self.get_mut().stream.as_mut().poll_next(cx)
        }
    }
    let bytes = std::mem::size_of::<S>()
        .checked_add(std::mem::size_of::<Owner<S>>())
        .and_then(|n| n.checked_add(512))
        .ok_or_else(|| crate::QueryError::Execution("stream admission extent overflow".into()))?;
    let lease = pool.allocate(bytes)?;
    Ok(Box::pin(Owner {
        stream: Box::pin(stream),
        _lease: lease,
    }))
}

/// Initialized streams with no output pre-pull. Known output bounds additionally
/// certify pool-independent future pulls. Unknown may preserve a legacy generic
/// wrapper requiring serial actual admission; it must never acquire an envelope.
/// Vec indices correspond to declared partitions; parallel output may interleave.
/// An unknown output bound retains these streams for serial actual admission.
pub struct PreparedQueueInput {
    pub streams: Vec<RecordBatchStream>,
    pub output: PreparedOutputBound,
}

#[derive(Clone, Debug)]
pub enum PreparedOutputBound {
    /// Owned lifecycle only. May include an uncertified legacy evaluator; consumers
    /// must use serial actual admission, never promote it to a known envelope.
    Unknown,
    Bytes(usize),
    Layouts(crate::physical::queue_layout::PreparedOutputLayouts),
}
impl PreparedOutputBound {
    pub fn max_bytes(&self) -> Option<usize> {
        match self {
            Self::Unknown => None,
            Self::Bytes(bytes) => Some(*bytes),
            Self::Layouts(layouts) => layouts.max_bytes(),
        }
    }
    pub(crate) fn filtered(&self) -> Self {
        match self {
            Self::Layouts(layouts) => layouts
                .filtered()
                .map(Self::Layouts)
                .unwrap_or(Self::Unknown),
            _ => Self::Unknown,
        }
    }
    pub(crate) fn projected(&self, exprs: &[crate::planner::Expr], schema: &SchemaRef) -> Self {
        match self {
            Self::Layouts(layouts) => layouts
                .projected(exprs, schema)
                .map(Self::Layouts)
                .unwrap_or(Self::Unknown),
            _ => Self::Unknown,
        }
    }
}

/// A proven value-preserving path from an output ordinal to a scan column.
/// Unknown/computed paths decline; names alone never authorize row pruning.
pub type RuntimeFilterTarget = (
    crate::physical::operators::streaming_parquet_scan::RuntimeFilterConfig,
    usize,
);

/// Physical operator trait
#[async_trait]
pub trait PhysicalOperator: Debug + Send + Sync {
    fn runtime_filter_target(&self, _output_ordinal: usize) -> Option<RuntimeFilterTarget> {
        None
    }
    /// No output pre-pull/producers. None declines without consuming input.
    /// Only audited internal producers can construct the returned descriptor.
    async fn prepare_admitted_queue_input(
        &self,
        _pool: crate::execution::SharedMemoryPool,
    ) -> Result<Option<PreparedAdmittedInput>> {
        Ok(None)
    }
    /// Opt-in preparation before a consuming queue reserves output slots.
    /// Own/cancel all initialization work; never pre-pull output or start output
    /// producers. None declines the capability without starting such producers.
    /// A returned descriptor is consumed exactly once, including serial fallback.
    async fn prepare_queue_input(&self) -> Result<Option<PreparedQueueInput>> {
        Ok(None)
    }

    /// Guaranteed copied-queue output bound for a resident pipeline whose pull
    /// does not depend on nested reservations from the same query pool. Not an
    /// upstream/RSS estimate. Unknown implementations must leave this as None.
    fn resident_queue_copy_bound(&self) -> Option<crate::physical::queue_layout::QueueCopyBound> {
        None
    }
    /// Optional actual-data repeated-take metadata for a resident pipeline.
    /// Every future pull is independent of nested same-pool reservations.
    /// Unknown operators decline; requesting ordinary queue bounds does not
    /// trigger this potentially O(rows) analysis.
    fn resident_gather_copy_bound(&self) -> Option<crate::physical::queue_layout::GatherCopyBound> {
        None
    }
    /// Guaranteed copied output from a pipeline whose future pulls do not
    /// require reservations from the consuming queue's pool. Source/decoder
    /// scratch and residency are separate; raw providers must enforce extents.
    fn pool_independent_queue_copy_bound(
        &self,
    ) -> Option<crate::physical::queue_layout::QueueCopyBound> {
        self.resident_queue_copy_bound()
    }
    /// As above, with repeated-take metadata. This does not assert residency.
    fn pool_independent_gather_copy_bound(
        &self,
    ) -> Option<crate::physical::queue_layout::GatherCopyBound> {
        self.resident_gather_copy_bound()
    }
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

    /// Execution choices omitted by the stable operator name. Diagnostics only;
    /// this must not initialize streams or inspect data.
    fn execution_details(&self) -> Option<String> {
        None
    }
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
    let mut result = format!("{}{}", "  ".repeat(indent), plan.name());
    if let Some(details) = plan.execution_details() {
        result.push(' ');
        result.push_str(&details);
    }
    result.push('\n');
    for child in plan.children() {
        result.push_str(&display_plan(child.as_ref(), indent + 1));
    }
    result
}
