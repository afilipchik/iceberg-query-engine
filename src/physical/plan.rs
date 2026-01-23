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

/// Display helper for physical plans
pub fn display_plan(plan: &dyn PhysicalOperator, indent: usize) -> String {
    let mut result = format!("{}{}\n", "  ".repeat(indent), plan.name());
    for child in plan.children() {
        result.push_str(&display_plan(child.as_ref(), indent + 1));
    }
    result
}
