//! Union operator

use crate::error::Result;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::fmt;
use std::sync::Arc;

/// Union execution operator
///
/// Concatenates the results of multiple input operators.
/// For UNION (without ALL), a Distinct operator should be placed on top.
#[derive(Debug)]
pub struct UnionExec {
    inputs: Vec<Arc<dyn PhysicalOperator>>,
    schema: SchemaRef,
}

impl UnionExec {
    pub fn new(inputs: Vec<Arc<dyn PhysicalOperator>>) -> Self {
        let schema = if inputs.is_empty() {
            Arc::new(arrow::datatypes::Schema::empty())
        } else {
            inputs[0].schema()
        };
        Self { inputs, schema }
    }
}

#[async_trait]
impl PhysicalOperator for UnionExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        self.inputs.clone()
    }

    fn name(&self) -> &str {
        "UnionExec"
    }

    async fn execute(&self, _partition: usize) -> Result<RecordBatchStream> {
        // Collect all input streams and chain them together
        let mut streams = Vec::new();
        for input in &self.inputs {
            streams.push(input.execute(0).await?);
        }

        // Create a stream that yields from each input in sequence
        let chained = stream::iter(streams)
            .flat_map(|s| s);

        Ok(Box::pin(chained))
    }
}
