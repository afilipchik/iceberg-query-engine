//! Union operator

use crate::error::Result;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream::{self, StreamExt, TryStreamExt};
use std::sync::Arc;

/// Union execution operator
///
/// Concatenates the results of multiple input operators.
/// For UNION (without ALL), a Distinct operator should be placed on top.
///
/// # Partitioning: single output, every input partition drained
///
/// This used to be `execute(_partition)` — argument ignored — calling
/// `input.execute(0)` on every input, with no `output_partitions()` override,
/// so it inherited the trait default of 1. Each branch's partitions `1..n` were
/// therefore never requested and their rows were silently dropped:
///
/// ```text
/// SELECT COUNT(*) FROM (SELECT l_orderkey FROM lineitem
///                       UNION ALL SELECT l_orderkey FROM lineitem) x
///   SF=0.1 -> 49,152     (truth 1,200,000; 49152 = 6 x 8192, i.e. partition 0)
/// ```
///
/// It looked right on `data/tpch-1mb` only because 6,000 rows fit in one batch,
/// hence one partition — which is why the setop tests were green.
///
/// **Why one output partition rather than the sum of the inputs'.** Summing
/// (`0..n0` from input 0, `n0..n0+n1` from input 1, …) is the better end state:
/// concatenation is exactly what UNION ALL means, so no redistribution is
/// needed and each branch keeps its parallelism. It was implemented and
/// measured, and it is **not correct yet**, because three callers drive only
/// partition 0 of the plan they are handed regardless of what it declares:
/// `run_subquery_blocking` (CTE materialization and every uncorrelated
/// subquery), `DelimJoinExec`, and `VectorSearchExec`'s exact fallback. Under
/// the sum version a twice-referenced CTE over UNION ALL went from 4 rows to 1
/// — trading a documented row loss for a new one. Measured on
/// `data/tpch-10mb`:
///
/// ```text
/// WITH t AS (SELECT l_orderkey FROM lineitem
///            UNION ALL SELECT l_orderkey FROM lineitem)
/// SELECT COUNT(*) FROM t a JOIN t b ON a.l_orderkey = b.l_orderkey
///  WHERE a.l_orderkey = 1
///   DuckDB 4 | sum version 1 | this version 4
/// ```
///
/// Declaring one partition is correct under *every* consumer, including those
/// three, so the fix does not depend on a bug it is not fixing. Switch to the
/// sum once those callers drain `0..output_partitions()`.
///
/// **The drain is lazy, not collected.** Partitions are opened one at a time as
/// the consumer pulls, so UNION ALL stays streaming and does not become a
/// pipeline breaker that materializes both branches — the sequential-drain
/// idiom used by `SortExec` would, and this engine's rule is that memory
/// safety is not negotiable.
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

    /// Every `(input, that input's local partition)` pair, in input order.
    ///
    /// Uses each input's own `output_partitions()`, so every local index handed
    /// back is one that input accepts — `check_partition` rejects anything else.
    fn all_input_partitions(&self) -> Vec<(Arc<dyn PhysicalOperator>, usize)> {
        let mut pairs = Vec::new();
        for input in &self.inputs {
            for p in 0..input.output_partitions() {
                pairs.push((input.clone(), p));
            }
        }
        pairs
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

    fn output_partitions(&self) -> usize {
        1
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        crate::physical::check_partition(self, partition)?;

        // Chain every input partition lazily: `then` opens a partition only
        // when the consumer has drained the previous one.
        let chained = stream::iter(self.all_input_partitions())
            .then(|(input, p)| async move { input.execute(p).await })
            .try_flatten();

        Ok(Box::pin(chained))
    }
}
