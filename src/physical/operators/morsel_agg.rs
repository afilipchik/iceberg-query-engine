//! Morsel-driven aggregate operator
//!
//! This operator implements parallel aggregation using morsel-driven parallelism
//! for Parquet data sources. It provides significant performance improvements over
//! the standard HashAggregateExec by:
//! - Reading Parquet files in parallel across row groups
//! - Using thread-local hash tables to avoid contention
//! - Final merge of all thread-local states

use crate::error::{QueryError, Result};
use crate::physical::morsel::{ParallelParquetSource, DEFAULT_MORSEL_SIZE};
use crate::physical::morsel_agg::AggregationState;
use crate::physical::operators::evaluate_expr;
use crate::physical::operators::hash_agg::AggregateExpr;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::{AggregateFunction, Expr};
use arrow::array::{Array, ArrayRef, BooleanArray};
use arrow::compute;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;
use rayon::prelude::*;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

/// Morsel-driven aggregate execution operator
///
/// This operator is used for aggregations over Parquet data sources.
/// It uses parallel row-group reading and thread-local hash tables
/// for optimal performance.
pub struct MorselAggregateExec {
    /// Parquet file paths to read from
    files: Vec<PathBuf>,
    /// Columns to project from Parquet (indices)
    projection: Option<Vec<usize>>,
    /// Optional filter to apply before aggregation
    filter: Option<Expr>,
    /// Group by expressions
    group_by: Vec<Expr>,
    /// Aggregate expressions
    aggregates: Vec<AggregateExpr>,
    /// Output schema
    schema: SchemaRef,
    /// Input schema from Parquet files
    input_schema: SchemaRef,
    /// HAVING predicate applied per output batch (references output columns)
    post_filter: Option<Expr>,
}

impl fmt::Debug for MorselAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MorselAggregateExec")
            .field("files", &self.files.len())
            .field("group_by", &self.group_by)
            .field("schema", &self.schema)
            .finish()
    }
}

impl MorselAggregateExec {
    /// Create a new MorselAggregateExec
    pub fn new(
        files: Vec<PathBuf>,
        input_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filter: Option<Expr>,
        group_by: Vec<Expr>,
        aggregates: Vec<AggregateExpr>,
        schema: SchemaRef,
    ) -> Self {
        Self {
            files,
            projection,
            filter,
            group_by,
            aggregates,
            schema,
            input_schema,
            post_filter: None,
        }
    }

    /// Attach a HAVING predicate applied to output batches before returning.
    pub fn with_post_filter(mut self, post_filter: Option<Expr>) -> Self {
        self.post_filter = post_filter;
        self
    }
}

#[async_trait]
impl PhysicalOperator for MorselAggregateExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![] // MorselAggregateExec reads directly from Parquet
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        // Morsel aggregate produces a single partition
        if partition != 0 {
            return Ok(Box::pin(stream::empty()));
        }

        // Create the parallel Parquet source with row group pruning
        let source = ParallelParquetSource::try_new_with_filter(
            self.files.clone(),
            self.input_schema.clone(),
            self.projection.clone(),
            DEFAULT_MORSEL_SIZE,
            self.filter.as_ref(),
        )?;

        // Determine input types for aggregates
        let plan_schema = crate::planner::PlanSchema::from(source.schema().as_ref());
        let input_types: Vec<DataType> = self
            .aggregates
            .iter()
            .map(|a| a.input.data_type(&plan_schema).unwrap_or(DataType::Float64))
            .collect();

        let num_threads = rayon::current_num_threads();

        // Clone expressions for use in parallel closure
        let group_by_exprs = self.group_by.clone();
        let agg_input_exprs: Vec<Expr> = self.aggregates.iter().map(|a| a.input.clone()).collect();
        let agg_funcs: Vec<AggregateFunction> = self.aggregates.iter().map(|a| a.func).collect();
        // When the filter was pushed into the parquet decoder (RowFilter),
        // re-applying it here would be wasted work.
        let filter_expr = if source.filter_pushed_down() {
            None
        } else {
            self.filter.clone()
        };
        let output_schema = self.schema.clone();

        let timing = std::env::var("AGG_TIMING").is_ok();
        let t0 = std::time::Instant::now();
        // Execute in parallel - each thread processes morsels and maintains its own hash table
        let thread_states: Vec<Result<AggregationState>> = (0..num_threads)
            .into_par_iter()
            .map(|_thread_id| {
                let mut state = AggregationState::new(agg_funcs.clone(), input_types.clone());

                // Keep processing morsels from the source
                while let Some(work) = source.get_work() {
                    let batches = source.read_row_group(&work)?;

                    for batch in batches {
                        // Apply filter if present
                        let filtered_batch = if let Some(ref filter) = filter_expr {
                            let filter_result = evaluate_expr(&batch, filter)?;
                            let filter_array = filter_result
                                .as_any()
                                .downcast_ref::<BooleanArray>()
                                .ok_or_else(|| {
                                    QueryError::Execution("Filter must return boolean".to_string())
                                })?;

                            // Use arrow's filter kernel
                            let filtered_columns: Vec<ArrayRef> = batch
                                .columns()
                                .iter()
                                .map(|col| compute::filter(col.as_ref(), filter_array))
                                .collect::<std::result::Result<Vec<_>, _>>()
                                .map_err(|e| {
                                    QueryError::Execution(format!("Filter failed: {}", e))
                                })?;

                            if filtered_columns.is_empty() || filtered_columns[0].len() == 0 {
                                continue;
                            }

                            RecordBatch::try_new(batch.schema(), filtered_columns).map_err(|e| {
                                QueryError::Execution(format!(
                                    "Failed to create filtered batch: {}",
                                    e
                                ))
                            })?
                        } else {
                            batch
                        };

                        // Process the batch
                        state.process_batch(&filtered_batch, &group_by_exprs, &agg_input_exprs)?;
                    }

                    source.complete_work();
                }

                Ok(state)
            })
            .collect();

        let states: Vec<AggregationState> =
            thread_states.into_iter().collect::<Result<Vec<_>>>()?;
        if timing {
            let groups: usize = states.iter().map(|s| s.group_count()).sum();
            eprintln!(
                "[AGG_TIMING] morsel scan+process: {:?} ({} thread-groups)",
                t0.elapsed(),
                groups
            );
        }
        let t1 = std::time::Instant::now();

        // Shared merge: parallel shard merge above 64K groups (raw-u64 pipeline
        // for single integer group columns), sequential fold below.
        let batches = crate::physical::morsel_agg::merge_states_to_batches_filtered(
            states,
            &agg_funcs,
            &input_types,
            &output_schema,
            self.post_filter.as_ref(),
        )?;
        if timing {
            eprintln!(
                "[AGG_TIMING] morsel merge+output+having: {:?}",
                t1.elapsed()
            );
        }
        Ok(Box::pin(stream::iter(batches.into_iter().map(Ok))))
    }

    fn name(&self) -> &str {
        "MorselAggregate"
    }

    fn output_partitions(&self) -> usize {
        // Aggregation produces a single partition (all groups combined)
        1
    }
}

impl fmt::Display for MorselAggregateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let groups: Vec<String> = self.group_by.iter().map(|e| e.to_string()).collect();
        let aggs: Vec<String> = self
            .aggregates
            .iter()
            .map(|a| format!("{}({})", a.func, a.input))
            .collect();
        write!(
            f,
            "MorselAggregate: files={}, group_by=[{}], aggs=[{}]",
            self.files.len(),
            groups.join(", "),
            aggs.join(", ")
        )
    }
}
