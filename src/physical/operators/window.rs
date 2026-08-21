//! Window function execution.
//!
//! `WindowExec` computes each window expression independently: sort a
//! permutation of the input by (partition keys, order keys), evaluate the
//! function over each partition in sorted order, then scatter the results
//! back to the ORIGINAL row order. Output = every input column unchanged, in
//! input order, plus one appended column per window expression — which is
//! exactly the contract the binder's Window node declares, and it means
//! window expressions with different PARTITION BY / ORDER BY specs never
//! constrain each other.
//!
//! v1 executes on a single partition (window queries are a correctness
//! feature first; the morsel treatment is a later perf epic) and supports the
//! function set the epic has reached — everything else is refused BY NAME at
//! plan time, never silently mis-evaluated.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, UInt32Array};
use arrow::compute::{self, SortColumn, SortOptions};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream;

use crate::error::{QueryError, Result};
use crate::physical::plan::{PhysicalOperator, RecordBatchStream};
use crate::planner::{NullOrdering, SortDirection};
use crate::planner::{WindowExpr, WindowFunc};

use super::filter::evaluate_expr;

/// Physical window operator. See the module docs for the evaluation model.
pub struct WindowExec {
    input: Arc<dyn PhysicalOperator>,
    /// (output column name, window expression), appended in this order.
    window_exprs: Vec<(String, WindowExpr)>,
    schema: SchemaRef,
}

impl WindowExec {
    pub fn try_new(
        input: Arc<dyn PhysicalOperator>,
        window_exprs: Vec<(String, WindowExpr)>,
        schema: SchemaRef,
    ) -> Result<Self> {
        // Refuse unsupported functions at plan time, by name.
        for (_, w) in &window_exprs {
            match &w.func {
                WindowFunc::RowNumber => {}
                other => {
                    return Err(QueryError::NotImplemented(format!(
                        "window function {other} OVER (...)"
                    )))
                }
            }
        }
        Ok(Self {
            input,
            window_exprs,
            schema,
        })
    }

    /// Evaluate one window expression against the whole input, returning the
    /// result column in ORIGINAL row order.
    fn evaluate_window(batch: &RecordBatch, w: &WindowExpr) -> Result<ArrayRef> {
        let n = batch.num_rows();

        // Sort permutation: partition keys first (ascending, arbitrary but
        // consistent), then the window's ORDER BY keys.
        let mut sort_columns: Vec<SortColumn> = Vec::new();
        let mut partition_arrays: Vec<ArrayRef> = Vec::new();
        for p in &w.partition_by {
            let values = evaluate_expr(batch, p)?;
            partition_arrays.push(values.clone());
            sort_columns.push(SortColumn {
                values,
                options: Some(SortOptions {
                    descending: false,
                    nulls_first: true,
                }),
            });
        }
        for o in &w.order_by {
            let values = evaluate_expr(batch, &o.expr)?;
            sort_columns.push(SortColumn {
                values,
                options: Some(SortOptions {
                    descending: o.direction == SortDirection::Desc,
                    nulls_first: matches!(o.nulls, NullOrdering::NullsFirst),
                }),
            });
        }

        // indices[i] = original row at sorted position i. With no keys at
        // all (OVER ()), the identity permutation avoids a useless sort.
        let indices: UInt32Array = if sort_columns.is_empty() {
            (0..n as u32).collect::<Vec<_>>().into()
        } else {
            compute::lexsort_to_indices(&sort_columns, None)?
        };

        // Partition ranges over the SORTED order. arrow's partition kernel
        // wants the sorted key columns themselves.
        let partition_ranges: Vec<std::ops::Range<usize>> = if partition_arrays.is_empty() {
            vec![0..n]
        } else {
            let sorted_keys: Vec<ArrayRef> = partition_arrays
                .iter()
                .map(|a| compute::take(a.as_ref(), &indices, None).map_err(Into::into))
                .collect::<Result<_>>()?;
            arrow::compute::kernels::partition::partition(&sorted_keys)?
                .ranges()
                .to_vec()
        };

        // Evaluate in sorted order, then scatter back: out[indices[i]] = v[i].
        let sorted_values = Self::evaluate_sorted(w, n, &partition_ranges)?;
        let mut inverse = vec![0u32; n];
        for (sorted_pos, orig) in indices.values().iter().enumerate() {
            inverse[*orig as usize] = sorted_pos as u32;
        }
        let inverse = UInt32Array::from(inverse);
        compute::take(sorted_values.as_ref(), &inverse, None).map_err(Into::into)
    }

    /// Compute the window function over each partition range of the sorted
    /// input, producing values in SORTED order.
    fn evaluate_sorted(
        w: &WindowExpr,
        n: usize,
        partitions: &[std::ops::Range<usize>],
    ) -> Result<ArrayRef> {
        match &w.func {
            WindowFunc::RowNumber => {
                let mut out = vec![0i64; n];
                for range in partitions {
                    for (i, slot) in out[range.clone()].iter_mut().enumerate() {
                        *slot = (i + 1) as i64;
                    }
                }
                Ok(Arc::new(Int64Array::from(out)))
            }
            other => Err(QueryError::NotImplemented(format!(
                "window function {other} OVER (...)"
            ))),
        }
    }
}

impl std::fmt::Debug for WindowExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WindowExec [{}]",
            self.window_exprs
                .iter()
                .map(|(n, w)| format!("{n}: {w}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[async_trait]
impl PhysicalOperator for WindowExec {
    fn name(&self) -> &str {
        "WindowExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.input.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        crate::physical::check_partition(self, partition)?;

        let (all_batches, _) =
            crate::physical::operators::spillable::collect_input_partitions_concurrently(
                &self.input,
            )
            .await?;

        if all_batches.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        // One contiguous batch; dictionary-encoded columns are normalized to
        // plain when batches disagree (same situation SortExec handles — the
        // declared schema is what the plan promised upward).
        let input_arrow_fields =
            self.schema.fields()[..self.schema.fields().len() - self.window_exprs.len()].to_vec();
        let input_schema = Arc::new(arrow::datatypes::Schema::new(input_arrow_fields));
        let normalized: Vec<RecordBatch> = all_batches
            .into_iter()
            .map(|b| {
                let cols: std::result::Result<Vec<ArrayRef>, arrow::error::ArrowError> = b
                    .columns()
                    .iter()
                    .map(|c| match c.data_type() {
                        arrow::datatypes::DataType::Dictionary(_, v) => {
                            compute::cast(c.as_ref(), v)
                        }
                        _ => Ok(c.clone()),
                    })
                    .collect();
                RecordBatch::try_new(input_schema.clone(), cols?).map_err(Into::into)
            })
            .collect::<Result<Vec<_>>>()?;
        let batch = if normalized.len() == 1 {
            normalized.into_iter().next().expect("checked")
        } else {
            compute::concat_batches(&input_schema, &normalized)?
        };

        let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
        for (_, w) in &self.window_exprs {
            columns.push(Self::evaluate_window(&batch, w)?);
        }
        let out = RecordBatch::try_new(self.schema.clone(), columns)?;
        Ok(Box::pin(stream::iter(vec![Ok(out)])))
    }
}
