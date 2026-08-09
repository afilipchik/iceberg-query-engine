//! Top-k vector search operator.
//!
//! # The one decision this operator exists to make
//!
//! `ORDER BY cosine_distance(embedding, [...]) LIMIT 10` has exactly one
//! correct answer: the 10 rows with the smallest distance. A vector index
//! (IVF_PQ, HNSW) answers a *different* question — "10 rows that are probably
//! among the closest" — very much faster.
//!
//! This operator holds both:
//!
//! * `fallback` is the unmodified exact plan the optimizer replaced. Running it
//!   reproduces the original query bit-for-bit.
//! * `provider.scan_knn(..)` is the index path, used only when the session has
//!   asked for [`VectorSearchMode::Indexed`] AND the provider says it can serve
//!   the request faithfully (right column, right dimension, prefilter
//!   expressible). Anything else falls back.
//!
//! The engine therefore never *silently* substitutes approximate results for
//! exact ones: the substitution requires a configuration setting whose whole
//! documented purpose is to permit it.

use crate::error::{QueryError, Result};
use crate::execution::{ExecutionConfig, VectorSearchMode};
use crate::physical::operators::TableProvider;
use crate::physical::vector::{VectorMetric, VectorQuery};
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::{Expr, SchemaField};
use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{Schema as ArrowSchema, SchemaRef};
use async_trait::async_trait;
use std::fmt;
use std::sync::Arc;

/// Lance names its computed distance column `_distance`.
///
/// It is dropped from the output. A `SELECT id, text` that suddenly grew a
/// third column because an index happened to be used would be a schema change
/// caused by a physical plan choice, which is exactly the kind of leak the
/// pushdown is not allowed to produce.
const DIST_COL: &str = "_distance";

pub struct VectorSearchExec {
    /// Exact plan; the source of truth.
    fallback: Arc<dyn PhysicalOperator>,
    /// Provider for the table under the search, if it is registered.
    provider: Option<Arc<dyn TableProvider>>,
    /// Column indices into the provider's schema, in output order.
    projection: Vec<usize>,
    /// Output fields, in output order.
    outputs: Vec<SchemaField>,
    /// Vector column name, as the provider knows it.
    column: String,
    query: Vec<f32>,
    k: usize,
    skip: usize,
    metric: VectorMetric,
    filter: Option<Expr>,
    schema: SchemaRef,
    config: ExecutionConfig,
}

impl fmt::Debug for VectorSearchExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorSearchExec")
            .field("column", &self.column)
            .field("dim", &self.query.len())
            .field("k", &self.k)
            .field("skip", &self.skip)
            .field("metric", &self.metric)
            .field("mode", &self.config.vector_search_mode)
            .field("has_provider", &self.provider.is_some())
            .finish()
    }
}

impl VectorSearchExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        fallback: Arc<dyn PhysicalOperator>,
        provider: Option<Arc<dyn TableProvider>>,
        projection: Vec<usize>,
        outputs: Vec<SchemaField>,
        column: String,
        query: Vec<f32>,
        k: usize,
        skip: usize,
        metric: VectorMetric,
        filter: Option<Expr>,
        schema: SchemaRef,
        config: ExecutionConfig,
    ) -> Self {
        Self {
            fallback,
            provider,
            projection,
            outputs,
            column,
            query,
            k,
            skip,
            metric,
            filter,
            schema,
            config,
        }
    }

    /// Attempt the index path. `Ok(None)` means "use the fallback".
    fn try_index(&self) -> Result<Option<Vec<RecordBatch>>> {
        if self.config.vector_search_mode != VectorSearchMode::Indexed {
            return Ok(None);
        }
        let Some(provider) = &self.provider else {
            return Ok(None);
        };
        // With OFFSET n LIMIT k the query needs the first n + k neighbours.
        let Some(want) = self.skip.checked_add(self.k) else {
            return Ok(None);
        };

        let q = VectorQuery {
            column: self.column.clone(),
            query: self.query.clone(),
            k: want,
            metric: self.metric,
            filter: self.filter.clone(),
            use_index: true,
            nprobes: self.config.vector_nprobes,
            refine_factor: self.config.vector_refine_factor,
        };

        let Some(batches) = provider.scan_knn(Some(&self.projection), &q)? else {
            return Ok(None);
        };
        Ok(Some(self.shape_output(batches)?))
    }

    /// Rename/reorder the provider's columns into this node's output schema,
    /// drop `_distance`, and apply OFFSET/LIMIT.
    fn shape_output(&self, batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let mut out = Vec::with_capacity(batches.len());
        let mut skipped = 0usize;
        let mut taken = 0usize;

        for batch in batches {
            if taken >= self.k {
                break;
            }
            let in_schema = batch.schema();
            let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.outputs.len());
            for field in &self.outputs {
                // Match by name: Lance returns the projected columns plus
                // `_distance`, in its own order.
                let idx = in_schema
                    .fields()
                    .iter()
                    .position(|f| f.name().eq_ignore_ascii_case(&field.name))
                    .ok_or_else(|| {
                        QueryError::Internal(format!(
                            "vector search result is missing column `{}`; got [{}]",
                            field.name,
                            in_schema
                                .fields()
                                .iter()
                                .map(|f| f.name().as_str())
                                .collect::<Vec<_>>()
                                .join(", ")
                        ))
                    })?;
                let col = batch.column(idx);
                // Guard against the type drifting under us (e.g. a provider
                // handing back Float32 where the plan promised Float64).
                if col.data_type() != &field.data_type {
                    return Err(QueryError::Internal(format!(
                        "vector search returned column `{}` as {:?}, plan expects {:?}",
                        field.name,
                        col.data_type(),
                        field.data_type
                    )));
                }
                columns.push(col.clone());
            }

            let mut b = RecordBatch::try_new(self.schema.clone(), columns)?;

            // OFFSET.
            if skipped < self.skip {
                let drop = (self.skip - skipped).min(b.num_rows());
                skipped += drop;
                if drop == b.num_rows() {
                    continue;
                }
                b = b.slice(drop, b.num_rows() - drop);
            }
            // LIMIT.
            if taken + b.num_rows() > self.k {
                b = b.slice(0, self.k - taken);
            }
            taken += b.num_rows();
            out.push(b);
        }
        Ok(out)
    }
}

#[async_trait]
impl PhysicalOperator for VectorSearchExec {
    fn name(&self) -> &str {
        "VectorSearchExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.fallback.clone()]
    }

    fn output_partitions(&self) -> usize {
        // Top-k is a global result; producing it from several partitions would
        // return k rows per partition.
        1
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        if partition != 0 {
            return Ok(Box::pin(futures::stream::iter(
                Vec::<Result<RecordBatch>>::new(),
            )));
        }

        if let Some(batches) = self.try_index()? {
            let results: Vec<Result<RecordBatch>> = batches.into_iter().map(Ok).collect();
            return Ok(Box::pin(futures::stream::iter(results)));
        }

        // Exact path: the plan the optimizer replaced, run verbatim.
        self.fallback.execute(0).await
    }
}

/// Drop `_distance` if a provider ever surfaces it in a projection.
pub(crate) fn without_distance_column(schema: &ArrowSchema) -> Vec<usize> {
    schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(_, f)| f.name() != DIST_COL)
        .map(|(i, _)| i)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field};

    fn batch(ids: &[i64], with_distance: bool) -> RecordBatch {
        let mut fields = vec![
            Field::new("id", DataType::Int64, true),
            Field::new("category", DataType::Utf8, true),
        ];
        let mut cols: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(ids.to_vec())),
            Arc::new(StringArray::from(vec!["a"; ids.len()])),
        ];
        if with_distance {
            fields.push(Field::new(DIST_COL, DataType::Float32, true));
            cols.push(Arc::new(arrow::array::Float32Array::from(vec![
                0.5f32;
                ids.len()
            ])));
        }
        RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), cols).unwrap()
    }

    fn exec_for(k: usize, skip: usize) -> VectorSearchExec {
        let outputs = vec![
            SchemaField::new("id", DataType::Int64),
            SchemaField::new("category", DataType::Utf8),
        ];
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("category", DataType::Utf8, true),
        ]));
        VectorSearchExec {
            fallback: Arc::new(crate::physical::operators::MemoryTableExec::new(
                "vectors",
                schema.clone(),
                vec![],
                None,
            )),
            provider: None,
            projection: vec![0, 1],
            outputs,
            column: "embedding".into(),
            query: vec![0.0; 4],
            k,
            skip,
            metric: VectorMetric::Cosine,
            filter: None,
            schema,
            config: ExecutionConfig::default(),
        }
    }

    #[test]
    fn distance_column_is_dropped_not_leaked() {
        let e = exec_for(10, 0);
        let out = e.shape_output(vec![batch(&[1, 2, 3], true)]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_columns(), 2, "_distance must not reach the user");
        assert_eq!(out[0].schema().field(0).name(), "id");
    }

    #[test]
    fn limit_and_offset_are_applied_to_index_results() {
        let e = exec_for(2, 1);
        let out = e.shape_output(vec![batch(&[1, 2, 3, 4, 5], true)]).unwrap();
        let ids: Vec<i64> = out
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(ids, vec![2, 3]);
    }

    #[test]
    fn missing_column_is_an_error_not_a_wrong_answer() {
        let e = exec_for(10, 0);
        let only_id = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![Field::new(
                "id",
                DataType::Int64,
                true,
            )])),
            vec![Arc::new(Int64Array::from(vec![1i64])) as ArrayRef],
        )
        .unwrap();
        let err = e.shape_output(vec![only_id]).unwrap_err().to_string();
        assert!(err.contains("category"), "{}", err);
    }

    #[test]
    fn exact_mode_never_calls_the_index() {
        let mut e = exec_for(10, 0);
        e.config.vector_search_mode = VectorSearchMode::Exact;
        assert!(e.try_index().unwrap().is_none());
    }
}
