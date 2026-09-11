//! Projection operator

use crate::error::Result;
use crate::physical::operators::filter::{evaluate_expr, evaluate_expr_with_subquery};
use crate::physical::operators::subquery::SubqueryExecutor;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::Expr;
use arrow::datatypes::{Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::TryStreamExt;
use futures::StreamExt;
use std::fmt;
use std::sync::Arc;

mod admitted;

/// Projection execution operator
pub struct ProjectExec {
    input: Arc<dyn PhysicalOperator>,
    memory_pool: Option<crate::execution::SharedMemoryPool>,
    exprs: Vec<Expr>,
    schema: SchemaRef,
    /// Optional subquery executor for handling subqueries in projection expressions
    subquery_executor: Option<SubqueryExecutor>,
}

impl fmt::Debug for ProjectExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ProjectExec")
            .field("exprs", &self.exprs)
            .field("has_subquery_executor", &self.subquery_executor.is_some())
            .finish()
    }
}

impl ProjectExec {
    pub fn new(input: Arc<dyn PhysicalOperator>, exprs: Vec<Expr>, schema: SchemaRef) -> Self {
        Self {
            input,
            exprs,
            schema,
            subquery_executor: None,
            memory_pool: None,
        }
    }

    pub fn with_memory_pool(mut self, pool: crate::execution::SharedMemoryPool) -> Self {
        self.memory_pool = Some(pool);
        self
    }

    /// Set the subquery executor for this projection
    pub fn with_subquery_executor(mut self, executor: SubqueryExecutor) -> Self {
        self.subquery_executor = Some(executor);
        self
    }

    pub fn try_new(input: Arc<dyn PhysicalOperator>, exprs: Vec<Expr>) -> Result<Self> {
        let input_schema = input.schema();

        // Build output schema from expressions
        let fields: Result<Vec<Field>> = exprs
            .iter()
            .map(|e| {
                let name = e.output_name();
                let plan_schema =
                    crate::planner::PlanSchema::from_qualified_arrow(input_schema.as_ref());
                let dt = e.data_type(&plan_schema)?;
                let mut field = crate::planner::SchemaField::new(name, dt);
                if let Expr::Column(column) = e {
                    field.name = column.name.clone();
                    field.relation = column.relation.clone();
                }
                Ok(field.to_arrow_field())
            })
            .collect();

        let schema = Arc::new(Schema::new(fields?));

        Ok(Self {
            input,
            exprs,
            schema,
            subquery_executor: None,
            memory_pool: None,
        })
    }
}

#[async_trait]
impl PhysicalOperator for ProjectExec {
    fn runtime_filter_target(
        &self,
        output: usize,
    ) -> Option<crate::physical::plan::RuntimeFilterTarget> {
        let mut expression = self.exprs.get(output)?;
        while let Expr::Alias { expr, .. } = expression {
            expression = expr;
        }
        let Expr::Column(column) = expression else {
            return None;
        };
        let input_schema = self.input.schema();
        let index =
            crate::physical::operators::find_column_index_in_schema(&input_schema, column).ok()?;
        if input_schema.field(index).data_type() != self.schema.fields().get(output)?.data_type() {
            return None;
        }
        self.input.runtime_filter_target(index)
    }
    async fn prepare_admitted_queue_input(
        &self,
        pool: crate::execution::SharedMemoryPool,
    ) -> Result<Option<crate::physical::PreparedAdmittedInput>> {
        if !self.exprs.iter().all(is_column_alias) {
            return admitted::prepare(self, pool).await;
        }
        if self.exprs.len() != self.schema.fields().len() {
            return Err(crate::QueryError::Execution(
                "admitted Project expression/schema width mismatch".into(),
            ));
        }
        let Some(prepared) = self
            .input
            .prepare_admitted_queue_input(pool.clone())
            .await?
        else {
            return Ok(None);
        };
        if !prepared.pool.is_within(&pool) {
            return Err(crate::QueryError::Execution(
                "admitted Project pool mismatch".into(),
            ));
        }
        fn column(expr: &Expr) -> &crate::planner::Column {
            match expr {
                Expr::Column(c) => c,
                Expr::Alias { expr, .. } => column(expr),
                _ => unreachable!(),
            }
        }
        let input_schema = self.input.schema();
        let mut indices =
            crate::execution::reserved_vec::ReservedVec::with_capacity(&pool, self.exprs.len())?;
        for (expr, field) in self.exprs.iter().zip(self.schema.fields()) {
            let index = crate::physical::operators::find_column_index_in_schema(
                &input_schema,
                column(expr),
            )?;
            if input_schema.field(index).data_type() != field.data_type() {
                return Err(crate::QueryError::Execution(
                    "admitted Project requires exact column types".into(),
                ));
            }
            indices.extend_reserved(1, [index])?;
        }
        let indices = Arc::new(indices);
        let mut streams = crate::execution::reserved_vec::ReservedVec::with_capacity(
            &pool,
            prepared.streams.as_slice().len(),
        )?;
        for input in prepared.streams.into_owned_iter() {
            let indices = indices.clone();
            let schema = self.schema.clone();
            let output_pool = pool.clone();
            let output = input.map(move |result| {
                let batch = result?;
                let mut arrays = crate::execution::reserved_vec::ReservedVec::with_capacity(
                    &output_pool,
                    indices.as_slice().len(),
                )?;
                for &index in indices.as_slice() {
                    arrays.extend_reserved(1, [batch.column(index).clone()])?;
                }
                crate::storage::admitted_batch::finish(
                    schema.clone(),
                    batch.num_rows(),
                    arrays,
                    &output_pool,
                )
            });
            streams.extend_reserved(1, [crate::physical::admit_stream(output, &pool)?])?;
        }
        Ok(Some(crate::physical::PreparedAdmittedInput {
            pool,
            streams,
        }))
    }
    async fn prepare_queue_input(&self) -> Result<Option<crate::physical::PreparedQueueInput>> {
        if !self.exprs.iter().all(is_column_alias) {
            return Ok(None);
        }
        let Some(prepared) = self.input.prepare_queue_input().await? else {
            return Ok(None);
        };
        let output = prepared.output.projected(&self.exprs, &self.schema);
        let mut streams = Vec::new();
        streams
            .try_reserve_exact(prepared.streams.len())
            .map_err(|e| {
                crate::error::QueryError::Execution(format!(
                    "prepared wrapper stream allocation failed: {e}"
                ))
            })?;
        for stream in prepared.streams {
            streams.push(self.wrap_stream(stream)?);
        }
        Ok(Some(crate::physical::PreparedQueueInput {
            streams,
            output,
        }))
    }

    fn resident_queue_copy_bound(&self) -> Option<crate::physical::queue_layout::QueueCopyBound> {
        self.input
            .resident_queue_copy_bound()?
            .projected(&self.exprs, &self.schema)
    }

    fn resident_gather_copy_bound(&self) -> Option<crate::physical::queue_layout::GatherCopyBound> {
        self.input
            .resident_gather_copy_bound()?
            .projected(&self.exprs, &self.schema)
    }

    fn pool_independent_queue_copy_bound(
        &self,
    ) -> Option<crate::physical::queue_layout::QueueCopyBound> {
        self.input
            .pool_independent_queue_copy_bound()?
            .projected(&self.exprs, &self.schema)
    }

    fn pool_independent_gather_copy_bound(
        &self,
    ) -> Option<crate::physical::queue_layout::GatherCopyBound> {
        self.input
            .pool_independent_gather_copy_bound()?
            .projected(&self.exprs, &self.schema)
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.input.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        crate::physical::check_partition(self, partition)?;

        let input_stream = self.input.execute(partition).await?;
        self.wrap_stream(input_stream)
    }

    fn name(&self) -> &str {
        "Project"
    }

    fn output_partitions(&self) -> usize {
        // Propagate partitions from input - projection preserves partitioning
        self.input.output_partitions()
    }
}

impl fmt::Display for ProjectExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let exprs: Vec<String> = self.exprs.iter().map(|e| e.to_string()).collect();
        write!(f, "Project: [{}]", exprs.join(", "))
    }
}

fn project_batch(
    batch: &RecordBatch,
    exprs: &[Expr],
    schema: &SchemaRef,
    subquery_exec: Option<&SubqueryExecutor>,
) -> Result<RecordBatch> {
    let columns: Result<Vec<_>> = exprs
        .iter()
        .map(|expr| {
            if subquery_exec.is_some() {
                evaluate_expr_with_subquery(batch, expr, subquery_exec)
            } else {
                evaluate_expr(batch, expr)
            }
        })
        .collect();

    // An expression whose VALUE is NULL but whose declared TYPE is not (a
    // literal NULL, or an uncorrelated scalar subquery precomputed to NULL)
    // evaluates to an untyped NullArray. RecordBatch::try_new rejects that
    // against the column's declared type, turning valid SQL into an execution
    // error; retype it as a typed all-NULL column instead.
    let columns: Vec<_> = columns?
        .into_iter()
        .zip(schema.fields())
        .map(|(col, field)| {
            if col.data_type() == &arrow::datatypes::DataType::Null
                && field.data_type() != &arrow::datatypes::DataType::Null
            {
                arrow::array::new_null_array(field.data_type(), col.len())
            } else {
                col
            }
        })
        .collect();

    // Dictionary-encoded columns pass through Column projections unchanged
    // (small-build join gathers); adjust declared field types to the actual
    // array types so the batch validates. See hash_join's
    // batch_with_actual_types for the contract.
    let schema = if columns
        .iter()
        .zip(schema.fields())
        .all(|(c, f)| c.data_type() == f.data_type())
    {
        schema.clone()
    } else {
        Arc::new(arrow::datatypes::Schema::new(
            schema
                .fields()
                .iter()
                .zip(&columns)
                .map(|(f, c)| {
                    if f.data_type() == c.data_type() {
                        f.as_ref().clone()
                    } else {
                        f.as_ref()
                            .clone()
                            .with_data_type(c.data_type().clone())
                            .with_nullable(true)
                    }
                })
                .collect::<Vec<_>>(),
        ))
    };

    // Row count must be stated explicitly: a projection may legitimately have
    // no columns (`SELECT COUNT(*)` shapes reduced to nothing), and a batch
    // with no columns cannot infer its length.
    let options =
        arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema, columns, &options).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::MemoryTableExec;
    use crate::planner::ScalarValue;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::TryStreamExt;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("value", DataType::Int64, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_project_columns() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));

        // Project: id, value
        let exprs = vec![Expr::column("id"), Expr::column("value")];

        let project = ProjectExec::try_new(scan, exprs).unwrap();

        let stream = project.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_columns(), 2);
        assert_eq!(results[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_project_expression() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));

        // Project: value * 2
        let exprs = vec![Expr::column("value").multiply(Expr::literal(ScalarValue::Int64(2)))];

        let project = ProjectExec::try_new(scan, exprs).unwrap();

        let stream = project.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_columns(), 1);

        let values = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(values.value(0), 20);
        assert_eq!(values.value(1), 40);
        assert_eq!(values.value(2), 60);
    }
}

impl ProjectExec {
    fn wrap_stream(&self, input_stream: RecordBatchStream) -> Result<RecordBatchStream> {
        let exprs = self.exprs.clone();
        let schema = self.schema.clone();
        let subquery_exec = self.subquery_executor.clone();
        let memory_pool = self.memory_pool.clone();

        let projected = input_stream.and_then(move |batch| {
            let exprs = exprs.clone();
            let schema = schema.clone();
            let subquery_exec = subquery_exec.clone();
            let memory_pool = memory_pool.clone();
            async move {
                if crate::execution::expression_memory::trace_enabled() {
                    eprintln!("[reserved-expression] {}", serde_json::json!({"event":"projection","rows":batch.num_rows(),"column_alias_only":exprs.iter().all(is_column_alias)}));
                }
                let evaluate = || project_batch(&batch, &exprs, &schema, subquery_exec.as_ref());
                match memory_pool {
                    Some(pool) => {
                        crate::execution::expression_memory::with_expression_pool(&pool, evaluate)
                    }
                    None => evaluate(),
                }
            }
        });

        Ok(Box::pin(projected))
    }
}

fn is_column_alias(expr: &Expr) -> bool {
    match expr {
        Expr::Column(_) => true,
        Expr::Alias { expr, .. } => is_column_alias(expr),
        _ => false,
    }
}
