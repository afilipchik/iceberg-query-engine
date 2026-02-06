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
use std::fmt;
use std::sync::Arc;

/// Projection execution operator
pub struct ProjectExec {
    input: Arc<dyn PhysicalOperator>,
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
        }
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
                let plan_schema = crate::planner::PlanSchema::from(input_schema.as_ref());
                let dt = e.data_type(&plan_schema)?;
                Ok(Field::new(name, dt, true))
            })
            .collect();

        let schema = Arc::new(Schema::new(fields?));

        Ok(Self {
            input,
            exprs,
            schema,
            subquery_executor: None,
        })
    }
}

#[async_trait]
impl PhysicalOperator for ProjectExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.input.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        let input_stream = self.input.execute(partition).await?;
        let exprs = self.exprs.clone();
        let schema = self.schema.clone();
        let subquery_exec = self.subquery_executor.clone();

        let projected = input_stream.and_then(move |batch| {
            let exprs = exprs.clone();
            let schema = schema.clone();
            let subquery_exec = subquery_exec.clone();
            async move { project_batch(&batch, &exprs, &schema, subquery_exec.as_ref()) }
        });

        Ok(Box::pin(projected))
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

    RecordBatch::try_new(schema.clone(), columns?).map_err(Into::into)
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
