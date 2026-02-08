//! Sort operator

use crate::error::Result;
use crate::physical::operators::filter::evaluate_expr;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::{SortDirection, SortExpr};
use arrow::array::ArrayRef;
use arrow::compute::{self, SortColumn, SortOptions};
use arrow::datatypes::{DataType, Field, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::{self, TryStreamExt};
use std::fmt;
use std::sync::Arc;

/// Sort execution operator
#[derive(Debug)]
pub struct SortExec {
    input: Arc<dyn PhysicalOperator>,
    order_by: Vec<SortExpr>,
    schema: SchemaRef,
}

impl SortExec {
    pub fn new(input: Arc<dyn PhysicalOperator>, order_by: Vec<SortExpr>) -> Self {
        let schema = input.schema();
        Self {
            input,
            order_by,
            schema,
        }
    }
}

#[async_trait]
impl PhysicalOperator for SortExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.input.clone()]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        // Sort always produces a single partition by collecting from all input partitions
        if partition != 0 {
            return Ok(Box::pin(stream::empty()));
        }

        // Collect from ALL input partitions (input may split data across partitions)
        let input_partitions = self.input.output_partitions().max(1);
        let mut all_batches = Vec::new();
        for p in 0..input_partitions {
            let input_stream = self.input.execute(p).await?;
            let batches: Vec<RecordBatch> = input_stream.try_collect().await?;
            all_batches.extend(batches);
        }

        // Concatenate into single batch
        if all_batches.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        // Check if Utf8 columns risk exceeding the 2GB i32 offset limit
        let needs_large_utf8 = check_string_overflow_risk(&self.schema, &all_batches);

        let (working_schema, working_batches) = if needs_large_utf8 {
            let schema = promote_utf8_schema(&self.schema);
            let batches = all_batches
                .iter()
                .map(|b| promote_utf8_batch(b))
                .collect::<Result<Vec<_>>>()?;
            (schema, batches)
        } else {
            (self.schema.clone(), all_batches)
        };

        let batch = concat_batches(&working_schema, &working_batches)?;

        // Sort
        let sorted = sort_batch(&batch, &self.order_by)?;

        let sorted = if needs_large_utf8 {
            demote_utf8_batch(&sorted)?
        } else {
            sorted
        };

        Ok(Box::pin(stream::once(async { Ok(sorted) })))
    }

    fn name(&self) -> &str {
        "Sort"
    }
}

impl fmt::Display for SortExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let order: Vec<String> = self
            .order_by
            .iter()
            .map(|s| {
                let dir = match s.direction {
                    SortDirection::Asc => "ASC",
                    SortDirection::Desc => "DESC",
                };
                format!("{} {}", s.expr, dir)
            })
            .collect();
        write!(f, "Sort: [{}]", order.join(", "))
    }
}

fn concat_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<RecordBatch> {
    if batches.is_empty() {
        return Ok(RecordBatch::new_empty(schema.clone()));
    }

    if batches.len() == 1 {
        return Ok(batches[0].clone());
    }

    compute::concat_batches(schema, batches).map_err(Into::into)
}

fn sort_batch(batch: &RecordBatch, order_by: &[SortExpr]) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return Ok(batch.clone());
    }

    // Evaluate sort keys
    let sort_columns: Result<Vec<SortColumn>> = order_by
        .iter()
        .map(|s| {
            let values = evaluate_expr(batch, &s.expr)?;
            Ok(SortColumn {
                values,
                options: Some(SortOptions {
                    descending: s.direction == SortDirection::Desc,
                    nulls_first: matches!(s.nulls, crate::planner::NullOrdering::NullsFirst),
                }),
            })
        })
        .collect();
    let sort_columns = sort_columns?;

    // Get sort indices
    let indices = compute::lexsort_to_indices(&sort_columns, None)?;

    // Reorder all columns
    let sorted_columns: Result<Vec<ArrayRef>> = batch
        .columns()
        .iter()
        .map(|col| compute::take(col.as_ref(), &indices, None).map_err(Into::into))
        .collect();

    RecordBatch::try_new(batch.schema(), sorted_columns?).map_err(Into::into)
}

/// Check if concatenating batches would risk exceeding the 2GB i32 offset limit
/// for any Utf8 column. Returns true if any string column's total data exceeds 1.5GB.
fn check_string_overflow_risk(schema: &SchemaRef, batches: &[RecordBatch]) -> bool {
    const THRESHOLD: usize = 1_500_000_000; // 1.5GB

    for (col_idx, field) in schema.fields().iter().enumerate() {
        if *field.data_type() == DataType::Utf8 {
            let total_bytes: usize = batches
                .iter()
                .map(|b| {
                    if col_idx < b.num_columns() {
                        b.column(col_idx).get_array_memory_size()
                    } else {
                        0
                    }
                })
                .sum();
            if total_bytes > THRESHOLD {
                return true;
            }
        }
    }
    false
}

/// Create a new schema with Utf8 fields promoted to LargeUtf8
fn promote_utf8_schema(schema: &SchemaRef) -> SchemaRef {
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|f| {
            if *f.data_type() == DataType::Utf8 {
                Arc::new(Field::new(f.name(), DataType::LargeUtf8, f.is_nullable()))
            } else {
                f.clone()
            }
        })
        .collect();
    Arc::new(arrow::datatypes::Schema::new(fields))
}

/// Cast Utf8 columns to LargeUtf8 in a batch
fn promote_utf8_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let new_schema = promote_utf8_schema(&batch.schema());
    let columns: Result<Vec<ArrayRef>> = batch
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| {
            if *batch.schema().field(i).data_type() == DataType::Utf8 {
                compute::cast(col.as_ref(), &DataType::LargeUtf8).map_err(Into::into)
            } else {
                Ok(col.clone())
            }
        })
        .collect();
    RecordBatch::try_new(new_schema, columns?).map_err(Into::into)
}

/// Cast LargeUtf8 columns back to Utf8 in a batch
fn demote_utf8_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    let fields: Vec<Arc<Field>> = batch
        .schema()
        .fields()
        .iter()
        .map(|f| {
            if *f.data_type() == DataType::LargeUtf8 {
                Arc::new(Field::new(f.name(), DataType::Utf8, f.is_nullable()))
            } else {
                f.clone()
            }
        })
        .collect();
    let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));

    let columns: Result<Vec<ArrayRef>> = batch
        .columns()
        .iter()
        .enumerate()
        .map(|(i, col)| {
            if *batch.schema().field(i).data_type() == DataType::LargeUtf8 {
                compute::cast(col.as_ref(), &DataType::Utf8).map_err(Into::into)
            } else {
                Ok(col.clone())
            }
        })
        .collect();
    RecordBatch::try_new(new_schema, columns?).map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::MemoryTableExec;
    use crate::planner::{Expr, NullOrdering};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::TryStreamExt;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![3, 1, 4, 1, 5])),
                Arc::new(StringArray::from(vec!["c", "a", "d", "b", "e"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_sort_ascending() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));

        let order_by = vec![SortExpr {
            expr: Expr::column("id"),
            direction: SortDirection::Asc,
            nulls: NullOrdering::NullsFirst,
        }];

        let sort = SortExec::new(scan, order_by);

        let stream = sort.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(results.len(), 1);

        let ids = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 1);
        assert_eq!(ids.value(2), 3);
        assert_eq!(ids.value(3), 4);
        assert_eq!(ids.value(4), 5);
    }

    #[tokio::test]
    async fn test_sort_descending() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));

        let order_by = vec![SortExpr {
            expr: Expr::column("id"),
            direction: SortDirection::Desc,
            nulls: NullOrdering::NullsLast,
        }];

        let sort = SortExec::new(scan, order_by);

        let stream = sort.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let ids = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(ids.value(0), 5);
        assert_eq!(ids.value(1), 4);
        assert_eq!(ids.value(2), 3);
    }
}
