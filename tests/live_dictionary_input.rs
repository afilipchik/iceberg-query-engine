//! Join output may retain dictionary encoding under a logical UTF8 plan schema.
use arrow::{
    array::*,
    datatypes::{DataType, Field, Int32Type, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{stream, TryStreamExt};
use query_engine::{
    execution::MemoryPool,
    physical::{
        operators::spillable::{AggregateExpr, SpillableHashAggregateExec},
        PhysicalOperator, RecordBatchStream,
    },
    planner::{AggregateFunction, Expr},
    ExecutionConfig, Result,
};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
#[derive(Debug)]
struct EncodedInput {
    schema: SchemaRef,
    batches: [RecordBatch; 2],
    calls: [AtomicUsize; 2],
}
#[async_trait]
impl PhysicalOperator for EncodedInput {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn name(&self) -> &str {
        "EncodedLogicalInput"
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn output_partitions(&self) -> usize {
        2
    }
    async fn execute(&self, p: usize) -> Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, p)?;
        assert_eq!(
            self.calls[p].fetch_add(1, Ordering::SeqCst),
            0,
            "input replayed"
        );
        Ok(Box::pin(stream::iter([
            Ok(self.batches[p].slice(0, 0)),
            Ok(self.batches[p].clone()),
        ])))
    }
}
fn encoded(reverse: bool) -> RecordBatch {
    let mut values: Vec<Option<String>> = (0..65)
        .map(|i| if i == 64 { None } else { Some(format!("g{i}")) })
        .collect();
    if reverse {
        values.reverse();
    }
    let keys = Int32Array::from(
        (0..65)
            .map(|i| {
                if i == 63 {
                    None
                } else {
                    Some(if reverse { 64 - i } else { i })
                }
            })
            .collect::<Vec<_>>(),
    );
    let groups: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(keys, Arc::new(StringArray::from(values))).unwrap(),
    );
    let values: ArrayRef = Arc::new(StringArray::from(if reverse {
        vec![Some("z"), None, Some("a")]
    } else {
        vec![Some("a"), Some("z"), None]
    }));
    let keys = Int32Array::from(
        (0..65)
            .map(|i| {
                Some(if i % 7 == 0 {
                    if reverse {
                        1
                    } else {
                        2
                    }
                } else {
                    0
                })
            })
            .collect::<Vec<_>>(),
    );
    let inputs: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap());
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", groups.data_type().clone(), true),
        Field::new("v", inputs.data_type().clone(), true),
    ]));
    RecordBatch::try_new(schema, vec![groups, inputs]).unwrap()
}
#[tokio::test]
async fn logical_dictionary_values_survive_changing_codebooks_and_spill() {
    for disjoint in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let pool = Arc::new(MemoryPool::new_named("encoded query", 4 << 20));
        let input = Arc::new(EncodedInput {
            schema: Arc::new(Schema::new(vec![
                Field::new("g", DataType::Utf8, true),
                Field::new("v", DataType::Utf8, true),
            ])),
            batches: [encoded(false), encoded(true)],
            calls: [AtomicUsize::new(0), AtomicUsize::new(0)],
        });
        let schema = Arc::new(Schema::new(vec![
            Field::new("g", DataType::Utf8, true),
            Field::new("c", DataType::Int64, false),
            Field::new("lo", DataType::Utf8, true),
            Field::new("hi", DataType::Utf8, true),
        ]));
        let operator = SpillableHashAggregateExec::new(
            input.clone(),
            vec![Expr::column("g")],
            [
                AggregateFunction::Count,
                AggregateFunction::Min,
                AggregateFunction::Max,
            ]
            .into_iter()
            .map(|func| AggregateExpr {
                func,
                input: Expr::column("v"),
                distinct: false,
                second_arg: None,
            })
            .collect(),
            schema,
            pool.clone(),
            ExecutionConfig::new()
                .with_memory_limit(8192)
                .with_spill_path(directory.path().to_path_buf()),
        )
        .with_disjoint_groups(disjoint);
        let results: Vec<RecordBatch> = operator
            .execute(0)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let mut seen = [false; 64];
        for batch in &results {
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let counts = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let lo = batch
                .column(2)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let hi = batch
                .column(3)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let id = if keys.is_null(row) {
                    63
                } else {
                    keys.value(row)[1..].parse::<usize>().unwrap()
                };
                assert!(!seen[id]);
                seen[id] = true;
                let null = id != 63 && id % 7 == 0;
                assert_eq!(counts.value(row), if null { 0 } else { 2 });
                assert_eq!(lo.is_null(row), null);
                assert_eq!(hi.is_null(row), null);
                if !null {
                    assert_eq!(lo.value(row), "a");
                    assert_eq!(hi.value(row), "z");
                }
            }
        }
        assert!(seen.into_iter().all(|v| v));
        for calls in &input.calls {
            assert_eq!(calls.load(Ordering::SeqCst), 1);
        }
        assert!(pool.spilled() > 0);
        drop(results);
        drop(operator);
        assert_eq!(pool.used(), 0);
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 0);
    }
}
