use super::*;
use crate::{
    execution::{create_memory_pool, ReservedBufferBuilder},
    physical::{MemoryTableExec, PreparedAdmittedInput},
};
use arrow::{
    array::{Array, Decimal128Array, Int32Array, Int64Array, StringArray, UInt8Array},
    buffer::ScalarBuffer,
    datatypes::{Field, Schema},
};
use std::sync::atomic::{AtomicUsize, Ordering};
fn decimal(values: Vec<Option<i128>>, precision: u8) -> ArrayRef {
    Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(precision, 2)
            .unwrap(),
    )
}
#[test]
fn admitted_projection_has_independent_decimal_temporal_and_try_cast_results() {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "name",
            Arc::new(StringArray::from(vec![Some("a"), None, Some("a")])) as ArrayRef,
        ),
        (
            "d",
            Arc::new(Date32Array::from(vec![Some(11016), None, Some(-1)])) as ArrayRef,
        ),
        ("price", decimal(vec![Some(10000), None, Some(-10000)], 10)),
        ("discount", decimal(vec![Some(10), Some(0), Some(10)], 4)),
        (
            "v",
            Arc::new(Int64Array::from(vec![Some(255), Some(256), None])) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut schema_metadata = std::collections::HashMap::new();
    schema_metadata.insert("origin".into(), "independent projection fixture".into());
    let source = Arc::new(MemoryTableExec::new(
        "t",
        batch.schema(),
        vec![batch.clone()],
        None,
    ));
    let exprs = vec![
        Expr::column("name"),
        Expr::ScalarFunc {
            func: ScalarFunction::Extract,
            args: vec![
                Expr::literal(ScalarValue::Utf8("YEAR".into())),
                Expr::column("d"),
            ],
        },
        Expr::column("price")
            .multiply(Expr::literal(ScalarValue::Int64(1)).subtract(Expr::column("discount"))),
        Expr::Cast {
            expr: Box::new(Expr::column("v")),
            data_type: DataType::UInt8,
            mode: CastMode::Try,
        },
    ];
    let mut project = ProjectExec::try_new(source, exprs).unwrap();
    project.schema = Arc::new(
        project
            .schema
            .as_ref()
            .clone()
            .with_metadata(schema_metadata),
    );
    let pool = create_memory_pool(4 * 1024 * 1024);
    let program = Program::bind(&project, &pool).unwrap().unwrap();
    let baseline = pool.used();
    for input in [batch.slice(0, 0), batch.clone(), batch.clone()] {
        let output = program.apply(input, &pool).unwrap();
        assert_eq!(output.schema(), project.schema);
        if output.num_rows() > 0 {
            assert_eq!(
                output
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some("a"), None, Some("a")]
            );
            assert_eq!(
                output
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(2000), None, Some(1969)]
            );
            assert_eq!(output.column(2).data_type(), &DataType::Decimal128(33, 4));
            assert_eq!(
                output
                    .column(2)
                    .as_any()
                    .downcast_ref::<Decimal128Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(900000), None, Some(-900000)]
            );
            assert_eq!(
                output
                    .column(3)
                    .as_any()
                    .downcast_ref::<UInt8Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>(),
                vec![Some(255), None, None]
            );
        }
        drop(output);
        assert_eq!(pool.used(), baseline);
    }
    let pressure = pool.allocate(pool.available()).unwrap();
    assert!(program
        .apply(batch.clone(), &pool)
        .unwrap_err()
        .is_memory_limit());
    drop(pressure);
    assert_eq!(pool.used(), baseline);
    let wrong = RecordBatch::try_from_iter(vec![(
        "name",
        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
    )])
    .unwrap();
    assert!(program.apply(wrong, &pool).is_err());
    assert_eq!(pool.used(), baseline);
    drop(program);
    assert_eq!(pool.used(), 0);
}
#[derive(Debug)]
struct Counted {
    schema: SchemaRef,
    pulls: Arc<AtomicUsize>,
    unsupported: bool,
}
#[async_trait]
impl PhysicalOperator for Counted {
    fn name(&self) -> &str {
        "counted admitted source"
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    async fn execute(&self, _: usize) -> Result<RecordBatchStream> {
        panic!("ordinary source replay")
    }
    async fn prepare_admitted_queue_input(
        &self,
        pool: SharedMemoryPool,
    ) -> Result<Option<PreparedAdmittedInput>> {
        assert!(!self.unsupported, "unsupported program prepared its child");
        let schema = self.schema.clone();
        let pulls = self.pulls.clone();
        let runtime = pool.clone();
        let source = futures::stream::iter(0..2).map(move |batch| {
            pulls.fetch_add(1, Ordering::SeqCst);
            let mut values = ReservedBufferBuilder::<i64>::with_capacity(&runtime, 2)?;
            values.extend_reserved(2, if batch == 0 { [1, i64::MAX] } else { [7, 9] })?;
            let array: ArrayRef = Arc::new(Int64Array::new(
                ScalarBuffer::new(values.finish(), 0, 2),
                None,
            ));
            let mut arrays = ReservedVec::with_capacity(&runtime, 1)?;
            arrays.extend_reserved(1, [array])?;
            crate::storage::admitted_batch::finish(schema.clone(), 2, arrays, &runtime)
        });
        let mut streams = ReservedVec::with_capacity(&pool, 1)?;
        streams.extend_reserved(1, [crate::physical::admit_stream(source, &pool)?])?;
        Ok(Some(PreparedAdmittedInput { pool, streams }))
    }
}
fn counted(unsupported: bool) -> Arc<Counted> {
    Arc::new(Counted {
        schema: Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
        pulls: Arc::new(AtomicUsize::new(0)),
        unsupported,
    })
}
#[tokio::test]
async fn computed_projection_errors_poison_without_a_second_source_pull() {
    for pressure in [false, true] {
        let source = counted(false);
        let pool = create_memory_pool(1024 * 1024);
        let project = ProjectExec::try_new(
            source.clone(),
            vec![Expr::column("v").add(Expr::literal(ScalarValue::Int64(1)))],
        )
        .unwrap();
        let prepared = project
            .prepare_admitted_queue_input(pool.clone())
            .await
            .unwrap()
            .unwrap();
        let mut stream = prepared.streams.into_owned_iter().next().unwrap();
        let hold = pressure.then(|| pool.allocate(pool.available()).unwrap());
        let error = stream.try_next().await.unwrap_err();
        assert_eq!(error.is_memory_limit(), pressure);
        assert!(stream.try_next().await.unwrap().is_none());
        assert_eq!(source.pulls.load(Ordering::SeqCst), 1);
        drop((hold, stream));
        assert_eq!(pool.used(), 0);
    }
}
#[tokio::test]
async fn unsupported_or_unadmitted_programs_decline_before_preparing_the_child() {
    let source = counted(true);
    for expr in [
        Expr::ScalarFunc {
            func: ScalarFunction::Random,
            args: vec![],
        },
        Expr::Cast {
            expr: Box::new(Expr::column("v")),
            data_type: DataType::Utf8,
            mode: CastMode::Strict,
        },
    ] {
        let project = ProjectExec::try_new(source.clone(), vec![expr]).unwrap();
        let pool = create_memory_pool(65536);
        assert!(project
            .prepare_admitted_queue_input(pool.clone())
            .await
            .unwrap()
            .is_none());
        assert_eq!(pool.used(), 0);
    }
    let project = ProjectExec::try_new(
        source,
        vec![Expr::column("v").add(Expr::literal(ScalarValue::Int64(1)))],
    )
    .unwrap();
    let pool = create_memory_pool(1);
    assert!(project
        .prepare_admitted_queue_input(pool.clone())
        .await
        .unwrap()
        .is_none());
    assert_eq!(pool.used(), 0);
}
