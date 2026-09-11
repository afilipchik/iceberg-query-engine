//! Independent values across binding wrappers and projection boundaries.
use arrow::{
    array::{Array, ArrayRef, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use query_engine::ExecutionContext;
use std::sync::Arc;

async fn check(sql: &str, empty: bool, expected: Vec<(Option<String>, Option<i64>, i64)>) {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "k",
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("b"),
                None,
                None,
                Some("c"),
            ])) as ArrayRef,
        ),
        (
            "v",
            Arc::new(Int64Array::from(vec![
                Some(2),
                Some(2),
                None,
                Some(5),
                None,
                Some(-1),
            ])) as ArrayRef,
        ),
    ])
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input.parquet");
    let props = parquet::file::properties::WriterProperties::builder()
        .set_max_row_group_row_count(Some(2))
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        batch.schema(),
        Some(props),
    )
    .unwrap();
    writer
        .write(&if empty { batch.slice(0, 0) } else { batch })
        .unwrap();
    writer.close().unwrap();
    let mut ctx = ExecutionContext::new().with_parallel_partitions(3);
    ctx.register_parquet("t", &path).unwrap();
    let output = ctx.sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
    let mut actual = Vec::new();
    for b in output.batches {
        assert_eq!(
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>(),
            vec!["g", "s", "n"],
            "{sql}"
        );
        let k = b.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let v = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let n = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            assert!(!n.is_null(i));
            actual.push((
                (!k.is_null(i)).then(|| k.value(i).to_owned()),
                (!v.is_null(i)).then(|| v.value(i)),
                n.value(i),
            ));
        }
    }
    actual.sort();
    let mut expected = expected;
    expected.sort();
    assert_eq!(actual, expected, "{sql}");
}

#[tokio::test]
async fn transparent_bindings_preserve_nullable_duplicate_groups() {
    let forms=[
        "SELECT k AS g,SUM(v) AS s,COUNT(v) AS n FROM t GROUP BY k",
        "SELECT g,SUM(x) AS s,COUNT(x) AS n FROM (SELECT v AS x,k AS g FROM t) d GROUP BY g",
        "WITH d AS (SELECT v AS x,k AS g FROM t) SELECT g,SUM(x) AS s,COUNT(x) AS n FROM d GROUP BY g",
        "SELECT d.g AS g,SUM(d.x) AS s,COUNT(d.x) AS n FROM t AS d(g,x) GROUP BY d.g",
        "SELECT g,SUM(x) AS s,COUNT(y) AS n FROM (SELECT v AS x,k AS g,v AS y FROM t) d GROUP BY g",
    ];
    for sql in forms {
        check(
            sql,
            false,
            vec![
                (None, Some(5), 1),
                (Some("a".into()), Some(4), 2),
                (Some("b".into()), None, 0),
                (Some("c".into()), Some(-1), 1),
            ],
        )
        .await;
        check(sql, true, vec![]).await;
    }
}

#[tokio::test]
async fn computed_filtered_distinct_and_limited_inputs_keep_their_semantics() {
    check("SELECT g,SUM(x) AS s,COUNT(x) AS n FROM (SELECT k AS g,v+10 AS x FROM t WHERE v IS NOT NULL) d GROUP BY g",false,
        vec![(None,Some(15),1),(Some("a".into()),Some(24),2),(Some("c".into()),Some(9),1)]).await;
    check("SELECT g,SUM(x) AS s,COUNT(x) AS n FROM (SELECT DISTINCT k AS g,v AS x FROM t) d GROUP BY g",false,
        vec![(None,Some(5),1),(Some("a".into()),Some(2),1),(Some("b".into()),None,0),(Some("c".into()),Some(-1),1)]).await;
    check("SELECT g,SUM(x) AS s,COUNT(x) AS n FROM (SELECT k AS g,v AS x FROM t ORDER BY v DESC NULLS LAST LIMIT 1) d GROUP BY g",false,
        vec![(None,Some(5),1)]).await;
}

#[tokio::test]
async fn physical_aggregate_must_not_erase_computed_project() {
    use arrow::datatypes::DataType;
    use futures::TryStreamExt;
    use query_engine::{
        execution::{ExecutionConfig, MemoryPool},
        physical::PhysicalPlanner,
        planner::{
            AggregateFunction, AggregateNode, BinaryOp, Expr, LogicalPlan, PlanSchema, ProjectNode,
            ScalarValue, ScanNode, SchemaField,
        },
        ParquetTable,
    };
    let batch = RecordBatch::try_from_iter(vec![(
        "x",
        Arc::new(Int64Array::from(vec![Some(1), Some(1), None, Some(2)])) as ArrayRef,
    )])
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("input.parquet");
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        std::fs::File::create(&path).unwrap(),
        batch.schema(),
        None,
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let schema = PlanSchema::from(batch.schema().as_ref());
    let input = LogicalPlan::Project(ProjectNode {
        input: Arc::new(LogicalPlan::Scan(ScanNode {
            table_name: "t".into(),
            schema: schema.clone(),
            projection: None,
            filter: None,
        })),
        exprs: vec![Expr::Alias {
            name: "x".into(),
            expr: Box::new(Expr::BinaryExpr {
                left: Box::new(Expr::column("x")),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(ScalarValue::Int64(10))),
            }),
        }],
        schema,
    });
    let plan = LogicalPlan::Aggregate(AggregateNode {
        input: Arc::new(input),
        group_by: vec![],
        aggregates: vec![Expr::Aggregate {
            func: AggregateFunction::Sum,
            args: vec![Expr::column("x")],
            distinct: false,
        }],
        schema: PlanSchema::new(vec![SchemaField::new("s", DataType::Int64)]),
    });
    for morsel in [false, true] {
        let mut config = ExecutionConfig::default();
        config.enable_morsel_execution = morsel;
        let mut planner =
            PhysicalPlanner::with_config(Arc::new(MemoryPool::new(32 * 1024 * 1024)), config);
        planner.register_table("t", Arc::new(ParquetTable::try_new(&path).unwrap()));
        let physical = planner.create_physical_plan(&plan).unwrap();
        let mut values = Vec::new();
        for partition in 0..physical.output_partitions() {
            for batch in physical
                .execute(partition)
                .await
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap()
            {
                values.extend(
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter(),
                );
            }
        }
        assert_eq!(
            values,
            vec![Some(34)],
            "morsel={morsel}, plan={}",
            physical.name()
        );
    }
}
