//! Independent SQL-value checks for lossless qualified-column identity.
use arrow::array::{Array, ArrayRef, Int64Array};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use query_engine::ExecutionContext;
use std::sync::Arc;

fn context(parquet: bool) -> (ExecutionContext, Option<tempfile::TempDir>) {
    let mut ctx = ExecutionContext::new().with_parallel_partitions(3);
    let directory = parquet.then(|| tempfile::tempdir().unwrap());
    for (name, id_name, value_name, ids, values) in [
        (
            "left_table",
            "aid",
            "b.c",
            vec![1, 2, 3, 4],
            vec![Some(1), Some(3), None, Some(1)],
        ),
        (
            "right_table",
            "bid",
            "c",
            vec![10, 20, 30],
            vec![Some(2), Some(3), None],
        ),
    ] {
        let batch = RecordBatch::try_from_iter(vec![
            (id_name, Arc::new(Int64Array::from(ids)) as ArrayRef),
            (value_name, Arc::new(Int64Array::from(values)) as ArrayRef),
        ])
        .unwrap();
        let batches = vec![batch.slice(0, 2), batch.slice(2, batch.num_rows() - 2)];
        if let Some(directory) = &directory {
            let path = directory.path().join(format!("{name}.parquet"));
            let mut writer =
                ArrowWriter::try_new(std::fs::File::create(&path).unwrap(), batch.schema(), None)
                    .unwrap();
            for part in &batches {
                writer.write(part).unwrap();
                writer.flush().unwrap();
            }
            writer.close().unwrap();
            ctx.register_parquet(name, &path).unwrap();
        } else {
            ctx.register_table(name, batch.schema(), batches);
        }
    }
    (ctx, directory)
}

async fn check(kind: usize) {
    let base = "FROM left_table a CROSS JOIN right_table \"a.b\"";
    let sql = match kind {
        0 => format!("SELECT a.aid, \"a.b\".bid {base}"),
        1 => format!("SELECT a.aid, \"a.b\".bid, a.\"b.c\" AS x, \"a.b\".c AS y {base}"),
        2 => format!("SELECT a.aid, \"a.b\".bid {base} WHERE a.\"b.c\"=1"),
        3 => format!("SELECT a.aid, \"a.b\".bid {base} WHERE \"a.b\".c=2"),
        4 => format!("SELECT a.aid, \"a.b\".bid {base} WHERE a.\"b.c\"=1 OR \"a.b\".c=2"),
        5 => format!(
            "SELECT a.aid, \"a.b\".bid {base} WHERE a.\"b.c\" IN (1,NULL) OR \"a.b\".c IN (2,NULL)"
        ),
        _ => unreachable!(),
    };
    // Compute expected SQL filter truth directly from original typed source cells.
    let mut expected = Vec::new();
    for (aid, x) in [(1, Some(1)), (2, Some(3)), (3, None), (4, Some(1))] {
        for (bid, y) in [(10, Some(2)), (20, Some(3)), (30, None)] {
            let keep = match kind {
                2 => x == Some(1),
                3 => y == Some(2),
                4 | 5 => x == Some(1) || y == Some(2),
                _ => true,
            };
            if keep {
                let mut row = vec![Some(aid), Some(bid)];
                if kind == 1 {
                    row.extend([x, y]);
                }
                expected.push(row);
            }
        }
    }
    expected.sort();
    for parquet in [false, true] {
        let (ctx, _directory) = context(parquet);
        let result = ctx.sql(&sql).await.unwrap();
        let mut actual = Vec::new();
        for batch in &result.batches {
            let arrays: Vec<_> = batch
                .columns()
                .iter()
                .map(|a| {
                    a.as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("exact Int64 result domain")
                })
                .collect();
            for i in 0..batch.num_rows() {
                actual.push(
                    arrays
                        .iter()
                        .map(|a| if a.is_null(i) { None } else { Some(a.value(i)) })
                        .collect::<Vec<_>>(),
                );
            }
        }
        actual.sort();
        assert_eq!(actual, expected, "parquet={parquet}; sql={sql}");
    }
}

#[tokio::test]
async fn distinct_id_columns_control() {
    check(0).await;
}
#[tokio::test]
async fn colliding_qualified_projection_preserves_both_values() {
    check(1).await;
}
#[tokio::test]
async fn left_predicate_control() {
    check(2).await;
}
#[tokio::test]
async fn right_predicate_control() {
    check(3).await;
}
#[tokio::test]
async fn colliding_qualified_or_preserves_filter_matches() {
    check(4).await;
}
#[tokio::test]
async fn colliding_qualified_in_or_preserves_null_filter_truth() {
    check(5).await;
}

#[tokio::test]
async fn dictionary_retyping_preserves_identity_across_join_projection_and_filter() {
    use arrow::array::{StringArray, StringDictionaryBuilder};
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    let mut ctx = ExecutionContext::new().with_parallel_partitions(3);
    for (table, id_name, value_name, ids, values) in [
        (
            "left_table",
            "aid",
            "b.c",
            vec![1, 2, 3, 4],
            vec![Some("A"), Some("C"), None, Some("A")],
        ),
        (
            "right_table",
            "bid",
            "c",
            vec![10, 20, 30],
            vec![Some("B"), Some("C"), None],
        ),
    ] {
        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        for value in values {
            match value {
                Some(v) => {
                    builder.append(v).unwrap();
                }
                None => builder.append_null(),
            }
        }
        let batch = RecordBatch::try_from_iter(vec![
            (id_name, Arc::new(Int64Array::from(ids)) as ArrayRef),
            (value_name, Arc::new(builder.finish()) as ArrayRef),
        ])
        .unwrap();
        let logical = Arc::new(Schema::new(vec![
            Field::new(id_name, DataType::Int64, true),
            Field::new(value_name, DataType::Utf8, true),
        ]));
        ctx.register_table(
            table,
            logical,
            vec![batch.slice(0, 2), batch.slice(2, batch.num_rows() - 2)],
        );
    }
    for filtered in [false, true] {
        let mut sql="SELECT a.aid, \"a.b\".bid, a.\"b.c\" AS x, \"a.b\".c AS y FROM left_table a CROSS JOIN right_table \"a.b\"".to_string();
        if filtered {
            sql.push_str(" WHERE a.\"b.c\"='A' OR \"a.b\".c='B'");
        }
        let result = ctx.sql(&sql).await.unwrap();
        let mut actual = Vec::new();
        for batch in &result.batches {
            let aid = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let bid = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for column in &batch.columns()[2..] {
                assert!(
                    match column.data_type() {
                        DataType::Utf8 => true,
                        DataType::Dictionary(_, values) => values.as_ref() == &DataType::Utf8,
                        _ => false,
                    },
                    "exact UTF8 logical domain"
                );
            }
            // Decode only the physical dictionary representation of the same UTF8 domain.
            let x = arrow::compute::cast(batch.column(2), &DataType::Utf8).unwrap();
            let y = arrow::compute::cast(batch.column(3), &DataType::Utf8).unwrap();
            let x = x.as_any().downcast_ref::<StringArray>().unwrap();
            let y = y.as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..batch.num_rows() {
                actual.push((
                    aid.value(i),
                    bid.value(i),
                    (!x.is_null(i)).then(|| x.value(i).to_owned()),
                    (!y.is_null(i)).then(|| y.value(i).to_owned()),
                ));
            }
        }
        let mut expected = Vec::new();
        for (aid, x) in [(1, Some("A")), (2, Some("C")), (3, None), (4, Some("A"))] {
            for (bid, y) in [(10, Some("B")), (20, Some("C")), (30, None)] {
                if !filtered || x == Some("A") || y == Some("B") {
                    expected.push((aid, bid, x.map(str::to_owned), y.map(str::to_owned)));
                }
            }
        }
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected, "filtered={filtered}");
    }
}

#[test]
fn inferred_aggregate_schema_reads_execution_identity_and_preserves_types() {
    use arrow::array::Float64Array;
    use arrow::datatypes::DataType;
    use query_engine::physical::operators::{HashAggregateExec, MemoryTableExec};
    use query_engine::physical::PhysicalOperator;
    use query_engine::planner::{Expr, PlanSchema, SchemaField};
    let logical = PlanSchema::new(vec![
        SchemaField::new("b.c", DataType::Int64).with_relation("a"),
        SchemaField::new("c", DataType::Float64).with_relation("a.b"),
    ]);
    let schema = logical.to_arrow_schema_ref();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Float64Array::from(vec![3.0, 4.0])),
        ],
    )
    .unwrap();
    let input = Arc::new(MemoryTableExec::new("fixture", schema, vec![batch], None));
    let aggregate = HashAggregateExec::try_new(
        input,
        vec![
            Expr::qualified_column("a", "b.c"),
            Expr::qualified_column("a.b", "c"),
        ],
        vec![],
    )
    .unwrap();
    assert_eq!(
        PlanSchema::from_qualified_arrow(aggregate.schema().as_ref()),
        logical
    );
}

#[tokio::test]
async fn shared_cte_aliases_keep_distinct_binding_namespaces() {
    for parquet in [false, true] {
        let (ctx, _directory) = context(parquet);
        let sql="WITH c AS (SELECT aid FROM left_table) SELECT x.aid AS x, y.aid AS y FROM c x CROSS JOIN c y";
        let result = ctx.sql(sql).await.unwrap();
        let mut actual = Vec::new();
        for batch in &result.batches {
            let x = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let y = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                actual.push((x.value(i), y.value(i)));
            }
        }
        actual.sort();
        let expected: Vec<_> = (1..=4).flat_map(|x| (1..=4).map(move |y| (x, y))).collect();
        assert_eq!(actual, expected);
    }
}

#[tokio::test]
async fn derived_alias_preserves_dotted_column_values() {
    for parquet in [false, true] {
        let (ctx, _directory) = context(parquet);
        let result=ctx.sql("SELECT \"d.e\".aid, \"d.e\".\"b.c\" FROM (SELECT aid, \"b.c\" FROM left_table) \"d.e\"").await.unwrap();
        let mut actual = Vec::new();
        for batch in &result.batches {
            let id = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let value = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                actual.push((id.value(i), (!value.is_null(i)).then(|| value.value(i))));
            }
        }
        actual.sort();
        assert_eq!(
            actual,
            vec![(1, Some(1)), (2, Some(3)), (3, None), (4, Some(1))]
        );
    }
}

#[tokio::test]
async fn pruned_derived_alias_keeps_the_emitted_column_identity() {
    for parquet in [false, true] {
        let (ctx, _directory) = context(parquet);
        let result = ctx
            .sql(r#"SELECT d."b.c" FROM (SELECT aid, "b.c" FROM left_table) d WHERE d.aid > 1"#)
            .await
            .unwrap();
        let mut actual = Vec::new();
        for batch in &result.batches {
            assert_eq!(batch.num_columns(), 1);
            let values = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            actual.extend(values.iter());
        }
        actual.sort();
        assert_eq!(actual, vec![None, Some(1), Some(3)]);
    }
}

#[tokio::test]
async fn shared_cte_consumers_keep_different_projected_roots() {
    for parquet in [false, true] {
        let (ctx, _directory) = context(parquet);
        let result = ctx.sql(r#"WITH c AS (SELECT aid, "b.c" FROM left_table) SELECT x.aid, y."b.c" FROM c x CROSS JOIN c y"#).await.unwrap();
        let mut actual = Vec::new();
        for batch in &result.batches {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let values = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            actual.extend(ids.iter().zip(values.iter()));
        }
        actual.sort();
        let mut expected: Vec<_> = (1..=4)
            .flat_map(|id| {
                [Some(1), Some(3), None, Some(1)]
                    .into_iter()
                    .map(move |value| (Some(id), value))
            })
            .collect();
        expected.sort();
        assert_eq!(actual, expected);
    }
}
