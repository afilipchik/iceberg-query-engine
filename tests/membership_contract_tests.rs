//! SQL three-valued membership, independent of benchmark data.
use arrow::array::{Array, ArrayRef, BooleanArray, Decimal128Array, Int64Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use query_engine::{ExecutionContext, QueryResult};
use std::sync::Arc;

fn register(context: &mut ExecutionContext, name: &str, values: ArrayRef) {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "v",
            values.data_type().clone(),
            true,
        )])),
        vec![values],
    )
    .unwrap();
    let midpoint = batch.num_rows() / 2;
    context.register_table(
        name,
        batch.schema(),
        vec![
            batch.slice(0, midpoint),
            batch.slice(midpoint, batch.num_rows() - midpoint),
        ],
    );
}

fn booleans(result: &QueryResult) -> Vec<Option<bool>> {
    result
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        })
        .collect()
}

#[tokio::test]
async fn list_match_dominates_unknown() {
    let ctx = ExecutionContext::new();
    for (sql, expected) in [
        ("SELECT 1 IN (NULL, 1)", Some(true)),
        ("SELECT 1 NOT IN (NULL, 1)", Some(false)),
        ("SELECT 2 IN (NULL, 1)", None),
        ("SELECT 2 NOT IN (NULL, 1)", None),
        ("SELECT NULL IN (1)", None),
        ("SELECT NULL NOT IN (1)", None),
    ] {
        assert_eq!(
            booleans(&ctx.sql(sql).await.unwrap()),
            vec![expected],
            "{sql}"
        );
    }
}

#[tokio::test]
async fn subquery_membership_truth_table_across_batches() {
    for workers in [1, 2, 4] {
        let mut ctx = ExecutionContext::new().with_parallel_partitions(workers);
        register(
            &mut ctx,
            "lhs",
            Arc::new(Int64Array::from(vec![None, Some(1), Some(2)])),
        );
        for (rhs, expected) in [
            (vec![], vec![Some(false), Some(false), Some(false)]),
            (vec![Some(1), Some(1)], vec![None, Some(true), Some(false)]),
            (vec![Some(1), None], vec![None, Some(true), None]),
            (vec![None], vec![None, None, None]),
        ] {
            register(&mut ctx, "rhs", Arc::new(Int64Array::from(rhs)));
            for negated in [false, true] {
                let op = if negated { "NOT IN" } else { "IN" };
                // ORDER BY the source key ensures cross-partition order is explicit.
                let sql =
                    format!("SELECT v {op} (SELECT v FROM rhs) FROM lhs ORDER BY v NULLS FIRST");
                let expected = expected
                    .iter()
                    .map(|v| v.map(|v| v != negated))
                    .collect::<Vec<_>>();
                assert_eq!(
                    booleans(&ctx.sql(&sql).await.unwrap()),
                    expected,
                    "{sql}; workers={workers}"
                );
                let sql = format!("SELECT v FROM lhs WHERE v {op} (SELECT v FROM rhs)");
                assert_eq!(
                    ctx.sql(&sql)
                        .await
                        .unwrap()
                        .batches
                        .iter()
                        .map(|b| b.num_rows())
                        .sum::<usize>(),
                    expected.iter().filter(|v| **v == Some(true)).count(),
                    "{sql}"
                );
            }
        }
    }
}

#[tokio::test]
async fn exact_membership_preserves_integer_and_decimal_domains() {
    let mut ctx = ExecutionContext::new();
    register(&mut ctx, "u", Arc::new(UInt64Array::from(vec![u64::MAX])));
    register(&mut ctx, "i", Arc::new(Int64Array::from(vec![-1])));
    assert_eq!(
        booleans(
            &ctx.sql("SELECT v IN (SELECT v FROM u) FROM i")
                .await
                .unwrap()
        ),
        vec![Some(false)]
    );
    let wide = (1_i128 << 100) + 1;
    register(
        &mut ctx,
        "d",
        Arc::new(
            Decimal128Array::from(vec![wide, wide + 1])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        ),
    );
    register(
        &mut ctx,
        "r",
        Arc::new(
            Decimal128Array::from(vec![wide])
                .with_precision_and_scale(38, 2)
                .unwrap(),
        ),
    );
    assert_eq!(
        booleans(
            &ctx.sql("SELECT v IN (SELECT v FROM r) FROM d ORDER BY v")
                .await
                .unwrap()
        ),
        vec![Some(true), Some(false)]
    );
}

#[tokio::test]
async fn dictionary_membership_observes_null_values_and_keys() {
    use arrow::array::{DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::Int32Type;
    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), Some(1), None, Some(2)]),
        Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
    )
    .unwrap();
    let mut ctx = ExecutionContext::new();
    register(&mut ctx, "d", Arc::new(dictionary));
    register(
        &mut ctx,
        "r",
        Arc::new(StringArray::from(vec![Some("a"), None])),
    );
    for (sql, mut expected) in [
        (
            "SELECT v IN ('a') FROM d",
            vec![Some(true), None, None, Some(false)],
        ),
        (
            "SELECT v IN (SELECT v FROM r) FROM d",
            vec![Some(true), None, None, None],
        ),
        (
            "SELECT v NOT IN ('a') FROM d",
            vec![Some(false), None, None, Some(true)],
        ),
    ] {
        let mut actual = booleans(&ctx.sql(sql).await.unwrap());
        actual.sort();
        expected.sort();
        assert_eq!(actual, expected, "{sql}");
    }
}

#[tokio::test]
async fn correlated_not_in_handles_empty_and_nullable_rhs() {
    let mut ctx = ExecutionContext::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int64, false),
        Field::new("v", DataType::Int64, true),
    ]));
    let outer = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(Int64Array::from(vec![Some(9), Some(9), None])),
        ],
    )
    .unwrap();
    let inner = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![None, Some(1)])),
        ],
    )
    .unwrap();
    ctx.register_table("outer_t", schema.clone(), vec![outer]);
    ctx.register_table("inner_t", schema, vec![inner]);
    for predicate in ["i.k = o.k", "i.k IN (o.k)", "i.k BETWEEN o.k AND o.k"] {
        for negated in [false, true] {
            let op = if negated { "NOT IN" } else { "IN" };
            let sql = format!("SELECT o.v {op} (SELECT i.v FROM inner_t i WHERE {predicate}) FROM outer_t o ORDER BY o.k");
            assert_eq!(
                booleans(&ctx.sql(&sql).await.unwrap()),
                vec![None, Some(negated), Some(negated)],
                "{sql}"
            );
            let sql = format!("SELECT o.k FROM outer_t o WHERE o.v {op} (SELECT i.v FROM inner_t i WHERE {predicate})");
            assert_eq!(
                ctx.sql(&sql)
                    .await
                    .unwrap()
                    .batches
                    .iter()
                    .map(|b| b.num_rows())
                    .sum::<usize>(),
                if negated { 2 } else { 0 },
                "{sql}"
            );
        }
    }
    let sql = "SELECT o.k FROM outer_t o WHERE o.v NOT IN (SELECT i.v FROM inner_t i WHERE i.k = o.k) ORDER BY o.k";
    let result = ctx.sql(sql).await.unwrap();
    let keys = result
        .batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    assert_eq!(keys, vec![2, 3]);
}

#[test]
fn empty_list_identity_includes_null_strings_and_dictionaries() {
    use arrow::array::{DictionaryArray, Int32Array, StringArray};
    use arrow::datatypes::Int32Type;
    use query_engine::physical::operators::evaluate_expr;
    use query_engine::planner::Expr;
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![None, Some("a")])),
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![None, Some(0)]),
                Arc::new(StringArray::from(vec!["a"])),
            )
            .unwrap(),
        ),
    ];
    for array in arrays {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "v",
                array.data_type().clone(),
                true,
            )])),
            vec![array],
        )
        .unwrap();
        for negated in [false, true] {
            let result = evaluate_expr(
                &batch,
                &Expr::InList {
                    expr: Box::new(Expr::column("v")),
                    list: vec![],
                    negated,
                },
            )
            .unwrap();
            let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
            assert_eq!(
                result.iter().collect::<Vec<_>>(),
                vec![Some(negated), Some(negated)]
            );
        }
    }
}

#[tokio::test]
async fn nested_outer_scope_is_refused_instead_of_capturing_same_named_column() {
    let mut ctx = ExecutionContext::new();
    register(&mut ctx, "outer_t", Arc::new(Int64Array::from(vec![1, 2])));
    register(&mut ctx, "inner_t", Arc::new(Int64Array::from(vec![3, 4])));
    let error = ctx
        .sql("SELECT o.v IN (SELECT i.v FROM inner_t i WHERE i.v IN (SELECT o.v)) FROM outer_t o")
        .await
        .unwrap_err();
    assert!(
        matches!(
            error,
            query_engine::error::QueryError::ColumnNotFound(_)
                | query_engine::error::QueryError::NotImplemented(_)
        ),
        "{error}"
    );
}
