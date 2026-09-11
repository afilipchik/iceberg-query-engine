use arrow::array::{Array, Decimal128Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use query_engine::ExecutionContext;

#[tokio::test]
async fn null_position_does_not_choose_values_string_type() {
    let ctx = ExecutionContext::new();
    for (sql, expected) in [
        (
            "SELECT v FROM (VALUES (NULL),('abc'),('abc')) t(v)",
            vec![None, Some("abc"), Some("abc")],
        ),
        (
            "SELECT v FROM (VALUES ('abc'),(NULL),('abc')) t(v)",
            vec![Some("abc"), None, Some("abc")],
        ),
    ] {
        let result = ctx.sql(sql).await.unwrap();
        let actual: Vec<_> = result
            .batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|s| s.map(str::to_owned))
            })
            .collect();
        assert_eq!(
            actual,
            expected
                .into_iter()
                .map(|s| s.map(str::to_owned))
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn values_widen_integer_columns_and_cast_every_row() {
    let ctx = ExecutionContext::new();
    let result=ctx.sql("SELECT v FROM (VALUES (CAST(NULL AS INTEGER)),(CAST(3000000000 AS BIGINT)),(CAST(7 AS SMALLINT))) t(v)").await.unwrap();
    let actual: Vec<_> = result
        .batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter()
        })
        .collect();
    assert_eq!(actual, vec![None, Some(3000000000), Some(7)]);
}

#[tokio::test]
async fn values_decimal_scales_preserve_exact_coefficients() {
    let ctx = ExecutionContext::new();
    let result=ctx.sql("SELECT v FROM (VALUES (CAST(1.2 AS DECIMAL(8,1))),(CAST(2.345 AS DECIMAL(8,3))),(NULL)) t(v)").await.unwrap();
    let mut actual = Vec::new();
    for b in &result.batches {
        let values = b
            .column(0)
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .unwrap();
        assert!(matches!(values.data_type(), DataType::Decimal128(_, 3)));
        actual.extend(values.iter());
    }
    assert_eq!(actual, vec![Some(1200), Some(2345), None]);
}

#[tokio::test]
async fn values_shape_and_conversion_fail_explicitly() {
    let ctx = ExecutionContext::new();
    for sql in ["VALUES (1),(1,2)", "VALUES (1),('not an integer')"] {
        assert!(ctx.sql(sql).await.is_err(), "{sql}");
    }
}

#[tokio::test]
async fn original_dynamic_substring_reproducer_executes_through_values() {
    let ctx = ExecutionContext::new();
    let result=ctx.sql("SELECT substring(s,p,n) FROM (VALUES ('abcd',1,2),(NULL,1,2),('abcd',NULL,2),('abcd',1,NULL),('abcd',0,2),('abcd',-2,2),('abcd',3,-2)) t(s,p,n)").await.unwrap();
    let actual: Vec<_> = result
        .batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|s| s.map(str::to_owned))
        })
        .collect();
    assert_eq!(
        actual,
        vec![
            Some("ab"),
            None,
            None,
            None,
            Some("a"),
            Some("cd"),
            Some("ab")
        ]
        .into_iter()
        .map(|s| s.map(str::to_owned))
        .collect::<Vec<_>>()
    );
}
