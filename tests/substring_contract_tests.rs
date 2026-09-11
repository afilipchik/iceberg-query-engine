use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

fn oracle() -> serde_json::Value {
    serde_json::from_str(include_str!("fixtures/substring_duckdb_1_4_4.json")).unwrap()
}

#[tokio::test]
async fn dynamic_substring_matches_independent_null_unicode_signed_oracle() {
    let oracle = oracle();
    let cases = oracle["cases"].as_array().unwrap();
    let batch = RecordBatch::try_from_iter([
        (
            "id",
            Arc::new(Int64Array::from_iter_values(0..cases.len() as i64)) as ArrayRef,
        ),
        (
            "s",
            Arc::new(StringArray::from(
                cases
                    .iter()
                    .map(|c| c["input"][0].as_str())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "p",
            Arc::new(Int64Array::from(
                cases
                    .iter()
                    .map(|c| c["input"][1].as_i64())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "n",
            Arc::new(Int64Array::from(
                cases
                    .iter()
                    .map(|c| c["input"][2].as_i64())
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .unwrap();
    let mut ctx = ExecutionContext::new();
    ctx.register_table(
        "t",
        batch.schema(),
        (0..batch.num_rows())
            .step_by(127)
            .map(|offset| batch.slice(offset, 127.min(batch.num_rows() - offset)))
            .collect(),
    );
    let result = ctx
        .sql("SELECT substring(s,p,n) AS value FROM t ORDER BY id")
        .await
        .unwrap();
    let actual: Vec<_> = result
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|s| s.map(str::to_owned))
        })
        .collect();
    let expected: Vec<_> = cases
        .iter()
        .map(|c| c["expected"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn constants_nullable_arguments_and_two_argument_form_agree() {
    let batch = RecordBatch::try_from_iter([(
        "s",
        Arc::new(StringArray::from(vec![Some("aé🙂z"), None, Some("")])) as ArrayRef,
    )])
    .unwrap();
    let mut ctx = ExecutionContext::new();
    ctx.register_batch("t", batch);
    for (expr, expected) in [
        ("substring(s,1,2)", vec![Some("aé"), None, Some("")]),
        ("substring(s FOR 2)", vec![Some("aé"), None, Some("")]),
        ("substring(s,-2,2)", vec![Some("🙂z"), None, Some("")]),
        ("substring(s,0,2)", vec![Some("a"), None, Some("")]),
        ("substring(s,3,-2)", vec![Some("aé"), None, Some("")]),
        ("substring(s,-2)", vec![Some("🙂z"), None, Some("")]),
        ("substring(s,0)", vec![Some("aé🙂z"), None, Some("")]),
        (
            "substring(s,CAST(NULL AS BIGINT),2)",
            vec![None, None, None],
        ),
        (
            "substring(s,1,CAST(NULL AS BIGINT))",
            vec![None, None, None],
        ),
    ] {
        let result = ctx.sql(&format!("SELECT {expr} FROM t")).await.unwrap();
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
                .collect::<Vec<_>>(),
            "{expr}"
        );
    }
}

#[tokio::test]
async fn sliced_dictionary_strings_and_small_integers_preserve_nulls() {
    use arrow::array::{DictionaryArray, Int16Array, Int8Array};
    use arrow::datatypes::Int8Type;
    let strings: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), Some(1), None, Some(0)]),
            Arc::new(StringArray::from(vec!["abcd", "aé🙂z"])),
        )
        .unwrap(),
    );
    let batch = RecordBatch::try_from_iter([
        ("s", strings),
        (
            "p",
            Arc::new(Int16Array::from(vec![1, -2, 1, 0])) as ArrayRef,
        ),
        (
            "n",
            Arc::new(Int8Array::from(vec![Some(2), Some(2), Some(2), None])) as ArrayRef,
        ),
    ])
    .unwrap()
    .slice(1, 3);
    let mut ctx = ExecutionContext::new();
    ctx.register_batch("t", batch);
    let result = ctx.sql("SELECT substring(s,p,n) FROM t").await.unwrap();
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
    assert_eq!(actual, vec![Some("🙂z".to_string()), None, None]);
}

#[tokio::test]
async fn unsupported_arity_and_noninteger_position_are_errors() {
    let ctx = ExecutionContext::new();
    for sql in [
        "SELECT substring('abcd')",
        "SELECT substring('abcd',1,2,3)",
        "SELECT substring('abcd',TRUE,2)",
    ] {
        assert!(ctx.sql(sql).await.is_err(), "{sql}");
    }
}
