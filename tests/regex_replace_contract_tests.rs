use arrow::array::{ArrayRef, StringArray};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

async fn run(
    columns: Vec<Vec<Option<&str>>>,
    sql: &str,
) -> query_engine::Result<Vec<Option<String>>> {
    let names = ["s", "p", "r", "o"];
    let batch = RecordBatch::try_from_iter(
        columns
            .into_iter()
            .enumerate()
            .map(|(i, values)| (names[i], Arc::new(StringArray::from(values)) as ArrayRef)),
    )
    .unwrap();
    let mut ctx = ExecutionContext::new();
    // Two batches also exercise batch-local cache reset.
    ctx.register_table("t", batch.schema(), vec![batch.clone(), batch]);
    let result = ctx.sql(sql).await?;
    Ok(result
        .batches
        .iter()
        .flat_map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.map(str::to_owned))
        })
        .collect())
}

#[tokio::test]
async fn four_argument_replacement_matches_pinned_duckdb_fixtures() {
    let fixtures = include_str!("fixtures/regex_replace_duckdb_1_4_4.jsonl")
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let columns = (0..4)
        .map(|col| {
            fixtures
                .iter()
                .map(|fixture| fixture["args"][col].as_str())
                .collect()
        })
        .collect();
    let actual = run(columns, "SELECT REGEXP_REPLACE(s,p,r,o) FROM t")
        .await
        .unwrap();
    let expected = fixtures
        .iter()
        .map(|fixture| Some(fixture["expected"].as_str().unwrap().to_owned()))
        .collect::<Vec<_>>();
    assert_eq!(actual, [expected.clone(), expected].concat());
}

#[tokio::test]
async fn three_argument_global_dollar_captures_and_nulls_are_preserved() {
    let actual = run(
        vec![
            vec![Some("abcabc"), Some("ab"), Some("ab"), None],
            vec![Some("(a)(b)"), Some("(?P<letter>a)b"), Some("a"), Some("[")],
            vec![Some("$2$1"), Some("${letter}"), Some(r"\$"), Some("x")],
        ],
        "SELECT REGEXP_REPLACE(s,p,r) FROM t",
    )
    .await
    .unwrap();
    let expected = vec![
        Some("bacbac".to_owned()),
        Some("a".to_owned()),
        Some("$b".to_owned()),
        None,
    ];
    assert_eq!(actual, [expected.clone(), expected].concat());
    let actual = run(
        vec![
            vec![Some("ab"); 4],
            vec![None, Some("a"), Some("a"), Some("a")],
            vec![Some("x"), None, Some("x"), Some("x")],
            vec![Some(""), Some(""), None, Some("g")],
        ],
        "SELECT REGEXP_REPLACE(s,p,r,o) FROM t",
    )
    .await
    .unwrap();
    assert_eq!(
        actual,
        [
            vec![None, None, None, Some("xb".to_owned())],
            vec![None, None, None, Some("xb".to_owned())]
        ]
        .concat()
    );
}

#[tokio::test]
async fn invalid_patterns_flags_and_replacements_refuse_explicitly() {
    for (pattern, replacement, options) in [
        ("[", "x", ""),
        ("a", "x", "m"),
        ("a", r"\q", ""),
        ("a", r"\1", ""),
        ("a", "\\", ""),
    ] {
        let error = run(
            vec![
                vec![Some("abc")],
                vec![Some(pattern)],
                vec![Some(replacement)],
                vec![Some(options)],
            ],
            "SELECT REGEXP_REPLACE(s,p,r,o) FROM t",
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("REGEXP_REPLACE"), "{error}");
    }
    assert!(run(
        vec![vec![Some("abc")], vec![Some("a")], vec![Some("$9")]],
        "SELECT REGEXP_REPLACE(s,p,r) FROM t"
    )
    .await
    .is_err());
}

#[tokio::test]
async fn repeated_constant_pattern_and_encoded_strings_preserve_values() {
    use arrow::datatypes::DataType;
    let array = StringArray::from(vec![Some("ab.ab"), None, Some("é.é")]);
    for data_type in [
        DataType::Utf8View,
        DataType::LargeUtf8,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
    ] {
        let values = arrow::compute::cast(&array, &data_type).unwrap();
        let batch = RecordBatch::try_from_iter(vec![("s", values)]).unwrap();
        let mut ctx = ExecutionContext::new();
        ctx.register_table("t", batch.schema(), vec![batch]);
        let result = ctx
            .sql("SELECT REGEXP_REPLACE(s, '[.]', '-', 'g') FROM t")
            .await
            .unwrap();
        let actual = result
            .batches
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
            })
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![Some("ab-ab"), None, Some("é-é")]);
    }
}
