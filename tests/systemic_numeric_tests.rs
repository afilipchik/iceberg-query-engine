//! Independent exact-value regressions, independent of benchmark SQL/data.
use arrow::array::{Array, Decimal128Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use query_engine::{ExecutionContext, QueryResult};
use std::fs::File;
use std::sync::Arc;

fn decimal_batch(precision: u8, scale: i8, values: Vec<Option<i128>>) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Decimal128(precision, scale), true),
    ]));
    let groups = (0..values.len())
        .map(|i| if i < 3 { "a" } else { "b" })
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(groups)),
            Arc::new(
                Decimal128Array::from(values)
                    .with_precision_and_scale(precision, scale)
                    .unwrap(),
            ),
        ],
    )
    .unwrap()
}

fn values() -> RecordBatch {
    decimal_batch(
        15,
        2,
        vec![Some(125), Some(250), None, Some(125), Some(-50), Some(0)],
    )
}

fn decimal_rows(result: &QueryResult, column: usize, scale: i8) -> Vec<Option<i128>> {
    result
        .batches
        .iter()
        .flat_map(|batch| {
            let array = batch
                .column(column)
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .expect("an exact decimal expression must return an exact decimal array");
            assert_eq!(array.scale(), scale);
            (0..array.len())
                .map(|row| (!array.is_null(row)).then(|| array.value(row)))
                .collect::<Vec<_>>()
        })
        .collect()
}

fn count(result: &QueryResult) -> i64 {
    result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn exact_decimal_aggregates_across_batches_and_partitions() {
    for workers in [1, 4] {
        let batch = values();
        let mut context = ExecutionContext::new().with_parallel_partitions(workers);
        context.register_table(
            "d",
            batch.schema(),
            vec![batch.slice(0, 3), batch.slice(3, 3)],
        );
        for (sql, expected, scale) in [
            ("SELECT SUM(v) FROM d", vec![Some(450)], 2),
            ("SELECT SUM(v*v) FROM d", vec![Some(96250)], 4),
            ("SELECT SUM(v*(1-v)) FROM d", vec![Some(-51250)], 4),
            (
                "SELECT g, SUM(v) FROM d GROUP BY g ORDER BY g",
                vec![Some(375), Some(75)],
                2,
            ),
            ("SELECT SUM(v) FROM d WHERE v IS NULL", vec![None], 2),
            ("SELECT SUM(v) FROM d WHERE 1=0", vec![None], 2),
        ] {
            let result = context
                .sql(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"));
            let column = if sql.starts_with("SELECT g,") { 1 } else { 0 };
            assert_eq!(
                decimal_rows(&result, column, scale),
                expected,
                "{sql}; workers={workers}"
            );
        }
    }
}

#[tokio::test]
async fn decimal_predicates_do_not_compare_scaled_integers_as_whole_numbers() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("d.parquet");
    let batch = values();
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(3))
        .build();
    let mut writer = ArrowWriter::try_new(
        File::create(&path).unwrap(),
        batch.schema(),
        Some(properties),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    for workers in [1, 4] {
        let mut context = ExecutionContext::new().with_parallel_partitions(workers);
        context.register_parquet("d", &path).unwrap();
        for (predicate, expected) in [
            ("v < 2", 4),
            ("v < 2.0", 4),
            ("2 > v", 4),
            ("v > 1", 3),
            ("NOT (v > 1)", 2),
            ("v = 1", 0),
            ("v BETWEEN 1 AND 2", 2),
        ] {
            let sql = format!("SELECT COUNT(*) FROM d WHERE {predicate}");
            assert_eq!(
                count(&context.sql(&sql).await.unwrap()),
                expected,
                "{sql}; workers={workers}"
            );
        }
    }
}

#[tokio::test]
async fn decimal_sum_retains_full_128_bit_coefficient_and_negative_scale() {
    for (scale, value) in [(2, 10_i128.pow(36) + 17), (-2, 1234)] {
        let batch = decimal_batch(38, scale, vec![Some(value), None, Some(value)]);
        let mut context = ExecutionContext::new();
        context.register_batch("d", batch);
        let result = context.sql("SELECT SUM(v) FROM d").await.unwrap();
        assert_eq!(decimal_rows(&result, 0, scale), vec![Some(value * 2)]);
    }
}

#[tokio::test]
async fn decimal_overflow_is_an_error_not_a_wrapped_value_or_null() {
    let batch = decimal_batch(38, 0, vec![Some(10_i128.pow(38) - 1); 2]);
    let mut context = ExecutionContext::new();
    context.register_batch("d", batch);
    assert!(context.sql("SELECT SUM(v) FROM d").await.is_err());
}

#[tokio::test]
async fn decimal_distinct_and_extrema_preserve_values() {
    let value = 10_i128.pow(36) + 17;
    let batch = decimal_batch(38, 2, vec![Some(value), Some(value), None, Some(value - 1)]);
    let mut context = ExecutionContext::new();
    context.register_batch("d", batch);
    let result = context
        .sql("SELECT SUM(DISTINCT v), COUNT(DISTINCT v), MIN(v), MAX(v) FROM d")
        .await
        .unwrap();
    assert_eq!(decimal_rows(&result, 0, 2), vec![Some(value * 2 - 1)]);
    assert_eq!(
        result.batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        2
    );
    assert_eq!(decimal_rows(&result, 2, 2), vec![Some(value - 1)]);
    assert_eq!(decimal_rows(&result, 3, 2), vec![Some(value)]);
    let result = context
        .sql("SELECT SUM(DISTINCT v) FROM d WHERE v IS NULL")
        .await
        .unwrap();
    assert_eq!(decimal_rows(&result, 0, 2), vec![None]);
}

#[tokio::test]
async fn aliases_have_scope_and_execute_their_renaming() {
    let mut context = ExecutionContext::new();
    context.register_batch("d", values());
    for sql in [
        "SELECT k, SUM(v) FROM (SELECT g, v FROM d) AS t(k,v) GROUP BY k ORDER BY k",
        "SELECT g AS k, SUM(v) FROM d GROUP BY k ORDER BY k",
        "SELECT k, SUM(v) FROM d AS t(k,v) GROUP BY k ORDER BY k",
        "WITH c AS (SELECT g, v FROM d) SELECT k, SUM(v) FROM c AS t(k,v) GROUP BY k ORDER BY k",
        "SELECT k, SUM(v) FROM (SELECT g, v FROM d) AS t(k) GROUP BY k ORDER BY k",
    ] {
        let result = context
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(
            decimal_rows(&result, 1, 2),
            vec![Some(375), Some(75)],
            "{sql}"
        );
    }
    for sql in [
        "SELECT SUM(v) AS k FROM d GROUP BY k",
        "SELECT SUM(v) FROM d GROUP BY 1",
        "SELECT g AS k, v AS k FROM d GROUP BY k",
        "SELECT ROW_NUMBER() OVER () AS k FROM d GROUP BY k",
        "SELECT * FROM d AS t(a,b,c)",
        "SELECT g AS k FROM d WHERE k = 'a'",
    ] {
        assert!(context.sql(sql).await.is_err(), "must reject {sql}");
    }
}

#[test]
fn decimal_scalar_equality_ordering_and_rescale_are_exact() {
    use query_engine::planner::DecimalValue as D;
    use std::collections::HashSet;
    assert_eq!(D::new(123, -2), D::new(1230000, 2));
    assert_eq!(HashSet::from([D::new(1, 0), D::new(100, 2)]).len(), 1);
    assert!(D::new(10_i128.pow(37) + 1, 2) > D::new(10_i128.pow(37), 2));
    assert!(D::new(-123, -2) < D::new(-123, 2));
    assert_eq!(D::new(125, 2).rescale(4).unwrap(), 12500);
    assert!(D::new(125, 2).rescale(1).is_err());
    assert!(D::validate_precision(10_i128.pow(38), 38).is_err());
}

#[tokio::test]
async fn exact_decimal_spill_merges_match_an_independent_integer_oracle() {
    exact_decimal_group_oracle(2000, true).await;
}

#[tokio::test]
async fn exact_decimal_fitting_groups_match_an_independent_integer_oracle() {
    exact_decimal_group_oracle(1000, false).await;
}

async fn exact_decimal_group_oracle(group_count: i64, require_spill: bool) {
    use query_engine::ExecutionConfig;
    let directory = tempfile::tempdir().unwrap();
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Int64, false),
        Field::new("v", DataType::Decimal128(38, 2), false),
    ]));
    let coefficient = 10_i128.pow(20) + 17;
    let batches = (0..20)
        .map(|_| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from_iter_values(0..group_count)),
                    Arc::new(
                        Decimal128Array::from(vec![coefficient; group_count as usize])
                            .with_precision_and_scale(38, 2)
                            .unwrap(),
                    ),
                ],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut context = ExecutionContext::with_config(
        ExecutionConfig::new()
            .with_memory_limit(256 * 1024)
            .with_spill_path(directory.path().join("spill")),
    );
    context.register_table("d", schema, batches);
    let result = context
        .sql("SELECT g, SUM(v) FROM d GROUP BY g")
        .await
        .unwrap();
    if require_spill {
        let spill = result
            .metrics
            .spill_metrics
            .as_ref()
            .expect("must exercise the disk path");
        // More groups force the disk path even after startup ownership adapts.
        // Keep the same query budget and independently exact decimal oracle.
        assert!(spill.bytes_spilled > 0, "{spill:?}");
    }
    let mut keys = result
        .batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect::<Vec<_>>();
    keys.sort_unstable();
    assert_eq!(keys, (0..group_count).collect::<Vec<_>>());
    assert_eq!(
        decimal_rows(&result, 1, 2),
        vec![Some(coefficient * 20); group_count as usize]
    );
}

#[tokio::test]
async fn negated_integer_pruning_does_not_round_above_two_to_the_53() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("ints.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![9007199254740992_i64]))],
    )
    .unwrap();
    let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let mut context = ExecutionContext::new();
    context.register_parquet("d", path).unwrap();
    assert_eq!(
        count(
            &context
                .sql("SELECT COUNT(*) FROM d WHERE NOT(v = 9007199254740993)")
                .await
                .unwrap()
        ),
        1
    );
}

#[tokio::test]
async fn case_result_type_is_independent_of_branch_order() {
    let mut context = ExecutionContext::new();
    context.register_batch("d", values());
    for (sql, expected) in [
        ("SELECT SUM(CASE WHEN g='a' THEN 1 ELSE v END) FROM d", 375),
        ("SELECT SUM(CASE WHEN g='b' THEN v ELSE 1 END) FROM d", 375),
        (
            "SELECT SUM(CASE WHEN g='a' THEN NULL ELSE v END) FROM d",
            75,
        ),
    ] {
        let result = context
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("{sql}: {e}"));
        assert_eq!(decimal_rows(&result, 0, 2), vec![Some(expected)], "{sql}");
    }
}

#[tokio::test]
async fn scalar_subqueries_preserve_decimal_nulls_and_count_all_batches() {
    let mut context = ExecutionContext::new();
    context.register_batch("d", values());
    let result = context.sql("SELECT (SELECT MAX(v) FROM d)").await.unwrap();
    assert_eq!(decimal_rows(&result, 0, 2), vec![Some(250)]);
    let result = context
        .sql("SELECT (SELECT v FROM d WHERE v IS NULL)")
        .await
        .unwrap();
    assert_eq!(decimal_rows(&result, 0, 2), vec![None]);
    assert_eq!(
        count(
            &context
                .sql("SELECT COUNT(*) FROM d WHERE v=(SELECT MAX(v) FROM d)")
                .await
                .unwrap()
        ),
        1
    );
    let batch = decimal_batch(38, 2, vec![Some(10_i128.pow(36) + 17)]);
    context.register_table(
        "one",
        batch.schema(),
        vec![batch.slice(0, 0), batch.clone()],
    );
    let result = context.sql("SELECT (SELECT v FROM one)").await.unwrap();
    assert_eq!(
        decimal_rows(&result, 0, 2),
        vec![Some(10_i128.pow(36) + 17)]
    );
    context.register_table("two", batch.schema(), vec![batch.clone(), batch]);
    assert!(context.sql("SELECT (SELECT v FROM two)").await.is_err());
}

#[tokio::test]
async fn decimal_arithmetic_rejects_unrepresentable_scale_without_saturation() {
    let mut context = ExecutionContext::new();
    context.register_batch("d", decimal_batch(2, -100, vec![Some(12)]));
    assert!(context.sql("SELECT v*v FROM d").await.is_err());
    context.register_batch("d", decimal_batch(2, -2, vec![Some(12)]));
    let result = context.sql("SELECT v*v FROM d").await.unwrap();
    assert_eq!(decimal_rows(&result, 0, -4), vec![Some(144)]);
}
