//! Independent scalar aggregate oracles for colliding generated output names.
use arrow::array::{Array, Int64Array};
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

fn context() -> ExecutionContext {
    let batch = RecordBatch::try_from_iter(vec![(
        "x",
        Arc::new(Int64Array::from(vec![Some(1), Some(1), Some(2), None])) as Arc<dyn Array>,
    )])
    .unwrap();
    let mut ctx = ExecutionContext::new().with_parallel_partitions(2);
    ctx.register_table(
        "t",
        batch.schema(),
        vec![batch.slice(0, 2), batch.slice(2, 2)],
    );
    ctx
}

async fn assert_pair(sql: &str, expected: [i64; 2], names: [&str; 2]) {
    let result = context().sql(sql).await.unwrap_or_else(|e| {
        panic!("Query failed before value validation (binding/planning/execution error): {e:?}; SQL={sql}")
    });
    let mut rows = Vec::new();
    for batch in &result.batches {
        assert_eq!(batch.num_columns(), 2, "Unexpected output width; SQL={sql}");
        for (i, name) in names.iter().enumerate() {
            assert_eq!(
                batch.schema().field(i).name(),
                name,
                "Display label changed; SQL={sql}"
            );
            assert_eq!(
                batch.column(i).data_type(),
                &DataType::Int64,
                "Wrong output type; SQL={sql}"
            );
        }
        let a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let b = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        rows.extend(a.iter().zip(b.iter()).map(|(a, b)| [a, b]));
    }
    assert_eq!(
        rows,
        vec![[Some(expected[0]), Some(expected[1])]],
        "Completed with wrong independent values; SQL={sql}"
    );
}

#[tokio::test]
async fn unnamed_sum_and_distinct_sum_keep_separate_values() {
    assert_pair(
        "SELECT SUM(x), SUM(DISTINCT x) FROM t",
        [4, 3],
        ["SUM(x)", "SUM(x)"],
    )
    .await;
}

#[tokio::test]
async fn unnamed_case_sums_keep_separate_values() {
    assert_pair("SELECT SUM(CASE WHEN x = 1 THEN 1 ELSE 0 END), SUM(CASE WHEN x = 2 THEN 1 ELSE 0 END) FROM t", [2,1], ["SUM(CASE)", "SUM(CASE)"]).await;
}

#[tokio::test]
async fn explicitly_aliased_aggregate_pairs_are_positive_controls() {
    assert_pair(
        "SELECT SUM(x) AS a, SUM(DISTINCT x) AS b FROM t",
        [4, 3],
        ["a", "b"],
    )
    .await;
    assert_pair("SELECT SUM(CASE WHEN x = 1 THEN 1 ELSE 0 END) AS a, SUM(CASE WHEN x = 2 THEN 1 ELSE 0 END) AS b FROM t", [2,1], ["a", "b"]).await;
}

async fn int_rows(sql: &str) -> Vec<Vec<Option<i64>>> {
    let result = context().sql(sql).await.unwrap_or_else(|e| {
        panic!("Query error before independent value comparison: {e:?}; {sql}")
    });
    let mut rows = Vec::new();
    for batch in &result.batches {
        for column in batch.columns() {
            assert_eq!(column.data_type(), &DataType::Int64);
        }
        for row in 0..batch.num_rows() {
            rows.push(
                batch
                    .columns()
                    .iter()
                    .map(|column| {
                        let a = column.as_any().downcast_ref::<Int64Array>().unwrap();
                        if a.is_null(row) {
                            None
                        } else {
                            Some(a.value(row))
                        }
                    })
                    .collect(),
            );
        }
    }
    rows
}

#[tokio::test]
async fn grouping_sets_keep_colliding_aggregate_outputs_separate() {
    let mut rows =
        int_rows("SELECT x, SUM(x), SUM(DISTINCT x) FROM t GROUP BY GROUPING SETS ((x), ())").await;
    rows.sort();
    let mut expected = vec![
        vec![Some(1), Some(2), Some(1)],
        vec![Some(2), Some(2), Some(2)],
        vec![None, None, None],
        vec![None, Some(4), Some(3)],
    ];
    expected.sort();
    assert_eq!(rows, expected);
}

#[tokio::test]
async fn grouped_case_collisions_keep_hidden_having_and_order_by_aggregates() {
    let rows = int_rows("SELECT x, SUM(CASE WHEN x = 1 THEN 1 ELSE 0 END), SUM(CASE WHEN x = 2 THEN 1 ELSE 0 END) FROM t GROUP BY x HAVING SUM(x) > 0 ORDER BY SUM(DISTINCT x)").await;
    assert_eq!(
        rows,
        vec![
            vec![Some(1), Some(2), Some(0)],
            vec![Some(2), Some(0), Some(1)]
        ]
    );
}

#[tokio::test]
async fn duplicate_display_names_survive_hidden_sort_and_limit() {
    let rows =
        int_rows("SELECT x AS v, x + 10 AS v FROM t WHERE x IS NOT NULL ORDER BY x DESC LIMIT 2")
            .await;
    assert_eq!(rows, vec![vec![Some(2), Some(12)], vec![Some(1), Some(11)]]);
}
