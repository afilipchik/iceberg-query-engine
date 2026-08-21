//! Hermetic window-function semantics tests: tiny fixtures, hand-computed
//! answers, no external oracle. The broad DuckDB-compared battery lives in
//! `scripts/window_validate.py` (63 cases); these pin the tricky standard
//! semantics so a regression fails in `cargo test` with a readable diff.

use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

use query_engine::ExecutionContext;

/// g | v | s : two groups of 3 and 2 rows, one NULL v.
fn ctx() -> ExecutionContext {
    let schema = Arc::new(Schema::new(vec![
        Field::new("g", DataType::Utf8, false),
        Field::new("v", DataType::Float64, true),
        Field::new("s", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a", "a", "a", "b", "b"])),
            Arc::new(Float64Array::from(vec![
                Some(10.0),
                Some(20.0),
                None,
                Some(5.0),
                Some(5.0),
            ])),
            Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
        ],
    )
    .unwrap();
    let mut ctx = ExecutionContext::new();
    ctx.register_table("t", schema, vec![batch]);
    ctx
}

async fn col_i64(sql: &str, col: &str) -> Vec<Option<i64>> {
    let r = ctx().sql(sql).await.expect(sql);
    let idx = r
        .schema
        .fields()
        .iter()
        .position(|f| f.name() == col)
        .unwrap_or_else(|| panic!("no column {col} in {sql}"));
    let mut out = Vec::new();
    for b in &r.batches {
        let a = b.column(idx);
        let a = arrow::compute::cast(a, &DataType::Int64).expect("castable");
        let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..a.len() {
            out.push(a.is_valid(i).then(|| a.value(i)));
        }
    }
    out
}

async fn col_f64(sql: &str, col: &str) -> Vec<Option<f64>> {
    let r = ctx().sql(sql).await.expect(sql);
    let idx = r
        .schema
        .fields()
        .iter()
        .position(|f| f.name() == col)
        .unwrap();
    let mut out = Vec::new();
    for b in &r.batches {
        let a = arrow::compute::cast(b.column(idx), &DataType::Float64).unwrap();
        let a = a.as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..a.len() {
            out.push(a.is_valid(i).then(|| a.value(i)));
        }
    }
    out
}

#[tokio::test]
async fn row_number_and_rank_families_respect_partitions_and_ties() {
    // s orders rows deterministically; b-group v values tie.
    let rn = col_i64(
        "SELECT ROW_NUMBER() OVER (PARTITION BY g ORDER BY s) AS r FROM t ORDER BY s",
        "r",
    )
    .await;
    assert_eq!(rn, [1, 2, 3, 1, 2].map(Some).to_vec());

    let rank = col_i64(
        "SELECT RANK() OVER (PARTITION BY g ORDER BY v) AS r, s FROM t ORDER BY s",
        "r",
    )
    .await;
    // ASC default is NULLS LAST (Postgres-style): 10.0=1, 20.0=2, NULL(s=3)=3;
    // group b tie: 1,1.
    assert_eq!(rank, [1, 2, 3, 1, 1].map(Some).to_vec());

    let dense = col_i64(
        "SELECT DENSE_RANK() OVER (ORDER BY v NULLS LAST) AS r, s FROM t ORDER BY s",
        "r",
    )
    .await;
    // 5,5,10,20,NULL -> dense ranks 1,1,2,3,4 by (v): s1=10->2 s2=20->3 s3=NULL->4 s4=5->1 s5=5->1
    assert_eq!(dense, [2, 3, 4, 1, 1].map(Some).to_vec());
}

#[tokio::test]
async fn ntile_distributes_the_remainder_to_leading_buckets() {
    let n = col_i64(
        "SELECT NTILE(2) OVER (ORDER BY s) AS r, s FROM t ORDER BY s",
        "r",
    )
    .await;
    // 5 rows, 2 buckets: sizes 3+2.
    assert_eq!(n, [1, 1, 1, 2, 2].map(Some).to_vec());

    let n = col_i64(
        "SELECT NTILE(7) OVER (ORDER BY s) AS r, s FROM t ORDER BY s",
        "r",
    )
    .await;
    // more buckets than rows: each row its own bucket.
    assert_eq!(n, [1, 2, 3, 4, 5].map(Some).to_vec());
}

#[tokio::test]
async fn lag_lead_defaults_apply_only_outside_the_partition() {
    let lag = col_f64(
        "SELECT LAG(v, 1, -1.0) OVER (PARTITION BY g ORDER BY s) AS r, s FROM t ORDER BY s",
        "r",
    )
    .await;
    // partition edges get the default; the NULL v at s=3 lags in as real NULL.
    assert_eq!(
        lag,
        vec![Some(-1.0), Some(10.0), Some(20.0), Some(-1.0), Some(5.0)]
    );

    let lead = col_f64(
        "SELECT LEAD(v) OVER (PARTITION BY g ORDER BY s) AS r, s FROM t ORDER BY s",
        "r",
    )
    .await;
    assert_eq!(lead, vec![Some(20.0), None, None, Some(5.0), None]);
}

#[tokio::test]
async fn default_frame_makes_last_value_the_running_current_peer() {
    // The standard default frame ends at CURRENT ROW('s peers) — the classic
    // LAST_VALUE surprise. With an explicit whole-partition frame it is the
    // true last.
    let dflt = col_i64(
        "SELECT LAST_VALUE(s) OVER (ORDER BY s) AS r, s FROM t ORDER BY s",
        "r",
    )
    .await;
    assert_eq!(dflt, [1, 2, 3, 4, 5].map(Some).to_vec());

    let whole = col_i64(
        "SELECT LAST_VALUE(s) OVER (ORDER BY s ROWS BETWEEN UNBOUNDED PRECEDING AND \
         UNBOUNDED FOLLOWING) AS r, s FROM t ORDER BY s",
        "r",
    )
    .await;
    assert_eq!(whole, [5, 5, 5, 5, 5].map(Some).to_vec());
}

#[tokio::test]
async fn range_current_row_extends_over_peers_rows_does_not() {
    // b-group v ties (5.0, 5.0): RANGE CURRENT ROW includes both peers.
    let range_sum = col_f64(
        "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v RANGE BETWEEN UNBOUNDED PRECEDING \
         AND CURRENT ROW) AS r, s FROM t ORDER BY s",
        "r",
    )
    .await;
    assert_eq!(range_sum[3], Some(10.0), "peer rows both included");
    assert_eq!(range_sum[4], Some(10.0));

    let rows_sum = col_f64(
        "SELECT SUM(v) OVER (PARTITION BY g ORDER BY v, s ROWS BETWEEN UNBOUNDED PRECEDING \
         AND CURRENT ROW) AS r, s FROM t ORDER BY s",
        "r",
    )
    .await;
    assert_eq!(rows_sum[3], Some(5.0), "ROWS stops at the current row");
    assert_eq!(rows_sum[4], Some(10.0));
}

#[tokio::test]
async fn empty_frames_yield_null_sum_and_zero_count() {
    let sum = col_f64(
        "SELECT SUM(v) OVER (ORDER BY s ROWS BETWEEN 3 FOLLOWING AND 4 FOLLOWING) AS r, s \
         FROM t ORDER BY s",
        "r",
    )
    .await;
    assert_eq!(sum[4], None, "frame past the partition end is empty");

    let cnt = col_i64(
        "SELECT COUNT(v) OVER (ORDER BY s ROWS BETWEEN 3 FOLLOWING AND 4 FOLLOWING) AS r, s \
         FROM t ORDER BY s",
        "r",
    )
    .await;
    assert_eq!(cnt[4], Some(0));
    assert_eq!(cnt[0], Some(2), "frame {{s=4,s=5}}: v=5,5 both counted");

    // COUNT skips the NULL v (s=3) inside a non-empty frame.
    let cnt = col_i64(
        "SELECT COUNT(v) OVER (ORDER BY s ROWS BETWEEN 1 FOLLOWING AND 2 FOLLOWING) AS r, s \
         FROM t ORDER BY s",
        "r",
    )
    .await;
    assert_eq!(cnt[0], Some(1), "frame {{s=2,s=3}} has one NULL v");
}

#[tokio::test]
async fn windows_compose_inside_expressions_and_named_windows() {
    let d = col_f64(
        "SELECT v - LAG(v, 1, 0.0) OVER w AS r, s FROM t WINDOW w AS (ORDER BY s) ORDER BY s",
        "r",
    )
    .await;
    assert_eq!(d[0], Some(10.0));
    assert_eq!(d[1], Some(10.0));
    assert_eq!(d[3], None, "NULL v propagates through arithmetic");
}

#[tokio::test]
async fn unsupported_shapes_are_refused_by_name() {
    let cases = [
        (
            "SELECT SUM(v) OVER (ORDER BY g RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) FROM t",
            "RANGE",
        ),
        ("SELECT NTILE(0) OVER (ORDER BY s) FROM t", "NTILE"),
        ("SELECT LAG(v, -1) OVER (ORDER BY s) FROM t", "offset"),
        ("SELECT STDDEV(v) OVER () FROM t", "STDDEV"),
    ];
    for (sql, needle) in cases {
        let err = ctx().sql(sql).await.expect_err(sql).to_string();
        assert!(
            err.to_uppercase().contains(&needle.to_uppercase()),
            "`{sql}` must be refused naming {needle}; said: {err}"
        );
    }
    // Windows outside the SELECT list are refused.
    let err = ctx()
        .sql("SELECT s FROM t WHERE ROW_NUMBER() OVER (ORDER BY s) = 1")
        .await
        .expect_err("window in WHERE")
        .to_string();
    assert!(err.contains("SELECT list"), "said: {err}");
}

#[tokio::test]
async fn grouping_sets_pad_with_nulls_and_grouping_marks_them() {
    let r = ctx()
        .sql("SELECT g, GROUPING(g) AS gg, COUNT(*) AS n FROM t GROUP BY ROLLUP (g)")
        .await
        .expect("rollup");
    // rows: (a,0,3), (b,0,2), (NULL,1,5)
    assert_eq!(r.row_count, 3);
    // The UNION emits one batch per branch — scan them all.
    let null_rows: usize = r
        .batches
        .iter()
        .map(|b| {
            let g = b.column(0);
            (0..g.len()).filter(|i| g.is_null(*i)).count()
        })
        .sum();
    assert_eq!(null_rows, 1, "one grand-total row");
}
