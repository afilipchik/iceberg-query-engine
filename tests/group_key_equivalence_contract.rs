use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

async fn scalar_count(ctx: &ExecutionContext, sql: &str) -> i64 {
    let result = ctx.sql(sql).await.unwrap();
    assert_eq!(result.row_count, 1);
    result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn group_equivalence_is_null_safe_and_matches_float_distinct() {
    for rows in [10usize, 65536] {
        let values = [
            Some(-0.0),
            Some(0.0),
            Some(f64::NAN),
            Some(f64::from_bits(0xfff8000000000001)),
            None,
        ];
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from_iter_values(0..rows as i64)) as ArrayRef,
            ),
            (
                "a",
                Arc::new(Float64Array::from(
                    (0..rows).map(|i| values[i % 5]).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
        ])
        .unwrap();
        for partitions in [1, 4] {
            let mut ctx = ExecutionContext::new().with_parallel_partitions(partitions);
            let batches = (0..rows)
                .step_by(4096)
                .map(|i| batch.slice(i, (rows - i).min(4096)))
                .collect();
            ctx.register_table("t", batch.schema(), batches);
            assert_eq!(
                scalar_count(&ctx, "SELECT COUNT(*) FROM (SELECT a FROM t GROUP BY a) g").await,
                3
            );
            assert_eq!(
                scalar_count(
                    &ctx,
                    "SELECT COUNT(*) FROM (SELECT a, COUNT(*) AS n FROM t GROUP BY a) g"
                )
                .await,
                3
            );
            assert_eq!(
                scalar_count(&ctx, "SELECT COUNT(DISTINCT a) FROM t").await,
                2
            );
            let non_null = (0..rows).filter(|i| i % 5 != 4).count() as i64;
            assert_eq!(
                scalar_count(
                    &ctx,
                    "SELECT COUNT(*) FROM t x JOIN t y ON x.id=y.id AND x.a<=y.a"
                )
                .await,
                non_null
            );
            assert_eq!(
                scalar_count(&ctx, "SELECT COUNT(*) FROM t WHERE a=a").await,
                non_null
            );
        }
    }
}

#[tokio::test]
async fn nullable_integer_and_string_group_keys_have_one_null_group() {
    for key in [
        Arc::new(Int64Array::from(vec![
            None,
            Some(1),
            None,
            Some(1),
            Some(2),
            None,
        ])) as ArrayRef,
        Arc::new(StringArray::from(vec![
            None,
            Some("a"),
            None,
            Some("a"),
            Some("b"),
            None,
        ])) as ArrayRef,
    ] {
        let batch = RecordBatch::try_from_iter(vec![("k", key)]).unwrap();
        let mut ctx = ExecutionContext::new().with_parallel_partitions(3);
        ctx.register_table(
            "t",
            batch.schema(),
            vec![batch.slice(0, 2), batch.slice(2, 4)],
        );
        assert_eq!(
            scalar_count(&ctx, "SELECT COUNT(*) FROM (SELECT k FROM t GROUP BY k) g").await,
            3
        );
        assert_eq!(
            scalar_count(&ctx, "SELECT COUNT(*) FROM t x JOIN t y ON x.k=y.k").await,
            5
        );
    }
}

#[test]
fn vector_keys_share_sql_hashes_but_keep_join_and_group_null_policies() {
    use query_engine::physical::operators::vectorized_hash::{
        compare_group_row, compare_row, hash_arrays,
    };
    let a = Arc::new(Float64Array::from(vec![
        Some(-0.0),
        Some(0.0),
        Some(f64::NAN),
        Some(f64::from_bits(0xfff8000000000001)),
        None,
    ])) as ArrayRef;
    let keys = vec![a];
    let hashes = hash_arrays(&keys, 5);
    assert_eq!(hashes[0], hashes[1]);
    assert_eq!(hashes[2], hashes[3]);
    assert!(compare_row(&keys, 0, &keys, 1));
    assert!(compare_row(&keys, 2, &keys, 3));
    assert!(!compare_row(&keys, 4, &keys, 4));
    assert!(compare_group_row(&keys, 4, &keys, 4));
    assert!(!compare_group_row(&keys, 0, &[], 0));
}
