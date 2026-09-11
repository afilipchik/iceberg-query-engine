use arrow::{
    array::*,
    datatypes::{DataType, TimeUnit},
    record_batch::RecordBatch,
};
use query_engine::{
    planner::{ScalarValue, TimestampValue},
    ExecutionContext,
};
use std::sync::Arc;

fn context() -> ExecutionContext {
    let input = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(Int64Array::from(vec![0, 1, 2, 3, 4])) as ArrayRef,
    )])
    .unwrap();
    let mut ctx = ExecutionContext::new().with_parallel_partitions(3);
    ctx.register_table(
        "t",
        input.schema(),
        vec![input.slice(0, 2), input.slice(2, 3)],
    );
    ctx
}

fn ticks(array: &ArrayRef) -> Vec<Option<i64>> {
    arrow::compute::cast(array.as_ref(), &DataType::Int64)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .iter()
        .collect()
}

#[tokio::test]
async fn declared_precision_survives_fold_expand_scalar_subquery_and_empty_output() {
    let ctx = context();
    for (p, unit, value) in [
        (0, TimeUnit::Second, -1),
        (1, TimeUnit::Millisecond, -877),
        (3, TimeUnit::Millisecond, -877),
        (4, TimeUnit::Microsecond, -876544),
        (6, TimeUnit::Microsecond, -876544),
        (7, TimeUnit::Nanosecond, -876543211),
        (9, TimeUnit::Nanosecond, -876543211),
    ] {
        let expr = format!("CAST('1969-12-31 23:59:59.123456789' AS TIMESTAMP({p}))");
        for e in [expr.clone(), format!("(SELECT {expr})")] {
            for predicate in ["id >= 0", "id < 0"] {
                let result = ctx
                    .sql(&format!("SELECT {e} AS v FROM t WHERE {predicate}"))
                    .await
                    .unwrap();
                assert_eq!(
                    result.schema.field(0).data_type(),
                    &DataType::Timestamp(unit.clone(), None),
                    "{e}/{predicate}"
                );
                assert_eq!(result.row_count, if predicate == "id >= 0" { 5 } else { 0 });
                for b in result.batches {
                    assert!(
                        ticks(b.column(0)).iter().all(|v| *v == Some(value)),
                        "{e}: {:?}",
                        ticks(b.column(0))
                    );
                }
            }
        }
    }
}

#[tokio::test]
async fn timezone_survives_typed_literal_cast_and_scalar_subquery() {
    let ctx = context();
    for expr in [
        "TIMESTAMP WITH TIME ZONE '1969-12-31 23:59:59.123456+02:00'",
        "CAST('1969-12-31 23:59:59.123456+02:00' AS TIMESTAMPTZ)",
    ] {
        for e in [expr.to_string(), format!("(SELECT {expr})")] {
            let result = ctx.sql(&format!("SELECT {e} AS v FROM t")).await.unwrap();
            assert_eq!(
                result.schema.field(0).data_type(),
                &DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
            );
            assert_eq!(result.row_count, 5);
            for b in result.batches {
                assert!(ticks(b.column(0)).iter().all(|v| *v == Some(-7200876544)));
            }
        }
    }
}

#[tokio::test]
async fn array_timestamp_metadata_survives_scalar_extraction_and_min_max() {
    for unit in [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ] {
        for zone in [
            None,
            Some(Arc::<str>::from("UTC")),
            Some(Arc::<str>::from("America/Los_Angeles")),
        ] {
            let dtype = DataType::Timestamp(unit.clone(), zone.clone());
            let counts = Int64Array::from(vec![Some(-123), None, Some(456), Some(-123)]);
            let array = arrow::compute::cast(&counts, &dtype).unwrap();
            let batch = RecordBatch::try_from_iter(vec![("v", array)]).unwrap();
            let mut ctx = ExecutionContext::new().with_parallel_partitions(3);
            ctx.register_table(
                "x",
                batch.schema(),
                vec![batch.slice(0, 2), batch.slice(2, 2)],
            );
            for sql in [
                "SELECT (SELECT v FROM x WHERE v IS NOT NULL ORDER BY v LIMIT 1) AS lo",
                "SELECT MIN(v) AS lo FROM x",
                "SELECT MIN(v) AS lo FROM x GROUP BY v IS NULL HAVING MIN(v) IS NOT NULL",
            ] {
                let result = ctx.sql(sql).await.unwrap();
                assert_eq!(result.schema.field(0).data_type(), &dtype, "{sql}");
                assert_eq!(result.row_count, 1, "{sql}");
                assert_eq!(
                    ticks(result.batches[0].column(0)),
                    vec![Some(-123)],
                    "{sql}"
                );
            }
        }
    }
}

#[tokio::test]
async fn timestamp_nulls_and_invalid_metadata_remain_typed_or_rejected() {
    let ctx = context();
    for sql in [
        "SELECT CAST(NULL AS TIMESTAMP(9)) AS v FROM t",
        "SELECT TRY_CAST('bad' AS TIMESTAMP(9)) AS v FROM t",
        "SELECT (SELECT CAST(NULL AS TIMESTAMP(9))) AS v FROM t",
    ] {
        let result = ctx.sql(sql).await.unwrap();
        assert_eq!(
            result.schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, None)
        );
        assert_eq!(result.row_count, 5);
        for batch in result.batches {
            assert_eq!(batch.column(0).null_count(), batch.num_rows());
        }
    }
    for dt in [
        "TIMESTAMP(10)",
        "TIMESTAMP(255)",
        "TIMESTAMP(3) WITH TIME ZONE",
    ] {
        for cast in ["CAST", "TRY_CAST"] {
            assert!(
                ctx.sql(&format!("SELECT {cast}(NULL AS {dt}) FROM t WHERE id < 0"))
                    .await
                    .is_err(),
                "{cast}/{dt}"
            );
        }
    }
}

#[test]
fn scalar_identity_includes_temporal_domain() {
    let a = ScalarValue::Timestamp(TimestampValue::new(1, TimeUnit::Second, None));
    let b = ScalarValue::Timestamp(TimestampValue::new(1, TimeUnit::Nanosecond, None));
    let c = ScalarValue::Timestamp(TimestampValue::new(1, TimeUnit::Second, Some("UTC".into())));
    assert_ne!(a, b);
    assert_ne!(a, c);
    assert_ne!(a.to_string(), b.to_string());
    assert_ne!(a.to_string(), c.to_string());
}

#[tokio::test]
async fn temporal_predicates_compare_instants_without_losing_submicrosecond_values() {
    let ctx = context();
    for predicate in [
        "CAST('1970-01-01 00:00:00.000000001' AS TIMESTAMP(9)) > CAST('1970-01-01 00:00:00' AS TIMESTAMP)",
        "CAST('1969-12-31 23:59:59.999999999' AS TIMESTAMP(9)) < CAST('1970-01-01 00:00:00' AS TIMESTAMP(3))",
        "CAST('1970-01-01 02:00:00+02:00' AS TIMESTAMPTZ) = CAST('1970-01-01 00:00:00+00:00' AS TIMESTAMPTZ)",
    ] {
        let result=ctx.sql(&format!("SELECT id FROM t WHERE {predicate}")).await.unwrap();
        assert_eq!(result.row_count,5,"{predicate}");
    }
}
