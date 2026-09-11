use arrow::array::{Array, Date32Array, Int64Array, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

fn context() -> ExecutionContext {
    let input = RecordBatch::try_from_iter(vec![(
        "id",
        Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])) as Arc<dyn Array>,
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

#[tokio::test]
async fn date_and_timestamp_literals_preserve_declared_types_and_exact_values() {
    let ctx = context();
    let result = ctx
        .sql("SELECT DATE '1969-12-31' AS d, TIMESTAMP '1969-12-31 23:59:59.123456' AS ts FROM t")
        .await
        .unwrap();
    assert_eq!(result.row_count, 5);
    for batch in result.batches {
        assert_eq!(batch.column(0).data_type(), &DataType::Date32);
        assert_eq!(
            batch.column(1).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        let dates = batch
            .column(0)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let times = batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        assert!(dates.iter().all(|v| v == Some(-1)));
        assert!(times.iter().all(|v| v == Some(-876544)));
    }
}

#[tokio::test]
async fn typed_literals_and_explicit_casts_share_comparison_and_empty_output_contracts() {
    let ctx = context();
    for expression in [
        "TIMESTAMP '1969-12-31 23:59:59.123456'",
        "CAST('1969-12-31 23:59:59.123456' AS TIMESTAMP)",
    ] {
        let result = ctx
            .sql(&format!(
                "SELECT id FROM t WHERE {expression} < TIMESTAMP '1970-01-01 00:00:00' ORDER BY id"
            ))
            .await
            .unwrap();
        assert_eq!(result.row_count, 5);
        let result = ctx
            .sql(&format!("SELECT {expression} AS v FROM t WHERE id < 0"))
            .await
            .unwrap();
        assert_eq!(result.row_count, 0);
        assert_eq!(
            result.schema.field(0).data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }
}

#[tokio::test]
async fn invalid_typed_literals_error_instead_of_becoming_strings() {
    let ctx = context();
    for literal in ["DATE 'not-a-date'", "TIMESTAMP 'not-a-timestamp'"] {
        assert!(
            ctx.sql(&format!("SELECT {literal} FROM t")).await.is_err(),
            "{literal}"
        );
    }
}

#[tokio::test]
async fn decimal_metadata_is_checked_before_narrowing_even_for_try_cast_and_empty_input() {
    let ctx = context();
    for target in [
        "DECIMAL(294,256)",
        "DECIMAL(38,256)",
        "DECIMAL(38,-256)",
        "DECIMAL(38,-129)",
        "DECIMAL(0)",
        "NUMERIC(39,0)",
        "DECIMAL(2,3)",
    ] {
        for cast in ["CAST", "TRY_CAST"] {
            for filter in ["", " WHERE id < 0"] {
                let error = ctx
                    .sql(&format!("SELECT {cast}(1 AS {target}) FROM t{filter}"))
                    .await
                    .unwrap_err()
                    .to_string();
                assert!(
                    error.contains("decimal precision/scale"),
                    "{target}: {error}"
                );
            }
        }
    }
    let result=ctx.sql("SELECT CAST('0.1' AS DECIMAL(1,1)), CAST('12345678901234567890123456789012345678' AS DECIMAL(38,0)) FROM t").await.unwrap();
    assert_eq!(result.row_count, 5);
    assert_eq!(
        result.schema.field(0).data_type(),
        &DataType::Decimal128(1, 1)
    );
    assert_eq!(
        result.schema.field(1).data_type(),
        &DataType::Decimal128(38, 0)
    );
}

#[tokio::test]
async fn unsigned_type_spellings_bind_to_the_existing_exact_numeric_domains() {
    let ctx = context();
    let result=ctx.sql("SELECT CAST(255 AS UTINYINT), CAST(65535 AS USMALLINT), CAST(4294967295 AS UINTEGER), CAST(18446744073709551615 AS UBIGINT) FROM t").await.unwrap();
    assert_eq!(result.row_count, 5);
    for batch in result.batches {
        let expected = [
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
        ];
        for (i, t) in expected.iter().enumerate() {
            assert_eq!(batch.column(i).data_type(), t);
        }
        let values = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::UInt64Array>()
            .unwrap();
        assert!(values.iter().all(|v| v == Some(u64::MAX)));
    }
    for spelling in [
        "UTINYINT",
        "UInt8",
        "TINYINT UNSIGNED",
        "USMALLINT",
        "UInt16",
        "SMALLINT UNSIGNED",
        "UINTEGER",
        "UInt32",
        "INTEGER UNSIGNED",
        "UBIGINT",
        "UInt64",
        "BIGINT UNSIGNED",
    ] {
        assert!(
            ctx.sql(&format!("SELECT CAST(-1 AS {spelling}) FROM t"))
                .await
                .is_err(),
            "{spelling}"
        );
        let result = ctx
            .sql(&format!("SELECT TRY_CAST(-1 AS {spelling}) FROM t"))
            .await
            .unwrap();
        assert_eq!(
            result.batches[0].column(0).null_count(),
            result.batches[0].num_rows()
        );
    }
}

#[tokio::test]
async fn supported_negative_decimal_scale_remains_exact() {
    let ctx = context();
    let result = ctx
        .sql("SELECT CAST(12345 AS DECIMAL(6,-2)) FROM t")
        .await
        .unwrap();
    assert_eq!(
        result.schema.field(0).data_type(),
        &DataType::Decimal128(6, -2)
    );
    for batch in result.batches {
        let values = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Decimal128Array>()
            .unwrap();
        assert!(values.iter().all(|v| v == Some(123)));
    }
}
