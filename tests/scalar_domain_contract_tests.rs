use arrow::array::{
    Array, ArrayRef, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray,
};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

#[tokio::test]
async fn length_preserves_dictionary_nulls_and_string_encodings() {
    use arrow::array::{DictionaryArray, LargeStringArray, StringViewArray};
    use arrow::datatypes::Int32Type;
    let dictionary = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![Some(0), Some(1), None]),
        Arc::new(StringArray::from(vec![Some("é🙂"), None])),
    )
    .unwrap();
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(dictionary),
        Arc::new(LargeStringArray::from(vec![Some("é🙂"), None, None])),
        Arc::new(StringViewArray::from(vec![Some("é🙂"), None, None])),
    ];
    for values in arrays {
        let batch = RecordBatch::try_from_iter(vec![("s", values)]).unwrap();
        let mut ctx = ExecutionContext::new();
        ctx.register_table("strings", batch.schema(), vec![batch]);
        let result = ctx
            .sql("SELECT LENGTH(s), STRLEN(s) FROM strings")
            .await
            .unwrap();
        for (column, first) in [(0, 2), (1, 6)] {
            let actual: Vec<_> = result
                .batches
                .iter()
                .flat_map(|b| {
                    b.column(column)
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                })
                .collect();
            assert_eq!(actual, vec![Some(first), None, None]);
        }
    }
}

#[tokio::test]
async fn character_and_byte_length_are_distinct_and_preserve_nulls() {
    let values = StringArray::from(vec![Some("é🙂"), Some("e\u{301}"), Some(""), None]);
    let batch = RecordBatch::try_from_iter(vec![("s", Arc::new(values) as ArrayRef)]).unwrap();
    let mut ctx = ExecutionContext::new();
    ctx.register_table("strings", batch.schema(), vec![batch]);
    let result = ctx
        .sql("SELECT LENGTH(s), CHAR_LENGTH(s), STRLEN(s), OCTET_LENGTH(s) FROM strings")
        .await
        .unwrap();
    for column in 0..4 {
        let actual: Vec<_> = result
            .batches
            .iter()
            .flat_map(|b| {
                b.column(column)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter()
            })
            .collect();
        let expected = if column < 2 {
            vec![Some(2), Some(2), Some(0), None]
        } else {
            vec![Some(6), Some(3), Some(0), None]
        };
        assert_eq!(actual, expected);
    }
    assert!(ctx.sql("SELECT LENGTH(123)").await.is_err());
}

#[tokio::test]
async fn extract_timestamp_parts_preserves_units_timezone_and_nulls() {
    for timezone in [None, Some("+02:00")] {
        let values = TimestampMicrosecondArray::from(vec![Some(-1), Some(3_661_123_456), None])
            .with_timezone_opt(timezone);
        let batch = RecordBatch::try_from_iter(vec![("ts", Arc::new(values) as ArrayRef)]).unwrap();
        let mut ctx = ExecutionContext::new();
        ctx.register_table("times", batch.schema(), vec![batch]);
        let result = ctx.sql("SELECT EXTRACT(HOUR FROM ts), EXTRACT(MINUTE FROM ts), EXTRACT(SECOND FROM ts) FROM times").await.unwrap();
        let expected = [
            if timezone.is_some() {
                vec![Some(1), Some(3), None]
            } else {
                vec![Some(23), Some(1), None]
            },
            vec![Some(59), Some(1), None],
            vec![Some(59), Some(1), None],
        ];
        for (column, expected) in expected.into_iter().enumerate() {
            let actual: Vec<_> = result
                .batches
                .iter()
                .flat_map(|b| {
                    b.column(column)
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .unwrap()
                        .iter()
                })
                .collect();
            assert_eq!(actual, expected);
        }
        // Unsupported fields must not fabricate zero, including on a date input.
        assert!(ctx
            .sql("SELECT EXTRACT(CENTURY FROM DATE '2001-01-01')")
            .await
            .is_err());
    }
}

#[tokio::test]
async fn extract_invalid_temporal_values_refuses_instead_of_inventing_null() {
    let values = TimestampMicrosecondArray::from(vec![Some(i64::MAX)]);
    let batch = RecordBatch::try_from_iter(vec![("ts", Arc::new(values) as ArrayRef)]).unwrap();
    let mut ctx = ExecutionContext::new();
    ctx.register_table("times", batch.schema(), vec![batch]);
    assert!(ctx
        .sql("SELECT EXTRACT(YEAR FROM ts) FROM times")
        .await
        .is_err());
}
