//! Dictionary encodings must not change logical grouped results.
use arrow::array::{
    Array, ArrayRef, DictionaryArray, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::sync::Arc;

#[tokio::test]
async fn cross_codebook_logical_null_and_composite_groups_match_plain_values() {
    let make = |values: Vec<Option<&str>>,
                keys: Vec<Option<i32>>,
                group: Vec<i64>,
                amounts: Vec<Option<f64>>| {
        let dict = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(keys),
            Arc::new(StringArray::from(values)),
        )
        .unwrap();
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("g", dict.data_type().clone(), true),
                Field::new("h", DataType::Int64, false),
                Field::new("v", DataType::Float64, true),
            ])),
            vec![
                Arc::new(dict),
                Arc::new(Int64Array::from(group)),
                Arc::new(Float64Array::from(amounts)),
            ],
        )
        .unwrap()
    };
    let batches = vec![
        make(
            vec![Some("a"), None, Some("")],
            vec![Some(0), Some(1), Some(2)],
            vec![1, 1, 2],
            vec![Some(1.0), Some(2.0), Some(3.0)],
        ),
        make(
            vec![Some(""), None, Some("a")],
            vec![Some(2), Some(1), Some(0), None],
            vec![1, 1, 2, 2],
            vec![Some(4.0), Some(5.0), Some(6.0), None],
        ),
    ];
    for workers in [1, 4] {
        for encoded in [true, false] {
            let selected: Vec<RecordBatch> = if encoded {
                batches.clone()
            } else {
                batches
                    .iter()
                    .map(|batch| {
                        let columns: Vec<ArrayRef> = vec![
                            arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap(),
                            batch.column(1).clone(),
                            batch.column(2).clone(),
                        ];
                        let schema = Arc::new(Schema::new(vec![
                            Field::new("g", DataType::Utf8, true),
                            Field::new("h", DataType::Int64, false),
                            Field::new("v", DataType::Float64, true),
                        ]));
                        RecordBatch::try_new(schema, columns).unwrap()
                    })
                    .collect()
            };
            let mut context = ExecutionContext::new().with_parallel_partitions(workers);
            context.register_table("t", selected[0].schema(), selected);
            let result = context
                .sql("SELECT g, h, SUM(v), COUNT(g) FROM t GROUP BY g, h ORDER BY g NULLS FIRST, h")
                .await
                .unwrap();
            let mut rows = vec![];
            for batch in result.batches {
                let g = arrow::compute::cast(batch.column(0), &DataType::Utf8).unwrap();
                let g = g.as_any().downcast_ref::<StringArray>().unwrap();
                let h = batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                let sum = batch
                    .column(2)
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap();
                let count = batch
                    .column(3)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                for row in 0..batch.num_rows() {
                    rows.push((
                        (!g.is_null(row)).then(|| g.value(row).to_string()),
                        h.value(row),
                        (!sum.is_null(row)).then(|| sum.value(row)),
                        count.value(row),
                    ));
                }
            }
            assert_eq!(
                rows,
                vec![
                    (None, 1, Some(7.0), 0),
                    (None, 2, None, 0),
                    (Some("".to_string()), 2, Some(9.0), 2),
                    (Some("a".to_string()), 1, Some(5.0), 2)
                ],
                "encoded={encoded}, workers={workers}"
            );
        }
    }
}
