//! Regression for post-filter batching, independently of SQL result correctness.
use super::*;
use crate::planner::{BinaryOp, ScalarValue};
use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
};
use futures::TryStreamExt;
use parquet::{basic::Compression, file::properties::WriterProperties};

type Row = (Option<i64>, Option<String>);

fn rows(batch: &RecordBatch, strings: bool) -> Vec<Row> {
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let text = strings.then(|| {
        batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
    });
    (0..batch.num_rows())
        .map(|i| {
            (
                values.is_valid(i).then(|| values.value(i)),
                text.and_then(|a| a.is_valid(i).then(|| a.value(i).to_owned())),
            )
        })
        .collect()
}

async fn filtered_quantum(strings: bool, every: Option<usize>) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("filtered.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("keep", DataType::Int64, true),
        Field::new("value", DataType::Int64, true),
        Field::new("text", DataType::Utf8, true),
    ]));
    let group_rows = 2049;
    let count = group_rows * 2;
    let keep = (0..count)
        .map(|i| (i % 7 != 0).then_some(i64::from(every.is_some_and(|n| i % n == 0))))
        .collect::<Vec<_>>();
    let values = (0..count)
        .map(|i| (i % 11 != 0).then_some((i % 37) as i64))
        .collect::<Vec<_>>();
    let text = (0..count)
        .map(|i| (i % 13 != 0).then(|| format!("value-{}", i % 5)))
        .collect::<Vec<_>>();
    let mut expected = (0..count)
        .filter(|&i| keep[i] == Some(1))
        .map(|i| (values[i], strings.then(|| text[i].clone()).flatten()))
        .collect::<Vec<_>>();
    expected.sort();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(keep)),
            Arc::new(Int64Array::from(values)),
            Arc::new(StringArray::from(text)),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(group_rows))
        .set_write_batch_size(64)
        .set_data_page_row_count_limit(64)
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        File::create(&path).unwrap(),
        schema.clone(),
        Some(props),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let predicate = Expr::BinaryExpr {
        left: Box::new(Expr::column("keep")),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Literal(ScalarValue::Int64(0))),
    };
    let pool = crate::execution::create_memory_pool(64 << 20);
    let requested = 32;
    let scan = StreamingParquetScanExec::try_new_with_batch_size(
        "filtered",
        &[path],
        schema.clone(),
        Some(if strings { vec![1, 2] } else { vec![1] }),
        Some(&predicate),
        &schema,
        requested,
        true,
    )
    .unwrap()
    .with_memory_pool(pool.clone());
    // The ordinary route is a useful batching control, but the value oracle above
    // is independent of both engine implementations.
    let mut ordinary = Vec::new();
    let mut ordinary_sizes = Vec::new();
    for partition in 0..scan.output_partitions() {
        let mut stream = scan.execute(partition).await.unwrap();
        while let Some(batch) = stream.try_next().await.unwrap() {
            ordinary_sizes.push(batch.num_rows());
            ordinary.extend(rows(&batch, strings));
        }
    }
    ordinary.sort();
    assert_eq!(ordinary, expected);
    let prepared = scan
        .prepare_admitted_queue_input(pool.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prepared.streams.as_slice().len(), scan.output_partitions());
    let (streams, stream_owner) = prepared.streams.into_parts();
    let mut actual = Vec::new();
    let mut sizes = Vec::new();
    let mut held = Vec::new();
    for mut stream in streams {
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert!(batch.num_rows() > 0 && batch.num_rows() <= requested);
            actual.extend(rows(&batch, strings));
            sizes.push(batch.num_rows());
            held.push(batch);
        }
    }
    actual.sort();
    assert_eq!(
        actual, expected,
        "NULLs, duplicates, non-output predicates and all partitions"
    );
    drop(stream_owner);
    drop(scan);
    if !held.is_empty() {
        assert!(pool.used() > 0, "held buffers retain admission");
    }
    drop(held);
    assert_eq!(pool.used(), 0);
    if !expected.is_empty() {
        assert!(
            ordinary_sizes.contains(&requested),
            "control must fill selected-row target"
        );
        assert!(
            sizes.contains(&requested),
            "post-filter batching gap: ordinary={ordinary_sizes:?}, admitted={sizes:?}"
        );
    }
}

#[tokio::test]
async fn fixed_output_fills_post_filter_quantum() {
    for every in [None, Some(1), Some(17)] {
        filtered_quantum(false, every).await;
    }
}

#[tokio::test]
async fn string_output_fills_post_filter_quantum() {
    for every in [None, Some(1), Some(17)] {
        filtered_quantum(true, every).await;
    }
}
