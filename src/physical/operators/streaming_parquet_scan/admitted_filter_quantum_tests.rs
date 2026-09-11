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
async fn fixed_empty_post_filter_quantum() {
    filtered_quantum(false, None).await;
}

#[tokio::test]
async fn fixed_dense_post_filter_quantum() {
    filtered_quantum(false, Some(1)).await;
}

#[tokio::test]
async fn fixed_sparse_post_filter_quantum() {
    filtered_quantum(false, Some(17)).await;
}

#[tokio::test]
async fn string_empty_post_filter_quantum() {
    filtered_quantum(true, None).await;
}

#[tokio::test]
async fn string_dense_post_filter_quantum() {
    filtered_quantum(true, Some(1)).await;
}

#[tokio::test]
async fn string_sparse_post_filter_quantum() {
    filtered_quantum(true, Some(17)).await;
}

fn write_filter_fixture(
    path: &std::path::Path,
    strings: Vec<String>,
    keep: Vec<Option<i64>>,
) -> SchemaRef {
    let schema = Arc::new(Schema::new(vec![
        Field::new("keep", DataType::Int64, true),
        Field::new("text", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(keep)),
            Arc::new(StringArray::from(strings)),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        File::create(path).unwrap(),
        schema.clone(),
        Some(props),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    schema
}
fn keep_predicate() -> Expr {
    Expr::BinaryExpr {
        left: Box::new(Expr::column("keep")),
        op: BinaryOp::Gt,
        right: Box::new(Expr::Literal(ScalarValue::Int64(0))),
    }
}

#[tokio::test]
async fn packing_byte_boundaries_and_oversized_values_preserve_order_and_progress() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("bytes.parquet");
    let values = vec![
        "skip".repeat(7500),
        "a".repeat(30000),
        "b".repeat(20000),
        "c".repeat(20000),
        "d".repeat(20000),
        "e".repeat(100000),
        "tail".to_string(),
    ];
    let expected = values[1..].to_vec();
    let schema = write_filter_fixture(
        &path,
        values,
        vec![None, Some(1), Some(1), Some(1), Some(1), Some(1), Some(1)],
    );
    let pool = crate::execution::create_memory_pool(4 << 20);
    let scan = StreamingParquetScanExec::try_new_with_batch_size(
        "bytes",
        &[path],
        schema.clone(),
        Some(vec![1]),
        Some(&keep_predicate()),
        &schema,
        32,
        true,
    )
    .unwrap()
    .with_memory_pool(pool.clone());
    let prepared = scan
        .prepare_admitted_queue_input(pool.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prepared.streams.as_slice().len(), 1);
    let (streams, owner) = prepared.streams.into_parts();
    let mut actual = Vec::new();
    let mut held = Vec::new();
    for mut stream in streams {
        while let Some(batch) = stream.try_next().await.unwrap() {
            let strings = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let bytes = strings.iter().map(|s| s.unwrap().len()).sum::<usize>();
            assert!(bytes <= VALUE_BYTES || batch.num_rows() == 1);
            actual.extend(strings.iter().map(|s| s.unwrap().to_owned()));
            held.push(batch);
        }
    }
    assert_eq!(actual, expected);
    drop(owner);
    drop(scan);
    assert!(pool.used() > 0);
    drop(held);
    assert_eq!(pool.used(), 0);
}

#[test]
fn packing_refusal_bypasses_only_a_whole_unconsumed_admitted_chunk() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("refusal.parquet");
    let schema = write_filter_fixture(
        &path,
        vec!["a".into(), "b".into(), "c".into()],
        vec![Some(1); 3],
    );
    for offset in [0, 1] {
        let pool = crate::execution::create_memory_pool(1 << 20);
        let scan = StreamingParquetScanExec::try_new_with_batch_size(
            "refusal",
            std::slice::from_ref(&path),
            schema.clone(),
            Some(vec![1]),
            Some(&keep_predicate()),
            &schema,
            32,
            true,
        )
        .unwrap()
        .with_memory_pool(pool.clone());
        let state = state(&scan, 0, pool.clone());
        let mut reader = Reader::open(&state.work[0], &state).unwrap();
        let batch = reader
            .next_chunk(&scan.schema(), &pool, 32)
            .unwrap()
            .unwrap();
        assert_eq!(batch.num_rows(), 3);
        reader.pending_output = Some((batch, offset));
        let blocker = pool.allocate(pool.available()).unwrap();
        let result = reader.next(&scan.schema(), &pool, 32);
        if offset == 0 {
            let output = result.unwrap().unwrap();
            assert_eq!(output.num_rows(), 3);
            assert_eq!(
                output
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap(),
                &StringArray::from(vec!["a", "b", "c"])
            );
            drop(output);
            assert!(reader.pending_output.is_none());
        } else {
            assert!(result.unwrap_err().is_memory_limit());
            assert_eq!(reader.pending_output.as_ref().unwrap().1, 1);
        }
        drop(blocker);
        drop(reader);
        drop(state);
        drop(scan);
        assert_eq!(pool.used(), 0);
    }
}

#[tokio::test]
async fn source_failure_after_buffered_prefix_remains_terminal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("terminal.parquet");
    let schema = Arc::new(Schema::new(vec![
        Field::new("keep", DataType::Int64, false),
        Field::new("text", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![0, 1, 1, 1])),
            Arc::new(StringArray::from(vec!["skip", "a", "b", "c"])),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .set_write_batch_size(2)
        .set_data_page_row_count_limit(2)
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        File::create(&path).unwrap(),
        schema.clone(),
        Some(props),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let pool = crate::execution::create_memory_pool(1 << 20);
    let scan = StreamingParquetScanExec::try_new_with_batch_size(
        "terminal",
        std::slice::from_ref(&path),
        schema.clone(),
        Some(vec![1]),
        Some(&keep_predicate()),
        &schema,
        2,
        true,
    )
    .unwrap()
    .with_memory_pool(pool.clone());
    let mut state = state(&scan, 0, pool.clone());
    let mut reader = Reader::open(&state.work[0], &state).unwrap();
    let prefix = reader
        .next_chunk(&scan.schema(), &pool, 2)
        .unwrap()
        .unwrap();
    assert_eq!(prefix.num_rows(), 1);
    reader.pending_output = Some((prefix, 0));
    state.reader = Some(reader);
    state.next_work = 1;
    // Change only this test-owned file after the first page was consumed. The
    // next physical page must fail, even though packing has a valid prefix.
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(0)
        .unwrap();
    let mut output = Box::pin(stream(state));
    assert!(output.try_next().await.is_err());
    assert!(output.try_next().await.unwrap().is_none());
    drop(output);
    drop(scan);
    assert_eq!(pool.used(), 0);
}
