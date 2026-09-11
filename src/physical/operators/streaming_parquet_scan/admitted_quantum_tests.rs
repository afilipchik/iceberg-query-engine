use super::*;
use arrow::datatypes::{DataType, Field, Schema};
use futures::TryStreamExt;
use parquet::{basic::Compression, file::properties::WriterProperties};

async fn prepared_quantum(requested: usize) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("quantum.parquet");
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        true,
    )]));
    let group_rows = 32775;
    let expected = (0..group_rows * 2)
        .map(|i| (i % 11 != 0).then_some((i % 37) as i64))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(expected.clone()))],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .set_max_row_group_row_count(Some(group_rows))
        .set_data_page_row_count_limit(group_rows)
        .set_data_page_size_limit(1 << 20)
        .build();
    let mut writer = parquet::arrow::ArrowWriter::try_new(
        File::create(&path).unwrap(),
        schema.clone(),
        Some(props),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let pool = crate::execution::create_memory_pool(64 << 20);
    let scan = StreamingParquetScanExec::try_new_with_batch_size(
        "quantum",
        &[path],
        schema.clone(),
        None,
        None,
        &schema,
        requested,
        true,
    )
    .unwrap()
    .with_memory_pool(pool.clone());
    let prepared = scan
        .prepare_admitted_queue_input(pool.clone())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(prepared.streams.as_slice().len(), scan.output_partitions());
    let mut actual = Vec::new();
    let mut sizes = Vec::new();
    let mut held = Vec::new();
    let (streams, stream_owner) = prepared.streams.into_parts();
    for mut stream in streams {
        while let Some(batch) = stream.try_next().await.unwrap() {
            assert!(
                batch.num_rows() <= requested,
                "admitted input exceeded planned row quantum"
            );
            sizes.push(batch.num_rows());
            actual.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter(),
            );
            held.push(batch);
        }
    }
    assert!(
        sizes.contains(&requested),
        "sufficiently large admitted pages must support the planned quantum"
    );
    actual.sort();
    let mut expected = expected;
    expected.sort();
    assert_eq!(
        actual, expected,
        "all partitions, duplicates, NULLs and tail rows"
    );
    drop(stream_owner);
    drop(scan);
    assert!(pool.used() > 0, "held output must retain admission");
    drop(held);
    assert_eq!(pool.used(), 0);
}

#[tokio::test]
async fn prepared_scan_honors_small_planned_quantum() {
    prepared_quantum(17).await;
}

#[tokio::test]
async fn prepared_scan_honors_large_planned_quantum() {
    prepared_quantum(32768).await;
}
