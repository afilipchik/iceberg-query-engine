//! Same-budget reader progress: distinguish output ordering from page floors.
use super::*;
use arrow::{
    array::{Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use std::sync::Arc;

fn fixture(
    path: &std::path::Path,
    count: usize,
    columns: usize,
) -> (File, ArrowReaderMetadata, SchemaRef) {
    let schema = Arc::new(Schema::new(
        (0..columns)
            .map(|c| Field::new(format!("c{c}"), DataType::Int64, true))
            .collect::<Vec<_>>(),
    ));
    let arrays = (0..columns)
        .map(|c| {
            Arc::new(Int64Array::from(
                (0..count)
                    .map(|i| (i % 17 != 0).then_some((i % 101) as i64 + c as i64 * 1000))
                    .collect::<Vec<_>>(),
            )) as arrow::array::ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), arrays).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_compression(Compression::UNCOMPRESSED)
        .set_write_batch_size(count)
        .set_data_page_row_count_limit(count)
        .set_max_row_group_row_count(Some(count))
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(path).unwrap(), schema.clone(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let file = File::open(path).unwrap();
    let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
    (file, metadata, schema)
}

fn check(batch: &RecordBatch, start: usize) {
    for (c, a) in batch.columns().iter().enumerate() {
        let a = a.as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..a.len() {
            let row = start + i;
            assert_eq!(a.is_valid(i), row % 17 != 0);
            if a.is_valid(i) {
                assert_eq!(a.value(i), (row % 101) as i64 + c as i64 * 1000);
            }
        }
    }
}

#[test]
fn fresh_small_target_progresses_at_same_budget_as_large_target() {
    let dir = tempfile::tempdir().unwrap();
    let (file, metadata, schema) = fixture(&dir.path().join("working-space.parquet"), 4096, 3);
    let budget = 160 << 10;
    let small_pool = MemoryPool::new(budget);
    let mut small = open(&file, &metadata, 0, &[0, 1, 2], schema.clone(), &small_pool).unwrap();
    let mut count = 0;
    while let Some(batch) = small.next(1, 65536, &small_pool).unwrap() {
        assert_eq!(batch.num_rows(), 1);
        check(&batch, count);
        count += batch.num_rows();
    }
    assert_eq!(count, 4096);
    drop(small);
    assert_eq!(small_pool.used(), 0);
    let large_pool = MemoryPool::new(budget);
    let mut large = open(&file, &metadata, 0, &[0, 1, 2], schema, &large_pool).unwrap();
    let first = large.next(4096, 65536, &large_pool);
    if let Err(error) = &first {
        assert!(error.is_memory_limit(), "{error}");
        eprintln!(
            "large first output refused: used={} limit={} error={error}",
            large_pool.used(),
            large_pool.max()
        );
        let retry = large.next(1, 65536, &large_pool);
        eprintln!(
            "retained reader one-row retry: {:?}; used={}",
            retry
                .as_ref()
                .map(|v| v.as_ref().map(RecordBatch::num_rows)),
            large_pool.used()
        );
        drop(retry);
    }
    let failed = first.is_err();
    if !failed {
        let mut next = first.unwrap();
        let mut count = 0;
        let mut largest = 0;
        while let Some(batch) = next {
            check(&batch, count);
            count += batch.num_rows();
            largest = largest.max(batch.num_rows());
            drop(batch);
            next = large.next(4096, 65536, &large_pool).unwrap();
        }
        assert_eq!(count, 4096);
        assert!(largest > 1, "coordination must preserve useful batching");
    }
    drop(large);
    assert_eq!(large_pool.used(), 0);
    assert!(
        !failed,
        "same budget completes a fresh one-row reader but refuses a large-target first output"
    );
}

#[test]
fn genuinely_oversized_page_refuses_even_with_one_row_target() {
    let dir = tempfile::tempdir().unwrap();
    let (file, metadata, schema) = fixture(&dir.path().join("page-floor.parquet"), 8192, 1);
    let pool = MemoryPool::new(32 << 10);
    let mut reader = open(&file, &metadata, 0, &[0], schema, &pool).unwrap();
    let error = reader.next(1, 65536, &pool).unwrap_err();
    assert!(error.is_memory_limit(), "{error}");
    eprintln!("one-row page-floor refusal: {error}; used={}", pool.used());
    drop(reader);
    assert_eq!(pool.used(), 0);
}

use crate::storage::{
    admitted_batch::ColumnSource, admitted_flat_column::OutputCheckpoint,
    admitted_page_read::PageSource,
};
use std::sync::atomic::{AtomicUsize, Ordering};

struct CountedFile {
    file: File,
    reads: Arc<AtomicUsize>,
}
impl PageSource for CountedFile {
    fn len(&self) -> std::io::Result<u64> {
        PageSource::len(&self.file)
    }
    fn read_at(&self, target: &mut [u8], offset: u64) -> std::io::Result<usize> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        PageSource::read_at(&self.file, target, offset)
    }
}
struct TrialColumn {
    inner: AdmittedFlatColumn<CountedFile>,
    reads: Arc<AtomicUsize>,
    refuse_once: bool,
    refusals: Arc<AtomicUsize>,
    terminal: bool,
}
impl ColumnSource for TrialColumn {
    type Checkpoint = OutputCheckpoint;
    const COORDINATED: bool = true;
    fn prepare_output(&mut self, pool: &MemoryPool) -> Result<bool> {
        self.inner.prepare_output(pool)
    }
    fn checkpoint(&self) -> Self::Checkpoint {
        self.inner.checkpoint()
    }
    fn restore(&mut self, checkpoint: &Self::Checkpoint) {
        self.inner.restore(checkpoint);
    }
    fn next(
        &mut self,
        _: usize,
        _: usize,
        _: &MemoryPool,
    ) -> Result<Option<arrow::array::ArrayRef>> {
        panic!("coordinated reader must not call the source-pulling method")
    }
    fn next_prepared(
        &mut self,
        rows: usize,
        bytes: usize,
        pool: &MemoryPool,
    ) -> Result<Option<arrow::array::ArrayRef>> {
        let reads = self.reads.load(Ordering::SeqCst);
        let result = self.inner.next_prepared(rows, bytes, pool);
        assert_eq!(
            self.reads.load(Ordering::SeqCst),
            reads,
            "output trials must not read pages"
        );
        if self.refuse_once && result.as_ref().is_ok_and(|v| v.is_some()) {
            self.refuse_once = false;
            self.refusals.fetch_add(1, Ordering::SeqCst);
            // Fault after actual decoder advancement tests cursor rollback, not
            // merely refusal before any work. Only typed memory denial retries.
            return Err(if self.terminal {
                QueryError::Storage("test terminal output fault".into())
            } else {
                pool.allocate(pool.max() + 1).unwrap_err()
            });
        }
        result
    }
}

fn trial_matrix(dictionary: bool, terminal: bool) {
    use arrow::array::StringArray;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trials.parquet");
    let count = 257;
    let schema = Arc::new(Schema::new(vec![
        Field::new("n", DataType::Int64, true),
        Field::new("s", DataType::Utf8, true),
    ]));
    let numbers = (0..count)
        .map(|i| (i % 7 != 0).then_some((i % 13) as i64))
        .collect::<Vec<_>>();
    let strings = (0..count)
        .map(|i| (i % 11 != 0).then(|| format!("é-value-{}", i % 17)))
        .collect::<Vec<_>>();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(numbers.clone())),
            Arc::new(StringArray::from(strings.clone())),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(dictionary)
        .set_compression(Compression::SNAPPY)
        .set_write_batch_size(16)
        .set_data_page_row_count_limit(31)
        .build();
    let mut writer =
        ArrowWriter::try_new(File::create(&path).unwrap(), schema.clone(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let file = File::open(&path).unwrap();
    let metadata = ArrowReaderMetadata::load(&file, Default::default()).unwrap();
    let pool = MemoryPool::new(1 << 20);
    let reads = Arc::new(AtomicUsize::new(0));
    let refusals = Arc::new(AtomicUsize::new(0));
    let mut columns = ReservedVec::with_capacity(&pool, 2).unwrap();
    for c in 0..2 {
        let column = metadata.metadata().row_group(0).column(c);
        let (start, bytes) = column.byte_range();
        let pages = AdmittedColumnPages::new(
            CountedFile {
                file: file.try_clone().unwrap(),
                reads: reads.clone(),
            },
            start,
            bytes,
            count as u64,
            column.compression(),
            65536,
        )
        .unwrap();
        let inner = if c == 0 {
            AdmittedFlatColumn::new_fixed(pages, true, Type::INT64, 0, DataType::Int64).unwrap()
        } else {
            AdmittedFlatColumn::new(pages, true)
        };
        columns
            .extend_reserved(
                1,
                [TrialColumn {
                    inner,
                    reads: reads.clone(),
                    refuse_once: c == 1,
                    refusals: refusals.clone(),
                    terminal,
                }],
            )
            .unwrap();
    }
    let mut reader = AdmittedBatchReader::new(columns, schema, count, &pool).unwrap();
    if terminal {
        assert!(reader
            .next(97, 47, &pool)
            .unwrap_err()
            .to_string()
            .contains("test terminal output fault"));
        let stopped_reads = reads.load(Ordering::SeqCst);
        assert!(reader
            .next(1, 47, &pool)
            .unwrap_err()
            .to_string()
            .contains("poisoned"));
        assert_eq!(reads.load(Ordering::SeqCst), stopped_reads);
        drop(reader);
        assert_eq!(pool.used(), 0);
    } else {
        let mut actual_n = Vec::new();
        let mut actual_s = Vec::new();
        let mut held = Vec::new();
        while let Some(batch) = reader.next(97, 47, &pool).unwrap() {
            actual_n.extend(
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .iter(),
            );
            actual_s.extend(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .map(|v| v.map(str::to_owned)),
            );
            held.push(batch);
        }
        assert_eq!(actual_n, numbers);
        assert_eq!(actual_s, strings);
        assert!(
            held.len() > 2,
            "exercise byte targets, pending prefixes and multiple pages"
        );
        drop(reader);
        assert!(pool.used() > 0);
        drop(held);
        assert_eq!(pool.used(), 0);
    }
    assert_eq!(refusals.load(Ordering::SeqCst), 1);
    assert!(reads.load(Ordering::SeqCst) > 0);
}

#[test]
fn plain_trials_restore_cursors_without_source_reads() {
    trial_matrix(false, false);
}
#[test]
fn dictionary_trials_restore_ids_without_source_reads() {
    trial_matrix(true, false);
}
#[test]
fn plain_output_errors_are_terminal() {
    trial_matrix(false, true);
}
#[test]
fn dictionary_output_errors_are_terminal() {
    trial_matrix(true, true);
}
