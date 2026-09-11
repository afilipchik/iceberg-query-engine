//! Retained flat string and fixed-width column state over admitted page owners.
//! The caller verifies physical/logical annotations and row-group metadata.
use super::{
    admitted_column_pages::{AdmittedColumnPages, AdmittedPage},
    admitted_dictionary_utf8::DictionaryUtf8Decoder,
    admitted_hybrid::HybridDecoder,
    admitted_page_read::PageSource,
    admitted_plain_fixed::PlainFixedDecoder,
    admitted_plain_utf8::PlainUtf8Decoder,
    bounded_page_header::TypedPageHeader,
};
use crate::{execution::MemoryPool, QueryError, Result};
use arrow::datatypes::DataType;
use arrow::{
    array::{Array, ArrayRef, StringArray, UInt32Array},
    buffer::{Buffer, NullBuffer},
};
use parquet::basic::{Encoding, Type};
use std::sync::Arc;

mod dictionary_page;
use dictionary_page::DictionaryPage;

fn invalid(message: &str) -> QueryError {
    QueryError::Storage(format!("admitted flat column: {message}"))
}
enum Output {
    Plain(PlainUtf8Decoder<'static>),
    Dictionary(DictionaryUtf8Decoder),
    Fixed(PlainFixedDecoder),
}
impl Output {
    fn next(&mut self, rows: usize, bytes: usize, pool: &MemoryPool) -> Result<Option<ArrayRef>> {
        match self {
            Output::Plain(cursor) => cursor
                .next(rows, bytes, pool)
                .map(|a| a.map(|a| Arc::new(a) as ArrayRef)),
            Output::Dictionary(cursor) => cursor
                .next(rows, bytes, pool)
                .map(|a| a.map(|a| Arc::new(a) as ArrayRef)),
            Output::Fixed(cursor) => cursor.next(rows, pool),
        }
    }
}
#[derive(Clone)]
enum Dictionary {
    Utf8(StringArray),
    Fixed(Buffer, usize),
}
impl Dictionary {
    fn len(&self) -> usize {
        match self {
            Self::Utf8(a) => a.len(),
            Self::Fixed(_, n) => *n,
        }
    }
}
pub(crate) struct AdmittedFlatColumn<S> {
    pages: AdmittedColumnPages<S>,
    optional: bool,
    pending: Option<AdmittedPage>,
    dictionary: Option<Dictionary>,
    fixed: Option<(Type, usize, DataType)>,
    output: Option<Output>,
    dictionary_page: Option<DictionaryPage>,
    failed: bool,
}
impl<S: PageSource> AdmittedFlatColumn<S> {
    pub(crate) fn new(pages: AdmittedColumnPages<S>, optional: bool) -> Self {
        Self {
            pages,
            optional,
            pending: None,
            dictionary: None,
            fixed: None,
            output: None,
            dictionary_page: None,
            failed: false,
        }
    }
    pub(crate) fn new_fixed(
        pages: AdmittedColumnPages<S>,
        optional: bool,
        physical: Type,
        width: usize,
        data_type: DataType,
    ) -> Result<Self> {
        PlainFixedDecoder::new(
            Buffer::from(Vec::<u8>::new()),
            0,
            None,
            physical,
            width,
            data_type.clone(),
        )?;
        let mut column = Self::new(pages, optional);
        column.fixed = Some((physical, width, data_type));
        Ok(column)
    }
    pub(crate) fn next(
        &mut self,
        rows: usize,
        bytes: usize,
        pool: &MemoryPool,
    ) -> Result<Option<ArrayRef>> {
        if self.failed {
            return Err(invalid("column is poisoned"));
        }
        if rows == 0 || bytes == 0 {
            return Err(invalid("positive output targets required"));
        }
        let result = self.next_chunk(rows, bytes, pool);
        if result.as_ref().is_err_and(|e| !e.is_memory_limit()) {
            self.failed = true;
        }
        result
    }
    fn next_chunk(
        &mut self,
        rows: usize,
        bytes: usize,
        pool: &MemoryPool,
    ) -> Result<Option<ArrayRef>> {
        loop {
            if let Some(output) = self.output.as_mut() {
                let result = output.next(rows, bytes, pool)?;
                if result.is_some() {
                    return Ok(result);
                }
                self.output = None;
            }
            if let Some(page) = self.dictionary_page.as_mut() {
                if let Some(array) = page.next(rows, bytes, pool)? {
                    return Ok(Some(array));
                }
                self.dictionary_page = None;
            }
            if self.pending.is_none() {
                self.pending = self.pages.next(pool)?;
            }
            let Some(page) = self.pending.as_ref() else {
                return Ok(None);
            };
            // Retain a handed-off page until all downstream admission succeeds.
            // A refusal can rebuild provisional state, but cannot reread this page.
            if let TypedPageHeader::Dictionary { values, encoding } = page.header() {
                if *encoding != Encoding::PLAIN {
                    return Err(QueryError::NotImplemented(
                        "non-PLAIN UTF8 dictionary".into(),
                    ));
                }
                if let Some((physical, width, data_type)) = &self.fixed {
                    PlainFixedDecoder::new(
                        page.body().clone(),
                        *values,
                        None,
                        *physical,
                        *width,
                        data_type.clone(),
                    )?;
                    self.dictionary = Some(Dictionary::Fixed(page.body().clone(), *values));
                    self.pending = None;
                    continue;
                }
                let mut cursor = PlainUtf8Decoder::from_owned(page.body().clone(), *values, None)?;
                let dictionary = match cursor.next((*values).max(1), usize::MAX, pool)? {
                    Some(dictionary) => dictionary,
                    None => {
                        let mut offsets =
                            crate::execution::ReservedBufferBuilder::<i32>::with_capacity(pool, 1)?;
                        offsets.extend_reserved(1, [0])?;
                        StringArray::try_new(
                            arrow::buffer::OffsetBuffer::new(arrow::buffer::ScalarBuffer::new(
                                offsets.finish(),
                                0,
                                1,
                            )),
                            Buffer::from(Vec::<u8>::new()),
                            None,
                        )?
                    }
                };
                self.dictionary = Some(Dictionary::Utf8(dictionary));
                self.pending = None;
                continue;
            }
            let (count, encoding, values, validity) = self.data(page, pool)?;
            let output = match encoding {
                Encoding::PLAIN => {
                    if let Some((physical, width, data_type)) = &self.fixed {
                        Output::Fixed(PlainFixedDecoder::new(
                            values,
                            count,
                            validity,
                            *physical,
                            *width,
                            data_type.clone(),
                        )?)
                    } else {
                        Output::Plain(PlainUtf8Decoder::from_owned(values, count, validity)?)
                    }
                }
                Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
                    let dictionary = self
                        .dictionary
                        .as_ref()
                        .ok_or_else(|| invalid("IDs precede dictionary"))?;
                    self.dictionary_page = Some(DictionaryPage::new(
                        dictionary.clone(),
                        self.fixed.clone(),
                        count,
                        values,
                        validity,
                    )?);
                    self.pending = None;
                    continue;
                }
                _ => {
                    return Err(QueryError::NotImplemented(format!(
                        "UTF8 data encoding {encoding:?}"
                    )))
                }
            };
            self.output = Some(output);
            self.pending = None;
        }
    }
    fn data(
        &self,
        page: &AdmittedPage,
        pool: &MemoryPool,
    ) -> Result<(usize, Encoding, Buffer, Option<NullBuffer>)> {
        let body = page.body();
        let (count, encoding, definitions, offset, declared_nulls) = match page.header() {
            TypedPageHeader::DataV1 {
                values,
                encoding,
                definition,
                ..
            } => {
                if self.optional {
                    if *definition != Encoding::RLE {
                        return Err(QueryError::NotImplemented(
                            "legacy BIT_PACKED definitions".into(),
                        ));
                    }
                    let length = u32::from_le_bytes(
                        body.get(..4)
                            .ok_or_else(|| invalid("missing level length"))?
                            .try_into()
                            .unwrap(),
                    ) as usize;
                    let end = 4usize
                        .checked_add(length)
                        .filter(|end| *end <= body.len())
                        .ok_or_else(|| invalid("invalid level extent"))?;
                    (
                        *values,
                        *encoding,
                        Some(body.slice_with_length(4, length)),
                        end,
                        None,
                    )
                } else {
                    (*values, *encoding, None, 0, None)
                }
            }
            TypedPageHeader::DataV2 {
                values,
                rows,
                nulls,
                encoding,
                definition_bytes,
                repetition_bytes,
                ..
            } => {
                if rows != values || *repetition_bytes != 0 {
                    return Err(invalid("nested page in flat column"));
                }
                if !self.optional && (*definition_bytes != 0 || *nulls != 0) {
                    return Err(invalid("required column has definitions or NULLs"));
                }
                if *definition_bytes > body.len() {
                    return Err(invalid("invalid definition extent"));
                }
                (
                    *values,
                    *encoding,
                    self.optional
                        .then(|| body.slice_with_length(0, *definition_bytes)),
                    *definition_bytes,
                    Some(*nulls),
                )
            }
            _ => return Err(invalid("expected data page")),
        };
        let validity = if let Some(definitions) = definitions {
            let mut cursor = HybridDecoder::from_owned(definitions, 1, count)?;
            cursor.next_validity(count.max(1), pool)?
        } else {
            None
        };
        if declared_nulls.is_some_and(|n| n != validity.as_ref().map_or(0, NullBuffer::null_count))
        {
            return Err(invalid("decoded NULL count differs from V2 header"));
        }
        Ok((count, encoding, body.slice(offset), validity))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use arrow::{
        array::Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use parquet::{
        basic::Compression,
        file::{
            properties::{WriterProperties, WriterVersion},
            reader::FileReader,
        },
    };
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    struct Counted {
        file: std::fs::File,
        reads: Arc<AtomicUsize>,
    }
    impl PageSource for Counted {
        fn len(&self) -> std::io::Result<u64> {
            PageSource::len(&self.file)
        }
        fn read_at(&self, target: &mut [u8], offset: u64) -> std::io::Result<usize> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            self.file.read_at(target, offset)
        }
    }
    #[test]
    fn real_flat_columns_resume_across_pages_encodings_and_budget_denial() {
        for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            for dictionary in [false, true] {
                for (optional, all_null) in [(false, false), (true, false), (true, true)] {
                    let directory = tempfile::tempdir().unwrap();
                    let path = directory.path().join("column.parquet");
                    let expected: Vec<Option<String>> = (0..257)
                        .map(|i| {
                            if optional && (all_null || i % 7 == 0) {
                                None
                            } else {
                                Some(format!("é-{}", i % 11))
                            }
                        })
                        .collect();
                    let schema =
                        Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, optional)]));
                    let batch = RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(StringArray::from(expected.clone()))],
                    )
                    .unwrap();
                    let props = WriterProperties::builder()
                        .set_writer_version(version)
                        .set_dictionary_enabled(dictionary)
                        .set_encoding(Encoding::PLAIN)
                        .set_compression(Compression::SNAPPY)
                        .set_data_page_row_count_limit(31)
                        .set_write_batch_size(16)
                        .set_max_row_group_row_count(Some(97))
                        .build();
                    let mut writer = parquet::arrow::ArrowWriter::try_new(
                        std::fs::File::create(&path).unwrap(),
                        schema,
                        Some(props),
                    )
                    .unwrap();
                    writer.write(&batch).unwrap();
                    writer.close().unwrap();
                    let metadata = parquet::file::serialized_reader::SerializedFileReader::new(
                        std::fs::File::open(&path).unwrap(),
                    )
                    .unwrap();
                    let pool = MemoryPool::new(65536);
                    let mut actual = Vec::new();
                    let reads = Arc::new(AtomicUsize::new(0));
                    for group in 0..metadata.num_row_groups() {
                        let column = metadata.metadata().row_group(group).column(0);
                        let (start, length) = column.byte_range();
                        let pages = AdmittedColumnPages::new(
                            Counted {
                                file: std::fs::File::open(&path).unwrap(),
                                reads: reads.clone(),
                            },
                            start,
                            length,
                            column.num_values() as u64,
                            column.compression(),
                            4096,
                        )
                        .unwrap();
                        let mut reader = AdmittedFlatColumn::new(pages, optional);
                        reader.pending = reader.pages.next(&pool).unwrap();
                        let before = reads.load(Ordering::SeqCst);
                        let input_charge = pool.used();
                        let held = pool.allocate(pool.max() - input_charge).unwrap();
                        assert!(reader.next(3, 17, &pool).unwrap_err().is_memory_limit());
                        assert_eq!(reads.load(Ordering::SeqCst), before);
                        drop(held);
                        assert_eq!(pool.used(), input_charge);
                        let first = reader.next(3, 17, &pool).unwrap().unwrap();
                        actual.extend(
                            first
                                .as_any()
                                .downcast_ref::<StringArray>()
                                .unwrap()
                                .iter()
                                .map(|v| v.map(str::to_owned)),
                        );
                        drop(first);
                        let before = reads.load(Ordering::SeqCst);
                        let held = pool.allocate(pool.max() - pool.used()).unwrap();
                        assert!(reader.next(3, 17, &pool).unwrap_err().is_memory_limit());
                        assert_eq!(reads.load(Ordering::SeqCst), before);
                        drop(held);
                        while let Some(chunk) = reader.next(3, 17, &pool).unwrap() {
                            assert!(chunk.len() <= 3);
                            actual.extend(
                                chunk
                                    .as_any()
                                    .downcast_ref::<StringArray>()
                                    .unwrap()
                                    .iter()
                                    .map(|v| v.map(str::to_owned)),
                            );
                        }
                        assert!(reader.next(3, 17, &pool).unwrap().is_none());
                        drop(reader);
                        assert_eq!(pool.used(), 0);
                    }
                    assert_eq!(
                        actual, expected,
                        "{version:?} dictionary={dictionary} optional={optional}"
                    );
                    assert!(
                        reads.load(Ordering::SeqCst) > 18,
                        "fixture must exercise multiple pages per group"
                    );
                }
            }
        }
    }
    #[test]
    fn unsupported_encoding_poison_does_not_replay_source() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("delta.parquet");
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                "alpha", "alphabet", "beta",
            ]))],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_writer_version(WriterVersion::PARQUET_2_0)
            .set_dictionary_enabled(false)
            .set_encoding(Encoding::DELTA_BYTE_ARRAY)
            .build();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema,
            Some(props),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let metadata = parquet::file::serialized_reader::SerializedFileReader::new(
            std::fs::File::open(&path).unwrap(),
        )
        .unwrap();
        let column = metadata.metadata().row_group(0).column(0);
        let (start, length) = column.byte_range();
        let reads = Arc::new(AtomicUsize::new(0));
        let pages = AdmittedColumnPages::new(
            Counted {
                file: std::fs::File::open(&path).unwrap(),
                reads: reads.clone(),
            },
            start,
            length,
            3,
            column.compression(),
            4096,
        )
        .unwrap();
        let pool = MemoryPool::new(65536);
        let mut reader = AdmittedFlatColumn::new(pages, false);
        let error = reader.next(2, 20, &pool).unwrap_err();
        assert!(matches!(error, QueryError::NotImplemented(_)));
        assert!(error.to_string().contains("DELTA_BYTE_ARRAY"));
        let before = reads.load(Ordering::SeqCst);
        assert!(reader
            .next(2, 20, &pool)
            .unwrap_err()
            .to_string()
            .contains("poisoned"));
        assert_eq!(reads.load(Ordering::SeqCst), before);
        drop(reader);
        assert_eq!(pool.used(), 0);

        let reader_metadata = parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
            &mut std::fs::File::open(&path).unwrap(),
            parquet::arrow::arrow_reader::ArrowReaderOptions::new(),
        )
        .unwrap();
        // The source has no payload: capability refusal must precede even page
        // extent construction, which would otherwise fail against this file.
        let empty_source = tempfile::tempfile().unwrap();
        let error = match crate::storage::admitted_row_group::open(
            &empty_source,
            &reader_metadata,
            0,
            &[0],
            batch.schema(),
            &pool,
        ) {
            Err(error) => error,
            Ok(_) => panic!("delta encoding passed preflight"),
        };
        assert!(error
            .to_string()
            .contains("codec or encoding requires another reader"));
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn large_dictionary_pages_emit_bounded_ids_with_exact_values() {
        use arrow::array::Int64Array;
        let numbers = (0..12000)
            .map(|i| {
                if i % 7 == 0 {
                    None
                } else {
                    Some((i % 3) as i64 - 1)
                }
            })
            .collect::<Vec<_>>();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(numbers.clone())),
            Arc::new(StringArray::from(
                numbers
                    .iter()
                    .map(|v| {
                        v.map(|n| match n {
                            -1 => "alpha",
                            0 => "beta",
                            _ => "gamma",
                        })
                    })
                    .collect::<Vec<_>>(),
            )),
        ];
        for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("dictionary.parquet");
            let batch = RecordBatch::try_from_iter(
                arrays
                    .iter()
                    .enumerate()
                    .map(|(i, a)| (format!("c{i}"), a.clone())),
            )
            .unwrap();
            let props = WriterProperties::builder()
                .set_writer_version(version)
                .set_dictionary_enabled(true)
                .set_compression(Compression::SNAPPY)
                .set_data_page_row_count_limit(20000)
                .set_write_batch_size(20000)
                .build();
            let mut writer = parquet::arrow::ArrowWriter::try_new(
                std::fs::File::create(&path).unwrap(),
                batch.schema(),
                Some(props),
            )
            .unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            let metadata = parquet::file::serialized_reader::SerializedFileReader::new(
                std::fs::File::open(&path).unwrap(),
            )
            .unwrap();
            for (index, expected) in arrays.iter().enumerate() {
                let column = metadata.metadata().row_group(0).column(index);
                assert!(column.dictionary_page_offset().is_some());
                let (start, length) = column.byte_range();
                let reads = Arc::new(AtomicUsize::new(0));
                let pages = AdmittedColumnPages::new(
                    Counted {
                        file: std::fs::File::open(&path).unwrap(),
                        reads: reads.clone(),
                    },
                    start,
                    length,
                    12000,
                    column.compression(),
                    1024,
                )
                .unwrap();
                let mut reader = if index == 0 {
                    AdmittedFlatColumn::new_fixed(pages, true, Type::INT64, 0, DataType::Int64)
                        .unwrap()
                } else {
                    AdmittedFlatColumn::new(pages, true)
                };
                let pool = MemoryPool::new(16 * 1024);
                let first = reader.next(37, 512, &pool).unwrap().unwrap();
                assert_eq!(first.to_data(), expected.slice(0, first.len()).to_data());
                let mut row = first.len();
                drop(first);
                let calls = reads.load(Ordering::SeqCst);
                let held = pool.allocate(pool.max() - pool.used()).unwrap();
                assert!(reader.next(37, 512, &pool).unwrap_err().is_memory_limit());
                assert_eq!(reads.load(Ordering::SeqCst), calls);
                drop(held);
                while let Some(chunk) = reader.next(37, 512, &pool).unwrap() {
                    assert!(chunk.len() <= 37);
                    assert_eq!(chunk.to_data(), expected.slice(row, chunk.len()).to_data());
                    row += chunk.len();
                    assert_eq!(reads.load(Ordering::SeqCst), calls);
                }
                assert_eq!(row, 12000);
                drop(reader);
                assert_eq!(pool.used(), 0);
            }
        }
    }

    #[test]
    fn fixed_columns_match_typed_oracle_across_plain_dictionary_and_v2_pages() {
        use arrow::array::{
            BooleanArray, Date32Array, Decimal128Array, Float64Array, Int64Array,
            TimestampMicrosecondArray, UInt32Array,
        };
        let values: Vec<Option<i64>> = (0..45)
            .map(|i| if i % 7 == 0 { None } else { Some(i % 11 - 5) })
            .collect();
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(values.clone())),
            Arc::new(UInt32Array::from(
                values
                    .iter()
                    .map(|v| v.map(|v| v as u32))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Date32Array::from(
                values
                    .iter()
                    .map(|v| v.map(|v| v as i32))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(
                Decimal128Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v as i128 * 10i128.pow(25)))
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(38, 4)
                .unwrap(),
            ),
            Arc::new(
                Decimal128Array::from(
                    values
                        .iter()
                        .map(|v| v.map(|v| v as i128 * 123))
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(9, 2)
                .unwrap(),
            ),
            Arc::new(Float64Array::from(
                values
                    .iter()
                    .map(|v| v.map(|v| v as f64 / 2.0))
                    .collect::<Vec<_>>(),
            )),
            Arc::new(BooleanArray::from(
                values.iter().map(|v| v.map(|v| v > 0)).collect::<Vec<_>>(),
            )),
            Arc::new(TimestampMicrosecondArray::from(values.clone()).with_timezone("UTC")),
            Arc::new(StringArray::from(
                values
                    .iter()
                    .map(|v| v.map(|v| format!("value-{v}")))
                    .collect::<Vec<_>>(),
            )),
        ];
        for version in [WriterVersion::PARQUET_1_0, WriterVersion::PARQUET_2_0] {
            for dictionary in [false, true] {
                let directory = tempfile::tempdir().unwrap();
                let path = directory.path().join("fixed.parquet");
                let schema = Arc::new(Schema::new(
                    arrays
                        .iter()
                        .enumerate()
                        .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
                        .collect::<Vec<_>>(),
                ));
                let batch = RecordBatch::try_new(schema.clone(), arrays.clone()).unwrap();
                let props = WriterProperties::builder()
                    .set_writer_version(version)
                    .set_dictionary_enabled(dictionary)
                    .set_encoding(Encoding::PLAIN)
                    .set_compression(Compression::SNAPPY)
                    .set_data_page_row_count_limit(8)
                    .set_write_batch_size(4)
                    .set_max_row_group_row_count(Some(17))
                    .build();
                let mut writer = parquet::arrow::ArrowWriter::try_new(
                    std::fs::File::create(&path).unwrap(),
                    schema,
                    Some(props),
                )
                .unwrap();
                writer.write(&batch).unwrap();
                writer.close().unwrap();
                let metadata = parquet::file::serialized_reader::SerializedFileReader::new(
                    std::fs::File::open(&path).unwrap(),
                )
                .unwrap();
                let pool = MemoryPool::new(1 << 20);
                let mut group_start = 0;
                for group in 0..metadata.num_row_groups() {
                    let rg = metadata.metadata().row_group(group);
                    for (index, expected) in arrays.iter().enumerate() {
                        let column = rg.column(index);
                        let (start, length) = column.byte_range();
                        let pages = AdmittedColumnPages::new(
                            std::fs::File::open(&path).unwrap(),
                            start,
                            length,
                            column.num_values() as u64,
                            column.compression(),
                            4096,
                        )
                        .unwrap();
                        let mut reader = if expected.data_type() == &DataType::Utf8 {
                            AdmittedFlatColumn::new(pages, true)
                        } else {
                            AdmittedFlatColumn::new_fixed(
                                pages,
                                true,
                                column.column_type(),
                                column.column_descr().type_length().max(0) as usize,
                                expected.data_type().clone(),
                            )
                            .unwrap()
                        };
                        let mut row = group_start;
                        while let Some(chunk) = reader.next(3, 100, &pool).unwrap() {
                            assert_eq!(
                                chunk.to_data(),
                                expected.slice(row, chunk.len()).to_data(),
                                "{version:?} dict={dictionary} column={index} row={row}"
                            );
                            row += chunk.len();
                        }
                        assert_eq!(row, group_start + rg.num_rows() as usize);
                        drop(reader);
                        assert_eq!(pool.used(), 0);
                    }
                    let mut sources = crate::execution::reserved_vec::ReservedVec::with_capacity(
                        &pool,
                        arrays.len(),
                    )
                    .unwrap();
                    for (index, expected) in arrays.iter().enumerate() {
                        let column = rg.column(index);
                        let (start, length) = column.byte_range();
                        let pages = AdmittedColumnPages::new(
                            std::fs::File::open(&path).unwrap(),
                            start,
                            length,
                            column.num_values() as u64,
                            column.compression(),
                            4096,
                        )
                        .unwrap();
                        let source = if expected.data_type() == &DataType::Utf8 {
                            AdmittedFlatColumn::new(pages, true)
                        } else {
                            AdmittedFlatColumn::new_fixed(
                                pages,
                                true,
                                column.column_type(),
                                column.column_descr().type_length().max(0) as usize,
                                expected.data_type().clone(),
                            )
                            .unwrap()
                        };
                        sources.extend_reserved(1, [source]).unwrap();
                    }
                    let mut aligned = crate::storage::admitted_batch::AdmittedBatchReader::new(
                        sources,
                        batch.schema(),
                        rg.num_rows() as usize,
                        &pool,
                    )
                    .unwrap();
                    let mut row = group_start;
                    let mut batches = 0;
                    use crate::planner::{BinaryOp, Column, Expr, ScalarValue};
                    let predicate = Expr::BinaryExpr {
                        left: Box::new(Expr::BinaryExpr {
                            left: Box::new(Expr::Column(Column::new("c0"))),
                            op: BinaryOp::Gt,
                            right: Box::new(Expr::Literal(ScalarValue::Int64(0))),
                        }),
                        op: BinaryOp::Or,
                        right: Box::new(Expr::BinaryExpr {
                            left: Box::new(Expr::Column(Column::new("c2"))),
                            op: BinaryOp::Lt,
                            right: Box::new(Expr::Literal(ScalarValue::Date32(-2))),
                        }),
                    };
                    let membership = Expr::InList {
                        expr: Box::new(Expr::Column(Column::new("c8"))),
                        list: vec![
                            Expr::Literal(ScalarValue::Utf8("value-1".into())),
                            Expr::Literal(ScalarValue::Utf8("value--3".into())),
                        ],
                        negated: false,
                    };
                    let predicate = Expr::BinaryExpr {
                        left: Box::new(Expr::BinaryExpr {
                            left: Box::new(predicate),
                            op: BinaryOp::And,
                            right: Box::new(membership),
                        }),
                        op: BinaryOp::Or,
                        right: Box::new(Expr::BinaryExpr {
                            left: Box::new(Expr::Column(Column::new("c8"))),
                            op: BinaryOp::Like,
                            right: Box::new(Expr::Literal(ScalarValue::Utf8("%5".into()))),
                        }),
                    };
                    let compiled =
                        crate::physical::compiled_expr::CompiledPredicate::compile_reserved(
                            &predicate,
                            batch.schema().as_ref(),
                            &pool,
                        )
                        .unwrap()
                        .unwrap();

                    while let Some(output) = aligned.next(3, 5, &pool).unwrap() {
                        for (index, expected) in arrays.iter().enumerate() {
                            assert_eq!(
                                output.column(index).to_data(),
                                expected.slice(row, output.num_rows()).to_data(),
                                "aligned {version:?} dict={dictionary} column={index} row={row}"
                            );
                        }
                        let mask = compiled.evaluate_admitted(&output, &pool).unwrap().unwrap();
                        assert_eq!(
                            mask.iter().collect::<Vec<_>>(),
                            values[row..row + output.num_rows()]
                                .iter()
                                .map(|v| v
                                    .map(|v| ((v > 0 || v < -2) && (v == 1 || v == -3))
                                        || v.abs() == 5))
                                .collect::<Vec<_>>()
                        );
                        let filtered =
                            crate::storage::admitted_gather::filter(&output, &mask, &pool).unwrap();
                        let selected = UInt32Array::from(
                            (0..output.num_rows())
                                .filter(|i| mask.is_valid(*i) && mask.value(*i))
                                .map(|i| (row + i) as u32)
                                .collect::<Vec<_>>(),
                        );
                        for (index, expected) in arrays.iter().enumerate() {
                            let oracle =
                                arrow::compute::take(expected.as_ref(), &selected, None).unwrap();
                            assert_eq!(filtered.column(index).to_data(), oracle.to_data());
                        }
                        row += output.num_rows();
                        batches += 1;
                    }
                    assert_eq!(row, group_start + rg.num_rows() as usize);
                    assert!(batches > (rg.num_rows() as usize).div_ceil(3));
                    drop(compiled);
                    drop(aligned);
                    assert_eq!(pool.used(), 0);
                    let reader_metadata = parquet::arrow::arrow_reader::ArrowReaderMetadata::load(
                        &mut std::fs::File::open(&path).unwrap(),
                        parquet::arrow::arrow_reader::ArrowReaderOptions::new(),
                    )
                    .unwrap();
                    for projection in [(0..arrays.len()).collect::<Vec<_>>(), vec![8usize, 0, 3, 8]]
                    {
                        let projected_schema =
                            Arc::new(batch.schema().project(&projection).unwrap());
                        let mut projected = crate::storage::admitted_row_group::open(
                            &std::fs::File::open(&path).unwrap(),
                            &reader_metadata,
                            group,
                            &projection,
                            projected_schema.clone(),
                            &pool,
                        )
                        .unwrap();
                        let mut row = group_start;
                        while let Some(output) = projected.next(3, 5, &pool).unwrap() {
                            assert_eq!(output.schema(), projected_schema);
                            for (position, index) in projection.iter().copied().enumerate() {
                                assert_eq!(
                                    output.column(position).to_data(),
                                    arrays[index].slice(row, output.num_rows()).to_data()
                                );
                            }
                            row += output.num_rows();
                        }
                        assert_eq!(row, group_start + rg.num_rows() as usize);
                        drop(projected);
                        assert_eq!(pool.used(), 0);
                    }
                    group_start += rg.num_rows() as usize;
                }
                assert_eq!(group_start, values.len());
            }
        }
    }
}
