//! Admitted Arrow output from an already decompressed flat PLAIN BYTE_ARRAY page.
//!
//! Borrowed and owned page inputs preserve decoded definition-level ownership. This
//! component does not admit decompression, dictionaries, or nested repetition.
//! Value-payload byte targets govern chunking; actual buffers are admitted before build.
use crate::{
    execution::{MemoryPool, ReservedBufferBuilder},
    QueryError, Result,
};
use arrow::{
    array::StringArray,
    buffer::{BooleanBuffer, Buffer, NullBuffer, OffsetBuffer, ScalarBuffer},
};

fn invalid(message: &str) -> QueryError {
    QueryError::Storage(format!("PLAIN UTF8 page: {message}"))
}

fn value(data: &[u8], offset: usize) -> Result<(&[u8], usize)> {
    let header_end = offset
        .checked_add(4)
        .ok_or_else(|| invalid("length overflow"))?;
    let header = data
        .get(offset..header_end)
        .ok_or_else(|| invalid("truncated length"))?;
    let length = u32::from_le_bytes(header.try_into().unwrap()) as usize;
    let end = header_end
        .checked_add(length)
        .ok_or_else(|| invalid("value extent overflow"))?;
    Ok((
        data.get(header_end..end)
            .ok_or_else(|| invalid("truncated value"))?,
        end,
    ))
}

enum PlainInput<'a> {
    Borrowed(&'a [u8]),
    Owned(Buffer),
}
impl std::ops::Deref for PlainInput<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(buffer) => buffer.as_slice(),
        }
    }
}
pub(crate) struct PlainUtf8Decoder<'a> {
    data: PlainInput<'a>,
    validity: Option<NullBuffer>,
    rows: usize,
    row: usize,
    offset: usize,
}

impl<'a> PlainUtf8Decoder<'a> {
    /// Validate every value and exact encoded extent without allocating output.
    /// The caller must have verified that this is a flat PLAIN BYTE_ARRAY body,
    /// with definition/repetition bytes removed and matching decoded validity.
    pub(crate) fn new(
        data: &'a [u8],
        rows: usize,
        validity: Option<&'a NullBuffer>,
    ) -> Result<Self> {
        Self::with_input(PlainInput::Borrowed(data), rows, validity.cloned())
    }

    pub(crate) fn from_owned(
        data: Buffer,
        rows: usize,
        validity: Option<NullBuffer>,
    ) -> Result<Self> {
        Self::with_input(PlainInput::Owned(data), rows, validity)
    }

    fn with_input(data: PlainInput<'a>, rows: usize, validity: Option<NullBuffer>) -> Result<Self> {
        if validity.as_ref().is_some_and(|v| v.len() != rows) {
            return Err(invalid("definition-level row count differs"));
        }
        let mut offset = 0;
        for row in 0..rows {
            if validity.as_ref().is_none_or(|v| v.is_valid(row)) {
                let (bytes, end) = value(&data, offset)?;
                std::str::from_utf8(bytes).map_err(|_| invalid("invalid UTF8"))?;
                offset = end;
            }
        }
        if offset != data.len() {
            return Err(invalid("trailing bytes or value-count mismatch"));
        }
        Ok(Self {
            data,
            validity,
            rows,
            row: 0,
            offset: 0,
        })
    }

    /// A denial leaves both row and encoded cursor unchanged. One oversized
    /// value can exceed the preferred byte target only if its actual buffers
    /// obtain pool admission. An Arrow i32 offset overflow refuses explicitly.
    pub(crate) fn next(
        &mut self,
        max_rows: usize,
        target_value_bytes: usize,
        pool: &MemoryPool,
    ) -> Result<Option<StringArray>> {
        if max_rows == 0 || target_value_bytes == 0 {
            return Err(invalid("positive row and byte targets required"));
        }
        if self.row == self.rows {
            return Ok(None);
        }
        let mut rows = 0;
        let mut payload = 0usize;
        let mut end = self.offset;
        while rows < max_rows && rows < self.rows - self.row {
            let (length, next) = if self
                .validity
                .as_ref()
                .is_none_or(|v| v.is_valid(self.row + rows))
            {
                let (bytes, next) = value(&self.data, end)?;
                (bytes.len(), next)
            } else {
                (0, end)
            };
            let total = payload
                .checked_add(length)
                .ok_or_else(|| invalid("output extent overflow"))?;
            if rows > 0 && (total > target_value_bytes || total > i32::MAX as usize) {
                break;
            }
            if total > i32::MAX as usize {
                return Err(invalid("one value exceeds Arrow UTF8 offset domain"));
            }
            payload = total;
            end = next;
            rows += 1;
        }
        let mut offsets = ReservedBufferBuilder::<i32>::with_capacity(
            pool,
            rows.checked_add(1)
                .ok_or_else(|| invalid("offset count overflow"))?,
        )?;
        let mut values = ReservedBufferBuilder::<u8>::with_capacity(pool, payload)?;
        let mut validity = if self.validity.is_some() {
            let bytes = rows.div_ceil(8);
            let mut bits = ReservedBufferBuilder::<u8>::with_capacity(pool, bytes)?;
            bits.extend_reserved(bytes, std::iter::repeat(0))?;
            Some(bits)
        } else {
            None
        };
        offsets.extend_reserved(1, [0])?;
        let mut cursor = self.offset;
        for i in 0..rows {
            if self
                .validity
                .as_ref()
                .is_none_or(|v| v.is_valid(self.row + i))
            {
                let (bytes, next) = value(&self.data, cursor)?;
                values.extend_reserved(bytes.len(), bytes.iter().copied())?;
                cursor = next;
                if let Some(bits) = validity.as_mut() {
                    bits.as_mut_slice()[i / 8] |= 1 << (i % 8);
                }
            }
            offsets.extend_reserved(1, [values.as_slice().len() as i32])?;
        }
        let nulls =
            validity.map(|bits| NullBuffer::new(BooleanBuffer::new(bits.finish(), 0, rows)));
        let offsets = OffsetBuffer::new(ScalarBuffer::new(offsets.finish(), 0, rows + 1));
        let array = StringArray::try_new(offsets, values.finish(), nulls)?;
        self.row += rows;
        self.offset = end;
        Ok(Some(array))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;

    fn encode(values: &[&str]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for value in values {
            bytes.extend_from_slice(&(value.len() as u32).to_le_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        bytes
    }

    #[test]
    fn owned_page_and_validity_survive_caller_handles() {
        let pool = MemoryPool::new(8192);
        let encoded = encode(&["first", "é"]);
        let mut bytes = ReservedBufferBuilder::<u8>::with_capacity(&pool, encoded.len()).unwrap();
        bytes.extend_reserved(encoded.len(), encoded).unwrap();
        let page = bytes.finish();
        let mut bits = ReservedBufferBuilder::<u8>::with_capacity(&pool, 1).unwrap();
        bits.extend_reserved(1, [0b101]).unwrap();
        let validity = NullBuffer::new(BooleanBuffer::new(bits.finish(), 0, 3));
        let input_charge = pool.used();
        let mut decoder =
            PlainUtf8Decoder::from_owned(page.clone(), 3, Some(validity.clone())).unwrap();
        drop(page);
        drop(validity);
        let first = decoder.next(2, 100, &pool).unwrap().unwrap();
        assert_eq!(first.iter().collect::<Vec<_>>(), vec![Some("first"), None]);
        drop(first);
        assert_eq!(pool.used(), input_charge);
        let last = decoder.next(2, 100, &pool).unwrap().unwrap();
        assert_eq!(last.value(0), "é");
        drop(decoder);
        assert!(pool.used() > 0);
        drop(last);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn byte_chunks_preserve_nulls_duplicates_empty_unicode_and_sliced_validity() {
        let page = encode(&["same", "", "é", "same"]);
        let validity =
            NullBuffer::from(vec![false, true, false, true, true, false, true]).slice(1, 6);
        let pool = MemoryPool::new(65536);
        let mut decoder = PlainUtf8Decoder::new(&page, 6, Some(&validity)).unwrap();
        let mut values = Vec::new();
        let mut chunks = 0;
        while let Some(array) = decoder.next(3, 5, &pool).unwrap() {
            chunks += 1;
            values.extend(array.iter().map(|s| s.map(str::to_owned)));
        }
        assert!(chunks > 1);
        assert_eq!(
            values,
            vec![
                Some("same".into()),
                None,
                Some("".into()),
                Some("é".into()),
                None,
                Some("same".into())
            ]
        );
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn denial_preserves_cursor_and_output_slices_retain_admission() {
        let long = "x".repeat(8192);
        let page = encode(&["first", &long, "last"]);
        let pool = MemoryPool::new(32768);
        let mut decoder = PlainUtf8Decoder::new(&page, 3, None).unwrap();
        let first = decoder.next(1, 16, &pool).unwrap().unwrap();
        assert_eq!(first.value(0), "first");
        drop(first);
        let held = pool.allocate(30000).unwrap();
        assert!(decoder.next(8, 16, &pool).unwrap_err().is_memory_limit());
        assert_eq!((decoder.row, decoder.offset), (1, 9));
        assert_eq!(pool.used(), 30000);
        drop(held);
        let oversized = decoder.next(8, 16, &pool).unwrap().unwrap();
        assert_eq!(oversized.len(), 1);
        assert_eq!(oversized.value(0), long);
        let slice = oversized.slice(0, 1);
        drop(oversized);
        assert!(pool.used() >= 8192);
        drop(slice);
        assert_eq!(pool.used(), 0);
        assert_eq!(
            decoder.next(8, 16, &pool).unwrap().unwrap().value(0),
            "last"
        );
        assert!(decoder.next(8, 16, &pool).unwrap().is_none());
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn malformed_pages_and_empty_inputs_do_not_allocate_output() {
        for bytes in [
            vec![1, 0],
            vec![255, 255, 255, 255],
            vec![1, 0, 0, 0, 255],
            encode(&["a", "b"]),
        ] {
            assert!(PlainUtf8Decoder::new(&bytes, 1, None).is_err());
        }
        let pool = MemoryPool::new(8192);
        let mut empty = PlainUtf8Decoder::new(&[], 0, None).unwrap();
        assert!(empty.next(1, 1, &pool).unwrap().is_none());
        let nulls = NullBuffer::new_null(5);
        let mut decoder = PlainUtf8Decoder::new(&[], 5, Some(&nulls)).unwrap();
        let array = decoder.next(5, 1, &pool).unwrap().unwrap();
        assert_eq!(array.null_count(), 5);
        drop(array);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn writer_produced_plain_pages_decode_with_admitted_output() {
        use parquet::file::reader::FileReader;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("plain.parquet");
        let values: Vec<String> = (0..37)
            .map(|i| format!("é-{}-{}", i % 4, "v".repeat(i * 3)))
            .collect();
        let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("v", arrow::datatypes::DataType::Utf8, false),
        ]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![std::sync::Arc::new(StringArray::from(values.clone()))],
        )
        .unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_max_row_group_row_count(Some(11))
            .build();
        let mut writer = parquet::arrow::ArrowWriter::try_new(
            std::fs::File::create(&path).unwrap(),
            schema,
            Some(props),
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let reader = parquet::file::serialized_reader::SerializedFileReader::new(
            std::fs::File::open(path).unwrap(),
        )
        .unwrap();
        let pool = MemoryPool::new(65536);
        let mut actual = Vec::new();
        for group in 0..reader.num_row_groups() {
            let row_group = reader.get_row_group(group).unwrap();
            let mut pages = row_group.get_column_page_reader(0).unwrap();
            while let Some(page) = pages.get_next_page().unwrap() {
                assert!(page.is_data_page());
                assert_eq!(page.encoding(), parquet::basic::Encoding::PLAIN);
                let mut decoder =
                    PlainUtf8Decoder::new(page.buffer(), page.num_values() as usize, None).unwrap();
                while let Some(output) = decoder.next(5, 128, &pool).unwrap() {
                    actual.extend(output.iter().map(|s| s.unwrap().to_owned()));
                }
                assert_eq!(pool.used(), 0);
            }
        }
        assert_eq!(actual, values);
    }
}
