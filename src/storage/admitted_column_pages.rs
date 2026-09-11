//! Owned positional column-page cursor. Memory denial preserves its position;
//! malformed/unsupported pages and I/O failures poison it without source replay.
use super::{
    admitted_page_body::decode_page_body,
    admitted_page_read::PageSource,
    bounded_page_header::{AdmittedPageHeader, TypedPageHeader},
};
use crate::{execution::MemoryPool, QueryError, Result};
use arrow::buffer::Buffer;
use parquet::basic::Compression;

fn invalid(message: &str) -> QueryError {
    QueryError::Storage(format!("admitted column pages: {message}"))
}
pub(crate) struct AdmittedColumnPages<S> {
    source: S,
    offset: u64,
    end: u64,
    expected_values: u64,
    values: u64,
    dictionary: bool,
    data_seen: bool,
    failed: bool,
    codec: Compression,
    header_bound: usize,
}
#[derive(Debug)]
pub(crate) struct AdmittedPage {
    header: TypedPageHeader,
    body: Buffer,
}
impl AdmittedPage {
    pub(crate) fn header(&self) -> &TypedPageHeader {
        &self.header
    }
    pub(crate) fn body(&self) -> &Buffer {
        &self.body
    }
}
impl<S: PageSource> AdmittedColumnPages<S> {
    pub(crate) fn new(
        source: S,
        offset: u64,
        bytes: u64,
        expected_values: u64,
        codec: Compression,
        header_bound: usize,
    ) -> Result<Self> {
        let end = offset
            .checked_add(bytes)
            .ok_or_else(|| invalid("column extent overflow"))?;
        if header_bound == 0 || end > source.len()? {
            return Err(invalid("invalid column extent or header bound"));
        }
        Ok(Self {
            source,
            offset,
            end,
            expected_values,
            values: 0,
            dictionary: false,
            data_seen: false,
            failed: false,
            codec,
            header_bound,
        })
    }
    pub(crate) fn next(&mut self, pool: &MemoryPool) -> Result<Option<AdmittedPage>> {
        if self.failed {
            return Err(invalid("cursor is poisoned"));
        }
        let result = self.next_page(pool);
        if result.as_ref().is_err_and(|error| !error.is_memory_limit()) {
            self.failed = true;
        }
        result
    }
    fn next_page(&mut self, pool: &MemoryPool) -> Result<Option<AdmittedPage>> {
        if self.offset == self.end {
            if self.values != self.expected_values {
                return Err(invalid("column value count differs"));
            }
            return Ok(None);
        }
        let header =
            AdmittedPageHeader::read(&self.source, self.offset, self.end, self.header_bound, pool)?;
        let typed = header.typed()?;
        let (next_values, dictionary, data_seen) = match &typed {
            TypedPageHeader::Dictionary { .. } => {
                if self.dictionary || self.data_seen {
                    return Err(invalid("dictionary must occur once before data"));
                }
                (self.values, true, false)
            }
            TypedPageHeader::DataV1 { values, .. } | TypedPageHeader::DataV2 { values, .. } => {
                let total = self
                    .values
                    .checked_add(*values as u64)
                    .ok_or_else(|| invalid("column value count overflow"))?;
                if total > self.expected_values {
                    return Err(invalid("page values exceed column count"));
                }
                (total, self.dictionary, true)
            }
        };
        let next_offset = self
            .offset
            .checked_add(header.envelope().header_bytes as u64)
            .and_then(|n| n.checked_add(header.envelope().compressed_bytes as u64))
            .ok_or_else(|| invalid("page extent overflow"))?;
        if next_offset == self.end && next_values != self.expected_values {
            return Err(invalid("final page value count differs"));
        }
        let encoded = header.read_body(&self.source, pool)?;
        let (codec, prefix) = typed.decode_layout(self.codec);
        let decoded_bytes = header.envelope().decoded_bytes;
        // Typed fields and the verified encoded body own everything needed
        // below. Release the potentially large header read window before
        // admitting decompression, rather than retaining three live buffers.
        drop(header);
        let body = decode_page_body(codec, &encoded, decoded_bytes, prefix, pool)?;
        // Commit only once every fallible operation has succeeded. Page body
        // owners survive this cursor; the temporary encoded owner drops here.
        self.offset = next_offset;
        self.values = next_values;
        self.dictionary = dictionary;
        self.data_seen = data_seen;
        Ok(Some(AdmittedPage {
            header: typed,
            body,
        }))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::Write;
    #[allow(deprecated)]
    fn page(value: &str) -> Vec<u8> {
        use parquet::thrift::TSerializable;
        let mut plain = (value.len() as u32).to_le_bytes().to_vec();
        plain.extend_from_slice(value.as_bytes());
        let compressed = snap::raw::Encoder::new().compress_vec(&plain).unwrap();
        let header = parquet::format::PageHeader::new(
            parquet::format::PageType::DATA_PAGE,
            plain.len() as i32,
            compressed.len() as i32,
            Some(crc32fast::hash(&compressed) as i32),
            Some(parquet::format::DataPageHeader::new(
                1,
                parquet::format::Encoding::PLAIN,
                parquet::format::Encoding::RLE,
                parquet::format::Encoding::RLE,
                None,
            )),
            None,
            None,
            None,
        );
        let mut bytes = Vec::new();
        header
            .write_to_out_protocol(&mut parquet::thrift::TCompactOutputProtocol::new(
                &mut bytes,
            ))
            .unwrap();
        bytes.extend(compressed);
        bytes
    }
    fn cursor(bytes: &[u8], expected: u64) -> AdmittedColumnPages<std::fs::File> {
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(bytes).unwrap();
        AdmittedColumnPages::new(
            file,
            0,
            bytes.len() as u64,
            expected,
            Compression::SNAPPY,
            65536,
        )
        .unwrap()
    }
    #[test]
    fn decoded_page_does_not_retain_consumed_header_window() {
        // Independent pseudo-random ASCII keeps the encoded page large enough
        // that an unnecessary 64KiB header owner would exceed this budget.
        let mut seed = 0x12345678u32;
        let value: String = (0..120000)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                char::from(b'!' + (seed % 90) as u8)
            })
            .collect();
        let bytes = page(&value);
        assert!(bytes.len() > 110000 && bytes.len() < 125000);
        let mut reader = cursor(&bytes, 1);
        let pool = MemoryPool::new(256 * 1024);
        let decoded = reader.next(&pool).unwrap().unwrap();
        assert_eq!(&decoded.body()[..4], &(value.len() as u32).to_le_bytes());
        assert_eq!(&decoded.body()[4..], value.as_bytes());
        assert!(reader.next(&pool).unwrap().is_none());
        drop(reader);
        assert!(pool.used() >= value.len());
        drop(decoded);
        assert_eq!(pool.used(), 0);
        assert!(pool.reserved_peak() <= pool.max());
    }

    #[test]
    fn admission_retry_after_prefix_preserves_cursor_and_page_owner() {
        let first = page("first");
        let mut bytes = first.clone();
        bytes.extend(page("second"));
        let mut cursor = cursor(&bytes, 2);
        let pool = MemoryPool::new(32768);
        let output = cursor.next(&pool).unwrap().unwrap();
        assert!(matches!(
            output.header(),
            TypedPageHeader::DataV1 { values: 1, .. }
        ));
        let prefix = cursor.offset;
        let charge = pool.used();
        assert_eq!(prefix, first.len() as u64);
        let held = pool.allocate(pool.max() - pool.used() - 1).unwrap();
        assert!(cursor.next(&pool).unwrap_err().is_memory_limit());
        assert_eq!(cursor.offset, prefix);
        assert!(!cursor.failed);
        drop(held);
        assert_eq!(pool.used(), charge);
        let second = cursor.next(&pool).unwrap().unwrap();
        assert!(cursor.next(&pool).unwrap().is_none());
        drop(cursor);
        let mut decoder =
            crate::storage::admitted_plain_utf8::PlainUtf8Decoder::new(second.body(), 1, None)
                .unwrap();
        assert_eq!(
            decoder.next(1, 1024, &pool).unwrap().unwrap().value(0),
            "second"
        );
        drop(second);
        drop(output);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn count_mismatch_and_corruption_poison_without_advancing() {
        let bytes = page("value");
        let pool = MemoryPool::new(65536);
        let mut mismatch = cursor(&bytes, 2);
        assert!(mismatch.next(&pool).is_err());
        assert_eq!(mismatch.offset, 0);
        assert!(mismatch
            .next(&pool)
            .unwrap_err()
            .to_string()
            .contains("poisoned"));
        let mut damaged = bytes;
        *damaged.last_mut().unwrap() ^= 1;
        let mut corrupt = cursor(&damaged, 1);
        assert!(corrupt
            .next(&pool)
            .unwrap_err()
            .to_string()
            .contains("CRC mismatch"));
        assert_eq!(corrupt.offset, 0);
        assert!(corrupt.failed);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn refusal_after_encoded_reads_releases_temporaries_before_retry() {
        struct Counted {
            bytes: Vec<u8>,
            reads: std::cell::Cell<usize>,
        }
        impl PageSource for Counted {
            fn len(&self) -> std::io::Result<u64> {
                Ok(self.bytes.len() as u64)
            }
            fn read_at(&self, out: &mut [u8], offset: u64) -> std::io::Result<usize> {
                self.reads.set(self.reads.get() + 1);
                let source = &self.bytes[offset as usize..];
                let n = source.len().min(out.len());
                out[..n].copy_from_slice(&source[..n]);
                Ok(n)
            }
        }
        // The decoded body alone exceeds the remaining budget, so denial does
        // not depend on retaining an already-consumed header read window.
        let bytes = page(&"a".repeat(1024));
        let size = bytes.len() as u64;
        let source = Counted {
            bytes,
            reads: std::cell::Cell::new(0),
        };
        let mut cursor =
            AdmittedColumnPages::new(source, 0, size, 1, Compression::SNAPPY, 65536).unwrap();
        let pool = MemoryPool::new(65536);
        let held = pool.allocate(65536 - 1300).unwrap();
        assert!(cursor.next(&pool).unwrap_err().is_memory_limit());
        assert_eq!(cursor.source.reads.get(), 2);
        assert_eq!(cursor.offset, 0);
        assert_eq!(pool.used(), 65536 - 1300);
        drop(held);
        let output = cursor.next(&pool).unwrap().unwrap();
        assert_eq!(cursor.source.reads.get(), 4);
        assert!(cursor.next(&pool).unwrap().is_none());
        drop(output);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    #[allow(deprecated)]
    fn dictionary_repetition_or_dictionary_after_data_is_rejected() {
        use parquet::thrift::TSerializable;
        let plain = [1u8, 0, 0, 0, b'x'];
        let compressed = snap::raw::Encoder::new().compress_vec(&plain).unwrap();
        let header = parquet::format::PageHeader::new(
            parquet::format::PageType::DICTIONARY_PAGE,
            plain.len() as i32,
            compressed.len() as i32,
            None,
            None,
            None,
            Some(parquet::format::DictionaryPageHeader::new(
                1,
                parquet::format::Encoding::PLAIN,
                None,
            )),
            None,
        );
        let mut dictionary = Vec::new();
        header
            .write_to_out_protocol(&mut parquet::thrift::TCompactOutputProtocol::new(
                &mut dictionary,
            ))
            .unwrap();
        dictionary.extend(compressed);
        for prefix in [dictionary.clone(), page("data")] {
            let start = prefix.len() as u64;
            let mut bytes = prefix;
            bytes.extend_from_slice(&dictionary);
            bytes.extend(page("tail"));
            let mut cursor = cursor(&bytes, 2);
            let pool = MemoryPool::new(65536);
            drop(cursor.next(&pool).unwrap().unwrap());
            assert!(cursor
                .next(&pool)
                .unwrap_err()
                .to_string()
                .contains("dictionary must occur once before data"));
            assert_eq!(cursor.offset, start);
            assert!(cursor.failed);
            assert_eq!(pool.used(), 0);
        }
    }
}
