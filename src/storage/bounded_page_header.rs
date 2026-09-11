//! Allocation-free Compact Protocol page-header envelope parsing. Nested
//! headers are retained as byte ranges for typed validation by the page reader.
use crate::{QueryError, Result};
use std::ops::Range;

fn invalid(message: &str) -> QueryError {
    QueryError::Storage(format!("Parquet page header: {message}"))
}

#[derive(Debug)]
pub(crate) struct PageHeaderEnvelope {
    pub page_type: i32,
    pub compressed_bytes: usize,
    pub decoded_bytes: usize,
    pub crc: Option<u32>,
    pub header_bytes: usize,
    /// Fields5–8: V1 data, index, dictionary, V2 data; ranges include STOP.
    pub subheaders: [Option<Range<usize>>; 4],
}

struct Compact<'a> {
    bytes: &'a [u8],
    cursor: usize,
    capped: bool,
    short_input: bool,
}

#[derive(Debug, PartialEq)]
pub(crate) enum TypedPageHeader {
    DataV1 {
        values: usize,
        encoding: parquet::basic::Encoding,
        definition: parquet::basic::Encoding,
        repetition: parquet::basic::Encoding,
    },
    Dictionary {
        values: usize,
        encoding: parquet::basic::Encoding,
    },
    DataV2 {
        values: usize,
        nulls: usize,
        rows: usize,
        encoding: parquet::basic::Encoding,
        definition_bytes: usize,
        repetition_bytes: usize,
        compressed: bool,
    },
}
fn encoding(value: i32) -> Result<parquet::basic::Encoding> {
    use parquet::basic::Encoding::*;
    Ok(match value {
        0 => PLAIN,
        2 => PLAIN_DICTIONARY,
        3 => RLE,
        4 => BIT_PACKED,
        5 => DELTA_BINARY_PACKED,
        6 => DELTA_LENGTH_BYTE_ARRAY,
        7 => DELTA_BYTE_ARRAY,
        8 => RLE_DICTIONARY,
        9 => BYTE_STREAM_SPLIT,
        _ => {
            return Err(QueryError::NotImplemented(format!(
                "Parquet page encoding {value}"
            )))
        }
    })
}
fn typed_fields(
    bytes: &[u8],
    integers: usize,
    boolean_field: Option<i16>,
) -> Result<([i32; 6], Option<bool>)> {
    let mut input = Compact {
        bytes,
        cursor: 0,
        capped: false,
        short_input: false,
    };
    let mut fields = [None; 6];
    let mut boolean = None;
    let mut previous = 0;
    while let Some((field, kind)) = input.field(&mut previous)? {
        if field >= 1 && field as usize <= integers {
            if kind != 5 {
                return Err(invalid("subheader integer has wrong type"));
            }
            let slot = &mut fields[field as usize - 1];
            if slot.is_some() {
                return Err(invalid("duplicate subheader integer"));
            }
            *slot = Some(input.i32()?);
        } else if Some(field) == boolean_field {
            if boolean.is_some() || !matches!(kind, 1 | 2) {
                return Err(invalid("invalid or duplicate subheader Boolean"));
            }
            boolean = Some(kind == 1);
        } else {
            input.skip(kind, true, 0)?;
        }
    }
    if input.cursor != bytes.len() {
        return Err(invalid("trailing subheader bytes"));
    }
    let mut result = [0; 6];
    for i in 0..integers {
        result[i] = fields[i].ok_or_else(|| invalid("missing required subheader field"))?;
    }
    Ok((result, boolean))
}

impl TypedPageHeader {
    pub(crate) fn decode_layout(
        &self,
        codec: parquet::basic::Compression,
    ) -> (parquet::basic::Compression, usize) {
        match self {
            Self::DataV2 {
                definition_bytes,
                repetition_bytes,
                compressed,
                ..
            } => (
                if *compressed {
                    codec
                } else {
                    parquet::basic::Compression::UNCOMPRESSED
                },
                definition_bytes + repetition_bytes,
            ),
            _ => (codec, 0),
        }
    }
}
impl Compact<'_> {
    fn take(&mut self, count: usize) -> Result<&[u8]> {
        let end = self
            .cursor
            .checked_add(count)
            .ok_or_else(|| invalid("extent overflow"))?;
        let Some(value) = self.bytes.get(self.cursor..end) else {
            self.short_input = true;
            return Err(if self.capped {
                QueryError::NotImplemented(
                    "Parquet page header exceeds configured byte bound".into(),
                )
            } else {
                invalid("truncated field")
            });
        };
        self.cursor = end;
        Ok(value)
    }
    fn byte(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn unsigned(&mut self) -> Result<u64> {
        let mut value = 0;
        for shift in (0..70).step_by(7) {
            let byte = self.byte()?;
            if shift == 63 && byte > 1 {
                return Err(invalid("varint overflow"));
            }
            value |= u64::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(value);
            }
        }
        Err(invalid("varint overflow"))
    }
    fn i32(&mut self) -> Result<i32> {
        let value = u32::try_from(self.unsigned()?).map_err(|_| invalid("i32 overflow"))?;
        Ok(((value >> 1) as i32) ^ -((value & 1) as i32))
    }
    fn field(&mut self, previous: &mut i16) -> Result<Option<(i16, u8)>> {
        let byte = self.byte()?;
        if byte == 0 {
            return Ok(None);
        }
        let kind = byte & 15;
        if !(1..=12).contains(&kind) {
            return Err(invalid("invalid field type"));
        }
        let delta = byte >> 4;
        *previous = if delta == 0 {
            i16::try_from(self.i32()?).map_err(|_| invalid("field id overflow"))?
        } else {
            previous
                .checked_add(i16::from(delta))
                .ok_or_else(|| invalid("field id overflow"))?
        };
        Ok(Some((*previous, kind)))
    }
    fn count(&mut self) -> Result<usize> {
        let count =
            usize::try_from(self.unsigned()?).map_err(|_| invalid("collection size overflow"))?;
        // Every collection element has at least one encoded byte, including
        // a Boolean or empty struct. Refuse impossible counts without looping.
        if count > self.bytes.len() - self.cursor {
            self.short_input = true;
            return Err(if self.capped {
                QueryError::NotImplemented(
                    "Parquet page collection exceeds configured header bound".into(),
                )
            } else {
                invalid("collection size exceeds header extent")
            });
        }
        Ok(count)
    }
    fn skip(&mut self, kind: u8, embedded_bool: bool, depth: usize) -> Result<()> {
        if depth > 32 {
            return Err(QueryError::NotImplemented(
                "Parquet page header nesting exceeds32".into(),
            ));
        }
        match kind {
            1 | 2 => {
                if !embedded_bool && !matches!(self.byte()?, 1 | 2) {
                    return Err(invalid("invalid Boolean"));
                }
            }
            3 => {
                self.take(1)?;
            }
            4 => {
                i16::try_from(self.i32()?).map_err(|_| invalid("i16 overflow"))?;
            }
            5 => {
                self.i32()?;
            }
            6 => {
                self.unsigned()?;
            }
            7 => {
                self.take(8)?;
            }
            8 => {
                let n = usize::try_from(self.unsigned()?)
                    .map_err(|_| invalid("binary size overflow"))?;
                self.take(n)?;
            }
            9 | 10 => {
                let header = self.byte()?;
                let count = if header >> 4 == 15 {
                    self.count()?
                } else {
                    usize::from(header >> 4)
                };
                for _ in 0..count {
                    self.skip(header & 15, false, depth + 1)?;
                }
            }
            11 => {
                let count = self.count()?;
                if count != 0 {
                    let types = self.byte()?;
                    for _ in 0..count {
                        self.skip(types >> 4, false, depth + 1)?;
                        self.skip(types & 15, false, depth + 1)?;
                    }
                }
            }
            12 => {
                let mut previous = 0;
                while let Some((_, kind)) = self.field(&mut previous)? {
                    self.skip(kind, true, depth + 1)?;
                }
            }
            _ => return Err(invalid("invalid collection type")),
        }
        Ok(())
    }
}

/// `chunk_remaining` includes this header and payload. The caller must retain
/// the immutable input bytes while using subheader ranges. No body is decoded.
pub(crate) fn parse_page_header(
    bytes: &[u8],
    max_header_bytes: usize,
    chunk_remaining: u64,
) -> Result<PageHeaderEnvelope> {
    if max_header_bytes == 0 {
        return Err(invalid("positive header bound required"));
    }
    let mut input = Compact {
        bytes: &bytes[..bytes.len().min(max_header_bytes)],
        cursor: 0,
        capped: bytes.len() >= max_header_bytes,
        short_input: false,
    };
    parse_envelope(&mut input, chunk_remaining)
}

fn parse_envelope(input: &mut Compact<'_>, chunk_remaining: u64) -> Result<PageHeaderEnvelope> {
    let mut scalar = [None; 4];
    let mut subheaders = [None, None, None, None];
    let mut previous = 0;
    while let Some((field, kind)) = input.field(&mut previous)? {
        match field {
            1..=4 => {
                if kind != 5 {
                    return Err(invalid("core field is not i32"));
                }
                let slot = &mut scalar[(field - 1) as usize];
                if slot.is_some() {
                    return Err(invalid("duplicate core field"));
                }
                *slot = Some(input.i32()?);
            }
            5..=8 => {
                if kind != 12 {
                    return Err(invalid("page subheader is not a struct"));
                }
                let slot = &mut subheaders[(field - 5) as usize];
                if slot.is_some() {
                    return Err(invalid("duplicate page subheader"));
                }
                let start = input.cursor;
                input.skip(kind, true, 0)?;
                *slot = Some(start..input.cursor);
            }
            _ => input.skip(kind, true, 0)?,
        }
    }
    let page_type = scalar[0].ok_or_else(|| invalid("missing page type"))?;
    let decoded_bytes = usize::try_from(scalar[1].ok_or_else(|| invalid("missing decoded size"))?)
        .map_err(|_| invalid("negative decoded size"))?;
    let compressed_bytes =
        usize::try_from(scalar[2].ok_or_else(|| invalid("missing compressed size"))?)
            .map_err(|_| invalid("negative compressed size"))?;
    let total = input
        .cursor
        .checked_add(compressed_bytes)
        .ok_or_else(|| invalid("page extent overflow"))?;
    if total as u128 > u128::from(chunk_remaining) {
        return Err(invalid("page exceeds column chunk"));
    }
    Ok(PageHeaderEnvelope {
        page_type,
        compressed_bytes,
        decoded_bytes,
        crc: scalar[3].map(|n| n as u32),
        header_bytes: input.cursor,
        subheaders,
    })
}

/// Owns the admitted header window; nested ranges never outlive these bytes.
pub(crate) struct AdmittedPageHeader {
    envelope: PageHeaderEnvelope,
    bytes: arrow::buffer::Buffer,
    body_offset: u64,
}
impl AdmittedPageHeader {
    pub(crate) fn envelope(&self) -> &PageHeaderEnvelope {
        &self.envelope
    }
    pub(crate) fn read(
        source: &impl super::admitted_page_read::PageSource,
        offset: u64,
        chunk_end: u64,
        max_header_bytes: usize,
        pool: &crate::execution::MemoryPool,
    ) -> Result<Self> {
        if chunk_end > source.len()? {
            return Err(invalid("column chunk exceeds source"));
        }
        if max_header_bytes == 0 {
            return Err(invalid("positive header bound required"));
        }
        let remaining = chunk_end
            .checked_sub(offset)
            .ok_or_else(|| invalid("column chunk ends before header"))?;
        let limit = usize::try_from(remaining.min(max_header_bytes as u64))
            .map_err(|_| invalid("header window overflow"))?;
        if limit == 0 {
            return Err(invalid("truncated field"));
        }
        // Bound speculative bytes by current admission. A tiny pool can still
        // admit a small header; no page body or typed output is consumed here.
        let owner_bytes =
            crate::execution::ReservedBufferBuilder::<u8>::initial_allocation_bytes(0)?;
        let mut window = limit
            .min(128)
            .min(pool.available().saturating_sub(owner_bytes))
            .max(1);
        loop {
            let bytes = super::admitted_page_read::read_page_range(source, offset, window, pool)?;
            let mut input = Compact {
                bytes: &bytes,
                cursor: 0,
                capped: window >= max_header_bytes,
                short_input: false,
            };
            match parse_envelope(&mut input, remaining) {
                Ok(envelope) => {
                    let body_offset = offset
                        .checked_add(envelope.header_bytes as u64)
                        .ok_or_else(|| invalid("body offset overflow"))?;
                    return Ok(Self {
                        envelope,
                        bytes,
                        body_offset,
                    });
                }
                Err(_) if input.short_input && window < limit => {}
                Err(error) => return Err(error),
            }
            // Only parser-proven incomplete immutable header prefixes are retried.
            // IO, malformed fields, format bounds and allocation errors terminate.
            // Release the old window before reserving its replacement.
            drop(bytes);
            window = window
                .saturating_mul(2)
                .min(limit)
                .min(pool.available().saturating_sub(owner_bytes))
                .max(window + 1);
        }
    }

    pub(crate) fn subheader(&self, index: usize) -> Option<&[u8]> {
        let range = self.envelope.subheaders.get(index)?.as_ref()?;
        self.bytes.get(range.clone())
    }
    pub(crate) fn typed(&self) -> Result<TypedPageHeader> {
        let index = match self.envelope.page_type {
            0 => 0,
            2 => 2,
            3 => 3,
            other => {
                return Err(QueryError::NotImplemented(format!(
                    "Parquet page type {other}"
                )))
            }
        };
        if self
            .envelope
            .subheaders
            .iter()
            .enumerate()
            .any(|(i, h)| i != index && h.is_some())
        {
            return Err(invalid("page kind conflicts with subheaders"));
        }
        let bytes = self
            .subheader(index)
            .ok_or_else(|| invalid("missing page-kind subheader"))?;
        let nonnegative = |value: i32| {
            usize::try_from(value).map_err(|_| invalid("negative page count or level extent"))
        };
        Ok(match index {
            0 => {
                let (f, _) = typed_fields(bytes, 4, None)?;
                let definition = encoding(f[2])?;
                let repetition = encoding(f[3])?;
                if !matches!(
                    definition,
                    parquet::basic::Encoding::RLE | parquet::basic::Encoding::BIT_PACKED
                ) || !matches!(
                    repetition,
                    parquet::basic::Encoding::RLE | parquet::basic::Encoding::BIT_PACKED
                ) {
                    return Err(invalid("invalid V1 level encoding"));
                }
                TypedPageHeader::DataV1 {
                    values: nonnegative(f[0])?,
                    encoding: encoding(f[1])?,
                    definition,
                    repetition,
                }
            }
            2 => {
                let (f, _) = typed_fields(bytes, 2, Some(3))?;
                TypedPageHeader::Dictionary {
                    values: nonnegative(f[0])?,
                    encoding: encoding(f[1])?,
                }
            }
            3 => {
                let (f, compressed) = typed_fields(bytes, 6, Some(7))?;
                let values = nonnegative(f[0])?;
                let nulls = nonnegative(f[1])?;
                let rows = nonnegative(f[2])?;
                let definition_bytes = nonnegative(f[4])?;
                let repetition_bytes = nonnegative(f[5])?;
                let prefix = definition_bytes
                    .checked_add(repetition_bytes)
                    .ok_or_else(|| invalid("level extent overflow"))?;
                if nulls > values || rows > values {
                    return Err(invalid("V2 counts exceed value count"));
                }
                if prefix > self.envelope.compressed_bytes || prefix > self.envelope.decoded_bytes {
                    return Err(invalid("V2 levels exceed page extent"));
                }
                let compressed = compressed.unwrap_or(true);
                if !compressed && self.envelope.compressed_bytes != self.envelope.decoded_bytes {
                    return Err(invalid("uncompressed V2 sizes differ"));
                }
                TypedPageHeader::DataV2 {
                    values,
                    nulls,
                    rows,
                    encoding: encoding(f[3])?,
                    definition_bytes,
                    repetition_bytes,
                    compressed,
                }
            }
            _ => unreachable!(),
        })
    }
    pub(crate) fn read_body(
        &self,
        source: &impl super::admitted_page_read::PageSource,
        pool: &crate::execution::MemoryPool,
    ) -> Result<arrow::buffer::Buffer> {
        let body = super::admitted_page_read::read_page_range(
            source,
            self.body_offset,
            self.envelope.compressed_bytes,
            pool,
        )?;
        if self
            .envelope
            .crc
            .is_some_and(|expected| crc32fast::hash(&body) != expected)
        {
            return Err(invalid("encoded body CRC mismatch"));
        }
        Ok(body)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[allow(deprecated)]
    fn typed_fixture(header: parquet::format::PageHeader) -> Result<TypedPageHeader> {
        use parquet::thrift::TSerializable;
        let mut bytes = Vec::new();
        header
            .write_to_out_protocol(&mut parquet::thrift::TCompactOutputProtocol::new(
                &mut bytes,
            ))
            .unwrap();
        let envelope = parse_page_header(&bytes, 65536, 1 << 20)?;
        AdmittedPageHeader {
            envelope,
            bytes: arrow::buffer::Buffer::from(bytes),
            body_offset: 0,
        }
        .typed()
    }

    #[test]
    #[allow(deprecated)]
    fn typed_v2_counts_prefix_and_compression_flag_are_checked() {
        use parquet::format::{DataPageHeaderV2, Encoding, PageHeader, PageType};
        let make = |nulls, rows, definition, repetition, compressed| {
            PageHeader::new(
                PageType::DATA_PAGE_V2,
                20,
                20,
                None,
                None,
                None,
                None,
                Some(DataPageHeaderV2::new(
                    7,
                    nulls,
                    rows,
                    Encoding::PLAIN,
                    definition,
                    repetition,
                    compressed,
                    None,
                )),
            )
        };
        let parsed = typed_fixture(make(2, 7, 3, 0, None)).unwrap();
        assert_eq!(
            parsed.decode_layout(parquet::basic::Compression::SNAPPY),
            (parquet::basic::Compression::SNAPPY, 3)
        );
        let parsed = typed_fixture(make(2, 7, 3, 0, Some(false))).unwrap();
        assert_eq!(
            parsed.decode_layout(parquet::basic::Compression::SNAPPY),
            (parquet::basic::Compression::UNCOMPRESSED, 3)
        );
        for header in [
            make(8, 7, 3, 0, None),
            make(-1, 7, 3, 0, None),
            make(2, 8, 3, 0, None),
            make(2, 7, 21, 0, None),
            make(2, 7, 10, 11, None),
        ] {
            assert!(typed_fixture(header).is_err());
        }
        let mut mismatched = make(2, 7, 3, 0, Some(false));
        mismatched.compressed_page_size = 19;
        assert!(typed_fixture(mismatched).is_err());
    }

    #[test]
    #[allow(deprecated)]
    fn typed_kind_conflicts_missing_fields_and_level_encodings_refuse() {
        use parquet::format::{
            DataPageHeader, DictionaryPageHeader, Encoding, PageHeader, PageType,
        };
        let dict = DictionaryPageHeader::new(3, Encoding::PLAIN, None);
        let parsed = typed_fixture(PageHeader::new(
            PageType::DICTIONARY_PAGE,
            20,
            20,
            None,
            None,
            None,
            Some(dict.clone()),
            None,
        ))
        .unwrap();
        assert_eq!(
            parsed,
            TypedPageHeader::Dictionary {
                values: 3,
                encoding: parquet::basic::Encoding::PLAIN
            }
        );
        let data = DataPageHeader::new(3, Encoding::PLAIN, Encoding::RLE, Encoding::RLE, None);
        assert!(typed_fixture(PageHeader::new(
            PageType::DATA_PAGE,
            20,
            20,
            None,
            Some(data.clone()),
            None,
            Some(dict),
            None
        ))
        .is_err());
        assert!(typed_fixture(PageHeader::new(
            PageType::DATA_PAGE,
            20,
            20,
            None,
            None,
            None,
            None,
            None
        ))
        .is_err());
        let mut bad = data.clone();
        bad.definition_level_encoding = Encoding::PLAIN;
        assert!(typed_fixture(PageHeader::new(
            PageType::DATA_PAGE,
            20,
            20,
            None,
            Some(bad),
            None,
            None,
            None
        ))
        .is_err());
        let mut negative = data;
        negative.num_values = -1;
        assert!(typed_fixture(PageHeader::new(
            PageType::DATA_PAGE,
            20,
            20,
            None,
            Some(negative),
            None,
            None,
            None
        ))
        .is_err());
        assert!(typed_fields(&[0], 4, None).is_err());
        assert!(typed_fields(&[0x15, 2, 0x05, 2, 2, 0], 1, None).is_err());
    }
    // Compact fields: page type0, decoded size3, compressed size3, V1 struct
    // containing num_values1, encoding0, def encoding3, rep encoding3.
    const HEADER: &[u8] = &[
        0x15, 0, 0x15, 6, 0x15, 6, 0x2c, 0x15, 2, 0x15, 0, 0x15, 6, 0x15, 6, 0, 0,
    ];
    #[test]
    fn core_extents_and_borrowed_subheaders_are_exact() {
        let header = parse_page_header(HEADER, 1024, 20).unwrap();
        assert_eq!(
            (
                header.page_type,
                header.decoded_bytes,
                header.compressed_bytes,
                header.header_bytes
            ),
            (0, 3, 3, 17)
        );
        assert_eq!(header.crc, None);
        assert_eq!(header.subheaders[0], Some(7..16));
        assert!(parse_page_header(HEADER, 1024, 19).is_err());
    }
    #[test]
    fn malformed_lengths_types_duplicates_and_nesting_refuse() {
        for bytes in [
            &HEADER[..4],
            &[0x15, 0, 0x15, 1, 0x15, 6, 0][..],
            &[0x15, 0, 0x05, 2, 0, 0][..],
            &[0x18, 255, 255, 255, 255, 127][..],
        ] {
            assert!(parse_page_header(bytes, 1024, 100).is_err());
        }
        assert!(matches!(
            parse_page_header(HEADER, 4, 100),
            Err(QueryError::NotImplemented(_))
        ));
        let deep = vec![0x9c; 40];
        assert!(matches!(
            parse_page_header(&deep, 1024, 100),
            Err(QueryError::NotImplemented(_))
        ));
    }
    #[test]
    fn unknown_binary_and_collection_fields_skip_without_materializing() {
        let mut bytes = HEADER[..HEADER.len() - 1].to_vec();
        // field9: binary xyz; field10: Boolean list [true,false].
        bytes.extend_from_slice(&[0x48, 3, b'x', b'y', b'z', 0x19, 0x21, 1, 2, 0]);
        let parsed = parse_page_header(&bytes, 1024, 100).unwrap();
        assert_eq!(parsed.header_bytes, bytes.len());
        let mut bad = HEADER[..HEADER.len() - 1].to_vec();
        bad.extend_from_slice(&[0x49, 0xf5, 255, 255, 255, 255, 15, 0]);
        assert!(parse_page_header(&bad, 1024, 100).is_err());
    }
    #[test]
    fn writer_header_uses_compatible_compact_encoding() {
        use parquet::thrift::TSerializable;
        #[allow(deprecated)]
        let original = parquet::format::PageHeader::new(
            parquet::format::PageType::DATA_PAGE,
            3,
            3,
            Some(-1),
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
        original
            .write_to_out_protocol(&mut parquet::thrift::TCompactOutputProtocol::new(
                &mut bytes,
            ))
            .unwrap();
        let header = parse_page_header(&bytes, 1024, 100).unwrap();
        assert_eq!(header.crc, Some(u32::MAX));
        assert_eq!(header.header_bytes, bytes.len());
        assert!(header.subheaders[0].is_some());
    }

    #[test]
    fn deterministic_malformed_corpus_terminates_without_panicking() {
        for byte in 0..=255u8 {
            let _ = parse_page_header(&[byte], 64, 64);
        }
        let mut state = 0x9e3779b9u32;
        for length in 0..64 {
            for _ in 0..64 {
                let mut bytes = [0u8; 64];
                for byte in &mut bytes[..length] {
                    state ^= state << 13;
                    state ^= state >> 17;
                    state ^= state << 5;
                    *byte = state as u8;
                }
                let _ = parse_page_header(&bytes[..length], 32, 64);
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn actual_parquet_headers_and_decoded_pages_match_library_reader() {
        use arrow::{
            array::StringArray,
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use parquet::file::reader::FileReader;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("pages.parquet");
        let values: Vec<String> = (0..37)
            .map(|i| format!("é-{}-{}", i % 4, "v".repeat(i * 3)))
            .collect();
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![std::sync::Arc::new(StringArray::from(values.clone()))],
        )
        .unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_dictionary_enabled(false)
            .set_compression(parquet::basic::Compression::SNAPPY)
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
        let file = std::fs::File::open(&path).unwrap();
        let oracle = parquet::file::serialized_reader::SerializedFileReader::new(
            std::fs::File::open(&path).unwrap(),
        )
        .unwrap();
        let pool = crate::execution::MemoryPool::new(65536);
        let mut actual = Vec::new();
        for group in 0..oracle.num_row_groups() {
            let column = oracle.metadata().row_group(group).column(0);
            let (mut offset, length) = column.byte_range();
            let end = offset + length;
            let mut owned_pages = crate::storage::admitted_column_pages::AdmittedColumnPages::new(
                std::fs::File::open(&path).unwrap(),
                offset,
                length,
                column.num_values() as u64,
                column.compression(),
                65536,
            )
            .unwrap();
            let row_group = oracle.get_row_group(group).unwrap();
            let mut reference = row_group.get_column_page_reader(0).unwrap();
            while offset < end {
                let header = AdmittedPageHeader::read(&file, offset, end, 65536, &pool).unwrap();
                assert_eq!(header.envelope().page_type, 0);
                let typed = header.typed().unwrap();
                let values = match typed {
                    TypedPageHeader::DataV1 {
                        values,
                        encoding: parquet::basic::Encoding::PLAIN,
                        ..
                    } => values,
                    _ => panic!("fixture must be PLAIN V1"),
                };
                let (codec, prefix) = typed.decode_layout(column.compression());
                let encoded = header.read_body(&file, &pool).unwrap();
                let decoded = crate::storage::admitted_page_body::decode_page_body(
                    codec,
                    &encoded,
                    header.envelope().decoded_bytes,
                    prefix,
                    &pool,
                )
                .unwrap();
                let expected = reference.get_next_page().unwrap().unwrap();
                let owned = owned_pages.next(&pool).unwrap().unwrap();
                assert_eq!(owned.header(), &typed);
                assert_eq!(owned.body().as_slice(), decoded.as_slice());
                assert_eq!(values, expected.num_values() as usize);
                assert_eq!(decoded.as_slice(), expected.buffer().as_ref());
                let mut strings = crate::storage::admitted_plain_utf8::PlainUtf8Decoder::new(
                    &decoded, values, None,
                )
                .unwrap();
                while let Some(array) = strings.next(5, 128, &pool).unwrap() {
                    actual.extend(array.iter().map(|s| s.unwrap().to_owned()));
                }
                offset +=
                    (header.envelope().header_bytes + header.envelope().compressed_bytes) as u64;
            }
            assert!(reference.get_next_page().unwrap().is_none());
            assert!(owned_pages.next(&pool).unwrap().is_none());
            assert_eq!(pool.used(), 0);
        }
        assert_eq!(actual, values);
    }

    #[cfg(unix)]
    #[test]
    fn admitted_header_and_body_validate_crc_and_keep_independent_owners() {
        use parquet::thrift::TSerializable;
        use std::io::{Seek, SeekFrom, Write};
        let body = b"abc";
        #[allow(deprecated)]
        let header = parquet::format::PageHeader::new(
            parquet::format::PageType::DATA_PAGE,
            3,
            3,
            Some(crc32fast::hash(body) as i32),
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
        let mut encoded = Vec::new();
        header
            .write_to_out_protocol(&mut parquet::thrift::TCompactOutputProtocol::new(
                &mut encoded,
            ))
            .unwrap();
        let header_len = encoded.len();
        encoded.extend_from_slice(body);
        let mut file = tempfile::tempfile().unwrap();
        file.write_all(&encoded).unwrap();
        let pool = crate::execution::MemoryPool::new(65536);
        let header = AdmittedPageHeader::read(&file, 0, encoded.len() as u64, 1024, &pool).unwrap();
        assert_eq!(header.envelope().header_bytes, header_len);
        assert!(header.subheader(0).is_some());
        assert!(header.subheader(3).is_none());
        let header_charge = pool.used();
        let output = header.read_body(&file, &pool).unwrap();
        assert_eq!(output.as_slice(), body);
        assert!(pool.used() > header_charge);
        drop(output);
        assert_eq!(pool.used(), header_charge);
        file.seek(SeekFrom::Start(header_len as u64)).unwrap();
        file.write_all(b"xbc").unwrap();
        assert!(header
            .read_body(&file, &pool)
            .unwrap_err()
            .to_string()
            .contains("CRC mismatch"));
        assert_eq!(pool.used(), header_charge);
        drop(header);
        assert_eq!(pool.used(), 0);
    }
}

#[cfg(test)]
mod incremental_tests;
