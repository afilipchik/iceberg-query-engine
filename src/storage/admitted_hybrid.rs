//! Bounded Parquet RLE/bit-packed hybrid values with admitted output chunks.
//! Used for level values and dictionary IDs; this is not legacy BIT_PACKED.
use crate::{
    execution::{MemoryPool, ReservedBufferBuilder},
    QueryError, Result,
};
use arrow::{
    array::UInt32Array,
    buffer::{Buffer, ScalarBuffer},
};
#[derive(Clone)]
enum HybridInput<'a> {
    Borrowed(&'a [u8]),
    Owned(Buffer),
}
impl std::ops::Deref for HybridInput<'_> {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        match self {
            Self::Borrowed(bytes) => bytes,
            Self::Owned(buffer) => buffer.as_slice(),
        }
    }
}
fn invalid(message: &str) -> QueryError {
    QueryError::Storage(format!("Parquet hybrid stream: {message}"))
}
#[derive(Clone, Copy)]
enum Run {
    Empty,
    Repeated {
        left: usize,
        value: u32,
    },
    Packed {
        start: usize,
        left: usize,
        index: usize,
    },
}
#[derive(Clone)]
pub(crate) struct HybridDecoder<'a> {
    data: HybridInput<'a>,
    width: usize,
    remaining: usize,
    offset: usize,
    run: Run,
    failed: bool,
}
impl<'a> HybridDecoder<'a> {
    /// Flat optional definition levels (maximum definition level1). Required
    /// columns do not have a level stream and must not use this conversion.
    pub(crate) fn next_validity(
        &mut self,
        max_values: usize,
        pool: &MemoryPool,
    ) -> Result<Option<arrow::buffer::NullBuffer>> {
        if self.failed {
            return Err(invalid("decoder is poisoned"));
        }
        if self.width != 1 || max_values == 0 {
            return Err(invalid("validity requires width1 and a positive quantum"));
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        let count = self.remaining.min(max_values);
        let bytes = count.div_ceil(8);
        let mut bitmap = ReservedBufferBuilder::<u8>::with_capacity(pool, bytes)?;
        bitmap.extend_reserved(bytes, std::iter::repeat(0))?;
        let mut working = self.clone();
        let mut row = 0;
        while row < count {
            let value = match working.value() {
                Ok(value @ (0 | 1)) => value,
                Ok(_) => {
                    self.failed = true;
                    return Err(invalid("definition level exceeds1"));
                }
                Err(error) => {
                    self.failed = true;
                    return Err(error);
                }
            };
            // value() has validated this run's header, extent and domain.
            // Consume only its repeated suffix within the current output window.
            let extra = match working.run {
                Run::Repeated { left, .. } => left.min(count - row - 1),
                _ => 0,
            };
            let end = row + 1 + extra;
            if value == 1 && extra == 0 {
                bitmap.as_mut_slice()[row / 8] |= 1 << (row % 8);
            } else if value == 1 {
                let first = row / 8;
                let last = (end - 1) / 8;
                let first_mask = u8::MAX << (row % 8);
                let last_mask = u8::MAX >> ((8 - end % 8) % 8);
                let bytes = bitmap.as_mut_slice();
                if first == last {
                    bytes[first] |= first_mask & last_mask;
                } else {
                    bytes[first] |= first_mask;
                    bytes[first + 1..last].fill(u8::MAX);
                    bytes[last] |= last_mask;
                }
            }
            if extra != 0 {
                let Run::Repeated { left, value } = working.run else {
                    unreachable!("only a validated repeated run has a bulk suffix")
                };
                working.run = if extra == left {
                    Run::Empty
                } else {
                    Run::Repeated {
                        left: left - extra,
                        value,
                    }
                };
                working.remaining -= extra;
            }
            row = end;
        }
        if working.remaining == 0 && working.offset != working.data.len() {
            self.failed = true;
            return Err(invalid("trailing encoded bytes"));
        }
        let result = arrow::buffer::NullBuffer::new(arrow::buffer::BooleanBuffer::new(
            bitmap.finish(),
            0,
            count,
        ));
        *self = working;
        Ok(Some(result))
    }
    pub(crate) fn new(data: &'a [u8], width: u8, values: usize) -> Result<Self> {
        Self::with_input(HybridInput::Borrowed(data), width, values)
    }
    /// Retain the encoded page owner, including its memory reservation.
    pub(crate) fn from_owned(data: Buffer, width: u8, values: usize) -> Result<Self> {
        Self::with_input(HybridInput::Owned(data), width, values)
    }
    fn with_input(data: HybridInput<'a>, width: u8, values: usize) -> Result<Self> {
        if width > 32 {
            return Err(invalid("bit width exceeds32"));
        }
        if values == 0 && !data.is_empty() {
            return Err(invalid("encoded bytes for zero values"));
        }
        Ok(Self {
            data,
            width: width as usize,
            remaining: values,
            offset: 0,
            run: Run::Empty,
            failed: false,
        })
    }
    fn take(&mut self, bytes: usize) -> Result<&[u8]> {
        let end = self
            .offset
            .checked_add(bytes)
            .ok_or_else(|| invalid("extent overflow"))?;
        let result = self
            .data
            .get(self.offset..end)
            .ok_or_else(|| invalid("truncated run"))?;
        self.offset = end;
        Ok(result)
    }
    fn header(&mut self) -> Result<u32> {
        let mut result = 0;
        for shift in (0..35).step_by(7) {
            let byte = self.take(1)?[0];
            if shift == 28 && byte > 15 {
                return Err(invalid("run length overflow"));
            }
            result |= u32::from(byte & 127) << shift;
            if byte & 128 == 0 {
                return Ok(result);
            }
        }
        Err(invalid("run length overflow"))
    }
    fn value(&mut self) -> Result<u32> {
        if matches!(self.run, Run::Empty) {
            let header = self.header()?;
            if header < 2 {
                return Err(invalid("zero-length run"));
            }
            if header & 1 == 0 {
                let count = (header >> 1) as usize;
                if count > self.remaining {
                    return Err(invalid("repeated run exceeds value count"));
                }
                let mut value = 0u32;
                for (i, byte) in self.take(self.width.div_ceil(8))?.iter().enumerate() {
                    value |= u32::from(*byte) << (i * 8);
                }
                if self.width < 32 && value >> self.width != 0 {
                    return Err(invalid("repeated value exceeds bit width"));
                }
                self.run = Run::Repeated { left: count, value };
            } else {
                let groups = (header >> 1) as usize;
                let count = groups
                    .checked_mul(8)
                    .ok_or_else(|| invalid("packed count overflow"))?;
                if count > i32::MAX as usize {
                    return Err(invalid("packed run exceeds format length limit"));
                }
                // Writers can pad a final full block (e.g. 256 values), not
                // merely its last group of eight. The caller's page value count
                // bounds logical output. Check encoded extents below and trailing
                // bytes at completion; do not interpret padding as logical IDs.
                let full = groups
                    .checked_mul(self.width)
                    .ok_or_else(|| invalid("packed extent overflow"))?;
                let available = self.data.len() - self.offset;
                let bytes = if full > available && count >= self.remaining {
                    // Some writers omit unused bytes of the final group. Accept
                    // only the exact bytes containing all declared value bits.
                    let needed = self
                        .remaining
                        .checked_mul(self.width)
                        .ok_or_else(|| invalid("packed bit extent overflow"))?
                        .div_ceil(8);
                    if available != needed {
                        return Err(invalid("truncated packed values"));
                    }
                    needed
                } else {
                    full
                };
                let start = self.offset;
                self.take(bytes)?;
                self.run = Run::Packed {
                    start,
                    left: count,
                    index: 0,
                };
            }
        }
        let (value, next) = match self.run {
            Run::Repeated { left, value } => (
                value,
                if left == 1 {
                    Run::Empty
                } else {
                    Run::Repeated {
                        left: left - 1,
                        value,
                    }
                },
            ),
            Run::Packed { start, left, index } => {
                let bit = index
                    .checked_mul(self.width)
                    .ok_or_else(|| invalid("packed bit offset overflow"))?;
                let mut value = 0;
                for i in 0..self.width {
                    let position = bit
                        .checked_add(i)
                        .ok_or_else(|| invalid("packed bit offset overflow"))?;
                    let byte = *self
                        .data
                        .get(start + position / 8)
                        .ok_or_else(|| invalid("truncated packed value"))?;
                    value |= u32::from((byte >> (position % 8)) & 1) << i;
                }
                (
                    value,
                    if left == 1 {
                        Run::Empty
                    } else {
                        Run::Packed {
                            start,
                            left: left - 1,
                            index: index + 1,
                        }
                    },
                )
            }
            Run::Empty => unreachable!(),
        };
        self.run = next;
        self.remaining -= 1;
        Ok(value)
    }
    pub(crate) fn next(
        &mut self,
        max_values: usize,
        max_value: u32,
        pool: &MemoryPool,
    ) -> Result<Option<UInt32Array>> {
        if self.failed {
            return Err(invalid("decoder is poisoned"));
        }
        if max_values == 0 {
            return Err(invalid("positive output quantum required"));
        }
        if self.remaining == 0 {
            return Ok(None);
        }
        let count = self.remaining.min(max_values);
        let mut output = ReservedBufferBuilder::<u32>::with_capacity(pool, count)?;
        let mut working = self.clone();
        let result = output.try_extend_reserved(
            count,
            (0..count).map(|_| {
                let value = working.value()?;
                if value > max_value {
                    return Err(invalid("value exceeds declared domain"));
                }
                Ok(value)
            }),
        );
        if let Err(error) = result {
            self.failed = true;
            return Err(error);
        }
        if working.remaining == 0 && working.offset != working.data.len() {
            self.failed = true;
            return Err(invalid("trailing encoded bytes"));
        }
        let array = UInt32Array::new(ScalarBuffer::new(output.finish(), 0, count), None);
        *self = working;
        Ok(Some(array))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn owned_page_survives_caller_and_cursor_chunks() {
        let pool = MemoryPool::new(8192);
        let mut encoded = ReservedBufferBuilder::<u8>::with_capacity(&pool, 2).unwrap();
        encoded.extend_reserved(2, [3, 0b01010101]).unwrap();
        let buffer = encoded.finish();
        let charge = pool.used();
        let mut decoder = HybridDecoder::from_owned(buffer.clone(), 1, 8).unwrap();
        drop(buffer);
        let first = decoder.next(3, 1, &pool).unwrap().unwrap();
        assert_eq!(first.values().as_ref(), &[1, 0, 1]);
        drop(first);
        assert_eq!(pool.used(), charge);
        let rest = decoder.next(5, 1, &pool).unwrap().unwrap();
        assert_eq!(rest.values().as_ref(), &[0, 1, 0, 1, 0]);
        drop(decoder);
        assert!(pool.used() > 0);
        drop(rest);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn validity_bitmap_retry_and_slice_ownership_are_exact() {
        let pool = MemoryPool::new(8192);
        let mut decoder = HybridDecoder::new(&[3, 0b01010101], 1, 8).unwrap();
        let first = decoder.next_validity(3, &pool).unwrap().unwrap();
        assert_eq!(first.iter().collect::<Vec<_>>(), vec![true, false, true]);
        drop(first);
        let held = pool.allocate(8191).unwrap();
        assert!(decoder
            .next_validity(5, &pool)
            .unwrap_err()
            .is_memory_limit());
        assert_eq!(decoder.remaining, 5);
        drop(held);
        let mask = decoder.next_validity(5, &pool).unwrap().unwrap();
        assert_eq!(
            mask.iter().collect::<Vec<_>>(),
            vec![false, true, false, true, false]
        );
        let slice = mask.slice(1, 3);
        drop(mask);
        assert!(pool.used() > 0);
        assert_eq!(slice.iter().collect::<Vec<_>>(), vec![true, false, true]);
        drop(slice);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn validity_repeated_spans_cross_bitmap_and_output_boundaries() {
        for first in 1..=17 {
            for middle in [1, 7, 8, 9, 63, 64, 65, 1025] {
                let lengths = [first, middle, 11];
                let mut encoded = Vec::new();
                let mut expected = Vec::new();
                for (count, valid) in lengths.into_iter().zip([true, false, true]) {
                    let mut header = count << 1;
                    while header >= 128 {
                        encoded.push((header as u8 & 127) | 128);
                        header >>= 7;
                    }
                    encoded.push(header as u8);
                    encoded.push(u8::from(valid));
                    expected.extend(std::iter::repeat_n(valid, count));
                }
                // Follow the repeated runs with a packed eight-value run.
                encoded.extend([3, 0b10100101]);
                expected.extend([true, false, true, false, false, true, false, true]);
                for quantum in [1, 3, 7, 8, 9, 31, 1024] {
                    let pool = MemoryPool::new(8192);
                    let mut decoder = HybridDecoder::new(&encoded, 1, expected.len()).unwrap();
                    let mut actual = Vec::new();
                    while let Some(mask) = decoder.next_validity(quantum, &pool).unwrap() {
                        actual.extend(mask.iter());
                    }
                    assert_eq!(
                        actual, expected,
                        "first={first} middle={middle} quantum={quantum}"
                    );
                    assert_eq!(pool.used(), 0);
                }
            }
        }
    }

    #[test]
    fn validity_bulk_prefix_errors_are_terminal_and_unpublished() {
        for bytes in [&[6, 1, 2, 2][..], &[6, 1, 4, 1][..], &[6, 1, 3][..]] {
            for prefix in [0, 2] {
                let pool = MemoryPool::new(8192);
                let mut decoder = HybridDecoder::new(bytes, 1, 4).unwrap();
                let held = if prefix == 0 {
                    None
                } else {
                    Some(decoder.next_validity(prefix, &pool).unwrap().unwrap())
                };
                let before = pool.used();
                assert!(decoder.next_validity(8, &pool).is_err());
                assert!(decoder.failed);
                assert_eq!(decoder.remaining, 4 - prefix);
                assert_eq!(pool.used(), before);
                assert!(decoder.next_validity(1, &pool).is_err());
                drop(held);
                assert_eq!(pool.used(), 0);
            }
        }
        let pool = MemoryPool::new(8192);
        let mut decoder = HybridDecoder::new(&[6, 1, 0], 1, 3).unwrap();
        assert!(decoder.next_validity(8, &pool).is_err());
        assert!(decoder.failed);
        assert_eq!(decoder.remaining, 3);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn final_packed_block_padding_does_not_extend_logical_values() {
        // Full 256-value packed block, with only three logical IDs. Padding may
        // contain stale/out-of-domain IDs; the page count defines logical values.
        let mut bytes = vec![65];
        bytes.extend([0xff; 96]);
        bytes[1] = 0x88;
        bytes[2] = 0xfe;
        let pool = MemoryPool::new(8192);
        let mut decoder = HybridDecoder::new(&bytes, 3, 3).unwrap();
        let first = decoder.next(2, 2, &pool).unwrap().unwrap();
        let last = decoder.next(2, 2, &pool).unwrap().unwrap();
        assert_eq!(first.values().as_ref(), [0, 1]);
        assert_eq!(last.values().as_ref(), [2]);
        assert!(decoder.next(2, 2, &pool).unwrap().is_none());
        drop((first, last));
        assert_eq!(pool.used(), 0);

        let mut levels = vec![65];
        levels.extend([0xff; 32]);
        levels[1] = 0xed;
        let mut decoder = HybridDecoder::new(&levels, 1, 5).unwrap();
        let validity = decoder.next_validity(8, &pool).unwrap().unwrap();
        assert_eq!(
            validity.iter().collect::<Vec<_>>(),
            [true, false, true, true, false]
        );
        drop(validity);
        assert_eq!(pool.used(), 0);

        bytes.push(0); // Bytes outside the declared run remain corruption.
        let mut decoder = HybridDecoder::new(&bytes, 3, 3).unwrap();
        assert!(decoder.next(8, 2, &pool).is_err());
        assert!(decoder.failed);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn mixed_runs_cross_chunks_and_final_padding_is_not_a_value() {
        // Three repeated5 values, then packed0..7 (width3).
        let bytes = [6, 5, 3, 0x88, 0xc6, 0xfa];
        let pool = MemoryPool::new(8192);
        let mut decoder = HybridDecoder::new(&bytes, 3, 11).unwrap();
        let mut actual = Vec::new();
        while let Some(array) = decoder.next(2, 7, &pool).unwrap() {
            actual.extend_from_slice(array.values());
        }
        assert_eq!(actual, [5, 5, 5, 0, 1, 2, 3, 4, 5, 6, 7]);
        assert_eq!(pool.used(), 0);
        for bytes in [&[3, 0x88, 0xc6, 0xfa][..], &[3, 0x88, 0x06][..]] {
            let mut decoder = HybridDecoder::new(bytes, 3, 3).unwrap();
            assert_eq!(
                decoder
                    .next(8, 2, &pool)
                    .unwrap()
                    .unwrap()
                    .values()
                    .as_ref(),
                [0, 1, 2]
            );
        }
    }
    #[test]
    fn refusal_preserves_consumed_prefix_and_slices_hold_charges() {
        let pool = MemoryPool::new(8192);
        let mut decoder = HybridDecoder::new(&[20, 1], 1, 10).unwrap();
        let first = decoder.next(2, 1, &pool).unwrap().unwrap();
        drop(first);
        let held = pool.allocate(8191).unwrap();
        assert!(decoder.next(4, 1, &pool).unwrap_err().is_memory_limit());
        assert_eq!(decoder.remaining, 8);
        drop(held);
        let output = decoder.next(8, 1, &pool).unwrap().unwrap();
        let slice = output.slice(1, 2);
        drop(output);
        assert!(pool.used() > 0);
        assert_eq!(slice.values().as_ref(), [1, 1]);
        drop(slice);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn packed_all_widths_offsets_chunks_and_domain_checks_are_exact() {
        for width in 0..=32u8 {
            let max = ((1u64 << width) - 1) as u32;
            let expected: Vec<u32> = (0..131u32)
                .map(|i| match i {
                    0 => 0,
                    1 => max,
                    _ => i.wrapping_mul(2654435761) & max,
                })
                .collect();
            let groups = expected.len().div_ceil(8);
            let mut header = (groups << 1) | 1;
            let mut prefix = Vec::new();
            while header >= 128 {
                prefix.push((header as u8 & 127) | 128);
                header >>= 7;
            }
            prefix.push(header as u8);
            // Independent bit-at-a-time writer; final physical values are padding.
            let mut payload = vec![0u8; groups * width as usize];
            for (row, value) in expected.iter().enumerate() {
                for bit in 0..width as usize {
                    let offset = row * width as usize + bit;
                    payload[offset / 8] |= (((value >> bit) & 1) as u8) << (offset % 8);
                }
            }
            for trimmed in [false, true] {
                let mut encoded = prefix.clone();
                let bytes = if trimmed {
                    (expected.len() * width as usize).div_ceil(8)
                } else {
                    payload.len()
                };
                encoded.extend_from_slice(&payload[..bytes]);
                for quantum in [1, 2, 7, 8, 9, 31, 128] {
                    let pool = MemoryPool::new(8192);
                    let mut decoder = HybridDecoder::new(&encoded, width, expected.len()).unwrap();
                    let mut actual = Vec::new();
                    while let Some(array) = decoder.next(quantum, max, &pool).unwrap() {
                        actual.extend_from_slice(array.values());
                    }
                    assert_eq!(
                        actual, expected,
                        "width={width}, trimmed={trimmed}, quantum={quantum}"
                    );
                    assert_eq!(pool.used(), 0);
                }
                if max > 0 {
                    let pool = MemoryPool::new(8192);
                    let mut decoder = HybridDecoder::new(&encoded, width, expected.len()).unwrap();
                    assert!(decoder.next(131, max - 1, &pool).is_err());
                    assert!(decoder.failed);
                    assert_eq!(decoder.remaining, expected.len());
                    assert_eq!(pool.used(), 0);
                }
            }
        }
    }

    #[test]
    fn zero_width_and_u32_width_have_exact_values() {
        let pool = MemoryPool::new(8192);
        let mut zero = HybridDecoder::new(&[6], 0, 3).unwrap();
        assert_eq!(
            zero.next(8, 0, &pool).unwrap().unwrap().values().as_ref(),
            [0, 0, 0]
        );
        let mut wide = HybridDecoder::new(&[2, 255, 255, 255, 255], 32, 1).unwrap();
        assert_eq!(
            wide.next(1, u32::MAX, &pool).unwrap().unwrap().value(0),
            u32::MAX
        );
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn malformed_runs_and_domain_errors_poison_without_leaks() {
        let pool = MemoryPool::new(8192);
        for (bytes, width, count, max) in [
            (&[0][..], 1, 1, 1),
            (&[4, 1][..], 1, 1, 1),
            (&[2, 2][..], 1, 1, 1),
            (&[3][..], 3, 3, 7),
            (&[2, 3][..], 2, 1, 2),
            (&[2, 1, 0][..], 1, 1, 1),
        ] {
            let mut decoder = HybridDecoder::new(bytes, width, count).unwrap();
            assert!(decoder.next(8, max, &pool).is_err());
            assert!(decoder.failed);
            assert_eq!(pool.used(), 0);
        }
    }

    #[cfg(unix)]
    #[test]
    fn writer_dictionary_ids_and_nullable_levels_reconstruct_exact_strings() {
        use crate::storage::{
            admitted_column_pages::AdmittedColumnPages, admitted_plain_utf8::PlainUtf8Decoder,
            bounded_page_header::TypedPageHeader,
        };
        use arrow::{
            array::{Array, StringArray},
            datatypes::{DataType, Field, Schema},
            record_batch::RecordBatch,
        };
        use parquet::file::reader::FileReader;
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("dictionary.parquet");
        let expected: Vec<Option<String>> = (0..257)
            .map(|i| {
                if i >= 90 && i % 7 == 0 {
                    None
                } else if i < 33 {
                    Some("same".into())
                } else {
                    Some(format!("é-{}", i % 11))
                }
            })
            .collect();
        let schema = std::sync::Arc::new(Schema::new(vec![Field::new("v", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![std::sync::Arc::new(StringArray::from(expected.clone()))],
        )
        .unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_dictionary_enabled(true)
            .set_compression(parquet::basic::Compression::SNAPPY)
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
        let mut dictionary_pages = 0;
        let mut data_pages = 0;
        for group in 0..metadata.num_row_groups() {
            let column = metadata.metadata().row_group(group).column(0);
            let (start, length) = column.byte_range();
            let mut pages = AdmittedColumnPages::new(
                std::fs::File::open(&path).unwrap(),
                start,
                length,
                column.num_values() as u64,
                column.compression(),
                65536,
            )
            .unwrap();
            let mut dictionary = None;
            while let Some(page) = pages.next(&pool).unwrap() {
                match page.header() {
                    TypedPageHeader::Dictionary {
                        values,
                        encoding: parquet::basic::Encoding::PLAIN,
                    } => {
                        dictionary_pages += 1;
                        let mut decoder =
                            PlainUtf8Decoder::new(page.body(), *values, None).unwrap();
                        dictionary = decoder.next(*values, usize::MAX, &pool).unwrap();
                        assert!(decoder.next(1, 1, &pool).unwrap().is_none());
                    }
                    TypedPageHeader::DataV1 {
                        values,
                        encoding: parquet::basic::Encoding::RLE_DICTIONARY,
                        definition: parquet::basic::Encoding::RLE,
                        ..
                    } => {
                        data_pages += 1;
                        let body = page.body();
                        let level_bytes =
                            u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
                        let mut levels =
                            HybridDecoder::new(&body[4..4 + level_bytes], 1, *values).unwrap();
                        let validity = levels.next_validity(*values, &pool).unwrap().unwrap();
                        assert!(levels.next_validity(1, &pool).unwrap().is_none());
                        let dict = dictionary.as_ref().unwrap();
                        let count = validity.len() - validity.null_count();
                        let encoded = &body[4 + level_bytes..];
                        let mut ids = HybridDecoder::new(&encoded[1..], encoded[0], count).unwrap();
                        let indices = ids
                            .next(count, (dict.len() - 1) as u32, &pool)
                            .unwrap()
                            .unwrap();
                        assert!(ids
                            .next(1, (dict.len() - 1) as u32, &pool)
                            .unwrap()
                            .is_none());
                        let mut expand =
                            crate::storage::admitted_dictionary_utf8::DictionaryUtf8Decoder::new(
                                dict,
                                &indices,
                                Some(&validity),
                            )
                            .unwrap();
                        while let Some(output) = expand.next(13, 33, &pool).unwrap() {
                            actual.extend(output.iter().map(|v| v.map(str::to_owned)));
                        }
                    }
                    other => panic!("expected nullable dictionary V1 fixture, got {other:?}"),
                }
            }
            drop(dictionary);
            assert_eq!(pool.used(), 0);
        }
        assert_eq!(dictionary_pages, 3);
        assert!(data_pages >= 3);
        assert_eq!(actual, expected);
    }
}
