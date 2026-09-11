//! Admitted encoded ranges with positional reads. No shared seek cursor and no
//! uncharged growable read buffer. Header validation and snapshot consistency
//! remain the page reader's responsibility.
use crate::{
    execution::{MemoryPool, ReservedBufferBuilder},
    QueryError, Result,
};
use arrow::buffer::Buffer;
use std::io;

pub(crate) trait PageSource {
    fn len(&self) -> io::Result<u64>;
    fn read_at(&self, target: &mut [u8], offset: u64) -> io::Result<usize>;
}

#[cfg(unix)]
impl PageSource for std::fs::File {
    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }
    fn read_at(&self, target: &mut [u8], offset: u64) -> io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(self, target, offset)
    }
}

pub(crate) fn read_page_range(
    source: &impl PageSource,
    offset: u64,
    bytes: usize,
    pool: &MemoryPool,
) -> Result<Buffer> {
    let end = u64::try_from(bytes)
        .ok()
        .and_then(|n| offset.checked_add(n))
        .ok_or_else(|| QueryError::Storage("encoded page extent overflow".into()))?;
    if end > source.len()? {
        return Err(QueryError::Storage(
            "encoded page extent exceeds source".into(),
        ));
    }
    let mut output = ReservedBufferBuilder::<u8>::with_capacity(pool, bytes)?;
    output.extend_reserved(bytes, std::iter::repeat(0))?;
    let mut done = 0;
    while done < bytes {
        match source.read_at(&mut output.as_mut_slice()[done..], offset + done as u64) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "encoded page truncated during read",
                )
                .into())
            }
            Ok(n) if n <= bytes - done => done += n,
            Ok(_) => {
                return Err(QueryError::Storage(
                    "page source returned an invalid read count".into(),
                ))
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(output.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;
    struct Source {
        bytes: Vec<u8>,
        calls: Cell<usize>,
        interrupted: bool,
        fail_after: Option<u64>,
    }
    impl PageSource for Source {
        fn len(&self) -> io::Result<u64> {
            Ok(self.bytes.len() as u64)
        }
        fn read_at(&self, target: &mut [u8], offset: u64) -> io::Result<usize> {
            let call = self.calls.get();
            self.calls.set(call + 1);
            if self.interrupted && call == 0 {
                return Err(io::ErrorKind::Interrupted.into());
            }
            if self.fail_after.is_some_and(|n| offset >= n) {
                return Err(io::Error::new(io::ErrorKind::Other, "source failure"));
            }
            let available = &self.bytes[offset as usize..];
            let n = 2.min(target.len()).min(available.len());
            target[..n].copy_from_slice(&available[..n]);
            Ok(n)
        }
    }
    fn source() -> Source {
        Source {
            bytes: b"0123456789".to_vec(),
            calls: Cell::new(0),
            interrupted: false,
            fail_after: None,
        }
    }

    #[test]
    fn admission_and_extent_errors_precede_payload_reads() {
        let source = source();
        let pool = MemoryPool::new(1);
        assert!(read_page_range(&source, 0, 10, &pool)
            .unwrap_err()
            .is_memory_limit());
        assert!(read_page_range(&source, 8, 3, &pool).is_err());
        assert!(read_page_range(&source, u64::MAX, 2, &pool).is_err());
        assert_eq!(source.calls.get(), 0);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn short_reads_interruptions_and_retained_slices_are_exact() {
        let mut source = source();
        source.interrupted = true;
        let pool = MemoryPool::new(4096);
        let buffer = read_page_range(&source, 2, 6, &pool).unwrap();
        assert_eq!(source.calls.get(), 4);
        assert_eq!(buffer.as_slice(), b"234567");
        let slice = buffer.slice_with_length(1, 3);
        drop(buffer);
        assert!(pool.used() > 0);
        assert_eq!(slice.as_slice(), b"345");
        drop(slice);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn failed_partial_read_releases_all_provisional_storage() {
        let mut source = source();
        source.fail_after = Some(4);
        let pool = MemoryPool::new(4096);
        let error = read_page_range(&source, 0, 10, &pool).unwrap_err();
        assert!(error.to_string().contains("source failure"));
        assert_eq!(source.calls.get(), 3);
        assert_eq!(pool.used(), 0);
        source.fail_after = None;
        let result = read_page_range(&source, 0, 10, &pool).unwrap();
        assert_eq!(result.as_slice(), b"0123456789");
        drop(result);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn truncation_after_extent_check_releases_provisional_storage() {
        struct Truncated(Source);
        impl PageSource for Truncated {
            fn len(&self) -> io::Result<u64> {
                Ok(12)
            }
            fn read_at(&self, target: &mut [u8], offset: u64) -> io::Result<usize> {
                self.0.read_at(target, offset)
            }
        }
        let source = Truncated(source());
        let pool = MemoryPool::new(4096);
        let error = read_page_range(&source, 0, 12, &pool).unwrap_err();
        assert!(matches!(error, QueryError::Io(ref e) if e.kind() == io::ErrorKind::UnexpectedEof));
        assert_eq!(source.0.calls.get(), 6);
        assert_eq!(pool.used(), 0);
    }

    #[cfg(unix)]
    #[test]
    fn file_to_decoded_page_to_strings_retains_separate_owners() {
        use std::io::{Seek, Write};
        let mut file = tempfile::tempfile().unwrap();
        let plain = [3u8, 0, 0, 0, b'o', b'n', b'e', 3, 0, 0, 0, b't', b'w', b'o'];
        let encoded = snap::raw::Encoder::new().compress_vec(&plain).unwrap();
        file.write_all(b"prefix").unwrap();
        file.write_all(&encoded).unwrap();
        file.rewind().unwrap();
        let pool = MemoryPool::new(65536);
        let input = read_page_range(&file, 6, encoded.len(), &pool).unwrap();
        assert_eq!(file.stream_position().unwrap(), 0);
        let encoded_charge = pool.used();
        let decoded = crate::storage::admitted_page_body::decode_page_body(
            parquet::basic::Compression::SNAPPY,
            &input,
            plain.len(),
            0,
            &pool,
        )
        .unwrap();
        assert!(pool.used() > encoded_charge);
        let array = {
            let mut decoder =
                crate::storage::admitted_plain_utf8::PlainUtf8Decoder::new(&decoded, 2, None)
                    .unwrap();
            decoder.next(8, 1024, &pool).unwrap().unwrap()
        };
        drop(input);
        drop(decoded);
        assert!(pool.used() > 0);
        assert_eq!(
            array.iter().collect::<Vec<_>>(),
            vec![Some("one"), Some("two")]
        );
        drop(array);
        assert_eq!(pool.used(), 0);
    }
}
