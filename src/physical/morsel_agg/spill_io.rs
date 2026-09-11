//! Optional query-admitted I/O batching. Logical positions survive read-ahead;
//! pending writes must be flushed explicitly before run publication.
use crate::execution::{reserved_vec::ReservedVec, MemoryPool};
use crate::Result;
use std::io::{self, Read, Seek, SeekFrom, Write};

fn buffer(pool: &MemoryPool) -> Result<Option<ReservedVec<u8>>> {
    let capacity = (pool.available() / 64).min(64 * 1024);
    if capacity < 1024 {
        return Ok(None);
    }
    let mut bytes = match ReservedVec::with_capacity(pool, capacity) {
        Ok(bytes) => bytes,
        Err(e) if e.is_memory_limit() => return Ok(None),
        Err(e) => return Err(e),
    };
    bytes.extend_reserved(capacity, std::iter::repeat_n(0, capacity))?;
    Ok(Some(bytes))
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, "spill I/O position overflow")
}

pub(super) struct Reader<R> {
    inner: R,
    bytes: Option<ReservedVec<u8>>,
    cursor: usize,
    filled: usize,
    position: u64,
}
impl<R: Read + Seek> Reader<R> {
    pub(super) fn new(mut inner: R, pool: &MemoryPool) -> Result<Self> {
        let bytes = buffer(pool)?;
        let position = inner.stream_position()?;
        Ok(Self {
            inner,
            bytes,
            cursor: 0,
            filled: 0,
            position,
        })
    }
    /// The caller supplies a newly opened descriptor at offset zero.
    pub(super) fn reset(&mut self, inner: R) {
        self.inner = inner;
        self.cursor = 0;
        self.filled = 0;
        self.position = 0;
    }
}
impl<R: Read> Read for Reader<R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        if out.is_empty() {
            return Ok(0);
        }
        let n = if let Some(bytes) = &mut self.bytes {
            if self.cursor == self.filled {
                self.cursor = 0;
                // Interrupted refills must not expose the previously consumed bytes.
                self.filled = 0;
                self.filled = self.inner.read(bytes.as_mut_slice())?;
            }
            let n = out.len().min(self.filled - self.cursor);
            out[..n].copy_from_slice(&bytes.as_slice()[self.cursor..self.cursor + n]);
            self.cursor += n;
            n
        } else {
            self.inner.read(out)?
        };
        self.position = self.position.checked_add(n as u64).ok_or_else(invalid)?;
        Ok(n)
    }
}
impl<R: Seek> Seek for Reader<R> {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let target = match from {
            SeekFrom::Start(p) => Some(p),
            SeekFrom::Current(delta) => Some(
                self.position
                    .checked_add_signed(delta)
                    .ok_or_else(invalid)?,
            ),
            SeekFrom::End(_) => None,
        };
        if let Some(target) = target {
            let start = self
                .position
                .checked_sub(self.cursor as u64)
                .ok_or_else(invalid)?;
            if self.bytes.is_some() && target >= start && target - start <= self.filled as u64 {
                self.cursor = (target - start) as usize;
                self.position = target;
                return Ok(target);
            }
        }
        // Invalidate read-ahead before a seek that may change the descriptor.
        self.cursor = 0;
        self.filled = 0;
        let p = self.inner.seek(target.map_or(from, SeekFrom::Start))?;
        self.position = p;
        Ok(p)
    }
    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.position)
    }
}

pub(super) struct Writer<W> {
    inner: W,
    bytes: Option<ReservedVec<u8>>,
    used: usize,
    failed: bool,
}
impl<W: Write> Writer<W> {
    pub(super) fn new(inner: W, pool: &MemoryPool) -> Result<Self> {
        Ok(Self {
            inner,
            bytes: buffer(pool)?,
            used: 0,
            failed: false,
        })
    }
    pub(super) fn inner(&self) -> &W {
        &self.inner
    }
    #[cfg(test)]
    pub(super) fn inner_mut(&mut self) -> &mut W {
        &mut self.inner
    }
    fn ready(&self) -> io::Result<()> {
        if self.failed {
            Err(io::Error::other("spill writer poisoned by I/O failure"))
        } else {
            Ok(())
        }
    }
    fn drain(&mut self) -> io::Result<()> {
        self.ready()?;
        if self.used != 0 {
            // A partial write followed by failure must never replay its prefix.
            self.failed = true;
            self.inner
                .write_all(&self.bytes.as_ref().unwrap().as_slice()[..self.used])?;
            self.used = 0;
            self.failed = false;
        }
        Ok(())
    }
}
impl<W: Write> Write for Writer<W> {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.ready()?;
        let Some(capacity) = self.bytes.as_ref().map(|b| b.as_slice().len()) else {
            let result = self.inner.write(input);
            if result
                .as_ref()
                .is_err_and(|e| e.kind() != io::ErrorKind::Interrupted)
            {
                self.failed = true;
            }
            return result;
        };
        if self.used == capacity {
            self.drain()?;
        }
        let n = input.len().min(capacity - self.used);
        self.bytes.as_mut().unwrap().as_mut_slice()[self.used..self.used + n]
            .copy_from_slice(&input[..n]);
        self.used += n;
        Ok(n)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.drain()?;
        self.failed = true;
        self.inner.flush()?;
        self.failed = false;
        Ok(())
    }
}
// No Drop flush: unpublished or failed runs are discarded by their file owner.

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    struct CountIo {
        data: Cursor<Vec<u8>>,
        reads: usize,
        writes: usize,
        seeks: usize,
    }
    impl Read for CountIo {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
            self.reads += 1;
            self.data.read(b)
        }
    }
    impl Write for CountIo {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.writes += 1;
            self.data.write(b)
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl Seek for CountIo {
        fn seek(&mut self, p: SeekFrom) -> io::Result<u64> {
            self.seeks += 1;
            self.data.seek(p)
        }
    }
    fn counted(data: Vec<u8>) -> CountIo {
        CountIo {
            data: Cursor::new(data),
            reads: 0,
            writes: 0,
            seeks: 0,
        }
    }
    #[test]
    fn small_operations_batch_exactly_and_read_positions_do_not_seek_per_frame() {
        let pool = MemoryPool::new_named("buffered spill", 8 * 1024 * 1024);
        let expected: Vec<u8> = (0..200_003).map(|i| (i % 251) as u8).collect();
        let mut writer = Writer::new(counted(vec![]), &pool).unwrap();
        for part in expected.chunks(7) {
            writer.write_all(part).unwrap();
        }
        writer.flush().unwrap();
        assert_eq!(writer.inner.data.get_ref(), &expected);
        assert!(writer.inner.writes <= 4);
        drop(writer);
        let mut reader = Reader::new(counted(expected.clone()), &pool).unwrap();
        for (offset, part) in expected.chunks(11).enumerate() {
            assert_eq!(reader.stream_position().unwrap(), (offset * 11) as u64);
            let mut actual = vec![0; part.len()];
            reader.read_exact(&mut actual).unwrap();
            assert_eq!(actual, part);
        }
        assert!(reader.inner.reads <= 4);
        assert_eq!(reader.inner.seeks, 1);
        reader.seek(SeekFrom::Start(65530)).unwrap();
        let mut bytes = [0; 25];
        reader.read_exact(&mut bytes).unwrap();
        reader.seek(SeekFrom::Current(-25)).unwrap();
        let mut again = [0; 25];
        reader.read_exact(&mut again).unwrap();
        assert_eq!(bytes, again);
        assert_eq!(bytes, expected[65530..65555]);
        assert_eq!(
            reader.seek(SeekFrom::End(-1)).unwrap(),
            expected.len() as u64 - 1
        );
        let mut last = [0];
        reader.read_exact(&mut last).unwrap();
        assert_eq!(last[0], *expected.last().unwrap());
        drop(reader);
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn unavailable_optional_buffer_uses_exact_unbuffered_io() {
        let pool = MemoryPool::new_named("no buffer", 1);
        let mut writer = Writer::new(counted(vec![]), &pool).unwrap();
        writer.write_all(b"abc").unwrap();
        writer.flush().unwrap();
        assert_eq!(pool.used(), 0);
        assert!(writer.bytes.is_none());
        let mut reader = Reader::new(counted(b"abc".to_vec()), &pool).unwrap();
        let mut b = [0; 3];
        reader.read_exact(&mut b).unwrap();
        assert_eq!(&b, b"abc");
        reader.seek(SeekFrom::Start(1)).unwrap();
        assert_eq!(reader.stream_position().unwrap(), 1);
    }
    struct PartialFailure {
        bytes: Vec<u8>,
        calls: usize,
    }
    impl Write for PartialFailure {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.calls += 1;
            match self.calls {
                1 => Err(io::ErrorKind::Interrupted.into()),
                2 => {
                    let n = b.len().min(3);
                    self.bytes.extend_from_slice(&b[..n]);
                    Ok(n)
                }
                _ => Err(io::Error::other("injected failure")),
            }
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    #[test]
    fn partial_flush_failure_is_terminal_and_never_replays_written_prefix() {
        let pool = MemoryPool::new_named("failed write", 1024 * 1024);
        let mut w = Writer::new(
            PartialFailure {
                bytes: vec![],
                calls: 0,
            },
            &pool,
        )
        .unwrap();
        w.write_all(b"abcdef").unwrap();
        assert!(w.flush().is_err());
        assert_eq!(w.inner.bytes, b"abc");
        let calls = w.inner.calls;
        assert!(w.flush().is_err());
        assert!(w.write_all(b"more").is_err());
        assert_eq!(w.inner.calls, calls);
        drop(w);
        assert_eq!(pool.used(), 0);
    }
}

#[cfg(test)]
mod interrupted_read_tests {
    use super::*;
    use std::io::Cursor;
    struct Interrupted {
        inner: Cursor<Vec<u8>>,
        calls: usize,
    }
    impl Read for Interrupted {
        fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
            self.calls += 1;
            if self.calls % 2 == 0 {
                return Err(io::ErrorKind::Interrupted.into());
            }
            let count = out.len().min(3);
            self.inner.read(&mut out[..count])
        }
    }
    impl Seek for Interrupted {
        fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
            self.inner.seek(from)
        }
    }
    #[test]
    fn interrupted_refill_never_republishes_the_consumed_buffer() {
        let pool = MemoryPool::new_named("interrupted spill read", 1024 * 1024);
        let mut reader = Reader::new(
            Interrupted {
                inner: Cursor::new(b"abcdefghi".to_vec()),
                calls: 0,
            },
            &pool,
        )
        .unwrap();
        let mut actual = [0; 9];
        reader.read_exact(&mut actual).unwrap();
        assert_eq!(&actual, b"abcdefghi");
        assert_eq!(reader.stream_position().unwrap(), 9);
        drop(reader);
        assert_eq!(pool.used(), 0);
    }
}
