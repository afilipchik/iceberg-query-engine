//! Query-local spill run framing. This does not publish or own a file. The
//! lifecycle owner retains the run identity and completed frame count, rejects
//! incomplete files and never releases source state after failed writes.
use super::group_rows::{GroupLayout, GroupRows};
use crate::{execution::reserved_vec::ReservedVec, QueryError, Result};
use std::io::{Read, Seek, SeekFrom, Write};
use uuid::Uuid;

const MAGIC: &[u8; 8] = b"QESPILL1";
const HEADER: usize = 64;
pub(super) const FRAME_OVERHEAD: u64 = (HEADER + 4) as u64;

#[derive(Clone, Copy)]
pub(super) struct RunIdentity {
    layout: Uuid,
    run: Uuid,
}
impl RunIdentity {
    pub(super) fn new(layout: &GroupLayout) -> Self {
        Self {
            layout: layout.identity(),
            run: Uuid::new_v4(),
        }
    }
}

pub(super) struct FrameCursor {
    identity: RunIdentity,
    next: u64,
    poisoned: bool,
}
fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("aggregate spill frame: {message}"))
}

impl FrameCursor {
    pub(super) fn new(identity: RunIdentity) -> Self {
        Self {
            identity,
            next: 0,
            poisoned: false,
        }
    }
    fn ready(&self) -> Result<()> {
        if self.poisoned {
            return Err(invalid("cursor poisoned by prior failure"));
        }
        if self.next == u64::MAX {
            return Err(invalid("frame ordinal exhausted"));
        }
        Ok(())
    }
    pub(super) fn write(
        &mut self,
        groups: &GroupRows,
        row: usize,
        writer: &mut impl Write,
    ) -> Result<()> {
        self.ready()?;
        // These errors occur before any file mutation, so retry is safe.
        if groups.layout_identity() != self.identity.layout {
            return Err(invalid("source layout mismatch"));
        }
        let length = groups.encoded_size(row)?;
        self.write_frame(length, writer, |payload| {
            groups.write_row(row, payload).map(|_| ())
        })
    }

    pub(super) fn copy(&mut self, frame: &FrameScratch, writer: &mut impl Write) -> Result<()> {
        self.ready()?;
        if frame.layout != self.identity.layout {
            return Err(invalid("copied frame layout mismatch"));
        }
        let bytes = frame.payload()?;
        self.write_frame(bytes.len(), writer, |payload| {
            payload.write_all(bytes)?;
            Ok(())
        })
    }

    fn write_frame<W: Write>(
        &mut self,
        length: usize,
        writer: &mut W,
        write_payload: impl FnOnce(&mut CheckedWriter<'_, W>) -> Result<()>,
    ) -> Result<()> {
        let mut header = [0; HEADER];
        header[..8].copy_from_slice(MAGIC);
        header[8..24].copy_from_slice(self.identity.layout.as_bytes());
        header[24..40].copy_from_slice(self.identity.run.as_bytes());
        header[40..48].copy_from_slice(&self.next.to_le_bytes());
        header[48..56].copy_from_slice(&(length as u64).to_le_bytes());
        let crc = crc32fast::hash(&header[..56]);
        header[56..60].copy_from_slice(&crc.to_le_bytes());
        self.poisoned = true;
        writer.write_all(&header)?;
        let mut payload = CheckedWriter {
            writer,
            crc: crc32fast::Hasher::new(),
            count: 0,
        };
        write_payload(&mut payload)?;
        if payload.count != length {
            return Err(invalid("written row length changed"));
        }
        let crc = payload.crc.finalize();
        writer.write_all(&crc.to_le_bytes())?;
        self.next += 1;
        self.poisoned = false;
        Ok(())
    }

    /// Scratch belongs to the same query budget as the consumer. Admission
    /// denial rewinds to the frame start; all other read failures poison the
    /// cursor. No payload is exposed until its checksum verifies.
    pub(super) fn read<'a>(
        &mut self,
        reader: &mut (impl Read + Seek),
        scratch: &'a mut FrameScratch,
    ) -> Result<&'a [u8]> {
        scratch.valid = false;
        self.ready()?;
        if scratch.layout != self.identity.layout {
            return Err(invalid("scratch layout mismatch"));
        }
        self.poisoned = true;
        let start = reader.stream_position()?;
        let mut header = [0; HEADER];
        reader.read_exact(&mut header)?;
        let crc = u32::from_le_bytes(header[56..60].try_into().unwrap());
        if header[..8] != MAGIC[..]
            || header[60..] != [0; 4]
            || crc32fast::hash(&header[..56]) != crc
        {
            return Err(invalid("invalid header"));
        }
        if header[8..24] != *self.identity.layout.as_bytes()
            || header[24..40] != *self.identity.run.as_bytes()
        {
            return Err(invalid("run/layout identity mismatch"));
        }
        if u64::from_le_bytes(header[40..48].try_into().unwrap()) != self.next {
            return Err(invalid("unexpected frame ordinal"));
        }
        let length = usize::try_from(u64::from_le_bytes(header[48..56].try_into().unwrap()))
            .map_err(|_| invalid("frame size overflow"))?;
        if length > isize::MAX as usize {
            return Err(invalid("frame size exceeds addressable memory"));
        }
        scratch.bytes.truncate(0);
        if let Err(error) = scratch.bytes.reserve(length) {
            if error.is_memory_limit() {
                reader.seek(SeekFrom::Start(start))?;
                self.poisoned = false;
            }
            return Err(error);
        }
        scratch
            .bytes
            .extend_reserved(length, std::iter::repeat_n(0, length))?;
        reader.read_exact(scratch.bytes.as_mut_slice())?;
        let mut checksum = [0; 4];
        reader.read_exact(&mut checksum)?;
        if crc32fast::hash(scratch.bytes.as_slice()) != u32::from_le_bytes(checksum) {
            return Err(invalid("payload checksum mismatch"));
        }
        self.next += 1;
        self.poisoned = false;
        scratch.valid = true;
        Ok(scratch.bytes.as_slice())
    }
}

pub(super) struct FrameScratch {
    bytes: ReservedVec<u8>,
    layout: Uuid,
    valid: bool,
}
impl FrameScratch {
    pub(super) fn key_bytes(&self) -> Result<&[u8]> {
        let bytes = self.payload()?;
        if bytes.first() != Some(&1) {
            return Err(invalid("unsupported row payload version"));
        }
        let size = bytes
            .get(1..9)
            .ok_or_else(|| invalid("truncated row key header"))?;
        let size = usize::try_from(u64::from_le_bytes(size.try_into().unwrap()))
            .map_err(|_| invalid("row key length overflow"))?;
        let end = 9usize
            .checked_add(size)
            .ok_or_else(|| invalid("row key length overflow"))?;
        bytes
            .get(9..end)
            .ok_or_else(|| invalid("truncated row key"))
    }
    pub(super) fn new(layout: &GroupLayout, capacity: usize) -> Result<Self> {
        Ok(Self {
            bytes: ReservedVec::with_capacity(layout.pool(), capacity)?,
            layout: layout.identity(),
            valid: false,
        })
    }
    /// Checksum-verified payload; the run reader must also accept its count/EOF
    /// boundary before a merge cursor marks this frame pending.
    pub(super) fn payload(&self) -> Result<&[u8]> {
        if !self.valid {
            return Err(invalid("no verified frame payload"));
        }
        Ok(self.bytes.as_slice())
    }
    pub(super) fn invalidate(&mut self) {
        self.valid = false;
    }
}

struct CheckedWriter<'a, W> {
    writer: &'a mut W,
    crc: crc32fast::Hasher,
    count: usize,
}
impl<W: Write> Write for CheckedWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let written = self.writer.write(bytes)?;
        self.crc.update(&bytes[..written]);
        self.count += written;
        Ok(written)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        execution::MemoryPool,
        planner::{AggregateFunction, ScalarValue},
    };
    use arrow::datatypes::DataType;
    use std::{io::Cursor, sync::Arc};

    fn fixture(pool: &MemoryPool) -> (Arc<GroupLayout>, GroupRows) {
        let layout = GroupLayout::bind(
            pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let mut groups = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        for v in [0, 1] {
            key.encode(&[ScalarValue::Int64(v)]).unwrap();
            groups
                .prepare_update(&key, &mut row, &[ScalarValue::Int64(7)])
                .unwrap()
                .commit();
        }
        (layout, groups)
    }

    #[test]
    fn frame_round_trip_rewinds_admission_denial_and_keeps_query_ownership() {
        let pool = MemoryPool::new_named("frames", 65536);
        let (layout, groups) = fixture(&pool);
        let identity = RunIdentity::new(&layout);
        let mut writer = FrameCursor::new(identity);
        let mut bytes = Vec::new();
        writer.write(&groups, 0, &mut bytes).unwrap();
        writer.write(&groups, 1, &mut bytes).unwrap();
        let mut scratch = FrameScratch::new(&layout, 0).unwrap();
        let mut reader = FrameCursor::new(identity);
        let mut input = Cursor::new(bytes);
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(reader
            .read(&mut input, &mut scratch)
            .unwrap_err()
            .is_memory_limit());
        assert_eq!(input.position(), 0);
        assert_eq!(reader.next, 0);
        assert!(!reader.poisoned);
        drop(pressure);
        let mut restored = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut row = layout.row_workspace().unwrap();
        for index in 0..2 {
            let payload = reader.read(&mut input, &mut scratch).unwrap();
            restored
                .prepare_restore(&mut key, &mut row, payload)
                .unwrap()
                .commit();
            assert_eq!(
                restored.value(index, 0).unwrap().as_ref(),
                &ScalarValue::Int64(1)
            );
        }
        assert_eq!(input.position(), input.get_ref().len() as u64);
        assert_eq!(reader.next, 2);
        assert!(reader.read(&mut input, &mut scratch).is_err());
        assert!(reader.poisoned);
        drop((restored, row, key, scratch, groups, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn every_corruption_and_truncation_is_rejected_and_poisoned() {
        let pool = MemoryPool::new_named("frame corruption", 65536);
        let (layout, groups) = fixture(&pool);
        let identity = RunIdentity::new(&layout);
        let mut wire = Vec::new();
        FrameCursor::new(identity)
            .write(&groups, 0, &mut wire)
            .unwrap();
        let mut scratch = FrameScratch::new(&layout, wire.len()).unwrap();
        let pressure = pool.allocate(pool.available()).unwrap();
        for offset in 0..wire.len() {
            let mut corrupt = wire.clone();
            corrupt[offset] ^= 1;
            let mut reader = FrameCursor::new(identity);
            assert!(reader
                .read(&mut Cursor::new(corrupt), &mut scratch)
                .is_err());
            assert!(reader.poisoned);
            assert!(reader.read(&mut Cursor::new(&wire), &mut scratch).is_err());
            let mut reader = FrameCursor::new(identity);
            assert!(reader
                .read(&mut Cursor::new(&wire[..offset]), &mut scratch)
                .is_err());
            assert!(reader.poisoned);
        }
        drop((pressure, scratch, groups, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn frame_identity_order_and_failed_writer_cannot_be_reused() {
        let pool = MemoryPool::new_named("frame identities", 131072);
        let (layout, groups) = fixture(&pool);
        let identity = RunIdentity::new(&layout);
        let (other_layout, other_groups) = fixture(&pool);
        let mut cursor = FrameCursor::new(identity);
        let mut wire = Vec::new();
        assert!(cursor.write(&other_groups, 0, &mut wire).is_err());
        assert!(wire.is_empty());
        cursor.write(&groups, 0, &mut wire).unwrap();
        let boundary = wire.len();
        cursor.write(&groups, 1, &mut wire).unwrap();
        let mut scratch = FrameScratch::new(&layout, wire.len()).unwrap();
        for wrong in [RunIdentity::new(&layout), RunIdentity::new(&other_layout)] {
            let mut wrong_scratch = if wrong.layout == layout.identity() {
                FrameScratch::new(&layout, wire.len()).unwrap()
            } else {
                FrameScratch::new(&other_layout, wire.len()).unwrap()
            };
            assert!(FrameCursor::new(wrong)
                .read(&mut Cursor::new(&wire), &mut wrong_scratch)
                .is_err());
        }
        assert!(FrameCursor::new(identity)
            .read(&mut Cursor::new(&wire[boundary..]), &mut scratch)
            .is_err());
        let mut reader = FrameCursor::new(identity);
        reader
            .read(&mut Cursor::new(&wire[..boundary]), &mut scratch)
            .unwrap();
        assert!(reader
            .read(&mut Cursor::new(&wire[..boundary]), &mut scratch)
            .is_err());
        for length in 0..boundary {
            let mut output = vec![0; length];
            let mut writer = FrameCursor::new(identity);
            assert!(writer
                .write(&groups, 0, &mut Cursor::new(output.as_mut_slice()))
                .is_err());
            assert!(writer.poisoned);
            assert!(writer.write(&groups, 0, &mut std::io::sink()).is_err());
            assert_eq!(groups.len(), 2);
        }
        drop((scratch, groups, layout, other_groups, other_layout));
        assert_eq!(pool.used(), 0);
    }
}
