//! Conservative preparation admission for immutable benchmark IPC streams.
//! This is an allowance, not an exact RSS measurement or a general IPC decoder.
use query_engine::execution::{MemoryReservation, SharedMemoryPool};
use query_engine::{QueryError, Result};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

fn invalid(message: impl std::fmt::Display) -> QueryError {
    QueryError::Execution(format!("IPC preload admission: {message}"))
}
fn add(a: usize, b: usize) -> Result<usize> {
    a.checked_add(b).ok_or_else(|| invalid("size overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize> {
    a.checked_mul(b).ok_or_else(|| invalid("size overflow"))
}

/// Plan all inputs before allocating any decoded table. Charge bodies twice for
/// read/alignment overlap and reserve conservative metadata/container headroom.
/// Compression and dictionary deltas need their own expansion model and refuse.
pub(super) fn admit<'a>(
    paths: impl Iterator<Item = &'a Path>,
    pool: &SharedMemoryPool,
) -> Result<MemoryReservation> {
    let mut total = 0;
    for path in paths {
        total = add(total, plan_stream(path, pool)?)?;
        // Refuse as soon as the accumulated residency cannot fit, before reading
        // another stream's metadata or constructing an Arrow table.
        if total > pool.available() {
            // A concurrent parent-pool release can make admission succeed
            // after the observation. Never then skip the remaining inputs.
            drop(pool.allocate(total)?);
        }
    }
    pool.allocate(total)
}

fn plan_stream(path: &Path, pool: &SharedMemoryPool) -> Result<usize> {
    let mut file = File::open(path)?;
    let length = file.metadata()?.len();
    let mut total = 4096usize;
    let mut schema = false;
    loop {
        let mut word = [0u8; 4];
        if file.read(&mut word[..1])? == 0 {
            break;
        }
        file.read_exact(&mut word[1..])?;
        let mut size = u32::from_le_bytes(word);
        if size == u32::MAX {
            file.read_exact(&mut word)?;
            size = u32::from_le_bytes(word);
        }
        if size == 0 {
            if file.stream_position()? != length {
                return Err(invalid("trailing stream bytes"));
            }
            break;
        }
        let size = size as usize;
        let end = file
            .stream_position()?
            .checked_add(size as u64)
            .ok_or_else(|| invalid("metadata extent overflow"))?;
        if end > length {
            return Err(invalid("truncated metadata"));
        }
        let metadata_allowance = add(mul(size, 64)?, 4096)?;
        let _scratch = pool.allocate(metadata_allowance)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(size)
            .map_err(|e| invalid(format!("metadata allocation refused: {e}")))?;
        bytes.resize(size, 0);
        file.read_exact(&mut bytes)?;
        let message = arrow::ipc::root_as_message(&bytes).map_err(invalid)?;
        let body = usize::try_from(message.bodyLength())
            .map_err(|_| invalid("negative/oversized body"))?;
        let body_end = end
            .checked_add(body as u64)
            .ok_or_else(|| invalid("body extent overflow"))?;
        if body_end > length {
            return Err(invalid("truncated body"));
        }
        let batch = match message.header_type() {
            arrow::ipc::MessageHeader::Schema if !schema => {
                schema = true;
                None
            }
            arrow::ipc::MessageHeader::RecordBatch if schema => Some(
                message
                    .header_as_record_batch()
                    .ok_or_else(|| invalid("missing record metadata"))?,
            ),
            arrow::ipc::MessageHeader::DictionaryBatch if schema => {
                let dictionary = message
                    .header_as_dictionary_batch()
                    .ok_or_else(|| invalid("missing dictionary metadata"))?;
                if dictionary.isDelta() {
                    return Err(invalid("dictionary delta expansion is not admitted"));
                }
                Some(
                    dictionary
                        .data()
                        .ok_or_else(|| invalid("missing dictionary data metadata"))?,
                )
            }
            _ => return Err(invalid("invalid stream message order/type")),
        };
        let mut frame = add(mul(body, 2)?, metadata_allowance)?;
        if let Some(batch) = batch {
            if batch.compression().is_some() {
                return Err(invalid("compressed IPC expansion is not admitted"));
            }
            let nodes = batch.nodes().map_or(0, |v| v.len());
            let buffers = batch.buffers().map_or(0, |v| v.len());
            frame = add(frame, mul(add(nodes, buffers)?, 1024)?)?;
        }
        total = add(total, frame)?;
        file.seek(SeekFrom::Start(body_end))?;
    }
    if !schema {
        return Err(invalid("missing stream schema"));
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        ipc::writer::StreamWriter,
        record_batch::RecordBatch,
    };
    use std::sync::Arc;

    #[test]
    fn compressed_stream_requires_explicit_expansion_support() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("compressed.arrow");
        let batch = RecordBatch::try_from_iter(vec![(
            "id",
            Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef,
        )])
        .unwrap();
        let options = arrow::ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(arrow::ipc::CompressionType::LZ4_FRAME))
            .unwrap();
        let mut writer = StreamWriter::try_new_with_options(
            File::create(&path).unwrap(),
            &batch.schema(),
            options,
        )
        .unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let pool = Arc::new(query_engine::execution::MemoryPool::new_named(
            "compressed preload",
            4 * 1024 * 1024,
        ));
        let error = admit([path.as_path()].into_iter(), &pool).unwrap_err();
        assert!(error
            .to_string()
            .contains("compressed IPC expansion is not admitted"));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn preparation_admits_all_inputs_and_releases_on_refusal() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("input.arrow");
        let batch = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from(vec![Some("é\0"), None])) as ArrayRef,
            ),
        ])
        .unwrap();
        let mut writer =
            StreamWriter::try_new(File::create(&path).unwrap(), &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
        drop(writer);
        let pool = Arc::new(query_engine::execution::MemoryPool::new_named(
            "preload test",
            4 * 1024 * 1024,
        ));
        let planned = plan_stream(&path, &pool).unwrap();
        assert_eq!(pool.used(), 0);
        let lease = admit([path.as_path(), path.as_path()].into_iter(), &pool).unwrap();
        assert_eq!(pool.used(), planned * 2);
        drop(lease);
        assert_eq!(pool.used(), 0);
        let hold = pool.allocate(pool.available() - planned).unwrap();
        let before = pool.used();
        let error = admit([path.as_path(), path.as_path()].into_iter(), &pool).unwrap_err();
        assert!(error.is_memory_limit());
        assert_eq!(pool.used(), before);
        drop(hold);
        assert_eq!(pool.used(), 0);
        std::fs::write(&path, [255, 255, 255, 255, 255, 255, 255, 127]).unwrap();
        assert!(admit([path.as_path()].into_iter(), &pool).is_err());
        assert_eq!(pool.used(), 0);
    }
}
