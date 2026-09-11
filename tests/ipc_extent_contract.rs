//! Malformed IPC extents must return errors, without unwinding the query reader.
use arrow::array::{Array, ArrayRef, DictionaryArray, Int64Array, Int8Array, StringArray};
use arrow::datatypes::{DataType, Int8Type};
use arrow::ipc::{reader::read_footer_length, writer::FileWriter, Block};
use arrow::record_batch::RecordBatch;
use query_engine::storage::ipc_cache::read_row_group;
use std::sync::Arc;

fn valid_file() -> Vec<u8> {
    let ids: ArrayRef = Arc::new(Int64Array::from(vec![Some(-7), None, Some(9)]));
    let labels: ArrayRef = Arc::new(
        DictionaryArray::<Int8Type>::try_new(
            Int8Array::from(vec![Some(0), None, Some(1)]),
            Arc::new(StringArray::from(vec!["alpha", "beta"])),
        )
        .unwrap(),
    );
    let batch = RecordBatch::try_from_iter([("id", ids), ("label", labels)]).unwrap();
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut bytes, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    bytes
}

fn assert_clean_errors(cases: Vec<(String, Vec<u8>)>) {
    let mut failures = Vec::new();
    for (name, bytes) in cases {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("rg_00000.arrow"), bytes).unwrap();
        let result = std::panic::catch_unwind(|| read_row_group(dir.path(), 0, None, None));
        match result {
            Ok(Err(error)) if error.to_string().contains("rg_00000.arrow") => (),
            Ok(Err(error)) => failures.push(format!("{name}: unnamed error: {error}")),
            Ok(Ok(_)) => failures.push(format!("{name}: accepted malformed file")),
            Err(_) => failures.push(format!("{name}: reader panicked")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[test]
fn footer_extent_errors_do_not_unwind() {
    let original = valid_file();
    let mut cases = Vec::new();
    for length in [i32::MAX, original.len() as i32, -1] {
        let mut bytes = original.clone();
        let trailer = bytes.len() - 10;
        bytes[trailer..trailer + 4].copy_from_slice(&length.to_le_bytes());
        cases.push((format!("footer length {length}"), bytes));
    }
    let mut minimal = b"ARROW1\0\0".to_vec();
    minimal.extend(1_000_000i32.to_le_bytes());
    minimal.extend(b"ARROW1");
    cases.push(("minimal oversized footer".into(), minimal));
    for length in [0, 4, 9] {
        cases.push((
            format!("truncated file {length}"),
            original[..length].to_vec(),
        ));
    }
    assert_clean_errors(cases);
}

#[test]
fn dictionary_and_record_extents_and_frames_do_not_unwind() {
    let original = valid_file();
    let trailer = original.len() - 10;
    let footer_start =
        trailer - read_footer_length(original[trailer..].try_into().unwrap()).unwrap();
    let footer = arrow::ipc::root_as_footer(&original[footer_start..trailer]).unwrap();
    let mut cases = Vec::new();
    for (kind, block) in [
        ("dictionary", footer.dictionaries().unwrap().get(0)),
        ("record", footer.recordBatches().unwrap().get(0)),
    ] {
        // Block is the public, fixed-size IPC descriptor borrowed from the footer.
        let descriptor = (block.0.as_ptr() as usize)
            .checked_sub(original.as_ptr() as usize)
            .unwrap();
        assert!(descriptor >= footer_start && descriptor + 24 <= trailer);
        let offset = block.offset();
        let metadata = block.metaDataLength();
        let body = block.bodyLength();
        assert_eq!(&original[offset as usize..offset as usize + 4], &[255; 4]);
        for (name, bad) in [
            ("negative offset", Block::new(-1, metadata, body)),
            ("negative metadata", Block::new(offset, -1, body)),
            ("negative body", Block::new(offset, metadata, -1)),
            ("offset beyond file", Block::new(i64::MAX, metadata, body)),
            ("body beyond file", Block::new(offset, metadata, i64::MAX)),
            ("extent overflow", Block::new(i64::MAX, i32::MAX, i64::MAX)),
            ("empty framing", Block::new(offset, 0, 0)),
            ("short continuation", Block::new(offset, 4, 0)),
            (
                "footer overlap",
                Block::new((footer_start - 4) as i64, 8, 0),
            ),
        ] {
            let mut bytes = original.clone();
            bytes[descriptor..descriptor + 24].copy_from_slice(&bad.0);
            cases.push((format!("{kind}: {name}"), bytes));
        }
    }
    assert_clean_errors(cases);
}

fn assert_values(batches: &[RecordBatch], reversed: bool) {
    let mut ids = Vec::new();
    let mut labels = Vec::new();
    for batch in batches {
        let (id, label) = if reversed { (1, 0) } else { (0, 1) };
        assert_eq!(batch.schema().field(id).name(), "id");
        assert_eq!(batch.schema().field(label).name(), "label");
        ids.extend(
            batch
                .column(id)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .iter(),
        );
        let strings = arrow::compute::cast(batch.column(label), &DataType::Utf8).unwrap();
        labels.extend(
            strings
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .map(|v| v.map(str::to_owned)),
        );
    }
    assert_eq!(ids, vec![Some(-7), None, Some(9), Some(-7), None, Some(9)]);
    assert_eq!(
        labels,
        vec![
            Some("alpha".into()),
            None,
            Some("beta".into()),
            Some("alpha".into()),
            None,
            Some("beta".into())
        ]
    );
}

#[test]
fn valid_dictionary_batches_projection_and_slicing_preserve_values() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("rg_00000.arrow"), valid_file()).unwrap();
    let full = read_row_group(dir.path(), 0, None, None).unwrap();
    assert_eq!(full.len(), 2);
    assert_values(&full, false);
    let projected = read_row_group(dir.path(), 0, Some(&[1, 0]), None).unwrap();
    assert_values(&projected, true);
    let sliced = read_row_group(dir.path(), 0, None, Some(2)).unwrap();
    assert_eq!(sliced.len(), 4);
    assert_values(&sliced, false);
}
