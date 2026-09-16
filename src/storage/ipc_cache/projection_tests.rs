use super::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::{array::*, datatypes::Int32Type, ipc::writer::FileWriter};
use std::sync::Arc;

#[allow(deprecated)]
fn dict_field(name: &str, id: i64, value: DataType) -> Field {
    Field::new_dict(
        name,
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(value)),
        true,
        id,
        false,
    )
}

#[test]
fn shared_dictionary_dependencies_follow_first_definition_and_decline_cycles() {
    use super::dictionary_projection::DictionaryProjection as P;
    let nested =
        |id| DataType::Struct(vec![Arc::new(dict_field("child", id, DataType::Utf8))].into());
    let schema = Schema::new(vec![
        dict_field("first", 7, nested(41)),
        dict_field("selected_alias", 7, nested(99)),
        dict_field("unused", -100, DataType::Utf8),
    ]);
    let bound = P::bind(&schema, Some(&[1])).unwrap();
    for id in [7, 41, 99] {
        assert_eq!(bound.required(id), Some(true));
    }
    assert_eq!(bound.required(-100), Some(false));
    assert_eq!(bound.required(12345), None);
    assert!(P::bind(&schema, None).is_none());
    let cyclic = Schema::new(vec![dict_field("cycle", 7, nested(7))]);
    assert!(P::bind(&cyclic, Some(&[0])).is_none());
    let wide = Schema::new(
        (0..65)
            .map(|id| dict_field(&format!("d{id}"), id, DataType::Utf8))
            .collect::<Vec<_>>(),
    );
    assert!(P::bind(&wide, Some(&[])).is_none());
    let mut deep = DataType::Int64;
    for _ in 0..80 {
        deep = DataType::List(Arc::new(Field::new("child", deep, true)));
    }
    assert!(P::bind(
        &Schema::new(vec![Field::new("deep", deep, true)]),
        Some(&[0])
    )
    .is_none());
}

#[test]
fn nested_reordered_repeated_and_empty_projections_preserve_rows() {
    let dict: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), None, Some(1)]),
            Arc::new(StringArray::from(vec!["alpha", "beta"])),
        )
        .unwrap(),
    );
    let nested: ArrayRef = Arc::new(StructArray::from(vec![(
        Arc::new(Field::new("label", dict.data_type().clone(), true)),
        dict.clone(),
    )]));
    let batch = RecordBatch::try_from_iter([
        (
            "number",
            Arc::new(Int64Array::from(vec![Some(7), None, Some(-9)])) as ArrayRef,
        ),
        ("unused", dict),
        ("nested", nested),
    ])
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileWriter::try_new(
        File::create(rg_path(dir.path(), 0)).unwrap(),
        &batch.schema(),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    for projection in [vec![2], vec![2, 0], vec![2, 2, 0], vec![]] {
        let before = DICTIONARY_DECODES.with(|n| n.get());
        let output = read_row_group(dir.path(), 0, Some(&projection), None).unwrap();
        assert_eq!(output.len(), 2);
        for rows in output {
            assert_eq!(rows.num_rows(), 3);
            assert_eq!(rows, batch.project(&projection).unwrap());
        }
        assert_eq!(
            DICTIONARY_DECODES.with(|n| n.get()) - before,
            usize::from(!projection.is_empty())
        );
    }
    for projection in [vec![3], vec![usize::MAX]] {
        let error = match open_row_group(dir.path(), 0, Some(&projection)) {
            Ok(_) => panic!("invalid projection accepted"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("rg_00000.arrow"));
    }
}

#[test]
fn numeric_projection_does_not_decode_unused_dictionaries() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), None, Some(1)]),
            Arc::new(StringArray::from(vec![
                "x".repeat(1 << 20),
                "y".repeat(1 << 20),
            ])),
        )
        .unwrap(),
    );
    let batch = RecordBatch::try_from_iter([
        (
            "number",
            Arc::new(Int64Array::from(vec![Some(7), None, Some(-9)])) as ArrayRef,
        ),
        ("unused", dictionary),
    ])
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut writer = FileWriter::try_new(
        File::create(rg_path(dir.path(), 0)).unwrap(),
        &batch.schema(),
    )
    .unwrap();
    writer.write(&batch).unwrap();
    writer.write(&batch).unwrap();
    writer.finish().unwrap();
    drop(writer);
    // Same decoder, file and projection: disabling only pruning must retain the
    // old dictionary work while producing exactly the same nullable output.
    let baseline_before = DICTIONARY_DECODES.with(|n| n.get());
    let baseline = open_row_group_with_dictionary_projection(dir.path(), 0, Some(&[0]), false)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(DICTIONARY_DECODES.with(|n| n.get()) - baseline_before, 1);
    assert_eq!(baseline, vec![batch.project(&[0]).unwrap(); 2]);
    let before = DICTIONARY_DECODES.with(|n| n.get());
    let mut reader = open_row_group(dir.path(), 0, Some(&[0])).unwrap();
    let first = reader.next().unwrap().unwrap();
    let second = reader.next().unwrap().unwrap();
    assert!(reader.next().is_none());
    drop(reader);
    for output in [first, second] {
        assert_eq!(output.num_columns(), 1);
        let values = output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            values.iter().collect::<Vec<_>>(),
            vec![Some(7), None, Some(-9)]
        );
    }
    assert_eq!(
        DICTIONARY_DECODES.with(|n| n.get()) - before,
        0,
        "numeric projection must not decode unused dictionary payloads"
    );
    let full = read_row_group(dir.path(), 0, None, None).unwrap();
    assert_eq!(full, vec![batch.clone(), batch]);
    assert!(DICTIONARY_DECODES.with(|n| n.get()) > before);
}

#[test]
fn dictionary_deltas_are_applied_in_order_only_when_selected() {
    use arrow::ipc::writer::{DictionaryHandling, IpcWriteOptions};
    let dir = tempfile::tempdir().unwrap();
    let make = |values: Vec<&str>, keys: Vec<Option<i32>>| {
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(keys),
                Arc::new(StringArray::from(values)),
            )
            .unwrap(),
        );
        RecordBatch::try_from_iter([
            (
                "number",
                Arc::new(Int64Array::from(vec![Some(7), None, Some(-9)])) as ArrayRef,
            ),
            ("label", dictionary),
        ])
        .unwrap()
    };
    let first = make(vec!["alpha", "beta"], vec![Some(0), None, Some(1)]);
    let second = make(vec!["alpha", "beta", "gamma"], vec![Some(2), Some(0), None]);
    let options = IpcWriteOptions::default().with_dictionary_handling(DictionaryHandling::Delta);
    let mut writer = FileWriter::try_new_with_options(
        File::create(rg_path(dir.path(), 0)).unwrap(),
        &first.schema(),
        options,
    )
    .unwrap();
    writer.write(&first).unwrap();
    writer.write(&second).unwrap();
    writer.finish().unwrap();
    drop(writer);
    // Assert this fixture really contains a delta, not a second full dictionary.
    let bytes = std::fs::read(rg_path(dir.path(), 0)).unwrap();
    let trailer = bytes.len() - 10;
    let footer_len =
        arrow::ipc::reader::read_footer_length(bytes[trailer..].try_into().unwrap()).unwrap();
    let footer = arrow::ipc::root_as_footer(&bytes[trailer - footer_len..trailer]).unwrap();
    let dictionaries = footer.dictionaries().unwrap();
    assert_eq!(dictionaries.len(), 2);
    let block = dictionaries.get(1);
    let start = block.offset() as usize;
    let message =
        arrow::ipc::root_as_message(&bytes[start + 8..start + block.metaDataLength() as usize])
            .unwrap();
    assert!(message.header_as_dictionary_batch().unwrap().isDelta());
    for projection in [vec![0], vec![1], vec![1, 0, 1], vec![]] {
        let before = DICTIONARY_DECODES.with(|n| n.get());
        let output = read_row_group(dir.path(), 0, Some(&projection), None).unwrap();
        assert_eq!(output.len(), 2);
        for (actual, expected) in output.iter().zip([&first, &second]) {
            assert_eq!(actual.num_rows(), 3);
            for (column, source) in projection.iter().enumerate() {
                // Final dictionary values may include later entries; compare SQL values.
                let actual = arrow::compute::cast(actual.column(column), &DataType::Utf8).unwrap();
                let expected =
                    arrow::compute::cast(expected.column(*source), &DataType::Utf8).unwrap();
                assert_eq!(actual.as_ref(), expected.as_ref());
            }
        }
        assert_eq!(
            DICTIONARY_DECODES.with(|n| n.get()) - before,
            if projection.contains(&1) { 2 } else { 0 }
        );
    }
}

#[test]
fn file_dictionary_ids_are_not_column_ordinals_and_can_be_shared() {
    let dictionary: ArrayRef = Arc::new(
        DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), None, Some(1)]),
            Arc::new(StringArray::from(vec!["alpha", "beta"])),
        )
        .unwrap(),
    );
    let batch = RecordBatch::try_from_iter([("first", dictionary.clone()), ("second", dictionary)])
        .unwrap();
    let mut original = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut original, &batch.schema()).unwrap();
        writer.write(&batch).unwrap();
        writer.finish().unwrap();
    }
    let trailer = original.len() - 10;
    let footer_start = trailer
        - arrow::ipc::reader::read_footer_length(original[trailer..].try_into().unwrap()).unwrap();
    let footer = arrow::ipc::root_as_footer(&original[footer_start..trailer]).unwrap();
    let encoding = footer
        .schema()
        .unwrap()
        .fields()
        .unwrap()
        .get(1)
        .dictionary()
        .unwrap();
    assert_eq!(encoding.id(), 1);
    let block = footer.dictionaries().unwrap().get(1);
    let message_start = block.offset() as usize + 8;
    let message = arrow::ipc::root_as_message(
        &original[message_start..block.offset() as usize + block.metaDataLength() as usize],
    )
    .unwrap();
    let dictionary = message.header_as_dictionary_batch().unwrap();
    assert_eq!(dictionary.id(), 1);
    let field_position = |table: usize, slot: u16| {
        let distance = i32::from_le_bytes(original[table..table + 4].try_into().unwrap());
        let vtable = usize::try_from(table as i64 - i64::from(distance)).unwrap();
        let offset = u16::from_le_bytes(
            original[vtable + slot as usize..vtable + slot as usize + 2]
                .try_into()
                .unwrap(),
        );
        assert_ne!(offset, 0);
        table + offset as usize
    };
    let schema_id = field_position(
        footer_start + encoding._tab.loc(),
        arrow::ipc::DictionaryEncoding::VT_ID,
    );
    let message_id = field_position(
        message_start + dictionary._tab.loc(),
        arrow::ipc::DictionaryBatch::VT_ID,
    );
    for id in [37i64, 0] {
        let mut bytes = original.clone();
        bytes[schema_id..schema_id + 8].copy_from_slice(&id.to_le_bytes());
        bytes[message_id..message_id + 8].copy_from_slice(&id.to_le_bytes());
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(rg_path(dir.path(), 0), bytes).unwrap();
        let before = DICTIONARY_DECODES.with(|n| n.get());
        let output = read_row_group(dir.path(), 0, Some(&[1]), None).unwrap();
        assert_eq!(output, vec![batch.project(&[1]).unwrap()]);
        assert_eq!(
            DICTIONARY_DECODES.with(|n| n.get()) - before,
            if id == 0 { 2 } else { 1 }
        );
    }
}
