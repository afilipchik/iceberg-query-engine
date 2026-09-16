#[test]
fn missing_dictionary_record_batch_is_a_named_error_for_all_projections() {
    let original = valid_file();
    let trailer = original.len() - 10;
    let footer_start = trailer
        - read_footer_length(original[trailer..].try_into().unwrap()).unwrap();
    let footer = arrow::ipc::root_as_footer(&original[footer_start..trailer]).unwrap();
    let block = footer.dictionaries().unwrap().get(0);
    let offset = block.offset() as usize;
    let metadata_end = offset + block.metaDataLength() as usize;
    assert_eq!(&original[offset..offset + 4], &[255; 4]);
    let message_start = offset + 8;
    let message = arrow::ipc::root_as_message(&original[message_start..metadata_end]).unwrap();
    let dictionary = message.header_as_dictionary_batch().unwrap();
    assert!(dictionary.data().is_some());
    let table = message_start + dictionary._tab.loc();
    let distance = i32::from_le_bytes(original[table..table + 4].try_into().unwrap());
    let vtable = usize::try_from(table as i64 - i64::from(distance)).unwrap();
    let slot = vtable + arrow::ipc::DictionaryBatch::VT_DATA as usize;
    let mut bytes = original.clone();
    bytes[slot..slot + 2].fill(0);
    // The structural verifier accepts this envelope; the application must reject
    // the absent record batch before handing it to the dictionary decoder.
    let message = arrow::ipc::root_as_message(&bytes[message_start..metadata_end]).unwrap();
    assert!(message.header_as_dictionary_batch().unwrap().data().is_none());
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("rg_00000.arrow"), bytes).unwrap();
    let projections: [Option<&[usize]>; 4] = [None, Some(&[0]), Some(&[1]), Some(&[])];
    let mut failures = Vec::new();
    for projection in projections {
        match std::panic::catch_unwind(|| read_row_group(dir.path(), 0, projection, None)) {
            Ok(Err(error)) if error.to_string().contains("rg_00000.arrow") => (),
            Ok(Err(error)) => failures.push(format!("{projection:?}: unnamed error: {error}")),
            Ok(Ok(_)) => failures.push(format!("{projection:?}: accepted missing dictionary data")),
            Err(_) => failures.push(format!("{projection:?}: reader panicked")),
        }
    }
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
