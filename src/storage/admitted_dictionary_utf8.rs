//! Admitted expansion of validated dictionary IDs into bounded UTF8 chunks.
//! Decoder clones retain dictionary, ID and definition-buffer owners. NULL
//! validity slices retain their original owner; output strings are copied into
//! separately admitted buffers. No dictionary-size estimate proves an ID valid.
use crate::{
    execution::{MemoryPool, ReservedBufferBuilder},
    QueryError, Result,
};
use arrow::{
    array::{Array, StringArray, UInt32Array},
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
};
fn invalid(message: &str) -> QueryError {
    QueryError::Storage(format!("dictionary UTF8 output: {message}"))
}
#[derive(Clone)]
pub(crate) struct DictionaryUtf8Decoder {
    dictionary: StringArray,
    ids: UInt32Array,
    validity: Option<NullBuffer>,
    rows: usize,
    row: usize,
    id: usize,
}
impl DictionaryUtf8Decoder {
    pub(crate) fn new(
        dictionary: &StringArray,
        ids: &UInt32Array,
        validity: Option<&NullBuffer>,
    ) -> Result<Self> {
        if dictionary.null_count() != 0 || ids.null_count() != 0 {
            return Err(invalid("dictionary entries and dense IDs must be non-NULL"));
        }
        let rows = validity.map_or(ids.len(), NullBuffer::len);
        if validity.is_some_and(|v| v.len() - v.null_count() != ids.len()) {
            return Err(invalid("ID count differs from non-NULL row count"));
        }
        if ids
            .values()
            .iter()
            .any(|id| *id as usize >= dictionary.len())
        {
            return Err(invalid("ID outside exact dictionary domain"));
        }
        Ok(Self {
            dictionary: dictionary.clone(),
            ids: ids.clone(),
            validity: validity.cloned(),
            rows,
            row: 0,
            id: 0,
        })
    }
    pub(crate) fn remaining(&self) -> usize {
        self.rows - self.row
    }

    pub(crate) fn next(
        &mut self,
        max_rows: usize,
        target_value_bytes: usize,
        pool: &MemoryPool,
    ) -> Result<Option<StringArray>> {
        if max_rows == 0 || target_value_bytes == 0 {
            return Err(invalid("positive row and byte targets required"));
        }
        if self.row == self.rows {
            return Ok(None);
        }
        let mut rows = 0;
        let mut id = self.id;
        let mut payload = 0usize;
        while rows < max_rows && rows < self.rows - self.row {
            let valid = self
                .validity
                .as_ref()
                .is_none_or(|v| v.is_valid(self.row + rows));
            let length = if valid {
                self.dictionary.value(self.ids.value(id) as usize).len()
            } else {
                0
            };
            let total = payload
                .checked_add(length)
                .ok_or_else(|| invalid("value extent overflow"))?;
            if rows > 0 && (total > target_value_bytes || total > i32::MAX as usize) {
                break;
            }
            if total > i32::MAX as usize {
                return Err(invalid("one value exceeds Arrow UTF8 offset domain"));
            }
            payload = total;
            rows += 1;
            if valid {
                id += 1;
            }
        }
        let mut offsets = ReservedBufferBuilder::<i32>::with_capacity(
            pool,
            rows.checked_add(1)
                .ok_or_else(|| invalid("offset count overflow"))?,
        )?;
        let mut values = ReservedBufferBuilder::<u8>::with_capacity(pool, payload)?;
        offsets.extend_reserved(1, [0])?;
        let mut current_id = self.id;
        for row in self.row..self.row + rows {
            if self.validity.as_ref().is_none_or(|v| v.is_valid(row)) {
                let bytes = self
                    .dictionary
                    .value(self.ids.value(current_id) as usize)
                    .as_bytes();
                values.extend_reserved(bytes.len(), bytes.iter().copied())?;
                current_id += 1;
            }
            offsets.extend_reserved(1, [values.as_slice().len() as i32])?;
        }
        let validity = self.validity.as_ref().map(|v| v.slice(self.row, rows));
        let output = StringArray::try_new(
            OffsetBuffer::new(ScalarBuffer::new(offsets.finish(), 0, rows + 1)),
            values.finish(),
            validity,
        )?;
        self.row += rows;
        self.id = id;
        Ok(Some(output))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn byte_target_nulls_duplicates_and_denial_preserve_prefix() {
        let long = "x".repeat(8192);
        let dictionary = StringArray::from(vec!["é", long.as_str()]);
        let ids = UInt32Array::from(vec![0, 1, 0]);
        let validity = NullBuffer::from(vec![true, false, true, true]);
        let pool = MemoryPool::new(32768);
        let mut decoder = DictionaryUtf8Decoder::new(&dictionary, &ids, Some(&validity)).unwrap();
        // The retained decoder must not depend on the caller's array handles.
        drop(dictionary);
        drop(ids);
        drop(validity);
        let first = decoder.next(8, 2, &pool).unwrap().unwrap();
        assert_eq!(first.iter().collect::<Vec<_>>(), vec![Some("é"), None]);
        drop(first);
        let held = pool.allocate(30000).unwrap();
        assert!(decoder.next(8, 2, &pool).unwrap_err().is_memory_limit());
        assert_eq!((decoder.row, decoder.id), (2, 1));
        assert_eq!(pool.used(), 30000);
        drop(held);
        let oversized = decoder.next(8, 2, &pool).unwrap().unwrap();
        assert_eq!(oversized.value(0), long);
        assert_eq!(oversized.len(), 1);
        let slice = oversized.slice(0, 1);
        drop(oversized);
        assert!(pool.used() >= 8192);
        drop(slice);
        assert_eq!(decoder.next(8, 2, &pool).unwrap().unwrap().value(0), "é");
        assert!(decoder.next(8, 2, &pool).unwrap().is_none());
        assert_eq!(pool.used(), 0);
    }
    #[test]
    fn invalid_ids_and_counts_refuse_before_output_allocation() {
        let dictionary = StringArray::from(vec!["a"]);
        let ids = UInt32Array::from(vec![1]);
        assert!(DictionaryUtf8Decoder::new(&dictionary, &ids, None).is_err());
        let ids = UInt32Array::from(vec![Some(0), None]);
        assert!(DictionaryUtf8Decoder::new(&dictionary, &ids, None).is_err());
        let ids = UInt32Array::from(vec![0]);
        let validity = NullBuffer::new_null(1);
        assert!(DictionaryUtf8Decoder::new(&dictionary, &ids, Some(&validity)).is_err());
        let dictionary = StringArray::from(vec![Some("a"), None]);
        assert!(DictionaryUtf8Decoder::new(&dictionary, &ids, None).is_err());
    }
    #[test]
    fn all_null_empty_dictionary_and_sliced_inputs_are_exact() {
        let pool = MemoryPool::new(8192);
        let dictionary = StringArray::from(Vec::<&str>::new());
        let ids = UInt32Array::from(Vec::<u32>::new());
        let validity = NullBuffer::new_null(5);
        let mut decoder = DictionaryUtf8Decoder::new(&dictionary, &ids, Some(&validity)).unwrap();
        let array = decoder.next(8, 1, &pool).unwrap().unwrap();
        assert_eq!(array.null_count(), 5);
        drop(array);
        assert_eq!(pool.used(), 0);
        let dictionary = StringArray::from(vec!["ignored", "same", "same"]).slice(1, 2);
        let ids = UInt32Array::from(vec![99, 1, 0, 99]).slice(1, 2);
        let validity = NullBuffer::from(vec![false, true, false, true, false]).slice(1, 3);
        let mut decoder = DictionaryUtf8Decoder::new(&dictionary, &ids, Some(&validity)).unwrap();
        let array = decoder.next(8, 100, &pool).unwrap().unwrap();
        assert_eq!(
            array.iter().collect::<Vec<_>>(),
            vec![Some("same"), None, Some("same")]
        );
        drop(array);
        assert_eq!(pool.used(), 0);
    }
}
