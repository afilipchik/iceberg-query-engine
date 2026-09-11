//! Incremental ID expansion over one retained dictionary page. A prepared ID
//! prefix survives output admission refusal; no page source is reread.
use super::*;

#[derive(Clone)]
pub(super) struct DictionaryPage {
    dictionary: Dictionary,
    fixed: Option<(Type, usize, DataType)>,
    ids: Option<HybridDecoder<'static>>,
    validity: Option<NullBuffer>,
    domain: u32,
    rows: usize,
    row: usize,
    output: Option<Output>,
}
impl DictionaryPage {
    pub(super) fn new(
        dictionary: Dictionary,
        fixed: Option<(Type, usize, DataType)>,
        rows: usize,
        values: Buffer,
        validity: Option<NullBuffer>,
    ) -> Result<Self> {
        let dense = rows - validity.as_ref().map_or(0, NullBuffer::null_count);
        let (ids, domain) = if dense == 0 {
            if values.len() > 1 || values.first().is_some_and(|w| *w > 32) {
                return Err(invalid("encoded IDs for all-NULL page"));
            }
            (None, 0)
        } else {
            let width = *values
                .first()
                .ok_or_else(|| invalid("missing ID bit width"))?;
            let domain = dictionary
                .len()
                .checked_sub(1)
                .and_then(|n| u32::try_from(n).ok())
                .ok_or_else(|| invalid("invalid dictionary domain"))?;
            (
                Some(HybridDecoder::from_owned(values.slice(1), width, dense)?),
                domain,
            )
        };
        Ok(Self {
            dictionary,
            fixed,
            ids,
            validity,
            domain,
            rows,
            row: 0,
            output: None,
        })
    }

    fn prepare(
        &self,
        rows: usize,
        pool: &MemoryPool,
    ) -> Result<(Option<HybridDecoder<'static>>, Output)> {
        let validity = self.validity.as_ref().map(|v| v.slice(self.row, rows));
        let dense = rows - validity.as_ref().map_or(0, NullBuffer::null_count);
        let mut working = self.ids.clone();
        let ids = if dense == 0 {
            UInt32Array::from(Vec::<u32>::new())
        } else {
            working
                .as_mut()
                .ok_or_else(|| invalid("missing dictionary ID cursor"))?
                .next(dense, self.domain, pool)?
                .ok_or_else(|| invalid("missing IDs"))?
        };
        let output = match &self.dictionary {
            Dictionary::Utf8(dictionary) => Output::Dictionary(DictionaryUtf8Decoder::new(
                dictionary,
                &ids,
                validity.as_ref(),
            )?),
            Dictionary::Fixed(body, count) => {
                let (physical, width, data_type) = self
                    .fixed
                    .as_ref()
                    .ok_or_else(|| invalid("dictionary type mismatch"))?;
                Output::Fixed(
                    PlainFixedDecoder::new(
                        body.clone(),
                        *count,
                        None,
                        *physical,
                        *width,
                        data_type.clone(),
                    )?
                    .with_dictionary_ids(ids, validity)?,
                )
            }
        };
        Ok((working, output))
    }

    pub(super) fn has_remaining(&self) -> bool {
        self.row < self.rows || self.output.as_ref().is_some_and(|o| o.remaining() > 0)
    }

    pub(super) fn next(
        &mut self,
        rows: usize,
        bytes: usize,
        pool: &MemoryPool,
    ) -> Result<Option<ArrayRef>> {
        loop {
            if let Some(output) = self.output.as_mut() {
                if let Some(array) = output.next(rows, bytes, pool)? {
                    return Ok(Some(array));
                }
                self.output = None;
            }
            if self.row == self.rows {
                return Ok(None);
            }
            let mut count = rows.min(self.rows - self.row);
            let (ids, output) = loop {
                match self.prepare(count, pool) {
                    Err(error) if error.is_memory_limit() && count > 1 => {
                        count = (count / 2).max(1)
                    }
                    result => break result?,
                }
            };
            // Commit decoded IDs once. If output cannot yet fit, this prefix
            // stays owned and its output cursor resumes on the next call.
            self.ids = ids;
            self.row += count;
            self.output = Some(output);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;

    fn page(encoded: Vec<u8>, rows: usize) -> DictionaryPage {
        DictionaryPage::new(
            Dictionary::Fixed(
                Buffer::from(
                    [-7i64, 91, -7]
                        .iter()
                        .flat_map(|v| v.to_le_bytes())
                        .collect::<Vec<_>>(),
                ),
                3,
            ),
            Some((Type::INT64, 0, DataType::Int64)),
            rows,
            Buffer::from(encoded),
            None,
        )
        .unwrap()
    }

    #[test]
    fn prepared_ids_survive_output_refusal_and_resume_exactly() {
        // Two RLE runs: two ID0 values followed by two ID1 values.
        let mut reader = page(vec![2, 4, 0, 4, 1], 4);
        let pool = MemoryPool::new(2048);
        let held = pool.allocate(1024).unwrap();
        assert!(reader.next(2, 100, &pool).unwrap_err().is_memory_limit());
        assert_eq!(reader.row, 2);
        assert!(reader.output.is_some());
        drop(held);
        let first = reader.next(2, 100, &pool).unwrap().unwrap();
        assert_eq!(
            first
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[-7, -7]
        );
        drop(first);
        let second = reader.next(2, 100, &pool).unwrap().unwrap();
        assert_eq!(
            second
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[91, 91]
        );
        drop(second);
        assert!(reader.next(2, 100, &pool).unwrap().is_none());
        drop(reader);
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn later_invalid_id_is_rejected_without_committing_the_bad_prefix() {
        let mut reader = page(vec![2, 4, 0, 4, 3], 4);
        let pool = MemoryPool::new(8192);
        let first = reader.next(2, 100, &pool).unwrap().unwrap();
        assert_eq!(
            first
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[-7, -7]
        );
        drop(first);
        assert!(reader.next(2, 100, &pool).is_err());
        assert_eq!(reader.row, 2);
        assert!(reader.output.is_none());
        drop(reader);
        assert_eq!(pool.used(), 0);
    }
}
