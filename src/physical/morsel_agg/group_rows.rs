//! Couples canonical keys and partial states before publishing a group index.
//! Exact hash indexing is admitted before publication; ingestion and spill files
//! remain separate components.
use super::{
    key_rows::{KeyLayout, KeyRef, KeyRows, KeyWorkspace},
    state_rows::{PreparedRow, RowWorkspace, StateRowLayout, StateRows},
};
use crate::{
    execution::{MemoryPool, MemoryReservation},
    planner::{AggregateFunction, ScalarValue},
    QueryError, Result,
};
use arrow::datatypes::DataType;
use hashbrown::HashTable;
use std::{borrow::Cow, io::Write, sync::Arc};

pub(super) struct GroupLayout {
    identity: uuid::Uuid,
    keys: Arc<KeyLayout>,
    states: Arc<StateRowLayout>,
    pool: MemoryPool,
    _reservation: MemoryReservation,
}

impl GroupLayout {
    pub(super) fn bind(
        pool: &MemoryPool,
        keys: &[DataType],
        states: &[(AggregateFunction, DataType, bool)],
    ) -> Result<Option<Arc<Self>>> {
        let reservation = pool.allocate(std::mem::size_of::<Self>() + 512)?;
        let Some(keys) = KeyLayout::bind(pool, keys)? else {
            return Ok(None);
        };
        let Some(states) = StateRowLayout::bind(pool, states)? else {
            return Ok(None);
        };
        Ok(Some(Arc::new(Self {
            identity: uuid::Uuid::new_v4(),
            keys,
            states,
            pool: pool.clone(),
            _reservation: reservation,
        })))
    }
    pub(super) fn key_workspace(&self) -> Result<KeyWorkspace> {
        KeyWorkspace::new(self.keys.clone())
    }
    pub(super) fn key_layout(&self) -> &Arc<KeyLayout> {
        &self.keys
    }
    pub(super) fn validate_key_arrays(&self, arrays: &[arrow::array::ArrayRef]) -> Result<()> {
        self.keys.validate_arrays(arrays)
    }
    pub(super) fn identity(&self) -> uuid::Uuid {
        self.identity
    }
    pub(super) fn pool(&self) -> &MemoryPool {
        &self.pool
    }
    pub(super) fn row_workspace(&self) -> Result<RowWorkspace> {
        RowWorkspace::new(self.states.clone())
    }
}

pub(super) struct GroupRows {
    index: HashTable<(u64, usize)>,
    index_poisoned: bool,
    keys: KeyRows,
    states: StateRows,
    // Keep the query-scoped combined layout owner alive with its children.
    _layout: Arc<GroupLayout>,
    // Table storage is dropped before its lease.
    index_reservation: Option<MemoryReservation>,
}

impl GroupRows {
    pub(super) fn validate_output_schema(&self, schema: &arrow::datatypes::Schema) -> Result<()> {
        if schema.fields().len() != self.output_columns() {
            return Err(index_error("output arity mismatch"));
        }
        for (i, field) in schema.fields().iter().enumerate() {
            let expected = if i < self._layout.keys.len() {
                Cow::Borrowed(self._layout.keys.data_type(i)?)
            } else {
                self.states.output_type(i - self._layout.keys.len())?
            };
            if field.data_type() != expected.as_ref() {
                return Err(index_error(
                    "output type differs from bound aggregate layout",
                ));
            }
        }
        Ok(())
    }
    /// An empty global aggregate has one group with untouched initial states.
    /// Grouped empty input remains empty. All publication growth is admitted.
    pub(super) fn ensure_empty_global(&mut self, key: &mut KeyWorkspace) -> Result<()> {
        if self._layout.keys.len() != 0 || self.len() != 0 {
            return Ok(());
        }
        key.encode(&[])?;
        let hash = key.key()?.hash64();
        self.reserve_index()?;
        let row = self.append_empty(key)?;
        self.index.insert_unique(hash, (hash, row), |entry| entry.0);
        Ok(())
    }
    pub(super) fn output_columns(&self) -> usize {
        self._layout.keys.len() + self.states.slot_count()
    }
    pub(super) fn output_pool(&self) -> &MemoryPool {
        self._layout.pool()
    }
    pub(super) fn key_layout(&self) -> &KeyLayout {
        &self._layout.keys
    }
    pub(super) fn layout_identity(&self) -> uuid::Uuid {
        self._layout.identity()
    }
    pub(super) fn encoded_size(&self, row: usize) -> Result<usize> {
        super::scalar_state_codec::add(
            super::scalar_state_codec::add(9, self.key(row)?.bytes().len())?,
            self.states.encoded_size(row)?,
        )
    }
    /// Restore a complete version-1 row into an absent group. For repeated keys
    /// across spill runs, restore into a staging store and merge partial states;
    /// never feed finalized values back through input aggregation.
    pub(super) fn prepare_restore<'a>(
        &'a mut self,
        key: &mut KeyWorkspace,
        workspace: &'a mut RowWorkspace,
        bytes: &[u8],
    ) -> Result<PreparedGroup<'a>> {
        if bytes.first() != Some(&1) {
            return Err(index_error("unsupported row version"));
        }
        let length = bytes
            .get(1..9)
            .ok_or_else(|| index_error("truncated row header"))?;
        let length = usize::try_from(u64::from_le_bytes(length.try_into().unwrap()))
            .map_err(|_| index_error("key length overflow"))?;
        let end = 9usize
            .checked_add(length)
            .ok_or_else(|| index_error("key length overflow"))?;
        let payload = bytes
            .get(9..end)
            .ok_or_else(|| index_error("truncated row key"))?;
        let states = &bytes[end..];
        self._layout.states.validate_encoded(states)?;
        key.load_encoded(payload)?;
        let hash = key.key()?.hash64();
        if self.find_hashed(key.key()?, hash)?.is_some() {
            return Err(index_error("restore requires an absent group"));
        }
        self.reserve_index()?;
        let row = self.keys.append(key)?;
        if row != self.states.len() {
            self.keys.truncate(row);
            return Err(index_error("key/state row mismatch"));
        }
        let state = match self.states.prepare_restore(workspace, states) {
            Ok(state) => state,
            Err(error) => {
                self.keys.truncate(row);
                return Err(error);
            }
        };
        Ok(PreparedGroup {
            state: Some(state),
            keys: &mut self.keys,
            index: &mut self.index,
            row,
            hash,
            new: true,
        })
    }
    /// Version 1 row payload: version byte, u64 key length, canonical key,
    /// then partial states in bound slot order. File identity/integrity and
    /// publication are separate; never release rows after a partial write.
    pub(super) fn write_row(&self, row: usize, writer: &mut impl Write) -> Result<usize> {
        let key = self.key(row)?;
        let states = self.states.encoded_size(row)?;
        let size = super::scalar_state_codec::add(
            super::scalar_state_codec::add(9, key.bytes().len())?,
            states,
        )?;
        writer.write_all(&[1])?;
        writer.write_all(&(key.bytes().len() as u64).to_le_bytes())?;
        writer.write_all(key.bytes())?;
        self.states.write_to(row, writer)?;
        Ok(size)
    }

    pub(super) fn new(layout: Arc<GroupLayout>) -> Result<Self> {
        let keys = KeyRows::new(layout.keys.clone())?;
        let states = StateRows::new(layout.states.clone())?;
        Ok(Self {
            index: HashTable::new(),
            index_poisoned: false,
            keys,
            states,
            _layout: layout,
            index_reservation: None,
        })
    }
    pub(super) fn len(&self) -> usize {
        self.states.len()
    }
    pub(super) fn key(&self, row: usize) -> Result<KeyRef<'_>> {
        self.keys.key(row)
    }
    pub(super) fn value(&self, row: usize, slot: usize) -> Result<Cow<'_, ScalarValue>> {
        self.states.value(row, slot)
    }

    fn reserve_index(&mut self) -> Result<()> {
        if self.index_poisoned {
            return Err(index_error("index layout is poisoned"));
        }
        if self.index.len() < self.index.capacity() {
            return Ok(());
        }
        let required = self
            .index
            .len()
            .checked_add(1)
            .ok_or_else(|| index_error("size overflow"))?;
        // HashTable<(u64, usize)> and the raw-key HashMap have the same
        // pinned hashbrown pair/control layout and audited growth bound.
        let bound = super::raw_state::index_bound(required)?;
        if self.index_reservation.is_none() {
            self.index_reservation = Some(self._layout.pool.allocate(512)?);
        }
        let lease = self.index_reservation.as_mut().unwrap();
        let old = lease.size();
        lease.resize(
            old.checked_add(bound)
                .ok_or_else(|| index_error("size overflow"))?,
        )?;
        if let Err(error) = self.index.try_reserve(1, |entry| entry.0) {
            lease.resize(old)?;
            return Err(index_error(&format!("allocation refused: {error}")));
        }
        let actual = self.index.allocation_size();
        if actual > bound {
            self.index_poisoned = true;
            return Err(index_error("allocation exceeds admitted layout bound"));
        }
        lease.resize(
            512usize
                .checked_add(actual)
                .ok_or_else(|| index_error("size overflow"))?,
        )?;
        Ok(())
    }

    fn find_hashed(&self, key: KeyRef<'_>, hash: u64) -> Result<Option<usize>> {
        if self.index_poisoned {
            return Err(index_error("index layout is poisoned"));
        }
        if !key.belongs_to(&self._layout.keys) {
            return Err(index_error("key layout mismatch"));
        }
        if self.index.len() != self.len() {
            return Err(index_error("unpublished group rows outside preparation"));
        }
        let mut error = None;
        let found = self
            .index
            .find(hash, |entry| match self.keys.key(entry.1) {
                Ok(stored) => stored == key,
                Err(cause) => {
                    error = Some(cause);
                    false
                }
            })
            .map(|entry| entry.1);
        if let Some(error) = error {
            return Err(error);
        }
        Ok(found)
    }

    pub(super) fn lookup(&self, key: &KeyWorkspace) -> Result<Option<usize>> {
        let key = key.key()?;
        self.find_hashed(key, key.hash64())
    }

    pub(super) fn prepare_update<'a>(
        &'a mut self,
        key: &KeyWorkspace,
        workspace: &'a mut RowWorkspace,
        values: &[ScalarValue],
    ) -> Result<PreparedGroup<'a>> {
        let hash = key.key()?.hash64();
        self.prepare_hashed(key, hash, workspace, values)
    }

    fn prepare_hashed<'a>(
        &'a mut self,
        key: &KeyWorkspace,
        hash: u64,
        workspace: &'a mut RowWorkspace,
        values: &[ScalarValue],
    ) -> Result<PreparedGroup<'a>> {
        let (row, new) = match self.find_hashed(key.key()?, hash)? {
            Some(row) => (row, false),
            None => {
                self.reserve_index()?;
                (self.append_empty(key)?, true)
            }
        };
        let state = match self.states.prepare_indexed(row, workspace, values, new) {
            Ok(state) => state,
            Err(error) => {
                if new {
                    self.keys.truncate(row);
                }
                return Err(error);
            }
        };
        Ok(PreparedGroup {
            state: Some(state),
            keys: &mut self.keys,
            index: &mut self.index,
            row,
            hash,
            new,
        })
    }

    /// Apply retained evaluated arrays transactionally. No expression/input is
    /// reexecuted; on failure the cursor identifies the first uncommitted row.
    pub(super) fn process_evaluated_from(
        &mut self,
        batch: &arrow::record_batch::RecordBatch,
        group_count: usize,
        start_row: usize,
        key: &mut KeyWorkspace,
        workspace: &mut RowWorkspace,
    ) -> super::IngestionResult<usize> {
        self.process_evaluated_with_limit(batch, group_count, start_row, key, workspace, usize::MAX)
    }

    /// Stop before a new group would exceed the working group limit. Existing
    /// keys still update at the limit. Query admission remains independent.
    pub(super) fn process_evaluated_with_limit(
        &mut self,
        batch: &arrow::record_batch::RecordBatch,
        group_count: usize,
        start_row: usize,
        key: &mut KeyWorkspace,
        workspace: &mut RowWorkspace,
        group_limit: usize,
    ) -> super::IngestionResult<usize> {
        self.process_rows_with_limit(
            batch,
            group_count,
            start_row,
            batch.num_rows(),
            start_row..batch.num_rows(),
            None,
            key,
            workspace,
            group_limit,
        )
    }

    /// Cursor positions refer to the selection, including repeated row indices.
    pub(super) fn process_selected_with_limit(
        &mut self,
        batch: &arrow::record_batch::RecordBatch,
        group_count: usize,
        selection: super::row_selection::RowSelection<'_>,
        start: usize,
        key: &mut KeyWorkspace,
        workspace: &mut RowWorkspace,
        group_limit: usize,
        prepared: Option<&super::prepared_keys::PreparedKeys<'_>>,
    ) -> super::IngestionResult<usize> {
        if selection.batch_rows() != batch.num_rows() || start > selection.len() {
            return Err(super::IngestionFailure::at(
                start,
                index_error("selection extent mismatch"),
            ));
        }
        self.process_rows_with_limit(
            batch,
            group_count,
            start,
            selection.len(),
            selection.rows()[start..].iter().copied(),
            prepared,
            key,
            workspace,
            group_limit,
        )
    }

    fn process_rows_with_limit(
        &mut self,
        batch: &arrow::record_batch::RecordBatch,
        group_count: usize,
        start_row: usize,
        end_position: usize,
        input_rows: impl ExactSizeIterator<Item = usize>,
        prepared: Option<&super::prepared_keys::PreparedKeys<'_>>,
        key: &mut KeyWorkspace,
        workspace: &mut RowWorkspace,
        group_limit: usize,
    ) -> super::IngestionResult<usize> {
        if group_limit == 0 {
            return Err(super::IngestionFailure::at(
                start_row,
                index_error("group limit must be positive"),
            ));
        }
        if start_row > end_position || group_count > batch.num_columns() {
            return Err(super::IngestionFailure::at(
                start_row,
                index_error("evaluated extent mismatch"),
            ));
        }
        if let Some(prepared) = prepared {
            prepared
                .validate(&self._layout, batch, group_count)
                .map_err(|error| super::IngestionFailure::at(start_row, error))?;
        }
        let (keys, values) = batch.columns().split_at(group_count);
        self._layout
            .keys
            .validate_arrays(keys)
            .map_err(|error| super::IngestionFailure::at(start_row, error))?;
        let values = self
            .states
            .bind_arrays(values, batch.num_rows())
            .map_err(|error| super::IngestionFailure::at(start_row, error))?;
        // Diagnostic sampling avoids a clock read on every row. Report only
        // completed calls; pressure-truncated calls are not a full-batch sample.
        let profile = std::env::var_os("QE_AGG_DETAIL_PROF").is_some();
        let mut sampled_rows = 0usize;
        let mut key_time = std::time::Duration::ZERO;
        let mut lookup_time = std::time::Duration::ZERO;
        let mut state_time = std::time::Duration::ZERO;
        let mut commit_time = std::time::Duration::ZERO;
        for (offset, input_row) in input_rows.enumerate() {
            let position = start_row + offset;
            let sampled = profile && offset % 1024 == 0;
            let key_start = sampled.then(std::time::Instant::now);
            let encoded = if let Some(prepared) = prepared {
                prepared.key(input_row)
            } else {
                key.encode_arrays(keys, input_row).and_then(|_| {
                    let key = key.key()?;
                    Ok((key, key.hash64()))
                })
            };
            let (key_ref, hash) =
                encoded.map_err(|error| super::IngestionFailure::at(position, error))?;
            if let Some(start) = key_start {
                key_time += start.elapsed();
            }
            if self.len() >= group_limit {
                match self.find_hashed(key_ref, hash) {
                    Ok(None) => return Ok(position),
                    Ok(Some(_)) => (),
                    Err(error) => return Err(super::IngestionFailure::at(position, error)),
                }
            }
            let result = (|| {
                let lookup_start = sampled.then(std::time::Instant::now);
                let (row, new) = match self.find_hashed(key_ref, hash)? {
                    Some(row) => (row, false),
                    None => {
                        self.reserve_index()?;
                        (self.append_empty_ref(key_ref)?, true)
                    }
                };
                if let Some(start) = lookup_start {
                    lookup_time += start.elapsed();
                }
                let state_start = sampled.then(std::time::Instant::now);
                let state = match self
                    .states
                    .prepare_arrays_indexed(row, workspace, &values, input_row, new)
                {
                    Ok(state) => state,
                    Err(error) => {
                        if new {
                            self.keys.truncate(row);
                        }
                        return Err(error);
                    }
                };
                if let Some(start) = state_start {
                    state_time += start.elapsed();
                }
                let commit_start = sampled.then(std::time::Instant::now);
                PreparedGroup {
                    state: Some(state),
                    keys: &mut self.keys,
                    index: &mut self.index,
                    row,
                    hash,
                    new,
                }
                .commit();
                if let Some(start) = commit_start {
                    commit_time += start.elapsed();
                    sampled_rows += 1;
                }
                Ok(())
            })();
            if let Err(error) = result {
                return Err(super::IngestionFailure::at(position, error));
            }
        }
        if profile {
            eprintln!(
                "live_aggregate_ingestion_sample rows={} samples={sampled_rows} key_ns={} lookup_ns={} state_ns={} commit_ns={}",
                end_position - start_row,
                key_time.as_nanos(),
                lookup_time.as_nanos(),
                state_time.as_nanos(),
                commit_time.as_nanos(),
            );
        }
        Ok(end_position)
    }

    pub(super) fn prepare_merge_update<'a>(
        &'a mut self,
        key: &KeyWorkspace,
        workspace: &'a mut RowWorkspace,
        source: &Self,
        source_row: usize,
    ) -> Result<PreparedGroup<'a>> {
        let key_ref = key.key()?;
        if key_ref != source.key(source_row)? {
            return Err(index_error("source canonical key mismatch"));
        }
        let hash = key_ref.hash64();
        let (row, new) = match self.find_hashed(key_ref, hash)? {
            Some(row) => (row, false),
            None => {
                self.reserve_index()?;
                (self.append_empty(key)?, true)
            }
        };
        let state =
            match self
                .states
                .prepare_merge_indexed(row, workspace, &source.states, source_row, new)
            {
                Ok(state) => state,
                Err(error) => {
                    if new {
                        self.keys.truncate(row);
                    }
                    return Err(error);
                }
            };
        Ok(PreparedGroup {
            state: Some(state),
            keys: &mut self.keys,
            index: &mut self.index,
            row,
            hash,
            new,
        })
    }

    /// Publish this index in the hash table only after success. Retained capacity
    /// can grow on denial, but no key-only or state-only logical row survives.
    fn append_empty(&mut self, key: &KeyWorkspace) -> Result<usize> {
        self.append_empty_ref(key.key()?)
    }

    fn append_empty_ref(&mut self, key: KeyRef<'_>) -> Result<usize> {
        let previous = self.len();
        let key_index = self.keys.append_ref(key)?;
        match self.states.push_empty() {
            Ok(state_index) if state_index == key_index && state_index == previous => {
                Ok(state_index)
            }
            result => {
                self.keys.truncate(previous);
                self.states.truncate(previous);
                Err(match result {
                    Err(error) => error,
                    Ok(_) => QueryError::Execution("group storage key/state index mismatch".into()),
                })
            }
        }
    }
    fn prepare<'a>(
        &'a mut self,
        row: usize,
        workspace: &'a mut RowWorkspace,
        values: &[ScalarValue],
    ) -> Result<PreparedRow<'a>> {
        self.states.prepare(row, workspace, values)
    }
    pub(super) fn prepare_merge<'a>(
        &'a mut self,
        row: usize,
        workspace: &'a mut RowWorkspace,
        source: &Self,
        source_row: usize,
    ) -> Result<PreparedRow<'a>> {
        if self.keys.key(row)? != source.keys.key(source_row)? {
            return Err(QueryError::Execution(
                "cannot merge aggregate rows with different canonical group keys".into(),
            ));
        }
        self.states
            .prepare_merge(row, workspace, &source.states, source_row)
    }
    pub(super) fn clear(&mut self) {
        if self.index_poisoned {
            self.index = HashTable::new();
            self.index_reservation = None;
            self.index_poisoned = false;
        } else {
            self.index.clear();
        }
        self.keys.truncate(0);
        self.states.truncate(0);
    }
}

fn index_error(message: &str) -> QueryError {
    QueryError::Execution(format!("canonical group index: {message}"))
}

pub(super) struct PreparedGroup<'a> {
    state: Option<PreparedRow<'a>>,
    keys: &'a mut KeyRows,
    index: &'a mut HashTable<(u64, usize)>,
    row: usize,
    hash: u64,
    new: bool,
}

impl PreparedGroup<'_> {
    pub(super) fn commit(mut self) -> usize {
        self.state.as_mut().unwrap().publish();
        if self.new {
            // Capacity was admitted before key/state preparation, and the
            // exclusive borrow prevents intervening insertions. No allocation.
            self.index
                .insert_unique(self.hash, (self.hash, self.row), |entry| entry.0);
        }
        self.new = false;
        self.row
    }
}
impl Drop for PreparedGroup<'_> {
    fn drop(&mut self) {
        self.state = None;
        if self.new {
            self.keys.truncate(self.row);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restored_rows_round_trip_partial_states_and_commit_only_once() {
        use crate::planner::DecimalValue;
        let pool = MemoryPool::new_named("row restore", 131072);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Utf8],
            &[
                (AggregateFunction::Count, DataType::Int64, false),
                (AggregateFunction::Avg, DataType::Float64, false),
                (AggregateFunction::Sum, DataType::Decimal128(38, 2), false),
                (AggregateFunction::Max, DataType::Utf8, false),
            ],
        )
        .unwrap()
        .unwrap();
        let mut source = GroupRows::new(layout.clone()).unwrap();
        let mut restored = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut scratch = layout.row_workspace().unwrap();
        key.encode(&[ScalarValue::Utf8("group".into())]).unwrap();
        let exact = (1i128 << 110) + 17;
        for v in [10.0, 30.0, 30.0, 30.0] {
            source
                .prepare_update(
                    &key,
                    &mut scratch,
                    &[
                        ScalarValue::Int64(1),
                        ScalarValue::Float64(v.into()),
                        ScalarValue::Decimal128(DecimalValue::new(exact, 2)),
                        ScalarValue::Utf8("z".repeat(1000)),
                    ],
                )
                .unwrap()
                .commit();
        }
        key.encode(&[ScalarValue::Null]).unwrap();
        source
            .prepare_update(&key, &mut scratch, &[const { ScalarValue::Null }; 4])
            .unwrap()
            .commit();
        let mut wire = Vec::new();
        source.write_row(0, &mut wire).unwrap();
        let mut null_wire = Vec::new();
        source.write_row(1, &mut null_wire).unwrap();
        drop(source);
        let before = pool.used();
        drop(
            restored
                .prepare_restore(&mut key, &mut scratch, &wire)
                .unwrap(),
        );
        assert_eq!(restored.len(), 0);
        assert_eq!(restored.index.len(), 0);
        assert!(restored.lookup(&key).unwrap().is_none());
        // Retained metadata may grow, but a second discarded preparation keeps
        // usage stable: selected payloads do not escape the transaction.
        assert!(pool.used() >= before);
        let retained = pool.used();
        drop(
            restored
                .prepare_restore(&mut key, &mut scratch, &wire)
                .unwrap(),
        );
        assert_eq!(pool.used(), retained);
        assert_eq!(
            restored
                .prepare_restore(&mut key, &mut scratch, &wire)
                .unwrap()
                .commit(),
            0
        );
        assert!(restored
            .prepare_restore(&mut key, &mut scratch, &wire)
            .is_err());
        assert_eq!(restored.len(), 1);
        assert_eq!(
            restored.value(0, 0).unwrap().as_ref(),
            &ScalarValue::Int64(4)
        );
        assert_eq!(
            restored.value(0, 1).unwrap().as_ref(),
            &ScalarValue::Float64(25.0.into())
        );
        assert_eq!(
            restored.value(0, 2).unwrap().as_ref(),
            &ScalarValue::Decimal128(DecimalValue::new(4 * exact, 2))
        );
        assert_eq!(
            restored
                .prepare_restore(&mut key, &mut scratch, &null_wire)
                .unwrap()
                .commit(),
            1
        );
        assert_eq!(
            restored.value(1, 0).unwrap().as_ref(),
            &ScalarValue::Int64(0)
        );
        for slot in 1..4 {
            assert_eq!(
                restored.value(1, slot).unwrap().as_ref(),
                &ScalarValue::Null
            );
        }
        for (row, expected) in [(0, wire), (1, null_wire)] {
            let mut bytes = Vec::new();
            restored.write_row(row, &mut bytes).unwrap();
            assert_eq!(bytes, expected);
        }
        // A separate run contains the same key with a different AVG weight.
        // Restore into a staging store, then merge its partial state exactly.
        let mut stage = GroupRows::new(layout.clone()).unwrap();
        key.encode(&[ScalarValue::Utf8("group".into())]).unwrap();
        stage
            .prepare_update(
                &key,
                &mut scratch,
                &[
                    ScalarValue::Int64(1),
                    ScalarValue::Float64(10.0.into()),
                    ScalarValue::Decimal128(DecimalValue::new(exact, 2)),
                    ScalarValue::Utf8("a".into()),
                ],
            )
            .unwrap()
            .commit();
        let mut run = Vec::new();
        stage.write_row(0, &mut run).unwrap();
        stage.clear();
        stage
            .prepare_restore(&mut key, &mut scratch, &run)
            .unwrap()
            .commit();
        let pressure = pool.allocate(pool.available()).unwrap();
        let merge = restored
            .prepare_merge_update(&key, &mut scratch, &stage, 0)
            .unwrap();
        drop(stage);
        merge.commit();
        drop(pressure);
        assert_eq!(
            restored.value(0, 0).unwrap().as_ref(),
            &ScalarValue::Int64(5)
        );
        assert_eq!(
            restored.value(0, 1).unwrap().as_ref(),
            &ScalarValue::Float64(22.0.into())
        );
        assert_eq!(
            restored.value(0, 2).unwrap().as_ref(),
            &ScalarValue::Decimal128(DecimalValue::new(5 * exact, 2))
        );
        drop((restored, key, scratch, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn restore_rejects_malformed_late_slots_before_admission_and_rolls_back_pressure() {
        let pool = MemoryPool::new_named("restore rollback", 262144);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Boolean],
            &[
                (AggregateFunction::Max, DataType::Utf8, false),
                (AggregateFunction::Max, DataType::Utf8, false),
                (AggregateFunction::Count, DataType::Int64, false),
            ],
        )
        .unwrap()
        .unwrap();
        let mut source = GroupRows::new(layout.clone()).unwrap();
        let mut target = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut scratch = layout.row_workspace().unwrap();
        key.encode(&[ScalarValue::Boolean(true)]).unwrap();
        source
            .prepare_update(
                &key,
                &mut scratch,
                &[
                    ScalarValue::Utf8("a".repeat(1000)),
                    ScalarValue::Utf8("b".repeat(8192)),
                    ScalarValue::Int64(1),
                ],
            )
            .unwrap()
            .commit();
        let mut wire = Vec::new();
        source.write_row(0, &mut wire).unwrap();
        drop(source);
        target
            .prepare_restore(&mut key, &mut scratch, &wire)
            .unwrap()
            .commit();
        target.clear();
        let pressure = pool.allocate(pool.available()).unwrap();
        let baseline = pool.used();
        for length in 0..wire.len() {
            let error = target
                .prepare_restore(&mut key, &mut scratch, &wire[..length])
                .err()
                .unwrap();
            assert!(!error.is_memory_limit());
            assert_eq!(target.len(), 0);
            assert_eq!(pool.used(), baseline);
        }
        let mut corrupt = wire.clone();
        let flag = corrupt.len() - 10;
        corrupt[flag] = 255;
        assert!(!target
            .prepare_restore(&mut key, &mut scratch, &corrupt)
            .err()
            .unwrap()
            .is_memory_limit());
        drop(pressure);
        // Warm metadata allows the first selected payload, but the later one
        // cannot fit. Its failure must drop all earlier decoded replacements.
        let pressure = pool.allocate(pool.available() - 3000).unwrap();
        let baseline = pool.used();
        assert!(target
            .prepare_restore(&mut key, &mut scratch, &wire)
            .err()
            .unwrap()
            .is_memory_limit());
        assert_eq!(target.len(), 0);
        assert_eq!(target.keys.len(), 0);
        assert_eq!(target.index.len(), 0);
        assert_eq!(pool.used(), baseline);
        drop(pressure);
        target
            .prepare_restore(&mut key, &mut scratch, &wire)
            .unwrap()
            .commit();
        let mut recovered = Vec::new();
        target.write_row(0, &mut recovered).unwrap();
        assert_eq!(recovered, wire);
        drop((target, key, scratch, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn grouped_wire_preserves_partial_algebra_and_selected_bits_under_pressure() {
        use crate::planner::DecimalValue;
        use arrow::datatypes::Field;
        let pool = MemoryPool::new_named("group write", 65536);
        let list_type = DataType::List(Arc::new(Field::new("v", DataType::Float64, true)));
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Float64],
            &[
                (AggregateFunction::Count, DataType::Int64, false),
                (AggregateFunction::Avg, DataType::Float64, false),
                (AggregateFunction::Sum, DataType::Decimal128(38, 2), false),
                (AggregateFunction::AnyValue, list_type, false),
                (AggregateFunction::Max, DataType::Utf8, false),
            ],
        )
        .unwrap()
        .unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut scratch = layout.row_workspace().unwrap();
        let mut groups = GroupRows::new(layout.clone()).unwrap();
        key.encode(&[ScalarValue::Float64((-0.0).into())]).unwrap();
        let exact = (1i128 << 110) + 17;
        let nan = 0xfff8_0000_0000_0042u64;
        for value in [10.0, 30.0, 30.0, 30.0] {
            groups
                .prepare_update(
                    &key,
                    &mut scratch,
                    &[
                        ScalarValue::Int64(1),
                        ScalarValue::Float64(value.into()),
                        ScalarValue::Decimal128(DecimalValue::new(exact, 2)),
                        ScalarValue::List(
                            vec![
                                ScalarValue::Null,
                                ScalarValue::Float64((-0.0).into()),
                                ScalarValue::Float64(f64::from_bits(nan).into()),
                            ],
                            Box::new(DataType::Float64),
                        ),
                        ScalarValue::Utf8("é\0z".into()),
                    ],
                )
                .unwrap()
                .commit();
        }
        // Independently assembled wire bytes: key zero is canonical, selected
        // zero/NaN bits are retained; AVG serializes 100/4, not final 25.
        let mut expected = vec![1];
        expected.extend(9u64.to_le_bytes());
        expected.push(1);
        expected.extend(0u64.to_le_bytes());
        expected.extend([1, 1, 0, 0]);
        expected.extend(4i64.to_le_bytes());
        expected.extend([1, 5, 0, 0]);
        expected.extend(100f64.to_le_bytes());
        expected.extend(4i64.to_le_bytes());
        expected.extend([1, 4, 3, 2]);
        expected.extend((4 * exact).to_le_bytes());
        expected.push(1);
        expected.extend(3u64.to_le_bytes());
        expected.push(0);
        expected.push(1);
        expected.extend((-0.0f64).to_le_bytes());
        expected.push(1);
        expected.extend(nan.to_le_bytes());
        expected.push(1);
        expected.extend(4u64.to_le_bytes());
        expected.extend("é\0z".as_bytes());
        let mut output = vec![0; expected.len()];
        let pressure = pool.allocate(pool.available()).unwrap();
        let mut writer = std::io::Cursor::new(output.as_mut_slice());
        assert_eq!(groups.write_row(0, &mut writer).unwrap(), expected.len());
        assert_eq!(writer.position(), expected.len() as u64);
        assert_eq!(output, expected);
        assert_eq!(groups.value(0, 0).unwrap().as_ref(), &ScalarValue::Int64(4));
        assert_eq!(
            groups.value(0, 1).unwrap().as_ref(),
            &ScalarValue::Float64(25.0.into())
        );
        // At every possible short destination, preserve source and IO identity.
        for limit in 0..expected.len() {
            let mut writer = std::io::Cursor::new(&mut output[..limit]);
            let error = groups.write_row(0, &mut writer).unwrap_err();
            assert!(
                matches!(error, QueryError::Io(ref e) if e.kind() == std::io::ErrorKind::WriteZero)
            );
            assert_eq!(groups.len(), 1);
            assert_eq!(groups.lookup(&key).unwrap(), Some(0));
        }
        let mut writer = std::io::Cursor::new(output.as_mut_slice());
        groups.write_row(0, &mut writer).unwrap();
        assert_eq!(output, expected);
        drop((pressure, groups, scratch, key, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn indexed_partial_merge_preserves_counts_weights_and_payload_ownership() {
        use crate::planner::DecimalValue;
        let pool = MemoryPool::new_named("indexed partial merge", 128 << 10);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Utf8],
            &[
                (AggregateFunction::Count, DataType::Int64, false),
                (AggregateFunction::Avg, DataType::Float64, false),
                (AggregateFunction::Sum, DataType::Decimal128(38, 2), false),
                (AggregateFunction::Max, DataType::Utf8, false),
            ],
        )
        .unwrap()
        .unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut scratch = layout.row_workspace().unwrap();
        let mut first = GroupRows::new(layout.clone()).unwrap();
        let mut second = GroupRows::new(layout.clone()).unwrap();
        let mut target = GroupRows::new(layout.clone()).unwrap();
        key.encode(&[ScalarValue::Utf8("group".into())]).unwrap();
        let exact = (1i128 << 100) + 17;
        first
            .prepare_update(
                &key,
                &mut scratch,
                &[
                    ScalarValue::Int64(1),
                    ScalarValue::Float64(10.0.into()),
                    ScalarValue::Decimal128(DecimalValue::new(exact, 2)),
                    ScalarValue::Utf8("a".into()),
                ],
            )
            .unwrap()
            .commit();
        for _ in 0..3 {
            second
                .prepare_update(
                    &key,
                    &mut scratch,
                    &[
                        ScalarValue::Int64(1),
                        ScalarValue::Float64(30.0.into()),
                        ScalarValue::Decimal128(DecimalValue::new(3 * exact, 2)),
                        ScalarValue::Utf8("z".repeat(1000)),
                    ],
                )
                .unwrap()
                .commit();
        }
        drop(
            target
                .prepare_merge_update(&key, &mut scratch, &first, 0)
                .unwrap(),
        );
        assert_eq!(target.len(), 0);
        assert_eq!(target.lookup(&key).unwrap(), None);
        assert_eq!(
            target
                .prepare_merge_update(&key, &mut scratch, &first, 0)
                .unwrap()
                .commit(),
            0
        );
        let pressure = pool.allocate(pool.available()).unwrap();
        let prepared = target
            .prepare_merge_update(&key, &mut scratch, &second, 0)
            .unwrap();
        drop(second); // The pending selected value must retain the source lease.
        assert_eq!(prepared.commit(), 0);
        assert_eq!(target.len(), 1);
        for (slot, expected) in [
            ScalarValue::Int64(4),
            ScalarValue::Float64(25.0.into()),
            ScalarValue::Decimal128(DecimalValue::new(10 * exact, 2)),
            ScalarValue::Utf8("z".repeat(1000)),
        ]
        .iter()
        .enumerate()
        {
            assert_eq!(target.value(0, slot).unwrap().as_ref(), expected);
        }
        drop(pressure);
        key.encode(&[ScalarValue::Utf8("different".into())])
            .unwrap();
        assert!(target
            .prepare_merge_update(&key, &mut scratch, &first, 0)
            .is_err());
        assert_eq!(target.len(), 1);
        assert_eq!(target.lookup(&key).unwrap(), None);
        drop((target, first, key, scratch, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn collision_chains_reuse_only_exact_groups_even_with_a_full_pool() {
        let pool = MemoryPool::new_named("collision groups", 8 << 20);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Utf8],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut scratch = layout.row_workspace().unwrap();
        let mut rows = GroupRows::new(layout.clone()).unwrap();
        for number in 0..300 {
            key.encode(&[ScalarValue::Utf8(format!("g{number:04}"))])
                .unwrap();
            assert_eq!(
                rows.prepare_hashed(&key, 0, &mut scratch, &[ScalarValue::Int64(1)])
                    .unwrap()
                    .commit(),
                number
            );
        }
        let pressure = pool.allocate(pool.available()).unwrap();
        for number in (0..300).rev() {
            key.encode(&[ScalarValue::Utf8(format!("g{number:04}"))])
                .unwrap();
            for _ in 0..2 {
                assert_eq!(
                    rows.prepare_hashed(&key, 0, &mut scratch, &[ScalarValue::Int64(1)])
                        .unwrap()
                        .commit(),
                    number
                );
            }
        }
        assert_eq!(rows.len(), 300);
        assert_eq!(rows.index.len(), 300);
        for row in 0..300 {
            assert_eq!(rows.value(row, 0).unwrap().as_ref(), &ScalarValue::Int64(3));
        }
        drop((rows, key, scratch, layout, pressure));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn failed_or_discarded_new_rows_never_publish_an_index_entry() {
        let pool = MemoryPool::new_named("indexed rollback", 65536);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Boolean],
            &[
                (AggregateFunction::Count, DataType::Int64, false),
                (AggregateFunction::Max, DataType::Utf8, false),
            ],
        )
        .unwrap()
        .unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut scratch = layout.row_workspace().unwrap();
        let mut rows = GroupRows::new(layout.clone()).unwrap();
        key.encode(&[ScalarValue::Boolean(false)]).unwrap();
        rows.prepare_update(
            &key,
            &mut scratch,
            &[ScalarValue::Int64(1), ScalarValue::Utf8("old".into())],
        )
        .unwrap()
        .commit();
        key.encode(&[ScalarValue::Boolean(true)]).unwrap();
        let pressure = pool.allocate(pool.available() - 2500).unwrap();
        let error = rows
            .prepare_update(
                &key,
                &mut scratch,
                &[ScalarValue::Int64(1), ScalarValue::Utf8("z".repeat(8192))],
            )
            .err()
            .unwrap();
        assert!(error.is_memory_limit());
        assert_eq!(rows.lookup(&key).unwrap(), None);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows.keys.len(), 1);
        assert_eq!(rows.index.len(), 1);
        assert_eq!(rows.value(0, 0).unwrap().as_ref(), &ScalarValue::Int64(1));
        assert_eq!(
            rows.value(0, 1).unwrap().as_ref(),
            &ScalarValue::Utf8("old".into())
        );
        drop(pressure);
        drop(
            rows.prepare_update(
                &key,
                &mut scratch,
                &[ScalarValue::Int64(1), ScalarValue::Utf8("new".into())],
            )
            .unwrap(),
        );
        assert_eq!(rows.lookup(&key).unwrap(), None);
        assert_eq!(rows.len(), 1);
        // Wrong arity also removes a newly appended but unpublished state row.
        assert!(rows.prepare_update(&key, &mut scratch, &[]).is_err());
        assert_eq!(rows.lookup(&key).unwrap(), None);
        assert_eq!(rows.keys.len(), rows.len());
        key.encode(&[ScalarValue::Boolean(false)]).unwrap();
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(rows
            .prepare_update(
                &key,
                &mut scratch,
                &[ScalarValue::Int64(1), ScalarValue::Utf8("z".repeat(1000))]
            )
            .err()
            .unwrap()
            .is_memory_limit());
        assert_eq!(rows.lookup(&key).unwrap(), Some(0));
        assert_eq!(rows.value(0, 0).unwrap().as_ref(), &ScalarValue::Int64(1));
        drop(pressure);
        rows.clear();
        assert_eq!(rows.len(), 0);
        assert_eq!(rows.index.len(), 0);
        let pressure = pool.allocate(pool.available()).unwrap();
        assert_eq!(
            rows.prepare_update(&key, &mut scratch, &[ScalarValue::Null, ScalarValue::Null])
                .unwrap()
                .commit(),
            0
        );
        assert_eq!(rows.lookup(&key).unwrap(), Some(0));
        assert_eq!(rows.value(0, 0).unwrap().as_ref(), &ScalarValue::Int64(0));
        drop((rows, key, scratch, layout, pressure));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn canonical_hash_metadata_growth_stays_within_the_pinned_bound() {
        let pool = MemoryPool::new_named("canonical hash growth", 8 << 20);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut scratch = layout.row_workspace().unwrap();
        let mut rows = GroupRows::new(layout.clone()).unwrap();
        for number in 0..20000 {
            key.encode(&[ScalarValue::Int64(number)]).unwrap();
            let old_capacity = rows.index.capacity();
            assert_eq!(
                rows.prepare_update(&key, &mut scratch, &[ScalarValue::Int64(1)])
                    .unwrap()
                    .commit(),
                number as usize
            );
            if old_capacity != rows.index.capacity() {
                assert!(
                    rows.index.allocation_size()
                        <= super::super::raw_state::index_bound(rows.len()).unwrap()
                );
                assert_eq!(
                    rows.index_reservation.as_ref().unwrap().size(),
                    512 + rows.index.allocation_size()
                );
            }
        }
        assert_eq!(rows.len(), 20000);
        for number in 0..20000 {
            key.encode(&[ScalarValue::Int64(number)]).unwrap();
            assert_eq!(rows.lookup(&key).unwrap(), Some(number as usize));
        }
        drop((rows, key, scratch, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn clearing_group_rows_releases_selected_payloads_and_reuses_capacity() {
        let pool = MemoryPool::new_named("group clear", 65536);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Utf8],
            &[(AggregateFunction::Max, DataType::Utf8, false)],
        )
        .unwrap()
        .unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut scratch = layout.row_workspace().unwrap();
        let mut rows = GroupRows::new(layout.clone()).unwrap();
        key.encode(&[ScalarValue::Utf8("group".into())]).unwrap();
        rows.append_empty(&key).unwrap();
        let baseline = pool.used();
        let value = ScalarValue::Utf8("x".repeat(8192));
        rows.prepare(0, &mut scratch, std::slice::from_ref(&value))
            .unwrap()
            .commit();
        assert!(pool.used() >= baseline + 8192);
        assert_eq!(rows.value(0, 0).unwrap().as_ref(), &value);
        rows.clear();
        assert_eq!(rows.len(), 0);
        assert!(rows.key(0).is_err());
        assert!(rows.value(0, 0).is_err());
        assert_eq!(
            pool.used(),
            baseline,
            "capacity remains owned, selected payload is released"
        );
        let pressure = pool.allocate(pool.available()).unwrap();
        assert_eq!(rows.append_empty(&key).unwrap(), 0);
        assert_eq!(rows.value(0, 0).unwrap().as_ref(), &ScalarValue::Null);
        drop((rows, key, scratch, layout, pressure));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn state_denial_rolls_back_an_already_appended_key() {
        let pool = MemoryPool::new_named("atomic group insertion", 65536);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Boolean],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut scratch = layout.row_workspace().unwrap();
        let mut rows = GroupRows::new(layout.clone()).unwrap();
        key.encode(&[ScalarValue::Boolean(true)]).unwrap();
        // Enough for the first key (2 bytes) and end offset (8 bytes), but
        // no room for the first aggregate-state row.
        let pressure = pool.allocate(pool.available() - 10).unwrap();
        assert!(rows.append_empty(&key).unwrap_err().is_memory_limit());
        assert_eq!(rows.len(), 0);
        assert!(rows.key(0).is_err());
        assert_eq!(rows.keys.len(), rows.states.len());
        drop(pressure);
        assert_eq!(rows.append_empty(&key).unwrap(), 0);
        rows.prepare(0, &mut scratch, &[ScalarValue::Int64(1)])
            .unwrap()
            .commit();
        assert_eq!(rows.value(0, 0).unwrap().as_ref(), &ScalarValue::Int64(1));
        assert!(rows.key(0).unwrap() == key.key().unwrap());
        rows.clear();
        assert_eq!(rows.len(), 0);
        assert_eq!(rows.append_empty(&key).unwrap(), 0);
        assert_eq!(rows.value(0, 0).unwrap().as_ref(), &ScalarValue::Int64(0));
        drop((rows, key, scratch, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn merge_checks_full_keys_before_changing_aggregate_state() {
        let pool = MemoryPool::new_named("exact group merge", 65536);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Utf8, DataType::Utf8],
            &[(AggregateFunction::Count, DataType::Int64, false)],
        )
        .unwrap()
        .unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut scratch = layout.row_workspace().unwrap();
        let mut left = GroupRows::new(layout.clone()).unwrap();
        let mut right = GroupRows::new(layout.clone()).unwrap();
        key.encode(&[
            ScalarValue::Utf8("a".into()),
            ScalarValue::Utf8("bc".into()),
        ])
        .unwrap();
        left.append_empty(&key).unwrap();
        right.append_empty(&key).unwrap();
        key.encode(&[
            ScalarValue::Utf8("ab".into()),
            ScalarValue::Utf8("c".into()),
        ])
        .unwrap();
        right.append_empty(&key).unwrap();
        left.prepare(0, &mut scratch, &[ScalarValue::Int64(1)])
            .unwrap()
            .commit();
        for row in 0..2 {
            for _ in 0..2 {
                right
                    .prepare(row, &mut scratch, &[ScalarValue::Int64(1)])
                    .unwrap()
                    .commit();
            }
        }
        let error = left
            .prepare_merge(0, &mut scratch, &right, 1)
            .err()
            .unwrap();
        assert!(error.to_string().contains("different canonical group keys"));
        assert_eq!(left.value(0, 0).unwrap().as_ref(), &ScalarValue::Int64(1));
        let pressure = pool.allocate(pool.available()).unwrap();
        left.prepare_merge(0, &mut scratch, &right, 0)
            .unwrap()
            .commit();
        assert_eq!(left.value(0, 0).unwrap().as_ref(), &ScalarValue::Int64(3));
        drop((left, right, key, scratch, layout, pressure));
        assert_eq!(pool.used(), 0);
    }
}

#[cfg(test)]
mod arrow_ingestion_tests {
    use super::*;
    use arrow::{
        array::*,
        datatypes::{Field, Int64Type, Schema},
        record_batch::RecordBatch,
    };

    fn batch(columns: Vec<ArrayRef>) -> RecordBatch {
        let fields: Vec<_> = columns
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn evaluated_ingestion_preserves_exact_values_and_borrowed_selected_payloads() {
        use crate::planner::DecimalValue;
        let pool = MemoryPool::new_named("evaluated grouped rows", 262144);
        let exact = (1i128 << 80) + 17;
        let input = batch(vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1), None, None])),
            Arc::new(StringArray::from(vec![
                Some("x"),
                None,
                Some("z"),
                Some("a"),
            ])),
            Arc::new(
                Decimal128Array::from(vec![Some(exact), Some(exact * 3), Some(-exact), None])
                    .with_precision_and_scale(38, 2)
                    .unwrap(),
            ),
            Arc::new(Float64Array::from(vec![
                Some(10.0),
                Some(30.0),
                None,
                Some(50.0),
            ])),
            Arc::new(StringArray::from(vec!["x", "a", "z", "b"])),
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>([
                Some(vec![Some(7), None]),
                Some(vec![Some(99)]),
                None,
                Some(vec![]),
            ])),
        ]);
        let functions = [
            AggregateFunction::Count,
            AggregateFunction::Sum,
            AggregateFunction::Avg,
            AggregateFunction::Min,
            AggregateFunction::AnyValue,
        ];
        let states: Vec<_> = functions
            .iter()
            .zip(&input.columns()[1..])
            .map(|(f, a)| (*f, a.data_type().clone(), false))
            .collect();
        let layout = GroupLayout::bind(&pool, &[DataType::Int64], &states)
            .unwrap()
            .unwrap();
        let mut rows = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut scratch = layout.row_workspace().unwrap();
        assert_eq!(
            rows.process_evaluated_from(&input, 1, 0, &mut key, &mut scratch)
                .unwrap(),
            4
        );
        assert_eq!(rows.len(), 2);
        for (group, count, sum, avg, min, list) in [
            (
                ScalarValue::Int64(1),
                1,
                exact * 4,
                20.0,
                "a",
                vec![ScalarValue::Int64(7), ScalarValue::Null],
            ),
            (ScalarValue::Null, 2, -exact, 50.0, "b", vec![]),
        ] {
            key.encode(&[group]).unwrap();
            let row = rows.lookup(&key).unwrap().unwrap();
            assert_eq!(*rows.value(row, 0).unwrap(), ScalarValue::Int64(count));
            assert_eq!(
                *rows.value(row, 1).unwrap(),
                ScalarValue::Decimal128(DecimalValue::new(sum, 2))
            );
            assert_eq!(
                *rows.value(row, 2).unwrap(),
                ScalarValue::Float64(avg.into())
            );
            assert_eq!(*rows.value(row, 3).unwrap(), ScalarValue::Utf8(min.into()));
            assert_eq!(
                *rows.value(row, 4).unwrap(),
                ScalarValue::List(list, Box::new(DataType::Int64))
            );
        }
        drop((rows, key, scratch, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn evaluated_empty_batches_validate_schema_and_selected_float_bits_survive() {
        let pool = MemoryPool::new_named("evaluated selected bits", 65536);
        let layout = GroupLayout::bind(
            &pool,
            &[DataType::Int64],
            &[
                (AggregateFunction::AnyValue, DataType::Float64, false),
                (AggregateFunction::Min, DataType::Float64, false),
            ],
        )
        .unwrap()
        .unwrap();
        let mut rows = GroupRows::new(layout.clone()).unwrap();
        let mut key = layout.key_workspace().unwrap();
        let mut scratch = layout.row_workspace().unwrap();
        let nan = f64::from_bits(0xfff8_0000_0000_0017);
        let input = batch(vec![
            Arc::new(Int64Array::from(vec![1, 1])),
            Arc::new(Float64Array::from(vec![nan, 0.0])),
            Arc::new(Float64Array::from(vec![-0.0, 0.0])),
        ]);
        rows.process_evaluated_from(&input, 1, 0, &mut key, &mut scratch)
            .unwrap();
        for (slot, bits) in [(0, nan.to_bits()), (1, (-0.0f64).to_bits())] {
            let value = rows.value(0, slot).unwrap();
            let ScalarValue::Float64(value) = value.as_ref() else {
                panic!("wrong selected type")
            };
            assert_eq!(value.into_inner().to_bits(), bits);
        }
        assert_eq!(
            rows.process_evaluated_from(&input.slice(0, 0), 1, 0, &mut key, &mut scratch)
                .unwrap(),
            0
        );
        let wrong = batch(vec![
            Arc::new(Int64Array::from(Vec::<i64>::new())),
            Arc::new(Int64Array::from(Vec::<i64>::new())),
        ]);
        assert!(rows
            .process_evaluated_from(&wrong, 1, 0, &mut key, &mut scratch)
            .is_err());
        assert_eq!(rows.len(), 1);
        drop((rows, key, scratch, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn evaluated_cursor_rolls_back_late_denial_and_resumes_without_duplicate_updates() {
        for new_group in [false, true] {
            let pool = MemoryPool::new_named("evaluated cursor", 65536);
            let layout = GroupLayout::bind(
                &pool,
                &[DataType::Int64],
                &[
                    (AggregateFunction::Count, DataType::Int64, false),
                    (AggregateFunction::Max, DataType::Utf8, false),
                ],
            )
            .unwrap()
            .unwrap();
            let mut rows = GroupRows::new(layout.clone()).unwrap();
            let mut key = layout.key_workspace().unwrap();
            let mut scratch = layout.row_workspace().unwrap();
            let seed = batch(vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["b"])),
            ]);
            rows.process_evaluated_from(&seed, 1, 0, &mut key, &mut scratch)
                .unwrap();
            let large = "z".repeat(4096);
            let input = batch(vec![
                Arc::new(Int64Array::from(vec![
                    Some(1),
                    if new_group { None } else { Some(1) },
                ])),
                Arc::new(Int64Array::from(vec![1, 1])),
                Arc::new(StringArray::from(vec!["a", large.as_str()])),
            ]);
            let pressure = pool.allocate(pool.available() - 1024).unwrap();
            let failure = rows
                .process_evaluated_from(&input, 1, 0, &mut key, &mut scratch)
                .unwrap_err();
            assert!(failure.error.is_memory_limit());
            assert_eq!(failure.next_row, 1);
            assert_eq!(rows.len(), 1);
            assert_eq!(*rows.value(0, 0).unwrap(), ScalarValue::Int64(2));
            assert_eq!(*rows.value(0, 1).unwrap(), ScalarValue::Utf8("b".into()));
            drop(pressure);
            rows.process_evaluated_from(&input, 1, failure.next_row, &mut key, &mut scratch)
                .unwrap();
            key.encode(&[if new_group {
                ScalarValue::Null
            } else {
                ScalarValue::Int64(1)
            }])
            .unwrap();
            let row = rows.lookup(&key).unwrap().unwrap();
            assert_eq!(
                *rows.value(row, 0).unwrap(),
                ScalarValue::Int64(if new_group { 1 } else { 3 })
            );
            assert_eq!(*rows.value(row, 1).unwrap(), ScalarValue::Utf8(large));
            drop((rows, key, scratch, layout));
            assert_eq!(pool.used(), 0);
        }
    }
}
