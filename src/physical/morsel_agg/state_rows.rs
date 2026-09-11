//! Contiguous partial aggregate rows with reusable preparation scratch.
//! No fixed or selected slot changes until preparation of the whole row succeeds.
mod array_view;
use array_view::FixedArrayView;

// Bound stack storage; wider layouts keep the checked generic adapter.
const INLINE_ARRAY_VIEWS: usize = 16;

use super::{
    fixed_cell::FixedCell,
    scalar_state_codec,
    selected_state::{matches_type, supported_type, BoundSelection, Payload, SelectedState},
    state_codec::FixedStateCodec,
    AccumulatorState,
};
use crate::execution::{reserved_scalar::ReservedDataType, reserved_vec::ReservedVec, MemoryPool};
use crate::planner::{AggregateFunction, ScalarValue};
use crate::{QueryError, Result};
use arrow::datatypes::DataType;
use std::{borrow::Cow, io::Write, sync::Arc};

enum BoundSlot {
    Fixed {
        function: AggregateFunction,
        codec: FixedStateCodec,
        input: ReservedDataType,
        index: usize,
    },
    Selected {
        layout: Arc<BoundSelection>,
        index: usize,
    },
}

pub(super) struct StateRowLayout {
    slots: ReservedVec<BoundSlot>,
    fixed: usize,
    selected: usize,
    pool: MemoryPool,
}

fn invalid(message: &str) -> QueryError {
    QueryError::Execution(format!("partial aggregate row: {message}"))
}

fn numeric(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal128(..)
    )
}

impl StateRowLayout {
    /// Check the whole row before any retained-value admission. A malformed
    /// later slot must not be hidden by pressure while decoding an earlier one.
    pub(super) fn validate_encoded(&self, mut bytes: &[u8]) -> Result<()> {
        for slot in self.slots.as_slice() {
            let consumed = match slot {
                BoundSlot::Fixed { codec, .. } => {
                    decode_fixed(*codec, bytes)?;
                    codec.width()
                }
                BoundSlot::Selected { layout, .. } => layout.validate_payload(bytes)?,
            };
            bytes = &bytes[consumed..];
        }
        if !bytes.is_empty() {
            return Err(invalid("trailing state-row bytes"));
        }
        Ok(())
    }
    /// This binds physical states and scalar input domains. Full query expression
    /// identity, canonical keys and on-disk layout identity are separate contracts.
    pub(super) fn bind(
        pool: &MemoryPool,
        inputs: &[(AggregateFunction, DataType, bool)],
    ) -> Result<Option<Arc<Self>>> {
        if inputs.is_empty() {
            return Ok(None);
        }
        let mut slots = ReservedVec::with_capacity(pool, inputs.len())?;
        let (mut fixed, mut selected) = (0, 0);
        for (function, input, distinct) in inputs {
            if *distinct || !supported_type(input, true, 0) {
                return Ok(None);
            }
            let slot = if let Some(codec) = FixedStateCodec::bind(function, input, *distinct) {
                let compatible = match codec {
                    FixedStateCodec::Count => true,
                    FixedStateCodec::BoolAnd | FixedStateCodec::BoolOr => {
                        input == &DataType::Boolean
                    }
                    _ => numeric(input),
                };
                if !compatible {
                    return Ok(None);
                }
                let slot = BoundSlot::Fixed {
                    function: *function,
                    codec,
                    input: ReservedDataType::try_copy(pool, input)?,
                    index: fixed,
                };
                fixed += 1;
                slot
            } else {
                let Some(layout) = BoundSelection::bind(pool, *function, input, *distinct)? else {
                    return Ok(None);
                };
                let slot = BoundSlot::Selected {
                    layout,
                    index: selected,
                };
                selected += 1;
                slot
            };
            slots.extend_reserved(1, std::iter::once(slot))?;
        }
        Ok(Some(Arc::new(Self {
            slots,
            fixed,
            selected,
            pool: pool.clone(),
        })))
    }

    fn empty_fixed(&self) -> impl Iterator<Item = FixedCell> + '_ {
        self.slots.as_slice().iter().filter_map(|slot| match slot {
            BoundSlot::Fixed { codec, .. } => Some(FixedCell::empty(*codec)),
            _ => None,
        })
    }

    fn empty_selected(&self) -> impl Iterator<Item = SelectedState> + '_ {
        self.slots.as_slice().iter().filter_map(|slot| match slot {
            BoundSlot::Selected { layout, .. } => Some(SelectedState::new(layout.clone())),
            _ => None,
        })
    }
}

/// Group rows share two flat admitted allocations. There is no Vec per group.
pub(super) struct StateRows {
    layout: Arc<StateRowLayout>,
    fixed: ReservedVec<FixedCell>,
    selected: ReservedVec<SelectedState>,
    rows: usize,
}

/// Borrowed immutable arrays whose arity, logical types and common extent have
/// been checked against this exact layout. No allocation or array copy occurs.
/// Plain fixed views bind once into bounded stack storage; wider layouts and
/// dictionaries retain the checked adapter. Transactional updates remain per row.
pub(super) struct BoundArrayInputs<'a> {
    layout: Arc<StateRowLayout>,
    arrays: &'a [arrow::array::ArrayRef],
    fixed_views: [Option<FixedArrayView<'a>>; INLINE_ARRAY_VIEWS],
    rows: usize,
}

/// Fixed/selected scratch is preallocated. Variable Arrow winner encoding uses
/// reusable, separately admitted byte scratch.
pub(super) struct RowWorkspace {
    layout: Arc<StateRowLayout>,
    fixed: ReservedVec<FixedCell>,
    selected: ReservedVec<Option<Payload>>,
    scalar_bytes: ReservedVec<u8>,
}

impl RowWorkspace {
    pub(super) fn new(layout: Arc<StateRowLayout>) -> Result<Self> {
        let mut fixed = ReservedVec::with_capacity(&layout.pool, layout.fixed)?;
        fixed.extend_reserved(layout.fixed, layout.empty_fixed())?;
        let mut selected = ReservedVec::with_capacity(&layout.pool, layout.selected)?;
        selected.extend_reserved(layout.selected, (0..layout.selected).map(|_| None))?;
        let scalar_bytes = ReservedVec::with_capacity(&layout.pool, 0)?;
        Ok(Self {
            layout,
            fixed,
            selected,
            scalar_bytes,
        })
    }

    fn clear_pending(&mut self) {
        for pending in self.selected.as_mut_slice() {
            *pending = None;
        }
    }
}

impl StateRows {
    pub(super) fn output_type(&self, slot: usize) -> Result<Cow<'_, DataType>> {
        let slot = self
            .layout
            .slots
            .as_slice()
            .get(slot)
            .ok_or_else(|| invalid("output slot out of range"))?;
        Ok(match slot {
            BoundSlot::Selected { layout, .. } => Cow::Borrowed(layout.input_type()),
            BoundSlot::Fixed { codec, input, .. } => Cow::Owned(match codec {
                FixedStateCodec::Count | FixedStateCodec::SumInt => DataType::Int64,
                FixedStateCodec::Sum | FixedStateCodec::Avg | FixedStateCodec::Variance => {
                    DataType::Float64
                }
                FixedStateCodec::BoolAnd | FixedStateCodec::BoolOr => DataType::Boolean,
                FixedStateCodec::SumDecimal(scale) => {
                    if matches!(
                        input.as_type(),
                        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64
                    ) {
                        DataType::UInt64
                    } else {
                        DataType::Decimal128(38, *scale)
                    }
                }
            }),
        })
    }
    pub(super) fn slot_count(&self) -> usize {
        self.layout.slots.as_slice().len()
    }
    /// Restore into a fresh unpublished row. The token owns rollback of all
    /// pending payloads and the appended row until the caller commits it.
    pub(super) fn prepare_restore<'a>(
        &'a mut self,
        workspace: &'a mut RowWorkspace,
        mut bytes: &[u8],
    ) -> Result<PreparedRow<'a>> {
        self.layout.validate_encoded(bytes)?;
        let row = self.push_empty()?;
        let prepared = self.begin(row, workspace, Some(row))?;
        for slot in prepared.rows.layout.slots.as_slice() {
            let consumed = match slot {
                BoundSlot::Fixed { codec, index, .. } => {
                    prepared.workspace.fixed.as_mut_slice()[*index] =
                        decode_fixed(*codec, bytes)?.try_into()?;
                    codec.width()
                }
                BoundSlot::Selected { layout, index } => {
                    let (payload, consumed) = layout.decode_payload(bytes)?;
                    prepared.workspace.selected.as_mut_slice()[*index] = payload;
                    consumed
                }
            };
            bytes = &bytes[consumed..];
        }
        Ok(prepared)
    }
    /// Validate every slot before the first output byte. The file envelope must
    /// bind logical slot order/types/functions to this payload representation.
    pub(super) fn encoded_size(&self, row: usize) -> Result<usize> {
        if row >= self.rows {
            return Err(invalid("row index out of range"));
        }
        self.layout
            .slots
            .as_slice()
            .iter()
            .try_fold(0, |size, slot| {
                let width = match slot {
                    BoundSlot::Fixed { codec, index, .. } => {
                        codec.encode(
                            &self.fixed.as_slice()[row * self.layout.fixed + index].state(),
                        )?;
                        codec.width()
                    }
                    BoundSlot::Selected { index, .. } => scalar_state_codec::size(
                        self.selected.as_slice()[row * self.layout.selected + index]
                            .value()
                            .unwrap_or(&ScalarValue::Null),
                        0,
                    )?,
                };
                scalar_state_codec::add(size, width)
            })
    }

    /// No finalization or source mutation. IO failure leaves source rows intact;
    /// the file owner must discard any incomplete output before releasing them.
    pub(super) fn write_to(&self, row: usize, writer: &mut impl Write) -> Result<()> {
        self.encoded_size(row)?;
        for slot in self.layout.slots.as_slice() {
            match slot {
                BoundSlot::Fixed { codec, index, .. } => {
                    let frame = codec
                        .encode(&self.fixed.as_slice()[row * self.layout.fixed + index].state())?;
                    writer.write_all(&frame[..codec.width()])?;
                }
                BoundSlot::Selected { index, .. } => scalar_state_codec::write(
                    self.selected.as_slice()[row * self.layout.selected + index]
                        .value()
                        .unwrap_or(&ScalarValue::Null),
                    writer,
                )?,
            }
        }
        Ok(())
    }

    pub(super) fn new(layout: Arc<StateRowLayout>) -> Result<Self> {
        let fixed = ReservedVec::with_capacity(&layout.pool, 0)?;
        let selected = ReservedVec::with_capacity(&layout.pool, 0)?;
        Ok(Self {
            layout,
            fixed,
            selected,
            rows: 0,
        })
    }

    pub(super) fn len(&self) -> usize {
        self.rows
    }

    /// Roll back an unpublished suffix or clear rows after a completed flush.
    pub(super) fn truncate(&mut self, rows: usize) {
        if rows >= self.rows {
            return;
        }
        self.fixed.truncate(rows * self.layout.fixed);
        self.selected.truncate(rows * self.layout.selected);
        self.rows = rows;
    }

    /// Finalize only after all partial rows have merged. Selected payloads stay
    /// borrowed from their owners; output builders must admit their own copies.
    pub(super) fn value(&self, row: usize, slot: usize) -> Result<Cow<'_, ScalarValue>> {
        if row >= self.rows {
            return Err(invalid("row index out of range"));
        }
        match self
            .layout
            .slots
            .as_slice()
            .get(slot)
            .ok_or_else(|| invalid("slot index out of range"))?
        {
            BoundSlot::Fixed {
                function, index, ..
            } => Ok(Cow::Owned(
                self.fixed.as_slice()[row * self.layout.fixed + index].finalize(function)?,
            )),
            BoundSlot::Selected { index, .. } => Ok(Cow::Borrowed(
                self.selected.as_slice()[row * self.layout.selected + index]
                    .value()
                    .unwrap_or(&ScalarValue::Null),
            )),
        }
    }

    pub(super) fn push_empty(&mut self) -> Result<usize> {
        let next = self
            .rows
            .checked_add(1)
            .ok_or_else(|| invalid("row count overflow"))?;
        // Both arrays must admit growth before either gains logical state.
        self.fixed.reserve(self.layout.fixed)?;
        self.selected.reserve(self.layout.selected)?;
        let old_fixed = self.fixed.as_slice().len();
        self.fixed
            .extend_reserved(self.layout.fixed, self.layout.empty_fixed())?;
        if let Err(error) = self
            .selected
            .extend_reserved(self.layout.selected, self.layout.empty_selected())
        {
            self.fixed.truncate(old_fixed);
            return Err(error);
        }
        let index = self.rows;
        self.rows = next;
        Ok(index)
    }

    fn begin<'a>(
        &'a mut self,
        row: usize,
        workspace: &'a mut RowWorkspace,
        rollback_to: Option<usize>,
    ) -> Result<PreparedRow<'a>> {
        if rollback_to.is_some() && row.checked_add(1) != Some(self.rows) {
            return Err(invalid("rollback requires the newly appended last row"));
        }
        let prepared = PreparedRow {
            rows: self,
            workspace,
            row,
            rollback_to,
        };
        if row >= prepared.rows.rows {
            return Err(invalid("row index out of range"));
        }
        if !Arc::ptr_eq(&prepared.rows.layout, &prepared.workspace.layout) {
            return Err(invalid("workspace layout mismatch"));
        }
        prepared.workspace.clear_pending();
        let offset = row * prepared.rows.layout.fixed;
        prepared.workspace.fixed.as_mut_slice().copy_from_slice(
            &prepared.rows.fixed.as_slice()[offset..offset + prepared.rows.layout.fixed],
        );
        Ok(prepared)
    }

    pub(super) fn prepare<'a>(
        &'a mut self,
        row: usize,
        workspace: &'a mut RowWorkspace,
        values: &[ScalarValue],
    ) -> Result<PreparedRow<'a>> {
        self.prepare_indexed(row, workspace, values, false)
    }

    /// A new, unpublished group is removed on preparation failure or token drop.
    pub(super) fn prepare_indexed<'a>(
        &'a mut self,
        row: usize,
        workspace: &'a mut RowWorkspace,
        values: &[ScalarValue],
        new: bool,
    ) -> Result<PreparedRow<'a>> {
        let prepared = self.begin(row, workspace, new.then_some(row))?;
        if values.len() != prepared.rows.layout.slots.as_slice().len() {
            return Err(invalid("input arity mismatch"));
        }
        for (slot, value) in prepared.rows.layout.slots.as_slice().iter().zip(values) {
            match slot {
                BoundSlot::Fixed { input, index, .. } => {
                    if !matches_type(value, input.as_type(), 0) {
                        return Err(invalid("input type mismatch"));
                    }
                    update_fixed(&mut prepared.workspace.fixed.as_mut_slice()[*index], value)?;
                }
                BoundSlot::Selected { index, .. } => {
                    let state = &prepared.rows.selected.as_slice()
                        [row * prepared.rows.layout.selected + index];
                    prepared.workspace.selected.as_mut_slice()[*index] =
                        state.prepare_replacement(value)?;
                }
            }
        }
        Ok(prepared)
    }

    pub(super) fn bind_arrays<'a>(
        &self,
        arrays: &'a [arrow::array::ArrayRef],
        rows: usize,
    ) -> Result<BoundArrayInputs<'a>> {
        if arrays.len() != self.layout.slots.as_slice().len() {
            return Err(invalid("Arrow input arity mismatch"));
        }
        for (slot, array) in self.layout.slots.as_slice().iter().zip(arrays) {
            let ty = match slot {
                BoundSlot::Fixed { input, .. } => input.as_type(),
                BoundSlot::Selected { layout, .. } => layout.input_type(),
            };
            if !super::key_rows::arrow_input::type_matches(array.data_type(), ty)
                || array.len() != rows
            {
                return Err(invalid("Arrow input type or extent mismatch"));
            }
        }
        let mut fixed_views = [None; INLINE_ARRAY_VIEWS];
        for ((view, slot), array) in fixed_views
            .iter_mut()
            .zip(self.layout.slots.as_slice())
            .zip(arrays)
        {
            if let BoundSlot::Fixed { codec, .. } = slot {
                *view =
                    FixedArrayView::bind(array.as_ref(), matches!(codec, FixedStateCodec::Count));
            }
        }
        Ok(BoundArrayInputs {
            layout: Arc::clone(&self.layout),
            arrays,
            fixed_views,
            rows,
        })
    }

    pub(super) fn prepare_arrays_indexed<'a>(
        &'a mut self,
        row: usize,
        workspace: &'a mut RowWorkspace,
        inputs: &BoundArrayInputs<'_>,
        input_row: usize,
        new: bool,
    ) -> Result<PreparedRow<'a>> {
        let prepared = self.begin(row, workspace, new.then_some(row))?;
        if !Arc::ptr_eq(&prepared.rows.layout, &inputs.layout) || input_row >= inputs.rows {
            return Err(invalid("bound Arrow input layout or extent mismatch"));
        }
        for (position, (slot, array)) in prepared
            .rows
            .layout
            .slots
            .as_slice()
            .iter()
            .zip(inputs.arrays)
            .enumerate()
        {
            match slot {
                BoundSlot::Fixed { codec, index, .. } => {
                    let state = &mut prepared.workspace.fixed.as_mut_slice()[*index];
                    if let Some(view) = inputs.fixed_views.get(position).copied().flatten() {
                        update_fixed(state, &view.value(input_row))?;
                    } else if matches!(codec, FixedStateCodec::Count) {
                        if super::key_rows::arrow_input::resolve(array.as_ref(), input_row, 0)?
                            .is_some()
                        {
                            update_fixed(state, &ScalarValue::Int64(1))?;
                        }
                    } else {
                        super::key_rows::arrow_input::with_inline(
                            array.as_ref(),
                            input_row,
                            |value| {
                                update_fixed(
                                    state,
                                    value.ok_or_else(|| {
                                        invalid("fixed aggregate needs inline input")
                                    })?,
                                )
                            },
                        )?;
                    }
                }
                BoundSlot::Selected { index, .. } => {
                    let state = &prepared.rows.selected.as_slice()
                        [row * prepared.rows.layout.selected + index];
                    prepared.workspace.selected.as_mut_slice()[*index] = state
                        .prepare_array_replacement(
                            array.as_ref(),
                            input_row,
                            &mut prepared.workspace.scalar_bytes,
                        )?;
                }
            }
        }
        Ok(prepared)
    }

    pub(super) fn prepare_merge<'a>(
        &'a mut self,
        row: usize,
        workspace: &'a mut RowWorkspace,
        source: &Self,
        source_row: usize,
    ) -> Result<PreparedRow<'a>> {
        self.prepare_merge_indexed(row, workspace, source, source_row, false)
    }

    pub(super) fn prepare_merge_indexed<'a>(
        &'a mut self,
        row: usize,
        workspace: &'a mut RowWorkspace,
        source: &Self,
        source_row: usize,
        new: bool,
    ) -> Result<PreparedRow<'a>> {
        let prepared = self.begin(row, workspace, new.then_some(row))?;
        if !Arc::ptr_eq(&prepared.rows.layout, &source.layout) {
            return Err(invalid("source layout mismatch"));
        }
        if source_row >= source.rows {
            return Err(invalid("source row index out of range"));
        }
        for index in 0..prepared.rows.layout.fixed {
            merge_fixed(
                &mut prepared.workspace.fixed.as_mut_slice()[index],
                &source.fixed.as_slice()[source_row * source.layout.fixed + index],
            )?;
        }
        for index in 0..prepared.rows.layout.selected {
            let target =
                &prepared.rows.selected.as_slice()[row * prepared.rows.layout.selected + index];
            let source = &source.selected.as_slice()[source_row * source.layout.selected + index];
            prepared.workspace.selected.as_mut_slice()[index] =
                target.prepare_merge_replacement(source)?;
        }
        Ok(prepared)
    }
}

fn decode_fixed(codec: FixedStateCodec, bytes: &[u8]) -> Result<AccumulatorState> {
    let payload = bytes
        .get(..codec.width())
        .ok_or_else(|| invalid("truncated fixed state"))?;
    let mut frame = [0; super::state_codec::FRAME_BYTES];
    frame[..payload.len()].copy_from_slice(payload);
    codec.decode(&frame)
}

/// The exclusive arena/workspace borrows prevent stale commits or reuse of
/// pending scratch. Drop rolls back selected replacements after any error.
pub(super) struct PreparedRow<'a> {
    rows: &'a mut StateRows,
    workspace: &'a mut RowWorkspace,
    row: usize,
    rollback_to: Option<usize>,
}

impl PreparedRow<'_> {
    pub(super) fn commit(mut self) {
        self.publish();
    }

    /// A containing, consuming transaction can publish without moving this token.
    /// Exclusive borrows still prevent any intervening scratch or row mutation.
    pub(super) fn publish(&mut self) {
        let fixed_start = self.row * self.rows.layout.fixed;
        self.rows.fixed.as_mut_slice()[fixed_start..fixed_start + self.rows.layout.fixed]
            .copy_from_slice(self.workspace.fixed.as_slice());
        let selected_start = self.row * self.rows.layout.selected;
        for (state, pending) in self.rows.selected.as_mut_slice()
            [selected_start..selected_start + self.rows.layout.selected]
            .iter_mut()
            .zip(self.workspace.selected.as_mut_slice())
        {
            state.commit_replacement(pending.take());
        }
        self.rollback_to = None;
    }
}

impl Drop for PreparedRow<'_> {
    fn drop(&mut self) {
        self.workspace.clear_pending();
        if let Some(rows) = self.rollback_to {
            self.rows.truncate(rows);
        }
    }
}

pub(super) fn checked_add(left: i64, right: i64) -> Result<i64> {
    left.checked_add(right)
        .ok_or_else(|| invalid("integer accumulator overflow"))
}

fn update_fixed(state: &mut FixedCell, value: &ScalarValue) -> Result<()> {
    state.update(value)
}

#[cfg(test)]
pub(super) fn update_fixed_state(state: &mut AccumulatorState, value: &ScalarValue) -> Result<()> {
    if matches!(value, ScalarValue::Null) {
        return Ok(());
    }
    // Preserve unsigned inputs exactly in the wider partial coefficient.
    if let AccumulatorState::SumDecimal { scale: 0, .. } = state {
        let unsigned = match value {
            ScalarValue::UInt8(v) => Some(*v as i128),
            ScalarValue::UInt16(v) => Some(*v as i128),
            ScalarValue::UInt32(v) => Some(*v as i128),
            ScalarValue::UInt64(v) => Some(*v as i128),
            _ => None,
        };
        if let Some(value) = unsigned {
            state.update(&ScalarValue::Decimal128(crate::planner::DecimalValue::new(
                value, 0,
            )));
            return Ok(());
        }
    }
    match state {
        AccumulatorState::Count(count) => {
            *count = checked_add(*count, 1)?;
            return Ok(());
        }
        AccumulatorState::SumInt(sum, seen) => {
            let value = match value {
                ScalarValue::Int8(value) => *value as i64,
                ScalarValue::Int16(value) => *value as i64,
                ScalarValue::Int32(value) => *value as i64,
                ScalarValue::Int64(value) => *value,
                _ => return Err(invalid("integer SUM input mismatch")),
            };
            *sum = checked_add(*sum, value)?;
            *seen = true;
            return Ok(());
        }
        AccumulatorState::Avg { count, .. } | AccumulatorState::Variance { count, .. } => {
            checked_add(*count, 1)?;
        }
        _ => {}
    }
    state.update(value);
    Ok(())
}

fn merge_fixed(target: &mut FixedCell, source: &FixedCell) -> Result<()> {
    let mut next = target.state();
    merge_fixed_state(&mut next, &source.state())?;
    *target = next.try_into()?;
    Ok(())
}

fn merge_fixed_state(target: &mut AccumulatorState, source: &AccumulatorState) -> Result<()> {
    match (&*target, source) {
        (AccumulatorState::Count(a), AccumulatorState::Count(b))
        | (AccumulatorState::SumInt(a, _), AccumulatorState::SumInt(b, _))
        | (AccumulatorState::Avg { count: a, .. }, AccumulatorState::Avg { count: b, .. })
        | (
            AccumulatorState::Variance { count: a, .. },
            AccumulatorState::Variance { count: b, .. },
        ) => {
            checked_add(*a, *b)?;
        }
        _ => {}
    }
    target.merge(source);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_rows_use_compact_copyable_cells() {
        let pool = MemoryPool::new_named("compact fixed state", 65536);
        let layout = StateRowLayout::bind(
            &pool,
            &[(AggregateFunction::Sum, DataType::Decimal128(38, 2), false)],
        )
        .unwrap()
        .unwrap();
        let mut rows = StateRows::new(layout).unwrap();
        rows.push_empty().unwrap();
        assert!(
            std::mem::size_of_val(&rows.fixed.as_slice()[0]) <= 32,
            "fixed state occupies {} bytes",
            std::mem::size_of_val(&rows.fixed.as_slice()[0])
        );
    }

    #[test]
    fn bound_arrays_preserve_selection_values_and_reject_foreign_layouts() {
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        let pool = MemoryPool::new_named("bound aggregate arrays", 65536);
        {
            let slots = [
                (AggregateFunction::Count, DataType::Int64, false),
                (AggregateFunction::Sum, DataType::Int64, false),
            ];
            let layout = StateRowLayout::bind(&pool, &slots).unwrap().unwrap();
            let mut rows = StateRows::new(layout.clone()).unwrap();
            let mut scratch = RowWorkspace::new(layout.clone()).unwrap();
            let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), None, Some(5)]));
            let arrays = [array.clone(), array];
            let before = pool.used();
            let inputs = rows.bind_arrays(&arrays, 3).unwrap();
            assert_eq!(pool.used(), before, "binding borrows without allocating");
            rows.push_empty().unwrap();
            for input in [2, 0, 2, 1] {
                rows.prepare_arrays_indexed(0, &mut scratch, &inputs, input, false)
                    .unwrap()
                    .commit();
            }
            assert_eq!(rows.value(0, 0).unwrap().as_ref(), &ScalarValue::Int64(3));
            assert_eq!(rows.value(0, 1).unwrap().as_ref(), &ScalarValue::Int64(12));

            let new = rows.push_empty().unwrap();
            assert!(rows
                .prepare_arrays_indexed(new, &mut scratch, &inputs, 3, true)
                .is_err());
            assert_eq!(
                rows.len(),
                1,
                "out-of-range preparation rolls back new state"
            );
            assert!(rows.bind_arrays(&arrays, 2).is_err());
            assert!(rows.bind_arrays(&arrays[..1], 3).is_err());
            let wrong: ArrayRef = Arc::new(StringArray::from(vec!["a", "b", "c"]));
            assert!(rows.bind_arrays(&[arrays[0].clone(), wrong], 3).is_err());

            let other = StateRowLayout::bind(&pool, &slots).unwrap().unwrap();
            let mut foreign = StateRows::new(other.clone()).unwrap();
            let mut foreign_scratch = RowWorkspace::new(other).unwrap();
            foreign.push_empty().unwrap();
            assert!(foreign
                .prepare_arrays_indexed(0, &mut foreign_scratch, &inputs, 0, true)
                .is_err());
            assert_eq!(
                foreign.len(),
                0,
                "same-shaped foreign layout cannot publish state"
            );
            assert_eq!(rows.value(0, 1).unwrap().as_ref(), &ScalarValue::Int64(12));
        }
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn row_write_prevalidates_later_slots_and_preserves_io_error() {
        let pool = MemoryPool::new_named("row write failure", 65536);
        let layout = StateRowLayout::bind(
            &pool,
            &[
                (AggregateFunction::Max, DataType::Utf8, false),
                (AggregateFunction::Count, DataType::Int64, false),
            ],
        )
        .unwrap()
        .unwrap();
        let mut rows = StateRows::new(layout.clone()).unwrap();
        let mut scratch = RowWorkspace::new(layout.clone()).unwrap();
        rows.push_empty().unwrap();
        rows.prepare(
            0,
            &mut scratch,
            &[ScalarValue::Utf8("retained".into()), ScalarValue::Int64(1)],
        )
        .unwrap()
        .commit();
        rows.fixed.as_mut_slice()[0] = AccumulatorState::Count(-1).try_into().unwrap();
        let mut output = Vec::new();
        assert!(rows.write_to(0, &mut output).is_err());
        assert!(output.is_empty());
        assert!(rows.write_to(1, &mut output).is_err());
        assert!(output.is_empty());
        rows.fixed.as_mut_slice()[0] = AccumulatorState::Count(1).try_into().unwrap();
        struct Failed;
        impl std::io::Write for Failed {
            fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
                Err(std::io::Error::from_raw_os_error(28))
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(
            matches!(rows.write_to(0, &mut Failed), Err(QueryError::Io(ref e)) if e.raw_os_error() == Some(28))
        );
        assert_eq!(
            rows.value(0, 0).unwrap().as_ref(),
            &ScalarValue::Utf8("retained".into())
        );
        assert_eq!(rows.value(0, 1).unwrap().as_ref(), &ScalarValue::Int64(1));
        drop((pressure, rows, scratch, layout));
        assert_eq!(pool.used(), 0);
    }
    use crate::planner::DecimalValue;

    fn bind(
        pool: &MemoryPool,
        inputs: &[(AggregateFunction, DataType, bool)],
    ) -> Arc<StateRowLayout> {
        StateRowLayout::bind(pool, inputs).unwrap().unwrap()
    }
    fn count(rows: &StateRows, row: usize, index: usize) -> i64 {
        match rows.fixed.as_slice()[row * rows.layout.fixed + index].state() {
            AccumulatorState::Count(value) => value,
            _ => panic!("not a count"),
        }
    }

    #[test]
    fn later_selected_pressure_cannot_commit_count_or_an_earlier_selection() {
        let pool = MemoryPool::new_named("atomic aggregate row", 128 << 10);
        let layout = bind(
            &pool,
            &[
                (AggregateFunction::Count, DataType::Int64, false),
                (AggregateFunction::Max, DataType::Utf8, false),
                (AggregateFunction::Max, DataType::Utf8, false),
            ],
        );
        let mut scratch = RowWorkspace::new(layout.clone()).unwrap();
        let mut rows = StateRows::new(layout.clone()).unwrap();
        let row = rows.push_empty().unwrap();
        rows.prepare(
            row,
            &mut scratch,
            &[
                ScalarValue::Int64(1),
                ScalarValue::Utf8("b".into()),
                ScalarValue::Utf8("b".into()),
            ],
        )
        .unwrap()
        .commit();
        let pressure = pool.allocate(pool.available() - 2000).unwrap();
        let baseline = pool.used();
        let inputs = [
            ScalarValue::Int64(1),
            ScalarValue::Utf8("c".repeat(100)),
            ScalarValue::Utf8("z".repeat(4096)),
        ];
        assert!(rows
            .prepare(row, &mut scratch, &inputs)
            .err()
            .unwrap()
            .is_memory_limit());
        assert_eq!(pool.used(), baseline);
        assert_eq!(count(&rows, row, 0), 1);
        for state in rows.selected.as_slice() {
            assert_eq!(state.value(), Some(&ScalarValue::Utf8("b".into())));
        }
        drop(pressure);
        // Explicitly dropping a successful preparation also rolls the whole row back.
        drop(rows.prepare(row, &mut scratch, &inputs).unwrap());
        assert_eq!(count(&rows, row, 0), 1);
        rows.prepare(row, &mut scratch, &inputs).unwrap().commit();
        assert_eq!(count(&rows, row, 0), 2);
        assert_eq!(rows.selected.as_slice()[0].value(), Some(&inputs[1]));
        assert_eq!(rows.selected.as_slice()[1].value(), Some(&inputs[2]));
        drop((rows, scratch, layout));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn partial_rows_merge_weighted_avg_exact_decimal_and_selected_values() {
        let pool = MemoryPool::new_named("partial row merge", 256 << 10);
        let layout = bind(
            &pool,
            &[
                (AggregateFunction::Count, DataType::Int64, false),
                (AggregateFunction::Sum, DataType::Decimal128(38, 2), false),
                (AggregateFunction::Avg, DataType::Float64, false),
                (AggregateFunction::Min, DataType::Int64, false),
                (AggregateFunction::Max, DataType::Utf8, false),
                (AggregateFunction::AnyValue, DataType::Utf8, false),
                (AggregateFunction::Arbitrary, DataType::Utf8, false),
            ],
        );
        let mut scratch = RowWorkspace::new(layout.clone()).unwrap();
        let mut left = StateRows::new(layout.clone()).unwrap();
        let mut right = StateRows::new(layout.clone()).unwrap();
        for rows in [&mut left, &mut right] {
            rows.push_empty().unwrap();
            rows.push_empty().unwrap();
        }
        let exact = (1i128 << 100) + 17;
        let first = [
            ScalarValue::Int64(10),
            ScalarValue::Decimal128(DecimalValue::new(exact, 2)),
            ScalarValue::Float64(10.0.into()),
            ScalarValue::Int64(10),
            ScalarValue::Utf8("b".into()),
            ScalarValue::Utf8("left".into()),
            ScalarValue::Utf8("alias-left".into()),
        ];
        let second = [
            ScalarValue::Int64(30),
            ScalarValue::Decimal128(DecimalValue::new(3 * exact, 2)),
            ScalarValue::Float64(30.0.into()),
            ScalarValue::Int64(30),
            ScalarValue::Utf8("z".into()),
            ScalarValue::Utf8("right".into()),
            ScalarValue::Utf8("alias-right".into()),
        ];
        left.prepare(0, &mut scratch, &first).unwrap().commit();
        for _ in 0..3 {
            right.prepare(0, &mut scratch, &second).unwrap().commit();
        }
        let nulls = vec![ScalarValue::Null; first.len()];
        left.prepare(1, &mut scratch, &nulls).unwrap().commit();
        right.prepare(1, &mut scratch, &nulls).unwrap().commit();
        // Merge uses admitted scratch and owner sharing even with a full pool.
        let pressure = pool.allocate(pool.available()).unwrap();
        left.prepare_merge(0, &mut scratch, &right, 0)
            .unwrap()
            .commit();
        left.prepare_merge(1, &mut scratch, &right, 1)
            .unwrap()
            .commit();
        assert_eq!(count(&left, 0, 0), 4);
        assert!(matches!(
            left.fixed.as_slice()[1].state(),
            AccumulatorState::SumDecimal {
                coefficient: Some(coefficient),
                scale: 2,
                seen: true
            } if coefficient == 10 * exact
        ));
        assert!(matches!(
            left.fixed.as_slice()[2].state(),
            AccumulatorState::Avg {
                sum: 100.0,
                count: 4
            }
        ));
        assert_eq!(
            left.fixed.as_slice()[2]
                .finalize(&AggregateFunction::Avg)
                .unwrap(),
            ScalarValue::Float64(25.0.into())
        );
        assert_eq!(
            left.selected.as_slice()[0].value(),
            Some(&ScalarValue::Int64(10))
        );
        assert_eq!(
            left.selected.as_slice()[1].value(),
            Some(&ScalarValue::Utf8("z".into()))
        );
        assert_eq!(
            left.selected.as_slice()[2].value(),
            Some(&ScalarValue::Utf8("left".into()))
        );
        assert_eq!(count(&left, 1, 0), 0);
        assert_eq!(
            left.selected.as_slice()[3].value(),
            Some(&ScalarValue::Utf8("alias-left".into()))
        );
        assert!(matches!(
            left.fixed.as_slice()[4].state(),
            AccumulatorState::SumDecimal {
                coefficient: Some(0),
                seen: false,
                ..
            }
        ));
        assert!(matches!(
            left.fixed.as_slice()[5].state(),
            AccumulatorState::Avg { count: 0, .. }
        ));
        for state in &left.selected.as_slice()[4..] {
            assert!(state.value().is_none());
        }
        let expected = [
            ScalarValue::Int64(4),
            ScalarValue::Decimal128(DecimalValue::new(10 * exact, 2)),
            ScalarValue::Float64(25.0.into()),
            ScalarValue::Int64(10),
            ScalarValue::Utf8("z".into()),
            ScalarValue::Utf8("left".into()),
            ScalarValue::Utf8("alias-left".into()),
        ];
        for (slot, value) in expected.iter().enumerate() {
            assert_eq!(left.value(0, slot).unwrap().as_ref(), value);
            let null_value = left.value(1, slot).unwrap();
            assert_eq!(
                null_value.as_ref(),
                if slot == 0 {
                    &ScalarValue::Int64(0)
                } else {
                    &ScalarValue::Null
                }
            );
        }
        assert!(
            matches!(left.value(0, 4).unwrap(), Cow::Borrowed(_)),
            "selected string must not be cloned by finalization"
        );
        assert!(left.value(2, 0).is_err());
        assert!(left.value(0, 7).is_err());
        drop(right);
        assert_eq!(
            left.selected.as_slice()[1].value(),
            Some(&ScalarValue::Utf8("z".into()))
        );
        drop((left, scratch, layout, pressure));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn overflow_and_invalid_layout_leave_the_destination_unchanged() {
        let pool = MemoryPool::new_named("checked aggregate row", 128 << 10);
        let inputs = [
            (AggregateFunction::Max, DataType::Utf8, false),
            (AggregateFunction::Count, DataType::Int64, false),
        ];
        let layout = bind(&pool, &inputs);
        let mut scratch = RowWorkspace::new(layout.clone()).unwrap();
        let mut rows = StateRows::new(layout.clone()).unwrap();
        rows.push_empty().unwrap();
        rows.prepare(
            0,
            &mut scratch,
            &[ScalarValue::Utf8("b".into()), ScalarValue::Int64(1)],
        )
        .unwrap()
        .commit();
        rows.fixed.as_mut_slice()[0] = AccumulatorState::Count(i64::MAX).try_into().unwrap();
        let baseline = pool.used();
        let error = rows
            .prepare(
                0,
                &mut scratch,
                &[ScalarValue::Utf8("z".repeat(1000)), ScalarValue::Int64(1)],
            )
            .err()
            .unwrap();
        assert!(error.to_string().contains("overflow"));
        assert!(!error.is_memory_limit());
        assert_eq!(pool.used(), baseline);
        assert_eq!(count(&rows, 0, 0), i64::MAX);
        assert_eq!(
            rows.selected.as_slice()[0].value(),
            Some(&ScalarValue::Utf8("b".into()))
        );
        let another = bind(&pool, &inputs);
        let mut wrong_scratch = RowWorkspace::new(another.clone()).unwrap();
        assert!(rows
            .prepare(
                0,
                &mut wrong_scratch,
                &[ScalarValue::Null, ScalarValue::Null]
            )
            .is_err());
        assert!(rows
            .prepare(9, &mut scratch, &[ScalarValue::Null, ScalarValue::Null])
            .is_err());
        assert!(rows.prepare(0, &mut scratch, &[]).is_err());
        let pressure = pool.allocate(pool.available()).unwrap();
        assert!(rows.push_empty().unwrap_err().is_memory_limit());
        assert_eq!(rows.len(), 1);
        assert_eq!(count(&rows, 0, 0), i64::MAX);
        drop((rows, scratch, layout, wrong_scratch, another, pressure));
        assert_eq!(pool.used(), 0);
    }

    #[test]
    fn every_integer_counter_and_sum_checks_update_and_merge_overflow() {
        for (function, input_type, input, full) in [
            (
                AggregateFunction::Count,
                DataType::Int64,
                ScalarValue::Int64(1),
                AccumulatorState::Count(i64::MAX),
            ),
            (
                AggregateFunction::Sum,
                DataType::Int64,
                ScalarValue::Int64(1),
                AccumulatorState::SumInt(i64::MAX, true),
            ),
            (
                AggregateFunction::Avg,
                DataType::Float64,
                ScalarValue::Float64(1.0.into()),
                AccumulatorState::Avg {
                    sum: 0.0,
                    count: i64::MAX,
                },
            ),
            (
                AggregateFunction::Variance,
                DataType::Float64,
                ScalarValue::Float64(1.0.into()),
                AccumulatorState::Variance {
                    count: i64::MAX,
                    mean: 0.0,
                    m2: 0.0,
                },
            ),
        ] {
            let pool = MemoryPool::new_named("row arithmetic", 65536);
            let layout = bind(&pool, &[(function, input_type, false)]);
            let mut scratch = RowWorkspace::new(layout.clone()).unwrap();
            let mut target = StateRows::new(layout.clone()).unwrap();
            let mut source = StateRows::new(layout.clone()).unwrap();
            target.push_empty().unwrap();
            source.push_empty().unwrap();
            target.fixed.as_mut_slice()[0] = full.try_into().unwrap();
            source
                .prepare(0, &mut scratch, std::slice::from_ref(&input))
                .unwrap()
                .commit();
            let before = format!("{:?}", target.fixed.as_slice());
            let error = target.prepare(0, &mut scratch, &[input]).err().unwrap();
            assert!(error.to_string().contains("overflow"));
            assert!(!error.is_memory_limit());
            assert_eq!(format!("{:?}", target.fixed.as_slice()), before);
            let error = target
                .prepare_merge(0, &mut scratch, &source, 0)
                .err()
                .unwrap();
            assert!(error.to_string().contains("overflow"));
            assert_eq!(format!("{:?}", target.fixed.as_slice()), before);
            // A NULL does not increment a full counter or alter an integer sum.
            target
                .prepare(0, &mut scratch, &[ScalarValue::Null])
                .unwrap()
                .commit();
            assert_eq!(format!("{:?}", target.fixed.as_slice()), before);
            drop((target, source, scratch, layout));
            assert_eq!(pool.used(), 0);
        }
    }
}

#[cfg(test)]
mod batch_binding_tests;
