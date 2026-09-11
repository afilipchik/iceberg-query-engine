//! Guaranteed copied-output bounds for resident, pool-independent pipelines.
//! This capability does not account for source residency, expression scratch,
//! allocator RSS or downstream state. Unknown layouts/wrappers decline it.
use crate::physical::operators::spillable::{
    owned_input_batch_base_charge, owned_input_column_charge, owned_input_schema_charge,
};
use crate::planner::{Column, Expr};
#[cfg(test)]
use arrow::datatypes::Field;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct QueueCopyBound {
    schema: SchemaRef,
    schema_bytes: usize,
    columns: Vec<usize>,
    max_rows: usize,
}

fn supported(t: &DataType) -> bool {
    match t {
        DataType::Null
        | DataType::Boolean
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary => true,
        DataType::FixedSizeBinary(n) => *n >= 0,
        DataType::Dictionary(key, value) => {
            key.is_integer()
                && supported(value)
                && !matches!(value.as_ref(), DataType::Dictionary(..))
        }
        other => other.primitive_width().is_some(),
    }
}

impl QueueCopyBound {
    /// Actual emitted schema certificate, never a logical/provider estimate.
    pub(crate) fn int64_membership_column(&self, ordinal: usize, column: &Column) -> bool {
        if crate::physical::operators::find_column_index_in_schema(&self.schema, column).ok()
            != Some(ordinal)
        {
            return false;
        }
        self.schema
            .fields()
            .get(ordinal)
            .is_some_and(|field| match field.data_type() {
                DataType::Int64 => true,
                DataType::Dictionary(key, value) => {
                    key.is_integer() && value.as_ref() == &DataType::Int64
                }
                _ => false,
            })
    }

    /// A declaration based on every actual immutable emitted batch, never on
    /// statistics/samples. Custom callers must also uphold pool-independent
    /// resident pulling; the queue validates actual output charge regardless.
    pub fn from_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Option<Self> {
        if !schema.fields().iter().all(|f| supported(f.data_type())) {
            return None;
        }
        let mut columns = vec![0; schema.fields().len()];
        let mut max_rows = 0;
        let mut schema_bytes = owned_input_schema_charge(schema).ok()?;
        // Include empty outputs: e.g. a Filter can reject every input row.
        let empty = RecordBatch::new_empty(schema.clone());
        for batch in std::iter::once(&empty).chain(batches) {
            if batch.schema() != *schema {
                return None;
            }
            schema_bytes = schema_bytes.max(owned_input_schema_charge(&batch.schema()).ok()?);
            max_rows = max_rows.max(batch.num_rows());
            for (bound, array) in columns.iter_mut().zip(batch.columns()) {
                *bound = (*bound).max(owned_input_column_charge(array).ok()?);
            }
        }
        let result = Self {
            schema: schema.clone(),
            schema_bytes,
            columns,
            max_rows,
        };
        result.max_bytes()?;
        Some(result)
    }

    pub fn max_bytes(&self) -> Option<usize> {
        self.columns.iter().try_fold(
            owned_input_batch_base_charge().checked_add(self.schema_bytes)?,
            |sum, col| sum.checked_add(*col),
        )
    }

    pub(crate) fn merge(&mut self, other: Self) -> Option<()> {
        if self.schema != other.schema || self.columns.len() != other.columns.len() {
            return None;
        }
        self.schema_bytes = self.schema_bytes.max(other.schema_bytes);
        self.max_rows = self.max_rows.max(other.max_rows);
        for (left, right) in self.columns.iter_mut().zip(other.columns) {
            *left = (*left).max(right);
        }
        self.max_bytes()?;
        Some(())
    }

    pub(crate) fn filtered(&self) -> Option<Self> {
        // Arrow58 filter_native/filter_bits/filter_bytes copy a subset without
        // duplicating rows; string values cannot exceed original exposed bytes.
        // filter_dict retains the complete values child (already in the bound).
        // Preserve the input extent bound for slice/all-selected shortcuts and
        // add a compact validity-buffer allowance for newly materialized masks.
        let validity = self
            .max_rows
            .checked_add(7)?
            .checked_div(8)?
            .checked_add(15)?
            .checked_div(16)?
            .checked_mul(16)?;
        let mut result = self.clone();
        for column in &mut result.columns {
            *column = column.checked_add(validity)?;
        }
        result.max_bytes()?;
        Some(result)
    }

    pub(crate) fn projected(&self, exprs: &[Expr], schema: &SchemaRef) -> Option<Self> {
        fn column(expr: &Expr) -> Option<&Column> {
            match expr {
                Expr::Column(c) => Some(c),
                Expr::Alias { expr, .. } => column(expr),
                _ => None,
            }
        }
        if exprs.len() != schema.fields().len() {
            return None;
        }
        let indices: Vec<_> = exprs
            .iter()
            .map(|expr| {
                crate::physical::operators::find_column_index_in_schema(&self.schema, column(expr)?)
                    .ok()
            })
            .collect::<Option<_>>()?;
        // project_batch retypes Null arrays to declared non-Null types. That is
        // a computed allocation, outside this column-only propagation contract.
        for (index, field) in indices.iter().zip(schema.fields()) {
            if self.schema.field(*index).data_type() == &DataType::Null
                && field.data_type() != &DataType::Null
            {
                return None;
            }
        }
        let types_match = indices
            .iter()
            .zip(schema.fields())
            .all(|(i, f)| self.schema.field(*i).data_type() == f.data_type());
        let effective = if types_match {
            schema.clone()
        } else {
            Arc::new(Schema::new(
                indices
                    .iter()
                    .zip(schema.fields())
                    .map(|(i, f)| {
                        let dt = self.schema.field(*i).data_type();
                        if dt == f.data_type() {
                            f.as_ref().clone()
                        } else {
                            f.as_ref()
                                .clone()
                                .with_data_type(dt.clone())
                                .with_nullable(true)
                        }
                    })
                    .collect::<Vec<_>>(),
            ))
        };
        let result = Self {
            schema_bytes: owned_input_schema_charge(&effective).ok()?,
            schema: effective,
            columns: indices.iter().map(|i| self.columns[*i]).collect(),
            max_rows: self.max_rows,
        };
        result.max_bytes()?;
        Some(result)
    }
}

/// Optional actual-data analysis for repeated Arrow `take`. Construction scans
/// variable-width values only when explicitly requested; ordinary queue bounds
/// remain metadata-only. Does not certify future upstream pool independence.
#[derive(Clone, Debug)]
pub struct GatherCopyBound {
    identity: QueueCopyBound,
    columns: Vec<GatherColumn>,
}

#[derive(Clone, Debug)]
enum GatherColumn {
    Null,
    Boolean,
    Fixed(usize),
    Bytes {
        offset_width: usize,
        max_value_bytes: usize,
    },
    Dictionary {
        key_width: usize,
        retained_child_bytes: usize,
    },
}

fn round_copy_bytes(bytes: usize) -> Option<usize> {
    bytes.checked_add(15)?.checked_div(16)?.checked_mul(16)
}

impl GatherCopyBound {
    /// Enforced compact fixed-width output only. Callers must normalize every
    /// emitted array's exposed buffers; a schema/row estimate is insufficient.
    pub(crate) fn from_compact_fixed_width(schema: &SchemaRef, rows: usize) -> Option<Self> {
        use crate::physical::fixed_width_output::{fixed_width_kind, FixedWidthKind};
        let kinds = schema
            .fields()
            .iter()
            .map(|f| fixed_width_kind(f.data_type()))
            .collect::<Option<Vec<_>>>()?;
        let bits = rows.checked_add(14)?.checked_div(8)?;
        let validity = round_copy_bytes(bits)?;
        let mut identity = QueueCopyBound::from_batches(schema, &[])?;
        identity.max_rows = rows;
        let mut columns = Vec::with_capacity(kinds.len());
        for (bytes, kind) in identity.columns.iter_mut().zip(kinds) {
            let (layout, payload) = match kind {
                FixedWidthKind::Null => (GatherColumn::Null, 0),
                FixedWidthKind::Boolean => (GatherColumn::Boolean, round_copy_bytes(bits)?),
                FixedWidthKind::Bytes(width) => (
                    GatherColumn::Fixed(width),
                    round_copy_bytes(rows.checked_mul(width)?)?,
                ),
            };
            *bytes = bytes.checked_add(payload)?.checked_add(validity)?;
            columns.push(layout);
        }
        identity.max_bytes()?;
        Some(Self { identity, columns })
    }
    pub(crate) fn input_copy_bound(&self) -> QueueCopyBound {
        self.identity.clone()
    }

    /// Merge actual immutable batches incrementally, preserving all maxima.
    pub(crate) fn merge(&mut self, other: Self) -> Option<()> {
        if self.columns.len() != other.columns.len() {
            return None;
        }
        // Work on a clone so a declined merge cannot leave a partial proof.
        let mut merged = self.clone();
        merged.identity.merge(other.identity)?;
        for (left, right) in merged.columns.iter_mut().zip(other.columns) {
            match (left, right) {
                (GatherColumn::Null, GatherColumn::Null)
                | (GatherColumn::Boolean, GatherColumn::Boolean) => (),
                (GatherColumn::Fixed(a), GatherColumn::Fixed(b)) if *a == b => (),
                (
                    GatherColumn::Bytes {
                        offset_width: a,
                        max_value_bytes: x,
                    },
                    GatherColumn::Bytes {
                        offset_width: b,
                        max_value_bytes: y,
                    },
                ) if *a == b => *x = (*x).max(y),
                (
                    GatherColumn::Dictionary {
                        key_width: a,
                        retained_child_bytes: x,
                    },
                    GatherColumn::Dictionary {
                        key_width: b,
                        retained_child_bytes: y,
                    },
                ) if *a == b => *x = (*x).max(y),
                _ => return None,
            }
        }
        *self = merged;
        Some(())
    }

    pub(crate) fn filtered(&self) -> Option<Self> {
        // Audited Arrow filter only selects rows: byte element maxima cannot
        // increase; dictionary values remain retained in full. Existing identity
        // propagation handles new validity and all-selected/slice extents.
        Some(Self {
            identity: self.identity.filtered()?,
            columns: self.columns.clone(),
        })
    }

    pub(crate) fn projected(&self, exprs: &[Expr], schema: &SchemaRef) -> Option<Self> {
        fn column(expr: &Expr) -> Option<&Column> {
            match expr {
                Expr::Column(c) => Some(c),
                Expr::Alias { expr, .. } => column(expr),
                _ => None,
            }
        }
        // This validates output types, computed Null conversions, schema
        // metadata and exact runtime resolution before propagating layouts.
        let identity = self.identity.projected(exprs, schema)?;
        let columns = exprs
            .iter()
            .map(|expr| {
                let index = crate::physical::operators::find_column_index_in_schema(
                    &self.identity.schema,
                    column(expr)?,
                )
                .ok()?;
                self.columns.get(index).cloned()
            })
            .collect::<Option<Vec<_>>>()?;
        Some(Self { identity, columns })
    }

    /// Every batch that may supply a take input must be included. The result
    /// covers one batch at a time, not concatenation of multiple dictionaries.
    pub fn from_batches(schema: &SchemaRef, batches: &[RecordBatch]) -> Option<Self> {
        use arrow::array::{Array, BinaryArray, LargeBinaryArray, LargeStringArray, StringArray};
        let identity = QueueCopyBound::from_batches(schema, batches)?;
        let mut columns = Vec::with_capacity(schema.fields().len());
        for (index, field) in schema.fields().iter().enumerate() {
            let layout = match field.data_type() {
                DataType::Null => GatherColumn::Null,
                DataType::Boolean => GatherColumn::Boolean,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Binary | DataType::LargeBinary => {
                    let mut max_value_bytes = 0;
                    for batch in batches {
                        let array = batch.column(index);
                        // Scan actual offset spans, including physical NULL slots;
                        // this is conservative even when take omits their bytes.
                        let longest: usize = match field.data_type() {
                            DataType::Utf8 => array
                                .as_any()
                                .downcast_ref::<StringArray>()?
                                .value_offsets()
                                .windows(2)
                                .try_fold(0usize, |longest, p| {
                                    Some(
                                        longest.max(usize::try_from(p[1].checked_sub(p[0])?).ok()?),
                                    )
                                })?,
                            DataType::LargeUtf8 => array
                                .as_any()
                                .downcast_ref::<LargeStringArray>()?
                                .value_offsets()
                                .windows(2)
                                .try_fold(0usize, |longest, p| {
                                    Some(
                                        longest.max(usize::try_from(p[1].checked_sub(p[0])?).ok()?),
                                    )
                                })?,
                            DataType::Binary => array
                                .as_any()
                                .downcast_ref::<BinaryArray>()?
                                .value_offsets()
                                .windows(2)
                                .try_fold(0usize, |longest, p| {
                                    Some(
                                        longest.max(usize::try_from(p[1].checked_sub(p[0])?).ok()?),
                                    )
                                })?,
                            DataType::LargeBinary => array
                                .as_any()
                                .downcast_ref::<LargeBinaryArray>()?
                                .value_offsets()
                                .windows(2)
                                .try_fold(0usize, |longest, p| {
                                    Some(
                                        longest.max(usize::try_from(p[1].checked_sub(p[0])?).ok()?),
                                    )
                                })?,
                            _ => unreachable!(),
                        };
                        max_value_bytes = max_value_bytes.max(longest);
                    }
                    GatherColumn::Bytes {
                        offset_width: if matches!(
                            field.data_type(),
                            DataType::LargeUtf8 | DataType::LargeBinary
                        ) {
                            8
                        } else {
                            4
                        },
                        max_value_bytes,
                    }
                }
                DataType::Dictionary(key, _) => {
                    let mut retained_child_bytes = 0;
                    for batch in batches {
                        let data = batch.column(index).to_data();
                        let child = data.child_data().first()?;
                        retained_child_bytes = retained_child_bytes.max(
                            owned_input_column_charge(&arrow::array::make_array(child.clone()))
                                .ok()?,
                        );
                    }
                    GatherColumn::Dictionary {
                        key_width: key.primitive_width()?,
                        retained_child_bytes,
                    }
                }
                DataType::FixedSizeBinary(width) => {
                    GatherColumn::Fixed(usize::try_from(*width).ok()?)
                }
                other => GatherColumn::Fixed(other.primitive_width()?),
            };
            columns.push(layout);
        }
        Some(Self { identity, columns })
    }

    /// Fresh Arrow take only, never identity reuse. Caller bounds the indices'
    /// exposed validity buffer length; zero means indices have no validity.
    /// Arrow may clone that whole buffer when the source has no logical NULLs.
    pub(crate) fn take_only(
        &self,
        rows: usize,
        max_index_validity_bytes: usize,
    ) -> Option<QueueCopyBound> {
        let bits = rows.checked_add(7)?.checked_div(8)?;
        let validity = round_copy_bytes(bits)?.max(round_copy_bytes(max_index_validity_bytes)?);
        let mut columns = Vec::with_capacity(self.columns.len());
        for (field, layout) in self.identity.schema.fields().iter().zip(&self.columns) {
            let empty = arrow::array::new_empty_array(field.data_type());
            let mut bytes = owned_input_column_charge(&empty).ok()?;
            let payload = match layout {
                GatherColumn::Null => 0,
                GatherColumn::Boolean => round_copy_bytes(bits)?,
                GatherColumn::Fixed(width) => round_copy_bytes(rows.checked_mul(*width)?)?,
                GatherColumn::Bytes {
                    offset_width,
                    max_value_bytes,
                } => round_copy_bytes(rows.checked_add(1)?.checked_mul(*offset_width)?)?
                    .checked_add(round_copy_bytes(rows.checked_mul(*max_value_bytes)?)?)?,
                GatherColumn::Dictionary {
                    key_width,
                    retained_child_bytes,
                } => {
                    // Extra child ArrayRef/array headers are conservative: the
                    // actual nested charge only needs child ArrayData/buffers.
                    bytes = bytes.checked_add(*retained_child_bytes)?;
                    round_copy_bytes(rows.checked_mul(*key_width)?)?
                }
            };
            columns.push(bytes.checked_add(payload)?.checked_add(validity)?);
        }
        let result = QueueCopyBound {
            schema: self.identity.schema.clone(),
            schema_bytes: self.identity.schema_bytes,
            columns,
            max_rows: rows,
        };
        result.max_bytes()?;
        Some(result)
    }

    /// Bound successful take outputs of at most max_output_rows, including
    /// repeated/NULL indices, plus identity reuse of qualifying input batches.
    /// Indices must have no validity or a compact exposed validity bitmap of
    /// at most ceil(max_output_rows/8) bytes. Arbitrary sliced-index validity
    /// can be retained by Arrow and is NOT covered by this legacy interface.
    /// This intentionally charges the entire identity extent AND fresh gathered
    /// payload, avoiding unsafe assumptions about retained parent/child buffers.
    pub fn gather(&self, max_output_rows: usize) -> Option<QueueCopyBound> {
        let bits = max_output_rows.checked_add(7)?.checked_div(8)?;
        let validity = round_copy_bytes(bits)?;
        let mut result = self.identity.clone();
        result.max_rows = max_output_rows;
        for (bound, layout) in result.columns.iter_mut().zip(&self.columns) {
            let payload = match layout {
                GatherColumn::Null => 0,
                GatherColumn::Boolean => round_copy_bytes(bits)?,
                GatherColumn::Fixed(width) => {
                    round_copy_bytes(max_output_rows.checked_mul(*width)?)?
                }
                GatherColumn::Bytes {
                    offset_width,
                    max_value_bytes,
                } => round_copy_bytes(max_output_rows.checked_add(1)?.checked_mul(*offset_width)?)?
                    .checked_add(round_copy_bytes(
                        max_output_rows.checked_mul(*max_value_bytes)?,
                    )?)?,
                GatherColumn::Dictionary {
                    key_width,
                    retained_child_bytes,
                } => {
                    // Identity charge already includes the complete retained
                    // child; max is defensive and adds no repeated-child guess.
                    *bound = (*bound).max(*retained_child_bytes);
                    round_copy_bytes(max_output_rows.checked_mul(*key_width)?)?
                }
            };
            *bound = bound.checked_add(payload)?.checked_add(validity)?;
        }
        result.max_bytes()?;
        Some(result)
    }
}

/// Different physical gather schemas must not masquerade as one schema-aware
/// propagation descriptor. A prepared consumer may reserve their maximum bytes.
#[derive(Clone, Debug)]
pub struct PreparedOutputLayouts {
    variants: Vec<QueueCopyBound>,
    // When present, covers every identity variant in the same order.
    gathers: Option<Vec<GatherCopyBound>>,
}
impl PreparedOutputLayouts {
    /// Every physical variant must support the same exact membership ordinal.
    pub(crate) fn int64_membership_column(&self, ordinal: usize, column: &Column) -> bool {
        !self.variants.is_empty()
            && self
                .variants
                .iter()
                .all(|variant| variant.int64_membership_column(ordinal, column))
    }

    pub fn from_bound(bound: QueueCopyBound) -> Option<Self> {
        bound.max_bytes()?;
        let mut variants = Vec::new();
        variants.try_reserve_exact(1).ok()?;
        variants.push(bound);
        Some(Self {
            variants,
            gathers: None,
        })
    }
    pub fn from_gather(bound: GatherCopyBound) -> Option<Self> {
        let mut result = Self::from_bound(bound.input_copy_bound())?;
        let mut gathers = Vec::new();
        gathers.try_reserve_exact(1).ok()?;
        gathers.push(bound);
        result.gathers = Some(gathers);
        Some(result)
    }
    pub fn gather_max_bytes(&self, rows: usize) -> Option<usize> {
        let gathers = self.gathers.as_ref()?;
        if gathers.len() != self.variants.len() || gathers.is_empty() {
            return None;
        }
        gathers
            .iter()
            .try_fold(0usize, |max, g| Some(max.max(g.gather(rows)?.max_bytes()?)))
    }
    pub fn max_bytes(&self) -> Option<usize> {
        if self.variants.is_empty() {
            return None;
        }
        self.variants
            .iter()
            .try_fold(0usize, |max, variant| Some(max.max(variant.max_bytes()?)))
    }
    fn transformed(
        &self,
        mut transform: impl FnMut(&QueueCopyBound) -> Option<QueueCopyBound>,
    ) -> Option<Self> {
        if self.variants.is_empty() {
            return None;
        }
        let mut variants = Vec::new();
        variants.try_reserve_exact(self.variants.len()).ok()?;
        for variant in &self.variants {
            variants.push(transform(variant)?);
        }
        let result = Self {
            variants,
            gathers: None,
        };
        result.max_bytes()?;
        Some(result)
    }
    pub(crate) fn filtered(&self) -> Option<Self> {
        let mut result = self.transformed(QueueCopyBound::filtered)?;
        result.gathers = self.transform_gathers(GatherCopyBound::filtered);
        Some(result)
    }
    pub(crate) fn projected(&self, exprs: &[Expr], schema: &SchemaRef) -> Option<Self> {
        let mut result = self.transformed(|variant| variant.projected(exprs, schema))?;
        result.gathers = self.transform_gathers(|g| g.projected(exprs, schema));
        Some(result)
    }
    fn transform_gathers(
        &self,
        mut f: impl FnMut(&GatherCopyBound) -> Option<GatherCopyBound>,
    ) -> Option<Vec<GatherCopyBound>> {
        let inputs = self.gathers.as_ref()?;
        if inputs.len() != self.variants.len() {
            return None;
        }
        let mut outputs = Vec::new();
        outputs.try_reserve_exact(inputs.len()).ok()?;
        for input in inputs {
            outputs.push(f(input)?);
        }
        Some(outputs)
    }
}

/// Columnar Inner gather only: actual single cached build batch, plus a probe
/// take guarantee. Does NOT certify preparation/pool dependencies or concat.
/// Masks are applied to these exact inputs (pass None for already-pruned cache).
/// Bound both values of QE_DICT_GATHER without reading the mutable environment.
pub(crate) fn inner_output_copy_bound(
    build_batches: &[RecordBatch],
    probe: &GatherCopyBound,
    build_keep: Option<&[bool]>,
    probe_keep: Option<&[bool]>,
    swapped: bool,
    declared: &SchemaRef,
    max_rows: usize,
) -> Option<PreparedOutputLayouts> {
    use arrow::array::{ArrayRef, DictionaryArray, Int32Array};
    use arrow::datatypes::Int32Type;
    if build_batches.len() != 1 {
        return None;
    }
    let build = &build_batches[0];
    let keep_indices = |count: usize, keep: Option<&[bool]>| -> Option<Vec<usize>> {
        if keep.is_some_and(|mask| mask.len() != count) {
            return None;
        }
        Some(
            (0..count)
                .filter(|i| keep.is_none_or(|mask| mask[*i]))
                .collect(),
        )
    };
    let build_indices = keep_indices(build.num_columns(), build_keep)?;
    let probe_indices = keep_indices(probe.columns.len(), probe_keep)?;
    let build = build.project(&build_indices).ok()?;
    let build_metadata =
        GatherCopyBound::from_batches(&build.schema(), std::slice::from_ref(&build))?;
    let build_gather = build_metadata.take_only(max_rows, 0)?;
    let probe_gather = probe.gather(max_rows)?;
    if build_indices.len().checked_add(probe_indices.len())? != declared.fields().len() {
        return None;
    }
    let make_variant = |dictionary: bool| -> Option<GatherCopyBound> {
        let mut build_types: Vec<DataType> = build
            .schema()
            .fields()
            .iter()
            .map(|f| f.data_type().clone())
            .collect();
        let mut build_bytes = build_gather.columns.clone();
        let mut build_columns = Vec::new();
        build_columns
            .try_reserve_exact(build_metadata.columns.len())
            .ok()?;
        build_columns.extend(build_metadata.columns.iter().cloned());
        if dictionary {
            for (index, array) in build.columns().iter().enumerate() {
                if array.data_type() != &DataType::Utf8 {
                    continue;
                }
                // No output payload is materialized: zero keys plus the actual
                // retained source values child establish exact structural charge.
                let empty: ArrayRef = Arc::new(
                    DictionaryArray::<Int32Type>::try_new(
                        Int32Array::from(Vec::<i32>::new()),
                        array.clone(),
                    )
                    .ok()?,
                );
                build_bytes[index] = owned_input_column_charge(&empty)
                    .ok()?
                    .checked_add(round_copy_bytes(max_rows.checked_mul(4)?)?)?
                    .checked_add(round_copy_bytes(max_rows.checked_add(7)?.checked_div(8)?)?)?;
                build_types[index] = empty.data_type().clone();
                build_columns[index] = GatherColumn::Dictionary {
                    key_width: 4,
                    retained_child_bytes: owned_input_column_charge(array).ok()?,
                };
            }
        }
        let probe_types: Vec<DataType> = probe_indices
            .iter()
            .map(|i| probe.identity.schema.field(*i).data_type().clone())
            .collect();
        let probe_bytes: Vec<usize> = probe_indices
            .iter()
            .map(|i| probe_gather.columns[*i])
            .collect();
        let probe_columns = probe_indices.iter().map(|i| probe.columns[*i].clone());
        let mut gather_columns = Vec::new();
        gather_columns
            .try_reserve_exact(build_columns.len().checked_add(probe_indices.len())?)
            .ok()?;
        if swapped {
            gather_columns.extend(probe_columns);
            gather_columns.extend(build_columns);
        } else {
            gather_columns.extend(build_columns);
            gather_columns.extend(probe_columns);
        }
        let (types, columns): (Vec<_>, Vec<_>) = if swapped {
            (
                probe_types.into_iter().chain(build_types).collect(),
                probe_bytes.into_iter().chain(build_bytes).collect(),
            )
        } else {
            (
                build_types.into_iter().chain(probe_types).collect(),
                build_bytes.into_iter().chain(probe_bytes).collect(),
            )
        };
        // Mirror batch_with_actual_types, preserving exactly matched declared
        // fields and their metadata while making mismatched types nullable.
        let matches = types
            .iter()
            .zip(declared.fields())
            .all(|(t, f)| t == f.data_type());
        let schema = if matches {
            declared.clone()
        } else {
            Arc::new(Schema::new(
                types
                    .iter()
                    .zip(declared.fields())
                    .map(|(t, f)| {
                        if t == f.data_type() {
                            f.as_ref().clone()
                        } else {
                            f.as_ref()
                                .clone()
                                .with_data_type(t.clone())
                                .with_nullable(true)
                        }
                    })
                    .collect::<Vec<_>>(),
            ))
        };
        let result = QueueCopyBound {
            schema_bytes: owned_input_schema_charge(&schema).ok()?,
            schema,
            columns,
            max_rows,
        };
        result.max_bytes()?;
        Some(GatherCopyBound {
            identity: result,
            columns: gather_columns,
        })
    };
    let mut gathers = Vec::new();
    gathers.try_reserve_exact(2).ok()?;
    gathers.push(make_variant(false)?);
    if build.num_rows() <= 4096
        && build
            .columns()
            .iter()
            .any(|a| a.data_type() == &DataType::Utf8)
    {
        gathers.push(make_variant(true)?);
    }
    let mut variants = Vec::new();
    variants.try_reserve_exact(gathers.len()).ok()?;
    for gather in &gathers {
        variants.push(gather.input_copy_bound());
    }
    let result = PreparedOutputLayouts {
        variants,
        gathers: Some(gathers),
    };
    result.max_bytes()?;
    Some(result)
}

/// Compose every prepared probe physical gather variant; callers retain their
/// streams and use Unknown if any required layout or allocation is unavailable.
pub(crate) fn inner_prepared_output_copy_bound(
    build: &[RecordBatch],
    probe: &PreparedOutputLayouts,
    probe_keep: Option<&[bool]>,
    swapped: bool,
    schema: &SchemaRef,
    rows: usize,
) -> Option<PreparedOutputLayouts> {
    let input = probe.gathers.as_ref()?;
    if input.is_empty() || input.len() != probe.variants.len() {
        return None;
    }
    let maximum = input.len().checked_mul(2)?;
    let mut variants = Vec::new();
    let mut gathers = Vec::new();
    variants.try_reserve_exact(maximum).ok()?;
    gathers.try_reserve_exact(maximum).ok()?;
    for source in input {
        let next = inner_output_copy_bound(build, source, None, probe_keep, swapped, schema, rows)?;
        variants.extend(next.variants);
        gathers.extend(next.gathers?);
    }
    let result = PreparedOutputLayouts {
        variants,
        gathers: Some(gathers),
    };
    result.max_bytes()?;
    Some(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::operators::{FilterExec, MemoryTableExec, ProjectExec};
    use crate::physical::{PhysicalOperator, RecordBatchStream};
    use crate::planner::{BinaryOp, ScalarValue};
    use arrow::array::{
        Array, ArrayRef, DictionaryArray, Int32Array, Int64Array, StringArray, StringViewArray,
    };
    use arrow::datatypes::Int32Type;
    use futures::TryStreamExt;

    fn resident(batch: RecordBatch) -> Arc<dyn PhysicalOperator> {
        Arc::new(MemoryTableExec::new(
            "resident",
            batch.schema(),
            vec![batch],
            None,
        ))
    }
    #[tokio::test]
    async fn dictionary_projection_bound_preserves_field_metadata_and_values() {
        let dictionary: ArrayRef = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![Some(0), None, Some(1), Some(0)]),
                Arc::new(StringArray::from(vec!["alpha", "beta"])),
            )
            .unwrap(),
        );
        let input = RecordBatch::try_from_iter([("value", dictionary)]).unwrap();
        let metadata = std::collections::HashMap::from([
            ("description".to_owned(), "retained metadata".repeat(64)),
            ("source".to_owned(), "independent fixture".to_owned()),
        ]);
        let field = Field::new("result", DataType::Utf8, true).with_metadata(metadata.clone());
        let op = ProjectExec::new(
            resident(input),
            vec![Expr::column("value")],
            Arc::new(Schema::new(vec![field])),
        );
        let output = collect_checked(&op).await;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].schema().field(0).metadata(), &metadata);
        let decoded = arrow::compute::cast(output[0].column(0), &DataType::Utf8).unwrap();
        assert_eq!(
            decoded
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("alpha"), None, Some("beta"), Some("alpha")]
        );
    }
    #[test]
    fn dictionary_join_bound_preserves_metadata_in_both_orientations() {
        let values = Arc::new(StringArray::from(vec!["alpha", "beta"]));
        let build = RecordBatch::try_from_iter([("value", values.clone() as ArrayRef)]).unwrap();
        let ids: ArrayRef = Arc::new(Int64Array::from(vec![7, 8, 9, 10]));
        let probe = RecordBatch::try_from_iter([("id", ids.clone())]).unwrap();
        let gather = GatherCopyBound::from_batches(&probe.schema(), &[probe.clone()]).unwrap();
        let metadata = std::collections::HashMap::from([("source".to_owned(), "m".repeat(2048))]);
        for swapped in [false, true] {
            let value_field =
                Field::new("value", DataType::Utf8, true).with_metadata(metadata.clone());
            let id_field = Field::new("id", DataType::Int64, false);
            let fields = if swapped {
                vec![id_field, value_field]
            } else {
                vec![value_field, id_field]
            };
            let declared = Arc::new(Schema::new(fields));
            let bound = inner_output_copy_bound(
                &[build.clone()],
                &gather,
                None,
                None,
                swapped,
                &declared,
                4,
            )
            .unwrap();
            let dictionary: ArrayRef = Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![Some(0), None, Some(1), Some(0)]),
                    values.clone(),
                )
                .unwrap(),
            );
            let value_index = usize::from(swapped);
            let mut actual_fields = declared
                .fields()
                .iter()
                .map(|f| f.as_ref().clone())
                .collect::<Vec<_>>();
            actual_fields[value_index] = actual_fields[value_index]
                .clone()
                .with_data_type(dictionary.data_type().clone());
            let columns = if swapped {
                vec![ids.clone(), dictionary]
            } else {
                vec![dictionary, ids.clone()]
            };
            let output =
                RecordBatch::try_new(Arc::new(Schema::new(actual_fields)), columns).unwrap();
            assert!(
                crate::physical::operators::spillable::owned_input_batch_charge(&output).unwrap()
                    <= bound.max_bytes().unwrap()
            );
            assert!(bound.variants.iter().all(|variant| variant
                .schema
                .field(value_index)
                .metadata()
                == &metadata));
        }
    }
    async fn collect_checked(op: &dyn PhysicalOperator) -> Vec<RecordBatch> {
        let bound = op.resident_queue_copy_bound().unwrap().max_bytes().unwrap();
        let mut out = vec![];
        for part in 0..op.output_partitions() {
            let mut stream = op.execute(part).await.unwrap();
            while let Some(batch) = stream.try_next().await.unwrap() {
                assert!(
                    crate::physical::operators::spillable::owned_input_batch_charge(&batch)
                        .unwrap()
                        <= bound
                );
                out.push(batch);
            }
        }
        out
    }
    fn fixture() -> RecordBatch {
        let dict = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1), None, Some(2)]),
            Arc::new(StringArray::from(vec![Some("x"), None, Some("z")])),
        )
        .unwrap();
        RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from(vec![
                    Some("large parent".repeat(1000)),
                    Some("a".into()),
                    None,
                    Some("z".into()),
                ])) as ArrayRef,
            ),
            ("dict", Arc::new(dict) as ArrayRef),
        ])
        .unwrap()
    }

    #[tokio::test]
    async fn filtered_sliced_strings_dictionary_and_empty_outputs_obey_bound() {
        for sliced in [false, true] {
            let batch = fixture();
            let batch = if sliced { batch.slice(1, 2) } else { batch };
            let row_count = batch.num_rows();
            for selected in [false, true] {
                let op = FilterExec::new(
                    resident(batch.clone()),
                    Expr::Literal(ScalarValue::Boolean(selected)),
                );
                let out = collect_checked(&op).await;
                assert_eq!(
                    out.iter().map(|b| b.num_rows()).sum::<usize>(),
                    if selected { row_count } else { 0 }
                );
                if selected {
                    assert_eq!(out, vec![batch.clone()]);
                }
            }
        }
        let op = FilterExec::new(
            resident(fixture()),
            Expr::BinaryExpr {
                left: Box::new(Expr::column("id")),
                op: BinaryOp::GtEq,
                right: Box::new(Expr::Literal(ScalarValue::Int64(2))),
            },
        );
        let out = collect_checked(&op).await;
        assert_eq!(
            out[0]
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[2, 3]
        );
        let values = arrow::compute::cast(out[0].column(2), &DataType::Utf8).unwrap();
        assert_eq!(
            values
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![None, Some("z")]
        );
    }

    #[tokio::test]
    async fn projected_duplicates_aliases_and_actual_dictionary_schema_are_bounded() {
        let name = "long_alias_".repeat(100);
        let schema = Arc::new(Schema::new(vec![
            Field::new(&name, DataType::Utf8, false),
            Field::new("duplicate", DataType::Utf8, true),
        ]));
        let op = ProjectExec::new(
            resident(fixture()),
            vec![
                Expr::Alias {
                    expr: Box::new(Expr::column("dict")),
                    name,
                },
                Expr::column("dict"),
            ],
            schema,
        );
        let out = collect_checked(&op).await;
        assert_eq!(out[0].column(0), out[0].column(1));
        assert!(matches!(
            out[0].schema().field(0).data_type(),
            DataType::Dictionary(..)
        ));
    }

    #[tokio::test]
    async fn projection_uses_shared_qualified_resolution_and_declines_ambiguity() {
        let batch = RecordBatch::try_from_iter(vec![
            (
                "a.v",
                Arc::new(StringArray::from(vec!["large".repeat(1000)])) as ArrayRef,
            ),
            (
                "b.v",
                Arc::new(StringArray::from(vec!["small"])) as ArrayRef,
            ),
        ])
        .unwrap();
        for (expr, expected) in [(Expr::qualified_column("b", "v"), "small".to_owned())] {
            let op = ProjectExec::new(
                resident(batch.clone()),
                vec![expr],
                Arc::new(Schema::new(vec![Field::new(
                    "result",
                    DataType::Utf8,
                    false,
                )])),
            );
            let out = collect_checked(&op).await;
            assert_eq!(
                out[0]
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                expected
            );
        }
        let ambiguous = ProjectExec::new(
            resident(batch),
            vec![Expr::qualified_column("missing", "v")],
            Arc::new(Schema::new(vec![Field::new(
                "result",
                DataType::Utf8,
                false,
            )])),
        );
        assert!(ambiguous.resident_queue_copy_bound().is_none());
        let mut stream = ambiguous.execute(0).await.unwrap();
        assert!(
            stream.try_next().await.is_err(),
            "runtime and certificate must both reject the ambiguous column"
        );
    }

    #[test]
    fn equal_schemas_preserve_maximum_metadata_capacity_and_merge_declines_mismatch() {
        fn schema(capacity: usize) -> SchemaRef {
            let mut name = String::with_capacity(capacity);
            name.push_str("v");
            let mut value = String::with_capacity(capacity);
            value.push_str("same");
            Arc::new(
                Schema::new(vec![Field::new(name, DataType::Int64, false)])
                    .with_metadata(std::collections::HashMap::from([("key".into(), value)])),
            )
        }
        let small = schema(4);
        let large = schema(16_384);
        assert_eq!(small, large);
        assert!(
            owned_input_schema_charge(&large).unwrap() > owned_input_schema_charge(&small).unwrap()
        );
        let columns: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![7]))];
        let first = RecordBatch::try_new(small.clone(), columns.clone()).unwrap();
        let second = RecordBatch::try_new(large.clone(), columns).unwrap();
        let bound = QueueCopyBound::from_batches(&small, &[first.clone(), second.clone()]).unwrap();
        assert!(
            bound.max_bytes().unwrap()
                >= crate::physical::operators::spillable::owned_input_batch_charge(&second)
                    .unwrap()
        );
        let mut merged = QueueCopyBound::from_batches(&small, &[first]).unwrap();
        merged
            .merge(QueueCopyBound::from_batches(&large, &[second]).unwrap())
            .unwrap();
        assert_eq!(merged.max_bytes(), bound.max_bytes());
        let wrong_schema = Arc::new(Schema::new(vec![Field::new(
            "different",
            DataType::Int64,
            false,
        )]));
        assert!(merged
            .merge(QueueCopyBound::from_batches(&wrong_schema, &[]).unwrap())
            .is_none());
        let wrong_type = Arc::new(Schema::new(vec![Field::new("v", DataType::Int32, false)]));
        assert!(merged
            .merge(QueueCopyBound::from_batches(&wrong_type, &[]).unwrap())
            .is_none());
    }

    #[derive(Debug)]
    struct Unknown;
    #[async_trait::async_trait]
    impl PhysicalOperator for Unknown {
        fn schema(&self) -> SchemaRef {
            Arc::new(Schema::empty())
        }
        fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
            vec![]
        }
        fn name(&self) -> &str {
            "unknown"
        }
        async fn execute(&self, _: usize) -> crate::error::Result<RecordBatchStream> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }
    #[test]
    fn unsupported_defaults_computed_projection_and_views_decline() {
        assert!(Unknown.resident_queue_copy_bound().is_none());
        let op = ProjectExec::new(
            resident(fixture()),
            vec![Expr::Literal(ScalarValue::Int64(1))],
            Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
        );
        assert!(op.resident_queue_copy_bound().is_none());
        let batch = RecordBatch::try_from_iter(vec![(
            "view",
            Arc::new(StringViewArray::from(vec!["x"])) as ArrayRef,
        )])
        .unwrap();
        assert!(resident(batch).resident_queue_copy_bound().is_none());
    }
}

#[cfg(test)]
mod gather_tests {
    use super::*;
    use crate::physical::operators::spillable::owned_input_batch_charge;
    use arrow::array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Decimal128Array, DictionaryArray, Int32Array,
        Int64Array, NullArray, StringArray, StringViewArray, UInt32Array,
    };
    use arrow::datatypes::Int32Type;

    fn batch(arrays: Vec<ArrayRef>) -> RecordBatch {
        let schema = Arc::new(Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, arrays).unwrap()
    }
    fn verify(input: &RecordBatch, indices: UInt32Array) -> RecordBatch {
        let capability = GatherCopyBound::from_batches(&input.schema(), &[input.clone()]).unwrap();
        let bound = capability.gather(indices.len()).unwrap();
        let output = RecordBatch::try_new(
            input.schema(),
            input
                .columns()
                .iter()
                .map(|a| arrow::compute::take(a.as_ref(), &indices, None).unwrap())
                .collect(),
        )
        .unwrap();
        assert!(owned_input_batch_charge(&output).unwrap() <= bound.max_bytes().unwrap());
        let identity = capability.gather(input.num_rows()).unwrap();
        assert!(owned_input_batch_charge(input).unwrap() <= identity.max_bytes().unwrap());
        output
    }
    #[test]
    fn repeated_long_sliced_string_and_binary_values() {
        let huge = "x".repeat(8_193);
        let parent = StringArray::from(vec![
            Some("discarded"),
            Some(huge.as_str()),
            None,
            Some("z"),
            Some("tail"),
        ]);
        let input = batch(vec![
            Arc::new(parent.slice(1, 3)),
            Arc::new(BinaryArray::from(vec![
                Some(&b"abc"[..]),
                None,
                Some(&b"z"[..]),
            ])),
        ]);
        let output = verify(&input, UInt32Array::from(vec![Some(0); 4096]));
        let strings = output
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(strings.len(), 4096);
        assert_eq!(strings.value(4095), huge);
        verify(
            &input,
            UInt32Array::from(vec![Some(2), None, Some(0), Some(1), Some(0)]),
        );
        verify(&input, UInt32Array::from(Vec::<u32>::new()));
    }
    #[test]
    fn dictionary_take_retains_entire_values_child() {
        let values: ArrayRef = Arc::new(StringArray::from(vec![
            Some("selected"),
            Some("x".repeat(131_071).as_str()),
            None,
        ]));
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), None, Some(2)]),
            values.clone(),
        )
        .unwrap();
        let input = batch(vec![Arc::new(dictionary)]);
        let output = verify(
            &input,
            UInt32Array::from(vec![Some(0), Some(0), None, Some(2)]),
        );
        let dictionary = output
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .unwrap();
        assert_eq!(dictionary.values().to_data(), values.to_data());
        assert_eq!(dictionary.keys().value(0), 0);
    }
    #[test]
    fn nullable_decimal_boolean_null_and_all_null_bytes() {
        let decimal = Decimal128Array::from(vec![Some(-1234), None, Some(5678)])
            .with_precision_and_scale(20, 3)
            .unwrap();
        let input = batch(vec![
            Arc::new(decimal),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            Arc::new(NullArray::new(3)),
            Arc::new(StringArray::from(vec![None::<&str>; 3])),
        ]);
        verify(
            &input,
            UInt32Array::from(
                (0..257)
                    .map(|i| if i % 5 == 0 { None } else { Some(i % 3) })
                    .collect::<Vec<_>>(),
            ),
        );
        verify(&input, UInt32Array::from(vec![None::<u32>; 19]));
        verify(&input, UInt32Array::from(Vec::<u32>::new()));
    }
    #[test]
    fn maxima_cover_every_batch_and_decline_mismatch_views_overflow() {
        let first = batch(vec![Arc::new(StringArray::from(vec!["a"]))]);
        let second = batch(vec![Arc::new(StringArray::from(vec!["longer"]))]);
        let cap = GatherCopyBound::from_batches(&first.schema(), &[first.clone(), second.clone()])
            .unwrap();
        let taken = verify(&second, UInt32Array::from(vec![0; 99]));
        assert!(
            owned_input_batch_charge(&taken).unwrap()
                <= cap.gather(99).unwrap().max_bytes().unwrap()
        );
        let wrong = batch(vec![Arc::new(Int64Array::from(vec![1]))]);
        assert!(GatherCopyBound::from_batches(&first.schema(), &[wrong.clone()]).is_none());
        assert!(GatherCopyBound::from_batches(&wrong.schema(), &[wrong])
            .unwrap()
            .gather(usize::MAX)
            .is_none());
        let view = batch(vec![Arc::new(StringViewArray::from(vec!["v"]))]);
        assert!(GatherCopyBound::from_batches(&view.schema(), &[view]).is_none());
    }
}

#[cfg(test)]
mod resident_gather_tests {
    use super::*;
    use crate::physical::operators::spillable::owned_input_batch_charge;
    use crate::physical::{
        operators::{FilterExec, MemoryTableExec, ProjectExec},
        PhysicalOperator,
    };
    use crate::planner::{BinaryOp, ScalarValue};
    use arrow::array::{
        ArrayRef, DictionaryArray, Int32Array, Int64Array, StringArray, StringViewArray,
        UInt32Array,
    };
    use arrow::datatypes::Int32Type;
    use futures::TryStreamExt;

    fn input() -> Arc<MemoryTableExec> {
        let dict = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), None, Some(1), Some(0)]),
            Arc::new(StringArray::from(vec![
                Some("dictionary".repeat(1000)),
                None,
            ])),
        )
        .unwrap();
        let first = RecordBatch::try_from_iter(vec![
            (
                "id",
                Arc::new(Int64Array::from(vec![0, 1, 2, 3])) as ArrayRef,
            ),
            (
                "text",
                Arc::new(StringArray::from(vec![
                    Some("parent".repeat(1000)),
                    Some("retained".repeat(1000)),
                    None,
                    Some("z".into()),
                ])) as ArrayRef,
            ),
            ("dict", Arc::new(dict) as ArrayRef),
        ])
        .unwrap();
        // Nullable take indices intentionally synthesize NULLs below, so the
        // fixture schema must permit them even in its all-valid id column.
        let first = RecordBatch::try_new(
            Arc::new(Schema::new(
                first
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone().with_nullable(true))
                    .collect::<Vec<_>>(),
            )),
            first.columns().to_vec(),
        )
        .unwrap();
        let sliced = first.slice(1, 3);
        Arc::new(MemoryTableExec::new(
            "resident",
            first.schema(),
            vec![first, sliced],
            None,
        ))
    }
    async fn check(op: &dyn PhysicalOperator) -> usize {
        let capability = op.resident_gather_copy_bound().unwrap();
        let mut rows = 0;
        for p in 0..op.output_partitions() {
            let mut output = op.execute(p).await.unwrap();
            while let Some(batch) = output.try_next().await.unwrap() {
                rows += batch.num_rows();
                let indices = if batch.num_rows() == 0 {
                    UInt32Array::from(Vec::<u32>::new())
                } else {
                    UInt32Array::from(
                        (0..41)
                            .map(|i| {
                                if i % 7 == 0 {
                                    None
                                } else {
                                    Some((i as usize % batch.num_rows()) as u32)
                                }
                            })
                            .collect::<Vec<_>>(),
                    )
                };
                let taken = RecordBatch::try_new(
                    batch.schema(),
                    batch
                        .columns()
                        .iter()
                        .map(|a| arrow::compute::take(a.as_ref(), &indices, None).unwrap())
                        .collect(),
                )
                .unwrap();
                assert!(
                    owned_input_batch_charge(&taken).unwrap()
                        <= capability
                            .gather(indices.len())
                            .unwrap()
                            .max_bytes()
                            .unwrap()
                );
                assert!(
                    owned_input_batch_charge(&batch).unwrap()
                        <= capability
                            .gather(batch.num_rows())
                            .unwrap()
                            .max_bytes()
                            .unwrap()
                );
            }
        }
        rows
    }
    #[tokio::test]
    async fn filtered_projected_duplicate_alias_dictionary_and_sliced_text() {
        for selected in [false, true] {
            let filter: Arc<dyn PhysicalOperator> = Arc::new(FilterExec::new(
                input(),
                Expr::Literal(ScalarValue::Boolean(selected)),
            ));
            let project = ProjectExec::new(
                filter,
                vec![
                    Expr::Alias {
                        expr: Box::new(Expr::column("text")),
                        name: "renamed".into(),
                    },
                    Expr::column("dict"),
                    Expr::column("dict"),
                ],
                Arc::new(Schema::new(vec![
                    Field::new("renamed", DataType::Utf8, true),
                    Field::new("d", DataType::Utf8, true),
                    Field::new("duplicate", DataType::Utf8, true),
                ])),
            );
            assert_eq!(check(&project).await, if selected { 7 } else { 0 });
        }
        let filter = FilterExec::new(
            input(),
            Expr::BinaryExpr {
                left: Box::new(Expr::column("id")),
                op: BinaryOp::GtEq,
                right: Box::new(Expr::Literal(ScalarValue::Int64(2))),
            },
        );
        assert_eq!(check(&filter).await, 4);
    }
    #[test]
    fn computed_projection_views_and_incompatible_merge_decline() {
        let computed = ProjectExec::new(
            input(),
            vec![Expr::Literal(ScalarValue::Int64(1))],
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)])),
        );
        assert!(computed.resident_gather_copy_bound().is_none());
        let view = RecordBatch::try_from_iter(vec![(
            "view",
            Arc::new(StringViewArray::from(vec!["x"])) as ArrayRef,
        )])
        .unwrap();
        let view = MemoryTableExec::new("view", view.schema(), vec![view], None);
        assert!(view.resident_gather_copy_bound().is_none());
        let mut original = input().resident_gather_copy_bound().unwrap();
        let before = original.gather(10).unwrap().max_bytes();
        let different = RecordBatch::try_from_iter(vec![(
            "v",
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        )])
        .unwrap();
        assert!(original
            .merge(GatherCopyBound::from_batches(&different.schema(), &[different]).unwrap())
            .is_none());
        assert_eq!(before, original.gather(10).unwrap().max_bytes());
    }
}

#[cfg(test)]
mod take_only_tests {
    use super::*;
    use crate::physical::operators::spillable::owned_input_batch_charge;
    use arrow::array::{
        Array, ArrayRef, BooleanArray, Decimal128Array, DictionaryArray, Int32Array, Int64Array,
        NullArray, StringArray, UInt32Array,
    };
    use arrow::datatypes::Int32Type;
    fn batch(arrays: Vec<ArrayRef>) -> RecordBatch {
        let schema = Arc::new(Schema::new(
            arrays
                .iter()
                .enumerate()
                .map(|(i, a)| Field::new(format!("c{i}"), a.data_type().clone(), true))
                .collect::<Vec<_>>(),
        ));
        RecordBatch::try_new(schema, arrays).unwrap()
    }
    fn take(input: &RecordBatch, indices: &UInt32Array) -> RecordBatch {
        RecordBatch::try_new(
            input.schema(),
            input
                .columns()
                .iter()
                .map(|a| arrow::compute::take(a.as_ref(), indices, None).unwrap())
                .collect(),
        )
        .unwrap()
    }
    fn checked(input: &RecordBatch, indices: UInt32Array) {
        let cap = GatherCopyBound::from_batches(&input.schema(), &[input.clone()]).unwrap();
        let index_bytes = indices.nulls().map_or(0, |n| n.buffer().len());
        let out = take(input, &indices);
        assert!(
            owned_input_batch_charge(&out).unwrap()
                <= cap
                    .take_only(indices.len(), index_bytes)
                    .unwrap()
                    .max_bytes()
                    .unwrap()
        );
    }
    #[test]
    fn large_source_small_repeated_take_drops_identity_charge() {
        let source = batch(vec![
            Arc::new(Int64Array::from_iter_values(0..100_000)),
            Arc::new(StringArray::from_iter_values((0..100_000).map(|_| "small"))),
        ]);
        let cap = GatherCopyBound::from_batches(&source.schema(), &[source.clone()]).unwrap();
        let bound = cap.take_only(3, 0).unwrap().max_bytes().unwrap();
        assert!(bound * 100 < cap.gather(3).unwrap().max_bytes().unwrap());
        let output = take(&source, &UInt32Array::from(vec![99_999, 99_999, 0]));
        assert!(owned_input_batch_charge(&output).unwrap() <= bound);
        assert_eq!(
            output
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .as_ref(),
            &[99_999, 99_999, 0]
        );
        checked(&source, UInt32Array::from(Vec::<u32>::new()));
        assert!(cap.take_only(usize::MAX, 0).is_none());
        assert!(cap.take_only(1, usize::MAX).is_none());
    }
    #[test]
    fn dictionary_child_null_decimal_boolean_and_empty_layouts_are_kept() {
        let dictionary = DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![Some(0), Some(1), None]),
            Arc::new(StringArray::from(vec![
                Some("x"),
                Some("long".repeat(32_768).as_str()),
                None,
            ])),
        )
        .unwrap();
        let input = batch(vec![
            Arc::new(dictionary),
            Arc::new(
                Decimal128Array::from(vec![Some(-123), None, Some(456)])
                    .with_precision_and_scale(20, 3)
                    .unwrap(),
            ),
            Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])),
            Arc::new(NullArray::new(3)),
            Arc::new(StringArray::from(vec![None::<&str>; 3])),
        ]);
        checked(
            &input,
            UInt32Array::from(vec![Some(0), Some(0), Some(2), None, Some(1)]),
        );
        checked(&input, UInt32Array::from(vec![None::<u32>; 17]));
        checked(&input, UInt32Array::from(Vec::<u32>::new()));
        let cap = GatherCopyBound::from_batches(&input.schema(), &[input.clone()]).unwrap();
        assert!(
            cap.take_only(1, 0).unwrap().max_bytes().unwrap() >= 131_072,
            "entire dictionary child is retained"
        );
    }
    #[test]
    fn sliced_index_validity_extent_is_required_not_logical_row_count() {
        let input = batch(vec![Arc::new(Int64Array::from(vec![42]))]);
        let parent = UInt32Array::from(
            (0..100_000)
                .map(|i| if i % 2 == 0 { Some(0) } else { None })
                .collect::<Vec<_>>(),
        );
        let indices = parent.slice(50_000, 2);
        let extent = indices.nulls().unwrap().buffer().len();
        assert!(extent > 1000);
        let output = take(&input, &indices);
        let actual = owned_input_batch_charge(&output).unwrap();
        let cap = GatherCopyBound::from_batches(&input.schema(), &[input]).unwrap();
        assert!(actual <= cap.take_only(2, extent).unwrap().max_bytes().unwrap());
        // Negative proof: lying about index validity makes BOTH old compact
        // assumptions insufficient. Never use these descriptors in a queue.
        assert!(actual > cap.take_only(2, 0).unwrap().max_bytes().unwrap());
        assert!(actual > cap.gather(2).unwrap().max_bytes().unwrap());
    }
}

#[cfg(test)]
mod prepared_layout_composition_tests {
    use super::*;
    #[test]
    fn transformed_overflow_is_unknown_without_forged_small_bound() {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, true)]));
        let empty = RecordBatch::new_empty(schema.clone());
        let mut bound = QueueCopyBound::from_batches(&schema, &[empty]).unwrap();
        // A conservative metadata maximum need not equal an actual batch size.
        // It may overflow a later checked validity calculation: decline it.
        bound.max_rows = usize::MAX;
        let output = crate::physical::PreparedOutputBound::Layouts(
            PreparedOutputLayouts::from_bound(bound).unwrap(),
        );
        assert!(output.max_bytes().is_some());
        assert!(matches!(
            output.filtered(),
            crate::physical::PreparedOutputBound::Unknown
        ));
        assert!(PreparedOutputLayouts {
            variants: vec![],
            gathers: None
        }
        .max_bytes()
        .is_none());
    }
}

#[cfg(test)]
mod membership_layout_tests {
    use super::*;
    use arrow::array::{ArrayRef, DictionaryArray, Float64Array, Int32Array, Int64Array};
    use arrow::datatypes::Int32Type;
    fn bound(name: &str, array: ArrayRef) -> QueueCopyBound {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                name,
                array.data_type().clone(),
                true,
            )])),
            vec![array],
        )
        .unwrap();
        QueueCopyBound::from_batches(&batch.schema(), &[batch]).unwrap()
    }
    #[test]
    fn membership_requires_every_actual_variant_and_runtime_column_resolution() {
        let column = Column::new("x");
        let integer = bound("x", Arc::new(Int64Array::from(vec![Some(1), None])));
        let dictionary = bound(
            "x",
            Arc::new(
                DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![Some(0), None]),
                    Arc::new(Int64Array::from(vec![Some(1), None])),
                )
                .unwrap(),
            ),
        );
        let mut layouts = PreparedOutputLayouts {
            variants: vec![integer.clone(), dictionary],
            gathers: None,
        };
        assert!(layouts.int64_membership_column(0, &column));
        assert!(!layouts.int64_membership_column(1, &column));
        assert!(!layouts.int64_membership_column(0, &Column::new("missing")));
        layouts
            .variants
            .push(bound("x", Arc::new(Float64Array::from(vec![1.0]))));
        assert!(!layouts.int64_membership_column(0, &column));
        assert!(!PreparedOutputLayouts {
            variants: vec![],
            gathers: None
        }
        .int64_membership_column(0, &column));
        let renamed = bound("other", Arc::new(Int64Array::from(vec![1])));
        assert!(!renamed.int64_membership_column(0, &column));
    }
}

/// Row-store gather materializes ONLY its validated fixed-width physical types.
/// Empty arrays supply structural Arrow charges, not value samples or statistics.
/// Caller must prove the store was built from this exact schema across all batches.
pub(crate) fn row_store_output_copy_bound(
    build_schema: &SchemaRef,
    probe: &GatherCopyBound,
    probe_keep: Option<&[bool]>,
    swapped: bool,
    declared: &SchemaRef,
    max_rows: usize,
) -> Option<PreparedOutputLayouts> {
    if build_schema.fields().is_empty()
        || !build_schema.fields().iter().all(|f| {
            matches!(
                f.data_type(),
                DataType::Int64 | DataType::Float64 | DataType::Int32 | DataType::Date32
            )
        })
    {
        return None;
    }
    let structural = RecordBatch::new_empty(build_schema.clone());
    inner_output_copy_bound(
        std::slice::from_ref(&structural),
        probe,
        None,
        probe_keep,
        swapped,
        declared,
        max_rows,
    )
}

/// Preserve all prepared probe physical variants and their exact gather metadata.
pub(crate) fn row_store_prepared_output_copy_bound(
    build_schema: &SchemaRef,
    probe: &PreparedOutputLayouts,
    probe_keep: Option<&[bool]>,
    swapped: bool,
    schema: &SchemaRef,
    rows: usize,
) -> Option<PreparedOutputLayouts> {
    let input = probe.gathers.as_ref()?;
    if input.is_empty() || input.len() != probe.variants.len() {
        return None;
    }
    let maximum = input.len().checked_mul(2)?;
    let mut variants = Vec::new();
    let mut gathers = Vec::new();
    variants.try_reserve_exact(maximum).ok()?;
    gathers.try_reserve_exact(maximum).ok()?;
    for source in input {
        let next =
            row_store_output_copy_bound(build_schema, source, probe_keep, swapped, schema, rows)?;
        variants.extend(next.variants);
        gathers.extend(next.gathers?);
    }
    let result = PreparedOutputLayouts {
        variants,
        gathers: Some(gathers),
    };
    result.max_bytes()?;
    Some(result)
}
