//! Bounded schema proof for skipping dictionary payloads, not a memory admission.
use arrow::datatypes::{DataType, Field, Schema};

const LIMIT: usize = 64;

pub(super) struct DictionaryProjection {
    ids: [i64; LIMIT],
    len: usize,
    required: u64,
}

impl DictionaryProjection {
    /// None retains ordinary decoding. Definitions follow Arrow's full-schema
    /// preorder and first-field rule, including IDs shared by projected fields.
    #[allow(deprecated)]
    pub(super) fn bind(schema: &Schema, projection: Option<&[usize]>) -> Option<Self> {
        let projection = projection?;
        let mut ids = [0; LIMIT];
        let mut definitions: [Option<&Field>; LIMIT] = [None; LIMIT];
        let mut len = 0;
        for field in schema.fields() {
            walk_field(field, 0, &mut |field| {
                if let Some(id) = field.dict_id() {
                    if !ids[..len].contains(&id) {
                        if len == LIMIT {
                            return None;
                        }
                        ids[len] = id;
                        definitions[len] = Some(field);
                        len += 1;
                    }
                }
                Some(())
            })?;
        }

        let mut dependencies = [0u64; LIMIT];
        for index in 0..len {
            let DataType::Dictionary(_, value) = definitions[index]?.data_type() else {
                return None;
            };
            walk_type(value, 0, &mut |field| {
                if let Some(id) = field.dict_id() {
                    let dependency = ids[..len].iter().position(|known| *known == id)?;
                    dependencies[index] |= 1u64 << dependency;
                }
                Some(())
            })?;
        }
        // Fixed-size transitive closure: no heap or recursive graph traversal.
        for via in 0..len {
            for from in 0..len {
                if dependencies[from] & (1u64 << via) != 0 {
                    dependencies[from] |= dependencies[via];
                }
            }
        }
        if (0..len).any(|i| dependencies[i] & (1u64 << i) != 0) {
            return None;
        }

        let mut required = 0;
        for &column in projection {
            walk_field(schema.fields().get(column)?, 0, &mut |field| {
                if let Some(id) = field.dict_id() {
                    let index = ids[..len].iter().position(|known| *known == id)?;
                    required |= (1u64 << index) | dependencies[index];
                }
                Some(())
            })?;
        }
        Some(Self { ids, len, required })
    }

    /// Unknown IDs remain errors, even if the selected fields are numeric.
    pub(super) fn required(&self, id: i64) -> Option<bool> {
        self.ids[..self.len]
            .iter()
            .position(|known| *known == id)
            .map(|index| self.required & (1u64 << index) != 0)
    }
}

fn walk_field<'a>(
    field: &'a Field,
    depth: usize,
    visit: &mut impl FnMut(&'a Field) -> Option<()>,
) -> Option<()> {
    if depth > LIMIT {
        return None;
    }
    visit(field)?;
    let value = match field.data_type() {
        DataType::Dictionary(_, value) => value.as_ref(),
        value => value,
    };
    walk_type(value, depth + 1, visit)
}

fn walk_type<'a>(
    data_type: &'a DataType,
    depth: usize,
    visit: &mut impl FnMut(&'a Field) -> Option<()>,
) -> Option<()> {
    if depth > LIMIT {
        return None;
    }
    match data_type {
        DataType::Struct(fields) => {
            for field in fields {
                walk_field(field, depth + 1, visit)?;
            }
        }
        DataType::Union(fields, _) => {
            for (_, field) in fields.iter() {
                walk_field(field, depth + 1, visit)?;
            }
        }
        DataType::List(field)
        | DataType::ListView(field)
        | DataType::LargeList(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => walk_field(field, depth + 1, visit)?,
        // Match Arrow58's dictionary-definition traversal. Run ends are integers.
        DataType::RunEndEncoded(_, values) => walk_field(values, depth + 1, visit)?,
        // A bare nested Dictionary has no Field carrying its ID. Do not guess.
        DataType::Dictionary(..) => return None,
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(..)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(..)
        | DataType::Time64(..)
        | DataType::Duration(..)
        | DataType::Interval(..)
        | DataType::Binary
        | DataType::FixedSizeBinary(..)
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => {}
    }
    Some(())
}
