//! Schema types for the query engine

use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

// Engine-created field identity is separate from its presentation name. The
// provider-to-PlanSchema conversion deliberately reconstructs trusted identity.
const BOUND_COLUMN_NAME: &str = "query_engine.bound_column.name";
const BOUND_COLUMN_RELATION: &str = "query_engine.bound_column.relation";

pub(crate) fn arrow_field_identity(field: &Field) -> Option<(Option<&str>, &str)> {
    let name = field.metadata().get(BOUND_COLUMN_NAME)?;
    Some((
        field
            .metadata()
            .get(BOUND_COLUMN_RELATION)
            .map(String::as_str),
        name.as_str(),
    ))
}

/// Resolve a bound column without using display equality for annotated fields.
/// Annotated qualified references require the same relation namespace. Raw
/// provider schemas retain unambiguous compatibility lookup.
pub(crate) fn resolve_arrow_column(schema: &ArrowSchema, column: &Column) -> Option<usize> {
    // Outer None means absent; Some(None) means ambiguous and must not fall back.
    fn matching(mut positions: impl Iterator<Item = usize>) -> Option<Option<usize>> {
        let first = positions.next()?;
        Some(positions.next().is_none().then_some(first))
    }
    if let Some(relation) = column.relation.as_deref() {
        if let Some(found) =
            matching(schema.fields().iter().enumerate().filter_map(|(i, field)| {
                arrow_field_identity(field)
                    .is_some_and(|(r, name)| r == Some(relation) && name == column.name)
                    .then_some(i)
            }))
        {
            return found;
        }
        // A joined legacy spelling cannot disambiguate dotted components.
        if !relation.contains('.') && !column.name.contains('.') {
            if let Some(found) =
                matching(schema.fields().iter().enumerate().filter_map(|(i, field)| {
                    (arrow_field_identity(field).is_none()
                        && field
                            .name()
                            .strip_prefix(relation)
                            .and_then(|suffix| suffix.strip_prefix('.'))
                            == Some(column.name.as_str()))
                    .then_some(i)
                }))
            {
                return found;
            }
        }
    }
    if let Some(found) = matching(schema.fields().iter().enumerate().filter_map(|(i, field)| {
        let identity = arrow_field_identity(field);
        if column.relation.is_some() && identity.is_some() {
            return None;
        }
        let name = identity.map_or(field.name().as_str(), |(_, name)| name);
        (name == column.name).then_some(i)
    })) {
        return found;
    }
    matching(schema.fields().iter().enumerate().filter_map(|(i, field)| {
        (arrow_field_identity(field).is_none()
            && field
                .name()
                .split_once('.')
                .is_some_and(|(_, name)| name == column.name))
        .then_some(i)
    }))
    .flatten()
}

/// A column in a schema
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Column {
    /// Optional table/relation name
    pub relation: Option<String>,
    /// Column name
    pub name: String,
}

impl Column {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            relation: None,
            name: name.into(),
        }
    }

    pub fn new_qualified(relation: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            relation: Some(relation.into()),
            name: name.into(),
        }
    }

    /// Returns the fully qualified name
    pub fn qualified_name(&self) -> String {
        match &self.relation {
            Some(r) => format!("{}.{}", r, self.name),
            None => self.name.clone(),
        }
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.qualified_name())
    }
}

/// Schema field with metadata
#[derive(Debug, Clone, PartialEq)]
pub struct SchemaField {
    pub name: String,
    pub data_type: ArrowDataType,
    pub nullable: bool,
    pub relation: Option<String>,
}

impl SchemaField {
    pub fn new(name: impl Into<String>, data_type: ArrowDataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            relation: None,
        }
    }

    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    pub fn with_relation(mut self, relation: impl Into<String>) -> Self {
        self.relation = Some(relation.into());
        self
    }

    pub fn to_arrow_field(&self) -> Field {
        // Use qualified name to handle self-joins and ambiguous column names
        let mut metadata = HashMap::from([(BOUND_COLUMN_NAME.to_string(), self.name.clone())]);
        if let Some(relation) = &self.relation {
            metadata.insert(BOUND_COLUMN_RELATION.to_string(), relation.clone());
        }
        Field::new(self.qualified_name(), self.data_type.clone(), self.nullable)
            .with_metadata(metadata)
    }

    pub fn qualified_name(&self) -> String {
        match &self.relation {
            Some(r) => format!("{}.{}", r, self.name),
            None => self.name.clone(),
        }
    }
}

/// Schema representing the output of a plan node
#[derive(Debug, Clone)]
pub struct PlanSchema {
    fields: Vec<SchemaField>,
    /// Map from column name to field index (for unqualified lookups)
    name_index: BTreeMap<String, Vec<usize>>,
    /// Map from qualified name to field index
    qualified_index: BTreeMap<String, BTreeMap<String, Vec<usize>>>,
}

impl PartialEq for PlanSchema {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl PlanSchema {
    pub fn new(fields: Vec<SchemaField>) -> Self {
        let mut name_index: BTreeMap<String, Vec<usize>> = BTreeMap::new();
        let mut qualified_index: BTreeMap<String, BTreeMap<String, Vec<usize>>> = BTreeMap::new();

        for (i, field) in fields.iter().enumerate() {
            name_index.entry(field.name.clone()).or_default().push(i);

            if let Some(relation) = &field.relation {
                qualified_index
                    .entry(relation.clone())
                    .or_default()
                    .entry(field.name.clone())
                    .or_default()
                    .push(i);
            }
        }

        Self {
            fields,
            name_index,
            qualified_index,
        }
    }

    pub fn empty() -> Self {
        Self::new(vec![])
    }

    pub fn fields(&self) -> &[SchemaField] {
        &self.fields
    }

    pub fn field(&self, index: usize) -> Option<&SchemaField> {
        self.fields.get(index)
    }

    pub fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Find a column by name (unqualified)
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.name_index.get(name).and_then(|indices| {
            if indices.len() == 1 {
                Some(indices[0])
            } else {
                None // Ambiguous
            }
        })
    }

    /// Find a column by qualified name
    pub fn index_of_qualified(&self, relation: Option<&str>, name: &str) -> Option<usize> {
        match relation {
            Some(rel) => {
                let positions = self.qualified_index.get(rel)?.get(name)?;
                (positions.len() == 1).then(|| positions[0])
            }
            None => self.index_of(name),
        }
    }

    /// Find column handling ambiguity
    pub fn resolve_column(&self, col: &Column) -> Option<(usize, &SchemaField)> {
        let idx = self.index_of_qualified(col.relation.as_deref(), &col.name)?;
        Some((idx, &self.fields[idx]))
    }

    /// Convert to Arrow schema
    pub fn to_arrow_schema(&self) -> ArrowSchema {
        let fields: Vec<Field> = self.fields.iter().map(|f| f.to_arrow_field()).collect();
        ArrowSchema::new(fields)
    }

    /// Convert to Arc<ArrowSchema>
    pub fn to_arrow_schema_ref(&self) -> Arc<ArrowSchema> {
        Arc::new(self.to_arrow_schema())
    }

    /// Merge two schemas (for joins)
    pub fn merge(&self, other: &PlanSchema) -> Self {
        let mut fields = self.fields.clone();
        fields.extend(other.fields.iter().cloned());
        Self::new(fields)
    }

    /// Build a PlanSchema from an EXECUTION-time Arrow schema whose field
    /// names may already be qualified ("orders.o_orderkey").
    ///
    /// `From<&ArrowSchema>` keeps the whole string as the field name, so an
    /// UNQUALIFIED column reference (`o_orderkey`) does not resolve against
    /// it. Type-inference call sites swallow that failure with a default
    /// type, which silently changes results: an integer SUM inferred as
    /// Float64 finalizes to a Float64 scalar that the Int64 output builder
    /// then writes as NULL. Splitting the qualifier off makes both
    /// `o_orderkey` and `orders.o_orderkey` resolve, and is strictly more
    /// permissive than the unsplit form (duplicate bare names stay ambiguous
    /// and resolve exactly as before).
    pub fn from_qualified_arrow(schema: &ArrowSchema) -> Self {
        let fields: Vec<SchemaField> = schema
            .fields()
            .iter()
            .map(|f| {
                let name = f.name();
                let field = if let Some((relation, column)) = arrow_field_identity(f) {
                    let mut field = SchemaField::new(column, f.data_type().clone());
                    field.relation = relation.map(str::to_owned);
                    field
                } else {
                    match name.split_once('.') {
                        Some((rel, col)) if !rel.is_empty() && !col.is_empty() => {
                            SchemaField::new(col.to_string(), f.data_type().clone())
                                .with_relation(rel.to_string())
                        }
                        _ => SchemaField::new(name.clone(), f.data_type().clone()),
                    }
                };
                field.with_nullable(f.is_nullable())
            })
            .collect();
        Self::new(fields)
    }

    /// Project specific columns
    pub fn project(&self, indices: &[usize]) -> Self {
        let fields: Vec<SchemaField> = indices
            .iter()
            .filter_map(|&i| self.fields.get(i).cloned())
            .collect();
        Self::new(fields)
    }
}

impl From<&ArrowSchema> for PlanSchema {
    fn from(schema: &ArrowSchema) -> Self {
        let fields: Vec<SchemaField> = schema
            .fields()
            .iter()
            .map(|f| {
                SchemaField::new(f.name().clone(), f.data_type().clone())
                    .with_nullable(f.is_nullable())
            })
            .collect();
        Self::new(fields)
    }
}

impl From<ArrowSchema> for PlanSchema {
    fn from(schema: ArrowSchema) -> Self {
        Self::from(&schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_qualified_name() {
        let col = Column::new("id");
        assert_eq!(col.qualified_name(), "id");

        let col = Column::new_qualified("users", "id");
        assert_eq!(col.qualified_name(), "users.id");
    }

    #[test]
    fn test_schema_index_of() {
        let schema = PlanSchema::new(vec![
            SchemaField::new("id", ArrowDataType::Int64),
            SchemaField::new("name", ArrowDataType::Utf8),
        ]);

        assert_eq!(schema.index_of("id"), Some(0));
        assert_eq!(schema.index_of("name"), Some(1));
        assert_eq!(schema.index_of("missing"), None);
    }

    #[test]
    fn test_schema_qualified_lookup() {
        let schema = PlanSchema::new(vec![
            SchemaField::new("id", ArrowDataType::Int64).with_relation("users"),
            SchemaField::new("id", ArrowDataType::Int64).with_relation("orders"),
        ]);

        assert_eq!(schema.index_of_qualified(Some("users"), "id"), Some(0));
        assert_eq!(schema.index_of_qualified(Some("orders"), "id"), Some(1));
        assert_eq!(schema.index_of("id"), None); // Ambiguous
    }
}

#[cfg(test)]
mod qualified_identity_contract {
    use super::*;
    #[test]
    fn qualified_lookup_keeps_relation_and_name_boundaries() {
        let fields = vec![
            SchemaField::new("b.c", ArrowDataType::Int64).with_relation("a"),
            SchemaField::new("c", ArrowDataType::Float64).with_relation("a.b"),
        ];
        let schema = PlanSchema::new(fields);
        assert_eq!(schema.index_of_qualified(Some("a"), "b.c"), Some(0));
        assert_eq!(schema.index_of_qualified(Some("a.b"), "c"), Some(1));
        assert_eq!(
            schema
                .resolve_column(&Column::new_qualified("a", "b.c"))
                .unwrap()
                .1
                .data_type,
            ArrowDataType::Int64
        );
    }
    #[test]
    fn execution_arrow_roundtrip_preserves_qualified_and_literal_dotted_names() {
        let original = PlanSchema::new(vec![
            SchemaField::new("b.c", ArrowDataType::Int64).with_relation("a"),
            SchemaField::new("c", ArrowDataType::Float64).with_relation("a.b"),
            SchemaField::new("literal.dot", ArrowDataType::Int64),
        ]);
        let restored = PlanSchema::from_qualified_arrow(&original.to_arrow_schema());
        assert_eq!(restored, original);
    }
    #[test]
    fn duplicate_exact_qualified_identity_is_ambiguous() {
        let schema = PlanSchema::new(vec![
            SchemaField::new("id", ArrowDataType::Int64).with_relation("a"),
            SchemaField::new("id", ArrowDataType::Int64).with_relation("a"),
        ]);
        assert_eq!(schema.index_of_qualified(Some("a"), "id"), None);
    }
}

#[cfg(test)]
mod arrow_identity_resolution_contract {
    use super::*;
    #[test]
    fn qualified_pairs_and_literal_dotted_name_resolve_separately() {
        let schema = PlanSchema::new(vec![
            SchemaField::new("b.c", ArrowDataType::Int64).with_relation("a"),
            SchemaField::new("c", ArrowDataType::Int64).with_relation("a.b"),
            SchemaField::new("a.b.c", ArrowDataType::Int64),
        ])
        .to_arrow_schema();
        assert_eq!(
            resolve_arrow_column(&schema, &Column::new_qualified("a", "b.c")),
            Some(0)
        );
        assert_eq!(
            resolve_arrow_column(&schema, &Column::new_qualified("a.b", "c")),
            Some(1)
        );
        assert_eq!(
            resolve_arrow_column(&schema, &Column::new("a.b.c")),
            Some(2)
        );
    }
    #[test]
    fn unqualified_ambiguity_never_selects_first_annotated_field() {
        let schema = PlanSchema::new(vec![
            SchemaField::new("id", ArrowDataType::Int64).with_relation("a"),
            SchemaField::new("id", ArrowDataType::Int64).with_relation("b"),
        ])
        .to_arrow_schema();
        assert_eq!(resolve_arrow_column(&schema, &Column::new("id")), None);
        assert_eq!(
            resolve_arrow_column(&schema, &Column::new_qualified("a", "id")),
            Some(0)
        );
        assert_eq!(
            resolve_arrow_column(&schema, &Column::new_qualified("b", "id")),
            Some(1)
        );
    }
    #[test]
    fn qualified_reference_cannot_cross_an_annotated_namespace() {
        let schema = PlanSchema::new(vec![
            SchemaField::new("id", ArrowDataType::Int64).with_relation("a")
        ])
        .to_arrow_schema();
        assert_eq!(
            resolve_arrow_column(&schema, &Column::new_qualified("b", "id")),
            None
        );
        assert_eq!(
            resolve_arrow_column(&schema, &Column::new_qualified("a", "id")),
            Some(0)
        );
    }

    #[test]
    fn provider_schema_conversion_reconstructs_identity_from_actual_names() {
        let supplied =
            Field::new("real", ArrowDataType::Int64, true).with_metadata(HashMap::from([
                (BOUND_COLUMN_NAME.to_string(), "fake".to_string()),
                (BOUND_COLUMN_RELATION.to_string(), "spoof".to_string()),
            ]));
        let input = ArrowSchema::new(vec![supplied]);
        let logical = PlanSchema::from(&input);
        assert_eq!(logical.fields()[0].name, "real");
        assert_eq!(logical.fields()[0].relation, None);
        let rebound =
            PlanSchema::new(vec![logical.fields()[0].clone().with_relation("t")]).to_arrow_schema();
        assert_eq!(
            resolve_arrow_column(&rebound, &Column::new_qualified("t", "real")),
            Some(0)
        );
        assert_eq!(
            resolve_arrow_column(&rebound, &Column::new_qualified("spoof", "fake")),
            None
        );
    }
}
