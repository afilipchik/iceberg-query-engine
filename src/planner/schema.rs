//! Schema types for the query engine

use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema};
use std::collections::HashMap;
use std::sync::Arc;

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
        Field::new(self.qualified_name(), self.data_type.clone(), self.nullable)
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
    name_index: HashMap<String, Vec<usize>>,
    /// Map from qualified name to field index
    qualified_index: HashMap<String, usize>,
}

impl PartialEq for PlanSchema {
    fn eq(&self, other: &Self) -> bool {
        self.fields == other.fields
    }
}

impl PlanSchema {
    pub fn new(fields: Vec<SchemaField>) -> Self {
        let mut name_index: HashMap<String, Vec<usize>> = HashMap::new();
        let mut qualified_index = HashMap::new();

        for (i, field) in fields.iter().enumerate() {
            name_index.entry(field.name.clone()).or_default().push(i);

            qualified_index.insert(field.qualified_name(), i);
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
                let qualified = format!("{}.{}", rel, name);
                self.qualified_index.get(&qualified).copied()
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
