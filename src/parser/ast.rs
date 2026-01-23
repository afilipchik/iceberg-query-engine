//! AST wrapper types
//!
//! Re-exports sqlparser types with convenience methods

pub use sqlparser::ast::{
    BinaryOperator, DataType, Expr as SqlExpr, FunctionArg, FunctionArgExpr,
    GroupByExpr, Ident, JoinConstraint, JoinOperator, ObjectName, OrderByExpr,
    Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins,
    UnaryOperator, Value,
};

/// Extension trait for Ident
pub trait IdentExt {
    fn as_str(&self) -> &str;
}

impl IdentExt for Ident {
    fn as_str(&self) -> &str {
        &self.value
    }
}

/// Extension trait for ObjectName
pub trait ObjectNameExt {
    fn table_name(&self) -> String;
}

impl ObjectNameExt for ObjectName {
    fn table_name(&self) -> String {
        self.0.iter().map(|i| i.value.clone()).collect::<Vec<_>>().join(".")
    }
}

/// Get the table name from a TableFactor
pub fn table_factor_name(tf: &TableFactor) -> Option<String> {
    match tf {
        TableFactor::Table { name, .. } => Some(name.table_name()),
        _ => None,
    }
}

/// Get the alias from a TableFactor
pub fn table_factor_alias(tf: &TableFactor) -> Option<String> {
    match tf {
        TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
        _ => None,
    }
}
