//! Error types for the query engine

use thiserror::Error;

/// Result type alias for query engine operations
pub type Result<T> = std::result::Result<T, QueryError>;

/// Main error type for the query engine
#[derive(Error, Debug)]
pub enum QueryError {
    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Plan error: {0}")]
    Plan(String),

    #[error("Bind error: {0}")]
    Bind(String),

    #[error("Type error: {0}")]
    Type(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Internal error: {0}")]
    Internal(String),
}

impl From<sqlparser::parser::ParserError> for QueryError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        QueryError::Parse(e.to_string())
    }
}
