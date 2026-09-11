//! Error types for the query engine

use thiserror::Error;

/// Result type alias for query engine operations
pub type Result<T> = std::result::Result<T, QueryError>;

/// The query boundary where a declared partition failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionPhase {
    Execution,
    Collection,
}
impl std::fmt::Display for PartitionPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Execution => "execution",
            Self::Collection => "collection",
        })
    }
}

/// Main error type for the query engine
#[derive(Error, Debug)]
pub enum QueryError {
    /// Share an original deferred error without erasing its classification.
    #[error(transparent)]
    Shared(std::sync::Arc<QueryError>),
    /// Add execution context without erasing the typed cause. This is diagnostic
    /// context, not permission to retry a partition or replay consumed input.
    #[error("Execution error: Partition {partition_id} {phase} failed: {source}")]
    Partition {
        partition_id: usize,
        phase: PartitionPhase,
        #[source]
        source: Box<QueryError>,
    },
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

    /// An atomic query-pool admission denial, distinct from allocator, codec,
    /// arithmetic and execution errors. It does not imply that an operator has
    /// a resumable cursor or that retrying consumed input is safe.
    #[error("Execution error: Memory limit exceeded in '{pool}': requested {requested} additional bytes, used {used}, limit {limit}")]
    MemoryLimit {
        pool: String,
        requested: usize,
        used: usize,
        limit: usize,
    },

    #[error("Storage error: {0}")]
    Storage(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Arrow error: {0}")]
    Arrow(arrow::error::ArrowError),

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

impl QueryError {
    /// Classify actual admission pressure without parsing display text. Shared
    /// deferred errors and partition context retain their original typed cause.
    pub fn is_memory_limit(&self) -> bool {
        matches!(self.root(), Self::MemoryLimit { .. })
    }

    pub fn root(&self) -> &Self {
        let mut error = self;
        loop {
            error = match error {
                Self::Shared(inner) => inner.as_ref(),
                Self::Partition { source, .. } => source.as_ref(),
                _ => return error,
            };
        }
    }

    /// The variant name, for structured error reporting (the `serve` query
    /// log groups failures by it). Stable strings: they are part of the
    /// `/queries` JSON contract.
    pub fn kind(&self) -> &'static str {
        // Shared is transparent to the public category. Partition context keeps
        // the historical Execution category even when its typed root is IO, etc.
        let mut error = self;
        while let Self::Shared(inner) = error {
            error = inner.as_ref();
        }
        match error {
            QueryError::Shared(_) => unreachable!("shared context was unwrapped"),
            QueryError::Partition { .. } => "Execution",
            QueryError::Parse(_) => "Parse",
            QueryError::Plan(_) => "Plan",
            QueryError::Bind(_) => "Bind",
            QueryError::Type(_) => "Type",
            QueryError::Execution(_) => "Execution",
            // Preserve the existing public query-log classification and display.
            // Internal recovery must use the typed variant, never this category.
            QueryError::MemoryLimit { .. } => "Execution",
            QueryError::Storage(_) => "Storage",
            QueryError::Io(_) => "Io",
            QueryError::Arrow(_) => "Arrow",
            QueryError::Parquet(_) => "Parquet",
            QueryError::TableNotFound(_) => "TableNotFound",
            QueryError::ColumnNotFound(_) => "ColumnNotFound",
            QueryError::InvalidArgument(_) => "InvalidArgument",
            QueryError::NotImplemented(_) => "NotImplemented",
            QueryError::Internal(_) => "Internal",
        }
    }
}

impl From<arrow::error::ArrowError> for QueryError {
    fn from(e: arrow::error::ArrowError) -> Self {
        // QE_ERR_BT=1: print where an arrow error entered the engine —
        // schema-mismatch errors surface far from the operator that built
        // the offending batch.
        if std::env::var("QE_ERR_BT").is_ok() {
            eprintln!(
                "[err-bt] ArrowError: {e}\n{}",
                std::backtrace::Backtrace::force_capture()
            );
        }
        QueryError::Arrow(e)
    }
}

impl From<sqlparser::parser::ParserError> for QueryError {
    fn from(e: sqlparser::parser::ParserError) -> Self {
        QueryError::Parse(e.to_string())
    }
}

#[cfg(test)]
mod shared_error_tests {
    use super::*;
    #[test]
    fn shared_error_preserves_original_kind_display_and_io_identity() {
        let original = std::sync::Arc::new(QueryError::Io(std::io::Error::from_raw_os_error(13)));
        let shared = QueryError::Shared(std::sync::Arc::new(QueryError::Shared(original.clone())));
        assert_eq!(shared.kind(), "Io");
        assert_eq!(shared.to_string(), original.to_string());
        assert!(std::ptr::eq(shared.root(), original.as_ref()));
        let QueryError::Io(io) = shared.root() else {
            panic!()
        };
        assert_eq!(io.raw_os_error(), Some(13));
    }
}
