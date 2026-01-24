//! CLI helper module for the interactive REPL
//!
//! Provides:
//! - Tab completion for SQL keywords, table names, and column names
//! - Syntax highlighting for SQL
//! - Output format options (table, CSV, JSON, vertical)

mod helper;
mod output;

pub use helper::ReplHelper;
pub use output::{OutputFormat, OutputFormatter};
