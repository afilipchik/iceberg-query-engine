//! REPL helper providing completion, highlighting, and hints

use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Helper};
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::{Arc, RwLock};

/// SQL keywords for completion and highlighting
pub const SQL_KEYWORDS: &[&str] = &[
    // Data Query
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP",
    "BY",
    "HAVING",
    "ORDER",
    "ASC",
    "DESC",
    "LIMIT",
    "OFFSET",
    "DISTINCT",
    "ALL",
    "AS",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",
    // Joins
    "JOIN",
    "INNER",
    "LEFT",
    "RIGHT",
    "FULL",
    "OUTER",
    "CROSS",
    "ON",
    "USING",
    "NATURAL",
    "SEMI",
    "ANTI",
    // Set operations
    "UNION",
    "INTERSECT",
    "EXCEPT",
    "MINUS",
    // Predicates
    "AND",
    "OR",
    "NOT",
    "IN",
    "EXISTS",
    "BETWEEN",
    "LIKE",
    "ILIKE",
    "IS",
    "NULL",
    "TRUE",
    "FALSE",
    // Aggregate functions
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "FIRST",
    "LAST",
    // Scalar functions
    "COALESCE",
    "NULLIF",
    "CAST",
    "EXTRACT",
    "SUBSTRING",
    "SUBSTR",
    "TRIM",
    "UPPER",
    "LOWER",
    "LENGTH",
    "CONCAT",
    "REPLACE",
    "POSITION",
    "STRPOS",
    "ROUND",
    "FLOOR",
    "CEIL",
    "CEILING",
    "ABS",
    "POWER",
    "SQRT",
    "EXP",
    "LN",
    "LOG",
    "DATE",
    "TIME",
    "TIMESTAMP",
    "INTERVAL",
    "YEAR",
    "MONTH",
    "DAY",
    "HOUR",
    "MINUTE",
    "SECOND",
    // Data types
    "INT",
    "INTEGER",
    "BIGINT",
    "SMALLINT",
    "TINYINT",
    "FLOAT",
    "DOUBLE",
    "REAL",
    "DECIMAL",
    "NUMERIC",
    "VARCHAR",
    "CHAR",
    "TEXT",
    "STRING",
    "BOOLEAN",
    "BOOL",
    "DATE",
    "TIMESTAMP",
    "BINARY",
    "VARBINARY",
    // DML (for future)
    "INSERT",
    "INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "DELETE",
    "TRUNCATE",
    // DDL (for future)
    "CREATE",
    "TABLE",
    "DROP",
    "ALTER",
    "ADD",
    "COLUMN",
    "PRIMARY",
    "KEY",
    "FOREIGN",
    "REFERENCES",
    "INDEX",
    "VIEW",
    "SCHEMA",
    "DATABASE",
    // Other
    "WITH",
    "RECURSIVE",
    "OVER",
    "PARTITION",
    "ROWS",
    "RANGE",
    "UNBOUNDED",
    "PRECEDING",
    "FOLLOWING",
    "CURRENT",
    "ROW",
    "NULLS",
    "FIRST",
    "LAST",
];

/// Dot commands for the REPL
pub const DOT_COMMANDS: &[&str] = &[
    ".help", ".h", ".quit", ".exit", ".q", ".tables", ".schema", ".load", ".tpch", ".mode",
    ".format",
];

/// REPL helper that provides completion, highlighting, and hints
#[derive(Clone)]
pub struct ReplHelper {
    /// Known table names (updated dynamically)
    tables: Arc<RwLock<HashSet<String>>>,
    /// Known column names per table (updated dynamically)
    columns: Arc<RwLock<std::collections::HashMap<String, Vec<String>>>>,
    /// Whether to enable syntax highlighting
    highlighting_enabled: bool,
}

impl ReplHelper {
    /// Create a new REPL helper
    pub fn new() -> Self {
        Self {
            tables: Arc::new(RwLock::new(HashSet::new())),
            columns: Arc::new(RwLock::new(std::collections::HashMap::new())),
            highlighting_enabled: true,
        }
    }

    /// Register a table for completion
    pub fn register_table(&self, name: &str, columns: Vec<String>) {
        if let Ok(mut tables) = self.tables.write() {
            tables.insert(name.to_lowercase());
        }
        if let Ok(mut cols) = self.columns.write() {
            cols.insert(name.to_lowercase(), columns);
        }
    }

    /// Remove a table from completion
    pub fn unregister_table(&self, name: &str) {
        if let Ok(mut tables) = self.tables.write() {
            tables.remove(&name.to_lowercase());
        }
        if let Ok(mut cols) = self.columns.write() {
            cols.remove(&name.to_lowercase());
        }
    }

    /// Clear all registered tables
    pub fn clear_tables(&self) {
        if let Ok(mut tables) = self.tables.write() {
            tables.clear();
        }
        if let Ok(mut cols) = self.columns.write() {
            cols.clear();
        }
    }

    /// Get all known completions for the current context
    fn get_completions(&self, word: &str, line: &str) -> Vec<Pair> {
        let word_lower = word.to_lowercase();
        let line_lower = line.to_lowercase();
        let mut completions = Vec::new();

        // Dot commands only at start of line
        if line.trim_start().starts_with('.') {
            for cmd in DOT_COMMANDS {
                if cmd.starts_with(&word_lower) {
                    completions.push(Pair {
                        display: cmd.to_string(),
                        replacement: cmd.to_string(),
                    });
                }
            }
            return completions;
        }

        // SQL keywords (preserve case based on what user started typing)
        let use_uppercase = word
            .chars()
            .next()
            .map(|c| c.is_uppercase())
            .unwrap_or(true);
        for &kw in SQL_KEYWORDS {
            if kw.to_lowercase().starts_with(&word_lower) {
                let replacement = if use_uppercase {
                    kw.to_string()
                } else {
                    kw.to_lowercase()
                };
                completions.push(Pair {
                    display: kw.to_string(),
                    replacement,
                });
            }
        }

        // Table names (after FROM, JOIN, INTO, UPDATE, etc.)
        let after_table_keyword = ["from", "join", "into", "update", "table"]
            .iter()
            .any(|kw| {
                line_lower
                    .rfind(kw)
                    .map(|pos| {
                        // Check if cursor is after this keyword
                        let after_kw = &line_lower[pos + kw.len()..];
                        // Count words after keyword - if 0 or 1 partial word, suggest tables
                        after_kw.split_whitespace().count() <= 1
                    })
                    .unwrap_or(false)
            });

        if after_table_keyword || word_lower.is_empty() {
            if let Ok(tables) = self.tables.read() {
                for table in tables.iter() {
                    if table.starts_with(&word_lower) {
                        completions.push(Pair {
                            display: table.clone(),
                            replacement: table.clone(),
                        });
                    }
                }
            }
        }

        // Column names (context-aware based on tables in query)
        let tables_in_query = self.extract_tables_from_query(&line_lower);
        if let Ok(cols) = self.columns.read() {
            for table in &tables_in_query {
                if let Some(columns) = cols.get(table) {
                    for col in columns {
                        let col_lower = col.to_lowercase();
                        if col_lower.starts_with(&word_lower) {
                            // Avoid duplicates
                            if !completions
                                .iter()
                                .any(|p| p.replacement.to_lowercase() == col_lower)
                            {
                                completions.push(Pair {
                                    display: col.clone(),
                                    replacement: col.clone(),
                                });
                            }
                        }
                    }
                }
            }
        }

        completions
    }

    /// Extract table names mentioned in a query
    fn extract_tables_from_query(&self, query: &str) -> Vec<String> {
        let mut tables = Vec::new();
        let words: Vec<&str> = query.split_whitespace().collect();

        for (i, word) in words.iter().enumerate() {
            if *word == "from" || *word == "join" {
                if let Some(table) = words.get(i + 1) {
                    // Remove any alias or comma
                    let table = table.trim_end_matches(',');
                    if let Ok(known_tables) = self.tables.read() {
                        if known_tables.contains(table) {
                            tables.push(table.to_string());
                        }
                    }
                }
            }
        }
        tables
    }

    /// Check if a word is a SQL keyword
    fn is_keyword(word: &str) -> bool {
        SQL_KEYWORDS.iter().any(|kw| kw.eq_ignore_ascii_case(word))
    }

    /// Check if a word is a function name
    fn is_function(word: &str) -> bool {
        const FUNCTIONS: &[&str] = &[
            "COUNT",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "COALESCE",
            "NULLIF",
            "CAST",
            "EXTRACT",
            "SUBSTRING",
            "SUBSTR",
            "TRIM",
            "UPPER",
            "LOWER",
            "LENGTH",
            "CONCAT",
            "REPLACE",
            "POSITION",
            "STRPOS",
            "ROUND",
            "FLOOR",
            "CEIL",
            "CEILING",
            "ABS",
            "POWER",
            "SQRT",
            "EXP",
            "LN",
            "LOG",
        ];
        FUNCTIONS.iter().any(|f| f.eq_ignore_ascii_case(word))
    }
}

impl Default for ReplHelper {
    fn default() -> Self {
        Self::new()
    }
}

impl Completer for ReplHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        // Find the start of the current word
        let line_to_cursor = &line[..pos];
        let word_start = line_to_cursor
            .rfind(|c: char| c.is_whitespace() || c == ',' || c == '(' || c == ')')
            .map(|i| i + 1)
            .unwrap_or(0);

        let word = &line[word_start..pos];
        let completions = self.get_completions(word, line_to_cursor);

        Ok((word_start, completions))
    }
}

impl Highlighter for ReplHelper {
    fn highlight<'l>(&self, line: &'l str, _pos: usize) -> Cow<'l, str> {
        if !self.highlighting_enabled {
            return Cow::Borrowed(line);
        }

        // Don't highlight empty lines
        if line.trim().is_empty() {
            return Cow::Borrowed(line);
        }

        // Dot commands - highlight in cyan
        if line.trim_start().starts_with('.') {
            return Cow::Owned(format!("\x1b[36m{}\x1b[0m", line));
        }

        let mut result = String::with_capacity(line.len() * 2);
        let mut chars = line.char_indices().peekable();
        let mut in_string = false;
        let mut string_char = '"';

        while let Some((i, c)) = chars.next() {
            if in_string {
                // Inside a string literal
                result.push(c);
                if c == string_char {
                    result.push_str("\x1b[0m"); // Reset after string
                    in_string = false;
                }
            } else if c == '\'' || c == '"' {
                // Start of string literal - green
                in_string = true;
                string_char = c;
                result.push_str("\x1b[32m"); // Green
                result.push(c);
            } else if c.is_ascii_digit()
                || (c == '.'
                    && chars
                        .peek()
                        .map(|(_, nc)| nc.is_ascii_digit())
                        .unwrap_or(false))
            {
                // Number - magenta
                result.push_str("\x1b[35m"); // Magenta
                result.push(c);
                // Continue with rest of number
                while let Some(&(_, nc)) = chars.peek() {
                    if nc.is_ascii_digit() || nc == '.' {
                        result.push(nc);
                        chars.next();
                    } else {
                        break;
                    }
                }
                result.push_str("\x1b[0m"); // Reset
            } else if c.is_alphabetic() || c == '_' {
                // Word - check if it's a keyword
                let mut word = String::new();
                word.push(c);
                while let Some(&(_, nc)) = chars.peek() {
                    if nc.is_alphanumeric() || nc == '_' {
                        word.push(nc);
                        chars.next();
                    } else {
                        break;
                    }
                }

                if Self::is_keyword(&word) {
                    // SQL keyword - bold blue
                    result.push_str("\x1b[1;34m"); // Bold blue
                    result.push_str(&word);
                    result.push_str("\x1b[0m");
                } else if Self::is_function(&word) {
                    // Function - yellow
                    result.push_str("\x1b[33m"); // Yellow
                    result.push_str(&word);
                    result.push_str("\x1b[0m");
                } else {
                    // Regular identifier
                    result.push_str(&word);
                }
            } else {
                result.push(c);
            }
        }

        // Close any unclosed string
        if in_string {
            result.push_str("\x1b[0m");
        }

        Cow::Owned(result)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(
        &'s self,
        prompt: &'p str,
        _default: bool,
    ) -> Cow<'b, str> {
        // Cyan prompt
        Cow::Owned(format!("\x1b[1;36m{}\x1b[0m", prompt))
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        // Dim gray for hints
        Cow::Owned(format!("\x1b[2;37m{}\x1b[0m", hint))
    }

    fn highlight_char(&self, _line: &str, _pos: usize, _forced: bool) -> bool {
        // Return true to trigger re-highlighting
        true
    }
}

impl Hinter for ReplHelper {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &Context<'_>) -> Option<String> {
        // Hints disabled - using tab completion instead
        None
    }
}

impl Validator for ReplHelper {}

impl Helper for ReplHelper {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keyword_detection() {
        assert!(ReplHelper::is_keyword("SELECT"));
        assert!(ReplHelper::is_keyword("select"));
        assert!(ReplHelper::is_keyword("From"));
        assert!(!ReplHelper::is_keyword("customers"));
        assert!(!ReplHelper::is_keyword("id"));
    }

    #[test]
    fn test_function_detection() {
        assert!(ReplHelper::is_function("COUNT"));
        assert!(ReplHelper::is_function("sum"));
        assert!(ReplHelper::is_function("Avg"));
        assert!(!ReplHelper::is_function("SELECT"));
        assert!(!ReplHelper::is_function("mytable"));
    }

    #[test]
    fn test_table_registration() {
        let helper = ReplHelper::new();
        helper.register_table("customers", vec!["id".to_string(), "name".to_string()]);
        helper.register_table("orders", vec!["id".to_string(), "customer_id".to_string()]);

        let tables = helper.tables.read().unwrap();
        assert!(tables.contains("customers"));
        assert!(tables.contains("orders"));
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn test_table_unregistration() {
        let helper = ReplHelper::new();
        helper.register_table("customers", vec!["id".to_string()]);
        helper.register_table("orders", vec!["id".to_string()]);

        helper.unregister_table("customers");

        let tables = helper.tables.read().unwrap();
        assert!(!tables.contains("customers"));
        assert!(tables.contains("orders"));
    }

    #[test]
    fn test_keyword_completions() {
        let helper = ReplHelper::new();

        // Test uppercase completion
        let completions = helper.get_completions("SEL", "SEL");
        assert!(completions.iter().any(|p| p.replacement == "SELECT"));

        // Test lowercase completion
        let completions = helper.get_completions("sel", "sel");
        assert!(completions.iter().any(|p| p.replacement == "select"));

        // Test partial match
        let completions = helper.get_completions("WH", "SELECT * FROM t WH");
        assert!(completions.iter().any(|p| p.display == "WHERE"));
    }

    #[test]
    fn test_table_completions() {
        let helper = ReplHelper::new();
        helper.register_table("customers", vec![]);
        helper.register_table("orders", vec![]);

        // After FROM, suggest tables
        let completions = helper.get_completions("c", "SELECT * FROM c");
        assert!(completions.iter().any(|p| p.replacement == "customers"));

        // After JOIN, suggest tables
        let completions = helper.get_completions("o", "SELECT * FROM customers JOIN o");
        assert!(completions.iter().any(|p| p.replacement == "orders"));
    }

    #[test]
    fn test_column_completions() {
        let helper = ReplHelper::new();
        helper.register_table(
            "customers",
            vec!["id".to_string(), "name".to_string(), "email".to_string()],
        );

        // After mentioning table, suggest its columns
        let completions = helper.get_completions("n", "SELECT n FROM customers");
        assert!(completions.iter().any(|p| p.replacement == "name"));
    }

    #[test]
    fn test_dot_command_completions() {
        let helper = ReplHelper::new();

        let completions = helper.get_completions(".t", ".t");
        assert!(completions.iter().any(|p| p.replacement == ".tables"));
        assert!(completions.iter().any(|p| p.replacement == ".tpch"));

        let completions = helper.get_completions(".h", ".h");
        assert!(completions.iter().any(|p| p.replacement == ".help"));
    }

    #[test]
    fn test_highlighting_keywords() {
        let helper = ReplHelper::new();
        let highlighted = helper.highlight("SELECT * FROM users", 0);

        // Should contain ANSI escape codes for keywords
        assert!(highlighted.contains("\x1b[1;34m")); // Bold blue for keywords
        assert!(highlighted.contains("SELECT"));
        assert!(highlighted.contains("FROM"));
    }

    #[test]
    fn test_highlighting_strings() {
        let helper = ReplHelper::new();
        let highlighted = helper.highlight("SELECT * FROM users WHERE name = 'John'", 0);

        // Should contain green for string
        assert!(highlighted.contains("\x1b[32m")); // Green for strings
        assert!(highlighted.contains("John"));
    }

    #[test]
    fn test_highlighting_numbers() {
        let helper = ReplHelper::new();
        let highlighted = helper.highlight("SELECT * FROM orders WHERE amount > 100.50", 0);

        // Should contain magenta for numbers
        assert!(highlighted.contains("\x1b[35m")); // Magenta for numbers
    }

    #[test]
    fn test_highlighting_dot_commands() {
        let helper = ReplHelper::new();
        let highlighted = helper.highlight(".tables", 0);

        // Should be cyan
        assert!(highlighted.contains("\x1b[36m"));
    }

    #[test]
    fn test_extract_tables_from_query() {
        let helper = ReplHelper::new();
        helper.register_table("customers", vec![]);
        helper.register_table("orders", vec![]);

        let tables =
            helper.extract_tables_from_query("select * from customers join orders on c.id = o.id");
        assert!(tables.contains(&"customers".to_string()));
        assert!(tables.contains(&"orders".to_string()));
    }

    #[test]
    fn test_clear_tables() {
        let helper = ReplHelper::new();
        helper.register_table("customers", vec!["id".to_string()]);
        helper.register_table("orders", vec!["id".to_string()]);

        helper.clear_tables();

        let tables = helper.tables.read().unwrap();
        assert!(tables.is_empty());

        let cols = helper.columns.read().unwrap();
        assert!(cols.is_empty());
    }
}
