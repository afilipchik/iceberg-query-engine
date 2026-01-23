//! SQL Parser module
//!
//! Wraps sqlparser-rs to parse SQL statements into AST

mod ast;

pub use ast::*;

use crate::error::{QueryError, Result};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

/// Parse a SQL query string into a Statement AST
pub fn parse_sql(sql: &str) -> Result<sqlparser::ast::Statement> {
    let dialect = GenericDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)?;

    if statements.is_empty() {
        return Err(QueryError::Parse("Empty SQL statement".to_string()));
    }

    if statements.len() > 1 {
        return Err(QueryError::Parse(
            "Multiple statements not supported".to_string(),
        ));
    }

    Ok(statements.remove(0))
}

/// Parse multiple SQL statements
pub fn parse_sql_statements(sql: &str) -> Result<Vec<sqlparser::ast::Statement>> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    Ok(statements)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_select() {
        let sql = "SELECT a, b FROM table1";
        let stmt = parse_sql(sql).unwrap();
        assert!(matches!(stmt, sqlparser::ast::Statement::Query(_)));
    }

    #[test]
    fn test_parse_select_with_where() {
        let sql = "SELECT a FROM table1 WHERE b > 10";
        let stmt = parse_sql(sql).unwrap();
        assert!(matches!(stmt, sqlparser::ast::Statement::Query(_)));
    }

    #[test]
    fn test_parse_join() {
        let sql = "SELECT a.x, b.y FROM a JOIN b ON a.id = b.id";
        let stmt = parse_sql(sql).unwrap();
        assert!(matches!(stmt, sqlparser::ast::Statement::Query(_)));
    }

    #[test]
    fn test_parse_aggregate() {
        let sql = "SELECT COUNT(*), SUM(amount) FROM orders GROUP BY status";
        let stmt = parse_sql(sql).unwrap();
        assert!(matches!(stmt, sqlparser::ast::Statement::Query(_)));
    }

    #[test]
    fn test_parse_error() {
        let sql = "SELEC a FROM b"; // typo
        assert!(parse_sql(sql).is_err());
    }
}
