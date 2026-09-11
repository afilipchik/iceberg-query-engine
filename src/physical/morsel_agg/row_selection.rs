//! Borrowed, validated row positions. The caller retains and admits the indices.
use crate::{QueryError, Result};

#[derive(Clone, Copy)]
pub(super) struct RowSelection<'a> {
    rows: &'a [usize],
    batch_rows: usize,
}

impl<'a> RowSelection<'a> {
    pub(super) fn new(rows: &'a [usize], batch_rows: usize) -> Result<Self> {
        if rows.iter().any(|&row| row >= batch_rows) {
            return Err(QueryError::Execution(
                "aggregate selection row out of bounds".into(),
            ));
        }
        Ok(Self { rows, batch_rows })
    }
    pub(super) fn len(self) -> usize {
        self.rows.len()
    }
    pub(super) fn batch_rows(self) -> usize {
        self.batch_rows
    }
    pub(super) fn rows(self) -> &'a [usize] {
        self.rows
    }
}
