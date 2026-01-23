//! Parquet file table provider

use crate::error::{QueryError, Result};
use crate::physical::operators::TableProvider;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fmt;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Table provider that reads from Parquet files
pub struct ParquetTable {
    /// Arrow schema for the table
    schema: SchemaRef,
    /// List of Parquet files to read
    files: Vec<PathBuf>,
}

impl fmt::Debug for ParquetTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ParquetTable")
            .field("schema", &self.schema)
            .field("files", &self.files)
            .finish()
    }
}

impl ParquetTable {
    /// Create a ParquetTable from a single file or directory
    ///
    /// If path is a file, reads that single Parquet file.
    /// If path is a directory, reads all .parquet files in it.
    pub fn try_new(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();

        let files = if path.is_dir() {
            Self::find_parquet_files(path)?
        } else if path.is_file() {
            vec![path.to_path_buf()]
        } else {
            return Err(QueryError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Path does not exist: {}", path.display()),
            )));
        };

        if files.is_empty() {
            return Err(QueryError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("No Parquet files found in: {}", path.display()),
            )));
        }

        // Infer schema from the first file
        let schema = Self::read_schema(&files[0])?;

        Ok(Self { schema, files })
    }

    /// Find all Parquet files in a directory (non-recursive)
    fn find_parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
        let mut files = Vec::new();

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_file() {
                if let Some(ext) = path.extension() {
                    if ext == "parquet" {
                        files.push(path);
                    }
                }
            }
        }

        // Sort for deterministic order
        files.sort();
        Ok(files)
    }

    /// Read schema from a Parquet file
    fn read_schema(path: &Path) -> Result<SchemaRef> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;
        Ok(builder.schema().clone())
    }

    /// Read all batches from a single Parquet file
    fn read_file(path: &Path, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

        // Apply projection if specified
        let reader = if let Some(indices) = projection {
            // Create projection mask from parquet schema
            let mask = parquet::arrow::ProjectionMask::roots(
                builder.parquet_schema(),
                indices.iter().copied(),
            );
            builder.with_projection(mask).build()?
        } else {
            builder.build()?
        };

        let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(batches)
    }

    /// Get the list of files this table reads from
    pub fn files(&self) -> &[PathBuf] {
        &self.files
    }

    /// Get total number of files
    pub fn file_count(&self) -> usize {
        self.files.len()
    }
}

impl TableProvider for ParquetTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();

        for file_path in &self.files {
            let batches = Self::read_file(file_path, projection)?;
            all_batches.extend(batches);
        }

        // If projection was applied, we need to update the schema in the batches
        // The Parquet reader already does this, so batches have correct schema
        Ok(all_batches)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parquet_table_not_found() {
        let result = ParquetTable::try_new("/nonexistent/path.parquet");
        assert!(result.is_err());
    }
}
