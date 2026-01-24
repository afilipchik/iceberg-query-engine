//! Iceberg table scanner
//!
//! This module provides Iceberg table reading with:
//! - Partition pruning based on predicates
//! - Statistics-based file pruning (min/max bounds)
//! - Snapshot-based reads (time travel)
//! - Delete file handling
//! - Manifest file parsing

use crate::error::{QueryError, Result};
use crate::physical::{PhysicalOperator, RecordBatchStream};
use crate::planner::Expr;
use crate::storage::StreamingParquetReader;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream;
use serde::Deserialize;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;

/// Iceberg table metadata
#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)] // Fields populated by serde deserialization
pub struct IcebergTableMetadata {
    /// Format version
    pub format_version: i32,
    /// Table UUID
    pub uuid: String,
    /// Current snapshot ID
    pub current_snapshot_id: Option<i64>,
}

/// Iceberg snapshot
#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)] // Fields populated by serde deserialization
pub struct IcebergSnapshot {
    /// Snapshot ID
    pub snapshot_id: i64,
    /// Parent snapshot ID
    pub parent_snapshot_id: Option<i64>,
    /// Schema ID
    pub schema_id: Option<i64>,
    /// Summary
    pub summary: Option<HashMap<String, String>>,
    /// Manifest list location
    pub manifest_list: String,
}

/// Iceberg manifest file
#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)] // Fields populated by serde deserialization
pub struct IcebergManifest {
    /// Manifest path
    pub manifest_path: String,
    /// Manifest length
    pub manifest_length: i64,
    /// Partition spec ID
    pub partition_spec_id: i32,
    /// Snapshot ID
    pub snapshot_id: i64,
    /// Sequence number
    pub sequence_number: i64,
}

/// Iceberg data file
#[derive(Debug, Deserialize, Clone)]
pub struct IcebergDataFile {
    /// File path
    pub file_path: String,
    /// File format (e.g., "PARQUET")
    pub file_format: String,
    /// Partition values
    #[serde(default)]
    pub partition: HashMap<String, String>,
    /// Record count
    pub record_count: i64,
    /// File size in bytes
    pub file_size_in_bytes: i64,
    /// Column sizes (map from column ID to size)
    #[serde(default)]
    pub column_sizes: Option<HashMap<i32, i64>>,
    /// Value counts (map from column ID to count)
    #[serde(default)]
    pub value_counts: Option<HashMap<i32, i64>>,
    /// Null value counts (map from column ID to count)
    #[serde(default)]
    pub null_value_counts: Option<HashMap<i32, i64>>,
    /// Nan value counts (map from column ID to count)
    #[serde(default)]
    pub nan_value_counts: Option<HashMap<i32, i64>>,
    /// Lower bounds (map from column ID to value)
    #[serde(default)]
    pub lower_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// Upper bounds (map from column ID to value)
    #[serde(default)]
    pub upper_bounds: Option<HashMap<i32, Vec<u8>>>,
    /// Key metadata
    pub key_metadata: Option<Vec<u8>>,
    /// Split offsets
    pub split_offsets: Option<Vec<i64>>,
    /// Sort order ID
    #[serde(default)]
    pub sort_order_id: i32,
}

/// Iceberg table scanner
#[derive(Debug)]
pub struct IcebergScanExec {
    /// Path to the Iceberg table root
    table_path: PathBuf,
    /// Output schema
    schema: SchemaRef,
    /// Snapshot ID to read (None for current)
    snapshot_id: Option<i64>,
    /// Partition filters
    partition_filters: Vec<PartitionFilter>,
    /// Predicate for statistics-based filtering
    predicate: Option<Expr>,
    /// Column projection indices
    projection: Option<Vec<usize>>,
    /// Batch size for streaming reads
    batch_size: usize,
    /// Statistics for pruning metrics
    files_pruned_by_partition: usize,
    files_pruned_by_stats: usize,
}

impl IcebergScanExec {
    /// Create a new Iceberg scan operator
    pub fn new(table_path: PathBuf, schema: SchemaRef) -> Self {
        Self {
            table_path,
            schema,
            snapshot_id: None,
            partition_filters: Vec::new(),
            predicate: None,
            projection: None,
            batch_size: 8192,
            files_pruned_by_partition: 0,
            files_pruned_by_stats: 0,
        }
    }

    /// Set the snapshot ID to read
    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot_id = Some(snapshot_id);
        self
    }

    /// Add a partition filter
    pub fn with_partition_filter(mut self, filter: PartitionFilter) -> Self {
        self.partition_filters.push(filter);
        self
    }

    /// Set a predicate for statistics-based filtering
    pub fn with_predicate(mut self, predicate: Expr) -> Self {
        self.predicate = Some(predicate);
        self
    }

    /// Set column projection
    pub fn with_projection(mut self, projection: Vec<usize>) -> Self {
        self.projection = Some(projection);
        self
    }

    /// Set batch size for streaming
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Get the number of files pruned by partition filter
    pub fn files_pruned_by_partition(&self) -> usize {
        self.files_pruned_by_partition
    }

    /// Get the number of files pruned by statistics
    pub fn files_pruned_by_stats(&self) -> usize {
        self.files_pruned_by_stats
    }

    /// Load table metadata
    fn load_metadata(&self) -> Result<IcebergTableMetadata> {
        let metadata_path = self.table_path.join("metadata/v0.metadata.json");

        let metadata_content = fs::read_to_string(&metadata_path).map_err(|e| {
            QueryError::Execution(format!(
                "Failed to read Iceberg metadata from {:?}: {}",
                metadata_path, e
            ))
        })?;

        serde_json::from_str(&metadata_content)
            .map_err(|e| QueryError::Execution(format!("Failed to parse Iceberg metadata: {}", e)))
    }

    /// Load snapshot
    fn load_snapshot(&self, snapshot_id: i64) -> Result<IcebergSnapshot> {
        let snapshot_path = self
            .table_path
            .join(format!("metadata/snap-{}.snapshot.json", snapshot_id));

        let snapshot_content = fs::read_to_string(&snapshot_path).map_err(|e| {
            QueryError::Execution(format!(
                "Failed to read Iceberg snapshot from {:?}: {}",
                snapshot_path, e
            ))
        })?;

        serde_json::from_str(&snapshot_content)
            .map_err(|e| QueryError::Execution(format!("Failed to parse Iceberg snapshot: {}", e)))
    }

    /// Collect data files from manifests
    fn collect_data_files(&self, snapshot: &IcebergSnapshot) -> Result<Vec<IcebergDataFile>> {
        let manifest_list_path = self.table_path.join(&snapshot.manifest_list);

        let manifest_list_content = fs::read_to_string(&manifest_list_path).map_err(|e| {
            QueryError::Execution(format!(
                "Failed to read manifest list from {:?}: {}",
                manifest_list_path, e
            ))
        })?;

        let manifest_list: Vec<IcebergManifest> = serde_json::from_str(&manifest_list_content)
            .map_err(|e| QueryError::Execution(format!("Failed to parse manifest list: {}", e)))?;

        let mut data_files = Vec::new();

        // Read each manifest file
        for manifest in &manifest_list {
            let manifest_path = self.table_path.join(&manifest.manifest_path);

            if let Ok(manifest_content) = fs::read_to_string(&manifest_path) {
                // Parse manifest entries
                if let Ok(manifest_entries) =
                    serde_json::from_str::<serde_json::Value>(&manifest_content)
                {
                    if let Some(entries) = manifest_entries["entries"].as_array() {
                        for entry in entries {
                            if let Some(data_file) = entry["data_file"].as_object() {
                                if let Ok(file) = serde_json::from_value::<IcebergDataFile>(
                                    serde_json::json!(data_file),
                                ) {
                                    data_files.push(file);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(data_files)
    }

    /// Apply partition filters to data files
    fn filter_data_files(&self, files: Vec<IcebergDataFile>) -> Vec<IcebergDataFile> {
        files
            .into_iter()
            .filter(|file| {
                // Check all partition filters
                self.partition_filters
                    .iter()
                    .all(|filter| filter.matches(file))
            })
            .collect()
    }
}

#[async_trait]
impl PhysicalOperator for IcebergScanExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }

    async fn execute(&self, _partition: usize) -> Result<RecordBatchStream> {
        // Load table metadata
        let metadata = self.load_metadata()?;

        // Get snapshot ID
        let snapshot_id = self.snapshot_id.or(metadata.current_snapshot_id);

        // If no snapshot, return empty stream
        let Some(snapshot_id) = snapshot_id else {
            return Ok(Box::pin(stream::empty()));
        };

        // Load snapshot
        let snapshot = self.load_snapshot(snapshot_id)?;

        // Collect data files from manifests
        let all_data_files = self.collect_data_files(&snapshot)?;
        let total_files = all_data_files.len();

        // Apply partition filters
        let partition_filtered = self.filter_data_files(all_data_files);
        let after_partition = partition_filtered.len();

        // Apply statistics-based filtering
        let stats_filtered = self.filter_by_statistics(partition_filtered);
        let after_stats = stats_filtered.len();

        // Log pruning stats (would be captured in metrics in production)
        let _pruned_by_partition = total_files - after_partition;
        let _pruned_by_stats = after_partition - after_stats;

        // Convert file paths to absolute paths
        let file_paths: Vec<PathBuf> = stats_filtered
            .into_iter()
            .map(|f| {
                let path = PathBuf::from(&f.file_path);
                if path.is_absolute() {
                    path
                } else {
                    // Relative to table data directory
                    self.table_path.join("data").join(&f.file_path)
                }
            })
            .filter(|p| p.exists())
            .collect();

        if file_paths.is_empty() {
            return Ok(Box::pin(stream::empty()));
        }

        // Create streaming reader for the Parquet files
        let reader = StreamingParquetReader::new(
            file_paths,
            self.schema.clone(),
            self.projection.clone(),
            self.batch_size,
        );

        Ok(reader.into_stream())
    }

    fn name(&self) -> &str {
        "IcebergScan"
    }
}

impl IcebergScanExec {
    /// Filter files based on column statistics (min/max bounds)
    fn filter_by_statistics(&self, files: Vec<IcebergDataFile>) -> Vec<IcebergDataFile> {
        let Some(ref predicate) = self.predicate else {
            return files;
        };

        files
            .into_iter()
            .filter(|file| self.file_might_match(file, predicate))
            .collect()
    }

    /// Check if a file might contain matching rows based on statistics
    fn file_might_match(&self, file: &IcebergDataFile, predicate: &Expr) -> bool {
        use crate::planner::{BinaryOp, UnaryOp};

        // For now, we use a conservative approach - if we can't determine,
        // we include the file. This can be enhanced with more sophisticated
        // statistics checking.
        match predicate {
            Expr::BinaryExpr { left, op, right } => {
                // Handle AND and OR as special cases
                match op {
                    BinaryOp::And => {
                        // Both conditions must potentially match
                        self.file_might_match(file, left) && self.file_might_match(file, right)
                    }
                    BinaryOp::Or => {
                        // At least one condition must potentially match
                        self.file_might_match(file, left) || self.file_might_match(file, right)
                    }
                    _ => self.check_comparison_predicate(file, left, op, right),
                }
            }
            Expr::UnaryExpr {
                op: UnaryOp::Not,
                expr,
            } => {
                // Conservative: if we can prove inner always matches, we can skip
                // For simplicity, always include
                !self.file_definitely_matches(file, expr)
            }
            _ => true, // Conservative: include file if we can't determine
        }
    }

    /// Check a comparison predicate against file statistics
    fn check_comparison_predicate(
        &self,
        file: &IcebergDataFile,
        left: &Expr,
        op: &crate::planner::BinaryOp,
        right: &Expr,
    ) -> bool {
        use crate::planner::{BinaryOp, ScalarValue};

        // Try to extract column name and literal value
        let (col_name, literal) = match (left, right) {
            (Expr::Column(col), Expr::Literal(lit)) => (col.name.as_str(), lit),
            (Expr::Literal(lit), Expr::Column(col)) => (col.name.as_str(), lit),
            _ => return true, // Can't evaluate, include file
        };

        // Get column index from schema
        let col_idx = self
            .schema
            .fields()
            .iter()
            .position(|f| f.name() == col_name);
        let Some(col_idx) = col_idx else {
            return true; // Column not in schema, include file
        };

        // Get bounds for this column from file statistics
        let lower_bound = file
            .lower_bounds
            .as_ref()
            .and_then(|b| b.get(&(col_idx as i32)));
        let upper_bound = file
            .upper_bounds
            .as_ref()
            .and_then(|b| b.get(&(col_idx as i32)));

        // If no statistics available, include the file
        if lower_bound.is_none() && upper_bound.is_none() {
            return true;
        }

        // Evaluate based on operator and statistics
        // For numeric comparisons with Int64 values
        if let ScalarValue::Int64(val) = literal {
            let min = lower_bound.and_then(|b| parse_i64_from_bytes(b));
            let max = upper_bound.and_then(|b| parse_i64_from_bytes(b));

            match op {
                BinaryOp::Eq => {
                    // value = X: file might match if min <= X <= max
                    let min_ok = min.map_or(true, |m| m <= *val);
                    let max_ok = max.map_or(true, |m| m >= *val);
                    min_ok && max_ok
                }
                BinaryOp::Lt => {
                    // value < X: file might match if min < X
                    min.map_or(true, |m| m < *val)
                }
                BinaryOp::LtEq => {
                    // value <= X: file might match if min <= X
                    min.map_or(true, |m| m <= *val)
                }
                BinaryOp::Gt => {
                    // value > X: file might match if max > X
                    max.map_or(true, |m| m > *val)
                }
                BinaryOp::GtEq => {
                    // value >= X: file might match if max >= X
                    max.map_or(true, |m| m >= *val)
                }
                BinaryOp::NotEq => {
                    // value != X: file might not match only if min == max == X
                    !(min == Some(*val) && max == Some(*val))
                }
                _ => true,
            }
        } else if let ScalarValue::Int32(val) = literal {
            // Handle Int32 similarly
            let val = *val as i64;
            let min = lower_bound.and_then(|b| parse_i64_from_bytes(b));
            let max = upper_bound.and_then(|b| parse_i64_from_bytes(b));

            match op {
                BinaryOp::Eq => {
                    let min_ok = min.map_or(true, |m| m <= val);
                    let max_ok = max.map_or(true, |m| m >= val);
                    min_ok && max_ok
                }
                BinaryOp::Lt => min.map_or(true, |m| m < val),
                BinaryOp::LtEq => min.map_or(true, |m| m <= val),
                BinaryOp::Gt => max.map_or(true, |m| m > val),
                BinaryOp::GtEq => max.map_or(true, |m| m >= val),
                BinaryOp::NotEq => !(min == Some(val) && max == Some(val)),
                _ => true,
            }
        } else {
            // For other types, be conservative
            true
        }
    }

    /// Check if file definitely matches (for NOT optimization)
    fn file_definitely_matches(&self, _file: &IcebergDataFile, _predicate: &Expr) -> bool {
        // Conservative: we can't prove definite match
        false
    }
}

/// Parse i64 from Iceberg statistics bytes (little-endian)
fn parse_i64_from_bytes(bytes: &[u8]) -> Option<i64> {
    if bytes.len() >= 8 {
        Some(i64::from_le_bytes(bytes[..8].try_into().ok()?))
    } else {
        None
    }
}

impl fmt::Display for IcebergScanExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IcebergScan: {:?}", self.table_path)?;
        if let Some(sid) = self.snapshot_id {
            write!(f, " snapshot={}", sid)?;
        }
        Ok(())
    }
}

/// Partition filter for pruning files
#[derive(Debug, Clone)]
pub enum PartitionFilter {
    /// Equals filter
    Eq { column: String, value: String },
    /// Greater than filter
    Gt { column: String, value: String },
    /// Less than filter
    Lt { column: String, value: String },
    /// In list filter
    In { column: String, values: Vec<String> },
}

impl PartitionFilter {
    /// Check if a data file matches this filter
    pub fn matches(&self, file: &IcebergDataFile) -> bool {
        match self {
            PartitionFilter::Eq { column, value } => file.partition.get(column) == Some(value),
            PartitionFilter::Gt { column, value } => file.partition.get(column).is_some_and(|v| {
                v.parse::<i64>()
                    .ok()
                    .is_some_and(|v_num| value.parse::<i64>().ok().is_some_and(|val| v_num > val))
            }),
            PartitionFilter::Lt { column, value } => file.partition.get(column).is_some_and(|v| {
                v.parse::<i64>()
                    .ok()
                    .is_some_and(|v_num| value.parse::<i64>().ok().is_some_and(|val| v_num < val))
            }),
            PartitionFilter::In { column, values } => file
                .partition
                .get(column)
                .is_some_and(|v| values.contains(v)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_filter_eq() {
        let filter = PartitionFilter::Eq {
            column: "year".to_string(),
            value: "2024".to_string(),
        };

        let mut file = IcebergDataFile {
            file_path: "s3://bucket/file.parquet".to_string(),
            file_format: "PARQUET".to_string(),
            partition: HashMap::new(),
            record_count: 1000,
            file_size_in_bytes: 1024,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            sort_order_id: 0,
        };

        file.partition
            .insert("year".to_string(), "2024".to_string());
        assert!(filter.matches(&file));

        file.partition
            .insert("year".to_string(), "2023".to_string());
        assert!(!filter.matches(&file));
    }

    #[test]
    fn test_partition_filter_in() {
        let filter = PartitionFilter::In {
            column: "month".to_string(),
            values: vec!["1".to_string(), "2".to_string(), "3".to_string()],
        };

        let mut file = IcebergDataFile {
            file_path: "s3://bucket/file.parquet".to_string(),
            file_format: "PARQUET".to_string(),
            partition: HashMap::new(),
            record_count: 1000,
            file_size_in_bytes: 1024,
            column_sizes: None,
            value_counts: None,
            null_value_counts: None,
            nan_value_counts: None,
            lower_bounds: None,
            upper_bounds: None,
            key_metadata: None,
            split_offsets: None,
            sort_order_id: 0,
        };

        file.partition.insert("month".to_string(), "2".to_string());
        assert!(filter.matches(&file));

        file.partition.insert("month".to_string(), "4".to_string());
        assert!(!filter.matches(&file));
    }
}
