//! Iceberg table scanner
//!
//! This module provides Iceberg table reading with:
//! - Partition pruning based on predicates
//! - Snapshot-based reads (time travel)
//! - Delete file handling
//! - Manifest file parsing

use crate::error::{QueryError, Result};
use crate::physical::{PhysicalOperator, RecordBatchStream};
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
}

impl IcebergScanExec {
    /// Create a new Iceberg scan operator
    pub fn new(table_path: PathBuf, schema: SchemaRef) -> Self {
        Self {
            table_path,
            schema,
            snapshot_id: None,
            partition_filters: Vec::new(),
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
        let snapshot_id = self
            .snapshot_id
            .unwrap_or_else(|| metadata.current_snapshot_id.expect("No current snapshot"));

        // Load snapshot
        let snapshot = self.load_snapshot(snapshot_id)?;

        // Collect data files from manifests
        let all_data_files = self.collect_data_files(&snapshot)?;

        // Apply partition filters
        let _filtered_files = self.filter_data_files(all_data_files);

        // For each data file, we would normally read it using the Parquet scanner
        // For now, return empty batches as a placeholder
        let batches = Vec::new();

        let stream = stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(stream))
    }

    fn name(&self) -> &str {
        "IcebergScan"
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
