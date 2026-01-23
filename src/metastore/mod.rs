//! Branching metastore REST API client
//!
//! This module provides a client for connecting to the branching metastore REST API,
//! enabling the query engine to fetch table metadata from the centralized metastore.

use crate::error::{QueryError, Result};
use arrow::datatypes::{DataType, Field, Fields, Schema};
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Configuration for metastore client reliability
#[derive(Debug, Clone)]
pub struct MetastoreConfig {
    /// Maximum number of retry attempts
    pub max_retries: usize,
    /// Initial retry delay in milliseconds
    pub retry_delay_ms: u64,
    /// Maximum retry delay in milliseconds
    pub max_retry_delay_ms: u64,
    /// Request timeout in seconds
    pub timeout_secs: u64,
    /// Connection pool max idle connections
    pub pool_max_idle: usize,
    /// Connection pool idle timeout in seconds
    pub pool_idle_timeout_secs: u64,
}

impl Default for MetastoreConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            retry_delay_ms: 100,
            max_retry_delay_ms: 5000,
            timeout_secs: 30,
            pool_max_idle: 10,
            pool_idle_timeout_secs: 90,
        }
    }
}

/// Branching metastore REST client
pub struct BranchingMetastoreClient {
    /// HTTP client with connection pooling
    client: Client,
    /// Base URL of the metastore API
    base_url: String,
    /// Branch ID for scoping table operations
    branch_id: String,
    /// Configuration for retries and timeouts
    config: MetastoreConfig,
}

impl BranchingMetastoreClient {
    /// Create a new metastore client with default configuration
    pub fn new(base_url: impl Into<String>, branch_id: impl Into<String>) -> Self {
        Self::with_config(base_url, branch_id, MetastoreConfig::default())
    }

    /// Create a new metastore client with custom configuration
    pub fn with_config(
        base_url: impl Into<String>,
        branch_id: impl Into<String>,
        config: MetastoreConfig,
    ) -> Self {
        let client = Client::builder()
            .pool_max_idle_per_host(config.pool_max_idle)
            .pool_idle_timeout(Duration::from_secs(config.pool_idle_timeout_secs))
            .timeout(Duration::from_secs(config.timeout_secs))
            .build()
            .unwrap_or_else(|e| {
                panic!("Failed to create HTTP client: {}", e);
            });

        Self {
            client,
            base_url: base_url.into(),
            branch_id: branch_id.into(),
            config,
        }
    }

    /// Execute an HTTP request with retry logic
    async fn execute_with_retry<T, F, Fut>(
        &self,
        mut request_fn: F,
        context: &str,
    ) -> Result<T>
    where
        T: for<'de> serde::Deserialize<'de>,
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = std::result::Result<reqwest::Response, reqwest::Error>>,
    {
        let mut delay = Duration::from_millis(self.config.retry_delay_ms);
        let mut last_error = None;

        for attempt in 0..=self.config.max_retries {
            // Execute the request
            let response_result = request_fn().await;

            match response_result {
                Ok(response) => {
                    let status = response.status();

                    // Check if response is successful
                    if status.is_success() {
                        return response
                            .json()
                            .await
                            .map_err(|e| QueryError::Execution(format!(
                                "{}: Failed to parse response: {}",
                                context, e
                            )));
                    }

                    // Don't retry client errors (4xx) except 429 (rate limit)
                    if status.is_client_error() && status.as_u16() != 429 {
                        return Err(QueryError::Execution(format!(
                            "{}: HTTP {} (client error, not retrying)",
                            context, status
                        )));
                    }

                    // Server errors and rate limits are retryable
                    last_error = Some(QueryError::Execution(format!(
                        "{}: HTTP {}",
                        context, status
                    )));
                }
                Err(e) => {
                    // Check if error is retryable (network errors, timeouts, etc.)
                    let is_retryable = e.is_timeout()
                        || e.is_connect()
                        || e.to_string().contains("timed out")
                        || e.to_string().contains("connection")
                        || e.to_string().contains("dns");

                    if !is_retryable {
                        return Err(QueryError::Execution(format!(
                            "{}: {} (not retryable)",
                            context, e
                        )));
                    }

                    last_error = Some(QueryError::Execution(format!(
                        "{}: {}",
                        context, e
                    )));
                }
            }

            // Don't sleep after the last attempt
            if attempt < self.config.max_retries {
                tokio::time::sleep(delay).await;
                // Exponential backoff with simple jitter
                delay = std::cmp::min(
                    delay * 2,
                    Duration::from_millis(self.config.max_retry_delay_ms),
                );
                // Add simple jitter using system time
                let jitter = Duration::from_millis(
                    (std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .subsec_millis() % 100) as u64
                );
                delay = delay.saturating_add(jitter);
            }
        }

        // All retries exhausted
        Err(last_error.unwrap_or_else(|| {
            QueryError::Execution(format!("{}: Max retries exceeded", context))
        }))
    }

    /// Get all databases in the current branch
    pub async fn list_databases(&self) -> Result<Vec<String>> {
        let url = format!("{}/branch/{}/databases", self.base_url, self.branch_id);
        let client = self.client.clone();

        let response: DatabasesResponse = self.execute_with_retry(
            || client.get(&url).send(),
            "list_databases",
        ).await?;

        Ok(response.databases)
    }

    /// Get database metadata
    pub async fn get_database(&self, database_name: &str) -> Result<DatabaseMetadata> {
        let url = format!(
            "{}/branch/{}/database/{}",
            self.base_url, self.branch_id, database_name
        );
        let client = self.client.clone();

        self.execute_with_retry(
            || client.get(&url).send(),
            "get_database",
        ).await
    }

    /// List tables in a database
    pub async fn list_tables(&self, database_name: &str) -> Result<Vec<TableInfo>> {
        let url = format!(
            "{}/branch/{}/database/{}/tables",
            self.base_url, self.branch_id, database_name
        );
        let client = self.client.clone();

        let response: TablesResponse = self.execute_with_retry(
            || client.get(&url).send(),
            "list_tables",
        ).await?;

        Ok(response.tables)
    }

    /// Get table metadata
    pub async fn get_table(&self, database_name: &str, table_name: &str) -> Result<TableMetadata> {
        let url = format!(
            "{}/branch/{}/table/{}/{}",
            self.base_url, self.branch_id, database_name, table_name
        );

        // Custom retry logic for get_table to handle 404 specially
        let mut delay = Duration::from_millis(self.config.retry_delay_ms);

        for attempt in 0..=self.config.max_retries {
            let response_result: std::result::Result<reqwest::Response, reqwest::Error> =
                self.client.get(&url).send().await;

            match response_result {
                Ok(response) => {
                    let status = response.status();

                    if status.is_success() {
                        return response
                            .json()
                            .await
                            .map_err(|e| QueryError::Execution(format!("Failed to parse table response: {}", e)));
                    }

                    if status.as_u16() == 404 {
                        // 404 is not retryable for tables
                        return Err(QueryError::TableNotFound(format!(
                            "Table not found: {}.{}",
                            database_name, table_name
                        )));
                    }

                    // Retry server errors and rate limits
                    if !status.is_server_error() && status.as_u16() != 429 {
                        return Err(QueryError::Execution(format!(
                            "Failed to get table: HTTP {}",
                            status
                        )));
                    }

                    // Fall through to retry
                }
                Err(e) => {
                    // Check if error is retryable
                    let is_retryable = e.is_timeout()
                        || e.is_connect()
                        || e.to_string().contains("timed out")
                        || e.to_string().contains("connection")
                        || e.to_string().contains("dns");

                    if !is_retryable {
                        return Err(QueryError::Execution(format!("Failed to get table: {}", e)));
                    }
                }
            }

            if attempt < self.config.max_retries {
                tokio::time::sleep(delay).await;
                delay = std::cmp::min(
                    delay * 2,
                    Duration::from_millis(self.config.max_retry_delay_ms),
                );
            }
        }

        Err(QueryError::Execution(format!(
            "Failed to get table after retries: {}.{}",
            database_name, table_name
        )))
    }

    /// Convert metastore table metadata to Arrow schema
    pub fn table_to_arrow_schema(metadata: &TableMetadata) -> Result<Schema> {
        let mut fields = Vec::new();

        for col in &metadata.columns {
            let data_type = parse_data_type(&col.data_type)?;
            fields.push(Field::new(col.name.clone(), data_type, col.nullable));
        }

        Ok(Schema::new(fields))
    }
}

/// Parse metastore data type string to Arrow DataType
fn parse_data_type(data_type: &str) -> Result<DataType> {
    match data_type.to_uppercase().as_str() {
        "BOOLEAN" | "BOOL" => Ok(DataType::Boolean),
        "TINYINT" | "INT1" => Ok(DataType::Int8),
        "SMALLINT" | "INT2" => Ok(DataType::Int16),
        "INT" | "INTEGER" | "INT4" => Ok(DataType::Int32),
        "BIGINT" | "LONG" | "INT8" => Ok(DataType::Int64),
        "FLOAT" | "REAL" | "FLOAT4" => Ok(DataType::Float32),
        "DOUBLE" | "FLOAT8" => Ok(DataType::Float64),
        "VARCHAR" | "STRING" | "TEXT" => Ok(DataType::Utf8),
        "DATE" => Ok(DataType::Date32),
        "TIMESTAMP" => Ok(DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None)),
        "DECIMAL" | "NUMERIC" => Ok(DataType::Decimal128(38, 10)),
        "ARRAY" => Ok(DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))),
        "MAP" => Ok(DataType::Map(
            Arc::new(Field::new("entries".to_string(), DataType::Utf8, true)),
            false,
        )),
        "ROW" | "STRUCT" => Ok(DataType::Struct(Fields::empty())),
        _ => Ok(DataType::Utf8), // Default to string for unknown types
    }
}

/// Database list response
#[derive(Debug, Deserialize)]
struct DatabasesResponse {
    databases: Vec<String>,
}

/// Tables list response
#[derive(Debug, Deserialize)]
struct TablesResponse {
    tables: Vec<TableInfo>,
}

/// Database metadata
#[derive(Debug, Deserialize)]
pub struct DatabaseMetadata {
    pub database_name: String,
    pub comment: Option<String>,
}

/// Table information
#[derive(Debug, Deserialize, Clone)]
pub struct TableInfo {
    pub table_name: String,
    pub table_type: String,
    #[serde(default)]
    pub comment: Option<String>,
}

/// Table metadata
#[derive(Debug, Deserialize)]
pub struct TableMetadata {
    pub table_name: String,
    pub database_name: String,
    pub table_type: String,
    pub columns: Vec<ColumnMetadata>,
    pub table_location: Option<String>,
    pub table_format: Option<String>,
}

/// Column metadata
#[derive(Debug, Deserialize, Clone)]
pub struct ColumnMetadata {
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
    #[serde(default)]
    pub comment: Option<String>,
}

/// Table provider backed by branching metastore
pub struct MetastoreTableProvider {
    /// Metastore client
    client: BranchingMetastoreClient,
    /// Database name
    database_name: String,
    /// Table name
    table_name: String,
    /// Table metadata
    metadata: TableMetadata,
}

impl MetastoreTableProvider {
    /// Create a new metastore table provider
    pub async fn new(
        base_url: impl Into<String>,
        branch_id: impl Into<String>,
        database_name: impl Into<String>,
        table_name: impl Into<String>,
    ) -> Result<Self> {
        let client = BranchingMetastoreClient::new(base_url, branch_id);

        let database_name = database_name.into();
        let table_name = table_name.into();

        let metadata = client
            .get_table(&database_name, &table_name)
            .await?;

        Ok(Self {
            client,
            database_name: metadata.database_name.clone(),
            table_name: metadata.table_name.clone(),
            metadata,
        })
    }

    /// Get the table metadata
    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    /// Get the Arrow schema
    pub fn schema(&self) -> Result<Schema> {
        BranchingMetastoreClient::table_to_arrow_schema(&self.metadata)
    }
}

/// Metastore catalog for registering tables from the branching metastore
pub struct MetastoreCatalog {
    client: BranchingMetastoreClient,
    cached_schemas: HashMap<String, Schema>,
}

impl MetastoreCatalog {
    /// Create a new metastore catalog
    pub fn new(base_url: impl Into<String>, branch_id: impl Into<String>) -> Self {
        Self {
            client: BranchingMetastoreClient::new(base_url, branch_id),
            cached_schemas: HashMap::new(),
        }
    }

    /// Get the client
    pub fn client(&self) -> &BranchingMetastoreClient {
        &self.client
    }

    /// Get table schema (cached)
    pub fn get_schema(&mut self, database_name: &str, table_name: &str) -> Result<Schema> {
        let key = format!("{}.{}", database_name, table_name);

        if let Some(schema) = self.cached_schemas.get(&key) {
            return Ok(schema.clone());
        }

        // Fetch schema from metastore
        let metadata = tokio::task::block_in_place(|| {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                self.client.get_table(database_name, table_name).await
            })
        }).map_err(|e| QueryError::Execution(format!("Failed to get table schema: {}", e)))?;

        let schema = BranchingMetastoreClient::table_to_arrow_schema(&metadata)?;
        self.cached_schemas.insert(key, schema.clone());

        Ok(schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_data_type() {
        assert_eq!(parse_data_type("int").unwrap(), DataType::Int32);
        assert_eq!(parse_data_type("bigint").unwrap(), DataType::Int64);
        assert_eq!(parse_data_type("varchar").unwrap(), DataType::Utf8);
        assert_eq!(parse_data_type("boolean").unwrap(), DataType::Boolean);
    }

    #[test]
    fn test_metastore_client_creation() {
        let client = BranchingMetastoreClient::new("http://localhost:8080", "main");
        assert_eq!(client.branch_id, "main");
        assert_eq!(client.base_url, "http://localhost:8080");
    }
}
