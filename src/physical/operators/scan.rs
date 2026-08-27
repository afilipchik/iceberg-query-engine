//! Table scan operator

use crate::error::Result;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

/// Table statistics from a data source
#[derive(Debug, Clone)]
pub struct TableStatistics {
    /// Exact row count from metadata
    pub row_count: usize,
    /// Total size in bytes (approximate)
    pub total_byte_size: u64,
    /// Per-column statistics keyed by (unqualified, lowercase) column name
    pub column_stats: std::collections::HashMap<String, ColumnStatistics>,
}

/// Per-column statistics derived from file metadata (e.g. Parquet footers).
///
/// `ndv_est` is an estimated number of distinct values. For integer columns it
/// is derived as `min(row_count - null_count, max - min + 1)`: TPC-H-style
/// surrogate keys are dense ranges, so the value range is a tight NDV bound,
/// and for genuinely sparse columns it still upper-bounds NDV by row count.
#[derive(Debug, Clone, Default)]
pub struct ColumnStatistics {
    /// Minimum value (integer-typed columns only for now)
    pub min_i64: Option<i64>,
    /// Maximum value (integer-typed columns only for now)
    pub max_i64: Option<i64>,
    /// Total null count across all files/row groups, if known everywhere
    pub null_count: Option<u64>,
    /// Estimated number of distinct values
    pub ndv_est: Option<u64>,
    /// SAMPLED minimum for float columns (Lance fills these from its first
    /// fragment). An estimate for selectivity gating only — NOT a bound, and
    /// never a correctness input.
    pub min_f64: Option<f64>,
    /// SAMPLED maximum for float columns; see `min_f64`.
    pub max_f64: Option<f64>,
    /// SAMPLED distinct-value count for string columns, capped (a capped
    /// sample reading "N" means ">= N distinct in the sample"). Estimate
    /// only; see `min_f64`.
    pub ndv_str: Option<u64>,
}

/// Table provider trait for accessing table data
pub trait TableProvider: Send + Sync + fmt::Debug {
    /// Get the schema of the table
    fn schema(&self) -> SchemaRef;

    /// Type-erased downcast escape hatch. Lets a caller that must recognize
    /// one SPECIFIC concrete provider type recover it from a type-erased
    /// `Arc<dyn TableProvider>`/`&dyn TableProvider` handle, without adding
    /// a provider-specific capability method to this trait itself.
    ///
    /// Used by `src/physical/planner.rs`'s dense-direct-address aggregate
    /// routing (task 005 of the native-tables-foundation epic) to
    /// recognize a `NativeTable` specifically. Deliberately NOT something
    /// every non-Parquet provider can satisfy generically: widening that
    /// fast path to arbitrary providers (e.g. Lance) was investigated and
    /// rejected on its own merits (see CLAUDE.md's "Tried, measured,
    /// REJECTED" table), so a shared capability method would be the wrong
    /// shape here — an explicit downcast keeps that boundary intentional.
    ///
    /// Every implementor: `fn as_any(&self) -> &dyn std::any::Any { self }`.
    fn as_any(&self) -> &dyn std::any::Any;

    /// Get all batches from the table
    fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>>;

    /// Scan with an optional filter predicate for row group pruning.
    /// Default implementation ignores the filter and delegates to `scan()`.
    fn scan_with_filter(
        &self,
        projection: Option<&[usize]>,
        _filter: Option<&crate::planner::Expr>,
    ) -> Result<Vec<RecordBatch>> {
        self.scan(projection)
    }

    /// Get table-level statistics (row count, byte size).
    /// Returns None if statistics are not available.
    fn statistics(&self) -> Option<TableStatistics> {
        None
    }

    /// Get Parquet file paths if this is a Parquet-based table
    /// Returns None for non-Parquet tables (e.g., MemoryTable)
    fn parquet_files(&self) -> Option<Vec<PathBuf>> {
        None
    }

    /// A stable, hashable identity for this provider's currently-loaded
    /// data. `None` means "no stable identity available" — the provider is
    /// then ineligible for any mechanism that needs to detect "this is the
    /// same data as last time" vs. "this data changed/reloaded" (today:
    /// `GpuAggPlan::pid()` in `src/physical/gpu.rs`, the GPU-resident
    /// aggregate offload cache key). Two calls returning equal bytes are
    /// treated as identical data; a provider whose underlying data changes
    /// (e.g. a table reload/replace) MUST return different bytes afterward,
    /// or a resident cache keyed on this identity would silently serve
    /// stale results.
    ///
    /// Default: derived from [`parquet_files`](Self::parquet_files) — `None`
    /// unless that returns `Some`, in which case a hash of the file list.
    /// This is exactly `GpuAggPlan::pid()`'s pre-existing, already-validated
    /// behavior, generalized here so a provider opts in by overriding
    /// EITHER method (a Parquet-backed provider typically only needs
    /// `parquet_files`; a provider with no file list of its own, e.g. a
    /// future native-table format with its own manifest version/snapshot
    /// marker, overrides `identity` directly) rather than `pid()` needing to
    /// special-case provider types by name. A provider that must NEVER be
    /// identity-eligible (e.g. `ShardedParquetTable`, whose distributed
    /// shards must never alias the GPU cache's full-table entries) achieves
    /// that today simply by its existing `parquet_files() -> None` override
    /// — no separate override of this method is needed for that guarantee
    /// to hold.
    fn identity(&self) -> Option<Vec<u8>> {
        let files = self.parquet_files()?;
        use std::hash::{Hash, Hasher};
        let mut h = std::collections::hash_map::DefaultHasher::new();
        for f in &files {
            f.hash(&mut h);
        }
        Some(h.finish().to_le_bytes().to_vec())
    }

    /// Serve a k-nearest-neighbour search from a vector index, if this provider
    /// has one.
    ///
    /// `Ok(None)` means "not supported" and is the default: the caller MUST
    /// then fall back to the exact brute-force path, which is always correct.
    /// A provider therefore cannot change any existing query's answer by
    /// existing, and this method can gain fields without touching implementors.
    ///
    /// # Approximate results
    ///
    /// An implementation backed by an approximate index (IVF_PQ, HNSW) may
    /// return rows that differ from the exact top-k. That is why
    /// [`VectorQuery::use_index`] exists and why the engine only calls this
    /// when the user has opted in — see `VectorSearchExec`.
    fn scan_knn(
        &self,
        _projection: Option<&[usize]>,
        _query: &crate::physical::vector::VectorQuery,
    ) -> Result<Option<Vec<RecordBatch>>> {
        Ok(None)
    }

    /// Enumerate this table's distributed splits — the atoms `assign_lpt`
    /// divides across cluster nodes. `None` (the default) means the provider
    /// cannot be sharded and a distributed query over it must be refused;
    /// `Some(Err(..))` means it should be shardable but enumeration failed,
    /// which must fail the query rather than downgrade it.
    ///
    /// The contract that makes leaderless assignment sound: every node's
    /// enumeration of the same table over the same data must be IDENTICAL
    /// (canonical order, same digest). See `distributed::splits`.
    fn distributed_splits(
        &self,
        table: &str,
        nodes: usize,
    ) -> Option<Result<crate::distributed::SplitSet>> {
        let _ = (table, nodes);
        None
    }

    /// A provider restricted to exactly `splits` — one node's shard of this
    /// table. Must be implemented by every provider whose
    /// [`distributed_splits`](Self::distributed_splits) returns `Some`.
    fn shard_by_splits(
        &self,
        splits: &[crate::distributed::Split],
    ) -> Option<Result<Arc<dyn TableProvider>>> {
        let _ = splits;
        None
    }
}

/// In-memory table provider
#[derive(Debug, Clone)]
pub struct MemoryTable {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl MemoryTable {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        Self { schema, batches }
    }

    pub fn try_new(batches: Vec<RecordBatch>) -> Result<Self> {
        let schema = if batches.is_empty() {
            Arc::new(arrow::datatypes::Schema::empty())
        } else {
            batches[0].schema()
        };
        Ok(Self { schema, batches })
    }
}

impl TableProvider for MemoryTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        match projection {
            Some(indices) => self
                .batches
                .iter()
                .map(|batch| {
                    let columns: Vec<_> =
                        indices.iter().map(|&i| batch.column(i).clone()).collect();
                    // Field TYPES must follow the actual columns, not the
                    // declared logical schema -- mirrors
                    // `MemoryTableExec::execute`'s own `rewrap` a few lines
                    // below in this same file (see its doc comment): a
                    // `MemoryTable` backing a native table's unfiltered
                    // `scan()` output can carry dictionary-encoded string
                    // columns (native tables dictionary-coerce
                    // low-cardinality Utf8 columns; `NativeTable::schema()`
                    // reports the plain logical type) while `self.schema`
                    // here still declares the plain type. Building the
                    // projected schema straight from `self.schema.field(i)`
                    // without this check makes `RecordBatch::try_new` fail
                    // outright ("expected Utf8 but found
                    // Dictionary(Int32, Utf8)") the moment ANY projection is
                    // pushed down to this call -- caught by task 002's own
                    // broader pruning-sweep validation
                    // (native-table-pruning epic), which is the first place
                    // in this codebase's history a dictionary-coerced
                    // native table's data was ever re-registered as a
                    // `MemoryTable` and then queried with a pushed-down
                    // projection.
                    let fields: Vec<_> = indices
                        .iter()
                        .zip(&columns)
                        .map(|(&i, c)| {
                            let f = self.schema.field(i);
                            if f.data_type() == c.data_type() {
                                f.clone()
                            } else {
                                arrow::datatypes::Field::new(
                                    f.name(),
                                    c.data_type().clone(),
                                    f.is_nullable(),
                                )
                            }
                        })
                        .collect();
                    let schema = Arc::new(arrow::datatypes::Schema::new(fields));
                    RecordBatch::try_new(schema, columns).map_err(Into::into)
                })
                .collect(),
            None => Ok(self.batches.clone()),
        }
    }

    fn statistics(&self) -> Option<TableStatistics> {
        let row_count: usize = self.batches.iter().map(|b| b.num_rows()).sum();
        let total_byte_size: u64 = self
            .batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();
        Some(TableStatistics {
            row_count,
            total_byte_size,
            column_stats: std::collections::HashMap::new(),
        })
    }
}

/// Memory table scan operator
#[derive(Debug)]
pub struct MemoryTableExec {
    table_name: String,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    projection: Option<Vec<usize>>,
}

impl MemoryTableExec {
    pub fn new(
        table_name: impl Into<String>,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let projected_schema = match &projection {
            Some(indices) => {
                let fields: Vec<_> = indices.iter().map(|&i| schema.field(i).clone()).collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => schema.clone(),
        };

        Self {
            table_name: table_name.into(),
            schema: projected_schema,
            batches,
            projection,
        }
    }

    pub fn from_provider(
        table_name: impl Into<String>,
        provider: &dyn TableProvider,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let batches = provider.scan(projection.as_deref())?;
        let schema = match &projection {
            Some(indices) => {
                let base_schema = provider.schema();
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| base_schema.field(i).clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => provider.schema(),
        };

        Ok(Self {
            table_name: table_name.into(),
            schema,
            batches,
            projection: None, // Already projected
        })
    }

    /// Create from provider with a specified logical schema (preserves table aliases)
    pub fn from_provider_with_schema(
        table_name: impl Into<String>,
        provider: &dyn TableProvider,
        projection: Option<Vec<usize>>,
        logical_schema: SchemaRef,
    ) -> Result<Self> {
        let batches = provider.scan(projection.as_deref())?;

        // Use the logical schema which has proper qualified names
        let schema = match &projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| logical_schema.field(i).clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => logical_schema,
        };

        Ok(Self {
            table_name: table_name.into(),
            schema,
            batches,
            projection: None, // Already projected
        })
    }
}

#[async_trait]
impl PhysicalOperator for MemoryTableExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        crate::physical::check_partition(self, partition)?;

        // Determine the number of partitions to use
        let num_partitions = self.output_partitions().max(1);

        // Split batches across partitions
        let partition_batches: Vec<RecordBatch> = self
            .batches
            .iter()
            .enumerate()
            .filter(|(i, _)| i % num_partitions == partition)
            .map(|(_, batch)| batch.clone())
            .collect();

        // Re-wrapping with the logical schema preserves qualified names
        // (e.g., "n1.n_name" vs "n2.n_name" for self-joins), but the FIELD
        // TYPES must follow the actual columns: intermediate results routed
        // through here (ExternalSortExec's in-memory path, subquery
        // materialization) may carry dictionary-encoded string columns from
        // small-build join gathers.
        let rewrap = |columns: Vec<arrow::array::ArrayRef>| -> Result<RecordBatch> {
            let types_match = columns
                .iter()
                .zip(self.schema.fields())
                .all(|(c, f)| c.data_type() == f.data_type());
            let schema = if types_match {
                self.schema.clone()
            } else {
                Arc::new(arrow::datatypes::Schema::new(
                    self.schema
                        .fields()
                        .iter()
                        .zip(&columns)
                        .map(|(f, c)| {
                            if f.data_type() == c.data_type() {
                                f.as_ref().clone()
                            } else {
                                arrow::datatypes::Field::new(f.name(), c.data_type().clone(), true)
                            }
                        })
                        .collect::<Vec<_>>(),
                ))
            };
            RecordBatch::try_new(schema, columns).map_err(Into::into)
        };
        let batches = match &self.projection {
            Some(indices) => partition_batches
                .iter()
                .map(|batch| {
                    let columns: Vec<_> =
                        indices.iter().map(|&i| batch.column(i).clone()).collect();
                    rewrap(columns)
                })
                .collect::<Result<Vec<_>>>()?,
            None => partition_batches
                .into_iter()
                .map(|batch| {
                    if batch.schema() != self.schema
                        && batch.num_columns() == self.schema.fields().len()
                    {
                        rewrap(batch.columns().to_vec())
                    } else {
                        Ok(batch)
                    }
                })
                .collect::<Result<Vec<_>>>()?,
        };

        let stream = stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(stream))
    }

    fn output_partitions(&self) -> usize {
        // Use rayon to determine the number of CPU cores for parallel execution
        // For small tables, use fewer partitions to avoid overhead
        let total_rows: usize = self.batches.iter().map(|b| b.num_rows()).sum();
        if total_rows < 1000 {
            1 // Small table, single partition
        } else {
            std::cmp::min(rayon::current_num_threads(), self.batches.len())
        }
    }

    fn name(&self) -> &str {
        "MemoryTableScan"
    }
}

impl fmt::Display for MemoryTableExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MemoryTableScan: {}", self.table_name)?;
        if let Some(proj) = &self.projection {
            write!(f, " projection={:?}", proj)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::TryStreamExt;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_memory_scan() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let exec = MemoryTableExec::new("test", schema, vec![batch.clone()], None);

        let stream = exec.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
        assert_eq!(results[0].num_columns(), 2);
    }

    #[tokio::test]
    async fn test_memory_scan_with_projection() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let exec = MemoryTableExec::new("test", schema, vec![batch], Some(vec![0]));

        let stream = exec.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 3);
        assert_eq!(results[0].num_columns(), 1);
    }

    /// Regression test (native-table-pruning epic, task 002): a `MemoryTable`
    /// registered with a plain-`Utf8`-declared schema but whose ACTUAL
    /// batches carry a `Dictionary(Int32, Utf8)` array for that column (the
    /// exact shape a native table's own `scan()` output has for a
    /// dictionary-coerced low-cardinality string column, per `native_table
    /// .rs`'s module doc) must still be projectable via `TableProvider::
    /// scan(Some(projection))` -- before this fix, `MemoryTable::scan`
    /// built the projected schema straight from the DECLARED field type
    /// and `RecordBatch::try_new` failed outright ("column types must
    /// match schema types, expected Utf8 but found Dictionary(Int32,
    /// Utf8)") the moment any predicate pushed a narrower projection down
    /// to a table in this shape. Found by task 002's own broader
    /// pruning-sweep validation (`examples/native_pruning_sweep.rs`),
    /// which was the first place in this codebase to re-register a
    /// dictionary-coerced native table's `scan(None)` output as a
    /// `MemoryTable` and then query it with a pushed-down projection.
    #[test]
    fn memory_table_scan_projection_tolerates_declared_vs_actual_dictionary_mismatch() {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::datatypes::Int32Type;

        // Declared (logical) schema says `name` is plain Utf8 -- matches
        // what `NativeTable::schema()` reports for a dictionary-coerced
        // column.
        let declared_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));

        // ACTUAL batch carries a Dictionary-encoded array for `name` --
        // matches what a native table's `ipc_cache`-backed segments
        // physically store on disk.
        let dict_array: DictionaryArray<Int32Type> = vec!["a", "b", "c"].into_iter().collect();
        let actual_batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new(
                    "name",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                ),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(dict_array),
            ],
        )
        .unwrap();

        let table = MemoryTable::new(declared_schema, vec![actual_batch]);

        // Full (unprojected) scan already worked before this fix (it never
        // rebuilds a RecordBatch from a separately-declared schema).
        let full = TableProvider::scan(&table, None).expect("unprojected scan");
        assert_eq!(full[0].num_columns(), 2);

        // Projected scan is the shape that used to fail.
        let projected = TableProvider::scan(&table, Some(&[1]))
            .expect("projected scan must tolerate a declared-vs-actual dictionary mismatch");
        assert_eq!(projected.len(), 1);
        assert_eq!(projected[0].num_columns(), 1);
        assert_eq!(
            projected[0].column(0).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            "projected field type must follow the ACTUAL column, not the declared Utf8 schema"
        );

        // A column whose actual type DOES match the declaration keeps its
        // original field metadata (name/nullability), not just its type --
        // confirms the fast (types_match) path is still exercised.
        let id_only = TableProvider::scan(&table, Some(&[0])).expect("projected scan of id");
        assert_eq!(id_only[0].schema().field(0).data_type(), &DataType::Int64);
        assert!(!id_only[0].schema().field(0).is_nullable());
    }
}
