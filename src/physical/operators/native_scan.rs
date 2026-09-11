//! `NativeStreamingScanExec` — a segment-at-a-time streaming scan over a
//! native table (oom-safety-hardening epic, task 004; epic Architecture
//! Decision 4: "streaming native scan is a new operator, not a rewrite of
//! `scan()`", mirroring the `ParquetTable::scan()` vs
//! `StreamingParquetScanExec` split).
//!
//! # Why this exists
//!
//! `NativeTable::scan()` materializes every active segment into one
//! `Vec<RecordBatch>` and therefore refuses (`check_scan_budget`) whenever
//! the table's on-disk size exceeds `memory_limit * spill_threshold`. That
//! refusal is the RIGHT answer for shapes that genuinely must materialize
//! (a raw `SELECT *` dump into a `QueryResult`), but it is the WRONG answer
//! for a query whose consumers are spill-capable — an aggregate or join can
//! process a larger-than-memory table batch-by-batch, spilling as needed
//! (PRD G2: complete by spilling, never refuse work the engine can do).
//! This operator is the streaming half of that split; `check_scan_budget`
//! stays as the guard for the materializing half, and the planner
//! (`src/physical/planner.rs`, Scan arm) decides which half a given query
//! shape gets — see `PhysicalPlanner::collect_spill_covered_scans`.
//!
//! # What it preserves from the materializing path — deliberately identical
//!
//! - **Deletion vectors**: every segment read goes through
//!   `NativeTable::open_segment_batches`, which applies
//!   the shared deletion cursor used by `scan()`/`scan_with_filter` — a
//!   tombstoned row can never reach a consumer through either path.
//! - **Segment pruning**: the segment list is
//!   `NativeTable::streaming_segment_ids(filter)`, the SAME
//!   `segment_might_match` pruning `scan_with_filter` performs. The planner
//!   still wraps a `FilterExec` above this operator whenever the Scan node
//!   carries a predicate (this operator never evaluates predicates itself),
//!   so pruning here can only skip PROVABLY unsatisfiable segments — a
//!   wrong "might match" costs performance, never correctness.
//! - **Dictionary transparency**: batches come back from
//!   `ipc_cache::read_row_group` genuinely dictionary-encoded while the
//!   declared schema reports the decoded value type; each yielded batch is
//!   re-tagged with the declared (logical, possibly alias-qualified) field
//!   NAMES but the columns' ACTUAL types — the exact `rewrap` pattern
//!   `MemoryTableExec::execute` established (`scan.rs`).
//!
//! # Memory shape
//!
//! Each partition holds one mapped segment reader and decodes/filters one batch
//! per pull. It never queues survivor copies for later batches. Mmap buffers keep
//! their mapping alive even after the reader drops. Footer metadata, dictionaries
//! and deletion vectors remain reader-owned; allocations and retained outputs are
//! not yet query-pool admitted. Partition count is capped (`MAX_PARTITIONS`).

use crate::error::{QueryError, Result};
use crate::physical::{check_partition, PhysicalOperator, RecordBatchStream};
use crate::planner::Expr;
use crate::storage::NativeTable;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use std::sync::Arc;

/// Upper bound on concurrently-driven partitions (and therefore on
/// concurrently-resident segments). Deliberately modest: this operator only
/// ever runs for tables that ALREADY exceed the memory budget, where
/// "slow but bounded" is the contract (Memory Safety Rule: being slow on
/// larger-than-memory datasets is acceptable; OOM is not). In-budget tables
/// never reach this operator, so their performance is untouched.
const MAX_PARTITIONS: usize = 8;

/// Streaming scan over a native table: reads its (post-pruning) segments
/// lazily, one at a time per partition, deletion-vector-filtered. See the
/// module doc.
#[derive(Debug)]
pub struct NativeStreamingScanExec {
    table_name: String,
    table: Arc<NativeTable>,
    /// Projected DECLARED schema (logical field names — possibly
    /// alias-qualified — with decoded value types). Actual batches may
    /// carry dictionary-encoded columns; `rewrap_batch` reconciles.
    schema: SchemaRef,
    projection: Option<Vec<usize>>,
    /// Post-pruning segment ids, canonical id order.
    segment_ids: Vec<u32>,
    partitions: usize,
}

impl NativeStreamingScanExec {
    /// `logical_schema` is the UNPROJECTED logical schema of the Scan node
    /// (`plan_schema_to_arrow(&node.schema)`); `projection` indexes into it
    /// (and, identically, into the manifest's column order — the two share
    /// one column order by construction, see `NativeTable::logical_schema`).
    /// `filter` is used ONLY for segment pruning; the caller remains
    /// responsible for wrapping a `FilterExec` above this operator.
    pub fn new(
        table_name: impl Into<String>,
        table: &NativeTable,
        logical_schema: SchemaRef,
        projection: Option<Vec<usize>>,
        filter: Option<&Expr>,
    ) -> Self {
        let segment_ids = table.streaming_segment_ids(filter);
        let schema = match &projection {
            Some(indices) => Arc::new(Schema::new(
                indices
                    .iter()
                    .map(|&i| logical_schema.field(i).clone())
                    .collect::<Vec<_>>(),
            )),
            None => logical_schema,
        };
        let partitions = segment_ids.len().clamp(1, MAX_PARTITIONS);
        Self {
            table_name: table_name.into(),
            table: Arc::new(table.clone()),
            schema,
            projection,
            segment_ids,
            partitions,
        }
    }

    /// How many segments this scan will actually read (post-pruning) —
    /// exposed for tests pinning that pruning applies on the streaming path.
    pub fn segment_count(&self) -> usize {
        self.segment_ids.len()
    }
}

/// Re-tag `batch` with the declared field NAMES while keeping the columns'
/// ACTUAL types — the established `MemoryTableExec::execute` `rewrap`
/// pattern (`scan.rs`): dictionary-coerced columns stay physically
/// dictionary-encoded (declared `Utf8`), and qualified/aliased logical
/// names survive for self-joins.
fn rewrap_batch(declared: &SchemaRef, batch: RecordBatch) -> Result<RecordBatch> {
    if batch.num_columns() != declared.fields().len() {
        return Err(QueryError::Internal(format!(
            "NativeStreamingScanExec: segment batch has {} columns but the declared schema has {}",
            batch.num_columns(),
            declared.fields().len()
        )));
    }
    let types_match = batch
        .columns()
        .iter()
        .zip(declared.fields())
        .all(|(c, f)| c.data_type() == f.data_type());
    let schema = if types_match {
        declared.clone()
    } else {
        Arc::new(Schema::new(
            declared
                .fields()
                .iter()
                .zip(batch.columns())
                .map(|(f, c)| {
                    if f.data_type() == c.data_type() {
                        f.as_ref().clone()
                    } else {
                        f.as_ref()
                            .clone()
                            .with_data_type(c.data_type().clone())
                            .with_nullable(true)
                    }
                })
                .collect::<Vec<_>>(),
        ))
    };
    RecordBatch::try_new(schema, batch.columns().to_vec()).map_err(Into::into)
}

#[async_trait]
impl PhysicalOperator for NativeStreamingScanExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }

    fn output_partitions(&self) -> usize {
        self.partitions
    }

    fn name(&self) -> &str {
        "NativeStreamingScanExec"
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        check_partition(self, partition)?;
        // Round-robin segment assignment: partition p reads segments
        // p, p+P, p+2P, ... in id order, lazily (next segment is opened
        // only after the previous one's batches are fully drained).
        let ids: Vec<u32> = self
            .segment_ids
            .iter()
            .enumerate()
            .filter(|(i, _)| i % self.partitions == partition)
            .map(|(_, id)| *id)
            .collect();
        let table = Arc::clone(&self.table);
        let projection = self.projection.clone();
        let declared = self.schema.clone();

        struct State {
            ids: std::vec::IntoIter<u32>,
            reader: Option<crate::storage::native_table::NativeSegmentReader>,
        }
        let stream = futures::stream::try_unfold(
            State {
                ids: ids.into_iter(),
                reader: None,
            },
            move |mut st| {
                let table = Arc::clone(&table);
                let projection = projection.clone();
                let declared = declared.clone();
                async move {
                    // Opening, decoding and deletion filtering stay off reactor
                    // threads. Exactly one output is produced by each blocking job.
                    tokio::task::spawn_blocking(move || -> Result<_> {
                        loop {
                            if let Some(reader) = &mut st.reader {
                                if let Some(batch) = reader.next() {
                                    return Ok(Some((rewrap_batch(&declared, batch?)?, st)));
                                }
                                st.reader = None;
                            }
                            let Some(seg_id) = st.ids.next() else {
                                return Ok(None);
                            };
                            st.reader =
                                Some(table.open_segment_batches(seg_id, projection.as_deref())?);
                        }
                    })
                    .await
                    .map_err(|e| {
                        QueryError::Execution(format!(
                            "NativeStreamingScanExec: segment read task failed: {e}"
                        ))
                    })?
                }
            },
        );
        let _ = &self.table_name; // retained for display/debug parity
        Ok(Box::pin(stream))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::operators::TableProvider;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field};
    use futures::TryStreamExt;

    /// Build a real on-disk native table with `n_segments` single-batch
    /// segments of `rows_per_segment` consecutive ids each (disjoint,
    /// ascending ranges — so range predicates can prune provably).
    async fn write_table(dir: &std::path::Path, n_segments: usize, rows_per_segment: usize) {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut batches = Vec::new();
        for s in 0..n_segments {
            let start = (s * rows_per_segment) as i64;
            let ids: Vec<i64> = (start..start + rows_per_segment as i64).collect();
            batches.push(Ok(RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(ids)) as arrow::array::ArrayRef],
            )
            .unwrap()));
        }
        let stream: RecordBatchStream = Box::pin(futures::stream::iter(batches));
        crate::storage::native_write::write_batches_with_options(
            stream,
            schema,
            dir,
            crate::storage::native_write::NativeWriteMode::Create,
            crate::storage::native_write::NativeWriteOptions {
                target_rows_per_segment: rows_per_segment,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    }

    /// A later malformed block must fail when pulled, without eagerly decoding
    /// the whole segment before the first valid output or replaying after error.
    #[tokio::test(flavor = "multi_thread")]
    async fn segment_decoding_is_incremental_and_late_errors_are_terminal() {
        use arrow::ipc::{reader::read_footer_length, writer::FileWriter, Block};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        write_table(&path, 1, 100).await;
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut bytes = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut bytes, &schema).unwrap();
            for start in [0, 50] {
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![Arc::new(Int64Array::from_iter_values(start..start + 50))],
                )
                .unwrap();
                writer.write(&batch).unwrap();
            }
            writer.finish().unwrap();
        }
        let trailer = bytes.len() - 10;
        let footer_start =
            trailer - read_footer_length(bytes[trailer..].try_into().unwrap()).unwrap();
        let footer = arrow::ipc::root_as_footer(&bytes[footer_start..trailer]).unwrap();
        let block = footer.recordBatches().unwrap().get(1);
        let descriptor = block.0.as_ptr() as usize - bytes.as_ptr() as usize;
        let bad = Block::new(-1, block.metaDataLength(), block.bodyLength());
        bytes[descriptor..descriptor + 24].copy_from_slice(&bad.0);
        std::fs::write(path.join("rg_00000.arrow"), bytes).unwrap();
        let table = NativeTable::try_new(&path).unwrap();
        let exec = NativeStreamingScanExec::new("t", &table, table.schema(), None, None);
        let mut stream = exec.execute(0).await.unwrap();
        let first = stream
            .try_next()
            .await
            .expect("first valid block must precede late error")
            .unwrap();
        let ids = first
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.values().as_ref(), &(0..50).collect::<Vec<i64>>());
        let held = first.column(0).clone();
        drop(first);
        let error = stream.try_next().await.unwrap_err();
        assert!(error.to_string().contains("invalid offset"), "{error}");
        assert!(stream.try_next().await.unwrap().is_none());
        drop(stream);
        drop(exec);
        drop(table);
        assert_eq!(
            held.as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(49),
            49
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn streams_every_row_across_all_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        write_table(&path, 5, 100).await;
        let table = NativeTable::try_new(&path).unwrap();
        let exec = NativeStreamingScanExec::new("t", &table, table.schema(), None, None);
        assert_eq!(exec.segment_count(), 5);
        let mut ids = Vec::new();
        for p in 0..exec.output_partitions() {
            let mut stream = exec.execute(p).await.unwrap();
            while let Some(batch) = stream.try_next().await.unwrap() {
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                ids.extend(col.values().iter().copied());
            }
        }
        ids.sort_unstable();
        assert_eq!(ids, (0..500).collect::<Vec<i64>>());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn pruning_applies_on_the_streaming_path() {
        use crate::planner::{BinaryOp, ScalarValue};
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        write_table(&path, 5, 100).await;
        let table = NativeTable::try_new(&path).unwrap();
        // id >= 450: only the LAST segment (ids 400..500) can match.
        let pred = Expr::BinaryExpr {
            left: Box::new(Expr::column("id")),
            op: BinaryOp::GtEq,
            right: Box::new(Expr::Literal(ScalarValue::Int64(450))),
        };
        let exec = NativeStreamingScanExec::new("t", &table, table.schema(), None, Some(&pred));
        assert_eq!(
            exec.segment_count(),
            1,
            "4 of 5 segments must be pruned for id >= 450"
        );
        let mut rows = 0usize;
        for p in 0..exec.output_partitions() {
            let mut stream = exec.execute(p).await.unwrap();
            while let Some(batch) = stream.try_next().await.unwrap() {
                rows += batch.num_rows();
            }
        }
        // Pruning yields the whole surviving segment (100 rows); the
        // planner's FilterExec above is what narrows it to the exact 50.
        assert_eq!(rows, 100);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deletion_vectors_are_applied_on_the_streaming_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        write_table(&path, 3, 100).await;
        // Tombstone rows via the REAL delete mechanism: id % 10 == 0.
        let pred = Expr::BinaryExpr {
            left: Box::new(Expr::BinaryExpr {
                left: Box::new(Expr::column("id")),
                op: crate::planner::BinaryOp::Modulo,
                right: Box::new(Expr::Literal(crate::planner::ScalarValue::Int64(10))),
            }),
            op: crate::planner::BinaryOp::Eq,
            right: Box::new(Expr::Literal(crate::planner::ScalarValue::Int64(0))),
        };
        let res = crate::storage::native_delete::delete_from_native_table(&path, Some(&pred))
            .await
            .unwrap();
        assert_eq!(res.rows_deleted, 30);

        let table = NativeTable::try_new(&path).unwrap();
        let exec = NativeStreamingScanExec::new("t", &table, table.schema(), None, None);
        let mut ids = Vec::new();
        for p in 0..exec.output_partitions() {
            let mut stream = exec.execute(p).await.unwrap();
            while let Some(batch) = stream.try_next().await.unwrap() {
                let col = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap();
                ids.extend(col.values().iter().copied());
            }
        }
        ids.sort_unstable();
        let expected: Vec<i64> = (0..300).filter(|i| i % 10 != 0).collect();
        assert_eq!(ids, expected, "tombstoned rows must never be streamed");
    }
}
