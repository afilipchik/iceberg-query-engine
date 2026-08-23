//! A table restricted to one node's assigned splits.
//!
//! [`ShardedParquetTable`] is an ordinary [`TableProvider`] that happens to see
//! only part of the data, so a worker executes a completely normal local plan —
//! no distributed operator, no exchange, nothing new below the scan. That is
//! the whole reason M2 needs none of the fixes in `DISTRIBUTED-READINESS.md`.
//!
//! # The one property that must not be lost
//!
//! `parquet_files()` returns **`None`**, on purpose and permanently.
//!
//! Three fast paths in the physical planner (`MorselAggregateExec`,
//! `StreamingParquetScanExec`, and the shared prescan) are keyed off
//! `parquet_files()` and read the returned paths *whole*. Handing them this
//! shard's file list would silently read every row of every file on every node
//! — every shard would see all the data, the two-phase merge would sum N copies
//! of the table, and `COUNT(*)` would return N times the truth while looking
//! entirely healthy. Returning `None` makes that unreachable by construction
//! rather than by remembering. The cost is that a shard scan does not get the
//! morsel aggregate path; the alternative is an answer that is wrong.

use crate::distributed::splits::Split;
use crate::error::{QueryError, Result};
use crate::physical::operators::{TableProvider, TableStatistics};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use std::fmt;
use std::sync::Arc;

/// The slice of a Parquet table that one node is responsible for.
pub struct ShardedParquetTable {
    schema: SchemaRef,
    splits: Vec<Split>,
    bytes: u64,
    rows: i64,
    /// Statistics of the whole table, scaled to this shard. Kept because the
    /// optimizer's cost model consults them and a shard that claims the full
    /// table's row count would misprice its own local plan.
    base_stats: Option<TableStatistics>,
}

impl fmt::Debug for ShardedParquetTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardedParquetTable")
            .field("splits", &self.splits.len())
            .field("rows", &self.rows)
            .field("bytes", &self.bytes)
            .finish()
    }
}

impl ShardedParquetTable {
    pub fn new(schema: SchemaRef, splits: Vec<Split>, base_stats: Option<TableStatistics>) -> Self {
        let bytes = splits.iter().map(|s| s.bytes).sum();
        let rows = splits.iter().map(|s| s.num_rows).sum();
        Self {
            schema,
            splits,
            bytes,
            rows,
            base_stats,
        }
    }

    pub fn split_count(&self) -> usize {
        self.splits.len()
    }

    pub fn assigned_bytes(&self) -> u64 {
        self.bytes
    }

    pub fn assigned_rows(&self) -> i64 {
        self.rows
    }

    /// Read one split: one row range of one row group.
    fn read_split(
        &self,
        split: &Split,
        projection: Option<&[usize]>,
        filter: Option<&crate::planner::Expr>,
    ) -> Result<Vec<RecordBatch>> {
        let builder = crate::storage::metadata_cache::cached_reader_builder(&split.path)?;
        let metadata = builder.metadata().clone();

        if split.row_group >= metadata.num_row_groups() {
            return Err(QueryError::Execution(format!(
                "split refers to row group {} of {} which has only {}; \
                 the node's copy of the data does not match the assignment",
                split.row_group,
                split.file,
                metadata.num_row_groups()
            )));
        }
        let rg_rows = metadata.row_group(split.row_group).num_rows();
        if split.row_offset + split.num_rows > rg_rows {
            return Err(QueryError::Execution(format!(
                "split {}[{}] rows {}..{} exceeds the row group's {rg_rows} rows; \
                 the node's copy of the data does not match the assignment",
                split.file,
                split.row_group,
                split.row_offset,
                split.row_offset + split.num_rows
            )));
        }

        // Row-group pruning still applies inside a shard: a split whose row
        // group cannot match the predicate is skipped without being read.
        if let Some(pred) = filter {
            let keep = crate::storage::row_group_pruning::prune_row_groups(
                &metadata,
                &self.schema,
                Some(pred),
            );
            if !keep.contains(&split.row_group) {
                return Ok(Vec::new());
            }
        }

        let mut builder = builder
            .with_batch_size(8_192)
            .with_row_groups(vec![split.row_group]);

        // Only build a selection when the split is a genuine sub-range; a
        // whole-row-group split takes the plain path arrow-rs optimizes best.
        if !split.is_whole_row_group(rg_rows) {
            let mut selectors = Vec::with_capacity(2);
            if split.row_offset > 0 {
                selectors.push(RowSelector::skip(split.row_offset as usize));
            }
            selectors.push(RowSelector::select(split.num_rows as usize));
            builder = builder.with_row_selection(RowSelection::from(selectors));
        }

        // Decoder-level predicate pushdown, same shape as ParquetTable's.
        // A FilterExec above the scan re-checks survivors, so this is a
        // performance device and never the thing that decides correctness.
        if let Some(pred) = filter {
            if !pred.contains_subquery() {
                let mut cols: Vec<String> = Vec::new();
                crate::physical::morsel::collect_expr_columns(pred, &mut cols);
                let indices: Option<Vec<usize>> = cols
                    .iter()
                    .map(|c| {
                        self.schema
                            .fields()
                            .iter()
                            .position(|f| f.name().eq_ignore_ascii_case(c))
                    })
                    .collect();
                if let Some(mut indices) = indices {
                    indices.sort_unstable();
                    indices.dedup();
                    if !indices.is_empty() {
                        use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
                        let mask = parquet::arrow::ProjectionMask::roots(
                            builder.parquet_schema(),
                            indices.iter().copied(),
                        );
                        let pred = pred.clone();
                        let f = ArrowPredicateFn::new(mask, move |batch: RecordBatch| {
                            let arr = crate::physical::operators::evaluate_expr(&batch, &pred)
                                .map_err(|e| {
                                    arrow::error::ArrowError::ComputeError(e.to_string())
                                })?;
                            arr.as_any()
                                .downcast_ref::<arrow::array::BooleanArray>()
                                .cloned()
                                .ok_or_else(|| {
                                    arrow::error::ArrowError::ComputeError(
                                        "row filter did not evaluate to boolean".into(),
                                    )
                                })
                        });
                        builder = builder.with_row_filter(RowFilter::new(vec![Box::new(f)]));
                    }
                }
            }
        }

        let reader = match projection {
            Some(indices) => {
                let mask = parquet::arrow::ProjectionMask::roots(
                    builder.parquet_schema(),
                    indices.iter().copied(),
                );
                builder.with_projection(mask).build()?
            }
            None => builder.build()?,
        };
        Ok(reader.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    fn scan_impl(
        &self,
        projection: Option<&[usize]>,
        filter: Option<&crate::planner::Expr>,
    ) -> Result<Vec<RecordBatch>> {
        use rayon::prelude::*;
        if self.splits.is_empty() {
            // An empty shard is a legitimate state (fewer splits than nodes),
            // and it must produce a correctly-typed EMPTY result, not an error
            // and not a missing column set: the aggregate above it still has to
            // emit its identity row.
            let schema = match projection {
                Some(indices) => Arc::new(arrow::datatypes::Schema::new(
                    indices
                        .iter()
                        .map(|&i| self.schema.field(i).clone())
                        .collect::<Vec<_>>(),
                )),
                None => self.schema.clone(),
            };
            return Ok(vec![RecordBatch::new_empty(schema)]);
        }
        let per_split: Vec<Vec<RecordBatch>> = self
            .splits
            .par_iter()
            .map(|s| self.read_split(s, projection, filter))
            .collect::<Result<Vec<_>>>()?;
        Ok(per_split.into_iter().flatten().collect())
    }
}

impl TableProvider for ShardedParquetTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn scan(&self, projection: Option<&[usize]>) -> Result<Vec<RecordBatch>> {
        self.scan_impl(projection, None)
    }

    fn scan_with_filter(
        &self,
        projection: Option<&[usize]>,
        filter: Option<&crate::planner::Expr>,
    ) -> Result<Vec<RecordBatch>> {
        self.scan_impl(projection, filter)
    }

    fn statistics(&self) -> Option<TableStatistics> {
        let base = self.base_stats.as_ref()?;
        let mut scaled = base.clone();
        scaled.row_count = self.rows.max(0) as usize;
        scaled.total_byte_size = self.bytes;
        Some(scaled)
    }

    /// Always `None`. See the module docs — this is a correctness guarantee,
    /// not an omission.
    fn parquet_files(&self) -> Option<Vec<std::path::PathBuf>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::distributed::splits::enumerate_parquet;
    use std::path::PathBuf;

    fn lineitem() -> PathBuf {
        PathBuf::from(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/data/tpch-1mb/lineitem.parquet"
        ))
    }

    fn schema_of(path: &PathBuf) -> SchemaRef {
        crate::storage::metadata_cache::cached_metadata(path)
            .unwrap()
            .schema()
            .clone()
    }

    /// The property everything else rests on: the shards, read separately and
    /// stacked, are exactly the table.
    #[test]
    fn shards_partition_the_table_exactly() {
        let path = lineitem();
        let schema = schema_of(&path);
        let set = enumerate_parquet("lineitem", std::slice::from_ref(&path), 3).unwrap();
        let whole: usize = crate::storage::ParquetTable::try_new(&path)
            .unwrap()
            .scan(None)
            .unwrap()
            .iter()
            .map(|b| b.num_rows())
            .sum();

        for nodes in [1usize, 2, 3, 7] {
            let a = crate::distributed::splits::assign_lpt(&set, nodes);
            let mut total = 0usize;
            for node in 0..nodes {
                let owned: Vec<_> = a.per_node[node]
                    .iter()
                    .map(|&i| set.splits[i].clone())
                    .collect();
                let shard = ShardedParquetTable::new(schema.clone(), owned, None);
                total += shard
                    .scan(None)
                    .unwrap()
                    .iter()
                    .map(|b| b.num_rows())
                    .sum::<usize>();
            }
            assert_eq!(
                total, whole,
                "nodes={nodes}: shards did not partition the table"
            );
        }
    }

    /// Forcing sub-row-group ranges (this file has a single row group) proves
    /// the `RowSelection` path, which is the part that could silently drop or
    /// duplicate rows.
    #[test]
    fn sub_row_group_ranges_cover_every_row_once() {
        let path = lineitem();
        let schema = schema_of(&path);
        let md = crate::storage::metadata_cache::cached_metadata(&path).unwrap();
        let rg_rows = md.metadata().row_group(0).num_rows();
        assert!(rg_rows > 100, "fixture must have enough rows to slice");

        // Cut row group 0 into 7 uneven pieces.
        let mut splits = Vec::new();
        let mut offset = 0i64;
        let mut piece = 0;
        while offset < rg_rows {
            let n = ((rg_rows / 7) + piece).min(rg_rows - offset);
            splits.push(Split {
                table: "lineitem".into(),
                path: path.clone(),
                file: "lineitem.parquet".into(),
                row_group: 0,
                row_offset: offset,
                num_rows: n,
                bytes: n as u64,
            });
            offset += n;
            piece += 1;
        }

        // Read each piece alone, concatenate, and compare against the whole.
        let key_of = |batches: &[RecordBatch]| -> Vec<i64> {
            let mut out = Vec::new();
            for b in batches {
                if b.num_rows() == 0 {
                    continue;
                }
                let a = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<arrow::array::Int64Array>()
                    .expect("l_orderkey is int64");
                out.extend(a.values().iter().copied());
            }
            out
        };

        let whole = ShardedParquetTable::new(
            schema.clone(),
            vec![Split {
                table: "lineitem".into(),
                path: path.clone(),
                file: "lineitem.parquet".into(),
                row_group: 0,
                row_offset: 0,
                num_rows: rg_rows,
                bytes: 1,
            }],
            None,
        );
        let expected = key_of(&whole.scan(Some(&[0])).unwrap());
        assert_eq!(expected.len(), rg_rows as usize);

        let mut got = Vec::new();
        for s in &splits {
            let shard = ShardedParquetTable::new(schema.clone(), vec![s.clone()], None);
            got.extend(key_of(&shard.scan(Some(&[0])).unwrap()));
        }
        assert_eq!(got, expected, "row ranges must reassemble in order");
    }

    /// The guarantee described in the module docs, asserted rather than
    /// documented: if this ever returns `Some`, the planner's whole-file fast
    /// paths make every shard read the entire table.
    #[test]
    fn parquet_files_is_none_so_no_fast_path_can_read_whole_files() {
        let path = lineitem();
        let shard = ShardedParquetTable::new(schema_of(&path), Vec::new(), None);
        assert!(shard.parquet_files().is_none());
    }

    #[test]
    fn an_empty_shard_scans_to_zero_rows_with_the_right_schema() {
        let path = lineitem();
        let schema = schema_of(&path);
        let shard = ShardedParquetTable::new(schema.clone(), Vec::new(), None);
        let batches = shard.scan(Some(&[0, 1])).unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 0);
        assert_eq!(batches[0].num_columns(), 2);
    }
}
