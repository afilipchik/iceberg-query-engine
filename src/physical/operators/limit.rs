//! Limit operator

use crate::error::Result;
use crate::physical::{PhysicalOperator, RecordBatchStream};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::stream::{self, StreamExt};
use std::fmt;
use std::sync::Arc;

/// Limit execution operator
///
/// # LIMIT/OFFSET counters must outlive a batch
///
/// This operator used to keep `skipped`/`fetched` as `usize` locals captured by
/// the outer `move` closure of a `filter_map`, and mutate them inside an inner
/// `async move` block. Both are `Copy`, so each per-batch future got its OWN
/// copy and every write died with that batch: the limit was applied per batch
/// rather than per query. `SELECT l_orderkey FROM lineitem LIMIT 10` returned
/// **740** rows on data/tpch-100mb (74 batches x 10). The
/// `#[allow(unused_assignments)]` that used to sit on `execute`, annotated
/// "Variables are read across multiple closure invocations", was the compiler
/// saying the exact opposite. It is gone; do not re-add it.
///
/// The counters now live in the stream's own state (`stream::unfold`), which is
/// threaded from one batch to the next by construction. Not an
/// `Arc<AtomicUsize>`: shared mutable operator state is what makes an operator
/// un-reusable across executions, and nothing here needs to be shared.
///
/// # LIMIT is global, so it produces exactly one partition
///
/// The other half of the same bug: `execute` forwarded its `partition`
/// argument straight to a multi-partition child while declaring one partition
/// (the trait default). Only partition 0 was ever driven, so a LIMIT whose
/// matching rows all lived elsewhere returned nothing at all —
/// `SELECT l_orderkey FROM lineitem WHERE l_orderkey > 140000 LIMIT 100000`
/// returned **0** of 39,722 rows on data/tpch-100mb.
///
/// A LIMIT over a multi-partition input is only correct if it either runs on a
/// single partition or coordinates a global counter across them. This takes the
/// first: `output_partitions()` is 1 and `execute(0)` walks the input's
/// partitions in index order. Because the walk is lazy, LIMIT also gains an
/// early exit it never had — partition `p+1` is not opened at all once `fetch`
/// is satisfied. TPC-H never saw any of this: `planner.rs` fuses Sort+Limit
/// into `SortExec::with_fetch` whenever `skip == 0`, so 7 of its 8 LIMITs never
/// construct a `LimitExec`, and the eighth sits over a Sort that emits one
/// batch from one partition.
#[derive(Debug)]
pub struct LimitExec {
    input: Arc<dyn PhysicalOperator>,
    skip: usize,
    fetch: Option<usize>,
    schema: SchemaRef,
}

impl LimitExec {
    pub fn new(input: Arc<dyn PhysicalOperator>, skip: usize, fetch: Option<usize>) -> Self {
        let schema = input.schema();
        Self {
            input,
            skip,
            fetch,
            schema,
        }
    }
}

/// Everything the limit needs to carry from one batch to the next.
///
/// Held BY the stream rather than by the closure, so `skipped`/`fetched` are
/// the same values on every batch instead of a fresh copy per batch.
struct LimitState {
    input: Arc<dyn PhysicalOperator>,
    input_partitions: usize,
    /// Next input partition to open; `current` is the one being drained.
    next_partition: usize,
    current: Option<RecordBatchStream>,
    skip: usize,
    fetch: Option<usize>,
    skipped: usize,
    fetched: usize,
    /// Set after yielding an error so the stream terminates instead of
    /// re-polling a stream that already failed.
    failed: bool,
}

impl LimitState {
    /// True once `fetch` rows have been emitted.
    fn satisfied(&self) -> bool {
        matches!(self.fetch, Some(limit) if self.fetched >= limit)
    }

    /// Apply OFFSET then LIMIT to one batch, updating the counters.
    /// Returns `None` when the batch contributes no output rows.
    fn take_from(&mut self, batch: RecordBatch) -> Option<RecordBatch> {
        let num_rows = batch.num_rows();

        let batch = if self.skipped < self.skip {
            let to_skip = (self.skip - self.skipped).min(num_rows);
            self.skipped += to_skip;
            if to_skip == num_rows {
                return None;
            }
            batch.slice(to_skip, num_rows - to_skip)
        } else {
            batch
        };

        let available = batch.num_rows();
        let emit = match self.fetch {
            Some(limit) => limit.saturating_sub(self.fetched).min(available),
            None => available,
        };
        if emit == 0 {
            return None;
        }
        self.fetched += emit;
        Some(if emit < available {
            batch.slice(0, emit)
        } else {
            batch
        })
    }
}

#[async_trait]
impl PhysicalOperator for LimitExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![self.input.clone()]
    }

    fn output_partitions(&self) -> usize {
        // LIMIT is global. Reporting the input's count would emit `fetch` rows
        // per partition; forwarding `partition` to a multi-partition child
        // while reporting 1 (what this did) drops every partition but 0.
        1
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        crate::physical::check_partition(self, partition)?;

        let state = LimitState {
            input_partitions: self.input.output_partitions().max(1),
            input: self.input.clone(),
            next_partition: 0,
            current: None,
            skip: self.skip,
            fetch: self.fetch,
            skipped: 0,
            fetched: 0,
            failed: false,
        };

        let limited = stream::unfold(state, |mut st| async move {
            loop {
                if st.failed || st.satisfied() {
                    return None;
                }

                // Open the next input partition when the current one is done.
                if st.current.is_none() {
                    if st.next_partition >= st.input_partitions {
                        return None;
                    }
                    let p = st.next_partition;
                    st.next_partition += 1;
                    match st.input.execute(p).await {
                        Ok(s) => st.current = Some(s),
                        Err(e) => {
                            st.failed = true;
                            return Some((Err(e), st));
                        }
                    }
                }

                match st
                    .current
                    .as_mut()
                    .expect("stream opened above")
                    .next()
                    .await
                {
                    None => {
                        st.current = None;
                    }
                    Some(Err(e)) => {
                        st.failed = true;
                        return Some((Err(e), st));
                    }
                    Some(Ok(batch)) => {
                        if let Some(out) = st.take_from(batch) {
                            return Some((Ok(out), st));
                        }
                    }
                }
            }
        });

        Ok(Box::pin(limited))
    }

    fn name(&self) -> &str {
        "Limit"
    }
}

impl fmt::Display for LimitExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Limit: skip={}, fetch={:?}", self.skip, self.fetch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical::MemoryTableExec;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;
    use std::sync::Arc;

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_limit_fetch() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));
        let limit = LimitExec::new(scan, 0, Some(3));

        let stream = limit.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);
    }

    #[tokio::test]
    async fn test_limit_skip() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));
        let limit = LimitExec::new(scan, 2, None);

        let stream = limit.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3); // 5 - 2 = 3
    }

    #[tokio::test]
    async fn test_limit_skip_and_fetch() {
        let batch = create_test_batch();
        let schema = batch.schema();

        let scan = Arc::new(MemoryTableExec::new("test", schema, vec![batch], None));
        let limit = LimitExec::new(scan, 1, Some(2));

        let stream = limit.execute(0).await.unwrap();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        // Should be values 2 and 3 (skip 1, fetch 2)
        let ids = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
        assert_eq!(ids.value(1), 3);
    }
}
