//! spill-join-correctness-2 epic, task 002: real, cgroup-verified proof that
//! `SpillableHashJoinExec`'s build side no longer needs to be fully
//! memory-resident before the spill decision runs.
//!
//! The OLD code (`collect_input_partitions_concurrently`, called
//! unconditionally as the first step of `SpillableHashJoinExec::execute`)
//! drained the ENTIRE build side into one flat `Vec<RecordBatch>` and only
//! THEN compared its total size against `memory_limit * spill_threshold` --
//! so a build side larger than the process's REAL available memory could
//! exhaust it during that initial collection, before the spill decision (or
//! anything it would have triggered) ever ran. This is independent of the
//! configured `memory_limit`: the old code paid the same "collect
//! everything first" cost whether the eventual decision was "fits" or
//! "spill."
//!
//! This binary builds a `SpillableHashJoinExec` whose build (left) side is a
//! LAZY, streaming synthetic generator (`LazyGeneratorExec` below) --
//! nothing is materialized until `execute()` actually pulls a batch -- that
//! totals several GB, several times this process's own cgroup memory cap.
//! The probe (right) side is a handful of in-memory rows. Run this binary
//! itself under a REAL cgroup `MemoryMax` (via `systemd-run --scope`, the
//! same mechanism `scripts/claude-safe-build.sh` uses) tighter than the
//! synthetic build side's total size but comfortably above what a bounded,
//! incremental collection needs:
//!
//! ```text
//! scripts/claude-safe-build.sh cargo build --release --example spill_join_oom_repro
//! systemd-run --user --scope --quiet --collect --unit="sjoom-$$" \
//!     -p MemoryMax=900M -p MemorySwapMax=0 -p ManagedOOMMemoryPressure=kill \
//!     -- ./target/release/examples/spill_join_oom_repro
//! echo "exit=$?"   # 0 = completed cleanly; 137 (128+9) = OOM-killed
//! ```
//!
//! Prints `RESULT: PASS` and exits 0 on a clean, correct completion; a
//! process that gets OOM-killed by the cgroup never gets to print anything
//! at all (SIGKILL), which is itself the (negative) signal this is checking
//! for when run against the pre-fix code.

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::stream::TryStreamExt;
use query_engine::execution::create_memory_pool;
use query_engine::physical::operators::{MemoryTableExec, SpillableHashJoinExec};
use query_engine::physical::{PhysicalOperator, RecordBatchStream};
use query_engine::planner::{Expr, JoinType};
use query_engine::ExecutionConfig;
use std::sync::Arc;

/// Total synthetic build-side rows. With `PAD_LEN` = 1024 bytes/row plus a
/// few bytes of Int64/offset overhead, ~3,200,000 rows is ~3.3GB total --
/// several times the cgroup cap this is meant to be run under, and far more
/// than `MEMORY_LIMIT` below.
const TOTAL_ROWS: i64 = 3_200_000;
/// Rows generated per lazily-produced batch.
const BATCH_ROWS: i64 = 4096;
/// Bytes of Utf8 padding per row (the bulk of each row's size).
const PAD_LEN: usize = 1024;

/// The engine's own configured memory budget for this join. Deliberately far
/// below the ~3.3GB build side, so the join MUST decide to spill -- the
/// question this binary answers is whether reaching that decision itself
/// requires more memory than the budget (the old bug) or stays bounded
/// (the fix).
const MEMORY_LIMIT: usize = 500 * 1024 * 1024; // 500MB

/// A build/probe side that generates its `RecordBatch`es LAZILY, on demand,
/// as its stream is polled -- nothing beyond the current batch is ever
/// materialized ahead of time. This is deliberate: if this example
/// pre-built a `Vec<RecordBatch>` before handing it to the join, THAT
/// collection step (not `SpillableHashJoinExec`'s own code) would be what
/// determines this process's peak memory, defeating the point of the test.
#[derive(Debug)]
struct LazyGeneratorExec {
    schema: SchemaRef,
    total_rows: i64,
    batch_rows: i64,
    pad_len: usize,
}

fn make_batch(schema: &SchemaRef, start: i64, n: i64, pad_len: usize) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n).collect();
    let pad = "x".repeat(pad_len);
    let strs: Vec<&str> = std::iter::repeat(pad.as_str()).take(n as usize).collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(StringArray::from(strs)),
        ],
    )
    .expect("build synthetic batch")
}

#[async_trait::async_trait]
impl PhysicalOperator for LazyGeneratorExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }

    fn output_partitions(&self) -> usize {
        1
    }

    fn name(&self) -> &str {
        "LazyGenerator"
    }

    async fn execute(&self, partition: usize) -> query_engine::Result<RecordBatchStream> {
        if partition != 0 {
            return Ok(Box::pin(futures::stream::empty()));
        }
        let schema = self.schema.clone();
        let total_rows = self.total_rows;
        let batch_rows = self.batch_rows;
        let pad_len = self.pad_len;
        let stream = futures::stream::unfold(0i64, move |emitted| {
            let schema = schema.clone();
            async move {
                if emitted >= total_rows {
                    return None;
                }
                let n = batch_rows.min(total_rows - emitted);
                let batch = make_batch(&schema, emitted, n, pad_len);
                Some((Ok(batch), emitted + n))
            }
        });
        Ok(Box::pin(stream))
    }
}

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    let peak_mb = || -> Option<u64> {
        let status = std::fs::read_to_string("/proc/self/status").ok()?;
        for line in status.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                return rest
                    .trim()
                    .trim_end_matches(" kB")
                    .trim()
                    .parse::<u64>()
                    .ok()
                    .map(|kb| kb / 1024);
            }
        }
        None
    };

    println!(
        "=== spill_join_oom_repro: build side ~{:.2}GB ({} rows x ~{}B), memory_limit={}MB ===",
        (TOTAL_ROWS as f64 * (PAD_LEN + 16) as f64) / (1024.0 * 1024.0 * 1024.0),
        TOTAL_ROWS,
        PAD_LEN + 16,
        MEMORY_LIMIT / (1024 * 1024),
    );

    let build_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("pad", DataType::Utf8, false),
    ]));
    let build_side: Arc<dyn PhysicalOperator> = Arc::new(LazyGeneratorExec {
        schema: build_schema,
        total_rows: TOTAL_ROWS,
        batch_rows: BATCH_ROWS,
        pad_len: PAD_LEN,
    });

    // Tiny in-memory probe side: three keys that DO exist in the generated
    // build side (0, mid-range, last row) and one that deliberately does
    // not (out of range) -- a real, checkable correctness assertion below,
    // not just "didn't crash."
    let probe_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "probe_id",
        DataType::Int64,
        false,
    )]));
    let probe_ids = vec![0i64, TOTAL_ROWS / 2, TOTAL_ROWS - 1, 999_999_999i64];
    let probe_batch = RecordBatch::try_new(
        probe_schema.clone(),
        vec![Arc::new(Int64Array::from(probe_ids))],
    )
    .expect("build probe batch");
    let probe_side: Arc<dyn PhysicalOperator> = Arc::new(MemoryTableExec::new(
        "probe",
        probe_schema,
        vec![probe_batch],
        None,
    ));

    let pool = create_memory_pool(MEMORY_LIMIT);
    let spill_dir =
        std::env::temp_dir().join(format!("qe_spill_join_oom_repro_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&spill_dir);
    let config = ExecutionConfig::new()
        .with_memory_limit(MEMORY_LIMIT)
        .with_spill_path(spill_dir.clone());

    let join = SpillableHashJoinExec::new(
        build_side,
        probe_side,
        vec![(Expr::column("id"), Expr::column("probe_id"))],
        JoinType::Inner,
        pool,
        config,
    );

    let mut stream = join.execute(0).await?;
    let mut total_matched = 0usize;
    while let Some(batch) = stream.try_next().await? {
        total_matched += batch.num_rows();
    }

    let _ = std::fs::remove_dir_all(&spill_dir);

    let expected = 3usize; // 0, TOTAL_ROWS/2, TOTAL_ROWS-1 match; 999_999_999 does not.
    let correct = total_matched == expected;
    println!(
        "matched_rows={total_matched} expected={expected} peak_rss_mb={:?}",
        peak_mb()
    );
    println!(
        "RESULT: {}",
        if correct {
            "PASS"
        } else {
            "FAIL (wrong row count)"
        }
    );
    if !correct {
        std::process::exit(1);
    }
    Ok(())
}
