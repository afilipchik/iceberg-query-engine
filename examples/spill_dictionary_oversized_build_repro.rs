//! `spill-size-estimate-fix` epic, task 001: the safety-critical stress
//! case the task's own acceptance criteria require -- proof that fixing
//! `estimate_batch_size`'s Dictionary-column over-counting bug did NOT
//! introduce the opposite, far more dangerous failure mode: silently
//! UNDER-counting a genuinely oversized Dictionary-heavy build side and
//! letting `SpillableHashJoinExec` try to hold it all in memory (an OOM
//! risk this program's "memory safety is never negotiable" rule exists to
//! prevent).
//!
//! Modeled directly on `examples/spill_join_oom_repro.rs` (same
//! `LazyGeneratorExec` streaming-build-side shape, same
//! `MEMORY_LIMIT`/cgroup-cap pattern), but where THAT repro's build side is
//! dominated by a wide `Utf8` "pad" column (the ALREADY-correct branch this
//! function's own doc comment credits as the template), this one's build
//! side is dominated ENTIRELY by a `Dictionary(Int32, Utf8)` column with a
//! small (7-entry) values pool -- so unlike the `Utf8` pad column, the
//! DOMINANT contributor to this build side's true size is exactly the
//! branch this task fixed. If the fix under-counted (e.g. a bug that
//! collapsed a Dictionary column's size to ~0, mistaking "small dictionary
//! VALUES" for "small column"), the join would wrongly decide the whole
//! ~700MB+ build side "fits" and try to hold it in memory -- which the
//! tight cgroup cap below would catch as a real OOM kill, not a subtle
//! logic error.
//!
//! ```text
//! scripts/claude-safe-build.sh cargo build --release --example spill_dictionary_oversized_build_repro
//! systemd-run --user --scope --quiet --collect --unit="sjdict-$$" \
//!     -p MemoryMax=900M -p MemorySwapMax=0 -p ManagedOOMMemoryPressure=kill \
//!     -- ./target/release/examples/spill_dictionary_oversized_build_repro
//! echo "exit=$?"   # 0 = completed cleanly (correct spill); 137 (128+9) = OOM-killed
//! ```

use arrow::array::{DictionaryArray, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Int32Type, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::stream::TryStreamExt;
use query_engine::execution::create_memory_pool;
use query_engine::physical::operators::{MemoryTableExec, SpillableHashJoinExec};
use query_engine::physical::{PhysicalOperator, RecordBatchStream};
use query_engine::planner::{Expr, JoinType};
use query_engine::ExecutionConfig;
use std::sync::Arc;

/// Total synthetic build-side rows. Row width is dominated by the
/// Dictionary column's keys (4 bytes/row) plus the `id` column (8
/// bytes/row) -- ~12 bytes/row logical content -- so 60,000,000 rows is
/// ~720MB, comfortably several times `MEMORY_LIMIT` below and the cgroup
/// cap this is meant to be run under.
const TOTAL_ROWS: i64 = 60_000_000;
/// Rows generated per lazily-produced batch.
const BATCH_ROWS: i64 = 131_072;

/// The engine's own configured memory budget for this join. Deliberately
/// far below the ~720MB build side, so the join MUST decide to spill --
/// and deliberately SMALL relative to `TOTAL_ROWS` (unlike
/// `spill_join_oom_repro.rs`'s 500MB/3.3GB ~15% ratio, this uses a ~4%
/// ratio) so the crossing point lands early and most of the build side
/// flows through the genuinely-streaming `rest` stream in
/// `finish_via_spill` rather than sitting pre-buffered in `flat_batches`
/// -- otherwise, for this repro's much narrower (~12B) rows, the SAME
/// absolute row count buffered pre-crossing is a much larger FRACTION of
/// the total than in the wide-row (~1040B) original, which inflates the
/// partitioning step's own transient memory need (a pre-existing
/// characteristic of `build_with_partitioning`'s per-partition-buffer
/// mechanics, independent of this task's own Dictionary-sizing fix).
const MEMORY_LIMIT: usize = 30 * 1024 * 1024; // 30MB

/// Q12's own real l_shipmode-shaped low-cardinality value pool, reused here
/// deliberately (not an arbitrary string set) so this repro's Dictionary
/// column mirrors the exact shape that originally triggered this epic.
const SHIPMODES: [&str; 7] = ["MAIL", "SHIP", "AIR", "RAIL", "TRUCK", "FOB", "REG AIR"];

/// A build/probe side that generates its `RecordBatch`es LAZILY, on demand
/// -- see `spill_join_oom_repro.rs`'s own doc comment for why this matters:
/// a pre-built `Vec<RecordBatch>` would make ITS collection, not
/// `SpillableHashJoinExec`'s own code, determine this process's peak
/// memory, defeating the point of the test.
#[derive(Debug)]
struct LazyGeneratorExec {
    schema: SchemaRef,
    total_rows: i64,
    batch_rows: i64,
}

fn make_batch(schema: &SchemaRef, start: i64, n: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n).collect();
    let values = StringArray::from(SHIPMODES.to_vec());
    let keys: Vec<i32> = (0..n).map(|i| ((start + i) % 7) as i32).collect();
    let keys = Int32Array::from(keys);
    let dict = DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values))
        .expect("build dictionary array");
    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(ids)), Arc::new(dict)],
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
        let stream = futures::stream::unfold(0i64, move |emitted| {
            let schema = schema.clone();
            async move {
                if emitted >= total_rows {
                    return None;
                }
                let n = batch_rows.min(total_rows - emitted);
                let batch = make_batch(&schema, emitted, n);
                Some((Ok(batch), emitted + n))
            }
        });
        Ok(Box::pin(stream))
    }
}

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    // Trace the spill decision itself (compute_build_decision's own
    // QE_SPILL_DEBUG-gated eprintln) so this binary's own stdout/stderr
    // directly confirms a spill actually happened, not just that the
    // process survived.
    std::env::set_var("QE_SPILL_DEBUG", "1");

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
        "=== spill_dictionary_oversized_build_repro: build side ~{:.2}GB \
         ({} rows x ~12B, dominated by a Dictionary(Int32,Utf8) column), \
         memory_limit={}MB ===",
        (TOTAL_ROWS as f64 * 12.0) / (1024.0 * 1024.0 * 1024.0),
        TOTAL_ROWS,
        MEMORY_LIMIT / (1024 * 1024),
    );

    let build_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "shipmode",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
    ]));
    let build_side: Arc<dyn PhysicalOperator> = Arc::new(LazyGeneratorExec {
        schema: build_schema,
        total_rows: TOTAL_ROWS,
        batch_rows: BATCH_ROWS,
    });

    // Tiny in-memory probe side: three keys that DO exist in the generated
    // build side (0, mid-range, last row) and one that deliberately does
    // not (out of range) -- a real, checkable correctness assertion, not
    // just "didn't crash."
    let probe_schema: SchemaRef = Arc::new(Schema::new(vec![Field::new(
        "probe_id",
        DataType::Int64,
        false,
    )]));
    let probe_ids = vec![0i64, TOTAL_ROWS / 2, TOTAL_ROWS - 1, 999_999_999_999i64];
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
    let spill_dir = std::env::temp_dir().join(format!(
        "qe_spill_dictionary_oversized_build_repro_{}",
        std::process::id()
    ));
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

    let expected = 3usize; // 0, TOTAL_ROWS/2, TOTAL_ROWS-1 match; 999_999_999_999 does not.
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
    query_engine::execution::alloc_profile::print_peak_snapshot(15);
    if !correct {
        std::process::exit(1);
    }
    Ok(())
}
