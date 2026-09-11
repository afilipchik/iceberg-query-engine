//! `oom-safety-hardening` epic, task 001: the ONE reusable adversarial
//! memory-cap harness (PRD G6). Four scenarios — one per OOM-shaped gap the
//! epic exists to close — each runnable under BOTH cap levers:
//!
//!   1. `agg`         — large GROUP BY through `SpillableHashAggregateExec`'s
//!                      collect-then-decide path (a `COUNT(DISTINCT ...)`
//!                      aggregate is used deliberately: `distinct` makes the
//!                      shape fused-streaming-INELIGIBLE, so it goes straight
//!                      to `collect_input_partitions_concurrently`, the exact
//!                      pre-fix hole named by the PRD). Pre-fix expectation:
//!                      kernel kill / abort under a cap smaller than the input.
//!   2. `sort`        — large `ORDER BY` through `ExternalSortExec` (same
//!                      collect-then-decide hole). Same pre-fix expectation.
//!   3. `native-scan` — a native-table scan whose segments exceed
//!                      `check_scan_budget`'s admission threshold, feeding an
//!                      aggregate. Pre-fix expectation: CLEAN NAMED REFUSAL
//!                      (exit 2 — that is a PASS today and documents the
//!                      boundary; post-fix per PRD G2 this must COMPLETE by
//!                      spilling instead).
//!   4. `insert`      — `CREATE TABLE ... AS SELECT` from a large parquet
//!                      source at a 512MB-class cap (the documented residual
//!                      from `native-tables-mutation` task 005). Pre-fix
//!                      expectation: SIGKILL.
//!   5. `semi-join` / `anti-join` — `spill-join-correctness-3` task 004: a
//!                      SEMI (resp. ANTI) `SpillableHashJoinExec` whose BUILD
//!                      side is far above `memory_limit`, so the join must
//!                      take the spill path. Pre-fix expectation: CLEAN NAMED
//!                      REFUSAL ("SEMI join build side exceeds the memory
//!                      budget, but the join spill path currently supports
//!                      only INNER joins", exit 2 — the documented Q4@SF=100
//!                      gap); post-fix this must COMPLETE with the closed-form
//!                      row count below. Orientation via
//!                      `QE_HARNESS_JOIN_BUILD_RIGHT` (default 1: build = right,
//!                      probe = left = output; 0: build = left = output). The
//!                      probe side is sized to stay well under the cap because
//!                      the join spill path materializes the whole probe side
//!                      before probing (pre-existing, not this task's scope).
//!
//! Cap levers (driven by `scripts/oom_cap_harness.sh`, which wraps every run):
//!   - cgroup: `systemd-run --user --scope -p MemoryMax=<N>` — kernel
//!     memcg kill (exit 137).
//!   - rlimit: `QE_MEM_CAP=<N>` — this binary's FIRST statement is
//!     `enforce_process_memory_cap()` (the same in-binary `RLIMIT_DATA` cap
//!     `src/main.rs` applies), so the engine aborts at the cap (exit 134)
//!     with the terminal untouched. Examples do NOT inherit this from
//!     `main.rs`, which is why it is called here explicitly.
//!
//! Exit-code protocol (consumed by the shell driver):
//!   0   = scenario COMPLETED with a correct-looking result
//!   2   = scenario REFUSED cleanly (allowlisted resource diagnostic only)
//!   1   = wrong result / unexpected error class
//!   134 = abort at the rlimit cap (allocation failure) — a FAIL verdict
//!   137 = SIGKILL by the kernel/memcg — a FAIL verdict
//!
//! Env knobs:
//!   QE_HARNESS_PARTITIONS    positive generator partition count (default 1)
//!   QE_HARNESS_RESIDENT_INPUT 1 = immutable development fixture (max 2M rows/input); default lazy
//!   QE_HARNESS_ROWS          synthetic rows for agg/sort (default 250_000_000 — ~4GB raw, comfortably above BOTH default cap levers)
//!   QE_HARNESS_MEMORY_LIMIT  engine memory_limit bytes for agg/sort
//!                            (default 256MB — far below the input, so the
//!                            operator MUST decide to spill to survive)
//!   QE_HARNESS_NATIVE_TABLE  native table dir for `native-scan`
//!                            (default data/tpch-10gb-native/lineitem)
//!   QE_HARNESS_SCAN_LIMIT    memory_limit for `native-scan` (default 512MB)
//!   QE_HARNESS_PARQUET       parquet source for `insert`
//!                            (default data/tpch-10gb/lineitem.parquet)
//!   QE_HARNESS_CTAS_ROOT     native_table_root for `insert`
//!                            (default .scratch/oom001/ctas_root)
//!   QE_HARNESS_INSERT_LIMIT  engine memory_limit bytes for `insert`
//!                            (default 512MB, matching the shell driver's
//!                            512M cap — see scenario_insert's comment)
//!   QE_HARNESS_JOIN_BUILD_ROWS  build-side rows for `semi-join`/`anti-join`
//!                            (default 40_000_000 — ~640MB raw, 2.5x the
//!                            256MB default memory_limit). Build ids are
//!                            0..B; the probe side is B/2 rows with ids
//!                            [3B/4, 5B/4) so exactly half of it matches.
//!   QE_HARNESS_JOIN_BUILD_RIGHT  1 (default) = build right / emit probe
//!                            rows; 0 = build left / emit build rows.
//!   QE_HARNESS_FILTER        `filtered-join` ON predicate: eq (default,
//!                            keeps all B/4 matched pairs) or ne (keeps 0).
//!   (spill-boundaries task 004 added `left-join` and `filtered-join`,
//!   same fixture; pre-fix expectation: clean named refusal — outer-join
//!   spill / ON-filter spill — post-fix: COMPLETED with the closed-form
//!   counts documented at `scenario_join`.)

use arrow::array::{Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use futures::stream::TryStreamExt;
use query_engine::execution::create_memory_pool;
use query_engine::physical::operators::spillable::AggregateExpr;
use query_engine::physical::operators::{
    ExternalSortExec, SpillableHashAggregateExec, SpillableHashJoinExec,
};
use query_engine::physical::queue_layout::QueueCopyBound;
use query_engine::physical::{PhysicalOperator, RecordBatchStream};
use query_engine::planner::{AggregateFunction, BinaryOp, Expr, JoinType, SortExpr};
use query_engine::{ExecutionConfig, ExecutionContext};
use std::sync::Arc;

const BATCH_ROWS: i64 = 131_072;

const VALUE_MODULUS: i64 = 1_000_003;
const VALUE_MULTIPLIER: i64 = 2_654_435_761;

fn harness_partitions() -> query_engine::Result<usize> {
    let value = std::env::var("QE_HARNESS_PARTITIONS").unwrap_or_else(|_| "1".into());
    value
        .parse::<usize>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| {
            query_engine::QueryError::InvalidArgument(
                "QE_HARNESS_PARTITIONS must be a positive integer".into(),
            )
        })
}

fn partition_range(
    total: i64,
    offset: i64,
    partitions: usize,
    partition: usize,
) -> query_engine::Result<(i64, i64)> {
    if total < 0 || offset < 0 || partitions == 0 || partition >= partitions {
        return Err(query_engine::QueryError::InvalidArgument(
            "invalid generator range/partition".into(),
        ));
    }
    let end = offset.checked_add(total).ok_or_else(|| {
        query_engine::QueryError::InvalidArgument("generator ID range overflow".into())
    })?;
    if end > i64::MAX / VALUE_MULTIPLIER {
        return Err(query_engine::QueryError::InvalidArgument(
            "generator range exceeds exact nonoverflow multiplication domain".into(),
        ));
    }
    let first = (total as u128 * partition as u128 / partitions as u128) as i64;
    let last = (total as u128 * (partition as u128 + 1) / partitions as u128) as i64;
    Ok((offset + first, last - first))
}

fn wrong(message: impl Into<String>) -> query_engine::QueryError {
    query_engine::QueryError::Execution(format!("WRONG RESULT: {}", message.into()))
}

fn bitmap(bits: usize) -> query_engine::Result<Vec<u64>> {
    let words = bits
        .checked_add(63)
        .ok_or_else(|| wrong("validator bitmap size overflow"))?
        / 64;
    let mut values = Vec::new();
    values
        .try_reserve_exact(words)
        .map_err(|e| wrong(format!("validator bitmap allocation failed: {e}")))?;
    values.resize(words, 0);
    Ok(values)
}
fn mark_unique(seen: &mut [u64], id: usize) -> bool {
    let bit = 1u64 << (id % 64);
    let word = &mut seen[id / 64];
    let fresh = *word & bit == 0;
    *word |= bit;
    fresh
}
fn int_column(batch: &RecordBatch, index: usize) -> query_engine::Result<&Int64Array> {
    batch
        .columns()
        .get(index)
        .and_then(|a| a.as_any().downcast_ref())
        .ok_or_else(|| wrong("missing or non-Int64 output column"))
}

struct AggregateOracle {
    total: i64,
    inverse: i64,
    seen: Vec<u64>,
    groups: usize,
}
impl AggregateOracle {
    fn new(total: i64) -> query_engine::Result<Self> {
        partition_range(total, 0, 1, 0)?;
        // Extended Euclid independently inverts the generator's modular map.
        let (mut old_r, mut r) = (VALUE_MODULUS, VALUE_MULTIPLIER % VALUE_MODULUS);
        let (mut old_t, mut t) = (0i64, 1i64);
        while r != 0 {
            let q = old_r / r;
            (old_r, r) = (r, old_r - q * r);
            (old_t, t) = (t, old_t - q * t);
        }
        if old_r != 1 {
            return Err(wrong("generator modular map is not bijective"));
        }
        Ok(Self {
            total,
            inverse: old_t.rem_euclid(VALUE_MODULUS),
            seen: bitmap(VALUE_MODULUS as usize)?,
            groups: 0,
        })
    }
    fn accept(&mut self, batch: &RecordBatch) -> query_engine::Result<()> {
        if batch.num_columns() != 2 {
            return Err(wrong("expected exactly two output columns"));
        }
        let (keys, counts) = (int_column(batch, 0)?, int_column(batch, 1)?);
        for row in 0..batch.num_rows() {
            if keys.is_null(row) || counts.is_null(row) {
                return Err(wrong("NULL aggregate key/count"));
            }
            let key = keys.value(row);
            if !(0..VALUE_MODULUS).contains(&key) {
                return Err(wrong("aggregate key outside generator domain"));
            }
            let residue = ((key as i128 * self.inverse as i128) % VALUE_MODULUS as i128) as i64;
            let expected = if residue < self.total {
                1 + (self.total - 1 - residue) / VALUE_MODULUS
            } else {
                0
            };
            if expected == 0
                || counts.value(row) != expected
                || !mark_unique(&mut self.seen, key as usize)
            {
                return Err(wrong("aggregate key multiplicity/count mismatch"));
            }
            self.groups += 1;
        }
        Ok(())
    }
    fn finish(&self) -> query_engine::Result<()> {
        if self.groups != self.total.min(VALUE_MODULUS) as usize {
            return Err(wrong("missing aggregate groups"));
        }
        Ok(())
    }
}

struct SortOracle {
    total: i64,
    seen: Vec<u64>,
    rows: usize,
    last: i64,
}
impl SortOracle {
    fn new(total: i64) -> query_engine::Result<Self> {
        partition_range(total, 0, 1, 0)?;
        Ok(Self {
            total,
            seen: bitmap(total as usize)?,
            rows: 0,
            last: i64::MIN,
        })
    }
    fn accept(&mut self, batch: &RecordBatch) -> query_engine::Result<()> {
        if batch.num_columns() != 2 {
            return Err(wrong("expected exactly two output columns"));
        }
        let (ids, values) = (int_column(batch, 0)?, int_column(batch, 1)?);
        for row in 0..batch.num_rows() {
            if ids.is_null(row) || values.is_null(row) {
                return Err(wrong("NULL sorted ID/value"));
            }
            let (id, value) = (ids.value(row), values.value(row));
            // Reduce operands first: this oracle does not duplicate generator
            // wrapping multiplication and stays bounded below modulus squared.
            let expected =
                (id.rem_euclid(VALUE_MODULUS) * (VALUE_MULTIPLIER % VALUE_MODULUS)) % VALUE_MODULUS;
            if !(0..self.total).contains(&id)
                || value != expected
                || value < self.last
                || !mark_unique(&mut self.seen, id as usize)
            {
                return Err(wrong("sorted range/value/order/uniqueness mismatch"));
            }
            self.last = value;
            self.rows += 1;
        }
        Ok(())
    }
    fn finish(&self) -> query_engine::Result<()> {
        if self.rows != self.total as usize {
            return Err(wrong("missing sorted IDs"));
        }
        Ok(())
    }
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn env_str(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn peak_rss_mb() -> Option<u64> {
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
}

/// Same lazily-generating source as `spill_join_oom_repro.rs` — a pre-built
/// `Vec<RecordBatch>` would make ITS collection determine peak memory, not
/// the operator under test.
#[derive(Debug)]
struct LazyGeneratorExec {
    schema: SchemaRef,
    total_rows: i64,
    /// First `id` emitted (ids are `id_offset .. id_offset + total_rows`).
    /// 0 for every scenario except the join ones, whose probe side needs
    /// a key range that only partially overlaps the build side.
    id_offset: i64,
    partitions: usize,
}

fn make_batch(schema: &SchemaRef, start: i64, n: i64) -> RecordBatch {
    let ids: Vec<i64> = (start..start + n).collect();
    // `val` cycles through a large-but-bounded space so COUNT(DISTINCT val)
    // and ORDER BY val both do real work without degenerate all-equal keys.
    let vals: Vec<i64> = (0..n)
        .map(|i| (start + i).wrapping_mul(2654435761) % 1_000_003)
        .collect();
    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(vals)),
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
        self.partitions
    }
    fn name(&self) -> &str {
        "LazyGenerator"
    }
    async fn execute(&self, partition: usize) -> query_engine::Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, partition)?;
        let (id_offset, total_rows) =
            partition_range(self.total_rows, self.id_offset, self.partitions, partition)?;
        let schema = self.schema.clone();
        let stream = futures::stream::unfold(0i64, move |emitted| {
            let schema = schema.clone();
            async move {
                if emitted >= total_rows {
                    return None;
                }
                let n = BATCH_ROWS.min(total_rows - emitted);
                Some((Ok(make_batch(&schema, id_offset + emitted, n)), emitted + n))
            }
        });
        Ok(Box::pin(stream))
    }
}

// This opt-in fixture is resident before execution. Its allocations are subject
// to the process/cgroup cap, not admitted by the query pool. The capability only
// covers copied queue output and absence of same-pool dependencies while pulling.
const MAX_RESIDENT_FIXTURE_ROWS: i64 = 2_000_000;

#[derive(Debug)]
struct ResidentGeneratorExec {
    schema: SchemaRef,
    batches: Arc<Vec<RecordBatch>>,
    partition_batches: Vec<std::ops::Range<usize>>,
    bound: QueueCopyBound,
    fixture_array_bytes: usize,
}

impl ResidentGeneratorExec {
    fn new(generator: LazyGeneratorExec) -> query_engine::Result<Self> {
        partition_range(
            generator.total_rows,
            generator.id_offset,
            generator.partitions,
            0,
        )?;
        if generator.total_rows > MAX_RESIDENT_FIXTURE_ROWS {
            return Err(query_engine::QueryError::Execution(format!(
                "resident harness fixture requires explicit rows <= {MAX_RESIDENT_FIXTURE_ROWS} per input"
            )));
        }
        let mut batches = Vec::new();
        let mut partition_batches = Vec::new();
        partition_batches
            .try_reserve_exact(generator.partitions)
            .map_err(|e| {
                query_engine::QueryError::Execution(format!(
                    "resident fixture partition metadata: {e}"
                ))
            })?;
        let mut fixture_array_bytes = 0usize;
        for partition in 0..generator.partitions {
            let (offset, rows) = partition_range(
                generator.total_rows,
                generator.id_offset,
                generator.partitions,
                partition,
            )?;
            let first = batches.len();
            let mut emitted = 0;
            while emitted < rows {
                let n = BATCH_ROWS.min(rows - emitted);
                let batch = make_batch(&generator.schema, offset + emitted, n);
                for array in batch.columns() {
                    fixture_array_bytes = fixture_array_bytes
                        .checked_add(array.get_array_memory_size())
                        .ok_or_else(|| {
                            query_engine::QueryError::Execution(
                                "resident fixture byte count overflow".into(),
                            )
                        })?;
                }
                batches.push(batch);
                emitted += n;
            }
            partition_batches.push(first..batches.len());
        }
        let bound = QueueCopyBound::from_batches(&generator.schema, &batches).ok_or_else(|| {
            query_engine::QueryError::Execution("resident fixture copy layout unsupported".into())
        })?;
        Ok(Self {
            schema: generator.schema,
            batches: Arc::new(batches),
            partition_batches,
            bound,
            fixture_array_bytes,
        })
    }
}

#[async_trait::async_trait]
impl PhysicalOperator for ResidentGeneratorExec {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }
    fn output_partitions(&self) -> usize {
        self.partition_batches.len()
    }
    fn name(&self) -> &str {
        "ResidentHarnessFixture"
    }
    fn resident_queue_copy_bound(&self) -> Option<QueueCopyBound> {
        Some(self.bound.clone())
    }
    async fn execute(&self, partition: usize) -> query_engine::Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, partition)?;
        let batches = self.batches.clone();
        let range = self.partition_batches[partition].clone();
        // Clone batch metadata only when requested, keeping exactly the declared
        // partition ranges, including empty partitions. No query-pool calls.
        Ok(Box::pin(futures::stream::iter(
            range.map(move |index| Ok(batches[index].clone())),
        )))
    }
}

fn generator_input(
    generator: LazyGeneratorExec,
    label: &str,
) -> query_engine::Result<Arc<dyn PhysicalOperator>> {
    match std::env::var("QE_HARNESS_RESIDENT_INPUT").as_deref() {
        Ok("1") => {
            let rows = generator.total_rows;
            let fixture = ResidentGeneratorExec::new(generator)?;
            eprintln!(
                "resident_input={label} rows={rows} fixture_array_bytes={} copied_queue_bound_bytes={} input_partitions={} rayon_threads={} envelope_selection=not_observed",
                fixture.fixture_array_bytes, fixture.bound.max_bytes().expect("validated bound"),
                fixture.output_partitions(), rayon::current_num_threads(),
            );
            Ok(Arc::new(fixture))
        }
        Err(std::env::VarError::NotPresent) | Ok("0") => Ok(Arc::new(generator)),
        _ => Err(query_engine::QueryError::Execution(
            "QE_HARNESS_RESIDENT_INPUT must be 0 or 1".into(),
        )),
    }
}

fn gen_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("val", DataType::Int64, false),
    ]))
}

fn spill_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("qe_oom_cap_harness_{}_{}", tag, std::process::id()))
}

async fn scenario_agg() -> query_engine::Result<String> {
    let total_rows = env_usize("QE_HARNESS_ROWS", 250_000_000) as i64;
    let memory_limit = env_usize("QE_HARNESS_MEMORY_LIMIT", 256 * 1024 * 1024);
    let input = generator_input(
        LazyGeneratorExec {
            schema: gen_schema(),
            total_rows,
            id_offset: 0,
            partitions: harness_partitions()?,
        },
        "agg",
    )?;
    // GROUP BY val (1,000,003 distinct groups), COUNT(DISTINCT id).
    // `distinct: true` makes this fused-streaming-INELIGIBLE by
    // construction (`fused_streaming_eligible` requires `!a.distinct`), so
    // execution goes straight to the collect-then-decide path under test.
    let out_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("val", DataType::Int64, false),
        Field::new("cnt", DataType::Int64, true),
    ]));
    let sd = spill_dir("agg");
    let _ = std::fs::remove_dir_all(&sd);
    let config = ExecutionConfig::new()
        .with_memory_limit(memory_limit)
        .with_spill_path(sd.clone());
    let pool = create_memory_pool(memory_limit);
    let agg = SpillableHashAggregateExec::new(
        input,
        vec![Expr::column("val")],
        vec![AggregateExpr {
            func: AggregateFunction::CountDistinct,
            input: Expr::column("id"),
            distinct: true,
            second_arg: None,
        }],
        out_schema,
        pool.clone(),
        config,
    );
    let mut oracle = AggregateOracle::new(total_rows)?;
    for partition in 0..agg.output_partitions() {
        let mut stream = agg.execute(partition).await?;
        while let Some(batch) = stream.try_next().await? {
            oracle.accept(&batch)?;
        }
    }
    oracle.finish()?;
    eprintln!(
        "query_reserved_peak_bytes={} envelope_selection=not_observed",
        pool.reserved_peak()
    );
    let _ = std::fs::remove_dir_all(&sd);
    Ok(format!("groups={} exact_counts=true input_partitions={} output_partitions={} spilled={} spill_accounted_bytes={} validator_bytes={}", oracle.groups, harness_partitions()?, agg.output_partitions(), pool.spilled() > 0, pool.spilled(), oracle.seen.len() * 8))
}

async fn scenario_sort() -> query_engine::Result<String> {
    let total_rows = env_usize("QE_HARNESS_ROWS", 250_000_000) as i64;
    let memory_limit = env_usize("QE_HARNESS_MEMORY_LIMIT", 256 * 1024 * 1024);
    let input = generator_input(
        LazyGeneratorExec {
            schema: gen_schema(),
            total_rows,
            id_offset: 0,
            partitions: harness_partitions()?,
        },
        "sort",
    )?;
    let sd = spill_dir("sort");
    let _ = std::fs::remove_dir_all(&sd);
    let config = ExecutionConfig::new()
        .with_memory_limit(memory_limit)
        .with_spill_path(sd.clone());
    let pool = create_memory_pool(memory_limit);
    let sort = ExternalSortExec::new(
        input,
        vec![SortExpr::new(Expr::column("val")).asc()],
        pool.clone(),
        config,
    );
    let mut oracle = SortOracle::new(total_rows)?;
    for partition in 0..sort.output_partitions() {
        let mut stream = sort.execute(partition).await?;
        while let Some(batch) = stream.try_next().await? {
            oracle.accept(&batch)?;
        }
    }
    oracle.finish()?;
    eprintln!(
        "query_reserved_peak_bytes={} envelope_selection=not_observed",
        pool.reserved_peak()
    );
    let _ = std::fs::remove_dir_all(&sd);
    Ok(format!("rows={} exact_ids_values=true globally_ordered=true input_partitions={} output_partitions={} spilled={} spill_accounted_bytes={} validator_bytes={}", oracle.rows, harness_partitions()?, sort.output_partitions(), pool.spilled() > 0, pool.spilled(), oracle.seen.len() * 8))
}

async fn scenario_native_scan() -> query_engine::Result<String> {
    let table_dir = env_str("QE_HARNESS_NATIVE_TABLE", "data/tpch-10gb-native/lineitem");
    if !std::path::Path::new(&table_dir)
        .join("_manifest.json")
        .exists()
    {
        return Err(query_engine::QueryError::Execution(format!(
            "SKIP: native table dir {table_dir} not found"
        )));
    }
    let memory_limit = env_usize("QE_HARNESS_SCAN_LIMIT", 512 * 1024 * 1024);
    let config = ExecutionConfig::new().with_memory_limit(memory_limit);
    let mut ctx = ExecutionContext::with_config(config);
    ctx.register_native_table("lineitem", &table_dir)?;
    // QE_HARNESS_SCAN_SQL (task 004, additive): override the consumer shape
    // — e.g. an aggregate-over-JOIN — while keeping the default aggregate
    // query byte-identical to task 001's pre-fix evidence runs. The table
    // registered is always `lineitem` (self-joins cover the join shape).
    let sql = env_str(
        "QE_HARNESS_SCAN_SQL",
        "SELECT l_returnflag, COUNT(*) AS c FROM lineitem GROUP BY l_returnflag",
    );
    let result = ctx.sql(&sql).await?;
    Ok(format!("completed: {} group rows", result.row_count))
}

async fn scenario_insert() -> query_engine::Result<String> {
    let parquet = env_str("QE_HARNESS_PARQUET", "data/tpch-10gb/lineitem.parquet");
    if !std::path::Path::new(&parquet).exists() {
        return Err(query_engine::QueryError::Execution(format!(
            "SKIP: parquet source {parquet} not found"
        )));
    }
    let root = env_str("QE_HARNESS_CTAS_ROOT", ".scratch/oom001/ctas_root");
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).ok();
    // Engine memory_limit for the CTAS (default 512MB, matching the shell
    // driver's 512M cap — override via QE_HARNESS_INSERT_LIMIT, bytes).
    //
    // History: task 001's PRE-FIX evidence runs used a deliberately
    // GENEROUS 8GB here, because the gap under test was that the
    // CTAS/INSERT write path had NO admission check consulting the limit
    // at all (and native-tables-mutation task 005 had already PROVEN the
    // limit had zero effect on this path — 1GB limit, 5.4GB actual use).
    // Post-fix (oom-safety-hardening task 005's named pre-flight
    // admission check in `ExecutionContext::check_insert_write_admission`)
    // the limit is exactly what drives the clean refusal, so the harness
    // now models a consistently-configured engine: limit == external cap.
    let memory_limit = env_usize("QE_HARNESS_INSERT_LIMIT", 512 * 1024 * 1024);
    let config = ExecutionConfig::new().with_memory_limit(memory_limit);
    let mut ctx = ExecutionContext::with_config(config).with_native_table_root(&root);
    ctx.register_parquet("lineitem_src", &parquet)?;
    let res = ctx
        .create_table_as_select("CREATE TABLE oom_harness_ctas AS SELECT * FROM lineitem_src")
        .await?;
    let rows = res.rows;
    let _ = std::fs::remove_dir_all(&root);
    Ok(format!("CTAS completed: {rows} rows written"))
}

/// `spill-join-correctness-3` task 004: SEMI/ANTI join whose build side
/// exceeds the budget. Build ids 0..B; probe ids [3B/4, 5B/4) (B/2 rows),
/// so exactly B/4 probe rows match and B/4 do not, and on the build side
/// exactly B/4 build rows are matched and 3B/4 are not. Closed-form
/// expectation per orientation:
///   build_right=1 (build = right, probe = LEFT = output):
///       SEMI emits B/4 probe rows, ANTI emits B/4 probe rows.
///   build_right=0 (build = LEFT = output):
///       SEMI emits B/4 build rows, ANTI emits 3B/4 build rows.
/// `spill-boundaries` task 004 extends the same fixture to LEFT (the
/// preserved side is the LEFT input, whichever side builds) and to a
/// filtered INNER join (`QE_HARNESS_FILTER=eq|ne`, default `eq`: the ON
/// predicate `val = pval` compares the two sides' payloads, which are
/// equal for every matched pair — `eq` keeps all B/4 pairs, `ne` keeps 0;
/// both exercise the compiled-filter path on the spill path):
///   left-join, build_right=1 (left = probe): every probe row is emitted,
///       matched or NULL-extended → B/2 rows.
///   left-join, build_right=0 (left = build): every build row → B rows.
///   filtered-join (INNER, filter eq): B/4 rows; (ne): 0 rows.
/// The probe side carries distinct column names (`pid`, `pval`) so the
/// ON filter can name both sides unambiguously.
async fn scenario_join(join_type: JoinType) -> query_engine::Result<String> {
    let build_rows = env_usize("QE_HARNESS_JOIN_BUILD_ROWS", 40_000_000) as i64;
    partition_range(build_rows, 0, 1, 0)?;
    let build_right = env_str("QE_HARNESS_JOIN_BUILD_RIGHT", "1") != "0";
    let memory_limit = env_usize("QE_HARNESS_MEMORY_LIMIT", 256 * 1024 * 1024);
    let filter_mode = env_str("QE_HARNESS_FILTER", "eq");
    let filtered = matches!(join_type, JoinType::Inner);
    let probe_rows = build_rows / 2;
    let probe_offset = build_rows * 3 / 4;
    let probe_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("pid", DataType::Int64, false),
        Field::new("pval", DataType::Int64, false),
    ]));
    let build = generator_input(
        LazyGeneratorExec {
            schema: gen_schema(),
            total_rows: build_rows,
            id_offset: 0,
            partitions: harness_partitions()?,
        },
        "join_build",
    )?;
    let probe = generator_input(
        LazyGeneratorExec {
            schema: probe_schema,
            total_rows: probe_rows,
            id_offset: probe_offset,
            partitions: harness_partitions()?,
        },
        "join_probe",
    )?;
    let (left, right, on) = if build_right {
        (
            probe,
            build,
            vec![(Expr::column("pid"), Expr::column("id"))],
        )
    } else {
        (
            build,
            probe,
            vec![(Expr::column("id"), Expr::column("pid"))],
        )
    };
    let tag = match join_type {
        JoinType::Semi => "semi_join",
        JoinType::Anti => "anti_join",
        JoinType::Left => "left_join",
        _ => "filtered_join",
    };
    let sd = spill_dir(tag);
    let _ = std::fs::remove_dir_all(&sd);
    let config = ExecutionConfig::new()
        .with_memory_limit(memory_limit)
        .with_spill_path(sd.clone());
    let filter = if filtered {
        let op = if filter_mode == "ne" {
            BinaryOp::NotEq
        } else {
            BinaryOp::Eq
        };
        Some(Expr::BinaryExpr {
            left: Box::new(Expr::column("val")),
            op,
            right: Box::new(Expr::column("pval")),
        })
    } else {
        None
    };
    let pool = create_memory_pool(memory_limit);
    let join = SpillableHashJoinExec::new(left, right, on, join_type, pool.clone(), config)
        .with_build_right(build_right)
        .with_filter(filter);
    let mut rows = 0usize;
    let output_partitions = join.output_partitions();
    for partition in 0..output_partitions {
        let mut stream = join.execute(partition).await?;
        while let Some(batch) = stream.try_next().await? {
            rows += batch.num_rows();
        }
    }
    drop(join);
    eprintln!(
        "query_reserved_peak_bytes={} envelope_selection=not_observed",
        pool.reserved_peak()
    );
    let _ = std::fs::remove_dir_all(&sd);
    let quarter = (build_rows / 4) as usize;
    let expected = match (join_type, build_right) {
        (JoinType::Semi, _) => quarter,
        (JoinType::Anti, true) => quarter,
        (JoinType::Anti, false) => 3 * quarter,
        (JoinType::Left, true) => 2 * quarter,
        (JoinType::Left, false) => 4 * quarter,
        (JoinType::Inner, _) => {
            if filter_mode == "ne" {
                0
            } else {
                quarter
            }
        }
        _ => unreachable!("scenario_join only runs SEMI/ANTI/LEFT/filtered INNER"),
    };
    if rows == expected {
        Ok(format!(
            "{join_type:?} join (build_right={build_right}) rows={rows} (expected {expected}) input_partitions={} output_partitions={output_partitions} spilled={} spill_accounted_bytes={}", harness_partitions()?, pool.spilled() > 0, pool.spilled()
        ))
    } else {
        Err(query_engine::QueryError::Execution(format!(
            "WRONG RESULT: {join_type:?} join (build_right={build_right}) rows={rows}, expected {expected}"
        )))
    }
}

/// Fail closed: recognize typed pool admission pressure and the one fixed
/// legacy join guard. Display text cannot prove a resource refusal.
fn resource_refusal(error: &query_engine::QueryError) -> Option<&'static str> {
    if error.is_memory_limit() {
        return Some("memory-pool");
    }
    match error.root() {
        query_engine::QueryError::Execution(message)
            if message == "HashJoin legacy candidate memory bound exceeded; bounded output support required" =>
        {
            Some("legacy-join-candidate")
        }
        _ => None,
    }
}

#[cfg(test)]
mod classifier_tests {
    use super::resource_refusal;
    use query_engine::{execution::MemoryPool, QueryError};

    #[test]
    fn real_owned_pool_denial_is_a_named_resource_refusal() {
        let pool = MemoryPool::new_named("cap-fixture", 16);
        let _held = pool.allocate(12).unwrap();
        let denied = pool.allocate(5).unwrap_err();
        assert_eq!(resource_refusal(&denied), Some("memory-pool"));
        assert_eq!(
            resource_refusal(&QueryError::Execution(
                "HashJoin legacy candidate memory bound exceeded; bounded output support required"
                    .into()
            )),
            Some("legacy-join-candidate")
        );
    }

    #[test]
    fn wrong_results_nonresource_errors_and_malformed_diagnostics_fail() {
        for error in [
            QueryError::Execution("WRONG RESULT: rows=1, expected 2".into()),
            QueryError::Execution("aggregate spill sub-partitioning routed rows incorrectly".into()),
            QueryError::Execution("Failed to create spill directory: permission denied".into()),
            QueryError::NotImplemented("memory limit exceeded".into()),
            QueryError::InvalidArgument("unknown scenario".into()),
            QueryError::Execution("unrelated memory budget problem".into()),
            QueryError::Execution("Memory limit exceeded in 'x': requested 1 additional bytes, used 0, limit 100".into()),
            QueryError::Execution("Memory limit exceeded in 'x': requested 2 additional bytes, used 0, limit 1 WRONG RESULT".into()),
            QueryError::Execution("Memory limit exceeded in 'x': requested many additional bytes, used 0, limit 1".into()),
        ] { assert_eq!(resource_refusal(&error), None, "{error}"); }
    }
}

fn main() {
    // FIRST statement, mirroring src/main.rs: the rlimit lever
    // (`QE_MEM_CAP`) only works if the example applies it itself.
    query_engine::execution::enforce_process_memory_cap();

    let scenario = std::env::args().nth(1).unwrap_or_default();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");
    let outcome: query_engine::Result<String> = rt.block_on(async {
        match scenario.as_str() {
            "agg" => scenario_agg().await,
            "sort" => scenario_sort().await,
            "native-scan" => scenario_native_scan().await,
            "insert" => scenario_insert().await,
            "semi-join" => scenario_join(JoinType::Semi).await,
            "anti-join" => scenario_join(JoinType::Anti).await,
            "left-join" => scenario_join(JoinType::Left).await,
            "filtered-join" => scenario_join(JoinType::Inner).await,
            other => Err(query_engine::QueryError::InvalidArgument(format!(
                "usage: oom_cap_harness <agg|sort|native-scan|insert|semi-join|anti-join|left-join|filtered-join> (got {other:?})"
            ))),
        }
    });

    match outcome {
        Ok(msg) => {
            println!(
                "HARNESS RESULT: COMPLETED scenario={scenario} {msg} peak_rss_mb={:?}",
                peak_rss_mb()
            );
            std::process::exit(0);
        }
        Err(e) => {
            if let Some(resource) = resource_refusal(&e) {
                println!(
                    "HARNESS RESULT: REFUSED scenario={scenario} resource={resource} error={e} peak_rss_mb={:?}",
                    peak_rss_mb()
                );
                std::process::exit(2);
            }
            println!(
                "HARNESS RESULT: FAILED scenario={scenario} error={e} peak_rss_mb={:?}",
                peak_rss_mb()
            );
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod partition_oracle_tests {
    use super::*;

    fn pair_batch(first: Vec<Option<i64>>, second: Vec<Option<i64>>) -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            (
                "a",
                Arc::new(Int64Array::from(first)) as arrow::array::ArrayRef,
            ),
            (
                "b",
                Arc::new(Int64Array::from(second)) as arrow::array::ArrayRef,
            ),
        ])
        .unwrap()
    }

    #[tokio::test]
    async fn generator_partitions_cover_exact_contiguous_ids_and_empty_ranges() {
        for (total, partitions) in [(10, 3), (2, 5), (0, 4)] {
            let generator = LazyGeneratorExec {
                schema: gen_schema(),
                total_rows: total,
                id_offset: 37,
                partitions,
            };
            let mut ids = Vec::new();
            for part in 0..partitions {
                let mut stream = generator.execute(part).await.unwrap();
                while let Some(batch) = stream.try_next().await.unwrap() {
                    let id = int_column(&batch, 0).unwrap();
                    let value = int_column(&batch, 1).unwrap();
                    for row in 0..batch.num_rows() {
                        ids.push(id.value(row));
                        assert_eq!(
                            value.value(row),
                            (id.value(row) % VALUE_MODULUS) * (VALUE_MULTIPLIER % VALUE_MODULUS)
                                % VALUE_MODULUS
                        );
                    }
                }
            }
            assert_eq!(ids, (37..37 + total).collect::<Vec<_>>());
            assert!(generator.execute(partitions).await.is_err());
        }
        assert!(partition_range(i64::MAX, 0, 1, 0).is_err());
        assert!(partition_range(1, 0, 0, 0).is_err());
    }

    #[test]
    fn aggregate_oracle_checks_counts_uniqueness_and_nulls() {
        let key = VALUE_MULTIPLIER % VALUE_MODULUS;
        let mut good = AggregateOracle::new(2).unwrap();
        good.accept(&pair_batch(
            vec![Some(key), Some(0)],
            vec![Some(1), Some(1)],
        ))
        .unwrap();
        good.finish().unwrap();
        for wrong_batch in [
            pair_batch(vec![Some(key), Some(0)], vec![Some(2), Some(1)]),
            pair_batch(vec![Some(0), Some(0)], vec![Some(1), Some(1)]),
            pair_batch(vec![None, Some(0)], vec![Some(1), Some(1)]),
            pair_batch(vec![Some(key), Some(0)], vec![None, Some(1)]),
        ] {
            assert!(AggregateOracle::new(2)
                .unwrap()
                .accept(&wrong_batch)
                .is_err());
        }
        let mut periodic = AggregateOracle::new(VALUE_MODULUS + 1).unwrap();
        periodic
            .accept(&pair_batch(vec![Some(0)], vec![Some(2)]))
            .unwrap();
        assert!(periodic.finish().is_err(), "missing groups must fail");
        AggregateOracle::new(0).unwrap().finish().unwrap();
    }

    #[test]
    fn sort_oracle_rejects_wrong_values_duplicates_range_and_nulls() {
        let key = VALUE_MULTIPLIER % VALUE_MODULUS;
        let mut good = SortOracle::new(2).unwrap();
        good.accept(&pair_batch(
            vec![Some(0), Some(1)],
            vec![Some(0), Some(key)],
        ))
        .unwrap();
        good.finish().unwrap();
        for bad in [
            pair_batch(vec![Some(0), Some(1)], vec![Some(0), Some(key + 1)]),
            pair_batch(vec![Some(0), Some(0)], vec![Some(0), Some(0)]),
            pair_batch(vec![Some(2)], vec![Some(0)]),
            pair_batch(vec![None], vec![Some(0)]),
            pair_batch(vec![Some(0)], vec![None]),
            pair_batch(vec![Some(1), Some(0)], vec![Some(key), Some(0)]),
        ] {
            assert!(SortOracle::new(2).unwrap().accept(&bad).is_err());
        }
        assert!(SortOracle::new(2).unwrap().finish().is_err());
        SortOracle::new(0).unwrap().finish().unwrap();
    }
}

#[cfg(test)]
mod resident_fixture_tests {
    use super::*;

    #[tokio::test]
    async fn resident_matches_lazy_exactly_with_fixed_partitions_and_valid_bound() {
        for (rows, partitions) in [(0, 4), (2, 4), (19, 4), (BATCH_ROWS * 4 + 11, 4)] {
            let make = || LazyGeneratorExec {
                schema: gen_schema(),
                total_rows: rows,
                id_offset: 37,
                partitions,
            };
            let lazy = make();
            let resident = ResidentGeneratorExec::new(make()).unwrap();
            assert_eq!(resident.output_partitions(), partitions);
            assert!(lazy.resident_queue_copy_bound().is_none());
            let bound = resident
                .resident_queue_copy_bound()
                .unwrap()
                .max_bytes()
                .unwrap();
            let mut seen = 0;
            for partition in 0..partitions {
                let mut expected = lazy.execute(partition).await.unwrap();
                let mut actual = resident.execute(partition).await.unwrap();
                loop {
                    match (
                        expected.try_next().await.unwrap(),
                        actual.try_next().await.unwrap(),
                    ) {
                        (Some(expected), Some(actual)) => {
                            assert_eq!(actual, expected);
                            for row in 0..actual.num_rows() {
                                let id = int_column(&actual, 0).unwrap().value(row);
                                let value = int_column(&actual, 1).unwrap().value(row);
                                assert_eq!(id, 37 + seen);
                                assert_eq!(
                                    value,
                                    (id % VALUE_MODULUS) * (VALUE_MULTIPLIER % VALUE_MODULUS)
                                        % VALUE_MODULUS
                                );
                                seen += 1;
                            }
                            assert!(
                                QueueCopyBound::from_batches(&actual.schema(), &[actual])
                                    .unwrap()
                                    .max_bytes()
                                    .unwrap()
                                    <= bound
                            );
                        }
                        (None, None) => break,
                        _ => panic!("partition batch boundary mismatch"),
                    }
                }
            }
            assert_eq!(seen, rows);
            assert!(resident.execute(partitions).await.is_err());
            assert!(resident.fixture_array_bytes >= rows as usize * 16);
        }
    }

    #[test]
    fn resident_fixture_rejects_accidental_large_or_invalid_preparation() {
        for (total_rows, partitions) in [(MAX_RESIDENT_FIXTURE_ROWS + 1, 4), (-1, 4), (1, 0)] {
            assert!(ResidentGeneratorExec::new(LazyGeneratorExec {
                schema: gen_schema(),
                total_rows,
                id_offset: 0,
                partitions,
            })
            .is_err());
        }
    }
}

#[cfg(test)]
mod shared_classifier_tests {
    #[test]
    fn shared_errors_keep_exact_refusal_grammar_without_broadening() {
        use query_engine::QueryError;
        use std::sync::Arc;
        let pool = query_engine::execution::create_memory_pool(1);
        let denied = pool.allocate(2).unwrap_err();
        assert_eq!(
            super::resource_refusal(&QueryError::Shared(Arc::new(denied))),
            Some("memory-pool")
        );
        for error in [
            QueryError::Execution("out of memory maybe".into()),
            QueryError::Type("Memory limit exceeded".into()),
        ] {
            assert_eq!(
                super::resource_refusal(&QueryError::Shared(Arc::new(error))),
                None
            );
        }
    }
}

#[cfg(test)]
mod typed_classifier_regressions {
    use super::resource_refusal;
    use query_engine::{error::PartitionPhase, execution::MemoryPool, QueryError};
    use std::sync::Arc;
    #[test]
    fn partition_refusal_preserves_type_and_rejects_matching_display_text() {
        let pool = MemoryPool::new_named("classification fixture", 16);
        let denial = pool.allocate(17).unwrap_err();
        let text = match &denial {
            QueryError::MemoryLimit { pool, requested, used, limit } =>
                format!("Memory limit exceeded in '{pool}': requested {requested} additional bytes, used {used}, limit {limit}"),
            other => panic!("expected actual pool denial: {other}"),
        };
        let lookalike = QueryError::Execution(text);
        assert_eq!(denial.to_string(), lookalike.to_string());
        assert_eq!(
            resource_refusal(&lookalike),
            None,
            "text cannot prove admission denial"
        );
        let nested = QueryError::Shared(Arc::new(QueryError::Partition {
            partition_id: 3,
            phase: PartitionPhase::Collection,
            source: Box::new(QueryError::Shared(Arc::new(denial))),
        }));
        assert_eq!(resource_refusal(&nested), Some("memory-pool"));
        let io = QueryError::Partition {
            partition_id: 1,
            phase: PartitionPhase::Execution,
            source: Box::new(QueryError::Io(std::io::Error::from(
                std::io::ErrorKind::OutOfMemory,
            ))),
        };
        assert_eq!(
            resource_refusal(&io),
            None,
            "allocator/IO error is not admitted pressure"
        );
    }
}
