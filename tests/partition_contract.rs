//! Partition-contract regression tests.
//!
//! # Why this file exists
//!
//! `PhysicalOperator::output_partitions()` is an advisory integer. A parent
//! drives `0..child.output_partitions()`; a child decides for itself how many
//! partitions it produces. Until the range guard landed, an out-of-range
//! `execute(partition)` returned an **empty stream** in every single
//! implementation, so a disagreement between the two was never an error and
//! always a silently wrong row count. Three shipped wrong-answer bugs came out
//! of exactly that hole:
//!
//! * `UnionExec` declared 1 partition and drained only partition 0 of each
//!   input — UNION ALL lost 96% of its rows at SF=0.1.
//! * `LimitExec` declared 1 partition while forwarding `partition` to a
//!   multi-partition child, so a filtered `LIMIT` returned 0 rows.
//! * `HashJoinExec` (already fixed) returned the *build* side's partition
//!   count for Left + `build_right`, so probe partitions were never requested.
//!
//! # Why these tests use a hand-written scan instead of `data/tpch-1mb`
//!
//! `data/tpch-1mb` is 6,000 rows in ONE 8,192-row batch. `MemoryTableExec`
//! reports `min(rayon::current_num_threads(), batches.len())` partitions and
//! only above 1,000 rows, so on that fixture EVERY plan is one partition of one
//! batch and none of the above is reachable. That is precisely why 850 green
//! tests never saw any of it.
//!
//! `FixedPartitionScan` below is multi-partition **by construction**, not by
//! luck of the machine's core count or the fixture's row count. Do not replace
//! it with a `MemoryTableExec` over a small table "for simplicity": that puts
//! the tests straight back into the blind spot they exist to cover. The
//! SQL-level tests at the bottom use `data/tpch-10mb` (60,000 lineitem rows =
//! 8 batches), the smallest committed fixture on which the bugs reproduce;
//! `tests/spill_tests.rs` already requires that directory.

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use futures::TryStreamExt;
use query_engine::error::Result;
use query_engine::execution::{MemoryPool, SharedMemoryPool};
use query_engine::physical::operators::hash_agg::AggregateExpr;
use query_engine::physical::operators::spillable::AggregateExpr as SpillAggregateExpr;
use query_engine::physical::{
    ExternalSortExec, FilterExec, HashAggregateExec, HashJoinExec, LimitExec, MemoryTableExec,
    PhysicalOperator, ProjectExec, RecordBatchStream, SortExec, SpillableHashAggregateExec,
    SpillableHashJoinExec, UnionExec,
};
use query_engine::planner::{AggregateFunction, Expr, JoinType, ScalarValue, SortExpr};
use query_engine::{ExecutionConfig, ExecutionContext};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// A scan that is multi-partition by construction
// ---------------------------------------------------------------------------

/// A leaf operator that reports a **fixed** partition count and serves a fixed
/// set of batches from each partition.
///
/// The point is determinism: `output_partitions()` here does not depend on
/// `rayon::current_num_threads()`, on the row count, or on any threshold. A
/// single-core CI box sees the same partitioning as a 32-core one, so a test
/// built on it cannot decay into a vacuous single-partition run.
#[derive(Debug)]
struct FixedPartitionScan {
    schema: SchemaRef,
    /// `batches[p]` is the list of batches served by partition `p`.
    batches: Vec<Vec<RecordBatch>>,
}

impl FixedPartitionScan {
    /// `partitions` partitions, each serving `batches_per_partition` batches of
    /// `rows_per_batch` rows. Values are globally unique and ascending so a
    /// test can tell "wrong rows" from "too few rows".
    fn new(partitions: usize, batches_per_partition: usize, rows_per_batch: usize) -> Self {
        assert!(partitions >= 2, "fixture must be multi-partition");
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let mut next: i64 = 0;
        let mut batches = Vec::with_capacity(partitions);
        for _ in 0..partitions {
            let mut part = Vec::with_capacity(batches_per_partition);
            for _ in 0..batches_per_partition {
                let values: Vec<i64> = (0..rows_per_batch as i64).map(|i| next + i).collect();
                next += rows_per_batch as i64;
                part.push(
                    RecordBatch::try_new(schema.clone(), vec![Arc::new(Int64Array::from(values))])
                        .unwrap(),
                );
            }
            batches.push(part);
        }
        Self { schema, batches }
    }

    fn total_rows(&self) -> usize {
        self.batches
            .iter()
            .flatten()
            .map(|b| b.num_rows())
            .sum::<usize>()
    }
}

#[async_trait]
impl PhysicalOperator for FixedPartitionScan {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>> {
        vec![]
    }

    fn output_partitions(&self) -> usize {
        self.batches.len()
    }

    async fn execute(&self, partition: usize) -> Result<RecordBatchStream> {
        query_engine::physical::check_partition(self, partition)?;
        // Unreachable past the guard. Written defensively so that if the guard
        // is ever weakened, this fixture reports it as a failed assertion in
        // `assert_rejects_out_of_range` rather than as an index panic.
        let batches = self.batches.get(partition).cloned().unwrap_or_default();
        Ok(Box::pin(futures::stream::iter(batches.into_iter().map(Ok))))
    }

    fn name(&self) -> &str {
        "FixedPartitionScan"
    }
}

/// Drive **every** declared partition of `op` and return the total row count.
/// This mirrors `ExecutionContext::sql`, which drives
/// `0..physical.output_partitions()`.
async fn run_all_partitions(op: &dyn PhysicalOperator) -> usize {
    let mut rows = 0;
    for p in 0..op.output_partitions() {
        let stream = op.execute(p).await.unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        rows += batches.iter().map(|b| b.num_rows()).sum::<usize>();
    }
    rows
}

fn scan(
    partitions: usize,
    batches_per_partition: usize,
    rows_per_batch: usize,
) -> Arc<dyn PhysicalOperator> {
    Arc::new(FixedPartitionScan::new(
        partitions,
        batches_per_partition,
        rows_per_batch,
    ))
}

fn count_agg(id: &Expr) -> AggregateExpr {
    AggregateExpr {
        func: AggregateFunction::Count,
        input: id.clone(),
        distinct: false,
        second_arg: None,
    }
}

fn spill_count_agg(id: &Expr) -> SpillAggregateExpr {
    SpillAggregateExpr {
        func: AggregateFunction::Count,
        input: id.clone(),
        distinct: false,
        second_arg: None,
    }
}

fn pool() -> SharedMemoryPool {
    Arc::new(MemoryPool::new(ExecutionConfig::default().memory_limit))
}

// ---------------------------------------------------------------------------
// Bug 3 — out-of-range execute(partition) must be a LOUD error
// ---------------------------------------------------------------------------

/// Assert that asking `op` for one partition past the end fails, and fails with
/// a message that names the operator and both numbers.
///
/// Before the guard this returned `Ok(empty_stream)` for every operator in the
/// engine, which is why a parent looping past a child's real partition count
/// produced a wrong row count instead of a crash.
async fn assert_rejects_out_of_range(op: &dyn PhysicalOperator) {
    let declared = op.output_partitions();
    assert!(
        declared >= 1,
        "{}: output_partitions() must be at least 1",
        op.name()
    );

    match op.execute(declared).await {
        Ok(_) => panic!(
            "{}: execute({}) succeeded but output_partitions() == {}. \
             An out-of-range partition MUST be an error — returning an empty \
             stream is what made three wrong-answer bugs silent.",
            op.name(),
            declared,
            declared
        ),
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("out of range"),
                "{}: expected an out-of-range error, got: {msg}",
                op.name()
            );
            assert!(
                msg.contains(op.name()),
                "{}: error must name the operator, got: {msg}",
                op.name()
            );
            assert!(
                msg.contains(&format!("output_partitions={declared}")),
                "{}: error must report the declared count, got: {msg}",
                op.name()
            );
        }
    }
}

#[tokio::test]
async fn out_of_range_partition_is_an_error_not_an_empty_stream() {
    let child = scan(4, 2, 500);

    let id = Expr::column("id");
    let always_true = Expr::column("id").gt_eq(Expr::literal(ScalarValue::Int64(0)));

    let ops: Vec<Arc<dyn PhysicalOperator>> = vec![
        // Leaf
        child.clone(),
        Arc::new(MemoryTableExec::new(
            "mem",
            child.schema(),
            vec![RecordBatch::try_new(
                child.schema(),
                vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
            )
            .unwrap()],
            None,
        )),
        // Partition-preserving
        Arc::new(FilterExec::new(child.clone(), always_true.clone())),
        Arc::new(ProjectExec::new(
            child.clone(),
            vec![id.clone()],
            child.schema(),
        )),
        // Pipeline breakers (declare exactly 1)
        Arc::new(SortExec::new(
            child.clone(),
            vec![SortExpr::new(id.clone())],
        )),
        Arc::new(HashAggregateExec::try_new(child.clone(), vec![], vec![count_agg(&id)]).unwrap()),
        // Row-preserving
        Arc::new(LimitExec::new(child.clone(), 0, Some(10))),
        Arc::new(UnionExec::new(vec![child.clone(), child.clone()])),
        // Join
        Arc::new(HashJoinExec::new(
            child.clone(),
            child.clone(),
            vec![(id.clone(), id.clone())],
            JoinType::Inner,
        )),
    ];

    for op in &ops {
        assert_rejects_out_of_range(op.as_ref()).await;
    }
}

#[tokio::test]
async fn out_of_range_partition_is_an_error_in_spillable_operators() {
    // The spillable operators are the ALWAYS-ON path in this engine
    // (see CLAUDE.md "Memory Safety Rule"), so they need the same guard.
    let child = scan(4, 2, 500);
    let id = Expr::column("id");

    let ops: Vec<Arc<dyn PhysicalOperator>> = vec![
        Arc::new(ExternalSortExec::new(
            child.clone(),
            vec![SortExpr::new(id.clone())],
            pool(),
            ExecutionConfig::default(),
        )),
        Arc::new(SpillableHashAggregateExec::new(
            child.clone(),
            vec![],
            vec![spill_count_agg(&id)],
            HashAggregateExec::try_new(child.clone(), vec![], vec![count_agg(&id)])
                .unwrap()
                .schema(),
            pool(),
            ExecutionConfig::default(),
        )),
        Arc::new(SpillableHashJoinExec::new(
            child.clone(),
            child.clone(),
            vec![(id.clone(), id.clone())],
            JoinType::Inner,
            pool(),
            ExecutionConfig::default(),
        )),
    ];

    for op in &ops {
        assert_rejects_out_of_range(op.as_ref()).await;
    }
}

#[tokio::test]
async fn every_declared_partition_is_answerable() {
    // The other half of the contract: a partition index BELOW the declared
    // count must never error. A guard that over-fires would break the engine
    // just as thoroughly as no guard at all.
    let child = scan(4, 2, 500);
    let id = Expr::column("id");

    let ops: Vec<Arc<dyn PhysicalOperator>> = vec![
        child.clone(),
        Arc::new(FilterExec::new(
            child.clone(),
            Expr::column("id").gt_eq(Expr::literal(ScalarValue::Int64(0))),
        )),
        Arc::new(ProjectExec::new(
            child.clone(),
            vec![id.clone()],
            child.schema(),
        )),
        Arc::new(SortExec::new(
            child.clone(),
            vec![SortExpr::new(id.clone())],
        )),
        Arc::new(UnionExec::new(vec![child.clone(), child.clone()])),
    ];

    for op in &ops {
        for p in 0..op.output_partitions() {
            op.execute(p)
                .await
                .unwrap_or_else(|e| panic!("{}: execute({p}) failed: {e}", op.name()));
        }
    }
}

#[tokio::test]
async fn fixture_really_is_multi_partition_and_multi_batch() {
    // Guards the guard: if this ever reports 1 partition or 1 batch per
    // partition, every other test in this file has quietly stopped testing
    // anything, exactly as data/tpch-1mb does for the rest of the suite.
    let s = FixedPartitionScan::new(4, 3, 500);
    assert_eq!(s.output_partitions(), 4);
    assert_eq!(s.total_rows(), 4 * 3 * 500);
    for p in 0..s.output_partitions() {
        let batches: Vec<RecordBatch> = s.execute(p).await.unwrap().try_collect().await.unwrap();
        assert_eq!(batches.len(), 3, "partition {p} must serve several batches");
    }
    assert_eq!(run_all_partitions(&s).await, 6_000);
}

// ---------------------------------------------------------------------------
// Bug 1 — UnionExec must drain every input partition
// ---------------------------------------------------------------------------

/// Collect every `id` value the operator emits across all of its partitions.
async fn collect_ids(op: &dyn PhysicalOperator) -> Vec<i64> {
    let mut ids = Vec::new();
    for p in 0..op.output_partitions() {
        let batches: Vec<RecordBatch> = op.execute(p).await.unwrap().try_collect().await.unwrap();
        for b in batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column");
            ids.extend(col.values().iter().copied());
        }
    }
    ids
}

#[tokio::test]
async fn union_all_keeps_every_row_of_every_input_partition() {
    // Each branch: 4 partitions x 3 batches x 500 rows = 6,000 rows, ids 0..5999.
    // The branches are separate fixtures, so the union must contain every id in
    // 0..5999 exactly TWICE.
    //
    // Before the fix `UnionExec::execute` ignored its argument and called
    // `input.execute(0)` on each input while declaring 1 partition, so it
    // returned 2 x 1,500 = 3,000 rows: a quarter of each branch. The count
    // assertion below is what catches that; the per-id assertion catches the
    // subtler failure where the right NUMBER of rows comes back from the wrong
    // partitions.
    let a = scan(4, 3, 500);
    let b = scan(4, 3, 500);
    let union = UnionExec::new(vec![a, b]);

    assert_eq!(
        run_all_partitions(&union).await,
        12_000,
        "UNION ALL must keep every row of every input partition"
    );

    let mut ids = collect_ids(&union).await;
    ids.sort_unstable();
    let expected: Vec<i64> = (0..6_000).flat_map(|i| [i, i]).collect();
    assert_eq!(ids, expected, "UNION ALL must keep each id exactly twice");
}

#[tokio::test]
async fn union_all_of_three_inputs_keeps_every_row() {
    let union = UnionExec::new(vec![scan(4, 3, 500), scan(2, 1, 1000), scan(3, 2, 250)]);
    let expected = 4 * 3 * 500 + 2 * 1000 + 3 * 2 * 250;
    assert_eq!(run_all_partitions(&union).await, expected);
    assert_eq!(expected, 9_500);
}

#[tokio::test]
async fn union_streams_lazily_rather_than_materializing() {
    // UNION ALL is not a pipeline breaker. Draining all input partitions must
    // not turn it into one — `collect_input_partitions_concurrently`, the
    // idiom SortExec uses, would materialize both branches before emitting a
    // row, which for UNION ALL over two large tables is an OOM the engine's
    // memory-safety rule does not permit. A lazily chained union emits its
    // first batch after opening exactly one partition, so taking one batch
    // must not require reading everything.
    let union = UnionExec::new(vec![scan(4, 3, 500), scan(4, 3, 500)]);
    let mut stream = union.execute(0).await.unwrap();
    let first = stream.try_next().await.unwrap().expect("a first batch");
    assert_eq!(first.num_rows(), 500, "expected one batch, not a concat");
}

// ---------------------------------------------------------------------------
// SQL-level regressions, DuckDB-validated against data/tpch-10mb
// ---------------------------------------------------------------------------

const TPCH_10MB: &str = "data/tpch-10mb";

fn tpch_10mb_ctx() -> ExecutionContext {
    let dir = format!("{}/{}", env!("CARGO_MANIFEST_DIR"), TPCH_10MB);
    let mut ctx = ExecutionContext::new();
    for table in [
        "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
    ] {
        let path = format!("{dir}/{table}.parquet");
        ctx.register_parquet(table, &path).unwrap_or_else(|e| {
            panic!(
                "Failed to load {path}: {e}\n\
                 Regenerate with: cargo run --release -- generate-parquet --sf 0.01 \
                 --output ./data/tpch-10mb"
            )
        });
    }
    ctx
}

async fn count(ctx: &ExecutionContext, sql: &str) -> i64 {
    let result = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n  {e}"));
    assert_eq!(result.batches.len(), 1, "expected one batch for {sql}");
    result.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

async fn rows(ctx: &ExecutionContext, sql: &str) -> usize {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("query failed: {sql}\n  {e}"))
        .row_count
}

#[tokio::test]
async fn multi_partition_queries_do_not_trip_the_guard() {
    // lineitem at SF=0.01 is 60,000 rows = 8 Arrow batches, so this plan really
    // is multi-partition on any machine with >1 core. Verified against DuckDB
    // reading the same parquet file.
    let ctx = tpch_10mb_ctx();
    assert_eq!(count(&ctx, "SELECT COUNT(*) FROM lineitem").await, 60_000);
    assert_eq!(
        count(
            &ctx,
            "SELECT COUNT(*) FROM lineitem WHERE l_orderkey > 14000"
        )
        .await,
        3_336
    );
    assert_eq!(
        rows(
            &ctx,
            "SELECT l_orderkey, SUM(l_quantity) AS q FROM lineitem \
             GROUP BY l_orderkey ORDER BY l_orderkey LIMIT 5"
        )
        .await,
        5
    );
}

/// UNION ALL over a table big enough to be scanned in several partitions.
///
/// Every expected value here was produced by DuckDB reading the SAME parquet
/// file (`duckdb.read_parquet('data/tpch-10mb/lineitem.parquet')`).
///
/// The fixture matters: `data/tpch-1mb` is 6,000 rows in ONE batch, so it is
/// one partition and this query is correct there even with the bug — which is
/// exactly why `tests/expected_results/setop/union_all.csv` stayed green while
/// UNION ALL was losing 96% of its rows at SF=0.1. Do not "simplify" these onto
/// tpch-1mb.
#[tokio::test]
async fn union_all_over_a_multi_partition_scan_keeps_every_row() {
    let ctx = tpch_10mb_ctx();

    // Self-validating: whatever the generator produces, the union is 2x it.
    let base = count(&ctx, "SELECT COUNT(*) FROM lineitem").await;
    assert_eq!(base, 60_000, "fixture changed; DuckDB oracle says 60000");

    assert_eq!(
        count(
            &ctx,
            "SELECT COUNT(*) FROM (SELECT l_orderkey FROM lineitem \
             UNION ALL SELECT l_orderkey FROM lineitem) x"
        )
        .await,
        2 * base, // DuckDB: 120000; pre-fix engine: 16384
    );

    assert_eq!(
        count(
            &ctx,
            "SELECT COUNT(*) FROM (SELECT l_orderkey FROM lineitem \
             UNION ALL SELECT l_orderkey FROM lineitem \
             UNION ALL SELECT l_orderkey FROM lineitem) x"
        )
        .await,
        3 * base, // DuckDB: 180000
    );

    // UNION (distinct) is lowered as group-by-every-column ON TOP of UnionExec,
    // so it inherited the same loss.
    assert_eq!(
        count(
            &ctx,
            "SELECT COUNT(*) FROM (SELECT DISTINCT l_orderkey FROM lineitem \
             UNION SELECT DISTINCT l_orderkey FROM lineitem) x"
        )
        .await,
        14_785, // DuckDB
    );
}

/// A UNION ALL that is NOT at the plan root.
///
/// CTE materialization and uncorrelated subqueries run their plan through
/// `run_subquery_blocking`, which drives partition 0 only. A union that
/// declared one partition per input partition would return 1/8 of its rows
/// here even though the top-level query above is correct — which is why
/// `UnionExec::output_partitions()` is 1. DuckDB on the same parquet: 4.
#[tokio::test]
async fn union_all_inside_a_twice_referenced_cte_keeps_every_row() {
    let ctx = tpch_10mb_ctx();
    assert_eq!(
        count(
            &ctx,
            "WITH t AS (SELECT l_orderkey FROM lineitem \
                        UNION ALL SELECT l_orderkey FROM lineitem) \
             SELECT COUNT(*) AS c FROM t a JOIN t b ON a.l_orderkey = b.l_orderkey \
             WHERE a.l_orderkey = 1"
        )
        .await,
        4,
    );

    // Same shape through an uncorrelated IN subquery.
    assert_eq!(
        count(
            &ctx,
            "SELECT COUNT(*) FROM lineitem WHERE l_orderkey IN \
             (SELECT l_orderkey FROM lineitem UNION ALL SELECT l_orderkey FROM lineitem)"
        )
        .await,
        60_000, // DuckDB: every orderkey is present
    );
}
