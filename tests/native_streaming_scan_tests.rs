//! oom-safety-hardening epic, task 004: end-to-end tests for the streaming
//! native-table scan path (`NativeStreamingScanExec`) and for the
//! raw-materialization refusal boundary it deliberately does NOT narrow.
//!
//! Fixture: a real on-disk native table whose on-disk size EXCEEDS the test
//! context's `memory_limit * spill_threshold` budget, so the materializing
//! `NativeTable::scan()` would refuse (`check_scan_budget`) — exactly the
//! pre-fix behavior for every shape. Post-fix contract under test:
//!   - spill-capable consumers (aggregate; aggregate-over-join) COMPLETE,
//!     cell-exact against an unconstrained context over the same directory;
//!   - deletion vectors and segment pruning still apply on the streaming
//!     path (mutated-table test is cell-exact with a non-empty vector);
//!   - genuinely materializing shapes (raw `SELECT *`, filter-only,
//!     ORDER BY-only) still refuse BY NAME (the pinning tests).

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use query_engine::storage::native_write::{
    write_batches_with_options, NativeWriteMode, NativeWriteOptions,
};
use query_engine::ExecutionContext;
use std::path::Path;
use std::sync::Arc;

const SEGMENTS: usize = 6;
const ROWS_PER_SEGMENT: usize = 10_000;
const TOTAL_ROWS: i64 = (SEGMENTS * ROWS_PER_SEGMENT) as i64;
/// Small enough that the fixture table (~0.9MB on disk) exceeds
/// `memory_limit * spill_threshold`, large enough that the spillable
/// operators can still do real work on this tiny dataset.
const SMALL_LIMIT: usize = 256 * 1024;
const BIG_LIMIT: usize = 1 << 30;

/// Write a real native table: `id` (unique, ascending — so segments have
/// disjoint id ranges and range predicates can prune provably) and
/// `grp = id % 5`.
async fn write_fixture(dir: &Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("grp", DataType::Int64, false),
    ]));
    let mut batches = Vec::new();
    for s in 0..SEGMENTS {
        let start = (s * ROWS_PER_SEGMENT) as i64;
        let ids: Vec<i64> = (start..start + ROWS_PER_SEGMENT as i64).collect();
        let grps: Vec<i64> = ids.iter().map(|i| i % 5).collect();
        batches.push(Ok(RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as arrow::array::ArrayRef,
                Arc::new(Int64Array::from(grps)) as arrow::array::ArrayRef,
            ],
        )
        .unwrap()));
    }
    let stream: query_engine::physical::RecordBatchStream =
        Box::pin(futures::stream::iter(batches));
    write_batches_with_options(
        stream,
        schema,
        dir,
        NativeWriteMode::Create,
        NativeWriteOptions {
            target_rows_per_segment: ROWS_PER_SEGMENT,
            ..Default::default()
        },
    )
    .await
    .unwrap();
}

fn small_ctx(dir: &Path, names: &[&str]) -> ExecutionContext {
    let mut ctx = ExecutionContext::with_memory_limit(SMALL_LIMIT);
    for name in names {
        ctx.register_native_table(*name, dir).unwrap();
    }
    ctx
}

fn big_ctx(dir: &Path, names: &[&str]) -> ExecutionContext {
    let mut ctx = ExecutionContext::with_memory_limit(BIG_LIMIT);
    for name in names {
        ctx.register_native_table(*name, dir).unwrap();
    }
    ctx
}

/// The fixture must genuinely be over budget in the small context, or every
/// test below is vacuous.
async fn assert_over_budget(dir: &Path) {
    let table = query_engine::storage::NativeTable::try_new(dir)
        .unwrap()
        .with_memory_budget(Some(
            (SMALL_LIMIT as f64 * query_engine::ExecutionConfig::default().spill_threshold) as u64,
        ));
    assert!(
        table.scan_budget_exceeded(),
        "fixture is not over budget — tests would be vacuous"
    );
}

fn fmt(result: &query_engine::QueryResult) -> String {
    arrow::util::pretty::pretty_format_batches(&result.batches)
        .unwrap()
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_select_star_still_refuses_by_name() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("t");
    write_fixture(&dir).await;
    assert_over_budget(&dir).await;

    let ctx = small_ctx(&dir, &["t"]);
    let err = ctx.sql("SELECT * FROM t").await.unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("memory safety budget"),
        "raw SELECT * must keep refusing by name, got: {msg}"
    );
    assert!(msg.contains("--memory-limit"), "{msg}");
}

#[tokio::test(flavor = "multi_thread")]
async fn other_materializing_shapes_still_refuse() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("t");
    write_fixture(&dir).await;

    let ctx = small_ctx(&dir, &["t"]);
    // Filter/project-only: no aggregate bounds the root materialization.
    let err = ctx.sql("SELECT id FROM t WHERE grp = 3").await.unwrap_err();
    assert!(err.to_string().contains("memory safety budget"), "{err}");
    // ORDER BY-only: ExternalSortExec is not a gate for this path (yet).
    let err = ctx.sql("SELECT id FROM t ORDER BY id").await.unwrap_err();
    assert!(err.to_string().contains("memory safety budget"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregate_over_oversized_table_completes_and_is_cell_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("t");
    write_fixture(&dir).await;
    assert_over_budget(&dir).await;

    let sql = "SELECT grp, COUNT(*) AS c, SUM(id) AS s FROM t GROUP BY grp ORDER BY grp";
    let small = small_ctx(&dir, &["t"]).sql(sql).await.expect(
        "an aggregate over an over-budget native table must COMPLETE via the streaming scan",
    );
    let big = big_ctx(&dir, &["t"]).sql(sql).await.unwrap();
    assert_eq!(small.row_count, 5);
    assert_eq!(fmt(&small), fmt(&big), "streaming path must be cell-exact");
}

#[tokio::test(flavor = "multi_thread")]
async fn filtered_aggregate_over_oversized_table_prunes_and_is_cell_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("t");
    write_fixture(&dir).await;

    // id >= 50_000 only lands in the last of 6 segments; pruning fires on
    // the streaming path (pinned at the operator level in
    // native_scan.rs's own tests) and FilterExec still narrows exactly.
    let sql = "SELECT grp, COUNT(*) AS c, SUM(id) AS s FROM t \
               WHERE id >= 50000 GROUP BY grp ORDER BY grp";
    let small = small_ctx(&dir, &["t"]).sql(sql).await.unwrap();
    let big = big_ctx(&dir, &["t"]).sql(sql).await.unwrap();
    assert_eq!(fmt(&small), fmt(&big));
    // Independent expectation: 10_000 rows survive, 2_000 per group.
    let total: i64 = TOTAL_ROWS - 50_000;
    let counts: i64 = small
        .batches
        .iter()
        .map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .sum::<i64>()
        })
        .sum();
    assert_eq!(counts, total);
}

#[tokio::test(flavor = "multi_thread")]
async fn aggregate_over_join_of_oversized_tables_completes_and_is_cell_exact() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("t");
    write_fixture(&dir).await;

    // Both scans sit beneath the aggregate; both stream; the join's build
    // side exceeds the tiny budget and spills (SpillableHashJoinExec's
    // streaming two-phase build).
    let sql = "SELECT a.grp AS g, COUNT(*) AS c FROM t1 a JOIN t2 b ON a.id = b.id \
               GROUP BY a.grp ORDER BY g";
    let small = small_ctx(&dir, &["t1", "t2"])
        .sql(sql)
        .await
        .expect("an aggregate over a join of over-budget native tables must COMPLETE by spilling");
    let big = big_ctx(&dir, &["t1", "t2"]).sql(sql).await.unwrap();
    assert_eq!(small.row_count, 5);
    assert_eq!(fmt(&small), fmt(&big));
    // Unique-key self-join: COUNT sums back to exactly TOTAL_ROWS.
    let counts: i64 = small
        .batches
        .iter()
        .map(|b| {
            b.column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .sum::<i64>()
        })
        .sum();
    assert_eq!(counts, TOTAL_ROWS);
}

#[tokio::test(flavor = "multi_thread")]
async fn deletion_vector_is_cell_exact_on_the_streaming_path() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("t");
    write_fixture(&dir).await;

    // Real DELETE through the real entrypoint: spread across every segment
    // (id % 7 == 0), leaving a non-empty deletion vector per segment.
    let mut ctx = small_ctx(&dir, &["t"]);
    let deleted = ctx
        .delete_from_native_table("DELETE FROM t WHERE id % 7 = 0")
        .await
        .unwrap();
    let expected_deleted = (0..TOTAL_ROWS).filter(|i| i % 7 == 0).count();
    assert_eq!(deleted.rows_deleted as usize, expected_deleted);

    let sql = "SELECT grp, COUNT(*) AS c, SUM(id) AS s FROM t GROUP BY grp ORDER BY grp";
    let small = ctx
        .sql(sql)
        .await
        .expect("mutated over-budget table must still complete via streaming");
    // Independently-computed expectation (never touches the engine).
    let mut expect: Vec<(i64, i64, i64)> = (0..5).map(|g| (g, 0, 0)).collect();
    for id in 0..TOTAL_ROWS {
        if id % 7 == 0 {
            continue;
        }
        let g = (id % 5) as usize;
        expect[g].1 += 1;
        expect[g].2 += id;
    }
    // SUM's physical output type may be Int64 or Float64 depending on the
    // aggregation tier; read it generically.
    fn col_i64(b: &RecordBatch, idx: usize, row: usize) -> i64 {
        let col = b.column(idx);
        if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
            a.value(row)
        } else if let Some(a) = col.as_any().downcast_ref::<arrow::array::Float64Array>() {
            a.value(row) as i64
        } else {
            panic!("unexpected aggregate output type {:?}", col.data_type());
        }
    }
    let mut got = Vec::new();
    for b in &small.batches {
        for i in 0..b.num_rows() {
            got.push((col_i64(b, 0, i), col_i64(b, 1, i), col_i64(b, 2, i)));
        }
    }
    got.sort_unstable();
    assert_eq!(
        got, expect,
        "deletion vector must be cell-exact on the streaming path"
    );

    // And an unconstrained context over the same mutated directory agrees.
    let big = big_ctx(&dir, &["t"]).sql(sql).await.unwrap();
    assert_eq!(fmt(&small), fmt(&big));
}

/// In-budget tables must take the pre-existing materializing path (the
/// dense-direct fast-path / GPU eligibility invariant is planner-level:
/// `scan_budget_exceeded()` is false, so nothing about their planning
/// changes). This pins the cheap observable half: same results, no refusal,
/// and `try_extract_native_dense_source` still fires for an in-budget
/// table (indirectly — the query completes identically).
#[tokio::test(flavor = "multi_thread")]
async fn in_budget_tables_are_unaffected() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().join("t");
    write_fixture(&dir).await;

    let ctx = big_ctx(&dir, &["t"]);
    let all = ctx.sql("SELECT * FROM t").await.unwrap();
    assert_eq!(all.row_count as i64, TOTAL_ROWS);
    let agg = ctx
        .sql("SELECT grp, COUNT(*) AS c FROM t GROUP BY grp ORDER BY grp")
        .await
        .unwrap();
    assert_eq!(agg.row_count, 5);
}
