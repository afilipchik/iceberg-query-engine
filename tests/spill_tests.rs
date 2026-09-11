//! Spill-path integration tests.
//!
//! These are the first tests that actually exercise the spill machinery:
//! Most historical tests run a query twice — once with an effectively unlimited memory
//! budget and once with a budget tiny enough that the spillable operator
//! MUST take its disk path — and asserts both that spilling really happened
//! (via QueryMetrics::spill_metrics) and that the results are identical.
//!
//! The retained aggregate pair instead checks an independent typed oracle with
//! actual spill bytes, and typed refusal when the result cannot fit its budget.
//!
//! Uses data/tpch-10mb (SF=0.01) rather than tpch-1mb: at SF=0.001 the whole
//! input fits in a single Arrow batch, and the partition-eviction logic in the
//! spillable operators only triggers once a later batch arrives while earlier
//! partitions hold data. Regenerate with:
//! `cargo run --release -- generate-parquet --sf 0.01 --output ./data/tpch-10mb`.

use arrow::array::*;
use arrow::datatypes::{DataType, TimeUnit};
use query_engine::{ExecutionConfig, ExecutionContext, QueryResult};
use std::path::PathBuf;

const TABLES: [&str; 8] = [
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

fn register_tables(ctx: &mut ExecutionContext) {
    let data_dir = format!("{}/data/tpch-10mb", env!("CARGO_MANIFEST_DIR"));
    for table in &TABLES {
        let path = format!("{data_dir}/{table}.parquet");
        ctx.register_parquet(*table, &path)
            .unwrap_or_else(|e| panic!("Failed to load {path}: {e}"));
    }
}

fn unlimited_ctx() -> ExecutionContext {
    let mut ctx = ExecutionContext::new();
    register_tables(&mut ctx);
    ctx
}

/// Context with a memory budget small enough that any pipeline-breaking
/// operator over TPC-H SF=0.001 exceeds `memory_limit * spill_threshold`
/// and must spill. Spill files go under target/ (gitignored).
fn spilling_ctx(limit_bytes: usize, test_name: &str) -> ExecutionContext {
    let spill_path = PathBuf::from(format!(
        "{}/target/test_spill/{test_name}",
        env!("CARGO_MANIFEST_DIR")
    ));
    let config = ExecutionConfig::new()
        .with_memory_limit(limit_bytes)
        .with_spill_path(spill_path);
    let mut ctx = ExecutionContext::with_config(config);
    register_tables(&mut ctx);
    ctx
}

/// Render all rows of a result as strings. Floats are rounded to 3 decimals so
/// legitimate accumulation-order differences between the in-memory and spill
/// paths don't produce false mismatches.
fn result_rows(result: &QueryResult) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in &result.batches {
        for row in 0..batch.num_rows() {
            let mut out = Vec::with_capacity(batch.num_columns());
            for col in batch.columns() {
                out.push(cell(col, row));
            }
            rows.push(out);
        }
    }
    rows
}

fn cell(col: &ArrayRef, row: usize) -> String {
    if col.is_null(row) {
        return "NULL".to_string();
    }
    match col.data_type() {
        DataType::Int32 => col
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Int64 => col
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Float64 => {
            format!(
                "{:.3}",
                col.as_any()
                    .downcast_ref::<Float64Array>()
                    .unwrap()
                    .value(row)
            )
        }
        DataType::Utf8 => col
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Date32 => col
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap()
            .value(row)
            .to_string(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => col
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .value(row)
            .to_string(),
        other => panic!("cell(): unhandled type {other:?} — extend the test helper"),
    }
}

/// Run `sql` on both contexts; assert the limited run spilled and that row
/// sets match. `ordered`: compare in order; otherwise compare sorted.
async fn assert_spill_matches(sql: &str, limit_bytes: usize, test_name: &str, ordered: bool) {
    let baseline = unlimited_ctx()
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("baseline run failed: {e}"));
    let spill_ctx = spilling_ctx(limit_bytes, test_name);
    let spilled = spill_ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("spill run failed: {e}"));

    assert!(
        spilled.metrics.spill_metrics.is_some(),
        "expected the {limit_bytes}-byte budget to force a spill, but none was recorded \
         — lower the limit or the operator silently stayed in memory"
    );

    let mut base_rows = result_rows(&baseline);
    let mut spill_rows = result_rows(&spilled);
    if !ordered {
        base_rows.sort();
        spill_rows.sort();
    }
    assert_eq!(
        base_rows.len(),
        spill_rows.len(),
        "row count mismatch: baseline {} vs spill {}",
        base_rows.len(),
        spill_rows.len()
    );
    assert_eq!(
        base_rows, spill_rows,
        "spill-path results differ from in-memory results"
    );
}

/// Hash join with a build side that exceeds the budget → SpillableHashJoinExec
/// partitioned spill path, plus spillable aggregation above the join.
#[tokio::test]
async fn join_spill_matches_in_memory() {
    assert_spill_matches(
        "SELECT o_orderpriority, COUNT(*) AS cnt, SUM(l_extendedprice) AS total \
         FROM lineitem, orders WHERE l_orderkey = o_orderkey \
         GROUP BY o_orderpriority ORDER BY o_orderpriority",
        256 * 1024,
        "join_spill",
        true,
    )
    .await;
}

// Independent oracle for the fixture-backed retained aggregate contract. Read
// every actual join key/date, preserving duplicate orders and nullable values.
type JoinedGroups = std::collections::BTreeMap<(i64, Option<i32>), (Option<f64>, f64, usize, i64)>;
const RETAINED_AGG_SQL: &str = "SELECT l_orderkey, o_orderdate, SUM(l_quantity) AS qty, COUNT(*) AS cnt FROM lineitem, orders WHERE l_orderkey = o_orderkey GROUP BY l_orderkey, o_orderdate";
fn joined_groups_oracle() -> JoinedGroups {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let read = |table: &str| {
        ParquetRecordBatchReaderBuilder::try_new(
            std::fs::File::open(format!(
                "{}/data/tpch-10mb/{table}.parquet",
                env!("CARGO_MANIFEST_DIR")
            ))
            .unwrap(),
        )
        .unwrap()
        .build()
        .unwrap()
    };
    let mut orders: std::collections::BTreeMap<i64, Vec<Option<i32>>> = Default::default();
    for batch in read("orders") {
        let batch = batch.unwrap();
        let keys = batch
            .column_by_name("o_orderkey")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let dates = batch
            .column_by_name("o_orderdate")
            .unwrap()
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if !keys.is_null(row) {
                orders
                    .entry(keys.value(row))
                    .or_default()
                    .push((!dates.is_null(row)).then(|| dates.value(row)));
            }
        }
    }
    let mut expected = JoinedGroups::new();
    for batch in read("lineitem") {
        let batch = batch.unwrap();
        let keys = batch
            .column_by_name("l_orderkey")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let quantities = batch
            .column_by_name("l_quantity")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            if keys.is_null(row) {
                continue;
            }
            let quantity = (!quantities.is_null(row)).then(|| {
                let value = quantities.value(row);
                assert!(
                    value.is_finite(),
                    "fixture oracle requires finite quantities"
                );
                value
            });
            for date in orders.get(&keys.value(row)).into_iter().flatten() {
                let (sum, abs_sum, additions, count) = expected
                    .entry((keys.value(row), *date))
                    .or_insert((None, 0.0, 0, 0));
                *count += 1;
                if let Some(q) = quantity {
                    let total = sum.unwrap_or(0.0) + q;
                    *abs_sum += q.abs();
                    assert!(total.is_finite() && abs_sum.is_finite());
                    *sum = Some(total);
                    *additions += 1;
                }
            }
        }
    }
    expected
}

#[tokio::test]
async fn retained_aggregate_over_budget_is_typed_refusal() {
    let expected = joined_groups_oracle();
    // Key/date/COUNT buffers alone, excluding SUM and all overhead. This is
    // specific to the current retained, separately encoded Arrow result API.
    assert!(expected.len() * (8 + 4 + 8) > 256 * 1024);
    let ctx = spilling_ctx(256 * 1024, "retained_agg_refusal");
    let baseline = ctx.memory_pool().used();
    let error = ctx.sql(RETAINED_AGG_SQL).await.unwrap_err();
    assert!(
        error.is_memory_limit(),
        "must refuse by typed admission, not unrelated failure: {error}"
    );
    assert_eq!(ctx.memory_pool().used(), baseline);
}

#[tokio::test]
async fn retained_aggregate_actual_spill_matches_independent_typed_oracle() {
    let expected = joined_groups_oracle();
    let spill_dir = tempfile::tempdir().unwrap();
    let budget = 16 * 1024 * 1024;
    let mut config = ExecutionConfig::new()
        .with_memory_limit(budget)
        .with_spill_path(spill_dir.path().to_path_buf());
    // Deliberately separate spilling policy from the budget needed to retain
    // the complete result; do not claim the old 256KiB query now completes.
    config.spill_threshold = 0.02;
    let mut ctx = ExecutionContext::with_config(config);
    register_tables(&mut ctx);
    let baseline = ctx.memory_pool().used();
    let result = ctx.sql(RETAINED_AGG_SQL).await.unwrap();
    let spilled = result.metrics.spill_metrics.as_ref().unwrap().bytes_spilled;
    assert!(
        spilled > 0,
        "require actual disk bytes, not presence of metrics"
    );
    assert!(result.metrics.reserved_peak_memory_bytes <= budget);
    let mut actual = std::collections::BTreeMap::new();
    for batch in &result.batches {
        assert_eq!(batch.num_columns(), 4);
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let dates = batch
            .column(1)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();
        let sums = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let counts = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            assert!(!keys.is_null(row) && !counts.is_null(row));
            let key = (
                keys.value(row),
                (!dates.is_null(row)).then(|| dates.value(row)),
            );
            let value = (
                (!sums.is_null(row)).then(|| sums.value(row)),
                counts.value(row),
            );
            assert!(
                actual.insert(key, value).is_none(),
                "duplicate emitted group"
            );
        }
    }
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>()
    );
    for (key, (expected_sum, abs_sum, additions, expected_count)) in expected {
        let (actual_sum, count) = actual[&key];
        assert_eq!(count, expected_count, "COUNT for {key:?}");
        match (actual_sum, expected_sum) {
            (None, None) => (),
            (Some(actual), Some(expected)) => {
                // Both arbitrary spill-tree summation and this independent
                // sequential sum obey the finite floating-point forward-error
                // bound gamma_n * sum(abs(x)). Use epsilon (twice unit roundoff)
                // and correct the independently accumulated absolute sum.
                let neps = additions as f64 * f64::EPSILON;
                assert!(neps < 0.5);
                let gamma = neps / (1.0 - neps);
                let bound = 2.0 * gamma * abs_sum / (1.0 - gamma);
                assert!(
                    actual.is_finite() && (actual - expected).abs() <= bound,
                    "SUM for {key:?}: actual={actual}, expected={expected}, bound={bound}"
                );
            }
            values => panic!("SUM NULL mismatch for {key:?}: {values:?}"),
        }
    }
    assert_eq!(result.row_count, actual.len());
    let retained = ctx.memory_pool().used() - baseline;
    assert!(retained > 0, "returned Arrow output remains admitted");
    eprintln!("retained aggregate: groups={}, spilled_bytes={}, reserved_peak={}, retained_bytes={retained}", actual.len(), spilled, result.metrics.reserved_peak_memory_bytes);
    drop(result);
    assert_eq!(ctx.memory_pool().used(), baseline);
}

/// Full sort with input exceeding the budget → ExternalSortExec merge-sort path.
#[tokio::test]
async fn sort_spill_matches_in_memory() {
    assert_spill_matches(
        "SELECT l_orderkey, l_linenumber, l_quantity, l_extendedprice FROM lineitem \
         ORDER BY l_extendedprice DESC, l_orderkey, l_linenumber, l_quantity",
        256 * 1024,
        "sort_spill",
        true,
    )
    .await;
}

/// `ORDER BY ... LIMIT` with input exceeding the budget → ExternalSortExec's
/// spill branch with a fused `fetch` (the top-k fusion rule folds a
/// `skip == 0` LIMIT straight into `with_fetch`, so the spill branch's own
/// truncation is the ONLY place the row count is cut —
/// spill-join-correctness-2 task 004's fix, re-pinned here at the SQL level
/// against oom-safety-hardening task 003's streamed merge delivery, where
/// the limit is now applied per arriving batch and ends the output stream
/// early). Sort keys are unique per row ((l_orderkey, l_linenumber) is a
/// key), so the ordered comparison is fully deterministic — no tie at the
/// LIMIT boundary can make two correct answers differ.
#[tokio::test]
async fn sort_spill_with_limit_matches_in_memory() {
    assert_spill_matches(
        "SELECT l_orderkey, l_linenumber, l_quantity, l_extendedprice FROM lineitem \
         ORDER BY l_extendedprice DESC, l_orderkey, l_linenumber LIMIT 50",
        256 * 1024,
        "sort_spill_limit",
        true,
    )
    .await;
}

/// Distinct aggregation across the spill boundary (COUNT(DISTINCT) has its own
/// accumulator path).
#[tokio::test]
async fn count_distinct_spill_matches_in_memory() {
    assert_spill_matches(
        "SELECT o_orderpriority, COUNT(DISTINCT o_custkey) AS custs \
         FROM orders, lineitem WHERE o_orderkey = l_orderkey \
         GROUP BY o_orderpriority ORDER BY o_orderpriority",
        256 * 1024,
        "count_distinct_spill",
        true,
    )
    .await;

    // Pin the absolute values (verified against DuckDB on data/tpch-10mb) so
    // both execution paths can't drift wrong together. Before the
    // distinct_set-union merge fix, the in-memory path returned one
    // partition's distinct count (~375) instead of these.
    let result = unlimited_ctx()
        .sql(
            "SELECT o_orderpriority, COUNT(DISTINCT o_custkey) AS custs \
             FROM orders, lineitem WHERE o_orderkey = l_orderkey \
             GROUP BY o_orderpriority ORDER BY o_orderpriority",
        )
        .await
        .unwrap();
    let rows = result_rows(&result);
    let expected: Vec<Vec<String>> = [
        ("1-URGENT", "1654"),
        ("2-HIGH", "1652"),
        ("3-MEDIUM", "1655"),
        ("4-NOT SPECIFIED", "1644"),
        ("5-LOW", "1654"),
    ]
    .iter()
    .map(|(p, c)| vec![p.to_string(), c.to_string()])
    .collect();
    assert_eq!(rows, expected, "COUNT(DISTINCT) disagrees with DuckDB");
}

/// High-cardinality COUNT(DISTINCT) GROUP BY at a very low limit —
/// oom-safety-hardening task 002's streaming two-phase reservation feeds
/// `aggregate_with_spilling` mid-stream, and the per-partition finalize
/// gate (raw bytes + predicted aggregation state vs the threshold)
/// necessarily trips at 128KB, forcing the chunked (sub-partitioned)
/// read-back for every partition. Results must match the unlimited run
/// exactly through the full SQL path.
#[tokio::test]
async fn agg_spill_chunked_finalize_matches_in_memory() {
    assert_spill_matches(
        "SELECT l_partkey, COUNT(DISTINCT l_orderkey) AS orders, SUM(l_quantity) AS qty \
         FROM lineitem, orders WHERE l_orderkey = o_orderkey \
         GROUP BY l_partkey",
        128 * 1024,
        "agg_chunked_finalize",
        false,
    )
    .await;
}

/// spill-boundaries task 003: LEFT/RIGHT/FULL joins whose build side
/// exceeds the budget now COMPLETE through the join spill path, cell-exact
/// vs the in-memory run (until this task they failed loudly by name —
/// `left_join_spill_fails_loudly_not_wrong`, replaced by this test).
/// `COUNT(<other side's key>)` alongside `COUNT(*)` pins the NULL-extended
/// rows: an inner-join-shaped answer would make the two counts equal.
/// Shapes: build = LEFT = the preserved side (orders LEFT JOIN lineitem —
/// the planner keeps the build on the left since 15k < 2 x 60k rows);
/// build = RIGHT with the PROBE side preserved and an ON-clause filter
/// (lineitem LEFT JOIN orders — 60k > 2 x 15k flips the build to the
/// right); a RIGHT join; a FULL join with an ON-clause filter.
#[tokio::test]
async fn outer_join_spill_matches_in_memory() {
    assert_spill_matches(
        "SELECT o_orderpriority, COUNT(*) AS cnt, COUNT(l_orderkey) AS matched FROM orders \
         LEFT JOIN lineitem ON o_orderkey = l_orderkey \
         GROUP BY o_orderpriority ORDER BY o_orderpriority",
        256 * 1024,
        "left_join_spill_build_preserved",
        true,
    )
    .await;
    assert_spill_matches(
        "SELECT l_linestatus, COUNT(*) AS cnt, COUNT(o_orderkey) AS matched FROM lineitem \
         LEFT JOIN orders ON l_orderkey = o_orderkey AND o_custkey <> l_suppkey \
         GROUP BY l_linestatus ORDER BY l_linestatus",
        8 * 1024,
        "left_join_spill_probe_preserved_filtered",
        true,
    )
    .await;
    assert_spill_matches(
        "SELECT o_orderpriority, COUNT(*) AS cnt, COUNT(l_orderkey) AS matched FROM lineitem \
         RIGHT JOIN orders ON l_orderkey = o_orderkey \
         GROUP BY o_orderpriority ORDER BY o_orderpriority",
        8 * 1024,
        "right_join_spill",
        true,
    )
    .await;
    assert_spill_matches(
        "SELECT COUNT(*) AS cnt, COUNT(l_orderkey) AS l_rows, COUNT(o_orderkey) AS o_rows \
         FROM lineitem FULL JOIN orders ON l_orderkey = o_orderkey AND l_quantity > 25.0",
        8 * 1024,
        "full_join_spill_filtered",
        true,
    )
    .await;
}

/// spill-join-correctness-3 task 004: a Q4-shaped `EXISTS` (SEMI join)
/// whose build side exceeds the budget must now COMPLETE through the join
/// spill path, cell-exact vs the in-memory run. No date filter on
/// `orders`, so both sides are far above an 8KB budget whichever side the
/// planner builds from.
#[tokio::test]
async fn semi_join_exists_spill_matches_in_memory() {
    assert_spill_matches(
        "SELECT o_orderpriority, COUNT(*) AS order_count FROM orders \
         WHERE EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey \
         AND l_commitdate < l_receiptdate) \
         GROUP BY o_orderpriority ORDER BY o_orderpriority",
        8 * 1024,
        "semi_exists_spill",
        true,
    )
    .await;
}

/// spill-join-correctness-3 task 004: a `NOT EXISTS` (ANTI join) — the
/// Q16/Q22 shape — through the spill path, cell-exact vs in-memory. Counts
/// per priority so every unmatched `orders` row is accounted for.
#[tokio::test]
async fn anti_join_not_exists_spill_matches_in_memory() {
    assert_spill_matches(
        "SELECT o_orderpriority, COUNT(*) AS cnt FROM orders \
         WHERE NOT EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey \
         AND l_shipmode = 'AIR') \
         GROUP BY o_orderpriority ORDER BY o_orderpriority",
        8 * 1024,
        "anti_not_exists_spill",
        true,
    )
    .await;
}

/// NOT IN retains its NULL-aware evaluator rather than an ordinary anti join.
/// Compare both budgets without demanding a join spill from this fallback;
/// the preceding NOT EXISTS test independently asserts actual anti-join spill.
#[tokio::test]
async fn not_in_membership_matches_across_budgets() {
    let sql = "SELECT COUNT(*) AS cnt, MIN(o_orderkey) AS mn, MAX(o_orderkey) AS mx FROM orders \
         WHERE o_orderkey NOT IN (SELECT l_orderkey FROM lineitem WHERE l_shipmode = 'AIR')";
    let baseline = unlimited_ctx().sql(sql).await.unwrap();
    let limited = spilling_ctx(8 * 1024, "not_in_membership_budget")
        .sql(sql)
        .await
        .unwrap();
    assert_eq!(result_rows(&baseline), result_rows(&limited));
}

/// spill-boundaries task 002: a SEMI join (EXISTS) and an ANTI join
/// (NOT EXISTS) whose correlated subquery carries a NON-EQUI predicate over
/// both sides — the ON-clause filter the spill path used to refuse
/// ("cannot evaluate an ON-clause filter"). At 8KB whichever side the
/// planner builds from is far above the budget, so a pass here means the
/// filter was evaluated per candidate pair INSIDE the spill path.
/// `l_suppkey <> o_custkey` is the compiled (two-column) shape;
/// `l_extendedprice > o_totalprice / 4` needs the gathered
/// `evaluate_expr` path.
#[tokio::test]
async fn filtered_semi_anti_join_spill_matches_in_memory() {
    assert_spill_matches(
        "SELECT o_orderpriority, COUNT(*) AS cnt FROM orders \
         WHERE EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey \
         AND l_suppkey <> o_custkey) \
         GROUP BY o_orderpriority ORDER BY o_orderpriority",
        8 * 1024,
        "filtered_semi_spill",
        true,
    )
    .await;
    assert_spill_matches(
        "SELECT o_orderpriority, COUNT(*) AS cnt FROM orders \
         WHERE NOT EXISTS (SELECT * FROM lineitem WHERE l_orderkey = o_orderkey \
         AND l_extendedprice > o_totalprice / 4) \
         GROUP BY o_orderpriority ORDER BY o_orderpriority",
        8 * 1024,
        "filtered_anti_spill",
        true,
    )
    .await;
}

/// A memory-limited context must still produce correct results for queries
/// that DON'T need to spill (small inputs stay on the in-memory fast path).
#[tokio::test]
async fn tiny_query_under_limit_still_correct() {
    let ctx = spilling_ctx(256 * 1024, "tiny_query");
    let result = ctx
        .sql("SELECT COUNT(*) AS c FROM nation")
        .await
        .expect("tiny query failed under memory limit");
    let rows = result_rows(&result);
    assert_eq!(rows, vec![vec!["25".to_string()]]);
}

/// Regression: a LEFT JOIN whose build side is the LEFT input must emit
/// unmatched build rows exactly once across all probe partitions. Before the
/// shared matched-bit fix, multi-partition probes dropped them entirely
/// (TPC-H Q13's zero-order-customer bucket vanished at SF=10) — and Right/Full
/// joins duplicated them instead. Expected values verified against DuckDB.
#[tokio::test]
async fn left_join_unmatched_build_rows_preserved() {
    let ctx = unlimited_ctx();
    let result = ctx
        .sql(
            "SELECT COUNT(*) AS c FROM orders LEFT JOIN \
             (SELECT l_orderkey FROM lineitem WHERE l_quantity > 49.0) t \
             ON o_orderkey = t.l_orderkey",
        )
        .await
        .expect("left join failed");
    let rows = result_rows(&result);
    assert_eq!(rows, vec![vec!["15078".to_string()]]);
}
