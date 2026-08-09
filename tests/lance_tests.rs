//! Lance reader integration tests.
//!
//! Requires `--features lance` AND `data/tpch-1mb-lance`, produced by
//! `scripts/lance_convert.py --parquet ./data/tpch-1mb --out ./data/tpch-1mb-lance`.
//! Every test skips cleanly when either is missing, so a checkout without the
//! generated data still runs green.
//!
//! The oracle here is the engine's OWN Parquet path over `data/tpch-1mb`, which
//! is itself cell-validated against DuckDB by `tests/duckdb_validated.rs`. The
//! Lance datasets are a straight conversion of that same Parquet, so any
//! difference between the two paths is a Lance reader bug by construction.

#![cfg(feature = "lance")]

use arrow::record_batch::RecordBatch;
use query_engine::ExecutionContext;
use std::path::{Path, PathBuf};

const TABLES: [&str; 8] = [
    "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
];

fn parquet_dir() -> PathBuf {
    PathBuf::from("data/tpch-1mb")
}
fn lance_dir() -> PathBuf {
    PathBuf::from("data/tpch-1mb-lance")
}

/// True when both datasets are present; otherwise the caller should skip.
fn data_available() -> bool {
    let ok =
        lance_dir().join("orders.lance").exists() && parquet_dir().join("orders.parquet").exists();
    if !ok {
        eprintln!(
            "SKIP: data/tpch-1mb-lance or data/tpch-1mb missing (run scripts/lance_convert.py)"
        );
    }
    ok
}

fn lance_ctx() -> ExecutionContext {
    let mut ctx = ExecutionContext::new();
    for t in &TABLES {
        ctx.register_lance(*t, lance_dir().join(format!("{}.lance", t)))
            .unwrap_or_else(|e| panic!("register_lance({}) failed: {}", t, e));
    }
    ctx
}

fn parquet_ctx() -> ExecutionContext {
    let mut ctx = ExecutionContext::new();
    for t in &TABLES {
        ctx.register_parquet(*t, parquet_dir().join(format!("{}.parquet", t)))
            .unwrap_or_else(|e| panic!("register_parquet({}) failed: {}", t, e));
    }
    ctx
}

/// Render results as CSV-ish text so two runs can be compared cell by cell.
fn to_cells(batches: &[RecordBatch]) -> Vec<String> {
    let mut out = Vec::new();
    for b in batches {
        let mut buf = Vec::new();
        {
            let mut w = arrow::csv::WriterBuilder::new()
                .with_header(false)
                .build(&mut buf);
            w.write(b).expect("csv write");
        }
        out.extend(
            String::from_utf8(buf)
                .expect("utf8")
                .lines()
                .map(str::to_string),
        );
    }
    out
}

/// Run `sql` on both paths and assert the results are cell-identical.
async fn assert_same(sql: &str) {
    let lance = lance_ctx().sql(sql).await.expect("lance query failed");
    let parquet = parquet_ctx().sql(sql).await.expect("parquet query failed");

    assert_eq!(
        lance.row_count, parquet.row_count,
        "row count differs for: {}",
        sql
    );

    let (lc, pc) = (to_cells(&lance.batches), to_cells(&parquet.batches));
    assert_eq!(
        lc.len(),
        pc.len(),
        "row count (rendered) differs for: {}",
        sql
    );
    for (i, (l, p)) in lc.iter().zip(pc.iter()).enumerate() {
        assert_eq!(l, p, "row {} differs for: {}", i, sql);
    }
}

#[test]
fn test_schema_matches_parquet() {
    if !data_available() {
        return;
    }
    let (l, p) = (lance_ctx(), parquet_ctx());
    for t in &TABLES {
        let ls = l
            .table_schema(t)
            .unwrap_or_else(|| panic!("no lance schema {}", t));
        let ps = p
            .table_schema(t)
            .unwrap_or_else(|| panic!("no parquet schema {}", t));
        assert_eq!(
            ls.fields().len(),
            ps.fields().len(),
            "{}: column count differs",
            t
        );
        for (lf, pf) in ls.fields().iter().zip(ps.fields().iter()) {
            assert_eq!(lf.name(), pf.name(), "{}: column name differs", t);
            assert_eq!(
                lf.data_type(),
                pf.data_type(),
                "{}: type of {} differs",
                t,
                lf.name()
            );
        }
    }
}

#[tokio::test]
async fn test_row_counts_match_parquet() {
    if !data_available() {
        return;
    }
    for t in &TABLES {
        assert_same(&format!("SELECT COUNT(*) AS n FROM {}", t)).await;
    }
}

#[tokio::test]
async fn test_projection_returns_requested_columns() {
    if !data_available() {
        return;
    }
    // Column order follows the SELECT list, not the table schema, which is the
    // contract the Lance projection pushdown has to honour.
    let ctx = lance_ctx();
    let r = ctx
        .sql("SELECT o_orderstatus, o_orderkey FROM orders ORDER BY o_orderkey LIMIT 5")
        .await
        .expect("query failed");
    let schema = r.batches[0].schema();
    assert_eq!(schema.fields().len(), 2);
    assert_eq!(schema.field(0).name(), "o_orderstatus");
    assert_eq!(schema.field(1).name(), "o_orderkey");
    assert_eq!(r.row_count, 5);
}

#[tokio::test]
async fn test_filter_and_aggregate_match_parquet() {
    if !data_available() {
        return;
    }
    assert_same(
        "SELECT l_returnflag, l_linestatus, COUNT(*) AS c, SUM(l_quantity) AS q \
         FROM lineitem WHERE l_shipdate <= DATE '1998-09-02' \
         GROUP BY l_returnflag, l_linestatus ORDER BY l_returnflag, l_linestatus",
    )
    .await;
}

#[tokio::test]
async fn test_join_matches_parquet() {
    if !data_available() {
        return;
    }
    assert_same(
        "SELECT n_name, COUNT(*) AS c FROM customer JOIN nation ON c_nationkey = n_nationkey \
         GROUP BY n_name ORDER BY n_name",
    )
    .await;
}

#[tokio::test]
async fn test_tpch_q01_matches_parquet() {
    if !data_available() {
        return;
    }
    assert_same(query_engine::tpch::get_query(1).expect("Q01")).await;
}

#[tokio::test]
async fn test_tpch_q03_matches_parquet() {
    if !data_available() {
        return;
    }
    assert_same(query_engine::tpch::get_query(3).expect("Q03")).await;
}

#[tokio::test]
async fn test_tpch_q05_matches_parquet() {
    if !data_available() {
        return;
    }
    assert_same(query_engine::tpch::get_query(5).expect("Q05")).await;
}

// ---------------------------------------------------------------------------
// Filter pushdown into Lance.
//
// THE FAILURE MODE THIS GUARDS: in lance 0.23.2 a double-quoted identifier
// parses as a STRING LITERAL, so `"category" = 'x'` is a constant FALSE that
// matches NOTHING and reports no error. A pushdown bug of that shape does not
// crash and does not slow anything down — it silently returns zero rows. So the
// gate is not "does the query still run", it is "are the rows IDENTICAL with
// pushdown on and off", including a predicate that should match nothing (which
// must match nothing for the RIGHT reason) and one that should match
// everything (which a constant-FALSE bug would empty out).
// ---------------------------------------------------------------------------

/// Same tables, pushdown explicitly disabled: the no-pushdown arm of the A/B.
fn lance_ctx_no_pushdown() -> ExecutionContext {
    use query_engine::storage::LanceTable;
    let mut ctx = ExecutionContext::new();
    for t in &TABLES {
        let table = LanceTable::try_new(lance_dir().join(format!("{}.lance", t)))
            .unwrap_or_else(|e| panic!("LanceTable::try_new({}) failed: {}", t, e))
            .with_filter_pushdown(false);
        ctx.register_table_provider(*t, std::sync::Arc::new(table));
    }
    ctx
}

/// The predicates the A/B runs. Chosen to cover every renderer branch AND the
/// two boundary cases a silent constant-FALSE would hide behind.
const PUSHDOWN_CASES: [(&str, &str); 12] = [
    (
        "date <=, the TPC-H Q01 shape",
        "SELECT COUNT(*) AS n, SUM(l_quantity) AS q FROM lineitem \
         WHERE l_shipdate <= DATE '1998-09-02'",
    ),
    (
        "string equality (the double-quote hazard)",
        "SELECT l_returnflag, COUNT(*) AS n FROM lineitem WHERE l_returnflag = 'R' \
         GROUP BY l_returnflag ORDER BY l_returnflag",
    ),
    (
        "MATCHES ZERO ROWS - must be empty for the right reason",
        "SELECT COUNT(*) AS n FROM lineitem WHERE l_returnflag = 'ZZZ_NO_SUCH_FLAG'",
    ),
    (
        "MATCHES EVERY ROW - a constant-FALSE bug empties this",
        "SELECT COUNT(*) AS n FROM lineitem WHERE l_orderkey > -1",
    ),
    (
        "date range, both ends",
        "SELECT COUNT(*) AS n FROM orders \
         WHERE o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01'",
    ),
    (
        "float comparison with an integer literal",
        "SELECT COUNT(*) AS n, SUM(l_extendedprice) AS p FROM lineitem WHERE l_quantity < 24",
    ),
    (
        "float comparison with float literals",
        "SELECT COUNT(*) AS n FROM lineitem WHERE l_discount >= 0.05 AND l_discount <= 0.07",
    ),
    (
        "IN list of strings",
        "SELECT l_shipmode, COUNT(*) AS n FROM lineitem \
         WHERE l_shipmode IN ('MAIL', 'SHIP') GROUP BY l_shipmode ORDER BY l_shipmode",
    ),
    (
        "column-to-column, same type",
        "SELECT COUNT(*) AS n FROM lineitem WHERE l_commitdate < l_receiptdate",
    ),
    (
        "OR of two pushable comparisons",
        "SELECT COUNT(*) AS n FROM orders WHERE o_orderstatus = 'F' OR o_totalprice > 100000",
    ),
    (
        "AND where only one side is renderable (LIKE is not)",
        "SELECT COUNT(*) AS n FROM part WHERE p_size = 15 AND p_type LIKE '%BRASS'",
    ),
    (
        "!= and BETWEEN together",
        "SELECT COUNT(*) AS n FROM lineitem \
         WHERE l_returnflag != 'N' AND l_linenumber BETWEEN 2 AND 4",
    ),
];

/// The mandatory verification: pushdown and no-pushdown must agree row for row.
///
/// Correctness of the pushdown is defined as "changes nothing but the time".
#[tokio::test]
async fn test_pushdown_matches_no_pushdown_row_for_row() {
    if !data_available() {
        return;
    }
    let (on, off) = (lance_ctx(), lance_ctx_no_pushdown());
    for (label, sql) in PUSHDOWN_CASES {
        let a = on
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("[{}] pushdown query failed: {}", label, e));
        let b = off
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("[{}] no-pushdown query failed: {}", label, e));
        assert_eq!(
            a.row_count, b.row_count,
            "[{}] row count differs with pushdown on/off: {}",
            label, sql
        );
        let (ca, cb) = (to_cells(&a.batches), to_cells(&b.batches));
        assert_eq!(
            ca, cb,
            "[{}] cells differ with pushdown on/off: {}",
            label, sql
        );
    }
}

/// The two boundary cases deserve their own assertions on the ABSOLUTE answer,
/// not just on agreement. Two identical wrong answers agree perfectly.
#[tokio::test]
async fn test_pushdown_boundary_predicates_have_the_right_absolute_answer() {
    if !data_available() {
        return;
    }
    let ctx = lance_ctx();

    let zero = ctx
        .sql("SELECT COUNT(*) AS n FROM lineitem WHERE l_returnflag = 'ZZZ_NO_SUCH_FLAG'")
        .await
        .expect("query failed");
    assert_eq!(count_of(&zero), 0, "a predicate matching nothing must be 0");

    // The all-rows case is the one a constant-FALSE pushdown bug destroys, and
    // it is compared against the row count Lance reports from metadata.
    let all = ctx
        .sql("SELECT COUNT(*) AS n FROM lineitem WHERE l_orderkey > -1")
        .await
        .expect("query failed");
    let total = ctx
        .sql("SELECT COUNT(*) AS n FROM lineitem")
        .await
        .expect("query failed");
    assert_eq!(
        count_of(&all),
        count_of(&total),
        "a predicate matching every row must not lose any"
    );
    assert!(count_of(&total) > 0, "fixture must not be empty");
}

fn count_of(r: &query_engine::execution::QueryResult) -> i64 {
    r.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("COUNT(*) is Int64")
        .value(0)
}

/// TPC-H is all narrow scalar columns, so `pushdown_pays` must DECLINE every
/// one of these. Pushing them unconditionally cost the SF=10 suite 18%
/// (7.96s -> 9.40s), which is the regression this asserts cannot come back.
#[tokio::test]
async fn test_pushdown_declines_on_narrow_tables() {
    if !data_available() {
        return;
    }
    use query_engine::storage::LanceTable;
    let table = std::sync::Arc::new(
        LanceTable::try_new(lance_dir().join("lineitem.lance")).expect("open lineitem"),
    );
    let mut ctx = ExecutionContext::new();
    ctx.register_table_provider("lineitem", table.clone());

    for sql in [
        "SELECT COUNT(*) AS n FROM lineitem WHERE l_shipdate <= DATE '1998-09-02'",
        "SELECT COUNT(*) AS n FROM lineitem WHERE l_orderkey = 1",
        "SELECT COUNT(*) AS n FROM lineitem WHERE l_returnflag = 'R'",
    ] {
        ctx.sql(sql).await.expect("query failed");
    }
    assert_eq!(
        table.pushed_filter_count(),
        0,
        "no TPC-H column is worth late-materializing; pushing here is a measured 18% regression"
    );
}

/// The case pushdown exists for: a table with an embedding column, filtered by
/// a selective scalar. An A/B that agrees because the pushdown never fires
/// proves nothing, so this asserts the predicate really reached Lance.
#[tokio::test]
async fn test_pushdown_fires_on_a_wide_payload_with_a_selective_filter() {
    let Some(path) = vectors_path() else { return };
    use query_engine::storage::LanceTable;
    let table = std::sync::Arc::new(LanceTable::try_new(&path).expect("open vectors"));
    let mut ctx = ExecutionContext::new();
    ctx.register_table_provider("vectors", table.clone());

    assert_eq!(table.pushed_filter_count(), 0);
    // Wide column projected, filter on a different (integer, statistic-bearing)
    // column, ~0.05% selective: all three gate conditions hold.
    ctx.sql("SELECT id, embedding FROM vectors WHERE id < 100")
        .await
        .expect("query failed");
    assert!(
        table.pushed_filter_count() > 0,
        "the predicate never reached Lance: pushdown is dead code"
    );

    // Same shape, but the filter is on a STRING column with no statistics, so
    // selectivity is unknown. Unknown must mean "do not push" — that is the
    // slowest measured case (0.4x).
    let before = table.pushed_filter_count();
    ctx.sql("SELECT id, embedding FROM vectors WHERE category = 'books'")
        .await
        .expect("query failed");
    assert_eq!(
        table.pushed_filter_count(),
        before,
        "unknown selectivity must decline"
    );

    // Same shape, but not selective enough.
    ctx.sql("SELECT id, embedding FROM vectors WHERE id < 190000")
        .await
        .expect("query failed");
    assert_eq!(
        table.pushed_filter_count(),
        before,
        "a filter that keeps 95% of rows must decline"
    );

    // A selective conjunct next to an unestimatable one still pushes the half
    // Lance can use; the string half stays engine-side.
    ctx.sql("SELECT id, embedding FROM vectors WHERE id < 100 AND category = 'books'")
        .await
        .expect("query failed");
    assert_eq!(
        table.pushed_filter_count(),
        before + 1,
        "a mixed AND must still push its estimatable, selective half"
    );
}

/// Row-for-row A/B on the dataset where pushdown actually fires. This is the
/// verification that matters: a silent constant-FALSE would show up here as an
/// empty result on the pushdown arm and a full one on the other.
#[tokio::test]
async fn test_pushdown_matches_no_pushdown_on_a_wide_table() {
    let Some(path) = vectors_path() else { return };
    use query_engine::storage::LanceTable;

    let mut on = ExecutionContext::new();
    on.register_table_provider(
        "vectors",
        std::sync::Arc::new(LanceTable::try_new(&path).expect("open")),
    );
    let mut off = ExecutionContext::new();
    off.register_table_provider(
        "vectors",
        std::sync::Arc::new(
            LanceTable::try_new(&path)
                .expect("open")
                .with_filter_pushdown(false),
        ),
    );

    let cases: [(&str, &str); 6] = [
        (
            "selective int filter (pushes)",
            "SELECT id, category, price FROM vectors WHERE id < 100 ORDER BY id",
        ),
        (
            "MATCHES ZERO ROWS",
            "SELECT COUNT(*) AS n FROM vectors WHERE id < -5",
        ),
        (
            "MATCHES EVERY ROW",
            "SELECT COUNT(*) AS n FROM vectors WHERE id >= 0",
        ),
        (
            "IN list",
            "SELECT id, category FROM vectors WHERE id IN (1, 7, 99) ORDER BY id",
        ),
        (
            "BETWEEN",
            "SELECT id, price FROM vectors WHERE id BETWEEN 10 AND 40 ORDER BY id",
        ),
        (
            "AND with a string conjunct the whitelist keeps engine-side",
            "SELECT id, category FROM vectors WHERE id < 500 AND category = 'books' ORDER BY id",
        ),
    ];

    for (label, sql) in cases {
        let a = on
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("[{}] pushdown failed: {}", label, e));
        let b = off
            .sql(sql)
            .await
            .unwrap_or_else(|e| panic!("[{}] no-pushdown failed: {}", label, e));
        assert_eq!(a.row_count, b.row_count, "[{}] row count differs", label);
        assert_eq!(
            to_cells(&a.batches),
            to_cells(&b.batches),
            "[{}] cells differ",
            label
        );
    }

    // Absolute answers for the boundary cases: two identical wrong answers
    // agree perfectly, so agreement alone is not enough.
    let zero = on
        .sql("SELECT COUNT(*) AS n FROM vectors WHERE id < -5")
        .await
        .unwrap();
    assert_eq!(count_of(&zero), 0);
    let all = on
        .sql("SELECT COUNT(*) AS n FROM vectors WHERE id >= 0")
        .await
        .unwrap();
    let total = on.sql("SELECT COUNT(*) AS n FROM vectors").await.unwrap();
    assert_eq!(count_of(&all), count_of(&total));
    assert!(count_of(&total) > 0);
}

fn vectors_path() -> Option<PathBuf> {
    let p = PathBuf::from("data/vectors.lance");
    if !p.exists() {
        eprintln!("SKIP: data/vectors.lance missing");
        return None;
    }
    Some(p)
}

/// Pushdown must not change what the Parquet oracle says either.
#[tokio::test]
async fn test_pushdown_still_matches_parquet() {
    if !data_available() {
        return;
    }
    for (_, sql) in PUSHDOWN_CASES {
        assert_same(sql).await;
    }
}

// ---------------------------------------------------------------------------
// Nested column types (struct / list), the shape real LanceDB tables have.
//
// Fixture: `.venv/bin/python scripts/lance_nested_gen.py --out data/nested.lance`
// ---------------------------------------------------------------------------

fn nested_path() -> PathBuf {
    PathBuf::from("data/nested.lance")
}

fn nested_ctx() -> Option<ExecutionContext> {
    if !nested_path().join("_versions").exists() && !nested_path().exists() {
        eprintln!("SKIP: data/nested.lance missing (run scripts/lance_nested_gen.py)");
        return None;
    }
    let mut ctx = ExecutionContext::new();
    match ctx.register_lance("t", nested_path()) {
        Ok(()) => Some(ctx),
        Err(e) => panic!("register_lance(nested) failed: {}", e),
    }
}

#[tokio::test]
async fn test_nested_select_star_carries_struct_and_list() {
    let Some(ctx) = nested_ctx() else { return };
    let r = ctx
        .sql("SELECT * FROM t ORDER BY id")
        .await
        .expect("SELECT * over nested columns must work");
    assert_eq!(r.row_count, 12);
    let schema = r.batches[0].schema();
    let by_name = |n: &str| {
        schema
            .fields()
            .iter()
            .find(|f| f.name().ends_with(n))
            .unwrap_or_else(|| panic!("column {} missing from SELECT *", n))
            .data_type()
            .clone()
    };
    assert!(matches!(
        by_name("meta"),
        arrow::datatypes::DataType::Struct(_)
    ));
    assert!(matches!(
        by_name("deep"),
        arrow::datatypes::DataType::Struct(_)
    ));
    assert!(matches!(
        by_name("tags"),
        arrow::datatypes::DataType::List(_)
    ));
    assert!(matches!(
        by_name("vec"),
        arrow::datatypes::DataType::FixedSizeList(_, 4)
    ));
}

#[tokio::test]
async fn test_nested_projection_filter_and_sort() {
    let Some(ctx) = nested_ctx() else { return };
    // A struct rides along through Filter (arrow `filter` kernel), Sort
    // (`take`) and Limit while a scalar column does the actual work.
    let r = ctx
        .sql("SELECT id, meta, deep FROM t WHERE id > 9 ORDER BY id DESC LIMIT 2")
        .await
        .expect("struct passthrough must survive filter+sort+limit");
    assert_eq!(r.row_count, 2);
    assert_eq!(r.batches[0].num_columns(), 3);
    let ids = r.batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .expect("id is Int64");
    assert_eq!(ids.value(0), 12);
    assert_eq!(ids.value(1), 11);
    // The struct must arrive as a real StructArray, not a stringified stand-in.
    let meta = r.batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StructArray>()
        .expect("meta must still be a StructArray after the pipeline");
    assert_eq!(meta.num_columns(), 3);
}

#[tokio::test]
async fn test_nested_join_and_group_by_scalar_columns() {
    let Some(ctx) = nested_ctx() else { return };
    // Grouping by a scalar while a struct exists on the table is fine; the
    // struct simply is not part of the key.
    let r = ctx
        .sql("SELECT COUNT(*) AS n FROM t WHERE id <= 6")
        .await
        .expect("aggregate over a table with nested columns");
    assert_eq!(r.row_count, 1);
}

/// Every operation that genuinely cannot work on a nested value must fail with
/// a message that NAMES THE COLUMN. A silent wrong answer is the failure mode
/// this whole type surface exists to prevent.
#[tokio::test]
async fn test_nested_unsupported_operations_name_the_column() {
    let Some(ctx) = nested_ctx() else { return };
    let cases: [(&str, &str); 6] = [
        ("SELECT DISTINCT * FROM t", "DISTINCT"),
        ("SELECT meta FROM t GROUP BY meta", "GROUP BY"),
        ("SELECT id FROM t ORDER BY meta", "ORDER BY"),
        ("SELECT id FROM t WHERE meta = 1", "meta"),
        ("SELECT SUM(tags) FROM t", "SUM"),
        ("SELECT * FROM t UNION SELECT * FROM t", "UNION"),
    ];
    for (sql, needle) in cases {
        let err = ctx
            .sql(sql)
            .await
            .err()
            .unwrap_or_else(|| panic!("expected `{}` to be rejected, it succeeded", sql));
        let msg = err.to_string();
        assert!(
            msg.contains(needle),
            "`{}` must mention `{}`, got: {}",
            sql,
            needle,
            msg
        );
        assert!(
            msg.contains("meta") || msg.contains("tags"),
            "`{}` must name the offending column, got: {}",
            sql,
            msg
        );
    }
}

/// Field access is deliberately NOT implemented. The error must say that,
/// rather than "column meta.source not found", which points at the wrong thing.
#[tokio::test]
async fn test_struct_field_access_says_it_is_unimplemented() {
    let Some(ctx) = nested_ctx() else { return };
    let err = ctx
        .sql("SELECT meta.source FROM t")
        .await
        .expect_err("field access is not implemented");
    let msg = err.to_string();
    assert!(msg.contains("field access"), "got: {}", msg);
    assert!(msg.contains("meta"), "got: {}", msg);
    assert!(!msg.contains("not found"), "misleading message: {}", msg);
}

/// UNION ALL only concatenates, so it is legal over nested columns even though
/// plain UNION (which de-duplicates on every column) is not.
#[tokio::test]
async fn test_union_all_is_allowed_over_nested_columns() {
    let Some(ctx) = nested_ctx() else { return };
    let r = ctx
        .sql("SELECT id, meta FROM t UNION ALL SELECT id, meta FROM t")
        .await
        .expect("UNION ALL must not require an ordering on the nested column");
    assert_eq!(r.row_count, 24);
}

// ---------------------------------------------------------------------------
// Versioning and time travel — the property that distinguishes Lance from a
// directory of Parquet files.
//
// Fixture: `.venv/bin/python scripts/lance_versions_gen.py`
//   v1 create    ids 1..3   3 rows
//   v2 append    ids 4..5   5 rows
//   v3 overwrite id 9       1 row
// The counts 3/5/1 are mutually distinct and v3 is SMALLER than v1, so neither
// a "reads the latest" nor a "reads the biggest" bug can pass by coincidence.
// ---------------------------------------------------------------------------

fn versioned_path() -> Option<PathBuf> {
    let p = PathBuf::from("data/versioned.lance");
    if !p.exists() {
        eprintln!("SKIP: data/versioned.lance missing (run scripts/lance_versions_gen.py)");
        return None;
    }
    Some(p)
}

#[test]
fn test_list_versions() {
    let Some(p) = versioned_path() else { return };
    use query_engine::storage::LanceTable;
    let versions = LanceTable::list_versions(&p).expect("list_versions");
    assert_eq!(
        versions.iter().map(|(v, _)| *v).collect::<Vec<_>>(),
        vec![1, 2, 3],
        "versions must come back ascending and complete"
    );
    for (_, ts) in &versions {
        assert!(ts.contains('T'), "timestamp should be RFC-3339, got {}", ts);
    }
}

#[tokio::test]
async fn test_time_travel_reads_each_version() {
    let Some(p) = versioned_path() else { return };
    // 3 rows created, 5 after the append, 1 after the overwrite.
    for (version, expected) in [(1u64, 3i64), (2, 5), (3, 1)] {
        let mut ctx = ExecutionContext::new();
        ctx.register_lance_version("t", &p, version)
            .unwrap_or_else(|e| panic!("register v{} failed: {}", version, e));
        let r = ctx
            .sql("SELECT COUNT(*) AS n FROM t")
            .await
            .expect("query failed");
        assert_eq!(
            count_of(&r),
            expected,
            "version {} should have {} rows",
            version,
            expected
        );
    }

    // No version means latest, which here is the SMALLEST version — so a reader
    // that confuses "latest" with "most rows" fails.
    let mut ctx = ExecutionContext::new();
    ctx.register_lance("t", &p).expect("register latest");
    let r = ctx
        .sql("SELECT COUNT(*) AS n FROM t")
        .await
        .expect("query failed");
    assert_eq!(count_of(&r), 1, "latest is v3, which has 1 row");
}

#[tokio::test]
async fn test_two_versions_of_one_path_join_in_one_query() {
    let Some(p) = versioned_path() else { return };
    // The point of time travel: diff two snapshots. v2 has ids 1..5, v1 has
    // 1..3, so the rows added between them are exactly {4, 5}.
    let mut ctx = ExecutionContext::new();
    ctx.register_lance_version("t_v1", &p, 1).expect("v1");
    ctx.register_lance_version("t_v2", &p, 2).expect("v2");
    let r = ctx
        .sql("SELECT id FROM t_v2 WHERE id NOT IN (SELECT id FROM t_v1) ORDER BY id")
        .await
        .expect("cross-version query failed");
    assert_eq!(to_cells(&r.batches), vec!["4".to_string(), "5".to_string()]);
}

#[test]
fn test_version_accessor_reports_the_checked_out_version() {
    let Some(p) = versioned_path() else { return };
    use query_engine::storage::LanceTable;
    assert_eq!(LanceTable::try_new_at_version(&p, 1).unwrap().version(), 1);
    assert_eq!(LanceTable::try_new_at_version(&p, 2).unwrap().version(), 2);
    assert_eq!(LanceTable::try_new(&p).unwrap().version(), 3);
}

/// An unknown version must FAIL. Silently serving the latest instead would
/// answer a question about the past with today's data — the worst possible
/// outcome for a feature whose whole purpose is reading history.
#[test]
fn test_unknown_version_is_an_error_not_a_fallback() {
    let Some(p) = versioned_path() else { return };
    use query_engine::storage::LanceTable;
    let err = LanceTable::try_new_at_version(&p, 999).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("999"), "error must name the version: {}", msg);
}

// ---------------------------------------------------------------------------
// Writing Lance datasets from Rust.
//
// These build their own fixtures, so unlike the read tests above they need no
// generated data and no Python at all — which is the point of the write path
// existing. A format you can only read is half-supported.
// ---------------------------------------------------------------------------

fn ints(name: &str, vals: &[i64]) -> (RecordBatch, arrow::datatypes::SchemaRef) {
    use arrow::datatypes::{DataType, Field, Schema};
    let schema = std::sync::Arc::new(Schema::new(vec![Field::new(name, DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![std::sync::Arc::new(arrow::array::Int64Array::from(
            vals.to_vec(),
        ))],
    )
    .expect("batch");
    (batch, schema)
}

#[tokio::test]
async fn test_write_batches_creates_a_readable_dataset() {
    use query_engine::storage::{lance_write, LanceWriteMode};
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("t.lance");

    let (batch, schema) = ints("id", &[1, 2, 3, 4]);
    let r = lance_write::write_batches(vec![batch], schema, &path, LanceWriteMode::Create)
        .expect("write failed");
    assert_eq!(r.rows, 4);
    assert_eq!(r.version, 1, "a fresh dataset starts at version 1");

    let mut ctx = ExecutionContext::new();
    ctx.register_lance("t", &path).expect("register");
    let out = ctx
        .sql("SELECT id FROM t ORDER BY id")
        .await
        .expect("query failed");
    assert_eq!(
        to_cells(&out.batches),
        vec![
            "1".to_string(),
            "2".to_string(),
            "3".to_string(),
            "4".to_string()
        ]
    );
}

/// The Phase-3 promise, now provable without Python: append creates a new
/// version and the OLD one still reads its own, smaller contents.
#[tokio::test]
async fn test_append_increments_version_and_history_survives() {
    use query_engine::storage::{lance_write, LanceTable, LanceWriteMode};
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("t.lance");

    let (b1, schema) = ints("id", &[1, 2, 3]);
    let v1 = lance_write::write_batches(vec![b1], schema.clone(), &path, LanceWriteMode::Create)
        .expect("create");
    assert_eq!((v1.rows, v1.version), (3, 1));

    let (b2, _) = ints("id", &[4, 5]);
    let v2 = lance_write::write_batches(vec![b2], schema.clone(), &path, LanceWriteMode::Append)
        .expect("append");
    assert_eq!(
        (v2.rows, v2.version),
        (5, 2),
        "append must add rows AND advance the version"
    );

    // An overwrite is a new version, not a delete: v1 and v2 stay readable.
    let (b3, _) = ints("id", &[9]);
    let v3 = lance_write::write_batches(vec![b3], schema, &path, LanceWriteMode::Overwrite)
        .expect("overwrite");
    assert_eq!((v3.rows, v3.version), (1, 3));

    for (version, expected) in [(1u64, 3usize), (2, 5), (3, 1)] {
        let t = LanceTable::try_new_at_version(&path, version).expect("checkout");
        assert_eq!(t.num_rows(), expected, "version {}", version);
        assert_eq!(t.version(), version);
    }
    assert_eq!(
        LanceTable::list_versions(&path).unwrap().len(),
        3,
        "three writes, three versions"
    );
}

#[test]
fn test_write_modes_refuse_the_wrong_situation() {
    use query_engine::storage::{lance_write, LanceWriteMode};
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("t.lance");
    let (b, schema) = ints("id", &[1]);

    // Append with nothing there: a typo'd path must not silently create.
    let err = lance_write::write_batches(
        vec![b.clone()],
        schema.clone(),
        &path,
        LanceWriteMode::Append,
    )
    .unwrap_err();
    assert!(err.to_string().contains("cannot append"), "{}", err);

    lance_write::write_batches(
        vec![b.clone()],
        schema.clone(),
        &path,
        LanceWriteMode::Create,
    )
    .expect("create");

    // Create over an existing dataset must not clobber it.
    let err = lance_write::write_batches(vec![b], schema.clone(), &path, LanceWriteMode::Create)
        .unwrap_err();
    assert!(err.to_string().contains("already exists"), "{}", err);

    // Zero batches almost always means "the source query returned nothing and
    // nobody noticed", so it is refused rather than silently written.
    let err =
        lance_write::write_batches(vec![], schema, &path, LanceWriteMode::Overwrite).unwrap_err();
    assert!(err.to_string().contains("zero batches"), "{}", err);
}

/// A Rust Parquet -> Lance conversion must produce the same rows the engine's
/// own Parquet reader sees. This replaces `scripts/lance_convert.py` for the
/// purposes of the test suite.
#[tokio::test]
async fn test_parquet_to_lance_conversion_matches_the_parquet_source() {
    if !data_available() {
        return;
    }
    use query_engine::storage::{lance_write, LanceWriteMode};
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("orders.lance");

    let src = parquet_dir().join("orders.parquet");
    let r = lance_write::write_from_parquet(&src, &path, LanceWriteMode::Create).expect("convert");
    assert_eq!(r.rows, 1500);
    assert_eq!(r.version, 1);

    let sql = "SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate \
               FROM orders ORDER BY o_orderkey LIMIT 50";

    let mut lance_ctx = ExecutionContext::new();
    lance_ctx.register_lance("orders", &path).expect("register");
    let a = lance_ctx.sql(sql).await.expect("lance query");

    let mut pq_ctx = ExecutionContext::new();
    pq_ctx.register_parquet("orders", &src).expect("register");
    let b = pq_ctx.sql(sql).await.expect("parquet query");

    assert_eq!(a.row_count, b.row_count);
    assert_eq!(
        to_cells(&a.batches),
        to_cells(&b.batches),
        "the Rust converter must be lossless"
    );
}

#[test]
fn test_vector_index_refuses_a_non_vector_column() {
    use query_engine::planner::vector_types::VectorMetric;
    use query_engine::storage::{lance_write, LanceWriteMode};
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("t.lance");
    let (b, schema) = ints("id", &[1, 2, 3]);
    lance_write::write_batches(vec![b], schema, &path, LanceWriteMode::Create).expect("create");

    // An IVF_PQ index over an Int64 column is meaningless; say so by name.
    let err = lance_write::create_vector_index(&path, "id", VectorMetric::L2, None, None, false)
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("id"), "must name the column: {}", msg);
    assert!(msg.contains("fixed-size list"), "{}", msg);

    // A column that is not there at all gets the column list, not a panic.
    let err = lance_write::create_vector_index(&path, "nope", VectorMetric::L2, None, None, false)
        .unwrap_err();
    assert!(err.to_string().contains("nope"), "{}", err);
}

#[test]
fn test_missing_dataset_is_a_clean_error() {
    let mut ctx = ExecutionContext::new();
    let err = ctx
        .register_lance("nope", Path::new("data/does-not-exist.lance"))
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.contains("does-not-exist.lance"),
        "error should name the path, got: {}",
        msg
    );
}
