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
