//! SQL-level pin for `hash-join-dictionary-semi-anti-fix` task 001: a
//! SEMI / ANTI join keyed on a native table's `Dictionary(Int32, Utf8)`
//! column, with the BUILD side on the LEFT (the output side), must answer
//! cell-for-cell identically to the same SQL over the plain Parquet source.
//!
//! Why this shape: `spill-join-correctness-3` task 004 found that the
//! in-memory `HashJoinExec` marked ONE build row per distinct matched key
//! on exactly this path (Dictionary keys -> no vectorized table -> generic
//! `probe_semi_anti_parallel` loop whose marking `break`-ed early), so a
//! `... WHERE l_shipmode IN (SELECT l_shipmode ...)` over a native table
//! returned one lineitem per ship mode instead of thousands. The planner
//! only builds from the RIGHT of a Semi/Anti when the left is estimated at
//! more than 2x the right (`build_right_for_left`, physical/planner.rs);
//! with a single-conjunct filter on BOTH sides the estimates are equal, so
//! the build stays LEFT — the buggy orientation. The unfiltered-left /
//! filtered-right variant is kept as the build-RIGHT control (it was never
//! wrong; probe-side output is decided per probe row).
//!
//! Every query asserts three things, because a comparison alone proves
//! little: (1) the join's own left input really delivers `l_shipmode` as
//! `Dictionary(Int32, Utf8)` arrays (a plain-Utf8 key would take the
//! vectorized path and never reach the defect); (2) the physical plan's
//! Semi/Anti join has the intended `build_right`; (3) the native answer is
//! byte-identical to the Parquet answer.
//!
//! Requires `data/tpch-10mb` (committed fixture).

use query_engine::physical::PhysicalOperator;
use query_engine::ExecutionContext;
use std::sync::Arc;

fn data_dir() -> String {
    format!("{}/data/tpch-10mb", env!("CARGO_MANIFEST_DIR"))
}

/// Cell-exact via the same rendering on both sides (the idiom
/// `tests/native_table_validation.rs` uses). Dictionary columns render
/// as their values, so a Dictionary-typed native result and a Utf8-typed
/// Parquet result compare on content, not encoding.
fn render(batches: &[arrow::record_batch::RecordBatch]) -> String {
    arrow::util::pretty::pretty_format_batches(batches)
        .map(|d| d.to_string())
        .unwrap_or_default()
}

/// `lineitem_src` (Parquet) and `lineitem_native` (CTAS copy in a private
/// tempdir, so parallel tests never share a native-table root).
async fn build_ctx() -> (ExecutionContext, tempfile::TempDir) {
    let tmp = tempfile::tempdir().expect("tempdir");
    let mut ctx = ExecutionContext::new().with_native_table_root(tmp.path().to_path_buf());
    ctx.register_parquet("lineitem_src", format!("{}/lineitem.parquet", data_dir()))
        .expect("register source lineitem parquet");
    let r = ctx
        .create_table_as_select("CREATE TABLE lineitem_native AS SELECT * FROM lineitem_src")
        .await
        .expect("CREATE TABLE ... AS SELECT must succeed");
    assert!(
        r.rows > 10_000,
        "fixture unexpectedly small: {} rows",
        r.rows
    );
    (ctx, tmp)
}

/// Debug renderings of every join operator in the physical plan.
fn collect_joins(op: &Arc<dyn PhysicalOperator>, out: &mut Vec<String>) {
    let d = format!("{:?}", op);
    if d.starts_with("SpillableHashJoinExec") || d.starts_with("HashJoinExec") {
        out.push(d);
    }
    for c in op.children() {
        collect_joins(&c, out);
    }
}

/// The Semi/Anti join operator of the plan for `sql`, if exactly one exists.
fn find_semi_anti_join(
    op: &Arc<dyn PhysicalOperator>,
    join_kind: &str,
    out: &mut Vec<Arc<dyn PhysicalOperator>>,
) {
    let d = format!("{:?}", op);
    if (d.starts_with("SpillableHashJoinExec") || d.starts_with("HashJoinExec"))
        && d.contains(join_kind)
    {
        out.push(op.clone());
    }
    for c in op.children() {
        find_semi_anti_join(&c, join_kind, out);
    }
}

/// The join's LEFT child (the build side when `build_right=false`) must
/// deliver `l_shipmode` as genuinely `Dictionary(Int32, Utf8)` arrays —
/// that is what `HashJoinExec` evaluates as its key. Probing the join's
/// own input is necessary: the provider's DECLARED catalog type is
/// deliberately the plain value type (`NativeTable::schema`'s doc
/// comment), and `sql()` results are decoded to it at the output boundary,
/// so neither the declared schema nor a top-level `SELECT l_shipmode`
/// witnesses what the operator sees.
async fn assert_join_left_input_shipmode_is_dictionary(
    ctx: &ExecutionContext,
    sql: &str,
    join_kind: &str,
) {
    use arrow::datatypes::DataType;
    use futures::TryStreamExt;
    let plan = ctx.physical_plan(sql).expect("physical plan");
    let mut joins = Vec::new();
    find_semi_anti_join(&plan, join_kind, &mut joins);
    assert_eq!(joins.len(), 1, "expected exactly one {join_kind} join");
    let left = joins[0].children()[0].clone();
    // Field names are relation-qualified inside the plan
    // (`lineitem_native.l_shipmode`); match on the unqualified name.
    let idx = left
        .schema()
        .fields()
        .iter()
        .position(|f| f.name() == "l_shipmode" || f.name().ends_with(".l_shipmode"))
        .expect("left input carries l_shipmode");
    let mut seen_rows = 0usize;
    for p in 0..left.output_partitions() {
        let stream = left.execute(p).await.expect("execute join left input");
        let batches: Vec<arrow::record_batch::RecordBatch> =
            stream.try_collect().await.expect("collect join left input");
        for b in batches.iter().filter(|b| b.num_rows() > 0) {
            seen_rows += b.num_rows();
            assert!(
                matches!(b.column(idx).data_type(), DataType::Dictionary(_, _)),
                "the join's left (build) input must carry Dictionary-typed l_shipmode, got {:?}",
                b.column(idx).data_type()
            );
        }
    }
    assert!(seen_rows > 0, "the join's left input must be non-empty");
}

/// The plan for `sql` over the native table must contain exactly one
/// Semi/Anti join, planned with the given `build_right`.
fn assert_join_orientation(ctx: &ExecutionContext, sql: &str, join_kind: &str, build_right: bool) {
    let plan = ctx.physical_plan(sql).expect("physical plan");
    let mut joins = Vec::new();
    collect_joins(&plan, &mut joins);
    let semi_anti: Vec<&String> = joins.iter().filter(|j| j.contains(join_kind)).collect();
    assert_eq!(
        semi_anti.len(),
        1,
        "expected exactly one {join_kind} join in the plan, found {joins:#?}"
    );
    let want = format!("build_right: {build_right}");
    assert!(
        semi_anti[0].contains(&want),
        "{join_kind} join must be planned with {want}: {}",
        semi_anti[0]
    );
}

async fn assert_native_matches_parquet(ctx: &ExecutionContext, sql_template: &str) {
    let src = ctx
        .sql(&sql_template.replace("{T}", "lineitem_src"))
        .await
        .expect("parquet query");
    let native = ctx
        .sql(&sql_template.replace("{T}", "lineitem_native"))
        .await
        .expect("native query");
    assert!(
        src.row_count > 0,
        "the reference query must not be empty, or the comparison proves nothing"
    );
    assert_eq!(
        render(&src.batches),
        render(&native.batches),
        "native answer differs from parquet answer for:\n{sql_template}"
    );
}

/// Build-LEFT (the defective orientation): a filter of fewer than three
/// AND-conjuncts on both the outer query and the subquery gets the
/// planner's flat 0.3 estimate on each side, so the left is not >2x the
/// right and the build stays left. The ANTI subquery excludes one ship
/// mode so the anti answer is non-empty.
const SEMI_BUILD_LEFT: &str = "SELECT l_shipmode, COUNT(*) AS n, SUM(l_orderkey) AS k \
     FROM {T} WHERE l_quantity > 25 \
     AND l_shipmode IN (SELECT l_shipmode FROM {T} WHERE l_discount > 0.05) \
     GROUP BY l_shipmode ORDER BY l_shipmode";

const ANTI_BUILD_LEFT: &str = "SELECT l_shipmode, COUNT(*) AS n, SUM(l_orderkey) AS k \
     FROM {T} WHERE l_quantity > 25 \
     AND l_shipmode NOT IN (SELECT l_shipmode FROM {T} WHERE l_discount > 0.05 AND l_shipmode <> 'TRUCK') \
     GROUP BY l_shipmode ORDER BY l_shipmode";

/// Build-RIGHT control: unfiltered outer (n rows) vs filtered subquery
/// (0.3n): the left is >2x the right, so the planner builds from the right.
const SEMI_BUILD_RIGHT: &str = "SELECT l_shipmode, COUNT(*) AS n, SUM(l_orderkey) AS k \
     FROM {T} \
     WHERE l_shipmode IN (SELECT l_shipmode FROM {T} WHERE l_discount > 0.05) \
     GROUP BY l_shipmode ORDER BY l_shipmode";

const ANTI_BUILD_RIGHT: &str = "SELECT l_shipmode, COUNT(*) AS n, SUM(l_orderkey) AS k \
     FROM {T} \
     WHERE l_shipmode NOT IN (SELECT l_shipmode FROM {T} WHERE l_discount > 0.05 AND l_shipmode <> 'TRUCK') \
     GROUP BY l_shipmode ORDER BY l_shipmode";

#[tokio::test]
async fn semi_join_on_dictionary_key_with_build_left_is_cell_exact() {
    let (ctx, _tmp) = build_ctx().await;
    let native_sql = SEMI_BUILD_LEFT.replace("{T}", "lineitem_native");
    assert_join_left_input_shipmode_is_dictionary(&ctx, &native_sql, "Semi").await;
    assert_join_orientation(&ctx, &native_sql, "Semi", false);
    assert_native_matches_parquet(&ctx, SEMI_BUILD_LEFT).await;
}

#[tokio::test]
async fn anti_join_on_dictionary_key_with_build_left_is_cell_exact() {
    let (ctx, _tmp) = build_ctx().await;
    let native_sql = ANTI_BUILD_LEFT.replace("{T}", "lineitem_native");
    assert_join_left_input_shipmode_is_dictionary(&ctx, &native_sql, "Anti").await;
    assert_join_orientation(&ctx, &native_sql, "Anti", false);
    assert_native_matches_parquet(&ctx, ANTI_BUILD_LEFT).await;
}

#[tokio::test]
async fn semi_join_on_dictionary_key_with_build_right_is_cell_exact() {
    let (ctx, _tmp) = build_ctx().await;
    let native_sql = SEMI_BUILD_RIGHT.replace("{T}", "lineitem_native");
    assert_join_left_input_shipmode_is_dictionary(&ctx, &native_sql, "Semi").await;
    assert_join_orientation(&ctx, &native_sql, "Semi", true);
    assert_native_matches_parquet(&ctx, SEMI_BUILD_RIGHT).await;
}

#[tokio::test]
async fn anti_join_on_dictionary_key_with_build_right_is_cell_exact() {
    let (ctx, _tmp) = build_ctx().await;
    let native_sql = ANTI_BUILD_RIGHT.replace("{T}", "lineitem_native");
    assert_join_left_input_shipmode_is_dictionary(&ctx, &native_sql, "Anti").await;
    assert_join_orientation(&ctx, &native_sql, "Anti", true);
    assert_native_matches_parquet(&ctx, ANTI_BUILD_RIGHT).await;
}
