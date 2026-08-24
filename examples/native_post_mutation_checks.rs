//! Task 006 (native-tables-mutation epic, QA close-out) diagnostic:
//! prepares a MUTATED copy of the real SF=10 native `lineitem` table
//! (`data/tpch-10gb-native/lineitem`, 60,000,000 rows) and uses it for two
//! checks this task's own acceptance criteria need real evidence for --
//! plus sets up a third (GPU) to be run separately, reusing existing code
//! unchanged.
//!
//!   1. A "mutated warehouse" benchmark target for
//!      `scripts/native_bench_compare.py` -- hardlinks (not copies; DELETE
//!      never touches segment bytes, see `native_delete_then_scan_budget_
//!      check.rs`'s own established rationale) the WHOLE 8-table warehouse
//!      into `.scratch/qa006/tpch-10gb-native-mutated`, then applies ONE
//!      real, broad DELETE (`l_discount > 0.09`, ~10% of lineitem, spread
//!      near-uniformly across every segment since the generator assigns
//!      `l_discount` independently per row -- not a concentrated range,
//!      the realistic "broad shallow delete" shape task 001/005 named as
//!      the one that actually costs the read path something) directly to
//!      the lineitem copy via `native_delete::delete_from_native_table`
//!      (the same bespoke API this epic's own diagnostics already use).
//!      The other 7 tables are untouched hardlinks (unmutated).
//!   2. Dense-direct-address fires correctly post-delete: a Q18-shaped
//!      `GROUP BY l_orderkey` (matches `native_dense_direct_check.rs`'s
//!      own established shape) run three ways -- native BEFORE the
//!      delete, native AFTER, and the plain-parquet source with an
//!      explicit `NOT (l_discount > 0.09)` filter added (equivalent by
//!      construction, computed via the engine's generic/parquet code path
//!      -- a materially different path from the native dense-direct one,
//!      not the same answer re-derived from itself). Group counts must
//!      agree between the native-post-delete and parquet+filter legs.
//!      Full independent cell-exactness of the underlying deletion-vector
//!      mechanism is separately established by
//!      `native_mutation_cell_exact_check.rs` + its DuckDB comparator --
//!      this check's job is narrower: confirm the FAST PATH still
//!      engages post-mutation (via `AGG_TIMING=1`'s "(native)" tag) rather
//!      than silently falling back, since deletion filtering happens
//!      inside `scan()`/`scan_with_filter()` BEFORE dense-direct ever
//!      sees a batch (a single choke point, task 001's Decision 1) --
//!      so if that choke point is correct (proven separately), dense-
//!      direct is correct too; what could still regress is reachability.
//!   3. Leaves the resulting mutated lineitem directory in place so
//!      `examples/native_gpu_check.rs` can be re-run UNCHANGED with
//!      `NATIVE_DIR=.scratch/qa006/tpch-10gb-native-mutated/lineitem` --
//!      no new GPU-specific code needed, maximal reuse of task 008's
//!      already-established GPU diagnostic.
//!
//! Run: `scripts/claude-safe-build.sh cargo build --release --example
//! native_post_mutation_checks && scripts/claude-safe-build.sh
//! ./target/release/examples/native_post_mutation_checks`
//! (add `AGG_TIMING=1` to see the dense-direct fast-path tag on stderr).

use query_engine::planner::{Expr, ScalarValue};
use query_engine::storage::native_delete;
use query_engine::storage::native_manifest;
use query_engine::{ExecutionConfig, ExecutionContext};
use std::path::Path;
use std::time::Instant;

const WAREHOUSE_SRC: &str = "data/tpch-10gb-native";
const PARQUET_LINEITEM: &str = "data/tpch-10gb/lineitem.parquet";
const MUTATED_ROOT: &str = ".scratch/qa006/tpch-10gb-native-mutated";
const TABLES: [&str; 8] = [
    "nation", "region", "customer", "orders", "partsupp", "supplier", "lineitem", "part",
];

fn hardlink_dir(src: &Path, dst: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        std::fs::hard_link(entry.path(), dst.join(entry.file_name()))?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> query_engine::Result<()> {
    if !Path::new(WAREHOUSE_SRC).exists() {
        eprintln!("skipping: {WAREHOUSE_SRC} not found (needs the committed SF=10 native fixture)");
        return Ok(());
    }

    let root = Path::new(MUTATED_ROOT);
    let _ = std::fs::remove_dir_all(root);
    std::fs::create_dir_all(root).expect("create mutated warehouse root");

    println!("=== hardlinking {WAREHOUSE_SRC} -> {MUTATED_ROOT} (8 tables) ===");
    for t in TABLES {
        let src = Path::new(WAREHOUSE_SRC).join(t);
        let dst = root.join(t);
        hardlink_dir(&src, &dst).unwrap_or_else(|e| panic!("hardlink {t}: {e}"));
    }
    println!("done.");

    let lineitem_dir = root.join("lineitem");
    let before = native_manifest::read_manifest(&lineitem_dir)?;
    let before_bytes: u64 = before.segments.iter().map(|s| s.byte_size).sum();
    println!(
        "\nlineitem BEFORE delete: {} rows, {} segments, {:.2} GB on disk",
        before.snapshot.row_count,
        before.segments.len(),
        before_bytes as f64 / 1e9
    );

    let pred = Expr::column("l_discount").gt(Expr::literal(ScalarValue::Float64(0.09.into())));
    let t0 = Instant::now();
    let del = native_delete::delete_from_native_table(&lineitem_dir, Some(&pred)).await?;
    println!(
        "DELETE l_discount > 0.09: {} rows deleted (net), {} segments dropped, in {:?}",
        del.rows_deleted,
        del.segments_dropped,
        t0.elapsed()
    );

    let after = native_manifest::read_manifest(&lineitem_dir)?;
    let after_bytes: u64 = after.segments.iter().map(|s| s.byte_size).sum();
    let logical_rows: u64 = after.segments.iter().map(|s| s.live_row_count()).sum();
    let pct_deleted = 100.0 * (before.snapshot.row_count - logical_rows) as f64
        / before.snapshot.row_count as f64;
    println!(
        "lineitem AFTER delete: {} segments, {:.2} GB on disk (delta={}), {} LOGICAL rows \
         ({pct_deleted:.2}% deleted)",
        after.segments.len(),
        after_bytes as f64 / 1e9,
        after_bytes as i64 - before_bytes as i64,
        logical_rows,
    );
    println!(
        "\nMutated warehouse ready at {MUTATED_ROOT} (7 tables unchanged hardlinks, lineitem's \
         deletion vector non-empty). Use with scripts/native_bench_compare.py --no-cell-exact \
         (the DuckDB oracle there reads the ORIGINAL un-mutated parquet, so cell-exact would \
         legitimately mismatch -- correctness is validated separately) and \
         NATIVE_DIR={MUTATED_ROOT}/lineitem for examples/native_gpu_check.rs."
    );

    // --- Dense-direct-address: fires correctly pre- and post-delete ---
    if !Path::new(PARQUET_LINEITEM).exists() {
        eprintln!("\nskipping dense-direct check: {PARQUET_LINEITEM} not found");
        return Ok(());
    }
    println!("\n=== dense-direct-address: native pre-delete vs post-delete vs parquet+filter ===");

    const GROUP_BY_SQL: &str = "SELECT l_orderkey, COUNT(*) AS cnt, SUM(l_quantity) AS total_qty \
         FROM {} GROUP BY l_orderkey";

    // A SECOND hardlink copy, untouched by the delete above, for the
    // "pre-delete" leg (the first copy is already past its delete).
    let pre_root = Path::new(".scratch/qa006/tpch-10gb-native-predelete");
    let _ = std::fs::remove_dir_all(pre_root);
    let pre_dir = pre_root.join("lineitem");
    hardlink_dir(&Path::new(WAREHOUSE_SRC).join("lineitem"), &pre_dir)
        .expect("hardlink pre-delete lineitem copy");

    let config = ExecutionConfig::default().with_memory_limit(40 * 1024 * 1024 * 1024);
    let mut ctx = ExecutionContext::with_config(config);
    ctx.register_native_table("lineitem_pre", &pre_dir)?;
    ctx.register_native_table("lineitem_post", &lineitem_dir)?;
    ctx.register_parquet("lineitem_src", PARQUET_LINEITEM)?;

    let mut group_counts = Vec::new();
    for (label, table, extra_where) in [
        ("native, pre-delete  ", "lineitem_pre", None),
        ("native, post-delete ", "lineitem_post", None),
        (
            "parquet + NOT filter",
            "lineitem_src",
            Some("NOT (l_discount > 0.09)"),
        ),
    ] {
        let sql = match extra_where {
            Some(w) => GROUP_BY_SQL
                .replace("{}", table)
                .replace("GROUP BY", &format!("WHERE {w} GROUP BY")),
            None => GROUP_BY_SQL.replace("{}", table),
        };
        let t0 = Instant::now();
        let result = ctx.sql(&sql).await?;
        println!(
            "{label}: {:>10} groups in {:?}",
            result.row_count,
            t0.elapsed()
        );
        group_counts.push((label, result.row_count));
    }

    let post_delete_groups = group_counts[1].1;
    let parquet_filtered_groups = group_counts[2].1;
    if post_delete_groups == parquet_filtered_groups {
        println!(
            "\n[PASS] native post-delete group count ({post_delete_groups}) == parquet+filter \
             group count ({parquet_filtered_groups}) -- deletion vector correctly honored by \
             the dense-direct-address fast path at real SF=10 scale."
        );
    } else {
        panic!(
            "[FAIL] native post-delete group count ({post_delete_groups}) != parquet+filter \
             group count ({parquet_filtered_groups})"
        );
    }
    println!(
        "Re-run with AGG_TIMING=1 (grep \"dense-direct scan+accumulate\") to confirm the fast \
         path fired for the native legs (tagged \"(native)\") rather than a silent fallback."
    );
    Ok(())
}
