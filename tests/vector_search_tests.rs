//! Vector search integration tests.
//!
//! Requires `--features lance` AND `data/vectors.lance` (200,000 rows of real
//! all-MiniLM-L6-v2 embeddings with an IVF_PQ index). Every test skips cleanly
//! when the dataset is missing.
//!
//! Two oracles are used, and they check different things:
//!
//! * `.scratch/vector_gt.json` — exact top-k computed by GPU brute force in
//!   float64. Proves the engine's ARITHMETIC.
//! * the `expected_category` in that file — proves the feature actually WORKS
//!   semantically: a natural-language query about hiking boots must return
//!   footwear, not merely a numerically defensible ordering.
//!
//! # Why the assertions compare distances, not id lists
//!
//! The dataset contains duplicate product texts, so many rows have *identical*
//! embeddings and therefore identical distances — including pairs that straddle
//! the k boundary. Which of two rows at distance 0.065103258 lands at rank 10
//! is arbitrary in any implementation. Asserting on ids would make this suite
//! fail for a reason that is not a bug, so the assertions are on the distance
//! sequence, with ids allowed to differ wherever the distance ties.

#![cfg(feature = "lance")]

use query_engine::execution::{ExecutionConfig, VectorSearchMode};
use query_engine::ExecutionContext;
use std::path::{Path, PathBuf};

const K: usize = 10;
const TIE_EPS: f64 = 2e-6;

fn dataset() -> PathBuf {
    PathBuf::from("data/vectors.lance")
}

fn ground_truth_path() -> PathBuf {
    PathBuf::from(".scratch/vector_gt.json")
}

fn data_available() -> bool {
    let ok = dataset().join("_versions").exists() || dataset().exists();
    if !ok {
        eprintln!("SKIP: data/vectors.lance missing");
    }
    ok
}

fn ctx_with(mode: VectorSearchMode) -> Option<ExecutionContext> {
    if !data_available() {
        return None;
    }
    let config = ExecutionConfig {
        vector_search_mode: mode,
        ..ExecutionConfig::default()
    };
    let mut ctx = ExecutionContext::with_config(config);
    ctx.register_lance("vectors", dataset()).ok()?;
    Some(ctx)
}

/// One ground-truth query.
struct GtQuery {
    text: String,
    expected_category: String,
    vector: Vec<f64>,
    ids: Vec<i64>,
    distances: Vec<f64>,
}

fn load_ground_truth() -> Option<Vec<GtQuery>> {
    let path = ground_truth_path();
    if !Path::new(&path).exists() {
        eprintln!("SKIP: {} missing", path.display());
        return None;
    }
    let raw = std::fs::read_to_string(&path).ok()?;
    let json: serde_json::Value = serde_json::from_str(&raw).ok()?;
    let queries = json.get("queries")?.as_array()?;
    Some(
        queries
            .iter()
            .map(|q| GtQuery {
                text: q["query"].as_str().unwrap_or_default().to_string(),
                expected_category: q["expected_category"]
                    .as_str()
                    .unwrap_or_default()
                    .to_string(),
                vector: q["vector"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_f64().unwrap())
                    .collect(),
                ids: q["exact_top_k"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|e| e["id"].as_i64().unwrap())
                    .collect(),
                distances: q["exact_top_k"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|e| e["distance"].as_f64().unwrap())
                    .collect(),
            })
            .collect(),
    )
}

fn vector_literal(v: &[f64]) -> String {
    let mut s = String::with_capacity(v.len() * 11 + 2);
    s.push('[');
    for (i, x) in v.iter().enumerate() {
        if i > 0 {
            s.push_str(", ");
        }
        s.push_str(&format!("{:.6}", x));
    }
    s.push(']');
    s
}

/// SQL that returns the distance alongside the row.
///
/// NOTE: putting the distance in the SELECT list makes the projection a
/// COMPUTED one, which `VectorSearchPushdown` deliberately refuses to match, so
/// this shape always runs the exact path regardless of mode. That is a
/// documented limitation (graceful: slow, never wrong) and it is exactly why
/// the recall test below must use [`knn_ids`] instead — an earlier version of
/// this file used this shape for both modes and measured a meaningless recall
/// of 1.000 because the "indexed" run was never indexed.
fn sql_with_distance(v: &[f64], k: usize, where_clause: Option<&str>) -> String {
    let lit = vector_literal(v);
    let w = where_clause
        .map(|w| format!(" WHERE {}", w))
        .unwrap_or_default();
    format!(
        "SELECT id, category, cosine_distance(embedding, {lit}) AS dist \
         FROM vectors{w} ORDER BY cosine_distance(embedding, {lit}) LIMIT {k}"
    )
}

/// The canonical, pushdown-eligible shape: only bare columns in the SELECT.
fn sql_ids_only(v: &[f64], k: usize, where_clause: Option<&str>) -> String {
    let lit = vector_literal(v);
    let w = where_clause
        .map(|w| format!(" WHERE {}", w))
        .unwrap_or_default();
    format!(
        "SELECT id, category FROM vectors{w} \
         ORDER BY cosine_distance(embedding, {lit}) LIMIT {k}"
    )
}

async fn ids_and_categories(ctx: &ExecutionContext, sql: &str) -> (Vec<i64>, Vec<String>) {
    use arrow::array::{Int64Array, StringArray};
    let result = ctx.sql(sql).await.expect("vector search failed");
    let mut ids = Vec::new();
    let mut cats = Vec::new();
    for b in &result.batches {
        let i = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        for r in 0..b.num_rows() {
            ids.push(i.value(r));
            cats.push(c.value(r).to_string());
        }
    }
    (ids, cats)
}

/// Run a k-NN query, returning `(ids, categories, distances)`.
async fn knn(
    ctx: &ExecutionContext,
    v: &[f64],
    k: usize,
    where_clause: Option<&str>,
) -> (Vec<i64>, Vec<String>, Vec<f64>) {
    use arrow::array::{Float64Array, Int64Array, StringArray};

    let sql = sql_with_distance(v, k, where_clause);
    let result = ctx.sql(&sql).await.expect("vector search failed");

    let mut ids = Vec::new();
    let mut cats = Vec::new();
    let mut dist = Vec::new();
    for b in &result.batches {
        let i = b
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("id must be Int64");
        let c = b
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("category must be Utf8");
        let d = b
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("distance must be Float64");
        for r in 0..b.num_rows() {
            ids.push(i.value(r));
            cats.push(c.value(r).to_string());
            dist.push(d.value(r));
        }
    }
    (ids, cats, dist)
}

#[tokio::test]
async fn exact_path_matches_gpu_ground_truth() {
    let Some(ctx) = ctx_with(VectorSearchMode::Exact) else {
        return;
    };
    let Some(gt) = load_ground_truth() else {
        return;
    };

    for q in &gt {
        let (ids, _, dist) = knn(&ctx, &q.vector, K, None).await;
        assert_eq!(
            ids.len(),
            K,
            "query `{}` returned {} rows",
            q.text,
            ids.len()
        );
        for rank in 0..K {
            assert!(
                (dist[rank] - q.distances[rank]).abs() < TIE_EPS,
                "query `{}` rank {}: engine distance {} vs ground truth {} (ids {} vs {})",
                q.text,
                rank,
                dist[rank],
                q.distances[rank],
                ids[rank],
                q.ids[rank]
            );
        }
        // Sorted ascending, as ORDER BY says.
        for w in dist.windows(2) {
            assert!(w[0] <= w[1] + 1e-12, "results are not ordered: {:?}", dist);
        }
    }
}

#[tokio::test]
async fn exact_path_is_semantically_right() {
    // The point of the feature: a natural-language query must retrieve rows
    // from the category a human would expect. Ground-truth precision@10 is
    // 1.000, so anything less here is a real regression, not noise.
    let Some(ctx) = ctx_with(VectorSearchMode::Exact) else {
        return;
    };
    let Some(gt) = load_ground_truth() else {
        return;
    };

    for q in &gt {
        let (_, cats, _) = knn(&ctx, &q.vector, K, None).await;
        let hits = cats.iter().filter(|c| **c == q.expected_category).count();
        assert_eq!(
            hits, K,
            "query `{}` expected all {} results in `{}`, got {:?}",
            q.text, K, q.expected_category, cats
        );
    }
}

#[tokio::test]
async fn pushdown_fires_only_on_the_canonical_shape() {
    // Without this, a test could "measure" the indexed path while silently
    // running the exact one and report a recall of 1.000.
    let Some(ctx) = ctx_with(VectorSearchMode::Indexed) else {
        return;
    };
    let Some(gt) = load_ground_truth() else {
        return;
    };
    let v = &gt[0].vector;
    let lit = vector_literal(v);

    let fires = |sql: String| {
        let plan = ctx.optimized_plan(&sql).expect("planning failed");
        plan.to_string().contains("VectorSearch")
    };

    assert!(
        fires(sql_ids_only(v, K, None)),
        "canonical shape must push down"
    );
    assert!(
        fires(sql_ids_only(v, K, Some("category = 'footwear'"))),
        "a scalar WHERE must still push down (as a prefilter)"
    );
    assert!(
        !fires(sql_with_distance(v, K, None)),
        "a computed SELECT column must NOT push down"
    );
    assert!(
        !fires(format!(
            "SELECT id FROM vectors ORDER BY cosine_distance(embedding, {lit}) DESC LIMIT {K}"
        )),
        "DESC on a distance asks for the FURTHEST rows; must not push down"
    );
    assert!(
        !fires(format!(
            "SELECT id FROM vectors ORDER BY cosine_distance(embedding, {lit})"
        )),
        "no LIMIT is not a k-NN"
    );
    assert!(
        !fires(format!(
            "SELECT id FROM vectors ORDER BY cosine_distance(embedding, {lit}), id LIMIT {K}"
        )),
        "a tiebreaker sort key must not push down"
    );
}

#[tokio::test]
async fn indexed_path_returns_k_rows_of_the_right_category() {
    // The indexed path is APPROXIMATE by construction, so its ids are not
    // asserted against ground truth — measured recall@10 is 0.91, which is
    // exactly why it is not the default. What must hold regardless: it returns
    // k rows, ordered, and semantically on-target.
    let Some(ctx) = ctx_with(VectorSearchMode::Indexed) else {
        return;
    };
    let Some(gt) = load_ground_truth() else {
        return;
    };

    for q in &gt {
        let (ids, cats) = ids_and_categories(&ctx, &sql_ids_only(&q.vector, K, None)).await;
        assert_eq!(
            ids.len(),
            K,
            "query `{}` returned {} rows",
            q.text,
            ids.len()
        );
        let hits = cats.iter().filter(|c| **c == q.expected_category).count();
        assert!(
            hits >= 8,
            "query `{}` expected mostly `{}`, got {:?}",
            q.text,
            q.expected_category,
            cats
        );
    }
}

#[tokio::test]
async fn indexed_recall_is_measured_and_above_the_documented_floor() {
    // Guards the number quoted in CLAUDE.md. If a Lance upgrade or a config
    // change moves recall, this fails loudly instead of the docs going stale.
    let (Some(exact), Some(indexed)) = (
        ctx_with(VectorSearchMode::Exact),
        ctx_with(VectorSearchMode::Indexed),
    ) else {
        return;
    };
    let Some(gt) = load_ground_truth() else {
        return;
    };

    // Sanity: the "indexed" side must actually be indexed. Without this the
    // recall number is meaningless (see pushdown_fires_only_on_the_canonical_shape).
    assert!(
        indexed
            .optimized_plan(&sql_ids_only(&gt[0].vector, K, None))
            .unwrap()
            .to_string()
            .contains("VectorSearch"),
        "recall measurement requires the pushdown to fire"
    );

    let mut total = 0.0;
    for q in &gt {
        let (want, _) = ids_and_categories(&exact, &sql_ids_only(&q.vector, K, None)).await;
        let (got, _) = ids_and_categories(&indexed, &sql_ids_only(&q.vector, K, None)).await;
        let hits = got.iter().filter(|g| want.contains(g)).count();
        total += hits as f64 / K as f64;
    }
    let recall = total / gt.len() as f64;
    eprintln!("measured recall@{} of indexed vs exact: {:.3}", K, recall);
    assert!(
        recall >= 0.80,
        "recall@{} fell to {:.3}; CLAUDE.md documents 0.91",
        K,
        recall
    );
    assert!(
        recall < 1.0,
        "recall reached {:.3}: if the index has become exact, revisit the Exact \
         default in ExecutionConfig and the CLAUDE.md rationale",
        recall
    );
}

#[tokio::test]
async fn filtered_search_is_exact_in_both_modes() {
    // Lance 0.23.2's index prefilter is broken (it post-filters, returning 0
    // rows for a filter matching 40,000 rows), so `LanceTable::scan_knn` drops
    // to a flat Lance scan whenever a filter is present. Both modes must
    // therefore produce identical distances.
    let (Some(exact), Some(indexed)) = (
        ctx_with(VectorSearchMode::Exact),
        ctx_with(VectorSearchMode::Indexed),
    ) else {
        return;
    };
    let Some(gt) = load_ground_truth() else {
        return;
    };
    let filter = Some("category = 'footwear' AND price > 20");

    for q in gt.iter().take(4) {
        // Reference: the exact path, k + 20 deep, so every id the indexed path
        // could legitimately return (including rows tied at the k-th distance)
        // has a known exact distance.
        let (deep_ids, _, deep_d) = knn(&exact, &q.vector, K + 20, filter).await;
        assert_eq!(deep_ids.len(), K + 20);
        let cutoff = deep_d[K - 1];

        let (got, cats) = ids_and_categories(&indexed, &sql_ids_only(&q.vector, K, filter)).await;
        assert_eq!(
            got.len(),
            K,
            "indexed+filter returned {} rows for `{}` — Lance's broken index \
             prefilter would silently truncate here",
            got.len(),
            q.text
        );
        assert!(
            cats.iter().all(|c| c == "footwear"),
            "prefilter leaked: {:?}",
            cats
        );

        // Every returned row must be a genuine top-k member: its exact distance
        // is at or below the k-th exact distance (ties included).
        for id in &got {
            let pos = deep_ids.iter().position(|d| d == id).unwrap_or_else(|| {
                panic!(
                    "indexed returned id {} outside the exact top-{}",
                    id,
                    K + 20
                )
            });
            assert!(
                deep_d[pos] <= cutoff + TIE_EPS,
                "id {} has exact distance {} > cutoff {}: the filtered path is not exact",
                id,
                deep_d[pos],
                cutoff
            );
        }
    }
}

#[tokio::test]
async fn distance_functions_agree_on_ordering() {
    // cosine_distance ascending and cosine_similarity descending must rank the
    // same rows; dot_product on unit-normalized vectors must too. This is the
    // sign-convention contract the pushdown rule relies on.
    let Some(ctx) = ctx_with(VectorSearchMode::Exact) else {
        return;
    };
    let Some(gt) = load_ground_truth() else {
        return;
    };
    let q = &gt[0];
    let lit = vector_literal(&q.vector);

    use arrow::array::Int64Array;
    async fn ids_of(ctx: &ExecutionContext, sql: String) -> Vec<i64> {
        let r = ctx.sql(&sql).await.expect("query failed");
        let mut out = Vec::new();
        for b in &r.batches {
            let c = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..b.num_rows() {
                out.push(c.value(i));
            }
        }
        out
    }

    let by_distance = ids_of(
        &ctx,
        format!("SELECT id FROM vectors ORDER BY cosine_distance(embedding, {lit}) LIMIT 5"),
    )
    .await;
    let by_similarity = ids_of(
        &ctx,
        format!("SELECT id FROM vectors ORDER BY cosine_similarity(embedding, {lit}) DESC LIMIT 5"),
    )
    .await;
    let by_dot = ids_of(
        &ctx,
        format!("SELECT id FROM vectors ORDER BY dot_product(embedding, {lit}) DESC LIMIT 5"),
    )
    .await;

    assert_eq!(
        by_distance, by_similarity,
        "cosine distance/similarity disagree"
    );
    assert_eq!(
        by_distance, by_dot,
        "dot_product must rank unit vectors like cosine"
    );
}

#[tokio::test]
async fn vector_columns_reject_scalar_operations_by_name() {
    let Some(ctx) = ctx_with(VectorSearchMode::Exact) else {
        return;
    };
    for (sql, what) in [
        ("SELECT SUM(embedding) FROM vectors", "SUM"),
        (
            "SELECT embedding, COUNT(*) FROM vectors GROUP BY embedding",
            "GROUP BY",
        ),
        ("SELECT id FROM vectors WHERE embedding > 3", "operator"),
        (
            "SELECT id, embedding FROM vectors ORDER BY embedding LIMIT 1",
            "ORDER BY",
        ),
    ] {
        let err = ctx
            .sql(sql)
            .await
            .expect_err(&format!("`{}` must be rejected", sql))
            .to_string();
        assert!(
            err.contains("embedding"),
            "message must name the column: {}",
            err
        );
        assert!(err.contains(what), "message must name `{}`: {}", what, err);
    }
}

#[tokio::test]
async fn vector_column_survives_projection_and_select_star() {
    let Some(ctx) = ctx_with(VectorSearchMode::Exact) else {
        return;
    };
    use arrow::datatypes::DataType;

    let r = ctx
        .sql("SELECT embedding FROM vectors LIMIT 5")
        .await
        .expect("selecting a vector column must work");
    assert_eq!(r.row_count, 5);
    assert!(matches!(
        r.schema.field(0).data_type(),
        DataType::FixedSizeList(_, 384)
    ));

    let r = ctx
        .sql("SELECT * FROM vectors LIMIT 2")
        .await
        .expect("SELECT *");
    assert_eq!(r.row_count, 2);
    assert_eq!(r.schema.fields().len(), 5);
}

#[tokio::test]
async fn offset_is_honoured_on_both_paths() {
    let (Some(exact), Some(indexed)) = (
        ctx_with(VectorSearchMode::Exact),
        ctx_with(VectorSearchMode::Indexed),
    ) else {
        return;
    };
    let Some(gt) = load_ground_truth() else {
        return;
    };
    let lit = vector_literal(&gt[0].vector);
    // Bare columns only, so the indexed run really is indexed.
    let sql = format!(
        "SELECT id, category FROM vectors \
         ORDER BY cosine_distance(embedding, {lit}) LIMIT 3 OFFSET 2"
    );
    assert!(
        indexed
            .optimized_plan(&sql)
            .unwrap()
            .to_string()
            .contains("VectorSearch"),
        "OFFSET must not disable the pushdown"
    );

    // OFFSET is a MECHANICS contract, tested per path against that path's
    // OWN top-5: the window must be exactly its ranks 3..5. Demanding the
    // exact path's ranks from the INDEXED path would make an approximate
    // method's recall a hard equality — it held under lance 0.23's index by
    // luck and broke on lance 10's. Recall has its own gated test.
    for (name, ctx) in [("exact", &exact), ("indexed", &indexed)] {
        let (full, _) = ids_and_categories(ctx, &sql_ids_only(&gt[0].vector, 5, None)).await;
        let expected: Vec<i64> = full[2..5].to_vec();
        let (ids, _) = ids_and_categories(ctx, &sql).await;
        assert_eq!(ids.len(), 3, "{} returned {} rows", name, ids.len());
        assert_eq!(
            ids.iter().collect::<std::collections::HashSet<_>>(),
            expected.iter().collect::<std::collections::HashSet<_>>(),
            "{}: OFFSET 2 LIMIT 3 should be ranks 3-5 of its own top-5, got {:?} want {:?}",
            name,
            ids,
            expected
        );
    }
}
