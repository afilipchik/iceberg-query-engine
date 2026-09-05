//! query-ui epic: the query log endpoints (`/queries`, `/queries/{id}`,
//! `/stats`, `/tables`) and the embedded UI, exercised against in-process
//! `serve` nodes over real TCP. Same harness conventions as
//! `distributed_cluster.rs`: `data/tpch-1mb`, ephemeral ports, loud panics.

use arrow::array::Int64Array;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use arrow_flight::client::FlightClient;
use arrow_flight::decode::DecodedPayload;
use arrow_flight::FlightDescriptor;
use futures::TryStreamExt;
use query_engine::distributed::{http_client, spawn, ServeOptions, ServerHandle, TableLoader};
use query_engine::physical::operators::TableProvider;
use query_engine::ExecutionConfig;
use query_engine::ExecutionContext;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

/// A one-row table whose scan takes `delay`, so a test can observe a query
/// in the `running` state deterministically instead of racing a real query.
#[derive(Debug)]
struct SlowTable {
    schema: SchemaRef,
    delay: Duration,
}

impl TableProvider for SlowTable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn scan(&self, _projection: Option<&[usize]>) -> query_engine::Result<Vec<RecordBatch>> {
        std::thread::sleep(self.delay);
        Ok(vec![RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64]))],
        )
        .unwrap()])
    }
}

const TABLES: [&str; 8] = [
    "nation", "region", "part", "supplier", "partsupp", "customer", "orders", "lineitem",
];
const DATA: &str = "data/tpch-1mb";
const HTTP_TIMEOUT: Duration = Duration::from_secs(60);

fn isolate_env() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        std::env::remove_var("QE_ADVERTISE_ADDR");
        std::env::remove_var("QE_NODE_ID");
        std::env::remove_var("POD_IP");
    });
}

fn data_dir() -> String {
    format!("{}/{}", env!("CARGO_MANIFEST_DIR"), DATA)
}

fn tpch_loader() -> TableLoader {
    Box::new(|| {
        let mut ctx = ExecutionContext::new();
        for t in TABLES {
            ctx.register_parquet(t, &format!("{}/{t}.parquet", data_dir()))?;
        }
        ctx.register_table_provider(
            "slow",
            Arc::new(SlowTable {
                schema: Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)])),
                delay: Duration::from_millis(1500),
            }),
        );
        Ok(ctx)
    })
}

fn options(node_id: u64, query_log_size: usize) -> ServeOptions {
    ServeOptions {
        bind: "127.0.0.1:0".into(),
        node_id: Some(node_id),
        discovery_interval: Duration::from_millis(50),
        probe_timeout: Duration::from_millis(2000),
        flight_bind: Some("none".into()),
        query_log_size,
        ..Default::default()
    }
}

/// A loader whose context has a tiny memory budget, so joins spill.
fn spilling_loader(limit_bytes: usize, name: &'static str) -> TableLoader {
    Box::new(move || {
        let spill_path = format!("{}/target/test_spill/ui_{name}", env!("CARGO_MANIFEST_DIR"));
        let config = ExecutionConfig::new()
            .with_memory_limit(limit_bytes)
            .with_spill_path(spill_path.into());
        let mut ctx = ExecutionContext::with_config(config);
        for t in TABLES {
            ctx.register_parquet(t, &format!("{}/{t}.parquet", data_dir()))?;
        }
        Ok(ctx)
    })
}

async fn flight_client(h: &ServerHandle) -> FlightClient {
    let addr = h.flight_addr().expect("flight enabled");
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("uri")
        .connect()
        .await
        .expect("flight connect");
    FlightClient::new(channel)
}

/// SQL over Flight (GetFlightInfo -> DoGet): rows and the trailing metadata JSON.
async fn flight_sql(client: &mut FlightClient, sql: &str) -> (usize, serde_json::Value) {
    let info = client
        .get_flight_info(FlightDescriptor::new_cmd(sql.as_bytes().to_vec()))
        .await
        .expect("get_flight_info");
    let ticket = info.endpoint[0].ticket.clone().expect("ticket");
    let mut decoder = client.do_get(ticket).await.expect("do_get").into_inner();
    let mut rows = 0;
    let mut meta = None;
    while let Some(d) = decoder.try_next().await.expect("stream") {
        if !d.inner.app_metadata.is_empty() {
            meta = Some(
                serde_json::from_slice::<serde_json::Value>(&d.inner.app_metadata).expect("json"),
            );
        }
        if let DecodedPayload::RecordBatch(b) = d.payload {
            rows += b.num_rows();
        }
    }
    (rows, meta.expect("trailing metadata"))
}

async fn start_one(query_log_size: usize) -> ServerHandle {
    isolate_env();
    let h = spawn(options(0, query_log_size), tpch_loader())
        .await
        .expect("bind");
    assert!(
        wait_ready(h.address(), Duration::from_secs(30)).await,
        "node never became ready"
    );
    h
}

async fn start_cluster(n: u64) -> Vec<ServerHandle> {
    isolate_env();
    let mut handles = Vec::new();
    for i in 0..n {
        handles.push(spawn(options(i, 100), tpch_loader()).await.expect("bind"));
    }
    let addrs: Vec<String> = handles.iter().map(|h| h.address().to_string()).collect();
    for h in &handles {
        h.set_peers(addrs.clone());
    }
    for h in &handles {
        assert!(wait_ready(h.address(), Duration::from_secs(30)).await);
    }
    // Wait until every node sees all n members up.
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let mut all = true;
        for h in &handles {
            let v = get_json(h.address(), "/cluster").await;
            if v["member_count"].as_u64() != Some(n) {
                all = false;
            }
        }
        if all {
            break;
        }
        assert!(Instant::now() < deadline, "cluster never converged");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    handles
}

async fn wait_ready(addr: &str, deadline: Duration) -> bool {
    let end = Instant::now() + deadline;
    while Instant::now() < end {
        if let Ok(r) = http_client::get(addr, "/readyz", HTTP_TIMEOUT).await {
            if r.status == 200 {
                return true;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

async fn get_json(addr: &str, path: &str) -> serde_json::Value {
    let r = http_client::get(addr, path, HTTP_TIMEOUT)
        .await
        .unwrap_or_else(|e| panic!("GET {addr}{path}: {e}"));
    serde_json::from_slice(&r.body)
        .unwrap_or_else(|e| panic!("GET {addr}{path} returned non-JSON ({e}): {}", r.text()))
}

async fn sql(addr: &str, query: &str, sql: &str) -> http_client::HttpResponse {
    http_client::post_text(addr, &format!("/sql?{query}"), sql, HTTP_TIMEOUT)
        .await
        .unwrap_or_else(|e| panic!("POST /sql to {addr}: {e}"))
}

#[tokio::test]
async fn queries_list_detail_stats_and_tables_describe_what_ran() {
    let h = start_one(100).await;
    let addr = h.address().to_string();

    // Before anything ran: empty log, stats at zero, all eight tables.
    let empty = get_json(&addr, "/queries").await;
    assert_eq!(empty["total"], 0);
    assert_eq!(empty["capacity"], 100);
    assert_eq!(empty["queries"].as_array().unwrap().len(), 0);
    let tables = get_json(&addr, "/tables").await;
    assert_eq!(tables["ready"], true);
    assert_eq!(
        tables["table_count"], 9,
        "eight TPC-H tables plus the test's `slow` table"
    );
    let lineitem = tables["tables"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "lineitem")
        .expect("lineitem listed");
    assert_eq!(lineitem["column_count"], 16);
    assert!(lineitem["columns"]
        .as_array()
        .unwrap()
        .iter()
        .any(|c| c["name"] == "l_orderkey" && c["data_type"].as_str().is_some()));

    // Three successes (one per format) and one failure.
    let r1 = sql(&addr, "format=json", "SELECT COUNT(*) AS n FROM lineitem").await;
    assert_eq!(r1.status, 200, "{}", r1.text());
    let id1 = r1
        .header("x-qe-query-id")
        .expect("query id header")
        .to_string();
    let rows1: usize = r1.header("x-qe-rows").unwrap().parse().unwrap();
    let r2 = sql(
        &addr,
        "format=csv",
        "SELECT o_orderpriority, COUNT(*) FROM orders o JOIN customer c ON o.o_custkey = c.c_custkey GROUP BY o_orderpriority ORDER BY 1",
    )
    .await;
    assert_eq!(r2.status, 200, "{}", r2.text());
    let r3 = sql(
        &addr,
        "format=arrow",
        "SELECT n_name FROM nation ORDER BY n_name LIMIT 3",
    )
    .await;
    assert_eq!(r3.status, 200);
    let bad = sql(&addr, "format=json", "SELECT nope FROM lineitem").await;
    assert_eq!(bad.status, 400);
    let bad_id = bad
        .header("x-qe-query-id")
        .expect("failed queries carry an id too")
        .to_string();

    // List: newest first, four records, previews and summaries only.
    let list = get_json(&addr, "/queries").await;
    assert_eq!(list["total"], 4);
    let q = list["queries"].as_array().unwrap();
    assert_eq!(q.len(), 4);
    assert_eq!(q[0]["query_id"], bad_id.as_str(), "newest first");
    assert_eq!(q[0]["state"], "failed");
    assert_eq!(
        q[0]["error_kind"].as_str().map(|s| !s.is_empty()),
        Some(true)
    );
    assert_eq!(q[3]["query_id"], id1.as_str());
    assert_eq!(q[3]["state"], "finished");
    assert_eq!(q[3]["front_door"], "http");
    assert_eq!(q[3]["result_format"], "json");
    assert_eq!(q[3]["rows"], rows1);
    assert!(q[3]["client_addr"]
        .as_str()
        .unwrap()
        .starts_with("127.0.0.1:"));
    assert!(
        q[3].get("physical_plan").is_none(),
        "summaries do not carry plans"
    );

    // Filters.
    let failed = get_json(&addr, "/queries?state=failed").await;
    assert_eq!(failed["matched"], 1);
    let by_text = get_json(&addr, "/queries?q=orderpriority").await;
    assert_eq!(by_text["matched"], 1);
    let by_table = get_json(&addr, "/queries?q=nation&state=finished").await;
    assert_eq!(by_table["matched"], 1);
    let limited = get_json(&addr, "/queries?limit=2").await;
    assert_eq!(limited["queries"].as_array().unwrap().len(), 2);
    assert_eq!(limited["matched"], 4);
    let r = http_client::get(&addr, "/queries?limit=many", HTTP_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(r.status, 400);

    // Detail: every debugging fact the PRD names.
    let d = get_json(&addr, &format!("/queries/{id1}")).await;
    assert_eq!(d["state"], "finished");
    assert_eq!(d["sql"], "SELECT COUNT(*) AS n FROM lineitem");
    assert_eq!(d["statement_kind"], "select");
    assert_eq!(d["requested_mode"], "auto");
    assert_eq!(d["tables"], serde_json::json!(["lineitem"]));
    assert!(
        d["physical_plan"].as_str().unwrap().contains("Aggregate"),
        "{}",
        d["physical_plan"]
    );
    assert!(d["optimized_plan"].as_str().unwrap().contains("lineitem"));
    assert_eq!(d["distributed"], false);
    assert_eq!(d["fallback_reason"], "only one cluster member is up");
    assert!(d["result_bytes"].as_u64().unwrap() > 0);
    assert!(d["finished_at"].as_str().is_some());
    let elapsed = d["elapsed_ms"].as_f64().unwrap();
    let phases: f64 = ["parse_ms", "plan_ms", "optimize_ms", "execute_ms"]
        .iter()
        .map(|k| d[k].as_f64().unwrap())
        .sum();
    assert!(
        phases > 0.0 && phases <= elapsed * 1.05 + 1.0,
        "phases {phases} vs elapsed {elapsed}"
    );
    assert!(d["peak_memory_bytes"].as_u64().is_some());
    assert_eq!(d["concurrent_at_start"], 0);
    let df = get_json(&addr, &format!("/queries/{bad_id}")).await;
    assert_eq!(df["state"], "failed");
    assert!(df["error"]["message"]
        .as_str()
        .unwrap()
        .to_lowercase()
        .contains("nope"));
    let missing = http_client::get(&addr, "/queries/does-not-exist", HTTP_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(missing.status, 404);

    // Stats reconcile with the list.
    let s = get_json(&addr, "/stats").await;
    assert_eq!(s["queries"]["total"], 4);
    assert_eq!(s["queries"]["finished"], 3);
    assert_eq!(s["queries"]["failed"], 1);
    assert_eq!(s["queries"]["running"], 0);
    assert_eq!(s["queries"]["local"], 3);
    assert_eq!(s["queries"]["lifetime_total"], 4);
    assert_eq!(s["latency_ms"]["samples"], 4);
    assert!(
        s["latency_ms"]["p95_ms"].as_f64().unwrap() >= s["latency_ms"]["p50_ms"].as_f64().unwrap()
    );
    let per_minute = s["per_minute"].as_array().unwrap();
    assert_eq!(per_minute.len(), 60);
    let in_window: u64 = per_minute
        .iter()
        .map(|b| b["count"].as_u64().unwrap())
        .sum();
    assert_eq!(in_window, 4);
    assert_eq!(s["tables"]["lineitem"], 1);
    assert_eq!(s["tables"]["orders"], 1);
    assert_eq!(
        s["errors_by_kind"]
            .as_object()
            .unwrap()
            .values()
            .map(|v| v.as_u64().unwrap())
            .sum::<u64>(),
        1
    );
    assert_eq!(s["slowest"].as_array().unwrap().len(), 4);
    assert_eq!(s["cluster"]["members"], 1);
    assert_eq!(s["cluster"]["ready"], true);
    assert!(s["uptime_s"].as_f64().unwrap() > 0.0);
    assert!(s["memory"]["max"].as_u64().is_some());

    // The plain-text index advertises the new routes; wrong methods are 405.
    let idx = http_client::get(&addr, "/", HTTP_TIMEOUT)
        .await
        .unwrap()
        .text();
    assert!(idx.contains("/queries") && idx.contains("/stats") && idx.contains("/tables"));
    let r = http_client::post_text(&addr, "/stats", "", HTTP_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(r.status, 405);

    h.shutdown().await;
}

#[tokio::test]
async fn the_embedded_ui_is_served_with_the_right_content_types() {
    let h = start_one(10).await;
    let addr = h.address().to_string();
    for (path, ctype, marker) in [
        (
            "/ui",
            "text/html; charset=utf-8",
            "<script type=\"module\" src=\"/ui/app.js\">",
        ),
        (
            "/ui/",
            "text/html; charset=utf-8",
            "<title>query_engine</title>",
        ),
        (
            "/ui/app.js",
            "text/javascript; charset=utf-8",
            "views.query = {",
        ),
        (
            "/ui/style.css",
            "text/css; charset=utf-8",
            "prefers-color-scheme: dark",
        ),
    ] {
        let r = http_client::get(&addr, path, HTTP_TIMEOUT).await.unwrap();
        assert_eq!(r.status, 200, "{path}");
        assert_eq!(r.header("content-type"), Some(ctype), "{path}");
        assert_eq!(r.header("cache-control"), Some("no-cache"), "{path}");
        assert!(r.text().contains(marker), "{path} lacks {marker:?}");
    }
    let r = http_client::get(&addr, "/ui/nope.js", HTTP_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(r.status, 404);
    // The UI never references anything outside the node.
    let js = http_client::get(&addr, "/ui/app.js", HTTP_TIMEOUT)
        .await
        .unwrap()
        .text();
    let html = http_client::get(&addr, "/ui", HTTP_TIMEOUT)
        .await
        .unwrap()
        .text();
    for src in [&js, &html] {
        assert!(
            !src.contains("https://") && !src.contains("cdn."),
            "UI must be self-contained"
        );
    }
    h.shutdown().await;
}

#[tokio::test]
async fn the_ring_evicts_oldest_finished_and_keeps_running_queries_visible() {
    let h = start_one(10).await;
    let addr = h.address().to_string();

    let mut ids = Vec::new();
    for i in 0..12 {
        let r = sql(
            &addr,
            "format=json",
            &format!("SELECT {i} AS v FROM nation LIMIT 1"),
        )
        .await;
        assert_eq!(r.status, 200);
        ids.push(r.header("x-qe-query-id").unwrap().to_string());
    }
    let list = get_json(&addr, "/queries?limit=all").await;
    assert_eq!(list["total"], 10, "ring holds its capacity");
    let listed: Vec<&str> = list["queries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|q| q["query_id"].as_str().unwrap())
        .collect();
    assert!(!listed.contains(&ids[0].as_str()) && !listed.contains(&ids[1].as_str()));
    assert_eq!(listed[0], ids[11]);
    let evicted = http_client::get(&addr, &format!("/queries/{}", ids[0]), HTTP_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(evicted.status, 404);
    assert!(evicted.text().contains("evicted"));
    let s = get_json(&addr, "/stats").await;
    assert_eq!(s["queries"]["total"], 10);
    assert_eq!(s["queries"]["lifetime_total"], 12);

    // A slow query is visible as `running` while it runs.
    let slow_addr = addr.clone();
    let slow = tokio::spawn(async move {
        sql(&slow_addr, "format=json", "SELECT COUNT(*) AS n FROM slow").await
    });
    let mut seen_running = false;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline && !slow.is_finished() {
        let v = get_json(&addr, "/queries?state=running").await;
        if v["matched"].as_u64() == Some(1) {
            let q = &v["queries"][0];
            assert_eq!(q["state"], "running");
            assert!(q["elapsed_ms"].is_null());
            assert!(q["submitted_at"].as_str().is_some());
            seen_running = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let r = slow.await.unwrap();
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(seen_running, "the running query was never observed");
    let d = get_json(
        &addr,
        &format!("/queries/{}", r.header("x-qe-query-id").unwrap()),
    )
    .await;
    assert_eq!(d["state"], "finished");
    assert!(d["elapsed_ms"].as_f64().unwrap() > 0.0);

    h.shutdown().await;
}

#[tokio::test]
async fn distributed_queries_record_the_distribution_and_workers_record_fragments() {
    let handles = start_cluster(3).await;
    let addrs: Vec<String> = handles.iter().map(|h| h.address().to_string()).collect();

    let r = sql(
        &addrs[0],
        "format=csv&distributed=1",
        "SELECT COUNT(*) AS n, SUM(l_quantity) AS q FROM lineitem",
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("x-qe-distributed"), Some("true"));
    let id = r.header("x-qe-query-id").unwrap().to_string();

    let d = get_json(&addrs[0], &format!("/queries/{id}")).await;
    assert_eq!(d["distributed"], true);
    assert_eq!(d["requested_mode"], "force");
    assert_eq!(d["distribution"]["shard_count"], 3);
    assert_eq!(d["distribution"]["nodes"].as_array().unwrap().len(), 3);
    assert!(d["distribution"]["imbalance"].as_f64().unwrap() >= 1.0);
    assert!(d["fallback_reason"].is_null());

    // Every WORKER ran one fragment over HTTP and says who sent it. The
    // initiator runs its own shard in-process, so its log holds only the
    // statement itself (the distribution record lists all three shards).
    let mut seen_shards: Vec<u64> = Vec::new();
    for (i, addr) in addrs.iter().enumerate() {
        let frags = get_json(addr, "/queries?door=fragment").await;
        if i == 0 {
            assert_eq!(frags["matched"], 0, "initiator: {frags}");
            let s = get_json(addr, "/stats").await;
            assert_eq!(s["queries"]["fragments"], 0);
            assert_eq!(s["cluster"]["members"], 3);
            continue;
        }
        assert_eq!(frags["matched"], 1, "node {i}: {frags}");
        let f = &frags["queries"][0];
        assert_eq!(f["front_door"], "fragment");
        assert_eq!(f["state"], "finished");
        assert!(f["client_addr"].as_str().unwrap().starts_with("127.0.0.1:"));
        let full = get_json(
            addr,
            &format!("/queries/{}", f["query_id"].as_str().unwrap()),
        )
        .await;
        assert_eq!(full["shard"]["count"], 3);
        assert_eq!(full["shard"]["table"], "lineitem");
        let idx = full["shard"]["index"].as_u64().unwrap();
        assert!(
            idx < 3 && !seen_shards.contains(&idx),
            "distinct shard per worker: {seen_shards:?} + {idx}"
        );
        seen_shards.push(idx);
        assert!(full["initiator"].as_str().is_some());
        assert!(
            full["physical_plan"].as_str().is_some(),
            "fragments are planned locally and keep their plan"
        );
        let s = get_json(addr, "/stats").await;
        assert_eq!(s["queries"]["fragments"], 1);
        assert_eq!(s["cluster"]["members"], 3);
        assert_eq!(s["cluster"]["up"], 3);
    }
    let s0 = get_json(&addrs[0], "/stats").await;
    assert_eq!(s0["queries"]["distributed"], 1);

    for h in handles {
        h.shutdown().await;
    }
}

/// G3: the same statement through HTTP and through Flight produces records
/// that differ only in identity, transport, timing and result encoding.
#[tokio::test]
async fn http_and_flight_records_agree_on_everything_but_transport() {
    isolate_env();
    let mut opts = options(0, 50);
    opts.flight_bind = None; // derive an ephemeral Flight port
    let h = spawn(opts, tpch_loader()).await.expect("bind");
    assert!(wait_ready(h.address(), Duration::from_secs(30)).await);
    let addr = h.address().to_string();
    let sql_text = "SELECT o_orderpriority, COUNT(*) AS n FROM orders o JOIN lineitem l ON o.o_orderkey = l.l_orderkey GROUP BY o_orderpriority ORDER BY 1";

    let r = sql(&addr, "format=csv", sql_text).await;
    assert_eq!(r.status, 200, "{}", r.text());
    let http_id = r.header("x-qe-query-id").unwrap().to_string();

    let mut client = flight_client(&h).await;
    let (flight_rows, meta) = flight_sql(&mut client, sql_text).await;
    let flight_id = meta["query_id"]
        .as_str()
        .expect("flight trailer carries the query id")
        .to_string();
    assert_ne!(http_id, flight_id);

    let a = get_json(&addr, &format!("/queries/{http_id}")).await;
    let b = get_json(&addr, &format!("/queries/{flight_id}")).await;
    assert_eq!(a["front_door"], "http");
    assert_eq!(b["front_door"], "flight");
    assert_eq!(a["result_format"], "csv");
    assert_eq!(b["result_format"], "flight");
    assert_eq!(b["rows"], flight_rows);
    assert!(b["client_addr"].as_str().unwrap().starts_with("127.0.0.1:"));

    let volatile = [
        "query_id",
        "seq",
        "front_door",
        "client_addr",
        "submitted_at",
        "submitted_unix_ms",
        "finished_at",
        "result_format",
        "result_bytes",
        "elapsed_ms",
        "parse_ms",
        "plan_ms",
        "optimize_ms",
        "execute_ms",
        "peak_memory_bytes",
        "concurrent_at_start",
    ];
    let strip = |v: &serde_json::Value| {
        let mut m = v.as_object().unwrap().clone();
        for k in volatile {
            m.remove(k);
        }
        m
    };
    let (sa, sb) = (strip(&a), strip(&b));
    assert_eq!(
        sa, sb,
        "records differ beyond identity/transport/timing:\n{a}\n{b}"
    );
    assert_eq!(sa["tables"], serde_json::json!(["lineitem", "orders"]));
    assert!(sa["physical_plan"].as_str().unwrap().contains("Join"));

    let list = get_json(&addr, "/queries?door=flight").await;
    assert_eq!(list["matched"], 1);
    h.shutdown().await;
}

/// G2: a query that spills reports a real pool high-water mark and spill facts.
#[tokio::test]
async fn a_spilling_query_reports_its_peak_and_spill_facts() {
    isolate_env();
    // 16KB budget: the ~30KB projected `orders` build side crosses
    // `memory_limit * spill_threshold` (0.8) and the join must spill.
    let limit = 16 * 1024;
    let threshold = (limit as f64 * ExecutionConfig::new().spill_threshold) as u64;
    let h = spawn(options(0, 50), spilling_loader(limit, "peak"))
        .await
        .expect("bind");
    assert!(wait_ready(h.address(), Duration::from_secs(30)).await);
    let addr = h.address().to_string();
    let r = sql(
        &addr,
        "format=csv",
        "SELECT o_orderpriority, COUNT(*) AS cnt, SUM(l_extendedprice) AS total FROM lineitem, orders WHERE l_orderkey = o_orderkey GROUP BY o_orderpriority ORDER BY o_orderpriority",
    )
    .await;
    assert_eq!(r.status, 200, "{}", r.text());
    let id = r.header("x-qe-query-id").unwrap();
    let d = get_json(&addr, &format!("/queries/{id}")).await;
    assert_eq!(d["memory_limit_bytes"], limit);
    assert!(
        d["spill"]["bytes"].as_u64().unwrap_or(0) > 0,
        "spill facts recorded: {}",
        d["spill"]
    );
    let peak = d["peak_memory_bytes"].as_u64().unwrap();
    assert!(
        peak >= threshold,
        "peak {peak} must be at least the {threshold}-byte spill threshold the operator crossed, not the post-release residual"
    );
    let s = get_json(&addr, "/stats").await;
    assert_eq!(s["spill_queries"], 1);
    assert!(s["spilled_bytes_total"].as_u64().unwrap() > 0);
    assert!(s["memory"]["peak"].as_u64().unwrap() >= peak / 2);
    h.shutdown().await;
}
