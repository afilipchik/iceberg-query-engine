//! M1 acceptance tests: the engine running as several independent instances.
//!
//! Two levels, deliberately:
//!
//! * **In-process servers on ephemeral ports** — real `TcpListener`s, real
//!   sockets, real HTTP, real Arrow IPC on the wire. Fast, and they can drive
//!   states a shell script cannot reach deterministically (the not-ready window
//!   is observable only if the test controls when the loader finishes).
//! * **Real OS processes** — the actual `query_engine` binary, three of them,
//!   killed with a real `SIGTERM`. This is the only level that proves signal
//!   handling and process teardown, and it is the closest thing to a pod that
//!   this machine can run (kind/Docker are unavailable here — see
//!   `.claude/plans/DISTRIBUTED-IMPLEMENTATION.md` §0).
//!
//! Nothing here uses a fixed port, and every spawned process is killed by a
//! `Drop` guard even if an assertion panics first.

use query_engine::distributed::{http_client, spawn, ServeOptions, ServerHandle, TableLoader};
use query_engine::ExecutionContext;
use std::net::TcpListener as StdListener;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

const DATA: &str = "data/tpch-1mb";
const TABLES: [&str; 8] = [
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];
const HTTP_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

/// Clear the environment the address-derivation code consults, so a developer
/// (or a CI runner) with `POD_IP` set does not get a cluster that advertises
/// addresses nothing can dial. Runs once per test binary.
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

/// Registers the eight TPC-H tables. Same convention as `duckdb_validated.rs`:
/// missing data is a loud panic, because CI regenerates `data/tpch-1mb` and a
/// silently skipped acceptance test is worse than a failing one.
fn tpch_loader() -> TableLoader {
    Box::new(|| {
        let mut ctx = ExecutionContext::new();
        for t in TABLES {
            let path = format!("{}/{t}.parquet", data_dir());
            ctx.register_parquet(t, &path)?;
        }
        Ok(ctx)
    })
}

fn in_process_context() -> ExecutionContext {
    let mut ctx = ExecutionContext::new();
    for t in TABLES {
        let path = format!("{}/{t}.parquet", data_dir());
        ctx.register_parquet(t, &path)
            .unwrap_or_else(|e| panic!("cannot load {path}: {e}"));
    }
    ctx
}

fn test_options(node_id: u64) -> ServeOptions {
    ServeOptions {
        bind: "127.0.0.1:0".into(),
        node_id: Some(node_id),
        // Fast enough that convergence is not a test-duration problem, slow
        // enough that we are not measuring the prober's own overhead.
        discovery_interval: Duration::from_millis(50),
        probe_timeout: Duration::from_millis(2000),
        ..Default::default()
    }
}

/// Start `n` in-process nodes on ephemeral ports, then tell each about all of
/// them (including itself — self-filtering is part of what is under test).
async fn start_cluster(n: u64) -> Vec<ServerHandle> {
    isolate_env();
    let mut handles = Vec::new();
    for i in 0..n {
        handles.push(
            spawn(test_options(i), tpch_loader())
                .await
                .expect("server failed to bind"),
        );
    }
    let addrs: Vec<String> = handles.iter().map(|h| h.address().to_string()).collect();
    for h in &handles {
        h.set_peers(addrs.clone());
    }
    handles
}

async fn shutdown_all(handles: Vec<ServerHandle>) {
    for h in handles {
        h.shutdown().await;
    }
}

async fn get_json(addr: &str, path: &str) -> serde_json::Value {
    let r = http_client::get(addr, path, HTTP_TIMEOUT)
        .await
        .unwrap_or_else(|e| panic!("GET {addr}{path}: {e}"));
    serde_json::from_slice(&r.body)
        .unwrap_or_else(|e| panic!("GET {addr}{path} returned non-JSON ({e}): {}", r.text()))
}

/// Poll `f` until it returns true or `deadline` elapses. Returns whether it
/// converged, so callers can produce a useful assertion message.
async fn converges<F, Fut>(deadline: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let start = Instant::now();
    loop {
        if f().await {
            return true;
        }
        if start.elapsed() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_ready(addr: &str, deadline: Duration) -> bool {
    converges(deadline, || async {
        matches!(http_client::get(addr, "/readyz", HTTP_TIMEOUT).await, Ok(r) if r.is_success())
    })
    .await
}

/// The identity of a member as every node must agree on it: address, node id,
/// and reachability. Timestamps are deliberately excluded — they cannot agree
/// and are not part of the membership contract.
fn member_identities(view: &serde_json::Value) -> Vec<(String, Option<u64>, String)> {
    view["members"]
        .as_array()
        .expect("members must be an array")
        .iter()
        .map(|m| {
            (
                m["address"].as_str().unwrap_or_default().to_string(),
                m["node_id"].as_u64(),
                m["status"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect()
}

fn csv_of(result: &query_engine::QueryResult) -> String {
    let mut buf = Vec::new();
    {
        let mut w = arrow::csv::WriterBuilder::new()
            .with_header(true)
            .build(&mut buf);
        for b in &result.batches {
            w.write(b).expect("csv encode");
        }
    }
    String::from_utf8(buf).expect("csv is utf-8")
}

// ---------------------------------------------------------------------------
// The gate
// ---------------------------------------------------------------------------

/// GATE: three nodes, and `/cluster` on each returns the same three-member view.
#[tokio::test]
async fn three_nodes_converge_on_one_identical_membership_view() {
    let nodes = start_cluster(3).await;
    let addrs: Vec<String> = nodes.iter().map(|h| h.local_addr().to_string()).collect();

    let converged = converges(Duration::from_secs(20), || {
        let addrs = addrs.clone();
        async move {
            for a in &addrs {
                let v = get_json(a, "/cluster").await;
                if v["member_count"].as_u64() != Some(3) {
                    return false;
                }
                if member_identities(&v)
                    .iter()
                    .any(|(_, id, st)| id.is_none() || st != "up")
                {
                    return false;
                }
            }
            true
        }
    })
    .await;

    let views: Vec<serde_json::Value> = {
        let mut v = Vec::new();
        for a in &addrs {
            v.push(get_json(a, "/cluster").await);
        }
        v
    };
    assert!(
        converged,
        "cluster did not converge; views were:\n{}",
        views
            .iter()
            .map(|v| serde_json::to_string_pretty(v).unwrap())
            .collect::<Vec<_>>()
            .join("\n---\n")
    );

    let reference = member_identities(&views[0]);
    assert_eq!(reference.len(), 3);
    for (i, v) in views.iter().enumerate() {
        assert_eq!(
            member_identities(v),
            reference,
            "node {i} disagrees about membership"
        );
    }

    // Every node knows exactly one of the three is itself, and it is a
    // different one on each node.
    let selves: Vec<u64> = views
        .iter()
        .map(|v| {
            let s: Vec<&serde_json::Value> = v["members"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|m| m["is_self"].as_bool() == Some(true))
                .collect();
            assert_eq!(s.len(), 1, "exactly one member must be self");
            s[0]["node_id"].as_u64().unwrap()
        })
        .collect();
    let mut sorted = selves.clone();
    sorted.sort_unstable();
    assert_eq!(sorted, vec![0, 1, 2], "self ids were {selves:?}");

    shutdown_all(nodes).await;
}

/// GATE (M1): each node answers `/sql` independently, and identically to the
/// single-process engine.
///
/// Pinned to `distributed=0` deliberately. This test's subject is the LOCAL
/// path — "a server process answers what the binary answers, byte for byte" —
/// and byte-for-byte is only a fair demand of the local path. A distributed
/// `SUM` over `f64` adds the column in shard-sized pieces, and floating-point
/// addition is not associative, so the last bits legitimately differ. The
/// distributed answers get their own gate
/// (`distributed_answers_match_the_single_process_engine`), which compares with
/// the same numeric tolerance the DuckDB-validated suite uses.
#[tokio::test]
async fn every_node_answers_exactly_what_the_single_process_engine_answers() {
    let nodes = start_cluster(3).await;
    let addrs: Vec<String> = nodes.iter().map(|h| h.local_addr().to_string()).collect();
    for a in &addrs {
        assert!(
            wait_ready(a, Duration::from_secs(30)).await,
            "{a} never ready"
        );
    }

    let local = in_process_context();

    let mut queries: Vec<(String, String)> = vec![
        ("count".into(), "SELECT COUNT(*) AS n FROM lineitem".into()),
        (
            "group".into(),
            "SELECT l_returnflag, l_linestatus, COUNT(*) AS c, SUM(l_quantity) AS q \
             FROM lineitem GROUP BY l_returnflag, l_linestatus \
             ORDER BY l_returnflag, l_linestatus"
                .into(),
        ),
        (
            "join".into(),
            "SELECT n_name, COUNT(*) AS c FROM customer JOIN nation ON c_nationkey = n_nationkey \
             GROUP BY n_name ORDER BY n_name"
                .into(),
        ),
    ];
    // Real TPC-H, straight from the same source the benchmark uses.
    for q in [1usize, 3, 5, 6, 10] {
        if let Some(sql) = query_engine::tpch::get_query_for_sf(q, 0.001) {
            queries.push((format!("tpch-q{q:02}"), sql));
        }
    }

    for (name, sql) in &queries {
        let expected = csv_of(&local.sql(sql).await.unwrap_or_else(|e| {
            panic!("single-process engine failed on {name}: {e}");
        }));

        for (i, addr) in addrs.iter().enumerate() {
            let resp =
                http_client::post_text(addr, "/sql?format=csv&distributed=0", sql, HTTP_TIMEOUT)
                    .await
                    .unwrap_or_else(|e| panic!("node {i} POST /sql ({name}): {e}"));
            assert!(
                resp.is_success(),
                "node {i} rejected {name}: HTTP {} {}",
                resp.status,
                resp.text()
            );
            assert_eq!(
                resp.text(),
                expected,
                "node {i} disagrees with the single-process engine on {name}"
            );
        }
    }

    shutdown_all(nodes).await;
}

/// The default `/sql` payload is a decodable Arrow IPC stream carrying the same
/// rows. Checked separately from the CSV comparison because CSV would hide a
/// schema or dictionary-encoding bug that the wire format must not have.
#[tokio::test]
async fn sql_returns_a_decodable_arrow_ipc_stream() {
    let nodes = start_cluster(1).await;
    let addr = nodes[0].local_addr().to_string();
    assert!(wait_ready(&addr, Duration::from_secs(30)).await);

    let sql = "SELECT l_returnflag, l_linestatus, COUNT(*) AS c, SUM(l_extendedprice) AS p \
               FROM lineitem GROUP BY l_returnflag, l_linestatus \
               ORDER BY l_returnflag, l_linestatus";

    let resp = http_client::post_text(&addr, "/sql", sql, HTTP_TIMEOUT)
        .await
        .expect("POST /sql");
    assert!(resp.is_success(), "HTTP {}: {}", resp.status, resp.text());

    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(resp.body), None)
        .expect("response is not a valid Arrow IPC stream");
    let schema = reader.schema();
    let batches: Vec<arrow::record_batch::RecordBatch> =
        reader.collect::<Result<_, _>>().expect("IPC decode");

    let local = in_process_context();
    let expected = local.sql(sql).await.unwrap();

    assert_eq!(
        schema.fields().len(),
        expected.schema.fields().len(),
        "column count differs"
    );
    let got_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(got_rows, expected.row_count);

    // Cell-exact, via the same rendering on both sides.
    let render = |bs: &[arrow::record_batch::RecordBatch]| {
        arrow::util::pretty::pretty_format_batches(bs)
            .map(|d| d.to_string())
            .unwrap_or_default()
    };
    assert_eq!(render(&batches), render(&expected.batches));

    shutdown_all(nodes).await;
}

/// GATE: `/readyz` is false before tables load and true after; `/healthz` is
/// true throughout.
///
/// The loader blocks on a channel this test owns, so the not-ready window is a
/// state the test *creates* rather than a race it hopes to catch.
#[tokio::test]
async fn readyz_is_false_until_tables_load_while_healthz_stays_true() {
    isolate_env();
    let (release, blocked) = std::sync::mpsc::channel::<()>();

    let loader: TableLoader = Box::new(move || {
        blocked.recv().ok();
        let mut ctx = ExecutionContext::new();
        for t in TABLES {
            ctx.register_parquet(t, format!("{}/{t}.parquet", data_dir()))?;
        }
        Ok(ctx)
    });

    let node = spawn(test_options(0), loader).await.expect("bind");
    let addr = node.local_addr().to_string();

    // Liveness is up as soon as the listener is: this is what stops Kubernetes
    // from killing a pod that is merely still loading.
    let health = http_client::get(&addr, "/healthz", HTTP_TIMEOUT)
        .await
        .expect("healthz");
    assert_eq!(health.status, 200, "healthz must be up before tables are");

    let ready = http_client::get(&addr, "/readyz", HTTP_TIMEOUT)
        .await
        .expect("readyz");
    assert_eq!(
        ready.status,
        503,
        "readyz must refuse traffic before tables load: {}",
        ready.text()
    );
    let body: serde_json::Value = serde_json::from_slice(&ready.body).unwrap();
    assert_eq!(body["ready"], serde_json::json!(false));
    assert_eq!(body["tables_loaded"], serde_json::json!(false));
    assert_eq!(body["reason"], serde_json::json!("tables not loaded yet"));

    // A query in this state must be refused, not answered emptily.
    let refused = http_client::post_text(&addr, "/sql", "SELECT 1 FROM nation", HTTP_TIMEOUT)
        .await
        .expect("sql");
    assert_eq!(refused.status, 503, "{}", refused.text());

    // Liveness must not have flickered.
    assert_eq!(
        http_client::get(&addr, "/healthz", HTTP_TIMEOUT)
            .await
            .unwrap()
            .status,
        200
    );

    release.send(()).unwrap();
    assert!(
        wait_ready(&addr, Duration::from_secs(30)).await,
        "node never became ready after the loader was released"
    );

    let body: serde_json::Value = get_json(&addr, "/readyz").await;
    assert_eq!(body["ready"], serde_json::json!(true));
    assert_eq!(body["tables_loaded"], serde_json::json!(true));
    assert_eq!(body["peers_resolved"], serde_json::json!(true));

    node.shutdown().await;
}

/// A node whose tables cannot load stays not-ready forever and says why. It
/// must never come up and answer queries against a partial schema.
#[tokio::test]
async fn a_failed_table_load_is_permanent_and_explained() {
    isolate_env();
    let loader: TableLoader = Box::new(|| {
        let mut ctx = ExecutionContext::new();
        ctx.register_parquet("nowhere", "/nonexistent/path/nope.parquet")?;
        Ok(ctx)
    });
    let node = spawn(test_options(0), loader).await.expect("bind");
    let addr = node.local_addr().to_string();

    let failed = converges(Duration::from_secs(10), || {
        let addr = addr.clone();
        async move {
            let v = get_json(&addr, "/readyz").await;
            v["load_error"].is_string()
        }
    })
    .await;
    assert!(failed, "the loader failure was never reported");

    let v = get_json(&addr, "/readyz").await;
    assert_eq!(v["ready"], serde_json::json!(false));
    assert_eq!(v["reason"], serde_json::json!("table load failed"));
    // Still alive: this is a readiness problem, not a reason to restart.
    assert_eq!(
        http_client::get(&addr, "/healthz", HTTP_TIMEOUT)
            .await
            .unwrap()
            .status,
        200
    );

    node.shutdown().await;
}

/// GATE: a node going away is reported as unreachable by the survivors, who
/// keep serving.
#[tokio::test]
async fn a_departed_node_is_marked_down_and_the_survivors_keep_serving() {
    let mut nodes = start_cluster(3).await;
    let addrs: Vec<String> = nodes.iter().map(|h| h.local_addr().to_string()).collect();
    for a in &addrs {
        assert!(wait_ready(a, Duration::from_secs(30)).await);
    }
    let converged = converges(Duration::from_secs(20), || {
        let a = addrs[0].clone();
        async move {
            member_identities(&get_json(&a, "/cluster").await)
                .iter()
                .all(|(_, _, st)| st == "up")
        }
    })
    .await;
    assert!(converged, "cluster never became fully up");

    let departing = nodes.pop().unwrap();
    let gone_addr = departing.address().to_string();
    departing.shutdown().await;

    let survivors: Vec<String> = nodes.iter().map(|h| h.local_addr().to_string()).collect();
    let noticed = converges(Duration::from_secs(20), || {
        let survivors = survivors.clone();
        let gone = gone_addr.clone();
        async move {
            for a in &survivors {
                let v = get_json(a, "/cluster").await;
                // Still a member — it is unreachable, not forgotten.
                if v["member_count"].as_u64() != Some(3) {
                    return false;
                }
                let ok = member_identities(&v)
                    .iter()
                    .any(|(addr, _, st)| *addr == gone && st == "down");
                if !ok {
                    return false;
                }
            }
            true
        }
    })
    .await;
    assert!(noticed, "survivors never marked the departed node down");

    // ...and they are still healthy, ready, and answering.
    for a in &survivors {
        assert_eq!(
            http_client::get(a, "/healthz", HTTP_TIMEOUT)
                .await
                .unwrap()
                .status,
            200
        );
        let r = http_client::get(a, "/readyz", HTTP_TIMEOUT).await.unwrap();
        assert_eq!(
            r.status,
            200,
            "a peer being down must not un-ready us: {}",
            r.text()
        );
        let q = http_client::post_text(
            a,
            "/sql?format=csv",
            "SELECT COUNT(*) AS n FROM orders",
            HTTP_TIMEOUT,
        )
        .await
        .unwrap();
        assert!(q.is_success(), "{}", q.text());
        assert_eq!(q.text().lines().count(), 2);
    }

    // The reported reason is the real one, not a placeholder.
    let v = get_json(&survivors[0], "/cluster").await;
    let down = v["members"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["address"].as_str() == Some(gone_addr.as_str()))
        .unwrap();
    assert!(
        down["last_error"].is_string(),
        "a down peer must carry the reason it is down"
    );
    assert!(down["consecutive_failures"].as_u64().unwrap_or(0) >= 1);

    shutdown_all(nodes).await;
}

/// Bad requests fail loudly and specifically. A cluster endpoint that answers
/// 200 with something unexpected is how a wrong distributed answer starts.
#[tokio::test]
async fn malformed_requests_are_rejected_with_the_right_status() {
    let nodes = start_cluster(1).await;
    let addr = nodes[0].local_addr().to_string();
    assert!(wait_ready(&addr, Duration::from_secs(30)).await);

    let unknown = http_client::get(&addr, "/does-not-exist", HTTP_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(unknown.status, 404);

    let wrong_method = http_client::get(&addr, "/sql", HTTP_TIMEOUT).await.unwrap();
    assert_eq!(wrong_method.status, 405);

    let empty = http_client::post_text(&addr, "/sql", "", HTTP_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(empty.status, 400);

    let bad_sql = http_client::post_text(&addr, "/sql", "SELECT FROM WHERE", HTTP_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(bad_sql.status, 400);
    let err: serde_json::Value = serde_json::from_slice(&bad_sql.body).unwrap();
    assert!(err["error"].is_string(), "errors must be structured");

    let missing_table =
        http_client::post_text(&addr, "/sql", "SELECT * FROM no_such_table", HTTP_TIMEOUT)
            .await
            .unwrap();
    assert_eq!(missing_table.status, 400);

    // An unknown format must not silently fall back to Arrow.
    let bad_format = http_client::post_text(
        &addr,
        "/sql?format=parquet",
        "SELECT 1 AS x FROM nation",
        HTTP_TIMEOUT,
    )
    .await
    .unwrap();
    assert_eq!(bad_format.status, 400);

    // Every response identifies the answering node, including the failures.
    assert!(bad_sql.status == 400);

    shutdown_all(nodes).await;
}

/// Membership tracks reconfiguration: adding and removing peers at runtime is
/// the same code path that Kubernetes pod churn drives through DNS.
#[tokio::test]
async fn membership_follows_peer_list_changes() {
    let nodes = start_cluster(2).await;
    let a = nodes[0].local_addr().to_string();
    assert!(
        converges(Duration::from_secs(20), || {
            let a = a.clone();
            async move { get_json(&a, "/cluster").await["member_count"].as_u64() == Some(2) }
        })
        .await,
        "two nodes never saw each other"
    );

    // Shrink to a single-node cluster.
    nodes[0].set_peers(vec![nodes[0].address().to_string()]);
    assert!(
        converges(Duration::from_secs(20), || {
            let a = a.clone();
            async move { get_json(&a, "/cluster").await["member_count"].as_u64() == Some(1) }
        })
        .await,
        "removed peer was never dropped"
    );

    // Grow back.
    nodes[0].set_peers(vec![
        nodes[0].address().to_string(),
        nodes[1].address().to_string(),
    ]);
    assert!(
        converges(Duration::from_secs(20), || {
            let a = a.clone();
            async move {
                let v = get_json(&a, "/cluster").await;
                v["member_count"].as_u64() == Some(2)
                    && member_identities(&v).iter().all(|(_, _, st)| st == "up")
            }
        })
        .await,
        "re-added peer never came back up"
    );

    shutdown_all(nodes).await;
}

// ---------------------------------------------------------------------------
// M2: balanced distributed scan and two-phase aggregation
// ---------------------------------------------------------------------------

/// Cell-by-cell comparison with the numeric tolerance the DuckDB-validated
/// suite uses, and rows compared as a SET.
///
/// Both relaxations are arithmetic facts, not looseness:
///
/// * `SUM` over `f64` is not associative, so adding a column in three
///   shard-sized pieces differs from adding it in one — in the last bits.
/// * `GROUP BY` without `ORDER BY` has unspecified row order in SQL, and the
///   merge hashes a handful of partial rows where the single-node run hashes
///   thousands of base rows. `ORDER BY` is REJECTED for distributed queries
///   precisely so nobody mistakes the incidental order for a promise.
fn assert_cells_match(got: &str, expected: &str, context: &str) {
    let norm = |s: &str| {
        let mut l: Vec<String> = s.trim().lines().map(|x| x.to_string()).collect();
        if l.len() > 1 {
            l[1..].sort();
        }
        l
    };
    let (g, e) = (norm(got), norm(expected));
    assert_eq!(
        g.len(),
        e.len(),
        "{context}: row count\n--got--\n{got}\n--want--\n{expected}"
    );
    for (row, (gl, el)) in g.iter().zip(e.iter()).enumerate() {
        let gc: Vec<&str> = gl.split(',').collect();
        let ec: Vec<&str> = el.split(',').collect();
        assert_eq!(gc.len(), ec.len(), "{context}: column count on row {row}");
        for (col, (a, b)) in gc.iter().zip(ec.iter()).enumerate() {
            if a == b {
                continue;
            }
            match (a.parse::<f64>(), b.parse::<f64>()) {
                (Ok(x), Ok(y)) => {
                    let tol = (1e-6 * x.abs().max(y.abs())).max(1e-9);
                    assert!(
                        (x - y).abs() <= tol,
                        "{context}: row {row} col {col}: {a} vs {b}"
                    );
                }
                _ => panic!("{context}: row {row} col {col}: `{a}` vs `{b}`"),
            }
        }
    }
}

async fn dsql(addr: &str, sql: &str) -> query_engine::distributed::HttpResponse {
    http_client::post_text(addr, "/sql?format=csv&distributed=1", sql, HTTP_TIMEOUT)
        .await
        .unwrap_or_else(|e| panic!("POST /sql?distributed=1 to {addr}: {e}"))
}

/// The M2 headline: a query fanned out across three processes returns what the
/// whole dataset returns — from whichever node received it.
#[tokio::test]
async fn distributed_answers_match_the_single_process_engine() {
    let nodes = start_cluster(3).await;
    let addrs: Vec<String> = nodes.iter().map(|h| h.local_addr().to_string()).collect();
    for a in &addrs {
        assert!(
            wait_ready(a, Duration::from_secs(30)).await,
            "{a} never ready"
        );
    }
    let local = in_process_context();

    let queries: &[(&str, &str)] = &[
        ("count", "SELECT COUNT(*) AS n FROM lineitem"),
        (
            "count_filtered",
            "SELECT COUNT(*) AS n FROM lineitem WHERE l_shipdate < '1995-01-01'",
        ),
        (
            "sum_min_max",
            "SELECT SUM(l_quantity) AS s, MIN(l_extendedprice) AS lo, \
             MAX(l_extendedprice) AS hi FROM lineitem",
        ),
        (
            "avg",
            "SELECT AVG(l_quantity) AS a, AVG(l_discount) AS d FROM lineitem",
        ),
        (
            "avg_of_nothing",
            "SELECT AVG(l_quantity) AS a, COUNT(*) AS c FROM lineitem WHERE l_orderkey < 0",
        ),
        (
            "group_by",
            "SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS q, \
             AVG(l_discount) AS d, COUNT(*) AS c \
             FROM lineitem GROUP BY l_returnflag, l_linestatus",
        ),
        (
            "group_by_having",
            "SELECT l_shipmode, COUNT(*) AS c FROM lineitem \
             GROUP BY l_shipmode HAVING COUNT(*) > 100",
        ),
        (
            "tiny_table",
            "SELECT COUNT(*) AS c, MIN(n_name) AS lo, MAX(n_name) AS hi FROM nation",
        ),
        (
            "pass_through",
            "SELECT l_orderkey, l_linenumber FROM lineitem WHERE l_quantity > 49.9",
        ),
    ];

    for (name, sql) in queries {
        let expected = csv_of(&local.sql(sql).await.unwrap_or_else(|e| {
            panic!("single-process engine failed on {name}: {e}");
        }));
        for (i, addr) in addrs.iter().enumerate() {
            let resp = dsql(addr, sql).await;
            assert!(
                resp.is_success(),
                "node {i} could not distribute {name}: HTTP {} {}",
                resp.status,
                resp.text()
            );
            // `distributed=1` must never fall back; if this header is false the
            // comparison below would be comparing local to local and proving
            // nothing.
            assert_eq!(
                resp.header("x-qe-distributed"),
                Some("true"),
                "node {i} answered {name} locally despite distributed=1"
            );
            assert_cells_match(&resp.text(), &expected, &format!("{name} @node{i}"));
        }
    }

    shutdown_all(nodes).await;
}

/// Every response says how the work was divided, and the division adds up.
#[tokio::test]
async fn the_distribution_is_reported_with_every_answer() {
    let nodes = start_cluster(3).await;
    let addrs: Vec<String> = nodes.iter().map(|h| h.local_addr().to_string()).collect();
    for a in &addrs {
        assert!(wait_ready(a, Duration::from_secs(30)).await);
    }
    assert!(
        converges(Duration::from_secs(20), || {
            let a = addrs[0].clone();
            async move { get_json(&a, "/cluster").await["member_count"].as_u64() == Some(3) }
        })
        .await,
        "cluster never converged"
    );

    let resp = dsql(&addrs[0], "SELECT COUNT(*) AS n FROM lineitem").await;
    assert!(resp.is_success(), "{}", resp.text());
    let d: serde_json::Value = serde_json::from_str(
        resp.header("x-qe-distribution")
            .expect("distribution header"),
    )
    .expect("distribution header is JSON");

    assert_eq!(d["shard_count"].as_u64(), Some(3));
    assert_eq!(d["table"].as_str(), Some("lineitem"));
    let per: Vec<u64> = d["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["assigned_bytes"].as_u64().unwrap())
        .collect();
    assert_eq!(
        per.iter().sum::<u64>(),
        d["total_bytes"].as_u64().unwrap(),
        "every byte must be attributed to exactly one node"
    );
    let imbalance = d["imbalance"].as_f64().unwrap();
    assert!(
        (1.0..=1.10).contains(&imbalance),
        "imbalance {imbalance} outside the gate"
    );
    // Exactly one node reports itself as the in-process shard: the initiator
    // works, it does not just coordinate.
    assert_eq!(
        d["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|n| n["local"].as_bool() == Some(true))
            .count(),
        1
    );

    shutdown_all(nodes).await;
}

/// `/splits` prices the division for ANY node count without running that many
/// nodes, because it is metadata arithmetic. This is the balance gate.
#[tokio::test]
async fn the_balance_gate_holds_at_three_and_eight_nodes() {
    let nodes = start_cluster(1).await;
    let addr = nodes[0].local_addr().to_string();
    assert!(wait_ready(&addr, Duration::from_secs(30)).await);

    for n in [3u64, 8] {
        let v = get_json(&addr, &format!("/splits?table=lineitem&nodes={n}")).await;
        let imbalance = v["imbalance"].as_f64().expect("imbalance is a number");
        assert!(
            imbalance <= 1.10,
            "lineitem at {n} nodes: imbalance {imbalance:.4} exceeds the 1.10 gate \
             ({} splits)",
            v["total_splits"]
        );
        assert_eq!(
            v["idle_nodes"].as_array().map(|a| a.len()),
            Some(0),
            "no node should be left with nothing at {n} nodes: {v}"
        );
        let assigned: u64 = v["per_node"]
            .as_array()
            .unwrap()
            .iter()
            .map(|p| p["bytes"].as_u64().unwrap())
            .sum();
        assert_eq!(assigned, v["total_bytes"].as_u64().unwrap());
    }

    // An unknown table is a 400 naming it, not an empty assignment.
    let r = http_client::get(&addr, "/splits?table=nope&nodes=3", HTTP_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(r.status, 400);
    assert!(r.text().contains("nope"), "{}", r.text());

    shutdown_all(nodes).await;
}

/// The rejection gate: each unsupported shape must be refused, by name, with a
/// 501 — never answered from one shard and presented as a cluster answer.
#[tokio::test]
async fn every_engine_supported_shape_is_answered_distributed() {
    let nodes = start_cluster(3).await;
    let addrs: Vec<String> = nodes.iter().map(|h| h.local_addr().to_string()).collect();
    for a in &addrs {
        assert!(wait_ready(a, Duration::from_secs(30)).await);
    }
    let local = in_process_context();

    // Everything M2 used to refuse and M2.5 (gather) now answers. The second
    // field says how to compare against the single-process answer: `true`
    // means cell-for-cell; `false` means row COUNT only, for statements whose
    // answer is legitimately nondeterministic (LIMIT without ORDER BY may
    // return any 10 rows, and which 10 depends on shard arrival order).
    let cases: &[(&str, &str, bool)] = &[
        (
            "inner_join",
            "SELECT COUNT(*) AS n FROM lineitem JOIN orders ON l_orderkey = o_orderkey",
            true,
        ),
        (
            "comma_join",
            "SELECT COUNT(*) AS n FROM lineitem, orders WHERE l_orderkey = o_orderkey",
            true,
        ),
        (
            "left_join",
            "SELECT COUNT(*) AS n, COUNT(o_orderkey) AS matched FROM orders \
             LEFT JOIN lineitem ON o_orderkey = l_orderkey AND l_quantity > 45",
            true,
        ),
        (
            "three_way_join",
            "SELECT n_name, COUNT(*) AS c FROM customer \
             JOIN orders ON c_custkey = o_custkey \
             JOIN nation ON c_nationkey = n_nationkey \
             GROUP BY n_name ORDER BY c DESC, n_name",
            true,
        ),
        (
            "count_distinct",
            "SELECT COUNT(DISTINCT l_orderkey) AS n FROM lineitem",
            true,
        ),
        (
            "in_subquery",
            "SELECT COUNT(*) AS n FROM lineitem WHERE l_orderkey IN (SELECT o_orderkey FROM orders)",
            true,
        ),
        (
            "exists_subquery",
            "SELECT COUNT(*) AS n FROM lineitem l WHERE EXISTS \
             (SELECT 1 FROM orders o WHERE o.o_orderkey = l.l_orderkey)",
            true,
        ),
        (
            "scalar_subquery",
            "SELECT COUNT(*) AS n FROM lineitem \
             WHERE l_quantity > (SELECT AVG(l_quantity) FROM lineitem)",
            true,
        ),
        (
            "order_by_limit",
            "SELECT l_orderkey FROM lineitem ORDER BY l_orderkey LIMIT 10",
            true,
        ),
        (
            "bare_limit",
            "SELECT l_orderkey FROM lineitem LIMIT 10",
            false,
        ),
        (
            "stddev",
            "SELECT STDDEV(l_quantity) AS s FROM lineitem",
            true,
        ),
        (
            "distinct",
            "SELECT DISTINCT l_returnflag FROM lineitem",
            true,
        ),
        (
            "union_all",
            "SELECT COUNT(*) AS n FROM lineitem UNION ALL SELECT COUNT(*) FROM orders",
            true,
        ),
        (
            "cte",
            "WITH x AS (SELECT * FROM lineitem) SELECT COUNT(*) AS n FROM x",
            true,
        ),
        (
            "derived_table",
            "SELECT COUNT(*) AS n FROM (SELECT * FROM lineitem) t",
            true,
        ),
        (
            "scalar_functions",
            "SELECT UPPER(l_shipmode) AS m, ROUND(SUM(l_extendedprice * (1 - l_discount)), 2) AS rev \
             FROM lineitem WHERE SUBSTRING(l_shipinstruct, 1, 4) = 'TAKE' \
             GROUP BY UPPER(l_shipmode) ORDER BY m",
            true,
        ),
    ];

    for (name, sql, exact) in cases {
        let expected = csv_of(&local.sql(sql).await.unwrap_or_else(|e| {
            panic!("single-process engine failed on {name}: {e}");
        }));
        let resp = dsql(&addrs[0], sql).await;
        assert!(
            resp.is_success(),
            "{name}: distributed=1 must now answer `{sql}`: HTTP {} {}",
            resp.status,
            resp.text()
        );
        assert_eq!(
            resp.header("x-qe-distributed"),
            Some("true"),
            "{name} answered locally despite distributed=1"
        );
        if *exact {
            assert_cells_match(&resp.text(), &expected, name);
        } else {
            let got_rows = resp.text().trim().lines().count();
            let want_rows = expected.trim().lines().count();
            assert_eq!(got_rows, want_rows, "{name}: row count");
        }
    }

    // A multi-table query must report a contribution from every node for
    // every table it gathered — that is what makes it distributed rather than
    // locally answered with extra steps.
    let resp = dsql(
        &addrs[1],
        "SELECT COUNT(*) AS n FROM lineitem JOIN orders ON l_orderkey = o_orderkey",
    )
    .await;
    let dist: serde_json::Value = serde_json::from_str(
        resp.header("x-qe-distribution")
            .expect("distribution header"),
    )
    .expect("distribution header is JSON");
    assert_eq!(dist["shape"], "gather");
    let tables_seen: std::collections::BTreeSet<&str> = dist["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|n| n["table"].as_str().unwrap())
        .collect();
    assert_eq!(
        tables_seen.into_iter().collect::<Vec<_>>(),
        vec!["lineitem", "orders"],
        "both joined tables must have been gathered"
    );

    shutdown_all(nodes).await;
}

/// What still cannot be distributed is refused with the reason — never
/// answered approximately, and never silently answered locally.
#[tokio::test]
async fn what_cannot_be_distributed_is_refused_by_name() {
    let nodes = start_cluster(3).await;
    let addrs: Vec<String> = nodes.iter().map(|h| h.local_addr().to_string()).collect();
    for a in &addrs {
        assert!(wait_ready(a, Duration::from_secs(30)).await);
    }

    let cases: &[(&str, &str)] = &[
        // The engine itself has no window functions yet; gather widens
        // distributed support to exactly the local envelope, never past it.
        ("SELECT SUM(l_quantity) OVER () FROM lineitem", "indow"),
        // No base table: there is nothing to shard.
        ("SELECT 1", "no base table"),
        ("DROP TABLE lineitem", "only SELECT"),
    ];

    for (sql, needle) in cases {
        let resp = dsql(&addrs[0], sql).await;
        assert_eq!(
            resp.status,
            501,
            "`{sql}` must be refused with 501, got {} {}",
            resp.status,
            resp.text()
        );
        let msg = resp.text();
        assert!(
            msg.to_lowercase().contains(&needle.to_lowercase()),
            "the rejection of `{sql}` must name the reason ({needle}); said: {msg}"
        );
    }

    // ...and in `auto` mode a gather-shaped query is answered LOCALLY over
    // this node's full copy of the data, with the response saying so and why:
    // when every node already holds all the data, moving it first is never
    // the faster correct answer. `distributed=1` remains the way to force it.
    let resp = http_client::post_text(
        &addrs[0],
        "/sql?format=csv&distributed=auto",
        "SELECT COUNT(*) AS n FROM lineitem JOIN orders ON l_orderkey = o_orderkey",
        HTTP_TIMEOUT,
    )
    .await
    .unwrap();
    assert!(resp.is_success(), "{} {}", resp.status, resp.text());
    assert_eq!(resp.header("x-qe-distributed"), Some("false"));
    assert!(
        resp.header("x-qe-distributed-skipped")
            .is_some_and(|r| r.contains("cross-shard joins")),
        "auto mode must record WHY it did not distribute: {:?}",
        resp.header("x-qe-distributed-skipped")
    );

    shutdown_all(nodes).await;
}

/// The full TPC-H suite, distributed. Every query the single-process engine
/// answers must come back identical through `distributed=1` — joins,
/// correlated subqueries, ORDER BY, LIMIT, HAVING, CASE, all of it.
#[tokio::test]
async fn all_22_tpch_queries_match_single_process_distributed() {
    let nodes = start_cluster(3).await;
    let addrs: Vec<String> = nodes.iter().map(|h| h.local_addr().to_string()).collect();
    for a in &addrs {
        assert!(wait_ready(a, Duration::from_secs(30)).await);
    }
    let local = in_process_context();

    for q in 1..=22 {
        let sql = query_engine::tpch::get_query(q).unwrap_or_else(|| panic!("no TPC-H Q{q}"));
        let expected = csv_of(&local.sql(sql).await.unwrap_or_else(|e| {
            panic!("single-process engine failed on Q{q}: {e}");
        }));
        // Rotate the initiator so all three nodes coordinate some queries.
        let addr = &addrs[q % addrs.len()];
        let resp = dsql(addr, sql).await;
        assert!(
            resp.is_success(),
            "Q{q} via distributed=1: HTTP {} {}",
            resp.status,
            resp.text()
        );
        assert_eq!(
            resp.header("x-qe-distributed"),
            Some("true"),
            "Q{q} answered locally despite distributed=1"
        );
        assert_cells_match(&resp.text(), &expected, &format!("TPC-H Q{q}"));
    }

    shutdown_all(nodes).await;
}

/// A node that dies mid-query must fail the query, loudly, naming itself —
/// never return the surviving shards' partial answer as if it were complete.
///
/// Made deterministic by giving the nodes an effectively infinite discovery
/// interval and driving convergence by hand: the initiator therefore still
/// believes the dead node is a member when the query is issued, which is
/// exactly the mid-query case. (A slower test that waited for the prober would
/// instead be testing the *recovery* path, which the second half checks.)
#[tokio::test]
async fn a_node_that_dies_fails_the_query_instead_of_answering_partially() {
    isolate_env();
    let mut handles = Vec::new();
    for i in 0..3 {
        let opts = ServeOptions {
            bind: "127.0.0.1:0".into(),
            node_id: Some(i),
            discovery_interval: Duration::from_secs(3600),
            probe_timeout: Duration::from_millis(2000),
            ..Default::default()
        };
        handles.push(spawn(opts, tpch_loader()).await.expect("bind"));
    }
    let addrs: Vec<String> = handles.iter().map(|h| h.local_addr().to_string()).collect();
    let peers: Vec<String> = handles.iter().map(|h| h.address().to_string()).collect();
    // `set_peers` kicks the discovery loop, so this is the ONLY probe round
    // that will happen for the next hour.
    for h in &handles {
        h.set_peers(peers.clone());
    }
    for a in &addrs {
        assert!(
            wait_ready(a, Duration::from_secs(30)).await,
            "{a} never ready"
        );
    }
    assert!(
        converges(Duration::from_secs(20), || {
            let a = addrs[0].clone();
            async move {
                let v = get_json(&a, "/cluster").await;
                v["member_count"].as_u64() == Some(3)
                    && member_identities(&v).iter().all(|(_, _, st)| st == "up")
            }
        })
        .await,
        "cluster never converged"
    );

    // Sanity: the three-node answer is right before anything dies.
    let before = dsql(&addrs[0], "SELECT COUNT(*) AS n FROM lineitem").await;
    assert!(before.is_success());
    assert_eq!(before.header("x-qe-shards"), Some("3"));
    let full = before.text();

    // Kill the last node. Node 0 will not re-probe.
    let victim = handles.pop().unwrap();
    victim.shutdown().await;

    let after = dsql(&addrs[0], "SELECT COUNT(*) AS n FROM lineitem").await;
    assert!(
        !after.is_success(),
        "a dead shard owner must fail the query; instead it returned:\n{}",
        after.text()
    );
    let msg = after.text();
    assert!(
        msg.contains(&addrs[2]),
        "the failure must name the node that did not answer ({}): {msg}",
        addrs[2]
    );
    assert!(
        msg.contains("did not complete shard"),
        "the failure must say what was lost: {msg}"
    );

    // And once the survivors are told the cluster is smaller, they re-divide
    // the work and return the SAME complete answer over two shards.
    for h in &handles {
        h.set_peers(vec![peers[0].clone(), peers[1].clone()]);
    }
    assert!(
        converges(Duration::from_secs(20), || {
            let a = addrs[0].clone();
            async move { get_json(&a, "/cluster").await["member_count"].as_u64() == Some(2) }
        })
        .await,
        "survivors never shrank the membership"
    );
    let recovered = dsql(&addrs[0], "SELECT COUNT(*) AS n FROM lineitem").await;
    assert!(recovered.is_success(), "{}", recovered.text());
    assert_eq!(recovered.header("x-qe-shards"), Some("2"));
    assert_eq!(
        recovered.text(),
        full,
        "the two-shard answer must equal the three-shard one"
    );

    shutdown_all(handles).await;
}

/// Nodes that disagree about what data exists must refuse to answer together.
///
/// Driven through the real `/fragment` endpoint with a deliberately wrong
/// digest, which is what a node holding a stale or partial copy of the table
/// would produce. Without this interlock each node would compute a share of a
/// *different* table and the merge would look perfectly healthy.
#[tokio::test]
async fn a_fragment_whose_digest_disagrees_is_refused() {
    let nodes = start_cluster(1).await;
    let addr = nodes[0].local_addr().to_string();
    assert!(wait_ready(&addr, Duration::from_secs(30)).await);

    let body = serde_json::json!({
        "sql": "SELECT COUNT(*) AS qe_a0 FROM lineitem",
        "table": "lineitem",
        "shard_index": 0,
        "shard_count": 2,
        "splits_digest": 1234567890u64,
    })
    .to_string();

    let resp = http_client::request(
        &addr,
        "POST",
        "/fragment",
        Some("application/json"),
        Some(body.as_bytes()),
        HTTP_TIMEOUT,
    )
    .await
    .expect("POST /fragment");

    assert_eq!(resp.status, 400, "{}", resp.text());
    let msg = resp.text();
    assert!(msg.contains("split digest mismatch"), "{msg}");
    assert!(
        msg.contains("Refusing"),
        "the message must be explicit that no answer is being given: {msg}"
    );

    shutdown_all(nodes).await;
}

// ---------------------------------------------------------------------------
// Real processes, real SIGTERM
// ---------------------------------------------------------------------------

/// Kills every child on drop, including when a test panics part-way through.
/// Without this an assertion failure leaves three servers holding ports.
struct Children(Vec<std::process::Child>);

impl Drop for Children {
    fn drop(&mut self) {
        for c in &mut self.0 {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// Reserve `n` ports by binding and immediately releasing them. Racy in
/// principle, but the alternative — fixed ports — is racy in practice, every
/// time two test binaries run at once.
fn reserve_ports(n: usize) -> Vec<u16> {
    let listeners: Vec<StdListener> = (0..n)
        .map(|_| StdListener::bind("127.0.0.1:0").expect("cannot reserve a port"))
        .collect();
    listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect()
}

fn send_sigterm(pid: u32) {
    let status = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .expect("cannot run kill(1)");
    assert!(status.success(), "kill -TERM {pid} failed");
}

/// GATE: three real processes; SIGTERM shuts one down cleanly and the other two
/// report it unreachable rather than crashing.
///
/// This is the highest-fidelity test available on a machine without Docker: the
/// same binary a pod would run, three separate address spaces, real TCP, and
/// the exact signal Kubernetes sends before it resorts to SIGKILL.
#[tokio::test]
async fn three_real_processes_serve_and_survive_a_sigterm() {
    isolate_env();
    let bin = env!("CARGO_BIN_EXE_query_engine");
    let ports = reserve_ports(3);
    let peer_list = ports
        .iter()
        .map(|p| format!("127.0.0.1:{p}"))
        .collect::<Vec<_>>()
        .join(",");

    let mut children = Children(Vec::new());
    for (i, p) in ports.iter().enumerate() {
        let child = std::process::Command::new(bin)
            .args([
                "serve",
                "--bind",
                &format!("127.0.0.1:{p}"),
                "--node-id",
                &i.to_string(),
                "--peers",
                &peer_list,
                "--data",
                &data_dir(),
                "--discovery-interval-ms",
                "200",
            ])
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("cannot spawn query_engine serve");
        children.0.push(child);
    }

    let addrs: Vec<String> = ports.iter().map(|p| format!("127.0.0.1:{p}")).collect();
    for a in &addrs {
        assert!(
            wait_ready(a, Duration::from_secs(60)).await,
            "{a} never became ready"
        );
    }

    // All three agree, out of three separate processes.
    let converged = converges(Duration::from_secs(30), || {
        let addrs = addrs.clone();
        async move {
            let mut views = Vec::new();
            for a in &addrs {
                let v = get_json(a, "/cluster").await;
                if v["member_count"].as_u64() != Some(3) {
                    return false;
                }
                views.push(member_identities(&v));
            }
            views.iter().all(|v| *v == views[0])
                && views[0]
                    .iter()
                    .all(|(_, id, st)| id.is_some() && st == "up")
        }
    })
    .await;
    assert!(converged, "three real processes never agreed on membership");

    // Each answers independently and identically.
    let local = in_process_context();
    let sql = "SELECT o_orderstatus, COUNT(*) AS c FROM orders GROUP BY o_orderstatus ORDER BY o_orderstatus";
    let expected = csv_of(&local.sql(sql).await.unwrap());
    for (i, a) in addrs.iter().enumerate() {
        let r = http_client::post_text(a, "/sql?format=csv", sql, HTTP_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("process {i}: {e}"));
        assert!(
            r.is_success(),
            "process {i}: HTTP {} {}",
            r.status,
            r.text()
        );
        assert_eq!(r.text(), expected, "process {i} answered differently");
    }

    // SIGTERM the last one: it must exit on its own, with status 0.
    //
    // Reaped with `try_wait`, deliberately, not with `kill -0`: a child that
    // has exited but not been waited on is a zombie, and `kill -0` on a zombie
    // *succeeds*. Probing liveness that way would report "ignored SIGTERM" for
    // a process that shut down perfectly.
    send_sigterm(children.0[2].id());
    let deadline = Instant::now() + Duration::from_secs(20);
    let mut exit_status = None;
    while Instant::now() < deadline {
        match children.0[2].try_wait().expect("try_wait") {
            Some(status) => {
                exit_status = Some(status);
                break;
            }
            None => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
    let status = exit_status.expect("process ignored SIGTERM — Kubernetes would SIGKILL it");
    assert!(
        status.success(),
        "SIGTERM must be a clean exit, got {status:?}"
    );

    // The survivors report it down and keep working.
    let survivors = &addrs[..2];
    let noticed = converges(Duration::from_secs(30), || {
        let survivors = survivors.to_vec();
        let gone = addrs[2].clone();
        async move {
            for a in &survivors {
                let v = get_json(a, "/cluster").await;
                if v["member_count"].as_u64() != Some(3) {
                    return false;
                }
                if !member_identities(&v)
                    .iter()
                    .any(|(addr, _, st)| *addr == gone && st == "down")
                {
                    return false;
                }
            }
            true
        }
    })
    .await;
    assert!(noticed, "survivors never noticed the SIGTERMed process");

    for a in survivors {
        let r = http_client::post_text(a, "/sql?format=csv", sql, HTTP_TIMEOUT)
            .await
            .unwrap();
        assert!(r.is_success());
        assert_eq!(
            r.text(),
            expected,
            "a survivor's answer changed after a peer died"
        );
    }
}
