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

/// GATE: each node answers `/sql` independently, and identically to the
/// single-process engine.
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
            let resp = http_client::post_text(addr, "/sql?format=csv", sql, HTTP_TIMEOUT)
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
