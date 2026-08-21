//! Arrow Flight endpoint integration tests: the in-process half of the
//! flight gate (`scripts/flight_validate.py` drives the shipped binary; these
//! drive `spawn()` directly with the arrow-flight client).
//!
//! Uses `data/tpch-1mb`, same convention as `distributed_cluster.rs`: missing
//! data is a loud panic, never a silent skip. No fixed ports anywhere — every
//! node binds `:0` and the bound address is read back.

use std::sync::OnceLock;
use std::time::{Duration, Instant};

use arrow_flight::client::FlightClient;
use arrow_flight::decode::DecodedPayload;
use arrow_flight::{Action, FlightDescriptor, Ticket};
use futures::TryStreamExt;
use query_engine::distributed::{spawn, ServeOptions, ServerHandle, TableLoader};
use query_engine::ExecutionContext;

const DATA: &str = "data/tpch-1mb";
const TABLES: [&str; 8] = [
    "customer", "lineitem", "nation", "orders", "part", "partsupp", "region", "supplier",
];

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
            let path = format!("{}/{t}.parquet", data_dir());
            ctx.register_parquet(t, &path)?;
        }
        Ok(ctx)
    })
}

fn test_options(node_id: u64) -> ServeOptions {
    ServeOptions {
        bind: "127.0.0.1:0".into(),
        node_id: Some(node_id),
        discovery_interval: Duration::from_millis(50),
        probe_timeout: Duration::from_millis(2000),
        ..Default::default()
    }
}

async fn flight_client(handle: &ServerHandle) -> FlightClient {
    let addr = handle.flight_addr().expect("flight enabled");
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .expect("uri")
        .connect()
        .await
        .expect("flight connect");
    FlightClient::new(channel)
}

/// Wait until the node has its tables (readiness also needs discovery, which
/// single-node tests do not exercise; tables are what the RPCs gate on).
async fn wait_tables(handle: &ServerHandle) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !handle.state().tables_loaded() {
        assert!(
            Instant::now() < deadline,
            "tables never loaded: {:?}",
            handle.state().load_error()
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Run SQL over Flight (GetFlightInfo → DoGet), returning the batches and the
/// trailing execution-metadata JSON.
async fn flight_sql(
    client: &mut FlightClient,
    sql: &str,
) -> (Vec<arrow::record_batch::RecordBatch>, serde_json::Value) {
    let info = client
        .get_flight_info(FlightDescriptor::new_cmd(sql.as_bytes().to_vec()))
        .await
        .expect("get_flight_info");
    let ticket = info.endpoint[0].ticket.clone().expect("ticket");
    let mut decoder = client.do_get(ticket).await.expect("do_get").into_inner();
    let mut batches = Vec::new();
    let mut meta = None;
    while let Some(d) = decoder.try_next().await.expect("stream") {
        if !d.inner.app_metadata.is_empty() {
            meta = Some(
                serde_json::from_slice::<serde_json::Value>(&d.inner.app_metadata)
                    .expect("metadata is JSON"),
            );
        }
        if let DecodedPayload::RecordBatch(b) = d.payload {
            batches.push(b);
        }
    }
    (batches, meta.expect("trailing metadata message"))
}

fn render(batches: &[arrow::record_batch::RecordBatch]) -> String {
    arrow::util::pretty::pretty_format_batches(batches)
        .expect("format")
        .to_string()
}

// ---------------------------------------------------------------------------
// Catalog surface
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_flights_names_every_table_and_get_schema_matches() {
    isolate_env();
    let node = spawn(test_options(0), tpch_loader()).await.expect("bind");
    wait_tables(&node).await;
    let mut client = flight_client(&node).await;

    let infos: Vec<_> = client
        .list_flights(Vec::<u8>::new())
        .await
        .expect("list_flights")
        .try_collect()
        .await
        .expect("collect");
    let mut names: Vec<String> = infos
        .iter()
        .map(|i| i.flight_descriptor.as_ref().expect("descriptor").path[0].clone())
        .collect();
    names.sort();
    assert_eq!(names, TABLES.map(String::from).to_vec());

    let schema = client
        .get_schema(FlightDescriptor::new_path(vec!["lineitem".into()]))
        .await
        .expect("get_schema");
    assert_eq!(schema.fields().len(), 16, "lineitem has 16 columns");

    node.shutdown().await;
}

#[tokio::test]
async fn cluster_action_reports_flight_addresses() {
    isolate_env();
    let node = spawn(test_options(0), tpch_loader()).await.expect("bind");
    wait_tables(&node).await;
    let mut client = flight_client(&node).await;

    let bodies: Vec<_> = client
        .do_action(Action::new("cluster", Vec::<u8>::new()))
        .await
        .expect("do_action")
        .try_collect()
        .await
        .expect("collect");
    let view: serde_json::Value = serde_json::from_slice(&bodies[0]).expect("json");
    assert_eq!(
        view["node"]["flight"].as_str().expect("flight advertised"),
        node.state().flight_address.as_deref().expect("set"),
    );

    node.shutdown().await;
}

// ---------------------------------------------------------------------------
// Query round trips
// ---------------------------------------------------------------------------

#[tokio::test]
async fn query_results_match_the_engine_run_directly() {
    isolate_env();
    let node = spawn(test_options(0), tpch_loader()).await.expect("bind");
    wait_tables(&node).await;
    let mut client = flight_client(&node).await;

    // A scan+filter, an aggregate, and a join — the three plan shapes the
    // gate cares about. Each has an ORDER BY so row order is defined.
    for sql in [
        "SELECT l_orderkey, l_extendedprice FROM lineitem \
         WHERE l_shipdate < '1992-06-01' ORDER BY l_orderkey, l_linenumber LIMIT 50",
        "SELECT l_returnflag, l_linestatus, COUNT(*) AS n, SUM(l_quantity) AS q \
         FROM lineitem GROUP BY l_returnflag, l_linestatus \
         ORDER BY l_returnflag, l_linestatus",
        "SELECT o_orderpriority, COUNT(*) AS n FROM orders, lineitem \
         WHERE o_orderkey = l_orderkey AND l_shipdate < '1995-01-01' \
         GROUP BY o_orderpriority ORDER BY o_orderpriority",
    ] {
        let (batches, meta) = flight_sql(&mut client, sql).await;

        let ctx = node.state().context().expect("ready");
        let local = ctx.sql(sql).await.expect("local run");

        assert_eq!(render(&batches), render(&local.batches), "sql: {sql}");
        assert_eq!(
            meta["rows"].as_u64().expect("rows") as usize,
            local.row_count,
            "reported rows, sql: {sql}"
        );
        assert_eq!(meta["distributed"], serde_json::json!(false));
        assert!(
            meta["skipped_reason"]
                .as_str()
                .expect("single node states why it did not distribute")
                .contains("only one cluster member"),
            "got: {}",
            meta["skipped_reason"]
        );
    }

    node.shutdown().await;
}

#[tokio::test]
async fn empty_result_streams_schema_and_no_rows() {
    isolate_env();
    let node = spawn(test_options(0), tpch_loader()).await.expect("bind");
    wait_tables(&node).await;
    let mut client = flight_client(&node).await;

    let sql = "SELECT * FROM orders WHERE 1 = 0";
    let info = client
        .get_flight_info(FlightDescriptor::new_cmd(sql.as_bytes().to_vec()))
        .await
        .expect("info");
    let schema = arrow::datatypes::Schema::try_from(arrow_flight::IpcMessage(info.schema.clone()))
        .expect("info schema decodes");
    assert_eq!(schema.fields().len(), 9, "orders has 9 columns");

    let (batches, meta) = flight_sql(&mut client, sql).await;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(rows, 0);
    assert_eq!(meta["rows"], serde_json::json!(0));

    node.shutdown().await;
}

// ---------------------------------------------------------------------------
// Error mapping
// ---------------------------------------------------------------------------

fn status_of(err: arrow_flight::error::FlightError) -> tonic::Code {
    match err {
        arrow_flight::error::FlightError::Tonic(s) => s.code(),
        other => panic!("expected a tonic status, got {other:?}"),
    }
}

#[tokio::test]
async fn error_statuses_are_mapped_not_generic() {
    isolate_env();
    let node = spawn(test_options(0), tpch_loader()).await.expect("bind");
    wait_tables(&node).await;
    let mut client = flight_client(&node).await;

    let bad_sql = client
        .get_flight_info(FlightDescriptor::new_cmd(b"SELEC 1".to_vec()))
        .await
        .expect_err("syntax error must fail");
    assert_eq!(status_of(bad_sql), tonic::Code::InvalidArgument);

    let no_table = client
        .get_flight_info(FlightDescriptor::new_cmd(
            b"SELECT * FROM no_such_table".to_vec(),
        ))
        .await
        .expect_err("unknown table must fail");
    assert_eq!(status_of(no_table), tonic::Code::NotFound);

    let no_schema = client
        .get_schema(FlightDescriptor::new_path(vec!["nope".into()]))
        .await
        .expect_err("unknown table schema must fail");
    assert_eq!(status_of(no_schema), tonic::Code::NotFound);

    let bad_ticket = client
        .do_get(Ticket::new(b"not json".to_vec()))
        .await
        .expect_err("malformed ticket must fail");
    assert_eq!(status_of(bad_ticket), tonic::Code::InvalidArgument);

    let huge_ticket = client
        .do_get(Ticket::new(vec![b'x'; 1024 * 1024 + 1]))
        .await
        .expect_err("oversized ticket must fail");
    assert_eq!(status_of(huge_ticket), tonic::Code::InvalidArgument);

    node.shutdown().await;
}

#[tokio::test]
async fn a_node_whose_tables_failed_to_load_is_unavailable() {
    isolate_env();
    let loader: TableLoader = Box::new(|| {
        Err(query_engine::error::QueryError::Execution(
            "deliberately broken loader".into(),
        ))
    });
    let node = spawn(test_options(0), loader).await.expect("bind");
    let deadline = Instant::now() + Duration::from_secs(30);
    while node.state().load_error().is_none() {
        assert!(Instant::now() < deadline, "load error never surfaced");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let mut client = flight_client(&node).await;

    let err = client
        .get_flight_info(FlightDescriptor::new_cmd(b"SELECT 1".to_vec()))
        .await
        .expect_err("not-ready node must refuse");
    assert_eq!(status_of(err), tonic::Code::Unavailable);

    node.shutdown().await;
}

#[tokio::test]
async fn flight_can_be_disabled() {
    isolate_env();
    let node = spawn(
        ServeOptions {
            flight_bind: Some("none".into()),
            ..test_options(0)
        },
        tpch_loader(),
    )
    .await
    .expect("bind");
    assert!(node.flight_addr().is_none(), "no flight listener");
    node.shutdown().await;
}

// ---------------------------------------------------------------------------
// Distributed: three real-TCP nodes, scatter through Flight
// ---------------------------------------------------------------------------

#[tokio::test]
async fn scatter_distributes_through_flight_and_agrees_across_nodes() {
    isolate_env();
    let mut handles = Vec::new();
    for i in 0..3u64 {
        handles.push(spawn(test_options(i), tpch_loader()).await.expect("bind"));
    }
    let addrs: Vec<String> = handles.iter().map(|h| h.address().to_string()).collect();
    for h in &handles {
        h.set_peers(addrs.clone());
    }
    let deadline = Instant::now() + Duration::from_secs(60);
    for h in &handles {
        while !h.state().ready() || h.state().membership.members().len() < 3 {
            assert!(Instant::now() < deadline, "cluster never converged");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // Scatter-eligible shape (no global ORDER BY); auto must distribute on a
    // 3-member cluster, and every node must produce the same answer.
    let sql = "SELECT l_returnflag, COUNT(*) AS n, SUM(l_quantity) AS q \
               FROM lineitem GROUP BY l_returnflag";
    let mut rendered = Vec::new();
    for h in &handles {
        let mut client = flight_client(h).await;
        let (batches, meta) = flight_sql(&mut client, sql).await;
        assert_eq!(
            meta["distributed"],
            serde_json::json!(true),
            "auto must scatter on a 3-member cluster (node {})",
            h.node_id()
        );
        assert_eq!(meta["shards"], serde_json::json!(3));
        // Canonicalize row order before comparing across nodes.
        let mut lines: Vec<String> = render(&batches).lines().map(String::from).collect();
        lines.sort();
        rendered.push(lines);
    }
    assert_eq!(rendered[0], rendered[1], "node 1 differs from node 0");
    assert_eq!(rendered[0], rendered[2], "node 2 differs from node 0");

    // Every member advertises a distinct flight address in the shared view.
    let mut client = flight_client(&handles[0]).await;
    let bodies: Vec<_> = client
        .do_action(Action::new("cluster", Vec::<u8>::new()))
        .await
        .expect("do_action")
        .try_collect()
        .await
        .expect("collect");
    let view: serde_json::Value = serde_json::from_slice(&bodies[0]).expect("json");
    let mut flights: Vec<String> = view["members"]
        .as_array()
        .expect("members")
        .iter()
        .map(|m| {
            m["flight"]
                .as_str()
                .expect("every member gossips flight")
                .to_string()
        })
        .collect();
    flights.sort();
    flights.dedup();
    assert_eq!(flights.len(), 3, "three distinct flight addresses");

    for h in handles {
        h.shutdown().await;
    }
}
