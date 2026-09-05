//! Arrow Flight endpoint for `serve` — the CLIENT-FACING gRPC front door.
//!
//! This is the second front door next to `POST /sql`, not a second engine:
//! queries go through the same [`super::server::execute_statement`] path, so
//! Flight and HTTP can never disagree about when a query distributes. The
//! INTERNAL shuffle/fragment transport deliberately stays on hyper +
//! arrow-ipc ("Flight semantics, not the Flight crate" — DISTRIBUTED-DESIGN.md);
//! arrow-flight 53 is used here because it tracks the exact arrow major the
//! engine (and Lance) pin, so the risk that ruling guarded against does not
//! arise for the client edge.
//!
//! Surface:
//! - `Handshake` — trivial success (no auth, parity with HTTP).
//! - `ListFlights` — one `FlightInfo` per registered table (path descriptor,
//!   schema attached, no endpoints: tables are not directly fetchable).
//! - `GetSchema` — path descriptor = table name → that table's Arrow schema.
//! - `GetFlightInfo` / `DoGet` — the query path (SQL in a cmd descriptor,
//!   self-contained JSON ticket, results streamed as Flight data).
//! - `DoAction("cluster")` — the membership view, parity with `GET /cluster`.
//! - Everything else — `unimplemented`, saying which RPC by name.
//!
//! Error mapping, used by every RPC here: parse/bind/type errors →
//! `InvalidArgument`; unknown table/column → `NotFound`; tables not loaded →
//! `Unavailable`; `NotImplemented` → `Unimplemented`; anything else →
//! `Internal`. The message is always the engine's own error text.

use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::Schema;
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightEndpoint, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::StreamExt;
use hyper::body::Bytes;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tonic::{Request, Response, Status, Streaming};

use crate::error::QueryError;

use super::query_log::{FrontDoor, QueryOrigin};
use super::server::{execute_statement, DistMode, ExecError, ExecOutcome, Executed, NodeState};

/// SQL statements and tickets share the HTTP body cap: a ticket is a statement
/// plus a few fixed fields, and an unbounded ticket is an unbounded allocation.
pub(crate) const MAX_TICKET_BYTES: usize = 1024 * 1024;

/// The self-contained `DoGet` ticket. Stateless on purpose: any node that can
/// serve `/sql` can serve any ticket it (or a peer) minted — no server-side
/// query registry, no expiry machinery, and a node crash between
/// `GetFlightInfo` and `DoGet` costs the client one retry, not a session.
#[derive(serde::Serialize, serde::Deserialize)]
struct QueryTicket {
    v: u32,
    sql: String,
    #[serde(default = "default_mode")]
    mode: String,
}

fn default_mode() -> String {
    "auto".to_string()
}

/// `distributed=` values, shared vocabulary with the HTTP query string.
fn parse_mode(mode: &str) -> Result<DistMode, Status> {
    match mode {
        "auto" => Ok(DistMode::Auto),
        "1" | "true" | "yes" | "force" => Ok(DistMode::Force),
        "0" | "false" | "no" | "local" | "off" => Ok(DistMode::Off),
        other => Err(Status::invalid_argument(format!(
            "unknown distributed mode {other:?}; expected auto, force or off"
        ))),
    }
}

/// A command descriptor is either raw SQL bytes, or — when it starts with
/// `{` — a JSON object `{"sql": "...", "mode": "auto|force|off"}` for clients
/// that need to pick the distribution mode.
fn parse_command(cmd: &[u8]) -> Result<(String, String), Status> {
    if cmd.len() > MAX_TICKET_BYTES {
        return Err(Status::invalid_argument(format!(
            "command exceeds {MAX_TICKET_BYTES} bytes"
        )));
    }
    let text = std::str::from_utf8(cmd)
        .map_err(|_| Status::invalid_argument("command is not valid UTF-8"))?
        .trim();
    if text.is_empty() {
        return Err(Status::invalid_argument("empty command"));
    }
    if text.starts_with('{') {
        #[derive(serde::Deserialize)]
        struct Cmd {
            sql: String,
            #[serde(default = "default_mode")]
            mode: String,
        }
        let c: Cmd = serde_json::from_str(text)
            .map_err(|e| Status::invalid_argument(format!("malformed command JSON: {e}")))?;
        let sql = c.sql.trim().to_string();
        if sql.is_empty() {
            return Err(Status::invalid_argument("empty sql in command JSON"));
        }
        parse_mode(&c.mode)?;
        Ok((sql, c.mode))
    } else {
        Ok((text.to_string(), default_mode()))
    }
}

/// Execution metadata, the Flight analogue of the `x-qe-*` response headers.
/// Carried on the trailing zero-row batch message (see
/// [`encode_flight_stream`]), which every Arrow client family surfaces as a
/// final chunk with `app_metadata` set.
fn outcome_metadata(outcome: &ExecOutcome) -> serde_json::Value {
    let mut m = serde_json::json!({
        "rows": outcome.result.row_count,
        "elapsed_ms": outcome.elapsed_ms,
        "distributed": outcome.distribution.is_some(),
    });
    if let Some(d) = &outcome.distribution {
        m["shards"] = serde_json::json!(d.nodes.len());
        m["imbalance"] = serde_json::json!(d.imbalance);
        m["wall_time_spread"] = serde_json::json!(d.wall_time_spread);
        if let Ok(full) = serde_json::to_value(d) {
            m["distribution"] = full;
        }
    }
    if let Some(reason) = &outcome.fallback_reason {
        m["skipped_reason"] = serde_json::json!(reason);
    }
    m
}

fn exec_error_status(e: ExecError) -> Status {
    match e {
        ExecError::NotReady(reason) => Status::unavailable(reason),
        ExecError::Query(e) => query_error_status(&e),
        ExecError::TaskFailed(e) => Status::internal(format!("query task failed: {e}")),
    }
}

/// Largest row count encoded into one `FlightData`. gRPC clients default to a
/// 4MB receive limit; the engine's ~8k-row batches with wide string columns
/// can brush against it, so batches are re-sliced before encoding.
const MAX_ENCODE_ROWS: usize = 4096;

/// Encode a result as the DoGet message sequence: schema, then every batch
/// (dictionaries included), then a zero-row batch of the same schema whose
/// `app_metadata` carries the execution facts.
///
/// Hand-rolled rather than `FlightDataEncoderBuilder` for one interop reason:
/// there is no metadata-ONLY trailer both client families accept. arrow-rs'
/// decoder refuses an empty `data_header` ("Error decoding root message") and
/// Arrow C++ / pyarrow refuses a NONE-typed one ("Header-type … is not
/// RecordBatch"). A zero-row RecordBatch message is legal everywhere, decodes
/// to nothing, and its `app_metadata` rides along in both implementations.
fn encode_flight_stream(
    batches: Vec<arrow::record_batch::RecordBatch>,
    schema: arrow::datatypes::SchemaRef,
    metadata: Bytes,
) -> Result<Vec<FlightData>, Status> {
    use arrow::ipc::writer::{DictionaryTracker, IpcDataGenerator};

    let opts = IpcWriteOptions::default();
    let generator = IpcDataGenerator::default();
    // Replacement allowed, same as arrow's own StreamWriter: the trailing
    // empty batch re-announces (empty) dictionaries for dictionary columns.
    let mut tracker = DictionaryTracker::new(false);
    let ipc_err =
        |e: arrow::error::ArrowError| Status::internal(format!("flight encoding failed: {e}"));

    let mut out: Vec<FlightData> = Vec::new();
    out.push(SchemaAsIpc::new(&schema, &opts).into());
    let trailer_batch = arrow::record_batch::RecordBatch::new_empty(schema.clone());
    for batch in batches.iter().chain(std::iter::once(&trailer_batch)) {
        let mut offset = 0;
        loop {
            let len = (batch.num_rows() - offset).min(MAX_ENCODE_ROWS);
            let slice = batch.slice(offset, len);
            let (dicts, data) = generator
                .encoded_batch(&slice, &mut tracker, &opts)
                .map_err(ipc_err)?;
            for d in dicts {
                out.push(d.into());
            }
            out.push(data.into());
            offset += len;
            if offset >= batch.num_rows() {
                break;
            }
        }
    }
    out.last_mut().expect("at least the trailer").app_metadata = metadata;
    Ok(out)
}

/// The Flight front door. Cheap to clone; state is shared with the HTTP server.
#[derive(Clone)]
pub(crate) struct QeFlightService {
    state: Arc<NodeState>,
}

impl QeFlightService {
    pub(crate) fn new(state: Arc<NodeState>) -> Self {
        Self { state }
    }

    /// The engine context, or the same not-ready story `/sql` tells.
    fn context(&self) -> Result<Arc<crate::execution::ExecutionContext>, Status> {
        self.state.context().ok_or_else(|| {
            let reason = self
                .state
                .load_error()
                .map(|e| format!("tables failed to load: {e}"))
                .unwrap_or_else(|| "tables are still loading".to_string());
            Status::unavailable(reason)
        })
    }
}

/// Map an engine error onto the gRPC status vocabulary. Kept in one place so
/// every RPC agrees; the message is the engine's error Display, verbatim.
pub(crate) fn query_error_status(e: &QueryError) -> Status {
    match e {
        QueryError::Parse(_) | QueryError::Bind(_) | QueryError::Type(_) | QueryError::Plan(_) => {
            Status::invalid_argument(e.to_string())
        }
        QueryError::TableNotFound(_) | QueryError::ColumnNotFound(_) => {
            Status::not_found(e.to_string())
        }
        QueryError::NotImplemented(_) => Status::unimplemented(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

fn schema_to_result(schema: &Schema) -> Result<SchemaResult, Status> {
    SchemaAsIpc::new(schema, &IpcWriteOptions::default())
        .try_into()
        .map_err(|e| Status::internal(format!("cannot encode schema: {e}")))
}

type PinStream<T> = Pin<Box<dyn futures::Stream<Item = Result<T, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightService for QeFlightService {
    type HandshakeStream = PinStream<HandshakeResponse>;
    type ListFlightsStream = PinStream<FlightInfo>;
    type DoGetStream = PinStream<FlightData>;
    type DoPutStream = PinStream<PutResult>;
    type DoExchangeStream = PinStream<FlightData>;
    type DoActionStream = PinStream<arrow_flight::Result>;
    type ListActionsStream = PinStream<ActionType>;

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        // No authentication, same as the HTTP server. Answer one empty
        // response so clients that insist on handshaking proceed.
        let out = futures::stream::once(async { Ok(HandshakeResponse::default()) });
        Ok(Response::new(out.boxed()))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        let ctx = self.context()?;
        let mut names = ctx.table_names();
        names.sort();
        let mut infos = Vec::with_capacity(names.len());
        for name in names {
            let Some(schema) = ctx.table_schema(&name) else {
                continue; // racing a deregistration; skip rather than fail the list
            };
            let info = FlightInfo::new()
                .try_with_schema(&schema)
                .map_err(|e| Status::internal(format!("cannot encode schema: {e}")))?
                .with_descriptor(FlightDescriptor::new_path(vec![name]));
            infos.push(Ok(info));
        }
        Ok(Response::new(futures::stream::iter(infos).boxed()))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        if descriptor.cmd.is_empty() {
            return Err(Status::invalid_argument(
                "GetFlightInfo needs a command descriptor carrying SQL",
            ));
        }
        let (sql, mode) = parse_command(&descriptor.cmd)?;
        let ctx = self.context()?;

        // Plan only — the query runs in DoGet. Planning is what validates the
        // SQL and yields the result schema; a plan that fails here fails with
        // the same status DoGet would have produced.
        let schema = ctx
            .physical_plan(&sql)
            .map_err(|e| query_error_status(&e))?
            .schema();

        let ticket = QueryTicket { v: 1, sql, mode };
        let ticket_bytes =
            serde_json::to_vec(&ticket).map_err(|e| Status::internal(e.to_string()))?;

        // One endpoint, no locations: per the Flight spec an empty location
        // list means "fetch from the service you are talking to", which is
        // exactly the contract — this node coordinates scatter/gather
        // internally. (An advertised address here would break clients that
        // cannot resolve the cluster-internal hostname.)
        let endpoint = FlightEndpoint::new().with_ticket(Ticket::new(ticket_bytes));
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("cannot encode schema: {e}")))?
            .with_descriptor(descriptor)
            .with_endpoint(endpoint)
            .with_app_metadata(Bytes::from(
                serde_json::json!({"node_id": self.state.node_id}).to_string(),
            ));
        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("PollFlightInfo"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let ctx = self.context()?;
        // Path descriptor: a registered table's schema.
        if let Some(name) = descriptor.path.first() {
            let schema = ctx
                .table_schema(name)
                .ok_or_else(|| Status::not_found(format!("no such table: {name}")))?;
            return Ok(Response::new(schema_to_result(&schema)?));
        }
        // Command descriptor: the result schema of the SQL, plan-only.
        if !descriptor.cmd.is_empty() {
            let (sql, _mode) = parse_command(&descriptor.cmd)?;
            let schema = ctx
                .physical_plan(&sql)
                .map_err(|e| query_error_status(&e))?
                .schema();
            return Ok(Response::new(schema_to_result(&schema)?));
        }
        Err(Status::invalid_argument(
            "GetSchema needs a path descriptor naming a table or a command descriptor carrying SQL",
        ))
    }

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        let peer = request.remote_addr().map(|a| a.to_string());
        let ticket = request.into_inner().ticket;
        if ticket.len() > MAX_TICKET_BYTES {
            return Err(Status::invalid_argument(format!(
                "ticket exceeds {MAX_TICKET_BYTES} bytes"
            )));
        }
        let ticket: QueryTicket = serde_json::from_slice(&ticket)
            .map_err(|e| Status::invalid_argument(format!("malformed ticket: {e}")))?;
        if ticket.v != 1 {
            return Err(Status::invalid_argument(format!(
                "unknown ticket version {}",
                ticket.v
            )));
        }
        let mode = parse_mode(&ticket.mode)?;

        let origin = QueryOrigin {
            front_door: FrontDoor::Flight,
            client_addr: peer,
        };
        let Executed { query_id, outcome } =
            execute_statement(&self.state, &ticket.sql, mode, origin).await;
        let outcome = outcome.map_err(exec_error_status)?;

        let mut meta = outcome_metadata(&outcome);
        if let Some(id) = &query_id {
            meta["query_id"] = serde_json::json!(id);
        }
        let metadata = Bytes::from(meta.to_string());

        // Same schema convention as the HTTP encoder: be governed by the data
        // when there is any, by the plan's description only when there is none.
        let schema = outcome
            .result
            .batches
            .first()
            .map(|b| b.schema())
            .unwrap_or_else(|| outcome.result.schema.clone());
        let batches = outcome.result.batches;

        let messages = encode_flight_stream(batches, schema, metadata)?;
        if let Some(id) = &query_id {
            let bytes: usize = messages
                .iter()
                .map(|m| m.data_header.len() + m.data_body.len())
                .sum();
            self.state.query_log.set_result(id, bytes, "flight");
        }
        Ok(Response::new(
            futures::stream::iter(messages.into_iter().map(Ok)).boxed(),
        ))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("DoPut"))
    }

    async fn do_exchange(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("DoExchange"))
    }

    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        let action = request.into_inner();
        match action.r#type.as_str() {
            "cluster" => {
                let view = super::server::cluster_view(&self.state);
                let bytes = serde_json::to_vec_pretty(&view)
                    .map_err(|e| Status::internal(format!("cannot serialize cluster view: {e}")))?;
                let out = futures::stream::once(async move {
                    Ok(arrow_flight::Result { body: bytes.into() })
                });
                Ok(Response::new(out.boxed()))
            }
            other => Err(Status::unimplemented(format!(
                "unknown action {other:?}; try \"cluster\""
            ))),
        }
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![Ok(ActionType {
            r#type: "cluster".to_string(),
            description: "membership view as JSON, parity with GET /cluster".to_string(),
        })];
        Ok(Response::new(futures::stream::iter(actions).boxed()))
    }
}

/// Run the Flight server on `listener` until `shutdown` flips true. Spawned by
/// `server::spawn` next to the HTTP accept loop; both share the same signal.
pub(crate) fn spawn_flight_server(
    listener: TcpListener,
    state: Arc<NodeState>,
    mut shutdown: watch::Receiver<bool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let incoming = tokio_stream::wrappers::TcpListenerStream::new(listener);
        let svc = FlightServiceServer::new(QeFlightService::new(state));
        let signal = async move {
            // Result deliberately ignored: a dropped sender means shutdown.
            while shutdown.changed().await.is_ok() {
                if *shutdown.borrow() {
                    break;
                }
            }
        };
        if let Err(e) = tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming_shutdown(incoming, signal)
            .await
        {
            tracing::error!("flight server error: {e}");
        }
    })
}
