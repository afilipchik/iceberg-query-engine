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
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaAsIpc, SchemaResult, Ticket,
};
use futures::StreamExt;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tonic::{Request, Response, Status, Streaming};

use crate::error::QueryError;

use super::server::NodeState;

/// SQL statements and tickets share the HTTP body cap: a ticket is a statement
/// plus a few fixed fields, and an unbounded ticket is an unbounded allocation.
pub(crate) const MAX_TICKET_BYTES: usize = 1024 * 1024;

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
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("GetFlightInfo"))
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
        Err(Status::invalid_argument(
            "GetSchema needs a path descriptor naming a table",
        ))
    }

    async fn do_get(
        &self,
        _request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("DoGet"))
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
