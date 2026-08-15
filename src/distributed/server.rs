//! `query_engine serve` — the HTTP substrate for running the engine as N
//! independent instances.
//!
//! # Scope
//!
//! This is **M1 only**: process, transport, membership, health. `POST /sql`
//! executes **locally**, against this node's own tables. There is no fan-out,
//! no shuffle, and no distributed answer yet — M2 adds shard-per-node
//! scatter-gather on top of this substrate. A node in a 3-node M1 cluster
//! answers exactly what the single-process binary answers, which is precisely
//! what the M1 acceptance gate checks.
//!
//! # Why hyper and not tonic/arrow-flight
//!
//! `hyper 1.11`, `hyper-util` and `http-body-util` are already in `Cargo.lock`
//! (they arrive under `--features lance`), so naming them adds zero new lock
//! entries. `tonic` and `arrow-flight` are **not** in the lock, and pulling
//! them risks forcing an arrow-major bump against the arrow-53 pin that Lance
//! requires — a single-node regression traded for a transport convenience. We
//! implement Flight's *semantics* (Arrow IPC bodies) without the Flight crate.
//!
//! # Threading
//!
//! Queries do not run on the HTTP runtime. They are dispatched to a separate,
//! lazily-created runtime (see [`query_runtime`]). The reason is specific and
//! operational: `run_subquery_blocking` parks its calling thread on a
//! `JoinHandle`, and the Kubernetes liveness probe hits `/healthz`. If a heavy
//! query can stall the thread that would have answered `/healthz`, the kubelet
//! eventually SIGKILLs a pod that is working perfectly. Separating the runtimes
//! makes liveness independent of query load by construction rather than by
//! thread-count luck.
//!
//! The query runtime is created on the first `/sql` request, so a process that
//! never serves — every benchmark and test run — never creates it.

use crate::distributed::http_client;
use crate::distributed::membership::{Discovery, Member, Membership, MembershipChange, NodeId};
use crate::error::{QueryError, Result};
use crate::execution::{ExecutionContext, QueryResult};
use http_body_util::{BodyExt, Full, Limited};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use parking_lot::RwLock;
use std::net::{IpAddr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Notify};

/// Largest `POST /sql` body accepted. A SQL statement is text; anything past a
/// megabyte is a mistake or an attack, and an unbounded body is a trivial OOM.
const MAX_SQL_BODY_BYTES: usize = 1024 * 1024;

/// Builds this node's tables. Run once, off the HTTP path, so the server binds
/// and answers `/healthz` immediately while `/readyz` stays false.
///
/// A closure rather than a path so that callers — including tests that need to
/// observe the not-ready window deterministically — decide what "load" means.
pub type TableLoader = Box<dyn FnOnce() -> Result<ExecutionContext> + Send + 'static>;

/// Everything `serve` needs. Defaults are the local-testbed values; the
/// Kubernetes manifests set the rest explicitly.
#[derive(Clone, Debug)]
pub struct ServeOptions {
    /// Address to bind. `host:0` picks an ephemeral port; the bound address is
    /// reported by [`ServerHandle::local_addr`].
    pub bind: String,
    /// Address other nodes should use to reach this one. Defaults to
    /// `QE_ADVERTISE_ADDR`, then `POD_IP` + bound port, then the bound address
    /// with an unspecified IP replaced by a real local one.
    pub advertise: Option<String>,
    /// Stable node identity. Defaults to `QE_NODE_ID`, then the StatefulSet pod
    /// ordinal parsed from the hostname, then a hash of the advertised address.
    pub node_id: Option<NodeId>,
    /// Static peer list. May include this node; it is filtered out.
    pub peers: Vec<String>,
    /// DNS name whose A records are the cluster (Kubernetes headless Service).
    pub peers_dns: Option<String>,
    /// Port to assume for `peers_dns` A records. Defaults to the bound port.
    pub peers_dns_port: Option<u16>,
    /// How often to re-resolve discovery and re-probe peers.
    pub discovery_interval: Duration,
    /// Per-probe timeout.
    pub probe_timeout: Duration,
    /// After the shutdown signal, keep serving (but report `/readyz` false) for
    /// this long before closing the listener. Gives a Kubernetes Service time
    /// to drop this pod from its endpoints, so in-flight clients are not
    /// connection-reset. Zero locally, seconds in the manifests.
    pub drain: Duration,
    /// Grace period for in-flight requests once the listener is closed.
    pub shutdown_grace: Duration,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:7777".into(),
            advertise: None,
            node_id: None,
            peers: Vec::new(),
            peers_dns: None,
            peers_dns_port: None,
            discovery_interval: Duration::from_millis(2000),
            probe_timeout: Duration::from_millis(1000),
            drain: Duration::ZERO,
            shutdown_grace: Duration::from_secs(10),
        }
    }
}

/// Runtime for query execution, kept off the HTTP runtime. See the module docs.
fn query_runtime() -> &'static tokio::runtime::Runtime {
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            // Operators fan out with `tokio::spawn` per partition; a narrow
            // pool here would serialize a plan that the single-process binary
            // runs in parallel, and the M1 gate compares against that binary.
            .worker_threads(num_cpus::get().max(2))
            .thread_name("qe-query")
            .enable_all()
            .build()
            .expect("failed to build query runtime")
    })
}

/// Shared server state. Cheap to clone (it is always behind an `Arc`).
pub struct NodeState {
    pub node_id: NodeId,
    pub address: String,
    pub membership: Arc<Membership>,
    /// `None` until the loader finishes. Readiness is defined by this.
    context: RwLock<Option<Arc<ExecutionContext>>>,
    /// Set if the loader failed. A node that cannot load its tables stays
    /// not-ready forever and says why, rather than serving empty answers.
    load_error: RwLock<Option<String>>,
    shutting_down: AtomicBool,
    queries_total: AtomicU64,
    queries_failed: AtomicU64,
    started: Instant,
    /// Kicks the discovery loop out of its sleep after a reconfiguration.
    discovery_kick: Notify,
}

impl NodeState {
    fn new(node_id: NodeId, address: String, membership: Arc<Membership>) -> Self {
        Self {
            node_id,
            address,
            membership,
            context: RwLock::new(None),
            load_error: RwLock::new(None),
            shutting_down: AtomicBool::new(false),
            queries_total: AtomicU64::new(0),
            queries_failed: AtomicU64::new(0),
            started: Instant::now(),
            discovery_kick: Notify::new(),
        }
    }

    pub fn context(&self) -> Option<Arc<ExecutionContext>> {
        self.context.read().clone()
    }

    pub fn tables_loaded(&self) -> bool {
        self.context.read().is_some()
    }

    pub fn load_error(&self) -> Option<String> {
        self.load_error.read().clone()
    }

    /// Ready = tables registered **and** discovery has resolved **and** we are
    /// not shutting down.
    ///
    /// The distinction from liveness is the whole point of having two probes.
    /// A node that is alive but has not found its peers must not receive client
    /// traffic — in M2 it would answer from an incomplete cluster. A node that
    /// is draining must be pulled from the Service before its listener closes.
    /// Neither condition means "restart me", which is what a failed liveness
    /// probe means.
    pub fn ready(&self) -> bool {
        !self.shutting_down.load(Ordering::Relaxed)
            && self.tables_loaded()
            && self.membership.resolved()
    }

    pub fn uptime(&self) -> Duration {
        self.started.elapsed()
    }
}

/// A running server. Dropping this does **not** stop the server; call
/// [`ServerHandle::shutdown`].
pub struct ServerHandle {
    local_addr: SocketAddr,
    state: Arc<NodeState>,
    shutdown_tx: watch::Sender<bool>,
    join: tokio::task::JoinHandle<()>,
}

impl ServerHandle {
    /// The address actually bound. Meaningful when `bind` used port 0.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    pub fn state(&self) -> &Arc<NodeState> {
        &self.state
    }

    pub fn node_id(&self) -> NodeId {
        self.state.node_id
    }

    /// The advertised `host:port` that peers should use.
    pub fn address(&self) -> &str {
        &self.state.address
    }

    /// Replace the peer list at runtime and wake the discovery loop.
    ///
    /// This is the same code path DNS churn takes; it exists as a public method
    /// because three nodes on ephemeral ports cannot know each other's
    /// addresses until all three have bound.
    pub fn set_peers(&self, peers: Vec<String>) {
        self.state
            .membership
            .set_discovery(Discovery::Static(peers));
        self.state.discovery_kick.notify_waiters();
    }

    /// Signal shutdown and wait for the server to finish draining.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.join.await;
    }
}

/// Bind, start the background tasks, and return once the listener is accepting.
///
/// Table loading happens **after** this returns, which is what makes the
/// not-ready window observable.
pub async fn spawn(opts: ServeOptions, loader: TableLoader) -> Result<ServerHandle> {
    let listener = TcpListener::bind(&opts.bind)
        .await
        .map_err(|e| QueryError::Execution(format!("cannot bind {}: {e}", opts.bind)))?;
    let local_addr = listener
        .local_addr()
        .map_err(|e| QueryError::Execution(format!("cannot read bound address: {e}")))?;

    let address = derive_advertise(local_addr, opts.advertise.as_deref());
    let node_id = derive_node_id(opts.node_id, &address);

    let discovery = match &opts.peers_dns {
        Some(name) => Discovery::Dns {
            name: name.clone(),
            port: opts.peers_dns_port.unwrap_or(local_addr.port()),
        },
        None => Discovery::Static(opts.peers.clone()),
    };

    tracing::info!(
        node_id,
        address = %address,
        bind = %local_addr,
        discovery = discovery.mode(),
        source = %discovery.source(),
        "serve: starting"
    );

    let membership = Arc::new(Membership::new(node_id, address.clone(), discovery));
    let state = Arc::new(NodeState::new(node_id, address, membership));
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Table loading: on a blocking thread, because registering Parquet tables
    // reads footers. `/healthz` answers throughout; `/readyz` flips when done.
    {
        let state = state.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            match tokio::task::spawn_blocking(loader).await {
                Ok(Ok(ctx)) => {
                    let names = ctx.table_names();
                    *state.context.write() = Some(Arc::new(ctx));
                    tracing::info!(
                        node_id = state.node_id,
                        tables = names.len(),
                        elapsed_ms = started.elapsed().as_millis() as u64,
                        "serve: tables registered"
                    );
                }
                Ok(Err(e)) => {
                    let msg = e.to_string();
                    tracing::error!(node_id = state.node_id, error = %msg, "serve: table load failed");
                    *state.load_error.write() = Some(msg);
                }
                Err(e) => {
                    let msg = format!("table loader panicked: {e}");
                    tracing::error!(node_id = state.node_id, error = %msg, "serve: table load failed");
                    *state.load_error.write() = Some(msg);
                }
            }
        });
    }

    // Discovery + health probing.
    {
        let state = state.clone();
        let rx = shutdown_rx.clone();
        let interval = opts.discovery_interval;
        let probe_timeout = opts.probe_timeout;
        tokio::spawn(async move { discovery_loop(state, rx, interval, probe_timeout).await });
    }

    let join = {
        let state = state.clone();
        let rx = shutdown_rx.clone();
        let drain = opts.drain;
        let grace = opts.shutdown_grace;
        tokio::spawn(async move { accept_loop(listener, state, rx, drain, grace).await })
    };

    Ok(ServerHandle {
        local_addr,
        state,
        shutdown_tx,
        join,
    })
}

/// Run until SIGTERM/SIGINT, then shut down gracefully.
///
/// SIGTERM handling is not optional politeness: Kubernetes sends SIGTERM, waits
/// `terminationGracePeriodSeconds`, then SIGKILLs. A process that ignores
/// SIGTERM is always killed hard, and every rolling restart looks like a
/// crash — including to anyone debugging a genuine crash.
pub async fn serve(opts: ServeOptions, loader: TableLoader) -> Result<()> {
    let handle = spawn(opts, loader).await?;
    println!(
        "query_engine serve: node {} listening on {} (advertising {})",
        handle.node_id(),
        handle.local_addr(),
        handle.address()
    );

    wait_for_signal().await;
    tracing::info!("serve: signal received, shutting down");
    handle.shutdown().await;
    tracing::info!("serve: stopped");
    Ok(())
}

#[cfg(unix)]
async fn wait_for_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(e) => {
            tracing::error!("cannot install SIGTERM handler: {e}");
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    let mut int = match signal(SignalKind::interrupt()) {
        Ok(s) => s,
        Err(_) => {
            term.recv().await;
            return;
        }
    };
    tokio::select! {
        _ = term.recv() => tracing::info!("SIGTERM"),
        _ = int.recv() => tracing::info!("SIGINT"),
    }
}

#[cfg(not(unix))]
async fn wait_for_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

/// Hand one accepted socket to hyper.
///
/// `stop` is separate from the shutdown signal on purpose: connections accepted
/// *during* the drain window must also be told to finish, and a receiver cloned
/// after the shutdown value was observed would never fire.
fn spawn_connection(
    stream: tokio::net::TcpStream,
    peer: SocketAddr,
    state: Arc<NodeState>,
    inflight: mpsc::Sender<()>,
    mut stop: watch::Receiver<bool>,
) {
    stream.set_nodelay(true).ok();
    let io = TokioIo::new(stream);
    tokio::spawn(async move {
        let _guard = inflight;
        let service = service_fn(move |req| {
            let state = state.clone();
            async move { Ok::<_, std::convert::Infallible>(route(req, state).await) }
        });
        let conn = http1::Builder::new()
            .keep_alive(true)
            .serve_connection(io, service);
        tokio::pin!(conn);
        tokio::select! {
            res = conn.as_mut() => {
                if let Err(e) = res {
                    tracing::debug!(%peer, "connection error: {e}");
                }
            }
            _ = stop.changed() => {
                // Finish the in-flight request, then refuse to start another
                // on this connection.
                conn.as_mut().graceful_shutdown();
                let _ = conn.await;
            }
        }
    });
}

async fn accept_loop(
    listener: TcpListener,
    state: Arc<NodeState>,
    mut shutdown: watch::Receiver<bool>,
    drain: Duration,
    grace: Duration,
) {
    // Every connection task holds a clone of this sender. When the last one is
    // dropped, `recv()` on the receiver returns `None` — an in-flight counter
    // that needs no extra dependency and cannot be miscounted.
    let (inflight_tx, mut inflight_rx) = mpsc::channel::<()>(1);
    // Told to wind down after the drain window, not when the signal arrives.
    let (stop_tx, stop_rx) = watch::channel(false);

    // Phase 1: serve until the shutdown signal.
    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => {
                if *shutdown.borrow() { break; }
            }
            accepted = listener.accept() => match accepted {
                Ok((stream, peer)) => spawn_connection(
                    stream, peer, state.clone(), inflight_tx.clone(), stop_rx.clone(),
                ),
                Err(e) => {
                    // EMFILE and friends: transient. Exiting the accept loop
                    // would silently turn a resource blip into a dead node
                    // that still passes its liveness probe.
                    tracing::warn!("accept failed: {e}");
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            },
        }
    }

    // Phase 2: report not-ready, but KEEP SERVING for the drain window.
    //
    // The order matters and is the opposite of the obvious one. Closing the
    // listener first gives connection-refused to every client that dials while
    // the Kubernetes Service still lists this pod — endpoint removal is
    // asynchronous and takes a moment to propagate to every kube-proxy. Failing
    // /readyz first and continuing to serve lets the endpoint drain quietly.
    state.shutting_down.store(true, Ordering::Relaxed);
    if !drain.is_zero() {
        tracing::info!(drain_ms = drain.as_millis() as u64, "serve: draining");
        let deadline = tokio::time::sleep(drain);
        tokio::pin!(deadline);
        loop {
            tokio::select! {
                biased;
                _ = &mut deadline => break,
                accepted = listener.accept() => match accepted {
                    Ok((stream, peer)) => spawn_connection(
                        stream, peer, state.clone(), inflight_tx.clone(), stop_rx.clone(),
                    ),
                    Err(e) => {
                        tracing::warn!("accept failed while draining: {e}");
                        break;
                    }
                },
            }
        }
    }
    drop(listener);

    // Phase 3: tell live connections to wind down, then wait for them.
    let _ = stop_tx.send(true);
    drop(inflight_tx);
    drop(stop_rx);
    if tokio::time::timeout(grace, inflight_rx.recv())
        .await
        .is_err()
    {
        tracing::warn!(
            grace_ms = grace.as_millis() as u64,
            "serve: shutdown grace expired with requests still in flight"
        );
    }
}

async fn discovery_loop(
    state: Arc<NodeState>,
    mut shutdown: watch::Receiver<bool>,
    interval: Duration,
    probe_timeout: Duration,
) {
    loop {
        resolve_once(&state).await;
        probe_once(&state, probe_timeout).await;

        tokio::select! {
            _ = shutdown.changed() => { if *shutdown.borrow() { return; } }
            _ = tokio::time::sleep(interval) => {}
            _ = state.discovery_kick.notified() => {}
        }
    }
}

async fn resolve_once(state: &Arc<NodeState>) {
    // BOTH the resolution and the self-filtering run on a blocking thread.
    // `Discovery::resolve` obviously calls getaddrinfo, but so does
    // `set_members` -> `is_self_address` whenever a peer is spelled as a
    // hostname rather than an IP (`qe-0.qe-headless:7777`). On a Kubernetes node
    // with a slow or failing CoreDNS either can take seconds, and neither may
    // park a runtime worker.
    let membership = state.membership.clone();
    let resolved = tokio::task::spawn_blocking(move || {
        let discovery = membership.discovery();
        discovery
            .resolve()
            .map(|addrs| membership.set_members(addrs))
    })
    .await;

    match resolved {
        Ok(Ok(changes)) => {
            for change in changes {
                match change {
                    MembershipChange::Added(a) => {
                        tracing::info!(node_id = state.node_id, peer = %a, "membership: peer added")
                    }
                    MembershipChange::Removed(a) => {
                        tracing::info!(node_id = state.node_id, peer = %a, "membership: peer removed")
                    }
                }
            }
        }
        Ok(Err(e)) => {
            // A headless Service with no ready endpoints NXDOMAINs. That is a
            // normal startup state, not a failure; keep the last known set.
            tracing::debug!(node_id = state.node_id, "discovery resolve failed: {e}");
            state.membership.record_resolve_error(e.to_string());
        }
        Err(e) => {
            tracing::warn!(node_id = state.node_id, "discovery task failed: {e}");
            state.membership.record_resolve_error(e.to_string());
        }
    }
}

async fn probe_once(state: &Arc<NodeState>, timeout: Duration) {
    let peers = state.membership.peer_addresses();
    if peers.is_empty() {
        return;
    }
    let probes = peers.into_iter().map(|addr| {
        let state = state.clone();
        async move {
            match http_client::get(&addr, "/healthz", timeout).await {
                Ok(resp) if resp.is_success() => {
                    let node_id = serde_json::from_slice::<serde_json::Value>(&resp.body)
                        .ok()
                        .and_then(|v| v.get("node_id").and_then(|n| n.as_u64()));
                    state.membership.record_up(&addr, node_id);
                }
                Ok(resp) => state
                    .membership
                    .record_down(&addr, format!("HTTP {}", resp.status)),
                Err(e) => state.membership.record_down(&addr, e.to_string()),
            }
        }
    });
    futures::future::join_all(probes).await;
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

async fn route(req: Request<Incoming>, state: Arc<NodeState>) -> Response<Full<Bytes>> {
    let path = req.uri().path().to_string();
    let method = req.method().clone();
    let query = req.uri().query().unwrap_or("").to_string();

    let mut resp = match (&method, path.as_str()) {
        (&Method::GET, "/healthz") => healthz(&state),
        (&Method::GET, "/readyz") => readyz(&state),
        (&Method::GET, "/cluster") => cluster(&state),
        (&Method::GET, "/splits") => splits(&state, &query),
        (&Method::POST, "/sql") => sql(req, &state, &query).await,
        (&Method::POST, "/fragment") => fragment(req, &state).await,
        (&Method::GET, "/") => index(&state),
        (_, "/healthz") | (_, "/readyz") | (_, "/cluster") | (_, "/splits") | (_, "/") => {
            error_response(StatusCode::METHOD_NOT_ALLOWED, "use GET")
        }
        (_, "/sql") => error_response(StatusCode::METHOD_NOT_ALLOWED, "use POST /sql"),
        (_, "/fragment") => error_response(StatusCode::METHOD_NOT_ALLOWED, "use POST /fragment"),
        _ => error_response(
            StatusCode::NOT_FOUND,
            "no such endpoint; try /healthz /readyz /cluster or POST /sql",
        ),
    };

    // Every response carries the answering node, so a client fanning out (or a
    // human behind a Service) can always tell who replied.
    resp.headers_mut().insert(
        "x-qe-node-id",
        state.node_id.to_string().parse().expect("numeric header"),
    );
    resp
}

fn index(state: &Arc<NodeState>) -> Response<Full<Bytes>> {
    let body = format!(
        "query_engine node {} at {}\n\n\
         GET  /healthz    liveness\n\
         GET  /readyz     readiness (tables loaded AND peers resolved)\n\
         GET  /cluster    membership view (JSON)\n\
         GET  /splits     how a table divides across N nodes (JSON)\n\
                          ?table=<name>&nodes=<N>\n\
         POST /sql        execute SQL; body is the statement.\n\
                          ?format=arrow (default) | json | csv\n\
                          ?distributed=auto (default) | 1 | 0\n\
         POST /fragment   execute one shard of a distributed query (internal)\n\n\
         Every /sql response carries x-qe-distributed: true|false, and when it\n\
         is true, x-qe-imbalance and x-qe-distribution describing exactly how\n\
         the work was divided. `distributed=1` NEVER falls back: an unsupported\n\
         query shape is a 400 with the reason, not a quietly local answer.\n",
        state.node_id, state.address
    );
    text_response(StatusCode::OK, body)
}

// ---------------------------------------------------------------------------
// Distributed execution
// ---------------------------------------------------------------------------

/// How a `/sql` request should be executed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DistMode {
    /// Distribute when the cluster has peers and the shape is supported;
    /// otherwise run locally over this node's full copy of the data. The
    /// response always says which happened and, when it fell back, why.
    Auto,
    /// Distribute or fail. An unsupported shape is an error, never a local
    /// answer — this is the mode a client uses when it needs to KNOW the
    /// cluster did the work.
    Force,
    /// Never distribute. M1 behaviour, kept because a node holding the whole
    /// dataset can always answer alone and that is a useful reference.
    Off,
}

impl DistMode {
    fn parse(query: &str) -> std::result::Result<Self, String> {
        for pair in query.split('&') {
            let Some((k, v)) = pair.split_once('=') else {
                continue;
            };
            if k != "distributed" {
                continue;
            }
            return match v {
                "1" | "true" | "yes" | "force" => Ok(DistMode::Force),
                "0" | "false" | "no" | "local" => Ok(DistMode::Off),
                "auto" => Ok(DistMode::Auto),
                other => Err(format!(
                    "unknown distributed mode {other:?}; expected auto, 1 or 0"
                )),
            };
        }
        Ok(DistMode::Auto)
    }
}

/// Peers this node may send fragments to, in a deterministic order.
///
/// Sorted by address (which is how `Membership::members` renders them), so
/// shard *i* means the same node no matter which member is the initiator. Only
/// members we have actually reached are included: sending a fragment to a peer
/// last seen as `Down` would fail the whole query, and a peer never probed
/// (`Unknown`) may not even exist.
fn participants(state: &Arc<NodeState>) -> Vec<crate::distributed::Participant> {
    state
        .membership
        .members()
        .into_iter()
        .filter(|m| m.is_self || m.status == crate::distributed::PeerStatus::Up)
        .map(|m| crate::distributed::Participant {
            node_id: m.node_id.unwrap_or(u64::MAX),
            address: m.address,
            is_self: m.is_self,
        })
        .collect()
}

/// Sends fragments to peers over HTTP. The initiator's own shard does not go
/// through here — it is executed in-process by the coordinator, on the same
/// code path, so there is only one fragment executor in the system.
struct HttpTransport {
    timeout: Duration,
}

#[async_trait::async_trait]
impl crate::distributed::FragmentTransport for HttpTransport {
    async fn send(
        &self,
        address: &str,
        req: &crate::distributed::FragmentRequest,
    ) -> Result<(Vec<u8>, usize, f64)> {
        let body = serde_json::to_vec(req)
            .map_err(|e| QueryError::Execution(format!("cannot encode fragment request: {e}")))?;
        let resp = http_client::post_json(address, "/fragment", &body, self.timeout)
            .await
            .map_err(|e| QueryError::Execution(e.to_string()))?;
        if !resp.is_success() {
            // Carry the peer's own message through verbatim: a digest mismatch
            // or an unsupported shape explains itself far better than
            // "HTTP 400" ever could.
            let detail = serde_json::from_slice::<serde_json::Value>(&resp.body)
                .ok()
                .and_then(|v| v.get("error").and_then(|e| e.as_str()).map(String::from))
                .unwrap_or_else(|| resp.text());
            return Err(QueryError::Execution(format!(
                "HTTP {} — {detail}",
                resp.status
            )));
        }
        let rows = resp
            .header("x-qe-rows")
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        let ms = resp
            .header("x-qe-elapsed-ms")
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(0.0);
        Ok((resp.body, rows, ms))
    }
}

/// `POST /fragment` — run one shard. Internal to the cluster.
async fn fragment(req: Request<Incoming>, state: &Arc<NodeState>) -> Response<Full<Bytes>> {
    let Some(ctx) = state.context() else {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &state
                .load_error()
                .map(|e| format!("tables failed to load: {e}"))
                .unwrap_or_else(|| "tables are still loading".to_string()),
        );
    };

    let body = match Limited::new(req.into_body(), MAX_SQL_BODY_BYTES)
        .collect()
        .await
    {
        Ok(b) => b.to_bytes(),
        Err(_) => {
            return error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                &format!("fragment body exceeds {MAX_SQL_BODY_BYTES} bytes"),
            )
        }
    };
    let request: crate::distributed::FragmentRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                &format!("malformed fragment request: {e}"),
            )
        }
    };

    state.queries_total.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();
    let joined = query_runtime()
        .spawn(async move {
            let (result, stats) = crate::distributed::execute_fragment(&ctx, &request).await?;
            let bytes =
                crate::distributed::coordinator::encode_ipc(&result.schema, &result.batches)?;
            Ok::<_, QueryError>((bytes, result.row_count, stats))
        })
        .await;
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;

    match joined {
        Ok(Ok((bytes, rows, stats))) => {
            let mut resp =
                raw_response(StatusCode::OK, "application/vnd.apache.arrow.stream", bytes);
            let h = resp.headers_mut();
            h.insert("x-qe-rows", rows.to_string().parse().expect("numeric"));
            h.insert(
                "x-qe-elapsed-ms",
                format!("{elapsed_ms:.3}").parse().expect("numeric"),
            );
            h.insert(
                "x-qe-shard-bytes",
                stats.bytes.to_string().parse().expect("numeric"),
            );
            h.insert(
                "x-qe-shard-rows",
                stats.rows.to_string().parse().expect("numeric"),
            );
            h.insert(
                "x-qe-shard-splits",
                stats.splits.to_string().parse().expect("numeric"),
            );
            resp
        }
        Ok(Err(e)) => {
            state.queries_failed.fetch_add(1, Ordering::Relaxed);
            error_response(StatusCode::BAD_REQUEST, &e.to_string())
        }
        Err(e) => {
            state.queries_failed.fetch_add(1, Ordering::Relaxed);
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("fragment task failed: {e}"),
            )
        }
    }
}

/// `GET /splits?table=lineitem&nodes=8` — how the work would be divided.
///
/// Exists so balance is auditable without running an N-node cluster: it is pure
/// metadata arithmetic. `nodes` defaults to the current member count.
fn splits(state: &Arc<NodeState>, query: &str) -> Response<Full<Bytes>> {
    let Some(ctx) = state.context() else {
        return error_response(StatusCode::SERVICE_UNAVAILABLE, "tables are still loading");
    };
    let mut table: Option<String> = None;
    let mut nodes: Option<usize> = None;
    for pair in query.split('&') {
        let Some((k, v)) = pair.split_once('=') else {
            continue;
        };
        match k {
            "table" => table = Some(v.to_string()),
            "nodes" => match v.parse::<usize>() {
                Ok(n) if n >= 1 => nodes = Some(n),
                _ => {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        &format!("nodes must be a positive integer, got {v:?}"),
                    )
                }
            },
            _ => {}
        }
    }
    let Some(table) = table else {
        return error_response(
            StatusCode::BAD_REQUEST,
            "missing ?table=<name>; try /cluster to see what is loaded",
        );
    };
    let nodes = nodes.unwrap_or_else(|| state.membership.members().len().max(1));

    let set = match crate::distributed::splits_of(&ctx, &table, nodes) {
        Ok(s) => s,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e.to_string()),
    };
    let assignment = crate::distributed::assign_lpt(&set, nodes);
    let body = serde_json::json!({
        "table": table,
        "nodes": nodes,
        "total_splits": set.len(),
        "total_rows": set.total_rows,
        "total_bytes": set.total_bytes,
        "target_split_bytes": set.target_split_bytes,
        "splits_digest": format!("{:#018x}", set.digest()),
        "imbalance": assignment.imbalance(),
        "idle_nodes": assignment.idle_nodes(),
        "per_node": (0..nodes).map(|i| serde_json::json!({
            "shard_index": i,
            "splits": assignment.node_splits[i],
            "bytes": assignment.node_bytes[i],
            "rows": assignment.node_rows[i],
        })).collect::<Vec<_>>(),
    });
    json_response(StatusCode::OK, &body)
}

/// Liveness. Must not touch tables, peers, locks held by queries, or disk:
/// anything it depends on becomes a reason for Kubernetes to kill a healthy
/// pod. It reports identity too, which is how peers learn each other's node ids.
fn healthz(state: &Arc<NodeState>) -> Response<Full<Bytes>> {
    let body = serde_json::json!({
        "status": "ok",
        "node_id": state.node_id,
        "address": state.address,
        "uptime_ms": state.uptime().as_millis() as u64,
    });
    json_response(StatusCode::OK, &body)
}

/// Readiness. 200 only when this node can correctly answer a query.
fn readyz(state: &Arc<NodeState>) -> Response<Full<Bytes>> {
    let tables_loaded = state.tables_loaded();
    let peers_resolved = state.membership.resolved();
    let shutting_down = state.shutting_down.load(Ordering::Relaxed);
    let ready = state.ready();

    let reason = if shutting_down {
        Some("shutting down")
    } else if let Some(_e) = state.load_error() {
        Some("table load failed")
    } else if !tables_loaded {
        Some("tables not loaded yet")
    } else if !peers_resolved {
        Some("peer discovery has not resolved")
    } else {
        None
    };

    let body = serde_json::json!({
        "ready": ready,
        "node_id": state.node_id,
        "tables_loaded": tables_loaded,
        "peers_resolved": peers_resolved,
        "shutting_down": shutting_down,
        "reason": reason,
        "load_error": state.load_error(),
    });
    json_response(
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        &body,
    )
}

#[derive(serde::Serialize)]
struct ClusterView {
    node: NodeView,
    discovery: DiscoveryView,
    member_count: usize,
    members: Vec<Member>,
}

#[derive(serde::Serialize)]
struct NodeView {
    id: NodeId,
    address: String,
    ready: bool,
    tables: Vec<String>,
    uptime_ms: u64,
    queries_total: u64,
    queries_failed: u64,
}

#[derive(serde::Serialize)]
struct DiscoveryView {
    mode: &'static str,
    source: String,
    resolved: bool,
    generation: u64,
    last_resolved_unix_ms: Option<u64>,
    last_error: Option<String>,
}

fn cluster(state: &Arc<NodeState>) -> Response<Full<Bytes>> {
    let discovery = state.membership.discovery();
    let members = state.membership.members();
    let view = ClusterView {
        node: NodeView {
            id: state.node_id,
            address: state.address.clone(),
            ready: state.ready(),
            tables: state
                .context()
                .map(|c| {
                    let mut t = c.table_names();
                    t.sort();
                    t
                })
                .unwrap_or_default(),
            uptime_ms: state.uptime().as_millis() as u64,
            queries_total: state.queries_total.load(Ordering::Relaxed),
            queries_failed: state.queries_failed.load(Ordering::Relaxed),
        },
        discovery: DiscoveryView {
            mode: discovery.mode(),
            source: discovery.source(),
            resolved: state.membership.resolved(),
            generation: state.membership.generation(),
            last_resolved_unix_ms: state.membership.last_resolved_unix_ms(),
            last_error: state.membership.last_resolve_error(),
        },
        member_count: members.len(),
        members,
    };
    match serde_json::to_vec_pretty(&view) {
        Ok(bytes) => raw_response(StatusCode::OK, "application/json", bytes),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &format!("cannot serialize cluster view: {e}"),
        ),
    }
}

/// Result encoding for `POST /sql`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ResultFormat {
    /// Arrow IPC stream — the wire format, and the default.
    Arrow,
    Json,
    Csv,
}

impl ResultFormat {
    fn parse(query: &str) -> std::result::Result<Self, String> {
        for pair in query.split('&') {
            let (k, v) = match pair.split_once('=') {
                Some(kv) => kv,
                None => continue,
            };
            if k != "format" {
                continue;
            }
            return match v {
                "arrow" | "ipc" => Ok(ResultFormat::Arrow),
                "json" => Ok(ResultFormat::Json),
                "csv" => Ok(ResultFormat::Csv),
                other => Err(format!(
                    "unknown format {other:?}; expected arrow, json or csv"
                )),
            };
        }
        Ok(ResultFormat::Arrow)
    }

    fn content_type(&self) -> &'static str {
        match self {
            ResultFormat::Arrow => "application/vnd.apache.arrow.stream",
            ResultFormat::Json => "application/json",
            ResultFormat::Csv => "text/csv; charset=utf-8",
        }
    }
}

async fn sql(req: Request<Incoming>, state: &Arc<NodeState>, query: &str) -> Response<Full<Bytes>> {
    let format = match ResultFormat::parse(query) {
        Ok(f) => f,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };
    let mode = match DistMode::parse(query) {
        Ok(m) => m,
        Err(e) => return error_response(StatusCode::BAD_REQUEST, &e),
    };

    let Some(ctx) = state.context() else {
        let reason = state
            .load_error()
            .map(|e| format!("tables failed to load: {e}"))
            .unwrap_or_else(|| "tables are still loading".to_string());
        return error_response(StatusCode::SERVICE_UNAVAILABLE, &reason);
    };

    let body = match Limited::new(req.into_body(), MAX_SQL_BODY_BYTES)
        .collect()
        .await
    {
        Ok(b) => b.to_bytes(),
        Err(_) => {
            return error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                &format!("SQL body exceeds {MAX_SQL_BODY_BYTES} bytes"),
            )
        }
    };
    let statement = match String::from_utf8(body.to_vec()) {
        Ok(s) => s.trim().to_string(),
        Err(_) => return error_response(StatusCode::BAD_REQUEST, "SQL body is not valid UTF-8"),
    };
    if statement.is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "empty SQL body");
    }

    state.queries_total.fetch_add(1, Ordering::Relaxed);
    let started = Instant::now();

    // Decide local vs distributed BEFORE any fan-out, and only ever fall back
    // for a capability reason. A fallback triggered by an execution failure
    // would hide a broken cluster behind a correct-looking answer.
    let members = participants(state);
    let (distribute, fallback_reason) = match mode {
        DistMode::Off => (false, Some("distributed=0 requested".to_string())),
        DistMode::Force => (true, None),
        DistMode::Auto => {
            if members.len() < 2 {
                (false, Some("only one cluster member is up".to_string()))
            } else {
                match crate::distributed::plan_distributed(&ctx, &statement) {
                    Ok(_) => (true, None),
                    Err(e) => (false, Some(e.to_string())),
                }
            }
        }
    };

    // Off the HTTP runtime — see the module docs.
    let joined = query_runtime()
        .spawn(async move {
            if !distribute {
                let result = ctx.sql(&statement).await?;
                let rows = result.row_count;
                let encoded = encode(&result, format)?;
                return Ok::<_, QueryError>((encoded, rows, None));
            }
            let transport = HttpTransport {
                timeout: crate::distributed::coordinator::DEFAULT_FRAGMENT_TIMEOUT,
            };
            let out =
                crate::distributed::execute_distributed(&ctx, &statement, &members, &transport)
                    .await?;
            let rows = out.result.row_count;
            let encoded = encode(&out.result, format)?;
            Ok((encoded, rows, Some(out.distribution)))
        })
        .await;

    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    match joined {
        Ok(Ok((bytes, rows, distribution))) => {
            let mut resp = raw_response(StatusCode::OK, format.content_type(), bytes);
            let h = resp.headers_mut();
            h.insert("x-qe-rows", rows.to_string().parse().expect("numeric"));
            h.insert(
                "x-qe-elapsed-ms",
                format!("{elapsed_ms:.3}").parse().expect("numeric"),
            );
            match &distribution {
                None => {
                    h.insert("x-qe-distributed", "false".parse().expect("ascii"));
                    if let Some(reason) = &fallback_reason {
                        if let Ok(v) = header_value(reason) {
                            h.insert("x-qe-distributed-skipped", v);
                        }
                    }
                }
                Some(d) => {
                    h.insert("x-qe-distributed", "true".parse().expect("ascii"));
                    h.insert(
                        "x-qe-shards",
                        d.nodes.len().to_string().parse().expect("numeric"),
                    );
                    h.insert(
                        "x-qe-imbalance",
                        format!("{:.4}", d.imbalance).parse().expect("numeric"),
                    );
                    h.insert(
                        "x-qe-wall-time-spread",
                        format!("{:.4}", d.wall_time_spread)
                            .parse()
                            .expect("numeric"),
                    );
                    // The full picture, so a production caller can see the
                    // division without a second request. Dropped rather than
                    // truncated if it would not fit a header cleanly — a
                    // half-JSON header is worse than none.
                    if let Ok(json) = serde_json::to_string(d) {
                        if json.len() <= 6144 {
                            if let Ok(v) = header_value(&json) {
                                h.insert("x-qe-distribution", v);
                            }
                        }
                    }
                }
            }
            resp
        }
        Ok(Err(e)) => {
            state.queries_failed.fetch_add(1, Ordering::Relaxed);
            let status = if matches!(e, QueryError::NotImplemented(_)) {
                StatusCode::NOT_IMPLEMENTED
            } else {
                StatusCode::BAD_REQUEST
            };
            let mut resp = error_response(status, &e.to_string());
            resp.headers_mut()
                .insert("x-qe-distributed", "false".parse().expect("ascii"));
            resp
        }
        Err(e) => {
            state.queries_failed.fetch_add(1, Ordering::Relaxed);
            error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &format!("query task failed: {e}"),
            )
        }
    }
}

/// A header value with control characters and non-ASCII stripped, since an
/// error message or a table name may contain either and an invalid header
/// aborts the whole response.
fn header_value(s: &str) -> std::result::Result<hyper::header::HeaderValue, ()> {
    let cleaned: String = s
        .chars()
        .map(|c| {
            if c.is_ascii() && !c.is_ascii_control() {
                c
            } else {
                ' '
            }
        })
        .collect();
    hyper::header::HeaderValue::from_str(&cleaned).map_err(|_| ())
}

/// Encode a result. Arrow IPC is the default because it is lossless and it is
/// what M2's fan-out will exchange; JSON and CSV exist for humans and for the
/// harness that diffs a cluster answer against the single-process binary.
fn encode(result: &QueryResult, format: ResultFormat) -> Result<Vec<u8>> {
    match format {
        ResultFormat::Arrow => {
            // Prefer a real batch's schema. `physical.schema()` can differ from
            // the produced batches in nullability, and `StreamWriter` rejects a
            // batch whose schema is not the one it was opened with — better to
            // be governed by the data than by the plan's description of it.
            let schema = result
                .batches
                .first()
                .map(|b| b.schema())
                .unwrap_or_else(|| result.schema.clone());
            let mut buf = Vec::new();
            {
                let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut buf, &schema)?;
                for b in &result.batches {
                    w.write(b)?;
                }
                w.finish()?;
            }
            Ok(buf)
        }
        ResultFormat::Json => {
            let mut w = arrow::json::ArrayWriter::new(Vec::new());
            for b in &result.batches {
                w.write(b)?;
            }
            w.finish()?;
            Ok(w.into_inner())
        }
        ResultFormat::Csv => {
            let mut buf = Vec::new();
            {
                let mut w = arrow::csv::WriterBuilder::new()
                    .with_header(true)
                    .build(&mut buf);
                for b in &result.batches {
                    w.write(b)?;
                }
            }
            Ok(buf)
        }
    }
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn raw_response(status: StatusCode, content_type: &str, body: Vec<u8>) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .body(Full::new(Bytes::from(body)))
        .expect("static response builder")
}

fn json_response(status: StatusCode, value: &serde_json::Value) -> Response<Full<Bytes>> {
    let body = serde_json::to_vec_pretty(value).unwrap_or_else(|_| b"{}".to_vec());
    raw_response(status, "application/json", body)
}

fn text_response(status: StatusCode, body: String) -> Response<Full<Bytes>> {
    raw_response(status, "text/plain; charset=utf-8", body.into_bytes())
}

fn error_response(status: StatusCode, message: &str) -> Response<Full<Bytes>> {
    json_response(
        status,
        &serde_json::json!({ "error": message, "status": status.as_u16() }),
    )
}

// ---------------------------------------------------------------------------
// Identity derivation
// ---------------------------------------------------------------------------

/// What address should peers use to reach us?
///
/// The bound address is often useless for this: `0.0.0.0:7777` is not routable
/// and `[::]:7777` even less so. Order of preference is explicit config, then
/// the Kubernetes downward-API `POD_IP`, then a real local interface address.
pub fn derive_advertise(local: SocketAddr, explicit: Option<&str>) -> String {
    if let Some(a) = explicit {
        return a.to_string();
    }
    if let Ok(a) = std::env::var("QE_ADVERTISE_ADDR") {
        if !a.trim().is_empty() {
            return a;
        }
    }
    if let Ok(ip) = std::env::var("POD_IP") {
        if !ip.trim().is_empty() {
            return format!("{}:{}", ip.trim(), local.port());
        }
    }
    if !local.ip().is_unspecified() {
        return local.to_string();
    }
    let best = crate::distributed::membership::local_ip_addresses()
        .into_iter()
        .filter(|ip| ip.is_ipv4() && !ip.is_loopback())
        .min_by_key(|ip| ip.to_string());
    match best {
        Some(ip) => format!("{}:{}", ip, local.port()),
        None => format!("{}:{}", IpAddr::from([127, 0, 0, 1]), local.port()),
    }
}

/// Stable node id. Explicit wins; then `QE_NODE_ID`; then the StatefulSet pod
/// ordinal (`qe-2` → 2), which is exactly why StatefulSet is the right workload
/// type here; then a hash of the advertised address so ids are at least stable
/// across restarts of the same node.
pub fn derive_node_id(explicit: Option<NodeId>, address: &str) -> NodeId {
    if let Some(id) = explicit {
        return id;
    }
    if let Ok(v) = std::env::var("QE_NODE_ID") {
        if let Ok(id) = v.trim().parse::<NodeId>() {
            return id;
        }
    }
    if let Some(id) = hostname().and_then(|h| pod_ordinal(&h)) {
        return id;
    }
    stable_hash(address)
}

/// `qe-7` → `Some(7)`. Anything else → `None`.
fn pod_ordinal(hostname: &str) -> Option<NodeId> {
    let (_, tail) = hostname.rsplit_once('-')?;
    if tail.is_empty() || !tail.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    tail.parse::<NodeId>().ok()
}

fn hostname() -> Option<String> {
    if let Ok(h) = std::env::var("HOSTNAME") {
        if !h.trim().is_empty() {
            return Some(h.trim().to_string());
        }
    }
    std::fs::read_to_string("/proc/sys/kernel/hostname")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// FNV-1a. Not cryptographic and does not need to be — it needs to be stable
/// across processes and releases, which `DefaultHasher` explicitly is not.
fn stable_hash(s: &str) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for b in s.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_defaults_to_arrow_and_rejects_nonsense() {
        assert_eq!(ResultFormat::parse("").unwrap(), ResultFormat::Arrow);
        assert_eq!(
            ResultFormat::parse("format=json").unwrap(),
            ResultFormat::Json
        );
        assert_eq!(
            ResultFormat::parse("format=csv").unwrap(),
            ResultFormat::Csv
        );
        assert_eq!(
            ResultFormat::parse("x=1&format=ipc").unwrap(),
            ResultFormat::Arrow
        );
        // An unknown format must be an error, not a silent fall back to Arrow:
        // a client asking for `?format=jsonl` and getting IPC bytes debugs the
        // wrong layer for an hour.
        assert!(ResultFormat::parse("format=jsonl").is_err());
    }

    #[test]
    fn distributed_mode_defaults_to_auto_and_rejects_nonsense() {
        assert_eq!(DistMode::parse("").unwrap(), DistMode::Auto);
        assert_eq!(DistMode::parse("format=csv").unwrap(), DistMode::Auto);
        assert_eq!(DistMode::parse("distributed=1").unwrap(), DistMode::Force);
        assert_eq!(
            DistMode::parse("distributed=true").unwrap(),
            DistMode::Force
        );
        assert_eq!(DistMode::parse("distributed=0").unwrap(), DistMode::Off);
        assert_eq!(
            DistMode::parse("format=csv&distributed=auto").unwrap(),
            DistMode::Auto
        );
        // A typo must not silently mean "auto": a caller writing
        // `?distributed=yes-please` and getting a local answer would believe
        // the cluster had participated.
        assert!(DistMode::parse("distributed=maybe").is_err());
    }

    #[test]
    fn header_values_survive_messages_with_newlines_and_unicode() {
        // Rejection reasons quote the user's SQL, which can contain anything.
        // An invalid header value would abort the whole response, turning a
        // clear 501 into a connection error.
        let v = header_value("bad\nreason: SUM(dépôt)\r\n").unwrap();
        assert!(!v.to_str().unwrap().contains('\n'));
        assert!(v.to_str().unwrap().starts_with("bad reason"));
    }

    #[test]
    fn pod_ordinal_reads_statefulset_hostnames() {
        assert_eq!(pod_ordinal("query-engine-0"), Some(0));
        assert_eq!(pod_ordinal("qe-17"), Some(17));
        assert_eq!(pod_ordinal("laptop"), None);
        assert_eq!(pod_ordinal("deploy-7f9c8-abcde"), None);
        assert_eq!(pod_ordinal("qe-"), None);
    }

    #[test]
    fn advertise_replaces_an_unspecified_bind_address() {
        let unspec: SocketAddr = "0.0.0.0:7777".parse().unwrap();
        let a = derive_advertise(unspec, None);
        assert!(!a.starts_with("0.0.0.0"), "advertised {a} is not routable");
        assert!(a.ends_with(":7777"));

        let concrete: SocketAddr = "127.0.0.1:7788".parse().unwrap();
        assert_eq!(derive_advertise(concrete, None), "127.0.0.1:7788");
        assert_eq!(
            derive_advertise(concrete, Some("host.example:9")),
            "host.example:9"
        );
    }

    #[test]
    fn stable_hash_is_stable_and_distinguishing() {
        assert_eq!(stable_hash("10.0.0.1:7777"), stable_hash("10.0.0.1:7777"));
        assert_ne!(stable_hash("10.0.0.1:7777"), stable_hash("10.0.0.2:7777"));
    }

    #[test]
    fn explicit_node_id_wins_over_everything() {
        assert_eq!(derive_node_id(Some(42), "10.0.0.1:7777"), 42);
    }
}
