//! Cluster membership: who else is out there, and are they answering.
//!
//! # Why this shape
//!
//! The one design decision that makes a laptop a faithful testbed for
//! Kubernetes is that **the same binary discovers peers both ways**:
//!
//! * `--peers a:7777,b:7777` — a static list. What `scripts/cluster_local.sh`
//!   uses to run N processes on one host.
//! * `--peers-dns qe-headless` — resolve A records on a timer. A Kubernetes
//!   headless Service publishes one A record per pod IP, so this is *the*
//!   Kubernetes mechanism, and re-resolving picks up pod churn.
//!
//! Both funnel into the same `set_members` diff, so the membership-change code
//! path that a rolling restart exercises in production is the same one the
//! local tests exercise.
//!
//! # The two things that are easy to get wrong
//!
//! 1. **Self-identification.** Every node resolves the *same* address set,
//!    including its own address. A node that fails to recognise itself counts
//!    itself as a peer, probes itself, and reports N+1 members — and worse, in
//!    M2 it would fan a query out to itself and double-count a shard. In
//!    Kubernetes the local socket address (`0.0.0.0:7777`) is never the address
//!    DNS advertises (`10.244.1.7:7777`), so string comparison alone is not
//!    enough; see [`is_self_address`].
//!
//! 2. **A down peer is a fact, not an error.** `set_members` and the prober
//!    never fail a node because a peer is unreachable. The peer is marked
//!    `Down` with the reason recorded, and the local node keeps serving. This
//!    is the difference between a cluster and a suicide pact.

use parking_lot::Mutex;
use std::collections::{BTreeMap, HashSet};
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::time::{SystemTime, UNIX_EPOCH};

/// Stable identifier for a node. Comes from `--node-id`, `QE_NODE_ID`, a
/// StatefulSet pod ordinal, or a hash of the advertised address — in that order.
pub type NodeId = u64;

/// What we currently believe about a peer's reachability.
///
/// `Unknown` is distinct from `Down` on purpose: "we have not probed you yet"
/// and "we probed you and you did not answer" lead to different operator
/// decisions, and collapsing them makes a starting cluster look like a broken
/// one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PeerStatus {
    Unknown,
    Up,
    Down,
}

/// One member of the cluster as this node sees it.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Member {
    /// `host:port` — the address other nodes use to reach this member.
    pub address: String,
    /// Learned from the member's own `/healthz`; `None` until first contact.
    pub node_id: Option<NodeId>,
    /// The member's Arrow Flight address, learned the same way; absent until
    /// first contact or when the member runs with Flight disabled.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flight: Option<String>,
    /// True for the node serving this view.
    pub is_self: bool,
    pub status: PeerStatus,
    /// Unix milliseconds of the last successful probe.
    pub last_seen_unix_ms: Option<u64>,
    /// Why the last probe failed, if it did.
    pub last_error: Option<String>,
    /// Failed probes since the last success. Zero when `Up`.
    pub consecutive_failures: u32,
}

/// How peers are found.
#[derive(Clone, Debug)]
pub enum Discovery {
    /// A fixed list of `host:port` strings, which MAY include this node.
    Static(Vec<String>),
    /// A DNS name whose A records are the cluster (Kubernetes headless
    /// Service), plus the port every node listens on.
    Dns { name: String, port: u16 },
}

impl Discovery {
    pub fn mode(&self) -> &'static str {
        match self {
            Discovery::Static(_) => "static",
            Discovery::Dns { .. } => "dns",
        }
    }

    pub fn source(&self) -> String {
        match self {
            Discovery::Static(list) => list.join(","),
            Discovery::Dns { name, port } => format!("{name}:{port}"),
        }
    }

    /// Produce the current address set. **Blocking** — `to_socket_addrs` calls
    /// `getaddrinfo`. Callers run this on a blocking thread.
    pub fn resolve(&self) -> std::io::Result<Vec<String>> {
        match self {
            Discovery::Static(list) => Ok(list.clone()),
            Discovery::Dns { name, port } => {
                let mut out: Vec<String> = (name.as_str(), *port)
                    .to_socket_addrs()?
                    .map(|sa| sa.to_string())
                    .collect();
                out.sort();
                out.dedup();
                Ok(out)
            }
        }
    }
}

/// A change to the member set, for logging. Membership churn that is not
/// logged is membership churn that is debugged from a packet capture.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MembershipChange {
    Added(String),
    Removed(String),
}

#[derive(Debug)]
struct PeerRecord {
    node_id: Option<NodeId>,
    flight: Option<String>,
    status: PeerStatus,
    last_seen_unix_ms: Option<u64>,
    last_error: Option<String>,
    consecutive_failures: u32,
}

impl PeerRecord {
    fn new() -> Self {
        Self {
            node_id: None,
            flight: None,
            status: PeerStatus::Unknown,
            last_seen_unix_ms: None,
            last_error: None,
            consecutive_failures: 0,
        }
    }
}

#[derive(Debug)]
struct State {
    /// Keyed by address so the view is deterministic and identical on every
    /// node — the M1 acceptance gate compares them verbatim.
    peers: BTreeMap<String, PeerRecord>,
    /// True once discovery has completed one pass, successfully. Readiness
    /// depends on this: a node that has not yet found the cluster is not ready
    /// to be sent traffic, even though it is perfectly alive.
    resolved: bool,
    /// Bumped on every membership change; lets a client tell a stale view from
    /// a current one without comparing member lists.
    generation: u64,
    last_resolved_unix_ms: Option<u64>,
    last_resolve_error: Option<String>,
}

/// This node's view of the cluster.
#[derive(Debug)]
pub struct Membership {
    self_id: NodeId,
    self_address: String,
    /// This node's own Flight address, set once at server startup (the
    /// listener binds after `Membership` is constructed in some tests, so it
    /// is a setter rather than a constructor argument).
    self_flight: Mutex<Option<String>>,
    /// Behind a lock because it is replaceable at runtime: reconfiguring the
    /// peer list is a legitimate operation (and it is how the integration tests
    /// wire three ephemeral-port nodes together after they have all bound).
    discovery: Mutex<Discovery>,
    state: Mutex<State>,
}

impl Membership {
    pub fn new(self_id: NodeId, self_address: impl Into<String>, discovery: Discovery) -> Self {
        Self {
            self_id,
            self_address: self_address.into(),
            self_flight: Mutex::new(None),
            discovery: Mutex::new(discovery),
            state: Mutex::new(State {
                peers: BTreeMap::new(),
                resolved: false,
                generation: 0,
                last_resolved_unix_ms: None,
                last_resolve_error: None,
            }),
        }
    }

    pub fn self_id(&self) -> NodeId {
        self.self_id
    }

    pub fn self_address(&self) -> &str {
        &self.self_address
    }

    pub fn discovery(&self) -> Discovery {
        self.discovery.lock().clone()
    }

    /// Replace the discovery source. The next discovery tick uses it.
    pub fn set_discovery(&self, discovery: Discovery) {
        *self.discovery.lock() = discovery;
    }

    /// Has discovery completed at least one successful pass?
    pub fn resolved(&self) -> bool {
        self.state.lock().resolved
    }

    pub fn generation(&self) -> u64 {
        self.state.lock().generation
    }

    /// Addresses of every peer (never including this node).
    pub fn peer_addresses(&self) -> Vec<String> {
        self.state.lock().peers.keys().cloned().collect()
    }

    /// Replace the member set with `addresses`, which MAY contain this node's
    /// own address — it is filtered out here.
    ///
    /// **Blocking**: self-identification resolves hostnames (see
    /// [`is_self_address`]). Call from a blocking thread, not a runtime worker.
    ///
    /// Records for surviving peers are preserved, so a re-resolution that
    /// returns the same set does not reset probe state and does not make a
    /// healthy cluster flicker through `Unknown`.
    pub fn set_members(&self, addresses: Vec<String>) -> Vec<MembershipChange> {
        let incoming: HashSet<String> =
            addresses.into_iter().filter(|a| !self.is_self(a)).collect();

        let mut state = self.state.lock();
        let mut changes = Vec::new();

        let existing: HashSet<String> = state.peers.keys().cloned().collect();
        for gone in existing.difference(&incoming) {
            state.peers.remove(gone);
            changes.push(MembershipChange::Removed(gone.clone()));
        }
        for added in incoming.difference(&existing) {
            state.peers.insert(added.clone(), PeerRecord::new());
            changes.push(MembershipChange::Added(added.clone()));
        }

        state.resolved = true;
        state.last_resolved_unix_ms = Some(now_unix_ms());
        state.last_resolve_error = None;
        if !changes.is_empty() {
            state.generation += 1;
        }
        changes.sort_by_key(|c| match c {
            MembershipChange::Added(a) => (1, a.clone()),
            MembershipChange::Removed(a) => (0, a.clone()),
        });
        changes
    }

    /// Record that discovery itself failed. Deliberately does NOT clear the
    /// member set: a DNS blip must not empty the cluster.
    pub fn record_resolve_error(&self, err: impl Into<String>) {
        let mut state = self.state.lock();
        state.last_resolve_error = Some(err.into());
    }

    /// This node's own Flight address, reflected in `members()` and gossiped
    /// to peers via `/healthz`.
    pub fn set_self_flight(&self, flight: Option<String>) {
        *self.self_flight.lock() = flight;
    }

    /// Record a successful probe of `address`.
    pub fn record_up(&self, address: &str, node_id: Option<NodeId>, flight: Option<String>) {
        let mut state = self.state.lock();
        if let Some(p) = state.peers.get_mut(address) {
            let was_down = p.status != PeerStatus::Up;
            p.status = PeerStatus::Up;
            p.last_seen_unix_ms = Some(now_unix_ms());
            p.last_error = None;
            p.consecutive_failures = 0;
            if node_id.is_some() {
                p.node_id = node_id;
            }
            if flight.is_some() {
                p.flight = flight;
            }
            if was_down {
                state.generation += 1;
            }
        }
    }

    /// Record a failed probe of `address`. Never removes the peer: an
    /// unreachable member is still a member, and forgetting it would make a
    /// restarting node look like a shrinking cluster.
    pub fn record_down(&self, address: &str, err: impl Into<String>) {
        let mut state = self.state.lock();
        if let Some(p) = state.peers.get_mut(address) {
            let was_up = p.status == PeerStatus::Up;
            p.status = PeerStatus::Down;
            p.last_error = Some(err.into());
            p.consecutive_failures = p.consecutive_failures.saturating_add(1);
            if was_up {
                state.generation += 1;
            }
        }
    }

    /// The full member list: this node first-class among the rest, sorted by
    /// address so every node in a healthy cluster renders an identical array.
    pub fn members(&self) -> Vec<Member> {
        let state = self.state.lock();
        let mut members: Vec<Member> = state
            .peers
            .iter()
            .map(|(address, p)| Member {
                address: address.clone(),
                node_id: p.node_id,
                flight: p.flight.clone(),
                is_self: false,
                status: p.status,
                last_seen_unix_ms: p.last_seen_unix_ms,
                last_error: p.last_error.clone(),
                consecutive_failures: p.consecutive_failures,
            })
            .collect();
        members.push(Member {
            address: self.self_address.clone(),
            node_id: Some(self.self_id),
            flight: self.self_flight.lock().clone(),
            is_self: true,
            // Answering this request is the proof.
            status: PeerStatus::Up,
            last_seen_unix_ms: Some(now_unix_ms()),
            last_error: None,
            consecutive_failures: 0,
        });
        members.sort_by(|a, b| a.address.cmp(&b.address));
        members
    }

    pub fn last_resolved_unix_ms(&self) -> Option<u64> {
        self.state.lock().last_resolved_unix_ms
    }

    pub fn last_resolve_error(&self) -> Option<String> {
        self.state.lock().last_resolve_error.clone()
    }

    fn is_self(&self, candidate: &str) -> bool {
        is_self_address(candidate, &self.self_address)
    }
}

/// Does `candidate` denote the node whose advertised address is `self_address`?
///
/// **Blocking** when either address is a hostname rather than an `ip:port`.
///
/// Checked in increasing order of cost, and every rule requires the ports to
/// agree — without that, running three nodes on one host (the local testbed,
/// and any `kind` node hosting two pods) would have every node swallow every
/// other as "self".
///
/// 1. Byte-identical strings. The overwhelmingly common case: a static peer
///    list that contains our own `--advertise` value verbatim.
/// 2. Same resolved `SocketAddr`. Catches `localhost:7777` vs `127.0.0.1:7777`
///    and any name/IP spelling difference.
/// 3. Same port **and** the candidate's IP belongs to a local interface. This
///    is the Kubernetes case: DNS hands back `10.244.1.7:7777`, our bind
///    address is `0.0.0.0:7777`, and the only thing that ties them together is
///    that `10.244.1.7` is on this pod's `eth0`.
pub fn is_self_address(candidate: &str, self_address: &str) -> bool {
    if candidate == self_address {
        return true;
    }

    let cand = resolve_all(candidate);
    if cand.is_empty() {
        return false;
    }
    let mine = resolve_all(self_address);

    if cand.iter().any(|c| mine.contains(c)) {
        return true;
    }

    let my_ports: HashSet<u16> = mine.iter().map(|s| s.port()).collect();
    if my_ports.is_empty() {
        return false;
    }
    let local = local_ip_addresses();
    cand.iter()
        .any(|c| my_ports.contains(&c.port()) && local.contains(&c.ip()))
}

/// Resolve a `host:port` authority to every socket address it denotes.
/// Blocking; returns empty on failure (an unresolvable candidate is simply not
/// us, which is the safe answer — it becomes a peer we will fail to probe and
/// mark `Down`, which is visible, rather than being silently swallowed as self).
fn resolve_all(authority: &str) -> Vec<SocketAddr> {
    if let Ok(sa) = authority.parse::<SocketAddr>() {
        return vec![sa];
    }
    authority
        .to_socket_addrs()
        .map(|it| it.collect())
        .unwrap_or_default()
}

/// Every IP address bound to a local interface.
///
/// Not cached: a pod's address set can change under it (a second interface from
/// a CNI plugin, an IPv6 address arriving late), and `getifaddrs` is one
/// syscall on a path that runs once per discovery tick.
pub fn local_ip_addresses() -> HashSet<IpAddr> {
    let mut out = HashSet::new();
    #[cfg(unix)]
    unsafe {
        use std::net::{Ipv4Addr, Ipv6Addr};
        let mut ifap: *mut libc::ifaddrs = std::ptr::null_mut();
        if libc::getifaddrs(&mut ifap) != 0 {
            return out;
        }
        let mut cur = ifap;
        while !cur.is_null() {
            let sa = (*cur).ifa_addr;
            if !sa.is_null() {
                match i32::from((*sa).sa_family) {
                    libc::AF_INET => {
                        let s = sa as *const libc::sockaddr_in;
                        let bits = u32::from_be((*s).sin_addr.s_addr);
                        out.insert(IpAddr::V4(Ipv4Addr::from(bits)));
                    }
                    libc::AF_INET6 => {
                        let s = sa as *const libc::sockaddr_in6;
                        out.insert(IpAddr::V6(Ipv6Addr::from((*s).sin6_addr.s6_addr)));
                    }
                    _ => {}
                }
            }
            cur = (*cur).ifa_next;
        }
        libc::freeifaddrs(ifap);
    }
    out
}

pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m() -> Membership {
        Membership::new(
            0,
            "127.0.0.1:7001",
            Discovery::Static(vec![
                "127.0.0.1:7001".into(),
                "127.0.0.1:7002".into(),
                "127.0.0.1:7003".into(),
            ]),
        )
    }

    #[test]
    fn self_is_filtered_out_of_the_peer_set_but_present_in_the_view() {
        let mem = m();
        let changes = mem.set_members(mem.discovery().resolve().unwrap());
        assert_eq!(changes.len(), 2, "two peers added, self excluded");
        assert_eq!(mem.peer_addresses().len(), 2);

        let members = mem.members();
        assert_eq!(members.len(), 3, "the view includes this node");
        assert_eq!(members.iter().filter(|x| x.is_self).count(), 1);
        // Sorted by address: identical ordering on every node.
        let addrs: Vec<&str> = members.iter().map(|x| x.address.as_str()).collect();
        assert_eq!(
            addrs,
            ["127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"]
        );
    }

    #[test]
    fn a_port_difference_alone_makes_a_peer_not_self() {
        // The local testbed depends on this: same host, same IP, three ports.
        assert!(!is_self_address("127.0.0.1:7002", "127.0.0.1:7001"));
        assert!(is_self_address("127.0.0.1:7001", "127.0.0.1:7001"));
    }

    #[test]
    fn localhost_and_the_loopback_literal_are_the_same_node() {
        assert!(is_self_address("localhost:7001", "127.0.0.1:7001"));
    }

    #[test]
    fn a_local_interface_ip_is_self_even_when_the_string_differs() {
        // Stands in for the Kubernetes case: DNS advertises the pod IP, we
        // bound something else, and only the interface list connects them.
        let local = local_ip_addresses();
        let ip = local
            .iter()
            .find(|ip| ip.is_ipv4() && !ip.is_loopback())
            .copied();
        if let Some(ip) = ip {
            assert!(is_self_address(&format!("{ip}:7001"), "127.0.0.1:7001"));
            // ...but only on a matching port.
            assert!(!is_self_address(&format!("{ip}:7999"), "127.0.0.1:7001"));
        }
    }

    #[test]
    fn resolution_is_idempotent_and_preserves_probe_state() {
        let mem = m();
        mem.set_members(mem.discovery().resolve().unwrap());
        mem.record_up("127.0.0.1:7002", Some(1), None);
        let gen_before = mem.generation();

        let changes = mem.set_members(mem.discovery().resolve().unwrap());
        assert!(changes.is_empty(), "no churn, no changes");
        assert_eq!(mem.generation(), gen_before, "generation must not advance");
        let up = mem
            .members()
            .into_iter()
            .find(|x| x.address == "127.0.0.1:7002")
            .unwrap();
        assert_eq!(up.status, PeerStatus::Up, "probe state survived");
        assert_eq!(up.node_id, Some(1));
    }

    #[test]
    fn churn_is_reported_and_bumps_the_generation() {
        let mem = m();
        mem.set_members(vec!["127.0.0.1:7001".into(), "127.0.0.1:7002".into()]);
        let g0 = mem.generation();
        let changes = mem.set_members(vec!["127.0.0.1:7001".into(), "127.0.0.1:7003".into()]);
        assert_eq!(
            changes,
            vec![
                MembershipChange::Removed("127.0.0.1:7002".into()),
                MembershipChange::Added("127.0.0.1:7003".into()),
            ]
        );
        assert!(mem.generation() > g0);
    }

    #[test]
    fn an_unreachable_peer_is_marked_not_removed() {
        let mem = m();
        mem.set_members(mem.discovery().resolve().unwrap());
        mem.record_up("127.0.0.1:7002", Some(1), None);
        mem.record_down("127.0.0.1:7002", "connection refused");
        mem.record_down("127.0.0.1:7002", "connection refused");

        let p = mem
            .members()
            .into_iter()
            .find(|x| x.address == "127.0.0.1:7002")
            .unwrap();
        assert_eq!(p.status, PeerStatus::Down);
        assert_eq!(p.consecutive_failures, 2);
        assert_eq!(p.last_error.as_deref(), Some("connection refused"));
        assert_eq!(mem.members().len(), 3, "still a member of the cluster");
    }

    #[test]
    fn a_resolve_failure_does_not_empty_the_cluster() {
        let mem = m();
        mem.set_members(mem.discovery().resolve().unwrap());
        mem.record_resolve_error("temporary failure in name resolution");
        assert_eq!(mem.peer_addresses().len(), 2);
        assert!(mem.resolved(), "an earlier success still counts");
        assert!(mem.last_resolve_error().is_some());
    }

    #[test]
    fn an_empty_static_list_is_a_resolved_single_node_cluster() {
        let mem = Membership::new(0, "127.0.0.1:7001", Discovery::Static(vec![]));
        assert!(!mem.resolved());
        mem.set_members(vec![]);
        assert!(mem.resolved(), "one node is a legitimate cluster");
        assert_eq!(mem.members().len(), 1);
    }
}
