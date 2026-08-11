//! Running the engine as multiple independent instances.
//!
//! **Milestone 1 only.** What is here is the substrate: a server process, peer
//! discovery, membership, health, and a SQL endpoint that executes *locally*.
//! There is deliberately no exchange, no shuffle, no fragment execution and no
//! distributed join — those are M2 and M3, and
//! `.claude/plans/DISTRIBUTED-READINESS.md` documents three single-node defects
//! that must be fixed before M3 is even attempted.
//!
//! The reason M1 is worth having on its own: it is the only part of the design
//! that can be tested to completion on a machine without Docker. N processes on
//! one host over real TCP have separate address spaces, real sockets, real
//! serialization and real partial failure. What they do not have is a network
//! partition or a kubelet, which is exactly what the Kubernetes artifacts in
//! `k8s/` are for — and those are marked UNVALIDATED-ON-CLUSTER until someone
//! runs `scripts/kind_test.sh` where Docker exists.
//!
//! ```text
//! query_engine serve --bind 0.0.0.0:7777 \
//!     --peers 10.0.0.1:7777,10.0.0.2:7777 \   # or --peers-dns qe-headless
//!     --node-id 0 --data ./data/tpch-10mb
//! ```

pub mod http_client;
pub mod membership;
pub mod server;

pub use http_client::{get as http_get, post_text as http_post, HttpResponse};
pub use membership::{Discovery, Member, Membership, MembershipChange, NodeId, PeerStatus};
pub use server::{serve, spawn, NodeState, ServeOptions, ServerHandle, TableLoader};
