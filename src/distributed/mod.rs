//! Running the engine as multiple independent instances.
//!
//! **Milestones 1 and 2.**
//!
//! * **M1** is the substrate: a server process, peer discovery, membership,
//!   health, and a SQL endpoint.
//! * **M2** is balanced scatter–gather: [`splits`] cuts a table into row ranges
//!   inside row groups, [`splits::assign_lpt`] divides them by *bytes*,
//!   [`shard`] gives one node a table restricted to its share, [`plan`] decides
//!   whether a query can be answered this way at all, and [`coordinator`] fans
//!   out and merges.
//!
//! There is deliberately still no exchange, no shuffle and no distributed
//! join — that is M3, and `.claude/plans/DISTRIBUTED-READINESS.md` documents
//! three single-node defects that must be fixed before it is attempted. M2 is
//! scoped precisely so it depends on none of them: every worker runs an
//! ordinary local plan over a smaller table, and every query shape that would
//! need more is REJECTED by name rather than answered approximately.
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

pub mod coordinator;
pub mod gather;
pub mod http_client;
pub mod membership;
pub mod plan;
pub mod server;
pub mod shard;
pub mod splits;

pub use coordinator::{
    execute_any_distributed, execute_distributed, execute_fragment, execute_gathered, splits_of,
    DistributedResult, Distribution, FragmentRequest, FragmentTransport, NodeContribution,
    Participant,
};
pub use gather::{plan_gather, GatherPlan, GatherTable};
pub use http_client::{get as http_get, post_text as http_post, HttpResponse};
pub use membership::{Discovery, Member, Membership, MembershipChange, NodeId, PeerStatus};
pub use plan::{plan_distributed, DistributedPlan, MergeShape};
pub use server::{serve, spawn, NodeState, ServeOptions, ServerHandle, TableLoader};
pub use shard::ShardedParquetTable;
pub use splits::{assign_lpt, enumerate_parquet, Assignment, Split, SplitSet};
