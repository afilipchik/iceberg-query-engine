//! Running the engine as multiple independent instances.
//!
//! **Milestone 1 only.** What is here is the substrate: peer discovery,
//! membership, and the small HTTP client the health prober needs. The server
//! itself lands next. There is deliberately no exchange, no shuffle, no
//! fragment execution and no distributed join — those are M2 and M3, and
//! `.claude/plans/DISTRIBUTED-READINESS.md` documents three single-node defects
//! that must be fixed before M3 is even attempted.

pub mod http_client;
pub mod membership;

pub use http_client::{get as http_get, post_text as http_post, HttpResponse};
pub use membership::{Discovery, Member, Membership, MembershipChange, NodeId, PeerStatus};
