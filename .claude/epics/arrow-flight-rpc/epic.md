---
name: arrow-flight-rpc
status: backlog
created: 2026-08-21T13:40:26Z
updated: 2026-08-21T13:40:26Z
progress: 0%
prd: .claude/prds/arrow-flight-rpc.md
github: (will be set on sync)
---

# Epic: arrow-flight-rpc

## Overview

Add an Arrow Flight gRPC endpoint to `serve`, running beside the existing
hyper HTTP server. SQL arrives as `FlightDescriptor::cmd`; results stream
back via `DoGet` as standard Flight data. Distributed queries reuse the
existing auto/scatter/gather machinery byte-for-byte — Flight is a second
front door, not a second engine.

## Architecture Decisions

1. **`arrow-flight = "53"` + `tonic 0.12`, and the arrow-53 pin does not
   move.** arrow-flight releases track arrow majors; 53.x builds against
   the exact arrow line this engine (and Lance 0.23) requires. The prior
   "Flight semantics, not the Flight crate" ruling (DISTRIBUTED-DESIGN.md)
   was about the *internal shuffle transport* and remains in force there —
   `/fragment` and gather stay on hyper + arrow-ipc. The lock-diff
   discipline from the Lance addition applies: adding the crates may only
   ADD entries, never change an existing version. `prost 0.13.5` is
   already in the lock and must be reused as-is.
2. **Extract the execution core out of the HTTP handler first.** The
   local-vs-distributed decision and execution (`server.rs::sql`,
   ~lines 1108–1148) is inline today. It moves to a shared
   `execute_statement(state, statement, mode) -> ExecOutcome` used by both
   the HTTP handler (behavior unchanged, headers built from `ExecOutcome`)
   and the Flight service. One decision path means Flight can never
   disagree with HTTP about when to distribute.
3. **Single endpoint per query.** `GetFlightInfo` returns one
   `FlightEndpoint` on the node that received the request; that node
   coordinates scatter/gather internally, exactly like `POST /sql`.
   Multi-endpoint fan-out (client fetches shards from every member) is
   designed room, not shipped scope (PRD out-of-scope).
4. **Tickets are self-contained and stateless**: the ticket carries the
   SQL statement + distribution mode (serde/JSON, size-capped like
   `MAX_SQL_BODY_BYTES`). No server-side query registry, no ticket expiry
   machinery, `GetFlightInfo` twice = plan twice. Matches the stateless
   `/sql` contract and keeps node crashes harmless.
5. **Flight port = HTTP port + 1 by default** (`--flight-bind` to
   override, `--flight-bind none` to disable). Advertised in `/cluster`
   membership JSON so tooling can discover it, but nodes never dial each
   other's Flight ports (internal traffic stays HTTP).
6. **Metadata parity with HTTP headers**: rows, elapsed, distributed
   true/false, shard count, and skip reason travel in
   `FlightInfo.app_metadata` / final `FlightData.app_metadata` as the same
   JSON the HTTP path builds, so the Python validator can assert parity.

## Technical Approach

### Backend Services

- `src/distributed/flight.rs` (new): `FlightService` impl —
  `GetFlightInfo` (plan, schema, one endpoint + ticket),
  `DoGet` (execute via shared core, `FlightDataEncoderBuilder` stream),
  `ListFlights` (registered tables), `GetSchema` (table or query schema),
  `Handshake` (no-op), `DoAction("cluster")` (membership JSON, parity
  with `GET /cluster`). Everything else: `Status::unimplemented`.
- `src/distributed/server.rs`: extract `execute_statement`; spawn tonic
  server from `serve`/`spawn` when Flight is enabled; thread
  `ServeOptions::flight_bind`; expose Flight address on `ServerHandle`
  for tests.
- `src/main.rs`: `--flight-bind` flag on the `serve` subcommand.
- Errors map to gRPC statuses: parse/bind → `InvalidArgument`, missing
  table → `NotFound`, not-ready → `Unavailable`, execution → `Internal`.

### Frontend Components

None (server feature). Client-side deliverable is a validation script,
`scripts/flight_validate.py` (pyarrow.flight), reused as the acceptance
gate.

### Infrastructure

- `scripts/cluster_local.sh`: pass/derive Flight ports for the 3-process
  harness; `verify` gains a Flight round-trip.
- CI-shaped gates stay local (no Docker on this box), same as M1/M2.

## Implementation Strategy

Land the dependency + extraction first (both are regression-risk-free and
unblock everything), then the service skeleton, then execution, then
distributed parity, then validation + docs. Rust integration tests use the
`arrow-flight` client half in-process against `spawn()`; Python validation
drives the shipped binary.

## Task Breakdown Preview

1. Dependencies: add arrow-flight/tonic, prove the lock diff is add-only,
   record the decision in Cargo.toml next to the existing note.
2. Extract `execute_statement` from the HTTP handler (no behavior change;
   suite green).
3. Flight service skeleton: module, serve wiring, `--flight-bind`,
   Handshake/ListFlights/GetSchema/DoAction("cluster").
4. Query path: GetFlightInfo + DoGet, ticket format, error mapping,
   metadata parity. [depends: 1,2,3]
5. Distributed parity: Flight queries through scatter AND gather on a
   3-process cluster; membership advertises flight ports. [depends: 4]
6. Rust integration tests (`tests/flight_tests.rs` + distributed_cluster
   extension). [depends: 4; distributed half 5]
7. Python acceptance: `scripts/flight_validate.py`, all-22 TPC-H parity
   single-node + 3-node, wired into `cluster_local.sh verify`. [depends: 5]
8. Docs: README, CLAUDE.md, DISTRIBUTED-DESIGN.md note update. [depends: 5]

## Dependencies

- `arrow-flight 53.x`, `tonic 0.12.x` (new; lock-diff gated).
- Reuses: `execute_any_distributed`, `plan_distributed`, `NodeState`,
  `membership`, `query_runtime`, `cluster_local.sh`, `.venv` pyarrow.

## Success Criteria (Technical)

- Lock diff: added entries only; default and `--features lance` builds
  green; full suite green with zero HTTP-path changes visible to tests.
- pyarrow client: 22/22 TPC-H cell-exact vs `POST /sql`, SF=0.01 + 0.1.
- 3-process cluster: 22/22 via Flight against each node, matching the
  single-process oracle; scatter-eligible queries report distributed=true
  in metadata.
- Error paths: bad SQL, unknown table, not-ready node each produce the
  mapped gRPC status (asserted in tests).

## Estimated Effort

~8 tasks. Largest risks: tonic/h2 lock interactions (task 1 proves it
early) and streaming metadata ergonomics in arrow-flight 53 (task 4).

## Tasks Created
- [ ] 001.md - Add arrow-flight/tonic dependencies with add-only lock diff (parallel: true)
- [ ] 002.md - Extract execute_statement from the HTTP sql handler (parallel: true)
- [ ] 003.md - Flight service skeleton and serve wiring (parallel: false)
- [ ] 004.md - Query path — GetFlightInfo, DoGet, tickets, error mapping (parallel: false)
- [ ] 005.md - Distributed parity — Flight on every node of a real cluster (parallel: false)
- [ ] 006.md - Rust integration tests for the Flight endpoint (parallel: true)
- [ ] 007.md - Python acceptance gate — pyarrow.flight TPC-H parity (parallel: true)
- [ ] 008.md - Documentation — README, CLAUDE.md, design-note update (parallel: true)

Total tasks: 8
Parallel tasks: 5
Sequential tasks: 3
Estimated total effort: 26 hours
