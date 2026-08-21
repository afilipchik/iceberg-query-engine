---
name: arrow-flight-rpc
description: Arrow Flight RPC endpoints for the query engine, serving single-node and distributed queries as gRPC Arrow streams
status: backlog
created: 2026-08-21T13:40:26Z
---

# PRD: arrow-flight-rpc

## Executive Summary

Expose the query engine over Arrow Flight — the Arrow ecosystem's standard
gRPC protocol for streaming columnar data. Today the only network interface is
the bespoke HTTP server (`serve`: `POST /sql` returning an Arrow IPC body).
That works, but no off-the-shelf client speaks it: every consumer needs custom
code to POST SQL and decode the body. With a Flight endpoint, `pyarrow.flight`,
ADBC drivers, and any Flight-capable tool can query the engine (and the
distributed cluster) with zero custom client code.

## Problem Statement

- The engine's results are Arrow RecordBatches end to end, but the last hop to
  a client is a custom HTTP contract. Standard tooling (Python, Java, ADBC,
  BI connectors) cannot connect without hand-written glue.
- The distributed mode (`serve` with peers) makes this worse: the custom
  headers (`x-qe-distributed`, etc.) and fragment endpoints are ours alone.
- Arrow Flight is purpose-built for exactly this: SQL in a
  `FlightDescriptor`, schema negotiation, and server-side streaming of Arrow
  data over gRPC, with multi-endpoint support for parallel fetch.

## User Stories

1. **Python analyst**: "I `pip install pyarrow`, connect with
   `flight.connect('grpc://host:7778')`, call
   `get_flight_info(FlightDescriptor.for_command(sql))` then `do_get(ticket)`,
   and receive the full result as Arrow batches."
   - *Acceptance*: a stock pyarrow client runs all 22 TPC-H queries and gets
     results cell-identical to `POST /sql`.
2. **Cluster operator**: "Every node of my 3-node cluster serves Flight. I can
   point a client at ANY node and get correct distributed answers, same as the
   HTTP path."
   - *Acceptance*: 3-process cluster (`cluster_local.sh`) with Flight enabled
     answers TPC-H distributed; results match the single-process oracle.
3. **Data engineer**: "I can list tables and fetch a table's schema over
   Flight without running a query."
   - *Acceptance*: `list_flights` enumerates registered tables;
     `get_schema` returns a table's Arrow schema.

## Functional Requirements

- `serve` starts a Flight gRPC server alongside the HTTP server, on its own
  port (`--flight-bind`, default HTTP port + 1; `--flight-bind none` disables).
- `GetFlightInfo` with `FlightDescriptor::cmd = <SQL bytes>` plans the query
  and returns schema + endpoint(s) whose tickets are self-contained.
- `DoGet(ticket)` executes and streams the result as Flight data
  (dictionary batches handled by the standard encoder).
- Distributed execution: a query received via Flight goes through the SAME
  auto/scatter/gather machinery as `POST /sql` (`distributed=auto`
  semantics). Whether execution was distributed is reported in metadata.
- `ListFlights` lists registered tables; `GetSchema` returns a table's or
  query's schema.
- Errors surface as proper gRPC statuses with the engine's error text, never
  as empty streams.

## Non-Functional Requirements

- **The arrow-53 pin is inviolable.** `arrow-flight` must be the 53.x line;
  adding it (and tonic) must not move ANY existing crate version in
  `Cargo.lock` (verified by diff, same discipline as the Lance addition).
- The prior "Flight semantics, not the Flight crate" decision stays in force
  for the internal shuffle/fragment transport — this PRD adds a client-facing
  endpoint, it does NOT rewrite `/fragment` or the gather path.
- Memory safety rules apply unchanged: streaming a large result must not
  buffer the whole result set beyond what the existing execution already does.
- No regression to the HTTP path or single-node performance (Flight code is
  additive; `serve` without Flight clients behaves identically).

## Success Criteria

1. All 22 TPC-H queries via pyarrow.flight == `POST /sql` results,
   cell-exact, single node (SF=0.01 and SF=0.1).
2. Same comparison green on a 3-process cluster, client pointed at each of
   the 3 nodes in turn.
3. `Cargo.lock` diff shows only ADDED crates; zero version changes to
   existing entries; default build (`cargo build`) and
   `--features lance` build both stay green.
4. Full test suite green; new Flight integration tests included.
5. README + CLAUDE.md document the endpoint and client usage.

## Constraints & Assumptions

- arrow 53 → `arrow-flight = "53"`, which pulls `tonic 0.12` / `prost 0.13`
  (prost 0.13.5 already in the lock).
- gRPC needs HTTP/2; that lives inside tonic's own hyper stack and does not
  require enabling the `http2` feature on our direct hyper dependency.
- The dev box has no Docker; distributed validation uses the established
  N-process harness (`scripts/cluster_local.sh`).
- Python validation uses the repo `.venv` (pyarrow already present for the
  existing oracle scripts).

## Out of Scope

- **Flight SQL** (the extended protocol with prepared statements, catalogs,
  transactions) — plain Flight with SQL-in-descriptor covers the user
  stories; Flight SQL can layer on later.
- Replacing the internal fragment/shuffle transport with Flight.
- Multi-endpoint parallel result fetch (one endpoint per cluster member for
  merge-free queries) — designed for, noted in tickets, but not required to
  ship. Single-endpoint-per-query is the contract.
- Authentication/TLS (the HTTP server has none either; parity is the bar).
- DoPut/DoExchange (writes over Flight).

## Dependencies

- `arrow-flight 53.x`, `tonic 0.12.x` (new crates in the lock).
- Existing: `src/distributed/server.rs` (execution entry points to reuse),
  `membership.rs` (cluster state), `scripts/cluster_local.sh` (validation).
