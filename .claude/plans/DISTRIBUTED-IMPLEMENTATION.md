# Distributed implementation plan (execution order)

**Goal:** run the engine as multiple separate instances, tested on Kubernetes
(kind). Companion to `DISTRIBUTED-DESIGN.md` (architecture) and
`DISTRIBUTED-READINESS.md` (blockers).

## 0. Environment constraint, stated up front

**kind cannot run on this development machine.** Verified: no `docker`,
`podman`, `kind` or `kubectl` binaries; no passwordless sudo; and
`kernel.apparmor_restrict_unprivileged_userns = 1` (Ubuntu 24.04 default) blocks
unprivileged user namespaces, so even rootless podman is unavailable —
`unshare --user --map-root-user` fails with EPERM. Docker needs a root daemon;
kind needs Docker or Podman.

Consequences, and how this plan handles them:

* Everything is built to run as **N separate OS processes talking over real TCP
  sockets**. That is the same distribution problem as pods — separate address
  spaces, real sockets, real serialization, real partial failure — and it is
  fully testable here. `scripts/cluster_local.sh` runs it.
* The **Kubernetes artifacts are produced and kept CI-verifiable**: `Dockerfile`,
  `k8s/` manifests, `kind-cluster.yaml`, and `scripts/kind_test.sh`. They are
  written to be correct-by-construction and validated as far as is possible
  without a cluster (YAML parse, image build steps, the discovery code path that
  k8s exercises). They are marked **UNVALIDATED-ON-CLUSTER** until someone runs
  them where Docker exists. No number from a real cluster will be claimed here.
* The discovery layer is designed so **the same binary works both ways**: a
  static `--peers` list (local testing) and DNS-based discovery against a
  headless Service (Kubernetes). This is the one design decision that makes the
  local testbed a faithful proxy rather than a toy.

## 1. What we are NOT doing yet, and why

`DISTRIBUTED-READINESS.md` documents three blockers for *correct distributed
query execution*: callers that drive only partition 0, an unenforced memory
budget, and a planner that executes sub-plans at planning time (87% of Q15).

M1 and M2 below are chosen specifically because they **do not depend on those
blockers**: M1 is process/transport substrate, and M2 shards a whole query per
node so each node runs an ordinary local plan. Queries whose shape would hit a
blocker are **rejected loudly**, never silently mis-answered — see M2's gate.
The blockers must be fixed before M3 (shuffle), where they become fatal.

## 2. Milestones

### M1 — Server mode, membership, health  *(the substrate; unblocks "multiple instances")*

* `query_engine serve --bind 0.0.0.0:7777 [--peers a:7777,b:7777 | --peers-dns <headless-svc>] [--node-id N]`
* HTTP over **hyper 1.11** with **arrow-ipc 53.4.1** payloads — both already in
  `Cargo.lock`. No `tonic`, no `arrow-flight`: adding them risks forcing an
  arrow-major bump against the arrow-53 pin Lance requires.
* Endpoints: `GET /healthz` (liveness), `GET /readyz` (ready = tables loaded and
  peers resolved), `GET /cluster` (membership view as JSON), `POST /sql` (submit
  a query, returns Arrow IPC), `POST /fragment` (execute a plan fragment).
* Discovery: static list, or DNS A-record resolution of a headless Service,
  re-resolved on a timer so pod churn is picked up. Self-identification by
  matching local addresses so a node never counts itself twice.
* **Gate:** 3 local processes; `GET /cluster` on each returns the same 3-member
  view; each answers `/sql` independently with results identical to the
  single-process binary.

### M2 — Distributed scatter-gather  *(the first real distributed answer)*

Shard-per-node, two-phase aggregation — the ClickHouse-shaped subset that needs
**no shuffle** and therefore none of the readiness blockers.

* Deterministic file-level sharding: node *i* owns files where
  `hash(path) % N == i`. Every node can compute the whole assignment, so no
  central assignment service is required.
* Any node may receive `/sql` and become the **initiator** for that query: it
  fans out to peers, each computes a partial over its own shard, the initiator
  finalizes. Control-plane cost therefore spreads across nodes by rotation.
* **Supported in M2, enforced by a planner check that FAILS LOUDLY otherwise:**
  scans, filters, projections, and aggregations whose partial/final split is
  exact (`COUNT`, `SUM`, `MIN`, `MAX`, and `AVG` as sum+count). Anything else —
  joins across shards, `COUNT(DISTINCT)`, correlated or uncorrelated subqueries,
  `ORDER BY`+`LIMIT` needing a global sort — returns
  `NotImplemented("<reason>; distributed execution supports ...")`.
  Rejecting is the entire point: a silent shard-local answer presented as a
  cluster answer is the failure mode this project exists to avoid.
* **Gate:** `SELECT COUNT(*)`, `SUM`, `MIN/MAX`, and a `GROUP BY` aggregate over
  sharded TPC-H **cell-exact against DuckDB reading all the data**, run against a
  3-node local cluster, and identical whichever node receives the query.

### M3 — Exchange / shuffle  *(needs the readiness blockers fixed first)*

Hash-partitioned exchange, distributed joins, `ExchangeExec` over the existing
partitioned operator model. Not started until P-1 lands.

## 3. Kubernetes artifacts (built in M1, exercised in M2)

* `Dockerfile` — multi-stage; builds the release binary, ships a slim runtime
  image. Data mounted, not baked.
* `k8s/statefulset.yaml` — stable pod identities; `QE_NODE_ID` from the ordinal.
* `k8s/service-headless.yaml` — the DNS name `--peers-dns` resolves.
* `k8s/service.yaml` — client entry point (any pod can serve).
* `kind-cluster.yaml` — 1 control-plane + 3 workers, with a host mount for data.
* `scripts/kind_test.sh` — create cluster, build and `kind load` the image,
  apply manifests, wait for readiness, run the M2 gate queries against the
  Service, tear down. **This is the script that must be run on a Docker-capable
  machine to validate the k8s path.**

## 4. Order of work

M1 server+membership → M1 k8s artifacts → M2 sharding+two-phase → M2 gate.
Then P-1 prerequisites, then M3.
