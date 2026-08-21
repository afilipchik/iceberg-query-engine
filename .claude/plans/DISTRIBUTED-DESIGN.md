# Distributed Execution Design — Iceberg Query Engine

**Status:** approved architecture, ready to implement
**Audience:** engineers who have read none of the prior research
**Acceptance criterion:** aggregate throughput scales linearly with node count, with no regression to single-node performance (SF=10 TPC-H in 7.4s parquet / 6.7s Lance) and no regression to memory safety (OOM is never acceptable)

---

## 0-CORRECTION (2026-08-10): the f=0.150 in this section is an ARTIFACT — do not act on it

The architecture review re-derived Section 0's numbers and found the measurement
method invalid. `scripts/scaling_bench.py` pinned CONTIGUOUS cpu ids on a hybrid
SMT machine, so its "cores" axis was not a core-count axis at all: on this
i9-13900KF, K=1 is one SMT thread, **K=2 is BOTH THREADS OF ONE PHYSICAL CORE**,
K=16 is all 8 P-cores, and K=32 adds 16 slower E-cores. The 1.18x at "2 cores"
that drove the high serial fraction was two hyperthreads sharing one core.

Re-measured over homogeneous PHYSICAL P-cores (harness fixed in the same pass):

| cores | wall | speedup | efficiency |
|------:|-----:|--------:|-----------:|
| 1 | 42.83s | 1.00 | 100% |
| 2 | 29.90s | 1.43 | 72% |
| 4 | 16.61s | 2.58 | 64% |
| 8 | 10.12s | 4.23 | 53% |

A per-point Amdahl fit gives f = 0.389 / 0.189 / 0.129 — a spread that REJECTS
the single-serial-fraction model. There is no "6.7x cap"; what the data shows is
a CONTENTION term, so fit a USL (contention + coherency) rather than quoting one f.

CONSEQUENCES FOR THIS PLAN, which must be re-derived before implementation:
* The claim "single-query speedup is capped at 6.7x forever" is withdrawn.
* P2's accept gate (efficiency 0.177 -> >= 0.25 at 32 cores) is measured on the
  invalid axis and must be restated over physical cores (baseline 53% at 8).
* The promotion of replica routing to P1 was justified partly by that cap. The
  +46% shared-nothing result is INDEPENDENT of this error and still stands, so
  P1 survives — but on its own evidence, not on f.

## 0. Read this first: the measurement that drives the whole plan

Two files in `.scratch/` were produced on this machine on 2026-08-09 by a sibling agent. I did not re-run them, but I read them and they are the empirical foundation of this design.

`.scratch/intranode_scaling.json` — one process, TPC-H mix, varying core count:

| cores | wall (s) | QPS | speedup | efficiency |
|------:|---------:|----:|--------:|-----------:|
| 1 | 46.03 | 0.478 | 1.00x | 1.000 |
| 2 | 39.04 | 0.564 | 1.18x | 0.590 |
| 4 | 21.71 | 1.013 | 2.12x | 0.530 |
| 8 | 12.60 | 1.746 | 3.65x | 0.457 |
| 16 | 8.75 | 2.514 | 5.26x | 0.329 |
| 32 | 8.13 | 2.706 | **5.66x** | **0.177** |

`.scratch/proc_scaling.json` — K independent processes, 32/K cores each, **same 32 cores total**:

| procs | cores/proc | aggregate QPS |
|------:|-----------:|--------------:|
| 1 | 32 | 2.733 |
| 2 | 16 | 3.088 |
| 4 | 8 | 3.657 |
| 8 | 4 | **3.988** |

Three conclusions follow, and they reorder the entire project:

**(1) The engine has a 15% serial fraction.** From speedup(32) = 5.66x, Amdahl gives `f = (1/5.66 − 1/32)/(1 − 1/32) = 0.150`. Single-query speedup is capped at **6.7x forever**, no matter how many cores. The four `output_partitions() == 1` funnels this document removes (`hash_agg.rs:214`, `morsel_agg.rs:283`, `hash_join.rs:1225`, `delim_join.rs:145/244`) are a direct attack on that `f`, and the sweep above is the regression gate for it.

**(2) Partitioning the box into independent processes is already worth +46% throughput.** 8 processes x 4 cores beats 1 process x 32 cores by 46% on identical hardware with **zero distribution machinery**. When the acceptance criterion is throughput, this is the single largest available win and it must be banked first. All three input designs placed replica-group routing at phase 7 or later; that is wrong, and this document places it at **P1**.

**(3) This box costs ~50% to shared memory bandwidth, and that must be cancelled out of every experiment.** Define the box contention factor

```
eta_box(K) = QPS_shared_nothing(K procs) / (K x QPS_1proc(32/K cores))
```

Measured: **1.01 at K=1, 0.614 at K=2, 0.524 at K=4, 0.492 at K=8.** An engineer who measures only absolute throughput at constant total cores would report a ~50% "distribution tax" at K=8 that is **entirely hardware**, and would then spend weeks optimizing a shuffle that was never the problem. Section 9 defines the paired-control experiment that removes this term exactly.

---

## 1. Decision and rationale

### 1.1 What we are building

**Fragment-and-Exchange over symmetric peers.** Every existing operator stays exactly as it is — partition-local, unaware that nodes exist. Distribution becomes a property of **plan structure**:

1. A new physical-optimizer pass, `EnforceDistribution`, inserts `ExchangeExec` wherever a child's declared partitioning fails to satisfy its parent's required distribution — and, just as importantly, **elides** exchanges that are already satisfied.
2. `PlanFragmenter` cuts the tree at exchanges marked `Remote`, producing a stage DAG whose roots are `ShuffleWriteExec` and whose leaves are `ShuffleReadExec`.
3. All cross-node reasoning lives in `src/distributed/` (~2500 lines). Nothing above or below an exchange learns what a node is.

This composes with the existing operator model with **zero change to the call convention**: `ExchangeExec::execute(partition)` returns the receive end of output channel `partition`. That is exactly the shape `async fn execute(&self, partition: usize) -> Result<RecordBatchStream>` was built for.

### 1.2 Why this backbone

The trait at `src/physical/plan.rs:16-33` (VERIFIED this session) is DataFusion's `ExecutionPlan` minus exactly three things:

```rust
pub trait PhysicalOperator: Debug + Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>>;
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream>;
    fn output_partitions(&self) -> usize { 1 }   // (1) no key expressions
    fn name(&self) -> &str;
}                                                 // (2) no required_input_distribution
                                                  // (3) no with_new_children
```

Gap (1) is not cosmetic. Concrete proof from Q03: after a hash exchange on `l_orderkey`, the aggregate groups on `(l_orderkey, o_orderdate, o_shippriority)` — a **superset** of the partition key — so it is already correctly partitioned and needs **no second exchange**. `Partitioning::Hash(exprs, n)` can express that; `usize` cannot. That single elision removes an entire shuffle round from Q03.

Gap (3) is a hard blocker I verified by grep: **zero occurrences of `with_new_children` anywhere in `src/physical/`**. No tree-rewriting physical pass is writable today. There are **18** `impl PhysicalOperator for` sites (not 12, as the inventory states); the method gets a `NotImplemented` default and is implemented on the ~8 that can appear above an exchange.

Splits already exist: `StreamingParquetScanExec` holds `partitioned_work: Vec<Vec<RowGroupWork>>` and returns its length from `output_partitions()` (`streaming_parquet_scan.rs:510`). `ParallelParquetSource::get_work()` (`morsel.rs:287`) is already an atomic pull-queue over row groups — cluster-level split stealing is that mechanism lifted one level, not an invention.

The partial/final aggregate split already exists: `AccumulatorState` is a closed enum at `morsel_agg.rs:623` with `merge` at `:896`, and `AggregationState::merge` at `:2270` — those **are** the partial+final combine, already used to merge thread-local tables. Distributed aggregation needs an *encoding*, not an algorithm.

### 1.3 The three grafts

**Graft 1 — symmetric peers with per-query initiator election (from Design C).** There is no dedicated coordinator process. One binary, role by flag; any node can be the **initiator** for any query. Rationale: let `F` be per-query fixed cost (parse, bind, optimize, dispatch, status aggregation, gather). On a dedicated coordinator, `F` runs on one process, so cluster QPS is capped at `1/F` **regardless of N** — Amdahl applied to the control plane. With initiator election the role rotates and QPS is `N/F`. When the criterion is throughput, this is not a close call.

**Graft 2 — Flight semantics, not the Flight crate (from Design C).** VERIFIED in `Cargo.lock`: `arrow-ipc 53.4.1` (:290), `hyper 1.11.0` (:2662), `prost 0.13.5` (:4286), `dashmap 6.1.0` (:1461), `parking_lot 0.12.5` (:4026) are all present; **`tonic` and `arrow-flight` are not**. The arrow-53 pin is deliberate (Lance requires it), and a transport that forces an arrow bump is an instant single-node regression risk. We own the `ShuffleTransport` trait and implement Flight's *semantics* (Arrow IPC bodies, ticket/mailbox descriptors, DoGet-shaped pull RPCs) over hyper. Note `bincode` is **not** in the lock; use `serde` + `prost` or add `bincode` explicitly.

> **AMENDMENT 2026-08-21 (arrow-flight-rpc epic).** The ruling above is
> UNCHANGED for the internal transport: `/fragment` and gather still run on
> hyper + arrow-ipc, and nodes never dial each other over gRPC. What changed:
> `arrow-flight = "53"` + `tonic 0.12` were added for the CLIENT-FACING
> endpoint (`serve --flight-bind`, `src/distributed/flight.rs`). The risk this
> ruling guarded against — a forced arrow-major bump breaking the Lance pin —
> was shown void for the 53.x line: the Cargo.lock diff on addition was
> verified ADD-ONLY (arrow stayed 53.4.1, prost stayed 0.13.5; ten new
> entries, zero moved). See `.claude/epics/arrow-flight-rpc/`.


**Graft 3 — replica-group routing promoted to P1 (from Design B).** Justified entirely by Section 0. Design A placed it at Phase 7; the measured data says it is the largest win available and it doubles as the experimental control.

### 1.4 What was rejected

| Rejected | Why |
|---|---|
| **Design B as backbone** (rewritten SQL + `PlanPins` to shards) | Correctness would depend on SQL text round-tripping losslessly through our binder **and** on every shard independently re-deriving a bit-identical plan. That makes cross-process plan determinism a *correctness requirement* — strictly stronger than serializing the plan once. Any construct whose `Display` does not re-parse identically becomes a silent wrong answer or an unannounced single-node fallback. B also cannot express Q09's three shuffle rounds without growing a stage DAG, i.e. without becoming A. |
| **Design C as a separate design** | It is A + initiator election + a dependency ruling. Merged wholesale, not rejected. |
| Dedicated coordinator (Trino topology) | Caps QPS at `1/F` regardless of N. |
| Spark-style materialize-every-stage | Doubles peak memory; violates the no-OOM rule. |
| Pinot broker scatter/gather | Cannot do a distributed join without building this exchange machinery anyway. |
| ClickHouse two-phase-only | Cannot express multi-shuffle plans (Q09). |
| `arrow-flight` / `tonic` | Absent from the lock; risks the arrow-53 Lance pin. |

| Speculative duplicate execution | Needs idempotent re-execution over materialized inputs; doubles peak memory. The token/sequence buffer covers the dominant failure class far more cheaply. |
| DataFusion `preserve_order` repartition | Allocates `N_in x N_out` channels — over a network, `N^2` streams. |
| All approximation-for-scale knobs | Pinot `numGroupsLimit`, `maxRowsInJoin`/`JoinOverFlowMode`, leaf-local group trim, ClickHouse `distributed_group_by_no_merge` / `distributed_product_mode=allow`, Trino `LowMemoryKiller`. Cell-exactness vs DuckDB is what makes every number here trustworthy. We take Trino's accounting and observability and leave the killer. |

---

## 2. Architecture

### 2.1 Node roles and process model

One binary. Role by flag:

```
query_engine serve --node-id 2 --peers h0:7100,h1:7100,h2:7100,h3:7100 \
                   --replica-groups "0,1;2,3" \
                   --control-port 7100 --data-port 7101 \
                   --cores 8 --memory-limit 24GB
```

Static membership — honest and sufficient at N ≤ 32, and it removes an entire dependency class (no etcd, ZooKeeper or Helix). Every node runs both the control service and the shuffle service, **on separate ports**, so a saturated shuffle can never starve cancellation.

**Initiator (per query, not per process).** The client connects to any node. That node becomes the initiator for that one query and does work that is `O(stages x N + N x K)` and **never `O(rows)`, `O(splits)` or `O(result)`**:

- parse → bind → logical optimize → physical plan → `EnforceDistribution` → `DeterminePartitionCount` → `PlanFragmenter`
- serialize the fragment DAG **once** (~10–100 KB) and unicast it to N peers in one RPC each
- ship a split **assignment rule** (`xxhash64(path, rg_idx) % N == my_id`), never a split list
- long-poll task status with a **version header** (so unchanged status is never re-serialized) and a randomized wait in `[T/2, T]` (so N tasks started together never resynchronize into a poll storm)
- host stage 0, the gather, which after top-K pushdown receives at most `N x K` rows

The initiator **never** routes a data page. Workers are peer-to-peer on the data path.

### 2.2 Two execution modes, both always available

| Mode | When | Fan-out | Shuffle | Scaling property |
|---|---|---|---|---|
| **Replica-local** (P1) | Query's tables all fit one replica group | 1 group | none | `QPS ≈ R x QPS(one group)`, linear in total nodes at **constant** per-query latency |
| **Sharded** (P5+) | Dataset larger than one group, or explicitly sharded | all N | stage DAG | weak scaling: data ∝ N, latency flat |

Replica-local is not a degenerate case — it is the throughput mode, and Section 0 says it is worth +46% on this box today. Sharded mode is the capacity mode.

### 2.3 Query lifecycle

```
client ──SQL──▶ ANY NODE (becomes INITIATOR)
   │
   ├─ admission gate (global semaphore + free-memory floor)
   ├─ parse → bind → Optimizer (logical rules + DPsize CBO, now with cluster_size and (cpu,net,mem) cost)
   ├─ PhysicalPlanner (unchanged)
   ├─ EnforceDistribution      [NEW: strip-then-reinsert; inserts AND elides]
   ├─ DeterminePartitionCount  [NEW: block-size-targeted widths]
   ├─ PlanFragmenter           [NEW: cut at Remote exchanges → Vec<PlanFragment>]
   └─ serialize DAG once → StartStages RPC to each peer (one RPC per peer, all stages)
        │
   W_i: build tasks → enumerate OWN splits from the rule
        → operators run UNCHANGED, partition-local
        → ShuffleWriteExec: partition → Arrow IPC encode → SinkBuffer (token/seq)
        │
   W_j: ShuffleReadExec pulls with credit from each peer mailbox
        → local ExchangeExec fans out to num_cpus partitions (SALT_LOCAL)
        → operators run UNCHANGED
        │
   INITIATOR: stage 0 gather → k-way merge of N pre-sorted top-K streams
        → STREAMS to client (no try_collect)
```

---

## 3. The concrete Rust surface

### 3.1 Modified files

| File | Change |
|---|---|
| `src/execution/memory.rs` | Add `OwnedMemoryReservation` alongside the existing `MemoryReservation<'a>` |
| `src/physical/plan.rs` | Four new trait methods, three defaulted |
| `src/physical/operators/scan.rs` | `splits()`, `scan_split()`, `output_partitioning()` on `TableProvider`, all defaulted |
| `src/physical/operators/vectorized_hash.rs` | Avalanche fix, salts, hard type gate |
| `src/physical/operators/hash_agg.rs`, `src/physical/morsel_agg.rs` | `AggregateMode`, state codec |
| `src/optimizer/rules/join_reorder.rs` | `(cpu, net, mem)` cost triple, `cluster_size` |
| `src/execution/context.rs` | `sql_stream()`, per-stage metrics |
| `src/main.rs` | `serve` subcommand |

### 3.2 New files

```
src/physical/partitioning.rs                      Partitioning, Distribution, satisfies()
src/physical/optimizer/mod.rs                     PhysicalOptimizerRule trait  (NEW DIRECTORY)
src/physical/optimizer/enforce_distribution.rs
src/physical/optimizer/determine_partition_count.rs
src/physical/exchange/mod.rs
src/physical/exchange/distributor.rs              port of DataFusion distributor_channels.rs
src/physical/exchange/partitioner.rs              counting-sort batch partitioner
src/physical/operators/exchange.rs                ExchangeExec

src/distributed/mod.rs
src/distributed/fragmenter.rs                     PlanFragment, PlanFragmenter  (~500 lines)
src/distributed/proto.rs                          FragmentProto serde IR
src/distributed/topology.rs                       ClusterTopology, BucketMap, SplitAssignment
src/distributed/operators/shuffle_write.rs
src/distributed/operators/shuffle_read.rs
src/distributed/transport/mod.rs                  ShuffleTransport / Sink / Source traits
src/distributed/transport/inproc.rs               THE DEFAULT
src/distributed/transport/tcp.rs                  hyper 1.11, ConnectionPool
src/distributed/codec.rs                          Arrow IPC page codec
src/distributed/credit.rs                         CreditController
src/distributed/sink_buffer.rs                    token/sequence page buffer
src/distributed/spill_ipc.rs                      shuffle spill to .arrows
src/distributed/runtime_filter.rs                 distributed RF union + ladder
src/distributed/gather.rs                         OrderedGather k-way merge
src/distributed/scheduler.rs                      split assignment, work stealing
src/distributed/admission.rs                      AdmissionController
src/distributed/lifecycle.rs                      TaskRegistry, cancellation, sweeper
src/distributed/metrics.rs                        ExchangeMetrics, StageMetrics
src/distributed/server.rs                         control + data HTTP services
```

### 3.3 Memory: the hard blocker, do this first

VERIFIED at `memory.rs:97`:

```rust
pub struct MemoryReservation<'a> { pool: &'a MemoryPool, size: usize }
```

A **borrow**. It cannot live in a task struct or move across `tokio::spawn`. Until this is fixed, every shuffle buffer, credit budget and retained page is memory the pool does not know about — the distributed component would be the one allocation class outside the no-OOM invariant.

```rust
// src/execution/memory.rs — ADD, do not replace
pub struct OwnedMemoryReservation { pool: SharedMemoryPool, size: usize }

impl MemoryPool {
    pub fn reserve_owned(self: &Arc<Self>, size: usize) -> Result<OwnedMemoryReservation>;
}
impl OwnedMemoryReservation {
    pub fn size(&self) -> usize;
    pub fn try_grow(&mut self, delta: usize) -> Result<()>;
    pub fn shrink(&mut self, delta: usize);
}
impl Drop for OwnedMemoryReservation { /* pool.release(self.size) */ }
```

`SharedMemoryPool = Arc<MemoryPool>` already exists at `memory.rs:~130`, so this is purely additive. ~1 day.

### 3.4 Partitioning properties

```rust
// src/physical/partitioning.rs  (NEW)
#[derive(Debug, Clone, PartialEq)]
pub enum Partitioning {
    Single,
    RoundRobin(usize),
    Hash { keys: Vec<Expr>, buckets: u32, partitions: usize, salt: u64 },
    Broadcast(usize),
    Unknown(usize),
}
impl Partitioning { pub fn count(&self) -> usize { /* ... */ } }

#[derive(Debug, Clone, PartialEq)]
pub enum Distribution {
    Unspecified,
    SinglePartition,
    HashPartitioned(Vec<Expr>),
    Broadcast,
}

/// `for_join` encodes the one rule everybody gets wrong.
pub fn satisfies(p: &Partitioning, req: &Distribution, for_join: bool) -> bool;
```

**The subset rule, stated exactly — do not blanket-disable it, and do not blanket-allow it.**

- For an **aggregation**: `Hash([a])` **DOES** satisfy `HashPartitioned([a, b])`. Rows sharing `(a,b)` necessarily share `a`, so they are already co-located and the group-by is correct with no extra shuffle. *This is the Q03 elision and it is worth an entire shuffle round.*
- For a **partitioned join**: `Hash([a])` **DOES NOT** satisfy `HashPartitioned([a, b])`. The other side is hashed over two columns; the two functions disagree and matches are **silently lost**.
- Stronger statement of the join requirement: both children must be partitioned by the **identical expression list, same hash function, same salt, same bucket count**. Never test the two sides independently.

### 3.5 Trait additions

```rust
// src/physical/plan.rs — three defaulted, one required
pub trait PhysicalOperator: Debug + Send + Sync {
    fn schema(&self) -> SchemaRef;
    fn children(&self) -> Vec<Arc<dyn PhysicalOperator>>;
    async fn execute(&self, partition: usize) -> Result<RecordBatchStream>;
    fn name(&self) -> &str;

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::Unknown(1)
    }

    /// PROVIDED — delegates, so the two can never disagree.
    /// This structurally retires the hazard documented at hash_join.rs:1225-1245.
    fn output_partitions(&self) -> usize { self.output_partitioning().count() }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::Unspecified; self.children().len()]
    }

    fn with_new_children(&self, _c: Vec<Arc<dyn PhysicalOperator>>)
        -> Result<Arc<dyn PhysicalOperator>> {
        Err(QueryError::NotImplemented(format!("with_new_children for {}", self.name())))
    }
}
```

**Migration note (real work the inventory underprices).** There are **18** impl sites. `output_partitions()` becoming a provided method means every operator that currently *overrides* it must instead override `output_partitioning()` — mechanical, but touch all 18. `with_new_children` is implemented on the ~8 that can sit above an exchange: `FilterExec`, `ProjectExec`, `HashJoinExec`, `HashAggregateExec`, `MorselAggregateExec`, `SortExec`, `LimitExec`, `UnionExec`.

### 3.6 ExchangeExec

```rust
// src/physical/operators/exchange.rs  (NEW)
pub enum ExchangeKind { Local, Remote { stage_id: StageId } }

pub struct ExchangeExec {
    input: Arc<dyn PhysicalOperator>,
    partitioning: Partitioning,
    kind: ExchangeKind,
    /// Set by the planner whenever the CONSUMER is an outer/anti/full join.
    replicate_nulls_and_any: bool,
    pool: SharedMemoryPool,
    metrics: Arc<ExchangeMetrics>,
    state: OnceCell<Arc<ExchangeState>>,
}
```

`execute(p)` returns the receive end of output channel `p`; the first call lazily spawns one producer task per **input** partition behind the `OnceCell`.

**The non-obvious correctness requirement.** Per-output-partition **bounded** channels deadlock: consumer A blocks on partition 0, the producer blocks writing partition 1, and partition 1's consumer is downstream of A. The correct invariant is a **global Gate** — a sender returns `Poll::Pending` only when **all** output channels are non-empty. VERIFIED present in the local checkout: `.scratch/datafusion/datafusion/physical-plan/src/repartition/distributor_channels.rs`, 855 lines, Apache-2.0, documented at its lines 36-38, implemented as `Gate { empty_channels: AtomicUsize, send_wakers: Mutex<...> }`. It depends only on `parking_lot` (already a dep) and `std` futures. Port it.

**`replicate_nulls_and_any`** — one day of work standing between us and a silent wrong-answer class that only appears *after* you distribute. Under hash partitioning a NULL key lands in one arbitrary partition, so an outer join's unmatched-side bookkeeping on every *other* partition is wrong; and replicating one arbitrary row lets a partition that received zero rows still emit its outer nulls. Must ship in the same commit as the first partitioned outer join.

### 3.7 The partitioner — the dominant new per-byte cost

**Explicit anti-pattern, already in our tree, do not copy it** (`spillable.rs:1832-1838`, VERIFIED):

```rust
for row in 0..batch.num_rows() {                      // row-at-a-time
    let key = extract_join_key(&key_arrays, row);
    let mut hasher = DefaultHashBuilder::default().build_hasher();
    key.hash(&mut hasher);
    let partition = (hasher.finish() as usize) % num_partitions;   // true modulo
    partition_indices[partition].push(row);
}
```

**Mandated shape:**

```rust
// src/physical/exchange/partitioner.rs  (NEW)
pub fn partition_batch(
    batch: &RecordBatch, keys: &[ArrayRef], parts_pow2: usize, salt: u64,
) -> Result<Vec<RecordBatch>> {
    // 1. ONE hash pass                      -> Vec<u64>
    // 2. part = ((h >> 32) as usize) & (parts_pow2 - 1)   (high bits, after avalanche)
    // 3. histogram -> prefix sums -> counting-sort row ids into ONE Vec<u32>
    // 4. ONE arrow::compute::take producing a grouped batch
    // 5. hand out zero-copy batch.slice(offset, len) per partition
}
```

This matters because the naive shape **degrades with partition count** — a genuinely superlinear-in-N term. Reported first-party on this box (**UNVERIFIED-BY-ME**): 3.11 GB/s/core at P=4 falling to 1.30 at P=32, i.e. more than 2x the cost of the IPC serialization it feeds. Planner corollary: **keep the network exchange width at N** and do fine-grained repartitioning locally on the receiving side, where there is no serialization.

Must land in the *first* `ExchangeExec` commit; retrofitting means re-benchmarking everything.

### 3.8 Hashing — two verified latent catastrophes

VERIFIED `vectorized_hash.rs:94-97`:

```rust
fn combine_hash(seed: u64, value: u64) -> u64 {
    seed.wrapping_mul(0x9e3779b97f4a7c15).wrapping_add(value)   // multiply then ADD
}
```

The value enters the **low** bits unmixed, so with the fixed seed the **high** bits are near-constant for a single dense key column. Any `h >> 32` or `h >> 56` bucketing degenerates toward one bucket.

VERIFIED `vectorized_hash.rs:89`:

```rust
    // Unknown types: hash stays unchanged (will match on equality check)
```

Every row of an unsupported key type hashes **identically**. Single-node that is merely catastrophically skewed; **across processes on a partitioned join it silently loses matches**, because the two sides disagree on partition assignment and nothing errors.

```rust
#[inline(always)]
fn finalize_hash(mut h: u64) -> u64 {           // murmur3 fmix64
    h ^= h >> 33; h = h.wrapping_mul(0xff51afd7ed558ccd);
    h ^= h >> 33; h = h.wrapping_mul(0xc4ceb9fe1a85ec53);
    h ^= h >> 33; h
}
pub fn hash_arrays_salted(keys: &[ArrayRef], rows: usize, salt: u64) -> Vec<u64>;
pub const SALT_REMOTE: u64 = 0x9E3779B97F4A7C15;
pub const SALT_LOCAL:  u64 = 0xC2B2AE3D27D4EB4F;
pub fn can_vectorize_arrays(keys: &[ArrayRef]) -> Result<()>;  // HARD GATE
```

Three requirements:

1. **Preserve the existing seed** `0x517cc1b727220a95` (`vectorized_hash.rs:11`) — cross-process determinism already exists there and is load-bearing.
2. **Two independent salts are mandatory.** Remote shuffle uses `SALT_REMOTE`; post-shuffle local repartitioning uses `SALT_LOCAL`. Skip this and each node's local partitions correlate perfectly with node id, leaving `(N−1)/N` of them empty. This is the most commonly missed bug in two-level shuffles.
3. **`can_vectorize_arrays` becomes a hard gate.** An unsupported key type forces `Distribution::SinglePartition` (an explicit gather) at plan time, never a silent mis-partition.

### 3.9 TableProvider splits

VERIFIED `scan.rs:48` — `fn scan(&self, projection) -> Result<Vec<RecordBatch>>` fuses enumeration with reading and materializes the whole table locally.

```rust
pub struct Split {
    pub id: SplitId,
    pub source: SplitSource,          // Parquet { path, row_group } | LanceFragment { id }
    pub byte_size: u64,
    pub row_count: u64,
    pub stats: Option<SplitStats>,
}

pub trait TableProvider: Send + Sync + fmt::Debug {
    // ... existing methods unchanged ...

    fn splits(&self, projection: Option<&[usize]>, filter: Option<&Expr>) -> Result<Vec<Split>> {
        Ok(vec![Split::whole_table()])          // default: one split
    }
    fn scan_split(&self, split: &Split, projection: Option<&[usize]>) -> Result<RecordBatchStream> {
        /* default: delegate to scan() */
    }
    /// Returns None today. Present so the CBO can cost a co-located join at net=0 later
    /// without a trait break, once a bucketed write path exists.
    fn output_partitioning(&self) -> Option<Partitioning> { None }
}
```

Defaults delegate to today's `scan()`, so `MemoryTable`, `IcebergTable` and the Lance provider are untouched. Parquet row groups and Lance fragments already *are* the split unit.

### 3.10 Aggregate modes

```rust
pub enum AggregateMode { Partial, Intermediate, Final, FinalPartitioned, Single }

pub trait AggStateCodec {
    fn intermediate_schema(aggs: &[AggregateExpr]) -> SchemaRef;
    fn encode(states: &[AccumulatorState], out: &mut MutableArrayData) -> Result<()>;
    fn decode(batch: &RecordBatch, col: usize) -> Result<Vec<AccumulatorState>>;
}
```

Half the work already exists and is battle-tested. The real work is the intermediate-state **schema**:

| Function | Intermediate state |
|---|---|
| Count / Sum / Min / Max | the value itself |
| Sum(f64) | `(f64::to_bits, saw_non_null: bool)` |
| Avg | `(sum: u64 bits, count: i64)` |
| Stddev / Variance | Welford triple `(count, mean bits, m2 bits)` — already tracked |

**Hard invariant: serialize the RAW state via `f64::to_bits()`, never a re-derived value.** Re-deriving drifts Q01/stddev in the last ulp and the cell-exact suite goes red weeks later.

**Two latent linear-scaling bugs to fix here.** VERIFIED: `hash_agg.rs:1932` implements `ApproxDistinct` as an **exact `std::collections::HashSet`** (the comment says "For SIMD path, just use exact count distinct"), and `:1925` materializes and sorts a full `Vec<f64>` for `ApproxPercentile`. Their partial state is **unbounded**, so they cannot merge across nodes and force the shuffle to `O(input)`. Switching to HLL / t-digest **changes results** — `tests/function_validation_tests.rs` asserts exact values for both, so regenerate those expectations deliberately in the same commit or do not ship the flip.

### 3.11 Transport

```rust
// src/distributed/transport/mod.rs
#[derive(Hash, Eq, PartialEq, Clone, Copy)]
pub struct MailboxId {
    pub query: QueryId, pub epoch: u32,
    pub send_stage: StageId, pub send_task: u16,
    pub recv_stage: StageId, pub recv_part: u16,
}

pub struct ShuffleResponse {
    pub pages: Vec<Page>,
    pub token: u64,
    pub next_token: u64,
    pub at_end: bool,
    pub remaining_bytes: u64,   // NOT optional — see below
}

#[async_trait]
pub trait ShuffleSource: Send + Sync {
    async fn request(&self, mb: MailboxId, token: u64, max_bytes: usize, max_wait: Duration)
        -> Result<ShuffleResponse>;
    fn is_local(&self) -> bool;
}

pub enum Page { Local(Arc<RecordBatch>), Encoded(Bytes) }
```

`MailboxId` is computed **identically on both ends from the plan**, so 1000 mailboxes cost **zero** rendezvous round trips. `remaining_bytes` is what lets the consumer size its next request; without it you either over-request (memory blowup) or under-request (latency stalls).

`InprocTransport` is **the default**: a `DashMap<MailboxId, Arc<InprocMailbox>>` passing `Arc<RecordBatch>` with no serialization. Because it is the default, the entire existing 837-900-test suite exercises the distributed operator graph for free, with no daemons, on every `cargo test`. This is the whole reason to build a trait instead of hard-wiring TCP.

### 3.12 Wire format and the cost argument

Frame: `[u32 len][PageHeader ~40B][Arrow IPC RecordBatch body]`. Schema is sent **once per mailbox stream** as a `StreamStart` frame, never per page.

1. **Arrow IPC write is memcpy plus 8-byte buffer alignment padding, not a row transposition.** Our data is Arrow end to end, so we get the columnar wire path essentially free. Velox/Presto pay a row-oriented Presto serde on top of columnar execution; we structurally do not.
2. **Serialization is not the dominant new cost — the partitioner is** (Section 3.7). Reported IPC figures on this box (**UNVERIFIED-BY-ME**): serialize 3.08 GB/s/core, deserialize 14.8 GB/s/core, loopback TCP 12.4 GB/s.
3. **Compression is runtime config, default OFF.** Reported: LZ4 1.81 GB/s at 0.566 wire ratio, ZSTD 1.13 GB/s at 0.411. Against loopback (12.4 GB/s) and 100 GbE compression is a pure **loss**; against 10/25 GbE it is a clear **win**. The decision *inverts* between this box and production, so **the single-box benchmark must never be used to set the production default**. Apply Trino's rule: compress speculatively, keep the compressed form only if it saved ≥ 20%.
4. **Two risks that are measurement gates, not assumptions.** arrow-rs `StreamReader<R: Read>` copies into a `Vec` — **UNVERIFIED** whether it approaches pyarrow's decode rate; mitigate with `StreamDecoder` over a pre-aligned 64-byte `MutableBuffer` and **measure before believing any projection**. Separately, we have dictionary-aware string paths: **IPC dictionary batches must be emitted once per stream with stable ids**, or dictionary-encoded string columns inflate the wire dramatically.

**Connections.** A hash shuffle between two P-partition stages across N nodes has `N^2 x P` logical streams — at N=8, P=32 that is 2048, i.e. 256 sockets/node/query if naively mapped. One long-lived multiplexed connection **per peer**, built at startup from `--peers`, makes it **N−1 sockets per node total**, amortized across every query forever. Local destinations bypass the network stack and pass `Arc<RecordBatch>`.

**The tax you pay for multiplexing:** transport flow control is per-*peer* and shared across all streams, so one slow consumer on one query can head-of-line-block every other query to that peer. Therefore application credit must be the **only** real flow control, and the transport window must be sized **above** aggregate application credit (rule of thumb: 4x `max_queued_bytes`). If the two loops fight you get sawtooth throughput that is hard to diagnose and easy to blame on the wrong layer.

### 3.13 Credit, buffering, spill

```rust
// src/distributed/credit.rs
pub struct CreditController {
    budget: OwnedMemoryReservation,   // ONE global budget across ALL sources of a ShuffleReadExec
    max_queued: usize, queued: usize, in_flight: usize,
}
```

With a **per-source** buffer, a consumer talking to 64 producers holds `64 x buffer` bytes **per stage** (at 32 MB that is 2 GB). With one global budget, memory is **flat in N** — the precondition for adding nodes without retuning. `in_flight` must be counted or the budget is a lie.

**Oversized-page escape hatch, separately essential.** If `queued == 0 && in_flight == 0` and the only available page exceeds the entire budget, issue an out-of-band oversized request (via `try_grow`, spilling other consumers if needed). Without it the query **hangs forever**, pinning its reservation until timeout — strictly worse than being slow.

```rust
// src/distributed/sink_buffer.rs
pub struct SinkBuffer {
    pages: VecDeque<(u64 /*token*/, Bytes)>,
    acked_through: u64,
    reservation: OwnedMemoryReservation,
    spill: Option<IpcSpillFile>,
}
```

Acknowledge-on-next-request: fetching token `T+1` implicitly acks everything ≤ `T`. A dropped connection is recovered by re-requesting the same token — no query failure, no duplicate rows. Without it, `P(query fails) = 1 − (1−p)^N`: reliability *decreases* as you scale, destroying effective throughput even at nominal linearity. **Put the sequence field in the wire protocol from day one** even if retry ships later; retrofitting it is a breaking change across every node. Two watermarks (block at `max_size`, unblock at `max_size/2`) or producers thrash at the boundary.

**Shuffle spill goes to Arrow IPC files, not Parquet.** VERIFIED three `ArrowWriter::try_new` sites at `spillable.rs:1738`, `:1876`, `:1912`. Already-serialized IPC bytes append **verbatim** — essentially a memcpy into page cache. Parquet encode is an order of magnitude more expensive and we need neither column pruning nor statistics for a transient buffer. Reuse `ExecutionConfig::spill_path` and the existing `SPILL_COUNTER` atomic that already prevents concurrent-operator directory collisions.

---

## 4. Stage planning rules and partitioning propagation

### 4.1 EnforceDistribution

**Strip-then-reinsert, always.** Insert-only accumulates exchanges across repeated optimizer passes.

```
1. Recursively STRIP every existing ExchangeExec.
2. Top-down, for each node and each child i:
     if satisfies(child.output_partitioning(), node.required_input_distribution()[i], is_join)
        → leave alone
     else → wrap child in ExchangeExec(required partitioning)
3. Mark each exchange Local or Remote by whether its producers and consumers
   are placed on the same node group.
4. Set replicate_nulls_and_any on any exchange whose consumer is Outer/Anti/Full.
```

**Non-negotiable CI gate:** with `cluster_size = 1`, the emitted physical plan's structural hash must be **byte-identical** to today's. Without the strip-then-reinsert elide pass, single-node regresses and the project fails on its own terms.

### 4.2 Partitioning propagation

| Operator | `output_partitioning()` | `required_input_distribution()` |
|---|---|---|
| Scan (streaming parquet) | `Unknown(partitioned_work.len())`, or the provider's if bucketed | — |
| Filter, Project | passthrough of child | `[Unspecified]` |
| HashJoin (partitioned) | child's `Hash`, keys rewritten to output columns | `[HashPartitioned(left_keys), HashPartitioned(right_keys)]` |
| HashJoin (broadcast) | probe side's partitioning | `[Broadcast, Unspecified]` |
| Aggregate `Partial` | passthrough | `[Unspecified]` |
| Aggregate `FinalPartitioned` | `Hash(group_keys, n)` | `[HashPartitioned(group_keys)]` |
| Sort (global) | `Single` | `[SinglePartition]` |
| Sort (per-partition top-K) | passthrough | `[Unspecified]` |
| Limit (global) | `Single` | `[SinglePartition]` |
| Union | `Unknown(sum of children)` | `[Unspecified; n]` |

### 4.3 The 256-bucket indirection — free now, impossible to retrofit

```rust
pub const SHUFFLE_BUCKETS: u32 = 256;              // power of two, INDEPENDENT of N
pub struct BucketMap { pub owner: [NodeId; 256] }  // 256 bytes on the wire
```

`bucket = ((h >> 32) as u32) & 255`; `node = bucket_map.owner[bucket]`, default `bucket % N`.

Consequences: adding a node changes one 256-byte table, never the hash function and never the data on disk; a hot bucket can be split across consumers without changing anything; the cluster is resizable. And the speed corollary: **keep the network exchange width at N** (one mailbox per destination node, buckets coalesced) and do fine-grained repartitioning **locally** on the receiving side with `SALT_LOCAL`, where it costs no serialization.

### 4.4 DeterminePartitionCount — fix the block size, not the partition count

Mean shuffle block is `S/(M x R)`. At fixed data volume, **doubling the cluster quarters the average block**, and the fixed per-block cost (RPC header, IPC schema/metadata) goes from negligible to dominant.

```
P = clamp(ceil(S / 256KB), N, 8N)     // max of byte-derived and row-derived estimates
```

Plus a sender-side coalescer with a flush deadline, so we never emit a schema header per 8K-row batch. This is also what keeps **small-query latency flat** as nodes are added — a 1 MB query on a 100-node cluster must not create 100 tasks; use `StageParallelism::Subset(k)` with `k = ceil(bytes / min_bytes_per_node)`.

**Treat any run whose mean frame is < 64 KB as a FAILED configuration**, not a slow one.

### 4.5 PlanFragmenter

```rust
pub struct PlanFragment {
    pub stage_id: StageId,
    pub root: Arc<dyn PhysicalOperator>,            // rooted at ShuffleWriteExec
    pub inputs: Vec<StageId>,
    pub distribution: StageDistribution,            // Source{table} | FixedHash{buckets} | Single
    pub parallelism: StageParallelism,              // AllNodes | Subset(k) | InitiatorOnly
    pub bucket_map: BucketMap,
}
```

Walk the tree; at each `ExchangeExec { kind: Remote }`, replace the node with `ShuffleReadExec { stage_id }` in the parent and emit a fragment rooted at `ShuffleWriteExec`. A `Remote` exchange never survives fragmentation — it is purely a marker, which keeps the operator set small. **This file is where all node-awareness lives**; that separation is what makes the work incrementally shippable.

### 4.6 Hard planner gates (correctness, not performance)

Two constructs **cannot** run distributed and must be forced to `InitiatorOnly`, surfaced in `EXPLAIN` as `stage: single-node (correlated subquery)` rather than silently producing a one-node plan:

- **`SubqueryExecutor`** (`src/physical/operators/subquery.rs`) executes row-by-row against a *local* table registry, substituting outer values per row. Running it shard-local over partial data is **silently wrong**, not slow.
- **`DelimJoinExec` / `DelimGetExec`** (`delim_join.rs:145`, `:244`) materialize everything and return one partition.

TPC-H Q4/Q17/Q20/Q21/Q22 are decorrelated by existing rules and are fine. Anything that survives decorrelation becomes an effectively single-node query. `FlattenDependentJoin` exists (`src/optimizer/rules/flatten_dependent_join.rs`) but is disabled pending column-resolution fixes; lifting this ceiling is out of scope for v1.

### 4.7 Distribution in the CBO

`join_reorder.rs` already costs from footer statistics (`TableStatistics` / `ColumnStatistics` at `scan.rs:13-40` with `ndv_est`) but is a pure `C_out` model with **no exchange term**. Widen the DP entry cost from a scalar to `(cpu, net, mem)`, add `cluster_size` to the optimizer context, and enumerate `{Broadcast, Partitioned}` per join (2x branching on DPsize, fine at n ≤ 12).

**Crossover: broadcast wins iff `B x (N−1) < P`**, with a hard side condition that `B` must fit the per-node budget after admission — a broadcast that forces the build side to spill is never a win.

This must land **before** any distributed benchmark. Broadcast is the one plan shape whose cost *grows* with cluster size (`B x (N−1)` network, `B x (N−1)` CPU, full build side on every node regardless of N — an Amdahl term `σ ≈ B/(P+B)` that never shrinks). Skipping it means every later measurement is dominated by a plan bug rather than the mechanism under test.

Known weakness to guard with a golden plan test: our DPsize uses **max-of-per-column-NDV** for composite keys, which *under*estimates composite NDV and could push the CBO toward broadcasting an 8 M-row `partsupp` in Q09.

**Accepted consequence:** the optimal plan is now a function of cluster size, so plans cannot be cached across resizes. Key plan caches on a topology version.

---

## 5. Walkthroughs on 4 nodes

Setup: workers `n0..n3`, client hits `n0` which becomes the initiator. 256 buckets, `owner[b] = b % 4`. Splits assigned by rule `xxhash64(path, rg_idx) % 4 == my_id`. Remote exchange width 4; each receiver then does a **local** exchange to `num_cpus` partitions with `SALT_LOCAL`. Byte figures are estimates from SF=10 cardinalities and current column widths, **not measurements** — re-derive them from the P5 `shuffle_bytes` counter before making any plan decision on them.

### 5.1 Q01 — scan, filter, GROUP BY (2 keys, 4 groups), ORDER BY

**Stage 1** (`AllNodes`): `StreamingParquetScan(lineitem, ~1/4 of row groups)` → `Filter(l_shipdate <= date − 90d)` → `MorselAggregate[Partial]` on `(l_returnflag, l_linestatus)` → `ShuffleWrite(XCHG-H1: Hash([l_returnflag, l_linestatus]))`.

The Partial aggregate collapses ~15 M local rows to **4 rows** before anything touches the wire. Encoded states for 4 groups x ~9 aggregates ≈ **a few hundred bytes per node**; ~3 KB total on the wire against ~4 GB scanned. Ratio ≈ 10⁻⁶.

**Stage 0** (`InitiatorOnly`): `ShuffleRead` → `HashAggregate[Final]` → `Sort` → stream to client.

**Honest note for the report: Q01 barely uses the shuffle.** It will scale beautifully and prove almost nothing about the exchange. Its real value is proving (a) the Partial/Final split is bit-exact against DuckDB and (b) `coordinator_cpu_ms_per_query` is flat as SF grows. Always report `shuffle_bytes` alongside so nobody mistakes it for a shuffle benchmark. Corollary for P6: because Q01's shuffle is kilobytes, any Q01 distribution tax below 0.95 is **fixed per-query overhead** (dispatch, poll, gather), not shuffle, and must be chased there.

### 5.2 Q03 — customer ⋈ orders ⋈ lineitem, GROUP BY 3, ORDER BY revenue DESC LIMIT 10

CBO at `cluster_size = 4`:
- `customer` after `c_mktsegment='BUILDING'` ≈ 300 K rows ≈ 3.6 MB projected. `B x 3 = 10.8 MB` vs orders probe ≈ 30 MB → **BROADCAST**.
- `orders ⋈ lineitem`: both large, and with the `mem` term counted (broadcasting 7.5 M rows to every node risks a spilling build side) → **PARTITIONED**.

**Stage 4** (customer): scan + filter → `ShuffleWrite(XCHG-B: Broadcast)`. Publishes runtime filter RF-1 on `c_custkey`.

**Stage 3** (orders): scan (RF-1 prunes row groups) + `Filter(o_orderdate < '1995-03-15')` → `HashJoin(build = broadcast customer)` → `ShuffleWrite(XCHG-H1: Hash([o_orderkey]))`. Publishes RF-2 on `o_orderkey`.

*Ordering is the whole game:* the broadcast join runs **before** the hash shuffle, so only surviving orders rows are repartitioned. Shuffle volume is a property of plan order, not of the exchange.

**Stage 2** (lineitem): scan (RF-2 prunes row groups) + `Filter(l_shipdate > date)` → `ShuffleWrite(XCHG-H2: Hash([l_orderkey]))`. Without RF-2, ~30 M surviving rows x 24 B x 3/4 ≈ 540 MB; with it, ~4 M rows ≈ 72 MB. **The runtime filter cuts total shuffle roughly 5x** — and it does so by eliminating rows *before* they are repartitioned, which is why runtime filters beat every other mechanism here.

**Stage 1** (`FixedHash` on orderkey): `ShuffleRead(H1)` + `ShuffleRead(H2)` → local exchange (`SALT_LOCAL`) → `HashJoin(o_orderkey = l_orderkey)` — **co-partitioned, no further shuffle** → aggregate.

> **THE ELISION.** The aggregate's group key `(l_orderkey, o_orderdate, o_shippriority)` is a **superset** of the partition key `l_orderkey`. Rows sharing the 3-tuple necessarily share `l_orderkey`, so they are already co-located: `EnforceDistribution` **elides the aggregate's exchange entirely** and runs it in `Single` mode with no Partial/Final split. This is the aggregation-side subset rule (§3.4) and it is exactly the case that would be *illegal* if this were a join on the 3-tuple. `Partitioning::Hash(exprs, n)` can express it; `output_partitions() -> usize` cannot.

Then `SortExec::with_fetch(10)` per task → `ShuffleWrite(XCHG-G: ordered gather)`.

**Stage 0**: k-way merge of 4 x 10 = **40 rows** → LIMIT 10 → stream. The initiator touches 40 rows, not ~11 M.

### 5.3 Q09 — 6-way join, GROUP BY (nation, year), ~175 output rows

The real scaling test: heaviest shuffle volume of the 22, three sequential shuffle rounds, and the query where we already sit at **1.0x DuckDB single-node** — so any distribution tax shows up here first and cannot hide behind an existing gap.

CBO at N=4: `nation` (25 rows) broadcast; `supplier` (100 K, 1.6 MB) broadcast; `part` after `p_name LIKE 'Part 1%'` (~222 K of 2 M partkeys) broadcast.

- **Stage 6** (part): scan + LIKE → `Broadcast(B1)`. Publishes RF-1 on `p_partkey` — note this is precisely the 222 K-key case the existing `RuntimeFilterPayload::Bitmap` was tuned for (`streaming_parquet_scan.rs:31-64`, documented ~30x cheaper than a HashSet probed 60 M times).
- **Stage 5** (supplier ⋈ nation): → `Broadcast(B2)`.
- **Stage 4** (partsupp, RF-1 pruned): → `Hash([ps_partkey, ps_suppkey])` = **H1**.
- **Stage 3** (lineitem, RF-1 pruned): scan → `HashJoin(broadcast part)` → `HashJoin(broadcast supplier x nation)` → `Hash([l_partkey, l_suppkey])` = **H2**. Both broadcast joins applied **before** the hash shuffle.
- **Stage 2** (`FixedHash` on the composite key): `ShuffleRead(H1)` + `ShuffleRead(H2)` → local exchange → `HashJoin` → `Hash([l_orderkey])` = **H3**. Publishes RF-2 on `l_orderkey`.

> **The one unavoidable second shuffle.** The join key changes from `(partkey, suppkey)` to `orderkey`; no single partitioning serves both. Two correctness notes: the exchange must hash **both** key columns, and `Hash([partkey])` must **NOT** be treated as satisfying `HashPartitioned([partkey, suppkey])` for this join — that is the subset trap and it is the same silent-row-loss class already documented at `hash_join.rs:1225-1245`.

- **Stage 1b** (orders, RF-2 pruned): → `Hash([o_orderkey])` = **H4**. The CBO compares this against broadcasting orders (15 M x 12 B x 3 ≈ **540 MB**) and picks the hash shuffle. *This is exactly the decision the `cluster_size` cost term exists to make;* getting it wrong is a 540 MB plan bug that would swamp every other measurement.
- **Stage 1**: `ShuffleRead(H3)` + `ShuffleRead(H4)` → `HashJoin` → `HashAggregate[Partial]` on `(n_name, year(o_orderdate))` → 175 groups/node → `Hash([nation, year])` = **H5**, ~700 rows total. Practically free — the `AggregateMode` lever doing its job.
- **Stage 0**: `Final` → `Sort` → 175 rows.

Estimated total: broadcasts ~74 MB + H2 ~201 MB + H4 ~135 MB ≈ **410 MB per query at SF=10**. Set this as the CI shuffle-bytes budget: it is hardware-independent, and a regression means a plan-order or runtime-filter regression, not a transport one.

> **The Q09 landmine, and it is not a distribution problem.** VERIFIED `hash_join.rs:1222` ends with `Ok(Box::pin(stream::iter(result.into_iter().map(Ok))))` — the probe **materializes the entire result** before returning a "stream", and `MorselAggregateExec` does the same. `execute()` is a **barrier, not a pipeline**. Q09 has three shuffle rounds; with barriers those stage times **add serially**, and we would measure S barriers and confidently conclude the transport design is bad when it is fine. **P2.5 fixes this before any distributed benchmark is interpreted.**

---

## 6. The scaling model

### 6.1 Definitions — pick one before writing code

Three claims get conflated; only two are honestly testable here.

- **Throughput scaling (the acceptance criterion): aggregate QPS at fixed per-query data.** This does *not* come from spreading one query wider. A fan-out-`S` query's p99 approximates the per-server `p(1 − 0.01/S)` quantile — at S=100 that is per-server p99.99 — so wider fan-out *raises* tail latency and per-RPC fixed cost. QPS scales by adding **replica groups**: `R` groups of `S` servers gives `QPS ≈ R x QPS(one group)`, linear in total nodes at **constant** per-query latency, with **zero cross-node traffic by construction**.
- **Weak scaling:** data grows ∝ N, per-query latency flat. What the exchange machinery targets.
- **Strong scaling:** fixed data, N grows, latency drops. **Cannot be honestly measured here** — K processes share one memory controller and one LLC, so the curve measures contention, not scale-out. `eta_box(2) = 0.614` proves it.

### 6.2 The formula

Let `W1` = useful core-seconds per query on one node; `c` = cores per node; `D` = bytes that must move; `B` = broadcast build bytes; `P` = probe bytes; `ρ` = skew ratio (max/mean partition bytes); `η_box` = shared-hardware contention (1.0 on real nodes).

**Per-query effective core-seconds:**

```
W_eff(N) = W1                                   useful work, constant
         + C_coord                              initiator: O(stages·N + N·K), NOT O(rows|splits|result)
         + D·(N−1)/N · (1/B_part(P) + 1/B_ipc)  partition + serialize; bounded by D
         + B·(N−1)·k_b                          BROADCAST — the only term linear in N
         + S·T_barrier                          materializing operators (P2.5 kills this)
```

**Throughput:**

```
QPS(N) = N · c / W_eff(N) · (1/ρ(N)) · η_box(N)
```

**Latency (weak scaling):**

```
T(N) ≈ T_fixed + ρ · W_par/(N·c) + S·(T_barrier + D/(N·c·B_net)) + σ·sqrt(2·ln N)
```

**Linearity holds iff `W_eff(N)` is constant in N and `ρ(N)` is bounded.** Every term above has a named owner:

| # | Term that breaks linearity | Mechanism that kills it | Section |
|---|---|---|---|
| 1 | Coordinator `O(result)` — VERIFIED `context.rs:325-357` `try_collect()`s every partition into one Vec before the caller sees a row | streaming result API + top-K pushdown + k-way gather → `O(stages·N + N·K)` | 3.x, P7 |
| 2 | Coordinator `O(splits)` per query | ship an assignment **rule**, not a list; `RoutingCatalog` at registration time | 2.1 |
| 3 | Control-plane `1/F` ceiling from a dedicated coordinator | **per-query initiator election** | 1.3 |
| 4 | GROUP BY shuffles `O(rows)` | `AggregateMode::Partial` → `O(groups/node)`; Q01: 60 M rows → 4 | 3.10 |
| 5 | **Broadcast: `B·(N−1)` — the only structurally superlinear term.** Amdahl `σ ≈ B/(P+B)` that never shrinks | CBO rule `broadcast iff B·(N−1) < P`, with `mem` counted | 4.7 |
| 6 | Join shuffle volume | broadcast-join-**before**-hash-shuffle plan ordering + distributed runtime filters | 5.2, 5.3 |
| 7 | `N^2` sockets: `N^2·P` logical streams (N=8,P=32 → 2048) | one multiplexed `PeerConn` per peer → **N−1 sockets/node total** | 3.12 |
| 8 | Mean block `S/(M·R)` — doubling N **quarters** the block | fix **block size ≥ 256 KB**, not partition count | 4.4 |
| 9 | Consumer memory `O(N producers)` | one **global** credit budget | 3.13 |
| 10 | Partitioner throughput **degrades with P** (3.11 → 1.30 GB/s/core) | counting sort + one grouped take; network width pinned to N | 3.7 |
| 11 | Skew: effective speedup `N/ρ` — **multiplicative** | 256 buckets ⇒ `P/N ≥ 8` up to N=32 for free (`ρ ≈ 1 + O(N/P)`: `P/N=1` costs ~40%, `P/N=8` costs ~12%) + greedy largest-first | 4.3, P7 |
| 12 | Stragglers: `E[max of N] ≈ μ + σ√(2 ln N)` — 10% variance costs ~20% at N=8 with **healthy** nodes | dynamic split pull with guided self-scheduling | P7 |
| 13 | `P(fail) = 1 − (1−p)^N` — reliability *decreases* with N | token/sequence buffer, ack-on-next-request | 3.13 |
| 14 | Concurrency thrash — morsel/rayon assume one query owns 32 cores | admission control **before** any QPS number is quoted | P1 |
| 15 | Serialized barriers (`hash_join.rs:1222`) — S stages measured as S barriers | P2.5 pipelining fix | 5.3 |
| 16 | Two fighting flow-control loops → sawtooth | transport window sized **above** aggregate application credit | 3.12 |

**The one mechanism that reduces work superlinearly:** distributed runtime filters. They cut probe I/O, probe CPU **and** shuffle bytes, because rows are eliminated *before* repartitioning — and unlike broadcast they add no constant-in-N term. The **degradation ladder is the scaling mechanism**: an uncapped distinct-value set unioned across N build tasks *grows* with N and turns the initiator into a memory bottleneck. Cap the size and degrade `exact set → bitmap → min/max → all-pass`, with a rendezvous timeout so a slow build cannot stall probe scans. Use the completed filter to skip **assigning splits**, not merely to filter rows.

### 6.3 Why the coordinator is provably not the bottleneck

1. **No `O(rows)` term** — workers are peer-to-peer; the initiator never routes a page. *Test: `coordinator_cpu_ms_per_query` flat across SF=1/10/100.*
2. **No `O(splits)` term** — assignment rule, not list; footer reads moved to registration.
3. **No `O(result)` term** — top-K pushed into every producer; the gather sees `N·K` rows.
4. **No singleton** — the role rotates per query, so control cost is `N/F`, not `1/F`.

---

## 7. Memory safety and spilling across nodes

CLAUDE.md's rule is absolute: **OOM is never acceptable; being slow on larger-than-memory data is fine.** The distributed layer must not become the one exception.

1. **Every distributed allocation is charged to the existing `MemoryPool`** via `OwnedMemoryReservation` (§3.3). This is why that is item zero: today `MemoryReservation<'a>` is a borrow and cannot cross `tokio::spawn`, so any shuffle buffer written before it lands is invisible to the budget.
2. **Consumer memory is `O(1)` in N** via one global credit budget (§3.13), not `O(N)` per-source buffers.
3. **Producer memory is bounded** by `SinkBuffer` watermarks; overflow spills to **Arrow IPC** files (verbatim byte append), never Parquet.
4. **Broadcast build sides are charged and demotable.** If a node cannot afford a broadcast build, it spills rather than failing — `SpillableHashJoinExec` already covers INNER. (Known hole, unchanged by this work: the join spill path supports INNER only; non-inner joins whose build side exceeds budget fail loudly rather than return wrong results.)
5. **Admission control is the *only* legitimate cluster memory role.** Bin-pack using `mem_free_bytes` in the heartbeat; refuse to *start* a stage on a node below a free-budget floor. **Never kill a running query** — Trino's `LowMemoryKiller` is exactly what our spillable operators exist to avoid.
6. **Leaked reservations are the silent killer.** If an initiator dies, its tasks hold reservations on every worker **forever**; under a no-OOM regime the effective pool shrinks **monotonically** until every query spills and cluster throughput collapses over hours in a way that looks exactly like a memory leak. `TaskRegistry` with `last_heartbeat`, a 5 s sweeper, and a TTL sweep over orphan exchange buffers releasing reservations on drop.
7. **Cancellation is a memory mechanism, not a nicety.** In a pull-based exchange, cancellation propagates *downstream* for free (the consumer stops requesting) but must be pushed *upstream* explicitly. Distinguish **cancel** (LIMIT satisfied — let sinks flush) from **abort** (failure — drop everything). Cancel the running work **first**, *then* wait for stats — it will not report until cancelled, and that ordering is the difference between a 100 ms and a 30 s cancel.
8. **The deadlock escape hatch is a memory-safety feature.** A hung query pins its reservation until timeout, which is strictly worse than a slow one.

---

## 8. Phased plan

Every phase is independently valuable, independently testable, and independently revertible. Phases P0–P4 ship **without a second process existing**.

### P0 — Foundations (~2 weeks). No distribution.

`OwnedMemoryReservation`; hash avalanche + salts + hard type gate; split-based `TableProvider`; `ExchangeMetrics` scaffolding into `QueryMetrics` (VERIFIED `context.rs:32-49` is query-level only — no per-stage breakdown exists).

**Accept:** chi-square uniformity on real TPC-H keys at 256 buckets over **both** low and high bits, `max/mean < 1.05`, `p > 0.01` — and the same test must **FAIL** against today's `combine_hash` on high-bit bucketing, proving the fix was load-bearing rather than cosmetic. `hash_arrays(k,n) == hash_arrays_salted(k,n,DEFAULT_SALT)` so no existing hash table changes shape. Unsupported key type returns `Err`, not the silent identity hash. Full suite green; `safe_benchmark.sh` SF=10 within ±3% of 7.4 s.

### P1 — Shared-nothing replica model (~3 weeks). **First linear-throughput result.**

`serve` mode; `ClusterTopology` + replica groups; split-to-group assignment as a pure function picking the **same candidate index for every split** (or co-location is silently lost); `AdmissionController`. **No exchange, no fragmenter, no transport.**

**Accept:** reproduce or beat `.scratch/proc_scaling.json` — aggregate QPS at K ∈ {1,2,4,8} processes x 32/K pinned cores must be ≥ 2.73 / 3.09 / 3.66 / 3.99, i.e. **≥ +46% at K=8 versus one 32-core process on the same 32 cores**. Per-query p50 flat in R; p99/p50 not degrading. Record `eta_box(K)` as the permanent control curve. Separately run the 8-way concurrency sweep with admission control **off** and show throughput turning over — that curve pre-empts misreading later distributed results as "distribution made it slower".

### P2 — Partitioning properties + local ExchangeExec (~3 weeks). **Must improve 7.4 s.**

`partitioning.rs`; trait additions across all 18 impls; `src/physical/optimizer/` with strip-then-reinsert `EnforceDistribution`; `ExchangeExec` + ported `distributor_channels` + counting-sort partitioner; `replicate_nulls_and_any`; per-partition byte metrics. Removes the four `output_partitions() == 1` funnels.

**Accept — primary:** re-run the exact core sweep that produced `.scratch/intranode_scaling.json`; efficiency at 32 cores must rise **from 0.177 to ≥ 0.25** (speedup ≥ 8.0x), directly reducing the measured `f = 0.150`. **Secondary:** `safe_benchmark.sh` SF=10 **strictly below 7.4 s**, gains concentrated in Q01/Q13/Q18/Q21. **Gate:** CI assertion that at `cluster_size = 1` the physical plan structural hash is byte-identical to pre-change. Partitioner microbench ≥ 3 GB/s/core at P=4 and ≥ 2 GB/s/core at P=32 (must not degrade the way the `spillable.rs` shape does). A 10,000-iteration deadlock test — consumer stalls partition 0 for 100 ms while others drain — must never hang, and must be **written to fail** against a naive per-channel bounded implementation. A partitioned LEFT/ANTI join with NULL keys and one zero-row partition must be cell-exact, **failing with `replicate_nulls_and_any = false`** and passing with it. All 22 + 156 cell-exact.

*If P2 is not a single-node win, the design is wrong and work should stop here.*

### P2.5 — Un-barrier the materializing operators (~1 week). **Prerequisite for interpreting anything.**

**Accept:** time-to-first-batch on Q03 drops from ≈ end-of-query to ≈ build-complete; peak RSS on Q09 at SF=10 drops measurably. No distributed benchmark may be quoted before this passes.

### P3 — AggregateMode with serializable state (~3 weeks).

**Accept:** property test — `decode(encode(s))` **bit-identical** for every variant including the Welford triple and the Sum saw-non-null flag; `merge(a,b)` after a round trip equals `merge(a,b)` without it. All 156 + 22 cell-exact with Partial/Final forced on, still single-node. Q01's final aggregate reports `output_partitions() > 1`. Simulated `cluster_size=4` shuffle bytes recorded for all 22 (Q01 must be kilobytes). Before flipping ApproxDistinct/ApproxPercentile, regenerate the exact-value expectations in `tests/function_validation_tests.rs` **in the same commit** or do not ship the flip.

### P4 — Distribution in DPsize (~1 week). Still no networking.

**Accept:** golden plan tests only. At N=4: Q03 broadcasts customer, hash-partitions orders–lineitem; Q09 broadcasts part/supplier/nation, hash-partitions on the composite key, does **not** broadcast partsupp or orders. At N=1 the plan is **identical to today's** for all 22. Sweep N ∈ {1,2,4,8,64} and assert every join's distribution choice is **monotone in N** (broadcast never becomes more attractive as N grows) — a cheap invariant that catches sign errors. Assert Q09's orders join flips broadcast→partitioned between N=2 and N=4.

### P5 — Fragmenter + inproc transport (~3 weeks). **Still one process. Highest leverage.**

**Accept — the payoff test:** the **entire existing suite** (837–900 tests, all 156 duckdb_validated) runs with `--cluster-size 4 --transport inproc` and passes, with no daemons, no ports, no processes. Cell-exact at K ∈ {1,2,4,8}. **Identity check:** summed `bytes_scanned` across simulated workers **equals** the single-node figure to row-group rounding (catches the classic silent split-assignment bug). Mean wire frame ≥ 64 KB on Q03/Q09. `shuffle_bytes` per query recorded as a CI budget with `projected_net_ms` tables for 10/25/100 GbE, labelled as projections. Plan-shape assertion for Q03: exactly one aggregate exchange elided.

### P6 — Real transport, first multi-process run (~4 weeks).

Arrow IPC codec (schema once per stream, dictionaries once with stable ids); `ConnectionPool` over hyper 1.11; `CreditController` + escape hatch; token/sequence `SinkBuffer`; IPC shuffle spill; cancellation + registry + sweeper; long-poll status with version header and `[T/2, T]` jitter.

**Accept — the headline: the paired-control distribution tax** (Section 9). `tau(8) ≥ 0.85` on the TPC-H mix, `tau(8) ≥ 0.95` on Q01. Supporting: cell-exact 22/22 and 156/156 at K ∈ {1,2,4,8} over **both** `inproc://` and `tcp://`, bit-for-bit agreement between transports; `shuffle_bytes` identical to P5 (proves the codec is transparent); sockets per node = K−1 regardless of concurrent query count; consumer RSS **flat** as K rises 2→8 at fixed data; injected consumer stall **plus** a page larger than the entire credit budget must **complete**, not hang; `cgroup memory.max` far below the working set gives cell-exact results with `spilled_bytes > 0` and no OOM; `kill -9` a worker mid-query and every survivor's `MemoryPool::used()` returns to its pre-query value within 5 s — **repeat 50x and assert no monotonic drift**; LIMIT query stops remote CPU within 200 ms. arrow-rs IPC decode throughput **measured, not assumed**. Compression A/B on loopback confirming it is a loss there, **with a written statement that this must not set the production default**.

### P7 — Filters, results, scheduling, lifecycle (~4 weeks).

**Accept:** Q03/Q09 `shuffle_bytes` and **splits assigned** with runtime filters on vs off must show a strict reduction (Q03 target ≈ 5x) — bytes, not wall time, because bytes project onto real hardware and loopback wall time does not. **Quantify stragglers before building stealing:** inject a CPU hog into one pinned cgroup, compare static assignment vs dynamic pull on Q09; three weeks is warranted only if the loss exceeds ~15%. Skew ratio `ρ` on a **deliberately skewed** TPC-H variant (Zipf on `l_orderkey` — stock TPC-H is too uniform to show anything) must stay below 1.3 with tier-1 greedy assignment. Initiator peak RSS independent of result size for a streaming consumer; LIMIT-10 on Q03 gathers 4x10 rows. `coordinator_cpu_ms_per_query` flat across SF=1/10/100 and linear-with-small-constant in N.

### P8 — Measurement-gated extras. Nothing scheduled.

(a) Two-level 256-bucket aggregation **only if** final merge > 15–20% of stage time (partly redundant with a plain hash exchange on the group key). (b) Skew tier 2 **only if** measured `ρ` > 1.5 after tier 1 — note Trino gates its own `SkewedPartitionRebalancer` to scaled-writer hash distribution and disables it under fault-tolerant execution, so it is not battle-tested even there. (c) Co-located zero-shuffle joins **only after** `generate-parquet --bucket-by <col> --buckets 256` exists; prove the payoff offline first (Q09 predicted 410 MB → 74 MB), then assert `net_bytes == 0`. (d) CTE/equivalent-stage dedup — a pure single-node win available today; CLAUDE.md already blames Q15's 9.0x gap on exactly this double scan.

---

## 9. The experiment (and what it cannot prove)

### 9.1 The paired-control distribution tax

All three input designs proposed measuring aggregate throughput at constant total cores and calling any drop "the distribution tax". **That measurement is invalid on this box**, and Section 0 proves it: running K independent processes with *no distribution layer at all* already loses ~50% of ideal throughput to shared LLC and memory bandwidth.

For each K ∈ {1,2,4,8}, run the identical TPC-H concurrent mix **twice**, at identical pinning (taskset + cgroup, 32/K cores/process), identical data, identical admission settings:

- **ARM S (control):** K processes, each a **full replica**, each query served entirely by one process. This is exactly the P1 configuration.
- **ARM D (distributed):** K processes, dataset sharded K ways, full stage DAG, real transport, every query fanned out to all K.

```
tau(K) = QPS_D(K) / QPS_S(K)
```

Because both arms run at identical K and cores-per-process on identical hardware, `eta_box(K)` appears in numerator and denominator and **cancels exactly**. `tau` is then a clean measurement of what the exchange, serialization, partitioner, credit protocol, scheduling and gather actually cost.

**Report verbatim:** *"distribution costs (1−tau)x100 percent of throughput at K=8."*
**Budget:** `tau(8) ≥ 0.85` on the mix; `tau(8) ≥ 0.95` on Q01 (whose shuffle is kilobytes, so anything lower there is fixed per-query overhead, not shuffle).

### 9.2 The falsifiable real-cluster prediction

On N genuinely separate machines `eta_box = 1` by construction, so:

```
QPS_pred(N) = N · q1(c) · tau(N) / rho(N)
```

`q1(c)` is single-process QPS at `c` cores measured here (2.706 at 32 cores); `tau` and `rho` are measured here. **`eta_box` is the only term this box cannot remove, and it is precisely the term a real cluster removes.** Publish this as a numbered prediction; it is testable the day real hardware exists.

### 9.3 Supporting measurements, all valid here

1. **Intra-node efficiency regression gate** (P2) — re-run the `intranode_scaling.json` sweep; efficiency at 32 cores from 0.177 → ≥ 0.25. The single most informative number in the plan, and it needs no distribution at all.
2. **Shuffle bytes per query**, asserted as a CI budget. Hardware-independent; `projected_net_ms = shuffle_bytes / (N x link_bw)`.
3. **No-duplicated-work identity** — summed `bytes_scanned` equals the single-node figure to row-group rounding.
4. **Mean wire frame ≥ 64 KB** (target 256 KB). Below is a *failed configuration*.
5. **Memory safety under pressure** — cell-exact **and** `spilled_bytes > 0` **and** no OOM kill.

### 9.4 What this hardware cannot prove — restate in every report

Network bandwidth saturation, TCP incast collapse, switch buffer exhaustion, real straggler tails, cross-rack RTT, `N^2` connection behaviour at N in the hundreds, and independent failure domains.

Loopback is roughly **4x the bandwidth and 6–25x lower latency** than a 25 GbE datacenter link, so every shuffle looks nearly free here and **hash-vs-broadcast decisions validated on loopback WILL be wrong on a real network**. That is why the CBO crossover takes link bandwidth as an explicit **config input**, never a loopback-tuned constant, and why the compression default is config-driven (the LZ4/ZSTD decision *inverts* between this box and 10/25 GbE production).

Partial mitigations, all of which must be labelled **SIMULATED** wherever their numbers appear: `tc qdisc netem` on `lo` for bandwidth and RTT; `cgroup io.max`; and a `--no-local-bypass` mode forcing co-located exchanges through the full serialization path so the codec cost is not silently elided.

And the hardest constraint: **never publish a bare latency-speedup curve from this box.** `eta_box(2) = 0.614` says it would be measuring contention, not scale-out.

---

## 10. Risk register

| # | Risk | Severity | Mitigation | Owner phase |
|---|---|---|---|---|
| 1 | Shuffle buffers allocate outside the memory pool; no-OOM invariant false in exactly the new code | **Critical** | `OwnedMemoryReservation` is item zero; nothing else merges before it | P0 |
| 2 | `combine_hash` multiply-then-ADD ⇒ high bits near-constant ⇒ bucketing degenerates | **Critical** | fmix64 avalanche + chi-square test that must fail on the old hash | P0 |
| 3 | Unsupported key type hashes identically ⇒ **silent lost matches** across processes | **Critical** | `can_vectorize_arrays` as a hard gate forcing a gather | P0 |
| 4 | Subset satisfaction applied to a partitioned join ⇒ silently lost rows | **Critical** | `satisfies(.., for_join)` with the two rules stated separately (§3.4) + a targeted test | P2 |
| 5 | NULL keys hash to one partition ⇒ wrong outer/anti results | **Critical** | `replicate_nulls_and_any`, same commit as the first partitioned outer join, with a test that fails without it | P2 |
| 6 | Per-output bounded channels deadlock | High | port DataFusion's global Gate; 10,000-iteration stall test written to fail on the naive version | P2 |
| 7 | Aggregate state round-trip drifts one ulp ⇒ cell-exact suite reds weeks later | High | serialize `f64::to_bits` raw; bit-exact property test | P3 |
| 8 | `EnforceDistribution` accretes exchanges ⇒ **single-node regression** | High | strip-then-reinsert; CI byte-identical plan hash at `cluster_size=1` | P2 |
| 9 | Materializing join/agg turn stages into serial barriers ⇒ misattributed to shuffle | High | P2.5 before any distributed benchmark is interpreted | P2.5 |
| 10 | Broadcast chosen wrongly ⇒ 540 MB plan bug swamps every measurement | High | CBO `B·(N−1) < P` **before** networking; golden plan tests; monotonicity-in-N invariant | P4 |
| 11 | Composite-key NDV underestimated (max-of-per-column) ⇒ wrong broadcast | Medium | golden plan test asserting Q09 does not broadcast partsupp | P4 |
| 12 | Partitioner degrades with P ⇒ superlinear-in-N term | High | counting sort + one grouped take, in the *first* exchange commit; microbench gate | P2 |
| 13 | Query hangs on an oversized page, pinning a reservation | High | out-of-band oversized request; explicit test | P6 |
| 14 | Leaked reservations from a dead initiator ⇒ cluster degrades over hours, looks like a leak | High | registry + 5 s sweeper + TTL; 50x kill loop asserting no drift | P6 |
| 15 | Transport and application flow control fight ⇒ sawtooth throughput | Medium | transport window ≈ 4x aggregate application credit; credit is the only real loop | P6 |
| 16 | HOL blocking across queries on a shared `PeerConn` | Medium | accepted trade for N−1 sockets; mitigated by #15; revisit only if measured | P6 |
| 17 | Small blocks as N grows ⇒ per-block cost dominates | Medium | block-size-targeted `P`; `< 64 KB` mean frame = failed config | P5 |
| 18 | Concurrency thrash misread as sublinear distribution scaling | Medium | admission control lands at **P1**, before any QPS claim | P1 |
| 19 | Skew invisible because TPC-H is uniform | Medium | deliberately skewed Zipf variant; never claim skew robustness from stock TPC-H | P7 |
| 20 | Correlated subqueries silently produce a one-node plan | Medium | hard planner gate + `EXPLAIN` surfaces `stage: single-node (correlated subquery)` | P5 |
| 21 | Adding `tonic`/`arrow-flight` forces an arrow bump, breaking Lance | High | ruled out by decision; hyper + arrow-ipc, both already in `Cargo.lock` | P6 |
| 22 | IPC dictionary batches emitted per page ⇒ wire explodes on string columns | Medium | dictionaries once per stream with stable ids; measured in P6 | P6 |
| 23 | arrow-rs IPC decode much slower than pyarrow's 14.8 GB/s | Medium | `StreamDecoder` over pre-aligned 64-byte `MutableBuffer`; **measure, do not assume** | P6 |
| 24 | ApproxDistinct/ApproxPercentile have unbounded partial state ⇒ shuffle `O(input)` | Medium | HLL / t-digest, with committed expectations regenerated in the same commit | P3/P8 |
| 25 | Loopback-tuned constants (compression, broadcast crossover) shipped to production | Medium | both are runtime config taking link bandwidth as input; every loopback number labelled SIMULATED | P6 |
| 26 | 18 impl sites must migrate `output_partitions` → `output_partitioning` | Low | mechanical; provided-method delegation makes divergence impossible afterwards | P2 |

---

## 11. What this design deliberately gives up

1. **Fault tolerance beyond page-level retry.** A worker loss fails the query. No task retry, no speculation, no materialized intermediates — speculation needs idempotent re-execution over materialized inputs and roughly doubles peak memory, violating the memory-safety rule. The token/sequence buffer eliminates the dominant failure class (transient connection drops) at near-zero cost, which is exactly why full retry is off the critical path.
2. **Correlated subqueries and DelimJoin cannot run distributed in v1** (§4.6). A real capability cliff, surfaced in `EXPLAIN`, not hidden.
3. **No elastic rescale mid-query.** Membership is static per query. The 256-bucket indirection makes the *cluster* resizable between queries at near-zero cost; a node joining or leaving mid-query fails that query.
4. **The initiator is a single point of failure per query.** Stateless *across* queries, so throughput scales by rotation, but an initiator crash kills its in-flight queries.
5. **Plans are a function of cluster size** and cannot be cached across resizes — the accepted price of getting broadcast-vs-partitioned right.
6. **Head-of-line blocking across queries** on a multiplexed peer connection.
7. **Small-query latency is worse than single-node, always.** `Subset(k)` keeps a 1 MB query at fan-out 1 so it does not get *dramatically* worse, but a 5 ms single-node query will not be 5 ms distributed. Accept it and measure it.
8. **Per-query parallelism is deliberately capped.** Admission control is a latency-vs-throughput dial pointed at throughput: fewer threads per query is worse single-query latency and strictly better aggregate QPS. Report single-query numbers under concurrency honestly rather than benchmarking them in isolation.
9. **Every approximation-for-scale knob is refused by default.** We will be slower than engines that quietly truncate on pathological queries. That is the intended trade: cell-exactness against DuckDB is what makes every number here trustworthy.
10. **Being slow under skew is accepted; failing or truncating is not.** A badly skewed shuffle spills to Arrow IPC and crawls. Tier-2 skew handling is gated on a measurement stock TPC-H will never produce.

---

## Appendix A — Verification log

Every claim about **this repository** was verified in source during this session:

`plan.rs:16-33` (trait shape; no `with_new_children` anywhere in `src/physical/` — grep, zero hits) · `memory.rs:97` (`MemoryReservation<'a>` is a borrow) · `memory.rs:~130` (`SharedMemoryPool = Arc<MemoryPool>`) · `scan.rs:13-40` (`TableStatistics` / `ColumnStatistics` with `ndv_est`) · `scan.rs:48` (`scan()` returns `Vec<RecordBatch>`) · `scan.rs:68` (`parquet_files()`) · `vectorized_hash.rs:11` (fixed seed `0x517cc1b727220a95`) · `:89` (`// Unknown types: hash stays unchanged`) · `:94-97` (multiply-then-ADD) · `hash_agg.rs:214`, `morsel_agg.rs:283`, `hash_join.rs:1225-1231`, `delim_join.rs:145`, `:244` (the four `output_partitions()==1` funnels, with the row-loss comment) · `hash_join.rs:1222` (`stream::iter(result.into_iter().map(Ok))`) · `morsel_agg.rs:623` (`enum AccumulatorState`), `:896` (`merge`), `:2270` (`AggregationState::merge`) · `spillable.rs:1826-1838` (row-at-a-time `DefaultHashBuilder` + true modulo) · `streaming_parquet_scan.rs:31-64` (`RuntimeFilterPayload::{Bitmap,Set}`, `SharedRuntimeFilter`, `RuntimeFilterConfig`), `:78/:238/:510` (`partitioned_work`) · `context.rs:279/325/339` (`sql()`, `output_partitions()`, `try_collect()`) · **18** `impl PhysicalOperator for` sites enumerated · `Cargo.lock`: `arrow-ipc 53.4.1` (:290), `dashmap 6.1.0` (:1461), `hyper 0.14.32` (:2638) **and** `hyper 1.11.0` (:2662), `prost 0.13.5` (:4286), `parking_lot 0.12.5` (:4026), `serde 1.0.228` (:5102); **`tonic`, `arrow-flight`, `bincode` absent** · `.scratch/datafusion/.../distributor_channels.rs` — 855 lines, Gate documented at :36-38, `Gate { empty_channels: AtomicUsize, send_wakers: Mutex }` at :62-63.

**Measured, not by me:** `.scratch/intranode_scaling.json` and `.scratch/proc_scaling.json` (written 2026-08-09 by a sibling agent). I read them and derived `f = 0.150` and `eta_box(K)` from them, but did not re-run the benchmarks.

**UNVERIFIED-BY-ME:** all first-party microbenchmark throughputs quoted from the prior transport report (IPC 3.08 GB/s serialize, 14.8 GB/s deserialize, loopback TCP 12.4 GB/s, LZ4 1.81 GB/s @ 0.566, ZSTD 1.13 GB/s @ 0.411, partitioner 3.11 → 1.30 GB/s/core at P=4 → 32); whether arrow-rs `StreamReader`/`StreamDecoder` approaches pyarrow's decode rate; all Trino, ClickHouse, Pinot and Velox behavioural and file:line claims, since I did not read those checkouts this session; and all SF=10 byte estimates in §5, which are computed from TPC-H cardinalities and current column widths rather than measured — re-derive them from the P5 `shuffle_bytes` counter before making any plan decision on them.