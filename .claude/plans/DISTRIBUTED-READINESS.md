# Distributed engine: readiness assessment and revised phasing

**Date:** 2026-08-10 · **Verdict: NOT READY. Do not start P0.**
Companion to `DISTRIBUTED-DESIGN.md` (the architecture, still valid in mechanism)
and `ARCHITECTURE-REVIEW.md` (the adversarial review that produced this).

## 1. What exists today

**Zero distributed code.** No exchange, shuffle, fragment, coordinator, or
transport module exists in `src/`. `ls src/distributed src/exchange src/cluster`
→ nothing. What exists is a 965-line design document and a set of measurements,
one of which was wrong (see §3).

So "current state" is: *a reviewed design, no implementation, and three
single-node defects that distribution would convert from latent to fatal.*

## 2. Why it is not ready — three prerequisites, all verified in source

These are not design disagreements. They are properties of the current code that
make the design's core assumptions false on arrival.

### P-1.a The partition contract is honoured by operators, not by callers

`check_partition` (landed in `df0fef8`) now makes an out-of-range
`execute(partition)` a loud error at all 18 operator sites. But three *callers*
still drive only partition 0 regardless of what the plan declares:

| site | what it drives |
|---|---|
| `src/physical/operators/subquery.rs:40` | `rt.block_on(physical.execute(0))` — CTE materialization and **every uncorrelated subquery** |
| `src/physical/operators/delim_join.rs:154,175` | both join inputs |
| `src/physical/operators/vector_search.rs:236` | the exact-path fallback |

`src/execution/context.rs:325` does it correctly (`output_partitions().max(1)`),
which proves the contract is expressible — these three simply predate it.

**Why this blocks distribution.** An exchange is *defined* by consumers draining
every partition a producer declares. A caller that reads partition 0 and stops is
locally a row-loss bug and remotely a shard-loss bug. It already forced a real
concession: `UnionExec` had to ship with `output_partitions() = 1` instead of the
parallel `sum(inputs)` form, because under the sum a twice-referenced CTE over
`UNION ALL` returned 1 row instead of 4 (measured against DuckDB). **Union cannot
be made parallel until these three callers are fixed.**

### P-1.b The memory rule is unenforced, and P1 multiplies the load 8x

`MemoryPool::try_allocate` / `allocate` have **zero production callers**.
`peak_memory_bytes` is always 0. `hash_join.rs`, `hash_agg.rs`, `morsel_agg.rs`
and `sort.rs` contain no accounting at all. The rule is implemented as seven
independent copies of `memory_limit * spill_threshold`, each comparing one
operator's local tally against the *entire* budget — so two concurrent operators
each "fit" while together they do not.

Worse, `spillable.rs:273-275` collects the **whole build side into RAM** and only
then evaluates `build_size > memory_threshold`. An oversized build OOMs before
the spill decision runs. `CLAUDE.md:882` describes this as a "known hole, fixed
by the Phase-5 streaming spill rewrite"; the code shows it is not fixed.

**Why this blocks distribution.** P1 (shared-nothing, 8 processes) multiplies
concurrency by 8 against a budget nobody enforces, and the design's cluster
memory plan is built on this pool. Adding an owned reservation handle first would
make shuffle pages the *only* class the budget can see — precise about kilobytes
while blind to gigabytes of build state. **Item zero is routing existing
consumers through the pool, not adding a new handle.**

### P-1.c The initiator executes sub-plans during planning

`run_subquery_blocking` executes whole sub-plans at *planning* time — measured at
**87% of Q15 and 92% of Q11** at SF=10. The design's §6.3 assumes initiator work
is O(1) in data size. It is O(rows).

Distributed, this is not merely a bottleneck: the results are frozen into the
plan as literals. Computed on one node over that node's shard, **Q11's `SUM` and
Q15's `MAX` become silently wrong** — the shard's value, presented as the
cluster's. The design's §4.6 capability gates cover only `DelimJoin`, which is
dead code (`FlattenDependentJoin` returns `plan.clone()`), so nothing catches it.

### Also missing, cheaper but required before transport

* **No cancellation anywhere in `src/`** (verified by grep). A failed fragment
  cannot stop its peers; a cancelled query cannot stop its fragments. Every
  distributed failure becomes a resource leak until completion.
* **Thread oversubscription**: rayon (32) + the main tokio runtime + the subquery
  runtime + a metastore runtime, with `block_on` called inside async contexts.
  Adding a transport that shares any of these produces stalled RPCs and spurious
  fragment timeouts. Fix before P3, not after.

## 3. One design input was wrong and has been corrected

`DISTRIBUTED-DESIGN.md` §0 quoted an Amdahl serial fraction `f = 0.150` and the
conclusion "single-query speedup is capped at 6.7x forever", using it to justify
phase ordering and to set P2's accept gate. That number came from
`scripts/scaling_bench.py` pinning **contiguous cpu ids on a hybrid SMT machine**,
so its "cores" axis varied core *type* as well as count — `K=2` was two
hyperthreads of one physical core, `K=32` silently added 16 E-cores.

Re-measured over homogeneous physical P-cores: **1.00 / 1.43 / 2.58 / 4.23** at
1/2/4/8 cores, 53% efficiency at 8. A per-point Amdahl fit gives
0.389 / 0.189 / 0.129 — a spread that rejects a single serial fraction. The cap
is withdrawn; fit a USL (contention + coherency) instead. The harness is fixed.

The **+46% shared-nothing result is independent of this error and stands**, so
P1 keeps its priority — on its own evidence.

## 4. What in the design survives unchanged

Preserve verbatim; these were checked and are right:

* The **hash-avalanche diagnosis**. `combine_hash` for a single `Int64` key is
  affine in the key, so the top bits are constant across the `l_orderkey` domain.
  Bucketing on high bits is broken *exactly*, not probabilistically.
* `replicate_nulls_and_any` for partitioned outer joins.
* The global `Gate` over per-channel bounded queues (backpressure).
* `inproc://` as the default transport, so every phase is testable on one box.
* `with_new_children` defaulting to `Err`.
* **Flight semantics without the Flight crate** — `hyper 1.11` and
  `arrow-ipc 53.4.1` are already in `Cargo.lock`; `tonic` and `arrow-flight` are
  not, and adding them risks forcing an arrow-major bump against the arrow-53 pin
  that Lance requires.
* The honest §11 trade-off disclosures.

## 5. Revised phasing

**P-1 — Prerequisites (NEW, blocking).** Fix the three partition-0-only callers;
route the existing large allocators through `MemoryPool` and make
`peak_memory_bytes` real; make `spillable` decide before materializing; add
cancellation; collapse the runtimes. *Every item is a single-node correctness or
robustness win that stands on its own merit if distribution is never built.*
Gate: `UnionExec` can flip to `sum(inputs)` with the twice-referenced-CTE case
still matching DuckDB; a query killed mid-flight releases its memory.

**P0 — Foundations.** As designed, with two corrections the review found: the
hash gate is self-contradictory (it demands an avalanche *and* that no existing
table changes shape — pick one), and it misses `hash_i64`, a second entry point
that probes the buckets `hash_arrays` builds. Change one without the other and
every point lookup silently misses while the gate passes.

**P1 — Shared-nothing replica model.** Unchanged in priority. Add admission
control *in this phase*, not later: 8 processes against an unenforced budget is
exactly where OOM appears.

**P2 — Partitioning/Distribution + EnforceDistribution + local exchange.**
Restate the accept gate over **physical cores** (baseline 53% at 8), and measure
it with a *concurrent* workload — the current gate is a latency measurement
wearing a throughput number's clothes. Make `output_partitioning()` **required**,
not defaulted to `Unknown(1)`: a default silently re-arms the exact bug class
`check_partition` was added to kill.

**P2.5 onward** — as designed, plus one addition the review rated highest
leverage-per-line and the design lacks entirely: a **~150-line `verify.rs`**
distribution-invariant checker run under `debug_assert` after
`EnforceDistribution`. It converts every existing test into a distribution
test for free.

## 6. Recommendation

Spend the next cycle on **P-1**. It is roughly two weeks, it fixes defects that
are wrong *today* on a single node, and it converts the distributed design's
three false assumptions into true ones. Starting P0 first would build an exchange
layer on top of callers that ignore partitions, a memory budget nothing consults,
and a planner that executes 87% of a query before the plan exists.
