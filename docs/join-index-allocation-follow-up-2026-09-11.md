# Join index allocation follow-up — 2026-09-11

Frozen864e645 checkpoint reproduces engine LanceQ9 warmup refusal: join index
allocation failed because the allocator returned an error. Reference-only I/O
quota16 cleared the reference barrier but did not change the engine runtime.
The current Lance sibling-task cleanup candidate has not yet been measured.

Read-only source evidence while pipeline99427 builds:

- VectorizedHashTable::build evaluates build keys, chooses hash/direct layout,
  computes index_bytes, obtains pool.allocate(index_bytes), then allocates
  heads/next/entries with try_reserve_exact. The reproduced text comes from this
  latter allocation, not a query-pool denial. I64 key buffers are shared views,
  not copied key payloads; evaluated/decoded keys remain a separate ownership item.
- SpillableHashJoin::compute_build_decision includes payload plus the index
  storage bound in its threshold decision, then memoizes an InMemory HashJoin.
  Its execute delegates directly. A subsequent index-allocation error propagates
  from HashJoin initialization; it does not cause a new spill decision.
- The visible failure is therefore compatible with process-cap exhaustion despite
  a successful local reservation. It does not establish whether the excess comes
  from provider data, runtime stacks, allocator retention, keys or other operators.

Next diagnostic: after all candidate timing, replay the successful engine request
prefix through Q9 with process VmData/RSS/thread snapshots. Compare a fresh Q9
worker with the persistent sequence. Retain the existing4/12GiB caps and validate
completed outputs independently. Do not claim the sibling cleanup fixes a failure
preceded exclusively by successful scans.

A possible later repair needs an explicit build-initialization transition before
publishing the in-memory decision: preserve owned input and any evaluated-key state,
release provisional index storage, and transition to spill on a supported typed
allocation refusal. This is a design hypothesis, not an implemented fallback.
Do not catch every Execution error or repeat source/expression evaluation. Numeric,
framing and semantic errors must remain terminal; capacity overflow is distinct
from allocator refusal. Check build ordering, runtime filters, duplicate/NULL and
outer/semi/anti semantics, cancellation and leases across the transition.

Reproduce with deterministic index-allocation failure injection, verifying actual
spill and independent typed results, plus terminal non-memory errors and exact
source/expression call counts. First identify the retained process allocations:
a fallback can otherwise mask a provider ownership defect without making the
engine resource-safe. No engine changes were made during frozen measurement.
