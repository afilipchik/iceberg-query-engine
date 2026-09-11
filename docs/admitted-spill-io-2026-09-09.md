# Admitted aggregate spill I/O — 2026-09-09

The [frozen HAVING diagnostics](admitted-having-output-2026-09-09.md) establish
correct but roughly99s native/Lance Q18 execution at4GiB. About76s is finalization;
a partial process sample records70M reads and96M writes. Spill frames previously
sent headers, scalar fields and checksums directly through `File`. Per-frame
logical-position queries also reached the descriptor. This repair changes the
shared spill-file boundary, with no query-specific rule or budget increase.

## Implementation contract

`morsel_agg/spill_io.rs` adds a query-admitted reader and writer. Optional buffers
use `ReservedVec<u8>`, capped at64KiB and sized from one sixty-fourth of currently
available query memory. Below1KiB capacity, or on optional memory admission refusal,
I/O stays unbuffered. No buffer allocation precedes reservation; leases stay with
the buffer. The run owner still accounts for reader/writer metadata. This does
not claim all query allocation paths are admitted.

Read-ahead maintains a logical position independently of the file descriptor.
`stream_position` reads that counter; seeks inside the buffered window update its
cursor, while other seeks reposition the descriptor and invalidate the window.
A new run resets the window/position while reusing the admitted storage. Existing
frame identity, ordinal, CRC, count, trailing-byte and length checks remain intact.
Memory-denied frame scratch still rewinds to the exact pre-header logical offset.
Interrupted refills invalidate consumed data before calling the underlying reader.

The writer coalesces small field writes. Explicit `flush` drains bytes before
`RunWriter::finish` checks file length or publishes a run. A partial drain followed
by an error poisons the buffer; subsequent calls cannot replay its written prefix.
There is no destructor flush. Unpublished/failed files remain owned and removed by
the existing run lifecycle. Original aggregation state is retained until a prepared
flush finishes successfully. Fault-injection tests flush pending data before
truncating the real descriptor; read-only-descriptor failure is now detected at
flush/publication, the first required physical write for a small buffered frame.

No dependency, spill wire-format, partitioning, aggregate arithmetic, default
memory budget or output semantics change. Repeated repartition scans, scalar
restore/merge work and serial finalization remain separate costs to measure.

## Validation checkpoint

- Focused38545 terminal0:135 aggregate library tests pass,1 ignored.
- A new interrupted-refill test46737 is red on the initial buffered reader:
  expected `abcdefghi`, observed `abcabcdef`. The corrected refill clears the
  consumed byte count before a potentially interrupted underlying read.
- Broad38093 terminal0:1,033 library tests pass,11 ignored;35 integrations pass.
- Spill73325 terminal101:6 pass,7 preceding failed names remain.
- Formatting and whitespace checks pass.
- Three changed/new source inputs versus fe3cc8fe;507 total inputs. The6-file
  [correctness archive](benchmarks/2026-09-09-spill-io/manifest.json) verifies.
- Release84684 terminal0 in8m52s: binary322c8042, all507 source hashes verified.
  Matched Lance57170 and provider diagnostic36153 are terminal0.
  Protected comparison8626 is terminal1; provider36745 is running. Source remains frozen.

Independent counting-I/O coverage writes200,003 patterned bytes in7-byte pieces,
reads in11-byte pieces and verifies exact contents. Each direction needs at most
four underlying operations with a64KiB buffer; querying each logical position
performs only the constructor's one underlying seek. Additional tests cover
buffer-crossing rewinds, end-relative seeks, unavailable optional admission,
interrupted/short reads and writes, partial-flush poisoning and reservation release.
Existing framing/repartition/merge tests cover corruption and retry contracts.

The optimized diagnostic results below verify a shared spill improvement.
Preserve `fe3cc8fe` as the immutable control. The completed protected comparison8626 uses
fresh matched DuckDB ceilings; full provider/residency/resource/concurrency
acceptance remains open. Component timing does not certify DuckDB leadership.


## Local reference-engine review

Read-only review of DuckDB checkout1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8
confirms its general serializer reader keeps buffered offset/total-read counters
and its writer batches small writes, flushing explicitly. See
[reader](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/common/serializer/buffered_file_reader.cpp:26)
and [writer](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/common/serializer/buffered_file_writer.cpp:30).
These are serializer utilities; they do not prove DuckDB external aggregation uses
that specific implementation. Its
[radix aggregate state](/media/afilipchik/nvme6tb/src/duckdb/duckdb/src/execution/radix_partitioned_hashtable.cpp:165)
registers temporary memory state and derives limits/block sizes from BufferManager.
The useful architectural distinction is page/block-managed partition storage and
memory coordination, beyond this engine's immediate syscall-batching repair.
This local checkout is not asserted to match the benchmark DuckDB wheel.


## Frozen optimized diagnostics

Matched Lance57170 is terminal0. Two blocks reverse binary order; every completed
output validates against the complete typed oracle. Query/process limits stay at
4/12GiB,16 threads on CPUs0–15. Both binaries and all507 source inputs verify after
execution, as do dataset/provider inputs. The48GiB scope peaks at14,338,469,888 bytes
with no max/OOM/kill events.

| Block | fe3cc8fe control | 322c8042 buffered | Last sampled read calls, control → buffered | Write calls, control → buffered |
| --- | ---: | ---: | ---: | ---: |
| 0 | 99,303.581ms | 23,396.432ms | 163,603,822 → 91,182 | 135,000,269 → 48,729 |
| 1 | 93,862.571ms | 22,937.650ms | 163,764,639 → 91,883 | 135,010,113 → 58,914 |

The observed geometric mean of the two paired ratios is0.23995, about76.0% lower
query time. Two instrumented blocks provide diagnostic attribution, not a robust
confidence interval or suite performance certification. The180s diagnostic
watchdog is not the10× DuckDB gate. I/O counters are the last once-per-second
process-wide samples; they include setup and may omit the final fraction of a
second. Sampling is identical for both binaries. Do not interpret them as exact
operator-exclusive totals or physical disk bytes.

Buffered Lance accumulation takes5.04–5.52s and finalization17.59–17.62s;
output construction/filtering remains1.319s. Logical spilled bytes remain about
1.59GB. Buffering removes the demonstrated syscall explosion but leaves repeated
partition scans and scalar restore/merge CPU work. The current scheduler serially
finalizes each worker and attempts a merge before scanning/repartitioning an
oversized partition. Attribute those remaining phases before changing scheduling
or splitting policy; row count is a cost estimate, never a uniqueness proof.

Provider diagnostic36153 also terminates0, all three outputs typed-correct:

| Mode | Query/process budget | Instrumented Q18 | Accumulation | Finalization |
| --- | --- | ---: | ---: | ---: |
| Raw Parquet | 4/12GiB | 949.858ms | Separate scan aggregate route | Separate scan aggregate route |
| Native | 4/12GiB | 23,695.138ms | 5,076.359ms | 18,011.627ms |
| Preloaded CPU | 32/48GiB | 5,592.843ms | 3,682.852ms | 1,389.124ms |

Native still spills1,590,000,848 logical bytes. Resident spills zero; its larger
budget is a separately labelled capacity experiment and does not clear lower
memory gates. These single requests are not paired performance claims against
older diagnostics. All binary/source/data/provider hashes verify after execution;
the64GiB scope peaks at21,550,166,016 bytes with no max/OOM/kill events.

All seven completed control/candidate outputs across these diagnostics are correct.
The222-file [immutable archive](benchmarks/2026-09-09-spill-io-diagnostics/manifest.json)
contains source, harness, drivers, inputs/oracles provenance, traces, outputs,
resource counters and build records. The source correctness archive remains
separate. Protected comparison8626 is terminal1; full acceptance is still open.
See the [paired and follow-up results](spill-io-paired-2026-09-09.md).


The custom-memory portion of8626 is terminal0 with completed/gated blocks, while
canonical comparison is now terminal1. This does not clear the protected slowdown
bound: customQ1 ratio1.0073 has95% interval0.9632–1.0535; customQ6 ratio1.0016 has
wide interval0.8974–1.1631. The prespecified fresh follow-up16613 completes208 correct/gated outputs with
ratio0.96290 and95% interval0.88593–1.05178; the original uncertainty remains
preserved alongside this stronger follow-up evidence. A driver
exit0 is not sufficient evidence for that stronger claim.


## Provider-dependent kernel divergence: next source audit

Current source explains an additional structural boundary behind the observed
roughly0.95s raw,5.59s resident-no-spill and23.7s native-spilling diagnostics.
`physical/planner.rs::create_aggregate` selects `MorselAggregateExec` for extracted
Parquet sources. Its generic Parquet route uses `AggregationState`, source-work
parallelism and `merge_states_to_batches_filtered`. Single primitive integer
keys can use the raw-u64 parallel shard merge; eligible dense ranges use a
range-partitioned direct-address merge. HAVING is evaluated after merged state.

The native provider route is narrower: it requires both native-source eligibility
and `dense_direct_shape`. That shape supports SUM over Int64/Float64 and AVG over
Float64; it rejects Decimal128 SUM. A native grouped decimal aggregate therefore
falls through to `SpillableHashAggregateExec` and the current `live_spill` path,
with at most four canonical-key controllers. Lance/MemoryTable do not receive the
Parquet generic aggregate route. These are source facts, not exclusive CPU
attribution or proof of the size of each effect.

The next design question is therefore broader than faster file calls: share
admitted compact primitive-key/state kernels across provider streams, with the
same spill and finalization contract. Do not simply route arbitrary providers
through the legacy Parquet operator: it owns a Parquet source, and its remaining
generic maps/null-state/output ownership do not certify a query-wide budget.
The existing raw-u64 state representation and checked dense-domain helpers are
reference components to evaluate, not permission to remove accounting or restore
estimated-uniqueness assumptions. Profile the current merge first, then choose a
bounded change whose independent semantic and resource tests cover duplicate and
NULL keys, wide decimals, batches/partitions, pressure, and actual spilled partials.


## Possible premature-spill policy: reproduce before changing

`live_spill.rs` computes `group_limit` from
`min(config.memory_limit * spill_threshold, pool.max()/4) / (64 + 48*aggregate_count)`.
`ParallelControllers` divides that count across workers. This count is a policy
estimate separate from actual query reservations; reaching it forces a flush even
when the pool can still admit more state. In the frozen diagnostics, native4GiB
Q18 peaks at1,943,468,467 tracked reserved bytes while spilling; the32GiB resident
case peaks at2,721,748,397 tracked reserved bytes without spill. Provider residency
and input ownership differ, so these numbers do not prove the4GiB run can complete
without spill, nor do they certify query-wide/RSS accounting. They justify a
specific capacity-policy experiment before assuming all remaining work needs a
new state layout or parallel merge implementation.

After current frozen measurement gates, add a generic grouped/HAVING case whose
state demonstrably fits its parent pool yet currently crosses the quarter-budget
count limit. Preserve independent expected values and actual spill accounting.
Compare actual admission pressure against the static count policy under the same
budget. Any candidate must retain prepared spill/retry scratch, spill on real
pressure, avoid source/expression replay, and pass low-memory mixed/variable-state
cases as well as fitting fixed-state cases. Never replace checked reservations
with the per-group estimate or raise the query limit to declare the bug fixed.
Only then freeze and test native/Lance at the original4GiB budget.
