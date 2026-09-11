# Aggregate spill threshold honors its configured budget — 2026-09-09

The [generic capacity reproduction](quarter-budget-spill-reproduction-2026-09-09.md)
showed fitting grouped state spilling under the separate quarter-pool count cap.
The new same-budget regression confirms that this is a policy problem, rather
than requiring more query memory for the tested workload.

## Change and preserved contracts

`morsel_agg/live_spill.rs` now derives its advisory group-count ceiling from the
configured spill threshold, bounded by the root pool maximum. It removes the
additional division of that maximum by four. The per-group estimate remains
costing only; it is not admission or proof that all state fits. This is shared
aggregate policy and contains no benchmark SQL, table or query identifiers.

Actual checked allocation reservations remain unchanged, including state/index
old-plus-new growth, admitted input/output and prepared spill resources. The
controller still prepares its writer before consuming input. A typed allocation
denial with nonempty resident state flushes through that writer, releases old
capacity, prepares the next working set and retries the exact uncommitted row.
Empty-state refusal and non-memory failures remain terminal. Source/expression
replay is not introduced. The configured query limit is unchanged; there is no
memory-safety opt-out or dependency/module addition.

Final output retention still has to be admitted. This change does not certify
all providers' query-wide accounting or guarantee every large result fits. The
new regression also checks an unfiltered50,000-group result at the same16MiB
budget, so the change is not validated only with a tiny HAVING result.

## Red/green evidence

- Initial test command fails to compile because the spill-path builder expects
  PathBuf; the fixture is corrected without an engine change.
- Red5041 terminal101: correct output and cleanup succeed, then the regression
  fails because5,445,212 bytes spilled at16MiB.
- Focused92376 terminal0:136 aggregate library tests pass,1 ignored. Its name
  filter excludes integration names; no integration coverage is claimed there.
- Green82548 terminal0:all4 HAVING integrations pass. The new test covers both
  selective final filtering and retained full output; fitting filtered state
  spills zero bytes at the original16MiB limit. NULL keys/values, duplicated
  contributions, exact Decimal128 sums, typed independent expected values,
  reservation release and spill-directory cleanup are checked.
- Broad93472 terminal0:1,033 library tests pass/11ignored and36 integrations pass.
- Spill80448 terminal101:6 pass,7 preceding failed names remain.
- Formatting and whitespace checks pass. The8-file
  [correctness archive](benchmarks/2026-09-09-aggregate-threshold/manifest.json)
  verifies507 inputs and the two-file change set.
- Release68533 terminal0 in8m51s, frozen1efb2554 with507 verified inputs.
  Matched Lance diagnostic84734 terminal0: all four outputs typed-correct;
  source, binary, provider and data hashes verify after execution.

Only live_spill.rs and aggregate_post_filter_admission.rs differ from322c8042.
## Matched Lance diagnostic

Frozen1efb2554 is compared with buffered-I/O control322c8042 in two fresh
blocks with reversed startup order,16 threads, CPU affinity0–15,4GiB query and
12GiB process limits. All four Q18 outputs pass the complete typed oracle,
including the result's limit-tie policy.

| Block | Control time | Candidate time |
| --- | ---: | ---: |
| 0 | 22,693.495ms | 5,366.393ms |
| 1 | 22,700.605ms | 5,397.359ms |

The observed geometric paired ratio is0.23712, or76.29% lower elapsed time.
The candidate reports zero logical spill; its first grouped stage takes about
3.71s accumulating and1.38s finalizing. This is an instrumented two-block
attribution experiment with a180-second watchdog, not a confidence interval,
matched DuckDB10× acceptance gate or suite certification. The48GiB scope peaks
at9,924,603,904 bytes with zero max/OOM/kill events. All507 source inputs and
binary/provider/data hashes verify after execution.

Raw/native/resident diagnostic48836 terminal0 adds three typed-correct outputs:
raw Parquet911.159ms, native5,623.089ms at4GiB, and preloaded CPU5,613.262ms at
32GiB query/48GiB process capacity. Native's main aggregation reports zero spill,
3,639.719ms ingestion and1,378.371ms finalization, with1,338.462ms output included
in finalization. Its tracked reserved peak is2,165,728,545 bytes. These timings
are single instrumented requests; the older23.7s native result is historical,
not a new paired comparison. The resident capacity case does not clear16GiB
preload admission. The64GiB scope peaks at21,574,123,520 bytes with zero max/OOM/kill.

The [222-file diagnostic archive](benchmarks/2026-09-09-aggregate-threshold-diagnostics/manifest.json)
verifies all507 source inputs and preserves drivers, raw outputs, plans, profiles,
oracles, resource samples and release provenance. Protected comparison22140 is
terminal1:527 completed outputs typed-correct,521 gated,6 late,8 timeouts and
81 not-run slots; Q9/Q12 remain incomplete. Fresh matched DuckDB calibration and
10× ceilings are unchanged. Fixed Q6 follow-up35348 also fails its uncertainty
gate; see [protected results](aggregate-threshold-paired-2026-09-09.md). Broader
provider, residency, resource and concurrency acceptance remain required. Preserve322c8042
and its completed evidence as the control. The separate unsupported-predicate
collecting fallback refusal remains open.

## Diagnostic environment correction

Review of48836 stderr found an attempted optional GPU runtime startup in each
CPU diagnostic, followed by a missing-NVRTC worker panic and successful CPU
query completion. The process exits0 and the physical plans/output are preserved,
but the driver did not explicitly disable GPU as the matched Lance and normal
benchmark harness do. Those three runs are qualified CPU-fallback diagnostics,
not clean CPU-control evidence. Their immutable archive remains intact. A new
`run_aggregate_threshold_q18_profile_02.py` sets `QE_GPU=0`, clears inherited
profiling/runtime overrides and rejects any worker panic or non-null GPU snapshot.
Repeat44630 is terminal0: all three outputs typed-correct, no worker panic and
all GPU snapshots null. Raw Q18 takes974.006ms, native5,716.821ms at4GiB and
preloaded CPU5,616.847ms at32GiB. The64GiB scope peaks at16,653,905,920 bytes,
with zero max/OOM/kill. All507 source inputs and binary/data/provider hashes
verify. The [182-file repeat archive](benchmarks/2026-09-09-aggregate-threshold-cpu-repeat/manifest.json)
is authoritative for explicit CPU-only diagnostics. The paired archive also
contains an incidental snapshot of this repeat's then-running log; that snapshot
is not its terminal result. No engine source change was needed.

## Remaining shared CPU work

The clean matched Lance profile attributes about1.56s to key preparation/routing
and2.14s to controller processing wall time, with four owners and7,323 parallel
batches. Output construction/filtering consumes1.34s of1.38s finalization. These
intervals identify stages; they do not identify exclusive instruction costs.

Source inspection explains a plausible output cost: `live_spill.rs` finalizes
64 groups per output batch, and `admitted_output.rs::build_output_range` validates
schema, admits metadata, decodes keys/aggregate values into scalar intermediates,
then builds Arrow arrays. HAVING filters those arrays after complete merging.
For millions of groups with a selective predicate, this repeats substantial
construction work even though few groups survive. The remaining work is shared
across generic grouped aggregation; it should not be specialized to Q18.

The next bounded CPU investigation should sample a frozen no-spill execution,
then compare output conversion, routing and updates independently. Candidate
changes should bind invariant output metadata once and emit fixed-width typed
values directly into admitted Arrow buffers, or choose output batches from a
checked resource envelope. Preserve exact decimals/float bits, NULLs, variable
state fallback, final-merge-before-HAVING semantics and surviving-buffer leases.
A larger hard-coded row count alone is not a memory contract. Require generic
filtered/unfiltered, multiple-batch and actual-pressure regressions before a new
release and matched provider/protected gates.

Local DuckDB's `RadixHTLocalSourceState::Scan` initializes a tuple scan once,
scans chunks, finalizes aggregate states into vectors and references those
vectors into output; see `src/execution/radix_partitioned_hashtable.cpp:889` at
local revision1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8. This is a source-level
design reference for reducing conversion/materialization, not proof that copying
that code or changing batch size alone would close this engine's measured gap.
