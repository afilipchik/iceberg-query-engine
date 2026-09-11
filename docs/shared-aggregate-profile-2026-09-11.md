# Shared aggregate attribution — 2026-09-11

The Lance refinement cycle is committed and pushed as
`bb38784aba074c288bedf17722e32d2f8c8cc500`; the remote branch hash was verified.
This next-cycle diagnostic uses its unchanged frozen binary `8c4936d8`.
Job 63494 is terminal 0: eight fresh-worker cases, one warmup and two samples
each, all 24 outputs independently typed-correct against the SF10 DuckDB oracles.
Four/sixteen threads share affinity 0–15; default disjoint ownership, 4 GiB query
and 12 GiB process budgets. QE_AGG_PROF and Lance scan timing are enabled.
The 16 GiB cgroup peaks at 5,300,224,000 bytes, zero OOM/max events, swap disabled.
This is attribution, not a reversed-block or multi-session performance comparison.

| Case | Median query ms | Median execute ms |
|---|---:|---:|
| raw_parquet-t4-q01 | 1345.652 | 1345.300 |
| raw_parquet-t4-q06 | 383.214 | 382.945 |
| lance-t16-q01 | 6231.889 | 5968.864 |
| lance-t16-q06 | 294.399 | 156.547 |
| raw_parquet-t16-q01 | 706.480 | 706.129 |
| raw_parquet-t16-q06 | 191.250 | 190.999 |
| lance-t4-q01 | 6099.550 | 5826.354 |
| lance-t4-q06 | 377.858 | 244.766 |

Raw Q1/Q6 select `MorselAggregate`; Lance selects `SpillableHashAggregate` over
Project/Filter/MemoryTableScan. The provider distinction also changes the CPU
algorithm. Planner `lower_aggregate_cpu` selects morsel aggregation from Parquet
files; the native alternative has a narrow dense-direct eligibility proof.
It does not expose the same aggregate kernel to arbitrary admitted batch sources.

Lance Q1's last measured 16-thread request reports 1,459 ms expression evaluation,
2,038 ms prepared-key construction plus routing, and 2,395 ms worker processing.
These are wall intervals; ingestion contains routing/processing, finish contains
output, and upstream work may overlap. They must not be added as exclusive CPU
samples. There are 7,323 batches and 59,142,609 filtered rows, four disjoint owners,
16 admitted input slots, four final groups, no spill. At four threads the same
four owners and roughly six-second query time remain. More input slots alone do
not parallelize evaluation, key preparation, owner routing and state ingestion.

Next implementation sequence:

1. Split existing opt-in routing telemetry into key construction and row dispatch;
   establish how much time is encoding/allocation versus routing before choosing
   a kernel. Keep batch ownership, terminal error behavior and budgets unchanged.
2. Audit common grouped-state updates and prepared-key storage against local
   DuckDB/ClickHouse batch aggregation. Target repeated work or avoidable barriers
   in the generic path, not SQL/query IDs. Do not merely raise the owner count:
   low group cardinality limits useful disjoint ownership.
3. Any alternate partial reduction must retain query-wide reservations, duplicate/
   NULL/encoding semantics, spill and merge correctness, and four-thread gates;
   historical partial-mode resource/numeric failures remain unresolved.
4. Reproduce the selected mechanism with a focused regression, then apply the
   narrow shared fix. Run ordinary/partial correctness and resource gates, frozen
   canonical SF10, paired attribution and provider endurance where relevant.
   Archive results, commit and push the completed cycle before continuing.

[Verified diagnostic artifacts](benchmarks/2026-09-11-shared-aggregate-profile/manifest.json).
No architectural code change in this diagnostic. The earlier provider/resource/
residency/concurrency gaps remain open.

## Completed routing phase diagnostic

The next source change only splits existing opt-in routing telemetry into key
preparation and row dispatch, with prepared/unprepared batch counts. Existing
`routing_ms` retains its original meaning. Row dispatch includes memory-pressure
recovery and retry; samples report completed streams and are wall intervals, not
exclusive CPU measurements. Default scheduling, ownership, admission and SQL
behavior are unchanged.

Test job 95466 is terminal 0: all nine existing parallel-controller tests pass,
including disjoint/partial spill and startup-pressure contracts. Formatting passes.
Release74760 completed0 in8m49s, freezing99e1c0dd from800inputs. Controller72486
completed0:12typed-correct outputs across Q1/Q18 at4/16threads, all800source hashes
verified after. Its16GiB scope peaked at5,489,876,992bytes withzeroOOM/max events.
The [46-file phase archive](benchmarks/2026-09-11-aggregate-routing-phases/manifest.json)
includes a verified source snapshot.

Q1 at16threads has1,733–1,758ms measured key preparation versus351–363ms row
dispatch; at4threads1,732–1,746ms versus328–329ms. All7,323batches use prepared keys,
so admission fallback does not explain the cost. Q18's large inner aggregate takes
743–775ms preparation/358–359ms dispatch at16threads and676ms/350–357ms at4threads;
its small outer aggregate is separate. Query medians are6,305/6,258ms forQ1 and
4,049/3,968ms forQ18 at16/4threads. These diagnostic samples isolate the phase;
they do not certify a speedup or a precise cross-thread regression.

Source audit identifies a specific hypothesis: `BoundKeyArrays::Column` binds
primitive representations once, but Utf8 falls into `Checked`. Every row visits
the generic Arrow encoder while computing size and again while writing bytes.
PreparedKeys repeats this for every row before routing. A borrowed Utf8 binding
could remove repeated representation checks without changing canonical bytes.
The phase results justify a targeted binding experiment; this is not yet a measured speedup.
Preserve NULL versus empty strings, embedded NUL/Unicode, sliced offsets, dictionary
logical equivalence, admission refusal and stale-key invalidation.

Local comparisons were rechecked at DuckDB
`1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8` and ClickHouse
`a1b25f3f4beb3ba49aa3b73671cc244185331b86`.
DuckDB's `aggregate_hashtable.cpp` caches dictionary-entry group addresses and
hashes only newly encountered entries; its update loop dispatches aggregates over
batches. ClickHouse's `Aggregator.cpp` likewise separates batch lookup and aggregate
instructions (`addBatch`/`addBatchSinglePlace`). These are implementation references,
not claims about the pinned benchmark wheel or compatible SQL/memory contracts.

The next test36734 runs the new borrowed-string mechanism regression against the
original encoder. It checks canonical bytes independently and counts downcasts
over512sliced rows withNULLs, empty strings, Unicode, embedded NUL and duplicates.
No encoder optimization has been applied yet.
