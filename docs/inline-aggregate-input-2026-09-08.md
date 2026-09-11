# Direct fixed-width aggregate inputs — September 8, 2026

Source inspection identifies an unnecessary serialization boundary in the measured
aggregate update path. `key_rows/arrow_input.rs::inline` resolves a logical Arrow
cell, writes its validity and value into a 17-byte stack frame through `visit`,
then immediately decodes those bytes through `ReservedScalar::try_decode_inline`.
`StateRows::prepare_arrays_indexed` uses this for every non-COUNT fixed aggregate;
selected numeric states also use it. The temporary uses stack memory, so this is
not a heap-allocation or pool-admission bug. Its isolated CPU contribution is
unmeasured; the prior state-update profile motivates a controlled comparison.

The encoded byte boundary is necessary for persisted state and canonical keys.
It is unnecessary for an immutable in-process Arrow primitive already checked
against its bound logical type. The proposed change reads the resolved primitive
into the same inline ScalarValue directly, preserving the existing transaction,
spill cursor and numeric update implementation. This avoids transposing aggregate
updates or introducing a new partial-mutation failure boundary.

Required preservation:

- Resolve dictionary codes with existing bounds/depth checks; NULL codes and NULL
  dictionary values remain SQL NULL. Sliced offsets remain Arrow's responsibility.
- Preserve signed and unsigned widths, Boolean, Date32/Date64, full i128 decimal
  coefficients and signed scale. Preserve float bit patterns, including signed
  zero and NaN payloads; aggregate input is not canonical key encoding.
- Keep variable payloads and timestamp metadata on the admitted selected-state
  path. Unsupported physical representations return an explicit error.
- Keep arity/layout/type validation, rollback, checked integer accumulation,
  selected-value admission, source ownership and retry semantics unchanged.
- Add independent expected-value tests for every supported primitive and decimal,
  NULLs, dictionaries, sliced arrays, unsupported domains and invalid rows.
  Retain existing selected-state, exact arithmetic, allocation-denial and spill
  regressions. Compare the frozen 476ec119 release with the new candidate under
  unchanged paired gates, including custom Q1 and canonical protected queries.

Implementation is now present after provider53182 and supplemental23286 completed
and archive1404files verified. `inline` directly constructs the same scalar variants
from checked typed Arrow values. Persisted state/key encoding is unchanged. Three
new independent tests cover primitive/date widths, decimal coefficients/scales,
NULL/sliced/duplicate selections, float bit patterns, all eight dictionary key
widths, invalid rows, admitted fallback domains and unsupported binary input.
Focused70124 terminal0:132 passed,1 existing ignored,879 filtered,2.65s.
Broad50678 terminal0:1001 library passes/11 existing ignored,40 selected integration
passes with no skips across nine binaries. Native10/10 and live forced-spill/retry
contracts remain green. Full spill89730 terminal101:6pass/7fail, the same seven
failed names as the frozen join-index gate. They remain unresolved failures; this
comparison does not prove identical denial boundaries. Formatting and whitespace
checks pass.

Exactly one of the493 frozen source inputs differs from476ec119:
`src/physical/morsel_agg/key_rows/arrow_input.rs`, including the three new tests.
Release77024 completed successfully in8m43s under48GiB/one build job,
locked/offline with lance,gpu. All493 source hashes verified; frozen binary
`a023079ff45b8159614b4a313a7874a4599b355b85ef1cfa449996465d6df0e2`,
source-hash manifest`cbb14e3dbdae4729f506586b9082560172bad6cc469ffcc983aafc25b885e4a6`.
Paired79406 is terminal1 under48GiB: custom CPU-resident Q1/Q6 terminal0,
canonical SF10 terminal1. No source/test/harness
edits or overlapping heavy jobs until terminal. Prepared paired drivers retain
custom Q1/Q6 and nine canonical SF10 queries, four fresh-process blocks, six
measured pairs per block, independent typed validation and unchanged timing gates.
No speedup or overall resource/leadership acceptance is claimed yet.

Test/source evidence: `benchmarks/2026-09-08-inline-input-tests/`. The preceding
476ec119 provider study remains separately frozen and archived. A prepared decoded
IPC/GPU driver has not run on a new candidate; residency acceptance remains open.

Local DuckDB/ClickHouse batch-update boundaries are documented in
`aggregate-update-boundary-2026-09-08.md`; no third-party implementation was copied.

## Completed custom component

The custom CPU-resident Q1/Q6 arm of79406 is terminal0. All112 completed engine
outputs (96 measured plus16 warmups) pass independent typed validation and the
unchanged timing gate. Four fresh-process blocks, six measured pairs per block,
four threads,4GiB query/8GiB process; no profiling or GPU execution.

Q1 after/before ratio0.7873724, whole-block bootstrap95%[0.7452554,0.8384664]:
21.26% improvement (95%16.15–25.47%). Q6 ratio0.7951253 with a wide interval
[0.6065599,1.0923840], so no Q6 improvement is established. Both protected upper
bounds remain below1.10. Scope peak383479808bytes with zero max/OOM events; this
is neither precise RSS nor proof of query-wide reservations.

This is the600000-row custom float component, not canonical SF10. The canonical
nine-query arm has completed with failures, detailed below. Raw custom evidence is in
`.scratch/parallel-aggregate-input/inline-input-smoke-pairs/`.

### Verified route distinction

Preserved Q1 plans differ: canonical raw Parquet selects
`ExternalSort → Project → MorselAggregate`; custom preloaded input selects
`ExternalSort → Project → SpillableHashAggregate → Project → Filter → MemoryTableScan`.
`operators/morsel_agg.rs` drives `AggregationState::process_batch`, whose evaluated
batch path pre-downcasts typed accessors. The changed helper belongs to the admitted
partial-row/selected-state path. This route distinction explains why the custom
result cannot establish a shared speedup across every provider or physical operator.
It is not an exclusive-CPU attribution of the canonical result. Preserve the fast
scan/aggregate route while developing admitted typed batch updates and a common
provider-neutral pipeline; simply changing routing would need its own resource and
performance evidence. Plans are in each study's `q01-b0/result.json`.

## Completed canonical component

All36 reference calibrations validate.346 completed canonical engine outputs pass
independent typed validation;338 pass the unchanged timing gate. Eight completed
outputs are late,16 requests time out and142 dependent requests are not run.
Q9/Q12/Q13 lack all four complete paired blocks and receive no ratio or suite
score. Scope peak2852982784bytes, zero max/OOM events.

| Canonical query | After/before ratio | Whole-block bootstrap95% interval |
|---|---:|---:|
| Q1 | 0.998071 | 0.994262–1.003345 |
| Q2 | 0.955102 | 0.902942–1.030886 |
| Q5 | 0.986181 | 0.979768–0.994933 |
| Q10 | 1.005690 | 0.985613–1.026176 |
| Q19 | 0.998514 | 0.967428–1.039345 |
| Q20 | 1.018524 | 0.989817–1.050540 |

Q5 improves1.38% (95%0.51–2.02%); other complete canonical intervals include1.
All complete-query protected upper bounds, including custom Q6, remain below1.10.
Across both components458 completed outputs validate and450 are gated. The
21.26% custom Q1 gain is confirmed for that workload, not overall DuckDB leadership
or a5% gain across the canonical component. Provider/residency/resource/concurrency
acceptance remains open. Evidence: `benchmarks/2026-09-08-inline-input-pairs/`,
including the complete source archive, both study roots and all failed outcomes.


Paired archive1125files and493 source inputs verified. Follow-up3109 is terminal1, with independent audit77556 terminal0,
under64GiB: canonical decoded IPC and preloaded CPU control at32GiB query/48GiB
process, dependent mixed GPU, then fresh custom CPU control and required GPU at
4GiB/8GiB. Existing NVRTC libraries are supplied by process-local LD_LIBRARY_PATH
and hashed. The32GiB canonical capacity experiment does not clear the lower-memory
preload refusal. The four-provider driver84971 for this candidate is now ACTIVE; preceding476ec119 evidence stays distinct.

Residency archive1100files verified: all244 completed outputs correct, custom
required GPU40/40 with request-scoped device evidence. Canonical mixed GPU does
not execute after incomplete CPU control. See [residency evidence](inline-input-residency-2026-09-08.md).
