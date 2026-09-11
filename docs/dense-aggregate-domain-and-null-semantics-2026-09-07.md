# Dense aggregate domains and NULL semantics — 2026-09-07

The [full SF10 comparison](dense-domain-full-sf10-2026-09-07.md) is now complete.
Every performance measurement here remains for frozen662. The separate decimal
bound edit compiles and has59 selected passes; validation also reproduces an
existing [parallel spill-completion failure](fused-aggregate-budget-overshoot-2026-09-07.md)
with both helper versions. Its optimized performance remains pending.


## Reproduced failures

A new optimized shared-merge probe uses70000groups, duplicate rows, two Parquet
row groups, exact decimals beyond f64 precision, Float64 SUM and a NULL key. It
compares typed outputs against DuckDB in a10GiB scope on CPUs0–3. The ordinary
dense decimal case passes. Both frozen658 and frozen653 reject nullable keys
when the planner chooses direct dense floating aggregation. With the full signed
Int64 key domain, frozen653 crashes in decimal shared merge;658 returns a named
out-of-domain error instead. Neither completes that valid query.

A separate six-row Parquet probe isolates all-NULL aggregate inputs without
NULL keys. Frozen658 returns SUM0and AVGNaN for that group; DuckDB returns NULL
for both. Nonempty and actual-zero groups agree. Its log proves direct dense
execution. Equal row counts did not detect this bug.

Evidence is under `.scratch/aggregate-state-arena-repair/arena-domain-probe-01/`,
`arena-domain-control-653/`, and
`.scratch/dense-domain-repair/null-input-control-658/`. These are reproduced
failures, not hypotheses or performance regressions introduced by the arena.
The earlier `dense-float-oracle.py` is a non-null primitive arithmetic probe;
its saved plans are Project/Sort/MemoryTableScan. It is not dense aggregate
coverage despite the potentially confusing name.

## Root causes and correction

Three dense-layout selectors narrowed `(max - min + 1)` to u64 before checking
its size. The full signed Int64 domain spans2^64values and wrapped to0, falsely
selecting a tiny direct-address allocation. `physical/dense_domain.rs` now
checks the inclusive range in i128 before narrowing. General/raw-float shared
merge and direct Parquet/native selection share this bounded-domain contract;
oversized ranges take their existing hash/general route. Observed dense keys
are separately range-checked before array indexing, so metadata cannot justify
an unchecked subscript.

Direct dense accumulation now reserves a separate trailing NULL-key slot; it
never steals a valid integer bit pattern. Per-batch index scratch is admitted
under the owning query pool. Output walks slot IDs and constructs nullable key
arrays, avoiding a synthetic key outside the domain (including at i64::MAX).

SUM accumulators now have reservation-accounted seen-value bitmaps. All-NULL
SUM groups emit NULL, while genuine zero sums remain zero. AVG uses its existing
non-NULL count to emit NULL when no value was seen. COUNT(expr) and COUNT(*) keep
their distinct semantics. These changes apply to both Parquet and native dense
execution. No SQL/query-ID dispatch, production limit increase or dependency
change was introduced.

## Validation and acceptance boundary

The default source passes730library tests and28selected integrations. The
Lance/GPU source passes792library tests and46integrations. Isolated IPC and eight CUDA tests also pass, yielding847unique selected passes;
only the existing flatten_exists ignore remains. New tests cover raw decimal
and bare-float shared merges at both signed extremes; checked small domains near
both endpoints; and actual Parquet dense aggregation with NULL keys, all-NULL
inputs, real zeros, SUM/AVG/count, Int64/Int32/Date32 keys and a NULL slot next to
i64::MAX. Existing real-spill decimal and arena admission regressions still pass.

Frozen658's other optimized semantic probes and both250million-row DISTINCT
spill caps passed (398/396MiB peaks,1000003exactgroups,3855541894spill bytes).
Those cap cases intentionally bypass fused aggregation and do not certify the
arena or new dense path. No658performance measurements were accepted or run
after the new correctness failures. Preserve its source/binary hashes and all
failure traces as a control; do not describe it as fully validated.

The corrected optimized build and both exact domain probes now pass with
explicit runtime-path evidence. General optimized and decimal gates also pass;
resource/GPU and provider performance gates remain required. Integer aggregate overflow/type width, broader nested state
and output ownership, and replayability contracts remain separate open work;
this report does not claim complete SQL or query-wide resource coverage.


## Frozen corrected candidate and archived control

Frozen662source archive SHA256:
`ceacdc98e8695b7c5eb90ce0aa815e8358261f2bf8a8aa8621d69ae2202744ab`;
manifest SHA256:
`57ed1a118c7d6c271350e972babf708bebdba3f25ef547b1b0d39546952e2569`.
All276current source/test/example/benchmark inputs are covered and unchanged
since freezing. Its optimized build completed in10m50s under64GiB/jobs1, with
actual cgroup/process observation preserved. Benchmark binary SHA256:
`8baa8eead1565c10997bb839165a40b9f5bac80f5f9ebd8a0e41b57b3f0062a9`.
Cap binary SHA256:
`c3269232e91cab5b94a1a53bcd10e878c663327fdb5c8bc9cb4c6766779fb24d`.
Both copied binary hashes were independently rechecked.

All four140000-row domain cases now match DuckDB, with runtime logs proving
dense execution on small domains and hash merging on full signed domains. The
six-row direct-dense reproducer now emits NULL/NULL for all-NULL SUM/AVG, while
preserving real zero and COUNT semantics. The full optimized semantic driver
and18decimal comparisons/two overflow refusals exit0. Eight known primitive
integer-division differences remain unchanged and explicitly excluded from
claims of full DuckDB semantic equivalence. No performance conclusion follows
from these correctness checks.

The prior658source, debug/optimized/cap results and failing domain controls are
archived in [the659-file evidence package](benchmarks/2026-09-07-aggregate-arena-domain-findings/).
Every member was verified; manifest SHA256:
`08cfe55f6e43f0f7eb5e47088269e34e4c8f2b6ac4ce8eb0ec69df97cc8f7cc6`.
Actual executables are excluded; their hashes and frozen source are retained.


## Optimized resource and GPU follow-through

The corrected662cap binary passes both250million-row real-spill scenarios:
1GiB cgroup peak395MiB and2GiB process data cap peak397MiB. Both emit1000003
exact groups and account3855541894spill bytes. These DISTINCT fallback cases
remain intentionally separate from fused-arena admission coverage.
The supported resident-float GPU fixture completes40CPU-control and40device
requests with typed correctness, the time gate, and per-request proof of one
successful device execution. It does not establish GPU support for all SQL.
Cross-provider component profiling against frozen653 is terminal, with the
failed control case described below. No new performance acceptance is claimed.


## Component results: a narrower success and a preserved failure

The instrumented 653/662 comparison reached all 15 provider/query cases. It
attempted 113 of 120 planned requests: 112 passed typed DuckDB comparison and
the fixed time ceiling. Native Q1's first control request timed out; that case
stopped before its remaining seven requests, including all candidate requests.
The driver exited 1. This is not a complete component gate.

The dominant Q18 interval medians were:

| Track | Control ms | Candidate ms | Candidate/control |
|---|---:|---:|---:|
| Native finalization/filter/destruction | 266.272 | 54.882 | 0.206 |
| Raw Parquet merge/output/HAVING | 487.921 | 491.201 | 1.007 |
| Iceberg merge/output/HAVING | 482.331 | 496.103 | 1.029 |
| Lance finalization/filter/destruction | 247.730 | 57.274 | 0.231 |
| Extra IPC finalization/filter/destruction | 273.018 | 52.126 | 0.191 |

These intervals differ by provider route and must not be summed as exclusive
CPU time. The arena removes substantial finalization cost in native/Lance/IPC;
it does not reduce the measured raw Parquet/Iceberg merge interval. Q13 also
shows lower finalization intervals across providers, but Iceberg Q13's query
median is 1.054 times control. Raw Q1's query median is 1.050 times control.
Three steady instrumented samples cannot establish latency acceptance.

Native Q1's control deadline was 1092.704 ms, calibrated from three DuckDB
requests near 109 ms. Before timeout the sampled process accumulated 865 major
faults and 580 ms CPU over about 1203 ms of observation; no completed query
response or candidate measurement exists. The cgroup reports no OOM events.
These observations support investigation of startup/first-touch effects, but
do not prove the cause or justify relaxing the deadline. The original logs,
partial counters, typed outcomes and missing requests remain preserved.

The uninstrumented full canonical SF10 comparison is now running separately
with unchanged 653/662 binaries, all 22 queries, five CPU tracks, and four
startup/execution-order combinations (9680 planned engine requests). It retains
the same 10-times-DuckDB time ceiling. The prepared three-query screen is not
also being run: the full screen includes those queries. Historical protected
Lance Q17 regression remains open and cannot be cleared by the Q18 improvement.


## Reproducible checkpoint archive

The [validation/component evidence](benchmarks/2026-09-07-dense-domain-and-arena/)
contains 1255 member-verified files. Manifest SHA256:
`da27e7f81a064050352b026a765c526b0e750efd2c80d9c33146c7768db64ebf`.
It includes frozen source and executable hashes, all typed probes, test/cap/GPU
logs, all component requests and the failed native Q1 control. Full SF10 results
are excluded while their separate run remains active. No new engine source or
dependency change was made during this optimized validation checkpoint.


## Halfway full-screen checkpoint (later progress linked below)

At the halfway checkpoint, ten of 20 cells independently audit 4840 requests. Each cell has all 22 queries,
one warmup and ten steady samples per side. All audited requests pass typed
comparison and time gates. Lance Q20 has an open median regression flag of
1.113272 (182.622 to 203.308 ms), while its mean ratio is 1.001293. The other
first-order cells have no median/mean flags above 1.10. The table below describes the first startup/execution ordering. The second IPC
cell also passes without flags (suite0.946858, geomean0.978243); other orders
have not yet established reproducibility. Second raw Parquet also passes without
flags (suite0.995957, geomean0.992232). Second native passes without flags
(suite0.956493, geomean0.982460). Second Iceberg passes without flags
(suite1.001360, geomean0.998355). Second Lance also passes without flags
(suite0.977551, geomean0.984959). Q20 reverses its median direction in that cell:
0.899984 (203.479 to183.128ms), with mean1.006270. The first flag is preserved,
not confirmed or erased. The third ordering had started at this checkpoint.

| First-order track | Suite candidate/control | Geometric mean | Q18 median candidate/control | Suite candidate/DuckDB |
|---|---:|---:|---:|---:|
| decoded_ipc | 0.959145 | 0.989587 | 0.720479 | 0.442269 |
| raw_parquet | 0.995191 | 0.995160 | 0.919798 | 2.344383 |
| native | 0.958845 | 0.988550 | 0.726583 | 3.367155 |
| iceberg | 1.000592 | 0.995876 | 0.958951 | 0.316298 |
| lance | 0.946276 | 0.968229 | 0.754099 | 1.257866 |

IPC is an extra residency track and is excluded from required CPU-track weighting.
Reference setup was source-checked to register the same Arrow files through
PyArrow, rather than substitute raw Parquet. Iceberg uses its direct extension
reader; mode scores must remain separate. The Lance reference disables its
known-incorrect extension optimizer for all queries, as recorded by the provider
setup. It is direct Lance scan plus DuckDB aggregation, not stock extension
pushdown performance. Raw Parquet and native still trail
DuckDB substantially. A large Q18 component gain translates into a much smaller
whole-suite gain; do not extrapolate a general engine speedup from that phase.
Native Q1 completed both sides in this full run, but the original diagnostic
control timeout remains preserved as a failed request.

The independent prefix audit is
`.scratch/dense-domain-repair/balanced-audit-10.json`. Prior immutable prefix
audits remain available. Ten cells, evaluation of the new Lance Q20 flag, and the historical protected follow-up
remain outstanding. The prepared protected driver compares the current candidate
against frozen647 using the original flagged membership, 50 samples and four
orders; it cannot launch before the full20-cell audit completes. No full provider,
resource, concurrency, holdout, public-workload or leadership gate is closed.


The [generated full-progress table](../.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-dense-domain-full-progress.md)
tracks subsequent audited prefixes without replacing historical cell outcomes.
