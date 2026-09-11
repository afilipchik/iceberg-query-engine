# Scalar-subquery phase follow-up

Latest outcome: the shared subquery-pruning candidate reduces Q22's carried input
from ~72 MB to 6.2 MB. Ten-pair untraced screens show Q22 20.5% faster than the
recent release and 12.1% faster than the scalar control, all 88 requests valid.
Five-mode canonical validation completed; this is not leadership acceptance.

| Mode | Suite candidate/control | Geometric mean |
|---|---:|---:|
| Decoded IPC | 0.9634 | 0.9743 |
| Raw Parquet | 0.9889 | 0.9872 |
| Native | 0.9769 | 0.9664 |
| Iceberg | 0.9886 | 0.9924 |
| Lance | 0.9462 | 0.9435 |

All 880 screen requests passed typed comparisons and fresh DuckDB time gates.
Each query has three steady pairs plus warmup; these are screens, not three-session
leadership certification. Ten-pair rechecks in two fresh sessions cleared IPC Q16
(1.0154/1.0114) and Lance Q6 (1.0230/0.9748), Q8 (1.0019/1.0062).
Lance Q14 remains unresolved: 1.1655/1.0937. Its saved plans are identical and it
has no expression subquery. Median planning rises from about 110 to 121 ms;
execution medians are similar. Full query timing also includes output/cleanup
outside current phase metrics. Attribute provider planning and cleanup before
changing operators; never subtract this work from the benchmark boundary.

Required GPU validation used the separate 600,000-row float fixture, not canonical
SF10: the corrected run verifies a 256 MiB effective cache target in every measured
GPU sample. A previous run used an ignored environment key and therefore the
24 GiB default; its evidence is retained with that actual setting. Both 1 MiB
capacity probes refused preparation by name. Cache targets are not hard VRAM caps.

Frozen candidate binary SHA-256:
`836fc368f37ef72cf992147d15531f8876778c75f0b26e15da4feb01a2965038`.
The [evidence archive](benchmarks/2026-09-06-subquery-projection/evidence.tar.gz)
contains 4,032 verified members, including exact source/binary and all screens,
rechecks, traces and GPU runs. Its SHA-256 is
`a6949b92f36a9fb93c01d27104602166594dc7cf9cf4769692ebdc64b7ced73e`.
Current production source is newer by the separately tested SUBSTRING/VALUES fixes.

## Fresh paired recheck and tracing checkpoint

The canonical SF10 decoded-IPC regression reproduces with ten steady pairs per
query plus one warmup per side. All 44 requests pass typed validation and the
fresh DuckDB 10× ceiling. The preserved scalar control is compared with the
passing GPU count-star release, with GPU disabled in both workers. This is a
two-query diagnostic subset, not suite acceptance.

| Median milliseconds | Scalar control | Current measured release | Ratio |
|---|---:|---:|---:|
| Q22 full query | 41.9301765 | 51.1439300 | 1.21974 |
| Q22 planning | 14.1838150 | 18.5254620 | — |
| Q22 execution | 26.7434065 | 31.4982265 | — |
| Q11 full query | 17.8974440 | 19.5703135 | 1.09347 |
| Q11 planning | 14.9894445 | 16.6355790 | — |

The [archived evidence](benchmarks/2026-09-06-scalar-rhs-recheck/paired-evidence.tar.gz)
contains every request, result, plan, reference and worker log. See
[provenance](benchmarks/2026-09-06-scalar-rhs-recheck/provenance.json) for binary
digests and the exact command. Archive members were checked against originals.
Settings: 16 threads/CPUs 0–15, 40 GiB query budget, 48 GiB process limit,
96 GiB cgroup; no build overlapped measurement.

Current source adds opt-in `QE_SCALAR_SUBQUERY_TRACE=1` diagnostics in
`physical/operators/subquery.rs`: cache events, actual RHS tree/partitions,
physical construction, bridge/runtime acquisition, execution, collection and
normalization phases. Failed phases are explicit. Timings overlap; do not sum
them or use traced debug timings as acceptance measurements. Default-off tracing
does not traverse plans or take timestamps. Cache and SQL behavior are unchanged.

Validation: four subquery unit tests with tracing off, two cache/error tests
with tracing on, and 18 numeric/materialization integration tests with tracing
on pass (24 executions, no skips). Structured log checks confirm three misses,
one hit, and two failed physical-planning phases with no failed-result caching.
Formatting and diff checks pass. Logs and frozen source are in
`.scratch/scalar-attribution-run/`. Only `subquery.rs` differs from the preceding
600-file production/test/harness source snapshot.

Both debug SF10 attempts exceeded the existing deadlines before emitting an RHS
trace; they are preserved as inconclusive diagnostic timeouts, not valid samples.
The optimized same-feature tracing build subsequently passed. No CPU
optimization or cause of the regression has yet been established. Source review
confirms that the nested planner inherits the memory pool and configuration;
there is no separate physical-planner partition setting that it forgets to copy.

### Optimized RHS trace: six valid requests

Release build 81396 completed in 8m22s. Frozen binary SHA256
`0ebe830e84be602c940f7feae83531ea9da166e269c6e60ddcc057a2891061b2`,
features `lance,gpu`; GPU disabled. Runner 81752 completed with exit zero.
Three requests per query, fresh workers per query and three fresh DuckDB
calibrations per query: all six engine outputs pass typed comparison and exact
time gates. The first request is cold initialization; none of these traced
samples establishes an acceptance speedup. Executable sources match the frozen
snapshot; the report was updated after that snapshot and recorded separately.

| Warm diagnostic phase (two requests) | Q22 ms | Q11 ms |
|---|---:|---:|
| RHS physical construction | 0.0060–0.0061 | 0.0091–0.0093 |
| Bridge dispatch | 0.0304–0.0354 | 0.0588–0.0993 |
| RHS execute | 17.495–17.772 | 1.233–1.268 |
| Final stream collection | 0.0017–0.0124 | 0.0077–0.0084 |

Q22's actual RHS tree is Project(1) → SpillableHashAggregate(1) → Filter(16)
→ MemoryTableScan(16). Q11 is Project(1) → SpillableHashAggregate(1)
→ MemoryTableScan(16); this is an already materialized input, not the full
upstream work. Both report 16 Rayon threads, visible CPUs and Tokio workers.
Each independent SQL request has one scalar cache miss, with no retry/error.

Thus the current Q22 RHS cost is execution inside the reported planning phase,
not nested physical-plan construction or OS-thread dispatch. This is not yet
a causal comparison of the old and new RHS kernels: only the current source has
this trace. Q11's much smaller scalar RHS cannot explain all its planning time;
CTE materialization needs separate attribution.

Next: instrument the generic aggregate input queue and scalar aggregate drain/
reduction boundary, including prepared/static/unknown mode, slots, copied bytes,
upstream polling and permit waits. Source comparison confirms the scalar control
forwarded queued batches, while current code adds explicit reservation and owned
copies. Measure that cost before changing it. Preserve admission, cancellation,
error propagation and SQL semantics; do not simply restore the old queue.

[Trace evidence, exact binary, source and tests](benchmarks/2026-09-06-scalar-rhs-trace/SHA256SUMS)
are preserved alongside the paired recheck. No heavy job remains after archival.

## Queue attribution follow-up

Current follow-up adds opt-in `QE_INPUT_QUEUE_TRACE=1` in `spillable.rs`.
It reports queue preparation (prepared/static descriptor, bound, slots and
envelope), per-producer upstream execution/polling, admission waits, charged copy
bytes, copying and sends, followed by aggregate drain and delegate execution.
Producer timings overlap; charged copy bytes are admission estimates, not exact
allocator/RSS measurements. Partial/error/panic/cancellation records are explicit.
It does not change queue policy, SQL or memory admission. Per-batch diagnostic
bookkeeping can perturb short timings; acceptance measurements must disable it.

The queue suite passes 28 tests with the switch off and the same 28 with it on.
Another 26 encoding/numeric/materialization tests pass with it on, no skips.
Structured records verify serial/parallel, prepared and interrupted paths.
Logs and source snapshot: `.scratch/aggregate-input-attribution/`.
The optimized diagnostic build 49554 and run 8498 completed: six requests pass
fresh typed/time gates. [Archived binary, source, tests and raw traces](benchmarks/2026-09-06-aggregate-input-trace/SHA256SUMS).

Q22 has 16 admitted slots, a 12,370,472-byte per-slot bound and a
197,927,552-byte envelope; its input is not serialized by the demand gate.
Warm drain takes 17.002/16.737 ms; delegate execution takes 1.718/1.720 ms.
The queue forwards 381,776 rows, charges 71,671,496 copied bytes and retains an
estimated 71,966,654 bytes across 23 batches. Concurrent producer copy-time sums
are 47.122/49.648 ms and polling sums 146.543/146.110 ms: these overlap, not
exclusive wall-time costs. Q11's scalar drain is 0.978/1.068 ms and delegate
execution 0.835/0.815 ms, with 304,774 rows and 7,329,360 charged copy bytes;
its earlier materialization remains separate.

### Shared subquery projection repair — implemented, performance pending

`SubqueryExecutor` passed scalar/IN/EXISTS plans directly to physical planning.
The outer projection rule intentionally does not recurse into their independent
scopes. Q22's scalar filter consequently retained the full customer schema.
The new shared `plan_subquery` boundary applies existing `ProjectionPushdown`
dependency analysis before provider scanning. Original logical plans remain cache
identities; memory admission, cardinality and collection rules are unchanged.
No query-specific rewrite or additional cost/semantic-proof rule is introduced.

Scope testing exposed an omission in the existing projection rule: correlated
outer references were collected for EXISTS/scalars but not IN. IN now retains
its outer dependencies too. Before this repair the nested regression returned
`NotImplemented("nested or unresolved correlated subquery scope")`; afterwards it
returns the independently expected sum 104. The refusal was a symptom of losing
the needed outer field, not accepted coverage or proof of unsupported SQL.

Six focused tests cover provider projection indices, multiple batches,
duplicates, NULLs, empty EXISTS, cache hits, scalar cardinality, repeated pruning
and nested correlation. Full default library plus seven selected integration
targets pass 715 tests. Two pre-existing library ignores remain: the flatten
dependent join test and the dedicated IPC-sidecar process test. Logs and frozen
source: `.scratch/subquery-projection-repair/`.

Candidate release build 41822 passed. Binary SHA256
`836fc368f37ef72cf992147d15531f8876778c75f0b26e15da4feb01a2965038`,
source archive SHA256 `dc9208d7175d246fa33673085520e1cf39518cbcd6b736269eb1d566a54c79f6`.
Executable source hashes were verified before execution; later documentation
edits are recorded separately. The dedicated IPC-sidecar test also passes,
bringing selected passing tests to 716 with only the historical flatten test
still unexecuted/ignored in this gate.

Trace 92848 passes six requests. Q22 now carries an estimated 6,156,147 bytes,
with a 1,057,320-byte slot bound and 16,917,120-byte envelope across 16 slots.
Its warm drain is 7.090/6.573 ms; delegate execution is 1.663/1.666 ms. This
supports reducing carried columns as the mechanism, while preserving admission.

Paired runner 44325 completes with exit zero, ten steady pairs per query/control
plus warmups. All 88 requests pass fresh typed/time gates. Tracing is off.

| Control | Query | Before median ms | Candidate median ms | Candidate/control |
|---|---|---:|---:|---:|
| Recent count-star release | Q22 | 49.1035555 | 39.0146255 | 0.794538 |
| Recent count-star release | Q11 | 18.4535060 | 19.4692830 | 1.055045 |
| Preserved scalar control | Q22 | 43.1155010 | 37.9051915 | 0.879155 |
| Preserved scalar control | Q11 | 18.0499215 | 18.2339570 | 1.010196 |

These are two-query development screens, not full-suite acceptance. All samples,
plans, typed outputs and provenance remain in
`.scratch/public-bench/subquery-projection-vs-{count-star,scalar}-01/`;
trace `.scratch/subquery-projection-trace/`, frozen binary/source and logs in
`.scratch/subquery-projection-repair/`. Publication archives follow the ongoing
latency run so compression does not overlap measurements.

Broader runner 13517 is active: all 22 canonical SF10 queries, three steady pairs
plus a warmup per query, sequential decoded IPC/raw Parquet/native/Iceberg/Lance,
paired against the recent count-star release. The required GPU residency rerun,
broader resource/concurrency/public/holdout gates and other systemic failures
remain outstanding. No overall DuckDB leadership claim is made.

## Historical source593 screen

Read-only investigation; no engine, build, benchmark or test executed. Evidence:
`.scratch/public-bench/prepared-rowstore-screens-01/ipc_vs_scalar/attempts.jsonl`.
Three non-warmup samples per side; `before` is accepted scalar control and `after`
is the frozen row-store candidate. These small screening samples are not a new
baseline or CPU profile.

| Median ms | before | after | delta |
|---|---:|---:|---:|
| Full query | 43.244243 | 49.404501 | +6.160258 |
| plan_ms | 14.054956 | 18.829820 | +4.774864 |
| execute_ms | 28.142831 | 29.554180 | +1.411349 |
| optimize_ms | 0.829401 | 0.816264 | -0.013137 |
| parse_ms | 0.104387 | 0.105921 | +0.001534 |

Independent medians need not add exactly. Most of the observed delta sits in
`plan_ms`, not optimizer time. The saved physical tree is identical on both
sides: Sort → Project → Aggregate → Project → Filter → Anti Join, with a filtered
customer MemoryTableScan and orders MemoryTableScan. The logical plan still
shows a scalar subquery above the Anti join; the display does not expose the
scalar RHS physical tree. The Anti output is one partition, so its immediate
consumer queue bypass is not evidence of a multi-partition serialization bug.

## Concrete source mechanism

`src/execution/context.rs:1548–1567` constructs a fresh physical planner,
enables subquery execution, creates the physical plan, and adds the complete
interval to `metrics.plan_time`. `src/physical/planner.rs:390–444` calls
`precompute_uncorrelated_scalars` while constructing a Filter. An uncorrelated
scalar invokes `SubqueryExecutor::execute_scalar` synchronously and becomes a
literal on success. Therefore actual RHS scan/filter/AVG execution is included
in reported planning time; the saved outer physical tree omits that work.

`src/physical/planner.rs:657–666` creates a new executor for each query;
`src/physical/operators/subquery.rs:123–134` initializes its empty cache.
`execute_scalar` at lines224–277 checks this per-query cache, constructs another
physical planner on a miss, fully collects the RHS, validates scalar cardinality,
and caches the result. It does not share a scalar result across independent SQL
requests. That freshness is intentional; a persistent result cache is not a
safe optimization without an explicit snapshot/version contract.

`subquery.rs:36–55` starts and joins an OS bridge thread per uncached execution,
then collects every declared output partition sequentially on the shared runtime.
`subquery.rs:22–31` sizes that runtime with `num_cpus::get().max(2)`, independently
of requested main-pool thread configuration. These are shared overhead/concurrency
mechanisms, not a demonstrated explanation for the *difference* between binaries.
Scalar RHS child queues or changed expression/materialization kernels can add
runtime cost inside plan_ms even when the outer plan and optimizer timings agree.
The available evidence cannot isolate which one accounts for +4.77ms.

The precompute error branch (`planner.rs:439–441`) retains the original expression
rather than returning the error. A later evaluation can retry failed RHS work.
This is a concrete generic duplicate-work possibility; no Q22 sample proves that
branch occurred, and successful normal execution should use the cached literal.

## Smallest discriminating trace

Instrument generic scalar cache misses, not SQL/query identifiers: separately
record cache hit/miss, RHS physical-plan construction, RHS execution/collection,
scalar normalization, actual RHS physical tree and each operator partition count.
Inside the RHS aggregate input queue record direct/serial/prepared selection and
copied bytes, with effective Rayon threads and subquery-runtime threads. Preserve
outer plan_ms and execute_ms, request process CPU and wall boundaries. Existing
CTE_DEBUG only logs entry before cache lookup and cannot establish misses/retries.

Use one unchanged SQL request per fresh process for each frozen binary, then a
small alternating repeat at the existing settings/typed oracle/time ceiling.
Do not subtract RHS work from the timed query or prewarm it out of the boundary.
If the +4.77ms is in RHS execution, investigate its actual queue/kernel path; if
in RHS physical construction or bridge startup, investigate initialization.
Only after this separation consider a general async scalar initialization path
that preserves cardinality, NULL/type/error timing and per-query ownership. No
scalar-result caching across mutable contexts, and no queue change inferred from
the outer Anti tree alone.

## Lance Q14 existing-trace follow-up

A fresh contained diagnostic using `QE_LANCE_TIMING=1` validates all six requests
against a newly calibrated typed DuckDB reference. It runs three requests per
frozen binary sequentially; it is not alternating performance acceptance and does
not clear the two earlier paired regressions. The scan requests themselves match:
2,000,000 part rows and **59,986,052 lineitem rows** per query. Warm lineitem scan
times are 105.6/112.6 ms in the control and 102.3/104.8 ms in the pruning candidate.
The earlier planning slowdown is not reproduced here. Warm full-query times are
266.7/274.5 versus 255.9/255.4 ms; unaccounted phase time is roughly 4.7/3.8 versus
3.6/3.7 ms, not the large residual seen in the earlier paired session.

[Trace archive](benchmarks/2026-09-06-subquery-projection/lance-q14-trace.tar.gz)
preserves scans, plans, outputs, reference checks and exact driver. Startup
statistics scans precede query scans and must not be counted as query execution.

Source hypothesis: `LanceTable::plan_pushdown` multiplies estimates for flattened
conjuncts. Two bounds on the same column are correlated, so multiplying their
individual fractions can substantially overestimate a narrow interval. The
estimator handles BETWEEN by subtracting cumulative fractions, while an equivalent
AND range uses independence. This is a shared costing inconsistency, not a semantic
proof and not yet a measured explanation of the binary-to-binary regression.
The completed same-binary forced-pushdown diagnostic validates six requests but
is slower: warm full-query 434.8/438.4 ms versus default 262.0/257.1 ms. Forced
lineitem scans return **28,117,026 rows**, not a month interval. The saved optimized
predicate is `(l_shipdate >= DATE(9374)) AND
(l_shipdate < CAST('1995-10-01' AS Date32))`. The constant CAST remains unevaluated;
Lance can render only the lower bound. Therefore same-column interval costing is
a source-level inconsistency, but missing constant normalization is the immediate
capability boundary in this query. Do not enable forced pushdown as a fix.

Next implementation: audit shared constant CAST evaluation and its error/type/NULL
contract; normalize only expressions proven constant through the ordinary strict
evaluator. Regress both literal and CAST spellings across providers, invalid casts,
NULLs, and signed date domains. Then measure full-interval pushdown and repair
same-column interval estimates if useful, with a multi-selectivity range workload.
This diagnostic does not explain or close the binary-to-binary Lance Q14 regression.

[Verified forced-pushdown evidence](benchmarks/2026-09-06-subquery-projection/lance-q14-pushdown.tar.gz) preserves both settings, all six requests and scan traces.
