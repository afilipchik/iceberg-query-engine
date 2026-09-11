# Preserved agent checkpoint history — 2026-09-06

Historical status prose moved verbatim from AGENTS.md during the source591 release. Later entries supersede earlier active-job statements. Relative links below retain their original repository-root base. Current status lives in AGENTS.md and the execution ledger.

## Current source and evidence — 2026-09-06

Current source, tests and frozen evidence take precedence over earlier checkpoints.
No dependencies changed during this continuation. Detailed history remains in
[the execution status](.claude/epics/realistic-benchmarks-duckdb-leadership/execution-status.md).

- Numeric inference and execution share exact decimal contracts. CAST/TRY_CAST,
  CASE, Boolean NULL logic, membership and dictionary normalization have typed
  regressions. Structural proofs replace estimated uniqueness. See
  [systemic contracts](docs/systemic-contracts-2026-09-06.md) and
  [scalar execution](docs/boolean-and-scalar-contracts-2026-09-06.md).
- Hierarchical reservations cover selected owned buffers and shared input queues;
  full query ownership remains incomplete. Queues copy exposed Arrow buffers,
  validate actual charges and own producer tasks. Unknown paths retain serial
  demand. Do not remove memory admission to recover parallelism.
- HashJoin initialization owns partition tasks and Semi/Anti prefetch. Inner
  probing is pull-driven with at most 4,096 candidates per step. Prepared Inner
  output completes both build phases before outer queue admission and owns
  unpulled probe streams. Single columnar caches with certified probes qualify;
  unsupported, spill and row-store paths decline. Join spill initialization owns
  its directory immediately and transfers it to the final SpillState owner.
  Aggregate/sort now share `SpillDirectoryOwner`; sort moves it into its blocking
  worker. `OwnedTaskOutputStream` observes worker completion/errors before EOF;
  `SortFetchStream` stops without polling beyond a fulfilled fetch. Both operator
  ingestion leaks reproduced before the patch; selected683tests pass, one
  pre-existing ignored. Spilled join output now also owns its producer; per-call
  `ProbeSpillFiles` ownership follows phase-A writers and phase-B workers,
  retaining memoized build state until last use. Panic and cancellation failures
  reproduced before this additional patch; final selected687tests pass with one
  pre-existing ignored. Frozen source580 release c4d05ab7 passes six spill cap
  cases but fails its completed provider matrix; do not attribute it to newer source. See [spill output evidence](docs/benchmarks/2026-09-06-spill-output-ownership/README.md).
  See [prepared ownership](docs/prepared-input-ownership-2026-09-06.md).
- Prepared Filter and Column/Alias Project now retain initialized streams and
  checked layout alternatives. Unknown transformed bounds use the same streams
  through serial admission. Combined library, explicit IPC-sidecar and selected integration gates pass:
  704 unique tests, one pre-existing ignored. Physical scan emission pruning is now
  integrated separately; it retains predicate reads, Project semantics and cache
  routing. Release and six caps pass; all-query screens retain substantial
  protected regressions. See [current regression map](docs/prepared-pipeline-regressions-2026-09-06.md).
- `fixed_width_output.rs` enforces raw fixed-width row/type/buffer extents before
  copied-output admission. Independent validity offsets are preserved; unaligned
  Boolean values need a bounded bitmap repack. General pool-independent queue
  and gather capabilities are distinct from resident input and propagate through
  audited Filter/column Project. Existing IPC routes decline raw bounds without
  switching providers. Raw projection restores requested order and duplicates.
- Build-only take bounds omit source identity payload but retain dictionary
  children. Index validity must be bounded explicitly: Arrow can retain an entire
  sliced index bitmap. Inner uses fresh nonnullable indices. The identity-safe
  gather helper requires compact/nonnullable index validity. See
  [streaming contracts](docs/streaming-fixed-width-contracts-2026-09-06.md).

### Measured source versus current candidate

| Evidence | Status |
|---|---|
| [Accepted scalar candidate](docs/benchmarks/2026-09-06-scalar-comparison/) | Six modes × 660 canonical SF10 pairs pass. Suite ratios versus DuckDB: native 3.358×, IPC 0.525×, Iceberg 0.316×, Lance 1.260×, raw CPU 2.258×, GPU routing 2.255×. Canonical GPU executes on CPU; separate supported-float smoke has 40 device samples. |
| [Prepared Inner candidate](docs/benchmarks/2026-09-06-prepared-inner/README.md) | IPC screen: 110 exact comparisons pass; Q14 recovers 82.6 vs 85.0 ms control. Six development cap cases pass with spill. Raw Q9 remains 25.8% slower and Q14 times out; 66 completed raw attempts validate. Overall performance acceptance fails. |
| [Current streaming candidate](docs/benchmarks/2026-09-06-streaming-fixed/README.md) | 674 selected tests pass, one pre-existing ignored. Explicit IPC-enabled routing/resource gate and formatting pass. Release and six development spill cap cases pass. Raw/IPC screens validate220/220; summed medians 0.995×/1.017× scalar control. Prior raw Q9/Q14 regression is absent; full matrix remains pending. |
| [Spill output candidate](docs/benchmarks/2026-09-06-spill-output-ownership/README.md) | 687 selected tests pass, one existing ignored; release and six cap cases pass. Matrix terminal: 3,300 attempts, 54 failures across native/IPC/raw CPU. Iceberg and Lance pass typed/time gates; performance regressions remain. Canonical GPU unrun because CPU control failed. Supported smoke: 39 device samples and one CPU sample. No full acceptance. |

Never attribute an earlier matrix to newer source. The paired comparator preserves
exact typed results, failures, plans and provenance; public JOB/ClickBench evidence
is development extracts, not full workload certification. Lance reference runs
require the documented extension-wide optimizer control for its decimal AVG bug.
See [provider evidence](docs/benchmarks/2026-09-06-canonical-provider-sf10/README.md)
and [comparator contracts](docs/benchmark-comparator-scaling-2026-09-06.md).

GPU wrapper partition fix is now integrated after that archived source.
Four focused hardware-independent tests pass (two pre-patch failures reproduced);
multi-output delegates stay on CPU and invalid partitions error before routing.
See [GPU partition evidence](docs/benchmarks/2026-09-06-gpu-partition-contract/README.md).

Next: prepared pipeline source581 passes704 selected tests, release and six cap
cases. Five CPU screens62780 are terminal:880/880typed/time-valid requests,
but suite regressions1.071–1.269×scalar reject performance acceptance. Q19 raw/IPC
improved; Q10/Q12/Q16/Q21 and other protected regressions remain. Archive2018
verified all2891files and both frozen binaries. Nested bounded-Inner gather propagation, GPU partition guard and exact decimal
root reuse are integrated. Combined gates pass724tests (one pre-existing ignored),
including explicitIPC. Decimal reuse identity preserves coefficient/scale after
a new regression caught generic Expr equality returning the wrong physical type.
New release, caps and performance remain pending.
Follow the exact live session in the epic/runtime evidence; do not restart a job
because its log is quiet. Remaining work includes variable-width raw bounds,
nested/computed prepared pipelines, remaining producer cancellation/error paths, source/
decoder/result memory ownership, GPU hard-budget admission, optimizer predicate
growth and broader provider/public/concurrency/leadership gates. Relevant audits:
[ownership](docs/next-memory-ownership-2026-09-06.md),
[GPU](docs/gpu-resource-contract-investigation-2026-09-06.md),
[GPU admission design](docs/gpu-hard-admission-design-2026-09-06.md),
[optimizer](docs/optimizer-overhead-investigation-2026-09-06.md).

2026-09-06 continuation review: preserved all dirty session work. The source582 release was deliberately stopped (session79142, exit143) before publication after new semantic regressions were isolated. Baseline projection composition tests reproduce three failures; direct typed decimal substitution tests reproduce four failures, with two positive controls passing. Ordinary dotted SQL literals bind Float64 and the earlier decimal expectation fixture is invalid evidence; explicit CAST SQL controls replace it. Projection identity guard is being integrated; global typed AST identity and binder name fallback remain under investigation. No new performance result belongs to this source.

Shared expression substitution follow-up: projection identity, typed Expr equality and removal of binder display-name fallbacks pass740unique selected tests (one pre-existing ignored). Additional real SQL regressions then reproduced two wrong-value aggregate-label collisions: SUM/SUM DISTINCT returns4,4 instead of4,3; two CASE sums return2,2 instead of2,1. Internal aggregate-field uniqueness repair is pending. Display labels, numeric equality and execution-slot identity are distinct contracts; ORDER BY ordinals currently lower to name references and require a broader slot-identity audit. Do not report the earlier740pass gate as closure of these subsequent failures.

Final expression/output identity checkpoint: internal aggregate collision handling, hidden-sort positional restoration and empty DISTINCT SUM semantics are integrated. Frozen585sourceSHA5a802b78737f68ce3e7501912c8d962eb84fc324da00ad88b560cef8a354572b passes750unique tests (one pre-existing ignored), including actual-spill typed oracles and explicitIPC; fmt and post-test source hashes pass. Release1693 is active under64GiB, log .scratch/expression-substitution-release-01.log; do not start a competing heavy job. Caps/performance remain pending; broader ordinal/slot identity and query-wide memory gaps remain open. See [identity findings](docs/expression-output-identity-2026-09-06.md).

Frozen585 release1693 and sixcap48772 are terminal0; all6capcases actuallyspill, RSS131–287MiB (join rowcount oracle only). Pinned benchmarkSHA b2cdf0385edfbbb967f5b88b6cd627a9c94a6388532e150afcb350eeb5f51d94. FiveCPUmode all22query pairedSF10screen18829 is active, .scratch/expression-substitution-screens-01.log. Do not run competing builds/profiles/archives during latency. No new performance acceptance yet.

Screen18829 continuation: raw andIPC each176/176typed/timepass; native is active, Iceberg/Lance queued. Raw suite1.074074×scalar; IPCsuite.961507×scalar butgeomean1.021448×. Protected regressions prevent acceptance in both. Keep source585/binaryb2cdf frozen; remaining measurements and source-linked profile decisions are pending.

FiveCPUmode screen18829 completed880/880typed/timevalid requests butprotectedregressions reject performance acceptance. Archive45952 verified2891files and bothbinaries (archiveSHAf84f7849e83e8a01542e26ff7e46e9566697222aea82919ab33a121eb2d8efe3). CanonicalGPUcontrol/routing9640 is active96GiB, .scratch/expression-substitution-gpu-canonical-01.log; no competingheavyjobs. Floatdevice smoke and main-pool scaling diagnostic remain queued.

GPUcanonical9640 andfloatdevice smoke88133 terminal0:canonical132samples allCPU fallback; float40GPUdevice+40CPUcontrol samples pass. GPUarchive15466 verified850files (SHA c9c0301554b1961435ef5fccce1ab1628c68377fcf0d3962ba1e00b12c1ab201). RawCPUdiagnostic70828 active96GiB on frozenb2cdf, .scratch/expression-substitution-raw-cpu-diagnostic-01.log. It varies mainpool1vs16; nestedsubquery runtime independentlyusesdetectedCPUs, so do not call itstrictsinglethread. Fullmemory/VRAM/concurrency/public/leadership gates remain open.

RawCPUdiagnostic70828 terminal0:64typed/timevalidrequests andcompleteCPUboundaries. Q10/Q12/Q16 loseparallelscaling despite lowerCPU16work; Q1reducesCPUandretainsscaling. Rawarchive78402verified214files, SHA fc1fa246f6df5a4e7a2cfef97ec26f8b2db66e439afcb04ce8d47ccdf51f505e. IPCcontrast2589 active96GiB, .scratch/expression-substitution-ipc-cpu-diagnostic-01.log, Q10/12/16main1vs16. Source remains585; see docs/shared-cpu-regression-diagnosis-2026-09-06.md.

2026-09-06 compaction review: preserved session changes. IPC diagnostic2589 and archive7297 are terminal0:48typed/time-valid requests with complete CPU boundaries;162archived files verified. Q12 IPC scaling8.90× versus raw1.36×; Q16 IPC0.986×. Added generic examples/prepared_plan_probe.rs for initialization-only public-plan capability inspection; no production contract changes since frozen585. Build73735 active48GiB, .scratch/prepared-plan-probe/build-01.log. See shared CPU diagnosis; prepared filter capability remains unimplemented.

Prepared-plan probe73735/96496/39282 completed: Q16 Filter None but raw/IPC children yield16bounded streams; Q12 rawNone/IPCLayouts. Held build state has no live pool reservation, confirming retained-build accounting remains open. Runtime-filter audit additionally reproduced signed-range overflow at Int64 MIN/MAX; fixed construction with unsigned abs_diff and checked probe offsets. Default-feature gates653unique passes with realIPC, one pre-existing ignored; fmt passes. This production change is newer than frozen585 and has no performance measurement. Evidence: docs/benchmarks/2026-09-06-runtime-filter-domain/.

2026-09-06 join-index ownership: VHT heads/next/entries now reserve before allocation and retain their guard through cached/stream lifetime. Direct/hash layout is selected before allocation, removing duplicate direct-path vectors; unsupported domains decline explicitly while errors propagate. All5production HashJoin constructors share caller pool. Default gates677unique passes with realIPC; one pre-existing ignored. SF10 raw/IPC prepared probes retain11561864bytes until plan drop, then zero. Other build/key/runtime-filter/output ownership gaps remain open; no Filter guard bypass. Frozen587sourceSHA cb685e61372454ccebc6c6a27c12f3f77c3d97e87c0d6e1f1bd5de42d47262dd verified; release78016 is active64GiB, .scratch/join-index-ownership/release-01.log. Do not start competing heavy jobs or attribute frozen585performance to this source. Evidence: docs/benchmarks/2026-09-06-join-index-ownership/.

Join-index release78016 terminal0 (10m22s); all587source hashes rechecked. BenchmarkSHA2707f44f53198ed4107afee1aace8cc57d3ed2837e23cbd6fa48a704899e8c46, capSHAcba899d59b78b3d56bd510ceed97f091bbdcbb6ebf2beac4d2d1090d1d1c1120. Cap driver79666 active8GiB, .scratch/join-index-ownership/caps-01.log; fiveCPU screens follow completion. Scratch-only int64-membership component now has geometric growth/minimum-capacity fallback and eight unrun tests; no production integration or initialized Filter capability claim.

Join-index sixcaps79666terminal0, allactuallyspill; RSS126–277MiB. Effectivecgroup1GiB/RLIMIT2GiB; olddriver mislabeledRLIMIT1G, nowdisplaycorrected (format-only scriptchange aftersourcefreeze). FiveCPU screen30427active96GiB, .scratch/join-index-ownership/screens-01.log. No competing builds/profiles/archives during latency.

2026-09-06 continuation review: preserved dirty session work; no production architecture changes since frozen587. CPU screen30427 terminal0: 880/880 typed/time-valid requests across raw/IPC/native/Iceberg/Lance, but every mode fails protected regression gates. CPU archive84003 verifies 2891 files and binaries. GPU canonical69153 terminal0:132 valid samples, zero device executions. Float smoke94144 terminal0:80 valid samples,39 device executions and one GPU-enabled CPU fallback. GPU archive49429 verifies all members and original files. Cap-label correction is the sole verified post-freeze source-manifest difference. Connected initialized MemoryTable filter remains scratch-only, uncompiled and unaccepted. See [current measured outcome](docs/frozen587-benchmark-outcome-2026-09-06.md); query-wide memory, VRAM, public/concurrency and leadership gates remain open.

Initialized MemoryTable root-IN filter integration now exists in production source: new closed_subquery/initialized_membership/int64_membership operator modules, fallible planner filter helper, physical-layout eligibility and original-kind Shared errors. Combined92997 is running48GiB; no tests/performance accepted yet. The frozen587 outcome predates this integration. A review correction for retained LHS name/relation metadata is pending. See architecture for scope; generic fallback remains uncertified and full-query memory remains open.

Initialized membership follow-up: retained identifier red57093 reproduces missing65536byte charge; checked fix passes combined91834 (710), explicitIPC1 and capclassifier8 =719unique selected passes, one pre-existing ignored. Probe82215 confirms IPC Q16 Filter Layouts16/max1340030bytes, held11567040bytes then zero after plan/context drop; raw FilterNone. Source591 archive736b136bffdabdef2832a29f98861b51dd80e42aac2086a4b024760d6f919749 verified. Release7826active64GiB/jobs1/featureslance,gpu, .scratch/initialized-memory-filter/release-01.log. No new performance claim; see initialized-membership evidence.


## Superseded checkpoint prose archived 2026-09-06T21:45:54.502416+00:00

Historical job states below are not current. Relative links were written for the repository root.

## Current source and evidence — 2026-09-06

Current source593 is newer than the latest measured source593. Preserve this
boundary; do not attribute old timings to new Rust. Detailed append-only status:
[execution ledger](.claude/epics/realistic-benchmarks-duckdb-leadership/execution-status.md).
Older guide checkpoints are preserved in
[the history archive](docs/agent-checkpoint-history-2026-09-06.md).

- Semantic repairs include structural uniqueness proofs, exact expression
  representation identity, collision-safe aggregate fields, positional hidden-sort
  restoration, decimal contracts and empty DISTINCT SUM semantics. Duplicate
  public labels in ORDER BY ordinals still require a slot-identity audit.
- Hierarchical reservations and owned producer/temporary-directory lifetimes
  cover selected paths. Prepared Inner joins finish build initialization before
  outer envelopes and preserve unpulled probe streams. Filter/column Project
  propagate actual layout alternatives; Unknown keeps the same streams under
  serial admission. Never bypass admission to restore parallelism.
- Raw fixed-width scan output bounds preserve validity offsets and actual physical
  encodings; read pruning retains predicate dependencies and provider routing.
  Variable-width raw output bounds and general retained source/result ownership
  remain open. See [architecture](docs/architecture.md).
- VHT heads/next/entries own persistent query-pool reservations; unsupported key
  domains are distinct from errors. Source/concat payload, keys, runtime filters,
  generic maps and scratch are not fully covered. Index pressure refuses cleanly;
  it does not yet initiate a new spill transition. Signed runtime-filter ranges
  use unsigned width and checked lookup at Int64 extremes.
- New `closed_subquery`, `initialized_membership`, `int64_membership` modules pin
  an actual immutable MemoryTable RHS for root IN/NOT IN. One budget-owned exact
  membership state initializes across every partition. Known parallel output
  requires compatible actual LHS layouts. Unsupported layouts use a pinned generic
  evaluator under Unknown; legacy allocations remain uncertified. Shared errors
  preserve original classification through `QueryError::root()`.
- Source591 selected gates:719 unique passes, one pre-existing ignored, including
  explicit IPC and resource-error classifier tests. Identifier admission has a
  reproduced negative control. Actual IPC SF10 Filter preparation yields16 bounded
  streams, held11567040bytes then zero after plan/context cleanup. Raw Parquet
  correctly declines. [New evidence](docs/benchmarks/2026-09-06-initialized-membership/README.md).
- Release7826 passed in10m23s with Lance/GPU features under64GiB/jobs1; log
  `.scratch/initialized-memory-filter/release-01.log`. Source archive SHA256
  `736b136bffdabdef2832a29f98861b51dd80e42aac2086a4b024760d6f919749`.
  All source hashes match; pinned binary6a2c065f. Sixcaps2910 passed with
  actual spill. Three matched IPC/raw screens88773 passed528 typed/time-valid requests. IPC
  suite is5.4% faster than frozen587; Q16 matches scalar control, but other
  protected regressions persist. Archive44735 verifies1735 files, both binaries
  and all591 source hashes. [Current outcome](docs/frozen591-benchmark-outcome-2026-09-06.md).
  Q10 probes65192 passed: nested and parent build-input joins return None;
  outer join returns Layouts16. Follow-up diagnostic24063 confirms row-store use;
  source593 repairs that boundary as described below.
  [Probe evidence](docs/benchmarks/2026-09-06-post-membership-plan-probe/README.md).
- [Previous full provider measurements](docs/frozen587-benchmark-outcome-2026-09-06.md):
  five CPU modes880/880 typed/time-valid, but every mode fails protected performance
  regressions. Canonical GPU132 valid samples execute entirely on CPU. Float smoke
  has80 valid samples,39 device runs and one GPU-enabled CPU fallback. Upload and
  grouping-code readiness race explains the latter; one warmup does not certify
  residency. [Required residency gate](docs/gpu-residency-benchmark-gate-2026-09-06.md).

Priority after current measurements: recover measured shared pipeline overlap,
then verify all provider/GPU modes and broaden public/concurrency/resource gates.
Raw variable-width output remains a separate barrier; use
[the CPU diagnosis](docs/shared-cpu-regression-diagnosis-2026-09-06.md).
Full query memory, GPU hard admission and DuckDB leadership remain open.

Row-store repair integrated: actual cross-batch physical types are validated; packed bytes/terminal offsets/column metadata and temporary packing views have guards, and checked fixed-width gather supports prepared multi-batch row stores. Generic multi-batch decline remains. Selected736 tests pass with explicitIPC/classifier, one pre-existing ignored. Probe63866 verifies nested/parent Q10 Layouts16 and full reservation cleanup. Source593 archive e7e3194fa2c9f4e0274c205bc5a76c1643fe0ea79c2251c2b1454dc026534b8e verified. Release91456 passed10m23s with Lance/GPU; pinned binary18e067aa. Six cap checks48446 passed with actual spill. Three matched all-query IPC/raw screens58473 are active under96GiB, affinity0–15; do not start competing heavy work or attribute source591 timings to this repair. Evidence: docs/benchmarks/2026-09-06-prepared-rowstore/.

Source593 initial IPC/591 screen completes176/176 typed/time-valid requests: suite0.93443, geomean0.95204, no >10% protected regression; Q10 improves21.8%. Session58473 continues scalar IPC/raw comparisons. These are development measurements, not full acceptance.

Source593 CPU58473 completes528/528 typed/time-valid requests. IPC/scalar suite0.94173, with Q22 protected1.14245×; raw/scalar1.08909, raw/DuckDB2.42783, protected Q10/Q12/Q16/Q17/Q18. Archive58894 verifies1735 files, both binaries and593 source hashes. Remaining Native/Iceberg/Lance screen43730 active96GiB/affinity0–15. [Current outcome](docs/frozen593-benchmark-outcome-2026-09-06.md). GPU resident worker draft is scratch-only, uncompiled, not the measured source.

Source593 provider43730 terminal0:528 valid; totalCPU1056. Native/DuckDB3.4571, Iceberg0.3229, Lance1.2735; every CPU mode has protected scalar regressions. Archive46216 verifies1735 provider files/all593 source/binaries. Canonical GPU36741 active96GiB/affinity0–15; source593 unchanged. GPU engine/adapter candidates remain scratch-only and uncompiled; five pure Python evidence-validator tests pass separately, not production coverage.

## Superseded pre-bitmap-release status

Current source fixes group/join key equivalence and constructs SQL float predicate
results as packed bitmaps. Final tests: 735 selected executions pass, including
13 spill regressions and dedicated IPC; historical flatten remains ignored.
The prior frozen predicate binary improved Q14 but regressed Q6 across three
providers. Component testing isolated the float result builder; the newer bitmap
implementation is not yet benchmarked. See
[performance evidence](docs/sql-float-cast-performance-2026-09-06.md) and
[group/key contract](docs/float-grouping-domain-findings-2026-09-06.md).

The measured release includes shared constant CAST normalization, checked optimizer
integer arithmetic, and a SQL float-comparison repair covering interpreter,
constant folding and compiled F64 masks. Signed zeros compare equal; NaNs compare
equal and above non-NaNs. The independent oracle exposed a failing intermediate
release, which is not accepted. Final default validation passes 718 selected executions; optimized release
validation remains pending. Do not
attribute earlier passing counts or benchmarks to this newer source. See
[constant normalization](docs/constant-cast-normalization-2026-09-06.md) and
[float-domain evidence](docs/float-comparison-domain-findings-2026-09-06.md).
A separate frozen-control probe reproduces incorrect float GROUP BY cardinality
(39,322 versus 3 groups). It is outside predicate-fix coverage; see
[float grouping findings](docs/float-grouping-domain-findings-2026-09-06.md).

Current source includes shared SUBSTRING and VALUES repairs, validated by 740
selected test executions including dedicated IPC; the historical flatten test
remains ignored. SUBSTRING preserves NULLs, Unicode and signed slice bounds;
VALUES infers each column across all rows and strictly casts cells. Optimized release oracle validation also passes; the five-query canonical SF10 release screen passes. See [string-domain findings](docs/string-domain-findings-2026-09-06.md).

The frozen subquery-pruning release cuts Q22 carried input from ~72 MB to 6.2 MB
and improves its paired median by 20.5% against the recent control. Five canonical
SF10 provider screens completed with 880 valid requests. Larger rechecks cleared
IPC Q16 and Lance Q6/Q8; Lance Q14 remains slower (16.6% and 9.4% in two sessions).
Required GPU execution passes on the separate float fixture at a verified 256 MiB
cache target; 1 MiB preparation refuses by name. These are not leadership or
hard-memory certification. No dependencies changed. See
[attribution and evidence](docs/ipc-scalar-subquery-attribution-2026-09-06.md).
Lance diagnostics identify a constant date CAST left in the optimized predicate;
forced pushdown then applies only one bound and is slower. Shared constant
normalization is being validated; complete-range forced pushdown was also slower.
Preserve default pushdown policy.



## Source 612 through admitted construction 621

2026-09-06 resumed checkpoint: reviewed session edits; no new production architecture
or dependency changes. Frozen 612-file release completes 880 full-provider and 308
protected-recheck requests with typed/time gates passing. All evidence is archived.
A small computed-output probe reproduces a 64 KiB query-budget violation with zero
reservations; this is not fixed. See current performance and memory reports.

2026-09-06 output construction checkpoint: `execution/reserved_buffer.rs` adds
`ReservedBufferBuilder<T>` for fallible pre-admitted allocation and geometric
growth, retaining charges through Arrow Buffer ownership without new dependencies.
20 selected allocation/lifetime tests pass. SQL evaluation and collection are not
yet integrated; the reproduced output-budget violation remains open. This module
is newer than the frozen 612-file benchmark source and has no performance claim.

2026-09-06 primitive projection integration: physical planning now scopes the
query pool during synchronous projection evaluation. Shared primitive arithmetic
and integer-to-Utf8 casts use pre-admitted ReservedBufferBuilder outputs. Both
64 KiB reproducers refuse cleanly; escaped data buffers retain their charges.
744 selected tests pass, two existing library ignores. Decimal/coercion/literal
allocations, other evaluator entry points, metadata and decoder ownership remain
open; release and performance validation are pending. See
`docs/projection-memory-integration-2026-09-06.md` for the exact scope.

2026-09-06 release-620 validation checkpoint: optimized primitive output budget
refusals pass; 89 float/date queries pass. Primitive oracle matches control 58/58
but only DuckDB 50/58 due to preserved integer-division differences. Focused SF10
330 requests and 44 rechecks validate; IPC Q12 retains >10% regressions in both
rechecks. A 110-request projection component validates but exposes float/string
construction overhead. Candidate is not performance-accepted; optimize shared
construction and attribute Q12 before further acceptance. All jobs are terminal.
See `docs/projection-memory-performance-2026-09-06.md`; no new architecture changes
since the preceding primitive integration checkpoint.

2026-09-06 admitted-construction optimization: existing builder now supports
bounded direct fills without payload scratch/copy for non-null float arithmetic.
Integer strings use Arrow's lexical formatter through the admitted writer.
747 selected tests pass, two existing ignores. Opt-in reserved-expression tracing
is available for separate diagnostics; keep it disabled in latency runs. Release,
cap, component and protected Q12 acceptance remain pending. See
`docs/admitted-construction-optimization-2026-09-06.md`; no dependency changes.


## Superseded active header, archived during storage refactor

# Query engine: Codex working guide

2026-09-07 active checkpoint: spill aggregate fallback finalizers now explicitly
pass the owning query pool through the exact decimal materialized helper.
The in-memory HashAggregateExec delegate and direct planner constructions also
retain that pool in sequential/parallel exact-decimal finalization.
Frozen653 passes852unique selected tests including isolated IPC and8CUDA cases;
only the existing flatten_exists library ignore remains in selected coverage.
The SQL before-probe refused correctly; isolated real-spill tests validate both
retained output ownership and clean128KiB refusal. Optimized semantics, both
250million-row spill caps and40CPU/40required-device controls pass; provider
timing remains due. Protected follow-up37654 is running. The prior full
647/649 baseline completes9680requests, with four track/query timing flags open.
Do not attribute frozen651 measurements to653. No dependency changes. See
[fallback pool audit and live validation boundary](docs/aggregate-fallback-pool-audit-2026-09-07.md).

Current production source is frozen651: direct exact Decimal128 aggregate output
with reservation-owned coefficient/validity buffers and explicit MemoryPool
propagation across sequential/disjoint/shard finalizers. Other state/key/filter/
concat/provider ownership gaps remain. No dependency changes.
Source archive SHA256: `5dfa82bdb7ffe2a0e5d37cea7d00cf73cdb3e27908b09e40aeb5745d5f25a18e`.
Binary SHA256: `9472e7d0cc7db5e52de429e2299508d98a798da28e859baf5eed43c237a5ac76`.

Selected validation:845tests pass,10existing library ignores; optimized semantic
oracles and18extra decimal comparisons/two overflow refusals pass. Both real-spill
caps pass (412/414MiB peaks);80CPU/device requests pass,40actual GPU executions
on the supported float fixture. Broader memory and canonical GPU coverage remain open.

Balanced screen60892 completed all12cells and792typed/time-gated requests with
no>10%median/mean flags. Across all four order combinations, Q18 latency ratios
651/649 are.920042native,.905373raw,.924235IPC. Three-query subset suite ratios
are.965091/.956368/.964569. This is not full-workload/independent-session or
DuckDB leadership acceptance; required component/provider/resource gates stay open.
The member-verified2396-file archive is docs/benchmarks/2026-09-07-decimal-output-ownership/.
Manifest SHA256: `22b1593fd41497f4b05dbd4fe5ef2354462f839cccd8012ebfc16dd2ea5ab55c`.

Original full-baseline coordinator2907628 has been resumed with unchanged frozen
647/649 binaries and original20cells. Thirteen cells independently audit6292correct/
time-gated requests; seven remain. Third raw/native cells add no new flags. Lance flags: first Q11 mean1.104273; second Q17
median1.136501 (mean1.044328). Q17 displayed plans match; recorded planning/execute
medians are close, while parse/optimizer spikes differ. Causes remain unproven;
IPC Q22 now flags median1.130157/mean1.090342 in its third cell. It has an extra
alias Project and an execution-time increase; causal overhead is not yet measured.
Keep all three flags open and finish original cells before protected follow-up.
Do not interrupt the baseline for speculative work or overlap heavy jobs with it.
Successor session98089 (supervisor3267903) is waiting in a4GiB CPU0 scope. It
will verify all20baseline cells, terminal PID and empty baseline cgroup, audit9680
requests, then automatically run probe-fallback-budget.py. Do not launch another
heavy job when the baseline ends: first inspect this successor's live handle and
.scratch/decimal-output-repair/budget-after-baseline-status.json/log. A probe exit1
can mean a reproduced budget escape; inspect results, not just the exit status.
Prepared .scratch/decimal-output-repair/profile-components.py must wait for its
completion/next verified idle boundary; it has not run. First run the prepared
probe-fallback-budget.py: source audit finds spill fallback calls the decimal
compatibility builder with the process pool, omitting the query pool. Runtime
escape is not yet reproduced. See docs/aggregate-fallback-pool-audit-2026-09-07.md.
The component diagnostic adds paired attribution
across four required CPU providers plus separately reported IPC. See the candidate
report and active epic007; no parent task completion is claimed.

Frozen649 repairs lossless bound-column identity, physical alias namespaces and
predicate pushdown across namespace-changing aliases. Source archive SHA256:
`d8957b73c8d52603004388744a58f3ec595a16b716c7b28e4f33054c7e371d07`;
benchmark binary SHA256:
`1f189fc6ac191c6b390e1c966ba46be2320dd4800bfed8d32f8faf3937cc387f`.
Its902 selected debug tests, optimized semantic/alias probes, two real-spill cap
cases,80CPU/device comparisons and36CPU diagnostic requests pass. Known integer
SQL division differences and broader resource/provider limitations remain open.


- [Identity contracts, frozen source and validation](docs/qualified-column-identity-2026-09-07.md)
- [Measured finalization costs and diagnostic status](docs/shared-aggregate-finalization-profile-2026-09-07.md)
- [Primary research and bounded alternatives](docs/aggregate-research-follow-up-2026-09-07.md)

