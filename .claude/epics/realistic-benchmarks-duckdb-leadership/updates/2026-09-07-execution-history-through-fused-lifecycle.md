# Execution checkpoint

**Live benchmark:62607**, current protected653/662 comparison. It runs all four
flagged track/query pairs (Lance Q2/Q11/Q20 and raw Q22),50 steady samples, four
startup/execution orderings,1632 planned requests. Do not overlap builds/tests or
other benchmarks. The frozen executables exclude the decimal/lifecycle edits.
See `.scratch/dense-domain-repair/current-protected.log` and the protected root.

Latest source follow-up: fused aggregate producer errors/panics are terminal,
pending sibling tasks cancel on input/worker failure, and send/flush failures
propagate. The final seven fixtures fail6/pass1 before and pass7 after; all25
selected lifecycle/aggregate/cleanup tests pass with no ignores. Candidate source
was restored and633 inputs verified after the matched control. No optimized
binary measures this change; direct debug artifacts were last
built for the preserved control and must be rebuilt through Cargo before use.
See [the lifecycle repair](../../../docs/fused-aggregate-error-lifecycle-2026-09-07.md).
The256KiB spill-completion failure and group-budget replay path remain open;
next work must address admission/spill transition without replaying consumed input.
This production change is additional to the decimal-bound experiment below.

The active benchmark candidate is frozen **662**: query-owned raw aggregate arena, checked dense
key domains, NULL-key grouping, and correct all-NULL SUM/AVG validity. The prior
658 candidate failed new domain probes; control 653 also crashes on the full
signed decimal merge. Preserve both controls and their failures.
See [the current report](../../../docs/dense-aggregate-domain-and-null-semantics-2026-09-07.md).

The working tree additionally replaces repeated decimal-bound exponentiation
with compile-time bounds and adds exact signed-boundary tests. It matches the
prepared patch, compiles and has59 selected test passes. A256KiB spill-completion
test fails with both helper versions; Rayon1 passes one fixed-budget probe,
while Rayon4/16 refuse. The trace identifies fused worker state growth before
spill fallback. See [the finding](../../../docs/fused-aggregate-budget-overshoot-2026-09-07.md).
No resource-policy fix is applied yet. Optimized helper performance is pending.
The frozen662 test/performance results below do not measure this helper change.

Benchmark source SHA256: `ceacdc98e8695b7c5eb90ce0aa815e8358261f2bf8a8aa8621d69ae2202744ab`.
The optimized build completed in 10m50s; copied benchmark/cap binary hashes were
verified. Selected validation passes 847 unique tests (792 feature library,
46 integration, one isolated IPC, eight CUDA); only `flatten_exists` remains
ignored. Optimized domain/NULL/general/decimal checks and both spill-cap tests
pass. Cap peaks were 395/397 MiB. Supported GPU controls pass 40 CPU and 40
confirmed device executions. Broader SQL/resource/GPU coverage remains open.

Component profile 20287 is terminal with exit 1: 112 requests pass, native Q1's
control warmup times out, and seven requests are not attempted. Native/Lance
Q18 finalization improves substantially; raw Parquet/Iceberg merge time does
not. This is not a complete performance gate.

**No live heavy job.** Session43033 exited0; all20 provider/order cells and9680
requests are audited. Equal-weight required-track suite ratio is0.978298
against653. Flags remain for raw Q22 and Lance Q2/Q11/Q20. The current protected
driver selects all four pairs for1632 requests; it has not run. Historical
647/649 flags, including Lance Q17, remain separate. The full raw archive is
compressed locally and member-verified; see [full results](../../../docs/dense-domain-full-sf10-2026-09-07.md).

Decimal test driver54567 exited101 at the spill test; control driver30142 and
worker/remaining-test driver79393 exited0 as orchestrators, with individual
failures explicitly preserved. Archive14780 exited0. Candidate source restoration
is hash-verified. Next repair must address fused admission/spill progress without
re-executing consumed input; the decimal optimization remains a separate experiment.

[The generated progress table](updates/2026-09-07-dense-domain-full-progress.md)
contains every audited cell, ratio and flag. Refresh it only after running
`.scratch/dense-domain-repair/audit-balanced-results.py`, using
`refresh-full-progress.py` in the same directory. It does not establish process
liveness or clear historical failures. Poll the actual handle separately.

After full completion, `run-historical-protected.py` rechecks original flags
against frozen647 with 50 samples and four orders (1632 requests); it is
prepared but not running. Evaluate new current-control flags separately.
Scripts, identities and status: `.scratch/dense-domain-repair/`.

Completed validation/component evidence: 1255 member-verified files in
`docs/benchmarks/2026-09-07-dense-domain-and-arena/`; full SF10 is excluded while
live. Prior failed 658 evidence: 659 verified files in
`docs/benchmarks/2026-09-07-aggregate-arena-domain-findings/`.

The local DuckDB/ClickHouse source comparison is complete and archived; see
`docs/local-engine-source-comparison-2026-09-07.md`. It identifies decimal
expression attribution, typed batch updates and provider execution as distinct
next experiments. No production code changed during that read-only analysis. Frozen disassembly
now proves repeated decimal-bound exponentiation remains in the coefficient loop.
A one-file compile-time-bound candidate is applied (not compiled/tested) at
`.scratch/decimal-precision-bound-repair/`; keep the live benchmark undisturbed.

Historical 647/649 baseline and protected follow-up are terminal (9680 and
1632 requests). Lance Q17 retains a median regression in all four protected
orders; Lance Q11 retains one flag. Tasks 001–010 remain open. Do not attribute
frozen measurements to newer source or count primitive arithmetic probes as
dense aggregate coverage.

## Historical execution checkpoints (superseded live-state statements)


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


- [Identity contracts, frozen source and validation](../../../docs/qualified-column-identity-2026-09-07.md)
- [Measured finalization costs and diagnostic status](../../../docs/shared-aggregate-finalization-profile-2026-09-07.md)
- [Primary research and bounded alternatives](../../../docs/aggregate-research-follow-up-2026-09-07.md)

## Historical checkpoints (superseded live-state statements)


Current frozen635 packs TRY_CAST validity in64-value blocks with dense/sparse/
all-null input routes, preserving admission. Final712 Rust tests pass (2 existing
library ignores); Arrow oracles cover2,704 combinations. Release session71239
is running in64GiB/jobs1,lance+gpu. Source SHA
`8bf2be7aea86e731f4689de8f3a9457e3df397e1db6689ae5b52d62d1e8e82d8`.
Next: poll build, optimized validation in10GiB, original components and four input
patterns in24GiB affinity0–3 against632/612 using prepared drivers under
`.scratch/try-bitmap-repair/`. No performance or broad acceptance claim yet.
Source632 is rejected for protected valid/nullable TRY_CAST regressions and
archived with2,181 verified files; source630 likewise rejected.
See `docs/try-cast-bitmap-construction-2026-09-07.md`.

Earlier checkpoints follow.

Current TRY_CAST input-validity candidate uses a non-null values-only loop while
retaining conversion failures and pre-admitted output validity. Tests session74911
completed:711 passes,2 existing library ignores. Expanded Arrow oracle covers2,000
combinations. Frozen632 release session26297 is running in64GiB/jobs1,lance+gpu.
Source SHA `42adf2e5e4bcd7d7d3e723a4e5fb1a5bfac5fb8b7aac76f839acf3556d21a40e`.
Semantic/component drivers under `.scratch/try-validity-repair/` are prepared.
Source630 completed all gates and archive46538 (4,912 verified files), but its
protected TRY_CAST component regresses1.10739/1.16128 vs612; not accepted.
Next: complete tests, freeze/build, compare components vs630/612, then broader gates.
See `docs/try-cast-input-validity-2026-09-06.md`.

Earlier checkpoints follow.

Source630 infallible numeric construction has completed its optimized build and
scoped semantic/component checks.711 Rust tests pass (2existing ignores), all103
harness tests pass with no skips. Optimized89 float/date,10 dense-float and56
coercion queries match DuckDB; documented integer-division/bare-NULL/timestamp
limitations remain preserved. All352 component requests pass; integer-to-double
is0.76354 of629 and0.82391 of612. This is one session per control, not leadership.
Full five-provider SF10 session12582 remains the only live heavy job,96GiB and
affinity0–15. IPC completed176 valid requests, suite0.96522 of612 and no>10%
query; raw Parquet is running, followed by native/Iceberg/Lance. Next: collect alltracks, run protected repeats, GPU and aggregate
caps, archive allsource/evidence. Do not change production source while measuring.
Frozen source SHA `b27789e2be7043b117f0aa12461f653d91e6341f53d0e427671e12568d0d934a`.
Binary SHA `c527d6d0f4349fde524dd748b33991abf4bb2b876b051abe51159ce7b9314f29`.
See `docs/infallible-cast-construction-2026-09-06.md`.

Earlier checkpoints follow.

Current source629 removes TRY_CAST failure-message allocation, checks SQL decimal
metadata before narrowing, and supports unsigned SQL spellings. All782 selected
tests pass (two existing ignores). Optimized release session5293 completed successfully
in 64GiB/jobs1,lance+gpu. Optimized oracles and 352 component requests pass.
Full SF10 session57718 completed all five tracks (880 requests); protected
session70009 completed132 requests. Corrected GPU session28746 completed80
CPU/device samples at256MiB cache target. Aggregate caps session70215 completed successfully: exact counts and actual
spill under1GiB cgroup and2GiB RLIMIT_DATA, peak407/410MiB RSS. Source SHA
`48e4af7b017a6187650a2d58637519061897194d6c68d6ae17073660915bac34`.
Source628 is rejected for36–46x TRY_CAST overhead, with861 verified archive members.
Evidence archive session13838 completed: 4,511 verified members under
`docs/benchmarks/2026-09-06-try-cast-memory/`. No heavy jobs remain live. Next: integer-to-double attribution,
temporal metadata and remaining allocation boundaries, then the broader epic gates. Lance Q20 is variable
(1.108/0.988 in protected repeats); integer-to-double component remains13% slower.
Timestamp precision/timezone and remaining allocation boundaries are still open.
See `docs/try-cast-construction-2026-09-06.md`.

Earlier checkpoints follow.

Current source628 adds primitive/integer-decimal coercion admission and direct
checked arithmetic fill, together with the typed-literal binder repair. All778
selected tests pass; two existing library ignores only. Frozen source SHA
`fff08baa04fa719657e1dc66cecb1b00f9217731ce7abfe661d1d1564e6ea9c6`
is building in release session29954,64GiB/jobs1,lance+gpu. No concurrent heavy
work. Prepared optimized validation includes32 new independent coercion queries.
Next: validate, component/full-provider/GPU/cap gates and evidence archive. See
`docs/coercion-memory-admission-2026-09-06.md`.

Earlier checkpoints follow.

Current binder increment fixes typed literals losing their declared type;712
selected tests pass (two existing ignores). Frozen623 release validation confirms
literal admission,89 float/date and10 dense float matches. Added literal oracle
reproduces TIMESTAMP-as-string plus four unsupported unsigned spellings on both
623 and621; bare NULL has equal values/different inferred Arrow types. Corrected
623 component session16031 completed220 valid requests; archive94911 completed with every member verified. No heavy jobs are live.
Integer addition/string conversion still trail612 by23%/17% in this session.
No optimized binder build yet. See `docs/typed-literal-binding-2026-09-06.md`.

Earlier checkpoints follow.

Current source623 adds pre-admitted flat scalar expansion and fallible shared
conversion. 770 selected tests pass with two existing ignores. Release/cap
build session20062 is running,64 GiB/jobs1,lance+gpu. Source is frozen; do not
restart on observation timeout. Next: freeze binaries and run prepared optimized
validation, then matched component/provider gates. See
`docs/literal-memory-admission-2026-09-06.md`.

Previous source checkpoints follow.

Current source621 release is built and validated: 747 selected tests, 89
float/date oracle cases, ten dense float cases, and both selected aggregate cap
scenarios pass. Primitive compatibility remains 50/58 against DuckDB (all58 match
control). Component float cost recovered; integer strings remain 28% slower than
source612. No performance acceptance. Q12 traces contain only column/alias
projections, so numeric kernel attribution is unsupported. Corrected batch-size
matrix passes66 requests; protected Q12 passes44 and is within1% of control
in both fresh sessions. No source change is justified by Q12 alone. Archive621 is verified (817 members); no jobs remain active. Next: address
literal/coercion admission and measured string cost. See
`docs/admitted-construction-optimization-2026-09-06.md` for identities and evidence.

Current release 620 is NOT performance-accepted. Optimized resource probes pass;
89 float/date oracle queries pass; primitive oracle has 50/58 DuckDB matches and
eight preserved integer-division compatibility mismatches. All 330 focused SF10
and 44 recheck requests validate, but IPC Q12 regresses 13.2%/10.0% in repeats.
The 110-request component validates but float multiplication/string formatting
regress 44%/65%. All jobs are terminal; verified archive is
`docs/benchmarks/2026-09-06-projection-memory/`. Next: optimize admitted buffer
construction/formatting with component attribution and diagnose Q12; then repeat
required provider/resource gates. See `docs/projection-memory-performance-2026-09-06.md`.

The following build/pending statements are historical checkpoints.

Newest production increment: primitive projection arithmetic and integer-string
casts now allocate through query reservations. Both 64 KiB reproducers refuse,
retained outputs stay charged, and 744 selected tests pass (two existing ignores).
Frozen 620-file release build is running in session 94701 (64 GiB, jobs=1,
features lance,gpu); no competing heavy jobs. Release validation and performance
are next. This does not certify decimal,
coercion/literal, other evaluator, metadata or decoder paths. See
`docs/projection-memory-integration-2026-09-06.md`.

Current candidate: frozen 612-file release, binary SHA
`e8174895539343b61796925247fedebdfef45f54caa4f133d4496e70df90aae6`.
735 selected tests, 89 optimized oracle queries, four grouping/join probes,
330 focused provider requests and 80 GPU/CPU samples pass.

The full SF10 screen (880 requests) and protected rechecks (308 requests) are
complete; all validate and pass time limits. None of seven initially flagged
query/provider cases exceeds 10% in either ten-pair repeat session. Lance Q20
remains 6.4%/8.9% slower, below threshold but retained as a concern.
Candidate/control suite ratios: IPC 0.95931, Parquet 0.99709, native 0.97202,
Iceberg 0.99043, Lance 0.91447. These do not establish DuckDB leadership.

No active jobs. Sessions 8187, 48875 and 88051 are terminal success. Full/recheck
archive has 3,510 SHA-verified members in
`docs/benchmarks/2026-09-06-group-key-bitmap-full/`.

Next resource work: the optimized small projection probe returns newly computed
128–133 KiB outputs with a 64 KiB query budget and zero reservations. Five ownership feasibility tests now pass using safe Bytes owner lifetimes.
Wrapped Arrow capacity underreports retained original capacity; admission metadata
must survive separately. Production integration is still open; follow
`docs/result-memory-admission-design-2026-09-06.md` before claiming this fixed. See `docs/result-memory-boundary-findings-2026-09-06.md`.
Group-table repeated-prefix hashing remains a source hypothesis requiring a
contained component experiment; external-sort typed comparison remains open.

Full resource, concurrency, larger-scale, additional public-workload and
multi-session leadership gates remain open. The new `ReservedBufferBuilder<T>` production primitive passes 20 selected
tests but is not yet called by SQL kernels/collection. Integrate pre-allocation
expression output and retained result metadata next. All current release
measurements refer to the older frozen binary.

## Historical checkpoints (superseded by the current checkpoint)


Latest checkpoint: group/join equivalence and packed float predicate construction
pass 735 selected final tests. Frozen predicate benchmarking completed 330 provider
requests plus 44 component requests and 80 GPU/control samples. Q14 improves,
but Q6 regresses; the component comparison isolates the shared float result builder.
The new bitmap source and group-key repair require a new release. All measurements
refer to the preserved prior binary, not live source. Overall milestone stays open.


Current milestone checkpoint: shared CAST normalization and SQL float comparisons
have 718 passing selected final test executions. The intermediate cast-only
release was rejected after an independent signed-zero probe exposed a constant/
row mismatch. Source now shares the predicate contract across interpreter,
folding and compiled F64 masks. A new optimized build is next; performance and
provider/resource acceptance remain open. No pushdown policy changed.
See `docs/float-comparison-domain-findings-2026-09-06.md`.


Newest work: shared constant CAST folding is implemented and final default
regression validation passes 701 selected executions. Earlier feature-Lance tests passed 22/22.
Complete-range forced pushdown remains slower, so no provider policy change is
planned. Release candidate build and matched canonical screens remain next.
See `docs/constant-cast-normalization-2026-09-06.md` for exact limitations.


Latest checkpoint: pruning five-mode screen and all rechecks are terminal; Lance
Q14 remains an unresolved protected regression. SUBSTRING/VALUES repairs have 740
selected passing tests, with the optimized release oracle and five-query SF10 screen also passing.
Lance Q14 diagnostics completed: forced pushdown is slower and pushes only the
lower bound because the constant upper-date CAST survives optimization. Next:
shared constant CAST normalization and range costing, with semantic regressions.
All jobs from this continuation are terminal; the broader milestone remains open.
Corrected GPU fixture validation verifies 256 MiB; 1 MiB preparation refuses.
See the updated attribution and string-domain documents. Milestone remains open.

Current measured repair: release 41822, trace 92848 and paired screen 44325 pass.
Q22 input falls ~72→6.2 MB; Q22 median improves 20.5% versus recent release and
12.1% versus scalar control. All 88 paired requests validate; Q11 ratios are
1.055/1.010. 716 selected tests pass including dedicated IPC; historical flatten
test remains ignored. Five-mode all-22-query canonical screen 13517 is active,
log `.scratch/subquery-projection-repair/run-broad.log`. No performance-suite or
epic closure yet. Detailed current evidence is in the scalar attribution report.

Latest repair: queue attribution 8498 completed (6/6 typed/time-valid). Q22 keeps
16 slots but carries ~72 MB through an unpruned scalar input; drain ~17 ms versus
reduction ~1.7 ms. Shared scalar/IN/EXISTS planning now applies projection pruning;
correlated IN dependency collection is repaired. 715 selected tests pass with
2 existing library ignores. Release build 41822 is active, log
`.scratch/subquery-projection-repair/release-build.log`. Performance acceptance
is pending. No task/epic closed. Source and evidence details remain in
`docs/ipc-scalar-subquery-attribution-2026-09-06.md`.

Latest continuation: canonical IPC Q22/Q11 ten-pair recheck completed, 44/44
typed/time-valid, but 22.0%/9.3% slower than scalar control. Evidence archived in
`docs/benchmarks/2026-09-06-scalar-rhs-recheck/`. Generic scalar RHS tracing is
integrated and passes 24 selected test executions; debug attribution timed out
under retained deadlines. Optimized build 81396 and trace 81752 subsequently
pass: 6/6 typed/time-valid requests. Q22 RHS execute is ~17.5 ms, construction
~0.006 ms; Q11 RHS execute is ~1.3 ms. Next isolate generic aggregate queue
copy/drain/reduction and Q11 materialization. No CPU
performance repair accepted; epic remains active. See
`docs/ipc-scalar-subquery-attribution-2026-09-06.md` for current evidence.

Updated 2026-09-06T07:51:05.242124+00:00

The epic remains in progress. Latest source/benchmark evidence takes precedence
over older checkpoints in task files. No commits or remote writes were made.

## Provider matrix completed; post-matrix checks active

Full canonical SF10 provider matrix: `.scratch/run_sf10_provider_gates_v2.py`,
output `.scratch/public-bench/canonical-sf10-provider-gates-02`, outer 96 GiB,
16 threads on CPUs 0–15, query 40 GiB / process 48 GiB, 22 queries × 10 pairs × 3 sessions
per track. Tracks run sequentially: IPC, native, Iceberg, Lance, GPU CPU control,
GPU routing. No other builds/tests/conversions may overlap latency measurement.
The provider driver finished with exit 1 for native timing failures; all six modes
finished. IPC/Iceberg/Lance/raw CPU control answer/time gates pass. GPU routing
also passes those gates but has zero device samples. The alternating screen, public development gates and four actual-spill cap
checks are complete. Detailed accepted results are preserved in the linked evidence.

Frozen benchmark binary SHA256: `5224c0c602f34de66c857b5d6bb2ff8dcb754fff4c34f038857512929ca5ef3b`.
Backup `.scratch/ipc-systemic-binary`; source `.scratch/ipc-systemic-source.tar.gz`.
The frozen benchmark describes its archived source. New Boolean NULL-contract
repairs are underway after that snapshot.
Do not silently attribute current source edits to an older measured binary.

## Completed in this repair

- Cached MemoryTable statistics repair the reproduced bad nation-key join order.
- Pull-driven inner joins bound candidate output and yield during unmatched work.
- Eager aggregation uses structural/schema proofs and preserves float expression
  evaluation before SUM.
- Selected 765 Rust tests pass (one pre-existing ignored), followed by 7 focused
  join-stream tests for the final cancellation change and 2 cap classifier tests.
  Harness 83 passes with optional Lance and real-spill tests enabled, zero skips.
- Full IPC/native/Iceberg/Lance conversions validate 86,586,082 rows per mode.
- Fresh Q5 raw/IPC diagnostics pass at unchanged caps; original IPC abort preserved.
- Cap harness now fails wrong-result/nonresource errors; four real cap scenarios pass with actual spill.

## Active next work

1. Completed the original decimal Q6 CPU/thread diagnostic. Sixteen-thread IPC
   uses roughly fifteen cores; aggregate processing is about 11 ms. Three other
   cases time out. Preserve failures; no completed scaling ratio is available.
2. Boolean NULL semantics are repaired in interpreted/compiled paths. Five new
   regressions reproduce before and pass after; nullable fused Boolean batches
   fall back until per-register validity exists.
3. General scalar/array comparison candidate is implemented. Final library gate:
   557 pass, one pre-existing ignored; 13 Boolean/scalar integrations pass after the dictionary constant boundary check.
   Release succeeds; both raw/IPC screens pass all220 executions. Q6 medians
   improve52.6%/75.3%, Q14 improve26.6%/64.0%; identical plans. IPC Q18+2.6%.
   Full six-mode SF10 matrix is now active, native first.
4. Continue optimizer predicate idempotence, query-wide ownership, native timing
   failures, full public/scale/layout/concurrency requirements.

Full latency matrix session 3888 is the sole heavy job. No builds/tests/conversions
or source edits may overlap. Log: .scratch/scalar-comparison-provider-gates-01.log.
Detailed evidence: `docs/benchmarks/2026-09-06-ipc-systemic-repair/README.md` and
`docs/benchmarks/2026-09-06-decimal-thread-scaling/README.md`.


Native gate completed: 660/660 pairs pass, no issues; suite3.358422×,
geomean2.998737×, zero query wins. Prior Q1/Q6/Q15 timing failures closed for
this measured candidate. Decoded IPC and remaining modes are active next.


Decoded IPC gate completed: 660/660 pairs, zero issues; suite 0.524594×,
geomean 0.421267×, 17/22 wins, worst 3.045515×. Strict leadership is still unmet.
Iceberg is now active, then Lance/raw control/GPU. Session 3888 remains live.
Read-only queue audit adds zero-partition and producer-panic handling requirements
in docs/next-memory-ownership-2026-09-06.md. No engine source changed this turn.


During the live latency window, semantic_proofs is preparing the queue-ownership
implementation and deterministic tests only in `.scratch/queue-ownership/`.
Main measured source remains unchanged. No build/test/engine job is allowed for
this scratch work until the full latency run ends. Review/apply the patch only
after preserving the completed scalar candidate evidence; then run its focused
ownership, cancellation, partition/panic and spill/cap gates.


Iceberg gate completed: 660/660 pairs, zero issues; suite 0.315968×,
geomean 0.289468×, 21/22 wins, worst 1.338778×. Warm-provider qualification applies.
Lance is now active in session 3888, followed by raw control and GPU routing.
Queue scratch candidate is prepared with eight unexecuted tests. Review corrected
Arrow's misleading capacity doc comment using actual pinned implementation:
Custom returns declared size. The draft uses universal pre-admitted owned copies
and preserves the rejected capacity-zero draft. Apply/test only after latency.

Lance completed: 660/660 pairs, zero issues; suite 1.259869×, geometric mean
1.123707×, 7/22 wins, worst 3.395529×. Raw CPU control is now active in session
3888, then GPU routing. Production source still matches the frozen candidate.
Parent review removed a potential busy self-wake from the scratch queue stream
and prepared a ninth deterministic completion-wakeup test. All nine queue tests
remain uncompiled/unexecuted; no queue production contract is claimed.

Completed-provider regression review now covers all 66 query/mode medians across
IPC/Iceberg/Lance: none exceeds a 10% engine regression versus the preceding
frozen candidate. Input sample hashes and reference drift are preserved in
`docs/benchmarks/2026-09-06-scalar-comparison/completed-provider-regression.json`.
Physical plan sets match; Q19 optimized text varies in duplicated conjunct order,
so optimizer idempotence/determinism remains open. Native has no passing prior
control score. Raw CPU/GPU and public gates are still outstanding.

Raw CPU control completed: 660/660 pairs, zero issues; suite 2.257953×,
geometric mean 2.043245×, 1/22 wins, worst 5.361387×. No query median exceeds
10% regression versus the prior frozen control (largest +1.5%, Q16). Full
regression artifact now covers 88 query/mode medians. GPU routing is the last
active mode in session 3888; actual device counters remain required.

GPU follow-up source review confirms that GpuCache::reserve permits individual
entries above the configured VRAM target. This is not a hard cap and canonical
CPU fallback cannot certify it. See docs/gpu-resource-contract-investigation-2026-09-06.md.
Prepared .scratch/run_scalar_gpu_smoke.py reuses the existing supported float
fixture and fresh same-binary CPU control after the active matrix; no fixture,
SQL, production source or running benchmark changed. Resource tests remain open.

Full scalar matrix session 3888 completed with exit zero: six modes × 660 pairs,
3,960 pairs with no correctness/time-gate failures. Final GPU suite 2.254746×,
geomean 2.036180×, zero successful device runs across 660 samples. The next
sole heavy job is supported-GPU smoke session 94857, including its fresh CPU
control under 16 GiB containment. Public checks and after CPU diagnostic follow.
No production source change; archival/checksum finalization remains pending.

Supported-GPU session 94857 exited zero: CPU control 40/40 pass; GPU 40/40 pass
and 40/40 actual device samples. Post-fix CPU/thread diagnostic is now the sole
heavy job, session 11699, log .scratch/decimal-thread-scaling-02.log.

Post-fix CPU diagnostic session 11699 exited zero: all 16 attempts validated
and passed fresh ceilings. IPC-16 steady elapsed median 76.473 ms / CPU 1,020 ms
versus prior 313.683 / 4,730 ms. All three previously failing configurations
now complete. Public development gates are the sole active heavy job, session
12676, log .scratch/scalar-comparison-public-gates-01.log. Source remains frozen.

Public development session 12676 exited zero: ClickBench 129/129 and JOB339/339
pass. Scalar archives contain 13,209 files verified byte-for-byte against their
originals; SHA256SUMS covers 46 artifacts. All 526 frozen source files matched
before the reviewed queue patch was applied to spillable.rs. Focused nine-test
compilation/execution is active in session 51877 under 32 GiB containment.
Formatting passes; test results are pending. This source is newer than the
frozen scalar benchmark binary.

Queue focused gate session 51877 exited zero: all nine tests pass, zero skips.
Compile time 26.10 seconds. Broader library/spill/partition/memory/join-stream
gates now run under 32 GiB containment; log .scratch/queue-ownership-integration-01.log.
New source has not yet passed cap or performance acceptance.

Broader queue gate session 40316 exits101: 547 library tests pass, 19 fail, one
pre-existing ignored. Integrations were not reached. All failures are named
queue-budget refusals caused by overlapping prefetched batches that fit alone.
Original source/logs preserved in docs/benchmarks/2026-09-06-queue-ownership/.
Do not increase fixture budgets or weaken spill assertions. semantic_proofs is
preparing demand-before-poll correction only in .scratch/queue-demand; no engine
job active. The correction's reduced prefetch concurrency must be measured.

Demand-before-poll correction reviewed and applied. It holds one owned permit
through upstream polling, copy/admission/send and handoff; reservation releases
before demand. All existing spill budgets/assertions remain unchanged. Two new
queue tests cover one-batch-budget delivery and cancellation of a producer
holding demand while parked. Broader rerun is active in session 90304, log
.scratch/queue-ownership-integration-02.log; no results claimed yet.

Demand rerun session90304 exits101: 620 selected tests pass, one fails, one
pre-existing ignored. All568 library tests (including11queue) pass, resolving
the original19 failures. Join7/materialization6/memory10/partition17 pass.
Spill12pass/1fail: one probe batch requires9,928 bytes versus8,192 query budget,
with no queued usage. Source/log preserved as demand candidate. semantic_proofs
is reviewing batch sizing/retained buffers read-only; no heavy job active.

Single-batch diagnostic session3349 reproduced the8KiB failure:1171 Int64 rows,
9368 logical payload bytes, noNULL/offset,9928 admitted bytes. This is upstream
batch sizing, not irrelevant retained buffers. Temporary diagnostics removed;
source restored to demand candidate. General scan output sizing/ownership is
under read-only design review; no heavy job active.

Upstream batching patch applied: streamed Parquet gets configured row target
before decoding; spill-covered memory pressure overrides the small-file eager
preference while filter/subquery/cache barriers remain. Fixed-width copy layout
selects a row target; actual queue admission still checks bytes. Strict targets
bypass the IPC shortcut that materializes row groups. Other source/decoder
ownership remains open. New actual-reader test writes multiple row groups and
checks all nullable values with <=17 rows per emitted batch. A missing test
trait import caused integration-03 compile failure and was fixed; integration-04
is active in session79441 under32GiB. Original spill tests/budgets unchanged.

Upstream correction gate session79441 exits zero:626 passed, one existing
ignored. All13 spill tests pass, including the original8KiB failure unchanged.
Frozen source snapshot covers526files; only spillable.rs, streaming_parquet_scan.rs
and planner.rs differ from the accepted scalar binary. Release build for the
embedded runner and cap harness is active in session17912 under64GiB, log
.scratch/queue-scan-release-01.log. No runtime benchmark/cap job overlaps.

Cap review found LazyGeneratorExec declares one partition, bypassing the new
queue. semantic_proofs is preparing only .scratch/queue-cap-harness with opt-in
multiple partitions and stronger typed result checks. No main edits/jobs from
that task. Release session17912 remains live as sole heavy job. Current standard
cap harness cannot be used alone as queue-specific evidence.


### 2026-09-06T10:54:55.102790+00:00: queue performance gate failed

Release passed; raw screen stopped at Q14 warmup timeout after66 correct executions. IPC110 correct executions but Q6/Q14 regress6.96×/2.77×. Candidate not accepted. See memory-ownership report and screen-summary evidence. Enhanced multi-partition cap harness5tests pass; release rebuild97772 active. No latency jobs active. Production source stays frozen while parallel ownership design is reviewed.


### 2026-09-06T10:58:00.806722+00:00: multi-partition cap gates complete

Cap release97772 exited0; cap driver97310 exited0. Six4-partition development cases complete with actual spill and unchanged caps/query budgets; agg/sort independent exact oracles pass. Complete raw/IPC screen archives verified226files. Performance candidate remains rejected. Next: implement/audit bounded parallel output ownership, prove overlapping upstream polls and cancellation, then rerun unchanged screens. Design: docs/parallel-input-ownership-design-2026-09-06.md. No heavy jobs active; goal remains open.


### 2026-09-06T11:14:34.539098+00:00: bounded resident parallel ownership implemented

Default609/feature639selected tests pass,1pre-existingignored; nine new tests prove layout guarantees, overlap, fallback and cancellation. Source527files frozen. Release build45183 active (64GiB/jobs1), no latency jobs. Prepared compare_parallel_queue_candidate.py uses accepted scalar control and frozen-new-binary path. Agent prepares scratch-only resident cap fixture extension because LazyGenerator defaultsNone and cannot prove parallel-envelope cap coverage. Architecture/contracts updated; full goal remains open.


### 2026-09-06T11:31:56.700010+00:00: parallel release screen completed with remaining failures

Release45183 passed; binary2eb501c5,527sourcehashes matched. Raw74968exit1 atQ14warmup timeout after66valid executions. IPC84284exit0/110valid: Q6recovers to80.7vs77.2ms, Q9~4%over; Q14still2.63× slower. Candidate notaccepted. Complete archives226files verified. Resident cap example41556exit0/7tests; cap release64917 active. Prepared Q14CPU/thread diagnostic, no latency jobactive. Goal remainsopen.


### 2026-09-06T11:40:04.959358+00:00: resident caps and Q14 diagnostic complete

Resident cap driver 79622 exited 0: six cases completed with actual spill. Q14 diagnostic 86597 exited 0: 16 exact validated attempts; one-thread unchanged, 16-thread elapsed83→219ms with CPU320→260ms. Candidate still rejected for performance. Full diagnostic archive54files and screen archives226files verified. Evidence README: docs/benchmarks/2026-09-06-parallel-queue/README.md. Agent audits prepared nested-join output bounds read-only; no active heavy jobs. Next implement a safe initialized-stream ownership contract rather than weakening caps. Goal remains open.


### 2026-09-06T11:59:44.934145+00:00: initialization ownership repaired; prepared contracts tested

Five deterministic failures reproduced on preserved pre-fix source (93755 exit101), all pass after owned JoinSets (13477 exit0;644selected tests,1ignored). Shared build extraction38526 passes25focused tests. Prepared handoff and gather tests18912 pass614selected tests,1ignored. Production joins do not yet opt into prepared output. No release build/benchmark for this source yet. Agent prepares scratch resident gather propagation; next integrate probe guarantees and actual join output encodings before enabling. Evidence: docs/prepared-input-ownership-2026-09-06.md. Goal remains open.

2026-09-06 resident gather propagation: five reviewed files applied after exact
base verification. First feature gate session13330 exit101:591pass,1new fixture
failure,1ignored; nullable-take fixture corrected. Repeat session35608 exit0:
617pass,1pre-existingignored; fmt check passes. Prepared production joins remain
disabled; spill-initialization cleanup patch is in progress before opt-in.

2026-09-06 spill initialization cleanup: real-file error/cancellation regressions
fail with the old finish helper (session54544 exit101:2fail2pass). Reviewed RAII
repair applied; session47565 exit0:621pass1pre-existingignored, fmt check passes.
Full source and red/green evidence archived and manifests verified under
docs/benchmarks/2026-09-06-spill-initialization/. No heavy jobs remain. Prepared
Inner output bound composition is being developed in scratch; production opt-in,
release/cap/performance gates and adjacent aggregate/sort cleanup remain open.

2026-09-06 prepared Inner integrated: session98936 compile failed only new test
import; corrected. Session72466 exit0:624pass1ignored; session85653 exit0:39pass.
Total663pass1pre-existingignored, fmt passes. Frozen578sourcefiles archived.
Release session38364 active, log .scratch/prepared-inner-release-01.log. Next:
freeze binaries, cap regressions, unchanged raw/IPC alternating screens.

Streaming follow-up plan reviewed and saved to docs/streaming-fixed-width-capability-plan-2026-09-06.md. It requires exposed-buffer normalization plus both queue and gather capabilities, preserving IPC routing. No implementation/measurement claim. Frozen prepared Inner release remains active in session38364; source578 and archive entries verified unchanged.

2026-09-06 prepared Inner release measured: build38364 exit0 in10m20s; cap1833
exit0 with6/6actualspill; raw19068 exit1 (66valid,Q9+25.8%,candidateQ14warmup
timeout1378.848ms,Q18notreached); IPC76883 exit0 (110valid,Q14 82.6vs85.0ms).
Completed paired physical plans match. Raw/IPC archives89+137files and6caplogs
verified; source/binaries preserved in prepared-inner evidence. No heavy jobs
remain. Overall acceptance is open. Next raw fixed-width normalization; scratch
take-only bound audit also addresses825,042,224-byte Q14 reservation without
build identity reuse. No claim of complete query memory/provider leadership.

2026-09-06 raw fixed-width contracts integrated. Compile61032 failed a test import;
gate82843 reproduced two contract failures (Boolean independent offsets and raw
projection order), repaired. Gate8570 exit0:640pass1ignored; direct raw queue test
added, IPC-enabled/resource gate29115 exit0:641pass1ignored. Unique selected total
674pass1pre-existingignored; fmt passes. Frozen580sourcefiles and verified evidence
in streaming-fixed archive. Release5437 now active, log .scratch/streaming-fixed-release-01.log.
Next poll exact build, preserve binaries, six cap regressions and unchanged
raw/IPC screen via .scratch/compare_streaming_fixed_candidate.py. No new score yet.

Working guide consolidated from360 to231lines: mandatory rules preserved, stale checkpoint prose replaced by exact source/evidence status table. Prior guide archived in streaming-fixed evidence; all current guide links resolve. Engine source580file freeze remains unchanged. Release5437 verified live.

### Streaming release and cap gate complete

Release5437 exited0 (10m20s). Frozen binary identities are in `.scratch/streaming-fixed-binary-identities.json`. Cap40149 exited0: all six aggregate/sort/filtered-join cases actually spill and complete. Raw screen25515 is active; no competing heavy jobs. Reviewed session changes after compaction; current architecture is already covered in AGENTS, whose gate status is updated. Aggregate/sort scratch patch remains unintegrated pending sort panic propagation review.

### Streaming screens terminal — candidate proceeds to broader validation

Raw25515 and IPC81852 exit0; 220/220 typed comparisons and all time gates pass; paired physical plans match. Five-query summed medians0.995× raw /1.017× IPC versus scalar control. Prior raw Q9/Q14 regressions absent; Q18~4% slower remains recorded. Evidence and verified archives: `docs/benchmarks/2026-09-06-streaming-fixed/`. No heavy jobs active. Next: scratch aggregate/sort ownership and worker panic→EOF patch red/green, then final source/provider gates. Full memory/resource/leadership goal stays active.

### Aggregate/sort spill ownership validated

Independent aggregate-first71050 and sort-first5828 baseline runs each fail both ingestion cleanup tests and pass both successful-output tests. Shared directory owner + owned merge handle + exact fetch boundary integrated. Library43969 passes617 with one pre-existing ignored; integration35199 passes66 with no skips (683selected total). Evidence: `docs/benchmarks/2026-09-06-spill-output-ownership/`. Source contract changes are reflected in architecture/AGENTS. No heavy jobs active; separate scratch spilled-join output lifecycle patch under review. No newer release/provider claim yet.

### Final spill output source frozen; release active

Spilled join baseline59939 reproduces panic-as-nonerror and a parked task surviving output drop (2fail2pass). Integrated owned output + per-call ProbeSpillFiles lifetime, independently source-reviewed. Final library/integration93503 exits0 (687pass, one pre-existing ignored), formatting passes. Source580 frozen in `.scratch/spill-output-source.tar.gz` and durable spill-output evidence; only spillable.rs differs from screened streaming snapshot. Release42922 is active under64G/jobs1, log `.scratch/spill-output-release-01.log`. Next preserve binaries, unchanged6cap, then `.scratch/run_spill_output_provider_gates.py` six-mode matrix. Agent has a read-only GPU hard-admission design task; no competing runtime jobs.

### Frozen ownership release/cap passed; full matrix active

Release42922 exits0 in10m21s. Binary `.scratch/spill-output-binary` SHA256c4d05ab79f8f7a328d6f2601e087ed5e515e1b2b4e4cc6fc82254b8bbd2648c8; cap binary d40f24414db013b278548e4decc5c35d22f0e656acf6a349498ef792cf3774b5. Cap94804 exits0 (6/6actualspill;128–287MiB harnessRSS); binaries and cap archive verified in durable evidence. Matrix73207 active, output `.scratch/public-bench/spill-output-provider-gates-01`, log `.scratch/spill-output-provider-gates-01.log`, driver `.scratch/run_spill_output_provider_gates.py`. Sequential native/IPC/Iceberg/Lance/rawCPU/GPU,22queries×10samples×3sessions each,16threads affinity0-15,40GiBquery/48GiBprocess/96GiBouter. No competing heavy work. GPU staged design is documented but unimplemented. Goal remains active; no new matrix completion or leadership claim.

### Live native time-gate failure preserved

Matrix73207 remains active. Native s1 completes220pairs. In s2,Q21 iteration8 times out at2406.421ms; watchdog worker termination causes iterations9/10 unavailable. Previous completed Q21 samples range1439.5–1992.7ms and validate. This is one primary timeout plus two consequential coverage failures, not three independent performance events. Candidate full-matrix acceptance fails; do not weaken the ceiling or infer correctness of unavailable outputs. Continue remaining sessions/modes before isolated shared-CPU diagnosis. GPU partition tests/proposal prepared in `.scratch/gpu-partition-contract/`, unrun and not integrated.

### Native matrix terminal: four failing attempts

Native660attempts:657typed-valid outputs, oneQ21 timeout and two consequent unavailable outputs. Additionally Q1s3iteration8 validates but1185.058417ms exceeds1177.904657ms gate by7.154ms. Thus656attempts satisfy both completion/correctness/time. Report.complete=false; no group/leadership report is manufactured from successful-only data. IPC track is active in same73207driver. Next after complete matrix/smoke/public gates: at most4Q21 diagnostic requests for completion attribution, then16Q1/Q6native/IPC process+aggregate counters, with unchangedSQL/oracles/fresh10×ceilings. Prepared scratch GPU partition guard is unrun/unintegrated.

### Post-matrix diagnostics prepared, not executed

`docs/native-cpu-next-profile-2026-09-06.md` records native s1shared decimal/filter/aggregate and native lifecycle hypotheses, plus final native failures. Reviewed `.scratch/diagnose_native_cpu_next.py` has completion/cpu modes, frozenSHA verification, original full oracleSQL, exactfresh10×gate, and own-worker counters captured at started/query_finished plus5ms samples. AST-only validation; no profiling run during matrix. `.scratch/run_spill_output_gpu_smoke.py` and `.scratch/run_spill_output_public_gates.py` are pinned to c4d05ab7 and prepared for post-matrix gates. GPU partition scratch has three unrunbaseline-compatible tests and a minimal structural single-output guard proposal; no main changes.

### IPC terminal: repeated Q19 throughput regression

IPC660attempts:630typed-valid/timepass, Q19firstmeasuredtimeout in all3sessions followed by9unavailable per session (3primary+27consequential). Completed warmups~3.1s and source-confirmed Filter prepared-capability gap support a focused queue/pipeline diagnosis, not yet a causal proof. Historical scalar Q19medians native268.723ms/IPC552.750ms versus currentnative~1.09s/IPCwarmups~3.1s are separate-run comparisons. Iceberg nowactive under same73207session. Durable native/ipc failurecheckpoints in spill-output evidence; fullarchivesafter matrix. Completion diagnostic nowmax8requests(nativeQ21+IPCQ19), extended for independentnewfailure, unchangedoracles/gates.

### Prepared-wrapper systemic repair designed

Reviewed `docs/prepared-wrapper-composition-plan-2026-09-06.md`: distinguish prepared pool-independent stream lifecycle from Unknown/Bytes/Layouts output certification, retain physical variants, propagate audited Filter/Column-AliasProject lazily, and retain exactly-once streams even when metadata or envelope falls back to serial. No unreserved parallel pulls or declaration based on opaque bytes. Agent drafting scratch-only patch/test split while matrix runs; no main source changes or runtime gates. The design is a candidate for the repeatedQ19 regression, not causal/performance proof.

### Wrapper draft review checkpoint

Reviewed session changes after compaction: frozen production source is unchanged; spill ownership contracts remain current. Scratch `.scratch/prepared-wrapper-composition/` now has exact base/candidate trees, baseline-compatible real Inner→Filter→duplicate-alias Project regression and separate metadata/lifecycle tests. Root reviewed serial reuse, physical variants and shared lazy execution helpers. Additional deterministic wrapped overlap/error tests are being drafted without runtime work. No compilation or performance acceptance is claimed. Matrix73207 remains the sole heavy job; do not restart it.

### Wrapper draft safety review preserved

Reviewed deterministic wrapped overlap, predicate-error and consumer-drop tests, plus prepared-stream serial reuse and physical-layout transformation. Agent added subquery preflight and real aggregate/filter/Inner residual OR with NULL/duplicate oracle. Exact patch, baseline-compatible red split, source hashes and notes preserved under `docs/benchmarks/2026-09-06-prepared-wrapper-draft/`, explicitly uncompiled/unrun. No production integration; all580 frozen source hashes match. Matrix73207 re-polled live, sole heavy job. Prior turn is a verified wait plus new scratch regression evidence; next dependent runtime action still requires terminal matrix.

### Frozen follow-up validation queued

Matrix73207 remains live. Scheduler99930 is now live but idle under its own96GiB/swap0 wrapper. `.scratch/run_spill_output_followups.py` waits for exactPID3957136/start314728571 AND empty scope safe-build-3957136-29476; requires terminal outcomes for all6tracks, frozen c4d05ab7 and pinned driver hashes. It then runs GPU smoke16G/4threads, public development96G/16threads, completion diagnostic(max8requests) and CPU diagnostic(max16requests) sequentially through individual wrappers. Each child scope must drain before the next stage. Nonzero results are preserved and do not erase matrix failures. State/logs: `.scratch/spill-output-followup-01/` and `.scratch/spill-output-followup-01.log`. Do not start duplicate stages or other heavy work while either session is active. No main source change.

### Iceberg terminal; broader shared regressions preserved

Iceberg660/660 pass typed/time gates; suite0.4538205/geomean0.3607169 vsDuckDB. This does not erase historical scalar regressions Q12 5.314×,Q19 4.808×,Q21 3.295×,Q10 1.518×,Q16 1.340×; exact per-query table and report preserved in spill-output/iceberg-checkpoint. Q19 probe Project→StreamingScan emits filter-only strings then drops them, preventing scan fixed-width capability before Project; prepared-wrapper propagation alone cannot repair that boundary. Reviewed separate physical emission-pruning design at docs/streaming-project-output-pruning-plan-2026-09-06.md; agent drafting scratch implementation/tests, no main edits or jobs. Matrix73207 continues into Lance; scheduler99930 remains waiting and owns subsequent runtime stages.

### Separate physical emission draft reviewed and preserved

Planner-only scratch implementation now has checked original/reduced resolver-root mapping, unchanged streaming eligibility and original ScanNode pointer, retained Project and cache semantics. Root source review found no new actionable issue in that routing. Baseline-compatible real Parquet fixed-output/NULL/duplicate/filter-root and two-consumer cache tests plus candidate resolver tests are prepared, not compiled/run. Exact patch splits/hashes/limitations preserved in docs/benchmarks/2026-09-06-streaming-project-pruning-draft. Multiple runtime-filter and real sidecar fixtures remain absent/source-reviewed only. Both drafts remain unintegrated. Wait for BOTH matrix73207 and automatic followup99930 before builds or heavy compression; scheduler starts its stages immediately after matrix scope drains.

### Lance terminal

All660 typed/time gates pass; suite1.4946713/geomean1.2552964 versus matched DuckDB,7/22wins,worst4.3568768(Q16). No leadership. Terminal report/manifest/workers/scope and per-query historical scalar comparisons preserved at docs/benchmarks/2026-09-06-spill-output-ownership/lance-checkpoint. Matrix73207 continues to raw CPU and GPU routing; scheduler99930 remains waiting. Both scratch repairs remain unintegrated and untested.

### Raw CPU Q19 fails independently in two sessions

Live gpu_control raw-Parquet track: Q19s2/s3firstmeasuredtimeouts at1888.036/1997.441ms, each followed by9unavailable after watchdog.2primary+18consequential failures preserved in spill-output/raw-cpu-checkpoint. Ten s1completed samples median2193.002ms; warmups2233–2311ms, execution~2108–2137ms. Physical probe Project→StreamingParquetScan matches the Iceberg emission-bound gap; timing observations support investigation, not proof that the draft fixes it. Matrix73207 remains live; scheduler99930 waiting. No driver changes or retries.

### Matrix terminal; automatic diagnostics now active

Matrix73207 exited1. Five modes3300attempts:3247typed-valid outputs,3246allgatespass;6primarytimeouts+47consequentialunavailable+1completedtimefailure=54failingattempts. Canonical GPU did not run because failed raw CPU control was rejected by the harness;660GPUattempts remain uncovered. Root matrix-summary and terminal raw report preserved. Scheduler99930 automatically advanced: GPUsmokeexit0,40CPU+40GPUtracktyped/timecomparisons but only39deviceexecutions;Q01iteration1 used CPU during24→24.6MBresidency/upload transition. Counters preserved in gpu-smoke-checkpoint. Public-development nowactive, then boundedcompletion/CPUdiagnostics. No builds/compression until scheduler terminal; no source changes.

### Prepared pipeline red/green implementation begins

All frozen checks terminal; full archives verified11421files. Baseline46500 reproduces wrapper lost-preparation(0pass1fail) and emission retained-string(1pass1fail), no compilation failures. Checked wrapper overlay integrated; first compile found missing QueryError qualification, fixed without changing semantics. Queue11013passes28; integration45445passes12. Full library23107 on wrapper-only source passes630 with1existingignored and only the still-unapplied emission red failing. Checked emission overlay is now integrated; full library run in .scratch/prepared-pipeline-green-lib-02.log active. Original baseline overlays needed exact newline matching; guard failures made no partial writes. No release/performance claim for new source. Dedicated ignored IPC sidecar test and broader integration/cap gates still required.

### Combined pipeline validation and release freeze

Both production repairs are integrated. Combined library633pass/2ignored, explicit IPC ignored test1pass, selected integrations70pass:704unique passing tests and one pre-existing ignored. Formatting passes. Reviewed session changes after compaction: these are prepared stream/layout and streaming emission contracts, with no dependency change. Source581 archived with SHA256fbaebd01d9402cf722c19c35a87e5150ab46a60e8fba6378c0cac73788ae0ca6. Release20531 is active under64GiB/jobs1; no competing heavy job. Candidate performance and caps remain unmeasured.

### Measured CPU target: decimal aggregate roots

Source audit confirms repeated discounted-price arithmetic in the saved Q1 plan and independent aggregate-input loops. The ~8s cumulative expression worker intervals are not exact CPU attribution. Provider-neutral reuse of an earlier successfully computed Decimal128 aggregate root, within the same batch and preserving normalizers/order/errors, is a bounded proposed optimization. Design and independent test obligations: docs/decimal-aggregate-root-reuse-design-2026-09-06.md. Agent is preparing scratch-only candidate while release20531 runs. No source/binary change or speedup claim.

### Pipeline release complete; caps active

Release20531exit0. All581source hashes rechecked unchanged. Engine65becb409a176c3358bdf9fd0d15dd00817c6fedb5fc19b0ce73143820b9db66, capc0e7ef7304c88c9d4f42bdea3f2f204a93976fec60731195df29609731d4286c, preserved in .scratch/prepared-pipeline-{binary,cap-binary}. Six cap cases23208 active with4resident partitions/2Mrows/32MiBquery, same1GiBcgroup/2GiBrlimit/8GiBcontainment. No latency jobs overlap.

### Pipeline cap and Q19 paired screens pass

Cap23208exit0:6actualspill completions,RSS131–255MiB. RawQ19screen91030exit0 and IPC98049exit0:16typed/time-valid engine requests total. Three-pair steady medians raw444.728→324.566ms(0.729807×),IPC549.572→446.873ms(0.813130×) versus accepted scalar control. Preliminary screens, not full acceptance. All22query fiveCPUmode screen62780 now active sequentially; retain every failure and investigate protected>10%regressions. Evidence in prepared-pipeline/q19-{raw,ipc}.

### Raw all-query screen rejects performance acceptance

All176rawrequests typed/timepass, but suite1.268686×accepted scalar and2.912016×freshDDcalibrations. Q10/12/16/18/21 medians exceed protected10%regression threshold, strongest Q12at4.787× andQ21at3.153×. All-query coverage caught unresolved shared pipeline gaps despite Q19recovery. IPC/remaining modes still running62780. Decimal root reuse scratch draft and independent integration fixture are preserved under docs/benchmarks/2026-09-06-decimal-root-reuse-draft, uncompiled/unrun/unintegrated. Source581 remains frozen.

### IPC all-query screen and next ownership design

IPC176/176typed/timepass; suite1.141461×scalar, protected regressions Q10/16/17/21/22;Q21at3.100×. Native nowactive within62780. Runtime variable-output design is preserved in docs/runtime-variable-output-quantum-design-2026-09-06.md: scanner-only postconditions cannot establish join repeated-gather bounds or decoder/tail ownership. No arbitrary max-string restriction, estimated proof, or unreserved tail is accepted as a fix.

### All five CPU screens terminal; evidence archiving

Screen62780exit0:880/880typed/time-valid engine requests (220warmup/660steady), plus16initialQ19requests. Performance is NOT accepted. Suite ratios candidate/scalar: raw1.268686,IPC1.141461,native1.145804,Iceberg1.265555,Lance1.070679; respective candidate/freshDDcalibrations2.912016,0.574697,3.836361,0.380012,1.440396. Perquery regressions preserved in prepared-pipeline/cpu-screen-summary.json. No newGPU/fullpublic/concurrency/leadership acceptance. Archive2018 active under4GiB, no latency/build jobs. Nested bounded-Inner gather propagation remains scratch-only work by agent; decimal reuse draft uncompiled.

### GPU wrapper partition defect reproduced and fixed

After all CPU screens and archive completed, added baseline-compatible GPU tests. Red27945exit101:1pass/2runtime failures (invalid partition acceptance and omitted ready nonzero partition). Applied7line check/multi-output CPU delegation and added partition-zero no-CUDA coverage. Green89829exit0:4pass/noignore. Only src/physical/gpu.rs differs from archived581source. Exact patch/red-green evidence in docs/benchmarks/2026-09-06-gpu-partition-contract. No new release/device benchmark or hard-admission claim.

### Nested preparation green; decimal reuse validation active

Nested baseline30842exit101:2expected failures. Integrated exact gather variants and recursive same-stream preparation; green12009exit0:17focused contracts pass, separateQE_DICT_GATHER=0run13pass. Source review found no actionable defect. Evidence under docs/benchmarks/2026-09-06-nested-prepared-gather. Next integrated reviewed decimal-root-reuse patch plus independent integration fixture; focused unit76976 active under48GiB. No new release/performance claim. GPU guard remains green4focusedtests.

### Decimal physical-identity defect found before benchmarking

Initial decimal units76976exit0(7pass) and aggregate integration44126exit0(3pass, actual spill) passed. Root source review found Expr::PartialEq delegates normalized DecimalValue equality. New scale regression83045exit101 returnedDecimal128(38,1) instead of(38,2). Replaced only physical-reuse identity with explicit recursive exact decimal coefficient/scale + literal type equality; global numeric SQL equality unchanged. Focused8test41398 is active. This is an implementation defect caught before any release/performance claim; original draft remains preserved as historical evidence.

### Combined new-source gates pass

Corrected decimal unit41398exit0:8pass. Combined67125exit0:723pass/2ignored across full library and10selected integrations. ExplicitIPC1pass gives724unique selected passing tests, one pre-existing ignored. Source582files includes GPU partition, nested prepared gather and exact decimal root reuse. No new release/performance result yet; evidence in docs/benchmarks/2026-09-06-nested-decimal.

### Release deliberately stopped for further correctness investigation

Frozen582source archiveSHAde25e5ce62faa65b54414bf00b753e62c7e66c52d0ac82e9cfafdd5d8a50e811 was built in79142/64GiB. Source audit found ProjectionPushdown collapses arbitrary equal computed Projects, plus typed-substitution equality risks. Stopped exactowned safe-build-191236-26566.scope;79142exit143 and drained. No new binary preserved or benchmarked. Projection isolated red23323 nowactive. Initial binder SQLfixture52579failed3Decimal assertions because dotted literals bindFloat64; this was an INVALID reproducer, not evidence of a substitution bug. Originalfixture/log retained; replaced SQL controls with explicitDecimal casts and requested direct typed-binder/HavingCSE reproducers.

### Shared expression substitution defects reproduced and fixes integrated

Projection23323exit101:3fail1pass; identity-mapping guard12961exit0:4pass. Typed binder/HAVING91509exit101:4fail1pass. Shared exhaustive Expr equality now preserves decimal representation, float bits and nested List types/values; scalar numeric equality unchanged. Contract86002exit101:18pass,1name-fallback failure. Removed both display-name substitutions, unmatched aggregate errors. Explicit CAST SQL compatibility controls82236exit0:3pass. Full library+11integration gate77950 active; no new release/performance result. Evidence in docs/benchmarks/2026-09-06-expression-substitution.

### Aggregate field labels and DISTINCT empty input repair

New SQL17814exit101 reproduced2wrong-value collisions; internal-name candidate23465 fixes them but exposes hidden-sort trim duplication and all-NULL SUM DISTINCT=0. Collision-safe intermediate sort names and positional restoration pass focused4948 plus5tests31141. DISTINCT finalizer red59701exit101:1fail/1valid-zero controlpass; shared integer/float empty-set fix integrated. Final combined36425 active (full lib+13integrations including actualspill oracle). Frozen585source archiveSHA5a802b78737f68ce3e7501912c8d962eb84fc324da00ad88b560cef8a354572b. No release/performance result yet. Broader slot-identity/ordinal ORDER BY audit remains open.

### Final identity correctness gate green; release active

Combined36425exit0:749pass/2ignored, explicitIPC1pass =>750unique passes, one pre-existing ignored. Actual DISTINCT spill oracle passes; fmt and all585source hashes pass. Release1693 active under64GiB/jobs1, featureslance,gpu, benchmark_embedded+oom_cap_harness examples, .scratch/expression-substitution-release-01.log. Do not restart or run competing heavy work. Next verify source/binary identities, preserve both binaries, sixcap cases, then matched CPU/provider screens. No new performance/cap result yet.

### Frozen585 release/caps pass; five-mode paired SF10 active

Release1693exit0 (10m23s), all585source hashes unchanged. Benchmark binary b2cdf0385edfbbb967f5b88b6cd627a9c94a6388532e150afcb350eeb5f51d94; cap c907e72a8d7cdd7e1db7d7a9c4d8f200c0bd97bf308c510743be4a953b3a7264 preserved. Cap48772exit0:6actualspill completions, RSS131–287MiB, same32MiBquery/1GiBcgroup/2GiBRLIMIT limits. Join cap oracle remains rowcount. Screen18829 active sequential raw/IPC/native/Iceberg/Lance, all22queries,3steady pairedsamples+warmup, freshDuckDB typed references/timegates,16threads affinity0–15,96GiBscope. No other heavy jobs. Evidence docs/benchmarks/2026-09-06-expression-substitution.

### Frozen585 raw SF10 screen terminal, IPC active

Raw176/176requests pass typed/time gates; suite1.074074×scalar control and2.427838×freshDuckDB calibrations; geomean1.074256×scalar. Q1.9112×,Q21.8880×,Q19.7128× show shared improvements; protected Q10 1.5943×,Q12 4.8609×,Q16 1.5744× reject performance acceptance. This is one paired development session, not three-session certification. Screen18829 continuesIPC/native/Iceberg/Lance. A read-only boundary review corrects Q21 attribution: Semi/Anti one-output consumers bypass the multi-partition queue; deeper Inner queues need measurement. See docs/next-shared-cpu-boundary-profile-2026-09-06.md.

### IPC SF10 paired screen terminal, native active

IPC176/176requests typed/timepass. Suite.961507×scalar (3.85%better), geomean1.021448×scalar (2.14%worse), suite.488914×freshDuckDB calibrations for the documented decoded-residency track. Q1.785376×scalar, Q21.973580×. Protected regressions Q2/10/14/16/17/22 prevent acceptance. Raw+IPC352validrequests so far; screen18829 continuesnative/Iceberg/Lance. Full-sourceGPU/public/concurrency/resource certification remains pending.

### Native SF10 screen terminal, Iceberg active

Native176/176typed/timepass; suite1.003238×scalar,geomean1.022413×scalar,suite3.685046×freshDuckDB. Q1.801481×scalar improves shared exact-decimal input work, but Q10/16/21 protected regressions persist. First3tracks528validrequests. Iceberg/Lance continue18829. Generic frozen-binary1vs16worker CPU diagnostic is being prepared in scratch, not executed; no source mutations or competing heavy work.

### All frozen585 CPU modes terminal; full archive active

Screen18829exit0:880/880typed/timevalid paired engine requests. Suite/control ratios raw1.074074,IPC.961507,native1.003238,Iceberg1.085966,Lance.983234. All modes have protected>10%regressions; no performance acceptance. Per-query ratios, geomeans and freshDD ratios in cpu-screen-summary.json. Archive45952 active4GiB, full output bytes and both binaries independently verified, no engine/latency jobs. Next canonicalGPU samebinaryCPUcontrol+routing, supportedfloatdevice smoke, then main-pool1vs16CPU diagnostics. Subquery runtime hardcodes independently sized workers, so diagnostic is NOT strict process-single-thread. No source changes.

### Archive verified; canonical GPU routing active

Archive45952exit0:2891files/518206132bytes verified, compressedarchive140162509bytes SHA f84f7849e83e8a01542e26ff7e46e9566697222aea82919ab33a121eb2d8efe3; both decompressed binaries match. GPU available RTX5090,32607MiB,1149MiBused at preflight (restricted-shell NVIDIA query fails; escalated read succeeds). CanonicalGPU9640 active96GiB,16threads affinity0–15,3samples1development session with freshsame-binarycontrol,40GiBquery48GiBprocess,QE_GPU_CACHE_MB=256,NVRTC librarypath. No device-use/VRAM-admission claim until evidence; floatdevice smoke follows. No competing heavy jobs.

### GPU routing/device coverage terminal; CPU diagnostic active

Canonical9640exit0:66GPU-enabled+66CPUcontrol samples typed/timepass, actualdevice runs0/uploads0; canonical decimal workload falls backCPU. Samebinary floatdevice smoke88133exit0:40device-executed GPU samples and40CPUcontrol samples pass. No VRAM-hard-admission or canonical GPU acceleration claim. GPUarchive15466exit0 verified850files/204982658bytes, archiveSHA c9c0301554b1961435ef5fccce1ab1628c68377fcf0d3962ba1e00b12c1ab201. RawCPU/main-pool diagnostic70828 active96GiB, Q1/10/12/16,1vs16 mainworkers,3pairedsteady+warmup, 40/48GiB budgets, freshtypedDDreferences; processCPU/fault/RSS/livethread boundaries required. Not strict single-thread because subqueries have independentruntime. No competingheavyjobs.

### Raw CPU diagnosis verifies lost overlap; IPC contrast active

Diagnostic70828exit0:64typed/timevalid requests, complete steady CPU boundaries. Q1 CPU1mainworker6830→5730ms; CPU16workers12750→10850ms, scaling8.41→8.22×. Q10 scaling5.63→3.44×, Q12 6.99→1.36×, Q16 1.82→1.17×; candidateCPU16is lower whilewall regresses, supporting lostoverlap ratherthanextra arithmetic. Q16main1has20livethreads, independentlysizedsubqueryruntime caveat persists. Rawarchive78402exit0:214files/196121850bytes, SHA fc1fa246f6df5a4e7a2cfef97ec26f8b2db66e439afcb04ce8d47ccdf51f505e. IPCcontrast2589 active96GiB, Q10/12/16main1vs16, unchanged typed/countergates. Agent prepares source-only immutable budgeted uncorrelated-subquery Filter capability proof. No production changes; topdocumentation checksums refresh after diagnosticlatency.

### IPC contrast complete; capability probe

IPC2589exit0:48requests typed/time-valid, complete CPU boundaries. Q12 candidate scales8.90× on decoded input versus1.36× raw; Q16 IPC scales0.986×. Archive7297exit0 verifies162files, SHA4287f43519f0c90adf6bf729ccd6b24fb50f4f0d1b703f9c483dc8eab0630073. Generic initialization-only example added, build73735 active48GiB. Probe will check actual child descriptors before proposing filter admission; no returned stream pulls or performance claims.

### Prepared capabilities verified; signed runtime-filter domain fixed

Probe build73735 and runs96496/39282exit0. Q16 FilterNone; child rawLayouts16×421534bytes/IPC16×1336958bytes. Q12 rawNone/IPCLayouts16×927982bytes. Held pool.used0 exposes retained build accounting gap; preparation is not query-wide memory certification. No guard bypass. Generic Int64 MIN/MAX regression68317 reproduced signed arithmetic panic; unsigned range/checked lookup repair passes653unique default-feature selected tests including realIPC, one pre-existing ignored, fmt pass. No new performance claim; production source now differs from frozen585. Next priority remains coherent retained-state ownership and immutable predicate preparation before further parallel admission.

### Retained join index implemented; release active

Source587 SHA cb685e61372454ccebc6c6a27c12f3f77c3d97e87c0d6e1f1bd5de42d47262dd is verified and frozen. VHT index vectors own shared-pool reservations, direct layout avoids duplicate allocation, and unsupported keys are distinct from errors. All5production constructors wired. Combined17816exit0 plus realIPC:677unique passes, one pre-existing ignored. Two prior fixture failures24886 preserved; fixture now allows bounded persistent build state and independently rejects premature envelopes. SF10 probes18471exit0 show11561864bytes retained raw/IPC and zero after plan drop. Source/payload/key/runtime-filter/generic-map ownership remains open. Release78016 active64GiB/jobs1/featureslance,gpu, .scratch/join-index-ownership/release-01.log. After success verify/copy/hash binaries, run6capcases, then matched CPU screens without competing heavy work. No newer-source performance acceptance.

### Verified release; cap driver active

Release78016exit0 in10m22s,587live source hashes rechecked, binaries pinned in .scratch/join-index-ownership-binaries.json. Benchmark2707f44f53198ed4107afee1aace8cc57d3ed2837e23cbd6fa48a704899e8c46; capcba899d59b78b3d56bd510ceed97f091bbdcbb6ebf2beac4d2d1090d1d1c1120. Sixcap79666active:2Mrows,4partitions,residentinputs,query32MiB,outer8GiB,cgroup/RLIMITvariants. No competingheavyjobs. Scratch int64-membership design adds amortized growth and minimal-size admission fallback; eight tests unrun and no main integration.

Sixcaps79666exit0,6actualspill completions,RSS126–277MiB. cgroup1GiB/RLIMIT2GiB; preserveddriver outputlabel1Gwasincorrect forRLIMIT, futuredriver label nowusesactualconfiguredcap. Noengine sourcechange. Screen30427active96GiB,16threadsaffinity0–15,40GiBquery48GiBprocess,all22queries×5CPUmodes,3steady+warmup pairs,typedfreshDDoracle and10×ceiling. No competingheavywork.

Join-index raw screen complete176/176typed/time-valid requests; suite1.077135×scalar, geomean1.082516×, suite2.418064×freshDuckDB. ProtectedQ10/12/16 ratios1.607409/4.630122/1.532988 reject performance acceptance. IPC continues in30427; no competingheavyjobs. Next immutable Filter design must preserve root-IN evaluation even on emitted zero-row batches, and suppress deferred RHS errors only when no predicate evaluation happens.

IPC screen176/176typed/time-valid: suite.969769×scalar, geomean1.021989×, suite.494135×matchedDuckDB. ProtectedQ10/16/22 ratios1.303910/1.366735/1.287531 prevent acceptance; residentIPC comparison is not rawCPU leadership. Native continues30427. Scratch-only coherent pinned-MemoryTable root-IN Filter implementation is being prepared; no production edits or builds during latency.

Native screen176/176typed/time-valid: suite1.002442×scalar,geomean1.016882×,suite3.720144×freshDuckDB. ProtectedQ10/16/21 ratios1.207177/1.232679/1.103273; no performance acceptance. Completed3tracks528validrequests. Iceberg continues30427. GPUcanonical/smoke drivers prepared against frozen2707binary but not launched; archive waits for terminalCPU.

### Frozen587 all-mode measurements complete

Updated 2026-09-06T19:26:14.035976+00:00

2026-09-06 continuation review: preserved dirty session work; no production architecture changes since frozen587. CPU screen30427 terminal0: 880/880 typed/time-valid requests across raw/IPC/native/Iceberg/Lance, but every mode fails protected regression gates. CPU archive84003 verifies 2891 files and binaries. GPU canonical69153 terminal0:132 valid samples, zero device executions. Float smoke94144 terminal0:80 valid samples,39 device executions and one GPU-enabled CPU fallback. GPU archive49429 verifies all members and original files. Cap-label correction is the sole verified post-freeze source-manifest difference. Connected initialized MemoryTable filter remains scratch-only, uncompiled and unaccepted. See [current measured outcome](../../../docs/frozen587-benchmark-outcome-2026-09-06.md); query-wide memory, VRAM, public/concurrency and leadership gates remain open.

Initialized membership follow-up: retained identifier red57093 reproduces missing65536byte charge; checked fix passes combined91834 (710), explicitIPC1 and capclassifier8 =719unique selected passes, one pre-existing ignored. Probe82215 confirms IPC Q16 Filter Layouts16/max1340030bytes, held11567040bytes then zero after plan/context drop; raw FilterNone. Source591 archive736b136bffdabdef2832a29f98861b51dd80e42aac2086a4b024760d6f919749 verified. Release7826active64GiB/jobs1/featureslance,gpu, .scratch/initialized-memory-filter/release-01.log. No new performance claim; see initialized-membership evidence.

Source591 release7826 terminal0 in10m23s, all hashes verified; benchmark6a2c065f52475ec5e867c8af53482993a59e61a68f8e90255ee684db57150f59 and cap a52a8aa10d582ee78577c3eb30e6cce616d4c7275e4f9f2920005a3fd138cb95 pinned. Sixcaps2910 terminal0, all spill, peak RSS129–294MiB; cgroup1GiB/RLIMIT2GiB. Exact aggregate/sort oracles; join rowcount only. Three all22-query paired screens88773 active96GiB: IPC versus frozen587, IPC versus scalar, raw versus scalar. No competing builds/profiles/archives during latency. Source remains591.

IPC versus frozen587 complete:176/176 typed/time-valid requests; suite.945715×, geomean.961881×, no protected>10% query regressions. Q16 median359.970→302.805ms (.841196×). Suite.478428×matchedDD on decoded residency track, not raw leadership. Scalar comparison is now running88773; overall performance acceptance remains open.

IPC versus scalar complete176/176 typed/time-valid; suite.963559×, geomean1.003677×, matchedDDsuite.485267×. Q16 recovered to1.006319×scalar (282.178→283.961ms); Q10 1.258568× and Q22 1.198924× still fail protected gates. Combined352 validrequests. Raw screen continues88773. No overall performance acceptance.

All source591 screens88773 terminal0:528 typed/time-valid requests. Raw suite1.068075×scalar,2.439731×DD, protectedQ10/12/16 persist. Archive44735 terminal0 verifies1735files/310522559bytes, archiveSHA ad70434ea956f7e9891c5d2dfba965ec210d77dbea9f8f9b5299f19264ba28b9, both decompressed binaries and591source hashes. Actual Q10 nested/parent/outer preparation probes65192 active96GiB, no output pulls. Q22 single-partition Anti bypasses the aggregate queue; do not attribute its regression to that multi-partition barrier.

Probe65192 terminal0: IPC Q10 nested root.0.0.0.0.1 and parent root.0.0.0.0 returnNone, held12877872/12878428bytes; outer root.0.0.0 yieldsLayouts16/max3252628bytes, held28144920. Zero output pulls. Build-input preparation barrier is verified; row-store causation remains a source hypothesis requiring explicit decline diagnostics. Source591 unchanged. Next raw sizing helper remains scratch-only/unrun; no unsafe capability expansion.

Row-store follow-up: existing diagnostic24063 verifies Q10 nested build573157rows/229batches/vht=true/row_store=true. New public regression93319 reproduces -1→4294967295 across Int64/Int32 physical payload batches; uniform control passes. Production source591 unchanged; new test is newer. Coherent checked row-store ownership/layout repair is being prepared in scratch; no guard bypass or repair acceptance. Evidence: docs/benchmarks/2026-09-06-rowstore-layout/.

Row-store repair passes combined18984 (718), follow-up49480 adds Unknown/late-VHT-failure/membership/IPC/classifier coverage for736 unique passes; one pre-existing ignored. Both independent physical-type corruption cases pass. Probe63866 terminal0: nested/parent/outer Q10 allLayouts16, max12443812/13213485/3252628bytes; held17465112/17465668/32732160, each zero after final cleanup. Source593 archivee7e3194fa2c9f4e0274c205bc5a76c1643fe0ea79c2251c2b1454dc026534b8e verified. Release91456 active64GiB/jobs1/featureslance,gpu. No new timing claim. Next verify/pin binary, caps, then paired IPC/raw screens before broadening certification.

Source593 release91456 passed10m23s; all593 live hashes verified and binaries pinned (benchmark18e067aa). Six cap checks48446 passed with actual spill; aggregate/sort exact validators, filtered join count-only and not row-store coverage. Paired canonical22-query IPC/591, IPC/scalar and raw/scalar screen58473 is active. No performance acceptance yet.

## First completed paired screen

IPC versus source591 is complete:22 queries,176/176 typed/time-valid requests, suite ratio0.9344275 (6.6% faster), geometric mean0.9520399, no protected regressions above10%. Q10 median478.1553→373.941935ms (21.8% faster); Q9 ratio0.763895 and Q21 ratio0.817540 also improve. Q16 is1.093356×, below but close to the protected threshold. This is one-session development evidence, not multi-session acceptance. Decoded IPC/DuckDB suite ratio0.475150 does not establish raw-Parquet leadership.

Scalar-control IPC and raw screens remain active in session58473. Remaining-provider/GPU scripts are prepared in `.scratch/prepared-rowstore/` but have not been launched. Preserve the active session; do not restart or overwrite its output. Full timing archive will be produced after all three CPU screens terminate.

Source593 CPU58473 terminal0:528 valid; IPC/scalar0.94173 with Q22 protected1.14245×, raw/scalar1.08909 and raw/DuckDB2.42783 with protected Q10/Q12/Q16/Q17/Q18. Archive58894 terminal0 verifies1735 files/all593 source/both binaries. Native/Iceberg/Lance43730 is live under96GiB, affinity0–15. Full result: docs/frozen593-benchmark-outcome-2026-09-06.md. GPU worker-session implementation exists only as uncompiled .scratch/gpu-residency-gate/production.patch with exact base hashes; no context/harness connection, no end-to-end residency claim.

Source593 provider43730 terminal0:528 valid; totalCPU1056. Native/DuckDB3.4571, Iceberg0.3229, Lance1.2735; every CPU mode has protected scalar regressions. Archive46216 verifies1735 provider files/all593 source/binaries. Canonical GPU36741 active96GiB/affinity0–15; source593 unchanged. GPU engine/adapter candidates remain scratch-only and uncompiled; five pure Python evidence-validator tests pass separately, not production coverage.

Frozen593 GPU36741/36738/96882 all terminal0: canonical132 valid but0device; float80 valid,39device+1fallback. New resident prototype integrated, checks passed (732 selectedRust before hardware test,99harness afteroptins). Real hardware54404 terminal101 at missing MemoryTable GPU identity. Typed-key/lifetime repair is being drafted; no live heavy jobs or release build. See docs/gpu-resident-session-implementation-2026-09-06.md.


## Exact identity repair and real-device regression

The former MemoryTable planning failure is repaired systemically. GPU column caches, grouping-code caches, queued-job deduplication and row metadata now use typed keys. Immutable MemoryTable keys strongly retain the actual provider and compare Arc allocation identity; versioned keys compare complete identity bytes and file vectors. Group-code keys retain ordered column vectors, avoiding delimiter collisions. Formatted hashes are diagnostic only. Normal mixed MemoryTable routing still declines; explicit resident planning admits the supported immutable provider.

Eviction removes exact mirror entries and releases row metadata after the last column/code entry. Failed upload commits do not leave row owners; failed enqueue removes its dedup key. Worker teardown clears mirrors and queued metadata after device-cache/session locals drop. This retains host tables while their device-cache entries live; it does not establish query-wide host or VRAM admission.

The actual CUDA lifecycle regression now passes (session9160, exit0, hardware-identity-02.log). It prepares numeric and grouped data, rejects conflicting sessions, validates distinct exact A/B results, and verifies old Run/Release messages cannot destroy a newer session. GPU library79809 passes693 tests, zero failures, three ignored; the hardware test is one of those ignored tests and was explicitly run successfully. Logs are in .scratch/gpu-residency-protocol/. The first capped attempt could not access the systemd bus; the escalated capped rerun succeeded. No bare execution was used.

Five typed-identity tests replace two obsolete string-format tests. Default adapter and full harness gates follow. The report validator also now turns malformed environment objects into explicit contract errors. Release and latency acceptance are still pending; frozen593 timings remain historical and must not be attributed to this repair.


## Source599 rejected by independent GPU semantic probes

The source599 release completed (session96980, features lance,gpu). Binary d51b27c40099df375ecb81c614bdfdcfdaefc25a6bb8ba2e8d735a76a6ef63e9, source archive0888f127f4a607241f924bfeae0bf2e14fc79ddd230ed74d14ba17d0c1407b58. Default check and all100 harness tests passed, but no performance acceptance was attempted: independent actual-device Arrow output comparisons reproduced two systemic semantic errors. `SELECT SUM(v), COUNT(v), MIN(v), MAX(v), AVG(v) FROM t WHERE v > 100` over [1,2] returned zero rows versus DuckDB's one [NULL,0,NULL,NULL,NULL]. All-NaN `MIN(v), MAX(v)` returned [+infinity,-infinity] versus [NaN,NaN]. Both GPU requests reported one completed device execution and zero dispatch failures; completion evidence is deliberately separate from correctness validation.

Reproducer results are .scratch/gpu-resident-identity/domain-red-03/{filtered,nan}/result.json; first two attempts had diagnostic script setup/event-reading errors and are preserved separately, not engine SQL failures. The scalar output repair now retains the scalar row and builds nullable non-COUNT outputs, while grouped empty output remains empty. Actual nonfinite source Float64 arrays now explicitly decline resident preparation and mixed cache upload. Four real-hardware edge tests are running in session68481. Finite fused MIN/MAX intermediates and delayed duplicate cache-upload accounting require follow-up review. Source599 is superseded and must not be certified by its narrow successful A/B lifecycle test.


## Duplicate cache jobs and computed extrema

The delayed duplicate-job hypothesis was reproduced on actual CUDA: replaying column and group-code jobs after preparation/release doubled resident byte accounting from36 to72 (hardware-duplicate-red-01.log, session16323, exit101). The shared upload helpers now check exact live cache keys when consuming jobs and reuse existing entries. Cache insertion additionally computes checked total-minus-replaced-plus-new bytes before mutation, and mirror accounting applies the net delta. The hardware regression now passes with stable byte/column/eviction/failure counters and independently expected grouped values (hardware-duplicate-green-02.log, session14200, exit0). The first green compile attempt had a test-only ambiguous float type, fixed with an explicit f64 suffix.

Nonfinite-source rejection alone did not bound fused MIN/MAX inputs: finite inputs can generate NaN through overflow followed by multiplication by zero. MIN/MAX now accept only direct column inputs in the shared supported-domain predicate. Planner construction, resident preparation/verification and actual dispatch enforce this rule, including manually constructed plans. SUM/AVG fusion retains its existing floating evaluation semantics; this is not proof of arbitrary precision or reassociation equivalence.

The scalar-empty regressions now check all five independent expected values: SUM NULL, COUNT0, MIN NULL, MAX NULL, AVG NULL. Physically empty and all-filtered inputs must execute on device and preserve the scalar row; grouped all-filtered output remains zero rows. Final library/hardware/IPC/default gates are being run sequentially. Source599 remains rejected; later production repairs require their own frozen release and measurements.


Final correctness gate23574 is terminal0: GPU library696 pass, eight ignored in ordinary invocation; six explicit CUDA cases and one explicit IPC case pass, leaving only the pre-existing flatten-dependent-join ignored test excluded. Default adapter check passes. Harness98930 previously passed100 tests with no skips and harness source has not changed since. Formatting and diff checks pass. These703 selected Rust tests and100 harness tests do not certify GPU hard admission or performance. A fresh release/benchmark must use the new source, not rejected source599.


## Exact integer SUM admission

Release4053 (binary2121b6ebd8549a91d8e5326d7c6f6d541f94971412b0bf36398e46e0e708e8fa, source7641b2cb...) independently passes the earlier scalar-empty/NaN-source reproducers, but is rejected for integer SUM. The actual device sums [2^52,1,2^52] to9007199254740992; DuckDB returns9007199254740993. Each uploaded integer is exactly representable, but their sum is not. Verified source/binary/Arrow evidence is archived in docs/benchmarks/2026-09-06-gpu-integer-sum-red/. No latency acceptance was attempted.

The shared aggregate-domain gate now admits SUM only with an exact Float64 output type; integer/decimal SUM follows ordinary CPU routing or explicitly refuses required GPU preparation. Planner, worker preparation, resident verification and dispatch all use the same gate. Direct-column MIN/MAX and COUNT remain eligible. Checks include aggregate field offsets after grouping fields and malformed schema refusal. Final gate42447 passes698 library tests, seven explicit real-CUDA tests and one explicit IPC case:706 selected Rust tests, one pre-existing ignored test excluded. Default build check passes; unchanged harness has100 passing tests with no skips. Full GPU numeric/reassociation and host/VRAM admission remain outside this acceptance.

Next shared CPU attribution is recorded in docs/ipc-scalar-subquery-attribution-2026-09-06.md: Q22's observed plan-time increase includes scalar RHS execution; trace that work before changing queue behavior or caching. This is a source-backed investigation plan, not a new performance measurement.


## COUNT(*) preflight coverage and full-fixture debug gate

Release90206 (binaryc7c1a7f05704741cbc394612c12b613a8dde7fc0622ef74f7c490d5d2e625608, sourcee104c589...) passes all three independent semantic probes, including exact CPU fallback for integer SUM. Its first measured resident run is nevertheless incomplete: CPU control40/40 comparisons pass; GPU Q6 has20/20 typed/device-valid samples, while Q1 preparation refuses and all20 requested samples remain not_run. No suite win is claimed. The full failed run is preserved in docs/benchmarks/2026-09-06-gpu-count-star-preflight-red/.

The closed-expression proof omitted wildcard syntax inside COUNT even though the aggregate planner/kernel supported COUNT(*). Preflight now admits a wildcard only as the sole argument of nondistinct COUNT; standalone wildcard, SUM(*) and DISTINCT COUNT(*) still decline. This is a general expression contract repair, with a positive SUM+COUNT(*) real-device test and negative context tests. Gate77913 passes699 library tests, eight explicit CUDA tests and one IPC case:708 selected Rust tests, one pre-existing ignored test excluded; default check passes.

Before another release build, both unchanged complete custom floating fixture queries were exercised through a debug adapter on real CUDA. Session30531 passes Q1 and Q6 with complete independent typed DuckDB comparisons and one device completion each. The first diagnostic attempt78109 had a missing comparator scratch directory after successful Q1 device execution; its error/output are preserved separately. Corrected evidence is .scratch/gpu-count-star-pilot-02/. These are correctness tests, not latency acceptance. Require this full-fixture debug check before future GPU release benchmark builds; small positive unit tests had missed actual SQL coverage.


## Completed resident development validation

Release36955 and sequential validation32255 are terminal0. Frozen sourcea8d140e1923431a6c5a211c03ef4c1e040a568166c9a28a1c9c30193ba621c45 (600 files), binary0c6f32881ad8c082dc19c0f917797066771f8cb28bd68100d395d4b65dc2a60c. All three independent semantic/fallback probes pass. Required GPU40/40 measured samples and same-binary CPU control40/40 typed comparisons pass, with zero fallback/time failures. GPU/DuckDB suite ratio0.09917, geometric mean0.09966 on the unchanged two-query600k-row custom float fixture only; preparation40.520/14.635ms is separate. Both1MiB cache preparations refuse explicitly, preserving requested not_run slots. This is not hard VRAM admission, canonical SF10 GPU support, or overall leadership.

Archive82203 verifies all243 passing-run files,87 capacity-run files, source archive and decompressed binary. Earlier wrong/refused candidates remain separately archived. Current report: docs/gpu-resident-validation-2026-09-06.md. Production/harness source remains identical to this measured snapshot; subsequent documentation updates are newer. No heavy job is live; no parent epic task or overall goal is closed. Next CPU work is the scratch-only generic scalar-subquery attribution patch/tests; GPU hard admission and full resource/provider/public/holdout/concurrency work remain required.
