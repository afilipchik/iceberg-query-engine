# Query engine: Codex working guide

Current source, tests and reproducible measurements take precedence over historical
status prose. Update the current checkpoint in place; keep chronological logs in
linked reports and the existing epic rather than prepending another status block.

## Current checkpoint — 2026-09-11

Current source retains coordinated aggregate spill across owners, exact retry
cursors, admitted input ownership, compact fixed cells and borrowed numeric input.
Copied-input prefetch leaves half the available pool, capped at1MiB, as a scheduling
heuristic; actual reservations remain authoritative. Native IPC decoding still
lacks full admitted ownership. Default ownership remains disjoint.

Semantic repairs preserve duplicate multiplicity in LEFT COUNT preaggregation and
correlated aggregates. Statistics never prove uniqueness. Correlated reduction
uses SEMI; scalar empty-result derivation plus a fresh presence marker distinguishes
missing groups from matched NULL. Unsupported shapes retain scalar execution.
Right-built SEMI runtime filters target the left probe by proven lineage;
right-built Anti/Left preserve unmatched probe rows. See [correlated reduction](docs/correlated-reduction-proof-audit-2026-09-11.md),
[empty results](docs/correlated-empty-result-contract-2026-09-11.md),
[LEFT COUNT](docs/left-count-retained-reduction-2026-09-11.md) and
[semi filtering](docs/right-built-semi-runtime-filter-2026-09-11.md).

Packed-word sequence56279 and evidence19981 are terminal0: release63cabb3a,
120typed-correct outputs/2476join traces,60identical plan pairs,915-file archive,
521verified inputs and zeroOOM/max. The optimization was rejected: genericQ1
ratio1.035281, nativeQ6 1.136598/nativeQ9 1.035107 and resident16Q18 1.047894;
several regressions persist in both blocks. RawQ18 0.970651 does not establish
broad throughput improvement. No confidence-certified gain or neutrality.
Current production hybrid code is restored exactly to measured240cd5e2, retaining
new widths0–32 regression tests. Three-file rollback archive verifies521inputs.
See [measurement and rejection](docs/packed-word-measurement-2026-09-11.md) and
[rollback](docs/decoder-rollback-2026-09-11.md).

Native Anti test-contract repair82790 is terminal0 after the benchmark archive:
27focused integrations pass in each mode;15-file source/evidence archive verifies
521inputs. NOT EXISTS now exercises actual dictionary Anti/build orientations;
original NOT IN comparisons and independent NULL/duplicate/empty-RHS expectations
remain. No engine rewrite or memory policy was changed. Prior packed-word broad
feature gates had1095library/11ignored,125contracts and28spill/numeric passes each,
with native58/2 disjoint,57/3 partial and six legacy spill failures each. These
pre-repair failures remain archived. Rollback validation79575 is terminal1:1095library/11ignored,125contracts and28
spill/numeric passes each; native63/0 disjoint and62/1 partial, six legacy spill
failures each.25-file archive verifies521inputs. See [test repair](docs/native-anti-test-contract-2026-09-11.md).

Current batch-view candidate adds `state_rows/array_view.rs`: checked fixed-array
views bind once into16stack slots; wider layouts/dictionaries retain the adapter.
No heap descriptors, default/budget change or early row publication. Production
hybrid code still matches240cd5e2. Broad80443 passes1036library/3ignored and62
integrations;11-file source archive verifies523inputs. Both-mode64579 terminal1:
1098library/11ignored,125contracts and28spill/numeric each; native63/0disjoint,
62/1partial,six legacy spill failures each.25-file validation archive verifies.

Sequence35732/evidence29174 terminal0: release480109f6 in8m52s,120typed-correct
outputs/2476traces,60identical plan pairs,914-file archive/523inputs,zeroOOM/max.
Q1 ratios generic raw0.819005/native0.789009/resident16 0.792398/resident4 0.813728
improve in both blocks. Q10 regresses in both blocks on raw1.028254,resident16
1.011322/resident4 1.031138; Q6 remains noisy. Retain provisionally, no confidence
or neutrality certification. See [measurement](docs/batch-views-measurement-2026-09-11.md).

Refusal trace79512/archive86197 terminal0: six reproduced failing tests,12denial
stacks,19files/523inputs/eight fixtures and frozen debug binary verify,zeroOOM/max.
8KiB semi/anti traces reach Reader::open schema admission; grouped join/outer
reach whole decoded-page admission after output-quantum halving; COUNT(DISTINCT)
reaches the speculative header read window. These remain unresolved resource
failures, not successful spill coverage. See [attribution](docs/legacy-spill-refusal-results-2026-09-11.md).

Full sequence31964 terminal1 and independent evidence13409 terminal0 for480109f6.
Providers:332typed-correct outputs,248/264valid pairs(raw66/native57/Iceberg64/
Lance61),1428verified archive files. Raw geomean2.557747/suite2.790526,zero wins;
other tracks incomplete. Residency:348typed-correct outputs,278/278valid pairs,
1458verified files. All three canonical tracks complete at32/48GiB capacity:
decoded IPC geomean0.760895/suite1.325514,14wins. Canonical mixed GPU has0device
requests; custom required GPU verifies40measured device requests. Preserve16GiB
preload, resource/concurrency and leadership gates.523inputs verify; screen peak
26892062720bytes,zeroOOM/max. See [providers](docs/batch-views-providers-screen-2026-09-11.md),
[residency and corrected device qualification](docs/batch-views-residency-screen-2026-09-11.md)
and [failure attribution](docs/batch-views-screen-failure-attribution-2026-09-11.md).

After archival, initial header baseline88461 exposed omitted buffer-owner overhead
in the draft tests. Corrected10231 reproduces3failures/3passes while preserving
that charge. Candidate now grows parser-proven header prefixes and shares checked
initial allocation cost in ReservedVec/ReservedBufferBuilder. Only three existing
source files plus the new test module differ from480109f6. Focused4648 passes15;
broad50886 passes1042library/3ignored and77integrations,retaining six legacy spill
failures. COUNT(DISTINCT) now requests9512bytes; no resource/performance closure.
Thirteen-file source/red/green/broad archive verifies524inputs.
Both-mode38824 terminal1:1104library/11ignored,125contracts,28spill/numeric
passes each; native63/0disjoint62/1partial,six legacy spill failures each. No new
failure names or missing executables;27-file archive verifies524inputs,zeroOOM/max.
Sequence57421/evidence18433 terminal0:13210a20 built8m53s,120typed-correct outputs,
2476traces,60equal plan pairs,914verified files/524inputs,zeroOOM/max. RawQ9ratio
0.963374 improves both blocks; nativeQ6ratio1.182007 regresses both, as do smaller
raw/nativeQ13 and resident16Q1 cases. No neutrality/leadership claim. Precision12690 terminal1: identical480 nativeQ6 fails interval0.820231–1.259049;
Q9 passes0.985811–1.016126. Dependent comparison NOTRUN.379-file archive verifies
32correct outputs/624traces and524worktree inputs; executed480 has523-input provenance.
Native execution profile40662 terminal0:149stopped snapshots/onecorrect result,32-file
archive.75snapshots contain filter comparison,44numeric coercion; no CPU-percent
claim. Source-line attempt failed before query execution; function-symbol retry used
the same480binary. Numeric coercion red25456 reproduces four compiler gaps.
Current candidate adds chunk-local float coercion/reuse and shared decimal type
admission. Focused88389 passes17tests; earlier57397 passed16 with one stale
unsupported-shape expectation, now corrected. Contracts28884 pass22tests;
library76131 passes1046/3ignored. Both-mode lance/gpu resource gate65485
terminal1: each library1108/11ignored,contracts125,spill28/6fail; native63/0
disjoint62/1partial. No changed failures;25-file archive verifies526inputs,zeroOOM/max.
Source526 frozen. Native probe95635 terminal0 proves actual decimal conversion
at source1171,one float register,divisor100,len1024,typed-correct canonicalQ6.
51-file archive verifies526inputs,zeroOOM/max; three earlier debugger assertion/display
failures remain preserved. Release43258 completed8m51s, freezes97e53169/526inputs.
Paired43258/evidence36280 terminal0:120typed-correct outputs,2476traces,
60unchanged plan pairs;914-file archive verifies526inputs,zeroOOM/max. Q6ratios
candidate/control:generic raw2.224442,raw1.203546,native1.421561,resident16
1.166881,resident4 1.376582,all slower in both blocks. Candidate not accepted.
Attribution28368 terminal0:48typed-correct controls/onecorrect profile,76snapshots,
350-file archive verifies526inputs,zeroOOM/max. Genericraw compiler on/off2.232903
candidate versus1.004317old; nativeQ6 remains noisy.63sampled evaluator leaves map
to a per-row float operator jump table. Current one-file candidate selects float
operators outside row loops; focused99196 passes17. Both-mode71658 terminal1:
each library1108/11ignored,contracts125,spill28/6fail;native63/0disjoint62/1partial.
No changed failures;27-file archive verifies526inputs,zeroOOM/max. Sequence92933
passes22numeric contracts; release completes8m55s,freezes ece6a4d6/526inputs.
Triage92933 terminal0:42correct/792traces/14equal plan groups,453verifiedfiles.
GenericrawQ6 ratio0.928567vs97e53169 but2.074367vs13210a20; candidate not accepted.
Profile44421 terminal0:67snapshots/onecorrect output,22-file archive verifies526inputs.
Runtime audit exposes confound: genericraw copied458batches becomes admitted7323
at unchanged16slots; resident4changes ownership route at unchanged916batches/4slots.
Native keeps916batches/1slot. Plan text equality misses these changes. Current
source propagates planned scan.batch_size into admitted Reader instead of fixed8192.
Red30866 reproduces both exceeded17-row and capped32768-row requests; green18452
passes14scanner tests. Source527 archived; both-mode93080 terminal1:1110library/11ignored,125contracts and28spill/numeric passes each; native63/0disjoint,62/1partial and six legacy spill failures each. No added/removed failures. Release95646 terminal0 in8m49s freezes beb0cdfd. Checkpoint37633 terminal1;
evidence56910 terminal0 verifies527inputs,452diagnostic/1433provider/1458residency
files. Providers340typed-correct,255/264valid pairs; raw geomean2.579082/suite2.808489.
NativeQ1 warmup timeout,IcebergQ9 referenceSIGSEGV,LanceQ9 join-index refusal remain.
Residency348typed-correct,278/278valid pairs at32/48GiB; canonicalGPU0device requests,
custom required40measured device proofs. Peak23061450752bytes,zeroOOM/max. Genericraw
Q6 still7323batches versus458baseline; no throughput recovery certified. All jobs
terminal. Intermediate commit0c554142 was pushed and exact remote branch hash verified.
Next-cycle matrix78387 terminal101:2empty-selection passes and4fixed/string
dense/sparse batching failures after independent values and ownership checks pass.
Production is unchanged; source528/red evidence verifies. See
[post-filter reproduction](docs/admitted-filter-batching-2026-09-11.md). Current
source removes predicate-only survivor copies via filter_projected and reuses a
static-only nullable mask directly. Gather5passes/scanner16passes with exactly4
known batching failures; source528/component archive verifies. Current source now adds admitted_coalesce with typed fixed-capacity buffers, bounded
UTF8 bytes and pre-reserved handoff. Reader retains one pending batch/exact offset;
whole-chunk construction refusal may bypass packing, source errors stay terminal.
Accumulator4/scanner23 passes, including the4batching regressions and post-prefix
I/O failure. Source530 archive verifies; broad84021 terminal1:1125library/11ignored,125contracts,
28spill/numeric passes each; native63/0disjoint,62/1partial and6spill failures each,
no added/removed failures. Validation26files verify,peak21864136704,zeroOOM/max.
Release63185 terminal0 in8m48s freezes1bba20b3/530inputs. Checkpoint11187 terminal1;
evidence76516 terminal0 verifies452diagnostic/1436provider/1097residency files.
Triage42typed-correct/792traces/14equal plan groups; genericrawQ6 restores458batches
from7323,ratio0.897777 versusbeb0cdfd but1.828793 versuspre-coercion13210a20.
Native/resident4Q9 regress versuscontrol in both blocks; no broad neutrality claim.
Providers339typed-correct,253/264valid pairs; rawgeomean2.447789/suite2.629099,0wins.
NativeQ1/Q6 warmup timeouts,IcebergQ13 referenceSIGSEGV,LanceQ9 join-index refusal.
Residency256typed-correct,209/212emitted measured pairs; intended278 not complete:
CPUcontrolQ1 timeout blocks all66canonical mixedGPU requests. DecodedIPC completes,
geomean0.708858/suite1.002784,14wins at32/48GiB capacity,not16GiBpreload acceptance.
CustomrequiredGPU40measured device proofs; canonicaldevice coverage0/notrun.
Peak22185889792bytes,zeroOOM/max. All jobs terminal. Current cycle is ready for
intermediate commit/push; then continue first-batch working-space reproduction.
See [current checkpoint](docs/admitted-coalesce-checkpoint-2026-09-11.md).
No performance or first-batch memory recovery claimed. See [quantum contract](docs/admitted-planned-quantum-2026-09-11.md).
See [route audit](docs/admitted-route-cost-confound-2026-09-11.md). See [dispatch attribution](docs/compiled-coercion-attribution-2026-09-11.md). See [measurement](docs/compiled-coercion-measurement-2026-09-11.md).
Trace69517/archive94698 terminal0: COUNTdistinct still fails,3stacks,13files/524inputs/
eightfixtures and frozen debug binary verify,zeroOOM/max. New barrier is encoded
page-body read during join build-input polling, not speculative header admission.
Ledger65075 terminal0 on source526 proves zero retained join batches at refusal;
11-file archive verifies,zeroOOM/max. The first batch reader requires investigation,
not accumulated join input. See [ledger](docs/first-batch-refusal-ledger-2026-09-11.md). Schema-only
reduction is insufficient: handoff4096bytes per column plus owners exceeds8KiB for
two columns. See [minimum-progress review](docs/legacy-spill-refusal-results-2026-09-11.md)
and [header implementation](docs/incremental-header-admission-plan-2026-09-11.md).

Sequence66120 is terminal0. Release240cd5e2 built in8m51s with521verified inputs;
120paired outputs against68912c23 are typed-correct,with2476join traces and zeroOOM/max.
All60logical/physical plan pairs are identical;915-file archive verifies. RawQ9/Q10
mean ratios0.959229/0.953205 improve in both blocks. Generic rawQ1 does not improve
(1.011340); rawQ1 and nativeQ17 are slower in both blocks (1.019667/1.019884).
Q6 controls have substantial variation. Candidate is retained provisionally;
no confidence-certified speedup,neutrality or DuckDB leadership. All240cd5e2 jobs terminal.
See [measurement](docs/validity-repeated-measurement-2026-09-11.md),
[implementation](docs/repeated-validity-decoding-2026-09-11.md) and
[validation](docs/validity-repeated-validation-2026-09-11.md).

Earlier completed full provider/residency measurement binary:68912c23,521 source inputs,lance/gpu features.
All its jobs and independent audits are terminal:

- Provider screen:330typed-correct outputs,247/264valid measured pairs
  (raw66/native58/Iceberg63/Lance60),1418-file archive. Raw completes22queries with
  geomean2.713836/suite2.975360 versus DuckDB,zero wins. NativeQ1/Q6 warmup gates and
  Q12 measured timeout remain. IcebergQ9 reference cal0 crashes with exit-11;
  LanceQ1 times out and Q9 reference refuses134217728bytes. No OOM/max cgroup events.
  [Provider evidence](docs/right-semi-provider-screen-2026-09-11.md).
- IPC/residency:252typed-correct outputs,206/212requested valid measured pairs,
  1100-file archive. Decoded IPC and preloaded CPU each have63/66pairs,Q1 warmup
  timeout. Canonical mixed GPU executes0requests because CPU control is incomplete.
  Custom float smoke has40required-device measured requests with valid request-scoped
  evidence. Default-disjoint32/48GiB canonical budgets do not clear16GiB preload.
  [Residency evidence](docs/right-semi-residency-2026-09-11.md).
- Same-binary aggregate-route control:16typed-correct outputs,276-file archive.
  Q1 morsel/generic ratios0.078444 at16threads and0.179815 at4threads; Q6 mixed or
  slightly slower. Generic Q1 already has16admitted input slots. These are two-block
  diagnostics, not confidence-certified comparisons. [Route control](docs/aggregate-route-control-2026-09-11.md).
- Current generic-Q1 profile:100snapshots and a correct output,22-file archive.
  Decoder and aggregate-update leaf frames suggest several costs; inlining does
  not identify validity decoding as dominant. [Profile](docs/generic-aggregate-profile-2026-09-11.md).
  Q9/Q13/Q17 diagnostic validates9outputs; Q17 shows SEMI/CASE but raw aggregate
  input remains1slot. [Diagnostic](docs/right-semi-measurement-2026-09-11.md).

No DuckDB leadership is certified. Remaining gates include shared CPU throughput,
native admission, full provider/residency/resource/concurrency acceptance, legacy
join/page/result allocation boundaries and short-query precision. Native Anti coverage now separates NOT IN semantics from ordinary Anti operators;
full native resource acceptance remains open. Earlier partial
ownership gains do not establish a safe default; the4thread Q18 regression and
16GiB preload refusal remain historical negative evidence. Canonical GPU has no
successful device execution. Identical-binary customQ6 precision control remains
failed (upper95%1.130588); its dependent comparison was not run.

Earlier candidate measurements, intermediate checkpoints and validation histories
are retained in the [September11 history](docs/agent-checkpoint-history-2026-09-11.md)
and linked reports. Their active states and binary-specific results are historical.

## Start here

- [Current decoder candidate and validation](docs/repeated-validity-decoding-2026-09-11.md)
- [Latest frozen provider screen](docs/right-semi-provider-screen-2026-09-11.md)
- [Current same-source CPU control](docs/aggregate-route-control-2026-09-11.md)
- [Latest frozen IPC/GPU residency screen](docs/right-semi-residency-2026-09-11.md)
- [Benchmark shutdown correction](docs/benchmark-worker-shutdown-2026-09-08.md)
- [Earlier scheduling candidate and tests](docs/aggregate-batch-dispatch-2026-09-08.md)
- [Parallel aggregate measurements and Q20 regression](docs/parallel-aggregate-candidate-2026-09-08.md)
- [Current architecture and testing map](docs/architecture.md)
- [Earlier canonical provider status and qualifications](docs/current-canonical-sf10-2026-09-07.md)
- [Existing implementation epic](.claude/epics/realistic-benchmarks-duckdb-leadership/epic.md)
- [Local DuckDB/ClickHouse source comparison](docs/local-engine-source-comparison-2026-09-07.md)
- [Active join stream and profiling gap](docs/join-stream-source-audit-2026-09-08.md)
- [Opt-in join instrumentation and completed diagnostics](docs/active-join-profile-2026-09-08.md)
- [Reproduced serial aggregate input](docs/serial-aggregate-input-2026-09-08.md)
- [Bounded parallel input candidate and live build](docs/parallel-aggregate-input-2026-09-08.md)
- [Reproduced scan-budget batching cliff and implementation sequence](docs/scan-budget-batching-cliff-2026-09-08.md)
- [Audit and path forward](docs/project-audit-2026-09-05.md), [original custom SF10 baseline](docs/benchmark-baseline-sf10-2026-09-05.md)
- [Prescan error contract](docs/shared-prescan-error-propagation-2026-09-08.md), [IPC admission](docs/ipc-preload-admission-2026-09-08.md)
- [Documentation index](docs/README.md), [README](README.md), [Claude history](CLAUDE.md)
- [Preserved pre-consolidation guide and checkpoint history](docs/agent-checkpoint-history-2026-09-08.md)
- [CCPM skill](.agents/skills/ccpm/SKILL.md) when planning or tracking delivery

Planning artifacts live in `.claude/`. Do not substitute `.Codex/` or create a
second planning tree. Older guide versions are preserved in Git and the linked
history; their validation counts and pending states are not current certification.

## Mandatory working rules

1. Stay inside this repository unless the task makes leaving necessary.
2. Never use `/tmp` for task files. Use the supplied scratchpad or repository
   `.scratch/`. Set `TMPDIR` to repository scratch for tools/tests that use temp files.
3. Project file edits are pre-approved. Respect the active tool sandbox and obtain
   required tool escalation; do not bypass it through another mechanism.
4. Before every commit, run `cargo fmt --all -- --check`; fix errors with
   `cargo fmt --all` and recheck.
5. After every context compaction, review session changes and update this guide
   for significant new modules, architecture, contracts or dependencies. Put
   detailed evidence in the linked documents; record “no architectural change”
   only when that is the actual review result.
6. Preserve user changes, inspect `git status` before edits, and do not mix unrelated
   modifications into a commit.

## Build, test, benchmark and engine containment

Every build, test, benchmark, engine invocation and script that may launch them
must run through the memory-capped wrapper. Include `cargo check` and `cargo clippy`
because they compile code. Formatting and read-only source inspection do not
need a build cgroup.

**The implemented wrapper is `scripts/claude-safe-build.sh`.**
The previous Codex conversion named `scripts/Codex-safe-build.sh`, which does not
exist in this checkout. Use the real implementation; its historical filename
does not change the safety contract.

```bash
mkdir -p .scratch
# Generate the required small fixtures on a clean checkout
TMPDIR="$PWD/.scratch" scripts/claude-safe-build.sh cargo run --locked -- generate-parquet --sf 0.001 --output data/tpch-1mb
TMPDIR="$PWD/.scratch" scripts/claude-safe-build.sh cargo run --locked -- generate-parquet --sf 0.01 --output data/tpch-10mb
TMPDIR="$PWD/.scratch" scripts/claude-safe-build.sh cargo build --locked --release
TMPDIR="$PWD/.scratch" scripts/claude-safe-build.sh cargo test --locked
TMPDIR="$PWD/.scratch" scripts/claude-safe-build.sh cargo test --locked --test spill_tests
TMPDIR="$PWD/.scratch" scripts/claude-safe-build.sh cargo test --locked --features lance
cargo fmt --all -- --check
```

The wrapper uses `systemd-run --user --scope` with MemoryMax (default 80G),
MemorySwapMax=0 and CARGO_BUILD_JOBS=8. Tune `SAFE_BUILD_MEM` and `SAFE_BUILD_JOBS`
to the host. Coordinate heavy jobs: several 80G scopes can still overcommit the
host together. If containment is unavailable, resolve it; never fall back to bare
execution. Release LTO can consume substantial memory.

The existing Claude/Codex hooks are supplementary guardrails. Do not infer that
a command is contained merely because a hook accepted it. Verify the actual
wrapper/cgroup. Hosted CI needs an explicitly safe runner strategy.

## Memory and correctness requirements

The engine must be resource-safe by default: exceed the working budget by
spilling or refuse cleanly by name. OOM, allocator aborts and silently incorrect
results are failures. Do not add an opt-out from memory safety.

- Preserve spillable joins, aggregates and external sort as the default policy.
  Specialized paths must obey the same query-wide budget and SQL semantics.
- Account for hash metadata, dictionaries, dense accumulators, queued batches,
  merge scratch and final results, not just Arrow payload size.
- Current `MemoryPool::observe` telemetry is a local-estimate high-water mark,
  not proof of query-wide reservations or exact RSS.
- The Linux binary starts with `enforce_process_memory_cap`: RLIMIT_DATA defaults
  to 64G, adjusted before startup with `QE_MEM_CAP`, floor 256MB, no unlimited
  spelling. It counts anonymous mappings and can cause allocation aborts.
  It is host protection, not graceful query memory management.
- `disable_transparent_hugepages` remains deliberate. Preserve it; use
  `QUERY_ENGINE_ALLOW_THP=1` only for a contained, controlled experiment.
- Call `check_partition` in operator execution; also ensure consumers drive
  **all** declared partitions. Range checks cannot detect omitted partitions.
- Physical result reuse requires representation-aware expression identity. Expr equality now preserves decimal coefficient/scale, float bits and nested
  literal representation; scalar numeric equality alone cannot prove array type/scale
  equivalence; preserve numeric SQL equality separately.
- Estimates, samples, ranges and NDV heuristics are for costing. They must never
  prove uniqueness, foreign keys or semantic equivalence.
- Unsupported capabilities must return an explicit error or a documented,
  always-correct fallback. Vector search is exact unless approximation is
  explicitly requested.

Remaining source-level risks include morsel aggregation budgets, in-memory join
admission and collected results. Estimated-uniqueness rewrites now require structural
ordinal key proofs; see `src/optimizer/properties.rs`. Shared
CTE/subquery materialization now drives all declared partitions and has a fixed
multi-partition regression; other consumption boundaries still require review.
Do not treat older “all paths certified” prose as proof of closure.

## Current implementation orientation

Dependencies: Rust 2021 / declared MSRV 1.93.0; Arrow/Parquet/Flight 58;
sqlparser 0.62; Tokio + Rayon; optional Lance 10, GPU and Pulsar. Verify
`Cargo.toml`/`Cargo.lock` before dependency work.

| Concern | Starting point |
|---|---|
| Query API, registry, metrics, mutations | `src/execution/context.rs` |
| SQL binding and plan semantics | `src/planner/{binder,logical_plan,logical_expr,schema}.rs` |
| Rewrite pipeline and live join DP | `src/optimizer/mod.rs`, `rules/join_reorder.rs` |
| Operator selection and provider routing | `src/physical/planner.rs` |
| Stream/partition and copied-output contracts | `src/physical/plan.rs`, `queue_layout.rs`, `fixed_width_output.rs` |
| Live grouped state, routing and scheduling | `src/physical/morsel_agg/{live_spill,input_frontier,parallel_controllers,row_router,row_selection}.rs` |
| Join and aggregate fast paths | `src/physical/operators/{hash_join,hash_agg,morsel_agg}.rs` |
| Spill algorithms and fault hooks | `src/physical/operators/spillable.rs` |
| Expressions and numeric fusion | `src/physical/operators/filter.rs`, `src/physical/compiled_expr.rs` |
| Providers, native snapshots and mutations | `src/storage/` |
| HTTP/Flight, partial/final and gather | `src/distributed/` |
| Configuration and memory cap | `src/execution/memory.rs` |

The engine is already parallel/vectorized with statistics-based join order.
The provider API often returns collected batches, and some planning/join/result
paths still materialize data. General distributed fallback gathers to an
initiator; it is not a distributed shuffle join. Consult the architecture map
before treating an old limitation or experiment as current behavior.

## Evidence and testing rules

Use focused tests appropriate to the change, then required integration/cap gates.
Data-dependent tests require fixtures; missing fixtures/hardware must be reported
as skips or blockers, not successful coverage.

For SQL/operator changes, include duplicates, NULLs, empty/nonempty outputs,
multiple batches/partitions, relevant encodings, and both ordinary/specialized
paths. For spill changes assert actual spill and compare with an independent
typed oracle; engine-versus-itself agreement can share a bug.

Record command, revision, features, environment, result and real skips.
Distinguish a source hypothesis, a reproduced failure and a historical report.

For prepared-capability changes, inspect the actual workload boundary before an
expensive optimized comparison. `examples/prepared_plan_probe.rs` accepts
`SETUP.json SQL.sql root[.CHILD_INDEX...] [copied|admitted]` and reports preparation
without output polling; preparation may execute build inputs. It uses the context
pool, so availability is not proof of query-time concurrency or complete admission.
Runtime typed validation and frontier traces are still required.

Benchmark rules:

- Use a matched DuckDB reference and a query-time ceiling of 10× DuckDB.
  If the query exceeds it, it fails. A startup watchdog allowance is separate.
- Historical August constants in `CLAUDE.md` are not current baselines.
  Rebaseline and update the harness whenever SQL, data or comparison conditions change.
- `scripts/safe_benchmark.sh` now delegates to the versioned harness in
  `scripts/benchmark/`; legacy flags fail with migration guidance. The new
  embedded runner and provider adapters preserve matched boundaries and typed
  validation. Certification belongs to each frozen binary/workload; newer source
  still needs its provider/resource/concurrency gates.
- Separate raw Parquet, decoded IPC cache, native, Iceberg, Lance and GPU residency.
- Current built-in data/SQL is custom TPC-H-derived. Do not call it standard
  TPC-H or use it as the sole optimizer evaluation.
- Match schemas, data, threads/affinity, memory, cache state and timing boundary.
  Preserve all samples, failures, plans and provenance; report per-query ratios,
  geometric mean and suite total.
- Correctness, completion, refusal, timeout, OOM and performance are separate outcomes.
  Empty results can be correct; row-count matching alone is not correctness.

At each completed cycle, run canonical SF10 across provider/residency modes,
record all outcomes, commit and push an intermediate checkpoint, then continue
the goal. This cadence is explicitly authorized by the user; preserve failed gates.

## Documentation maintenance

Use dated research/benchmark documents with source links and reproducible
artifacts. Keep long incident narratives and experimental tables out of this
entry point. Update `docs/architecture.md` when contracts/module routing change.
Preserve useful Claude history with explicit dates and qualifications.
