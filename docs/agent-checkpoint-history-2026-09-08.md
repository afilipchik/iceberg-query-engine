# Historical Codex guide snapshot — 2026-09-08

Preserved before consolidating repeated checkpoints. This is historical evidence,
not the current operational guide. Use [AGENTS.md](../AGENTS.md) for current rules.
Older statements that work is pending or complete apply only to their dated source.

# Query engine: Codex working guide

Current scheduling candidate keeps the same canonical owners but dispatches
serially below256 rows per active owner, or when only one owner has work.
Alternating serial/parallel real-spill and hot-key tests pass. Final gate:
896 library passes (10 ignores),37 integrations. Source is frozen and release
measurement is pending; Q20's regression is not yet closed. No new dependency or
ownership contract. [Evidence and build handle](docs/aggregate-batch-dispatch-2026-09-08.md).

Current live aggregate candidate: `row_selection`, `row_router` and
`parallel_controllers` route canonical keys to at most four query-pool-backed
spillable state owners. Scoped per-batch work retains input and admitted indices;
startup admission may choose one worker before consumption. Final validation:
895 library passes (10 explicit ignores),37 integrations. Balanced SF10 Q13
measures31.6% lower median latency with32 typed outputs passing. Broader
regression/time-gate acceptance is still open. No dependency change.
Full raw SF10 now has51 typed completions but48 within the time gate; Q20 newly
exceeds its ceiling. Balanced Q20 confirms4.4% slower, so the candidate is not
accepted. Q13 outer/Q20 phase traces identify small-batch processing overhead.
[Current contracts and build checkpoint](docs/parallel-aggregate-candidate-2026-09-08.md).

Current performance evidence: canonical SF10 Q13 ingestion remains4.4s at1/4/16
threads; all18 diagnostic outputs pass. In that frozen binary, concurrent input feeds one
state controller. A hash-only probe is insufficient to justify changing hashing.
No architecture/dependency change in these diagnostics. Next bounded parallel
state work and its gates are linked in [the scaling report](docs/aggregate-thread-scaling-2026-09-08.md).

Shared-table prescan now propagates provider errors through physical planning;
failed scans cannot become cache misses followed by a successful retry. A
fail-first provider reproduced the old behavior. Successful shared caching remains.
No dependency change. See [validation and remaining boundaries](docs/shared-prescan-error-propagation-2026-09-08.md).

Current benchmark preparation contract: `benchmark_support/ipc_preload.rs`
preflights immutable IPC streams and retains a conservative context-pool allowance
before decoding, including explicit host-Arrow GPU inputs. The original SF10 setup
now refuses cleanly in debug at4GiB query/12GiB process caps; two helper tests and
an admitted three-batch DuckDB oracle pass. Compression/dictionary deltas refuse.
This is not a general decoder bound or completed residency coverage. No production
engine architecture/dependency change. See [evidence](docs/ipc-preload-admission-2026-09-08.md).

UTF8 group output now borrows validated canonical string fields before a single
copy into admitted Arrow buffers; intermediate owned scalars are removed.
Gates:889 library passes (10 ignores),31 selected integration passes. Release
comparison measures9.0% lower SF10 Q10 median with32/32 typed outputs passing;
normal-deadline raw SF10 finishes51/66 valid pairs with five query deadlines missed.
Earlier provider baselines use the
frozen pre-change binary. See [output ownership evidence](docs/borrowed-key-output-2026-09-07.md).

Current optimizer contract: LEFT/RIGHT non-preserved-side ON predicates have a
separate pushdown path from WHERE predicates, with qualified ownership and an
explicit total-expression policy. Gates:888 library passes (10 ignores),28 selected
integration passes. The implementation now measures17.0% lower Q13 median on
unchanged SQL in a balanced comparison with32/32 typed outputs passing. Q13
still times out in canonical SF1; fresh SF10 validation remains pending.
See [outer ON predicate evidence](docs/outer-on-predicate-pushdown-2026-09-07.md).

2026-09-07 current validation: the new live aggregate boundary now resolves
runtime dictionary arrays to borrowed logical values for keys and states, including
both NULL kinds and changing codebooks across spill. Gates:888 library passes
(10 ignores),26 integration passes; all six reproduced canonical dictionary
failures pass typed DuckDB oracles in debug diagnostics. The first release SF1
smoke remains failed, including a separate Q13 timeout. No speedup or new SF10
baseline is claimed. See [dictionary repair](docs/live-dictionary-boundary-2026-09-07.md).

The production simple grouped aggregate route now uses `morsel_agg/live_spill.rs`:
bound schemas, a single partial-state controller, concurrent owned input frontier,
pressure-driven spill/merge, and admitted Arrow/result ownership. It never replays
input after a budget abort. Both unchanged replay tests and the original256KiB
decimal spill-completion test pass. Final gate:887 library passes (10 explicit
ignores),25 integration passes, zero failures. The single aggregation working set
is provisional; post-filters/unsupported layouts and global queries still use the
ordinary route. Provider/evaluator allocation coverage, parallel state policy and
performance/resource certification remain open. No speedup is claimed.
See [live integration evidence](docs/live-spill-integration-2026-09-07.md) and the
[current implementation contract](.claude/epics/realistic-benchmarks-duckdb-leadership/updates/2026-09-07-fused-spill-implementation.md).

Latest correctness repair: floating MIN/MAX typed ingestion and hash fallback
updates/merges share the existing SQL NaN/signed-zero comparator. Before-fix
tests reproduce wrong results in ordinary and both fused modes. Final validation:
808 library passes (10 explicit ignores), 33 integration passes; formatting
passes. No allocation, dependency or routing change; spill replay remains open.
See [the source comparison and evidence](docs/float-extrema-ordering-2026-09-07.md).

Latest ingestion contract: `AggregationState::process_evaluated_from` returns
an exact first-unapplied-row cursor on typed admission denial, preserving
retained evaluated arrays and partially migrated state. Ordinary callers still
return errors; complete serialization and bounded merge/controller integration remain open.
Final validation:799 library passes (10 explicit ignores) plus39 integration
passes. Both fused budget-transition regressions remain red. See
[the cursor contract and evidence](docs/aggregate-ingestion-cursor-2026-09-07.md).

Latest admission contract: `MemoryLimit` is a typed pool-denial error carrying
the limiting hierarchy node and byte snapshot; `is_memory_limit()` unwraps shared
errors. Display/log category remain compatible. Aggregate normalization and
bare-SUM demotion preserve untransferred state on denial. Final validation:
796 library passes (10 explicit ignores) plus39 integration passes. Both fused
budget-transition tests still fail; bounded state storage and spill control remain
open. The cursor follow-up is recorded above. See [the admission repair](docs/aggregate-admission-migration-2026-09-07.md).

Retained evaluated-input contract: disjoint fused aggregation routes and updates from
the same evaluated arrays (`prepare_aggregate_batch` /
`AggregationState::process_evaluated_batch`). This fixes reproduced duplicate
groups from volatile expressions.54 selected tests pass, no ignores; both
consuming-source budget-transition tests remain red. The subsequent cursor is
recorded above; state spilling remains open. Optimized performance and full
queue/scratch ownership remain unverified. See
[the repair and exact evidence](docs/fused-evaluated-input-2026-09-07.md).

Earlier2026-09-07 source-only continuation review: no production architecture or
dependency change in that step. Both consuming-source budget-transition tests were red (0pass/2fail):
shared/disjoint fused fallback re-executes each partition. The state-preserving
spill implementation remains open. Full and both protected frozen benchmarks
are terminal; current653/662 and historical647/662 protected follow-ups each
validate1632 requests with no per-order mean/median regression above10%.
See [the current execution checkpoint](.claude/epics/realistic-benchmarks-duckdb-leadership/execution-status.md)
and [local engine spill contracts](docs/local-engine-source-comparison-2026-09-07.md).

Latest working-tree lifecycle change: fused aggregation preserves producer/task
errors, cancels pending sibling producers through an owned JoinSet, and wakes the
coordinator on worker failure before joining workers. Error branches never retry
input. All25 selected lifecycle/aggregate/cleanup tests pass; optimized timing is
pending. The group-budget replay path and256KiB spill-completion issue remain open.
See [the lifecycle report](docs/fused-aggregate-error-lifecycle-2026-09-07.md).
This change is additional to the decimal-bound experiment below and absent from
the completed frozen662 benchmarks. No dependencies changed.

Working-tree follow-up: shared decimal precision validation now uses compile-time
bounds, with a new signed-boundary regression. Compilation and 59 selected tests
pass. A separate 256KiB spill-completion test fails with both the frozen helper
and the new helper; see the [parallel budget finding](docs/fused-aggregate-budget-overshoot-2026-09-07.md).
Optimized decimal performance remains unmeasured. The completed full SF10 run
uses frozen662 executables/source and does not measure this edit.
See `.scratch/decimal-precision-bound-repair/status.json` for the exact delta.

2026-09-07 active checkpoint: dense aggregate selection now checks inclusive
signed ranges before narrowing (`physical/dense_domain.rs`). Full Int64 spans
must use the existing hash/general route. Direct dense aggregation has an admitted
NULL-key slot, checked/query-owned index scratch and SUM seen-value bitmaps;
all-NULL SUM/AVG emit NULL. Both Parquet/native routes share these semantics.
Final Lance/GPU validation passes847unique selected tests including isolated IPC
and8CUDA cases; only the existing flatten_exists ignore remains. New optimized
build and both new domain probes pass, including typed values and runtime-path
evidence. Broader optimized semantic/decimal gates pass; resource/GPU/performance
gates remain in progress. Optimized spill caps and40CPU/40device controls pass;
component profiling ends with112passes and a nativeQ1 control timeout
(7requests not attempted). Full balanced canonical SF10 completes9680requests;
all typed/time gates pass. Equal-weight required-track suite ratio is0.9783
against653. Both current/historical protected follow-ups are now terminal and
below the10% per-order regression threshold; older failed experiments remain
preserved. The working-tree spill transition still has two red regressions.
See the [full results](docs/dense-domain-full-sf10-2026-09-07.md). No whole
performance or DuckDB leadership pass is claimed.
See [domain and NULL fixes](docs/dense-aggregate-domain-and-null-semantics-2026-09-07.md).

Frozen658 integrated the raw state arena and passed its selected optimized/spill
gates, but new domain probes found existing bugs:653crashes on a full signed
shared merge;658refuses it. Both reject dense NULL keys;658also returns0/NaN for
all-NULL SUM/AVG. No658performance screen was run. Preserve its frozen evidence.
[The arena report](docs/aggregate-state-arena-2026-09-07.md) describes its query
ownership/fallible merge contract and remaining nested/general/bare-float gaps.

Historical frozen release653 (query-pool propagation through decimal aggregate
fallbacks); its852 selected tests, optimized semantics, two real-spill caps and
40CPU/40device controls pass. These are historical results for653, not validation
of later source. See [fallback audit](docs/aggregate-fallback-pool-audit-2026-09-07.md).

The647/649 full baseline completed9,680requests. Its protected follow-up completed
1,632requests: LanceQ17 retains a median regression in all four orders; LanceQ11
retains one flag. IPCQ22 and IcebergQ8 did not repeat their flags in that follow-up.
See [protected findings](docs/qualified-identity-protected-followup-2026-09-07.md).
All those benchmark/diagnostic jobs are terminal. No DuckDB leadership, complete
query-wide memory ownership or full current-source performance acceptance is claimed.

## Start here

- [Local DuckDB/ClickHouse source comparison and next experiments](docs/local-engine-source-comparison-2026-09-07.md)

- [Aggregate fallback pool audit and pending reproducer](docs/aggregate-fallback-pool-audit-2026-09-07.md)

- [Current architecture and test map](docs/architecture.md)
- [Computed-result memory budget reproducer](docs/result-memory-boundary-findings-2026-09-06.md)
- [Constant casts and SQL float semantics](docs/float-comparison-domain-findings-2026-09-06.md)
- [SUBSTRING/VALUES fixes](docs/string-domain-findings-2026-09-06.md)
- [Shared subquery projection pruning](docs/ipc-scalar-subquery-attribution-2026-09-06.md)
- [2026-09-05 audit and path forward](docs/project-audit-2026-09-05.md)
- [Fresh SF10 baseline and evidence](docs/benchmark-baseline-sf10-2026-09-05.md)
- [Systemic correctness fixes and current validation](docs/systemic-correctness-fixes-2026-09-05.md)
- [Current contract hardening and remaining ownership](docs/systemic-contracts-2026-09-06.md)
- [Canonical SF1 failures and small reproducers](docs/canonical-sf1-findings-2026-09-05.md)
- [README](README.md) for the product/CLI entry point
- [Claude's preserved engineering record](CLAUDE.md) and `.claude/plans/`,
  `.claude/epics/` for historical decisions and evidence
- [CCPM skill](.agents/skills/ccpm/SKILL.md) when planning or tracking delivery

Planning artifacts actually live in `.claude/`. Do not mechanically substitute
`.Codex/` or maintain duplicate planning trees. The previous long AGENTS guide is
recoverable with `git show 88849c4:AGENTS.md`.

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

## Documentation maintenance

Use dated research/benchmark documents with source links and reproducible
artifacts. Keep long incident narratives and experimental tables out of this
entry point. Update `docs/architecture.md` when contracts/module routing change.
Preserve useful Claude history with explicit dates and qualifications.

## Current source and evidence — 2026-09-06

Current production has an integrated, unaccepted resident-GPU prototype newer
than the last measured source593. Preserve the frozen binary/artifacts when
attributing timings. Full earlier checkpoint prose is preserved in
[checkpoint history](docs/agent-checkpoint-history-2026-09-06.md).

- Source593 fixes cross-batch row-store type corruption, owns packed data/offsets/
  metadata/temporary views, and composes bounded prepared output through validated
  multi-batch row stores. IPC Q10 improves21.8% versus591. It retains earlier exact
  numeric/semantic/partition repairs, VHT ownership and initialized immutable IN
  membership. General source/decoder/result and query-wide memory remain open.
- [Frozen593 outcome](docs/frozen593-benchmark-outcome-2026-09-06.md): six paired CPU
  screens (five modes plus IPC/591 control) validate1056 requests. All modes still
  have protected scalar regressions. Raw/DuckDB2.4278, IPC0.4830, native3.4571,
  Iceberg0.3229, Lance1.2735 by suite sum of medians. No leadership acceptance.
  CPU58473/provider43730 and archives58894/46216 are terminal0.
- Frozen593 GPU36741/36738 and archive96882 are terminal0: canonical132 valid
  measured requests execute entirely on CPU; custom float80 valid requests have
  39 device runs and one measured GPU-enabled CPU fallback. Mixed routing is not
  resident certification. [Evidence](docs/benchmarks/2026-09-06-prepared-rowstore/README.md)
  includes verified source, binary, typed output, plan, trace and archive hashes.
- The new feature-gpu preparation/session APIs share ordinary sql_impl timing,
  bind exact replanned provider/schema/aggregate/predicate bits and force device
  dispatch in required mode. Worker Prepare/RunResident/ReleaseResident messages
  acknowledge numeric columns plus group codes and isolate active sessions.
  The versioned benchmark now has required policy and matched Arrow preload;
  preparation and device evidence are separate from correctness/time gates.
- Resident GPU correctness repairs are integrated: exact provider/code cache
  identity, scalar empty-output NULL/COUNT semantics, explicit nonfinite-input
  refusal, direct-column-only MIN/MAX, Float64-only SUM outputs, and delayed cache jobs with
  checked replacement accounting. Actual CUDA reproduced the old scalar/NaN
  errors and duplicate-byte inflation; source599 is preserved as rejected.
- Final gate77913 passes699 library tests, eight explicit real-CUDA tests and one
  explicit IPC test (708 selected Rust tests); only the pre-existing ignored
  flatten-dependent-join test remains excluded. Default adapter check passes.
  Full benchmark harness98930 passes100 including both opt-in cases.
  COUNT(*) is admitted contextually by the resident expression proof. Full
  unchanged float-fixture SQL passes actual GPU + typed DuckDB debug validation
  (30531). Run this end-to-end check before future GPU release benchmarks.
  Release36955 / validation32255 completes40/40 required GPU samples and40/40
  CPU-control comparisons on the unchanged float fixture; zero fallback or
  time-gate failures. Two1MiB cache cases refuse explicitly. This is a narrow
  resident development result; hard memory admission and general leadership
  remain open. No heavy job is currently live. Failed candidates remain archived.
  [Current validation and next steps](docs/gpu-resident-validation-2026-09-06.md).
  [Current implementation and limits](docs/gpu-resident-session-implementation-2026-09-06.md).
- Next CPU attribution: raw variable output/decoder ownership and IPC scalar
  subquery work are different boundaries. [Decoder/lease prerequisites](docs/variable-output-ownership-integration-review-2026-09-06.md)
  explain why post-decode byte sizing alone does not establish safe parallelism.
  GPU host/VRAM/kernel hard admission, broad public/concurrency/holdout/provider
  requirements and overall DuckDB leadership remain incomplete.

Session logs/drafts: `.scratch/gpu-residency-protocol/`,
`.scratch/gpu-residency-engine/`, `.scratch/gpu-residency-runner/`,
`.scratch/gpu-resident-hardware-lifecycle/`. The validated float IPC fixture is
`.scratch/public-bench/custom-gpu-float-ipc-01/provider.json`. Source593's frozen
benchmark binary is `.scratch/prepared-rowstore-binary` (SHA25618e067aa…);
its older timings must not be attributed to the current prototype.

2026-09-06 current architecture checkpoint: primitive projection evaluation scopes
its query pool; reservation-owned builders admit fixed capacity and old-plus-new
growth. Direct float filling and Arrow lexical string formatting now have regression
tests. `QE_TRACE_RESERVED_EXPRESSIONS=1` is diagnostic-only; keep disabled during
latency measurements. No dependency changes. Detailed status/history is linked above.


## Superseded operational checkpoint before the ed721285 provider review

The following accumulated entry is preserved as history, not current status.

## Current checkpoint — 2026-09-08

Frozen ed721285 full SF10 screens: raw60/66, Native60/66, Iceberg63/66 and
Lance57/66 paired validations. All246 completed engine outputs validate including
six supplemental Q9 checks; reference crashes keep their timing pairs invalid.
No track passes full acceptance. Details: `docs/admitted-queue-provider-screen-2026-09-08.md`.
Normal benchmark Worker teardown now uses bounded EOF cleanup; deadline/protocol
failures still abort immediately.112 harness tests pass with2 existing skips,
including real DuckDB spill-file cleanup. No new production engine change in
this follow-up. See `docs/benchmark-worker-shutdown-2026-09-08.md`.
All jobs are terminal. Next confirm possible raw Q2/Q19 regressions with controlled
pairs, then integrate admitted ownership in the aggregate input frontier; that
scheduler still ignores the new protocol. IPC/GPU and resource gates remain open.

Session review: a distinct `PreparedAdmittedInput` protocol is now under test in
the shared spillable input queue, eligible raw Parquet scans, and column-only
Project wrappers. It binds decoder/output leases to the consuming pool and does
not weaken the existing pool-independent copy contract. Scheduler metadata and
boxed prepared streams are admitted separately. This source postdates the frozen
benchmarks. Library41692 passes980 tests with10 existing ignores;33 selected
integrations pass without skips. Error reporting now reaps cancelled producers
before returning; ordinary drop retains asynchronous cancellation ownership.
Release77076 is complete (frozen ed721285). Strict Q12 diagnostic34658 returns
correct typed output at both budgets but fails both ceilings:947.924/963.811ms
against932.184/944.380ms. Build-input traces confirm16 admitted slots; no OOM or
leftover spill files. No heavy job is active. Performance acceptance remains open.
See `docs/admitted-queue-q12-2026-09-08.md` and
`docs/admitted-queue-protocol-2026-09-08.md` for current evidence and limitations.

The live grouped aggregate routes canonical keys into at most four spillable
state controllers under one query pool. Small batches (below256 rows per active
owner) and single-owner batches run serially without changing ownership. Partial
states spill/merge without replaying evaluated input.

The new `input_frontier` separately schedules capable partition streams on owned
Tokio tasks. Explicit output bounds admit a finite slot envelope; batches retain
their charge and demand permit through handoff. Prepared streams are consumed
once. Unknown or underfunded streams remain serial. Errors stop/join tasks before
return; cancellation retains owners until running synchronous polls stop. Copied
output admission does not certify provider decoder scratch or exact RSS.

Current candidate:896 library tests and39 selected integrations pass;10 documented
library ignores remain. Optimized build26752 is complete. All12 diagnostic and144
paired outputs validate. Against the preceding frozen CPU control, Q5/Q10/Q20
latency is83.4%/57.2%/84.8% lower; Q9/Q12 are unchanged and Q13 is1.9% lower.
Fresh raw SF10 validates57/66 pairs: Q5/Q10 recover their deadlines, while
Q9/Q12/Q13 still time out. Native validates60/66; Q1/Q13 still time out.
Iceberg validates66/66, suite0.624× DuckDB,18/22 wins, worst query2.20×.
Lance validates58/66 pairs: Q1/Q13 engine timeouts plus Q9 reference refusal/crash.
Two additional admitted parallel-input tests force real spill and pass exact
state oracles with zero retained pool charges. Budget matrix36181 is complete:
70/72 outputs validate; both binaries hit the process watchdog on the second
Q12 request at1GiB query/4GiB process limits after a correct158.6s first request.
Limit-isolation6280 confirms the query-budget trigger:159.1s at1/12GiB versus
1.37s at4/4GiB, both exact. Variable-width pressure produces310,803 one-row build
batches versus458 normally. The scan policy remains unfixed; bounded variable
decoding is the next priority. Other residency and concurrency gates
remain open. No leadership pass is claimed.
Current source now adds optional physical execution details: Streaming Parquet
plans expose reader quantum, pressure, types, route and copy capability. One
new regression checks displayed versus actual nullable-string batches. The latest
source library gate71744 is complete:973 pass,10 documented ignores. These edits
postdate the frozen binary; no new performance claim applies to this source.
Storage components provide admitted positional reads, bounded Compact
headers/CRC checks, UNCOMPRESSED/SNAPPY/ZSTD page buffers and retained column cursors.
Direct snap1.1.1 reuses the pinned transitive version. `admitted_flat_column`
(replacing `admitted_utf8_column`) shares page/level/denial state across UTF8 and
fixed-width PLAIN/dictionary output. `admitted_plain_fixed` preserves integer,
date/timestamp, float/Boolean and exact decimal values in admitted typed chunks;
it validates exact ID domains and retains input/output owners. Caller-validated
logical annotations are required; no implicit rescaling occurs. Tests include
real V1/V2 multi-page files, typed numeric/date/decimal oracles, nullable/all-NULL
strings, admission retry and unsupported delta refusal without replay.
`admitted_batch` now aligns column chunks with retained prefixes, declared-row/
type/NULL checks and pre-admitted handoff metadata attached to output buffers.
Typed clones and ArrayData retain leases. `admitted_row_group` now validates
flat metadata, counts, annotations/codecs and repeated projections before page
construction; footer/cache admission and snapshot pairing remain caller contracts.
`admitted_gather` now copies flat survivors into exact admitted buffers; WHERE
masks discard NULLs, ID domains are exact and output leases survive empty typed
arrays/ArrayData. `CompiledPredicate::evaluate_admitted` now reserves numeric
register/mask storage and handles nullable Boolean programs with per-register SQL
validity. Compilation metadata and unsupported predicate families remain outside
that evaluation contract. The separate `compile_for_admitted_evaluation` adds
UTF8 comparisons, constant IN/NOT IN (NULL-aware) and LIKE/NOT LIKE using the
existing allocation-free matcher. Ordinary compiler selection is unchanged;
legacy evaluation refuses extended instructions. `compile_reserved` now admits
bounded construction before cloning and retains actual program Vec/String capacity
charges after shrinking temporary allowance. AST/schema remain caller-owned.
Shared qualified-column resolution no longer formats a temporary String; namespace
and ambiguity rules are preserved. Metadata cache identity now uses the opened
file (Unix device/inode/ctime plus size/mtime), keeps schema-key owners and hands
builders the same file handle;32 selected integrations pass. Later in-place
mutation still requires immutability. Cache retention is bounded to256 variants/
256MiB reported estimates. Raw scan row-group work now shares query-owned metadata
from pruning and checks the independently opened file version before decoding;
replacement refuses, and eviction cannot trigger per-group reparsing. This requires
Unix file identity. Footer/query metadata admission and admitted decoder routing
remain; see `docs/metadata-cache-identity-2026-09-08.md`.
Live raw variable-width pressure scans now route through
`physical/operators/streaming_parquet_scan/admitted.rs`, with the planner query
pool, reserved static/runtime masks, survivor gather and repeated/reordered output.
They use an8192-row maximum/64KiB preferred string target, with actual admission;
errors terminate without legacy replay. ZSTD now uses a pre-admitted static context
and fixed destination, through the existing pinned zstd-sys2.0.13+zstd1.5.6
(experimental bindings, no version upgrade). Actual SF10 footers use supported
ZSTD/PLAIN/dictionary encodings; release/Q12 measurement remains before any
performance claim. The first frozen candidate557a9582 failed strict Q12:1/12GiB
exposed an erroneous seven-padding-value bound;4/4GiB timed out. Hybrid decoding
now accepts final full-block padding using logical page counts, with extent/domain
checks intact. A real DuckDB1.4.4 fixture passes. Corrected frozen ea6fb42d then timed out in
both strict Q12 cases; no complete engine result/gain certified. A shared Project/
join-build queue has16 partitions but1 slot because its copy bound is unknown;
completed build-input polling takes about0.85s. Investigate a safe bounded/admitted
output contract, not unchecked extra permits. See the input-serialization report. Preparation now retains plain/override
metadata views, admits projected-schema cloning and shares immutable work/predicate
data across streams. A separate consuming-pool-bound admitted-buffer protocol is
required before parallel queue promotion; do not reuse the pool-independent marker.
See `docs/admitted-queue-preparation-2026-09-08.md`.
See `docs/packed-block-padding-2026-09-08.md` and the ZSTD report. Footer/schema/query metadata, ordinary fixed-width and IPC
paths remain outside this new decoder contract. See
`docs/live-admitted-parquet-scan-2026-09-08.md` for scope and next steps.
Earlier candidates and their distinct frozen provider results are preserved in
the linked reports. Verify a live job before restarting it.


## Checkpoint consolidated during expression-quantum release

Historical snapshot; active-job statements below are superseded by AGENTS.md.

## Current checkpoint — 2026-09-08

Generic grouped expression evaluation now uses1024-row slices while retaining
full source backing charges, input owners and demand permits through all slices.
View/evaluation metadata is admitted before construction. Retained input still
deduplicates primitive allocation identities per batch; unknown layouts remain
conservative. See docs/aggregate-expression-quantum-2026-09-08.md and the retained
input report. Library992pass/10ignores; focused13pass; native improves to9pass/1fail
at unchanged256KiB. Join still refuses383984bytes. Release74944 is ACTIVE;
no source edits or overlapping heavy jobs until its handle is terminal. Freeze
then measure dispatch/performance impact; no current-source certification yet.

Optional shared native prescan now respects scan_budget_exceeded before reading,
so an over-budget self-join plans two streaming scans instead of refusing during
cache preparation. At that checkpoint native had four completion failures at256KiB;
the newer bounded-expression results above supersede that count. See docs/native-shared-prescan-budget-2026-09-08.md.

Newest source fixes a reproduced physical-planner wrong answer: computed Project
was omitted by aggregate source extraction (SUM4 instead of34). Parquet/native
now share an exact identity guard. 988 library passes/10 existing ignores;
48 selected integration passes,4 native streaming failures reproduced unchanged
against verified frozen9c868dc0 source. No heavy job is active. See
`docs/aggregate-source-projection-correctness-2026-09-08.md`. This postdates all
performance measurements below; alias capability propagation remains open.

Frozen engine ed721285 completed four canonical SF10 screens: raw60/66,
Native60/66, Iceberg63/66 and Lance57/66 valid measured pairs. All246 completed
engine outputs validate, including six supplemental Q9 checks; reference crashes
keep those timing pairs invalid. No track passes full acceptance. One session/
three samples is development evidence, not multi-session certification. See
`docs/admitted-queue-provider-screen-2026-09-08.md` and its immutable archive.
The separate instrumented Q12 budget cases return correct output but still exceed
their fresh strict ceilings; see `docs/admitted-queue-q12-2026-09-08.md`.

The shared spillable input queue consumes `PreparedAdmittedInput` from eligible
raw variable-width Parquet scans through column-only Project. Decoder, scratch and
output leases bind to the consuming pool; output owners survive handoff. This is
separate from the legacy pool-independent copy-bound contract. Queue errors reap
cancelled producers before returning; ordinary drop retains asynchronous cleanup
ownership. The grouped aggregate still uses at most four spillable controllers.
Its input frontier now also consumes the admitted protocol, with an explicit
one-slot/parallel ownership distinction preventing duplicate consumer charges.
This source postdates frozen ed721285; see
`docs/admitted-aggregate-frontier-2026-09-08.md`.

The admitted scan supports flat PLAIN/dictionary values, V1/V2 levels, bounded
headers/CRC and UNCOMPRESSED/SNAPPY/ZSTD pages. ZSTD uses pinned static workspace;
logical counts bound final packed-block padding. Plain/override metadata views
share checked file identity; immutable work/predicates are shared. Footer/plan
residency, legacy/fixed-width/IPC paths and exact query-wide accounting remain
separate limitations. Consult `docs/architecture.md` and the admitted-queue report.

Engine validation:980 library passes with10 existing ignores,33 selected
integration passes without skips; release77076 verified487 input hashes. The
follow-up changed benchmark lifecycle only: normal Worker teardown now uses
bounded EOF cleanup, while deadlines/protocol errors still abort immediately.
112 harness tests pass with2 existing skips, including actual DuckDB spill-file
cleanup. See `docs/benchmark-worker-shutdown-2026-09-08.md`.

Paired Q2/Q19 confirmation validates112 executions and does not confirm the
separate-screen regressions; Q2 remains uncertain. Current frontier source passes
987 library tests (10 existing ignores) and45 integrations without skips.
Release76420 completed as frozen9c868dc0 with488 verified input hashes.
Equivalent-SQL benchmark41823 failed: bare form32/32 typed/gated outputs, ratio
0.99654 (95%0.96729–1.01520); wrapped forms24 warmup timeouts on both binaries,
72 later requests not run. No demonstrated speedup. No heavy job remains active.
See docs/admitted-aggregate-frontier-2026-09-08.md and its immutable evidence.
Compaction review: the admitted-frontier ownership contract is documented in the
architecture map; no further engine or dependency changes in this review.
Next investigate capability routing with exact bound-column mappings; see
`docs/aggregate-capability-routing-2026-09-08.md`.
Keep encoded filtering/selective-batch costs as separate measured hypotheses.
IPC/GPU, resource/concurrency, broader public workloads and leadership gates remain
open. Earlier candidates and superseded checkpoint prose are preserved in linked
reports and `docs/agent-checkpoint-history-2026-09-08.md`.


## Superseded checkpoint before e6a60347 provider completion

## Current checkpoint — 2026-09-08

Frozen3ff868c7 completed corrected-harness canonical SF10 screens: raw57/66,
native60/66, Iceberg61/66, Lance57/66 valid measured pairs. All315 completed
measured/warmup engine outputs validate. Reference failures remain separate from
engine failures; no full acceptance. See docs/budget-quantum-provider-screen-2026-09-08.md.

Reference retirement now closes failed calibration/measured reference workers
before later SQL reuse;118 harness tests pass,2 optional skips. Residency22984
is terminal: canonical control refuses28.6GB preload at16GiB; decoded IPC has a
redundant-flag configuration error. Custom float Q1 warmup fails the time gate;
20 Q6 pairs validate. Both dependent GPU runs reject incomplete CPU controls.
No GPU execution established. Archive538 files verified; no heavy job active.
See docs/budget-quantum-residency-2026-09-08.md. Compaction review found no further
engine architecture/dependency changes; preceding contracts remain as below.
Instrumented custom memory-input diagnostic25084 is terminal1: four input slots
and four aggregate workers, no spill; Q1 still fails warmup. Routing and updates
dominate reported spans. Local DuckDB/ClickHouse source comparison identifies
duplicated canonical key preparation as the next bounded measurement target,
not a proven isolated bottleneck. See docs/memory-aggregate-source-comparison-2026-09-08.md.
Key-preparation component54832 terminal0: retained KeyRows/hashes take0.593–0.602
of double-encoding time across five shapes, eight paired blocks each. One selected
optimized test passes;168 source hashes and7 archived files verified. Test-only
source, no production speedup claimed. The integration checkpoint follows below.
See docs/retained-key-component-2026-09-08.md for limitations and semantic gates.
Production prepared_keys now binds admitted canonical bytes/hashes to the exact
evaluated batch/layout for routing and worker lookup, preserving full equality
and spill cursors. Optional preparation is capped at one eighth of available pool
memory; only admission refusal falls back before state mutation. Focused127pass;
broad994library pass/11ignored,14 selected integrations pass; native9pass/1fail
with the same383984-byte join refusal at256KiB. Release57568 completed1a0ece71,
493 input hashes verified,8m43s. Paired81974 terminal1: canonical Q10 improves
3.97% (95%2.54–5.34%), Q19 improves1.67% (0.27–3.61%); Q12/Q13 still fail gates.
All346 completed canonical outputs correct,339 gated;7 late,9 timeouts,93 not_run.
Custom-memory Q1 all8 warmups correct but late,48 not_run; Q6 no confirmed gain.
Archive1030 files verified. Full screen69779 terminal1 on1a0ece71: raw57/native60/
Iceberg63/Lance57 valid pairs of66 each; all317 completed measured/warmup outputs
correct. Six engine warmup timeouts, one late warmup and two reference-invalid
queries leave27 not_run slots. Provider archive1393 files verified. No heavy job
active. See docs/prepared-key-provider-screen-2026-09-08.md; acceptance remains open.
The shared reference calibration now stops immediately on failure and blocks
resident preparation/dependent work;123 harness tests pass,2 optional skips.
New harness archive158 files verified. See docs/reference-calibration-stop-2026-09-08.md.
See docs/prepared-aggregate-keys-2026-09-08.md.
Latest profiles are terminal and archived231files. Custom Q1 is partial/timeout;
canonical Q10 completes with75% of candidate ingestion in preparation/routing
(418.405 of557.951ms in the sample). Next measure scoped parallel key preparation
under one child budget, preserving canonical owners/cursors and low-memory fallback.
See docs/prepared-key-state-profile-2026-09-08.md. Batch-update dispatch remains
a later source hypothesis; column-wise updates cannot reuse the old spill cursor
after partial mutation. The profile itself changed no production source.
PreparedKeys now uses up to four scoped chunks under the same child budget, with
checked original-row lookup and pre-mutation fallback. Only prepared_keys.rs
differs from1a0ece71. Focused128pass/1ignored; broad995library pass/11ignored and
14selected integrations pass; native9pass/1unchanged join refusal. Release16406
completed e6a60347,493 verified inputs,8m44s. Paired6123 terminal1: canonical
Q10 improves14.29% (95%14.05–14.53%); Q19/Q20 smaller gains. All347 completed
canonical outputs correct;339 gated,8 late,8 timeouts,93 not_run. Q12/Q13 remain
failures. Q2 ratio1.01655 (95%0.91137–1.13388) leaves protected regression unresolved.
Custom Q1 candidate completes18 samples across3/4 blocks; control fails all warmups,
candidate one. No paired Q1 ratio. All82 completed custom outputs correct,77 gated.
Archive1048 files verified. Compaction review confirms scoped chunk preparation
is the only new engine contract; no further architecture/dependency change.
Next: prespecified Q2 follow-up, eight fresh blocks and12 pairs per block on the
same binaries/conditions, fixed-seed whole-block bootstrap; retain both studies.
See docs/parallel-key-preparation-2026-09-08.md.

Completed paired49103 against0ba65b55: Q10 improves7.72% (95%4.31–11.01%);
Q5 slows1.29%; Q2/Q19/Q20 intervals include1. All294 completed outputs correct,
286 gates pass;8 Q12 gate failures,42 not_run. Evidence is immutable. Broader
canonical/provider/resource/residency acceptance and leadership remain open.

Current grouped expression evaluation selects1–8192 rows from pool limit and
evaluated width (scheduling estimate only). The256KiB three-column case retains
1024 rows. Full source owners/leases/permits and actual allocation checks remain.
Library992pass/10ignores; focused14pass; native9pass/1fail at unchanged256KiB.
Join still refuses383984bytes. Only scheduling source and its test differ from
0ba65b55; the paired result above measures this policy change.

Frozen0ba65b55 paired results:293 completed outputs correct,285 pass time gates;
8 Q12 gate failures,43 requests not run. Q10 regresses7.7% (95%6.6–9.3%);
Q2/Q5/Q19/Q20 intervals include1. Preserve that evidence. Retained input still
deduplicates primitive allocation identities per batch while retaining full
backing capacities; unknown layouts remain conservative. See the expression
quantum and retained-input reports for completed resource fixes and limitations.

Parquet/native aggregate source extraction shares an exact Project identity
proof, fixing a physical-planner SUM4-versus34 wrong answer. Computed/renamed
projections cannot be discarded. Optional native prescan respects the provider's
materialization budget before reading; actual source failures still propagate.
Exact binding/capability propagation through aliases remains performance work.

Latest completed equivalent-SQL comparison belongs to frozen9c868dc0: bare32/32
typed/gated outputs, ratio0.99654 (95%0.96729–1.01520); wrapped forms24 warmup
timeouts on both binaries,72 requests not run. No gain demonstrated. Earlier
ed721285 canonical SF10 screens: raw60/66, native60/66, Iceberg63/66, Lance57/66
valid pairs; all246 completed engine outputs validate. Q9 reference crashes keep
Iceberg/Lance timing pairs invalid. No full acceptance claim applies to any track.
Paired Q2/Q19 did not confirm earlier regressions; Q2 remains uncertain.

Admitted raw Parquet supports flat PLAIN/dictionary values, V1/V2 levels,
CRC and UNCOMPRESSED/SNAPPY/ZSTD; static ZSTD workspace and checked metadata
identity are retained. Eligible scans/project wrappers feed admitted join and
aggregate queues. Error shutdown reaps producers; drop retains cleanup owners.
Footer/plan residency, legacy/IPC paths and query-wide accounting remain open.
Normal benchmark worker shutdown uses bounded EOF cleanup; errors/deadlines abort.
See the linked architecture and dated reports for exact contracts and evidence.
Older checkpoints are in docs/agent-checkpoint-history-2026-09-08.md.

Current contracts to preserve:

- Aggregate source extraction must prove exact projection field/value identity.
  Both Parquet and native share the guard; computed/renamed projections cannot
  be discarded. See `docs/aggregate-source-projection-correctness-2026-09-08.md`.

- Runtime dictionary values, both NULL kinds and changing codebooks resolve to
  logical keys/states; physical codes never prove identity.
- Shared prescan propagates provider errors instead of retrying failed sources.
- UTF8 group output borrows canonical strings into admitted Arrow buffers.
- LEFT/RIGHT ON pushdown is separate from WHERE and uses explicit total-expression
  eligibility; no estimate proves semantic equivalence.
- Benchmark IPC preload preflights immutable streams and retains a conservative
  query-pool allowance. Compression/dictionary deltas refuse. This is not general
  decoder memory certification. Reused setup failures are marked unexecuted.



## Superseded checkpoint before frozen11d16e73 residency consolidation (2026-09-09)

## Current checkpoint — 2026-09-08

Current source replaces a fixed-width aggregate-input serialization/decode round
trip with direct typed reads after the existing checked dictionary resolution.
The row transaction, exact retry cursor, numeric updates and variable-value
admission remain unchanged. Three new independent semantic regressions cover
primitive widths, decimal scale, float bits, NULLs, slices and dictionaries.
Focused70124 terminal0:132 passed/1ignored. Broad50678 terminal0:1001 library passes/11ignored,40 integration passes.
Full spill89730 terminal101:6pass/7same failed names as preceding gate.
Release77024 terminal0 in8m43s; frozena023079f,493 source hashes verified.
Only arrow_input.rs differs from476ec119. Paired79406 terminal1:458 completed outputs correct/450 gated,8 late,16 timeout,
142 not_run. Custom Q1 improves21.26%, canonical Q5 improves1.38%; Q9/Q12/Q13
incomplete. Archive1125files/493inputs verified. Residency3109 terminal1; audit77556 terminal0. All244 completed outputs correct,
200 measured pairs valid; four canonical warmup timeouts, dependent canonical GPU
not executed. Custom required GPU40/40 with request-scoped device evidence.
Archive1100files verified. Provider84971 terminal1; audit86520 terminal0:
321 completed outputs typed-correct,240/264 valid measured pairs. Iceberg completes
66/66 in one session, but Q13 exceeds2x DuckDB and leadership remains uncertified.
Archive1403files verified. IPC extent regressions29372 terminal101 reproduced
footer subtraction, block arithmetic/slicing and truncated framing panics:1pass,
2fail. Current source adds shared checked footer/block extents and framing for
native/decoded IPC; focused70608 passes3 tests; broad1001 library passes/11ignored. Native scans10
and table validation12 pass. Four native insert/delete/update queue-bound failures
reproduce with the IPC repair removed (control26497). They are now repaired:
projection/join bound schema adaptation preserves field metadata. Two new red
regressions pass; green73489:1003 library passes/11ignored and56 IPC/native
integration passes. Spill99630 still6pass/7fail. Debugger86984 attributes the
120512-byte refusal to whole-page Parquet decompression, not queue copies.
The consumed header window is now released before decoding (new independent red
fixture, focused40548:5pass). Broad50491:1004 library passes/11ignored and13
IPC/native passes; spill remains6pass/7fail. GDB82310 locates the next66048-byte
refusal in8192-row Int64 output allocation with222320 bytes already live.
PlainFixedDecoder now retries smaller chunks only on memory refusal; batch
handoff metadata is reserved before column pulls. New dictionary regression red
then green; focused93750:50pass. Broad96184:1005 library passes/11ignored and13
IPC/native passes; spill remains6pass/7fail at changed boundaries. Dictionary IDs now decode in bounded prefixes through the new
admitted_flat_column/dictionary_page.rs, retaining pending prefixes across output
refusal. Independent12000-row V1/V2 fixed/string fixture red then green at16KiB.
Focused57253:53pass; broad70736:1008 library passes/11ignored,43 integration passes;
spill6pass/7fail. Whole page/dictionary retention remains open. Release15551 terminal0 in8m48s, frozen11d16e73; source hashes verified.
Paired82129 terminal1:467 completed outputs correct/460 gated,7 late,16timeouts,
133not_run. No established speed change;Q9/Q12/Q13 incomplete. Archive1133files/
495inputs verified. Q2 upper95% ratio1.124 requires one expanded8block/12pair
confirmation7394 terminal0:208correct/gated,95%0.96282–1.06577; protected bound
cleared. Provider94344 terminal1/audit79244 terminal0:320 completed correct,240/264 valid
pairs;raw60/native60/Iceberg63/Lance57. Six warmup timeouts,two reference failures.
Archive1397files verified. Separate IPC/GPU residency screen now starting. No source/test/harness edits or overlapping heavy jobs.
See [incremental IDs](docs/incremental-dictionary-ids-2026-09-09.md). See
[output quantum evidence](docs/decoded-output-quantum-2026-09-09.md) and
[page lifetime evidence](docs/page-header-lifetime-2026-09-09.md). Mapping ownership and Arrow
validation remain unchanged; no dependency change. Compaction review preserved
existing dirty changes and terminal frozen measurement evidence. See
[Queue metadata and page diagnosis](docs/queue-schema-metadata-contract-2026-09-09.md),
[IPC extent repair](docs/ipc-extent-validation-2026-09-08.md) and
[inline aggregate input](docs/inline-aggregate-input-2026-09-08.md).

Frozen476ec119 repaired join index costing after debugger attribution of the native
256KiB refusal. Shared checked index sizing participates in spill costing; direct
addressing cannot exceed hashed bucket storage. Actual allocation checks remain;
payload/keys/generic-map admission is still incomplete.998 library passes/11
ignored, native10/10 and32 ownership tests pass. Spill6pass/7fail also occurs on
isolated pre-change source; differing denial boundaries remain open. Release95682
terminal0 in8m43s,493 frozen source inputs verified. Paired26121 terminal1:
358 completed correct/353 gated; Q9/Q12/Q13 incomplete. Q19 improves1.19%; other
complete intervals include1, all upper bounds below1.10. Archive931files verified.
Provider53182 terminal1; supplemental23286 terminal0: all322 completed outputs
correct,240/264 valid pairs, six warmup timeouts and two reference failures.
Archive1404files verified. This is not complete residency/resource certification.
See [join repair](docs/join-index-admission-2026-09-08.md) and
[provider screen](docs/join-index-provider-screen-2026-09-08.md).

Preceding frozen8670b94c binds invariant aggregate metadata once per batch.
996 library passes/11ignored;14 selected integration passes; native9pass/1prior
join refusal. Canonical Q5 improves3.29%, custom Q1 improves8.71%. All457 completed
outputs correct,450 gated; Q12/Q13 still fail. Both protected follow-ups terminal0:
416 additional correct/gated outputs, Q2/Q6 upper95% ratios below1.05.
Archive1680files/493source inputs verified. Broader candidate provider/resource
acceptance remains open. See [bound arrays](docs/bound-aggregate-arrays-2026-09-08.md).
The earlier e6a60347 evidence below remains distinct.

Frozen candidate `e6a60347` uses admitted canonical key/hash preparation in up to
four scoped chunks. The exact evaluated batch/layout, full key equality, original
row mapping and spill retry cursor are preserved. A shared child pool caps optional
preparation at one eighth of available query memory; admission refusal cleans up
before ordinary routing, with no source or volatile-expression replay. This is the
only changed source input versus serial-preparation binary `1a0ece71`.

Validation:995 library passes/11 ignored,14 selected integration passes;
native9 passes/1 unchanged383984-byte join refusal at256KiB. Release completed,
493 frozen inputs verified. No dependency change. Compaction review confirms the
chunk ownership contract; detailed evidence lives in linked reports.

Paired canonical comparison: Q10 improves14.29% (95%14.05–14.53%) against serial
preparation. All347 completed outputs validate,339 pass timing; Q12/Q13 still fail.
Custom memory Q1 completes18 candidate samples in3/4 blocks, but neither side has
a complete paired study. This is not canonical SF10 or GPU execution. Archive1048
files verified. Q2's prespecified follow-up adds96 pairs in8 fresh blocks: all208
engine outputs correct/gated, ratio0.98058 (95%0.91485–1.04999). The original wider
interval remains preserved; the follow-up bounds slowdown below10% for this study.
See [parallel preparation](docs/parallel-key-preparation-2026-09-08.md).

Full e6a60347 provider screen48932 terminal1: raw60/native57/Iceberg63/Lance57
valid pairs of66 each. All317 completed engine outputs validate, including80
warmups; six warmup timeouts, one late warmup and two reference refusals remain.
Archive1391 files verified; no scope OOM events. See
[provider screen](docs/parallel-key-provider-screen-2026-09-08.md).

Residency29847 terminal1: canonical decoded IPC and CPU control at32GiB query/
48GiB process each validate60/66 measured pairs; Q1/Q13 time out. This separately
labelled capacity experiment does not clear the16GiB preload refusal. All202
completed measured/warmup outputs correct. Custom CPU control passes40/40; required
GPU startup fails because NVRTC is absent from the loader search path. The dependent
canonical GPU suite rejects incomplete CPU control. Archive1055 files verified.
See [residency screen](docs/parallel-key-residency-2026-09-08.md).

A process-local path to existing repository NVRTC libraries resolves GPU startup.
Fresh matched custom control/required-GPU81085 terminal0:80 measured outputs plus4
warmups correct; all40 measured GPU requests have successful request-scoped device
evidence, no fallback/upload. Resident medians Q1 0.980ms/Q6 1.274ms; separate
preparation43.407ms/27.765ms. This is600000-row custom float smoke, not canonical
SF10 or upload-inclusive timing. Archive415files verified; host scope OOM events0.
See [GPU evidence](docs/parallel-key-gpu-runtime-2026-09-08.md).

The current benchmark harness immediately stops failed reference calibration and
preserves the primary startup failure instead of overwriting it with residency
acknowledgement errors.126 tests run:124 pass,2 optional skips. New regression was
red in6 subcases; archive159files verified. Engine binary/source unchanged by this
harness fix; measurement archives preserve the preceding harness. See
[startup diagnostics](docs/resident-startup-diagnostics-2026-09-08.md).
All preceding measurement jobs are terminal; their formatting/whitespace checks pass.
The new bound-array gate is active as recorded above.

Remaining work: full provider/residency/resource/concurrency acceptance, Q12/Q13
attribution, and query-wide admission gaps. Existing native join refusal and
collected outer joins remain open. Do not infer DuckDB leadership from one shared
operator improvement. Historical checkpoint details are preserved in
[checkpoint history](docs/agent-checkpoint-history-2026-09-08.md) and the existing epic.



## Preserved checkpoint before September 9 consolidation

The following snapshot includes superseded pending states. It is retained as
history; the current guide and linked measurement records are authoritative.

## Current checkpoint — 2026-09-09

Current source removes identity row-index staging from contiguous resident copies.
New `storage/admitted_selection/range.rs` preserves typed values, packed validity,
UTF8 offsets and admitted output ownership; dictionaries retain checked gather.
Red25100 split65,536 rows at17,476; green98115 passes. Broad98963 terminal0:
1,027 library passes/11ignored and27 integration passes; spill42219 remains6/7.
Release24874 terminal0 in8m51s; frozenacdb8c51,503inputs verified. Diagnostics88718/51590
terminal0:24typed-correct outputs; resident Q12 second221/178ms versus312/332ms,
Q13 retains16admitted slots. Archive6implementation/261diagnostic files verified;
no scope OOM events. This is diagnostic evidence, not paired acceptance.
Balanced40018 terminal1:510completed typed-correct,506gated,4late,13timeouts,93not_run.
Q13 candidate24/24measured pass versus4control warmup timeouts. Q9/Q12 incomplete;
Q1/Q2/Q20 small regressions, all complete upper95%ratios below1.10. Archive1179files
verified. Provider10708 now running raw/native/Iceberg/Lance; no source edits.
See [paired gate](docs/contiguous-copy-paired-2026-09-09.md).
Compaction review preserved prior changes; no dependency change. See
[contiguous copies](docs/contiguous-resident-copy-2026-09-09.md).

Current source admits runtime-filter payloads in a bounded optional child domain
and borrows the hash table's evaluated key arrays, removing key staging/replay.
New module `physical/operators/runtime_filter.rs`; no dependency change. The
hashbrown0.17.1 allocation bound must be reviewed on dependency updates.
Ownership red66681 is repaired. Final3586 plus the streaming supplement pass
1,023 library tests/11ignored and74 integrations, including exact join completion
under optional-filter refusal. Spill remains6/7. Archive8files/502inputs verified;
all jobs terminal. No new release/performance certification. See [filter admission](docs/runtime-filter-admission-2026-09-09.md).

The preceding source repair replaces name-based runtime-filter linking with
explicit output ordinals. Red33715 lost a valid computed-projection row;33167
passes1,020 library tests/11ignored and72 integrations. Direct aliases remain
linked, computed/unknown/preserved outputs decline. Archive9files/501inputs
verified. No release after that repair yet. See
[runtime-filter lineage](docs/runtime-filter-lineage-2026-09-09.md).

Frozen dbca414a binds typed selection and byte-admitted resident quantums.
1,020 library passes/11ignored and67 integrations; spill6/7. Release85464 terminal0
in8m50s,500inputs verified. Diagnostics53339/34649 terminal0:24correct outputs;
Q13 retains16slots, resident Q12 returns to916batches and312ms second request
from595ms, still above186ms baseline. Archive7implementation/261diagnostic files
verified. Binary retains the lineage bug; no broad acceptance. See
[typed selection and quantum](docs/bound-selection-and-resident-quantum-2026-09-09.md).

Frozen7b5b7e93 adds bounded outer joins and transitive admitted resident/filter
input. It passes1,018 library tests/11ignored and67 integrations; spill remains
6pass/7same failing names. Release79400 completed8m48s,500inputs verified.
Diagnostics57985/49264:18typed-correct outputs, all Q13 outer frontiers16slots
instead of1. Q13 improves diagnostically, but resident Q12 regresses (595ms versus
186ms second request) with7,323 versus916 filtered build batches. Candidate not
accepted. Archives13implementation/243diagnostic files verified. See
[bounded outer pipeline](docs/bounded-outer-pipeline-2026-09-09.md).
New modules from that phase: `hash_join/outer_probe.rs`, `scan/admitted_memory.rs`,
`filter/admitted_input.rs`, `storage/admitted_selection.rs`; no dependency change.

Frozen candidate `11d16e73` (HEAD `88849c4f` plus the preserved dirty tree) contains
IPC footer/block extent validation, dictionary projection/join metadata accounting,
earlier release of consumed page headers, adaptive fixed-width output with early
batch-handoff admission, and incremental dictionary-ID prefixes. The new
`src/storage/admitted_flat_column/dictionary_page.rs` retains a prepared prefix
across output refusal without rereading pages. Whole decoded pages, dictionary
values and definition bitmaps remain live. No dependency changes or admission opt-out.

Current validation: **1,008 library passes, 11 ignored, 43 integration passes**;
53 admitted-storage tests pass. Seven tiny-budget spill failures remain (6 pass).
Independent red/green fixtures and debugger evidence are linked from
[incremental IDs](docs/incremental-dictionary-ids-2026-09-09.md),
[output quantum](docs/decoded-output-quantum-2026-09-09.md),
[page lifetime](docs/page-header-lifetime-2026-09-09.md),
[metadata accounting](docs/queue-schema-metadata-contract-2026-09-09.md) and
[IPC extents](docs/ipc-extent-validation-2026-09-08.md).

Release15551 completed in 8m48s; all **495 source hashes** verified. Paired82129
against `a023079f`: 467 completed outputs correct, 460 gated; 7 late, 16 timeouts,
133 not-run. No complete interval establishes a speed change; Q9/Q12/Q13 remain
incomplete. Expanded Q2 confirmation7394 passes all 208 outputs and bounds its
95% ratio to 0.96282–1.06577. Archives: 1,133 paired files and 305 follow-up files.
See [paired results](docs/dictionary-chunks-paired-2026-09-09.md).

Provider94344 terminal1; audit79244 terminal0: **320 completed outputs correct**,
240/264 valid pairs (raw60, native60, Iceberg63, Lance57). Six engine warmup
timeouts and two reference failures remain. Archive1,397 files verified. All four
modes are incomplete; see [provider screen](docs/dictionary-chunks-provider-screen-2026-09-09.md).

Residency48225 terminal1; audit79688 terminal0: **244 completed outputs correct**.
Canonical decoded IPC and preloaded CPU control each pass60/66 at32GiB query /
48GiB process, with Q1/Q13 warmup timeouts. Canonical mixed GPU rejects the
incomplete control and does not execute. Custom CPU/GPU each pass40/40; all40
measured required-GPU requests have device evidence, no fallback/upload. Archive
1,100 files verified. This is not lower-budget, canonical-GPU or cold/upload-inclusive
certification. See [residency evidence](docs/dictionary-chunks-residency-2026-09-09.md).

All jobs above are terminal. Formatting/whitespace checks pass. No commit or push
has been requested. Preserve frozen archives and the existing dirty tree.

Current source additionally extends opt-in join phase telemetry only. Diagnostic
release12129 froze `b61ce3d3` with495 verified inputs; only `hash_join.rs` differs
from11d16e73.32 traced integration and32 default-mode join library tests pass.
Diagnostics40828/22219 terminal0:18 completed canonical outputs typed-correct,
raw/native/resident Q13 frontier consistently16 partitions but1 slot, no admitted
buffers or copied-output bound. Preflight245–256ms cumulative does not explain
4.3–4.9s query wall; first aggregate ingestion1258–1371ms is also material.
Archive246files verified; no performance gain claimed. See
[collected join attribution and implementation sequence](docs/collected-join-profile-2026-09-09.md).

Next: implement bounded, admitted outer-probe output so the existing parallel
frontier can safely use multiple slots. Preserve post-filter match semantics,
all-partition unmatched-build completion and legacy candidate guards until the
replacement passes semantic/resource gates. Compare native/resident aggregate
routing with the raw fast path before choosing another shared CPU kernel change.
Dictionary-preserving scan output separately requires physical-schema and recursive ownership contracts;
the current admitted handoff accepts flat arrays only. Full provider, workload,
resource/concurrency acceptance and query-wide admission remain open. Earlier
candidate gains and operational states are historical, not current certification;
see [checkpoint history](docs/agent-checkpoint-history-2026-09-08.md).

