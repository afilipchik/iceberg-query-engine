# Query engine: Codex working guide

Current source, tests and reproducible measurements take precedence over historical
status prose. Update the current checkpoint in place; keep chronological logs in
linked reports and the existing epic rather than prepending another status block.

## Current checkpoint — 2026-09-15

Frozen scheduler candidate `be164253` on pushed/remote-verified parent `a857215`
repairs the aggregate one-slot fallback. Unknown inputs previously overlapped
pending opens/pulls despite slots=1. One retained opening future/current stream
now drains each partition; cancelling next() cannot replay or lose pending input.
Admitted multi-slot scheduling is unchanged. No dependency, default ownership,
query budget or native-admission capability change. The temporary IPC diagnostic
environment switch is removed; its decode-all comparison remains in unit tests.

Red53440 reproduces3simultaneous operations; focused lifecycle tests pass11.
First broad69274 preserves six startup-expectation failures in an18-file archive.
The eight existing lifecycle tests now cover both serial failure without opening
later partitions and deterministically initialized bounded-parallel siblings;
focused9871 passes8. Final cycle30002 has1142library passes/11ignored and128contracts
per ownership mode. Native/IPC64default and63partial plus its known numeric failure;
spill/numeric28passes plus the same6failures each. Strict executable/count/name
comparison passes, with no retired tests. Validation archive32 verifies802inputs.
Release completes0 in8m51s.

Canonical provider SF10 independently validates336outputs/252of264pairs:
raw66/native57/Iceberg63/Lance66. Raw geomean2.433841/suite2.642917 (0wins), Lance
1.997011/2.949275 (1win). NativeQ1/Q6 and additionalQ18warmups time out; their nine
measured engine requests are NOTRUN. IcebergDuckDBQ9oracle refuses268435456bytes;
its three dependent pairs are NOTRUN. Cumulative validation/build/provider peak
24,877,572,096bytes under48GiB,zeroOOM/max,swap0. Archives1275SF10/173paired verify.
No DuckDB leadership is certified.

Paired attribution validates80outputs with unchanged plans. Candidate/parent
ratios:Q1 1.003247/1.005119;Q6 0.867331/0.916584;Q12 0.665246/1.072963;
Q18 0.987896/0.993545;Q9 0.975818/0.998965. Q18 does not reproduce a candidate
slowdown here, but its canonical timeout remains a failed gate. Q12 varies;
no blanket regression-free claim. Shared Q1 ingestion remains~4.64s and Q18~2.7s,
with~1sfinish whose output time is nested. Postcheck79671 is terminal0:348typed outputs/278pairs across all5residency cases.
Decoded geomean0.798434/suite1.254066;GPUcontrol0.805281/1.289498;
mixed0.798088/1.290919,14wins each. Canonical mixed records0successful device runs;
custom required smoke proves40/40measured requests. Peak21,012,893,696bytes under
64GiB,zeroOOM/max,swap0. These32/48GiB capacity results do not clear16GiB preload.
Source remains frozen while final archives/commit/push are prepared. See the
[current scheduler cycle](docs/serial-frontier-contract-2026-09-15.md).

Previous pushed checkpoint `a857215` freezes IPC candidate `138199d2`: checked
dictionary envelopes, bounded first-definition ID/dependency projection, repeated
column normalization and shared aliases. It validated340provider outputs/255pairs
and348residency outputs/278pairs, but Q12 regressed in both paired blocks; preserve
that failed performance acceptance. The completed same-binary pruning control
validates96outputs with unchanged plans and no consistent Q6/Q12 effect. Its
initial audit keyword error and successful corrected audit are preserved in the
268-file control archive; timing was not rerun. Q12 phase follow-up validates16
outputs,8declared native join-build partitions/1slot,zero major faults. All73native
lineitem/orders schemas were inspected; representative dictionary value buffers
are only10–64bytes. A large synthetic dictionary does not establish SF10 relevance.
See [IPC cycle](docs/ipc-dictionary-projection-2026-09-15.md) and
[control/phase follow-up](docs/ipc-q12-phase-follow-up-2026-09-15.md).

The [native admitted IPC design](docs/native-admitted-ipc-design-2026-09-15.md)
remains proposed. The pinned Arrow decoder audit identifies metadata/schema/map
and delta-copy allocation obligations; alignment control alone is not admission.
Benchmark affinity0–15 covers8physical cores withSMT and a powersave governor;
this is provenance, not a demonstrated cause of timing variance. Earlier decimal
results belong to frozen `6d0b9317`; see the
[decimal checkpoint](docs/decimal-scale-cache-2026-09-11.md).

Parent `8c89899` and residency checkpoint `d37e836` are pushed and remote-verified.
Their UTF-8 binding, prior failures and residency results remain qualified in
[the UTF-8 cycle](docs/bound-utf8-aggregate-keys-2026-09-11.md) and
[residency report](docs/bound-utf8-residency-2026-09-11.md). State-detail42323 on
that binary validates six outputs; its 25-file archive informed the current
candidate. No DuckDB leadership or canonical GPU acceleration is certified.

Pushed parent `bb38784` freezes Lance refinement candidate8c4936d8. The pinned
Lance10 vendor patch runs refinement in owned tasks within its decode window;
UPSTREAM.json preserves265original hashes, and benchmark manifests hash vendor
content. Its SF10 validates343outputs/257pairs, Lancegeomean2.021921. Two reversed
blocks establish Q12/Q19 improvements; default-allocator endurance176/176typed-correct,
48max observed I/Othreads/97total, sequence-end VmData growth11004KiB,zeroOOM/max.
See [Lance refinement evidence](docs/lance-refinement-concurrency-2026-09-11.md).

Frozen parent `835ae7ad` (531 verified source inputs, Lance/GPU features) uses
one ordered Lance scanner across selected fragments. This shares Lance's internal
I/O/decode scheduling instead of spawning a scanner per fragment. Explicit empty
subsets return no rows; provider subsets preserve dataset order. Projection,
filters and AllLate materialization remain. The obsolete outer tasks and their
cancellation guard are removed together. Collected output and internal blocking
work remain resource-accounting boundaries. No allocator, budget, dependency or
default-ownership change. See [scanner evidence](docs/ordered-lance-scanner-2026-09-11.md).

Independent multi-fragment/deletion fixture38877 passes. Production library76256
passes28 and SQLintegration52325 passes31 with fixtures. Broad39700 is terminal1:
each ownership mode1132library passes/11ignored,125contracts; native/IPC63passes
disjoint and62partial plus the known numeric failure; spill/numeric28passes plus
the same six failures each. Complete inventory comparison verifies no new failures
and exactly four retired outer-task tests, replaced by one fixture. Validation
archive25files verifies531source inputs; compile/test peak43.907GB under48GiB,
zeroOOM/max,swap0. Pinned Lance/Lance-IO/Lance-Core sources match364crate files.

Release44289 completed0 in8m48s; provider screen is terminal1. Independent audit1801
validates341outputs/255of264measured pairs: raw66, native60, Iceberg63, Lance66.
Raw and Lance complete all22queries; raw geomean2.435505/suite2.634762 (0wins),
Lance2.219364/3.310772 (1win) versus DuckDB. NativeQ1warmup/Q6measured1 timeout;
DuckDB IcebergQ9calibration1 crashes withSIGSEGV, so engineQ9 is not run. The
1,275-file SF10 archive verifies source/binary/harness; combined scopepeak25.843GB,
zeroOOM/max. The21common Lance queries are14.6%slower in an unpaired historical
comparison, especiallyQ12/Q19. Preserve this tradeoff; no speedup is certified.

Repeated default-allocator Lance37426 completes176requests; independent35908
validates all176outputs. Observed I/O threads peak46; end-of-sequence VmData grows
only64KiB between sequences. Scopepeak7.944GB under16GiB,zeroOOM/max. Archive190files
verifies after correcting a worker/archive manifest filename collision. This
clears the reproduced two-sequence failure, not general query-wide admission or
unbounded-duration acceptance. The cycle is committed and pushed as `580aeca`; the exact remote hash is verified.
Paired attribution96509 is terminal0; all80outputs independently validate. Both
blocks reproduce Q12 at4.76×/4.64× and Q19 at1.93×/1.98×; added time is in provider
scans during planning, with identical optimized/physical plans. Archive174files
preserves this diagnostic and the first concurrency reproduction.
Current candidate is provisional pending
performance work. No new decoded IPC/GPU or concurrency acceptance.

Earlier completed evidence belongs to its frozen binary:

- `9ba109ec`:340typed-correctoutputs/255of264provider pairs. Raw completes at
  geomean2.433312/suite2.639173 versus DuckDB,0wins. NativeQ1timeout, Iceberg
  referenceQ9allocation refusal and LanceengineQ9join-index refusal remain.
- Same binary: seven allocator/sequence controls preserve500typed outputs in a
  559-file archive. FreshQ9passes; persistent default fails after80requests.
  Lazycommit fails after82; no-arena completes88 then aborts on the next sequence.
  Neither allocator control is a production fix. Q19's retained writable mapping
  growth continues even with fixed thread count. See [allocation investigation](docs/join-index-allocation-follow-up-2026-09-11.md).
- `7ddbf9ce`: coordinated admitted reader trials repair fixed-budget progress and
  preserve useful batching. RawQ6 still regresses versus pre-coercion13210a20.
  The decoded IPC screen uses32/48GiB capacity and excludes preload; it does not
  clear16GiB preload refusal. Canonical GPU has no successful device execution;
  custom required-GPU40measured requests have device evidence. See the
  [completed checkpoint](docs/coordinated-output-checkpoint-2026-09-11.md).

No DuckDB leadership is certified. Native prepared admission, shared CPU throughput,
whole-page and legacy join/result allocation boundaries, full provider/resource/
concurrency acceptance and short-query precision remain open. Default ownership
stays disjoint. Preserve the failed identical-binary customQ6 precision control
(upper95%1.130588) and its dependent comparison NOTRUN. Chronological detail is in
[September11 history](docs/agent-checkpoint-history-2026-09-11.md), linked reports
and the existing epic; historical active states are not current certification.

## Start here

- [Current decimal binding candidate](docs/decimal-scale-cache-2026-09-11.md)
- [Latest completed UTF-8/SF10 cycle](docs/bound-utf8-aggregate-keys-2026-09-11.md)
- [Latest completed IPC/GPU residency screen](docs/bound-utf8-residency-2026-09-11.md)

- [Current Lance scanner candidate](docs/ordered-lance-scanner-2026-09-11.md)
- [Current completed checkpoint](docs/coordinated-output-checkpoint-2026-09-11.md)
- [Coordinated reader implementation](docs/coordinated-reader-output-2026-09-11.md)
- [Previous pushed SF10 checkpoint](docs/admitted-coalesce-checkpoint-2026-09-11.md)
- [Current provider screen](docs/coordinated-output-providers-screen-2026-09-11.md)
- [Current IPC/GPU screen](docs/coordinated-output-residency-screen-2026-09-11.md)
- [First-batch memory reproduction and ledger](docs/first-batch-refusal-ledger-2026-09-11.md)
- [Bounded filtered output and local DuckDB comparison](docs/admitted-filter-batching-2026-09-11.md)
- [Native prepared-admission boundary](docs/native-admission-follow-up-2026-09-11.md)
- [Current architecture and testing map](docs/architecture.md)
- [Existing implementation epic](.claude/epics/realistic-benchmarks-duckdb-leadership/epic.md)
- [Local DuckDB/ClickHouse comparison](docs/local-engine-source-comparison-2026-09-07.md)
- [Audit and path forward](docs/project-audit-2026-09-05.md)
- [Original custom SF10 baseline](docs/benchmark-baseline-sf10-2026-09-05.md)
- [Documentation index](docs/README.md), [README](README.md), [Claude history](CLAUDE.md)
- [CCPM skill](.agents/skills/ccpm/SKILL.md) when planning or tracking delivery

Planning artifacts live in `.claude/`. Do not substitute `.Codex/` or create a
second planning tree. Older guide versions are preserved in Git and linked history;
their validation counts and pending states are not current certification.

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

- Before a performance change, inspect the actual workload encoding and sizes and
  measure the targeted cost. Synthetic mechanism tests do not establish benchmark
  relevance; record that distinction before an expensive frozen comparison.

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
