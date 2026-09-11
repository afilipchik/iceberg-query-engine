# Query engine: Codex working guide

Current source, tests and reproducible measurements take precedence over historical
status prose. Update the current checkpoint in place; keep chronological logs in
linked reports and the existing epic rather than prepending another status block.

## Current checkpoint — 2026-09-11

Current candidate `7ddbf9ce` (531 verified source inputs, Lance/GPU) builds on pushed
checkpoint `de7605f`. Admitted readers prepare missing columns' page state before
output trials. Query-pool-admitted decoder checkpoints allow typed memory denials
to discard provisional arrays and retry a smaller common row target. Existing
pending prefixes and final output leases remain owned. Trials never read source
pages; provisional dictionary IDs may be decoded again after a refusal. Non-memory
errors stay terminal. No dependency, default-ownership or budget change.

The fixed-budget reproduction now completes all 4,096 independently expected rows
at 160 KiB for both one-row and larger targets; larger output retains useful
batching. Plain/dictionary SNAPPY fault tests cover cursor rollback, source-read
counts, NULLs, duplicates, unequal prefixes, output lifetime and terminal errors.
See [implementation and evidence](docs/coordinated-reader-output-2026-09-11.md).

Final validation38419 is terminal1: each ownership mode passes 1,131 library tests
(11 ignored), 125 contracts and 28 spill/numeric checks. Native/IPC passes 63 in
disjoint mode and 62 with one failure in partial mode. The same six spill failures
remain in each mode; complete executable/count/exit comparison finds no added or
removed failures. The 25-file archive verifies 531 inputs. The combined build/test
scope reached 48 GiB with 103 max events, zero OOM and swap disabled. This is not
query-only memory telemetry or complete resource acceptance.

Diagnostic18068 is terminal0 with one expected failing test (inferior101). The
remaining COUNT(DISTINCT) refusal requests 23,043 bytes for whole-page decompression
with 248,058 used against 262,144, before output trials. The reader already requests
one row and the join has collected zero batches. The fixture uses SNAPPY and
dictionary encoding. Ten archived files verify 531 inputs and eight fixtures;
probe peak is 1,722,372,096 bytes, zero OOM/max. A complete ownership ledger is still
needed; this is not proof that every viable working-space policy fails.

Release5788 is terminal0 in 8m49s, freezing `7ddbf9ce`. Checkpoint98921 is
terminal1; independent evidence72426 is terminal0. All jobs are terminal. Archives
verify 452 diagnostic, 1,425 provider and 1,097 residency files and all 531 inputs.
The cycle is committed and pushed as `bf88b95`; the remote branch hash is verified.

Latest completed SF10 candidate `7ddbf9ce`:

- Paired diagnostic: 42 typed-correct outputs, 792 join traces, 14 equal plan groups.
  Generic raw Q6 ratio1.030694 versus previous `1bba20b3`, native Q9 1.018964 and
  resident4 Q9 1.020718 regress in both blocks. Native Q6 improves to0.875128 in
  both, but no broad gain or neutrality is certified. Generic Q6 remains1.779016
  versus pre-coercion `13210a20`. Retain provisionally as a memory-progress repair.
- Providers: 337 correct outputs, 252/264 valid pairs (raw66/native60/Iceberg63/
  Lance63). Raw geometric mean2.373308/suite2.528335 versus DuckDB, zero wins.
  Native Q1 warmup and Q6 measured1 timeouts remain. Iceberg Q13/Lance Q9 reference
  calibration0 refuses32MiB/128MiB; dependent requests are not run. The earlier
  engine Lance Q9 join-index failure is not cleared by missing execution.
- Residency: 256 correct outputs, 209/278 planned valid pairs (212 emitted).
  Decoded IPC completes, geometric mean0.839134/suite1.393701, 14 wins. Its engine
  suite time is nearly unchanged across separate screens; the reference time
  changes substantially. CPU-control Q1 timeout blocks all canonical mixed-GPU
  requests. Custom required GPU validates 40 measured device requests. Canonical
  32/48GiB with preload excluded does not clear16GiB preload admission.
- Diagnostic/screen scope peak39,497,113,600 bytes under48GiB, swap0, zero OOM/max.
  This is distinct from the compile/test scope's103max events. All completed
  outputs pass independent typed validation; failed and missing pairs remain open.

See the [completed checkpoint](docs/coordinated-output-checkpoint-2026-09-11.md).
Previous pushed checkpoint `de7605f` and its baseline are retained in the
[bounded-output report](docs/admitted-coalesce-checkpoint-2026-09-11.md).

No DuckDB leadership is certified. Native prepared admission, shared CPU throughput,
whole-page and legacy join/result allocation boundaries, full provider/resource/
concurrency acceptance and short-query precision remain open. Default ownership
stays disjoint; partial-mode gains do not clear its numeric/resource failures or
historical four-thread Q18 regression. Preserve the failed identical-binary custom
Q6 precision control (upper95%1.130588) and its dependent comparison NOTRUN.

Same-cap fresh reference controls pass8typed outputs. Persistent sequence49356
completes Iceberg121/121 but reproduces LanceQ9cal0 128MiB refusal after183
completed requests;431threads and VmData near12GiB cap, zero cgroupOOM/max.
Five boundary outputs are typed-verified; earlier replay outputs only hashed.
Mapping13207 repeats Lance refusal with349Tokio-named threads. I/O quota16
control1762 completes184requests with123Tokio-named threads and five Q9 outputs
across both runs typed-verified; one pair only, no harness change or rebaseline.
Reverse quota39397 completes184; default2125 refuses all three Q9 requests
(181complete). Harness adds explicit reference-only quota with27tests/no skips.
Full matched LanceSF10 screen75607 terminal1: referenceQ9 completes, engineQ9
warmup join-index allocation fails, three measured requests NOTRUN;21other
queries typed-correct.531source/binary/harness hashes verify. Archive then
commit/push this cycle and continue
systemic native/page ownership and shared CPU work. See
[reference diagnostics](docs/reference-worker-initialization-follow-up-2026-09-11.md). Detailed prior experiments and active-state
snapshots are preserved in the [September11 history](docs/agent-checkpoint-history-2026-09-11.md)
and linked reports; historical results certify only their frozen candidates.

## Start here

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
