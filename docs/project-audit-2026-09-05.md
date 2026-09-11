# Query engine audit and path forward

**Audit date:** 2026-09-05  
**Source baseline:** commit `88849c4`, clean working tree at audit start.  
**Scope:** documentation, implementation architecture, test and benchmark methodology, modern engine designs, and research priorities.

This is an engineering audit, not a fresh performance certification. Three independent review streams examined source architecture, benchmark/testing evidence, and external literature. Findings were reconciled against the current source. No engine builds, tests, or benchmarks were run for this audit. Numerical summaries below are recomputed from committed historical results; source-level counterexamples are explicitly distinguished from executed reproductions.

Navigation: [assessment](#1-assessment) · [benchmark evidence](#2-what-the-performance-evidence-actually-says) · [architecture findings](#4-current-architecture-strengths-and-structural-problems) · [research](#7-research-directions-for-the-next-generation) · [delivery plan](#9-delivery-plan-and-decision-gates).

## 1. Assessment

The project has made substantial progress. It already implements many techniques a modern analytical engine needs: statistics-based bushy join enumeration, subquery decorrelation, vectorized Arrow kernels, specialized integer joins, runtime filters, morsel aggregation, pruning, mmap IPC caches, native storage, and bounded spill algorithms. Explaining its performance as “no vectorization,” “no parallelism,” or “Rust is slow” would be inaccurate.

The fundamental weakness is that these techniques do not consistently share the same semantic, resource, and execution contracts. Performance paths evolved around a small, custom workload, while documentation and benchmark gates overstate what was established. Important work happens outside the apparent streaming pipeline; fast and spilling implementations have different resource coverage; estimated properties can authorize semantic rewrites; and some tests cannot distinguish success from a wrong or empty answer.

The evidence supports **consolidating and measuring the existing engine before committing to a replacement execution paradigm**. First repair the basis for trustworthy answers and measurements. Then target the amount of work performed, its representation, and the lifetime of intermediate data. Keep a bounded DataFusion reuse experiment available as an ownership decision, not as a presumed performance cure.

The primary recommendations are:

1. Establish one reproducible, correctness-qualified benchmark contract with separate storage and cache tracks.
2. Immediately reproduce and contain the estimated-uniqueness and multipartition subquery/CTE risks identified below.
3. Enforce a shared memory budget across specialized and general operators, with bounded result delivery.
4. Convert the common in-memory join and provider paths into actual incremental execution, preserving useful kernels.
5. Attribute remaining gaps to plan quality, data movement, scheduling, and decoding before choosing fusion, new compression, adaptivity, JIT, GPU, or shuffle work.

“Faster than DuckDB” must become a specific workload and resource contract. General superiority across arbitrary SQL, storage conditions, and hardware is neither supported by current evidence nor a useful engineering acceptance criterion.

## 2. What the performance evidence actually says

**Subsequent measurement:** the [fresh SF10 baseline](benchmark-baseline-sf10-2026-09-05.md) now supersedes the historical numbers below for three local CPU tracks: raw Parquet 1.638×, decoded IPC/Parquet 1.150× and native/native 1.700× by suite sum of medians, with 660/660 measured answer comparisons passing. Conditions differ from the earlier report; this is a new baseline, not a measured source regression. The same dated report now includes Iceberg (1.288×), direct Lance (1.016×) and GPU-assisted IPC/Parquet (1.089×), plus a same-GPU-binary CPU control (1.151×). Across all seven settings, 1,540 measured executions passed validation. Device execution occurred only on Q1/Q6; the measured GPU suite reduction versus its control is 5.63%, with residency preparation excluded.

### 2.1 Latest recorded results

The newest committed measurement is the [September eight-way report](../.claude/plans/research/2026-09-05-sf10-eightway-benchmark.md), rather than the January report in `docs/`. It records a 32-vCPU, 125G machine, DuckDB 1.4.4, sequential legs, desktop load, and best-of-three timings. The report identifies engine binaries with `ec1839f` and follow-on merged work; it does not provide a complete immutable binary/data/environment manifest.

Here, a ratio above 1 means the engine is slower. Totals are sums of per-query best times, not elapsed time for one measured suite invocation.

| Recorded comparison | Engine total | DuckDB total | Ratio of totals | Geometric mean of query ratios | Engine wins |
|---|---:|---:|---:|---:|---:|
| Engine Parquet **with decoded IPC cache** / DuckDB Parquet | 4.828 s | 4.521 s | 1.068 | 1.028 | 9/22 |
| Native storage / native storage | 5.757 s | 3.288 s | 1.751 | 2.205 | 1/22 |
| Iceberg / Iceberg | 8.591 s | 7.020 s | 1.224 | 1.186 | 5/22 |
| Engine cached Parquet / DuckDB native | 4.828 s | 3.288 s | 1.469 | 2.054 | 1/22 |

These are descriptive summaries of the recorded experiment, not statistically controlled current ratios. The native median ratio is 2.171. Q9 accounts for approximately 42% of DuckDB's native total and is the engine's only native win; consequently, the total ratio understates how widespread the per-query gap is. Both suite sum and geometric mean are useful, but answer different questions.

Native examples:

| Query | Engine | DuckDB | Ratio | Why investigate |
|---|---:|---:|---:|---|
| Q1 | 494.1 ms | 110.0 ms | 4.49 | Scan/aggregation and provider-path comparison |
| Q6 | 117.8 ms | 25.9 ms | 4.55 | Simple filtered reduction: strong diagnostic workload |
| Q13 | 695.3 ms | 129.6 ms | 5.36 | Largest recorded native excess; 247.3 ms on engine cached Parquet |
| Q9 | 1,226.3 ms | 1,391.9 ms | 0.88 | Preserve this strength; investigate representation and plan differences |

Q13 and Q1 account for about 38.5% of the net native excess time. That makes them sensible first profiling candidates; it does not establish their bottlenecks without profiles.

The [spill-boundaries closeout](../.claude/epics/archived/spill-boundaries/004.md), around line 68, separately records cache-off Parquet totals of 7.52 s and 7.24 s. Against the newer 4.52 s DuckDB Parquet total this suggests approximately 1.60–1.66×, but **the measurements are not a controlled pair**. Do not publish that range as a fresh benchmark result.

The Lance comparison is also narrow: DuckDB queries materialized Arrow registrations through views, with loading excluded. It supports an integration-path comparison, not a general conclusion about native engine execution.

### 2.2 The benchmark's storage premise changed

The September “same files” comparison sets `QE_IPC_CACHE` to Auto with fresh `.qeipc` sidecars. The engine therefore sometimes reads predecoded Arrow IPC; DuckDB reads Parquet. This can be a useful product configuration, but it must be named correctly.

[ipc_cache.rs](../src/storage/ipc_cache.rs), lines 5–22, describes the cache and its additional storage footprint. The earlier [parity plan](../.claude/plans/PARITY-PLAN.md) explicitly recognized that a decoded cache changes the premise. The later headline lost that qualification.

Maintain separate tracks:

- Raw Parquet with decoded cache disabled on both sides; warm OS page cache is a separately declared condition.
- Engine decoded IPC cache versus DuckDB owned/native storage, with build/load costs and disk amplification reported.
- Each engine's native storage, with layout and loading explicitly specified.
- Iceberg on the same snapshot and files, including metadata and delete semantics.
- Optional Lance/GPU/integration configurations, clearly identifying materialization and residency.

A warm decoded-cache win can be valuable. It is not evidence of a faster Parquet decoder.

### 2.3 This is a custom TPC-H-derived workload

The current generator and queries should not be described as standard TPC-H:

- [generator.rs](../src/tpch/generator.rs), line 270, emits “Part N” names; [queries.rs](../src/tpch/queries.rs), lines 312 and 627, substitutes `LIKE 'Part 1%'` in Q9/Q20.
- Order comments are constant at generator line 452. Query comments at queries lines 418–423 acknowledge that Q13's filter rejects no generated rows.
- Generator lines 439–445 draw order customer keys up to 1.5× the existing customer count, creating missing foreign-key references. The comment calls this the specification, but the specification assigns orders to a subset of existing customers.
- [schema.rs](../src/tpch/schema.rs), line 158, fixes lineitem count to 6,000,000 × SF; monetary fields use Float64 rather than a canonical decimal workload.

The TPC-H specification describes orders referencing customer keys and customers without orders; these are different from orders referencing nonexistent customers. Use pinned dbgen/qgen tooling and validate referential integrity, types, row counts, and parameter sets. See the [TPC-H 3.0.1 specification](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-h_v3.0.1.pdf).

The existing workload remains valuable as a regression corpus. Preserve it under an honest name instead of silently replacing its generator and invalidating its oracle files. Add canonical TPC-H as an independent corpus, and do not call internal timing runs audited TPC results.

### 2.4 “Cell-exact” is stronger than the comparator

[scripts/native_bench_compare.py](../scripts/native_bench_compare.py), lines 268–315, rounds numeric values to two decimals and accepts a tolerance of `max(0.02, abs(reference) * 1e-9)`. It collapses NULL and empty string and can treat numeric-looking strings as numbers. Lance and scratch Parquet validators use similar normalization. Iceberg's benchmark checks row counts only.

Thus “22/22 cell-exact” in those reports means agreement under that comparator, not exact typed SQL equivalence. This does not establish that the results were wrong; it limits what the test could detect.

A replacement oracle must compare schemas and typed values: exact integer, decimal, string, date/time and NULL semantics; an explicit absolute/relative or ULP policy only for floating results; and row multisets or ordered rows according to the SQL contract. Resolve LIMIT ties deliberately rather than making ordering tests nondeterministic.

## 3. Documentation audit and Codex integration

The original root `AGENTS.md` was 326,708 bytes and 5,464 lines; `CLAUDE.md` was 332,620 bytes and 5,543 lines. They mixed instructions, architecture, experiments, incident reports, old performance tables, and later corrections.

Specific drift included Arrow 53 versus current 58, sqlparser 0.52 versus 0.62, a three-rule optimizer versus the current rule pipeline, and “distributed M1 only” versus current partial/final and gather execution. The root README still advertised Rust 1.70 and Lance 0.23, while `Cargo.toml` declares Rust 1.93.0 and Lance 10.

The Codex conversion also replaced real `.claude/` paths with nonexistent `.Codex/` paths and referred to nonexistent `scripts/Codex-safe-build.sh`. The executable actually present is [scripts/claude-safe-build.sh](../scripts/claude-safe-build.sh). Renaming prose without mapping real resources made the guide less executable.

Official Codex documentation specifies a default 32 KiB combined project-instruction limit. A 326 KB guide is unsuitable as a default instruction entry point, even where a particular session supplies more text. Use a small `AGENTS.md` that links task-specific documents. See [official AGENTS.md guidance](https://learn.chatgpt.com/docs/agent-configuration/agents-md).

The existing `.codex/hooks.json` format should **not** be dismissed solely because it resembles Claude configuration: current Codex supports repository hooks, canonical Bash matching for unified exec, and the relevant denial response. Its absolute checkout path and actual activation still need verification. The script's substring-based “already wrapped” check and acceptance of `MemoryHigh` as an alternative to `MemoryMax` are insufficient proof of a hard containment boundary. Hooks are a guardrail; the real cgroup wrapper must remain the operational rule. See [official hook documentation](https://learn.chatgpt.com/docs/hooks) and [the local hook](../scripts/claude_hooks/enforce_safe_build.sh).

This audit establishes:

| Document | Role |
|---|---|
| [AGENTS.md](../AGENTS.md) | Compact Codex entry point, mandatory rules, commands and evidence conventions |
| [architecture.md](architecture.md) | Current source map, execution behavior and testing architecture |
| This document | Dated findings, research comparison, experiments and decision gates |
| [README.md](../README.md) | Product entry point with corrected prerequisites and pointers |
| [CLAUDE.md](../CLAUDE.md), `.claude/plans` and `.claude/epics` | Preserved historical investigations and delivery evidence |

The former AGENTS content remains recoverable at `git show 88849c4:AGENTS.md`; Claude's underlying record remains available. Existing CCPM artifacts stay in `.claude/`, because the Codex skill already understands those paths. A parallel renamed planning tree would introduce a second source of truth.

Future documentation should separate enduring contracts from measured claims. A claim needs its revision, conditions, command/driver, and artifact; a historical correction belongs beside the original claim, with a current index pointing to the replacement. Do not append another 100-line retrospective to the operational guide after every task.

## 4. Current architecture: strengths and structural problems

The detailed module map is in [architecture.md](architecture.md). Source inventory at the baseline: 102 Rust source files and 102,439 lines under `src/`, including comments and inline tests; 29 Rust integration-test files. Large modules include spillable execution (9,644 lines), expression evaluation (6,396), binder (6,112), and hash join (5,683). Size is a maintenance signal, not evidence that code is slow.

### 4.1 Estimates have crossed into SQL correctness

**Priority P0; source-established unsound premise; runtime reproducer not executed in this audit.**

Parquet's integer `ndv_est` is `min(non_null_rows, max - min + 1)`, an upper bound, not an exact distinct count ([storage/parquet.rs](../src/storage/parquet.rs):257; [scan.rs](../src/physical/operators/scan.rs):26).

[GroupKeyReduction::is_unique_key](../src/optimizer/rules/group_key_reduction.rs), around line 100, interprets `ndv_est >= row_count` with no NULLs as uniqueness. The rewrite can drop grouping columns and carry them as ANY_VALUE. [EagerAggregation](../src/optimizer/rules/eager_aggregation.rs):159 uses the same premise in a LEFT-join count rewrite.

A minimal counterexample is a Parquet table with Int64 k and Utf8 d:

| k | d |
|---:|---|
| 1 | a |
| 1 | b |
| 3 | c |

Its row count and range-based NDV bound are both 3, but key 1 is duplicated. `SELECT k, d, COUNT(*) FROM t GROUP BY k, d` requires three groups. The rule's premise permits grouping only by k, which merges two distinct groups.

The fix is not “better NDV estimation.” **Costing estimates and correctness properties must be different types/contracts.** Approximate NDV, including HLL, cannot prove uniqueness or referential integrity. Require validated constraints or exact snapshot-scoped proofs for semantic rewrites. Retain estimates for choosing among equivalent plans.

Next test: construct the actual Parquet fixture with typed columns, inspect the optimized plan, compare typed rows with the rule on/off and DuckDB, and cover the analogous LEFT-count and decoration-join-removal rewrites.

### 4.2 Partition completeness is still a caller convention

**Priority P0; direct source hazard; end-to-end reproducer pending.**

[run_subquery_blocking](../src/physical/operators/subquery.rs):35 executes only `physical.execute(0)`. `run_subquery_plan` delegates to it, and shared CTE materialization invokes that helper at [planner.rs](../src/physical/planner.rs):723.

A multipartition scan/project/filter CTE may therefore materialize only one valid partition. The new [check_partition](../src/physical/plan.rs):59 rejects invalid indices, but cannot detect a caller that never requests the remaining valid indices.

Next test: a twice-referenced, non-aggregate CTE over at least four Parquet row groups, with all partitions containing distinct marker rows; control Rayon's worker count, assert the physical root reports output_partitions() > 1, and compare full multisets at 1/2/4 workers. Aggregate-rooted CTE tests alone cannot expose this.

Introduce one shared driver for all-partition execution and explicit partition/distribution properties. Keep partition guards, but do not treat them as a proof of partition completeness.

### 4.3 Memory safety is local rather than compositional

**Priority P0/P1; contract gaps visible in source; no new OOM reproduction attempted.**

Recent spill work is substantial and worth preserving. Streaming spill input, chunked partition readback, join-family coverage, cleanup, and chaos testing closed real failures. However, passing selected workloads under process caps does not prove that every operator obeys one query-wide budget.

Four current gaps matter:

1. [MemoryPool::observe](../src/execution/memory.rs):272 records the maximum of submitted values without reserving them. Spillable operators independently compare against `memory_limit * spill_threshold`. Multiple resident operators can each approach the same budget.
2. [MorselAggregateExec](../src/physical/operators/morsel_agg.rs):37 has no pool/config field and is selected before the spillable aggregate at [planner.rs](../src/physical/planner.rs):1275. Generic worker hash states grow until exhaustion. Dense execution caps key-domain width at 64 million rather than accounting for bytes across accumulators; one AVG uses two eight-byte arrays per key.
3. [SpillableHashJoinExec](../src/physical/operators/spillable.rs):809–934 admits the in-memory branch based on Arrow build-batch bytes, then constructs HashJoinExec without a memory context. Hash metadata/row-store amplification is outside that fit decision.
4. [ExecutionContext::sql](../src/execution/context.rs):1420–1453 collects the entire result. A streaming operator alone cannot make an arbitrarily large final result bounded.

The repository itself records Q9 SF100 at `--memory-limit 1G` using approximately 10.7 GB peak RSS under a 16G process cap ([join-spill-streaming task 003](../.claude/epics/archived/join-spill-streaming/003.md):114). That is evidence of completion with a configured operator budget, not completion inside a 1 GB process limit.

The process rlimit protects the host/session by causing allocations to fail; an allocator abort is not graceful query spilling. Rust language memory safety, engine resource boundedness, and host OOM containment are distinct properties.

Required architecture: process/query/operator reservations, bounded channels, memory-aware task concurrency, and shared ownership of spillable state. Reserve before growth; use coarse reservations to avoid an atomic operation per value. Account for build metadata, queued batches, filter state, merge scratch, result buffers, and allocator overhead. Track estimates separately from allocator and cgroup/RSS measurements.

### 4.4 A stream return type is not an incremental pipeline

**Priority P1; observed implementation behavior; performance impact requires profiling.**

`PhysicalOperator` returns an async batch stream, but the common paths contain eager boundaries:

| Boundary | Source evidence | Consequence |
|---|---|---|
| Provider scan returns `Vec<RecordBatch>` | [scan.rs](../src/physical/operators/scan.rs):74 | Storage interface does not require lazy production |
| Planning prescans/materializes CTEs | [planner.rs](../src/physical/planner.rs):1135–1166 | Data execution appears in planning time and prolongs residency |
| Generic scan fallback eagerly invokes provider | planner.rs:1571 | Different providers can get fundamentally different pipelines |
| In-memory join collects probe input and builds result vectors | [hash_join.rs](../src/physical/operators/hash_join.rs):1416, 1479, 1559 | Delays downstream work and retains intermediates |
| Root API collects every partition result | [context.rs](../src/execution/context.rs):1420–1453 | No bounded final sink |

Do not remove legitimate pipeline breakers: hash-build state, global ordering, and aggregation need state. The question is whether probe/consume/output must also be fully materialized and whether that state has bounded, spillable ownership.

The first vertical slice should preserve the stream ABI: a provider-neutral lazy scan feeding a hash build plus incrementally consumed probe, then a bounded aggregation/result sink. Move shared CTE execution into an explicit dependency stage. Add cancellation and output backpressure. Only change the global scheduler or ABI after measuring this slice.

DataFusion is a counterexample to the claim that Rust/Arrow streams inherently prevent a competitive engine. Push execution can improve lifecycle control, but the literature does not quantify an isolated push-versus-pull gain applicable to this project.

### 4.5 Fast paths are not uniformly available across providers

Parquet file exposure routes eligible queries directly to morsel operators; native providers receive narrower dense specializations; other providers often use general eager scans. This is visible in [planner.rs](../src/physical/planner.rs):169, 1275 onward and the provider trait.

The recorded native-versus-cached-Parquet Q1/Q13 differences make this a strong investigation target. They do not prove native storage itself is slow: planning, schema/encoding, HTTP overhead, and operator selection also differ.

Replace provider identity checks with carefully scoped capabilities where semantics permit: scan tasks, exact statistics, row-group pruning, dictionary/encoded access, snapshot identity, and projected batch production. Preserve restrictions that protect correctness. Do not expose a capability until both fallback behavior and provenance are explicit.

### 4.6 Scheduling is fragmented

Rayon workers drive morsel kernels, Tokio tasks drive some partitions, scans synchronously decode inside async stream bodies, and subqueries maintain another full-width Tokio runtime plus an OS-thread spawn/join. Top-level `join_all` enables concurrent polling; it does not by itself parallelize CPU-bound work.

Oversubscription, skew and runtime blocking are plausible costs, not established measurements. Profile at fixed total core counts of 1/2/4/8/16. Track CPU/wall ratio, context switches, run queues, per-phase occupancy and NUMA locality. Share a bounded CPU task budget while retaining async I/O. Prefer local state and batch-sized tasks; do not serialize all operators behind a global lock.

### 4.7 The optimizer is real, but its evidence and objective are narrow

The live join enumerator is DPsize in [join_reorder.rs](../src/optimizer/rules/join_reorder.rs):711–840. It minimizes estimated intermediate row counts. The separate public `CostEstimator` in [cost.rs](../src/optimizer/cost.rs) is not the active planner.

Missing NDV warnings improve visibility but retain the risky fallback cost. Native staleness handling can invalidate an estimate without supplying a safer strategy. The optimizer does not generally price row width, representation, decode work, memory amplification, spill risk or network transfer. Debug-tree formatting implements fixpoint comparison in [optimizer/mod.rs](../src/optimizer/mod.rs):181; rule sequencing also needs special handling after packed-key rewrites.

First log estimated and actual cardinalities and bytes at each boundary. For bad cases, inject exact cardinalities while holding kernels constant. If plans improve dramatically, prioritize estimates and robust physical choices. If good plans remain slow, focus on execution. Do not start by building a sophisticated learned cost model.

### 4.8 Data movement is a more useful hypothesis than “needs JIT”

General expressions allocate array temporaries and full-length literals; filters compact columns; joins gather payloads; generic keys own vectors and strings. Specialized alternatives and a fused numeric predicate evaluator already exist. These observations point to memory traffic and representation as candidates.

Measure bytes decoded, copied, compacted and gathered, allocations, cycles per input row and output fanout. Then compare reusable expression buffers, selection vectors, encoded strings, narrower join payloads and selective probe-to-aggregate fusion.

Late materialization also has costs: scattered gathers, additional indirection, random I/O, and lifetime management. Dense/selectivity-insensitive workloads can prefer eager processing. One expression microbenchmark cannot establish that JIT “buys nothing,” just as a compiled-engine paper cannot justify rewriting every operator.

## 5. Testing and delivery approach

There is real engineering discipline in the newer adversarial work: forced spill with activity assertions, real cgroup and rlimit harnesses, checksum observations, mutation crash tests, partition regressions, and real-process distributed checks. The flaw is overgeneralizing those results into universal certification.

| Existing layer | Value | Current limitation |
|---|---|---|
| Inline unit and SQL integration tests | Broad operator and language coverage | Small fixtures do not necessarily activate thresholds, encodings, concurrency or memory transitions |
| 177-case DuckDB fixture manifest | Independent reference outputs | SF .001; Q5/Q20/Q21/Q22 have empty expected outputs; comparator parses numerics as f64 and ignores headers |
| TPC-H smoke tests | All 22 SQL shapes execute | Successful execution alone passes |
| Spill differential tests | Compare physical modes and assert real spilling | Shared engine bugs can pass both sides; some normalization loses precision |
| Cap harness | Real OS containment and kill/refusal classification | Default scenarios omit join variants; safe refusal is not successful completion |
| Chaos/certification runs | Valuable targeted stress evidence | Key drivers/logs still live under ignored scratch directories |
| Distributed and Flight tests | Real processes, membership, transport and local/global agreement | Same-engine local oracle can share semantic defects; multi-host performance is a separate gate |
| Optional features | Useful dedicated suites | Missing fixtures/devices can early-return as green tests |
| CI | Tests, format, clippy, release build | No committed performance/cap job; tiny data only; floating toolchain and incomplete feature-environment provisioning |

Sources: [duckdb_validated.rs](../tests/duckdb_validated.rs):18, 273, 332; [manifest.json](../tests/expected_results/manifest.json); [spill_tests.rs](../tests/spill_tests.rs):122–155; [oom_cap_harness.sh](../scripts/oom_cap_harness.sh):51, 115; [CI](../.github/workflows/ci.yml):33–43.

### 5.1 The performance gate does not enforce its stated rule

Three concrete harness problems need repair before performance acceptance:

- At audited source `88849c4`, `safe_benchmark.sh`:16, 129–137 imposed a five-second timeout floor. At the script's 12 ms Q11 baseline that permitted roughly 417× wall time. Lines 297–306 labeled any parsed completion PASS without comparing its ratio or answer; the script lacked a failure-count-derived nonzero exit. **Subsequent implementation:** the entry point now delegates to the [versioned harness](../scripts/benchmark/README.md); legacy arguments fail explicitly. Canonical SF1 exposed [16 failing queries](canonical-sf1-findings-2026-09-05.md). Full mode/profile implementation remains open.
- [benches/tpch.rs](../benches/tpch.rs):46–50 silently ignores failed queries in the full-suite benchmark. A broken query can make the benchmark look faster.
- [main.rs](../src/main.rs):817–840 records errors with zero time and uses nonzero row count as success, although empty results can be correct.

Separate process watchdog, measured query-time gate, correctness, and completion. Record errors/timeouts/refusals as explicit statuses, never zero-latency successes. A watchdog can include startup allowance, but the measured query threshold remains 10× its matched DuckDB reference. Every gate must fail closed with a nonzero status.

### 5.2 Required verification matrix

Use a shared harness across providers and algorithms:

| Dimension | Required cases |
|---|---|
| SQL semantics | NULL versus empty; duplicate and missing keys; empty/nonempty outputs; decimal/overflow; timestamps; outer/semi/anti joins; shared CTEs; windows; DISTINCT |
| Physical structure | 1 and many batches/partitions; sliced arrays; independent dictionaries; Int32/Int64; strings/composite keys; all join orientations |
| Algorithm transition | Generic/specialized; in-memory/spill; just below/above admission threshold; re-execution; cancellation and fault injection |
| Statistics | Exact, missing, sampled, stale; sparse/duplicate keys; misleading ranges; correlated columns |
| Resource shape | 1/2/4 concurrent queries; overlapping joins; high group cardinality; wide results; bounded output; disk-full and cleanup |
| Storage/distribution | Raw Parquet/cache/native/Iceberg; snapshot consistency; metadata/deletes; slow consumer; worker failure and skew |

Every optimized path needs both a selection assertion and an independent result oracle. Existing chaos infrastructure is a starting point for deterministic seeded SQL/plan generation and minimized regression fixtures. Promote reusable scratch drivers into version control with compact result manifests. Report skipped capabilities explicitly.

Tests also need to check the claimed property: a memory test must verify the real physical path and measured RSS/cgroup cap; a pruning test must prove skipped work; a parallel test must cross its threshold; a partition test must put unique output in every partition.

## 6. What modern engines teach

The relevant lesson is a coordinated execution substrate, not a checklist of fashionable features. Source-code links below are study entry points; mutable branches must be pinned to SHAs before experiments.

| Reference engine | What to study | What it does not prove |
|---|---|---|
| [DuckDB execution](https://github.com/duckdb/duckdb/tree/main/src/execution), [pipeline executor](https://github.com/duckdb/duckdb/blob/main/src/parallel/pipeline_executor.cpp) | Join/aggregate tables, tuple collections, pipeline dependencies, buffer management, runtime compression | That copying a kernel will transfer its system-level performance |
| [DataFusion 50.0.0 source](https://github.com/apache/datafusion/tree/50.0.0/datafusion), [architecture](https://datafusion.apache.org/contributor-guide/architecture.html) | Rust/Arrow execution properties, physical optimization, memory pools, datasource boundary, SQLLogicTest; modular reuse | That it automatically beats this engine or current DuckDB; the tag is a study snapshot |
| [Velox](https://github.com/facebookincubator/velox/tree/main/velox), [2022 paper](https://www.vldb.org/pvldb/vol15/p3372-pedreira.pdf) | Lazy/encoded vectors, expression reuse, execution/resource contracts and reusable execution beneath frontends | That introducing a C++ runtime is a free integration |
| [ClickHouse source](https://github.com/ClickHouse/ClickHouse/tree/master/src), [2024 paper](https://www.vldb.org/pvldb/vol17/p3731-schulze.pdf) | Processors, columns, aggregation, storage pruning, specialized representations | Universal join-workload superiority: the paper's v24.6 experiment excluded 11 TPC-H queries |
| [StarRocks pipelines](https://github.com/StarRocks/starrocks/tree/main/be/src/exec/pipeline), [execution guide](https://docs.starrocks.io/docs/best_practices/query_tuning/query_planning/) | Drivers, dependencies, scan/hash/aggregate/exchange tasks, local versus distributed scheduling | That more nodes reduce warm single-node latency |
| [Trino 477 source](https://github.com/trinodb/trino/tree/477/core/trino-main/src/main/java/io/trino), [dynamic filtering](https://trino.io/docs/current/admin/dynamic-filtering.html) | Planner/connector/filter coordination, memory, split enumeration and exchange | That Java distributed operator implementations should be transplanted as Rust CPU kernels |

DuckDB's [Saving Private Hash Join, PVLDB 2025](https://www.vldb.org/pvldb/vol18/p2748-kuiper.pdf) is especially relevant: adaptive external join, unified temporary/persistent buffering, runtime compression, and memory assignment among concurrent operators work together. The paper states the principal techniques were available in v1.2.0. This argues for common resource and representation infrastructure across fast/spill modes, rather than adding another independent spill threshold.

[Morsel-Driven Parallelism, SIGMOD 2014](https://db.in.tum.de/~leis/papers/morsels.pdf) separates small runtime work units from query-plan structure and emphasizes locality. The project already has morsel machinery; apply the lesson to uncovered build/probe/merge phases before replacing it.

The [2018 vectorization-versus-compilation study](https://www.vldb.org/pvldb/vol11/p2209-kersten.pdf) finds advantages depending on computation and memory access. It supports measuring intermediates and stalls. It does not establish an unconditional winner, and compilation latency is outside its core comparisons.

The [2025 JOB retrospective](https://www.vldb.org/pvldb/vol18/p5531-viktor.pdf) stresses cardinality errors, realistic correlations, and runtime robustness. Add JOB-style correlated joins and skew; even canonical TPC-H alone is an incomplete optimizer evaluation.

### 6.1 Distributed architecture is a different objective

The current engine implements partial/final aggregation, TopN, supported joins with replicated tables, and a general gather fallback. In gather mode it transfers selected base columns and executes the original query locally at the initiator. There is no general repartitioned shuffle join.

[coordinator.rs](../src/distributed/coordinator.rs):500–507 admits gather using compressed file bytes against half the memory limit. Compressed size is not a bound on decoded residency. General distributed execution must budget decoded data, in-flight network buffers and coordinator output, not only source bytes.

The [Snowflake NSDI 2020 paper](https://www.usenix.org/conference/nsdi20/presentation/vuppalapati) studies elasticity and disaggregated storage in production. Such systems optimize service objectives, concurrency and cost, with cache and placement management. Their architecture is not a shortcut to winning a local query benchmark.

For a distributed product, define a separate target: multi-host throughput/cost, large-data completion, failure recovery and scalability at fixed aggregate resources. Build shuffle only after local scheduling, resource ownership, cancellation and semantic contracts are dependable. A same-host cluster benchmark alone does not establish scale-out efficiency.

## 7. Research directions for the next generation

These are gated research candidates, not predicted speedups for this project.

| Direction and primary source | Evidence maturity | Plausible project experiment | Stop condition |
|---|---|---|---|
| [RPT+: Robust Predicate Transfer with Dynamic Execution, PVLDB 2026](https://yimingqiao.github.io/files/rpt_plus.pdf), [artifact](https://github.com/embryo-labs/dynamic-predicate-transfer) | Published DuckDB-based research; not assumed stock DuckDB behavior | Extend runtime filters with selectivity/size feedback, abandon unhelpful builds, combine coarse pruning with tuple filters | Filter cost or synchronization exceeds avoided work; regressions on nonselective joins |
| [Adaptive Factorization Using Linear-Chained Hash Tables, CIDR 2025](https://vldb.org/cidrdb/papers/2025/p21-gro.pdf) | Hash-table component adopted in DuckDB 1.1.0; broader factorization experimental | Delay duplicate expansion where a many-to-many join feeds aggregation; compare with normal joins | Few duplicates, unsupported semantics, or setup/representation overhead dominates |
| [The FastLanes File Format, PVLDB 2025](https://www.vldb.org/pvldb/vol18/p4629-afroozeh.pdf), [artifact](https://github.com/cwida/fastlanes-vldb2025) | Published v0.1 format research | Reader/kernel prototype using lightweight encoded vectors and selective decoding | No end-to-end gain after decode, conversion and fallback costs |
| Encoded/lazy execution ([Velox paper](https://www.vldb.org/pvldb/vol15/p3372-pedreira.pdf)) | Production architecture | Preserve dictionaries/selection state through filter/project/join; avoid repeated flattening | Sparse gathers or ownership complexity outweigh saved traffic |
| Narrow adaptive plan choices ([JOB retrospective](https://www.vldb.org/pvldb/vol18/p5531-viktor.pdf)) | Broad research/production motivation; local gain unknown | Correct build orientation or aggregation placement at an existing materialization boundary | Replanning overhead, unstable decisions or no material plan mistakes |
| Shared temporary memory ([Saving Private Hash Join](https://www.vldb.org/pvldb/vol18/p2748-kuiper.pdf)) | Production-integrated research | Reuse compact pageable state and redistribute budgets across active joins/aggregates | Contention/fairness regressions; tune reservation granularity before expansion |

RPT+ is unusually pertinent because it explicitly controls the overhead of predicate transfer. Its reported geometric-mean gains on DuckDB v1.3.0 are 1.17× TPC-H, 1.47× JOB, 1.28× SQLStorm and 1.01× Appian. The variation matters: avoid promising a universal gain. The project's existing runtime-filter chaining should first obtain clean measured benefit and cost telemetry.

A reasoned forecast is that robust execution, compact/encoded representations, and work avoidance will remain useful across future hardware. It is a forecast based on these sources, not a claim that one future architecture has been settled.

Defer full learned optimization, full-query JIT, GPU joins, new native formats and global adaptive replanning until experiments identify a workload advantage. GPU work should price residency, upload, eviction, transfers and repeated-query count. The existing numeric/GPU fast paths are assets to evaluate, not proof of whole-suite leadership.

## 8. Proposed benchmark contract

Use immutable manifests containing:

- Engine commit, binary hash, features, compiler/flags and dependency lock hash; comparator version and settings.
- Dataset generator/version/seed, table/schema/file hashes, SQL and parameters, Iceberg snapshot, statistics and physical layout.
- CPU model/topology, affinity, actual worker counts for every pool, memory/process cap, cache state, storage/network and background load.
- Timed boundary, loading/cache-building costs, complete samples, output-validation status, physical plans and selected fast paths.
- Wall and CPU time, RSS/cgroup peak, spill bytes/read/write time, materialized bytes, files/row groups skipped, estimated/actual cardinality.

Report both query execution and end-to-end service latency using matched boundaries. Today, CLI `ctx.sql` timing, HTTP+CSV timing, and Python `fetchall` timing differ. Also, `context.rs` records total/execute time before final dictionary decoding; planning time includes eager data work. Repair phase definitions before comparing them.

Run randomized alternating A/B blocks on reserved hardware, with enough repetitions to estimate uncertainty; begin with at least ten measured samples per query after declared warm-up, then increase if intervals remain wide. Store every sample rather than only minima. Keep cold-cache, warm-file, decoded-cache and resident-GPU experiments separate. Never globally drop host caches on a shared machine as part of an ordinary benchmark.

Proposed acceptance policy, to ratify as the product goal:

- **Correctness:** every required query matches its typed oracle; zero missing cases disguised as skips.
- **Resource safety:** zero allocator aborts/kernel kills; required workloads complete at declared caps. Clean refusal is reported separately.
- **Regression control:** no unexplained >10% regression on a protected workload, assessed against uncertainty.
- **Parity milestone:** geometric mean ≤1.00 and suite sum ≤1.00 on the primary track, with per-query tails explicitly bounded and no failures.
- **Leadership milestone:** all parity gates still hold (including suite sum ≤1.00), plus geometric mean ≤0.90, a majority of per-query wins, and no query >2× without a reviewed explanation; sustained across a holdout corpus and repeated runs.

These are proposed project gates, not claims already achieved. Keep the existing 10× rule as a failure ceiling; passing it is not parity. Use current matched DuckDB baselines, not August constants carried forward indefinitely.

For the named product, primary track should initially be CPU-only, single-node analytical SQL over identical raw Parquet/Iceberg data. If the intended product is actually an owned native database, promote native/native to primary explicitly. Until that decision is made, report both without collapsing them into one number.

## 9. Delivery plan and decision gates

Effort below is approximate engineering effort, not a schedule commitment. Sequence is dependency-driven. Benchmark/oracle work can run in parallel with minimal correctness reproductions; kernel optimization waits for both.

| Stage | Work | Deliverable and exit gate | Approximate effort |
|---|---|---|---|
| A — Restore trustworthy evidence | Typed oracle, canonical dataset lane, fixed fail-closed harness, immutable manifests; reproduce NDV and partition hazards | Minimal regressions committed; affected unsafe rewrites contained; benchmark cannot silently pass errors/omissions | 1–2 engineer-weeks |
| B — Attribute the gap | Q1/Q6/Q13 profiles, raw/cache/native matched runs, per-operator rows/bytes, plan experiments, 1–16 core scaling | Ranked cost breakdown; reproduce the leading hypothesis by an isolated A/B change | 1–2 engineer-weeks |
| C — Make resource contracts common | Reservations across fast/spill state; morsel aggregate admission/spill; account join metadata; bounded results; concurrency caps | Required workloads complete under real caps at 1/2/4 concurrency; measured peaks reconcile with reservations/headroom | 2–4 engineer-weeks for one vertical slice; wider rollout depends on findings |
| D — Remove demonstrated materialization | Stream in-memory probe/output; lazy provider tasks; explicit CTE stage; selective payload/fusion work | Measured reduction in intermediate bytes and latency with typed equivalence, cancellation and re-execution tests | 2–4 engineer-weeks, bounded slice first |
| E — Robust plans and representation | Better stats/provenance, conservative missing-stats strategies, generic runtime filters, selected encoded/fused kernels | Improvements survive parameter/data/encoding holdouts and concurrent/tight-memory runs | Incremental, evidence-selected |
| F — Product differentiation | Choose cloud/Iceberg pruning, native analytics, resident GPU or distributed throughput target | Repeatable advantage on the chosen workload plus honest costs and limits | Separate scoped program |

The first ten concrete work items are:

1. Commit the three-row false-uniqueness fixture and the multi-row-group shared-CTE fixture.
2. Separate exact constraints from estimation metadata; disable only unsafe semantic rewrites until proofs exist.
3. Centralize all-partition execution for root, CTE and subquery consumers.
4. Replace lossy comparators and fix harness exit/status semantics, including legitimate empty results.
5. Add canonical dbgen/qgen data alongside the frozen custom corpus and preserve manifests.
6. Publish one matched raw-Parquet and native/native baseline with complete samples.
7. Add query/operator IDs and actual rows, bytes, resource and phase metrics.
8. Reproduce morsel and just-under-threshold join resource gaps under a real cap.
9. Implement one reservation-backed scan→join→aggregate→sink vertical slice and compare it with the existing path.
10. Review the evidence to select the next optimization or the reuse alternative.

Do not require blanket architecture replacement to close stages A/B. Minimal safety/correctness fixes can precede larger types or scheduler refactoring. Acceptance should attach to independently verifiable outcomes rather than the number of closed epic tasks.

### 9.1 Build-versus-reuse checkpoint

In parallel with stage B or after A, timebox a DataFusion prototype to approximately one engineer-week. Keep the existing storage/catalog and benchmark contract, and compare representative scan, join, CTE, aggregation and tight-memory cases.

Evaluate semantic compatibility, Arrow/Lance version alignment, custom native-provider support, extensibility, performance, peak memory, correctness and maintenance burden. A slower prototype is still informative; integration overhead must be separated from core execution. Do not adopt DataFusion because it is Rust, or reject it because the current engine already contains 100k lines.

Continue owning the full engine if the shared-foundation work produces repeatable wins and the team can maintain the semantic/resource matrix. Prefer a reusable execution core if the differentiator lies in catalog/storage/service integration and the full SQL engine keeps consuming effort without durable performance advantage. This is a decision to test, not an already-justified rewrite.

## 10. Limits and final recommendation

This audit establishes code paths, documentation inconsistencies, mathematical counterexamples to unsafe premises, historical benchmark summaries, and evidence-backed experiments. It does not establish fresh wall times, prove every suspected failure at runtime, audit the entire SQL surface, or certify distributed reliability.

Prior historical reviews already proposed many sensible techniques. The recurring problem is that findings become narrative “done” claims without a durable, sufficiently broad executable contract. More literature alone will not solve that.

The recommended next program is **trustworthy measurement plus one shared execution/resource foundation**, preserving proven optimizations. Success means correct answers, bounded execution and reproducible wins on a declared workload. The next architectural choice should follow the measurements that program produces.
