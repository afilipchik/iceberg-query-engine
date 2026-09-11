---
name: realistic-benchmarks-duckdb-leadership
status: in-progress
created: 2026-09-05T21:52:30Z
updated: 2026-09-10T01:06:56.416603+00:00
progress: 0%
prd: .claude/prds/realistic-benchmarks-duckdb-leadership.md
---
# Realistic benchmarks and DuckDB leadership

The user approved all four CPU storage modes as separate leadership targets, public workloads, and evidence-based component replacement. Implement incrementally; the ten parent tasks are not ten small patches. Split each parent into narrow sequential changes with reproducer, contract, exact test command, before/after evidence and limitations before implementation.

## Frozen success contract

Raw Parquet and Iceberg use identical files/snapshots in DuckDB; native uses identical logical data in native DuckDB tables; Lance uses the direct reader over the same version. Never combine track scores. IPC additionally compares with equivalent preloaded Arrow. GPU compares with the same binary CPU control and matched DuckDB storage; cold upload and resident results remain separate.

For each required CPU mode and each main workload: all queries complete correctly; geometric mean and sum-of-query-medians ratios <=0.90; majority query wins; no query median >2x; 95% confidence upper bound for each aggregate <1.0; reproduced in three independent sessions. No OOM/crash, missing cases, or confirmed >10% protected regression. Required cap/concurrency scenarios must complete; clean refusal is separately safe but not completion. The 10x fresh DuckDB timing ceiling is a failure threshold, never parity.

## Benchmark profiles

Preserve existing custom SF10 as regression only. Canonical TPC-H SF1 development, SF10 main, SF100 holdout; full JOB and ClickBench main, deterministic small development extracts. Pin generators, SQL, source hashes, licenses and schemas. Never independently regenerate logical data per provider. Keep decimal semantics. Only documented equivalent SQL translations; unsupported queries are visible coverage failures.

Main latency: four modes x SF10/full JOB/full ClickBench. Holdout: four modes x SF100. Memory: SF10 at 1/4/16 GiB and SF100 at 4/16 GiB. Concurrency: SF10 at 1/2/4 clients under one shared 40 GiB process execution budget. Layout: ~256/16 MiB compressed files, ~128K/1M row groups, sorted/shuffled, date partitioning, Iceberg history/evolution/position and equality deletes, native/Lance compacted/fragmented versions. Cached IPC and GPU have explicit cold-process/warm-host/resident profiles. Label cold storage only when file-cache eviction is verified; never drop global caches.

Default existing host CPUs 0-15, 16 execution threads, 40 GiB query budget, matched process/cgroup settings. One warmup, three DuckDB calibration samples, ten randomized pairs, three sessions for acceptance, thirty pairs when inconclusive. One engine active at a time for latency. Record every pool, startup/catalog/cache construction/load/upload, CPU/RSS/cgroup/spill/temp disk, first batch/full consumption/teardown. Primary timing is embedded parse through full Arrow sink consumption on both sides; transport separately. No result cache or query-specific materialized answer in primary comparison.

## Safety and delivery

Follow AGENTS.md. Every engine/build/test/benchmark invocation goes through scripts/claude-safe-build.sh; TMPDIR is repository .scratch; coordinate heavy jobs. Preserve uncommitted work and old baseline artifacts. No weakening semantics, memory guarantees or query membership to win. Single-node relational analytics is the scope; distributed scaling and approximate vector search cannot count toward leadership. No speculative JIT/global scheduler/GPU join/new format project.

Sources: docs/project-audit-2026-09-05.md, docs/architecture.md, docs/benchmark-baseline-sf10-2026-09-05.md and the conversation's approved plan. Public sources: https://duckdb.org/docs/lts/core_extensions/tpch ; https://github.com/gregrahn/join-order-benchmark ; https://github.com/ClickHouse/ClickBench .

## Tasks

Current implementation and evidence are maintained in
[execution-status.md](execution-status.md) and
[the latest project checkpoint](../../../docs/execution-checkpoint-2026-09-06.md).
Earlier narrative is preserved in
[dated historical checkpoints](updates/2026-09-06-pre-scalar-checkpoints.md).
Do not attribute measurements from different source snapshots to current source.

- Shared numeric, CAST, membership, structural-proof and partition fixes restore
  canonical correctness. Cached MemoryTable statistics and pull-driven inner joins
  repair the reproduced IPC join-order/allocation failure.
- The frozen IPC repair passes full decoded IPC, Iceberg, Lance and raw-control
  SF10 gates; native previously failed timing and canonical GPU routing executed
  zero device samples. Public development extracts pass; full public workloads
  remain required. Four filtered-join cap scenarios complete with actual spill.
- The subsequent Boolean/scalar candidate fixes SQL NULL truth tables and removes
  full-length comparison-literal arrays. Its final library gate passes 557 tests
  with one pre-existing ignored; 13 focused Boolean/scalar tests pass. Matched
  raw/IPC screens validate all 220 executions: Q6 medians improve 52.6%/75.3%,
  Q14 26.6%/64.0%, with identical plans. IPC Q18 is 2.6% slower.
- The frozen scalar candidate completed all six canonical SF10 modes: 3,960
  validated pairs with no timing-gate failures. Those measurements belong to that
  binary; they do not certify later queue or ownership changes.
- Subsequent spill output ownership fixes pass 687 selected tests (one existing
  ignored test) and six development spill-cap cases. Their frozen full matrix is
  active. Native has four failing attempts; IPC Q19 times out independently in
  all three sessions. The current candidate therefore fails full acceptance.
  A prepared Filter/Project capability-composition repair is drafted separately,
  with runtime and performance validation pending. See execution-status.md for
  the exact live session and preserved failure evidence.
- Query-wide ownership, all provider/resource/concurrency/layout requirements,
  full public workloads and SF100 remain incomplete. No parent task is closed;
  the 0% frontmatter counts closed parent tasks, not engineering effort.

- [ ] [001 — Consolidate benchmark harness](001.md)
- [ ] [002 — Prepare public workloads and storage layouts](002.md)
- [ ] [003 — Run realistic baseline profiles](003.md)
- [ ] [004 — Repair semantic proof and partition hazards](004.md)
- [ ] [005 — Unify attribution and resource ownership](005.md)
- [ ] [006 — Implement incremental provider-neutral pipeline](006.md)
- [ ] [007 — Optimize dominant CPU costs](007.md)
- [ ] [008 — Finish provider and GPU behavior](008.md)
- [ ] [009 — Evaluate component replacement when triggered](009.md)
- [ ] [010 — Certify leadership and regression gates](010.md)
