---
name: realistic-benchmarks-duckdb-leadership
status: active
created: 2026-09-05T21:52:30Z
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
