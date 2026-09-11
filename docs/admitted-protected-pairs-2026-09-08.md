# Controlled Q2/Q19 regression check — September 8, 2026

The separate full-screen increases for Q2 and Q19 are not confirmed by controlled
paired runs. Frozen b03f53d3 is the before candidate; ed721285 is after. Both use
the same data/SQL,16 threads, inherited matched CPU affinity,4GiB query budget,
12GiB process cap and48GiB containing scope. Each query has four fresh blocks,
balancing startup order and starting execution order independently. A block has
one gated warmup and six alternating measured pairs, with a fresh DuckDB warmup
and three typed-validated calibrations establishing its10× ceiling.

| Query | Geometric mean of four block median ratios (after/before) | Block-bootstrap 95% interval | Confirmed >10% regression |
|---|---:|---:|---|
| Q2 | 1.0096 | 0.9177–1.1195 | No |
| Q19 | 0.9665 | 0.9319–0.9973 | No |

The interval resamples the four whole blocks, not individual correlated samples.
Four blocks are limited evidence: Q2 does not establish either a regression or
equivalence within10%. Keep it protected. This is not a full-suite or leadership
result and does not clear existing query timeouts or provider/reference failures.

Job29163 exited0. All112 engine executions (16 warmups plus96 measured) pass exact
typed validation and their fresh time gates. All24 workers, including references,
exit0 without forced shutdown. Scope max/OOM/OOM-kill counters remain zero;
peak1929084928 bytes. The new normal-EOF harness cleanup was used. No engine source
change was made during measurement; later aggregate-frontier edits are not part of
either frozen binary.

Driver, settings, hashes, SQL contracts, all responses/results, references,
teardown records and summaries are preserved in
`docs/benchmarks/2026-09-08-admitted-protected-pairs/`. The previous separate-screen
observations remain in `admitted-queue-provider-screen-2026-09-08.md`.
