# Shared-provider prescan error propagation

The planner's shared-table cache preparation used `provider.scan(...).ok()?`
inside a parallel `filter_map`. A provider failure became a cache miss, and
ordinary planning invoked that provider again. A second successful scan could
hide an error after the first invocation had already consumed or changed input.

The regression reproduces this on unchanged SQL:
`SELECT v FROM t UNION ALL SELECT v FROM t`. Its provider returns a named storage
error on the first scan and data on subsequent calls. Before the fix, the query
incorrectly succeeds with six rows. The paired success control passes. This is
an observed lifecycle failure, not just a source hypothesis.

`prescan_shared_tables` now returns `Result<()>`; parallel tasks collect
`Result<Vec<_>>` and propagate the provider error through physical planning.
Cache publication occurs only after every collected task succeeds. Already
running sibling scans may complete, but this path does not replay a failed
provider or return a successful query after its error. The successful cache path
is preserved, including duplicates, SQL NULLs and empty input batches.

This change does not bound prescan allocation, remove eager planning scans,
declare snapshot/replayability capabilities, or fix every speculative subquery
execution boundary. Multiple concurrent errors do not have a promised ordering.
No dependency change or performance gain is claimed. Frozen release benchmark
results predate this repair.

## Validation

All Rust commands use `TMPDIR="$PWD/.scratch"`, `RAYON_NUM_THREADS=4`,
`SAFE_BUILD_MEM=48G`, `SAFE_BUILD_JOBS=1` and `scripts/claude-safe-build.sh`.

- Before: `cargo test --locked --features lance,gpu --test shared_prescan_errors`
  gives one reproduced failure and one successful control.
- Initial after-fix gate: shared prescan, materialization partition audit,
  aggregate input lifecycle and outer-ON pushdown integrations pass19 tests with
  zero skips. A typed MemoryLimit preservation test is subsequently added.
- Final gate:889 library passes, zero failures,10 explicit ignores;20 selected
  integration passes, zero failures/ignores. The ignores are eight dedicated CUDA
  cases, one dedicated IPC-cache case and the pre-existing dependent-join case.
  Typed MemoryLimit fields and one-call/no-replay behavior are verified.
- `cargo fmt --all -- --check` and `git diff --check` pass.

[Archived source and before/after logs](benchmarks/2026-09-08-shared-prescan/manifest.json)
preserve the exact reproduction. No commit or release rebuild was performed.

The benchmark supervisor also now distinguishes reused setup failures from
executed query failures. Its109-test gate passes with zero skips, including real
Lance and comparator spill. [Preparation and supervisor evidence](ipc-preload-admission-2026-09-08.md).

## Next shared work

The measured Q13 path still spends seconds ingesting1.5 million aggregate groups
through one state controller. Q10 output ownership removed measured work but did
not close its deadline gap. Further parallel or typed-batch changes must preserve
the admitted partial-state/spill lifecycle and all-partition execution; the local
[DuckDB/ClickHouse source comparison](local-engine-source-comparison-2026-09-07.md)
provides implementation references, not proof that a copied layout will improve
this engine. Provider preparation remains a separate source of memory exposure
and work inside planning. Neither issue is fixed by a query-specific shortcut.
