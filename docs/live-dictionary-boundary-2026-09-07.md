# Logical dictionary values at the live aggregate boundary

The first release canonical SF1 smoke after live spill integration failed:
15 of 22 queries validated three measured pairs each; Q05, Q07, Q09, Q10,
Q18 and Q21 returned query errors, and Q13 exceeded its query deadline.
There is no valid suite performance score. Original evidence remains in
`.scratch/public-bench/live-spill-sf1-01/`; the frozen executable is
`.scratch/live-spill-integration/benchmark_embedded`.

All six query errors reproduced at one shared boundary: the bound group type
was Utf8 but joins supplied Dictionary(Int32, Utf8) arrays. Physical encoding
was incorrectly treated as a different logical domain by the new partial-state
implementation. This was a regression in the live integration, not evidence
that these six queries had always failed.

## Repair and scope

`key_rows/arrow_input.rs` now resolves dictionary cells by borrowing the values
array and checking its index. All eight signed/unsigned index widths are supported.
Both NULL codes and NULL dictionary values are SQL NULL. Group keys, fixed state
inputs and selected MIN/MAX/first values share that resolution. No decoded string
array is allocated. Dictionary codes are never persisted as group identity.
Logical type validation still preserves decimal scale and timestamp identity.
Declared dictionary layouts, views and other unsupported layouts retain their
existing pre-input capability fallback; this repair handles runtime dictionaries
under a supported bound logical schema.

The local DuckDB snapshot at `1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`
provides a useful comparison. `aggregate_executor.hpp:270` dispatches flat,
constant and unified input at the batch boundary. `vector.cpp:1199` exposes
dictionary selection, child data and child validity through `ToUnifiedFormat`.
Our borrowed resolver repairs semantics; it does not yet provide DuckDB's
batch-specialized dispatch. Repeated per-row dispatch is a performance hypothesis
to measure, not a demonstrated cause of Q13's timeout.

## Verification

Working tree based on HEAD `88849c4f8e47b6fc9149ffd15de0429cd9d3ad41`,
features `lance,gpu`. Builds and execution used `scripts/claude-safe-build.sh`,
repository TMPDIR, 48GiB build scopes and one build job. Tests used four Rayon
threads. The debug query worker used a 32GiB scope and QE_MEM_CAP=12G.

- Library: 888 passed, zero failed, 10 explicitly ignored. The ignores retain
  the dedicated GPU/IPC and dependent-join limitations recorded in the live
  integration report; this is not new hardware coverage.
- Selected integrations: 26 passed, zero failed or ignored: budget transition,
  input errors, float extrema, systemic numeric and live dictionary input.
- New integration: two input partitions, changing/reversed dictionary codebooks,
  empty batches, NULL keys and values, COUNT/MIN/MAX, both planner hints,
  independent 64-group oracle, actual spill under an 8KiB operator budget and
  4MiB query pool, one execution per partition and reservation/file cleanup.
- Six original canonical queries: all completed with the repaired debug worker
  and passed the existing typed comparator against preserved DuckDB oracles,
  including ordered and LIMIT policies. These are correctness diagnostics,
  **not release latency measurements**.
- `cargo fmt --all -- --check` passed.

Commands, diagnostic responses, comparison results, logs and the four changed
source/test files are [archived with hashes](benchmarks/2026-09-07-live-dictionary-boundary/manifest.json).
The first comparison attempt lacked the comparator's scratch directories; its
validation errors are preserved separately. Creating those directories resolved
the invocation error without changing either query result.

## Open gates

Q13's first measured request timed out against a 347.181ms query ceiling.
The next two failures report an unavailable worker after watchdog termination;
they are consequences of that timeout, not independent OOM observations.
Rebuild release and repeat canonical validation before a fresh SF10 baseline.
Attribute ingestion, merge and output work before changing shared execution.
The current 64-row output chunks and single state controller are hypotheses to
investigate. Provider modes, constrained-memory/concurrency certification and
DuckDB leadership remain open.
