# Prepared-key state and routing profile — September 8, 2026

Two contained diagnostics refine the next CPU target. No production code changed.
The formal four-block Q10/Q19 performance evidence remains in the prepared-key
implementation report; instrumented times here are not a replacement benchmark.

## Custom memory input: partial evidence only

Process26035 exited1. Instrumented custom float Q1 hit the fresh response deadline.
Its completed ingestion-call samples cover561864 rows and684 sampled positions:
key53151ns, lookup134646ns, state906294ns, commit79404ns. Missing/truncated calls
and unsampled rows prevent treating these totals as whole-query CPU time. Q6
warmup completed in5.087948ms. This is not canonical SF10 or GPU execution.

## Canonical Q10: complete diagnostic

Process57037 exited0. Fresh DuckDB warmup plus three calibrations and two engine
requests per binary all pass typed validation and the unchanged calibrated time
gate. Before is3ff868c7; after is1a0ece71. Each engine runs one warmup and one
sample, in fixed before/after order,16 threads,4GiB query/12GiB process limits,
under48GiB. `QE_AGG_PROF=1` and `QE_AGG_DETAIL_PROF=1` add overhead.

Both binaries use16 input slots and four aggregate workers, with1147084 input rows
in458 batches per request and no spill. Both produce381105 grouped rows in5957
output batches before downstream sorting/limiting. In the sample requests:

| Span (ms) | Before | After |
|---|---:|---:|
|Preparation/routing|360.313|418.405|
|Worker processing wall time|251.987|139.058|
|Total ingestion|612.494|557.951|
|Expression evaluation|20.572|20.524|
|Finish (includes output)|213.492|217.782|
|Output component of finish|212.290|216.563|

Do not sum nested spans or interpret worker wall time as summed CPU time. The
candidate's preparation/routing accounts for roughly75% of its reported ingestion
span in this sample. Shared key reuse moves work out of workers and adds retained
storage construction before dispatch; that serial phase is now the stronger
complete-query investigation target. The single ordered diagnostic cannot prove
the magnitude of an isolated component speedup or regression.

Across the two requests per binary,3664 sampled positions are reported. Before:
key4156232ns, lookup2393493ns, state1304327ns, commit277301ns. After:
key448926ns, lookup2475630ns, state3104067ns, commit554573ns. Sampling is sparse
and scheduling/cache effects differ; these numbers do not prove state updates
became more expensive in aggregate or isolate repeated type checks.

## Next bounded experiment

Test parallel preparation of disjoint input row ranges into admitted canonical
key/hash chunks. Retain the same key encoding, exact equality, batch/layout
identity and canonical owner routing. Admit chunk metadata and storage in one
shared child budget before mutation. Scoped work must finish before fallback,
error or owner destruction; only preparation admission denial may choose the
ordinary path before any aggregate state changes. Preserve row order within each
owner and existing spill cursors. Compare short/wide keys and small/large batches
to quantify dispatch overhead and retain low-memory completion tests.

The [batch-update-boundary analysis](aggregate-update-boundary-2026-09-08.md)
remains a valid architectural follow-up, but does not yet justify prioritizing
type-check removal over measured serial preparation. Full provider/resource and
residency acceptance remains open. Evidence:
`docs/benchmarks/2026-09-08-prepared-key-profiles/`.
