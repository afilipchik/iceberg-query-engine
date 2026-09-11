# Bounded admitted output checkpoint — 2026-09-11

The admitted scan now constructs only projected survivor columns and packs filtered
rows into bounded typed output buffers. Final handoff storage is reserved before
consumption; a single pending batch and exact offset preserve progress. Source
errors remain terminal. Frozen binary `1bba20b3` contains 530 verified source inputs,
with Lance/GPU features, based on pushed checkpoint `0c554142`.

Four independent batching regressions now pass. Final focused coverage passes 23
scanner and four accumulator tests. Both ownership modes pass 1,125 library tests
(11 ignored), 125 contracts and 28 spill/numeric checks. Six existing spill failures
remain per mode; native/IPC passes 63 disjoint tests and 62 with one failure in
partial ownership. Full executable/count/exit comparison finds no added or removed
failures. First-batch working-space admission is still unresolved.

The three-binary diagnostic has 42 typed-correct outputs, 792 join traces and 14
identical logical/physical plan groups. Generic raw Q6 now produces 458 batches
instead of 7,323 for the same 1,139,264 selected rows. Its time is 0.898 times the
previous checkpoint, improving in both reverse-order blocks, but still 1.829 times
the pre-coercion baseline. Raw Q6 is 0.940 times the previous checkpoint; native and
resident-four-thread Q9 regress in both blocks. Two blocks do not establish broad
neutrality or confidence-certified performance. Retain this contract repair
provisionally; further attribution must explain remaining decoding/conversion cost.

Fresh canonical SF10 uses one session and three samples per query, default disjoint
ownership, 16 threads and matched 4/12 GiB query/process caps for providers. There
are 339 independently typed-correct outputs and 253/264 valid measured pairs:
raw 66, native 60, Iceberg 64 and Lance 63. Raw completes all 22 queries at 2.448
times DuckDB by geometric mean and 2.629 by suite time, with zero query wins.
Native Q1/Q6 time out in warmup. Iceberg Q13's second measured DuckDB request crashes
with SIGSEGV; the final dependent pair is not run. Lance Q9's warmup fails join-index
allocation. No incomplete provider receives a full-suite performance score.

Residency records 256 typed-correct outputs and 209/212 emitted measured pairs.
The intended 278-pair matrix is incomplete: canonical CPU-control Q1 times out,
and that failed prerequisite prevents all 66 canonical mixed-GPU measurements.
Canonical decoded IPC completes at 0.709 times DuckDB by geometric mean and 1.003
by suite time, with 14 query wins. Its 32/48 GiB capacity setting and excluded
preload do not clear the 16 GiB preload gate. Canonical GPU execution is not run;
the separate custom float smoke validates all 40 measured required-device requests.
Custom smoke is not canonical SF10 GPU coverage.

Checkpoint11187 is terminal1; independent evidence76516 is terminal0. All completed
outputs pass typed comparison. Archives verify 452 diagnostic, 1,436 provider and
1,097 residency files, plus source, focused and broad-validation evidence. Eight
empty temporary payloads are omitted. The cumulative diagnostic/screen scope peaks
at 22,185,889,792 bytes under 48 GiB, swap disabled, with zero OOM/max events.
No DuckDB leadership, complete resource or concurrency acceptance is claimed.

Commit and push this completed cycle, then continue the fixed-budget first-batch
reader reproduction: establish fresh one-row progress, compare a larger target,
and inspect retained early-column outputs on refusal. Separately retain an
oversized-page refusal control. Native prepared-input admission and selection-aware
decoding remain additional systemic work; do not force concurrency or relax budgets.

- [Implementation, tests and local DuckDB comparison](admitted-filter-batching-2026-09-11.md)
- [Targeted diagnostic](admitted-coalesce-triage-2026-09-11.md)
- [Provider screen](admitted-coalesce-providers-screen-2026-09-11.md)
- [Residency screen and per-query supplement](admitted-coalesce-residency-screen-2026-09-11.md)
- [Terminal checkpoint records](benchmarks/2026-09-11-admitted-coalesce-checkpoint/manifest.json)
- [First-batch memory evidence and next reproduction](first-batch-refusal-ledger-2026-09-11.md)
- [Native admission boundary](native-admission-follow-up-2026-09-11.md)
