# Native SF10 aggregate output quantum — September 9, 2026

## Result

Frozen `01bb077a` improves canonical native SF10 Q18 by an observed5.33% at four
threads and5.28% at sixteen compared with frozen `bd689bf6`. Both binaries use
partial ownership and parallel final reduction. All16outputs match independent
typed oracles; all16ownership/reduction traces verify. This is a two-block
instrumented diagnostic without confidence intervals, not DuckDB acceptance or
full protected-regression certification.

| Threads | Query | Fixed64, block ms | Adaptive up to1024, block ms | Geometric mean paired ratio |
|---:|---|---|---|---:|
|4|Q1|8165.288 /8218.055|8380.357 /8227.052|1.013639|
|4|Q18|5646.178 /5791.392|5400.587 /5426.333|0.946684|
|16|Q1|6067.353 /6149.687|6078.300 /5977.954|0.986827|
|16|Q18|4471.818 /4569.897|4424.068 /4144.456|0.947217|

Q1 has an observed1.36% slowdown at four threads and1.32% improvement at sixteen.
Preserve both observations; two blocks cannot establish the required regression
bound. No default ownership change is made.

## Phase attribution

Second-block sixteen-thread Q18's dominant aggregate has59,986,052input rows:

| Phase | Fixed64 ms | Adaptive ms |
|---|---:|---:|
|Ingestion|1879.678|1791.769|
|Finalization including output|2055.257|1743.354|
|Output construction and HAVING|1316.582|1005.099|
|Finalization excluding output|738.675|738.255|

The output interval falls311.483ms (23.66%), while other finalization work is
nearly unchanged. The first four-thread block also reduces output1259.490→944.358ms.
This supports the targeted repeated-construction diagnosis; it is not an exclusive
allocator profile. Output batch boundaries change, but624final rows remain exact.
All24profiled aggregate stages report zero spill.

## Reproduction and provenance

Job59512 exits0. Two fresh-process blocks reverse query, thread and binary order;
one process runs at a time and is reaped before the next. Both sides use the same
canonical SF10 native provider, original Q1/Q18 SQL,4GiB query/12GiB process
budgets, CPU affinity0–15 and4/16query threads. GPU is explicitly disabled. This
mask follows host SMT topology and is not sixteen dedicated physical cores.
The180-second diagnostic watchdog remains separate from10×DuckDB acceptance.

Release34350 exits0 in8m53s, locked/offline with `lance,gpu`, one build job and
48GiB cap. Candidate SHA256:
`01bb077a6793b955f43a098e0bf271bc52ce9a7570482e43aeeeb2459d9c975f`.
Control SHA256:
`bd689bf675ea1977e8573d6a63587cf5a8fb882164161b26439c8793b202ab23`.
All511candidate source inputs, both binaries, provider, dataset, driver and
harness verify after execution. Measurement scope peak6,909,325,312bytes, zero
swap, zeroOOM/max events. Build memory evidence is a labelled live snapshot,
not a terminal peak.

[Construction contract and correctness](aggregate-output-quantum-2026-09-09.md).
[Immutable measurement archive](benchmarks/2026-09-09-output-quantum-native/manifest.json).

## Acceptance still required

Run the protected query set and provider/residency/resource gates for this new
frozen binary. The [earlier provider screen](parallel-reduction-provider-screen-2026-09-09.md)
belongs tobd689bf6; its Iceberg result does not certify01bb077a. In particular,
larger constructed batches can change transient/filtering memory needs even
though the constructor reduces its own range on typed admission denial. Existing
library, integration and spill gates pass with their recorded six prior spill
failures, but broad cap/concurrency acceptance remains open. Do not replay HAVING
or publish partial output to force a constrained case to complete.
