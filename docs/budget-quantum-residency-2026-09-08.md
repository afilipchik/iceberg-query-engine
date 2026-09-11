# Residency screen — September 8, 2026

Driver22984 finished with exit1. No GPU performance was established. This screen
uses frozen engine3ff868c77eca9340c517f492ce2f9adb1c66e5fc0d904dac7e9e5b503a7e4628
and the reference-retirement harness, sequentially inside the48GiB wrapper.
The driver records commands, timestamps, workload labels and terminal statuses.

| Case | Budget: query/process | Result |
|---|---|---|
| Canonical SF10 decoded IPC |16/32GiB|Driver supplied redundant explicit preload flag; CLI rejected configuration before execution|
| Canonical SF10 GPU CPU control |16/32GiB|Setup refused a28,600,174,512-byte preload reservation against17,179,869,184-byte limit;66 measured engine slots not_run|
| Canonical SF10 mixed GPU |16/32GiB|Rejected because CPU control did not complete correctly; no GPU execution|
| Custom float smoke CPU control |4/8GiB|Q1 completed warmup late;20 dependent engine samples not_run. All20 Q6 measured comparisons valid|
| Custom float smoke required GPU |4/8GiB|Rejected because CPU control did not complete correctly; no GPU execution|

The custom fixture is not canonical TPC-H or SF10. Its Q1 warmup took165.805961ms
against a124.90751221776009ms ceiling, calibrated from DuckDB samples
12.20550574362278,12.490751221776009,13.489697128534317ms. It returned six rows;
the harness short-circuits warmup comparison after a failed time gate, so those
rows have not been independently certified by this screen. Its physical plan is
ExternalSort -> Project -> SpillableHashAggregate -> Project -> Filter ->
MemoryTableScan. Q6 warmup completed in10.105143ms.

The16GiB canonical preload refusal supersedes the preceding assumption that this
budget would permit this input. The requested reservation is conservative admission,
not observed RSS. Do not reduce it without proving the retained allocation and
decoder lifetime contract. The invalid decoded-IPC invocation is a driver error,
not an engine failure. Omitting the redundant flag fixes configuration but does
not establish that the same preload fits16GiB.

Evidence is preserved under `docs/benchmarks/2026-09-08-budget-residency/` with
checksums, original traces, output files, manifests, source freeze, driver and
harness. No engine source changed during this screen. No complete residency,
device-use, resource or leadership claim follows from it.

Next: investigate generic memory-table/filter input and aggregate execution under
the existing budget, with source inspection and bounded component profiles. A
separately labeled higher-budget preload experiment may measure a different
configuration, but cannot close this16GiB failure. Preserve strict CPU-control and
device-evidence gates. The decoded-IPC correction prepared in scratch still uses
16GiB; do not launch a full suite expecting it to fit.
