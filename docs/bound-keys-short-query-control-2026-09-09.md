# Bound key arrays: fixed-window Q6 control — 2026-09-09

Fresh identical-binary control45941 passes the prespecified precision gate before
the conditional candidate comparison. Both sides use frozen0f30c946. All32,784
engine outputs (32,768 measured and16 warmups) are correct and meet their fresh
DuckDB10× ceilings. All16 process-isolated windows close gracefully and exceed
the2-second exposure minimum; actual exposures range6292.749–8025.257 ms.

The geometric mean of fixed-window mean-time ratios is0.977710, with the
20,000-resample block-bootstrap95% interval0.900449–1.054570 (seed20260909).
It satisfies0.90≤lower≤1≤upper≤1.10 using unrounded values. This passes the
null rule; it is not an engine improvement. Earlier failed conditions remain
preserved in the isolated-window report.

Each side executes exactly2048 measured requests per block across eight balanced
blocks on physical-core mask0,2,4,6,8,10,12,14. Four query threads use4 GiB query
and8 GiB process budgets. One worker is alive at a time; failed attempts stop the
side and do not trigger retries. The48 GiB scope peaks at824,209,408 bytes with
zero OOM/max events. All508 source inputs and harness/binary hashes verify after
execution. The [33,055-file archive](benchmarks/2026-09-09-bound-keys-q6-isolated-physical-null-01/manifest.json)
preserves every sample, typed comparison, process/context snapshot and prespec.

Candidate4841 finished successfully under the same predeclared conditions,
comparing threshold control1efb2554 with candidate0f30c946. All32,784 outputs
are correct/gated, all16 windows complete with graceful cleanup, and exposures
range6407.898–8124.932 ms. The mean-window ratio is0.970951, with95% interval
0.903637–1.062568: the upper bound passes1.10, but the interval includes1, so
this does not establish a speedup. Scope peak is823,832,576 bytes, zero OOM/max
events. The [33,055-file candidate archive](benchmarks/2026-09-09-bound-keys-q6-isolated-physical-candidate-01/manifest.json)
verifies all508 source inputs and preserves the unchanged measured harness.
Archive16765 completed successfully before source edits resumed. The original
alternating-request and failed null studies remain distinct historical evidence.
