# Native Q12 phase follow-up — 2026-09-15

Intermediate checkpoint `a857215baec3c7cc91c0245738b4e914df09cf82` is pushed and
remote-verified. The dictionary candidate remains provisional because the first
native comparison regressed Q12 in both blocks. This continuation makes no
production source change and does not replace that failed gate.

## Completed diagnostic

Contained run64196 compares the same frozen parent6d0b9317 and candidate138199d2
in two reversed blocks, fresh worker per case, one warmup/three samples, nativeQ12,
16threads/affinity0–15,4GiB query/12GiB process,180second diagnostic watchdog.
`QE_AGG_PROF=1`, `QE_INPUT_QUEUE_TRACE=1` and `HJ_TIMING=1` instrument both sides.
Per-request `/proc` counters and stderr byte ranges are preserved. A separate
contained audit validates all16outputs with identical optimized/physical plans.

Candidate/parent ratios are1.034244 and1.233830 (785.823→812.733ms and
687.515→848.277ms). Instrumentation changes the measurement; these are not
replacement acceptance timings or a proof that pruning caused the difference.

Every join-build trace declares8partitions but `prepared=false`, no admitted
buffers or declared byte bound, and **slots=1**. Median producer permit waits
are528.645/572.867ms in block1(parent/candidate) and462.403/602.923ms in block2.
Median producer polling is73.365/79.289ms and63.323/82.781ms respectively.
Concurrent producer durations overlap: never sum them as query wall time.
`sort input queue` preparation wraps upstream execution; its long duration is
not evidence that sorting two output rows is the bottleneck.

All16requests record zero major page faults. Minor-fault medians are similar
(46–49thousand). This does not prove that cache behavior or memory placement is
irrelevant, but offers no evidence for major-fault disk I/O as the cause here.
The diagnostic cgroup peak is188,227,584bytes under32GiB, zeroOOM/max/no swap.
It is not RSS: recorded process high-water marks are about475MB, and shared
file-backed pages need not be newly charged to this scope.

## Actual data weakens the payload-cost hypothesis

A capped metadata audit checks all58lineitem and15orders IPC files. Their schemas
are consistent within each table. Lineitem has dictionary-encoded return flag,
line status, ship instruction and ship mode; orders has dictionary-encoded order
status and priority. Comment/clerk columns are plain strings.

The first file/batch representative has lineitem dictionary value buffers of
15,10,64,58bytes (3,2,4,7values), and orders15,62bytes (3,5values). These exact
sizes are a representative check, not an all-file payload inventory. They make
large unused dictionary payload decoding an implausible explanation for the
hundreds-of-milliseconds timing changes. The synthetic large-dictionary test
proves mechanism and correctness, not SF10 relevance.

This was a gap in the optimization approach: the actual workload's dictionary
sizes should have been checked before proposing a speedup. Future performance
work must establish the target encoding, size and measured cost before a full
candidate cycle. Preserve the envelope/repeated-projection correctness fixes;
do not promote the pruning optimization on the basis of this synthetic test.

## Next bounded work

1. Use a matched same-binary control to isolate pruning from unrelated code-layout,
   allocation and sequence variation. Retain all samples and failures.
2. Prioritize the directly observed missing native admission contract. Account for
   footer/schema/descriptors, dictionary/deletion state, decode scratch and retained
   outputs before advertising prepared concurrency. Do not force slots above1.
3. Verify the actual prepared workload boundary, then test NULL/duplicate/empty,
   multi-partition/deletion, cancellation, retained-owner and named-refusal cases.
4. Only after a measured change, repeat both ownership gates, frozen provider and
   residency SF10, archive, commit and push the next intermediate checkpoint.

Artifacts remain under `.scratch/parallel-aggregate-input/paired-ipc-q12-trace-01`,
with drivers `paired_ipc_q12_trace.py`/`audit_paired_ipc_q12_trace.py`, plus
`ipc-q12-native-schema-audit.json` and `ipc-q12-dictionary-sizes.json`.
They belong to the next cycle's archive; no second checkpoint or new performance
acceptance is claimed yet.


## Same-binary control in progress

The frozen diagnostic binary `cfc3b4d10029e02717bdc90a07f6a58a4b8ab50d4e7afb13fadc916e8f8b6d24` adds `QE_IPC_DICTIONARY_CONTROL=prune|decode-all`, cached and announced once per worker. Both routes retain envelope validation, repeated-projection normalization and all limits. Focused1023 passes5domain tests; integration77833 passes31tests per policy. Control35904 completes four alternating/reversed blocks on Q1/Q6/Q12 without aggregate/queue profiling. All96 outputs independently validate and plans match.

Prune/decode-all ratios by block:

| Query | Block1 | Block2 | Block3 | Block4 |
|---|---:|---:|---:|---:|
| Q1 | .986503 | 1.003574 | .994544 | 1.006391 |
| Q6 | 1.141353 | 1.350620 | .784615 | .837124 |
| Q12 | 1.330559 | .620936 | 1.550088 | .750241 |

There is no consistent causal pruning effect. The previous between-binary Q12 regression remains a failed performance gate; this control does not erase it. Cumulative build/control peak is10,581,041,152bytes under48GiB, zeroOOM/max, swap0. The first audit failed because a template substitution corrupted the `mkdir(parents=True)` keyword. The corrected audit alone passed; timings were not rerun. Original and corrected scripts, failure, recovery and all outputs are preserved in the [268-file control archive](benchmarks/2026-09-15-ipc-dictionary-control/manifest.json), including an802-input verified source snapshot.

The temporary production environment control has now been removed; the private test helper retains a decode-all comparison. The [proposed admitted-reader design](native-admitted-ipc-design-2026-09-15.md) is still unimplemented. A separate reproduced one-slot scheduler defect is under validation; see [scheduler evidence](serial-frontier-contract-2026-09-15.md).
