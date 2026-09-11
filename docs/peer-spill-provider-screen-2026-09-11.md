# Coordinated spill: canonical SF10 provider screen

Frozen a4103dfa provider25249 is terminal1. The independent completed-output
audit verifies332 typed-correct outputs:249 measured pairs and83 warmups.
All249 completed measurements pass their query-time gates;15 requested pairs
were not run because their warmup or reference calibration failed.

| Provider | Valid/requested pairs | Missing measurements |
|---|---:|---|
| Raw Parquet |66/66|None|
| Native |60/66|Q1 and Q6 warmup timeouts|
| Iceberg |63/66|Q9 reference calibration allocation refusal|
| Lance |60/66|Q1 warmup timeout; Q9 reference calibration allocation refusal|

Raw Parquet gives geomean2.955825909450407, suite ratio3.3481674313969916,
zero query wins and worst ratio8.494388281184435. Ratios are engine/DuckDB;
this complete single session does not establish leadership. Incomplete providers
have no accepted suite result.

Native Q1 warmup times out at2790.3704391792417ms ceiling and Q6 at
1008.2943923771381ms. Iceberg Q9 calibration cal1 refuses a524288-byte block
after348.9963267929852ms. Lance Q1 warmup times out at7742.103170603514ms;
Q9 calibration cal0 refuses a262144-byte block after94.83767487108707ms.
Reference allocation exceptions do not identify which resource layer caused them.
No dependent timing ceiling is invented for failed calibrations.

After-run checks verify520 source inputs, the frozen binary and harness/driver
provenance. The capped scope peaks at20658921472 bytes, with swap disabled and
zero max/OOM/kill events. These observations do not certify query-wide memory
accounting. All timed work and the completed-output audit have finished.

Conditions: canonical SF10, default disjoint ownership,16threads/CPU0–15,
4GiBquery/12GiBprocess,3samples/1session, GPUoff. Fresh DuckDB calibration sets the
10×query-time ceiling; startup allowance is separate. The48GiB capped wrapper
contains engines and reference/comparator work with swap disabled. No source or
harness changes during measurement. Decoded residency and GPU execution are separate.

Original artifacts: `.scratch/public-bench/peer-spill-sf10-providers-01`. Preserve
all refusals, timeouts, reference failures and missing pairs; do not retry until
favorable. Full resource/concurrency and multi-session acceptance remain open.

See [correctness and resource changes](coordinated-spill-progress-2026-09-11.md)
and [completed paired diagnostic](peer-spill-measurement-2026-09-11.md).

Archive: [verified manifest](benchmarks/2026-09-11-peer-spill-providers/manifest.json).
