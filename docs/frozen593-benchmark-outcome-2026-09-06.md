# Frozen593: owned row store and prepared output — 2026-09-06

The row-store repair fixes reproduced silent type corruption and restores bounded nested-join parallelism. Decoded IPC improves against both the immediately preceding build and the accepted scalar control. Overall performance acceptance still fails: raw Parquet is 2.4278× matched DuckDB by suite sum of medians, and protected regressions remain.

## Change and validation

Actual physical types/counts and null-free arrays are checked across all build batches before row packing. Packed data, terminal batch offsets, column metadata and temporary packing views own checked query-pool reservations. Both gather entry points preserve signed values and Float64 bits. Validated fixed-width row stores now compose copied-output proofs across prepared multi-batch joins; unsupported generic multi-batch paths still decline. No query identifiers or query-specific constants were added.

The independent negative cases reproduced Int32 -1 becoming Int64 4294967295 and Float64 -1.0 being interpreted as an Int64 bit pattern. The repaired source passes736 selected tests, with one pre-existing ignored test remaining. Six cap checks complete with actual spill; aggregate/sort validators are exact, while the filtered-join cap only checks row count and does not exercise row-store execution. Dedicated ownership tests cover row-store admission and cleanup.

Actual IPC SF10 preparation probes show both previously declined nested build joins exposing16 bounded streams, with every reservation released after plan/context cleanup. The fix does not establish full query memory ownership: original batches, keys, runtime filters, decoder state, generic maps and general output scratch still have open work.

## Completed development screens

All22 canonical SF10 queries, three paired steady samples plus one warmup,16 threads/affinity0–15,40 GiB query pool,48 GiB process cap,96 GiB containment. Each query uses a fresh DuckDB reference and10× query-time gate with independent typed result validation. All1056 engine requests across six paired screens pass correctness and time gates (five CPU modes plus the additional IPC/591 comparison). These are one-session development screens, not multi-session leadership certification.

| Comparison | Suite candidate/control | Geomean candidate/control | Suite candidate/DuckDB | Protected regressions >10% |
|---|---:|---:|---:|---|
| Decoded IPC vs preceding591 |0.9344|0.9520|0.4751|None|
| Decoded IPC vs scalar control |0.9417|0.9852|0.4830|Q22:1.1425×|
| Raw Parquet vs scalar control |1.0891|1.0864|2.4278|Q10:1.5430×; Q12:4.7650×; Q16:1.6190×; Q17:1.1822×; Q18:1.1271×|
| Native vs scalar control |0.9949|1.0101|3.4571|Q16:1.2387×; Q21:1.1125×|
| Iceberg vs scalar control |1.0798|1.0778|0.3229|Q10:1.5937×; Q12:4.9251×; Q16:1.5735×|
| Lance vs scalar control |0.9865|1.0212|1.2735|Q2:1.1283×; Q8:1.3102×; Q14:1.2901×; Q16:1.4415×; Q20:1.1290×; Q22:1.1276×|

IPC Q10 improves478.155→373.942ms against591 (21.8%). Its separate scalar comparison is361.040→377.706ms (4.6% slower), recovering the prior protected regression. The direct comparison's execution-phase medians decrease449.648→348.445ms with identical printed plans. These observations support restored pipeline overlap; printed-plan equality is not proof of identical internal execution, and phase medians do not sum to a paired latency median.

IPC Q22 remains14.2% slower than scalar. Its planning medians are14.055→18.830ms and execution28.143→29.554ms. Planning includes scalar-subquery execution; this is the next attribution target, not proof of a particular operator cause. Q22's one-output Anti join bypasses the multi-partition queue repaired for Q10. Raw Q17/Q18 also cross the development protected threshold in this session and must remain visible; no causal attribution or selective retest acceptance is claimed.

Decoded residency is separate from raw storage. The IPC/DuckDB ratios cannot establish raw-Parquet leadership. [All ratios](benchmarks/2026-09-06-prepared-rowstore/cpu-screen-summary.json) and [phase medians](benchmarks/2026-09-06-prepared-rowstore/cpu-phase-summary.json) preserve the per-query detail.

## Identity and remaining work

Source593 archive SHA256 `e7e3194fa2c9f4e0274c205bc5a76c1643fe0ea79c2251c2b1454dc026534b8e`; benchmark binary `18e067aa86040bcd43baa6097a82d7434aecd3d4beb5a5c86628129348dea6a3`. Release91456 passed10m23s with Lance/GPU features. All593 live source hashes match the frozen archive. Default-feature tests and feature release compilation are separate coverage. [Evidence directory](benchmarks/2026-09-06-prepared-rowstore/README.md) records exact commands, source, binaries, outputs, plans and archive verification.

Native, Iceberg and Lance43730 completed528/528 valid requests. Archive46216 verifies1735 provider files, originals, all593 source hashes and pinned binaries. Provider archive SHA256 `ec3630ae4c46c1d096c4a8e3c59aeb18b5da5073cc95dbce3159e4573e737786`; [provider ratios](benchmarks/2026-09-06-prepared-rowstore/provider-screen-summary.json). Every CPU mode still has a protected regression against scalar control. The Iceberg/DuckDB win is specific to that matched provider/reference setup; it does not repair regression against the engine's own control.

Canonical GPU36741 completed132 valid measured requests (66 GPU-enabled,66 same-binary CPU control), with **zero device executions and zero upload requests**. Suite/DuckDB ratios2.38284 GPU-enabled and2.36499 control describe CPU routing, not canonical acceleration.

Float smoke36738 completed80 valid measured requests (40 per track), with39 device executions and one GPU-enabled CPU fallback at Q1 iteration1. Suite/DuckDB ratios0.067729 GPU-enabled and1.265474 control apply only to the600K-row custom float fixture. The fallback is preserved, not discarded as warmup. GPU archive96882 verifies850 files and all593 then-live source hashes, SHA256 `fa73991c29ca702989c594223812dac1da0a975a32af9aae1f2be268044d2964`. [Device counters](benchmarks/2026-09-06-prepared-rowstore/gpu-execution-summary.json).

These are frozen593 mixed-routing measurements. Subsequent resident-session implementation changes are newer and have no accepted release measurement yet. Resident GPU certification requires [the acknowledged session contract](gpu-resident-session-implementation-2026-09-06.md), not repeated warmups. The [variable-output ownership review](variable-output-ownership-integration-review-2026-09-06.md) identifies decoder and retained-buffer prerequisites for the raw path. Both remain implementation work. Broader public, concurrency, resource and leadership certification remain incomplete.
