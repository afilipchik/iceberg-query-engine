# Aggregate one-slot input contract — 2026-09-15

Frozen candidate `be16425374af0afeb0dac1828acb96acc8bfdb31061aaf7e9806c119ff0357e9` repairs a reproduced aggregate input scheduling contract. Validation and the full provider/residency cycle are complete. The resource-contract fix passes its regressions, but canonical native timeouts and a reference refusal remain failed gates. No overall DuckDB leadership or general performance acceptance is certified.

## Defect and implementation

The frontier advertised one demand slot for unknown or underfunded input while using FuturesUnordered for opens and SelectAll for streams. Pending operations could retain working buffers while other partitions were polled. A three-partition fixture reproduces three simultaneous operations with slots=1, independently of any specific SQL query.

One retained opening future and one current stream now drain each partition before advancing the pending queue. Both live in the frontier across cancelled next() calls, preventing replay or loss of pending input. Prepared unknown streams are consumed exactly once. Panic conversion, partition checks and admitted multi-slot scheduling remain. This bounds frontier activity, not provider-internal background work, prepared-source allocations or whole-query memory. No dependency, ownership default, budget or native admission capability changes.

The temporary same-binary IPC pruning environment control is absent from production source. A private test helper retains the decode-all comparison, with identical typed output and an actual decode-count difference. The completed control and its failed/corrected audit are preserved separately in the [268-file control archive](benchmarks/2026-09-15-ipc-dictionary-control/manifest.json); see [control and Q12 attribution](ipc-q12-phase-follow-up-2026-09-15.md).

## Regression evidence

Red53440 fails on three overlapping operations. Green42528 passes10frontier tests; lifecycle green38474 passes11, including pending open/pull cancellation, resume and shutdown, prepared unknown input, empty batches, NULLs, duplicate rows and every partition. Existing tests exercise admitted overlap, retained-buffer owners, actual spill and preparation errors.

First broad69274 exposed six tests that required both unknown partitions to start even after the first failed. Preserve the [18-file failed-run archive](benchmarks/2026-09-15-serial-frontier-validation/manifest.json). All eight existing lifecycle test names remain. Each now covers serial and bounded parallel input in both aggregate ownership modes. Serial failure asserts later partitions stay unopened. Parallel fixtures declare their fixed nullable Int64 output bound and synchronize initialization, proving a pending sibling exists before the injected error. Original error/panic classification, no replay, exact successful rows and zero leaked reservations remain asserted. Timeout now covers execution and stream collection. Focused9871 passes8.

Final cycle30002 compares executable counts, retained test names, failure names and exit status against pushed parenta857215:

| Suite | Disjoint | Partial |
|---|---|---|
| Library | 1142 pass,11 ignored | 1142 pass,11 ignored |
| Contracts | 128 pass | 128 pass |
| Native/IPC | 64 pass | 63 pass,1 known numeric failure |
| Spill/numeric | 28 pass,6 known failures | 28 pass,6 known failures |

No prior test is retired. The partial native aggregate SUM/AVG exact-bit failure and the six legacy spill failures remain failures. The [32-file validation archive](benchmarks/2026-09-15-serial-frontier-v2-validation/manifest.json) includes an802-input source snapshot, red/green fixtures, lifecycle evidence and commands. The locked/offline Lance+GPU release completes0 in8m51s with source hashes verified before and after.

## Canonical SF10 providers

One matched session, three measured samples/query,16logical threads/affinity0–15,4GiB query/12GiB process, default disjoint ownership. The query ceiling remains10× the matched DuckDB calibration. Independent audit validates all336completed engine outputs and252of264measured pairs. Ratios are engine/DuckDB; lower is faster.

| Track | Typed outputs | Valid pairs | Geometric mean | Suite ratio | Wins |
|---|---:|---:|---:|---:|---:|
| Raw Parquet | 88 | 66/66 | 2.433841 | 2.642917 | 0/22 |
| Native | 76 | 57/66 | incomplete | incomplete | not certified |
| Iceberg | 84 | 63/66 | incomplete | incomplete | not certified |
| Lance | 88 | 66/66 | 1.997011 | 2.949275 | 1/22 |

NativeQ1/Q6 and additionalQ18warmups time out. Their nine measured engine requests are NOTRUN, without retries. IcebergDuckDBQ9oracle refuses268435456bytes, preventing calibration and three dependent pairs. This is a named reference allocation refusal, not an engine correctness failure or OOM kill. Preserve every failed gate. All802source inputs, frozen binary and harness hashes verify after timing. See the [1275-file provider archive](benchmarks/2026-09-15-serial-frontier-v2-sf10/manifest.json).

## Matched native attribution

Two reversed blocks compare frozen parent138199d2 with candidatebe164253, one warmup/three samples per fresh worker/query, matching native setup and budgets. Aggregate profiling and a180s diagnostic deadline make this attribution evidence, not normal performance acceptance. All80outputs independently validate and optimized/physical plans match.

| Query | Candidate/parent block1 | Block2 |
|---|---:|---:|
| Q1 | 1.003247 | 1.005119 |
| Q6 | .867331 | .916584 |
| Q12 | .665246 | 1.072963 |
| Q18 | .987896 | .993545 |
| Q9 | .975818 | .998965 |

Q18 does not reproduce a candidate slowdown: parent4314.684/4355.040ms, candidate4262.461/4326.930ms. This revises the scheduling-slowdown hypothesis but does not clear its canonical timeout. Q12 remains variable. Q6's lower ratios are observations, not a certified speedup. No blanket regression-free claim follows.

Q1 candidate ingestion takes4639–4656ms and evaluation1408–1410ms. Q18 ingestion takes2677–2719ms and finish993–995ms. Output time is nested within finish and must not be added again. These are phase elapsed times, not sampled CPU attribution; a fresh owned-child profile is required before selecting a hot-function optimization. See the [173-file attribution archive](benchmarks/2026-09-15-serial-frontier-v2-attribution/manifest.json).

## Residency and actual GPU execution

Post79671 completes0. All five cases complete; independent audit validates348outputs/278pairs. Canonical cases use32GiB query/48GiB process capacity, with preload excluded from query timing. They do not clear16GiB preload or the full resource/concurrency matrix.

| Case | Typed outputs | Valid pairs | Geometric mean | Suite ratio | Wins |
|---|---:|---:|---:|---:|---:|
| Canonical decoded IPC | 88 | 66 | .798434 | 1.254066 | 14/22 |
| Canonical GPU control | 88 | 66 | .805281 | 1.289498 | 14/22 |
| Canonical mixed GPU | 88 | 66 | .798088 | 1.290919 | 14/22 |
| Custom float GPU control | 42 | 40 | 1.332155 | 1.682236 | 1/2 |
| Custom float required GPU | 42 | 40 | .082191 | .080222 | 2/2 |

Canonical mixed mode records zero successful device runs across88outputs. It is not demonstrated GPU acceleration. The separate custom required-GPU smoke proves40/40measured device requests, matching preparation sessions, no query-time uploads, and no new fallback/allocation failures. It is not canonical SF10. See the [residency archive](benchmarks/2026-09-15-serial-frontier-v2-residency/manifest.json).

Cumulative validation/build/provider peak24,877,572,096bytes stays under48GiB; the separate postcheck peak21,012,893,696bytes stays under64GiB. Both record zeroOOM/max and swap0. These cgroup peaks are not process RSS or proof of query-wide admission. The read-only host snapshot shows16logical CPUs representing8physical cores withSMT and the powersave governor. Both engines retain matching settings; no host setting changed and this snapshot does not prove a cause for timing variation.

## Reproduction and next work

The [controller archive](benchmarks/2026-09-15-serial-frontier-v2-controller/manifest.json) preserves stage commands, environment, source/binary hashes, scope observations, drivers and the final summary. Drivers run sequentially through `scripts/claude-safe-build.sh`, repository TMPDIR, local protoc/NVRTC, the existing Lance Python environment and affinity0–15. No source edits occur during compilation or frozen measurements. Failed gates remain in their original records.

1. Commit and push this intermediate checkpoint with all failed gates preserved, and verify the exact remote revision.
2. Use the frozen candidate for a fresh owned-child stack/CPU investigation of the large aggregate ingestion phases; stopped-thread stack counts alone are not CPU percentages. Tie any optimization to measured shared work and typed regression coverage.
3. Implement the [proposed admitted native IPC reader](native-admitted-ipc-design-2026-09-15.md) only after auditing actual block/projection working sets, metadata allocations, retained mappings, cancellation and progress. Do not force queue concurrency ahead of admission proof.
4. Preserve the remaining1/4/16GiB, shared-budget concurrency, larger-scale and timing-precision gates. Repeat the complete provider/residency cycle for the next finished candidate. Overall DuckDB leadership remains unproven.
