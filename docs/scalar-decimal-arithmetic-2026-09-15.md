# Scalar operands in decimal arithmetic — 2026-09-15

Current state: shared scalar arithmetic candidate `0857a768` passes the full test-inventory comparison and provider SF10 has completed with preserved failures. Paired native Q1 is 3–5% faster in both diagnostic blocks; full performance acceptance is not established. All five residency cases and independent audits are complete. The frozen parent is `be164253`, pushed as `46858df8`. Its nativeQ1 stack profile has six top ReservedVec frames whose callers are numeric arithmetic, plus reserved casts. This motivates investigating literal materialization; it does not prove the fraction of query time that a change can remove.

Unlike comparison operands, arithmetic literals are expanded to batch-sized arrays before numeric coercion. A4,096-row nullable Decimal128 expression with an Int64 literal requires a32,768-byte literal payload and a65,536-byte decimal coercion payload before its output. The focused output-sized workspace test refuses requesting66,048bytes with33,280used in a74,240-byte pool. This is an unnecessary allocation target, not an unsafe refusal: the existing engine refuses cleanly.

Test83395 completes101 with2passes/1expected failure. The passing tests preserve empty/all-NULL behavior and late overflow cleanup, including a scale38 case where eager scalar rescaling would overflow even though NULL/empty rows must do no value work. The allocation test covers both operand positions, subtraction/multiplication, a non-byte-aligned NULL slice, duplicates/negative values, ordinary and aggregate evaluators, exact decimal coefficient/type expectations and escaped output ownership; later cases remain unexecuted until the first refusal is fixed. Do not report them as covered by the red run.

[Red source/log archive](benchmarks/2026-09-15-scalar-decimal-allocation-red/manifest.json) verifies803inputs, the test-only module addition and exact failure. The48GiB wrapper was used; no terminal cgroup counters were captured for this focused run, so it is not resource acceptance.

## Implementation constraints

Preserve a literal's scalar representation through numeric coercion and the admitted decimal output kernel instead of expanding/coercingNidentical operands. Apply the shared mechanism to ordinary and aggregate expression evaluation, with explicit output cardinality and checked operand shape. Keep numeric coercion/result metadata centralized. This must work for data/SQL beyond Q1; do not recognize query text, table names or particular constants.

Empty batches and NULL pairs must not acquire eager value errors. Arithmetic overflow, precision checks, scale-domain checks and output admission remain. Reserve output/validity/metadata before writing, and retain owners through extracted buffers/slices. Evaluate nonliteral expressions once; preserve errors and avoid replay or unchecked fallback. Unsupported types/operators need the existing correct path, without materializing intermediates merely to choose that path. Batch-sized results remain required even when both operands are scalar.

Scale multipliers are already outside the decimal fill loop, and precision bounds already use a static table. This work must address representation/materialization, not repeat an already-applied metadata cache. Add typed equivalence coverage against independent Arrow/DuckDB expectations, including both operand orders, all exact operators, multiple batches, nulls, empties, real overflow/refusal, output retention and normal/specialized paths. Then use a frozen matched benchmark to determine whether the measured query benefits. Full provider/residency SF10, preserved failures and an intermediate push remain required for the completed candidate.


## Implemented candidate and validation — 2026-09-16

The shared `filter/scalar_arithmetic.rs` capability check accepts exact arithmetic trees before evaluating them, with no temporary type-inference arrays or statistics. Literal aliases are supported; casts/functions/CASE/dictionaries/floating inputs and pool-free evaluation keep the existing path. A chosen CASE branch can use the route on its selected batch. Nonliteral expressions are evaluated once, in order, and existing aggregate-root reuse remains.

Centralized numeric operand coercion now has an explicit scalar entry point bound to the supplied query pool. It validates scalar/array extents and delegates to the existing pre-admitted decimal kernel. One-element literals remain scalar during coercion, while the result always has the current batch cardinality. Empty input uses empty operands; scaling and checked arithmetic remain inside the valid-row path, avoiding eager overflow for NULLs. Array/array arithmetic retains the same kernel with both flags false.

Focused35012 passes the original three scalar regressions, filter, admitted decimal and numeric integrations. Expanded4790 also passes independent Arrow comparisons for all four exact operators and both operand orders, nested prior-root reuse, both-scalar cardinality, true output refusal and modulo-zero cleanup. Its scopepeak5,196,529,664bytes remains under48GiB with zeroOOM/max and swap0. A seventh test adds signed/unsigned literals and negative scales; the explicit pool binding is also included in full cycle77964. Source inventory is804. The source is frozen for canonical providers, paired native attribution and all residency modes.


The final both-mode validation completes with1149library passes/11ignored and128contracts per mode. Native/IPC is64default passes and63partial plus the known numeric failure; spill/numeric is28passes plus the same6failures each. Strict executable/count/name comparison against46858df8 passes with seven new library tests and no retired test. Scopepeak29,882,253,312bytes stays under48GiB,zeroOOM/max and swap0. Release compilation completed successfully; post95938 completes provider audits, paired attribution and residency. The804-input source remains frozen.

## Local engine comparison

The pinned local DuckDB source at1c27c54f uses explicit constant/constant, flat/constant, constant/flat and flat/flat branches in `src/include/duckdb/common/vector_operations/binary_executor.hpp`. Constant/constant produces a constant vector; the flat branches carry scalar-side template flags. Local ClickHouse ata1b25f3 uses `src/Columns/ColumnConst.h` to retain one physical element with a separate logical cardinality. File hashes and exact revisions are recorded in `scalar-decimal-local-engine-sources.json` for the controller archive.

These are source-level examples of preserving representation through execution, not evidence of a particular speedup here or a license to copy their semantic/error behavior. The current candidate preserves singleton operands only inside admitted exact arithmetic and still materializes the required batch-sized result. Broader constant propagation across expression results would require an explicit internal scalar/vector result contract and retained admission owners across all consumers; estimates or observed single-valued columns cannot prove constancy. Measure this candidate before expanding that scope.


Release77964 completes0 in8m51s and freezes `0857a7685cffe22b02aca4dc149b7e5a479b18c1a4adc1d4fe0f6a738aa02c2a` from804verified source inputs. Cumulative validation/build peak remains29,882,253,312bytes under48GiB,zeroOOM/max and swap0. The39-file validation archive verifies. Provider SF10, paired diagnostics and all residency audits are complete.

## Provider SF10 and paired attribution

The independent audit validates339engine outputs and252of264measured pairs: raw66,
native57, Iceberg63 and Lance66. Raw completes22queries at geomean2.427727 and
suite2.616827 versus DuckDB, with0wins. Lance completes22queries at1.973992 and
2.910767, with1win. Ratios are engine/DuckDB; lower is better. These are current
baselines, not paired proof of improvement over the preceding checkpoint.

Native Q1 warmup times out. Native Q12 warmup is typed-correct but takes1268.585ms
against1209.057ms; Q17 measured1 is typed-correct but takes962.336ms against950.768ms.
Their dependent requests are not run. Iceberg DuckDB Q13 warmup refuses a262144-byte
allocation, invalidating calibration and leaving its three comparisons NOTRUN.
These remain failed gates. Cumulative validation/build/provider scopepeak is
30,425,083,904bytes under48GiB, with zeroOOM/max and swap0.

The two reversed native diagnostic blocks validate all80outputs, with identical
optimized and physical plans. They use aggregate telemetry and a separate180s
diagnostic deadline; they cannot clear the canonical query ceilings.

| Query | Candidate/parent block1 | Candidate/parent block2 |
|---|---:|---:|
| Q1 | 0.953476 | 0.969844 |
| Q6 | 0.912140 | 0.644041 |
| Q12 | 1.050877 | 0.664454 |
| Q18 | 0.989140 | 0.981202 |
| Q9 | 1.005279 | 1.013808 |

Q1 expression evaluation falls from1416.703/1402.543ms to1148.294/1144.833ms.
Its ingestion remains4631–4729ms across these cases. This supports the intended
reduction in repeated operand construction, but most elapsed work remains in
ingestion. Phase elapsed values are not CPU attribution. Q6 varies substantially,
Q12 changes direction, and Q9 is slightly slower in both blocks. Do not claim
blanket regression freedom or attribute every change to scalar arithmetic.

## Residency and completed checkpoint

All five cases complete and independently validate348outputs/278measured pairs.
The canonical decoded/GPU cases use32GiB query budgets and48GiB process caps with
preload outside query timing. They do not clear16GiB preload or complete resource/
concurrency acceptance. Ratios below compare the matched DuckDB boundary.

| Mode | Geometric mean | Suite ratio | Wins |
|---|---:|---:|---:|
| canonical_decoded_ipc | 0.818134 | 1.277217 | 14/22 |
| canonical_gpu_control | 0.798882 | 1.265608 | 14/22 |
| canonical_gpu_mixed | 0.795011 | 1.262422 | 14/22 |
| smoke_gpu_control | 1.213511 | 2.005701 | 1/2 |
| smoke_gpu_required | 0.088420 | 0.086790 | 2/2 |

Canonical mixed records88completed outputs but zero successful device execution.
The separate custom float required-GPU case validates40/40measured device proofs,
plus two warmups, with no per-request uploads, new fallback or upload failures.
Its speedup is not canonical SF10 GPU acceleration. Postcheck scopepeak is
21,508,247,552bytes under64GiB,zeroOOM/max,swap0.

Reproduction commands, exact binary/source/harness hashes, raw samples, independent
comparisons and all failures are preserved in the cycle archives:
[validation](benchmarks/2026-09-16-scalar-decimal-validation/manifest.json),
[provider SF10](benchmarks/2026-09-16-scalar-decimal-sf10/manifest.json),
[paired attribution](benchmarks/2026-09-16-scalar-decimal-attribution/manifest.json),
[residency](benchmarks/2026-09-16-scalar-decimal-residency/manifest.json), and
[controller](benchmarks/2026-09-16-scalar-decimal-controller/manifest.json).
The initial red and owned-child profile remain linked above and in the
[profile report](native-aggregate-profile-2026-09-15.md).

## Next cycle

Retain the shared representation fix and its semantic/admission tests. The next
bounded step is a verified inventory of native IPC record/dictionary block sizes
and page-rounded mapping windows, followed by projection footprint and decoder
allocation accounting. The native reader still lacks prepared admission; do not
force parallel readers or treat whole-file mmap as admitted memory. Implement the
[existing native-reader design](native-admitted-ipc-design-2026-09-15.md) only with
preallocation, retained ownership, cancellation and progress proofs. Q1 ingestion
also remains a larger measured phase than expression evaluation; further CPU work
needs fresh attribution. Keep Q9's small paired slowdown, Q12 variability and all
canonical failed gates visible. Overall DuckDB leadership remains unachieved.
