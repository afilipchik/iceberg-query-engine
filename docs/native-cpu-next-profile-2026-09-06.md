# Next native CPU profile — 2026-09-06

Read-only inspection while isolated matrix 73207 is active. No engine, benchmark, build, profiling process or source edit. Samples are a session-1 checkpoint, not a final provider verdict or a fresh accepted optimization.

## Measured facts and boundary qualification

Input: `.scratch/public-bench/spill-output-provider-gates-01/native/samples.jsonl`, session s1, ten samples/query. All 220 sampled engine executions completed and all 220 comparisons are validated at inspection. Binary SHA256 is `c4d05ab79f8f7a328d6f2601e087ed5e515e1b2b4e4cc6fc82254b8bbd2648c8`; reference DuckDB1.4.4;16threads/affinity0–15;40GiB query,48GiB process;warm_host. Native provider conversion02 is verified; preserve manifest source/binary hashes and exact SQL.

Medians in milliseconds. Ratio is median engine/median reference. Residual is the median of each individual sample's end-to-end minus its four phase timers; medians of columns are not additive.

| Query | Engine | DuckDB | Ratio | Plan | Optimize | Execute | Residual |
|---|---:|---:|---:|---:|---:|---:|---:|
| q01 | 1091.72 | 127.60 | 8.556 | 92.75 | 0.81 | 866.01 | 111.98 |
| q06 | 197.91 | 30.56 | 6.476 | 4.56 | 0.44 | 117.46 | 77.69 |
| q10 | 747.95 | 184.40 | 4.056 | 73.38 | 43.86 | 544.90 | 83.00 |
| q13 | 1032.60 | 247.62 | 4.170 | 97.79 | 0.77 | 903.07 | 27.11 |
| q14 | 182.65 | 48.99 | 3.728 | 6.70 | 1.26 | 105.78 | 69.21 |
| q15 | 176.70 | 39.54 | 4.469 | 165.36 | 1.19 | 6.34 | 0.32 |
| q16 | 461.53 | 65.81 | 7.013 | 3.35 | 0.80 | 417.39 | 5.24 |
| q18 | 1315.84 | 223.53 | 5.887 | 13.63 | 17.43 | 1211.39 | 54.43 |
| q19 | 1085.81 | 128.90 | 8.424 | 80.30 | 6.78 | 899.96 | 94.82 |
| q21 | 1561.39 | 275.73 | 5.663 | 21.48 | 5.05 | 1409.33 | 93.89 |

The benchmark timer (`examples/benchmark_embedded.rs:155`) wraps `context.sql().await` and ends before Arrow file serialization. In `context.rs:1475`, execute_time stops after result collection, and total_time is recorded immediately afterwards. Output dictionary decoding, end-of-function destruction of physical plan/provider scan batches and some intervening plan formatting lie outside individual phase timers. `plan_time` includes physical planning and any work it triggers, not just optimizer work; q15's6.34ms execute must not be read as its whole CTE/aggregate cost. Tiny returned row counts on q01/q06 make output serialization an implausible explanation of their large residual; serialization is outside the measured timer anyway. The residual is NOT established CPU or mmap cost without counters.

No same-snapshot decoded_ipc s1 samples were present at the inspected sibling path. Do not manufacture a matched IPC comparison from older binary reports. Historical post-scalar q06 evidence (docs/native-decimal-pipeline-investigation-2026-09-06.md) showed IPC16 76.5ms/1020msCPU and native16 141.9ms/1180msCPU, but those values belong to the older scalar binary. They motivate a matched diagnostic, not a current regression calculation.

## First shared candidate and competing explanations

Profile the **provider-neutral decimal expression/filter/aggregate pipeline**, with original q01 as the high-cost grouped case and original q06 as the narrow no-join control. q01 takes1091.7ms overall and866.0ms execution despite a tiny output; q06 takes197.9ms overall and117.5ms execution. Their plans share MemoryTableScan -> Filter -> Project -> SpillableHashAggregate. This is a candidate selection, not proof that a particular decimal kernel dominates.

Source mechanisms to separate:

1. FilterExec (`operators/filter.rs:113–151`) evaluates the predicate mask then applies Arrow filter separately to every retained column. Scalar comparisons already use ComparisonOperand; the old full-length comparison-literal bug is fixed. Do not re-implement that historical fix. Decimal predicates can still fall back from compiled expressions, requiring several mask passes/materialized outputs.
2. Ordinary arithmetic expressions still evaluate children as arrays (`filter.rs:241–298`), and a non-comparison literal uses `scalar_to_array`. `planner/numeric.rs:150–193` may cast operands, runs the Arrow arithmetic kernel, then scans decimal results again for precision validation. q01's `price*(1-discount)` occurs both alone and as the child of the charge expression. AggregationState evaluates each aggregate input independently (`morsel_agg.rs:1936–1944`), so repeated deterministic arithmetic is not shared by that loop. Quantify expression time before considering a shared scalar arithmetic/validated-vector path or per-batch expression reuse. Exact overflow, scale, NULL and error-order semantics remain mandatory; no float substitution or general volatile-expression CSE.
3. Generic exact aggregation (`hash_agg.rs:435–489`) creates AggregationState and processes collected batches in a loop. Grouped q01 can spend time in typed key access and accumulator updates, whereas q06's global aggregate is a discriminating control. Earlier q06 CPU evidence already ruled out serial reduction as its dominant old cost; it does not rule out grouped q01 state work today.
4. Native row-group IPC reads mmap files (`storage/ipc_cache.rs:433–469`). The physical plan holds projected batch buffers, which retain Arc<Mmap>. Final plan destruction can unmap after execute_time ends; first-touch page-table costs can occur during execution. q01/q06/q14's residuals warrant lifecycle attribution, but neither page faults nor munmap have yet been measured in this snapshot. A universal mapping cache would also require immutable identity/eviction and memory ownership, so is not a safe speculative first optimization.
5. Separate confirmed optimizer duplication: q19's saved part filter contains many copies of the same brand/container IN lists; q07 has the documented repeated nation predicate. See docs/optimizer-overhead-investigation-2026-09-06.md and DeriveOrPredicates/PredicatePushdown interaction. q19's6.78ms optimize cannot explain899.96ms execution by itself, but repeated predicate evaluation may contribute. Preserve a later q19 control if stacks point there; do not patch a query ID or suppress the original OR. q18/q21 and q13 have large independent join/group costs and are not explained away by this candidate.

## Minimal contained experiment after the matrix

Do not run during any measured workload. Adapt the existing `.scratch/diagnose_decimal_scaling_after.py` Worker/proc-counter mechanism into a NEW `.scratch/diagnose_native_cpu_next.py`; preserve its old evidence unchanged. This document is the driver specification, not a claim that this new driver exists or has run.

Use the manifest-pinned frozen binary above (resolve/preserve exact bytes before launching), unchanged original q01 and q06 SQL from canonical dataset.json, and verified conversion02/native and decoded_ipc. Four configurations only: (q01,native16), (q01,IPC16), (q06,native16), (q06,IPC16); fresh worker per configuration, one warmup +three steady attempts =16 engine requests total. Same16threads/affinity0–15,40/48GiB limits. Derive reference setups through provider_setup, not hand-written raw comparison. For each configuration use3 fresh DuckDB calibrations, original typed oracle, and unchanged10×median reference ceiling; record and stop that worker on timeout. Keep startup allowance separate.

Set QE_AGG_PROF=1, QE_GPU=0, RAYON_NUM_THREADS=16, QE_MEM_CAP=48G and the provider adapter's recorded IPC policy. Preserve the QE_AGG_PROF process/group/aggregate-expression deltas emitted by benchmark_embedded. These are cumulative worker durations, not additive wall-clock phases; process-minus-group-minus-expression approximates updates only within that instrumented function and does not cover upstream filters. Missing metrics are missing coverage, not zero cost. Use /proc/<own-worker>/stat at existing started/query_finished events to record user/systemCPU, minor/majorfaults and RSS, plus reservation peaks and phase residuals. Exclude calibration, serialization and setup from query CPU windows as far as the existing event sampling allows; document tick/poll quantization.

Invocation after driver review and scheduling:

`TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=64G SAFE_BUILD_JOBS=2 PYTHONPATH="$PWD/scripts" scripts/claude-safe-build.sh taskset -c 0-15 .scratch/venv-lance/bin/python .scratch/diagnose_native_cpu_next.py --mode cpu --output .scratch/public-bench/native-cpu-next-01`

Use the existing pinned Python environment that imports the harness (verify the interpreter path during preparation). No package install, host perf-permission change or rebuild is implied. The prior host denied perf counters: do not repeat an unprivileged-perf loop or treat counter access as an approval reason. This first diagnostic deliberately uses existing counters. Capture command/env/source/binary/setup/SQLhashes and all oracles/plans/logs in a fresh directory, separate from acceptance samples.

Decision gates:

- Large q01 aggregate-expressionCPU fraction, shared across providers: next bounded experiment targets shared typed arithmetic/materialized intermediates; preserve current precision/error checks and independent typed tests.
- Large q01 update/group fraction but small q06: profile grouped typed accumulator/key loops, not scalar reduction generally.
- Large CPU outside aggregation with good parallel CPU use: obtain focused Filter/Arrow-kernel instrumentation or stacks in a separately built diagnostic candidate before choosing fusion.
- Native-only residual/systemCPU/fault increase: instrument physical-plan drop/mmap lifetime boundaries next; do not move cleanup outside benchmark timing to improve the score.
- Low CPU/high wall: investigate queue scheduling/pool admission and ownership preparation before changing arithmetic. Previous demand serialization was fixed; require current evidence to reopen it.

A one-thread follow-up is optional only if these16requests leave scaling ambiguous; calibrate a fresh one-thread reference and retain timeouts. Do not enlarge ceilings to force a completed profile. No implementation acceptance or full-source leadership claim follows from this small diagnostic. Raw/native/IPC/Iceberg/Lance/GPU, all-query regression gates and resource correctness remain the overall objective.

## Session-2 Q21 timeout: separate completion investigation

Later matrix steering reports q21 s2 iteration8 exceeded its fresh2406.421ms ceiling. The saved sample is a primary query timeout (`worker response deadline exceeded`); iterations9/10 are `startup_error: worker unavailable`, exit -9 after the watchdog killed that worker. They are consequential unavailable attempts, not two independent slow-query measurements. Do not assign2406ms or zero to missing times, or average only successes into a passing result.

Successful q21 distributions:

| Session | Completed | Median ms | Minimum ms | Maximum ms |
|---|---:|---:|---:|---:|
| s1 | 10/10 |1561.388|1468.477|1992.674|
| s2 | 7/10 |1513.747|1439.510|1528.889|

Every available successful optimized-plan hash is `a09921afecec7d9d2bd566431f4cc349f8a15f0964649a5db1e181570046df75`; every successful physical-plan hash is `3f9d32b1a7edf578d93739151a6d161aa9604e47c4550cb734726e2d63a18b52`. The timeout has no completed plan/timing result; do not infer its runtime phase from successful samples. The matching successful plans rule out an observed plan change across those samples, not a runtime scheduler/memory event.

The plan is aggregate COUNT by supplier over ANTI(orderkey, residual supplier inequality), SEMI(orderkey, residual supplier inequality), and inner joins, with repeated lineitem scans. This is a separate shared filtered-Semi/Anti/probe/scan lifetime candidate, not primarily decimal arithmetic. Check current hash_join filtered candidate handling and spillable prepared-input decline for non-Inner before proposing any optimization; boolean existence and NULL semantics must stay exact. The first seven s2 successes do not show a gradual slowdown; a single interrupted attempt cannot establish a new steady-state regression or its cause.

Before the decimal throughput profile, preserve watchdog/execution logs and perform a bounded completion diagnostic after the whole matrix: same frozen binary/setup/original q21,16threads, fresh matched calibration and unchanged10×ceiling, one warmup plus at most3steady requests with the same own-worker process counters. Stop on the first failure; never retry to erase it. If the timeout reproduces, prioritize stage/cancellation attribution of filtered Semi/Anti and repeated scan initialization before throughput work. If it does not, retain the full-gate failure and treat the small diagnostic as non-reproduction, not closure. This adds at most4requests; it does not authorize another full matrix while the current one is running.

## Terminal native matrix qualification

Parent reports native is now terminal:660attempts and4issues. In addition to the q21 timeout and its two consequential unavailable attempts, q01 s3 iteration8 completed with valid output at1185.058417ms, exceeding its unchanged1177.904657ms ceiling. Report this as a timing failure with correct output, not a correctness failure or success rounded under the ceiling. Its phase metrics were execute954.791655ms,optimize33.866625ms,plan72.630899ms,parse0.099055ms. IPC is still running; no diagnostic execution is authorized during it. The native result remains a failed full gate regardless of small diagnostic outcomes.

Prepared driver now exists at `.scratch/diagnose_native_cpu_next.py`; only AST syntax inspection was performed. Modes: `--mode completion` allows only native q21 with at most4engine requests; `--mode cpu` allows q01/q06 times native/IPC with at most16engine requests. Each case stops at the first failed exact time gate/correctness/completion. Its Worker.read override captures counters when started/query_finished events are received, retaining5ms raw samples and delivery/tick/timeout qualifications. It loads and validates the unchanged `oracle_sql_path` (q21 complete LIMIT-ties oracle), not the arbitrary timed top100 subset. Dataset/provider full checksum checks happen outside timed work when the driver is eventually run.

Completion command (after matrix and parent scheduling):

`TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=64G SAFE_BUILD_JOBS=2 PYTHONPATH="$PWD/scripts" scripts/claude-safe-build.sh taskset -c 0-15 .scratch/venv-lance/bin/python .scratch/diagnose_native_cpu_next.py --mode completion --output .scratch/public-bench/native-completion-next-01`

The matching CPU command appears above. Both output roots must be fresh. A nonzero driver exit preserves any timeout, exact timing failure, oracle/correctness failure or incomplete case; it does not restart a killed worker or raise a ceiling.

## New independent IPC Q19 failure and prepared-Filter composition gap

Saved IPC execution.jsonl has s1 q19 warmup3152.460ms (execute3079.751,optimize56.237,plan0.611), reservedpeak37,864,144B. The first measured request timed out at2860.681ms; the next9requests were consequential unavailable-worker attempts. Session2 independently repeats this: warmup3115.575ms (execute3096.148,optimize14.082,plan0.597), same37,864,144B peak; first measured timeout then9unavailable. Neither session supplies a successful measured q19 median. Warmups are diagnostic observations, not accepted samples or evidence that the10×gate passed.

Same-binary native q19 completes all30measured attempts. Session medians (total/execute ms) are s1 1085.815/899.961, s2 1046.051/880.338, s3 1046.934/880.549. Each has median reservedpeak21,545,168B. Native and IPC share the displayed physical operator shape:

Project -> SpillableHashAggregate -> Filter -> SpillableHashJoin, with Filter->MemoryTableScan on build and Project->Filter->MemoryTableScan on probe.

Confirmed source mechanism: PhysicalOperator::prepare_queue_input defaultsNone (`physical/plan.rs:33`). FilterExec implements ordinary resident/general queue and gather propagation, but does not override preparation (`operators/filter.rs:71–103`). SpillableHashJoin offers a prepared descriptor only after its eligible Inner initialization (`spillable.rs:1163`, delegated `hash_join.rs:1486`). Ordinary join queue-bound getters remainNone, because build initialization can require the same pool. Thus a Filter over a join hides the child's preparation from an aggregate queue: its static forwarded queue bound is alsoNone. The aggregate merge helper (`spillable.rs:577–621`) consequently chooses one demand permit for that unknown path; the permit spans upstream try_next, so join probing AND downstream residual filtering are serialized across that queue's partitions. This is a confirmed capability-composition hole, not a reason to falsely declare uninitialized joins pool-independent.

**Causation qualification:** saved plans do not reveal actual partition counts/descriptor selection, and no current CPU/concurrency diagnostic has run. Both native and IPC have this wrapper shape, so the hole alone does not explain their3×warmup/execution difference. Different dictionary/plain string encodings, source batch layout, typed filter paths, join build representation and child-queue bounds can change work. Reserved peaks are whole-query peaks;21.5 versus37.9MB does not itself prove which envelope was selected. q19 repeats brand/container predicates as previously documented; runtime work from those predicates remains a competing shared explanation.

A safe future compositional fix would have Filter prepare its child BEFORE outer admission, then lazily wrap each already-prepared stream with the SAME existing predicate evaluator, preserving exactly-once initialization, no pre-pull and cancellation. Reject subqueries or other future same-pool dependencies. The present descriptor exposes only opaque `max_copy_bytes`; blindly reuse that number for filtered output is unsound because Arrow may add validity/compact buffers and physical schemas can vary. Extend preparation metadata with audited physical layout variants/row bounds sufficient to calculate Filter output (including Dictionary children, Boolean/null bitmap extents and strings), or decline when proof unavailable. Column Project has the analogous preparation gap and should be handled through the same checked composition design, including duplicate aliases and shared runtime column resolution. Tests must prove overlapping pulls, bounded queue peak, exact residual OR/NULL outputs, build-before-envelope ordering, cancellation, and mismatch refusal. No production change is made here.

The prepared diagnostic driver completion mode now includes BOTH independent failures: native q21 and decoded_ipc q19, at most8engine requests total (one warmup+three attempts each), each with its own fresh complete oracle/calibration/unchanged exact ceiling. It stops the failing worker immediately, including a warmup exceeding the fresh ceiling, and never pads nine unavailable measurements. This supersedes the earlier4request completion specification; cpu mode remains16requests. Only AST inspection was performed after this update. Do not run while matrix73207 is active.

## Iceberg exposes a second composition boundary

Completed Q19 samples (30 per snapshot) show current Iceberg median2188.269ms versus scalar455.106ms, while DuckDB medians remain1552.789/1538.433ms. Both runs pass typed comparison and10× gates; that threshold conceals a4.808× engine regression. See [exact Q19 checkpoint](benchmarks/2026-09-06-spill-output-ownership/iceberg-q19-checkpoint/README.md). Current probe is Project→StreamingParquetScan with filter-only string columns still emitted by the scan. The all-column fixed-width descriptor declines before the numeric-only Project; prepared-wrapper propagation alone cannot fix that boundary. A separate physical projection/pruning review is required, preserving decoder filter inputs and schema/alias/order contracts. Existing queued diagnostics remain unchanged and pinned; add Iceberg to subsequent candidate performance validation, not to the live run.
