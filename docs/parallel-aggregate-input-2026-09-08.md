# Bounded parallel aggregate input candidate

Implemented an owned input frontier for the live spillable aggregate, addressing
the [reproduced serial source polling](serial-aggregate-input-2026-09-08.md).
The candidate changes how partition streams are pulled, not SQL expressions,
group ownership, partial-state merging or the query memory limit.

`physical/morsel_agg/input_frontier.rs` consumes prepared streams exactly once.
An explicit copied-output bound allows at most the lesser of declared partitions
and Rayon workers to run independently on Tokio tasks. Slot count is reduced
until its entire output envelope can be admitted. Unknown output or insufficient
parallel admission retains the original serial opening/SelectAll behavior.
An unknown prepared descriptor is never replaced with an unprepared capability
or a second execution of its source.

Each task owns one partition stream and one pull. It verifies the actual copied
output charge against the declared bound before using the existing owned-batch
copy helper. A completed batch retains the shared envelope and its demand permit
through consumer processing. Retaining a batch prevents reusing that permit.
Streams are recycled after handoff; later partitions use slots freed by EOF.
There is no unbounded producer queue or partition replay.

Metadata and envelope owners follow running tasks. An execution error shuts down
and joins tasks, clears pending/serial streams and then propagates the original
error. Dropping a cancelled execution aborts owned JoinSet tasks; already-running
synchronous source polls retain their owners until they return. Immediate
preemption of arbitrary synchronous decoder work is not promised.

These capabilities bound copied output, not arbitrary decoder scratch, source
residency or all Arrow/Tokio allocator metadata. Provider construction remains
subject to its existing contract. This does not certify query-wide decoder
memory safety. The frontier metadata allowance follows the earlier conservative
partition allowance; it is not exact RSS accounting.

With `QE_AGG_PROF` enabled, an `aggregate-frontier` record reports admitted slots,
partition count and copied-output envelope. It describes the selected capacity;
actual CPU overlap must still be measured. Other profile flags remain unchanged.

## Validation checkpoint

All commands use `.scratch` as TMPDIR and the memory wrapper, Rayon4,48GiB scope,
one build job, `--locked --features lance,gpu`.

- The original overlap regression is now green; exact duplicate/NULL sums and
  single execution of each partition remain unchanged.
- The first affected integration gate passes24 tests with no skips.
- Expanded gate passes38 tests with no skips. New cases cover two admitted slots
  under16MiB, inability to admit two slots, prepared unknown overriding an ordinary
  known capability, unknown ordinary input, and complete pool release after EOF.
- A further source-prefix failure case now checks both ordinary known and
  prepared two-slot routes for error classification, no replay, stopped source
  polls and zero retained query-pool bytes at error return.
- Final gate completed with exit0 (handle16697):896 library passes with10
  documented ignores;39 integration passes without skips. The ignores remain
  eight dedicated CUDA cases, one dedicated IPC case and a pre-existing
  dependent-join case. Log:
  `.scratch/join-stream-profile/parallel-input-library-gate.log`.

The affected integration targets are `fused_input_cpu_parallelism`,
`fused_aggregate_input_errors`, `fused_aggregate_budget_transition`,
`live_dictionary_input`, `systemic_numeric_tests`, `aggregate_encoding_contract`
and `streaming_prepared_join_contract`. These include real partial-state spill,
wide decimals, weighted AVG, dictionaries, pending peers and source panics.
Formatting and whitespace checks pass.

Source is frozen at282 Rust/Cargo hashes in
`.scratch/parallel-aggregate-input/source-hashes.json`. Optimized build26752 completed with exit0 in8m41s under48GiB containment, with log
`.scratch/parallel-aggregate-input/release-build.log`. All282 source hashes were verified before freezing the executable, SHA256
b03f53d332c7d464070d9aac0ebb3cb060bf26a2a71ae0786c6ef3f5fe4fb999.
The balanced comparison is now running; compare
the five unresolved raw queries plus protected Q20 against the immutable join
profiling/control executable with timers disabled. Verify actual frontier
capacity and CPU use separately. Required provider, memory and concurrency
gates remain open; prior timeout records are preserved.

Prepared drivers in `.scratch/parallel-aggregate-input/`: `run_profile.py` records
frontier/queue/join/aggregate phases and process CPU for six queries.
`run_paired.py` uses four alternating binary orders, one warmup and two steady
samples per process with timers disabled. Both preserve typed oracles and dataset
checks under the same16 threads/CPUs0–15 and4/12/32GiB budgets.

## Optimized measurement checkpoint

The six-query phase diagnostic completed with exit0 (handle88024),12/12 typed
outputs and dataset checks before/after. Q5/Q10/Q20 each admit16 input slots;
the other three retain one slot because their copied-output bound is unknown.

| Query | Instrumented query ms, two requests | Admitted input slots | Whole-process CPU utilization |
|---|---|---:|---:|
| Q5 |479.8 /461.9|16|926%|
| Q9 |6183.5 /6044.2|1|124%|
| Q10 |1353.3 /1292.7|16|306%|
| Q12 |1345.0 /1318.8|1|102%|
| Q13 |4704.6 /4757.0|1|303%|
| Q20 |260.5 /247.9|16|800%|

CPU percentages include startup and serialization across two requests;100% is
one core. The profiler control measured120% for Q5 and134% for Q10. This verifies
real execution overlap, not merely a higher configured slot count. All runs use
the same16 threads and CPU affinity. Timed phases remain diagnostic wall intervals.

The resource tradeoff is visible: Q5 process CPU work rises from7.04 to11.82s
across its two requests, while peak RSS rises from713,780 to1,024,040KiB. Q10
peak RSS rises from1,040,676 to1,262,048KiB. These instrumented process metrics
are not exact query-pool accounting and do not substitute for resource or
concurrent-throughput gates. No spill or allocation safety rule was relaxed.

The uninstrumented paired run completed with exit0 (handle51723),144/144 typed
outputs and dataset integrity checks before/after. Each query has four alternating
order blocks,8 steady samples and4 warmups per binary. Results:

| Query | Control median ms | Candidate median ms | Candidate/control |
|---|---:|---:|---:|
| Q5 |2752.647|456.898|0.165985|
| Q9 |6020.527|6020.116|0.999932|
| Q10 |3017.040|1292.768|0.428489|
| Q12 |1322.540|1323.666|1.000852|
| Q13 |4784.164|4693.139|0.980974|
| Q20 |1613.123|245.668|0.152293|

Q5/Q10/Q20 improve83.4%/57.2%/84.8%, with every order block agreeing.
Q9/Q12 are effectively unchanged. Q13 is1.9% lower across this run; that small
change does not establish broad acceptance. All block medians and raw outputs
are in `.scratch/parallel-aggregate-input/sf10-paired/`.

Full raw SF10 completed with exit1 (handle15233):57/66 pairs pass typed
correctness and fresh timing gates, versus51/66 for the scheduling control.
Q5/Q10 now pass. Q9/Q12/Q13 each time out once, followed by two unavailable-worker
records. No other query adds a failure. Results are in
`.scratch/public-bench/parallel-input-sf10-raw-01/`.

Q5 measures463.882/457.846/464.062ms beneath its2261.579ms ceiling; Q10 measures
1274.918/1373.542/1356.383ms beneath2292.333ms; Q20 measures275.254/293.983/245.326ms
beneath1644.640ms. Deadlines come from fresh matched DuckDB calibration. This
remains an incomplete full suite with no valid suite score or leadership claim.

Native SF10 completed with exit1 (handle7201):60/66 valid pairs, no new failures.
Q1/Q13 each time out, then report two unavailable-worker records. Evidence is in
`.scratch/public-bench/parallel-input-sf10-native-01/`.

Iceberg completed with exit0 (handle33717),66/66 typed and time-valid pairs at
`.scratch/public-bench/parallel-input-sf10-iceberg-01/`. Suite sum of medians is
21,125.362ms versus DuckDB33,863.378ms: ratio0.623841, geometric mean0.389071,
18/22 query wins. Worst query Q13 remains2.198947× DuckDB. This is one development
session, not the required three-session certification or a leadership pass.

Lance completed with exit1 (handle51407) at
`.scratch/public-bench/parallel-input-sf10-lance-01/`:58/66 valid pairs,60 completed
engine outputs. Q1/Q13 each time out, followed by two unavailable-worker records.
The reference refuses Q9 sample1 (128MiB allocation) and crashes on sample3;
sample2 validates. No suite score is valid. All three engine Q9 outputs were
independently checked against the preserved raw typed oracle: all pass. This
raises independently correct engine outputs to60, but does not repair the
failed paired samples. The pinned extension
still has its optimizer disabled because of the previously reproduced decimal
AVG bug; do not interpret this as stock pushdown performance.

Both runs use the same frozen binary,16 threads/CPUs0–15,4GiB query budget,
12GiB process cap and32GiB scope. Other residency and concurrency gates remain open.

Two additional tests in `tests/parallel_input_spill_contract.rs` pass (handle90408,
exit0). Each exercises two actually admitted input slots,2,048 rows across six
batches and512 groups, including NULL keys and duplicates. Each spills346,848
bytes and checks independently computed COUNT, exact decimal SUM, weighted AVG,
MIN/MAX and allowed selected values. Every source partition executes once;
query-pool usage returns to zero after output release. A32MiB pool and8KiB
operator limit force spill, but do not certify whole-query pressure. This new
test file was added after the282-file binary freeze; the frozen inputs remain
unchanged. Logs are `.scratch/parallel-aggregate-input/parallel-spill.*`.

A separate sequential resource diagnostic (handle36181, terminal0) compares the frozen
control/candidate across six queries at1/4/16GiB query budgets,4/12/24GiB process
caps and a32GiB scope. It preserves typed comparisons, process RSS, errors,
remaining temporary files and cgroup events. It is not a fresh DuckDB timing or
concurrent-throughput certification. An initial driver attempt stopped before
engine execution because of a telemetry helper signature error; that log is
preserved and the corrected run uses a new output directory. All36 processes
were attempted;70/72 requested outputs completed and passed typed validation.
Q12 under1GiB query/4GiB process limits completes its first request in158.625s
(control) and158.693s (candidate), then reaches the180s process watchdog during
the second request. All other cases complete twice. At4/12GiB and16/24GiB,
Q12 takes about1.3s on both binaries. This is a pre-existing resource-dependent
latency cliff, not an input-frontier regression or a successful resource gate.

Dataset integrity verifies before/after. Cgroup OOM, OOM-kill and max events stay
zero; cgroup peak is2,390,794,240 bytes. No completed process reports a named
memory refusal. Query-pool telemetry still omits substantial process memory:
at1GiB, Q9/Q13 process RSS exceeds the query budget while recorded reservations
remain below it. This does not certify exhaustive query-wide accounting.
The timeout kills the timing parent too, so final RSS is unavailable for those
two cases; do not substitute zero. Full evidence is in
`.scratch/parallel-aggregate-input/sf10-resource-matrix-02/`.

Source inspection identifies a relevant general policy in
`physical/planner.rs::parquet_scan_batch_policy`: for unsupported variable-width
output, a whole-table estimate above the query budget selects one row per reader
batch. The ordinary plan text does not expose this quantum. A separate traced
two-case diagnostic (handle6280, terminal0) varies query budget and process cap
independently:159.058s at1GiB query/12GiB process versus1.367s at4GiB query/4GiB
process, both typed-correct. The same310,803 build rows arrive in310,803 batches
versus458. This confirms the query-budget trigger. The scan policy remains
unchanged. See the [source diagnosis and implementation sequence](scan-budget-batching-cliff-2026-09-08.md).
These traced results remain separate from uninstrumented acceptance measurements.

## Remaining capability boundaries

Source inspection explains the three serial cases without treating fixture
values as proofs. Q9's computed projection includes YEAR extraction and decimal
arithmetic; ProjectExec preparation and copied-output propagation currently
support column aliases only. Q12 includes variable-width scan payloads, while
StreamingParquetScan's explicit copy/gather capability is fixed-width only.
Q13's LEFT join is outside HashJoinExec's prepared inner-stream capability.
These are general operator families to extend, not query-number exceptions.

Do not advertise a known envelope just because a final result has a fixed type.
Computed projections must cover intermediate expression allocations and their
query-pool dependencies. Variable-width scans need validated copied extents and
a separate decoder-scratch contract. Outer joins need bounded resumable output
with correct unmatched-row and cancellation semantics before parallel admission.
