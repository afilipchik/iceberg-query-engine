# Current canonical SF10 provider validation

Latest bounded-input candidate: raw57/66 pairs pass, restoring Q5/Q10 deadlines;
Q9/Q12/Q13 still time out. Balanced Q5/Q10/Q20 latency improves83.4%/57.2%/84.8%
against the preceding frozen CPU control. Native validates60/66 pairs, with
Q1/Q13 deadlines still missed. Iceberg validates66/66 pairs, suite0.623841×
DuckDB and18/22 wins, but worst Q13 remains2.198947×. Lance has58/66 valid pairs,
with Q1/Q13 engine deadlines and Q9 reference refusal/crash. No leadership pass
is available. [Current evidence](parallel-aggregate-input-2026-09-08.md).

Earlier scheduling follow-up: raw51/66 and native60/66 pairs pass correctness
and timing, with the original deadline failures. Q20 recovers its raw deadline
and the balanced Q13 gain is34.7%; Q10 is1.016× serial. Iceberg validates66/66
pairs with suite ratio0.878601, geomean0.576103 and12/22 wins. Q13 remains
2.18724× slower; this is one session, not leadership. Lance has57/66 valid
pairs: engine Q1/Q13 deadlines and reference Q9 allocation refusals.
The current frozen release also cleanly refuses the oversized decoded-IPC
preload before decoding; no IPC query performance result is available.
[Current candidate evidence](aggregate-batch-dispatch-2026-09-08.md).
These are separate frozen-binary results; the older provider matrix below remains
historical evidence.

Earlier parallel-state candidate: full raw SF10 records51 typed completed pairs,
but only48 also pass timing. The same five queries time out and Q20 newly exceeds
its deadline. Balanced checks show Q13 improves31.6%, Q10 is1.0041× control and
Q20 regresses4.4%. This candidate is not accepted and does not replace the frozen
provider table below. [Exact results and phase evidence](parallel-aggregate-candidate-2026-09-08.md).

## Consolidated frozen-binary status, 2026-09-08

| Track | Correct completed engine outputs | Valid measured pairs | Blocking outcome |
|---|---:|---:|---|
| Raw Parquet |51/66|51/66|Engine deadlines:Q5/Q9/Q10/Q12/Q13|
| Native |60/66|60/66|Engine deadlines:Q1/Q13|
| Iceberg |66/66|63/66|Q13 reference allocation refusal, then signal11|
| Lance |60/66|58/66|Engine Q1/Q13 deadlines; reference Q9 allocation refusals|
| Decoded IPC |0|0/42 recorded|Interrupted after repeated setup allocator aborts|
| GPU CPU control |51/66|51/66|Same five deadline failures as raw|
| GPU-assisted |Not executed|Not executed|Harness rejects incomplete CPU control|

There is **no valid full-suite performance score** for any completed paired
track in this4GiB/12GiB resource configuration. IPC was intentionally stopped,
and GPU preflight exited2 with `GPU CPU control must complete correctly`.
That is a benchmark evidence prerequisite, not a permission/approval rejection.
No GPU kernel execution or residency coverage is claimed from this run.

The UTF8-output candidate now measures9.0% lower Q10 latency in a balanced
32-output comparison, but its normal-deadline raw rerun still validates51/66
pairs and misses the same five query deadlines. All provider results above use
the preserved pre-candidate binary. A later debug IPC preparation guard replaces
the original setup allocator abort with a named pool refusal; that is separate
from completed residency coverage. See [the follow-up](ipc-preload-admission-2026-09-08.md).

2026-09-08 Lance and residency follow-up:

-Lance:58 valid paired samples. Engine Q1/Q13 time out; the two later samples
  for each report unavailable workers. DuckDB reference Q9 samples2/3 refuse
  allocation of128MiB blocks. Both corresponding engine outputs independently
  match the preserved oracle, giving60 correct completed engine outputs. No
  valid full-suite score.
-Decoded IPC:stopped after42 sample records across14 queries, zero valid pairs.
  Setup repeatedly aborts the engine allocator under the12GiB process cap
  (signal6; the preserved stderr reports an11,322,760-byte allocation failure).
  The adapter calls StreamReader::collect for every table without preload
  admission. This is a benchmark setup resource-safety failure, not a query
  timeout or clean refusal. The task-owned supervisor was interrupted; its
  recorded workers were verified gone and the tool returned130. This is an
  incomplete, intentionally interrupted run, not22-query coverage.
-The GPU same-binary CPU control is running. Ordinary GPU/control setup registers
  raw Parquet; decoded IPC and explicit host-Arrow preload share the unsafe
  collection path above. GPU residency validation therefore remains open.

The IPC bug must be addressed through bounded preparation/admission or an
explicit, correctly accounted mapped-residency contract. Increasing a cap alone
would not close the unbounded setup contract. The current query-latency deadline
does not protect work done before the worker announces readiness.

A file-size audit records13.256GiB of serialized IPC inputs, of which lineitem
is9.654GiB. The process cap is12GiB and the context pool is4GiB. File sizes are
not decoded-allocation or RSS measurements, but they make the unconditional
full-residency assumption untenable for this setup. ExecutionContext exposes a
shared, process-parented memory pool; the adapter's StreamReader collection does
not use it. Admission must precede decode allocations and retained input owners
must keep their charge. A post-decode size check or a file-size estimate alone
would not prove that arbitrary IPC setup is safe. The audited sizes are archived
under `decoded_ipc/preload-size-audit.json`.

2026-09-08 Iceberg follow-up: all66 measured engine outputs complete and pass
typed validation, including three Q13 outputs rechecked directly against the
preserved oracle. Only63 paired measurements are valid. The DuckDB Iceberg
reference path reports an allocation refusal on Q13's first measured request,
crashes with signal11 on the second, then reports worker unavailable. The oracle,
warmup and calibration had completed in that same reference worker. No result
mismatch is demonstrated, and this is not an engine timeout. It remains a failed
paired suite with no valid performance score. The exact refusal/crash is archived
in `iceberg/execution.jsonl`; independent engine checks are adjacent.

Lance validation is now running with the same frozen binary and4GiB settings.
Its pinned reference disables the Lance extension optimizer because the recorded
extension version truncates decimal AVG during pushdown. This measures direct
Lance scanning with DuckDB aggregation, not stock extension pushdown performance.

The frozen release candidate containing the dictionary repair and general outer
ON pushdown completes two fresh canonical SF10 tracks with failures. Neither
track has a valid suite performance score.

| Track | Fully validated queries | Typed measured pairs | Deadline failures |
|---|---:|---:|---|
| Raw Parquet |17/22|51/66|Q5, Q9, Q10, Q12, Q13|
| Native |20/22|60/66|Q1, Q13|

Every listed query first exceeds its matched DuckDB deadline. The watchdog kills
that query's worker; the remaining two samples report worker unavailable with
exit-9. These are timeout consequences, not independent startup or OOM findings.
Each successful query has three validated measured pairs. No typed mismatch was
observed among completed measured pairs; missing outputs are not correctness passes.

## Conditions and reproduction

Binary `.scratch/live-schema-boundary/outer_on_benchmark_embedded`,
features `lance,gpu`,660 source inputs based on HEAD88849c4. Both tracks use
canonical dataset `.scratch/public-bench/tpch-sf10/dataset.json`, DuckDB1.4.4,
16 threads, CPUs0–15,4GiB query memory,12GiB process cap,32GiB cgroup, three
samples and one session. GPU is not enabled merely by compilation features.
Native uses `canonical-sf10-providers-02/native/provider.json` and the harness's
provider provenance checks. Full artifacts remain in
`.scratch/public-bench/outer-on-sf10-{raw,native}-01/`.

Commands follow the versioned `python -m benchmark run` harness through
`scripts/claude-safe-build.sh`; exact settings, samples, execution events,
reports, binary/source hashes and logs are [archived](benchmarks/2026-09-07-current-sf10/manifest.json).
These4GiB runs are not directly interchangeable with the older frozen40GiB
provider comparison. This evidence does not attribute every failure to the most
recent optimizer or spill integration changes.

## Raw timeout diagnosis

Each of the five raw failures was rerun twice under the same resources with
existing aggregate/input-queue traces and a120s diagnostic process watchdog.
All ten completed outputs pass the independent typed ordered/LIMIT oracle.
Longer diagnostic watchdogs do not convert the original deadline failures to passes.

| Query | Diagnostic query ms range | Notable aggregate work |
|---|---:|---|
| Q5 |2738–2741|About11ms ingestion; output negligible|
| Q9 |5941–6118|See preserved full phase trace|
| Q10 |3238–3282|555–563ms ingestion;477–480ms output construction|
| Q12 |1307–1316|About46ms ingestion; output negligible|
| Q13 |7332–7375|Inner ingestion over4s;1.5million output groups|

Wall intervals can overlap across operators and include diagnostic overhead;
they are not exclusive CPU totals. No aggregate spill occurs in the displayed
Q5/Q10/Q12/Q13 phase records. The evidence requires separate scan/join investigation
for Q5/Q12 and state/key ingestion investigation for Q13. It also identifies a
substantial shared output cost in Q10: UTF8 canonical keys are decoded into
individually owned scalars before Arrow copying.

A candidate now borrows validated UTF8 key fields and writes them once into
reservation-owned Arrow buffers, preserving output ownership and refusal cleanup.
It compiles and all111 aggregate component tests pass, including a new admission
sweep and retained Arrow ownership test; broader regressions are running.
Neither benchmark binary includes that candidate.
No output speedup is claimed. Iceberg, Lance, decoded IPC and GPU residency
reruns remain open, as do wider memory/concurrency and workload acceptance gates.
