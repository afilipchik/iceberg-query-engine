# Historical implementation checkpoints

Preserved from epic.md on 2026-09-06T08:31:30.079681+00:00.
These describe earlier binaries and incomplete runs; use the execution checkpoint for current status.

Initial checkpoint — 2026-09-06 UTC: shared numeric, cast, membership and semantic-proof
fixes restored canonical correctness. The initial 2026-09-05 SF1 result of 6/22 is
historical; the initial archived canonical SF10 validates 22/22 across 660 pairs in three
sessions, with no time-gate failures/OOM events. Its suite ratio is 2.784506× DuckDB
and geometric mean 2.640705×; this is a raw-Parquet development baseline, not leadership.
The [measured source/evidence](../../../docs/benchmarks/2026-09-06-canonical-sf10/README.md)
predates later dense admission and physical-API guards. See the [contract report](../../../docs/systemic-contracts-2026-09-06.md)
for implemented semantics, admission infrastructure and outstanding ownership boundaries.

Current checkpoint: the harness has **75 passing tests, zero skips**. Real-data JOB and
ClickBench extracts preserve all 113/43 queries; complete-oracle slice comparison
closes the original eight ClickBench comparator gaps. JOB's second development
run passes 113/113 with zero issues. ClickBench's second run passes 41/43, leaving
q29's regex timeout and q43's grouped ORDER BY binding failure. Source fixes pass
that combined gate: **748 Rust tests**, one pre-existing ignored test; subsequent
dictionary validation brings the current total to **750 Rust tests**, one ignored.
The subsequent release rebuild completed. Final public development reruns now
pass all **43 ClickBench queries across 129 pairs and 113 JOB queries across
339 pairs**, with zero issues. Suite ratios are 2.234947×/1.675876× and geometric
means 1.671372×/1.421927× respectively; neither establishes leadership. The
earlier 41/43 ClickBench screen remains historical evidence. The pre-dictionary
660-pair SF10 gate completed at 2.801267× suite time. The subsequent dictionary
candidate also passes all 660 pairs at **2.763338112× suite / 2.61140432× geometric
mean**. Q1 improves from 1,057.773 to 914.626 ms; no query median regresses over
10% against its immediate control. Public JOB339/ClickBench129 pairs pass too.
See [candidate evidence](../../../docs/benchmarks/2026-09-06-dictionary-preservation/README.md).
This accepts the bounded development change, not the broader leadership task.
JOB04 quotes the incompatible alias with original SQL preserved;
ClickBench04 makes DuckDB regex options explicit identically in both engines and
oracle. See the [public workload guide](../../../scripts/benchmark/PUBLIC_WORKLOADS.md).

Current provider/GPU certification, IPC residency equivalence, complete memory ownership,
full workloads/layouts/caps/concurrency and SF100 remain open. The isolated typed
decimal-accessor change is accepted: Q1 improved 20.9% in its alternating screen;
the 660-pair full gate reached 2.711019× suite time. This earlier measured source
predates later proof/aggregate/binder/scalar fixes and does not certify their
performance. The subsequent 66-pair SF10 screen was 2.685959×, with possible Q9
regression still requiring attribution. No parent
task is closed and no leadership gate is met; the unchanged 0% field counts closed parent
tasks, not an estimate of engineering work completed.

Candidate adapter smokes pass 66 pairs each for IPC/Iceberg/Lance; native retains
eight time failures despite 66 correct answers. GPU and same-binary control pass
40 pairs each, with 39 GPU samples executing on device. Full canonical SF10
provider conversions are currently running in
`.scratch/public-bench/canonical-sf10-providers-01`; no outcomes are claimed yet.
The harness-only aligned exact comparator fast path processed a synthetic 60M-row
probe in 3.161 seconds at 267 MiB RSS. This is validation throughput, not engine
speed or full data-conversion evidence. Older Q9 attribution remains open.

