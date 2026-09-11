# Frozen3ff868c7 canonical SF10 provider screen — September 8, 2026

All runs use the same source-verified491-input release, strict corrected harness,
canonical SF10 data/SQL,16 threads,4GiB query and12GiB process caps. Raw68263 and
sequential provider76528 both terminate with exit1; each track has one session and
three requested measured samples/query under48GiB containment. This is development
evidence, not multi-session certification.

| Track | Valid measured pairs | Completed warmups validated | Invalid measured slots |
|---|---:|---:|---|
| Raw Parquet |57/66|20|Q9/Q13 warmup timeouts; Q12 late warmup|
| Native |60/66|20|Q1/Q13 warmup timeouts|
| Iceberg |61/66|21|Q9 reference refusal; Q19 invalid reference calibration|
| Lance |57/66|19|Q1/Q13 warmup timeouts; Q9 reference crash|

All235 completed measured engine outputs validate and pass their query-time gate.
All80 completed saved warmup outputs independently validate; raw Q12's late
warmup remains a timing failure.315 completed engine outputs therefore have typed
correctness evidence. The29 remaining measured requests are explicitly not_run;
they are not29 independent engine crashes/timeouts. No full-suite ratio or
leadership claim can be computed from these incomplete screens.

Iceberg Q9 completes its first paired sample; its second measured reference
request returns DuckDB OutOfMemoryException for a262144-byte block (bad allocation).
The reference later SIGSEGVs on Q19 EXPLAIN; engine Q19 never executes because
calibration is invalid. This observed sequence does not establish the crash's
root cause. Lance Q9's reference aborts with SIGABRT during reference warmup;
its three calibration requests encounter the already-dead worker. Engine Q9
never executes. Preserve reference failures independently of engine correctness.
The Lance reference retains its pinned extension and documented optimizer-disabled
policy from the provider manifest; this is not stock extension-pushdown performance.

The shared provider cgroup peak is24904105984bytes; its per-track snapshots are
cumulative, not separate query peaks. Raw scope peak5511491584bytes. All scope
memory.events max/oom/oom_kill are zero. DuckDB's allocation refusal/crashes are
still failures despite the absence of cgroup OOM events. Saved-output postprocessing
used a small capped scope and issued no new queries.

Evidence: raw is immutable under docs/benchmarks/2026-09-08-query-gates-raw/;
providers under docs/benchmarks/2026-09-08-budget-providers/. They contain original
traces, samples, plans, output files, setup/manifests, provider provenance,
source/harness hashes, lifecycles, scope events and supplemental warmup validation.
Interrupted temporary payloads are inventoried and excluded from archives.
No heavy job remains active at this checkpoint.

Next: retire a failed reference process at first measured reference failure
before reusing it for another SQL query, with a request/lifecycle regression.
This does not repair old timings. Then run the prepared run_budget_residency.py:
canonical decoded IPC, same-binary GPU CPU control and mixed GPU mode at16GiB
query/32GiB process (initially assumed sufficient for preload), separately followed
by unchanged custom float smoke control/required-GPU tests at4GiB/8GiB. Required
GPU results must prove device dispatch; custom float results are not canonical
SF10. Keep the4GiB preload refusal and all wider resource/concurrency/public-
workload/leadership gates open rather than treating a larger budget as a repair.

Follow-up: reference retirement is implemented and tested. The residency driver
is terminal; this canonical input requested28.6GB and refused at16GiB, disproving
the assumption above. Neither GPU case executed because CPU controls failed.
See [residency evidence](budget-quantum-residency-2026-09-08.md).
