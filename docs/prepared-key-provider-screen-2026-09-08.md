# Prepared-key canonical provider screen — September 8, 2026

Frozen1a0ece71 completed the four-track screen (driver69779, exit1). No track
passes full acceptance. All237 completed measured engine outputs are typed/time
valid; all80 completed warmups independently validate from saved files, including
the late raw Q12 warmup. These317 outputs do not cover the27 not_run sample slots.

| Track | Valid measured pairs | Completed warmups validated | Blocking outcomes |
|---|---:|---:|---|
|Raw Parquet|57/66|20|Q9/Q13 warmup timeouts; Q12 late completed warmup|
|Native|60/66|20|Q1/Q13 warmup timeouts|
|Iceberg|63/66|21|Q9 DuckDB calibration allocation refusal|
|Lance|57/66|19|Q1/Q13 warmup timeouts; Q9 DuckDB warmup/calibration allocation refusals|

Raw Q12 completed correctly in987.017697ms against978.5276092588902ms ceiling.
Its three measured engine slots remain not_run. Across engine warmups there are
six timeouts and this one late completion; two other queries have invalid
references and no engine warmup. Empty results and missing executions remain
different outcomes.

Iceberg Q9's third calibration refused262144bytes after two completed calibrations
of3328.075073 and3328.507067ms. There is no valid three-sample ceiling. The failed
reference retired before Q19; all three Q19 pairs pass. Lance Q9's reference
warmup refused134217728bytes, then all three calibration calls repeated that
refusal. This run records allocation refusals, not the SIGABRT seen historically.
The harness should stop reference calibration immediately after an earlier
reference failure; current dependent engine gating remains correct, but those
extra requests reuse a failed process unnecessarily. Preserve this evidence
before changing that control flow.

Conditions: all22 canonical SF10 queries per track,16 threads,4GiB query/12GiB
process limits, three measured samples and one session, sequential48GiB scope.
The scope peak is25,204,043,776bytes and max/oom/oom_kill are zero. Per-track scope
snapshots are cumulative and include charged file cache; they are not individual
query RSS. Allocation refusals remain failures despite zero cgroup OOM events.

Pinned DuckDB/Python/provider policy is preserved in manifests. Lance retains the
documented optimizer-disabled extension reference because its decimal AVG pushdown
was previously wrong; this is not stock pushdown performance. No engine, SQL,
data or harness changed during the run. Postprocessing72379 completed under2GiB
and issued no new queries. Raw/Native completion counts match the preceding
screen; Iceberg's reference-failure phase and worker lifecycle differ. Do not
attribute its extra valid pairs to key reuse.

Evidence: `docs/benchmarks/2026-09-08-prepared-key-providers/`, linked to the
493-input source snapshot in `../2026-09-08-prepared-key-pairs/`. The independent
paired Q10/Q19 gains remain qualified in
[the implementation report](prepared-aggregate-keys-2026-09-08.md). No full suite
ratio or DuckDB leadership follows from these incomplete tracks. IPC/GPU residency,
lower budgets/concurrency, larger workloads and full acceptance remain open.
