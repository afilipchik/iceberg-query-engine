# Frozen acdb8c51 canonical SF10 provider screen — 2026-09-09

Provider10708 terminal1; independent completed-output audit52220 terminal0.
All503 source inputs and the frozen binary verify unchanged before/after.
No engine source or dependency change during this screen.

| Provider | Valid measured pairs | Completed warmups | Incomplete queries |
|---|---:|---:|---|
| Raw Parquet |63/66|21|Q9 warmup timeout|
| Native |63/66|21|Q1 warmup timeout|
| Iceberg |64/66|22|Q18 measured DuckDB refusal, two dependent pairs skipped|
| Lance |57/66|19|Q1 timeout, Q18 input-domain refusal, Q9 invalid reference calibration|

All247 completed measured outputs and83 completed warmups match complete typed
DuckDB oracles:330 correct outputs. This audit does not reclassify incomplete or
failed pairs. All four tracks remain incomplete. There are three engine warmup
timeouts, one engine warmup query error, and two distinct reference failures.
No scope max/OOM/kill events occurred; cumulative scope high-water reached
25,400,700,928 bytes. This does not certify query-wide memory reservations.

## Findings that change the next action

Q13 completes in these provider screens after the bounded outer pipeline repair.
Raw Q9 still times out near a5-second ceiling; native Q9 completes around2.9s.
One recorded native request spends2,847ms executing and about29ms planning and
optimizing (parsing is separate). Its plan includes a NativeStreamingScanExec
under a chain of inner joins and computed Projects. A prepared Q9 diagnostic will
record join phases and aggregate input-frontier capability before selecting a fix.
Provider timings alone do not isolate scan, routing, scheduling or representation.

Lance Q18 fails with an explicit query error: `memory scan working space` requests
8,192 more bytes while using536,857,792 of536,863,955 admitted bytes. This is the
resident input child domain, approximately one eighth of the4GiB query budget.
It is a clean refusal, not an allocator abort or host OOM. It does not prove the
whole query exhausted4GiB, nor identify which consumer retained the buffers.
Reproduce against the older frozen control and trace ownership before modifying
budgets. Investigate whether build/materialization ownership must move from
transient input to operator state, or spill, rather than accumulating under a
fixed input quota. Do not weaken admission or increase budgets to hide the result.

Iceberg Q18 has a completed correct engine warmup; a later DuckDB measured request
raises OutOfMemoryException, so dependent requests stop. Lance Q9 lacks a valid
reference calibration. Preserve those reference outcomes; they do not establish
engine correctness or speed for the skipped requests.

## Conditions and reproducibility

Canonical SF10; one session, three measured samples/query,16 threads and affinity
0–15,4GiB query and12GiB process per worker. The full run uses a48GiB memory-capped
scope with swap disabled and TMPDIR in repository scratch. Each provider has a
matched reference, fresh calibration and10×query ceiling; startup allowances stay
separate. Providers, snapshots/data, SQL, output schemas, timings and provenance
are retained by the harness. Raw, native, Iceberg and Lance remain distinct modes.

Drivers are `.scratch/parallel-aggregate-input/run_contiguous_copy_providers.py`
and `summarize_contiguous_copy_providers.py`, executed with the required wrapper,
PYTHONPATH=scripts and the existing venv-lance Python. The audit checks every
completed measured/warmup Arrow output against the complete typed oracle. No
GPU execution is implied by build features or by these provider results.

This is a single-session screen, not the full three-session resource/concurrency
acceptance matrix. IPC/preloaded CPU/GPU residency92861 is now running separately;
its results are not yet available. See also the
[balanced component](contiguous-copy-paired-2026-09-09.md) and
[current copy contract](contiguous-resident-copy-2026-09-09.md).

Archive: [manifest](benchmarks/2026-09-09-contiguous-copy-providers/manifest.json),
1,410 files verified. Twelve omitted scratch spill/temp files contain zero bytes.
