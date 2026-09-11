# Parallel key preparation: full provider screen — September 8, 2026

Frozen e6a60347 completed all four scheduled canonical SF10 tracks. The screen
remains incomplete:237 of264 requested measured pairs validate and meet timing.
All317 completed engine outputs validate independently, including80 warmups.
There is no valid full-suite ratio or leadership claim.

|Track|Valid pairs /66|Completed, correct warmups|Remaining failures|
|---|---:|---:|---|
|Raw Parquet|60|20|Q9/Q13 engine warmup timeouts|
|Native|57|20|Q1/Q13 engine warmup timeouts; Q6 late warmup|
|Iceberg|63|21|Q9 reference calibration refusal|
|Lance|57|19|Q1/Q13 engine warmup timeouts; Q9 reference warmup refusal|

Native Q6 completes correctly in693.125825ms against667.052381ms ceiling. It
remains failed; no measured retry was made. Raw Q12 passes this screen but failed
the earlier paired comparison. These different warmup outcomes are not evidence
that parallel key preparation caused or resolved a particular gate failure.

Iceberg Q9's first calibration refuses262144bytes; remaining calibrations are
explicit not_run records. Lance Q9 refuses134217728bytes during reference warmup;
all calibrations are skipped. Both failed reference workers close before later
SQL. No reference crash occurred in these cases. In total: six engine warmup
timeouts, one late completed warmup, two reference-invalid queries,27 not_run
measured slots. All237 completed measured pairs pass typed and elapsed gates.

Run48932 exited1 as required. Supplemental validation50146 exited0. Each track
uses all22 canonical queries,3 samples,1 session,16 threads,4GiB query/12GiB
process and matched provider inputs. Tracks run sequentially inside one48GiB
scope with repository TMPDIR. Scope peak20,264,665,088bytes; max/oom/oom_kill zero.
The peak includes shared file cache and preceding tracks, not isolated query RSS.

The same frozen lance/gpu-feature binary and immediate-stop reference harness
were used throughout; neither was edited during measurement. Command, timings,
plans, hashes, independent oracle files, all failures and supplemental warmup
validation are preserved in `benchmarks/2026-09-08-parallel-key-providers/`.
Source snapshot: `benchmarks/2026-09-08-parallel-key-pairs/source.tar.gz`.
Driver: `.scratch/parallel-aggregate-input/run_parallel_key_providers.py`.

This screen is separate from decoded IPC and GPU residency. Next capacity screen
uses32GiB query/48GiB process for canonical preloaded data, under64GiB containment.
That experiment cannot clear the earlier16GiB preload refusal. Custom float
GPU control/required execution remains a distinct workload, not canonical SF10.
Resource/concurrency and protected multi-session acceptance remain open.
