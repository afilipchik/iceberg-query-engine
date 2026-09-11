# Admitted parallel queue: strict Q12 result — September 8, 2026

Frozen binary `ed72128518c36ed427a9474310f5f256bf48cc26633661361aebb6dceda123e0`
completes both previously failing budget cases with exact typed results, but
**both still fail the strict performance gate**. No leadership pass or suite
speedup is claimed.

| Query / process budget | Fresh DuckDB median | Engine | 10× ceiling | Typed result | Time gate |
|---|---:|---:|---:|---|---|
| 1 / 12 GiB | 93.218 ms | 947.924 ms | 932.184 ms | Valid | Fail |
| 4 / 4 GiB | 94.438 ms | 963.811 ms | 944.380 ms | Valid | Fail |

Each budget has one DuckDB warmup, three fresh reference calibrations and one
instrumented engine execution. This is attribution/budget isolation, not a paired
multi-session statistical benchmark. The unchanged watchdog permits response
delivery separately from the exact query-time gate; receiving a correct answer
after the ceiling does not make it pass. Driver34658 exited1 as required. Both
engine workers exited0, all reference comparisons passed, no cgroup max/OOM/
OOM-kill counters increased, and neither case left temporary files. The shared
scope peak was258768896 bytes. Reservation peaks and RSS remain different measures.

## What changed in the measured path

Both Project/join-build queues now report `prepared=true`,
`admitted_buffers=true`,16 partitions and16 demand slots, with no copied-output
envelope. All16 producers finish, yielding310803 rows in7323 batches. The prior
ordinary-path trace had one slot and458 batches for the same result cardinality.

| Build-input evidence | 1 / 12 GiB | 4 / 4 GiB |
|---|---:|---:|
| Preparation | 2.453 ms | 1.912 ms |
| Producer wall-span range | 389.305–442.494 ms | 391.159–455.772 ms |
| Summed completed poll spans | 6601.628 ms | 6611.941 ms |
| Summed copy spans | 0.199 ms | 0.194 ms |
| Per-producer permit-wait range | 2.395–3.677 ms | 2.370–3.112 ms |

The earlier serialized trace had producer wall spans843.494–854.900ms and
per-producer permit waits792.112–802.670ms. These are separate instrumented runs,
not an A/B speedup estimate. The new trace establishes that the selected queue
uses concurrent admitted pulls and removes that permit serialization. It does
not establish a16× query improvement. Poll and wait spans overlap and are not
additive CPU time.

The admitted reader now emits survivors of each8192-row input chunk, producing
many small batches after selective filtering. It also expands dictionary strings
before evaluating the static predicate. Both are general source-level costs to
investigate; current spans do not independently attribute a fraction to either.
The aggregate frontier above SpillableHashJoin still reports one slot with an
unknown bound. In the4GiB case, probe input wait is roughly22–26ms per partition,
and the aggregate consumes115 batches/310803 rows with no aggregate spill.
Do not promote that join path merely by adding permits: decoder, candidate and
gather allocations need a valid ownership/admission contract first.

## Reproduction and evidence

Release77076 completed in8m44s with locked/offline lance,gpu, one build job and
a48GiB scope. Its487 input hashes cover all files under src/tests/examples/benches
plus manifests/build configuration/wrapper. Source was checked before and after
compilation. Library41692:980 pass/10 existing ignores; integrations48793:33 pass,
no skips. The deterministic cleanup regression failed before the shared fix.

The diagnostic used `.scratch/parallel-aggregate-input/isolate_admitted_q12_gated.py`
with explicit `--binary admitted_queue_benchmark_embedded`,
`--release admitted-queue-release.json` and `--output q12-queue-gated-isolation`
paths under that scratch directory. It ran via the safe-build wrapper with
repository TMPDIR, the pinned Python environment and PYTHONPATH=scripts. Worker
setup fixes16 threads and preserves dataset, plan, boundary and resource evidence.

The archive is `docs/benchmarks/2026-09-08-admitted-queue/`: complete diagnostic
outputs, source hashes/snapshot, build/test logs, driver, trace extraction and
hash manifest. Previous candidates and failure archives remain unchanged.

Next: use the local-source comparison in
`encoded-input-source-review-2026-09-08.md` to isolate encoded filter/expansion
and selective-batch costs before another engine change. Preserve both this
failed gate and the broader protected/provider/resource/concurrency requirements.
