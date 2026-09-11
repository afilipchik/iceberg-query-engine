# Aggregate batch views: residency screen — 2026-09-11

Frozen candidate480109f6, 523 verified source inputs, default disjoint ownership. One session with three samples per canonical SF10 query. This is a screening run, not multi-session, resource, concurrency or DuckDB leadership certification.

Raw/native/Iceberg/Lance use16threads and4/12GiB query/process caps. Canonical decoded IPC and GPU controls use16threads and32/48GiB, with preload excluded: a capacity experiment that does not clear16GiB preload admission. Custom float GPU smoke uses4threads and4/8GiB,20samples per query; its device proof does not establish canonical SF10 device execution.

| Track | Valid measured pairs | Typed-correct outputs including warmup | Completion |
|---|---:|---:|---|
| canonical_decoded_ipc | 66/66 | 88 | complete screen |
| canonical_gpu_control | 66/66 | 88 | complete screen |
| canonical_gpu_mixed | 66/66 | 88 | complete screen |
| smoke_gpu_control | 40/40 | 42 | complete screen |
| smoke_gpu_required | 40/40 | 42 | complete screen |

Canonical mixed-GPU completed88CPU requests, with zero reported successful device requests and zero resident-evidence records; case exit0. Custom required-GPU request-scoped device validation passes all40measured requests. All CPU controls completed in this run. The original archived analysis labels the88completed request records as device-evidence records; the archived supplemental-validation.json contains the authoritative zero-device counts. This corrected report distinguishes execution from device coverage.

| Residency track | Geometric mean ratio | Suite total ratio | Wins |
|---|---:|---:|---:|
| Decoded IPC | 0.760895 | 1.325514 | 14/22 |
| Canonical CPU control | 0.782442 | 1.365811 | 14/22 |
| Canonical mixed GPU (CPU execution) | 0.790478 | 1.381345 | 14/22 |

These are engine/DuckDB ratios. No canonical suite total beats DuckDB.

Cumulative build-free screen scope resource evidence:

```json
{
  "path": "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/safe-build-2939533-17286.scope",
  "memory.max": "51539607552\n",
  "memory.swap.max": "0\n",
  "memory.peak": "26892062720\n",
  "memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n"
}
```

Every completed output was independently compared with its typed oracle after all timed stages ended. Strict valid-pair counts require both engines within the matched query ceiling. Timeouts, refusals, reference crashes and not-run samples remain failed outcomes; typed correctness alone does not clear performance gates.

[Complete archive](benchmarks/2026-09-11-batch-views-residency/manifest.json). See runs/state.json, each report.json, samples.jsonl and execution.jsonl for exact failure provenance.
