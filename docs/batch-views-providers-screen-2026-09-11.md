# Aggregate batch views: providers screen — 2026-09-11

Frozen candidate480109f6, 523 verified source inputs, default disjoint ownership. One session with three samples per canonical SF10 query. This is a screening run, not multi-session, resource, concurrency or DuckDB leadership certification.

Raw/native/Iceberg/Lance use16threads and4/12GiB query/process caps. Canonical decoded IPC and GPU controls use16threads and32/48GiB, with preload excluded: a capacity experiment that does not clear16GiB preload admission. Custom float GPU smoke uses4threads and4/8GiB,20samples per query; its device proof does not establish canonical SF10 device execution.

| Track | Valid measured pairs | Typed-correct outputs including warmup | Completion |
|---|---:|---:|---|
| raw_parquet | 66/66 | 88 | complete screen |
| native | 57/66 | 76 | incomplete |
| iceberg | 64/66 | 86 | incomplete |
| lance | 61/66 | 82 | incomplete |

| Track | Geometric mean ratio | Suite total ratio |
|---|---:|---:|
| raw_parquet | 2.557746776550353 | 2.790526306454685 |
| native | incomplete | incomplete |
| iceberg | incomplete | incomplete |
| lance | incomplete | incomplete |

Ratios are engine/DuckDB; lower is better. Incomplete tracks receive no full-suite score. All query ratios and failures remain in the archive.

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

[Complete archive](benchmarks/2026-09-11-batch-views-providers/manifest.json). See runs/state.json, each report.json, samples.jsonl and execution.jsonl for exact failure provenance.
