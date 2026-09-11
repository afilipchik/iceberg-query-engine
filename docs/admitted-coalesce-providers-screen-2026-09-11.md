# Bounded admitted output: providers screen — 2026-09-11

Frozen candidate recorded in the archived release manifest, 530 verified source inputs, default disjoint ownership. One session with three samples per canonical SF10 query. This is a screening run, not multi-session, resource, concurrency or DuckDB leadership certification.

Raw/native/Iceberg/Lance use16threads and4/12GiB query/process caps. Canonical decoded IPC and GPU controls use16threads and32/48GiB, with preload excluded: a capacity experiment that does not clear16GiB preload admission. Custom float GPU smoke uses4threads and4/8GiB,20samples per query; its device proof does not establish canonical SF10 device execution.

| Track | Valid measured pairs | Typed-correct outputs including warmup | Completion |
|---|---:|---:|---|
| raw_parquet | 66/66 | 88 | complete screen |
| native | 60/66 | 80 | incomplete |
| iceberg | 64/66 | 87 | incomplete |
| lance | 63/66 | 84 | incomplete |

| Track | Geometric mean ratio | Suite total ratio |
|---|---:|---:|
| raw_parquet | 2.4477885337376444 | 2.6290993723968255 |
| native | incomplete | incomplete |
| iceberg | incomplete | incomplete |
| lance | incomplete | incomplete |

Ratios are engine/DuckDB; lower is better. Incomplete tracks receive no full-suite score. All query ratios and failures remain in the archive.

Cumulative build-free screen scope resource evidence:

```json
{
  "path": "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/safe-build-3346565-9008.scope",
  "memory.max": "51539607552\n",
  "memory.swap.max": "0\n",
  "memory.peak": "22185889792\n",
  "memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n"
}
```

Independent audit exit codes: {'providers': 0, 'provider_ratios': 0, 'residency': 0}. A nonzero audit remains a failed gate and is preserved in this archive.

Every completed output was independently compared with its typed oracle after all timed stages ended. Strict valid-pair counts require both engines within the matched query ceiling. Timeouts, refusals, reference crashes and not-run samples remain failed outcomes; typed correctness alone does not clear performance gates.

[Complete archive](benchmarks/2026-09-11-admitted-coalesce-providers/manifest.json). See runs/state.json, each report.json, samples.jsonl and execution.jsonl for exact failure provenance.
