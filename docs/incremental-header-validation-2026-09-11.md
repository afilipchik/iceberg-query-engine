# Incremental header admission: feature and resource gates — 2026-09-11

Source524, lance/gpu features, both ownership modes,16Rayon threads. Commands, full outputs, failures and source hashes are preserved. New-failure comparison also requires complete executable results, consistent pass/ignored counts and correct process exits; unchanged panic names alone do not prove a completed run.

| Mode/suite | Passed | Failed | Ignored |
|---|---:|---:|---:|
| disjoint-library | 1104 | 0 | 11 |
| disjoint-contracts | 125 | 0 | 0 |
| disjoint-native_ipc | 63 | 0 | 0 |
| disjoint-spill | 28 | 6 | 0 |
| partial-library | 1104 | 0 | 11 |
| partial-contracts | 125 | 0 | 0 |
| partial-native_ipc | 62 | 1 | 0 |
| partial-spill | 28 | 6 | 0 |

No newly added failure names compared with frozen480109f6. Existing failures remain failed gates; the header repair does not certify general spill progress or native allocation ownership. Source stays frozen for the optimized matched comparison.

```json
{
  "path": "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/safe-build-2998918-14249.scope",
  "memory.max": "51539607552\n",
  "memory.swap.max": "0\n",
  "memory.peak": "30022651904\n",
  "memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n"
}
```

[Source and red/green regressions](incremental-header-admission-plan-2026-09-11.md). [Complete validation archive](benchmarks/2026-09-11-incremental-header-validation/manifest.json).
