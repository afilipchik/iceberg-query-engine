# Native header comparison: precision control — 2026-09-11

Identical frozen480109f6 binaries produced32typed-correct outputs across8balanced blocks of nativeQ6/Q9. Every request uses a fresh process with16threads,4/12GiB query/process limits and the same instrumentation as the original two-block diagnostic. All16within-block logical and physical plan pairs match. The current524source inputs are a worktree freeze guard; the executed480109f6 binary has its own earlier523-input provenance.

| Query | Ratio of means | Bootstrap interval | Precision gate |
|---|---:|---|---|
| q06 | 1.025549 | 0.820231–1.259049 | fail |
| q09 | 1.001427 | 0.985811–1.016126 | pass |

The protocol predeclared5000paired-block bootstrap resamples,seed20260911,quantiles0.0125/0.9875, and required the whole interval within0.95–1.05 for both queries. This small-sample bootstrap is a diagnostic precision check, not guaranteed confidence coverage or performance certification.

NativeQ6 fails. The dependent13210a20 comparison was not run. Preserve the earlier18.2%observed nativeQ6 slowdown as unresolved: this failed control neither proves a regression nor establishes neutrality. Q9 passes this precision check; it does not rescue the failed prerequisite.

Existing profiles show nativeQ6 cumulative aggregate processing of about7–22ms while query execution spans about709–1883ms, with identical reported reservation peaks. These counters do not cover native scan time or prove where the remaining wall time is spent. Inspect input/filter execution and first-request behavior before choosing a CPU change.

```json
{
  "path": "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/safe-build-3051480-17654.scope",
  "memory.max": "51539607552\n",
  "memory.swap.max": "0\n",
  "memory.peak": "1405321216\n",
  "memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n"
}
```

[Original paired result](incremental-header-measurement-2026-09-11.md). [Complete archive](benchmarks/2026-09-11-native-header-precision/manifest.json). No source, budget or SQL changed for this control.
