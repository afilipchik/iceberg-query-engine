# Native Q6 execution profile — 2026-09-11

Frozen480109f6 produced one independently typed-correct nativeQ6 result during149stopped-thread snapshots. Source524 is the current worktree guard; the executed binary retains its earlier523-input provenance.16threads,4/12GiB query/process,48GiB containment. No source or SQL changed.

The initial source-line-breakpoint attempt failed before query execution because the release has no source-line debug information. The second attempt used the existing sql_impl first-poll function symbol, covering parsing/planning and execution while excluding worker setup. Sampling requests were5ms apart, capped at200snapshots. GDB stop/resume overhead perturbs execution: these counts are not CPU percentages, unbiased time shares or latency evidence.

| Frame present in snapshot | Snapshots |
|---|---:|
| query_engine::physical::operators::filter::compare_operands | 75 |
| query_engine::physical::operators::filter::evaluate_expr_internal | 75 |
| query_engine::planner::numeric::cast_array | 44 |
| query_engine::physical::operators::filter::coerce_arrays | 44 |
| query_engine::physical::compiled_expr::PredicateEvaluator::evaluate | 31 |
| query_engine::planner::numeric::compare_float_arrays | 18 |
| core::ptr::drop_in_place<query_engine::storage::ipc_cache::RowGroupReader> | 14 |
| query_engine::physical::operators::spillable::stream_merge_input_partitions::{{closure}}::{{closure}} | 11 |

Across all threads, leaf counts include23`__floattidf`,21`arrow_cast::cast::cast_from_decimal`,17BooleanBuffer construction,13Arrow comparison,10filter_native and14munmap observations. Most other thread stacks are parked; the main thread is parked in147snapshots. Depth8and inlining limit attribution.

This identifies mixed numeric predicate coercion as a shared CPU investigation target. NativeQ6 uses NativeStreamingScanExec → Filter → Project → SpillableHashAggregate. The precision control already shows aggregate processing is a small fraction of its execution duration. Before changing code, inspect decimal/float scalar comparison and repeated BETWEEN coercions, and construct independent boundary/NULL/non-finite regressions. Any fused or borrowed comparison must reproduce existing SQL coercion semantics, not silently switch to exact decimal-versus-float ordering. The unselected IPC-dictionary source hypothesis remains unmeasured and is lower priority than this captured path.

```json
{
  "path": "/sys/fs/cgroup/user.slice/user-1000.slice/user@1000.service/app.slice/safe-build-3062526-24922.scope",
  "memory.max": "51539607552\n",
  "memory.swap.max": "0\n",
  "memory.peak": "201396224\n",
  "memory.events": "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n"
}
```

[Failed identical-binary precision control](native-header-precision-2026-09-11.md). [Complete profile archive](benchmarks/2026-09-11-native-q6-profile/manifest.json). The failed first attempt is preserved separately in the archive.
