# Retained LEFT COUNT: both-mode feature gates

Frozen0c867c5c validation20503 is terminal1 because historical gates remain
failing. The complete after-run check verifies520 source inputs against the
release snapshot. No source changes occurred during validation.

| Gate | Default disjoint | Experimental partial |
|---|---:|---:|
| Library |1,092 pass/11 ignored|1,092 pass/11 ignored|
| Contract integrations |115 pass|115 pass|
| Native/IPC integrations |58 pass/2 fail|57 pass/3 fail|
| Spill/numeric/decimal integrations |28 pass/6 fail|28 pass/6 fail|

Within the last row, systemic numeric13 and the focused decimal/parallel-input
checks7 pass in each mode; legacy spill remains8 pass/6 fail. Native streaming
passes10 in each mode. The two dictionary semi/anti plan assertions persist;
partial additionally fails the formatted floating-point native comparison.
Legacy spill failures retain the join/semi/anti/count-distinct names recorded
against a4103dfa. No new or removed failure names occur in the full comparison.
Unchanged names do not prove identical failure causes or clear resource gates.

Commands run locked/offline with features `lance,gpu`, `--no-fail-fast`,
`QE_AGG_OWNERSHIP=disjoint` then `partial`, `RAYON_NUM_THREADS=16`, repository
TMPDIR and the48GiB/one-build-job wrapper. The exact per-suite target lists,
commands, environment and exit codes are in `left-count-validation.json`.
The engine source is the same520-input snapshot used by the release and paired
measurement. No dependency/default ownership change occurs.

[Independent low-budget spill oracle](left-count-resource-2026-09-11.md) adds
three typed-correct executions of the new rewrite at1/4/16MiB, with actual spill
at1MiB. This is distinct from the historical failing gates and does not replace
them. [Paired measurements](left-count-measurement-2026-09-11.md) show Q13 gains
and protected Q9 regression signals. Full provider/residency/resource/concurrency
acceptance and DuckDB leadership remain unproven.
