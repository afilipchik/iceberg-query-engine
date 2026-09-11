# Retained LEFT COUNT: frozen runtime evidence

Release14244 terminal0 in8m51s freezes binary
`0c867c5c1aaa288b09c4c0c275ac10037f79d66339facfd60286a2cda90f84f4`,
with520 verified source inputs, locked/offline lance/gpu features and48GiB/one
build job. Current source differs from a4103dfa only in the eager-aggregation
rule and semantic-proof tests, including the qualifier/name repair.

Diagnostic10779 terminal0 verifies three typed-correct canonical SF10 Q13
outputs. Optimized plans in raw Parquet, native and decoded-resident modes all
contain right COUNT preaggregation and retained SUM(COALESCE(__ea_cnt,0)).
The rewrite is therefore exercised at the actual workload boundary.

| Mode | Single instrumented sample, ms |
|---|---:|
| Raw Parquet,16 threads |644.573876|
| Native,16 threads |1759.245008|
| Decoded resident,16 threads |2131.383298|

These are activation/correctness diagnostics, not a paired performance claim.
Default disjoint ownership, CPU0–15, GPUoff. Raw/native use4GiB query/12GiB
process budgets; resident uses32GiB query/48GiB process with preload excluded.
The180s diagnostic watchdog is not fresh DuckDB10× acceptance. All outputs use
independent typed oracles and preserved SQL/data/provider provenance. After-run
checks verify source/binary/harness/driver/data/provider hashes. The48GiB scope
peaks at21965078528bytes, swap0, zero max/OOM/kill events.

The47 join traces are retained in raw output. They are diagnostic events, not
exclusive CPU samples. Do not infer attribution from their count. Paired97041 is terminal0:80 typed-correct outputs and2,036 complete join traces
against frozen a4103dfa. Source/binary/data/provider/harness/driver after-run checks
pass. The48GiB scope peaks at17230487552bytes, swap0, zero max/OOM/kill events.
Only Q13 logical and physical plans change; all four protected queries retain
their plans in every mode/block.

| Mode | Q13 candidate/control mean ratio | Block1 | Block2 |
|---|---:|---:|---:|
| Raw16 |0.294943|0.302080|0.287818|
| Native16 |0.871506|0.879640|0.863495|
| Resident16 |0.919702|0.930771|0.908862|
| Resident4 |0.826499|0.832098|0.820940|

Q13 outer-probe output falls from15,345,388 to1,500,000 rows in every mode/block.
The right count preaggregate emits999,981 groups; the retained final aggregate
preserves left multiplicity. These are measured cardinalities, not exclusive CPU
attribution. Q13 improves consistently within this two-block diagnostic.

Protected-query ratios retain the negative evidence: raw Q9 is1.041648
(blocks1.080432/1.003891), resident4 Q9 is1.028211(1.016979/1.039342).
Native Q9 is1.028008 with opposing blocks1.070141/0.986245. Native Q1/Q18
are1.005236/1.006423, both slower in both blocks. Raw Q1's0.871046 mean ratio
has very different block ratios0.990131/0.778121 despite identical plans; do not
attribute that apparent gain to the count rewrite. Other protected ratios are
in the archived paired-ratios file. No confidence interval, performance neutrality,
or DuckDB leadership is certified. Full provider/resource/concurrency and
multi-session acceptance remain open.

See [implementation and regression](left-count-retained-reduction-2026-09-11.md).

[Diagnostic archive](benchmarks/2026-09-11-left-count-diagnostic/manifest.json)
verifies193 files and520 source inputs.

[Paired archive](benchmarks/2026-09-11-left-count-paired/manifest.json).

The paired archive verifies672 files and520 inputs. See the
[independent low-budget spill oracle](left-count-resource-2026-09-11.md).
Both-mode feature validation20503 is terminal with no added failure names;
[full validation](left-count-validation-2026-09-11.md) preserves historical gates.
Full canonical provider20601 is active.
