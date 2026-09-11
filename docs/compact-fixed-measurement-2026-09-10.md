# Compact fixed state: memory benefit with performance regressions

Frozen8b6a82e9 produces64independently typed-correct outputs, but this candidate
is not accepted as a performance improvement. Resident16thread Q18 mean time rises
16.96%, with both blocks slower; rawQ1 mean rises13.21%. Q18 reservations decrease
substantially. Retain the memory benefit as a design objective while removing the
hot-loop cost; do not weaken regression gates or tune a query-specific exception.

## Reproduction and scope

Release64441 is terminal0 in8m53s, SHA256
`8b6a82e9d85ba9c70d15ca68c738b548af5b1d646fb1eadd76ef224aada6d68c`,
519verified inputs. Controlc20b0648 is the preceding incremental native reader.
Paired4982 terminal0: two fresh-process blocks with reversed binary order for
Q1/Q9/Q17/Q18 across raw/native16threads and decoded resident16/4threads. Affinity
CPU0–15 for16workers and0–3 for4workers. Default disjoint ownership, GPU0;
raw/native4GiBquery/12GiBprocess, resident32/48GiB with preload excluded. Enclosing
48GiB capped wrapper, swap disabled, repository TMPDIR. Each query has a180second
diagnostic watchdog; this is not matched DuckDB10times acceptance or a full suite.

All64outputs match independent typed oracles, all1820join traces complete
without unwinding. Source/dataset/binary/driver/harness after-guards verify.
Scope peak33064067072bytes, zeroOOM/max events. This does not establish query-wide
reservation coverage. Two blocks provide directional evidence, not a protected
confidence bound or between-session certification.

## Timings

Candidate/control ratios below1favor candidate. Every sample remains archived.

| Mode/query | Control ms | Candidate ms | Ratio of means | Block ratios |
|---|---|---|---:|---|
| raw_parquet/q01 | 613.225, 639.410 | 670.534, 747.564 | 1.132091 | 1.093454, 1.169146 |
| raw_parquet/q09 | 1506.702, 1483.971 | 1568.405, 1482.961 | 1.020294 | 1.040953, 0.999319 |
| raw_parquet/q17 | 1831.026, 1824.393 | 1834.691, 1830.985 | 1.002806 | 1.002002, 1.003614 |
| raw_parquet/q18 | 1039.684, 1036.948 | 1035.253, 952.276 | 0.957092 | 0.995738, 0.918345 |
| native/q01 | 11680.202, 11493.789 | 12170.107, 12878.266 | 1.080883 | 1.041943, 1.120454 |
| native/q09 | 2705.512, 2673.151 | 2582.753, 2975.452 | 1.033380 | 0.954626, 1.113088 |
| native/q17 | 1293.890, 1326.475 | 1299.204, 1355.488 | 1.013100 | 1.004107, 1.021873 |
| native/q18 | 4751.819, 4922.186 | 4556.685, 4518.378 | 0.938088 | 0.958935, 0.917962 |
| cpu_resident_32g/q01 | 11165.494, 11106.613 | 11778.749, 11746.077 | 1.056246 | 1.054924, 1.057575 |
| cpu_resident_32g/q09 | 1594.608, 1576.430 | 1592.244, 1610.633 | 1.010041 | 0.998518, 1.021697 |
| cpu_resident_32g/q17 | 1073.982, 1054.656 | 1098.090, 1062.344 | 1.014937 | 1.022448, 1.007289 |
| cpu_resident_32g/q18 | 4801.146, 4765.342 | 5562.434, 5626.138 | 1.169559 | 1.158564, 1.180637 |
| cpu_resident_4t_32g/q01 | 14223.407, 14081.243 | 13816.908, 13806.296 | 0.975925 | 0.971420, 0.980474 |
| cpu_resident_4t_32g/q09 | 2450.502, 2377.373 | 2386.814, 2341.477 | 0.979373 | 0.974010, 0.984901 |
| cpu_resident_4t_32g/q17 | 1289.084, 1298.643 | 1282.881, 1297.657 | 0.997222 | 0.995188, 0.999241 |
| cpu_resident_4t_32g/q18 | 5703.155, 5697.182 | 6167.414, 6156.284 | 1.080994 | 1.081404, 1.080584 |

## Memory and attribution

Resident16thread Q18 reserves about2.722GB in control and2.185GB in candidate
in both blocks, a roughly512MiB reduction. NativeQ18 also falls from about2.166GB
to1.629/1.558GB. These are query reservation high-water marks, not total process
RSS. Low-cardinality Q1 has only a few live group states, so halving their cells
barely changes query-wide high water.

First-block resident Q18 routing is essentially unchanged(1083.127/1082.828ms),
while aggregate processing rises2100.494to2906.015ms and ingestion3190.337to
3995.550ms. Finish/output is slightly faster. This places the observed extra work
in ingestion, without proving every source-level cause. The second block retains
the Q18 regression. NativeQ18 improves in this small study, demonstrating that a
single provider's result cannot substitute for the others.

The frozen disassembly of StateRows::prepare_arrays_indexed contains a call to
FixedCell::try_from at0x5fcf06b targeting0x5fa4bc0, after shared numeric updates.
The first compact representation reconstructs general AccumulatorState and converts
it back for every fixed update. That out-of-line conversion is a source/assembly
fact and a concrete next hypothesis; it is not by itself a timing attribution.
Raw Q1 also regresses and must remain a protected control in the next comparison.
Do not dismiss it as noise or assume conversion alone explains it.

## Required next change

Implement direct fixed-cell update using shared arithmetic primitives for decimal
rescaling/overflow, float conversion, AVG and Welford variance. Preserve checked
counts/signed sums and exact unsigned coefficients, boolean/NULL states and
whole-row scratch/commit. Keep general-state conversion at cold compatibility
boundaries when appropriate. Validate independently before another frozen study.

Retain the same c20b0648 parent control when judging whether the overall change
recovers the observed regression; comparing only against the slower8b6a82e9 could
hide a remaining loss. Check the compiled loop and rerun the prespecified provider/
thread shapes. Full protected/provider/residency/resource/concurrency acceptance
remains open. No default ownership promotion or DuckDB leadership claim.

[Representation and tests](compact-fixed-state-2026-09-10.md),
[archive](benchmarks/2026-09-10-compact-fixed-paired/manifest.json).
