# Materialized build schema and terminal spill refusal

The fixed-width admitted scan candidate2f9ad9f1 still leaves raw Q9 serial despite
supporting the file's types and encodings. Generic initialization-only probes
narrow the boundary: the raw leaf, first part join and orders join offer admitted
preparation, but the supplier join and enclosing projection decline. No output
stream is polled by these probes; preparation may execute build inputs. The
probe uses the context pool rather than sql()'s per-query child and is diagnostic,
not a benchmark or full runtime acceptance test.

## Declared schema must survive materialization

`SpillableHashJoinExec::compute_build_decision` previously constructed its in-memory
build table with the first returned batch's schema. An operator declaring logical
UTF8 can return physical dictionary-encoded batches. Substituting that batch's
schema changes the delegate's declared field type, causing otherwise supported
admitted joins to decline. This is a general representation-boundary error,
not a query-specific join-order issue.

The candidate preserves `build_side.schema()` when constructing the in-memory
build table. Original physical batches and codebooks remain intact; existing
MemoryTableExec output adaptation handles their actual representation. The
admitted gather selects logical values through its checked dictionary handling.
Build decision, keys, spill costing, ordinary representation handling and query
budgets remain in place. Field metadata is retained.

Red75599 reproduces the decline with a logical-UTF8 producer returning an encoded
batch. Green72928 passes. The expanded regression covers both build orientations,
three batches including an empty batch and changed codebooks, NULL keys/values,
duplicates, field metadata, all four probe partitions and output-owner cleanup.

## Preserve the real cause of an unsplittable memory refusal

The first combined library gate65131 had1,075passes/11ignored and one failure:
a SQL retained-result test received generic "single-key working set" failure
instead of its typed memory cause. Isolated41524 passes, so the exact boundary is
sensitive to resource/scheduling conditions; a pass does not erase the broad failure.

Source review found that PartitionScheduler::merge discards memory errors into
None, and visit replaces that absence with a generic error when split planning
finds only one exact key. New deterministic red62644 leaves8KiB available for
one spilled COUNT group: split planning succeeds and proves it unsplittable, while
merge state cannot fit. The old scheduler loses the typed allocator refusal.

The repair keeps the original error until repartition feasibility is known and
returns it when no split exists. Partial merge state has already dropped. Group
count limits remain separate; non-memory and output-consumer errors are unchanged.
This repairs error fidelity, not the underlying insufficient-memory condition.
The regression checks no output, the named typed pool limit and complete file/
reservation cleanup. No source or output replay is introduced.

## Diagnostic utility and current gates

The existing `prepared_plan_probe` now accepts optional `copied|admitted` mode;
default copied behavior remains. It reports whether admitted preparation exists
and the declared partition count without exposing or polling private streams.
An initial SQL-path typo failed before execution and is preserved. The corrected
path comes from the dataset manifest. Neither diagnostic timing nor declared
partition count alone proves runtime concurrency.

All builds/tests/probes run through the repository48GiB memory-capped wrapper,
TMPDIR=.scratch, locked/offline features lance,gpu, one build job. Combined7587
is terminal0:1,077library passes/11ignored and43integrations. Both-mode20831
is terminal1: partial library1,077/11ignored; native10default versus6partial
passes/4failures, aggregate-memory10passes each, spill8/6 and numeric11/1 each.
The known resource failures remain open. The frozen2f9ad9f1 performance archive remains separate from these
changes; no new optimized speed claim is established.

[Prior fixed-width candidate and negative diagnostic](fixed-width-admitted-scan-2026-09-09.md),
[previous resident improvement](admitted-pipeline-q09-measurement-2026-09-09.md).

Corrected initialization probe69311 is terminal0: the complete canonical raw-Q9
computed projection now offers admitted preparation for16declared partitions,
without polling output. Held reservations1465142380bytes drop to1452141140 after
the descriptor and0 after plan/context drop. Peak1465143148 under4GiB query pool;
this includes initialized build state and is not full-query RSS certification.
The next optimized build freezes this tested candidate before runtime measurement.

## Frozen execution diagnostic

Release76303 completed in8m53s, freezing703b8564 with518verified source inputs.
Diagnostic74715 is terminal0: all3outputs are independently typed-correct. RawQ9
now uses16admitted input slots and takes1520.513059ms; native2709.797782ms remains
one-slot; resident1631.213323ms remains16slots. All aggregate outputs contain175rows
and no aggregate spill. Raw ingestion handles3,261,613rows in7,323batches, with
506.921msrouting/241.849msprocessing/756.634mstotal ingestion; this phase overlaps
upstream work and should not be added to cumulative join waits.

Scope peak19642445824bytes under48GiB, swap0, allOOM/max counters0. After-run source,
binary, driver, dataset and harness guards verify. A reversed-order two-block
comparison against2f9ad9f1 is running next. No full-suite performance claim yet.

## Matched two-block result — terminal24687

Control2f9ad9f1 and candidate703b8564 run once per mode in each of two fresh-process
blocks, reversed control/candidate order. Identical16threads, CPU0–15, default
disjoint ownership, raw/native4GiB query and12GiB process caps, resident32/48GiB,
GPU disabled. Timing is instrumented query time; resident preload is separate.
All12outputs match independent typed oracles. All720join traces complete without
unwinding, and every frontier matches the expected route: raw candidate16slots,
raw control1, native1, both resident16. Zero timeouts or process failures.

| Mode | Control mean ms | Candidate mean ms | Candidate/control | Block ratios |
|---|---:|---:|---:|---|
| Raw Parquet | 5939.239608 | 1504.320659 | 0.253285 | 0.248564,0.258097 |
| Native | 2730.525095 | 2734.512850 | 1.001460 | 0.990372,1.012565 |
| Resident CPU32GiB | 1601.950227 | 1569.095289 | 0.979491 | 0.950482,1.009539 |

Raw Q9 query time is74.67%lower in this diagnostic. Native is effectively unchanged;
the resident interval is not estimated from these two blocks. This does not prove
DuckDB leadership, a protected-query regression bound or full-provider acceptance.
The result estimates the schema/cause/probe-source delta atop the admitted fixed
scan candidate; the combined path still depends on the earlier projection/join
and decoder capability work.

Scope peak16310198272bytes,48GiB cap,swap0,zeroOOM/max. After-run source/binary,
dataset, driver and harness guards pass. Correctness archive41files, diagnostic188
and paired252files verify518inputs. Formatting and whitespace checks pass.

Full canonical SF10 provider screen52333 is terminal1; independent audit42325
is terminal0. All336completed outputs validate;252/264measured pairs pass.
Raw and Iceberg complete; native and Lance retain documented failures.
[Full provider results](build-schema-admission-provider-screen-2026-09-09.md) and
[next CPU attribution](build-schema-bottleneck-attribution-2026-09-10.md) preserve
conditions, failures and implementation sequence. No leadership is certified.

[Correctness archive](benchmarks/2026-09-09-build-schema-admission/manifest.json),
[execution diagnostic](benchmarks/2026-09-09-build-schema-admission-q09-diagnostics/manifest.json).
