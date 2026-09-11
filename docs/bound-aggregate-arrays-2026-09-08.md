# Bind invariant aggregate array metadata once — September 8, 2026

The live grouped update path validated aggregate argument arity/types once at
batch entry, then repeated arity/type/extent validation across all arguments for
every selected row. Current source and the local DuckDB/ClickHouse comparison in
aggregate-update-boundary-2026-09-08.md motivate separating invariant batch binding
from per-row SQL updates. The isolated contribution has not yet been measured.

StateRows::bind_arrays now returns private BoundArrayInputs. It borrows immutable
ArrayRef storage, retains the exact StateRowLayout Arc and records the validated
common extent. Binding validates all argument types and lengths before group
mutation. It allocates no vector or buffer. Each update checks layout identity
and row range in constant time, then performs the unchanged per-cell decode and
transactional update. The token cannot be constructed externally or detached from
its arrays' lifetime. It carries no costing estimate or semantic assumption about
values, uniqueness or dictionary codes.

NULL/dictionary resolution, decimal arithmetic, overflow handling and selected
value admission remain in the existing kernels. State scratch/commit ordering,
new-group rollback, original selection order/multiplicity and the first-unapplied
spill cursor remain unchanged. This does not transpose aggregate loops or make
updates allocation-free. Typed direct accessors and batch kernels remain separate
future work with stronger transaction requirements.

Only state_rows.rs and group_rows.rs change against frozen e6a60347. A new
regression independently checks COUNT3/SUM12 for selection[2,0,2,1] over[2,NULL,5].
It checks allocation-free binding, wrong arity/type/extent, a same-shaped foreign
layout, rollback after invalid input row, prior-state preservation and full lease
cleanup. Existing encoding, exact decimal spill, multi-partition and input-error
coverage remains required.

Focused26736:129pass,0fail,1ignored component benchmark,877filtered,2.64s.
Broad17045 terminal101:996 library passes/11 ignored,14 selected integration
passes, native9pass/1unchanged383984-byte join refusal at256KiB. Library execution
15.39s; no selected integration skips. Formatting and whitespace checks pass.
Release62889 completed in8m43s, locked/offline lance,gpu under48GiB/one build
job. All493 frozen inputs verified. Binary8670b94caa1f524d9510fe8d86f934a8f7dfab31b29707fce0d40412cac88ede.
Paired driver8583 is terminal1 under a new48GiB scope.
Prepared drivers compare against e6a60347 using the same four-block/six-pair
canonical and custom-memory studies. Preserve failures and protected uncertainty.
No performance gain or acceptance is claimed before measurement.

Scratch evidence prefix: `.scratch/parallel-aggregate-input/bound-array-`.
An outer-join repeat-state review earlier in this turn did not establish a live
production reproducer; it is not a confirmed bug or justification for this change.

Test archive `benchmarks/2026-09-08-bound-array-tests/` has10 verified files,
including exact before/after source and patch. It records the release as running
at archival time; the completed freeze above supersedes that status.

## Completed paired measurement

Four fresh process blocks, six measured pairs plus gated warmup, same frozen
binaries/data/SQL and independently balanced startup/execution order. Canonical
SF10 uses16 threads,4GiB query/12GiB process. Custom memory Q1/Q6 uses4 threads,
4GiB query/8GiB process. Each block has fresh DuckDB calibration and independent
typed oracle checks. Ratios are candidate/control block-median geometric means;
intervals resample whole paired blocks.

|Workload/query|After/before|95% interval|Interpretation|
|---|---:|---:|---|
|Canonical Q1|0.99969|0.99199–1.00744|No confirmed change|
|Canonical Q2|1.04362|0.95172–1.16356|Protected regression unresolved|
|Canonical Q5|0.96711|0.94933–0.98523|3.29% improvement|
|Canonical Q10|1.00483|0.99404–1.01575|No confirmed change|
|Canonical Q19|1.00040|0.98188–1.02305|No confirmed change|
|Canonical Q20|0.97298|0.95502–1.00255|No confirmed change|
|Custom Q1|0.91291|0.89935–0.92808|8.71% improvement|
|Custom Q6|0.91703|0.78592–1.12376|Protected regression unresolved|

Q12/Q13 have no complete canonical block. Canonical:345 completed outputs all
correct,338 gated,7 late,9 timeouts,94 not_run. Custom:112 completed outputs all
correct/gated, zero failures. These are separate workloads, not a combined score.
Shared scope peak2,772,287,488bytes; max/oom/oom_kill zero. Custom scope snapshot
precedes canonical execution and reports294,531,072bytes. Neither is query RSS.

Prespecified follow-up22848 is terminal0 for canonical Q2 and custom Q6: eight fresh
blocks,12 measured pairs per block, unchanged conditions,20000 whole-block
bootstrap draws with seed20260908. Both original wide intervals remain preserved.
No repeated run-until-passing policy. Broader provider/resource gates remain open.

Both protected follow-ups complete96 measured pairs/208 engine outputs, all
correct and gated. Canonical Q2 ratio0.98669 (95%0.92742–1.03073); custom Q6
ratio0.96156 (95%0.88159–1.04276). Both upper bounds are below1.10; neither
establishes a speedup. The original studies are retained above.

Complete paired evidence and source snapshot:
`benchmarks/2026-09-08-bound-array-pairs/`. No whole-suite/provider acceptance
follows from these component and protected-query results.
