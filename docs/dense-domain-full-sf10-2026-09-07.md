# Frozen 653/662 balanced canonical SF10 — 2026-09-07

The full process exited0 and all **9680 requests across20 cells** pass recorded
typed comparison and the fresh10×DuckDB query-time ceiling. This is a completed
development comparison, not leadership or full resource certification. The
separate working-tree decimal-bound change is absent from both binaries.

Four predeclared startup/execution orderings each cover22 queries,10 steady
samples plus one warmup per engine. Features are Lance/GPU, execution is CPU,
affinity0–15,16 threads,40GiB query budget and48GiB process cap inside a96GiB
cgroup. Commands, plans, calibration, samples, outcomes and provenance are
preserved in the [member-verified archive](benchmarks/2026-09-07-dense-domain-full/).
The two binary identities and source snapshots are linked from the
[domain/arena report](dense-aggregate-domain-and-null-semantics-2026-09-07.md).

| Track | Suite662/653 | Query-ratio geometric mean | Suite662/DuckDB |
|---|---:|---:|---:|
| Raw Parquet |0.993511|0.989540|2.331445|
| Native |0.960798|0.984879|3.432811|
| Iceberg |0.995293|0.993626|0.316632|
| Lance |0.964116|0.979345|1.254983|
| Extra decoded IPC |0.948311|0.981246|0.426374|

Each entry is the geometric mean of the four order-specific ratios. The suite
ratio uses summed per-query medians within each ordering. Equal weighting of
the four required CPU tracks gives suite ratio **0.978298** and query ratio
**0.986833** against653. IPC is excluded from those weights. These four orderings
are not four independent sessions; no confidence-interval or three-session gate
is claimed. Provider/reference paths remain distinct. In particular, the Lance
reference disables its known-incorrect extension optimizer and measures direct
scan plus DuckDB aggregation, not stock extension pushdown performance.

## Flags and remaining gates

The full audit preserves every query whose mean or median exceeds1.10× control:

- Raw Parquet Q22, before-first/offset1: mean1.108418, median1.060306.
- Lance Q20, before-first/offset0; its flag did not repeat in the next two
  orderings. All four distributions remain in the full summary.
- Lance Q2 and Q11, before-first/offset1.

`.scratch/dense-domain-repair/run-current-protected.py` selects **all four flagged
track/query pairs** from the immutable full audit for50 samples across four
orderings against653,1632 requests. That [follow-up is complete](dense-domain-current-protected-2026-09-07.md):
all requests pass and no mean/median flag exceeds1.10 in any ordering. The
original flags are preserved but not confirmed by the longer comparison.
The separate historical follow-up compares against647 and retains the older
Lance Q17 regression; [it completed with1632 validated requests](dense-domain-historical-protected-2026-09-07.md)
and no mean/median ratio above1.10 in any ordering. No flag is waived merely by suite improvement.
The earlier component native Q1 control timeout remains a separate failure.

Raw/native still substantially trail DuckDB. The arena improved some expensive
finalization paths but did not solve shared expression cost or provider
materialization. The decimal-bound experiment remains separate. Its validation
also exposed a [fused aggregation spill-completion failure](fused-aggregate-budget-overshoot-2026-09-07.md)
at256KiB with the frozen helper, reinforcing that the SF10 budget is not a
general memory certificate. Public workloads, holdouts, lower budgets,
concurrency and broader provider/GPU resource contracts remain open.
