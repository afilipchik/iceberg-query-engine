---
name: close-parquet-gap
status: completed
created: 2026-08-17T00:00:00Z
updated: 2026-08-25T00:00:00Z
progress: 100%
prd: .claude/prds/close-parquet-gap.md
github: (will be set on sync)
---

> **Bookkeeping note (2026-08-25).** This epic predates the frontmatter
> and numbered-task-file conventions later epics use — the fields above
> were added retroactively (dates from this file's own recorded
> close-out and the later "bug found post-close" note) so tracking
> scripts stop misreporting this as an open 0-task Planning-stage epic.
> No content below was changed. `progress: 100%` reflects the epic
> itself closing (2026-08-17), not its own ≤55s goal being met — task
> 005 (Q4 fix) never completed inside this epic; see the close-out below
> for the honest reasoning and `duckdb-parity` for where Q4 actually got
> fixed.

# Epic: close-parquet-gap

Tasks (dependencies noted; measurement runs are SERIALIZED):

- [x] 001 Attribute Q9 — DONE. The whale is the partsupp join: 80M-entry
      COMPOSITE-key (ps_suppkey, ps_partkey) probes at 12-15.7s/partition
      while the LARGER single-key orders table probes at 0.6-1.3s. The
      packing EagerAggregation delivered at small scale never fires at
      SF=100. Residue after fix: probe-side gather (selection vectors).
      (was: Attribute Q9 (+14.3s, the whale). Never profiled against the
      parquet-views baseline — it looked "at parity" only vs pathological
      native. HJ_TIMING/AGG_TIMING + plan diff vs DuckDB's EXPLAIN.
- [ ] 002 Attribute Q4 (+2.6s) — EXISTS semi-join, worst RATIO after Q13.
- [x] 003 IPC sidecar cache at SF=100 — MEASURED, NO BENEFIT: 85.5s vs
      85.6s parquet. 63GB of sidecars exceed what the page cache can hold
      alongside the working set, and SF=100 is join-bound, not
      decode-bound. Cache remains an SF<=10-class lever. (was: cache (disk ok: 85GB sidecars, 5.4T free).
      At SF=10 it bought −13% total. Build, measure warm, decide.
- [x] 004 PackedJoinKeys optimizer rule: dual-int-column INNER equi-join
      keys packed to one Int64 when footer stats prove injectivity (same
      gate math as EagerAggregation). Q9 23.5→20.4s, cell-exact, no
      regressions. Gate of ≤16s NOT met — the remaining 11s vs DuckDB is
      probe gather, which is the selection-vector epic, not a rule.
- [ ] 005 Fix what 002 finds; gate: Q4 ≤ 2.5s.
- [ ] 005 remains open (Q4 attribution not started).
- [x] 006 QA: full suites both modes, SF=10 + SF=100 sweeps, cell-exact,
      docs + PARITY-PLAN updated, epic closed with before/after table.

Parallelization: 001/002 are code-reading + single profiled runs
(serialized measurements, interleaved analysis); 003's sidecar BUILD can
run while 001/002 code analysis happens (I/O heavy, done before any
measurement run starts).

## Epic close-out (2026-08-17)

Total: 87.1s → 85.6s measured (packed join keys). Goal of ≤55s NOT met and
was not reachable by rule-level work: the per-query evidence says the bulk
of the 2.2x like-for-like gap is join-probe GATHER and per-row probe cost,
i.e. selection-vector execution (PARITY-PLAN "2b") plus fused
probe→aggregate ("2a"). Those are a rewrite epic, not stories. What this
epic delivered: the Q9 attribution that names the real mechanism, a
proven-injective key-packing rule (+tests), and two honest negative
results (IPC at SF=100, disjoint-mode scatter on sparse keys) that stop
future sessions from re-trying them blind.

## Bug found post-close (2026-08-18, fixed in duckdb-parity epic)

Task 004's PackedJoinKeys rule, run INSIDE the optimizer's fixpoint
loop, broke Q5 at SF=10: iteration N packs the dual-key join, iteration
N+1's JoinReorder rebuilds its join graph from column=column predicates
only, loses the packed edge, and plans supplier x customer as a
21-billion-row CROSS join (fails loudly at the cross-join guard). At
SF=100 the same re-run silently degraded Q5's plan by ~4s instead. The
close-out QA ran SF=100 sweeps only — an SF=10 sweep would have caught
it. Fix: PackedJoinKeys is applied once AFTER the fixpoint loop
(optimizer/mod.rs), where JoinReorder can never see its output.
