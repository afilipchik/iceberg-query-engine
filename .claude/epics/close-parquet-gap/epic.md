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
