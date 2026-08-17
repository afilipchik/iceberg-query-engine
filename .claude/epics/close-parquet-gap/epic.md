# Epic: close-parquet-gap

Tasks (dependencies noted; measurement runs are SERIALIZED):

- [ ] 001 Attribute Q9 (+14.3s, the whale). Never profiled against the
      parquet-views baseline — it looked "at parity" only vs pathological
      native. HJ_TIMING/AGG_TIMING + plan diff vs DuckDB's EXPLAIN.
- [ ] 002 Attribute Q4 (+2.6s) — EXISTS semi-join, worst RATIO after Q13.
- [ ] 003 IPC sidecar cache at SF=100 (disk ok: 85GB sidecars, 5.4T free).
      At SF=10 it bought −13% total. Build, measure warm, decide.
- [ ] 004 Fix what 001 finds; gate: Q9 ≤ 16s, no regressions elsewhere.
- [ ] 005 Fix what 002 finds; gate: Q4 ≤ 2.5s.
- [ ] 006 QA: full suites both modes, SF=10 + SF=100 sweeps, cell-exact,
      docs + PARITY-PLAN updated, epic closed with before/after table.

Parallelization: 001/002 are code-reading + single profiled runs
(serialized measurements, interleaved analysis); 003's sidecar BUILD can
run while 001/002 code analysis happens (I/O heavy, done before any
measurement run starts).
