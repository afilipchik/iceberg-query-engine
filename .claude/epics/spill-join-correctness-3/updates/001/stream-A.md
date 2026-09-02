# spill-join-correctness-3 / task 001 — stream A

Recalibrate the ~0.34% duplicate-counting bug against the rewritten
(post-oom-safety-hardening) spill path. Measurement only.

## 2026-09-02 — setup

- Branch: `epic/spill-join-correctness-3`, working tree CLEAN.
- **Pinned commit: `306dc15541414cdcf47fb6086615e4f98c915718`**
  ("CCPM: spill-join-correctness-3 PRD + epic + 5 tasks..."), i.e. current
  main + the merged oom-safety-hardening/spill-size-estimate-fix rewrite
  (0659d3e) — exactly the code the recalibration must target.
- Build started (background, via `scripts/claude-safe-build.sh`):
  `cargo build --release --bin query_engine --example spill_chaos_harness`.
  Log: `.scratch/sjc3-001/logs/build.log`. Binaries will be COPIED to
  `.scratch/sjc3-001/bin/` and run from there — immune to `target/` churn
  from task 003's concurrent spillable.rs work; NO rebuild for the rest of
  this task.
- Read: 001.md spec, PRD req 1, archived spill-join-correctness 001/003
  (repro matrix: Q12 native serve + POST /sql, oracle MAIL,353822,529784 /
  SHIP,352224,530051; spill signature build_batches=916
  build_rows=1765881; rate math 1/290, CI [0.01%, 1.91%]), archived
  sjc-2 003 (chaos harness contract: QE_CHAOS_TRIALS/SEED/DATA_DIR,
  per-trial order-independent XOR checksum, exits 1 on any mismatch or
  missed injection), oom-safety-hardening 007 (hash-table budgeting +
  eviction + chunked read-back rewrite).
- Plan:
  1. Chaos: >=5,000 trials in batches (varying seed + fixture scale:
     tpch-10mb and tpch-100mb), each batch under
     `systemd-run --user --scope -p MemoryMax=` + `QE_MEM_CAP`. The
     harness itself mixes QE_SPILL_CHAOS_FORCE_SPILL (WHEN 0-2) and
     _PARTITIONS (10% decision-only / 40% all / 50% random subset) per
     trial; checksums verified per trial by the harness, full logs kept.
  2. Full-query: find the budget at which native Q12 REALLY spills on
     current code (40G no longer does, post estimate fix) by probing
     `serve --tables data/tpch-10gb-native --memory-limit <N>` with
     QE_SPILL_DEBUG=1, confirming `execute_spill_path START` +
     build_batches/build_rows signature; then >=200 trials, each
     cell-exact vs the archived oracle, hash-check lines captured.
  3. Binomial 95% CI verdict; artifacts preserved on any mismatch.

## Build pinned + budget probe

- Build finished clean (2m54s, only pre-existing dead-code warnings).
  Binaries copied to `.scratch/sjc3-001/bin/{query_engine,
  spill_chaos_harness}` — all trials run from there; NO rebuilds after
  this point. Tree still clean at `306dc15` when copied.
- Q12 native budget probe (1 cold trial each, QE_SPILL_DEBUG=1, under
  `systemd-run --user --scope -p MemoryMax=12G`, `QE_MEM_CAP=8G`):
  - 64M: NOSPILL (no `execute_spill_path`; fused-streaming agg only),
    answer cell-exact. Confirms the PRD's premise — the old 40G-natural
    spill is gone post-estimate-fix, and even 64M doesn't spill.
  - 32M: NOSPILL, cell-exact.
  - **16M: SPILLS** — `execute_spill_path START in_memory_partitions=2
    (rows=54707) spilled_partitions=62`, `probe collected:
    probe_partitions=8 probe_rows=15000000`, `DONE in_memory_matched=
    54707 spilled_matched=1711174 total_matched=1765881 elapsed=3.42s`,
    124 hash-check-ok / 0 HASH-MISMATCH, answer cell-exact
    (MAIL,353822,529784 / SHIP,352224,530051).
    total_matched=1,765,881 EXACTLY matches the archived epic's
    build_rows=1765881 signature (same join: filtered lineitem x 15M
    orders; 62/64 partitions through the real disk round trip) — this is
    the Q12-class spill signature on current code. **16M chosen as the
    trial budget.**
- Chaos batches launched (background driver, one capped scope per
  batch): A=2000@seed20260902/10mb, B=2000@seed987654321/10mb,
  C=600@seed424242/100mb, D=600@seed777/100mb,
  E=400@seed1234/10mb+QE_SPILL_DEBUG=1. Total 5,600.
- Q12 battery launched: 2 parallel lanes x 110 cold trials at 16M
  (=220; NOSPILL trials won't count, will top up if needed).
