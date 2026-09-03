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

## Chaos battery: COMPLETE — 5,600/5,600 passed, 0 mismatches

| batch | trials | seed | fixture | passed | mismatch | genuine-disk | missed-injection |
|---|---|---|---|---|---|---|---|
| A | 2000 | 20260902 | tpch-10mb | 2000 | 0 | 1784 | 0 |
| B | 2000 | 987654321 | tpch-10mb | 2000 | 0 | 1807 | 0 |
| C | 600 | 424242 | tpch-100mb | 600 | 0 | 520 | 0 |
| D | 600 | 777 | tpch-100mb | 600 | 0 | 540 | 0 |
| E | 400 | 1234 | tpch-10mb (QE_SPILL_DEBUG=1) | 400 | 0 | 370 | 0 |
| **total** | **5600** | | | **5600** | **0** | **5021** | **0** |

- All 5 batches `RESULT: PASS`, exit 0, each under its own
  `systemd-run --user --scope -p MemoryMax=8G` with `QE_MEM_CAP=6G`.
- Per-trial WHEN (QE_SPILL_CHAOS_FORCE_SPILL 0-2) and WHICH
  (_PARTITIONS: 10% decision-only / 40% all / 50% random subset) mixed
  by the harness's per-trial RNG; order-independent XOR checksum
  verified per trial by the harness (prints diagnostics + exit 1 on any
  mismatch — none occurred).
- Batch E additionally captured write-vs-read spill checksums:
  **34,528 `hash-check-ok` lines, 0 `HASH-MISMATCH`.**
- Logs: `.scratch/sjc3-001/chaos/batch_{A..E}.log`, `driver.log`.
- Chaos wall time total: ~5.2 min (~19.7-181 ms/trial).

## Q12 full-query battery, wave 1: 220/220 cell-exact, 0 wrong

- 2 lanes x 110 cold trials (fresh `serve` per trial, pinned binary,
  native tables, `--memory-limit 16M`, QE_SPILL_DEBUG=1, per-trial
  TMPDIR isolation, each lane under `systemd-run --user --scope
  -p MemoryMax=12G`, `QE_MEM_CAP=8G`).
- **220 pass / 0 wrong / 0 nospill / 0 error.** Every single trial
  verified to fire `execute_spill_path` (220/220 START lines); every
  result cell-exact vs the archived oracle (MAIL,353822,529784 /
  SHIP,352224,530051).
- Write-vs-read spill checksums: **27,280 `hash-check-ok`
  (124/trial = 62 spilled build + 62 probe partitions), 0
  `HASH-MISMATCH`.**
- Counting the spilling 16M probe trial: full-query class so far
  0 wrong / 221 spilling trials.
- Wave 2 launched to strengthen this leg (0/221 alone would still be
  47% likely even if the old 0.34% rate persisted): lane3 = 110 more at
  16M, lane4 = 110 at a tighter 12M (probe: spills, cell-exact,
  in_memory_partitions=2 rows=54464, spilled=62, total_matched=1765881,
  124 hash-check-ok) — more eviction/chunked-read-back pressure.
- Artifacts: `.scratch/sjc3-001/q12/lane{1,2}/` (per-trial serve.log +
  result.csv kept), `probe{64,32M,16M,12M}/`.

## Q12 wave 2 + FINAL VERDICT

- Lane 3 (110 @ 16M): 109 pass / 0 wrong / 1 infrastructure error —
  trial 1's serve hit `cannot bind 127.0.0.1:25001: Address already in
  use` at startup; NO query was ever executed, so it is excluded from
  the denominator (not a correctness event; artifacts kept at
  `q12/lane3/trial_1/`). 13,516 hash-check-ok / 0 HASH-MISMATCH.
- Lane 4 (110 @ 12M, tighter budget): 110/110 pass, 13,640
  hash-check-ok / 0 HASH-MISMATCH.

### Totals on the pinned post-rewrite binary (306dc15)

| class | trials | wrong | notes |
|---|---|---|---|
| chaos harness (mixed FORCE_SPILL/_PARTITIONS) | 5,600 | 0 | 5,021 genuine-disk, 0 missed-injection; 34,528 hash-check-ok / 0 mismatch (batch E) |
| Q12 full-query, native, spilling budgets (16M/12M) | 441 | 0 | 441/441 `execute_spill_path` confirmed; every trial cell-exact vs oracle; 54,684 hash-check-ok / 0 HASH-MISMATCH |
| **pooled** | **6,041** | **0** | 1 excluded infra error (port collision, no query ran) |

### Verdict (binomial 95% Clopper-Pearson)

**0 wrong / 6,041 verified trials on the post-rewrite spill path.**
- Full-query Q12-class leg alone: 0/441 → 95% CI [0%, 0.833%]
  (one-sided 95% bound 0.677%). At the archived 0.34% rate, seeing
  0/441 had probability ~22% — so this leg alone DISFAVORS but cannot
  exclude the old rate.
- Chaos leg: 0/5,600 → 95% CI [0%, 0.066%].
- Pooled: 0/6,041 → 95% CI [0%, 0.061%].

**This is a BOUND, not proof of absence.** The pooled bound leans on
the chaos leg, whose trials adversarially cover the same
spill/unspill machinery (forced WHEN/WHICH injection, real disk round
trips, order-independent checksums) but are NOT the original trigger
distribution (small parquet fixtures vs SF=10 native natural spill);
the full-query leg IS the original condition class (same join, same
oracle, archived total_matched=1,765,881 signature, genuinely spilling
budgets on current code) and independently bounds the rate at 0.83%.
No wrong answer, no checksum mismatch, and no missed injection was
observed anywhere in this task. The bug as previously measured
(1/290 = 0.34%, CI [0.01%, 1.91%]) did NOT reproduce on the rewritten
path.

- Nothing to hand to task 002: no artifacts of a failure exist because
  no failure occurred. Task closed clean.
- Cleanup: no stray serve/harness processes (verified via pgrep);
  pinned binaries never rebuilt (mtimes 08:13 throughout; task 003's
  concurrent spillable.rs work never entered this sample).
