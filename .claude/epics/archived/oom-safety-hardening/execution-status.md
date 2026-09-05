# Execution Status — oom-safety-hardening (+ spill-size-estimate-fix dependency)

Started: 2026-08-29T21:30:20Z (branch: epic/spill-size-estimate-fix)
Updated: 2026-08-29T23:59:00Z

## EPIC COMPLETE — all 7 tasks closed, 100%

Task 006's QA close-out re-verified everything at final HEAD (also
closing the external `spill-size-estimate-fix` epic's task 002 — one
shared run set):

- Harness `scripts/oom_cap_harness.sh`: **8/8 PASS** (agg 406/408MB,
  sort 845/858MB, native-scan 164/148MB completed; insert clean-refusal
  exit 2, ~30MB) — zero 137s, zero 134s.
- Q12 native: cell-exact 3/3, 0.18-0.21s, zero spill traces; 22-query
  `QE_SPILL_DEBUG=1` sweep: zero spill traces suite-wide.
- Suites: default 1317/0, lance 1382/0, gpu 1326/0, pulsar 1320/0
  (+32 each vs pre-epic 1285/1350/1294/1288); fmt clean.
- Perf: native sweep 5288ms 22/22 (band 5324-5667; was 8.20s); parquet
  cache-off 7.29-7.37s (historical 7.03-7.40s range); INSERT RSS
  ~1.59GB (band ~1.6-1.7GB).
- M1/M2 gates: PASS (one pre-existing verify-harness SQL-extraction bug
  fixed in scripts/cluster_local.sh, e286d33).
- CLAUDE.md updated (Memory Safety Rule epic-close bullet, Q12/stale
  number corrections); G1-G6 verdicts in epic.md close-out.

## Completed
- 001 harness + root-cause (CLOSED): unbudgeted execute_spill_path hash
  tables (~10-20x); incident = bare uncapped repro run pre-hook
- 007 join spill-path hash-table budgeting (CLOSED)
- 002 agg streaming two-phase reservation (CLOSED)
- 003 sort streaming + streamed merge delivery (CLOSED)
- 004 native streaming scan into spilling consumers (CLOSED)
- 005 INSERT/CTAS admission check (CLOSED)
- 006 QA close-out (CLOSED)
- spill-size-estimate-fix 001 + 002 (external epic, CLOSED, 100%)

## Open items carried out of the epic (by design)
- ~0.34% spilling-INNER-join duplicate-counting bug (own future task)
- rollup last-ULP flake (pre-existing; own small task)
- Q4 SEMI-join spill / Q13 SF=100 rename error / spill-dir collision
  (pre-existing, documented)
