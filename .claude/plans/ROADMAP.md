# Query Engine Development Roadmap

**Rewritten 2026-08-07.** The previous version of this file (dated 2026-01-28) was
stale in almost every claim (it predated the spec-query restoration, the Feb 2026
optimization work, and the honest re-baseline). History is in git.

## Ground truth (2026-08-07)

- **Queries**: all 22 TPC-H queries are now **spec-compliant** (Q02/Q04/Q14/Q16/Q18/Q19
  were previously simplified; restored in commit `d84f3a2`). Remaining intentional
  adaptations for generated data: Q09/Q20 use `'Part 1%'`, Q22 uses 2-digit phone codes.
- **Data**: `src/tpch/generator.rs` is spec-compliant (`o_custkey` sparse 1.5x range)
  and **byte-for-byte deterministic**. `tests/expected_results/*.csv` are only valid
  against data produced by the current generator. CI regenerates `data/tpch-1mb`.
- **Tests**: all suites green (incl. 156 DuckDB-validated + 128 comprehensive).
  CI now generates data before testing (previously the 156-suite never ran in CI).
- **DuckDB baseline**: re-measured 2026-08-07 on spec queries + spec data via
  `scripts/duckdb_rebaseline.py`; constants updated in CLAUDE.md and
  `scripts/safe_benchmark.sh`. Total DuckDB: **2.94s** at SF=10.
- **Engine at SF=10** (48G cgroup, logs `safe_benchmark_20260807_222225/222410/223235.log`):
  only **Q01 passes the 10x rule** (4.2x); Q04 (11.7x) and Q17 (10.9x) borderline.
  Never finish even with 1000x budgets: **Q05 (>48s), Q07 (>72s), Q09 (>250s), Q18 (>232s)**.
  100x-500x: Q03 12.7s, Q08 22.1s, Q10 29.6s, Q14 20.5s, Q19 17.9s, Q20 24.9s.
  25-80x: Q02, Q06, Q11, Q12 6.6s, Q13 4.7s, Q15, Q16, Q21 16.4s, Q22.

## Known-broken / not-integrated (verified in code)

- `FlattenDependentJoin::optimize` is a stub (`Ok(plan.clone())`) — DelimJoin stack dead.
- `SpillableHashJoinExec` fully materializes the build side before deciding to spill —
  the "never OOM" guarantee has a hole; no test exercises `memory_limit`.
- Join ordering is greedy with table-name heuristics; `cost.rs` unused.
- Morsel parallelism only applies to bare scan-aggregates; post-join aggregation is
  single-stream.
- Window functions: `OVER(...)` silently binds as a plain aggregate (wrong results).
- Iceberg: `IcebergScanExec` is dead code reading a non-spec JSON layout; no Avro dep.

## Plan (agreed 2026-08-07)

Full plan with per-task files/verification:
`~/.claude/plans/analyze-the-repo-understand-compressed-willow.md`

| Phase | Goal | Exit criterion |
|-------|------|----------------|
| 0 (done) | Stabilize tree, commit WIP, fix CI, re-baseline | all suites green; honest baseline logged |
| 1 (done) | Spill tests (caught 4 real bugs, fixed in `f7960da`); A/B of `3eb2b5a` → **KEEP** (Q02 −49%, Q11 −43%, rest neutral; logs `230905` vs `231007`); OVER() rejected | spill tests in CI |
| 2 | Cost-based join ordering (stats from Parquet footers, DPsize) | Q05/Q07/Q08/Q09 ≤10x |
| 3 | Parallelism above joins (multi-partition join output, parallel agg) | Q03/Q10/Q12/Q13/Q14/Q18/Q19 ≤10x |
| 4 | DelimJoin re-enable + decorrelation robustness | 22/22 finish, ≥18/22 ≤10x |
| 5 | True streaming spill + SF=100 certification (regen tpch-100gb first!) | 0 OOM under cgroup; 22/22 SF=100 correct |
| 6 | Iceberg reads via iceberg-rust (parallel track) | real Iceberg table incl. deletes queryable |
| 7 | SQL debt: INTERVAL units, ILIKE, TRIM chars, bitwise ops | test workarounds deleted |

## Standing rules

- `cargo fmt --all -- --check` before every commit.
- Memory safety is not optional; spillable operators only, no opt-outs.
- All 10x claims measured against the re-baselined DuckDB constants on identical
  data + queries. Re-run `scripts/duckdb_rebaseline.py` whenever either changes.
