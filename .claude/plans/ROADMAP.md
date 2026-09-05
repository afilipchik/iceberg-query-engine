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
| 2 (done 2026-08-08) | CBO join ordering (DPsize + footer stats + range selectivity); join build-side concat-once fix; planner trusts optimizer orientation; parallel agg merge | 20/22 complete, 9 ≤10x, aggregate 32.3s vs 2.94s (log `20260808_011334`) |
| 3 | Parallelism above joins; semi-join pushdown (Q18); parallel HashAggregateExec merge | Q03/Q10/Q13/Q15/Q17/Q18 ≤10x |
| 4 | DelimJoin re-enable + decorrelation robustness | 22/22 finish, ≥18/22 ≤10x |
| 5 | True streaming spill + SF=100 certification (regen tpch-100gb first!) | 0 OOM under cgroup; 22/22 SF=100 correct |
| 6 | Iceberg reads via iceberg-rust (parallel track) | real Iceberg table incl. deletes queryable |
| 7 | SQL debt: INTERVAL units, ILIKE, TRIM chars, bitwise ops | test workarounds deleted |

## Standing rules

- `cargo fmt --all -- --check` before every commit.
- Memory safety is not optional; spillable operators only, no opt-outs.
- All 10x claims measured against the re-baselined DuckDB constants on identical
  data + queries. Re-run `scripts/duckdb_rebaseline.py` whenever either changes.

## Addendum — ground truth as of 2026-09-03

The 2026-08-07 sections above are now historical. Between 2026-08-08 and
2026-09-03 the following epics landed on `main` (each archived under
`.claude/epics/archived/<name>/` with per-task evidence; CLAUDE.md is the
authoritative running record): duckdb-parity-2, close-parquet-gap,
dependency-modernization, gpu-acceleration, native-tables-{foundation,
mutation,rollups,tiering}, native-table-pruning, join-order-stats-
hardening, runtime-filter-chaining, spill-join-correctness (1, 2, 3),
spill-size-estimate-fix, oom-safety-hardening.

**Memory safety / larger-than-memory (phase 5 above) — DONE and certified
2026-09-03** (`spill-join-correctness-3`): every covered operator spills
or refuses cleanly by name under a configured `--memory-limit`; the join
spill path covers INNER, SEMI and ANTI in both build orientations;
TPC-H SF=100 is **22/22 cell-exact on parquet at 64G, 8G and 1G** (six
queries genuinely spilling at 1G, zero write/read checksum mismatches)
and **22/22 on native tables at 100G**; the historical ~0.34% duplicate-
counting bug did not reproduce in 6,041 trials on the rewritten path
(bound [0%, 0.061%]). Three remaining boundaries, all clean named
refusals, all documented in CLAUDE.md "Current limitations":
LEFT/RIGHT/FULL join spill (Q20 at 256M), ON-clause-filter join spill
(Q21 at 256M), and over-budget NATIVE scans feeding joins (Q02/Q10/Q11/
Q15/Q20 at 1G on native; the parquet provider streams).

**Open items surfaced by the certification (candidate next epics, in
suggested order):**
1. In-memory `HashJoinExec` wrong answer: build-side-output SEMI/ANTI
   with `Dictionary(Int32,Utf8)` keys and repeated build keys marks one
   build row per distinct key (pinned `#[ignore]`d in spillable.rs
   tests). Correctness — highest priority.
2. Join spill path performance at scale: the whole probe side is
   materialized before probing and spilled partitions are processed one
   at a time on one thread (Q09 ~1,400s at 1G/256M SF=100). Stream the
   probe side through the partition writers; parallelize read-back.
3. `NativeTable::scan()` spill-aware for join consumers (the native-scan
   boundary above).
4. Outer-join spill and ON-clause-filter spill (design-level; PRD-scoped
   out of `spill-join-correctness-3`).
5. The phase-6/7 items above (Iceberg via iceberg-rust; SQL debt) remain
   as listed; check CLAUDE.md before assuming their status.

## Addendum — 2026-09-05: the three certification follow-ups are closed

All three items listed as "candidate next epics" above landed on `main`
between 2026-09-04 and 2026-09-05 (each archived under
`.claude/epics/archived/<name>/`; CLAUDE.md carries the mechanisms and
numbers):

1. **`hash-join-dictionary-semi-anti-fix`** — the in-memory
   `HashJoinExec` wrong answer for build-side SEMI/ANTI over Dictionary
   keys (one row per distinct key) is fixed at its confirmed mechanism
   (the generic candidate loop `break`ing after the first build entry);
   three sibling filtered-Semi/Anti defects in the same function were
   found by the audit tests and fixed; Dictionary keys now take the
   vectorized path; build-side marking is O(probe + build).
2. **`join-spill-streaming`** — the join spill path streams its probe
   side and its output and processes spilled partitions K-way under one
   budget: Q9 SF=100 @1G 222s (was ~1,650s); 600M-row-build SEMI/ANTI
   joins complete under the default 1G cap; every 2026-09-03 SF=100
   verdict reproduced faster and under smaller caps.
3. **`spill-boundaries`** — over-budget native scans stream into
   spillable joins and sorts (planner "spill-covered" routing, pre-pass
   before shared-CTE materialization); ON-clause filters are evaluated
   per candidate pair on the spill path; LEFT/RIGHT/FULL spill via
   preserved-side bitmaps + NULL-extended emission. **TPC-H SF=100 is
   22/22 cell-exact on parquet at 64G/8G/1G/256M and on native at
   100G/1G** — no named refusal remains on TPC-H at any tested budget;
   the spill path's only refusals are CROSS/SINGLE/MARK.

**Remaining known items (none are wrong answers or refusals on TPC-H):**
- Q9's whole-engine peak at a 1G budget is ~10.7GB (scan parallelism +
  in-flight channels, not the join) — bounded, but above the budget.
- The rollup last-ULP float flake (aggregate merge order) still exists
  as a rare test flake (documented since oom-safety-hardening 003).
- Native-scan admission still refuses raw dumps / filter-only / LIMIT-
  only shapes over an over-budget native table (by design; the result
  would have to be materialized for the client).
- Phase 6 (Iceberg via iceberg-rust) and phase 7 (SQL debt) above remain
  the next roadmap phases; check CLAUDE.md for their current status.
