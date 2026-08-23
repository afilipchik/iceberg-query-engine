---
name: duckdb-parity-2
status: in-progress
created: 2026-08-23T00:38:20Z
updated: 2026-08-23T08:20:00Z
progress: 71%
prd: .claude/prds/duckdb-parity-2.md
github: (will be set on sync)
---

# Epic: duckdb-parity-2

## Overview

Four parallel investigations (query profiling on today's binary, two fix designs, a floor-cluster regression check) plus direct measurement found the SF=10 gap is smaller and more tractable than it first appeared: one measurement-default issue inflates the whole suite's apparent ratio for free money, and exactly two queries (Q13, Q16) have genuine, precisely-attributed, two-cause-each residue. Everything else is confirmed unregressed "architecture floor" from the prior six-epic program. This epic fixes the defaults/docs issue, ships both root-cause fixes for Q13 and Q16, and lands a staged (Stage 0+1 only) dense-group-id design for the one query (Q20, pending Q10 confirmation) that plausibly still needs it after the cheaper fixes land.

## Architecture Decisions

- **Fix the cheap, evidence-clear things first, in dependency order that lets later scope decisions use earlier measurements.** Task 002 (Q13's disjoint-threshold) must land and get measured before task 006 (dense-group-id Stage 1) finalizes whether Q13 needs a new data structure at all — the threshold fix may make it moot for Q13 specifically.
- **Join-pruning extension is pruning-only, not fusion.** The Q13 design explicitly evaluated extending 2a JoinAggregate fusion to Q13's shape and declined it: `streaming-fusion`'s own "Lever B" (full fusion) was already investigated and declined for the *simpler* Inner-only case. Don't re-open that.
- **Dense group-id remapping is NOT radix partitioning.** Ruling from the design investigation, load-bearing for anyone touching this code: radix partitioning (refuted, `examples/radix_bench.rs`) is about *scatter-then-probe* row routing; this design changes only the *payload* behind an existing, already-fine hash lookup (heap-boxed `Vec<AccumulatorState>` per group → flat `Vec<T>` indexed by a dense id). No new partitioning step, anywhere, in this epic.
- **Three join-output-pruning gates (`planner.rs:945`, `hash_join.rs:808`, `spillable.rs:216`) move together or not at all.** `spillable.rs`'s gate currently has *no* type/filter guard of its own — it only "works" today because the upstream gate never hands it an ineligible mask. Extending the upstream gates without adding the missing guard at `spillable.rs:216` creates a real schema/column-count mismatch bug, not a hypothetical one.
- **File-level task sequencing over parallelism where the project's own culture already prefers it.** `planner.rs`/`spillable.rs` (task 002 vs 003) and `hash_join.rs` (task 003 vs 004) are each touched by two tasks; run them sequentially per the six prior epics' "measurements serialized, commit-or-revert per lever" discipline, even though the specific line ranges rarely overlap.
- **Measure-first discipline applies to the threshold constant too.** `planner.rs:384`'s `2_000_000` floor is not assumed wrong; task 002 investigates before changing it, matching the exact "microbenchmark or kill-switch first" pattern every one of the six prior epics used.

## Technical Approach

### Methodology / defaults (touches: `src/storage/ipc_cache.rs`, `CLAUDE.md`, `scripts/`)
Fix the doc-comment/actual-default mismatch in `ipc_cache.rs`; make the benchmark path's premise unambiguous (either auto-build sidecars by default with a clear one-line disk-cost note, or make `benchmark-parquet`/`safe_benchmark.sh` print which premise it's using — the smaller, safer change). Add the like-for-like ratio to CLAUDE.md's SF=10 section (the one benchmark section currently missing it). Delete the stale Q9 "IPC loses to parquet" caveat. Re-baseline CLAUDE.md's SF=10 numbers post-dependency-upgrade.

### Q13 (touches: `src/physical/planner.rs`, `src/physical/operators/{hash_join,spillable}.rs`)
Two independent fixes, sequenced:
1. **Disjoint-aggregation threshold** — measure whether SF=10's c_custkey range (1.5M, just under the current 2M floor) genuinely benefits from disjoint-mode the way SF=100's 15M range does (doc comment at `planner.rs:357` names this exact query as the mechanism's motivating case); extend the floor if the evidence supports it.
2. **Join-output-pruning + runtime-filter extension to filtered LEFT/RIGHT/FULL joins** — the fully-specified design from this epic's investigation: new gate condition (`join_type ∈ {Inner,Left,Right,Full} AND (filter.is_none() OR no subquery in filter)`) applied identically at all three gates; force-keep any column the ON-clause filter itself references, regardless of downstream need; fix `create_combined_batch`'s stale-schema bug (`hash_join.rs`, currently reads `self.combined_schema`, never updated by `set_retained`); port `probe_keep` pruning into the ~5 outer-join gather branches that currently ignore it despite the parameter already existing.

### Q16 (touches: `src/physical/operators/hash_join.rs`, `src/physical/operators/hash_agg.rs`)
Two independent fixes:
1. **Anti-join parallel-probe eligibility** — investigate why `JoinType::Anti` is excluded from the batch-parallel-probe gate (design choice vs. oversight); if safe, extend it. This is the larger of Q16's two causes (55.5% of wall time, single-threaded 8M-row probe next to a 32-way-parallel second join in the same query).
2. **`distinct_set` hasher swap** — `std::collections::HashSet<GroupValue>` (SipHash) → `hashbrown::HashSet` (zero new dependency; `hashbrown` is already a direct `Cargo.toml` dependency; every other hash table in both aggregation files already uses it). Real but a minority contributor to Q16's cost — land regardless of its individual measured impact.

### Dense group-id remapping, Stage 0+1 only (touches: new code in `src/physical/morsel_agg.rs`, `examples/`)
Kill-switch microbenchmark first (`examples/dense_group_id_bench.rs`, same idiom as `radix_bench.rs`/`gpu_price_bench.rs`). If it clears, implement the single-int/date-column general-function flat accumulator (`FlatAgg` enum + `HashMap<u64,u32>` id table replacing `HashMap<u64,Vec<AccumulatorState>>` in the raw-single-column tier), scoped to whatever queries the post-002/003/004 measurement still shows needing it (expected: Q20; Q10 only if `PLAN_DEBUG` confirms it's still single-column after `GroupKeyReduction` — plausible it already isn't). Stage 2 (dense-merge payload consolidation) and Stage 3+ (multi-column/string keys) are explicitly out of scope for this epic — see the PRD.

### QA / close-out
Full suite (default/lance/gpu/pulsar), cell-exact SF=10 AND a confirmatory SF=100 sweep, benchmark re-measurement, CLAUDE.md update, epic close-out in the established six-epic house style (headline before/after table, named residues, honest gate-met/not-met accounting).

## Implementation Strategy

Sequenced by file-overlap and by which measurements gate later scope decisions, not by a fixed phase count:
1. Methodology/docs (001) — independent, land first, cheap, de-risks every subsequent measurement by making the premise unambiguous.
2. Q13 threshold (002) — independent, land before 006 so Stage 1's scope decision has real data.
3. Q13 pruning (003) and Q16 anti-parallel (004) — each conflicts with a neighbor on file overlap (003↔002 via planner.rs/spillable.rs, 003↔004 via hash_join.rs); sequence rather than parallelize per house culture.
4. Q16 hasher (005) — fully independent, land any time.
5. Dense group-id Stage 0+1 (006) — depends on 002's outcome for scope.
6. QA close-out (007) — last, depends on everything.

Every lever: implement → cell-exact validate → benchmark → commit-or-revert, through `scripts/claude-safe-build.sh`.

## Task Breakdown Preview

- 001: IPC-cache defaults/documentation fix + CLAUDE.md re-baseline (parallel: true)
- 002: Q13 — disjoint-aggregation threshold investigation + fix (parallel: false)
- 003: Q13 — join-output-pruning + runtime-filter extension to filtered outer joins (parallel: false)
- 004: Q16 — anti-join parallel-probe investigation + fix (parallel: false)
- 005: Q16 — distinct_set hasher swap (parallel: true)
- 006: Dense group-id remapping, Stage 0 + Stage 1 (parallel: false)
- 007: QA close-out — full suites, cell-exact SF=10+SF=100, docs, epic close (parallel: false)

Total tasks: 7
Parallel tasks: 2
Sequential tasks: 5
Estimated total effort: 15-24 hours (measurement wall time and the M-effort join-pruning port dominate)

## Dependencies

- 003 conflicts_with 002 (both touch `planner.rs`/`spillable.rs`) and conflicts_with 004 (both touch `hash_join.rs`) — sequence, don't parallelize.
- 006 depends_on 002 (scope decision needs 002's measured outcome).
- 007 depends_on everything (001-006).
- 001 and 005 are fully independent and can run any time, including in parallel with everything else.

## Success Criteria (Technical)

- G1: a plain benchmark run's cache premise is unambiguous (from output or docs); CLAUDE.md's SF=10 section states the like-for-like ratio.
- G2: Q13 total time at SF=10 measurably improved from the 415-500ms baseline; both fixes attempted and individually measured; full closure to DuckDB parity not required (the design's own risk note already flags Q13 may retain a residual double-gather cost).
- G3: Q16 total time at SF=10 measurably improved from the 153-224ms baseline; anti-join fix attempted and measured; hasher swap landed.
- G4: 22/22 cell-exact at SF=10 and SF=100, full suite green in all 4 feature combinations, no regression anywhere.
- G5: dense-group-id Stage 0 kill-switch verdict recorded; Stage 1 implemented only if the verdict and post-002 measurement both support it on a named query.

## Estimated Effort

- 001: S (0.5-1 day, mostly docs + one small code/default change).
- 002: S-M (0.5-1.5 days: measurement first, then a small, evidence-justified change).
- 003: M (per the design's own estimate: ~150-200 lines across 3 files, 1-1.5 days incl. new test fixtures).
- 004: S-M (0.5-1.5 days: investigate the gate, extend if safe).
- 005: XS (a few hours: one type swap, one file).
- 006: M (Stage 0 microbenchmark ~0.5-1 day; Stage 1 implementation ~2-3 days if the kill-switch clears).
- 007: S-M (0.5-1.5 days: full suite + both scales + docs).
- Total: one to two focused working sessions, matching the pace of the six prior epics in this program.

## Tasks Created
- [x] 001.md - IPC-cache defaults/documentation fix + CLAUDE.md re-baseline (parallel: true)
- [x] 002.md - Q13: disjoint-aggregation threshold investigation + fix (parallel: false)
- [x] 003.md - Q13: join-output-pruning + runtime-filter extension to filtered outer joins (parallel: false)
- [ ] 004.md - Q16: anti-join parallel-probe investigation + fix (parallel: false)
- [x] 005.md - Q16: distinct_set hasher swap (parallel: true)
- [x] 006.md - Dense group-id remapping, Stage 0 + Stage 1 (parallel: false)
- [ ] 007.md - QA close-out — full suites, cell-exact SF=10+SF=100, docs, epic close (parallel: false)

Total tasks: 7
Parallel tasks: 2
Sequential tasks: 5
Estimated total effort: 15-24 hours

## Phase 1 checkpoint (2026-08-23)

Tasks 001, 002, 005 shipped (branch `epic/duckdb-parity-2`, not yet merged
to main). Full suite green throughout (988 tests, 0 failed) after every
commit and again on the fully-combined state; `disjoint_aggregation_
matches_plain_aggregation_exactly` and `tests/duckdb_validated.rs`
(cell-exact) unaffected. All numbers below are clean, serialized SF=10
`safe_benchmark.sh` runs (3 iterations, one premise at a time — an
earlier attempt to run both premises concurrently produced visibly
contended, discarded numbers, e.g. Q16/Q22 going the wrong direction
under cache-on; the project's own "measurements serialized" rule holds).

| | cache-off total | like-for-like (vs 4.37s) | native ratio | cache-on total | like-for-like | native ratio |
|---|---|---|---|---|---|---|
| Before phase 1 | 8.86s | 2.03x | 2.88x | 5.99s | 1.37x | 1.80x |
| After phase 1 | **7.32s** | **1.68x** | **2.2x** | **5.79s** | **1.32x** | **1.7x** |

Q13 (task 002's target): 457.7ms/3.4x → **290.0ms/2.1x** (cache-off,
-36.6%); 275.5ms/2.0x (cache-on). Matches task 002's isolated
measurement (-40.2%, 223ms→31ms on the merge phase specifically) within
normal run-to-run noise once folded into the full 22-query sweep.

Q16 (task 005's target): 194.9ms/4.6x → 199.0ms/4.7x (cache-off,
essentially flat, as expected — the hasher swap was never the dominant
cause); 201.4ms/4.7x (cache-on). Its larger cause (anti-join running on
1 of 32 threads) is still open, task 004.

Task 002 also surfaced a new, small, out-of-scope finding for task 006:
at SF=100, `finalize_disjoint_states` still pays real merge-machinery
cost (~205ms/iter) for a single oversized (>65,536-group) disjoint
worker state — a targeted follow-up, not evidence the larger
dense-group-id rewrite is needed. Recorded in `002.md` and
`updates/002/stream-A.md`.

Remaining: 003 (Q13 join-pruning), 004 (Q16 anti-join parallelism), 006
(dense group-id Stage 0+1, now informed by 002's outcome — Q13 no
longer needs it), 007 (QA close-out).
