---
name: duckdb-parity-2
status: completed
created: 2026-08-23T00:38:20Z
updated: 2026-08-23T10:30:00Z
progress: 100%
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
- [x] 004.md - Q16: anti-join parallel-probe investigation + fix (parallel: false)
- [x] 005.md - Q16: distinct_set hasher swap (parallel: true)
- [x] 006.md - Dense group-id remapping, Stage 0 + Stage 1 (parallel: false)
- [x] 007.md - QA close-out — full suites, cell-exact SF=10+SF=100, docs, epic close (parallel: false)

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

## Epic close-out (2026-08-23)

All 7 tasks shipped and validated on branch `epic/duckdb-parity-2`
(commits `ff3fa97`..`9042a64` for 001-006, plus this task's docs/archive
commits). Full suite green in **all four feature combinations** (default
995/0/1, lance 1059/0/2, gpu 995/0/1, pulsar 998/0/1 — passed/failed/
ignored), 22/22 CELL-EXACT at SF=10 **and** SF=100, `cargo fmt --all
-- --check` clean, M1 + M2 distributed gates PASS.

### Headline: whole-program before/after (SF=10, both cache premises)

| | cache-off total | like-for-like | native ratio | cache-on total | like-for-like | native ratio |
|---|---|---|---|---|---|---|
| Epic start (pre-001) | 8.86s | 2.03x | 2.88x | 5.99s | 1.37x | 1.80x |
| After phase 1 (001+002+005) | 7.32s | 1.68x | 2.2x | 5.79s | 1.32x | 1.7x |
| **Epic end (all 6 tasks, this close-out)** | **7.03s** | **1.67x** | **2.1x** | **5.75s** | **1.36x** | **1.7x** |

Net epic-wide: cache-off **-20.7%** (8.86s→7.03s), cache-on **-4.0%**
(5.99s→5.75s) — cache-off moved more because it's the premise where
Q13/Q16's costs aren't already partly masked by the IPC sidecar path.
Like-for-like DuckDB reference re-measured fresh this session (4.22s,
consistent with the 4.18-4.37s band seen across this epic's several
measurement sessions — normal run-to-run system noise, not a moving
target).

### Q13 and Q16: this epic's two named targets

| query | before (PRD band) | after (tight, best-of-8, this task) | improvement |
|---|---|---|---|
| Q13 | 415-500ms | cache-off 259.9ms avg / cache-on 223.0ms avg | **~37-48%** |
| Q16 | 153-224ms | cache-off 131.4ms avg / cache-on 114.9ms avg | **~23-49%** |

Q13's win came almost entirely from task 002 (disjoint-aggregation
threshold, 2M→1M floor, -40.2% on its own controlled measurement); task
003's join-output-pruning + runtime-filter extension shipped correctly,
fully tested, and is now generally available for filtered outer joins —
but delivered a measured **negligible** additional effect on Q13
specifically, because the pre-existing `ProjectionPushdown` rule had
already cut its join inputs to 4 columns before task 003's own gate ever
ran. Q16's win came almost entirely from task 004 (anti-join batch-
parallel probe, an oversight fix — `JoinType::Anti` simply wasn't in the
gate `Inner`/`Left` matched — 41.5ms→6.2ms on the 8M-row probe itself,
~21% total-query, this epic's single largest individual win) with a
secondary, confirmed win on **Q22** (~18% faster, same VHT-served
unfiltered-anti-join shape); task 005's hasher swap landed as required
but was, as predicted going in, a minority contributor.

### G1-G5 (PRD success criteria): all MET

- **G1 (methodology)** — MET. `ipc_cache.rs` doc now matches the true
  `Auto` default; `benchmark-parquet` and `safe_benchmark.sh` both print
  the active cache premise; CLAUDE.md's SF=10 section states the
  like-for-like ratio (1.67x/1.36x) alongside native, matching the SF=100
  four-way matrix's format.
- **G2 (Q13)** — MET, not fully closed (PRD explicitly doesn't require
  full DuckDB parity). Measurably improved (~37-48%); both fixes
  attempted and individually measured, including the honest negative
  result on task 003's specific contribution. Named residue: Q13
  permanently cannot take the direct u32 match-emission fast path
  (`hash_join.rs`'s `u32_path`, gated `filter.is_none()`) so it always
  pays a double gather of the wide `o_comment` filter column — evaluated,
  scoped out (join→aggregate fusion for this shape was declined, per
  `streaming-fusion`'s prior finding for the simpler Inner-only case).
- **G3 (Q16)** — MET. Measurably improved (~23-49%); anti-join fix
  attempted and measured as a real, large win; hasher swap landed
  regardless of its individual size, as required.
- **G4 (no regression)** — MET. 22/22 cell-exact at SF=10 AND SF=100 (this
  task's own fresh validation, not carried over from an earlier session);
  full suite green in all 4 feature combinations; benchmark sanity
  re-run and reported (SF=10 dual-premise + SF=100 confirmatory sweep,
  Q9=11.21s/Q18=4.86s both unregressed).
- **G5 (forward-looking)** — MET. Dense-group-id staged design is
  committed (`examples/dense_group_id_bench.rs`, task 006's design notes).
  Stage 0 kill-switch **CLEARS** (24.5-44.5% win, 1M-50M groups,
  1-aggregate shape — with an honest nuance recorded: the win degrades to
  +10.8% at 2 aggregates and reverses to -9.9% at 3, so any future Stage 1
  should scope narrowly). Stage 1 correctly **did NOT implement**, per the
  gate's own rule ("Stage 0 clearing alone is not sufficient... don't
  force Stage 1 against a query that doesn't need it") — post-fix
  measurement showed neither Q10 nor Q20 reaches the boxed `raw_groups`
  tier Stage 1 would replace; both already bypass it via the independently
  -shipped, leaner `raw_sums` tier. A smaller, directly-evidenced fix
  shipped instead: `finalize_disjoint_states`' single-state fast path
  (SF=100 Q13 merge step ~205ms/iter → ~168ms/iter).

### New this task: Iceberg-table and CPU/GPU-split benchmarks

**Iceberg vs plain parquet (SF=10)**: engine 8.325s vs DuckDB-`iceberg_scan`
6.745s = **1.23x** (row counts match all 22 queries). Reported alongside,
not instead of, the plain-parquet numbers (1.67x cache-off / 1.36x
cache-on) per this program's "report multiple premises separately"
convention. Notable: Iceberg's manifest/snapshot indirection costs the
engine only ~+18.5% over its own parquet baseline but costs DuckDB's
`iceberg_scan` ~+60% over its own like-for-like baseline — the
competitive ratio actually *narrows* under Iceberg. New committed script:
`scripts/iceberg_bench_compare.py`.

**CPU vs GPU split (SF=10, `--features gpu`)**: full-suite CPU-only
(7.03-7.17s) vs GPU-enabled single cold pass (7.87s, worse — expected,
first-touch-always-CPU + un-amortized async upload). Targeted warm
per-query measurement (Q1/Q6/Q14/Q15, GPU engagement independently
confirmed via `nvidia-smi` VRAM 1066→1572 MiB) found **no measurable
full-query win on any of them**, including Q1/Q6 which structurally
*are* eligible for offload. Root cause, confirmed rather than assumed:
the previously-documented "Q6 shape 58.7x, Q1 17.0x" numbers are from an
isolated kernel microbenchmark (`gpu_price_bench.rs`, synthetic
already-VRAM-resident data, no scan/decode overhead) — a real result at
the kernel level, but at the full-query level scan+decode dominates
Q1/Q6's wall time, not the final reduction, so the kernel speedup never
shows up end-to-end at SF=10. This is a genuine, well-evidenced
correction to how the GPU offload epic's own numbers should be read, not
a regression of anything — the GPU mechanism itself is unchanged and
still correctly never makes a query slower once warm.

### Residues (named as one class, matching this program's convention)

1. **Q13's permanent double-gather of `o_comment`** — architectural, out
   of scope (fusion evaluated and declined). The remaining lever, if ever
   revisited, is a Q13-specific fused filtered-join→aggregate path, which
   `streaming-fusion` already declined for the simpler Inner-only case.
2. **Dense-group-id Stage 1** — correctly deferred, not abandoned. Design
   and Stage 0 evidence live in the repo (`006.md`,
   `examples/dense_group_id_bench.rs`) for a future epic to re-open
   against a freshly-confirmed query; note the aggregate-count nuance
   (win at 1 agg, reversal at 3) when scoping it.
3. **GPU offload's kernel-vs-full-query gap** — the mechanism is
   correctly implemented and never regresses a query, but its documented
   speedup does not yet reach full TPC-H wall time at SF=10 because scan/
   decode dominates. Future work with a plausible payoff: GPU-side decode,
   or measuring at a scale where the aggregate is a larger wall-time
   share.
4. **Sidecar disk-cost policy** (2.6x parquet footprint) remains an
   explicit product decision, not a performance question — unchanged from
   the PRD's own Out of Scope note.

### Commits

`ff3fa97` (start) → `d9aeef4`+`6058ff7` (001) → `62900b7`+`169974e` (002) →
`ebf6b82`+`1383cb7`+`2371da3` (003) → `eba694d`+`5e44ca4` (005) →
`ee9997c`+`1540a9a`+`8c176d9`+`f9c7403` (006) →
`ab86423`+`642428d`+`9042a64` (004) → this task's docs/archive commits.

### Archival

Epic moved to `.claude/epics/archived/duckdb-parity-2/` as this task's
final step, mirroring `dependency-modernization`'s archival pattern
(`git mv`, commit `ee4414e`). Not merged to `main` — that decision and
action is left to the user/orchestrating session per this task's own
instructions.
