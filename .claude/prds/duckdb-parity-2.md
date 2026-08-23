---
name: duckdb-parity-2
description: Close the residual SF=10 gap after 6 completed parity epics — measurement-default fix plus Q13/Q16's newly-attributed two-cause-each root problems
status: completed
created: 2026-08-23T00:38:20Z
---

# PRD: duckdb-parity-2

## Executive Summary

Six completed epics (`duckdb-parity`, `radix-execution`, `streaming-fusion`, `decode-path`, `ipc-default`, `perf-marathon`) drove the SF=100 like-for-like gap against DuckDB from 2.23x to ~1.0-1.2x between 2026-08-18 and 2026-08-20, all measured and gated at SF=100. None of that program's attribution work ever ran at SF=10, and a `2026-08-22` dependency upgrade (arrow 53→58.4, lance 0.23→10.0, sqlparser 0.52→0.62) was validated only by a whole-suite average, never per-query. This PRD is the result of a fresh, from-scratch SF=10 investigation (four parallel deep-dives: query profiling, two fix designs, a floor-cluster regression check) that found:

1. **A measurement-default bug, not a performance bug, explains most of the "drastically behind" appearance.** `QE_IPC_CACHE`'s true default (`Auto`: silently use a sidecar if one already exists, never build one) contradicts its own module doc comment ("default off"). Because this repo's `data/tpch-10gb` already had sidecars built on 2026-08-18, a "plain" benchmark run silently gets the cache-on premise — and even that run showed contamination from an apparent first-touch/noise effect. A genuinely forced `QE_IPC_CACHE=0` run measured **8.86s total (2.03x DuckDB-over-parquet, 2.88x DuckDB-native)** — the true cache-off number, and meaningfully worse than anything previously reported at this scale. The already-shipped, already-validated `QE_IPC_CACHE=1` mode measures **5.99s (1.37x / 1.80x)** on the identical data — closing most of the gap with zero new engineering.
2. **Q13 (worst outlier, 3.4-3.5x under either cache mode — the one query the cache doesn't help) has two separable, independently-fixable root causes, split almost exactly down the middle**: 44.7% join-side cost (its `LEFT OUTER JOIN ... ON ... AND o_comment NOT LIKE ...` shape is structurally excluded from join-output-pruning and the runtime-filter mechanism — both gate on `JoinType::Inner` only), and 55.3% aggregation-side cost (its inner `GROUP BY c_custkey` has a footer-stat range of 1.5M at SF=10, just under the existing disjoint-aggregation optimization's `2,000,000` lower bound — a mechanism whose own code comment names *this exact query* as its motivating case at SF=100, where the range is 15M and clears the floor easily).
3. **Q16 (second-worst, 2.8-4.6x) also has two separable causes**, but not the ones the prior research file's generic OLAP-engine findings assumed: 55.5% of its cost is a `NOT IN` anti-join that runs on a single thread while the query's other join uses all 32 (`JoinType::Anti` isn't in the batch-parallel-probe gate); the `COUNT(DISTINCT ps_suppkey)` per-group `std::collections::HashSet` (SipHash) is real but a minority contributor, not the dominant one.
4. Everything else in the suite sits in a 1.0-1.6x-under-cache / 3-7x-cache-off band the project's own six-epic history already calls "architecture floor," confirmed unregressed post-dependency-upgrade at both SF=10 and (spot-checked) SF=100.

## Problem Statement

The user asked for every slower-than-DuckDB query to be analyzed and a catch-up plan produced, then synthesized into one implementation plan. A naive per-query "why is X slow" pass would have re-derived (worse, with less evidence) conclusions the project already reached across six epics and a 67-finding competitive research sweep (`.claude/plans/research/wave2-olap-engines.json`) — or worse, mistaken a measurement artifact for a performance gap. The actual problem is narrower and more tractable than "the engine is drastically behind": one benchmarking/defaults issue inflates the apparent gap across the whole suite, and exactly two queries have genuine, well-attributed, fixable engine-side residue.

## User Stories

**As someone benchmarking this engine against DuckDB**, I want a plain `benchmark-parquet`/`safe_benchmark.sh` run to report an unambiguous, correctly-labeled number, so that I don't mistake a stale/absent sidecar cache for a performance regression.
- Acceptance: running with `QE_IPC_CACHE` unset against a dataset with no sidecars yet produces the same class of number as `QE_IPC_CACHE=1` after a documented one-command build step, not a silent, unlabeled worse number.
- Acceptance: CLAUDE.md's SF=10 section states the like-for-like (DuckDB-over-the-same-parquet) ratio explicitly, matching the SF=100 four-way matrix's existing convention.

**As the engine's maintainer**, I want Q13 and Q16 no longer to be outliers relative to the rest of the suite, so that the next benchmark sweep doesn't need a fresh multi-agent investigation to explain them.
- Acceptance: Q13's LEFT-join shape gets the same join-output-pruning and runtime-filter treatment every Inner-join query already has, with cell-exact correctness preserved including the "all candidate rows filtered out → c_count=0" edge case.
- Acceptance: Q13's `GROUP BY c_custkey` aggregation is evaluated (after measurement) for whether the disjoint-aggregation threshold should extend to cover SF=10's 1.5M range.
- Acceptance: Q16's anti-join is evaluated for parallel-probe eligibility, and its `COUNT(DISTINCT)` state uses the same hashbrown-backed hasher every other hash table in the codebase already uses.

**As a future contributor to this program**, I want the still-open "dense group-id remapping" item to have a concrete, staged, correctly-scoped design — not a repeat of the wave2 research file's generic estimates — so that the next epic can start implementing instead of re-investigating.
- Acceptance: a staged design exists (kill-switch microbenchmark → single-column general path → multi-column path), explicitly scoped away from what task-level fixes in this PRD already cover (Q13's aggregation side, once its cheaper threshold fix is evaluated, may not need this at all), and explicitly ruled distinct from the already-refuted radix-partitioning approach.

## Functional Requirements

1. Fix or clearly work around the `QE_IPC_CACHE` default/documentation mismatch; state both premises wherever benchmark numbers are published.
2. Remove the stale "IPC loses to parquet on Q9" caveat (superseded by perf-marathon's 2026-08-20 fix, never removed from the record).
3. Extend join-output-pruning and the runtime-filter mechanism to cover filtered `LEFT`/`RIGHT`/`FULL` joins (currently `Inner`-only across three gates that must move in lockstep: `planner.rs:945`, `hash_join.rs:808`, `spillable.rs:216`), per the fully-specified design produced during this investigation.
4. Investigate and, if the evidence supports it, extend the disjoint-aggregation hint's range floor (`planner.rs:384`, currently `2_000_000..=64_000_000`) to cover smaller dense-domain group keys.
5. Investigate and, if safe, extend batch-parallel join probing to `Anti` (and audit `Semi`) joins, currently excluded from whatever gate restricts parallel probing to `Inner`/`Left`.
6. Swap `AccumulatorState::distinct_set`'s `std::collections::HashSet` (SipHash) for a `hashbrown::HashSet`, matching every other hash table in `hash_agg.rs`/`morsel_agg.rs`.
7. Produce (this PRD's investigation already produced) a staged design for generalized dense-group-id/flat-accumulator aggregation; implement its cheapest, evidence-justified first slice if scope and measurement support it after items 3-6 land.
8. Re-baseline CLAUDE.md's benchmark sections (stale since 2026-08-17/18, pre-dating the 2026-08-22 dependency upgrade) with fresh SF=10 numbers, both premises stated.

## Non-Functional Requirements

- **Memory safety is never negotiable** (project mandate): no new opt-out flags; any new data structure must fit inside the existing spillable-operator budget/group-count-limit machinery.
- **Correctness over speed, always**: every lever validated cell-exact against DuckDB (`tests/duckdb_validated.rs`, `scripts/distributed_validate.py` conventions), not row-count-only — this project has a specific, documented history of wrong-answer bugs in exactly the join-pruning and aggregation-merge code this PRD touches, and every one of them was caught only by full-value comparison.
- **No bare `cargo build`/`test`/`bench`**: every build in this program runs through `scripts/claude-safe-build.sh` per CLAUDE.md's Sandboxed Build Rule.
- **Commit-or-revert per lever, measurements serialized**: match the established house style from all six prior epics.
- **No regression** on any of the 22 TPC-H queries, at SF=10 or SF=100, in any of the four feature combinations (default/lance/gpu/pulsar).

## Success Criteria

- G1 (methodology): a plain benchmark run's premise is unambiguous from its own output/logging; CLAUDE.md states both premises for SF=10.
- G2 (Q13): total time at SF=10 measurably improved from the current 415-500ms band (depending on cache premise) toward the suite's general 1.3-1.8x floor band; both the join-side and aggregation-side fixes attempted and their individual contributions measured and reported, even if full closure to DuckDB parity isn't reached (the join-pruning design's own risk note already flags this as possible).
- G3 (Q16): total time at SF=10 measurably improved from the current 153-224ms band; anti-join parallelism fix attempted and measured; hasher swap landed regardless of its individual impact size.
- G4 (no regression): 22/22 cell-exact at SF=10 and SF=100, full suite green in all four feature combinations, benchmark sanity re-run and reported.
- G5 (forward-looking): dense-group-id staged design is committed to the repo (this PRD's investigation product) with Stage 0/1 implemented only if Stage 0's kill-switch microbenchmark clears and post-fix measurement still shows a real gap on a concretely-named query (expected: Q20, pending Q10 confirmation) — not implemented speculatively.

## Constraints & Assumptions

- Investigation and gates are SF=10-scoped by default (the scale the user's original benchmark ran at, and the scale the 6-epic program never covered); a confirmatory SF=100 pass is required before epic close-out but is not the primary gate, matching how this program has always treated scale as a secondary axis.
- Assumes the existing profiling instrumentation (`AGG_TIMING`, `HJ_TIMING`, `HJ_PROF`, `PLAN_DEBUG`, `QE_AGG_PROF`, `QE_PRUNE_DEBUG`, `QE_JOIN_PRUNE`) remains accurate; where it doesn't cover a code path (confirmed gap: Q20's Semi/Left-join-dominated plan bypasses both `AGG_TIMING` and `HJ_PROF`), that gap is reported, not silently worked around.
- Assumes the machine's own measurement noise (empirically 15-25% run-to-run on 150-300ms queries under any concurrent load) means single-run deltas under ~10% should not be treated as signal; every gate re-measurement should be best-of-N on an otherwise-idle box, matching house convention.
- The disjoint-aggregation threshold extension (#4) is explicitly a "measure, then maybe change a constant" task, not a "the constant is obviously wrong" task — the existing `2_000_000` floor may reflect a real minimum-problem-size consideration, not just a range-density heuristic; treat it with the same evidence discipline as everything else in this program's six-epic history.

## Out of Scope

- **Dense group-id remapping Stage 3+ (multi-column/string keys) and Stage 4 (DISTINCT-aware flat state)**: real, designed (see the epic's Architecture Decisions), but large (L/XL) and speculative against queries not yet confirmed to need them post-Stage-1. Recommended as a follow-up epic once Stage 1 (if built at all) validates the pattern in production.
- **Generalized selection-vector execution ("2b generalized")**: named as "the next structural lever" in nearly every prior epic's close-out, and deprioritized every single time in favor of smaller targeted wins. Not resurrected here without fresh, specific evidence it's the binding constraint on a named query — none of this investigation's findings point at it.
- **Join→aggregate fusion for Q13's LEFT-join shape**: explicitly evaluated and declined by the join-pruning design (streaming-fusion's own "Lever B" was already investigated project-wide and declined for the *simpler* Inner-only case; Q13's shape is strictly harder).
- **Radix-partitioned joins or aggregation**: refuted by `radix-execution`'s own microbenchmark (`examples/radix_bench.rs`) on this hardware; nothing in this PRD's findings reopens that question, and the dense-group-id design is explicitly confirmed to be a different mechanism (payload flattening, not row scatter), not a re-litigation of it.
- **Sidecar disk-cost tradeoff policy** (2.6x parquet footprint): this PRD fixes the *default/documentation mismatch*, not the underlying policy question of whether to ship sidecars by default for all scales; that's a product decision, not a performance-engineering one, and is called out for the user/maintainer to decide explicitly rather than silently defaulted.
- **Bespoke parquet decoders**: refuted by `bespoke-decoders`'s own kill-switch microbenchmark (arrow-rs already at or better than a memcpy floor on the dominant column types).

## Dependencies

- Builds directly on six completed epics (`duckdb-parity`, `radix-execution`, `streaming-fusion`, `decode-path`, `ipc-default`, `perf-marathon`) and the `wave2-olap-engines.json` research sweep — no new external dependency, no new crate.
- Depends on the `dependency-modernization` epic's arrow-58/lance-10/sqlparser-0.62 upgrade already being merged to `main` (confirmed done, 2026-08-22).
- Depends on `scripts/claude-safe-build.sh` (the OOM-safe build sandbox) for every build in this program.
