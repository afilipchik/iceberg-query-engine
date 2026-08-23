---
issue: 006
stream: main
started: 2026-08-23T07:30:00Z
status: completed
completed: 2026-08-23T08:14:02Z
---

## Scope
See `.claude/epics/duckdb-parity-2/006.md`. Depends on task 002's outcome
(read first, per the task's own instructions): 002 lowered
`disjoint_group_hint`'s range floor 2,000,000 -> 1,000,000 and took Q13
SF=10 413.1ms -> 246.9ms (-40.2%). **Per the task file's own instructions,
this makes Q13 no longer this task's target** (redundant investment).
Candidates going in: Q20 (needs re-confirmation with better instrumentation
than the floor-cluster check achieved) and Q10 (only if `PLAN_DEBUG=1`
shows its GROUP BY is still multi-column after `GroupKeyReduction`).

## Verdict (headline)

- **Stage 0 (kill-switch microbenchmark): CLEARS**, decisively, for the
  1-aggregate shape that actually matters (Q13/Q20-like) — 24.5%-44.5%
  wall-time win across the full 1M-50M group sweep, dense and sparse
  domains alike. See full table below.
- **Q10 confirmation: needs NOTHING.** `GroupKeyReduction` already
  collapses its 7-column GROUP BY to a single `c_custkey AS __topk_key`
  column (confirmed live, `PLAN_DEBUG=1` + `QE_AGG_PATH=1`), and that
  single column is `[Sum(Float64)]`-shaped, so it *also* already uses the
  even-leaner `raw_sums` bare-pair tier, not the boxed `raw_groups` tier
  Stage 1 would touch. The aggregation phase itself is ~0.3-0.7% of Q10's
  wall time (~1ms of ~200ms; `[fused-agg]`/`[raw-sum-merge]` timing) — the
  other ~99% is join drain.
- **Q20 re-confirmation: needs NOTHING either.** Live re-measurement
  (`AGG_TIMING=1 QE_AGG_PROF=1`, SF=10) shows Q20's dominant aggregate —
  the decorrelated `SUM(l_quantity)` subquery — compiles to `Aggregate:
  group_by=[__pk], aggs=[SUM(l_quantity)]` where `__pk` is a single packed
  Int64 key (`(l_partkey * 131072) + l_suppkey`, an `EagerAggregation`-style
  pack). That is exactly `[Sum(Float64)]` shape, so it *already* uses the
  `raw_sums` fast tier (confirmed by the `[raw-sum-merge] 4459569 groups
  dense=false ...` print — the code comment right above that function
  literally says "Q20: 4.4M groups, exactly [Sum(F64)]", and it is
  current/accurate, not stale). Even so, the aggregation-specific cost
  (raw-sum-merge + group-eval) is only ~25-30ms of Q20's ~380-410ms total
  (~6-8%) — the dominant cost is the join/scan drain phase (`[fused-agg]
  drain(join+scan+send)`, ~150-200ms) feeding the fused join+aggregate,
  plus the SEMI/LEFT joins around it that the PRD's own floor-cluster
  check correctly flagged as bypassing `AGG_TIMING`/`HJ_PROF` (those are
  genuinely join-side, not aggregation-side, cost — confirmed, not
  worked around). Cross-checked against the wave2 competitive research
  file (`.claude/plans/research/wave2-olap-engines.json`): **every single
  one of its ~13 Q20 mentions classifies it under "B2" (join probe +
  output gather), never once under a "B3" (aggregation/HashMap-fallback)
  finding** — the "Q20 needs dense-group-id" hypothesis traces to
  `perf-marathon`'s `IDEAS.md` idea #6 alone (itself hedged: "No single
  lever"), not to the wave2 research's own aggregation-structure analysis.
  Three independent sources (live measurement, the shipped code's own
  comments, and the competitive research file) now agree.
- **Stage 1: does NOT proceed.** Stage 0's kill-switch clearing is
  necessary but not sufficient per the task's own gate — a concrete
  target query is *also* required, and neither candidate has one. Both
  Q10 and Q20 already bypass the exact tier (`raw_groups: HashMap<u64,
  Vec<AccumulatorState>>`) Stage 1 would replace, via the pre-existing,
  even-leaner `raw_sums` bare-`(u64,f64)` representation. This is a
  complete, well-evidenced "no work needed" outcome, which this task's
  own instructions and this program's culture treat as equally valid to
  a positive result.
- **Separate small fix shipped instead**: task 002's out-of-scope SF=100
  finding (`finalize_disjoint_states` paying real shard/rehash cost to
  "merge" a single oversized disjoint-worker state with nothing) — see
  below. In-scope for this task (file is `morsel_agg.rs`), well-evidenced,
  low-risk, and directly informed by this task's own investigation.

## Q10 confirmation evidence

`PLAN_DEBUG=1 ./target/release/query_engine benchmark-parquet --path
./data/tpch-10gb --query 10 --iterations 1`:
```
Aggregate: group_by=[c_custkey AS __topk_key], aggs=[SUM((l_extendedprice * (1 - l_discount)))]
```
Single column, already reduced (the original SQL's `GROUP BY c_custkey,
c_name, c_acctbal, c_phone, n_name, c_address, c_comment` collapses to
`c_custkey`; the other six ride along as `__fd_N` functional-dependency
projections after the join, per `GroupKeyReduction`).

`QE_AGG_PATH=1` across all ~32 morsel workers: `[agg-path] combined=0
types=[Int64]` (every worker) — confirms a single raw Int64 group column
at the operator, not a multi-column or dictionary-combined key.

`AGG_TIMING=1 QE_AGG_PROF=1`, 3 iterations: `[raw-sum-merge] 276035
groups dense=true ...` fires (confirms the `[Sum(Float64)]` bare-pair
tier, not `raw_groups`); `[fused-agg] ... total: ~147-163ms` of a
~190-202ms query total, and the raw-sum-merge step inside that is
0.6-1.4ms. Aggregation is not the bottleneck at all here; it was already
this cheap before this task.

## Q20 re-confirmation evidence

`PLAN_DEBUG=1`, SF=10, shows the full plan; the relevant fragment:
```
Project: [l_partkey, l_suppkey, 0.5 * SUM(l_quantity) AS __scalar_result]
  Project: [l_partkey, l_suppkey, (0.5 * SUM(l_quantity))]
    Project: [BITWISE_RIGHT_SHIFT(__pk, 17) AS l_partkey, BITWISE_AND(__pk, 131071) AS l_suppkey, SUM(l_quantity)]
      Aggregate: group_by=[((CAST(l_partkey AS Int64) * 131072) + CAST(l_suppkey AS Int64)) AS __pk], aggs=[SUM(l_quantity)]
        INNER Join on: p_partkey = l_partkey
          ... part (LIKE 'Part 1%') ...
          ... lineitem (l_shipdate range) ...
```
`QE_AGG_PATH=1`: `[agg-path] combined=0 types=[Int64]` — single packed key.

`AGG_TIMING=1 QE_AGG_PROF=1`, 3 iterations, SF=10 (Q20 total 355-412ms):
```
[raw-sum-merge] 4459569 groups dense=false shard: 13.1ms; merge+build: 12.3ms; total: 25.4ms
[agg-prof] group-eval: 76.1ms; agg-eval: 1.3ms (cumulative ACROSS ~32 workers, not wall)
[fused-agg] drain(join+scan+send): 201.4ms; workers done: 202.1ms; merge ... -> out: 26.4ms; worker busy sum: 1174.1ms; total: 228.5ms
```
`[fused-agg] total` (228.5ms of 412.3ms, 55%) covers the `part ⋈
lineitem` join AND the aggregate build+merge together; within it, the
merge/aggregate-specific slice (`[raw-sum-merge]`, wall) is ~25ms (~6% of
Q20's total) — the rest of that 228.5ms is join probe/scan drain. The
other ~184ms of Q20's wall time (outside `[fused-agg]`) is the SEMI/LEFT
joins around `partsupp`/`supplier`/`nation` plus load — genuinely
join-side, matching the PRD's own note that this part of the plan
bypasses `AGG_TIMING`/`HJ_PROF`. Group count (4.44-4.47M at SF=10) scales
consistently to the `perf-marathon` IDEAS.md idea #6 SF=100 figure of
44.6M (~10.1x, matching the scale factor).

Wave2 research file cross-check: `grep -n "Q20"
.claude/plans/research/wave2-olap-engines.json` returns 13 lines, every
one tagged `"maps_to": "B2 ..."` or citing Q20 under a B2 (join
probe/gather) `est_payoff`. Not one of wave2's B3 (HashMap-fallback
aggregation) findings — which repeatedly name Q10/Q13/Q16/Q22 — ever
names Q20. The competitive research never independently flagged Q20 as
an aggregation-structure problem in the first place.

## Stage 0: kill-switch microbenchmark

`examples/dense_group_id_bench.rs` (same idiom as `radix_bench.rs` /
`disjoint_merge_bench.rs`): today's `HashMap<u64, Vec<AccumulatorState>>`
("boxed") vs. the proposed `HashMap<u64,u32>` dense-id table +
`Vec<FlatAgg>` flat columnar storage ("flat"), both built by 32
morsel-style parallel workers then merged (sequential reduce — see note
below) and finalized into output columns. Keys drawn via
`raw_id = xorshift() % groups` (exact group-count control); the
dense/sparse domain sweep uses `scatter()`, an odd-multiplier bijection
on u64, to hold group count fixed while asking whether numeric key
locality matters to a HashMap-keyed structure (it shouldn't, and doesn't
— see results). Boxed and flat are seeded identically and their finalized
checksums are asserted equal every point (`assert_eq!`) — a live
correctness self-check on the bench's own faithfulness, not a substitute
for the real suite.

Run: `scripts/claude-safe-build.sh cargo run --release --example
dense_group_id_bench`. Full output archived below (also see git history
of this file for the raw run).

### Primary sweep: 1 aggregate (Sum), 1M-50M groups, dense vs sparse domain

| groups | domain | boxed total | flat total | win |
|---|---|---|---|---|
| 1M | dense | 316.8ms | 186.6ms | **+41.1%** |
| 1M | sparse | 371.9ms | 207.0ms | **+44.3%** |
| 5M | dense | 2386.6ms | 1365.9ms | **+42.8%** |
| 5M | sparse | 2134.3ms | 1184.2ms | **+44.5%** |
| 10M | dense | 3625.3ms | 2588.0ms | **+28.6%** |
| 10M | sparse | 3619.7ms | 2591.2ms | **+28.4%** |
| 20M | dense | 7606.2ms | 5701.9ms | **+25.0%** |
| 20M | sparse | 7544.0ms | 5693.7ms | **+24.5%** |
| 50M | dense | 25210.5ms | 16668.7ms | **+33.9%** |
| 50M | sparse | 24234.8ms | 16984.7ms | **+29.9%** |

**All 10 points clear the >=15-20% gate**, comfortably (24.5%-44.5%
range). Dense vs. sparse domain makes negligible difference at every
group count (as predicted — hashing doesn't care about numeric
locality), so the domain-shape question this task's design flagged is
answered: it doesn't matter for this specific data-structure choice.

### Secondary sweep: aggregate count sensitivity (10M groups, dense domain)

| aggs | boxed total | flat total | win |
|---|---|---|---|
| 1 | 4189.4ms | 2477.1ms | **+40.9%** |
| 2 | 3881.1ms | 3462.5ms | +10.8% (below 15-20% gate) |
| 3 | 4219.0ms | 4635.9ms | **-9.9% (REGRESSION)** |

**Important, honest nuance**: the flat representation's win concentrates
in the 1-aggregate case and degrades, then reverses, as aggregate count
grows. Mechanistically this is expected: a boxed group's single
`Vec<AccumulatorState>` allocation already amortizes across N aggregate
slots in ONE heap allocation and keeps a group's fields colocated; a
flat/columnar layout with N *separate* `Vec<T>` per aggregate must touch
N separate buffers per group (worse locality) and grow N vecs per new
group instead of one. This does not change this task's conclusion (Q13
and Q20 are both single-aggregate `[Sum(Float64)]` shapes, and both
already bypass this tier entirely via `raw_sums`), but it is a real,
useful data point for anyone who picks up Stage 1 in the future against
a genuinely-confirmed multi-aggregate target: the flat design as
specified (one `Vec<T>` per aggregate) is not a strict win in that
regime and would need a smarter layout (e.g. one combined per-group
record) or a scope restriction to single-aggregate shapes.

**Methodology note**: the bench's merge phase is intentionally
*sequential* for both representations (not rayon-parallel) — this is a
deliberate simplification to isolate the representation's inherent cost
under identical merge treatment; it means the *absolute* times shown
(especially "merge") should not be read as production wall-clock
predictions, only the boxed-vs-flat *delta* is the actionable signal,
which is exactly what the gate checks.

## Separate fix: `finalize_disjoint_states` single-state fast path

Task 002's SF=100 spot-check surfaced (out of its own scope, in mine):
`finalize_disjoint_states` (`morsel_agg.rs`) calls `merge_states_to_
batches_filtered(vec![state], ...)` once per disjoint worker (each
worker's state is already-disjoint by construction — no cross-worker
duplicate keys exist, so there is nothing to "merge"). The pre-existing
top branch of `merge_states_to_batches_filtered` only checked `total_
groups > 65,536`, not `states.len() > 1` (unlike the GroupKey branch
right below it, which already had exactly this guard) — so any single
worker state above that threshold silently paid the full shard-then-
rehash machinery designed for combining *multiple* overlapping states,
for nothing. At SF=10 this never fires (46.9K groups/worker < 65,536);
at SF=100 it does (Q13: 469K groups/worker), costing task 002's measured
~205ms/iteration.

**Fix**: added an `states.len() == 1` fast path at the top of
`merge_states_to_batches_filtered` (before the raw_dt/total_groups
branch it was hiding inside): `state.demote_raw_sums()` (folds any
bare-f64 `raw_sums` entries back into `raw_groups` — the ONE prep step
`AggregationState::build_output` cannot do on its own, since unlike
every other consumer in this file it does not know about the `raw_sums`
representation) then `build_filtered_output(&state, schema, post_filter)`
directly — no sharding, no fresh per-shard HashMap rebuild, no nested
rayon parallelism (the real, root cause of the disproportionate cost:
32 outer-parallel workers each spawning up to 64 *more* inner-parallel
shard tasks is 2048 tasks fighting over ~32 threads). This also
transparently benefits the pre-existing small-single-state case
(`total_groups <= 65,536`, e.g. any small non-disjoint aggregate), which
previously paid an `AggregationState::new() + merge()` round-trip
(allocating a second state and re-inserting every entry into it) where
now it operates on the already-owned state in place — strictly cheaper,
never behaviorally different (traced every representation `build_output`
reads: perfect-hash, GroupKey `groups`, `raw_groups`, `raw_null` are
already unioned correctly by `build_output` itself; only `raw_sums`
needed the explicit `demote_raw_sums()` first).

Left untouched: the dense-direct-address merge path, the perfect-hash
tier, and the multi-state (`states.len() > 1`) disjoint/stats-gated
paths — this fix only changes the single-state case.

**Measured (SF=100, `data/tpch-100gb`, 3 iterations, `AGG_TIMING=1`)**:
`[fused-agg] ... merge 15000000 state-groups -> out: 175.1ms / 175.3ms /
153.5ms` (avg **168.0ms**), down from task 002's measured baseline of
**~205ms/iteration** (~18% faster on this step). The `[raw-merge]`
diagnostic print that `merge_raw_states_to_batches` emits when the old
wasteful path fires — the same signal task 002 used to confirm ITS fix
took effect — **does not appear at all** in this run's output, positively
confirming the old per-worker shard/rehash path is no longer reached.
(Q13's inner aggregate is `COUNT(o_orderkey)`, not `SUM`, so this
exercises the boxed `raw_groups` tier specifically, not `raw_sums`.) Note:
this is a cross-session before/after (task 002's number vs. mine, same
machine, same general code area) rather than a strict single-binary A/B
— the mechanistic evidence (print disappearance, matching task 002's own
verification method) is the stronger of the two signals.

Cell-exact SF=100 check: `python3 scripts/sf100_engine_validate.py
--query=13` — see Testing section below for the result.

## Testing

- `disjoint_aggregation_matches_plain_aggregation_exactly` (`spillable.rs`,
  the exact correctness-pinned test named in this task's rules): **PASS**
  (`cargo test --release --lib`, 1 passed, 0 failed, 0.17s).
- Full correctness-pinned suite (`cargo test --release --lib --tests` —
  lib unit tests + all of `tests/*.rs`, i.e. every test that isn't a
  standalone example/benchmark binary): **ALL GREEN, 0 failed anywhere**:
  lib unittests 239 passed/1 ignored, `main.rs` unittests 24, `cli_tests`
  18, `distributed_cluster` 19, `duckdb_validated` 177, `flight_tests` 8,
  `function_tests` 98, `function_validation_tests` 225, `lance_tests` 0
  (feature-gated, skipped on the default build), `partition_contract` 16,
  `spill_tests` 7, `sql_comprehensive` 129, `tpch_queries` 23 (includes
  `tpch_q10`, `tpch_q13`, `tpch_q20` individually), `vector_search_tests`
  0 (feature-gated), `window_functions` 9. **Total: 992 passed, 0 failed,
  1 ignored** (vs. the pre-epic-session baseline of 988 — the +4 is two
  new `duckdb_validated` fixtures from another in-flight task in this
  shared checkout, unrelated to this task).
- `tests/duckdb_validated.rs` (cell-exact): **PASS, 177 passed, 0 failed**
  (0.26s) — includes `validated_tpch_q10`, `validated_tpch_q13`,
  `validated_tpch_q20` individually, all green. (177 vs. task 002's
  documented 175 reflects two new fixtures another in-flight task in this
  shared checkout added concurrently — `validated_join_left_all_filtered`,
  `validated_join_left_on_filter_mixed` — unrelated to this task.)
- SF=100 cell-exact spot check, Q13 (`scripts/sf100_engine_validate.py
  --query=13`): **MATCH** (2870.3ms engine, 1251ms DuckDB, 2.3x ratio —
  the ratio itself is unrelated to this task's fix, which only touches
  merge-step cost, not overall query architecture).
- Full 22-query SF=10 smoke test (`benchmark-parquet --path
  data/tpch-10gb --iterations 1`): **22/22 successful**, all row counts
  match known-correct values (Q13=24, Q20=3953, Q10=20, etc.).
- `disjoint_aggregation_matches_plain_aggregation_exactly`
  (`cargo test --release --lib`, isolated run before the full suite):
  **PASS**, 1 passed, 0 failed, 0.17s.
- `cargo fmt --all -- --check`: clean (one formatting nit in the new
  bench file, `cargo fmt --all` auto-fixed it; re-checked clean).

## Files changed

- `src/physical/morsel_agg.rs` — `merge_states_to_batches_filtered`:
  added the `states.len() == 1` fast path described above. No other
  change (the raw single-column boxed tier, `PERFECT_HASH_MAX_GROUPS`,
  `into_shards`'s fixed-seed hasher, the dense-direct-address merge path,
  and the disjoint/stats-gated multi-state paths are all untouched).
- `examples/dense_group_id_bench.rs` — new Stage 0 kill-switch
  microbenchmark (see above).
