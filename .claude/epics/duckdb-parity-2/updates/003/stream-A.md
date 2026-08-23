---
issue: 003
stream: main
started: 2026-08-23T00:00:00Z
status: completed
completed: 2026-08-23T08:20:00Z
---
## Scope
See .claude/epics/duckdb-parity-2/003.md

## Progress

- Read all three pruning gates end to end before touching anything:
  `analyze_join_output_usage` (`planner.rs:915-998`), `HashJoinExec::
  set_retained` (`hash_join.rs:805-829`), `SpillableHashJoinExec::
  set_retained` (`spillable.rs:212-233`), plus `rt_eligible`
  (`planner.rs:~1428`), `create_combined_batch` (`hash_join.rs:~1720`),
  and every `probe_vectorized`/`probe_hash_table` gather branch.
  Traced how `SpillableHashJoinExec` delegates to an inner `HashJoinExec`
  for the in-memory build path (`hj.set_retained(self.retained.clone())`)
  — this is exactly why the three gates must move in lockstep: a
  disagreement is a silent column-count mismatch, not a loud one.

- `planner.rs`: widened the pruning gate in `analyze_join_output_usage`'s
  `LogicalPlan::Join` arm from `Inner && filter.is_none()` to
  `{Inner,Left,Right,Full} && (filter.is_none() || !filter.contains_subquery())`.
  Force-keep implemented as `retained = needed ∪ collect_columns(filter)`
  at the same insertion site, reusing `eager_aggregation::collect_columns`.
  Widened `rt_eligible` to admit `Left` when the build stays on the left
  (mirrors the existing Semi/Anti `!build_right_for_left` rule) — NOT
  Right or Full: Right always builds from its own preserved right side, so
  the existing wiring (which targets the physical RIGHT child as "the
  probe scan") would target the wrong side; Full preserves both sides, so
  neither may be dropped from the probe-side scan at all.

- `hash_join.rs`: widened `HashJoinExec::set_retained`'s gate identically
  (type + no-subquery-filter check, same field-length checks kept as-is —
  they already held for all four types since `self.schema` is always
  left++right width pre-mask for non-Semi/Anti). Fixed
  `create_combined_batch`'s stale-schema bug: it read the `combined_schema`
  field fixed once at construction from the ORIGINAL unpruned schemas;
  once build-side pruning is live for a filtered join this produced a
  column-count mismatch against the actually-pruned `build_batches`. Now
  rebuilds the schema from `build_batches[0].schema() ++ probe_batch
  .schema()` (respecting `swapped`) right where `all_columns` is built, so
  the two can never drift again by construction. Ported `probe_keep`
  pruning (the Inner path's existing pattern) into the 7 outer-join gather
  call sites that still passed the raw `probe_batch`: `probe_vectorized`'s
  Left parallel-batch branch (Q13's actual hot path at scale — >=32 probe
  batches), its Left/Right/Full sequential fallbacks, and
  `probe_hash_table`'s generic non-VHT Left/Right/Full arms. Left
  `filter_candidate_pairs`'s own `probe_batch` argument untouched
  everywhere (it must always see the FULL probe batch — pruning for
  output only happens after filtering, via the separate `gather_probe`
  local).

- `spillable.rs`: added the identical type/filter gate to
  `SpillableHashJoinExec::set_retained` (it had NONE before — the doc
  comment says plainly it "only worked because the upstream gate never
  handed it an ineligible mask").

- Tests added:
  - `join/left_all_filtered` (task's own suggested SQL, verbatim):
    `customer LEFT JOIN orders ON c_custkey=o_custkey AND o_orderkey<0
    WHERE c_custkey IN (1,2,3,4,5)` — 5 rows, `c_count=0` each. Exercises
    the all-false-mask branch of `filter_candidate_pairs` and force-keep
    for a column needed for two independent reasons at once.
  - `join/left_on_filter_mixed` (new, via the DuckDB-oracle script):
    `SELECT c_mktsegment, COUNT(o_orderkey), SUM(o_totalprice) FROM
    customer LEFT JOIN orders ON c_custkey=o_custkey AND
    o_orderstatus='O' GROUP BY c_mktsegment` over the FULL customer x
    orders relationship (not a LIMIT slice) — three independently
    pruned/force-kept columns at once: a build-side GROUP BY column
    unrelated to the filter, a probe-side SUM column unrelated to the
    filter, and a probe-side filter-only column. Both wired into
    `generate_expected_results.py`'s `get_queries()`, `manifest.json`,
    and two new `duckdb_validated_test!` invocations.
  - `HashJoinExec::left_join_filtered_pruned_build_parallel_batch_path`
    (new, `hash_join.rs`'s own `#[cfg(test)]` module): Left join, a
    build-side filter-only column, 40 one-row probe batches (forces
    `probe_batches.len() >= MIN_BATCHES_FOR_PARALLEL`, landing in the
    exact branch this task fixed), a hand-set retained mask dropping both
    join keys and two unrelated columns. Asserts exact per-row values
    (odd ids matched + real filter value, even ids NULL-extended with
    their real build-side filter value preserved) and exact output width.
  - `SpillableHashJoinExec::spillable_hash_join_retained_mask_matches_
    delegate_schema` (new, `spillable.rs`'s own `#[cfg(test)]` module):
    Left join + filter + retained mask; asserts every returned batch's
    schema matches `join.schema()` EXACTLY (width + names) — this is
    specifically the check that would fail if Gate B and Gate C ever
    disagreed, which no `HashJoinExec`-only test can see.
  - Confirmed existing fixtures `left_on_both_sides`, `full_on_filter`,
    `right_on_filter`, `left_on_filter_cols` (the sharpest pre-existing
    regression guards — they already force-keep filter-only columns
    neither side selects) stay green now that pruning is actually LIVE
    for their join types instead of a permanent no-op.

- Correctness: hit two real compile errors on the first full-suite run
  (both in my own new `spillable.rs` test, caught immediately by another
  concurrently-running task's `cargo test` in this shared checkout before
  my own re-run even finished — fixed both within minutes): `is_null`
  needs `use arrow::array::Array` in scope (not re-exported through
  `PrimitiveArray`'s inherent methods), and a `Vec<&str>` collected from
  `join.schema().fields()` outlived the temporary `Arc<Schema>` returned
  by `.schema()` (E0716) — fixed by binding the schema to a local first.
  After both fixes: full `cargo test` green — **992 tests, 0 failed** (was
  988 pre-epic; +2 new unit tests, +2 new `duckdb_validated` fixtures,
  net of whatever else landed concurrently in this shared checkout).
  `disjoint_aggregation_matches_plain_aggregation_exactly` (task 002's own
  regression guard) green. `tests/duckdb_validated.rs`: 177/177 (was 175).
  `cargo fmt --all -- --check` clean for all 4 touched Rust files (ran
  `rustfmt` directly on just those 4 files rather than `cargo fmt --all`,
  since another agent's concurrently-edited `examples/dense_group_id_bench.rs`
  was also failing the format check and is out of my scope — confirmed via
  a final `cargo fmt --all -- --check` that only that unrelated file
  remains flagged).
  SF=10: **22/22 CELL-EXACT** (`.scratch/validate22.py`, unchanged
  methodology from the project's own established pattern).
  SF=100: **22/22 CELL-EXACT** (new `.scratch/validate22_sf100.py`,
  `data/tpch-100gb`, Q11's `0.0001` threshold divided by SF like the
  SF=10 script already does). One caveat worth recording: the naive
  2-decimal-place comparator (fine at SF=10 scale) flagged Q01 — a
  no-join query, completely outside this task's blast radius — as 7
  "cell mismatches" at SF=100; every one measured at ~1e-14 to 1e-16
  RELATIVE error (IEEE754 float64 noise from parallel-summation order,
  not a wrong answer — the values are in the trillions, where a 2-decimal
  absolute tolerance is meaningless). Switched the SF=100 script to
  relative tolerance for |v|>1 (matching `scripts/sf100_engine_validate.py`'s
  own existing convention) and it reads clean.

- Q13 end-to-end re-measurement, SF=10, `./target/release/query_engine
  benchmark-parquet --path ./data/tpch-10gb --query 13 --iterations 5`:
  - **This shared checkout is under heavy, uncontrolled concurrent load**
    from other tasks' builds/benchmarks (`uptime` load average measured
    2.9 at the start of this task's work and 25.6 by the end) — a naive
    before/after wall-clock diff captured at two different points in time
    is not trustworthy here. Both premises below were re-measured with a
    controlled, same-binary, same-moment A/B instead (see next bullet),
    which is the number this report actually stands behind.
  - Literal reproduction of the requested command, both premises, BEFORE
    my changes (fresh release build of the pre-task-003 tree) vs AFTER
    (fresh release build with every change in this commit):
    - `QE_IPC_CACHE=0`: before avg 275.4ms (5 iter, range 244-303ms) ->
      after avg 278.7ms (5 iter, range 262-315ms).
    - `QE_IPC_CACHE=1`: before avg 224.3ms (5 iter, range 216-231ms) ->
      after avg first-run 366.9ms / retry avg ~290ms — this premise's
      "after" numbers were captured while system load had already risen
      past 12-25; not comparable to the "before" numbers captured at
      load ~2.9. Not a regression signal, a contention signal.
  - **Controlled measurement (the trustworthy one)**: same after-binary,
    same moment, back-to-back, 8 iterations each — default (both
    mechanisms on, confirmed engaging via `QE_PRUNE_DEBUG=1` — "3 of 4
    cols kept" — and `RT_DEBUG=1` — "linked col 1 (o_custkey)", "publish:
    skip=false") vs `QE_JOIN_PRUNE=0 RT_DISABLE=1` (both off, the
    pre-task-003 behavior): **276.98ms vs 277.2ms — not a statistically
    significant difference** (both arms overlap the same noise band this
    task's own design doc predicted when it ran the identical A/B
    pre-implementation and found pruning never engaged at all).
  - Root cause of the small measured effect, confirmed rather than
    assumed: `QE_PRUNE_DEBUG=1` shows the join's input schemas are
    ALREADY down to 4 columns total (`customer` -> `c_custkey` alone,
    `orders` -> `o_custkey`/`o_orderkey`/`o_comment`) before this task's
    mask ever applies — `ProjectionPushdown` (a pre-existing, unrelated
    optimizer rule) had already done the big cut. This task's pruning
    drops exactly one more column: the now-redundant `o_custkey` join
    key. The runtime filter DOES now link and publish for Q13 (confirmed
    via `RT_DEBUG=1`, previously impossible since `rt_eligible` excluded
    Left entirely), but its measured wall-clock contribution is also
    negligible here — the per-task note in `003.md` already flagged that
    Q13 can never take the direct u32 match-emission fast path
    (`hash_join.rs`'s `u32_path`, gated `filter.is_none()`) and always
    pays a double gather of the wide `o_comment` column (once to
    evaluate the filter, once for the retained output, since force-keep
    puts it in both), which almost certainly dominates whatever join-side
    cost remains regardless of column count or scan-side row pruning.
  - Net: the mechanisms are now CORRECT, TESTED, and SAFELY available for
    the filtered-outer-join shapes they were built for, but Q13
    SPECIFICALLY does not show a measurable win on this box — its
    remaining join-side cost sits somewhere this task's design explicitly
    scoped out (the double-gather of a wide string filter column), not in
    unpruned column count or un-filtered scan volume. Reporting this
    directly rather than presenting the noise-band "after" number as an
    improvement.

## Files changed
- `src/physical/planner.rs` — pruning gate widened to Inner/Left/Right/Full
  + no-subquery-filter, force-keep rule, `rt_eligible` widened to Left
  (build-stays-left only).
- `src/physical/operators/hash_join.rs` — `set_retained` gate widened;
  `create_combined_batch` schema fix; `probe_keep` wired into 7 outer-join
  gather branches; 1 new white-box test.
- `src/physical/operators/spillable.rs` — `set_retained` gate widened (had
  none before); 1 new Gate-B/Gate-C lockstep test.
- `scripts/generate_expected_results.py` — 2 new query definitions.
- `tests/expected_results/manifest.json`, `tests/expected_results/join/
  left_all_filtered.csv`, `tests/expected_results/join/
  left_on_filter_mixed.csv` — new fixtures.
- `tests/duckdb_validated.rs` — 2 new `duckdb_validated_test!` invocations.
- `.scratch/validate22_sf100.py` (gitignored scratch, not committed) — new
  SF=100 cell-exact harness, relative-tolerance variant of the project's
  existing `.scratch/validate22.py`.
