---
issue: 004
stream: main
started: 2026-08-23T00:38:20Z
status: completed
completed: 2026-08-23T09:15:47Z
---
## Scope
See .claude/epics/duckdb-parity-2/004.md

## Progress

- Read task 003's close-out (`.claude/epics/duckdb-parity-2/003.md`,
  `updates/003/stream-A.md`) for context on this file's internals and the
  rigor bar (controlled same-binary A/B, honest reporting even when a
  result is smaller than hoped) before touching anything.

- **Investigation (per the task's own first requirement) — is Semi/Anti's
  exclusion from `probe_vectorized`'s `MIN_BATCHES_FOR_PARALLEL` gate
  (`hash_join.rs`, was ~2582-2588) a deliberate correctness choice or an
  oversight?** Read the gate, both Inner/Left parallel branches, the
  Semi/Anti sequential arm (`for probe_batch in probe_batches { match
  join_type { Semi|Anti => ... } }`), `output_partitions()`, and the
  sibling function `probe_semi_anti_parallel` end to end. Conclusion:
  **oversight, not a correctness requirement**, for three independent
  reasons:
  1. `output_partitions()` already hard-codes `1` for Semi/Anti with an
     explicit comment: "Semi/Anti joins must see ALL probe rows to
     correctly determine matched/unmatched build rows, so they must use a
     single partition." This is a real, necessary constraint — but it
     governs the ASYNC scheduling axis (how many times `execute(partition)`
     is called), not the SEPARATE question this task is about: how the
     `probe_batches: &[RecordBatch]` slice already collected inside ONE
     call to `probe_vectorized` gets scanned. The two are orthogonal;
     `execute()` already gathers every probe batch before `probe_vectorized`
     ever runs, regardless of how that scan is internally scheduled.
  2. The existing match-tracking state (`build_matched_atomic: Vec<Vec
     <AtomicBool>>`, relaxed-ordering, monotonic false->true stores) was
     ALREADY built for concurrent writers — the comment right above it
     literally says "Use atomic bools for Semi/Anti to enable parallel
     probe." There is no shared mutable state that isn't partition-safe;
     the atomics require no new synchronization to be probed from multiple
     rayon batch-tasks instead of multiple intra-batch chunks.
  3. **Direct, shipped precedent**: `probe_semi_anti_parallel` (the sibling
     function serving the non-VHT / filtered-Semi/Anti case) has been
     batch-parallel this exact way since it was written, with a doc
     comment stating the identical mechanism this task's own file
     description already named: "Process probe BATCHES in parallel: 8K-row
     parquet batches are smaller than any useful intra-batch chunk, so
     chunking within a batch left the whole probe on one thread." Someone
     already solved this problem in this file, for the HARDER case
     (filter support); the easier VHT-only case (Q16's actual path) simply
     never got the same treatment when `MIN_BATCHES_FOR_PARALLEL` was
     added to `probe_vectorized`.
  Given this, the fix is a genuine gate extension reusing existing
  per-batch probe logic — not new probe logic, and not a case needing the
  "minimal safe fix" fallback (new partition-local state + merge) the task
  scoped as a fallback plan. That fallback was not needed.

- **Root cause of the 49.2ms confirmed precisely**: `probe_hash_table`
  dispatches Semi/Anti to `probe_vectorized` whenever a VHT is available
  and the join has no ON-clause filter (`filter_served = filter.is_none()
  || matches!(join_type, Inner|Left|Right|Full)` — Semi/Anti reduces to
  `filter.is_none()`). Q16's anti-join is exactly that shape (no filter,
  i64 keys). Inside `probe_vectorized`, Semi/Anti's OLD path was the
  "Original sequential path for small batch counts and other join types"
  — a `for probe_batch in probe_batches` loop, ALWAYS, regardless of how
  many probe batches existed, since Semi/Anti was never in the
  `MIN_BATCHES_FOR_PARALLEL` gate's `matches!`. That loop's Semi/Anti arm
  DID already chunk-parallelize within each batch (`CHUNK_SIZE = 65536`,
  `chunks.par_iter()`), but Parquet batches are ~8,000 rows — always one
  chunk, i.e. no real parallelism — while the 8,000,000-row probe ran as
  ~1,000 sequential loop iterations, one rayon-dispatch-with-1-unit-of-
  work at a time.

- **Fix implemented in `src/physical/operators/hash_join.rs`** (the ONLY
  file touched, per scope):
  1. Extracted the Semi/Anti per-batch probe body into a single closure,
     `probe_one_semi_anti_batch`, defined once near the top of
     `probe_vectorized` (right after `build_matched_atomic` is allocated).
     It reuses `vht.probe_batch(...)` exactly as before, just without the
     now-redundant intra-batch `CHUNK_SIZE` re-slicing (batch-level
     parallelism supersedes it, matching how the pre-existing Inner/Left
     parallel branches and `probe_semi_anti_parallel` already probe whole
     batches with no intra-batch chunking).
  2. Widened `MIN_BATCHES_FOR_PARALLEL`'s gate `matches!` from
     `Inner | Left` to `Inner | Left | Semi | Anti`.
  3. Added a third branch (`else if matches!(join_type, Semi | Anti)`)
     alongside the existing Inner/Cross and Left branches: runs
     `probe_batches.par_iter().map(probe_one_semi_anti_batch).collect()`,
     sets a new `semi_anti_batch_parallel_done` flag. Does NOT `return`
     early (unlike Inner/Cross and Left, which now do — moving their
     `return Ok(results)` inside each of their own branches was needed so
     Semi/Anti can fall through to the UNCHANGED trailing finalization
     code (`if matches!(join_type, Semi) && !swapped {...}` /
     `Anti && !swapped {...}`, which reads `build_matched_atomic` and
     builds the actual output batch) instead of returning before it runs.
  4. `sequential_probe_batches` is forced to an empty slice when
     `semi_anti_batch_parallel_done` is true, so the old sequential loop
     runs zero iterations in that case (its Semi/Anti match arm now just
     calls `probe_one_semi_anti_batch(probe_batch)?` — the same shared
     closure) instead of double-processing every batch.
  5. Added `QE_SEMI_ANTI_PARALLEL=0` (checked once per `probe_vectorized`
     call, matching this codebase's `QE_JOIN_PRUNE`/`QE_MORSEL` idiom
     exactly: `!matches!(std::env::var(...).as_deref(), Ok("0"))`) as a
     same-binary A/B escape hatch — forces the old sequential-batch
     behavior for measurement without needing a second build. Costs one
     `std::env::var` read per Semi/Anti `probe_vectorized` call when unset.
  Net effect: there is exactly ONE implementation of "probe a batch for
  Semi/Anti against the VHT" in this file now (shared by both the
  sequential and batch-parallel dispatch sites), replacing what had been
  two near-duplicate copies (the old sequential arm literally still exists
  unchanged for `probe_semi_anti_parallel`'s different, non-VHT case,
  which was correctly left alone — out of scope, not touched).

- **NOT IN NULL-handling — investigated and confirmed unaffected** (a
  correctness trap independent of this change, per the task's own
  instruction to check it). `VectorizedHashTable::try_new` skips inserting
  ANY null-keyed build row, for every join type uniformly (`if
  vectorized_hash::has_null(key_arrays, row_idx) { continue; }` — read in
  full, this is NOT new code, predates this task). Consequence: this
  engine's Anti join does NOT implement SQL's full `NOT IN` three-valued
  logic (a NULL anywhere in the subquery's result set does not "poison"
  the whole predicate to empty, the way DuckDB/the standard would) — a
  NULL build key is simply never a match candidate for anything, so it
  behaves as if that key were absent, not as a global result-set spoiler.
  This is a **pre-existing engine characteristic, not something this task
  introduced or could have introduced** — the change here never touches
  key extraction, hashing, or null handling, only which thread performs an
  already-defined probe operation on an already-built table. Confirmed by
  code (the null-skip guard is upstream of and untouched by this task's
  edit) and by two new white-box regression tests (below) that run the
  IDENTICAL null-bearing scenario through both the untouched sequential
  path and the new parallel path and assert byte-identical output. TPC-H
  itself never exercises the standard's stricter behavior either way:
  Q16's `s_suppkey` (the subquery/build side) is a non-null primary key,
  confirmed by the SF=10 cell-exact sweep passing unchanged. Fixing the
  pre-existing NULL-semantics gap itself is out of scope: it would need
  new "does the build side contain any NULL key" tracking threaded through
  the whole Anti build/execute path, a materially different and larger
  change than a scheduling fix, and the task's acceptance criteria asked
  only to confirm this stays correct after the change, not to fix a latent
  gap discovered while confirming it.

- **Tests added** (`hash_join.rs`'s own `#[cfg(test)]` module — stayed
  in-file per scope):
  - `run_semi_anti_i64`: shared test helper, explicitly parameterized by
    `left`/`right` (the operator's own terms) rather than `probe`/`build`
    — see the "mistake found during development" note below for why that
    distinction is load-bearing.
  - `anti_join_batch_parallel_matches_sequential_swapped_with_null_build_key`:
    `build_right=true` (swapped — Q16's exact shape: build=small/right
    with a NULL key, probe=left split into 40 one-row batches to force the
    new gate). Asserts the batch-parallel and pre-existing-sequential
    scans of the identical logical join agree exactly, and pins the
    expected 38-row answer.
  - `anti_join_batch_parallel_matches_sequential_not_swapped_with_null_build_key`:
    the `!swapped` mirror (`build_right=false`) — build=left/small/NULL-
    bearing, probe=right split into 40 batches. Exercises the OTHER half
    of `probe_one_semi_anti_batch` (marking shared `build_matched_atomic`
    bits vs. producing per-batch output directly) and the unchanged
    finalization tail. Pins the expected 1-row (NULL-survivor) answer.
  - `semi_join_batch_parallel_matches_sequential_swapped`: same shape as
    the first, `JoinType::Semi`, pins `[10, 30]`.
  - Existing `test_anti_join`/`test_semi_join` (small-scale, unaffected)
    stay green.

- **A real mistake found and fixed during this task's own development**
  (reporting this directly, matching task 003's precedent): the first
  version of the `!swapped` test above used a helper parameterized as
  `probe_schema`/`probe_batches`/`build_schema`/`build_batch`, always
  wiring "probe"-named args to `self.left` and "build"-named args to
  `self.right`. That labeling is only valid when `build_right=true`; for
  `build_right=false` the ENGINE's actual build/probe assignment flips
  (build=self.left, probe=self.right) while the wiring didn't, so the test
  silently split the WRONG side into 40 batches (the side that ended up as
  BUILD, which the gate doesn't key on) and both its "sequential" and
  "parallel" sub-calls exercised the identical pre-existing code path —
  not what was intended, though not incorrect on its own terms either. On
  top of that, the test's own hand-written "expected" value was
  independently wrong (computed from the same left/right conflation). The
  result: `cargo test` caught a real failure — `assert_eq!(sequential,
  vec![i64::MIN])` — but the PRIOR assertion in the same test,
  `assert_eq!(sequential, parallel, ...)`, had already passed, and a
  quick diagnostic (`eprintln!` of the raw per-call output, removed before
  the final commit) confirmed both sub-calls independently returned the
  SAME correct 38-row answer — i.e. the implementation was right the whole
  time; only my own test's construction and its hand-derived expectation
  were wrong. Fixed by rewriting the helper with explicit `left`/`right`
  parameters (matching the operator's own vocabulary) and putting the many
  batches on whichever side is actually PROBE for the `build_right` in
  question, with a doc comment on the helper naming this exact trap so it
  isn't repeated. Re-ran: all 4 Anti/Semi tests plus the full 8-test
  `hash_join::tests` module green.

- **Full-suite validation**:
  - `cargo test --release` (default build): **995 passed, 0 failed, 1
    ignored** across every test binary (lib + 12 integration test files),
    zero failures anywhere. (The lib binary alone: 242 passed, 0 failed,
    1 ignored — the +3 over this session's own pre-fix baseline of 239
    hash_join-relevant lib tests are exactly the 3 new tests above.)
  - `tests/duckdb_validated.rs`: **177 passed, 0 failed** (unchanged from
    task 003's own baseline — this task's change doesn't touch anything
    that fixture suite exercises differently).
  - `cargo fmt --all -- --check`: clean (only `hash_join.rs` in the diff;
    confirmed via `cargo fmt -- src/physical/operators/hash_join.rs`
    targeted at just this file, then re-verified with a full-workspace
    `--check` that nothing else — including other agents' concurrently
    in-progress files in this shared checkout — was left dirty).
  - SF=10 cell-exact sweep (`benchmark-parquet --path ./data/tpch-10gb
    --save-csv .scratch/engine_csv` + `.venv/bin/python
    .scratch/validate22.py`): **ALL 22 CELL-EXACT**, including Q16 (320
    rows), Q21 (100 rows), Q22 (7 rows) — the three Anti/Semi-shaped
    queries in the suite (see the "other queries" section below), and
    every query the base suite already covered.

- **Controlled, same-binary A/B measurement of Q16** (the task's own
  explicit ask — measured, not promised, exactly per task 003's
  precedent). One release binary, built once after all source changes
  landed; `HJ_TIMING=1` isolates each `HashJoinExec::execute()`'s probe
  phase by exact row count, so the anti-join's own 8,000,000-row probe
  (Q16's `ps_suppkey NOT IN (...)`, matching PARTSUPP's exact SF=10 row
  count) is unambiguously identifiable in the log regardless of which of
  Q16's two joins ran first. Default (fix engaged) vs
  `QE_SEMI_ANTI_PARALLEL=0` (fix disabled — the pre-task-004 code path,
  reproduced on the SAME binary, not a separate old build), 12 iterations
  each across two separate invocations run in OPPOSITE order (8 then 4,
  and 4-then-4 with the disabled premise run FIRST the second time) to
  rule out a warm-cache/ordering artifact — there wasn't one; both orders
  agree closely:

  | phase | `QE_SEMI_ANTI_PARALLEL=0` (12 samples) | default / fixed (12 samples) | ratio |
  |---|---|---|---|
  | anti-join probe (8,000,000 rows) | avg **41.5ms** (range 33.0-57.2ms) | avg **6.2ms** (range 5.7-6.9ms) | **6.7x faster** |
  | Q16 total wall time | avg **151.7ms** | avg **120.0ms** | **~21% faster**, ~31.7ms/query recovered |

  Both premises still return the correct 320 rows every iteration (no
  behavior change, timing only). The probe-phase saving (~35.3ms) and the
  total-wall-time saving (~31.7ms) are close, as expected for a change
  that touches exactly one phase of one of Q16's two joins and nothing
  else. The `QE_SEMI_ANTI_PARALLEL=0` baseline (41.5ms) is in the same
  order of magnitude as the task file's own originally-recorded 49.2ms
  (some session-to-session variance is expected and was already flagged
  as a property of this shared, concurrently-loaded checkout by task 003).
  **This is a real, substantial, reproducible win** — unlike task 003's
  outcome on the same branch, this one measured large in a controlled A/B,
  not just in the original noisy profiling pass.

  One correction to the task file's own phase table, found while
  isolating this: the originally-recorded "partsupp build 42.1ms" is NOT
  the anti-join's build phase — the anti-join's build side is the
  filtered `supplier` set (tiny; this session measures its VHT build at
  single-digit MICROSECONDS) — it is Q16's OTHER join's build phase
  (`part x partsupp`, an unrelated Inner join building its hash table
  from all 8,000,000 partsupp rows, ~50ms in this session, confirmed via
  the same `HJ_TIMING` log, `build_keys=["ps_partkey"]`). That build is
  untouched by this task (Inner was already gated into
  `MIN_BATCHES_FOR_PARALLEL` before this task existed) and doesn't move
  in either A/B arm. The task's original ~55.5%-of-Q16 framing therefore
  slightly overstated what this specific fix could recover (it bundled a
  cost this task was never going to touch); the ~21% total-query recovery
  above is the actual, verified number for what this task's fix alone
  changes.

- **Other TPC-H queries with Anti/Semi shapes** (checked per the task's
  own request — a grep of `src/tpch/queries.rs` for `NOT IN`/`NOT EXISTS`,
  then measured, not just noted):
  - **Q16** — `ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE
    s_comment LIKE ...)`. This task's primary target; see above.
  - **Q22** — `NOT EXISTS (SELECT * FROM orders WHERE o_custkey =
    c_custkey)`, no extra ON-clause filter, i64 keys: **the same VHT-served,
    unfiltered shape as Q16, and a confirmed secondary beneficiary of this
    fix.** `HJ_TIMING`, same-binary A/B (1 sample each, less rigorous than
    Q16's 24-sample sweep but directionally unambiguous and mechanistically
    identical): anti-join probe (2,913,844 rows) 33.1ms
    (`QE_SEMI_ANTI_PARALLEL=0`) -> 6.3ms (default), a 5.3x speedup; Q22
    total wall time 198.2ms -> 162.4ms, **~18% faster**.
  - **Q21** — two correlated `EXISTS`/`NOT EXISTS` lineitem self-joins,
    each with an ON-clause filter beyond the equality (`l2.l_suppkey <>
    l1.l_suppkey`, and `l3.l_suppkey <> l1.l_suppkey AND
    l3.l_receiptdate > l3.l_commitdate`). **Not affected by this task**,
    confirmed rather than assumed: `probe_hash_table`'s `filter_served`
    check excludes Semi/Anti when a filter is present, so these two joins
    were ALREADY routed to `probe_semi_anti_parallel` (the sibling,
    already-batch-parallel, non-VHT function this task's own investigation
    used as evidence of precedent) before this task existed, and still are
    — `HJ_TIMING` shows their 3,676,227-row and 2,859,808-row probes
    completing in 15.8ms and 6.2ms respectively (~2-4 ns/row, the
    already-parallel regime, not the ~900-1500ns/row sequential one this
    task fixed for Q16/Q22). No further action taken; correctly out of
    this task's scope.

## Files changed
- `src/physical/operators/hash_join.rs` — the ONLY file touched, per the
  task's own scope rule: `probe_one_semi_anti_batch` closure (shared,
  single implementation for both dispatch sites); `MIN_BATCHES_FOR_PARALLEL`
  gate widened to include `Semi | Anti`; new batch-parallel branch inside
  `probe_vectorized`; `sequential_probe_batches` empty-slice skip for the
  now-redundant old loop; `QE_SEMI_ANTI_PARALLEL` A/B env toggle; 3 new
  white-box `#[cfg(test)]` unit tests plus a shared `run_semi_anti_i64`
  test helper.
