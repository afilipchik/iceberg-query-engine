# Task 003 — Q13 SF=100 temp-file rename error — stream B

## 2026-09-02 session start

### Code archaeology (before any runs)

- The pruning epic's failing "after" binary was built from `2534739`
  (native-table-pruning task 002 close, 2026-08-27 02:15). Its "before"
  binary was `e0e10cc` (2026-08-27 00:43). BOTH contained the same spill
  code (post O(n²)-append fix `21ed410`), and NEITHER contained:
  - `cd8a33b` (sjc-2 001, 2026-08-27 02:53): PID-embedded default spill
    path — the spill-directory-collision fix;
  - `b607594` (sjc-2 004): multi_pass_merge carried-run deletion fix;
  - `a0b760d` (oom-safety 002, 2026-08-29): SpillableHashAggregateExec
    open-writer rewrite, which DELETED `merge_parquet_files`.
- At `2534739`, the ONLY `std::fs::rename` in
  `src/physical/operators/spillable.rs` was in `merge_parquet_files`
  (line 2361: "Failed to rename merged file: {}") — called from
  `SpillableHashAggregateExec::aggregate_with_spilling` ONLY (two call
  sites, lines 1507/1539; the temp-file variant writes
  `temp_{idx}_{count}.parquet` then renames `merged_{idx}.parquet` onto
  the existing spill file). The JOIN path had no rename at that commit
  (its rename-per-append died in `21ed410`). So "SpillableHashJoinExec
  temp-file rename error" in the pruning record was almost certainly the
  AGG stage above the spilling join in Q13's plan (or a loose
  attribution) — the error string names no operator.
- On current HEAD (`306dc15`): `merge_parquet_files` is GONE (open-writer
  appends per partition, `agg_part_{idx}.parquet`), there is NO
  `fs::rename` anywhere in spillable.rs, and the default spill path
  embeds the PID.
- Concurrency context for the historical failure: sjc-2 task 001's own
  Outcome (same day, same machine) documented cross-process spill-dir
  bleed between an SF=100 process and SF=10 trials via the PID-less
  shared `$TMPDIR/query_engine_spill` — i.e. the collision mechanism was
  demonstrably LIVE on 2026-08-27. A colliding process's
  `remove_dir_all` cleanup deleting `merged_{idx}.parquet` between its
  creation and its rename is a direct producer of exactly "Failed to
  rename merged file: No such file or directory".
- Also relevant: at `2534739` the `estimate_batch_size` Dictionary
  mmap-capacity bug (~4,000x overestimate, fixed 2026-08-29) was still
  present, which is what pushed Q13's join/agg into the spill path even
  at `--memory-limit 100G`.

### Plan

1. Rebuild HEAD release binary (safe wrapper), run Q13 SF=100 at the
   pruning epic's settings (serve --tables data/tpch-100gb-native
   --memory-limit 100G, QE_MEM_CAP=110G, MemoryMax generous, 20+ min
   timeout, QE_SPILL_DEBUG=1).
2. Build `2534739` into a separate CARGO_TARGET_DIR; run the same query
   there (a) solo, (b) with a controlled adversary reproducing the
   cross-process deletion — to confirm the failure and its mechanism
   live.
3. Regression test pinning the lifecycle (no rename-window intermediates
   during a spilling aggregate; adversarial deletion of `merged_*`/
   `temp_*` names is harmless), unit-level, no SF=100 needed.
4. Validate Q13 SF=100 cell-exact vs fresh DuckDB oracle.

### Run results (2026-09-02)

- **DuckDB oracle (fresh)**: Q13 SF=100 over `data/tpch-100gb` parquet =
  2 rows: `(15, 10000000)`, `(0, 5000000)` (2.0s;
  `.scratch/sjc3-003/q13_oracle.py` / `q13_oracle.json`).
- **Current HEAD (306dc15 + this task)**: serve `--tables
  data/tpch-100gb-native --memory-limit 100G`, `QE_MEM_CAP=110G`,
  `QE_SPILL_DEBUG=1`, systemd-run `MemoryMax=110G`. Q13 = **HTTP 200 in
  13.5s, cell-exact** (both rows match the oracle exactly). ZERO spill
  traces in the serve log — with the Dictionary size-estimate fix the
  query never enters the spill path at 100G at all, so the failing
  mechanism is doubly unreachable (code deleted AND path not taken).
  Side observation (pre-existing, out of scope): `execute_fused_streaming`
  call_id=1 aborts with a Dictionary(Int32,Utf8)/Utf8 concat error in a
  drain task and falls back to the streaming two-phase path (adds ~2.7s);
  answer unaffected.
- New regression test added:
  `agg_spill_has_no_rename_window_under_adversarial_intermediate_deletion`
  (spillable.rs test module) — forced-spill aggregate + adversary thread
  deleting `merged_*`/`temp_*` intermediates; asserts spill engaged, zero
  rename-window intermediates observed, results byte-identical to the
  unlimited run.
- In flight: Q13 SF=100 live run at `2534739` (the pruning epic's exact
  failing binary lineage) + a controlled small-scale collision demo at
  that commit.

### Gold-standard reproduction at 2534739 (2026-09-02, live)

Ran Q13 SF=100 at the pruning epic's exact settings against the binary
built from `2534739` (their failing "after" lineage), solo, wrapped
(`MemoryMax=110G`), `QE_SPILL_DEBUG=1`:

1. Fused-streaming aborts (pre-existing Dictionary/Utf8 concat), falls
   back → `[spill-agg] collected 2290 batches, 155000000 rows,
   356340216310 bytes` — the DICTIONARY OVERESTIMATE (356GB claimed) is
   what pushes the aggregate ABOVE the join into the spill path even at
   `--memory-limit 100G`. Spill dir: PID-less
   `/tmp/query_engine_spill/agg_0_0`. Live observation mid-run:
   `merged_12.parquet` present alongside `part_12_191.parquet` — the
   rename-window intermediate, and partition 12 already on its 191st
   spill append (the O(n²) merge grind that explains the historical
   'before' 900s timeout).
2. Acting as the colliding process (what concurrent agents' cleanup did
   on 2026-08-27): a loop deleting `merged_*.parquet` from the shared
   dir. First hit (inside the fused drain): `drain task p=0 returned
   Err: Execution error: Failed to rename merged file: No such file or
   directory (os error 2)` — SWALLOWED by the fused-fallback, which
   re-executed from scratch (626s wasted).
3. Second hit, during the retry's non-fused spill-agg pass
   (`agg_0_1`): propagated to the client — **HTTP 400 in 744.3s:
   "Execution error: Partition 0 execution failed: Execution error:
   Failed to rename merged file: No such file or directory (os error
   2)"** — the exact historical error, client-visible, at the exact
   commit/settings. Total: 2 deletions → 2 failures; the mechanism is
   fully deterministic given a colliding deleter.

Root cause (final): NOT the join. `SpillableHashAggregateExec`'s
repeat-eviction append (`merge_parquet_files`: write
`merged_{idx}.parquet` → `fs::rename` over the partition spill file; the
only rename spillable.rs ever had) + the PID-less shared default spill
root. Removed on main by TWO independent merged changes: `cd8a33b`
(sjc-2 001, PID-embedded spill root — ends cross-process sharing) and
`a0b760d` (oom-safety 002, open-writer agg rewrite — deletes
`merge_parquet_files`/the rename entirely); additionally `2912456`+
(estimate fix) means Q13@100G never even enters the spill path now.

### Validation

- Q13 SF=100 HEAD: 13.5s, HTTP 200, programmatic cell-exact check vs
  fresh DuckDB oracle: PASS (order-sensitive, value-exact).
- New regression test passes; spill_tests 9/9; fmt clean after
  `cargo fmt`. Full default suite in flight.
- Cleanup: no stray serve processes; PID-less `/tmp/query_engine_spill`
  removed; task-001's `query_engine_spill_<pid>` dirs untouched.

### Final validation numbers (2026-09-02)

- Full default suite at HEAD (release): **1318 passed / 0 failed / 1
  ignored**, exit 0 (`.scratch/sjc3-003/suite_default.log`).
- spill_tests: 9/9. New regression test + both neighboring agg-spill
  tests pass. `cargo fmt --all -- --check` clean.
- Q13 SF=100 cell-exact check vs fresh DuckDB oracle: PASS
  (programmatic, order-sensitive, value-exact; engine 13.5s HTTP 200).
