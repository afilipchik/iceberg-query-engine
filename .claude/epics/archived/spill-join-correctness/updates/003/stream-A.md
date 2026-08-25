---
issue: 003
stream: main
started: 2026-08-25T00:20:00Z
status: completed
---

## Scope
See .claude/epics/spill-join-correctness/003.md. Characterization only — no
fix attempted, per the epic's "no guess-fixes" gate. Read task 001's Outcome
+ stream-A.md, task 002's Outcome, the epic's re-scope note, CLAUDE.md's
documented history of this bug, and the post-task-002 `spillable.rs` before
starting.

## Tooling built (`.scratch/spill_blast_radius/`, gitignored)

`native_bench_compare.py`'s existing query-parsing/DuckDB-oracle/cell-compare
functions were imported (not re-derived) into new harness scripts:

- `common.py`: shared `free_port`/`wait_ready`/`start_server`/`stop_server`/
  `run_query`/`run_query_dist` helpers.
- `sweep.py`: runs all-or-a-subset of the 22 TPC-H queries ONE AT A TIME
  against a single warm `serve` process with `QE_SPILL_DEBUG=1`, tracking
  the server log's byte offset before/after each query so a
  `[sj-trace] execute_spill_path START` line found in that query's own
  window is unambiguous, DIRECT proof (not inferred from wall time) that
  `SpillableHashJoinExec`'s spill path fired for that specific query. Also
  counts the aggregate's separately-tagged `[spill-agg]` lines so the two
  are never conflated. Optionally cell-compares every result against a
  fresh DuckDB oracle in the same pass.
- `trials.py`: repeated-trial runner for ONE canonical query against ONE
  source config, `--mode cold` (fresh process per trial) or `--mode warm`
  (one persistent server, repeated queries) — mirrors task 001/002's own
  methodology exactly. Always sets `QE_SPILL_DEBUG=1` so a wrong trial's
  full trace is preserved; always cell-compares against a freshly-computed
  DuckDB oracle (never row-count-only).
- `trials_custom_sql.py`: same as `trials.py` but for a hand-written SQL
  file instead of one of the 22 canonical queries (used for the
  cardinality/skew variant).
- `dist_check.py`: hand-rolled 3-process local cluster launcher (matches
  `cluster_local.sh`'s shape — static peer list, real TCP — but adds
  `--tables` support, which `cluster_local.sh` does not have, and
  `--isolate-tmpdir`, which became necessary — see below). Supports
  `--trials N` for repeated fresh-cluster trials.

## Step 1: identify which queries engage the JOIN spill path at SF=10

`sweep.py` against `data/tpch-10gb-native` (`--tables`) and `data/tpch-10gb`
(`--data`), both via `serve --memory-limit 40G` (task 001's own exact
setting) — the realistic condition, not an artificially tightened one.

**Native, 40G: ONLY Q12 spills** (`execute_spill_path START` fires once,
for Q12 only; all other 21 queries show `join_spill_events=0` and complete
in <1.4s). Re-ran at `--memory-limit 10G` (task 001's own confirmed
admission-control floor): identical — only Q12 spills; Q4 correctly
refuses loudly (`SEMI join build side exceeds the memory budget...
supports only INNER joins`) — the pre-existing, intentional, documented
safety behavior, not a new bug.

**Plain parquet, 40G: NONE spill — not even Q12** (0.162s, no
`execute_spill_path` at all). Pushed `--memory-limit` down through
8G/4G/2G/1G/512M/256M/128M/64M with ZERO effect on Q12 (still 0.17s, no
spill every time). Only between 64M and 32M does parquet's Q12 start
spilling. Root cause, confirmed by diffing `PLAN_DEBUG=1` output for both
sources side by side: the LOGICAL/OPTIMIZED PLAN IS IDENTICAL (same
`projection: [0, 10, 11, 12, 14]`, same filter expression) — the
difference is purely at the SCAN level. Parquet applies its projection +
predicate pushdown physically (row-group stats + RowFilter), so its build
stream is already the ~1.76M filtered rows before the join ever sees it.
Native has no scan-level pushdown (confirmed via `sj-trace`:
**`build_batches=916 build_rows=1765881` — IDENTICAL to parquet's own
numbers once forced to spill** — i.e. this is NOT a row-count difference
into the join at all, contrary to how CLAUDE.md's own prior text reads
("native-table join inputs are larger"). The trigger difference is in
something about the per-batch/per-row BYTE-SIZE ESTIMATE feeding
`execute_spill_path`'s `memory_threshold` check, not row count. NOT
root-caused further (out of this task's charter — it's a threshold/
estimate question, not the wrong-answer mechanism) but flagged as a
refinement to CLAUDE.md's existing explanation.

**Settled on `--memory-limit 1M` for all "forced-parquet-spill" trials**:
at 1M, parquet Q12 shows `in_memory_partitions=1 spilled_partitions=63`
(98.5% spilled) — closely matching native's natural
`in_memory_partitions=0 spilled_partitions=64` (100% spilled) shape. Same
`build_rows=1765881`/`build_batches=916` either way; only the split
between in-memory and spilled partitions differs by construction of the
forcing mechanism.

## Step 2: Q12 trial characterization (the task's primary deliverable)

All via `trials.py`, `QE_SPILL_DEBUG=1` always on, every trial cell-compared
fresh against DuckDB. See 003.md's Outcome for the full numeric table; raw
per-trial data lives in `.scratch/spill_blast_radius/logs/*.json` (not
committed — gitignored, per this repo's `.scratch/` convention, exactly
like task 001's own `.scratch/spill_join_repro/`).

- Native @ 40G (natural spill): 60 cold + 20 warm = 80 trials, **0 wrong**,
  every trial's log confirms exactly 1 `execute_spill_path START`.
- Parquet @ 1M (forced spill, same code path): 60 cold + 20 warm = 80
  trials, **0 wrong** — the single most important result this task
  produces; see 003.md Outcome for the full statistical framing.

## Step 3: cardinality/skew (native only, time-boxed)

Built a widened Q12 variant (`.scratch/spill_blast_radius/q12_allmodes.sql`)
— identical query, `l_shipmode IN (...)` broadened from 2 values (MAIL,
SHIP) to all 7 TPC-H shipmodes — giving a build side ~3.5x larger
(elapsed 6.2s vs 3.2s, consistent with the size increase) and 7 output
groups instead of 2. DuckDB oracle computed directly. 20 cold trials via
`trials_custom_sql.py` against native @ 40G: **0/20 wrong**, all 20 confirm
`spill_events=1`. Parquet skew variant NOT attempted — explicit time-budget
cut, named in 003.md.

## Step 4: distributed (M1/M2) — a real surprise along the way

`cluster_local.sh` only supports `--data` (plain parquet), not `--tables`,
so `dist_check.py` hand-rolls a 3-process cluster for native tables.

**First attempt (no TMPDIR isolation) FAILED** with `Parquet error:
Required field schema is missing`, HTTP 400, `x-qe-distributed: false`.
Server logs showed ALL THREE nodes independently running
`execute_spill_path` with `build_rows` summing EXACTLY to 1,765,881
(586783+586268+592830) — i.e. each node ran its OWN local join over its
OWN ~1/3 shard of `lineitem` joined to a full local `orders` — the
**SCATTER "sharded-fact/replicated-dims" `two_phase` model**, NOT the
GATHER path a first read of CLAUDE.md's summary text suggested for joins
(a real correction: Q12's shape — one large table referenced once,
joined to a smaller table, exactly-mergeable SUM aggregates — qualifies
for scatter's ClickHouse-style election). Root-caused the failure, not
just observed it: `spill_path: std::env::temp_dir().join
("query_engine_spill")` has NO per-process disambiguation beyond a
PER-PROCESS-LOCAL `SPILL_COUNTER` starting at 0 in every process — so
THREE CONCURRENT `serve` processes sharing one host's `$TMPDIR` all
computed `spill_id=0` and wrote to the SAME
`/tmp/query_engine_spill/join_0_0/` directory simultaneously. Confirmed
directly: `find /tmp/query_engine_spill` after the failed run showed
`join_0_0` and `join_0_1` (matching the observed retry) still on disk,
never cleaned up (the cleanup line is only reached on the SUCCESS path).
Removed the stale directories (`rm -rf`, safe OS temp cleanup, not a repo
change).

**Fix-shape test**: added `--isolate-tmpdir` to `dist_check.py` (gives each
node process its own `TMPDIR` env var, so the default spill_path resolves
to a per-node directory). Re-ran: **status=200, cell-exact CORRECT**
(`MAIL,353822,529784` / `SHIP,352224,530051`), `x-qe-distribution` header
confirms `"shape":"two_phase","shard_count":3`. All 3 nodes' own local
partial spilled-join runs succeeded independently and merged correctly.
This directly confirms the collision was the sole cause of the earlier
failure — not a distributed-specific correctness bug in the join itself.

**40 repeated distributed trials** (fresh 3-node cluster per trial,
`--isolate-tmpdir`, native @ 40G, `distributed=1`): **0/40 wrong**, every
trial's `x-qe-distributed: true` and every trial shows at least the entry
node (and usually 2-3 of 3) independently confirming
`execute_spill_path START` via its own log.

The spill-directory collision is reported as a NEW, DISTINCT finding (not
fixed, per the gate) — see 003.md Outcome. It is unrelated to the ~4.8%
wrong-answer bug (fails LOUDLY with a clear I/O error; never silently
returns a wrong answer) but is a real, practical correctness/availability
risk for any deployment where nodes share a temp filesystem (this repo's
own local-cluster test harness included).

Parquet-distributed NOT independently tested: the SAME `--memory-limit`
gates BOTH gather/scatter's own admission check (needs total compressed
bytes across gathered/sharded tables — GBs at SF=10 — to fit under half
the budget) AND the join's own spill threshold (needs ~tens of MB for
parquet's small post-filter build side) — these two requirements are
~50-100x apart and cannot be satisfied by one uniform flag. Stated as a
reasoned extrapolation from the single-process parquet≈native equivalence,
not an independently-confirmed fact.

## Step 5: broader "under memory pressure" bonus sweep + three unrelated findings

`sweep.py` at an aggressive, deliberately-adversarial `--memory-limit 1M`
across all 22 parquet queries (single pass, NOT a trial-count
characterization): 13 queries' joins engage the spill path
(2,3,5,7,8,9,10,12,14,15,16,19,21). Two cell-exact MISMATCHES surfaced
(Q2, Q3) — investigated immediately given the potential significance:

- Both mismatches are ROW-COUNT blowups (Q2: 2085 vs 100; Q3: 755880 vs
  10), NOT value inflation. Directly compared the engine's first-N rows
  (N = DuckDB's LIMIT-ed row count) against the DuckDB oracle: **BYTE-FOR-
  BYTE IDENTICAL** for every row compared. This is a "correct top-K
  prefix, LIMIT never applied to truncate it" bug — a completely different
  symptom shape than the join's value-inflation bug (which keeps row/group
  count correct and inflates the VALUES). Both affected queries have
  `ORDER BY ... LIMIT N`; not seen on any LIMIT-free query. Reported as a
  new, distinct finding (likely `ExternalSortExec`'s spill path or the
  top-k fusion optimizer rule losing the LIMIT under a spilled input) —
  NOT investigated further (out of charter) or fixed (gate).
- Q10 crashed: `Failed to open run file
  ".../sort_0_27/merged_pass0_48.parquet": No such file or directory` — a
  SORT-spill run-file-management bug, again clearly distinct from (and not
  conflated with) the join bug. Not investigated further.
- Q4/13/18/20/21/22 all correctly, loudly refuse
  (`SEMI`/`LEFT`/`ANTI join build side exceeds the memory budget... only
  INNER joins`) — confirmed this is the pre-existing, intentional,
  documented safety behavior, not a new bug, even under this adversarial
  setting.
- Q9 alone took 207s at this setting — repeat-trial characterization of
  the other 1M-only-spilling queries was explicitly NOT attempted (named,
  not silently skipped): the cost (Q9's 207s/trial alone) was judged not
  worth it against this task's remaining time budget, especially once two
  DIFFERENT, unrelated bugs had already been found and needed accurate,
  careful characterization instead.

## Step 6: SF=100 (time-boxed, partial)

`data/tpch-100gb-native` (65GB) and `data/tpch-100gb` (97GB) both exist.
Ran one identify pass at `--memory-limit 100G`: **native — only Q12
spills** (35.5s, cell-exact correct this pass); Q4 refuses loudly as
before; **Q13 timed out at 300s** with `agg_spill=1` (the AGGREGATE's own,
separate spill mechanism — not the join's; `join_spill_events=0` for Q13)
— consistent with, and independently corroborating, task 002's own
explicitly-named follow-up risk ("`merge_parquet_files` ... NOT examined
for an analogous O(n^2) pattern ... worth a future look"). Not
investigated further here (different operator, out of charter). Parquet
@ 100G: Q12 does not spill (matches the SF=10 pattern).

8 additional cold trials of SF=100 native Q12: **0/8 wrong** (all confirm
spill, ~32s each). SF=100 parquet-forced-spill trials NOT attempted (time
budget) — named explicitly in 003.md, not silently skipped.

## Cleanup

`/tmp/query_engine_spill` (685MB accumulated across all trials, mostly
leftover from queries that errored before reaching their own cleanup
code — the non-inner-join refusals and the Q10 sort crash) removed at the
end of the session (`rm -rf`, safe OS temp cleanup). Verified `git status
--short` is empty throughout (zero Rust source changes — everything lives
under the gitignored `.scratch/spill_blast_radius/`).

## Full suite + fmt

`scripts/claude-safe-build.sh cargo test --release` (default),
`--features lance` (`PROTOC=.scratch/tools/protoc/bin/protoc`),
`--features gpu` (`LD_LIBRARY_PATH=$PWD/.venv/lib/python3.12/
site-packages/nvidia/cuda_nvrtc/lib`), `--features pulsar` — all four ran
much faster than task 002's own report (task 002's build had JUST
finished, so `target/release` was fully warm and zero Rust source changed
this task, so nothing needed recompiling). All four: **EXIT=0**, and the
summed pass/fail/ignored counts are **byte-identical** to task 002's own
recorded baseline: default 1190/0/1, lance 1255/0/2, gpu 1190/0/1, pulsar
1193/0/1. `cargo fmt --all -- --check`: clean (exit 0), as expected since
no Rust files were touched.
