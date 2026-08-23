---
issue: 008
stream: main
started: 2026-08-23T21:09:19Z
status: completed
---
## Scope
See .claude/epics/native-tables-foundation/008.md

## Progress

### Read phase (done)
- Read 008.md, epic.md, PRD (.claude/prds/native-tables.md), and all of
  001.md-007.md in full (Outcome sections especially — each ends with a
  "For task 008" section, used directly rather than re-derived).
- Read the duckdb-parity-2 epic.md close-out (the explicit style reference)
  and its task 007 commit history to confirm the exact archival mechanics:
  the `git mv` + benchmarks/CLAUDE.md land in ONE commit
  ("Task 007: fresh SF=10/SF=100 benchmarks..."), the epic.md close-out
  write-up + PRD status flip land in a SEPARATE follow-up commit
  ("duckdb-parity-2: epic complete..."). Will mirror this two-commit shape.
- Note: duckdb-parity-2's PRD was 1:1 with its epic, so flipping PRD
  status to `completed` made sense there. native-tables.md's PRD is a
  FOUR-phase program (foundation/mutation/tiering/rollups) and this epic
  is only phase 1 — confirmed other multi-epic-program PRDs in this repo
  (gpu-acceleration.md, catalog-integrations.md) stay `backlog` even after
  their own epic shipped and closed. Decision: leave
  `.claude/prds/native-tables.md`'s status field untouched (not
  "completed") — task 008's own acceptance criteria never asks for a PRD
  edit either, only epic.md + epic frontmatter + archival.

### Key facts confirmed from source (not just trusted from Outcome text)
- `ExecutionContext::register_native_table` budget = `memory_limit *
  spill_threshold`, `spill_threshold` default = 0.8 (confirmed in
  `src/execution/memory.rs:335`). Matches task 006's 858,993,459 =
  1GiB*0.8 arithmetic exactly.
- `serve --tables <dir>` auto-detects a native table subdirectory via
  `is_native_table_dir` (`_manifest.json` present), registered under the
  subdirectory's name -- confirmed at `src/main.rs` ~1549-1559. This means
  N native tables can be served from one parent directory the same way
  Parquet/Iceberg/Lance already are, which is what the benchmark harness
  below relies on.
- `write-native --from-parquet <file> --out <dir> --mode create|overwrite`
  converts ONE parquet source into ONE native table dir; there is no
  `benchmark-native` CLI subcommand, so the benchmark harness drives the
  HTTP `/sql` surface directly, mirroring `scripts/iceberg_bench_compare.py`
  (this session's own directly-comparable precedent) rather than inventing
  a new CLI command.
- CSV output format (`src/cli/output.rs::write_csv`) is ordinary
  comma-separated, empty string for NULL, quoted only when the value
  itself contains a comma/quote/newline -- safe to parse with Python's
  stdlib `csv` module, matching what `scripts/validate_lance.py` already
  assumes for engine CSVs.
- data/tpch-10gb = 9.6G, data/tpch-100gb = 97G, data/tpch-10gb-iceberg =
  2.8G (on disk, `du -sh`). No SF=100 Iceberg fixture exists, so the
  Iceberg-vs-native comparison is SF=10 only (consistent with
  duckdb-parity-2's own Iceberg benchmark, also SF=10-only).

### New script
- `scripts/native_bench_compare.py` (new, executable): `--write` mode
  converts an 8-table parquet source dir into 8 native-table
  subdirectories via `write-native`; default mode starts
  `serve --tables <native-dir>`, runs the 22 TPC-H queries over HTTP,
  compares timing vs DuckDB-over-parquet AND (if `--iceberg-dir` given)
  DuckDB-over-iceberg, and cell-compares every value against the
  DuckDB-parquet oracle (reuses `validate_lance.py`'s `norm()`
  float-tolerant/date-truncation comparison idiom). Supports `--env
  KEY=VALUE` (repeatable) for the CPU/GPU split (`QE_GPU=0/1`) and
  `--memory-limit` (task 006's admission-control budget needs explicit
  sizing per its own "For task 008" note).

### Test suite status (through scripts/claude-safe-build.sh, --release)
- **default features**: PASS. 1060 passed / 0 failed / 1 ignored across
  all binaries (lib 295, cli_tests 24, distributed_cluster 19 -- M1 gate,
  duckdb_validated 177, flight_tests 8, function_tests 98,
  function_validation_tests 225, native_table_validation 12,
  partition_contract 16, spill_tests 7, sql_comprehensive 129,
  tpch_queries 23, window_functions 9). Reused the existing incremental
  target/ cache (no full rebuild needed) -- finished in well under a
  minute of test execution.
- **--features lance**: PASS. 1125 passed / 0 failed / 2 ignored (18
  "test result: ok" binaries incl. lib 318, cli_tests 24,
  distributed_cluster 20, duckdb_validated 177, flight_tests 8,
  function_tests 98, function_validation_tests 225, lance_tests 31,
  native_table_validation 12, partition_contract 16, spill_tests 7,
  sql_comprehensive 129, tpch_queries 23, vector_search_tests 10,
  window_functions 9). First real full run post-epic (task 005 only ran
  `cargo check` for this feature) -- CLEAN, no lance-specific regression
  from this epic.
- **--features gpu**: RUNNING (background).
- **--features pulsar**: queued next.

### QA regression found: Q12 + Q13 fail against SF=10 native tables
(dictionary-typed schema mismatch after JOIN, and a wrong Q13 row count)
- Full SF=10 native-table benchmark run (`scripts/native_bench_compare.py`,
  22 queries via `serve --tables`) found TWO real failures out of 22:
  - **Q12**: HTTP 400, `Arrow error: Invalid argument error: column types
    must match schema types, expected Utf8 but found Dictionary(Int32,
    Utf8) at column index 0`.
  - **Q13**: no crash, but WRONG ROW COUNT -- engine=23 vs duckdb-parquet
    oracle=24. This is the EXACT wrong-answer SHAPE CLAUDE.md documents as
    a historic bug class ("23 rows instead of 24" at SF=10, commit
    be575fb) -- could be a resurgence for native tables specifically, not
    assumed to be the same root cause without checking.
  - All other 20 queries: cell-exact OK against the DuckDB-parquet oracle.
  - Both queries involve a dictionary-coerced native-table string column
    (`o_orderpriority`/`l_shipmode` for Q12, `o_comment` for Q13's ON-clause
    `NOT LIKE`) flowing through a JOIN before an aggregate/filter -- a
    genuinely different code path from task 004/005's own passing tests
    (small-scale GROUP BY directly over one native table, no join).
  - Repro: `examples/native_q12_q13_repro.rs` (throwaway debug aid, not
    yet decided whether to keep as a permanent regression test -- decide
    once root-caused).
  - Investigating root cause now (hash_agg.rs's `aggregate_batches_hash`/
    `build_group_array`/`extract_group_value` are the prime suspects for
    Q12; Q13's LEFT JOIN ON-clause NOT LIKE evaluation over a Dictionary
    array is the prime suspect for Q13).
  - This is genuinely IN SCOPE to fix per task 008's own instructions
    ("if QA finds a real regression, fix it, small scoped fix only").

### Q13 root-caused and FIXED (not native-table-specific; a pre-existing
concurrency bug in `HashJoinExec`, merely EXPOSED by native tables)

Root cause (confirmed via a throwaway diagnostic build with a temporary
`QE_DEBUG_UNMATCHED=1` eprintln, since removed): `HashJoinExec`'s "emit
unmatched BUILD rows exactly once" mechanism (`build_matched` shared
atomic-bool bits + a `completed_partitions` AtomicUsize on the per-join
`OnceCell<BuildSideCache>`) assumes `execute(partition)` is called EXACTLY
ONCE per partition index for the WHOLE QUERY, comparing the running total
against `self.output_partitions()` for EXACT equality to detect "the last
partition, emit now."

`SpillableHashAggregateExec::execute_fused_streaming` (the memory-safe
default aggregate path) violates that assumption: it drives its child
(here, the LEFT JOIN) through its ENTIRE `0..output_partitions()` range
once, and if a worker's group-count budget trips (discovered only AFTER
that full round already completed -- each spawned drain task's
`input.execute(p).await` computes that partition's join output, including
the counter increment, SYNCHRONOUSLY, before the task ever checks
`abort`), the function returns `Ok(None)` and `execute()` falls through to
`collect_input_partitions_concurrently`, which re-executes the SAME
`0..output_partitions()` range a SECOND time. The counter sails from
`target` up to `2*target` and never lands on `target` again, so the
SECOND round -- the one whose output is actually used, since the first
round's partial aggregate state was discarded -- never re-triggers the
emission. Every unmatched (NULL-extended) build row is silently lost from
the query's real output. Confirmed with a raw
`SELECT COUNT(*) FROM customer LEFT JOIN orders ...` diagnostic and a
`[unmatched-debug]` instrumented build: round 1 correctly computed and
would have emitted 1931 unmatched-customer rows (matching an independent
`c_custkey NOT IN (SELECT o_custkey FROM orders)` cross-check exactly),
but that round's result was discarded; round 2 (`done` climbing 33..64,
target=32) never fired the emission again.

**This is NOT native-table-specific** -- it is a latent concurrency bug in
`HashJoinExec` general enough to hit ANY provider whenever
`execute_fused_streaming` attempts and then abandons a LEFT/RIGHT/FULL
join with a large-enough group-by cardinality to trip its budget. It
happens to be native tables that expose it in this suite (statistics/
plan-shape differences make the fused path attempt-then-abandon for Q13's
native shape but not for Q13's parquet shape at the same SF=10 -- the
parquet oracle's own debug trace shows exactly ONE clean round, target=15,
no doubling).

**Fix** (`src/physical/operators/hash_join.rs`, the single call site,
~line 1371): changed the completion check from `done == target` to
`done % target == 0`. Each full round of the shared child is now
self-contained: since the `matched` bits reflect a fixed, round-
independent fact (whether a build row was EVER probed by a matching probe
row), recomputing "which build rows are unmatched" once per round is
correct, and a discarded round's own (correct) emission is simply thrown
away with the rest of that round's output -- exactly as harmless as
everything else about an abandoned fused-streaming attempt. One-line
semantic change, behaviorally IDENTICAL to before for the overwhelming
common case (exactly one round: `done` only ever equals `target` once
either way). No new field, no opt-out flag, no change to the trait or to
any other call site (verified only one `completed_partitions` use exists
in the file).

**Verified via the same diagnostic build**: Q13 against `data/tpch-10gb-
native` now returns 24 rows, `sum(custdist) = 1500000` (exact customer
count), and the `c_count=0, custdist=1931` row matches the parquet oracle
exactly, cell for cell.

**Q12 status**: root-caused (NOT the same bug as Q13; documented, not
fixed -- see below for why).

### Q12 root-caused: a genuine performance pathology, downstream of this
epic's own already-documented "no scan-level pruning" boundary --
investigated with real tooling (gdb thread dump + filesystem evidence),
NOT fixed (a correctly-scoped "stop and document" call, not an oversight)

Q12 (`orders, lineitem` INNER join, `GROUP BY l_shipmode`, CASE WHEN over
`o_orderpriority`) against SF=10 native tables did not complete within
500s via HTTP (vs ~150-200ms for the parquet oracle) on a clean, otherwise-
idle box -- not merely slow, so investigated with real tools rather than
guessed at:

- **`RUST_BACKTRACE`/`QUERY_PTRACE_ANY=1` + `gdb -p <pid> --batch -ex
  "thread apply all bt"`** against a live, stuck server process (131
  threads) found ONE thread actively executing
  `parquet::column::writer::GenericColumnWriter::write_dictionary_page`
  called from `SnappyCodec::compress` -- i.e., the engine was WRITING a
  compressed Parquet file, something no `SELECT` should ever do.
- **Filesystem evidence, not inference**: `/tmp/query_engine_spill/`
  contained live, actively-growing `join_0_0/` and `join_0_1/` directories
  (194MB and 281MB and climbing) full of `build_N.parquet` files (131+ in
  one alone) -- this is `SpillableHashJoinExec`'s own spill format,
  confirming the JOIN's build side is being partitioned and spilled to
  disk as many small Parquet files.
- **`QE_WORKER_DEBUG=1`** on the aggregate side showed every fused-
  streaming worker stuck at `batches=0 groups=0` throughout -- consistent
  with the aggregate simply waiting on a join that has not produced a
  single output row yet because it is still spilling its build side.

**Root cause chain, each link independently confirmed, not assumed**:
native tables have no scan-level row-group/zone-map pruning yet (task
004/005's own explicitly-documented, INTENTIONALLY out-of-scope boundary
for this epic -- "no predicate pushdown into the IPC reader... relies on
the physical planner's own FilterExec above the scan"). Parquet's Q12
prunes lineitem's date-range filter at the row-group level before the
join ever sees most rows; native tables read every row of both 60M-row
lineitem and 15M-row orders unconditionally. The resulting build side
(or the byte-size estimate used to size it) is large enough to cross the
`memory_limit * spill_threshold` budget that comfortably holds parquet's
much-smaller post-pruning join, so the ALREADY-DOCUMENTED,
pre-existing `SpillableHashJoinExec` limitation applies:
CLAUDE.md's own text (predating this epic) already names this exact
class of cost -- "`SpillableHashJoinExec` still materializes the build
side before deciding to spill (known hole, fixed by the Phase-5
streaming spill rewrite, see ROADMAP)" -- meaning the engine must fully
build the in-memory hash table FIRST and only then discover it should
have spilled, then pay to write it back out as many small Parquet files
before the join can even start probing.

**Why this is NOT a small, scoped fix, and is correctly a "stop and
document" case per this task's own instructions**: closing it for real
means either (a) building scan-level pruning for native tables (a
materially larger feature, already named as this epic's own explicit,
intentional boundary -- not a regression to silently absorb into a QA
task), or (b) rewriting `SpillableHashJoinExec`'s spill path to stream
rather than fully materialize-then-spill (a pre-existing, separately-
tracked ROADMAP item, unrelated to native tables, that predates this
epic and touches the shared join operator every table type relies on).
Neither is a one-file, low-risk change like the Q13 fix. This is reported
as a found-and-explained residual limitation, not silently absorbed and
not left mysterious.

**Scope check -- is this a NEW regression this epic introduced, or an
existing engine characteristic newly exposed?** The latter. Nothing in
tasks 001-007 touched `SpillableHashJoinExec`'s spill mechanics; native
tables merely lack a capability (scan pruning) that lets OTHER providers
avoid tripping this pre-existing, already-known-imperfect code path at
this data volume. `data/tpch-10gb-native`'s other 21 queries all complete
in comparable time to parquet and are cell-exact -- Q12 is the one shape
in the 22-query suite whose post-filter row volume is large enough,
combined with no pruning, to cross the spill threshold.

Cleaned up: `/tmp/query_engine_spill/` (stale + this investigation's
spill files, ~3.4GB) removed; the throwaway `examples/
native_q12_q13_repro.rs` diagnostic deleted after extracting a proper,
fast, deterministic regression test instead (see below).

### Important precedent found: this exact symptom happened once before

`tests/spill_tests.rs::left_join_unmatched_build_rows_preserved` (pre-
existing, unrelated to this epic) has a doc comment that describes THIS
EXACT SYMPTOM as a HISTORICAL bug, already fixed once: "Before the shared
matched-bit fix, multi-partition probes dropped them entirely (TPC-H
Q13's zero-order-customer bucket vanished at SF=10)". That earlier fix IS
the `build_matched`/`completed_partitions` mechanism itself (its own doc
comment at `hash_join.rs:221-226` says the same thing). What task 008
found is a SECOND, independent way to reintroduce the identical symptom
(the fused-streaming-then-fallback double execution) that the FIRST
fix's own regression test does not cover -- it uses `unlimited_ctx()`
(no memory pressure), so `execute_fused_streaming` never trips its own
budget and only ONE round ever executes, never exercising the scenario
task 008's fix addresses. **"Q13's zero-order customers vanish" has now
been fixed twice, by two different mechanisms, each with its own
regression test** (`spill_tests.rs`'s existing test for the original
multi-partition-drop bug; `hash_join.rs`'s new test for the multi-round
reappearance). Confirmed no overlap/conflict: `spill_tests` 7/7 still
pass (7 = the file's full test count, matching every suite run this
session, before and after this fix).

### Permanent regression test added

`src/physical/operators/hash_join.rs::tests::
left_join_reemits_unmatched_build_rows_on_a_second_full_round` (new):
drives the SAME `HashJoinExec` instance through its full
`0..output_partitions()` range TWICE (simulating exactly what
`SpillableHashAggregateExec::execute_fused_streaming`'s abandon-and-
fallback does, without needing to actually trip a spill budget -- fast,
deterministic, no I/O). Asserts BOTH rounds return the correct
1900-row total (1200 matched + 700 NULL-extended). **Verified it actually
pins the bug**: reverted the one-line fix locally, re-ran just this test
-- failed exactly as predicted (round 1: 1200 rows, missing all 700
unmatched), confirming the test is not vacuous. Restored the fix
immediately after. `cargo test --release --lib hash_join::` — 9/9 passed
(all pre-existing hash-join tests unaffected).

## Results so far

### SF=10 native tables (post-fix): 22/22 queries, ALL CELL-EXACT
Write: 23.52s total (8 tables), 6.5G on disk (vs 9.6G parquet source --
SMALLER, dictionary coercion wins). Full 22-query run (2 iterations,
`scripts/native_bench_compare.py`): **engine 5324.13ms / DuckDB-parquet
4320.55ms (1.23x) / DuckDB-iceberg 6888.14ms (0.77x -- engine FASTER)**.
All 22 rows cell-exact vs the DuckDB-parquet oracle, including Q13 (24
rows, fix confirmed holding through the real HTTP/serve path, not just
the isolated repro).

### SF=100 native tables: 65G on disk (vs 97G source, again smaller).
Write: 209.60s total (lineitem 600M rows/573 segments in 173.9s).
21/22 queries (all but Q12) run: 19 cell-exact + successful (after
correcting a too-tight ABSOLUTE float tolerance in my own comparison
script -- Q01's "mismatch" was 0.044 absolute on a ~4.8e12 value, i.e.
9e-15 RELATIVE, textbook FP summation-order noise, not a bug; fixed
`cell_compare` to use `max(0.02, abs(value)*1e-9)`, matching this repo's
own established magnitude-aware tolerance convention). Engine total
(19 queries) 75.17s vs DuckDB-parquet 50.02s = 1.50x.

**Q04 and Q13 ALSO hit the same root-cause family as Q12 at SF=100**
(did not at SF=10, confirming this is scale/data-volume-dependent, not
query-shape-specific): Q04 (EXISTS/SEMI-join decorrelation, orders
`EXISTS` lineitem) refuses cleanly even at a 115G memory limit (92G
budget) -- "SEMI join build side exceeds the memory budget... spill path
currently supports only INNER joins" -- confirming the build side is
genuinely large (likely lineitem itself, 600M rows, wrongly favored over
the much smaller date-filtered `orders` side -- consistent with native
tables' statistics/pruning gap affecting JOIN PLANNING, not just scan
volume). Q13 times out (>300s) at SF=100 for the same reason Q12 did at
SF=10: the LEFT JOIN build side crosses the spill threshold at this data
volume and pays the same "materialize fully, then spill via many small
Parquet files" cost. **Net: 3 of 22 queries (Q4, Q12, Q13) hit this ONE
documented, already-understood root cause, at scale-dependent
thresholds** (SF=10: only Q12; SF=100: Q4/Q13 also). All three refuse
cleanly or run very slowly -- NEVER a wrong answer -- consistent with
CLAUDE.md's own pre-existing "fail loudly instead of returning wrong
results" characterization of this spill path.

### M1/M2 distributed gates: both PASS (real 3-process clusters, not just
the in-process `distributed_cluster` test binary)
- `scripts/cluster_local.sh start 3` + `verify`: **M1 GATE: PASS** —
  membership agreement, 5 TPC-H queries byte-identical across all 3 nodes
  AND the single-process binary, healthz/readyz, Flight==HTTP on all 3
  nodes (4/4 shapes), SIGTERM survival (survivors correctly mark the
  killed node down and keep answering).
- `verify-m2`: **M2 GATE: PASS** — work-division imbalance <=1.1 at 3 and
  8 nodes, 13 scatter-path aggregates cell-exact vs DuckDB, 13 gather-path
  shapes (joins/subqueries/DISTINCT/ORDER BY/CTEs) cell-exact vs DuckDB,
  refusals correctly named.
- Confirms this epic's own G5 boundary precisely: nothing this epic
  touched broke EXISTING distributed behavior for parquet/Iceberg/Lance
  tables (native tables were not part of either gate's data set, by
  design — matches the epic's own explicit "doesn't need to work inside a
  distributed cluster yet" scope).

### Final 4-way suite re-verification (fix + regression test baked in)
- **default**: 1061 passed / 0 failed / 1 ignored (1060 + the new
  hash_join regression test). `cargo fmt --all -- --check` clean.
- **lance**: 1126 passed / 0 failed / 2 ignored (1125 + the new test).
- **gpu**: 1061 passed / 0 failed / 1 ignored. `/tmp/query_engine_gpu` saved
  aside (copy of the gpu-featured binary) for the CPU/GPU split benchmark
  before the pulsar build overwrites `target/release/query_engine`.
- **pulsar**: 1064 passed / 0 failed / 1 ignored.

**ALL FOUR FEATURE COMBINATIONS GREEN with the final code state (fix +
regression test + new gpu-check example)**: default 1061/0/1, lance
1126/0/2, gpu 1061/0/1, pulsar 1064/0/1 (passed/failed/ignored).
`cargo fmt --all -- --check` clean throughout.

### CPU/GPU split on native tables: a REAL full-query win found (unlike
### parquet) -- and a real methodology gap found along the way

**Methodology gap found first**: `scripts/native_bench_compare.py` (HTTP/
`serve`-based, like `iceberg_bench_compare.py`) showed ZERO VRAM growth
across 6 iterations x 4 queries -- GPU offload never engaged at all.
Root cause: `serve` is a "distributed context" by construction (even
single-node) and `ExecutionConfig::gpu_offload` is intentionally never
enabled there (byte-exactness). This is NOT a gap task 008 introduced --
it's the pre-existing, documented architecture (CLAUDE.md's own GPU
section: "single-process CLI paths... call `ctx.enable_gpu_offload()`").
**A second, real gap found along the way**: `load-native --query` (the
ONLY existing single-process CLI surface for native tables) doesn't call
`enable_gpu_offload()` EITHER, AND (worse) doesn't even register a real
`NativeTable` -- it fully materializes via `native_write::read_back` and
registers a plain `MemoryTable`, which has no `identity()` override and
so could never pass the GPU eligibility gate regardless. Not fixed (out
of scope -- a CLI wiring gap predating this task, not a regression);
worked around with a small new diagnostic instead:
`examples/native_gpu_check.rs` (kept permanently, matching the
`native_dense_direct_check.rs`/`gpu_price_bench.rs` precedent) calls
`register_native_table` (the REAL provider) + `enable_gpu_offload()`
directly and runs Q1/Q6 six times each.

**Real finding, VRAM-confirmed** (`nvidia-smi` sampled every 0.3s
concurrently, RTX 5090): VRAM grew **1048 -> 3858 MiB** the moment the
GPU-enabled run touched `lineitem`'s columns, and stayed there --
independent, direct confirmation offload actually engaged for a REAL
`NativeTable`, not assumed from a log line (the `RUST_LOG=...gpu=debug`
lines didn't print in this harness; the VRAM evidence is the stronger
proof anyway, per this program's own "never just trust a log line" rule).

| query | CPU steady (avg, QE_GPU=0) | GPU cold (iter 1) | GPU warm (iters 2-6 avg) | verdict |
|---|---|---|---|---|
| Q6 (single SUM, no GROUP BY) | ~140ms | ~2.0-2.2s | **~7-8ms** | **~18-20x FASTER end to end** |
| Q1 (10 aggs, GROUP BY x2) | ~597ms | ~0.8-1.1s | ~505-623ms | flat / inconclusive (matches parquet's own Q1 finding) |

Reproduced twice, consistent both times (Q6 warm 7-8ms both runs).

**This DIRECTLY ANSWERS this task's own framing question**: does native
tables' mmap-resident-by-construction reads change the "GPU shows no
full-query win" calculus `duckdb-parity-2` found for parquet-sourced
queries? **For Q6's shape (single, ungrouped aggregate): YES, decisively
-- this is the first shape in this program's entire GPU-offload
investigation (parquet OR native) to show a real, reproducible,
order-of-magnitude full-query win**, exactly because there is no decode
step to dominate wall time the way parquet's row-group/dictionary decode
does (duckdb-parity-2's own root-cause finding for WHY parquet Q6 showed
no win). For Q1's shape (multi-aggregate + GROUP BY): NO -- consistent
with parquet's own Q1 result being flat too, so this specific shape's
bottleneck is something other than scan/decode OR the reduction kernel
(plausibly kernel dispatch/grouping overhead at only ~4-6 output groups,
not investigated further -- out of this task's scope to chase a SECOND
GPU-shape investigation).

## Next
1. Re-verify lance/gpu/pulsar feature builds with the FINAL hash_join.rs
   state (fix + new regression test) -- my earlier 4-way sweep predates
   both.
2. `cargo fmt --all -- --check`.
3. CPU vs GPU split on native tables (SF=10, `--features gpu` binary).
4. M1/M2 distributed gate re-confirmation (already covered by the
   distributed_cluster 19-20/19-20 pass in every feature combo above, plus
   the real-process `cluster_local.sh verify`/`verify-m2` gate for extra
   confirmation).
5. G1-G5 verdicts, CLAUDE.md section, epic.md close-out, archive, commit.
