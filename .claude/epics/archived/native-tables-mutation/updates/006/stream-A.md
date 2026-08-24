---
issue: 006
stream: main
started: 2026-08-24T09:00:00Z
status: completed
---
## Scope
See .claude/epics/native-tables-mutation/006.md

## Plan
1. Read 001-005 Outcome sections + epic.md + archived epics for style. DONE.
2. Full suite, all 4 feature combos (default/lance/gpu/pulsar) via
   scripts/claude-safe-build.sh. IN PROGRESS.
3. Cell-exact INSERT/DELETE/UPDATE at real scale (data/tpch-10gb-derived
   native table), independent reference.
4. Benchmark: never-mutated table vs phase 1 SF=10 baseline (5.324s,
   1.23x); mutated-table regression honestly reported; dense-direct-
   address + GPU offload still fire.
5. M1/M2 distributed gate re-confirmation.
6. G1-G5 verdicts with evidence.
7. CLAUDE.md native-tables section update (mutation support + limits:
   no compaction incl. the two task-005 residual risks, single-writer,
   no distributed participation).
8. epic.md close-out section, house style per archived/native-tables-
   foundation and archived/duckdb-parity-2.
9. cargo fmt --all -- --check.
10. Move epic to archived/, commit.

## Progress
- Context gathered: epic.md (G1-G5), 001.md Outcome (6 decisions),
  005.md Outcome (7 findings incl. O(N^2) manifest rewrite + deletion-
  vector density residual risks), archived epic.md style refs.
- Confirmed warm release build caches exist for default/lance/gpu/pulsar
  (target/release/deps has lance-*, cudarc-*, tungstenite-* artifacts).
- DEFAULT suite: cargo test --release -> 1185 passed, 0 failed, 1
  ignored (matches task 005's own last-known count exactly, zero
  regression). Log: .scratch/qa006/default.log
- Saved a preserved default-feature binary at
  .scratch/qa006/bin/query_engine_default BEFORE starting the lance
  build (which overwrites target/release/query_engine) so M1/M2 gates
  and benchmarks have a stable default binary regardless of what
  feature combo is mid-build.
- M1 GATE: PASS (cluster_local.sh verify, 3 local processes, default
  binary). M2 GATE: PASS (verify-m2, work division + cell-exact +
  gather path + refusal-by-name + timing, all sections PASS). Logs:
  .scratch/qa006/m1_verify.log, .scratch/qa006/m2_verify.log.
- CLAUDE.md "Current limitations" section (under Native Tables)
  rewritten: added "No compaction" (task 001 Decision 3 + task 003's
  100%-tombstone exception + task 005's two residual risks with their
  real numbers), "Single-writer only" (task 001 Decision 5, mechanism +
  crash-safety + the two-atomic-publish-mechanisms distinction), and
  broadened "No distributed participation" to explicitly cover mutation
  too. Not yet done: the "Benchmarks" section extension (needs numbers).
- New diagnostic examples written (not yet built -- blocked on cargo's
  target-dir lock, held by the still-compiling lance test run):
  - examples/native_mutation_cell_exact_check.rs -- real-scale (SF=10
    orders, 1.5M rows) CREATE+INSERT+DELETE+UPDATE sequence, dumps
    final table to CSV.
  - scripts/native_mutation_cell_exact_check.py -- independent DuckDB
    DML reference over the same source parquet + EXCEPT-based
    cell-exact comparator.
  - examples/native_post_mutation_checks.rs -- hardlink-based mutated
    warehouse prep (broad ~10% lineitem DELETE, l_discount > 0.09,
    spread near-uniformly) for scripts/native_bench_compare.py
    --no-cell-exact, PLUS a dense-direct-address pre/post-delete
    correctness+timing cross-check (native vs parquet+equivalent-filter).
    Leaves the mutated lineitem dir in place for
    examples/native_gpu_check.rs (NATIVE_DIR env var, reused unchanged).
- lance feature test build: STILL COMPILING as of this checkpoint
  (~15+ min elapsed just for compilation, 0 test binaries have started
  executing yet -- this feature pulls in the ~490-crate lance tree, a
  known-slow build per CLAUDE.md's own Dependencies section). cargo
  holds a whole-target-dir lock for the duration, so no other cargo
  invocation can proceed concurrently -- M1/M2 gates above were run
  using the PRESERVED binary specifically to make progress without
  needing the lock. Log: .scratch/qa006/lance.log (background pid
  144923, nohup'd, not the Bash tool's own run_in_background -- polling
  via ps/tail manually per this task's explicit instruction not to rely
  on auto-wake).

## REAL BUG FOUND AND FIXED (2026-08-24, during real-scale cell-exact validation)

`SELECT ... FROM <native table with a Dictionary-coerced string column>
ORDER BY ...` failed outright once the sort was large enough to SPILL:
`Arrow error: Invalid argument error: column types must match schema
types, expected Utf8 but found Dictionary(Int32, Utf8) at column index N`.

- Found while running `examples/native_mutation_cell_exact_check.rs`'s
  final `SELECT * ... ORDER BY o_orderkey` against the real SF=10 `orders`
  table (15M rows, post CREATE+INSERT+DELETE+UPDATE).
- **Confirmed PRE-EXISTING, not a mutation regression**: reproduces
  identically against the PRISTINE, never-mutated
  `data/tpch-10gb-native/orders` fixture from phase 1 (native-tables-
  foundation), via a plain `serve --tables` + curl repro with zero epic
  code involved. TPC-H's own 22-query cell-exact suite never hit this
  because its ORDER BY clauses sit above aggregation (which decodes
  dictionaries away) — this is the first time a plain, unaggregated
  `ORDER BY` over a Dictionary-coerced column at spill-triggering scale
  was exercised.
- **Root cause**: `ExternalSortExec` (the ALWAYS-used spillable sort,
  per this engine's memory-safety rule) sets `self.schema = input.schema()`
  at construction — for a `Project`/`Window` input, that schema comes from
  `plan_schema_to_arrow(&node.schema)`, converting the LOGICAL `PlanSchema`
  (which has NO Dictionary representation, so a string column is always
  reported as plain `Utf8`) to Arrow. The engine has THREE pre-existing,
  independently-written fixes for this exact "declared Utf8 vs actual
  Dictionary" mismatch class (`ProjectExec`'s `project_batch` in
  project.rs, `MemoryTableExec::execute()`'s `rewrap` in scan.rs,
  `hash_join.rs`'s `batch_with_actual_types`) — but `ExternalSortExec`'s
  SPILL path (`flush_run`, `build_merged_batch`, `build_merged_batch_final`
  in spillable.rs) had NONE of them: all three called
  `RecordBatch::try_new`/`concat_batches` directly against the stale
  `self.schema`. The IN-MEMORY sort path (small enough data) was already
  safe because it delegates to `MemoryTableExec`+`SortExec`, both of which
  already have the fix.
- **Fix** (`src/physical/operators/spillable.rs`): `flush_run` now checks
  whether its buffered batches agree on their own actual schema (concat
  under that when they do; cast Dictionary columns down to plain when
  they don't, mirroring `SortExec::execute()`'s identical in-memory
  logic). `build_merged_batch`/`build_merged_batch_final` now route
  through a new local `batch_with_actual_types` helper (mirrors
  `hash_join.rs`'s own function of the same name — this file follows the
  established local-duplication convention rather than a new cross-module
  dependency for a 3-line function).
- **Regression test**: `external_sort_spill_path_handles_dictionary_encoded_columns`
  in `spillable.rs`'s own test module — forces the SPILL branch via a
  1KB memory budget, Dictionary-encoded input, asserts correct sort order
  AND that the output stays Dictionary-typed.
- Small, scoped, root-cause fix — not a redesign. No API changes.

## SECOND REAL BUG FOUND AND FIXED (2026-08-24, same investigation)

After fixing the Dictionary/schema bug above, re-running the SAME
real-scale cell-exact check hit a SECOND, more severe, pre-existing bug
in the exact same area — masked until now because the first bug always
crashed earlier in the same code path:

`streaming_k_way_merge` (spillable.rs, `ExternalSortExec`'s spill-merge)
accumulates `output_rows: Vec<(run_idx, row_idx)>` where `row_idx`
indexes into `run_buffers[run_idx]`'s CURRENT in-memory Parquet batch —
but the loop can RELOAD `run_buffers[run_idx]` (when a run's current
batch is exhausted, `ParquetRecordBatchReaderBuilder`'s `next()` pulls
the next chunk) or drop it to `None` (run fully exhausted) WITHOUT ever
flushing pending `output_rows` entries that still reference the OLD
buffer. Any run whose Parquet file needs more than one
`buffer_rows`-sized read during merge (`MERGE_BUFFER_ROWS` = 8192 —
i.e., any real spill run over 8192 rows, the ORDINARY case, not an edge
case) hits this: a pending row reference silently reads the WRONG data
once the new (differently-sized) batch loads, or panics
out-of-bounds if the new batch is shorter than the stale index
("index out of bounds: the len is 5329 but the index is 5329", hit on
the real 15M-row `orders` sort). This is a SILENT-WRONG-DATA risk, not
just a crash risk — more severe than the first bug, in the engine's
own "spillable operators ALWAYS used for memory safety" default path,
reachable by ANY sufficiently large sort (not native-table-specific).

**Fix**: flush `output_rows` (build + clear) immediately BEFORE
`run_buffers[run_idx]` is overwritten or nulled, for every transition —
not only at the pre-existing periodic `buffer_rows`-size flush. This
makes the invariant `build_merged_batch` requires (every pending row
indexes into the buffer CURRENTLY loaded for its run) hold at all
times, and — argued in-code, not just asserted — makes the loop's
trailing post-exit flush (which calls the differently-semantically-
scoped `build_merged_batch_final`) unreachable in practice, since every
run's final exhaustion now flushes on its own way to `None`. Left
`build_merged_batch_final` itself unchanged (a defensive fallback, not
provably dead by exhaustive proof, and its own row-semantics
[absolute position in a from-scratch-concatenated whole run] would need
a separate redesign if ever actually reached — out of this fix's scope
since it is not the reachable path).

**Regression test**:
`k_way_merge_survives_a_run_needing_more_than_one_buffer_load` — calls
`streaming_k_way_merge` directly with `buffer_rows=4` against a 10-row
and a 5-row pre-sorted run (interleaved distinct values), asserting the
merged output is EXACTLY the expected 15-value sorted sequence (not
just "didn't crash" — this would also catch silent misordering/
duplication/loss).

Both bugs are now fixed in the SAME commit-worthy change
(`src/physical/operators/spillable.rs`); re-running
`native_mutation_cell_exact_check` next to confirm both fixes together
resolve the original real-scale validation failure end to end.

## REAL-SCALE CELL-EXACT VALIDATION: PASS

After both fixes, `native_mutation_cell_exact_check` ran to completion
against real SF=10 `orders` (`data/tpch-10gb/orders.parquet`):
CREATE 12,000,000 rows -> INSERT +3,000,000 (15,000,000 total) ->
DELETE -492,202 (14,507,798 total) -> UPDATE 2,071,620 rows recomputed
in place (14,507,798 total, unchanged by design) -> final
`SELECT * ... ORDER BY o_orderkey` (this IS the spilling sort that hit
both bugs above; 149.4s wall — slow, a pre-existing, NOT newly
introduced, single-threaded/per-row characteristic of
`ExternalSortExec`'s k-way merge comparator, noted honestly, not fixed
-- out of this task's scope).
`scripts/native_mutation_cell_exact_check.py` independently recomputed
the IDENTICAL 4-statement sequence as REAL DuckDB DML against the SAME
source parquet (row counts agreed exactly: 12,000,000 /
+3,000,000=15,000,000 / -492,202=14,507,798 / unchanged=14,507,798) and
compared every cell via DuckDB `EXCEPT` in both directions:
**0 rows different either direction — PASS, cell-exact, 14,507,798 rows
x 9 columns, real SF=10 scale.** (Script needed 2 small, obvious fixes
of its own along the way, no engine involvement: a DuckDB binder quirk
casting-and-realiasing a column to its own name, and accounting for the
engine's own pre-existing, already-documented "bare `SELECT *` carries
qualified field names" behavior in the CSV header.)

## Dense-direct-address + GPU offload post-mutation: both fire correctly; GPU speedup vanishes under a mutated table (real, honest, bounded cost)

`native_post_mutation_checks` (SF=10 lineitem, 60M rows, hardlink-mutated
copy, `l_discount > 0.09` DELETE, ~10% / 5,997,226 rows, spread near-
uniformly across all 58 segments):
- Dense-direct-address: native pre-delete 15,000,000 groups (142ms),
  native post-delete 14,594,694 groups (344ms), parquet+equivalent-filter
  14,594,694 groups (628ms) — **exact group-count match** between native
  post-delete and the independent parquet+filter cross-check.
  `AGG_TIMING=1` confirms the `(native)` dense-direct tag fires for BOTH
  native legs (47.5ms / 303.5ms scan+accumulate) — the fast path never
  silently falls back post-mutation.
- GPU offload (`native_gpu_check`, `NATIVE_DIR` pointed at the mutated
  lineitem copy): still engages, still correct (Q1=6 rows, Q6=1 row, as
  expected). **But the speedup vanishes** — full 2x2 CPU/GPU x
  pristine/mutated matrix (Q6, warm, ms):

  | table | CPU | GPU | speedup |
  |---|---|---|---|
  | pristine (never-mutated) | ~106 | ~6 (cold iter1 ~2.3s) | **~18x**, matches task 008's own documented finding exactly |
  | mutated (10% deletion vector) | ~445 | ~415 | **~1.07x — effectively none** |

  Root cause, reasoned not just observed: Q6's own filter
  (`l_discount BETWEEN 0.05 AND 0.07`) and my delete predicate
  (`l_discount > 0.09`) are DISJOINT ranges — no deleted row could ever
  have matched Q6's own WHERE clause, so Q6's ANSWER is unaffected by
  the delete, yet CPU wall time still ~4.2x'd (106ms -> 445ms). This
  isolates the cost to the deletion-vector CONSULTATION itself (paid
  per-batch, on every scanned row, at the single choke point inside
  `scan()`/`scan_with_filter()`) — not to any change in the query's own
  selectivity. Since deletions are spread near-uniformly (not
  concentrated), the per-batch "no deleted position in this batch's
  range" fast-path skip almost never fires, so nearly every batch pays
  the `arrow::compute::filter` cost. Q1 (multi-agg, GROUP BY, no
  l_discount filter at all) shows the same ~2x pattern (487ms -> 977ms
  CPU) for the identical reason applied to ALL 60M rows instead of a
  filtered subset.
  **This is the honest, bounded "does mutation regress read
  performance" answer this task's own benchmark instruction asks for —
  reported plainly, not hidden**: a genuinely mutated table (non-empty
  deletion vector spread broadly) pays real per-query overhead
  proportional to segment/row count scanned, REGARDLESS of whether the
  query's own selectivity happens to overlap the deleted rows, and this
  overhead is large enough to fully mask GPU offload's kernel-level
  speedup for the one shape (Q6) where that speedup was previously
  measured as dramatic. A never-mutated table pays none of this (dense-
  direct and GPU numbers above match already-published pristine numbers
  exactly) — the cost is real but confined to tables that have actually
  been mutated, exactly as designed.

## THIRD occurrence of the SAME bug class, found by the never-mutated benchmark run, fixed

Running the never-mutated (pristine `data/tpch-10gb-native`) 22-query
TPC-H benchmark hit the SAME error class a third time — Q12 (which,
per CLAUDE.md's own already-documented "no scan-level pruning" finding
from task 008, is the ONE query already known to spill at SF=10):
`column types must match schema types, expected Utf8 but found
Dictionary(Int32, Utf8) at column index 0` — this time inside
`SpillableHashJoinExec`'s spill path, not `ExternalSortExec`.
`create_joined_batch` (spillable.rs) gathers build/probe columns via
`compute::take` (preserves actual Dictionary encoding) then constructed
the joined `RecordBatch` against a caller-supplied `output_schema` — the
SAME `plan_schema_to_arrow`-derived declared schema with no Dictionary
representation. Q12 groups by `l_shipmode` (low-cardinality, Dictionary-
coerced), so its spilled join output carries exactly the mismatch.
Fixed identically: routed through the SAME `batch_with_actual_types`
helper. Swept the WHOLE file for every remaining
`RecordBatch::try_new`/`concat_batches` call afterward (grep, not
sampling) — every other occurrence already uses a batch's own
`.schema()` (data-derived, safe by construction) rather than a
separately-tracked declared field, so this is confirmed to be the LAST
one, not just the next one found by luck.

Three real bugs, one root cause, one fix pattern, applied at all three
call sites it was missing from (`ExternalSortExec::flush_run`,
`ExternalSortExec::{build_merged_batch,build_merged_batch_final}`,
`SpillableHashJoinExec`'s `create_joined_batch`) plus the one genuinely
distinct k-way-merge staleness bug — all in
`src/physical/operators/spillable.rs`, all pre-existing (reproduce on
unmutated native tables), all found because this task's real-scale
validation was the first time this codebase exercised "large enough to
spill" AND "carries a Dictionary-coerced column" at the same time,
across both the sort and the join operator.

## FOURTH finding: a DEEPER, pre-existing join-spill correctness bug, found but NOT fixed (documented instead — judgment call, matches "if large, stop and document")

Re-running the never-mutated 22-query benchmark with all 3 schema fixes
landed: Q12 no longer crashes, but now (a) takes **320.2 SECONDS** (vs
~150-350ms for every other query) and (b) returns a **WRONG ANSWER**:
`row 0 col 1 (high_line_count): engine=707644 duckdb=353822` — EXACTLY
2x too high. This reproduces on the PRISTINE, never-mutated table (not
mutation-specific), confirming it is pre-existing.

**This SUPERSEDES CLAUDE.md's own prior claim** (native-tables-
foundation task 008) that the known Q12-at-SF=10 spill situation is
"Always a safe, clean refusal or a slow-but-correct completion — never
wrong data." That claim was true only because the schema-mismatch crash
(fixed above) always fired FIRST, before the join's own spill/partition
code ever ran far enough to expose this second, independent, more
severe bug.

**Investigated, not fixed — a genuine, bounded judgment call**: read
`SpillableHashJoinExec::execute_spill_path`/`build_with_partitioning`/
`probe_with_spilling`/`process_spilled_partition` (spillable.rs)
end to end looking for a duplicate-counting mechanism (the ~2x ratio
strongly suggests some rows are matched twice). The build-side
partition/spill bookkeeping (`partitions[idx]` vs `spilled[idx]`,
mutually exclusive via `.take()`) and the probe-side dispatch
(`if let Some(ht) = hash_tables[idx] { probe in-memory } else if
spilled_partitions[idx].is_some() { spill for later }`) both read as
correct, mutually-exclusive-by-construction on inspection — no
DICTIONARY-specific mechanism is involved anywhere in this code (pure
row/hash-key bookkeeping), meaning this is NOT a variant of the same
"declared vs actual type" bug class already fixed 3 times above; it is
a genuinely SEPARATE, deeper bug in the partition/spill algorithm
itself (possibly outside `spillable.rs` entirely — e.g., a caller
re-executing partition 0 of the join operator more than once). Finding
the exact root cause would need materially more investigation than the
three fixes above, which were each a single missing schema-
reconciliation call; this is corroborated by CLAUDE.md's own PRE-
EXISTING framing of this exact code path ("a streaming rewrite of the
join spill path — real, separately-scoped future work, not a same-task
fix") from BEFORE this task ever started.

**Decision, reasoned explicitly**: do NOT attempt a fix under time
pressure with incomplete root-cause confidence — silently "fixing"
without being sure would risk a WORSE outcome (still-wrong answers,
now harder to detect because they no longer crash). Do NOT revert the
3 schema fixes either: the duplication mechanism (per the code read
above) has nothing to do with Dictionary columns specifically, so it is
a PRE-EXISTING vulnerability of ANY sufficiently large spilling INNER
join with hash partitioning — reverting the schema fixes would only
mask this ONE manifestation (native tables, which happen to dictionary-
coerce almost every string column) while leaving the SAME underlying
bug fully reachable via a large-enough spilling join over plain Parquet
with no dictionary columns at all. Documenting honestly, matching this
task's own explicit instruction ("if large, stop and document rather
than scope-creep"), is the responsible choice here.

**Benchmark handling**: Q12 excluded from the headline "22-query"
totals below (via `--queries`, excluding 12) with its own separate,
honest verdict reported alongside — mirrors this program's own
established precedent (CLAUDE.md's SF=100 section: "19/22 cell-exact +
successful (Q4/Q12/Q13: see Limitations)").

## Next steps
1. Wait for lance test run to finish; record pass/fail counts.
2. Build the 2 new examples (should be fast once the lock frees --
   default rlib is independently warm).
3. Run native_mutation_cell_exact_check + its .py comparator ->
   cell-exact verdict.
4. Run native_post_mutation_checks -> mutated warehouse ready.
5. Run native_bench_compare.py twice: pristine data/tpch-10gb-native
   (never-mutated parity check vs phase 1's 5.324s/1.23x) and the
   mutated warehouse (--no-cell-exact, regression check).
6. Run native_dense_direct_check.rs fresh (unchanged) + confirm via
   native_post_mutation_checks' own dense-direct leg.
7. Build+run gpu combo suite; then native_gpu_check.rs against both
   pristine and mutated lineitem (NATIVE_DIR env var).
8. Build+run pulsar combo suite.
9. Fill CLAUDE.md Benchmarks section + G1-G5 verdicts.
10. Write epic.md close-out section.
11. cargo fmt --all -- --check.
12. Commit, git mv to archived/, final commit.
