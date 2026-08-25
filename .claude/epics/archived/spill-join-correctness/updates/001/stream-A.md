---
issue: 001
stream: main
started: 2026-08-24T14:19:55Z
status: completed (confidence gate NOT met — honest partial result)
---

## Scope
See .claude/epics/spill-join-correctness/001.md

## Context gathered
- Read CLAUDE.md's "Mutation: QA close-out" (task 006) section in full,
  and `.claude/epics/archived/native-tables-mutation/006.md`'s Outcome +
  its `updates/006/stream-A.md` progress log (the raw, more detailed
  original trace).
- **Correction to how 001.md's task description frames the original
  finding**: 001.md says the bug was found "against a native table that
  went through a full CREATE->INSERT->DELETE->UPDATE sequence." Reading
  the RAW original `stream-A.md` log directly shows this is not quite
  right: the wrong-answer finding came from running the standard 22-query
  `native_bench_compare.py` benchmark against the PRISTINE, NEVER-MUTATED
  `data/tpch-10gb-native` warehouse (built via `write-native
  --from-parquet`, not `CREATE TABLE AS SELECT`) — stream-A.md's own
  words: "This reproduces on the PRISTINE, never-mutated table (not
  mutation-specific), confirming it is pre-existing." The
  CREATE->INSERT->DELETE->UPDATE sequence is a SEPARATE diagnostic
  (`examples/native_mutation_cell_exact_check.rs`) that only ever touched
  the `orders` table and never ran Q12 at all. Mutation history is NOT
  part of the trigger condition. This matters for anyone re-reading
  001.md cold — the repro below uses the PRISTINE warehouse, matching
  the ACTUAL original finding, not a mutation sequence.

## Reliable reproduction established (exact original path)

`data/tpch-10gb-native` (pristine, already on disk, unchanged since
phase 1 / native-tables-mutation) + `target/release/query_engine serve
--bind 127.0.0.1:<port> --tables data/tpch-10gb-native --flight-bind
none --memory-limit 40G` + `POST /sql?format=csv&distributed=0` with
Q12's exact spec SQL (from `src/tpch/queries.rs`, byte-identical to what
`native_bench_compare.py` sends). DuckDB oracle independently computed
via `.venv/bin/python` + `duckdb` + `read_parquet` views over
`data/tpch-10gb/{orders,lineitem}.parquet`: `MAIL,353822,529784` /
`SHIP,352224,530051` — matches CLAUDE.md's cited 353822/352224 exactly.

**CONFIRMED non-deterministic, empirically, not assumed — final tally:
21 total runs of the byte-identical repro, 1 wrong (4.8%).** Every run
(right or wrong) took 140-291s vs 150-350ms for every other TPC-H query
— the SLOWNESS reproduces 21/21, always, unconditionally. Wrongness
reproduces intermittently:

| # | process | config | result | wall time |
|---|---|---|---|---|
| 1 | fresh | 40G | **WRONG**: MAIL 705002/1055639, SHIP 701718/1056148 (~1.99x DuckDB, BOTH aggregates BOTH shipmodes inflated by the same ~1.9925-1.9927x ratio) | 291s |
| 2-21 | mix of warm repeats + fresh restarts, 40G and one 10G variant (1G fails admission control outright — needs 10G+) | 40G/10G | correct, every time | 140-147s |

Grand total across all 21: **1/21 (4.8%) wrong**, all 21/21 uniformly
slow. The one wrong run's wall time (291s) is close to 2x a correct
run's (~141s) — consistent with, but (see below) NOT proof of, "the
whole join ran twice."

An early "consistent within one warm process, differs across fresh
restarts" pattern (2/2 then 3/3 correct within-process, before the wrong
case recurred) turned out NOT to be a real per-process-seed effect on
closer look: `partition_batch_by_hash` uses an EXPLICITLY fixed hash
seed specifically BECAUSE hashbrown's default hasher was found to reseed
per-process in the past (see the comment at that function — a previous
real bug of exactly that shape, already fixed). Partition routing is
therefore deterministic across process instances. The apparent
within-process consistency was well within chance at a ~5-10% true
per-query rate and should NOT be relied on as a real mechanism.

**Smaller/faster repro: NOT FOUND.** Tried lowering `--memory-limit`
from 40G to 10G (1G fails NativeTable's admission-control check
outright, needing 10G+ to even start): identical signature (all 64 hash
partitions already spilled at 40G, so 10G changes nothing — the trigger,
whatever it is, isn't gated by how tight the memory budget is once
you're already fully spilling). Did not attempt a deliberately downsized
synthetic dataset (smaller SF, or a hand-built tiny table forced through
the same spill shape) — the natural, full-scale repro was the only one
exercised, given time was spent instead on directly testing the leading
structural hypothesis (see below), which turned out to be higher-value.
Left as explicit follow-up if this investigation continues.

## Instrumentation added (`QE_SPILL_DEBUG` env var — reuses this file's
own pre-existing name for the sibling aggregate-spill diagnostic rather
than inventing a new one; matches the `HJ_TIMING`/`AGG_TIMING` convention)

Final, SHIPPED state (the temporary chaos/fault-injection hook described
below was removed before finishing — see "Chaos test" section):

- `execute_spill_path`: START/DONE trace with `spill_id` (unique per
  call, via the pre-existing `SPILL_COUNTER`), build row count, build
  partitioning breakdown (in-memory vs spilled partition counts + rows
  AT THE MOMENT each partition spilled — NOT its final row count, since
  more rows get appended after that point), probe row count,
  in-memory-matched vs spilled-matched vs total-matched row counts,
  elapsed time.
- `execute_fused_streaming` (the "fused streaming aggregate" the
  `BuildDecision::Spill` doc comment refers to): START trace
  (input_partitions, disjoint mode, a new monotonic `call_id` via
  `next_sj_trace_id()`/`SJ_TRACE_SEQ`); on `Ok(None)` (drain_failed or
  abort), an ABORTED trace with drain_failed/abort flags, group-limit
  state, AND captured error text from whichever site set it (a drain
  task's `Err`, a drain task's panic/`JoinError`, a worker thread panic,
  or `process_batch`'s own error message via a new
  `Arc<Mutex<Option<String>>> abort_reason` slot filled by the FIRST
  worker to hit an error); on success, an OK trace with
  total_groups/out_rows/elapsed.
- `SpillableHashAggregateExec::execute`: one line right before the
  `collect_input_partitions_concurrently(&self.input)` fallback call,
  printing whether it's reached because fused-streaming was ineligible
  or because it aborted — this is the literal call site that
  RE-EXECUTES the (non-idempotent) join child from scratch when reached
  after a fused-streaming abort.

All gated behind `std::env::var("QE_SPILL_DEBUG").is_ok()` (checked once
per call/function invocation into a local bool, never re-checked
per-row/per-batch) or the `sj_trace` bool propagated from that check;
zero new allocations/atomics/locks on the hot per-row path when unset
(the `abort_reason: Mutex<Option<String>>` and the extra per-error
formatting only execute inside already-cold error/abort branches, and
the mutex lock itself is only taken there, not per-batch).
`cargo fmt --all -- --check` clean; `cargo build --lib`/`--release`
clean (zero new warnings — confirmed by diffing warnings before/after).
Full suite green: **1188 passed, 0 failed, 1 ignored** (default
feature set) — identical count to task 006's own last-recorded baseline
in CLAUDE.md, confirming this instrumentation is fully behavior-neutral.

## Chaos test: a DIRECT, DECISIVE experiment that DISPROVES the leading structural hypothesis

Before any natural catch, reading the code produced a strong structural
hypothesis: `execute_fused_streaming`'s own doc comment already states
"Returns Ok(None) to fall back to the materializing path ... the input
is re-executed there." Mechanism read from the code:

1. `execute_fused_streaming` spawns one `tokio::spawn` drain task per
   `self.input.output_partitions()` (32 for Q12's plan — the probe
   side's, i.e. `orders`', parallel-scan partition count). Each drain
   task calls `input.execute(p)`; for `SpillableHashJoinExec`, only
   `p=0` does real work (`execute_spill_path`) — `p>0` returns an empty
   stream immediately after a cheap, shared, `OnceCell`-memoized
   `build_decision` lookup.
2. `execute_spill_path` is NOT idempotent and has NO cache of its own
   output (unlike `build_decision`). It fully redoes build-partition,
   probe-collect, in-memory-probe, and spilled-partition-probe from
   scratch on every call.
3. If ANY drain task errors/panics (`drain_failed`) or ANY aggregation
   worker sets the shared `abort` flag (a `process_batch` error, or —
   structurally impossible for Q12's 2-group shape, `group_limit` here
   is 6,710,886 — a group-count budget trip), `execute_fused_streaming`
   returns `Ok(None)`, discarding its own results.
4. `SpillableHashAggregateExec::execute` then falls through to
   `collect_input_partitions_concurrently(&self.input)`, which spawns a
   FRESH set of per-partition tasks and calls `input.execute(p)` AGAIN —
   for the join, a SECOND, independent, full `execute_spill_path` run.

**This was tested DIRECTLY, not just reasoned about.** A temporary env
var (`QE_SPILL_CHAOS_FORCE_ABORT=1`, since removed) forced step 3 to
fire AFTER the real computation of step 1-2 had ALREADY run to full,
correct completion — isolating exactly "ran once (successfully),
discarded, ran again" without any real error, corruption, or partial
work. Result (2026-08-24, `.scratch/spill_join_repro/serve_chaos1.log`,
`q12_chaos1.csv`):

```
execute_spill_path START spill_id=0 ... build_rows=1765881
execute_spill_path DONE spill_id=0 ... total_matched=1765881  (elapsed 141.3s)
execute_fused_streaming ABORTED ... chaos_force_abort=true ... -> falling back
agg fallback: fused_eligible=true -> (re-)executing input via collect_input_partitions_concurrently
execute_spill_path START spill_id=1 ... build_rows=1765881
execute_spill_path DONE spill_id=1 ... total_matched=1765881  (elapsed 141.4s)
```
Total wall time: **283s** (matches the original wrong run's 291s
closely — confirms this mechanism explains the SLOWNESS pattern
precisely: "the whole expensive computation ran twice" costs
~2x the single-run time, whether or not it's also wrong).

**Final HTTP response: `MAIL,353822,529784` / `SHIP,352224,530051` —
CORRECT, exactly matching DuckDB.** Forcing the EXACT suspected
mechanism (clean full completion, discard, full re-execution) does
**NOT** reproduce the wrong answer. This is a genuine, direct,
evidence-based REFUTATION of the leading hypothesis, not an assumption.
Structurally this makes sense on reflection: `execute_fused_streaming`
returning `Ok(None)` discards `states` unconditionally — there is no
code path where a discarded attempt's partial or full work leaks into
the fallback attempt's count, REGARDLESS of when in the aborted
attempt's lifecycle the abort is recognized (even an EARLY abort can't
meaningfully cut the join's own work short, since `execute_spill_path`'s
"streaming" output is actually a fully-materialized `Vec` computed
before the first item is ever yielded — so "abort timing" doesn't change
this conclusion either).

## Other hypotheses checked and RULED OUT by direct reading (not just inspection-once-more — each traced to a specific, checkable fact)

- **Sort operator (`ORDER BY l_shipmode`, confirmed present via
  `PLAN_DEBUG=1` — `Sort: [l_shipmode Asc]` sits directly above
  `Project`/`Aggregate`/`INNER Join`) re-executing the aggregate.**
  RULED OUT: `SpillableHashAggregateExec` does not override
  `output_partitions()` (grepped — only `SpillableHashJoinExec` has an
  override in this file), so it inherits the `PhysicalOperator` trait's
  default (`src/physical/plan.rs:27`, hardcoded `1`). `ExternalSortExec::execute`
  calls `collect_input_partitions_concurrently(&self.input)`, whose
  `input_partitions == 1` fast path calls `input.execute(0)` EXACTLY
  ONCE (no `tokio::spawn`, no retry). The aggregate is therefore called
  exactly once by its own parent for this query — confirmed via code
  reading, not assumed.
- **Per-process hash-seed randomization** (would explain "outcome fixed
  once per process, varies across restarts"). RULED OUT:
  `partition_batch_by_hash` uses an explicit fixed xxHash64 seed
  (`0x517c_c1b7_2722_0a95`) specifically because a past hashbrown
  version upgrade (0.14->0.17, ahash->foldhash) reseeded per-instance and
  broke exactly this kind of determinism — the comment at that function
  documents the prior incident. Partition routing is deterministic
  across process restarts. (The apparent within-process consistency
  observed early in this investigation is explained by chance at a
  ~5-10% true rate, not a real mechanism — see above.)
- **Build-side or probe-side double-collection.** RULED OUT for the
  build side by the chaos test itself: BOTH the aborted attempt
  (spill_id=0) and the retried attempt (spill_id=1) reported the IDENTICAL
  `build_rows=1765881`, confirming `build_decision`'s `OnceCell` correctly
  shares ONE cached collection across repeated `execute(0)` calls (not
  re-collected each time). The probe side is deliberately NOT cached
  (re-read every `execute_spill_path` call, by design) but both chaos
  attempts independently computed the correct `total_matched=1765881` —
  no duplication from re-reading it fresh.
- **Duplication within a single, clean `execute_spill_path` call**
  (`build_with_partitioning`/`probe_with_spilling`/
  `process_spilled_partition`/`probe_partition`/`build_hash_table`/
  `JoinKey`). Read in full. The build-partition spill bookkeeping
  (`partitions[idx]` vs `spilled[idx]`, mutually exclusive via
  `.take()`) and the probe-side dispatch (in-memory-probe XOR
  spill-for-later, by construction from the SAME idx's build-side state)
  are airtight by construction, matching the prior investigation's own
  read. `probe_partition`/`build_hash_table`/`JoinKey` are simple,
  deterministic hash-table operations with no aliasing or
  double-insertion path found. Every one of the 20 CORRECT runs (with
  full row-count tracing on) reported EXACTLY the right total
  (1,765,881, matching high+low+high+low from the DuckDB oracle) with
  ONLY one `execute_spill_path` call — meaning this code path is not
  inherently/deterministically duplicating; whatever the real trigger
  is, it is genuinely rare and/or timing-dependent, not a always-there
  logic bug that 20/21 runs somehow avoided by luck.
- **Aggregation-state merge (`AggregationState::merge`,
  `morsel_agg.rs:2480`) reconciling per-worker or per-batch DIFFERENT
  dictionary-index layouts for `l_shipmode` incorrectly.** Considered as
  a candidate because Q12 groups by a dictionary-coerced, low-cardinality
  string column, and different spilled-partition result batches could in
  principle carry differently-ordered per-batch dictionaries. RULED OUT
  as an explanation for the OBSERVED symptom specifically: `merge()`
  reconciles via `find_perfect_index(key)`, a real-VALUE (`GroupKey`)
  lookup, not a raw per-batch dictionary index — so even if per-batch
  dictionary layouts differ, merge-time reconciliation is layout-agnostic.
  More fundamentally: a dictionary-index MISATTRIBUTION bug would
  redistribute counts BETWEEN groups (one too high, another too low,
  grand total unchanged) — it would not explain the OBSERVED pattern,
  where BOTH aggregates (`high_line_count`/`low_line_count`) for BOTH
  groups (`MAIL`/`SHIP`) were inflated by the SAME ~1.9925-1.9927x ratio
  (i.e. the GRAND TOTAL itself came out ~2x, not redistributed). Not
  independently chaos-tested (harder to force deterministically than the
  retry hypothesis was), so this is a reasoned elimination from the
  evidence's shape rather than a direct experimental refutation like the
  retry hypothesis got — flagged as the weaker of the two eliminations.

## Slowness/duplication shared-cause verdict

**Partial, evidenced verdict, not a clean yes/no.** The chaos test
proves that "the whole expensive computation running twice" (whatever
triggers it) is SUFFICIENT to explain the SLOWNESS symptom precisely
(291s original wrong run ~= 283s forced-clean-retry run ~= 2x a normal
141s run) — so slowness-when-something-retries is CONFIRMED, mechanism
known, and not itself a correctness bug. But the SAME chaos-forced retry
did NOT reproduce the WRONGNESS — so "shares a root cause" is **NOT
established**: the wrongness requires something beyond a clean two-attempt
retry. It remains an OPEN, evidenced possibility that the wrong run
also involved two attempts (its own timing, ~291s, fits that pattern) but
with some ADDITIONAL corrupting factor the clean chaos test didn't
reproduce (a genuinely mid-computation failure leaving something
inconsistent, rather than a clean post-completion discard) — or that the
timing coincidence is just that, a coincidence, and the true mechanism
is unrelated to retries entirely. Both remain open.

## A separate, real, independently-valuable finding along the way (not
the wrong-answer bug, but very likely explains most of the deterministic SLOWNESS)

`append_to_parquet` (spillable.rs) does a full READ of the ENTIRE
existing spill file plus a full REWRITE to a temp file plus an atomic
rename on EVERY SINGLE call — i.e. every time one more (possibly tiny)
batch needs to be appended to a partition's spill file. With
`NUM_PARTITIONS=64` and (per the instrumented trace, EVERY run) all 64
build partitions spilling almost immediately (`in_memory_partitions=0
spilled_partitions=64`, only ~4% of the eventual data — 70,936 of
1,765,881 rows — was ever in memory at spill time) and 916 build batches
getting hash-partitioned across them, this is an O(n^2)-ish disk I/O
pattern: partition k's spill file is read-and-fully-rewritten roughly
once per build batch that routes ANY row to partition k, and the file
only grows across those calls. The identical pattern applies on the
probe side (`probe_with_spilling`'s own `append_to_parquet` calls, and
here ALL 32 probe partitions' worth of 15,000,000 `orders` rows go
through it too, since `in_memory_matched=0` in every trace — 100% of
matches came via the spilled-partition path). This is a strong,
independent candidate for why even a CORRECT run of this query takes
140+ seconds (vs 150-350ms for every other query) — worth flagging even
though it is a distinct question from the wrong-answer bug (this task's
own charter is instrumentation/root-cause, not a fix; noting this for
whoever picks up a fix, per the task's "report what you'd fix and why,
don't implement it" instruction — NOT attempted here).

## Honest confidence-gate verdict

**Root cause NOT confidently found.** Per the task's own explicit
allowance, this is reported as a complete, valid, honestly-evidenced
outcome rather than stretched into an unconfirmed claim:

- Reproduction: RELIABLE and well-characterized (21 runs, exact
  original fixture/method, 4.8% wrong / 100% slow, explicitly tested and
  quantified for non-determinism as instructed).
- Smaller/faster repro: not found; not attempted beyond varying
  `--memory-limit` (which didn't change anything, since 40G was already
  saturating all 64 partitions into the spill path).
- Instrumentation: real, direct, row-counting/call-counting/error-capturing
  tracing added and left in place (gated, clean, tested); DID catch and
  characterize the CORRECT signature precisely across 20 runs; did NOT
  catch a NATURAL occurrence of the wrong signature within the 21-run
  budget spent (1 wrong run occurred before instrumentation existed).
- Root-cause mechanism: the single most obvious, code-documented
  candidate ("operator above retries child") was directly, experimentally
  DISPROVEN via a controlled chaos test — a genuinely valuable negative
  result, not a null result. Several other candidates were eliminated by
  direct reading with specific supporting facts (see above), not by
  "reading the code again and it looked fine."
- Slowness/duplication shared-cause: PARTIALLY resolved — slowness is
  fully explained by "computation ran ~2x" (confirmed); wrongness is
  NOT explained by the same mechanism in its simplest (clean, sequential
  retry) form.

**Best remaining hypotheses for whoever continues this investigation:**
1. A genuinely MID-COMPUTATION failure (not a clean post-completion
   discard like the chaos test simulated) — e.g. a real transient I/O
   error inside the extremely I/O-heavy `append_to_parquet`-based spill
   path (tens of thousands of open/read/write/rename cycles per query),
   occurring DURING `build_with_partitioning` or `probe_with_spilling`,
   possibly leaving SOME state (memory-pool accounting, an orphaned
   spill directory, or something not yet identified) inconsistent in a
   way the clean chaos test's "let it finish, then discard" shape can't
   exercise. Next step: instrument `append_to_parquet`/
   `write_batches_to_parquet`/`read_parquet` themselves (call counts,
   error capture) to see if any of the 20 correct runs even come close
   to an I/O error, and if a natural ABORTED trace line is ever caught
   (none was, in 21 runs, but the sample including a genuine catch is
   only n=1 without tracing).
2. A mechanism entirely outside the retry/re-execution family this
   task's instrumentation targeted — e.g. something in expression
   evaluation, batch concatenation, or Arrow buffer aliasing under a
   SPECIFIC (rare) partition-size/batch-boundary condition that produces
   a batch whose "logical" row count and "physical" row count diverge.
   Not investigated (the task's own named functions and the
   retry/dictionary hypotheses were higher-prior and consumed the
   available budget).
3. Build a downsized synthetic repro (small SF or a hand-built dataset
   forced through the identical 64-partition/all-spill shape at a much
   smaller row count) specifically to get the per-attempt cost down from
   140s to seconds, enabling hundreds of trials instead of ~20 — the
   single highest-leverage next step if this investigation resumes,
   NOT attempted here because the natural/chaos-test path looked more
   directly informative per attempt and consumed the available time
   first.

Given this, **task 002 (the fix) should NOT proceed** per the epic's own
architecture decision ("No guess-fixes... If task 001 can't reach real
confidence in the mechanism, task 002 does not proceed this epic") —
this task's own honest conclusion is the epic's likely stopping point
unless a follow-up investigation is separately chartered.

## Reproduce this investigation

```bash
# 1. Confirm the fixture exists (built by native-tables-foundation/mutation,
#    unchanged): data/tpch-10gb-native/{orders,lineitem,...}
# 2. Start the server (setsid REQUIRED for a backgrounded server to survive
#    this sandbox's per-tool-call teardown — plain nohup/disown alone was
#    observed to fail intermittently, exit code 144, no log file, no process):
QE_SPILL_DEBUG=1 setsid nohup target/release/query_engine serve \
    --bind 127.0.0.1:PORT --tables data/tpch-10gb-native \
    --flight-bind none --memory-limit 40G < /dev/null > serve.log 2>&1 &
disown
# 3. Wait for readyz, then:
curl -sS -m 470 -X POST "http://127.0.0.1:PORT/sql?format=csv&distributed=0" \
    --data-binary @Q12.sql
# expect 140-291s; correct = MAIL,353822,529784 / SHIP,352224,530051;
# grep serve.log for "sj-trace" to see the full execution trace.
```
