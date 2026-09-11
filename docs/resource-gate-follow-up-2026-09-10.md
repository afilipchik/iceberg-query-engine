# Resource gates: separate startup admission from spill validation

The historical audit below led to the [startup admission repair](aggregate-startup-headroom-2026-09-10.md).
Final validation99343 clears all four prior partial-native memory failures:
10/10 native streaming tests pass in each ownership mode, alongside1090 library
passes/11ignored and82 contract integration passes per mode. Other recorded resource
failures remain. The37-file correctness archive verifies520 source inputs.
Optimized release008d92f7 is complete; paired session81205 completed against
38966ae6 with64typed-correct outputs. Native2–4% and rawQ9 3.97% mean slowdowns
remain; see the [comparison](startup-headroom-measurement-2026-09-10.md).
Candidate38966ae6's preceding regression and provider screens are complete.
No ownership default changed.

The ordering and proposed steps below describe the source **before** that repair;
the linked report documents the current implementation and reproduced boundaries.

## Worker/input admission ordering

`live_spill.rs` constructs ParallelControllers before InputFrontier. Grouped default
execution caps owners at4; experimental partial execution caps them at16. Worker
constructors admit their own state and fall back to one worker only when that
construction fails with a typed memory limit. Partial construction also admits the
final merger and optional parallel merger before opening input.

After construction succeeds, input preparation and the first retained batch still
need the same pool. Successful worker admission therefore does not establish that
one input batch fits. An unadmitted incoming batch gets a retained-byte lease only
after arrival. On denial, release_for_input can release resident group capacity,
but it does not inherently abandon an unused worker/merge set. Source facts are in
`live_spill.rs:135–168` and `parallel_controllers.rs:496–602`.

The four known partial native failures are consistent with a startup headroom
problem; the allocation site must still be reproduced before assigning their exact
cause. A request of163434bytes at183961used exceeds262144, but those byte numbers
alone do not identify which operator owns every reservation.

Next steps:

1. Reproduce the first failing allocation with typed ownership evidence and counters
   for input preparation, output pulls and committed rows. Include an independent
   expected aggregate and verify cleanup to zero. Do not infer source replay safety
   merely from zero output pulls: preparation may already execute join builds.
2. Reserve a complete startup envelope where a certified input bound exists. For
   unknown native output bounds, do not invent a proof from estimates or mmap size.
3. If denial occurs with a retained first batch and no row has ever been committed,
   consider reducing unused workers while keeping that same batch and prepared
   input. That must not reopen the source, reevaluate expressions or change a
   populated ownership map. It requires explicit empty-state/merge-ledger proofs.
4. If input preparation itself fails, retain terminal-error semantics. Dropping
   controllers and preparing the child again is not a safe general fallback.
5. Validate enough-budget parallel startup, constrained successful startup and
   impossible-budget named refusal in both ownership modes, including volatile
   expression counters, late errors and partial construction cleanup. Only then
   revisit automatic ownership costing with low/high NDV, skew and4/16threads.

## Legacy integration gates do not fully prove spill correctness

`tests/spill_tests.rs::assert_spill_matches` requires successful completion at the
chosen budget, checks only that spill_metrics is Some, and compares formatted
rows from two engine executions. Floats are rounded to3decimals. Several join
cases demand completion at8KiB; current runs stop at named metadata reservations
near that cap. Other256KiB cases stop at whole-page or retained-result admission.
These are real failures of the tests' completion contract. They do not by themselves
prove a wrong SQL result or unbounded memory use.

The helper's Some check is weaker than nonzero spill bytes, and engine-versus-engine
agreement can share a semantic bug. COUNT DISTINCT additionally pins independent
historical counts, but those assertions follow the failing comparison and are not
reached in that run. The retained-aggregate test already provides a stronger model:
separate low-budget typed refusal from a fitting independently typed, actual-spill
completion fixture.

Next steps:

1. Preserve the existing failures and their commands in the historical reports.
   Do not label a raised budget or changed assertion as an engine performance fix.
2. Keep deliberately impossible budgets as explicit named-refusal/cleanup tests.
   Preserve any feasible completion requirement that exposes a source bug.
3. For spill completion, use a declared budget that fits decoder/metadata/output
   necessities and enough rows or an explicit spill threshold to force real disk
   work. Require bytes_spilled >0, not merely Some(metrics).
4. Compare typed values with an independent oracle for inner/outer/semi/anti joins,
   duplicate and NULL keys, ON residuals, filtered misses, multiple partitions and
   empty/nonempty outputs. Pin fixture provenance. Do not round away float errors.
5. Report the coverage change separately from source repairs and rerun both
   ownership modes. These corrected gates do not replace canonical SF10/SF100
   resource, provider or concurrency acceptance.

No test budget, expectation, engine default or source file changes as part of this
audit. The six spill failures and partial native failures remain open.


## Fresh read during the progress-credit comparison

The four partial native refusals persist with the same failure names after the
memory-scan credit repair. `live_spill.rs` admits an unadmitted retained batch
before incrementing the input counters or evaluating its expressions. Its retry
calls `ParallelControllers::release_for_input`, which visits existing workers.
`IngestionController::release_for_input` returns false for an empty group table;
it does not release unused worker writers, row/key workspaces or partial-merge
reservations. Thus the existing path cannot reclaim an unused startup envelope.
The 163434-byte failure is consistent with this boundary; allocation-site tracing
is still required before calling that the runtime cause.

A debugger command file is prepared in repository scratch for the existing exact
native aggregate test, stopping at the 163434-byte reservation and recording its
call stack. Run only after the active benchmark terminates, inside the capped
wrapper, with partial ownership and a bounded watchdog. Keep test-source and
binary hashes with the trace. No source edits or debugger perturbation during the
performance comparison.

Any later shrinking implementation needs a positive never-ingested-state proof,
not `groups.is_empty()` after a spill. Retain the same incoming batch and prepared
frontier; never call the child factory again. Drop only unused workers/merge
ledgers, preserve resource-safe finalization and handle refusal if the remaining
single-owner envelope still cannot fit. `ReservedVec::truncate` drops suffix owners
but keeps its own allocation; include that retained capacity in the accounting.
Per-worker group limits and preallocated writer bounds also need review before
assuming an existing worker is equivalent to a freshly constructed single owner.

The prepared owned trace subsequently completed as2503 and confirmed the retained-input allocation site. See [full evidence and limits](partial-native-input-attribution-2026-09-10.md). Worker-startup reclamation is still unimplemented.

## Remaining denials on startup-corrected source

Final disjoint validation99343 still records these named refusals in
`.scratch/parallel-aggregate-input/startup-headroom-disjoint-spill.log`:

| Test family | Requested bytes | Used bytes | Budget bytes |
|---|---:|---:|---:|
| Semi/anti joins, including residual-filter case |37376|7641–7645|8192|
| COUNT DISTINCT |32117|253449|262144|
| Inner/outer joins |6168|257773–257794|262144|
| Independent exact-decimal aggregate spill |512|261649|262144|

These are unchanged failure names, not proof of identical allocation ownership.
The decimal case retains a typed `Partition -> MemoryLimit` cause. Its fixture
contains20 batches of1000 groups and requires a spilled exact decimal sum under
256KiB. The512-byte request needs allocation-site attribution before changing
state, merge or output scheduling. Do not infer the owning component from request
size alone, remove the independent oracle or raise the budget to make it pass.

After the frozen startup measurements, reproduce that single decimal denial in
an owned capped diagnostic, identify live input/state/merge/output reservations,
and test a generic lifecycle repair against both enough-budget spilling and
impossible-budget refusal. Keep the tiny8KiB join cases distinct: their requested
allocation alone exceeds the whole budget, which still requires attribution to
determine whether it is an avoidable batch quantum or an irreducible floor.

Owned trace46836 is terminal0 at the debugger-protocol level; the exact decimal
test still fails (0passed/1failed/11filtered). Three512-byte denial stops locate:
optional worker KeyRows construction at261705used, fixed-cell growth at261882used,
and the terminal GroupRows::reserve_index at261649used while processing from
row24. The latter is ingestion/index growth, not final result construction.
This does not yet identify all live reservations or prove the eventual repair.
The48GiB scope records1,898,057,728peak bytes and zero max/OOM events;520source
inputs verify. The [6-file trace archive](benchmarks/2026-09-11-decimal-spill-denial/manifest.json)
preserves all stacks, exact command and debug-binary identity, linked to final
startup source. Next inspect reserve_index and spill/retry lifetime accounting.

### Candidate cause to verify: copied queue admission competes with spill progress

Source inspection after trace46836 shows `InputFrontier::new` chooses the largest
copied-output slot count whose entire envelope fits the currently available pool.
It reserves that envelope before aggregate workers. It does not reserve a minimum
aggregate/spill working set while selecting the slot count. This is distinct from
the repaired optional-worker startup ordering: the producer envelope can itself
consume downstream progress space.

The20,000-row decimal fixture declares up to16 MemoryTable partitions. At256KiB,
the admitted memory path's per-frame minimum cannot fit its initial one-eighth
budget, so it declines before streams. The copied path is therefore relevant;
the actual selected frontier slots and envelope still need runtime confirmation.
Do not claim their size from the final512-byte denial alone.

After provider61495 completes, enable the existing input-frontier trace on the
same exact regression, retaining typed failure and no-replay evidence. Attribute
the retained envelope alongside run count and resident state at the empty-state
refusal. A generic repair must balance input concurrency with downstream progress,
release only idle capacity or preserve outstanding batch ownership, and leave
all declared partitions consumed exactly once. Arbitrarily raising the test budget,
unaccounting queued buffers, or restarting the source would not fix this contract.

### Local DuckDB comparison: reservations constrain concurrency

Rechecked local DuckDB HEAD1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8,
`src/execution/radix_partitioned_hashtable.cpp:217–234` computes a sink minimum
reservation from row width, hash capacity, radix partitions, block allocation and
thread count, then registers it with the temporary-memory state. At lines676–689,
finalization derives usable memory after stored allocator costs and limits concurrent
partitions according to the reservation and maximum partition size.

This is source evidence for coordinating concurrency with operator working memory;
it is not proof of strict RSS bounds or a recipe to copy those constants. Our
copied frontier currently admits slot envelopes before reserving aggregate progress.
The next repair should make their competition explicit, with reserved minimum
progress ownership and costing separate from hard admission. Keep the unknown
source first-pull contract and terminal errors established by the startup repair.
Do not restore worker-first preparation or infer SQL facts from size estimates.

Runtime frontier trace79319 is terminal0 (debugger protocol), with the decimal
test still failing. It confirms MemoryTableScan has16 declared partitions,
9 copied slots,25340-byte per-slot bound and228060-byte retained envelope under
262144-byte query budget. Admitted buffers are false. The same three512-byte
stops recur. Thus copied queue admission holds87.0%of the whole budget before
frontier metadata, expression scratch, aggregate state and spill-run metadata.

The [6-file frontier archive](benchmarks/2026-09-11-decimal-spill-frontier/manifest.json)
verifies520source inputs. Scope peak1,872,535,552bytes,zero max/OOM events.
All timed provider jobs completed before this diagnostic. This confirms the
competing reservation; a generic red/green regression must establish the repair,
including exact partition consumption and outstanding copied-batch ownership.
