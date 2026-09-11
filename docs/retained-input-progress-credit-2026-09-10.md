# Retained scan output and input progress credit

The admitted memory scan imposed a hard local cap on copied output retained by
its consumer. A generic regression reproduces refusal with25,158,968bytes still
available in the query parent. Current source preserves the initial input progress
reservation and admits additional retained output against the same query budget.
The canonical LanceQ7 refusal names this same pool; a rebuilt workload run is
still required to establish whether this fixes that complete query.

## Reproduction

Frozen baseline fde271b1 has519source inputs. The new test
`retained_outputs_can_grow_beyond_input_progress_credit` constructs600,000Int64
values, accounts4.8MBof resident source separately, and collects copied outputs
under a32MiBquery budget. Red session72396 refuses in `memory scan working space`:
requested520,used3,594,048,limit3,594,304, with25,158,968parent bytes free.
The source reserves available()/8 up front and used to treat that reservation as
an absolute output-lifetime limit. A small retained-batch test did not exceed it.

Command: repository TMPDIR, SAFE_BUILD_MEM=48G, SAFE_BUILD_JOBS=1,
`scripts/claude-safe-build.sh cargo test --locked --offline --lib retained_outputs_can_grow_beyond_input_progress_credit`.
Exit101is the expected reproduced failure; the assertion requires successful
completion, exact sequential values and final cleanup to zero.

## Accounting contract

`MemoryPool::child_with_progress_credit(parent,name,B)` reserves minimum creditB
before producing input. Its attributable parent charge is `max(B, child.used)`.
Within the unused credit, later sibling reservations cannot prevent input progress.
Above the credit, every additional byte is admitted through all query ancestors.
Releasing excess returns it to the parent; releasing below the floor preserves
credit until the last child/output owner disappears. A consumer never needs to
release earlier output merely to unlock a queue token.

The existing hierarchy mutex protects validation, commit and release. Admission
is checked at all ancestors before any usage or peak changes. Nested credits
propagate only the excess delta. Availability includes unused protected credit,
constrained by local and ancestor limits. Existing hard prepaid children retain
their original behavior, including scheduling-credit lifetime ordering.

Memory scan preparation keeps its initial available()/8 credit, the existing
per-frame byte cap, quantum, selected-stream terminal errors and schema checks.
Only retained-output growth changes. There is no budget opt-out, replay, dependency
change, new ownership default or SQL specialization. The registered resident source
still has its existing separate ownership/admission contract.

## Validation and limits

Session86260, locked/offline lance,gpu,48GiBscope/1buildjob: **1088library passes,
11ignored**, including retained outputs, original input-progress/refusal tests,
failed-resize atomicity, parent-full progress, nested hard/growing pools and
8threads each performing2000grow/release cycles with exact final counters.
Partial ownership in session73252 also passes1088library/11ignored.
Corrected memory/prepared-stream session91137 passes41integration tests.
The initial memory integration compilation failed because the existing opaque
pool owner prevents automatic RefUnwindSafe inference. The frozen control archive
contains that same owner and test closure. The test now uses AssertUnwindSafe only
around its fresh owner-free root and deliberate panic; its cleanup assertions stay.

Both-mode native/spill driver73252 is terminal101. Sorted failure names match
fde271b1: default native58passes/2dictionary-plan failures, partial53passes/7failures;
each mode has7focused decimal/parallel-spill passes, full spill8passes/6failures,
and systemic numeric11passes/1failure. These unresolved gates are not promoted to
passes. Formatting and whitespace checks pass.

The two engine files `src/execution/memory.rs` and
`src/physical/operators/scan/admitted_memory.rs`, plus the unwind test in
`tests/memory_reservation_contract.rs`, differ from fde271b1. The
[immutable evidence archive](benchmarks/2026-09-10-retained-input-credit/manifest.json)
retains red/green logs, failure comparisons, the519input source snapshot and hashes.
No new optimized benchmark or Lance completion claim is made by these tests.
A true global query-memory refusal remains an allowed resource-safe outcome.


## Release validation

Release42098 completed in8m52s, freezing38966ae6 with519verified source inputs.
The matched LanceQ7 diagnostic72127 reproduces two old-binary refusals and two
new-binary typed-correct completions at553–557ms under unchanged4GiBquery and
12GiBprocess budgets. See the [191-file diagnostic archive and analysis](retained-input-credit-q07-2026-09-10.md).
Shared comparison88178 completed64correct outputs. Full provider4926 validates
333completed outputs including3LanceQ7samples, but other deadline/reference failures
remain. See [current provider evidence](retained-input-credit-provider-screen-2026-09-10.md).
No full provider, regression or resource acceptance is claimed.
