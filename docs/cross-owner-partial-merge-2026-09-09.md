# Cross-owner partial-state merge — 2026-09-09

`partial_merge.rs` implements the merge boundary needed before changing the
fixed-key worker ownership that limits low-cardinality/skewed grouping. The original
component gate below precedes the subsequent [experimental live integration](balanced-aggregate-ownership-2026-09-09.md),
which adds bounded adoption of worker spill runs. Default controllers still own
disjoint keys; no performance improvement is claimed without measurement. The measured motivation remains the [Q1 owner imbalance](q1-aggregate-owner-skew-2026-09-09.md).

## Ownership and progress contract

`PartialMerge::new` admits its resident group/key/state workspaces, one spill
writer and a publication slot before local workers fill the shared query pool.
`ingest` borrows complete local GroupRows partial states. It compares canonical
keys and applies existing checked whole-state transactions; finalized AVG or
COUNT DISTINCT values are never merged. Existing layout eligibility remains the
caller contract, including the existing DISTINCT fallback.

When the target fits, no data is spilled. On actual typed target admission
refusal, or its working group-count threshold, the consumer writes its prior
merged state to the prepared writer, drops resident capacities, and appends the
current uncommitted source row followed by remaining partial rows. Every source
row exists exactly once in the resident accumulator or spool. The original
source stays alive until transfer succeeds; no source expression or row prefix
is replayed. Non-admission errors poison the consumer.

Once spooling starts, the same writer receives all remaining local partials. It
does not allocate replacement writers or compact runs while other local owners
occupy memory. Direct partial-row framing uses the existing streaming serializer;
no second materialized row buffer is needed to escape target admission pressure.
The number of run handles is bounded at one; disk volume still reflects partial
state size and IO failures remain terminal. This deliberately keeps the fitting
case in memory instead of making all aggregation write to disk.

After local owners are released, `finish` flushes and validates the writer's
complete file length before publishing its pre-admitted run. The existing
PartitionScheduler merges equal canonical keys with bounded state and recursive
partitioning before invoking output callbacks. HAVING and final Arrow output
belong after this boundary. An in-memory finish synthesizes the existing empty
global COUNT/AVG result. `RunCollection::publish_writer` checks layout identity
and publishes into its reserved slot; failed publication releases the run.

## Validation and limits

Initial focused54353 failed to compile because the transactional closure needed
an explicit `Result<bool>` type. Corrected30644 passes three new merge tests plus
one existing indexed-merge test selected by the same filter. Four local owners
contain overlapping keys and independently expected COUNT10, exact Decimal128
SUM30×((1<<80)+17), weighted AVG30, NULL keys and all-NULL SUM/AVG groups.
The tests cover fitting state, a seven-group working limit, and fully exhausted
query memory while all source owners remain retained. Spill paths assert actual
bytes; all paths check unique complete groups and reservation/file cleanup.
Foreign layout input poisons the consumer; subsequent ingestion/finalization
cannot publish. An inconsistent spool extent fails validation before output.

The fourth new test checks startup refusal without owner leaks and empty global
COUNT0/AVG NULL without spill. Broad98836 terminates0:1,046 library passes/11ignored plus8 parallel-spill,
admitted-HAVING and typed-pressure integration passes. Commands use
`TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1
scripts/claude-safe-build.sh cargo test --locked --offline --features lance,gpu`.
Focused arguments: `--lib partial_merge`; broad arguments: `--lib
--test parallel_input_spill_contract --test aggregate_post_filter_admission
--test typed_memory_pressure`. Full spill92656 terminates101:8pass/6same failed names. Formatting and
whitespace checks pass. The [component archive](benchmarks/2026-09-09-cross-owner-partial-merge/manifest.json)
preserves509 source inputs and all build/test logs. Only morsel_agg.rs and
spill_files.rs change alongside the new partial_merge.rs, relative to the
preceding typed-error/spill archive. No dependency change.

## Next integration

Add an explicit partial-ownership controller constructor that reserves this
consumer before opening input. Route retained evaluated rows in balanced chunks,
keeping original row indices and the existing prepared-key identity. The complete
controller set must be owned before selecting the mode; startup refusal may
choose an ordinary compatible path before consumption. One-worker operation
still needs exactly one valid finish path.

Each local controller's partial-state callback feeds this consumer. Release each
local controller as it finishes; invoke the cross-owner finish only after all
have dropped. Include its spill/merge metrics exactly once. No concatenated final
output or HAVING may bypass this final merge. Add consuming-source, dictionary,
NULL/decimal/AVG, skew/balanced, 1/4/16-worker and low-memory integration tests
before enabling cost-based selection. Measure scaling and regressions on frozen
sources; worker count alone does not establish improvement or DuckDB leadership.
