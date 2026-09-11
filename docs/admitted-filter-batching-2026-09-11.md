# Admitted post-filter batching reproduction — 2026-09-11

The SF10 checkpoint `0c554142` has been pushed to the epic branch and its exact
remote hash verified. This next cycle starts from that committed implementation.

Focused matrix 78387 is terminal with two passes and four expected failures.
Empty selection passes for fixed-width and string outputs. Dense and sparse
selection fail their batching assertions for both layouts. Independent expected
values, NULL handling, duplicate multiplicity, all declared partitions, retained
output admission and cleanup to zero pass before the batching assertions.

The fixture has two row groups of 2,049 rows and 64-row data pages, with a requested
32-row output quantum. A nullable integer predicate is absent from the projection.
Output layouts are nullable Int64 alone and nullable Int64 plus Utf8. The ordinary
Parquet reader is a batching control, while the value oracle is computed directly
from fixture construction. The admitted reader returns smaller filtered input
quanta instead of filling the selected-output target across input chunks. Dense
selection returns mostly 27–28 rows; sparse selection is smaller still. This is a
batching/work-amplification reproduction, not a SQL wrong-result finding.

Initial red 71325 also reached the intended assertions, but each layout ran its
cases in one loop: dense failure prevented the sparse case from running. That
attempt is preserved. Matrix 78387 makes all six cases independent.

Both invocations use the required 48 GiB wrapper, one build job, repository TMPDIR,
locked/offline default features and a single test thread. No production behavior
has changed yet: only the test module and its cfg(test) declaration were added.
The source archive verifies 528 inputs.

Next implement admitted post-filter output construction with exact cursors and
bounded retained memory, alongside the separate first-batch multi-column admission
reproduction. Remove unnecessary predicate-only intermediates as part of that
pipeline; do not replace memory admission with estimates or replay consumed input.
After focused and broader gates, compare against both frozen binaries, then run
SF10, commit, push and continue at the next completed cycle.

- [Complete red evidence and source](benchmarks/2026-09-11-admitted-filter-batching-red/manifest.json)
- [Measured checkpoint](admitted-quantum-checkpoint-2026-09-11.md)
- [Implementation sequence and source analysis](admitted-planned-quantum-2026-09-11.md)

## Projected survivor construction component

Current production source adds `admitted_gather::filter_projected`: validate output
positions and types, build the SQL valid-and-true selection, and copy only final
columns into reserved output buffers. Repeated/reordered columns and empty output
schemas remain valid. No temporary `RecordBatch::project` or unreserved payload is
introduced. Static-only scans now use the admitted predicate's nullable bitmap
directly; intersected runtime masks retain their existing valid-and-true semantics.
The scanner no longer gathers predicate-only columns and then creates a second
projected handoff.

Focused gather tests 32346 pass 5/5. A controlled unused megabyte UTF8 column makes
the full gather refuse 32 KiB, while projected output succeeds with exact values
and retained leases. The tests include nullable masks, decimal scale, float bits,
slices, duplicate positions, zero-column results, invalid shapes and cleanup.
Scanner integration passes 16 and retains exactly the four known batching failures.
No additional scanner failures appeared. The batching fix is still incomplete and
no performance improvement is claimed. All jobs are terminal; source 528 and logs
are archived in the [component evidence](benchmarks/2026-09-11-admitted-projected-gather/manifest.json).

The next output accumulator must avoid collecting an unbounded list of batch
owners or repeatedly concatenating growing arrays. Use admitted typed column
buffers with fixed row capacity and bounded UTF8 bytes, plus one retained input
and an exact selected-row offset. Reserve final handoff metadata before consuming
rows. Check all columns and capacities before publishing a consumed prefix.
A construction refusal may flush a nonempty prefix without replay; decoder/source
errors remain terminal. A first complete admitted chunk may bypass optional
packing when packing cannot be admitted, but a partially consumed chunk must never
be returned whole. Oversized UTF8 values must progress or refuse by name, never
spin. The separate first-batch multi-column working-space problem remains open.

## Bounded accumulation candidate

Current source adds `storage/admitted_coalesce.rs` and direct component tests.
Typed fixed-width buffers preserve integer widths, float bits, decimals, dates and
timestamps. Boolean values and validity use reserved bitmaps; UTF8 has a reserved
row-offset buffer and a separate bounded byte capacity. Final output-vector and
handoff metadata are reserved before consuming rows. The builder checks every
input column and capacity before appending, and returns the exact consumed prefix.
It retains column buffers rather than a growing list of input batches. Temporary
construction metadata remains charged until final handoff owners take over.
`admitted_batch::finish_reserved` validates and transfers that existing reservation.

The admitted reader retains at most one pending filtered batch and its consumed
row offset. It fills an output target across input quanta, preserving a suffix
across calls when the target or UTF8 byte cap is reached. A whole already-admitted
chunk may bypass optional packing on a construction-only memory refusal; a partly
consumed chunk cannot be returned whole. A single oversized UTF8 value progresses
through its admitted chunk. Decoder/source errors remain terminal, including when
an otherwise valid output prefix has been buffered. This does not fix cross-column
first-batch page working-space admission or certify all resource gates.

Initial scanner64668 passes20, accumulator72990 passes4, boundary79397 passes22,
and final scanner49083 passes23. The four batching reproductions now pass. Added
coverage checks all supported primitive representations, float NaN/zero bits,
negative decimal scales, timestamp timezone, slices, UTF8 byte boundaries and
oversized values, zero-column results, pre-reserved finish with no available pool,
whole/partial packing refusal, and terminal I/O failure after buffering a prefix.
Formatting and source snapshot verification pass. Source530 and all focused logs
are in the [candidate archive](benchmarks/2026-09-11-admitted-coalesce-source/manifest.json).

Both-mode Lance/GPU validation84021 is terminal1. Each mode passes1125 library
tests (11ignored),125 contracts and28 spill/numeric checks; native/IPC passes63
in disjoint and62 with1failure in partial. The same6spill failures remain in each
mode. Complete executable/count/exit checks find no added or removed failures.
The26-file validation archive verifies530inputs; peak21864136704bytes under48GiB,
swap0 and zeroOOM/max events. [Validation archive](benchmarks/2026-09-11-admitted-coalesce-validation/manifest.json).

Release63185 terminal0 in8m48s freezes1bba20b3 with530verified inputs.
Checkpoint11187 is terminal1; evidence76516 is terminal0 after independent
comparison and archive verification. Source remained frozen through measurement. No throughput
or complete memory-progress claim is made from focused or existing resource tests.

## Local engine comparison: what batching does not solve

Read-only inspection of local DuckDB commit
`1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8` found no working-tree changes to
`extension/parquet/parquet_reader.cpp`. Its filtered scan registers non-filter
columns for lazy fetching, evaluates filter columns first using an adaptive order,
then skips remaining columns when no rows survive or calls their selective reader
with the selection vector. See [prefetch registration](https://github.com/duckdb/duckdb/blob/1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8/extension/parquet/parquet_reader.cpp#L1366)
and [filter/select execution](https://github.com/duckdb/duckdb/blob/1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8/extension/parquet/parquet_reader.cpp#L1433).

That code also slices a scan chunk to its surviving rows; it is not evidence that
DuckDB always refills every filtered output chunk. Our packing repair addresses
cost at this engine's owned RecordBatch boundaries. Lower batch counts alone do
not prove equivalent decoding, allocation or downstream processing costs.
The admitted reader still decodes the union of predicate and output columns before
applying its mask. Projected gather avoids copying predicate-only survivors, but
it does not avoid decoding rejected payload rows. After the frozen comparison,
attribute remaining time to decoding, numeric conversion and batch construction;
consider selection-aware column reads with exact skip/cursor and memory contracts.
The reference code is a design comparison, not proof of query-wide reservation or
safety under this project's budget contract.

Checkpoint11187 triage is terminal0 with42typed-correct executions. Direct queue
trace inspection confirms both generic rawQ6 candidate blocks produce458batches
for1139264selected rows, versus7323batches on beb0cdfd. The pre-coercion baseline
also produces458. This establishes restoration of runtime output batching; it
does not establish recovery of all earlier performance or selective decoding.
Full SF10 screens and audits are terminal. Generic rawQ6 ratio is0.897777 versus
beb0cdfd but1.828793 versus13210a20; native/resident4Q9 regress versuscontrol in
both blocks. See the [completed checkpoint](admitted-coalesce-checkpoint-2026-09-11.md)
for complete ratios, failed gates and source/evidence verification.
