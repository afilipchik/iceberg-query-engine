# Borrowed fixed aggregate input and in-place publication

The direct-update candidate improved native/residentQ1 but left a repeatable
residentQ18 ingestion regression. Debugger PCs identify repeated temporary scalar
and transaction-token moves as the next source-level hypothesis. This candidate
removes those moves without selecting queries or changing default ownership.

## Source contract

`key_rows::arrow_input::with_inline` resolves and checks the same Arrow cells as
before, then gives a short-lived scalar reference to a consumer. Only the consumer's
result crosses the call boundary. NULL, dictionary codes/values, integer widths,
decimal scale and raw float bits retain the existing decoder. The owning `inline`
adapter now wraps this shared decoder for callers that need a scalar result.
There is no input-binding allocation, new dependency or per-query exception.

Fixed numeric array updates consume the reference immediately inside StateRows'
reserved scratch row. COUNT still uses checked logical validity and never needs
a numeric conversion. Consumer errors propagate once; invalid row access fails
before invoking the consumer. Unsupported selected payloads stay on their prior
selected-value admission path.

PreparedGroup retains its consuming commit method. It calls PreparedRow::publish
through the existing Option's mutable reference, avoiding extraction and copying
of the transaction token. Publication still copies reserved fixed scratch, commits
selected replacements and clears rollback only after successful preparation.
The group's Drop clears its contained state in place before key rollback. Exclusive
row/workspace borrows and index-capacity admission are unchanged; the public
transaction operation remains single-use.

## Validation checkpoint

Focused aggregate82858 is terminal0:162passes/1ignored,932filtered. Full default library68938 and partial36369 are terminal0:1084passes/11ignored
each. Both-mode integration15226 is terminal1. Failure-name sets match ea1e9019:
native/IPC/mutation/dictionary default58pass/2plan failures; partial53pass/7fail
(2plan,4memory refusals,1formatted-float comparison). Each mode passes7focused
decimal/parallel-spill tests; full spill8pass/6fail and numeric11pass/1fail remain
unresolved. Failure logs are preserved, not converted to skips or successful gates.
Formatting and whitespace checks pass. Source hashes confirm exactly three changed
inputs versus ea1e9019: arrow_input.rs, state_rows.rs and group_rows.rs. Commands use locked/offline lance,gpu,48GiB cap, one build
job and repository TMPDIR. The new callback regression checks bit-exact signed
zero and repeated selections, NULL, invalid-row rejection before consumption and
terminal callback error without replay. Existing codec, dictionary, row rollback,
selected payload and independently typed spill tests remain required. No optimized
measurement exists for this source yet.

The preceding frozen attribution archive contains34verified files/519source inputs:
[attribution](benchmarks/2026-09-10-direct-fixed-q18-attribution/manifest.json).
That evidence is a hypothesis guide, not a hardware-counter proof of stalls or
certification of this implementation. Compare the next frozen candidate with c20b0648.

The correctness archive contains16verified files/519source inputs at
[manifest](benchmarks/2026-09-10-borrowed-fixed-input/manifest.json). Release78412
completed in8m49s through build_borrowed_fixed.py with before/after source hashes,
frozen fde271b1. [Paired51623](borrowed-fixed-measurement-2026-09-10.md) completed
64typed-correct outputs and recovered the residentQ18 regression, with nativeQ9
slower. Full provider19994 is active. Source
and harness changes are paused through the optimized comparison.
