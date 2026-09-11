# Aggregate output batching audit — 2026-09-09

The current frozen key-binding candidate is `0f30c946`; no output change is
implemented by this audit. Its instrumented Lance Q18 block0 spends1345.923 ms
in final output after the key-binding improvement. That wall interval includes
Arrow construction and HAVING filtering, not an exclusive allocator profile.
The earlier sampling profiler had a timer-change warning; its percentages cannot
establish which output component dominates.

## Reproduced source condition

`physical/morsel_agg/live_spill.rs` finalizes complete merged groups using
`(0..groups.len()).step_by(64)`. Each range reserves result collection growth,
calls `GroupRows::build_output_range`, then applies an already bound HAVING
predicate. `admitted_output.rs` validates the output schema and computes batch
metadata on each call. Per non-UTF8 column it constructs admitted scalar/value
and reference vectors before constructing the final admitted Arrow array.

Thus the output batch limit is fixed independently of available query memory.
The source proves repeated work; a controlled measurement must establish whether
changing its granularity materially lowers time. This is distinct from the
per-row key-binding candidate and must use a different frozen binary.

## Bounded next candidate

1. Keep the existing pure `build_output_range(&self, ...)` construction and exact
   payload/type/batch admission. Bind a target output quantum once for finalization,
   initially capped at1024 rows. The cap is a policy to measure, not evidence that
   the allocation fits. Do not raise the query budget or reserve untracked memory.
2. Construct a complete candidate output range before publishing it. If construction
   returns a typed memory-limit error, ensure all temporary owners are dropped,
   then halve that same range down to one row. Retain the successful quantum for
   later ranges within that completed group owner instead of repeatedly attempting
   a known oversized range. Reset the target at the next owner boundary, where
   retained state and available memory can differ; a short final range alone must
   not lower the target. Other
   errors propagate immediately; an unsatisfied one-row request refuses by name.
3. Only construction is eligible for this retry. Apply HAVING exactly once after
   a batch has been constructed successfully, and preserve its original complete
   merged-group semantics. A filtering, collection or downstream refusal remains
   terminal until it has its own proved retry contract. Do not retry source input,
   volatile expressions, partial aggregate updates or already published output.
4. Advance the group cursor only after successful admission/publication, or after
   a successful filter discards the completed range. Do not lose or duplicate rows
   across range reduction. Preserve output schema metadata and buffer owners when
   callers extract or slice arrays.
5. Add independent typed regressions with enough groups to cross1024 and a final
   short range: duplicate contributions, NULL keys/values, decimals, floats,
   dictionary input, variable UTF8 and empty/nonempty filtered output. Inject
   available-memory pressure at construction boundaries; verify a reduced range
   completes, impossible one-row construction refuses, and all owners release.
   Existing real-spill tests must continue to cover complete merged groups.
6. Compare against frozen0f30c946 under matched conditions. Measure unfiltered
   large output and selective HAVING separately, across low and high cardinality,
   raw/native/Lance/resident routes. Report construction failures, peak reservations,
   batch counts and full query times. Protect short queries and keep canonical
   provider/resource/concurrency gates. Do not infer a win from fewer batches alone.

This candidate does not remove per-row scalar conversion. Direct typed output and
late construction of columns excluded by HAVING are separate potential changes;
each needs independent semantics/admission evidence and an isolated measurement.

## Debugger attribution: unnecessary validity storage

After protected2204 finished, capped debugger91207 ran only
`agg_spill_matches_in_memory` on the feature-enabled debug test binary.
The test still failed (inferior exit101; debugger exit0). Exactly one514-byte
denial was observed, and execution continued to the original terminal failure.
The stack is `PoolState::grow` → `ReservedVec<u8>::with_capacity(2)` →
`ReservedBufferBuilder<u8>` → `admitted_output::validity` → `array` →
`GroupRows::build_output_range(start=192,end=207)` → live finalization.
The named query limit is262144 bytes, with262005 used at this denial.
This attributes this occurrence to output validity construction, not aggregation
input or Parquet decompression. It does not establish that every spill failure
has the same cause or that bitmap removal alone completes this query.

Source inspection finds `validity` always allocates a bitmap and returns a
`NullBuffer`, and every supported array branch receives `Some(nulls)`.
For all-valid values Arrow can instead use absent validity. The exact nullness
must be established from the actual retained values; schema nullability or an
NDV estimate is not a proof. NULL-typed arrays, sliced arrays and dictionary
input require their existing logical semantics. The OwnedArray/ArrayOwner and
payload-buffer owners must retain metadata admission even without a bitmap.

Prioritize a separate all-valid output representation candidate before installing
the quantum draft: make validity optional after checking actual values, exercise
all-valid and nullable primitive/boolean/UTF8/list output, verify extraction and
slice lifetime, and sweep admission pressure with typed expected values. Confirm
a red regression where existing unnecessary validity admission refuses and the
new representation fits, then rerun the original failure without changing its
budget. Keep quantum unchanged to attribute any improvement. If another refusal
appears, preserve it and trace that boundary rather than calling the query fixed.


A scratch-only implementation and three regression drafts are prepared in
`.scratch/parallel-aggregate-input/optional_output_validity.rs.draft` and
`optional_validity_tests.rs.draft`. They are not installed or compiled while
provider5434 runs. The red admission test exhausts remaining capacity after
creating the existing owners: all-valid and empty validity must succeed without
a bitmap, whereas real NULL input must refuse without losing its mask. Additional
tests check mixed nested nulls, borrowed UTF8, extracted/sliced payload owners and
zero-length extracted primitive buffers. These are proposed gates, not passes.

The existing admitted range copier already has an optional-validity contract:
`storage/admitted_selection/range.rs::nulls` omits a bitmap when the actual
input buffer reports zero NULLs. The proposed aggregate-output change aligns
construction with that representation contract; it does not introduce nullable
schema inference or alter SQL nullability. The two routines have different input
forms (Arrow arrays versus retained scalars), so do not force a shared helper
without preserving their respective ownership and validation boundaries.
