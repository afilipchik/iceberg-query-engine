# Fused aggregation spill implementation

The production simple grouped route now composes this pipeline. Both original
replay tests and the unchanged256KiB decimal gate pass; broader resource and
performance acceptance is not complete. See the current gate below.

| Component | Current contract and evidence |
|---|---|
| Input/cursor | Evaluated arrays are retained once; the applied-row cursor reports the first unapplied row on typed admission pressure. [Evidence](../../../../docs/aggregate-ingestion-cursor-2026-09-07.md) |
| Payload/type owners | Fallible admitted scalar copying with attached leases and owned bound metadata. [Evidence](../../../../docs/selected-scalar-ownership-2026-09-07.md) |
| Selected slots | Prepare/commit rollback and same-layout merge sharing; numeric values remain inline. [Evidence](../../../../docs/selected-state-slots-2026-09-07.md) |
| Fixed codec | Exact partial arithmetic/Boolean/variance frames and admitted scratch; not a complete row/file codec. [Evidence](../../../../docs/aggregate-state-codec-2026-09-07.md) |
| Aggregate rows | Contiguous fixed/selected arrays, reusable scratch, atomic row update/merge and borrowed selected output. [Evidence](../../../../docs/aggregate-state-rows-2026-09-07.md) |
| Group keys/index | Admitted full-key hash lookup, atomic key/state/index publication and indexed partial merge; flat canonical key buffers. [Evidence](../../../../docs/indexed-group-storage-2026-09-07.md) |
| Key reads | Full canonical payload validation without decoded allocation; admitted framed reads preserve IO errors and invalidate partial keys. File envelope remains open. [Evidence](../../../../docs/key-read-validation-2026-09-07.md) |
| Row writing | Complete key/partial-state payload from borrowed rows; prevalidation before output, exact selected bits, source retained after IO failure. File publication/read-back remain open. [Evidence](../../../../docs/state-row-write-2026-09-07.md) |
| Selected reads | Validate, admit retained payload/type metadata, then construct owned scalars; numeric/NULL values remain inline. Whole-row installation is now composed below. [Evidence](../../../../docs/scalar-state-read-2026-09-07.md) |
| Row restoration | Whole-payload validation, pending fixed/selected decode, rollback on error/drop, key/state/index publication on commit; duplicate runs merge through staging. [Evidence](../../../../docs/row-restoration-2026-09-07.md) |
| Framing | Query-layout/run UUIDs, ordered checksummed frames, admitted scratch and read rewind on budget denial. Complete-file ownership/count checks remain open. [Evidence](../../../../docs/spill-frames-2026-09-07.md) |
| File ownership | Admitted private-file owner/reader metadata, completed-run capability, expected count/length validation and last-owner cleanup. Run collections/parent directories remain separate. [Evidence](../../../../docs/spill-files-2026-09-07.md) |
| Flush publication | Admitted run collection, pre-reserved slot/file owner, completed-run insertion before source clear, failure rollback and owned consuming iteration. [Evidence](../../../../docs/run-collection-2026-09-07.md) |
| Run merge | Retained verified frame/staged row across admission denial, one partial commit per step, verified EOF and terminal non-memory errors. Scheduler remains open. [Evidence](../../../../docs/run-merge-2026-09-07.md) |
| Parent lifetime | Admitted private directory retained through file/readers; production writer/flush requires its owner, and final cleanup removes only an empty directory. [Evidence](../../../../docs/spill-directory-2026-09-07.md) |
| Repartition | Exact canonical-key split independent of hashes; one admitted frame/two writers; source retained until both children finish. Scheduler progress selection remains open. [Evidence](../../../../docs/repartition-2026-09-07.md) |
| Split progress | Full-run scan proves nonempty child counts; source-bound plan required for production repartition; equal-key runs decline splitting after validation. Multi-run global progress remains open. [Evidence](../../../../docs/split-progress-2026-09-07.md) |
| Global partition plan | One split/count proof across all runs, two completed children and one reader reservation reused only after verified EOF. Bounded task scheduling remains open. [Evidence](../../../../docs/partition-plan-2026-09-07.md) |
| Completed-run scheduler | Admitted tasks, merge/split under real pool pressure, smaller-first progress and complete-leaf callback. Actual256KiB COUNT storage gate passes; live integration remains open. [Evidence](../../../../docs/partition-scheduler-2026-09-07.md) |
| Run accumulation | Bounded prepare compacts completed partial runs; separately admitted owner sharing preserves originals until replacement finishes. [Evidence](../../../../docs/run-compaction-2026-09-07.md) |
| Arrow key input | Borrowed arrays, complete-row validation before admission, canonical NULL/float/list encoding without intermediate owned scalars. [Evidence](../../../../docs/arrow-key-input-2026-09-07.md) |
| Evaluated state input | Complete key/state/index transaction over borrowed arrays; admitted variable winners, exact resume cursor and late-denial rollback. Live controller remains open. [Evidence](../../../../docs/arrow-state-input-2026-09-07.md) |
| Ingestion controller | Prepared writer, pressure-driven partial flush, capacity release, bounded compaction, exact cursor resume and borrowed final partitions. Live workers/output remain open. [Evidence](../../../../docs/ingestion-controller-2026-09-07.md) |
| Arrow output | Admitted range construction with payload/type/batch owners; exact unsigned SUM conversion and empty global results. Live operator/collection admission remains open. [Evidence](../../../../docs/admitted-group-output-2026-09-07.md) |

| Live operator | One admitted state controller with concurrent input frontier; no budget-triggered replay. Post-filters/unsupported layouts choose ordinary execution before input. [Evidence](../../../../docs/live-spill-integration-2026-09-07.md) |

Current gate:887 library passes (10 explicit ignores) and25 integration passes,
zero failures. Both unchanged consuming-source replay regressions and the original
256KiB decimal query pass with actual spilling. Their reproduction below is now
historical. Parallel state policy, provider/evaluator allocation coverage,
post-filter output admission and benchmark/resource certification remain open.
No performance gain is claimed.

The [local DuckDB/ClickHouse comparison](../../../../docs/local-engine-source-comparison-2026-09-07.md)
traces actual index reset, tuple transfer, non-final state serialization,
ownership transfer and reservation-based merge concurrency. DuckDB's retained
aggregate allocator and ClickHouse's after-batch threshold are explicit limits
of those references, not guarantees to copy. Preserve the implementation and
acceptance contracts below.

## Original reproductions (now passing on the selected live route)

- `systemic_numeric_tests::exact_decimal_spill_merges_match_an_independent_integer_oracle`:
  unchanged256KiB query budget; parallel state growth can refuse before fallback.
- `fused_aggregate_budget_transition`: both shared/disjoint tests currently fail
  because each consuming input executes twice. The second execution is empty.
  The fixture demands actual spill,512 groups, duplicate rows, NULL groups and
  all-NULL values. COUNT, exact decimal SUM, weighted AVG, MIN/MAX, ANY_VALUE and
  ARBITRARY must all agree with independent values. Operator8KiB/query32MiB are
  explicit distinct budgets; this does not replace the256KiB query test.
- Preserve all25 passing lifecycle/aggregate/cleanup tests, especially pending
  sibling cancellation, original errors, single execution and reservation release.

Changing the tests to accept replay, increasing their budgets, returning partial
results, replaying volatile expressions, or retaining all historical input is not
the implementation. The existing raw-row spill path cannot consume finalized
partial outputs: COUNT and AVG require different merge algebra.

## Representation and ownership

Bind an explicit internal aggregate-state layout before the first input pull.
It records logical group types and function/input types once; runtime states do
not infer their identity from sampled values, NDV, output names or SQL strings.
Unsupported state capabilities must select the existing raw spill route before
consumption, with correct ordinary SQL behavior. They cannot discover a missing
codec after applying input and request a retry.

Use query-owned key/payload storage and checked state/index growth. Group equality
must include field boundaries and validity, canonical floating zero/NaN grouping,
exact decimal coefficient at its bound scale, and timestamp unit/timezone identity.
Dictionary codes are batch-local; encode logical values, not dictionary indices.
Do not replace exact key equality with a hash or statistical range proof.

State fields required by the current fused functions:

| Function | Internal state that must survive spill |
|---|---|
| COUNT | Count, with checked arithmetic and final type validation |
| Integer SUM | Sum and seen flag; never debug panic/release wrap |
| Floating SUM | Sum bits and seen flag; preserve the existing nonfinite contract |
| Decimal SUM | Fulli128 coefficient, signed scale, seen flag, sticky arithmetic-overflow state |
| AVG | Floating running sum and count; divide only at finalization |
| MIN/MAX | Optional typed value, with the existing comparison semantics |
| ANY_VALUE/ARBITRARY | Optional chosen non-NULL typed value; do not invent ordered-FIRST semantics |

Decimal intermediate coefficients can exceed38 digits while fittingi128 and later
cancel into the valid final range. Store internal bits/flags; do not validate a
flush as finalized Decimal128(38,s). The distributed SQL partial/final rewrite
provides useful algebra but is not an in-process state codec, particularly for
the existing floating AVG accumulator over decimal input.

Variable-width keys and selected scalar values need owned byte storage whose
leases survive spill/merge movement. A decoded row must not clone strings into
unreserved state. Account for old-plus-new growth, hash metadata, selection
indices, queued/evaluated arrays, codec scratch and output. Reuse existing
`ReservedVec`/Arrow owner mechanisms where their contract actually applies.

## Resumable ingestion

1. Evaluate group and aggregate expressions once per bounded input chunk, under
   the query pool. Retain the evaluated arrays until all rows are applied.
   Route disjoint workers using those same evaluated group arrays. The current
   drain/worker boundary previously evaluated grouping expressions twice; the
   evaluated-input repair now fixes that reproduced failure. Its regression
   checks group uniqueness and complete input counts without assuming a
   particular random sequence. Preserve this contract during spill integration.
2. Prepare a bounded prefix: group identities/indices and every required state,
   key and variable-value allocation. No row is committed until its required
   growth is admitted. A partially grown capacity may be retained, but it must
   not imply a partially applied logical row.
3. Apply the prefix with function/type dispatch outside the row loop where
   practical. Return the exact applied-row cursor, not a group-count estimate.
   Numeric overflow and expression errors remain distinct from spill pressure.
4. On pressure, stop pulling upstream. Flush current partial state using already
   admitted scratch, release state ownership, and continue from the retained
   arrays/cursor. Do not evaluate the original expression or input again.

Prepare encode/decode working space before filling the state budget. Worker count
must leave feasible queue/state/flush working space within the same query budget;
CPU count alone is not admission. If even the minimum working set cannot fit,
return a named refusal with cleanup. Such refusal cannot hide a feasible spill
transition in the fixed-budget regression.

The existing code's after-batch/every16-batches group check is not sufficient.
Simply catching an allocation error and retrying `process_batch` is also wrong:
the current method may already have applied a prefix, with no returned cursor.

## Spill and merge lifecycle

Partition by a stable hash of the full canonical key and retain exact key checks.
Prefer disjoint ownership to unnecessary copies of the same group across workers,
but measure routing cost; the earlier arena evidence does not certify an always-
disjoint implementation. Selection indices into shared, admitted evaluated arrays
can avoid copying every projected payload during routing.

Spill records need a version/layout identity and checked lengths. Preserve partial
states, not final SQL outputs. Merge equal keys with their aggregate-specific
state operation; spilled and in-memory remnants must combine before final SQL
precision/NULL checks and HAVING. Large partitions need bounded repartitioning
or another bounded merge; never collect all spill files back into memory.

Own spill files from creation through the last reader. On input, worker, codec or
IO failure, cancel/drain producers, finish blocking ownership cleanup and return
an error. A closed channel is not successful completion. Preserve the repaired
error lifecycle; group-pressure notification must not masquerade as an error or
trigger source replay.

## Implementation and verification sequence

1. Implement the state layout/codec and owned state storage as the basis for the
   real execution path. Test exact intermediate values, NULL/seen distinctions,
   AVG weights, type identity, corrupted/truncated records and reservation cleanup.
2. Add prepare/apply cursor semantics to ingestion. Test denial before the first
   row and inside a multi-batch input, with duplicates crossing flush boundaries.
   Include a volatile/counting expression oracle proving no re-evaluation.
3. Wire the spill transition and remove the group-budget `Ok(None)` replay branch.
   Run both one-shot transition tests, the fixed256KiB test at several Rayon
   counts, real spill/cleanup and all affected ordinary/specialized paths.
4. Freeze the validated source, build optimized binaries through containment and
   run typed optimized/resource controls. Keep the decimal-bound experiment
   separately attributable by using matched sources with and without that helper.
5. Measure shared component and full provider performance, preserve negative
   results and protected flags, then continue the full parent gates. No acceptance
   claim from a codec-only test, a narrower input type or a single query win.

Use existing task007/evidence documents for status. Avoid a second planning tree
or an untracked change to what completion means.
