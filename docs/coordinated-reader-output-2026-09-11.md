# Coordinated reader output candidate — 2026-09-11

This cycle starts from pushed checkpoint de7605f. A fixed160KiB fixture proves that
fresh one-row execution can complete all4096rows while a large-target reader fails
before its first output and cannot recover by lowering the retained target. The
[original red evidence](first-batch-refusal-ledger-2026-09-11.md) is preserved.

Current source separates AdmittedFlatColumn page preparation from prepared output
trials. Preparation reads and validates required pages, dictionaries and validity
before any newly missing output column is allocated. Prepared trials cannot fetch
another page. OutputCheckpoint clones only reversible decoder state and retained
buffer handles; it does not clone or seek the page source. Its backing vector is
query-pool admitted before preparation begins.

AdmittedBatchReader coordinates missing columns. A typed memory denial discards
only the current trial's arrays, restores their decoder checkpoints, and halves a
common row target. Existing pending prefixes remain untouched. A one-row denial
returns by name. Dictionary IDs produced during an unsuccessful trial are also
provisional and may be decoded again from their retained page; this is explicitly
not a claim that every ID is decoded only once. Source reads are never replayed.
Other errors remain terminal. Successful output and its existing handoff metadata
retain their original leases through typed arrays and slices. Default ownership,
query budgets, source fallback policy and dependencies are unchanged.

Initial focused56840 passes both memory-progress tests. The larger target now
returns every independently expected row in useful multi-row batches, with cleanup
to zero. The32KiB oversized-page control still refuses. Existing admitted-path
39033 passes135tests. Original COUNT(DISTINCT) spill test22126 remains terminal101:
request23043bytes with248034used/262144limit. This differs from the prior request,
but the gate is unresolved. No source-level attribution of this new request yet.

Added plain/dictionary SNAPPY tests cover nullable integers and UTF8, duplicate
values, multiple pages, unequal byte-bounded output prefixes and retained leases.
A fault after real decoding forces rollback of advanced cursors/IDs. Every prepared
trial asserts that its counted file source performs no reads. Separate non-memory
fault cases assert that subsequent calls are poisoned and perform no source reads.
Full default library77899 passes1069tests with3explicit skips. The subsequent
checkpoint-vector refinement reserves only missing columns and skips allocation
when all prefixes are already pending. Full Lance/GPU disjoint/partial validation38419 is terminal1 on final source531.
Each mode passes1131library/11ignored,125contracts and28spill/numeric checks;
native/IPC passes63disjoint,62with1failurepartial. Six spill failures remain in
each mode. Complete failure/executable/count/exit comparison passes with no added
or removed failures. [25-file validation archive](benchmarks/2026-09-11-coordinated-output-validation/manifest.json)
verifies531inputs. Combined build/test scope reached48GiB, with103max events but
zeroOOM and swap disabled. This is not query-only memory telemetry.
[Seven-file source/initial-evidence archive](benchmarks/2026-09-11-coordinated-output-source/manifest.json)
qualifies the initial logs as predating that final reservation refinement.
Frozen SF10 measurements remain required before the next intermediate commit/push.

## Remaining refusal attribution

Contained diagnostic18068 is terminal0 with one expected failing test (inferior101),
one allocation-denial stack,531verified source inputs and eight fixture hashes.
The remaining COUNT(DISTINCT) failure is in decode_page_body during required page
preparation, before an output trial. It requests23043bytes with248058used against
262144. The reader is already at max_rows1, remaining15000. The build decision
still has flat_rows0, flat_size0 and zero collected batches. This is not retained
join input or a large output-trial allocation. A complete page/dictionary/metadata
working-space ledger remains necessary before changing that boundary; do not
interpret the test's failure as proof that every possible admission policy fails.

Probe scope peak1722372096bytes under16GiB, swap0, zeroOOM/max events. No timing
overlap occurred. [Verified diagnostic archive](benchmarks/2026-09-11-coordinated-output-refusal/manifest.json).
The source candidate now proceeds to optimized release and frozen SF10 comparison;
no performance or broad resource closure is claimed.

Release5788 is terminal0 in8m49s, freezing7ddbf9ce with531verified source inputs.
Checkpoint98921 is running three-binary diagnostics; full SF10 provider/residency
screens follow in sequence.
Evidence72426 waits for every timing stage to finish before independent typed
comparison and archival. Source531 stays frozen; no duplicate jobs.

## Follow-up working-space audit during the frozen build

Read-only PyArrow footer inspection of the existing10MB regression fixture shows
SNAPPY and PLAIN/RLE/RLE_DICTIONARY for the selected orders columns. First row-group
totals (encoded/decoded, including column pages) are: o_orderkey86401/146350bytes,
o_custkey31605/40599bytes, o_orderpriority433/5776bytes, with15000rows. These totals
are not individual page allocation sizes and must not be treated as a complete
query-pool ownership ledger. The first-row failure still needs dictionary bodies,
current encoded/decoded pages, validity, checkpoint/handoff metadata and other
query owners separated at the actual denial.

Source inspection also finds an independent representation cost: the page reader
passes an admitted encoded Buffer by borrowed slice into decode_page_body, whose
UNCOMPRESSED branch allocates and copies a second same-sized buffer. An owned
transfer could preserve the original lease after exact extent/prefix checks, but
would require tests for CRC/header validation, pointer/lease retention, slices,
malformed input, V2 is_compressed=false and empty pages. The borrowed helper's
callers cannot automatically receive that optimization. No such edit or benchmark
has been made in this frozen cycle. Because the unresolved fixture uses SNAPPY,
this opportunity must not be presented as its fix.

Checkpoint98921 targeted stage is terminal0 with42typed-correct executions. Raw
Parquet full screen is terminal0 and completes22queries, geometric mean2.373308
and suite2.528335 versus DuckDB, zero wins. Native is active. These are harness
screen results; remaining providers/residency and final independent audit/archive
remain pending. Ratios across separate fresh-reference runs do not isolate a
candidate improvement; retain the paired diagnostics and all failed gates.

The root guide was consolidated during timing, with prior checkpoint chronology
preserved in the September11 history. Mandatory rules and all following operating
sections were verified byte-identical. No engine/harness/source change occurred.

Native screen is terminal1: Q1 warmup times out, Q6 passes warmup but its first
measured request times out. Iceberg Q13 calibration0 records a DuckDB32MiB block
allocation refusal; dependent requests are not run. This is distinct from earlier
reference SIGSEGV evidence. See the [initialization follow-up](reference-worker-initialization-follow-up-2026-09-11.md);
no source/harness change or causal claim was made during timing.

Decoded IPC screen is terminal0:22queries, geometric mean0.839134/suite1.393701
versus DuckDB,14wins, at the explicit32/48GiB capacity setting. Its engine suite
time is18108.629434ms versus18120.118760ms in the prior1bba20b3 screen, while the
reference suite time changes from18069.815889ms to12993.196972ms. The observed
ratio increase is therefore largely a reference-time change, not an increase in
engine suite time. These are separate one-session screens: neither unchanged
aggregate time nor this decomposition proves per-query neutrality or a causal
performance result. Preserve all per-query samples and the paired diagnostics.
CPU control is running; canonical mixed-GPU and final audits remain pending.

## Completed frozen cycle

Checkpoint98921 terminal1; evidence72426 terminal0. All completed outputs pass
independent typed checks:337providers and256residency/smoke. Archives verify531
source inputs and452triage/1425provider/1097residency files. Canonical mixed GPU
is not run after CPUcontrolQ1warmup timeout; all40custom measured device proofs
validate. The paired genericQ6/nativeQ9/resident4Q9 regressions remain explicit.
See [final checkpoint and qualifications](coordinated-output-checkpoint-2026-09-11.md).
This is a provisional memory-progress repair, not a certified CPU improvement.
