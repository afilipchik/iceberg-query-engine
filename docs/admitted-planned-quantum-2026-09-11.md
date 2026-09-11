# Preserve planned row quantum in admitted scans — 2026-09-11

The optional admitted Parquet reader ignored the planner's configured row limit
and always requested8192rows. This was both an upper-bound contract issue for small
planned batches and an avoidable cap for larger batches. The runtime-route audit
reproduced a458to7323batch transition at unchanged16slots when a predicate became
admissible; the exact batch-count recovery still needs optimized measurement.

Focused red30866 fails two independent scanner tests at their intended assertions:
requested17is exceeded; requested32768is never reached despite sufficiently large
pages and64MiB admission. Both tests use two real Parquet row groups, duplicates,
NULLs and tails; they drive every declared prepared stream and verify independent
expected values and retained output ownership. No compiler/test-harness failure is
counted as the reproduction.

Current source passes scan.batch_size through admitted State into Reader::next and
AdmittedBatchReader::next. Ordinary pressure execution and optional preparation now
honor the same planned row upper bound. It remains a target: page boundaries,
variable-width byte limits and admitted decoder halving can yield smaller batches.
Execution details report the same row target. The previous variable-width pressure
test expected a one-row request to be ignored; it now expects39one-row batches for
its39rows. This updates the violated contract, not SQL results or memory assertions.

The separate65536-byte variable-width target, every allocation check, buffer owner,
pending cursor, preparation decline and terminal stream error rule remain in place.
A planned output row count is not a proof that decoder pages, predicates, handoff or
concurrent readers fit. In particular, this change does not claim to resolve the
[first-batch refusal](first-batch-refusal-ledger-2026-09-11.md) or memory-wide fairness.
No query-name conditions, dependencies or default ownership changes were introduced.

Green18452 terminal0:14scanner tests pass,1037filtered,0.28seconds. Both commands use
TMPDIR=repository scratch, SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 and
scripts/claude-safe-build.sh cargo test --locked --offline --lib; red selects
physical::operators::streaming_parquet_scan::admitted::quantum_tests, green selects
physical::operators::streaming_parquet_scan; both use --test-threads=1. Formatting
passes. [Verified source527 and red/green archive](benchmarks/2026-09-11-admitted-planned-quantum/manifest.json).

Both-mode Lance/GPU validation93080 is terminal1: each mode passes1110 library
checks (11ignored),125 contracts and28 spill/numeric checks. Native/IPC passes63
in disjoint and62 with1 failure in partial ownership. The same six spill failures
remain in both modes; executable/count/exit-checked comparison finds no additions
or removals. Peak23655981056bytes under48GiB,swap0 and zeroOOM/max events.
These are unresolved gates, not a successful full acceptance run.

Release95646 terminal0 in8m49s freezes beb0cdfd (527inputs,lance/gpu). Contained
checkpoint37633 is running serial triage and full provider/residency
screens on epic/realistic-benchmarks-duckdb-leadership. Next inspect actual runtime
route/batch counts against ece6a4d6 and pre-coercion13210a20, then run canonical SF10
providers and residency. The user requires each completed cycle to record SF10,
commit and push its intermediate source/results, then continue. Correctness passes
alone do not establish improvement; preceding regressions remain preserved.

Checkpoint37633 triage is terminal0:42typed-correct outputs; full SF10 providers/
residency are active on that frozen binary. Preliminary generic rawQ6 trace still
reports the8192row target and many small post-filter producer batches. Source
Reader::next applies the target before filtering, whereas the ordinary Parquet
reader installs an Arrow RowFilter. Target propagation repairs the configurable
contract but does not by itself establish matched post-filter batching or decoder
cost. Complete runtime audit and all timing stages must finish before final ratios
and evidence archival. No SF10 commit/push has occurred yet.

Completed triage route extraction verifies both blocks: generic rawQ6 sends
1139264selected rows through16producer streams in458batches on13210a20, versus
7323batches on both ece6a4d6 and beb0cdfd. Native and resident4 remain at916producer
batches across all three binaries. Default raw morsel execution emits no such queue
trace; the extractor's zero count there means absent instrumentation, not no work.
The candidate therefore does not remove the observed batch amplification. Predicate
placement and post-filter packing must be measured separately; Parquet page limits
remain a possibility, not an established explanation for this workload.

The locked Parquet dependency is58.4.0 (Cargo.lock checksum
`d298093b2dec60289dce0684c986d0f7679e9dd15771c2c65406e1aaf604a704`). Local
`parquet-58.4.0/src/arrow/arrow_reader/mod.rs` supplies the contrasting implementation:
`build` evaluates RowFilter predicates into a read plan before building the output
projection reader; `next_inner`'s selector cursor accumulates selected records up to
batch_size while skipping excluded records. Its mask cursor is a distinct path and
must not be described as equivalent selected-record accumulation. The ordinary
engine scanner installs RowFilter at streaming_parquet_scan.rs:693. The admitted
Reader instead decodes a batch, evaluates masks, gathers all read columns, projects,
and returns the first nonempty filtered batch. Both can report8192while exposing
different batches to the aggregate. This is a source-supported mechanism; proving
which Arrow selection policy ran requires runtime evidence, not inference alone.

After the SF10 checkpoint is committed and pushed, the next cycle should:

1. Reproduce batching with a small independent multi-page/multi-row-group fixture,
   several selectivities including0and1, NULLs, duplicates, and predicate columns
   that are not output columns. Hold the compiled program, decoder and partition
   route constant. Record input rows, selected rows and actual output batches.
2. Separately reproduce the first-batch multi-column memory starvation using a
   deterministic budget and an allocation ledger. Prepare required column/page
   working state before allowing early column output to consume remaining memory.
   Preserve all pending positions, owner charges and terminal stream errors.
3. Add bounded post-filter output construction across input quanta with explicit
   reservations and exact selected-row cursors. A nonempty admitted prefix may be
   handed off when further construction cannot fit; failure before any output must
   remain a named refusal. Never replay consumed predicates or source input. Cover
   flat numeric and variable-width layouts or retain their explicit correct route.
4. Measure batching separately from selective decoding. Reducing producer batches
   alone does not eliminate decoding/gathering of rejected rows or prove faster
   execution. Preserve the pre-coercion13210a20 baseline as well as currentbeb0cdfd;
   use varied SQL/data and full provider/resource gates before claiming retention.

This is the next-cycle implementation sequence, not an implemented memory fix or a
performance claim. The active SF10 measurement remains frozen.

Further source inspection for that next cycle finds two avoidable intermediates:
`Reader::next` reconstructs a non-null bitmap even when a static predicate is the
only filter, then `admitted_gather::filter` copies every read column before the
reader discards predicate-only columns. `admitted_gather::filter` already applies
SQL WHERE semantics (`valid && true`), so nullable static masks do not inherently
require normalization. A projected survivor-copy interface could gather only final
output positions after all predicates are evaluated. These are hypotheses for
removing redundant work within the post-filter construction step, not measured
gains. Tests must cover static-only/runtime-only/intersected masks, nullable masks,
non-output predicate columns, repeated/reordered output positions, empty output
schemas, all/none selection, exact types, ownership retention and named refusal.
Do not introduce an unreserved `RecordBatch::project` intermediate to implement it.
