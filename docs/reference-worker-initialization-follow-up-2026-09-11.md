# Reference-worker initialization follow-up — 2026-09-11

The active7ddbf9ce Iceberg screen records a DuckDB Q13 calibration0 refusal:
OutOfMemoryException, failed32MiB block allocation (bad allocation). Calibration1/2
and all dependent engine/reference measurements are NOTRUN. This is not the prior
1bba20b3 Q13 measured-request SIGSEGV, nor a successful comparison. Exact responses
remain in the current run's execution.jsonl and will be archived after timing.

Read-only worker source inspection establishes the initialization sequence:
RLIMIT_DATA is set, DuckDB/PyArrow are imported, a default DuckDB connection is
created, then threads, memory limit and temp directory are configured with SET.
Provider configuration follows. Query execution includes fetch_arrow_table and
excludes output serialization from the reported query interval. No active harness
or benchmark parameter was changed during this inspection.

One hypothesis is that default connection initialization creates resources before
the requested settings apply. Any retained allocator/thread state could affect
headroom under RLIMIT_DATA. This is NOT a reproduced cause of the refusal. A second
possibility is later provider/query allocation behavior; the investigation must
separate them rather than label every reference failure a setup bug.

After the frozen cycle is audited, committed and pushed, use owned diagnostic
workers to compare existing initialization with construction-time configuration,
if supported by pinned DuckDB1.4.4. Keep the same query/process caps, affinity,
provider snapshots and requested threads. Record actual settings and /proc process
Threads, VmData, RssAnon/RssFile and cgroup events at import, connection creation,
SET/configuration, provider-ready and each query boundary. Neither RSS nor VmData
alone proves the precise denied allocation; preserve both and any named errors.

Run a small setup-only control before replaying the affected query under a separate
bounded diagnostic deadline. Validate completed outputs against the existing typed
oracle. Keep default and proposed setup in independent fresh workers; do not reuse
a previously failed process. Preserve all attempts, crashes, refusals and not-run
requests. Do not raise caps, disable memory protection, or change query SQL to make
the control pass. If initialization changes, rebaseline the matched benchmark
conditions before interpreting any new engine/DuckDB performance ratio.

This follow-up is independent of engine native admission and the coordinated
Parquet reader repair. Existing incomplete provider screens remain incomplete.

## Provider-specific setup confirmed from source

The current Lance screen also fails reference calibration0 for Q9, requesting a
128MiB block. Its engine Q9 requests are NOTRUN; this cannot clear the older engine
join-index failure. Provider setup uses actual DuckDB extensions for Iceberg and
Lance and creates views over their sources. It does not eagerly convert those
providers into registered PyArrow tables. Native reference setup separately uses
CREATE TABLE AS from its reference Parquet, and decoded IPC registers preloaded
Arrow data; those are different measured modes.

Lance setup imports the Python Lance package to verify the pinned dataset version,
in addition to loading DuckDB's Lance extension. The existing compatibility setting
disables the DuckDB extension optimizer because the pinned extension's decimal AVG
pushdown truncates values. This setting is recorded for every query and is not
stock extension-optimizer performance. Preserve it in result qualifications.

The diagnostic should distinguish import/default connection/SET from extension
loading and each provider-registration step. In particular, measure before and
after Python Lance import, LOAD avro/iceberg/lance, and view creation. A provider
can allocate outside DuckDB's managed query-memory budget; no causal attribution
to such allocations has yet been established for these failures. Keep the actual
process cap and query cap unchanged during the proposed comparison.

## Fresh-worker control completed

After remote verification of `bf88b95`, setup20829 completed eight fresh workers:
Iceberg/Lance × default/construction-time configuration × two reversed blocks.
Each retained 16-thread configuration, CPU affinity0–15, 4GiB query/12GiB process
caps and the existing provider snapshots. A16GiB wrapper contained execution;
per-run temporary directories isolated spill files. No engine/harness changes.

Default connection creation observed79 threads versus63 with construction-time
configuration. At provider-ready both methods observed63 threads for Iceberg and97
for Lance. VmData was approximately1.1GiB and2.2GiB respectively, with similar
values across methods. This does not establish initialization as the refusal cause.

Query51319 then completed eight fresh-worker canonicalSF10 replays: IcebergQ13
and LanceQ9, both initialization methods, two reversed blocks. All eight pass
independent typed ordered comparison against the existing raw-Parquet DuckDB
oracles. Q13 query times ranged2.152–2.161s; Q9 ranged0.687–0.710s. These are
diagnostic timings, not a new performance baseline. A120s watchdog bounded each
worker, separate from canonical10× comparison rules. No request timed out.

The first validation invocation used tuple order keys and failed with invalid-order
errors; its record is preserved. The corrected invocation uses the canonical
dataset manifest's dictionary keys and passes8/8. No query was rerun to correct
validation. Scope peak/events were not captured for these short-lived scopes; do
not infer zero memory events from successful exit. Source traces include process
status at imports, connections, SETs and provider SQL boundaries; Python Lance
import is included between SQL boundaries, not separately instrumented.

Evidence: [hashed diagnostic archive](benchmarks/2026-09-11-reference-initialization-diagnostic/manifest.json).
The next controlled reproduction should preserve the persistent worker's preceding
query sequence and sample count, capture process/cgroup memory after each request,
and distinguish retained allocations from fresh-query working space. Both old
provider failures remain recorded; fresh-worker success does not clear them.
Construction-time configuration alone is not justified as a fix by this evidence.

## Persistent-sequence reproduction completed

Diagnostic49356 terminal0 replays the original reference request prefixes (including
plans, unlimited oracles, warmups, calibration and measured requests), unchanged
SQL/provider snapshots/16threads/4GiB query/12GiB process caps. It omits engine
interleaving and idle delays. Source worker is copied with only import relocation,
process snapshots at emitted responses, and an extra post-result-release event.
Each provider runs in a fresh persistent process;900s total watchdog, no timeout.

Iceberg completes121/121 requests in184.924s, including the previously failing
Q13cal0. VmData after the final release is9,838,212KiB. Its refusal is not reproduced.
Lance completes183/184 then reproduces the exact Q9cal0 OutOfMemoryException:
failed134,217,728-byte block allocation. At refusal VmData is12,574,152KiB, just
below the12GiB cap (12,582,912KiB); RSS is6,933,260KiB. The driver exits0 because
the worker reports the refusal normally; this is not a successful query gate.

Lance threads grow from97 at setup to168 after Q8oracle,335 after Q7oracle, and431
after Q14oracle, remaining431 at refusal. DuckDB is configured for16 execution
threads; that setting does not constrain every provider/runtime thread observed.
These snapshots establish sequence-dependent process-cap exhaustion but do not yet
identify which allocator or thread pool owns the retained address space. Do not
label the entire VmData–RSS difference a leak or thread stacks without attribution.

The combined16GiB scope peaks at7,466,983,424bytes, swap disabled, zero max/OOM
events. All five completed boundary outputs (Iceberg Q13oracle/warmup/cal0 and
Lance Q9oracle/warmup) pass independent canonical typed ordered comparison. Other
completed replay outputs are retained and hashed but not yet independently
validated;183/184 is completion accounting, not typed correctness certification.
The five verified Arrow outputs, traces, worker, request SQL, caps, provenance and
hashes are in the [sequence archive](benchmarks/2026-09-11-reference-sequence-diagnostic/archive-manifest.json).

Next: attribute threads by /proc task names and stack/runtime provenance, and
anonymous mappings by smaps at request boundaries. Test any runtime-pool control
only with explicit recorded provenance and same caps; a changed reference
configuration requires fresh matched baselines. Preserve Iceberg non-reproduction
and Lance refusal; do not replace persistent-worker benchmarks with fresh-query
workers solely to hide accumulated state. No production harness change yet.

## I/O quota diagnostic

Mapping13207 terminal0 repeats the Lance prefix with thread-name and smaps
snapshots. It reproduces183completed then Q9cal0 128MiB refusal. At refusal
there are349 `tokio-runtime-w`,62 `python`,16 `lance_backgroun` and1 jemalloc
background threads; VmData12,473,620KiB. Anonymous writable unnamed mappings
account for12,440,392KiB of address space and6,668,648KiB RSS. These mapping
classes are not allocator ownership attribution. Tokio names do not distinguish
async worker from blocking threads.

The installed pinned extension contains LANCE_PROCESS_IO_THREADS_LIMIT. Local
Lance0.23.2 scheduler.rs explains a process-wide semaphore limiting outstanding
I/O, unlimited by default. This local source is supporting semantics, not proof
that the extension was built from exactly that revision. The environment control
is tested directly against the installed extension SHA recorded in control.json.

Control1762 terminal0 sets only LANCE_PROCESS_IO_THREADS_LIMIT=16, retaining
SQL, provider, affinity0–15, DuckDBthreads16 and4/12GiB query/process limits.
It completes184/184, including the previously refusing Q9cal0. Final VmData is
10,151,104KiB with123Tokio-named threads. Combined thread count is202, so this
is not a16-thread process cap. It is an I/O concurrency control.

Default/control scope peaks7,416,651,776/5,601,427,456bytes, zeroOOM/max and
swap disabled. All five completed Q9 outputs across both runs independently
pass canonical typed ordered comparison. Other preceding completed outputs have
not been independently revalidated. Instrumented duration91.138/81.110s is not
a performance comparison. Original engine interleaving/idle delays remain omitted.

[Hashed mapping/control evidence](benchmarks/2026-09-11-reference-io-quota-diagnostic/archive-manifest.json).
This provides an actionable provider-concurrency hypothesis with one successful
control, not full acceptance. Next repeat in reverse order, then make any harness
control explicit and recorded (including effective engine-side applicability),
rebaseline the complete matched provider suite and preserve old failures. Do not
silently inherit a host environment setting or claim a fixed global thread cap.
Production source and harness remain unchanged.

## Reverse control and explicit harness option

Quota39397 repeats184/184 completed requests; finalVmData10,007,288KiB and102
Tokio-named threads. Reversed default2125 completes181/184: Q9oracle refuses
256MiB, then warmup/cal0 each refuses128MiB in the same worker. The scratch
replay preserves later attempts after refusal; those are not independent trials.
Both reversed scopes report zeroOOM/max, peaks5,425,360,896/7,123,283,968bytes.
Reverse boundary outputs still await independent validation; completion alone is
not correctness certification. Raw traces remain in repository scratch.

The harness now accepts opt-in --reference-lance-io-limit (positive integer,
Lance-only), stores it in setup/manifest, and applies it inside the DuckDB worker
before importing DuckDB/PyArrow/provider code. Ready provenance records the
reference-only scope. It does not configure the engine's separate Lance version,
and does not claim equal total thread count. Inherited quota values on Lance
runs are rejected in favor of the explicit option, preventing an unrecorded control.
The default remains unset. The worker validates direct invocations as well.

Focused quota/provider/query-gate tests pass27 with BENCHMARK_TEST_LANCE=1 and
no skips (the earlier run skipped that opt-in integration). Formatting and diff
whitespace checks pass. Frozen engine531source inputs still match7ddbf9ce.
Full matched canonicalSF10 Lance screen75607 is active, all22queries/3samples/
1session,16threads/affinity0–15,4/12GiB query/process limits,48GiB containment,
reference quota16, default disjoint engine. New harness provenance is frozen for
this screen; no benchmark/source edits during timing. Its outcome is not yet known.

## Full matched Lance screen completed

Screen75607 terminal1; all531frozen engine source inputs and binary/harness hashes
verify after execution. ReferenceQ9 completes under explicitquota16. EngineQ9
warmup fails after669.921216ms: join index allocation failed because the allocator
returned an error. Three engine measured requests are NOTRUN, preserving the
normal query gate. Other21queries complete with typed-correct engine outputs;
there is no full-suite score or leadership certification for this incomplete track.
The earlier engine join-index failure is now reproduced instead of hidden behind
a reference calibration refusal.

[Full screen archive](benchmarks/2026-09-11-reference-quota-sf10/archive-manifest.json).
Next audit artifacts and reverse controls, commit/push this intermediate cycle,
then diagnose engine native/Lance ownership and join-index allocation under the
existing caps. No cap increase, result-validation bypass or query-specific rewrite.

Post-run audit67671 terminal0 independently validates all84completed engine
outputs in the full screen and all3Q9outputs from the reversed quota control
(87checks total). Reversed default Q9 produced no completed outputs. Detailed
smaps traces are losslessly gzip-compressed in Git; original scratch traces remain.
The driver and archive preserve the named engine failure and63/66 valid measured
pairs, not a full-suite performance score. No engine source change this cycle.
