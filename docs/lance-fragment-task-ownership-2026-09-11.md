# Lance fragment task ownership — 2026-09-11

Parent checkpoint864e645 is pushed and remotely verified. Source audit found that
scan_fragments_inner spawns all fragment scans, then awaits JoinHandles in order.
On an early error, dropping remaining handles detached live siblings. Their scan
state and queued batches could outlive the failed scan. This is independent of
the still-open Q9 join-index allocation failure; no causal link has been measured.

Reproduction71040 terminal101 extracts the existing collection boundary without
changing behavior. A pending sibling owns a drop-signal guard and acknowledges
startup before an earlier fragment fails. The collector returns its error while
the sibling has not released its guard. One deterministic test fails as expected.

Current source adds FragmentTasks ownership and collect_fragment_tasks. Ownership
is established synchronously, before the returned future is polled. Drop requests
abort on all retained handles. Normal error/panic paths abort and await remaining
siblings before returning the original error, discarding sibling results/errors.
Success extends output in original fragment order. Empty fragments and batches,
NULLs and duplicates retain their original representation and order.

Cancellation via future drop is cooperative: it requests abort but cannot await
cleanup in Drop. Blocking work already spawned inside Lance may have its own
lifetime; this change does not prove all nested work is cancellable. The fragment
fanout, collected-result API and its memory accounting remain unchanged. It does
not establish query-wide admission or repair process allocator headroom.

Validation, all through scripts/claude-safe-build.sh with TMPDIR in repository
scratch,48GiB cap/one build job, locked dependencies and lance,gpu features:

- Green46458:4focused tests pass (error drain, panic drain, unpolled cancellation,
  ordered outputs including NULL/duplicate/empty cases).
- Lance library:31pass/0fail/0ignored, including runtime-bridge contracts.
- Integration89065:31pass/0fail/0ignored using present data/tpch-1mb and
  data/tpch-1mb-lance fixtures. These compare engine paths, not an independent
  DuckDB oracle.
- cargo fmt --all -- --check and git diff --check pass.

Commands/counts and source SHA are retained in repository scratch under
parallel-aggregate-input/lance-fragment-cleanup-progress.json; full library and
integration logs are adjacent. No optimized performance result yet. Next complete
broader required validation, freeze/build candidate and run matched canonicalSF10
with independent typed results. Archive, commit and push this cycle before further
engine changes, preserving the unresolved Q9 failure and other resource gates.

Broader89208 terminal1: each ownership mode passes1135library/11ignored;
contracts pass, defaultnative/IPC pass, partialnative/IPC retains its prior numeric
failure. Both modes retain the same six spill failures. Comparator verifies full
executable/count/exit/failure inventories and no missing previous test names.
531source hashes verify. Combined build/test peak42,620,379,136bytes under48GiB,
swap0, zeroOOM/max. [Validation archive](benchmarks/2026-09-11-lance-fragment-validation/manifest.json).

Pipeline99427 is active: optimized Lance/GPU release then serial full canonical
SF10 raw/native/Iceberg/Lance, defaultdisjoint,16threads/affinity0–15,4/12GiB,
3samples/1session, explicit reference-only Lance I/Oquota16. No source/harness
edits until all timing ends. This pipeline does not rerun decodedIPC/GPU residency
or certify those modes for the new binary. Final evidence audit/commit/push follows.

While pipeline99427 remains active, read-only follow-up confirms the Q9 failure
site in hash_join.rs: pool.allocate(index_bytes) succeeds before an index vector's
try_reserve_exact reports allocator refusal. That source sequence does not by
itself identify missing reservations, fragmentation, or provider-retained memory.
The engine's ProfilingAlloc wrapper is wired in lib.rs, contrary to its historical
module header, but benchmark_embedded has no print_peak_snapshot call. Merely
setting QE_ALLOC_PROFILE does not provide the requested ownership report.

A scratch-only engine_lance_sequence_probe.py is prepared, not launched. After
all timing, it will replay the prior engine requests in one fresh owned process,
with the same setup/caps, capture VmData/RSS and thread names before/after each
request, and stop on first failure. It omits reference interleaving and idle delays,
uses a separate180s diagnostic ceiling, and cannot certify performance. Validate
completed outputs independently afterward. Do not run it alongside timed screens.

Release stage99427 terminal0 in8m49s, freezing binary9ba109ec
(9ba109ec7f66277fdc65f7263dfc61fb36a4548fc08291fa7b297967b89ad24e),531inputs.
The same live pipeline has advanced to rawParquetSF10, then native/Iceberg/Lance.
No finished provider score yet. Read-only join-allocation follow-up is documented
in [join index transition requirements](join-index-allocation-follow-up-2026-09-11.md).

RawParquet track completes22queries/66pairs: geometricmean2.433312, suite2.639173
versus matched DuckDB,0wins. This one-session screen is not throughput improvement
or broad acceptance. Remaining tracks are still running; audit_lance_fragment_sf10.py
is prepared to revalidate all completed engine outputs only after timing ends.

## Completed SF10 checkpoint

Pipeline99427 terminal1, postrun audit86235 terminal0. All340completed engine
outputs independently typed-correct,255/264valid measured pairs: raw88outputs/
66pairs; native84/63; Iceberg84/63; Lance84/63. Rawgeomean2.433312/suite2.639173,
0wins. No suite score for incomplete tracks. NativeQ1warmup times out; Iceberg
referenceQ9warmup refuses256KiB; LanceengineQ9warmup refusesjoin-indexallocation
after668.327861ms despite referencequota16 completing. Cleanup does not solveQ9.

[1271-file verified measurement archive](benchmarks/2026-09-11-lance-fragment-sf10/manifest.json)
contains allsamples,plans,oracles,failures,postrunchecks and frozen531input/binary/
harness provenance. Combined release/screen scopepeak27,743,027,200bytes under
48GiB,zeroOOM/max,swap0. This differs from the earlier42,620,379,136byte validation
scope. No fresh decodedIPC/GPU residency or concurrency certification thiscycle.
Next commit/push checkpoint, then run the prepared engine sequence diagnostic.
