# Decoded IPC SF10 resource failure investigation — 2026-09-06

The full decoded-IPC SF10 run exposed an allocator abort, reproduced in a fresh worker. Diagnostics established a low-cardinality many-to-many intermediate caused by missing cached-table costing statistics, followed by unbounded join output collection. Shared statistics and pull-driven inner-join fixes now pass the selected regression gate; their release rebuild and full-scale validation are pending. The initial investigation below is preserved as historical evidence, followed by the confirmed diagnosis and implementation.

## Observed failure and matched control

Evidence roots under `.scratch/public-bench/`:

- Failure: `canonical-sf10-provider-gates-01/decoded_ipc/` (`manifest.json`, `setup.json`, `execution.jsonl`, `samples.jsonl`, `s1-engine.stderr`).
- Matched raw control: `dictionary-candidate-gates-01/tpch-sf10/`, same filenames.

Both manifests identify engine SHA256 `39425cd86a3ea0d52f357152147b515aec02a4c5f74bc8a5c56449a3a80d9cf5` and dataset-manifest SHA256 `ff097ac1f71b689618970e7c7c16c721ccab205c76286ea1f29866f34a8c7e99`. Both setups use 16 threads, a 42,949,672,960-byte query setting (40 GiB) and a 51,539,607,552-byte process cap (48 GiB). Q05 SQL SHA256 is `495def7debb02ab1def31da07fbc88b4796813b754832ce15d54a35dcb104577`.

IPC `s1-q05-warmup` reports `status=crash`, `phase=query`, `exit_code=-6`. Its stderr says:

```text
[mem-cap] process hard-capped at 48.0G anonymous memory (RLIMIT_DATA; the engine aborts if exceeded, the terminal survives; size with QE_MEM_CAP)
memory allocation of 1073741824 bytes failed
```

The ten subsequent Q05 engine samples report worker unavailable with exit -6. These are fallout from one warmup abort, not ten independent reproductions. Q08, Q20, Q06, Q07, Q21, Q10, Q15 and Q14 warmups preceded the failing Q05 in session 1. A fresh-worker diagnostic is needed to separate intrinsic Q05 behavior from retained state across earlier requests. Later requests could complete after worker replacement; they do not erase the crash.

The matching raw control completed all 30 Q05 samples with five output rows and typed ordered validation. Engine times ranged 546.678–742.353 ms, median 656.817 ms. A representative execution reports reserved peak zero and local observed peak 36,983,780 bytes. These are incomplete operator telemetry, **not total memory usage or proof of safe admission**.

The parent reports the full timing matrix was intentionally interrupted after the first-session IPC failure, with scope peak approximately 54.25 GB and no cgroup OOM event. That scope includes multiple processes and file cache; it cannot be equated to engine anonymous memory. An RLIMIT allocation abort does not require a cgroup OOM event.

## Confirmed differences in source contracts

1. **IPC registration discards column costing statistics.** `examples/benchmark_embedded.rs:116` collects every IPC batch before registering a `MemoryTable` through `ExecutionContext::register_table` (`context.rs:533`). `MemoryTable::statistics` (`operators/scan.rs:270`) returns exact row count and approximate Arrow bytes, but an empty `column_stats` map. `collect_table_statistics` (`context.rs:482`) forwards these to the optimizer. Raw Parquet instead caches footer integer/date ranges, null counts and NDV estimates, plus some dictionary-derived string NDV estimates (`storage/parquet.rs:113`, `:250`, `:645`). Equal data therefore does not imply equal optimizer information.

2. **Missing NDV changes join cardinality and orientation.** Join DP (`optimizer/rules/join_reorder.rs:754`) substitutes filtered base-row count when column NDV is unavailable, and costs equality edges using reciprocal maximum endpoint NDV. This can severely underpredict an intermediate joining large tables on low-cardinality foreign-key columns: an unknown nation key is treated numerically like a highly distinct key. It is a costing fallback, not a uniqueness proof. Missing date ranges also make the two order-date inequalities multiply generic 0.3 selectivities (0.09 combined) rather than use a footer-derived date band (`:1058`, `:1123`). These are concrete mechanisms for provider-dependent plans, but the failing IPC plan has not yet been captured.

3. **Inner-join physical planning trusts the logical orientation.** `physical/planner.rs:1651` deliberately keeps the optimizer's left build side. An underestimated large intermediate can therefore remain the build input. Blindly restoring physical side-swapping is not justified: its separate coarse estimator previously reversed better optimizer choices, as the source explains.

4. **IPC residency reduces headroom before query work begins.** The adapter retains all tables and columns as Arrow batches before accepting requests. The eight IPC files total 14,233,757,672 bytes; the raw input files total 2,353,285,015 bytes. These are file sizes, not measured retained Arrow/RSS. Projection clones Arrow references (`scan.rs:220`), so it does not copy every payload, but it also cannot free unreferenced columns retained by the registered source. Registration has no corresponding owned reservation. Query budget and process headroom are therefore different quantities.

5. **Scan execution capabilities differ.** Raw unfiltered scans can use `StreamingParquetScan`, including linked runtime join filters (`physical/planner.rs:1391–1458`, `:1785`). IPC uses `MemoryTableExec`; default `scan_with_filter` delegates to scan and predicates are evaluated by a wrapping filter. Provider-specific streaming/filter capability is another possible execution-volume difference even if logical plans match. Do not attribute every difference to statistics without comparing plans and join diagnostics.

The existing exact typed provider conversion validation establishes equal table values/multiplicity. It does not establish equal statistics, operator routing, residency or safe query execution.

## Available raw plan and missing IPC proof

The matching raw optimized Q05 plan joins region filtered to ASIA with nation, then supplier; separately filtered orders join lineitem. Those intermediates join on supplier key, and customer joins using both nation key and customer key. The physical tree uses spillable hash joins and aggregate, external sort, streaming Parquet scans and filtered memory scans. It does **not** use the older packed arithmetic nation/customer join expression found in earlier evidence directories.

No optimized or physical IPC Q05 plan appears in the failed response: the worker died before returning its normal result. Therefore this report does not claim a particular IPC Cartesian-sized intermediate, hash-table capacity, or leaking operator. The 1 GiB requested allocation alone cannot identify its caller, nor prove that it represents 1 GiB of logically necessary data.

## Bounded next steps

1. Use the frozen binary's existing `PLAN_DEBUG` pre-execution output (`context.rs:1408`) in fresh raw/IPC Q05 diagnostics, alongside existing `HJ_TIMING` and `QE_SPILL_DEBUG`, at unchanged caps. The parent owns these runs and fresh reference calibration. Preserve stderr, workload order, plans, build/probe rows, spill decision and failure phase. No rebuild or new benchmark-only EXPLAIN protocol is necessary for optimized-plan capture.
2. Compare the emitted join trees and build orientation. If different, reduce the mismatch to a small multi-table fixture with identical values but providers exposing complete versus absent column NDV/ranges. Include duplicate low-cardinality join keys and skew; assert exact results and bounded resource behavior. Do not assert that estimated NDV establishes a semantic key.
3. Fix admission independently of plan quality: an underestimated plan must spill or return a named resource refusal before an infallible capacity allocation. Cover input residency, overlapping live join state and scratch, and release on failure/cancellation. A better plan alone cannot certify the resource contract.
4. Design a shared provider statistics contract with explicit provenance and costing-only estimates. Computing or carrying bounded statistics for IPC may improve planning; benchmark setup work and persistent memory must be recorded. Avoid injecting Q05-specific metadata or inheriting stale raw statistics without dataset/schema identity.
5. Re-run focused correctness/resource tests, then the matched provider matrix with fresh evidence. Keep the present abort and incomplete matrix classified as failures. Increasing caps would obscure the issue and is not the proposed remedy.

## Follow-up: standalone diagnosis and shared statistics implementation

The initial read-only investigation above is preserved. Subsequent authorized work reproduced the failure in a fresh worker and changed the shared memory provider.

`.scratch/public-bench/ipc-q05-diagnostic-02/decoded_ipc/engine.log` now establishes the previously missing IPC plan. It joins filtered region/nation/supplier to customer on nation key, **before** applying the customer/order and order/lineitem joins. Join diagnostics show 20,037 supplier-side build rows on `s_nationkey=c_nationkey`; the next reported event is a failed 536,870,912-byte allocation. Given 1.5 million customers spread across 25 nations, this orientation implies roughly 1.2 billion supplier/customer pairs before later selective joins. This cardinality is an estimate from the data shape, not a completed materialization measurement. The standalone raw control selects the order/lineitem-first plan and validates. A fresh-worker failure rules out prior requests being necessary for this reproduction.

`MemoryTable` now lazily caches `TableStatistics` in an `Arc<OnceLock<_>>`, shared by immutable provider clones. The first statistics request merges Arrow vectorized min/max and logical null counts across batches; subsequent requests clone only the small statistics map. Supported range domains are signed integer widths, unsigned widths when representable in i64, and Date32. Estimated NDV is bounded by inclusive range width and non-null row count. No row-value hash set or per-query table rescan is introduced.

The helper declines ranges/NDV for dictionary encodings, Date64, timestamp, other unsupported domains, UInt64 values above i64::MAX, and spans exceeding signed i64 subtraction. It still records logical NULL counts, including dictionary values that are NULL behind valid dictionary keys. Duplicate case-insensitive column names are omitted because the shared map cannot unambiguously identify them. Date64 is deliberately excluded until consumers carry matching units. These statistics are for costing only; they do not establish uniqueness or expression lineage. Audit also found pre-existing eager-aggregation statistics consumers for null/packing proofs; the parent owns their separate review before broad rollout.

Focused validation:

```sh
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=32G SAFE_BUILD_JOBS=2 \
  scripts/claude-safe-build.sh cargo test --locked --test memory_table_statistics
```

Result: **4 passed, zero failed or ignored**, test execution 0.01 s, exit 0. Log: `.scratch/memory-table-statistics-tests.log`. Fixtures cover duplicates, NULLs, multiple batches, small integer widths, representable and unrepresentable unsigned extrema, signed full-span refusal, Date32 versus Date64, dictionary logical NULLs, empty/all-NULL arrays, ambiguous names, and a renamed three-table planning regression that rejects joining the low-NDV many-to-many edge before selective key joins. This gate does not certify full SF10 execution or join admission. The parent implements and validates bounded join candidate buffering independently; the original failure evidence remains unchanged.


## Combined implementation validation

The current candidate additionally bounds inner-join candidate chunks at 4,096
pairs, preserves full-key equality and typed integer probing, and stops pulling
input when the stream is dropped. Legacy materialized candidates use fallible
growth and an explicit per-probe-batch limit. This does not yet bound build-side
collection, incoming batch bytes, arbitrarily wide gathers or retained results.

Eager aggregation no longer treats estimated null counts or name-matched ranges
as semantic evidence, and scalar multiplication remains before floating SUM.
The latter has an independent regression where finite zero products sum to zero
but reassociation would produce NaN after an overflowing partial sum.

Combined selected validation: **765 Rust tests passed, one pre-existing ignored**.
Logs: `.scratch/ipc-systemic-full-gate-02.log` and
`.scratch/ipc-systemic-remaining-gate.log`. The initial combined run exposed an
outdated optimizer test expecting the now-forbidden floating rewrite; updating
that assertion and adding the execution regression produced the passing gate.
Formatting and whitespace checks pass. These are correctness results, not yet
a new accepted performance baseline.

Inner-join probing also yields cooperatively every 4,096 cursor steps,
independently of matches, so unmatched batches/collision chains remain
cancellable. A deterministic single-poll regression covers stream drop before
the next input batch. All seven focused join-stream tests pass, including this cancellation check.
The first release rebuild was intentionally interrupted before measurement;
the final candidate is rebuilding.


### Fixed release: standalone SF10 reproduction now passes

The final `lance,gpu` release build completed in 8m18s. At unchanged 40 GiB
query / 48 GiB process limits and 16 threads, fresh raw and IPC workers both
return the exact five-row Q5 answer: raw **545.533 ms**, IPC **645.253 ms**.
Both satisfy independently calibrated 10× DuckDB ceilings. These instrumented
single-query diagnostics are not suite performance estimates.

The new IPC plan applies orders/lineitem restrictions before the supplier and
customer relationships, removing the reproduced nation-key intermediate. The
whole diagnostic scope peaks at **17,900,060,672 bytes**, with zero OOM events;
this is scope-wide memory, not a query reservation measurement. The original
allocator failure remains preserved.

Evidence: `docs/benchmarks/2026-09-06-ipc-systemic-repair/`, including source
hashes, build/test logs, binary identity and the complete diagnostic archive.
The fresh full provider matrix is running in
`.scratch/public-bench/canonical-sf10-provider-gates-02`; no complete score is
claimed until it finishes. Full current-source raw/public and resource gates
remain outstanding.

The repaired release now passes the complete decoded-IPC SF10 gate: 660 pairs
across three sessions, suite0.677527914× DuckDB, geometric mean0.636333509×,
12/22 query wins and zero correctness/time-gate failures. This completes the
IPC workload gate, not broader resource or cross-provider certification. See
[the repair evidence](benchmarks/2026-09-06-ipc-systemic-repair/README.md).
