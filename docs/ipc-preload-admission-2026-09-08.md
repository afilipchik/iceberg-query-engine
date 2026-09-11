# IPC benchmark preparation admission

The decoded-IPC SF10 setup previously collected every stream without charging
the execution pool, repeatedly aborting under a 12 GiB process cap. The current
debug adapter refuses the original setup before decoding table bodies:
7,762,147,216 bytes requested against a 4,294,967,296-byte execution pool.
It exits 1 with structured `status=refused`, `phase=setup`, and
`error_kind=preload_admission`; elapsed wall time is 0.03 seconds and measured
maximum RSS is 53,124 KiB. This is a safe refusal, **not completed SF10 coverage**.

`examples/benchmark_support/ipc_preload.rs` walks stream metadata and seeks over
bodies before registration. It checks frame extents/order with checked arithmetic,
admits metadata scratch, and reserves a conservative allowance for all inputs
on the shared context pool. The reservation outlives the context and registered
tables. Both decoded IPC and explicit host-Arrow GPU preload use this boundary.
Compressed record batches and dictionary deltas refuse explicitly because their
expansion is not modeled. Normal file-backed GPU setup is unaffected.

The allowance includes twice the serialized body size plus metadata/container
headroom. It is not exact RSS, a proof of every Arrow decoder allocation, or a
general-purpose bounded decoder. Validated immutable benchmark inputs are assumed;
file mutation between preflight and decoding remains outside this guarantee.
Production provider decoders, registration statistics and other allocation
boundaries still require separate admission work. No dependencies changed.

## Validation

All commands use repository `.scratch` as TMPDIR and `scripts/claude-safe-build.sh`.

- `cargo test --locked --features lance,gpu --example benchmark_embedded`:
  two tests pass, zero failures/ignores. They cover multiple inputs/batches,
  reservation lifetime and cleanup on denial, truncated metadata and actual LZ4
  compression refusal. The later structured setup-error emission compiles in
  `cargo build --locked --features lance,gpu --example benchmark_embedded`.
- Original setup: `QE_MEM_CAP=12G SAFE_BUILD_MEM=32G`, `/usr/bin/time -v
  timeout 30s target/debug/examples/benchmark_embedded
  .scratch/public-bench/outer-on-sf10-ipc-01/setup.json`, stdin EOF.
  The measured outcome above replaces the allocator abort for this setup only.
- A three-batch admitted fixture passes exact ordered COUNT/COUNT(non-NULL)/SUM
  and grouping checks against DuckDB 1.4.4, with PyArrow 25.0.1. It includes SQL
  NULL, empty strings, Unicode and embedded NULs. SUM's physical Int64 versus
  Decimal128(38,0) representation is normalized by exact integer/decimal equality.
  Query pool is 16,777,216 bytes; process/cgroup caps are 12/32 GiB.
- The first positive-fixture attempt failed before setup because Tokio could not
  spawn a worker under a 1 GiB process cap. The second rejected `16MiB` as an
  unsupported engine configuration spelling. Both are preserved; neither counts
  as successful low-memory coverage. The third uses an explicit byte count.

Evidence is in [the archive](benchmarks/2026-09-08-ipc-preload/manifest.json).
This debug validation makes no latency claim. The frozen borrowed-output release
binary and its completed benchmark do not contain this change.

## Supervisor follow-up

The supervisor now preserves the original non-ready setup response for dependent
requests, attaching `phase=setup`, `executed=false` and
`setup_failure_reused=true`. It no longer retries an identical failed setup before
every query in an ordinary session. A worker that initialized successfully and
then died during a query remains eligible for restart before the next query.
Fresh-session and explicit per-query resident preparation policies remain separate.

An actual worker check against the original SF10 setup records one startup and
three unexecuted requests carrying the original named refusal. These are not
three independently executed queries or independent resource refusals. The full
harness gate passes109 tests with zero skips, including the pinned Lance reader
and actual high-cardinality comparator spill. The initial default gate passes107
with those two opt-in tests skipped. Source provenance now includes all Rust
helpers under `examples/benchmark_support`, including the preload admission module.

## Remaining performance result

The normal-deadline borrowed-output raw SF10 run completed unsuccessfully:
51 of 66 measured pairs validate; Q5, Q9, Q10, Q12 and Q13 each time out once,
then have two unavailable-worker records after the watchdog kills that worker.
These are five deadline failures, not fifteen independent engine crashes.
The measured 9.0% Q10 improvement is real but insufficient to pass its deadline.
There is no valid full-suite score or DuckDB leadership result.

Release follow-up: the frozen scheduling binary now reproduces the named preload
refusal against the same4GiB context/12GiB process setup, under a2GiB containment
scope. Peak RSS is16,720KiB; no query executes. See the
[release preparation evidence](aggregate-batch-dispatch-2026-09-08.md#release-ipc-preparation-check).

## September11 source follow-up: projected dictionary preparation

`ipc_cache::open_row_group` configures FileDecoder with a projection, then iterates every dictionary block in the footer and calls read_dictionary. In the installed Arrow IPC58.4.0 source, FileDecoder::read_dictionary uses the full schema and dictionary map without consulting the projection. It builds dictionary values through a temporary schema and RecordBatchDecoder; projection is applied separately during record-batch reads. Thus projection does not currently eliminate unselected dictionary decoding at this boundary. This is a source fact, not measured attribution of native SF10 latency.

A bounded follow-up should first inspect the actual native workload's projected dictionary IDs and bytes after the active timing freeze, then reproduce unnecessary decoding with an independent projected-output fixture. Any selective path must preserve dictionary IDs shared across fields, required base/delta order, NULL code/value semantics and dependencies of selected nested types. A closed flat-type path may decline unsupported dependency shapes before decoding and retain the existing reader. All dictionary block framing/extents and message kinds still need validation; skipping a value decode must not skip bounds checks or hide a selected dictionary failure. Source and expression replay remains forbidden after output starts.

This is separate from full IPC admission: skipping unused dictionaries does not account for selected dictionary buffers, decompression, footer schemas, deletion masks or retained outputs. Do not advertise it as an admitted native reader or assume it removes the native parallelism barrier. [Source file paths and hashes](benchmarks/2026-09-11-ipc-dictionary-projection-source.json) preserve the exact inspection inputs. No source change or engine execution was performed for this follow-up.
