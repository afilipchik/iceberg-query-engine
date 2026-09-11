# Bounded ZSTD decoding, September 8

The admitted raw-Parquet pressure route now supports ZSTD, in addition to
UNCOMPRESSED and SNAPPY. Canonical SF10 input is unchanged. A contained footer
inspection of all eight tables found ZSTD with PLAIN/PLAIN_DICTIONARY encodings;
these codecs/encodings are supported by the current page and column adapters.
Full query completion and performance still require execution of a new candidate.

## Allocation and FFI contract

`storage/admitted_page_body.rs` allocates the exact declared output through
ReservedBufferBuilder, then admits a caller-owned ZSTD workspace sized by
`ZSTD_estimateDCtxSize`. A u64 backing vector retains its reservation through the
call. `ZSTD_initStaticDCtx` validates capacity and eight-byte alignment; failure
refuses cleanly, including on a target whose allocation alignment is insufficient.
`ZSTD_decompressDCtx` receives disjoint input and fixed output slices. It cannot
grow that output. The returned byte count must equal the page's declared value
extent; errors release destination and workspace. V2 level prefixes remain intact.
The workspace is dropped before the returned output buffer; output clones/slices
retain their own admission. No context free function is called on static storage.

The direct dependency is zstd-sys=2.0.13 with default features disabled and the
experimental bindings enabled. This was already pinned transitively as
2.0.13+zstd1.5.6: no package version changed. Cargo.lock adds the root dependency;
feature unification rebuilds Arrow/Parquet/Lance dependents. The static-context API
requires re-audit when this pinned implementation changes. A test verifies the
linked ZSTD version is1.5.6, including builds that might select a system library.

Primary C source review: `ZSTD_estimateDCtxSize` returns sizeof context;
`ZSTD_initStaticDCtx` rejects insufficient/misaligned storage before use.
Single-pass `ZSTD_decompressDCtx` calls decompression with no supplied dictionary;
literal scratch lives in the destination or context. Legacy frame decoding that
would allocate another decoder explicitly rejects a static context. Streaming
and dictionary-loading APIs are not used. Source paths and SHA256 hashes are
preserved in `zstd-source-audit.json` with the evidence. The unsafe block documents
buffer extent, lifetime, alignment and ownership requirements.

This bounds these decoder allocations, not exact process RSS or all query memory.
The compressed input must retain its separate admission. Footer/Arrow schema
construction, query metadata ownership and other provider paths remain open.
Fixed stack state and ordinary error objects are not represented as exact RSS by
the pool. Whole compressed/decompressed pages must fit; this is not streaming
within a compressed page.

## Validation

Focused94037: two tests passed, zero skips, after the dependency rebuild.
They cover zero/one byte and 128KiB block boundaries through512KiB output,
level prefixes, exact values, output-owner lifetime, workspace release,
truncation/corruption, too-small/too-large declared output and query-pool denial
after destination admission. A V2 level-only body is also checked.

Full library24107:970 passed, zero failures,10 existing ignored,31.02 seconds.
The live scan matrix now covers V1/V2 × dictionary on/off × SNAPPY/ZSTD. It checks
static LIKE/numeric masks, two runtime filters, nullable and repeated columns,
multiple groups/partitions, independent exact values and retained/released pool
ownership. The unsupported-codec no-replay regression now uses GZIP.

All build/test commands use the48GiB safe-build wrapper, repository TMPDIR,
Rayon4, one build job, locked/offline lance,gpu features. Integration1592 exited zero: partition17, runtime-filter2, shared-prescan3,
numeric12 and typed-memory-pressure2;36 passed with zero skips. Source hashes and
logs are preserved in `benchmarks/2026-09-08-admitted-zstd/`. Formatting and whitespace
checks pass. No new release
performance result is claimed by these gates.

## Next measurement

Freeze a release `benchmark_embedded` binary and its complete source hashes.
The prepared `.scratch/parallel-aggregate-input/isolate_admitted_q12.py` requires
`admitted-release.json` to match the binary hash, verifies canonical data, checks
exact output against the preserved typed oracle, and repeats query/process caps
1/12GiB and4/4GiB. It retains errors, watchdog outcomes, process time and cgroup
memory events. This is an instrumented budget-isolation diagnostic, not the full
matched SF10/provider/concurrency certification. Follow it with the required
resource matrix and matched workload/provider gates if successful.
