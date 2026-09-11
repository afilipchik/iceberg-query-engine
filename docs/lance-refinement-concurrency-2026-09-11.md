# Bounded Lance refinement concurrency — September 11, 2026

The single-scanner change fixed the reproduced persistent-worker failure but
regressed selective scan throughput. The next candidate parallelizes refinement
within the existing decode window, preserving the single scanner and its reduced
I/O fanout. It changes a shared provider mechanism, with no query-ID routing.

## Reproduced bottleneck

Paired diagnostic 96509 completed two reversed blocks across Q1, Q6, Q12, Q19 and
Q9, with a fresh worker per query and three measured samples after warmup. All
80 outputs pass independent typed validation. Every pair has identical optimized
and physical plans. The diagnostic enables scan timing and uses a 180-second
ceiling; it is not normal performance acceptance.

| Query | Candidate / parent, block 1 | Block 2 |
|---|---:|---:|
| Q1 | 1.0178× | 1.0196× |
| Q6 | 1.0438× | 1.0522× |
| Q12 | 4.7608× | 4.6354× |
| Q19 | 1.9256× | 1.9786× |
| Q9 | 0.9467× | 0.9606× |

Q12 planning, which includes provider materialization, grows from about 205 ms to
1,361 ms in block 1. Downstream execution stays near 100 ms. Scan traces directly
locate the added time in the filtered lineitem read. Q19 has the same attribution.
The scope peaks at 6,965,157,888 bytes under 16 GiB, with zero OOM/max events.
The [174-file diagnostic archive](benchmarks/2026-09-11-lance-scan-attribution/manifest.json)
preserves outputs, plans, traces, frozen binary/source hashes and the initial
concurrency regression source.

## Mechanism and patch

Lance10's `FilteredReadStream::get_stream` buffers decode futures concurrently.
Its `wrap_with_filter` previously used an inline future map to evaluate the
refinement predicate. The single stream consumer therefore serializes that CPU
work even while decoding happens in background tasks. The former scanner per
fragment accidentally supplied parallel filter consumers, along with excessive
I/O scheduling and retained process memory.

The patch uses an owned `SpawnedTask` for decode completion and refinement. The
existing buffered window limits in-flight batch futures and preserves output
order. Dropping a future aborts its task; already executing CPU work remains
cooperatively cancellable. Original filter errors are propagated. This does not
establish query-wide memory admission for Lance or collected results.

The change is vendored at `vendor/lance`, from the exact pinned 10.0.0 crate.
UPSTREAM.json records the crate checksum and all 265 original file hashes. Only
`src/io/exec/filtered_read.rs` differs from the original source; provenance and
license files are added. The external Cargo registry is untouched. Cargo.lock
only drops the registry source/checksum for that local package; dependency
versions and unrelated lockfile edges are preserved.

Benchmark provenance now includes every vendor file, including protobuf inputs,
so local dependency edits cannot hide behind an unchanged lockfile. Its unit test
checks changed and deleted dependency files. Clean local rebuilds require
`PROTOC="$PWD/.scratch/tools/protoc/bin/protoc"` on this sandbox.

## Validation checkpoint

- Red 91654: independently expected filtered rows match, but the two-batch
  rendezvous fails because refinement is serialized.
- Initial patched rebuild: no test result; protoc was absent from PATH. The
  existing repository-local executable resolves that toolchain prerequisite.
- Green 24871: the original concurrency regression passes.
- Strengthened contracts 11512: all three pass, covering overlap, a four-batch
  window with eight runtime threads available, and terminal error propagation.
  The filtered-row oracle preserves NULL semantics, duplicates and order.
- Same job: all 31 Lance SQL integration tests pass with fixtures present.
- Provenance unit test: one passes. Formatting and source whitespace checks pass.
- Broad validation 36475 is terminal: both modes pass 1,132 library tests
  (11 ignored) and 128 contracts. Native/IPC passes 63 in default mode and 62 in
  partial mode with its known numeric failure. Spill/numeric passes 28 with the
  same six failures in each mode. Executable counts, exits, failure names and
  retained tests match the parent plus the three new contracts. No new failures.
  Scope peak is 37,346,496,512 bytes under 48 GiB, with zero OOM/max events.
- The 32-file validation archive verifies 800 archived source inputs, including
  the vendor tree and added harness source. Protoc is version 25.3; its executable
  checksum is recorded. Release 3743 completed successfully in 11m28s and freezes `8c4936d8` from all
  800 inputs. Its canonical SF10 provider screen is complete; source stays frozen through postchecks.

## Canonical SF10 result

Pipeline 3743 is terminal: release 0, providers 1. The independent audit validates
all 343 completed engine outputs and 257/264 measured pairs:

| Provider | Typed outputs | Valid pairs | Complete | Geometric mean vs DuckDB | Suite time vs DuckDB |
|---|---:|---:|---|---:|---:|
| Raw Parquet | 88 | 66/66 | Yes | 2.4229× | 2.6304× |
| Native | 83 | 62/66 | No | — | — |
| Iceberg | 84 | 63/66 | No | — | — |
| Lance | 88 | 66/66 | Yes | 2.0219× | 3.1048× |

Raw has zero per-query wins and Lance one. Native Q1 warmup and Q6 measured3 time
out. DuckDB's Iceberg Q9 warmup refuses a 128 MiB allocation; engine Q9 is not run.
These failures remain open. Source, binary and harness hashes verify. The combined
release/screen scope peaks at 25,882,361,856 bytes under 48 GiB, with zero OOM/max
events and swap disabled.

## Reversed-block diagnostic

Postchecks 50101 complete the paired run and independent audit: all 80 outputs
are typed-correct, and optimized/physical plans match in every comparison.
Candidate/parent query-time ratios in the two reversed blocks are:

| Query | Block 1 | Block 2 |
|---|---:|---:|
| Q1 | 0.9846 | 0.9902 |
| Q6 | 0.9619 | 0.9829 |
| Q12 | 0.3576 | 0.2421 |
| Q19 | 0.5143 | 0.4876 |
| Q9 | 0.9797 | 0.9489 |

Q12 planning falls from 832/1,352 ms to 238/244 ms; Q19 from 793/790 ms to
383/385 ms. This agrees with the reproduced provider refinement bottleneck.
Two instrumented blocks are diagnostic, not multi-session performance acceptance.
Q1 still spends approximately six seconds executing after a quarter-second scan;
shared aggregate execution is the next CPU attribution target.

## Endurance and checkpoint

Postchecks 50101 are terminal 0, including all five stages. One default-allocator
worker completes both recorded SF10 sequences: 176/176 requests, all independently
typed-correct. Observed maximums at request boundaries are 48 Lance I/O threads
and 97 total threads. VmData grows 11,004 KiB between the two sequence ends
(10,690,972 to 10,701,976 KiB). This is bounded observed behavior over two sequences,
not a proof of indefinite stability or query-wide memory accounting.

The sequential 16 GiB postcheck scope peaks at 8,284,438,528 bytes, with swap
disabled and zero OOM/max events. That peak is cumulative across stages, not
per-query RSS. The frozen binary and all 800 source inputs verify after completion.

Verified archives: [validation](benchmarks/2026-09-11-lance-refinement-validation/manifest.json)
(32 files), [SF10](benchmarks/2026-09-11-lance-refinement-sf10/manifest.json)
(1,282), [paired diagnostic](benchmarks/2026-09-11-lance-refinement-attribution/manifest.json)
(173), and [endurance](benchmarks/2026-09-11-lance-refinement-endurance/manifest.json)
(197). The preceding [scan attribution](benchmarks/2026-09-11-lance-scan-attribution/manifest.json)
contains 174 files. Counts exclude each top-level manifest.

This cycle is an intermediary checkpoint. No DuckDB leadership or full resource
acceptance is certified. Next, attribute shared aggregate execution across raw
and Lance scans at four and sixteen threads, preserving default ownership and
query budgets. Native admission, the Iceberg reference failure, IPC/GPU residency
and multi-session/concurrency acceptance remain open.
