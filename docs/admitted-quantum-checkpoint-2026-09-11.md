# Query-engine intermediate checkpoint — 2026-09-11

The planned scan row limit is now honored by the admitted decoder, with independent
small/large-quantum regressions. This is a contract repair, not a demonstrated
throughput improvement. Frozen candidate `beb0cdfd` contains 527 verified source
inputs and the Lance/GPU features. The broader mixed-numeric candidate remains
provisional: generic raw Q6 is still 2.016 times the pre-coercion baseline in the
two-block diagnostic, and its producer batch count remains 7,323 versus 458.

Both ownership modes pass 1,110 library tests (11 ignored), 125 contract tests and
28 spill/numeric tests. Six existing spill failures remain in each mode. Native/IPC
passes 63 tests in disjoint ownership, and 62 with one numeric failure in partial
ownership. No failures were added or removed. These are unresolved acceptance gates.

Canonical SF10 uses a fresh matched DuckDB reference, three samples and one session.
Raw/native/Iceberg/Lance use 16 threads and 4/12 GiB query/process caps. Providers
produce 340 independently typed-correct outputs and 255/264 valid measured pairs:
raw 66, native 63, Iceberg 63 and Lance 63. Raw is 2.579 times DuckDB by geometric mean
and 2.808 by suite time. Native Q1 times out in warmup; Iceberg Q9's DuckDB calibration
crashes with SIGSEGV; Lance Q9 refuses its join-index allocation in warmup. Failed
or unrun requests are not successful performance comparisons.

Residency produces 348 typed-correct outputs and 278/278 valid measured pairs.
Canonical decoded IPC, CPU control and mixed GPU each complete at 32/48 GiB with
preload excluded. This does not clear the 16 GiB preload gate. Canonical mixed GPU
records zero successful device requests. The separate custom float smoke validates
all 40 measured required-device requests; it is not canonical SF10 GPU coverage.
The cumulative diagnostic/screen scope peaks at 23,061,450,752 bytes under 48 GiB,
with swap disabled and no OOM or memory-limit events.

All timing and audits are terminal. Independent audits pass; manifests verify
1,433 provider files, 1,458 residency files, 452 diagnostic files and all 527 source
inputs. Only four empty temporary payloads were omitted from the provider archive.
The source and validation archives are also included in this checkpoint.

This intermediate commit preserves the current source, tests, harness, reports and
current-cycle evidence. Historical benchmark payloads remain locally preserved;
about 28 GB of earlier artifacts are not added to Git. No DuckDB leadership, complete
resource acceptance, or concurrency acceptance is claimed.

After pushing the checkpoint, continue the systemic reader work: reproduce
cross-column first-batch memory starvation, coordinate reader working space, and
build bounded post-filter outputs with exact cursors. Separate downstream batching
from selective decoding and eliminate unnecessary predicate-only intermediates.
Keep baseline 13210a20 and current beb0cdfd in subsequent comparisons.

- [Implementation and next-cycle test sequence](admitted-planned-quantum-2026-09-11.md)
- [Three-binary diagnostic](admitted-quantum-triage-2026-09-11.md)
- [Provider results](admitted-quantum-providers-screen-2026-09-11.md)
- [Residency results and device qualifications](admitted-quantum-residency-screen-2026-09-11.md)
- [Terminal checkpoint records](benchmarks/2026-09-11-admitted-quantum-checkpoint/manifest.json)
