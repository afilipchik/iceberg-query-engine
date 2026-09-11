# Borrowed UTF8 group-key output

2026-09-08 release measurement: the implementation lowers canonical SF10 Q10
median latency from3219.122ms to2929.182ms, **9.0% lower**, against the frozen
outer-ON control. Four alternating-order blocks provide12 steady samples and4
warmups per binary. All32 outputs pass the independent typed LIMIT-aware oracle.
Per-block candidate/control median ratios are0.9030,0.9157,0.9067,0.9044.
Both binaries use unchanged SQL,16 threads, CPUs0–15,4GiB query memory and12GiB
process caps inside32GiB containment. This diagnostic comparison records queries
past the normal deadline; it does not turn Q10 into an acceptance pass.

A separate two-request-per-binary phase check gives output construction of
469.8–474.4ms in the control versus215.1–221.8ms in the candidate. Ingestion
remains in the555–574ms range. All four instrumented outputs pass the oracle.
These are wall intervals, not exclusive CPU samples or acceptance latency.
The first phase script mistakenly expected four responses after requesting two;
that script assertion failure and its outputs are preserved. The corrected run
uses a fresh output directory. The main32-output comparison was unaffected.

The optimized build succeeded in8m42s and its660 source hashes were verified
before freezing `.scratch/live-schema-boundary/borrowed_output_benchmark_embedded`.
The full normal-deadline canonical SF10 raw rerun has completed unsuccessfully in
`.scratch/public-bench/borrowed-output-sf10-raw-01/`:51/66 valid measured pairs;
Q5/Q9/Q10/Q12/Q13 time out, each followed by two unavailable-worker records.
Broader provider/protected acceptance remains open. Metadata is preserved with
the [subsequent preparation repair](ipc-preload-admission-2026-09-08.md).

The current raw SF10 diagnostics identify477–480ms of output construction in
Q10. Source inspection shows that each string group key was decoded into an
individually owned ReservedScalar before copying into final Arrow storage.
That ownership cycle is unnecessary because the completed group storage remains
borrowed throughout output construction.

`KeyLayout::utf8_field` now returns a borrowed string from a validated canonical
field. `KeyRef::bytes` exposes the underlying storage lifetime rather than the
temporary view's lifetime. Both remain constrained by the immutable key owner;
no unsafe borrowing or mutable alias is introduced.

`GroupRows::build_output_range` collects these borrowed strings in reserved
scratch and writes them directly into reserved Arrow payload, offsets and
validity buffers. Array/type/batch ownership remains retained through the existing
OwnedArray and buffer-owner wrappers. NULL and empty strings remain distinct.
Nested keys and selected aggregate values retain their existing output paths.
There is no output chunk-size, spill, memory-cap or dependency change.

## Validation

-111 aggregate component tests pass, including the new test sweeping admitted
  headroom in256-byte increments through refusal and success.
-Full library:889 passes, zero failures,10 explicit ignores.
-Selected integrations:31 passes, zero failures/ignores: aggregate encoding,
  consuming-source spill transition, input lifecycle, dictionary input and
  exact systemic numeric cases.
-The new ownership check includes empty/nonempty strings, Unicode, embedded NUL,
  SQL NULL, input destruction, Arrow data/slice retention and final pool cleanup.
-Formatting and whitespace checks pass.

The component run overlaps the full-library test set; these are not1000 distinct
tests. [Logs, changed source and660 input hashes](benchmarks/2026-09-07-borrowed-key-output/manifest.json)
record the implementation. The earlier SF10 provider matrix uses the frozen
outer-ON binary, which does not contain this change. Its failures remain preserved;
this measured single-workload improvement does not establish suite leadership.
