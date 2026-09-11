# Bounded aggregate output ranges — September 9, 2026

## Problem and implementation

Current native Q18 profiles attribute roughly1.25–1.30s to final Arrow output
construction and HAVING. The live aggregate constructs64rows at a time regardless
of available query memory. This repeats schema/type/metadata admission and scalar
construction work, and can refuse a64-row output even when smaller output fits.
See the [earlier audit](aggregate-output-quantum-audit-2026-09-09.md) and
[frozen reduction measurements](parallel-reduction-native-2026-09-09.md).

`morsel_agg/output_quantum.rs` now provides a construction-only helper. It starts
with at most1024rows of complete merged state. If actual checked construction
returns a typed memory-limit error, temporary owners drop before retrying the
same start with half as many rows, down to one. A failing one-row construction
returns its named error. Schema/other errors propagate without range reduction.
The remembered target decreases only after a refusal; a successful short tail
does not decrease it. The target resets at each completed group-owner boundary.

The live output loop reserves collection growth before construction. It applies
HAVING exactly once after successful construction, publishes the resulting batch
if any, and advances by the constructed input range length only after successful
publication or a successful filter discard. Filtering, collection growth and
publication errors are terminal. Input, expressions, partial-state updates and
already-published output are never replayed. Pure Arrow construction is the only
retry boundary. Larger ranges are a bounded scheduling policy, not proof that an
allocation fits; actual payload/type/batch admission remains mandatory.

Default aggregate ownership, group equality, spill policy and dependencies are
unchanged. This source follows frozenbd689bf6, but is not represented by that
binary's measurements. No performance improvement is claimed before freezing
and measuring the new source.

## Evidence

Contained red98327 extracts the old fixed64-row behavior into the helper. Both
new tests fail: a4MiB named-pool allocation refusal requesting1024bytes with only
22bytes available despite an individual row fitting, and33output batches for
2053groups instead of three bounded ranges. The latter is the batching condition;
semantic values and buffer lifetime are independently checked after construction.

The first green38229 passes the large-range semantic test but exposes an incorrect
pressure-test premise: an all-valid row fitting does not prove a NULL-bearing row
fits. The test now checks every single-row case before requiring whole-domain
completion. Corrected green93352 passes both tests. It verifies:

- Every group appears exactly once across1024/1024/5rows, with duplicate input
  contributions, NULL keys/values, exact Decimal128 coefficients/scales, float
  output bits, schema/field metadata and retained extracted-array ownership.
- Actual pool-pressure range reduction, cleanup after every attempted range,
  named one-row refusal, immediate non-memory error propagation, and invalid
  cursor/target rejection. No budget increase or allocator bypass is used.

A new live HAVING integration checks8193groups across three duplicate-contribution
batches against independently computed decimal results, including NULL keys and
NULL sums, with no duplicate publication. Default broad27479 and explicit partial99929 both exit0 with1,056library passes/
11ignored and13integration passes. Full spill26167 exits1: both modes retain
8passes/6same failed names as the preceding gate. Formatting and whitespace pass.
Release34350 exits0 in8m53s, freezing01bb077a/511verifiedinputs. Paired59512
exits0 with16correct outputs and verified ownership/reduction traces. Q18 observes
5.33%/5.28% lower time at4/16threads; Q1 varies+1.36%/-1.32%. These two blocks do
not establish a regression bound. All jobs are terminal. See
[paired output measurements](output-quantum-native-2026-09-09.md).
The [11-file correctness archive](benchmarks/2026-09-09-aggregate-output-quantum/manifest.json)
verifies511source inputs; the separate measurement archive verifies253files.
