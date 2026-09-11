# Direct updates for compact aggregate state

The first compact representation reduced reservations but regressed resident Q18
and raw Q1. This follow-up removes the per-input conversion to and from the owning
AccumulatorState enum. It remains provisional until a frozen comparison against
c20b0648, the pre-compaction baseline, completes.

## Contract

FixedCell updates its numeric words directly. Decimal rescaling and sticky overflow,
AVG accumulation and Welford variance share arithmetic helpers with the general
accumulator. Float addition order remains unchanged. Count, signed SUM, AVG and
variance retain checked integer bounds; unsigned SUM inputs retain exact i128
coefficients at scale zero. NULL and boolean handling remain unchanged. Cold merge,
restore, serialization and finalization still use the established general state
and wire codec. No query-specific routing, dependency or default ownership change.

StateRows still copies an entire row to reserved scratch before updating it and
publishes only after every slot succeeds. Selected payload admission and rollback
are untouched. A refused individual fixed update also leaves its cell unchanged.

## Validation in progress

Focused73998 is terminal0: two fixed-cell tests pass, including a new differential
regression comparing the established checked general update with direct updates
byte-for-byte after every input. It rotates NULL, signed/unsigned boundaries,
decimal scales, overflow, boolean values, signed zero, infinity and a NaN payload
through all fixed codecs and near-overflow initial states. Existing independent
spill-oracle and row-rollback tests remain required; differential agreement alone
is not independent SQL certification.

Default library20590 and partial library15025 are terminal0: each passes1083
tests with11ignored. Integration gates16252 are terminal1, retaining identical failure-name sets to
8b6a82e9 (checked as sorted sets; concurrent output ordering differs). Default
native/IPC/mutation/dictionary passes58 with2plan failures; partial passes53 with
7failures (2plan,4memory,1formatted float comparison). Both modes pass7focused
decimal/parallel-spill tests, while full spill remains8pass/6fail and systemic
numeric11pass/1fail. Denial byte boundaries match the preceding compact candidate;
these remain unresolved resource gates. No new failure is hidden by the aggregate
summary. Formatting and whitespace checks pass. Optimized measurements have not
started.
Logs and reproduction drivers use `.scratch/parallel-aggregate-input/direct-fixed-*`
and `build_direct_fixed.py`, `run_direct_fixed_gates.py`,
`run_direct_fixed_paired.py`. All engine/build/test jobs use the memory-capped wrapper,
48GiB, one build job, repository TMPDIR, locked/offline dependencies and lance,gpu
features. The paired driver retains raw/native and decoded resident 16/4-thread
Q1/Q9/Q17/Q18 controls, two reversed blocks, independent typed oracles and immutable
source/binary guards. It is diagnostic, not full DuckDB acceptance.

The correctness archive contains15verified files and519source inputs at
[manifest](benchmarks/2026-09-10-direct-fixed-updates/manifest.json). Release99456
completed in8m52s, frozen ea1e9019, with before/after source hashing.
[Paired measurement52198](direct-fixed-measurement-2026-09-10.md) is complete:
64correct outputs but a14.59%resident16threadQ18 regression remains. A separate
[typed input binding proposal](typed-state-input-follow-up-2026-09-10.md) records
local DuckDB dispatch evidence and a conditional follow-up; it is not included
in this candidate's source changes.
