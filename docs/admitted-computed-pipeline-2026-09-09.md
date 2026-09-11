# Admitted computed projection and inner-join input

The previous frozen Q9 diagnostic (`01bb077a`) found 16 declared raw/resident
partitions but only one aggregate input slot. Computed projection and inner-join
preparation declined the transitive admitted-buffer capability. The current
candidate repairs these shared operator boundaries. No query names, cardinality
assumptions, optimizer rewrites, dependencies or ownership defaults changed.

## Implementation and ownership

`physical/operators/project/admitted.rs` binds a closed typed expression program
before preparing its child. It supports flat column references, numeric literals,
checked numeric arithmetic, audited primitive casts and Date32 EXTRACT. Unsupported
expressions and binding-memory refusal decline before pulling or preparing a
source. Decimal division, decimal/float coercion, volatile functions and variable
literal construction remain outside this capability. Copied-output bounds remain
column-only: an admitted expression stream must never claim pool-independent pulls.

Program nodes, runtime slots, evaluation bookkeeping and new output buffers use
the shared query pool. Child values are consumed once, temporary values are dropped
after use, and output metadata survives batch/array extraction. Each declared
partition is wrapped in a terminal-on-error stream. Neither memory errors nor
arithmetic errors replay a source batch or expression.

`filter/temporal.rs` evaluates EXTRACT's date input once without expanding the
field-name string for every row. Flat Date32 extraction writes an admitted Int32
buffer, preserves input validity, and retains metadata with the output owner.
Calendar and range behavior matches the existing contract. Dictionary normalization
and other temporal types use the ordinary fallback and are not advertised as
admitted by the computed program.

`hash_join/outer_probe.rs` now also supplies the admitted inner-join capability
for the audited direct-key, flat-output domain. Inner joins reuse checked candidate
cursors, full key equality and bounded admitted gathers, without allocating outer
join match tickets or unmatched-build state. The ordinary inner route stays as
before. Preparation requires an eligible initialized build and transitively
admitted probe partitions. This does not certify every legacy build-cache allocation.

The capability audit also exposed mixed signed/unsigned division metadata that
could select Decimal128 while execution required float division. Shared operand
selection now chooses Float64 consistently. Independent tests cover -1/2, 7/2
and NULL under both ordinary and admitted evaluation.

## Reproductions and verification

All Rust commands use the repository memory-capped wrapper, 48GiB, one build job,
locked/offline dependencies and features `lance,gpu`. Logs are preserved under
`.scratch/parallel-aggregate-input` and the linked archive.

- Temporal red47190: one semantic pass and two expected admission failures;
  green43551: three passes. An earlier fixture compilation error is preserved.
- Computed projection red95693: four partitions received one input slot.
  The repaired barrier test proves overlapping pulls, one preparation, exact
  duplicate/empty-batch output and retained-buffer cleanup.
- Initial library73061 had one incorrect test expectation; corrected independent
  arithmetic expectations pass. This is recorded as a fixture error.
- Mixed division red90375 exposes the metadata mismatch.
- Inner join red41119 declines preparation; green56595 passes the independent
  duplicate/NULL oracle. Expanded tests cover both build orientations and empties.
- Combined38284: **1,072 library passes / 11 ignored and 43 integration passes**.
- Separate70820 is terminal1: partial library also **1,072 / 11 ignored**;
  default native **10/10**, partial native **6/10**; aggregate memory and decimal
  root reuse **10/10 in each mode**; spill **8 pass / 6 fail in each mode**;
  systemic numeric **11 pass / 1 fail in each mode**.

The four partial-native failure names and allocation amounts match the preceding
controlled source: scan admission requests 137–163KiB after roughly 180KiB has
already been reserved under a 256KiB budget. The six spill failure names remain,
but some denial boundaries differ. Default decimal spill still refuses 4,096
bytes at 260,501 used / 262,144; partial now refuses 1,008 at 261,395 used.
These are unresolved acceptance failures, not successful coverage or proof that
every current failure is causally unchanged. Existing evaluator-only controls
belong to the preceding decimal snapshot.

## Measurement and remaining work

The [correctness archive](benchmarks/2026-09-09-admitted-computed-pipeline/manifest.json)
verifies 26 files and all 518 source inputs. Formatting and whitespace checks pass.
Release82930 completed successfully in 8m53s and froze `3172f9e5`, verifying
all 518 source inputs. Diagnostic18871 completed: all three Q9 outputs are correct, but all frontiers
still use one slot. The planner wrapper excluded inner delegation; see
[negative diagnostic and wrapper attribution](admitted-pipeline-q09-measurement-2026-09-09.md). The next diagnostic repeats
canonical SF10 Q9 on raw Parquet, native and decoded resident CPU data with the
same default ownership, thread affinity and independent typed oracles as the
previous frozen diagnostic. Its extended watchdog is attribution-only, not the
10× DuckDB acceptance ceiling. A unit-test overlap result does not establish
SF10 performance. Paired protected tests and separate provider/resource gates
remain required before promotion.

The partial-worker startup headroom, whole-page decompression refusal, collected
results and legacy join reservations remain open. No DuckDB leadership claim.

Sources: [prior Q9 attribution](output-quantum-q09-attribution-2026-09-09.md),
[decimal prerequisite](decimal-expression-admission-2026-09-09.md),
[current architecture](architecture.md).

Local DuckDB source review at `1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`:
`src/execution/operator/projection/physical_projection.cpp` keeps an expression
executor per operator state and executes whole input chunks; the corresponding
header declares `ParallelOperator() = true`. This supports compositional parallel
projection as an architectural direction, without proving equivalent performance
or a shared memory-accounting implementation.

## Planner wrapper correction

The direct HashJoin capability was insufficient for the actual physical plan.
`SpillableHashJoinExec` now allows Inner alongside its existing outer join types
when consulting the cached build decision. Only InMemory decisions delegate;
Spill decisions still decline. The new internal regression forces all four
probe partitions to reach a barrier and checks an independent duplicate/NULL
oracle, one preparation, zero pre-pulls and retained-buffer cleanup.

An initial integration-test attempt could not access private descriptor fields
and failed compilation; it executed no tests. Internal red11478 reproduces the
missing delegation; green81365 passes. Broad70213 passes **1,073 library tests /
11 ignored and 47 integrations**. Both-mode resource gates17836 are terminal1: partial library1,073/11ignored;
native default10/10, partial6/10; aggregate-memory10/10 each; spill8/6 and
systemic numeric11/1 each. Failed names remain; the partial-native join denial
now occurs at176,021used instead of182,677. These are open resource failures.
The earlier 518-input archive and binary remain separate from this correction.

Corrected release66680 completed (e4608ccf,518inputs). Diagnostic57734 validates
all3outputs and confirms16resident slots; raw/native remain1. Matched91923
validates12outputs/720traces; resident ratio0.565225, raw0.994857/native1.001873.
Archives188/252files verify; all jobs terminal, zeroOOM/max. Full performance
acceptance remains open. See [measurements and implementation sequence](admitted-pipeline-q09-measurement-2026-09-09.md).
