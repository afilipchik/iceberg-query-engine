# Shared compiled numeric comparisons — 2026-09-11

The nativeQ6 profile locates repeated comparison/coercion work:75of149stopped-thread snapshots contain compare_operands,44contain numeric cast_array/coerce_arrays, with decimal-to-float conversion among the leaves. These are attribution counts, not CPU percentages. Identical-binary nativeQ6 precision failed; preserve that result and the earlier observed header-candidate regression. The next change targets the shared comparison compiler, not a query identifier or a hand-written Q6 predicate.

Current `filter::compare_operands` coerces operands to a shared type before comparison. Decimal/Float64 pairs therefore materialize a converted array. BETWEEN lowers to two comparisons, which can repeat conversion. `compiled_expr::Compiler::boolean` currently declines mixed types; its extended decimal branch accepts only exact decimal/integer scale comparisons. The ordinary compiler also restricts decimal input binding. Changing only admitted compilation would miss the profiled ordinary native path.

First reproduce the compiler boundary with independent semantic tests. Cover ordinary and reserved predicate compilation; nullable/non-null/empty/multi-batch inputs; comparisons in both operand orders; all six comparison operators; BETWEEN/NOT BETWEEN and AND/OR/NOT under three-valued logic; coefficients around2^53,precision38extrema,positive/zero/negative scales,Float64rounding boundaries,signed zero,NaN and infinities. Check explicit independently computed results for key boundary cases, with differential comparison against the existing Arrow coercion path as additional coverage. Test dictionary physical encodings and unsupported domains decline correctly before consuming input. A test that merely matches the same new helper is insufficient.

Inspect the installed Arrow cast implementation before implementing conversion. Preserve its exact operation order and rounding, including scaling/division; do not replace mixed decimal/float coercion with exact decimal ordering. Reuse the engine's SQL float comparison rules. The shared numeric typing function must determine eligibility; temporal domains must not accidentally enter numeric coercion. Preserve decimal scale and float representation in any expression identity.

Add a closed typed conversion operation to the existing predicate program and bind its scale/conversion metadata once. Permit ordinary and reserved evaluators to bind the supported physical input consistently. Where the same bound column is converted twice, reuse its typed conversion register only with structural column/type identity. Preserve checked program/register bounds and output ownership. Unsupported types or encodings retain the existing correct path; no selected-source or expression replay is introduced. Do not add a query-name switch, change default ownership, relax memory limits or introduce a dependency.

Run focused red/green tests, existing numeric/float/decimal/Boolean/encoding contracts and the both-mode resource gates. Before release comparison, verify the actual native plan uses the compiled comparison boundary and still returns the independent typed result. Measure shared comparison workloads as well as canonical native/raw/resident controls. Keep first-request and steady-state timing separate. Existing nativeQ6 first-request precision is inadequate for small effects; characterize it instead of hiding failures or claiming neutrality from a noisy average.

[Profile and archive](native-q6-execution-profile-2026-09-11.md), [failed precision control](native-header-precision-2026-09-11.md), [header candidate measurement](incremental-header-measurement-2026-09-11.md). Implementation is in progress; performance acceptance remains open.


Implementation checkpoint: focused red run25456 failed all four new regression
families at the expected unsupported/mismatched compiler boundaries. The red source
and log are retained under `.scratch/parallel-aggregate-input/compiled-coercion-red-source/`
and `compiled-coercion-red.log`. The candidate adds a chunk-local CoerceF64 instruction,
column-slot conversion reuse, ordinary Decimal128 binding, and a shared-type guard
before exact decimal comparisons. Decimal conversion uses coefficient-as-f64 divided
by `10_f64.powi(scale)`, matching installed Arrow58.4.0. Register slabs and compilation
envelopes retain their existing reservation mechanisms.

The older exact-decimal test used Decimal128(38,-1) against Decimal128(38,2), whose
shared precision exceeds38. Its small negative-scale values now declare precision35,
so the expected exact ordering tests a representable common domain. The new rejection
regression separately preserves unsupported-domain coverage. Initial candidate test
build55331 failed on child-module method visibility; that visibility was corrected
before retry57397. No optimized measurement exists for this candidate.


Focused88389 passes all17compiler tests (1032filtered). Retry57397 first passed16
and failed only the older Int64/Float64 decline expectation; that case is now covered
by independent successful mixed-domain tests instead. The new test helper requires
ordinary compilation and permits runtime fallback only for nullable Boolean programs.
Formatting passes. Contract run28884 covers float comparison, Boolean NULL semantics,
decimal expression admission, coercion memory admission and scalar comparisons.


Contract28884 terminal0:22passes across five integration executables, no skips.
Full library76131 terminal0:1046passes/3ignored,115.12s. Commands use `TMPDIR="$PWD/.scratch"`,
`SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh`,
`cargo test --locked --offline`, no optional features, and `--test-threads=1`.
Focused command adds `--lib physical::compiled_expr`; contract command adds
`--test float_comparison_contract --test boolean_null_contract_tests
--test decimal_expression_admission --test coercion_memory_admission
--test scalar_comparison_contract_tests`; full library adds `--lib`.
Logs preserve compiler warnings and intermediate failures. Current source is a dirty
worktree based on88849c4f, distinct from frozen13210a20 performance evidence.


Source/log snapshots and intermediate failures are preserved with SHA256 verification
in [the validation archive](benchmarks/2026-09-11-compiled-numeric-coercion/manifest.json).
All jobs are terminal. Both-mode feature/resource gates, actual native workload
compiler-boundary proof and frozen performance measurements remain outstanding.
No dependencies, default ownership policy or memory limits changed.


Feature/resource continuation65485: source526 frozen; both disjoint and experimental
partial ownership run library,19contract executables,7native/IPC executables and5
spill/numeric executables with lance,gpu enabled. This is the same suite as the
incremental-header baseline, with complete executable/exit/count checks before
comparing failures. Disjoint library passes1108/11ignored. Complete source snapshot
is retained in the existing archive with a separate source-input manifest and tarball
hash. Driver: `.scratch/parallel-aggregate-input/run_compiled_coercion_validation.py`.
Actual native boundary proof will use a debugger stop in CompiledPredicate::eval_chunk,
inspect the instruction program and FilterStream caller, then complete typed oracle
validation. That probe is diagnostic, never timing evidence.


Feature/resource65485 terminal1, complete comparison passes: each mode has1108
library passes/11ignored,125contract passes,28spill/numeric passes and6legacy spill
failures. Native/IPC has63passes in disjoint;62passes/1existing failure in partial.
No added/removed failures, missing executable results or inconsistent exits/counts.
Peak29,986,852,864bytes within48GiB; swap0 and zero max/OOM events. These are still
failed resource gates, not acceptance. [Full logs and verification](benchmarks/2026-09-11-compiled-coercion-validation/manifest.json).

Sequence46649 stopped at its diagnostic assertion before release or paired runs.
Debug compilation succeeded. First native probe reached eval_chunk through FilterStream
with one float register, but raw GDB Vec display omitted the instruction elements;
that is insufficient proof. It stopped the owned query before completion. Retry38965
reads the vector elements explicitly; source remains unchanged.


Native boundary95635 terminal0: source-line breakpoint1171 proves actual execution
of decimal coefficient-to-Float64 division in CoerceF64 through the live filter wrapper.
Observed start0,len1024,column slot1,destination register0,divisor100; program has one
float register and10instructions. The completed canonical nativeQ6 result matches the
independent typed Arrow oracle. Its physical plan retains NativeStreamingScanExec.
Debug/GDB timings are not performance evidence. Peak2,067,390,464bytes,swap0,zero
max/OOM events. [51-file archive](benchmarks/2026-09-11-native-compiled-boundary/manifest.json)
verifies526source inputs and retains a hash of the separately frozen debug executable.

All three earlier probe failures are preserved. Attempt1 could not inspect Vec
contents; attempt2 hit GDB's i128 string-formatting limitation; attempt3 reached the
exact conversion source line but asserted an obsolete FilterStream symbol instead of
the observed filter::wrap_stream_with_membership_mode. Each stopped its owned query
before result completion. Attempt4 uses the source breakpoint, observed wrapper and
completed-result physical-plan check. No engine changes were needed for these retries.

Measurement sequence43258 is live: source-frozen optimized lance/gpu build, then two
reverse-order comparison blocks against13210a20 across raw/native/resident16/resident4,
including generic raw controls. Source remains526. The180second diagnostic watchdog
is not a fresh matched DuckDB10x acceptance gate, and two blocks do not certify a
confidence interval. Earlier failed nativeQ6 precision and resource failures remain
open. Driver `.scratch/parallel-aggregate-input/run_compiled_coercion_measurement_sequence.py`
persists step state and halts on build/driver failure.


Optimized release step43258 completed in8m51s, source526 verified. Frozen binary
`97e53169faead2f401b929842341077dcd0b84e122644a0893579c95d2624f4e`,
2026-09-11T14:13:52.755294Z. The same sequence is now running matched diagnostics.
Summarizers and archival checks are prepared in repository scratch; full provider
and residency drivers are prepared but not launched. A selected-field GDB ownership
ledger is prepared for the remaining page-body refusal, also not launched during
measurement. None of these preparations changes engine source or establishes results.


During43258 first-block review, generic rawQ6 and nativeQ6 are slower in the
candidate. These incomplete directional observations are not an acceptance result;
source remains frozen until all cases finish. If the reverse block corroborates the
regression, do not promote the conversion candidate based on eliminated allocations
alone. The shared exact-decimal instruction still dispatches operand/type/value and
scaled comparison inside its row loop, unlike the hoisted I64/F64 comparison shapes.
That is a source hypothesis for further attribution, not a measured explanation.
A same-binary QE_COMPILE control and/or owned-child profile can distinguish compiled
execution cost before committing to another implementation. Complete benchmark review
must precede full provider reruns and the next source edit.


The prepared follow-up uses a2x2 diagnostic: frozen13210a20 and97e53169, each with
QE_COMPILE=0/1. Four prespecified balanced orderings cover generic rawQ6 and native
Q6/Q9 (48fresh-process requests total), preserving matched resources, typed oracles,
plans and instrumentation. The switch disables the entire predicate compiler, so it
is not an isolated switch for the new instruction; the old-binary switch comparison
and Q9 control are needed to interpret that broader effect. No confidence-certified
claim follows merely from four blocks. The paired matrix must finish and be archived
before this additional diagnostic or the prepared genericQ6 stopped-thread profile
runs. Source inspection already shows null-free admitted programs skip their validity
program, so redundant validity processing is not the proposed fix for this workload.


Paired43258 and evidence36280 terminal0:120typed-correct outputs,2476join traces,
60unchanged logical/physical plan pairs,914verified archive files/526source inputs.
No max/OOM events. Q6 is slower in both ordering blocks in every mode: ratio2.224442
generic raw,1.203546raw,1.421561native,1.166881resident16,1.376582resident4.
[Complete measurements](compiled-coercion-measurement-2026-09-11.md) preserve every
query and smaller directional changes. This candidate is not accepted as a performance
improvement. Attribution28368 now runs the prepared balanced binary/switch diagnostic,
then the generic rawQ6 profile, sequentially in one48GiB scope. Source remains frozen;
full provider reruns remain withheld until this regression is resolved.
