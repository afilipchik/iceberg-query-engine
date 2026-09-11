# Signed-zero comparison domain mismatch

A small independent DuckDB 1.4.4 probe on the validated SUBSTRING/VALUES release
reproduces an existing SQL comparison discrepancy. With input strings `-0.0`,
`0.0`, `NaN`, `Infinity`, `-Infinity`, comparing `CAST(s AS DOUBLE)` with zero gives
`[false,true,false,false,false]`; DuckDB gives `[true,true,false,false,false]`.
The all-constant cast form also gives false on this control; DuckDB gives true.
NaN equality gives true in both engines in the same probe.

Control binary SHA-256:
`ad506267227122efe6b67c426d113468e191fdb821f4d3ea8afe95e1fba20585`.
Reproducer: `.scratch/constant-cast-repair/float-edge.py`; preserved input, exact
SQL, outputs, independent results, cgroup and worker log:
`.scratch/constant-cast-float-control/`. This is correctness evidence, not a timing
benchmark. Its 10 GiB scope ran alongside the contained 64 GiB release build.

Arrow 58's `arrow-ord/src/cmp.rs` documents IEEE totalOrder, explicitly distinguishing
positive and negative zeros. `filter.rs::compare_operands` delegates to these
kernels; the optimizer's `eval_float64` uses ordinary Rust comparisons. Therefore
new constant-cast folding can expose an additional constant-versus-row discrepancy
for signed zero even though the new constant answer agrees with DuckDB. The
candidate must not be accepted solely on the 701 passing selected tests.

The release build is still running against its immutable 606-file source snapshot.
After freezing that executable, run the same probe on it to verify the inferred
new mismatch. Then repair the shared SQL floating-comparison contract across
scalar/array operands, NULLs, signed zeros and NaN, and audit optimized filter,
CASE/IN/BETWEEN and compiled paths. Numeric SQL comparisons must use one contract;
Arrow's ordering for physical sort must not silently define SQL equality. Preserve
all samples and keep canonical performance acceptance pending until the semantic
boundary is resolved. Join/group-key equivalence remains a separate required audit.

Independent fixture generation completed: 384 DuckDB comparisons (eight input
classes crossed with eight, across six operators). The draft
`tests/float_comparison_contract.rs` checks typed Float32/Float64, row-wise string
casts, constant casts, NULLs, multiple batches and partitions. It has not yet been
compiled or run, to avoid overlapping the release build with another heavy job.
It is outside the frozen candidate source manifest and must not be counted among
the 701 earlier selected passes.

Source audit also finds ordinary Rust comparisons in `compiled_expr.rs::Cmp` and
the vectorized `CmpF64` loops. `filter.rs::evaluate_binary_op` serves CASE/IN and
uses Arrow comparison kernels separately from scalar/broadcast comparisons. A
repair must cover both evaluator entry points and compiled execution; changing
only constant folding would leave the domain split intact.

[Verified diagnostic archive](benchmarks/2026-09-06-constant-cast-diagnostics/evidence.tar.gz)
contains the control reproducer, independent fixture generator/results and literal
range experiment. No current-candidate performance evidence is claimed.

## Reproduced candidate discrepancy and shared repair

The first optimized cast candidate completed in 8m23s; binary SHA-256
`c754cac49166fea4f1ff910a9557f90522a73ae2df4d66ce2e8214fb9aa68eb3`,
frozen source SHA-256
`f74b155996fa0238f8b3598e339c7fa27df1474c19ea584efc04d32656ab6ff9`.
It returns true for constant signed-zero equality but false for row-wise equality,
confirming the inferred discrepancy. `float-contract-red.log` independently
records one failed array-domain test and one passing constant-domain test.
This candidate is not accepted and no performance screen was launched for it.

Current source is newer: `planner::numeric::sql_float_compare` implements equal
signed zeros and equal NaNs ordered above non-NaN values, matching the independent
SQL oracle. `compare_float_arrays` preserves NULLs and scalar broadcasting without
copying/normalizing input arrays. The regular binary evaluator (including CASE/IN),
scalar/broadcast evaluator, constant folder and compiled F64 masks use the shared
contract. Physical sort and join/group-key policies are not certified by this fix.
The earlier temporary non-finite cast-folding guard is removed now that folding
and execution share comparison semantics. Unsupported scalar metadata and typed
NULLs still retain their casts.

Initial repaired array/constant oracle tests and date-cast tests pass. The expanded
full library and integration gate is running, including direct compiled execution,
sliced batches, Float32/Float64, broadcasts, CASE, IN and BETWEEN. The prior 701
passes belong to the earlier cast-only source. A new frozen release and matched
performance validation are required after this gate.

## Final default-source gate

The final source has 677 passing library tests and 40 passing selected integration
tests, plus the dedicated IPC regression: **718 selected executions pass**. The
historical flatten-dependent-join test remains ignored. The older scalar comparison
test had encoded Arrow totalOrder as its expected answer; it was changed to the
independent DuckDB SQL ordering, and the complete six-operator/both-direction
check passes. The earlier failing gate is preserved in `float-full-contracts.log`;
the successful rerun is `float-final-contracts.log`, with the dedicated IPC result
in `float-ipc-dedicated.log`. No failure or skip is counted as successful coverage.

Current production is now ready for a new optimized build and independent release
oracle validation. Performance, including the compiled float loop, remains
unmeasured on this source. Canonical multi-provider, GPU and resource acceptance
remain open. Join/group equivalence and physical sort are separate outstanding
contracts; this predicate fix does not certify them.
