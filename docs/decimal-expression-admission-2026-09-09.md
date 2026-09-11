# Decimal expression admission — 2026-09-09

The shared Decimal128 arithmetic evaluator previously allocated Arrow output
without query-pool admission or retained output ownership. A new independent
regression reproduces a64KiB decimal result succeeding under a4KiB pool. This is
a prerequisite memory-contract repair for computed projection parallelism, not
permission to enable parallel input for the still-unsupported projection/join
chain identified by [Q9 attribution](output-quantum-q09-attribution-2026-09-09.md).

`planner/reserved_decimal.rs` now writes add/subtract/multiply/modulo results
directly into reserved buffers after existing operand coercion and scale-domain
validation. It uses checked coefficient arithmetic, exact Arrow/Hive precision
and scale rules, and per-result precision validation. Division keeps the engine's
existing Float64 result contract. No dependency, ownership default, SQL rewrite,
query-specific selection or benchmark harness changed.

Values and optional validity buffers reserve before allocation. A fixed1024-byte
metadata allowance survives extraction of either buffer; payload reservations
survive array clones and slices. NULL rows skip arithmetic and initialize their
payload to zero. Late overflow, zero-divisor, length mismatch and admission
failure drop partial output. Arithmetic is never replayed after a published
prefix. Original input ownership remains the caller's responsibility; this does
not certify all expression/coercion functions or query-wide memory accounting.

Type inference still uses pinned Arrow kernels on empty arrays, independently
of any active query allocation scope. The existing checked decimal-domain guard
is shared by execution and type inference. The budgeted implementation computes
metadata in wider integers and is compared with the pinned metadata across a
matrix of positive/negative scales and precisions. Independent coefficient/type
fixtures cover actual nonempty arithmetic, so engine-versus-Arrow agreement is
not the only semantic oracle.

Validation so far (locked/offline, lance,gpu,48GiB capped wrapper, one build job,
repositoryTMPDIR):

- Red8805: one expected admission failure, one independent semantic pass.
- First green17703 failed compilation on a macro-expression semicolon; no tests
  executed. Corrected52917 passes both regressions.
- Expanded14954 passes5tests: coefficients/types, negative scales, sliced NULLs,
  retained values/validity, empty arrays, late errors and allocation-independent
  metadata inference.
- Broad73649:1061 library passes/11ignored;3coercion,2new decimal SQL and7projection
  integrations pass. The existing systemic numeric suite has11passes/1refusal.
- Evaluator-only control24942 restores the exact `numeric.rs` from frozen01bb077a;
  the same systemic spill test refuses4096bytes at260501/262144. The new module
  remains compiled but unreachable from that restored evaluator. Candidate was
  restored after the terminal control.
- Gate37366: full spill remains8passes/6same failed names in each ownership mode.
  Its integration invocations use an incorrect abbreviated target and execute
  no tests; corrected86252 uses `native_streaming_scan_tests`.
- Corrected default integration:5output-filter,10native and2typed-memory passes.
  Partial mode:5output-filter passes,6native passes/4refusals; the subsequent
  typed-memory target is not reached in that invocation.
- Evaluator-only partial control95935 reproduces all4native failed names with
  matching requested/used/limit values. Candidate was restored afterwards.
- Partial-library/typed-memory completion99675 exits0:1061library passes/11ignored
  and2typed-memory passes. Formatting and whitespace checks pass. All jobs are
  terminal, with the candidate evaluator restored.

The four partial native refusals are `aggregate_over_join_of_oversized_tables_completes_and_is_cell_exact`,
`aggregate_over_oversized_table_completes_and_is_cell_exact`,
`deletion_vector_is_cell_exact_on_the_streaming_path`, and
`filtered_aggregate_over_oversized_table_prunes_and_is_cell_exact`.
They request137328–163434bytes with179786–184906bytes already used in a262144-byte
pool. These are unresolved acceptance failures, not evidence that unbudgeted
allocation should be allowed. Optional partial-state/reducer startup headroom
needs separate attribution and costing; it is not repaired by this evaluator.

All engine source differences from frozen01bb077a are `planner/mod.rs`,
`planner/numeric.rs`, new `planner/reserved_decimal.rs` and its tests, plus the new
`tests/decimal_expression_admission.rs`. There is no new optimized binary or
performance certification yet. Earlier residency/benchmark evidence belongs to
frozen01bb077a. Next establish admitted temporal/coercion and computed-projection
contracts, preserve source lifecycle and then address the inner-join capability
boundary before measuring the combined pipeline change.

[Correctness and source archive](benchmarks/2026-09-09-decimal-expression-admission/manifest.json)
preserves the independent regressions, failed invocations, evaluator-only controls,
full gate logs and source snapshot. Reproduce using the archived gate drivers
inside `scripts/claude-safe-build.sh`; keep each gate's recorded ownership mode.
