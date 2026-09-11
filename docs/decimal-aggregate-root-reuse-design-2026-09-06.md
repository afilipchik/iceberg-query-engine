# Decimal aggregate input reuse: bounded source audit and next experiment

2026-09-06. Source-only investigation; no source changes, builds, tests or runtime jobs. Current release remains frozen. Proposed optimization is provider-neutral and has no query/table identifier branches.

## Evidence and profiling boundary

The frozen c4 diagnostic artifacts are `.scratch/public-bench/native-cpu-next-01/{native,decoded_ipc}-q01-16/result.json` and `engine.log`, also preserved in `docs/benchmarks/2026-09-06-spill-output-ownership/native-cpu-next-01.tar.gz`. Do not attribute these times to the newer prepared-pipeline candidate.

Native steady attempts record aggregate-expression worker intervals of 7,925.618 / 7,968.542 / 7,948.615 ms, versus process_batch totals 11,197.569 / 11,221.787 / 11,241.157 ms. Corresponding engine wall times are 857.018 / 849.860 / 845.321 ms. IPC steady expression intervals are 9,661.855 / 7,669.100 / 7,537.352 ms and engine wall 1,030.086 / 799.680 / 798.540 ms. These intervals sum elapsed durations across concurrent workers; they are neither wall time nor exact per-thread CPU time. They include scheduling and all work in the measured section. Their magnitude identifies a useful target; it does not quantify any particular redundant subtree's cost.

`src/physical/morsel_agg.rs:1922–1946` starts the process_batch timer, evaluates groups separately, then times every aggregate input's `evaluate_expr` plus `normalize_aggregate_array`. Accessor construction, key handling and accumulator updates are outside that expression interval but inside process_batch. The static counters at lines 54–60 are process-global; `spillable.rs:3652–3657` prints cumulative values, not individual-query deltas. The saved structured `aggregation_profile` fields above are per-request differences. Never sum successive cumulative log totals as independent queries.

`SpillableHashAggregateExec::execute_fused_streaming`, `spillable.rs:3348–3349`, collects each aggregate's input separately; workers use the common morsel state. `hash_agg.rs:675–687`, `1446–1455` and `2155–2164` likewise evaluate inputs independently in vectorized, partial-hash and generic paths. `operators/morsel_agg.rs` feeds the same common morsel machinery. This is not specific to raw, IPC or native storage.

## Source-confirmed repeated work

The saved optimized plan includes these unmodified aggregate inputs:

- `SUM(l_extendedprice * (1 - l_discount))`
- `SUM((l_extendedprice * (1 - l_discount)) * (1 + l_tax))`

It also contains separate SUM/AVG inputs for quantity and extendedprice. Column evaluation only clones an ArrayRef, so avoiding those cheap clones is not the primary target.

`filter.rs:232–291` recursively evaluates both operands of a non-comparison binary expression, in left-to-right order. Literal arrays are batch-length arrays. It has no shared aggregate-input context. `filter.rs:590–607` normalizes operands and routes arithmetic to `planner/numeric.rs:150–193`: checked operand/scale selection, strict casts where required, exact Arrow arithmetic, and Decimal128 precision validation over every non-NULL coefficient. Consequently the later charge expression recomputes subtraction and multiplication already completed by the earlier discounted-price aggregate input, including their associated casts/literal arrays/validation. This repetition is proved by the plan and code, not merely inferred from high timings.

`compiled_expr.rs:231–269` has a Float64 arithmetic compiler; `PredicateEvaluator` at 736–773 compiles Boolean predicates and falls back by domain/batch type. It does not compile Decimal128 aggregate input expressions. This audit does not propose silently moving decimals to Float64 or extending that compiler in the first increment.

## Smallest proposed increment: reuse earlier successful aggregate roots

Avoid a full common-subexpression DAG or caching every intermediate. Add an aggregate-input evaluator that can reuse **already materialized earlier aggregate roots** when that exact expression occurs as a subtree of a later input, within the same immutable RecordBatch. In the example, discounted-price is already retained in `agg_arrays`; reuse its ArrayRef while evaluating the charge expression. No additional decimal payload needs to stay live. The unchanged parent multiplication still receives the same typed array.

Suggested implementation boundary:

1. Add a narrow `evaluate_aggregate_inputs` helper near the existing expression evaluator, with an explicit normalization callback or two existing caller adapters. Iterate in original aggregate order. Preserve group evaluation order and normalization failure ordering.
2. Walk only the audited deterministic arithmetic subset: Column, exact integer/decimal Literal, Alias, and Add/Subtract/Multiply/Divide/Modulo with recursively eligible children. At runtime require resolved columns to be supported exact integer/Decimal128 domains; keep floats, temporal coercions, dictionaries and all other expressions on the old evaluator initially. For maximum simplicity, cache only prior results of physical Decimal128 type; both existing aggregate normalizers return those arrays unchanged (`morsel_agg.rs:3165–3211`, `hash_agg.rs:2253–2264`). Never substitute a normalized Int16/temporal/dictionary result for its pre-normalization expression.
3. Before evaluating a subtree, look up an eligible earlier successful aggregate root by structural equality, including literal type/precision/scale, qualification and full operator tree. `Expr` has PartialEq (`planner/logical_expr.rs:1027`), but DecimalValue equality normalizes scale. It is NOT representation-aware structural identity: 1.0 and 1.00 compare equal. Use explicit recursive identity with exact decimal coefficient/scale and literal type, not Expr equality. No SQL formatting key, commutative sorting, alias stripping, NDV/range inference or float-NaN equivalence. Nonmatching spellings merely miss reuse.
4. If no reusable root exists, retain left-to-right recursive evaluation and call the **same** normalized `evaluate_binary_op` arithmetic entry. Refactor visibility narrowly if necessary; do not duplicate decimal arithmetic/coercion logic. This helper must not bypass normalization/error checks.
5. CASE, COALESCE/function calls, subqueries, arbitrary UDFs and CAST/TRY_CAST are barriers in the initial increment: evaluate the entire containing unsupported expression through the existing evaluator. In particular, never pre-evaluate a CASE branch or migrate reuse across a selected-row batch. CAST could be added later only with mode-aware equivalence proof; it is not needed for these canonical arithmetic roots.
6. Scope lookup references to the current batch only. Do not put evaluated arrays in an operator-wide cache or reuse across partitions/batches. Reference existing result slots rather than cloning all input trees per batch. Added metadata has a checked/fallible size bounded by aggregate count; decline the optimization before work if allocation fails. A later compiled lookup-index plan is optional only if measured metadata lookup is material.
7. Replace the common morsel aggregate-input loop first, then the three hash aggregate loops with the same helper once tests establish parity. No accumulator update, SUM merge order, AVG division, spill layout or aggregate result schema changes.

This preserves exact arithmetic order and intermediate overflow/scale checks. A repeated subtree may skip a second execution only after its identical first execution succeeded on the same immutable rows. Any earlier failure still returns at the original first occurrence. No reassociation or movement of multiplication across SUM is allowed. NULL bitmaps and decimal metadata are the already computed exact arrays. Precision validation remains mandatory on newly computed arithmetic outputs.

This is a narrower form of common-subexpression elimination with an explicit ownership advantage: the reusable root was already retained by the original input vector. It does not introduce an unbudgeted cache of otherwise-dead intermediate arrays. Existing expression temporaries and aggregate payload ownership are not thereby fully admitted or certified.

## Independent regressions and acceptance

- Decimal128 quantity/price/discount/tax with independently computed checked-i128 coefficients for discounted-price and charge; SUM plus repeated-root AVG/COUNT cases. Assert exact coefficients, precision/scale, NULL bitmap and result type, not only row count or engine-versus-engine equality.
- Multiple groups, duplicate rows, NULL in each input independently, all-NULL groups, empty batches between nonempty batches, multiple partitions and changing values across batches. This catches accidental cross-batch caching and reordering.
- A structurally equal earlier root reused as both left and right later subtree; a similar expression with distinct literal scale/type, reversed operands or different qualified column must not collide. A test-only evaluation counter or returned diagnostic trace can prove the repeated root's arithmetic is executed once without a wall-time assertion. No production profiling overhead is needed.
- Decimal mixed positive/negative scales and negative coefficients; valid boundary coefficients; checked intermediate overflow and invalid scale must retain the same error at the first failing root even if a later expression would algebraically cancel it. Independently specify expected error category/domain. Invalid NULL values must not be spuriously inspected.
- Barrier tests: strict CAST errors, TRY_CAST NULLs, selected CASE with failing unselected branch, and volatile/unsupported function expressions must stay on existing evaluation; tests can assert eligibility declines without calling the function twice for validation. Include physical dictionary and Float64 fallback, including NaN, to prevent accidental broadened reuse.
- Raw/IPC/native provider-independent small integration results through ordinary and fused aggregate routes, and existing spill tests unchanged. Empty-batch evaluation behavior remains owned by each existing caller (the common morsel path currently returns before evaluating expressions).
- Only after correctness gates, repeat the accepted-scalar paired screen and a matched serial diagnostic using unchanged SQL. Hypothesis: expression worker time and decimal allocation/validation work decrease. No percentage improvement is claimed by this source audit. Check other queries and caps; reject a gain that changes errors, memory lifetime, output type or aggregate arithmetic order.

Remaining hypotheses, not implementation commitments: scalar arithmetic Datum support could separately remove full-length constants, and fused decimal evaluation could remove more intermediates. Both require broader type/coercion/error proofs. The first experiment above deliberately avoids that expansion.

## Integration caught a representation-identity defect

The first implementation used Expr equality and passed seven focused tests plus
three independent aggregate/spill fixtures. Source review then found normalized
DecimalValue equality underneath that comparison. A new regression reproduced
returning Decimal128(38,1) where Decimal128(38,2) was required for two numerically
equal decimal literals with different scales. The evaluator now uses exact
coefficient/scale-aware identity recursively over its admitted expression subset.
Generic scalar numeric equality remains unchanged because SQL equality has a
different contract. Eight focused tests are being rerun; no speedup is claimed.

### Shared identity follow-up

The later expression-substitution fix promotes exact representation identity into Expr::PartialEq itself, exhaustively covering AST/scalar variants, decimal coefficient/scale, floating bits and recursive list metadata. Aggregate root reuse now uses that shared identity; its eligibility and allocation restrictions are unchanged. Earlier warnings about derived Expr equality describe the pre-fix implementation. The binder/HAVING regressions are preserved in the expression-substitution evidence directory.
