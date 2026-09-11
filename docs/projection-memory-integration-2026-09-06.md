# Primitive projection memory admission: production integration

Status: 744 selected tests pass (680 library, 64 integration), with two existing
library ignores. Release validation and performance remain pending.

The physical planner now supplies its query pool to ordinary and delimiter-state
ProjectExec paths. A synchronous scoped guard selects that pool while evaluating
projection expressions and restores nested callers on return or unwind; it never
spans an await or propagates implicitly to another worker. Direct evaluation and
other worker entry points still require explicit integration.

The shared numeric evaluator routes primitive arithmetic for all eight integer
widths and Float32/Float64 through pre-admitted typed output buffers. It uses the
same Arrow checked scalar operations to preserve overflow/division/remainder
semantics. Integer-to-Utf8 casts across all eight integer widths use admitted
value and offset builders, formatting directly into the bounded writer. NULL
validity is allocated through the same mechanism. Known output capacity is
admitted before fixed allocation; string payload growth reserves old plus new
storage until replacement completes. Returned buffers own their charges.

## Reproduced behavior change

With 16,384 resident Int64 input rows and a 64 KiB query budget, both
`SELECT i + 1 FROM t` and `SELECT CAST(i AS VARCHAR) FROM t` now return a named
query-memory refusal. The arithmetic request is refused before allocating its
131,072-byte payload. Existing result values under an adequate budget match
explicit expected integers and strings, including signed/unsigned extremes and
NULL. Float arithmetic preserves signed zero and NULL behavior.

A many-batch query refuses when accumulated computed outputs consume the same
query budget, and all reservations release after the error. An escaped primitive
array or extracted Buffer retains the query charge after QueryResult and its
ExecutionContext are dropped. Tests cover every integer width plus error and
unwind cleanup. Independent optimized validation is still required.

## Remaining contracts

This is an incremental production integration, not full resource certification.
Decimal kernels, non-integer casts, coercion temporaries, literal expansion,
filter/compiled/aggregate worker entry points, collection metadata, dictionary
expansion and provider/decoder allocations still have unadmitted paths. They
must be migrated rather than treated as safe fallbacks. No configuration opt-out
was introduced. The synchronous scope is not proof that all evaluators share it.
Prepared output bounds must not certify a computed projection as pool-independent.

The 512-byte buffer-owner envelope is not exact RSS or a bound on complete array,
schema or expression metadata. The existing result API remains unchanged; shared
resident input and bufferless metadata need separate ownership handling. No
benchmark result for frozen source 612 is attributed to this new implementation.

Validation command (48 GiB wrapper, one build job, repository TMPDIR):

```bash
scripts/claude-safe-build.sh cargo test --locked --lib \
  --test projection_memory_admission --test reserved_buffer_builder \
  --test result_buffer_ownership --test memory_reservation_contract \
  --test systemic_numeric_tests --test cast_contract_tests \
  --test constant_cast_folding_contract --test float_comparison_contract \
  --test group_key_equivalence_contract --test spill_tests
```

Log: `.scratch/result-memory-boundary/full-primitive-contracts.log`.
The new source must pass optimized oracle checks and paired canonical screens
before performance acceptance; no performance gain is claimed from unit tests.
