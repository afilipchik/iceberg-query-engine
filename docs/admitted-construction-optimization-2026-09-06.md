# Optimizing admitted primitive construction

Status: 747 selected tests pass with two existing library ignores. Optimized
semantic and selected aggregate cap checks pass. Component measurements recover
float performance, but string output remains slower than source 612. Performance
acceptance and broad provider validation remain open.

`ReservedBufferBuilder::extend_reserved` fills an already admitted capacity
directly from at most a declared number of iterator items. It never grows the
allocation. Misleading iterator size hints cannot bypass the Take limit; short
iterators restore the original length and return an error. No application unsafe
code is introduced. Existing growth still reserves old plus replacement storage.

Non-null Float32/Float64 arithmetic now selects the operation once and writes the
mapped values directly into that admitted buffer. It removes the stack chunk and
second payload copy. Floating wrapping and checked Arrow operations have the same
IEEE behavior; integer arithmetic remains checked, and nullable arithmetic retains
its checked validity path. Signed zero, NaN, infinities and floating division by
zero are exercised through SQL for both widths.

Integer-to-Utf8 conversion now uses Arrow's ArrayFormatter, the same lexical
formatter as its ordinary cast kernel, writing into the admitted string builder.
This replaces Rust Display without changing the supported type domain. Query-memory
errors from the bounded writer retain their original classification and pool name.
The eight integer widths, signed/unsigned extrema and NULLs retain exact tests.

`QE_TRACE_RESERVED_EXPRESSIONS=1` enables opt-in projection/kernel events, read
once per fresh process. With it disabled, no diagnostic JSON/formatting allocation
occurs. This is for a separate contained Q12 attribution run, never latency samples.
Do not infer that Q12's existing 13.2%/10.0% regression came from numeric kernels:
its saved projections contain only column/alias expressions. Trace coverage and
matched timings must establish the next action.

Production changes: `execution/reserved_buffer.rs`, `planner/reserved_numeric.rs`,
`execution/expression_memory.rs`, and projection diagnostics. No dependency or
feature changes. The open literal/coercion/decimal/other-evaluator/metadata/decoder
admission gaps remain open; this is not full query-memory certification.

Validation: the full library plus projection admission, reserved builder, buffer
ownership, hierarchy, numeric, cast, float comparison, group-key and spill contracts
passed under the 48 GiB/jobs1 wrapper. Log:
`.scratch/result-memory-boundary/direct-fill-full-contracts.log`.


## Optimized evidence

Frozen source manifest SHA256:
`081dee7ce6fc1fb0832280eabbfa7a3c560f157f4a214fa8dc2924955bce7e34`.
Benchmark executable SHA256:
`149ef1113c53075b653a1209c2066a72ccf2fec7381eba79db591ba9db0fc0fb`.
Release and cap examples built with `lance,gpu`, through the 64 GiB/jobs1 wrapper.
Evidence and exact drivers are in `.scratch/admitted-construction-repair/`.

- 89 float/date oracle queries and ten additional non-null Float32/Float64
  arithmetic queries match DuckDB. All 58 primitive cases match the control's
  values and schemas; 50 match DuckDB. Eight integer division differences are
  preexisting and remain explicit failures of DuckDB compatibility.
- At a 64 KiB query budget, borrowed input completes and the arithmetic/string
  projections refuse by query-memory name. Literal/coercion allocations remain
  outside this proof.
- Both selected aggregate cap scenarios complete with actual spill and exact
  group counts: cgroup 1 GiB, peak RSS 410 MiB; RLIMIT_DATA 2048 MiB inside an
  8 GiB scope, peak RSS 412 MiB. Each reports 1,000,003 groups and 3,855,541,894
  accounted spill bytes. These are scoped aggregate gates, not global certification.

Ten alternating pairs plus warmup per component, 262,144 rows in 65,536-row
batches, four pinned threads and a 256 MiB query budget yielded:

| Component | 621 / rejected 620 | 621 / control 612 |
|---|---:|---:|
| Borrowed integer column | 0.989 | 1.028 |
| Integer addition | 1.013 | 1.132 |
| Non-null float multiplication | 0.714 | 0.974 |
| Integer to string | 0.773 | 1.283 |
| Addition plus integer to string | 0.797 | 1.300 |

All 220 engine requests passed correctness and fresh time ceilings. These are
single-session component ratios, not DuckDB leadership measurements.

Two separate diagnostic canonical SF10 Q12 executions passed canonical result
validation. Across 1,834 projection events every expression was column/alias-only;
no admitted arithmetic or integer-string kernel ran. The trace does not establish
those kernels as Q12's direct bottleneck. Trace timings are excluded from latency
acceptance. The initial trace driver's schema-sensitive equality failed on an
Int64/Decimal SUM representation; canonical revalidation passed and both initial
failure and corrected evidence are preserved.

The initial batch-size component similarly used order-sensitive equality for SQL
without ORDER BY. Its 1,024-row-batch control result passes typed multiset
revalidation. The corrected matrix uses the canonical bag comparator and preserves
the original failed run. The corrected matrix passes all 66 engine requests.
Candidate/control medians are 0.9950 at 65,536 rows/batch, 1.0147 at 1,024,
and 1.0210 at 64. This single-session component does not explain the prior
10–13% Q12 regression by itself. Protected Q12 repeats are complete: 44/44 engine requests validate and meet
time ceilings. Candidate/control ratios are 0.9899417 and 0.9983067 across
two fresh ten-pair sessions. The previous Q12 regression does not recur on
this binary. These measurements do not establish why the older binary regressed.

The remaining measured component issue is integer/string construction: addition
is 13.2% and integer-string conversion 28.3% slower than source612 in the
component session. Keep budget admission intact while investigating this cost.
Do not remove shared projection scoping on the assumption that it caused Q12.
Full provider validation and remaining allocation-boundary fixes are still open.


Verified archive: [provenance](benchmarks/2026-09-06-admitted-construction/provenance.json)
and [member hashes](benchmarks/2026-09-06-admitted-construction/members-sha256.json).
All 817 evidence members and the frozen source members were hash-verified.
Archive SHA256: `7259f2318f190eb8f8808cb5eb035fca04f485600e960f6bbcafcc6c4fae7339`.
Both original comparator failures and successful canonical revalidations are retained.
No production source changed during this follow-up; all measured results refer to
frozen621. Formatting and diff whitespace checks pass. No heavy job remains active.
