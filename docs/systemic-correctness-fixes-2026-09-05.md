# Systemic correctness fixes — 2026-09-05

This follows the [canonical SF1 investigation](canonical-sf1-findings-2026-09-05.md).
The implementation changes engine contracts rather than recognizing benchmark
queries, changing their SQL, or modifying their inputs. The original canonical
baseline validated 6 of 22 queries. The corrected engine validates **22 of 22**
in two fresh sessions, with **132 paired samples / 264 measured executions**.
Every typed comparison and fresh 10× DuckDB ceiling passes. The engine is still
**2.140× slower by suite time** (2.070× geometric mean); this establishes a valid
development baseline, not performance leadership.

## Causes and implementation

| Shared contract | Defect | Change |
|---|---|---|
| Arithmetic types | Binder always declared decimal arithmetic as `(38,10)`; runtime could coerce a decimal/integer pair to integers. Arrow kernels emitted different scales. | `planner/numeric.rs` defines operand coercion and result inference together. Decimal arithmetic uses the pinned Arrow kernels for both inference and execution; exact coefficients are checked against result precision. Decimal division produces floating point; SUM remains exact. Extreme scales that Arrow 58 would saturate or evaluate with overflowing metadata are refused explicitly. |
| Scalar representation | `rust_decimal` has a 96-bit coefficient and nonnegative scale, incompatible with the full Arrow Decimal128 domain. Literal broadcast also imposed scale 10. | `planner/decimal.rs::DecimalValue` carries an i128 coefficient and signed scale. Equality, hashing, ordering and exact rescaling preserve values. This changes the public payload of `ScalarValue::Decimal128`. |
| Aggregate state | Several SUM implementations used f64 or narrowed decimal coefficients to i64. Finalization could silently emit NULL for a non-NULL value of the wrong type. | Streaming, morsel, materialized and spill read-back decimal aggregation share an exact accumulator with checked addition and error propagation through merging/finalization. DISTINCT retains exact decimal keys. Invalid final output types return errors. Spill admission estimates use actual aggregate/key layout sizes. |
| Metadata proofs | Parquet physical integers were compared as whole numbers even for logically scaled decimals. The all-match proof rounded i64 statistics through f64 and was used underneath NOT. | Pruning requires compatible logical types and a flat matching leaf identity. Signed integer proofs remain exact through i128. Unsupported encodings retain the row group; both may-match and all-match proofs obey that rule. |
| Binding | GROUP BY could not resolve SELECT aliases; table/derived-table column alias lists were ignored. | Bare unresolved GROUP BY names can resolve to a unique SELECT alias, with input names taking precedence. Aggregate/window group keys are rejected. Column alias lists generate executable rename projections, including over CTE references. Duplicate source names are explicitly refused until ordinal binding exists. |
| CASE types | Planning chose only the first THEN type; execution had a separate, branch-order-sensitive coercion rule. | Planning and execution use the same common type across every result branch. |
| Subquery boundaries | Decimal scalar extraction was unsupported; a separate broadcaster could produce integer zero for unsupported values. Correlated result typing depended on the first row, and some errors became NULL/false. Materialization drove only partition 0 and scalar cardinality checked only the first batch. | Scalar broadcasting is shared with expression evaluation; conversion preserves decimal coefficients/scales. Correlated results use the declared result type and propagate errors. Materialization drives every declared partition; scalar cardinality covers all batches, including empty leading batches. |

These changes do not implement query-wide memory reservations, distributed
shuffle, or a new optimizer. Collected results and subquery caches still need the
resource-budget work identified in the audit. Conservative pruning can perform
more I/O; recovering a pruning optimization requires a proof in the correct
logical domain. Correct output is a prerequisite for a performance comparison.

## Regression approach

`tests/systemic_numeric_tests.rs` constructs independent values without benchmark
SQL/data. It checks exact Arrow coefficients and scales, including values above
64 bits and the old 96-bit scalar range, negative scales, NULLs, empty inputs,
multiple batches, worker counts, DISTINCT, mixed CASE branches, alias scope,
scalar cardinality and forced spill with an independently calculated answer.

`tests/partition_contract.rs` uses a fixed multi-partition operator to exercise
materialization. Its partition count does not depend on machine core count or
fixture size. Existing SQL, spill and partition suites are included in validation.

## Evidence and reproducibility

First diagnostic run: `.scratch/public-bench/tpch-sf1-systemic-01`. It uses the
unchanged pinned DuckDB canonical SF1 dataset and the versioned paired harness.
All 21 completed queries passed typed result validation; Q15 returned an explicit
unsupported decimal scalar-subquery error. Compilation overlapped this diagnostic
run, so its timings must not be treated as a performance baseline.

Builds and runs use `scripts/claude-safe-build.sh`, repository `.scratch` as
TMPDIR, and separate finite cgroups. No engine or benchmark ran without containment.

Durable evidence is in [the evidence directory](benchmarks/2026-09-05-systemic-correctness/README.md):
raw Arrow outputs, all samples, original SQL, plans, source/binary provenance,
worker traces, final reports, cap logs and checksums. The initial diagnostic is
preserved separately from the final run.

| Verification | Result |
|---|---|
| New systemic integration regressions | 12 passed, no ignored tests |
| Existing SQL / spill / partition suites | 130 / 13 / 17 passed on the final library; zero ignored tests |
| Formatting and diff whitespace checks | Passed |
| Canonical SF1, raw Parquet, default CPU features | 22/22; 132/132 typed comparisons; no failed time ceilings |
| Canonical run cgroup | Peak 1,922,670,592 bytes; zero OOM / kill / max events |
| Aggregate cap: 250,000,000 input rows, 1,000,003 output groups, 256 MiB engine budget | Both scenarios completed, exit 0 |
| Aggregate cgroup lever | 1 GiB cap; 408 MiB reported peak RSS |
| Aggregate RLIMIT_DATA lever | 2 GiB actual limit; 407 MiB reported peak RSS |

The cap driver prints a 1 GiB base cap for the RLIMIT scenario but adds its
existing 1 GiB virtual-startup allowance: the actual RLIMIT_DATA is **2 GiB**.
Its result check counts groups; the independent decimal spill regression checks
actual group keys and exact coefficient values. QueryMetrics currently populates
spill bytes but not its file-count field, so a zero file count is not evidence
that no spilling occurred.

Reproduction commands (run from the repository):

```bash
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=32G SAFE_BUILD_JOBS=1 \
  scripts/claude-safe-build.sh cargo build --locked --release \
  --example benchmark_embedded --example oom_cap_harness

TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=64G SAFE_BUILD_JOBS=2 \
  scripts/claude-safe-build.sh cargo test --locked --release \
  --test systemic_numeric_tests --test partition_contract \
  --test sql_comprehensive --test spill_tests --no-fail-fast -- --test-threads=2

TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=32G PYTHONPATH=scripts \
  scripts/claude-safe-build.sh taskset -c 0-15 .scratch/venv-lance/bin/python \
  -m benchmark run --dataset .scratch/public-bench/tpch-sf1/dataset.json \
  --engine-binary target/release/examples/benchmark_embedded \
  --output .scratch/public-bench/tpch-sf1-systemic-repeat \
  --samples 3 --sessions 2 --threads 16 --memory-gib 4 --process-cap-gib 12

TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=8G OOM_HARNESS_SCENARIOS=agg \
  OOM_HARNESS_LOGDIR=.scratch/systemic-cap-repeat \
  scripts/claude-safe-build.sh scripts/oom_cap_harness.sh

cargo fmt --all -- --check
```

The final canonical run had no overlapping builds or cap tests. SQL, data,
DuckDB version, threads/affinity, memory and timing boundaries come from the
versioned manifest. This run uses warm raw Parquet and collected Arrow results.
The previous SF10 custom-workload results remain historical evidence. Native,
Iceberg, Lance, decoded IPC and GPU need renewed certification with the current
source; this run does not establish their correctness or performance.

## Next work, in order

1. Keep the 12 independent regressions and canonical typed gate as required
   correctness checks. Extend them to randomized decimals, mixed signed/unsigned
   arithmetic, implicit casts versus CAST/TRY_CAST, and NULL-sensitive IN/NOT IN.
   Source inspection still finds default Arrow casts that can turn conversion
   failures into NULL and a generic IN fallback with incomplete NULL semantics.
   Those paths are not certified by 22 canonical queries.
2. Profile Q1/Q6 and the shared decimal aggregate kernels. They are 4.160× and
   3.815× DuckDB respectively. The shared accumulator is now correct, but
   Decimal128 still uses generic scalar extraction in the morsel accessor.
   Add typed decimal batch kernels, selection-mask handling and checked partial
   sums while retaining exact overflow/NULL/merge behavior. Measure the fraction
   spent decoding, computing, grouping and merging before choosing a rewrite.
3. Restore decimal/float metadata pruning only with type-correct proofs and
   adversarial row-group regressions. Retain an explicit conservative fallback.
   Evaluate compact decimal execution/storage as a separate physical-design
   experiment: Arrow Decimal128 representation and logical SQL precision need
   not dictate every internal vector representation, but exported Arrow schemas
   and overflow semantics must remain correct.
4. Close the remaining audit contracts: semantic uniqueness must come from
   verified constraints; query-wide memory reservations must cover operator
   metadata, materialization, caches and results. The selected cap passes do not
   certify all providers, concurrency, or collected-result safety.
5. Re-run canonical SF10 and provider/residency/concurrency tracks with the
   approved benchmark matrix. Require correctness first, then the documented
   leadership gates. Do not compare new correct decimal execution with old
   timings from queries that returned NULL or dropped rows.

