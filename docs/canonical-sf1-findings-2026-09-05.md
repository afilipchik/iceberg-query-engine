# Canonical SF1: reproduced correctness failures

**Follow-up:** the [systemic fixes](systemic-correctness-fixes-2026-09-05.md) now validate 22/22 canonical queries. This document preserves the original failure evidence and pre-fix analysis.

**2026-09-05, production source `88849c4`.** The new embedded harness validates
only **6 of the 22 canonical TPC-H queries**. The same 16 queries fail in the
initial one-pair run, a three-pair repeat, and a final one-pair run with plans.
This is a correctness finding, not a valid performance baseline or a leadership
claim. Production Rust source and Cargo dependencies have not been changed.

The original custom SF10 results remain valid evidence for their exact data,
SQL and comparison contract. They did not establish canonical decimal or SQL
coverage. The new run demonstrates why optimizing that custom suite alone was
insufficient.

## Evidence

[Artifact directory](benchmarks/2026-09-05-canonical-sf1/) contains:

- [Per-query outcomes](benchmarks/2026-09-05-canonical-sf1/query-outcomes.json),
  [dataset manifest](benchmarks/2026-09-05-canonical-sf1/dataset.json), all 22
  original/adapted/oracle SQL files, and the generator license.
- `tpch-sf1-run01.tar.gz`: initial one-pair diagnostic run.
- `tpch-sf1-run02.tar.gz`: three pairs per query, source/harness hashes, all
  Arrow results and traces, worker setup and cgroup counters.
- `tpch-sf1-run03-plans.tar.gz`: final adapter verification, engine physical and
  optimized plans, DuckDB EXPLAIN results, and existing engine spill telemetry.
- `decimal-probes-run01.tar.gz`, the tiny `decimal-probes/` fixture and
  `prepare_probes.py`: independently reproducible counterexamples.
- [SHA256SUMS](benchmarks/2026-09-05-canonical-sf1/SHA256SUMS) covers the artifacts.

The large input Parquet files remain in `.scratch/public-bench/tpch-sf1` and can
be regenerated using the pinned preparer. Full result streams, including
unlimited LIMIT oracles, are in the archives. No query or unsuccessful sample
was removed. The three-pair run contains 66 pairs: 18 validate, 48 fail answer
validation or execution. Its cgroup reported zero OOM/OOM-kill events and a
combined high-water mark of 1,943,085,056 bytes; that is not per-query engine RSS.

## Conditions and commands

DuckDB 1.4.4 with its matching `tpch` extension; PyArrow 25.0.1; default-feature
release `benchmark_embedded` example; CPUs 0–15; 16 execution threads; 4 GiB
query budgets; equal 12 GiB RLIMIT_DATA; 32 GiB shared cgroup with swap disabled.
One persistent worker per engine, sequential execution with randomized pair
order, warm host cache, one warmup per engine and three fresh DuckDB calibration
samples per query. Native, Iceberg, Lance, IPC and GPU were not run in this new
canonical experiment. Their existing custom-SF10 evidence is unchanged.

Both engines read the same original Parquet files with decimal schemas intact.
Engine IPC cache and GPU offload are disabled. Embedded timing includes parsing
through complete Arrow result collection; result-file serialization and
validation are separate. Correctness compares exact integer/decimal values,
floating tolerances, ordering and LIMIT boundary ties. The runner returns exit
1 and withholds aggregate scores when any required query fails.

```bash
mkdir -p .scratch
export TMPDIR="$PWD/.scratch"
export PYTHONPATH="$PWD/scripts"

SAFE_BUILD_MEM=64G scripts/claude-safe-build.sh cargo build --locked --release --example benchmark_embedded

SAFE_BUILD_MEM=4G scripts/claude-safe-build.sh .scratch/venv-lance/bin/python -m benchmark prepare \
  --workload tpch --scale-factor 1 --output .scratch/public-bench/tpch-sf1

SAFE_BUILD_MEM=32G scripts/claude-safe-build.sh taskset -c 0-15 .scratch/venv-lance/bin/python -m benchmark run \
  --dataset .scratch/public-bench/tpch-sf1/dataset.json \
  --engine-binary target/release/examples/benchmark_embedded \
  --output .scratch/public-bench/tpch-sf1-run02 \
  --samples 3 --sessions 1 --threads 16 --memory-gib 4 --process-cap-gib 12

SAFE_BUILD_MEM=4G scripts/claude-safe-build.sh .scratch/venv-lance/bin/python -m unittest discover -s scripts/benchmark/tests -v
cargo fmt --all -- --check
```

Use new output paths when repeating; existing datasets/runs are deliberately
not overwritten. The tests pass **34/34, no skips**; the release adapter build
and formatting check pass. Existing production compiler warnings remain.
The benchmark's nonzero exit is the expected response to the engine failures,
not a successful correctness gate. Detailed CLI behavior and remaining harness
limits are in [the harness guide](../scripts/benchmark/README.md).

The generator/SQL source is DuckDB's pinned
[TPC-H extension](https://github.com/duckdb/duckdb/tree/v1.4.4/extension/tpch),
used through its documented
[dbgen/tpch_queries interfaces](https://duckdb.org/docs/lts/core_extensions/tpch).
All original SQL hashes are pinned in the harness; Q11's threshold is adapted
as `0.0001 / SF`. This is an engineering workload, not an audited TPC-H result.
The first dataset predates the preparer's license-copy addition; its original
manifest is preserved and the verified generator license is included beside it.

## What failed

| Queries | Reproduced result |
|---|---|
| Q2, Q4, Q12, Q16, Q17, Q21 | Correct in every measured sample |
| Q1, Q3, Q5, Q6, Q7, Q9, Q10, Q22 | Decimal aggregate outputs include NULL where DuckDB returns non-NULL values |
| Q8 | NULL market shares; engine declares decimal output while DuckDB declares double; strict schema comparison rejects it first |
| Q11 | Execution error: expected Decimal128(38,10), found Float64 |
| Q13 | Binder rejects derived-table column alias `c_count` |
| Q15 | Binder rejects GROUP BY output alias `supplier_no` |
| Q14 | `16.37921338292933` versus `16.380778626395543`, well outside tolerance |
| Q18 | Zero rows versus DuckDB's nonempty result |
| Q19 | `0.0003226754` versus `3083843.0578` |
| Q20 | Zero rows versus 186 |

For Q6, the engine returns NULL versus `123141078.2283`. For Q1 the first
group's quantity sum is NULL versus `37734107.00`. These are value failures,
not precision-display differences. Do not relax the comparator to pass them.

## Six-row counterexamples

The fixture has two Parquet row groups of three rows, with decimal values stored
as physical integers. Its columns are `g: string` and `v: decimal(15,2)`:

| g | v |
|---|---:|
| a | 1.25 |
| a | 2.50 |
| a | NULL |
| b | 1.25 |
| b | -0.50 |
| b | 0.00 |

| SQL | Engine | Independent expected result |
|---|---|---|
| `SELECT SUM(v) FROM d` | NULL | 4.50 |
| `SELECT g, SUM(v) FROM d GROUP BY g` | Both NULL | a=3.75, b=0.75 |
| `SELECT SUM(v*v) FROM d` | NULL | 9.6250 |
| `SELECT COUNT(*) FROM d WHERE v < 2` | 3 | 4 |
| `SELECT COUNT(*) FROM d WHERE v < 2.0` | 4 | 4 |
| `SELECT AVG(v) FROM d` | 0.9 | 0.9 |
| `SELECT COUNT(*) FROM d x JOIN d y ON x.v=y.v` | 7 | 7 |
| `SELECT g AS label, COUNT(*) FROM d GROUP BY label` | Column not found | a=3, b=3 |
| `SELECT label FROM (SELECT g FROM d) AS t(label)` | Column not found | Three a and three b |

These examples establish distinct defects rather than assuming every canonical
failure has one cause. The passing decimal join/average probes do not certify
those operations at other scales, encodings or spill paths.

## Source analysis and next implementation steps

1. **Exact decimal aggregate state.** In
   `src/physical/morsel_agg.rs`, `AccumulatorState::new` routes decimal SUM to a
   floating accumulator. Decimal result construction accepts only Decimal128
   scalar values and silently appends NULL for other values. That mechanism
   explains the small SUM failure. The same file constructs decimals from
   mantissas without reconciling source and destination scales. In
   `operators/hash_agg.rs`, decimal sums narrow i128 values into i64. Implement
   checked exact state, scale-aware finalization and merge/spill serialization;
   audit the shared accumulator users. Test 38-digit inputs, overflow, negative
   scales, duplicate/DISTINCT values, NULLs, empty inputs and all execution paths.
   Do not repair this by converting exact decimals through f64 or merely
   replacing NULL with a floating result.
2. **Type-aware pruning proofs.**
   `src/storage/row_group_pruning.rs::check_comparison` dispatches on the literal
   type and compares integer physical footer values without checking the
   decimal column's scale. `definite_comparison` has the analogous problem for
   predicates declared always true. The `v < 2` versus `v < 2.0` reproducer is
   consistent with that mechanism. Add real Parquet tests for both pruning and
   filter elimination, all physical decimal encodings, both operand orders,
   missing stats and large integer boundaries. Until an exact conversion can
   prove a result, keep the row group and evaluate its predicate.
3. **SQL alias scope.** Add independent binder/execution tests for GROUP BY
   aliases and derived-table column alias lists, including conflicts with input
   names, joins and multiple columns. Fix name resolution with explicit scope
   rules, or record an equivalent translation on both benchmark sides. Preserve
   original SQL and hashes. Do not replace canonical queries silently.
4. **Remaining query failures.** Re-run all 22 after each narrow semantic fix.
   Q14/Q18/Q19/Q20 still need differential plans and intermediate checks; their
   exact root causes are not established by this report. The captured Q19/Q20
   plans use spillable aggregates and joins with materialized scans. Do not
   infer actual disk spill from an operator's name.
5. **Continue the existing plan.** Preserve the earlier NDV/uniqueness and CTE
   partition investigations, query-wide budget work and provider benchmarks.
   These new reproductions belong in the correctness gate before further
   performance tuning. Validate canonical SF1 first, then obtain valid SF10,
   JOB and ClickBench baselines across all required modes. Wrong answers cannot
   contribute to a DuckDB speed comparison.

The six-row failures and source inspection are completed diagnostic work.
Production fixes, complete provider adapters, resource/concurrency matrices
and DuckDB leadership certification remain unfinished.
