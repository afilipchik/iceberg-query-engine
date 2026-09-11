# Native decimal pipeline investigation — 2026-09-06

The native SF10 Q06 time-gate failure exposes a likely shared decimal execution bottleneck. The same binary's native and decoded IPC warmups spend approximately 300 ms executing the same physical pipeline. Native faces a much faster DuckDB native-storage reference. This is not evidence that native decoding alone accounts for the gap.

The full latency matrix and its post-matrix gates have finished. The original source investigation below is retained; a completed thread/CPU diagnostic and its limitations follow.

## Evidence and identity

Evidence root: `.scratch/public-bench/canonical-sf10-provider-gates-02/`, with `native/` and `decoded_ipc/` subdirectories. Both identify:

- Engine binary SHA256: `5224c0c602f34de66c857b5d6bb2ff8dcb754fff4c34f038857512929ca5ef3b`.
- Dataset manifest SHA256: `ff097ac1f71b689618970e7c7c16c721ccab205c76286ea1f29866f34a8c7e99`.
- Original SQL SHA256: `3638535fb92a6026b3359a2ad9e6ed7da9ad9080152513e6fd3818d1a853c29f`.
- 16 threads, 40 GiB query setting and 48 GiB process cap.

The query computes exact decimal `SUM(l_extendedprice * l_discount)` over a date range, decimal discount band and decimal quantity predicate. The original SQL is in the warmup request; no rewrite or floating-point substitution is proposed.

| Recorded warmup | Native | Decoded IPC |
|---|---:|---:|
| Total engine ms | 364.496188 | 323.800490 |
| Execute ms | 299.578184 | 315.246687 |
| Optimize ms | 0.415286 | 0.433375 |
| Plan ms | 4.072496 | 7.909085 |
| Output rows | 1 | 1 |
| Observed local bytes | 36,742,074 | 36,742,074 |
| Reserved peak bytes | 0 | 0 |

These are individual **warmups**, not accepted medians or a paired performance estimate. Component timers do not sum to the complete SQL-call timer; do not assign the residual to decoding without measurement. Local observed bytes are incomplete accounting, not RSS or query-wide owned reservations.

Native DuckDB calibration was 22.781400, 22.750190 and 27.042241 ms, giving a 227.814000 ms ten-times-median ceiling. Native's first measured engine request timed out; nine later samples were worker-unavailable fallout. This is one watchdog timeout followed by unavailable samples, not ten independent slow executions and not an OOM. The warmup completed one correct row, but it does not turn the failed measured query into an accepted result.

Decoded IPC DuckDB calibration was 238.237490, 239.965248 and 237.492079 ms. Its first measured engine sample completed in 325.686058 ms with typed bag validation (`decimal128(38,4)`, one row). The reference timing boundary is deliberately provider-specific: `scripts/benchmark/providers.py:113` materializes native reference data into DuckDB via `CREATE TABLE ... AS SELECT * FROM read_parquet(...)`; decoded IPC registers a preloaded Arrow table. DuckDB's native column storage versus Arrow scan explains why equal engine runtimes can pass one mode's ceiling and fail another. It does not excuse the native failure or authorize changing its reference.

### Exact artifact hashes

At the initial inspection the JSONL files were still growing. The hashes below identify the **single raw UTF-8 JSONL record including its terminating newline**, selected by the named request/sample, rather than an unstable whole-file hash.

| Artifact | SHA256 |
|---|---|
| native/manifest.json | `23ec1347824c2f650502fdc372787b3ed6c3569bb1e503a6d3a5d96deda5b71e` |
| native/setup.json | `c7bfeecb64bf8e3b892729f4c83ce590dbea72a423f53f0c22871d5ea8e8bfc0` |
| native/execution.jsonl, engine s1-q06-warmup | `c5de9d02852c07645634afdd2744d4d489ef4ebb6ba8099d512c473fcad37bd1` |
| native/samples.jsonl, s1 q06 iteration 1 | `7005c5bf61b614a27ef587ae21c1d78b795bf48427a35f3bc42bb89937f84cc0` |
| decoded_ipc/manifest.json | `ed5d14933bc23743ab72b98af8694f63b587a3e7ba439db23c37289376611d15` |
| decoded_ipc/setup.json | `e844b4ee03df0f3656c8dfe6012b392baf5228866e3c0f5bee15cacb5352fe88` |
| decoded_ipc/execution.jsonl, engine s1-q06-warmup | `c69ca7877dcc65109bbfd7586e4c1ca5522b5fff698e8760ddd477b3169ed14f` |
| decoded_ipc/samples.jsonl, s1 q06 iteration 1 | `c6e205be2d0e0335791853fdfbd1e7fe80fdb0f2d597d980397dabd9f6966a18` |

Manifest hashes describe the files at inspection time; any final provenance rewrite must preserve this distinction.

## Proved routing and bottleneck hypotheses

Both warmups record this physical plan:

```text
Project
  SpillableHashAggregate
    Project
      Filter
        MemoryTableScan
```

The source supports several concrete mechanisms, but no CPU profile yet assigns their relative cost:

1. **The decimal predicate uses the interpreter.** `src/physical/compiled_expr.rs:278` supports primitive Float64/Int64/Int32/Date32 comparison inputs, not Decimal128. Its evaluator falls back to `evaluate_expr` when compilation fails (`:749`). The query's decimal comparisons therefore prevent the full predicate from using this compiled path. This establishes routing; its runtime fraction remains a hypothesis.
2. **Filter output is materialized before reduction.** `src/physical/operators/filter.rs:80` evaluates a mask and filters every retained column into output arrays. `spillable.rs:3224` collects filtered batches until the local threshold, then builds a temporary MemoryTableExec and HashAggregateExec (`:3246`). The observed local input total agrees across modes. Repeated mask/intermediate allocation and gathers are plausible bandwidth costs.
3. **The exact decimal reduction iterates batches serially.** `hash_agg.rs:413` selects `aggregate_exact_decimals`; its state processes each batch in a normal loop (`:475`). Exactness is required. A serial reduction stage can limit scaling even though upstream MemoryTableScan partitions and filtering run concurrently. No measurement yet proves this stage dominates total time.
4. **Native cannot use this Parquet aggregate shortcut.** `physical/planner.rs:1274` routes suitable Parquet sources to MorselAggregate. Native routing is restricted to unfiltered dense-group shapes; `try_extract_native_dense_source` rejects filtered scans (`:246`). A filtered global decimal sum thus takes the recorded generic pipeline. This is a provider capability gap, not justification for a query-name branch.
5. **Native reads are mmap-backed, with deferred work possible.** `storage/native_table.rs:807` prunes segments and collects projected batches; it does not evaluate the complete predicate. `storage/ipc_cache.rs:433` maps IPC files and retains their mappings through Arrow buffers. Small plan time does not rule out execution-time page faults. Conversely, the similar IPC execution time makes a native-only decode explanation insufficient. `QE_DEBUG_NATIVE_PRUNING=1` can expose scanned/skipped segments without guessing.

An earlier raw Q06 warmup used MorselAggregate and took 374.274 ms, but its binary SHA was `39425cd86a3ea0d52f357152147b515aec02a4c5f74bc8a5c56449a3a80d9cf5`. It is historical context, **not a matched current-binary raw comparison**.

## Confirmed literal broadcasting in the interpreted predicate

A further source trace confirms a specific allocation mechanism for ordinary decimal-column comparisons against literals. This confirms behavior, **not its measured runtime cost**:

- `filter.rs:209` evaluates every generic `Expr::Literal` with `scalar_to_array(value, batch.num_rows())`. The Decimal128 case (`:465`) constructs `vec![coefficient; num_rows]` and a full-length Decimal128Array with the literal scale. Its coefficient buffer alone is 16 bytes per row. Integer literals likewise expand into full-length integer arrays (`:451` and adjacent cases).
- Generic binary evaluation recursively evaluates both operands (`:211`, `:247`) before `evaluate_binary_op`. The intervening literal shortcuts cover dictionary strings and LIKE, not decimal comparisons. An explicit CAST also evaluates its child array first, then casts that array (`:255`); it does not retain a scalar constant representation.
- `coerce_arrays` (`:676`) computes a common numeric type and invokes strict array casts on operands whose types differ. Thus an integer literal compared with a decimal column first exists as an integer array and can then require a decimal array; scale/precision coercion can introduce additional array work. Identical types reuse their existing arrays, so this is not a claim that every comparison copies both operands.
- `compare_arrays` (`:705`) accepts the kernel's `Datum` interface but passes the two `ArrayRef` values directly. Arrow 58's `arrow-array/src/scalar.rs:83` implements ordinary-array Datum with the scalar flag false; the separate Scalar wrapper uses true (`:148`). The generic comparison here does not construct that scalar wrapper or use compiled scalar access.
- `BETWEEN` evaluates the value and both bound expressions as arrays, computes two comparison masks and combines them (`filter.rs:357`). Full-length literal buffers and temporary masks are therefore additional allocation candidates alongside filter gathers. Their overlapping lifetimes and allocator reuse need measurement; multiplying buffer sizes across all batches would not establish peak RSS.

A bounded follow-up should inspect allocation stacks and compare scalar-Datum evaluation against the existing interpreter in a generic exact-decimal regression fixture. It must preserve common-type coercion, scale, NULL and overflow behavior. No implementation or benchmark change has been made for this finding.

## Next bounded experiment

After the active matrix finishes, retain the frozen binary, original SQL, identical data, current reference and unchanged 40/48 GiB caps. Profile diagnostic warmups separately from acceptance samples; preserve the existing failed time gate.

- Capture native and decoded IPC CPU call stacks, task-clock and page-fault counters, with `PLAN_DEBUG` and `QE_DEBUG_NATIVE_PRUNING=1`. Attribute cost to decimal predicate evaluation, Arrow filter/gather, exact aggregation, mapping faults, or another measured component.
- Repeat matched one-thread and sixteen-thread diagnostic warmups. Scaling upstream filtering with a nearly constant reduction component would support the serial-reduction hypothesis. Similar high fault costs would instead motivate scan residency investigation. These are hypotheses to discriminate, not predicted results.
- `QE_AGG_PROF=1` may provide aggregate sections, but verify which counters are emitted: generic exact aggregation does not promise complete phase coverage. Missing counters are not zero cost.
- If computation/materialization dominates, evaluate a shared exact decimal predicate/filter/reduction pipeline over provider-neutral batch streams. Preserve checked decimal scales/overflow, NULL semantics and bounded retained state; never cast the computation to Float64 for speed. Add independent typed tests for decimal boundaries, duplicates, NULLs, batches and ordinary/specialized paths before broader measurement.

No query-ID, table-name, literal-date, or workload-specific fix is proposed. A fused implementation and full resource safety are future work; neither is certified by these warmups.

## Completed thread/CPU diagnostic

[Preserved evidence](benchmarks/2026-09-06-decimal-thread-scaling/README.md)
uses the unchanged original SQL and frozen binary above. Decoded IPC at sixteen
threads completes three steady requests in 311.673–314.283 ms, using
4,700–4,740 ms process CPU per request, about fifteen cores. Aggregate process
counters total 10.806–10.954 ms, including 7.716–7.981 ms expression evaluation.
These counters do not cover upstream filtering. The trace does **not** support
serial aggregation as the dominant explanation of this query's runtime.
Predicate evaluation/filter materialization is the better next target, although
allocation and individual kernel costs still require an isolated experiment.

No major page faults were observed. Minor faults remain present; this does not
exclude memory bandwidth or allocation costs. The one-thread IPC and both native
cases time out under their fresh, unchanged 10× reference ceilings. Their partial
CPU windows cannot establish a completed-query thread scaling ratio. A cold IPC
optimizer pass takes 409.550 ms; subsequent passes take about 0.4 ms, consistent
with the lazy statistics cache. Keep cold setup cost separate from steady work.

The host denies perf counters; own-worker process counters were used without
changing host permissions. This diagnostic supplies no accepted suite score.
Next: fix independently found Boolean NULL semantics, then test a general scalar
comparison representation to avoid full-length literal arrays. Preserve exact
decimals and typed NULL/cast behavior; measure against the frozen candidate.

## Post-scalar-fix diagnostic completed

The unchanged Q6 diagnostic now completes all 16 attempts (four configurations,
one warmup plus three steady attempts each), with typed validation and unchanged
fresh 10× DuckDB ceilings. The previous IPC-one-thread and both native cases
timed out; those failures remain preserved rather than imputed before timings.

Steady median elapsed/CPU milliseconds:

| Configuration | Elapsed | Process CPU |
|---|---:|---:|
| IPC, 1 thread | 482.829 | 490 |
| IPC, 16 threads | 76.473 | 1,020 |
| Native, 1 thread | 632.638 | 630 |
| Native, 16 threads | 141.941 | 1,180 |

The prior completed IPC-16 case had median elapsed 313.683 ms and CPU 4,730 ms.
The new candidate reduces both, consistent with removal of repeated full-batch
comparison-constant work rather than a change in join plan or query-specific
branch. The diagnostic has few samples and profiling overhead; full-suite
acceptance belongs to the separate 660-pair reports. Native still has a
substantial gap against DuckDB, and thread speedup is sublinear in both providers.

All raw counters, plans, oracles and setup files are retained under
`.scratch/public-bench/decimal-thread-scaling-02`; compact summary is in the
scalar candidate evidence directory. Source/binary identity remains the frozen
scalar candidate. No host perf permissions were changed.
