# Architecture Review — iceberg-query-engine

Reviewing architect's final report. Every claim below was verified in source or by measurement; line numbers are from the working tree at review time. Findings that did not survive verification are listed in §6 so you know they were considered.

---

## 1. Health assessment

**What this engine gets right, specifically:**

- **The cost model is real.** DPsize enumeration over actual Parquet footer statistics, with a documented conservative bias (`side_combined_ndv` argues full correlation "can only overestimate join outputs, never hide a blow-up"). This is what took SF=10 from 339x to 10.2x, and it is not a heuristic pile.
- **The fast paths are earned, not lucky.** Morsel-driven aggregation with raw u64 group keys, RowStore row-native join build, direct-address hash join, batch-level vectorized probe, runtime filter pushdown, semi-join reduction, EagerAggregation. Q09 at 1.0x native and Q13 at 1.1x are real numbers against a real competitor.
- **The capability-neutral parts of the storage interface work.** `statistics()` and `scan_with_filter()` ask what a provider *can do*. Because of that, `LanceTable` reached statistics parity and now beats the Parquet leg by 8% at SF=10 (`src/storage/lance.rs:28-32`) **despite having none of the decoder fast lanes**. That is the strongest empirical argument in the codebase for the capability refactor recommended in F12.
- **`scan_knn` is the model contract.** `Ok(None)` means "not supported, caller MUST fall back to the always-correct exact path" (`src/physical/operators/scan.rs:72-92`). Approximate results are opt-in. This is exactly right and should be the written rule for every future optional trait method.
- **Where the team applied discipline, it held.** `spillable.rs:352-382` refuses non-INNER spilling joins with an error naming the limitation, and `tests/spill_tests.rs:247-263` asserts the error text contains `"INNER"` with the comment that silently returning inner-join results "is data corruption." That is the only place in the test tree that treats a wrong answer as worse than a failure, and it is the template for everything else.
- **`tests/spill_tests.rs` is the best-architected file in `tests/`.** Same query, two configurations, results must match — *and* an assertion at `:133` that the spill actually happened, so the test cannot silently degrade into running the same path twice.

**Where it is structurally weak:**

Three invariants are carried by convention rather than by type, and all three are load-bearing:

1. **Partition driving.** `output_partitions() -> usize` (`src/physical/plan.rs:16-33`) is an advisory integer with a default of 1, no `with_new_children`, no `as_any`, and therefore no physical rewrite pass. Out-of-range `execute(partition)` returns an **empty stream** in every implementation, so a disagreement is never an error and always a wrong row count.
2. **Memory.** `MemoryPool::try_allocate`/`allocate` have **zero production callers** (verified by grep across `src/ tests/ benches/ examples/`). The rule is enforced as seven independent copies of `memory_limit * spill_threshold`, each comparing one operator's local tally against the *entire* budget.
3. **Schema/column identity.** `LogicalPlan` caches a schema in every node, and `with_new_children` copies the stale one (`src/planner/logical_plan.rs:135,145,152,168,174`). There is **no plan validator anywhere** — `grep "fn validate|validate_plan" src/planner/ src/optimizer/` returns nothing.

And the oracle cannot see any of it: `data/tpch-1mb` is 6,000 rows in one 8192-row batch, so `MIN_BATCHES_FOR_PARALLEL = 32`, `PARALLEL_BUILD_THRESHOLD = 10_000`, RowStore's `>= 100_000` and the parallel-aggregate gates are all structurally unreachable by the 850-test suite.

---

## 2. Findings, ranked by (severity × confidence) / fix cost

### Tier A — critical, confirmed, small fix. Ship this week.

---

**F1. `UnionExec` drops every input partition except 0 — UNION ALL loses 96% of rows**
*Severity: critical · Confidence: CONFIRMED (reproduced) · Cost: small*

**Evidence.** `src/physical/operators/union.rs:45-55`: `async fn execute(&self, _partition: usize)` ignores its argument and calls `input.execute(0).await?` for every input. There is no `output_partitions()` override in the file, so it inherits the trait default of 1 (`src/physical/plan.rs:27-29`).

Reproduced with the shipped release binary on `data/tpch-100mb`:
```
SELECT COUNT(*) FROM (SELECT l_orderkey FROM lineitem
                      UNION ALL SELECT l_orderkey FROM lineitem) x
  -> 49152        (truth: 1,200,000)
```
49152 = 6 × 8192 = partition 0's three batches from each branch. The same query on `data/tpch-1mb` returns 12,000 — correct — which is why the setop tests are green.

**Failure mode.** Any UNION/UNION ALL over a table scanned in more than one partition silently returns a small deterministic subset. UNION (distinct) is lowered as group-by-every-column on top of this and inherits the loss.

**Fix.** `fn output_partitions(&self) -> usize { self.inputs.iter().map(|i| i.output_partitions()).sum() }` plus an `execute(partition)` that maps the global index onto (input, local partition). The lower-risk variant matching the existing idiom is to keep `output_partitions() == 1`, return `stream::empty()` for `partition != 0`, and drain via `collect_input_partitions_concurrently` (`spillable.rs:50`) — this is what `SortExec` already does at `sort.rs:64-77`. Prefer the sum version; it preserves parallelism.

---

**F2. `LimitExec` applies LIMIT/OFFSET per batch, not per query — `LIMIT 10` returns 740 rows**
*Severity: critical · Confidence: CONFIRMED (reproduced) · Cost: small*

**Evidence.** `src/physical/operators/limit.rs:42-104`. `skipped`/`fetched` are `usize` locals captured by the outer `move` closure; the mutations happen inside an inner `async move` block, which **copies** both `Copy` values into every per-batch future. The writes never escape one batch. The `#[allow(unused_assignments)]` at `limit.rs:41` with the comment *"Variables are read across multiple closure invocations"* is the compiler telling you the exact opposite, and is the tell.

Reproduced on `data/tpch-100mb`: `SELECT l_orderkey FROM lineitem LIMIT 10` → **740 rows** (74 batches × 10). On `data/tpch-1mb` → 10, correct.

**Why TPC-H never sees it.** `src/physical/planner.rs:1327-1352` fuses Sort+Limit into `SortExec::with_fetch` whenever `skip == 0` and the child is a Sort, so no TPC-H top-N query constructs a `LimitExec` at all. With OFFSET, the LimitExec sits over a SortExec that emits exactly one batch, which masks it. `tests/sql_comprehensive.rs:519-600` uses a 5-row in-memory table.

**Fix.** Replace `filter_map(move |r| async move {...})` with `futures::stream::unfold`/`scan` threading `(skipped, fetched)` as explicit stream state. Then delete `#[allow(unused_assignments)]` and never re-add it. Do **not** use `Arc<AtomicUsize>` — that re-introduces the per-operator-mutable-state hazard of F17.

---

**F3. `LimitExec` declares 1 partition but forwards `partition` to a multi-partition child — filtered LIMIT returns 0 rows**
*Severity: critical · Confidence: CONFIRMED (reproduced) · Cost: small*

**Evidence.** `limit.rs` has no `output_partitions()` override (reports 1) while `execute` does `self.input.execute(partition)` at `limit.rs:44`. `src/execution/context.rs:325` therefore drives only partition 0.

```
SELECT COUNT(*) FROM lineitem WHERE l_orderkey > 140000        -> 39722
SELECT l_orderkey FROM lineitem WHERE l_orderkey > 140000 LIMIT 100000 -> 0 rows
```
(on `data/tpch-100mb` — every match lives outside scan partition 0.)

This is **shipped bug #8's exact shape** in a different operator. It compounds with F2: the two can mask each other.

**Fix.** Keep `output_partitions() == 1`, return `stream::empty()` for `partition != 0`, drain all input partitions. **Caveat worth stating:** draining all partitions removes LIMIT's early exit. That is not a regression today — every scan below already materializes into a `Vec` and hands back `stream::iter` (`scan.rs:296`, `parquet.rs:129`), so nothing in this engine short-circuits on LIMIT. Prefer routing this through `required_input_distribution() == SinglePartition` + a `CoalescePartitionsExec` so the eventual streaming fix has one place to land.

---

**F4. CTE materialization and uncorrelated subqueries execute only partition 0 — a twice-referenced CTE returns 0 rows**
*Severity: critical · Confidence: CONFIRMED (reproduced) · Cost: small*

**Evidence.** `src/physical/operators/subquery.rs:35-49`: `run_subquery_blocking` does `rt.block_on(physical.execute(0))` and never consults `physical.output_partitions()`. It is the entry point for CTE materialization (`src/physical/planner.rs:541`) and all three uncorrelated-subquery paths (`subquery.rs:275, 319, 368`).

With `CTE_DEBUG=1` on `data/tpch-100mb`: `[cte] materialized t ... -> 0 rows` for a CTE whose predicate independently counts 39,722; the query returns 0 instead of 79,444. Requires ≥2 references, because single-reference CTEs are inlined (`planner.rs:525-535`).

**Why TPC-H survives.** Q15's CTE is aggregate-rooted, so its plan root is already single-partition.

**Fix.** Drain `0..physical.output_partitions()` via `collect_input_partitions_concurrently`. Separately: this function does `std::thread::spawn(...).join()` from inside `FilterExec`'s async body, parking a tokio worker for the whole subquery — see F19.

---

**F5. `ExternalSortExec` k-way merge reuses row indices across buffer refills — panics, or silently emits wrong rows**
*Severity: critical · Confidence: CONFIRMED (reproduced) · Cost: small*

**Evidence.** `src/physical/operators/spillable.rs:1490-1517`. `output_rows: Vec<(run_idx, row_idx)>` accumulates up to `buffer_rows` (8192) entries before flushing, but the exhaustion branch **replaces** `run_buffers[run_idx]` with the next batch and resets `run_indices[run_idx] = 0` while stale entries are still pending. `build_merged_batch` resolves every pending pair against the **current** `run_buffers` (`:1554`). `build_merged_batch_final` is independently wrong the same way (`:1615-1692`).

Reproduced: `SELECT l_orderkey, l_partkey, l_extendedprice FROM lineitem ORDER BY l_orderkey` on `data/tpch-100mb` at an 8 MB limit →
```
panicked at arrow-select-53.4.1/src/take.rs:420:
index out of bounds: the len is 1984 but the index is 1984
   ... ExternalSortExec::streaming_k_way_merge
```
`generate_runs` sizes runs at `memory_limit * 0.8` while the merge reader uses `with_batch_size(8192)` (`:1415`), so mid-window refills are the **normal** case, not an edge case. When the refreshed batch is full-size the index is in range and the query silently returns rows from the wrong positions — an out-of-order, duplicated-and-dropped result set with no error.

`tests/spill_tests.rs` cannot see it: SF=0.001 yields runs under 8192 rows, so no refill ever happens inside a window.

**Fix.** Flush `output_rows` via `build_merged_batch` **before the whole exhaustion branch** (not just before the refill — the sibling `run_buffers[run_idx] = None` case at `:1503` leaves pending rows unresolvable too). Then delete `build_merged_batch_final`. The proper fix is a per-run `RunCursor { batch: RecordBatch, pos: usize }` so an index cannot outlive its buffer — that also lets you kill the two `compute::take` calls **per row per column** at `:1559-1563`, which make this path quadratic regardless. Add a spill test parameterised over SF so runs exceed `MERGE_BUFFER_ROWS`.

---

**F6. Spill directories are a fixed shared path, and leak on every error**
*Severity: high · Confidence: CONFIRMED · Cost: small*

**Evidence.** `spill_path = temp_dir().join("query_engine_spill")` (`memory.rs:323`) — no pid, no uuid. `SPILL_COUNTER` is a **process-local** `AtomicU64` starting at 0 (`spillable.rs:34`). Directory names are `join_0_{id}` / `agg_{p}_{id}` / `sort_{p}_{id}`; file names inside are `build_{i}.parquet`, `run_{n}.parquet`. Observed on this box: `/tmp/query_engine_spill/` contains exactly `join_0_0` and `sort_0_0` — the names *every* process produces for its first spill.

Cleanup is a single `remove_dir_all` per operator on the success path only (`spillable.rs:461, 1008, 1252`). Every `?` in between leaks; a panic leaks unconditionally. Observed: **3.1 GB** left behind by two crashed runs.

**Failure mode.** Two `query_engine` processes — **the 8×4 configuration that measured +46% throughput** — write into the same `/tmp/query_engine_spill/join_0_0/build_7.parquet` and read each other's rows. Silently wrong join output or a parquet decode error, depending on interleaving. One process's `remove_dir_all` deletes the other's in-flight spill.

**Fix.** `spill_path = temp_dir().join(format!("query_engine_spill_{}_{}", process::id(), rand_u64))`, held in an RAII guard whose `Drop` removes it. **Implementation caveat:** `ExecutionConfig` is `#[derive(Clone)]` and is cloned into the planner and every operator, so a guard stored by value would be dropped by the first clone that dies, deleting the live spill root mid-query. It must be `Arc<SpillDirGuard>`, or a local guard created inside each operator's `execute`. Sequence this *after* F13 (lazy output), or a Drop-on-scope-exit guard will delete the directory before a lazy stream finishes reading it.

---

**F7. `INTERSECT ALL` / `EXCEPT ALL` are lowered to Semi/Anti joins — set semantics where SQL requires multiset**
*Severity: high · Confidence: CONFIRMED (measured vs DuckDB) · Cost: small (reject) / medium (implement)*

**Evidence.** `src/planner/binder.rs` — the Intersect arm builds a `JoinType::Semi` on all columns, the Except arm a `JoinType::Anti`, and in **both** arms the `all` flag is consumed only by `if !all { wrap in Distinct }`. The lowering itself is set-semantics regardless.

Measured against DuckDB reading the same `data/tpch-1mb/nation.parquet`:

| Query | Engine | DuckDB |
|---|---|---|
| `... n_regionkey=0 INTERSECT ALL ... n_nationkey=0` | **5** | 1 |
| `... n_regionkey=0 EXCEPT ALL ... n_nationkey=0` | **0** | 4 |

The non-ALL forms are correct only because the wrapping `Distinct` happens to repair them.

**Fix.** Immediate: reject `INTERSECT ALL`/`EXCEPT ALL` with `QueryError::NotImplemented` naming the construct — a few lines, and strictly better than a wrong answer; it matches the project's own stated rule at `src/planner/vector_types.rs:1-18` ("fail loudly, naming the column, rather than coerce"). Proper: the standard COUNT-based rewrite (aggregate both sides on all columns, INNER/LEFT join, emit `LEAST(lc,rc)` / `GREATEST(lc-COALESCE(rc,0),0)` copies), behind a dedicated `LogicalPlan::SetOp` node carrying the multiset flag so the physical planner cannot lower it as a plain join by accident. **The rewrite must itself call `require_scalar_row`** — it groups on all columns and would otherwise become a sixth construct bypassing the four existing guards.

---

**F8. Set operations never check operand arity or type — `zip` silently truncates**
*Severity: medium · Confidence: CONFIRMED (measured) · Cost: small*

**Evidence.** The Union arm takes `left_plan.schema()` with no comparison to the right. Both the Intersect and Except arms build join keys with `left_schema.fields().iter().zip(right_schema.fields().iter())` — `zip` stops at the shorter side, yielding a *narrower* join instead of an error.

```
SELECT n_nationkey, n_name FROM nation WHERE n_nationkey < 3
EXCEPT SELECT n_regionkey FROM nation
  -> engine: 0 rows, NO ERROR
  -> DuckDB: Binder Error: Set operations can only apply to expressions
             with the same number of result columns
```
The reverse direction fails at *execution* with `Column not found: n_nationkey`; a type mismatch fails deep in Arrow with `expected Utf8 but found Int64`. All three are bind-time errors surfacing at runtime or not at all.

**Fix.** One `check_set_op_compatible(op, left, right)` helper called from all three arms before any plan is constructed; replace both `zip` sites with an index loop. Best home is inside the `LogicalPlan::validate()` of F16 as a set-op invariant, so it is enforced at every rule boundary rather than only where the binder remembers.

---

**F9. `ParquetTable` infers the table schema from `files[0]` and applies table-level projection indices per file — measured silent column swap**
*Severity: high · Confidence: CONFIRMED (reproduced) · Cost: small*

**Evidence.** `src/storage/parquet.rs:70-72`: `let schema = Self::read_schema(&files[0])?;` with no cross-file validation. Each file is then read with `ProjectionMask::roots(builder.parquet_schema(), indices)` (`:301-304, 338, 379, 422, 445, 619, 903, 981`) where `indices` are **table-schema** indices. `MemoryTableExec::execute` positionally re-labels (`scan.rs:283-287`).

Reproduced with two files written by pyarrow — part-0 `['a','b']`, part-1 `['b','a']`:
```
SELECT a, b FROM t -> (1,10)(2,20)(3,30)(40,4)(50,5)   -- last two SWAPPED, no warning
SELECT SUM(a) FROM t -> Error: Column not found: a      -- morsel path resolves by name
```

**Failure mode.** Any "directory = table" whose files were not all written in the same column order returns silently wrong answers (identical types) or an opaque error. Schema evolution — a column appended or dropped in later files — is unhandled. This is **shipped bug #9's shape**: column identity re-asserted by *position* across a boundary where only *name* is meaningful. TPC-H never trips it because one generator writes all 8 files.

**Fix.** Validate every file's schema in `try_new` and reject on mismatch (~15 lines, fails loud, fixes both symptoms). The per-file name→index map is the better end state and buys schema evolution, but must be applied at all seven `ProjectionMask::roots` sites plus `streaming_parquet_scan.rs` — bigger than it looks. This is a **prerequisite** for any Iceberg wiring, since Iceberg schema evolution is exactly per-file schema divergence.

---

**F10. Type-resolution failure silently becomes `DataType::Float64` — bug #9's mechanism, still live in five places**
*Severity: high · Confidence: CONFIRMED · Cost: small — BUT THE OBVIOUS FIX IS WRONG*

**Evidence.** `.unwrap_or(DataType::Float64)` on a resolution result at `src/physical/operators/morsel_agg.rs:173` and `:440`, `src/physical/morsel_agg.rs:2682` and `:3068`, `src/physical/operators/hash_agg.rs:1141`. The irony is exact: `morsel_agg.rs:169` calls the *new* `PlanSchema::from_qualified_arrow` added to fix bug #9, and `:173` discards its failure. The `schema.rs` comment documenting bug #9 names the failure mode verbatim — "an integer SUM inferred as Float64 finalizes to a Float64 scalar that the Int64 output builder then writes as NULL" — and the swallow was never removed.

**Do not "just add `?`".** I traced it: the binder maps `FunctionArgExpr::Wildcard` to `Expr::Wildcard`, so a `COUNT(*)` aggregate's input **is** `Expr::Wildcard`, and `Expr::data_type` returns `Err(Internal("Cannot determine type of wildcard"))` for that variant (`src/planner/logical_expr.rs:1589-1591`). These `unwrap_or`s are **load-bearing for COUNT(\*)**. Propagating with `?` as written would fail every query containing COUNT(*) — Q01, Q04, Q13, Q16, Q18, Q21, Q22 among them.

**Fix.** Make the wildcard case explicit instead of swallowing all errors:
```rust
match &a.input {
    Expr::Wildcard | Expr::QualifiedWildcard(_) => COUNT_PLACEHOLDER_TYPE,
    e => e.data_type(&plan_schema)?,
}
```
Better: resolve aggregate input types **once** during physical planning, store the resolved `DataType` in the physical aggregate descriptor, and have operators consume it rather than re-deriving from a schema they reconstruct. Failure becomes a plan-time `QueryError`. (Note: `hash_agg.rs:149`'s `_ => Float64` and `logical_expr.rs:1754/1767` are total functions over an *already-resolved* type — a different class. Audit separately; bundling them dilutes the argument.)

---

### Tier B — critical/high, confirmed, medium-to-large. These are the architecture.

---

**F11. ROOT CAUSE: partitioning is an advisory integer with no property, no requirement, and no enforcement pass**
*Severity: critical · Confidence: CONFIRMED · Cost: large · **Makes a class unrepresentable***

**Evidence.** `src/physical/plan.rs:16-33` declares exactly `schema`, `children`, `execute(partition)`, `output_partitions` (default 1), `name`. `grep -rn 'with_new_children|fn as_any' src/physical/` returns **nothing**, so no physical rewrite pass is writable; physical decisions are set inline on `pub` fields in the 1893-line planner (`planner.rs:1195` `hj.probe_runtime_filter = ...`).

Out-of-range partitions are silently empty everywhere: `scan.rs:257-268` filters `i % num_partitions == partition`; `hash_agg.rs:164-166`, `sort.rs:65-68`, `vector_search.rs:227-232` all return an empty stream. **A partition-count disagreement is never an error, always a wrong row count.**

Eleven hand-rolled fan-in sites. The probe-side selection predicate `if self.build_right || matches!(join_type, Right)` is copy-pasted **four** times (`hash_join.rs:1113-1116` and `:1240-1244`; `spillable.rs:255-260` and `:243-249`) — bug #8 was precisely those copies disagreeing. A **fifth** instance nobody has flagged: `DelimJoinExec` declares `output_partitions() == 1` (`delim_join.rs:144-146`) and then calls `self.left.execute(0)` / `self.right.execute(0)` on possibly multi-partitioned children.

The bug-#8 regression test (`hash_join.rs:3699-3755`) states the invariant **in a prose doc comment** and then asserts it for one operator instance. F1, F3 and F4 are the identical violation in three other operators.

**Fix.** Replace the bare integer with a properties/requirement pair plus a rewrite hook:
```rust
fn properties(&self) -> PlanProperties;                    // output_partitioning
fn required_input_distribution(&self) -> Vec<Distribution>; // Single | Unspecified | Hash(keys)
fn with_new_children(self: Arc<Self>, children: Vec<Arc<dyn PhysicalOperator>>)
    -> Result<Arc<dyn PhysicalOperator>>;
```
plus one `EnforceDistribution` pass inserting `CoalescePartitionsExec` (later `RepartitionExec`/`ExchangeExec`) wherever a child's partitioning does not satisfy the parent's requirement. Sort, HashAggregate, MorselAggregate, DelimJoin, VectorSearch, Limit and Semi/Anti joins declare `SinglePartition` and their `execute()` becomes a plain `child.execute(partition)`. All eleven fan-in loops and all four copies of the probe-side predicate are deleted. 18 impl sites (verified count), mechanical.

**Ship the cheap 80% first, this week:** `if partition >= self.output_partitions() { return Err(Internal(..)) }` at every `execute` entry. That alone converts the entire class from silent row loss into a loud failure, and it costs an afternoon.

**Sequencing hazard.** `HashJoinExec`'s unmatched-build-row emission fires on `done == self.output_partitions()` (`hash_join.rs:1189-1194`). Any change to a join's reported partition count **must** land with the driving-contract change, or LEFT/FULL silently degrade to INNER again — bug #1 all over.

---

**F12. `MemoryPool` has zero production callers — "memory-safe by default" is per-operator guessing against the full budget**
*Severity: critical · Confidence: CONFIRMED · Cost: medium (usable API) + large (route consumers) · **Makes a class unrepresentable***

**Evidence.** `grep -rn 'try_allocate|\.allocate\(' src/ tests/ benches/ examples/` excluding `src/execution/memory.rs` returns **ZERO**. Inside `memory.rs` the only call sites are the pool's own unit tests (`:486, 490, 494, 507`, all below `#[cfg(test)]`). `MemoryConsumer` (`memory.rs:194-209`) has **no implementors**.

The root cause is a type decision: `MemoryReservation<'a> { pool: &'a MemoryPool }` (`memory.rs:155`) borrows the pool, but every operator holds `Arc<MemoryPool>`. Storing a reservation would be self-referential, so **the API is literally unusable** and the accounting became decoration.

Consequence: `metrics.peak_memory_bytes = self.memory_pool.used()` (`context.rs:364`) is permanently 0 — and `context.rs:464-467` suppresses the line when it is 0, so the breakage is invisible rather than obviously wrong. Measured: a 64 MB-limited `SELECT DISTINCT l_orderkey, l_partkey FROM lineitem` on `data/tpch-10gb` reported `peak_memory_bytes 0` against **VmHWM 1249 MB — 19x the configured budget**.

Actual admission control is seven independent copies of `(memory_limit as f64 * spill_threshold) as usize` (verified at `spillable.rs:272, 479, 732, 902, 1044, 1191, 1272`), each comparing one operator's local tally against the **entire** budget. A join under an aggregate under a sort permits 3 × 0.8 × limit before anything spills; N concurrent queries multiply that again (`sql(&self)` at `context.rs:279` has no admission control of any kind).

**Fix, in order:**
1. Drop the lifetime: `MemoryReservation { pool: Arc<MemoryPool>, size }`. Mechanical — only tests construct reservations today.
2. Introduce a `BatchBuffer`/`ReservedBatches` newtype owning `Vec<RecordBatch>` + a reservation, whose `push` returns `Err(OutOfBudget)` when the pool refuses. Replace every bare `Vec<RecordBatch>` accumulator in `src/physical/operators/`. Add clippy `disallowed_types` so a raw accumulator cannot be reintroduced. **This is what turns the project's first rule from a comment into a type.**
3. Fix `peak_memory_bytes` to report the pool high-water mark, and add a debug-mode invariant test that `pool.used()` returns to its pre-query value after every query in the suite. *This is the cheapest item and the one that makes the class unrepresentable.*
4. Only then replace the seven `memory_limit * spill_threshold` sites with `pool.available()`. **Land this behind a measured A/B** — once operator N's reservation is visible, operator N+1 starts spilling on workloads that fit today, so SF=10 timings and the spill_tests baselines both move.

---

**F13. Morsel aggregation — the default path for the default format — has no memory accounting and cannot spill**
*Severity: critical · Confidence: CONFIRMED · Cost: medium*

**Evidence.** `planner.rs:1258-1266` builds `MorselAggregateExec::new(files, input_schema, projection, filter, group_by, aggregates, schema)` — no pool, no config — and the struct (`operators/morsel_agg.rs:33-50`) has no such fields. `grep -cnE 'memory|spill|MemoryPool|limit'` returns **0** for both `src/physical/operators/morsel_agg.rs` and the 3218-line `src/physical/morsel_agg.rs` (verified).

It is reached only when `provider.parquet_files()` is `Some` (`planner.rs:127,137`) and is **on by default** (`enable_morsel_execution: true`, `memory.rs:330`). Contrast `SpillableHashAggregateExec::execute_fused_streaming`, which drives the *same* aggregation core but computes `group_limit = ((memory_threshold / n_workers) / per_group_bytes).max(64)` first (`spillable.rs:728-733`).

**Failure mode.** A high-cardinality `GROUP BY` over a Parquet table (group by `l_orderkey` at SF=100 ≈ 150M groups) builds unbounded per-thread hash tables and OOM-kills the process, **ignoring `--memory-limit` entirely**. The identical query over Lance or a MemoryTable takes the spillable path and survives. Being Parquet buys you fast aggregation *and removes your memory safety* — a direct violation of the project's hard rule.

**Fix.** Do **not** do the naive "add a fallback": `MorselAggregateExec` has no children (`children() -> vec![]`) and reads Parquet directly, so a runtime abort needs the planner to build the spillable sub-plan eagerly and hand it in. The right shape is to **delete the duplicate operator** and make the morsel core a *strategy* selected inside `SpillableHashAggregateExec` when the input is a re-readable source — which the fused-streaming path already proves is workable — so the budget check sits on the path that cannot be bypassed.

---

**F14. `LogicalPlan` caches a schema in every node and `with_new_children` copies the stale one — and 6 of 22 shipping TPC-H plans already violate it**
*Severity: high · Confidence: CONFIRMED (measured divergence) · Cost: large · **Makes a class unrepresentable***

**Evidence.** `src/planner/logical_plan.rs:83-101` returns a **stored** schema for Scan/Project/Join/Aggregate/Union/SubqueryAlias/etc.; `with_new_children` (`:123-176`) installs new children but writes `schema: node.schema.clone()` at five sites (verified). Nine rules rebuild nodes through that function, and some deliberately reshape children (`eager_aggregation.rs:96-111` inserts an Aggregate under a join). There is **no validator**: `grep "fn validate|validate_plan" src/planner/ src/optimizer/` returns nothing.

This is not theoretical. Walking the fully-optimized plans and comparing each Join's stored schema against the schema derived from its actual children:

| Query | Join type | stored | derived |
|---|---|---|---|
| Q02 | Left | 30 | 21 |
| Q04 | Semi | 9 | 2 |
| Q10 | Inner | 33 / 17 | 20 / 10 |
| Q17 | Left | 27 | 19 |
| Q20 | Inner | 17 | 4 |
| Q21 | Semi | 36 | 25 |

Six of the twenty-two cell-exact-vs-DuckDB queries ship a plan whose logical schema is a claim the data does not satisfy. They produce correct results because downstream consumers happen to use the children's physical Arrow schemas rather than the stale logical claim. **Correctness here is incidental, not enforced** — which is precisely the point.

**Failure mode.** Any rule that changes a subtree's column set produces a parent whose `schema()` lies. Downstream rules resolve against the lie. This is the shape of shipped bugs #8 and #9, and every future reshaping rule re-enters the trap silently.

**Fix, and note the mis-costing in the obvious plan.** Landing `validate()` with a `stored == derived` assertion as a cheap down-payment **does not work** — that assertion fires on 6 of 22 queries today. The realistic order is:
1. The *other* three invariants first, which are independent and genuinely cheap: every `Expr::Column` resolves in the node's input; for `Join`, each `on` pair's left expr resolves **only** in the left input and the right **only** in the right (**this single check makes shipped bug #7 unrepresentable**); `Union` inputs equal in arity and type (subsumes F8).
2. Then implement `derived_schema()` and make `schema()` delegate to it. Must special-case Semi/Anti (left-only) and be careful about column **order** for Right/Full and for build/probe swaps — a naive `merge(left,right)` is wrong for the join types this engine already swaps.
3. Then the equality assertion becomes trivially true and can be enabled under `cfg(debug_assertions)` after every rule.

---

**F15. The spillable operators fully materialize their input *before* deciding to spill**
*Severity: critical · Confidence: CONFIRMED (measured) · Cost: very large*

**Evidence.** Identical collect-then-compare shape at all three sites: join build `spillable.rs:271-276`, aggregate fallback `:900-904`, external sort `:1189-1193` (`let (all_batches, total_size) = collect_input_partitions_concurrently(...).await?; let exceeded = total_size > memory_threshold;` — the whole input is resident before `exceeded` can be computed). The join spill path *also* fully collects the probe side (`:417-427`).

The one genuinely streaming path, `execute_fused_streaming`, is gated on `!group_by.is_empty() && !aggregates.is_empty()` (`:684-696`) — so **global aggregates, DISTINCT and UNION** (group-by-every-column, zero aggregates) and every DISTINCT/APPROX_PERCENTILE aggregate take the materializing path. `SELECT DISTINCT * FROM <bigger-than-RAM table>` is an unconditional OOM. Measured 1249 MB against a 64 MB limit.

CLAUDE.md documents this hole **for the join build side only**; the sort, the probe side and the output paths are undocumented.

**Fix.** This is the Phase-5 ROADMAP item and it is the single highest-value work for the stated invariant. `build_with_partitioning` (`:466`), `aggregate_with_spilling` (`:1031`) and `generate_runs` (`:1263`) already take a `RecordBatchStream` and already spill incrementally — feed them the live input stream instead of `stream::iter(already_collected)`. Depends on F12's reservation type. **Preserve `collect_input_partitions_concurrently`'s concurrency**: it exists specifically to keep a parallel scan/join beneath a pipeline breaker from serializing onto one core (comments at `:268-270`), so a naive sequential stream rewrite will regress SF=10.

---

**F16. Every spill path re-materializes its entire *output* in memory**
*Severity: high · Confidence: CONFIRMED · Cost: medium*

**Evidence.** Join: `all_results` accumulates every matched batch across all 64 partitions plus all spilled ones (`spillable.rs:449-458`) before `stream::iter` at `:463`. Aggregate: same at `:982-1006`. Sort: `merge_runs -> Result<Vec<RecordBatch>>` (`:1323`), everything pushed into `result_batches` and wrapped at `:1254`. The comment at `:1430-1431` claims "this is memory-safe."

**Failure mode.** An external sort of a 500 GB table writes bounded runs to disk and then holds the entire 500 GB sorted result in RAM before returning the first row. For sort (output == input) the spill machinery reduces peak memory by **nothing**.

There is also no backpressure anywhere: 18 `stream::iter(collected_vec)` sites across the operator set mean a fast producer cannot be slowed by a slow consumer — it has already run to completion by the time the stream is returned.

**Fix.** `stream::unfold` over the partition index for join/aggregate; for sort, make the merge a real `Stream` — same rewrite as F5's `RunCursor`, so do them as one change. Watch the interaction with F6: lazy output must not read a spill directory that a scope-exit guard has already deleted.

---

**F17. Capability gates keyed on provider identity (`parquet_files()`) instead of declared capabilities**
*Severity: high · Confidence: PARTIALLY CONFIRMED · Cost: medium · **Makes a class unrepresentable***

**Evidence.** One `Option<Vec<PathBuf>>` accessor (`scan.rs:66-70`) is the boolean proxy for four unrelated capabilities: morsel eligibility (`planner.rs:127,137`), the >400MB size test (`:881-890`), streaming scan construction — **the only runtime-filter registration site** (`:906`) — and the prescan memory cap (`:446`). The comment at `planner.rs:441-445` documents that this exact shape had **already produced a real memory bug**: "Keying this off `parquet_files()` alone left every non-Parquet provider exempt from the cap." That instance was patched; the pattern was not.

Contrast the correct pattern one screen away: `scan_knn` returns `Ok(None)` meaning "not supported → caller MUST fall back to the always-correct path" (`scan.rs:72-92`).

**Partial:** the claim that `planner.rs:888` is a live unfixed bug does not hold — no non-Parquet streaming exec exists, so a Lance table could not stream regardless of that computation. It is the same shape with zero incremental consequence today.

**Failure mode.** Adding a storage format silently loses morsel aggregation, streaming scans, runtime-filter bitmaps and row-group pruning, with no compile error and no test failure — and historically, silently loses a memory cap. Every future fast path is one more `if provider.parquet_files().is_some()` a new provider must discover by profiling.

**Fix.** Delete `parquet_files()` from the trait. Add `fn scan_source(&self) -> Option<ScanSource>` returning an enum so consumers must **match** rather than test-for-`Some`, plus `fn capabilities(&self) -> ProviderCapabilities` — `#[non_exhaustive]`, `Default`-all-false. Make each fast operator's constructor **require** the capability value as an argument, so a fast path is unbuildable without proof of capability. Route every engine *invariant* (memory cap, spill, byte sizing) through capability-neutral methods so no provider can be exempted from safety by omission. Note this is a breaking change to public trait API (`context.rs:186-187`).

---

**F18. CPU-bound and blocking work runs on tokio worker threads across three runtimes plus rayon — 203 OS threads measured for one query on 32 CPUs**
*Severity: high · Confidence: CONFIRMED (measured) · Cost: large*

**Evidence.** Re-measured by sampling `/proc/<pid>/task`: **203 threads** peak during `benchmark-parquet` on `data/tpch-10gb`. Sources: the `#[tokio::main]` runtime (`main.rs:268`); a **second** `new_multi_thread().worker_threads(num_cpus)` runtime in `subquery.rs:21-31`; a **third** in `storage/lance.rs:66-77`; the global rayon pool at every CPU (`topology.rs:392-401`); `std::thread::spawn` per worker per fused aggregate (`spillable.rs:748`); and one `tokio::task::spawn_blocking` **per batch** (`spillable.rs:789-791`).

Blocking-in-async: `MorselAggregateExec::execute` is `async fn` but runs `(0..num_threads).into_par_iter()` inline (`operators/morsel_agg.rs:198-201`), holding a tokio worker for the whole scan+aggregate. `run_subquery_blocking` does `std::thread::spawn(...).join()` from inside `FilterExec`'s async body, **once per cache-missing row** (`subquery.rs:753-756, 798-802`).

**Failure mode.** ~6x oversubscription, so the P-cores `topology.rs` carefully ranks are contended by threads it does not know about — a plausible contributor to the measured serial fraction. Under concurrency, one query's morsel aggregate occupies all 32 workers and another makes zero progress *including its I/O*. **For the distributed design this is disqualifying**: the Arrow-IPC-over-hyper transport shares that runtime, so a blocked worker pool means stalled RPCs, missed heartbeats and spurious fragment timeouts.

**Fix, in value order.** (a) Wrap every synchronous rayon region in `spawn_blocking` so no `execute` future blocks its poller. (b) Batch correlated-subquery evaluation to one execution per **distinct** correlation tuple per batch instead of per row — already half-built, since `extract_correlation_values` + `correlated_cache` compute exactly that key. (c) Replace per-batch `spawn_blocking` with one long-lived sender task per partition. **Do not** naively delete the subquery runtime (item (2) in the naive plan): it exists because subqueries are evaluated inside *synchronous* expression evaluation, so removing it means making `evaluate_expr` async — a very large refactor with real single-node perf risk.

---

**F19. No cancellation anywhere**
*Severity: medium · Confidence: CONFIRMED · Cost: medium*

**Evidence.** `grep -rn 'CancellationToken|\.abort()|JoinSet|select!|timeout('` over `src/` returns only reqwest options in `metastore/mod.rs`. Bare `tokio::spawn` at `spillable.rs:66, 781` — dropping the awaiting future does **not** stop those tasks. `.join()` calls are uninterruptible; rayon regions have no abort check.

**Failure mode.** A client disconnect, a timeout, or a dropped `sql()` future leaves the entire scan/join/aggregate running to completion, holding its full (unaccounted) memory and its spill files. Under memory pressure the operator you most need to stop is the one you cannot stop. The project's own benchmark rule ("if the query cannot complete within 10x DuckDB time it FAILS") has no in-process mechanism.

**Fix.** Cheapest meaningful step: `JoinSet` in `collect_input_partitions_concurrently` plus an `AtomicBool` consulted by `ParallelParquetSource::get_work` — the single choke point every morsel worker passes through.

---

**F20. Only Arrow input bytes are counted — hash tables, accumulators and sort indices are invisible**
*Severity: high · Confidence: CONFIRMED · Cost: medium*

**Evidence.** `estimate_batch_size` is exactly `batch.get_array_memory_size()` (`spillable.rs:1671-1674`) and is the sole input to every spill decision. The dominant allocation is never measured: `JoinKey { values: Vec<JoinValue> }` (`:1990-1992`) heap-allocates a `Vec` per key and an owned `String` per string column. Hash tables for all 64 in-memory partitions are built up-front (`:405-415`) on top of the batches they index. The fused aggregate's `per_group_bytes = 64 + 48 * aggregates.len()` (`:731`) is a hand-rolled guess re-checked every 16 batches.

**Failure mode.** A build side reported as "800 MB, fits the 1 GB budget" builds a multi-GB hash table and OOMs while the accounting says it is comfortably under. The error is systematically in the unsafe direction and grows with key width and string cardinality.

**Fix.** Have `build_hash_table` return its own byte estimate and add it to the tally before the threshold test (small, local). The better answer — reuse the existing RowStore/packed-u64 keys in the spill path so a key's size is `size_of` and not a guess — is effectively part of F15.

---

### Tier C — real but lower leverage

- **F21. Optimizer never proves a fixpoint.** `max_iterations: 10` (`optimizer/mod.rs:75`) with convergence detected by `format!("{:?}")` string comparison (`:154`). `DeriveOrPredicates` × `PredicatePushdown` is a monotone growth cycle: each rule is idempotent *alone*, the pair is not, and nothing checks the composed property. **Q07 and Q19 ship non-fixpoint plans** (Q09 does *not* — see §6). Q19's `part` scan filter carries ~10 copies of `p_container IN (12 values)`. Semantically harmless (`A AND A ≡ A`), so this is a robustness/perf defect, **medium not high**. Fix: dedup derived conjuncts against the *subtree* (child Filter predicates + `ScanNode.filter`), not just the local list. **Hazard:** making non-convergence a hard error *before* the cycle fix would fail Q07 and Q19 outright — a 2/22 benchmark regression. Cycle fix first, verify all 22, then enable the error.
- **F22. Two duplicated 400MB thresholds** (`planner.rs:434` and `:888`), neither reading `config.memory_limit`. The **prescan** half is a genuine memory-safety gap (a 399MB table is decoded outside the pool under a 64MB limit) and should be derived from the budget. The **other** half is an annotated *measured* choice (Q16's part scan lost 180ms as a filtered stream, `planner.rs:876-880`) — do **not** fold them into one constant; rename it `STREAMING_SCAN_MIN_BYTES` with the measurement in the comment so they stop looking like duplicates.
- **F23. Memory limit is derived from the data's scale factor, not the machine.** `main.rs:516-520`: `((sf * 4.0).max(1.0) as usize * 1GB).min(64GB)`, with `sf` inferred from *substrings of the path*. The budget scales **up** with input size, which is backwards for a safety limit. Nothing reads `/proc/meminfo` or cgroup limits. Currently latent because the pool is dead (F12).
- **F24. `SubqueryExecutor`'s uncorrelated cache is unbounded** (`subquery.rs:283-375`, no cap, unlike its sibling's `MAX_CORRELATED_CACHE_SIZE`), keyed by `format!("{:?}", plan)` hashed **per row** (`:377-383`). Skip the insert for substituted plans — those keys are provably single-use.
- **F25. `peak_memory_bytes` and `SpillMetrics` are wrong.** `spilled()` is cumulative for the context's life with no reset; four of five `SpillMetrics` fields are always zero. Two-line snapshot-and-delta fix. (This does **not** weaken the spill tests — see §6.)
- **F26. Iceberg — the project's namesake — is unreachable dead code.** 630 lines in `src/physical/operators/iceberg.rs`; `IcebergScanExec` is never constructed anywhere. There is no `impl TableProvider for IcebergTable`, yet `context.rs:186` and CLAUDE.md both advertise one. Decide: delete and correct the docs, or reshape as a provider — and note F9 is a hard prerequisite for the latter.
- **F27. `BranchingMetastoreClient::get_schema` builds and destroys a whole multi-thread runtime per call** inside `block_in_place` (`metastore/mod.rs:450-454`). Cold path today; trivial fix.
- **F28. `HashAggregateExec` has no Utf8 offset-overflow guard** where `SortExec` does (`sort.rs:85`, `:186`); a high-cardinality string group-by can exceed the i32 offset limit in the single concatenated output batch. Port `check_string_overflow_risk`/`promote_utf8`. Arrow error, not corruption — but cheap to fix.

---

### Deliberate, well-reasoned trade-offs — NOT defects

Listed so they are not re-litigated:

- **Non-INNER joins and ON-filters fail loudly in the spill path** (`spillable.rs:352-382`), with a test asserting the error text (`spill_tests.rs:247-263`). This is the in-repo template for everything else.
- **`scan_knn`'s `Ok(None)` contract** and exact-by-default vector search (`scan.rs:72-92`, `lance.rs:1440-1458`).
- **Lance's partial predicate push** — only renderable conjuncts pushed, planner always re-applies the full predicate, so over-approximation is free and under-approximation impossible (`lance.rs:1391-1436`).
- **Small filtered Parquet scans stay eager** because streaming measured 180ms slower on Q16 (`planner.rs:876-880`).
- **The Semi/Anti `output_partitions() == 1` funnel** (`hash_join.rs:1226-1237`) — documented, correct-by-construction, and its comment names bug #8's exact failure mode. A ceiling, not a bug.
- **The nested-type row guard** (`vector_types.rs:182-188`) — its doc comment names shipped bug #4's failure path verbatim and explains why per-expression guards structurally cannot catch it. Correct diagnosis, correct placement. *One improvement:* move the call into the Distinct/Union arms of `create_physical_planning` (`planner.rs:1410, 1437`) where the group-by-all-columns is actually created, so it is a property of the lowering rather than of four hand-written binder call sites.
- **Lance's stats probe failing soft** ("Statistics are an optimization, not a correctness input", `lance.rs:512-517`), the arrow-53 pin, and the DPsize `n <= 12` cap.
- **The optimizer rule ordering rationale** in `optimizer/mod.rs:44-76`, including the Q17 cross-join-explosion reason.

---

## 3. SPECIAL SECTION — why this class keeps escaping

Ten bugs shipped. Four more of the same shape were found this review. This is not ten mistakes; it is one arrangement producing bugs on a schedule.

### The root-cause pattern, in three composing conditions

**(1) Every semantic obligation is a convention between physically separated sites, recorded in prose at one of them.**

- "The parent must drive exactly `output_partitions()` partitions, and for a join that count is the *probe* side's, which is not always `self.right`" — stated in a comment at `hash_join.rs:1226-1237` and in a doc comment on one regression test. Violated *right now* in `union.rs`, `limit.rs`, `subquery.rs` and `delim_join.rs`.
- "Projection indices are indices into `provider.schema()`" — stated nowhere, violated by `ParquetTable` per file (F9).
- "Operators must respect the memory limit" — stated in CLAUDE.md, implemented as seven copies of a multiplication (F12).
- "Row indices in `output_rows` refer to the buffers in `run_buffers`" — stated nowhere, violated on every refill (F5).
- "`stored_schema == derived_schema`" — stated nowhere, violated in 6 of 22 shipping plans (F14).

Rust can carry every one of these in a type. None of them do.

**(2) Where information is missing, the code *defaults* instead of *erroring* — and always in the unsafe direction.**

- `output_partitions()` defaults to **1** — silently correct for a pipeline breaker, silently catastrophic for a forwarding operator, and the compiler cannot tell them apart.
- `execute(out_of_range_partition)` returns an **empty stream** in every implementation (`scan.rs:257-268`, `hash_agg.rs:164`, `sort.rs:65`, `vector_search.rs:227`). A contract violation is therefore *never* a crash and *always* a wrong row count.
- Type resolution failure → `DataType::Float64` (F10).
- Missing join-key NDV → *the relation's row count*, i.e. perfectly distinct, the most optimistic value available.
- Missing relation row count → 10,000, so an unstatted 60M-row table is scheduled as the smallest input.
- `zip` on mismatched set-op arity → a narrower join instead of an error (F8).

Every one of these turns "I don't know" into a confident wrong answer.

**(3) The only oracle sits below every threshold the code branches on.**

This is the condition that makes (1) and (2) survive to production. `data/tpch-1mb`'s largest table is 6,000 rows in one row group = **one 8192-row batch**. The engine branches on:

| Gate | Threshold | Reachable at 1mb? |
|---|---|---|
| `MIN_BATCHES_FOR_PARALLEL` (`hash_join.rs:2353`) | 32 batches (~262k rows) | no |
| `PARALLEL_BUILD_THRESHOLD` (`:1328`) | 10,000 rows | no |
| probe parallel (`:2938`) | 10,000 rows | no |
| RowStore (`:946`) | 100,000 rows | no |
| parallel aggregate (`hash_agg.rs:195, 1212`) | 4 batches / 50k / 100k | no |

Bug #2 lived in the `probe_batches.len() >= 32` branch. Bug #4 needed DISTINCT-over-all-columns. Bug #5 needed an all-NULL group. **Every one of the ten bugs has a threshold below which the code is correct, and the fixture sits below all of them.**

And the data cannot express the semantics either. Measured with DuckDB: `SELECT count(*) FROM customer WHERE c_custkey NOT IN (SELECT o_custkey FROM orders)` = **0** at both 1mb and 10mb (21 at 100mb). `count(distinct o_comment)` = **1** at every scale — `generator.rs:452` is literally `o_comment.append_value("order comment")`. Consequences: Q13 is the only TPC-H query with an outer join, and with zero unmatched customers **its LEFT JOIN is extensionally identical to an INNER JOIN**, so bug #1 was undetectable by the flagship suite; and Q13's `NOT LIKE '%special%requests%'` is a tautology. The generator comment at `:439-441` asserts the opposite intent ("some real customers will have no orders (needed for Q22's NOT EXISTS)") — an intent that is never checked and does not hold.

Then: **four of twenty-two TPC-H expected-result files are empty** (q05, q20, q21, q22) and six more have one row. `duckdb_validated.rs:351-363` compares lengths then zips, so an empty expectation passes whenever the engine returns nothing, *for any reason*. `validated_tpch_q21` — the three-way self-join with EXISTS and NOT EXISTS — is green if SubqueryDecorrelation, the Semi/Anti probe, *or* the aggregate fails totally. The manifest even carries a `row_count` field (parsed at `:63`) that is **never asserted**.

Finally, the one harness that *does* cross those thresholds has no committed oracle: `scripts/safe_benchmark.sh` compares elapsed time and prints a row count, never a cell. `git ls-files .scratch` = **0 files**. The claim "all 22 cell-exact at SF=10 on both storage paths" rests entirely on a gitignored script and an agent's discipline, and is unreproducible from a clean clone.

### A fourth aggravating factor: semantics are re-implemented once per performance path

`hash_join.rs` has four independent probe implementations — `probe_inner_i64_parallel:1883`, `probe_semi_anti_parallel:1977`, `probe_vectorized:2308`, `probe_hash_table:2875` — dispatched on **input size** (`:2890-2948`), plus a second size fork inside `probe_vectorized` at `:2353`. Outer-join match-bit tracking is written separately in at least three. Aggregation is implemented independently in `hash_agg.rs`, `operators/morsel_agg.rs`, `physical/morsel_agg.rs`, `vectorized_agg.rs` and `spillable.rs`.

Bugs #2, #3, #5 and #8 are each *"one of N paths implements the semantic wrongly."* Because dispatch is a data-size threshold, **which semantics you get is a function of how much data you have** — a query can be correct in tests and wrong in production with identical SQL.

### Architectural changes that make these classes unrepresentable

Not "more tests." Six changes, ordered by leverage per unit cost.

**A. Make an out-of-range partition impossible to ignore, then impossible to express.**
Step 1 (an afternoon): `if partition >= self.output_partitions() { return Err(Internal(...)) }` at every `execute` entry. This converts the *entire* silent-row-loss class into a loud failure and would have caught bugs #2, #8, F1, F3, F4 on the day they were written.
Step 2 (large): replace the bare `usize` with a token the operator mints — `fn partitions(&self) -> Vec<PartitionRef>` and `execute(&self, p: PartitionRef)`. A parent then *cannot* iterate a range the child did not produce, and a join *cannot* forward an index to the wrong side's `execute`. This is the single change that structurally retires the two most expensive bugs on the list.
Step 3: `output_partitioning()` + `required_input_distribution()` + `with_new_children()` + one `EnforceDistribution` pass, which is also the prerequisite for the distributed work.

**B. Make the memory rule a type, not a comment.**
`MemoryReservation { pool: Arc<..> }` → a `BatchBuffer` newtype that owns `Vec<RecordBatch>` + a reservation and whose `push` can fail → replace every bare `Vec<RecordBatch>` accumulator → clippy `disallowed_types` so a raw accumulator cannot come back. After this, an operator *physically cannot* hold batches without charging them. Today the rule is prose; after this it is the borrow checker.

**C. Derive the schema instead of caching it, and validate the plan.**
`derived_schema()` + `LogicalPlan::validate()` asserting: stored == derived; every `Expr::Column` resolves in the node's input; join `on` left-exprs resolve *only* in left and right *only* in right; `Union` inputs equal in arity and type. Run after every rule under `cfg(debug_assertions)`. This turns bugs #1, #7, #8, #9 and F8 into panics at plan-build time. (Sequence per F14 — the equality assertion cannot land first.)

**D. Ban silent defaults on resolution results.**
No `unwrap_or` on a `data_type()` result, anywhere. Handle `Expr::Wildcard` explicitly. Resolve aggregate input types once at physical-planning time and store them. Replace the statistics `Option`s with an explicit `Estimate::{Known, Unknown}` that *propagates*, so "unknown" becomes a plan-visible fact a test can assert on per backend — rather than a 2x regression nobody notices. (Lower priority; see §6 on severity.)

**E. One cross-path conformance harness, replacing N per-path test suites.**
Add a test-only `ExecutionConfig::force_path` (ScalarReference | Vectorized | BatchParallel | RowStore | Spilling) that overrides every threshold; run all 175 manifest queries under every legal forcing with the reference path as oracle (no DuckDB needed — fast and hermetic); and **assert the forced path was actually taken** (`spill_tests.rs:133` already does this for spilling). This makes "one path disagrees with another" unrepresentable. Two caveats: RowStore has legality preconditions (Inner, no filter, `!swapped`, null-free fixed-width columns), so forcing must be "force when legal, else hard-error," not "force"; and BatchParallel cannot be forced at all without (F) below, since its gate is a batch *count*.

**F. One metamorphic runner, replacing the fixture problem.**
Plumb the **dead** `ExecutionConfig::batch_size` knob (declared `memory.rs:257`, defaulted `:325`, setter `:401`, and the **only** read in the entire crate is its own unit test at `:557`; every consumer hardcodes 8192) into the scans — the constructors already take a `batch_size` parameter, so this is threading a value, not new plumbing. Then run the whole manifest under {batch_size 1, 64, 8192} × {partitions 1, 3, 32} and require identical output. **At `batch_size=64`, `data/tpch-1mb` produces 94 lineitem batches and crosses the 32-batch join gate on a 396 KB dataset.** This single harness would have caught bugs #2 and #8 and F1/F2/F3/F4, and catches the next one free.

**G. Turn the existing corpus into two more oracles at near-zero marginal cost.**
- **Optimizer differential.** `ExecutionContext` currently *discards* `self.optimizer` and hardcodes `Optimizer::new()` whenever table statistics are non-empty (`context.rs:297-302`), so `Optimizer::with_rules` is unreachable for any real query. Make the rule list configurable and run every manifest query twice — full rules vs. a minimal correctness set — asserting identical results. Bugs #1 and #4 are lowering/rewrite defects an unoptimized reference would have caught instantly. *Caveat:* `JoinReorder` is not optional here — the binder emits comma-join cross products — so the "minimal" set must retain JoinReorder and the decorrelation rules, or Q05/Q08/Q09 attempt cartesian products.
- **Data adequacy as a build error.** Move the validated suite to SF=0.1 (generation measured at 227 ms; yields 74 lineitem batches and 21 unmatched customers), and add generation-time assertions that FAIL the build if a discriminator is absent: unmatched-left rows > 0, unmatched-right rows > 0, ≥1 NULL in each nullable column, distinct-count > 1 for every string column used in a LIKE, ≥1 group with all-NULL aggregate input. Add a non-vacuity gate to `generate_expected_results.py` refusing to write a CSV with <2 rows absent an explicit allowlist entry with a written justification.

**H. Kill the query-text forks.** The 22 TPC-H queries exist independently in `src/tpch/queries.rs`, `scripts/generate_expected_results.py`, and `scripts/sf100_validate_and_benchmark.py` — and the third already diverges (Q1 uses `DATE '1998-12-01' - INTERVAL '90' DAY` where the others use `DATE '1998-09-02'`). Nothing asserts they agree; they currently do, so the drift window is open but unused. **This is the precise mechanism by which a "simplified" benchmark query ships while the validated suite checks the original.** A ~20-line normalising test comparing manifest SQL to the `queries.rs` constants closes it in two hours. Delete the third copy in favour of `get_queries()`, as `duckdb_rebaseline.py:26` already does correctly.

**I. Promote the oracle into the repo.** `.scratch/validate22.py` → `scripts/validate_tpch.py`, parameterised by `--sf`/`--data-dir`/`--format`, reusing `get_queries()`. Wire it into `safe_benchmark.sh` so a benchmark run that produces wrong answers **fails** instead of reporting a fast time. Nightly at SF=1.

Only after all of the above is a randomized differential generator (SQLancer/SQLsmith style over the fixed TPC-H schema) worth building — and it must come last, because a generated LEFT JOIN over data with zero unmatched rows is indistinguishable from an INNER JOIN. Every earlier item makes the generator strictly more powerful.

---

## 4. Verdict on the distributed design

**The design is sound in mechanism and wrong about its own empirical foundation. Do not start implementation on the current numbers.**

What it gets right, and should be preserved: the hash-avalanche diagnosis (`combine_hash(seed, v) = seed*K + v` for a single Int64 key is *affine in the key* — the top bits are constant across the whole `l_orderkey` domain, so the defect is exact, not probabilistic); `replicate_nulls_and_any` for partitioned outer joins; the global Gate over per-channel bounded queues; the 256-bucket indirection; inproc-as-default; "the test must FAIL against the old hash"; `with_new_children` defaulting to `Err`; the hard type gate; and the honest §11 trade-off disclosures (refusing every approximation-for-scale knob, treating compression/broadcast crossover as link-bandwidth config rather than loopback constants, page-level-retry-only fault tolerance, worse small-query latency). Symmetric per-query initiator election is correct: `QPS(N) = N/(F0 + αN)` saturates at 1/α rather than growing linearly, but with α ~100 µs against a realized ~3 QPS that is four orders of magnitude away. The two O(splits) hazards are already handled — `ParquetTable::statistics()` memoizes behind a `OnceCell` (`parquet.rs:523-526`).

**Five things must change before implementation starts.**

**D1 — Section 0's `f = 0.150` is a pinning artifact, and it is what reorders the entire project.** `scripts/scaling_bench.py:57` pins with contiguous `taskset -c 0..c-1`. `lscpu -e` on this box: CPU0/CPU1 are SMT siblings of CORE 0; CPUs 0-15 are 8 P-cores at 5.5-5.8 GHz; CPUs 16-31 are 16 E-cores at 4.3 GHz. So the "cores" axis is: 1 thread → 1 physical core → 2 → 4 → 8 P-cores → 8 P-cores + 16 E-cores. Recomputing Amdahl per point gives f = 0.696 / 0.296 / 0.170 / 0.136 / 0.150 — **a 5x spread, which rejects the single-f model outright**. `src/execution/topology.rs:12-17` already documents this exact hazard. "Single-query speedup is capped at 6.7x forever" (`:35`) is not a measurement, and it is used to promote replica routing to P1 and to set the P2 threshold. **Re-run the sweep over homogeneous physical P-cores (`taskset -c 0,2,4,6,8,10,12,14`), report SMT and E-core contributions as separate deltas, and fit the USL the harness docstring already promises (`scaling_bench.py:22`) but never computes.** Then re-derive every threshold that quotes f.

**D2 — The memory invariant the whole cluster plan rests on does not exist.** §7 asserts "Every distributed allocation is charged to the existing MemoryPool" and §3.3 budgets `OwnedMemoryReservation` at ~1 day as "purely additive." Per F12, the pool has **zero** production callers. Adding the owned handle would make shuffle buffers the *only* class the pool sees — so the CreditController budget (§3.13) and the admission controller's `mem_free_bytes` floor (§7.5) would both be computed from a number that omits every hash table, sort run and join build side on the node. A budget precise about kilobytes of shuffle pages and blind to gigabytes of build state. Worse: **P1 targets 8 concurrent query processes per box** while today 8 admitted queries each contain several operators each independently entitled to the full limit. That is OOM-by-construction in a project whose first rule is OOM-never. **Item zero is "route existing consumers through the pool," not "add an owned handle." Do not ship P1's concurrency increase until `pool.available()` actually reflects sibling usage.**

**D3 — The initiator executes whole sub-plans locally during planning.** `precompute_uncorrelated_scalars` (`planner.rs:185-198`) and `materialize_shared_ctes` (`:494, :541`) both bottom out in `run_subquery_blocking` — blocking, fully materializing, outside all accounting — and both run inside `create_physical_plan`, which §2.3 places in the initiator's phase. Measured at SF=10 with `CTE_DEBUG=1`: **Q15 spends 152.6 ms of 175.6 ms (87%)** materializing `revenue` (100,000 rows); **Q11 spends 54.3 ms of 59.1 ms (92%)**. §6.3's "No O(rows) term — the initiator never routes a page" and §2.1's "never O(rows)" are both false for this query class. Once sharded, the initiator's local tables hold 1/N of the rows, so Q11's `SUM(ps_supplycost*ps_availqty)*0.0001` and Q15's `MAX(total_revenue)` would be computed over a fraction of the data and **frozen into the plan as a literal** — a wrong answer with no error and no EXPLAIN annotation. §4.6's hard planner gates cover only `SubqueryExecutor`'s row-by-row path and DelimJoin — and DelimJoin is **dead code**: `FlattenDependentJoin::optimize` returns `plan.clone()` (`flatten_dependent_join.rs:37-41`) despite being registered, and PLAN_DEBUG shows 0 DelimJoin nodes across all TPC-H. So §4.6 currently gates nothing that can occur, while the real plan-time-execution path is ungated. Note also that `run_subquery_blocking` calls `execute(0)` only — F4, sitting in this path today. **Fix: emit these sub-plans as real broadcast fragments (cheap — 1 row for Q11/Q22, 100k for Q15), or fail with `NotImplemented` and surface it in EXPLAIN. Add a fragmenter debug assertion so any future plan-time execution path trips it.**

**D4 — The paired-control experiment does not cancel the hardware term it claims to cancel.** §9.1 argues `eta_box` appears in numerator and denominator of τ and "cancels exactly." That holds only if both arms have the same *sensitivity* to heterogeneity, and they do not: in ARM S each query is served by one process (a slow process just contributes less throughput), while in ARM D every query is fanned out to all K and **finishes at the speed of the slowest shard**. The pinning is contiguous (`scaling_bench.py:107`), so at K=8, processes 0-3 land on P-cores and 4-7 on E-cores. Measured on the full 22-query mix: **CPUs 0-15 = 8.59/8.68/8.60 s vs CPUs 16-31 = 11.34/11.59/11.40 s — a 1.33x slow half.** A fan-out query completes at ~1.33T against a balanced ~1.17T, an ARM-D-only straggler penalty of ~1.14x, i.e. a hardware ceiling near 0.88 against a **0.85 budget**. The likely outcome is a failed P6 gate attributed to "the shuffle" — precisely the failure mode §0's own conclusion (3) was written to prevent. **Fix: stripe the pinning (`shards[i] = list(range(tc))[i::K]`) so every process gets an identical P/E blend and the term becomes common-mode; re-record `eta_box(K)` before quoting it as "the permanent control curve."**

**D5 — The design re-creates the exact defect class it is retiring.** §3.5 gives `output_partitioning()` a **default** of `Partitioning::Unknown(1)`, with `output_partitions()` provided-and-delegating; risk #26 rates the 18-site migration "Low" because "provided-method delegation makes divergence impossible afterwards" — true of the two functions *relative to each other*, false of either *relative to reality*. An impl that deletes its `output_partitions` override and adds nothing **compiles and silently reports 1**; `context.rs:325` then calls only `execute(0)`. That is shipped bug #8, verbatim, re-armed by a default — and the dangerous sites are exactly the ones with non-trivial logic (`hash_join.rs:1224-1246`, whose own comment spells out the hazard). §4.2's propagation table also has no Semi/Anti row and does not say which child a partitioned HashJoin passes through under `build_right` — the ambiguity that produced bugs #2 and #8. **Fix (free): make `output_partitioning()` a REQUIRED trait method with no default, so the compiler enumerates all 18 sites; add the Semi/Anti row and a `build_right` column to §4.2.**

**Additionally, and this is the highest leverage-per-line item in the entire plan:**

**D6 — There is no verification pass.** §3.4 states the join-vs-aggregate subset asymmetry and warns matches are "silently lost"; risk #4 rates it **Critical** and lists the mitigation as `satisfies(.., for_join)` "+ a targeted test." §4.1 applies `satisfies` while *inserting* exchanges and never re-checks the result. Grepping the whole 935-line document for "verif" outside the word VERIFIED returns only an appendix heading. Add `src/physical/optimizer/verify.rs` (~150 lines), run after EnforceDistribution and asserted under `debug_assert` everywhere: for each node/child, assert `satisfies(child.output_partitioning(), node.required_input_distribution()[i], is_join)`; assert that any node requiring `HashPartitioned` has **all** children carrying the identical `(keys, salt, buckets)` triple; assert the driven partition range matches the child's count for pass-through operators (scope "pass-through" via §4.2 or it will fire on joins). Because the P5 gate already runs the entire test suite under `--cluster-size 4 --transport inproc`, **a debug-mode verifier converts every existing test into a distribution-invariant test at zero marginal cost.** A targeted test covers cases someone thought of; a verifier covers cases nobody thought of, including plans produced by rules that do not exist yet.

**Two smaller specification fixes.** (a) §3.13's single global credit budget over P local consumers has the same *shape* as the per-channel deadlock §3.6 correctly diagnoses one layer down, and the oversized-page hatch (`queued == 0 && in_flight == 0`) will not fire when a blocked consumer pins the budget. Whether it is reachable depends on whether `ShuffleReadExec` exposes P independently-consumed outputs — which the doc never states — so **resolve that structural question first**; if it does, reserve a per-partition floor of one max page and share only the surplus. Also state the `O(concurrent_queries × stages × budget)` term; §7.2's "O(1) in N" is true in N and silent about concurrency, which is P1's whole premise. (b) §4.3 mandates remote width N with local repartitioning, while §4.4 gives `P = clamp(ceil(S/256KB), N, 8N)` with no remote/local qualifier and `PlanFragment` carries a single `bucket_map`. The `N` floor actively invites the wrong reading, under which per-node mailbox state becomes O(N²) — 8192 live `SinkBuffer`s with their own reservations at N=32. Name `P_remote` and `P_local` explicitly and add a width field to `PlanFragment`.

**One acceptance criterion is self-contradictory.** P0 demands both the fmix64 avalanche **and** `hash_arrays(k,n) == hash_arrays_salted(k,n,DEFAULT_SALT)` "so no existing hash table changes shape." These cannot both hold; the equality only proves the two *new* entry points agree with each other. Meanwhile `hash_i64` (`vectorized_hash.rs:102-104`) is a separate entry point whose doc comment claims it computes "exactly what `hash_arrays` computes for a one-column Int64 key" — an invariant maintained only by comment — and it probes the same chained buckets that `hash_arrays` built (`hash_join.rs:387` builds, `:544` probes). **Add the finalizer to one and not the other and every point lookup on that path silently misses, while P0's stated gate passes.** Replace the false clause with a permanent randomized property test asserting `hash_i64(v) == hash_arrays(&[Int64Array::from(vec![v])], 1)[0]` including negatives and `i64::MIN/MAX`.

**What would not deliver linear throughput, concretely:** (i) P2's gate is measured by a sweep that runs queries *sequentially*, so it is a single-query-latency gate wearing a throughput number's clothes; the four funnels it attacks are mostly already rayon-parallel underneath (only the terminal concat is serial), and one of them (DelimJoin) is unreachable dead code. Replace it with mechanism-level gates: assert `output_partitions() > 1` on the final aggregate and Semi/Anti joins; measure the terminal concat/merge share of Q01/Q13/Q18 before and after; keep "SF=10 strictly below 7.4s" but specify median-of-5 with the ±3% band P0 already uses. (ii) The tokio/rayon oversubscription of F18 (203 threads on 32 CPUs, three runtimes, blocking-in-async, `std::thread::spawn` per row in correlated subqueries) will manifest as stalled RPCs and spurious fragment timeouts the moment the transport shares that runtime — fix it *before* P3, not after. (iii) With no cancellation anywhere (F19), a failed fragment cannot cancel its peers.

---

## 5. Prioritized action plan

### Fix first — this week (all small, all confirmed, all silent wrong answers)

| # | Item | Why now |
|---|---|---|
| 1 | **Range guard at every `execute` entry** (`partition >= output_partitions()` → `Err`) | An afternoon. Converts the entire silent-row-loss class into a loud failure. Do this *before* the four operator fixes so they cannot regress. |
| 2 | F1 UnionExec, F3 LimitExec partitions, F4 CTE `execute(0)` | Three variants of the same bug, all reproduced, all small |
| 3 | F2 LimitExec per-batch counter | Reproduced; delete the `#[allow(unused_assignments)]` tell |
| 4 | F5 ExternalSort merge index reuse | Process abort or silent wrong rows in the code path the OOM-never rule forces every large sort through |
| 5 | F7 reject `INTERSECT ALL`/`EXCEPT ALL` (NotImplemented) | A few lines; measured wrong vs DuckDB today |
| 6 | F9 ParquetTable cross-file schema validation | ~15 lines, fails loud, kills a silent column swap |
| 7 | F6 per-process spill root + RAII guard (`Arc`, per the caveat) | Blocks the 8×4 deployment that measured +46% |
| 8 | F10 explicit `Expr::Wildcard` handling, delete the `unwrap_or(Float64)`s | Bug #9's live mechanism. **Do not just add `?`** |

### Then — the test architecture (weeks 2-3, before any new features)

| # | Item |
|---|---|
| 9 | **Plumb `ExecutionConfig::batch_size`** (dead knob) and add a CI matrix at `batch_size=64` — crosses the 32-batch join gate on a 396 KB dataset |
| 10 | **Add `data/tpch-100mb` as a second `duckdb_validated` fixture** — 31 MB, multi-partition, costs seconds. The cheapest item on this entire list |
| 11 | Metamorphic runner: full manifest × {batch_size 1,64,8192} × {partitions 1,3,32}, byte-identical output required |
| 12 | Non-vacuity gate in `generate_expected_results.py` + assert the dead `row_count` manifest field |
| 13 | Query-text fork test (~20 lines) + delete the third copy in `sf100_validate_and_benchmark.py` |
| 14 | Promote `.scratch/validate22.py` → `scripts/validate_tpch.py`, wire into `safe_benchmark.sh` so wrong answers **fail** |
| 15 | Move validated suite to SF=0.1 + generation-time data-adequacy assertions |
| 16 | Cross-path conformance harness (`force_path`), depends on #9 |
| 17 | Optimizer differential (make the rule list configurable at `context.rs:297`), retaining JoinReorder in the minimal set |

### Then — the structural work (quarter scale)

18. **F12 memory: `Arc`-owned reservations → `BatchBuffer` newtype → route every accumulator → clippy `disallowed_types` → fix `peak_memory_bytes`.** Item 4 of that sequence (`pool.available()`) behind a measured A/B.
19. **F13 fold `MorselAggregateExec` into `SpillableHashAggregateExec` as a strategy** so the budget check cannot be bypassed.
20. **F11 the properties/distribution refactor + EnforceDistribution**, sequenced with the join partition-count change so LEFT/FULL cannot silently degrade.
21. **F14 `derived_schema()`** — the three non-schema invariants first, then derivation, then the equality assertion.
22. **F15/F16 streaming spill** (Phase 5), preserving concurrent partition draining.
23. **F18 async hygiene** — items (a), (b), (c) only; leave the subquery runtime alone until `evaluate_expr` is asynchronous.

### Deliberately do NOT fix (with reasons)

- **The `output_partitions() == 1` funnels themselves.** They are correct-by-construction and documented. They are a *ceiling*, not a defect. Attack them via `Partitioning::Hash` in the F11 refactor, sold as a capability the distributed design needs — not as a bug fix.
- **F17's `planner.rs:888` "second unfixed instance."** No non-Parquet streaming exec exists, so the computation has zero consequence today. Fix the *pattern* (capability refactor) on its own schedule; don't chase this line.
- **The runtime-filter pointer map, as a correctness item.** The ABA hazard is refuted (see §6). The typed-sink change is a pluggability and readability improvement — schedule it behind F11/F12.
- **The statistics `Estimate::{Known,Unknown}` refactor, for now.** Optimistic defaults for missing stats are a standard choice, the plans are cell-exact on both backends today, and "penalise Unknown estimates" is a **plan-changing** proposal requiring full 22-query re-validation *and* a timing re-baseline on both storage paths. Real value, wrong quarter. Take the cheap half only: per-backend `estimates_all_known()` assertions and plan-shape golden tests, which turn a silent Lance/Parquet divergence into a red test without changing a single plan.
- **The randomized query generator.** Correct idea, must come **last**. Generated LEFT JOINs over data with zero unmatched rows cannot discriminate the bugs it exists to find. Items 9-17 each make it strictly more powerful.
- **`src/physical/operators/iceberg.rs`.** Don't wire it now — F9 is a hard prerequisite and the capability interface is not there yet. Either delete it and fix the two doc claims, or leave it and mark it clearly as unwired. Just stop advertising it as a working TableProvider.
- **`tests/tpch_queries.rs`.** It asserts only "did not error" (one assert in 119 lines, with the row-count check explicitly waived at `:53-54`) and inflates the headline test count — but it is currently the *only* thing exercising the in-memory generator path. Delete it only after item 15 lands.

---

## 6. Considered and dismissed

Reported so you know these were examined and are not lurking.

- **Runtime-filter pointer map ABA hazard → REFUTED on mechanism.** `RuntimeFilterConfig` is `Arc<Mutex<Option<...>>>` and **the map holds that Arc**. If an exec were dropped and its address recycled, the lookup returns an orphaned-but-alive config and the write goes nowhere — a lost optimization, not a dropped row. A collision with a *live* scan is impossible: a live operator occupies its own address, and if it had registered it would have overwritten the key. The planner is also constructed fresh per query. The pluggability half of that finding stands; the correctness half does not.
- **"Q09 never reaches a fixpoint" → REFUTED.** Measured `p1 == p2 == p3` at 30,121 bytes. Q09 *is* a fixpoint. Only **Q07 and Q19** ship non-fixpoint plans (2 of 22, not 3). The claimed "70-97% of planning time is the Debug-string comparison" and the SF=10 A/B timings are **UNVERIFIED** — do not quote them.
- **"P2's 8.0x gate is hardware-unachievable" → REFUTED.** The load-bearing measurement ("both halves have equal solo capacity, 7.62s vs 7.61s") is not reproducible: I measured 8.59-8.68 s (P-half) vs 11.34-11.59 s (E-half). And the design's own `proc_scaling.json` shows K=8 reaching 3.988/0.478 = **8.34x, efficiency 0.261 — above the 0.25 gate**. The gate should still be replaced with mechanism-level criteria (D-section), but *not* on the grounds that it is unreachable.
- **"Propagate the type-resolution error with `?`" → the FIX is refuted, the finding stands.** `COUNT(*)`'s input is `Expr::Wildcard`, whose `data_type()` returns `Err`. A naive `?` breaks Q01, Q04, Q13, Q16, Q18, Q21, Q22. See F10 for the correct form.
- **"`PlanSchema::from(&ArrowSchema)` at `hash_agg.rs:62` / `project.rs:57` is part of the silent path" → REFUTED.** Both use `?` and propagate. They can produce a spurious hard error — the safe direction.
- **"Cumulative `spilled()` weakens the spill tests" → REFUTED.** `spilling_ctx` builds a **fresh** `ExecutionContext` per call (`spill_tests.rs:42-52`) and each test runs exactly one query on it. The metric defect is real (F25); the test-weakening claim is not.
- **"No test file imports the `queries.rs` constants" → REFUTED.** `tests/tpch_queries.rs:16-19` imports Q1..Q22. It doesn't rescue anything — those tests assert only `is_ok()` — but the stated evidence was wrong.
- **"HashJoinExec's `OnceCell` per-run state turns LEFT into INNER on re-execution" → PARTIALLY CONFIRMED, latent only.** The contract hole is real (`build_matched`/`completed_partitions` live in per-operator memory with no reset, and `async fn execute(&self, _)` implies idempotence it does not have), but no path executes one Arc twice today. Its reachable consequence — under-driven partitions never let `done` reach N — is entirely subsumed by F1/F3/F4. Fix as a rider on F11, not on its own.
- **"The statistics model is a high-severity defect" → DOWNGRADED, and one citation was fabricated.** The finding attributed a "missing float NDV cost 2x on Lance; missing int stats made Q05 non-terminating" measurement to the brief. **The brief says no such thing.** The code facts (four divergent optimistic defaults, backend-asymmetric stat availability, name-string base-table resolution) are all real and verified; the impact claim is unverified and the severity is medium at most. See §5 for the cheap half worth taking.
- **"`MorselAggregateExec` emits one giant batch" → REFUTED.** It emits multiple (`morsel_agg.rs:283`). The single-batch claim holds only for `HashAggregateExec` (`:206`) and `SortExec` (`:110`). The Utf8 overflow sub-issue is confirmed (F28) but yields an Arrow error, not corruption.
- **"The validated suite reaches *no* vectorized path" → OVERSTATED.** `probe_vectorized` is **not** size-gated (`hash_join.rs:1016` builds the VHT whenever `!build_keys.is_empty()`; the dispatcher tries it first) and *is* exercised at 1mb. What is unreachable is its ≥32-batch sub-branch plus parallel build, the i64 parallel probe, RowStore, and the parallel aggregate paths — still the majority of the fast-path surface, so the finding stands on substance.
- **Two irreproducible figures from the source analyses**, corrected here so they don't propagate: the twice-referenced-CTE control on `data/tpch-1mb` was quoted as 11,098 rows — that predicate returns **zero** rows at SF=0.001 (MAX(l_orderkey)=1500). The corrected control is 3,902 = 2 × 1,951. And `require_scalar_row` has **four** call sites, not five.
