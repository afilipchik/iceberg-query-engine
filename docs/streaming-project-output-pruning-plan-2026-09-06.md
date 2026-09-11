# Streaming scan emission beneath a column Project — bounded proposal

Read-only source audit during the active matrix. No production changes, runtime work, or changes to the separate prepared-wrapper draft.

## Evidence and exact mechanism

Parent reports current Iceberg Q19:30validated/gate-passing samples, median2188.269ms, versus historical scalar455.106ms; current DuckDB about1553ms. These are unpaired historical snapshots, so the large regression is evidence requiring investigation rather than a proven causal fraction.

The recorded shape is Project -> Aggregate -> Filter -> SpillableHashJoin; the probe is Project -> StreamingParquetScan. Scan projection [1,4,5,6,13,14] emits numeric join/aggregate fields and string shipinstruct/shipmode fields. The immediate Project keeps only key/quantity/price/discount. This is a general filter-column liveness problem, not a query-specific missing operator.

Confirmed source chain:

- `physical/planner.rs:1682` lowers the Project input without supplying its needed output columns, then wraps ProjectExec.
- Scan lowering (`:1489`) gives StreamingParquetScan `node.projection.clone()` unchanged. That optimizer projection includes filter dependencies.
- `streaming_parquet_scan.rs:298–317` builds projected_schema and tries FixedWidthOutputLayout over ALL emitted columns. Any Utf8/Dictionary output prevents the fixed-width capability. Project's ordinary gather propagation cannot recover it because the child has no descriptor to transform.
- Raw decoder static filter_spec is constructed independently from provider-schema indices (`:229–256`). ArrowPredicateFn gets its own ProjectionMask for predicate columns; the reader final projection is separately configured (`:586–670`). Filter-only columns therefore need decoding but need not be emitted.
- IPC helper `ipc_read_work:798–837` constructs read_set as union(output roots, static-filter roots, runtime-filter roots), then filters and projects. Its semantics already separate read dependencies from emission. Nevertheless a scan with any eligible IPC sidecar still declines fixed_output; reducing emission alone does not certify that route.

Thus wrapper preparation propagation alone cannot prove the string-bearing scan's projected numeric output. Emitting fewer columns at the source can expose the existing fixed-width normalizer and general copy/gather capability for the raw reader. It also reduces discarded output materialization. This is source feasibility, not measured speedup.

## Smallest general fix: narrow physical emission, retain logical Project

Start only with an immediate Logical Project whose expressions are recursively Column/Alias(Column), directly over a Logical Scan. Do not move projections through joins, residual Filter nodes, subquery aliases, shared CTEs or arbitrary expressions in this increment. Keep ProjectExec, so aliases, order, duplicate columns and declared output schema remain runtime behavior.

Refactor the existing Scan match body into `lower_scan(node: &ScanNode, streaming_output_hint: Option<&[usize]>)`; ordinary scan calls passNone. The Project arm computes a validated hint and calls lower_scan on the ORIGINAL child ScanNode reference. Avoid cloning the logical ScanNode: spill_covered_scans is keyed by its pointer (`planner.rs:1430`), so a clone can lose its spill-routing proof. Avoid a mutable planner-global hint whose lifetime leaks into another branch on error.

The hint affects only a successfully selected StreamingParquetScan constructor. Existing eager/native/provider-scan branches and table-level scan_cache use node.projection unchanged. Preserve original shared-scan prescan unioning and cache identity; never store a narrowed one-consumer batch set in the table-name cache.

### Derive/validate the output hint

1. Build the exact ORIGINAL effective scan schema as current lowering does: logical node.schema transformed by node.projection. Original scan indices are provider/root positions; Project column resolution returns positions within this projected schema. These index domains must not be conflated.
2. Resolve every recursively unwrapped Column with shared find_column_index_in_schema. If resolution fails, decline the hint; let ordinary execution retain its error behavior.
3. Translate each resolved position back through node.projection (or identity mapping) to a provider/root index. Select a nonempty unique subset, preserving original scan emission order. The Project still duplicates/reorders/aliases as requested. Do not sort by name or assume column names uniquely identify provider ordinals.
4. Construct the proposed emitted logical schema from these root indices and rerun the SAME Project resolver for every expression. Map its result back to provider roots and require equality with the original resolution. This protects qualification/exact/suffix ambiguity and duplicate names from changing the selected column after pruning. If schema/root mapping is invalid, or a physical nested root cannot be mapped safely, decline.
5. Keep the declared Project schema unchanged. A column whose physical type later differs (e.g. dictionary) still follows existing Project runtime type reconciliation. A Null-to-declared-type projection remains computed allocation and may decline queue metadata even after pruning.

No alias rewrite is necessary: the scan keeps original logical names; the Project alone produces alias output names. Repeated references are materialized by Project exactly as today. Existing reorder_parquet_projection (`streaming_parquet_scan.rs:895`) restores requested root order/duplicates after Parquet's sorted unique ProjectionMask; retain it, even if the new hint itself is normally ordered/unique.

### Preserve routing and decoder correctness

Calculate ordinary filter_streams/shared-cache/routing eligibility using existing conditions first. Only in the chosen streaming branch apply the validated hint to emitted schema and reader projection. Recompute the fixed-width row quantum using the reduced emitted schema and existing configured batch_size/query budget for that branch; estimates remain routing inputs only. This avoids accidentally selecting a new eager/streaming path merely because a hint changed a costing heuristic.

Do not strip predicate columns from provider_schema, filter_spec, pruning or dict_schema. Predicate dictionary-coercion eligibility still sees the full static expression and provider schema. `coerce_back` must be derived for the NEW emitted projection so removed filter-only dictionary positions cannot accidentally cast an unrelated retained output column.

Runtime filters use provider-root indices, not emitted positions. Preserve RuntimeFilterConfig and `streaming_scans` registration at the real scan Arc using provider_schema; the existing Project-unwrapping link path must still find the scanner. Apply multiple runtime predicates even when their key columns are not emitted. Chain publication remains unchanged. Do not replace provider_schema with the reduced logical schema in the link registry.

The raw RowFilter must fully evaluate the original static predicate before omission. If `filter_streams` rejects subqueries/unresolved predicates, use the untouched fallback route. A remaining external Filter needs its columns and is deliberately outside immediate Project->Scan narrowing.

IPC remains explicit: use the existing unioned read_set and final projection for correctness, but keep fixed capabilityNone whenever IPC may serve output, unless a separate IPC extent normalization proof is implemented. Do not silently bypass a sidecar to force a favorable capability. Existing explicit small-batch/raw-reader admission routes may already exclude sidecars; record that route normally.

## Alternative and why defer it

A generic `projected_queue/gather_bound(indices)` provider capability could analyze just emitted columns while preserving a wider scan batch. But advertising a compact fixed bound for that subset requires normalizing the subset after Project, before any prepared join can trust its exposed buffers; the current scan-wide normalizer is absent when strings are present. This needs new projected-source lifecycle/layout APIs and still materializes discarded strings. Direct emission pruning uses existing decoder contracts and is the smaller first fix for the proven immediate pattern. It does not solve arbitrary multi-wrapper projection propagation or variable-width capability certification.

## Red/green regression specification

1. Small real Parquet with fixed id/Decimal128/date plus Utf8 filter-only column, multiple row groups and partitions, configured batch17 so no IPC shortcut. Build direct logical Project(aliases, duplicate/out-of-order fixed columns)->Scan with a static string predicate. Keep complete SQL-equivalent original predicate. Old planner emits Utf8 and scan general gatherNone; new plan emits only numeric fields and Project general queue/gatherSome, residentNone. Verify rows/duplicates/NULL decimals against an independent typed oracle and actual normalized copy charges, not engine-vs-engine only.
2. Include filter-only NULL strings, IN/LIKE and an expression unsafe for dictionary coercion; filter correctness and survivors must be identical. Assert string column still appears in decoder predicate projection while absent from output schema. Unsupported/subquery filter must retain old safe route.
3. Attach static plus two runtime filters, at least one on a root absent from emitted output. Exact row oracle and correct linking through retained Project. Verify original provider root indices survive reordered/duplicated original scan projection.
4. Duplicate/qualified/suffix names: validate root correspondence before and after narrowing; cases that would change resolution must decline the hint. Computed Project and empty output hint decline, without changing cardinality/error behavior.
5. Shared scan/CTE: two consumers require different columns; prescan cache must retain their existing union. Narrowing one consumer must not corrupt the other. Spill-covered pointer routing and configured small-budget reader quantum remain active.
6. Real sidecar fixture: narrowing preserves read_set predicate/runtime dependencies and correct output but does not falsely advertise raw fixed capabilities. No hidden route change.
7. Integration after the prepared-wrapper patch: aggregate -> residual Filter -> Inner -> Project -> streaming fixed subset. Prove preparationSome, no source read during preparation, same-pool initialization before envelope, overlapping subsequent pulls, exact outputs, bounded copied peak and cancellation. Without source narrowing the raw probe gather should decline despite Project retaining only numeric fields.

A logical planner fixture can force existing memory-pressure/small-batch streaming eligibility without downloading a >400MB file or changing production heuristics; preserve all budgets. Tests must not special-case Q19/table names. Compile/red/green and unchanged full gates belong to the parent after the active matrix/diagnostics.

## Limits and measurement

Output pruning does not admit decoder page buffers, string predicate scratch, source mappings or total RSS. The fixed-width capability covers normalized exposed output/copies and pool-independent future pulls only. It does not make varlen output fixed or permit estimates-as-bounds. No performance claim until same-binary controls and a fresh candidate compare original SQL/data, correct results and unchanged10×gate. Keep all modes and failures visible; Iceberg's current pass against DuckDB does not erase a large historical regression.
