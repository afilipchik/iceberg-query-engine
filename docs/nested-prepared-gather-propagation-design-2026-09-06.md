# Nested already-bounded Inner preparation

Source-only design, 2026-09-06. No coding, builds, tests or engine jobs. This is separate from variable-width runtime byte-quantum work. It may help nested fixed/raw or resident pipelines; no causal claim or predicted Q10 improvement is established by this audit.

## Confirmed metadata/lifecycle gap

`queue_layout.rs:498` currently stores `PreparedOutputLayouts { variants: Vec<QueueCopyBound> }`. `inner_output_copy_bound` (around 542–674) has the actual single cached build batch, its `GatherCopyBound`, the probe GatherCopyBound, retained masks and orientation. It computes physical output identity/copy variants but discards their repeated-take column metadata. Filter/Project preserve only those queue variants.

Both `HashJoinExec::prepare_queue_input` (`hash_join.rs:1486–1510`) and `SpillableHashJoinExec::prepare_queue_input` (`spillable.rs:1163–1174`) preflight only the **static** probe `pool_independent_gather_copy_bound`. A probe which is itself a prepared Inner join has no static capability and is declined before its prepared descriptor can be consumed. This excludes a class of otherwise already-bounded nested pipelines without proving anything about the measured Q10 plan's exact eligibility.

The relevant existing primitives already exist: `GatherCopyBound::filtered` and `projected` (`queue_layout.rs:274–311`) preserve actual value maxima/retained children and transform identity layouts; `merge` (235–272) checks compatible schemas/column kinds and combines maxima. `take_only` (410 onward) covers fresh take with explicit index-validity extent, and `gather` covers probe identity reuse plus compact-index take. Inner emits <=4,096 pairs and creates fresh nonnullable indices; it neither introduces NULL sentinel rows nor mutates source payload values.

## Proposed representation and construction

Retain one record per physical variant:

```
PreparedLayoutVariant {
    identity: QueueCopyBound,
    gather: Option<GatherCopyBound>,
}
PreparedOutputLayouts { variants: Vec<PreparedLayoutVariant> }
```

The identity can alternatively live solely inside GatherCopyBound when gather is present; avoid independently mutable duplicate proofs. Queue max remains the checked maximum across identity variants, so a gather decline need not discard an already-valid queue bound. Expose `gather_variants()` only if **every** possible physical variant has gather metadata. Do not combine plain Utf8 and dictionary encodings under one forged schema.

Generalize Inner composition to accept probe gather variants and produce their cross product with the existing build plain/dictionary alternatives. For each output variant:

- Use the existing queue computation unchanged for identity/exposed copied charge, including actual physical field/schema metadata and retained masks/orientation.
- Preserve each build/probe fixed-width, Boolean or Null GatherColumn kind.
- A taken plain Utf8/Binary value cannot exceed the original actual maximum element length. Copy that bound, not an average and not the full input byte budget.
- Existing dictionary columns keep their key width and complete retained-child copied charge. Arrow take/filter preserve that child.
- The small-build plain Utf8→Dictionary alternative (`create_joined_batch`, `hash_join.rs:4490–4516`) creates Int32 keys into the exact retained original build array. Its repeated-take descriptor must be `Dictionary { key_width: 4, retained_child_bytes: owned_input_column_charge(actual_build_column) }`. Do not confuse the child charge with the outer dictionary charge. No extra scan of values is required solely to price this alternative.
- Probe identity reuse (`hash_join.rs:4525–4538`) is covered by the composed identity layout; a later outer take uses the same element maxima/retained children. For output arrays built by plain take, the existing take-only build model remains preferable to carrying the full original build identity payload.

Column masks must select both layout and gather metadata at the same original indices, with the same swapped order and actual output schema rewriting. `Filter` transforms both pieces with their corresponding existing methods; column/Alias Project uses the shared runtime resolver and actual field-type rules. A transform whose queue bound is known but gather metadata cannot be proven retains the queue variant with gather=None. A failed queue transform retains prepared streams with Unknown as today.

Cross-product allocation and row/byte arithmetic must be checked/fallible. Coalesce only exactly compatible physical schemas using the existing maxima merge (including schema String-capacity charge), not logical-name/type equivalence. If metadata construction becomes excessive or allocation fails, decline gather or output bound while retaining streams; do not allocate an unbounded cross product or invent one schema. Repeated conservative gather identity additions may still make deep plans overreserve; tightness is a measurement/resource issue, not permission to remove a required extent.

## Safe recursive preparation order

1. Preflight the outer operator's existing semantic barriers: Inner only, no subquery keys/ON filter, partition correspondence. Build/init outer state before reserving its output envelope. This is already the consuming queue's preparation order.
2. In SpillableHashJoin, compute/cache the build decision **before preparing probe streams**. If spilled, return None without starting/initializing child output solely to discard it. If in-memory, delegate to the cached HashJoin's preparation exactly once. Its old static-probe-only preflight must be replaced, not retained above recursion.
3. In HashJoin, finish `ensure_build_cache`, then verify factory prerequisites (single cached batch, vectorized hash table, no row store and existing nonempty restrictions) before child preparation. Unsupported outer execution variants decline now, when no child streams need preservation.
4. Obtain probe input exactly once: first request `probe.prepare_queue_input()`. If it returns Some, own that Vec, check declared count, and use it directly; never call `probe.execute` for those partitions. If it returns None, the existing static pool-independent gather route can still initialize ordinary probe streams once. None must still mean no child output producers were started.
5. If child Some has complete gather variants, compose output metadata. If child Some is Unknown/Bytes-only or composition fails, **still construct the already-supported InnerProbeStream around those same child streams and return Some with Unknown output** (or a separately proved queue bound). The descriptor's pool-independent future-pull lifecycle survives a metadata decline. Do not fall back to ordinary outer execute after consuming child preparation.
6. Return unpulled streams via the shared InnerProbeStream factory. Do not create output producer tasks. The consuming queue alone chooses parallel envelope or serial actual-admission fallback; its existing Some(Unknown) behavior already preserves streams.
7. Any later error/cancellation drops the owned stream Vec, completed cache references and initialized streams. Child initialization errors must cancel/drop their own pending tasks. No nested stream may reserve the same queue pool during future pulls unless that dependency is first removed or the capability declined.

An explicit private helper accepting a preinitialized probe descriptor can avoid accidentally preparing it in both SpillableHashJoin and its HashJoin delegate. Prefer keeping all probe preparation in the delegate; Spillable owns only the build-decision gate. Static-resident leaves generally return None from prepare and remain on their original execute path.

## Deliberate declines

- Spilled child/outer joins: their producer, partition readback, finalize reservations and file ownership do not satisfy this in-memory pool-independent promise merely because initialization finished. Keep None; do not start spill output to inspect it.
- Semi/Anti: single-output build/probe trackers, prefetch, dedup/matched state and post-probe finalization are different lifecycles. No extension here.
- Left/Right/Full: NULL sentinel gathers, retained unmatched state/final passes and index validity require separate proofs. No extension here.
- Multi-batch build concat and row-store output: existing composition declines persist until their physical gathers are separately bounded.
- Unsupported views/nested/encoded payloads: preserve queue-only metadata where valid; no fabricated GatherColumn. Variable raw scans remain outside static gather proof unless existing actual/fixed metadata already certifies them.
- Computed Project/subquery Filter remain preflight barriers as today.

## Concrete regressions before integration

1. Pure composition: actual two-level Inner outputs followed by repeated Arrow take; fixed values, long resident strings, existing dictionaries, small-build string dictionary alternative, nullable payloads, empty output, duplicate output columns, both orientations, retained masks, all-selected/filtered slices. Compare actual queue charge <= identity and actual repeated-take charge <= gather for every physical variant; preserve compact/nonnullable-index precondition.
2. Real Aggregate→Inner→Filter/ColumnAliasProject→Inner→fixed raw scan and equivalent resident leaf, three partitions and batches. Independent full-row/group oracle for duplicates/NULLs; both Inner prepares must succeed with gather variants, zero output pre-pull, exactly one execute per actual leaf partition. No join-ID special handling.
3. Same-pool build/init reservations at both levels must succeed before the outer queue envelope. A deterministic two-slot overlap test must show two prepared output pulls before either is released and no third; distinguish this observed concurrency from merely having a descriptor.
4. Child Some(Unknown), Bytes-only, unsupported projected gather, failed checked metadata composition: return outer Some(Unknown) using original streams; prohibit fixture execute-after-prepare and assert exact rows. If outer factory prerequisites fail, assert child prepare was never called.
5. Error/cancel during inner build and successive child stream initialization: prior streams drop, pending owned tasks cancel, no producer starts, reservations/files follow existing owners. Drop before any output and mid-output both need deterministic acknowledgments.
6. Negative capability tests: spilled decision, Semi/Anti, outer joins, row-store/multi-batch, subquery barriers and unsupported encodings decline without output work. Do not interpret these as covered parallel paths.

This is a feasible narrower follow-up: retaining actual gather metadata and recursive lifecycle composition can unlock nested fixed/resident Inner pipelines without arbitrary string restrictions or a new runtime quantum. It still requires the existing exact output-bound checks and all resource/latency gates. Neither the Q10 causal attribution nor performance improvement is proven by source inspection.
