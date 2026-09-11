# Prepared Filter/Project composition — proposed contract

Scratch-only design during matrix73207. No production edits or runtime work.

## Motivation and evidence boundary

Current native Q19 measured medians are about1046–1086ms; IPC warmups3116–3152ms precede repeated measured timeouts. Parent reports historical scalar fullmatrix medians native268.723325ms and IPC552.749609ms (30each). These are different snapshots and not an alternating A/B, so they establish serious regression evidence, not a measured causal fraction. Source confirms Filter hides eligible Inner preparation; the outer aggregate's unknown-bound queue uses one pre-poll demand permit. See `.scratch/native-cpu-next-profile.md` for samples, source locations and qualifications. No query-ID behavior is proposed.

## API: lifecycle and output layout are distinct

Retain the current trait method signature `Result<Option<PreparedQueueInput>>`. Interpret None only as preparation not performed / declined without starting output producers. Some guarantees exact ordered partition streams, initialized without pulling output, with pool-independent future pulls. A layout certificate is optional within Some:

```rust
pub struct PreparedQueueInput {
    pub streams: Vec<RecordBatchStream>,
    pub output: PreparedOutputBound,
}

#[derive(Clone, Debug)]
pub enum PreparedOutputBound {
    Unknown,
    // Existing byte-only producers can migrate without fabricating a schema.
    Bytes(usize),
    Layouts(PreparedOutputLayouts),
}

#[derive(Clone, Debug)]
pub struct PreparedOutputLayouts {
    // Private: only audited constructors/transforms establish guarantees.
    variants: Vec<QueueCopyBound>,
}
```

Expose checked `PreparedOutputBound::max_bytes()->Option<usize>`; Unknown returnsNone, Bytes returns its certified requested-copy ceiling, Layouts returns the checked maximum. Expose `PreparedOutputLayouts::filtered()` and `::projected(exprs,declared_schema)` as checked, all-variants-or-decline transformations. Do not publicly permit arbitrary unvalidated Vec construction. Layout constructors remain restricted to audited physical code or test fixtures built from actual immutable batches. If crate visibility conflicts with the public PhysicalOperator return type, make the opaque types public with private fields and controlled constructors, following QueueCopyBound.

Each QueueCopyBound already stores exact physical SchemaRef/schema charge, checked per-column copied extent bound and max_rows. These cover the universal queue-copy charge, not retained source owners or expression/decoder scratch. Nonempty variants are required; an empty list must not vacuously certify arbitrary output at0bytes. Empty-stream preparation may conservatively use Unknown rather than invent a zero-byte schema. Keep variant creation bounded by the producer (current Inner alternatives at most2); Filter/Project map1:1, avoiding combinatorial expansion. Do not deduplicate equal schemas unless schema/header capacity charges are conservatively merged too.

Promote/rename the existing private `InnerOutputCopyBound` into the generic layout container, or add a consuming conversion that preserves its existing variants. It already holds both actual plain and small-build Utf8-to-Dictionary alternatives, after keep masks/orientation and actual-schema synthesis. HashJoin preparation should transfer that descriptor instead of throwing it away after `max_bytes()`. It must retain the existing single cached batch/no-row-store/eligible probe limitations. No new gather domain or join variant is certified by this tranche.

## Queue consumer rule: never discard prepared streams on missing bound

`stream_merge_input_partitions` continues zero/single fast paths and checked capacity handling. For multiple partitions:

1. Ask input.prepare_queue_input before reserving an outer envelope.
2. Some: validate stream count exactly equals declared partitions. Extract output.max_bytes and retain streams, even when it returnsNone. Never fall back to calling input.execute after Some. Do not call an unrelated static getter to replace a prepared Unknown certificate.
3. None: static pool_independent_queue_copy_bound may be used as today, with ordinary execute initialization.
4. Attempt the fixed checked K*bound envelope if known. If unavailable/too large/no slots fit, retain the prepared streams and use the existing one-permit, actual-byte fallible admission path.
5. Each prepared stream is consumed exactly once, indexed by declared partition order; parallel scheduling and output may interleave. Any count-contract failure/query error drops all owned streams rather than reexecuting. Ordinary drop remains eventual for async cancellation.

This change is essential: failure to calculate transformed metadata AFTER child preparation is not permission to returnNone and repeat child initialization. Unknown is a safe loss of parallelism, not a loss of lifecycle ownership. No unreserved pulled batch waits for byte admission; the serial path retains its current pre-poll demand and immediate named refusal.

## Filter implementation contract

Before requesting child preparation, reject `predicate.contains_subquery()`; do not initialize a child and then discover a same-pool subquery dependency. A configured SubqueryExecutor alone does not prove dependency; the actual no-subquery runtime branch is already explicit. Other expressions must retain existing evaluate/error behavior; this contract makes no claim about admitting expression scratch.

If child returnsNone, Filter returnsNone. If Some:

- Transform Layouts with existing audited QueueCopyBound::filtered per variant. It keeps identity/all-selected/slice exposure bounds, preserves full dictionary values children and adds checked rounded compact validity allowance per column. This follows Arrow58's fixed/Boolean/bytes/dictionary filter paths. Unknown or Bytes cannot prove this transformation: output becomesUnknown.
- Map each owned stream with the exact same lazy helper used by ordinary Filter.execute. The helper must reuse evaluator.evaluate followed by Arrow filter on every column and `RecordBatch::try_new(batch.schema(),...)`. No new predicate evaluator, no eager next/try_next, no background producer.
- Return Some even if layout transform declines/overflows; the same wrapped streams will be used serially. A runtime predicate/Arrow error propagates unchanged and drops pending upstream owners normally.

Do not claim filter is byte nonincreasing merely because row count shrinks. NULL bitmap materialization, existing sliced layouts, dictionary child retention, schema capacities and alignment remain part of the existing bound proof. Supported layouts only; nested/views stay declined. The safe queue's actual charge check remains authoritative against a claimed bound.

## Project implementation contract

Preflight expressions as recursively `Column` or `Alias(Column/Alias(...))`; computed expressions, CASTs and subqueries decline BEFORE child preparation. Aliases do not create evaluation side effects. If child returnsSome, map all layout variants using existing QueueCopyBound::projected and wrap streams via the SAME `project_batch` routine as ordinary execute.

Use shared `find_column_index_in_schema`, preserving current qualified/exact/suffix behavior. Resolve separately for each physical variant; do not assume declared types equal actual dictionary types. Preserve requested order and duplicated aliases by charging each emitted column independently. Use the runtime's actual-schema reconstruction rules and retained Field/Schema metadata charges. Existing Null-array-to-declared-type conversion is outside column-only metadata propagation; if encountered only after obtaining variants, retain prepared wrapped streams but setUnknown. Do not execute the child twice or change runtime conversion semantics to force a bound.

Even when a SubqueryExecutor is configured, column-only expressions follow the same project_batch call as ordinary execution. Audit that evaluate_expr_with_subquery for Column/Alias cannot execute a subquery or reserve the consumer's pool; otherwise tighten this preflight rather than declaring unsupported calls safe.

## Ownership and failure cases

- Preparation can initialize joins using the same pool: all such work finishes before Filter/Project return and before queue envelope admission.
- Dropping the wrapper's preparation future while awaiting the child drops the child future; existing owned join initialization cancels children. No wrapper task is spawned.
- Wrapping Vec streams has no await or output pull. If metadata fails, it changes only the output certificate toUnknown.
- Allocation failure building descriptors/stream Vecs must be handled consistently with existing fallible contract; use try_reserve for added containers. If construction errors after initialization, drop streams and return Err; never silently reexecute.
- Metadata does not retain source batches. Schema arcs/descriptors may retain strings; bound construction is small metadata overhead, not a complete query-memory guarantee.
- Foreign/malformed public descriptors must fail stream-count/actual-byte checks; private constructors reduce accidental false proofs but cannot make an arbitrary PhysicalOperator trustworthy.

## Required focused regressions before performance work

1. Real SpillableHashAggregate -> Filter(residual OR) -> SpillableHashJoin Inner -> certified3-part probe. Each probe execute reserves the whole shared pool transiently; successful preparation proves build/init before outer envelope. Independent duplicate/NULL/string oracle; assert3executes,0prepulls during preparation and overlapping later pulls.
2. Actual grouped/native dictionary and IPC plain-string variants, both dictionary-gather choices; aliases duplicated/reordered through Filter->Project and Project->Filter. Every emitted Arrow batch's actual owned-copy charge <=selected ceiling; all/none-selected, empty batches, sliced NULL/value bitmap and large dictionary children.
3. Unknown and Bytes-only child certificates: Filter and Project preserve Some/lazy streams, execute each child once and deliver exact outputs with serial actual admission. A mock iterator tracks distinct stream identity so an accidental reexecution cannot pass.
4. Actual Layouts projection decline after preparation (Null physical input declared non-Null output), plus checked overflow in metadata transformation: outputUnknown, same stream identities, no duplicate initialization, correct runtime result/refusal. Preflight computed expression/subquery instead yieldsNone before child execute.
5. Envelope admission failure with a valid transformed bound: fallback consumes prepared streams exactly once, holds one demand, no extra unreserved pending batch. Existing spill budgets/tests remain unchanged.
6. Cancel during child preparation and after wrappers returned, midstream errors, schema mismatch/lying bound and zero/one partitions. Assert eventual reservations/file cleanup with deterministic notifications; no sleep-based timing assertions.
7. Pure projection resolver ambiguity and metadata-capacity tests reused from queue_layout. No guessed name lookup or collapsed dictionary schema.

After focused correctness/lifecycle gates, run isolated unchanged-ceiling completion diagnostics and alternating native/IPC Q19 plus Q14/no-residual control using original SQL and frozen baseline. Device/native/Iceberg/Lance/public/resource gates remain required; recovering this shape is not cross-provider leadership. Do not accept performance by dropping duplicate predicates, skipping residual filters, increasing memory limits, or declaring all joins pool-independent.

## Implementation order and scope

First metadata/queue-consumer migration (plan.rs, queue_layout.rs, spillable.rs, hash_join.rs; update existing mock descriptors/tests). Then shared lazy Filter/Project helper extraction and preparation propagation. Then tests above. This is a bounded API correction; no new scheduler, dependency, operator specialization or changed timing boundary. A code patch is deliberately deferred until this contract is reviewed; source remains frozen during the matrix.
