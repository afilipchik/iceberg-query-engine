# Prepared inner-join queue capability audit

2026-09-06, read-only source review. No source edits or runtime jobs. Parent's
completed IPC Q14 diagnostic reports approximately unchanged one-thread time,
but 16-thread wall time ~83 ms before versus ~219 ms after, with process CPU
~320 versus ~260 ms. The outer queue sees an unknown-bound SpillableHashJoin;
serial demand covers its entire try_next. This identifies a missing capability
boundary; it does not quantify the separate cost of copying or establish a fix.

## Conclusion

A prepared **in-memory Inner** join can support a useful guaranteed copied-output
bound, provided its actual cache and all future probe dependencies are certified.
It cannot safely gain that capability by calling ordinary execute on every
partition and then assuming initialization is finished. A narrow explicit
preparation protocol is needed. The initial supported scope should exclude spill,
non-Inner joins, unknown probe dependencies and unaudited gather encodings.
Preparation must finish before the outer output-envelope reservation. Estimates
of width, NDV, uniqueness and build fit never establish this capability.

## Exact current boundary

- `spillable.rs:1005–1044`: execute initializes `build_decision: OnceCell`, then
  either calls the in-memory HashJoin delegate or starts execute_spill_path on
  partition zero. BuildDecision alone is insufficient: the delegate has a second
  `build_cache: OnceCell<Arc<BuildSideCache>>` initialized in
  `hash_join.rs:1134–1483`. Cache materialization/concat/hash construction must
  finish before publishing a prepared guarantee.
- `hash_join.rs:1486–1535`: **all current successful nonempty JoinType::Inner
  executions** return InnerProbeStream. Empty build returns empty; unsupported
  vectorized key domains return an explicit error. Older Inner branches later
  in the file are not reachable through this public execute path. Cross, outer,
  semi and anti routes do not inherit this bound.
- `InnerProbeStream::next_batch`, :1816–2001, allocates at most 4096 candidate
  pairs, applies a filter that only removes pairs, then emits one gathered batch.
  Exact direct/typed/composite checks preserve duplicates and skip logical NULL
  keys. The next output has <=4096 rows regardless of multiplicity. A no-match
  pull may consume many probe batches; cooperative yield occurs every 4096 cursor
  steps and after rejected chunks. The row bound is not a byte or pull-work bound.
- Each stream owns its cache Arc, a probe stream, current probe batch, key arrays,
  hashes and typed/null handles (:1795). Dropping the stream releases that state.
  The operator's OnceCell retains another cache Arc until operator drop; cache
  ownership/reservations must therefore not be tied only to queue-stream lifetime.
- `create_joined_batch`, :4335–4440, gathers row-store or columnar build values,
  then gathers probe values; identity probe indices reuse whole probe columns.
  The bound must include their actual exposed buffers, not only 4096 logical rows.

## Gather layout is richer than a scalar copy-bound number

Current QueueCopyBound records column copied-layout maxima and a schema. It is
not yet a proof of the output layout of arbitrary repeated take operations.

1. Primitive/Boolean take needs <=4096 values plus conservative validity/offset
   allowance. A row-store gather needs the same audited physical output types.
   Identity probe reuse needs the full declared probe exposed-buffer bound.
2. Repeated Utf8/Binary values require 4096 times an **actual maximum value
   length**, plus offsets and validity. The source buffer total is a safe but
   potentially enormous upper bound on one value; average width is unsafe. A
   reusable actual-data gather capability could cache per-value maxima while
   preparing resident inputs. Unsupported future streaming strings decline it.
3. Single-batch build <=4096 rows may wrap plain Utf8 as Dictionary(Int32,Utf8)
   (`hash_join.rs:4388–4401`), retaining the complete original values array.
   Existing dictionaries survive Arrow take through retained dictionary children.
   Queue copying recursively copies those whole children per output; the child
   charge must be included even if one value is selected. Physical output schema
   is adjusted by batch_with_actual_types, not always the logical join schema.
4. Multi-batch `gather_column`, :4701–4801, may concatenate whole build columns,
   temporarily promote Utf8 to LargeUtf8 and cast output back. Retained dictionary
   children/concat encoding must be audited or this path declined. Whole-cache
   concat scratch is not admitted by an output-envelope bound.
5. Retention masks prune cached build columns and probe columns, and swapped
   orientation changes ordering. Derive the bound from the exact post-pruning
   cache plus actual gather behavior. Logical schema alone is insufficient.
6. QE_DICT_GATHER is read during gather. Either bind that behavior at preparation
   or bound both supported physical alternatives. Include actual emitted schema
   metadata allocations/capacities, NULL payloads, empty outputs and checked
   multiplication/addition/rounding. A useful guarantee must not depend on a
   mutable environment switch remaining unchanged accidentally.

A minimal first version can admit fixed-width builds/probes, then add exactly
audited plain/dictionary string cases using real cache content and propagated
gather metadata. That staging may not cover the measured Q14 shape. Coverage
must be reported, not inferred from the join type or the presence of a cache.

## Future pulls and two-phase initialization

The in-memory Inner next_batch itself has no MemoryPool calls. It does call
`self.input.try_next`, evaluates arbitrary join-key expressions, and may evaluate
an ON expression. A probe operator can defer allocation/spill/nested queue work
until this call even after execute returned. Thus **execute completion is not a
pool-independence proof**. Require a recursive prepared capability on the probe
stream/owner, and decline unsupported key/filter expression domains if they can
invoke unaudited nested execution. Expression/gather scratch remains an explicit
separate limitation even for certified pool-independent expressions.

Do not generically initialize every unknown operator as a workaround:

- SpillableHashJoin's spill execute starts an output producer immediately
  (`spillable.rs:1332ff`). Merely obtaining that stream can start upstream work
  and retain outputs, before the proposed outer envelope exists.
- Direct HashJoin cache initialization uses ordinary tokio::spawn JoinHandles
  for build partitions (`hash_join.rs:1150–1176`). Dropping a JoinHandle detaches
  it. Cancelling an outer initializer/OnceCell future is therefore not currently
  enough to prove all its nested build tasks stop and release buffers. A new
  top-level JoinSet alone does not fix those internal ownership gaps.
- Cached decisions can be retried after cancelled/error initialization. A
  preparation attempt must not leave detached work, partial publication or stale
  per-call spill files. Existing query build/residency gaps do not disappear when
  preparation runs before admission.

Minimum protocol: an explicit opt-in prepare operation owns its tasks and either
returns a prepared in-memory Inner owner with cache + certified probe descriptors,
or declines without starting output-producing work. It must not reserve the
outer K*B envelope while recursive initialization needs that same pool. Once all
required partition descriptors are ready, derive a common maximum bound,
reserve K*B (or select the existing safe serial fallback), and start pulling the
already prepared streams exactly once. Prepared state must hold the cache and
probe guarantees for their complete lifetime. Zero declared partitions do no
work; one partition keeps its existing direct policy. No pre-pulled batch may
wait outside admission. Errors/panics cancel and await nested owned work; stream
drop aborts tasks and retains guards until asynchronous cancellation actually
drops their buffers. Do not repeat execute to enter fallback.

## Minimum contract tests

- Prepared and ordinary Inner exact typed oracle: multi-partition/multi-batch,
  both build orientations, empty build/probe, NULL and composite keys, duplicate
  hot keys producing many 4096 chunks, filters rejecting some/all candidates.
- Actual copied output <= cached bound across row-store, columnar, probe identity,
  partial take, masks, nullable primitives and every admitted string/dictionary
  alternative. One very long repeated string and large dictionary with one key
  must defeat naive per-row or logical-schema bounds.
- Deterministically overlap at least two output pulls only after cache/probe
  preparation; prove common root peak <= admitted envelope plus other real
  reservations. Hold a sibling owner and exercise reduced-slot/serial fallback.
- A probe with deferred same-pool work must decline the prepared guarantee even
  if execute returns immediately. Spill/non-Inner/unknown wrappers must decline;
  no optimistic 4096 declaration on their output.
- Cancel/fail/panic during build initialization, probe initialization, admission,
  first pull and queued output. Verify no detached initializer, no duplicate
  execution, exact eventual guard release and safe OnceCell retry semantics.

No existing broad resource gap is claimed closed here. The prepared-output
capability controls queue copies and scheduling dependencies, not build-table
ownership, upstream allocation or total query RSS.
