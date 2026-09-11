# Aggregate key binding: measured stage and source audit — 2026-09-09

The implemented shared CPU candidate binds invariant Arrow key metadata before
row encoding. Threshold residency92429 is terminal and archived; the newer source
passes the gates recorded below; release43237 completed and froze0f30c946. An initial instrumented Lance Q18 comparison observes lower time; broader
performance acceptance remains open.

## Evidence and profiling limit

The clean matched Lance and explicit CPU-only native diagnostics attribute about
1.56 seconds to key preparation/routing and about1.34 seconds to output work.
These wall-stage measurements remain distinct from statistical CPU sampling.

Clock-profile74684 completes native Q18 correctly at4GiB, with zero spill and
5,679.686ms instrumented elapsed time. GNU gprofng2.42 is installed locally and
collects without changing perf_event_paranoid=4. Collection uses1ms clock
sampling, no hardware events, no descendant collection and no automatic binary
archiving. TMPDIR stays in the repository, core size is zero and the collector's
core-dump option prevents its alternative /tmp crash report path.

The profiler header warns that the collection timer changed from1000 to0 and
that profile data may be unreliable. Function samples include canonical-key
encoding, group lookup, state preparation and Rayon scheduling. Almost all
inclusive sampled time is rooted in Rayon workers; apparent coverage of the
1.34-second output stage is weak. Do not use these percentages as an exclusive
whole-query CPU decomposition or treat unobserved work as free. The query's typed
correctness, raw traces and profiler warning are preserved separately.

## Source fact

`key_rows/arrow_input.rs::KeyWorkspace::encode_arrays` validates array arity and
logical types on every row. It visits each value once to measure encoded size,
admits the scratch buffer, then visits it again to write bytes. `visit`/`resolve`
perform runtime type dispatch and downcasts; dictionary codes remain checked.
`prepared_keys.rs::PreparedKeys::try_new` invokes this for every row in each
scoped preparation chunk. Arrays and logical types are invariant within that
retained evaluated batch, so repeated metadata work is a concrete shared cost.
The earlier bound aggregate-value arrays do not remove this key-encoding work.

## Bounded implementation sequence

1. Add an admitted borrowed binding for supported primitive key arrays. Validate
   layout/type identity and downcast once before encoding rows. Account for the
   binding vector through the existing pool; retain the evaluated arrays for its
   full lifetime. Preserve the existing generic path for unsupported encodings.
2. Use the binding inside prepared-key chunks. Preserve the same canonical bytes,
   hash function, full-key equality, original row positions and evaluated-batch/
   layout identity. Do not change owner routing, spill format or SQL expressions.
3. For fixed-width primitives, measure/write from bound typed values while keeping
   NULL tags, float zero/NaN normalization, decimal scale and temporal units exact.
   Do not allocate a ScalarValue per cell or silently cast types. Variable/nested
   and dictionary fallback keeps the existing checked semantics; binding is not
   permission to skip dictionary code validation.
4. Preserve optional preparation's shared one-eighth child budget and cleanup on
   refusal. No row dispatch occurs before successful preparation; metadata
   refusal must not replay the source or volatile expressions. Actual state and
   output admission, prepared spill resources and exact-row retry stay unchanged.
5. Add independent canonical-byte/hash and SQL regressions for signed/unsigned
   widths, booleans, floats, decimal scales, temporal values, NULLs, slices,
   duplicate keys, multiple batches and dictionary fallback. Exercise denied
   binding/preparation, owner release, malformed representation and existing
   spill/retry cases. Compare with independently constructed bytes/typed values,
   not only the old implementation.
6. Run focused and broad semantic/resource gates through the capped wrapper,
   freeze a new binary, and compare shared grouped workloads under the protected
   protocol. Include native/Lance/CPU and raw routes; preserve complete provider,
   residency and resource gates. Do not infer DuckDB leadership from a component
   improvement or widen the query limit to obtain it.

A separate output improvement remains plausible: bind invariant output metadata
once and emit typed values directly into admitted Arrow buffers, rather than
constructing64-row batches through scalar intermediates. Keep that as a distinct
change and measurement so attribution is not lost. Neither proposal changes
cardinality proofs, NULL/duplicate semantics or default memory safety.

## Implemented candidate and validation

Current source adds `key_rows/bound_arrays.rs`, used once per prepared-key chunk.
Bindings borrow typed primitive arrays and validity buffers from the retained
batch. A reserved column vector belongs to the existing optional child pool.
Every row still admits destination growth before publishing its key; strings,
lists and dictionaries use the original checked traversal. Exact layout identity,
row bounds, decimal/temporal logical types, NULL tags and canonical floating-point
bits remain explicit. The original batch cannot be replayed on admission refusal.
There is no dependency or query-budget change.

Three independent regressions construct expected bytes for integer widths, dates,
all timestamp units, scaled Decimal128, booleans, NULLs, slices, signed zero,
NaN payloads, infinities and dictionary NULL values/codes. An admission test checks
failed binding cleanup, invalidated keys after failed encoding, row/length/type
errors and foreign-layout rejection. Existing prepared-key tests cover multiple
chunks, duplicate keys and partial construction cleanup under pressure; integration
tests cover changed dictionary codebooks and real spill across batches/partitions.

Initial compile61394 failed on a test fixture's Arrow slice construction and is
preserved in `bound-key-focused.log`; no engine defect was diagnosed by that
compile. Corrected focused46830 passes all three tests. Default-feature broad21188
passes974 library tests/3ignored and10 integrations. Commands use
`TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=8
scripts/claude-safe-build.sh cargo test --locked`; the focused filter is
`--lib physical::morsel_agg::key_rows::bound_arrays`, and the integration targets
are `aggregate_post_filter_admission`, `parallel_input_spill_contract`,
`dictionary_group_contract_tests` and `aggregate_binding_transparency`.
Lance/GPU-enabled gate77560 passed1,036 library tests/11ignored and36 integrations.
Full spill76725 remains6pass/7fail, with exactly the preceding threshold gate
failure names; those resource failures remain open. Formatting and whitespace
checks pass. No candidate performance claim exists yet. The preceding threshold residency is separately archived and cannot
certify this newer source.

The [nine-file correctness archive](benchmarks/2026-09-09-bound-key-arrays/manifest.json)
preserves all508 source inputs and test logs. Release43237 completed in8m52s through `build_bound_keys.py` with48GiB and
one build job, locked/offline with `lance,gpu` features. Frozen binary SHA256 is
`0f30c946ad79f08fba94ac8941394fee0e6c2401fe4b1c3628e4809aaf12c3e0`; all508
source inputs verified unchanged. The two existing changed inputs are
`key_rows.rs` and `prepared_keys.rs`; `key_rows/bound_arrays.rs` is new.
Source remains frozen for measurements.

## Remaining structural cost: batch probing

The binding change removes repeated type dispatch; it does not remove per-input-row
canonical scratch, copying into prepared `KeyRows`, hashing, or scalar index lookup.
`PreparedKeys::try_new` stores a key and hash for every input row, including duplicate
keys. `GroupRows::find_hashed` then resolves one row at a time; permanent group keys
are copied only for new groups. Distinguish temporary duplicate preparation from
permanent group ownership when profiling memory and CPU.

Local DuckDB source at revision `1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8`
provides a larger comparison: `src/execution/aggregate_hashtable.cpp:641`
(`FindOrCreateGroupsInternal`) references the group's vectors and hashes, converts
the chunk to unified format, computes offsets/salts in a batch, and partitions
lookup work into empty and comparison selections. It appends tuple storage for
selected new groups. This source inspection supports a batch-probing hypothesis;
it does not prove a speedup in this engine or justify removing memory admission.

After measuring this isolated binding change, a distinct candidate can batch hash
and probe a bounded row selection, preserving canonical full equality and original
row positions. First retain existing transactions and key representation; admit
hash/selection/address scratch before use, resolve repeated keys within the same
batch, and commit rows in retry-cursor order. Test forced hash collisions, duplicate
keys spanning the candidate batch, NULLs and dictionaries, and denial immediately
before and after a new group's publication. A later reduction of duplicate key
materialization must provide an equally strong borrowed-key equality contract.
Do not combine these changes with hash replacement, new spill framing or output
construction; separate frozen measurements are needed to identify the actual gain.


## Initial frozen diagnostic — 0f30c946

Matched Lance49598 completed with all four outputs typed-correct. At4 GiB query/
12 GiB process,16 threads on CPUs0–15, the two fresh reversed-order blocks are
5431.932→4897.538 ms and5333.440→4931.286 ms (1efb2554→0f30c946).
The observed geometric ratio is0.913037, about8.70% lower; two instrumented blocks
are not a confidence interval or a10× acceptance screen. In block0 main routing
falls1574.886→1100.340 ms; candidate main aggregation spills zero bytes and
output remains1345.923 ms. The capped scope peaks at9,321,074,688 bytes with
zero OOM/max events. All508 source inputs, binaries, provider and data verify
unchanged after execution. Explicit CPU diagnostic63930 terminal0 validates all
three outputs without GPU execution or panic: raw954.107 ms, native5373.955 ms,
resident5199.943 ms. These are single candidate diagnostics, not matched ratios.
Raw/native use4/12GiB, resident32/48GiB remains a capacity experiment. The64GiB
scope peaks at20,710,543,360 bytes with zero OOM/max events.
The [223-file diagnostic archive](benchmarks/2026-09-09-bound-keys-diagnostics/manifest.json)
verifies508 source inputs and preserves all seven outputs, traces, comparisons
and provider/binary evidence. Protected suite2204 is running. Full provider/
residency/resource/concurrency acceptance remains open.
