# Q1: provider kernel gap and aggregate owner imbalance — 2026-09-09

Frozen `0f30c946` completes canonical Q1 correctly in632.511 ms on raw Parquet,
11930.298 ms on native and11192.850 ms on decoded resident CPU input. These
are single instrumented diagnostics with a180-second watchdog, not acceptance
measurements. Native and resident CPU still fail their ordinary10× DuckDB gates.

## Measured stages and actual row ownership

| Stage | Native | Resident CPU |
|---|---:|---:|
| Expression evaluation | 1655.349 ms | 1663.066 ms |
| Key preparation/routing | 2672.316 ms | 2134.232 ms |
| Aggregate processing wall time | 6950.487 ms | 6924.257 ms |
| Output | 0.035 ms | 0.044 ms |

Both live aggregate runs use four controllers and process59,142,609 rows without
spill. Raw Q1 uses `MorselAggregate`; native/resident use the generic live grouped
path. Removing output validity allocation will not fix this Q1 performance gap.

The validated output supplies these counts. A standalone Rust reproduction uses
the exact current canonical UTF8 presence/length/bytes encoding, slice Hash with
DefaultHasher, and `hash % 4` from `row_router.rs`:

| Group | Rows | Owner |
|---|---:|---:|
| A,F | 14,804,077 | 2 |
| N,F | 385,998 | 1 |
| N,O | 29,144,351 | 0 |
| R,F | 14,808,183 | 2 |

The counts sum to the measured input count. Owner2 receives29,612,260 rows,
owner0 receives29,144,351, owner1 receives385,998, and owner3 receives none.
About99.35% of state-update rows therefore occupy two owners despite16 available
query threads. The three nonempty owners also agree with the three output batches.
This establishes row imbalance, not exact per-owner CPU-cycle attribution.

## Why the current design limits throughput

`live_spill.rs` caps controllers at four, and `row_router.rs` sends every canonical
key to one fixed owner. `ParallelControllers::finish` can concatenate complete
groups because owners are disjoint. That simplifies exact merge ownership and
avoids repeated group state, but prevents many workers from updating a frequent
group in parallel. Raising the controller count alone cannot split a frequent key.
The same limitation affects any low-cardinality or skewed grouping, not just Q1.

DuckDB's local grouped aggregation and later partial-state merge provide a
structural comparison; see the existing local-source audit and the batch-probing
section of the key-binding report. Copying only its worker count would omit the
merge and memory contracts that make parallel partial aggregation correct.

## Implementation path

1. Preserve the all-valid output admission repair as a separate resource fix.
   Keep adaptive output sizing separate too; neither addresses this hot stage.
2. Add a generic partial-state ingestion mode that splits evaluated row selections
   among workers, allowing the same key in several local states. Borrow the exact
   retained batch and preserve per-shard row order and exact retry cursors. Do not
   re-evaluate source expressions or volatile values on denial.
3. Before any final output or HAVING evaluation, merge partial states for equal
   canonical keys. Merge SUM/COUNT, AVG's sum/count and actual DISTINCT state;
   never sum finalized averages or distinct counts. Reuse checked canonical full
   equality, spill framing and bounded partition merge where possible. The current
   concatenate-only finish contract is insufficient for this mode.
4. Admit local-state metadata, prepared keys, row selections, spill writers and
   merge scratch from one query budget. Replicated groups can increase memory;
   local growth must spill or refuse cleanly. Finish-time merge needs its own
   reserved progress resources. Do not multiply the root budget by thread count.
5. A cost model may choose disjoint ownership versus partial-state processing,
   based on observed group counts/skew and memory. Both choices must always be
   semantically valid, even when a sample or NDV estimate is wrong. No estimated
   uniqueness or dictionary cardinality may justify dropping the final merge.
6. Validate with independently computed results for a single hot key, skewed and
   balanced groups, NULLs, dictionary codebooks, decimals, floats, AVG and DISTINCT;
   include multiple batches/partitions, empty input and real spill. Force denial
   during local updates and cross-worker merge and prove exact row multiplicity.
7. Compare1/4/16-thread scaling on the same frozen input and budget, covering both
   low and high cardinality. Record worker row counts and phase wall times. Include
   canonical protected queries and native/Lance/resident providers; larger thread
   count is not itself success. Audit the remaining provider-specific fast-kernel
   routing and expression-evaluation gap after this stage is measured.

## Reproduction and limitations

CPU diagnostic98218 exits0: all three outputs validate, all508 source inputs,
provider data and binary hashes verify afterward. Raw/native use4 GiB query and
12 GiB process limits; resident32/48 GiB is a capacity experiment, not clearance
of the16 GiB preload gate. The sequential64 GiB scope peaks at14,760,538,112 bytes
with zero OOM/max events. QE_GPU=0 is explicit and GPU/panic checks pass.

Use `run_bound_keys_q01_profile.py` under the repository capped wrapper with
`TMPDIR="$PWD/.scratch"`, `PYTHONPATH="$PWD/scripts"`, `SAFE_BUILD_MEM=64G`,
one build job and `taskset -c 0-15`. The standalone `q01_owner_map.rs` is compiled
and run in separate1 GiB scopes after the diagnostic is terminal; it is a routing
reproduction, not a timing benchmark. The [archive](benchmarks/2026-09-09-bound-keys-q1-diagnostics/manifest.json)
contains the raw evidence and exact reproduction. No partial-state ingestion
implementation or performance claim for that proposed mode exists yet.

## Existing components and required new boundary

`GroupRows::prepare_merge_update` already compares the source canonical key and
prepares the entire partial-state row transaction before publication. A merge
consumer can use `KeyWorkspace::load_encoded` and that transaction, retaining the
source GroupRows throughout admission and commit. This must receive partial
states, not Arrow final values produced by `build_output_range`.

`IngestionController::finish` already exposes complete local GroupRows by a
borrowed callback, and `RunWriter::append(&GroupRows,row)` writes their partial
representation. `PartitionScheduler` merges run rows through bounded state and
repartitions on pressure. The missing component is a cross-owner merge consumer:
it needs a pre-admitted spill writer and publication slot before local ingestion
fills memory, an exact source-row cursor, and a way to spool unmerged partial
rows when other retained local owners prevent resident target growth. Do not
return a memory error simply because the empty target cannot copy a source row
while a prepared partial-row spill path remains usable. Do not clear or release
the sole source representation before successful transfer to an admitted owner.
I/O failure poisons the query and must never publish partial output. New writer
creation and bounded run compaction need explicit progress points between owner
lifetimes; they must not rely on unaccounted emergency allocations.

Initially preserve `StateRowLayout::bind` eligibility: current source declines
DISTINCT before ingestion. Its existing fallback must remain correct and receive
coverage; the initial partial mode must not pretend to merge DISTINCT final
counts. Supporting DISTINCT within this mode would be a separate capability with
actual distinct-set merge and its own budget/spill contract. Fixed SUM/COUNT/AVG
and already supported selected states can reuse their present merge semantics.

Keep three independent changes reviewable: (1) a tested partial-state merge
consumer with denial/spill recovery; (2) a row-selection routing policy that
requires that consumer at finish; (3) measured cost-based selection between
partial ingestion and existing disjoint ownership. Do not enable the routing
change until the final merge consumer passes independent cross-owner tests.
