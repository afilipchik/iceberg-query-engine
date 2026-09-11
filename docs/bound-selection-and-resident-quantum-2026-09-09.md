# Typed selection and resident output quantum — 2026-09-09

## Evidence driving the change

Frozen `7b5b7e93` removed the collected outer-join boundary and changed Q13's
admitted aggregate frontier from one to sixteen slots. Its eighteen diagnostic
outputs validate, but resident Q12 increased from185.9ms to595.3ms on the second
request. Build input has the same310,803 rows but7,323 batches instead of916.
This is diagnostic evidence, not a paired confidence interval or an acceptable
protected-query result. Preserve [the frozen control](bounded-outer-pipeline-2026-09-09.md).

Two shared costs are being addressed: per-cell dynamic dispatch in admitted
selection, and an arbitrary8,192-row resident-source output ceiling. No SQL,
query IDs, provider data, dependencies, memory limits or semantic proofs change.

## Candidate contracts

`storage/admitted_selection.rs` binds the actual ordinary primitive, Boolean or
UTF8 array once per source column. Row selection remains bounds-checked. Encoded
arrays retain checked dictionary key/value traversal rather than assuming a key
range or replacing logical NULLs with physical validity. Primitive and Boolean
construction combine validity and values in one traversal; UTF8 combines validity
with its exact byte-size pass before admitted allocation. Full logical type
validation still occurs before construction, including all-NULL inputs. Bound
metadata itself is reserved and released after construction.

`scan/admitted_memory.rs` replaces the fixed row ceiling with a child byte cap
for each output construction. The cap shares the protected input domain across
partitions and finite queue prefetch; it charges actual live buffers, not the
maximum capacity of each retained frame. Initial row count is bounded by row-index
storage and available source extent. Admission refusal halves the attempted
prefix; only successful output advances the source cursor. Fitting source batches
can pass intact, and large/variable outputs split under pressure. No semaphore or
wait depends on a consumer releasing retained output. Exhaustion refuses by name.

Resident input is still copied before a downstream filter. These changes do not
claim predicate fusion or removal of that copying; its cost must be measured.

## Validation so far

All tests use the real memory wrapper, repository TMPDIR, locked/offline features
`lance,gpu`, RAYON_NUM_THREADS=4, SAFE_BUILD_MEM=48G and SAFE_BUILD_JOBS=1.

-23250 exits0: the existing three admitted-selection tests pass after typed binding.
-84467 exits101: the new fitting-batch test reproduces8,192 output rows versus
  expected65,536 under a sufficient byte budget. This is before the quantum edit.
-The same test also checks every value across all smaller outputs under pressure,
  completion and final release. An additional independent selection test covers
  mixed plain/dictionary batches, NULL keys/values, duplicate addressing and
  positive/negative decimal coefficients wider than i64 at scale7.
-Broad gate46852 exits0: **1,020 library passes, 11 ignored and67 integration
  passes**, including all ten native streaming tests. The fitting/pressure
  regression and mixed-encoding/wide-decimal oracle both pass.
-Spill44438 exits101: six pass and the same seven historical names fail. Their
  failures are preserved separately; library spill tests do not certify this gate.
-Formatting and whitespace checks pass. [Implementation evidence](benchmarks/2026-09-09-bound-selection-quantum/manifest.json)
  archives seven verified files and500 source inputs. Only
  `admitted_selection.rs` and `scan/admitted_memory.rs` differ from frozen7b5b7e93.
-Release85464 exits0 in8m50s, frozen2026-09-09T18:27:10.254357Z. All500
  inputs verified; binary SHA256
  `dbca414acffd9377ba6e4a9b5a3676c3496bf767525813c3578231582af27a5a`. Driver:
  `.scratch/parallel-aggregate-input/build_bound_selection_quantum.py`.
  Diagnostics53339/34649 completed successfully; results below. The separate
  runtime-filter lineage failure remains present in this frozen candidate.

Next: finish the source-verified release and repeat Q12/Q13 diagnostics against
unchanged typed oracles.
Acceptance requires recovering resident Q12 while retaining Q13 correctness and
parallel input. Only then proceed to randomized protected-query and complete
provider/residency/resource/concurrency gates. The original goal remains open.

## Frozen diagnostic result

Primary53339 and supplemental34649 both exit0. All24 completed outputs are typed-
correct. Dataset/provider inventories, binary and500source inputs verify before
and after. Conditions match the preceding diagnostic:16threads/CPUs0–15, raw/native
4GiB query and12GiB process, preloaded CPU32GiB query and48GiB process. Each query
has two requests in a fresh process; setup and serialization are outside query
wall time. This is an180s-watchdog diagnostic, not a10×DuckDB acceptance run.

| Mode | Primary Q12 ms | Primary Q13 ms |
|---|---:|---:|
| Raw Parquet |1096.9 /1117.4|2624.4 /2587.1|
| Native |499.2 /497.2|2419.5 /2382.1|
| Preloaded CPU32GiB |747.3 /311.8|2766.0 /2394.1|

Resident Q12's build input returns to916 batches for310,803 rows, from7,323 batches
with the8,192-row ceiling. Its second request improves from595.3ms to311.8ms, but
remains above the earlier185.9ms control. Q13 retains the earlier diagnostic
improvement over the collected outer pipeline; no new paired confidence interval
is established for this change. The candidate is still not acceptable as a
protected-query or overall leadership result.

Supplemental Q12 values are raw1065.7 /1036.5, native509.4 /502.9 and resident
727.9 /331.6ms. Supplemental Q13 values are raw2583.3 /2618.9, native2447.8 /2374.0
and resident2725.9 /2319.9ms. All six Q13 frontiers show16 admitted slots for
16 outer-join partitions. No source consumption or SQL semantics were relaxed.

Primary scope peak19,583,442,944bytes; supplemental15,224,430,592bytes. Both scopes
have zero max/OOM/kill events and swap disabled. The
[diagnostic archive](benchmarks/2026-09-09-bound-selection-quantum-diagnostics/manifest.json)
contains261 verified files and500source inputs, including full typed outputs,
oracles, traces, provider setup, drivers and harness.

Both measurements are terminal. The next source task is the independently
[reproduced runtime-filter lineage wrong answer](runtime-filter-lineage-2026-09-09.md),
followed by the remaining resident copy cost and strict performance gates. The
frozen diagnostic binary retains that bug; these24 query results are not a broad
semantic certification.
