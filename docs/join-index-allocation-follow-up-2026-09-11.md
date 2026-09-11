# Join index allocation follow-up — 2026-09-11

Frozen864e645 checkpoint reproduces engine LanceQ9 warmup refusal: join index
allocation failed because the allocator returned an error. Reference-only I/O
quota16 cleared the reference barrier but did not change the engine runtime.
The current Lance sibling-task cleanup candidate has not yet been measured.

Read-only source evidence while pipeline99427 builds:

- VectorizedHashTable::build evaluates build keys, chooses hash/direct layout,
  computes index_bytes, obtains pool.allocate(index_bytes), then allocates
  heads/next/entries with try_reserve_exact. The reproduced text comes from this
  latter allocation, not a query-pool denial. I64 key buffers are shared views,
  not copied key payloads; evaluated/decoded keys remain a separate ownership item.
- SpillableHashJoin::compute_build_decision includes payload plus the index
  storage bound in its threshold decision, then memoizes an InMemory HashJoin.
  Its execute delegates directly. A subsequent index-allocation error propagates
  from HashJoin initialization; it does not cause a new spill decision.
- The visible failure is therefore compatible with process-cap exhaustion despite
  a successful local reservation. It does not establish whether the excess comes
  from provider data, runtime stacks, allocator retention, keys or other operators.

Next diagnostic: after all candidate timing, replay the successful engine request
prefix through Q9 with process VmData/RSS/thread snapshots. Compare a fresh Q9
worker with the persistent sequence. Retain the existing4/12GiB caps and validate
completed outputs independently. Do not claim the sibling cleanup fixes a failure
preceded exclusively by successful scans.

A possible later repair needs an explicit build-initialization transition before
publishing the in-memory decision: preserve owned input and any evaluated-key state,
release provisional index storage, and transition to spill on a supported typed
allocation refusal. This is a design hypothesis, not an implemented fallback.
Do not catch every Execution error or repeat source/expression evaluation. Numeric,
framing and semantic errors must remain terminal; capacity overflow is distinct
from allocator refusal. Check build ordering, runtime filters, duplicate/NULL and
outer/semi/anti semantics, cancellation and leases across the transition.

Reproduce with deterministic index-allocation failure injection, verifying actual
spill and independent typed results, plus terminal non-memory errors and exact
source/expression call counts. First identify the retained process allocations:
a fallback can otherwise mask a provider ownership defect without making the
engine resource-safe. No engine changes were made during frozen measurement.

## Engine sequence reproduction

After pushed97cea5c, diagnostic9582 on frozen9ba109ec completes80requests then
reproduces Q9warmup index refusal (552.203ms). Immediately beforeQ9 VmData is
11,745,120KiB, after refusal12,007,840KiB;339threads include290lance-io. The
16GiB scope peaks9,139,961,856bytes withzeroOOM/max andswap0.
Fresh-worker control50471 executes the identicalQ9 successfully (1352.153ms),
endingVmData8,974,824KiB. Its setup has187lance-io threads, ending235. Counts
are observations, not fixed runtime ceilings. Audit51920 independently validates
all80sequence outputs plusfreshQ9 against canonical DuckDB oracles (81pass).
These establish sequence-dependent engine exhaustion, not the retained owner's
identity. Driver omits reference interleaving/idle delays; timings arediagnostic.

Allocator statistics replay42956 is active with MIMALLOC_SHOW_STATS=1, samecaps.
Local libmimalloc-sys0.1.49 defaults to v3 absent thev2 feature. Its arena reservation
default is1GiB, scales witharena count, adds metadata room, and supports a smaller
128MiB fallback only when the request fits. Linux-overcommit eagercommit can make
a reservation writable while statistics defer charging committed bytes. These
source facts suggest checking arena commitment versus RLIMIT_DATA, but do not
yet prove that setting or allocator caused the denial. No allocatorpolicy changed.

Statistics42956 terminal0 reproduces Q9indexrefusal after80completed requests.
Mimalloc exit statistics report10.8GiBreserved/10.6GiBcommitted and16arenas;
negative page counters make this unsuitable as an exact live ownership ledger.
Same-cap lazycommit98431 (MIMALLOC_ARENA_EAGER_COMMIT=0 plusSHOW_STATS=1)
completesQ9warmup at1372.263ms andVmData12,225,192KiB. It then terminates1
because the diagnostic driver accesses an output field absent from the original
NOTRUN request. The worker is closed by finally; no cgroup-after file was written.
Preserve this driver error separately from the completedQ9. v1driver is retained;
v2 stops after the original first-failure boundary even if the control succeeds.
Audit86306 is running over all completed defaultstats/lazy outputs. No production
allocator change; one successful near-cap warmup does not prove repeated queries
fit or that lazycommit is a sufficient repair. Repeat and full-sequence controls
are required before changing defaults, and caps must remain enforced.

Full lazycommit75024 terminal0 preserves82completed requests then Q9measured2
indexrefusal (531.826ms), VmData12,582,432KiB immediately after refusal, against
12,582,912KiB cap. Warmup and firstmeasuredQ9 succeed; the next fails. Thus
lazycommit is insufficient and must not be promoted as the fix. Scopepeak
9,427,148,800bytes,zeroOOM/max,swap0. This run's completed outputs await audit.

Release fingerprints show no libmimalloc-sys feature enablingv2. The v3 Unix
decommit implementation normally calls MADV_DONTNEED and setsneeds_recommit=false;
only debug/high-secure paths also mprotect(PROT_NONE). This explains why purged
physical pages need not release writable address-space accounting; exact retained
heap ownership remains to be measured. No vendor source modification.

Control88552 is active with MIMALLOC_ARENA_RESERVE=0 andSHOW_STATS=1, same
4/12GiB caps,16GiBscope and fullengine SQL sequence. The local allocator declines
arena reservation atzero and may use directOS allocation; outcome is not known.
This tests retained arena contribution, not a relaxation of process memory safety.

No-arena88552 terminal0 completes88/88requests, including all4Q9 and all4Q19
requests, same4/12GiB limits. Data memory is near5.3GiB afterQ9 but increases
acrossQ19 to9,501,072KiB; one full sequence is not sustained memory acceptance.
Scopepeak6,860,095,488bytes,zeroOOM/max,swap0. Exit page counters remain
inconsistent, so they are not an exact ownership ledger. Audit71662 is running
over82lazy-full plus88no-arena outputs. No production allocator policy changed.
Next repeat the full sequence in the same process and attribute residual growth
before treating no-arena allocation as a solution or measuring its CPU tradeoff.

## Endurance failure and next provider boundary

No-arena20717 terminal0 driver reports enginecrash on d2-Q8warmup after88first
sequence completions. Stderr: memoryallocation of8,388,608bytes failed. Scopepeak
6,864,224,256bytes,zeroOOM/max,swap0; process allocator abort remains a failure.
Audit7007 verifies88completed outputs. All seven controls now total500independently
typed-correct outputs; [hashed archive](benchmarks/2026-09-11-engine-sequence-allocation/manifest.json)
verifies531source inputs and preserves each failure. No-arena is insufficient for
sustained execution and is not promoted as a production policy.

During endurance the runtime reached509lance-io threads. Source clarification:
Lance scanner retains a2GiB legacy default, but current filtered-read scheduler
uses explicit override or max_bandwidth(objectstore),32MiB per I/O thread. Do not
assert2GiB per active scanner without confirming its path. ScannerI/O allowance
is not a hard total cap and excludes returned output ownership.

The provider currently spawns one scanner per fragment. Each can independently
schedule batch decode and I/O. Lance10's Scanner supports with_fragments plus
scan_in_order(true), which promises fragment-vector order while performing reads
concurrently. A single ordered multi-fragment scanner could share scheduling
across fragments and remove redundant outer fanout. This is the next candidate,
not proven equivalent or faster. Test projection, filter, explicit subsets and
empty subsets, duplicates/NULLs, ordering, deletion handling and multiple batches
before changing the provider. Preserve budget refusal and task lifetime; reducing
scanner count alone does not establish full query-wide memory admission.


Further inspection of the retained no-arena full-sequence records shows that
thread count alone does not explain the residual growth. Across all fourQ19
requests, total threads stay577 (528 named lance-io). VmData grows from5,549,328
KiB to9,501,072KiB; RSS grows from701,992KiB to1,277,324KiB. Thus most additional
writable mapping capacity is not resident at the observed request boundaries.
This is consistent with retained allocator mappings, but does not identify live
owners or prove a leak. The new scanner candidate must be tested repeatedly;
a lower thread count or one successful Q9 is insufficient.
