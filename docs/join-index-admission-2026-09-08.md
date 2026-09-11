# Join index size omitted from spill decision — September 8, 2026

The unchanged native256KiB self-join failure is now attributed by debugger evidence.
The existing test fails requesting383984bytes. Two controlled debugger runs stop
at MemoryPool::allocate(size=383984), before allocation, then deliberately end the
test process. They are diagnostics, not passing test runs. Both debugger drivers
exit0 under8GiB containment with repository TMPDIR.

Stack: VectorizedHashTable::build → HashJoinExec::ensure_build_cache → prepared
queue initialization → SpillableHashJoinExec's chosen in-memory delegate. Captured
locals:12000 rows,32768 hash buckets,59996 selected direct buckets,12000 index rows,
direct range[2,59997]. Index bytes are59996×4 +12000×12 =383984. This is not a
native decoder or copied-input queue reservation. Those other resource gaps remain.

compute_build_decision compared only collected Arrow payload size against its
spill threshold. The later index reservation was not part of that decision.
Independently, direct addressing used a fixed range ceiling without requiring
that its index be smaller than the hashed representation. For this input even
hashing needs275072 index bytes, so merely disabling direct addressing cannot
make the256KiB query fit. The build must spill.

Repair: share checked index sizing between construction and spill costing; include
that storage in the build decision while retaining the original source/prefix.
Select direct addressing only when it does not exceed the hashed index bound.
Keep actual pool reservations and allocator-capacity checks. The size bound covers
heads/next/entries, not all keys, payload, row stores or other operators; it must
not be described as full query-wide admission. No estimated uniqueness, expression
replay, memory opt-out or benchmark-specific threshold is allowed.

The unchanged native failure is in bound-array-broad-01.log. Debugger logs and
commands are in `benchmarks/2026-09-08-join-index-diagnosis/` with exact source hashes.
Implementation, independent typed/spill tests and broader performance gates follow.

## Implementation checkpoint

The shared checked helpers now price hash buckets and row links/entries in both
VectorizedHashTable construction and spill costing. The direct-address candidate
must use no more buckets than hashing. The spill decision includes index storage
while streaming the existing prefix; it does not evaluate or replay key expressions
to make this decision. Generic-map and auxiliary allocation accounting remain open.

New sparse-key regression fails before the fix with a60000040-byte request under
65536bytes. It includes duplicate and NULL keys with an independent expected match
set. Additional coverage checks dense direct selection, all-NULL and extreme signed
keys, cleanup, index bounds and integer overflow. The existing native query now
also asserts exact(id,grp) pairs[(2,2),(7,2),…,(42,2)] and positive spilled bytes.

First broad gate fails compilation due to a missing QueryError import in the new
helper; corrected before rerunning. Gate50440 is terminal101 under48GiB/Rayon4/one build job:998 library tests
pass/11 ignored, native10/10 pass and30 ownership tests pass. Cargo stops at
spill_tests (6 pass/7 fail); the remaining two ownership tests pass separately
in18286. Native completion now includes the independent typed oracle and real
spill assertion. No
performance or complete resource-acceptance claim yet. Logs: join-index-gates-*.log
under the existing parallel-aggregate-input scratch directory.

## Baseline audit and next measurement

An isolated copy of all493 hash-verified8670b94c source inputs was built with the
same compiler/features/Rayon setting and the same data fixture, referenced through
a read-only data symlink. Baseline13757 also fails the same seven spill tests
(6 pass/7 fail). No previously passing spill test regresses in this comparison.
The exact resource errors are not all identical: isolated manifest paths change
metadata sizes, and agg_spill reaches a different denial boundary after the repair.
Do not interpret same failed-test names as proof that every underlying issue is
identical or resolved. Raw logs and normalized failure details are preserved.

Unresolved spill cases: aggregate-over-join, ordinary join, outer join, distinct
count, semi join, anti join and filtered semi/anti. The latter three use8KiB;
others use256KiB. All remain failures, not skips or reclassified passes.

Formatting and whitespace checks pass. The native regression is closed for this
fixture; full query-wide memory admission remains open. Release95682 completed successfully in8m43s
under48GiB/one build job. Frozen binary476ec119ea06b591767a09e2cdb7107aec433b48a15951b8dd23032c3ff602a8
has493 verified source inputs. Paired comparison26121 completed with exit1 against8670b94c
across nine canonical queries including Q9, with four fresh-process blocks,
six measured pairs per block,16 threads,4GiB query and12GiB process limits. No performance
improvement is claimed for the index/spill change before that comparison.

Completed repair evidence: `benchmarks/2026-09-08-join-index-tests/`. It preserves
the initial sparse regression, compile-error log, all current/baseline gates, exact
before/after source, hashes and the continuing release status at archive time.

## Local upstream comparison

Read-only inspection during the frozen-binary comparison uses DuckDB snapshot
`1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8` and ClickHouse snapshot
`a1b25f3f4beb3ba49aa3b73671cc244185331b86`; these are local revisions, not a claim
about the installed DuckDB 1.4.4 benchmark wheel. No upstream code was copied.

DuckDB `src/execution/operator/join/perfect_hash_join_executor.cpp`,
`CanDoPerfectHashJoin`/`BuildPerfectHashTable`, gates its direct representation
by integer equality shape and a bounded key range, then allocates output-column
dictionaries and a validity bitmap indexed by that range. Its build scan checks
actual duplicate occupancy and can reject the specialization. This representation
is different from our chained direct index, which retains duplicate matches.
Copying a range threshold or inferring uniqueness from statistics would not
provide our memory or SQL contract.

ClickHouse `src/Interpreters/HashJoin/HashJoin.cpp::getTotalByteCount` combines
stored-data allocation, NULL maps, arena storage and hash-map byte counts.
`src/Interpreters/GraceHashJoin.cpp::addBlockToJoinImpl` checks the constructed
hash join's rows/bytes after adding the current block and repartitions on overflow.
This illustrates why payload-only costing is incomplete. The inspected code is a
soft-limit/repartition policy; it is not evidence of pre-allocation admission or
an exact query-wide cap, and should not be ported as such.

Our next resource work must retain actual ownership reservations across the build
handoff, include evaluated keys and auxiliary structures, and leave sufficient
admitted working space for spill/reload. The current index bound is one repair
within that larger contract. The historical Photon/reservation prose still present
above `finish_via_spill` overstates what estimate-and-observe code guarantees;
current executable checks and this qualified contract take precedence.

## Paired canonical result

Comparison26121 is terminal1: all36 reference calibrations are valid;358 completed
engine outputs pass typed validation,353 pass the unchanged timing gate. Five
completed outputs are late,17 requests time out and129 dependent requests are
not run. No scope OOM/max events; scope peak2777362432bytes is not exact engine RSS.
Q9, Q12 and Q13 lack all four complete paired blocks and receive no ratio.

| Query | After/before ratio | Whole-block bootstrap 95% interval |
|---|---:|---:|
| Q1 | 1.00742 | 0.99972–1.02027 |
| Q2 | 0.99809 | 0.89892–1.07461 |
| Q5 | 0.99160 | 0.97350–1.01003 |
| Q10 | 0.99475 | 0.97997–1.00976 |
| Q19 | 0.98811 | 0.98085–0.99311 |
| Q20 | 0.98723 | 0.95179–1.02398 |

Q19 improves1.19% (95%0.69–1.92%); other complete intervals include no change.
All complete-query upper bounds are below1.10. This does not clear failed queries,
establish a suite score, or satisfy the5% component objective. The native256KiB
correctness/spill regression is repaired; broad performance leadership is open.
Raw samples, startup/execution order, independent oracle hashes, plans, source,
build and failed gates are preserved in `benchmarks/2026-09-08-join-index-pairs/`.

Follow-up53182 is terminal1, with supplemental23286 terminal0: the same476ec119 candidate runs raw Parquet, native,
Iceberg and Lance sequentially at16threads/4GiBquery/12GiBprocess, three samples
per canonical SF10 query, under48GiB containment. This is a provider screen, not
multi-session concurrency or full resource certification. Other residency tracks
still require separate validation on this candidate.

The completed provider screen validates all322 completed measured/warmup outputs;
240/264 pairs meet all gates. Six warmup timeouts and two reference failures remain.
See [provider evidence](join-index-provider-screen-2026-09-08.md).
