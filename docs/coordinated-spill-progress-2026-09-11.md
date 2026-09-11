# Coordinate aggregate peers before spill compaction

The copied queue cap alone left the original decimal integration failing in merge
and repartition FrameScratch allocation. Trace27619 identifies that phase while
other aggregate owners retain resident groups. A direct four-owner fixture passes
with120KiB retained-input pressure (32213) but fails at136KiB (39463), requesting
512bytes at262135used under the same256KiB query budget.

Current multi-owner ingestion saves an admitted progress entry per worker. Each
worker applies selected rows until completion, a group-count boundary, or typed
memory pressure. The progress record carries the exact next unapplied selection
position; the caller retains evaluated arrays, prepared keys and original routes.
Non-memory errors remain terminal. Rayon joins all started work before coordination.

When any worker needs spill progress, the parent publishes every nonempty peer's
resident partial state using already-prepared writers, then releases all resident
working sets and unused writers. It prepares/compacts parked writers before
recreating resident state. Workers resume their own saved positions; completed
workers do not reapply rows. Group ownership and partition routing stay fixed.
If no resident groups remain to spill, the original typed pressure is returned
instead of looping or replaying input. The existing single-worker path remains.

Corrected focused24294 passes the independent exact-decimal peer regression.
Initial55339 was a compile error requiring an explicit closure result type, not a
behavioral test. Original decimal integration11636 is terminal0 at unchanged256KiB
in default disjoint mode: the independent exact oracle and actual-spill check pass. Broader validation, parallel/partial coverage and
performance measurement remain open. This source is not yet a certified repair.

All execution uses the48GiB wrapper,swap0,one build job,locked/offline lance,gpu,
and repository TMPDIR. No dependency, SQL, data, test-budget or default-ownership
change. The pressure fixture's named input lease is released before final output;
the oracle requires all1000 exact decimal group sums, actual spill and cleanup.

Full both-mode validation23676 is active, freezing520source inputs. Formatting
and whitespace checks pass; no source changes during the validation run.

Full validation23676 is terminal1 with520 source inputs verified. Both modes pass
1092library tests/11ignored. Each contract family has81passes/1new failure:
prepared_frontier_concurrency_follows_available_admission, because the initial
half-pool copied queue cap serialized two6MiB slots under a16MiB pool. Existing
native/IPC failure names remain; legacy spill remains8passes/6failures each.
Systemic numeric now passes12/12 in disjoint mode, but partial remains11/12:
exact decimal refuses1016bytes at262112used/262144. Do not claim both-mode closure.

Current queue scheduling leaves half of available space, capped at a1MiB
working window. This heuristic preserves tiny-pool headroom without reserving
half of every large pool. Focused45421 passes all6 unchanged input-concurrency
integrations, restoring the two-slot case. Full validation of this correction
and partial-mode allocation attribution remain pending.

Partial trace27661 is terminal0 (debugger protocol) and reproduces1016bytes at
262112used in row_router::route_balanced, allocating63 selected-row indices for
one of16 workers. This occurs before aggregate rows are applied. The earlier
irreversible flag was set before routing, preventing unused-worker reclamation.
Current source sets it immediately before single-owner ingestion or after successful
multi-owner routing. Startup reclamation then retains the same evaluated arrays,
restores the global group limit, and retries only routing with fewer unused owners.
No source or expression is replayed. Partial decimal regression32808 is running;
corrected broad validation remains pending.

Partial32808 completes the1000-group query after delaying the ingestion flag, but
fails the old requirement that this now-fitting state must spill. Preserve that
result. The numeric fixture now retains1000 groups as a separate exact-typed
correctness case, and uses2000 groups for required actual spilling, still at256KiB
with20 batches and the independent integer coefficient oracle. This strengthens
disk coverage; it is a test-fixture change, not a budget increase or performance gain.
Focused partial97824 passes all3 exact-decimal cases, including nonzero spill.
Corrected full both-mode validation77936 is active; source/harness performance remains
unmeasured. The initial broad failure record is retained unchanged.

## Corrected validation outcome

Full77936 is terminal1 with all520 source inputs verified. Each mode passes1092
library tests/11ignored,82 contract integrations and13 systemic numeric tests.
The new concurrency failure is removed, and the exact-decimal failure is removed
in both modes with the fitting and strengthened actual-spill oracles. Remaining
failure names match008d92f7: native/IPC58passes/2dictionary-plan failures default,
57passes/3failures partial (same two assertions plus formatted float comparison);
legacy spill8passes/6failures each. Each mode also passes7focused decimal/parallel
spill tests. These remaining failures are not certified away.

Only five source/test files differ from008d92f7: input_frontier.rs and its tests,
ingestion_controller.rs, parallel_controllers.rs, and systemic_numeric_tests.rs.
No dependency, benchmark harness or ownership default change. Formatting/whitespace
pass. Optimized performance is still unmeasured for this source.

The [correctness archive](benchmarks/2026-09-11-coordinated-spill-progress/manifest.json)
retains exact final source, initial broad-validation source, reconstructed-and-hash-
verified partial trace source, all focused/broad outcomes and failure comparisons.
The partial trace remains a failing intermediate candidate, not final validation.

The50-file archive verifies520source inputs. Release56527 terminal0 in8m51s freezes a4103dfa
(full SHA256a4103dfad1f7c82c82255273e6331b4552c26f9617d4d996b739fe915db7737d).
Paired69904 terminal0 against008d92f7:64correct outputs/1820complete traces,
572verified files/520inputs. Small mixed timing changes remain; see the
[comparison](peer-spill-measurement-2026-09-11.md). No source or harness changes
during build or measurement.
