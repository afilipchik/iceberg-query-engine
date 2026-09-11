# Shared CPU regression diagnosis, 2026-09-06

Frozen b2cdf candidate versus accepted scalar control, canonical raw SF10, main pools1and16,3steady paired samples plus one warmup. All64engine requests pass typed/time gates; every steady sample has complete process CPU boundaries. This is diagnostic timing with5ms process sampling, not acceptance latency.

| Query | Control wall1→16 ms | Candidate wall1→16 ms | Control CPU16 ms | Candidate CPU16 ms |
|---|---:|---:|---:|---:|
| q01 | 6843.8 → 813.6 | 5730.2 → 697.2 | 12750 | 10850 |
| q10 | 2898.8 → 515.0 | 2839.6 → 826.4 | 5880 | 5610 |
| q12 | 1321.0 → 189.0 | 1218.4 → 894.6 | 2390 | 1470 |
| q16 | 590.9 → 325.2 | 591.2 → 504.3 | 1900 | 1090 |

Q1 reduces CPU work at both pool sizes and retains about8×wall scaling. Q10/Q12/Q16 are near parity or faster with one main worker but lose scaling at16. Q12 drops from6.99×to1.36×, Q10 from5.63×to3.44×, Q16 from1.82×to1.17×. Their candidate CPU work at16is lower, while elapsed time is higher. This supports lost execution overlap as the primary regression mechanism, rather than added arithmetic work.

Source review independently identifies raw variable-width scan/gather preparation barriers and the Filter subquery preparation guard. The measurements do not identify a unique queue instance; do not infer exclusive operator costs from nested timings or loosen memory contracts based on this diagnosis. The [boundary review](next-shared-cpu-boundary-profile-2026-09-06.md) identifies precise follow-up instrumentation.

Q16 is NOT a strict single-thread case: even mainpool1has20observed live OSthreads due to an independently sized nested-subquery runtime. Atmainpool16control/candidate peak sampled live threads differ65/50. Live threads are not concurrently active CPU threads; process CPU and wall are the relevant work/overlap evidence. Counter boundaries can include pipe scheduling/serialization delay, tick quantization and sampled RSS is not exact peak.

The completed IPC contrast below covers Q10/Q12/Q16 under the same diagnostic. RawQ12has a source-proven variable-width preparation barrier; decoded resident input can distinguish it from a common join-kernel cost. Separately design an initialized immutable, budgeted uncorrelated-subquery predicate capability, proving correlation/NULL/error/lifetime contracts before any Filter guard changes. Q1benefits are already measurable; repeat them only if needed for causal isolation.

[Full source and CPU/GPU evidence](benchmarks/2026-09-06-expression-substitution/README.md). Raw diagnostic archive verified214files, SHA fc1fa246f6df5a4e7a2cfef97ec26f8b2db66e439afcb04ce8d47ccdf51f505e. IPC contrast2589 completed successfully; all48requests passed typed/time validation and complete CPU boundaries. Archive7297 verified162files, SHA4287f43519f0c90adf6bf729ccd6b24fb50f4f0d1b703f9c483dc8eab0630073.

## Completed decoded IPC contrast

| Query | Control wall 1→16 ms | Candidate wall 1→16 ms | Control CPU16 ms | Candidate CPU16 ms |
|---|---:|---:|---:|---:|
| Q10 | 1427.5 → 378.5 | 1521.3 → 486.5 | 3800 | 3820 |
| Q12 | 1221.1 → 145.7 | 1235.6 → 138.8 | 1990 | 1950 |
| Q16 | 454.9 → 281.7 | 385.3 → 390.6 | 1070 | 960 |

Q12 candidate scaling recovers to8.90× with decoded resident input versus1.36× raw. Q16 remains effectively serial (0.986×), while its control scales1.615×. This separates the raw scan/gather barrier from the subquery-filter barrier; it does not by itself prove which prepared descriptor can safely admit parallel pulls. A generic initialization-only probe now checks the actual descriptor, retained build memory and inferred envelope fit. Returned output streams are never polled.

## Actual initialized capabilities

The default-feature debug diagnostic uses the same585 production sources plus the new generic `prepared_plan_probe` example. All585 source hashes were rechecked before the later runtime-filter arithmetic fix. Each selected path runs in a fresh process with16main workers, affinity0–15,40GiB context pool,48GiB process cap and96GiB outer cgroup. It never polls returned output streams; preparation can execute build inputs. This is capability evidence, not latency comparison.

| Selection | Raw | Decoded IPC |
|---|---|---|
| Q16 Filter | None | None |
| Q16 child Inner | Layouts,16streams,421534bytes per copied output | Layouts,16streams,1336958bytes |
| Q12 Inner | None | Layouts,16streams,927982bytes |

The inferred16-slot Q16 envelopes are6744544bytes raw and21391328bytes IPC, both below available context-pool bytes. No queue reservation or actual slot selection was attempted. These descriptors confirm the distinct preparation barriers suggested by the scaling experiment.

Every held join descriptor reports zero current pool reservations despite retained initialized build state. Source inspection confirms `BuildSideCache` has no reservation owner and the spillable join’s retained prefix is tracked with `observe`, not admission. This is evidence of the existing retained-state accounting gap, not proof that initialization used no memory. It prevents treating envelope feasibility as complete query-memory certification.

Next implementation must preserve this distinction: immutable budgeted predicate initialization can remove future subquery admissions, but it cannot repair retained join-build ownership by itself. Unknown providers, correlation, volatility, error order and shared terminal-error lifetime need positive contracts before Filter’s guard can change.
