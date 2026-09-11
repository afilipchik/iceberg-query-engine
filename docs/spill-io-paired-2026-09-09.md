# Frozen spill-I/O protected comparison — 2026-09-09

Binary322c8042 adds admitted spill buffering to fe3cc8fe. Its
[Q18 diagnostic improvement](admitted-spill-io-2026-09-09.md) is distinct from this
balanced protected-query screen. Three source inputs differ,507 are frozen;
there is no dependency change.

## Conditions and outcome

Job8626 is terminal1: custom-memory child0, canonical child1. There are44 blocks
(36 canonical and8 custom), each with six measured pairs plus warmups. Startup
and query order are balanced. Every block has fresh, typed-correct DuckDB
calibration and a10× query ceiling. Canonical raw Parquet uses4GiB query/12GiB
process,16 threads and CPUs0–15; the separately labelled custom-memory float
workload uses4 threads. The harness records every sample, late output, timeout
and dependent not-run slot. This is a component comparison, not a complete suite.

All519 completed engine outputs validate;512 pass timing,7 are late. There are
8 timeouts and89 not-run slots. No reference block is invalid. Q9 times out on
both binaries; Q12 remains incomplete. The scope records no max/OOM/kill events
and a4,538,335,232-byte peak. Both binaries and507 source inputs verify after
execution. Full acceptance remains open.

| Workload/query | Candidate/control ratio | 95% block-bootstrap interval |
| --- | ---: | --- |
| Canonical Q1 | 1.00657 | 0.99723–1.01408 |
| Canonical Q2 | 1.01710 | 1.00227–1.03170 |
| Canonical Q5 | 1.00594 | 0.98364–1.02580 |
| Canonical Q9 | Incomplete | No valid suite comparison |
| Canonical Q10 | 1.00381 | 0.99361–1.01141 |
| Canonical Q12 | Incomplete | No valid suite comparison |
| Canonical Q13 | 0.99851 | 0.98722–1.02064 |
| Canonical Q19 | 0.99035 | 0.97604–1.00173 |
| Canonical Q20 | 1.00205 | 0.97204–1.03298 |
| Custom memory Q1 | 1.00731 | 0.96316–1.05347 |
| Custom memory Q6 | 1.00155 | 0.89744–1.16315 |

All complete canonical intervals have upper bounds below1.033. Q2 shows a small
measured slowdown; do not describe every protected query as unchanged. Custom Q6
is too uncertain to exclude a10% slowdown despite its near-one point estimate.
The original study is preserved and is not replaced by subsequent measurements.

## Prespecified custom Q6 follow-up

Before execution, spill-io-q6-followup-prespec.json fixes eight fresh blocks,
twelve measured pairs each, the same matched conditions and fresh reference
calibration,20,000 bootstrap samples and seed20260909. Acceptance requires all
outputs typed-correct/gated, all blocks complete, and the95% upper ratio no greater
than1.10. Follow-up16613 is terminal0. All208 outputs (192 measured plus16 warmups)
are typed-correct and gated. The ratio is0.96290 with95% interval0.88593–1.05178;
this follow-up bounds slowdown below10%, while the original wider interval stays
preserved. All507 source inputs and both binaries verify after execution. The driver cannot report success merely
because the lower interval fails to prove a slowdown.

The [paired archive](benchmarks/2026-09-09-spill-io-pairs/manifest.json) has1,191
verified files; the [follow-up archive](benchmarks/2026-09-09-spill-io-q6-followup/manifest.json)
has303 verified files. The incidental follow-up log in the paired archive also
matches its terminal log. Provider36745 is now running; additional provider,
residency, resource and concurrency gates still belong to this frozen candidate.
