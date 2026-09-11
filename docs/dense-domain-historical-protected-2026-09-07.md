# Historical protected regressions: frozen647/662

The follow-up completed **1,632 requests across 12 cells**, all passing the
recorded typed comparisons and calibrated10×DuckDB query-time gates. Process73559
exited0. It covers every selected historical flag: decoded IPC Q22, Iceberg Q8,
and Lance Q11/Q17, with50 steady samples plus one warmup per engine in each of
four startup/execution orderings.

No mean or median candidate/control ratio exceeds1.10 in any ordering.

| Track/query | Geometric mean of median ratios | Geometric mean of mean ratios |
|---|---:|---:|
| Decoded IPC Q22 |0.995558|0.993189|
| Iceberg Q8 |0.996677|1.006528|
| Lance Q11 |0.947465|0.945663|
| Lance Q17 |1.052746|1.009231|

These ratios compare frozen662 with frozen647. They are separate from the
[current653/662 protected comparison](dense-domain-current-protected-2026-09-07.md).
Lance Q17 remains about5.3% slower by this descriptive median-ratio aggregation,
below the protected10% threshold. Its previous647/649 median regression of
1.164773 remains historical evidence. Several production changes separate647
and662; this comparison cannot assign recovery to one change.

The four orderings are not four independent sessions. No confidence interval,
statistical equivalence or DuckDB leadership follows. The Lance reference uses
its documented optimizer-disabled configuration. IPC is an additional residency
track, excluded from required-track weighting. Frozen binaries exclude the
subsequent decimal precision-bound and fused error-lifecycle edits.

The final audit is `.scratch/dense-domain-repair/historical-protected-01/audit-12.json`,
SHA256 `14b7a669d696297712aeb776e752dc429210939f693cc4d311dea88b72fd2f80`.
The adjacent `final-summary.json` preserves all order-specific mean/median ratios.
Both are also [preserved in the repository evidence directory](benchmarks/2026-09-07-dense-domain-protected/historical/).
The auditor checks membership, binary identities, calibrated ceilings and
recorded outcome arithmetic; it does not independently re-execute the Arrow oracle.

Drivers `run-historical-protected.py` and `audit-historical-protected.py`, logs
and prefix audits remain under `.scratch/dense-domain-repair/`. Raw outputs
remain under `.scratch/public-bench/dense-domain-historical-protected-*`.
Execution retained the96GiB wrapper, CPUs0–15,16 threads,40GiB query budget and
48GiB process cap. These artifacts are local; do not assume a clean checkout
contains the raw samples.

The [full SF10 report](dense-domain-full-sf10-2026-09-07.md) still shows raw and
native slower than DuckDB. The native Q1 diagnostic control timeout and the
low-budget spill regressions remain failures in their respective experiments.
Task007 and the parent workload/resource/performance requirements remain open.
