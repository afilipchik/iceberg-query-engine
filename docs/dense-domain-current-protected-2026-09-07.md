# Current protected regressions: frozen653/662

The longer comparison completed **1632 requests**, all passing typed comparison
and the fresh10×DuckDB time ceiling. It includes every flag from the full653/662
audit: Lance Q2/Q11/Q20 and raw Parquet Q22. Each has50 steady samples plus one
warmup per engine in each of four startup/execution orderings.

No query's mean or median exceeded1.10×control in any ordering. The original
short-run flags are preserved but are **not confirmed by this follow-up**.

| Track/query | Geometric mean of median ratios | Geometric mean of mean ratios |
|---|---:|---:|
| Lance Q2 |1.011066|1.017852|
| Lance Q11 |0.965655|0.964824|
| Lance Q20 |0.997135|0.998886|
| Raw Parquet Q22 |1.003659|1.005863|

These aggregate four order-specific ratios; samples are not pooled and the
orderings are not claimed as independent sessions. No confidence-interval,
statistical-equivalence or leadership gate is implied. The Lance reference
retains its documented optimizer-disabled qualification.

The process62607 exited0. The independent membership/timing auditor checked all
eight cells, calibrated ceilings, binary identities, warmup/sample membership,
typed outcome records and arithmetic. It reused recorded Arrow comparisons,
not a new oracle execution. Final audit SHA256:
`9d7df34a79ef30f4ed369e7628a63d302973fd52dc5be871bd4dd4ac1b0926cf`.
The final audit and descriptive summary are [preserved with hashes](benchmarks/2026-09-07-dense-domain-protected/current/).

Plan, commands, logs, immutable prefix/final audits and final descriptive summary
are in `.scratch/dense-domain-repair/current-protected-01/`; all raw cell outputs
remain under `.scratch/public-bench/dense-domain-current-protected-*`.
`run-current-protected.py` and `audit-current-protected.py` preserve selection and
verification. Runs use the existing96GiB wrapper, CPUs0–15,16 threads,40GiB query
budget and48GiB process cap. The frozen binaries exclude subsequent decimal and
fused-lifecycle edits.

The separate historical647/649 flags, particularly Lance Q17, are not resolved
by this comparison. Their [separate647/662 follow-up completed](dense-domain-historical-protected-2026-09-07.md)
with1632 validated requests and no mean/median ratio above1.10 in any ordering. The
[full SF10 results](dense-domain-full-sf10-2026-09-07.md), native Q1 component
timeout, low-budget spill transition, public workloads, concurrency and wider
resource/provider requirements remain separate evidence and open work.
