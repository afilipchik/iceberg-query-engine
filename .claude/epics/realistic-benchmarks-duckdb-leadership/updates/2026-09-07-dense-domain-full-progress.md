# Frozen 653/662 full SF10 progress

This summarizes immutable prefix audits, not a new Arrow oracle run or a
performance acceptance decision. Frozen binaries, data and timing boundaries
are unchanged. The actual process must be polled separately; this document
does not infer that a process is live from status files.

Audited cells: **20/20**. Recorded requests: **9680**.
Audit: `.scratch/dense-domain-repair/balanced-audit-20.json`, SHA256 `157cecd33c313bc1ddca931e357b3cc629311b2283128e4afb305a4be63eb02e`.

| Track | Startup / offset | Requests | Suite 662/653 | Geometric mean | Suite 662/DuckDB | Flags >10% |
|---|---|---:|---:|---:|---:|---|
| decoded_ipc | before-first / 0 | 484 | 0.959145 | 0.989587 | 0.442269 | None |
| raw_parquet | before-first / 0 | 484 | 0.995191 | 0.995160 | 2.344383 | None |
| native | before-first / 0 | 484 | 0.958845 | 0.988550 | 3.367155 | None |
| iceberg | before-first / 0 | 484 | 1.000592 | 0.995876 | 0.316298 | None |
| lance | before-first / 0 | 484 | 0.946276 | 0.968229 | 1.257866 | q20: median 1.113272, mean 1.001293 |
| decoded_ipc | after-first / 1 | 484 | 0.946858 | 0.978243 | 0.438259 | None |
| raw_parquet | after-first / 1 | 484 | 0.995957 | 0.992232 | 2.343934 | None |
| native | after-first / 1 | 484 | 0.956493 | 0.982460 | 3.386750 | None |
| iceberg | after-first / 1 | 484 | 1.001360 | 0.998355 | 0.320265 | None |
| lance | after-first / 1 | 484 | 0.977551 | 0.984959 | 1.273819 | None |
| decoded_ipc | after-first / 0 | 484 | 0.946004 | 0.978837 | 0.424358 | None |
| raw_parquet | after-first / 0 | 484 | 0.995323 | 0.986914 | 2.330166 | None |
| native | after-first / 0 | 484 | 0.965449 | 0.985526 | 3.502324 | None |
| iceberg | after-first / 0 | 484 | 0.993684 | 0.987472 | 0.315648 | None |
| lance | after-first / 0 | 484 | 0.988980 | 0.988683 | 1.259709 | None |
| decoded_ipc | before-first / 1 | 484 | 0.941330 | 0.978364 | 0.401801 | None |
| raw_parquet | before-first / 1 | 484 | 0.987597 | 0.983893 | 2.307491 | q22: median 1.060306, mean 1.108418 |
| native | before-first / 1 | 484 | 0.962428 | 0.982992 | 3.476937 | None |
| iceberg | before-first / 1 | 484 | 0.985616 | 0.992835 | 0.314349 | None |
| lance | before-first / 1 | 484 | 0.944435 | 0.975642 | 1.228964 | q02: median 1.013912, mean 1.102366, q11: median 1.143053, mean 1.238031 |

Only complete cells above have all recorded typed comparisons and timing gates
verified by the prefix auditor. Every flag remains visible; absence in another
cell does not erase it. The native Q1 diagnostic control timeout and historical
647/649 Lance Q17 regression remain separate unresolved evidence.

IPC is extra and excluded from required CPU-track weighting. Native/raw/Iceberg/
Lance scores must remain separate. The Lance reference disables its known-incorrect
extension optimizer; it is direct scan plus DuckDB aggregation, not stock
extension pushdown performance. These development orderings do not certify
leadership, lower-budget completion, concurrency, public workloads or holdouts.

A shared decimal precision-bound patch is applied in the working tree, compiles
and has59 selected passes. A fixed256KiB spill-completion test fails with both
helper versions; see `.scratch/decimal-precision-bound-repair/status.json` and
the linked fused-budget finding. Optimized helper performance remains pending.
It is not part of the frozen binaries measured here.
The full and both protected processes are verified terminal. Each protected
follow-up validates1632 requests with no per-order mean/median ratio above1.10.
The two consuming-source spill-transition regressions are red; see the main
execution checkpoint and saved implementation contract for next actions.
