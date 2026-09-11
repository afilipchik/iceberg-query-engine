# Frozen587 benchmark outcome — 2026-09-06

The retained join-index ownership candidate is correct on the measured workloads, but **fails performance acceptance in every CPU mode**. This is a one-session development screen, not completed resource, concurrency or leadership certification.

## CPU results

All 880 paired engine requests pass typed comparison and the fresh DuckDB 10× time gate. Ratios below use suite sums of query medians; lower is better. The control is the preserved accepted scalar binary, not the immediately preceding candidate.

| Mode | Candidate / scalar control | Candidate / matched DuckDB |
|---|---:|---:|
| Raw Parquet | 1.0771 | 2.4181 |
| Decoded IPC | 0.9698 | 0.4941 |
| Native | 1.0024 | 3.7201 |
| Iceberg | 1.0715 | 0.3243 |
| Lance | 0.9829 | 1.2805 |

Every mode contains protected query regressions above 10%. Raw Q10/Q12/Q16 are 1.6074×/4.6301×/1.5330× the scalar control. Favorable decoded IPC or Iceberg comparisons apply to their documented provider/residency boundaries; they do not establish raw or native CPU leadership. All query ratios and geometric means are in [the CPU summary](benchmarks/2026-09-06-join-index-ownership/cpu-screen-summary.json).

## GPU execution, separately verified

Canonical SF10: 66 GPU-enabled and 66 same-binary CPU-control samples pass typed/time gates. **Zero device runs and zero uploads** were recorded in both tracks. Their respective suite/DuckDB ratios are 2.3499 and 2.3789; these measure CPU execution with routing enabled or disabled.

The supported 600,000-row floating-point fixture passes 40 GPU-enabled and 40 CPU-control samples. Counters record **39 device runs**, one upload and one CPU fallback among GPU-enabled samples. Q1 iteration 1 is the fallback: its captured trace reports data not yet resident and requests an upload; all CPU-control samples record zero device runs. GPU-enabled suite/DuckDB ratio is 0.0709 on this separate fixture. Do not characterize all 40 GPU-enabled samples as device executed or extrapolate this small supported workload to canonical decimals. VRAM hard admission and full residency certification remain open. [Counters and summaries](benchmarks/2026-09-06-join-index-ownership/gpu-execution-summary.json).

## Correctness and resource evidence

Selected default-feature gates pass 677 unique tests, with one pre-existing ignored test; the dedicated IPC test ran explicitly. The release was built with Lance and GPU features, which does not imply full feature-specific test coverage. Six cap scenarios complete with actual spill and reported peak RSS 126–277 MiB. Aggregate/sort have independent exact oracles; the join cap oracle checks row count only. Effective limits are 1 GiB cgroup and 2 GiB RLIMIT_DATA.

The retained join index reserves 11,561,864 bytes in both raw and IPC SF10 probes and releases the reservation after final plan/context drop. This covers private heads/next/entries capacities, not all retained build data or the query-wide working set. Index admission pressure refuses cleanly; a new spill transition is not implemented.

## Source and reproducibility

Frozen source archive: 587 files, SHA256 `cb685e61372454ccebc6c6a27c12f3f77c3d97e87c0d6e1f1bd5de42d47262dd`. Benchmark binary: `2707f44f53198ed4107afee1aace8cc57d3ed2837e23cbd6fa48a704899e8c46`. CPU archive verifies 2,891 files and both decompressed binaries; GPU archive verifies all members and original files. [Evidence directory](benchmarks/2026-09-06-join-index-ownership/README.md) preserves commands, plans, samples, oracles, logs and verification manifests.

The sole post-freeze source-manifest change at this checkpoint corrects the cap driver's displayed RLIMIT label. Archive validation proves the exact display-only difference and checks every other source hash. Original mislabeled logs remain preserved and qualified.

## Next implementation boundary

[Measured CPU diagnostics](shared-cpu-regression-diagnosis-2026-09-06.md) show lost parallel overlap despite lower CPU work. Raw Q12 and IPC Q16 have different barriers. The next connected slice prepares a budget-owned, immutable root-IN membership filter once, preserving all child partitions and exact SQL/error semantics. Eligibility must prove actual physical layouts; unsupported domains retain the existing generic path without claiming parallel admission. Scratch design is not yet production capability.

After focused semantic, cancellation and admission tests pass, probe the actual prepared descriptors, then compare the frozen candidate against both the previous candidate and scalar control. Raw variable-width output ownership, general build/result ownership, broader public workloads, concurrency, VRAM admission and full DuckDB leadership remain separate open gates. No query-specific thresholds or SQL recognition are authorized by these measurements.
