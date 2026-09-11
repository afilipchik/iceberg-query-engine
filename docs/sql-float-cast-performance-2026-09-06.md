# Shared CAST/float predicate release screen

The frozen predicate candidate passes 89 independent optimized oracle queries
(701 returned rows), including Float32/Float64/string casts, six comparison
operators, filters, constants, NULLs and date conversions. It also fixes the
65,536-row control's ordinary filter and post-join filter counts (52,429 expected
and actual). GROUP BY still returns 39,322 groups instead of 3; a separate key
contract repair is now drafted and is not included in these measurements.

Candidate binary SHA-256:
`2ee209618e5a8b730d21d0422e4f15255f8d4fa5b2b02dcd118496e29d37fb31`.
Source SHA-256:
`237e14d86e68c325f91a24e4c453e1d1189c1ff45e5bdfddb97c544958525e15` (609 files).
Control is the validated SUBSTRING/VALUES release
`ad506267227122efe6b67c426d113468e191fdb821f4d3ea8afe95e1fba20585`.

Each mode screens canonical SF10 Q6/Q12/Q14/Q19/Q22, ten steady pairs plus one
warmup per side, same 16-thread affinity, 40 GiB query budget, 48 GiB process cap
and 96 GiB cgroup, with no competing heavy job. Fresh matched DuckDB calibration
provides independent typed results and 10x query ceilings. All **330 requests**
validate; no measured requests are missing. This is not full-suite or multi-session
leadership acceptance.

| Mode | Suite candidate/control | Geomean | Q6 | Q12 | Q14 | Q19 | Q22 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Decoded IPC | 1.0380 | 1.0420 | 1.6467 | 1.0048 | 0.7965 | 0.9948 | 0.9370 |
| Lance | 0.9713 | 0.9826 | 1.2976 | 0.8783 | 0.7691 | 0.9710 | 1.0763 |
| Raw Parquet | 1.0090 | 1.0336 | 1.2502 | 0.9936 | 0.9227 | 0.9888 | 1.0407 |

Q14's improvement is consistent with eliminating repeated constant casts, but
Q6 regresses in every mode. The binary is not accepted. Q6's optimized plan changes
its two CAST date bounds to Date32 literals; the physical operator shape is
unchanged. IPC execution grows roughly 50 ms, so this is not simply additional
planning overhead. A ten-pair component comparison against the cast-only candidate
is running to separate constant normalization from the shared comparison changes.
That intermediate binary is known semantically incomplete outside this canonical
subset and is used only for attribution.

Drivers and results:
`.scratch/sql-float-comparison-repair/{run-pairs.py,paired-status.json,component-pairs.py}`;
`.scratch/public-bench/sql-float-cast-{decoded_ipc,lance,raw_parquet}-01/`;
`.scratch/sql-float-comparison-repair/release-oracle/`.
Required GPU validation and archival of this completed matrix remain follow-up work.

## Component attribution and GPU validation

The component comparison completed with 44/44 typed/time-valid requests. Q6 grows
from 81.9662 ms in the cast-only release to 132.2455 ms after the SQL float repair
(1.6134x), while Q14 is unchanged at 67.8177/67.3688 ms (0.9934x). Thus the date
normalization itself is not the Q6 regression. Source confirms that fractional
SQL literals bind as Float64; Q6's decimal discount bounds coerce into the float
comparison path. The new row-wise `Option<bool>` builder is a shared bottleneck.
Current source replaces it with direct packed Boolean bitmap construction and
separate NULL-buffer handling, hoisting operator and scalar shape out of the row
loop. This newer code is in testing and has not been measured yet.

Required GPU validation on the separate 600,000-row float fixture also completed:
40 measured device samples each report exactly one completed device execution,
zero failures/fallbacks and a verified 268,435,456-byte cache target. The matching
CPU control has 40 valid samples. GPU/DuckDB suite ratio is 0.09988; CPU control
is 0.96129. These are this small fixture's timings, not canonical GPU SF10 or hard
VRAM certification. All three provider screens, component screen and GPU jobs
are terminal. Production source is newer by grouping/key and bitmap repairs.

[Verified evidence archive](benchmarks/2026-09-06-sql-float-cast/evidence.tar.gz)
contains 1,071 checked members, including exact source/binary, all provider/component
samples, independent release oracles and GPU validation. Archive SHA-256:
`a70f319d1328f2a85f6be8b20490180452b4516f08d5de62796bd31855bc5a9d`.
The bitmap/key follow-up source passes 735 selected test executions but has not
yet been measured; retain the earlier regression as an unresolved acceptance gate.
