# Direct fixed-state update comparison

Frozen candidate ea1e9019 removes the general-state roundtrip from each fixed-cell
update. Performance is not yet established. Control is c20b0648, the pre-compaction
incremental-native-reader candidate, so the test must recover the compact
prototype's regressions rather than merely improve on a slower intermediate.

## Frozen source and assembly

Release99456 completed with exit0 in8m52s. SHA256:
`ea1e9019fab3615c5910f7c9d77c74b6b85c2071e1afdf6cb937f7d72f31f555`.
519source inputs verify before/after the locked/offline lance,gpu release build,
48GiB cap, one build job. Source/tests are archived in
[direct-update correctness archive](benchmarks/2026-09-10-direct-fixed-updates/manifest.json).

`inspect_direct_fixed.py` verifies the binary hash and disassembles the named
StateRows::prepare_arrays_indexed symbol, address0x5fce580, size0x19a7.
There is no FixedCell::try_from or general AccumulatorState::update call in that
function's call list. The preceding compact prototype had an out-of-line conversion.
The function is larger (previous size0x17c6); absence of the call alone does not
prove a throughput improvement. Arrow input inline/resolve calls remain.

## Completed comparison

Paired52198 is terminal0:64independently typed-correct outputs,1820complete join
traces, no unwind, and all source/binary/data/harness guards verified. Scope peak
32478322688bytes, swap disabled, zeroOOM/max events. Two reversed blocks are
only a diagnostic; they do not establish a confidence interval or full acceptance.

The design runs Q1/Q9/Q17/Q18 in raw/native16thread and decoded resident16/4thread
modes, default disjoint ownership, GPU0, CPU0–15 or0–3. Raw/native4/12GiB;
resident32/48GiB with preload excluded. The180second diagnostic watchdog is not
matched DuckDB10times acceptance. Full provider/resource/concurrency gates remain
open and cannot inherit certification from earlier binaries.

## Results

Ratios below1favor candidate; all individual samples are retained.

| Mode/query | Control ms | Candidate ms | Ratio of means | Block ratios |
|---|---|---|---:|---|
| raw_parquet/q01 | 615.444, 633.441 | 756.596, 616.088 | 1.099127 | 1.229350, 0.972605 |
| raw_parquet/q09 | 1555.601, 1514.556 | 1546.074, 1586.080 | 1.020193 | 0.993875, 1.047224 |
| raw_parquet/q17 | 1831.217, 1841.694 | 1828.962, 1844.777 | 1.000225 | 0.998769, 1.001674 |
| raw_parquet/q18 | 1063.532, 983.514 | 948.552, 955.134 | 0.929967 | 0.891889, 0.971144 |
| native/q01 | 11504.753, 11485.162 | 10347.210, 10315.171 | 0.898758 | 0.899386, 0.898130 |
| native/q09 | 2658.142, 2638.099 | 2565.303, 2544.800 | 0.964855 | 0.965074, 0.964634 |
| native/q17 | 1318.751, 1371.136 | 1281.424, 1297.062 | 0.958585 | 0.971695, 0.945977 |
| native/q18 | 4764.866, 4859.384 | 4526.640, 4531.956 | 0.941226 | 0.950004, 0.932619 |
| cpu_resident_32g/q01 | 11092.949, 11028.412 | 10034.193, 9948.125 | 0.903304 | 0.904556, 0.902045 |
| cpu_resident_32g/q09 | 1603.852, 1578.048 | 1581.925, 1527.738 | 0.977298 | 0.986328, 0.968119 |
| cpu_resident_32g/q17 | 1071.599, 1101.313 | 1055.123, 1035.796 | 0.962266 | 0.984624, 0.940510 |
| cpu_resident_32g/q18 | 4799.875, 4658.710 | 5491.639, 5347.280 | 1.145935 | 1.144121, 1.147803 |
| cpu_resident_4t_32g/q01 | 14020.797, 13938.264 | 12191.089, 12259.118 | 0.874500 | 0.869500, 0.879530 |
| cpu_resident_4t_32g/q09 | 2400.697, 2337.655 | 2335.920, 2374.552 | 0.994116 | 0.973018, 1.015784 |
| cpu_resident_4t_32g/q17 | 1286.738, 1298.559 | 1284.194, 1261.827 | 0.984808 | 0.998023, 0.971713 |
| cpu_resident_4t_32g/q18 | 5695.090, 5742.200 | 6081.837, 5822.321 | 1.040820 | 1.067909, 1.013953 |

NativeQ1 improves10.12%, resident16threadQ1 improves9.67%, and resident4threadQ1
improves12.55%. Nevertheless resident16threadQ18 regresses14.59%, with both blocks
slower by about14–15%. This remains a provisional candidate, not an accepted
performance improvement. Resident4threadQ18 is4.08%slower on average.

RawQ1 has opposing blocks:22.94%slower and2.74%faster, yielding9.91%higher mean
time. Preserve that uncertainty rather than calling it a reproduced source-level
regression or discarding the first sample. In its first block the cumulative worker
scan, expression and state phases all increase; this is not specific attribution
to the compact-row path. RawQ1 uses the existing morsel aggregation route.

## Memory and next attribution

native Q18 reservation high-water bytes: control[2165749709, 2165773005]; candidate[1557599521, 1557599521].

cpu_resident_32g Q18 reservation high-water bytes: control[2721733692, 2721750076]; candidate[2184878957, 2184895341].

cpu_resident_4t_32g Q18 reservation high-water bytes: control[2721562294, 2721529526]; candidate[2184674791, 2184674791].

First resident16threadQ18 block: routing1077.994/1064.877ms,
processing2115.809/2872.384ms, ingestion3200.676/3943.818ms,
finish1014.771/961.234ms (control/candidate). Input59,986,052rows/916batches,
4disjoint workers and7323parallel batches, no spill. Do not sum overlapping phase
intervals or treat reservations as process RSS.

Frozen disassembly also shows StateRows::begin using a general Clone call in
control and dynamic-length memcpy in candidate. That difference is a follow-up
hypothesis, not the demonstrated cause. A contained owned-child GDB comparison
will begin sampling at the first state-row preparation entry, after resident
preload, to locate the remaining ingestion cost. No source changes are justified
solely by the removed conversion call or the smaller cell size.

Reproduction drivers: build_direct_fixed.py, run_direct_fixed_paired.py,
summarize_direct_fixed_paired.py, inspect_direct_fixed.py and
archive_direct_fixed_paired.py in `.scratch/parallel-aggregate-input`.
