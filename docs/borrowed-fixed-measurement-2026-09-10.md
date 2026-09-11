# Borrowed fixed-input matched comparison

Frozen fde271b1 is the follow-up to the direct-update candidate's unresolved
residentQ18 regression. Performance is pending; source-level simplification and
correctness tests alone do not certify a speedup.

## Frozen source and generated code

Release78412 completed with exit0 in8m49s,519source inputs verified before/after.
SHA256 `fde271b12025f33836f2e43c300e909f3ccedbea6b233ec8705f4f7d8fbfe2f6`.
Locked/offline lance,gpu release build,48GiB cap, one build job, repository TMPDIR.
The16-file correctness archive includes the full source and tests:
[manifest](benchmarks/2026-09-10-borrowed-fixed-input/manifest.json).

Hash-checked inspection finds no FixedCell::try_from or owning arrow_input::inline
call in StateRows::prepare_arrays_indexed. The borrowed decoder is inlined; checked
resolve/downcast calls remain. The function grows from0x19a7 to0x21b1 bytes, so
instruction footprint still needs runtime validation. PreparedGroup::commit now
loads the contained row fields directly, removing the previous Option extraction
and vector token-copy sequence. Its stack reservation falls from0x68 to0x38 bytes.
This confirms the intended code transformation, not the performance hypothesis.

## Completed measurement

Paired51623 is terminal0:64independently typed-correct outputs,1820complete join
traces, no unwind and all source/binary/data/harness guards verified. Scope peak
31357698048bytes, swap disabled and zeroOOM/max events. Two reversed blocks are
only a diagnostic; they do not establish a protected confidence bound or full
provider, resource or concurrency acceptance.

Control is c20b0648, the pre-compaction baseline. Same Q1/Q9/Q17/Q18,
raw/native16thread and decoded resident16/4thread cases, default disjoint ownership,
GPU0, CPU0–15 or0–3. Raw/native4/12GiB; resident32/48GiB, preload excluded.
The180second diagnostic watchdog is not matched DuckDB10times acceptance.

## Results

Ratios below1favor candidate. All individual samples remain archived.

| Mode/query | Control ms | Candidate ms | Ratio of means | Block ratios |
|---|---|---|---:|---|
| raw_parquet/q01 | 618.130, 616.446 | 613.033, 639.233 | 1.014329 | 0.991754, 1.036965 |
| raw_parquet/q09 | 1433.054, 1560.296 | 1423.889, 1510.192 | 0.980200 | 0.993605, 0.967889 |
| raw_parquet/q17 | 1825.837, 1835.074 | 1827.935, 1847.548 | 1.003980 | 1.001149, 1.006797 |
| raw_parquet/q18 | 989.137, 988.991 | 944.108, 956.511 | 0.960817 | 0.954477, 0.967158 |
| native/q01 | 11511.965, 11303.527 | 8491.421, 8368.347 | 0.738961 | 0.737617, 0.740331 |
| native/q09 | 2508.856, 2601.703 | 2571.169, 2845.549 | 1.059907 | 1.024837, 1.093725 |
| native/q17 | 1326.975, 1313.372 | 1341.658, 1308.574 | 1.003744 | 1.011065, 0.996347 |
| native/q18 | 4828.349, 4770.920 | 4223.157, 4310.426 | 0.888983 | 0.874659, 0.903479 |
| cpu_resident_32g/q01 | 10948.000, 11104.169 | 8113.788, 7925.108 | 0.727316 | 0.741121, 0.713706 |
| cpu_resident_32g/q09 | 1600.720, 1563.123 | 1588.928, 1566.222 | 0.997252 | 0.992633, 1.001982 |
| cpu_resident_32g/q17 | 1065.989, 1085.990 | 1065.133, 1058.792 | 0.986964 | 0.999197, 0.974956 |
| cpu_resident_32g/q18 | 4743.834, 4686.747 | 4542.144, 4519.866 | 0.960917 | 0.957484, 0.964393 |
| cpu_resident_4t_32g/q01 | 14006.824, 14102.608 | 10385.432, 10429.338 | 0.740491 | 0.741455, 0.739533 |
| cpu_resident_4t_32g/q09 | 2498.863, 2365.984 | 2383.457, 2308.622 | 0.964487 | 0.953817, 0.975756 |
| cpu_resident_4t_32g/q17 | 1295.679, 1291.600 | 1296.152, 1277.693 | 0.994808 | 1.000365, 0.989233 |
| cpu_resident_4t_32g/q18 | 5685.482, 5688.940 | 5620.234, 5430.673 | 0.971558 | 0.988524, 0.954602 |

NativeQ1 improves26.10%, resident16threadQ1 improves27.27%, and resident4threadQ1
improves25.95%. ResidentQ18 now improves3.91%at16threads and2.84%at4threads,
recovering the preceding compact candidate's observed regression. NativeQ18
improves11.10%. The direction agrees across both blocks for these cases.

NativeQ9 is5.99%slower on average (2.48%and9.37%slower blocks); preserve this
concern. RawQ1 has opposing small blocks and1.43%higher mean time. These are not
proof of zero regression or suite leadership. Do not promote based only on the
four-query screen; run the complete matched provider suite before more edits.

## Memory and attribution

native Q18 reservation high-water bytes: control[2165740641, 2165737825]; candidate[1557599541, 1557599541].

cpu_resident_32g Q18 reservation high-water bytes: control[2721766477, 2721766477]; candidate[2184878975, 2184862591].

cpu_resident_4t_32g Q18 reservation high-water bytes: control[2721529543, 2721562311]; candidate[2184674809, 2184691193].

control first resident16threadQ18 profiles:

```text
live_aggregate_workers workers=4 ownership=disjoint routing_ms=1070.122 processing_wall_ms=2075.118 serial_batches=0 parallel_batches=7323
live_aggregate_profile input_rows=59986052 input_batches=916 evaluation_ms=5.826 ingestion_ms=3151.688 finish_ms=1011.739 output_ms=969.823 output_rows=624 output_batches=612 spilled_bytes=0
```

candidate first resident16threadQ18 profiles:

```text
live_aggregate_workers workers=4 ownership=disjoint routing_ms=1064.519 processing_wall_ms=1925.650 serial_batches=0 parallel_batches=7323
live_aggregate_profile input_rows=59986052 input_batches=916 evaluation_ms=6.066 ingestion_ms=2996.699 finish_ms=954.800 output_ms=954.591 output_rows=624 output_batches=612 spilled_bytes=0
```

Reservation high-water marks are not process RSS, and overlapping worker/phase
intervals must not be summed. This experiment tests the combined compact state,
direct updates and borrowed input/token boundary against the pre-compaction source.
The preceding negative candidates and debugger evidence are preserved separately;
the measurements do not assign an exact saved-time fraction to each code change.

Reproduction in `.scratch/parallel-aggregate-input`: build_borrowed_fixed.py,
inspect_borrowed_fixed.py, run_borrowed_fixed_paired.py,
summarize_borrowed_fixed_paired.py and archive_borrowed_fixed_paired.py.
