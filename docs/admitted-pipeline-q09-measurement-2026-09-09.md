# Canonical Q9: admitted pipeline candidate still declines

Release82930 completed in 8m53s, freezing `3172f9e5` with 518 verified source
inputs. Diagnostic18871 completed successfully: all three outputs match the
independent typed DuckDB oracle. Default disjoint ownership, 16 threads pinned
to CPUs0–15, GPU disabled, one fresh process/query/provider. Raw/native query
budgets are4GiB, process12GiB; resident32/48GiB. The 180s watchdog is diagnostic,
not the required 10× DuckDB acceptance ceiling.

| Mode | Query ms | Declared partitions | Input slots |
|---|---:|---:|---:|
| Raw Parquet | 5894.372408 | 16 | 1 |
| Native | 2720.998609 | 8 | 1 |
| Decoded resident CPU | 2826.784982 | 16 | 1 |

All three frontier traces report `admitted_buffers=false`, `copy_bound=null`.
The prior isolated diagnostic was5947.815/2737.775/2829.978ms respectively;
these are separate single samples, not evidence of a performance improvement.
Scope peak19623858176bytes; memory.max48GiB, swap0, all OOM/max counters0.
After-run guards verify binary, dataset, driver, harness and all518source inputs.

Source inspection identifies an additional capability barrier:
`SpillableHashJoinExec::prepare_admitted_queue_input` accepts only Left/Right/Full,
so the planner's wrapper never delegates inner joins to the newly eligible
HashJoinExec. The physical Q9 tree contains that wrapper at every join.
The prepared two-block comparison was not launched because the intended routing
change did not occur. Next add a wrapper-level regression, delegate eligible
in-memory inner decisions, preserve spill fallback, and repeat the diagnostic.
This finding also shows why direct operator tests alone are insufficient.

[Implementation and correctness](admitted-computed-pipeline-2026-09-09.md).

## Additional source boundary found during corrected build

`streaming_parquet_scan/admitted.rs::prepare` explicitly declines when
`fixed_output.is_some()` or IPC directories are present. Q9's raw lineitem scan
reports a fixed-width copied-output bound. This is a source-level reason to
expect the corrected wrapper alone may still leave raw Parquet serial; the
corrected native/resident measurements remain necessary and independent.

A future general adapter must preserve both protocols rather than reinterpret a
copied-output bound as admitted ownership. A copied bound certifies the compact
copy, not the full retained extent of arbitrary borrowed Arrow buffers. Admission
must precede copy construction, and its lifetime must follow extracted arrays
through projection/join output. Prepared source streams must be used exactly once;
unsupported descendants, cancellation, late errors and insufficient metadata
headroom must not replay a source. The existing aggregate frontier's external
`InputAdmission::Copied` lease is scoped to its consumer batch and cannot simply
be advertised as buffer-owned admission through arbitrary downstream operators.
This adapter is a design requirement, not an implemented or measured fix.

The native physical tree ends in `NativeStreamingScanExec`, whose PhysicalOperator
implementation has no admitted-preparation override. It therefore also has a
source-level capability barrier. Resident Q9 ends in `MemoryTableScan`, which
implements admitted copies through `scan/admitted_memory.rs`. Its reserved child
working pool remains part of the shared query budget and must be preserved through
join and projection composition. These observations predict routing only; the
corrected binary's actual execution still needs validation.

## Corrected release and routing diagnostic

Release66680 completed in8m55s and froze `e4608ccf` with518verified inputs; only
`spillable.rs` and its admitted_queue_tests differ from3172f9e5. Diagnostic57734
is terminal0: all three independent typed outputs are correct. Raw5902.560719ms
and native2734.204327ms remain one-slot, as predicted by their scan contracts.
Resident1626.211741ms uses16/16slots with admitted_buffers=true and66completed
admitted_inner_probe traces. All180join traces complete. No source/volatile
replay is introduced; the normal scan residency budget remains separate.

Scope peak19719819264bytes,48GiB cap,swap0,zeroOOM/max events. All518source inputs,
binary, dataset, driver and harness pass after-run guards. This one-query sample
supports actual parallel routing but is not a regression bound or DuckDB
leadership result. Two reversed-order blocks against01bb077a are running next.

## Reversed-order matched comparison — terminal91923

Two fresh-process blocks use control/candidate order then candidate/control order,
with identical CPU0–15 affinity,16threads, SQL, provider data, memory budgets and
instrumentation. Control is01bb077a; candidate is e4608ccf. All12outputs match
independent typed oracles; all720join traces complete without unwinding. Both
resident candidate runs use16admitted frontier slots; raw/native and resident
control remain at1slot. No timeouts or process failures.

| Mode | Control mean ms | Candidate mean ms | Candidate/control | Block ratios |
|---|---:|---:|---:|---|
| Raw Parquet | 5911.349432 | 5880.945289 | 0.994857 | 0.989350,1.000414 |
| Native | 2705.868772 | 2710.935978 | 1.001873 | 0.996098,1.007621 |
| Resident CPU32GiB | 2842.938364 | 1606.900412 | 0.565225 | 0.557709,0.572979 |

Resident query time is43.48%lower in this matched diagnostic. Raw/native remain
essentially unchanged; they do not reach the new admitted pipeline. Two blocks
provide no confidence interval or protected-query regression bound. Resident
registration/preload is outside query timing; this is not an upload-inclusive
comparison or a canonical GPU result. The control is the previous full frozen
pipeline, so this estimates the combined admitted-expression/join/wrapper change,
not the isolated cost of one function.

Scope peak16174403584bytes,48GiB cap,swap0,zeroOOM/max. After-run binary/source,
dataset, driver and harness guards pass. Source archive remains518inputs.

## Next implementation and acceptance sequence

1. Add a general, ownership-preserving bridge from certified copied source output
   into admitted downstream computation; test detached arrays, repeated/empty
   batches, multiple partitions, cancellation, late errors and pressure before
   touching production routing. Never turn a compact-copy bound into a claim
   about retained source buffers. Preserve prepared streams and exact cursors.
2. Extend native scan preparation using its actual snapshot/selection and deletion
   semantics. Admit construction metadata and output owners; demonstrate bounded
   first output and correctness across mutations, not just factory availability.
3. For each changed scan boundary repeat the same raw/native/resident Q9 routing
   diagnostic, then protected canonical queries. Keep16GiB preload refusal and
   partial ownership's tight-budget failures separate and open.
4. Run full frozen raw/Iceberg/Lance/native and decoded/GPU residency screens,
   resource and concurrency gates under matched DuckDB ceilings. The current
   two-block result does not replace those gates or establish leadership.

[Pipeline correctness archive](benchmarks/2026-09-09-admitted-computed-pipeline/manifest.json),
[wrapper correctness archive](benchmarks/2026-09-09-admitted-inner-wrapper/manifest.json),
[initial negative diagnostic](benchmarks/2026-09-09-admitted-pipeline-q09-diagnostics/manifest.json),
[corrected diagnostic](benchmarks/2026-09-09-admitted-wrapper-q09-diagnostics/manifest.json).
