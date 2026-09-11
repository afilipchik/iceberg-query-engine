# Exact timestamp scalar domain — 2026-09-07

Status: implementation and validation in progress. Initial contained gate passes
686 library and11 integration tests, with two pre-existing library ignores.
The source differs from frozen635; no635 or older performance measurement applies.

The reproduced defect was systemic: SQL timestamp precision/timezone was discarded
at binding, while scalar storage, extraction and expansion represented only
microseconds without timezone. Changing only the binder would have moved the
corruption to scalar subqueries and output construction.

`planner/temporal.rs::TimestampValue` now carries exact i64 ticks, Arrow TimeUnit
and optional timezone. Scalar identity and display include the complete domain.
Timestamp casts can fold only when their exact output type is preserved. Scalar
subqueries extract all four Arrow timestamp units without floating conversion.
Ordinary expansion and reserved expansion wrap raw Int64 counts with the same
metadata; reserved buffers retain their lease owners across zero-copy wrapping.
The former Timestamp(i64) Rust enum payload is now Timestamp(TimestampValue);
From<i64> constructs the former microsecond/None domain explicitly.

The binder maps precision0 to seconds,1–3 to milliseconds,4–6/default to
microseconds,7–9 to nanoseconds; values outside0–9 error before execution.
WITH TIME ZONE/TIMESTAMPTZ uses microseconds with UTC metadata and rejects precision
modifiers. This engine has no newly implemented configurable session timezone.
Provider arrays retain their own timezone metadata when extracted and expanded.
Native ColumnStats lacks timestamp unit metadata, so timestamp pruning is now
conservative instead of comparing potentially incompatible raw counts. Parquet's
existing logical-type compatibility gate remains in force.

The independently installed DuckDB1.4.4 reference was queried under containment;
raw Arrow units/timezones/i64 ticks and invalid-modifier errors are preserved in
`.scratch/temporal-domain-repair/reference.json`. It unexpectedly accepts precision10
and handles explicit offsets differently between naive microsecond/nanosecond
string types; those behaviors are not silently adopted as SQL contracts here.
The documented timestamp domains and timezone distinction are described in
[DuckDB's timestamp documentation](https://duckdb.org/docs/current/sql/data_types/timestamp.html).

Commands and logs:

```bash
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=10G scripts/claude-safe-build.sh .scratch/venv-lance/bin/python .scratch/temporal-domain-repair/reference.py
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh cargo test --locked --lib --test typed_literal_contract --test timestamp_domain_contract
```

Initial log: contracts.log. Tests cover exact negative epochs, direct/folded/typed
casts, scalar subqueries, all Arrow units and timezone metadata, grouped minimums,
NULL/empty results and invalid precision declarations. Further tests cover i64
extremes, escaped-buffer lease lifetime/refusal and cross-precision predicates.

Still required: complete the extended gates and independent optimized execution,
then relevant provider/regression validation. Date arithmetic/truncation/extraction
functions have microsecond-specific routes that require a separate domain audit;
this patch does not certify them for every new unit/timezone. Offset parsing of
naive strings and session-timezone semantics remain explicit follow-up domains.
The cast-performance regression, query-wide memory ownership, public-workload and
leadership gates remain open. No broad temporal or performance completion is claimed.

## Extended validation before freeze

The first cross-precision predicate test reproduced `Cannot coerce Timestamp...`
in shared type inference (integration-contracts.log). The common-type rule now
selects the finer timestamp unit, retains identical zones and uses UTC for mixed
zone metadata. Strict casts retain overflow errors. The complete rerun passes all
35 selected integration tests (integration-contracts-02.log).

The full shared-contract rerun passes687 library tests and30 selected integration
tests, with two existing library ignores. Accounting for overlaps and the six
typed-literal tests gives755 unique passing Rust tests. The focused lease test
passed separately; its command filter selected zero integration tests, so that
invocation is not counted as integration coverage. The later unfiltered run covers
all six timestamp integration tests. All logs are retained under the scratch root.

Next: freeze the source, build the lance+gpu optimized benchmark/cap binaries,
rerun independent semantic oracles (requiring all three former metadata failures
to match), then provider/performance/resource gates. No optimized acceptance yet.

Frozen638 source:638 verified members, SHA256
`484e1e6b2eb7197d1f739607df878363c94f2b03c0d6832a2480f40cf4ce5200`.
Release build57004 is running with lance+gpu,64GiB/jobs1. The archived guide
precedes this build-status update; production source remains frozen.

The two existing library ignores are
`optimizer::rules::flatten_dependent_join::tests::test_flatten_exists` and
`physical::planner::narrowed_streaming_read_dependencies::narrowed_existing_ipc_reads_predicate_and_two_absent_runtime_roots`
(the latter requires a dedicated QE_IPC_CACHE=auto sidecar process). Neither is
counted as covered by these test runs.

## Optimized validation

Build57004 completed successfully in10m43s. Optimized binary SHA256:
`fa33d4754a1e88348b300b1cd96e75b9d187daeb4189fb1b3ef4397e20b3733d`.
Validation42145 passed:89 float/date,10 dense-float and56 coercion queries match
DuckDB;58 primitive cases match the preserved control (50 match DuckDB, eight
pre-existing integer-division differences). Literal checks retain19 canonical
matches and the bounded bare-NULL value/schema distinction. Expanded literal,
DOUBLE and DECIMAL buffers refuse by memory name under64KiB while borrowed input
succeeds. Decimal metadata wrap cases continue to reject.

All three original timestamp metadata probes now match. The additional
`timestamp-oracle.py` run passes43 queries covering all declared precisions0–9,
scalar subqueries, typed NULLs, MIN/MAX, negative epochs, aware offsets, empty
output and a mixed-precision predicate. Comparisons include field names, logical
Arrow types, raw i64 ticks and NULL values; schema metadata/nullability is preserved
in raw files but is not part of that Python comparison. DuckDB normalizes aware
provider columns to microseconds; non-microsecond aware preservation remains
covered by the independent Rust domain tests, not misreported as DuckDB parity.

Commands (10GiB scope with repository TMPDIR and PYTHONPATH=scripts):
`python validate-release.py` and `python timestamp-oracle.py`, using
`.scratch/venv-lance/bin/python` and the mandatory wrapper. Logs and exact binary
identities are under `.scratch/temporal-domain-repair/`. Component91451 is running;
no638 provider/performance/GPU/cap acceptance yet.

## Component screen and current provider run

Component91451 completed all352 paired requests with typed correctness and time
gates. Ratios638/635 for borrowed input, integer addition, float multiplication,
integer-to-string, mixed output, integer-to-double, integer-to-decimal and
overflow-heavy TRY_CAST:0.88757,0.98657,1.00101,0.97418,0.97631,0.94263,0.92051,0.99532.
No new>10% component flag in this session. Against612:0.99143,0.99345,1.08470,
1.01144,1.02516,0.95880,0.72409,0.88899. These original eight components do not
close the previously failing valid/nullable TRY_CAST distribution gates.

Full canonicalSF10 screen53414 is running in96GiB on CPUs0–15: decodedIPC,
rawParquet, native, Iceberg and Lance; three steady pairs per query against612,
fresh matched DuckDB time ceilings, all raw outputs/plans preserved. This is a
development screen, not the three-session leadership benchmark. GPU supported
residency and aggregate-cap drivers are prepared and will run sequentially after
CPU latency measurement. No full638 provider/GPU/cap result is claimed yet.

## Completed provider, protected, GPU and cap validation

Full screen53414 completed all880 typed/time-gated requests. Ratios below use
suite sums of medians and per-query geometric means; all per-query samples and
ratios are retained in screen-summary.json and the raw case files. This is one
three-pair development screen, not the multi-session leadership gate.

| Mode | Suite/612 | Geomean/612 | Suite/DuckDB | Geomean/DuckDB | Wins/DuckDB |
|---|---:|---:|---:|---:|---:|
| decoded_ipc | 0.9601 | 0.9762 | 0.4668 | 0.3923 | 18/22 |
| raw_parquet | 0.9982 | 0.9909 | 2.4240 | 2.1769 | 1/22 |
| native | 0.9588 | 0.9818 | 3.5218 | 3.1862 | 0/22 |
| iceberg | 0.9891 | 0.9943 | 0.3175 | 0.2934 | 20/22 |
| lance | 0.9365 | 0.9514 | 1.2555 | 1.1182 | 9/22 |

Lance Q8 initially flagged1.21962, then repeated0.99422/1.02289. Q20 initially
flagged1.13446; four fresh repeats are1.07130/1.14040/1.05113/0.99266. All132
protected requests pass correctness/time gates. Q20 remains variable; all sessions
are preserved rather than choosing only favorable runs. No overall performance
adoption is claimed: prior valid/nullable TRY_CAST distribution regressions remain
open, and raw Parquet/native/Lance still exceed DuckDB suite time substantially.

GPU41674 passed40 CPU-control and40 required-device executions on the supported
custom600k-row float fixture. Every device sample records device_executed and one
successful request-scoped device run, with256MiB cache target.16GiB host scope,
CPUs0–3; NVRTC from `.venv/lib/python3.12/site-packages/nvidia/cuda_nvrtc/lib`. This
is supported-path validation, not canonicalSF10 device coverage or a hard VRAM cap.

Cap6231 passes both250-million-row aggregate scenarios:1,000,003 groups, exact
counts and actual spill (3,855,541,894 accounted bytes). PeakRSS410MiB under1GiB
cgroup and412MiB under2GiB RLIMIT_DATA; the process-limit scenario has8GiB outer
containment. These gates do not establish query-wide reservation coverage.

Current638 query-level triage again ranksQ18/Q13/Q1 first using reference-suite
normalization and equal required-track weights. The current Q7 plan also confirms
ten-fold derived-predicate growth. The next optimizer proposal is scratch-only,
not integrated or tested; see the linked convergence proposal before applying it.

## Final evidence archive

[Archive and manifest](benchmarks/2026-09-07-timestamp-domain/README.md):4,421
verified files, archive SHA256
`fcbc8b34f0933b281fb07b3c6ad87e1c0fd065545f917496b6a43dbbe956874a`.
Source638, binaries, tests, all raw oracles/screens/repeats/GPU/caps and the then-
unintegrated optimizer proposal are preserved. The archived report predates this
link paragraph. No638 jobs remain live; the next optimizer red tests have started.
