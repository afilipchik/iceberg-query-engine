# Input progress credit: canonical Lance Q7 diagnostic

The general input-credit repair clears the reproduced Lance Q7 refusal in two
matched diagnostic blocks. The old fde271b1 binary refuses twice in `memory scan
working space`; the new 38966ae6 binary completes twice with independently
validated typed results. This establishes a workload consequence of the generic
resource bug. It does not certify the full Lance suite or a DuckDB speedup.

## Frozen implementation and conditions

Release session 42098 completed in 8m52s. SHA256:
`38966ae6b181c3d84223b7f11864cd97ab719d44f41c6079dc16094d67def07d`.
All 519 source inputs verify. The two engine-file changes are the shared memory
credit mode and its use by admitted memory scans. The additional test edit makes
an existing panic-cleanup test compile with opaque pool owners. See the
[contract and red/green validation](retained-input-progress-credit-2026-09-10.md).

Session 72127 completed with exit 0. Two blocks reverse old/new execution order;
each request starts a fresh process. Canonical SF10 Lance data, 16 threads on
CPU 0–15, 4 GiB query budget, 12 GiB process cap, default disjoint ownership,
GPU disabled. All invocations and independent typed comparisons run under the
required 48 GiB cgroup with swap disabled and repository TMPDIR.

The 180-second diagnostic watchdog is separate from benchmark acceptance. These
trials do not replace a fresh matched DuckDB calibration or its 10× query ceiling.
Every completed output is compared with the canonical DuckDB oracle using the
registered ordered typed policy, including exact decimal revenue.

## Outcomes

| Block | Old binary | New binary | New query time | Reserved peak |
|---|---|---|---:|---:|
| Old then new | Named memory refusal | Typed correct, 4 rows | 556.683 ms | 1,813,194,311 B |
| New then old | Named memory refusal | Typed correct, 4 rows | 553.308 ms | 1,813,194,295 B |

The old refusals have the same 410,718,424-byte child limit. The first requests
20,480 bytes at 410,714,363 used; the second requests 63,080 at 410,688,369 used.
Their refusal latencies are not successful query latencies and must not be used
as a speedup denominator. The new query uses parent-backed growth beyond that
fixed child allowance while retaining the query-wide budget.

The scope peak is 9,834,819,584 bytes, with zero max/OOM events. Cgroup peak
includes provider setup, engine and comparator work; reserved peak is accounting
telemetry, not a complete RSS proof. Source, binaries, provider, dataset, driver
and harness guards verify after the trials. Full raw/native/resident shared-path
comparison88178 subsequently completed64typed-correct outputs; see the
[results and qualifications](retained-input-credit-measurement-2026-09-10.md).

[Immutable diagnostic archive](benchmarks/2026-09-10-retained-input-credit-q07/manifest.json)
retains all requests, responses, errors, profiles, typed checks, independent oracle,
commands, source provenance, build logs and resource observations. No failure was
retried until it passed or hidden by increasing the query budget.
