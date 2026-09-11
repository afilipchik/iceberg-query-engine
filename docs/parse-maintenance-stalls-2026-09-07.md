# Deferred memory cleanup in query parse latency

The confirmed Lance Q20 regression in frozen640 led to a systemic finding:
large bursts of main-thread `madvise(..., MADV_DONTNEED)` calls occur near the
start of later queries and dominate windows reporting slow parsing. This happens
in both612 and640, at different frequencies. No parser source changed between
them. The allocator call stack and the producer of the discarded allocations
still need attribution; no production fix or performance acceptance is claimed.

## Evidence

Three contained diagnostics each completed44 engine requests with independent
DuckDB typed comparisons and fresh10× time ceilings: sequential/all-thread trace,
paired/main-thread trace, then paired/main-thread trace including futex/yield/sleep.
All132 requests passed. Each run includes untraced controls. Traced timings are
not acceptance benchmarks: all-thread tracing raised total query time substantially
and changed execution cadence. Every raw sample and trace remains preserved.

The paired runs maintain both engine workers and alternate execution order as
in the protected Q20 comparison. CPU affinity0–15,40GiB query budget,48GiB process
cap,96GiB outer scope; GPU and IPC caching are disabled. Lance snapshots and SQL
match the preserved canonical SF10 setup. Binary digests are verified before use.

Representative main-thread trace observations:

| Binary/run | Sample | Parse ms | DONTNEED calls | Sum syscall ms |
|---|---:|---:|---:|---:|
| 612/main | 8 | 83.982 | 2,430 | 68.421 |
| 640/main | 2 | 86.246 | 2,824 | 68.404 |
| 640/main | 5 | 12.105 | 574 | 8.340 |
| 640/main | 7 | 75.007 | 2,422 | 59.474 |
| 612/locks | 8 | 92.037 | 2,849 | 74.235 |
| 640/locks | 1 | 44.695 | 1,273 | 36.404 |
| 640/locks | 9 | 48.305 | 1,580 | 37.715 |

These are approximate parse windows: their start is the protocol `started` write,
slightly before the internal parse timer, and their length is the measured parse
time. They are not exact per-phase CPU attribution. The raw syscall timestamps,
protocol events and all samples permit independent review. Fast windows contain
no such memory calls. The lock-enabled run finds no futex/yield/sleep calls in
these slow windows. Most individual memory calls are short; summing them is
necessary. The initial search for a single long syscall was insufficient.

The current CPU hierarchy inspection shows unlimited ancestor CPU quotas and
zero recorded throttling. It does not certify absence of all scheduler pressure.
No host scheduling, allocator or memory-protection settings were changed.

## Interpretation and next steps

This is evidence of deferred page discard affecting request latency, rather than
expensive SQL syntax processing. The engine globally uses mimalloc through its
disabled profiling wrapper; sqlparser performs ordinary AST allocations inside
the parse timer. Attribution specifically to mimalloc maintenance is a strong
source-backed inference, but syscall call stacks have not yet been captured.
The allocation source and why cadence differs between binaries remain unproven.

Next obtain bounded call-stack evidence, then connect discarded ranges/bytes to
provider materialization, operator state or result lifetime. Prefer reducing or
reusing the allocations under the existing query-wide admission rules. Keep any
allocator-policy experiment separate, with long repeated streams, tail latency,
CPU, RSS, refusal and spill evidence across providers and workload shapes.

Do not move cleanup outside the benchmark timing boundary to manufacture a gain.
Do not disable reclamation or resource protection to retain an apparent speedup.
The original protected Q20 failures still reject overall640 performance; changed
stall frequency in a diagnostic does not clear them. The separate structural OR
identity counterexample also remains unexecuted and needs a semantic regression.

Drivers, results, process counters, CPU hierarchy and raw traces are under
`.scratch/parse-maintenance-profile/`. Frozen640's complete acceptance record is
in [the optimizer report](optimizer-convergence-proposal-2026-09-07.md).

[Verified diagnostic archive](benchmarks/2026-09-07-parse-maintenance/README.md): 1119 files, all member hashes checked. Archive SHA `9a9d5e09149f3ad980f62bc0293758196ff7581e32efa7d2609a515e8e624c76`. All diagnostic processes are terminal; no production source or dependency changed.

## Subsequent bounded stack attribution

[Stack evidence](benchmarks/2026-09-07-mimalloc-discard-stacks/README.md) captures
at most32 main-thread madvise calls per binary. Both requests pass typed/time
gates. The discard stacks identify mimalloc `_mi_prim_decommit`; unwinding stops
at that primitive, so a higher-level allocation producer is not proven. The
read/write bridge's per-operation thread churn is separately reproduced and a
persistent-runtime candidate is described in the Lance bridge report. Its impact
on discard bursts remains a measurement question.
