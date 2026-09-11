# Benchmark worker shutdown correction — September 8, 2026

Normal benchmark teardown used the same SIGKILL path as query deadlines. This
bypassed the DuckDB worker's explicit connection close and left roughly11.24GB
of temporary files after the Native SF10 screen. A tiny protocol-worker reproducer
failed deterministically: exit -9 and its EOF cleanup marker absent. The original
result is preserved under `.scratch/parallel-aggregate-input/worker-close-red/`.

Worker.close now requests EOF and waits at most10 seconds during normal teardown.
If cleanup does not finish, it kills and reaps the process group. Startup failures,
watchdog timeouts, malformed protocol and response-ID errors request immediate
abort, so normal teardown grace does not extend a query deadline. Close remains
idempotent and tolerates broken pipes/process-exit races. Each existing worker
record gains teardown evidence: requested action, whether termination was forced,
whether grace expired, and the actual exit code. Startup-read failures retain this
evidence even when they happen before the ready record is published.

New regressions cover clean EOF cleanup, startup/query deadline aborts, bounded
stuck cleanup and a real DuckDB spill. The real worker creates500000 rows with
256-byte payloads under a32MB DuckDB memory limit, asserts actual temporary files,
checks exact count and sum, then requires exit0 and no residual files after EOF.
It does not claim a query-engine performance result.

Focused lifecycle tests:8 passed, no skips. Full harness job72799:114 tests run,
112 passed and2 existing optional skips,2.745s. The skipped tests require
BENCHMARK_TEST_EXACT_BAG_SPILL=1 and BENCHMARK_TEST_LANCE=1. Tests ran under the
safe-build wrapper with2GiB containment, the pinned Python environment and
repository TMPDIR. Whitespace checks pass. No production engine code changed.

The Native leftovers were removed only after job11660 terminated and their exact
paths/sizes matched the archived inventory. Cleanup removed13 `.tmp` files,
11238014976 bytes; all benchmark results and failure records remain. The immutable
four-track archive records the pre-fix harness. Future screens must use the fixed
harness and still report forced cleanup as a separate resource outcome.

This does not resolve Q9 reference crashes, engine timeouts, missing residency
certification or the broader memory-budget contract. It also does not change SQL,
measured query elapsed times, the strict10× gate, or startup watchdog policy.
