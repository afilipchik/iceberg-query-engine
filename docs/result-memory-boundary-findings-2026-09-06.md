# Computed projection and collected-result budget gap

A small optimized-release probe reproduces an unenforced query-memory boundary.
The binary is the validated 612-file candidate, SHA-256
`e8174895539343b61796925247fedebdfef45f54caa4f133d4496e70df90aae6`.
It runs in a 10 GiB scope with swap disabled, an 8 GiB process cap, one thread,
a 64 KiB query budget and 16,384 resident Int64 input rows. No latency benchmark
was running concurrently. This is a bounded diagnostic, not an OOM experiment.

| Query | Output logical bytes | Reserved/observed peak | Result |
|---|---:|---:|---|
| `SELECT i FROM t` | 131072 | 0 / 0 | Correct borrowed-input control |
| `SELECT i + 1 AS v FROM t` | 131072 | 0 / 0 | Correct values; new output exceeds query budget |
| `SELECT CAST(i AS VARCHAR) AS v FROM t` | 136346 | 0 / 0 | Correct values; new output exceeds query budget |

All values match independent DuckDB evaluation. The borrowed-input control alone
would not establish a working-allocation violation: its buffers were resident
before the query. The arithmetic and string conversions allocate new output;
each output alone exceeds the configured query budget. Zero pool telemetry
therefore cannot certify resource safety or measure total query working memory.

`ProjectExec` evaluates expressions without a pool/admission contract. The SQL
collector in `execution/context.rs` retains all partitions through `try_collect`
and `join_all`, then decodes dictionaries without result ownership reservations.
Public `QueryResult.batches` allows callers to clone or move Arrow arrays. Adding
a guard only to QueryResult would release the charge while escaped arrays remain
alive. Charging only after expression evaluation would also leave the allocation
itself unprotected. These are separate admission and lifetime obligations.

The next implementation must define pre-allocation bounds for expression output
and dictionary expansion, explicitly handle unknown variable-size output, and
carry reservations for as long as retained buffers remain owned. Streaming and
collected API paths must share the contract. Test multiple partitions, batches,
borrowed slices, computed fixed/variable output, errors/cancellation, and escaped
results; require named refusal or admitted execution and verify release on drop.
Do not describe a post-allocation collector check as complete memory safety.
No production fix is made in this checkpoint.

[Verified evidence](benchmarks/2026-09-06-result-memory-boundary/members-sha256.json)
preserves input/output Arrow streams, setup, plans, metrics, probe source and logs.
Reproduce with the repository wrapper:

```bash
TMPDIR="$PWD/.scratch" SAFE_BUILD_MEM=10G SAFE_BUILD_JOBS=1 \
  scripts/claude-safe-build.sh env PYTHONPATH="$PWD/scripts" \
  .scratch/venv-lance/bin/python .scratch/result-memory-boundary/probe.py \
  .scratch/group-key-equivalence-repair/release-binary \
  .scratch/result-memory-boundary/run-02
```

The [admission/ownership design](result-memory-admission-design-2026-09-06.md)
now includes five passing lifetime feasibility tests and the demonstrated opaque
owner capacity caveat. These are not a production fix for the reproduced failure.
