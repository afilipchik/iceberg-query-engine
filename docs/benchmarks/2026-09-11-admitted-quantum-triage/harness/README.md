# Versioned benchmark harness

`prepare`, `convert`, `validate`, `run` and `report` implement a development
profile with separate raw Parquet, native, Iceberg, Lance, decoded IPC and
GPU-assisted tracks. The paired runner uses persistent embedded processes and
preserves SQL/data/binary hashes, worker setup, conversion evidence, all traces,
Arrow answers, plans, errors and cgroup counters. These adapters do not establish
full resource/concurrency or leadership certification.

Native compares with DuckDB native tables loaded from the same canonical source.
Iceberg registers original Parquet files and pins an immutable snapshot; both
engines read that snapshot through their actual providers. Lance pins the engine
version and requires the direct DuckDB extension's current version to match it.
Decoded IPC explicitly preloads Arrow into **both** engines; this is separate
from the historical asymmetric Parquet sidecar experiment. GPU uses raw Parquet
and requires a completed same-binary CPU control run. No extension installation,
fallback reader, type coercion, or row-count-only conversion validation is used.

The existing six-mode custom SF10 evidence remains in
[the dated baseline](../../docs/benchmark-baseline-sf10-2026-09-05.md). It predates
the current correctness changes. JOB, ClickBench, layouts, cap/concurrency
matrices and final certification are still pending.

## Reproduce on the development host

Run from the repository root. This host already has DuckDB 1.4.4, its `tpch`
extension, and PyArrow 25.0.1 in `.scratch/venv-lance`. On another host provide an
equivalent Python environment; extension auto-installation is disabled. The
SQL pin is checked before generation. The generator license is preserved in
`licenses/duckdb-tpch-v1.4.4-LICENSE` and copied into new datasets.

```bash
mkdir -p .scratch
export TMPDIR="$PWD/.scratch"
export PYTHONPATH="$PWD/scripts"
export BENCHMARK_PYTHON="$PWD/.scratch/venv-lance/bin/python"

SAFE_BUILD_MEM=64G scripts/claude-safe-build.sh cargo build --locked --release --example benchmark_embedded

SAFE_BUILD_MEM=4G scripts/claude-safe-build.sh "$BENCHMARK_PYTHON" -m benchmark prepare \
  --workload tpch --scale-factor 1 --output .scratch/public-bench/tpch-sf1

SAFE_BUILD_MEM=32G scripts/claude-safe-build.sh taskset -c 0-15 "$BENCHMARK_PYTHON" -m benchmark run \
  --dataset .scratch/public-bench/tpch-sf1/dataset.json \
  --engine-binary target/release/examples/benchmark_embedded \
  --output .scratch/public-bench/tpch-sf1-run \
  --samples 3 --sessions 1 --threads 16 --memory-gib 4 --process-cap-gib 12

SAFE_BUILD_MEM=4G scripts/claude-safe-build.sh "$BENCHMARK_PYTHON" -m benchmark report \
  --manifest .scratch/public-bench/tpch-sf1-run/manifest.json \
  --samples .scratch/public-bench/tpch-sf1-run/samples.jsonl \
  --output .scratch/public-bench/tpch-sf1-run/rechecked-report.json --gate complete

SAFE_BUILD_MEM=4G scripts/claude-safe-build.sh "$BENCHMARK_PYTHON" -m unittest discover -s scripts/benchmark/tests -v
cargo fmt --all -- --check
```

Preparation and run output directories must be new. Preserve failed runs and
choose a different destination when repeating. Never drop global page caches.
`scripts/safe_benchmark.sh` now delegates these commands through the real
memory-capped wrapper; its old flags fail with migration guidance. There is no
uncapped or `prlimit` fallback.

## Provider conversion and GPU control

Build the embedded example with `--features lance,gpu` when those modes are
required. Missing optional features, Python packages, DuckDB extensions or GPU
initialization fail visibly. The harness never downloads dependencies.

For each of `native`, `iceberg`, `lance`, and `decoded_ipc`, prepare a **new**
conversion directory and then pass its manifest to the runner. For example:

```bash
SAFE_BUILD_MEM=32G scripts/claude-safe-build.sh "$BENCHMARK_PYTHON" -m benchmark convert \
  --dataset .scratch/public-bench/tpch-sf1/dataset.json --track native \
  --engine-binary target/release/examples/benchmark_embedded \
  --output .scratch/public-bench/tpch-sf1-native

SAFE_BUILD_MEM=32G scripts/claude-safe-build.sh taskset -c 0-15 "$BENCHMARK_PYTHON" -m benchmark run \
  --dataset .scratch/public-bench/tpch-sf1/dataset.json --track native \
  --provider-manifest .scratch/public-bench/tpch-sf1-native/provider.json \
  --engine-binary target/release/examples/benchmark_embedded \
  --output .scratch/public-bench/tpch-sf1-native-run \
  --samples 3 --sessions 1 --threads 16 --memory-gib 4 --process-cap-gib 12
```

Conversion reads back every row through the engine and matched DuckDB provider,
requires the original column types (including decimal precision/scale), and
performs exact typed bag comparisons with the original source. It records all
provider file hashes, snapshots/versions, writer versions and validation results.
Both beginning and end of a run verify input inventories. Conversion currently
defaults to `--memory-gib 4 --process-cap-gib 12` and a 600-second engine
readback deadline (`--validation-timeout-seconds`). Full engine readback collects
results; a larger-than-budget conversion can fail and is not certified. Disk
admission requires an estimated eight times the current table's uncompressed
Parquet size (minimum 1 GiB) before starting that table. This estimate is not a
disk quota; filesystem and comparison failures leave a failed manifest.

GPU runs first require `--track gpu_control` with the GPU-enabled binary. Then
run `--track gpu --gpu-control-manifest <control-output>/manifest.json`, keeping
the binary, source dataset, threads, affinity and budgets identical. GPU queries
use `QE_GPU_DEBUG` trace records to count successful device executions, including
ones hidden inside CTE plans. Cache snapshots and upload/fallback observations
are retained per request. A GPU-labelled run with zero device executions remains
CPU execution evidence; it is never described as GPU acceleration. First-use
traces are retained, but synchronous isolated cold-upload measurement is not yet
implemented. Mixed CPU/GPU and observed device-executed samples are distinguishable.

## Evidence and timing contract

Timing starts immediately before parsing/execution and ends when the embedded
API has collected every Arrow result batch: `ExecutionContext::sql` versus
DuckDB `execute(...).fetch_arrow_table()`. Registration, IPC serialization,
oracle execution and validation are outside that timer and recorded separately.
Both APIs currently collect results; this is not the future bounded streaming
API. Each side has its own query budget and equal RLIMIT_DATA, with a shared
outer cgroup protecting the host. The two processes remain resident, but only
one query executes at a time. This is not concurrency certification.

Every query gets one warmup on each side, three fresh DuckDB calibration samples,
and the requested randomized pairs. LIMIT validation uses a separate unlimited
ordered DuckDB result and permits only eligible boundary ties with correct
multiplicity. Integer/decimal values compare exactly; floating values use
relative `1e-10` / absolute `1e-8` tolerance. Ordering and NULL placement are
explicit. Different integer widths and decimal scales can compare by exact
value; decimal and floating schemas are not silently interchanged.

The query ceiling is exactly ten times the median of the three calibrations.
Worker phase messages distinguish dispatch, query execution and serialization.
The process watchdog adds 100 ms for pipe/scheduling latency; the measured
10× gate has **no grace or minimum duration**. Warmup/query calibration and
serialization watchdogs are 120 seconds, dispatch 30 seconds, startup 120 seconds.
A signal is retained as a signal; SIGKILL alone is not labeled OOM. Cgroup OOM
events are recorded separately. A worker failure is isolated from later queries
by restarting the worker; unsuccessful samples never count as wins.

Missing, duplicate, malformed, incorrect or non-completed samples prevent
aggregate scores. Exit codes: 0 for the selected gate, 1 for failed evidence/gate,
2 for invalid input. `report` defaults to the latency leadership gate;
`--gate complete` checks correctness/completion/time ceilings only. Even a
successful development report cannot certify DuckDB leadership. Full latency
leadership requires the frozen workloads/modes, ten samples and three sessions,
per-session gates and paired hierarchical bootstrap intervals. Resource,
concurrency and protected regression certification remain separate.

Comparison uses disk-backed SQLite with an 8 MiB page cache. Arrow batches are
processed in small Python chunks. Ambiguous floating buckets above 2,000 rows
fail explicitly; large-result validation and disk-space admission need further
work. Do not interpret this bound as general full-workload certification.

## Current findings

See [canonical SF1 correctness findings](../../docs/canonical-sf1-findings-2026-09-05.md)
for the reproduced failures and small counterexamples. Fix SQL/type semantics
before optimizing timings that were obtained from wrong answers.

## Comparing two engine binaries

Use `python -m benchmark.paired` for a canonical SF10 CPU development screen.
Supply `--before-binary`, `--after-binary`, both `--expected-*-sha256` values,
`--track`, the matching `--provider-manifest` when required, and a fresh `--output`
under repository `.scratch`. Run through the memory-capped wrapper in96 GiB with
`taskset -c 0-15`; this host-specific screen uses16 threads and40/48 GiB query/
process limits. `--samples 10` retains10 measured pairs plus one gated warmup.

Balance both `--startup-order before-first|after-first` and
`--execution-offset 0|1` across fresh sessions. Include identical-binary controls;
alternating execution order alone does not balance process startup. Preserve
means/totals/maxima as well as medians because deferred cleanup can change the
median without reducing total work. Every invocation remains a development
screen, not certification. See the [startup-order investigation](../../docs/benchmark-startup-order-2026-09-07.md).
