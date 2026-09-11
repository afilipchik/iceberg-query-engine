# Cumulative reader candidate paired results — September 9, 2026

Candidate11d16e73 includes IPC extent checking, projection/join schema metadata
accounting, consumed-header release, adaptive fixed output/early handoff admission
and incremental dictionary-ID prefixes. Baselinea023079f is the preceding frozen
candidate. These measurements cannot attribute a change to one repair alone.
Release15551 terminal0 in8m48s;495 source hashes verified. No dependency changes.

Paired82129 terminal1: custom phase succeeds; canonical phase fails completion.
Four fresh-process blocks, six measured pairs plus warmup per query, balanced
startup/execution order. Each block has a fresh DuckDB calibration and10x query
ceiling; full typed oracles and failures are preserved. Canonical raw SF10 uses
16threads,4GiB query/12GiB process; custom600000-row resident float control uses
4threads,4GiB query/8GiB process. GPU disabled. Timing includes parse to consumed
Arrow. These are protected/component comparisons, not complete suite certification.

Canonical:355 completed outputs all correct,348 gated,7 late,16 timeouts,133
not_run. All36 reference blocks valid. Custom:112 completed, all correct/gated,
all8 reference blocks valid. Total467 completed correct/460 gated. Canonical
scope peak5652566016 bytes, custom695984128; zero OOM/max events. Scope peaks are
not query admission or per-process RSS certification.

| Query | After/before geometric block ratio | 95% block-bootstrap interval |
|---|---:|---:|
| Canonical Q1 |0.99508|0.98685–1.00337|
| Canonical Q2 |1.02751|0.93268–1.12422|
| Canonical Q5 |1.01168|0.98780–1.03614|
| Canonical Q10 |0.98550|0.96154–1.01197|
| Canonical Q19 |1.00029|0.96738–1.02817|
| Canonical Q20 |0.99669|0.95602–1.03909|
| Custom Q1 |0.97534|0.94328–1.00850|
| Custom Q6 |0.86167|0.62526–1.05759|

No complete interval establishes a speed change. Q9/Q13 time out on both binaries
in all blocks; Q12 has completed-late calls and no complete four-block estimate.
Q2 alone has an upper bound above1.10 and requires the existing expanded protected
confirmation:8fresh blocks,12pairs each, unchanged query/budgets/gates. The first
study remains immutable regardless of follow-up outcome. No rerun-until-pass.

Drivers: measure_dictionary_chunks.py, measure_dictionary_chunks_smoke.py and
run_dictionary_chunks_measurements.py under .scratch/parallel-aggregate-input.
Invocation: TMPDIR=$PWD/.scratch PYTHONPATH=$PWD/scripts RAYON_NUM_THREADS=4
SAFE_BUILD_MEM=48G SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh
.scratch/venv-lance/bin/python .scratch/parallel-aggregate-input/run_dictionary_chunks_measurements.py.
[Archive](benchmarks/2026-09-09-dictionary-chunks-pairs/manifest.json) preserves
outputs, plans/events, source tarball/hashes, harness, drivers and failure counts.
Provider/residency/resource/concurrency qualification remains open.

## Local source follow-up

DuckDB checkout1c27c54f27bea397d18d86f7c0d6a9f1d4aa85f8,
extension/parquet/decoder/dictionary_decoder.cpp::Read decodes batch-sized IDs
into a selection vector and checks their exact domain. At result_offset0 it
returns a dictionary vector; at a nonzero offset it copies into flat output.
Our admitted reader now bounds IDs but still expands fixed/string values into
flat Arrow arrays. Preserving encoded values is a separate physical-schema and
operator contract, not justified by this neutral performance result alone.
Profile expansion/filter/group use first; maintain representation-aware schema,
NULL and ownership rules. No upstream code was copied or host settings changed.

Expanded Q2 confirmation7394 terminal0:8fresh blocks and96measured pairs plus16
warmup outputs (208 total) all correct/gated. Ratio1.01530,95%0.96282–1.06577.
This bounds slowdown below10% for this confirmation without establishing a speed
change. The initial wider interval remains preserved. No further Q2 rerun is
needed under the protected-regression rule.
[Follow-up evidence](benchmarks/2026-09-09-dictionary-chunks-q2-followup/manifest.json).
