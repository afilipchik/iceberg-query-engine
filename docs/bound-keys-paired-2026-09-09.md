# Bound key arrays: protected comparison — 2026-09-09

Candidate `0f30c946` is compared with frozen threshold control `1efb2554`.
This protected component screen does not certify canonical SF10 leadership.

There are 511 completed engine outputs; all 511
validate against independent typed oracles. 504 completed outputs
pass their fresh DuckDB10× query ceiling; 7 correct outputs are
late. The study preserves 9 timeouts, 96 not-run
slots and 0 invalid reference blocks.
Incomplete query studies: Canonical raw q09, Canonical raw q12.

| Workload/query | Candidate/control ratio | Block-bootstrap95% interval |
|---|---:|---|
| Canonical raw q01 | 0.955056 | 0.914724–0.997166 |
| Canonical raw q02 | 1.021262 | 1.014515–1.032059 |
| Canonical raw q05 | 0.998516 | 0.979665–1.021981 |
| Canonical raw q09 | Incomplete | Not estimated |
| Canonical raw q10 | 0.973140 | 0.967174–0.979144 |
| Canonical raw q12 | Incomplete | Not estimated |
| Canonical raw q13 | 0.933711 | 0.923321–0.943638 |
| Canonical raw q19 | 1.017661 | 0.978665–1.058211 |
| Canonical raw q20 | 0.993290 | 0.955079–1.027703 |
| Custom memory q01 | 0.984982 | 0.962254–1.009698 |
| Custom memory q06 | 0.958153 | 0.920420–0.997433 |

Four fresh blocks use six measured pairs and one warmup per side, balanced
process startup and request order. Canonical raw Parquet uses16 threads,
4 GiB query/12 GiB process limits and affinity0–15; custom memory uses four
threads and4/8 GiB. DuckDB is calibrated afresh in each block. SQL, schemas,
data and timing boundaries are matched. Ratios summarize complete paired
block medians; incomplete studies do not receive a ratio. Short-query
alternating-request precision remains qualified by the prior null-control
failure; the prespecified physical-core fixed-window Q6 screen is separate.

Execution2204 contains the sequential custom and canonical children. Source
and binary hashes verify after both children; source remains frozen for the
remaining provider/residency/resource gates. Preserve every failed condition;
a lower observed ratio alone does not prove causality on unchanged raw routes.

Run `run_bound_keys_measurements.py` from the archived drivers using
`TMPDIR="$PWD/.scratch" PYTHONPATH="$PWD/scripts" SAFE_BUILD_MEM=48G
SAFE_BUILD_JOBS=1 scripts/claude-safe-build.sh taskset -c 0-15
.scratch/venv-lance/bin/python`. Output roots are exclusive. The
[archive](benchmarks/2026-09-09-bound-keys-pairs/manifest.json) preserves outputs,
failures, plans, raw samples, harness and all508 source inputs.

The48GiB measurement scope peaks at4,543,655,936 bytes with zero OOM/max events.
The verified archive contains1,180 files and508 source inputs. Q2 shows a small
slowdown; all complete upper95% bounds stay below1.059 under these conditions.
Q10 andQ13 intervals correspond to about2.1–3.3% and5.6–7.7% lower time,
respectively. Q1 uses an unchanged raw route, so its observed reduction requires
separate attribution. Neither Q9 norQ12 has a complete paired result.
